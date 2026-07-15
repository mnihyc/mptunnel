//! Pure production path eligibility and completion scoring.
//!
//! Formulas consume live evidence and protocol-derived timing; this module owns
//! no queues, flow lifetimes, carrier I/O, or simulator-only heuristics.

use super::FlowLane;
use crate::model::path::PathPolicy;
use crate::protocol::{PathId, UnderlayProtocol};

const BBR_DEFAULT_CWND_GAIN: f64 = 2.0;
pub(crate) const QUIC_INITIAL_WINDOW_PACKETS: f64 = 10.0;
const QUIC_MAX_ACK_DELAY_MS: f64 = 25.0;
const QUIC_PERSISTENT_CONGESTION_THRESHOLD: f64 = 3.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathState {
    Active,
    Suspect,
    Draining,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathRateScope {
    /// Product ACK timing already measures one flow's delivered share.
    PerFlowGoodput,
    /// Carrier telemetry or a configured prior describes shared path capacity.
    PathCapacity,
}

#[derive(Debug, Clone, Copy)]
pub struct PathSnapshot {
    pub id: PathId,
    pub underlay: UnderlayProtocol,
    pub state: PathState,
    pub policy: PathPolicy,
    pub srtt_ms: f64,
    pub jitter_ms: f64,
    pub delivery_rate_bps: f64,
    pub rate_scope: PathRateScope,
    pub product_progress_rate_bps: Option<f64>,
    /// Exact product ACK accounting satisfies the transport-specific durable
    /// sample threshold; a point rate alone is not admission evidence.
    pub has_durable_product_progress: bool,
    pub loss_rate: f64,
    pub queue_bytes: u64,
    pub product_queue_bytes: u64,
    pub bytes_in_flight: u64,
    pub product_bytes_in_flight: u64,
    pub active_flows: u32,
    pub active_latency_sensitive_flows: u32,
    pub session_active_latency_sensitive_flows: u32,
    pub pacing_rate_bps: f64,
    pub inflight_limit_bytes: u64,
    pub confidence: f64,
    pub app_limited: bool,
}

impl PathSnapshot {
    pub fn new(
        id: PathId,
        underlay: UnderlayProtocol,
        srtt_ms: f64,
        delivery_rate_bps: f64,
    ) -> Self {
        Self {
            id,
            underlay,
            state: PathState::Active,
            policy: PathPolicy::default(),
            srtt_ms,
            jitter_ms: 0.0,
            delivery_rate_bps,
            rate_scope: PathRateScope::PathCapacity,
            product_progress_rate_bps: None,
            has_durable_product_progress: false,
            loss_rate: 0.0,
            queue_bytes: 0,
            product_queue_bytes: 0,
            bytes_in_flight: 0,
            product_bytes_in_flight: 0,
            active_flows: 0,
            active_latency_sensitive_flows: 0,
            session_active_latency_sensitive_flows: 0,
            pacing_rate_bps: delivery_rate_bps,
            inflight_limit_bytes: 0,
            confidence: 1.0,
            app_limited: false,
        }
    }
}

/// Retains a current lead when the challenger does not clear measured timing
/// and one scheduling quantum of queue uncertainty.
pub(crate) fn path_within_adaptive_lead_hysteresis(
    old_eta_ms: f64,
    old_snapshot: PathSnapshot,
    best_eta_ms: f64,
    best_snapshot: PathSnapshot,
    payload_bytes: usize,
) -> bool {
    let jitter_hysteresis_ms = old_snapshot.jitter_ms.max(best_snapshot.jitter_ms);
    let queue_hysteresis_bytes = payload_bytes as u64;
    old_eta_ms <= best_eta_ms + jitter_hysteresis_ms
        && old_snapshot.queue_bytes <= best_snapshot.queue_bytes + queue_hysteresis_bytes
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PathScore {
    pub path_id: PathId,
    pub eta_ms: f64,
}

pub fn choose_path(
    paths: &[PathSnapshot],
    lane: FlowLane,
    payload_bytes: usize,
) -> Option<PathScore> {
    paths
        .iter()
        .filter_map(|path| score_path(*path, lane, payload_bytes))
        .min_by(|left, right| left.eta_ms.total_cmp(&right.eta_ms))
}

pub fn score_path(path: PathSnapshot, lane: FlowLane, payload_bytes: usize) -> Option<PathScore> {
    if !path_is_schedulable(path, lane) {
        return None;
    }

    let rate = effective_path_rate_bps(path, lane);
    let effective_inflight = if path.inflight_limit_bytes > 0 {
        let adaptive_ceiling =
            (path.inflight_limit_bytes as f64 * BBR_DEFAULT_CWND_GAIN).ceil() as u64;
        path.bytes_in_flight
            .min(adaptive_ceiling.max(path.inflight_limit_bytes))
    } else {
        path.bytes_in_flight
    };
    let queued_bits = path
        .queue_bytes
        .saturating_add(path.product_queue_bytes)
        .saturating_add(effective_inflight) as f64
        * 8.0;
    let payload_bits = payload_bytes as f64 * 8.0;

    let mut eta_ms = path.srtt_ms / 2.0;
    eta_ms += queued_bits / rate * 1000.0;
    eta_ms += payload_bits / rate * 1000.0;
    eta_ms += path.jitter_ms;
    eta_ms += adaptive_loss_repair_penalty_ms(path);
    eta_ms += adaptive_low_confidence_penalty_ms(path);
    eta_ms += active_flow_penalty_ms(path, lane);

    if path.state == PathState::Suspect {
        eta_ms += suspect_penalty_ms(path, lane);
    }
    if path.policy.backup {
        eta_ms += adaptive_backup_penalty_ms(path);
    }
    if path.policy.expensive {
        eta_ms += adaptive_expensive_path_penalty_ms(path, payload_bytes);
    }
    Some(PathScore {
        path_id: path.id,
        eta_ms,
    })
}

fn active_flow_penalty_ms(path: PathSnapshot, lane: FlowLane) -> f64 {
    match lane {
        FlowLane::Throughput | FlowLane::Background => {
            f64::from(path.active_latency_sensitive_flows) * path_pto_ms(path)
        }
        FlowLane::Control | FlowLane::RealtimeDatagram | FlowLane::Latency => {
            f64::from(path.active_flows) * path_pto_ms(path) / QUIC_INITIAL_WINDOW_PACKETS
        }
    }
}

fn effective_path_rate_bps(path: PathSnapshot, lane: FlowLane) -> f64 {
    let rate = match path.rate_scope {
        PathRateScope::PerFlowGoodput => path.delivery_rate_bps,
        PathRateScope::PathCapacity => path.pacing_rate_bps.max(path.delivery_rate_bps),
    }
    .max(1.0);
    match lane {
        FlowLane::Throughput | FlowLane::Background
            if matches!(path.rate_scope, PathRateScope::PathCapacity) =>
        {
            let active_bulk_flows = path
                .active_flows
                .saturating_sub(path.active_latency_sensitive_flows)
                .max(1) as f64;
            rate / active_bulk_flows
        }
        FlowLane::Control
        | FlowLane::Latency
        | FlowLane::RealtimeDatagram
        | FlowLane::Throughput
        | FlowLane::Background => rate,
    }
}

pub(crate) fn path_is_schedulable(path: PathSnapshot, lane: FlowLane) -> bool {
    if matches!(path.state, PathState::Failed | PathState::Draining) {
        return false;
    }
    if path.policy.probe_only && lane != FlowLane::Control {
        return false;
    }
    if lane == FlowLane::Throughput && !path.policy.bulk_allowed {
        return false;
    }
    if lane == FlowLane::RealtimeDatagram && path.policy.no_udp {
        return false;
    }
    true
}

fn suspect_penalty_ms(path: PathSnapshot, lane: FlowLane) -> f64 {
    if prefers_low_reorder(lane) {
        0.0
    } else {
        path_pto_ms(path) * QUIC_PERSISTENT_CONGESTION_THRESHOLD
    }
}

fn prefers_low_reorder(lane: FlowLane) -> bool {
    lane.is_latency_sensitive()
}

fn adaptive_loss_repair_penalty_ms(path: PathSnapshot) -> f64 {
    let loss = path.loss_rate.clamp(0.0, 1.0);
    if loss <= f64::EPSILON {
        return 0.0;
    }
    let denominator_floor = 1.0 / QUIC_INITIAL_WINDOW_PACKETS;
    let expected_repairs = loss / (1.0 - loss).max(denominator_floor);
    expected_repairs * path_pto_ms(path)
}

fn adaptive_low_confidence_penalty_ms(path: PathSnapshot) -> f64 {
    (1.0 - path.confidence.clamp(0.0, 1.0)) * path_pto_ms(path) / QUIC_INITIAL_WINDOW_PACKETS
}

fn adaptive_backup_penalty_ms(path: PathSnapshot) -> f64 {
    path_pto_ms(path)
}

fn adaptive_expensive_path_penalty_ms(path: PathSnapshot, payload_bytes: usize) -> f64 {
    path_pto_ms(path).max(payload_tx_ms(path, payload_bytes))
}

pub(crate) fn path_bdp_bytes(path: PathSnapshot) -> usize {
    ((effective_path_rate_bps(path, FlowLane::Throughput) / 8.0) * (path.srtt_ms.max(1.0) / 1000.0))
        .ceil()
        .max(1.0) as usize
}

pub(crate) fn payload_tx_ms(path: PathSnapshot, payload_bytes: usize) -> f64 {
    payload_bytes as f64 * 8.0 / effective_path_rate_bps(path, FlowLane::Throughput) * 1000.0
}

pub(crate) fn path_pto_ms(path: PathSnapshot) -> f64 {
    let srtt = path.srtt_ms.max(1.0);
    let rttvar = path.jitter_ms.max(srtt / 8.0);
    srtt + (4.0 * rttvar).max(1.0) + srtt.min(QUIC_MAX_ACK_DELAY_MS)
}

#[cfg(test)]
#[path = "policy_test.rs"]
mod tests;
