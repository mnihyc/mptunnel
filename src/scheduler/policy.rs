//! Pure production path eligibility and completion scoring.
//!
//! Formulas consume live evidence and protocol-derived timing; this module owns
//! no queues, flow lifetimes, carrier I/O, or simulator-only heuristics.

use super::TrafficClass;
use crate::model::path::PathPolicy;
use crate::protocol::{PathId, PathUsage, UnderlayProtocol};

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
    /// The receiver's directional preference for data sent by this endpoint.
    /// Local health and endpoint-local policy remain independent inputs.
    pub peer_usage: Option<PathUsage>,
    pub srtt_ms: f64,
    pub jitter_ms: f64,
    pub delivery_rate_bps: f64,
    pub rate_scope: PathRateScope,
    /// Qualified native carrier delivery capacity, when distinct from the
    /// product flow's completion rate.
    pub carrier_delivery_rate_bps: Option<f64>,
    pub product_progress_rate_bps: Option<f64>,
    /// Exact product ACK accounting satisfies the transport-specific durable
    /// sample threshold; a point rate alone is not admission evidence.
    pub has_durable_product_progress: bool,
    pub loss_rate: f64,
    /// Bytes waiting in the carrier-owned writer/socket queue.
    pub queue_bytes: u64,
    /// MPP bytes waiting above the carrier queue.
    pub data_level_queue_bytes: u64,
    /// Bytes reported in flight by the native carrier.
    pub bytes_in_flight: u64,
    /// MPP transmissions awaiting a Data ACK on this path.
    pub data_level_bytes_in_flight: u64,
    pub active_flows: u32,
    pub active_latency_sensitive_flows: u32,
    pub session_active_latency_sensitive_flows: u32,
    pub pacing_rate_bps: f64,
    /// Native carrier congestion-window or inflight credit; zero is unknown.
    pub carrier_inflight_limit_bytes: u64,
    /// Explicit MPP per-path scheduling window; zero requests model derivation.
    pub data_level_limit_bytes: u64,
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
            peer_usage: None,
            srtt_ms,
            jitter_ms: 0.0,
            delivery_rate_bps,
            rate_scope: PathRateScope::PathCapacity,
            carrier_delivery_rate_bps: None,
            product_progress_rate_bps: None,
            has_durable_product_progress: false,
            loss_rate: 0.0,
            queue_bytes: 0,
            data_level_queue_bytes: 0,
            bytes_in_flight: 0,
            data_level_bytes_in_flight: 0,
            active_flows: 0,
            active_latency_sensitive_flows: 0,
            session_active_latency_sensitive_flows: 0,
            pacing_rate_bps: delivery_rate_bps,
            carrier_inflight_limit_bytes: 0,
            data_level_limit_bytes: 0,
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
    lane: TrafficClass,
    payload_bytes: usize,
) -> Option<PathScore> {
    let choose = |allow_backup: bool| {
        paths
            .iter()
            .filter(|path| allow_backup || !path_is_backup(**path))
            .filter_map(|path| score_path(*path, lane, payload_bytes))
            .min_by(|left, right| left.eta_ms.total_cmp(&right.eta_ms))
    };
    choose(false).or_else(|| choose(true))
}

/// MPTCP-style backup preference is directional. Local configuration may be
/// stricter than the peer, so either source can reserve a path as fallback.
pub fn path_is_backup(path: PathSnapshot) -> bool {
    path.policy.backup || path.peer_usage == Some(PathUsage::Backup)
}

pub fn score_path(
    path: PathSnapshot,
    lane: TrafficClass,
    payload_bytes: usize,
) -> Option<PathScore> {
    if !path_is_schedulable(path, lane) {
        return None;
    }

    let rate = effective_path_rate_bps(path, lane);
    let carrier_work = path.queue_bytes.saturating_add(path.bytes_in_flight);
    let data_level_work = path
        .data_level_queue_bytes
        .saturating_add(path.data_level_bytes_in_flight);
    // MPTCP ECF compares the ordered Data Sequence completion frontier, for
    // which native and Data-ACK flight overlap. Independent latency-sensitive
    // work is not behind another flow's Data ACK; it follows only bytes still
    // queued above the carrier plus work owned by the native transport.
    let path_work = match lane {
        TrafficClass::Throughput => carrier_work.max(data_level_work),
        TrafficClass::Control | TrafficClass::RealtimeDatagram | TrafficClass::Latency => {
            path.data_level_queue_bytes.saturating_add(carrier_work)
        }
    };
    let queued_bits = path_work as f64 * 8.0;
    let payload_bits = payload_bytes as f64 * 8.0;

    let mut eta_ms = path.srtt_ms / 2.0;
    eta_ms += queued_bits / rate * 1000.0;
    eta_ms += payload_bits / rate * 1000.0;
    eta_ms += path.jitter_ms;
    eta_ms += adaptive_loss_reinjection_penalty_ms(path);
    eta_ms += adaptive_low_confidence_penalty_ms(path);
    eta_ms += active_flow_penalty_ms(path, lane);

    if path.state == PathState::Suspect {
        eta_ms += suspect_penalty_ms(path, lane);
    }
    if path.policy.expensive {
        eta_ms += adaptive_expensive_path_penalty_ms(path, payload_bytes);
    }
    Some(PathScore {
        path_id: path.id,
        eta_ms,
    })
}

fn active_flow_penalty_ms(path: PathSnapshot, lane: TrafficClass) -> f64 {
    match lane {
        TrafficClass::Throughput => {
            f64::from(path.active_latency_sensitive_flows) * path_pto_ms(path)
        }
        TrafficClass::Control | TrafficClass::RealtimeDatagram | TrafficClass::Latency => {
            f64::from(path.active_flows) * path_pto_ms(path) / QUIC_INITIAL_WINDOW_PACKETS
        }
    }
}

fn effective_path_rate_bps(path: PathSnapshot, lane: TrafficClass) -> f64 {
    let rate = match path.rate_scope {
        PathRateScope::PerFlowGoodput => path.delivery_rate_bps,
        // Completion predicts achieved service. A congestion controller's
        // pacing rate is its current send intent and can transiently exceed the
        // delivered path rate by a large startup gain.
        PathRateScope::PathCapacity => path.delivery_rate_bps,
    }
    .max(1.0);
    match lane {
        TrafficClass::Throughput if matches!(path.rate_scope, PathRateScope::PathCapacity) => {
            let active_bulk_flows = path
                .active_flows
                .saturating_sub(path.active_latency_sensitive_flows)
                .max(1) as f64;
            rate / active_bulk_flows
        }
        TrafficClass::Control
        | TrafficClass::Latency
        | TrafficClass::RealtimeDatagram
        | TrafficClass::Throughput => rate,
    }
}

pub(crate) fn path_is_schedulable(path: PathSnapshot, lane: TrafficClass) -> bool {
    if matches!(path.state, PathState::Failed | PathState::Draining) {
        return false;
    }
    if path.policy.probe_only && lane != TrafficClass::Control {
        return false;
    }
    if lane == TrafficClass::Throughput && !path.policy.bulk_allowed {
        return false;
    }
    if lane == TrafficClass::RealtimeDatagram && path.policy.no_udp {
        return false;
    }
    true
}

fn suspect_penalty_ms(path: PathSnapshot, lane: TrafficClass) -> f64 {
    if prefers_low_reorder(lane) {
        0.0
    } else {
        path_pto_ms(path) * QUIC_PERSISTENT_CONGESTION_THRESHOLD
    }
}

fn prefers_low_reorder(lane: TrafficClass) -> bool {
    lane.is_latency_sensitive()
}

fn adaptive_loss_reinjection_penalty_ms(path: PathSnapshot) -> f64 {
    let loss = path.loss_rate.clamp(0.0, 1.0);
    if loss <= f64::EPSILON {
        return 0.0;
    }
    let denominator_floor = 1.0 / QUIC_INITIAL_WINDOW_PACKETS;
    let expected_reinjections = loss / (1.0 - loss).max(denominator_floor);
    expected_reinjections * path_pto_ms(path)
}

fn adaptive_low_confidence_penalty_ms(path: PathSnapshot) -> f64 {
    (1.0 - path.confidence.clamp(0.0, 1.0)) * path_pto_ms(path) / QUIC_INITIAL_WINDOW_PACKETS
}

fn adaptive_expensive_path_penalty_ms(path: PathSnapshot, payload_bytes: usize) -> f64 {
    path_pto_ms(path).max(payload_tx_ms(path, payload_bytes))
}

pub(crate) fn path_bdp_bytes(path: PathSnapshot) -> usize {
    ((effective_path_rate_bps(path, TrafficClass::Throughput) / 8.0)
        * (path.srtt_ms.max(1.0) / 1000.0))
        .ceil()
        .max(1.0) as usize
}

pub(crate) fn payload_tx_ms(path: PathSnapshot, payload_bytes: usize) -> f64 {
    payload_bytes as f64 * 8.0 / effective_path_rate_bps(path, TrafficClass::Throughput) * 1000.0
}

pub(crate) fn path_pto_ms(path: PathSnapshot) -> f64 {
    let srtt = path.srtt_ms.max(1.0);
    let rttvar = path.jitter_ms.max(srtt / 8.0);
    srtt + (4.0 * rttvar).max(1.0) + srtt.min(QUIC_MAX_ACK_DELAY_MS)
}

#[cfg(test)]
#[path = "tests_policy.rs"]
mod tests;
