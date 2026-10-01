//! Pure production path eligibility and completion scoring.
//!
//! Formulas consume live evidence and protocol-derived timing; this module owns
//! no queues, flow lifetimes, carrier I/O, or simulator-only heuristics.

use super::{TrafficClass, snapshot::CompletionPath};
use crate::protocol::{PathId, PathUsage};

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

/// Retains a current lead when the challenger does not clear measured timing
/// and one scheduling quantum of queue uncertainty.
pub(super) fn path_within_adaptive_lead_hysteresis(
    old_eta_ms: f64,
    old_snapshot: CompletionPath<'_>,
    best_eta_ms: f64,
    best_snapshot: CompletionPath<'_>,
    payload_bytes: usize,
) -> bool {
    let jitter_hysteresis_ms = old_snapshot
        .timing()
        .rttvar_ms()
        .max(best_snapshot.timing().rttvar_ms());
    let queue_hysteresis_bytes = payload_bytes as u64;
    old_eta_ms <= best_eta_ms + jitter_hysteresis_ms
        && old_snapshot.queue_bytes() <= best_snapshot.queue_bytes() + queue_hysteresis_bytes
}

/// Whether one candidate has a completion lead beyond measured timing
/// uncertainty. Both ETAs already include carrier/Product flight, queue, and
/// the next scheduling quantum, so raw queue bytes must not be counted again.
pub(super) fn path_has_material_completion_advantage(
    candidate_eta_ms: f64,
    candidate_snapshot: CompletionPath<'_>,
    available_eta_ms: f64,
    available_snapshot: CompletionPath<'_>,
) -> bool {
    let jitter_hysteresis_ms = candidate_snapshot
        .timing()
        .rttvar_ms()
        .max(available_snapshot.timing().rttvar_ms());
    candidate_eta_ms + jitter_hysteresis_ms < available_eta_ms
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PathScore {
    pub path_id: PathId,
    pub eta_ms: f64,
}

/// MPTCP-style backup preference is directional. Local configuration may be
/// stricter than the peer, so either source can reserve a path as fallback.
pub(super) fn path_is_backup(path: CompletionPath<'_>) -> bool {
    path.policy().backup || path.peer_usage() == Some(PathUsage::Backup)
}

pub(super) fn score_path(
    path: CompletionPath<'_>,
    lane: TrafficClass,
    payload_bytes: usize,
) -> Option<PathScore> {
    if !path_is_schedulable(path, lane) {
        return None;
    }

    let rate = effective_path_rate_bps(path, lane);
    let carrier_work = path.queue_bytes().saturating_add(path.bytes_in_flight());
    let data_level_work = path
        .data_level_queue_bytes()
        .saturating_add(path.data_level_bytes_in_flight());
    // MPTCP ECF compares the ordered Data Sequence completion frontier, for
    // which native and Data-ACK flight overlap. Independent latency-sensitive
    // work is not behind another flow's Data ACK; it follows only bytes still
    // queued above the carrier plus work owned by the native transport.
    let path_work = match lane {
        TrafficClass::Throughput => carrier_work.max(data_level_work),
        TrafficClass::Control | TrafficClass::RealtimeDatagram | TrafficClass::Latency => {
            path.data_level_queue_bytes().saturating_add(carrier_work)
        }
    };
    let queued_bits = path_work as f64 * 8.0;
    let payload_bits = payload_bytes as f64 * 8.0;

    let mut eta_ms = path.timing().srtt_ms() / 2.0;
    eta_ms += queued_bits / rate * 1000.0;
    eta_ms += payload_bits / rate * 1000.0;
    eta_ms += path.timing().rttvar_ms();
    eta_ms += adaptive_loss_reinjection_penalty_ms(path);
    eta_ms += adaptive_low_confidence_penalty_ms(path);
    eta_ms += active_flow_penalty_ms(path, lane);

    if path.state() == PathState::Suspect {
        eta_ms += suspect_penalty_ms(path, lane);
    }
    if path.policy().expensive {
        eta_ms += adaptive_expensive_path_penalty_ms(path, payload_bytes);
    }
    Some(PathScore {
        path_id: path.id(),
        eta_ms,
    })
}

fn active_flow_penalty_ms(path: CompletionPath<'_>, lane: TrafficClass) -> f64 {
    match lane {
        TrafficClass::Throughput => {
            f64::from(path.active_latency_sensitive_flows()) * path_pto_ms(path)
        }
        TrafficClass::Control | TrafficClass::RealtimeDatagram | TrafficClass::Latency => {
            f64::from(path.active_flows()) * path_pto_ms(path) / QUIC_INITIAL_WINDOW_PACKETS
        }
    }
}

pub(super) fn effective_path_rate_bps(path: CompletionPath<'_>, lane: TrafficClass) -> f64 {
    let rate = match path.rate_scope() {
        PathRateScope::PerFlowGoodput => path.delivery_rate_bps(),
        // Completion predicts achieved service. A congestion controller's
        // pacing rate is its current send intent and can transiently exceed the
        // delivered path rate by a large startup gain.
        PathRateScope::PathCapacity => path.delivery_rate_bps(),
    }
    .max(1.0);
    match lane {
        TrafficClass::Throughput if matches!(path.rate_scope(), PathRateScope::PathCapacity) => {
            let active_bulk_flows = path
                .active_flows()
                .saturating_sub(path.active_latency_sensitive_flows())
                .max(1) as f64;
            rate / active_bulk_flows
        }
        TrafficClass::Control
        | TrafficClass::Latency
        | TrafficClass::RealtimeDatagram
        | TrafficClass::Throughput => rate,
    }
}

pub(super) fn path_is_schedulable(path: CompletionPath<'_>, lane: TrafficClass) -> bool {
    if matches!(path.state(), PathState::Failed | PathState::Draining) {
        return false;
    }
    if path.policy().probe_only && lane != TrafficClass::Control {
        return false;
    }
    if lane == TrafficClass::Throughput && !path.policy().bulk_allowed {
        return false;
    }
    if lane == TrafficClass::RealtimeDatagram && path.policy().no_udp {
        return false;
    }
    true
}

fn suspect_penalty_ms(path: CompletionPath<'_>, lane: TrafficClass) -> f64 {
    if prefers_low_reorder(lane) {
        0.0
    } else {
        path_pto_ms(path) * QUIC_PERSISTENT_CONGESTION_THRESHOLD
    }
}

fn prefers_low_reorder(lane: TrafficClass) -> bool {
    lane.is_latency_sensitive()
}

fn adaptive_loss_reinjection_penalty_ms(path: CompletionPath<'_>) -> f64 {
    let loss = path.loss_rate().clamp(0.0, 1.0);
    if loss <= f64::EPSILON {
        return 0.0;
    }
    let denominator_floor = 1.0 / QUIC_INITIAL_WINDOW_PACKETS;
    let expected_reinjections = loss / (1.0 - loss).max(denominator_floor);
    expected_reinjections * path_pto_ms(path)
}

fn adaptive_low_confidence_penalty_ms(path: CompletionPath<'_>) -> f64 {
    (1.0 - path.confidence().clamp(0.0, 1.0)) * path_pto_ms(path) / QUIC_INITIAL_WINDOW_PACKETS
}

fn adaptive_expensive_path_penalty_ms(path: CompletionPath<'_>, payload_bytes: usize) -> f64 {
    path_pto_ms(path).max(payload_tx_ms(path, payload_bytes))
}

pub(super) fn payload_tx_ms(path: CompletionPath<'_>, payload_bytes: usize) -> f64 {
    payload_bytes as f64 * 8.0 / effective_path_rate_bps(path, TrafficClass::Throughput) * 1000.0
}

pub(super) fn path_pto_ms(path: CompletionPath<'_>) -> f64 {
    let srtt = path.timing().srtt_ms().max(1.0);
    let rttvar = path.timing().rttvar_ms().max(srtt / 8.0);
    srtt + (4.0 * rttvar).max(1.0) + srtt.min(QUIC_MAX_ACK_DELAY_MS)
}
