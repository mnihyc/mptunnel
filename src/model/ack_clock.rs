//! TCP product-ACK calibration policy.
//!
//! This model provides bounded product evidence when native TCP telemetry is
//! unavailable. QUIC packet ACKs remain carrier-owned and never enter here.

use super::admission::{
    BulkExplorationCompletionProjection, bulk_candidate_pipe_bytes,
    bulk_service_horizon_payload_bytes, bulk_tcp_calibration_completion_projection,
};
use crate::model::capacity::{BBR_MAX_SEND_QUANTUM_BYTES, PATH_OPEN_SCORE_BYTES};
use crate::mux::MuxLimits;
use crate::protocol::UnderlayProtocol;
use crate::scheduler::PathSnapshot;

#[derive(Debug, Clone, Copy)]
#[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
pub(crate) enum TcpAckClockCalibrationOpportunity {
    Admit(BulkExplorationCompletionProjection),
    Retire(BulkExplorationCompletionProjection),
}

impl TcpAckClockCalibrationOpportunity {
    #[cfg(feature = "lab-diagnostics")]
    pub(crate) fn projection(self) -> BulkExplorationCompletionProjection {
        match self {
            Self::Admit(projection) | Self::Retire(projection) => projection,
        }
    }

    pub(crate) fn is_admitted(self) -> bool {
        matches!(self, Self::Admit(_))
    }
}

/// Decide whether a fresh, zero-spend TCP stage can finish before Service
/// consumes the ordered reservoir. Begun stages are deliberately not reevaluated.
pub(crate) fn reliable_tcp_ack_clock_calibration_opportunity(
    service_snapshot: PathSnapshot,
    service_eta_ms: f64,
    candidate_snapshot: PathSnapshot,
    candidate_eta_ms: f64,
    authorized_bytes: u64,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> TcpAckClockCalibrationOpportunity {
    debug_assert_eq!(service_snapshot.underlay, UnderlayProtocol::Tcp);
    debug_assert_eq!(candidate_snapshot.underlay, UnderlayProtocol::Tcp);
    let projection = bulk_tcp_calibration_completion_projection(
        service_snapshot,
        service_eta_ms,
        candidate_snapshot,
        candidate_eta_ms,
        authorized_bytes,
        payload_bytes,
        mux_limits,
    );
    if projection.completes_within_service_reservoir() {
        TcpAckClockCalibrationOpportunity::Admit(projection)
    } else {
        TcpAckClockCalibrationOpportunity::Retire(projection)
    }
}

pub(crate) fn reliable_ack_clock_calibration_limit_bytes(mux_limits: MuxLimits) -> u64 {
    let resource_ceiling = reliable_ack_clock_calibration_ceiling_bytes(mux_limits);
    if resource_ceiling == 0 {
        return 0;
    }
    let service_horizon =
        bulk_service_horizon_payload_bytes(BBR_MAX_SEND_QUANTUM_BYTES, mux_limits) as u64;
    service_horizon.min(resource_ceiling)
}

pub(crate) fn reliable_ack_clock_calibration_rate_coverage_floor_bytes(
    mux_limits: MuxLimits,
) -> u64 {
    let product_limit = reliable_ack_clock_calibration_limit_bytes(mux_limits);
    if product_limit == 0 {
        return 0;
    }
    product_limit
        .div_ceil(2)
        .max(PATH_OPEN_SCORE_BYTES as u64)
        .min(product_limit)
}

pub(crate) fn reliable_request_ack_clock_calibration_target_bytes(mux_limits: MuxLimits) -> u64 {
    let base = reliable_ack_clock_calibration_limit_bytes(mux_limits);
    let ceiling = reliable_ack_clock_calibration_ceiling_bytes(mux_limits);
    if base == 0 {
        return 0;
    }
    // This bounded epoch proves causal request ownership; it is not a second
    // congestion controller and must not serialize an entire high-BDP pipe.
    // Continuous exact samples mature after admission. Reserve one maximum
    // frame so non-divisible configured geometry can cross the target.
    let max_payload = BBR_MAX_SEND_QUANTUM_BYTES
        .min(mux_limits.max_reliable_relay_chunk_bytes)
        .min(mux_limits.max_payload_bytes)
        .min(mux_limits.max_path_flight_bytes)
        .max(1) as u64;
    if max_payload > ceiling {
        return 0;
    }
    let reachable_target = ceiling.saturating_sub(max_payload);
    if reachable_target == 0 {
        base.min(ceiling)
    } else {
        base.min(reachable_target)
    }
}

pub(crate) fn reliable_tcp_ack_clock_calibration_initial_limit_bytes(
    candidate: PathSnapshot,
    mux_limits: MuxLimits,
) -> u64 {
    let product_limit = reliable_ack_clock_calibration_limit_bytes(mux_limits);
    if product_limit == 0 {
        return 0;
    }

    // The first product stage must fit the candidate TCP pipe. This is product
    // measurement policy above kernel TCP, not a replacement congestion window.
    bulk_candidate_pipe_bytes(candidate)
        .max(BBR_MAX_SEND_QUANTUM_BYTES as u64)
        .min(product_limit)
}

pub(crate) fn reliable_ack_clock_calibration_ceiling_bytes(mux_limits: MuxLimits) -> u64 {
    let resource_ceiling = (mux_limits.max_path_flight_bytes as u64)
        .min(mux_limits.max_repair_bytes as u64)
        .min(mux_limits.max_reorder_bytes as u64)
        .min(mux_limits.max_stream_window_bytes);
    if resource_ceiling < PATH_OPEN_SCORE_BYTES as u64 {
        0
    } else {
        resource_ceiling
    }
}

#[cfg(test)]
mod tests;
