use super::bulk_admission::{
    BulkExplorationCompletionProjection, bulk_candidate_pipe_bytes,
    bulk_exploration_completion_projection, bulk_service_horizon_payload_bytes,
};
use super::prelude::*;
use super::{BBR_MAX_SEND_QUANTUM_BYTES, PATH_OPEN_SCORE_BYTES};

// Product ACK timing is a TCP-carrier fallback for request and response path
// measurement. QUIC packet ACKs remain below this module and continue to own
// UDP carrier congestion, pacing, and delivery-rate evidence.

#[derive(Debug, Clone, Copy)]
#[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
pub(super) enum TcpAckClockCalibrationOpportunity {
    Admit(BulkExplorationCompletionProjection),
    Retire(BulkExplorationCompletionProjection),
}

impl TcpAckClockCalibrationOpportunity {
    #[cfg(feature = "lab-diagnostics")]
    pub(super) fn projection(self) -> BulkExplorationCompletionProjection {
        match self {
            Self::Admit(projection) | Self::Retire(projection) => projection,
        }
    }

    pub(super) fn is_admitted(self) -> bool {
        matches!(self, Self::Admit(_))
    }
}

/// Decide whether a fresh, zero-spend TCP stage can finish before Service
/// consumes the ordered reservoir. Begun stages are deliberately not reevaluated.
pub(super) fn reliable_tcp_ack_clock_calibration_opportunity(
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
    let projection = bulk_exploration_completion_projection(
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

pub(super) fn reliable_ack_clock_calibration_limit_bytes(mux_limits: MuxLimits) -> u64 {
    let resource_ceiling = reliable_ack_clock_calibration_ceiling_bytes(mux_limits);
    if resource_ceiling == 0 {
        return 0;
    }
    let service_horizon =
        bulk_service_horizon_payload_bytes(BBR_MAX_SEND_QUANTUM_BYTES, mux_limits) as u64;
    service_horizon.min(resource_ceiling)
}

pub(super) fn reliable_ack_clock_calibration_rate_coverage_floor_bytes(
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

pub(super) fn reliable_tcp_ack_clock_calibration_initial_limit_bytes(
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

pub(super) fn reliable_ack_clock_calibration_ceiling_bytes(mux_limits: MuxLimits) -> u64 {
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
mod tests {
    use super::*;

    #[test]
    fn calibration_never_raises_a_configured_resource_ceiling() {
        let below_sample_floor = MuxLimits {
            max_path_flight_bytes: PATH_OPEN_SCORE_BYTES - 1,
            ..MuxLimits::default()
        };
        assert_eq!(
            reliable_ack_clock_calibration_ceiling_bytes(below_sample_floor),
            0
        );
        assert_eq!(
            reliable_ack_clock_calibration_limit_bytes(below_sample_floor),
            0
        );

        let exact_sample_floor = MuxLimits {
            max_path_flight_bytes: PATH_OPEN_SCORE_BYTES,
            ..MuxLimits::default()
        };
        assert_eq!(
            reliable_ack_clock_calibration_ceiling_bytes(exact_sample_floor),
            PATH_OPEN_SCORE_BYTES as u64
        );
        assert_eq!(
            reliable_ack_clock_calibration_limit_bytes(exact_sample_floor),
            PATH_OPEN_SCORE_BYTES as u64
        );
    }

    #[test]
    fn tcp_calibration_starts_with_one_candidate_pipe() {
        let mux_limits = MuxLimits::default();
        let product_limit = reliable_ack_clock_calibration_limit_bytes(mux_limits);
        let slow = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 4_605.0, 146_000.0);
        let fast = PathSnapshot::new(PathId(2), UnderlayProtocol::Tcp, 436.0, 46_000_000.0);

        let slow_limit = reliable_tcp_ack_clock_calibration_initial_limit_bytes(slow, mux_limits);
        assert!(slow_limit >= BBR_MAX_SEND_QUANTUM_BYTES as u64);
        assert!(slow_limit < product_limit);
        assert!(
            reliable_ack_clock_calibration_rate_coverage_floor_bytes(mux_limits) > slow_limit,
            "a path-sized seed must not lower the independent publication floor"
        );
        assert_eq!(
            reliable_tcp_ack_clock_calibration_initial_limit_bytes(fast, mux_limits),
            product_limit
        );
    }
}
