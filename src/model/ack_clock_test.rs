use super::*;
use crate::model::admission::bulk_candidate_pipe_bytes;
use crate::protocol::{PathId, UnderlayProtocol};
use crate::scheduler::PathSnapshot;

#[test]
fn measurement_never_raises_a_configured_resource_ceiling() {
    let below_sample_floor = MuxLimits {
        max_path_flight_bytes: PATH_OPEN_SCORE_BYTES - 1,
        ..MuxLimits::default()
    };
    assert_eq!(
        reliable_ack_clock_measurement_ceiling_bytes(below_sample_floor),
        0
    );
    assert_eq!(
        reliable_ack_clock_measurement_limit_bytes(below_sample_floor),
        0
    );

    let exact_sample_floor = MuxLimits {
        max_path_flight_bytes: PATH_OPEN_SCORE_BYTES,
        ..MuxLimits::default()
    };
    assert_eq!(
        reliable_ack_clock_measurement_ceiling_bytes(exact_sample_floor),
        PATH_OPEN_SCORE_BYTES as u64
    );
    assert_eq!(
        reliable_ack_clock_measurement_limit_bytes(exact_sample_floor),
        PATH_OPEN_SCORE_BYTES as u64
    );
}

#[test]
fn rate_coverage_geometry_is_shared_by_both_stream_directions() {
    let mux_limits = MuxLimits::default();
    let directional_floor = reliable_data_ack_rate_coverage_floor_bytes(mux_limits);
    assert!(directional_floor >= PATH_OPEN_SCORE_BYTES as u64);
    assert!(directional_floor <= reliable_ack_clock_measurement_limit_bytes(mux_limits));
}

#[test]
fn request_measurement_is_a_bounded_seed_below_a_high_bdp_pipe() {
    let mux_limits = MuxLimits::default();
    let high_bdp_path = PathSnapshot::new(PathId(3), UnderlayProtocol::Tcp, 180.0, 500_000_000.0);
    let target = reliable_request_ack_clock_measurement_target_bytes(mux_limits);
    assert!(target <= reliable_ack_clock_measurement_ceiling_bytes(mux_limits));
    assert_eq!(
        target,
        reliable_ack_clock_measurement_limit_bytes(mux_limits)
    );
    assert!(target < bulk_candidate_pipe_bytes(high_bdp_path));
}

#[test]
fn request_measurement_target_is_reachable_with_configured_chunk_geometry() {
    let mux_limits = MuxLimits {
        max_stream_window_bytes: 30_000,
        max_repair_bytes: 30_000,
        max_reorder_bytes: 30_000,
        max_path_flight_bytes: 30_000,
        max_reliable_relay_chunk_bytes: 9_000,
        ..MuxLimits::default()
    };
    let target = reliable_request_ack_clock_measurement_target_bytes(mux_limits);
    let emitted = target.div_ceil(9_000) * 9_000;
    assert!(target > 0);
    assert!(emitted <= reliable_ack_clock_measurement_ceiling_bytes(mux_limits));

    let frame_exceeds_ceiling = MuxLimits {
        max_repair_bytes: 30_000,
        max_reorder_bytes: 30_000,
        max_stream_window_bytes: 30_000,
        max_reliable_relay_chunk_bytes: 64 * 1024,
        max_payload_bytes: 64 * 1024,
        max_path_flight_bytes: 64 * 1024,
        ..MuxLimits::default()
    };
    assert_eq!(
        reliable_request_ack_clock_measurement_target_bytes(frame_exceeds_ceiling),
        0,
        "request measurement is disabled when one maximum frame cannot fit its resource ceiling"
    );
}
