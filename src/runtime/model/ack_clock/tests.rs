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

#[test]
fn request_calibration_is_a_bounded_seed_below_a_high_bdp_pipe() {
    let mux_limits = MuxLimits::default();
    let service = PathSnapshot::new(PathId(3), UnderlayProtocol::Tcp, 180.0, 500_000_000.0);
    let target = reliable_request_ack_clock_calibration_target_bytes(mux_limits);
    assert!(target <= reliable_ack_clock_calibration_ceiling_bytes(mux_limits));
    assert_eq!(
        target,
        reliable_ack_clock_calibration_limit_bytes(mux_limits)
    );
    assert!(target < bulk_candidate_pipe_bytes(service));
}

#[test]
fn request_calibration_target_is_reachable_with_configured_chunk_geometry() {
    let mux_limits = MuxLimits {
        max_stream_window_bytes: 30_000,
        max_repair_bytes: 30_000,
        max_reorder_bytes: 30_000,
        max_path_flight_bytes: 30_000,
        max_reliable_relay_chunk_bytes: 9_000,
        ..MuxLimits::default()
    };
    let target = reliable_request_ack_clock_calibration_target_bytes(mux_limits);
    let emitted = target.div_ceil(9_000) * 9_000;
    assert!(target > 0);
    assert!(emitted <= reliable_ack_clock_calibration_ceiling_bytes(mux_limits));

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
        reliable_request_ack_clock_calibration_target_bytes(frame_exceeds_ceiling),
        0,
        "request calibration is disabled when one maximum frame cannot fit its resource ceiling"
    );
}
