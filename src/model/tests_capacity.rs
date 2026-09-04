use super::{
    MAX_RELIABLE_SERVICE_QUANTUM_BYTES, PATH_OPEN_SCORE_BYTES, RELIABLE_INITIAL_RTT,
    ReliableOriginalDataOutput, TcpCapacityProofCandidate, adaptive_reliable_relay_chunk_bytes,
    adaptive_reliable_relay_chunk_bytes_with_frame_limit, reliable_bulk_carrier_feed_quantum_bytes,
    reliable_bulk_product_windows, reliable_bulk_unproven_exploration_limit_bytes,
    reliable_product_feedback_window_bytes, reliable_product_measurement_session_envelope_bytes,
    reliable_product_recovery_window_bytes, reliable_relay_buffer_len,
    reliable_relay_scheduler_quantum_cap, reliable_relay_sender_dispatch_budget,
    reliable_stream_ack_update_bytes, reliable_stream_advertised_window_bytes,
    reliable_stream_initial_advertised_window_bytes, reliable_stream_max_data_update_bytes,
    reliable_stream_source_admission, reliable_unproven_path_startup_flight_limit_bytes,
    valid_tcp_capacity_proof_candidate_at,
};
use crate::mux::MuxLimits;
use crate::protocol::codec::CodecLimits;
use crate::protocol::{PathId, UnderlayProtocol};
use crate::scheduler::{PathSnapshot, TrafficClass};
use std::time::{Duration, Instant};

#[test]
fn bulk_product_authority_is_the_resource_envelope_not_a_native_feedback_clock() {
    let mux_limits = MuxLimits::default();
    let expected = reliable_product_measurement_session_envelope_bytes(mux_limits) as usize;
    let mut constrained = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 100.0, 5_242_880.0);
    constrained.carrier_inflight_limit_bytes = 64 * 1024;
    constrained.carrier_delivery_rate_bps = Some(5_242_880.0);
    constrained.product_progress_rate_bps = Some(5_242_880.0);
    constrained.has_durable_product_progress = true;

    let constrained_product = reliable_product_feedback_window_bytes(
        Some(constrained),
        TrafficClass::Throughput,
        mux_limits,
    );

    let mut recovered = constrained;
    recovered.carrier_inflight_limit_bytes = 6_250_000;
    recovered.carrier_delivery_rate_bps = Some(500_000_000.0);
    recovered.product_progress_rate_bps = Some(500_000_000.0);
    let recovered_product = reliable_product_feedback_window_bytes(
        Some(recovered),
        TrafficClass::Throughput,
        mux_limits,
    );

    assert_eq!(constrained_product, expected);
    assert_eq!(recovered_product, expected);
    assert_eq!(
        constrained_product, recovered_product,
        "native C/R may rank and pace a carrier, but cannot revoke or grant stream Product bytes",
    );
}

#[test]
fn custom_low_limits_define_exact_w_p_e_geometry_and_source_aggregation() {
    let mux_limits = MuxLimits {
        max_stream_window_bytes: 12 * 1024,
        max_repair_bytes: 10 * 1024,
        max_reorder_bytes: 8 * 1024,
        max_path_flight_bytes: 4 * 1024,
        max_reliable_relay_chunk_bytes: 1024,
        max_payload_bytes: 1024,
        ..MuxLimits::default()
    };
    let windows = reliable_bulk_product_windows(mux_limits);
    assert_eq!(windows.stream_resource_limit_bytes, 8 * 1024);
    assert_eq!(windows.per_output_product_limit_bytes, 4 * 1024);

    let mut path = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 100.0, 1_000_000.0);
    assert_eq!(
        reliable_bulk_unproven_exploration_limit_bytes(path, mux_limits),
        4 * 1024,
        "E is always clamped by custom-low P",
    );
    path.data_level_limit_bytes = windows.per_output_product_limit_bytes;
    let output = ReliableOriginalDataOutput {
        snapshot: path,
        stale: false,
    };
    for (outputs, expected) in [
        (vec![output], 4 * 1024),
        (vec![output, output], 8 * 1024),
        (vec![output, output, output], 8 * 1024),
    ] {
        assert_eq!(
            reliable_stream_source_admission(outputs, TrafficClass::Throughput, 1024, mux_limits,)
                .window_bytes,
            expected,
            "the chosen source tier sums exact P and caps the sum at W",
        );
    }
}

#[test]
fn data_level_budgets_expand_for_bulk_without_second_congestion_feedback() {
    let mux_limits = MuxLimits {
        max_reliable_relay_chunk_bytes: 1024 * 1024,
        ..MuxLimits::default()
    };
    let stable = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 120.0, 300_000_000.0);
    let mut unstable = stable;
    unstable.loss_rate = 0.25;
    unstable.jitter_ms = 120.0;
    unstable.queue_bytes = 8 * 1024 * 1024;

    let interactive_chunk =
        adaptive_reliable_relay_chunk_bytes(Some(stable), TrafficClass::Latency, mux_limits);
    let bulk_chunk =
        adaptive_reliable_relay_chunk_bytes(Some(stable), TrafficClass::Throughput, mux_limits);
    let unstable_bulk_chunk =
        adaptive_reliable_relay_chunk_bytes(Some(unstable), TrafficClass::Throughput, mux_limits);
    assert!(bulk_chunk > interactive_chunk);
    assert_eq!(
        unstable_bulk_chunk, bulk_chunk,
        "TCP bulk congestion is governed by kernel backpressure; the MPP record quantum remains a bounded carrier feed unit"
    );

    let interactive_inflight =
        reliable_product_feedback_window_bytes(Some(stable), TrafficClass::Latency, mux_limits);
    let bulk_inflight =
        reliable_product_feedback_window_bytes(Some(stable), TrafficClass::Throughput, mux_limits);
    let mut stable_with_flight = stable;
    stable_with_flight.bytes_in_flight =
        ((stable.delivery_rate_bps / 8.0) * (stable.srtt_ms / 1000.0)).ceil() as u64;
    let bulk_inflight_with_flight = reliable_product_feedback_window_bytes(
        Some(stable_with_flight),
        TrafficClass::Throughput,
        mux_limits,
    );
    let unstable_bulk_inflight = reliable_product_feedback_window_bytes(
        Some(unstable),
        TrafficClass::Throughput,
        mux_limits,
    );
    assert_eq!(
        bulk_inflight, interactive_inflight,
        "P is one configured Product resource envelope; traffic class changes service arbitration and quantum, not receive authority"
    );
    assert_eq!(
        bulk_inflight_with_flight, bulk_inflight,
        "in-flight bytes are the controlled BDP-scale flight, not queue pressure"
    );
    assert_eq!(
        unstable_bulk_inflight, bulk_inflight,
        "loss, jitter, and carrier backlog remain native congestion signals; MPP does not multiply them into a second flight controller"
    );
}

#[test]
fn reliable_bulk_quantum_keeps_tcp_and_quic_streams_fed_without_rate_prior() {
    let mux_limits = MuxLimits::default();
    let srtt_ms = RELIABLE_INITIAL_RTT.as_secs_f64() * 1000.0;
    let rate_bps = PATH_OPEN_SCORE_BYTES as f64 * 8.0 / RELIABLE_INITIAL_RTT.as_secs_f64();
    let unknown_tcp = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, srtt_ms, rate_bps);
    let unknown_udp = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, srtt_ms, rate_bps);

    assert_eq!(
        adaptive_reliable_relay_chunk_bytes(
            Some(unknown_tcp),
            TrafficClass::Throughput,
            mux_limits
        ),
        MAX_RELIABLE_SERVICE_QUANTUM_BYTES.min(reliable_relay_buffer_len(mux_limits))
    );
    assert_eq!(
        adaptive_reliable_relay_chunk_bytes(
            Some(unknown_udp),
            TrafficClass::Throughput,
            mux_limits
        ),
        MAX_RELIABLE_SERVICE_QUANTUM_BYTES.min(reliable_relay_buffer_len(mux_limits)),
        "QUIC packet pacing is below the product sender; reliable UDP bulk must not self-limit to a 2*MSS product record"
    );
    assert_eq!(
        adaptive_reliable_relay_chunk_bytes(Some(unknown_tcp), TrafficClass::Latency, mux_limits),
        PATH_OPEN_SCORE_BYTES
    );
    assert_eq!(
        adaptive_reliable_relay_chunk_bytes(Some(unknown_udp), TrafficClass::Latency, mux_limits),
        PATH_OPEN_SCORE_BYTES
    );
}

#[test]
fn unknown_path_product_window_uses_configured_p_not_an_inferred_bdp() {
    let mux_limits = MuxLimits::default();
    let startup =
        reliable_product_feedback_window_bytes(None, TrafficClass::Throughput, mux_limits);
    assert_eq!(
        startup,
        usize::try_from(reliable_bulk_product_windows(mux_limits).per_output_product_limit_bytes)
            .expect("default Product window fits usize"),
        "P is configured resource authority even before a path observation; bounded startup exploration E is modeled separately"
    );
}

#[test]
fn exact_bulk_output_uses_product_p_while_unproven_exploration_stays_bounded() {
    let mux_limits = MuxLimits::default();
    let path = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 360.0, 3_200_000.0);

    let inflight =
        reliable_product_feedback_window_bytes(Some(path), TrafficClass::Throughput, mux_limits);

    assert_eq!(
        inflight,
        reliable_bulk_product_windows(mux_limits).per_output_product_limit_bytes as usize,
    );
    assert_eq!(
        reliable_bulk_unproven_exploration_limit_bytes(path, mux_limits),
        reliable_unproven_path_startup_flight_limit_bytes(mux_limits),
        "the separate unproven-additional-path envelope E retains portable startup",
    );
}

#[test]
fn durable_product_proof_without_native_shape_uses_configured_writer_bounded_window() {
    let mux_limits = MuxLimits::default();
    for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
        let mut path = PathSnapshot::new(PathId(0), underlay, 100.0, 10_000_000.0);
        path.product_progress_rate_bps = Some(10_000_000.0);
        path.has_durable_product_progress = true;
        path.app_limited = true;

        let constrained = reliable_product_feedback_window_bytes(
            Some(path),
            TrafficClass::Throughput,
            mux_limits,
        );
        path.product_progress_rate_bps = Some(500_000_000.0);
        let recovered = reliable_product_feedback_window_bytes(
            Some(path),
            TrafficClass::Throughput,
            mux_limits,
        );

        assert_eq!(constrained, mux_limits.max_path_flight_bytes);
        assert_eq!(recovered, constrained);
    }
}

#[test]
fn reliable_product_assignment_authority_is_class_and_underlay_independent() {
    let mux_limits = MuxLimits::default();
    let expected = reliable_bulk_product_windows(mux_limits).per_output_product_limit_bytes;
    for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
        let path = PathSnapshot::new(PathId(0), underlay, 100.0, 500_000_000.0);
        let latency =
            reliable_product_feedback_window_bytes(Some(path), TrafficClass::Latency, mux_limits);
        let throughput = reliable_product_feedback_window_bytes(
            Some(path),
            TrafficClass::Throughput,
            mux_limits,
        );

        assert_eq!(latency as u64, expected);
        assert_eq!(throughput as u64, expected);
    }
}

#[test]
fn quic_product_window_is_independent_of_native_flight_and_feedback_delay() {
    let mux_limits = MuxLimits::default();
    let mut path = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 100.0, 500_000_000.0);
    path.carrier_inflight_limit_bytes = 6_250_000;
    path.carrier_delivery_rate_bps = Some(500_000_000.0);
    path.data_level_bytes_in_flight = 32 * 1024 * 1024;

    let inflight =
        reliable_product_feedback_window_bytes(Some(path), TrafficClass::Throughput, mux_limits);

    let expected = reliable_bulk_product_windows(mux_limits).per_output_product_limit_bytes;
    assert_eq!(inflight, expected as usize);
    assert!(inflight > path.carrier_inflight_limit_bytes as usize);
    assert_eq!(inflight, mux_limits.max_path_flight_bytes);
    path.data_level_limit_bytes = inflight as u64;
    let source = reliable_stream_source_admission(
        [ReliableOriginalDataOutput {
            snapshot: path,
            stale: false,
        }],
        TrafficClass::Throughput,
        PATH_OPEN_SCORE_BYTES,
        mux_limits,
    );
    assert_eq!(
        source.window_bytes, inflight,
        "source staging uses P while later output assignment independently revalidates N",
    );
}

#[test]
fn quic_native_window_downshift_does_not_revoke_product_p() {
    let mux_limits = MuxLimits::default();
    let mut path = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 100.0, 500_000_000.0);
    path.carrier_delivery_rate_bps = Some(500_000_000.0);
    path.carrier_inflight_limit_bytes = 1024 * 1024;

    assert_eq!(
        reliable_bulk_unproven_exploration_limit_bytes(path, mux_limits),
        1_114_112,
        "fresh exact C plus the bounded acquisition quantum shapes E",
    );
    assert_eq!(
        reliable_product_feedback_window_bytes(Some(path), TrafficClass::Throughput, mux_limits,),
        reliable_bulk_product_windows(mux_limits).per_output_product_limit_bytes as usize,
        "native C owns writer admission; it is not authority to revoke Product P",
    );
}

#[test]
fn native_rate_changes_do_not_change_product_p() {
    let mux_limits = MuxLimits::default();
    let mut path = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 100.0, 10_000_000.0);
    path.carrier_inflight_limit_bytes = 1024 * 1024;
    path.product_progress_rate_bps = Some(10_000_000.0);
    path.has_durable_product_progress = true;
    let exploration = reliable_bulk_unproven_exploration_limit_bytes(path, mux_limits);

    let low_rate =
        reliable_product_feedback_window_bytes(Some(path), TrafficClass::Throughput, mux_limits);
    assert!(
        low_rate as u64 >= exploration,
        "bounded C-derived acquisition remains inside Product P"
    );

    path.product_progress_rate_bps = Some(500_000_000.0);
    let newer_high_rate =
        reliable_product_feedback_window_bytes(Some(path), TrafficClass::Throughput, mux_limits);
    assert_eq!(newer_high_rate, low_rate);
    assert_eq!(
        newer_high_rate,
        reliable_bulk_product_windows(mux_limits).per_output_product_limit_bytes as usize,
    );

    path.data_level_limit_bytes = newer_high_rate as u64;
    let source = reliable_stream_source_admission(
        [ReliableOriginalDataOutput {
            snapshot: path,
            stale: false,
        }],
        TrafficClass::Throughput,
        PATH_OPEN_SCORE_BYTES,
        mux_limits,
    );
    assert_eq!(
        source.window_bytes, newer_high_rate,
        "source staging consumes the exact runtime-published Product authority",
    );
}

#[test]
fn exact_output_without_published_product_authority_fails_closed() {
    let mux_limits = MuxLimits::default();
    let mut path = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 100.0, 500_000_000.0);
    path.carrier_inflight_limit_bytes = 6_250_000;
    path.carrier_delivery_rate_bps = Some(500_000_000.0);
    path.product_progress_rate_bps = Some(500_000_000.0);
    path.has_durable_product_progress = true;
    path.data_level_limit_bytes = 0;

    let source = reliable_stream_source_admission(
        [ReliableOriginalDataOutput {
            snapshot: path,
            stale: false,
        }],
        TrafficClass::Throughput,
        PATH_OPEN_SCORE_BYTES,
        mux_limits,
    );
    assert_eq!(
        source.window_bytes, 0,
        "an exact consumer cannot reconstruct timestamp-less Product authority when its owner publishes P=0",
    );
    assert_eq!(
        reliable_product_recovery_window_bytes(Some(path), TrafficClass::Throughput, mux_limits,),
        0,
        "recovery consumes the same published Product authority and must also fail closed",
    );
}

#[test]
fn unqualified_pacing_cannot_enlarge_native_or_exploration_authority() {
    let mux_limits = MuxLimits::default();
    let mut path = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 100.0, 500_000_000.0);
    path.pacing_rate_bps = 900_000_000.0;
    path.carrier_inflight_limit_bytes = 1024 * 1024;
    path.app_limited = false;

    let expected_exploration = path
        .carrier_inflight_limit_bytes
        .saturating_add(reliable_bulk_carrier_feed_quantum_bytes(mux_limits) as u64);
    let product =
        reliable_product_feedback_window_bytes(Some(path), TrafficClass::Throughput, mux_limits);
    assert_eq!(
        product,
        reliable_bulk_product_windows(mux_limits).per_output_product_limit_bytes as usize,
    );
    assert_eq!(
        reliable_bulk_unproven_exploration_limit_bytes(path, mux_limits),
        expected_exploration,
        "E may use fresh C+Q, but pacing intent and app-limited state cannot enlarge it",
    );
}

#[test]
fn tcp_native_cwnd_shrink_retains_debt_without_renewing_forward_credit() {
    let mux_limits = MuxLimits::default();
    let mut path = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 80.0, 4_000_000_000.0);
    path.carrier_inflight_limit_bytes = 1024 * 1024;
    path.data_level_bytes_in_flight = 8 * 1024 * 1024;
    let forward_ceiling = path.carrier_inflight_limit_bytes as usize
        + reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    path.data_level_limit_bytes = forward_ceiling as u64;

    assert_eq!(
        reliable_product_feedback_window_bytes(Some(path), TrafficClass::Throughput, mux_limits,),
        reliable_bulk_product_windows(mux_limits).per_output_product_limit_bytes as usize,
        "a native cwnd shrink does not rewrite configured Product P",
    );
    assert_eq!(
        reliable_product_recovery_window_bytes(Some(path), TrafficClass::Throughput, mux_limits,),
        forward_ceiling,
        "retained debt cannot enlarge the exact target Product envelope; K supplies only its separately bounded emergency quantum",
    );
}

#[test]
fn product_progress_does_not_downshift_source_read_below_carrier_evidence() {
    let mux_limits = MuxLimits::default();
    let mut path = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 80.0, 4_000_000_000.0);
    path.pacing_rate_bps = 4_000_000_000.0;
    path.carrier_inflight_limit_bytes = mux_limits.max_path_flight_bytes as u64;
    path.product_progress_rate_bps = Some(160_000_000.0);

    let inflight =
        reliable_product_feedback_window_bytes(Some(path), TrafficClass::Throughput, mux_limits);

    assert_eq!(inflight, mux_limits.max_path_flight_bytes);
    assert_eq!(
        path.delivery_rate_bps, 4_000_000_000.0,
        "carrier rate remains carrier evidence; product progress is a separate field"
    );
}

#[test]
fn quic_product_p_and_native_exploration_authorities_remain_separate() {
    let mux_limits = MuxLimits::default();
    let mut path = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 425.0, 25_000_000.0);
    path.pacing_rate_bps = 175_000_000.0;
    path.carrier_inflight_limit_bytes = 32 * 1024 * 1024;
    path.app_limited = true;

    let inflight =
        reliable_product_feedback_window_bytes(Some(path), TrafficClass::Throughput, mux_limits);

    let native_exploration = (path.carrier_inflight_limit_bytes as usize)
        .saturating_add(reliable_bulk_carrier_feed_quantum_bytes(mux_limits));
    assert_eq!(
        inflight,
        reliable_bulk_product_windows(mux_limits).per_output_product_limit_bytes as usize,
    );
    assert_eq!(
        reliable_bulk_unproven_exploration_limit_bytes(path, mux_limits),
        native_exploration as u64,
    );
    assert!(
        inflight > native_exploration,
        "Product memory authority must not become a duplicate QUIC congestion window",
    );
}

#[test]
fn cold_quic_product_p_does_not_replace_its_bounded_exploration_e() {
    let mux_limits = MuxLimits::default();
    let mut path = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 360.0, 1_000_000.0);
    path.carrier_inflight_limit_bytes = 2 * 1024 * 1024;

    let inflight =
        reliable_product_feedback_window_bytes(Some(path), TrafficClass::Throughput, mux_limits);

    assert_eq!(
        inflight,
        reliable_bulk_product_windows(mux_limits).per_output_product_limit_bytes as usize,
    );
    assert_eq!(
        reliable_bulk_unproven_exploration_limit_bytes(path, mux_limits),
        (2 * 1024 * 1024 + reliable_bulk_carrier_feed_quantum_bytes(mux_limits)) as u64,
        "unqualified additional-path acquisition remains bounded by fresh C+Q",
    );
}

#[test]
fn tcp_product_p_is_invariant_while_exploration_tracks_native_credit() {
    let mux_limits = MuxLimits::default();
    let path = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 425.0, 25_000_000.0);
    let modeled =
        reliable_product_feedback_window_bytes(Some(path), TrafficClass::Throughput, mux_limits);
    let mut with_cached_native_window = path;
    with_cached_native_window.carrier_inflight_limit_bytes = 32 * 1024 * 1024;

    let with_headroom = reliable_product_feedback_window_bytes(
        Some(with_cached_native_window),
        TrafficClass::Throughput,
        mux_limits,
    );
    let expected_exploration = (with_cached_native_window.carrier_inflight_limit_bytes as usize)
        .saturating_add(reliable_bulk_carrier_feed_quantum_bytes(mux_limits));
    assert_eq!(with_headroom, modeled);
    assert_eq!(
        with_headroom,
        reliable_bulk_product_windows(mux_limits).per_output_product_limit_bytes as usize,
    );
    assert_eq!(
        reliable_bulk_unproven_exploration_limit_bytes(with_cached_native_window, mux_limits),
        expected_exploration as u64,
    );
}

#[test]
fn tcp_product_p_cannot_enlarge_exploration_e() {
    let mux_limits = MuxLimits::default();
    let mut path = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 800.0, 4_000_000_000.0);
    path.product_progress_rate_bps = Some(4_000_000_000.0);
    path.has_durable_product_progress = true;
    path.carrier_inflight_limit_bytes = 2 * 1024 * 1024;

    let product_window =
        reliable_product_feedback_window_bytes(Some(path), TrafficClass::Throughput, mux_limits);
    let native_window = (path.carrier_inflight_limit_bytes as usize)
        .saturating_add(reliable_bulk_carrier_feed_quantum_bytes(mux_limits))
        .max(reliable_relay_buffer_len(mux_limits));

    assert_eq!(
        reliable_bulk_unproven_exploration_limit_bytes(path, mux_limits),
        native_window as u64,
    );
    assert_eq!(
        product_window,
        reliable_bulk_product_windows(mux_limits).per_output_product_limit_bytes as usize,
    );
    assert!(product_window > native_window);
}

#[test]
fn shared_source_staging_sums_exact_live_output_windows() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 2 * 1024 * 1024,
        max_repair_bytes: 8 * 1024 * 1024,
        max_reorder_bytes: 8 * 1024 * 1024,
        max_stream_window_bytes: 8 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let mut path = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 800.0, 4_000_000_000.0);
    path.product_progress_rate_bps = Some(4_000_000_000.0);
    path.has_durable_product_progress = true;
    path.carrier_inflight_limit_bytes = 2 * 1024 * 1024;

    let path_limit =
        reliable_product_feedback_window_bytes(Some(path), TrafficClass::Throughput, mux_limits);
    path.data_level_limit_bytes = path_limit as u64;
    let output = ReliableOriginalDataOutput {
        snapshot: path,
        stale: false,
    };
    let one_output = reliable_stream_source_admission(
        [output],
        TrafficClass::Throughput,
        PATH_OPEN_SCORE_BYTES,
        mux_limits,
    );
    let two_outputs = reliable_stream_source_admission(
        [output, output],
        TrafficClass::Throughput,
        PATH_OPEN_SCORE_BYTES,
        mux_limits,
    );

    assert_eq!(one_output.window_bytes, path_limit);
    assert_eq!(
        one_output.selected_path.map(|selected| selected.id),
        Some(path.id)
    );
    assert_eq!(two_outputs.window_bytes, path_limit.saturating_mul(2));
    assert!(
        two_outputs.window_bytes > mux_limits.max_path_flight_bytes,
        "a per-path cap cannot suppress the sum of two exact output windows",
    );
    assert_eq!(
        reliable_stream_source_admission(
            [],
            TrafficClass::Throughput,
            PATH_OPEN_SCORE_BYTES,
            mux_limits,
        )
        .window_bytes,
        0
    );
}

#[test]
fn source_admission_uses_one_schedulable_precedence_set() {
    let mux_limits = MuxLimits::default();
    let mut regular = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 40.0, 100_000_000.0);
    regular.carrier_inflight_limit_bytes = 256 * 1024;
    regular.data_level_limit_bytes =
        reliable_product_feedback_window_bytes(Some(regular), TrafficClass::Throughput, mux_limits)
            as u64;
    let mut backup = regular;
    backup.id = PathId(1);
    backup.policy.backup = true;
    let mut draining = regular;
    draining.id = PathId(2);
    draining.state = crate::scheduler::PathState::Draining;

    let admission = reliable_stream_source_admission(
        [
            ReliableOriginalDataOutput {
                snapshot: regular,
                stale: true,
            },
            ReliableOriginalDataOutput {
                snapshot: backup,
                stale: false,
            },
            ReliableOriginalDataOutput {
                snapshot: draining,
                stale: false,
            },
        ],
        TrafficClass::Throughput,
        PATH_OPEN_SCORE_BYTES,
        mux_limits,
    );

    assert_eq!(
        admission.selected_path.map(|selected| selected.id),
        Some(backup.id),
        "a schedulable non-stale backup precedes stale regular and draining outputs"
    );
    assert_eq!(
        admission.window_bytes,
        reliable_product_feedback_window_bytes(Some(backup), TrafficClass::Throughput, mux_limits)
    );
}

#[test]
fn sender_dispatch_budget_batches_bounded_bulk_quanta() {
    let mux_limits = MuxLimits::default();
    let adaptive_chunk = 64 * 1024;
    let inflight_limit = 8 * reliable_relay_buffer_len(mux_limits);
    let queue_limit = inflight_limit;

    let (latency_bytes, latency_items) = reliable_relay_sender_dispatch_budget(
        mux_limits,
        TrafficClass::Latency,
        adaptive_chunk,
        inflight_limit,
        queue_limit,
    );
    assert_eq!(latency_bytes, adaptive_chunk);
    assert_eq!(latency_items, 1);

    let (bulk_bytes, bulk_items) = reliable_relay_sender_dispatch_budget(
        mux_limits,
        TrafficClass::Throughput,
        adaptive_chunk,
        inflight_limit,
        queue_limit,
    );
    assert_eq!(bulk_bytes, reliable_relay_buffer_len(mux_limits));
    assert_eq!(
        bulk_items,
        reliable_relay_buffer_len(mux_limits) / adaptive_chunk
    );
    assert!(bulk_bytes < inflight_limit);
}

#[test]
fn reliable_relay_chunking_uses_product_payload_envelope() {
    let mux_limits = MuxLimits {
        max_reliable_relay_chunk_bytes: 64 * 1024,
        max_ack_ranges: 16,
        ..MuxLimits::default()
    };
    let max_frame_payload = CodecLimits::default()
        .max_payload_bytes
        .max(1)
        .min(mux_limits.max_reliable_relay_chunk_bytes)
        .max(1);

    let latency_chunk = adaptive_reliable_relay_chunk_bytes_with_frame_limit(
        None,
        TrafficClass::Latency,
        mux_limits,
        max_frame_payload,
    );
    let bulk_chunk = adaptive_reliable_relay_chunk_bytes_with_frame_limit(
        None,
        TrafficClass::Throughput,
        mux_limits,
        max_frame_payload,
    );
    let fast = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 50.0, 2_000_000_000.0);
    let fast_bulk_chunk = adaptive_reliable_relay_chunk_bytes_with_frame_limit(
        Some(fast),
        TrafficClass::Throughput,
        mux_limits,
        max_frame_payload,
    );

    assert_eq!(
        latency_chunk,
        adaptive_reliable_relay_chunk_bytes(None, TrafficClass::Latency, mux_limits)
            .min(max_frame_payload)
            .max(1)
    );
    assert_eq!(
        bulk_chunk,
        adaptive_reliable_relay_chunk_bytes(None, TrafficClass::Throughput, mux_limits)
            .min(max_frame_payload)
            .max(1)
    );
    assert_eq!(
        fast_bulk_chunk,
        adaptive_reliable_relay_chunk_bytes(Some(fast), TrafficClass::Throughput, mux_limits)
            .min(max_frame_payload)
            .max(1)
    );
    assert!(latency_chunk <= max_frame_payload);
    assert!(bulk_chunk <= max_frame_payload);
    assert!(
        fast_bulk_chunk
            <= reliable_relay_scheduler_quantum_cap(
                Some(fast),
                TrafficClass::Throughput,
                mux_limits
            )
    );
}

#[test]
fn reliable_product_window_is_configured_authority_not_class_or_underlay_policy() {
    let mux_limits = MuxLimits::default();
    for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
        for lane in [TrafficClass::Latency, TrafficClass::Throughput] {
            assert_eq!(
                reliable_stream_initial_advertised_window_bytes(underlay, lane, mux_limits),
                mux_limits.max_stream_window_bytes,
            );
        }
    }

    let snapshot = PathSnapshot::new(PathId(7), UnderlayProtocol::Udp, 40.0, 200_000_000.0);
    for lane in [TrafficClass::Latency, TrafficClass::Throughput] {
        assert_eq!(
            reliable_stream_advertised_window_bytes(Some(snapshot), lane, mux_limits),
            mux_limits.max_stream_window_bytes,
        );
    }
}

#[test]
fn reliable_recv_progress_default_bulk_ack_step_tracks_service_quantum() {
    let mux_limits = MuxLimits::default();
    let ack_step = reliable_stream_ack_update_bytes(None, TrafficClass::Throughput, mux_limits);

    assert_eq!(ack_step, MAX_RELIABLE_SERVICE_QUANTUM_BYTES as u64);
    let window =
        reliable_stream_advertised_window_bytes(None, TrafficClass::Throughput, mux_limits);
    assert!(ack_step < reliable_stream_max_data_update_bytes(window, mux_limits));
    assert_eq!(
        reliable_stream_ack_update_bytes(None, TrafficClass::Latency, mux_limits),
        1
    );
}

#[test]
fn tcp_capacity_proof_requires_exact_fresh_receipt() {
    let accepted_at = Instant::now();
    let proof = TcpCapacityProofCandidate {
        token: 7,
        train_bytes: 2 * 1024 * 1024,
        received_bytes: 2 * 1024 * 1024,
        rate_sample_bytes: 2 * 1024 * 1024,
        proof_elapsed: Duration::from_millis(400),
        receipt_rate_bps: 40_000_000,
        rate_bps: 80_000_000,
        accepted_at,
        expires_at: accepted_at + Duration::from_secs(1),
    };

    assert!(valid_tcp_capacity_proof_candidate_at(proof, accepted_at));
    assert!(!valid_tcp_capacity_proof_candidate_at(
        TcpCapacityProofCandidate {
            received_bytes: proof.received_bytes - 1,
            ..proof
        },
        accepted_at,
    ));
    assert!(!valid_tcp_capacity_proof_candidate_at(
        proof,
        proof.expires_at
    ));
}
