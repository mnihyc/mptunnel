use super::{
    MAX_RELIABLE_SERVICE_QUANTUM_BYTES, PATH_OPEN_SCORE_BYTES, RELIABLE_INITIAL_RTT,
    RELIABLE_PIPE_WINDOW_BDPS, TcpCapacityProofCandidate, adaptive_reliable_relay_chunk_bytes,
    adaptive_reliable_relay_chunk_bytes_with_frame_limit, adaptive_reliable_relay_inflight_bytes,
    min_reliable_pipe_bytes, reliable_bulk_carrier_feed_quantum_bytes, reliable_relay_buffer_len,
    reliable_relay_scheduler_quantum_cap, reliable_relay_sender_dispatch_budget,
    reliable_startup_bdp_bytes, reliable_startup_send_quantum_bytes,
    reliable_stream_ack_update_bytes, reliable_stream_advertised_window_bytes,
    reliable_stream_initial_advertised_window_bytes, reliable_stream_max_data_update_bytes,
    valid_tcp_capacity_proof_candidate_at,
};
use crate::mux::MuxLimits;
use crate::protocol::codec::CodecLimits;
use crate::protocol::{PathId, UnderlayProtocol};
use crate::scheduler::{PathSnapshot, TrafficClass};
use std::time::{Duration, Instant};

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
        adaptive_reliable_relay_inflight_bytes(Some(stable), TrafficClass::Latency, mux_limits);
    let bulk_inflight =
        adaptive_reliable_relay_inflight_bytes(Some(stable), TrafficClass::Throughput, mux_limits);
    let mut stable_with_flight = stable;
    stable_with_flight.bytes_in_flight =
        ((stable.delivery_rate_bps / 8.0) * (stable.srtt_ms / 1000.0)).ceil() as u64;
    let bulk_inflight_with_flight = adaptive_reliable_relay_inflight_bytes(
        Some(stable_with_flight),
        TrafficClass::Throughput,
        mux_limits,
    );
    let unstable_bulk_inflight = adaptive_reliable_relay_inflight_bytes(
        Some(unstable),
        TrafficClass::Throughput,
        mux_limits,
    );
    assert!(bulk_inflight >= interactive_inflight);
    assert_eq!(
        bulk_inflight_with_flight, bulk_inflight,
        "in-flight bytes are the controlled BDP-scale flight, not queue pressure"
    );
    assert!(
        interactive_inflight <= reliable_relay_buffer_len(mux_limits),
        "interactive streams should not inherit the bulk path ceiling"
    );
    assert!(
        bulk_inflight >= interactive_inflight.saturating_mul(8),
        "bulk transfer should be able to ramp far beyond interactive budget on high-BDP paths"
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
fn unknown_path_startup_inflight_uses_default_bdp_not_configured_ceiling() {
    let mux_limits = MuxLimits::default();
    let startup =
        adaptive_reliable_relay_inflight_bytes(None, TrafficClass::Throughput, mux_limits);
    let default_service_window = (reliable_startup_bdp_bytes() * RELIABLE_PIPE_WINDOW_BDPS)
        .max(reliable_startup_send_quantum_bytes() as f64)
        .max(min_reliable_pipe_bytes(mux_limits) as f64)
        .ceil() as usize;

    assert_eq!(
        startup,
        default_service_window.max(reliable_relay_buffer_len(mux_limits))
    );
    assert!(
        startup < mux_limits.max_path_flight_bytes,
        "configured inflight is a ceiling, not an unknown-path startup target"
    );
}

#[test]
fn unproven_portable_tcp_keeps_bounded_startup_service() {
    let mux_limits = MuxLimits::default();
    let path = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 360.0, 3_200_000.0);

    let inflight =
        adaptive_reliable_relay_inflight_bytes(Some(path), TrafficClass::Throughput, mux_limits);

    assert!(inflight < mux_limits.max_path_flight_bytes);
    assert_eq!(inflight, reliable_relay_buffer_len(mux_limits));
}

#[test]
fn proven_portable_tcp_uses_product_resource_ceiling() {
    let mux_limits = MuxLimits::default();
    let mut path = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 360.0, 3_200_000.0);
    path.product_progress_rate_bps = Some(3_200_000.0);
    path.has_durable_product_progress = true;
    path.app_limited = true;

    let inflight =
        adaptive_reliable_relay_inflight_bytes(Some(path), TrafficClass::Throughput, mux_limits);

    assert_eq!(inflight, mux_limits.max_path_flight_bytes);
}

#[test]
fn portable_tcp_latency_service_remains_bounded() {
    let mux_limits = MuxLimits::default();
    let mut path = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 360.0, 3_200_000.0);
    path.has_durable_product_progress = true;

    let inflight =
        adaptive_reliable_relay_inflight_bytes(Some(path), TrafficClass::Latency, mux_limits);

    assert!(inflight < mux_limits.max_path_flight_bytes);
}

#[test]
fn carrier_inflight_evidence_does_not_cap_product_source_read_horizon() {
    let mux_limits = MuxLimits::default();
    let mut path = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 80.0, 4_000_000_000.0);
    path.carrier_inflight_limit_bytes = 1024 * 1024;

    let inflight =
        adaptive_reliable_relay_inflight_bytes(Some(path), TrafficClass::Throughput, mux_limits);

    assert!(inflight > path.carrier_inflight_limit_bytes as usize);
    assert!(
        inflight <= mux_limits.max_path_flight_bytes,
        "carrier cwnd is a carrier emission gate, not a product source-read cap"
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
        adaptive_reliable_relay_inflight_bytes(Some(path), TrafficClass::Throughput, mux_limits);

    assert_eq!(inflight, mux_limits.max_path_flight_bytes);
    assert_eq!(
        path.delivery_rate_bps, 4_000_000_000.0,
        "carrier rate remains carrier evidence; product progress is a separate field"
    );
}

#[test]
fn quic_source_read_window_preserves_native_congestion_window_authority() {
    let mux_limits = MuxLimits::default();
    let mut path = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 425.0, 25_000_000.0);
    path.pacing_rate_bps = 175_000_000.0;
    path.carrier_inflight_limit_bytes = 32 * 1024 * 1024;
    path.app_limited = true;

    let inflight =
        adaptive_reliable_relay_inflight_bytes(Some(path), TrafficClass::Throughput, mux_limits);

    let modeled = ((path.delivery_rate_bps / 8.0) * (path.srtt_ms / 1000.0) * 2.0) as usize;
    let expected = (path.carrier_inflight_limit_bytes as usize)
        .saturating_add(reliable_bulk_carrier_feed_quantum_bytes(mux_limits));
    assert_eq!(inflight, expected);
    assert!(
        inflight > modeled.saturating_add(reliable_bulk_carrier_feed_quantum_bytes(mux_limits)),
        "an app-limited product sample must not become a second QUIC congestion window",
    );
    assert!(inflight < mux_limits.max_path_flight_bytes);
}

#[test]
fn cold_quic_path_gets_one_service_quantum_beyond_native_window() {
    let mux_limits = MuxLimits::default();
    let mut path = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 360.0, 1_000_000.0);
    path.carrier_inflight_limit_bytes = 2 * 1024 * 1024;

    let inflight =
        adaptive_reliable_relay_inflight_bytes(Some(path), TrafficClass::Throughput, mux_limits);

    assert_eq!(
        inflight,
        2 * 1024 * 1024 + reliable_bulk_carrier_feed_quantum_bytes(mux_limits),
        "startup can keep Quinn fed without exposing the configured 64 MiB ceiling",
    );
}

#[test]
fn tcp_source_read_window_preserves_native_congestion_window_authority() {
    let mux_limits = MuxLimits::default();
    let path = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 425.0, 25_000_000.0);
    let modeled =
        adaptive_reliable_relay_inflight_bytes(Some(path), TrafficClass::Throughput, mux_limits);
    let mut with_cached_native_window = path;
    with_cached_native_window.carrier_inflight_limit_bytes = 32 * 1024 * 1024;

    let with_headroom = adaptive_reliable_relay_inflight_bytes(
        Some(with_cached_native_window),
        TrafficClass::Throughput,
        mux_limits,
    );
    let expected = (with_cached_native_window.carrier_inflight_limit_bytes as usize)
        .saturating_add(reliable_bulk_carrier_feed_quantum_bytes(mux_limits));
    assert_eq!(
        with_headroom, expected,
        "TCP_INFO supplies feed geometry while the socket retains native send authority",
    );
    assert!(with_headroom > modeled);
    assert!(with_headroom < mux_limits.max_path_flight_bytes);
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
fn bulk_product_window_is_configured_memory_authority_not_path_proof() {
    let mux_limits = MuxLimits::default();
    let tcp_initial = reliable_stream_initial_advertised_window_bytes(
        UnderlayProtocol::Tcp,
        TrafficClass::Throughput,
        mux_limits,
    );
    let udp_initial = reliable_stream_initial_advertised_window_bytes(
        UnderlayProtocol::Udp,
        TrafficClass::Throughput,
        mux_limits,
    );

    assert_eq!(tcp_initial, mux_limits.max_stream_window_bytes);
    assert_eq!(udp_initial, mux_limits.max_stream_window_bytes);

    let snapshot = PathSnapshot::new(PathId(7), UnderlayProtocol::Udp, 40.0, 200_000_000.0);
    let measured_window = reliable_stream_advertised_window_bytes(
        Some(snapshot),
        TrafficClass::Throughput,
        mux_limits,
    );

    assert_eq!(measured_window, mux_limits.max_stream_window_bytes);
    assert!(
        reliable_stream_initial_advertised_window_bytes(
            UnderlayProtocol::Udp,
            TrafficClass::Latency,
            mux_limits,
        ) < udp_initial,
        "latency QUIC retains its bounded startup product window"
    );
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
