use super::*;

#[test]
fn nonblocking_udp_open_uses_zero_initial_window_without_accept() {
    let options = UdpStreamOpenOptions {
        wait_for_accept: false,
        role: StreamOpenRole::Validation,
    };

    assert_eq!(udp_stream_open_initial_max_offset(options, None), 0);
}

#[test]
fn blocking_udp_open_uses_accepted_initial_window() {
    assert_eq!(
        udp_stream_open_initial_max_offset(UdpStreamOpenOptions::ACTIVE_WAIT, Some(8192)),
        8192
    );
}

#[test]
fn reliable_output_guard_detaches_on_abnormal_stream_exit() {
    let registry = Arc::new(ServerReliableStreamRegistry::new(
        ResourceLimits::default().max_streams,
    ));
    let session_id = SessionId(201);
    let stream_id = StreamId(301);
    let path_id = PathId(0);
    let path_registration =
        registry.register_carrier_path(session_id, UnderlayProtocol::Udp, path_id);
    let target = TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80)));
    let (commands, _receivers) = reliable_path_command_channels(8);
    let commands_for_guard = commands.clone();
    let stream = match registry
        .open_or_attach(
            ServerReliableStreamOpenRequest {
                session_id,
                stream_id,
                target: &target,
                lane: FlowLane::Throughput,
                attachment: ServerReliablePathAttachment {
                    path_registration: path_registration.clone(),
                    commands,
                    max_frame_payload_bytes: udp_path_max_stream_payload_bytes(
                        CodecLimits::default(),
                        MuxLimits::default(),
                    ),
                    role: StreamOpenRole::Active,
                    initial_metrics: None,
                },
            },
            MuxLimits::default(),
            ResourceLimits::default().max_streams,
        )
        .expect("open UDP response stream")
    {
        ServerReliableStreamOpen::New(stream) => stream,
        _ => panic!("expected new UDP response stream"),
    };
    let ReliablePathStreamOutput::Switchable(binding) = &stream.output else {
        panic!("expected switchable response output");
    };
    assert_eq!(
        binding
            .sender_path_targets(FlowLane::Throughput, BBR_MAX_SEND_QUANTUM_BYTES)
            .len(),
        1
    );

    drop(ServerUdpReliableOutputDetachGuard {
        registry,
        session_id,
        stream_id,
        path_id,
        commands: commands_for_guard,
    });

    assert!(
        binding
            .sender_path_targets(FlowLane::Throughput, BBR_MAX_SEND_QUANTUM_BYTES)
            .is_empty(),
        "every server QUIC stream exit must detach its response output"
    );
}

fn quic_congestion(
    congestion_window: u64,
    pacing_rate_bps: Option<u64>,
) -> quic_carrier::CongestionMetrics {
    quic_carrier::CongestionMetrics {
        congestion_window,
        bytes_in_flight: Some(0),
        pending_bytes: 0,
        pacing_rate_bps,
        loss_ppm: None,
        ecn_ppm: None,
        newly_acked_bytes: None,
        non_app_limited_acked_bytes: None,
        timed_non_app_limited_acked_bytes: None,
        non_app_limited_ack_elapsed: None,
        delivery_evidence_written_bytes: 0,
        delivery_sample_count: 0,
        non_app_limited_delivery_sample_count: 0,
        timed_non_app_limited_delivery_sample_count: 0,
        app_limited: true,
        capacity_probe: None,
    }
}

fn with_delivery_evidence_written(
    mut metrics: quic_carrier::CongestionMetrics,
    bytes: u64,
) -> quic_carrier::CongestionMetrics {
    metrics.delivery_evidence_written_bytes = bytes;
    metrics
}

fn with_acked_bytes(
    metrics: quic_carrier::CongestionMetrics,
    bytes: u64,
    sample_count: u64,
) -> quic_carrier::CongestionMetrics {
    with_acked_bytes_elapsed(metrics, bytes, sample_count, Duration::from_millis(100))
}

fn with_acked_bytes_elapsed(
    mut metrics: quic_carrier::CongestionMetrics,
    bytes: u64,
    sample_count: u64,
    elapsed: Duration,
) -> quic_carrier::CongestionMetrics {
    metrics.newly_acked_bytes = Some(bytes);
    metrics.non_app_limited_acked_bytes = Some(bytes);
    metrics.timed_non_app_limited_acked_bytes = (!elapsed.is_zero()).then_some(bytes);
    metrics.non_app_limited_ack_elapsed = (!elapsed.is_zero()).then_some(elapsed);
    metrics.delivery_sample_count = sample_count;
    metrics.non_app_limited_delivery_sample_count = sample_count;
    metrics.timed_non_app_limited_delivery_sample_count =
        if elapsed.is_zero() { 0 } else { sample_count };
    metrics.app_limited = false;
    metrics
}

fn capacity_probe_metrics(
    token: u64,
    now: Instant,
    warmup_bytes: u64,
    required_bytes: u64,
    timed_bytes: u64,
    timed_count: u64,
    timed_elapsed: Option<Duration>,
) -> quic_carrier::CapacityProbeMetrics {
    let sample_floor_bytes = required_bytes.saturating_add(PATH_OPEN_SCORE_BYTES as u64);
    let train_payload_bytes = warmup_bytes
        .saturating_add(required_bytes)
        .max(sample_floor_bytes);
    let receipt_elapsed = Duration::from_millis(80);
    quic_carrier::CapacityProbeMetrics {
        token,
        train_payload_bytes,
        sample_floor_bytes,
        warmup_carrier_bytes: warmup_bytes,
        required_timed_carrier_bytes: required_bytes,
        expires_at: now + Duration::from_secs(5),
        phase: quic_carrier::CapacityProbePhase::Proven,
        started_clean: false,
        write_committed: true,
        written_payload_bytes: train_payload_bytes,
        written_data_frame_count: train_payload_bytes.div_ceil(64 * 1024),
        total_acked_carrier_bytes: train_payload_bytes,
        total_ack_sample_count: timed_count.saturating_add(u64::from(warmup_bytes > 0)),
        warmup_acked_carrier_bytes: warmup_bytes,
        warmup_ack_sample_count: u64::from(warmup_bytes > 0),
        measurement_acked_carrier_bytes: train_payload_bytes.saturating_sub(warmup_bytes),
        measurement_ack_sample_count: timed_count,
        timed_measurement_acked_carrier_bytes: timed_bytes,
        timed_measurement_ack_sample_count: timed_count,
        app_limited_acked_carrier_bytes: timed_bytes,
        app_limited_ack_sample_count: timed_count,
        timed_measurement_ack_elapsed: timed_elapsed,
        native_proved_at: timed_elapsed.map(|_| now),
        proved_at: Some(now),
        proof_validity: Duration::from_secs(3),
        receipt_received_payload_bytes: train_payload_bytes,
        receipt_elapsed: Some(receipt_elapsed),
        receipt_rtt: Some(Duration::from_millis(20)),
        receipt_at: Some(now),
        last_authoritative_in_flight: Some(0),
        last_authoritative_in_flight_at: Some(now),
        last_authoritative_sent_watermark: Some(train_payload_bytes),
        receipt_frozen_sent_watermark: Some(train_payload_bytes),
        current_sent_watermark: train_payload_bytes,
    }
}

fn with_capacity_probe(
    mut metrics: quic_carrier::CongestionMetrics,
    probe: quic_carrier::CapacityProbeMetrics,
) -> quic_carrier::CongestionMetrics {
    metrics.capacity_probe = Some(probe);
    metrics
}

#[test]
fn quic_product_payload_uses_sender_quantum_not_packet_train_cap() {
    let mux_limits = MuxLimits::default();
    let codec_limits = CodecLimits::default();
    let payload_cap = udp_path_max_stream_payload_bytes(codec_limits, mux_limits);

    assert!(
        payload_cap >= BBR_MAX_SEND_QUANTUM_BYTES,
        "QUIC product dispatch must stay BDP/service-quantum sized; only carrier serialization may split records"
    );
}

#[test]
fn quic_reliable_stream_reader_queue_stays_logical_product_queue() {
    let mux_limits = MuxLimits::default();
    let codec_limits = CodecLimits::default();
    let queue = udp_reliable_stream_frame_queue(codec_limits, mux_limits);

    assert_eq!(
        queue,
        reliable_stream_frame_queue(mux_limits),
        "carrier recordization must not multiply the product reader queue or hide backlog"
    );
}

#[test]
fn quic_stats_feed_sender_side_udp_path_metrics() {
    let mut tracker = UdpPathMetricTracker::default();
    let congestion = quic_congestion(4 * 1024 * 1024, Some(500_000_000));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(50);
    stats.path.cwnd = 4 * 1024 * 1024;
    stats.path.current_mtu = 1400;

    let startup = tracker.quic.observe(stats, congestion, 2);
    assert_eq!(startup.direction, 2);
    assert_eq!(startup.delivery_sample_count, 0);
    assert_eq!(startup.delivery_rate_bps.round() as u64, 500_000_000);
    assert_eq!(startup.inflight_hi, 4 * 1024 * 1024);
    stats.frame_rx.acks = 4;
    let measured = tracker.quic.observe(
        stats,
        with_acked_bytes(
            with_delivery_evidence_written(congestion, 8 * 1024 * 1024),
            8 * 1024 * 1024,
            4,
        ),
        2,
    );
    assert_eq!(measured.direction, 2);
    assert_eq!(measured.delivery_sample_count, 4);
    assert!(measured.delivery_rate_bps > 0.0);
    assert!(measured.last_delivery_sample_at.is_some());
    assert!(!measured.app_limited);
}

#[test]
fn quic_delivery_rate_uses_carrier_ack_elapsed_not_metrics_poll_phase() {
    let sample_bytes = RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES / 2;
    let congestion = quic_congestion(sample_bytes, Some(500_000_000));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(20);
    stats.path.cwnd = sample_bytes;
    stats.path.current_mtu = 1400;
    let base = Instant::now();
    let mut fast_poll = QuicPathMetricTracker::default();
    let mut slow_poll = QuicPathMetricTracker::default();
    let _ = fast_poll.observe_at(stats, congestion, 2, base);
    let _ = slow_poll.observe_at(stats, congestion, 2, base);
    let ack = with_acked_bytes_elapsed(
        with_delivery_evidence_written(congestion, sample_bytes),
        sample_bytes,
        QUIC_INITIAL_WINDOW_PACKETS as u64,
        Duration::from_millis(20),
    );

    let fast = fast_poll.observe_at(stats, ack, 2, base + Duration::from_millis(10));
    let slow = slow_poll.observe_at(stats, ack, 2, base + Duration::from_millis(500));

    assert_eq!(
        fast.delivery_rate_bps.round() as u64,
        slow.delivery_rate_bps.round() as u64,
        "scheduler poll phase must not enter the carrier delivery-rate denominator"
    );
    assert_eq!(
        fast.delivery_rate_bps.round() as u64,
        (sample_bytes as f64 * 8.0 / 0.020).round() as u64
    );
}

#[test]
fn quic_zero_span_ack_batch_proves_reachability_without_rate() {
    let sample_bytes = RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES / 2;
    let congestion = quic_congestion(sample_bytes, Some(500_000_000));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(200);
    stats.path.cwnd = sample_bytes;
    stats.path.current_mtu = 1400;
    let mut tracker = QuicPathMetricTracker::default();
    let startup = tracker.observe(stats, congestion, 2);

    let untimed = tracker.observe(
        stats,
        with_acked_bytes_elapsed(
            with_delivery_evidence_written(congestion, sample_bytes),
            sample_bytes,
            QUIC_INITIAL_WINDOW_PACKETS as u64,
            Duration::ZERO,
        ),
        2,
    );

    assert!(untimed.ack_derived_data_seen);
    assert_eq!(untimed.delivery_sample_bytes, 0);
    assert_eq!(untimed.delivery_sample_count, 0);
    assert_eq!(untimed.delivery_rate_bps, startup.delivery_rate_bps);
    assert!(untimed.app_limited);
}

#[test]
fn quic_combined_poll_excludes_untimed_ack_bytes_from_rate() {
    let timed_bytes = RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES / 2;
    let total_bytes = timed_bytes * 2;
    let congestion = quic_congestion(timed_bytes, Some(500_000_000));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(20);
    stats.path.cwnd = timed_bytes;
    stats.path.current_mtu = 1400;
    let mut tracker = QuicPathMetricTracker::default();
    let _ = tracker.observe(stats, congestion, 2);

    let mut combined = with_delivery_evidence_written(congestion, total_bytes);
    combined.newly_acked_bytes = Some(total_bytes);
    combined.non_app_limited_acked_bytes = Some(total_bytes);
    combined.timed_non_app_limited_acked_bytes = Some(timed_bytes);
    combined.non_app_limited_ack_elapsed = Some(Duration::from_millis(20));
    combined.delivery_sample_count = (QUIC_INITIAL_WINDOW_PACKETS * 2) as u64;
    combined.non_app_limited_delivery_sample_count = (QUIC_INITIAL_WINDOW_PACKETS * 2) as u64;
    combined.timed_non_app_limited_delivery_sample_count = QUIC_INITIAL_WINDOW_PACKETS as u64;
    combined.app_limited = false;

    let measured = tracker.observe(stats, combined, 2);

    assert!(measured.ack_derived_data_seen);
    assert_eq!(measured.delivery_sample_bytes, timed_bytes);
    assert_eq!(
        measured.delivery_rate_bps.round() as u64,
        (timed_bytes as f64 * 8.0 / 0.020).round() as u64,
        "untimed reachability ACKs must not enter a timed rate numerator"
    );
}

#[test]
fn quic_split_ack_polls_sum_carrier_elapsed_before_one_timer_clamp() {
    let sample_bytes = RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES / 2;
    let chunk_bytes = sample_bytes / 2;
    let congestion = quic_congestion(sample_bytes, Some(500_000_000));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(20);
    stats.path.cwnd = sample_bytes;
    stats.path.current_mtu = 1400;
    let mut tracker = QuicPathMetricTracker::default();
    let _ = tracker.observe(stats, congestion, 2);

    let first = tracker.observe(
        stats,
        with_acked_bytes_elapsed(
            with_delivery_evidence_written(congestion, sample_bytes),
            chunk_bytes,
            (QUIC_INITIAL_WINDOW_PACKETS / 2) as u64,
            Duration::from_millis(20),
        ),
        2,
    );
    assert_eq!(first.delivery_sample_count, 0);
    let measured = tracker.observe(
        stats,
        with_acked_bytes_elapsed(
            with_delivery_evidence_written(congestion, sample_bytes),
            chunk_bytes,
            (QUIC_INITIAL_WINDOW_PACKETS / 2) as u64,
            Duration::from_millis(30),
        ),
        2,
    );

    assert_eq!(measured.delivery_sample_bytes, sample_bytes);
    assert_eq!(
        measured.delivery_rate_bps.round() as u64,
        (sample_bytes as f64 * 8.0 / 0.050).round() as u64
    );
}

#[test]
fn quic_ack_only_stats_do_not_create_delivery_rate_evidence() {
    let mut tracker = UdpPathMetricTracker::default();
    let congestion = quic_congestion(4 * 1024 * 1024, Some(500_000_000));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(1);
    stats.path.cwnd = 4 * 1024 * 1024;
    stats.path.current_mtu = 1400;
    let _ = tracker.quic.observe(stats, congestion, 1);

    stats.frame_rx.acks = 1;
    let ack_only = tracker.quic.observe(stats, congestion, 1);
    assert_eq!(ack_only.delivery_sample_count, 0);
    assert!(ack_only.last_delivery_sample_at.is_none());
    assert_eq!(ack_only.delivery_rate_bps.round() as u64, 500_000_000);
}

#[test]
fn quic_tx_bytes_without_newly_acked_bytes_do_not_create_delivery_rate_evidence() {
    let mut tracker = UdpPathMetricTracker::default();
    let congestion = quic_congestion(4 * 1024 * 1024, Some(500_000_000));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(50);
    stats.path.cwnd = 4 * 1024 * 1024;
    stats.path.current_mtu = 1400;
    let _ = tracker.quic.observe(stats, congestion, 2);
    let tx_only = tracker.quic.observe(
        stats,
        with_delivery_evidence_written(congestion, 8 * 1024 * 1024),
        2,
    );

    assert_eq!(tx_only.delivery_sample_count, 0);
    assert!(tx_only.last_delivery_sample_at.is_none());
    assert_eq!(tx_only.delivery_rate_bps.round() as u64, 500_000_000);
}

#[test]
fn quic_product_data_accepted_by_quinn_counts_as_queue_until_ack() {
    let mut tracker = UdpPathMetricTracker::default();
    let congestion = quic_congestion(4 * 1024 * 1024, Some(500_000_000));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(50);
    stats.path.cwnd = 4 * 1024 * 1024;
    stats.path.current_mtu = 1400;
    let _ = tracker.quic.observe(stats, congestion, 2);

    let queued = tracker.quic.observe(
        stats,
        with_delivery_evidence_written(congestion, 8 * 1024 * 1024),
        2,
    );
    assert_eq!(queued.bytes_in_flight, 0);
    assert_eq!(queued.pending_bytes, 8 * 1024 * 1024);
    let product_metrics = path_metrics_from_quic_path(PathId(7), queued);
    assert_eq!(product_metrics.queue_bytes, 8 * 1024 * 1024);

    let partially_acked = tracker.quic.observe(
        stats,
        with_acked_bytes(
            with_delivery_evidence_written(congestion, 8 * 1024 * 1024),
            2 * 1024 * 1024,
            1,
        ),
        2,
    );
    assert_eq!(partially_acked.pending_bytes, 6 * 1024 * 1024);
}

#[test]
fn quic_loss_unknown_is_not_reported_as_observed_zero() {
    let metrics = UdpPathMetrics {
        direction: 2,
        srtt: Duration::from_millis(20),
        rttvar: Duration::from_millis(2),
        min_rtt: Duration::from_millis(18),
        min_rtt_observed: true,
        delivery_rate_bps: 500_000_000.0,
        pacing_rate_bps: 500_000_000.0,
        inflight_hi: 4 * 1024 * 1024,
        bytes_in_flight: 128 * 1024,
        pending_bytes: 256 * 1024,
        loss_ppm: None,
        ecn_ppm: None,
        app_limited: true,
        ack_derived_data_seen: false,
        delivery_sample_count: 0,
        delivery_sample_bytes: 0,
        last_delivery_sample_at: None,
        bulk_proof_expires_at: None,
        latest_delivery_sample_bytes: 0,
        latest_delivery_sample_count: 0,
        latest_carrier_ack_elapsed: None,
        latest_rate_sample_elapsed: None,
        capacity_proof_candidate: None,
        capacity_probe: None,
        #[cfg(feature = "lab-diagnostics")]
        ack_poll: QuicAckPollDiagnostics::default(),
    };

    let path_metrics = path_metrics_from_quic_path(PathId(7), metrics);

    assert_eq!(path_metrics.loss_ppm, 0);
    assert!(!path_metrics.loss_observed);
    assert_eq!(path_metrics.ecn_ppm, 0);
    assert!(!path_metrics.ecn_observed);
    assert_eq!(path_metrics.bytes_in_flight, 128 * 1024);
    assert_eq!(path_metrics.queue_bytes, 128 * 1024);
}

#[test]
fn quic_unknown_capacity_ack_sample_does_not_create_bulk_evidence() {
    let mut tracker = UdpPathMetricTracker::default();
    let congestion = quic_congestion(0, None);
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(50);

    let _ = tracker.quic.observe(stats, congestion, 2);
    stats.frame_rx.acks = 1;
    let unknown_capacity = tracker.quic.observe(
        stats,
        with_acked_bytes(with_delivery_evidence_written(congestion, 4096), 4096, 1),
        2,
    );

    assert_eq!(unknown_capacity.delivery_sample_count, 0);
    assert!(unknown_capacity.last_delivery_sample_at.is_none());
    assert_eq!(
        unknown_capacity.delivery_rate_bps.round() as u64,
        default_path_rate_bps(UnderlayProtocol::Udp).round() as u64
    );
    assert!(unknown_capacity.app_limited);
}

#[test]
fn quic_tiny_startup_pacing_does_not_poison_product_scheduler_rate() {
    let mut tracker = UdpPathMetricTracker::default();
    let congestion = quic_congestion(0, Some(4));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(50);

    let startup = tracker.quic.observe(stats, congestion, 2);
    let udp_startup_rate = default_path_rate_bps(UnderlayProtocol::Udp).round() as u64;

    assert_eq!(startup.delivery_sample_count, 0);
    assert!(startup.last_delivery_sample_at.is_none());
    assert_eq!(startup.delivery_rate_bps.round() as u64, udp_startup_rate);
    assert_eq!(startup.pacing_rate_bps.round() as u64, udp_startup_rate);
    stats.frame_rx.acks = 1;
    let app_limited =
        tracker
            .quic
            .observe(stats, with_delivery_evidence_written(congestion, 4096), 2);

    assert_eq!(app_limited.delivery_sample_count, 0);
    assert!(app_limited.last_delivery_sample_at.is_none());
    assert_eq!(
        app_limited.delivery_rate_bps.round() as u64,
        udp_startup_rate
    );
    assert_eq!(app_limited.pacing_rate_bps.round() as u64, udp_startup_rate);
    assert!(app_limited.app_limited);
}

#[test]
fn quic_udp_command_queue_tracks_sender_quantum_not_record_size() {
    let mux_limits = MuxLimits::default();
    let codec_limits = CodecLimits::default();
    let product_queue = reliable_path_command_queue(mux_limits);
    let quic_udp_queue = udp_path_command_queue(mux_limits, codec_limits);
    let sender_quantum =
        reliable_relay_scheduler_quantum_cap(None, FlowLane::Throughput, mux_limits);
    let record_sized_queue = reliable_path_command_queue_for_payload(
        mux_limits,
        sender_quantum.min(UDP_DEFAULT_MTU_PAYLOAD_BYTES).max(1),
    );

    assert_eq!(
        quic_udp_queue, product_queue,
        "command queue capacity must stay tied to the logical sender quantum"
    );
    assert_ne!(
        quic_udp_queue, record_sized_queue,
        "carrier packet/record sizing must not inflate the command queue"
    );
}

#[test]
fn quic_app_limited_low_ack_sample_does_not_poison_delivery_rate() {
    let mut tracker = UdpPathMetricTracker::default();
    let congestion = quic_congestion(4 * 1024 * 1024, Some(500_000_000));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(50);
    stats.path.cwnd = 4 * 1024 * 1024;
    stats.path.current_mtu = 1400;
    let _ = tracker.quic.observe(stats, congestion, 2);
    stats.frame_rx.acks = 1;
    let app_limited = tracker.quic.observe(
        stats,
        with_acked_bytes(
            with_delivery_evidence_written(congestion, 32 * 1024),
            32 * 1024,
            1,
        ),
        2,
    );

    assert_eq!(app_limited.delivery_sample_count, 0);
    assert!(app_limited.last_delivery_sample_at.is_none());
    assert_eq!(app_limited.delivery_rate_bps.round() as u64, 500_000_000);
    assert!(app_limited.app_limited);

    let mut changed_pacing = congestion;
    changed_pacing.pacing_rate_bps = Some(750_000_000);
    let refreshed_prior = tracker.quic.observe(stats, changed_pacing, 2);
    assert_eq!(refreshed_prior.delivery_sample_count, 0);
    assert_eq!(
        refreshed_prior.delivery_rate_bps.round() as u64,
        750_000_000,
        "a rejected app-limited ACK must not freeze the live pacing prior in the measured-rate slot"
    );
}

#[test]
fn quic_initial_full_quantum_sample_does_not_seed_tiny_bulk_rate() {
    let mut tracker = UdpPathMetricTracker::default();
    let congestion = quic_congestion(PATH_OPEN_SCORE_BYTES as u64, Some(500_000_000));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(50);
    stats.path.cwnd = PATH_OPEN_SCORE_BYTES as u64;
    stats.path.current_mtu = 1400;
    let startup = tracker.quic.observe(stats, congestion, 2);
    stats.frame_rx.acks = 1;
    let measured = tracker.quic.observe(
        stats,
        with_acked_bytes_elapsed(
            with_delivery_evidence_written(congestion, PATH_OPEN_SCORE_BYTES as u64),
            PATH_OPEN_SCORE_BYTES as u64,
            1,
            Duration::from_millis(1000),
        ),
        2,
    );

    assert_eq!(measured.delivery_sample_count, 1);
    assert_eq!(
        measured.delivery_rate_bps.round() as u64,
        startup.delivery_rate_bps.round() as u64,
        "a single underfed validation quantum must not replace the startup/pacing fallback with a tiny rate"
    );
}

#[test]
fn quic_poll_retains_non_app_limited_ack_bytes_after_later_idle_ack() {
    let mut tracker = UdpPathMetricTracker::default();
    let sample_bytes = 256 * 1024_u64;
    let congestion = quic_congestion(PATH_OPEN_SCORE_BYTES as u64, Some(500_000_000));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(20);
    stats.path.cwnd = PATH_OPEN_SCORE_BYTES as u64;
    stats.path.current_mtu = 1400;
    let _ = tracker.quic.observe(stats, congestion, 2);
    let mut polled = with_acked_bytes(
        with_delivery_evidence_written(congestion, sample_bytes),
        sample_bytes,
        QUIC_INITIAL_WINDOW_PACKETS as u64,
    );
    polled.app_limited = true;
    let measured = tracker.quic.observe(stats, polled, 2);

    assert_eq!(measured.delivery_sample_bytes, sample_bytes);
    assert!(measured.delivery_sample_count >= QUIC_INITIAL_WINDOW_PACKETS as u64);
    assert!(
        !measured.app_limited,
        "a later idle ACK flag must not erase non-app-limited bytes accumulated before the metrics poll"
    );
}

#[test]
fn quic_capacity_evidence_accumulates_across_small_ack_polls() {
    let mut tracker = UdpPathMetricTracker::default();
    let sample_bytes = RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES / 2;
    let chunk_bytes = sample_bytes / 8;
    let congestion = quic_congestion(sample_bytes, Some(500_000_000));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(20);
    stats.path.cwnd = sample_bytes;
    stats.path.current_mtu = 1400;
    let _ = tracker.quic.observe(stats, congestion, 2);

    let mut measured = None;
    for _ in 0..8 {
        measured = Some(tracker.quic.observe(
            stats,
            with_acked_bytes(
                with_delivery_evidence_written(congestion, sample_bytes),
                chunk_bytes,
                2,
            ),
            2,
        ));
    }
    let measured = measured.expect("split calibration sample");
    assert_eq!(measured.delivery_sample_bytes, sample_bytes);
    assert!(!measured.app_limited);

    let idle = tracker.quic.observe(
        stats,
        with_delivery_evidence_written(congestion, sample_bytes),
        2,
    );
    assert!(
        !idle.app_limited,
        "an idle metrics poll inside the 3-PTO horizon must preserve capacity evidence"
    );
}

#[test]
fn quic_app_limited_capacity_probe_emits_candidate_without_generic_proof() {
    let now = Instant::now();
    let required_bytes = 240 * 1024_u64;
    let mut congestion = quic_congestion(256 * 1024, Some(100_000_000));
    congestion.app_limited = true;
    congestion = with_capacity_probe(
        congestion,
        capacity_probe_metrics(
            41,
            now,
            0,
            required_bytes,
            required_bytes,
            32,
            Some(Duration::from_millis(40)),
        ),
    );
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(20);
    stats.path.cwnd = 256 * 1024;
    stats.path.current_mtu = 1400;
    let mut tracker = QuicPathMetricTracker::default();

    let observed = tracker.observe_at(stats, congestion, 2, now);
    let candidate = observed
        .capacity_proof_candidate
        .expect("receiver-confirmed capacity token");

    assert_eq!(candidate.token, 41);
    assert!(candidate.receipt_confirmed);
    assert_eq!(candidate.received_bytes, candidate.train_bytes);
    assert_eq!(candidate.proof_elapsed, Duration::from_millis(80));
    assert!(candidate.written_data_frame_count > 0);
    assert!(observed.app_limited);
    assert_eq!(observed.delivery_sample_count, 0);
    assert_eq!(observed.delivery_sample_bytes, 0);
    assert!(observed.bulk_proof_expires_at.is_none());
}

#[test]
fn quic_capacity_receipt_publishes_after_terminalization_and_freezes_rate() {
    let now = Instant::now();
    let required_bytes = 240 * 1024_u64;
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(20);
    stats.path.cwnd = 256 * 1024;
    stats.path.current_mtu = 1400;
    let base = quic_congestion(256 * 1024, Some(100_000_000));
    let mut probe = capacity_probe_metrics(
        42,
        now,
        0,
        required_bytes,
        required_bytes,
        32,
        Some(Duration::from_millis(40)),
    );
    probe.phase = quic_carrier::CapacityProbePhase::Proven;
    probe.last_authoritative_in_flight = Some(0);
    probe.last_authoritative_sent_watermark = Some(10_000);
    probe.receipt_frozen_sent_watermark = Some(11_200);
    probe.current_sent_watermark = 11_200;
    let mut tracker = QuicPathMetricTracker::default();

    let measured = tracker.observe_at(stats, with_capacity_probe(base, probe), 2, now);
    let candidate = measured
        .capacity_proof_candidate
        .expect("terminal exact receipt publishes independently of native cleanup");
    assert_eq!(candidate.proof_elapsed, Duration::from_millis(80));
    assert_eq!(candidate.accepted_at, now);
    assert_eq!(candidate.expires_at, now + candidate.proof_validity);
    assert_eq!(
        candidate.rate_bps,
        quic_capacity_receipt_rate_bps(candidate.train_bytes, candidate.proof_elapsed)
            .expect("receipt rate")
    );

    probe.phase = quic_carrier::CapacityProbePhase::Proven;
    probe.timed_measurement_ack_elapsed = Some(Duration::from_secs(2));
    probe.current_sent_watermark = 12_400;
    let later = tracker.observe_at(
        stats,
        with_capacity_probe(base, probe),
        2,
        now + Duration::from_millis(10),
    );
    assert_eq!(later.capacity_proof_candidate, Some(candidate));

    let mut late_tracker = QuicPathMetricTracker::default();
    let independently_observed = late_tracker.observe_at(
        stats,
        with_capacity_probe(base, probe),
        2,
        now + Duration::from_millis(20),
    );
    assert_eq!(
        independently_observed.capacity_proof_candidate,
        Some(candidate)
    );
}

#[test]
fn quic_capacity_candidate_accepts_only_receipted_publishable_phases() {
    let now = Instant::now();
    let required_bytes = 240 * 1024_u64;
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(20);
    stats.path.cwnd = 256 * 1024;
    stats.path.current_mtu = 1400;
    let base = quic_congestion(256 * 1024, Some(100_000_000));

    let proven = QuicPathMetricTracker::default().observe_at(
        stats,
        with_capacity_probe(
            base,
            capacity_probe_metrics(43, now, 0, required_bytes, 0, 0, None),
        ),
        2,
        now,
    );
    assert!(proven.capacity_proof_candidate.is_some());
    for phase in [
        quic_carrier::CapacityProbePhase::Writing,
        quic_carrier::CapacityProbePhase::Measuring,
        quic_carrier::CapacityProbePhase::ProvenDraining,
        quic_carrier::CapacityProbePhase::Expired,
        quic_carrier::CapacityProbePhase::Aborted,
    ] {
        let mut probe = capacity_probe_metrics(44, now, 0, required_bytes, 0, 0, None);
        probe.phase = phase;
        let observed = QuicPathMetricTracker::default().observe_at(
            stats,
            with_capacity_probe(base, probe),
            2,
            now,
        );
        assert!(
            observed.capacity_proof_candidate.is_none(),
            "phase {phase:?} cannot publish receipt authority"
        );
    }
}

#[test]
fn quic_active_capacity_probe_uses_bounded_quarter_rtt_poll_cadence() {
    let now = Instant::now();
    let required_bytes = 240 * 1024_u64;
    let metrics_for = |phase, rtt: Duration| {
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = rtt;
        stats.path.cwnd = 256 * 1024;
        stats.path.current_mtu = 1400;
        let mut probe = capacity_probe_metrics(45, now, 0, required_bytes, 0, 0, None);
        probe.phase = phase;
        QuicPathMetricTracker::default().observe_at(
            stats,
            with_capacity_probe(quic_congestion(256 * 1024, None), probe),
            2,
            now,
        )
    };

    for phase in [
        quic_carrier::CapacityProbePhase::Writing,
        quic_carrier::CapacityProbePhase::Measuring,
        quic_carrier::CapacityProbePhase::ProvenDraining,
        quic_carrier::CapacityProbePhase::Proven,
    ] {
        assert_eq!(
            quic_path_metrics_poll_interval(metrics_for(phase, Duration::from_millis(80))),
            Duration::from_millis(20),
            "phase {phase:?} must be polled faster than idle PTO cadence"
        );
    }
    assert_eq!(
        quic_path_metrics_poll_interval(metrics_for(
            quic_carrier::CapacityProbePhase::Proven,
            Duration::from_millis(400),
        )),
        QUIC_MAX_ACK_DELAY
    );
    assert_eq!(
        quic_path_metrics_poll_interval(metrics_for(
            quic_carrier::CapacityProbePhase::Measuring,
            Duration::from_millis(2),
        )),
        QUIC_TIMER_GRANULARITY
    );
    let expired = metrics_for(
        quic_carrier::CapacityProbePhase::Expired,
        Duration::from_millis(80),
    );
    assert!(quic_path_metrics_poll_interval(expired) > Duration::from_millis(20));
}

#[test]
fn quic_capacity_probe_requires_exact_full_train_receipt() {
    let now = Instant::now();
    let warmup_bytes = 384 * 1024_u64;
    let required_bytes = 240 * 1024_u64;
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(20);
    stats.path.cwnd = 512 * 1024;
    stats.path.current_mtu = 1400;
    let base = quic_congestion(512 * 1024, Some(100_000_000));
    let mut tracker = QuicPathMetricTracker::default();

    let mut incomplete_receipt =
        capacity_probe_metrics(51, now, warmup_bytes, required_bytes, 0, 0, None);
    incomplete_receipt.receipt_received_payload_bytes = incomplete_receipt.train_payload_bytes - 1;
    let below_floor =
        tracker.observe_at(stats, with_capacity_probe(base, incomplete_receipt), 2, now);
    assert!(below_floor.capacity_proof_candidate.is_none());

    let proven = tracker.observe_at(
        stats,
        with_capacity_probe(
            base,
            capacity_probe_metrics(51, now, warmup_bytes, required_bytes, 0, 0, None),
        ),
        2,
        now + Duration::from_millis(1),
    );
    let candidate = proven
        .capacity_proof_candidate
        .expect("exact receiver-confirmed train");
    assert_eq!(candidate.warmup_bytes, warmup_bytes);
    assert_eq!(candidate.received_bytes, candidate.train_bytes);
    assert_eq!(candidate.required_proof_bytes, required_bytes);
}

#[test]
fn quic_capacity_receipt_candidate_is_sticky_and_frozen() {
    let now = Instant::now();
    let required_bytes = 240 * 1024_u64;
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(20);
    stats.path.cwnd = 256 * 1024;
    stats.path.current_mtu = 1400;
    let base = quic_congestion(256 * 1024, Some(100_000_000));
    let mut tracker = QuicPathMetricTracker::default();
    let probe = |token, elapsed| {
        with_capacity_probe(
            base,
            capacity_probe_metrics(token, now, 0, required_bytes, required_bytes, 32, elapsed),
        )
    };

    let received = tracker.observe_at(stats, probe(61, None), 2, now);
    let accepted = received
        .capacity_proof_candidate
        .expect("receipt does not depend on a native ACK span");
    let mut retried = tracker.observe_at(
        stats,
        probe(61, Some(Duration::from_millis(40))),
        2,
        now + Duration::from_millis(2),
    );
    let retried_candidate = retried
        .capacity_proof_candidate
        .expect("transient rejection must retain sticky token");
    assert_eq!(retried_candidate.token, accepted.token);
    tracker.accept_capacity_proof(&mut retried, retried_candidate);
    let frozen_deadline = retried_candidate.expires_at;
    assert_eq!(
        frozen_deadline,
        retried_candidate.accepted_at + retried_candidate.proof_validity
    );
    let sticky = tracker.observe_at(
        stats,
        probe(61, Some(Duration::from_millis(40))),
        2,
        now + Duration::from_millis(3),
    );
    assert!(sticky.capacity_proof_candidate.is_none());
    assert!(sticky.bulk_proof_expires_at.is_none());
    let expired_sticky = tracker.observe_at(
        stats,
        probe(61, Some(Duration::from_millis(40))),
        2,
        frozen_deadline,
    );
    assert!(expired_sticky.app_limited);
    assert!(expired_sticky.capacity_proof_candidate.is_none());
    let rollover_at = frozen_deadline + Duration::from_millis(1);
    let rollover = tracker.observe_at(
        stats,
        with_capacity_probe(
            base,
            capacity_probe_metrics(
                62,
                rollover_at,
                0,
                required_bytes,
                required_bytes,
                32,
                Some(Duration::from_millis(40)),
            ),
        ),
        2,
        rollover_at,
    );
    assert_eq!(
        rollover.capacity_proof_candidate.map(|proof| proof.token),
        Some(62)
    );
}

#[test]
fn quic_bulk_proof_deadline_does_not_shrink_with_falling_rtt() {
    let sample_bytes = RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES / 2;
    let congestion = quic_congestion(sample_bytes, Some(100_000_000));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(400);
    stats.path.cwnd = sample_bytes;
    stats.path.current_mtu = 1400;
    let base = Instant::now();
    let proof_at = base + Duration::from_millis(1);
    let mut tracker = QuicPathMetricTracker::default();
    let _ = tracker.observe_at(stats, congestion, 2, base);
    let proven = tracker.observe_at(
        stats,
        with_acked_bytes(
            with_delivery_evidence_written(congestion, sample_bytes),
            sample_bytes,
            QUIC_INITIAL_WINDOW_PACKETS as u64,
        ),
        2,
        proof_at,
    );
    let frozen_deadline = proven
        .bulk_proof_expires_at
        .expect("accepted proof deadline");

    stats.path.rtt = Duration::from_millis(20);
    let smaller_horizon = quic_bulk_proof_freshness_horizon(stats.path.rtt, stats.path.rtt / 4);
    assert!(proof_at + smaller_horizon < frozen_deadline);
    let still_fresh = tracker.observe_at(
        stats,
        with_delivery_evidence_written(congestion, sample_bytes),
        2,
        proof_at + smaller_horizon,
    );
    assert!(!still_fresh.app_limited);
    assert_eq!(still_fresh.bulk_proof_expires_at, Some(frozen_deadline));

    let expired = tracker.observe_at(
        stats,
        with_delivery_evidence_written(congestion, sample_bytes),
        2,
        frozen_deadline,
    );
    assert!(expired.app_limited);
    assert!(expired.bulk_proof_expires_at.is_none());
}

#[test]
fn quic_expired_proof_preserves_new_pending_sample() {
    let sample_bytes = RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES / 2;
    let fragment_bytes = sample_bytes / 8;
    let congestion = quic_congestion(sample_bytes, Some(100_000_000));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(20);
    stats.path.cwnd = sample_bytes;
    stats.path.current_mtu = 1400;
    let base = Instant::now();
    let proof_at = base + Duration::from_millis(1);
    let mut tracker = QuicPathMetricTracker::default();
    let _ = tracker.observe_at(stats, congestion, 2, base);
    let proven = tracker.observe_at(
        stats,
        with_acked_bytes(
            with_delivery_evidence_written(congestion, sample_bytes),
            sample_bytes,
            QUIC_INITIAL_WINDOW_PACKETS as u64,
        ),
        2,
        proof_at,
    );
    let deadline = proven.bulk_proof_expires_at.expect("proof deadline");
    let written_bytes = sample_bytes.saturating_mul(3);
    let _ = tracker.observe_at(
        stats,
        with_acked_bytes(
            with_delivery_evidence_written(congestion, written_bytes),
            fragment_bytes,
            2,
        ),
        2,
        deadline - QUIC_TIMER_GRANULARITY,
    );
    assert_eq!(tracker.pending_non_app_limited_sample_bytes, fragment_bytes);

    let expired = tracker.observe_at(
        stats,
        with_delivery_evidence_written(congestion, written_bytes),
        2,
        deadline,
    );
    assert!(expired.app_limited);
    assert_eq!(tracker.pending_non_app_limited_sample_bytes, fragment_bytes);
    assert_eq!(tracker.pending_non_app_limited_sample_count, 2);
    assert!(!tracker.pending_non_app_limited_sample_elapsed.is_zero());
}

#[test]
fn quic_bulk_proof_is_fresh_inside_persistent_congestion_horizon() {
    let sample_bytes = RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES / 2;
    let congestion = quic_congestion(sample_bytes, Some(500_000_000));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(20);
    stats.path.cwnd = sample_bytes;
    stats.path.current_mtu = 1400;
    let base = Instant::now();
    let proof_at = base + Duration::from_millis(1);
    let horizon = quic_bulk_proof_freshness_horizon(stats.path.rtt, stats.path.rtt / 4);
    let mut tracker = QuicPathMetricTracker::default();
    let _ = tracker.observe_at(stats, congestion, 2, base);
    let proven = tracker.observe_at(
        stats,
        with_acked_bytes(
            with_delivery_evidence_written(congestion, sample_bytes),
            sample_bytes,
            QUIC_INITIAL_WINDOW_PACKETS as u64,
        ),
        2,
        proof_at,
    );

    assert!(!proven.app_limited);
    let fresh = tracker.observe_at(
        stats,
        with_delivery_evidence_written(congestion, sample_bytes),
        2,
        proof_at + horizon - QUIC_TIMER_GRANULARITY,
    );
    assert_eq!(fresh.delivery_sample_count, proven.delivery_sample_count);
    assert_eq!(fresh.delivery_sample_bytes, proven.delivery_sample_bytes);
    assert!(!fresh.app_limited);
}

#[test]
fn quic_aged_bulk_proof_expires_without_erasing_ack_reachability() {
    let sample_bytes = RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES / 2;
    let congestion = quic_congestion(sample_bytes, Some(500_000_000));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(20);
    stats.path.cwnd = sample_bytes;
    stats.path.current_mtu = 1400;
    let base = Instant::now();
    let proof_at = base + Duration::from_millis(1);
    let horizon = quic_bulk_proof_freshness_horizon(stats.path.rtt, stats.path.rtt / 4);
    let mut tracker = QuicPathMetricTracker::default();
    let _ = tracker.observe_at(stats, congestion, 2, base);
    let proven = tracker.observe_at(
        stats,
        with_acked_bytes(
            with_delivery_evidence_written(congestion, sample_bytes),
            sample_bytes,
            QUIC_INITIAL_WINDOW_PACKETS as u64,
        ),
        2,
        proof_at,
    );
    assert!(proven.ack_derived_data_seen);

    let aged = tracker.observe_at(
        stats,
        with_delivery_evidence_written(congestion, sample_bytes),
        2,
        proof_at + horizon,
    );
    assert!(aged.ack_derived_data_seen);
    assert_eq!(aged.delivery_rate_bps, proven.delivery_rate_bps);
    assert_eq!(aged.delivery_sample_count, proven.delivery_sample_count);
    assert_eq!(aged.delivery_sample_bytes, proven.delivery_sample_bytes);
    assert_eq!(aged.last_delivery_sample_at, proven.last_delivery_sample_at);
    assert!(aged.bulk_proof_expires_at.is_none());
    assert!(aged.app_limited);
}

#[test]
fn quic_reproved_bulk_rights_are_not_permanently_sticky() {
    let sample_bytes = RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES / 2;
    let congestion = quic_congestion(sample_bytes, Some(500_000_000));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(20);
    stats.path.cwnd = sample_bytes;
    stats.path.current_mtu = 1400;
    let base = Instant::now();
    let first_proof_at = base + Duration::from_millis(1);
    let horizon = quic_bulk_proof_freshness_horizon(stats.path.rtt, stats.path.rtt / 4);
    let mut tracker = QuicPathMetricTracker::default();
    let _ = tracker.observe_at(stats, congestion, 2, base);
    let _ = tracker.observe_at(
        stats,
        with_acked_bytes(
            with_delivery_evidence_written(congestion, sample_bytes),
            sample_bytes,
            QUIC_INITIAL_WINDOW_PACKETS as u64,
        ),
        2,
        first_proof_at,
    );
    let _ = tracker.observe_at(
        stats,
        with_delivery_evidence_written(congestion, sample_bytes),
        2,
        first_proof_at + horizon,
    );

    let second_proof_at = first_proof_at + horizon + QUIC_TIMER_GRANULARITY;
    let reproved = tracker.observe_at(
        stats,
        with_acked_bytes(
            with_delivery_evidence_written(congestion, sample_bytes * 2),
            sample_bytes,
            QUIC_INITIAL_WINDOW_PACKETS as u64,
        ),
        2,
        second_proof_at,
    );
    assert!(!reproved.app_limited);
    assert!(reproved.delivery_sample_count > 0);

    let aged_again = tracker.observe_at(
        stats,
        with_delivery_evidence_written(congestion, sample_bytes * 2),
        2,
        second_proof_at + horizon,
    );
    assert!(aged_again.app_limited);
    assert_eq!(aged_again.delivery_rate_bps, reproved.delivery_rate_bps);
    assert_eq!(
        aged_again.delivery_sample_count,
        reproved.delivery_sample_count
    );
    assert_eq!(
        aged_again.delivery_sample_bytes,
        reproved.delivery_sample_bytes
    );
    assert_eq!(
        aged_again.last_delivery_sample_at,
        reproved.last_delivery_sample_at
    );
    assert!(aged_again.bulk_proof_expires_at.is_none());
    assert!(aged_again.ack_derived_data_seen);
}

#[test]
fn quic_first_confident_sample_replaces_optimistic_startup_prior() {
    let mut tracker = UdpPathMetricTracker::default();
    let congestion = quic_congestion(PATH_OPEN_SCORE_BYTES as u64, Some(500_000_000));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(50);
    stats.path.cwnd = PATH_OPEN_SCORE_BYTES as u64;
    stats.path.current_mtu = 1400;
    let startup = tracker.quic.observe(stats, congestion, 2);
    stats.frame_rx.acks = 1;
    let first_quantum = tracker.quic.observe(
        stats,
        with_acked_bytes(
            with_delivery_evidence_written(congestion, PATH_OPEN_SCORE_BYTES as u64),
            PATH_OPEN_SCORE_BYTES as u64,
            1,
        ),
        2,
    );
    assert_eq!(first_quantum.delivery_sample_count, 1);
    assert_eq!(first_quantum.delivery_rate_bps, startup.delivery_rate_bps);

    let measured_bytes = 2 * 1024 * 1024_u64;
    stats.frame_rx.acks += 9;
    let confident = tracker.quic.observe(
        stats,
        with_acked_bytes_elapsed(
            with_delivery_evidence_written(
                congestion,
                PATH_OPEN_SCORE_BYTES as u64 + measured_bytes,
            ),
            measured_bytes,
            9,
            Duration::from_millis(200),
        ),
        2,
    );

    assert_eq!(
        confident.delivery_sample_count,
        QUIC_INITIAL_WINDOW_PACKETS as u64
    );
    assert!(confident.delivery_rate_bps < startup.delivery_rate_bps);
    let expected_rate = measured_bytes as f64 * 8.0 / 0.2;
    assert!(
        confident.delivery_rate_bps >= expected_rate * 0.95
            && confident.delivery_rate_bps <= expected_rate,
        "the first confident rate must replace, not maximize against, the unmeasured pacing prior: expected~{expected_rate} actual={}",
        confident.delivery_rate_bps,
    );
}

#[test]
fn quic_confidence_boundary_discards_inflated_preconfidence_sample() {
    let mut tracker = UdpPathMetricTracker::default();
    let congestion = quic_congestion(PATH_OPEN_SCORE_BYTES as u64, Some(500_000_000));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(50);
    stats.path.cwnd = PATH_OPEN_SCORE_BYTES as u64;
    stats.path.current_mtu = 1400;
    let startup = tracker.quic.observe(stats, congestion, 2);

    let fast_sample_bytes = 64 * 1024_u64;
    stats.frame_rx.acks = 1;
    let preconfidence = tracker.quic.observe(
        stats,
        with_acked_bytes_elapsed(
            with_delivery_evidence_written(congestion, fast_sample_bytes),
            fast_sample_bytes,
            1,
            Duration::from_millis(1),
        ),
        2,
    );
    assert_eq!(preconfidence.delivery_sample_count, 1);
    assert!(
        preconfidence.delivery_rate_bps > startup.delivery_rate_bps,
        "the setup must retain an inflated provisional sample before confidence"
    );

    let measured_bytes = 2 * 1024 * 1024_u64;
    stats.frame_rx.acks += 9;
    let confident = tracker.quic.observe(
        stats,
        with_acked_bytes_elapsed(
            with_delivery_evidence_written(
                congestion,
                fast_sample_bytes.saturating_add(measured_bytes),
            ),
            measured_bytes,
            9,
            Duration::from_millis(200),
        ),
        2,
    );

    let expected_rate = measured_bytes as f64 * 8.0 / 0.2;
    assert_eq!(
        confident.delivery_sample_count,
        QUIC_INITIAL_WINDOW_PACKETS as u64
    );
    assert!(
        confident.delivery_rate_bps >= expected_rate * 0.95
            && confident.delivery_rate_bps <= expected_rate,
        "confidence graduation must use the establishing sample, not retain a faster preconfidence outlier: expected~{expected_rate} actual={}",
        confident.delivery_rate_bps,
    );
}

#[test]
fn quic_confidence_requires_ack_samples_and_current_flight_volume() {
    let mut tracker = UdpPathMetricTracker::default();
    let startup_cwnd = PATH_OPEN_SCORE_BYTES as u64;
    let startup_congestion = quic_congestion(startup_cwnd, Some(500_000_000));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(50);
    stats.path.cwnd = startup_cwnd;
    stats.path.current_mtu = 1400;
    let startup = tracker.quic.observe(stats, startup_congestion, 2);
    let first = tracker.quic.observe(
        stats,
        with_acked_bytes(
            with_delivery_evidence_written(startup_congestion, startup_cwnd),
            startup_cwnd,
            1,
        ),
        2,
    );
    assert_eq!(first.delivery_sample_count, 1);

    let grown_cwnd = 4 * 1024 * 1024_u64;
    let tiny_followup = 9 * 1024_u64;
    let grown_congestion = quic_congestion(grown_cwnd, Some(500_000_000));
    stats.path.cwnd = grown_cwnd;
    let count_only = tracker.quic.observe(
        stats,
        with_acked_bytes(
            with_delivery_evidence_written(
                grown_congestion,
                startup_cwnd.saturating_add(tiny_followup),
            ),
            tiny_followup,
            9,
        ),
        2,
    );
    assert_eq!(
        count_only.delivery_sample_count,
        QUIC_INITIAL_WINDOW_PACKETS.saturating_sub(1) as u64,
        "sample count alone cannot graduate below the current carrier flight evidence floor"
    );
    assert_eq!(count_only.delivery_rate_bps, startup.delivery_rate_bps);
    let byte_confident = tracker.quic.observe(
        stats,
        with_acked_bytes(
            with_delivery_evidence_written(
                grown_congestion,
                startup_cwnd
                    .saturating_add(tiny_followup)
                    .saturating_add(grown_cwnd),
            ),
            grown_cwnd,
            1,
        ),
        2,
    );
    assert_eq!(
        byte_confident.delivery_sample_count,
        QUIC_INITIAL_WINDOW_PACKETS as u64
    );
    assert!(byte_confident.delivery_rate_bps < startup.delivery_rate_bps);
}

#[test]
fn quic_app_limited_duplicate_ack_counts_as_ack_data_seen_not_bulk_rate() {
    let mut tracker = UdpPathMetricTracker::default();
    let congestion = quic_congestion(4 * 1024 * 1024, Some(500_000_000));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(50);
    stats.path.cwnd = 4 * 1024 * 1024;
    stats.path.current_mtu = 1400;
    let _ = tracker.quic.observe(stats, congestion, 2);
    stats.frame_rx.acks = 1;
    let app_limited = tracker.quic.observe(
        stats,
        with_acked_bytes(
            with_delivery_evidence_written(congestion, 32 * 1024),
            32 * 1024,
            1,
        ),
        2,
    );
    let product_metrics = path_metrics_from_quic_path(PathId(7), app_limited);

    assert!(app_limited.ack_derived_data_seen);
    assert_eq!(app_limited.delivery_sample_count, 0);
    assert!(app_limited.app_limited);
    assert!(product_metrics.has_ack_derived_data_sample);
    assert_eq!(product_metrics.data_sample_count, 0);
}

#[test]
fn quic_server_metrics_publish_ack_data_seen_even_when_app_limited() {
    let metrics = UdpPathMetrics {
        direction: 2,
        srtt: Duration::from_millis(50),
        rttvar: Duration::from_millis(5),
        min_rtt: Duration::from_millis(45),
        min_rtt_observed: true,
        delivery_rate_bps: 500_000_000.0,
        pacing_rate_bps: 500_000_000.0,
        inflight_hi: 4 * 1024 * 1024,
        bytes_in_flight: 0,
        pending_bytes: 0,
        loss_ppm: None,
        ecn_ppm: None,
        app_limited: true,
        ack_derived_data_seen: true,
        delivery_sample_count: 0,
        delivery_sample_bytes: 0,
        last_delivery_sample_at: None,
        bulk_proof_expires_at: None,
        latest_delivery_sample_bytes: 0,
        latest_delivery_sample_count: 0,
        latest_carrier_ack_elapsed: None,
        latest_rate_sample_elapsed: None,
        capacity_proof_candidate: None,
        capacity_probe: None,
        #[cfg(feature = "lab-diagnostics")]
        ack_poll: QuicAckPollDiagnostics::default(),
    };

    assert!(quic_path_metrics_should_publish_local_sender(metrics));
    let product_metrics = path_metrics_from_quic_path(PathId(7), metrics);
    assert!(product_metrics.has_ack_derived_data_sample);
    assert_eq!(product_metrics.data_sample_count, 0);
    assert!(product_metrics.app_limited);
}

#[test]
fn quic_ack_after_prior_data_send_counts_as_ack_data_seen() {
    let mut tracker = UdpPathMetricTracker::default();
    let congestion = quic_congestion(4 * 1024 * 1024, Some(500_000_000));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(50);
    stats.path.cwnd = 4 * 1024 * 1024;
    stats.path.current_mtu = 1400;
    let _ = tracker.quic.observe(stats, congestion, 2);

    let sent_without_ack = tracker.quic.observe(
        stats,
        with_delivery_evidence_written(congestion, 32 * 1024),
        2,
    );
    assert!(!sent_without_ack.ack_derived_data_seen);
    let ack_after_send = tracker.quic.observe(
        stats,
        with_acked_bytes(
            with_delivery_evidence_written(congestion, 32 * 1024),
            32 * 1024,
            1,
        ),
        2,
    );

    assert!(
        ack_after_send.ack_derived_data_seen,
        "QUIC ACK-derived data evidence must survive normal TX/ACK timing; it cannot require TX and ACK in the same metrics poll"
    );
    assert_eq!(ack_after_send.delivery_sample_count, 0);
    assert!(ack_after_send.app_limited);
}

#[test]
fn quic_compressed_ack_sample_cannot_jump_beyond_startup_gain() {
    let mut tracker = UdpPathMetricTracker::default();
    let congestion = quic_congestion(4 * 1024 * 1024, Some(500_000_000));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(50);
    stats.path.cwnd = 4 * 1024 * 1024;
    stats.path.current_mtu = 1400;
    let startup = tracker.quic.observe(stats, congestion, 2);
    stats.frame_rx.acks = 64;
    let measured = tracker.quic.observe(
        stats,
        with_acked_bytes(
            with_delivery_evidence_written(congestion, 64 * 1024 * 1024),
            64 * 1024 * 1024,
            64,
        ),
        2,
    );

    assert_eq!(measured.delivery_sample_count, 64);
    assert!(measured.delivery_rate_bps <= startup.delivery_rate_bps * BBR_DEFAULT_CWND_GAIN);
}

#[test]
fn quic_lower_full_sample_smoothly_reduces_bulk_rate_model() {
    let mut tracker = UdpPathMetricTracker::default();
    let congestion = quic_congestion(512 * 1024, Some(100_000_000));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(50);
    stats.path.cwnd = 512 * 1024;
    stats.path.current_mtu = 1400;
    let _ = tracker.quic.observe(stats, congestion, 2);
    stats.udp_tx.bytes = 8 * 1024 * 1024;
    stats.frame_tx.stream = 512;
    stats.frame_rx.acks = 16;
    let raised = tracker.quic.observe(
        stats,
        with_acked_bytes(
            with_delivery_evidence_written(congestion, 8 * 1024 * 1024),
            8 * 1024 * 1024,
            16,
        ),
        2,
    );
    stats.udp_tx.bytes += 512 * 1024;
    stats.frame_tx.stream += 512;
    stats.frame_rx.acks += 16;
    let after_low = tracker.quic.observe(
        stats,
        with_acked_bytes(
            with_delivery_evidence_written(congestion, 8 * 1024 * 1024 + 512 * 1024),
            512 * 1024,
            16,
        ),
        2,
    );

    assert_eq!(after_low.delivery_sample_count, 32);
    let low_sample_rate = 512.0 * 1024.0 * 8.0 / 0.100;
    assert!(after_low.delivery_rate_bps < raised.delivery_rate_bps);
    assert!(after_low.delivery_rate_bps > low_sample_rate);
    assert!(after_low.delivery_rate_bps <= raised.delivery_rate_bps * 0.5);
    assert_eq!(
        after_low.delivery_rate_bps,
        raised
            .delivery_rate_bps
            .mul_add(0.25, low_sample_rate * 0.75)
    );
}
