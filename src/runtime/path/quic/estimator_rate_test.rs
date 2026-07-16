use super::super::estimator_test_support::*;
use super::*;

#[test]
fn quic_stats_feed_sender_side_udp_path_metrics() {
    let mut tracker = UdpPathMetricTracker::default();
    let congestion = quic_congestion(4 * 1024 * 1024, Some(500_000_000));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(50);
    stats.path.cwnd = 4 * 1024 * 1024;
    stats.path.current_mtu = 1400;

    let startup = tracker
        .quic
        .observe(stats, congestion, PathMetricDirection::ServerToClient);
    assert_eq!(startup.direction, PathMetricDirection::ServerToClient);
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
        PathMetricDirection::ServerToClient,
    );
    assert_eq!(measured.direction, PathMetricDirection::ServerToClient);
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
    let _ = fast_poll.observe_at(stats, congestion, PathMetricDirection::ServerToClient, base);
    let _ = slow_poll.observe_at(stats, congestion, PathMetricDirection::ServerToClient, base);
    let ack = with_acked_bytes_elapsed(
        with_delivery_evidence_written(congestion, sample_bytes),
        sample_bytes,
        QUIC_INITIAL_WINDOW_PACKETS as u64,
        Duration::from_millis(20),
    );

    let fast = fast_poll.observe_at(
        stats,
        ack,
        PathMetricDirection::ServerToClient,
        base + Duration::from_millis(10),
    );
    let slow = slow_poll.observe_at(
        stats,
        ack,
        PathMetricDirection::ServerToClient,
        base + Duration::from_millis(500),
    );

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
    let startup = tracker.observe(stats, congestion, PathMetricDirection::ServerToClient);

    let untimed = tracker.observe(
        stats,
        with_acked_bytes_elapsed(
            with_delivery_evidence_written(congestion, sample_bytes),
            sample_bytes,
            QUIC_INITIAL_WINDOW_PACKETS as u64,
            Duration::ZERO,
        ),
        PathMetricDirection::ServerToClient,
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
    let _ = tracker.observe(stats, congestion, PathMetricDirection::ServerToClient);

    let mut combined = with_delivery_evidence_written(congestion, total_bytes);
    combined.newly_acked_bytes = Some(total_bytes);
    combined.non_app_limited_acked_bytes = Some(total_bytes);
    combined.timed_non_app_limited_acked_bytes = Some(timed_bytes);
    combined.non_app_limited_ack_elapsed = Some(Duration::from_millis(20));
    combined.delivery_sample_count = (QUIC_INITIAL_WINDOW_PACKETS * 2) as u64;
    combined.non_app_limited_delivery_sample_count = (QUIC_INITIAL_WINDOW_PACKETS * 2) as u64;
    combined.timed_non_app_limited_delivery_sample_count = QUIC_INITIAL_WINDOW_PACKETS as u64;
    combined.app_limited = false;

    let measured = tracker.observe(stats, combined, PathMetricDirection::ServerToClient);

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
    let _ = tracker.observe(stats, congestion, PathMetricDirection::ServerToClient);

    let first = tracker.observe(
        stats,
        with_acked_bytes_elapsed(
            with_delivery_evidence_written(congestion, sample_bytes),
            chunk_bytes,
            (QUIC_INITIAL_WINDOW_PACKETS / 2) as u64,
            Duration::from_millis(20),
        ),
        PathMetricDirection::ServerToClient,
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
        PathMetricDirection::ServerToClient,
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
    let _ = tracker
        .quic
        .observe(stats, congestion, PathMetricDirection::ClientToServer);

    stats.frame_rx.acks = 1;
    let ack_only = tracker
        .quic
        .observe(stats, congestion, PathMetricDirection::ClientToServer);
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
    let _ = tracker
        .quic
        .observe(stats, congestion, PathMetricDirection::ServerToClient);
    let tx_only = tracker.quic.observe(
        stats,
        with_delivery_evidence_written(congestion, 8 * 1024 * 1024),
        PathMetricDirection::ServerToClient,
    );

    assert_eq!(tx_only.delivery_sample_count, 0);
    assert!(tx_only.last_delivery_sample_at.is_none());
    assert_eq!(tx_only.delivery_rate_bps.round() as u64, 500_000_000);
}

#[test]
fn quic_unknown_capacity_ack_sample_does_not_create_bulk_evidence() {
    let mut tracker = UdpPathMetricTracker::default();
    let congestion = quic_congestion(0, None);
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(50);

    let _ = tracker
        .quic
        .observe(stats, congestion, PathMetricDirection::ServerToClient);
    stats.frame_rx.acks = 1;
    let unknown_capacity = tracker.quic.observe(
        stats,
        with_acked_bytes(with_delivery_evidence_written(congestion, 4096), 4096, 1),
        PathMetricDirection::ServerToClient,
    );

    assert_eq!(unknown_capacity.delivery_sample_count, 0);
    assert!(unknown_capacity.last_delivery_sample_at.is_none());
    assert_eq!(
        unknown_capacity.delivery_rate_bps.round() as u64,
        default_path_rate_bps().round() as u64
    );
    assert!(unknown_capacity.app_limited);
}

#[test]
fn quic_tiny_startup_pacing_does_not_poison_product_scheduler_rate() {
    let mut tracker = UdpPathMetricTracker::default();
    let congestion = quic_congestion(0, Some(4));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(50);

    let startup = tracker
        .quic
        .observe(stats, congestion, PathMetricDirection::ServerToClient);
    let udp_startup_rate = default_path_rate_bps().round() as u64;

    assert_eq!(startup.delivery_sample_count, 0);
    assert!(startup.last_delivery_sample_at.is_none());
    assert_eq!(startup.delivery_rate_bps.round() as u64, udp_startup_rate);
    assert_eq!(startup.pacing_rate_bps.round() as u64, udp_startup_rate);
    stats.frame_rx.acks = 1;
    let app_limited = tracker.quic.observe(
        stats,
        with_delivery_evidence_written(congestion, 4096),
        PathMetricDirection::ServerToClient,
    );

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
fn quic_app_limited_low_ack_sample_does_not_poison_delivery_rate() {
    let mut tracker = UdpPathMetricTracker::default();
    let congestion = quic_congestion(4 * 1024 * 1024, Some(500_000_000));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(50);
    stats.path.cwnd = 4 * 1024 * 1024;
    stats.path.current_mtu = 1400;
    let _ = tracker
        .quic
        .observe(stats, congestion, PathMetricDirection::ServerToClient);
    stats.frame_rx.acks = 1;
    let app_limited = tracker.quic.observe(
        stats,
        with_acked_bytes(
            with_delivery_evidence_written(congestion, 32 * 1024),
            32 * 1024,
            1,
        ),
        PathMetricDirection::ServerToClient,
    );

    assert_eq!(app_limited.delivery_sample_count, 0);
    assert!(app_limited.last_delivery_sample_at.is_none());
    assert_eq!(app_limited.delivery_rate_bps.round() as u64, 500_000_000);
    assert!(app_limited.app_limited);

    let mut changed_pacing = congestion;
    changed_pacing.pacing_rate_bps = Some(750_000_000);
    let refreshed_prior =
        tracker
            .quic
            .observe(stats, changed_pacing, PathMetricDirection::ServerToClient);
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
    let startup = tracker
        .quic
        .observe(stats, congestion, PathMetricDirection::ServerToClient);
    stats.frame_rx.acks = 1;
    let measured = tracker.quic.observe(
        stats,
        with_acked_bytes_elapsed(
            with_delivery_evidence_written(congestion, PATH_OPEN_SCORE_BYTES as u64),
            PATH_OPEN_SCORE_BYTES as u64,
            1,
            Duration::from_millis(1000),
        ),
        PathMetricDirection::ServerToClient,
    );

    assert_eq!(measured.delivery_sample_count, 1);
    assert_eq!(
        measured.delivery_rate_bps.round() as u64,
        startup.delivery_rate_bps.round() as u64,
        "a single underfed measurement quantum must not replace the startup/pacing fallback with a tiny rate"
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
    let _ = tracker
        .quic
        .observe(stats, congestion, PathMetricDirection::ServerToClient);
    let mut polled = with_acked_bytes(
        with_delivery_evidence_written(congestion, sample_bytes),
        sample_bytes,
        QUIC_INITIAL_WINDOW_PACKETS as u64,
    );
    polled.app_limited = true;
    let measured = tracker
        .quic
        .observe(stats, polled, PathMetricDirection::ServerToClient);

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
    let _ = tracker
        .quic
        .observe(stats, congestion, PathMetricDirection::ServerToClient);

    let mut measured = None;
    for _ in 0..8 {
        measured = Some(tracker.quic.observe(
            stats,
            with_acked_bytes(
                with_delivery_evidence_written(congestion, sample_bytes),
                chunk_bytes,
                2,
            ),
            PathMetricDirection::ServerToClient,
        ));
    }
    let measured = measured.expect("split measurement sample");
    assert_eq!(measured.delivery_sample_bytes, sample_bytes);
    assert!(!measured.app_limited);

    let idle = tracker.quic.observe(
        stats,
        with_delivery_evidence_written(congestion, sample_bytes),
        PathMetricDirection::ServerToClient,
    );
    assert!(
        !idle.app_limited,
        "an idle metrics poll inside the 3-PTO horizon must preserve capacity evidence"
    );
}

#[test]
fn quic_ack_after_prior_data_send_counts_as_ack_data_seen() {
    let mut tracker = UdpPathMetricTracker::default();
    let congestion = quic_congestion(4 * 1024 * 1024, Some(500_000_000));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(50);
    stats.path.cwnd = 4 * 1024 * 1024;
    stats.path.current_mtu = 1400;
    let _ = tracker
        .quic
        .observe(stats, congestion, PathMetricDirection::ServerToClient);

    let sent_without_ack = tracker.quic.observe(
        stats,
        with_delivery_evidence_written(congestion, 32 * 1024),
        PathMetricDirection::ServerToClient,
    );
    assert!(!sent_without_ack.ack_derived_data_seen);
    let ack_after_send = tracker.quic.observe(
        stats,
        with_acked_bytes(
            with_delivery_evidence_written(congestion, 32 * 1024),
            32 * 1024,
            1,
        ),
        PathMetricDirection::ServerToClient,
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
    let startup = tracker
        .quic
        .observe(stats, congestion, PathMetricDirection::ServerToClient);
    stats.frame_rx.acks = 64;
    let measured = tracker.quic.observe(
        stats,
        with_acked_bytes(
            with_delivery_evidence_written(congestion, 64 * 1024 * 1024),
            64 * 1024 * 1024,
            64,
        ),
        PathMetricDirection::ServerToClient,
    );

    assert_eq!(measured.delivery_sample_count, 64);
    assert!(measured.delivery_rate_bps <= startup.delivery_rate_bps * RELIABLE_PIPE_WINDOW_BDPS);
}

#[test]
fn quic_lower_full_sample_smoothly_reduces_bulk_rate_model() {
    let mut tracker = UdpPathMetricTracker::default();
    let congestion = quic_congestion(512 * 1024, Some(100_000_000));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(50);
    stats.path.cwnd = 512 * 1024;
    stats.path.current_mtu = 1400;
    let _ = tracker
        .quic
        .observe(stats, congestion, PathMetricDirection::ServerToClient);
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
        PathMetricDirection::ServerToClient,
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
        PathMetricDirection::ServerToClient,
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
