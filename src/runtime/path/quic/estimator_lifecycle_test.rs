use super::super::estimator_test_support::*;
use super::*;

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
    let _ = tracker.observe_at(stats, congestion, PathMetricDirection::ServerToClient, base);
    let proven = tracker.observe_at(
        stats,
        with_acked_bytes(
            with_delivery_evidence_written(congestion, sample_bytes),
            sample_bytes,
            QUIC_INITIAL_WINDOW_PACKETS as u64,
        ),
        PathMetricDirection::ServerToClient,
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
        PathMetricDirection::ServerToClient,
        proof_at + smaller_horizon,
    );
    assert!(!still_fresh.app_limited);
    assert_eq!(still_fresh.bulk_proof_expires_at, Some(frozen_deadline));

    let expired = tracker.observe_at(
        stats,
        with_delivery_evidence_written(congestion, sample_bytes),
        PathMetricDirection::ServerToClient,
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
    let _ = tracker.observe_at(stats, congestion, PathMetricDirection::ServerToClient, base);
    let proven = tracker.observe_at(
        stats,
        with_acked_bytes(
            with_delivery_evidence_written(congestion, sample_bytes),
            sample_bytes,
            QUIC_INITIAL_WINDOW_PACKETS as u64,
        ),
        PathMetricDirection::ServerToClient,
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
        PathMetricDirection::ServerToClient,
        deadline - QUIC_TIMER_GRANULARITY,
    );
    assert_eq!(tracker.pending_non_app_limited_sample_bytes, fragment_bytes);

    let expired = tracker.observe_at(
        stats,
        with_delivery_evidence_written(congestion, written_bytes),
        PathMetricDirection::ServerToClient,
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
    let _ = tracker.observe_at(stats, congestion, PathMetricDirection::ServerToClient, base);
    let proven = tracker.observe_at(
        stats,
        with_acked_bytes(
            with_delivery_evidence_written(congestion, sample_bytes),
            sample_bytes,
            QUIC_INITIAL_WINDOW_PACKETS as u64,
        ),
        PathMetricDirection::ServerToClient,
        proof_at,
    );

    assert!(!proven.app_limited);
    let fresh = tracker.observe_at(
        stats,
        with_delivery_evidence_written(congestion, sample_bytes),
        PathMetricDirection::ServerToClient,
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
    let _ = tracker.observe_at(stats, congestion, PathMetricDirection::ServerToClient, base);
    let proven = tracker.observe_at(
        stats,
        with_acked_bytes(
            with_delivery_evidence_written(congestion, sample_bytes),
            sample_bytes,
            QUIC_INITIAL_WINDOW_PACKETS as u64,
        ),
        PathMetricDirection::ServerToClient,
        proof_at,
    );
    assert!(proven.ack_derived_data_seen);

    let aged = tracker.observe_at(
        stats,
        with_delivery_evidence_written(congestion, sample_bytes),
        PathMetricDirection::ServerToClient,
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
    let _ = tracker.observe_at(stats, congestion, PathMetricDirection::ServerToClient, base);
    let _ = tracker.observe_at(
        stats,
        with_acked_bytes(
            with_delivery_evidence_written(congestion, sample_bytes),
            sample_bytes,
            QUIC_INITIAL_WINDOW_PACKETS as u64,
        ),
        PathMetricDirection::ServerToClient,
        first_proof_at,
    );
    let _ = tracker.observe_at(
        stats,
        with_delivery_evidence_written(congestion, sample_bytes),
        PathMetricDirection::ServerToClient,
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
        PathMetricDirection::ServerToClient,
        second_proof_at,
    );
    assert!(!reproved.app_limited);
    assert!(reproved.delivery_sample_count > 0);

    let aged_again = tracker.observe_at(
        stats,
        with_delivery_evidence_written(congestion, sample_bytes * 2),
        PathMetricDirection::ServerToClient,
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
    let startup = tracker
        .quic
        .observe(stats, congestion, PathMetricDirection::ServerToClient);
    stats.frame_rx.acks = 1;
    let first_quantum = tracker.quic.observe(
        stats,
        with_acked_bytes(
            with_delivery_evidence_written(congestion, PATH_OPEN_SCORE_BYTES as u64),
            PATH_OPEN_SCORE_BYTES as u64,
            1,
        ),
        PathMetricDirection::ServerToClient,
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
        PathMetricDirection::ServerToClient,
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
    let startup = tracker
        .quic
        .observe(stats, congestion, PathMetricDirection::ServerToClient);

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
        PathMetricDirection::ServerToClient,
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
        PathMetricDirection::ServerToClient,
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
    let startup = tracker.quic.observe(
        stats,
        startup_congestion,
        PathMetricDirection::ServerToClient,
    );
    let first = tracker.quic.observe(
        stats,
        with_acked_bytes(
            with_delivery_evidence_written(startup_congestion, startup_cwnd),
            startup_cwnd,
            1,
        ),
        PathMetricDirection::ServerToClient,
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
        PathMetricDirection::ServerToClient,
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
        PathMetricDirection::ServerToClient,
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
    assert!(app_limited.ack_derived_data_seen);
    assert_eq!(app_limited.delivery_sample_count, 0);
    assert!(app_limited.app_limited);
}
