use super::super::estimator_test_support::*;
use super::*;

#[test]
fn new_quic_path_epoch_discards_rate_confidence_and_bulk_proof() {
    let mut tracker = UdpPathMetricTracker::default();
    let congestion = quic_congestion(8 * 1024 * 1024, Some(500_000_000));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(50);
    stats.path.cwnd = congestion.congestion_window;
    stats.path.current_mtu = 1400;
    let startup = tracker
        .quic
        .observe(stats, congestion, PathMetricDirection::ServerToClient);

    let sample_bytes = 8 * 1024 * 1024_u64;
    let established = tracker.quic.observe(
        stats,
        with_acked_bytes_elapsed(
            with_delivery_evidence_written(congestion, sample_bytes),
            sample_bytes,
            QUIC_INITIAL_WINDOW_PACKETS as u64,
            Duration::from_millis(200),
        ),
        PathMetricDirection::ServerToClient,
    );
    assert!(established.delivery_sample_count >= QUIC_INITIAL_WINDOW_PACKETS as u64);
    assert!(established.delivery_sample_bytes >= sample_bytes);
    assert!(established.bulk_proof_expires_at.is_some());

    let mut migrated = with_delivery_evidence_written(congestion, sample_bytes);
    migrated.path_epoch += 1;
    let reset = tracker
        .quic
        .observe(stats, migrated, PathMetricDirection::ServerToClient);

    assert_eq!(reset.delivery_sample_count, 0);
    assert_eq!(reset.delivery_sample_bytes, 0);
    assert!(!reset.ack_derived_data_seen);
    assert_eq!(reset.last_delivery_sample_at, None);
    assert_eq!(reset.bulk_proof_expires_at, None);
    assert_eq!(reset.delivery_rate_bps, startup.delivery_rate_bps);
}

#[test]
fn new_quic_path_epoch_discards_unpublished_delivery_clock_acquisition() {
    let sample_floor = RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES / 2;
    let fragment_bytes = 16 * 1024_u64;
    let congestion = quic_congestion(sample_floor, Some(100_000_000));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(50);
    stats.path.cwnd = sample_floor;
    stats.path.current_mtu = 1400;
    let mut tracker = QuicPathMetricTracker::default();
    let _ = tracker.observe(stats, congestion, PathMetricDirection::ServerToClient);
    let carrier = with_acked_bytes(
        with_delivery_evidence_written(congestion, fragment_bytes),
        fragment_bytes,
        1,
    );
    let _ = tracker.observe(stats, carrier, PathMetricDirection::ServerToClient);
    assert_eq!(
        tracker
            .pending_delivery_sample
            .expect("old-path pending acquisition")
            .sample_bytes,
        fragment_bytes
    );

    let mut migrated =
        with_delivery_clock_epoch(with_delivery_evidence_written(carrier, fragment_bytes), 1);
    migrated.path_epoch += 1;
    let reset = tracker.observe(stats, migrated, PathMetricDirection::ServerToClient);

    assert!(tracker.pending_delivery_sample.is_none());
    assert_eq!(reset.delivery_sample_count, 0);
    assert_eq!(reset.delivery_sample_bytes, 0);
    assert!(reset.bulk_proof_expires_at.is_none());
}

#[test]
fn app_limited_delivery_clock_boundary_discards_only_old_pending_epoch() {
    let sample_floor = RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES / 2;
    let fragment_bytes = 16 * 1024_u64;
    let congestion = quic_congestion(sample_floor, Some(100_000_000));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(50);
    stats.path.cwnd = sample_floor;
    stats.path.current_mtu = 1400;
    let mut tracker = QuicPathMetricTracker::default();
    let _ = tracker.observe(stats, congestion, PathMetricDirection::ServerToClient);
    let mut carrier = with_acked_bytes(
        with_delivery_evidence_written(congestion, fragment_bytes),
        fragment_bytes,
        1,
    );
    let _ = tracker.observe(stats, carrier, PathMetricDirection::ServerToClient);

    carrier = with_delivery_evidence_written(carrier, fragment_bytes);
    let idle = tracker.observe(stats, carrier, PathMetricDirection::ServerToClient);
    assert!(idle.app_limited);
    assert_eq!(
        tracker
            .pending_delivery_sample
            .expect("idle flag alone is not a clock boundary")
            .sample_bytes,
        fragment_bytes
    );

    carrier = with_delivery_clock_epoch(
        with_delivery_evidence_written(carrier, fragment_bytes * 2),
        2,
    );
    carrier = with_acked_bytes(carrier, fragment_bytes, 1);
    let _ = tracker.observe(stats, carrier, PathMetricDirection::ServerToClient);
    let pending = tracker
        .pending_delivery_sample
        .expect("new delivery clock starts its own acquisition");
    assert_eq!(pending.key.delivery_clock_epoch, 2);
    assert_eq!(pending.sample_bytes, fragment_bytes);
    assert_eq!(pending.sample_count, 1);
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
    assert!(
        still_fresh.app_limited,
        "an idle carrier remains app-limited even while its independent bulk proof is fresh"
    );
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
fn quic_expiry_resets_committed_epoch_but_preserves_same_clock_pending() {
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
    let mut carrier = with_acked_bytes(
        with_delivery_evidence_written(congestion, sample_bytes),
        sample_bytes,
        QUIC_INITIAL_WINDOW_PACKETS as u64,
    );
    let proven = tracker.observe_at(
        stats,
        carrier,
        PathMetricDirection::ServerToClient,
        proof_at,
    );
    let deadline = proven.bulk_proof_expires_at.expect("proof deadline");
    let written_bytes = sample_bytes.saturating_mul(3);
    carrier = with_acked_bytes(
        with_delivery_evidence_written(carrier, written_bytes),
        fragment_bytes,
        2,
    );
    let _ = tracker.observe_at(
        stats,
        carrier,
        PathMetricDirection::ServerToClient,
        deadline - QUIC_TIMER_GRANULARITY,
    );
    assert_eq!(
        tracker
            .pending_delivery_sample
            .expect("pre-expiry pending clock epoch")
            .sample_bytes,
        fragment_bytes
    );

    carrier = with_delivery_evidence_written(carrier, written_bytes);
    let expired = tracker.observe_at(
        stats,
        carrier,
        PathMetricDirection::ServerToClient,
        deadline,
    );
    assert!(expired.app_limited);
    assert_eq!(expired.delivery_sample_count, 0);
    assert_eq!(expired.delivery_sample_bytes, 0);
    let pending = tracker
        .pending_delivery_sample
        .expect("same-clock acquisition survives unrelated proof expiry");
    assert_eq!(pending.sample_bytes, fragment_bytes);
    assert_eq!(pending.sample_count, 2);
    assert!(!pending.sample_elapsed.is_zero());

    let post_expiry_at = deadline + QUIC_TIMER_GRANULARITY;
    carrier = with_acked_bytes(
        with_delivery_evidence_written(carrier, written_bytes + sample_bytes),
        sample_bytes,
        1,
    );
    let post_expiry = tracker.observe_at(
        stats,
        carrier,
        PathMetricDirection::ServerToClient,
        post_expiry_at,
    );
    assert_eq!(post_expiry.delivery_sample_count, 3);
    assert_eq!(
        post_expiry.delivery_sample_bytes,
        sample_bytes + fragment_bytes
    );
    assert!(
        post_expiry.delivery_sample_count < QUIC_INITIAL_WINDOW_PACKETS as u64,
        "fresh committed confidence is built only from the completed same-clock pending sample"
    );
    assert_eq!(post_expiry.last_delivery_sample_at, Some(post_expiry_at));
    assert!(post_expiry.bulk_proof_expires_at.is_some());
}

#[test]
fn quic_sustained_drained_subwindow_fragments_reestablish_bulk_proof_after_expiry() {
    // A bulk sender can drain one 16--64 KiB Product fragment between metric
    // polls while remaining continuously active. Those ACKs must eventually
    // establish a new native-rate epoch even when no individual callback
    // covers the 256-KiB carrier window by itself.
    let mut trapped_fragments = Vec::new();
    for fragment_bytes in [16 * 1024_u64, 32 * 1024, 64 * 1024] {
        let sample_floor = RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES / 2;
        let congestion = quic_congestion(sample_floor, Some(100_000_000));
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(100);
        stats.path.cwnd = sample_floor;
        stats.path.current_mtu = 1400;
        let base = Instant::now();
        let proof_at = base + Duration::from_millis(1);
        let mut tracker = QuicPathMetricTracker::default();
        let _ = tracker.observe_at(stats, congestion, PathMetricDirection::ServerToClient, base);
        let mut carrier = with_acked_bytes_elapsed(
            with_delivery_evidence_written(congestion, sample_floor),
            sample_floor,
            QUIC_INITIAL_WINDOW_PACKETS as u64,
            Duration::from_millis(100),
        );
        let proven = tracker.observe_at(
            stats,
            carrier,
            PathMetricDirection::ServerToClient,
            proof_at,
        );
        let first_deadline = proven.bulk_proof_expires_at.expect("initial bulk proof");
        let mut written_bytes = sample_floor;
        let mut observed = proven;
        let poll_interval = Duration::from_millis(50);
        let mut observed_at = proof_at;
        let end_at = first_deadline + poll_interval * 16;
        let mut post_expiry_fragments = 0_u32;
        let mut reproved_after_first_deadline = false;
        while observed_at < end_at {
            observed_at += poll_interval;
            written_bytes = written_bytes.saturating_add(fragment_bytes);
            stats.frame_rx.acks = stats.frame_rx.acks.saturating_add(1);
            carrier = with_acked_bytes_elapsed(
                with_delivery_evidence_written(carrier, written_bytes),
                fragment_bytes,
                1,
                poll_interval,
            );
            observed = tracker.observe_at(
                stats,
                carrier,
                PathMetricDirection::ServerToClient,
                observed_at,
            );
            if observed_at > first_deadline {
                post_expiry_fragments = post_expiry_fragments.saturating_add(1);
                reproved_after_first_deadline |= observed
                    .last_delivery_sample_at
                    .is_some_and(|sample_at| sample_at > first_deadline);
            }
        }

        assert!(observed.ack_derived_data_seen);
        if post_expiry_fragments < 16 || !reproved_after_first_deadline {
            trapped_fragments.push((
                fragment_bytes,
                observed.delivery_sample_count,
                observed.delivery_sample_bytes,
            ));
        }
    }
    assert!(
        trapped_fragments.is_empty(),
        "continuous non-app-limited ACK fragments remained trapped without a fresh native-rate proof after more than two proof horizons: {trapped_fragments:?}"
    );
}

#[test]
fn quic_full_window_and_poll_partitions_publish_identical_clock_sample() {
    fn observe_partition(fragment_bytes: u64, base: Instant) -> UdpPathMetrics {
        let sample_bytes = RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES / 2;
        let total_sample_count = 16_u64;
        let total_elapsed = Duration::from_millis(160);
        let fragment_count = u32::try_from(sample_bytes / fragment_bytes).unwrap();
        let fragment_sample_count = total_sample_count / u64::from(fragment_count);
        let fragment_elapsed = total_elapsed / fragment_count;
        let congestion = quic_congestion(sample_bytes, Some(100_000_000));
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(100);
        stats.path.cwnd = sample_bytes;
        stats.path.current_mtu = 1400;
        let mut tracker = QuicPathMetricTracker::default();
        let _ = tracker.observe_at(stats, congestion, PathMetricDirection::ServerToClient, base);
        let mut carrier = congestion;
        let mut written_bytes = 0_u64;
        let mut observed = None;
        for fragment_index in 1..=fragment_count {
            written_bytes = written_bytes.saturating_add(fragment_bytes);
            carrier = with_acked_bytes_elapsed(
                with_delivery_evidence_written(carrier, written_bytes),
                fragment_bytes,
                fragment_sample_count,
                fragment_elapsed,
            );
            observed = Some(tracker.observe_at(
                stats,
                carrier,
                PathMetricDirection::ServerToClient,
                base + fragment_elapsed * fragment_index,
            ));
        }
        observed.expect("partition publishes a final metric")
    }

    let base = Instant::now();
    let full = observe_partition(RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES / 2, base);
    for fragment_bytes in [16 * 1024_u64, 32 * 1024, 64 * 1024] {
        let partitioned = observe_partition(fragment_bytes, base);
        assert_eq!(
            partitioned.delivery_sample_bytes,
            full.delivery_sample_bytes
        );
        assert_eq!(
            partitioned.delivery_sample_count,
            full.delivery_sample_count
        );
        assert_eq!(
            partitioned.latest_delivery_sample_bytes,
            full.latest_delivery_sample_bytes
        );
        assert_eq!(
            partitioned.latest_delivery_sample_count,
            full.latest_delivery_sample_count
        );
        assert_eq!(
            partitioned.latest_carrier_ack_elapsed,
            full.latest_carrier_ack_elapsed
        );
        assert_eq!(
            partitioned.delivery_rate_bps.to_bits(),
            full.delivery_rate_bps.to_bits()
        );
        assert_eq!(
            partitioned.last_delivery_sample_at,
            full.last_delivery_sample_at
        );
        assert_eq!(
            partitioned.bulk_proof_expires_at,
            full.bulk_proof_expires_at
        );
    }
}

#[test]
fn pending_delivery_sample_keeps_frozen_floor_while_live_cwnd_grows() {
    let initial_floor = 64 * 1024_u64;
    let fragment_bytes = 16 * 1024_u64;
    let mut carrier = quic_congestion(initial_floor, Some(100_000_000));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(50);
    stats.path.cwnd = initial_floor;
    stats.path.current_mtu = 1400;
    let mut tracker = QuicPathMetricTracker::default();
    let _ = tracker.observe(stats, carrier, PathMetricDirection::ServerToClient);
    carrier = with_acked_bytes(
        with_delivery_evidence_written(carrier, fragment_bytes),
        fragment_bytes,
        1,
    );
    let first = tracker.observe(stats, carrier, PathMetricDirection::ServerToClient);
    assert_eq!(first.delivery_sample_count, 0);
    let pending = tracker
        .pending_delivery_sample
        .expect("first fragment freezes acquisition geometry");
    assert_eq!(pending.publish_floor, initial_floor);
    assert_eq!(pending.durable_floor, initial_floor);

    let grown_cwnd = RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES / 2;
    stats.path.cwnd = grown_cwnd;
    carrier.congestion_window = grown_cwnd;
    for fragment_index in 2..=4_u64 {
        carrier = with_acked_bytes(
            with_delivery_evidence_written(carrier, fragment_index * fragment_bytes),
            fragment_bytes,
            1,
        );
        let measured = tracker.observe(stats, carrier, PathMetricDirection::ServerToClient);
        if fragment_index < 4 {
            assert_eq!(measured.delivery_sample_count, 0);
        } else {
            assert_eq!(measured.delivery_sample_bytes, initial_floor);
            assert_eq!(measured.delivery_sample_count, 4);
            assert!(measured.bulk_proof_expires_at.is_some());
        }
    }
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
    assert!(fresh.app_limited);
    assert!(fresh.bulk_proof_expires_at.is_some());
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
    assert_eq!(tracker.delivery_rate_bps, Some(proven.delivery_rate_bps));
    assert_eq!(
        aged.delivery_rate_bps,
        congestion.pacing_rate_bps.expect("test pacing") as f64,
        "without current confidence, projection returns to the transport startup rate while retaining estimator history internally"
    );
    assert_eq!(aged.delivery_sample_count, 0);
    assert_eq!(aged.delivery_sample_bytes, 0);
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
    assert_eq!(tracker.delivery_rate_bps, Some(reproved.delivery_rate_bps));
    assert_eq!(
        aged_again.delivery_rate_bps,
        congestion.pacing_rate_bps.expect("test pacing") as f64
    );
    assert_eq!(aged_again.delivery_sample_count, 0);
    assert_eq!(aged_again.delivery_sample_bytes, 0);
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
    let mut carrier = with_acked_bytes(
        with_delivery_evidence_written(congestion, PATH_OPEN_SCORE_BYTES as u64),
        PATH_OPEN_SCORE_BYTES as u64,
        1,
    );
    let first_quantum = tracker
        .quic
        .observe(stats, carrier, PathMetricDirection::ServerToClient);
    assert_eq!(first_quantum.delivery_sample_count, 1);
    assert_eq!(first_quantum.delivery_rate_bps, startup.delivery_rate_bps);

    let measured_bytes = 2 * 1024 * 1024_u64;
    stats.frame_rx.acks += 9;
    carrier = with_acked_bytes_elapsed(
        with_delivery_evidence_written(carrier, PATH_OPEN_SCORE_BYTES as u64 + measured_bytes),
        measured_bytes,
        9,
        Duration::from_millis(200),
    );
    let confident = tracker
        .quic
        .observe(stats, carrier, PathMetricDirection::ServerToClient);

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
    let mut carrier = with_acked_bytes_elapsed(
        with_delivery_evidence_written(congestion, fast_sample_bytes),
        fast_sample_bytes,
        1,
        Duration::from_millis(1),
    );
    let preconfidence = tracker
        .quic
        .observe(stats, carrier, PathMetricDirection::ServerToClient);
    assert_eq!(preconfidence.delivery_sample_count, 1);
    assert!(
        preconfidence.delivery_rate_bps > startup.delivery_rate_bps,
        "the setup must retain an inflated provisional sample before confidence"
    );

    let measured_bytes = 2 * 1024 * 1024_u64;
    stats.frame_rx.acks += 9;
    carrier = with_acked_bytes_elapsed(
        with_delivery_evidence_written(carrier, fast_sample_bytes.saturating_add(measured_bytes)),
        measured_bytes,
        9,
        Duration::from_millis(200),
    );
    let confident = tracker
        .quic
        .observe(stats, carrier, PathMetricDirection::ServerToClient);

    let expected_rate = measured_bytes as f64 * 8.0 / 0.2;
    assert_eq!(
        confident.delivery_sample_count,
        QUIC_INITIAL_WINDOW_PACKETS as u64
    );
    assert!(
        confident.delivery_rate_bps >= expected_rate * 0.95
            && confident.delivery_rate_bps <= expected_rate,
        "confidence capacity_admission must use the establishing sample, not retain a faster preconfidence outlier: expected~{expected_rate} actual={}",
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
    let mut carrier = with_acked_bytes(
        with_delivery_evidence_written(startup_congestion, startup_cwnd),
        startup_cwnd,
        1,
    );
    let first = tracker
        .quic
        .observe(stats, carrier, PathMetricDirection::ServerToClient);
    assert_eq!(first.delivery_sample_count, 1);

    let grown_cwnd = 4 * 1024 * 1024_u64;
    let tiny_followup = 9 * 1024_u64;
    carrier.congestion_window = grown_cwnd;
    stats.path.cwnd = grown_cwnd;
    carrier = with_acked_bytes(
        with_delivery_evidence_written(carrier, startup_cwnd.saturating_add(tiny_followup)),
        tiny_followup,
        9,
    );
    let count_only = tracker
        .quic
        .observe(stats, carrier, PathMetricDirection::ServerToClient);
    assert_eq!(
        count_only.delivery_sample_count, 1,
        "subfloor samples stay pending and cannot change committed confidence"
    );
    assert_eq!(
        tracker
            .quic
            .pending_delivery_sample
            .expect("subfloor count remains in the current clock acquisition")
            .sample_count,
        9
    );
    assert_eq!(count_only.delivery_rate_bps, startup.delivery_rate_bps);
    carrier = with_acked_bytes(
        with_delivery_evidence_written(
            carrier,
            startup_cwnd
                .saturating_add(tiny_followup)
                .saturating_add(grown_cwnd),
        ),
        grown_cwnd,
        1,
    );
    let byte_confident = tracker
        .quic
        .observe(stats, carrier, PathMetricDirection::ServerToClient);
    assert_eq!(
        byte_confident.delivery_sample_count,
        QUIC_INITIAL_WINDOW_PACKETS as u64 + 1
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
    let mut duplicate_ack = with_delivery_evidence_written(congestion, 32 * 1024);
    duplicate_ack.newly_acked_bytes = Some(32 * 1024);
    duplicate_ack.delivery_evidence_newly_acked_bytes = Some(32 * 1024);
    duplicate_ack.delivery_sample_count = 1;
    let app_limited =
        tracker
            .quic
            .observe(stats, duplicate_ack, PathMetricDirection::ServerToClient);
    assert!(app_limited.ack_derived_data_seen);
    assert_eq!(app_limited.delivery_sample_count, 0);
    assert!(app_limited.app_limited);
}
