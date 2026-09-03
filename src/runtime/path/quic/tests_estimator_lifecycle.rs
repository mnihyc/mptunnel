use super::super::estimator_test_support::*;
use super::*;

#[test]
fn new_quic_path_epoch_resets_native_ack_clock_without_inheriting_product_proof() {
    let congestion = quic_congestion(512 * 1024, Some(100_000_000));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(50);
    stats.path.cwnd = congestion.congestion_window;
    stats.path.current_mtu = 1400;
    let mut tracker = QuicPathMetricTracker::default();
    let _ = tracker.observe(stats, congestion, PathMetricDirection::ServerToClient);
    let current = tracker.observe(
        stats,
        with_acked_bytes_elapsed(congestion, 128 * 1024, 4, Duration::from_millis(20)),
        PathMetricDirection::ServerToClient,
    );
    assert_eq!(
        current.latest_carrier_ack_elapsed,
        Some(Duration::from_millis(20))
    );

    let mut migrated = with_acked_bytes_elapsed(congestion, 32 * 1024, 1, Duration::from_millis(7));
    migrated.path_epoch += 1;
    let reset = tracker.observe(stats, migrated, PathMetricDirection::ServerToClient);

    assert_eq!(reset.controller_path_epoch, migrated.path_epoch);
    assert_eq!(
        reset.latest_carrier_ack_elapsed,
        Some(Duration::from_millis(7))
    );
    assert!(!reset.ack_derived_data_seen);
    assert_eq!(reset.delivery_sample_count, 0);
    assert_eq!(reset.bulk_proof_expires_at, None);
}

#[test]
fn native_delivery_clock_epoch_uses_only_current_epoch_totals() {
    let congestion = quic_congestion(512 * 1024, Some(100_000_000));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(50);
    stats.path.cwnd = congestion.congestion_window;
    stats.path.current_mtu = 1400;
    let mut tracker = QuicPathMetricTracker::default();
    let first = with_acked_bytes_elapsed(congestion, 128 * 1024, 4, Duration::from_millis(20));
    let observed_first = tracker.observe(stats, first, PathMetricDirection::ServerToClient);
    assert_eq!(
        observed_first.latest_carrier_ack_elapsed,
        Some(Duration::from_millis(20))
    );

    let second = with_acked_bytes_elapsed(
        with_delivery_clock_epoch(congestion, 2),
        16 * 1024,
        1,
        Duration::from_millis(3),
    );
    let observed_second = tracker.observe(stats, second, PathMetricDirection::ServerToClient);

    assert_eq!(
        observed_second.latest_carrier_ack_elapsed,
        Some(Duration::from_millis(3))
    );
    assert_eq!(observed_second.latest_rate_sample_elapsed, None);
}

#[test]
fn repeated_native_clock_snapshot_does_not_duplicate_elapsed() {
    let congestion = quic_congestion(512 * 1024, Some(100_000_000));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(50);
    stats.path.cwnd = congestion.congestion_window;
    stats.path.current_mtu = 1400;
    let mut tracker = QuicPathMetricTracker::default();
    let ack = with_acked_bytes_elapsed(congestion, 64 * 1024, 2, Duration::from_millis(10));

    let first = tracker.observe(stats, ack, PathMetricDirection::ClientToServer);
    let repeated = tracker.observe(stats, ack, PathMetricDirection::ClientToServer);

    assert_eq!(
        first.latest_carrier_ack_elapsed,
        Some(Duration::from_millis(10))
    );
    assert_eq!(repeated.latest_carrier_ack_elapsed, None);
    assert!(!repeated.ack_derived_data_seen);
}

#[test]
fn app_limited_native_state_never_relabels_idle_time_as_product_freshness() {
    let congestion = quic_congestion(512 * 1024, Some(100_000_000));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(50);
    stats.path.cwnd = congestion.congestion_window;
    stats.path.current_mtu = 1400;
    let mut tracker = QuicPathMetricTracker::default();

    let idle = tracker.observe(stats, congestion, PathMetricDirection::ServerToClient);

    assert!(idle.app_limited);
    assert_eq!(idle.last_delivery_sample_at, None);
    assert_eq!(idle.bulk_proof_expires_at, None);
    assert!(!idle.ack_derived_data_seen);
}
