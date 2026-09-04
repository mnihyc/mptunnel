use super::super::estimator_test_support::*;
use super::*;

#[test]
fn full_window_native_ack_qualifies_carrier_rate_without_product_proof() {
    let mut tracker = UdpPathMetricTracker::default();
    let mut congestion = quic_congestion(4 * 1024 * 1024, Some(550_000_000));
    congestion.bandwidth_estimate_bps = Some(500_000_000);
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(50);
    stats.path.cwnd = 4 * 1024 * 1024;
    stats.path.current_mtu = 1400;

    let startup = tracker
        .quic
        .observe(stats, congestion, PathMetricDirection::ServerToClient);
    assert_eq!(startup.direction, PathMetricDirection::ServerToClient);
    assert_eq!(startup.delivery_rate_bps.round() as u64, 500_000_000);
    assert_eq!(startup.pacing_rate_bps.round() as u64, 550_000_000);
    assert_eq!(startup.inflight_hi, 4 * 1024 * 1024);
    assert!(!startup.ack_derived_data_seen);
    assert_eq!(startup.delivery_sample_count, 0);
    assert_eq!(startup.bulk_proof_expires_at, None);

    let native_ack = tracker.quic.observe(
        stats,
        with_acked_bytes(congestion, 8 * 1024 * 1024, 4),
        PathMetricDirection::ServerToClient,
    );
    assert_eq!(
        native_ack.latest_carrier_ack_elapsed,
        Some(Duration::from_millis(100))
    );
    assert!(native_ack.ack_derived_data_seen);
    assert_eq!(native_ack.delivery_sample_count, 4);
    assert_eq!(native_ack.delivery_sample_bytes, 8 * 1024 * 1024);
    assert!(native_ack.last_delivery_sample_at.is_some());
    assert!(native_ack.bulk_proof_expires_at.is_some());
    assert_eq!(
        native_ack.latest_rate_sample_elapsed,
        Some(Duration::from_millis(100))
    );
}

#[test]
fn arbitrary_native_ack_bytes_cannot_mint_product_reachability_or_rate() {
    let congestion = quic_congestion(256 * 1024, Some(80_000_000));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(80);
    stats.path.cwnd = congestion.congestion_window;
    stats.path.current_mtu = 1400;
    let mut tracker = QuicPathMetricTracker::default();
    let startup = tracker.observe(stats, congestion, PathMetricDirection::ClientToServer);

    // These bytes may be H3 headers/control, QUIC DATAGRAM, a sibling request,
    // retransmission, or Product. Native ACK telemetry cannot distinguish them.
    let observed = tracker.observe(
        stats,
        with_acked_bytes_elapsed(congestion, 64 * 1024, 8, Duration::from_millis(20)),
        PathMetricDirection::ClientToServer,
    );

    assert!(!observed.ack_derived_data_seen);
    assert_eq!(observed.delivery_sample_count, 0);
    assert_eq!(observed.delivery_sample_bytes, 0);
    assert_eq!(observed.latest_rate_sample_elapsed, None);
    assert_eq!(observed.bulk_proof_expires_at, None);
    assert_eq!(
        observed.delivery_rate_bps, startup.delivery_rate_bps,
        "unattributed ACK bytes cannot replace the carrier's pre-existing native rate projection"
    );
}

#[test]
fn native_ack_clock_is_independent_of_metrics_poll_phase() {
    let congestion = quic_congestion(512 * 1024, Some(100_000_000));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(20);
    stats.path.cwnd = congestion.congestion_window;
    stats.path.current_mtu = 1400;
    let base = Instant::now();
    let ack = with_acked_bytes_elapsed(congestion, 256 * 1024, 4, Duration::from_millis(20));
    let mut fast = QuicPathMetricTracker::default();
    let mut slow = QuicPathMetricTracker::default();
    let _ = fast.observe_at(stats, congestion, PathMetricDirection::ServerToClient, base);
    let _ = slow.observe_at(stats, congestion, PathMetricDirection::ServerToClient, base);

    let fast_observed = fast.observe_at(
        stats,
        ack,
        PathMetricDirection::ServerToClient,
        base + Duration::from_millis(25),
    );
    let slow_observed = slow.observe_at(
        stats,
        ack,
        PathMetricDirection::ServerToClient,
        base + Duration::from_secs(1),
    );

    assert_eq!(
        fast_observed.latest_carrier_ack_elapsed,
        slow_observed.latest_carrier_ack_elapsed
    );
    assert_eq!(
        fast_observed.latest_carrier_ack_elapsed,
        Some(Duration::from_millis(20))
    );
    assert_eq!(
        fast_observed.latest_rate_sample_elapsed,
        Some(Duration::from_millis(20))
    );
    assert_eq!(
        slow_observed.latest_rate_sample_elapsed,
        Some(Duration::from_millis(20))
    );
    assert_eq!(
        fast_observed.delivery_rate_bps,
        slow_observed.delivery_rate_bps
    );
}

#[test]
fn native_pending_and_flight_are_reported_without_product_pending_scalar() {
    let mut congestion = quic_congestion(512 * 1024, Some(100_000_000));
    congestion.pending_bytes = 96 * 1024;
    congestion.bytes_in_flight = Some(128 * 1024);
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(50);
    stats.path.cwnd = congestion.congestion_window;
    stats.path.current_mtu = 1400;
    let mut tracker = UdpPathMetricTracker::default();

    let observed = tracker.observe(stats, congestion, PathMetricDirection::ServerToClient);

    assert_eq!(observed.bytes_in_flight, 128 * 1024);
    assert_eq!(observed.pending_bytes, 128 * 1024);
}

#[test]
fn controller_bandwidth_is_native_rate_and_pacing_remains_separate() {
    let mut congestion = quic_congestion(512 * 1024, Some(180_000_000));
    congestion.bandwidth_estimate_bps = Some(150_000_000);
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(50);
    stats.path.cwnd = congestion.congestion_window;
    stats.path.current_mtu = 1400;
    let mut tracker = UdpPathMetricTracker::default();

    let observed = tracker.observe(stats, congestion, PathMetricDirection::ServerToClient);

    assert_eq!(observed.controller_bandwidth_bps, Some(150_000_000));
    assert_eq!(observed.delivery_rate_bps.round() as u64, 150_000_000);
    assert_eq!(observed.pacing_rate_bps.round() as u64, 180_000_000);
}

#[cfg(feature = "lab-diagnostics")]
#[test]
fn quic_loss_declaration_is_reported_once_per_native_counter_advance() {
    let mut tracker = UdpPathMetricTracker::default();
    let mut congestion = quic_congestion(4 * 1024 * 1024, Some(500_000_000));
    let stats = quinn::ConnectionStats::default();
    congestion.lost_bytes = 1_000;

    let baseline = tracker.observe(stats, congestion, PathMetricDirection::ServerToClient);
    assert_eq!(baseline.ack_poll.newly_lost_bytes, 0);

    congestion.lost_bytes = 2_400;
    let advanced = tracker.observe(stats, congestion, PathMetricDirection::ServerToClient);
    assert_eq!(advanced.ack_poll.newly_lost_bytes, 1_400);

    let repeated = tracker.observe(stats, congestion, PathMetricDirection::ServerToClient);
    assert_eq!(repeated.ack_poll.newly_lost_bytes, 0);
}
