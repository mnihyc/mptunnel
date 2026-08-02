use super::super::estimator_test_support::*;
use super::*;

fn quic_metrics_for_polling() -> UdpPathMetrics {
    UdpPathMetrics {
        direction: PathMetricDirection::ServerToClient,
        srtt: Duration::from_millis(180),
        rttvar: Duration::from_millis(45),
        rtt_observed: true,
        delivery_rate_bps: 500_000_000.0,
        pacing_rate_bps: 500_000_000.0,
        inflight_hi: 4 * 1024 * 1024,
        bytes_in_flight: 0,
        pending_bytes: 0,
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
        #[cfg(feature = "lab-diagnostics")]
        ack_poll: QuicAckPollDiagnostics::default(),
    }
}

#[test]
fn idle_app_limited_quic_path_polls_on_transport_pto() {
    let metrics = quic_metrics_for_polling();
    assert_eq!(
        quic_path_metrics_poll_interval(metrics),
        transport_pto_from_ms(180.0, 45.0)
    );
}

#[test]
fn active_app_limited_quic_path_polls_on_ack_clock() {
    let expected = Duration::from_millis(90);
    assert_eq!(
        quic_path_metrics_ack_interval(quic_metrics_for_polling()),
        expected
    );
    let mut pending = quic_metrics_for_polling();
    pending.pending_bytes = 64 * 1024;
    assert_eq!(quic_path_metrics_poll_interval(pending), expected);

    let mut in_flight = quic_metrics_for_polling();
    in_flight.bytes_in_flight = 64 * 1024;
    assert_eq!(quic_path_metrics_poll_interval(in_flight), expected);
}

#[test]
fn quic_product_data_accepted_by_quinn_counts_as_queue_until_ack() {
    let mut tracker = UdpPathMetricTracker::default();
    let congestion = quic_congestion(4 * 1024 * 1024, Some(500_000_000));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(50);
    stats.path.cwnd = 4 * 1024 * 1024;
    stats.path.current_mtu = 1400;
    let _ = tracker.observe(stats, congestion, PathMetricDirection::ServerToClient);

    let queued = tracker.observe(
        stats,
        with_delivery_evidence_written(congestion, 8 * 1024 * 1024),
        PathMetricDirection::ServerToClient,
    );
    assert_eq!(queued.bytes_in_flight, 0);
    assert_eq!(queued.pending_bytes, 8 * 1024 * 1024);
    let product_metrics = path_metrics_from_quic_path(PathId(7), queued);
    assert_eq!(product_metrics.queue_bytes, 8 * 1024 * 1024);

    let partially_acked = tracker.observe(
        stats,
        with_acked_bytes(
            with_delivery_evidence_written(congestion, 8 * 1024 * 1024),
            2 * 1024 * 1024,
            1,
        ),
        PathMetricDirection::ServerToClient,
    );
    assert_eq!(partially_acked.pending_bytes, 6 * 1024 * 1024);
}

#[test]
fn quic_loss_unknown_is_not_reported_as_observed_zero() {
    let metrics = UdpPathMetrics {
        direction: PathMetricDirection::ServerToClient,
        srtt: Duration::from_millis(20),
        rttvar: Duration::from_millis(2),
        rtt_observed: true,
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
fn quic_server_metrics_publish_ack_data_seen_even_when_app_limited() {
    let metrics = UdpPathMetrics {
        direction: PathMetricDirection::ServerToClient,
        srtt: Duration::from_millis(50),
        rttvar: Duration::from_millis(5),
        rtt_observed: true,
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
        #[cfg(feature = "lab-diagnostics")]
        ack_poll: QuicAckPollDiagnostics::default(),
    };

    assert!(quic_path_metrics_should_publish_local_sender(metrics));
    let product_metrics = path_metrics_from_quic_path(PathId(7), metrics);
    assert!(product_metrics.has_ack_derived_data_sample);
    assert_eq!(product_metrics.data_sample_count, 0);
    assert!(product_metrics.app_limited);
}
