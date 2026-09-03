use super::super::estimator_test_support::*;
use super::*;

fn quic_metrics_for_polling() -> UdpPathMetrics {
    UdpPathMetrics {
        controller_path_epoch: 1,
        direction: PathMetricDirection::ServerToClient,
        srtt: Duration::from_millis(180),
        rttvar: Duration::from_millis(45),
        rtt_observed: true,
        delivery_rate_bps: 500_000_000.0,
        pacing_rate_bps: 500_000_000.0,
        controller_bandwidth_bps: None,
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
fn live_controller_capacity_is_typed_separately_from_product_proof() {
    let mut tracker = UdpPathMetricTracker::default();
    let mut congestion = quic_congestion(4 * 1024 * 1024, Some(625_000_000));
    congestion.path_epoch = 9;
    congestion.bandwidth_estimate_bps = Some(500_000_000);
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(100);
    stats.path.cwnd = 4 * 1024 * 1024;
    stats.path.current_mtu = 1400;

    let metrics = tracker.observe(stats, congestion, PathMetricDirection::ServerToClient);
    assert_eq!(metrics.controller_path_epoch, 9);
    assert_eq!(metrics.controller_bandwidth_bps, Some(500_000_000));
    assert_eq!(metrics.delivery_sample_count, 0);
    assert_eq!(metrics.bulk_proof_expires_at, None);
    assert!(quic_path_metrics_should_publish_local_sender(metrics, true));
    assert!(
        !quic_path_metrics_should_publish_local_sender(metrics, false),
        "unchanged idle controller state must not walk all bound response streams",
    );
}

#[test]
fn controller_publication_cursor_tracks_path_epoch_and_operational_bandwidth() {
    let mut cursor = QuicControllerPublicationCursor::default();
    let mut metrics = quic_metrics_for_polling();
    assert!(
        cursor.changed(metrics),
        "the first state clears any old task state"
    );
    assert!(!cursor.changed(metrics));

    metrics.app_limited = !metrics.app_limited;
    assert!(
        !cursor.changed(metrics),
        "current app-limited state is not response-side live-capacity authority",
    );

    metrics.controller_bandwidth_bps = Some(500_000_000);
    assert!(
        cursor.changed(metrics),
        "a changed controller-owned operational rate must reach bound response streams"
    );
    assert!(!cursor.changed(metrics));
    metrics.controller_bandwidth_bps = Some(10_000_000);
    assert!(cursor.changed(metrics), "a native downshift must publish");
    metrics.controller_bandwidth_bps = Some(500_000_000);
    assert!(cursor.changed(metrics), "a native recovery must publish");
    metrics.controller_path_epoch = 2;
    assert!(
        cursor.changed(metrics),
        "same rate on a new path is new authority"
    );
    metrics.controller_bandwidth_bps = None;
    assert!(
        cursor.changed(metrics),
        "loss of operational-rate availability must clear the prior published value"
    );

    metrics.controller_bandwidth_bps = Some(0);
    assert!(
        !cursor.changed(metrics),
        "zero is normalized to unavailable"
    );
}

#[test]
fn connection_wide_native_queue_is_reported_without_product_attribution() {
    let mut tracker = UdpPathMetricTracker::default();
    let mut congestion = quic_congestion(4 * 1024 * 1024, Some(500_000_000));
    congestion.pending_bytes = 8 * 1024 * 1024;
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(50);
    stats.path.cwnd = 4 * 1024 * 1024;
    stats.path.current_mtu = 1400;
    let _ = tracker.observe(stats, congestion, PathMetricDirection::ServerToClient);

    let queued = tracker.observe(stats, congestion, PathMetricDirection::ServerToClient);
    assert_eq!(queued.bytes_in_flight, 0);
    assert_eq!(queued.pending_bytes, 8 * 1024 * 1024);
    let native_metrics = path_metrics_from_quic_path(PathId(7), queued, None);
    assert_eq!(native_metrics.queue_bytes, 8 * 1024 * 1024);

    // An arbitrary connection ACK cannot subtract from an alleged Product
    // scalar. Only Quinn's own native pending snapshot changes this queue.
    let acked = tracker.observe(
        stats,
        with_acked_bytes(congestion, 2 * 1024 * 1024, 1),
        PathMetricDirection::ServerToClient,
    );
    assert_eq!(acked.pending_bytes, 8 * 1024 * 1024);
    congestion.pending_bytes = 6 * 1024 * 1024;
    let partially_acked = tracker.observe(stats, congestion, PathMetricDirection::ServerToClient);
    assert_eq!(partially_acked.pending_bytes, 6 * 1024 * 1024);
}

#[test]
fn quic_loss_unknown_is_not_reported_as_observed_zero() {
    let metrics = UdpPathMetrics {
        controller_path_epoch: 1,
        direction: PathMetricDirection::ServerToClient,
        srtt: Duration::from_millis(20),
        rttvar: Duration::from_millis(2),
        rtt_observed: true,
        delivery_rate_bps: 500_000_000.0,
        pacing_rate_bps: 500_000_000.0,
        controller_bandwidth_bps: None,
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

    let path_metrics = path_metrics_from_quic_path(PathId(7), metrics, None);

    assert_eq!(path_metrics.loss_ppm, 0);
    assert!(!path_metrics.loss_observed);
    assert_eq!(path_metrics.ecn_ppm, 0);
    assert!(!path_metrics.ecn_observed);
    assert_eq!(path_metrics.bytes_in_flight, 128 * 1024);
    assert_eq!(path_metrics.queue_bytes, 128 * 1024);
}

#[test]
fn quic_server_metrics_publish_native_ack_qualification_even_when_currently_app_limited() {
    let metrics = UdpPathMetrics {
        controller_path_epoch: 1,
        direction: PathMetricDirection::ServerToClient,
        srtt: Duration::from_millis(50),
        rttvar: Duration::from_millis(5),
        rtt_observed: true,
        delivery_rate_bps: 500_000_000.0,
        pacing_rate_bps: 500_000_000.0,
        controller_bandwidth_bps: None,
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

    assert!(quic_path_metrics_should_publish_local_sender(
        metrics, false
    ));
    let native_metrics = path_metrics_from_quic_path(PathId(7), metrics, None);
    assert!(native_metrics.has_ack_derived_data_sample);
    assert_eq!(native_metrics.data_sample_count, 0);
    assert!(native_metrics.app_limited);
}

#[test]
fn server_quic_sidecar_freezes_expiry_and_survives_expired_rtt_refreshes_until_epoch_reset() {
    let observed_at = Instant::now();
    let expires_at = observed_at + Duration::from_millis(300);
    let mut metrics = quic_metrics_for_polling();
    metrics.delivery_rate_bps = 80_000_000.0;
    metrics.pacing_rate_bps = 100_000_000.0;
    metrics.delivery_sample_count = 4;
    metrics.delivery_sample_bytes = 512 * 1024;
    metrics.last_delivery_sample_at = Some(observed_at);
    metrics.bulk_proof_expires_at = Some(expires_at);
    let frozen = retained_quic_delivery_rate_sample(None, metrics).expect("fresh sidecar");
    assert_eq!(frozen.observed_at, observed_at);
    assert_eq!(frozen.expires_at, expires_at);
    assert_eq!(frozen.delivery_rate_bps, 80_000_000);
    assert_eq!(frozen.pacing_rate_bps, Some(100_000_000));

    let projected = path_metrics_from_quic_path(
        PathId(7),
        UdpPathMetrics {
            delivery_rate_bps: 7_000_000.0,
            pacing_rate_bps: 9_000_000.0,
            delivery_sample_count: 999,
            delivery_sample_bytes: 999 * 1024 * 1024,
            app_limited: true,
            ack_derived_data_seen: false,
            ..metrics
        },
        Some(frozen),
    );
    assert_eq!(projected.delivery_rate_bps, 80_000_000);
    assert_eq!(projected.pacing_rate_bps, 100_000_000);
    assert!(projected.rate_observed);
    assert!(projected.pacing_rate_observed);
    assert!(projected.rate_valid_for_us > 0);
    assert_eq!(projected.data_sample_count, 4);
    assert_eq!(projected.data_sample_bytes, 512 * 1024);
    assert_eq!(
        projected.confidence_ppm,
        ratio_to_ppm((4.0 / QUIC_INITIAL_WINDOW_PACKETS as f64).clamp(0.0, 1.0))
    );
    assert!(
        projected.app_limited,
        "a retained qualified rate epoch must not overwrite current Quinn application-limited state",
    );
    assert!(projected.has_ack_derived_data_sample);

    let stale = path_metrics_from_quic_path(
        PathId(7),
        metrics,
        Some(CarrierDeliveryRateSample {
            expires_at: Instant::now() - Duration::from_micros(1),
            ..frozen
        }),
    );
    assert_eq!(stale.rate_valid_for_us, 0);
    assert!(stale.rate_observed);
    assert!(stale.pacing_rate_observed);
    assert_eq!(stale.delivery_rate_bps, 80_000_000);
    assert_eq!(stale.pacing_rate_bps, 100_000_000);

    assert_eq!(
        retained_quic_delivery_rate_sample(
            Some(frozen),
            UdpPathMetrics {
                delivery_rate_bps: 7_000_000.0,
                pacing_rate_bps: 9_000_000.0,
                ..metrics
            },
        ),
        Some(frozen),
        "an ACK-less shape refresh cannot rewrite values in the same sample epoch"
    );

    for later_rtt in [Duration::from_millis(1), Duration::from_secs(10)] {
        let expired_refresh = UdpPathMetrics {
            srtt: later_rtt,
            rttvar: later_rtt / 2,
            delivery_rate_bps: 7_000_000.0,
            pacing_rate_bps: 9_000_000.0,
            bulk_proof_expires_at: None,
            ..metrics
        };
        assert_eq!(
            retained_quic_delivery_rate_sample(Some(frozen), expired_refresh),
            Some(frozen),
            "later RTT growth or shrink cannot move or erase the expired immutable sample"
        );
    }

    assert_eq!(
        retained_quic_delivery_rate_sample(
            Some(frozen),
            UdpPathMetrics {
                last_delivery_sample_at: None,
                bulk_proof_expires_at: None,
                ..metrics
            },
        ),
        None,
        "only an explicit tracker epoch reset clears the retained sidecar"
    );
}
