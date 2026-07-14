use super::super::estimator_test_support::*;
use super::*;

#[test]
fn quic_product_data_accepted_by_quinn_counts_as_queue_until_ack() {
    let mut tracker = UdpPathMetricTracker::default();
    let congestion = quic_congestion(4 * 1024 * 1024, Some(500_000_000));
    let mut stats = quinn::ConnectionStats::default();
    stats.path.rtt = Duration::from_millis(50);
    stats.path.cwnd = 4 * 1024 * 1024;
    stats.path.current_mtu = 1400;
    let _ = tracker.observe(stats, congestion, 2);

    let queued = tracker.observe(
        stats,
        with_delivery_evidence_written(congestion, 8 * 1024 * 1024),
        2,
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
        UdpPathMetricTracker::default().observe_at(
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
