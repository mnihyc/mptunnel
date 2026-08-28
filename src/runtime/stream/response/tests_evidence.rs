use super::super::attachment::ResponseProductRateEpoch;
use super::super::next_server_carrier_path_instance_id;
use super::super::test_support::{binding_for_underlay, output_entry_for_key};
use super::{
    ServerPathMetricsEntry, ServerPathMetricsSource, server_output_has_bulk_rate_evidence,
    server_output_has_durable_product_ack_progress, server_output_local_path_metrics,
    server_path_metrics_bulk_sample_floor_bytes, server_path_metrics_estimate_rate_bps,
    server_path_metrics_has_bulk_rate_evidence, server_path_metrics_has_bulk_rate_evidence_at,
    server_path_metrics_snapshot_is_fresh_at,
};
use crate::model::capacity::{PATH_OPEN_SCORE_BYTES, reliable_path_startup_sample_limit_bytes};
use crate::model::path::CarrierPathKey;
use crate::mux::MuxLimits;
use crate::protocol::{PathId, PathMetricDirection, PathMetrics, UnderlayProtocol};
use crate::runtime::path::CarrierDeliveryRateSample;
use crate::runtime::path::model::metric_epoch_now;
use std::time::{Duration, Instant};

fn response_metrics(key: CarrierPathKey) -> PathMetrics {
    PathMetrics {
        path_id: key.path_id,
        underlay: key.underlay,
        direction: PathMetricDirection::ServerToClient,
        metric_epoch: metric_epoch_now(),
        metric_age_us: 0,
        rate_valid_for_us: 1_000_000,
        rate_observed: true,
        srtt_us: 20_000,
        rttvar_us: 2_000,
        jitter_us: 1_000,
        delivery_rate_bps: 200_000_000,
        pacing_rate_bps: 200_000_000,
        pacing_rate_observed: true,
        loss_ppm: 0,
        ecn_ppm: 0,
        loss_observed: false,
        ecn_observed: false,
        bytes_in_flight_observed: false,
        queue_observed: false,
        bytes_in_flight: 0,
        queue_bytes: 0,
        inflight_limit_bytes: PATH_OPEN_SCORE_BYTES as u64,
        inflight_hi_bytes: PATH_OPEN_SCORE_BYTES as u64,
        confidence_ppm: 1_000_000,
        app_limited: false,
        has_ack_derived_data_sample: true,
        data_sample_count: 1,
        data_sample_bytes: u64::MAX,
    }
}

fn local_metrics_entry(metrics: PathMetrics) -> ServerPathMetricsEntry {
    ServerPathMetricsEntry {
        metrics,
        source: ServerPathMetricsSource::LocalSender,
        native_drain_observed: false,
        carrier_delivery_rate_sample: None,
        recorded_at: Instant::now(),
    }
}

#[test]
fn wire_rate_authority_expires_at_exact_remaining_budget_boundary() {
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(9),
    };
    let received_at = Instant::now();
    let entry = ServerPathMetricsEntry {
        metrics: PathMetrics {
            metric_age_us: u32::MAX,
            rate_valid_for_us: 1,
            ..response_metrics(key)
        },
        source: ServerPathMetricsSource::LocalSender,
        native_drain_observed: false,
        carrier_delivery_rate_sample: None,
        recorded_at: received_at,
    };
    assert!(server_path_metrics_snapshot_is_fresh_at(
        entry,
        received_at + Duration::from_nanos(999),
    ));
    assert!(!server_path_metrics_snapshot_is_fresh_at(
        entry,
        received_at + Duration::from_micros(1),
    ));
}

#[test]
fn metric_update_requires_exact_path_instance_and_preserves_incarnation() {
    let (binding, key, _receivers) = binding_for_underlay(UnderlayProtocol::Udp);
    let initial = output_entry_for_key(&binding, key);
    let initial_generation = binding.response_model_generation();
    let updates = binding.subscribe_updates();

    binding.update_path_metrics_for_instance(
        CarrierPathKey {
            path_id: PathId(key.path_id.0 + 1),
            ..key
        },
        initial.path_instance_id,
        response_metrics(key),
        ServerPathMetricsSource::LocalSender,
    );
    binding.update_path_metrics_for_instance(
        key,
        next_server_carrier_path_instance_id(),
        response_metrics(key),
        ServerPathMetricsSource::LocalSender,
    );

    let unchanged = output_entry_for_key(&binding, key);
    assert!(unchanged.local_path_metrics.is_none());
    assert_eq!(unchanged.incarnation, initial.incarnation);
    assert_eq!(binding.response_model_generation(), initial_generation);
    assert!(!updates.has_changed().expect("response update channel"));

    let metrics = response_metrics(key);
    binding.update_path_metrics_for_instance(
        key,
        initial.path_instance_id,
        metrics,
        ServerPathMetricsSource::LocalSender,
    );

    let updated = output_entry_for_key(&binding, key);
    assert_eq!(updated.incarnation, initial.incarnation);
    assert_eq!(
        updated.local_path_metrics.map(|entry| entry.metrics),
        Some(metrics)
    );
    assert_eq!(binding.response_model_generation(), initial_generation + 1);
    assert!(updates.has_changed().expect("response update channel"));
}

#[test]
fn response_bulk_evidence_rejects_peer_wrong_direction_and_foreign_path_metrics() {
    let (binding, key, _receivers) = binding_for_underlay(UnderlayProtocol::Udp);
    let path_instance_id = output_entry_for_key(&binding, key).path_instance_id;
    let metrics = response_metrics(key);

    binding.update_path_metrics_for_instance(
        key,
        path_instance_id,
        metrics,
        ServerPathMetricsSource::PeerHint,
    );
    let peer_only = output_entry_for_key(&binding, key);
    assert!(server_output_local_path_metrics(&peer_only).is_none());
    assert!(!server_output_has_bulk_rate_evidence(
        &peer_only,
        MuxLimits::default()
    ));

    for invalid in [
        PathMetrics {
            direction: PathMetricDirection::ClientToServer,
            ..metrics
        },
        PathMetrics {
            underlay: UnderlayProtocol::Tcp,
            ..metrics
        },
        PathMetrics {
            path_id: PathId(key.path_id.0 + 1),
            ..metrics
        },
    ] {
        binding.update_path_metrics_for_instance(
            key,
            path_instance_id,
            invalid,
            ServerPathMetricsSource::LocalSender,
        );
        let output = output_entry_for_key(&binding, key);
        assert!(server_output_local_path_metrics(&output).is_none());
        assert!(!server_output_has_bulk_rate_evidence(
            &output,
            MuxLimits::default()
        ));
    }

    binding.update_path_metrics_for_instance(
        key,
        path_instance_id,
        metrics,
        ServerPathMetricsSource::LocalSender,
    );
    let output = output_entry_for_key(&binding, key);
    assert_eq!(
        server_output_local_path_metrics(&output).map(|entry| entry.metrics),
        Some(metrics)
    );
    assert!(server_output_has_bulk_rate_evidence(
        &output,
        MuxLimits::default()
    ));
}

#[test]
fn ack_derived_bulk_evidence_requires_native_window_coverage() {
    for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
        let key = CarrierPathKey {
            underlay,
            path_id: PathId(11),
        };
        let mut metrics = response_metrics(key);
        metrics.inflight_hi_bytes = 1_500_000;
        metrics.inflight_limit_bytes = 2_012_844;
        let sample_floor = server_path_metrics_bulk_sample_floor_bytes(metrics);
        assert_eq!(sample_floor, metrics.inflight_limit_bytes);
        let accounting_slack = (PATH_OPEN_SCORE_BYTES as u64).min(sample_floor / 8);
        metrics.data_sample_bytes = sample_floor.saturating_sub(accounting_slack + 1);
        assert!(!server_path_metrics_has_bulk_rate_evidence(
            local_metrics_entry(metrics)
        ));

        metrics.data_sample_bytes = sample_floor.saturating_sub(accounting_slack);
        assert!(server_path_metrics_has_bulk_rate_evidence(
            local_metrics_entry(metrics)
        ));

        metrics.app_limited = true;
        assert!(server_path_metrics_has_bulk_rate_evidence(
            local_metrics_entry(metrics)
        ));
    }
}

#[test]
fn data_ack_progress_is_tcp_fallback_not_udp_carrier_evidence() {
    let mux_limits = MuxLimits::default();
    let sample_floor = reliable_path_startup_sample_limit_bytes(mux_limits);
    let accounting_slack = (PATH_OPEN_SCORE_BYTES as u64).min(sample_floor / 8);

    for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
        let (binding, key, _receivers) = binding_for_underlay(underlay);
        let mut output = output_entry_for_key(&binding, key);
        output.product_rate_epoch = ResponseProductRateEpoch::new(
            100_000_000.0,
            1,
            sample_floor,
            Instant::now(),
            Duration::from_secs(60),
        );
        output.original_data_acked_bytes = sample_floor.saturating_sub(accounting_slack + 1);
        assert!(!server_output_has_durable_product_ack_progress(
            &output, mux_limits
        ));
        assert!(!server_output_has_bulk_rate_evidence(&output, mux_limits));

        output.original_data_acked_bytes = sample_floor.saturating_sub(accounting_slack);
        assert!(server_output_has_durable_product_ack_progress(
            &output, mux_limits
        ));
        assert_eq!(
            server_output_has_bulk_rate_evidence(&output, mux_limits),
            underlay == UnderlayProtocol::Tcp,
            "Data ACK goodput is a TCP response fallback; UDP requires carrier evidence"
        );
    }
}

#[test]
fn native_quic_rate_sample_is_expiring_bulk_evidence() {
    let mux_limits = MuxLimits::default();
    let (binding, key, _receivers) = binding_for_underlay(UnderlayProtocol::Udp);
    let mut output = output_entry_for_key(&binding, key);
    let metrics = response_metrics(key);
    let accepted = ServerPathMetricsEntry {
        metrics,
        source: ServerPathMetricsSource::LocalSender,
        native_drain_observed: false,
        carrier_delivery_rate_sample: None,
        recorded_at: Instant::now(),
    };
    output.local_path_metrics = Some(accepted);
    assert_eq!(
        server_path_metrics_estimate_rate_bps(accepted),
        metrics.delivery_rate_bps as f64
    );
    assert!(server_path_metrics_has_bulk_rate_evidence(accepted));
    assert!(server_output_has_bulk_rate_evidence(&output, mux_limits));

    let expired_entry = ServerPathMetricsEntry {
        recorded_at: Instant::now() - Duration::from_secs(2),
        ..accepted
    };
    output.local_path_metrics = Some(expired_entry);
    assert!(!server_path_metrics_has_bulk_rate_evidence(expired_entry));
    assert!(!server_output_has_bulk_rate_evidence(&output, mux_limits));
}

#[test]
fn retained_native_tcp_sample_is_carrier_capacity_not_product_ack_evidence() {
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let mut metrics = response_metrics(key);
    metrics.delivery_rate_bps = 6_000_000;
    metrics.app_limited = true;
    metrics.has_ack_derived_data_sample = false;
    metrics.data_sample_count = 0;
    metrics.data_sample_bytes = 0;
    let observed_at = Instant::now();
    let expires_at = observed_at + Duration::from_millis(100);
    let mut entry = ServerPathMetricsEntry {
        metrics,
        source: ServerPathMetricsSource::LocalSender,
        native_drain_observed: true,
        carrier_delivery_rate_sample: Some(CarrierDeliveryRateSample {
            delivery_rate_bps: 80_000_000,
            pacing_rate_bps: Some(100_000_000),
            sample_count: 8,
            sample_bytes: 512 * 1024,
            delivery_window_covered: true,
            observed_at,
            expires_at,
        }),
        recorded_at: Instant::now(),
    };

    assert!(server_path_metrics_has_bulk_rate_evidence(entry));
    assert_eq!(server_path_metrics_estimate_rate_bps(entry), 80_000_000.0);
    assert!(!entry.metrics.has_ack_derived_data_sample);
    assert!(server_path_metrics_has_bulk_rate_evidence_at(
        entry,
        expires_at - Duration::from_nanos(1),
    ));

    // A fresh registry refresh may retain old wire ACK counters; the sidecar
    // deadline remains the sole authority whenever the sidecar exists.
    entry.metrics.has_ack_derived_data_sample = true;
    entry.metrics.data_sample_count = 99;
    entry.metrics.data_sample_bytes = u64::MAX;
    entry.recorded_at = expires_at;
    assert!(!server_path_metrics_has_bulk_rate_evidence_at(
        entry, expires_at,
    ));
    assert_eq!(
        server_path_metrics_estimate_rate_bps(entry),
        80_000_000.0,
        "the immutable sidecar value cannot fall back to retained wire rate at expiry; eligibility owns the deadline"
    );
}

#[test]
fn scheduling_equivalent_metric_refresh_does_not_advance_generation_or_notify() {
    let (binding, key, _receivers) = binding_for_underlay(UnderlayProtocol::Udp);
    let path_instance_id = output_entry_for_key(&binding, key).path_instance_id;
    let metrics = response_metrics(key);
    let mut updates = binding.subscribe_updates();

    binding.update_path_metrics_for_instance(
        key,
        path_instance_id,
        metrics,
        ServerPathMetricsSource::LocalSender,
    );
    let installed_generation = binding.response_model_generation();
    assert!(updates.has_changed().expect("response update channel"));
    updates.borrow_and_update();

    binding.install_stored_path_metrics_for_instance(
        key,
        path_instance_id,
        ServerPathMetricsEntry {
            metrics: PathMetrics {
                metric_epoch: metrics.metric_epoch.wrapping_add(1),
                metric_age_us: metrics.metric_age_us.saturating_add(100),
                ..metrics
            },
            source: ServerPathMetricsSource::LocalSender,
            native_drain_observed: false,
            carrier_delivery_rate_sample: None,
            recorded_at: Instant::now(),
        },
    );
    assert_eq!(binding.response_model_generation(), installed_generation);
    assert!(!updates.has_changed().expect("response update channel"));

    binding.update_path_metrics_for_instance(
        key,
        path_instance_id,
        PathMetrics {
            delivery_rate_bps: metrics.delivery_rate_bps / 2,
            ..metrics
        },
        ServerPathMetricsSource::LocalSender,
    );
    assert_eq!(
        binding.response_model_generation(),
        installed_generation + 1
    );
    assert!(updates.has_changed().expect("response update channel"));
}
