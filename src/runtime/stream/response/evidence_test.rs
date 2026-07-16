use super::super::next_server_carrier_path_instance_id;
use super::super::test_support::{
    binding_for_underlay, output_entry_for_key, test_quic_capacity_proof,
};
use super::{
    ServerPathMetricsEntry, ServerPathMetricsSource, server_output_has_bulk_rate_evidence,
    server_output_has_durable_product_ack_progress, server_output_local_path_metrics,
    server_path_metrics_bulk_sample_floor_bytes, server_path_metrics_estimate_rate_bps,
    server_path_metrics_has_bulk_rate_evidence, server_quic_capacity_proof,
};
use crate::model::capacity::{PATH_OPEN_SCORE_BYTES, reliable_path_startup_sample_limit_bytes};
use crate::model::path::CarrierPathKey;
use crate::mux::MuxLimits;
use crate::protocol::{PathId, PathMetricDirection, PathMetrics, UnderlayProtocol};
use crate::runtime::path::model::metric_epoch_now;
use std::time::{Duration, Instant};

fn response_metrics(key: CarrierPathKey) -> PathMetrics {
    PathMetrics {
        path_id: key.path_id,
        underlay: key.underlay,
        direction: PathMetricDirection::ServerToClient,
        metric_epoch: metric_epoch_now(),
        metric_age_us: 0,
        srtt_us: 20_000,
        rttvar_us: 2_000,
        jitter_us: 1_000,
        delivery_rate_bps: 200_000_000,
        pacing_rate_bps: 200_000_000,
        loss_ppm: 0,
        ecn_ppm: 0,
        loss_observed: false,
        ecn_observed: false,
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
        recorded_at: Instant::now(),
        capacity_proof: None,
    }
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
fn ack_derived_bulk_evidence_uses_transport_specific_sample_floors() {
    for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
        let key = CarrierPathKey {
            underlay,
            path_id: PathId(11),
        };
        let mut metrics = response_metrics(key);
        let sample_floor = server_path_metrics_bulk_sample_floor_bytes(metrics);
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
        assert!(!server_path_metrics_has_bulk_rate_evidence(
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
        output.product_progress_rate_bps = Some(100_000_000.0);
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
fn quic_receipt_proof_is_exact_expiring_evidence() {
    let mux_limits = MuxLimits::default();
    let (binding, key, _receivers) = binding_for_underlay(UnderlayProtocol::Udp);
    let mut output = output_entry_for_key(&binding, key);
    let mut metrics = response_metrics(key);
    metrics.app_limited = true;
    metrics.has_ack_derived_data_sample = false;
    metrics.data_sample_count = 0;
    metrics.data_sample_bytes = 0;
    let proof = test_quic_capacity_proof(mux_limits, 41, Duration::from_secs(1));
    let accepted = ServerPathMetricsEntry {
        metrics,
        source: ServerPathMetricsSource::LocalSender,
        recorded_at: Instant::now(),
        capacity_proof: Some(proof),
    };
    output.local_path_metrics = Some(accepted);
    assert_eq!(server_quic_capacity_proof(accepted), Some(proof));
    assert_eq!(
        server_path_metrics_estimate_rate_bps(accepted),
        proof.rate_bps as f64
    );
    assert!(server_path_metrics_has_bulk_rate_evidence(accepted));
    assert!(server_output_has_bulk_rate_evidence(&output, mux_limits));

    let proof_validity = Duration::from_secs(1);
    let accepted_at = Instant::now() - Duration::from_secs(2);
    let expired = crate::model::capacity::QuicCapacityProofCandidate {
        accepted_at,
        expires_at: accepted_at + proof_validity,
        proof_validity,
        ..proof
    };
    let expired_entry = ServerPathMetricsEntry {
        capacity_proof: Some(expired),
        ..accepted
    };
    output.local_path_metrics = Some(expired_entry);
    assert!(server_quic_capacity_proof(expired_entry).is_none());
    assert!(!server_path_metrics_has_bulk_rate_evidence(expired_entry));
    assert!(!server_output_has_bulk_rate_evidence(&output, mux_limits));
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

    binding.update_path_metrics_for_instance(
        key,
        path_instance_id,
        PathMetrics {
            metric_epoch: metrics.metric_epoch.wrapping_add(1),
            metric_age_us: metrics.metric_age_us.saturating_add(100),
            ..metrics
        },
        ServerPathMetricsSource::LocalSender,
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
