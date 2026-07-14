use super::super::next_server_carrier_path_instance_id;
use super::super::response_admission::{
    server_output_has_bulk_rate_evidence, server_output_has_sender_evidence,
    server_output_has_service_feed_evidence_with_limits,
};
use super::super::response_quic_capacity::quic_capacity_receipt_rate_bps;
use super::super::response_session::ServerPathLaneTracker;
use super::super::response_snapshot::{server_bulk_output_snapshot, server_output_confidence};
use super::super::response_topology::ResponseStreamOutputEntry;
use super::{
    ServerPathMetricsEntry, ServerPathMetricsSource, server_path_metrics_estimate_rate_bps,
    server_path_metrics_has_bulk_rate_evidence, server_path_metrics_has_sender_evidence,
    server_path_metrics_rate_bps,
};
use crate::model::capacity::{
    PATH_OPEN_SCORE_BYTES, QUIC_TIMER_GRANULARITY, reliable_subflow_startup_sample_limit_bytes,
};
use crate::model::path::CarrierPathKey;
use crate::model::timing::quic_bulk_proof_freshness_horizon;
use crate::mux::MuxLimits;
use crate::protocol::{
    PathId, PathMetricDirection, PathMetrics, SessionId, StreamOpenRole, UnderlayProtocol,
};
use crate::runtime::path::commands::reliable_path_command_channels;
use crate::runtime::path::model::{default_path_rate_bps, metric_epoch_now};
use crate::runtime::path::quic::metrics::QuicCapacityProofCandidate;
use crate::scheduler::FlowLane;
use std::time::{Duration, Instant};

#[test]
fn local_carrier_bulk_evidence_requires_response_direction_and_exact_path_identity() {
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(21),
    };
    let metrics = PathMetrics {
        path_id: key.path_id,
        underlay: key.underlay,
        direction: PathMetricDirection::ServerToClient,
        metric_epoch: metric_epoch_now(),
        metric_age_us: 0,
        min_rtt_us: 20_000,
        srtt_us: 20_000,
        rttvar_us: 1_000,
        jitter_us: 1_000,
        delivery_rate_bps: 200_000_000,
        pacing_rate_bps: 200_000_000,
        loss_ppm: 0,
        ecn_ppm: 0,
        loss_observed: false,
        ecn_observed: false,
        bytes_in_flight: PATH_OPEN_SCORE_BYTES as u64,
        queue_bytes: 0,
        inflight_limit_bytes: 4 * PATH_OPEN_SCORE_BYTES as u64,
        inflight_hi_bytes: 4 * PATH_OPEN_SCORE_BYTES as u64,
        confidence_ppm: 1_000_000,
        app_limited: false,
        has_ack_derived_data_sample: true,
        data_sample_count: 32,
        data_sample_bytes: 4 * PATH_OPEN_SCORE_BYTES as u64,
    };
    let entry_with_metrics = |metrics| {
        let (commands, _receivers) = reliable_path_command_channels(8);
        ResponseStreamOutputEntry {
            key,
            path_instance_id: next_server_carrier_path_instance_id(),
            incarnation: 1,
            commands,
            role: StreamOpenRole::Validation,
            owner_data_in_flight_bytes: 0,
            bytes_in_flight: 0,
            product_queue_bytes: 0,
            product_progress_rate_bps: None,
            delivery_rate_bps: None,
            tcp_ack_clock_rate_bps: None,
            tcp_product_rate_evidence: None,
            tcp_capacity_prior: None,
            srtt_ms: None,
            delivery_samples: 0,
            owner_data_acked_bytes: 0,
            local_path_metrics: Some(ServerPathMetricsEntry {
                source: ServerPathMetricsSource::LocalSender,
                recorded_at: Instant::now(),
                capacity_proof: None,
                tcp_capacity_proof: None,
                metrics,
            }),
            peer_path_metrics: None,
        }
    };

    assert!(server_output_has_bulk_rate_evidence(&entry_with_metrics(
        metrics
    )));
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
        let entry = entry_with_metrics(invalid);
        assert!(!server_output_has_sender_evidence(&entry));
        assert!(!server_output_has_bulk_rate_evidence(&entry));
        let snapshot = server_bulk_output_snapshot(
            &entry,
            SessionId(79),
            FlowLane::Throughput,
            &ServerPathLaneTracker::default(),
            MuxLimits::default(),
            Instant::now(),
        );
        assert_eq!(
            snapshot.delivery_rate_bps,
            default_path_rate_bps(key.underlay),
            "foreign carrier metrics must not influence the response path model"
        );
    }
}

#[test]
fn aged_udp_metric_loses_handoff_rights_but_keeps_sender_reachability() {
    let metrics = PathMetrics {
        path_id: PathId(22),
        underlay: UnderlayProtocol::Udp,
        direction: PathMetricDirection::ServerToClient,
        metric_epoch: metric_epoch_now(),
        metric_age_us: 0,
        min_rtt_us: 20_000,
        srtt_us: 20_000,
        rttvar_us: 5_000,
        jitter_us: 5_000,
        delivery_rate_bps: 200_000_000,
        pacing_rate_bps: 200_000_000,
        loss_ppm: 0,
        ecn_ppm: 0,
        loss_observed: false,
        ecn_observed: false,
        bytes_in_flight: 0,
        queue_bytes: 0,
        inflight_limit_bytes: 4 * PATH_OPEN_SCORE_BYTES as u64,
        inflight_hi_bytes: 4 * PATH_OPEN_SCORE_BYTES as u64,
        confidence_ppm: 1_000_000,
        app_limited: false,
        has_ack_derived_data_sample: true,
        data_sample_count: 32,
        data_sample_bytes: 4 * PATH_OPEN_SCORE_BYTES as u64,
    };
    let freshness_horizon = quic_bulk_proof_freshness_horizon(
        Duration::from_micros(u64::from(metrics.srtt_us)),
        Duration::from_micros(u64::from(metrics.rttvar_us)),
    );
    let local = |metrics| ServerPathMetricsEntry {
        source: ServerPathMetricsSource::LocalSender,
        metrics,
        recorded_at: Instant::now(),
        capacity_proof: None,
        tcp_capacity_proof: None,
    };

    let mut fresh = metrics;
    fresh.metric_age_us = u32::try_from((freshness_horizon - QUIC_TIMER_GRANULARITY).as_micros())
        .expect("test freshness horizon");
    assert!(server_path_metrics_has_bulk_rate_evidence(local(fresh)));

    let mut aged = metrics;
    aged.metric_age_us =
        u32::try_from(freshness_horizon.as_micros()).expect("test freshness horizon");
    let aged = local(aged);
    assert!(!server_path_metrics_has_bulk_rate_evidence(aged));
    assert!(server_path_metrics_has_sender_evidence(aged));

    let delayed_idle_refresh = ServerPathMetricsEntry {
        source: ServerPathMetricsSource::LocalSender,
        metrics,
        recorded_at: Instant::now() - freshness_horizon,
        capacity_proof: None,
        tcp_capacity_proof: None,
    };
    assert!(!server_path_metrics_has_bulk_rate_evidence(
        delayed_idle_refresh
    ));
    assert!(server_path_metrics_has_sender_evidence(
        delayed_idle_refresh
    ));
}

#[test]
fn accepted_quic_capacity_marker_uses_frozen_floor_rate_and_deadline() {
    let sample_floor = reliable_subflow_startup_sample_limit_bytes(MuxLimits::default());
    let accepted_at = Instant::now();
    let accounting_slack = (PATH_OPEN_SCORE_BYTES as u64).min(sample_floor / 8);
    let required_proof_bytes = sample_floor - accounting_slack;
    let proof_elapsed = Duration::from_millis(10);
    let candidate = QuicCapacityProofCandidate {
        token: 31,
        train_bytes: sample_floor,
        sample_floor_bytes: sample_floor,
        accounting_slack_bytes: accounting_slack,
        warmup_bytes: 0,
        required_proof_bytes,
        written_bytes: sample_floor,
        written_data_frame_count: 32,
        receipt_confirmed: true,
        received_bytes: sample_floor,
        proof_elapsed,
        rate_bps: quic_capacity_receipt_rate_bps(sample_floor, proof_elapsed)
            .expect("valid receipt rate"),
        accepted_at,
        expires_at: accepted_at + Duration::from_secs(1),
        proof_validity: Duration::from_secs(1),
    };
    let metrics = PathMetrics {
        path_id: PathId(23),
        underlay: UnderlayProtocol::Udp,
        direction: PathMetricDirection::ServerToClient,
        metric_epoch: metric_epoch_now(),
        metric_age_us: u32::MAX,
        min_rtt_us: 20_000,
        srtt_us: 1,
        rttvar_us: 0,
        jitter_us: 0,
        delivery_rate_bps: 1,
        pacing_rate_bps: 1,
        loss_ppm: 0,
        ecn_ppm: 0,
        loss_observed: false,
        ecn_observed: false,
        bytes_in_flight: 0,
        queue_bytes: 0,
        inflight_limit_bytes: 1,
        inflight_hi_bytes: 1,
        confidence_ppm: 1_000_000,
        app_limited: true,
        has_ack_derived_data_sample: false,
        data_sample_count: 0,
        data_sample_bytes: 0,
    };
    let accepted = ServerPathMetricsEntry {
        metrics,
        source: ServerPathMetricsSource::LocalSender,
        recorded_at: accepted_at,
        capacity_proof: Some(candidate),
        tcp_capacity_proof: None,
    };
    assert!(server_path_metrics_has_bulk_rate_evidence(accepted));
    assert_eq!(
        server_path_metrics_rate_bps(accepted),
        candidate.rate_bps as f64
    );
    let (commands, _receivers) = reliable_path_command_channels(8);
    let output = ResponseStreamOutputEntry {
        key: CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: metrics.path_id,
        },
        path_instance_id: next_server_carrier_path_instance_id(),
        incarnation: 1,
        commands,
        role: StreamOpenRole::Validation,
        owner_data_in_flight_bytes: 0,
        bytes_in_flight: 0,
        product_queue_bytes: 0,
        product_progress_rate_bps: None,
        delivery_rate_bps: None,
        tcp_ack_clock_rate_bps: None,
        tcp_product_rate_evidence: None,
        tcp_capacity_prior: None,
        srtt_ms: None,
        delivery_samples: 0,
        owner_data_acked_bytes: 0,
        local_path_metrics: Some(accepted),
        peer_path_metrics: None,
    };
    assert_eq!(
        server_output_confidence(&output, Instant::now()),
        1.0,
        "exact receipt bytes establish confidence without generic ACK samples"
    );
    let lane_tracker = ServerPathLaneTracker::default();
    let accepted_snapshot = server_bulk_output_snapshot(
        &output,
        SessionId(77),
        FlowLane::Throughput,
        &lane_tracker,
        MuxLimits::default(),
        Instant::now(),
    );
    assert_eq!(
        accepted_snapshot.delivery_rate_bps,
        candidate.rate_bps as f64
    );

    let expired = ServerPathMetricsEntry {
        capacity_proof: Some(QuicCapacityProofCandidate {
            accepted_at: accepted_at - Duration::from_secs(2),
            expires_at: accepted_at - Duration::from_secs(1),
            ..candidate
        }),
        ..accepted
    };
    assert!(!server_path_metrics_has_bulk_rate_evidence(expired));
    assert_eq!(
        server_path_metrics_estimate_rate_bps(expired),
        candidate.rate_bps as f64
    );
    let mut expired_output = output;
    expired_output.local_path_metrics = Some(expired);
    let expired_snapshot = server_bulk_output_snapshot(
        &expired_output,
        SessionId(77),
        FlowLane::Throughput,
        &lane_tracker,
        MuxLimits::default(),
        Instant::now(),
    );
    assert_eq!(
        expired_snapshot.delivery_rate_bps,
        candidate.rate_bps as f64
    );
    assert!(!server_output_has_bulk_rate_evidence(&expired_output));
}

#[test]
fn udp_bulk_rate_evidence_requires_source_fresh_non_app_limited_state() {
    let (commands, _receivers) = reliable_path_command_channels(8);
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    let mut entry = ResponseStreamOutputEntry {
        key,
        path_instance_id: next_server_carrier_path_instance_id(),
        incarnation: 1,
        commands,
        role: StreamOpenRole::Active,
        owner_data_in_flight_bytes: 0,
        bytes_in_flight: 0,
        product_queue_bytes: 0,
        product_progress_rate_bps: Some(500_000_000.0),
        delivery_rate_bps: None,
        tcp_ack_clock_rate_bps: None,
        tcp_product_rate_evidence: None,
        tcp_capacity_prior: None,
        srtt_ms: None,
        delivery_samples: 0,
        owner_data_acked_bytes: 0,
        local_path_metrics: Some(ServerPathMetricsEntry {
            source: ServerPathMetricsSource::LocalSender,
            recorded_at: Instant::now(),
            capacity_proof: None,
            tcp_capacity_proof: None,
            metrics: PathMetrics {
                path_id: key.path_id,
                underlay: key.underlay,
                direction: PathMetricDirection::ServerToClient,
                metric_epoch: metric_epoch_now(),
                metric_age_us: 0,
                min_rtt_us: 20_000,
                srtt_us: 20_000,
                rttvar_us: 1_000,
                jitter_us: 1_000,
                delivery_rate_bps: 200_000_000,
                pacing_rate_bps: 200_000_000,
                loss_ppm: 0,
                ecn_ppm: 0,
                loss_observed: false,
                ecn_observed: false,
                bytes_in_flight: PATH_OPEN_SCORE_BYTES as u64,
                queue_bytes: 0,
                inflight_limit_bytes: 4 * PATH_OPEN_SCORE_BYTES as u64,
                inflight_hi_bytes: 4 * PATH_OPEN_SCORE_BYTES as u64,
                confidence_ppm: 1_000_000,
                app_limited: true,
                has_ack_derived_data_sample: true,
                data_sample_count: 32,
                data_sample_bytes: 4 * PATH_OPEN_SCORE_BYTES as u64,
            },
        }),
        peer_path_metrics: None,
    };

    assert!(
        !server_output_has_bulk_rate_evidence(&entry),
        "the QUIC tracker keeps fresh historical proof non-app-limited; a published app-limited state cannot authorize placement"
    );
    assert!(
        server_output_has_service_feed_evidence_with_limits(&entry, MuxLimits::default()),
        "a substantial local ACK-derived QUIC sample may feed its current Service while native QUIC still owns cwnd and pacing"
    );
    assert!(server_output_has_sender_evidence(&entry));
    let snapshot = server_bulk_output_snapshot(
        &entry,
        SessionId(77),
        FlowLane::Throughput,
        &ServerPathLaneTracker::default(),
        MuxLimits::default(),
        Instant::now(),
    );
    assert_eq!(snapshot.delivery_rate_bps, 200_000_000.0);
    assert!(
        !server_output_has_bulk_rate_evidence(&entry),
        "retaining a QUIC bandwidth estimate must not mint placement authority"
    );

    entry
        .local_path_metrics
        .as_mut()
        .expect("local QUIC sender metrics")
        .metrics
        .app_limited = false;
    assert!(
        server_output_has_bulk_rate_evidence(&entry),
        "the same full-volume sample becomes optional-path proof only after the carrier reports non-app-limited delivery"
    );
}
