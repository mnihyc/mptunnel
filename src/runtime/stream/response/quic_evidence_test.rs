use super::super::attachment::ResponseStreamOutputEntry;
use super::super::next_server_carrier_path_instance_id;
use super::super::session::ServerPathLaneTracker;
use super::super::snapshot::server_bulk_output_snapshot;
use super::super::subflow::{
    server_output_has_bulk_rate_evidence, server_output_has_sender_evidence,
};
use super::{
    ServerPathMetricsEntry, ServerPathMetricsSource, server_path_metrics_has_ack_data_evidence,
};
use crate::model::capacity::{
    PATH_OPEN_SCORE_BYTES, RELIABLE_INITIAL_WINDOW_PACKETS,
    RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES, TRANSPORT_MSS_BYTES,
};
use crate::model::path::CarrierPathKey;
use crate::mux::MuxLimits;
use crate::protocol::{
    PathId, PathMetricDirection, PathMetrics, SessionId, StreamOpenRole, UnderlayProtocol,
};
use crate::runtime::path::commands::reliable_path_command_channels;
use crate::runtime::path::model::{default_path_rate_bps, metric_epoch_now};
use crate::scheduler::FlowLane;
use std::time::Instant;

#[test]
fn udp_app_limited_ack_data_snapshot_keeps_carrier_inflight_limit() {
    let (commands, _receivers) = reliable_path_command_channels(8);
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(7),
    };
    let entry = ResponseStreamOutputEntry {
        key,
        path_instance_id: next_server_carrier_path_instance_id(),
        incarnation: 1,
        commands,
        role: StreamOpenRole::Active,
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
            metrics: PathMetrics {
                path_id: key.path_id,
                underlay: key.underlay,
                direction: PathMetricDirection::ServerToClient,
                metric_epoch: metric_epoch_now(),
                metric_age_us: 0,
                min_rtt_us: 80_000,
                srtt_us: 80_000,
                rttvar_us: 2_000,
                jitter_us: 2_000,
                delivery_rate_bps: default_path_rate_bps(UnderlayProtocol::Udp).round() as u64,
                pacing_rate_bps: default_path_rate_bps(UnderlayProtocol::Udp).round() as u64,
                loss_ppm: 0,
                ecn_ppm: 0,
                loss_observed: false,
                ecn_observed: false,
                bytes_in_flight: 0,
                queue_bytes: 0,
                inflight_limit_bytes: 2 * 1024 * 1024,
                inflight_hi_bytes: 2 * 1024 * 1024,
                confidence_ppm: 0,
                app_limited: true,
                has_ack_derived_data_sample: true,
                data_sample_count: 0,
                data_sample_bytes: 0,
            },
        }),
        peer_path_metrics: None,
    };

    let lane_tracker = ServerPathLaneTracker::default();
    let snapshot = server_bulk_output_snapshot(
        &entry,
        SessionId(77),
        FlowLane::Throughput,
        &lane_tracker,
        MuxLimits::default(),
        Instant::now(),
    );

    assert_eq!(
        snapshot.delivery_rate_bps,
        default_path_rate_bps(UnderlayProtocol::Udp),
        "app-limited ACK-data must not create a tiny bulk-rate model"
    );
    assert_eq!(
        snapshot.inflight_limit_bytes,
        2 * 1024 * 1024,
        "carrier inflight credit is path-local QUIC state and remains usable for bounded exploration"
    );
    assert!(
        !server_output_has_bulk_rate_evidence(&entry),
        "ACK-data seen without non-app-limited samples is not ordinary bulk-rate proof"
    );
}

#[test]
fn local_proof_metrics_are_sender_evidence_not_ack_data_evidence() {
    let (commands, _receivers) = reliable_path_command_channels(8);
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(10),
    };
    let entry = ResponseStreamOutputEntry {
        key,
        path_instance_id: next_server_carrier_path_instance_id(),
        incarnation: 1,
        commands,
        role: StreamOpenRole::Active,
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
            metrics: PathMetrics {
                path_id: key.path_id,
                underlay: key.underlay,
                direction: PathMetricDirection::ServerToClient,
                metric_epoch: metric_epoch_now(),
                metric_age_us: 0,
                min_rtt_us: 40_000,
                srtt_us: 40_000,
                rttvar_us: 2_000,
                jitter_us: 2_000,
                delivery_rate_bps: 32_000,
                pacing_rate_bps: 32_000,
                loss_ppm: 0,
                ecn_ppm: 0,
                loss_observed: false,
                ecn_observed: false,
                bytes_in_flight: 0,
                queue_bytes: 0,
                inflight_limit_bytes: PATH_OPEN_SCORE_BYTES as u64,
                inflight_hi_bytes: PATH_OPEN_SCORE_BYTES as u64,
                confidence_ppm: 1_000_000,
                app_limited: true,
                has_ack_derived_data_sample: false,
                data_sample_count: 0,
                data_sample_bytes: 0,
            },
        }),
        peer_path_metrics: None,
    };

    assert!(server_output_has_sender_evidence(&entry));
    assert!(!server_output_has_bulk_rate_evidence(&entry));
}

#[test]
fn udp_tiny_non_app_limited_sample_is_ack_data_not_bulk_rate_evidence() {
    let (commands, _receivers) = reliable_path_command_channels(8);
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(9),
    };
    let sample_floor = 2 * 1024 * 1024;
    let entry = ResponseStreamOutputEntry {
        key,
        path_instance_id: next_server_carrier_path_instance_id(),
        incarnation: 1,
        commands,
        role: StreamOpenRole::Active,
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
            metrics: PathMetrics {
                path_id: key.path_id,
                underlay: key.underlay,
                direction: PathMetricDirection::ServerToClient,
                metric_epoch: metric_epoch_now(),
                metric_age_us: 0,
                min_rtt_us: 80_000,
                srtt_us: 80_000,
                rttvar_us: 2_000,
                jitter_us: 2_000,
                delivery_rate_bps: 12_000_000,
                pacing_rate_bps: 12_000_000,
                loss_ppm: 0,
                ecn_ppm: 0,
                loss_observed: false,
                ecn_observed: false,
                bytes_in_flight: 0,
                queue_bytes: 0,
                inflight_limit_bytes: sample_floor,
                inflight_hi_bytes: sample_floor,
                confidence_ppm: 1_000_000,
                app_limited: false,
                has_ack_derived_data_sample: true,
                data_sample_count: 4,
                data_sample_bytes: PATH_OPEN_SCORE_BYTES as u64,
            },
        }),
        peer_path_metrics: None,
    };

    assert!(matches!(
        entry.local_path_metrics,
        Some(path_metrics) if server_path_metrics_has_ack_data_evidence(path_metrics)
    ));
    assert!(
        !server_output_has_bulk_rate_evidence(&entry),
        "bulk-rate promotion requires enough ACKed byte volume, not just non-app-limited ACK count"
    );
    let snapshot = server_bulk_output_snapshot(
        &entry,
        SessionId(77),
        FlowLane::Throughput,
        &ServerPathLaneTracker::default(),
        MuxLimits::default(),
        Instant::now(),
    );
    assert_eq!(
        snapshot.delivery_rate_bps,
        default_path_rate_bps(UnderlayProtocol::Udp)
    );
    assert!(snapshot.confidence < 1.0);
}

#[test]
fn udp_startup_window_sample_graduates_even_when_inflight_limit_is_larger() {
    let (commands, _receivers) = reliable_path_command_channels(8);
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(11),
    };
    let sample_bytes = RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES / 2;
    let entry = ResponseStreamOutputEntry {
        key,
        path_instance_id: next_server_carrier_path_instance_id(),
        incarnation: 1,
        commands,
        role: StreamOpenRole::Active,
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
            metrics: PathMetrics {
                path_id: key.path_id,
                underlay: key.underlay,
                direction: PathMetricDirection::ServerToClient,
                metric_epoch: metric_epoch_now(),
                metric_age_us: 0,
                min_rtt_us: 160_000,
                srtt_us: 160_000,
                rttvar_us: 5_000,
                jitter_us: 5_000,
                delivery_rate_bps: 42_000_000,
                pacing_rate_bps: 42_000_000,
                loss_ppm: 0,
                ecn_ppm: 0,
                loss_observed: false,
                ecn_observed: false,
                bytes_in_flight: 0,
                queue_bytes: 0,
                inflight_limit_bytes: RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES,
                inflight_hi_bytes: RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES,
                confidence_ppm: 1_000_000,
                app_limited: false,
                has_ack_derived_data_sample: true,
                data_sample_count: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
                data_sample_bytes: sample_bytes,
            },
        }),
        peer_path_metrics: None,
    };

    assert!(
        server_output_has_bulk_rate_evidence(&entry),
        "a path with substantial non-app-limited QUIC ACK-derived product data must not be trapped below a transient inflight-limit floor"
    );
}

#[test]
fn udp_near_startup_window_sample_graduates_with_packet_accounting_slack() {
    let (commands, _receivers) = reliable_path_command_channels(8);
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(12),
    };
    let sample_floor = RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES / 2;
    let entry = ResponseStreamOutputEntry {
        key,
        path_instance_id: next_server_carrier_path_instance_id(),
        incarnation: 1,
        commands,
        role: StreamOpenRole::Active,
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
            metrics: PathMetrics {
                path_id: key.path_id,
                underlay: key.underlay,
                direction: PathMetricDirection::ServerToClient,
                metric_epoch: metric_epoch_now(),
                metric_age_us: 0,
                min_rtt_us: 160_000,
                srtt_us: 160_000,
                rttvar_us: 5_000,
                jitter_us: 5_000,
                delivery_rate_bps: 42_000_000,
                pacing_rate_bps: 42_000_000,
                loss_ppm: 0,
                ecn_ppm: 0,
                loss_observed: false,
                ecn_observed: false,
                bytes_in_flight: 0,
                queue_bytes: 0,
                inflight_limit_bytes: RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES,
                inflight_hi_bytes: RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES,
                confidence_ppm: 1_000_000,
                app_limited: false,
                has_ack_derived_data_sample: true,
                data_sample_count: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
                data_sample_bytes: sample_floor.saturating_sub(TRANSPORT_MSS_BYTES as u64),
            },
        }),
        peer_path_metrics: None,
    };

    assert!(
        server_output_has_bulk_rate_evidence(&entry),
        "bulk-rate graduation should tolerate packet-accounting slack around the startup evidence floor"
    );
}
