use super::super::next_server_carrier_path_instance_id;
use super::super::response_evidence::{ServerPathMetricsEntry, ServerPathMetricsSource};
use super::super::response_session::ServerPathLaneTracker;
use super::super::response_snapshot::server_bulk_output_snapshot;
use super::super::response_topology::ResponseStreamOutputEntry;
use super::{
    server_output_has_bulk_rate_evidence, server_output_has_bulk_rate_evidence_with_limits,
    server_output_has_durable_product_progress, server_output_has_sender_evidence,
    server_output_has_service_feed_evidence_with_limits,
};
use crate::model::capacity::{
    BBR_MAX_SEND_QUANTUM_BYTES, RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES,
    reliable_subflow_startup_sample_limit_bytes,
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
fn udp_product_ack_without_unique_owner_rate_is_sender_evidence_not_bulk_rate() {
    let (commands, _receivers) = reliable_path_command_channels(8);
    let entry = ResponseStreamOutputEntry {
        key: CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        },
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
        delivery_samples: 1,
        owner_data_acked_bytes: 0,
        local_path_metrics: None,
        peer_path_metrics: None,
    };

    assert!(
        server_output_has_sender_evidence(&entry),
        "product ACK samples still prove end-to-end sender progress"
    );
    assert!(
        !server_output_has_bulk_rate_evidence(&entry),
        "a UDP product ACK without a path-scoped owner rate is sender evidence, not bulk-rate evidence"
    );
}

#[test]
fn udp_unique_owner_ack_product_rate_does_not_replace_carrier_rate() {
    let (commands, _receivers) = reliable_path_command_channels(8);
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    let product_rate = 42_000_000.0;
    let mut entry = ResponseStreamOutputEntry {
        key,
        path_instance_id: next_server_carrier_path_instance_id(),
        incarnation: 1,
        commands,
        role: StreamOpenRole::Active,
        owner_data_in_flight_bytes: 0,
        bytes_in_flight: 0,
        product_queue_bytes: 0,
        product_progress_rate_bps: Some(product_rate),
        delivery_rate_bps: None,
        tcp_ack_clock_rate_bps: None,
        tcp_product_rate_evidence: None,
        tcp_capacity_prior: None,
        srtt_ms: None,
        delivery_samples: 1,
        owner_data_acked_bytes: reliable_subflow_startup_sample_limit_bytes(MuxLimits::default()),
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
                delivery_rate_bps: default_path_rate_bps(UnderlayProtocol::Udp).round() as u64,
                pacing_rate_bps: 200_000_000,
                loss_ppm: 0,
                ecn_ppm: 0,
                loss_observed: false,
                ecn_observed: false,
                bytes_in_flight: 0,
                queue_bytes: 0,
                inflight_limit_bytes: RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES,
                inflight_hi_bytes: RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES,
                confidence_ppm: 1_000_000,
                app_limited: true,
                has_ack_derived_data_sample: true,
                data_sample_count: 0,
                data_sample_bytes: 0,
            },
        }),
        peer_path_metrics: None,
    };

    assert!(!server_output_has_bulk_rate_evidence(&entry));
    assert!(
        server_output_has_service_feed_evidence_with_limits(&entry, MuxLimits::default()),
        "unique product ACK progress may release the current QUIC Service feed without replacing carrier rate"
    );
    let snapshot = server_bulk_output_snapshot(
        &entry,
        SessionId(78),
        FlowLane::Throughput,
        &ServerPathLaneTracker::default(),
        MuxLimits::default(),
        Instant::now(),
    );
    assert_eq!(
        snapshot.delivery_rate_bps,
        default_path_rate_bps(UnderlayProtocol::Udp),
        "product STREAM_ACK timing is backlog evidence, not QUIC carrier delivery rate"
    );
    assert_eq!(snapshot.product_progress_rate_bps, Some(product_rate));
    assert!(snapshot.has_durable_product_progress);
    assert_eq!(
        snapshot.pacing_rate_bps, 200_000_000.0,
        "local QUIC pacing remains carrier-owned scheduling evidence even when the carrier ACK sample is app-limited"
    );

    entry.product_progress_rate_bps = None;
    let fragmented_snapshot = server_bulk_output_snapshot(
        &entry,
        SessionId(78),
        FlowLane::Throughput,
        &ServerPathLaneTracker::default(),
        MuxLimits::default(),
        Instant::now(),
    );
    assert!(fragmented_snapshot.has_durable_product_progress);
    assert!(fragmented_snapshot.product_progress_rate_bps.is_none());
    assert!(!server_output_has_durable_product_progress(
        &entry,
        MuxLimits::default()
    ));
}

#[test]
fn one_owner_quantum_is_sender_evidence_but_not_bulk_rate_proof() {
    let mux_limits = MuxLimits::default();
    let sample_floor = reliable_subflow_startup_sample_limit_bytes(mux_limits);
    assert!(sample_floor > BBR_MAX_SEND_QUANTUM_BYTES as u64);

    for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
        let (commands, _receivers) = reliable_path_command_channels(8);
        let entry = ResponseStreamOutputEntry {
            key: CarrierPathKey {
                underlay,
                path_id: PathId(13),
            },
            path_instance_id: next_server_carrier_path_instance_id(),
            incarnation: 1,
            commands,
            role: StreamOpenRole::Validation,
            owner_data_in_flight_bytes: 0,
            bytes_in_flight: 0,
            product_queue_bytes: 0,
            product_progress_rate_bps: Some(80_000_000.0),
            delivery_rate_bps: (underlay == UnderlayProtocol::Tcp).then_some(80_000_000.0),
            tcp_ack_clock_rate_bps: None,
            tcp_product_rate_evidence: None,
            tcp_capacity_prior: None,
            srtt_ms: Some(40.0),
            delivery_samples: 1,
            owner_data_acked_bytes: BBR_MAX_SEND_QUANTUM_BYTES as u64,
            local_path_metrics: None,
            peer_path_metrics: None,
        };

        assert!(server_output_has_sender_evidence(&entry), "{underlay:?}");
        assert!(
            !server_output_has_durable_product_progress(&entry, mux_limits),
            "{underlay:?} one-quantum point rate is not durable product progress"
        );
        assert!(
            !server_output_has_bulk_rate_evidence_with_limits(&entry, mux_limits),
            "{underlay:?} must not graduate from one application-limited OwnerData quantum"
        );
    }
}
