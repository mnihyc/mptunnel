use super::super::next_server_carrier_path_instance_id;
use super::*;
use crate::model::capacity::reliable_subflow_startup_sample_limit_bytes;
use crate::runtime::path::commands::reliable_path_command_channels;

#[test]
fn tcp_response_snapshot_persistent_delivery_samples_override_default_prior() {
    let (commands, _receivers) = reliable_path_command_channels(8);
    let prior_rate = default_path_rate_bps(UnderlayProtocol::Tcp);
    let entry = ResponseStreamOutputEntry {
        key: CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        },
        path_instance_id: next_server_carrier_path_instance_id(),
        incarnation: 1,
        commands,
        role: StreamOpenRole::Active,
        owner_data_in_flight_bytes: 0,
        bytes_in_flight: 0,
        product_queue_bytes: 0,
        product_progress_rate_bps: Some(prior_rate / 10.0),
        delivery_rate_bps: Some(prior_rate / 10.0),
        tcp_ack_clock_rate_bps: None,
        tcp_product_rate_evidence: None,
        tcp_capacity_prior: None,
        srtt_ms: Some(default_path_srtt_ms(UnderlayProtocol::Tcp)),
        delivery_samples: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
        owner_data_acked_bytes: reliable_subflow_startup_sample_limit_bytes(MuxLimits::default()),
        local_path_metrics: None,
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

    assert_eq!(snapshot.delivery_rate_bps, prior_rate / 10.0);
}

#[test]
fn response_eta_uses_delivered_rate_not_inflated_quic_pacing_rate() {
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(2),
    };
    let mut baseline = PathSnapshot::new(key.path_id, key.underlay, 100.0, 50_000_000.0);
    baseline.pacing_rate_bps = 50_000_000.0;
    baseline.confidence = 1.0;

    let mut inflated_pacing = baseline;
    inflated_pacing.pacing_rate_bps = 5_000_000_000.0;

    let payload_bytes = 64 * 1024;
    let baseline_eta = server_bulk_output_eta_ms(
        key,
        baseline,
        Some(key),
        FlowLane::Throughput,
        payload_bytes,
        MuxLimits::default(),
    );
    let inflated_eta = server_bulk_output_eta_ms(
        key,
        inflated_pacing,
        Some(key),
        FlowLane::Throughput,
        payload_bytes,
        MuxLimits::default(),
    );

    assert!(
        (baseline_eta - inflated_eta).abs() < 0.001,
        "QUIC pacing is carrier send permission, not delivered product throughput"
    );
}

#[test]
fn response_subflow_eta_uses_owner_quantum_not_service_horizon() {
    let service = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    let subflow = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let mut snapshot = PathSnapshot::new(subflow.path_id, subflow.underlay, 80.0, 40_000_000.0);
    snapshot.confidence = 1.0;

    let eta_ms = server_bulk_output_eta_ms(
        subflow,
        snapshot,
        Some(service),
        FlowLane::Throughput,
        64 * 1024,
        MuxLimits::default(),
    );

    assert!(
        eta_ms < 100.0,
        "Subflow ETA must model the next assigned owner range, not a full Service horizon; got {eta_ms:.3}ms"
    );
}
