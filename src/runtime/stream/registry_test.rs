use super::*;
use crate::model::capacity::QuicCapacityProofCandidate;
use crate::runtime::path::commands::reliable_path_command_channels;
use crate::runtime::path::model::metric_epoch_now;
use crate::runtime::stream::response::{ServerPathMetricsEntry, ServerPathMetricsSource};
use std::net::SocketAddr;
use std::time::Duration;

fn receipt_test_metrics(path_id: PathId) -> PathMetrics {
    PathMetrics {
        path_id,
        underlay: UnderlayProtocol::Udp,
        direction: PathMetricDirection::ServerToClient,
        metric_epoch: metric_epoch_now(),
        metric_age_us: 0,
        min_rtt_us: 20_000,
        srtt_us: 20_000,
        rttvar_us: 1_000,
        jitter_us: 0,
        delivery_rate_bps: 1,
        pacing_rate_bps: 1,
        loss_ppm: 0,
        ecn_ppm: 0,
        loss_observed: false,
        ecn_observed: false,
        bytes_in_flight: 0,
        queue_bytes: 0,
        inflight_limit_bytes: 100,
        inflight_hi_bytes: 100,
        confidence_ppm: 0,
        app_limited: true,
        has_ack_derived_data_sample: false,
        data_sample_count: 0,
        data_sample_bytes: 0,
    }
}

#[test]
fn late_open_and_closed_output_replacement_inherit_capacity_receipt() {
    let registry = Arc::new(ServerReliableStreamRegistry::new(16));
    let session_id = SessionId(701);
    let stream_id = StreamId(9);
    let path_id = PathId(3);
    let registration = registry.register_carrier_path(session_id, UnderlayProtocol::Udp, path_id);
    let path_instance_id = registration.path_instance_id();
    let metrics = receipt_test_metrics(path_id);
    let accepted_at = Instant::now();
    let candidate = QuicCapacityProofCandidate {
        token: 41,
        train_bytes: 100,
        sample_floor_bytes: 100,
        accounting_slack_bytes: 12,
        warmup_bytes: 0,
        required_proof_bytes: 88,
        written_bytes: 100,
        written_data_frame_count: 1,
        receipt_confirmed: true,
        received_bytes: 100,
        proof_elapsed: Duration::from_millis(10),
        rate_bps: 80_000,
        accepted_at,
        expires_at: accepted_at + Duration::from_secs(1),
        proof_validity: Duration::from_secs(1),
    };
    registry
        .path_metrics
        .lock()
        .expect("test path metrics lock")
        .insert(
            (session_id, UnderlayProtocol::Udp, path_id, path_instance_id),
            ServerPathMetricsEntry {
                metrics,
                source: ServerPathMetricsSource::LocalSender,
                recorded_at: Instant::now(),
                capacity_proof: Some(candidate),
                tcp_capacity_proof: None,
            },
        );

    let target = TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80)));
    let (commands, first_receivers) = reliable_path_command_channels(8);
    let stream = match registry
        .open_or_attach(
            ServerReliableStreamOpenRequest {
                session_id,
                stream_id,
                target: &target,
                lane: FlowLane::Throughput,
                attachment: ServerReliablePathAttachment {
                    path_registration: registration.clone(),
                    commands,
                    max_frame_payload_bytes: MuxLimits::default().max_payload_bytes,
                    role: StreamOpenRole::Active,
                    initial_metrics: Some(metrics),
                },
            },
            MuxLimits::default(),
            16,
        )
        .expect("open response stream")
    {
        ServerReliableStreamOpen::New(stream) => stream,
        _ => panic!("expected new response stream"),
    };
    let ReliablePathStreamOutput::Switchable(binding) = &stream.output else {
        panic!("expected switchable response binding");
    };
    let inherited = binding
        .sender_path_targets(FlowLane::Throughput, 1)
        .into_iter()
        .find(|target| target.key.path_id == path_id)
        .expect("inherited receipt target");
    assert!(inherited.has_bulk_rate_evidence);
    assert_eq!(inherited.snapshot.confidence, 1.0);

    drop(first_receivers);
    let (replacement_commands, _replacement_receivers) = reliable_path_command_channels(8);
    assert!(matches!(
        registry
            .open_or_attach(
                ServerReliableStreamOpenRequest {
                    session_id,
                    stream_id,
                    target: &target,
                    lane: FlowLane::Throughput,
                    attachment: ServerReliablePathAttachment {
                        path_registration: registration.clone(),
                        commands: replacement_commands,
                        max_frame_payload_bytes: MuxLimits::default().max_payload_bytes,
                        role: StreamOpenRole::Active,
                        initial_metrics: Some(metrics),
                    },
                },
                MuxLimits::default(),
                16,
            )
            .expect("replace closed response output"),
        ServerReliableStreamOpen::Existing
    ));
    let replacement = binding
        .sender_path_targets(FlowLane::Throughput, 1)
        .into_iter()
        .find(|target| target.key.path_id == path_id)
        .expect("replacement receipt target");
    assert!(replacement.has_bulk_rate_evidence);
    assert_eq!(replacement.snapshot.confidence, 1.0);
}
