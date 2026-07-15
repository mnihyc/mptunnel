use super::super::RequestSenderService;
use super::super::test_support::*;
use super::*;
use crate::runtime::path::commands::{
    QuicCapacityProbeOwner, ReliablePathCommand, reliable_path_command_channels,
    try_recv_reliable_path_command,
};

#[tokio::test]
async fn request_quic_single_path_skips_health_lock() {
    let stream_id = StreamId(208);
    let context =
        client_test_context_with_paths(&["udp://127.0.0.1:10328?srtt-ms=20&rate-mbps=500"]);
    let (service_commands, _service_rx) = reliable_path_command_channels(8);
    let remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream_with_underlay(
            stream_id,
            UnderlayProtocol::Udp,
            0,
            service_commands,
        ),
        8,
    );
    let service = remotes.active_path_instance().expect("Active Service");
    let mut sender = RequestSenderService::new(stream_id);
    sender.request.ordered_service = Some(service);
    poison_client_path_health_for_test(&context);

    sender.try_start_request_quic_capacity_calibration(&context, &remotes, FlowLane::Throughput);

    assert!(sender.quic_capacity.active.is_none());
}

#[tokio::test]
async fn request_quic_capacity_skips_an_earlier_exhausted_path_share() {
    let stream_id = StreamId(213);
    let context = client_test_context_with_paths(&[
        "udp://127.0.0.1:10339?srtt-ms=20&rate-mbps=500",
        "udp://127.0.0.1:10340?srtt-ms=40&rate-mbps=500",
        "udp://127.0.0.1:10341?srtt-ms=80&rate-mbps=500",
    ]);
    let (service_commands, _service_rx) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream_with_underlay(
            stream_id,
            UnderlayProtocol::Udp,
            0,
            service_commands,
        ),
        8,
    );
    let (first_commands, mut first_rx) = reliable_path_command_channels(8);
    let (second_commands, mut second_rx) = reliable_path_command_channels(8);
    remotes.attach_for_validation(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        1,
        first_commands,
    ));
    remotes.attach_for_validation(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        2,
        second_commands,
    ));
    consume_client_validation_proof_for_test(&mut first_rx);
    consume_client_validation_proof_for_test(&mut second_rx);

    let service = remotes.active_path_instance().expect("Active Service");
    let candidate = |index| {
        remotes
            .paths
            .iter()
            .find(|path| path.key().index == index)
            .expect("Validation candidate")
    };
    let first = candidate(1).instance();
    let second = candidate(2).instance();
    for instance in [first, second] {
        mark_client_validation_proof_fresh_for_test(
            &context,
            &remotes,
            instance,
            Duration::from_millis(10),
        );
    }
    seed_client_bulk_evidence_for_test(&context, service.key);
    context.health().lock().expect("path health lock").udp[service.key.index]
        .relay_bytes_in_flight = reliable_subflow_startup_sample_limit_bytes(context.mux_limits);

    let eligible_candidates =
        context.automatic_bulk_path_count(UnderlayProtocol::Udp, Some(service.key.index));
    assert_eq!(eligible_candidates, 2);
    let stable_share =
        request_capacity_stable_candidate_share_bytes(context.mux_limits, eligible_candidates);
    let now = Instant::now();
    let mut spent = context
        .try_reserve_request_quic_capacity_probe(
            StreamId(212),
            first.key.index,
            first,
            9_100,
            stable_share,
            stable_share,
            Arc::new(RequestCapacityProbeCampaignBudget::default()),
            candidate(1).attached_at,
            now + Duration::from_secs(1),
            Duration::from_secs(1),
            CapacityProbeCommandTicket::new(),
        )
        .expect("reserve the earlier path's complete fixed share");
    spent.commit();
    drop(spent);
    assert_eq!(
        context.request_quic_capacity_probe_path_remaining_bytes(first.key.index, stable_share,),
        0
    );

    let mut sender = RequestSenderService::new(stream_id);
    sender.request.ordered_service = Some(service);
    sender.try_start_request_quic_capacity_calibration(&context, &remotes, FlowLane::Throughput);

    assert!(try_recv_reliable_path_command(&mut first_rx).is_none());
    let probe = match try_recv_reliable_path_command(&mut second_rx) {
        Some(ReliablePathCommand::SendQuicCapacityProbe(probe)) => probe,
        _ => panic!("the first viable later candidate must receive the probe"),
    };
    assert!(matches!(
        probe.owner,
        QuicCapacityProbeOwner::Request { path_instance, .. } if path_instance == second
    ));
}

#[tokio::test]
async fn request_quic_train_waits_for_candidate_latency_pressure_to_clear() {
    let stream_id = StreamId(210);
    let context = client_test_context_with_paths(&[
        "udp://127.0.0.1:10326?srtt-ms=20&rate-mbps=500",
        "udp://127.0.0.1:10327?srtt-ms=10&rate-mbps=500",
    ]);
    let (service_commands, _service_rx) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream_with_underlay(
            stream_id,
            UnderlayProtocol::Udp,
            0,
            service_commands,
        ),
        8,
    );
    let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(8);
    remotes.attach_for_validation(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        1,
        candidate_commands,
    ));
    consume_client_validation_proof_for_test(&mut candidate_rx);
    let service = remotes.active_path_instance().expect("Active Service");
    let candidate = remotes
        .paths
        .iter()
        .find(|path| path.placement == RelayPathPlacement::Validation)
        .expect("Validation candidate")
        .instance();
    mark_client_validation_proof_fresh_for_test(
        &context,
        &remotes,
        candidate,
        Duration::from_millis(10),
    );
    seed_client_bulk_evidence_for_test(&context, service.key);
    context.health().lock().expect("path health lock").udp[service.key.index]
        .relay_bytes_in_flight = reliable_subflow_startup_sample_limit_bytes(context.mux_limits);
    context.health().lock().expect("path health lock").udp[candidate.key.index]
        .reserve_load(FlowLane::Latency);

    let mut sender = RequestSenderService::new(stream_id);
    sender.request.ordered_service = Some(service);
    sender.try_start_request_quic_capacity_calibration(&context, &remotes, FlowLane::Throughput);
    assert!(sender.quic_capacity.active.is_none());
    assert!(sender.quic_capacity.attempted_paths.is_empty());

    context.health().lock().expect("path health lock").udp[candidate.key.index]
        .release_load(FlowLane::Latency);
    sender.try_start_request_quic_capacity_calibration(&context, &remotes, FlowLane::Throughput);
    assert!(sender.quic_capacity.active.is_some());
    assert!(matches!(
        try_recv_reliable_path_command(&mut candidate_rx),
        Some(ReliablePathCommand::SendQuicCapacityProbe(_))
    ));
}
