use super::super::test_support::*;
use super::*;
use crate::model::capacity::{
    RELIABLE_INITIAL_WINDOW_PACKETS, reliable_capacity_calibration_session_limit_bytes,
};
use crate::model::request::capacity::{
    request_capacity_stable_candidate_share_bytes, request_tcp_capacity_calibration_geometry,
};
use crate::model::request::evidence::RequestPerFlowRateModel;
use crate::runtime::path::commands::{
    ReliablePathCommand, TcpCapacityProbeOwner, reliable_path_command_channels,
    try_recv_reliable_path_command,
};
use crate::runtime::stream::request::RequestStreamState;
use std::time::Duration;

#[tokio::test]
async fn request_tcp_capacity_batches_only_policy_eligible_sockets() {
    let stream_id = StreamId(211);
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10330?srtt-ms=20&rate-mbps=500",
        "tcp://127.0.0.1:10331?srtt-ms=10&rate-mbps=500&expensive=true",
        "tcp://127.0.0.1:10332?srtt-ms=80&rate-mbps=500",
        "tcp://127.0.0.1:10333?srtt-ms=160&rate-mbps=500",
    ]);
    let (service_commands, _service_rx) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, service_commands), 8);
    let (forbidden_commands, mut forbidden_rx) = reliable_path_command_channels(8);
    let (first_commands, mut first_rx) = reliable_path_command_channels(8);
    let (second_commands, mut second_rx) = reliable_path_command_channels(8);
    remotes.attach_for_validation(opened_test_relay_stream(stream_id, 1, forbidden_commands));
    remotes.attach_for_validation(opened_test_relay_stream(stream_id, 2, first_commands));
    remotes.attach_for_validation(opened_test_relay_stream(stream_id, 3, second_commands));
    consume_client_validation_proof_for_test(&mut forbidden_rx);
    consume_client_validation_proof_for_test(&mut first_rx);
    consume_client_validation_proof_for_test(&mut second_rx);

    let service = remotes.active_path_instance().expect("Active Service");
    let instance = |index| {
        remotes
            .paths
            .iter()
            .find(|path| path.key().index == index)
            .expect("attached path")
            .instance()
    };
    let forbidden = instance(1);
    let first = instance(2);
    let second = instance(3);
    for candidate in [forbidden, first, second] {
        mark_client_validation_proof_fresh_for_test(
            &context,
            &remotes,
            candidate,
            Duration::from_millis(10),
        );
    }
    seed_client_bulk_evidence_for_test(&context, service.key);
    let registration = context.reliable_tcp_request_bulk_flow_registration();
    registration.update(true, Some(UnderlayProtocol::Tcp));
    let service_model = RequestPerFlowRateModel {
        rate_bps: 100_000_000.0,
        delivery_samples: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
    };
    let mut request = RequestStreamState::default();
    let mut controller = RequestTcpCapacityController::default();
    request.ordered_service = Some(service);
    request
        .subflows
        .get_mut(service)
        .set_per_flow_rate(service_model);

    assert!(!context.relay_path_allows_automatic_bulk_use(forbidden.key));
    assert_eq!(
        context.automatic_bulk_path_count(UnderlayProtocol::Tcp, Some(service.key.index)),
        2,
        "the forbidden path must not dilute the train envelope"
    );
    let train_envelope = request_capacity_stable_candidate_share_bytes(context.mux_limits, 2);
    let expected_first = request_tcp_capacity_calibration_geometry(
        context
            .reliable_path_snapshot(first.key)
            .expect("first candidate snapshot"),
        service_model,
        context.mux_limits,
        train_envelope,
    )
    .expect("first train geometry");
    let expected_second = request_tcp_capacity_calibration_geometry(
        context
            .reliable_path_snapshot(second.key)
            .expect("second candidate snapshot"),
        service_model,
        context.mux_limits,
        train_envelope,
    )
    .expect("second train geometry");
    controller.try_start(
        stream_id,
        &request,
        &context,
        &remotes,
        FlowLane::Throughput,
    );

    assert!(try_recv_reliable_path_command(&mut forbidden_rx).is_none());
    let first_probe = match try_recv_reliable_path_command(&mut first_rx) {
        Some(ReliablePathCommand::SendTcpCapacityProbe(probe)) => probe,
        _ => panic!("expected first TCP capacity probe"),
    };
    let second_probe = match try_recv_reliable_path_command(&mut second_rx) {
        Some(ReliablePathCommand::SendTcpCapacityProbe(probe)) => probe,
        _ => panic!("expected second TCP capacity probe"),
    };
    assert_eq!(first_probe.train_payload_bytes, expected_first.train_bytes);
    assert_eq!(
        second_probe.train_payload_bytes,
        expected_second.train_bytes
    );
    assert_ne!(first_probe.calibration_id, second_probe.calibration_id);
    assert!(first_probe.valid_request_tcp_train());
    assert!(second_probe.valid_request_tcp_train());
    assert!(matches!(
        first_probe.owner,
        TcpCapacityProbeOwner::Request { path_instance, .. } if path_instance == first
    ));
    assert!(matches!(
        second_probe.owner,
        TcpCapacityProbeOwner::Request { path_instance, .. } if path_instance == second
    ));
    assert_eq!(
        controller.attempted_paths,
        HashSet::from([first.key.index, second.key.index])
    );
    assert_eq!(controller.calibrations.len(), 2);
    assert!(controller.calibrations.contains_key(&first));
    assert!(controller.calibrations.contains_key(&second));
    assert_eq!(
        context.automatic_bulk_path_count(UnderlayProtocol::Tcp, Some(service.key.index)),
        2,
        "attempted paths must not collapse the configured budget denominator"
    );
}

#[tokio::test]
async fn request_tcp_capacity_flow_campaign_rejects_third_parallel_train() {
    let stream_id = StreamId(214);
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10343?srtt-ms=20&rate-mbps=500",
        "tcp://127.0.0.1:10344?srtt-ms=80&rate-mbps=500",
        "tcp://127.0.0.1:10345?srtt-ms=220&rate-mbps=500",
        "tcp://127.0.0.1:10346?srtt-ms=340&rate-mbps=500",
        "tcp://127.0.0.1:10347?srtt-ms=420&rate-mbps=500",
    ]);
    let (service_commands, _service_rx) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, service_commands), 8);
    let mut candidate_receivers = Vec::new();
    for index in 1..=4 {
        let (commands, mut receiver) = reliable_path_command_channels(8);
        remotes.attach_for_validation(opened_test_relay_stream(stream_id, index, commands));
        consume_client_validation_proof_for_test(&mut receiver);
        candidate_receivers.push(receiver);
    }

    let service = remotes.active_path_instance().expect("Active Service");
    let candidates = (1..=4)
        .map(|index| {
            remotes
                .paths
                .iter()
                .find(|path| path.key().index == index)
                .expect("attached candidate")
                .instance()
        })
        .collect::<Vec<_>>();
    for (candidate, srtt_ms) in candidates.iter().copied().zip([80_u64, 220, 340, 420]) {
        mark_client_validation_proof_fresh_for_test(
            &context,
            &remotes,
            candidate,
            Duration::from_millis(srtt_ms),
        );
    }
    seed_client_bulk_evidence_for_test(&context, service.key);
    let registration = context.reliable_tcp_request_bulk_flow_registration();
    registration.update(true, Some(UnderlayProtocol::Tcp));
    let service_model = RequestPerFlowRateModel {
        rate_bps: 100_000_000.0,
        delivery_samples: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
    };
    let mut request = RequestStreamState::default();
    let mut controller = RequestTcpCapacityController::default();
    request.ordered_service = Some(service);
    request
        .subflows
        .get_mut(service)
        .set_per_flow_rate(service_model);

    let stable_share = request_capacity_stable_candidate_share_bytes(context.mux_limits, 4);
    let geometries = candidates
        .iter()
        .map(|candidate| {
            request_tcp_capacity_calibration_geometry(
                context
                    .reliable_path_snapshot(candidate.key)
                    .expect("candidate snapshot"),
                service_model,
                context.mux_limits,
                stable_share,
            )
            .expect("each train independently fits one candidate share")
        })
        .collect::<Vec<_>>();
    assert!(geometries[0].train_bytes + geometries[1].train_bytes <= stable_share);
    assert!(
        geometries[0].train_bytes + geometries[1].train_bytes + geometries[2].train_bytes
            > stable_share
    );

    controller.try_start(
        stream_id,
        &request,
        &context,
        &remotes,
        FlowLane::Throughput,
    );

    for (index, receiver) in candidate_receivers.iter_mut().enumerate() {
        let command = try_recv_reliable_path_command(receiver);
        if index < 2 {
            let probe = match command {
                Some(ReliablePathCommand::SendTcpCapacityProbe(probe)) => probe,
                _ => panic!("expected campaign-admitted TCP capacity probe"),
            };
            assert_eq!(probe.train_payload_bytes, geometries[index].train_bytes);
        } else {
            assert!(
                command.is_none(),
                "the flow campaign must reject every train beyond its residual share"
            );
        }
    }
    assert_eq!(
        controller.attempted_paths,
        HashSet::from([candidates[0].key.index, candidates[1].key.index])
    );
    assert_eq!(controller.calibrations.len(), 2);
    assert_eq!(
        controller.campaign.remaining_bytes(stable_share),
        stable_share - geometries[0].train_bytes - geometries[1].train_bytes
    );
    assert_eq!(
        context.request_tcp_capacity_probe_remaining_bytes(),
        reliable_capacity_calibration_session_limit_bytes(context.mux_limits)
            - geometries[0].train_bytes
            - geometries[1].train_bytes,
        "rejected flow work must preserve the session envelope for later streams"
    );
    let campaign_remaining = controller.campaign.remaining_bytes(stable_share);
    let session_remaining = context.request_tcp_capacity_probe_remaining_bytes();

    controller.try_start(
        stream_id,
        &request,
        &context,
        &remotes,
        FlowLane::Throughput,
    );

    assert!(
        candidate_receivers
            .iter_mut()
            .all(|receiver| try_recv_reliable_path_command(receiver).is_none()),
        "repeated planning must not reopen rejected campaign work"
    );
    assert_eq!(
        controller.campaign.remaining_bytes(stable_share),
        campaign_remaining
    );
    assert_eq!(
        context.request_tcp_capacity_probe_remaining_bytes(),
        session_remaining
    );
    assert_eq!(
        controller.attempted_paths,
        HashSet::from([candidates[0].key.index, candidates[1].key.index]),
        "campaign rejection is not a path retirement decision"
    );
}

#[tokio::test]
async fn request_tcp_stable_share_rejects_oversized_train_without_retiring_candidate() {
    let stream_id = StreamId(212);
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10334?srtt-ms=20&rate-mbps=500",
        "tcp://127.0.0.1:10335?srtt-ms=800&rate-mbps=500",
        "tcp://127.0.0.1:10336?srtt-ms=80&rate-mbps=500",
        "tcp://127.0.0.1:10337?srtt-ms=160&rate-mbps=500",
        "tcp://127.0.0.1:10338?srtt-ms=320&rate-mbps=500",
    ]);
    let (service_commands, _service_rx) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, service_commands), 8);
    let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(8);
    remotes.attach_for_validation(opened_test_relay_stream(stream_id, 1, candidate_commands));
    consume_client_validation_proof_for_test(&mut candidate_rx);

    let service = remotes.active_path_instance().expect("Active Service");
    let candidate = remotes
        .paths
        .iter()
        .find(|path| path.key().index == 1)
        .expect("candidate path")
        .instance();
    mark_client_validation_proof_fresh_for_test(
        &context,
        &remotes,
        candidate,
        Duration::from_millis(800),
    );
    seed_client_bulk_evidence_for_test(&context, service.key);
    let registration = context.reliable_tcp_request_bulk_flow_registration();
    registration.update(true, Some(UnderlayProtocol::Tcp));
    let service_model = RequestPerFlowRateModel {
        rate_bps: 100_000_000.0,
        delivery_samples: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
    };
    let mut request = RequestStreamState::default();
    let mut controller = RequestTcpCapacityController::default();
    request.ordered_service = Some(service);
    request
        .subflows
        .get_mut(service)
        .set_per_flow_rate(service_model);

    let full_session_geometry = request_tcp_capacity_calibration_geometry(
        context
            .reliable_path_snapshot(candidate.key)
            .expect("candidate snapshot"),
        service_model,
        context.mux_limits,
        reliable_capacity_calibration_session_limit_bytes(context.mux_limits),
    )
    .expect("candidate train fits the session envelope");
    let eligible_candidates =
        context.automatic_bulk_path_count(UnderlayProtocol::Tcp, Some(service.key.index));
    assert_eq!(eligible_candidates, 4);
    let stable_share =
        request_capacity_stable_candidate_share_bytes(context.mux_limits, eligible_candidates);
    assert!(full_session_geometry.train_bytes > stable_share);
    assert!(
        request_tcp_capacity_calibration_geometry(
            context
                .reliable_path_snapshot(candidate.key)
                .expect("candidate snapshot"),
            service_model,
            context.mux_limits,
            stable_share,
        )
        .is_none(),
        "a late path must not inherit unused shares from earlier candidates"
    );
    let session_remaining_before = context.request_tcp_capacity_probe_remaining_bytes();

    controller.try_start(
        stream_id,
        &request,
        &context,
        &remotes,
        FlowLane::Throughput,
    );

    assert!(try_recv_reliable_path_command(&mut candidate_rx).is_none());
    assert!(controller.attempted_paths.is_empty());
    assert!(controller.calibrations.is_empty());
    assert_eq!(
        context.request_tcp_capacity_probe_remaining_bytes(),
        session_remaining_before,
        "an oversized fixed-share train must not consume the session budget"
    );
}
