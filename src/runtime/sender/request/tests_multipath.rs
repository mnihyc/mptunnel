use super::super::scheduling::choose_request_ack_clock_measurement_with_rates;
use super::super::test_support::{
    client_test_context_with_paths, consume_client_path_proof_for_test,
    mark_client_path_proof_fresh_for_test, opened_test_relay_stream,
    opened_test_relay_stream_with_native_source, opened_test_relay_stream_with_underlay, security,
    seed_client_bulk_evidence_for_test,
};
use super::*;
use crate::config::ResourceLimits;
use crate::model::capacity::{
    PATH_OPEN_SCORE_BYTES, PathRateSample, RELIABLE_INITIAL_WINDOW_PACKETS,
    TcpCapacityProofCandidate, adaptive_reliable_relay_reinjection_bytes,
    reliable_bulk_carrier_feed_quantum_bytes, reliable_bulk_product_windows,
    reliable_path_startup_sample_limit_bytes, reliable_product_feedback_window_bytes,
    reliable_product_recovery_window_bytes,
};
use crate::model::path::{PathPolicy, next_carrier_path_instance_id};
use crate::model::requalification::StreamPathQualification;
use crate::model::request_capacity::request_tcp_capacity_candidate_can_start_receipt;
use crate::model::request_evidence::RequestProductRateEpoch;
use crate::model::work::ReliableWorkClass;
use crate::mux::MuxLimits;
use crate::protocol::frame::reliable_stream_frame_extent;
use crate::protocol::{PathId, PathUsage};
use crate::runtime::path::commands::{
    ReliablePathCommand, recv_reliable_path_command, reliable_path_command_channels,
    reliable_path_command_pending_bytes, try_recv_reliable_path_command,
};
use crate::runtime::path::tcp::group::ClientTcpEndpointControlState;
use crate::runtime::sender::queue::ReliableRelayQueuedWorkKind;
use crate::runtime::stream::ReliableRelayRemoteSet;
use crate::transport::PathSpec;
use bytes::Bytes;

fn data_frame(stream_id: StreamId, offset: u64, payload_bytes: usize) -> Frame {
    Frame::StreamData {
        stream_id,
        offset,
        payload: Bytes::from(vec![0x5a; payload_bytes]),
    }
}

async fn mixed_remote_set() -> (
    ClientPathContext,
    ReliableRelayRemoteSet,
    RelayPathInstance,
    RelayPathInstance,
) {
    let context =
        client_test_context_with_paths(&["tcp://127.0.0.1:10251", "quic://127.0.0.1:10252"]);
    let stream_id = StreamId(17);
    let (tcp_commands, _tcp_receivers) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, tcp_commands), 8);
    let tcp = remotes.paths[0].instance();

    let (udp_commands, _udp_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        0,
        udp_commands,
    ));
    let udp = remotes
        .paths
        .iter()
        .find(|path| path.key().underlay == UnderlayProtocol::Udp)
        .expect("UDP attachment")
        .instance();
    (context, remotes, tcp, udp)
}

#[tokio::test]
async fn bounded_ack_gap_uses_an_active_unmeasured_alternate() {
    let context =
        client_test_context_with_paths(&["tcp://127.0.0.1:10251", "quic://127.0.0.1:10252"]);
    let stream_id = StreamId(17);
    let (tcp_commands, mut tcp_receivers) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, tcp_commands), 8);
    let tcp = remotes.paths[0].instance();
    let (udp_commands, mut udp_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        0,
        udp_commands,
    ));
    let udp = remotes
        .paths
        .iter()
        .find(|path| path.key().underlay == UnderlayProtocol::Udp)
        .expect("UDP attachment")
        .instance();
    consume_client_path_proof_for_test(&mut tcp_receivers);
    consume_client_path_proof_for_test(&mut udp_receivers);
    context.install_relay_path_instance_for_test(tcp);
    context.install_relay_path_instance_for_test(udp);

    let controller = RequestMultipathController::new(stream_id);
    let frame = data_frame(stream_id, 0, 4096);

    let selected = controller
        .choose_lowest_eta_relay_path(
            &context,
            &remotes,
            &frame,
            TrafficClass::Throughput,
            RelaySendCause::AckGapReinjection,
            &[tcp],
        )
        .expect("bounded repair needs an active alternate, not a mature rate model");
    assert_eq!(remotes.paths[selected].instance(), udp);

    assert!(matches!(
        controller.choose_lowest_eta_relay_path(
            &context,
            &remotes,
            &frame,
            TrafficClass::Throughput,
            RelaySendCause::PersistentAckGapReinjection,
            &[tcp],
        ),
        Err(RequestMultipathPlanError::OutputUnavailable)
    ));

    let mut recovery_observation = observe_request_relay_scheduling(
        &context,
        stream_id,
        remotes.membership_generation(),
        &remotes.paths,
        None,
        TrafficClass::Throughput,
        PATH_OPEN_SCORE_BYTES,
        true,
        &StreamPathRequalification::default(),
    );
    recovery_observation
        .paths
        .iter_mut()
        .find(|path| path.instance == udp)
        .expect("frozen alternate observation")
        .has_bulk_model_evidence = true;
    assert!(
        !context.relay_path_instance_has_bulk_model_evidence(udp),
        "the live context must disagree with the deliberately frozen evidence batch",
    );
    let selected = controller
        .choose_lowest_eta_relay_path_for_extent(
            &context,
            &remotes,
            &frame,
            TrafficClass::Throughput,
            RelaySendCause::PersistentAckGapReinjection,
            &[tcp],
            reliable_stream_frame_accounted_bytes(&frame),
            Some(&recovery_observation),
        )
        .expect("persistent repair must use its one immutable evidence batch");
    assert_eq!(remotes.paths[selected].instance(), udp);
}

#[tokio::test]
async fn ordinary_data_does_not_borrow_a_successor_carriers_health() {
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10258?initial-srtt-s=0.005",
        "quic://127.0.0.1:10259?initial-srtt-s=0.08",
    ]);
    let stream_id = StreamId(172);
    let (predecessor_commands, _predecessor_receivers) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream(stream_id, 0, predecessor_commands),
        8,
    );
    let predecessor = remotes.paths[0].instance();
    let (alternate_commands, _alternate_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        0,
        alternate_commands,
    ));
    let alternate = remotes
        .paths
        .iter()
        .find(|path| path.key().underlay == UnderlayProtocol::Udp)
        .expect("current alternate")
        .instance();
    let successor = RelayPathInstance {
        key: predecessor.key,
        path_instance_id: next_carrier_path_instance_id(),
        attachment_id: predecessor.attachment_id.wrapping_add(1),
    };
    context.install_relay_path_instance_for_test(successor);
    context.install_relay_path_instance_for_test(alternate);

    let frame = data_frame(stream_id, 0, 4096);
    let observation = observe_request_relay_scheduling(
        &context,
        stream_id,
        remotes.membership_generation(),
        &remotes.paths,
        Some(&frame),
        TrafficClass::Latency,
        4096,
        false,
        &StreamPathRequalification::default(),
    );
    let predecessor_observation = observation
        .path_by_instance(predecessor)
        .expect("predecessor remains in attachment topology");
    assert!(predecessor_observation.shared_snapshot.is_none());
    assert!(!predecessor_observation.can_enqueue_stream_lane);
    assert!(matches!(
        choose_observed_ordinary_data_path(
            &observation,
            TrafficClass::Latency,
            4096,
            0,
            &[],
            None,
        ),
        ObservedOrdinaryPathChoice::Selected(instance) if instance == alternate
    ));
}

#[tokio::test]
async fn exact_current_unmeasured_attachment_keeps_startup_admission() {
    let context = client_test_context_with_paths(&["tcp://127.0.0.1:10260"]);
    let stream_id = StreamId(173);
    let (commands, _receivers) = reliable_path_command_channels(8);
    let remotes = ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, commands), 8);
    let current = remotes.paths[0].instance();
    context.install_relay_path_instance_for_test(current);
    let frame = data_frame(stream_id, 0, 4096);

    let observation = observe_request_relay_scheduling(
        &context,
        stream_id,
        remotes.membership_generation(),
        &remotes.paths,
        Some(&frame),
        TrafficClass::Latency,
        4096,
        false,
        &StreamPathRequalification::default(),
    );
    let current_observation = observation
        .path_by_instance(current)
        .expect("exact current attachment observation");
    assert!(current_observation.shared_snapshot.is_some());
    assert!(current_observation.can_enqueue_stream_lane);
    assert!(!current_observation.has_bulk_model_evidence);
    assert!(matches!(
        choose_observed_ordinary_data_path(
            &observation,
            TrafficClass::Latency,
            4096,
            0,
            &[],
            None,
        ),
        ObservedOrdinaryPathChoice::Selected(instance) if instance == current
    ));
}

#[tokio::test]
async fn sole_authoritative_gap_owner_keeps_its_proven_product_evidence() {
    let context = client_test_context_with_paths(&["quic://127.0.0.1:10266?initial-srtt-s=0.02"]);
    let stream_id = StreamId(177);
    let (commands, _receivers) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream_with_underlay(stream_id, UnderlayProtocol::Udp, 0, commands),
        8,
    );
    let owner = remotes.paths[0].instance();
    seed_client_bulk_evidence_for_test(&context, owner);
    let qualified_sample = PathRateSample::new(4 * 1024 * 1024, Duration::from_millis(20))
        .expect("qualified bulk sample");
    for _ in 0..RELIABLE_INITIAL_WINDOW_PACKETS {
        context.mark_relay_path_rate_sample_for_test(owner.key, qualified_sample);
    }

    // This exact owner has more flight than the unproven startup allowance,
    // but remains inside its measured Product service window. A receiver-
    // proven gap revokes only its native-only exemption; it must not erase the
    // capacity evidence needed by ordinary Product admission.
    let prior_bytes = 1024 * 1024;
    let prior = data_frame(stream_id, 0, prior_bytes);
    let next = data_frame(stream_id, prior_bytes as u64, 4096);
    context.record_relay_path_send(owner, prior_bytes);
    let mut controller = RequestMultipathController::new(stream_id);
    controller
        .request
        .path_states
        .get_mut(owner)
        .set_product_rate_epoch(RequestProductRateEpoch::for_test(
            qualified_sample.rate_bps(),
            RELIABLE_INITIAL_WINDOW_PACKETS as u32,
        ));
    controller.record_original_frame_for_test(owner, &prior);

    let plan = controller
        .plan_relay_path_send_at_frontier(
            &context,
            &mut remotes,
            &next,
            TrafficClass::Throughput,
            RelaySendCause::StreamData,
            &[],
            ReliableDataAckFrontierState::Live,
        )
        .expect("measured sole owner remains inside ordinary Product admission");
    assert_eq!(plan.target().1, owner);
}

fn fill_exact_original_product_debt(
    controller: &mut RequestMultipathController,
    instance: RelayPathInstance,
    stream_id: StreamId,
    bytes: usize,
) {
    let end = fill_exact_original_product_debt_at(controller, instance, stream_id, 0, bytes);
    assert_eq!(end, bytes as u64);
}

fn fill_exact_original_product_debt_at(
    controller: &mut RequestMultipathController,
    instance: RelayPathInstance,
    stream_id: StreamId,
    mut offset: u64,
    bytes: usize,
) -> u64 {
    let end = offset.saturating_add(bytes as u64);
    while offset < end {
        let payload_bytes = usize::try_from((end - offset).min(64 * 1024)).unwrap();
        controller.record_original_frame_for_test(
            instance,
            &data_frame(stream_id, offset, payload_bytes),
        );
        offset += payload_bytes as u64;
    }
    offset
}

fn original_data_apply_plan(
    remotes: &ReliableRelayRemoteSet,
    target: RelayPathInstance,
) -> RequestMultipathPlan {
    RequestMultipathPlan {
        target: RequestMultipathTarget {
            membership_generation: remotes.membership_generation(),
            instance: target,
        },
        product_mutation: RequestProductSendMutation::Data,
        product_limit_bytes: None,
        request_load_expectation: None,
        request_proof_expectation: None,
        native_authority_stamp: None,
        path_eligibility_expectation: SmallVec::new(),
    }
}

fn bounded_product_test_context(paths: &[&str]) -> ClientPathContext {
    let product_window = 1024 * 1024;
    let limits = ResourceLimits {
        max_stream_window_bytes: product_window as u64,
        max_repair_bytes: product_window,
        max_reorder_bytes: product_window,
        max_path_flight_bytes: product_window,
        ..ResourceLimits::default()
    };
    ClientPathContext::new(
        paths
            .iter()
            .map(|path| path.parse::<PathSpec>().expect("path"))
            .collect(),
        security(),
        limits,
    )
    .expect("bounded Product context")
}

fn bounded_mixed_remote_set(
    stream_id: StreamId,
) -> (
    ClientPathContext,
    ReliableRelayRemoteSet,
    RelayPathInstance,
    RelayPathInstance,
    crate::runtime::path::commands::ReliablePathCommandSender,
    crate::runtime::path::commands::ReliablePathCommandReceivers,
    crate::runtime::path::commands::ReliablePathCommandReceivers,
) {
    let context = bounded_product_test_context(&[
        "tcp://127.0.0.1:10370?initial-srtt-s=0.02",
        "quic://127.0.0.1:10371?initial-srtt-s=0.02",
    ]);
    let (tcp_commands, tcp_receivers) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, tcp_commands), 8);
    let tcp = remotes.paths[0].instance();
    let (udp_commands, udp_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        0,
        udp_commands.clone(),
    ));
    let udp = remotes
        .paths
        .iter()
        .find(|path| path.key().underlay == UnderlayProtocol::Udp)
        .expect("UDP attachment")
        .instance();
    context.install_relay_path_instance_for_test(tcp);
    context.install_relay_path_instance_for_test(udp);
    (
        context,
        remotes,
        tcp,
        udp,
        udp_commands,
        tcp_receivers,
        udp_receivers,
    )
}

#[tokio::test]
async fn request_product_acquisition_does_not_preempt_ordinary_completion_order() {
    let stream_id = StreamId(377);
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10375?initial-srtt-s=0.02&initial-rate-mbps=100",
        "tcp://127.0.0.1:10376?initial-srtt-s=0.5&initial-rate-mbps=1",
        "tcp://127.0.0.1:10377?initial-srtt-s=0.005&initial-rate-mbps=1000",
    ]);
    let (owner_commands, mut owner_receivers) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, owner_commands), 8);
    let owner = remotes.paths[0].instance();

    let (attachment_first_commands, mut attachment_first_receivers) =
        reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream(
        stream_id,
        1,
        attachment_first_commands.clone(),
    ));
    let attachment_first = remotes
        .paths
        .iter()
        .find(|path| path.key().index == 1)
        .expect("first additional attachment")
        .instance();

    let (attachment_second_commands, mut attachment_second_receivers) =
        reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream(
        stream_id,
        2,
        attachment_second_commands,
    ));
    let attachment_second = remotes
        .paths
        .iter()
        .find(|path| path.key().index == 2)
        .expect("second additional attachment")
        .instance();

    for receivers in [
        &mut owner_receivers,
        &mut attachment_first_receivers,
        &mut attachment_second_receivers,
    ] {
        consume_client_path_proof_for_test(receivers);
    }
    for instance in [owner, attachment_first, attachment_second] {
        context.install_relay_path_instance_for_test(instance);
    }

    let mut controller = RequestMultipathController::new(stream_id);
    for instance in [owner, attachment_first, attachment_second] {
        controller.request.path_states.get_mut(instance);
    }
    controller.record_original_frame_for_test(owner, &data_frame(stream_id, 0, 4096));
    let pending = data_frame(stream_id, 4096, 4096);
    assert!(attachment_first_commands.can_enqueue_frame_now(&pending, TrafficClass::Throughput));

    let observation = observe_request_relay_scheduling(
        &context,
        stream_id,
        remotes.membership_generation(),
        &remotes.paths,
        Some(&pending),
        TrafficClass::Throughput,
        4096,
        true,
        &controller.request.requalification,
    );
    let ordinary = choose_ordinary_bulk_relay_path_avoiding(BulkRelayFrameRequest {
        observation: &observation,
        lane: TrafficClass::Throughput,
        frame: &pending,
        cursor: 0,
        avoid_instances: &[],
        path_flights: Some(&controller.request.flights),
        request_state: Some(RequestSchedulingState {
            operation: controller.request.ack_clock_operation,
            path_states: &controller.request.path_states,
            flights: Some(&controller.request.flights),
        }),
        frontier_state: ReliableDataAckFrontierState::Live,
    });
    let BulkRelayPathChoice::Selected(ordinary_target) = ordinary else {
        panic!("ordinary completion policy must select a live Product output: {ordinary:?}");
    };
    assert_ne!(
        ordinary_target, attachment_first,
        "the fixture must make attachment order disagree with ordinary completion order",
    );

    let planned = controller
        .plan_relay_path_send(
            &context,
            &mut remotes,
            &pending,
            TrafficClass::Throughput,
            RelaySendCause::StreamData,
            &[],
        )
        .expect("ordinary Product placement remains schedulable");
    assert_eq!(
        planned.target().1,
        ordinary_target,
        "qualification may bound Product eligibility, but cannot preempt ordinary ECF plus owner hysteresis",
    );
}

#[tokio::test]
async fn request_ack_clock_annotation_cannot_retarget_the_ordinary_ecf_choice() {
    let stream_id = StreamId(379);
    let context = client_test_context_with_paths(&[
        "quic://127.0.0.1:10381?initial-srtt-s=0.02&initial-rate-mbps=100",
        "tcp://127.0.0.1:10382?initial-srtt-s=0.08&initial-rate-mbps=100",
        "tcp://127.0.0.1:10383?initial-srtt-s=0.005&initial-rate-mbps=1000",
    ]);
    let (reference_commands, mut reference_receivers) = reliable_path_command_channels(1);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream_with_underlay(
            stream_id,
            UnderlayProtocol::Udp,
            0,
            reference_commands.clone(),
        ),
        8,
    );
    let reference = remotes.paths[0].instance();

    let (cursor_first_commands, mut cursor_first_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Tcp,
        0,
        cursor_first_commands,
    ));
    let cursor_first = remotes
        .paths
        .iter()
        .find(|path| {
            path.key()
                == RelayPathKey {
                    underlay: UnderlayProtocol::Tcp,
                    index: 0,
                }
        })
        .expect("cursor-first TCP candidate")
        .instance();

    let (ordinary_commands, mut ordinary_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Tcp,
        1,
        ordinary_commands,
    ));
    let ordinary_target = remotes
        .paths
        .iter()
        .find(|path| {
            path.key()
                == RelayPathKey {
                    underlay: UnderlayProtocol::Tcp,
                    index: 1,
                }
        })
        .expect("fast ordinary TCP candidate")
        .instance();

    for receivers in [
        &mut reference_receivers,
        &mut cursor_first_receivers,
        &mut ordinary_receivers,
    ] {
        consume_client_path_proof_for_test(receivers);
    }
    for instance in [reference, cursor_first, ordinary_target] {
        seed_client_bulk_evidence_for_test(&context, instance);
    }
    remotes.retry_pending_path_proofs(&context);
    for receivers in [
        &mut reference_receivers,
        &mut cursor_first_receivers,
        &mut ordinary_receivers,
    ] {
        consume_client_path_proof_for_test(receivers);
    }
    for candidate in [cursor_first, ordinary_target] {
        mark_client_path_proof_fresh_for_test(
            &context,
            &remotes,
            candidate,
            Duration::from_millis(20),
        );
    }

    let mut controller = RequestMultipathController::new(stream_id);
    controller
        .request
        .path_states
        .get_mut(reference)
        .mark_product_path_use_proven();
    for candidate in [cursor_first, ordinary_target] {
        let state = controller.request.path_states.get_mut(candidate);
        state.mark_capacity_admitted();
        state.mark_ack_clock_first_window();
    }
    controller.record_original_frame_for_test(reference, &data_frame(stream_id, 0, 4096));
    let pending = data_frame(stream_id, 4096, 4096);

    // The reference still defines the lower Product frontier, but cannot own
    // the next native reservation. Both TCP candidates remain valid receipt
    // measurements, so the legacy acquisition ordering would choose the first
    // cursor candidate even though ordinary completion order prefers the fast
    // second candidate.
    reference_commands
        .try_enqueue_admitted_frame(
            data_frame(StreamId(1379), 0, 4096),
            TrafficClass::Throughput,
        )
        .expect("fill only the reference writer");
    let observation = observe_request_relay_scheduling(
        &context,
        stream_id,
        remotes.membership_generation(),
        &remotes.paths,
        Some(&pending),
        TrafficClass::Throughput,
        4096,
        true,
        &controller.request.requalification,
    );
    let request_state = RequestSchedulingState {
        operation: controller.request.ack_clock_operation,
        path_states: &controller.request.path_states,
        flights: Some(&controller.request.flights),
    };
    let reference_observation = observation
        .path_by_instance(reference)
        .expect("reference observation");
    assert!(reference_observation.has_bulk_model_evidence);
    assert!(!observation.latency_pressure);
    for candidate in [cursor_first, ordinary_target] {
        let candidate_observation = observation
            .path_by_instance(candidate)
            .expect("candidate observation");
        assert!(candidate_observation.has_bulk_model_evidence);
        assert!(!candidate_observation.has_fresh_native_carrier_rate_evidence);
        assert!(candidate_observation.fresh_proof.is_some());
        assert!(candidate_observation.can_enqueue_frame);
        let state = controller
            .request
            .path_states
            .get(candidate)
            .expect("candidate state");
        assert!(state.capacity_admitted());
        assert!(state.ack_clock_first_window());
        assert!(!state.ack_clock_proven());
    }
    let old_arbitration = choose_request_ack_clock_measurement_with_rates(
        &observation,
        TrafficClass::Throughput,
        4096,
        4096,
        0,
        Some(reference.key),
        Some(&controller.request.flights),
        Some(request_state),
    );
    assert!(
        matches!(
            old_arbitration,
            Some(BulkRelayPathChoice::SelectedAckClockMeasurement { candidate, .. })
                if candidate == cursor_first
        ),
        "legacy arbitration fixture selected {old_arbitration:?}"
    );
    assert_eq!(
        choose_ordinary_bulk_relay_path_avoiding(BulkRelayFrameRequest {
            observation: &observation,
            lane: TrafficClass::Throughput,
            frame: &pending,
            cursor: 0,
            avoid_instances: &[],
            path_flights: Some(&controller.request.flights),
            request_state: Some(request_state),
            frontier_state: ReliableDataAckFrontierState::Live,
        }),
        BulkRelayPathChoice::Selected(ordinary_target),
    );

    let planned = controller
        .plan_relay_path_send(
            &context,
            &mut remotes,
            &pending,
            TrafficClass::Throughput,
            RelaySendCause::StreamData,
            &[],
        )
        .expect("ordinary target can carry its own receipt annotation");
    assert_eq!(planned.target().1, ordinary_target);
    assert!(matches!(
        planned.product_mutation,
        RequestProductSendMutation::OriginalData { candidate, .. }
            if candidate == ordinary_target
    ));
}

#[tokio::test]
async fn request_ordinary_writer_failure_replans_around_a_and_commits_same_tier_b() {
    let stream_id = StreamId(378);
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10378?initial-srtt-s=0.02",
        "tcp://127.0.0.1:10379?initial-srtt-s=0.02",
        "tcp://127.0.0.1:10380?initial-srtt-s=0.02",
    ]);
    let (owner_commands, mut owner_receivers) = reliable_path_command_channels(1);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream(stream_id, 0, owner_commands.clone()),
        8,
    );
    let owner = remotes.paths[0].instance();

    let (a_commands, mut a_receivers) = reliable_path_command_channels(1);
    remotes.attach_candidate(opened_test_relay_stream(stream_id, 1, a_commands.clone()));
    let a = remotes
        .paths
        .iter()
        .find(|path| path.key().index == 1)
        .expect("same-tier candidate A")
        .instance();

    let (b_commands, mut b_receivers) = reliable_path_command_channels(1);
    remotes.attach_candidate(opened_test_relay_stream(stream_id, 2, b_commands.clone()));
    let b = remotes
        .paths
        .iter()
        .find(|path| path.key().index == 2)
        .expect("same-tier candidate B")
        .instance();

    for receivers in [&mut owner_receivers, &mut a_receivers, &mut b_receivers] {
        consume_client_path_proof_for_test(receivers);
    }
    for instance in [owner, a, b] {
        seed_client_bulk_evidence_for_test(&context, instance);
    }

    // Keep the established owner structurally Regular but transiently unable
    // to accept this quantum. Ordinary ECF must therefore choose between the
    // two independently writable additional outputs.
    owner_commands
        .try_enqueue_admitted_frame(
            data_frame(StreamId(1377), 0, 4096),
            TrafficClass::Throughput,
        )
        .expect("concurrent carrier work fills the owner's one-slot writer");

    let mut controller = RequestMultipathController::new(stream_id);
    controller.record_original_frame_for_test(owner, &data_frame(stream_id, 0, 4096));
    let pending = data_frame(stream_id, 4096, 4096);

    let plan_a = controller
        .plan_relay_path_send(
            &context,
            &mut remotes,
            &pending,
            TrafficClass::Throughput,
            RelaySendCause::StreamData,
            &[],
        )
        .expect("ordinary ECF selects the first legal additional output");
    assert_eq!(plan_a.target().1, a);

    // Another producer fills A's shared carrier writer after observation. The
    // real reservation now fails. The retry records only exact A as locally
    // rejected and runs ordinary ECF again; it owns no persistent acquisition
    // turn and cannot change the relative order of the remaining candidates.
    a_commands
        .try_enqueue_admitted_frame(
            data_frame(StreamId(1378), 0, 4096),
            TrafficClass::Throughput,
        )
        .expect("concurrent carrier work fills A's one-slot writer");
    assert!(!a_commands.can_enqueue_frame_now(&pending, TrafficClass::Throughput));
    assert!(matches!(
        a_commands.try_reserve_admitted_frame(pending.clone(), TrafficClass::Throughput),
        Err(crate::runtime::RuntimeError::SenderServiceBlocked)
    ));
    let mut rejected = SmallVec::<[RelayPathInstance; 8]>::new();
    assert!(plan_a.reject_failed_bulk_original_target(TrafficClass::Throughput, &mut rejected,));
    assert_eq!(rejected.as_slice(), &[a]);

    let plan_b = controller
        .plan_relay_path_send(
            &context,
            &mut remotes,
            &pending,
            TrafficClass::Throughput,
            RelaySendCause::StreamData,
            &rejected,
        )
        .expect("ordinary ECF remains work-conserving after exact A fails");
    assert_eq!(plan_b.target().1, b);
    let command_b = b_commands
        .try_reserve_admitted_frame(pending.clone(), TrafficClass::Throughput)
        .expect("independent B writer remains available");
    let load_claim = plan_b
        .load_expectation()
        .map(|(key, active, latency_sensitive)| {
            assert_eq!(key, b.key);
            context
                .try_reserve_relay_path_load_if_unchanged(
                    b,
                    TrafficClass::Throughput,
                    active,
                    latency_sensitive,
                )
                .expect("B's observed load authority remains current")
        });
    let authority = controller
        .bulk_original_data_apply_authority(
            &context,
            &remotes,
            &plan_b,
            &pending,
            TrafficClass::Throughput,
            ReliableDataAckFrontierState::Live,
            load_claim.is_some(),
        )
        .expect("exact B Product authority is still current");
    assert!(authority.has_headroom());

    controller
        .record_emitted_frame(&context, b, &pending, RelaySendCause::StreamData, None)
        .expect("B receipt and flight commit before carrier publication");
    let b_position = remotes
        .paths
        .iter()
        .position(|path| path.instance() == b)
        .expect("B remains attached");
    if let Some(claim) = load_claim {
        remotes.paths[b_position].load_lease = Some(claim);
    }
    controller.commit_enqueued_request_product_send(
        &context,
        &pending,
        &plan_b,
        b_position,
        remotes.paths.len(),
    );
    command_b.commit();

    assert_eq!(
        controller.request.flights.original_data_in_flight_bytes(a),
        0
    );
    assert_eq!(
        controller.request.flights.original_data_in_flight_bytes(b),
        4096,
        "only B owns the committed pending Product range",
    );
    let published = loop {
        match try_recv_reliable_path_command(&mut b_receivers) {
            Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. })) => continue,
            command => break command,
        }
    };
    match published {
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            stream_id: emitted_stream,
            offset: 4096,
            payload,
        })) => {
            assert_eq!(emitted_stream, stream_id);
            assert_eq!(payload.len(), 4096);
        }
        Some(ReliablePathCommand::SendFrame(frame)) => {
            panic!("B published a non-Product frame: {frame:?}")
        }
        Some(_) => panic!("B published a non-frame command"),
        None => panic!("B did not publish its committed Product frame"),
    }
}

#[tokio::test]
async fn request_native_authority_change_after_reservation_cannot_publish_product() {
    let stream_id = StreamId(379);
    let context = client_test_context_with_paths(&["quic://127.0.0.1:10381?initial-srtt-s=0.02"]);
    let (commands, mut receivers) = reliable_path_command_channels(8);
    let (opened, native) = opened_test_relay_stream_with_native_source(
        stream_id,
        UnderlayProtocol::Udp,
        0,
        commands.clone(),
        crate::transport::RateHint::BitsPerSecond(25_000_000),
        7,
        Some(100_000_000),
    );
    let native = native.expect("initial exact QUIC authority");
    let instance = RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        },
        path_instance_id: opened.path_instance_id(),
        attachment_id: 0,
    };
    let scope = crate::model::carrier_rate_authority::CarrierRateAuthorityScope::new(
        instance.path_instance_id,
        crate::protocol::PathMetricDirection::ClientToServer,
    );
    let mut remotes = ReliableRelayRemoteSet::new(opened, 8);
    let target = remotes.paths[0].instance();
    assert_eq!(target.path_instance_id, instance.path_instance_id);
    consume_client_path_proof_for_test(&mut receivers);
    seed_client_bulk_evidence_for_test(&context, target);

    let frame = data_frame(stream_id, 0, 4096);
    let mut sender = crate::runtime::sender::request::RequestSenderService::new(stream_id);
    let native_after_reserve = native.clone();
    sender.set_after_frame_reservation_for_test(move || {
        native_after_reserve
            .advance_transport_activation_for_test(2)
            .expect("transport switches activation after reservation");
        native_after_reserve
            .publish_observation_for_test(2, 8, Some(100_000_000))
            .expect("same-rate successor advances the central authority stamp");
        let _ = native_after_reserve
            .refresh_scheduling_shape_for_test(
                scope,
                2,
                8,
                Some(100_000_000),
                Duration::from_millis(20),
                Duration::from_millis(4),
                512 * 1024,
                0,
                1400,
                Some(100_000_000),
                false,
            )
            .expect("successor shape is ready for the immediate replan");
    });

    let outcome = sender
        .send_frame(
            &context,
            &mut remotes,
            frame.clone(),
            RelaySendCause::StreamData,
            Some(TrafficClass::Throughput),
        )
        .await
        .expect("stale Native authority replans the same live path");
    assert_eq!(outcome.path_key, target.key);

    assert_eq!(
        sender
            .multipath
            .request
            .flights
            .original_data_in_flight_bytes(target),
        4096,
        "only the successor-authorized apply publishes Product ownership",
    );
    let mut published_data = 0;
    while let Some(published) = try_recv_reliable_path_command(&mut receivers) {
        match published {
            ReliablePathCommand::SendFrame(Frame::PathProofData { .. }) => {}
            ReliablePathCommand::SendFrame(Frame::StreamData {
                stream_id: emitted_stream,
                offset: 0,
                payload,
            }) => {
                assert_eq!(emitted_stream, stream_id);
                assert_eq!(payload.len(), 4096);
                published_data += 1;
            }
            ReliablePathCommand::SendFrame(frame) => {
                panic!("request apply published an unexpected frame: {frame:?}")
            }
            _ => panic!("request apply published a non-frame command"),
        }
    }
    assert_eq!(
        published_data, 1,
        "the stale attempt must not publish a duplicate carrier command",
    );
}

#[tokio::test]
async fn request_reinjection_commit_uses_same_stamp_current_native_recovery_clock() {
    let stream_id = StreamId(381);
    let context = client_test_context_with_paths(&[
        "quic://127.0.0.1:10386?initial-srtt-s=0.02&initial-rate-mbps=100",
    ]);
    let (commands, mut receivers) = reliable_path_command_channels(8);
    let (opened, native) = opened_test_relay_stream_with_native_source(
        stream_id,
        UnderlayProtocol::Udp,
        0,
        commands,
        crate::transport::RateHint::BitsPerSecond(100_000_000),
        7,
        Some(100_000_000),
    );
    let native = native.expect("exact QUIC authority");
    let instance = RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        },
        path_instance_id: opened.path_instance_id(),
        attachment_id: 0,
    };
    let scope = crate::model::carrier_rate_authority::CarrierRateAuthorityScope::new(
        instance.path_instance_id,
        crate::protocol::PathMetricDirection::ClientToServer,
    );
    let initial_shape = native
        .refresh_scheduling_shape_for_test(
            scope,
            1,
            7,
            Some(100_000_000),
            Duration::from_millis(20),
            Duration::from_millis(4),
            512 * 1024,
            0,
            1400,
            Some(100_000_000),
            false,
        )
        .expect("initial coherent shape");
    let mut remotes = ReliableRelayRemoteSet::new(opened, 8);
    let target = remotes.paths[0].instance();
    consume_client_path_proof_for_test(&mut receivers);
    seed_client_bulk_evidence_for_test(&context, target);

    let mut sender = crate::runtime::sender::request::RequestSenderService::new(stream_id);
    let native_after_reserve = native.clone();
    sender.set_after_frame_reservation_for_test(move || {
        let current_shape = native_after_reserve
            .refresh_scheduling_shape_for_test(
                scope,
                1,
                7,
                Some(100_000_000),
                Duration::from_millis(300),
                Duration::from_millis(50),
                64 * 1024,
                60 * 1024,
                1400,
                Some(100_000_000),
                false,
            )
            .expect("same-source current coherent shape");
        assert_eq!(
            current_shape.stamp(),
            initial_shape.stamp(),
            "the race changes only activation-local shape, not Native authority",
        );
    });

    let accepted_before = Instant::now();
    let outcome = sender
        .send_frame(
            &context,
            &mut remotes,
            data_frame(stream_id, 0, 4096),
            RelaySendCause::CompletionTailReinjection(ClientReinjectionOutputIdentity {
                instance: target,
            }),
            Some(TrafficClass::Throughput),
        )
        .await
        .expect("same-stamp current Native shape remains publishable");
    let accepted_after = Instant::now();
    let committed_deadline = outcome
        .accepted_copy_deadline
        .expect("reinjection publishes one immutable suppression deadline");
    let current_interval = crate::model::timing::transport_pto_from_ms(300.0, 50.0);
    assert!(
        committed_deadline >= accepted_before + current_interval
            && committed_deadline <= accepted_after + current_interval,
        "accepted reinjection must freeze the current same-stamp Native recovery clock",
    );
}

#[tokio::test]
async fn request_native_activation_shape_gap_falls_through_to_tcp_without_blacklisting() {
    let stream_id = StreamId(380);
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10384?initial-srtt-s=0.2&initial-rate-mbps=1",
        "quic://127.0.0.1:10385?initial-srtt-s=0.005&initial-rate-mbps=500",
    ]);

    let (tcp_commands, mut tcp_receivers) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream_with_underlay(
            stream_id,
            UnderlayProtocol::Tcp,
            0,
            tcp_commands.clone(),
        ),
        8,
    );
    let tcp = remotes.paths[0].instance();

    let (udp_commands, mut udp_receivers) = reliable_path_command_channels(8);
    let (udp_opened, native) = opened_test_relay_stream_with_native_source(
        stream_id,
        UnderlayProtocol::Udp,
        0,
        udp_commands.clone(),
        crate::transport::RateHint::BitsPerSecond(500_000_000),
        17,
        Some(500_000_000),
    );
    let native = native.expect("initial fast QUIC authority");
    remotes.attach_candidate(udp_opened);
    let udp = remotes
        .paths
        .iter()
        .find(|path| path.key().underlay == UnderlayProtocol::Udp)
        .expect("QUIC candidate")
        .instance();
    consume_client_path_proof_for_test(&mut tcp_receivers);
    consume_client_path_proof_for_test(&mut udp_receivers);
    context.install_relay_path_instance_for_test(tcp);
    context.install_relay_path_instance_for_test(udp);
    context.mark_tcp_path_open_success(0, Duration::from_millis(200), TrafficClass::Throughput);
    context.mark_udp_path_open_success(0, Duration::from_millis(5));

    let frame = data_frame(stream_id, 0, 4096);
    let mut sender = crate::runtime::sender::request::RequestSenderService::new(stream_id);
    let native_after_reserve = native.clone();
    sender.set_after_frame_reservation_for_test(move || {
        native_after_reserve
            .advance_transport_activation_for_test(2)
            .expect("transport advances before successor shape publication");
        native_after_reserve
            .publish_observation_for_test(2, 18, Some(500_000_000))
            .expect("same-rate successor advances central authority");
        // Deliberately leave the coherent shape unavailable. Request
        // observation must not borrow the predecessor shape from health.
    });

    let outcome = sender
        .send_frame(
            &context,
            &mut remotes,
            frame,
            RelaySendCause::StreamData,
            Some(TrafficClass::Throughput),
        )
        .await
        .expect("the healthy TCP peer wins while QUIC shape is unpublished");
    assert_eq!(outcome.path_key, tcp.key);
    assert_eq!(
        sender
            .multipath
            .request
            .flights
            .original_data_in_flight_bytes(udp),
        0,
        "stale QUIC never gains Product ownership",
    );
    assert_eq!(
        sender
            .multipath
            .request
            .flights
            .original_data_in_flight_bytes(tcp),
        4096,
        "fallback is a fresh ordinary decision, not a path failure",
    );
    assert!(
        !matches!(
            try_recv_reliable_path_command(&mut udp_receivers),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
        ),
        "the stale QUIC reservation must remain unpublished",
    );
}

#[tokio::test]
async fn request_apply_shared_stream_window_blocks_before_each_output_product_window() {
    let stream_id = StreamId(370);
    let (context, remotes, target, sibling, target_commands, _tcp_receivers, _udp_receivers) =
        bounded_mixed_remote_set(stream_id);
    let windows = reliable_bulk_product_windows(context.mux_limits);
    assert_eq!(
        windows.stream_resource_limit_bytes, windows.per_output_product_limit_bytes,
        "the fixture isolates shared W with two half-full outputs",
    );
    let half = usize::try_from(windows.stream_resource_limit_bytes / 2).unwrap();
    let mut controller = RequestMultipathController::new(stream_id);
    let next_offset =
        fill_exact_original_product_debt_at(&mut controller, target, stream_id, 0, half);
    let next_offset =
        fill_exact_original_product_debt_at(&mut controller, sibling, stream_id, next_offset, half);
    let frame = data_frame(stream_id, next_offset, 4096);
    let plan = original_data_apply_plan(&remotes, target);

    let reservation = target_commands
        .try_reserve_admitted_frame(frame.clone(), TrafficClass::Throughput)
        .expect("the actual native writer reservation has headroom");
    let authority = controller
        .bulk_original_data_apply_authority(
            &context,
            &remotes,
            &plan,
            &frame,
            TrafficClass::Throughput,
            ReliableDataAckFrontierState::Live,
            false,
        )
        .expect("exact target remains structurally eligible");
    assert_eq!(
        authority.position,
        BulkCandidatePosition::ContiguousFrontier
    );
    assert_eq!(
        authority.stream_outstanding_bytes,
        authority.stream_limit_bytes,
    );
    assert!(authority.output_outstanding_bytes < authority.output.assignment_limit_bytes);
    assert!(
        !authority.has_headroom(),
        "O_stream == W must fail even after the native reservation while O_i < P",
    );
    drop(reservation);
}

#[tokio::test]
async fn request_apply_additional_output_uses_e_until_exact_qualification() {
    let stream_id = StreamId(371);
    let (context, remotes, owner, target, _target_commands, _tcp_receivers, _udp_receivers) =
        bounded_mixed_remote_set(stream_id);
    let mut controller = RequestMultipathController::new(stream_id);
    let owner_bytes = 4096;
    let target_entry =
        fill_exact_original_product_debt_at(&mut controller, owner, stream_id, 0, owner_bytes);
    let plan = original_data_apply_plan(&remotes, target);
    let preview = data_frame(stream_id, target_entry, 4096);
    let initial = controller
        .bulk_original_data_apply_authority(
            &context,
            &remotes,
            &plan,
            &preview,
            TrafficClass::Throughput,
            ReliableDataAckFrontierState::Live,
            false,
        )
        .expect("unqualified additional output has bounded acquisition authority");
    assert_eq!(initial.position, BulkCandidatePosition::AdditionalPath);
    assert!(initial.output.exploration_limit_bytes < initial.output.product_limit_bytes);
    assert_eq!(
        initial.output.assignment_limit_bytes,
        initial.output.exploration_limit_bytes,
    );

    let exploration = usize::try_from(initial.output.exploration_limit_bytes).unwrap();
    let next_offset = fill_exact_original_product_debt_at(
        &mut controller,
        target,
        stream_id,
        target_entry,
        exploration,
    );
    let next = data_frame(stream_id, next_offset, 4096);
    let exhausted = controller
        .bulk_original_data_apply_authority(
            &context,
            &remotes,
            &plan,
            &next,
            TrafficClass::Throughput,
            ReliableDataAckFrontierState::Live,
            false,
        )
        .expect("additional target remains structurally eligible");
    assert!(!exhausted.has_headroom(), "O_i == E must fail closed");

    seed_client_bulk_evidence_for_test(&context, target);
    let sample = PathRateSample::new(4 * 1024 * 1024, Duration::from_millis(20))
        .expect("qualified Product sample");
    for _ in 0..RELIABLE_INITIAL_WINDOW_PACKETS {
        context.mark_relay_path_rate_sample_for_test(target.key, sample);
    }
    let qualification_floor = reliable_path_startup_sample_limit_bytes(context.mux_limits);
    assert!(qualification_floor < exploration as u64);
    let qualification = controller.apply_product_ack(
        &context,
        &remotes,
        &[OffsetRange {
            start: target_entry,
            end: target_entry + qualification_floor,
        }],
        Instant::now(),
    );
    assert_eq!(qualification.as_slice(), &[target]);
    let target_state = controller.request.path_states.get_mut(target);
    assert!(target_state.product_assignment_qualified());
    target_state.mark_capacity_admitted();
    target_state.set_product_rate_epoch(RequestProductRateEpoch::for_test(
        sample.rate_bps(),
        RELIABLE_INITIAL_WINDOW_PACKETS as u32,
    ));
    let qualified = controller
        .bulk_original_data_apply_authority(
            &context,
            &remotes,
            &plan,
            &next,
            TrafficClass::Throughput,
            ReliableDataAckFrontierState::Live,
            false,
        )
        .expect("qualified additional target remains eligible");
    assert_eq!(qualified.position, BulkCandidatePosition::AdditionalPath);
    assert_eq!(
        qualified.output.assignment_limit_bytes,
        qualified.output.product_limit_bytes,
    );
    assert!(
        qualified.has_headroom(),
        "exact qualification expands E to P"
    );
}

#[tokio::test]
async fn request_apply_one_ack_and_sibling_product_evidence_do_not_qualify_additional_output() {
    let stream_id = StreamId(372);
    let (context, remotes, owner, target, _target_commands, _tcp_receivers, _udp_receivers) =
        bounded_mixed_remote_set(stream_id);
    let mut controller = RequestMultipathController::new(stream_id);
    let owner_end = fill_exact_original_product_debt_at(&mut controller, owner, stream_id, 0, 4096);
    let plan = original_data_apply_plan(&remotes, target);
    let preview = data_frame(stream_id, owner_end, 4096);
    let exploration = controller
        .bulk_original_data_apply_authority(
            &context,
            &remotes,
            &plan,
            &preview,
            TrafficClass::Throughput,
            ReliableDataAckFrontierState::Live,
            false,
        )
        .expect("bounded additional authority")
        .output
        .exploration_limit_bytes;
    let next_offset = fill_exact_original_product_debt_at(
        &mut controller,
        target,
        stream_id,
        owner_end,
        usize::try_from(exploration).unwrap(),
    );
    let one_ack = controller.apply_product_ack(
        &context,
        &remotes,
        &[OffsetRange {
            start: owner_end,
            end: owner_end + 1,
        }],
        Instant::now(),
    );
    assert_eq!(one_ack.as_slice(), &[target]);
    let target_state = controller
        .request
        .path_states
        .get(target)
        .expect("tagged target state");
    assert!(target_state.product_path_use_proven());
    assert!(!target_state.product_assignment_qualified());
    let sibling_sample = PathRateSample::new(4 * 1024 * 1024, Duration::from_millis(20))
        .expect("sibling Product sample");
    for _ in 0..RELIABLE_INITIAL_WINDOW_PACKETS {
        context.mark_relay_path_rate_sample_for_test(target.key, sibling_sample);
    }
    assert!(
        context.relay_path_instance_has_bulk_model_evidence(target),
        "the shared carrier record contains mature sibling Product evidence",
    );

    let next = data_frame(stream_id, next_offset, 4096);
    let authority = controller
        .bulk_original_data_apply_authority(
            &context,
            &remotes,
            &plan,
            &next,
            TrafficClass::Throughput,
            ReliableDataAckFrontierState::Live,
            false,
        )
        .expect("additional target remains eligible");
    assert_eq!(authority.position, BulkCandidatePosition::AdditionalPath);
    assert_eq!(authority.output.assignment_limit_bytes, exploration);
    assert!(
        !authority.has_headroom(),
        "one ACK bit and another stream's shared Product epoch cannot expand E to P",
    );
}

#[tokio::test]
async fn request_apply_recomputes_target_after_unrelated_sibling_retirement() {
    let stream_id = StreamId(373);
    let (context, mut remotes, sibling, target, target_commands, _tcp_receivers, _udp_receivers) =
        bounded_mixed_remote_set(stream_id);
    let controller = RequestMultipathController::new(stream_id);
    let frame = data_frame(stream_id, 0, 4096);
    let observation = observe_request_relay_scheduling(
        &context,
        stream_id,
        remotes.membership_generation(),
        &remotes.paths,
        Some(&frame),
        TrafficClass::Throughput,
        4096,
        true,
        &controller.request.requalification,
    );
    let plan = original_data_apply_plan(&remotes, target)
        .with_eligibility_expectation(
            &observation,
            TrafficClass::Throughput,
            Some(RequestSchedulingState {
                operation: controller.request.ack_clock_operation,
                path_states: &controller.request.path_states,
                flights: Some(&controller.request.flights),
            }),
        )
        .expect("both initial outputs are regular");
    let reservation = target_commands
        .try_reserve_admitted_frame(frame.clone(), TrafficClass::Throughput)
        .expect("target writer reservation succeeds before sibling retirement");

    assert_eq!(sibling.key.underlay, UnderlayProtocol::Tcp);
    context.set_tcp_endpoint_control(sibling.key.index, ClientTcpEndpointControlState::Failed);
    assert!(
        !plan.target_retains_exact_eligibility(&context, TrafficClass::Throughput),
        "the former whole-vector fence rejects unrelated retirement",
    );
    let prior_generation = plan.target().0;
    drop(
        remotes
            .remove_path_instance(sibling)
            .expect("retire only the unrelated sibling attachment"),
    );
    assert_ne!(remotes.membership_generation(), prior_generation);
    assert!(
        plan.target_position_for_apply(&remotes, TrafficClass::Throughput)
            .is_some(),
        "bulk apply follows exact target identity across unrelated membership retirement",
    );
    let authority = controller
        .bulk_original_data_apply_authority(
            &context,
            &remotes,
            &plan,
            &frame,
            TrafficClass::Throughput,
            ReliableDataAckFrontierState::Live,
            false,
        )
        .expect("fresh structural recheck retains the exact healthy target");
    assert_eq!(authority.position, BulkCandidatePosition::FirstPath);
    assert!(authority.has_headroom());
    drop(reservation);
}

#[tokio::test]
async fn request_bulk_apply_rejects_backup_displaced_by_fresh_regular_sibling() {
    let stream_id = StreamId(376);
    let (context, remotes, sibling, target, target_commands, _tcp_receivers, _udp_receivers) =
        bounded_mixed_remote_set(stream_id);
    for instance in [sibling, target] {
        assert!(context.update_relay_path_usage_for_test(instance, 1, PathUsage::Backup));
    }
    let controller = RequestMultipathController::new(stream_id);
    let frame = data_frame(stream_id, 0, 4096);
    let plan = original_data_apply_plan(&remotes, target);
    let reservation = target_commands
        .try_reserve_admitted_frame(frame.clone(), TrafficClass::Throughput)
        .expect("backup target writer reservation succeeds");

    assert!(context.update_relay_path_usage_for_test(sibling, 2, PathUsage::Available));
    assert!(
        controller
            .bulk_original_data_apply_authority(
                &context,
                &remotes,
                &plan,
                &frame,
                TrafficClass::Throughput,
                ReliableDataAckFrontierState::Live,
                false,
            )
            .is_none(),
        "a fresh regular tier must displace the planned backup before publication",
    );
    drop(reservation);
}

#[tokio::test]
async fn request_apply_sole_current_tier_remains_additional_behind_stale_owner() {
    let stream_id = StreamId(377);
    let (context, remotes, owner, target, target_commands, _tcp_receivers, _udp_receivers) =
        bounded_mixed_remote_set(stream_id);
    let mut controller = RequestMultipathController::new(stream_id);
    let next_offset =
        fill_exact_original_product_debt_at(&mut controller, owner, stream_id, 0, 4096);
    assert!(controller.mark_path_stale(owner));

    let frame = data_frame(stream_id, next_offset, 4096);
    let plan = original_data_apply_plan(&remotes, target);
    let reservation = target_commands
        .try_reserve_admitted_frame(frame.clone(), TrafficClass::Throughput)
        .expect("the sole current-tier writer accepts the native reservation");
    let authority = controller
        .bulk_original_data_apply_authority(
            &context,
            &remotes,
            &plan,
            &frame,
            TrafficClass::Throughput,
            ReliableDataAckFrontierState::Live,
            false,
        )
        .expect("the sole current-tier target remains structurally eligible");

    assert_eq!(authority.position, BulkCandidatePosition::AdditionalPath);
    assert_eq!(
        authority.output.assignment_limit_bytes, authority.output.exploration_limit_bytes,
        "a different exact stale owner keeps the sole current-tier target inside E",
    );
    assert!(
        authority.output.exploration_limit_bytes < authority.output.product_limit_bytes,
        "only exact qualification may expand an additional target from E to P",
    );
    drop(reservation);
}

#[tokio::test]
async fn request_apply_same_key_successor_does_not_inherit_contiguous_frontier() {
    let stream_id = StreamId(374);
    let (context, remotes, target, _sibling, _target_commands, _tcp_receivers, _udp_receivers) =
        bounded_mixed_remote_set(stream_id);
    let predecessor = RelayPathInstance {
        key: target.key,
        path_instance_id: next_carrier_path_instance_id(),
        attachment_id: target.attachment_id.wrapping_add(100),
    };
    assert_ne!(predecessor, target);
    let mut controller = RequestMultipathController::new(stream_id);
    let target_entry =
        fill_exact_original_product_debt_at(&mut controller, predecessor, stream_id, 0, 4096);
    let plan = original_data_apply_plan(&remotes, target);
    let preview = data_frame(stream_id, target_entry, 4096);
    let exploration = controller
        .bulk_original_data_apply_authority(
            &context,
            &remotes,
            &plan,
            &preview,
            TrafficClass::Throughput,
            ReliableDataAckFrontierState::Live,
            false,
        )
        .expect("same-key successor is eligible only as an additional output");
    assert_eq!(exploration.position, BulkCandidatePosition::AdditionalPath);
    let next_offset = fill_exact_original_product_debt_at(
        &mut controller,
        target,
        stream_id,
        target_entry,
        usize::try_from(exploration.output.exploration_limit_bytes).unwrap(),
    );
    let next = data_frame(stream_id, next_offset, 4096);
    let exhausted = controller
        .bulk_original_data_apply_authority(
            &context,
            &remotes,
            &plan,
            &next,
            TrafficClass::Throughput,
            ReliableDataAckFrontierState::Live,
            false,
        )
        .expect("same-key successor remains structurally eligible");
    assert_eq!(exhausted.position, BulkCandidatePosition::AdditionalPath);
    assert!(
        !exhausted.has_headroom(),
        "a reconnect cannot inherit the old incarnation's P-sized contiguous authority",
    );
}

#[tokio::test]
async fn request_apply_first_assignment_becomes_exact_contiguous_frontier() {
    let stream_id = StreamId(375);
    let (context, remotes, target, _sibling, _target_commands, _tcp_receivers, _udp_receivers) =
        bounded_mixed_remote_set(stream_id);
    let mut controller = RequestMultipathController::new(stream_id);
    let plan = original_data_apply_plan(&remotes, target);
    let first = data_frame(stream_id, 0, 4096);
    let first_authority = controller
        .bulk_original_data_apply_authority(
            &context,
            &remotes,
            &plan,
            &first,
            TrafficClass::Throughput,
            ReliableDataAckFrontierState::Live,
            false,
        )
        .expect("empty stream has one leading assignment authority");
    assert_eq!(first_authority.position, BulkCandidatePosition::FirstPath);
    assert_eq!(
        first_authority.output.assignment_limit_bytes,
        first_authority.output.product_limit_bytes,
    );
    assert!(first_authority.has_headroom());

    controller.record_original_frame_for_test(target, &first);
    let next = data_frame(stream_id, 4096, 4096);
    let contiguous = controller
        .bulk_original_data_apply_authority(
            &context,
            &remotes,
            &plan,
            &next,
            TrafficClass::Throughput,
            ReliableDataAckFrontierState::Live,
            false,
        )
        .expect("the exact first owner becomes the contiguous frontier");
    assert_eq!(
        contiguous.position,
        BulkCandidatePosition::ContiguousFrontier
    );
    assert_eq!(
        contiguous.output.assignment_limit_bytes,
        contiguous.output.product_limit_bytes,
    );
    assert!(contiguous.has_headroom());
}

#[tokio::test]
async fn active_ack_clock_original_data_rechecks_downshifted_product_window_and_ack_reopens() {
    let context = client_test_context_with_paths(&["tcp://127.0.0.1:10270?initial-srtt-s=0.1"]);
    let stream_id = StreamId(184);
    let (commands, _receivers) = reliable_path_command_channels(8);
    let remotes = ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, commands), 8);
    let owner = remotes.paths[0].instance();
    context.install_relay_path_instance_for_test(owner);
    let mut controller = RequestMultipathController::new(stream_id);
    controller.request.ack_clock_operation = Some(RequestAckClockOperation::Owner {
        candidate: owner,
        target_bytes: 16 * 1024 * 1024,
    });
    let preview = data_frame(stream_id, 0, 4096);
    let initial_observation = observe_request_relay_scheduling(
        &context,
        stream_id,
        remotes.membership_generation(),
        &remotes.paths,
        Some(&preview),
        TrafficClass::Throughput,
        4096,
        true,
        &controller.request.requalification,
    );
    let downshifted_window = request_original_data_authority_snapshot(
        &initial_observation,
        owner,
        None,
        TrafficClass::Throughput,
        Some(RequestSchedulingState {
            operation: controller.request.ack_clock_operation,
            path_states: &controller.request.path_states,
            flights: Some(&controller.request.flights),
        }),
        false,
    )
    .expect("exact request authority")
    .data_level_limit_bytes as usize;
    fill_exact_original_product_debt(&mut controller, owner, stream_id, downshifted_window);
    let observation = observe_request_relay_scheduling(
        &context,
        stream_id,
        remotes.membership_generation(),
        &remotes.paths,
        Some(&preview),
        TrafficClass::Throughput,
        4096,
        true,
        &controller.request.requalification,
    );
    let plan = RequestMultipathPlan {
        target: RequestMultipathTarget {
            membership_generation: remotes.membership_generation(),
            instance: owner,
        },
        product_mutation: RequestProductSendMutation::OriginalData {
            candidate: owner,
            target_bytes: 16 * 1024 * 1024,
            payload_bytes: 4096,
            entry_offset: downshifted_window as u64,
            foreign_optional_ranges: 0,
            foreign_optional_bytes: 0,
        },
        product_limit_bytes: None,
        request_load_expectation: None,
        request_proof_expectation: None,
        native_authority_stamp: None,
        path_eligibility_expectation: SmallVec::new(),
    }
    .with_eligibility_expectation(
        &observation,
        TrafficClass::Throughput,
        Some(RequestSchedulingState {
            operation: controller.request.ack_clock_operation,
            path_states: &controller.request.path_states,
            flights: Some(&controller.request.flights),
        }),
    )
    .expect("measurement plan captures exact Product authority");
    assert_eq!(plan.product_limit_bytes, Some(downshifted_window as u64));

    assert!(
        !controller.plan_retains_exact_product_headroom(&plan),
        "an active ACK-clock owner is still OriginalData and cannot continue when exact O == downshifted P",
    );
    controller.apply_product_ack(
        &context,
        &remotes,
        &[OffsetRange {
            start: 0,
            end: 64 * 1024,
        }],
        Instant::now(),
    );
    assert!(
        controller.plan_retains_exact_product_headroom(&plan),
        "exact DataACK debt release reopens the same immutable Product envelope",
    );
}

#[tokio::test]
async fn sole_request_product_window_exhausts_until_exact_data_ack_reopens_it() {
    let context = client_test_context_with_paths(&["quic://127.0.0.1:10267?initial-srtt-s=0.1"]);
    let stream_id = StreamId(178);
    let (commands, _receivers) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream_with_underlay(stream_id, UnderlayProtocol::Udp, 0, commands),
        8,
    );
    let owner = remotes.paths[0].instance();
    seed_client_bulk_evidence_for_test(&context, owner);
    let product_window = reliable_product_feedback_window_bytes(
        context.reliable_path_snapshot_for_instance(owner),
        TrafficClass::Throughput,
        context.mux_limits,
    );
    let mut controller = RequestMultipathController::new(stream_id);
    fill_exact_original_product_debt(&mut controller, owner, stream_id, product_window);

    let next = data_frame(stream_id, product_window as u64, 4096);
    assert!(
        matches!(
            controller.plan_relay_path_send(
                &context,
                &mut remotes,
                &next,
                TrafficClass::Throughput,
                RelaySendCause::StreamData,
                &[],
            ),
            Err(RequestMultipathPlanError::ServiceBlocked)
        ),
        "a sole path cannot bypass exact per-stream Product debt O == P"
    );

    let ack = OffsetRange::new(0, product_window as u64).expect("complete Product ACK");
    controller.apply_product_ack(&context, &remotes, &[ack], Instant::now());
    let reopened = controller
        .plan_relay_path_send(
            &context,
            &mut remotes,
            &next,
            TrafficClass::Throughput,
            RelaySendCause::StreamData,
            &[],
        )
        .expect("exact Data ACK releases Product authority");
    assert_eq!(reopened.target().1, owner);
}

#[tokio::test]
async fn latency_request_cannot_bypass_exact_product_window() {
    let context = client_test_context_with_paths(&["quic://127.0.0.1:10268?initial-srtt-s=0.1"]);
    let stream_id = StreamId(179);
    let (commands, _receivers) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream_with_underlay(stream_id, UnderlayProtocol::Udp, 0, commands),
        8,
    );
    let owner = remotes.paths[0].instance();
    seed_client_bulk_evidence_for_test(&context, owner);
    let product_window = reliable_product_feedback_window_bytes(
        context.reliable_path_snapshot_for_instance(owner),
        TrafficClass::Latency,
        context.mux_limits,
    );
    let mut controller = RequestMultipathController::new(stream_id);
    fill_exact_original_product_debt(&mut controller, owner, stream_id, product_window);

    let next = data_frame(stream_id, product_window as u64, 4096);
    assert!(
        matches!(
            controller.plan_relay_path_send(
                &context,
                &mut remotes,
                &next,
                TrafficClass::Latency,
                RelaySendCause::StreamData,
                &[],
            ),
            Err(RequestMultipathPlanError::ServiceBlocked)
        ),
        "latency priority may bypass ranking, never the Product O < P authority"
    );
}

#[tokio::test]
async fn request_product_debt_isolated_from_sibling_stream_carrier_aggregate() {
    let context = client_test_context_with_paths(&["quic://127.0.0.1:10269?initial-srtt-s=0.1"]);
    let stream_id = StreamId(180);
    let (commands, _receivers) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream_with_underlay(stream_id, UnderlayProtocol::Udp, 0, commands),
        8,
    );
    let owner = remotes.paths[0].instance();
    seed_client_bulk_evidence_for_test(&context, owner);
    let sibling_bytes = reliable_product_feedback_window_bytes(
        context.reliable_path_snapshot_for_instance(owner),
        TrafficClass::Throughput,
        context.mux_limits,
    );
    context.record_relay_path_send(owner, sibling_bytes);

    let mut controller = RequestMultipathController::new(stream_id);
    let first = data_frame(stream_id, 0, 4096);
    let plan = controller
        .plan_relay_path_send(
            &context,
            &mut remotes,
            &first,
            TrafficClass::Throughput,
            RelaySendCause::StreamData,
            &[],
        )
        .expect("this stream has O=0 even when the carrier aggregate is full");
    assert_eq!(plan.target().1, owner);
}

#[tokio::test]
async fn request_plan_revalidates_exact_health_after_same_key_replacement() {
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10261?initial-srtt-s=0.005",
        "quic://127.0.0.1:10262?initial-srtt-s=0.08",
    ]);
    let stream_id = StreamId(174);
    let (selected_commands, mut selected_receivers) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream(stream_id, 0, selected_commands.clone()),
        8,
    );
    let selected = remotes.paths[0].instance();
    let (alternate_commands, _alternate_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        0,
        alternate_commands,
    ));
    let alternate = remotes
        .paths
        .iter()
        .find(|path| path.key().underlay == UnderlayProtocol::Udp)
        .expect("alternate attachment")
        .instance();
    consume_client_path_proof_for_test(&mut selected_receivers);
    context.install_relay_path_instance_for_test(selected);
    context.install_relay_path_instance_for_test(alternate);
    let observed_generation = remotes.membership_generation();
    let frame = data_frame(stream_id, 0, 4096);
    let mut controller = RequestMultipathController::new(stream_id);
    let plan = controller
        .plan_relay_path_send(
            &context,
            &mut remotes,
            &frame,
            TrafficClass::Latency,
            RelaySendCause::StreamData,
            &[],
        )
        .expect("initial exact-current plan");
    assert_eq!(plan.target(), (observed_generation, selected));
    assert!(plan.target_retains_exact_eligibility(&context, TrafficClass::Latency));
    // Installing the first physical owner advances the health proof
    // generation. The fixture stream starts bulk, so planning must refresh
    // that proof before it can publish Product data.
    consume_client_path_proof_for_test(&mut selected_receivers);
    assert!(
        try_recv_reliable_path_command(&mut selected_receivers).is_none(),
        "the planner must emit only the required refreshed path proof",
    );

    let successor = RelayPathInstance {
        key: selected.key,
        path_instance_id: next_carrier_path_instance_id(),
        attachment_id: selected.attachment_id.wrapping_add(1),
    };
    context.install_relay_path_instance_for_test(successor);

    assert_eq!(remotes.membership_generation(), observed_generation);
    assert!(
        context
            .reliable_path_snapshot_for_instance(successor)
            .is_some()
    );
    let (_key, active_flows, active_latency_flows) =
        plan.load_expectation().expect("unowned selected path load");
    assert!(
        context
            .try_reserve_relay_path_load_if_unchanged(
                selected,
                TrafficClass::Latency,
                active_flows,
                active_latency_flows,
            )
            .is_none(),
        "a predecessor plan cannot reserve successor load"
    );
    let reserved_predecessor_command = selected_commands
        .try_reserve_admitted_frame(frame.clone(), TrafficClass::Latency)
        .expect("the predecessor queue can still reserve before exact apply validation");
    assert!(
        !plan.target_retains_exact_eligibility(&context, TrafficClass::Latency),
        "apply must reject the observed predecessor even though attachment membership did not change",
    );
    drop(reserved_predecessor_command);
    match try_recv_reliable_path_command(&mut selected_receivers) {
        None => {}
        Some(ReliablePathCommand::SendFrame(published)) if published == frame => {
            panic!("rejected apply published the planned predecessor StreamData")
        }
        Some(_) => panic!("rejected apply produced an unclassified carrier command"),
    }
    assert_eq!(
        context
            .reliable_path_snapshot_for_instance(successor)
            .expect("successor remains exact-current")
            .active_flows,
        0,
        "rejected apply rolls the logical load claim back",
    );
}

#[test]
fn request_apply_eligibility_preserves_only_schedulable_regular_backup_classes() {
    let mut snapshot = PathSnapshot::new(PathId(7), UnderlayProtocol::Tcp, 20.0, 10_000_000.0);
    snapshot.peer_usage = Some(PathUsage::Available);
    assert_eq!(
        request_path_eligibility(Some(snapshot), TrafficClass::Latency),
        RequestPathEligibility::Regular,
    );

    snapshot.state = scheduler::PathState::Suspect;
    assert_eq!(
        request_path_eligibility(Some(snapshot), TrafficClass::Latency),
        RequestPathEligibility::Regular,
        "Suspect remains an RFC-schedulable carrier",
    );
    for state in [scheduler::PathState::Failed, scheduler::PathState::Draining] {
        snapshot.state = state;
        assert_eq!(
            request_path_eligibility(Some(snapshot), TrafficClass::Latency),
            RequestPathEligibility::Unavailable,
        );
    }

    snapshot.state = scheduler::PathState::Active;
    snapshot.peer_usage = Some(PathUsage::Backup);
    assert_eq!(
        request_path_eligibility(Some(snapshot), TrafficClass::Latency),
        RequestPathEligibility::Backup,
    );
    snapshot.peer_usage = None;
    assert_eq!(
        request_path_eligibility(Some(snapshot), TrafficClass::Latency),
        RequestPathEligibility::Unavailable,
        "an attachment without authenticated directional usage is not ready",
    );

    snapshot.peer_usage = Some(PathUsage::Available);
    snapshot.policy.probe_only = true;
    assert_eq!(
        request_path_eligibility(Some(snapshot), TrafficClass::Latency),
        RequestPathEligibility::Unavailable,
        "apply uses the same immutable lane policy as selection",
    );

    let policies = [
        PathPolicy::default(),
        PathPolicy {
            backup: true,
            ..PathPolicy::default()
        },
        PathPolicy {
            expensive: true,
            ..PathPolicy::default()
        },
        PathPolicy {
            bulk_allowed: false,
            ..PathPolicy::default()
        },
        PathPolicy {
            probe_only: true,
            ..PathPolicy::default()
        },
        PathPolicy {
            no_udp: true,
            ..PathPolicy::default()
        },
    ];
    for state in [
        scheduler::PathState::Active,
        scheduler::PathState::Suspect,
        scheduler::PathState::Draining,
        scheduler::PathState::Failed,
    ] {
        for lane in [
            TrafficClass::Control,
            TrafficClass::Latency,
            TrafficClass::Throughput,
            TrafficClass::RealtimeDatagram,
        ] {
            for policy in policies {
                for peer_usage in [PathUsage::Available, PathUsage::Backup] {
                    snapshot.state = state;
                    snapshot.policy = policy;
                    snapshot.peer_usage = Some(peer_usage);
                    let schedulable = scheduler::path_is_schedulable(snapshot, lane);
                    assert_eq!(
                        scheduler::score_path(snapshot, lane, 4096).is_some(),
                        schedulable,
                        "score eligibility drifted from the scheduler predicate: state={state:?} lane={lane:?} policy={policy:?}",
                    );
                    let expected = if !schedulable {
                        RequestPathEligibility::Unavailable
                    } else if scheduler::path_is_backup(snapshot) {
                        RequestPathEligibility::Backup
                    } else {
                        RequestPathEligibility::Regular
                    };
                    assert_eq!(
                        request_path_eligibility(Some(snapshot), lane),
                        expected,
                        "apply eligibility drifted from selection: state={state:?} lane={lane:?} policy={policy:?} peer_usage={peer_usage:?}",
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn control_only_request_path_cannot_authorize_withdrawing_a_payload_owner() {
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10274",
        "quic://127.0.0.1:10275?control-only=true",
    ]);
    let stream_id = StreamId(176);
    let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, owner_commands), 8);
    let owner = remotes.paths[0].instance();
    let (alternate_commands, _alternate_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        0,
        alternate_commands,
    ));
    let alternate = remotes
        .paths
        .iter()
        .find(|path| path.key().underlay == UnderlayProtocol::Udp)
        .expect("control-only alternate")
        .instance();
    for instance in [owner, alternate] {
        context.install_relay_path_instance_for_test(instance);
    }
    let controller = RequestMultipathController::new(stream_id);

    assert!(
        !controller.has_reinjection_path(&context, &remotes, owner, TrafficClass::Throughput,),
        "a control-only attachment cannot be the payload-recovery survivor",
    );
    assert_eq!(
        controller.owner_capable_instances(&context, &remotes, TrafficClass::Throughput),
        vec![owner],
        "tail-recovery ownership must use the same payload eligibility as request dispatch",
    );
}

#[tokio::test]
async fn request_plan_revalidates_same_instance_health_transitions() {
    let context = client_test_context_with_paths(&["tcp://127.0.0.1:10263?initial-srtt-s=0.005"]);
    let stream_id = StreamId(175);
    let (commands, _receivers) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, commands), 8);
    let instance = remotes.paths[0].instance();
    context.install_relay_path_instance_for_test(instance);
    let frame = data_frame(stream_id, 0, 4096);
    let mut controller = RequestMultipathController::new(stream_id);
    let plan = controller
        .plan_relay_path_send(
            &context,
            &mut remotes,
            &frame,
            TrafficClass::Latency,
            RelaySendCause::StreamData,
            &[],
        )
        .expect("active exact-current plan");

    context.set_tcp_endpoint_control(0, ClientTcpEndpointControlState::Suspect);
    assert!(plan.target_retains_exact_eligibility(&context, TrafficClass::Latency));
    context.set_tcp_endpoint_control(0, ClientTcpEndpointControlState::Failed);
    assert!(
        context
            .reliable_path_snapshot_for_instance(instance)
            .is_some_and(|snapshot| snapshot.state == scheduler::PathState::Failed),
        "same-instance health remains present while becoming unschedulable",
    );
    assert!(!plan.target_retains_exact_eligibility(&context, TrafficClass::Latency));

    context.set_tcp_endpoint_control(0, ClientTcpEndpointControlState::Enabled);
    let replacement_plan = controller
        .plan_relay_path_send(
            &context,
            &mut remotes,
            &frame,
            TrafficClass::Latency,
            RelaySendCause::StreamData,
            &[],
        )
        .expect("Suspect remains schedulable after re-enable");
    context.set_tcp_endpoint_control(0, ClientTcpEndpointControlState::Disabled);
    assert!(
        context
            .reliable_path_snapshot_for_instance(instance)
            .is_some_and(|snapshot| snapshot.state == scheduler::PathState::Failed),
        "manual disable retains exact identity until ordered retirement",
    );
    assert!(!replacement_plan.target_retains_exact_eligibility(&context, TrafficClass::Latency));
}

#[tokio::test]
async fn request_plan_revalidates_regular_backup_set_changes() {
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10264?initial-srtt-s=0.005",
        "quic://127.0.0.1:10265?initial-srtt-s=0.08",
    ]);
    let stream_id = StreamId(176);
    let (tcp_commands, _tcp_receivers) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, tcp_commands), 8);
    let tcp = remotes.paths[0].instance();
    let (udp_commands, _udp_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        0,
        udp_commands,
    ));
    let udp = remotes
        .paths
        .iter()
        .find(|path| path.key().underlay == UnderlayProtocol::Udp)
        .expect("UDP attachment")
        .instance();
    for instance in [tcp, udp] {
        context.install_relay_path_instance_for_test(instance);
        assert!(context.update_relay_path_usage_for_test(instance, 1, PathUsage::Backup));
    }
    let frame = data_frame(stream_id, 0, 4096);
    let mut controller = RequestMultipathController::new(stream_id);
    let plan = controller
        .plan_relay_path_send(
            &context,
            &mut remotes,
            &frame,
            TrafficClass::Latency,
            RelaySendCause::StreamData,
            &[],
        )
        .expect("backup-only set remains usable");
    assert!(plan.target_retains_exact_eligibility(&context, TrafficClass::Latency));

    let alternate = if plan.target().1 == tcp { udp } else { tcp };
    assert!(context.update_relay_path_usage_for_test(alternate, 2, PathUsage::Available));
    assert!(
        !plan.target_retains_exact_eligibility(&context, TrafficClass::Latency),
        "a newly regular alternate changes the RFC eligibility set and requires recomputation",
    );
}

#[tokio::test]
async fn persistent_ack_gap_does_not_borrow_a_successor_carriers_rate_model() {
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10253?initial-srtt-s=0.04",
        "quic://127.0.0.1:10254?initial-srtt-s=0.005",
        "quic://127.0.0.1:10255?initial-srtt-s=0.08",
    ]);
    let stream_id = StreamId(170);
    let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, owner_commands), 8);
    let owner = remotes.paths[0].instance();

    let (predecessor_commands, _predecessor_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        0,
        predecessor_commands,
    ));
    let predecessor = remotes
        .paths
        .iter()
        .find(|path| path.key().underlay == UnderlayProtocol::Udp && path.key().index == 0)
        .expect("predecessor UDP attachment")
        .instance();

    let (current_commands, _current_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        1,
        current_commands,
    ));
    let current = remotes
        .paths
        .iter()
        .find(|path| path.key().underlay == UnderlayProtocol::Udp && path.key().index == 1)
        .expect("current UDP attachment")
        .instance();

    context.install_relay_path_instance_for_test(owner);
    let successor = RelayPathInstance {
        key: predecessor.key,
        path_instance_id: next_carrier_path_instance_id(),
        attachment_id: predecessor.attachment_id.wrapping_add(1),
    };
    context.install_relay_path_instance_for_test(successor);
    context.install_relay_path_instance_for_test(current);

    let successor_sample = PathRateSample::new(4 * 1024 * 1024, Duration::from_millis(20))
        .expect("fast successor sample");
    let current_sample = PathRateSample::new(4 * 1024 * 1024, Duration::from_secs(1))
        .expect("slower current attachment sample");
    for _ in 0..RELIABLE_INITIAL_WINDOW_PACKETS {
        context.mark_relay_path_rate_sample(successor, successor_sample);
        context.mark_relay_path_rate_sample(current, current_sample);
    }
    assert!(context.relay_path_instance_has_bulk_model_evidence(successor));
    assert!(context.relay_path_instance_has_bulk_model_evidence(current));
    assert!(
        !context.relay_path_instance_has_bulk_model_evidence(predecessor),
        "the predecessor attachment cannot own its same-key successor's model"
    );

    let frame = data_frame(stream_id, 0, 64 * 1024);
    let mut controller = RequestMultipathController::new(stream_id);
    controller.record_original_frame_for_test(owner, &frame);
    let observation = controller.data_ack_gap_reinjection_model(
        &context,
        &remotes,
        &frame,
        TrafficClass::Throughput,
    );

    assert_eq!(
        observation.reinjection_target.map(|(target, _)| target),
        Some(ClientReinjectionOutputIdentity { instance: current }),
        "persistent repair must use a measured current attachment, never a predecessor ranked with successor evidence",
    );
}

#[tokio::test]
async fn persistent_ack_gap_does_not_time_a_predecessor_with_successor_evidence() {
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10256?initial-srtt-s=0.005",
        "quic://127.0.0.1:10257?initial-srtt-s=0.04",
    ]);
    let stream_id = StreamId(171);
    let (predecessor_commands, _predecessor_receivers) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream(stream_id, 0, predecessor_commands),
        8,
    );
    let predecessor = remotes.paths[0].instance();

    let (alternate_commands, _alternate_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        0,
        alternate_commands,
    ));
    let alternate = remotes
        .paths
        .iter()
        .find(|path| path.key().underlay == UnderlayProtocol::Udp)
        .expect("current UDP alternate")
        .instance();

    let successor = RelayPathInstance {
        key: predecessor.key,
        path_instance_id: next_carrier_path_instance_id(),
        attachment_id: predecessor.attachment_id.wrapping_add(1),
    };
    context.install_relay_path_instance_for_test(successor);
    context.install_relay_path_instance_for_test(alternate);
    let alternate_sample = PathRateSample::new(4 * 1024 * 1024, Duration::from_millis(50))
        .expect("measured current alternate");
    for _ in 0..RELIABLE_INITIAL_WINDOW_PACKETS {
        context.mark_relay_path_rate_sample(alternate, alternate_sample);
    }
    assert!(context.reliable_path_snapshot(predecessor.key).is_some());
    assert!(
        context
            .reliable_path_snapshot_for_instance(predecessor)
            .is_none()
    );
    assert!(context.relay_path_instance_has_bulk_model_evidence(alternate));

    let frame = data_frame(stream_id, 0, 64 * 1024);
    let mut controller = RequestMultipathController::new(stream_id);
    controller.record_original_frame_for_test(predecessor, &frame);
    let observation = controller.data_ack_gap_reinjection_model(
        &context,
        &remotes,
        &frame,
        TrafficClass::Throughput,
    );

    assert!(observation.has_live_original_path);
    assert_eq!(
        observation.reinjection_target.map(|(target, _)| target),
        Some(ClientReinjectionOutputIdentity {
            instance: alternate,
        }),
        "the matching-current alternate is a valid persistent recovery target",
    );
    assert!(
        observation.original_path_timing.is_none(),
        "a predecessor attachment must not inherit its same-key successor's recovery clock",
    );
    assert_eq!(
        observation.owner_completion, None,
        "a predecessor attachment cannot contribute a current owner completion projection",
    );
}

#[tokio::test]
async fn retained_tail_uses_only_a_measured_earlier_completion() {
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10253?initial-srtt-s=0.02",
        "quic://127.0.0.1:10254?initial-srtt-s=0.02",
        "quic://127.0.0.1:10255?initial-srtt-s=0.02",
    ]);
    let stream_id = StreamId(18);
    let (tcp_commands, mut tcp_receivers) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, tcp_commands), 8);
    let tcp = remotes.paths[0].instance();
    let (udp_commands, mut udp_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        0,
        udp_commands,
    ));
    let udp = remotes
        .paths
        .iter()
        .find(|path| path.key().underlay == UnderlayProtocol::Udp)
        .expect("UDP attachment")
        .instance();
    let (unmeasured_commands, mut unmeasured_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        1,
        unmeasured_commands,
    ));
    let unmeasured = remotes
        .paths
        .iter()
        .find(|path| path.key().underlay == UnderlayProtocol::Udp && path.key().index == 1)
        .expect("unmeasured UDP attachment")
        .instance();
    consume_client_path_proof_for_test(&mut tcp_receivers);
    consume_client_path_proof_for_test(&mut udp_receivers);
    consume_client_path_proof_for_test(&mut unmeasured_receivers);

    context.install_relay_path_instance_for_test(tcp);
    context.install_relay_path_instance_for_test(udp);
    context.install_relay_path_instance_for_test(unmeasured);

    let slow_owner_sample = PathRateSample::new(4 * 1024 * 1024, Duration::from_secs(10))
        .expect("single-digit-Mbps measured owner");
    let fast_alternate_sample = PathRateSample::new(4 * 1024 * 1024, Duration::from_millis(20))
        .expect("fast measured alternate");
    for _ in 0..RELIABLE_INITIAL_WINDOW_PACKETS {
        context.mark_relay_path_rate_sample(tcp, slow_owner_sample);
        context.mark_relay_path_rate_sample(udp, fast_alternate_sample);
    }
    assert!(context.relay_path_instance_has_bulk_model_evidence(tcp));
    assert!(context.relay_path_instance_has_bulk_model_evidence(udp));
    assert!(!context.relay_path_instance_has_bulk_model_evidence(unmeasured));

    let frame = data_frame(stream_id, 0, 64 * 1024);
    let mut controller = RequestMultipathController::new(stream_id);
    controller.record_original_frame_for_test(tcp, &frame);
    context.record_relay_path_send(tcp, reliable_stream_frame_accounted_bytes(&frame));

    let original = context
        .reliable_path_snapshot_for_instance(tcp)
        .expect("exact original snapshot");
    let alternate = context
        .reliable_path_snapshot_for_instance(udp)
        .expect("exact alternate snapshot");
    let payload_bytes = reliable_stream_frame_accounted_bytes(&frame);
    let original_score = scheduler::score_path(original, TrafficClass::Throughput, payload_bytes)
        .expect("original completion score");
    let alternate_score = scheduler::score_path(alternate, TrafficClass::Throughput, payload_bytes)
        .expect("alternate completion score");
    assert!(
        alternate_score.eta_ms < original_score.eta_ms,
        "alternate ETA {} ms must beat original ETA {} ms (rates {}/{})",
        alternate_score.eta_ms,
        original_score.eta_ms,
        alternate.delivery_rate_bps,
        original.delivery_rate_bps,
    );
    assert!(!scheduler::path_within_adaptive_lead_hysteresis(
        original_score.eta_ms,
        original,
        alternate_score.eta_ms,
        alternate,
        payload_bytes,
    ));

    let target = controller
        .tail_reinjection_earlier_completion_target(
            &context,
            &remotes,
            &frame,
            TrafficClass::Throughput,
        )
        .expect("the measured alternate completes earlier");
    assert_eq!(target, ClientReinjectionOutputIdentity { instance: udp });
    let selected = controller
        .choose_lowest_eta_relay_path(
            &context,
            &remotes,
            &frame,
            TrafficClass::Throughput,
            RelaySendCause::CompletionTailReinjection(target),
            &[tcp],
        )
        .expect("the completion copy remains bound to its proven output");
    assert_eq!(
        remotes.paths[selected].instance(),
        udp,
        "an unmeasured attachment cannot replace the proven completion output"
    );
    assert!(!controller.path_is_stale(tcp));
}

#[tokio::test]
async fn ack_gap_avoidance_does_not_exclude_a_same_key_replacement() {
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10251?initial-srtt-s=0.02&initial-rate-mbps=200",
    ]);
    let stream_id = StreamId(32);
    let (old_commands, _old_receivers) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, old_commands), 8);
    let old = remotes.paths[0].instance();
    let frame = data_frame(stream_id, 0, 4096);
    let mut controller = RequestMultipathController::new(stream_id);
    controller.record_original_frame_for_test(old, &frame);

    drop(remotes.remove_path_instance(old));
    let (replacement_commands, _replacement_receivers) = reliable_path_command_channels(8);
    remotes.attach(opened_test_relay_stream(
        stream_id,
        old.key.index,
        replacement_commands,
    ));
    let replacement = remotes.paths[0].instance();
    assert_eq!(replacement.key, old.key);
    assert_ne!(replacement, old);
    context.install_relay_path_instance_for_test(replacement);
    let avoid =
        controller.reinjection_avoid_instances(&frame, RelaySendCause::AckGapReinjection, &remotes);
    assert_eq!(
        avoid,
        vec![old],
        "un-DataACKed ownership remains exact to the retired incarnation",
    );

    let selected = controller
        .choose_lowest_eta_relay_path(
            &context,
            &remotes,
            &frame,
            TrafficClass::Throughput,
            RelaySendCause::AckGapReinjection,
            &avoid,
        )
        .expect("the replacement is a distinct carrier output");
    assert_eq!(remotes.paths[selected].instance(), replacement);
}

#[tokio::test]
async fn ack_gap_repair_history_remains_avoided_until_data_ack() {
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10251?initial-srtt-s=0.02&initial-rate-mbps=200",
        "quic://127.0.0.1:10252?initial-srtt-s=0.015&initial-rate-mbps=250",
        "quic://127.0.0.1:10253?initial-srtt-s=0.01&initial-rate-mbps=300",
    ]);
    let stream_id = StreamId(33);
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, original_commands), 8);
    let original = remotes.paths[0].instance();

    let (first_commands, _first_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        0,
        first_commands,
    ));
    let first_repair = remotes
        .paths
        .iter()
        .find(|path| {
            path.key()
                == RelayPathKey {
                    underlay: UnderlayProtocol::Udp,
                    index: 0,
                }
        })
        .expect("first repair attachment")
        .instance();

    let (second_commands, _second_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        1,
        second_commands,
    ));
    let second_repair = remotes
        .paths
        .iter()
        .find(|path| {
            path.key()
                == RelayPathKey {
                    underlay: UnderlayProtocol::Udp,
                    index: 1,
                }
        })
        .expect("second repair attachment")
        .instance();
    for instance in [original, first_repair, second_repair] {
        context.install_relay_path_instance_for_test(instance);
    }

    let frame = data_frame(stream_id, 0, 4096);
    let mut controller = RequestMultipathController::new(stream_id);
    controller
        .request
        .flights
        .record_original_frame_instance(original, &frame);
    for repair in [first_repair, second_repair] {
        controller
            .request
            .flights
            .record_reinjection_frame_instance(repair, &frame);
    }
    let avoid =
        controller.reinjection_avoid_instances(&frame, RelaySendCause::AckGapReinjection, &remotes);
    assert_eq!(
        avoid,
        vec![original, first_repair, second_repair],
        "every unresolved exact copy remains ineligible for same-incarnation retry",
    );
    assert!(matches!(
        controller.choose_lowest_eta_relay_path(
            &context,
            &remotes,
            &frame,
            TrafficClass::Throughput,
            RelaySendCause::AckGapReinjection,
            &avoid,
        ),
        Err(RequestMultipathPlanError::OutputUnavailable)
    ));
    controller
        .request
        .flights
        .age_reinjected_flights_for_test(Duration::from_secs(1));
    let aged_avoid =
        controller.reinjection_avoid_instances(&frame, RelaySendCause::AckGapReinjection, &remotes);
    assert_eq!(
        aged_avoid, avoid,
        "native suppression expiry cannot release exact Product ownership",
    );
    assert!(matches!(
        controller.choose_lowest_eta_relay_path(
            &context,
            &remotes,
            &frame,
            TrafficClass::Throughput,
            RelaySendCause::AckGapReinjection,
            &aged_avoid,
        ),
        Err(RequestMultipathPlanError::OutputUnavailable)
    ));
    controller
        .request
        .flights
        .release_normalized_acked_ranges(&[OffsetRange {
            start: 0,
            end: 4096,
        }]);
    assert!(
        controller
            .reinjection_avoid_instances(&frame, RelaySendCause::AckGapReinjection, &remotes)
            .is_empty(),
        "Product DataACK is the event that releases every exact copy",
    );
}

#[tokio::test]
async fn portable_quic_repair_uses_exact_k_despite_unrelated_product_flight() {
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10251?initial-srtt-s=0.02&initial-rate-mbps=200",
        "quic://127.0.0.1:10252?initial-srtt-s=0.01&initial-rate-mbps=300",
    ]);
    let stream_id = StreamId(35);
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, original_commands), 8);
    let original = remotes.paths[0].instance();
    let (quic_commands, _quic_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        0,
        quic_commands,
    ));
    let quic = remotes
        .paths
        .iter()
        .find(|path| path.key().underlay == UnderlayProtocol::Udp)
        .expect("QUIC attachment")
        .instance();
    context.install_relay_path_instance_for_test(original);
    context.install_relay_path_instance_for_test(quic);
    let mut controller = RequestMultipathController::new(stream_id);
    let repair = data_frame(stream_id, 0, 4096);
    let quic_original = data_frame(stream_id, 4096, 4096);
    controller.record_original_frame_for_test(quic, &quic_original);

    let selected = controller
        .choose_lowest_eta_relay_path(
            &context,
            &remotes,
            &repair,
            TrafficClass::Throughput,
            RelaySendCause::AckGapReinjection,
            &[original],
        )
        .expect("unrelated Product flight consumes K but does not hard-veto repair");
    assert_eq!(remotes.paths[selected].instance(), quic);
    drop(remotes.remove_path_instance(original));
    let selected = controller
        .choose_lowest_eta_relay_path(
            &context,
            &remotes,
            &repair,
            TrafficClass::Throughput,
            RelaySendCause::PathFailureReinjection,
            &[original],
        )
        .expect("confirmed failure may reschedule onto the only live survivor");
    assert_eq!(remotes.paths[selected].instance(), quic);

    controller
        .request
        .flights
        .release_normalized_acked_ranges(&[OffsetRange {
            start: 4096,
            end: 8192,
        }]);
    let selected = controller
        .choose_lowest_eta_relay_path(
            &context,
            &remotes,
            &repair,
            TrafficClass::Throughput,
            RelaySendCause::AckGapReinjection,
            &[original],
        )
        .expect("Data ACK restores the Product bytes consumed by unrelated flight");
    assert_eq!(remotes.paths[selected].instance(), quic);
}

#[tokio::test]
async fn repair_uses_reserved_lane_while_writer_has_unrelated_dequeued_bytes() {
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10251?initial-srtt-s=0.02&initial-rate-mbps=200",
        "quic://127.0.0.1:10252?initial-srtt-s=0.01&initial-rate-mbps=300",
    ]);
    let stream_id = StreamId(36);
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, original_commands), 8);
    let original = remotes.paths[0].instance();
    let (quic_commands, mut quic_receivers) = reliable_path_command_channels(8);
    let quic_commands_for_writer = quic_commands.clone();
    remotes.attach_candidate(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        0,
        quic_commands,
    ));
    let quic = remotes
        .paths
        .iter()
        .find(|path| path.key().underlay == UnderlayProtocol::Udp)
        .expect("QUIC attachment")
        .instance();
    context.install_relay_path_instance_for_test(original);
    context.install_relay_path_instance_for_test(quic);
    consume_client_path_proof_for_test(&mut quic_receivers);
    quic_commands_for_writer
        .try_enqueue_stream_ordered_frame(
            data_frame(stream_id, 4096, 4096),
            TrafficClass::Throughput,
        )
        .expect("queue fresh carrier work");
    let dequeued = recv_reliable_path_command(&mut quic_receivers)
        .await
        .expect("writer dequeues fresh carrier work");
    let controller = RequestMultipathController::new(stream_id);
    let repair = data_frame(stream_id, 0, 4096);

    assert!(quic_commands_for_writer.can_enqueue_reinjection_frame_now(&repair));
    let selected = controller
        .choose_lowest_eta_relay_path(
            &context,
            &remotes,
            &repair,
            TrafficClass::Throughput,
            RelaySendCause::AckGapReinjection,
            &[original],
        )
        .expect("actual reinjection-lane capacity, not unrelated dequeued bytes, owns admission");
    assert_eq!(remotes.paths[selected].instance(), quic);
    quic_receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&dequeued));
    let selected = controller
        .choose_lowest_eta_relay_path(
            &context,
            &remotes,
            &repair,
            TrafficClass::Throughput,
            RelaySendCause::AckGapReinjection,
            &[original],
        )
        .expect("writer drain does not change already-valid repair eligibility");
    assert_eq!(remotes.paths[selected].instance(), quic);
}

#[tokio::test]
async fn native_backlog_ranks_repair_but_cannot_veto_exact_k() {
    let context = client_test_context_with_paths(&[
        "quic://127.0.0.1:10251?initial-srtt-s=0.02&initial-rate-mbps=200",
        "tcp://127.0.0.1:10252?initial-srtt-s=0.002&initial-rate-mbps=1000",
        "tcp://127.0.0.1:10253?initial-srtt-s=0.03&initial-rate-mbps=100",
    ]);
    let stream_id = StreamId(34);
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream_with_underlay(
            stream_id,
            UnderlayProtocol::Udp,
            0,
            original_commands,
        ),
        8,
    );
    let original = remotes.paths[0].instance();

    let (busy_commands, _busy_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream(stream_id, 0, busy_commands));
    let busy = remotes
        .paths
        .iter()
        .find(|path| {
            path.key()
                == RelayPathKey {
                    underlay: UnderlayProtocol::Tcp,
                    index: 0,
                }
        })
        .expect("busy TCP attachment")
        .instance();

    let (idle_commands, _idle_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream(stream_id, 1, idle_commands));
    let idle = remotes
        .paths
        .iter()
        .find(|path| {
            path.key()
                == RelayPathKey {
                    underlay: UnderlayProtocol::Tcp,
                    index: 1,
                }
        })
        .expect("idle TCP attachment")
        .instance();

    for instance in [original, busy, idle] {
        context.install_relay_path_instance_for_test(instance);
    }

    {
        let mut health = context.health().lock().expect("path health lock");
        health.tcp[busy.key.index].carrier_bytes_in_flight = 64 * 1024;
        health.tcp[busy.key.index].carrier_bytes_in_flight_observed = true;
        health.tcp[busy.key.index].carrier_queue_bytes = 8 * 1024;
        health.tcp[busy.key.index].carrier_queue_bytes_observed = true;
        health.tcp[busy.key.index].carrier_inflight_limit_bytes = 512 * 1024;
        health.tcp[busy.key.index].native_drain_observed = true;
    }
    let frame = data_frame(stream_id, 0, 4096);
    let controller = RequestMultipathController::new(stream_id);
    let selected = controller
        .choose_lowest_eta_relay_path(
            &context,
            &remotes,
            &frame,
            TrafficClass::Throughput,
            RelaySendCause::AckGapReinjection,
            &[original],
        )
        .expect("a K-valid carrier remains eligible despite sampled native backlog");
    assert_eq!(
        remotes.paths[selected].instance(),
        busy,
        "native backlog remains ETA evidence, but cannot make the faster K-valid carrier unavailable",
    );

    // Production attachments publish their exact carrier instance before rate
    // evidence can be admitted. Keep this measured-recovery control faithful
    // to that ownership model instead of seeding only a numeric path key.
    seed_client_bulk_evidence_for_test(&context, busy);
    assert!(context.relay_path_instance_has_bulk_model_evidence(busy));
    let busy_snapshot = context
        .reliable_path_snapshot(busy.key)
        .expect("busy TCP snapshot");
    let bound_cause = RelaySendCause::persistent_client_ack_gap_reinjection(
        ClientReinjectionOutputIdentity { instance: busy },
        busy_snapshot,
    );
    let selected = controller
        .choose_lowest_eta_relay_path(
            &context,
            &remotes,
            &frame,
            TrafficClass::Throughput,
            bound_cause,
            &[original],
        )
        .expect("the measured recovery decision admits bounded repair on a busy carrier");
    assert_eq!(remotes.paths[selected].instance(), busy);
}

#[tokio::test]
async fn capacity_reference_is_the_fastest_mature_attached_path() {
    let (context, remotes, tcp, udp) = mixed_remote_set().await;
    seed_client_bulk_evidence_for_test(&context, tcp);
    seed_client_bulk_evidence_for_test(&context, udp);
    let mut controller = RequestMultipathController::new(StreamId(17));
    controller
        .request
        .path_states
        .get_mut(tcp)
        .set_product_rate_epoch(RequestProductRateEpoch::for_test(120_000_000.0, 10));
    assert_eq!(
        context
            .reliable_path_snapshot(tcp.key)
            .map(|path| path.state),
        Some(scheduler::PathState::Active)
    );
    assert!(
        controller
            .request
            .path_states
            .get(tcp)
            .and_then(|state| state.product_rate_epoch())
            .is_some()
    );
    controller
        .request
        .path_states
        .get_mut(udp)
        .set_product_rate_epoch(RequestProductRateEpoch::for_test(240_000_000.0, 10));

    let (reference, model) = controller
        .request_capacity_reference(&context, &remotes)
        .expect("mature path reference");
    assert_eq!(reference, udp);
    assert_eq!(model.rate_bps, 240_000_000.0);

    context.mark_udp_path_failure(udp.key.index);
    assert_eq!(
        controller
            .request_capacity_reference(&context, &remotes)
            .map(|value| value.0),
        Some(tcp)
    );
}

#[tokio::test]
async fn normal_product_planning_does_not_enqueue_an_automatic_tcp_capacity_train() {
    let context = client_test_context_with_paths(&[
        "quic://127.0.0.1:10380?initial-srtt-s=0.02&initial-rate-mbps=500",
        "tcp://127.0.0.1:10381?initial-srtt-s=0.08&initial-rate-mbps=500",
    ]);
    let stream_id = StreamId(183);
    let (reference_commands, mut reference_receivers) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream_with_underlay(
            stream_id,
            UnderlayProtocol::Udp,
            0,
            reference_commands,
        ),
        8,
    );
    let (candidate_commands, mut candidate_receivers) = reliable_path_command_channels(8);
    remotes.attach(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Tcp,
        0,
        candidate_commands,
    ));
    consume_client_path_proof_for_test(&mut reference_receivers);
    consume_client_path_proof_for_test(&mut candidate_receivers);

    let reference = remotes
        .paths
        .iter()
        .find(|path| path.key().underlay == UnderlayProtocol::Udp)
        .expect("QUIC reference attachment")
        .instance();
    let candidate = remotes
        .paths
        .iter()
        .find(|path| path.key().underlay == UnderlayProtocol::Tcp)
        .expect("TCP candidate attachment")
        .instance();
    seed_client_bulk_evidence_for_test(&context, reference);
    context.install_relay_path_instance_for_test(candidate);
    context.mark_tcp_path_open_success(
        candidate.key.index,
        Duration::from_millis(20),
        TrafficClass::Throughput,
    );
    mark_client_path_proof_fresh_for_test(&context, &remotes, candidate, Duration::from_millis(20));

    let mut controller = RequestMultipathController::new(stream_id);
    controller
        .request
        .path_states
        .qualify_product_assignment_for_test(reference);
    let reference_state = controller.request.path_states.get_mut(reference);
    reference_state.mark_product_path_use_proven();
    reference_state.mark_capacity_admitted();
    reference_state.set_product_rate_epoch(RequestProductRateEpoch::for_test(
        200_000_000.0,
        RELIABLE_INITIAL_WINDOW_PACKETS as u32,
    ));
    assert_eq!(
        controller
            .request_capacity_reference(&context, &remotes)
            .map(|(instance, _)| instance),
        Some(reference),
        "the fixture must expose the mature reference that triggered the legacy automatic train",
    );
    assert!(
        !context.relay_path_instance_has_bulk_model_evidence(candidate),
        "the candidate must still require exact per-output Product evidence",
    );
    assert!(!context.reliable_relay_has_latency_pressure());
    assert!(
        context.automatic_bulk_path_count(UnderlayProtocol::Tcp, None) > 0,
        "the legacy automatic campaign must see the configured TCP candidate",
    );
    assert!(request_tcp_capacity_candidate_can_start_receipt(
        context
            .reliable_path_snapshot_for_instance(candidate)
            .expect("candidate snapshot"),
    ));
    assert!(
        remotes
            .paths
            .iter()
            .find(|path| path.instance() == candidate)
            .expect("candidate attachment")
            .stream
            .can_enqueue_work_lane_now(ReliableWorkClass::Data, TrafficClass::Throughput),
    );

    let frame = data_frame(stream_id, 0, 4096);
    let _ = controller.plan_relay_path_send(
        &context,
        &mut remotes,
        &frame,
        TrafficClass::Throughput,
        RelaySendCause::StreamData,
        &[],
    );
    consume_client_path_proof_for_test(&mut candidate_receivers);
    mark_client_path_proof_fresh_for_test(&context, &remotes, candidate, Duration::from_millis(20));

    let _ = controller.plan_relay_path_send(
        &context,
        &mut remotes,
        &frame,
        TrafficClass::Throughput,
        RelaySendCause::StreamData,
        &[],
    );

    while let Some(command) = try_recv_reliable_path_command(&mut candidate_receivers) {
        assert!(
            !matches!(command, ReliablePathCommand::SendTcpCapacityProbe(_)),
            "normal Product planning must not inject an offset-free PATH_CAPACITY train before bounded exact Product acquisition",
        );
    }
}

#[test]
fn tcp_capacity_receipt_does_not_create_request_product_evidence() {
    let target = RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        },
        path_instance_id: next_carrier_path_instance_id(),
        attachment_id: 184,
    };
    let accepted_at = Instant::now();
    let mut controller = RequestMultipathController::new(StreamId(184));

    controller.apply_request_tcp_capacity_event(RequestTcpCapacityEvent::CarrierProofAccepted {
        target,
        token: 1,
        proof: TcpCapacityProofCandidate {
            token: 1,
            train_bytes: PATH_OPEN_SCORE_BYTES as u64,
            received_bytes: PATH_OPEN_SCORE_BYTES as u64,
            rate_sample_bytes: PATH_OPEN_SCORE_BYTES as u64,
            proof_elapsed: Duration::from_millis(20),
            receipt_rate_bps: 200_000_000,
            rate_bps: 200_000_000,
            accepted_at,
            expires_at: accepted_at + Duration::from_secs(1),
        },
    });

    let state = controller
        .request
        .path_states
        .get(target)
        .expect("the legacy receipt installs a target record");
    assert!(
        !state.has_product_evidence(),
        "an offset-free carrier receipt cannot create, seed, or qualify exact per-output Product evidence",
    );
}

#[tokio::test]
async fn capacity_reference_ignores_immature_higher_rate_samples() {
    let (context, remotes, tcp, udp) = mixed_remote_set().await;
    seed_client_bulk_evidence_for_test(&context, tcp);
    seed_client_bulk_evidence_for_test(&context, udp);
    let mut controller = RequestMultipathController::new(StreamId(17));
    controller
        .request
        .path_states
        .get_mut(tcp)
        .set_product_rate_epoch(RequestProductRateEpoch::for_test(100_000_000.0, 10));
    controller
        .request
        .path_states
        .get_mut(udp)
        .set_product_rate_epoch(RequestProductRateEpoch::for_test(900_000_000.0, 1));
    assert_eq!(
        context
            .reliable_path_snapshot(tcp.key)
            .map(|path| path.state),
        Some(scheduler::PathState::Active)
    );
    assert!(
        controller
            .request
            .path_states
            .get(tcp)
            .and_then(|state| state.product_rate_epoch())
            .is_some()
    );

    assert_eq!(
        controller
            .request_capacity_reference(&context, &remotes)
            .map(|value| value.0),
        Some(tcp)
    );
}

#[tokio::test]
async fn capacity_reference_ignores_faster_expired_product_epoch() {
    let (context, remotes, tcp, udp) = mixed_remote_set().await;
    seed_client_bulk_evidence_for_test(&context, tcp);
    seed_client_bulk_evidence_for_test(&context, udp);
    let now = Instant::now();
    let mut controller = RequestMultipathController::new(StreamId(17));
    controller
        .request
        .path_states
        .get_mut(tcp)
        .set_product_rate_epoch(
            RequestProductRateEpoch::new(100_000_000.0, 10, now, Duration::from_secs(60))
                .expect("fresh TCP epoch"),
        );
    controller
        .request
        .path_states
        .get_mut(udp)
        .set_product_rate_epoch(
            RequestProductRateEpoch::new(
                900_000_000.0,
                10,
                now - Duration::from_secs(2),
                Duration::from_secs(1),
            )
            .expect("expired UDP epoch"),
        );

    assert_eq!(
        controller
            .request_capacity_reference(&context, &remotes)
            .map(|value| value.0),
        Some(tcp),
        "expired rate is retained diagnostically but cannot size a new measurement",
    );
}

#[test]
fn request_product_rate_after_expiry_starts_a_new_unblended_epoch() {
    let instance = RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        },
        path_instance_id: next_carrier_path_instance_id(),
        attachment_id: 991,
    };
    let observed_at = Instant::now();
    let expired = RequestProductRateEpoch::new(
        1_000_000.0,
        100,
        observed_at - Duration::from_secs(2),
        Duration::from_secs(1),
    )
    .expect("expired diagnostic epoch");
    let mut controller = RequestMultipathController::new(StreamId(181));
    controller
        .request
        .path_states
        .get_mut(instance)
        .set_product_rate_epoch(expired);
    let sample = PathRateSample::new(1024 * 1024, Duration::from_millis(20))
        .expect("fresh exact Product sample");
    controller.record_request_per_flow_rate_sample(
        instance,
        sample,
        false,
        observed_at,
        Duration::from_secs(1),
    );

    let epoch = controller
        .request
        .path_states
        .get(instance)
        .and_then(|state| state.product_rate_epoch())
        .expect("new Product authority epoch");
    assert_eq!(epoch.rate_bps, sample.rate_bps());
    assert_eq!(epoch.delivery_samples, 1);
    assert_eq!(epoch.observed_at, observed_at);
    assert_eq!(epoch.expires_at, observed_at + Duration::from_secs(1));
}

#[tokio::test]
async fn expired_request_product_epoch_resets_successor_boundary_only_once() {
    let (context, remotes, _tcp, udp) = mixed_remote_set().await;
    context.install_relay_path_instance_for_test(udp);
    let stream_id = StreamId(182);
    let now = Instant::now();
    let expired = RequestProductRateEpoch::new(
        1_000_000.0,
        100,
        now - Duration::from_secs(2),
        Duration::from_secs(1),
    )
    .expect("expired diagnostic epoch");
    let mut controller = RequestMultipathController::new(stream_id);
    controller
        .request
        .path_states
        .get_mut(udp)
        .set_product_rate_epoch(expired);

    let first_bytes = PATH_OPEN_SCORE_BYTES / 2;
    let second_bytes = PATH_OPEN_SCORE_BYTES - first_bytes;
    let first = data_frame(stream_id, 0, first_bytes);
    let second = data_frame(stream_id, first_bytes as u64, second_bytes);
    controller.record_original_frame_for_test(udp, &first);
    controller.record_original_frame_for_test(udp, &second);

    let first_ack_at = now + Duration::from_millis(10);
    controller.apply_product_ack(
        &context,
        &remotes,
        &[OffsetRange {
            start: 0,
            end: first_bytes as u64,
        }],
        first_ack_at,
    );
    assert!(
        controller
            .request
            .path_states
            .get(udp)
            .and_then(|state| state.product_rate_epoch())
            .is_some_and(|epoch| epoch.fresh_rate_at(first_ack_at).is_none()),
        "one sub-floor ACK retains only the expired diagnostic epoch",
    );

    let second_ack_at = now + Duration::from_millis(20);
    controller.apply_product_ack(
        &context,
        &remotes,
        &[OffsetRange {
            start: first_bytes as u64,
            end: PATH_OPEN_SCORE_BYTES as u64,
        }],
        second_ack_at,
    );
    let successor = controller
        .request
        .path_states
        .get(udp)
        .and_then(|state| state.product_rate_epoch())
        .expect("cumulative post-expiry ACKs cross the unchanged rate floor");
    assert_eq!(successor.delivery_samples, 1);
    assert_eq!(successor.observed_at, second_ack_at);
    assert!(successor.fresh_rate_at(second_ack_at).is_some());
}

#[tokio::test]
async fn duplicated_product_ack_does_not_invent_exact_path_progress() {
    let (context, remotes, tcp, udp) = mixed_remote_set().await;
    let stream_id = StreamId(17);
    let payload_bytes = 64 * 1024;
    let frame = data_frame(stream_id, 0, payload_bytes);
    let (_, end, _) = reliable_stream_frame_extent(&frame).expect("data extent");
    let range = OffsetRange::new(0, end).expect("ACK range");
    let mut controller = RequestMultipathController::new(stream_id);
    assert!(controller.mark_path_stale(tcp));
    assert_eq!(
        controller
            .request
            .flights
            .record_original_frame_instance(tcp, &frame),
        payload_bytes
    );
    assert_eq!(
        controller
            .request
            .flights
            .record_reinjection_frame_instance(udp, &frame),
        payload_bytes
    );

    let data_ack_progress_paths =
        controller.apply_product_ack(&context, &remotes, &[range], Instant::now());
    assert!(
        data_ack_progress_paths.is_empty(),
        "a Data ACK cannot identify which duplicate delivered the range"
    );
    assert!(
        controller.path_is_stale(tcp),
        "ambiguous reinjection progress must not reactivate the original path"
    );
    assert!(
        controller
            .latest_unacked_ranges_for_path_instance(tcp)
            .is_empty()
    );
    assert!(
        controller
            .latest_unacked_ranges_for_path_instance(udp)
            .is_empty()
    );
}

#[tokio::test]
async fn unique_sub_floor_product_ack_proves_use_but_not_assignment_qualification() {
    let (context, remotes, _tcp, udp) = mixed_remote_set().await;
    let stream_id = StreamId(171);
    let payload_bytes = 4096;
    let frame = data_frame(stream_id, 0, payload_bytes);
    let mut controller = RequestMultipathController::new(stream_id);
    controller.record_original_frame_for_test(udp, &frame);
    let range = OffsetRange {
        start: 0,
        end: payload_bytes as u64,
    };

    let progress = controller.apply_product_ack(&context, &remotes, &[range], Instant::now());
    assert_eq!(progress.as_slice(), &[udp]);
    let state = controller
        .request
        .path_states
        .get(udp)
        .expect("exact-path state");
    assert!(
        state.product_path_use_proven(),
        "one uniquely attributed Product ACK proves exact path use",
    );
    assert!(
        !state.product_assignment_qualified(),
        "one uniquely attributed Product ACK proves path use, but exact assignment qualification requires the full Product-volume floor",
    );
    assert!(
        !state.capacity_admitted(),
        "sub-floor progress is bounded acquisition evidence, not a rate qualification",
    );
    assert!(state.product_rate_epoch().is_none());

    let replay = controller.apply_product_ack(&context, &remotes, &[range], Instant::now());
    assert!(replay.as_slice().is_empty());
    let state = controller
        .request
        .path_states
        .get(udp)
        .expect("retained exact-path state");
    assert!(
        !state.product_assignment_qualified(),
        "a duplicate Data ACK cannot advance Product qualification",
    );
    assert!(!state.capacity_admitted());
    assert!(state.product_rate_epoch().is_none());
}

#[test]
fn rejected_request_qualification_admission_installs_no_flight() {
    let stream_id = StreamId(170);
    let owner = RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        },
        path_instance_id: next_carrier_path_instance_id(),
        attachment_id: 170,
    };

    let mut revoked = RequestMultipathController::new(stream_id);
    revoked
        .request
        .path_states
        .get_mut(owner)
        .reset_for_requalification();
    assert_eq!(
        revoked.record_original_frame_with_qualification(
            owner,
            &data_frame(stream_id, 0, 8),
            true,
            MuxLimits::default(),
        ),
        Err(ProductQualificationAdmissionError::AuthorityRevoked),
    );
    assert_eq!(
        revoked
            .request
            .flights
            .total_original_data_in_flight_bytes(),
        0
    );

    let mut oversized = RequestMultipathController::new(stream_id);
    let mut small_quantum = MuxLimits::default();
    small_quantum.max_reliable_relay_chunk_bytes = 4;
    assert_eq!(
        oversized.record_original_frame_with_qualification(
            owner,
            &data_frame(stream_id, 0, 8),
            true,
            small_quantum,
        ),
        Err(ProductQualificationAdmissionError::QuantumExceedsMaximum),
    );
    assert_eq!(
        oversized
            .request
            .flights
            .total_original_data_in_flight_bytes(),
        0,
    );

    let mut overlap = RequestMultipathController::new(stream_id);
    let frame = data_frame(stream_id, 0, 8);
    assert_eq!(
        overlap
            .record_original_frame_with_qualification(owner, &frame, true, MuxLimits::default(),),
        Ok(8),
    );
    let before = overlap
        .request
        .flights
        .total_original_data_in_flight_bytes();
    assert_eq!(
        overlap
            .record_original_frame_with_qualification(owner, &frame, true, MuxLimits::default(),),
        Err(ProductQualificationAdmissionError::OverlapsOutstandingTag),
    );
    assert_eq!(
        overlap
            .request
            .flights
            .total_original_data_in_flight_bytes(),
        before,
        "a rejected overlapping admission cannot install a second flight",
    );
}

#[tokio::test]
async fn request_final_product_quantum_tags_only_the_remaining_floor_byte() {
    let (context, remotes, tcp, _udp) = mixed_remote_set().await;
    let stream_id = StreamId(172);
    let floor = reliable_path_startup_sample_limit_bytes(MuxLimits::default());
    assert!(floor > 1);
    let prefix_bytes = usize::try_from(floor - 1).expect("test floor fits usize");
    let mut controller = RequestMultipathController::new(stream_id);
    controller.record_original_frame_for_test(tcp, &data_frame(stream_id, 0, prefix_bytes));
    controller.apply_product_ack(
        &context,
        &remotes,
        &[OffsetRange {
            start: 0,
            end: floor - 1,
        }],
        Instant::now(),
    );
    let before_final = controller
        .request
        .path_states
        .get(tcp)
        .expect("qualification generation")
        .product_qualification_invariant();
    assert_eq!(before_final.verified_bytes, floor - 1);
    assert_eq!(before_final.outstanding_tag_bytes, 0);
    assert!(
        !controller
            .request
            .path_states
            .get(tcp)
            .expect("qualification generation")
            .product_assignment_qualified()
    );

    let final_quantum_bytes = 4096;
    let final_start = floor - 1;
    let final_end = final_start + final_quantum_bytes as u64;
    controller.record_original_frame_for_test(
        tcp,
        &data_frame(stream_id, final_start, final_quantum_bytes),
    );
    let after_commit = controller
        .request
        .path_states
        .get(tcp)
        .expect("final qualification tag")
        .product_qualification_invariant();
    assert_eq!(after_commit.verified_bytes, floor - 1);
    assert_eq!(after_commit.outstanding_tag_bytes, 1);
    assert_eq!(
        controller
            .request
            .flights
            .original_data_in_flight_bytes(tcp),
        final_quantum_bytes as u64,
        "the full ordinary quantum enters O_i while only its residual prefix is tagged",
    );

    controller.apply_product_ack(
        &context,
        &remotes,
        &[OffsetRange {
            start: floor,
            end: final_end,
        }],
        Instant::now(),
    );
    let after_surplus = controller
        .request
        .path_states
        .get(tcp)
        .expect("retained one-byte qualification tag");
    assert!(!after_surplus.product_assignment_qualified());
    assert_eq!(
        after_surplus
            .product_qualification_invariant()
            .verified_bytes,
        floor - 1,
        "untagged exact Product remains eligible for rate sampling but cannot advance V",
    );

    controller.apply_product_ack(
        &context,
        &remotes,
        &[OffsetRange {
            start: final_start,
            end: floor,
        }],
        Instant::now(),
    );
    let qualified = controller
        .request
        .path_states
        .get(tcp)
        .expect("qualified exact output");
    assert!(qualified.product_assignment_qualified());
    assert_eq!(
        qualified.product_qualification_invariant().verified_bytes,
        floor
    );
}

#[tokio::test]
async fn request_qualification_survives_capacity_retirement_but_not_stale_lifecycle() {
    let (context, remotes, tcp, _udp) = mixed_remote_set().await;
    let stream_id = StreamId(173);
    let floor = reliable_path_startup_sample_limit_bytes(MuxLimits::default());
    let mut controller = RequestMultipathController::new(stream_id);
    controller.record_original_frame_for_test(
        tcp,
        &data_frame(
            stream_id,
            0,
            usize::try_from(floor).expect("test floor fits usize"),
        ),
    );
    controller.apply_product_ack(
        &context,
        &remotes,
        &[OffsetRange {
            start: 0,
            end: floor,
        }],
        Instant::now(),
    );
    {
        let state = controller.request.path_states.get_mut(tcp);
        assert!(state.product_assignment_qualified());
        state.mark_tcp_capacity_proven();
        state.mark_capacity_admitted();
    }
    assert!(!controller.revoke_request_tcp_capacity_measurement(tcp, false));
    assert!(
        controller
            .request
            .path_states
            .get(tcp)
            .expect("retained active state")
            .product_assignment_qualified(),
        "carrier capacity retirement cannot falsify exact Product-volume history",
    );

    let predecessor = data_frame(stream_id, floor, 4096);
    controller.record_original_frame_for_test(tcp, &predecessor);
    let retained_debt = controller
        .request
        .flights
        .original_data_in_flight_bytes(tcp);
    assert_eq!(retained_debt, 4096);
    assert!(controller.mark_path_stale(tcp));
    let stale = controller
        .request
        .path_states
        .get(tcp)
        .expect("stale exact state");
    assert!(!stale.product_assignment_qualified());
    assert_eq!(stale.product_qualification_invariant().floor_bytes, None);
    assert_eq!(
        controller
            .request
            .flights
            .original_data_in_flight_bytes(tcp),
        retained_debt,
        "stale entry scrubs qualification metadata without deleting O_i",
    );

    let predecessor_ack = controller.apply_product_ack(
        &context,
        &remotes,
        &[OffsetRange {
            start: floor,
            end: floor + 4096,
        }],
        Instant::now(),
    );
    assert!(predecessor_ack.is_empty());
    assert!(
        !controller
            .request
            .path_states
            .get(tcp)
            .expect("stale exact state")
            .product_assignment_qualified()
    );
}

#[tokio::test]
async fn request_detach_scrubs_qualification_without_deleting_product_debt() {
    let (context, mut remotes, owner, _survivor) = mixed_remote_set().await;
    let stream_id = StreamId(175);
    let payload_bytes = 4096;
    let frame = data_frame(stream_id, 0, payload_bytes);
    let mut controller = RequestMultipathController::new(stream_id);
    controller.record_original_frame_for_test(owner, &frame);
    assert_eq!(
        controller
            .request
            .path_states
            .get(owner)
            .expect("active qualification generation")
            .product_qualification_invariant()
            .outstanding_tag_bytes,
        payload_bytes as u64,
    );

    drop(remotes.remove_path_instance(owner));
    controller.reconcile_request_attachment_membership(&remotes);
    assert!(
        controller.request.path_states.get(owner).is_none(),
        "detach removes the exact predecessor's qualification generation",
    );
    assert_eq!(
        controller
            .request
            .flights
            .original_data_in_flight_bytes(owner),
        payload_bytes as u64,
        "detach preserves unresolved O_i for ACK release and recovery",
    );

    let predecessor_ack = controller.apply_product_ack(
        &context,
        &remotes,
        &[OffsetRange {
            start: 0,
            end: payload_bytes as u64,
        }],
        Instant::now(),
    );
    assert!(predecessor_ack.is_empty());
    assert!(
        controller.request.path_states.get(owner).is_none(),
        "a detached predecessor ACK cannot recreate qualification state",
    );
    assert_eq!(
        controller
            .request
            .flights
            .original_data_in_flight_bytes(owner),
        0,
        "the predecessor ACK still releases its retained Product debt",
    );
}

#[tokio::test]
async fn accepted_request_reinjection_removes_only_overlapping_tag_weight() {
    let (_context, _remotes, owner, repair) = mixed_remote_set().await;
    let stream_id = StreamId(174);
    let floor = reliable_path_startup_sample_limit_bytes(MuxLimits::default());
    let mut controller = RequestMultipathController::new(stream_id);
    controller.record_original_frame_for_test(
        owner,
        &data_frame(
            stream_id,
            0,
            usize::try_from(floor).expect("test floor fits usize"),
        ),
    );
    let overlap_bytes = 4096_u64.min(floor);
    controller.record_reinjected_frame_for_test(
        repair,
        &data_frame(stream_id, 0, usize::try_from(overlap_bytes).unwrap()),
    );
    let state = controller
        .request
        .path_states
        .get(owner)
        .expect("original qualification state");
    let invariant = state.product_qualification_invariant();
    assert_eq!(invariant.verified_bytes, 0);
    assert_eq!(invariant.outstanding_tag_bytes, floor - overlap_bytes);
    assert_eq!(
        state.product_qualification_deficit_bytes(),
        Some(overlap_bytes),
    );
    assert_eq!(
        controller
            .request
            .flights
            .original_data_in_flight_bytes(owner),
        floor,
        "accepted recovery changes qualification attribution, not Product ownership",
    );
}

#[tokio::test]
async fn request_load_becomes_idle_only_after_final_unique_original_ack() {
    let (context, remotes, owner, repair) = mixed_remote_set().await;
    let stream_id = StreamId(17);
    let first = data_frame(stream_id, 0, 1024);
    let second = data_frame(stream_id, 1024, 1024);
    let mut controller = RequestMultipathController::new(stream_id);
    controller
        .request
        .flights
        .record_original_frame_instance(owner, &first);
    controller
        .request
        .flights
        .record_original_frame_instance(owner, &second);
    controller
        .request
        .flights
        .record_reinjection_frame_instance(repair, &second);

    let partial = controller.apply_product_ack(
        &context,
        &remotes,
        &[OffsetRange { start: 0, end: 512 }],
        Instant::now(),
    );
    assert!(partial.idle_original_data_instances.is_empty());

    let replay = controller.apply_product_ack(
        &context,
        &remotes,
        &[OffsetRange { start: 0, end: 512 }],
        Instant::now(),
    );
    assert!(replay.idle_original_data_instances.is_empty());

    let first_complete = controller.apply_product_ack(
        &context,
        &remotes,
        &[OffsetRange {
            start: 512,
            end: 1024,
        }],
        Instant::now(),
    );
    assert!(first_complete.idle_original_data_instances.is_empty());

    let final_release = controller.apply_product_ack(
        &context,
        &remotes,
        &[OffsetRange {
            start: 1024,
            end: 2048,
        }],
        Instant::now(),
    );
    assert_eq!(
        final_release.idle_original_data_instances.as_slice(),
        &[owner],
        "only the exact OriginalData owner becomes idle; its reinjection copy never owns load",
    );
}

#[tokio::test]
async fn request_requalification_ack_on_authenticated_sibling_resolves_pending_target() {
    let context =
        client_test_context_with_paths(&["tcp://127.0.0.1:10251", "quic://127.0.0.1:10252"]);
    let stream_id = StreamId(182);
    let payload_bytes = 4096;
    let (candidate_commands, mut candidate_receivers) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream(stream_id, 0, candidate_commands),
        8,
    );
    let candidate = remotes.paths[0].instance();
    let (sibling_commands, mut sibling_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        0,
        sibling_commands,
    ));
    let sibling = remotes
        .paths
        .iter()
        .find(|path| path.key().underlay == UnderlayProtocol::Udp)
        .expect("authenticated sibling return carrier")
        .instance();
    consume_client_path_proof_for_test(&mut candidate_receivers);
    consume_client_path_proof_for_test(&mut sibling_receivers);
    context.install_relay_path_instance_for_test(candidate);
    context.install_relay_path_instance_for_test(sibling);

    let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
    let old = send_stream
        .send_data(Bytes::from(vec![0x35; payload_bytes]))
        .expect("candidate Product source");
    let sibling_source = send_stream
        .send_data(Bytes::from(vec![0x36; payload_bytes]))
        .expect("sibling Product source");
    let mut controller = RequestMultipathController::new(stream_id);
    controller.record_original_frame_for_test(candidate, &old);
    controller.record_original_frame_for_test(sibling, &sibling_source);
    assert!(controller.mark_path_stale(candidate));
    assert!(matches!(
        controller.try_enqueue_requalification_probe(
            &context,
            &remotes,
            &send_stream,
            TrafficClass::Throughput,
            payload_bytes,
        ),
        Ok(attempt) if attempt.published_payload_bytes() == Some(payload_bytes)
    ));
    let probe = match try_recv_reliable_path_command(&mut candidate_receivers) {
        Some(ReliablePathCommand::SendFrame(Frame::StreamRequalifyData {
            probe_id,
            offset,
            payload,
            ..
        })) => StreamRequalificationProbe {
            id: probe_id,
            offset,
            payload_bytes: payload.len() as u32,
        },
        _ => panic!("candidate receives the pending exact probe"),
    };

    assert!(
        controller.acknowledge_requalification_probe(sibling, probe),
        "the exact pending tuple, not its authenticated return carrier, identifies the target"
    );
    assert!(
        controller
            .request
            .requalification
            .state(candidate)
            .acquiring()
    );
    assert_eq!(
        controller.request.requalification.state(sibling),
        StreamPathQualification::Qualified,
        "the ACK carrier is not the requalification target"
    );
}

#[tokio::test]
async fn request_requalification_needs_exact_probe_then_fresh_original_ack() {
    let context =
        client_test_context_with_paths(&["tcp://127.0.0.1:10251", "quic://127.0.0.1:10252"]);
    let stream_id = StreamId(18);
    let payload_bytes = 4096;
    let (tcp_commands, mut tcp_receivers) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, tcp_commands), 8);
    let tcp = remotes.paths[0].instance();
    let (udp_commands, mut udp_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        0,
        udp_commands,
    ));
    let udp = remotes
        .paths
        .iter()
        .find(|path| path.key().underlay == UnderlayProtocol::Udp)
        .expect("healthy UDP attachment")
        .instance();
    consume_client_path_proof_for_test(&mut tcp_receivers);
    consume_client_path_proof_for_test(&mut udp_receivers);
    context.install_relay_path_instance_for_test(tcp);
    context.install_relay_path_instance_for_test(udp);

    let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
    let old = send_stream
        .send_data(Bytes::from(vec![0x31; payload_bytes]))
        .expect("pre-stale candidate data");
    let healthy = send_stream
        .send_data(Bytes::from(vec![0x32; payload_bytes]))
        .expect("healthy retained probe source");
    let fresh = send_stream
        .send_data(Bytes::from(vec![0x33; payload_bytes]))
        .expect("post-probe candidate data");
    let mut controller = RequestMultipathController::new(stream_id);
    controller.record_original_frame_for_test(tcp, &old);
    controller.record_original_frame_for_test(udp, &healthy);
    assert!(controller.mark_path_stale(tcp));
    assert!(matches!(
        controller.try_enqueue_requalification_probe(
            &context,
            &remotes,
            &send_stream,
            TrafficClass::Throughput,
            payload_bytes,
        ),
        Ok(attempt) if attempt.published_payload_bytes() == Some(payload_bytes)
    ));
    let probe = match try_recv_reliable_path_command(&mut tcp_receivers) {
        Some(ReliablePathCommand::SendFrame(Frame::StreamRequalifyData {
            probe_id,
            offset,
            payload,
            ..
        })) => StreamRequalificationProbe {
            id: probe_id,
            offset,
            payload_bytes: payload.len() as u32,
        },
        _ => panic!("stale exact attachment receives the requalification probe"),
    };
    assert_eq!(probe.offset, 0);
    assert_eq!(
        controller.latest_unacked_ranges_for_path_instance(tcp),
        vec![OffsetRange {
            start: 0,
            end: payload_bytes as u64,
        }],
        "a same-owner probe never becomes an alternate DSN owner"
    );
    assert_eq!(
        controller.latest_unacked_ranges_for_path_instance(udp),
        vec![OffsetRange {
            start: payload_bytes as u64,
            end: (payload_bytes * 2) as u64,
        }]
    );

    assert!(!controller.acknowledge_requalification_probe(
        udp,
        StreamRequalificationProbe {
            id: probe.id + 1,
            ..probe
        },
    ));
    assert!(controller.path_is_stale(tcp));
    assert!(controller.acknowledge_requalification_probe(udp, probe));
    assert!(controller.request.requalification.state(tcp).acquiring());
    assert_eq!(
        controller.request.requalification.state(udp),
        StreamPathQualification::Qualified,
        "the authenticated ACK carrier is not the forward probe target"
    );

    let old_progress = controller.apply_product_ack(
        &context,
        &remotes,
        &[OffsetRange {
            start: 0,
            end: payload_bytes as u64,
        }],
        Instant::now(),
    );
    assert!(old_progress.is_empty());
    assert!(controller.request.requalification.state(tcp).acquiring());
    assert!(
        !controller
            .request
            .path_states
            .get(tcp)
            .is_some_and(|state| state.has_product_evidence()),
        "pre-stale ACK releases credit but cannot rebuild acquisition authority"
    );
    assert!(!controller.acknowledge_requalification_probe(tcp, probe));

    controller.record_original_frame_for_test(tcp, &fresh);
    let fresh_progress = controller.apply_product_ack(
        &context,
        &remotes,
        &[OffsetRange {
            start: (payload_bytes * 2) as u64,
            end: (payload_bytes * 3) as u64,
        }],
        Instant::now(),
    );
    assert_eq!(fresh_progress.as_slice(), &[tcp]);
    assert!(
        !controller.path_is_stale(tcp),
        "only post-probe uniquely owned OriginalData qualifies the attachment"
    );
    assert_eq!(
        controller.request.requalification.state(tcp),
        StreamPathQualification::Qualified
    );
}

#[tokio::test]
async fn quic_only_stale_fallback_can_requalify_without_a_qualified_source() {
    let context = client_test_context_with_paths(&["quic://127.0.0.1:10252"]);
    let stream_id = StreamId(181);
    let payload_bytes = 4096;
    let (commands, mut receivers) = reliable_path_command_channels(8);
    let remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream_with_underlay(stream_id, UnderlayProtocol::Udp, 0, commands),
        8,
    );
    let quic = remotes.paths[0].instance();
    consume_client_path_proof_for_test(&mut receivers);
    context.install_relay_path_instance_for_test(quic);

    let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
    let fallback = send_stream
        .send_data(Bytes::from(vec![0x51; payload_bytes]))
        .expect("sole-stale fallback Product data");
    let mut controller = RequestMultipathController::new(stream_id);
    assert!(controller.mark_path_stale(quic));
    controller.record_original_frame_for_test(quic, &fallback);
    assert!(controller.path_is_stale(quic));

    assert!(matches!(
        controller.try_enqueue_requalification_probe(
            &context,
            &remotes,
            &send_stream,
            TrafficClass::Throughput,
            payload_bytes,
        ),
        Ok(attempt) if attempt.published_payload_bytes() == Some(payload_bytes)
    ));
    let probe = match try_recv_reliable_path_command(&mut receivers) {
        Some(ReliablePathCommand::SendFrame(Frame::StreamRequalifyData {
            probe_id,
            offset,
            payload,
            ..
        })) => StreamRequalificationProbe {
            id: probe_id,
            offset,
            payload_bytes: payload.len() as u32,
        },
        _ => panic!("sole stale QUIC attachment receives the exact probe"),
    };
    assert_eq!(probe.offset, 0);
    assert_eq!(
        controller.latest_unacked_ranges_for_path_instance(quic),
        vec![OffsetRange {
            start: 0,
            end: payload_bytes as u64,
        }],
        "the probe adds no alternate DSN owner"
    );
    assert!(controller.acknowledge_requalification_probe(quic, probe));
    assert!(controller.request.requalification.state(quic).acquiring());

    let fresh = send_stream
        .send_data(Bytes::from(vec![0x52; payload_bytes]))
        .expect("post-probe fresh Product data");
    controller.record_original_frame_for_test(quic, &fresh);
    assert_eq!(
        controller
            .apply_product_ack(
                &context,
                &remotes,
                &[OffsetRange {
                    start: payload_bytes as u64,
                    end: (payload_bytes * 2) as u64,
                }],
                Instant::now(),
            )
            .as_slice(),
        &[quic]
    );
    assert_eq!(
        controller.request.requalification.state(quic),
        StreamPathQualification::Qualified
    );
}

#[tokio::test]
async fn requalification_skips_draining_and_full_stale_attachments() {
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10251",
        "tcp://127.0.0.1:10252",
        "tcp://127.0.0.1:10253",
    ]);
    let stream_id = StreamId(182);
    let (draining_commands, mut draining_receivers) = reliable_path_command_channels(1);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream(stream_id, 0, draining_commands.clone()),
        8,
    );
    let draining = remotes.paths[0].instance();
    let (full_commands, mut full_receivers) = reliable_path_command_channels(1);
    remotes.attach_candidate(opened_test_relay_stream(
        stream_id,
        1,
        full_commands.clone(),
    ));
    let full = remotes
        .paths
        .iter()
        .find(|path| path.key().index == 1)
        .expect("full attachment")
        .instance();
    let (ready_commands, mut ready_receivers) = reliable_path_command_channels(1);
    remotes.attach_candidate(opened_test_relay_stream(stream_id, 2, ready_commands));
    let ready = remotes
        .paths
        .iter()
        .find(|path| path.key().index == 2)
        .expect("ready attachment")
        .instance();
    consume_client_path_proof_for_test(&mut draining_receivers);
    consume_client_path_proof_for_test(&mut full_receivers);
    consume_client_path_proof_for_test(&mut ready_receivers);
    draining_commands.begin_path_drain();
    full_commands
        .try_enqueue_reinjection_frame(data_frame(StreamId(999), 0, 4096), TrafficClass::Throughput)
        .expect("fill first active candidate reinjection queue");

    let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
    let source = send_stream
        .send_data(Bytes::from(vec![0x71; 4096]))
        .expect("retained fallback source");
    let mut controller = RequestMultipathController::new(stream_id);
    controller.record_original_frame_for_test(ready, &source);
    assert!(controller.mark_path_stale(draining));
    assert!(controller.mark_path_stale(full));
    assert!(controller.mark_path_stale(ready));

    assert!(matches!(
        controller.try_enqueue_requalification_probe(
            &context,
            &remotes,
            &send_stream,
            TrafficClass::Throughput,
            4096,
        ),
        Ok(attempt) if attempt.published_payload_bytes() == Some(4096)
    ));
    assert!(try_recv_reliable_path_command(&mut draining_receivers).is_none());
    assert!(matches!(
        try_recv_reliable_path_command(&mut full_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            stream_id: StreamId(999),
            ..
        }))
    ));
    assert!(matches!(
        try_recv_reliable_path_command(&mut ready_receivers),
        Some(ReliablePathCommand::SendFrame(
            Frame::StreamRequalifyData { .. }
        ))
    ));
}

#[tokio::test]
async fn all_full_stale_requalification_returns_bounded_backpressure() {
    let context =
        client_test_context_with_paths(&["tcp://127.0.0.1:10251", "tcp://127.0.0.1:10252"]);
    let stream_id = StreamId(183);
    let (first_commands, mut first_receivers) = reliable_path_command_channels(1);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream(stream_id, 0, first_commands.clone()),
        8,
    );
    let first = remotes.paths[0].instance();
    let (second_commands, mut second_receivers) = reliable_path_command_channels(1);
    remotes.attach_candidate(opened_test_relay_stream(
        stream_id,
        1,
        second_commands.clone(),
    ));
    let second = remotes
        .paths
        .iter()
        .find(|path| path.key().index == 1)
        .expect("second attachment")
        .instance();
    consume_client_path_proof_for_test(&mut first_receivers);
    consume_client_path_proof_for_test(&mut second_receivers);
    for (commands, filler_stream) in [(&first_commands, 991), (&second_commands, 992)] {
        commands
            .try_enqueue_reinjection_frame(
                data_frame(StreamId(filler_stream), 0, 4096),
                TrafficClass::Throughput,
            )
            .expect("fill stale reinjection queue");
    }
    let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
    let source = send_stream
        .send_data(Bytes::from(vec![0x72; 4096]))
        .expect("retained fallback source");
    let mut controller = RequestMultipathController::new(stream_id);
    controller.record_original_frame_for_test(first, &source);
    assert!(controller.mark_path_stale(first));
    assert!(controller.mark_path_stale(second));

    assert!(matches!(
        controller.try_enqueue_requalification_probe(
            &context,
            &remotes,
            &send_stream,
            TrafficClass::Throughput,
            4096,
        ),
        Ok(attempt) if attempt.is_capacity_blocked()
    ));
    assert!(controller.requalification_deadline().is_none());
    assert!(controller.path_is_stale(first));
    assert!(controller.path_is_stale(second));
}

#[tokio::test]
async fn stale_path_is_not_selected_for_new_request_data() {
    let context =
        client_test_context_with_paths(&["tcp://127.0.0.1:10251", "quic://127.0.0.1:10252"]);
    let stream_id = StreamId(19);
    let (tcp_commands, _tcp_receivers) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, tcp_commands), 8);
    let tcp = remotes.paths[0].instance();
    let (udp_commands, mut udp_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        0,
        udp_commands.clone(),
    ));
    super::super::test_support::consume_client_path_proof_for_test(&mut udp_receivers);
    let udp = remotes
        .paths
        .iter()
        .find(|path| path.key().underlay == UnderlayProtocol::Udp)
        .expect("UDP attachment")
        .instance();
    seed_client_bulk_evidence_for_test(&context, tcp);
    seed_client_bulk_evidence_for_test(&context, udp);
    let frame = data_frame(stream_id, 0, 4096);
    assert!(udp_commands.can_enqueue_frame_now(&frame, TrafficClass::Throughput));
    let mut controller = RequestMultipathController::new(stream_id);
    assert!(controller.mark_path_stale(tcp));

    let observation = observe_request_relay_scheduling(
        &context,
        stream_id,
        remotes.membership_generation(),
        &remotes.paths,
        Some(&frame),
        TrafficClass::Latency,
        4096,
        false,
        &controller.request.requalification,
    );
    assert!(
        !observation
            .path_by_instance(tcp)
            .expect("stale path observation")
            .can_enqueue_stream_lane
    );
    assert!(
        observation
            .path_by_instance(udp)
            .expect("alternate path observation")
            .can_enqueue_stream_lane
    );
    assert!(matches!(
        choose_observed_ordinary_data_path(
            &observation,
            TrafficClass::Latency,
            4096,
            0,
            &[],
            None,
        ),
        ObservedOrdinaryPathChoice::Selected(instance) if instance == udp
    ));

    let plan = controller
        .plan_relay_path_send(
            &context,
            &mut remotes,
            &frame,
            TrafficClass::Latency,
            RelaySendCause::StreamData,
            &[],
        )
        .expect("the progressing path remains available");

    assert_eq!(plan.target().1, udp);

    let fin = Frame::StreamFin {
        stream_id,
        final_offset: 4096,
    };
    let fin_plan = controller
        .plan_relay_path_send(
            &context,
            &mut remotes,
            &fin,
            TrafficClass::Latency,
            RelaySendCause::StreamFin,
            &[],
        )
        .expect("connection FIN remains schedulable on the progressing path");
    assert_eq!(fin_plan.target().1, udp);

    udp_commands.begin_path_drain();
    let fallback_observation = observe_request_relay_scheduling(
        &context,
        stream_id,
        remotes.membership_generation(),
        &remotes.paths,
        Some(&frame),
        TrafficClass::Latency,
        4096,
        false,
        &controller.request.requalification,
    );
    assert!(
        fallback_observation
            .path_by_instance(tcp)
            .expect("stale active fallback")
            .can_enqueue_stream_lane
    );
    assert!(
        !fallback_observation
            .path_by_instance(udp)
            .expect("draining alternate")
            .can_enqueue_stream_lane
    );
    let fallback = controller
        .plan_relay_path_send(
            &context,
            &mut remotes,
            &frame,
            TrafficClass::Latency,
            RelaySendCause::StreamData,
            &[],
        )
        .expect("a Product-inactive drain restores the stale active fallback");
    assert_eq!(fallback.target().1, tcp);
    assert!(
        controller.path_is_stale(tcp),
        "sole-survivor scheduling must not erase stale evidence"
    );
}

#[tokio::test]
async fn current_recovery_copy_does_not_clock_a_disjoint_stale_range() {
    let context =
        client_test_context_with_paths(&["tcp://127.0.0.1:10251", "quic://127.0.0.1:10252"]);
    let stream_id = StreamId(20);
    let (tcp_commands, _tcp_receivers) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, tcp_commands), 8);
    let tcp = remotes.paths[0].instance();
    let (udp_commands, _udp_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        0,
        udp_commands,
    ));
    let udp = remotes
        .paths
        .iter()
        .find(|path| path.key().underlay == UnderlayProtocol::Udp)
        .expect("UDP attachment")
        .instance();
    for instance in [tcp, udp] {
        context.install_relay_path_instance_for_test(instance);
    }
    let first = data_frame(stream_id, 0, 4096);
    let second = data_frame(stream_id, 4096, 4096);
    let mut controller = RequestMultipathController::new(stream_id);
    controller
        .request
        .flights
        .record_original_frame_instance(tcp, &first);
    controller
        .request
        .flights
        .record_original_frame_instance(tcp, &second);
    controller
        .request
        .flights
        .record_reinjection_frame_instance(udp, &first);
    assert!(controller.mark_path_stale(tcp));
    assert_eq!(
        controller
            .path_recovery_state(&context, &remotes, tcp, TrafficClass::Throughput,)
            .uncovered_ranges,
        vec![OffsetRange {
            start: 4096,
            end: 8192,
        }],
        "the never-attempted second range remains immediately eligible"
    );

    let data_ack_progress_paths = controller.apply_product_ack(
        &context,
        &remotes,
        &[OffsetRange {
            start: 0,
            end: 4096,
        }],
        Instant::now(),
    );

    assert!(
        data_ack_progress_paths.is_empty(),
        "a Data ACK for duplicated data does not identify the delivering path"
    );
    assert!(controller.path_is_stale(tcp));
    assert_eq!(
        controller
            .path_recovery_state(&context, &remotes, tcp, TrafficClass::Throughput,)
            .uncovered_ranges,
        vec![OffsetRange {
            start: 4096,
            end: 8192,
        }],
        "acknowledging one recovery range leaves the disjoint range eligible"
    );
}

#[tokio::test]
async fn exact_recovery_copy_suppresses_only_its_recovery_interval() {
    let stream_id = StreamId(21);
    let context =
        client_test_context_with_paths(&["tcp://127.0.0.1:10251", "quic://127.0.0.1:10252"]);
    let (tcp_commands, mut tcp_receivers) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, tcp_commands), 8);
    let tcp = remotes.paths[0].instance();
    let (udp_commands, mut udp_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        0,
        udp_commands,
    ));
    let udp = remotes
        .paths
        .iter()
        .find(|path| path.key().underlay == UnderlayProtocol::Udp)
        .expect("UDP attachment")
        .instance();
    for instance in [tcp, udp] {
        context.install_relay_path_instance_for_test(instance);
    }
    consume_client_path_proof_for_test(&mut tcp_receivers);
    consume_client_path_proof_for_test(&mut udp_receivers);
    let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
    let frame = send_stream
        .send_data(Bytes::from(vec![0x5b; 4096]))
        .expect("retained original data");
    let mut controller = RequestMultipathController::new(stream_id);
    controller
        .request
        .flights
        .record_original_frame_instance(tcp, &frame);
    controller
        .request
        .flights
        .record_reinjection_frame_instance(udp, &frame);
    assert!(controller.mark_path_stale(tcp));

    let recovery =
        controller.path_recovery_state(&context, &remotes, tcp, TrafficClass::Throughput);
    assert!(
        recovery.uncovered_ranges.is_empty(),
        "the current exact-range recovery copy suppresses a duplicate"
    );
    assert!(
        recovery.retry_deadline.is_some(),
        "the current copy exposes its retry wake deadline"
    );

    controller
        .request
        .flights
        .age_reinjected_flights_for_test(Duration::from_secs(2));
    assert_eq!(
        controller
            .path_recovery_state(&context, &remotes, tcp, TrafficClass::Throughput,)
            .uncovered_ranges,
        vec![OffsetRange {
            start: 0,
            end: 4096,
        }],
        "an unacknowledged exact range is eligible after its recovery interval"
    );

    drop(remotes.remove_path_instance(udp));
    controller.reconcile_request_path_state(&context, &remotes);
    assert!(
        controller.path_is_stale(tcp),
        "sole-survivor scheduling keeps stale evidence until exact requalification"
    );
    assert!(!controller.has_reinjection_path(&context, &remotes, tcp, TrafficClass::Throughput,));
    assert!(matches!(
        controller.try_enqueue_requalification_probe(
            &context,
            &remotes,
            &send_stream,
            TrafficClass::Throughput,
            4096,
        ),
        Ok(attempt) if attempt.published_payload_bytes() == Some(4096)
    ));
    assert!(matches!(
        try_recv_reliable_path_command(&mut tcp_receivers),
        Some(ReliablePathCommand::SendFrame(
            Frame::StreamRequalifyData { .. }
        ))
    ));
}

#[tokio::test]
async fn accepted_request_copy_owns_exact_reserve_until_data_ack() {
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10301?initial-srtt-s=0.05&initial-rate-mbps=100",
        "tcp://127.0.0.1:10302?initial-srtt-s=0.01&initial-rate-mbps=500",
    ]);
    let stream_id = StreamId(230);
    let (owner_commands, mut owner_receivers) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, owner_commands), 8);
    let owner = remotes.paths[0].instance();
    let (target_commands, mut target_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream(
        stream_id,
        1,
        target_commands.clone(),
    ));
    let target = remotes
        .paths
        .iter()
        .find(|path| path.key().index == 1)
        .expect("recovery target")
        .instance();
    consume_client_path_proof_for_test(&mut owner_receivers);
    consume_client_path_proof_for_test(&mut target_receivers);
    for instance in [owner, target] {
        context.install_relay_path_instance_for_test(instance);
    }
    seed_client_bulk_evidence_for_test(&context, target);

    let limits = context.mux_limits;
    let mut controller = RequestMultipathController::new(stream_id);
    let target_path = remotes
        .paths
        .iter()
        .find(|path| path.instance() == target)
        .expect("exact target path");
    let snapshot = controller
        .request_reinjection_target_snapshot(&context, &remotes, target_path)
        .expect("published exact target Product authority");
    let pending_bytes =
        reliable_product_recovery_window_bytes(Some(snapshot), TrafficClass::Throughput, limits)
            .max(
                adaptive_reliable_relay_reinjection_bytes(
                    Some(snapshot),
                    TrafficClass::Throughput,
                    limits,
                )
                .max(reliable_bulk_carrier_feed_quantum_bytes(limits)),
            );
    let repair = data_frame(stream_id, 0, pending_bytes);
    target_commands
        .try_enqueue_reinjection_frame(repair.clone(), TrafficClass::Throughput)
        .expect("accepted target command remains pending");

    controller.record_original_frame_for_test(owner, &repair);
    let (_, accepted_deadline) = controller
        .request
        .flights
        .record_reinjection_frame_instance_with_suppression_interval(
            target,
            &repair,
            Duration::ZERO,
        );
    assert!(accepted_deadline.is_some_and(|deadline| deadline <= Instant::now()));
    assert_eq!(
        controller.accepted_reinjected_data_bytes(target),
        pending_bytes,
        "a retry deadline never releases the same exact reliable target's accepted Product debt",
    );
    let sender_queue = ReliableRelaySenderQueue::default();
    let (target_selection, target_service_exhausted) = controller.reinjection_path_snapshot(
        &context,
        &remotes,
        &[owner],
        &sender_queue,
        4096,
        limits,
    );
    assert!(
        target_selection.is_none() && target_service_exhausted,
        "the accepted un-DataACKed copy consumes the exact target reserve after its retry deadline",
    );
    let next_gap = data_frame(stream_id, pending_bytes as u64, 4096);
    controller.record_original_frame_for_test(owner, &next_gap);
    let persistent = controller.data_ack_gap_reinjection_service_model(
        &context,
        &remotes,
        &next_gap,
        TrafficClass::Throughput,
        &sender_queue,
        4096,
        limits,
    );
    assert!(persistent.reinjection_target.is_none());
    assert!(persistent.target_service_exhausted);

    let pending = recv_reliable_path_command(&mut target_receivers)
        .await
        .expect("pending recovery command");
    target_receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&pending));
    assert_eq!(
        controller
            .reinjection_path_snapshot(&context, &remotes, &[owner], &sender_queue, 4096, limits,)
            .0
            .map(|(instance, _, _)| instance),
        None,
        "native command release cannot mint same-incarnation Product recovery authority",
    );

    controller.apply_product_ack(
        &context,
        &remotes,
        &[OffsetRange {
            start: 0,
            end: pending_bytes as u64,
        }],
        Instant::now(),
    );
    assert_eq!(controller.accepted_reinjected_data_bytes(target), 0);
    assert_eq!(
        controller
            .reinjection_path_snapshot(&context, &remotes, &[owner], &sender_queue, 4096, limits,)
            .0
            .map(|(instance, _, _)| instance),
        Some(target),
        "Data ACK releases the exact Product repair debt and restores target service",
    );
}

#[tokio::test]
async fn request_recovery_skips_exhausted_fast_target_for_free_second_target() {
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10311?initial-srtt-s=0.08&initial-rate-mbps=100",
        "tcp://127.0.0.1:10312?initial-srtt-s=0.005&initial-rate-mbps=1000",
        "tcp://127.0.0.1:10313?initial-srtt-s=0.04&initial-rate-mbps=200",
    ]);
    let stream_id = StreamId(231);
    let (owner_commands, mut owner_receivers) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, owner_commands), 8);
    let owner = remotes.paths[0].instance();
    let (fast_commands, mut fast_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream(stream_id, 1, fast_commands));
    let fast = remotes
        .paths
        .iter()
        .find(|path| path.key().index == 1)
        .expect("fast recovery target")
        .instance();
    let (free_commands, mut free_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream(stream_id, 2, free_commands));
    let free = remotes
        .paths
        .iter()
        .find(|path| path.key().index == 2)
        .expect("free second target")
        .instance();
    for receivers in [
        &mut owner_receivers,
        &mut fast_receivers,
        &mut free_receivers,
    ] {
        consume_client_path_proof_for_test(receivers);
    }
    for instance in [owner, fast, free] {
        context.install_relay_path_instance_for_test(instance);
    }
    for instance in [fast, free] {
        seed_client_bulk_evidence_for_test(&context, instance);
    }

    let limits = context.mux_limits;
    let mut controller = RequestMultipathController::new(stream_id);
    let sender_queue = ReliableRelaySenderQueue::default();
    let fast_path = remotes
        .paths
        .iter()
        .find(|path| path.instance() == fast)
        .expect("fast recovery target path");
    let fast_snapshot = controller
        .request_reinjection_target_snapshot(&context, &remotes, fast_path)
        .expect("fast target must publish exact Product authority");
    assert!(fast_snapshot.data_level_limit_bytes > 0);
    assert_eq!(
        controller
            .reinjection_path_snapshot(&context, &remotes, &[owner], &sender_queue, 4096, limits,)
            .0
            .map(|(instance, _, _)| instance),
        Some(fast),
        "the fixture's fastest target must win before its service is consumed",
    );
    let occupied_bytes = reliable_product_recovery_window_bytes(
        Some(fast_snapshot),
        TrafficClass::Throughput,
        limits,
    )
    .max(
        adaptive_reliable_relay_reinjection_bytes(
            Some(fast_snapshot),
            TrafficClass::Throughput,
            limits,
        )
        .max(reliable_bulk_carrier_feed_quantum_bytes(limits)),
    );
    let repair = data_frame(stream_id, 0, occupied_bytes);
    controller.record_original_frame_for_test(owner, &repair);
    controller
        .request
        .flights
        .record_reinjection_frame_instance_with_suppression_interval(
            fast,
            &repair,
            Duration::from_secs(60),
        );

    assert_eq!(
        controller
            .reinjection_path_snapshot(&context, &remotes, &[owner], &sender_queue, 4096, limits,)
            .0
            .map(|(instance, _, _)| instance),
        Some(free),
        "stale/failure recovery must skip a faster target whose service window is exhausted",
    );
    let persistent_gap = data_frame(stream_id, occupied_bytes as u64, 4096);
    controller.record_original_frame_for_test(owner, &persistent_gap);
    let persistent = controller.data_ack_gap_reinjection_service_model(
        &context,
        &remotes,
        &persistent_gap,
        TrafficClass::Throughput,
        &sender_queue,
        4096,
        limits,
    );
    assert_eq!(
        persistent
            .reinjection_target
            .map(|(target, _)| target.instance),
        Some(free),
        "persistent ACK-gap recovery must make the same target-service decision",
    );
}

#[tokio::test]
async fn bound_request_repair_consumes_only_its_exact_targets_emergency_reserve() {
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10321?initial-srtt-s=0.05&initial-rate-mbps=100",
        "tcp://127.0.0.1:10322?initial-srtt-s=0.01&initial-rate-mbps=500",
        "tcp://127.0.0.1:10323?initial-srtt-s=0.02&initial-rate-mbps=300",
    ]);
    let stream_id = StreamId(232);
    let (owner_commands, mut owner_receivers) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, owner_commands), 8);
    let owner = remotes.paths[0].instance();
    let (first_commands, mut first_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream(stream_id, 1, first_commands));
    let first = remotes
        .paths
        .iter()
        .find(|path| path.key().index == 1)
        .expect("first recovery target")
        .instance();
    let (second_commands, mut second_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream(stream_id, 2, second_commands));
    let second = remotes
        .paths
        .iter()
        .find(|path| path.key().index == 2)
        .expect("second recovery target")
        .instance();
    for receivers in [
        &mut owner_receivers,
        &mut first_receivers,
        &mut second_receivers,
    ] {
        consume_client_path_proof_for_test(receivers);
    }
    for instance in [owner, first, second] {
        context.install_relay_path_instance_for_test(instance);
    }
    for instance in [first, second] {
        seed_client_bulk_evidence_for_test(&context, instance);
    }

    let limits = context.mux_limits;
    let mut controller = RequestMultipathController::new(stream_id);
    let first_path = remotes
        .paths
        .iter()
        .find(|path| path.instance() == first)
        .expect("first exact target path");
    let second_path = remotes
        .paths
        .iter()
        .find(|path| path.instance() == second)
        .expect("second exact target path");
    let first_snapshot = controller
        .request_reinjection_target_snapshot(&context, &remotes, first_path)
        .expect("first target must publish exact Product authority");
    let second_snapshot = controller
        .request_reinjection_target_snapshot(&context, &remotes, second_path)
        .expect("second target must publish exact Product authority");
    let first_window = reliable_product_recovery_window_bytes(
        Some(first_snapshot),
        TrafficClass::Throughput,
        limits,
    );
    let second_window = reliable_product_recovery_window_bytes(
        Some(second_snapshot),
        TrafficClass::Throughput,
        limits,
    );
    assert_eq!(
        first_window, second_window,
        "the configured path-flight cap makes both ordinary windows identical",
    );
    let emergency_quantum = adaptive_reliable_relay_reinjection_bytes(
        Some(first_snapshot),
        TrafficClass::Throughput,
        limits,
    )
    .max(reliable_bulk_carrier_feed_quantum_bytes(limits));

    let mut sender_queue = ReliableRelaySenderQueue::default();
    let first_original = data_frame(stream_id, 0, first_window);
    let repair = data_frame(stream_id, first_window as u64, emergency_quantum);
    sender_queue.push_critical_reinjection_with_cause(
        repair.clone(),
        RelaySendCause::ClientPathFailureReinjection(ClientReinjectionOutputIdentity {
            instance: first,
        }),
    );
    let (_, queued_front) = sender_queue.front().expect("bound repair queue front");
    let ReliableRelayQueuedWorkKind::Reinjection { cause, .. } = queued_front.kind else {
        panic!("critical bound repair must remain at the queue front");
    };
    assert_eq!(
        cause.client_target(),
        Some(first),
        "request recovery stores an exact target before carrier dispatch",
    );

    controller.record_original_frame_for_test(first, &first_original);
    controller.record_original_frame_for_test(owner, &repair);
    let selected = controller
        .reinjection_path_snapshot(
            &context,
            &remotes,
            &[owner],
            &sender_queue,
            limits.max_repair_bytes,
            limits,
        )
        .0
        .map(|(instance, _, _)| instance);
    assert_eq!(
        selected,
        Some(second),
        "a queued quantum bound to target A consumes only A's exact emergency reserve; target B retains its independent reserve",
    );
    let persistent = controller.data_ack_gap_reinjection_service_model(
        &context,
        &remotes,
        &repair,
        TrafficClass::Throughput,
        &sender_queue,
        limits.max_repair_bytes,
        limits,
    );
    assert_eq!(
        persistent
            .reinjection_target
            .map(|(target, _)| target.instance),
        Some(second),
        "persistent-gap selection must use the same exact-target queue view as failure recovery",
    );
}

#[tokio::test]
async fn committed_request_recovery_copy_survives_target_drain_until_retry_deadline() {
    let stream_id = StreamId(22);
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10251",
        "quic://127.0.0.1:10252",
        "tcp://127.0.0.1:10253",
    ]);
    let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, owner_commands), 8);
    let owner = remotes.paths[0].instance();
    let (copy_commands, _copy_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        0,
        copy_commands.clone(),
    ));
    let copy = remotes
        .paths
        .iter()
        .find(|path| path.key().underlay == UnderlayProtocol::Udp)
        .expect("QUIC recovery copy")
        .instance();
    let (survivor_commands, _survivor_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream(stream_id, 1, survivor_commands));
    let survivor = remotes
        .paths
        .iter()
        .find(|path| path.key().underlay == UnderlayProtocol::Tcp && path.key().index == 1)
        .expect("TCP recovery survivor")
        .instance();
    for instance in [owner, copy, survivor] {
        context.install_relay_path_instance_for_test(instance);
    }
    let frame = data_frame(stream_id, 0, 4096);
    let mut controller = RequestMultipathController::new(stream_id);
    controller
        .request
        .flights
        .record_original_frame_instance(owner, &frame);
    controller
        .request
        .flights
        .record_reinjection_frame_instance(copy, &frame);
    assert!(controller.mark_path_stale(owner));

    copy_commands.begin_path_drain();
    let recovery =
        controller.path_recovery_state(&context, &remotes, owner, TrafficClass::Throughput);
    assert!(
        recovery.uncovered_ranges.is_empty(),
        "an already-committed request copy remains transport-owned during ordered drain",
    );
    assert!(
        recovery.retry_deadline.is_some(),
        "the committed request copy suppresses only until its bounded retry deadline",
    );

    drop(remotes.remove_path_instance(copy));
    assert_eq!(
        controller
            .path_recovery_state(&context, &remotes, owner, TrafficClass::Throughput,)
            .uncovered_ranges,
        vec![OffsetRange {
            start: 0,
            end: 4096,
        }],
        "exact detach revokes the old copy while the healthy survivor makes recovery ready",
    );
}

#[tokio::test]
async fn missing_owner_detection_is_fenced_by_attachment_instance() {
    let context = client_test_context_with_paths(&["tcp://127.0.0.1:10251"]);
    let stream_id = StreamId(31);
    let (first_commands, _first_receivers) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, first_commands), 8);
    let old_instance = remotes.paths[0].instance();
    context.install_relay_path_instance_for_test(old_instance);
    let frame = data_frame(stream_id, 0, 4096);
    let mut controller = RequestMultipathController::new(stream_id);
    controller
        .request
        .flights
        .record_original_frame_instance(old_instance, &frame);
    let original_assignment_at = controller
        .request
        .flights
        .unique_original_sent_at_for_frame(&frame)
        .expect("original request assignment epoch");

    drop(remotes.remove_path_instance(old_instance));
    let (replacement_commands, _replacement_receivers) = reliable_path_command_channels(8);
    remotes.attach(opened_test_relay_stream(stream_id, 0, replacement_commands));
    let replacement = remotes.paths[0].instance();
    context.install_relay_path_instance_for_test(replacement);
    assert_eq!(replacement.key, old_instance.key);
    assert_ne!(replacement, old_instance);
    let observation = controller.data_ack_gap_reinjection_model(
        &context,
        &remotes,
        &frame,
        TrafficClass::Throughput,
    );
    assert!(
        observation.original_path_timing.is_none(),
        "a replacement attachment cannot lend path timing to the prior instance's Data ACK gap"
    );
    assert_eq!(
        observation.original_assignment_at,
        Some(original_assignment_at),
        "Data ACK recovery retains the exact original assignment epoch across attachment replacement",
    );

    assert_eq!(
        controller.request_recovery_original_paths(&remotes),
        vec![old_instance]
    );
    assert_eq!(
        controller
            .path_recovery_state(&context, &remotes, old_instance, TrafficClass::Throughput,)
            .uncovered_ranges,
        vec![OffsetRange {
            start: 0,
            end: 4096,
        }],
        "failure makes the exact unresolved original range immediately recoverable"
    );

    controller
        .request
        .flights
        .record_reinjection_frame_instance(replacement, &frame);
    let current_recovery =
        controller.path_recovery_state(&context, &remotes, old_instance, TrafficClass::Throughput);
    assert!(current_recovery.uncovered_ranges.is_empty());
    assert!(current_recovery.retry_deadline.is_some());

    controller
        .request
        .flights
        .age_reinjected_flights_for_test(Duration::from_secs(2));
    assert_eq!(
        controller
            .path_recovery_state(&context, &remotes, old_instance, TrafficClass::Throughput,)
            .uncovered_ranges,
        vec![OffsetRange {
            start: 0,
            end: 4096,
        }],
        "an unresolved failed-owner range becomes eligible after its recovery interval"
    );

    controller.release_all(&context);
}
