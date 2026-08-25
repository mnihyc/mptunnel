use super::super::test_support::{
    client_test_context_with_paths, consume_client_path_proof_for_test, opened_test_relay_stream,
    opened_test_relay_stream_with_underlay, seed_client_bulk_evidence_for_test,
};
use super::*;
use crate::model::capacity::{PathRateSample, RELIABLE_INITIAL_WINDOW_PACKETS};
use crate::model::path::{PathPolicy, next_carrier_path_instance_id};
use crate::model::request_evidence::RequestPerFlowRateModel;
use crate::protocol::frame::reliable_stream_frame_extent;
use crate::protocol::{PathId, PathUsage};
use crate::runtime::path::commands::{
    ReliablePathCommand, recv_reliable_path_command, reliable_path_command_channels,
    reliable_path_command_pending_bytes, try_recv_reliable_path_command,
};
use crate::runtime::path::tcp::group::ClientTcpEndpointControlState;
use crate::runtime::stream::ReliableRelayRemoteSet;
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
}

#[tokio::test]
async fn ordinary_data_does_not_borrow_a_successor_carriers_health() {
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10258?initial-srtt-ms=5",
        "quic://127.0.0.1:10259?initial-srtt-ms=80",
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
        &HashSet::new(),
    );
    let predecessor_observation = observation
        .path_by_instance(predecessor)
        .expect("predecessor remains in attachment topology");
    assert!(predecessor_observation.shared_snapshot.is_none());
    assert!(!predecessor_observation.can_enqueue_stream_lane);
    assert!(matches!(
        choose_observed_ordinary_data_path(&observation, TrafficClass::Latency, 4096, 0, &[]),
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
        &HashSet::new(),
    );
    let current_observation = observation
        .path_by_instance(current)
        .expect("exact current attachment observation");
    assert!(current_observation.shared_snapshot.is_some());
    assert!(current_observation.can_enqueue_stream_lane);
    assert!(!current_observation.has_bulk_model_evidence);
    assert!(matches!(
        choose_observed_ordinary_data_path(&observation, TrafficClass::Latency, 4096, 0, &[]),
        ObservedOrdinaryPathChoice::Selected(instance) if instance == current
    ));
}

#[tokio::test]
async fn request_plan_revalidates_exact_health_after_same_key_replacement() {
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10261?initial-srtt-ms=5",
        "quic://127.0.0.1:10262?initial-srtt-ms=80",
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
    let (key, active_flows, active_latency_flows) =
        plan.load_expectation().expect("unowned selected path load");
    let stale_plan_load_claim = context
        .try_reserve_relay_path_load_if_unchanged(
            key,
            TrafficClass::Latency,
            active_flows,
            active_latency_flows,
        )
        .expect("configured-slot load remains reservable across replacement");
    let reserved_predecessor_command = selected_commands
        .try_reserve_admitted_frame(frame.clone(), TrafficClass::Latency)
        .expect("the predecessor queue can still reserve before exact apply validation");
    assert!(
        !plan.target_retains_exact_eligibility(&context, TrafficClass::Latency),
        "apply must reject the observed predecessor even though attachment membership did not change",
    );
    drop(reserved_predecessor_command);
    drop(stale_plan_load_claim);
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
async fn request_plan_revalidates_same_instance_health_transitions() {
    let context = client_test_context_with_paths(&["tcp://127.0.0.1:10263?initial-srtt-ms=5"]);
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
        "tcp://127.0.0.1:10264?initial-srtt-ms=5",
        "quic://127.0.0.1:10265?initial-srtt-ms=80",
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
        "tcp://127.0.0.1:10253?initial-srtt-ms=40",
        "quic://127.0.0.1:10254?initial-srtt-ms=5",
        "quic://127.0.0.1:10255?initial-srtt-ms=80",
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
        "tcp://127.0.0.1:10256?initial-srtt-ms=5",
        "quic://127.0.0.1:10257?initial-srtt-ms=40",
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
}

#[tokio::test]
async fn retained_tail_uses_only_a_measured_earlier_completion() {
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10253?initial-srtt-ms=20",
        "quic://127.0.0.1:10254?initial-srtt-ms=20",
        "quic://127.0.0.1:10255?initial-srtt-ms=20",
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
        "tcp://127.0.0.1:10251?initial-srtt-ms=20&initial-rate-mbps=200",
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
    assert!(
        controller
            .reinjection_avoid_instances(&frame, RelaySendCause::AckGapReinjection, &remotes,)
            .is_empty(),
        "an original attachment that is no longer live cannot exclude its replacement",
    );

    let selected = controller
        .choose_lowest_eta_relay_path(
            &context,
            &remotes,
            &frame,
            TrafficClass::Throughput,
            RelaySendCause::AckGapReinjection,
            &[old],
        )
        .expect("the replacement is a distinct carrier output");
    assert_eq!(remotes.paths[selected].instance(), replacement);
}

#[tokio::test]
async fn ack_gap_repair_history_does_not_exhaust_non_original_outputs() {
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10251?initial-srtt-ms=20&initial-rate-mbps=200",
        "quic://127.0.0.1:10252?initial-srtt-ms=15&initial-rate-mbps=250",
        "quic://127.0.0.1:10253?initial-srtt-ms=10&initial-rate-mbps=300",
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
        vec![original],
        "repair history is duplicate suppression evidence, not a permanent path blacklist",
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
        Err(RequestMultipathPlanError::ServiceBlocked)
    ));
    controller
        .request
        .flights
        .age_reinjected_flights_for_test(Duration::from_secs(1));
    let selected = controller
        .choose_lowest_eta_relay_path(
            &context,
            &remotes,
            &frame,
            TrafficClass::Throughput,
            RelaySendCause::AckGapReinjection,
            &avoid,
        )
        .expect("a live non-original attachment remains available");
    assert!(
        [first_repair, second_repair].contains(&remotes.paths[selected].instance()),
        "the repair must use a non-original live attachment",
    );
}

#[tokio::test]
async fn portable_quic_repair_waits_for_exact_stream_flight_but_failure_recovery_does_not() {
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10251?initial-srtt-ms=20&initial-rate-mbps=200",
        "quic://127.0.0.1:10252?initial-srtt-ms=10&initial-rate-mbps=300",
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

    assert!(matches!(
        controller.choose_lowest_eta_relay_path(
            &context,
            &remotes,
            &repair,
            TrafficClass::Throughput,
            RelaySendCause::AckGapReinjection,
            &[original],
        ),
        Err(RequestMultipathPlanError::ServiceBlocked)
    ));
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
        .expect("Data ACK drains exact QUIC-stream ownership");
    assert_eq!(remotes.paths[selected].instance(), quic);
}

#[tokio::test]
async fn repair_waits_for_writer_dequeued_bytes_to_flush() {
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10251?initial-srtt-ms=20&initial-rate-mbps=200",
        "quic://127.0.0.1:10252?initial-srtt-ms=10&initial-rate-mbps=300",
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

    assert!(matches!(
        controller.choose_lowest_eta_relay_path(
            &context,
            &remotes,
            &repair,
            TrafficClass::Throughput,
            RelaySendCause::AckGapReinjection,
            &[original],
        ),
        Err(RequestMultipathPlanError::ServiceBlocked)
    ));
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
        .expect("flushed writer batch reopens repair eligibility");
    assert_eq!(remotes.paths[selected].instance(), quic);
}

#[tokio::test]
async fn unbound_reinjection_requires_idle_but_measured_recovery_may_use_busy() {
    let context = client_test_context_with_paths(&[
        "quic://127.0.0.1:10251?initial-srtt-ms=20&initial-rate-mbps=200",
        "tcp://127.0.0.1:10252?initial-srtt-ms=2&initial-rate-mbps=1000",
        "tcp://127.0.0.1:10253?initial-srtt-ms=30&initial-rate-mbps=100",
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
        health.tcp[busy.key.index].carrier_queue_bytes = 8 * 1024;
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
        .expect("the idle TCP carrier remains eligible");
    assert_eq!(
        remotes.paths[selected].instance(),
        idle,
        "unbound repair must not queue behind native flight on a faster TCP carrier",
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
        .set_per_flow_rate(RequestPerFlowRateModel {
            rate_bps: 120_000_000.0,
            delivery_samples: 10,
        });
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
            .and_then(|state| state.per_flow_rate())
            .is_some()
    );
    controller
        .request
        .path_states
        .get_mut(udp)
        .set_per_flow_rate(RequestPerFlowRateModel {
            rate_bps: 240_000_000.0,
            delivery_samples: 10,
        });

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
async fn capacity_reference_ignores_immature_higher_rate_samples() {
    let (context, remotes, tcp, udp) = mixed_remote_set().await;
    seed_client_bulk_evidence_for_test(&context, tcp);
    seed_client_bulk_evidence_for_test(&context, udp);
    let mut controller = RequestMultipathController::new(StreamId(17));
    controller
        .request
        .path_states
        .get_mut(tcp)
        .set_per_flow_rate(RequestPerFlowRateModel {
            rate_bps: 100_000_000.0,
            delivery_samples: 10,
        });
    controller
        .request
        .path_states
        .get_mut(udp)
        .set_per_flow_rate(RequestPerFlowRateModel {
            rate_bps: 900_000_000.0,
            delivery_samples: 1,
        });
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
            .and_then(|state| state.per_flow_rate())
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
async fn product_ack_returns_the_exact_path_that_made_progress() {
    let (context, remotes, tcp, _udp) = mixed_remote_set().await;
    let stream_id = StreamId(18);
    let payload_bytes = 4096;
    let frame = data_frame(stream_id, 0, payload_bytes);
    let mut controller = RequestMultipathController::new(stream_id);
    assert!(controller.mark_path_stale(tcp));
    controller
        .request
        .flights
        .record_original_frame_instance(tcp, &frame);

    let data_ack_progress_paths = controller.apply_product_ack(
        &context,
        &remotes,
        &[OffsetRange {
            start: 0,
            end: payload_bytes as u64,
        }],
        Instant::now(),
    );

    assert_eq!(data_ack_progress_paths.as_slice(), &[tcp]);
    assert!(
        !controller.path_is_stale(tcp),
        "exact Data ACK progress makes the path schedulable again"
    );
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
        &controller.request.stale_paths,
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
        choose_observed_ordinary_data_path(&observation, TrafficClass::Latency, 4096, 0, &[]),
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
        &controller.request.stale_paths,
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
            .path_recovery_state(&remotes, tcp, Duration::from_secs(60))
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
            .path_recovery_state(&remotes, tcp, Duration::from_secs(60))
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
    let frame = data_frame(stream_id, 0, 4096);
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

    let recovery = controller.path_recovery_state(&remotes, tcp, Duration::from_secs(1));
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
            .path_recovery_state(&remotes, tcp, Duration::from_secs(1))
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
        !controller.path_is_stale(tcp),
        "staleness is removed when the original attachment becomes the sole survivor"
    );
    assert!(!controller.has_reinjection_path(&remotes, tcp));
}

#[tokio::test]
async fn missing_owner_detection_is_fenced_by_attachment_instance() {
    let context = client_test_context_with_paths(&["tcp://127.0.0.1:10251"]);
    let stream_id = StreamId(31);
    let (first_commands, _first_receivers) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, first_commands), 8);
    let old_instance = remotes.paths[0].instance();
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
            .path_recovery_state(&remotes, old_instance, Duration::from_secs(1))
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
        controller.path_recovery_state(&remotes, old_instance, Duration::from_secs(1));
    assert!(current_recovery.uncovered_ranges.is_empty());
    assert!(current_recovery.retry_deadline.is_some());

    controller
        .request
        .flights
        .age_reinjected_flights_for_test(Duration::from_secs(2));
    assert_eq!(
        controller
            .path_recovery_state(&remotes, old_instance, Duration::from_secs(1))
            .uncovered_ranges,
        vec![OffsetRange {
            start: 0,
            end: 4096,
        }],
        "an unresolved failed-owner range becomes eligible after its recovery interval"
    );

    controller.release_all(&context);
}
