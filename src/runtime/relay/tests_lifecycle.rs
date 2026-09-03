use super::*;
use crate::config::{ClientSecurityConfig, ResourceLimits, SharedSecret};
use crate::model::path::{RelayPathInstance, next_carrier_path_instance_id};
use crate::protocol::{PathId, PathUsage, StreamAttachmentPhase, TargetAddr};
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandSender, recv_reliable_path_command,
    reliable_path_command_channels, try_recv_reliable_path_command,
};
use crate::runtime::relay::io::accepted_copy_wake_is_due;
use crate::runtime::stream::{ReliablePathStream, ReliablePathStreamOutput};
use crate::transport::PathSpec;
use std::time::Duration;

fn test_stream(
    stream_id: StreamId,
    underlay: UnderlayProtocol,
    path_index: usize,
    commands: ReliablePathCommandSender,
    lane: TrafficClass,
) -> ReliablePathStream {
    let (_frames_tx, frames_rx) = mpsc::channel(1);
    ReliablePathStream {
        stream_id,
        max_offset: MuxLimits::default().max_stream_window_bytes,
        lane,
        underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
        output: ReliablePathStreamOutput::fixed(
            underlay,
            PathId(path_index as u16),
            commands,
            MuxLimits::default(),
        ),
        frames: frames_rx.into(),
    }
}

fn singleton_return_plan(remotes: &ReliableRelayRemoteSet) -> ClientReliableReturnPlan {
    let opening = remotes.paths[0].instance();
    ClientReliableReturnPlan::from_initial_open(
        ReliableRelayOpenedStartup {
            plan: Arc::new(
                ReliableRelayReturnPlan::new(
                    0,
                    PathUsage::Available,
                    vec![(opening.key, Some(opening.path_instance_id))],
                )
                .expect("singleton return plan"),
            ),
            opening_ordinal: 0,
            failed_ordinals: Vec::new(),
        },
        opening,
    )
    .expect("singleton client return state")
}

fn relay_key(underlay: UnderlayProtocol, index: usize) -> RelayPathKey {
    RelayPathKey { underlay, index }
}

fn accepted_two_candidate_return_plan(
    stream_id: StreamId,
) -> (
    ClientReliableReturnPlan,
    ReliableRelayRemoteSet,
    RelayPathInstance,
) {
    let first_key = relay_key(UnderlayProtocol::Tcp, 0);
    let second_key = relay_key(UnderlayProtocol::Udp, 0);
    let (first_commands, _first_receivers) = reliable_path_command_channels(8);
    let first = OpenedRemoteStream::pending(
        test_stream(
            stream_id,
            first_key.underlay,
            first_key.index,
            first_commands,
            TrafficClass::Throughput,
        ),
        first_key.index,
    );
    let first_path_instance_id = first.path_instance_id();
    let (second_commands, _second_receivers) = reliable_path_command_channels(8);
    let second = OpenedRemoteStream::pending(
        test_stream(
            stream_id,
            second_key.underlay,
            second_key.index,
            second_commands,
            TrafficClass::Throughput,
        ),
        second_key.index,
    );
    let plan = Arc::new(
        ReliableRelayReturnPlan::new(
            58_400,
            PathUsage::Available,
            vec![
                (first_key, Some(first_path_instance_id)),
                (second_key, None),
            ],
        )
        .expect("two-candidate return plan"),
    );
    let startup = ReliableRelayOpenedStartup {
        plan,
        opening_ordinal: 0,
        failed_ordinals: Vec::new(),
    };
    let mut remotes = ReliableRelayRemoteSet::new(first, 8);
    let opening = remotes.paths[0].instance();
    let mut return_plan =
        ClientReliableReturnPlan::from_initial_open(startup, opening).expect("client return state");
    assert_eq!(
        return_plan.begin_candidate_for_open(second_key, None),
        Some(1),
    );
    assert_eq!(
        remotes.attach_candidate(second),
        ReliableRelayAttachOutcome::Attached,
    );
    let second_instance = remotes
        .path_instance_for_key(second_key)
        .expect("committed second attachment");
    return_plan
        .settle_accepted(1, second_instance)
        .expect("settle second startup candidate");
    (return_plan, remotes, second_instance)
}

async fn expect_return_plan_final(
    receivers: &mut crate::runtime::path::commands::ReliablePathCommandReceivers,
    stream_id: StreamId,
    expected: &[u8],
) {
    for _ in 0..8 {
        let command = recv_reliable_path_command(receivers)
            .await
            .expect("request control command");
        if let ReliablePathCommand::SendFrame(Frame::StreamReturnPlanFinal {
            stream_id: actual_stream_id,
            retained_ordinals,
        }) = command
        {
            assert_eq!(actual_stream_id, stream_id);
            assert_eq!(retained_ordinals, expected);
            return;
        }
    }
    panic!("STREAM_RETURN_PLAN_FINAL was not queued");
}

#[test]
fn immutable_return_plan_preserves_ordinals_tier_and_wire_phase() {
    let tcp = relay_key(UnderlayProtocol::Tcp, 0);
    let quic = relay_key(UnderlayProtocol::Udp, 0);
    let plan = ReliableRelayReturnPlan::new(
        58_400,
        PathUsage::Available,
        vec![(tcp, Some(next_carrier_path_instance_id())), (quic, None)],
    )
    .expect("return plan");

    assert_eq!(plan.candidates()[0].ordinal, 0);
    assert_eq!(plan.candidates()[1].ordinal, 1);
    assert_eq!(plan.candidates()[1].path_instance_id, None);
    assert_eq!(plan.candidate_tier(), PathUsage::Available);
    assert_eq!(
        plan.wire(StreamAttachmentPhase::Startup, 1),
        crate::protocol::StreamReturnPlan {
            trigger_bytes: 58_400,
            candidate_total: 2,
            candidate_tier: PathUsage::Available,
            phase: StreamAttachmentPhase::Startup,
            candidate_ordinal: 1,
        },
    );
}

#[test]
fn backup_only_singleton_preserves_backup_tier_without_a_gate() {
    let key = relay_key(UnderlayProtocol::Tcp, 0);
    let plan = ReliableRelayReturnPlan::new(0, PathUsage::Backup, vec![(key, None)])
        .expect("backup singleton");
    let wire = plan.wire(StreamAttachmentPhase::Startup, 0);

    assert_eq!(wire.trigger_bytes, 0);
    assert_eq!(wire.candidate_total, 1);
    assert_eq!(wire.candidate_tier, PathUsage::Backup);
}

#[tokio::test]
async fn exact_successor_cannot_inherit_a_frozen_startup_ordinal() {
    let stream_id = StreamId(40);
    let first_key = relay_key(UnderlayProtocol::Tcp, 0);
    let frozen_key = relay_key(UnderlayProtocol::Udp, 0);
    let (commands, _receivers) = reliable_path_command_channels(8);
    let first = OpenedRemoteStream::pending(
        test_stream(
            stream_id,
            first_key.underlay,
            first_key.index,
            commands,
            TrafficClass::Throughput,
        ),
        first_key.index,
    );
    let first_path_instance_id = first.path_instance_id();
    let frozen = next_carrier_path_instance_id();
    let successor = next_carrier_path_instance_id();
    let plan = Arc::new(
        ReliableRelayReturnPlan::new(
            58_400,
            PathUsage::Available,
            vec![
                (first_key, Some(first_path_instance_id)),
                (frozen_key, Some(frozen)),
            ],
        )
        .expect("exact return plan"),
    );
    let remotes = ReliableRelayRemoteSet::new(first, 8);
    let opening = remotes.paths[0].instance();
    let mut startup = ClientReliableReturnPlan::from_initial_open(
        ReliableRelayOpenedStartup {
            plan,
            opening_ordinal: 0,
            failed_ordinals: Vec::new(),
        },
        opening,
    )
    .expect("client return state");

    assert_eq!(
        startup.begin_candidate_for_open(frozen_key, Some(successor)),
        None,
    );
    assert!(startup.observe_response_frontier(58_400));
    assert_eq!(
        startup.begin_unresolved_after_response_trigger(),
        vec![ReliableRelayReturnCandidate {
            key: frozen_key,
            path_instance_id: Some(frozen),
            ordinal: 1,
        }],
    );
}

#[tokio::test]
async fn terminal_plan_classifies_later_request_attachments_as_ordinary() {
    let stream_id = StreamId(401);
    let opening_key = relay_key(UnderlayProtocol::Tcp, 0);
    let pending_key = relay_key(UnderlayProtocol::Udp, 0);
    let (commands, _receivers) = reliable_path_command_channels(8);
    let opening = OpenedRemoteStream::pending(
        test_stream(
            stream_id,
            opening_key.underlay,
            opening_key.index,
            commands,
            TrafficClass::Throughput,
        ),
        opening_key.index,
    );
    let opening_instance = RelayPathInstance {
        key: opening_key,
        path_instance_id: opening.path_instance_id(),
        attachment_id: 0,
    };
    let mut startup = ClientReliableReturnPlan::from_initial_open(
        ReliableRelayOpenedStartup {
            plan: Arc::new(
                ReliableRelayReturnPlan::new(
                    58_400,
                    PathUsage::Available,
                    vec![
                        (opening_key, Some(opening_instance.path_instance_id)),
                        (pending_key, None),
                    ],
                )
                .expect("return plan"),
            ),
            opening_ordinal: 0,
            failed_ordinals: Vec::new(),
        },
        opening_instance,
    )
    .expect("client return state");

    startup.observe_response_terminal(1_000, 1_000);

    assert!(startup.is_done());
    assert_eq!(startup.begin_candidate_for_open(pending_key, None), None);
}

#[tokio::test]
async fn pending_slot_request_open_binds_startup_and_finalizes_before_h() {
    let (mut startup, remotes, _second) = accepted_two_candidate_return_plan(StreamId(41));
    let base = ReliableRelayOpenSpec::new(
        TargetAddr::Ip("127.0.0.1:9".parse().expect("target")),
        TrafficClass::Throughput,
    )
    .with_startup_plan(startup.plan().clone());
    let wire = base.for_startup_ordinal(1).return_plan();

    assert_eq!(wire.phase, StreamAttachmentPhase::Startup);
    assert_eq!(wire.candidate_ordinal, 1);
    assert_eq!(startup.prepare_final(&remotes), Some(&[0, 1][..]));
    assert!(
        !startup.observe_response_frontier(58_400),
        "an early immutable FINAL leaves no h-trigger work",
    );
    assert!(startup.begin_unresolved_after_response_trigger().is_empty());
}

#[tokio::test]
async fn recovery_open_on_a_frozen_pending_slot_keeps_its_startup_ordinal() {
    let context = ClientPathContext::new(
        [
            "tcp://127.0.0.1:11121?max-tcp-carriers=1",
            "quic://127.0.0.1:11122",
        ]
        .into_iter()
        .map(|path| path.parse::<PathSpec>().expect("test path"))
        .collect(),
        ClientSecurityConfig::for_test(
            SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
        ),
        ResourceLimits::default(),
    )
    .expect("client context");
    let stream_id = StreamId(47);
    let tcp = relay_key(UnderlayProtocol::Tcp, 0);
    let quic = relay_key(UnderlayProtocol::Udp, 0);
    let (commands, _receivers) = reliable_path_command_channels(8);
    let opening = OpenedRemoteStream::pending(
        test_stream(
            stream_id,
            tcp.underlay,
            tcp.index,
            commands,
            TrafficClass::Throughput,
        ),
        tcp.index,
    );
    let opening_path_instance_id = opening.path_instance_id();
    context.install_relay_path_instance_for_test(RelayPathInstance {
        key: tcp,
        path_instance_id: opening_path_instance_id,
        attachment_id: 0,
    });
    let plan = Arc::new(
        ReliableRelayReturnPlan::new(
            58_400,
            PathUsage::Available,
            vec![(tcp, Some(opening_path_instance_id)), (quic, None)],
        )
        .expect("return plan"),
    );
    let remotes = ReliableRelayRemoteSet::new(opening, 8);
    let mut startup = ClientReliableReturnPlan::from_initial_open(
        ReliableRelayOpenedStartup {
            plan: plan.clone(),
            opening_ordinal: 0,
            failed_ordinals: Vec::new(),
        },
        remotes.paths[0].instance(),
    )
    .expect("client return state");
    let spec = ReliableRelayOpenSpec::new(
        TargetAddr::Ip("127.0.0.1:9".parse().expect("target")),
        TrafficClass::Throughput,
    )
    .with_startup_plan(plan);
    let send_stream = ReliableSendStream::new(stream_id, context.mux_limits);
    let mut pending = HashMap::new();
    let (result_tx, _result_rx) = mpsc::channel(1);

    assert!(spawn_reliable_relay_recovery_path_open(
        &context,
        &spec,
        &mut startup,
        ReliableRelayPathLanes::same(TrafficClass::Throughput),
        &remotes,
        &send_stream,
        &ClientRelayPathOpenSuppressions::default(),
        &mut pending,
        &result_tx,
    ));
    let task = pending.get(&quic).expect("pending QUIC recovery open");
    assert_eq!(task.startup_ordinal, Some(1));
    assert_eq!(task.startup_expected_instance, None);
    cancel_pending_additional_path_opens(stream_id, &mut pending);
}

#[tokio::test]
async fn exact_h_opening_is_joined_and_delayed_fin_does_not_duplicate_it() {
    let stream_id = StreamId(42);
    let opening_key = relay_key(UnderlayProtocol::Tcp, 0);
    let pending_key = relay_key(UnderlayProtocol::Udp, 0);
    let (commands, _receivers) = reliable_path_command_channels(8);
    let opening = OpenedRemoteStream::pending(
        test_stream(
            stream_id,
            opening_key.underlay,
            opening_key.index,
            commands,
            TrafficClass::Throughput,
        ),
        opening_key.index,
    );
    let opening_path_instance_id = opening.path_instance_id();
    let plan = Arc::new(
        ReliableRelayReturnPlan::new(
            58_400,
            PathUsage::Available,
            vec![
                (opening_key, Some(opening_path_instance_id)),
                (pending_key, None),
            ],
        )
        .expect("return plan"),
    );
    let remotes = ReliableRelayRemoteSet::new(opening, 8);
    let opening_instance = remotes.paths[0].instance();
    let mut startup = ClientReliableReturnPlan::from_initial_open(
        ReliableRelayOpenedStartup {
            plan,
            opening_ordinal: 0,
            failed_ordinals: Vec::new(),
        },
        opening_instance,
    )
    .expect("client return state");

    assert!(!startup.observe_response_frontier(58_399));
    assert_eq!(startup.begin_candidate_for_open(pending_key, None), Some(1),);
    assert!(startup.observe_response_frontier(58_400));
    assert!(
        startup.begin_unresolved_after_response_trigger().is_empty(),
        "the h wake joins the already-opening ordinal",
    );
    startup.observe_response_terminal(58_400, 58_400);
    assert!(startup.is_done());
}

#[tokio::test]
async fn fin_before_missing_ghost_data_requires_final_settlement() {
    let stream_id = StreamId(48);
    let opening_key = relay_key(UnderlayProtocol::Tcp, 0);
    let opening_pending_key = relay_key(UnderlayProtocol::Udp, 0);
    let never_opened_key = relay_key(UnderlayProtocol::Tcp, 1);
    let (commands, _receivers) = reliable_path_command_channels(8);
    let opening = OpenedRemoteStream::pending(
        test_stream(
            stream_id,
            opening_key.underlay,
            opening_key.index,
            commands,
            TrafficClass::Throughput,
        ),
        opening_key.index,
    );
    let opening_path_instance_id = opening.path_instance_id();
    let plan = Arc::new(
        ReliableRelayReturnPlan::new(
            58_400,
            PathUsage::Available,
            vec![
                (opening_key, Some(opening_path_instance_id)),
                (opening_pending_key, None),
                (never_opened_key, None),
            ],
        )
        .expect("return plan"),
    );
    let remotes = ReliableRelayRemoteSet::new(opening, 8);
    let mut startup = ClientReliableReturnPlan::from_initial_open(
        ReliableRelayOpenedStartup {
            plan,
            opening_ordinal: 0,
            failed_ordinals: Vec::new(),
        },
        remotes.paths[0].instance(),
    )
    .expect("client return state");
    assert_eq!(
        startup.begin_candidate_for_open(opening_pending_key, None),
        Some(1),
    );

    startup.observe_response_terminal(1_000, 0);

    assert!(!startup.is_done());
    assert_eq!(
        startup.settlement(2),
        Some(ReliableReturnCandidateSettlement::Failed),
        "FIN forbids launching a never-opened slot",
    );
    assert!(startup.begin_unresolved_after_response_trigger().is_empty());
    assert_eq!(startup.prepare_final(&remotes), None);
    startup
        .settle_failed(1)
        .expect("lost early acceptance settles as omitted");
    assert_eq!(startup.prepare_final(&remotes), Some(&[0][..]));
}

#[tokio::test]
async fn final_linearization_omits_preexisting_detach_but_is_immutable_afterward() {
    let (mut before, mut before_remotes, before_second) =
        accepted_two_candidate_return_plan(StreamId(43));
    drop(
        before_remotes
            .remove_path_instance(before_second)
            .expect("detach before FINAL"),
    );
    assert_eq!(before.prepare_final(&before_remotes), Some(&[0][..]));

    let (mut after, mut after_remotes, after_second) =
        accepted_two_candidate_return_plan(StreamId(44));
    assert_eq!(after.prepare_final(&after_remotes), Some(&[0, 1][..]));
    drop(
        after_remotes
            .remove_path_instance(after_second)
            .expect("detach after FINAL"),
    );
    assert_eq!(
        after.prepare_final(&after_remotes),
        Some(&[0, 1][..]),
        "serialized FINAL retries keep the same receipt",
    );
}

#[tokio::test]
async fn final_can_publish_empty_after_all_startup_members_leave() {
    let (mut startup, mut remotes, second) = accepted_two_candidate_return_plan(StreamId(45));
    let first = remotes.paths[0].instance();
    drop(
        remotes
            .remove_path_instance(second)
            .expect("remove second startup member"),
    );
    drop(
        remotes
            .remove_path_instance(first)
            .expect("remove first startup member"),
    );
    let ordinary_key = relay_key(UnderlayProtocol::Tcp, 2);
    let (commands, _receivers) = reliable_path_command_channels(8);
    assert_eq!(
        remotes.attach_candidate(OpenedRemoteStream::pending(
            test_stream(
                StreamId(45),
                ordinary_key.underlay,
                ordinary_key.index,
                commands,
                TrafficClass::Throughput,
            ),
            ordinary_key.index,
        )),
        ReliableRelayAttachOutcome::Attached,
    );

    assert_eq!(startup.prepare_final(&remotes), Some(&[][..]));
    assert!(
        remotes.publish_return_plan_final(&[]).is_ok(),
        "an empty client-commit receipt is a valid FINAL",
    );
}

#[tokio::test]
async fn immutable_final_retries_once_per_attachment_membership_wave() {
    let stream_id = StreamId(46);
    let first_key = relay_key(UnderlayProtocol::Tcp, 0);
    let (first_commands, mut first_receivers) = reliable_path_command_channels(8);
    let first = OpenedRemoteStream::pending(
        test_stream(
            stream_id,
            first_key.underlay,
            first_key.index,
            first_commands,
            TrafficClass::Throughput,
        ),
        first_key.index,
    );
    let mut remotes = ReliableRelayRemoteSet::new(first, 8);

    assert!(
        remotes
            .publish_return_plan_final(&[0])
            .expect("publish FINAL"),
    );
    expect_return_plan_final(&mut first_receivers, stream_id, &[0]).await;
    assert!(
        !remotes
            .publish_return_plan_final(&[0])
            .expect("idempotent FINAL retry"),
    );

    let second_key = relay_key(UnderlayProtocol::Udp, 0);
    let (second_commands, mut second_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        remotes.attach_candidate(OpenedRemoteStream::pending(
            test_stream(
                stream_id,
                second_key.underlay,
                second_key.index,
                second_commands,
                TrafficClass::Throughput,
            ),
            second_key.index,
        )),
        ReliableRelayAttachOutcome::Attached,
    );
    assert!(remotes.has_pending_return_plan_final_publication());
    assert!(remotes.retry_pending_return_plan_final());
    expect_return_plan_final(&mut second_receivers, stream_id, &[0]).await;
    assert!(
        try_recv_reliable_path_command(&mut first_receivers).is_none(),
        "an attachment that published this immutable FINAL is not duplicated",
    );
}

#[test]
fn live_demand_class_change_updates_lane_accounting() {
    assert!(reliable_relay_lane_changed(
        TrafficClass::Latency,
        TrafficClass::Throughput,
    ));
    assert!(reliable_relay_lane_changed(
        TrafficClass::Throughput,
        TrafficClass::Latency,
    ));
    assert!(!reliable_relay_lane_changed(
        TrafficClass::Throughput,
        TrafficClass::Throughput,
    ));
}

#[test]
fn pending_fin_waits_for_the_ordered_sender_queue() {
    assert!(reliable_relay_can_send_pending_fin(true, true));
    assert!(!reliable_relay_can_send_pending_fin(true, false));
    assert!(!reliable_relay_can_send_pending_fin(false, true));
}

#[test]
fn bulk_path_opens_do_not_serialize_tcp_and_quic() {
    let candidates = vec![
        RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 1,
        },
        RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        },
        RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 1,
        },
    ];

    assert_eq!(
        reliable_relay_available_path_open_candidates(candidates.clone(), &HashMap::new()),
        candidates,
    );
}

#[test]
fn bulk_path_open_does_not_bypass_stream_local_failure_suppression() {
    let context = ClientPathContext::new(
        vec![
            "quic://127.0.0.1:10908"
                .parse::<PathSpec>()
                .expect("test path"),
        ],
        ClientSecurityConfig::for_test(
            SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
        ),
        ResourceLimits::default(),
    )
    .expect("client context");
    let failed = RelayPathKey {
        underlay: UnderlayProtocol::Udp,
        index: 0,
    };
    let failed_instance = RelayPathInstance {
        key: failed,
        path_instance_id: next_carrier_path_instance_id(),
        attachment_id: 1,
    };
    context.install_relay_path_instance_for_test(failed_instance);
    let mut suppressions = ClientRelayPathOpenSuppressions::default();
    suppressions.suppress(
        failed_instance,
        tokio::time::Instant::now() + Duration::from_secs(60),
    );
    let candidates = reliable_relay_bulk_path_open_candidates(
        &context,
        vec![failed],
        &suppressions,
        &HashMap::new(),
    );

    assert!(
        candidates.is_empty(),
        "bulk expansion must consult the same logical-stream retry bound as recovery"
    );
}

#[test]
fn scheduled_sender_retry_blocks_immediate_redispatch() {
    assert!(reliable_relay_queued_send_blocked_for_retry(
        false,
        Some(tokio::time::Instant::now()),
    ));
    assert!(!reliable_relay_queued_send_blocked_for_retry(
        true,
        Some(tokio::time::Instant::now()),
    ));
    assert!(!reliable_relay_queued_send_blocked_for_retry(false, None));
}

#[test]
fn expected_response_arms_stall_watch_independent_of_demand_class() {
    let send_stream = ReliableSendStream::new(StreamId(1), MuxLimits::default());
    let recv_stream = ReliableRecvStream::new(StreamId(1), MuxLimits::default());

    for lane in [TrafficClass::Latency, TrafficClass::Throughput] {
        assert!(reliable_relay_stall_watch_active(
            &send_stream,
            &recv_stream,
            true,
            lane,
            true,
            MuxLimits::default(),
        ));
    }
}

#[test]
fn response_stall_anchor_ignores_unrelated_request_progress() {
    let recv_stream = ReliableRecvStream::new(StreamId(1), MuxLimits::default());
    let started = Instant::now();
    let last_delivery = started;
    let last_reinjection = started + Duration::from_millis(100);
    let later_request_progress = started + Duration::from_secs(5);

    assert_eq!(
        reliable_relay_stall_progress_anchor(
            later_request_progress,
            last_delivery,
            last_reinjection,
            &recv_stream,
            true,
            TrafficClass::Latency,
            true,
            MuxLimits::default(),
        ),
        last_reinjection,
    );
}

#[test]
fn repeated_stall_attempts_use_a_future_bounded_deadline() {
    let started = Instant::now();
    let pto = transport_pto_from_snapshot(None);
    assert_eq!(
        reliable_relay_product_stall_deadline(started, None, None),
        tokio::time::Instant::from_std(started + pto),
    );

    let last_attempt = started + pto;
    assert_eq!(
        reliable_relay_product_stall_deadline(started, Some(last_attempt), None),
        tokio::time::Instant::from_std(
            last_attempt + pto.saturating_mul(QUIC_PERSISTENT_CONGESTION_THRESHOLD),
        ),
    );

    let later_progress = last_attempt + Duration::from_secs(1);
    assert_eq!(
        reliable_relay_product_stall_deadline(later_progress, Some(last_attempt), None),
        tokio::time::Instant::from_std(later_progress + pto),
    );
}

#[tokio::test]
async fn product_stall_preserves_an_existing_multipath_attachment_set() {
    let (tcp_commands, _tcp_receivers) = reliable_path_command_channels(1);
    let (udp_commands, _udp_receivers) = reliable_path_command_channels(1);
    let first = OpenedRemoteStream::pending(
        test_stream(
            StreamId(1),
            UnderlayProtocol::Tcp,
            0,
            tcp_commands,
            TrafficClass::Latency,
        ),
        0,
    );
    let second = OpenedRemoteStream::pending(
        test_stream(
            StreamId(1),
            UnderlayProtocol::Udp,
            0,
            udp_commands,
            TrafficClass::Latency,
        ),
        0,
    );
    let mut remotes = ReliableRelayRemoteSet::new(first, 4);
    assert_eq!(
        remotes.attach_candidate(second),
        ReliableRelayAttachOutcome::Attached
    );
    let membership = remotes.path_instances();

    assert!(reliable_relay_product_stall_preserves_attached_path_set(
        &remotes
    ));
    assert!(!reliable_relay_product_stall_should_try_alternate_attach(
        &remotes
    ));
    assert!(
        reliable_relay_should_open_recovery_path(&remotes),
        "persistent stream-local recovery may add one unattached configured carrier"
    );
    assert_eq!(remotes.path_instances(), membership);
}

#[tokio::test]
async fn product_stall_on_a_sole_carrier_requests_an_alternative() {
    let (commands, _receivers) = reliable_path_command_channels(1);
    let opened = OpenedRemoteStream::pending(
        test_stream(
            StreamId(1),
            UnderlayProtocol::Tcp,
            0,
            commands,
            TrafficClass::Latency,
        ),
        0,
    );
    let remotes = ReliableRelayRemoteSet::new(opened, 4);

    assert!(reliable_relay_product_stall_should_try_alternate_attach(
        &remotes
    ));
    assert!(reliable_relay_should_open_recovery_path(&remotes));
    assert!(!reliable_relay_product_stall_preserves_attached_path_set(
        &remotes
    ));
}

#[tokio::test]
async fn recovery_open_adds_one_unattached_path_to_an_existing_set() {
    let security = ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    );
    let context = ClientPathContext::new(
        [
            "tcp://127.0.0.1:11171",
            "quic://127.0.0.1:11172?initial-srtt-s=0.18&initial-rate-mbps=500",
            "tcp://127.0.0.1:11173?initial-srtt-s=0.04&initial-rate-mbps=500",
        ]
        .into_iter()
        .map(|path| path.parse::<PathSpec>().expect("test path"))
        .collect(),
        security,
        ResourceLimits::default(),
    )
    .expect("client context");
    let (commands, _receivers) = reliable_path_command_channels(1);
    let opened = OpenedRemoteStream::pending(
        test_stream(
            StreamId(2),
            UnderlayProtocol::Tcp,
            0,
            commands,
            TrafficClass::Latency,
        ),
        0,
    );
    let mut remotes = ReliableRelayRemoteSet::new(opened, 4);
    let (second_commands, _second_receivers) = reliable_path_command_channels(1);
    assert_eq!(
        remotes.attach_candidate(OpenedRemoteStream::pending(
            test_stream(
                StreamId(2),
                UnderlayProtocol::Udp,
                0,
                second_commands,
                TrafficClass::Latency,
            ),
            0,
        )),
        ReliableRelayAttachOutcome::Attached
    );
    assert_eq!(remotes.accepted_path_count(), 2);
    let mut startup = singleton_return_plan(&remotes);
    let mut send_stream = ReliableSendStream::new(StreamId(2), context.mux_limits);
    let spec = ReliableRelayOpenSpec::new(
        TargetAddr::Ip("127.0.0.1:9".parse().expect("test target")),
        TrafficClass::Latency,
    );
    let mut pending = HashMap::new();
    let (result_tx, _result_rx) = mpsc::channel(1);

    assert!(spawn_reliable_relay_recovery_path_open(
        &context,
        &spec,
        &mut startup,
        ReliableRelayPathLanes::same(TrafficClass::Latency),
        &remotes,
        &send_stream,
        &ClientRelayPathOpenSuppressions::default(),
        &mut pending,
        &result_tx,
    ));
    assert_eq!(pending.len(), 1);
    assert!(pending.contains_key(&RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 1,
    }));
    cancel_pending_additional_path_opens(StreamId(2), &mut pending);

    let third = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 1,
    };
    let (third_commands, _third_receivers) = reliable_path_command_channels(1);
    let mut last_stream_progress_at = Instant::now();
    assert!(matches!(
        handle_additional_path_open_result(
            StreamId(2),
            &mut remotes,
            &mut send_stream,
            false,
            TrafficClass::Throughput,
            RelayAdditionalPathOpenResult {
                key: third,
                generation: next_relay_additional_path_open_generation(),
                mode: ReliableRelayAttachMode::Recovery,
                startup_ordinal: None,
                startup_expected_instance: None,
                result: Ok(OpenedRemoteStream::pending(
                    test_stream(
                        StreamId(2),
                        UnderlayProtocol::Tcp,
                        1,
                        third_commands,
                        TrafficClass::Latency,
                    ),
                    1,
                )),
            },
            0,
            &mut last_stream_progress_at,
        ),
        Some(ReliableRelayAttachMode::Recovery)
    ));
    assert_eq!(remotes.accepted_path_count(), 3);
    assert!(
        remotes
            .paths
            .iter()
            .all(|path| path.stream.lane == TrafficClass::Throughput),
        "a completed open inherits current request demand, not its captured lane"
    );
}

#[test]
fn asynchronous_recovery_open_selects_one_non_suppressed_path() {
    let tcp0 = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    let tcp1 = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 1,
    };
    let udp0 = RelayPathKey {
        underlay: UnderlayProtocol::Udp,
        index: 0,
    };
    let pending = HashMap::new();
    let context = ClientPathContext::new(
        [
            "tcp://127.0.0.1:10909",
            "tcp://127.0.0.1:10910",
            "quic://127.0.0.1:10911",
        ]
        .into_iter()
        .map(|path| path.parse::<PathSpec>().expect("test path"))
        .collect(),
        ClientSecurityConfig::for_test(
            SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
        ),
        ResourceLimits::default(),
    )
    .expect("client context");
    let tcp0_instance = RelayPathInstance {
        key: tcp0,
        path_instance_id: next_carrier_path_instance_id(),
        attachment_id: 1,
    };
    context.install_relay_path_instance_for_test(tcp0_instance);
    let mut suppressions = ClientRelayPathOpenSuppressions::default();
    suppressions.suppress(
        tcp0_instance,
        tokio::time::Instant::now() + Duration::from_secs(60),
    );

    assert_eq!(
        reliable_relay_recovery_path_open_candidates(
            &context,
            vec![tcp0, tcp1, udp0],
            &ClientRelayPathOpenSuppressions::default(),
            &pending,
        ),
        vec![tcp0],
    );
    assert_eq!(
        reliable_relay_recovery_path_open_candidates(
            &context,
            vec![tcp0, tcp1, udp0],
            &suppressions,
            &pending,
        ),
        vec![tcp1],
    );
}

#[tokio::test(start_paused = true)]
async fn disconnected_path_open_waits_for_exact_suppression_deadline() {
    let context = ClientPathContext::new(
        vec![
            "quic://127.0.0.1:10912"
                .parse::<PathSpec>()
                .expect("test path"),
        ],
        ClientSecurityConfig::for_test(
            SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
        ),
        ResourceLimits::default(),
    )
    .expect("client context");
    let stream_id = StreamId(908);
    let (commands, _receivers) = reliable_path_command_channels(1);
    let opened = OpenedRemoteStream::pending(
        test_stream(
            stream_id,
            UnderlayProtocol::Udp,
            0,
            commands,
            TrafficClass::Latency,
        ),
        0,
    );
    let mut remotes = ReliableRelayRemoteSet::new(opened, 4);
    let mut startup = singleton_return_plan(&remotes);
    let failed = remotes.paths[0].instance();
    context.install_relay_path_instance_for_test(failed);
    drop(
        remotes
            .remove_path_instance(failed)
            .expect("remove failed attachment"),
    );
    let retry_delay = Duration::from_secs(1);
    let mut suppressions = ClientRelayPathOpenSuppressions::default();
    suppressions.suppress(failed, tokio::time::Instant::now() + retry_delay);
    let spec = ReliableRelayOpenSpec::new(
        TargetAddr::Ip("127.0.0.1:9".parse().expect("test target")),
        TrafficClass::Latency,
    );
    let send_stream = ReliableSendStream::new(stream_id, context.mux_limits);
    let mut attempted = HashSet::new();
    let mut pending = HashMap::new();
    let (result_tx, _result_rx) = mpsc::channel(1);

    assert!(!spawn_reliable_relay_disconnected_path_open(
        &context,
        &spec,
        &mut startup,
        ReliableRelayPathLanes::same(TrafficClass::Latency),
        &remotes,
        &send_stream,
        &suppressions,
        &mut attempted,
        &mut pending,
        &result_tx,
    ));
    tokio::time::advance(retry_delay).await;
    assert!(spawn_reliable_relay_disconnected_path_open(
        &context,
        &spec,
        &mut startup,
        ReliableRelayPathLanes::same(TrafficClass::Latency),
        &remotes,
        &send_stream,
        &suppressions,
        &mut attempted,
        &mut pending,
        &result_tx,
    ));
    cancel_pending_additional_path_opens(stream_id, &mut pending);
}

#[tokio::test]
async fn stale_additional_path_result_cannot_consume_replacement_open() {
    let key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    let stale_generation = next_relay_additional_path_open_generation();
    let current_generation = next_relay_additional_path_open_generation();
    let mut pending = HashMap::from([(
        key,
        RelayAdditionalPathOpenTask {
            generation: current_generation,
            startup_ordinal: None,
            startup_expected_instance: None,
            #[cfg(feature = "lab-diagnostics")]
            lane: TrafficClass::Throughput,
            handle: tokio::spawn(std::future::pending()),
        },
    )]);

    assert!(
        take_matching_additional_path_open(&mut pending, key, stale_generation).is_none(),
        "a delayed result must not consume a newer open for the same path"
    );
    assert_eq!(pending.len(), 1);

    take_matching_additional_path_open(&mut pending, key, current_generation)
        .expect("exact open generation")
        .handle
        .abort();
    assert!(pending.is_empty());
}

#[tokio::test(start_paused = true)]
async fn completed_open_cleanup_cannot_hold_actor_past_accepted_copy_deadline() {
    let stream_id = StreamId(909);
    let key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 1,
    };
    let stale_generation = next_relay_additional_path_open_generation();
    let current_generation = next_relay_additional_path_open_generation();
    let (initial_commands, _initial_receivers) = reliable_path_command_channels(1);
    let initial = OpenedRemoteStream::pending(
        test_stream(
            stream_id,
            UnderlayProtocol::Tcp,
            0,
            initial_commands,
            TrafficClass::Throughput,
        ),
        0,
    );
    let mut remotes = ReliableRelayRemoteSet::new(initial, 4);
    let mut startup = singleton_return_plan(&remotes);
    let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
    let mut last_stream_progress_at = Instant::now();
    let mut pending = HashMap::from([(
        key,
        RelayAdditionalPathOpenTask {
            generation: current_generation,
            startup_ordinal: None,
            startup_expected_instance: None,
            #[cfg(feature = "lab-diagnostics")]
            lane: TrafficClass::Throughput,
            handle: tokio::spawn(std::future::pending()),
        },
    )]);
    let (stale_commands, mut stale_receivers) = reliable_path_command_channels(1);
    stale_commands
        .send_control(ReliablePathCommand::CloseStream(StreamId(999)))
        .await
        .expect("fill bounded control queue");
    let stale = OpenedRemoteStream::pending(
        test_stream(
            stream_id,
            UnderlayProtocol::Tcp,
            key.index,
            stale_commands,
            TrafficClass::Throughput,
        ),
        key.index,
    );
    let (result_tx, mut result_rx) = mpsc::channel(1);
    result_tx
        .send(RelayAdditionalPathOpenResult {
            key,
            generation: stale_generation,
            mode: ReliableRelayAttachMode::Recovery,
            startup_ordinal: None,
            startup_expected_instance: None,
            result: Ok(stale),
        })
        .await
        .expect("publish stale completed open");
    let accepted_copy_deadline = tokio::time::Instant::now() + Duration::from_millis(10);

    assert!(!drain_completed_additional_path_opens(
        stream_id,
        &mut startup,
        &mut remotes,
        &mut send_stream,
        false,
        TrafficClass::Throughput,
        &mut pending,
        &mut result_rx,
        &mut last_stream_progress_at,
    ));
    assert_eq!(
        pending.get(&key).map(|task| task.generation),
        Some(current_generation),
        "a stale completion must not consume the replacement open",
    );

    tokio::time::advance(Duration::from_millis(10)).await;
    assert!(accepted_copy_wake_is_due(
        Some(accepted_copy_deadline.into_std()),
        tokio::time::Instant::now().into_std(),
    ));

    assert!(matches!(
        recv_reliable_path_command(&mut stale_receivers).await,
        Some(ReliablePathCommand::SendFrame(Frame::StreamDetach {
            stream_id: retired,
        })) if retired == stream_id
    ));
    assert!(matches!(
        recv_reliable_path_command(&mut stale_receivers).await,
        Some(ReliablePathCommand::CloseStream(retired)) if retired == stream_id
    ));
    cancel_pending_additional_path_opens(stream_id, &mut pending);
}

#[tokio::test]
async fn additional_attachment_timeout_preserves_live_carrier_health_and_work_accounting() {
    let security = ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    );
    let context = ClientPathContext::new(
        ["tcp://127.0.0.1:11173", "tcp://127.0.0.1:11174"]
            .into_iter()
            .map(|path| path.parse::<PathSpec>().expect("test path"))
            .collect(),
        security,
        ResourceLimits::default(),
    )
    .expect("client context");
    let candidate = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 1,
    };
    let candidate_instance = next_carrier_path_instance_id();
    let before = {
        let mut health = context.health().lock().expect("path health");
        let record = health
            .tcp_record_mut(candidate.index)
            .expect("candidate record");
        record.install_peer_usage(candidate_instance, 0, PathUsage::Available);
        record.relay_bytes_in_flight = 65_536;
        record.relay_queue_bytes = 131_072;
        record.product_delivery_rate_bps = Some(250_000_000.0);
        record.product_delivery_sample_bytes = 1_048_576;
        record.path_proof_success = true;
        (
            record.eligibility_fingerprint(),
            record.consecutive_failures,
            record.failed_until,
            record.active_flows,
            record.relay_bytes_in_flight,
            record.relay_queue_bytes,
            record.product_delivery_rate_bps,
            record.product_delivery_sample_bytes,
            record.path_proof_success,
        )
    };

    let stream_id = StreamId(5);
    let (commands, _receivers) = reliable_path_command_channels(1);
    let opened = OpenedRemoteStream::pending(
        test_stream(
            stream_id,
            UnderlayProtocol::Tcp,
            0,
            commands,
            TrafficClass::Throughput,
        ),
        0,
    );
    let mut remotes = ReliableRelayRemoteSet::new(opened, 4);
    let membership = remotes.path_instances();
    let mut send_stream = ReliableSendStream::new(stream_id, context.mux_limits);
    let mut last_stream_progress_at = Instant::now();

    assert!(
        handle_additional_path_open_result(
            stream_id,
            &mut remotes,
            &mut send_stream,
            false,
            TrafficClass::Throughput,
            RelayAdditionalPathOpenResult {
                key: candidate,
                generation: next_relay_additional_path_open_generation(),
                mode: ReliableRelayAttachMode::BulkStriping,
                startup_ordinal: None,
                startup_expected_instance: None,
                result: Err(RuntimeError::PathOpenTimedOut),
            },
            0,
            &mut last_stream_progress_at,
        )
        .is_none()
    );
    assert_eq!(remotes.path_instances(), membership);

    let after = {
        let health = context.health().lock().expect("path health");
        let record = health
            .tcp_record(candidate.index)
            .expect("candidate record");
        (
            record.eligibility_fingerprint(),
            record.consecutive_failures,
            record.failed_until,
            record.active_flows,
            record.relay_bytes_in_flight,
            record.relay_queue_bytes,
            record.product_delivery_rate_bps,
            record.product_delivery_sample_bytes,
            record.path_proof_success,
        )
    };
    assert_eq!(after, before);
}

#[test]
fn receive_hole_reinjection_requires_live_remote_and_buffered_gap() {
    let mut recv_stream = ReliableRecvStream::new(StreamId(4), MuxLimits::default());
    recv_stream
        .receive_data(0, bytes::Bytes::from_static(b"prefix"))
        .expect("contiguous prefix");
    recv_stream
        .receive_data(1024, bytes::Bytes::from_static(b"gap"))
        .expect("out-of-order data");

    assert!(reliable_relay_receive_hole_reinjection_active(
        &recv_stream,
        true
    ));
    assert!(!reliable_relay_receive_hole_reinjection_active(
        &recv_stream,
        false
    ));
}
