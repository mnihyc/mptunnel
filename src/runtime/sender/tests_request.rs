use super::test_support::*;
use super::*;
use crate::config::ResourceLimits;
use crate::model::admission::ReliableDataAckFrontierState;
use crate::model::capacity::PathRateSample;
use crate::model::capacity::{reliable_product_feedback_window_bytes, reliable_relay_buffer_len};
use crate::model::timing::reliable_relay_tail_reinjection_delay;
use crate::model::work::{
    ReliableReinjectionTargetWork, ReliableWorkClass,
    reliable_critical_tail_reinjection_limit_bytes, reliable_reinjection_service_limit_bytes,
};
use crate::protocol::frame::reliable_stream_frame_extent;
use crate::protocol::{PathId, SessionId};
use crate::runtime::path::commands::{
    ReliablePathCommand, recv_reliable_path_command, reliable_path_command_channels,
    reliable_path_command_pending_bytes, try_recv_reliable_path_command,
    try_recv_reliable_path_priority_command,
};
use crate::runtime::sender::response::ServerResponseSenderService;
use crate::runtime::stream::response::ResponseStreamBinding;
use crate::runtime::stream::{
    FixedReliablePathOutput, OpenedRemoteStream, ReliablePathStream, ReliableRelayAttachOutcome,
    ReliableRelayRemotePath,
};
use crate::transport::PathSpec;
use std::sync::Arc;

#[test]
fn response_critical_reinjection_preempts_original_data_and_is_exactly_accounted() {
    let mux_limits = MuxLimits::default();
    let stream_id = StreamId(79);
    let mut sender = ServerResponseSenderService::new_with_performance(
        SessionId(79),
        stream_id,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 1,
        },
    );
    let startup_floor = sender_optional_reinjection_startup_floor_bytes(mux_limits);

    sender.enqueue_data_for_lane(Bytes::from_static(b"owner"), TrafficClass::Throughput);
    let accounted_before = sender.optional_reinjection.reinjected_bytes();
    sender.enqueue_reinjection_frame_with_priority(
        Frame::StreamData {
            stream_id,
            offset: 0,
            payload: Bytes::from(vec![0x7a; startup_floor]),
        },
        true,
    );

    assert_eq!(
        sender.queue.front().map(|(lane, _)| lane),
        Some(ReliableWorkClass::Reinjection)
    );
    assert_eq!(
        sender.optional_reinjection.reinjected_bytes(),
        accounted_before + startup_floor as u64,
    );
}

fn request_handle(output: ReliablePathStreamOutput) -> ReliablePathStreamHandle {
    ReliablePathStreamHandle {
        stream_id: StreamId(7),
        max_offset: 64 * 1024,
        lane: TrafficClass::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: 16 * 1024,
        output,
    }
}

fn opened_request_stream_with_retained_input(
    stream_id: StreamId,
    path_index: usize,
    commands: crate::runtime::path::commands::ReliablePathCommandSender,
) -> (
    OpenedRemoteStream,
    tokio::sync::mpsc::Sender<Result<Frame, RuntimeError>>,
) {
    let (frames_tx, frames_rx) = tokio::sync::mpsc::channel(1);
    let limits = MuxLimits::default();
    (
        OpenedRemoteStream::pending(
            ReliablePathStream {
                stream_id,
                max_offset: limits.max_stream_window_bytes,
                lane: TrafficClass::Throughput,
                underlay: UnderlayProtocol::Tcp,
                max_frame_payload_bytes: reliable_relay_buffer_len(limits),
                output: ReliablePathStreamOutput::fixed(
                    UnderlayProtocol::Tcp,
                    PathId(path_index as u16),
                    commands,
                    limits,
                ),
                frames: frames_rx.into(),
            },
            path_index,
        ),
        frames_tx,
    )
}

#[test]
fn request_dispatch_preserves_classified_and_stream_ordered_queues() {
    let (commands, mut receivers) = reliable_path_command_channels(1);
    let fixed = FixedReliablePathOutput::new(
        UnderlayProtocol::Tcp,
        PathId(0),
        commands,
        MuxLimits::default(),
    );
    let handle = request_handle(ReliablePathStreamOutput::Fixed(fixed));

    emit_request_frame_with_mode(
        &handle,
        Frame::Ping { nonce: 1 },
        TrafficClass::Control,
        CarrierEmitMode::Classified,
        false,
    )
    .expect("classified control uses the priority queue");
    assert!(matches!(
        emit_request_frame_with_mode(
            &handle,
            Frame::Ping { nonce: 2 },
            TrafficClass::Control,
            CarrierEmitMode::Classified,
            false,
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    emit_request_frame_with_mode(
        &handle,
        Frame::Ping { nonce: 3 },
        TrafficClass::Control,
        CarrierEmitMode::StreamOrdered,
        false,
    )
    .expect("stream-ordered control uses the data queue");

    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::Ping { nonce: 1 }))
    ));
    assert!(matches!(
        try_recv_reliable_path_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::Ping { nonce: 3 }))
    ));
}

#[test]
fn request_dispatch_rejects_switchable_response_output() {
    let (commands, _receivers) = reliable_path_command_channels(1);
    let binding = ResponseStreamBinding::new(
        SessionId(9),
        UnderlayProtocol::Tcp,
        PathId(0),
        commands,
        TrafficClass::Throughput,
    );
    let handle = request_handle(ReliablePathStreamOutput::Switchable(binding));

    assert!(matches!(
        emit_request_frame_with_mode(
            &handle,
            Frame::Ping { nonce: 1 },
            TrafficClass::Control,
            CarrierEmitMode::Classified,
            false,
        ),
        Err(RuntimeError::Protocol("request relay path is not fixed"))
    ));
}

#[tokio::test]
async fn request_planner_and_reservation_preserve_closed_admission_identity() {
    let stream_id = StreamId(711);
    let context = client_test_context_with_paths(&["tcp://127.0.0.1:10711"]);
    let (commands, mut receivers) = reliable_path_command_channels(1);
    let (opened, _frames_tx) =
        opened_request_stream_with_retained_input(stream_id, 0, commands.clone());
    let mut remotes = ReliableRelayRemoteSet::new(opened, 4);
    consume_client_path_proof_for_test(&mut receivers);
    let instance = remotes.paths[0].instance();
    context.install_relay_path_instance_for_test(instance);
    let frame = Frame::StreamData {
        stream_id,
        offset: 0,
        payload: Bytes::from_static(b"payload"),
    };
    let mut controller = RequestMultipathController::new(stream_id);

    let initial_plan = controller
        .plan_relay_path_send(
            &context,
            &mut remotes,
            &frame,
            TrafficClass::Throughput,
            RelaySendCause::StreamData,
            &[],
        )
        .expect("active attachment is initially selectable");
    assert_eq!(initial_plan.target().1, instance);
    // Installing the exact physical health owner advances the proof epoch.
    // C9 preparation refreshes that priority proof, so consume it before the
    // test deliberately fills and later drains the independent data lane.
    consume_client_path_proof_for_test(&mut receivers);
    commands
        .try_enqueue_admitted_frame(frame.clone(), TrafficClass::Throughput)
        .expect("fill the active data command queue");
    assert!(matches!(
        controller.plan_relay_path_send(
            &context,
            &mut remotes,
            &frame,
            TrafficClass::Throughput,
            RelaySendCause::StreamData,
            &[],
        ),
        Err(RequestMultipathPlanError::ServiceBlocked)
    ));
    let busy = recv_reliable_path_command(&mut receivers)
        .await
        .expect("release the active queue slot");
    receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&busy));

    let selected_before_close = controller
        .plan_relay_path_send(
            &context,
            &mut remotes,
            &frame,
            TrafficClass::Throughput,
            RelaySendCause::StreamData,
            &[],
        )
        .expect("selection precedes the synchronous admission-close race");
    let (generation, selected) = selected_before_close.target();
    assert_eq!(selected, instance);
    commands.begin_path_drain();
    receivers.close_for_path_drain();
    let position = remotes
        .path_position_at_generation(generation, selected)
        .expect("exact selected membership remains registered");
    let selected_commands = fixed_request_output_commands(&remotes.paths[position].stream.output)
        .expect("client output is fixed");
    assert!(matches!(
        reserve_request_frame_with_mode(
            selected_commands,
            frame.clone(),
            TrafficClass::Throughput,
            CarrierEmitMode::Classified,
            false,
        ),
        Err(RequestFrameAdmissionError::OrderedTerminalPending)
    ));
    assert!(remotes.contains_path_instance(instance));
    assert!(receivers.finish_planned_path_retirement());
}

#[tokio::test]
async fn request_all_full_writers_finish_one_finite_production_pass_and_park() {
    let stream_id = StreamId(714);
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10714?initial-srtt-s=0.02&initial-rate-mbps=100",
        "tcp://127.0.0.1:10715?initial-srtt-s=0.03&initial-rate-mbps=100",
        "tcp://127.0.0.1:10716?initial-srtt-s=0.04&initial-rate-mbps=100",
    ]);
    let (first_commands, mut first_receivers) = reliable_path_command_channels(1);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream(stream_id, 0, first_commands.clone()),
        4,
    );
    let (second_commands, mut second_receivers) = reliable_path_command_channels(1);
    remotes.attach_candidate(opened_test_relay_stream(
        stream_id,
        1,
        second_commands.clone(),
    ));
    let (third_commands, mut third_receivers) = reliable_path_command_channels(1);
    remotes.attach_candidate(opened_test_relay_stream(
        stream_id,
        2,
        third_commands.clone(),
    ));

    for receivers in [
        &mut first_receivers,
        &mut second_receivers,
        &mut third_receivers,
    ] {
        consume_client_path_proof_for_test(receivers);
    }
    for instance in remotes.path_instances() {
        seed_client_bulk_evidence_for_test(&context, instance);
    }
    remotes.retry_pending_path_proofs(&context);
    for receivers in [
        &mut first_receivers,
        &mut second_receivers,
        &mut third_receivers,
    ] {
        consume_client_path_proof_for_test(receivers);
    }

    let filler_stream = StreamId(1714);
    for (ordinal, commands) in [&first_commands, &second_commands, &third_commands]
        .into_iter()
        .enumerate()
    {
        commands
            .try_enqueue_admitted_frame(
                Frame::StreamData {
                    stream_id: filler_stream,
                    offset: (ordinal * 4096) as u64,
                    payload: Bytes::from(vec![0x31 + ordinal as u8; 4096]),
                },
                TrafficClass::Throughput,
            )
            .expect("fill each exact data writer once");
    }

    let pending = Frame::StreamData {
        stream_id,
        offset: 0,
        payload: Bytes::from_static(b"pending Product quantum"),
    };
    let mut sender = RequestSenderService::new(stream_id);
    let outcome = tokio::time::timeout(
        Duration::from_millis(250),
        sender.send_frame(
            &context,
            &mut remotes,
            pending,
            RelaySendCause::StreamData,
            Some(TrafficClass::Throughput),
        ),
    )
    .await
    .expect("the finite candidate pass must park without spinning");
    assert!(matches!(outcome, Err(RuntimeError::SenderServiceBlocked)));
    assert!(
        sender
            .multipath
            .request_recovery_original_paths(&remotes)
            .is_empty(),
        "a zero-commit pass cannot publish Product ownership",
    );

    for (ordinal, receivers) in [
        &mut first_receivers,
        &mut second_receivers,
        &mut third_receivers,
    ]
    .into_iter()
    .enumerate()
    {
        let command = try_recv_reliable_path_command(receivers)
            .expect("the one preexisting command remains on each exact writer");
        assert!(matches!(
            command,
            ReliablePathCommand::SendFrame(Frame::StreamData {
                stream_id: queued_stream,
                offset,
                ..
            }) if queued_stream == filler_stream && offset == (ordinal * 4096) as u64
        ));
        assert!(
            try_recv_reliable_path_command(receivers).is_none(),
            "the finite pass cannot enqueue or retry an exact writer twice",
        );
    }
}

#[tokio::test]
async fn bound_recovery_waits_for_registered_terminal_then_cancels_when_absent() {
    let stream_id = StreamId(712);
    let context =
        client_test_context_with_paths(&["tcp://127.0.0.1:10712", "tcp://127.0.0.1:10713"]);
    let (target_commands, mut target_receivers) = reliable_path_command_channels(4);
    let (target_opened, _target_frames_tx) =
        opened_request_stream_with_retained_input(stream_id, 0, target_commands.clone());
    let mut remotes = ReliableRelayRemoteSet::new(target_opened, 4);
    let (owner_commands, mut owner_receivers) = reliable_path_command_channels(4);
    let (owner_opened, _owner_frames_tx) =
        opened_request_stream_with_retained_input(stream_id, 1, owner_commands);
    remotes.attach_candidate(owner_opened);
    consume_client_path_proof_for_test(&mut target_receivers);
    consume_client_path_proof_for_test(&mut owner_receivers);
    let target = remotes
        .path_instance_for_key(RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        })
        .expect("bound recovery target");
    let owner = remotes
        .path_instance_for_key(RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 1,
        })
        .expect("distinct retained OriginalData owner");
    context.install_relay_path_instance_for_test(target);
    context.install_relay_path_instance_for_test(owner);
    let snapshot = context
        .reliable_path_snapshot_for_instance(target)
        .expect("installed exact path evidence");
    let cause = RelaySendCause::persistent_client_ack_gap_reinjection(
        ClientReinjectionOutputIdentity { instance: target },
        snapshot,
    );
    let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
    let frame = send_stream
        .send_data(Bytes::from_static(b"repair"))
        .expect("retained OriginalData debt");
    let mut sender = RequestSenderService::new(stream_id);
    sender.record_original_frame_for_test(owner, &frame);
    let mut sender_queue = ReliableRelaySenderQueue::default();
    sender.enqueue_critical_reinjection_frame(&mut sender_queue, frame, cause);

    target_commands.begin_path_drain();
    target_receivers.close_for_path_drain();
    assert!(matches!(
        sender
            .dispatch_client_queued_work(
                &context,
                TrafficClass::Throughput,
                &mut remotes,
                &mut send_stream,
                &mut sender_queue,
                6,
                ReliableDataAckFrontierState::Live,
            )
            .await,
        Err(RuntimeError::SenderServiceBlocked)
    ));
    assert!(
        remotes.contains_path_instance(target),
        "closed admission cannot remove ahead of the ordered terminal"
    );
    assert_eq!(
        sender_queue.reinjection_bytes(),
        6,
        "blocked production dispatch retains exact queued recovery debt"
    );

    assert!(target_receivers.finish_planned_path_retirement());
    let terminal = tokio::time::timeout(Duration::from_secs(1), remotes.recv_frame())
        .await
        .expect("ordered terminal deadline")
        .expect("ordered terminal frame");
    assert_eq!(terminal.instance, target);
    assert!(matches!(
        terminal.frame,
        Err(RuntimeError::ReliablePathRetired)
    ));
    drop(
        remotes
            .remove_path_instance(target)
            .expect("terminal receiver removes the exact attachment"),
    );
    assert!(matches!(
        sender
            .dispatch_client_queued_work(
                &context,
                TrafficClass::Throughput,
                &mut remotes,
                &mut send_stream,
                &mut sender_queue,
                6,
                ReliableDataAckFrontierState::Live,
            )
            .await
            .expect("absent exact target cancels the retained queued batch"),
        ClientQueuedDispatch::PersistentReinjectionCancelled
    ));
    assert!(sender_queue.is_empty());
}

#[tokio::test]
async fn client_ack_gap_model_separates_owner_transport_from_reinjection_output() {
    let stream_id = StreamId(90);
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10260?initial-srtt-s=0.5&initial-rate-mbps=400",
        "quic://127.0.0.1:10261?initial-srtt-s=0.04&initial-rate-mbps=200",
        "quic://127.0.0.1:10262?initial-srtt-s=0.005&initial-rate-mbps=500",
    ]);
    let (tcp_commands, _tcp_receivers) = reliable_path_command_channels(8);
    let (udp_commands, mut udp_receivers) = reliable_path_command_channels(1);
    let (proof_only_commands, mut proof_only_receivers) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream_with_underlay(
            stream_id,
            UnderlayProtocol::Udp,
            0,
            udp_commands.clone(),
        ),
        8,
    );
    consume_client_path_proof_for_test(&mut udp_receivers);
    remotes.attach_candidate(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Tcp,
        0,
        tcp_commands,
    ));
    remotes.attach_candidate(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        1,
        proof_only_commands,
    ));
    consume_client_path_proof_for_test(&mut proof_only_receivers);
    let tcp = remotes
        .paths
        .iter()
        .find(|path| path.key().underlay == UnderlayProtocol::Tcp)
        .map(ReliableRelayRemotePath::instance)
        .expect("slow TCP original path");
    let udp = remotes
        .path_instance_for_key(RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        })
        .expect("measured UDP path");
    let proof_only = remotes
        .path_instance_for_key(RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 1,
        })
        .expect("proof-only UDP path");
    context.install_relay_path_instance_for_test(tcp);
    context.install_relay_path_instance_for_test(proof_only);

    let limits = MuxLimits::default();
    let mut send_stream = ReliableSendStream::new(stream_id, limits);
    let blocked = send_stream
        .send_data(Bytes::from(vec![0x41; 4096]))
        .expect("blocked owner data");
    send_stream
        .send_data(Bytes::from(vec![0x42; 4096]))
        .expect("later delivered data");
    let mut sender = RequestSenderService::new(stream_id);
    let sender_queue = ReliableRelaySenderQueue::default();
    sender.record_original_frame_for_test(tcp, &blocked);
    let ranges = [OffsetRange {
        start: 4096,
        end: 8192,
    }];

    // Match the relay actor's race-free ordering: generation precedes every
    // model read that can conclude there is no measured alternate.
    let path_model_generation_before_observation = context.path_model_generation();
    let observation = sender.data_ack_gap_reinjection_model(
        &context,
        &remotes,
        &send_stream,
        &sender_queue,
        &ranges,
        64 * 1024,
        TrafficClass::Throughput,
    );
    let original_underlay = observation.original_underlay;
    let original_path_timing = observation.original_path_timing;
    let unproven_reinjection_path = observation.reinjection_target;
    assert_eq!(original_underlay, Some(UnderlayProtocol::Tcp));
    assert_eq!(
        original_path_timing.map(|snapshot| snapshot.srtt_ms),
        Some(500.0),
        "persistent-gap proof time follows the original TCP path"
    );
    assert!(
        unproven_reinjection_path.is_none(),
        "proof-only membership may carry a bounded reinjection quantum but must not authorize a BDP-sized burst from configured hints"
    );
    // Exercise the exact lost-wake interval: measurement is published after
    // the negative model read but before the relay can arm its waiter.
    seed_client_bulk_evidence_for_test(&context, udp);
    let path_model_publication =
        context.arm_path_model_publication(path_model_generation_before_observation);
    tokio::time::timeout(Duration::from_millis(50), path_model_publication)
        .await
        .expect("bulk evidence must wake a client blocked on an unmeasured ACK-gap alternate");
    let observation = sender.data_ack_gap_reinjection_model(
        &context,
        &remotes,
        &send_stream,
        &sender_queue,
        &ranges,
        64 * 1024,
        TrafficClass::Throughput,
    );
    let original_underlay = observation.original_underlay;
    let original_path_timing = observation.original_path_timing;
    let reinjection_path = observation.reinjection_target;

    assert_eq!(original_underlay, Some(UnderlayProtocol::Tcp));
    assert_eq!(
        original_path_timing.map(|snapshot| snapshot.underlay),
        Some(UnderlayProtocol::Tcp)
    );
    assert_eq!(
        reinjection_path.map(|(_, snapshot)| snapshot.underlay),
        Some(UnderlayProtocol::Udp),
        "the exact ACK-gap selector must avoid the TCP owner and model the distinct QUIC reinjection output"
    );
    let (reinjection_target, reinjection_path) =
        reinjection_path.expect("distinct reinjection path");
    let persistent_service_limit = reliable_reinjection_service_limit_bytes(
        ReliableReinjectionTargetWork::new(Some(reinjection_path), 0, 0),
        limits.max_repair_bytes,
        limits,
    );
    assert_eq!(
        persistent_service_limit,
        reliable_product_feedback_window_bytes(
            Some(reinjection_path),
            TrafficClass::Throughput,
            limits,
        ),
        "persistent Data ACK-gap recovery uses the measured alternate's available Product service window",
    );
    assert!(
        persistent_service_limit
            > adaptive_reliable_relay_reinjection_bytes(
                Some(reinjection_path),
                TrafficClass::Throughput,
                limits,
            ),
        "a persistent gap is serviced by target capacity rather than one latency quantum",
    );
    assert_eq!(
        reliable_critical_tail_reinjection_limit_bytes(
            persistent_service_limit,
            limits.max_repair_bytes,
            limits,
        ),
        persistent_service_limit,
        "the shared repair and path-flight envelopes preserve the target-sized service authority",
    );

    seed_client_bulk_evidence_for_test(&context, proof_only);

    udp_commands
        .try_enqueue_admitted_frame(
            Frame::StreamData {
                stream_id: StreamId(91),
                offset: 0,
                payload: Bytes::from_static(b"busy"),
            },
            TrafficClass::Throughput,
        )
        .expect("fill the modeled reinjection output after sizing");
    let unrelated = recv_reliable_path_command(&mut udp_receivers)
        .await
        .expect("ordered writer accepts unrelated carrier work");
    let bound_cause =
        RelaySendCause::persistent_client_ack_gap_reinjection(reinjection_target, reinjection_path);
    let mut queue = ReliableRelaySenderQueue::default();
    sender.enqueue_critical_reinjection_frame(&mut queue, blocked.clone(), bound_cause);
    let dispatch = sender
        .dispatch_client_queued_work(
            &context,
            TrafficClass::Throughput,
            &mut remotes,
            &mut send_stream,
            &mut queue,
            4096,
            ReliableDataAckFrontierState::Live,
        )
        .await
        .expect("queued bound repair uses headroom independent of shared writer work");
    assert!(matches!(dispatch, ClientQueuedDispatch::Reinjection { .. }));
    assert!(queue.is_empty());
    udp_receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&unrelated));
    assert!(matches!(
        try_recv_reliable_path_command(&mut udp_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            ref payload,
            ..
        })) if payload.len() == 4096
    ));
    assert!(
        try_recv_reliable_path_command(&mut proof_only_receivers).is_none(),
        "a persistent repair stays bound to the modeled output instead of switching to another proven output"
    );

    let replacement = remotes
        .paths
        .iter_mut()
        .find(|path| path.instance() == reinjection_target.instance)
        .expect("modeled reinjection attachment remains present");
    replacement.attachment_id = replacement.attachment_id.saturating_add(1);
    sender.enqueue_critical_reinjection_frame(&mut queue, blocked, bound_cause);
    assert_eq!(
        queue.reinjection_bytes(),
        4096,
        "bound repair must retain exact recovery debt while queued",
    );
    let dispatch = sender
        .dispatch_client_queued_work(
            &context,
            TrafficClass::Throughput,
            &mut remotes,
            &mut send_stream,
            &mut queue,
            4096,
            ReliableDataAckFrontierState::Live,
        )
        .await
        .expect("stale bound reinjection is cancelled without aborting the stream");
    assert!(matches!(
        dispatch,
        ClientQueuedDispatch::PersistentReinjectionCancelled
    ));
    assert!(queue.is_empty());
    assert_eq!(queue.reinjection_bytes(), 0);
}

#[tokio::test]
async fn disappeared_bound_path_recovery_target_is_cancelled_and_reselected() {
    let stream_id = StreamId(232);
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10321?initial-srtt-s=0.08&initial-rate-mbps=100",
        "tcp://127.0.0.1:10322?initial-srtt-s=0.005&initial-rate-mbps=1000",
        "tcp://127.0.0.1:10323?initial-srtt-s=0.04&initial-rate-mbps=200",
    ]);
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
        .expect("initial recovery target")
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

    let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
    let original = send_stream
        .send_data(Bytes::from(vec![0x6e; 4096]))
        .expect("retained original data");
    let mut sender = RequestSenderService::new(stream_id);
    sender.record_original_frame_for_test(owner, &original);
    assert!(sender.multipath.mark_path_stale(owner));
    let mut queue = ReliableRelaySenderQueue::default();
    assert!(
        sender
            .drive_request_path_recovery(
                &mut queue,
                &context,
                &remotes,
                &send_stream,
                TrafficClass::Throughput,
            )
            .queued
    );

    drop(remotes.remove_path_instance(first));
    let cancelled = sender
        .dispatch_client_queued_work(
            &context,
            TrafficClass::Throughput,
            &mut remotes,
            &mut send_stream,
            &mut queue,
            4096,
            ReliableDataAckFrontierState::Live,
        )
        .await
        .expect("lost exact target cancels only its bound recovery work");
    assert!(matches!(
        cancelled,
        ClientQueuedDispatch::PathRecoveryReinjectionCancelled
    ));
    assert!(queue.is_empty());

    assert!(
        sender
            .drive_request_path_recovery(
                &mut queue,
                &context,
                &remotes,
                &send_stream,
                TrafficClass::Throughput,
            )
            .queued,
        "the uncovered range remains immediately eligible",
    );
    assert!(matches!(
        sender
            .dispatch_client_queued_work(
                &context,
                TrafficClass::Throughput,
                &mut remotes,
                &mut send_stream,
                &mut queue,
                4096,
                ReliableDataAckFrontierState::Live,
            )
            .await
            .expect("replacement target dispatch"),
        ClientQueuedDispatch::Reinjection { .. }
    ));
    assert!(matches!(
        try_recv_reliable_path_command(&mut second_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
    assert!(try_recv_reliable_path_command(&mut owner_receivers).is_none());
}

#[tokio::test]
async fn client_live_tail_uses_retained_send_extent_beyond_ack_snapshot() {
    let stream_id = StreamId(126);
    let context =
        client_test_context_with_paths(&["tcp://127.0.0.1:10341", "quic://127.0.0.1:10342"]);
    let (tcp_commands, mut tcp_receivers) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream_with_underlay(stream_id, UnderlayProtocol::Tcp, 0, tcp_commands),
        8,
    );
    consume_client_path_proof_for_test(&mut tcp_receivers);
    let tcp = remotes.paths[0].instance();
    let (udp_commands, mut udp_receivers) = reliable_path_command_channels(8);
    let udp_commands_for_writer = udp_commands.clone();
    assert_eq!(
        remotes.attach(opened_test_relay_stream_with_underlay(
            stream_id,
            UnderlayProtocol::Udp,
            0,
            udp_commands,
        )),
        ReliableRelayAttachOutcome::Attached
    );
    consume_client_path_proof_for_test(&mut udp_receivers);
    let udp = remotes
        .path_instance_for_key(RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        })
        .expect("UDP tail-recovery path");
    seed_client_bulk_evidence_for_test(&context, tcp);
    seed_client_bulk_evidence_for_test(&context, udp);

    let limits = MuxLimits::default();
    let mut send_stream = ReliableSendStream::new(stream_id, limits);
    let acknowledged = send_stream
        .send_data(Bytes::from(vec![0x41; 64]))
        .expect("acknowledged prefix");
    let live_tail = send_stream
        .send_data(Bytes::from(vec![0x42; 64]))
        .expect("unacknowledged live tail");
    let mut sender = RequestSenderService::new(stream_id);
    sender.record_original_frame_for_test(tcp, &acknowledged);
    sender.record_original_frame_for_test(tcp, &live_tail);
    let ack_ranges = [OffsetRange { start: 0, end: 64 }];
    let _ = send_stream.apply_ack(&ack_ranges);

    let recovery_interval =
        reliable_relay_tail_reinjection_delay(context.reliable_path_snapshot(tcp.key));
    tokio::time::sleep(recovery_interval + Duration::from_millis(10)).await;
    udp_commands_for_writer
        .try_enqueue_stream_ordered_frame(
            Frame::StreamData {
                stream_id: StreamId(999),
                offset: 0,
                payload: Bytes::from_static(b"unrelated carrier work"),
            },
            TrafficClass::Throughput,
        )
        .expect("queue unrelated work on the shared carrier");
    let unrelated = recv_reliable_path_command(&mut udp_receivers)
        .await
        .expect("ordered writer accepts unrelated carrier work");
    let mut sender_queue = ReliableRelaySenderQueue::default();
    assert!(sender.enqueue_tail_reinjection(
        &mut sender_queue,
        &context,
        &remotes,
        &send_stream,
        &ack_ranges,
        true,
        Some(send_stream.next_offset()),
        64,
        TrafficClass::Latency,
    ));
    assert!(matches!(
        sender
            .dispatch_client_queued_work(
                &context,
                TrafficClass::Latency,
                &mut remotes,
                &mut send_stream,
                &mut sender_queue,
                64,
                ReliableDataAckFrontierState::Live,
            )
            .await
            .expect("live tail dispatch"),
        ClientQueuedDispatch::Reinjection {
            payload_bytes: 64,
            ..
        }
    ));
    udp_receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&unrelated));
    assert!(matches!(
        try_recv_reliable_path_command(&mut udp_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            offset: 64,
            payload,
            ..
        })) if payload.as_ref() == [0x42; 64]
    ));
    assert!(
        try_recv_reliable_path_command(&mut tcp_receivers).is_none(),
        "the bounded probe must use the distinct live attachment"
    );
}

#[tokio::test]
async fn client_live_tail_stops_at_an_already_queued_frontier_copy() {
    let stream_id = StreamId(226);
    let context =
        client_test_context_with_paths(&["tcp://127.0.0.1:11341", "quic://127.0.0.1:11342"]);
    let (tcp_commands, mut tcp_receivers) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream_with_underlay(stream_id, UnderlayProtocol::Tcp, 0, tcp_commands),
        8,
    );
    consume_client_path_proof_for_test(&mut tcp_receivers);
    let tcp = remotes.paths[0].instance();
    let (udp_commands, mut udp_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        remotes.attach(opened_test_relay_stream_with_underlay(
            stream_id,
            UnderlayProtocol::Udp,
            0,
            udp_commands,
        )),
        ReliableRelayAttachOutcome::Attached,
    );
    consume_client_path_proof_for_test(&mut udp_receivers);
    let udp = remotes
        .path_instance_for_key(RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        })
        .expect("UDP tail-recovery path");
    seed_client_bulk_evidence_for_test(&context, tcp);
    seed_client_bulk_evidence_for_test(&context, udp);

    let limits = MuxLimits::default();
    let mut send_stream = ReliableSendStream::new(stream_id, limits);
    let prefix = send_stream
        .send_data(Bytes::from(vec![0x41; 64]))
        .expect("acknowledged prefix");
    let first_tail = send_stream
        .send_data(Bytes::from(vec![0x42; 64]))
        .expect("lowest retained tail frame");
    let later_tail = send_stream
        .send_data(Bytes::from(vec![0x43; 64]))
        .expect("later retained tail frame");
    let mut sender = RequestSenderService::new(stream_id);
    for frame in [&prefix, &first_tail, &later_tail] {
        sender.record_original_frame_for_test(tcp, frame);
    }
    let ack_ranges = [OffsetRange { start: 0, end: 64 }];
    let _ = send_stream.apply_ack(&ack_ranges);
    let recovery_interval =
        reliable_relay_tail_reinjection_delay(context.reliable_path_snapshot(tcp.key));
    tokio::time::sleep(recovery_interval + Duration::from_millis(10)).await;

    let mut sender_queue = ReliableRelaySenderQueue::default();
    sender.enqueue_critical_reinjection_frame(
        &mut sender_queue,
        first_tail,
        RelaySendCause::TailReinjection,
    );
    let queued_before = sender_queue.bytes();
    assert!(!sender.enqueue_tail_reinjection(
        &mut sender_queue,
        &context,
        &remotes,
        &send_stream,
        &ack_ranges,
        true,
        Some(send_stream.next_offset()),
        64,
        TrafficClass::Latency,
    ));
    assert_eq!(
        sender_queue.bytes(),
        queued_before,
        "an occupied lowest frontier must stop the batch; later tail extents cannot consume the live-owner opportunity",
    );
}

#[tokio::test]
async fn completion_tail_apply_shrinks_to_exact_target_service_before_consuming_epoch() {
    let stream_id = StreamId(227);
    let resource_limits = ResourceLimits {
        max_repair_bytes: 4096,
        max_path_flight_bytes: 4096,
        ..ResourceLimits::default()
    };
    let limits = MuxLimits::from(resource_limits);
    let context = ClientPathContext::new(
        [
            "tcp://127.0.0.1:11351?initial-srtt-s=0.005&initial-rate-mbps=1",
            "quic://127.0.0.1:11352?initial-srtt-s=0.04&initial-rate-mbps=500",
            "tcp://127.0.0.1:11353?initial-srtt-s=0.04&initial-rate-mbps=500",
        ]
        .into_iter()
        .map(|path| path.parse::<PathSpec>().expect("path"))
        .collect(),
        security(),
        resource_limits,
    )
    .expect("context");
    let (owner_commands, mut owner_receivers) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream_with_underlay(stream_id, UnderlayProtocol::Tcp, 0, owner_commands),
        8,
    );
    consume_client_path_proof_for_test(&mut owner_receivers);
    let owner = remotes.paths[0].instance();
    let (target_commands, mut target_receivers) = reliable_path_command_channels(1);
    let target_commands_for_fill = target_commands.clone();
    assert_eq!(
        remotes.attach(
            opened_test_relay_stream_with_native_source(
                stream_id,
                UnderlayProtocol::Udp,
                0,
                target_commands,
                crate::transport::RateHint::BitsPerSecond(500_000_000),
                1,
                None,
            )
            .0,
        ),
        ReliableRelayAttachOutcome::Attached,
    );
    consume_client_path_proof_for_test(&mut target_receivers);
    let target = remotes
        .path_instance_for_key(RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        })
        .expect("completion-tail target");
    let (unmeasured_commands, mut unmeasured_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        remotes.attach(opened_test_relay_stream_with_underlay(
            stream_id,
            UnderlayProtocol::Tcp,
            1,
            unmeasured_commands,
        )),
        ReliableRelayAttachOutcome::Attached,
    );
    consume_client_path_proof_for_test(&mut unmeasured_receivers);
    let unmeasured = remotes
        .path_instance_for_key(RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 1,
        })
        .expect("unmeasured completion-tail alternate");
    context.install_relay_path_instance_for_test(owner);
    context.install_relay_path_instance_for_test(target);
    context.install_relay_path_instance_for_test(unmeasured);
    context.mark_tcp_path_open_success(
        owner.key.index,
        Duration::from_millis(5),
        TrafficClass::Throughput,
    );
    context.mark_udp_path_open_success(target.key.index, Duration::from_millis(40));
    context.mark_tcp_path_open_success(
        unmeasured.key.index,
        Duration::from_millis(40),
        TrafficClass::Throughput,
    );
    context.mark_relay_path_rate_sample_for_test(
        owner.key,
        PathRateSample::new(64 * 1024, Duration::from_millis(524)).expect("slow owner sample"),
    );

    let mut send_stream = ReliableSendStream::new(stream_id, limits);
    let retained = send_stream
        .send_data(Bytes::from(vec![0x55; 4096]))
        .expect("retained tail");
    let mut sender = RequestSenderService::new(stream_id);
    sender.record_original_frame_for_test(owner, &retained);
    let mut model_wait_sender = RequestSenderService::new(stream_id);
    model_wait_sender.record_original_frame_for_test(owner, &retained);
    let mut queue = ReliableRelaySenderQueue::default();
    let owner_interval = reliable_data_retransmission_interval(
        Some(owner.key.underlay),
        context.reliable_path_snapshot_for_instance(owner),
    );
    tokio::time::sleep(owner_interval + Duration::from_millis(10)).await;
    let observed_generation = context.path_model_generation();
    let model_wait = model_wait_sender.enqueue_completion_tail_reinjection(
        &mut queue,
        &context,
        &remotes,
        &send_stream,
        &[],
        true,
        0,
        TrafficClass::Throughput,
    );
    assert!(!model_wait.queued);
    assert!(!model_wait.blocked_for_carrier_capacity);
    assert!(
        model_wait.waiting_for_path_model_publication,
        "a due horizon-zero EOF tail without measured alternate Product evidence must retain a publication wake",
    );
    let model_publication = context.arm_path_model_publication(observed_generation);
    context.mark_relay_path_rate_sample_for_test(
        target.key,
        PathRateSample::new(64 * 1024, Duration::from_micros(1049)).expect("fast target sample"),
    );
    tokio::time::timeout(Duration::from_millis(50), model_publication)
        .await
        .expect("completion-tail model publication cannot be lost after the due observation");

    let initial_target = sender
        .multipath
        .tail_reinjection_earlier_completion_service_target(
            &context,
            &remotes,
            &retained,
            TrafficClass::Throughput,
            &queue,
            send_stream.reinjection_bytes(),
            limits,
        )
        .expect("faster alternate has positive exact service");
    assert_eq!(initial_target.identity.instance, target);
    let initial_service = initial_target.service_limit_bytes;
    assert!(initial_service > 32, "service={initial_service}");
    sender.enqueue_critical_reinjection_frame(
        &mut queue,
        Frame::StreamData {
            stream_id,
            offset: 1_000_000,
            payload: Bytes::from(vec![0x33; initial_service - 32]),
        },
        RelaySendCause::CompletionTailReinjection(ClientReinjectionOutputIdentity {
            instance: target,
        }),
    );
    let queued_before = queue.reinjection_bytes();
    let mut exhausted_sender = RequestSenderService::new(stream_id);
    exhausted_sender.record_original_frame_for_test(owner, &retained);
    let mut capacity_sender = RequestSenderService::new(stream_id);
    capacity_sender.record_original_frame_for_test(owner, &retained);
    let mut exhausted_queue = ReliableRelaySenderQueue::default();
    exhausted_sender.enqueue_critical_reinjection_frame(
        &mut exhausted_queue,
        Frame::StreamData {
            stream_id,
            offset: 2_000_000,
            payload: Bytes::from(vec![0x44; initial_service]),
        },
        RelaySendCause::CompletionTailReinjection(ClientReinjectionOutputIdentity {
            instance: target,
        }),
    );
    let target_interval = reliable_data_retransmission_interval(
        Some(target.key.underlay),
        context.reliable_path_snapshot_for_instance(target),
    );
    assert_ne!(
        target_interval, owner_interval,
        "fixture requires asymmetric R"
    );
    tokio::time::sleep(owner_interval + Duration::from_millis(10)).await;

    let mut capacity_queue = ReliableRelaySenderQueue::default();
    target_commands_for_fill
        .try_enqueue_reinjection_frame(
            Frame::StreamData {
                stream_id,
                offset: 3_000_000,
                payload: Bytes::from_static(b"full"),
            },
            TrafficClass::Throughput,
        )
        .expect("fill exact completion target native queue");
    let capacity_wait = crate::runtime::stream::arm_carrier_capacity_notifies(
        remotes
            .paths
            .iter()
            .flat_map(|path| path.stream.capacity_notifies())
            .collect::<Vec<_>>(),
    )
    .expect("completion target exposes native capacity edge");
    let blocked = capacity_sender.enqueue_completion_tail_reinjection(
        &mut capacity_queue,
        &context,
        &remotes,
        &send_stream,
        &[],
        true,
        0,
        TrafficClass::Throughput,
    );
    assert!(
        !blocked.queued,
        "full-target outcome={blocked:?} queue={capacity_queue:?}",
    );
    assert!(
        blocked.blocked_for_carrier_capacity,
        "full-target outcome={blocked:?}",
    );
    assert!(
        blocked.waiting_for_path_model_publication,
        "a full measured target and a distinct unmeasured alternate retain both independent wake edges",
    );
    let filler = try_recv_reliable_path_command(&mut target_receivers)
        .expect("release the full completion target queue");
    target_receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&filler));
    tokio::time::timeout(Duration::from_millis(50), capacity_wait)
        .await
        .expect("pre-armed request completion-tail capacity wake cannot be lost");
    assert!(
        capacity_sender
            .enqueue_completion_tail_reinjection(
                &mut capacity_queue,
                &context,
                &remotes,
                &send_stream,
                &[],
                true,
                0,
                TrafficClass::Throughput,
            )
            .queued,
        "capacity release makes the same retained exact tail admissible",
    );

    let accepted_after = Instant::now();
    assert!(
        sender
            .enqueue_completion_tail_reinjection(
                &mut queue,
                &context,
                &remotes,
                &send_stream,
                &[],
                true,
                0,
                TrafficClass::Throughput,
            )
            .queued
    );
    let accepted_by = Instant::now();
    assert_eq!(
        queue.reinjection_bytes() - queued_before,
        32,
        "Apply must shrink the ranked tail preview to the selected target's exact remaining Product service",
    );
    let successor_deadline = sender
        .live_owner_frontier_floor_deadline()
        .expect("only the accepted exact prefix consumes the shared floor epoch");
    assert!(
        successor_deadline >= accepted_after + target_interval
            && successor_deadline <= accepted_by + target_interval,
        "T_f uses the owner's R, but the accepted target-bound successor G must use selected target R_t",
    );
    if target_interval < owner_interval {
        assert!(successor_deadline < accepted_after + owner_interval);
    }

    let (_, filler) = queue.pop_front().expect("target service filler");
    assert!(matches!(
        filler.kind,
        ReliableRelayQueuedWorkKind::Reinjection {
            frame: Frame::StreamData {
                offset: 1_000_000,
                ..
            },
            ..
        }
    ));
    assert_eq!(queue.reinjection_bytes(), 32);
    let dispatch = sender
        .dispatch_client_queued_work(
            &context,
            TrafficClass::Throughput,
            &mut remotes,
            &mut send_stream,
            &mut queue,
            32,
            ReliableDataAckFrontierState::Live,
        )
        .await
        .expect("the M-bound completion target remains dispatchable after Apply shrinks to F");
    assert!(matches!(
        dispatch,
        ClientQueuedDispatch::Reinjection {
            payload_bytes: 32,
            ..
        }
    ));
    assert!(matches!(
        try_recv_reliable_path_command(&mut target_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            offset: 0,
            ref payload,
            ..
        })) if payload.len() == 32
    ));
    assert!(try_recv_reliable_path_command(&mut owner_receivers).is_none());

    assert!(
        !exhausted_sender
            .enqueue_completion_tail_reinjection(
                &mut exhausted_queue,
                &context,
                &remotes,
                &send_stream,
                &[],
                true,
                0,
                TrafficClass::Throughput,
            )
            .queued
    );
    assert_eq!(
        exhausted_sender.live_owner_frontier_floor_deadline(),
        None,
        "an oversized preview rejected by exact target service cannot consume the epoch",
    );
}

#[tokio::test]
async fn request_completion_tail_extent_is_percentage_invariant() {
    let stream_id = StreamId(229);
    let resource_limits = ResourceLimits {
        max_repair_bytes: 512 * 1024,
        max_path_flight_bytes: 512 * 1024,
        max_reliable_relay_chunk_bytes: 64 * 1024,
        ..ResourceLimits::default()
    };
    let limits = MuxLimits::from(resource_limits);
    let context = ClientPathContext::new(
        [
            "tcp://127.0.0.1:11371?initial-srtt-s=0.005&initial-rate-mbps=1",
            "quic://127.0.0.1:11372?initial-srtt-s=0.04&initial-rate-mbps=500",
        ]
        .into_iter()
        .map(|path| path.parse::<PathSpec>().expect("path"))
        .collect(),
        security(),
        resource_limits,
    )
    .expect("context");
    let (owner_commands, mut owner_receivers) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream_with_underlay(stream_id, UnderlayProtocol::Tcp, 0, owner_commands),
        8,
    );
    consume_client_path_proof_for_test(&mut owner_receivers);
    let owner = remotes.paths[0].instance();
    let (target_commands, mut target_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        remotes.attach(
            opened_test_relay_stream_with_native_source(
                stream_id,
                UnderlayProtocol::Udp,
                0,
                target_commands,
                crate::transport::RateHint::BitsPerSecond(500_000_000),
                1,
                None,
            )
            .0,
        ),
        ReliableRelayAttachOutcome::Attached,
    );
    consume_client_path_proof_for_test(&mut target_receivers);
    let target = remotes
        .path_instance_for_key(RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        })
        .expect("completion-tail target");
    context.install_relay_path_instance_for_test(owner);
    context.install_relay_path_instance_for_test(target);
    context.mark_tcp_path_open_success(
        owner.key.index,
        Duration::from_millis(5),
        TrafficClass::Throughput,
    );
    context.mark_udp_path_open_success(target.key.index, Duration::from_millis(40));
    context.mark_relay_path_rate_sample_for_test(
        owner.key,
        PathRateSample::new(64 * 1024, Duration::from_millis(524)).expect("slow owner sample"),
    );
    context.mark_relay_path_rate_sample_for_test(
        target.key,
        PathRateSample::new(64 * 1024, Duration::from_micros(1049)).expect("fast target sample"),
    );

    let tail_bytes = 256 * 1024;
    let mut send_stream = ReliableSendStream::new(stream_id, limits);
    let retained = send_stream
        .send_data(Bytes::from(vec![0x61; tail_bytes]))
        .expect("retained completion tail");
    let startup_floor = sender_optional_reinjection_startup_floor_bytes(limits);
    assert!(tail_bytes > startup_floor);
    let delivered_bytes = tail_bytes.saturating_mul(10);
    let default_percent = MppPerformanceConfig::default().optional_reinjection_budget_percent;
    let mut cases = [0, default_percent, 200].map(|percent| {
        let mut sender = RequestSenderService::new_with_performance(
            stream_id,
            MppPerformanceConfig {
                optional_reinjection_budget_percent: percent,
            },
        );
        sender.record_original_frame_for_test(owner, &retained);
        sender.record_delivered_data_for_test(delivered_bytes);
        sender.record_reinjection_for_test(startup_floor);
        (percent, sender)
    });

    let owner_interval = reliable_data_retransmission_interval(
        Some(owner.key.underlay),
        context.reliable_path_snapshot_for_instance(owner),
    );
    tokio::time::sleep(owner_interval + Duration::from_millis(10)).await;

    let outcomes = cases.each_mut().map(|(percent, sender)| {
        let mut queue = ReliableRelaySenderQueue::default();
        let outcome = sender.enqueue_completion_tail_reinjection(
            &mut queue,
            &context,
            &remotes,
            &send_stream,
            &[],
            true,
            0,
            TrafficClass::Throughput,
        );
        let queued_bytes = queue.reinjection_bytes();
        let mut exact_target = true;
        while let Some((_, work)) = queue.pop_front() {
            exact_target &= matches!(
                work.kind,
                ReliableRelayQueuedWorkKind::Reinjection {
                    cause: RelaySendCause::CompletionTailReinjection(identity),
                    ..
                } if identity.instance == target
            );
        }
        (*percent, outcome.queued, queued_bytes, exact_target)
    });
    let structural = outcomes[2];
    assert!(
        structural.1 && structural.2 > startup_floor && structural.3,
        "the large-hint control must expose a multi-quantum structurally eligible exact-target tail: {structural:?}",
    );
    for outcome in outcomes {
        assert_eq!(
            (outcome.1, outcome.2, outcome.3),
            (structural.1, structural.2, structural.3),
            "fixed range, exact owner/target, clocks, Product headroom, resource limits, and native capacity must make completion-tail admission and extent invariant to the traffic percentage: percent={}",
            outcome.0,
        );
    }
}

#[tokio::test]
async fn completion_tail_uses_cache_independent_common_extent_for_target_and_apply() {
    let stream_id = StreamId(228);
    let resource_limits = ResourceLimits {
        max_repair_bytes: 128 * 1024,
        max_path_flight_bytes: 64 * 1024,
        max_reliable_relay_chunk_bytes: 64 * 1024,
        ..ResourceLimits::default()
    };
    let limits = MuxLimits::from(resource_limits);
    let context = ClientPathContext::new(
        [
            "tcp://127.0.0.1:11361?initial-srtt-s=0.005&initial-rate-mbps=1",
            "quic://127.0.0.1:11362?initial-srtt-s=0.04&initial-rate-mbps=500",
            "tcp://127.0.0.1:11363?initial-srtt-s=0.001&initial-rate-mbps=500",
        ]
        .into_iter()
        .map(|path| path.parse::<PathSpec>().expect("path"))
        .collect(),
        security(),
        resource_limits,
    )
    .expect("context");
    let (owner_commands, mut owner_receivers) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream_with_underlay(stream_id, UnderlayProtocol::Tcp, 0, owner_commands),
        8,
    );
    consume_client_path_proof_for_test(&mut owner_receivers);
    let owner = remotes.paths[0].instance();
    let (target_commands, mut target_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        remotes.attach(
            opened_test_relay_stream_with_native_source(
                stream_id,
                UnderlayProtocol::Udp,
                0,
                target_commands,
                crate::transport::RateHint::BitsPerSecond(500_000_000),
                1,
                None,
            )
            .0,
        ),
        ReliableRelayAttachOutcome::Attached,
    );
    consume_client_path_proof_for_test(&mut target_receivers);
    let target = remotes
        .path_instance_for_key(RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        })
        .expect("completion-tail target");
    context.install_relay_path_instance_for_test(owner);
    context.install_relay_path_instance_for_test(target);
    context.mark_tcp_path_open_success(
        owner.key.index,
        Duration::from_millis(5),
        TrafficClass::Throughput,
    );
    context.mark_udp_path_open_success(target.key.index, Duration::from_millis(40));
    context.mark_relay_path_rate_sample_for_test(
        owner.key,
        PathRateSample::new(64 * 1024, Duration::from_millis(524)).expect("slow owner sample"),
    );
    context.mark_relay_path_rate_sample_for_test(
        target.key,
        PathRateSample::new(64 * 1024, Duration::from_micros(1049)).expect("fast target sample"),
    );

    let mut send_stream = ReliableSendStream::new(stream_id, limits);
    let first = send_stream
        .send_data(Bytes::from(vec![0x61; 1024]))
        .expect("1 KiB cache chunk");
    let second = send_stream
        .send_data(Bytes::from(vec![0x62; 63 * 1024]))
        .expect("63 KiB cache chunk");
    let mut sender = RequestSenderService::new(stream_id);
    sender.record_original_frame_for_test(owner, &first);
    sender.record_original_frame_for_test(owner, &second);
    let queue = ReliableRelaySenderQueue::default();
    assert!(
        sender
            .multipath
            .tail_reinjection_earlier_completion_service_target(
                &context,
                &remotes,
                &first,
                TrafficClass::Throughput,
                &queue,
                send_stream.reinjection_bytes(),
                limits,
            )
            .is_none(),
        "the low-RTT owner wins if storage's 1 KiB first chunk is incorrectly used as M",
    );
    assert_eq!(
        sender
            .multipath
            .tail_reinjection_earlier_completion_service_target_for_extent(
                &context,
                &remotes,
                &first,
                TrafficClass::Throughput,
                &queue,
                send_stream.reinjection_bytes(),
                limits,
                64 * 1024,
            )
            .expect("the high-rate alternate wins the common 64 KiB extent")
            .identity
            .instance,
        target,
    );

    let owner_interval = reliable_data_retransmission_interval(
        Some(owner.key.underlay),
        context.reliable_path_snapshot_for_instance(owner),
    );
    tokio::time::sleep(owner_interval + Duration::from_millis(10)).await;

    let mut owner_wins_stream = ReliableSendStream::new(stream_id, limits);
    let owner_wins_frame = owner_wins_stream
        .send_data(Bytes::from(vec![0x60; 1024]))
        .expect("small owner-favored tail");
    let mut owner_wins_sender = RequestSenderService::new(stream_id);
    owner_wins_sender.record_original_frame_for_test(owner, &owner_wins_frame);
    tokio::time::sleep(owner_interval + Duration::from_millis(10)).await;
    let mut owner_wins_queue = ReliableRelaySenderQueue::default();
    let owner_wins_outcome = owner_wins_sender.enqueue_completion_tail_reinjection(
        &mut owner_wins_queue,
        &context,
        &remotes,
        &owner_wins_stream,
        &[],
        true,
        0,
        TrafficClass::Throughput,
    );
    assert!(
        owner_wins_outcome.queued,
        "after the exact owner's fallback deadline, G may use the best measured distinct target even when that target does not beat the owner's stale ETA",
    );
    let (_, owner_wins_work) = owner_wins_queue
        .pop_front()
        .expect("post-fallback request completion repair");
    assert!(matches!(
        owner_wins_work.kind,
        ReliableRelayQueuedWorkKind::Reinjection {
            cause: RelaySendCause::CompletionTailReinjection(identity),
            ..
        } if identity.instance == target
    ));

    let mut queue = ReliableRelaySenderQueue::default();
    assert!(
        sender
            .enqueue_completion_tail_reinjection(
                &mut queue,
                &context,
                &remotes,
                &send_stream,
                &[],
                true,
                0,
                TrafficClass::Throughput,
            )
            .queued
    );

    let mut cursor = 0_u64;
    while let Some((lane, work)) = queue.pop_front() {
        assert_eq!(lane, ReliableWorkClass::Reinjection);
        let ReliableRelayQueuedWorkKind::Reinjection { frame, cause } = work.kind else {
            panic!("completion-tail queue contains only reinjection work");
        };
        assert_eq!(
            cause,
            RelaySendCause::CompletionTailReinjection(ClientReinjectionOutputIdentity {
                instance: target,
            }),
        );
        let (start, end, _) = reliable_stream_frame_extent(&frame).expect("queued STREAM_DATA");
        assert_eq!(start, cursor, "Apply keeps one exact contiguous prefix");
        cursor = end;
    }
    assert_eq!(
        cursor,
        64 * 1024,
        "Apply uses the same owner-uniform range scored independently of the two cache chunks",
    );

    let mut late_suffix_stream = ReliableSendStream::new(stream_id, limits);
    let ranked_prefix = late_suffix_stream
        .send_data(Bytes::from(vec![0x63; 64 * 1024]))
        .expect("ranked 64 KiB prefix");
    let mut late_suffix_sender = RequestSenderService::new(stream_id);
    late_suffix_sender.record_original_frame_for_test(owner, &ranked_prefix);
    tokio::time::sleep(owner_interval + Duration::from_millis(10)).await;
    let unranked_suffix = late_suffix_stream
        .send_data(Bytes::from(vec![0x64; 64 * 1024]))
        .expect("fresh suffix beyond M");
    late_suffix_sender.record_original_frame_for_test(owner, &unranked_suffix);
    let mut late_suffix_queue = ReliableRelaySenderQueue::default();
    let late_suffix_outcome = late_suffix_sender.enqueue_completion_tail_reinjection(
        &mut late_suffix_queue,
        &context,
        &remotes,
        &late_suffix_stream,
        &[],
        true,
        0,
        TrafficClass::Throughput,
    );
    assert!(late_suffix_outcome.queued);
    assert_eq!(
        late_suffix_queue.reinjection_bytes(),
        64 * 1024,
        "a fresh same-owner assignment beyond ranked M cannot postpone recovery of the mature lowest M",
    );

    let (boundary_target_commands, mut boundary_target_receivers) =
        reliable_path_command_channels(8);
    assert_eq!(
        remotes.attach(opened_test_relay_stream_with_underlay(
            stream_id,
            UnderlayProtocol::Tcp,
            1,
            boundary_target_commands,
        )),
        ReliableRelayAttachOutcome::Attached,
    );
    consume_client_path_proof_for_test(&mut boundary_target_receivers);
    let boundary_target = remotes
        .path_instance_for_key(RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 1,
        })
        .expect("low-RTT boundary target");
    context.install_relay_path_instance_for_test(boundary_target);
    context.mark_tcp_path_open_success(
        boundary_target.key.index,
        Duration::from_millis(1),
        TrafficClass::Throughput,
    );
    context.mark_relay_path_rate_sample_for_test(
        boundary_target.key,
        PathRateSample::new(64 * 1024, Duration::from_micros(1049))
            .expect("fast boundary-target sample"),
    );
    let mut boundary_stream = ReliableSendStream::new(stream_id, limits);
    let owner_prefix = boundary_stream
        .send_data(Bytes::from(vec![0x71; 1024]))
        .expect("A-owned prefix");
    let target_suffix = boundary_stream
        .send_data(Bytes::from(vec![0x72; 63 * 1024]))
        .expect("B-owned suffix");
    let mut boundary_sender = RequestSenderService::new(stream_id);
    boundary_sender.record_original_frame_for_test(owner, &owner_prefix);
    boundary_sender.record_original_frame_for_test(boundary_target, &target_suffix);
    let uniform = boundary_sender
        .multipath
        .live_owner_uniform_frontier(
            OffsetRange {
                start: 0,
                end: 64 * 1024,
            },
            &[owner, target, boundary_target],
        )
        .expect("lowest owner-uniform prefix");
    assert_eq!(
        uniform.range,
        OffsetRange {
            start: 0,
            end: 1024
        }
    );
    assert_eq!(uniform.owners, vec![owner]);

    let owner_interval = reliable_data_retransmission_interval(
        Some(owner.key.underlay),
        context.reliable_path_snapshot_for_instance(owner),
    );
    tokio::time::sleep(owner_interval + Duration::from_millis(10)).await;
    let mut boundary_queue = ReliableRelaySenderQueue::default();
    assert!(
        boundary_sender
            .enqueue_completion_tail_reinjection(
                &mut boundary_queue,
                &context,
                &remotes,
                &boundary_stream,
                &[],
                true,
                0,
                TrafficClass::Throughput,
            )
            .queued
    );
    assert_eq!(
        boundary_queue.reinjection_bytes(),
        1024,
        "one target-bound transaction stops before the next exact owner set",
    );
    let (_, work) = boundary_queue.pop_front().expect("bounded repair prefix");
    assert!(matches!(
        work.kind,
        ReliableRelayQueuedWorkKind::Reinjection {
            frame: Frame::StreamData {
                offset: 0,
                ref payload,
                ..
            },
            cause: RelaySendCause::CompletionTailReinjection(identity),
        } if payload.len() == 1024 && identity.instance == boundary_target
    ));
    assert!(boundary_queue.pop_front().is_none());
}

#[tokio::test]
async fn request_product_ack_preserves_exact_data_ack_progress_path() {
    let stream_id = StreamId(91);
    let context = client_test_context_with_paths(&["tcp://127.0.0.1:10263"]);
    let (commands, _receivers) = reliable_path_command_channels(8);
    let remotes = ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, commands), 8);
    let owner = remotes.paths[0].instance();
    let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
    let frame = send_stream
        .send_data(Bytes::from(vec![0x41; 4096]))
        .expect("request data");
    let mut sender = RequestSenderService::new(stream_id);
    sender.record_original_frame_for_test(owner, &frame);

    let ack = crate::mux::stream::validate_stream_ack(
        true,
        vec![OffsetRange {
            start: 0,
            end: 4096,
        }],
        send_stream.next_offset(),
    )
    .expect("ACK assigned request data");
    let outcome = sender
        .apply_request_product_ack(&context, &remotes, &mut send_stream, &ack)
        .expect("ACK does not exceed retained send chunks");

    assert_eq!(outcome.data_ack_progress_paths.as_slice(), &[owner]);
    assert_eq!(outcome.mux.released_bytes, 4096);

    let replay = sender
        .apply_request_product_ack(&context, &remotes, &mut send_stream, &ack)
        .expect("replayed ACK remains within the frozen send extent");
    assert_eq!(replay.mux.released_bytes, 0);
}

#[tokio::test]
async fn committed_request_copy_deadline_is_not_recomputed_from_later_path_timing() {
    let stream_id = StreamId(191);
    let context =
        client_test_context_with_paths(&["tcp://127.0.0.1:10361", "quic://127.0.0.1:10362"]);
    let (owner_commands, mut owner_receivers) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream_with_underlay(stream_id, UnderlayProtocol::Tcp, 0, owner_commands),
        8,
    );
    consume_client_path_proof_for_test(&mut owner_receivers);
    let owner = remotes.paths[0].instance();
    let (copy_commands, mut copy_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        remotes.attach(opened_test_relay_stream_with_underlay(
            stream_id,
            UnderlayProtocol::Udp,
            0,
            copy_commands,
        )),
        ReliableRelayAttachOutcome::Attached,
    );
    consume_client_path_proof_for_test(&mut copy_receivers);
    let copy = remotes
        .paths
        .iter()
        .find(|path| path.key().underlay == UnderlayProtocol::Udp)
        .expect("recovery target")
        .instance();
    seed_client_bulk_evidence_for_test(&context, owner);
    seed_client_bulk_evidence_for_test(&context, copy);

    let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
    let frame = send_stream
        .send_data(Bytes::from(vec![0x6d; 4096]))
        .expect("retained original data");
    let mut sender = RequestSenderService::new(stream_id);
    sender.record_original_frame_for_test(owner, &frame);
    assert!(sender.multipath.mark_path_stale(owner));
    let committed_target_interval = crate::model::timing::reliable_data_retransmission_interval(
        Some(copy.key.underlay),
        context.reliable_path_snapshot_for_instance(copy),
    );
    let owner_interval_before_commit = crate::model::timing::reliable_data_retransmission_interval(
        Some(owner.key.underlay),
        context.reliable_path_snapshot_for_instance(owner),
    );
    assert_ne!(
        owner_interval_before_commit, committed_target_interval,
        "the fixture must distinguish the stale owner clock from the selected-copy clock",
    );
    let mut sender_queue = ReliableRelaySenderQueue::default();
    assert!(
        sender
            .drive_request_path_recovery(
                &mut sender_queue,
                &context,
                &remotes,
                &send_stream,
                TrafficClass::Throughput,
            )
            .queued,
        "stale retained OriginalData must enter the production recovery queue",
    );
    let accepted_before = Instant::now();
    let dispatch = sender
        .dispatch_client_queued_work(
            &context,
            TrafficClass::Throughput,
            &mut remotes,
            &mut send_stream,
            &mut sender_queue,
            4096,
            ReliableDataAckFrontierState::Live,
        )
        .await
        .expect("actual queued carrier command commitment");
    let accepted_after = Instant::now();
    let ClientQueuedDispatch::Reinjection {
        payload_bytes: 4096,
        accepted_copy_deadline: committed_deadline,
    } = dispatch
    else {
        panic!("queued path recovery must commit one exact reinjection: {dispatch:?}");
    };
    assert!(sender_queue.is_empty());
    assert!(
        committed_deadline >= accepted_before + committed_target_interval
            && committed_deadline <= accepted_after + committed_target_interval,
        "the accepted deadline must use the selected exact carrier snapshot at commitment",
    );

    let committed =
        sender
            .multipath
            .path_recovery_state(&context, &remotes, owner, TrafficClass::Throughput);
    assert_eq!(committed.retry_deadline, Some(committed_deadline));
    let accepted_at = committed_deadline
        .checked_sub(committed_target_interval)
        .expect("committed deadline retains its accepted-copy epoch");
    context.mark_relay_path_proof_observation(
        owner.key.underlay,
        owner.key.index,
        owner.path_instance_id,
        crate::runtime::path::PathProofObservation {
            proof_id: u64::MAX - 1,
            elapsed: Duration::from_secs(5),
            sent_at: Instant::now(),
        },
    );
    let later_owner_interval = crate::model::timing::reliable_data_retransmission_interval(
        Some(owner.key.underlay),
        context.reliable_path_snapshot_for_instance(owner),
    );
    assert_ne!(
        later_owner_interval, owner_interval_before_commit,
        "the stale owner's actual timing model must change",
    );
    assert_ne!(
        Some(accepted_at + later_owner_interval),
        committed.retry_deadline,
        "the legacy stale-owner dynamic clock would move away from the committed selected-copy deadline",
    );
    assert_eq!(
        sender
            .multipath
            .path_recovery_state(&context, &remotes, owner, TrafficClass::Throughput)
            .retry_deadline,
        committed.retry_deadline,
        "later stale-owner timing cannot move an accepted copy's absolute deadline",
    );
    context.mark_relay_path_proof_observation(
        copy.key.underlay,
        copy.key.index,
        copy.path_instance_id,
        crate::runtime::path::PathProofObservation {
            proof_id: u64::MAX,
            elapsed: Duration::from_secs(5),
            sent_at: Instant::now(),
        },
    );
    let later_dynamic_interval = crate::model::timing::reliable_data_retransmission_interval(
        Some(copy.key.underlay),
        context.reliable_path_snapshot_for_instance(copy),
    );
    assert!(
        later_dynamic_interval > committed_target_interval,
        "the selected carrier's actual RTT model must change enough to expose dynamic recomputation",
    );
    let after_timing_growth =
        sender
            .multipath
            .path_recovery_state(&context, &remotes, owner, TrafficClass::Throughput);
    assert_eq!(
        after_timing_growth.retry_deadline, committed.retry_deadline,
        "later RTT/jitter/model growth cannot postpone an accepted copy's absolute deadline",
    );
}

#[tokio::test]
async fn client_recv_progress_backpressure_is_retryable_not_stream_fatal() {
    let stream_id = StreamId(92);
    let context = client_test_context();
    let (commands, mut receivers) = reliable_path_command_channels(1);
    commands
        .try_enqueue_admitted_frame(
            Frame::StreamAck {
                stream_id,
                complete: false,
                ranges: Vec::new(),
            },
            TrafficClass::Control,
        )
        .expect("prefill priority queue");
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, commands), 4);
    let mut recv_stream = ReliableRecvStream::new(stream_id, MuxLimits::default());
    recv_stream
        .receive_data(0, Bytes::from_static(b"reply"))
        .expect("receive response bytes");
    let mut progress = ReliableRecvProgress::default();
    let mut sender = RequestSenderService::new(stream_id);

    let sent = sender
        .send_recv_progress(
            &mut remotes,
            &context,
            &mut recv_stream,
            &mut progress,
            RelayRecvProgressSend::new(None, TrafficClass::Throughput, false),
        )
        .await
        .expect("recv progress backpressure should not close the product stream");

    assert!(!sent, "blocked advisory progress must report no frame sent");
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
    ));

    let retried = remotes.retry_pending_stream_ack();
    assert!(retried.published);
    assert!(!retried.pending);
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
    ));

    let (replacement_commands, mut replacement_rx) = reliable_path_command_channels(4);
    remotes.attach(opened_test_relay_stream(stream_id, 1, replacement_commands));
    consume_client_path_proof_for_test(&mut replacement_rx);
    let replacement = remotes.retry_pending_stream_ack();
    assert!(replacement.published);
    assert!(!replacement.pending);
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut replacement_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
    ));
}

#[tokio::test]
async fn client_stream_ack_publication_resumes_at_the_exact_cumulative_chunk() {
    let stream_id = StreamId(920);
    let (commands, mut receivers) = reliable_path_command_channels(1);
    commands
        .try_enqueue_admitted_frame(
            Frame::StreamAck {
                stream_id,
                complete: false,
                ranges: Vec::new(),
            },
            TrafficClass::Control,
        )
        .expect("prefill priority queue");
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, commands), 4);
    let chunks = vec![
        Frame::StreamAck {
            stream_id,
            complete: false,
            ranges: vec![OffsetRange { start: 0, end: 4 }],
        },
        Frame::StreamAck {
            stream_id,
            complete: false,
            ranges: vec![OffsetRange { start: 8, end: 12 }],
        },
    ];

    let blocked = remotes.publish_stream_ack(1, chunks);
    assert!(!blocked.published);
    assert!(blocked.pending);
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck {
            ranges,
            ..
        })) if ranges.is_empty()
    ));

    let first = remotes.retry_pending_stream_ack();
    assert!(first.accepted);
    assert!(!first.published);
    assert!(first.pending);
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck {
            ranges,
            ..
        })) if ranges == vec![OffsetRange { start: 0, end: 4 }]
    ));

    let second = remotes.retry_pending_stream_ack();
    assert!(second.accepted);
    assert!(second.published);
    assert!(!second.pending);
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck {
            ranges,
            ..
        })) if ranges == vec![OffsetRange { start: 8, end: 12 }]
    ));
}

#[tokio::test]
async fn client_max_data_credit_commits_only_after_control_queue_accepts_it() {
    let stream_id = StreamId(97);
    let context = client_test_context();
    let (commands, mut receivers) = reliable_path_command_channels(1);
    commands
        .try_enqueue_admitted_frame(
            Frame::StreamAck {
                stream_id,
                complete: false,
                ranges: Vec::new(),
            },
            TrafficClass::Control,
        )
        .expect("prefill priority queue");
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, commands), 4);
    let original_instance = remotes.paths[0].instance();
    let mut recv_stream =
        ReliableRecvStream::new_with_initial_max_offset(stream_id, MuxLimits::default(), 0);
    let mut progress = ReliableRecvProgress::default();
    let mut sender = RequestSenderService::new(stream_id);

    let sent = sender
        .send_recv_progress(
            &mut remotes,
            &context,
            &mut recv_stream,
            &mut progress,
            RelayRecvProgressSend::new(None, TrafficClass::Throughput, false),
        )
        .await
        .expect("blocked MAX_DATA publication is retryable");

    assert!(!sent);
    assert_eq!(
        recv_stream.published_max_offset(),
        0,
        "credit must remain unavailable until the frame is queued"
    );
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
    ));

    drop(remotes.remove_path_instance(original_instance));
    let (replacement_commands, mut replacement_rx) = reliable_path_command_channels(4);
    remotes.attach(opened_test_relay_stream(stream_id, 0, replacement_commands));
    consume_client_path_proof_for_test(&mut replacement_rx);
    assert!(
        try_recv_reliable_path_priority_command(&mut replacement_rx).is_none(),
        "neutral attachment cannot publish receive credit outside the actor commit"
    );
    let publication = remotes.retry_pending_max_data();
    let published_offset = publication
        .published_offset
        .expect("replacement must replay retained MAX_DATA through the actor");
    recv_stream.commit_max_data(published_offset);
    assert!(!publication.pending);
    let Some(ReliablePathCommand::SendFrame(Frame::StreamMaxData {
        stream_id: published_stream_id,
        max_offset,
    })) = try_recv_reliable_path_priority_command(&mut replacement_rx)
    else {
        panic!("replacement retry must enqueue STREAM_MAX_DATA");
    };
    assert_eq!(published_stream_id, stream_id);
    assert_eq!(recv_stream.published_max_offset(), max_offset);
    assert_eq!(published_offset, max_offset);
    assert!(max_offset > 0);
    recv_stream
        .receive_data(published_offset.saturating_sub(1), Bytes::from_static(b"x"))
        .expect("data within replacement-published credit must remain admissible");
}

#[tokio::test]
async fn client_max_data_retries_only_the_blocked_attachment() {
    let stream_id = StreamId(98);
    let context = client_test_context();
    let (blocked_commands, mut blocked_rx) = reliable_path_command_channels(1);
    blocked_commands
        .try_enqueue_admitted_frame(
            Frame::StreamAck {
                stream_id,
                complete: false,
                ranges: Vec::new(),
            },
            TrafficClass::Control,
        )
        .expect("prefill first priority queue");
    let (available_commands, mut available_rx) = reliable_path_command_channels(4);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, blocked_commands), 4);
    remotes.attach(opened_test_relay_stream(stream_id, 1, available_commands));
    consume_client_path_proof_for_test(&mut available_rx);
    let mut recv_stream =
        ReliableRecvStream::new_with_initial_max_offset(stream_id, MuxLimits::default(), 0);
    let mut progress = ReliableRecvProgress::default();
    let mut sender = RequestSenderService::new(stream_id);

    assert!(
        sender
            .send_recv_progress(
                &mut remotes,
                &context,
                &mut recv_stream,
                &mut progress,
                RelayRecvProgressSend::new(None, TrafficClass::Throughput, false),
            )
            .await
            .expect("one live attachment publishes shared credit")
    );
    let Some(ReliablePathCommand::SendFrame(Frame::StreamMaxData {
        max_offset: published,
        ..
    })) = try_recv_reliable_path_priority_command(&mut available_rx)
    else {
        panic!("available attachment must publish STREAM_MAX_DATA");
    };
    assert_eq!(recv_stream.published_max_offset(), published);
    assert!(remotes.has_pending_max_data_publication());

    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut blocked_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
    ));
    let retry = remotes.retry_pending_max_data();
    assert_eq!(retry.published_offset, Some(published));
    assert!(!retry.pending);
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut blocked_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamMaxData {
            max_offset,
            ..
        })) if max_offset == published
    ));
    assert!(
        try_recv_reliable_path_priority_command(&mut available_rx).is_none(),
        "an already-published attachment must not receive an unchanged duplicate"
    );

    let (replacement_commands, mut replacement_rx) = reliable_path_command_channels(4);
    remotes.attach(opened_test_relay_stream(stream_id, 2, replacement_commands));
    consume_client_path_proof_for_test(&mut replacement_rx);
    assert!(
        try_recv_reliable_path_priority_command(&mut replacement_rx).is_none(),
        "attachment itself cannot publish credit outside the receive owner"
    );
    let replacement_publication = remotes.retry_pending_max_data();
    assert_eq!(replacement_publication.published_offset, Some(published));
    assert!(!replacement_publication.pending);
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut replacement_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamMaxData {
            max_offset,
            ..
        })) if max_offset == published
    ));
}

#[tokio::test]
async fn client_recv_progress_uses_available_control_queue_instead_of_full_low_eta_path() {
    let stream_id = StreamId(93);
    let first_path = "tcp://127.0.0.1:10251"
        .parse::<PathSpec>()
        .expect("first path");
    let second_path = "tcp://127.0.0.1:10252"
        .parse::<PathSpec>()
        .expect("second path");
    let context = ClientPathContext::new(
        vec![first_path, second_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    let (first_commands, mut first_rx) = reliable_path_command_channels(1);
    first_commands
        .try_enqueue_admitted_frame(
            Frame::StreamAck {
                stream_id,
                complete: false,
                ranges: Vec::new(),
            },
            TrafficClass::Control,
        )
        .expect("prefill first priority queue");
    let (second_commands, mut second_rx) = reliable_path_command_channels(1);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, first_commands), 4);
    remotes.attach(opened_test_relay_stream(stream_id, 1, second_commands));
    consume_client_path_proof_for_test(&mut second_rx);
    let mut recv_stream = ReliableRecvStream::new(stream_id, MuxLimits::default());
    recv_stream
        .receive_data(0, Bytes::from_static(b"reply"))
        .expect("receive response bytes");
    let mut progress = ReliableRecvProgress::default();
    let mut sender = RequestSenderService::new(stream_id);

    let sent = sender
        .send_recv_progress(
            &mut remotes,
            &context,
            &mut recv_stream,
            &mut progress,
            RelayRecvProgressSend::new(None, TrafficClass::Throughput, false),
        )
        .await
        .expect("available alternate control queue should accept recv progress");

    assert!(sent);
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut first_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
    ));
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut second_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
    ));
}

#[tokio::test]
async fn first_nonempty_request_data_acquires_load_but_empty_data_does_not() {
    let stream_id = StreamId(123);
    let context = client_test_context();
    let key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    let opening_lease = context
        .reserve_relay_path_load(key, TrafficClass::Throughput)
        .expect("prospective initial-open load");
    let (commands, _receivers) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream(stream_id, 0, commands).with_load_lease(opening_lease),
        8,
    );
    let instance = remotes.paths[0].instance();
    context.install_relay_path_instance_for_test(instance);
    assert_eq!(
        context.health().lock().expect("path health").tcp[0].active_flows,
        0,
        "attachment membership is idle after the open transaction commits",
    );
    let mut sender = RequestSenderService::new(stream_id);

    sender
        .send_frame(
            &context,
            &mut remotes,
            Frame::StreamData {
                stream_id,
                offset: 0,
                payload: Bytes::new(),
            },
            RelaySendCause::StreamData,
            Some(TrafficClass::Throughput),
        )
        .await
        .expect("empty stream data remains harmless carrier work");
    assert!(!remotes.paths[0].has_load_reservation());
    assert_eq!(
        context.health().lock().expect("path health").tcp[0].active_flows,
        0,
        "empty StreamData has no OriginalData extent and cannot leak active demand",
    );

    sender
        .send_frame(
            &context,
            &mut remotes,
            Frame::StreamData {
                stream_id,
                offset: 0,
                payload: Bytes::from_static(b"first-original"),
            },
            RelaySendCause::StreamData,
            Some(TrafficClass::Throughput),
        )
        .await
        .expect("first OriginalData assignment");
    assert!(remotes.paths[0].has_load_reservation());
    assert_eq!(
        context.health().lock().expect("path health").tcp[0].active_flows,
        1,
        "the existing CAS/reservation transaction publishes first active demand",
    );
}

#[tokio::test]
async fn client_path_failure_releases_path_load_without_cleanup_queue_wait() {
    let stream_id = StreamId(124);
    let context = Arc::new(client_test_context());
    let (commands, mut receivers) = reliable_path_command_channels(1);
    commands
        .send_control(ReliablePathCommand::CloseStream(StreamId(999)))
        .await
        .expect("prefill cleanup control queue");
    let load_lease = context
        .reserve_relay_path_load(
            RelayPathKey {
                underlay: UnderlayProtocol::Tcp,
                index: 0,
            },
            TrafficClass::Throughput,
        )
        .expect("initial path load");
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream(stream_id, 0, commands).with_load_lease(load_lease),
        1,
    );
    let instance = remotes.paths[0].instance();
    context.install_relay_path_instance_for_test(instance);
    assert_eq!(
        context.health().lock().expect("path health lock").tcp[0].active_flows,
        0,
        "attachment commit must end the prospective open reservation"
    );
    let product_lease = context
        .try_reserve_relay_path_load_if_unchanged(instance, TrafficClass::Throughput, 0, 0)
        .expect("reserve active OriginalData demand");
    remotes.commit_path_instance_load_claim(instance, product_lease);
    assert_eq!(
        context.health().lock().expect("path health lock").tcp[0].active_flows,
        1
    );
    let mut sender = RequestSenderService::new(stream_id);

    assert!(sender.fail_client_path_instance(&context, &mut remotes, instance));

    assert_eq!(
        context.health().lock().expect("path health lock").tcp[0].active_flows,
        0,
        "a detached path must release its load synchronously"
    );
    assert!(remotes.is_empty());
    assert!(matches!(
        recv_reliable_path_command(&mut receivers).await,
        Some(ReliablePathCommand::SendFrame(Frame::StreamDetach { stream_id: id }))
            if id == stream_id
    ));
    assert!(matches!(
        recv_reliable_path_command(&mut receivers).await,
        Some(ReliablePathCommand::CloseStream(id)) if id == stream_id
    ));
    assert!(matches!(
        recv_reliable_path_command(&mut receivers).await,
        Some(ReliablePathCommand::CloseStream(StreamId(999)))
    ));
    assert!(matches!(
        recv_reliable_path_command(&mut receivers).await,
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
    ));
}

#[tokio::test]
async fn client_path_failure_releases_optional_load_without_cleanup_queue_wait() {
    let stream_id = StreamId(125);
    let context = Arc::new(client_test_context_with_paths(&[
        "tcp://127.0.0.1:10331?initial-srtt-s=0.02&initial-rate-mbps=500",
        "tcp://127.0.0.1:10332?initial-srtt-s=0.02&initial-rate-mbps=500",
    ]));
    let (service_commands, _service_rx) = reliable_path_command_channels(1);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, service_commands), 2);
    let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(1);
    candidate_commands
        .send_control(ReliablePathCommand::CloseStream(StreamId(999)))
        .await
        .expect("prefill cleanup control queue");
    remotes.attach_candidate(opened_test_relay_stream(stream_id, 1, candidate_commands));
    let candidate = remotes
        .path_instance_for_key(RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 1,
        })
        .expect("additional candidate");
    context.install_relay_path_instance_for_test(candidate);
    let lease = context
        .try_reserve_relay_path_load_if_unchanged(candidate, TrafficClass::Throughput, 0, 0)
        .expect("reserve optional path load");
    remotes.commit_path_instance_load_claim(candidate, lease);
    let mut sender = RequestSenderService::new(stream_id);

    assert!(sender.fail_client_path_instance(&context, &mut remotes, candidate));

    assert_eq!(
        context.health().lock().expect("path health lock").tcp[1].active_flows,
        0,
        "a removed optional path must release load synchronously"
    );
    assert!(matches!(
        recv_reliable_path_command(&mut candidate_rx).await,
        Some(ReliablePathCommand::SendFrame(Frame::StreamDetach { stream_id: id }))
            if id == stream_id
    ));
    assert!(matches!(
        recv_reliable_path_command(&mut candidate_rx).await,
        Some(ReliablePathCommand::CloseStream(id)) if id == stream_id
    ));
    assert!(matches!(
        recv_reliable_path_command(&mut candidate_rx).await,
        Some(ReliablePathCommand::CloseStream(StreamId(999)))
    ));
}

#[test]
fn request_reinjection_final_enqueue_is_percentage_invariant_and_exactly_accounted() {
    let mux_limits = MuxLimits::default();
    let stream_id = StreamId(93);
    let startup_floor = sender_optional_reinjection_startup_floor_bytes(mux_limits);
    let delivered_bytes = startup_floor.saturating_mul(100);
    let payload_bytes = startup_floor;
    let default_percent = MppPerformanceConfig::default().optional_reinjection_budget_percent;

    let outcomes = [0, default_percent, 200].map(|percent| {
        let mut sender = RequestSenderService::new_with_performance(
            stream_id,
            MppPerformanceConfig {
                optional_reinjection_budget_percent: percent,
            },
        );
        sender.record_delivered_data_for_test(delivered_bytes);
        sender.record_reinjection_for_test(startup_floor);
        let accounted_before = sender.optional_reinjection.reinjected_bytes();
        let mut sender_queue = ReliableRelaySenderQueue::default();
        sender.enqueue_reinjection_frame_with_priority(
            &mut sender_queue,
            Frame::StreamData {
                stream_id,
                offset: 0,
                payload: Bytes::from(vec![0x33; payload_bytes]),
            },
            RelaySendCause::AckGapReinjection,
            false,
        );
        let accounted_delta = sender
            .optional_reinjection
            .reinjected_bytes()
            .saturating_sub(accounted_before);
        (percent, sender_queue.reinjection_bytes(), accounted_delta)
    });

    for (percent, queued_bytes, accounted_delta) in outcomes {
        assert_eq!(
            (queued_bytes, accounted_delta),
            (payload_bytes, payload_bytes as u64),
            "a configured traffic percentage may not reject an already-authorized request recovery frame or change its exact accounting: percent={percent}",
        );
    }
}

#[test]
fn client_exact_failure_reinjection_remains_critical_and_exactly_accounted() {
    let stream_id = StreamId(95);
    let mut sender = RequestSenderService::new_with_performance(
        stream_id,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 0,
        },
    );
    let mut sender_queue = ReliableRelaySenderQueue::default();
    let payload = Bytes::from_static(b"tail");
    let frame = Frame::StreamData {
        stream_id,
        offset: 0,
        payload: payload.clone(),
    };
    let accounted_before = sender.optional_reinjection.reinjected_bytes();
    sender.enqueue_critical_reinjection_frame(
        &mut sender_queue,
        frame,
        RelaySendCause::PathFailureReinjection,
    );
    assert_eq!(
        sender.optional_reinjection.reinjected_bytes(),
        accounted_before + payload.len() as u64,
    );
    let (lane, work) = sender_queue
        .pop_front()
        .expect("exact failure recovery remains queued");
    assert_eq!(lane, ReliableWorkClass::Reinjection);
    assert!(matches!(
        work.kind,
        ReliableRelayQueuedWorkKind::Reinjection {
            cause: RelaySendCause::PathFailureReinjection,
            ..
        }
    ));
}

#[tokio::test]
async fn equal_expiry_request_candidates_preserve_one_nonstale_survivor() {
    let stream_id = StreamId(197);
    let context =
        client_test_context_with_paths(&["tcp://127.0.0.1:10251", "tcp://127.0.0.1:10252"]);
    let (first_commands, mut first_receivers) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, first_commands), 8);
    let first = remotes.paths[0].instance();
    let (second_commands, mut second_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream(stream_id, 1, second_commands));
    let second = remotes
        .paths
        .iter()
        .find(|path| path.key().index == 1)
        .expect("second request attachment")
        .instance();
    consume_client_path_proof_for_test(&mut first_receivers);
    consume_client_path_proof_for_test(&mut second_receivers);
    for instance in [first, second] {
        context.install_relay_path_instance_for_test(instance);
    }
    let mut sender = RequestSenderService::new(stream_id);

    assert!(sender.mark_request_path_stale(&context, &remotes, first, TrafficClass::Throughput,));
    assert!(
        !sender.mark_request_path_stale(&context, &remotes, second, TrafficClass::Throughput,),
        "request candidates returned at one deadline are revalidated serially, so the last live attachment survives",
    );
    assert!(sender.request_path_is_stale(first));
    assert!(!sender.request_path_is_stale(second));
}

#[tokio::test]
async fn exhausted_optional_budget_still_allows_one_charged_requalification_quantum() {
    let mux_limits = MuxLimits::default();
    let stream_id = StreamId(96);
    let context =
        client_test_context_with_paths(&["tcp://127.0.0.1:10251", "quic://127.0.0.1:10252"]);
    let (stale_commands, mut stale_receivers) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, stale_commands), 8);
    let stale = remotes.paths[0].instance();
    let (healthy_commands, mut healthy_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        0,
        healthy_commands,
    ));
    let healthy = remotes
        .paths
        .iter()
        .find(|path| path.key().underlay == UnderlayProtocol::Udp)
        .expect("healthy attachment")
        .instance();
    consume_client_path_proof_for_test(&mut stale_receivers);
    consume_client_path_proof_for_test(&mut healthy_receivers);
    context.install_relay_path_instance_for_test(stale);
    context.install_relay_path_instance_for_test(healthy);

    let mut send_stream = ReliableSendStream::new(stream_id, mux_limits);
    let source = send_stream
        .send_data(Bytes::from(vec![0x61; 4096]))
        .expect("retained healthy source");
    let mut sender = RequestSenderService::new_with_performance(
        stream_id,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 1,
        },
    );
    sender.record_original_frame_for_test(healthy, &source);
    assert!(sender.mark_request_path_stale(&context, &remotes, stale, TrafficClass::Throughput,));

    let startup_floor = sender_optional_reinjection_startup_floor_bytes(mux_limits);
    sender
        .optional_reinjection
        .record_reinjection(startup_floor);
    assert_eq!(sender.optional_reinjection_budget_remaining(mux_limits), 0);
    let charged_before = sender.optional_reinjection.reinjected_bytes();
    assert!(
        sender
            .try_send_requalification_probe(
                &context,
                &remotes,
                &send_stream,
                TrafficClass::Throughput,
            )
            .expect("critical requalification attempt")
            .published_payload_bytes()
            .is_some()
    );
    assert_eq!(
        sender.optional_reinjection.reinjected_bytes(),
        charged_before + 4096,
        "critical liveness remains charged as optional-traffic debt"
    );
    assert!(matches!(
        try_recv_reliable_path_command(&mut stale_receivers),
        Some(ReliablePathCommand::SendFrame(
            Frame::StreamRequalifyData { .. }
        ))
    ));
    let pending = sender
        .try_send_requalification_probe(&context, &remotes, &send_stream, TrafficClass::Throughput)
        .expect("one pending transaction is not an error");
    assert!(pending.published_payload_bytes().is_none());
    assert!(!pending.is_capacity_blocked());
    assert_eq!(
        sender.optional_reinjection.reinjected_bytes(),
        charged_before + 4096
    );
}
