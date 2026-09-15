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
use crate::runtime::sender::PreparedOriginalClaim;
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

fn try_recv_request_command_after_path_proofs_for_test(
    receivers: &mut crate::runtime::path::commands::ReliablePathCommandReceivers,
) -> Option<ReliablePathCommand> {
    loop {
        let command = try_recv_reliable_path_command(receivers)?;
        if let ReliablePathCommand::SendFrame(Frame::PathProofData { payload, .. }) = &command {
            // Installing the fixture's exact live incarnation can change its
            // proof generation after attachment. Actual claim preparation
            // retries that control proof; it is not Original source work.
            assert!(!payload.is_empty());
            receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&command));
            continue;
        }
        return Some(command);
    }
}

#[tokio::test]
async fn prepared_request_data_keeps_wire_horizon_unclaimed_until_writer_start() {
    // The actual actor producer publishes weak wake metadata only. No writer
    // is spawned or data command consumed, preserving the original RED premise.
    let stream_id = StreamId(714);
    let context = client_test_context_with_paths(&["tcp://127.0.0.1:10714"]);
    let limits = context.mux_limits;
    let command_capacity = crate::runtime::path::commands::reliable_path_command_queue(limits);
    let (commands, mut receivers) = reliable_path_command_channels(command_capacity);
    let (opened, _frames_tx) =
        opened_request_stream_with_retained_input(stream_id, 0, commands.clone());
    let (remotes, _remote_input) = ReliableRelayRemoteSet::new(opened, command_capacity);
    let proof = try_recv_reliable_path_priority_command(&mut receivers)
        .expect("attachment publishes its separate priority proof");
    assert!(matches!(
        proof,
        ReliablePathCommand::SendFrame(Frame::PathProofData { .. })
    ));
    receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&proof));
    let owner = remotes.paths[0].instance();
    context.install_relay_path_instance_for_test(owner);
    let sender = RequestSenderService::new(stream_id);
    let send_stream = ReliableSendStream::new(stream_id, limits);
    let mut queue = ReliableRelaySenderQueue::default();

    let admission = sender.reliable_stream_source_admission(
        &context,
        &remotes,
        TrafficClass::Throughput,
        reliable_relay_buffer_len(limits),
    );
    let selected = admission
        .selected_path
        .expect("the ordinary singleton target is eligible without fabricated rate proof");
    let quantum = crate::model::capacity::adaptive_reliable_relay_chunk_bytes(
        Some(selected),
        TrafficClass::Throughput,
        limits,
    );
    let source_bytes = 2 * quantum;
    assert!(command_capacity > 2);
    assert!(source_bytes <= admission.window_bytes);
    assert!(source_bytes <= limits.max_repair_bytes);
    assert!(source_bytes <= send_stream.send_credit_bytes());
    assert_eq!(
        remotes.paths.len(),
        1,
        "no alternate or migration is required"
    );
    assert!(commands.can_enqueue_lane_now(TrafficClass::Throughput));
    let source = Bytes::from(vec![0x71; source_bytes]);
    queue.push_data(source.clone());
    assert!(matches!(
        queue.front().map(|(_, work)| &work.kind),
        Some(ReliableRelayQueuedWorkKind::Data(payload)) if payload == &source
    ));
    assert_eq!(queue.data_bytes(), source_bytes);
    assert_eq!(send_stream.next_offset(), 0);
    assert!(
        sender
            .multipath
            .latest_unacked_ranges_for_path_instance(owner)
            .is_empty()
    );

    let shared = SharedRequestProduct::new(RequestProductState {
        sender_queue: queue,
        sender,
        send_stream,
        last_send_ack: Default::default(),
        remotes,
        prepared: RequestPreparedSource::new(TrafficClass::Throughput, quantum),
    });
    let _actor_lifetime = shared.actor_lifetime();
    let mut state = shared.lock();
    for _ in 0..2 {
        crate::runtime::relay::control::publish_prepared_request_work(
            &mut state,
            &shared,
            &context,
            TrafficClass::Throughput,
            quantum,
            true,
        );
    }
    assert_eq!(
        state.sender_queue.data_bytes() + state.send_stream.reinjection_bytes(),
        source_bytes
    );
    assert_eq!(state.sender_queue.data_bytes(), source_bytes);
    assert_eq!(state.sender_queue.reinjection_bytes(), 0);
    assert_eq!(commands.pending_bytes(), 0, "notices carry no payload debt");
    assert_eq!(
        commands.writer_pending_bytes(),
        0,
        "no command reached a writer"
    );
    assert!(commands.can_enqueue_lane_now(TrafficClass::Throughput));

    let assigned = state
        .sender
        .multipath
        .latest_unacked_ranges_for_path_instance(owner);
    assert_eq!(
        (state.send_stream.next_offset(), assigned.is_empty()),
        (0, true),
        "prepared but unconsumed source must not advance the wire horizon or own \
         exact Original ranges on a future writer; assigned={assigned:?}"
    );
}

#[tokio::test]
async fn prepared_request_ready_alternate_claims_one_shared_prefix_without_queue_binding() {
    prepared_request_ready_alternate_claims_shared_prefix(false).await;
}

#[tokio::test]
async fn prepared_request_stale_writer_waits_for_occupied_nonstale_alternate() {
    prepared_request_ready_alternate_claims_shared_prefix(true).await;
}

async fn prepared_request_ready_alternate_claims_shared_prefix(stale_a: bool) {
    let stream_id = StreamId(718);
    let context =
        client_test_context_with_paths(&["tcp://127.0.0.1:10718", "tcp://127.0.0.1:10719"]);
    let limits = context.mux_limits;
    let capacity = crate::runtime::path::commands::reliable_path_command_queue(limits);
    let (a_commands, mut a_receivers) = reliable_path_command_channels(capacity);
    let (b_commands, mut b_receivers) = reliable_path_command_channels(capacity);
    let (a_opened, _a_input) =
        opened_request_stream_with_retained_input(stream_id, 0, a_commands.clone());
    let (b_opened, _b_input) =
        opened_request_stream_with_retained_input(stream_id, 1, b_commands.clone());
    let (mut remotes, _remote_input) = ReliableRelayRemoteSet::new(a_opened, capacity);
    assert_eq!(
        remotes.attach(b_opened),
        ReliableRelayAttachOutcome::Attached
    );
    consume_client_path_proof_for_test(&mut a_receivers);
    consume_client_path_proof_for_test(&mut b_receivers);
    let a = remotes
        .paths
        .iter()
        .find(|path| path.key().index == 0)
        .unwrap()
        .instance();
    let b = remotes
        .paths
        .iter()
        .find(|path| path.key().index == 1)
        .unwrap()
        .instance();
    context.install_relay_path_instance_for_test(a);
    context.install_relay_path_instance_for_test(b);
    // Additional bulk admission requires real path validation, not merely a
    // ready writer. Installing the exact native identities invalidates the
    // earlier challenges, so acknowledge the current producer's challenges.
    remotes.retry_pending_path_proofs(&context);
    for (instance, receivers) in [(a, &mut a_receivers), (b, &mut b_receivers)] {
        let command = try_recv_reliable_path_priority_command(receivers)
            .expect("current attachment challenge");
        let ReliablePathCommand::SendFrame(frame @ Frame::PathProofData { .. }) = &command else {
            panic!("validation consumes the actual path challenge");
        };
        let mut tracker = crate::runtime::path::PathProofTracker::from_limits(limits);
        tracker.record_sent_frame(frame);
        let Frame::PathProofData {
            path_id,
            proof_id,
            payload,
        } = frame
        else {
            unreachable!();
        };
        let path = remotes
            .paths
            .iter()
            .find(|path| path.instance() == instance)
            .expect("exact attached output");
        assert_eq!(path.path_proof_id, Some(*proof_id));
        let receipt = tracker
            .acknowledge(*path_id, *proof_id, payload.len().try_into().unwrap())
            .expect("the exact challenge has an actual tracked receipt");
        context.mark_relay_path_proof_observation(
            instance.key.underlay,
            instance.key.index,
            instance.path_instance_id,
            receipt,
        );
        assert!(context.relay_path_has_fresh_proof(
            instance.key.underlay,
            instance.key.index,
            *proof_id,
            path.attached_at,
        ));
        receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&command));
    }
    let sender = RequestSenderService::new(stream_id);
    let admission = sender.reliable_stream_source_admission(
        &context,
        &remotes,
        TrafficClass::Throughput,
        reliable_relay_buffer_len(limits),
    );
    let quantum = crate::model::capacity::adaptive_reliable_relay_chunk_bytes(
        admission.selected_path,
        TrafficClass::Throughput,
        limits,
    );
    let total = 3 * quantum;
    assert!(total <= admission.window_bytes);
    let mut queue = ReliableRelaySenderQueue::default();
    queue.push_data(Bytes::from(vec![0x78; total]));
    let shared = SharedRequestProduct::new(RequestProductState {
        sender_queue: queue,
        sender,
        send_stream: ReliableSendStream::new(stream_id, limits),
        last_send_ack: Default::default(),
        remotes,
        prepared: RequestPreparedSource::new(TrafficClass::Throughput, quantum),
    });
    let actor_lifetime = shared.actor_lifetime();
    {
        let mut state = shared.lock();
        crate::runtime::relay::control::publish_prepared_request_work(
            &mut state,
            &shared,
            &context,
            TrafficClass::Throughput,
            quantum,
            true,
        );
        assert_eq!(state.send_stream.next_offset(), 0);
        assert_eq!(state.sender_queue.data_bytes(), total);
        assert_eq!(state.prepared.last_claimed_at, None);
    }
    let take_notice =
        |receivers: &mut crate::runtime::path::commands::ReliablePathCommandReceivers| {
            loop {
                let command =
                    try_recv_reliable_path_command(receivers).expect("actual actor notice");
                match command {
                    ReliablePathCommand::PreparedOriginal(work) => break work,
                    ReliablePathCommand::SendFrame(Frame::PathProofData { .. }) => {
                        receivers.release_pending_command_bytes(
                            reliable_path_command_pending_bytes(&command),
                        );
                    }
                    _ => panic!("prepared source must not become an ordinary payload command"),
                }
            }
        };
    assert!(a_commands.writer_boundary().snapshot().is_none());
    let b_work = take_notice(&mut b_receivers);
    let b_ready = b_receivers
        .writer_ready_boundary(b.path_instance_id)
        .expect("actual B writer boundary");
    let first_claim_started = std::time::Instant::now();
    let PreparedOriginalClaim::Claimed(first) = b_work.try_claim(b_ready) else {
        panic!("the only ready exact writer must claim the first admitted shared prefix");
    };
    let first_claim_finished = std::time::Instant::now();
    let first_claimed_at = shared
        .lock()
        .prepared
        .last_claimed_at
        .expect("successful first claim timestamp");
    assert!(first_claim_started <= first_claimed_at && first_claimed_at <= first_claim_finished);
    assert_eq!(
        reliable_stream_frame_extent(&first),
        Some((0, quantum as u64, quantum))
    );
    let charged = b_receivers.register_claimed_writer_frame(&first);
    assert_eq!(b_commands.writer_pending_bytes(), charged as u64);
    assert!(charged >= quantum);
    assert_eq!(a_commands.pending_bytes(), 0);
    {
        let state = shared.lock();
        assert_eq!(state.send_stream.next_offset(), quantum as u64);
        assert_eq!(state.send_stream.reinjection_bytes(), quantum);
        assert_eq!(state.sender_queue.data_bytes(), 2 * quantum);
        assert_eq!(
            state.sender_queue.data_bytes() + state.send_stream.reinjection_bytes(),
            total
        );
        assert!(
            state
                .sender
                .multipath
                .latest_unacked_ranges_for_path_instance(a)
                .is_empty()
        );
        assert_eq!(
            state
                .sender
                .multipath
                .latest_unacked_ranges_for_path_instance(b),
            vec![OffsetRange {
                start: 0,
                end: quantum as u64
            }]
        );
    }
    if stale_a {
        // Exercise the validated stale consumer, not the persistence timer.
        // B is structurally admitted and non-stale while its real claimed
        // writer frame remains owned; temporary Ready absence must not undo A's
        // placement withdrawal.
        assert!(b_commands.writer_boundary().snapshot().is_none());
        assert_eq!(b_commands.writer_pending_bytes(), charged as u64);
        {
            let mut state = shared.lock();
            let RequestProductState {
                sender, remotes, ..
            } = &mut *state;
            assert!(
                sender.mark_request_path_stale(&context, remotes, a, TrafficClass::Throughput,)
            );
            assert!(sender.request_path_is_stale(a));
            assert!(!sender.request_path_is_stale(b));
            assert!(
                remotes
                    .paths
                    .iter()
                    .find(|path| path.instance() == b)
                    .unwrap()
                    .stream
                    .product_admission_active()
            );
        }
        let a_work = take_notice(&mut a_receivers);
        let a_ready = a_receivers
            .writer_ready_boundary(a.path_instance_id)
            .expect("stale A really reaches an idle writer boundary");
        let a_receipt = a_ready.receipt();
        match a_work.try_claim(a_ready) {
            PreparedOriginalClaim::Blocked(wait) => drop(wait),
            PreparedOriginalClaim::Claimed(frame) => panic!(
                "stale A positively claimed forbidden fresh Original {:?} while non-stale B owned its writer transaction",
                reliable_stream_frame_extent(&frame),
            ),
            _ => panic!("demanded source must remain blocked for the non-stale writer"),
        }
        assert_eq!(a_commands.writer_boundary().snapshot(), Some(a_receipt));
        assert!(b_commands.writer_boundary().snapshot().is_none());
        assert_eq!(a_commands.pending_bytes(), 0);
        assert_eq!(b_commands.writer_pending_bytes(), charged as u64);
        {
            let state = shared.lock();
            assert_eq!(state.send_stream.next_offset(), quantum as u64);
            assert_eq!(state.sender_queue.data_bytes(), 2 * quantum);
            assert_eq!(state.send_stream.reinjection_bytes(), quantum);
            assert_eq!(state.sender_queue.reinjection_bytes(), 0);
            assert_eq!(state.prepared.last_claimed_at, Some(first_claimed_at));
            assert!(
                state
                    .sender
                    .multipath
                    .latest_unacked_ranges_for_path_instance(a)
                    .is_empty()
            );
            assert_eq!(
                state
                    .sender
                    .multipath
                    .latest_unacked_ranges_for_path_instance(b),
                vec![OffsetRange {
                    start: 0,
                    end: quantum as u64
                }]
            );
        }
        // Finish B's actual writer ownership, then invoke its existing weak
        // callback at a new real Ready epoch. No deadline or retry loop is used.
        b_receivers.release_pending_command_bytes(charged);
        assert_eq!(b_commands.writer_pending_bytes(), 0);
        let b_ready = b_receivers
            .writer_ready_boundary(b.path_instance_id)
            .expect("released B can offer the next native writer boundary");
        let PreparedOriginalClaim::Claimed(second) = b_work.try_claim(b_ready) else {
            panic!("the released non-stale writer must claim the retained next prefix");
        };
        assert_eq!(
            reliable_stream_frame_extent(&second),
            Some((quantum as u64, (2 * quantum) as u64, quantum)),
        );
        let Frame::StreamData { payload, .. } = &second else {
            panic!("the callback must return the actual Original payload");
        };
        assert_eq!(payload.as_ref(), vec![0x78; quantum].as_slice());
        let second_charge = b_receivers.register_claimed_writer_frame(&second);
        assert_eq!(b_commands.writer_pending_bytes(), second_charge as u64);
        {
            let state = shared.lock();
            assert_eq!(state.send_stream.next_offset(), (2 * quantum) as u64);
            assert_eq!(state.sender_queue.data_bytes(), quantum);
            assert_eq!(state.send_stream.reinjection_bytes(), 2 * quantum);
            assert_eq!(state.sender_queue.reinjection_bytes(), 0);
            assert!(state.sender.request_path_is_stale(a));
            assert!(!state.sender.request_path_is_stale(b));
            assert!(
                state
                    .sender
                    .multipath
                    .latest_unacked_ranges_for_path_instance(a)
                    .is_empty()
            );
            assert!(state.prepared.last_claimed_at.unwrap() >= first_claimed_at);
        }
        b_receivers.release_pending_command_bytes(second_charge);
        assert_eq!(a_commands.pending_bytes(), 0);
        assert_eq!(b_commands.pending_bytes(), 0);
        drop(actor_lifetime);
        return;
    }
    // B's protected frame remains charged. It is not ready again; A claims
    // the next distinct prefix, never a copy or a transfer of B's wire owner.
    let a_work = take_notice(&mut a_receivers);
    let a_ready = a_receivers
        .writer_ready_boundary(a.path_instance_id)
        .expect("actual A writer boundary");
    {
        let state = shared.lock();
        let frame = state
            .send_stream
            .prepare_data(Bytes::from(vec![0x78; quantum]))
            .expect("current second Original extent");
        let inputs = super::multipath::RequestRelayNativeCapture::new(
            state.remotes.membership_generation(),
            &state.remotes.paths,
        )
        .resolve();
        let observation = state
            .sender
            .multipath
            .observe_original_claim_from_inputs(
                &context,
                &state.remotes,
                &frame,
                TrafficClass::Throughput,
                true,
                inputs,
            )
            .expect("current exact Native and Product observation");
        let plan = state
            .sender
            .multipath
            .plan_original_claim_from_observation(
                &context,
                &observation,
                &observation,
                &state.remotes,
                &frame,
                TrafficClass::Throughput,
                ReliableDataAckFrontierState::Live,
                &[a],
            )
            .expect("actual proof admits A within unchanged additional-path P/E authority");
        assert_eq!(plan.target().1, a);
        assert_eq!(state.send_stream.next_offset(), quantum as u64);
        assert_eq!(state.send_stream.reinjection_bytes(), quantum);
    }
    let second_claim_started = std::time::Instant::now();
    let PreparedOriginalClaim::Claimed(second) = a_work.try_claim(a_ready) else {
        panic!("the next ready output must use unchanged whole-frame Product authority");
    };
    let second_claim_finished = std::time::Instant::now();
    let second_claimed_at = shared
        .lock()
        .prepared
        .last_claimed_at
        .expect("successful second claim timestamp");
    assert!(
        second_claim_started <= second_claimed_at && second_claimed_at <= second_claim_finished
    );
    assert!(first_claimed_at <= second_claimed_at);
    assert_eq!(
        reliable_stream_frame_extent(&second),
        Some((quantum as u64, (2 * quantum) as u64, quantum))
    );
    let a_charged = a_receivers.register_claimed_writer_frame(&second);
    {
        let state = shared.lock();
        assert_eq!(state.send_stream.next_offset(), (2 * quantum) as u64);
        assert_eq!(state.send_stream.reinjection_bytes(), 2 * quantum);
        assert_eq!(state.sender_queue.data_bytes(), quantum);
        assert_eq!(
            state.sender_queue.data_bytes() + state.send_stream.reinjection_bytes(),
            total
        );
        assert_eq!(
            state
                .sender
                .multipath
                .latest_unacked_ranges_for_path_instance(a),
            vec![OffsetRange {
                start: quantum as u64,
                end: (2 * quantum) as u64
            }]
        );
    }
    b_receivers.release_pending_command_bytes(charged);
    a_receivers.release_pending_command_bytes(a_charged);
    assert_eq!(a_commands.pending_bytes(), 0);
    assert_eq!(b_commands.pending_bytes(), 0);

    // The logical actor owns cancellation even while this test retains the
    // shared owner and a previously admitted weak token. U must not be claimed.
    drop(actor_lifetime);
    let ready = b_receivers
        .writer_ready_boundary(b.path_instance_id)
        .unwrap();
    assert!(matches!(
        b_work.try_claim(ready),
        PreparedOriginalClaim::Empty
    ));
    let state = shared.lock();
    assert_eq!(state.send_stream.next_offset(), (2 * quantum) as u64);
    assert_eq!(state.sender_queue.data_bytes(), quantum);
    assert!(!state.prepared.claims_active);
    assert_eq!(
        state.prepared.last_claimed_at,
        Some(second_claimed_at),
        "the failed post-abort claim cannot manufacture progress time",
    );

    // Both actual successful claims precede this one actor observation. The
    // used helper takes no observation clock that could renew their age.
    let mut observed_offset = 0;
    let mut last_stream_at = first_claim_started;
    let observed_bytes = crate::runtime::relay::control::observe_prepared_request_claims(
        &state,
        &mut observed_offset,
        &mut last_stream_at,
    );
    assert_eq!(observed_bytes, 2 * quantum);
    assert_eq!(observed_offset, (2 * quantum) as u64);
    let recv_stream = crate::mux::stream::ReliableRecvStream::new(stream_id, limits);
    let anchor = crate::runtime::relay::lifecycle::reliable_relay_stall_progress_anchor(
        last_stream_at,
        first_claim_started,
        first_claim_started,
        &recv_stream,
        false,
        TrafficClass::Throughput,
        false,
        limits,
    );
    assert_eq!(
        anchor, second_claimed_at,
        "delayed actor observation must not renew the nonresponse fallback anchor past the actual successful claim",
    );
    assert_eq!(
        crate::runtime::relay::control::observe_prepared_request_claims(
            &state,
            &mut observed_offset,
            &mut last_stream_at,
        ),
        0,
        "the same claimed horizon is not new progress",
    );
    assert_eq!(last_stream_at, second_claimed_at);

    // A newer ACK/control event already owned by the actor must survive
    // observation of these older claims. The existing path-derived PTO only
    // orders the synthetic later event; no sleep or new threshold is involved.
    let newer_ack_at = second_claimed_at
        + crate::model::timing::transport_pto_from_snapshot(admission.selected_path);
    let mut unobserved_offset = 0;
    let mut newer_progress = newer_ack_at;
    assert_eq!(
        crate::runtime::relay::control::observe_prepared_request_claims(
            &state,
            &mut unobserved_offset,
            &mut newer_progress,
        ),
        2 * quantum,
    );
    assert_eq!(newer_progress, newer_ack_at);
}

#[tokio::test]
async fn fenced_request_source_commit_validates_and_preserves_product_state() {
    let stream_id = StreamId(715);
    let context = client_test_context_with_paths(&["tcp://127.0.0.1:10715"]);
    let limits = context.mux_limits;
    let capacity = crate::runtime::path::commands::reliable_path_command_queue(limits);
    let (commands, mut receivers) = reliable_path_command_channels(capacity);
    let (opened, _frames_tx) =
        opened_request_stream_with_retained_input(stream_id, 0, commands.clone());
    let (mut remotes, _remote_input) = ReliableRelayRemoteSet::new(opened, capacity);
    let proof = try_recv_reliable_path_priority_command(&mut receivers).unwrap();
    assert!(matches!(
        proof,
        ReliablePathCommand::SendFrame(Frame::PathProofData { .. })
    ));
    receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&proof));
    let owner = remotes.paths[0].instance();
    context.install_relay_path_instance_for_test(owner);
    let generation = remotes.membership_generation();
    let mut sender = RequestSenderService::new(stream_id);
    let mut send_stream = ReliableSendStream::new(stream_id, limits);
    let mut queue = ReliableRelaySenderQueue::default();
    let payload = Bytes::from(vec![0x72; 4096]);
    queue.push_data(payload.clone());
    // Exercise the shared Product commit used by actual prepared claims. This
    // is a transaction-unit control, not an obsolete Data-command publication
    // path or proof of physical writer scheduling.
    let prepared = send_stream.prepare_data(payload.clone()).unwrap();
    sender
        .multipath
        .prepare_original_claim(&context, &mut remotes, &prepared)
        .expect("normal Original preparation establishes the actual request state");
    let inputs = super::multipath::RequestRelayNativeCapture::new(
        remotes.membership_generation(),
        &remotes.paths,
    )
    .resolve();
    let observation = sender
        .multipath
        .observe_original_claim_from_inputs(
            &context,
            &remotes,
            &prepared,
            TrafficClass::Throughput,
            true,
            inputs,
        )
        .expect("actual singleton Product observation");
    let plan = sender
        .multipath
        .plan_original_claim_from_observation(
            &context,
            &observation,
            &observation,
            &remotes,
            &prepared,
            TrafficClass::Throughput,
            ReliableDataAckFrontierState::Live,
            &[owner],
        )
        .expect("unchanged actual singleton admission");
    assert_eq!(plan.target().1, owner);
    let position = plan
        .target_position_for_apply(&remotes, TrafficClass::Throughput)
        .unwrap();

    for source_replaced in [false, true] {
        let mut frame = send_stream.prepare_data(payload.clone()).unwrap();
        if source_replaced {
            queue
                .commit_front()
                .expect("replace the uncommitted source item");
            let replacement = Bytes::copy_from_slice(&payload);
            assert_ne!(replacement.as_ptr(), payload.as_ptr());
            queue.push_data(replacement);
        } else {
            let Frame::StreamData { offset, .. } = &mut frame else {
                unreachable!()
            };
            *offset += 1;
        }
        let result = sender.commit_fenced_frame_product(
            &context,
            &mut remotes,
            RequestFrameProductCommit {
                plan: &plan,
                frame: &frame,
                cause: RelaySendCause::StreamData,
                position,
                path_count: 1,
                reinjection_target_snapshot: None,
                request_load_claim: None,
            },
            Some(&mut RequestQueuedSourceCommit {
                send_stream: &mut send_stream,
                sender_queue: &mut queue,
            }),
        );
        if source_replaced {
            assert!(matches!(
                result,
                Err(RequestFrameAdmissionError::SourceChanged)
            ));
        } else {
            assert!(matches!(
                result,
                Err(RequestFrameAdmissionError::Source(
                    StreamError::InvalidPreparedFrame
                ))
            ));
        }
        assert_eq!(
            (send_stream.next_offset(), send_stream.reinjection_bytes()),
            (0, 0)
        );
        assert_eq!(queue.data_bytes(), payload.len());
        assert!(
            sender
                .multipath
                .latest_unacked_ranges_for_path_instance(owner)
                .is_empty()
        );
        assert_eq!(remotes.membership_generation(), generation);
        assert_eq!(remotes.paths.len(), 1);
        assert_eq!(remotes.paths[0].instance(), owner);
        assert_eq!(commands.pending_bytes(), 0);
        assert!(try_recv_request_command_after_path_proofs_for_test(&mut receivers).is_none());
    }

    let (_, queued) = queue.front().unwrap();
    let ReliableRelayQueuedWorkKind::Data(current) = &queued.kind else {
        unreachable!()
    };
    let frame = send_stream.prepare_data(current.clone()).unwrap();
    let request_load_claim = plan
        .load_expectation()
        .map(|(_, active, latency_sensitive)| {
            context
                .try_reserve_relay_path_load_if_unchanged(
                    owner,
                    TrafficClass::Throughput,
                    active,
                    latency_sensitive,
                )
                .expect("the rejected transactions did not consume the path load lease")
        });
    let result = sender
        .commit_fenced_frame_product(
            &context,
            &mut remotes,
            RequestFrameProductCommit {
                plan: &plan,
                frame: &frame,
                cause: RelaySendCause::StreamData,
                position,
                path_count: 1,
                reinjection_target_snapshot: None,
                request_load_claim,
            },
            Some(&mut RequestQueuedSourceCommit {
                send_stream: &mut send_stream,
                sender_queue: &mut queue,
            }),
        )
        .expect("the same Product core accepts the actual current source once");
    assert_eq!(result, (payload.len(), None));
    assert_eq!(queue.data_bytes(), 0);
    assert_eq!(
        (send_stream.next_offset(), send_stream.reinjection_bytes()),
        (payload.len() as u64, payload.len())
    );
    assert_eq!(
        sender
            .multipath
            .latest_unacked_ranges_for_path_instance(owner),
        vec![OffsetRange {
            start: 0,
            end: payload.len() as u64
        }]
    );
    assert!(try_recv_request_command_after_path_proofs_for_test(&mut receivers).is_none());
    assert_eq!(commands.pending_bytes(), 0);
}

#[tokio::test]
async fn prepared_native_rejection_preserves_uncommitted_source() {
    for close_receiver in [false, true] {
        let stream_id = StreamId(716);
        let context = client_test_context_with_paths(&["quic://127.0.0.1:10716"]);
        let limits = context.mux_limits;
        let capacity = crate::runtime::path::commands::reliable_path_command_queue(limits);
        let (commands, mut receivers) = reliable_path_command_channels(capacity);
        let (opened, native) = opened_test_relay_stream_with_native_source(
            stream_id,
            UnderlayProtocol::Udp,
            0,
            commands.clone(),
            crate::transport::RateHint::BitsPerSecond(25_000_000),
            7,
            Some(100_000_000),
        );
        let native = native.expect("actual attached QUIC authority");
        let (remotes, _remote_input) = ReliableRelayRemoteSet::new(opened, capacity);
        let instance = remotes.paths[0].instance();
        consume_client_path_proof_for_test(&mut receivers);
        seed_client_bulk_evidence_for_test(&context, instance);
        let payload = Bytes::from(vec![0x73; 4096]);
        let mut queue = ReliableRelaySenderQueue::default();
        queue.push_data(payload.clone());
        let shared = SharedRequestProduct::new(RequestProductState {
            sender: RequestSenderService::new(stream_id),
            send_stream: ReliableSendStream::new(stream_id, limits),
            sender_queue: queue,
            last_send_ack: Default::default(),
            remotes,
            prepared: RequestPreparedSource::new(TrafficClass::Throughput, payload.len()),
        });
        let _actor_lifetime = shared.actor_lifetime();
        {
            let mut state = shared.lock();
            crate::runtime::relay::control::publish_prepared_request_work(
                &mut state,
                &shared,
                &context,
                TrafficClass::Throughput,
                payload.len(),
                true,
            );
        }
        let ReliablePathCommand::PreparedOriginal(work) =
            try_recv_request_command_after_path_proofs_for_test(&mut receivers)
                .expect("actual weak prepared-source notice")
        else {
            panic!("Original source must not enter the Data command lane");
        };
        let ready_receipt = receivers
            .writer_ready_boundary(instance.path_instance_id)
            .expect("actual exclusive writer reaches its ready boundary")
            .receipt();
        let mut receivers = Some(receivers);
        let capture_cut = Arc::new(std::sync::atomic::AtomicBool::new(false));
        if close_receiver {
            // Drop invalidates the actual owned epoch. Claim-after-Drop is
            // borrow-unreachable; retaining only its receipt cannot recreate
            // an owner or claim authority. Actual concurrent drain rejection
            // is separately exercised by the claim-boundary control.
            drop(receivers.take());
            assert!(!ready_receipt.is_current());
        } else {
            let capture_cut = capture_cut.clone();
            shared.before_prepared_native_resolve_once_for_test(move || {
                capture_cut.store(true, std::sync::atomic::Ordering::SeqCst);
                native.advance_transport_activation_for_test(2).unwrap();
                native
                    .publish_observation_for_test(2, 8, Some(100_000_000))
                    .unwrap();
                // The current activation has no coherent shape. This is the
                // actual claim's unlocked Native capture cut, not the removed
                // Original command-reservation path.
            });
        }
        if let Some(receivers) = receivers.as_mut() {
            let ready = receivers
                .writer_ready_boundary(instance.path_instance_id)
                .unwrap();
            assert_eq!(ready.receipt(), ready_receipt);
            assert!(
                matches!(work.try_claim(ready), PreparedOriginalClaim::Blocked(_)),
                "an unpublished Native successor must retain unclaimed source"
            );
        }
        let state = shared.lock();
        assert_eq!(
            (
                state.send_stream.next_offset(),
                state.send_stream.reinjection_bytes()
            ),
            (0, 0)
        );
        assert_eq!(state.sender_queue.data_bytes(), payload.len());
        assert!(
            matches!(state.sender_queue.front().map(|(_, work)| &work.kind),
                Some(ReliableRelayQueuedWorkKind::Data(current)) if current == &payload)
        );
        assert!(
            state
                .sender
                .multipath
                .latest_unacked_ranges_for_path_instance(instance)
                .is_empty()
        );
        assert_eq!(
            state.remotes.paths.len(),
            1,
            "rejected source admission is not carrier retirement"
        );
        assert_eq!(state.remotes.paths[0].instance(), instance);
        assert_eq!(state.prepared.last_claimed_at, None);
        assert_eq!(commands.pending_bytes(), 0);
        if let Some(receivers) = receivers.as_mut() {
            assert!(capture_cut.load(std::sync::atomic::Ordering::SeqCst));
            assert!(try_recv_request_command_after_path_proofs_for_test(receivers).is_none());
        }
    }
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
    let (mut remotes, _remote_input) = ReliableRelayRemoteSet::new(opened, 4);
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
    let (mut remotes, _remote_input) = ReliableRelayRemoteSet::new(
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
    let outcome = tokio::time::timeout(Duration::from_millis(250), async {
        sender.send_frame(
            &context,
            &mut remotes,
            pending,
            RelaySendCause::StreamData,
            Some(TrafficClass::Throughput),
        )
    })
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
    let (mut remotes, mut _remote_input) = ReliableRelayRemoteSet::new(target_opened, 4);
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
            .dispatch_client_repair_work(
                &context,
                TrafficClass::Throughput,
                &mut remotes,
                &mut sender_queue,
            )
            .map(|dispatch| dispatch.expect("the exact queued repair remains present")),
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
    let terminal = tokio::time::timeout(Duration::from_secs(1), _remote_input.recv_frame())
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
            .dispatch_client_repair_work(
                &context,
                TrafficClass::Throughput,
                &mut remotes,
                &mut sender_queue,
            )
            .map(|dispatch| dispatch.expect("the exact queued repair remains present"))
            .expect("absent exact target cancels the retained queued batch"),
        ClientQueuedDispatch::PersistentReinjectionCancelled
    ));
    assert!(sender_queue.is_empty());
}

#[tokio::test]
async fn client_ack_gap_model_separates_owner_transport_from_reinjection_output() {
    use crate::runtime::relay::io::{
        AuthoritativeStreamAckSnapshot, begin_reliable_stream_ack,
        update_reinjection_authoritative_ack_snapshot,
    };
    let stream_id = StreamId(90);
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10260?initial-srtt-s=0.5&initial-rate-mbps=400",
        "quic://127.0.0.1:10261?initial-srtt-s=0.04&initial-rate-mbps=200",
        "quic://127.0.0.1:10262?initial-srtt-s=0.005&initial-rate-mbps=500",
    ]);
    let (tcp_commands, _tcp_receivers) = reliable_path_command_channels(8);
    let (udp_commands, mut udp_receivers) = reliable_path_command_channels(1);
    let (proof_only_commands, mut proof_only_receivers) = reliable_path_command_channels(8);
    let (mut remotes, _remote_input) = ReliableRelayRemoteSet::new(
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
    let positive_ranges = vec![OffsetRange {
        start: 4096,
        end: 8192,
    }];
    let ack = begin_reliable_stream_ack(&send_stream, Some(0), positive_ranges)
        .expect("later positive receipt validates against committed source");
    sender
        .apply_request_product_ack(&context, &remotes, &mut send_stream, &ack)
        .expect("positive receipt releases the actual cache");
    let mut evidence = AuthoritativeStreamAckSnapshot::default();
    update_reinjection_authoritative_ack_snapshot(&mut evidence, &ack, &send_stream);
    let ranges = evidence.gaps();
    assert_eq!(
        ranges,
        &[OffsetRange {
            start: 0,
            end: 4096
        }]
    );

    // Match the relay actor's race-free ordering: generation precedes every
    // model read that can conclude there is no measured alternate.
    let path_model_generation_before_observation = context.path_model_generation();
    let observation = sender.data_ack_gap_reinjection_model(
        &context,
        &remotes,
        &send_stream,
        &sender_queue,
        ranges,
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
        ranges,
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
        .dispatch_client_repair_work(&context, TrafficClass::Throughput, &mut remotes, &mut queue)
        .map(|dispatch| dispatch.expect("the exact queued repair remains present"))
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
        .dispatch_client_repair_work(&context, TrafficClass::Throughput, &mut remotes, &mut queue)
        .map(|dispatch| dispatch.expect("the exact queued repair remains present"))
        .expect("stale bound reinjection is cancelled without aborting the stream");
    assert!(matches!(
        dispatch,
        ClientQueuedDispatch::PersistentReinjectionCancelled
    ));
    assert!(queue.is_empty());
    assert_eq!(queue.reinjection_bytes(), 0);
}

#[tokio::test]
async fn request_path_recovery_waits_when_every_attachment_already_owns_the_range() {
    request_path_recovery_without_a_new_target(false).await;
}

#[tokio::test]
async fn request_path_recovery_does_not_close_when_last_target_becomes_stale() {
    request_path_recovery_without_a_new_target(true).await;
}

async fn request_path_recovery_without_a_new_target(stale_before_dispatch: bool) {
    let stream_id = StreamId(233);
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10321?initial-srtt-s=0.08&initial-rate-mbps=100",
        "tcp://127.0.0.1:10322?initial-srtt-s=0.02&initial-rate-mbps=500",
        "tcp://127.0.0.1:10323?initial-srtt-s=0.04&initial-rate-mbps=200",
    ]);
    let (owner_commands, mut owner_receivers) = reliable_path_command_channels(8);
    let (mut remotes, _remote_input) =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, owner_commands), 8);
    let owner = remotes.paths[0].instance();
    let (copy_commands, mut copy_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream(stream_id, 1, copy_commands));
    let copy = remotes.paths[1].instance();
    for receivers in [&mut owner_receivers, &mut copy_receivers] {
        consume_client_path_proof_for_test(receivers);
    }
    for instance in [owner, copy] {
        context.install_relay_path_instance_for_test(instance);
    }

    let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
    let original = send_stream
        .send_data(Bytes::from(vec![0x6e; 4096]))
        .expect("retained original data");
    let mut sender = RequestSenderService::new(stream_id);
    sender.record_original_frame_for_test(owner, &original);
    sender.record_reinjected_frame_for_test(copy, &original);
    assert!(sender.multipath.mark_path_stale(owner));
    let deadline = sender
        .earliest_reinjection_suppression_deadline(&remotes)
        .expect("accepted copy has an immutable retry deadline");
    tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
    let queue = ReliableRelaySenderQueue::default();
    let mut recovery = sender.collect_request_path_recovery(&remotes, &queue);
    if stale_before_dispatch {
        assert!(sender.multipath.mark_path_stale(copy));
    }
    assert!(
        sender
            .dispatch_next_request_path_recovery(
                &mut recovery,
                &context,
                &mut remotes,
                &send_stream,
                &queue,
            )
            .expect("no new target must not close the relay")
            .is_none()
    );
    assert!(recovery.blocked_for_carrier_capacity);
    assert!(
        recovery.retry_deadline.is_none(),
        "no expired timer busy loop"
    );
    assert!(queue.is_empty());
    assert_eq!(send_stream.reinjection_bytes(), 4096);
    assert_eq!(sender.multipath.accepted_reinjected_data_bytes(copy), 4096);
    assert_eq!(remotes.paths.len(), 2);
    assert!(try_recv_reliable_path_command(&mut owner_receivers).is_none());
    assert!(try_recv_reliable_path_command(&mut copy_receivers).is_none());

    let generation_before = remotes.membership_generation();
    let (fresh_commands, mut fresh_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream(stream_id, 2, fresh_commands));
    consume_client_path_proof_for_test(&mut fresh_receivers);
    let fresh = remotes.paths[2].instance();
    context.install_relay_path_instance_for_test(fresh);
    assert_ne!(remotes.membership_generation(), generation_before);
    let mut recovery = sender.collect_request_path_recovery(&remotes, &queue);
    assert!(matches!(
        sender
            .dispatch_next_request_path_recovery(
                &mut recovery,
                &context,
                &mut remotes,
                &send_stream,
                &queue,
            )
            .expect("membership publication reselects a real target"),
        Some(ClientQueuedDispatch::Reinjection {
            payload_bytes: 4096,
            ..
        })
    ));
    assert!(matches!(
        try_recv_reliable_path_command(&mut fresh_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
    assert!(queue.is_empty());
    assert_eq!(sender.multipath.accepted_reinjected_data_bytes(copy), 4096);
    assert_eq!(sender.multipath.accepted_reinjected_data_bytes(fresh), 4096);
    assert_eq!(send_stream.reinjection_bytes(), 4096);
}

#[tokio::test]
async fn request_recovery_prefix_first_owner_order_control() {
    request_recovery_orders_retained_ranges(false, false, false).await;
}

#[tokio::test]
async fn request_recovery_copy_debt_query_control() {
    let (calls, _) = request_recovery_copy_debt_query_work(false).await;
    assert!(
        calls > 0,
        "the actual dispatcher queries accepted-copy debt"
    );
}

#[tokio::test]
async fn request_recovery_copy_debt_query_ignores_original_suffix_fragmentation() {
    let control = request_recovery_copy_debt_query_work(false).await;
    let fragmented = request_recovery_copy_debt_query_work(true).await;
    assert!(
        control.0 > 0,
        "the actual dispatcher queries accepted-copy debt"
    );
    assert_eq!(fragmented.0, control.0, "identical repair query count");
    assert_eq!(
        fragmented.1, control.1,
        "accepted-copy debt discovery must not revisit irrelevant Original suffix fragments when the exact repair and target debt are unchanged"
    );
}

async fn request_recovery_copy_debt_query_work(fragmented_suffix: bool) -> (usize, usize) {
    use crate::runtime::stream::request::RequestFlightLedger;

    let stream_id = StreamId(237);
    let context =
        client_test_context_with_paths(&["tcp://127.0.0.1:10384", "tcp://127.0.0.1:10385"]);
    let (owner_commands, mut owner_receivers) = reliable_path_command_channels(8);
    let (mut remotes, _remote_input) =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, owner_commands), 8);
    let owner = remotes.paths[0].instance();
    let (target_commands, mut target_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream(stream_id, 1, target_commands));
    let target = remotes.paths[1].instance();
    for receivers in [&mut owner_receivers, &mut target_receivers] {
        consume_client_path_proof_for_test(receivers);
    }
    for instance in [owner, target] {
        context.install_relay_path_instance_for_test(instance);
    }

    let q = 4096;
    let suffix_bytes = 64 * 1024;
    let suffix_chunk = if fragmented_suffix {
        1024
    } else {
        suffix_bytes
    };
    let retained_bytes = q + suffix_bytes;
    let mut send_stream = ReliableSendStream::new(stream_id, context.mux_limits);
    let mut sender = RequestSenderService::new(stream_id);
    let head = send_stream
        .send_data(Bytes::from(vec![0x73; q]))
        .expect("legal unchanged head cache quantum");
    // The production cache and exact Original-flight producers establish the
    // fixture; this does not assert that Original packets crossed a network.
    sender.record_original_frame_for_test(owner, &head);
    for _ in 0..suffix_bytes / suffix_chunk {
        let suffix = send_stream
            .send_data(Bytes::from(vec![0x74; suffix_chunk]))
            .expect("legal suffix fragmentation within unchanged default bounds");
        sender.record_original_frame_for_test(owner, &suffix);
    }
    assert_eq!(send_stream.data_ack_frontier(), 0);
    assert_eq!(send_stream.next_offset(), retained_bytes as u64);
    assert_eq!(send_stream.reinjection_bytes(), retained_bytes);
    assert_eq!(sender.multipath.accepted_reinjected_data_bytes(target), 0);
    assert!(
        sender
            .earliest_reinjection_suppression_deadline(&remotes)
            .is_none()
    );
    assert!(sender.multipath.mark_path_stale(owner));
    let queue = ReliableRelaySenderQueue::default();
    let (selection, exhausted) = sender.multipath.reinjection_path_snapshot(
        &context,
        &remotes,
        &[owner],
        &queue,
        retained_bytes,
        context.mux_limits,
    );
    assert!(!exhausted);
    let (selected, _, service_limit) =
        selection.expect("ordinary target has real repair authority");
    assert_eq!(selected, target);
    assert_eq!(
        service_limit,
        context.mux_limits.max_repair_bytes.min(retained_bytes)
    );
    assert!(service_limit >= q);
    assert!(try_recv_reliable_path_command(&mut target_receivers).is_none());
    let mut batch = sender.collect_request_path_recovery(&remotes, &queue);
    assert!(batch.has_pending());

    RequestFlightLedger::take_reinjection_debt_query_work_for_test();
    // The counter interval contains one actual synchronous dispatcher call,
    // with no await or post-dispatch assertion queries mixed into its counts.
    let dispatch = sender.dispatch_next_request_path_recovery(
        &mut batch,
        &context,
        &mut remotes,
        &send_stream,
        &queue,
    );
    let work = RequestFlightLedger::take_reinjection_debt_query_work_for_test();
    assert!(matches!(
        dispatch.expect("unchanged exact native admission"),
        Some(ClientQueuedDispatch::Reinjection { payload_bytes, .. }) if payload_bytes == q
    ));
    let command = try_recv_reliable_path_command(&mut target_receivers)
        .expect("actual target receiver observes committed repair");
    target_receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&command));
    assert!(matches!(
        command,
        ReliablePathCommand::SendFrame(Frame::StreamData { stream_id: actual, offset: 0, payload })
            if actual == stream_id && payload == vec![0x73; q]
    ));
    assert!(try_recv_reliable_path_command(&mut target_receivers).is_none());
    assert!(try_recv_reliable_path_command(&mut owner_receivers).is_none());
    assert_eq!(sender.multipath.accepted_reinjected_data_bytes(target), q);
    assert_eq!(sender.multipath.accepted_reinjected_data_bytes(owner), 0);
    assert_eq!(sender.optional_reinjection.reinjected_bytes(), q as u64);
    assert_eq!(send_stream.data_ack_frontier(), 0);
    assert_eq!(send_stream.next_offset(), retained_bytes as u64);
    assert_eq!(send_stream.reinjection_bytes(), retained_bytes);
    assert!(
        queue.is_empty(),
        "direct repair adds no provisional source debt"
    );
    work
}

#[tokio::test]
async fn request_recovery_suffix_first_owner_order_preserves_lowest_prefix() {
    request_recovery_orders_retained_ranges(true, false, false).await;
}

#[tokio::test]
async fn request_recovery_interleaved_owners_preserve_global_range_order() {
    request_recovery_orders_retained_ranges(false, true, false).await;
}

#[tokio::test]
async fn request_recovery_pending_suffix_yields_to_newly_due_prefix() {
    request_recovery_orders_retained_ranges(true, false, true).await;
}

async fn request_recovery_orders_retained_ranges(
    suffix_first: bool,
    interleaved: bool,
    prequeue_suffix: bool,
) {
    let stream_id = StreamId(236);
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10381",
        "tcp://127.0.0.1:10382",
        "tcp://127.0.0.1:10383",
    ]);
    let limits = context.mux_limits;
    let (a_commands, mut a_receivers) = reliable_path_command_channels(8);
    let (mut remotes, _remote_input) =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, a_commands), 8);
    let a = remotes.paths[0].instance();
    let (b_commands, mut b_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream(stream_id, 1, b_commands));
    let b = remotes.paths[1].instance();
    let (c_commands, mut c_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream(stream_id, 2, c_commands));
    let c = remotes.paths[2].instance();
    for receivers in [&mut a_receivers, &mut b_receivers, &mut c_receivers] {
        consume_client_path_proof_for_test(receivers);
    }
    for instance in [a, b, c] {
        context.install_relay_path_instance_for_test(instance);
    }
    if prequeue_suffix {
        // Equal default target scores resolve by attached iteration order.
        // Keep C ahead of still-live A without changing either path's model.
        remotes.paths.swap(0, 2);
    }
    let mut sender = RequestSenderService::new(stream_id);
    let queue = ReliableRelaySenderQueue::default();
    // Every range fits C's unchanged startup repair envelope. This isolates
    // publication order, not exhaustion of measured native service credit.
    let q = reliable_relay_buffer_len(limits).min(limits.max_repair_bytes / 3);
    assert!(q > 0);
    let geometry = if interleaved {
        vec![(a, q), (b, q), (a, q)]
    } else {
        vec![(a, q), (b, q)]
    };
    let mut send_stream = ReliableSendStream::new(stream_id, limits);
    let mut expected_a = Vec::new();
    let mut expected_b = Vec::new();
    for (owner, bytes) in geometry {
        let start = send_stream.next_offset();
        let frame = send_stream
            .send_data(Bytes::from(vec![0x73; bytes]))
            .expect("legal retained OriginalData cache producer");
        // This is the existing exact OriginalData-flight fixture, not an
        // assertion that original TCP packets have traversed a real network.
        sender.record_original_frame_for_test(owner, &frame);
        let range = OffsetRange {
            start,
            end: send_stream.next_offset(),
        };
        if owner == a {
            expected_a.push(range);
        } else {
            expected_b.push(range);
        }
    }
    assert_eq!(send_stream.data_ack_frontier(), 0);
    assert_eq!(
        send_stream.reinjection_bytes(),
        send_stream.next_offset() as usize
    );
    assert!(
        sender
            .earliest_reinjection_suppression_deadline(&remotes)
            .is_none()
    );
    let (selection, exhausted) = sender.multipath.reinjection_path_snapshot(
        &context,
        &remotes,
        &[a, b],
        &queue,
        send_stream.reinjection_bytes(),
        limits,
    );
    assert!(!exhausted);
    let (target, _, capacity) = selection.expect("fresh C has current repair authority");
    assert_eq!(target, c);
    assert_eq!(
        capacity,
        limits.max_repair_bytes.min(send_stream.reinjection_bytes())
    );
    assert_eq!(capacity, if interleaved { 3 * q } else { 2 * q });
    for (position, owner) in (if suffix_first { [b, a] } else { [a, b] })
        .into_iter()
        .enumerate()
    {
        assert!(sender.multipath.mark_path_stale(owner));
        if prequeue_suffix && position == 0 {
            assert!(!sender.multipath.path_is_stale(a));
            assert_eq!(
                sender.multipath.request_recovery_original_paths(&remotes),
                vec![b],
                "the lower Original owner is not yet due for structural recovery"
            );
            let (target, _) = sender.multipath.reinjection_path_snapshot(
                &context,
                &remotes,
                &[b],
                &queue,
                send_stream.reinjection_bytes(),
                limits,
            );
            assert_eq!(target.map(|(target, _, _)| target), Some(c));
            let previous_batch = sender.collect_request_path_recovery(&remotes, &queue);
            assert!(previous_batch.has_pending());
            assert!(
                queue.is_empty(),
                "collection materializes no provisional copy"
            );
            assert!(try_recv_reliable_path_command(&mut c_receivers).is_none());
            assert!(
                sender
                    .earliest_reinjection_suppression_deadline(&remotes)
                    .is_none(),
                "collected later ownership is not an accepted native copy"
            );
        }
    }
    assert_eq!(
        sender.multipath.request_recovery_original_paths(&remotes),
        if suffix_first { vec![b, a] } else { vec![a, b] }
    );
    for (owner, expected) in [(a, expected_a), (b, expected_b)] {
        let recovery = sender.multipath.path_recovery_state(&remotes, owner);
        assert_eq!(recovery.uncovered_ranges, expected);
        assert!(
            recovery.retry_deadline.is_none(),
            "no prior accepted-copy suppression"
        );
    }
    assert!(try_recv_reliable_path_command(&mut c_receivers).is_none());
    let mut recovery = sender.collect_request_path_recovery(&remotes, &queue);
    assert!(recovery.has_pending());
    assert!(queue.is_empty());
    // The interleaved case observes two native handoffs; merely sorting owners
    // by their first range must not publish A's later range ahead of B's head.
    for expected_offset in if interleaved {
        vec![0, q as u64]
    } else {
        vec![0]
    } {
        let dispatch = sender
            .dispatch_next_request_path_recovery(
                &mut recovery,
                &context,
                &mut remotes,
                &send_stream,
                &queue,
            )
            .expect("selected repair passes unchanged native admission")
            .expect("lowest eligible range enters native service in this batch");
        assert!(matches!(dispatch, ClientQueuedDispatch::Reinjection { .. }));
        let command = try_recv_reliable_path_command(&mut c_receivers)
            .expect("actual native command receiver observes repair commitment");
        c_receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&command));
        let ReliablePathCommand::SendFrame(Frame::StreamData {
            offset, payload, ..
        }) = command
        else {
            panic!("expected committed repair STREAM_DATA");
        };
        assert!(!payload.is_empty());
        assert_eq!(
            offset, expected_offset,
            "historical owner-entry order must not spend C's repair authority on a later range before the lowest eligible retained prefix"
        );
    }
}

#[tokio::test]
async fn direct_request_recovery_preserves_queued_live_copy_and_accepted_deadline() {
    request_recovery_preserves_queued_live_copy(false, false).await;
}

#[tokio::test]
async fn direct_request_recovery_services_both_sides_of_queued_partial_copy() {
    request_recovery_preserves_queued_live_copy(true, false).await;
}

#[tokio::test]
async fn direct_request_recovery_resumes_after_queued_live_target_becomes_stale() {
    request_recovery_preserves_queued_live_copy(false, true).await;
}

async fn request_recovery_preserves_queued_live_copy(partial_copy: bool, cancel_queued: bool) {
    let stream_id = StreamId(237);
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10384",
        "tcp://127.0.0.1:10385",
        "tcp://127.0.0.1:10386",
    ]);
    let (owner_commands, mut owner_receivers) = reliable_path_command_channels(8);
    let (mut remotes, _remote_input) =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, owner_commands), 8);
    let owner = remotes.paths[0].instance();
    let (copy_commands, mut copy_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream(stream_id, 1, copy_commands));
    let copy = remotes.paths[1].instance();
    let (free_commands, mut free_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream(stream_id, 2, free_commands));
    let free = remotes.paths[2].instance();
    for receivers in [
        &mut owner_receivers,
        &mut copy_receivers,
        &mut free_receivers,
    ] {
        consume_client_path_proof_for_test(receivers);
    }
    for instance in [owner, copy, free] {
        context.install_relay_path_instance_for_test(instance);
    }
    let mut send_stream = ReliableSendStream::new(stream_id, context.mux_limits);
    let source_bytes = if partial_copy { 3 * 4096 } else { 4096 };
    let original = send_stream
        .send_data(Bytes::from(vec![0x5b; source_bytes]))
        .expect("retained OriginalData producer");
    let mut sender = RequestSenderService::new(stream_id);
    sender.record_original_frame_for_test(owner, &original);
    let mut queue = ReliableRelaySenderQueue::default();
    let cause = RelaySendCause::CompletionTailReinjection(ClientReinjectionOutputIdentity {
        instance: copy,
    });
    let queued_range = if partial_copy {
        OffsetRange {
            start: 4096,
            end: 8192,
        }
    } else {
        OffsetRange {
            start: 0,
            end: 4096,
        }
    };
    let queued_copy = send_stream
        .retransmission_frames_for_ranges(&[queued_range], 4096)
        .pop()
        .expect("exact queued copy is sliced from the actual retained cache");
    sender.enqueue_critical_reinjection_frame(&mut queue, queued_copy, cause);
    let accounted_before = sender.optional_reinjection.reinjected_bytes();
    assert!(sender.multipath.mark_path_stale(owner));
    if partial_copy {
        crate::runtime::sender::queue::take_recovery_overlap_visits_for_test();
    }
    let mut recovery = sender.collect_request_path_recovery(&remotes, &queue);
    if partial_copy {
        assert_eq!(
            crate::runtime::sender::queue::take_recovery_overlap_visits_for_test(),
            (1, 0),
            "collection observes the single queued extent once before native service"
        );
        // One cache chunk straddles the queued middle interval. Its uncovered
        // prefix and suffix remain independently eligible within this batch.
        for expected_offset in [0, 8192] {
            let dispatch = sender
                .dispatch_next_request_path_recovery(
                    &mut recovery,
                    &context,
                    &mut remotes,
                    &send_stream,
                    &queue,
                )
                .expect("partial queued overlap does not fence another range")
                .expect("uncovered range has native service");
            assert!(matches!(
                dispatch,
                ClientQueuedDispatch::Reinjection {
                    payload_bytes: 4096,
                    ..
                }
            ));
            let mut observed = Vec::new();
            for receivers in [&mut copy_receivers, &mut free_receivers] {
                if let Some(command) = try_recv_reliable_path_command(receivers) {
                    receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(
                        &command,
                    ));
                    let ReliablePathCommand::SendFrame(Frame::StreamData {
                        offset, payload, ..
                    }) = command
                    else {
                        panic!("expected exact uncovered repair command");
                    };
                    observed.push((offset, payload.len()));
                }
            }
            assert_eq!(observed, vec![(expected_offset, 4096)]);
        }
        assert!(
            sender
                .dispatch_next_request_path_recovery(
                    &mut recovery,
                    &context,
                    &mut remotes,
                    &send_stream,
                    &queue,
                )
                .expect("finite batch terminates")
                .is_none()
        );
        assert_eq!(queue.reinjection_bytes(), 4096);
        let (_, work) = queue.front().expect("exact queued middle remains intact");
        assert!(matches!(
            &work.kind,
            ReliableRelayQueuedWorkKind::Reinjection {
                cause: retained, frame: Frame::StreamData { offset: 4096, payload, .. },
            } if *retained == cause && payload.len() == 4096
        ));
        assert_eq!(
            sender.optional_reinjection.reinjected_bytes(),
            accounted_before + 8192
        );
        assert_eq!(send_stream.reinjection_bytes(), source_bytes);
        // This current-thread test retains the same thread-local count across
        // actual direct awaits. Target debt-accounting visits are not counted.
        assert_eq!(
            crate::runtime::sender::queue::take_recovery_overlap_visits_for_test(),
            (0, 0),
            "one serialized recovery batch must not rediscover queued overlap for each frame after collection"
        );
        return;
    }
    assert!(
        sender
            .dispatch_next_request_path_recovery(
                &mut recovery,
                &context,
                &mut remotes,
                &send_stream,
                &queue,
            )
            .expect("queued live repair remains authoritative")
            .is_none()
    );
    assert_eq!(queue.reinjection_bytes(), 4096);
    assert!(matches!(
        &queue.front().expect("existing live intent remains queued").1.kind,
        ReliableRelayQueuedWorkKind::Reinjection { cause: retained, .. } if *retained == cause
    ));
    assert_eq!(
        sender.optional_reinjection.reinjected_bytes(),
        accounted_before
    );
    for instance in [copy, free] {
        assert_eq!(sender.multipath.accepted_reinjected_data_bytes(instance), 0);
    }
    assert!(try_recv_reliable_path_command(&mut copy_receivers).is_none());
    assert!(try_recv_reliable_path_command(&mut free_receivers).is_none());

    if cancel_queued {
        assert!(sender.multipath.mark_path_stale(copy));
    }
    let dispatch = sender
        .dispatch_client_repair_work(&context, TrafficClass::Throughput, &mut remotes, &mut queue)
        .map(|dispatch| dispatch.expect("the exact queued repair remains present"))
        .expect("queued live repair resolves without closing the stream");
    if cancel_queued {
        assert!(matches!(
            dispatch,
            ClientQueuedDispatch::ReinjectionDeferred
        ));
        assert!(queue.is_empty());
        assert!(try_recv_reliable_path_command(&mut copy_receivers).is_none());
        assert_eq!(sender.multipath.accepted_reinjected_data_bytes(copy), 0);
        assert!(
            sender
                .earliest_reinjection_suppression_deadline(&remotes)
                .is_none()
        );
        // Recollect after the real queued-removal outcome, just as a new
        // actor recovery pass does; do not mutate or reuse its old batch.
        let mut retry = sender.collect_request_path_recovery(&remotes, &queue);
        assert!(matches!(
            sender
                .dispatch_next_request_path_recovery(
                    &mut retry,
                    &context,
                    &mut remotes,
                    &send_stream,
                    &queue,
                )
                .expect("removed queued authority exposes retained recovery"),
            Some(ClientQueuedDispatch::Reinjection {
                payload_bytes: 4096,
                ..
            })
        ));
        let command = try_recv_reliable_path_command(&mut free_receivers)
            .expect("the distinct surviving target receives the formerly excluded prefix");
        free_receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&command));
        assert!(
            matches!(command, ReliablePathCommand::SendFrame(Frame::StreamData {
            offset: 0, payload, ..
        }) if payload.len() == 4096)
        );
        assert_eq!(sender.multipath.accepted_reinjected_data_bytes(free), 4096);
        assert_eq!(
            sender.optional_reinjection.reinjected_bytes(),
            accounted_before + 4096
        );
        assert!(
            sender
                .dispatch_next_request_path_recovery(
                    &mut retry,
                    &context,
                    &mut remotes,
                    &send_stream,
                    &queue,
                )
                .expect("one finite retry batch")
                .is_none()
        );
        assert!(try_recv_reliable_path_command(&mut free_receivers).is_none());
        assert_eq!(send_stream.reinjection_bytes(), 4096);
        return;
    }
    let ClientQueuedDispatch::Reinjection {
        accepted_copy_deadline,
        ..
    } = dispatch
    else {
        panic!("expected existing live copy commitment");
    };
    let command = try_recv_reliable_path_command(&mut copy_receivers)
        .expect("bound live copy reaches its native command receiver");
    copy_receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&command));
    assert!(queue.is_empty());
    assert_eq!(sender.multipath.accepted_reinjected_data_bytes(copy), 4096);
    assert!(accepted_copy_deadline > Instant::now());
    let mut recovery = sender.collect_request_path_recovery(&remotes, &queue);
    assert_eq!(recovery.retry_deadline, Some(accepted_copy_deadline));
    assert!(
        sender
            .dispatch_next_request_path_recovery(
                &mut recovery,
                &context,
                &mut remotes,
                &send_stream,
                &queue,
            )
            .expect("accepted copy retains its immutable suppression")
            .is_none()
    );
    assert!(try_recv_reliable_path_command(&mut free_receivers).is_none());
    assert_eq!(sender.multipath.accepted_reinjected_data_bytes(free), 0);
    assert_eq!(send_stream.reinjection_bytes(), 4096);
}

#[tokio::test]
async fn disappeared_path_recovery_target_is_reselected_at_direct_commit() {
    let stream_id = StreamId(232);
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10321?initial-srtt-s=0.08&initial-rate-mbps=100",
        "tcp://127.0.0.1:10322?initial-srtt-s=0.005&initial-rate-mbps=1000",
        "tcp://127.0.0.1:10323?initial-srtt-s=0.04&initial-rate-mbps=200",
    ]);
    let (owner_commands, mut owner_receivers) = reliable_path_command_channels(8);
    let (mut remotes, _remote_input) =
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
    let queue = ReliableRelaySenderQueue::default();
    let mut recovery = sender.collect_request_path_recovery(&remotes, &queue);
    assert!(recovery.has_pending());
    let (initial, _) = sender.multipath.reinjection_path_snapshot(
        &context,
        &remotes,
        &[owner],
        &queue,
        send_stream.reinjection_bytes(),
        context.mux_limits,
    );
    assert_eq!(initial.map(|(target, _, _)| target), Some(first));

    drop(remotes.remove_path_instance(first));
    assert!(matches!(
        sender
            .dispatch_next_request_path_recovery(
                &mut recovery,
                &context,
                &mut remotes,
                &send_stream,
                &queue,
            )
            .expect("replacement target dispatch"),
        Some(ClientQueuedDispatch::Reinjection { .. })
    ));
    assert!(matches!(
        try_recv_reliable_path_command(&mut second_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
    assert!(queue.is_empty());
    assert!(try_recv_reliable_path_command(&mut first_receivers).is_none());
    assert_eq!(sender.multipath.accepted_reinjected_data_bytes(first), 0);
    assert_eq!(
        sender.multipath.accepted_reinjected_data_bytes(second),
        4096
    );
    assert!(try_recv_reliable_path_command(&mut owner_receivers).is_none());
}

#[tokio::test]
async fn client_live_tail_uses_retained_send_extent_beyond_ack_snapshot() {
    let stream_id = StreamId(126);
    let context =
        client_test_context_with_paths(&["tcp://127.0.0.1:10341", "quic://127.0.0.1:10342"]);
    let (tcp_commands, mut tcp_receivers) = reliable_path_command_channels(8);
    let (mut remotes, _remote_input) = ReliableRelayRemoteSet::new(
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
        TrafficClass::Latency,
    ));
    assert!(matches!(
        sender
            .dispatch_client_repair_work(
                &context,
                TrafficClass::Latency,
                &mut remotes,
                &mut sender_queue,
            )
            .map(|dispatch| dispatch.expect("the exact queued repair remains present"))
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
    let (mut remotes, _remote_input) = ReliableRelayRemoteSet::new(
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
    let (mut remotes, _remote_input) = ReliableRelayRemoteSet::new(
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
    let model_wait = model_wait_sender.enqueue_retained_frontier_reinjection(
        &mut queue,
        &context,
        &remotes,
        &send_stream,
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
    let blocked = capacity_sender.enqueue_retained_frontier_reinjection(
        &mut capacity_queue,
        &context,
        &remotes,
        &send_stream,
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
            .enqueue_retained_frontier_reinjection(
                &mut capacity_queue,
                &context,
                &remotes,
                &send_stream,
                TrafficClass::Throughput,
            )
            .queued,
        "capacity release makes the same retained exact tail admissible",
    );

    let accepted_after = Instant::now();
    assert!(
        sender
            .enqueue_retained_frontier_reinjection(
                &mut queue,
                &context,
                &remotes,
                &send_stream,
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
        .dispatch_client_repair_work(&context, TrafficClass::Throughput, &mut remotes, &mut queue)
        .map(|dispatch| dispatch.expect("the exact queued repair remains present"))
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
            .enqueue_retained_frontier_reinjection(
                &mut exhausted_queue,
                &context,
                &remotes,
                &send_stream,
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
    let (mut remotes, _remote_input) = ReliableRelayRemoteSet::new(
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
    let ranked_frontier_bytes = adaptive_reliable_relay_reinjection_bytes(
        context.reliable_path_snapshot_for_instance(target),
        TrafficClass::Throughput,
        limits,
    );
    assert!(ranked_frontier_bytes > 0);
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
        let outcome = sender.enqueue_retained_frontier_reinjection(
            &mut queue,
            &context,
            &remotes,
            &send_stream,
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
    assert!(structural.1 && structural.3);
    assert_eq!(
        structural.2, ranked_frontier_bytes,
        "a live completion-tail decision must admit only the exact frontier it ranked",
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
async fn completion_tail_uses_cache_independent_ranked_frontier_for_target_and_apply() {
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
    let (mut remotes, _remote_input) = ReliableRelayRemoteSet::new(
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
    let ranked_frontier_bytes = adaptive_reliable_relay_reinjection_bytes(
        context.reliable_path_snapshot_for_instance(target),
        TrafficClass::Throughput,
        limits,
    );
    assert!(ranked_frontier_bytes > 0 && ranked_frontier_bytes < 64 * 1024);

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
    let owner_wins_outcome = owner_wins_sender.enqueue_retained_frontier_reinjection(
        &mut owner_wins_queue,
        &context,
        &remotes,
        &owner_wins_stream,
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
            .enqueue_retained_frontier_reinjection(
                &mut queue,
                &context,
                &remotes,
                &send_stream,
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
        cursor, ranked_frontier_bytes as u64,
        "Apply uses the same ranked owner-uniform frontier independently of the two cache chunks",
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
    let late_suffix_outcome = late_suffix_sender.enqueue_retained_frontier_reinjection(
        &mut late_suffix_queue,
        &context,
        &remotes,
        &late_suffix_stream,
        TrafficClass::Throughput,
    );
    assert!(late_suffix_outcome.queued);
    assert_eq!(
        late_suffix_queue.reinjection_bytes(),
        ranked_frontier_bytes,
        "a fresh same-owner assignment beyond the ranked frontier cannot widen or postpone recovery of that mature frontier",
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
            .enqueue_retained_frontier_reinjection(
                &mut boundary_queue,
                &context,
                &remotes,
                &boundary_stream,
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

    // Keep the same legal ranked prefix, but append storage chunks that cannot
    // enter its selection quantum. These are real cache admissions and exact
    // OriginalData flight records, not claims of native delivery. The observed
    // call is synchronous: no unrelated task can contribute to its visit count.
    let storage_chunk_bytes = 64;
    let selection_quantum = remotes
        .path_instances()
        .into_iter()
        .map(|instance| {
            adaptive_reliable_relay_reinjection_bytes(
                context.reliable_path_snapshot_for_instance(instance),
                TrafficClass::Throughput,
                limits,
            )
        })
        .max()
        .expect("existing live repair alternatives");
    let prefix_chunks = selection_quantum.div_ceil(storage_chunk_bytes);
    let suffix_chunks = limits.max_path_flight_bytes / storage_chunk_bytes;
    assert!(prefix_chunks > 0 && prefix_chunks < suffix_chunks);
    let mut prefix_visits = None;
    for chunks in [prefix_chunks, suffix_chunks] {
        let mut retained_stream = ReliableSendStream::new(stream_id, limits);
        let mut retained_sender = RequestSenderService::new(stream_id);
        for _ in 0..chunks {
            let original = retained_stream
                .send_data(Bytes::from(vec![0x75; storage_chunk_bytes]))
                .expect("retained cache admission remains within existing limits");
            retained_sender.record_original_frame_for_test(owner, &original);
        }
        let retained_bytes = chunks * storage_chunk_bytes;
        assert!(retained_bytes <= limits.max_path_flight_bytes);
        assert!(retained_bytes <= limits.max_repair_bytes);
        assert_eq!(retained_stream.data_ack_frontier(), 0);
        assert_eq!(retained_stream.next_offset(), retained_bytes as u64);
        assert_eq!(retained_stream.reinjection_bytes(), retained_bytes);
        let uniform = retained_sender
            .multipath
            .live_owner_uniform_frontier(
                OffsetRange {
                    start: 0,
                    end: retained_bytes as u64,
                },
                &remotes.path_instances(),
            )
            .expect("every retained byte has its exact original owner");
        assert_eq!(uniform.range.end, retained_bytes as u64);
        assert_eq!(uniform.owners, vec![owner]);
        assert_eq!(uniform.avoid, vec![owner]);
        let mut retained_queue = ReliableRelaySenderQueue::default();
        let (_, visits) = crate::model::work::observe_frontier_span_visits_for_test(|| {
            retained_sender.enqueue_retained_frontier_reinjection(
                &mut retained_queue,
                &context,
                &remotes,
                &retained_stream,
                TrafficClass::Throughput,
            )
        });
        println!(
            "retained frontier: {chunks} chunks, {selection_quantum} ranked bytes, {visits} visits"
        );
        if let Some(prefix_visits) = prefix_visits {
            assert_eq!(
                visits, prefix_visits,
                "unrankable suffix chunks must not add frontier visits beyond the identical ranked prefix",
            );
        } else {
            assert!(visits > 0, "control must reach actual frontier discovery");
            assert!(
                visits <= 12 * prefix_chunks,
                "bounded prefix discovery in the control",
            );
            prefix_visits = Some(visits);
        }
    }
}

#[tokio::test]
async fn request_product_ack_preserves_exact_data_ack_progress_path() {
    let stream_id = StreamId(91);
    let context = client_test_context_with_paths(&["tcp://127.0.0.1:10263"]);
    let (commands, _receivers) = reliable_path_command_channels(8);
    let (remotes, _remote_input) =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, commands), 8);
    let owner = remotes.paths[0].instance();
    let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
    let frame = send_stream
        .send_data(Bytes::from(vec![0x41; 4096]))
        .expect("request data");
    let mut sender = RequestSenderService::new(stream_id);
    sender.record_original_frame_for_test(owner, &frame);

    let ack = crate::mux::stream::validate_stream_ack(
        None,
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
    let (mut remotes, _remote_input) = ReliableRelayRemoteSet::new(
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
    let sender_queue = ReliableRelaySenderQueue::default();
    let mut recovery = sender.collect_request_path_recovery(&remotes, &sender_queue);
    assert!(recovery.has_pending());
    let accepted_before = Instant::now();
    let dispatch = sender
        .dispatch_next_request_path_recovery(
            &mut recovery,
            &context,
            &mut remotes,
            &send_stream,
            &sender_queue,
        )
        .expect("actual carrier command commitment")
        .expect("stale retained OriginalData commits through direct recovery");
    let accepted_after = Instant::now();
    let ClientQueuedDispatch::Reinjection {
        payload_bytes: 4096,
        accepted_copy_deadline: committed_deadline,
    } = dispatch
    else {
        panic!("direct path recovery must commit one exact reinjection: {dispatch:?}");
    };
    assert!(sender_queue.is_empty());
    assert!(
        committed_deadline >= accepted_before + committed_target_interval
            && committed_deadline <= accepted_after + committed_target_interval,
        "the accepted deadline must use the selected exact carrier snapshot at commitment",
    );

    let committed = sender.multipath.path_recovery_state(&remotes, owner);
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
            .path_recovery_state(&remotes, owner)
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
    let after_timing_growth = sender.multipath.path_recovery_state(&remotes, owner);
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
                scope_start: None,
                ranges: Vec::new(),
            },
            TrafficClass::Control,
        )
        .expect("prefill priority queue");
    let (mut remotes, _remote_input) =
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
        .expect("recv progress backpressure should not close the product stream");

    assert!(
        !sent.ack.accepted && sent.max_data.published_offset.is_none(),
        "blocked advisory progress must report no frame sent"
    );
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
    ));

    let retried = remotes.retry_pending_stream_ack();
    assert!(retried.ack.published);
    assert!(!retried.ack.pending);
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
    ));

    let (replacement_commands, mut replacement_rx) = reliable_path_command_channels(4);
    remotes.attach(opened_test_relay_stream(stream_id, 1, replacement_commands));
    consume_client_path_proof_for_test(&mut replacement_rx);
    let replacement = remotes.retry_pending_stream_ack();
    assert!(replacement.ack.published);
    assert!(!replacement.ack.pending);
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
                scope_start: None,
                ranges: Vec::new(),
            },
            TrafficClass::Control,
        )
        .expect("prefill priority queue");
    let (mut remotes, _remote_input) =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, commands), 4);
    let chunks = vec![
        Frame::StreamAck {
            stream_id,
            scope_start: Some(0),
            ranges: vec![OffsetRange { start: 0, end: 4 }],
        },
        Frame::StreamAck {
            stream_id,
            scope_start: Some(4),
            ranges: vec![OffsetRange { start: 8, end: 12 }],
        },
    ];

    let blocked = remotes.publish_stream_ack(1, chunks.clone(), chunks);
    assert!(!blocked.ack.published);
    assert!(blocked.ack.pending);
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck {
            ranges,
            ..
        })) if ranges.is_empty()
    ));

    let first = remotes.retry_pending_stream_ack();
    assert!(first.ack.accepted);
    assert!(!first.ack.published);
    assert!(first.ack.pending);
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck {
            ranges,
            ..
        })) if ranges == vec![OffsetRange { start: 0, end: 4 }]
    ));

    let second = remotes.retry_pending_stream_ack();
    assert!(second.ack.accepted);
    assert!(second.ack.published);
    assert!(!second.ack.pending);
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck {
            ranges,
            ..
        })) if ranges == vec![OffsetRange { start: 8, end: 12 }]
    ));
}

#[tokio::test]
async fn scoped_ack_actual_two_attachment_publication_preserves_catchup_and_replacement() {
    fn take_ack(
        receivers: &mut crate::runtime::path::commands::ReliablePathCommandReceivers,
    ) -> Frame {
        match try_recv_reliable_path_priority_command(receivers) {
            Some(ReliablePathCommand::SendFrame(frame @ Frame::StreamAck { .. })) => frame,
            _ => panic!("expected immediately queued ACK"),
        }
    }

    let stream_id = StreamId(921);
    let context = client_test_context();
    let (blocked_commands, mut blocked_rx) = reliable_path_command_channels(1);
    blocked_commands
        .try_enqueue_admitted_frame(
            Frame::StreamAck {
                stream_id,
                scope_start: None,
                ranges: vec![],
            },
            TrafficClass::Control,
        )
        .expect("prefill one attachment without blocking the other");
    let (available_commands, mut available_rx) = reliable_path_command_channels(8);
    let (mut remotes, _remote_input) =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, blocked_commands), 4);
    remotes.attach(opened_test_relay_stream(stream_id, 1, available_commands));
    // Every current attachment independently retains its scoped ACK catch-up.
    consume_client_path_proof_for_test(&mut available_rx);
    let mux_limits = MuxLimits {
        max_ack_ranges: 1,
        ..MuxLimits::default()
    };
    let mut recv_stream = ReliableRecvStream::new(stream_id, mux_limits);
    let mut progress = ReliableRecvProgress::default();
    let mut sender = RequestSenderService::new(stream_id);
    for offset in [0, 10, 20] {
        recv_stream
            .receive_data(offset, Bytes::from_static(b"x"))
            .unwrap();
    }
    assert!(
        sender
            .send_recv_progress(
                &mut remotes,
                &context,
                &mut recv_stream,
                &mut progress,
                RelayRecvProgressSend::ack_only(None, TrafficClass::Throughput),
            )
            .unwrap()
            .ack
            .published
    );
    assert_eq!(progress.ack_generation(), 1);
    let initial_cumulative = recv_stream.ack_frames();
    for expected in recv_stream.ack_frames() {
        assert_eq!(take_ack(&mut available_rx), expected);
    }
    assert!(remotes.has_pending_stream_ack_publication());
    assert!(try_recv_reliable_path_priority_command(&mut available_rx).is_none());

    for offset in [30, 40] {
        recv_stream
            .receive_data(offset, Bytes::from_static(b"x"))
            .unwrap();
    }
    assert!(
        sender
            .send_recv_progress(
                &mut remotes,
                &context,
                &mut recv_stream,
                &mut progress,
                RelayRecvProgressSend::ack_only(None, TrafficClass::Throughput),
            )
            .unwrap()
            .ack
            .published
    );
    assert_eq!(progress.ack_generation(), 2);
    for (start, scope) in [(30, 21), (40, 31)] {
        assert_eq!(
            take_ack(&mut available_rx),
            Frame::StreamAck {
                stream_id,
                scope_start: Some(scope),
                ranges: vec![OffsetRange {
                    start,
                    end: start + 1
                }],
            }
        );
    }
    assert!(
        try_recv_reliable_path_priority_command(&mut available_rx).is_none(),
        "the current attachment receives only new positive support"
    );

    assert_eq!(
        take_ack(&mut blocked_rx),
        Frame::StreamAck {
            stream_id,
            scope_start: None,
            ranges: vec![],
        }
    );
    let partial = remotes.retry_pending_stream_ack();
    assert!(partial.ack.accepted && partial.ack.pending);
    assert_eq!(take_ack(&mut blocked_rx), recv_stream.ack_frames()[0]);

    // New desired state cannot restart the immutable unfinished generation.
    // Finish its exact tail before bridging the missed generations with the
    // current cumulative state; no intervening delta ancestry is borrowed.
    recv_stream
        .receive_data(50, Bytes::from_static(b"x"))
        .unwrap();
    assert!(
        sender
            .send_recv_progress(
                &mut remotes,
                &context,
                &mut recv_stream,
                &mut progress,
                RelayRecvProgressSend::ack_only(None, TrafficClass::Throughput),
            )
            .unwrap()
            .ack
            .published
    );
    assert_eq!(progress.ack_generation(), 3);
    assert_eq!(
        take_ack(&mut available_rx),
        Frame::StreamAck {
            stream_id,
            scope_start: Some(41),
            ranges: vec![OffsetRange { start: 50, end: 51 }],
        }
    );
    let cumulative = recv_stream.ack_frames();
    assert_eq!(take_ack(&mut blocked_rx), initial_cumulative[1]);
    let old_tail = remotes.retry_pending_stream_ack();
    assert!(old_tail.ack.accepted && old_tail.ack.pending);
    assert_eq!(take_ack(&mut blocked_rx), initial_cumulative[2]);
    for expected in &cumulative {
        let retry = remotes.retry_pending_stream_ack();
        assert!(retry.ack.accepted);
        assert_eq!(&take_ack(&mut blocked_rx), expected);
        assert!(try_recv_reliable_path_priority_command(&mut available_rx).is_none());
    }
    assert!(!remotes.has_pending_stream_ack_publication());

    let previous_instance = remotes.paths[1].instance();
    drop(remotes.remove_path_instance(previous_instance));
    let (replacement_commands, mut replacement_rx) = reliable_path_command_channels(8);
    remotes.attach(opened_test_relay_stream(stream_id, 1, replacement_commands));
    consume_client_path_proof_for_test(&mut replacement_rx);
    let replacement = remotes.retry_pending_stream_ack();
    assert!(replacement.ack.published && !replacement.ack.pending);
    for expected in cumulative {
        assert_eq!(take_ack(&mut replacement_rx), expected);
    }
    assert!(try_recv_reliable_path_priority_command(&mut blocked_rx).is_none());
    assert!(try_recv_reliable_path_priority_command(&mut replacement_rx).is_none());
    assert!(
        matches!(recv_stream.take_ack_update().as_slice(),
        [Frame::StreamAck { scope_start: None, ranges, .. }] if ranges.is_empty()),
        "retry/replacement did not create or consume another receive generation"
    );
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
                scope_start: None,
                ranges: Vec::new(),
            },
            TrafficClass::Control,
        )
        .expect("prefill priority queue");
    let (mut remotes, _remote_input) =
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
        .expect("blocked MAX_DATA publication is retryable");

    assert!(!sent.ack.accepted && sent.max_data.published_offset.is_none());
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
        .max_data
        .published_offset
        .expect("replacement must replay retained MAX_DATA through the actor");
    recv_stream.commit_max_data(published_offset);
    assert!(!publication.max_data.pending);
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
                scope_start: None,
                ranges: Vec::new(),
            },
            TrafficClass::Control,
        )
        .expect("prefill first priority queue");
    let (available_commands, mut available_rx) = reliable_path_command_channels(4);
    let (mut remotes, _remote_input) =
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
            .expect("one live attachment publishes shared credit")
            .max_data
            .published_offset
            .is_some()
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
    assert_eq!(retry.max_data.published_offset, Some(published));
    assert!(!retry.max_data.pending);
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
    assert_eq!(
        replacement_publication.max_data.published_offset,
        Some(published)
    );
    assert!(!replacement_publication.max_data.pending);
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
                scope_start: None,
                ranges: Vec::new(),
            },
            TrafficClass::Control,
        )
        .expect("prefill first priority queue");
    let (second_commands, mut second_rx) = reliable_path_command_channels(1);
    let (mut remotes, _remote_input) =
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
        .expect("available alternate control queue should accept recv progress");

    assert!(sent.ack.published);
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
    let (mut remotes, _remote_input) = ReliableRelayRemoteSet::new(
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
    let (mut remotes, _remote_input) = ReliableRelayRemoteSet::new(
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
    let (mut remotes, _remote_input) =
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

#[tokio::test]
async fn client_exact_failure_recovery_keeps_full_structural_target_service() {
    let stream_id = StreamId(95);
    let limits = MuxLimits::default();
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10371?initial-srtt-s=0.08&initial-rate-mbps=10",
        "tcp://127.0.0.1:10372?initial-srtt-s=0.02&initial-rate-mbps=500",
    ]);
    let (owner_commands, mut owner_receivers) = reliable_path_command_channels(8);
    let (mut remotes, _remote_input) =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, owner_commands), 8);
    consume_client_path_proof_for_test(&mut owner_receivers);
    let owner = remotes.paths[0].instance();
    let (target_commands, mut target_receivers) = reliable_path_command_channels(8);
    remotes.attach_candidate(opened_test_relay_stream(stream_id, 1, target_commands));
    consume_client_path_proof_for_test(&mut target_receivers);
    let target = remotes
        .path_instance_for_key(RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 1,
        })
        .expect("exact-failure target");
    seed_client_bulk_evidence_for_test(&context, owner);
    seed_client_bulk_evidence_for_test(&context, target);

    let mut send_stream = ReliableSendStream::new(stream_id, limits);
    let retained = send_stream
        .send_data(Bytes::from(vec![0x74; 256 * 1024]))
        .expect("multi-quantum retained request");
    let mut sender = RequestSenderService::new_with_performance(
        stream_id,
        MppPerformanceConfig {
            optional_reinjection_budget_percent: 0,
        },
    );
    sender.record_original_frame_for_test(owner, &retained);
    assert!(sender.fail_client_path_instance(&context, &mut remotes, owner));

    let sender_queue = ReliableRelaySenderQueue::default();
    let (modeled, exhausted) = sender.multipath.reinjection_path_snapshot(
        &context,
        &remotes,
        &[owner],
        &sender_queue,
        send_stream.reinjection_bytes(),
        limits,
    );
    assert!(!exhausted);
    let (modeled_target, target_snapshot, target_service_limit) =
        modeled.expect("exact failure retains a structurally eligible target");
    assert_eq!(modeled_target, target);
    let ranked_frontier_bytes = adaptive_reliable_relay_reinjection_bytes(
        Some(target_snapshot),
        TrafficClass::Throughput,
        limits,
    );
    assert!(target_service_limit > ranked_frontier_bytes);

    let accounted_before = sender.optional_reinjection.reinjected_bytes();
    let mut recovery = sender.collect_request_path_recovery(&remotes, &sender_queue);
    assert!(recovery.has_pending());
    let mut committed_bytes = 0usize;
    while let Some(dispatch) = sender
        .dispatch_next_request_path_recovery(
            &mut recovery,
            &context,
            &mut remotes,
            &send_stream,
            &sender_queue,
        )
        .expect("exact-failure recovery retains native admission")
    {
        let ClientQueuedDispatch::Reinjection { payload_bytes, .. } = dispatch else {
            panic!("expected exact-failure native repair commitment");
        };
        let command = try_recv_reliable_path_command(&mut target_receivers)
            .expect("the full service batch reaches its actual native command receiver");
        target_receivers
            .release_pending_command_bytes(reliable_path_command_pending_bytes(&command));
        let ReliablePathCommand::SendFrame(Frame::StreamData {
            offset, payload, ..
        }) = command
        else {
            panic!("expected exact-failure STREAM_DATA");
        };
        assert_eq!(offset, committed_bytes as u64);
        assert_eq!(payload.len(), payload_bytes);
        committed_bytes += payload_bytes;
    }
    assert_eq!(
        committed_bytes, target_service_limit,
        "exact failure must retain full bounded target service rather than the live-owner frontier cap",
    );
    assert_eq!(
        sender
            .optional_reinjection
            .reinjected_bytes()
            .saturating_sub(accounted_before),
        target_service_limit as u64,
    );
    assert!(sender_queue.is_empty());
    assert_eq!(
        sender.multipath.accepted_reinjected_data_bytes(target),
        committed_bytes
    );
}

#[tokio::test]
async fn equal_expiry_request_candidates_preserve_one_nonstale_survivor() {
    let stream_id = StreamId(197);
    let context =
        client_test_context_with_paths(&["tcp://127.0.0.1:10251", "tcp://127.0.0.1:10252"]);
    let (first_commands, mut first_receivers) = reliable_path_command_channels(8);
    let (mut remotes, _remote_input) =
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
    let (mut remotes, _remote_input) =
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

#[tokio::test]
async fn retained_frontier_suppresses_new_target_until_accepted_copy_deadline() {
    let stream_id = StreamId(713);
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10713",
        "tcp://127.0.0.1:10714",
        "tcp://127.0.0.1:10715",
    ]);
    let (owner_commands, mut owner_receivers) = reliable_path_command_channels(8);
    let (remotes, _remote_input) =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, owner_commands), 8);
    consume_client_path_proof_for_test(&mut owner_receivers);
    let owner = remotes.paths[0].instance();
    seed_client_bulk_evidence_for_test(&context, owner);

    // Copies still use real carrier commands. Original ownership below comes
    // from the production shared-source claim, not the retired dispatcher.
    let take_data =
        |receivers: &mut crate::runtime::path::commands::ReliablePathCommandReceivers| {
            loop {
                let command = try_recv_reliable_path_command(receivers)
                    .expect("an admitted data command must remain in its exact writer");
                receivers
                    .release_pending_command_bytes(reliable_path_command_pending_bytes(&command));
                match command {
                    ReliablePathCommand::SendFrame(Frame::PathProofData { .. }) => continue,
                    ReliablePathCommand::SendFrame(frame @ Frame::StreamData { .. }) => {
                        break frame;
                    }
                    _ => panic!("unexpected command before admitted Product data"),
                }
            }
        };
    let mut queue = ReliableRelaySenderQueue::default();
    queue.push_data(Bytes::from(vec![0x71; 4096]));
    let shared = SharedRequestProduct::new(RequestProductState {
        sender: RequestSenderService::new(stream_id),
        send_stream: ReliableSendStream::new(stream_id, context.mux_limits),
        sender_queue: queue,
        last_send_ack: Default::default(),
        remotes,
        prepared: RequestPreparedSource::new(TrafficClass::Throughput, 4096),
    });
    let _actor_lifetime = shared.actor_lifetime();
    {
        let mut state = shared.lock();
        crate::runtime::relay::control::publish_prepared_request_work(
            &mut state,
            &shared,
            &context,
            TrafficClass::Throughput,
            4096,
            true,
        );
    }
    let ReliablePathCommand::PreparedOriginal(work) =
        try_recv_request_command_after_path_proofs_for_test(&mut owner_receivers)
            .expect("actual sole-owner prepared notice")
    else {
        panic!("source is unbound until its writer claims it");
    };
    let ready = owner_receivers
        .writer_ready_boundary(owner.path_instance_id)
        .unwrap();
    let PreparedOriginalClaim::Claimed(original) = work.try_claim(ready) else {
        panic!("the actual sole A writer must claim the retained original");
    };
    owner_receivers.register_claimed_writer_frame(&original);
    owner_receivers.release_pending_command_bytes(
        crate::protocol::frame::reliable_path_frame_pacing_bytes(&original),
    );
    assert_eq!(
        reliable_stream_frame_extent(&original),
        Some((0, 4096, 4096))
    );
    assert!(shared.lock().sender_queue.is_empty());

    let (copy_commands, mut copy_receivers) = reliable_path_command_channels(8);
    let copy = {
        let mut state = shared.lock();
        let remotes = &mut state.remotes;
        assert_eq!(
            remotes.attach(opened_test_relay_stream(stream_id, 1, copy_commands)),
            ReliableRelayAttachOutcome::Attached
        );
        consume_client_path_proof_for_test(&mut copy_receivers);
        let copy = remotes
            .paths
            .iter()
            .find(|path| path.key().index == 1)
            .expect("B is the sole alternate")
            .instance();
        seed_client_bulk_evidence_for_test(&context, copy);
        copy
    };
    let owner_interval = crate::model::timing::reliable_data_retransmission_interval(
        Some(owner.key.underlay),
        context.reliable_path_snapshot_for_instance(owner),
    );
    tokio::time::sleep(owner_interval + Duration::from_millis(10)).await;
    let (alternate_commands, mut alternate_receivers) = reliable_path_command_channels(8);
    let (accepted_copy_deadline, alternate) = {
        let mut state = shared.lock();
        let RequestProductState {
            sender,
            send_stream,
            sender_queue: queue,
            remotes,
            ..
        } = &mut *state;
        assert!(
            sender
                .enqueue_retained_frontier_reinjection(
                    queue,
                    &context,
                    remotes,
                    send_stream,
                    TrafficClass::Throughput,
                )
                .queued,
            "A must mature before the first actual recovery commitment on B"
        );
        let dispatch = sender
            .dispatch_client_repair_work(&context, TrafficClass::Throughput, remotes, queue)
            .expect("real first-copy reservation and commitment")
            .expect("the exact queued repair remains present");
        let ClientQueuedDispatch::Reinjection {
            payload_bytes: 4096,
            accepted_copy_deadline,
        } = dispatch
        else {
            panic!("B must own an accepted recovery copy: {dispatch:?}");
        };
        assert_eq!(take_data(&mut copy_receivers), original);
        assert!(queue.is_empty());
        assert_eq!(
            sender.reinjection_suppression_deadline_for_frame(&original, remotes),
            Some(accepted_copy_deadline),
            "draining B's writer command cannot release its un-DataACKed copy",
        );

        assert_eq!(
            remotes.attach(opened_test_relay_stream(stream_id, 2, alternate_commands)),
            ReliableRelayAttachOutcome::Attached
        );
        consume_client_path_proof_for_test(&mut alternate_receivers);
        let alternate = remotes
            .paths
            .iter()
            .find(|path| path.key().index == 2)
            .expect("C is a distinct vacant alternate")
            .instance();
        seed_client_bulk_evidence_for_test(&context, alternate);
        assert!(context.relay_path_instance_has_bulk_model_evidence(alternate));
        assert!(remotes.contains_path_instance(copy));
        assert!(
            Instant::now() < accepted_copy_deadline,
            "the RED assertion must run before B's actual immutable deadline"
        );
        let suppressed = sender.enqueue_retained_frontier_reinjection(
            queue,
            &context,
            remotes,
            send_stream,
            TrafficClass::Throughput,
        );
        assert!(
            !suppressed.queued && queue.is_empty(),
            "a fresh vacant C does not bypass B's global same-range repeat delay: {suppressed:?}"
        );
        (accepted_copy_deadline, alternate)
    };

    // Same live B and same exact retained range: expiry permits C, but never
    // makes B's own publication slot vacant or releases Product ownership.
    tokio::time::sleep(
        accepted_copy_deadline.saturating_duration_since(Instant::now())
            + Duration::from_millis(10),
    )
    .await;
    let mut state = shared.lock();
    let RequestProductState {
        sender,
        send_stream,
        sender_queue: queue,
        remotes,
        ..
    } = &mut *state;
    assert!(
        sender
            .enqueue_retained_frontier_reinjection(
                queue,
                &context,
                remotes,
                send_stream,
                TrafficClass::Throughput,
            )
            .queued,
        "the identical measured C becomes eligible after B's repeat delay"
    );
    let (_, work) = queue.pop_front().expect("post-deadline C control");
    assert!(matches!(
        work.kind,
        ReliableRelayQueuedWorkKind::Reinjection {
            frame: Frame::StreamData { offset: 0, payload, .. },
            cause: RelaySendCause::CompletionTailReinjection(identity),
        } if payload.len() == 4096 && identity.instance == alternate
    ));
    assert_eq!(send_stream.reinjection_bytes(), 4096);
    assert!(remotes.contains_path_instance(copy));
    assert!(queue.is_empty());
}

#[tokio::test]
async fn prepared_request_deferred_notice_wakes_before_poll_and_does_not_retain_source() {
    let stream_id = StreamId(719);
    let context =
        client_test_context_with_paths(&["tcp://127.0.0.1:10720", "tcp://127.0.0.1:10721"]);
    let limits = context.mux_limits;
    let capacity = crate::runtime::path::commands::reliable_path_command_queue(limits);
    let (a_commands, mut a_receivers) = reliable_path_command_channels(capacity);
    let (b_commands, mut b_receivers) = reliable_path_command_channels(capacity);
    let (a_opened, _a_input) =
        opened_request_stream_with_retained_input(stream_id, 0, a_commands.clone());
    let (b_opened, _b_input) =
        opened_request_stream_with_retained_input(stream_id, 1, b_commands.clone());
    let (mut remotes, _remote_input) = ReliableRelayRemoteSet::new(a_opened, capacity);
    assert_eq!(
        remotes.attach(b_opened),
        ReliableRelayAttachOutcome::Attached
    );
    for receivers in [&mut a_receivers, &mut b_receivers] {
        let proof = try_recv_reliable_path_priority_command(receivers).unwrap();
        assert!(matches!(
            proof,
            ReliablePathCommand::SendFrame(Frame::PathProofData { .. })
        ));
        receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&proof));
    }
    let a = remotes.paths[0].instance();
    let b = remotes.paths[1].instance();
    context.install_relay_path_instance_for_test(a);
    context.install_relay_path_instance_for_test(b);
    let sender = RequestSenderService::new(stream_id);
    let admission = sender.reliable_stream_source_admission(
        &context,
        &remotes,
        TrafficClass::Throughput,
        reliable_relay_buffer_len(limits),
    );
    let quantum = crate::model::capacity::adaptive_reliable_relay_chunk_bytes(
        admission.selected_path,
        TrafficClass::Throughput,
        limits,
    );
    assert!(admission.selected_path.is_some());
    assert!(quantum <= admission.window_bytes);
    let source: Arc<[u8]> = Arc::from(vec![0x79; quantum]);
    let weak_source = Arc::downgrade(&source);
    let mut queue = ReliableRelaySenderQueue::default();
    queue.push_data(Bytes::from_owner(source));
    let shared = SharedRequestProduct::new(RequestProductState {
        sender_queue: queue,
        sender,
        send_stream: ReliableSendStream::new(stream_id, limits),
        last_send_ack: Default::default(),
        remotes,
        prepared: RequestPreparedSource::new(TrafficClass::Throughput, quantum),
    });
    let weak_product = shared.downgrade();
    let actor_lifetime = shared.actor_lifetime();
    let registration = {
        let mut state = shared.lock();
        crate::runtime::relay::control::publish_prepared_request_work(
            &mut state,
            &shared,
            &context,
            TrafficClass::Throughput,
            quantum,
            true,
        );
        state.prepared.registrations[1].clone()
    };
    assert_eq!(registration.request_instance(), Some(b));
    let weak_registration = Arc::downgrade(&registration);
    let take_b_notice =
        |receivers: &mut crate::runtime::path::commands::ReliablePathCommandReceivers| {
            let command = try_recv_request_command_after_path_proofs_for_test(receivers)
                .expect("actual weak notice");
            let ReliablePathCommand::PreparedOriginal(work) = command else {
                panic!("prepared source must remain a payload-free notice");
            };
            work
        };

    // Both actual default writers are ready. The unchanged ordinary order
    // selects A; B must park without claiming or manufacturing a new delay.
    let _a_ready = a_receivers
        .writer_ready_boundary(a.path_instance_id)
        .unwrap();
    let work = take_b_notice(&mut b_receivers);
    let ready = b_receivers
        .writer_ready_boundary(b.path_instance_id)
        .unwrap();
    let b_idle_receipt = ready.receipt();
    let PreparedOriginalClaim::Blocked(wait) = work.try_claim(ready) else {
        panic!("B must defer to the eligible ordinary A writer");
    };
    b_receivers.defer_prepared_work(work, wait);
    // These notifications follow claim entry, before the deferred future's
    // first poll. Neither Product, Native nor A readiness changes here.
    registration.notify();
    registration.notify();
    let work = take_b_notice(&mut b_receivers);
    assert!(try_recv_request_command_after_path_proofs_for_test(&mut b_receivers).is_none());
    assert!(a_commands.writer_boundary().snapshot().is_some());
    assert_eq!(
        b_commands.writer_boundary().snapshot(),
        Some(b_idle_receipt.clone())
    );

    // Without a fresh notification the same real failed claim stays parked;
    // a refused metadata attempt retains B's one physical idle epoch.
    let ready = b_receivers
        .writer_ready_boundary(b.path_instance_id)
        .unwrap();
    assert_eq!(ready.receipt(), b_idle_receipt);
    let PreparedOriginalClaim::Blocked(wait) = work.try_claim(ready) else {
        panic!("unchanged ordinary selection must still choose A");
    };
    b_receivers.defer_prepared_work(work, wait);
    assert!(try_recv_request_command_after_path_proofs_for_test(&mut b_receivers).is_none());
    {
        let state = shared.lock();
        assert_eq!(state.send_stream.next_offset(), 0);
        assert_eq!(state.send_stream.reinjection_bytes(), 0);
        assert_eq!(state.sender_queue.data_bytes(), quantum);
    }
    assert_eq!(a_commands.pending_bytes(), 0);
    assert_eq!(b_commands.pending_bytes(), 0);
    drop(registration);
    drop(actor_lifetime);
    assert!(weak_registration.upgrade().is_none());
    {
        let state = shared.lock();
        assert!(!state.prepared.claims_active);
        assert!(state.prepared.registrations.is_empty());
        assert_eq!(state.sender_queue.data_bytes(), quantum);
    }
    drop(shared);
    // Keep A's queued token and B's deferred token/receivers alive and unpolled:
    // neither may retain the logical owner or its actual source allocation.
    assert!(weak_product.upgrade().is_none());
    assert!(weak_source.upgrade().is_none());
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PreparedWriterClaimCase {
    Uncontended,
    LoserWithdraws,
    SelectedDrains,
    BackupFallback,
    RegularBecomesReady,
    FreshWriterNotReady,
    FreshReadyDisplacesStale,
}

fn prepared_competing_writer_claim_case(case: PreparedWriterClaimCase) -> (u64, usize) {
    let stream_id = StreamId(721);
    let context =
        client_test_context_with_paths(&["tcp://127.0.0.1:10722", "tcp://127.0.0.1:10723"]);
    let limits = context.mux_limits;
    let capacity = crate::runtime::path::commands::reliable_path_command_queue(limits);
    let (a_commands, mut a_receivers) = reliable_path_command_channels(capacity);
    let (b_commands, mut b_receivers) = reliable_path_command_channels(capacity);
    let (a_opened, _a_input) =
        opened_request_stream_with_retained_input(stream_id, 0, a_commands.clone());
    let (b_opened, _b_input) =
        opened_request_stream_with_retained_input(stream_id, 1, b_commands.clone());
    let (mut remotes, _remote_input) = ReliableRelayRemoteSet::new(a_opened, capacity);
    assert_eq!(
        remotes.attach(b_opened),
        ReliableRelayAttachOutcome::Attached
    );
    for receivers in [&mut a_receivers, &mut b_receivers] {
        let proof = try_recv_reliable_path_priority_command(receivers).unwrap();
        assert!(matches!(
            proof,
            ReliablePathCommand::SendFrame(Frame::PathProofData { .. })
        ));
        receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&proof));
    }
    let a = remotes.paths[0].instance();
    let b = remotes.paths[1].instance();
    context.install_relay_path_instance_for_test(a);
    context.install_relay_path_instance_for_test(b);
    let a_is_backup = matches!(
        case,
        PreparedWriterClaimCase::BackupFallback | PreparedWriterClaimCase::RegularBecomesReady
    );
    if a_is_backup {
        assert!(context.update_relay_path_usage_for_test(a, 1, crate::protocol::PathUsage::Backup));
    }
    // The hook may publish B's actual exclusive receiver boundary while A is
    // suspended. This test-local owner is never a production synchronization.
    let b_receivers = Arc::new(std::sync::Mutex::new(b_receivers));
    let sender = RequestSenderService::new(stream_id);
    let admission = sender.reliable_stream_source_admission(
        &context,
        &remotes,
        TrafficClass::Throughput,
        reliable_relay_buffer_len(limits),
    );
    assert!(admission.selected_path.is_some());
    let quantum = crate::model::capacity::adaptive_reliable_relay_chunk_bytes(
        admission.selected_path,
        TrafficClass::Throughput,
        limits,
    );
    assert!(quantum <= admission.window_bytes);
    let mut queue = ReliableRelaySenderQueue::default();
    queue.push_data(Bytes::from(vec![0x7b; quantum]));
    let shared = SharedRequestProduct::new(RequestProductState {
        sender_queue: queue,
        sender,
        send_stream: ReliableSendStream::new(stream_id, limits),
        last_send_ack: Default::default(),
        remotes,
        prepared: RequestPreparedSource::new(TrafficClass::Throughput, quantum),
    });
    let _actor_lifetime = shared.actor_lifetime();
    {
        let mut state = shared.lock();
        crate::runtime::relay::control::publish_prepared_request_work(
            &mut state,
            &shared,
            &context,
            TrafficClass::Throughput,
            quantum,
            true,
        );
    }
    let take_notice =
        |receivers: &mut crate::runtime::path::commands::ReliablePathCommandReceivers| {
            let command = try_recv_request_command_after_path_proofs_for_test(receivers)
                .expect("actual initial or deferred-wake notice");
            let ReliablePathCommand::PreparedOriginal(work) = command else {
                panic!("source must remain an unbound weak notice");
            };
            work
        };
    for _round in 0..2 {
        let a_work = take_notice(&mut a_receivers);
        let mut b_work = Some(take_notice(&mut b_receivers.lock().unwrap()));
        let a_ready = a_receivers
            .writer_ready_boundary(a.path_instance_id)
            .unwrap();
        let stale_case = matches!(
            case,
            PreparedWriterClaimCase::FreshWriterNotReady
                | PreparedWriterClaimCase::FreshReadyDisplacesStale
        );
        if stale_case {
            assert!(a_ready.receipt().is_current());
            assert!(b_commands.writer_boundary().snapshot().is_none());
            let mut state = shared.lock();
            let Some((_, queued)) = state.sender_queue.front() else {
                panic!("actual admitted source is present");
            };
            let ReliableRelayQueuedWorkKind::Data(payload) = &queued.kind else {
                panic!("the source producer retained raw unclaimed data");
            };
            let frame = state.send_stream.prepare_data(payload.clone()).unwrap();
            let RequestProductState {
                sender, remotes, ..
            } = &mut *state;
            sender
                .multipath
                .prepare_original_claim(&context, remotes, &frame)
                .expect("actual current membership has no missing Original incarnation");
            let observe = |state: &RequestProductState| {
                let inputs = super::multipath::RequestRelayNativeCapture::new(
                    state.remotes.membership_generation(),
                    &state.remotes.paths,
                )
                .resolve();
                state
                    .sender
                    .multipath
                    .observe_original_claim_from_inputs(
                        &context,
                        &state.remotes,
                        &frame,
                        TrafficClass::Throughput,
                        true,
                        inputs,
                    )
                    .expect("actual exact current Native/attachment receipt")
            };
            let fresh = observe(&state);
            let plan = state
                .sender
                .multipath
                .plan_original_claim_from_observation(
                    &context,
                    &fresh,
                    &fresh,
                    &state.remotes,
                    &frame,
                    TrafficClass::Throughput,
                    ReliableDataAckFrontierState::Live,
                    &[a],
                )
                .expect("the actual sole Ready A has a valid ordinary plan before staleness");
            assert_eq!(plan.target().1, a);
            let RequestProductState {
                sender, remotes, ..
            } = &mut *state;
            assert!(
                sender.mark_request_path_stale(&context, remotes, a, TrafficClass::Throughput,)
            );
            assert!(sender.request_path_is_stale(a));
            assert!(!sender.request_path_is_stale(b));
            let current = observe(&state);
            assert_eq!(current.paths.len(), 2);
            assert_eq!(
                current.membership_generation,
                state.remotes.membership_generation()
            );
            assert!(current.paths.iter().any(|path| path.instance == a));
            let b_snapshot = current
                .paths
                .iter()
                .find(|path| path.instance == b)
                .and_then(|path| path.shared_snapshot)
                .expect("fresh B is an actual scorable output");
            assert!(
                crate::scheduler::score_path(b_snapshot, TrafficClass::Throughput, quantum,)
                    .is_some()
            );
            assert!(
                state
                    .remotes
                    .paths
                    .iter()
                    .find(|path| path.instance() == b)
                    .unwrap()
                    .stream
                    .product_admission_active()
            );
            // The prior plan supplies only the exact A identity/load expectation.
            // This production helper recomputes CURRENT debt, position and W/P/E
            // after stale qualification was reset; no observation flag is changed.
            // F=0 is legitimately FirstPath, not an Additional/E-exhaustion test.
            assert!(
                state
                    .sender
                    .multipath
                    .bulk_original_data_authority_from_observation(
                        &context,
                        &state.remotes,
                        &plan,
                        &frame,
                        ReliableDataAckFrontierState::Live,
                        plan.load_expectation().is_some(),
                        &current,
                    )
                    .expect("stale A still has exact current Product authority")
                    .has_headroom()
            );
            assert_eq!(state.send_stream.next_offset(), 0);
            assert_eq!(state.send_stream.reinjection_bytes(), 0);
            assert_eq!(state.sender_queue.data_bytes(), quantum);
            assert!(state.send_stream.send_credit_bytes() >= quantum);
        }
        if !a_is_backup && case != PreparedWriterClaimCase::FreshWriterNotReady {
            b_receivers
                .lock()
                .unwrap()
                .writer_ready_boundary(b.path_instance_id)
                .unwrap();
        }
        let parked_b = Arc::new(std::sync::Mutex::new(None));
        let newly_ready_b = Arc::new(std::sync::Mutex::new(None));
        if case == PreparedWriterClaimCase::LoserWithdraws {
            let parked_b = parked_b.clone();
            let b_work = b_work.take().unwrap();
            let receivers = b_receivers.clone();
            let commands = b_commands.clone();
            shared.before_prepared_native_resolve_once_for_test(move || {
                // A is paused with Product unlocked. B's metadata refusal
                // preserves idle readiness; its subsequent genuine queued
                // control work withdraws only B's epoch before A resumes.
                let mut receivers = receivers.lock().unwrap();
                let b_ready = receivers.writer_ready_boundary(b.path_instance_id).unwrap();
                let PreparedOriginalClaim::Blocked(wait) = b_work.try_claim(b_ready) else {
                    panic!("the unchanged default ordinary choice must be A, not B");
                };
                commands
                    .try_enqueue_admitted_frame(Frame::Ping { nonce: 721 }, TrafficClass::Control)
                    .expect("actual occupying control work on B");
                let control = try_recv_request_command_after_path_proofs_for_test(&mut receivers)
                    .expect("the real B writer selects its queued control");
                assert!(matches!(
                    control,
                    ReliablePathCommand::SendFrame(Frame::Ping { nonce: 721 })
                ));
                receivers.withdraw_writer_ready();
                receivers
                    .release_pending_command_bytes(reliable_path_command_pending_bytes(&control));
                *parked_b.lock().unwrap() = Some((b_work, wait));
            });
        } else if case == PreparedWriterClaimCase::SelectedDrains {
            let commands = a_commands.clone();
            shared.before_prepared_native_resolve_once_for_test(move || {
                commands.begin_path_drain();
            });
        } else if case == PreparedWriterClaimCase::RegularBecomesReady {
            let receivers = b_receivers.clone();
            let newly_ready_b = newly_ready_b.clone();
            assert!(b_commands.writer_boundary().snapshot().is_none());
            shared.before_prepared_native_resolve_once_for_test(move || {
                let receipt = receivers
                    .lock()
                    .unwrap()
                    .writer_ready_boundary(b.path_instance_id)
                    .expect("healthy regular B reaches its actual writer boundary")
                    .receipt();
                *newly_ready_b.lock().unwrap() = Some(receipt);
            });
        }
        match a_work.try_claim(a_ready) {
            PreparedOriginalClaim::Claimed(frame) => {
                assert!(
                    !matches!(
                        case,
                        PreparedWriterClaimCase::SelectedDrains
                            | PreparedWriterClaimCase::RegularBecomesReady
                            | PreparedWriterClaimCase::FreshReadyDisplacesStale
                    ),
                    "A cannot commit after its drain or a newly ready regular B"
                );
                assert_eq!(
                    reliable_stream_frame_extent(&frame),
                    Some((0, quantum as u64, quantum))
                );
                let mut state = shared.lock();
                assert_eq!(state.sender_queue.data_bytes(), 0);
                assert_eq!(state.send_stream.reinjection_bytes(), quantum);
                assert_eq!(
                    state
                        .sender
                        .multipath
                        .latest_unacked_ranges_for_path_instance(a),
                    vec![OffsetRange {
                        start: 0,
                        end: quantum as u64
                    }]
                );
                assert!(
                    state
                        .sender
                        .multipath
                        .latest_unacked_ranges_for_path_instance(b)
                        .is_empty()
                );
                if stale_case {
                    assert!(state.sender.request_path_is_stale(a));
                    let ranges = [OffsetRange {
                        start: 0,
                        end: quantum as u64,
                    }];
                    state.send_stream.apply_ack(&ranges).unwrap();
                    let RequestProductState {
                        sender, remotes, ..
                    } = &mut *state;
                    let release = sender.multipath.apply_product_ack(
                        &context,
                        remotes,
                        &ranges,
                        std::time::Instant::now(),
                    );
                    assert_eq!(release.idle_original_data_instances.as_slice(), &[a]);
                    assert!(
                        release.data_ack_progress_paths.is_empty(),
                        "stale fallback delivery cannot manufacture proving/qualification progress"
                    );
                    assert!(sender.request_path_is_stale(a));
                    assert_eq!(state.send_stream.reinjection_bytes(), 0);
                }
                return (state.send_stream.next_offset(), quantum);
            }
            PreparedOriginalClaim::Blocked(wait) => {
                if case == PreparedWriterClaimCase::FreshWriterNotReady {
                    let state = shared.lock();
                    assert_eq!(state.send_stream.next_offset(), 0);
                    assert_eq!(state.send_stream.reinjection_bytes(), 0);
                    assert_eq!(state.sender_queue.data_bytes(), quantum);
                    assert!(state.sender.request_path_is_stale(a));
                    assert!(a_ready.receipt().is_current());
                    assert!(b_commands.writer_boundary().snapshot().is_none());
                    return (0, quantum);
                }
                if matches!(
                    case,
                    PreparedWriterClaimCase::RegularBecomesReady
                        | PreparedWriterClaimCase::FreshReadyDisplacesStale
                ) {
                    {
                        let state = shared.lock();
                        assert_eq!(state.send_stream.next_offset(), 0);
                        assert_eq!(state.send_stream.reinjection_bytes(), 0);
                        assert_eq!(state.sender_queue.data_bytes(), quantum);
                    }
                    let receipt = if case == PreparedWriterClaimCase::FreshReadyDisplacesStale {
                        b_commands
                            .writer_boundary()
                            .snapshot()
                            .expect("fresh B is Ready")
                    } else {
                        newly_ready_b
                            .lock()
                            .unwrap()
                            .take()
                            .expect("regular B became ready during A's unlocked observation")
                    };
                    let frame = {
                        let mut receivers = b_receivers.lock().unwrap();
                        let ready = receivers.writer_ready_boundary(b.path_instance_id).unwrap();
                        assert_eq!(ready.receipt(), receipt);
                        let PreparedOriginalClaim::Claimed(frame) =
                            b_work.take().unwrap().try_claim(ready)
                        else {
                            panic!("the newly ready regular must claim the unchanged shared head");
                        };
                        frame
                    };
                    assert_eq!(
                        reliable_stream_frame_extent(&frame),
                        Some((0, quantum as u64, quantum))
                    );
                    let state = shared.lock();
                    if stale_case {
                        assert!(state.sender.request_path_is_stale(a));
                        assert!(!state.sender.request_path_is_stale(b));
                    }
                    assert_eq!(state.sender_queue.data_bytes(), 0);
                    assert!(
                        state
                            .sender
                            .multipath
                            .latest_unacked_ranges_for_path_instance(a)
                            .is_empty()
                    );
                    assert_eq!(
                        state
                            .sender
                            .multipath
                            .latest_unacked_ranges_for_path_instance(b),
                        vec![OffsetRange {
                            start: 0,
                            end: quantum as u64
                        }]
                    );
                    return (state.send_stream.next_offset(), quantum);
                }
                if case == PreparedWriterClaimCase::SelectedDrains {
                    let state = shared.lock();
                    assert_eq!(state.send_stream.reinjection_bytes(), 0);
                    assert_eq!(state.sender_queue.data_bytes(), quantum);
                    assert!(a_commands.writer_boundary().snapshot().is_none());
                    assert_eq!(a_commands.pending_bytes(), 0);
                    return (state.send_stream.next_offset(), quantum);
                }
                assert!(
                    case == PreparedWriterClaimCase::LoserWithdraws,
                    "an uncontended current writer must pass actual admission"
                );
                let (b_work, b_wait) = parked_b
                    .lock()
                    .unwrap()
                    .take()
                    .expect("the competing actual B claim completed before A resumed");
                a_receivers.defer_prepared_work(a_work, wait);
                b_receivers
                    .lock()
                    .unwrap()
                    .defer_prepared_work(b_work, b_wait);
                let state = shared.lock();
                assert_eq!(state.send_stream.next_offset(), 0);
                assert_eq!(state.send_stream.reinjection_bytes(), 0);
                assert_eq!(state.sender_queue.data_bytes(), quantum);
                assert_eq!(a_commands.pending_bytes(), 0);
                assert_eq!(b_commands.pending_bytes(), 0);
                // The next round consumes both actual receiver-deferred wakes.
                // No source, ACK, Native, policy or capacity event is injected.
            }
            PreparedOriginalClaim::Empty if case == PreparedWriterClaimCase::SelectedDrains => {
                let state = shared.lock();
                assert_eq!(state.send_stream.reinjection_bytes(), 0);
                assert_eq!(state.sender_queue.data_bytes(), quantum);
                assert!(
                    state
                        .sender
                        .multipath
                        .latest_unacked_ranges_for_path_instance(a)
                        .is_empty()
                );
                assert!(a_commands.writer_boundary().snapshot().is_none());
                assert_eq!(a_commands.pending_bytes(), 0);
                return (state.send_stream.next_offset(), quantum);
            }
            _ => panic!("eligible current writers must either claim or await changed evidence"),
        }
    }
    let state = shared.lock();
    (state.send_stream.next_offset(), quantum)
}

#[tokio::test]
async fn prepared_request_ready_claim_control_progresses() {
    let (claimed, quantum) =
        prepared_competing_writer_claim_case(PreparedWriterClaimCase::Uncontended);
    assert_eq!(claimed, quantum as u64);
}

#[tokio::test]
async fn prepared_request_ready_claim_loser_withdrawal_cannot_prevent_progress() {
    let (claimed, quantum) =
        prepared_competing_writer_claim_case(PreparedWriterClaimCase::LoserWithdraws);
    assert_eq!(
        claimed, quantum as u64,
        "the losing writer's own withdrawal must not repeatedly invalidate the \
         ordinary winner and regenerate both retries without any claim"
    );
}

#[tokio::test]
async fn prepared_request_ready_claim_selected_drain_retains_source() {
    let (claimed, _) =
        prepared_competing_writer_claim_case(PreparedWriterClaimCase::SelectedDrains);
    assert_eq!(claimed, 0);
}

#[tokio::test]
async fn prepared_request_ready_claim_new_regular_displaces_backup() {
    let (claimed, quantum) =
        prepared_competing_writer_claim_case(PreparedWriterClaimCase::BackupFallback);
    assert_eq!(
        claimed, quantum as u64,
        "backup may serve after the regular writer's failed pass"
    );
    let (claimed, quantum) =
        prepared_competing_writer_claim_case(PreparedWriterClaimCase::RegularBecomesReady);
    assert_eq!(claimed, quantum as u64);
}

#[tokio::test]
async fn prepared_request_stale_ready_claim_waits_for_nonready_fresh_writer() {
    let (claimed, _) =
        prepared_competing_writer_claim_case(PreparedWriterClaimCase::FreshWriterNotReady);
    assert_eq!(
        claimed, 0,
        "temporary writer occupancy does not remove the non-stale structural \
         alternative or reactivate stale Original placement"
    );
}

#[tokio::test]
async fn prepared_request_stale_ready_claim_yields_to_fresh_ready_writer() {
    let (claimed, quantum) =
        prepared_competing_writer_claim_case(PreparedWriterClaimCase::FreshReadyDisplacesStale);
    assert_eq!(claimed, quantum as u64);
}

fn prepared_blocked_writer_idle_retry_case(path_count: usize) -> [usize; 2] {
    use crate::runtime::path::tcp::group::ClientTcpEndpointControlState;

    let endpoints = ["tcp://127.0.0.1:10724", "tcp://127.0.0.1:10725"];
    let context = client_test_context_with_paths(&endpoints[..path_count]);
    let stream_id = StreamId(722);
    let limits = context.mux_limits;
    let capacity = crate::runtime::path::commands::reliable_path_command_queue(limits);
    let mut writers = Vec::new();
    let mut opened = Vec::new();
    let mut input_guards = Vec::new();
    for index in 0..path_count {
        let (commands, receivers) = reliable_path_command_channels(capacity);
        let (path, input) =
            opened_request_stream_with_retained_input(stream_id, index, commands.clone());
        opened.push(path);
        input_guards.push(input);
        writers.push((commands, receivers));
    }
    let mut opened = opened.into_iter();
    let (mut remotes, _remote_input) =
        ReliableRelayRemoteSet::new(opened.next().unwrap(), capacity);
    for path in opened {
        assert_eq!(remotes.attach(path), ReliableRelayAttachOutcome::Attached);
    }
    let instances = remotes.path_instances();
    assert_eq!(instances.len(), path_count);
    for (instance, (_, receivers)) in instances.iter().zip(&mut writers) {
        consume_client_path_proof_for_test(receivers);
        context.install_relay_path_instance_for_test(*instance);
    }
    let sender = RequestSenderService::new(stream_id);
    let admission = sender.reliable_stream_source_admission(
        &context,
        &remotes,
        TrafficClass::Throughput,
        reliable_relay_buffer_len(limits),
    );
    assert!(admission.selected_path.is_some());
    let quantum = crate::model::capacity::adaptive_reliable_relay_chunk_bytes(
        admission.selected_path,
        TrafficClass::Throughput,
        limits,
    );
    assert!(quantum > 0 && quantum <= admission.window_bytes);
    let source = Bytes::from(vec![0x7c; quantum]);
    let mut queue = ReliableRelaySenderQueue::default();
    queue.push_data(source.clone());
    let shared = SharedRequestProduct::new(RequestProductState {
        sender,
        sender_queue: queue,
        send_stream: ReliableSendStream::new(stream_id, limits),
        last_send_ack: Default::default(),
        remotes,
        prepared: RequestPreparedSource::new(TrafficClass::Throughput, quantum),
    });
    let _actor_lifetime = shared.actor_lifetime();
    {
        let mut state = shared.lock();
        crate::runtime::relay::control::publish_prepared_request_work(
            &mut state,
            &shared,
            &context,
            TrafficClass::Throughput,
            quantum,
            true,
        );
    }
    // The source was admitted while paths were usable. A real non-draining
    // management transition now refuses new Original admission. This is not
    // an impossible fixture with U inserted behind zero initial mux credit.
    // Settle the only policy change before any claim's waits are armed.
    for index in 0..path_count {
        context.set_tcp_endpoint_control(index, ClientTcpEndpointControlState::Failed);
    }
    let assert_retained_refusal = || {
        let state = shared.lock();
        assert_eq!(state.send_stream.next_offset(), 0);
        assert_eq!(state.send_stream.reinjection_bytes(), 0);
        assert_eq!(state.sender_queue.data_bytes(), quantum);
        assert!(
            matches!(state.sender_queue.front().map(|(_, queued)| &queued.kind),
            Some(ReliableRelayQueuedWorkKind::Data(payload)) if payload == &source)
        );
        assert_eq!(state.prepared.last_claimed_at, None);
        assert_eq!(state.remotes.path_instances(), instances);
        for path in &state.remotes.paths {
            assert!(
                path.stream.product_admission_active(),
                "physical writer remains live"
            );
            let snapshot = context
                .reliable_path_snapshot_for_instance(path.instance())
                .unwrap();
            assert!(!crate::scheduler::path_is_schedulable(
                snapshot,
                TrafficClass::Throughput
            ));
            assert!(
                state
                    .sender
                    .multipath
                    .latest_unacked_ranges_for_path_instance(path.instance())
                    .is_empty()
            );
        }
    };
    assert_retained_refusal();

    // Establish every real initial idle opportunity before arming any claim
    // wait. Otherwise the first sibling appearance is a legitimate wake and
    // must not be mistaken for refusal-generated recurrence.
    let idle_receipts: Vec<_> = writers
        .iter_mut()
        .zip(&instances)
        .map(|((_, receivers), instance)| {
            receivers
                .writer_ready_boundary(instance.path_instance_id)
                .unwrap()
                .receipt()
        })
        .collect();
    for index in 0..path_count {
        let (_, receivers) = &mut writers[index];
        let ReliablePathCommand::PreparedOriginal(work) =
            try_recv_request_command_after_path_proofs_for_test(receivers)
                .expect("the producer's initial weak notice")
        else {
            panic!("no source payload may precede the actual claim");
        };
        let ready = receivers
            .writer_ready_boundary(instances[index].path_instance_id)
            .unwrap();
        assert_eq!(ready.receipt(), idle_receipts[index]);
        let PreparedOriginalClaim::Blocked(wait) = work.try_claim(ready) else {
            panic!("current policy refusal must park this otherwise live physical writer");
        };
        receivers.defer_prepared_work(work, wait);
        // The actual receiver owns one persistent idle epoch. Refused metadata
        // borrows it without producing Native work or a false transition.
        assert_eq!(
            receivers
                .writer_ready_boundary(instances[index].path_instance_id)
                .unwrap()
                .receipt(),
            idle_receipts[index],
        );
    }
    assert_retained_refusal();
    let stable_model = context.path_model_generation();
    let mut retries = [0; 2];
    for retry_count in &mut retries {
        for index in 0..path_count {
            let (_, receivers) = &mut writers[index];
            let Some(command) = try_recv_request_command_after_path_proofs_for_test(receivers)
            else {
                continue;
            };
            let ReliablePathCommand::PreparedOriginal(work) = command else {
                panic!("only a receiver-deferred weak retry can recur here");
            };
            *retry_count += 1;
            // Mirror the current native metadata path: borrow the receiver's
            // unchanged idle owner. Only real occupying work may withdraw it.
            let ready = receivers
                .writer_ready_boundary(instances[index].path_instance_id)
                .unwrap();
            assert_eq!(ready.receipt(), idle_receipts[index]);
            let PreparedOriginalClaim::Blocked(wait) = work.try_claim(ready) else {
                panic!("unchanged policy cannot authorize a claim during the retry cycle");
            };
            receivers.defer_prepared_work(work, wait);
            assert_eq!(
                receivers
                    .writer_ready_boundary(instances[index].path_instance_id)
                    .unwrap()
                    .receipt(),
                idle_receipts[index],
            );
        }
        assert_retained_refusal();
        assert_eq!(context.path_model_generation(), stable_model);
        for (commands, _) in &writers {
            assert_eq!(commands.pending_bytes(), 0);
            assert_eq!(commands.writer_pending_bytes(), 0);
        }
    }
    retries
}

#[tokio::test]
async fn prepared_blocked_single_writer_parks_without_idle_retry() {
    assert_eq!(prepared_blocked_writer_idle_retry_case(1), [0, 0]);
}

#[tokio::test]
async fn prepared_blocked_writers_do_not_regenerate_idle_retries() {
    assert_eq!(
        prepared_blocked_writer_idle_retry_case(2),
        [0, 0],
        "refused metadata attempts must not regenerate sibling retries in successive idle cycles without source, policy, Native or admission progress",
    );
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PreparedAdvisoryLockCut {
    Uncontended,
    Initial,
    AfterNativeCapture,
}

fn prepared_advisory_lock_case(cut: PreparedAdvisoryLockCut, cancel_after_busy: bool) {
    use futures::FutureExt;
    use std::sync::mpsc;

    // This bounds a deliberately blocked test thread, not Product latency or
    // any runtime policy. Both old blocking paths must release/join on RED.
    const CLAIM_COMPLETION_GUARD: Duration = Duration::from_secs(1);
    struct ReleaseHolder {
        start: mpsc::Sender<()>,
        release: mpsc::Sender<()>,
    }
    impl Drop for ReleaseHolder {
        fn drop(&mut self) {
            // Also start a not-yet-reached hook's holder during unwinding, so
            // scoped joining never strands it waiting for an acquisition.
            let _ = self.start.send(());
            let _ = self.release.send(());
        }
    }

    let stream_id = StreamId(723);
    let context = client_test_context_with_paths(&["tcp://127.0.0.1:10726"]);
    let limits = context.mux_limits;
    let capacity = crate::runtime::path::commands::reliable_path_command_queue(limits);
    let (commands, mut receivers) = reliable_path_command_channels(capacity);
    let (opened, _source_input) =
        opened_request_stream_with_retained_input(stream_id, 0, commands.clone());
    let (mut remotes, _remote_input) = ReliableRelayRemoteSet::new(opened, capacity);
    let instance = remotes.paths[0].instance();
    let initial_proof = try_recv_reliable_path_priority_command(&mut receivers).unwrap();
    assert!(matches!(
        initial_proof,
        ReliablePathCommand::SendFrame(Frame::PathProofData { .. })
    ));
    receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&initial_proof));
    context.install_relay_path_instance_for_test(instance);
    remotes.retry_pending_path_proofs(&context);
    let proof = try_recv_reliable_path_priority_command(&mut receivers).unwrap();
    let ReliablePathCommand::SendFrame(frame @ Frame::PathProofData { .. }) = &proof else {
        panic!("acknowledge the actual current attachment challenge");
    };
    let mut tracker = crate::runtime::path::PathProofTracker::from_limits(limits);
    tracker.record_sent_frame(frame);
    let Frame::PathProofData {
        path_id,
        proof_id,
        payload,
    } = frame
    else {
        unreachable!();
    };
    let receipt = tracker
        .acknowledge(*path_id, *proof_id, payload.len().try_into().unwrap())
        .unwrap();
    context.mark_relay_path_proof_observation(
        instance.key.underlay,
        instance.key.index,
        instance.path_instance_id,
        receipt,
    );
    receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&proof));
    let sender = RequestSenderService::new(stream_id);
    let admission = sender.reliable_stream_source_admission(
        &context,
        &remotes,
        TrafficClass::Throughput,
        reliable_relay_buffer_len(limits),
    );
    let selected = admission
        .selected_path
        .expect("actual default singleton admission");
    let quantum = crate::model::capacity::adaptive_reliable_relay_chunk_bytes(
        Some(selected),
        TrafficClass::Throughput,
        limits,
    );
    assert!(quantum > 0 && quantum <= admission.window_bytes);
    let source = Bytes::from(vec![0x7d; quantum]);
    let mut queue = ReliableRelaySenderQueue::default();
    queue.push_data(source.clone());
    let shared = SharedRequestProduct::new(RequestProductState {
        sender,
        sender_queue: queue,
        send_stream: ReliableSendStream::new(stream_id, limits),
        last_send_ack: Default::default(),
        remotes,
        prepared: RequestPreparedSource::new(TrafficClass::Throughput, quantum),
    });
    let actor_lifetime = shared.actor_lifetime();
    {
        let mut state = shared.lock();
        crate::runtime::relay::control::publish_prepared_request_work(
            &mut state,
            &shared,
            &context,
            TrafficClass::Throughput,
            quantum,
            true,
        );
    }
    let ReliablePathCommand::PreparedOriginal(work) =
        try_recv_request_command_after_path_proofs_for_test(&mut receivers).unwrap()
    else {
        panic!("the actual producer must publish an unbound weak notice");
    };
    let idle = receivers
        .writer_ready_boundary(instance.path_instance_id)
        .unwrap()
        .receipt();
    let snapshot = |state: &RequestProductState| {
        (
            state.send_stream.next_offset(),
            state.send_stream.reinjection_bytes(),
            state.sender_queue.data_bytes(),
            state
                .sender_queue
                .front()
                .map(|(_, work)| match &work.kind {
                    ReliableRelayQueuedWorkKind::Data(payload) => payload.clone(),
                    _ => panic!("same retained source kind"),
                }),
            state
                .sender
                .multipath
                .latest_unacked_ranges_for_path_instance(instance),
            state.prepared.last_claimed_at,
            commands.pending_bytes(),
            commands.writer_pending_bytes(),
            commands.writer_boundary().snapshot(),
        )
    };
    let before = snapshot(&shared.lock());
    assert_eq!(before.0, 0);
    assert_eq!(before.1, 0);
    assert_eq!(before.2, quantum);
    assert_eq!(before.3.as_ref(), Some(&source));
    assert!(before.4.is_empty());
    assert_eq!(before.5, None);
    assert_eq!((before.6, before.7), (0, 0));
    assert_eq!(before.8.as_ref(), Some(&idle));

    let (work, mut receivers, first_claim) = if cut == PreparedAdvisoryLockCut::Uncontended {
        let ready = receivers
            .writer_ready_boundary(instance.path_instance_id)
            .unwrap();
        let claim = work.try_claim(ready);
        (work, receivers, claim)
    } else {
        std::thread::scope(|scope| {
            let (start_tx, start_rx) = mpsc::channel();
            let (held_tx, held_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            let release = ReleaseHolder {
                start: start_tx.clone(),
                release: release_tx,
            };
            let owner = &shared;
            let take_snapshot = &snapshot;
            let holder = scope.spawn(move || {
                start_rx.recv().expect("start actual Product holder");
                let state = owner.lock();
                let _ = held_tx.send(());
                release_rx.recv().expect("bounded test releases Product");
                // Taken before dropping P: even old blocking code cannot
                // modify these fields between this snapshot and release.
                take_snapshot(&state)
            });
            if cut == PreparedAdvisoryLockCut::Initial {
                start_tx.send(()).unwrap();
                held_rx
                    .recv_timeout(CLAIM_COMPLETION_GUARD)
                    .expect("holder acquires Product before the initial claim");
            } else {
                shared.before_prepared_native_resolve_once_for_test(move || {
                    start_tx.send(()).unwrap();
                    held_rx
                        .recv_timeout(CLAIM_COMPLETION_GUARD)
                        .expect("holder acquires Product after the first guard is released");
                });
            }
            let (returned_tx, returned_rx) = mpsc::channel();
            let runtime = tokio::runtime::Handle::current();
            let writer = scope.spawn(move || {
                let _runtime = runtime.enter();
                let ready = receivers
                    .writer_ready_boundary(instance.path_instance_id)
                    .unwrap();
                let result = work.try_claim(ready);
                let _ = returned_tx.send(());
                (work, receivers, result)
            });
            let returned_while_held = returned_rx.recv_timeout(CLAIM_COMPLETION_GUARD).is_ok();
            drop(release);
            let held_snapshot = holder
                .join()
                .expect("holder exits without poisoning Product");
            let result = writer
                .join()
                .expect("writer finishes after bounded cleanup");
            assert_eq!(
                held_snapshot, before,
                "contention must not mutate source, C, cache, flight, charges or readiness"
            );
            assert!(
                returned_while_held,
                "actual prepared writer advisory Product lock must return Busy before the holder releases; the 1s guard is test-thread cleanup, not desired runtime latency",
            );
            result
        })
    };

    let frame = if cut == PreparedAdvisoryLockCut::Uncontended {
        let PreparedOriginalClaim::Claimed(frame) = first_claim else {
            panic!("same real source and writer must claim without contention");
        };
        frame
    } else {
        let PreparedOriginalClaim::Busy(wait) = first_claim else {
            panic!("advisory Product contention must return the existing Busy outcome");
        };
        // Poll before acquiring any validation guard: its later unlock must
        // not mask a missing notification from the actual contending holder.
        assert_eq!(
            wait.now_or_never(),
            Some(()),
            "unlock before the first poll must survive"
        );
        assert_eq!(snapshot(&shared.lock()), before);
        if cancel_after_busy {
            drop(actor_lifetime);
            let ready = receivers
                .writer_ready_boundary(instance.path_instance_id)
                .unwrap();
            assert_eq!(ready.receipt(), idle);
            assert!(matches!(
                work.try_claim(ready),
                PreparedOriginalClaim::Empty
            ));
            assert_eq!(
                snapshot(&shared.lock()),
                before,
                "Busy is not authority to claim after logical cancellation"
            );
            return;
        }
        let ready = receivers
            .writer_ready_boundary(instance.path_instance_id)
            .unwrap();
        assert_eq!(
            ready.receipt(),
            idle,
            "a failed metadata attempt preserves idle ownership"
        );
        let PreparedOriginalClaim::Claimed(frame) = work.try_claim(ready) else {
            panic!("identical current source and writer must claim after the actual unlock");
        };
        frame
    };
    assert!(matches!(&frame, Frame::StreamData { offset: 0, payload, .. } if payload == &source));
    let charge = receivers.register_claimed_writer_frame(&frame);
    assert!(charge >= quantum);
    assert_eq!(commands.pending_bytes(), charge as u64);
    assert_eq!(commands.writer_pending_bytes(), charge as u64);
    {
        let state = shared.lock();
        assert_eq!(state.send_stream.next_offset(), quantum as u64);
        assert_eq!(state.send_stream.reinjection_bytes(), quantum);
        assert_eq!(state.sender_queue.data_bytes(), 0);
        assert_eq!(
            state
                .sender
                .multipath
                .latest_unacked_ranges_for_path_instance(instance),
            vec![OffsetRange {
                start: 0,
                end: quantum as u64
            }]
        );
    }
    receivers.release_pending_command_bytes(charge);
    assert_eq!(
        (commands.pending_bytes(), commands.writer_pending_bytes()),
        (0, 0)
    );
}

#[tokio::test]
async fn prepared_advisory_product_lock_uncontended_claim_control() {
    prepared_advisory_lock_case(PreparedAdvisoryLockCut::Uncontended, false);
}

#[tokio::test]
async fn prepared_advisory_product_lock_initial_returns_busy() {
    prepared_advisory_lock_case(PreparedAdvisoryLockCut::Initial, false);
}

#[tokio::test]
async fn prepared_advisory_product_lock_after_native_capture_returns_busy() {
    prepared_advisory_lock_case(PreparedAdvisoryLockCut::AfterNativeCapture, false);
}

#[tokio::test]
async fn prepared_advisory_product_lock_busy_cancellation_preserves_source() {
    prepared_advisory_lock_case(PreparedAdvisoryLockCut::Initial, true);
}
