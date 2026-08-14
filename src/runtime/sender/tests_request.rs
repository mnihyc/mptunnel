use super::test_support::*;
use super::*;
use crate::config::ResourceLimits;
use crate::model::capacity::adaptive_reliable_relay_inflight_bytes;
use crate::model::work::{
    ReliableWorkClass, reliable_critical_tail_reinjection_limit_bytes,
    reliable_reinjection_service_limit_bytes,
};
use crate::protocol::{PathId, SessionId};
use crate::runtime::path::commands::{
    ReliablePathCommand, recv_reliable_path_command, reliable_path_command_channels,
    reliable_path_command_pending_bytes, try_recv_reliable_path_command,
    try_recv_reliable_path_priority_command,
};
use crate::runtime::sender::response::ServerResponseSenderService;
use crate::runtime::stream::response::ResponseStreamBinding;
use crate::runtime::stream::{
    FixedReliablePathOutput, ReliableRelayAttachOutcome, ReliableRelayRemotePath,
};
use crate::transport::PathSpec;
use std::sync::Arc;

#[test]
fn budgeted_critical_reinjection_preempts_original_data_and_debits_budget() {
    let mux_limits = MuxLimits::default();
    let stream_id = StreamId(79);
    let mut sender = ServerResponseSenderService::new_with_performance(
        SessionId(79),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 1,
        },
    );
    let startup_floor = sender_extra_traffic_startup_floor_bytes(mux_limits);

    sender.enqueue_data_for_lane(Bytes::from_static(b"owner"), TrafficClass::Throughput);
    assert!(
        sender
            .enqueue_reinjection_frame_with_priority(
                Frame::StreamData {
                    stream_id,
                    offset: 0,
                    payload: Bytes::from(vec![0x7a; startup_floor]),
                },
                mux_limits,
                true,
            )
            .is_some(),
        "startup reinjection floor should be spendable"
    );

    assert_eq!(
        sender.queue.front().map(|(lane, _)| lane),
        Some(ReliableWorkClass::Reinjection)
    );
    assert_eq!(
        sender.reinjection_extra_budget_remaining(mux_limits),
        0,
        "critical priority is not budget bypass"
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
async fn client_ack_gap_model_separates_owner_transport_from_reinjection_output() {
    let stream_id = StreamId(90);
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10260?initial-srtt-ms=500&initial-rate-mbps=400",
        "quic://127.0.0.1:10261?initial-srtt-ms=40&initial-rate-mbps=200",
        "quic://127.0.0.1:10262?initial-srtt-ms=5&initial-rate-mbps=500",
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

    let limits = MuxLimits::default();
    let mut send_stream = ReliableSendStream::new(stream_id, limits);
    let blocked = send_stream
        .send_data(Bytes::from(vec![0x41; 4096]))
        .expect("blocked owner data");
    send_stream
        .send_data(Bytes::from(vec![0x42; 4096]))
        .expect("later delivered data");
    let mut sender = RequestSenderService::new(stream_id);
    sender.record_original_frame_for_test(
        remotes
            .paths
            .iter()
            .find(|path| path.key().underlay == UnderlayProtocol::Tcp)
            .map(ReliableRelayRemotePath::instance)
            .expect("slow TCP original path"),
        &blocked,
    );
    let ranges = [OffsetRange {
        start: 4096,
        end: 8192,
    }];

    let observation = sender.data_ack_gap_reinjection_model(
        &context,
        &remotes,
        &send_stream,
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
    seed_client_bulk_evidence_for_test(
        &context,
        RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        },
    );
    let observation = sender.data_ack_gap_reinjection_model(
        &context,
        &remotes,
        &send_stream,
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
        Some(reinjection_path),
        0,
        limits.max_repair_bytes,
        limits,
    );
    assert_eq!(
        persistent_service_limit,
        adaptive_reliable_relay_inflight_bytes(
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

    seed_client_bulk_evidence_for_test(
        &context,
        RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 1,
        },
    );

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
    sender
        .send_reinjection_frame(&context, &mut remotes, blocked.clone(), bound_cause)
        .await
        .expect("bound repair uses headroom independent of shared writer work");
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
    assert!(matches!(
        sender
            .send_reinjection_frame(&context, &mut remotes, blocked.clone(), bound_cause)
            .await,
        Err(RuntimeError::ReliablePathSessionClosed)
    ));
    let mut queue = ReliableRelaySenderQueue::default();
    queue.push_critical_reinjection_with_cause(blocked, bound_cause);
    let dispatch = sender
        .dispatch_client_queued_work(
            &context,
            TrafficClass::Throughput,
            &mut remotes,
            &mut send_stream,
            &mut queue,
            4096,
        )
        .await
        .expect("stale bound reinjection is cancelled without aborting the stream");
    assert!(matches!(
        dispatch,
        ClientQueuedDispatch::PersistentReinjectionCancelled
    ));
    assert!(queue.is_empty());
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
    seed_client_bulk_evidence_for_test(&context, tcp.key);
    seed_client_bulk_evidence_for_test(
        &context,
        RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        },
    );

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
            )
            .await
            .expect("live tail dispatch"),
        ClientQueuedDispatch::Reinjection { payload_bytes: 64 }
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
async fn client_path_failure_releases_path_load_before_cleanup_waits() {
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
    assert_eq!(
        context.health().lock().expect("path health lock").tcp[0].active_flows,
        1
    );
    let mut sender = RequestSenderService::new(stream_id);

    let task_context = context.clone();
    let failure = tokio::spawn(async move {
        let removed = sender
            .fail_client_path_instance(&task_context, &mut remotes, instance)
            .await;
        (removed, sender, remotes)
    });
    tokio::task::yield_now().await;

    assert_eq!(
        context.health().lock().expect("path health lock").tcp[0].active_flows,
        0,
        "a detached path must release its load before cleanup can await"
    );
    assert!(
        !failure.is_finished(),
        "the full control queue must keep detach cleanup pending for the race assertion"
    );
    assert!(matches!(
        recv_reliable_path_command(&mut receivers).await,
        Some(ReliablePathCommand::CloseStream(StreamId(999)))
    ));
    assert!(matches!(
        recv_reliable_path_command(&mut receivers).await,
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
    ));
    assert!(matches!(
        recv_reliable_path_command(&mut receivers).await,
        Some(ReliablePathCommand::SendFrame(Frame::StreamDetach { stream_id: id }))
            if id == stream_id
    ));
    assert!(matches!(
        recv_reliable_path_command(&mut receivers).await,
        Some(ReliablePathCommand::CloseStream(id)) if id == stream_id
    ));
    let (removed, _, remotes) = failure.await.expect("path failure task");
    assert!(removed);
    assert!(remotes.is_empty());
}

#[tokio::test]
async fn client_path_failure_releases_optional_load_before_cleanup_waits() {
    let stream_id = StreamId(125);
    let context = Arc::new(client_test_context_with_paths(&[
        "tcp://127.0.0.1:10331?initial-srtt-ms=20&initial-rate-mbps=500",
        "tcp://127.0.0.1:10332?initial-srtt-ms=20&initial-rate-mbps=500",
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
    let lease = context
        .try_reserve_relay_path_load_if_unchanged(candidate.key, TrafficClass::Throughput, 0, 0)
        .expect("reserve optional path load");
    remotes.commit_path_instance_load_claim(candidate, lease);
    let mut sender = RequestSenderService::new(stream_id);

    let task_context = context.clone();
    let failure = tokio::spawn(async move {
        let removed = sender
            .fail_client_path_instance(&task_context, &mut remotes, candidate)
            .await;
        (removed, sender, remotes)
    });
    tokio::task::yield_now().await;

    assert_eq!(
        context.health().lock().expect("path health lock").tcp[1].active_flows,
        0,
        "a removed optional path must release load before detach can block"
    );
    assert!(!failure.is_finished());
    assert!(matches!(
        recv_reliable_path_command(&mut candidate_rx).await,
        Some(ReliablePathCommand::CloseStream(StreamId(999)))
    ));
    loop {
        match recv_reliable_path_command(&mut candidate_rx).await {
            Some(ReliablePathCommand::SendFrame(Frame::StreamDetach { stream_id: id }))
                if id == stream_id =>
            {
                break;
            }
            Some(_) => continue,
            None => panic!("candidate command channel closed before detach"),
        }
    }
    assert!(matches!(
        recv_reliable_path_command(&mut candidate_rx).await,
        Some(ReliablePathCommand::CloseStream(id)) if id == stream_id
    ));
    let (removed, _, _) = failure.await.expect("path failure task");
    assert!(removed);
}

#[test]
fn client_reinjection_extra_budget_is_cumulative_not_per_event() {
    let mux_limits = MuxLimits::default();
    let stream_id = StreamId(93);
    let mut sender = RequestSenderService::new_with_performance(
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 1,
        },
    );
    let mut sender_queue = ReliableRelaySenderQueue::default();
    let startup_floor = sender_extra_traffic_startup_floor_bytes(mux_limits);
    let reinjection_payload = Bytes::from(vec![0x33; startup_floor]);

    assert!(sender.enqueue_reinjection_frame_with_priority(
        &mut sender_queue,
        Frame::StreamData {
            stream_id,
            offset: 0,
            payload: reinjection_payload.clone(),
        },
        RelaySendCause::AckGapReinjection,
        mux_limits,
        false,
    ));
    assert!(!sender.enqueue_reinjection_frame_with_priority(
        &mut sender_queue,
        Frame::StreamData {
            stream_id,
            offset: startup_floor as u64,
            payload: reinjection_payload.clone(),
        },
        RelaySendCause::AckGapReinjection,
        mux_limits,
        false,
    ));

    sender.record_delivered_data_for_test(startup_floor.saturating_mul(100));
    assert!(sender.enqueue_reinjection_frame_with_priority(
        &mut sender_queue,
        Frame::StreamData {
            stream_id,
            offset: (startup_floor * 2) as u64,
            payload: reinjection_payload,
        },
        RelaySendCause::PathFailureReinjection,
        mux_limits,
        false,
    ));
}

#[test]
fn client_critical_reinjection_closes_tail_after_optional_budget_exhaustion() {
    let mux_limits = MuxLimits::default();
    let stream_id = StreamId(95);
    let mut sender = RequestSenderService::new_with_performance(
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 1,
        },
    );
    let mut sender_queue = ReliableRelaySenderQueue::default();
    let startup_floor = sender_extra_traffic_startup_floor_bytes(mux_limits);
    let frame = Frame::StreamData {
        stream_id,
        offset: 0,
        payload: Bytes::from(vec![0x33; startup_floor]),
    };
    assert!(sender.enqueue_reinjection_frame_with_priority(
        &mut sender_queue,
        frame,
        RelaySendCause::AckGapReinjection,
        mux_limits,
        false,
    ));

    let closure_frame = Frame::StreamData {
        stream_id,
        offset: startup_floor as u64,
        payload: Bytes::from_static(b"tail"),
    };
    assert!(!sender.enqueue_reinjection_frame_with_priority(
        &mut sender_queue,
        closure_frame.clone(),
        RelaySendCause::AckGapReinjection,
        mux_limits,
        false,
    ));

    sender.enqueue_critical_reinjection_frame(
        &mut sender_queue,
        closure_frame,
        RelaySendCause::AckGapReinjection,
    );
    assert_eq!(sender.extra_traffic_budget_remaining(mux_limits), 0);
}
