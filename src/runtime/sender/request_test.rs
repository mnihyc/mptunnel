use super::test_support::*;
use super::*;
use crate::config::ResourceLimits;
use crate::model::work::{ReliableWorkClass, reliable_persistent_ack_gap_reinjection_limit_bytes};
use crate::protocol::{PathId, SessionId};
use crate::runtime::path::commands::{
    ReliablePathCommand, recv_reliable_path_command, reliable_path_command_channels,
    try_recv_reliable_path_command, try_recv_reliable_path_priority_command,
};
use crate::runtime::sender::response::ServerResponseSenderService;
use crate::runtime::stream::response::ResponseStreamBinding;
use crate::runtime::stream::{FixedReliablePathOutput, ReliableRelayRemotePath};
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
        "tcp://127.0.0.1:10260?srtt-ms=500&rate-mbps=400",
        "udp://127.0.0.1:10261?srtt-ms=40&rate-mbps=200",
        "udp://127.0.0.1:10262?srtt-ms=5&rate-mbps=500",
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
    assert!(
        reliable_persistent_ack_gap_reinjection_limit_bytes(
            Some(reinjection_path),
            original_underlay,
            TrafficClass::Throughput,
            limits.max_repair_bytes,
            limits,
        ) > adaptive_reliable_relay_reinjection_bytes(
            Some(reinjection_path),
            TrafficClass::Throughput,
            limits,
        ),
        "TCP owner persistence controls amplification even when QUIC carries the reinjection"
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
    let bound_cause =
        RelaySendCause::persistent_client_ack_gap_reinjection(reinjection_target, reinjection_path);
    sender
        .send_reinjection_frame(&context, &mut remotes, blocked.clone(), bound_cause)
        .await
        .expect("bound repair uses headroom independent of fresh data");
    assert!(matches!(
        try_recv_reliable_path_command(&mut udp_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            ref payload,
            ..
        })) if payload.len() == 4096
    ));
    assert!(matches!(
        try_recv_reliable_path_command(&mut udp_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            ref payload,
            ..
        })) if payload.as_ref() == b"busy"
    ));
    assert!(
        try_recv_reliable_path_command(&mut proof_only_receivers).is_none(),
        "an amplified batch stays bound to the modeled output instead of switching to another proven output"
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

    let outcome = sender.apply_request_product_ack(
        &context,
        &remotes,
        &mut send_stream,
        &[OffsetRange {
            start: 0,
            end: 4096,
        }],
    );

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
            &recv_stream,
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

    let retried = sender
        .send_recv_progress(
            &mut remotes,
            &context,
            &recv_stream,
            &mut progress,
            RelayRecvProgressSend::new(None, TrafficClass::Throughput, false),
        )
        .await
        .expect("recv progress should retry once queue capacity returns");

    assert!(
        retried,
        "progress watermark must roll back after a blocked enqueue"
    );
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
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
            &recv_stream,
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
async fn client_recv_progress_uses_lowest_eta_attached_path() {
    let stream_id = StreamId(96);
    let tcp_path = "tcp://127.0.0.1:10270?srtt-ms=500&rate-mbps=50"
        .parse::<PathSpec>()
        .expect("tcp path");
    let udp_path = "udp://127.0.0.1:10271?srtt-ms=5&rate-mbps=500"
        .parse::<PathSpec>()
        .expect("udp path");
    let context = ClientPathContext::new(
        vec![tcp_path, udp_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    let (tcp_commands, mut tcp_rx) = reliable_path_command_channels(8);
    let (udp_commands, mut udp_rx) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream_with_underlay(stream_id, UnderlayProtocol::Tcp, 0, tcp_commands),
        8,
    );
    consume_client_path_proof_for_test(&mut tcp_rx);
    remotes.attach_candidate(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        0,
        udp_commands,
    ));
    consume_client_path_proof_for_test(&mut udp_rx);
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
            &recv_stream,
            &mut progress,
            RelayRecvProgressSend::new(None, TrafficClass::Throughput, false),
        )
        .await
        .expect("recv progress should use a live return path");

    assert!(sent);
    assert!(try_recv_reliable_path_priority_command(&mut tcp_rx).is_none());
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut udp_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
    ));
}

#[tokio::test]
async fn client_recv_progress_uses_metrics_not_attachment_order() {
    let stream_id = StreamId(97);
    let tcp_path = "tcp://127.0.0.1:10272?srtt-ms=5&rate-mbps=500"
        .parse::<PathSpec>()
        .expect("tcp path");
    let udp_path = "udp://127.0.0.1:10273?srtt-ms=500&rate-mbps=50"
        .parse::<PathSpec>()
        .expect("udp path");
    let context = ClientPathContext::new(
        vec![tcp_path, udp_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    let (tcp_commands, mut tcp_rx) = reliable_path_command_channels(8);
    let (udp_commands, mut udp_rx) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream_with_underlay(stream_id, UnderlayProtocol::Tcp, 0, tcp_commands),
        8,
    );
    consume_client_path_proof_for_test(&mut tcp_rx);
    remotes.attach_candidate(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        0,
        udp_commands,
    ));
    consume_client_path_proof_for_test(&mut udp_rx);
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
            &recv_stream,
            &mut progress,
            RelayRecvProgressSend::new(None, TrafficClass::Latency, true),
        )
        .await
        .expect("recv progress should use a live return path");

    assert!(sent);
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut tcp_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
    ));
    assert!(try_recv_reliable_path_priority_command(&mut udp_rx).is_none());
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
        "tcp://127.0.0.1:10331?srtt-ms=20&rate-mbps=500",
        "tcp://127.0.0.1:10332?srtt-ms=20&rate-mbps=500",
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
