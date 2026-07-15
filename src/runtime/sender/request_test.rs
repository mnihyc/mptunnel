use super::test_support::*;
use super::*;
use crate::config::ResourceLimits;
use crate::model::work::ReliableWorkClass;
use crate::protocol::{PathId, SessionId, TargetAddr};
use crate::runtime::path::commands::{
    ReliablePathCommand, recv_reliable_path_command, reliable_path_command_channels,
    try_recv_reliable_path_command, try_recv_reliable_path_priority_command,
};
use crate::runtime::relay::ReliableRelayRemotePath;
use crate::runtime::relay::io::reliable_persistent_ack_gap_repair_limit_bytes;
use crate::runtime::sender::response::ServerResponseSenderService;
use crate::runtime::stream::FixedReliablePathOutput;
use crate::runtime::stream::response::ResponseStreamBinding;
use crate::transport::PathSpec;
use std::net::SocketAddr;
use std::sync::Arc;

#[test]
fn budgeted_critical_repair_preempts_owner_data_and_debits_budget() {
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

    sender.enqueue_data_for_lane(Bytes::from_static(b"owner"), FlowLane::Throughput);
    assert!(
        sender
            .enqueue_repair_frame_with_priority(
                Frame::StreamData {
                    stream_id,
                    offset: 0,
                    payload: Bytes::from(vec![0x7a; startup_floor]),
                },
                mux_limits,
                true,
            )
            .is_some(),
        "startup repair floor should be spendable"
    );

    assert_eq!(
        sender.queue.front().map(|(lane, _)| lane),
        Some(ReliableWorkClass::Repair)
    );
    assert_eq!(
        sender.repair_extra_budget_remaining(mux_limits),
        0,
        "critical priority is not budget bypass"
    );
}

fn request_handle(output: ReliablePathStreamOutput) -> ReliablePathStreamHandle {
    ReliablePathStreamHandle {
        stream_id: StreamId(7),
        max_offset: 64 * 1024,
        lane: FlowLane::Throughput,
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
        FlowLane::Control,
        CarrierEmitMode::Classified,
    )
    .expect("classified control uses the priority queue");
    assert!(matches!(
        emit_request_frame_with_mode(
            &handle,
            Frame::Ping { nonce: 2 },
            FlowLane::Control,
            CarrierEmitMode::Classified,
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    emit_request_frame_with_mode(
        &handle,
        Frame::Ping { nonce: 3 },
        FlowLane::Control,
        CarrierEmitMode::StreamOrdered,
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
        FlowLane::Throughput,
    );
    let handle = request_handle(ReliablePathStreamOutput::Switchable(binding));

    assert!(matches!(
        emit_request_frame_with_mode(
            &handle,
            Frame::Ping { nonce: 1 },
            FlowLane::Control,
            CarrierEmitMode::Classified,
        ),
        Err(RuntimeError::Protocol("request relay path is not fixed"))
    ));
}

#[tokio::test]
async fn client_ack_gap_model_separates_owner_transport_from_repair_output() {
    let stream_id = StreamId(90);
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10260?srtt-ms=500&rate-mbps=400",
        "udp://127.0.0.1:10261?srtt-ms=40&rate-mbps=200",
        "udp://127.0.0.1:10262?srtt-ms=5&rate-mbps=500",
    ]);
    let (tcp_commands, _tcp_receivers) = reliable_path_command_channels(8);
    let (udp_commands, _udp_receivers) = reliable_path_command_channels(1);
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
    remotes.attach_for_validation(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Tcp,
        0,
        tcp_commands,
    ));
    remotes.attach_for_validation(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        1,
        proof_only_commands,
    ));
    consume_client_validation_proof_for_test(&mut proof_only_receivers);

    let limits = MuxLimits::default();
    let mut send_stream = ReliableSendStream::new(stream_id, limits);
    let blocked = send_stream
        .send_data(Bytes::from(vec![0x41; 4096]))
        .expect("blocked owner data");
    send_stream
        .send_data(Bytes::from(vec![0x42; 4096]))
        .expect("later delivered data");
    let mut sender = RequestSenderService::new(stream_id);
    sender.record_owner_frame_for_test(
        remotes
            .paths
            .iter()
            .find(|path| path.key().underlay == UnderlayProtocol::Tcp)
            .map(ReliableRelayRemotePath::instance)
            .expect("slow TCP validation owner"),
        &blocked,
    );
    let ranges = [OffsetRange {
        start: 4096,
        end: 8192,
    }];

    let (unproven_owner, owner_timing_path, unproven_repair_path) = sender
        .ack_gap_repair_path_model(
            &context,
            &remotes,
            &send_stream,
            &ranges,
            64 * 1024,
            FlowLane::Throughput,
        );
    assert_eq!(unproven_owner, Some(UnderlayProtocol::Tcp));
    assert_eq!(
        owner_timing_path.map(|snapshot| snapshot.srtt_ms),
        Some(500.0),
        "persistent-gap proof time follows the slow exact owner rather than the 40 ms Active repair output"
    );
    assert!(
        unproven_repair_path.is_none(),
        "a proof-only Validation output may carry a bounded repair quantum but must not authorize a BDP-sized burst from configured hints"
    );
    seed_client_bulk_evidence_for_test(
        &context,
        RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        },
    );
    let (owner_underlay, owner_timing_path, repair_path) = sender.ack_gap_repair_path_model(
        &context,
        &remotes,
        &send_stream,
        &ranges,
        64 * 1024,
        FlowLane::Throughput,
    );

    assert_eq!(owner_underlay, Some(UnderlayProtocol::Tcp));
    assert_eq!(
        owner_timing_path.map(|snapshot| snapshot.underlay),
        Some(UnderlayProtocol::Tcp)
    );
    assert_eq!(
        repair_path.map(|(_, snapshot)| snapshot.underlay),
        Some(UnderlayProtocol::Udp),
        "the exact ACK-gap selector must avoid the TCP owner and model the distinct QUIC repair output"
    );
    let (repair_target, repair_path) = repair_path.expect("distinct repair output");
    assert!(
        reliable_persistent_ack_gap_repair_limit_bytes(
            Some(repair_path),
            owner_underlay,
            FlowLane::Throughput,
            limits.max_repair_bytes,
            limits,
        ) > adaptive_reliable_relay_repair_bytes(Some(repair_path), FlowLane::Throughput, limits,),
        "TCP owner persistence controls amplification even when QUIC carries the repair"
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
            FlowLane::Throughput,
        )
        .expect("fill the modeled repair output after sizing");
    let bound_cause = RelaySendCause::persistent_client_ack_gap_repair(repair_target, repair_path);
    assert!(matches!(
        sender
            .send_repair_frame(&context, &mut remotes, blocked.clone(), bound_cause,)
            .await,
        Err(RuntimeError::SenderServiceBlocked)
    ));
    assert!(
        try_recv_reliable_path_command(&mut proof_only_receivers).is_none(),
        "an amplified batch stays bound to the modeled output instead of switching to another proven output"
    );

    let replacement = remotes
        .paths
        .iter_mut()
        .find(|path| path.instance() == repair_target.instance)
        .expect("modeled repair attachment remains present");
    replacement.instance_id = replacement.instance_id.saturating_add(1);
    assert!(matches!(
        sender
            .send_repair_frame(&context, &mut remotes, blocked.clone(), bound_cause)
            .await,
        Err(RuntimeError::ReliablePathSessionClosed)
    ));
    let mut queue = ReliableRelaySenderQueue::default();
    queue.push_critical_repair_with_cause(blocked, bound_cause);
    let dispatch = sender
        .dispatch_client_queued_work(
            &context,
            &ReliableRelayOpenSpec {
                target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80))),
            },
            FlowLane::Throughput,
            FlowLane::Throughput,
            &mut remotes,
            &mut send_stream,
            &mut queue,
            true,
            &HashSet::new(),
            4096,
        )
        .await
        .expect("stale bound repair is cancelled without aborting the stream");
    assert!(matches!(
        dispatch,
        ClientQueuedDispatch::PersistentRepairCancelled
    ));
    assert!(queue.is_empty());
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
            FlowLane::Control,
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
            RelayRecvProgressSend::new(None, FlowLane::Throughput, false),
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
            RelayRecvProgressSend::new(None, FlowLane::Throughput, false),
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
            FlowLane::Control,
        )
        .expect("prefill first priority queue");
    let (second_commands, mut second_rx) = reliable_path_command_channels(1);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, first_commands), 4);
    remotes.attach(opened_test_relay_stream(stream_id, 1, second_commands));
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
            RelayRecvProgressSend::new(None, FlowLane::Throughput, false),
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
async fn client_recv_progress_prefers_active_service_path_over_validation_probe() {
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
    let (udp_commands, _udp_rx) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream_with_underlay(stream_id, UnderlayProtocol::Tcp, 0, tcp_commands),
        8,
    );
    remotes.attach_for_validation(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        0,
        udp_commands,
    ));
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
            RelayRecvProgressSend::new(None, FlowLane::Throughput, false),
        )
        .await
        .expect("recv progress should use the active service return path");

    assert!(sent);
    assert!(
        matches!(
            try_recv_reliable_path_priority_command(&mut tcp_rx),
            Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
        ),
        "STREAM_ACK for received OwnerData should prefer the Active Service path; a lower-ETA validation probe must not own the product ACK clock while the Service path is usable"
    );
}

#[tokio::test]
async fn client_stall_recv_progress_prefers_accepted_repair_path() {
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
    remotes.attach_for_repair(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        0,
        udp_commands,
    ));
    let mut recv_stream = ReliableRecvStream::new(stream_id, MuxLimits::default());
    recv_stream
        .receive_data(0, Bytes::from_static(b"reply"))
        .expect("receive response bytes");
    let mut progress = ReliableRecvProgress::default();
    let mut sender = RequestSenderService::new(stream_id);

    let ordinary_sent = sender
        .send_recv_progress(
            &mut remotes,
            &context,
            &recv_stream,
            &mut progress,
            RelayRecvProgressSend::new(None, FlowLane::Latency, true),
        )
        .await
        .expect("ordinary receive progress should use Active");

    assert!(ordinary_sent);
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut tcp_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
    ));
    while try_recv_reliable_path_priority_command(&mut tcp_rx).is_some() {}
    assert!(try_recv_reliable_path_priority_command(&mut udp_rx).is_none());

    let mut progress = ReliableRecvProgress::default();
    let sent = sender
        .send_recv_progress(
            &mut remotes,
            &context,
            &recv_stream,
            &mut progress,
            RelayRecvProgressSend::new(None, FlowLane::Latency, true).recover_stalled_service(),
        )
        .await
        .expect("stall receive progress should use an accepted repair carrier");

    assert!(sent);
    assert!(
        try_recv_reliable_path_priority_command(&mut tcp_rx).is_none(),
        "the stalled Active path must not keep the recovery ACK when Repair is usable"
    );
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut udp_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
    ));
    assert_eq!(
        remotes.active_path_key(),
        Some(RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        }),
        "routing recovery control over Repair must not promote it to Active"
    );
}

#[tokio::test]
async fn client_stall_recv_progress_falls_back_to_active_when_repair_is_full() {
    let stream_id = StreamId(98);
    let context = client_test_context();
    let (active_commands, mut active_rx) = reliable_path_command_channels(1);
    let (repair_commands, mut repair_rx) = reliable_path_command_channels(1);
    repair_commands
        .try_enqueue_admitted_frame(
            Frame::StreamAck {
                stream_id,
                complete: false,
                ranges: Vec::new(),
            },
            FlowLane::Control,
        )
        .expect("prefill repair control queue");
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream_with_underlay(
            stream_id,
            UnderlayProtocol::Tcp,
            0,
            active_commands,
        ),
        4,
    );
    remotes.attach_for_repair(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        0,
        repair_commands,
    ));
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
            RelayRecvProgressSend::new(None, FlowLane::Latency, true).recover_stalled_service(),
        )
        .await
        .expect("a full repair queue should fall back to Active");

    assert!(sent);
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut active_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
    ));
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut repair_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
    ));
    assert!(try_recv_reliable_path_priority_command(&mut repair_rx).is_none());
}

#[tokio::test]
async fn client_stall_recv_progress_never_uses_validation_path() {
    let stream_id = StreamId(99);
    let context = client_test_context();
    let (active_commands, mut active_rx) = reliable_path_command_channels(1);
    active_commands
        .try_enqueue_admitted_frame(
            Frame::StreamAck {
                stream_id,
                complete: false,
                ranges: Vec::new(),
            },
            FlowLane::Control,
        )
        .expect("prefill active control queue");
    let (validation_commands, mut validation_rx) = reliable_path_command_channels(2);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream_with_underlay(
            stream_id,
            UnderlayProtocol::Tcp,
            0,
            active_commands,
        ),
        4,
    );
    remotes.attach_for_validation(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        0,
        validation_commands,
    ));
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut validation_rx),
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
    ));
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
            RelayRecvProgressSend::new(None, FlowLane::Latency, true).recover_stalled_service(),
        )
        .await
        .expect("blocked recovery feedback remains retryable");

    assert!(!sent);
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut active_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
    ));
    assert!(
        try_recv_reliable_path_priority_command(&mut validation_rx).is_none(),
        "Validation must remain product-ineligible during ACK recovery"
    );
}

#[tokio::test]
async fn client_path_failure_unpublishes_contention_before_cleanup_waits() {
    let stream_id = StreamId(124);
    let context = Arc::new(client_test_context());
    let registration = context.reliable_tcp_request_bulk_flow_registration();
    registration.update(true, Some(UnderlayProtocol::Tcp));
    assert_eq!(context.active_tcp_service_request_bulk_flows(), 1);

    let (commands, mut receivers) = reliable_path_command_channels(1);
    commands
        .send_control(ReliablePathCommand::CloseStream(StreamId(999)))
        .await
        .expect("prefill cleanup control queue");
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, commands), 1);
    let service = remotes.active_path_instance().expect("active Service");
    let mut sender = RequestSenderService::new(stream_id);
    sender.bind_request_bulk_flow_registration(registration.clone());

    let task_context = context.clone();
    let failure = tokio::spawn(async move {
        let removed = sender
            .fail_client_path_instance(&task_context, &mut remotes, service)
            .await;
        (removed, sender, remotes)
    });
    tokio::task::yield_now().await;

    assert_eq!(
        context.active_tcp_service_request_bulk_flows(),
        0,
        "a removed Service must stop authorizing concurrent exploration before cleanup can await"
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
    remotes.attach_for_validation(opened_test_relay_stream(stream_id, 1, candidate_commands));
    let candidate = remotes
        .path_instance_for_key(RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 1,
        })
        .expect("Validation candidate");
    let lease = context
        .try_reserve_relay_path_load_if_unchanged(candidate.key, FlowLane::Throughput, 0, 0)
        .expect("reserve optional path load");
    remotes.commit_path_instance_load_claim(candidate, lease);
    let registration = context.reliable_tcp_request_bulk_flow_registration();
    registration.update(true, Some(UnderlayProtocol::Tcp));
    let mut sender = RequestSenderService::new(stream_id);
    sender.bind_request_bulk_flow_registration(registration);

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
    assert_eq!(
        context.active_tcp_service_request_bulk_flows(),
        1,
        "optional cleanup must not unpublish the still-live TCP Service"
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
fn client_repair_extra_budget_is_cumulative_not_per_event() {
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
    let repair_payload = Bytes::from(vec![0x33; startup_floor]);

    assert!(sender.enqueue_repair_frame_with_priority(
        &mut sender_queue,
        Frame::StreamData {
            stream_id,
            offset: 0,
            payload: repair_payload.clone(),
        },
        RelaySendCause::AckGapRepair,
        mux_limits,
        false,
    ));
    assert!(!sender.enqueue_repair_frame_with_priority(
        &mut sender_queue,
        Frame::StreamData {
            stream_id,
            offset: startup_floor as u64,
            payload: repair_payload.clone(),
        },
        RelaySendCause::AckGapRepair,
        mux_limits,
        false,
    ));

    sender.record_owner_progress_for_test(startup_floor.saturating_mul(100));
    assert!(sender.enqueue_repair_frame_with_priority(
        &mut sender_queue,
        Frame::StreamData {
            stream_id,
            offset: (startup_floor * 2) as u64,
            payload: repair_payload,
        },
        RelaySendCause::PathFailureRepair,
        mux_limits,
        false,
    ));
}

#[test]
fn client_critical_repair_closes_tail_after_optional_budget_exhaustion() {
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
    assert!(sender.enqueue_repair_frame_with_priority(
        &mut sender_queue,
        frame,
        RelaySendCause::AckGapRepair,
        mux_limits,
        false,
    ));

    let closure_frame = Frame::StreamData {
        stream_id,
        offset: startup_floor as u64,
        payload: Bytes::from_static(b"tail"),
    };
    assert!(!sender.enqueue_repair_frame_with_priority(
        &mut sender_queue,
        closure_frame.clone(),
        RelaySendCause::AckGapRepair,
        mux_limits,
        false,
    ));

    sender.enqueue_critical_repair_frame(
        &mut sender_queue,
        closure_frame,
        RelaySendCause::AckGapRepair,
    );
    assert_eq!(sender.extra_traffic_budget_remaining(mux_limits), 0);
}
