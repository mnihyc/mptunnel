use super::*;
use crate::config::{ClientSecurityConfig, ResourceLimits, SharedSecret};
use crate::model::capacity::reliable_relay_buffer_len;
use crate::mux::MuxLimits;
use crate::protocol::{OffsetRange, PathId, StreamId, TargetAddr, UnderlayProtocol};
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandReceivers, recv_reliable_path_command,
    reliable_path_command_channels, try_recv_reliable_path_priority_command,
};
use crate::runtime::sender::{
    ProductWorkloadIdentity, RequestProductAckOriginalResolution, RequestProductAckReceipt,
    RequestProductAckReceiptTarget,
};
use crate::runtime::stream::{ReliablePathStream, ReliablePathStreamOutput};
use crate::transport::PathSpec;
use bytes::Bytes;
use std::num::NonZeroU64;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};
use tokio::sync::mpsc;

fn test_security() -> ClientSecurityConfig {
    ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    )
}

#[tokio::test]
async fn client_ack_extent_rejection_precedes_all_transaction_mutation() {
    let stream_id = StreamId(901);
    let path = "tcp://127.0.0.1:10901"
        .parse::<PathSpec>()
        .expect("test path");
    let context = ClientPathContext::new(vec![path], test_security(), ResourceLimits::default())
        .expect("client context");
    let limits = context.mux_limits;
    let (commands, _receivers) = reliable_path_command_channels(8);
    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let opened = OpenedRemoteStream::pending(
        ReliablePathStream {
            stream_id,
            max_offset: limits.max_stream_window_bytes,
            lane: TrafficClass::Throughput,
            underlay: UnderlayProtocol::Tcp,
            max_frame_payload_bytes: reliable_relay_buffer_len(limits),
            output: ReliablePathStreamOutput::fixed(
                UnderlayProtocol::Tcp,
                PathId(0),
                commands,
                limits,
            ),
            frames: frames_rx.into(),
        },
        0,
    );
    let remotes = ReliableRelayRemoteSet::new(opened, 8);
    let mut send_stream = ReliableSendStream::new(stream_id, limits);
    let sent = send_stream
        .send_data(Bytes::from_static(b"abcdefgh"))
        .expect("send request data");
    let mut sender = RequestSenderService::new(stream_id);
    sender.record_original_frame_for_test(remotes.paths[0].instance(), &sent);
    let mut sender_queue = ReliableRelaySenderQueue::default();
    sender_queue.push_reinjection(sent.clone());
    let mut state = ClientRelayState::new();

    let stream_before = send_stream.clone();
    let queue_bytes_before = sender_queue.bytes();
    let last_stream_at_before = state.progress.last_stream_at;
    let frontier_before = state.progress.last_send_ack_frontier;
    let snapshot_before = state.progress.last_send_ack.clone();
    let mut rejected_receipts = Vec::<RequestProductAckReceipt>::new();
    let rejected = {
        let mut sink = |receipt| rejected_receipts.push(receipt);
        apply_client_stream_ack(
            ClientStreamAckContext {
                state: &mut state,
                sender: &mut sender,
                sender_queue: &mut sender_queue,
                context: &context,
                remotes: &remotes,
                send_stream: &mut send_stream,
                path_snapshot: None,
                relay_lane: TrafficClass::Throughput,
                product_ack_receipt: Some(RequestProductAckReceiptTarget {
                    identity: ProductWorkloadIdentity {
                        stream_id,
                        lifecycle_generation: NonZeroU64::new(11).expect("nonzero generation"),
                    },
                    sink: &mut sink,
                }),
            },
            stream_id,
            true,
            vec![OffsetRange { start: 4, end: 9 }],
        )
    };

    assert!(matches!(
        rejected,
        Err(crate::mux::stream::StreamError::AckRangeBeyondAssigned {
            start: 4,
            end: 9,
            assigned_end: 8,
        })
    ));
    assert_eq!(
        send_stream, stream_before,
        "send cache mutated on rejection"
    );
    assert_eq!(sender_queue.bytes(), queue_bytes_before);
    assert!(!sender_queue.is_empty(), "queued repair was released");
    assert_eq!(state.progress.last_stream_at, last_stream_at_before);
    assert_eq!(state.progress.last_send_ack_frontier, frontier_before);
    assert_eq!(state.progress.last_send_ack, snapshot_before);
    assert!(
        rejected_receipts.is_empty(),
        "a rejected transaction has no completed receipt"
    );

    let released = apply_client_stream_ack(
        ClientStreamAckContext {
            state: &mut state,
            sender: &mut sender,
            sender_queue: &mut sender_queue,
            context: &context,
            remotes: &remotes,
            send_stream: &mut send_stream,
            path_snapshot: None,
            relay_lane: TrafficClass::Throughput,
            product_ack_receipt: None,
        },
        stream_id,
        true,
        vec![OffsetRange { start: 0, end: 8 }],
    )
    .expect("exact assigned ACK commits");
    assert_eq!(released, 8);
    assert!(sender_queue.is_empty());
    assert_eq!(state.progress.last_send_ack.horizon(), Some(8));
}

#[tokio::test]
async fn client_product_ack_receipt_is_one_complete_transaction() {
    let stream_id = StreamId(902);
    let path = "tcp://127.0.0.1:10902"
        .parse::<PathSpec>()
        .expect("test path");
    let context = ClientPathContext::new(vec![path], test_security(), ResourceLimits::default())
        .expect("client context");
    let limits = context.mux_limits;
    let (commands, _receivers) = reliable_path_command_channels(8);
    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let opened = OpenedRemoteStream::pending(
        ReliablePathStream {
            stream_id,
            max_offset: limits.max_stream_window_bytes,
            lane: TrafficClass::Throughput,
            underlay: UnderlayProtocol::Tcp,
            max_frame_payload_bytes: reliable_relay_buffer_len(limits),
            output: ReliablePathStreamOutput::fixed(
                UnderlayProtocol::Tcp,
                PathId(0),
                commands,
                limits,
            ),
            frames: frames_rx.into(),
        },
        0,
    );
    let remotes = ReliableRelayRemoteSet::new(opened, 8);
    let owner = remotes.paths[0].instance();
    let mut send_stream = ReliableSendStream::new(stream_id, limits);
    let original = send_stream
        .send_data(Bytes::from_static(b"abcdefgh"))
        .expect("request range");
    let repair = Frame::StreamData {
        stream_id,
        offset: 0,
        payload: Bytes::from_static(b"abcd"),
    };
    let mut sender = RequestSenderService::new(stream_id);
    sender.record_original_frame_for_test(owner, &original);
    sender.record_reinjection_frame_for_test(owner, &repair);
    let mut sender_queue = ReliableRelaySenderQueue::default();
    sender_queue.push_reinjection(repair);
    let mut state = ClientRelayState::new();
    let mut receipts = Vec::<RequestProductAckReceipt>::new();

    let released = {
        let mut sink = |receipt| receipts.push(receipt);
        apply_client_stream_ack(
            ClientStreamAckContext {
                state: &mut state,
                sender: &mut sender,
                sender_queue: &mut sender_queue,
                context: &context,
                remotes: &remotes,
                send_stream: &mut send_stream,
                path_snapshot: None,
                relay_lane: TrafficClass::Throughput,
                product_ack_receipt: Some(RequestProductAckReceiptTarget {
                    identity: ProductWorkloadIdentity {
                        stream_id,
                        lifecycle_generation: NonZeroU64::new(17).expect("nonzero generation"),
                    },
                    sink: &mut sink,
                }),
            },
            stream_id,
            true,
            vec![OffsetRange { start: 0, end: 8 }],
        )
        .expect("complete ACK transaction")
    };

    assert_eq!(released, 8);
    assert!(sender_queue.is_empty(), "ACK-owned repair was reconciled");
    assert_eq!(receipts.len(), 1);
    let receipt = &receipts[0];
    assert_eq!(receipt.identity.stream_id, stream_id);
    assert_eq!(receipt.identity.lifecycle_generation.get(), 17);
    assert_eq!(receipt.completed_at, state.progress.last_stream_at);
    assert_eq!(receipt.original_releases.len(), 2);
    assert_eq!(
        receipt.original_releases[0].range,
        OffsetRange { start: 0, end: 4 }
    );
    assert_eq!(receipt.original_releases[0].bytes, 4);
    assert_eq!(receipt.original_releases[0].instance, owner);
    assert_eq!(
        receipt.original_releases[0].resolution,
        RequestProductAckOriginalResolution::Ambiguous,
    );
    assert_eq!(
        receipt.original_releases[1].range,
        OffsetRange { start: 4, end: 8 }
    );
    assert_eq!(receipt.original_releases[1].bytes, 4);
    assert_eq!(receipt.original_releases[1].instance, owner);
    assert_eq!(
        receipt.original_releases[1].resolution,
        RequestProductAckOriginalResolution::Unambiguous,
    );

    {
        let mut sink = |receipt| receipts.push(receipt);
        let duplicate = apply_client_stream_ack(
            ClientStreamAckContext {
                state: &mut state,
                sender: &mut sender,
                sender_queue: &mut sender_queue,
                context: &context,
                remotes: &remotes,
                send_stream: &mut send_stream,
                path_snapshot: None,
                relay_lane: TrafficClass::Throughput,
                product_ack_receipt: Some(RequestProductAckReceiptTarget {
                    identity: ProductWorkloadIdentity {
                        stream_id,
                        lifecycle_generation: NonZeroU64::new(17).expect("nonzero generation"),
                    },
                    sink: &mut sink,
                }),
            },
            stream_id,
            true,
            vec![OffsetRange { start: 0, end: 8 }],
        )
        .expect("subsumed ACK remains valid");
        assert_eq!(duplicate, 0);
    }
    assert_eq!(
        receipts.len(),
        1,
        "a subsumed ACK has no new completed Product transaction"
    );
}

async fn closed_output_relay(
    stream_id: StreamId,
) -> (
    tokio::io::DuplexStream,
    tokio::task::JoinHandle<Result<PathDeliveryStats, RuntimeError>>,
    mpsc::Sender<Result<Frame, RuntimeError>>,
) {
    let unused = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("reserve unused endpoint");
    let unused_addr = unused.local_addr().expect("unused endpoint address");
    drop(unused);
    let path = format!("tcp://{unused_addr}")
        .parse::<PathSpec>()
        .expect("TCP path");
    let mut context =
        ClientPathContext::new(vec![path], test_security(), ResourceLimits::default())
            .expect("client context");
    context.session_retention_timeout = Duration::from_millis(100);

    let limits = context.mux_limits;
    let (commands, command_receivers) = reliable_path_command_channels(4);
    drop(command_receivers);
    let (frames_tx, frames_rx) = mpsc::channel(4);
    let opened = OpenedRemoteStream::pending(
        ReliablePathStream {
            stream_id,
            max_offset: limits.max_stream_window_bytes,
            lane: TrafficClass::Latency,
            underlay: UnderlayProtocol::Tcp,
            max_frame_payload_bytes: reliable_relay_buffer_len(limits),
            output: ReliablePathStreamOutput::fixed(
                UnderlayProtocol::Tcp,
                PathId(0),
                commands,
                limits,
            ),
            frames: frames_rx.into(),
        },
        0,
    );
    let (application, relay_side) = duplex(4096);
    let relay_context = context.clone();
    let relay = tokio::spawn(async move {
        relay_migrating_tcp_stream(
            relay_side,
            &relay_context,
            MppPerformanceConfig::default(),
            ReliableRelayOpenSpec {
                target: TargetAddr::Ip(unused_addr),
            },
            opened,
        )
        .await
    });
    (application, relay, frames_tx)
}

async fn blocked_feedback_relay(
    stream_id: StreamId,
) -> (
    tokio::io::DuplexStream,
    tokio::task::JoinHandle<Result<PathDeliveryStats, RuntimeError>>,
    mpsc::Sender<Result<Frame, RuntimeError>>,
    ReliablePathCommandReceivers,
) {
    let unused = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("reserve unused endpoint");
    let unused_addr = unused.local_addr().expect("unused endpoint address");
    drop(unused);
    let path = format!("tcp://{unused_addr}")
        .parse::<PathSpec>()
        .expect("TCP path");
    let context = ClientPathContext::new(vec![path], test_security(), ResourceLimits::default())
        .expect("client context");
    let limits = context.mux_limits;
    let (commands, command_receivers) = reliable_path_command_channels(1);
    commands
        .try_enqueue_admitted_frame(
            Frame::StreamAck {
                stream_id,
                complete: false,
                ranges: Vec::new(),
            },
            TrafficClass::Control,
        )
        .expect("fill carrier control queue");
    let (frames_tx, frames_rx) = mpsc::channel(4);
    let opened = OpenedRemoteStream::pending(
        ReliablePathStream {
            stream_id,
            max_offset: limits.max_stream_window_bytes,
            lane: TrafficClass::Latency,
            underlay: UnderlayProtocol::Tcp,
            max_frame_payload_bytes: reliable_relay_buffer_len(limits),
            output: ReliablePathStreamOutput::fixed(
                UnderlayProtocol::Tcp,
                PathId(0),
                commands,
                limits,
            ),
            frames: frames_rx.into(),
        },
        0,
    );
    let (application, relay_side) = duplex(4096);
    let relay_context = context.clone();
    let relay = tokio::spawn(async move {
        relay_migrating_tcp_stream(
            relay_side,
            &relay_context,
            MppPerformanceConfig::default(),
            ReliableRelayOpenSpec {
                target: TargetAddr::Ip(unused_addr),
            },
            opened,
        )
        .await
    });
    (application, relay, frames_tx, command_receivers)
}

async fn assert_absolute_retention_timeout(
    relay: &mut tokio::task::JoinHandle<Result<PathDeliveryStats, RuntimeError>>,
) {
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(
        !relay.is_finished(),
        "carrier loss bypassed the retention interval"
    );
    let result = tokio::time::timeout(Duration::from_secs(1), relay)
        .await
        .expect("retention expiry timeout")
        .expect("relay task");
    assert!(matches!(result, Err(RuntimeError::SessionRetentionTimeout)));
}

#[tokio::test]
async fn send_first_carrier_loss_enters_absolute_session_retention() {
    let (mut application, mut relay, frames_tx) = closed_output_relay(StreamId(610)).await;

    application
        .write_all(b"send-first failure")
        .await
        .expect("application write");
    assert_absolute_retention_timeout(&mut relay).await;
    drop(frames_tx);
}

#[tokio::test]
async fn fin_feedback_carrier_loss_enters_absolute_session_retention() {
    let (application, mut relay, frames_tx) = closed_output_relay(StreamId(612)).await;
    frames_tx
        .send(Ok(Frame::StreamFin {
            stream_id: StreamId(612),
            final_offset: 0,
        }))
        .await
        .expect("remote FIN");

    assert_absolute_retention_timeout(&mut relay).await;
    drop(application);
    drop(frames_tx);
}

#[tokio::test]
async fn retained_in_order_fin_commits_after_reattachment_feedback() {
    let recv_stream = ReliableRecvStream::new(StreamId(613), MuxLimits::default());
    let mut state = ClientRelayState::new();
    assert!(
        receive_stream_fin(
            &recv_stream,
            &mut state.endpoint.pending_remote_fin_offset,
            0,
        )
        .expect("in-order FIN")
    );
    assert_eq!(state.endpoint.pending_remote_fin_offset, Some(0));

    let (mut application, mut relay_side) = duplex(64);
    commit_pending_remote_fin(&mut relay_side, &mut state, &recv_stream, true)
        .await
        .expect("commit retained FIN");

    assert!(!state.endpoint.remote_open);
    assert_eq!(state.endpoint.pending_remote_fin_offset, None);
    let mut byte = [0u8; 1];
    assert_eq!(
        application.read(&mut byte).await.expect("read half-close"),
        0
    );
}

#[tokio::test]
async fn client_completion_retains_ack_until_every_live_attachment_accepts_it() {
    let stream_id = StreamId(615);
    let limits = MuxLimits::default();
    let opened = |path_index, commands| {
        let (frames_tx, frames_rx) = mpsc::channel(1);
        (
            frames_tx,
            OpenedRemoteStream::pending(
                ReliablePathStream {
                    stream_id,
                    max_offset: limits.max_stream_window_bytes,
                    lane: TrafficClass::Latency,
                    underlay: UnderlayProtocol::Tcp,
                    max_frame_payload_bytes: reliable_relay_buffer_len(limits),
                    output: ReliablePathStreamOutput::fixed(
                        UnderlayProtocol::Tcp,
                        PathId(path_index),
                        commands,
                        limits,
                    ),
                    frames: frames_rx.into(),
                },
                usize::from(path_index),
            ),
        )
    };
    let (first_commands, mut first_receivers) = reliable_path_command_channels(4);
    let (_first_frames, first) = opened(0, first_commands);
    let mut remotes = ReliableRelayRemoteSet::new(first, 4);
    while try_recv_reliable_path_priority_command(&mut first_receivers).is_some() {}

    let (blocked_commands, mut blocked_receivers) = reliable_path_command_channels(1);
    let (_blocked_frames, blocked) = opened(1, blocked_commands.clone());
    remotes.attach(blocked);
    while try_recv_reliable_path_priority_command(&mut blocked_receivers).is_some() {}
    blocked_commands
        .try_enqueue_admitted_frame(Frame::Ping { nonce: 1 }, TrafficClass::Control)
        .expect("block one exact attachment");

    let publication = remotes.publish_stream_ack(
        1,
        vec![Frame::StreamAck {
            stream_id,
            complete: true,
            ranges: Vec::new(),
        }],
    );
    assert!(publication.published);
    assert!(publication.pending);

    let mut state = ClientRelayState::new();
    state.record_local_eof();
    state.record_local_fin_sent();
    state.record_terminal_fin_replayed();
    state.record_remote_finished();
    let send_stream = ReliableSendStream::new(stream_id, limits);
    let recv_stream = ReliableRecvStream::new(stream_id, limits);
    let sender_queue = ReliableRelaySenderQueue::default();
    assert!(!client_relay_finished(
        &state,
        &send_stream,
        &recv_stream,
        &sender_queue,
        &remotes,
    ));

    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut blocked_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::Ping { nonce: 1 }))
    ));
    let publication = remotes.retry_pending_stream_ack();
    assert!(publication.published);
    assert!(!publication.pending);
    assert!(client_relay_finished(
        &state,
        &send_stream,
        &recv_stream,
        &sender_queue,
        &remotes,
    ));
}

#[tokio::test]
async fn final_feedback_backpressure_keeps_fin_pending_until_ack_is_queued() {
    let stream_id = StreamId(614);
    let (mut application, relay, frames_tx, mut command_receivers) =
        blocked_feedback_relay(stream_id).await;
    frames_tx
        .send(Ok(Frame::StreamFin {
            stream_id,
            final_offset: 0,
        }))
        .await
        .expect("remote FIN");

    let mut byte = [0_u8; 1];
    assert!(
        tokio::time::timeout(Duration::from_millis(50), application.read(&mut byte))
            .await
            .is_err(),
        "remote FIN committed before its final Data ACK entered a carrier queue"
    );

    assert!(matches!(
        recv_reliable_path_command(&mut command_receivers).await,
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
    ));
    let final_feedback = tokio::time::timeout(
        Duration::from_secs(1),
        recv_reliable_path_command(&mut command_receivers),
    )
    .await
    .expect("final feedback enqueue deadline");
    match final_feedback {
        Some(ReliablePathCommand::SendFrame(frame)) => assert!(
            matches!(
                &frame,
                Frame::StreamAck {
                    stream_id: ack_stream_id,
                    complete: true,
                    ..
                } if *ack_stream_id == stream_id
            ),
            "unexpected frame before final Data ACK: {frame:?}"
        ),
        Some(_) => panic!("unexpected non-frame command before final Data ACK"),
        None => panic!("carrier command queue closed before final Data ACK"),
    }
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), application.read(&mut byte))
            .await
            .expect("remote half-close deadline")
            .expect("application read"),
        0
    );

    relay.abort();
}

#[test]
fn request_outstanding_limit_uses_stream_resources_then_exact_ack_headroom() {
    let mux_limits = MuxLimits {
        max_stream_window_bytes: 4 * 1024 * 1024,
        max_repair_bytes: 4 * 1024 * 1024,
        max_reorder_bytes: 4 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let payload_bytes = 64 * 1024;
    let limit = reliable_relay_request_outstanding_limit_bytes(
        TrafficClass::Throughput,
        payload_bytes,
        mux_limits,
    );
    let accounting_limit = mux_limits
        .max_repair_bytes
        .min(mux_limits.max_reorder_bytes)
        .min(mux_limits.max_stream_window_bytes as usize);
    assert_eq!(limit, accounting_limit);

    let mut send_stream = ReliableSendStream::new(StreamId(90), mux_limits);
    send_stream
        .send_data(Bytes::from(vec![0x11; 512 * 1024]))
        .expect("first dispatched request chunk");
    send_stream
        .send_data(Bytes::from(vec![0x22; 512 * 1024]))
        .expect("second dispatched request chunk");
    let mut sender_queue = ReliableRelaySenderQueue::default();
    sender_queue.push_data(Bytes::from(vec![0x33; 1024 * 1024]));

    assert_eq!(
        reliable_relay_request_outstanding_headroom_bytes(
            &send_stream,
            &sender_queue,
            accounting_limit,
        ),
        2 * 1024 * 1024
    );
    sender_queue.push_data(Bytes::from(vec![0x44; 2 * 1024 * 1024]));
    assert_eq!(
        reliable_relay_request_outstanding_headroom_bytes(
            &send_stream,
            &sender_queue,
            accounting_limit,
        ),
        0,
        "raw request data and Data-ACK-retained ranges share one unique-byte budget"
    );
    let ack = send_stream
        .apply_ack(&[OffsetRange {
            start: 0,
            end: 1024 * 1024,
        }])
        .expect("ACK remains within assigned request data");
    assert_eq!(ack.released_bytes, 1024 * 1024);
    assert_eq!(
        reliable_relay_request_outstanding_headroom_bytes(
            &send_stream,
            &sender_queue,
            accounting_limit,
        ),
        1024 * 1024,
        "unique STREAM_ACK release must resume source reads without double-counting raw queue bytes"
    );
}

#[test]
fn latency_request_outstanding_limit_keeps_the_staging_reservoir() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = 64 * 1024;
    let limit = reliable_relay_request_outstanding_limit_bytes(
        TrafficClass::Latency,
        payload_bytes,
        mux_limits,
    );

    assert_eq!(limit, reliable_relay_buffer_len(mux_limits));
    assert!(limit < mux_limits.max_stream_window_bytes as usize);
}

#[tokio::test]
async fn bulk_request_staging_uses_resource_ceiling_and_bounded_ready_work() {
    let limits = MuxLimits::default();
    assert_eq!(
        reliable_relay_request_outstanding_limit_bytes(TrafficClass::Throughput, 64 * 1024, limits,),
        limits
            .max_repair_bytes
            .min(limits.max_reorder_bytes)
            .min(limits.max_stream_window_bytes as usize),
    );

    let staging_limits = MuxLimits {
        max_payload_bytes: 16,
        max_stream_window_bytes: 64,
        max_repair_bytes: 64,
        max_reorder_bytes: 64,
        max_path_flight_bytes: 64,
        max_reliable_relay_chunk_bytes: 16,
        ..MuxLimits::default()
    };
    let sender_queue_limit = reliable_relay_buffer_len(staging_limits);
    let (sender_dispatch_byte_budget, sender_dispatch_item_budget) =
        reliable_relay_sender_dispatch_budget(
            staging_limits,
            TrafficClass::Throughput,
            4,
            sender_queue_limit,
            sender_queue_limit,
        );
    assert_eq!(
        (sender_dispatch_byte_budget, sender_dispatch_item_budget),
        (16, 4),
        "the authoritative bulk sender budget permits bounded batching"
    );

    let send_stream = ReliableSendStream::new(StreamId(451), staging_limits);
    let mut sender_queue = ReliableRelaySenderQueue::default();
    sender_queue.push_data(Bytes::from_static(b"12345"));
    let bounds = ClientOpportunisticReadBounds {
        sender_dispatch_byte_budget,
        sender_dispatch_item_budget,
        sender_queue_limit,
        source_read_ceiling: reliable_relay_buffer_len(staging_limits),
        request_outstanding_limit: 64,
    };
    assert_eq!(
        reliable_relay_client_opportunistic_read_budget(1, &send_stream, &sender_queue, bounds),
        11,
        "bulk may stage more work, but only through the remaining dispatch-byte budget"
    );
    assert_eq!(
        reliable_relay_client_opportunistic_read_budget(
            sender_dispatch_item_budget,
            &send_stream,
            &sender_queue,
            bounds,
        ),
        0,
        "one pass cannot exceed its authoritative dispatch-item budget"
    );
    assert_eq!(
        reliable_relay_client_opportunistic_read_budget(
            1,
            &send_stream,
            &sender_queue,
            ClientOpportunisticReadBounds {
                request_outstanding_limit: sender_queue.data_bytes() + 3,
                ..bounds
            },
        ),
        3,
        "the exact outstanding-resource headroom caps the next source read"
    );
    assert_eq!(
        reliable_relay_client_opportunistic_read_budget(
            1,
            &send_stream,
            &sender_queue,
            ClientOpportunisticReadBounds {
                sender_queue_limit: sender_queue.bytes(),
                ..bounds
            },
        ),
        0,
        "sender queue backpressure stops opportunistic source reads"
    );

    assert_eq!(
        ready_at_entry(std::future::ready(7_u8)).await,
        Some(7),
        "work ready when the bounded drain starts is admitted"
    );
    assert_eq!(
        ready_at_entry(std::future::pending::<u8>()).await,
        None,
        "the opportunistic drain never waits for future source work"
    );

    let session_send_buffer = crate::runtime::stream::SessionSendBuffer::new(8);
    let mut updates = session_send_buffer.subscribe();
    assert_eq!(
        ready_at_entry(async {
            let _permit = session_send_buffer.reserve(&mut updates, 8).await;
            std::future::pending::<()>().await;
        })
        .await,
        None
    );
    assert_eq!(
        session_send_buffer.available_bytes(),
        8,
        "cancelling a not-ready opportunistic source read releases its reservation"
    );
}
