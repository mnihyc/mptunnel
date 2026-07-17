use super::*;
use crate::config::{ResourceLimits, SecurityConfig, SharedSecret};
use crate::model::capacity::reliable_relay_buffer_len;
use crate::mux::MuxLimits;
use crate::protocol::{OffsetRange, PathId, StreamId, TargetAddr, UnderlayProtocol};
use crate::runtime::path::commands::reliable_path_command_channels;
use crate::runtime::stream::{ReliablePathStream, ReliablePathStreamOutput};
use crate::transport::PathSpec;
use bytes::Bytes;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};
use tokio::sync::mpsc;

fn test_security() -> SecurityConfig {
    SecurityConfig::encrypted(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    )
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
            frames: frames_rx,
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
    let ack = send_stream.apply_ack(&[OffsetRange {
        start: 0,
        end: 1024 * 1024,
    }]);
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

#[test]
fn bulk_request_outstanding_limit_is_the_configured_stream_resource_ceiling() {
    let limits = MuxLimits::default();
    assert_eq!(
        reliable_relay_request_outstanding_limit_bytes(TrafficClass::Throughput, 64 * 1024, limits,),
        limits
            .max_repair_bytes
            .min(limits.max_reorder_bytes)
            .min(limits.max_stream_window_bytes as usize),
    );
}
