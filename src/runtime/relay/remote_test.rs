use super::{
    AcceptedRemoteStreamGuard, OpenedRemoteStream, ReliableRelayAttachOutcome,
    ReliableRelayRemoteSet,
};
use crate::config::{ResourceLimits, SecurityConfig, SharedSecret};
use crate::model::capacity::reliable_relay_buffer_len;
use crate::mux::MuxLimits;
use crate::protocol::{Frame, PathId, StreamId, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::ClientPathContext;
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandReceivers, recv_reliable_path_command,
    reliable_path_command_channels, try_recv_reliable_path_command,
};
use crate::runtime::stream::{ReliablePathStream, ReliablePathStreamOutput};
use crate::scheduler::FlowLane;
use crate::transport::PathSpec;
use std::time::Duration;
use tokio::sync::mpsc;

fn security() -> SecurityConfig {
    SecurityConfig::encrypted(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    )
}

fn opened_relay_stream_for_test(
    stream_id: StreamId,
    underlay: UnderlayProtocol,
    path_index: usize,
) -> (
    OpenedRemoteStream,
    ReliablePathCommandReceivers,
    mpsc::Sender<Result<Frame, RuntimeError>>,
) {
    let mux_limits = MuxLimits::default();
    let (commands, command_rx) = reliable_path_command_channels(4);
    let (frames_tx, frames_rx) = mpsc::channel(4);
    (
        OpenedRemoteStream {
            path_index,
            stream: ReliablePathStream {
                stream_id,
                max_offset: mux_limits.max_stream_window_bytes,
                lane: FlowLane::Throughput,
                underlay,
                max_frame_payload_bytes: reliable_relay_buffer_len(mux_limits),
                output: ReliablePathStreamOutput::fixed(
                    underlay,
                    PathId(path_index as u16),
                    commands,
                    mux_limits,
                ),
                frames: frames_rx,
            },
        },
        command_rx,
        frames_tx,
    )
}

#[test]
fn dropped_accepted_stream_guard_queues_detach_and_local_close() {
    let stream_id = StreamId(92);
    let (opened, mut receivers, _frames) =
        opened_relay_stream_for_test(stream_id, UnderlayProtocol::Udp, 0);
    drop(AcceptedRemoteStreamGuard::new(opened.stream));

    assert!(matches!(
        try_recv_reliable_path_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamDetach { stream_id: id })) if id == stream_id
    ));
    assert!(matches!(
        try_recv_reliable_path_command(&mut receivers),
        Some(ReliablePathCommand::CloseStream(id)) if id == stream_id
    ));
}

#[tokio::test]
async fn rejected_opened_stream_detaches_peer_before_local_close() {
    let stream_id = StreamId(93);
    let (opened, mut receivers, _frames) =
        opened_relay_stream_for_test(stream_id, UnderlayProtocol::Udp, 0);
    opened.close().await;

    assert!(matches!(
        recv_reliable_path_command(&mut receivers).await,
        Some(ReliablePathCommand::SendFrame(Frame::StreamDetach { stream_id: id })) if id == stream_id
    ));
    assert!(matches!(
        recv_reliable_path_command(&mut receivers).await,
        Some(ReliablePathCommand::CloseStream(id)) if id == stream_id
    ));
}

#[tokio::test]
async fn duplicate_remote_set_attach_releases_the_uncommitted_stream() {
    let stream_id = StreamId(94);
    let (first, _first_receivers, _first_frames) =
        opened_relay_stream_for_test(stream_id, UnderlayProtocol::Udp, 0);
    let mut remotes = ReliableRelayRemoteSet::new(first, 4);
    let (duplicate, mut duplicate_receivers, _duplicate_frames) =
        opened_relay_stream_for_test(stream_id, UnderlayProtocol::Udp, 0);

    assert_eq!(
        remotes.attach_for_validation(duplicate),
        ReliableRelayAttachOutcome::RejectedDuplicate,
    );

    assert_eq!(remotes.path_keys().len(), 1);
    assert!(matches!(
        tokio::time::timeout(
            Duration::from_secs(1),
            recv_reliable_path_command(&mut duplicate_receivers),
        )
        .await
        .expect("duplicate detach deadline"),
        Some(ReliablePathCommand::SendFrame(Frame::StreamDetach { stream_id: id })) if id == stream_id
    ));
    assert!(matches!(
        tokio::time::timeout(
            Duration::from_secs(1),
            recv_reliable_path_command(&mut duplicate_receivers),
        )
        .await
        .expect("duplicate close deadline"),
        Some(ReliablePathCommand::CloseStream(id)) if id == stream_id
    ));
}

#[tokio::test]
async fn remote_set_close_depublishes_load_before_carrier_cleanup_waits() {
    let stream_id = StreamId(95);
    let path = "tcp://127.0.0.1:11095"
        .parse::<PathSpec>()
        .expect("tcp path");
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("context");
    context.reserve_tcp_path_load(0, FlowLane::Throughput);

    let mux_limits = MuxLimits::default();
    let (commands, receivers) = reliable_path_command_channels(1);
    commands
        .send_control(ReliablePathCommand::CloseStream(StreamId(999)))
        .await
        .expect("prefill carrier control queue");
    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let opened = OpenedRemoteStream {
        path_index: 0,
        stream: ReliablePathStream {
            stream_id,
            max_offset: mux_limits.max_stream_window_bytes,
            lane: FlowLane::Throughput,
            underlay: UnderlayProtocol::Tcp,
            max_frame_payload_bytes: reliable_relay_buffer_len(mux_limits),
            output: ReliablePathStreamOutput::fixed(
                UnderlayProtocol::Tcp,
                PathId(0),
                commands,
                mux_limits,
            ),
            frames: frames_rx,
        },
    };
    let mut remotes = ReliableRelayRemoteSet::new(opened, 1);

    let mut close = Box::pin(remotes.close_all(&context));
    assert!(matches!(
        futures::poll!(&mut close),
        std::task::Poll::Pending
    ));
    assert_eq!(
        context
            .health()
            .lock()
            .expect("client path health lock")
            .tcp[0]
            .active_flows,
        0,
        "carrier cleanup may wait, but scheduling ownership has already ended",
    );

    drop(receivers);
    close.await;
}
