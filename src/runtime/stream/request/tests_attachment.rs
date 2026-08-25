use super::*;
use crate::model::capacity::reliable_relay_buffer_len;
use crate::protocol::PathId;
use crate::runtime::path::commands::{
    ReliablePathCommandReceivers, reliable_path_command_channels,
};
use crate::runtime::stream::{ReliablePathStream, ReliablePathStreamOutput};
use std::time::Duration;

fn opened_stream(
    stream_id: StreamId,
) -> (
    OpenedRemoteStream,
    mpsc::Sender<Result<Frame, RuntimeError>>,
    ReliablePathCommandReceivers,
) {
    opened_stream_at(stream_id, 0)
}

fn opened_stream_at(
    stream_id: StreamId,
    path_index: usize,
) -> (
    OpenedRemoteStream,
    mpsc::Sender<Result<Frame, RuntimeError>>,
    ReliablePathCommandReceivers,
) {
    let limits = MuxLimits::default();
    let (commands, receivers) = reliable_path_command_channels(4);
    let (frames_tx, frames_rx) = mpsc::channel(4);
    let stream = ReliablePathStream {
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
    };
    (
        OpenedRemoteStream::pending(stream, path_index),
        frames_tx,
        receivers,
    )
}

#[tokio::test]
async fn carrier_failure_after_fin_remains_visible_to_attachment_owner() {
    let stream_id = StreamId(801);
    let (opened, frames_tx, command_receivers) = opened_stream(stream_id);
    let mut remotes = ReliableRelayRemoteSet::new(opened, 4);
    frames_tx
        .send(Ok(Frame::StreamFin {
            stream_id,
            final_offset: 8,
        }))
        .await
        .expect("queue FIN");
    frames_tx
        .send(Err(RuntimeError::ReliablePathSessionClosed))
        .await
        .expect("queue carrier failure");

    assert!(matches!(
        remotes.recv_frame().await.expect("FIN frame").frame,
        Ok(Frame::StreamFin {
            stream_id: received_stream_id,
            final_offset: 8,
        }) if received_stream_id == stream_id
    ));
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), remotes.recv_frame())
            .await
            .expect("carrier failure deadline")
            .expect("carrier failure frame")
            .frame,
        Err(RuntimeError::ReliablePathSessionClosed)
    ));
    drop(command_receivers);
}

#[tokio::test]
async fn product_terminal_suppresses_input_close_but_not_later_carrier_terminal() {
    let stream_id = StreamId(802);
    let (opened, frames_tx, command_receivers) = opened_stream(stream_id);
    let mut remotes = ReliableRelayRemoteSet::new(opened, 4);
    frames_tx
        .send(Ok(Frame::StreamFin {
            stream_id,
            final_offset: 0,
        }))
        .await
        .expect("queue FIN");
    drop(frames_tx);

    assert!(matches!(
        remotes.recv_frame().await.expect("FIN frame").frame,
        Ok(Frame::StreamFin {
            stream_id: received_stream_id,
            final_offset: 0,
        }) if received_stream_id == stream_id
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(25), remotes.recv_frame())
            .await
            .is_err(),
        "product terminal suppresses only an unclassified input close"
    );
    drop(command_receivers);
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), remotes.recv_frame())
            .await
            .expect("later carrier terminal deadline")
            .expect("later carrier terminal frame")
            .frame,
        Err(RuntimeError::ReliablePathSessionClosed)
    ));
}

#[tokio::test]
async fn attachment_removal_cancels_product_terminal_lifecycle_watch() {
    let stream_id = StreamId(803);
    let (opened, frames_tx, command_receivers) = opened_stream(stream_id);
    let mut remotes = ReliableRelayRemoteSet::new(opened, 4);
    let instance = remotes.paths[0].instance();
    frames_tx
        .send(Ok(Frame::StreamFin {
            stream_id,
            final_offset: 0,
        }))
        .await
        .expect("queue FIN");
    drop(frames_tx);
    assert!(matches!(
        remotes.recv_frame().await.expect("FIN frame").frame,
        Ok(Frame::StreamFin { .. })
    ));

    drop(
        remotes
            .remove_path_instance(instance)
            .expect("remove exact attachment"),
    );
    drop(command_receivers);
    assert!(
        tokio::time::timeout(Duration::from_millis(25), remotes.recv_frame())
            .await
            .is_err(),
        "removed attachment cannot publish a later stale lifecycle terminal"
    );
}
