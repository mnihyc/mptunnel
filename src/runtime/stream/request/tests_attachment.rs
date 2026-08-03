use super::*;
use crate::model::capacity::reliable_relay_buffer_len;
use crate::protocol::PathId;
use crate::runtime::path::commands::reliable_path_command_channels;
use crate::runtime::stream::{ReliablePathStream, ReliablePathStreamOutput};
use std::time::Duration;

fn opened_stream(
    stream_id: StreamId,
) -> (
    OpenedRemoteStream,
    mpsc::Sender<Result<Frame, RuntimeError>>,
) {
    opened_stream_at(stream_id, 0)
}

fn opened_stream_at(
    stream_id: StreamId,
    path_index: usize,
) -> (
    OpenedRemoteStream,
    mpsc::Sender<Result<Frame, RuntimeError>>,
) {
    let limits = MuxLimits::default();
    let (commands, _receivers) = reliable_path_command_channels(4);
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
    (OpenedRemoteStream::pending(stream, path_index), frames_tx)
}

#[tokio::test]
async fn carrier_failure_after_fin_remains_visible_to_attachment_owner() {
    let stream_id = StreamId(801);
    let (opened, frames_tx) = opened_stream(stream_id);
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
}
