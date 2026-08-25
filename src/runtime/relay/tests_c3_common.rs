use super::*;
use crate::model::capacity::reliable_relay_buffer_len;
use crate::mux::MuxLimits;
use crate::protocol::{PathId, StreamId, UnderlayProtocol};
use crate::runtime::path::commands::reliable_path_command_channels;
use crate::runtime::stream::{ReliablePathStream, ReliablePathStreamOutput};
use bytes::Bytes;
use std::time::Duration;
use tokio::sync::mpsc;

fn pre_model_opened_remote_stream(
    stream_id: StreamId,
    commands: crate::runtime::path::commands::ReliablePathCommandSender,
    frames: mpsc::Receiver<Result<Frame, RuntimeError>>,
) -> OpenedRemoteStream {
    let limits = MuxLimits::default();
    OpenedRemoteStream::pending(
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
            frames: frames.into(),
        },
        0,
    )
}

/// This is the pre-model relay loop's terminal-inference expression, kept in
/// the red control so the failure is deterministic without running a complete
/// application relay and its unrelated recovery timers.
fn pre_model_out_of_band_terminal(
    remotes: &ReliableRelayRemoteSet,
) -> Option<ReliableRelayRemoteFrame> {
    remotes
        .paths
        .iter()
        .find(|path| path.stream.output_is_terminally_closed())
        .map(|path| ReliableRelayRemoteFrame {
            instance: path.instance(),
            frame: Err(RuntimeError::ReliablePathSessionClosed),
        })
}

async fn pre_model_wait_for_buffered_remote_frame(remotes: &ReliableRelayRemoteSet) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while !remotes.has_buffered_frame() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("RED setup failed: first carrier frame never reached the merged queue");
}

#[tokio::test]
async fn pre_model_red_planned_drain_is_not_terminal_before_ordered_path_close() {
    let stream_id = StreamId(909);
    let (commands, mut command_receivers) = reliable_path_command_channels(8);
    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let remotes = ReliableRelayRemoteSet::new(
        pre_model_opened_remote_stream(stream_id, commands.clone(), frames_rx),
        8,
    );

    commands.begin_path_drain();
    command_receivers.close_for_path_drain();
    assert!(
        pre_model_out_of_band_terminal(&remotes).is_none(),
        "RED: close_for_path_drain only freezes admission while the carrier still owes ordered PATH_CLOSE, but the old relay already manufactures ReliablePathSessionClosed"
    );
}

#[tokio::test]
async fn pre_model_red_terminal_cannot_bypass_preaccepted_cap_one_frame() {
    let stream_id = StreamId(910);
    let (commands, mut command_receivers) = reliable_path_command_channels(8);
    let (frames_tx, frames_rx) = mpsc::channel(1);
    let mut remotes = ReliableRelayRemoteSet::new(
        pre_model_opened_remote_stream(stream_id, commands.clone(), frames_rx),
        1,
    );

    frames_tx
        .send(Ok(Frame::StreamData {
            stream_id,
            offset: 0,
            payload: Bytes::from_static(b"A"),
        }))
        .await
        .expect("RED setup failed: queue carrier frame A");
    pre_model_wait_for_buffered_remote_frame(&remotes).await;
    frames_tx
        .send(Ok(Frame::StreamData {
            stream_id,
            offset: 1,
            payload: Bytes::from_static(b"B"),
        }))
        .await
        .expect("RED setup failed: queue carrier frame B");
    let held_input_permit = tokio::time::timeout(Duration::from_secs(1), frames_tx.reserve())
        .await
        .expect("RED setup failed: attachment forwarder never accepted frame B")
        .expect("RED setup failed: carrier input closed before frame B was accepted");

    commands.begin_path_drain();
    command_receivers.close_for_path_drain();
    let first = remotes
        .try_recv_frame()
        .expect("RED setup failed: merged frame A disappeared");
    assert!(matches!(
        first.frame,
        Ok(Frame::StreamData {
            offset: 0,
            payload,
            ..
        }) if payload == Bytes::from_static(b"A")
    ));
    assert!(
        pre_model_out_of_band_terminal(&remotes).is_none(),
        "RED: old out-of-band terminal inference overtakes preaccepted frame B while B is blocked behind merged queue capacity one"
    );

    drop(held_input_permit);
    let second = tokio::time::timeout(Duration::from_secs(1), remotes.recv_frame())
        .await
        .expect("RED: frame B remained blocked after merged capacity was released")
        .expect("RED: merged queue closed before preaccepted frame B");
    assert!(matches!(
        second.frame,
        Ok(Frame::StreamData {
            offset: 1,
            payload,
            ..
        }) if payload == Bytes::from_static(b"B")
    ));
}

#[tokio::test]
async fn pre_model_control_unexpected_owner_drop_still_requires_terminal_liveness() {
    let stream_id = StreamId(911);
    let (commands, command_receivers) = reliable_path_command_channels(8);
    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let remotes = ReliableRelayRemoteSet::new(
        pre_model_opened_remote_stream(stream_id, commands, frames_rx),
        8,
    );
    drop(command_receivers);
    let terminal = pre_model_out_of_band_terminal(&remotes)
        .expect("unexpected command-owner loss must remain terminal without input closure");
    assert!(matches!(
        terminal.frame,
        Err(RuntimeError::ReliablePathSessionClosed)
    ));
}
