use super::*;
use crate::model::capacity::reliable_relay_buffer_len;
use crate::protocol::PathId;
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandReceivers, recv_reliable_path_command,
    reliable_path_command_channels, try_recv_reliable_path_command,
};
use crate::runtime::stream::{ReliablePathStream, ReliablePathStreamOutput};
use bytes::Bytes;
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

#[tokio::test]
async fn pending_exact_requalification_ack_does_not_block_healthy_path_progress() {
    let stream_id = StreamId(804);
    let (opened, _frames_tx, mut receivers) = opened_stream(stream_id);
    let mut remotes = ReliableRelayRemoteSet::new(opened, 4);
    let stale = remotes.paths[0].instance();
    let (healthy_opened, healthy_frames, mut healthy_receivers) = opened_stream_at(stream_id, 1);
    assert_eq!(
        remotes.attach_candidate(healthy_opened),
        ReliableRelayAttachOutcome::Attached
    );
    let healthy = remotes.paths[1].instance();

    // Attachment publication queues its path proof first.
    assert!(matches!(
        recv_reliable_path_command(&mut receivers).await,
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
    ));
    assert!(matches!(
        recv_reliable_path_command(&mut healthy_receivers).await,
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
    ));
    for nonce in 0..4 {
        remotes.paths[0]
            .stream
            .try_enqueue_request_control_frame(Frame::Ping { nonce })
            .expect("fill exact control queue");
    }

    let ack = Frame::StreamRequalifyAck {
        stream_id,
        probe_id: 9,
        offset: 4096,
        payload_bytes: 256,
    };
    assert!(
        !remotes
            .publish_requalification_ack(stale, ack.clone())
            .expect("retain ACK on live stale attachment")
    );
    assert!(remotes.has_pending_requalification_ack());

    let healthy_data = Frame::StreamData {
        stream_id,
        offset: 0,
        payload: Bytes::from_static(b"healthy"),
    };
    healthy_frames
        .send(Ok(healthy_data.clone()))
        .await
        .expect("publish healthy sibling Product frame");
    let received = tokio::time::timeout(Duration::from_secs(1), remotes.recv_frame())
        .await
        .expect("pending stale ACK must not block healthy carrier input")
        .expect("healthy carrier input");
    assert_eq!(received.instance, healthy);
    assert!(matches!(received.frame, Ok(frame) if frame == healthy_data));

    assert!(matches!(
        recv_reliable_path_command(&mut receivers).await,
        Some(ReliablePathCommand::SendFrame(Frame::Ping { nonce: 0 }))
    ));
    assert!(
        remotes
            .retry_pending_requalification_ack()
            .expect("capacity release publishes retained ACK")
    );
    assert!(!remotes.has_pending_requalification_ack());
    for nonce in 1..4 {
        assert!(matches!(
            recv_reliable_path_command(&mut receivers).await,
            Some(ReliablePathCommand::SendFrame(Frame::Ping { nonce: received }))
                if received == nonce
        ));
    }
    assert!(matches!(
        recv_reliable_path_command(&mut receivers).await,
        Some(ReliablePathCommand::SendFrame(frame)) if frame == ack
    ));
}

#[tokio::test]
async fn delayed_probe_ack_cannot_replace_a_newer_pending_exact_ack() {
    let stream_id = StreamId(805);
    let (first_opened, _first_frames, mut first_receivers) = opened_stream(stream_id);
    let mut remotes = ReliableRelayRemoteSet::new(first_opened, 4);
    let first = remotes.paths[0].instance();
    let (second_opened, _second_frames, mut second_receivers) = opened_stream_at(stream_id, 1);
    assert_eq!(
        remotes.attach_candidate(second_opened),
        ReliableRelayAttachOutcome::Attached
    );
    let second = remotes.paths[1].instance();
    assert!(matches!(
        recv_reliable_path_command(&mut first_receivers).await,
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
    ));
    assert!(matches!(
        recv_reliable_path_command(&mut second_receivers).await,
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
    ));
    for nonce in 0..4 {
        remotes.paths[0]
            .stream
            .try_enqueue_request_control_frame(Frame::Ping { nonce })
            .expect("fill first exact control queue");
        remotes.paths[1]
            .stream
            .try_enqueue_request_control_frame(Frame::Ping { nonce: nonce + 10 })
            .expect("fill second exact control queue");
    }
    let newer = Frame::StreamRequalifyAck {
        stream_id,
        probe_id: 10,
        offset: 8192,
        payload_bytes: 512,
    };
    let older = Frame::StreamRequalifyAck {
        stream_id,
        probe_id: 9,
        offset: 4096,
        payload_bytes: 256,
    };
    assert!(
        !remotes
            .publish_requalification_ack(second, newer.clone())
            .expect("retain newer exact ACK")
    );
    assert!(
        !remotes
            .publish_requalification_ack(first, older)
            .expect("ignore delayed older ACK")
    );

    assert!(matches!(
        recv_reliable_path_command(&mut second_receivers).await,
        Some(ReliablePathCommand::SendFrame(Frame::Ping { nonce: 10 }))
    ));
    assert!(
        remotes
            .retry_pending_requalification_ack()
            .expect("publish retained newer ACK")
    );
    for nonce in 11..14 {
        assert!(matches!(
            recv_reliable_path_command(&mut second_receivers).await,
            Some(ReliablePathCommand::SendFrame(Frame::Ping { nonce: received }))
                if received == nonce
        ));
    }
    assert!(matches!(
        recv_reliable_path_command(&mut second_receivers).await,
        Some(ReliablePathCommand::SendFrame(frame)) if frame == newer
    ));
    for nonce in 0..4 {
        assert!(matches!(
            recv_reliable_path_command(&mut first_receivers).await,
            Some(ReliablePathCommand::SendFrame(Frame::Ping { nonce: received }))
                if received == nonce
        ));
    }
    assert!(try_recv_reliable_path_command(&mut first_receivers).is_none());
}
