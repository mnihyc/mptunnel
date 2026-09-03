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
async fn attachment_identity_exhaustion_fails_before_request_membership_publication() {
    let stream_id = StreamId(800);
    let (opened, _frames, _receivers) = opened_stream_at(stream_id, 0);
    let mut remotes = ReliableRelayRemoteSet::new(opened, 4);
    assert_eq!(remotes.paths[0].attachment_id, 0);
    remotes.next_instance_id = Some(u64::MAX);

    let (last, _last_frames, _last_receivers) = opened_stream_at(stream_id, 1);
    assert_eq!(
        remotes
            .try_attach_candidate(last)
            .expect("MAX remains one valid exact identity"),
        ReliableRelayAttachOutcome::Attached,
    );
    assert_eq!(remotes.paths[1].attachment_id, u64::MAX);
    assert_eq!(remotes.next_instance_id, None);

    let paths_before = remotes.path_instances();
    let generation_before = remotes.membership_generation();
    let (rejected, _rejected_frames, mut rejected_receivers) = opened_stream_at(stream_id, 2);
    assert!(matches!(
        remotes.try_attach_candidate(rejected),
        Err(RuntimeError::ExactIdentityExhausted),
    ));
    assert_eq!(remotes.path_instances(), paths_before);
    assert_eq!(remotes.membership_generation(), generation_before);
    assert!(matches!(
        recv_reliable_path_command(&mut rejected_receivers)
            .await
            .expect("uncommitted attachment retirement detach"),
        ReliablePathCommand::SendFrame(Frame::StreamDetach { stream_id: retired })
            if retired == stream_id
    ));
    assert!(matches!(
        recv_reliable_path_command(&mut rejected_receivers)
            .await
            .expect("uncommitted attachment retirement close"),
        ReliablePathCommand::CloseStream(retired) if retired == stream_id
    ));

    let (repeated, _repeated_frames, _repeated_receivers) = opened_stream_at(stream_id, 3);
    assert!(matches!(
        remotes.try_attach_candidate(repeated),
        Err(RuntimeError::ExactIdentityExhausted),
    ));
    assert_eq!(remotes.path_instances(), paths_before);
    assert_eq!(remotes.membership_generation(), generation_before);
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
async fn failed_attachment_retirement_cannot_block_healthy_sibling_scheduling() {
    let stream_id = StreamId(806);
    let (opened, _frames_tx, mut receivers) = opened_stream(stream_id);
    let mut remotes = ReliableRelayRemoteSet::new(opened, 4);
    let failed = remotes.paths[0].instance();

    // Consume the attachment proof, then fill the ordinary bounded control
    // queue. A failed carrier's detach/close must transfer to the independent
    // retirement lane instead of parking the Product actor behind this queue.
    assert!(matches!(
        recv_reliable_path_command(&mut receivers).await,
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
    ));
    for _ in 0..4 {
        remotes.paths[0].stream.send_detach().await;
    }

    assert!(remotes.retire_path_instance(failed));
    assert!(remotes.paths.is_empty());
    assert!(matches!(
        recv_reliable_path_command(&mut receivers).await,
        Some(ReliablePathCommand::SendFrame(Frame::StreamDetach {
            stream_id: retired,
        })) if retired == stream_id
    ));
    assert!(matches!(
        recv_reliable_path_command(&mut receivers).await,
        Some(ReliablePathCommand::CloseStream(retired)) if retired == stream_id
    ));
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
        remotes.paths[1]
            .stream
            .try_enqueue_request_control_frame(Frame::Ping { nonce: nonce + 10 })
            .expect("fill sibling control queue");
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
async fn response_requalification_ack_can_use_authenticated_sibling_return_carrier() {
    let stream_id = StreamId(807);
    let (opened, _frames_tx, mut preferred_receivers) = opened_stream(stream_id);
    let mut remotes = ReliableRelayRemoteSet::new(opened, 4);
    let observed_forward_attachment = remotes.paths[0].instance();
    let (sibling_opened, _sibling_frames, mut sibling_receivers) = opened_stream_at(stream_id, 1);
    assert_eq!(
        remotes.attach_candidate(sibling_opened),
        ReliableRelayAttachOutcome::Attached
    );

    // Consume each attachment proof, then block only the attachment that
    // carried the forward probe. The sibling remains an authenticated return
    // carrier in the same logical session.
    assert!(matches!(
        try_recv_reliable_path_command(&mut preferred_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
    ));
    assert!(matches!(
        try_recv_reliable_path_command(&mut sibling_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
    ));
    for nonce in 0..4 {
        remotes.paths[0]
            .stream
            .try_enqueue_request_control_frame(Frame::Ping { nonce })
            .expect("fill preferred ACK return queue");
    }

    let ack = Frame::StreamRequalifyAck {
        stream_id,
        probe_id: 11,
        offset: 4096,
        payload_bytes: 1024,
    };
    assert!(
        remotes
            .publish_requalification_ack(observed_forward_attachment, ack.clone())
            .expect("publish exact ACK through the authenticated remote set"),
        "a blocked forward attachment must not prevent ACK return on an authenticated sibling"
    );
    assert!(!remotes.has_pending_requalification_ack());
    assert!(matches!(
        try_recv_reliable_path_command(&mut sibling_receivers),
        Some(ReliablePathCommand::SendFrame(frame)) if frame == ack
    ));
}

#[tokio::test]
async fn response_requalification_ack_replicates_once_to_each_queue_admitting_attachment() {
    let stream_id = StreamId(808);
    let (opened, _frames_tx, mut carrying_receivers) = opened_stream(stream_id);
    let mut remotes = ReliableRelayRemoteSet::new(opened, 4);
    let carrying = remotes.paths[0].instance();
    let (sibling_opened, _sibling_frames, mut sibling_receivers) = opened_stream_at(stream_id, 1);
    assert_eq!(
        remotes.attach_candidate(sibling_opened),
        ReliableRelayAttachOutcome::Attached
    );

    // Remove attachment-establishment work. Both exact return writers are now
    // queue-admitting, but either native writer may subsequently stall.
    assert!(matches!(
        try_recv_reliable_path_command(&mut carrying_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
    ));
    assert!(matches!(
        try_recv_reliable_path_command(&mut sibling_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
    ));

    let ack = Frame::StreamRequalifyAck {
        stream_id,
        probe_id: 13,
        offset: 16_384,
        payload_bytes: 2048,
    };
    assert!(
        remotes
            .publish_requalification_ack(carrying, ack.clone())
            .expect("publish the exact ACK on the bounded return set")
    );
    assert!(
        !remotes.has_pending_requalification_ack(),
        "a pass that publishes to every queue-admitting snapshot attachment retains no pending copy"
    );
    assert!(matches!(
        try_recv_reliable_path_command(&mut carrying_receivers),
        Some(ReliablePathCommand::SendFrame(frame)) if frame == ack
    ));
    assert!(matches!(
        try_recv_reliable_path_command(&mut sibling_receivers),
        Some(ReliablePathCommand::SendFrame(frame)) if frame == ack
    ));
    assert!(
        !remotes
            .retry_pending_requalification_ack()
            .expect("completed bounded pass has no retry work")
    );
    assert!(try_recv_reliable_path_command(&mut carrying_receivers).is_none());
    assert!(try_recv_reliable_path_command(&mut sibling_receivers).is_none());
}

#[tokio::test]
async fn response_requalification_ack_does_not_complete_on_known_terminal_return_writer() {
    let stream_id = StreamId(812);
    let limits = MuxLimits::default();
    let opened =
        |path_index: usize, commands: crate::runtime::path::commands::ReliablePathCommandSender| {
            let (frames_tx, frames_rx) = mpsc::channel(4);
            (
                OpenedRemoteStream::pending(
                    ReliablePathStream {
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
                    },
                    path_index,
                ),
                frames_tx,
            )
        };
    let (terminal_commands, mut terminal_receivers) = reliable_path_command_channels(4);
    let (terminal_opened, _terminal_frames) = opened(0, terminal_commands.clone());
    let mut remotes = ReliableRelayRemoteSet::new(terminal_opened, 4);
    let (healthy_commands, mut healthy_receivers) = reliable_path_command_channels(1);
    let (healthy_opened, _healthy_frames) = opened(1, healthy_commands.clone());
    assert_eq!(
        remotes.attach_candidate(healthy_opened),
        ReliableRelayAttachOutcome::Attached
    );
    while try_recv_reliable_path_command(&mut terminal_receivers).is_some() {}
    while try_recv_reliable_path_command(&mut healthy_receivers).is_some() {}

    terminal_commands.terminate_failed_path();
    healthy_commands
        .try_enqueue_admitted_frame(Frame::Ping { nonce: 812 }, TrafficClass::Control)
        .expect("fill the only live reverse-control queue");
    let healthy = remotes.paths[1].instance();
    let ack = Frame::StreamRequalifyAck {
        stream_id,
        probe_id: 21,
        offset: 32_768,
        payload_bytes: 1024,
    };
    assert!(
        !remotes
            .publish_requalification_ack(healthy, ack)
            .expect("a known-terminal writer is not receipt publication authority"),
        "only the full live writer remains eligible, so the receipt must stay pending",
    );
    assert!(remotes.has_pending_requalification_ack());
    assert!(
        try_recv_reliable_path_command(&mut terminal_receivers).is_none(),
        "terminal failure cannot accept a receipt that is then cleared globally",
    );
}

#[tokio::test]
async fn response_requalification_ack_remains_admissible_during_planned_drain() {
    let stream_id = StreamId(813);
    let limits = MuxLimits::default();
    let (commands, mut receivers) = reliable_path_command_channels(4);
    let (frames_tx, frames_rx) = mpsc::channel(4);
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
                commands.clone(),
                limits,
            ),
            frames: frames_rx.into(),
        },
        0,
    );
    let mut remotes = ReliableRelayRemoteSet::new(opened, 4);
    while try_recv_reliable_path_command(&mut receivers).is_some() {}
    commands.begin_path_drain();

    let ack = Frame::StreamRequalifyAck {
        stream_id,
        probe_id: 22,
        offset: 36_864,
        payload_bytes: 1024,
    };
    assert!(
        remotes
            .publish_requalification_ack(remotes.paths[0].instance(), ack.clone())
            .expect("planned drain retains Product-neutral settlement control")
    );
    assert!(matches!(
        try_recv_reliable_path_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(frame)) if frame == ack
    ));
    drop(frames_tx);
}

#[tokio::test]
async fn completed_response_requalification_ack_rejects_replay_without_new_fanout() {
    let stream_id = StreamId(809);
    let (opened, _frames_tx, mut receivers) = opened_stream(stream_id);
    let mut remotes = ReliableRelayRemoteSet::new(opened, 4);
    let carrying = remotes.paths[0].instance();
    assert!(matches!(
        try_recv_reliable_path_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
    ));

    let ack = Frame::StreamRequalifyAck {
        stream_id,
        probe_id: 13,
        offset: 16_384,
        payload_bytes: 2048,
    };
    assert!(
        remotes
            .publish_requalification_ack(carrying, ack.clone())
            .expect("publish first exact receipt")
    );
    assert!(matches!(
        try_recv_reliable_path_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(frame)) if frame == ack
    ));

    assert!(
        !remotes
            .publish_requalification_ack(carrying, ack.clone())
            .expect("an already-published exact replay is a bounded no-op")
    );
    let older = Frame::StreamRequalifyAck {
        stream_id,
        probe_id: 12,
        offset: 8192,
        payload_bytes: 1024,
    };
    assert!(
        !remotes
            .publish_requalification_ack(carrying, older)
            .expect("an older replay is a bounded no-op")
    );
    let mismatched = Frame::StreamRequalifyAck {
        stream_id,
        probe_id: 13,
        offset: 16_385,
        payload_bytes: 2048,
    };
    assert!(matches!(
        remotes.publish_requalification_ack(carrying, mismatched),
        Err(RuntimeError::Protocol(_))
    ));
    assert!(try_recv_reliable_path_command(&mut receivers).is_none());
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
