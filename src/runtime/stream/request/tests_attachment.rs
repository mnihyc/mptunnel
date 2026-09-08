use super::*;
use crate::model::capacity::reliable_relay_buffer_len;
use crate::protocol::{OffsetRange, PathId, SessionId};
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandReceivers, recv_reliable_path_command,
    reliable_path_command_channels, reliable_path_command_pending_bytes,
    try_recv_reliable_path_command,
};
use crate::runtime::stream::response::{ResponseStreamAttachOutcome, ResponseStreamBinding};
use crate::runtime::stream::{ReliablePathStream, ReliablePathStreamOutput};
use bytes::Bytes;
use futures::FutureExt;
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

// The receive boundary starts at actually published/decoded Product frames;
// it is not an encrypted transport or full relay-performance fixture.
struct ReadyMaxDataInput {
    remotes: ReliableRelayRemoteSet,
    input: ReliableRelayRemoteInput,
    frames: [mpsc::Sender<Result<Frame, RuntimeError>>; 2],
    _client_commands: [ReliablePathCommandReceivers; 2],
    publisher: std::sync::Arc<ResponseStreamBinding>,
    publication_commands: [ReliablePathCommandReceivers; 2],
}

impl ReadyMaxDataInput {
    fn new(stream_id: StreamId) -> Self {
        let (a, a_frames, a_commands) = opened_stream_at(stream_id, 0);
        let (mut remotes, input) = ReliableRelayRemoteSet::new(a, 4);
        let (b, b_frames, b_commands) = opened_stream_at(stream_id, 1);
        assert_eq!(
            remotes.attach_candidate(b),
            ReliableRelayAttachOutcome::Attached
        );
        let (a_output, a_publications) = reliable_path_command_channels(4);
        let (b_output, b_publications) = reliable_path_command_channels(4);
        let publisher = ResponseStreamBinding::new(
            SessionId(809),
            UnderlayProtocol::Tcp,
            PathId(0),
            a_output,
            TrafficClass::Throughput,
        );
        assert_eq!(
            publisher.attach(
                UnderlayProtocol::Tcp,
                PathId(1),
                b_output,
                TrafficClass::Throughput
            ),
            ResponseStreamAttachOutcome::Attached,
        );
        Self {
            remotes,
            input,
            frames: [a_frames, b_frames],
            _client_commands: [a_commands, b_commands],
            publisher,
            publication_commands: [a_publications, b_publications],
        }
    }

    fn publish(&mut self, max_offset: u64) -> [Frame; 2] {
        let stream_id = self.remotes.stream_id();
        let publication = self.publisher.publish_max_data(stream_id, max_offset);
        assert_eq!(publication.published_offset, Some(max_offset));
        assert!(!publication.pending);
        self.publication_commands.each_mut().map(|commands| {
            let command = try_recv_reliable_path_command(commands).expect("real MAX publication");
            let charged = reliable_path_command_pending_bytes(&command);
            let ReliablePathCommand::SendFrame(frame) = command else {
                panic!("MAX publication must use its actual control command");
            };
            commands.release_pending_command_bytes(charged);
            assert_eq!(
                frame,
                Frame::StreamMaxData {
                    stream_id,
                    max_offset
                }
            );
            frame
        })
    }

    async fn admit(&mut self, path: usize, frame: Result<Frame, RuntimeError>, ready: usize) {
        self.frames[path]
            .send(frame)
            .await
            .expect("admit actual attachment input");
        // This bounds fixture scheduling/cleanup only; it is not a Product
        // latency target, batching timer, or change to any queue capacity.
        tokio::time::timeout(Duration::from_secs(1), async {
            while self.input.ready_frame_count() < ready {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the real forwarder must fill the finite ready backlog");
        assert_eq!(self.input.ready_frame_count(), ready);
    }

    fn take_ready(&mut self, asynchronous: bool) -> ReliableRelayRemoteFrame {
        if asynchronous {
            self.input
                .recv_frame()
                .now_or_never()
                .expect("ready-only folding must never wait for another grant")
                .expect("admitted input")
        } else {
            self.input.try_recv_frame().expect("admitted ready input")
        }
    }
}

async fn ready_max_data_prefix_case(asynchronous: bool) {
    let stream_id = StreamId(809);
    let mut fixture = ReadyMaxDataInput::new(stream_id);
    assert!(fixture.input.recv_frame().now_or_never().is_none());
    assert!(fixture.input.try_recv_frame().is_none());
    let limits = MuxLimits::default();
    let initial = limits.max_stream_window_bytes;
    let greatest = initial
        .checked_add(reliable_relay_buffer_len(limits) as u64)
        .unwrap();
    let [a_initial, b_initial] = fixture.publish(initial);
    fixture.admit(1, Ok(b_initial.clone()), 1).await;
    let isolated = fixture.take_ready(asynchronous);
    assert_eq!(isolated.instance, fixture.remotes.paths[1].instance());
    assert!(matches!(isolated.frame, Ok(frame) if frame == b_initial));
    assert!(
        !fixture.input.has_buffered_frame(),
        "an isolated grant is not delayed"
    );

    let [a_latest, b_latest] = fixture.publish(greatest);
    fixture.admit(0, Ok(a_initial), 1).await;
    fixture.admit(1, Ok(b_latest.clone()), 2).await;
    fixture.admit(0, Ok(a_latest), 3).await;
    let data = Frame::StreamData {
        stream_id,
        offset: 0,
        payload: Bytes::from_static(b"ready response"),
    };
    fixture.admit(0, Ok(data.clone()), 4).await;
    let ready_before = fixture.input.ready_frame_count();
    let mut grants = Vec::new();
    let mut response = None;
    for _ in 0..ready_before {
        let incoming = fixture.take_ready(asynchronous);
        match incoming.frame {
            Ok(Frame::StreamMaxData {
                stream_id: id,
                max_offset,
            }) => {
                assert_eq!(id, stream_id);
                assert!(max_offset == initial || max_offset == greatest);
                grants.push((incoming.instance, max_offset));
            }
            Ok(frame) => {
                response = Some((incoming.instance, frame));
                break;
            }
            Err(error) => panic!("legal publication/input must not fail: {error}"),
        }
    }
    assert_eq!(response, Some((fixture.remotes.paths[0].instance(), data)));
    assert_eq!(grants.iter().map(|(_, grant)| *grant).max(), Some(greatest));
    assert_eq!(
        grants
            .iter()
            .find(|(_, grant)| *grant == greatest)
            .unwrap()
            .0,
        fixture.remotes.paths[1].instance(),
        "the first greatest grant retains its actual source; an equal sibling cannot replace it",
    );
    assert_eq!(fixture.input.ready_frame_count(), 0);
    assert!(fixture.input.try_recv_frame().is_none());
    // Proposed MAX-only receive-fold model RED: all authority, order, exact
    // source, and payload checks above pass before counting actor input turns.
    assert_eq!(
        grants.len(),
        1,
        "one finite ready MAX prefix must expose one greatest grant, not one actor preparation turn per superseded grant"
    );
    assert_eq!(grants[0], (fixture.remotes.paths[1].instance(), greatest));
}

#[tokio::test]
async fn ready_max_data_prefix_folds_before_response_async() {
    ready_max_data_prefix_case(true).await;
}

#[tokio::test]
async fn ready_max_data_prefix_folds_before_response_try() {
    ready_max_data_prefix_case(false).await;
}

#[tokio::test]
async fn ready_max_data_prefix_preserves_every_nonmatching_boundary() {
    let stream_id = StreamId(810);
    for asynchronous in [false, true] {
        // The different-stream frame is a defensive malformed-input boundary,
        // not a claim that the real carrier demultiplexer accepts it.
        let boundaries = [
            Some(Frame::StreamAck {
                stream_id,
                complete: false,
                ranges: vec![OffsetRange { start: 0, end: 1 }],
            }),
            Some(Frame::StreamData {
                stream_id,
                offset: 0,
                payload: Bytes::from_static(b"boundary"),
            }),
            Some(Frame::StreamFin {
                stream_id,
                final_offset: 0,
            }),
            Some(Frame::StreamReset {
                stream_id,
                reason: ResetReason::RemoteClosed,
            }),
            Some(Frame::StreamMaxData {
                stream_id: StreamId(stream_id.0 + 1),
                max_offset: u64::MAX,
            }),
            None,
        ];
        for boundary in boundaries {
            let mut fixture = ReadyMaxDataInput::new(stream_id);
            let first = MuxLimits::default().max_stream_window_bytes;
            let [a_first, _b_first] = fixture.publish(first);
            let [_a_last, b_last] = fixture.publish(first + 1);
            fixture.admit(0, Ok(a_first.clone()), 1).await;
            fixture
                .admit(
                    0,
                    boundary
                        .clone()
                        .ok_or(RuntimeError::ReliablePathSessionClosed),
                    2,
                )
                .await;
            fixture.admit(1, Ok(b_last.clone()), 3).await;
            let first_input = fixture.take_ready(asynchronous);
            assert_eq!(first_input.instance, fixture.remotes.paths[0].instance());
            assert!(matches!(first_input.frame, Ok(frame) if frame == a_first));
            assert_eq!(
                fixture.input.ready_frame_count(),
                2,
                "a deferred boundary remains visible to ready-batch accounting"
            );
            let middle = fixture.take_ready(asynchronous);
            assert_eq!(middle.instance, fixture.remotes.paths[0].instance());
            match boundary {
                Some(expected) => assert!(matches!(middle.frame, Ok(frame) if frame == expected)),
                None => assert!(matches!(
                    middle.frame,
                    Err(RuntimeError::ReliablePathSessionClosed)
                )),
            }
            assert_eq!(fixture.input.ready_frame_count(), 1);
            let last = fixture.take_ready(asynchronous);
            assert_eq!(last.instance, fixture.remotes.paths[1].instance());
            assert!(matches!(last.frame, Ok(frame) if frame == b_last));
            assert_eq!(fixture.input.ready_frame_count(), 0);
        }
    }
}

#[tokio::test]
async fn ready_max_data_prefix_drains_closed_input_without_waiting() {
    for asynchronous in [false, true] {
        let stream_id = StreamId(811);
        let mut fixture = ReadyMaxDataInput::new(stream_id);
        let [grant, _] = fixture.publish(MuxLimits::default().max_stream_window_bytes);
        let fin = Frame::StreamFin {
            stream_id,
            final_offset: 0,
        };
        fixture.admit(0, Ok(grant.clone()), 1).await;
        fixture.admit(0, Ok(fin.clone()), 2).await;
        fixture.input.frames_rx.close();
        let first = fixture.take_ready(asynchronous);
        assert!(matches!(first.frame, Ok(frame) if frame == grant));
        assert_eq!(fixture.input.ready_frame_count(), 1);
        assert!(fixture.input.has_buffered_frame());
        let last = fixture.take_ready(asynchronous);
        assert!(matches!(last.frame, Ok(frame) if frame == fin));
        assert_eq!(fixture.input.ready_frame_count(), 0);
        assert!(!fixture.input.has_buffered_frame());
        assert!(fixture.input.try_recv_frame().is_none());
        assert!(matches!(
            fixture.input.recv_frame().now_or_never(),
            Some(Err(RuntimeError::ReliablePathSessionClosed))
        ));
    }
}

#[tokio::test]
async fn request_membership_serializes_same_key_replacement() {
    let stream_id = StreamId(799);
    let (opened, _frames, _receivers) = opened_stream_at(stream_id, 0);
    let (mut remotes, _remote_input) = ReliableRelayRemoteSet::new(opened, 4);
    let predecessor = remotes.paths[0].instance();

    let (overlap, _overlap_frames, _overlap_receivers) = opened_stream_at(stream_id, 0);
    assert_eq!(
        remotes
            .try_attach_candidate(overlap)
            .expect("attachment identity remains available"),
        ReliableRelayAttachOutcome::RejectedDuplicate,
        "one current attachment already owns the stable request path key",
    );
    assert_eq!(remotes.path_instances(), vec![predecessor]);

    drop(remotes.remove_path_instance(predecessor));
    let (replacement, _replacement_frames, _replacement_receivers) = opened_stream_at(stream_id, 0);
    assert_eq!(
        remotes
            .try_attach_candidate(replacement)
            .expect("attachment identity remains available"),
        ReliableRelayAttachOutcome::Attached,
        "serialized predecessor removal makes the stable key vacant",
    );
    let successor = remotes.paths[0].instance();
    assert_eq!(successor.key, predecessor.key);
    assert_ne!(successor, predecessor);
}

#[tokio::test]
async fn attachment_identity_exhaustion_fails_before_request_membership_publication() {
    let stream_id = StreamId(800);
    let (opened, _frames, _receivers) = opened_stream_at(stream_id, 0);
    let (mut remotes, _remote_input) = ReliableRelayRemoteSet::new(opened, 4);
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
    let (_remotes, mut remote_input) = ReliableRelayRemoteSet::new(opened, 4);
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
        remote_input.recv_frame().await.expect("FIN frame").frame,
        Ok(Frame::StreamFin {
            stream_id: received_stream_id,
            final_offset: 8,
        }) if received_stream_id == stream_id
    ));
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), remote_input.recv_frame())
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
    let (_remotes, mut remote_input) = ReliableRelayRemoteSet::new(opened, 4);
    frames_tx
        .send(Ok(Frame::StreamFin {
            stream_id,
            final_offset: 0,
        }))
        .await
        .expect("queue FIN");
    drop(frames_tx);

    assert!(matches!(
        remote_input.recv_frame().await.expect("FIN frame").frame,
        Ok(Frame::StreamFin {
            stream_id: received_stream_id,
            final_offset: 0,
        }) if received_stream_id == stream_id
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(25), remote_input.recv_frame())
            .await
            .is_err(),
        "product terminal suppresses only an unclassified input close"
    );
    drop(command_receivers);
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), remote_input.recv_frame())
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
    let (mut remotes, mut remote_input) = ReliableRelayRemoteSet::new(opened, 4);
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
        remote_input.recv_frame().await.expect("FIN frame").frame,
        Ok(Frame::StreamFin { .. })
    ));

    drop(
        remotes
            .remove_path_instance(instance)
            .expect("remove exact attachment"),
    );
    drop(command_receivers);
    assert!(
        tokio::time::timeout(Duration::from_millis(25), remote_input.recv_frame())
            .await
            .is_err(),
        "removed attachment cannot publish a later stale lifecycle terminal"
    );
}

#[tokio::test]
async fn failed_attachment_retirement_cannot_block_healthy_sibling_scheduling() {
    let stream_id = StreamId(806);
    let (opened, _frames_tx, mut receivers) = opened_stream(stream_id);
    let (mut remotes, _remote_input) = ReliableRelayRemoteSet::new(opened, 4);
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
    let (mut remotes, mut remote_input) = ReliableRelayRemoteSet::new(opened, 4);
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
    let received = tokio::time::timeout(Duration::from_secs(1), remote_input.recv_frame())
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
    let (mut remotes, _remote_input) = ReliableRelayRemoteSet::new(opened, 4);
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
    let (mut remotes, _remote_input) = ReliableRelayRemoteSet::new(opened, 4);
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
    let (mut remotes, _remote_input) = ReliableRelayRemoteSet::new(terminal_opened, 4);
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
    let (mut remotes, _remote_input) = ReliableRelayRemoteSet::new(opened, 4);
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
    let (mut remotes, _remote_input) = ReliableRelayRemoteSet::new(opened, 4);
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
    let (mut remotes, _remote_input) = ReliableRelayRemoteSet::new(first_opened, 4);
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
