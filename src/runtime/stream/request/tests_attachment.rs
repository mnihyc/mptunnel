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
use std::sync::Weak;
use std::sync::atomic::AtomicUsize;
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
type InputForwarderObservers = HashMap<RelayPathInstance, Weak<AtomicUsize>>;
static INPUT_FORWARDER_OBSERVERS: OnceLock<Mutex<InputForwarderObservers>> = OnceLock::new();

pub(super) fn observe_attachment_input_processed(instance: RelayPathInstance) {
    if let Some(completed) = INPUT_FORWARDER_OBSERVERS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap()
        .get(&instance)
        .and_then(Weak::upgrade)
    {
        completed.fetch_add(1, Ordering::Release);
    }
}

struct InputForwarderWitness {
    instance: RelayPathInstance,
    completed: Arc<AtomicUsize>,
}

impl InputForwarderWitness {
    fn new(instance: RelayPathInstance) -> Self {
        let completed = Arc::new(AtomicUsize::new(0));
        assert!(
            INPUT_FORWARDER_OBSERVERS
                .get_or_init(|| Mutex::new(HashMap::new()))
                .lock()
                .unwrap()
                .insert(instance, Arc::downgrade(&completed))
                .is_none()
        );
        Self {
            instance,
            completed,
        }
    }
}

impl Drop for InputForwarderWitness {
    fn drop(&mut self) {
        INPUT_FORWARDER_OBSERVERS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .unwrap()
            .remove(&self.instance);
    }
}

struct ReadyMaxDataInput {
    remotes: ReliableRelayRemoteSet,
    input: ReliableRelayRemoteInput,
    frames: [mpsc::Sender<Result<Frame, RuntimeError>>; 2],
    _client_commands: [ReliablePathCommandReceivers; 2],
    publisher: std::sync::Arc<ResponseStreamBinding>,
    publication_commands: [ReliablePathCommandReceivers; 2],
    processed: [InputForwarderWitness; 2],
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
        let processed = [remotes.paths[0].instance(), remotes.paths[1].instance()];
        Self {
            remotes,
            input,
            frames: [a_frames, b_frames],
            _client_commands: [a_commands, b_commands],
            publisher,
            publication_commands: [a_publications, b_publications],
            processed: processed.map(InputForwarderWitness::new),
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

    async fn admit(&mut self, path: usize, frame: Result<Frame, RuntimeError>) {
        let completed = self.processed[path].completed.load(Ordering::Acquire);
        self.frames[path]
            .send(frame)
            .await
            .expect("admit actual attachment input");
        // This bounds fixture scheduling/cleanup only; it is not a Product
        // latency target, batching timer, or change to any queue capacity.
        tokio::time::timeout(Duration::from_secs(1), async {
            while self.processed[path].completed.load(Ordering::Acquire) == completed {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the actual attachment forwarder must finish processing this admitted input");
    }

    fn take_ready(&mut self, asynchronous: bool) -> ReliableRelayRemoteFrame {
        Self::take_ready_input(&mut self.input, asynchronous)
    }

    fn take_ready_input(
        input: &mut ReliableRelayRemoteInput,
        asynchronous: bool,
    ) -> ReliableRelayRemoteFrame {
        if asynchronous {
            input
                .recv_frame()
                .now_or_never()
                .expect("ready input must never wait for another grant")
                .expect("admitted input")
        } else {
            input.try_recv_frame().expect("admitted ready input")
        }
    }
}

async fn latest_max_data_crosses_ack_case(asynchronous: bool) {
    let stream_id = StreamId(809);
    let mut fixture = ReadyMaxDataInput::new(stream_id);
    assert!(fixture.input.recv_frame().now_or_never().is_none());
    assert!(fixture.input.try_recv_frame().is_none());
    let limits = MuxLimits::default();
    let initial = limits.max_stream_window_bytes;
    let first = initial
        .checked_add(reliable_relay_buffer_len(limits) as u64)
        .unwrap();
    let greatest = first
        .checked_add(reliable_relay_buffer_len(limits) as u64)
        .unwrap();
    let [_a_initial, b_initial] = fixture.publish(initial);
    fixture.admit(1, Ok(b_initial.clone())).await;
    let isolated = fixture.take_ready(asynchronous);
    assert_eq!(isolated.instance, fixture.remotes.paths[1].instance());
    assert!(matches!(isolated.frame, Ok(frame) if frame == b_initial));
    assert!(
        !fixture.input.has_buffered_frame(),
        "an isolated grant is not delayed"
    );

    let [a_first, _] = fixture.publish(first);
    let [_, b_latest] = fixture.publish(greatest);
    let ack = Frame::StreamAck {
        stream_id,
        scope_start: None,
        ranges: vec![OffsetRange { start: 0, end: 1 }],
    };
    let data = Frame::StreamData {
        stream_id,
        offset: 0,
        payload: Bytes::from_static(b"ready response"),
    };
    // All four inputs cross the real forwarders before any actor receive.
    // ACK is kept as evidence, but is not an authority barrier for MAX.
    fixture.admit(0, Ok(a_first)).await;
    fixture.admit(0, Ok(ack.clone())).await;
    fixture.admit(1, Ok(b_latest)).await;
    fixture.admit(1, Ok(data.clone())).await;
    let ready_before = fixture.input.ready_frame_count();
    let mut grants = Vec::new();
    let mut non_credit = Vec::new();
    for _ in 0..ready_before {
        let incoming = fixture.take_ready(asynchronous);
        match incoming.frame {
            Ok(Frame::StreamMaxData {
                stream_id: id,
                max_offset,
            }) => {
                assert_eq!(id, stream_id);
                assert!(max_offset == first || max_offset == greatest);
                grants.push((incoming.instance, max_offset));
            }
            Ok(frame) => {
                non_credit.push((incoming.instance, frame));
            }
            Err(error) => panic!("legal publication/input must not fail: {error}"),
        }
    }
    assert_eq!(
        non_credit,
        vec![
            (fixture.remotes.paths[0].instance(), ack),
            (fixture.remotes.paths[1].instance(), data),
        ],
        "every ACK and Data frame retains its exact contents, source and non-credit FIFO order",
    );
    assert_eq!(grants.iter().map(|(_, grant)| *grant).max(), Some(greatest));
    assert_eq!(
        grants
            .iter()
            .find(|(_, grant)| *grant == greatest)
            .unwrap()
            .0,
        fixture.remotes.paths[1].instance(),
        "the greatest grant retains its actual publishing attachment",
    );
    assert_eq!(fixture.input.ready_frame_count(), 0);
    assert!(fixture.input.try_recv_frame().is_none());
    // Proposed logical latest-credit model RED, not a pre-existing RFC
    // violation or a CPU/time assertion. The consecutive-fold implementation
    // passes all conservation/authority checks above before failing here.
    assert_eq!(
        grants.len(),
        1,
        "ACK-separated already-admitted MAX revisions must expose one latest grant, not two actor credit turns"
    );
    assert_eq!(grants[0], (fixture.remotes.paths[1].instance(), greatest));
}

#[tokio::test]
async fn latest_max_data_crosses_ack_before_response_async() {
    latest_max_data_crosses_ack_case(true).await;
}

#[tokio::test]
async fn latest_max_data_crosses_ack_before_response_try() {
    latest_max_data_crosses_ack_case(false).await;
}

#[tokio::test]
async fn latest_max_data_preserves_fifo_identity_and_reset_fence() {
    let stream_id = StreamId(810);
    for asynchronous in [false, true] {
        // Credit may cross these events; each event itself remains unchanged.
        // The different-stream MAX is a defensive malformed-input check, not
        // a claim that the authenticating carrier accepts another stream ID.
        let boundaries = [
            Some(Frame::StreamAck {
                stream_id,
                scope_start: None,
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
            let [a_last, b_last] = fixture.publish(first + 1);
            let older = a_first.clone();
            let equal = b_last.clone();
            fixture.admit(0, Ok(a_first)).await;
            fixture
                .admit(
                    0,
                    boundary
                        .clone()
                        .ok_or(RuntimeError::ReliablePathSessionClosed),
                )
                .await;
            fixture.admit(1, Ok(b_last.clone())).await;
            if boundary.is_some() {
                fixture.admit(0, Ok(a_last)).await;
            } else {
                // A's actual error terminates that forwarder, not the shared
                // authority: B remains able to publish and repeat the grant.
                fixture.admit(1, Ok(b_last)).await;
            }
            let mut grants = Vec::new();
            let mut preserved = 0;
            while fixture.input.has_buffered_frame() {
                let incoming = fixture.take_ready(asynchronous);
                match incoming.frame {
                    Ok(Frame::StreamMaxData {
                        stream_id: id,
                        max_offset,
                    }) if id == stream_id => {
                        grants.push((incoming.instance, max_offset));
                    }
                    other => {
                        assert_eq!(incoming.instance, fixture.remotes.paths[0].instance());
                        match &boundary {
                            Some(expected) => {
                                assert!(matches!(other, Ok(frame) if frame == *expected))
                            }
                            None => assert!(matches!(
                                other,
                                Err(RuntimeError::ReliablePathSessionClosed)
                            )),
                        }
                        preserved += 1;
                    }
                }
            }
            assert_eq!(preserved, 1);
            assert_eq!(fixture.input.ready_frame_count(), 0);
            assert_eq!(
                grants,
                vec![(fixture.remotes.paths[1].instance(), first + 1)],
                "only the strict greatest matching-stream grant survives; its equal sibling cannot replace the first greatest instance"
            );
            fixture.admit(1, Ok(equal)).await;
            fixture.admit(1, Ok(older)).await;
            assert!(
                !fixture.input.has_buffered_frame(),
                "equal and older grants cannot create new credit turns after the greatest was consumed"
            );
            assert!(fixture.input.try_recv_frame().is_none());
        }

        let mut fixture = ReadyMaxDataInput::new(stream_id);
        let first = MuxLimits::default().max_stream_window_bytes;
        let [a_first, _] = fixture.publish(first);
        fixture.admit(0, Ok(a_first.clone())).await;
        let isolated = fixture.take_ready(asynchronous);
        assert_eq!(isolated.instance, fixture.remotes.paths[0].instance());
        assert!(matches!(isolated.frame, Ok(frame) if frame == a_first));
        // Consuming the isolated grant makes ordinary fairness prefer FIFO.
        // The later pending grant must nevertheless be returned before RESET.
        let [_, b_greatest] = fixture.publish(first + 1);
        let [_, b_after_reset] = fixture.publish(first + 2);
        let reset = Frame::StreamReset {
            stream_id,
            reason: ResetReason::RemoteClosed,
        };
        fixture.admit(1, Ok(b_greatest.clone())).await;
        fixture.admit(0, Ok(reset.clone())).await;
        fixture.admit(1, Ok(b_after_reset)).await;
        let before_reset = fixture.take_ready(asynchronous);
        assert_eq!(before_reset.instance, fixture.remotes.paths[1].instance());
        assert!(matches!(before_reset.frame, Ok(frame) if frame == b_greatest));
        let terminal = fixture.take_ready(asynchronous);
        assert_eq!(terminal.instance, fixture.remotes.paths[0].instance());
        assert!(matches!(terminal.frame, Ok(frame) if frame == reset));
        assert!(
            !fixture.input.has_buffered_frame(),
            "same-stream RESET seals the logical credit owner; an already-processed later grant cannot resurrect it"
        );
        assert!(fixture.input.try_recv_frame().is_none());
    }
}

#[tokio::test]
async fn latest_max_data_drains_closed_input_and_cancelled_wait() {
    for asynchronous in [false, true] {
        let stream_id = StreamId(811);
        let mut fixture = ReadyMaxDataInput::new(stream_id);
        let [grant, _] = fixture.publish(MuxLimits::default().max_stream_window_bytes);
        let fin = Frame::StreamFin {
            stream_id,
            final_offset: 0,
        };
        assert!(fixture.input.recv_frame().now_or_never().is_none());
        fixture.admit(0, Ok(grant.clone())).await;
        fixture.admit(0, Ok(fin.clone())).await;
        let instance = fixture.remotes.paths[0].instance();
        let ReadyMaxDataInput {
            mut remotes,
            mut input,
            frames,
            _client_commands,
            publisher: _publisher,
            publication_commands: _publication_commands,
            processed: _processed,
        } = fixture;
        // Exact attachment removal cannot revoke admitted shared credit.
        // Drop both real forwarding owners and wait only for task cleanup,
        // leaving the input to drain its already-admitted state and FIFO.
        drop(remotes.remove_path_instance(instance));
        drop(remotes);
        for sender in &frames {
            tokio::time::timeout(Duration::from_secs(1), sender.closed())
                .await
                .expect("removed attachment input closes");
        }
        assert_eq!(input.ready_frame_count(), 2);
        let mut seen_grant = false;
        let mut seen_fin = false;
        for remaining in [1, 0] {
            let incoming = ReadyMaxDataInput::take_ready_input(&mut input, asynchronous);
            assert_eq!(incoming.instance, instance);
            match incoming.frame {
                Ok(frame) if frame == grant => {
                    assert!(!seen_grant);
                    seen_grant = true;
                }
                Ok(frame) if frame == fin => {
                    assert!(!seen_fin);
                    seen_fin = true;
                }
                _ => panic!("closed logical input must preserve both admitted frames"),
            }
            assert_eq!(input.ready_frame_count(), remaining);
        }
        assert!(seen_grant && seen_fin);
        assert!(!input.has_buffered_frame());
        assert!(input.try_recv_frame().is_none());
        assert!(matches!(
            input.recv_frame().now_or_never(),
            Some(Err(RuntimeError::ReliablePathSessionClosed))
        ));

        let mut fixture = ReadyMaxDataInput::new(stream_id);
        assert!(fixture.input.recv_frame().now_or_never().is_none());
        let [grant, _] = fixture.publish(MuxLimits::default().max_stream_window_bytes);
        fixture.admit(0, Ok(grant.clone())).await;
        assert!(matches!(fixture.take_ready(asynchronous).frame, Ok(frame) if frame == grant));
        let later = fixture.publish(MuxLimits::default().max_stream_window_bytes + 1);
        drop(fixture.input);
        for (sender, grant) in fixture.frames.iter().zip(later) {
            let _ = sender.send(Ok(grant)).await;
            tokio::time::timeout(Duration::from_secs(1), sender.closed())
                .await
                .expect("cancelled logical input must release each real forwarder");
        }
        // Raw stream-ID reuse is not reuse of the cancelled input owner.
        let mut replacement = ReadyMaxDataInput::new(stream_id);
        assert!(replacement.input.try_recv_frame().is_none());
        assert!(replacement.input.recv_frame().now_or_never().is_none());
    }
}

#[tokio::test]
async fn latest_max_data_and_ready_fifo_have_fair_service() {
    for asynchronous in [false, true] {
        let stream_id = StreamId(812);
        let mut fixture = ReadyMaxDataInput::new(stream_id);
        let expected = vec![
            Frame::StreamAck {
                stream_id,
                scope_start: None,
                ranges: vec![
                    OffsetRange { start: 0, end: 3 },
                    OffsetRange { start: 4, end: 7 },
                ],
            },
            Frame::StreamData {
                stream_id,
                offset: 0,
                payload: Bytes::from_static(b"ordered"),
            },
            Frame::StreamAck {
                stream_id,
                scope_start: Some(0),
                ranges: vec![OffsetRange { start: 0, end: 7 }],
            },
        ];
        for frame in &expected {
            fixture.admit(0, Ok(frame.clone())).await;
        }
        let mut preserved = Vec::new();
        let mut classes = Vec::new();
        // There are two continuously ready input classes. Replenish actual
        // credit before each take; one pair of turns must service both classes,
        // not drain an unbounded stream of credits before the queued FIFO.
        for revision in 0..2 * expected.len() {
            let offset = MuxLimits::default().max_stream_window_bytes + revision as u64;
            let [_, grant] = fixture.publish(offset);
            fixture.admit(1, Ok(grant)).await;
            let incoming = fixture.take_ready(asynchronous);
            match incoming.frame {
                Ok(Frame::StreamMaxData {
                    stream_id: id,
                    max_offset,
                }) => {
                    assert_eq!(id, stream_id);
                    assert_eq!(max_offset, offset);
                    assert_eq!(incoming.instance, fixture.remotes.paths[1].instance());
                    classes.push(true);
                }
                Ok(frame) => {
                    assert_eq!(incoming.instance, fixture.remotes.paths[0].instance());
                    preserved.push(frame);
                    classes.push(false);
                }
                Err(error) => panic!("ready source input cannot fail: {error}"),
            }
        }
        assert_eq!(
            preserved, expected,
            "both ACKs and the Data frame retain exact FIFO evidence"
        );
        assert!(
            classes.chunks_exact(2).all(|pair| pair[0] != pair[1]),
            "a continuously updated latest grant and a ready FIFO each receive service"
        );
        assert!(fixture.input.ready_frame_count() <= 1);
        if fixture.input.has_buffered_frame() {
            let incoming = fixture.take_ready(asynchronous);
            assert_eq!(incoming.instance, fixture.remotes.paths[1].instance());
            assert!(
                matches!(incoming.frame, Ok(Frame::StreamMaxData { stream_id: id, .. }) if id == stream_id)
            );
        }
        assert_eq!(fixture.input.ready_frame_count(), 0);
        assert!(fixture.input.try_recv_frame().is_none());
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
