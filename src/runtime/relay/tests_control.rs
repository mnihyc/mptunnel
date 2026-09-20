use super::*;
use crate::config::{ClientSecurityConfig, ResourceLimits, SharedSecret};
use crate::model::capacity::{
    reliable_relay_buffer_len, reliable_stream_initial_advertised_window_bytes,
};
use crate::model::path::RelayPathKey;
use crate::mux::MuxLimits;
use crate::protocol::{
    CloseReason, OffsetRange, PathId, PathUsage, StreamId, TargetAddr, UnderlayProtocol,
};
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandReceivers, recv_reliable_path_command,
    reliable_path_command_channels, try_recv_reliable_path_command,
    try_recv_reliable_path_priority_command,
};
use crate::runtime::stream::{
    ReliablePathStream, ReliablePathStreamOutput, ReliableRelayRemoteInput,
};
use crate::transport::PathSpec;
use bytes::Bytes;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll, Waker};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf, duplex};
use tokio::sync::{Notify, mpsc};

fn ready_feedback_item(frame: Result<Frame, RuntimeError>) -> ReliableRelayRemoteFrame {
    ReliableRelayRemoteFrame {
        instance: crate::model::path::RelayPathInstance {
            key: RelayPathKey {
                underlay: UnderlayProtocol::Tcp,
                index: 7,
            },
            path_instance_id: crate::model::path::CarrierPathInstanceId::from_raw(19),
            attachment_id: 23,
        },
        frame,
    }
}

#[test]
fn ready_client_feedback_preserves_each_ack_and_max_in_order() {
    let stream_id = StreamId(741);
    let sparse = Frame::StreamAck {
        stream_id,
        scope_start: Some(0),
        ranges: vec![OffsetRange { start: 4, end: 8 }],
    };
    let complete = Frame::StreamAck {
        stream_id,
        scope_start: None,
        ranges: vec![OffsetRange { start: 0, end: 8 }],
    };
    let expected = vec![
        sparse.clone(),
        Frame::StreamMaxData {
            stream_id,
            max_offset: 32,
        },
        complete.clone(),
        complete,
    ];
    let mut pending = expected[1..]
        .iter()
        .cloned()
        .map(|frame| ready_feedback_item(Ok(frame)))
        .collect::<VecDeque<_>>();
    let mut applied = Vec::new();
    let deferred = apply_ready_client_feedback(
        sparse,
        stream_id,
        pending.len(),
        || pending.pop_front(),
        |frame| {
            applied.push(frame);
            Ok(())
        },
    )
    .unwrap();
    assert!(deferred.is_none());
    assert!(pending.is_empty());
    assert_eq!(
        applied, expected,
        "replay and both ACK scopes remain separate transactions"
    );
}

#[test]
fn ready_client_feedback_retains_first_barrier_and_its_exact_instance() {
    let stream_id = StreamId(742);
    let ack = Frame::StreamAck {
        stream_id,
        scope_start: None,
        ranges: Vec::new(),
    };
    let barriers = vec![
        Ok(Frame::StreamData {
            stream_id,
            offset: 0,
            payload: Bytes::from_static(b"x"),
        }),
        Ok(Frame::StreamFin {
            stream_id,
            final_offset: 1,
        }),
        Ok(Frame::StreamReset {
            stream_id,
            reason: crate::protocol::ResetReason::RemoteClosed,
        }),
        Ok(Frame::StreamFeedbackProbe {
            stream_id,
            token: 5,
            max_offset: 32,
        }),
        Ok(Frame::StreamFeedbackReceipt {
            stream_id,
            token: 5,
        }),
        Ok(Frame::StreamMaxData {
            stream_id: StreamId(743),
            max_offset: 32,
        }),
        Ok(Frame::StreamAck {
            stream_id: StreamId(743),
            scope_start: None,
            ranges: Vec::new(),
        }),
        Err(RuntimeError::Protocol("ordered barrier")),
    ];
    for barrier in barriers {
        let expected_frame = barrier.as_ref().ok().cloned();
        let barrier = ready_feedback_item(barrier);
        let expected_instance = barrier.instance;
        let mut pending = VecDeque::from([barrier, ready_feedback_item(Ok(ack.clone()))]);
        let mut applied = Vec::new();
        let deferred = apply_ready_client_feedback(
            ack.clone(),
            stream_id,
            pending.len(),
            || pending.pop_front(),
            |frame| {
                applied.push(frame);
                Ok(())
            },
        )
        .unwrap()
        .expect("the non-feedback head must be retained");
        assert_eq!(deferred.instance, expected_instance);
        match expected_frame {
            Some(expected) => assert_eq!(deferred.frame.unwrap(), expected),
            None => assert!(matches!(
                deferred.frame,
                Err(RuntimeError::Protocol("ordered barrier"))
            )),
        }
        assert_eq!(applied.as_slice(), std::slice::from_ref(&ack));
        assert_eq!(
            pending.len(),
            1,
            "the ACK behind a barrier must not be inspected"
        );
        assert_eq!(pending.pop_front().unwrap().frame.unwrap(), ack);
    }
}

#[test]
fn ready_client_feedback_bounds_replenished_input_and_stops_on_apply_error() {
    let stream_id = StreamId(744);
    let ack = Frame::StreamAck {
        stream_id,
        scope_start: None,
        ranges: Vec::new(),
    };
    for ready_items in [0, 3] {
        let mut polls = 0;
        let mut applied = 0;
        let deferred = apply_ready_client_feedback(
            ack.clone(),
            stream_id,
            ready_items,
            || {
                polls += 1;
                Some(ready_feedback_item(Ok(ack.clone())))
            },
            |_| {
                applied += 1;
                Ok(())
            },
        )
        .unwrap();
        assert!(deferred.is_none());
        assert_eq!(
            polls, ready_items,
            "new arrivals cannot enlarge the entry snapshot"
        );
        assert_eq!(applied, 1 + ready_items);
    }
    let rejected = Frame::StreamAck {
        stream_id,
        scope_start: None,
        ranges: vec![OffsetRange { start: 0, end: 9 }],
    };
    let mut pending = VecDeque::from([
        ready_feedback_item(Ok(rejected.clone())),
        ready_feedback_item(Ok(ack.clone())),
    ]);
    let mut applied = Vec::new();
    let result = apply_ready_client_feedback(
        ack.clone(),
        stream_id,
        pending.len(),
        || pending.pop_front(),
        |frame| {
            if frame == rejected {
                return Err(RuntimeError::Protocol("apply rejected"));
            }
            applied.push(frame);
            Ok(())
        },
    );
    assert!(matches!(
        result,
        Err(RuntimeError::Protocol("apply rejected"))
    ));
    assert_eq!(
        applied.as_slice(),
        std::slice::from_ref(&ack),
        "accepted prefix is not replayed or discarded"
    );
    assert_eq!(
        pending.len(),
        1,
        "no successor is consumed after Apply fails"
    );
    assert_eq!(pending.pop_front().unwrap().frame.unwrap(), ack);
}

#[test]
fn request_live_tail_uses_the_immutable_shared_epoch_as_its_actor_wake() {
    let observed_at = Instant::now();
    let owner_deadline = observed_at + Duration::from_millis(20);
    let epoch_deadline = observed_at + Duration::from_millis(50);

    assert_eq!(
        request_live_owner_tail_wake(
            true,
            Some(owner_deadline),
            Some(epoch_deadline),
            observed_at,
        ),
        LiveOwnerRecoveryWake {
            due: false,
            deadline: Some(epoch_deadline),
        },
    );
    assert_eq!(
        request_live_owner_tail_wake(true, None, Some(epoch_deadline), observed_at),
        LiveOwnerRecoveryWake {
            due: false,
            deadline: None,
        },
        "an old stream epoch is not an independent cause without an owner fallback",
    );
    assert_eq!(
        request_live_owner_tail_wake(
            false,
            Some(owner_deadline),
            Some(epoch_deadline),
            observed_at,
        ),
        LiveOwnerRecoveryWake {
            due: false,
            deadline: None,
        },
        "an owner fallback is not an independent cause without a retained live tail",
    );
}

#[derive(Default)]
struct BlockedLocalDeliveryState {
    accepted: Mutex<Vec<u8>>,
    blocked: AtomicBool,
    released: AtomicBool,
    blocked_notify: Notify,
    write_waker: Mutex<Option<Waker>>,
    block_flush: bool,
}

#[derive(Clone)]
struct BlockedLocalDeliveryControl {
    state: Arc<BlockedLocalDeliveryState>,
}

impl BlockedLocalDeliveryControl {
    async fn wait_blocked(&self) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let blocked = self.state.blocked_notify.notified();
                if self.state.blocked.load(Ordering::Acquire) {
                    return;
                }
                blocked.await;
            }
        })
        .await
        .expect("relay reached the scripted blocked local write");
    }

    fn accepted_bytes(&self) -> Vec<u8> {
        self.state
            .accepted
            .lock()
            .expect("blocked local delivery bytes")
            .clone()
    }

    fn release(&self) {
        self.state.released.store(true, Ordering::Release);
        if let Some(waker) = self
            .state
            .write_waker
            .lock()
            .expect("blocked local delivery waker")
            .take()
        {
            waker.wake();
        }
    }
}

struct BlockedLocalDelivery {
    state: Arc<BlockedLocalDeliveryState>,
}

impl BlockedLocalDelivery {
    fn new() -> (Self, BlockedLocalDeliveryControl) {
        Self::with_flush_block(false)
    }

    fn with_flush_block(block_flush: bool) -> (Self, BlockedLocalDeliveryControl) {
        let state = Arc::new(BlockedLocalDeliveryState {
            block_flush,
            ..Default::default()
        });
        (
            Self {
                state: state.clone(),
            },
            BlockedLocalDeliveryControl { state },
        )
    }
}

impl AsyncRead for BlockedLocalDelivery {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut TaskContext<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Pending
    }
}

impl AsyncWrite for BlockedLocalDelivery {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let state = &self.state;
        let mut accepted = state.accepted.lock().expect("blocked local delivery bytes");
        if state.block_flush {
            accepted.extend_from_slice(buf);
            return Poll::Ready(Ok(buf.len()));
        }
        if accepted.is_empty() {
            accepted.push(buf[0]);
            return Poll::Ready(Ok(1));
        }
        if state.released.load(Ordering::Acquire) {
            accepted.extend_from_slice(buf);
            return Poll::Ready(Ok(buf.len()));
        }
        drop(accepted);
        state.blocked.store(true, Ordering::Release);
        state.blocked_notify.notify_waiters();
        *state
            .write_waker
            .lock()
            .expect("blocked local delivery waker") = Some(cx.waker().clone());
        if state.released.load(Ordering::Acquire) {
            cx.waker().wake_by_ref();
        }
        Poll::Pending
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        if self.state.block_flush && !self.state.released.load(Ordering::Acquire) {
            self.state.blocked.store(true, Ordering::Release);
            self.state.blocked_notify.notify_waiters();
            *self.state.write_waker.lock().expect("blocked flush waker") = Some(cx.waker().clone());
            if self.state.released.load(Ordering::Acquire) {
                cx.waker().wake_by_ref();
            }
            return Poll::Pending;
        }
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[derive(Default)]
struct ResponseStartupProgressObservation {
    open_started: bool,
    ack_frontier: u64,
    final_retained_ordinals: Option<Vec<u8>>,
    maximum_data_offset: Option<u64>,
}

impl ResponseStartupProgressObservation {
    fn observe_command(&mut self, command: ReliablePathCommand, stream_id: StreamId) {
        let ReliablePathCommand::SendFrame(frame) = command else {
            return;
        };
        match frame {
            Frame::StreamAck {
                stream_id: ack_stream_id,
                ranges,
                ..
            } if ack_stream_id == stream_id => {
                self.ack_frontier = self.ack_frontier.max(
                    ranges
                        .iter()
                        .filter(|range| range.start == 0)
                        .map(|range| range.end)
                        .max()
                        .unwrap_or(0),
                );
            }
            Frame::StreamReturnPlanFinal {
                stream_id: final_stream_id,
                retained_ordinals,
            } if final_stream_id == stream_id => {
                self.final_retained_ordinals = Some(retained_ordinals);
            }
            Frame::StreamMaxData {
                stream_id: max_data_stream_id,
                max_offset,
            } if max_data_stream_id == stream_id => {
                self.maximum_data_offset = Some(
                    self.maximum_data_offset
                        .unwrap_or(max_offset)
                        .max(max_offset),
                );
            }
            _ => {}
        }
    }
}

async fn observe_response_startup_progress(
    context: &ClientPathContext,
    receivers: &mut ReliablePathCommandReceivers,
    stream_id: StreamId,
    trigger_bytes: u64,
    observation: &mut ResponseStartupProgressObservation,
    timeout: Duration,
) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        observation.open_started |= context.response_startup_open_rounds_for_test() > 0;
        while let Some(command) = try_recv_reliable_path_command(receivers) {
            observation.observe_command(command, stream_id);
        }
        if observation.ack_frontier >= trigger_bytes
            && observation.final_retained_ordinals.is_some()
        {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            return;
        }
        tokio::task::yield_now().await;
    }
}

fn test_security() -> ClientSecurityConfig {
    ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    )
}

fn test_opened_remote_stream(
    stream_id: StreamId,
    path_index: usize,
    commands: crate::runtime::path::commands::ReliablePathCommandSender,
    frames: mpsc::Receiver<Result<Frame, RuntimeError>>,
) -> OpenedRemoteStream {
    test_opened_remote_stream_on(
        stream_id,
        path_index,
        UnderlayProtocol::Tcp,
        commands,
        frames,
    )
}

fn test_opened_remote_stream_on(
    stream_id: StreamId,
    path_index: usize,
    underlay: UnderlayProtocol,
    commands: crate::runtime::path::commands::ReliablePathCommandSender,
    frames: mpsc::Receiver<Result<Frame, RuntimeError>>,
) -> OpenedRemoteStream {
    OpenedRemoteStream::pending(
        ReliablePathStream {
            stream_id,
            max_offset: MuxLimits::default().max_stream_window_bytes,
            lane: TrafficClass::Latency,
            underlay,
            max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
            output: ReliablePathStreamOutput::fixed(
                underlay,
                PathId(path_index as u16),
                commands,
                MuxLimits::default(),
            ),
            frames: frames.into(),
        },
        path_index,
    )
}

async fn wait_for_buffered_remote_frame(remote_input: &ReliableRelayRemoteInput) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while !remote_input.has_buffered_frame() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("relay frame-forwarder deadline");
}

#[tokio::test]
async fn restart_reset_terminates_during_blocked_product_write() {
    for obsolete_generation in [false, true] {
        for insert_pending_task in [false, true] {
            restart_reset_during_blocked_product_write(obsolete_generation, insert_pending_task)
                .await;
        }
    }
}

async fn restart_reset_during_blocked_product_write(
    obsolete_generation: bool,
    insert_pending_task: bool,
) {
    let stream_id = StreamId(621);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener");
    let address = listener.local_addr().expect("test address");
    let context = ClientPathContext::new(
        vec![
            format!("tcp://{address}")
                .parse::<PathSpec>()
                .expect("test path"),
        ],
        test_security(),
        ResourceLimits::default(),
    )
    .expect("client context");
    let (commands, mut receivers) = reliable_path_command_channels(32);
    let (frames_tx, frames_rx) = mpsc::channel(2);
    let initial = test_opened_remote_stream(stream_id, 0, commands, frames_rx);
    let (local, control) = BlockedLocalDelivery::new();
    let release_reset = Arc::new(Notify::new());
    let (consumed_tx, consumed_rx) = tokio::sync::oneshot::channel();
    let ingress = super::super::lifecycle::BlockedWriteOpenTestIngress {
        startup_ordinal: None,
        insert_pending_task,
        obsolete_generation,
        opened: None,
        key: RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        },
        release: release_reset.clone(),
        consumed: consumed_tx,
    };
    let relay = tokio::spawn(async move {
        relay_migrating_tcp_stream_active(
            local,
            &context,
            MppPerformanceConfig::default(),
            ReliableRelayOpenSpec::new(TargetAddr::Ip(address), TrafficClass::Latency),
            initial,
            None,
            Some(ingress),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if matches!(
                recv_reliable_path_command(&mut receivers).await,
                Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
            ) {
                break;
            }
        }
    })
    .await
    .expect("relay initialized");
    frames_tx
        .send(Ok(Frame::StreamData {
            stream_id,
            offset: 0,
            payload: Bytes::from_static(b"blocked response"),
        }))
        .await
        .expect("inject Product response");
    control.wait_blocked().await;
    release_reset.notify_one();
    let _ = tokio::time::timeout(Duration::from_secs(1), consumed_rx)
        .await
        .expect("actor consumes the reset or terminates its open-task owner");
    let result = tokio::time::timeout(Duration::from_secs(1), relay)
        .await
        .expect("stream-terminal authority cannot wait for unrelated Product output")
        .expect("relay task");
    assert!(
        matches!(
            result,
            Err(RuntimeError::RemoteReset(
                crate::protocol::ResetReason::RemoteClosed
            ))
        ),
        "terminal reset survives blocked output: obsolete generation={obsolete_generation}, pending entry={insert_pending_task}"
    );
    assert_eq!(control.accepted_bytes(), b"b");
}

#[tokio::test]
async fn response_startup_ack_and_final_progress_during_blocked_local_delivery() {
    const STARTUP_TRIGGER_BYTES: usize = 58_400;
    let stream_id = StreamId(58_400);
    let first_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("first test carrier endpoint");
    let second_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("second test carrier endpoint");
    let first_addr = first_listener
        .local_addr()
        .expect("first test carrier address");
    let second_addr = second_listener
        .local_addr()
        .expect("second test carrier address");
    drop(second_listener);
    let context = ClientPathContext::new(
        vec![
            format!("tcp://{first_addr}")
                .parse::<PathSpec>()
                .expect("first test path"),
            format!("tcp://{second_addr}")
                .parse::<PathSpec>()
                .expect("second test path"),
        ],
        test_security(),
        ResourceLimits::default(),
    )
    .expect("client context");
    context.fail_response_startup_opens_for_test();
    let limits = context.mux_limits;
    let initial_window = reliable_stream_initial_advertised_window_bytes(
        UnderlayProtocol::Tcp,
        TrafficClass::Latency,
        limits,
    );
    assert!(
        reliable_relay_buffer_len(limits) >= STARTUP_TRIGGER_BYTES,
        "the decisive trigger must fit one received frame"
    );

    let (commands, mut command_receivers) = reliable_path_command_channels(32);
    let (frames_tx, frames_rx) = mpsc::channel(2);
    let initial = test_opened_remote_stream(stream_id, 0, commands, frames_rx);
    let initial_instance_id = initial.path_instance_id();
    let plan = Arc::new(
        ReliableRelayReturnPlan::new(
            STARTUP_TRIGGER_BYTES as u64,
            PathUsage::Available,
            vec![
                (
                    RelayPathKey {
                        underlay: UnderlayProtocol::Tcp,
                        index: 0,
                    },
                    Some(initial_instance_id),
                ),
                (
                    RelayPathKey {
                        underlay: UnderlayProtocol::Tcp,
                        index: 1,
                    },
                    None,
                ),
            ],
        )
        .expect("two-candidate lazy response-startup plan"),
    );
    let initial = initial.with_startup(plan, 0, Vec::new());
    let (local, local_control) = BlockedLocalDelivery::new();
    let relay_context = context.clone();
    let relay = tokio::spawn(async move {
        relay_migrating_tcp_stream(
            local,
            &relay_context,
            MppPerformanceConfig::default(),
            ReliableRelayOpenSpec::new(TargetAddr::Ip(first_addr), TrafficClass::Latency),
            initial,
            None,
        )
        .await
    });

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if matches!(
                recv_reliable_path_command(&mut command_receivers).await,
                Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
            ) {
                return;
            }
        }
    })
    .await
    .expect("initial attachment path proof is queued before response injection");

    let payload = Bytes::from(vec![0x5a; STARTUP_TRIGGER_BYTES]);
    frames_tx
        .send(Ok(Frame::StreamData {
            stream_id,
            offset: 0,
            payload: payload.clone(),
        }))
        .await
        .expect("inject exact contiguous response-startup trigger");
    local_control.wait_blocked().await;
    let accepted_while_blocked = local_control.accepted_bytes();

    let mut observation = ResponseStartupProgressObservation::default();
    observe_response_startup_progress(
        &context,
        &mut command_receivers,
        stream_id,
        STARTUP_TRIGGER_BYTES as u64,
        &mut observation,
        Duration::from_secs(1),
    )
    .await;
    let open_while_blocked = observation.open_started;
    let ack_frontier_while_blocked = observation.ack_frontier;
    let final_while_blocked = observation.final_retained_ordinals.clone();
    let max_data_while_blocked = observation.maximum_data_offset;

    local_control.release();
    observe_response_startup_progress(
        &context,
        &mut command_receivers,
        stream_id,
        STARTUP_TRIGGER_BYTES as u64,
        &mut observation,
        Duration::from_secs(1),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(1), async {
        while local_control.accepted_bytes().len() < STARTUP_TRIGGER_BYTES {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("release proves the same trigger progresses only after local delivery");
    let delivered_after_release = local_control.accepted_bytes();
    let open_rounds_after_release = context.response_startup_open_rounds_for_test();
    relay.abort();
    let _ = relay.await;

    assert_eq!(
        accepted_while_blocked,
        vec![0x5a],
        "the scripted Product sink must accept one byte and then block"
    );
    assert!(
        max_data_while_blocked.is_none_or(|max_offset| max_offset <= initial_window),
        "blocked Product bytes must not reopen receive credit beyond initial W; observed {max_data_while_blocked:?}"
    );
    assert_eq!(
        delivered_after_release.as_slice(),
        payload.as_ref(),
        "releasing the Product sink must deliver the exact payload once"
    );
    assert_eq!(
        open_rounds_after_release, 0,
        "reaching the response prefix closes membership without an h-only STARTUP round"
    );
    assert_eq!(
        observation.ack_frontier, STARTUP_TRIGGER_BYTES as u64,
        "the exact injected Product range must produce exact Data ACK [0,h)"
    );
    assert_eq!(
        observation.final_retained_ordinals,
        Some(vec![0]),
        "the exact committed opening attachment must produce FINAL [0]"
    );
    assert!(
        ack_frontier_while_blocked >= STARTUP_TRIGGER_BYTES as u64
            && !open_while_blocked
            && final_while_blocked == Some(vec![0]),
        "contiguous frontier h={STARTUP_TRIGGER_BYTES} must publish ACK and exact FINAL independently of blocked Product output and unused enrollment (ACK frontier={ack_frontier_while_blocked}, STARTUP={open_while_blocked}, FINAL={final_while_blocked:?})"
    );
}

#[test]
fn client_ack_gap_path_model_wait_tracks_measured_and_unmeasured_alternates() {
    assert!(
        reliable_relay_client_ack_gap_path_model_wait_active(true, true,),
        "the wait is deliberately independent of current target measurement: a target can appear, while a measured slow target can improve enough to pull fallback forward to the owner loss boundary"
    );
    assert!(!reliable_relay_client_ack_gap_path_model_wait_active(
        false, true,
    ));
    assert!(!reliable_relay_client_ack_gap_path_model_wait_active(
        true, false,
    ));
    assert!(reliable_relay_client_ack_gap_capacity_wait_arm_active(
        true, true,
    ));
    assert!(!reliable_relay_client_ack_gap_capacity_wait_arm_active(
        true, false,
    ));
}

#[tokio::test]
async fn prearmed_ack_gap_capacity_wait_retains_release_before_select_poll() {
    let capacity = Arc::new(tokio::sync::Notify::new());
    let wait = arm_carrier_capacity_notifies(vec![capacity.clone()])
        .expect("one carrier capacity notification");

    capacity.notify_waiters();

    tokio::time::timeout(Duration::from_millis(50), wait)
        .await
        .expect("pre-armed capacity release must remain ready before the select poll");
}

#[tokio::test]
async fn direct_recovery_service_wait_retains_capacity_release_before_next_select() {
    use futures::FutureExt;

    let stream_id = StreamId(920);
    let context = ClientPathContext::new(
        ["tcp://127.0.0.1:10922", "tcp://127.0.0.1:10923"]
            .into_iter()
            .map(|path| path.parse::<PathSpec>().expect("test path"))
            .collect(),
        test_security(),
        ResourceLimits::default(),
    )
    .expect("client context");
    let (owner_commands, mut owner_receivers) = reliable_path_command_channels(1);
    let (_owner_frames, owner_frames_rx) = mpsc::channel(1);
    let (mut remotes, _remote_input) = ReliableRelayRemoteSet::new(
        test_opened_remote_stream(stream_id, 0, owner_commands, owner_frames_rx),
        8,
    );
    let owner = remotes.paths[0].instance();
    let (target_commands, mut target_receivers) = reliable_path_command_channels(1);
    let (_target_frames, target_frames_rx) = mpsc::channel(1);
    remotes.attach_candidate(test_opened_remote_stream(
        stream_id,
        1,
        target_commands.clone(),
        target_frames_rx,
    ));
    let target = remotes.paths[1].instance();
    for receivers in [&mut owner_receivers, &mut target_receivers] {
        assert!(matches!(
            try_recv_reliable_path_priority_command(receivers),
            Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
        ));
    }
    for instance in [owner, target] {
        context.install_relay_path_instance_for_test(instance);
    }
    let mut send_stream = ReliableSendStream::new(stream_id, context.mux_limits);
    let original = send_stream
        .send_data(Bytes::from(vec![0x4b; 4096]))
        .expect("actual retained request source");
    let mut sender = RequestSenderService::new(stream_id);
    sender.record_original_frame_for_test(owner, &original);
    assert!(sender.mark_request_path_stale(&context, &remotes, owner, TrafficClass::Throughput));
    target_commands
        .try_enqueue_reinjection_frame(
            Frame::StreamData {
                stream_id: StreamId(921),
                offset: 0,
                payload: Bytes::from(vec![0x7a; 4096]),
            },
            TrafficClass::Throughput,
        )
        .expect("occupy the actual native repair lane");
    assert!(matches!(
        target_commands.try_enqueue_reinjection_frame(original, TrafficClass::Throughput),
        Err(RuntimeError::SenderServiceBlocked)
    ));

    // This is the actor's actual combined waiter, armed before observation and
    // retained across a blocked direct attempt without ever polling it yet.
    let service_wait = arm_request_recovery_service_wait(&context, &remotes);
    let queue = ReliableRelaySenderQueue::default();
    let mut recovery = sender.collect_request_path_recovery(&remotes, &queue);
    assert!(
        sender
            .dispatch_next_request_path_recovery(
                &mut recovery,
                &context,
                &mut remotes,
                &send_stream,
                &queue,
            )
            .expect("full native lane is not terminal")
            .is_none()
    );
    assert!(recovery.blocked_for_carrier_capacity);
    let filler = try_recv_reliable_path_command(&mut target_receivers)
        .expect("native repair capacity becomes available after the attempt");
    target_receivers.release_pending_command_bytes(
        crate::runtime::path::commands::reliable_path_command_pending_bytes(&filler),
    );
    assert!(
        service_wait.now_or_never().is_some(),
        "the same carried waiter must observe release before the next actor select poll"
    );
    assert!(queue.is_empty());
    assert_eq!(send_stream.reinjection_bytes(), 4096);
}

#[tokio::test]
async fn actor_recovery_pass_selects_survivor_after_collected_target_disappears() {
    let stream_id = StreamId(919);
    let context = ClientPathContext::new(
        [
            "tcp://127.0.0.1:10919?initial-srtt-s=0.08&initial-rate-mbps=100",
            "tcp://127.0.0.1:10920?initial-srtt-s=0.005&initial-rate-mbps=1000",
            "tcp://127.0.0.1:10921?initial-srtt-s=0.04&initial-rate-mbps=200",
        ]
        .into_iter()
        .map(|path| path.parse::<PathSpec>().expect("test path"))
        .collect(),
        test_security(),
        ResourceLimits::default(),
    )
    .expect("client context");

    let (owner_commands, mut owner_receivers) = reliable_path_command_channels(8);
    let (owner_frames, owner_frames_rx) = mpsc::channel(1);
    let (mut remotes, _remote_input) = ReliableRelayRemoteSet::new(
        test_opened_remote_stream(stream_id, 0, owner_commands, owner_frames_rx),
        8,
    );
    let owner = remotes.paths[0].instance();

    let (lost_commands, mut lost_receivers) = reliable_path_command_channels(8);
    let (lost_frames, lost_frames_rx) = mpsc::channel(1);
    remotes.attach_candidate(test_opened_remote_stream(
        stream_id,
        1,
        lost_commands,
        lost_frames_rx,
    ));
    let lost = remotes
        .paths
        .iter()
        .find(|path| path.key().index == 1)
        .expect("initial fast recovery target")
        .instance();

    let (survivor_commands, mut survivor_receivers) = reliable_path_command_channels(8);
    let (survivor_frames, survivor_frames_rx) = mpsc::channel(1);
    remotes.attach_candidate(test_opened_remote_stream(
        stream_id,
        2,
        survivor_commands,
        survivor_frames_rx,
    ));
    let survivor = remotes
        .paths
        .iter()
        .find(|path| path.key().index == 2)
        .expect("surviving recovery target")
        .instance();

    for receivers in [
        &mut owner_receivers,
        &mut lost_receivers,
        &mut survivor_receivers,
    ] {
        assert!(matches!(
            try_recv_reliable_path_priority_command(receivers),
            Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
        ));
    }
    for instance in [owner, lost, survivor] {
        context.install_relay_path_instance_for_test(instance);
    }

    let mut send_stream = ReliableSendStream::new(stream_id, context.mux_limits);
    let original = send_stream
        .send_data(Bytes::from(vec![0x5a; 4096]))
        .expect("retained request range");
    let mut sender = RequestSenderService::new(stream_id);
    sender.record_original_frame_for_test(owner, &original);
    assert!(sender.mark_request_path_stale(&context, &remotes, owner, TrafficClass::Throughput,));
    let sender_queue = ReliableRelaySenderQueue::default();
    let mut recovery = sender.collect_request_path_recovery(&remotes, &sender_queue);
    assert!(recovery.has_pending());
    assert!(
        sender_queue.is_empty(),
        "collection has no provisional target reservation"
    );

    drop(
        remotes
            .remove_path_instance(lost)
            .expect("retire the previously available fast target"),
    );
    let dispatch = sender
        .dispatch_next_request_path_recovery(
            &mut recovery,
            &context,
            &mut remotes,
            &send_stream,
            &sender_queue,
        )
        .expect("survivor recovery dispatch")
        .expect("the same actor pass chooses the survivor without a queued stale binding");
    assert!(matches!(dispatch, ClientQueuedDispatch::Reinjection { .. }));
    assert!(matches!(
        try_recv_reliable_path_command(&mut survivor_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            offset: 0,
            payload,
            ..
        })) if payload.len() == 4096
    ));
    assert!(sender_queue.is_empty());
    assert!(try_recv_reliable_path_command(&mut lost_receivers).is_none());
    assert!(try_recv_reliable_path_command(&mut owner_receivers).is_none());

    // Keep the mock attachment inputs alive until after the recovery assertion.
    drop((owner_frames, lost_frames, survivor_frames));
}

#[tokio::test]
async fn stale_path_failure_does_not_blacklist_same_key_successor() {
    let stream_id = StreamId(905);
    let path = "tcp://127.0.0.1:10905"
        .parse::<PathSpec>()
        .expect("test path");
    let context = ClientPathContext::new(vec![path], test_security(), ResourceLimits::default())
        .expect("client context");
    let (old_commands, _old_command_receivers) = reliable_path_command_channels(8);
    let (_old_frames_tx, old_frames_rx) = mpsc::channel(1);
    let (mut remotes, _remote_input) = ReliableRelayRemoteSet::new(
        test_opened_remote_stream(stream_id, 0, old_commands, old_frames_rx),
        8,
    );
    let stale = remotes.paths[0].instance();
    drop(
        remotes
            .remove_path_instance(stale)
            .expect("remove predecessor attachment"),
    );

    let (replacement_commands, _replacement_command_receivers) = reliable_path_command_channels(8);
    let (_replacement_frames_tx, replacement_frames_rx) = mpsc::channel(1);
    remotes.attach(test_opened_remote_stream(
        stream_id,
        0,
        replacement_commands,
        replacement_frames_rx,
    ));
    let successor = remotes.paths[0].instance();
    assert_eq!(successor.key, stale.key);
    assert_ne!(successor, stale);

    let mut sender = RequestSenderService::new(stream_id);
    let mut suppressions = ClientRelayPathOpenSuppressions::default();
    let error = RuntimeError::ReliablePathSessionClosed;
    let native_settlement = begin_client_relay_path_error(&context, &mut remotes, stale, &error);
    if let Some(settlement) = native_settlement.await {
        finish_client_relay_path_error(
            &mut sender,
            &context,
            &mut remotes,
            &mut suppressions,
            settlement,
        );
    }

    assert_eq!(remotes.paths.len(), 1);
    assert_eq!(remotes.paths[0].instance(), successor);
    assert!(
        !suppressions.blocks(&context, successor.key, tokio::time::Instant::now()),
        "a delayed exact-instance miss must not blacklist the live same-key successor"
    );
}

#[tokio::test]
async fn matching_path_failure_still_removes_and_suppresses_the_failed_instance() {
    let stream_id = StreamId(906);
    let path = "tcp://127.0.0.1:10906"
        .parse::<PathSpec>()
        .expect("test path");
    let context = ClientPathContext::new(vec![path], test_security(), ResourceLimits::default())
        .expect("client context");
    let (commands, _command_receivers) = reliable_path_command_channels(8);
    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let (mut remotes, _remote_input) = ReliableRelayRemoteSet::new(
        test_opened_remote_stream(stream_id, 0, commands, frames_rx),
        8,
    );
    let failed = remotes.paths[0].instance();
    context.install_relay_path_instance_for_test(failed);
    let mut sender = RequestSenderService::new(stream_id);
    let mut suppressions = ClientRelayPathOpenSuppressions::default();

    let error = RuntimeError::ReliablePathSessionClosed;
    let native_settlement = begin_client_relay_path_error(&context, &mut remotes, failed, &error);
    if let Some(settlement) = native_settlement.await {
        finish_client_relay_path_error(
            &mut sender,
            &context,
            &mut remotes,
            &mut suppressions,
            settlement,
        );
    }

    assert!(remotes.is_empty());
    assert!(suppressions.blocks(&context, failed.key, tokio::time::Instant::now()));
    let health = context.health().lock().expect("path health");
    assert_eq!(health.tcp[0].consecutive_failures, 1);
}

#[tokio::test]
async fn quic_request_stream_abandonment_detaches_only_its_logical_attachment() {
    let stream_id = StreamId(907);
    let path = "quic://127.0.0.1:10907"
        .parse::<PathSpec>()
        .expect("test QUIC path");
    let context = ClientPathContext::new(vec![path], test_security(), ResourceLimits::default())
        .expect("client context");
    let (commands, _command_receivers) = reliable_path_command_channels(8);
    let (frames_tx, frames_rx) = mpsc::channel(1);
    let (mut remotes, mut _remote_input) = ReliableRelayRemoteSet::new(
        test_opened_remote_stream_on(stream_id, 0, UnderlayProtocol::Udp, commands, frames_rx),
        8,
    );
    let attached = remotes.paths[0].instance();
    context.install_relay_path_instance_for_test(attached);
    let mut sender = RequestSenderService::new(stream_id);
    let mut suppressions = ClientRelayPathOpenSuppressions::default();
    frames_tx
        .send(Err(RuntimeError::QuicCarrier(
            crate::transport::quic::QuicCarrierError::H3Stream(
                h3::error::StreamError::RemoteTerminate {
                    code: h3::error::Code::from(0_u64),
                },
            ),
        )))
        .await
        .expect("publish request-stream abandonment");
    wait_for_buffered_remote_frame(&_remote_input).await;
    let ReliableRelayRemoteFrame { instance, frame } = _remote_input
        .recv_frame()
        .await
        .expect("forwarded request-stream abandonment");
    let error = frame.expect_err("request stream must report its abandonment");
    assert!(reliable_path_error_is_migratable(&error));

    let native_settlement = begin_client_relay_path_error(&context, &mut remotes, instance, &error);
    if let Some(settlement) = native_settlement.await {
        finish_client_relay_path_error(
            &mut sender,
            &context,
            &mut remotes,
            &mut suppressions,
            settlement,
        );
    }

    assert!(remotes.is_empty(), "only the abandoned attachment retires");
    assert!(
        suppressions.blocks(&context, attached.key, tokio::time::Instant::now()),
        "the failed logical stream must not immediately reopen the same QUIC request path"
    );
    let health = context.health().lock().expect("QUIC path health");
    let record = &health.udp[0];
    assert_eq!(record.path_instance_id(), Some(attached.path_instance_id));
    assert!(
        record.accepts_product_commit(attached.path_instance_id),
        "a request-stream reset must leave the shared QUIC carrier eligible"
    );
    assert_eq!(record.consecutive_failures, 0);
}

#[tokio::test]
async fn planned_drain_before_path_close_does_not_publish_terminal() {
    let stream_id = StreamId(909);
    let (commands, mut command_receivers) = reliable_path_command_channels(8);
    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let (_remotes, mut _remote_input) = ReliableRelayRemoteSet::new(
        test_opened_remote_stream(stream_id, 0, commands.clone(), frames_rx),
        8,
    );

    // Production starts the exact-instance planned lifecycle before the actor
    // closes fresh command admission, then waits for ordered PATH_CLOSE. No
    // ReliablePathRetired input can exist throughout this interval.
    commands.begin_path_drain();
    command_receivers.close_for_path_drain();
    assert!(
        !commands.is_terminal(),
        "closed admission remains a nonterminal planned-drain phase"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(25), _remote_input.recv_frame())
            .await
            .is_err(),
        "planned drain cannot publish a terminal event before ordered PATH_CLOSE"
    );
    drop(command_receivers);
}

#[tokio::test]
async fn planned_retirement_follows_every_preaccepted_frame_through_cap_one_fan_in() {
    let stream_id = StreamId(910);
    let (commands, mut command_receivers) = reliable_path_command_channels(8);
    let (frames_tx, frames_rx) = mpsc::channel(1);
    let (remotes, mut _remote_input) = ReliableRelayRemoteSet::new(
        test_opened_remote_stream(stream_id, 0, commands.clone(), frames_rx),
        1,
    );
    let instance = remotes.paths[0].instance();

    frames_tx
        .send(Ok(Frame::StreamData {
            stream_id,
            offset: 0,
            payload: Bytes::from_static(b"A"),
        }))
        .await
        .expect("queue first carrier frame");
    wait_for_buffered_remote_frame(&_remote_input).await;
    frames_tx
        .send(Ok(Frame::StreamData {
            stream_id,
            offset: 1,
            payload: Bytes::from_static(b"B"),
        }))
        .await
        .expect("queue second carrier frame");
    let held_input_permit = tokio::time::timeout(Duration::from_secs(1), frames_tx.reserve())
        .await
        .expect("forwarder accepted second frame")
        .expect("carrier input remains open");

    commands.begin_path_drain();
    command_receivers.close_for_path_drain();
    assert!(command_receivers.finish_planned_path_retirement());
    assert!(commands.is_terminal());

    let first = _remote_input
        .try_recv_frame()
        .expect("first merged carrier frame");
    assert_eq!(first.instance, instance);
    assert!(matches!(
        first.frame,
        Ok(Frame::StreamData {
            offset: 0,
            payload,
            ..
        }) if payload == Bytes::from_static(b"A")
    ));
    let second = tokio::time::timeout(Duration::from_secs(1), _remote_input.recv_frame())
        .await
        .expect("second frame deadline")
        .expect("second merged carrier frame");
    assert_eq!(second.instance, instance);
    assert!(matches!(
        second.frame,
        Ok(Frame::StreamData {
            offset: 1,
            payload,
            ..
        }) if payload == Bytes::from_static(b"B")
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(25), _remote_input.recv_frame())
            .await
            .is_err(),
        "terminal cannot bypass a pre-terminal input reservation"
    );
    drop(held_input_permit);
    let terminal = tokio::time::timeout(Duration::from_secs(1), _remote_input.recv_frame())
        .await
        .expect("planned terminal deadline")
        .expect("planned terminal frame");
    assert_eq!(terminal.instance, instance);
    assert!(matches!(
        terminal.frame,
        Err(RuntimeError::ReliablePathRetired)
    ));
}

#[tokio::test]
async fn unexpected_output_owner_drop_closes_retained_input_with_failure() {
    let stream_id = StreamId(908);
    let (commands, command_receivers) = reliable_path_command_channels(8);
    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let (remotes, mut _remote_input) = ReliableRelayRemoteSet::new(
        test_opened_remote_stream(stream_id, 0, commands, frames_rx),
        8,
    );
    let instance = remotes.paths[0].instance();
    drop(command_receivers);

    let terminal = tokio::time::timeout(Duration::from_secs(1), _remote_input.recv_frame())
        .await
        .expect("unexpected carrier terminal deadline")
        .expect("unexpected carrier terminal frame");
    assert_eq!(terminal.instance, instance);
    assert!(matches!(
        terminal.frame,
        Err(RuntimeError::ReliablePathSessionClosed)
    ));
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
    let (mut remotes, _remote_input) = ReliableRelayRemoteSet::new(opened, 8);
    let mut send_stream = ReliableSendStream::new(stream_id, limits);
    let sent = send_stream
        .send_data(Bytes::from_static(b"abcdefgh"))
        .expect("send request data");
    let mut sender = RequestSenderService::new(stream_id);
    sender.record_original_frame_for_test(remotes.paths[0].instance(), &sent);
    let mut sender_queue = ReliableRelaySenderQueue::default();
    sender_queue.push_reinjection(sent.clone());
    let mut state = ClientRelayState::new();
    let mut last_send_ack = AuthoritativeStreamAckSnapshot::default();

    let stream_before = send_stream.clone();
    let queue_bytes_before = sender_queue.bytes();
    let last_stream_at_before = state.progress.last_stream_at;
    let frontier_before = state.progress.last_send_ack_frontier;
    let snapshot_before = last_send_ack.clone();
    let rejected = apply_client_stream_ack(
        ClientStreamAckContext {
            state: &mut state,
            sender: &mut sender,
            sender_queue: &mut sender_queue,
            context: &context,
            remotes: &mut remotes,
            send_stream: &mut send_stream,
            last_send_ack: &mut last_send_ack,
            relay_lane: TrafficClass::Throughput,
        },
        stream_id,
        Some(0),
        vec![OffsetRange { start: 4, end: 9 }],
    );

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
    assert_eq!(last_send_ack, snapshot_before);
    let released = apply_client_stream_ack(
        ClientStreamAckContext {
            state: &mut state,
            sender: &mut sender,
            sender_queue: &mut sender_queue,
            context: &context,
            remotes: &mut remotes,
            send_stream: &mut send_stream,
            last_send_ack: &mut last_send_ack,
            relay_lane: TrafficClass::Throughput,
        },
        stream_id,
        Some(0),
        vec![OffsetRange { start: 0, end: 8 }],
    )
    .expect("exact assigned ACK commits");
    assert_eq!(released.released_bytes, 8);
    assert!(released.has_new_facts);
    assert!(sender_queue.is_empty());
    assert!(last_send_ack.gaps().is_empty());
}

async fn closed_output_relay(
    stream_id: StreamId,
    preaccepted_input: Option<Frame>,
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
    let (frames_tx, frames_rx) = mpsc::channel(4);
    if let Some(frame) = preaccepted_input {
        frames_tx
            .send(Ok(frame))
            .await
            .expect("queue pre-terminal carrier input");
    }
    drop(command_receivers);
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
            ReliableRelayOpenSpec::new(TargetAddr::Ip(unused_addr), TrafficClass::Latency),
            opened,
            None,
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
                scope_start: None,
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
            ReliableRelayOpenSpec::new(TargetAddr::Ip(unused_addr), TrafficClass::Latency),
            opened,
            None,
        )
        .await
    });
    (application, relay, frames_tx, command_receivers)
}

#[tokio::test]
async fn sticky_session_terminal_at_relay_entry_preempts_without_polling_a_saturated_carrier_queue()
{
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
    let stream_id = StreamId(609);
    let (commands, _command_receivers) = reliable_path_command_channels(1);
    let (frames_tx, frames_rx) = mpsc::channel(1);
    frames_tx
        .try_send(Ok(Frame::StreamData {
            stream_id,
            offset: 0,
            payload: Bytes::from_static(b"accepted before close"),
        }))
        .expect("saturate established carrier frame queue");
    assert_eq!(frames_tx.capacity(), 0);
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
    let (_application, relay_side) = duplex(4096);
    context.retire_session(CloseReason::PolicyRejected);

    let result = relay_migrating_tcp_stream(
        relay_side,
        &context,
        MppPerformanceConfig::default(),
        ReliableRelayOpenSpec::new(TargetAddr::Ip(unused_addr), TrafficClass::Latency),
        opened,
        None,
    )
    .await;
    assert!(matches!(
        result,
        Err(RuntimeError::RemoteClosed(CloseReason::PolicyRejected))
    ));
}

#[tokio::test]
async fn sticky_session_terminal_wins_when_ready_local_read_publishes_pending_reset() {
    use crate::model::path::{RelayPathInstance, next_carrier_path_instance_id};
    use crate::protocol::ResetReason;
    use crate::runtime::path::{
        ClientStreamTerminalScope, OpenedReliableCarrierStream, PendingStreamTerminal,
    };
    use std::future::Future;

    struct TerminalOnRead {
        context: ClientPathContext,
        pending: PendingStreamTerminal,
        stream_id: StreamId,
        session_first: bool,
        published: Arc<AtomicBool>,
    }

    impl AsyncRead for TerminalOnRead {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            assert!(
                buf.remaining() > 0,
                "actual source read has admission credit"
            );
            assert!(!self.published.swap(true, Ordering::AcqRel));
            if self.session_first {
                self.context.retire_session(CloseReason::PolicyRejected);
            }
            self.pending
                .publish_reset(self.stream_id, ResetReason::RemoteClosed);
            if !self.session_first {
                self.context.retire_session(CloseReason::PolicyRejected);
            }
            Poll::Ready(Err(std::io::Error::other("ready local read terminates")))
        }
    }

    impl AsyncWrite for TerminalOnRead {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
            bytes: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Poll::Ready(Ok(bytes.len()))
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    for session_first in [false, true] {
        let endpoint = "127.0.0.1:9".parse().unwrap();
        let context = ClientPathContext::new(
            vec!["tcp://127.0.0.1:9".parse::<PathSpec>().unwrap()],
            test_security(),
            ResourceLimits::default(),
        )
        .unwrap();
        let stream_id = StreamId(610);
        let limits = context.mux_limits;
        let (scope, owner) =
            ClientStreamTerminalScope::for_open(None, context.session_id, stream_id).unwrap();
        // The initial input commits normally. A different still-pending
        // attachment retains logical terminal authority across that commit.
        let pending = scope.pending_input();
        let (commands, mut receivers) = reliable_path_command_channels(32);
        let (_frames_tx, frames_rx) = mpsc::channel(4);
        let snapshot = crate::scheduler::PathSnapshot::new(
            PathId(0),
            UnderlayProtocol::Tcp,
            crate::runtime::path::model::default_path_srtt_ms(),
            crate::runtime::path::model::default_path_rate_bps(),
        );
        let carrier = OpenedReliableCarrierStream {
            retirement: None,
            terminal: Some(scope.pending_input()),
            terminal_owner: owner,
            stream_id,
            path_instance_id: next_carrier_path_instance_id(),
            max_offset: limits.max_stream_window_bytes,
            lane: TrafficClass::Latency,
            underlay: UnderlayProtocol::Tcp,
            max_frame_payload_bytes: reliable_relay_buffer_len(limits),
            portable_startup: snapshot,
            startup: snapshot,
            startup_native_window: None,
            startup_metrics: None,
            commands,
            mux_limits: limits,
            frames: frames_rx,
        }
        .guard_retirement();
        let opened = OpenedRemoteStream::from_opened_carrier(carrier, 0, 0);
        context.install_relay_path_instance_for_test(RelayPathInstance {
            key: RelayPathKey {
                underlay: UnderlayProtocol::Tcp,
                index: 0,
            },
            path_instance_id: opened.path_instance_id(),
            attachment_id: 0,
        });
        let published = Arc::new(AtomicBool::new(false));
        let local = TerminalOnRead {
            context: context.clone(),
            pending,
            stream_id,
            session_first,
            published: published.clone(),
        };
        let mut relay = Box::pin(relay_migrating_tcp_stream(
            local,
            &context,
            MppPerformanceConfig::default(),
            ReliableRelayOpenSpec::new(TargetAddr::Ip(endpoint), TrafficClass::Latency),
            opened,
            None,
        ));
        let result = tokio::time::timeout(Duration::from_secs(1), std::future::poll_fn(|cx| {
            let published_before = published.load(Ordering::Acquire);
            let result = relay.as_mut().poll(cx);
            if !published_before && published.load(Ordering::Acquire) {
                assert!(result.is_ready(), "the actual publication poll must settle, not wait for another session-select poll");
            }
            result
        })).await.expect("existing relay-control containment budget");
        assert!(published.load(Ordering::Acquire));
        assert!(
            matches!(
                result,
                Err(RuntimeError::RemoteClosed(CloseReason::PolicyRejected))
            ),
            "sticky session wins after ready settlement: session_first={session_first}"
        );
        assert!(
            matches!(
                scope.reset_error(),
                Some(RuntimeError::RemoteReset(ResetReason::RemoteClosed))
            ),
            "the pending attachment actually published the competing stream terminal"
        );
        let mut reset_closes = 0;
        while let Some(command) = try_recv_reliable_path_command(&mut receivers) {
            if matches!(command, ReliablePathCommand::ResetAndCloseStream { stream_id: id, .. } if id == stream_id)
            {
                reset_closes += 1;
            }
        }
        assert_eq!(
            reset_closes, 1,
            "active I/O-failure cleanup completes once inside its existing domain envelope"
        );
    }
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
    let (mut application, mut relay, frames_tx) = closed_output_relay(StreamId(610), None).await;

    application
        .write_all(b"send-first failure")
        .await
        .expect("application write");
    assert_absolute_retention_timeout(&mut relay).await;
    drop(frames_tx);
}

#[tokio::test]
async fn fin_feedback_carrier_loss_enters_absolute_session_retention() {
    let (application, mut relay, frames_tx) = closed_output_relay(
        StreamId(612),
        Some(Frame::StreamFin {
            stream_id: StreamId(612),
            final_offset: 0,
        }),
    )
    .await;

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
async fn retained_in_order_fin_commits_when_blocked_final_ack_retry_is_admitted() {
    let stream_id = StreamId(614);
    let limits = MuxLimits::default();
    let (commands, mut receivers) = reliable_path_command_channels(1);
    let (_frames_tx, frames_rx) = mpsc::channel(1);
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
                commands.clone(),
                limits,
            ),
            frames: frames_rx.into(),
        },
        0,
    );
    let (mut remotes, _remote_input) = ReliableRelayRemoteSet::new(opened, 4);
    while try_recv_reliable_path_priority_command(&mut receivers).is_some() {}
    commands
        .try_enqueue_admitted_frame(Frame::Ping { nonce: 1 }, TrafficClass::Control)
        .expect("occupy the exact control admission slot");

    let mut recv_stream = ReliableRecvStream::new(stream_id, limits);
    let mut state = ClientRelayState::new();
    assert!(
        receive_stream_fin(
            &recv_stream,
            &mut state.endpoint.pending_remote_fin_offset,
            0,
        )
        .expect("in-order FIN")
    );
    let publication = remotes.publish_stream_ack(
        1,
        vec![Frame::StreamAck {
            stream_id,
            scope_start: None,
            ranges: Vec::new(),
        }],
        vec![Frame::StreamAck {
            stream_id,
            scope_start: None,
            ranges: Vec::new(),
        }],
    );
    assert!(!publication.ack.published);
    assert!(publication.ack.pending);
    assert!(state.endpoint.remote_open);

    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::Ping { nonce: 1 }))
    ));
    let (mut application, mut relay_side) = duplex(64);
    let local_shutdown = retry_stream_ack_and_commit_ready_fin(
        &mut relay_side,
        &mut state,
        &mut recv_stream,
        &mut remotes,
    );
    local_shutdown
        .await
        .expect("retry final ACK and commit retained FIN");

    assert!(!state.endpoint.remote_open);
    assert_eq!(state.endpoint.pending_remote_fin_offset, None);
    assert!(!remotes.has_pending_stream_ack_publication());
    let mut byte = [0u8; 1];
    assert_eq!(
        application.read(&mut byte).await.expect("read half-close"),
        0
    );
}

#[tokio::test]
async fn final_ack_retry_commits_cross_kind_credit_without_releasing_fin_on_max_only() {
    let stream_id = StreamId(616);
    let limits = MuxLimits::default();
    let (commands, mut receivers) = reliable_path_command_channels(1);
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
                commands.clone(),
                limits,
            ),
            frames: frames_rx.into(),
        },
        0,
    );
    let (mut remotes, _remote_input) = ReliableRelayRemoteSet::new(opened, 4);
    while try_recv_reliable_path_priority_command(&mut receivers).is_some() {}
    let mut recv_stream = ReliableRecvStream::new_with_initial_max_offset(stream_id, limits, 2);
    recv_stream
        .receive_data(0, Bytes::from_static(b"a"))
        .unwrap();
    let first =
        remotes.publish_stream_ack(1, recv_stream.take_ack_update(), recv_stream.ack_frames());
    assert!(first.ack.published && !first.ack.pending);
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck { ranges, .. }))
            if ranges == vec![OffsetRange { start: 0, end: 1 }]
    ));

    commands
        .try_enqueue_admitted_frame(Frame::Ping { nonce: 1 }, TrafficClass::Control)
        .expect("block the one feedback slot after an actual ACK admission");
    let blocked_max = remotes.publish_max_data(128);
    assert!(blocked_max.max_data.pending);
    assert_eq!(blocked_max.max_data.published_offset, None);
    recv_stream
        .receive_data(1, Bytes::from_static(b"b"))
        .unwrap();
    let final_ack =
        remotes.publish_stream_ack(2, recv_stream.take_ack_update(), recv_stream.ack_frames());
    assert_eq!(final_ack.ack_generation, 2);
    assert!(!final_ack.ack.published && final_ack.ack.pending);
    assert_eq!(recv_stream.published_max_offset(), 2);

    let mut state = ClientRelayState::new();
    assert!(
        receive_stream_fin(
            &recv_stream,
            &mut state.endpoint.pending_remote_fin_offset,
            2,
        )
        .unwrap()
    );
    let (mut application, mut relay_side) = duplex(64);
    relay_side.write_all(b"ab").await.unwrap();
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::Ping { nonce: 1 }))
    ));
    retry_stream_ack_and_commit_ready_fin(
        &mut relay_side,
        &mut state,
        &mut recv_stream,
        &mut remotes,
    )
    .await
    .unwrap();

    assert_eq!(
        recv_stream.published_max_offset(),
        128,
        "ACK-triggered service commits actual MAX admission"
    );
    assert!(
        state.endpoint.remote_open,
        "MAX alone cannot publish the final ACK generation"
    );
    assert_eq!(state.endpoint.pending_remote_fin_offset, Some(2));
    assert!(remotes.has_pending_stream_ack_publication());
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamMaxData {
            max_offset: 128,
            ..
        }))
    ));

    let completed_from_max_retry = remotes.retry_pending_max_data();
    assert_eq!(completed_from_max_retry.ack_generation, 2);
    assert!(completed_from_max_retry.ack.published && !completed_from_max_retry.ack.pending);
    assert_eq!(completed_from_max_retry.max_data.published_offset, None);
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck { ranges, .. }))
            if ranges == vec![OffsetRange { start: 0, end: 2 }]
    ));
    // The FIN owner re-reads current publication status, even though another
    // trigger already completed the retained ACK job without publishing MAX.
    retry_stream_ack_and_commit_ready_fin(
        &mut relay_side,
        &mut state,
        &mut recv_stream,
        &mut remotes,
    )
    .await
    .unwrap();
    assert!(!state.endpoint.remote_open);
    assert_eq!(state.endpoint.pending_remote_fin_offset, None);
    let mut delivered = Vec::new();
    application.read_to_end(&mut delivered).await.unwrap();
    assert_eq!(delivered, b"ab");
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
    let (mut remotes, _remote_input) = ReliableRelayRemoteSet::new(first, 4);
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
            scope_start: None,
            ranges: Vec::new(),
        }],
        vec![Frame::StreamAck {
            stream_id,
            scope_start: None,
            ranges: Vec::new(),
        }],
    );
    assert!(publication.ack.published);
    assert!(publication.ack.pending);

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
    assert!(publication.ack.published);
    assert!(!publication.ack.pending);
    assert!(client_relay_finished(
        &state,
        &send_stream,
        &recv_stream,
        &sender_queue,
        &remotes,
    ));
}

#[tokio::test]
async fn client_completion_retains_zero_publication_requalification_ack() {
    let stream_id = StreamId(616);
    let limits = MuxLimits::default();
    let (commands, mut receivers) = reliable_path_command_channels(1);
    let (_frames_tx, frames_rx) = mpsc::channel(1);
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
                commands.clone(),
                limits,
            ),
            frames: frames_rx.into(),
        },
        0,
    );
    let (mut remotes, _remote_input) = ReliableRelayRemoteSet::new(opened, 1);
    while try_recv_reliable_path_priority_command(&mut receivers).is_some() {}
    commands
        .try_enqueue_admitted_frame(Frame::Ping { nonce: 616 }, TrafficClass::Control)
        .expect("fill the only exact reverse-control queue");

    let target = remotes.paths[0].instance();
    assert!(
        !remotes
            .publish_requalification_ack(
                target,
                Frame::StreamRequalifyAck {
                    stream_id,
                    probe_id: 1,
                    offset: 4096,
                    payload_bytes: 512,
                },
            )
            .expect("retain a zero-publication exact receipt")
    );
    assert!(remotes.has_pending_requalification_ack());

    let mut state = ClientRelayState::new();
    state.record_local_eof();
    state.record_local_fin_sent();
    state.record_terminal_fin_replayed();
    state.record_remote_finished();
    let send_stream = ReliableSendStream::new(stream_id, limits);
    let recv_stream = ReliableRecvStream::new(stream_id, limits);
    let sender_queue = ReliableRelaySenderQueue::default();
    assert!(
        !client_relay_finished(&state, &send_stream, &recv_stream, &sender_queue, &remotes,),
        "stream completion must not discard its retained exact requalification receipt",
    );
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
                    scope_start: None,
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

#[tokio::test]
async fn client_feedback_probe_requires_logical_delivery_and_actual_max_in_write_and_flush() {
    async fn receipt(receivers: &mut ReliablePathCommandReceivers) -> u64 {
        loop {
            let command = recv_reliable_path_command(receivers)
                .await
                .expect("live carrier");
            receivers.release_pending_command_bytes(
                crate::runtime::path::commands::reliable_path_command_pending_bytes(&command),
            );
            if let ReliablePathCommand::SendFrame(Frame::StreamFeedbackReceipt { token, .. }) =
                command
            {
                return token;
            }
        }
    }

    for block_flush in [false, true] {
        let stream_id = StreamId(if block_flush { 619 } else { 618 });
        let address = "127.0.0.1:9".parse().unwrap();
        let context = ClientPathContext::new(
            vec!["tcp://127.0.0.1:9".parse::<PathSpec>().unwrap()],
            test_security(),
            ResourceLimits::default(),
        )
        .unwrap();
        let initial_peer_max = context.mux_limits.max_stream_window_bytes;
        let (commands, mut receivers) = reliable_path_command_channels(16);
        let (frames_tx, frames_rx) = mpsc::channel(8);
        let opened = test_opened_remote_stream(stream_id, 0, commands, frames_rx);
        let (local, control) = BlockedLocalDelivery::with_flush_block(block_flush);
        let relay = tokio::spawn(async move {
            relay_migrating_tcp_stream(
                local,
                &context,
                MppPerformanceConfig::default(),
                ReliableRelayOpenSpec::new(TargetAddr::Ip(address), TrafficClass::Latency),
                opened,
                None,
            )
            .await
        });
        frames_tx
            .send(Ok(Frame::StreamData {
                stream_id,
                offset: 0,
                payload: Bytes::from_static(b"ab"),
            }))
            .await
            .unwrap();
        control.wait_blocked().await;
        frames_tx
            .send(Ok(Frame::StreamAck {
                stream_id,
                scope_start: None,
                ranges: Vec::new(),
            }))
            .await
            .unwrap();
        frames_tx
            .send(Ok(Frame::StreamFeedbackProbe {
                stream_id,
                token: 73,
                max_offset: 0,
            }))
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(30), receipt(&mut receivers))
                .await
                .is_err(),
            "native decode/FIFO admission cannot confirm a marker ahead of its blocked logical owner"
        );
        control.release();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), receipt(&mut receivers))
                .await
                .unwrap(),
            73
        );
        assert_eq!(
            control.accepted_bytes(),
            b"ab",
            "retained write is neither dropped nor replayed"
        );

        let required = initial_peer_max + 1;
        frames_tx
            .send(Ok(Frame::StreamFeedbackProbe {
                stream_id,
                token: 74,
                max_offset: required,
            }))
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(30), receipt(&mut receivers))
                .await
                .is_err(),
            "Probe.max_offset is not a credit grant"
        );
        frames_tx
            .send(Ok(Frame::StreamMaxData {
                stream_id,
                max_offset: required,
            }))
            .await
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), receipt(&mut receivers))
                .await
                .unwrap(),
            74,
            "actual diverted MAX wakes the retained exact reply"
        );
        frames_tx
            .send(Ok(Frame::StreamFeedbackProbe {
                stream_id,
                token: 74,
                max_offset: required,
            }))
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(30), receipt(&mut receivers))
                .await
                .is_err(),
            "an already admitted exact receipt is not a reply loop"
        );
        relay.abort();
        let _ = relay.await;
    }
}

#[tokio::test]
async fn ready_ack_gap_is_filled_before_intermediate_recovery_discovery() {
    use super::super::client::ReadyFeedbackObserver;
    use crate::model::capacity::PathRateSample;
    use crate::model::path::RelayPathInstance;
    use crate::mux::stream::ReliableRecvStream;
    use crate::runtime::path::commands::reliable_path_command_pending_bytes;
    use crate::runtime::sender::PreparedOriginalClaim;

    let stream_id = StreamId(723);
    // Long real initial RTT keeps setup away from due recovery. Actual source
    // claims and receiver-generated ACKs, not fabricated flight/cache state,
    // create and then discharge the interior omission.
    let context = ClientPathContext::new(
        [
            "tcp://127.0.0.1:9?initial-srtt-s=10&initial-rate-mbps=100",
            "tcp://127.0.0.1:10?initial-srtt-s=10&initial-rate-mbps=100",
        ]
        .into_iter()
        .map(|path| path.parse::<PathSpec>().unwrap())
        .collect(),
        test_security(),
        ResourceLimits::default(),
    )
    .unwrap();
    let limits = context.mux_limits;
    let (owner_commands, mut owner_receivers) = reliable_path_command_channels(32);
    let (owner_frames, owner_input) = mpsc::channel(8);
    let mut initial = test_opened_remote_stream(stream_id, 0, owner_commands, owner_input);
    initial.stream_mut().lane = TrafficClass::Throughput;
    let (target_commands, mut target_receivers) = reliable_path_command_channels(8);
    let (_target_frames, target_input) = mpsc::channel(8);
    let mut target = test_opened_remote_stream(stream_id, 1, target_commands, target_input);
    target.stream_mut().lane = TrafficClass::Throughput;
    for (index, opened) in [(0, &initial), (1, &target)] {
        let instance = RelayPathInstance {
            key: RelayPathKey {
                underlay: UnderlayProtocol::Tcp,
                index,
            },
            path_instance_id: opened.path_instance_id(),
            attachment_id: index as u64,
        };
        context.install_relay_path_instance_for_test(instance);
        context.mark_tcp_path_open_success(
            index,
            Duration::from_secs(10),
            TrafficClass::Throughput,
        );
        context.mark_relay_path_rate_sample_for_test(
            instance.key,
            PathRateSample::new(4 * 1024 * 1024, Duration::from_millis(20)).unwrap(),
        );
        assert!(context.relay_path_instance_has_bulk_model_evidence(instance));
    }
    let release_attach = Arc::new(Notify::new());
    let (attached_tx, attached_rx) = tokio::sync::oneshot::channel();
    let ingress = super::super::lifecycle::BlockedWriteOpenTestIngress {
        startup_ordinal: None,
        insert_pending_task: true,
        key: RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 1,
        },
        obsolete_generation: false,
        opened: Some(target),
        release: release_attach.clone(),
        consumed: attached_tx,
    };
    let source = Bytes::from(vec![0x61; 3 * reliable_relay_buffer_len(limits)]);
    let (local, mut peer) = duplex(source.len());
    peer.write_all(&source).await.unwrap();
    peer.shutdown().await.unwrap();
    let observer = Arc::new(ReadyFeedbackObserver::default());
    let relay_observer = observer.clone();
    let relay_context = context.clone();
    let relay = tokio::spawn(async move {
        relay_observer
            .run(
                stream_id,
                relay_migrating_tcp_stream_active(
                    local,
                    &relay_context,
                    MppPerformanceConfig::default(),
                    ReliableRelayOpenSpec::new(
                        TargetAddr::Ip("127.0.0.1:9".parse().unwrap()),
                        TrafficClass::Throughput,
                    ),
                    initial,
                    None,
                    Some(ingress),
                ),
            )
            .await
    });
    let mut originals = Vec::new();
    let mut claimed_bytes = 0usize;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let command = recv_reliable_path_command(&mut owner_receivers)
                .await
                .unwrap();
            let pending = reliable_path_command_pending_bytes(&command);
            match command {
                ReliablePathCommand::PreparedOriginal(work) => {
                    let ready = owner_receivers
                        .writer_ready_boundary(work.path_instance_id())
                        .expect("actual native writer opportunity");
                    match work.try_claim(ready) {
                        PreparedOriginalClaim::Claimed(frame) => {
                            let charged = owner_receivers.register_claimed_writer_frame(&frame);
                            let Frame::StreamData {
                                stream_id: actual,
                                offset,
                                payload,
                            } = &frame
                            else {
                                panic!("an actual Original claim contains DATA");
                            };
                            assert_eq!(*actual, stream_id);
                            assert_eq!(*offset, claimed_bytes as u64);
                            assert_eq!(
                                payload.as_ref(),
                                &source[claimed_bytes..claimed_bytes + payload.len()]
                            );
                            claimed_bytes += payload.len();
                            originals.push(frame);
                            owner_receivers.release_pending_command_bytes(charged);
                            work.requeue();
                        }
                        PreparedOriginalClaim::CarrierFailed(error) => {
                            panic!("unexpected fixture carrier failure: {error}")
                        }
                        PreparedOriginalClaim::RecoveryQueued => {
                            panic!("fixture has no independently due alternate recovery");
                        }
                        PreparedOriginalClaim::Empty => {}
                        PreparedOriginalClaim::Busy(_) => {
                            panic!("sole fixture writer has no competing Product holder");
                        }
                        PreparedOriginalClaim::Blocked(wait) => {
                            assert_eq!(claimed_bytes, source.len(),
                                "sole healthy ready writer must claim all finite source before blocking");
                            // The final requeued notice can retain recovery work
                            // for these unACKed Originals after source is exhausted.
                            // Park its actual wait while the actor publishes FIN.
                            owner_receivers.defer_prepared_work(work, wait);
                        }
                    }
                }
                ReliablePathCommand::SendFrame(Frame::StreamFin { final_offset, .. }) => {
                    owner_receivers.withdraw_writer_ready();
                    owner_receivers.release_pending_command_bytes(pending);
                    assert_eq!(final_offset, source.len() as u64);
                    break;
                }
                ReliablePathCommand::SendFrame(Frame::StreamData { .. }) => {
                    panic!("actor may not bypass actual native Original claims");
                }
                ReliablePathCommand::SendFrame(_) => owner_receivers.withdraw_writer_ready(),
                _ => panic!("unexpected source lifecycle command"),
            }
            owner_receivers.release_pending_command_bytes(pending);
        }
    })
    .await
    .expect("actual actor claims all source before FIN");
    assert_eq!(claimed_bytes, source.len());
    assert!(originals.len() >= 3);
    release_attach.notify_one();
    tokio::time::timeout(Duration::from_secs(5), attached_rx)
        .await
        .unwrap()
        .unwrap();
    // The real successful attachment first replays the fixed FIN, then issues
    // its ordinary path proof; neither command manufactures copied ownership.
    let fin = try_recv_reliable_path_priority_command(&mut target_receivers).unwrap();
    assert!(
        matches!(&fin, ReliablePathCommand::SendFrame(Frame::StreamFin { stream_id: actual, final_offset })
        if *actual == stream_id && *final_offset == source.len() as u64)
    );
    target_receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&fin));
    let proof = try_recv_reliable_path_priority_command(&mut target_receivers).unwrap();
    assert!(matches!(
        &proof,
        ReliablePathCommand::SendFrame(Frame::PathProofData { .. })
    ));
    target_receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&proof));

    let mut receiver = ReliableRecvStream::new(stream_id, limits);
    for frame in [&originals[0], originals.last().unwrap()] {
        let Frame::StreamData {
            offset, payload, ..
        } = frame
        else {
            unreachable!()
        };
        receiver.receive_data(*offset, payload.clone()).unwrap();
    }
    let mut sparse = receiver.ack_frames();
    assert_eq!(sparse.len(), 1);
    let ack1 = sparse.pop().unwrap();
    let Frame::StreamAck {
        scope_start: scope1,
        ranges: ranges1,
        ..
    } = &ack1
    else {
        unreachable!()
    };
    assert_eq!(*scope1, Some(0));
    assert_eq!(ranges1.len(), 2);
    let expected_gap = OffsetRange {
        start: ranges1[0].end,
        end: ranges1[1].start,
    };
    assert!(expected_gap.start < expected_gap.end);
    for frame in &originals[1..originals.len() - 1] {
        let Frame::StreamData {
            offset, payload, ..
        } = frame
        else {
            unreachable!()
        };
        receiver.receive_data(*offset, payload.clone()).unwrap();
    }
    assert_eq!(receiver.next_offset(), source.len() as u64);
    let mut complete = receiver.ack_frames();
    assert_eq!(complete.len(), 1);
    let ack2 = complete.pop().unwrap();
    let Frame::StreamAck {
        scope_start: scope2,
        ranges: ranges2,
        ..
    } = &ack2
    else {
        unreachable!()
    };
    // The receiver now has one full positive prefix. scoped_ack_frames omits
    // an empty negative-authority header (last_range.start == high_water == 0);
    // positive-only ACK2 still discharges ACK1's retained omission.
    assert_eq!(*scope2, None);
    assert_eq!(
        ranges2,
        &[OffsetRange {
            start: 0,
            end: source.len() as u64
        }]
    );
    let signatures = [(*scope1, ranges1.clone()), (*scope2, ranges2.clone())];
    owner_frames.send(Ok(ack1)).await.unwrap();
    owner_frames.send(Ok(ack2)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), observer.wait_for_applied(2))
        .await
        .expect("both actual ordered ACK transactions apply");
    let before = observer.before.lock().unwrap().clone();
    let after = observer.after.lock().unwrap().clone();
    relay.abort();
    let _ = relay.await;

    assert_eq!(
        observer.ready_after_first_selection.load(Ordering::Acquire),
        1,
        "ACK2 alone is already ready in the shared input before ACK1 Apply"
    );
    assert_eq!(before.len(), 2);
    assert_eq!(after.len(), 2);
    for (index, (scope, ranges)) in signatures.iter().enumerate() {
        assert_eq!(
            (before[index].scope_start, &before[index].ranges),
            (*scope, ranges)
        );
        assert_eq!(
            (after[index].scope_start, &after[index].ranges),
            (*scope, ranges)
        );
        assert_eq!(before[index].assigned, source.len() as u64);
    }
    assert_eq!(before[0].retained, source.len());
    assert_eq!(after[0].gaps, vec![expected_gap]);
    assert_eq!(before[1].gaps, after[0].gaps);
    assert_eq!(after[0].frontier, expected_gap.start);
    assert_eq!(
        after[0].retained as u64,
        expected_gap.end - expected_gap.start
    );
    assert!(after[1].gaps.is_empty());
    assert_eq!(after[1].frontier, source.len() as u64);
    assert_eq!(after[1].retained, 0);
    assert_eq!(
        before[1].calls, before[0].calls,
        "already-ready ACK2 must fill ACK1's real gap before intermediate recovery discovery"
    );
}

#[tokio::test]
async fn prepared_request_actor_keeps_eof_source_claimable_until_final_offset() {
    use crate::model::capacity::adaptive_reliable_relay_chunk_bytes;
    use crate::model::path::RelayPathInstance;
    use crate::runtime::path::commands::{
        reliable_path_command_pending_bytes, reliable_path_command_queue,
    };
    use crate::runtime::sender::PreparedOriginalClaim;

    struct EofObservedSource {
        remaining: Bytes,
        eof_observed: Arc<Notify>,
    }

    impl AsyncRead for EofObservedSource {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            if buf.remaining() == 0 {
                return Poll::Ready(Ok(()));
            }
            if self.remaining.is_empty() {
                self.eof_observed.notify_one();
            } else {
                let count = buf.remaining().min(self.remaining.len());
                buf.put_slice(&self.remaining.split_to(count));
            }
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for EofObservedSource {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("reserve fixture endpoint");
    let endpoint = listener.local_addr().expect("fixture endpoint");
    let context = ClientPathContext::new(
        vec![format!("tcp://{endpoint}").parse().expect("TCP path")],
        test_security(),
        ResourceLimits::default(),
    )
    .expect("client context");
    let limits = context.mux_limits;
    let quantum = adaptive_reliable_relay_chunk_bytes(None, TrafficClass::Latency, limits);
    let source = Bytes::from(
        (0..2 * quantum + 13)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>(),
    );
    let stream_id = StreamId(720);
    let (commands, mut receivers) =
        reliable_path_command_channels(reliable_path_command_queue(limits));
    let (frames_tx, frames_rx) = mpsc::channel(8);
    let opened = test_opened_remote_stream(stream_id, 0, commands.clone(), frames_rx);
    context.install_relay_path_instance_for_test(RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        },
        path_instance_id: opened.path_instance_id(),
        attachment_id: 0,
    });
    let eof_observed = Arc::new(Notify::new());
    let local = EofObservedSource {
        remaining: source.clone(),
        eof_observed: eof_observed.clone(),
    };
    let relay_context = context.clone();
    let mut relay = tokio::spawn(async move {
        relay_migrating_tcp_stream(
            local,
            &relay_context,
            MppPerformanceConfig::default(),
            ReliableRelayOpenSpec::new(TargetAddr::Ip(endpoint), TrafficClass::Latency),
            opened,
            None,
        )
        .await
    });

    // The actor itself observes EOF before this fixture ever offers a writer
    // boundary. These bytes therefore remain unclaimed U, not queued Originals.
    tokio::time::timeout(Duration::from_secs(5), eof_observed.notified())
        .await
        .expect("the ordinary source admission stages this finite input through EOF");
    assert!(commands.writer_boundary().snapshot().is_none());
    assert!(
        !relay.is_finished(),
        "EOF must not discard unclaimed source"
    );
    let mut notices = std::collections::VecDeque::new();
    while let Some(command) = try_recv_reliable_path_command(&mut receivers) {
        let pending = reliable_path_command_pending_bytes(&command);
        match command {
            ReliablePathCommand::PreparedOriginal(work) => notices.push_back(work),
            ReliablePathCommand::SendFrame(Frame::StreamData { .. } | Frame::StreamFin { .. }) => {
                panic!("neither assigned source nor FIN may precede the first writer claim");
            }
            ReliablePathCommand::SendFrame(_) => {}
            _ => panic!("unexpected native lifecycle command before claiming EOF source"),
        }
        receivers.release_pending_command_bytes(pending);
    }
    assert!(
        !notices.is_empty(),
        "EOF source retains its actual weak writer notice"
    );
    assert_eq!(
        commands.pending_bytes(),
        0,
        "prepared notices carry no payload debt"
    );
    assert_eq!(commands.writer_pending_bytes(), 0);

    let mut claimed = Vec::new();
    let final_offset = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let command = match notices.pop_front() {
                Some(work) => ReliablePathCommand::PreparedOriginal(work),
                None => recv_reliable_path_command(&mut receivers)
                    .await
                    .expect("EOF source keeps its carrier live"),
            };
            let pending = reliable_path_command_pending_bytes(&command);
            match command {
                ReliablePathCommand::PreparedOriginal(work) => {
                    let ready = receivers
                        .writer_ready_boundary(work.path_instance_id())
                        .expect("actual fixture writer enters its next native transaction");
                    match work.try_claim(ready) {
                        PreparedOriginalClaim::Claimed(frame) => {
                            let charged = receivers.register_claimed_writer_frame(&frame);
                            let Frame::StreamData {
                                stream_id: actual,
                                offset,
                                payload,
                            } = frame
                            else {
                                panic!("an Original claim returns only StreamData");
                            };
                            assert_eq!(actual, stream_id);
                            assert_eq!(offset, claimed.len() as u64);
                            assert!(!payload.is_empty());
                            claimed.extend_from_slice(&payload);
                            assert!(claimed.len() <= source.len());
                            assert_eq!(claimed.as_slice(), &source[..claimed.len()]);
                            receivers.release_pending_command_bytes(charged);
                            work.requeue();
                        }
                        PreparedOriginalClaim::CarrierFailed(error) => {
                            panic!("unexpected fixture carrier failure: {error}")
                        }
                        PreparedOriginalClaim::RecoveryQueued => {
                            panic!("fixture has no independently due alternate recovery");
                        }
                        PreparedOriginalClaim::Empty => {
                            // An exhausted or superseded weak notice owns no bytes.
                        }
                        PreparedOriginalClaim::Busy(_) => {
                            panic!("the sole ready healthy fixture writer has no competing Product holder");
                        }
                        PreparedOriginalClaim::Blocked(wait) => {
                            assert_eq!(claimed.len(), source.len(),
                                "all admitted EOF source must remain immediately claimable");
                            // The final weak notice can retain outstanding recovery
                            // deadlines after source is exhausted. The real writer
                            // parks that notice and still services the queued FIN.
                            receivers.defer_prepared_work(work, wait);
                        }
                    }
                }
                ReliablePathCommand::SendFrame(Frame::StreamFin {
                    stream_id: actual,
                    final_offset,
                }) => {
                    receivers.withdraw_writer_ready();
                    assert_eq!(actual, stream_id);
                    assert_eq!(claimed.as_slice(), source.as_ref());
                    receivers.release_pending_command_bytes(pending);
                    break final_offset;
                }
                ReliablePathCommand::SendFrame(Frame::StreamData { .. }) => {
                    panic!(
                        "unclaimed source must use the actual writer claim, not payload commands"
                    );
                }
                ReliablePathCommand::SendFrame(_) => {
                    receivers.withdraw_writer_ready();
                }
                _ => panic!("unexpected lifecycle command before the EOF final offset"),
            }
            receivers.release_pending_command_bytes(pending);
        }
    })
    .await
    .expect("all prepared EOF bytes become contiguous native claims before FIN");
    assert_eq!(final_offset, source.len() as u64);
    frames_tx
        .send(Ok(Frame::StreamAck {
            stream_id,
            scope_start: Some(0),
            ranges: vec![OffsetRange {
                start: 0,
                end: final_offset,
            }],
        }))
        .await
        .expect("settle all claimed request bytes");
    frames_tx
        .send(Ok(Frame::StreamFin {
            stream_id,
            final_offset: 0,
        }))
        .await
        .expect("peer finishes its empty response");
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            tokio::select! {
                result = &mut relay => {
                    result.expect("relay task").expect("orderly EOF relay completion");
                    break;
                }
                command = recv_reliable_path_command(&mut receivers) => {
                    let command = command.expect("carrier remains until relay cleanup");
                    receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&command));
                }
            }
        }
    })
    .await
    .expect("final ACK and peer FIN settle the actual actor");
    // Actor completion means ordered terminal commands were published, not
    // that this fixture's independent native receiver consumed them already.
    while let Some(command) = try_recv_reliable_path_command(&mut receivers) {
        assert!(
            matches!(
                &command,
                ReliablePathCommand::SendFrame(
                    Frame::StreamAck { .. }
                        | Frame::StreamMaxData { .. }
                        | Frame::StreamFin { .. }
                        | Frame::StreamDetach { .. }
                ) | ReliablePathCommand::CloseStream(_)
            ),
            "only ordered terminal/control work may remain after complete EOF source"
        );
        receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&command));
    }
    assert_eq!(commands.pending_bytes(), 0);
    assert_eq!(commands.writer_pending_bytes(), 0);
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
    let accounting_limit = mux_limits
        .max_repair_bytes
        .min(mux_limits.max_reorder_bytes)
        .min(mux_limits.max_stream_window_bytes as usize);
    let limit = reliable_relay_request_outstanding_limit_bytes(
        TrafficClass::Throughput,
        payload_bytes,
        accounting_limit,
        mux_limits,
    );
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
fn request_source_staging_exhausts_sum_product_window_and_data_ack_reopens_it() {
    let mux_limits = MuxLimits {
        max_stream_window_bytes: 4 * 1024 * 1024,
        max_repair_bytes: 4 * 1024 * 1024,
        max_reorder_bytes: 4 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let product_window = 2 * 1024 * 1024;
    let limit = reliable_relay_request_outstanding_limit_bytes(
        TrafficClass::Throughput,
        64 * 1024,
        product_window,
        mux_limits,
    );
    assert_eq!(limit, product_window);

    let mut send_stream = ReliableSendStream::new(StreamId(91), mux_limits);
    send_stream
        .send_data(Bytes::from(vec![0x51; 512 * 1024]))
        .expect("first retained Product frame");
    send_stream
        .send_data(Bytes::from(vec![0x52; 512 * 1024]))
        .expect("second retained Product frame");
    let mut sender_queue = ReliableRelaySenderQueue::default();
    sender_queue.push_data(Bytes::from(vec![0x53; 1024 * 1024]));
    assert_eq!(
        reliable_relay_request_outstanding_headroom_bytes(&send_stream, &sender_queue, limit,),
        0,
        "retained plus queued unique bytes consume the complete sum(P_i) source envelope",
    );

    let ack = send_stream
        .apply_ack(&[OffsetRange {
            start: 0,
            end: 1024 * 1024,
        }])
        .expect("exact Data ACK");
    assert_eq!(ack.released_bytes, 1024 * 1024);
    assert_eq!(
        reliable_relay_request_outstanding_headroom_bytes(&send_stream, &sender_queue, limit,),
        1024 * 1024,
        "Data ACK release reopens source staging without borrowing another stream's window",
    );
}

#[test]
fn latency_request_outstanding_limit_keeps_the_staging_reservoir() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = 64 * 1024;
    let limit = reliable_relay_request_outstanding_limit_bytes(
        TrafficClass::Latency,
        payload_bytes,
        reliable_relay_buffer_len(mux_limits),
        mux_limits,
    );

    assert_eq!(limit, reliable_relay_buffer_len(mux_limits));
    assert!(limit < mux_limits.max_stream_window_bytes as usize);
}

#[tokio::test]
async fn bulk_request_staging_uses_resource_ceiling_and_bounded_ready_work() {
    let limits = MuxLimits::default();
    assert_eq!(
        reliable_relay_request_outstanding_limit_bytes(
            TrafficClass::Throughput,
            64 * 1024,
            usize::MAX,
            limits,
        ),
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

/// One selected third endpoint uses the existing authenticated TCP test-peer
/// sequence. Initial members remain channel-backed writer producers, as in the
/// ready-ACK actor fixture; no third pending task/result is installed by a test.
#[tokio::test]
async fn first_product_stall_acquires_one_real_tcp_output_without_inventing_replay() {
    for application_holds_reply in [false, true] {
        first_product_stall_acquisition_fixture(application_holds_reply).await;
    }
}

async fn first_product_stall_acquisition_fixture(application_holds_reply: bool) {
    use super::super::client::observe_first_product_stall_for_test;
    use crate::config::ServerSecurityConfig;
    use crate::model::path::RelayPathInstance;
    use crate::mux::stream::ReliableRecvStream;
    use crate::protocol::StreamAttachmentPhase;
    use crate::protocol::codec::CodecLimits;
    use crate::runtime::path::commands::reliable_path_command_pending_bytes;
    use crate::runtime::sender::PreparedOriginalClaim;
    use crate::runtime::stream::arm_client_relay_attachment_commits_for_test;
    use crate::transport::encrypted::EncryptedFramedStream;
    use std::sync::atomic::AtomicUsize;

    struct FixtureTasks {
        context: ClientPathContext,
        aborts: Vec<tokio::task::AbortHandle>,
    }
    impl Drop for FixtureTasks {
        fn drop(&mut self) {
            self.context.retire_session(CloseReason::Normal);
            for task in &self.aborts {
                task.abort();
            }
        }
    }

    // The watchdog is fixture containment, not a selected recovery deadline.
    tokio::time::timeout(Duration::from_secs(10), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap();
        let context = ClientPathContext::new(
            ["tcp://127.0.0.1:9?max-tcp-carriers=1".to_owned(),
             "tcp://127.0.0.1:10?max-tcp-carriers=1".to_owned(),
             format!("tcp://{endpoint}?max-tcp-carriers=1")]
                .into_iter().map(|path| path.parse::<PathSpec>().unwrap()).collect(),
            test_security(), ResourceLimits::default(),
        ).unwrap();
        let mut tasks = FixtureTasks { context: context.clone(), aborts: Vec::new() };
        let stream_id = StreamId(if application_holds_reply { 726 } else { 725 });
        let target = TargetAddr::Ip("127.0.0.1:443".parse().unwrap());
        let limits = context.mux_limits;
        let received_request = Arc::new(Mutex::new((ReliableRecvStream::new(stream_id, limits), Vec::<u8>::new())));
        let request_ready = Arc::new(Notify::new());
        let original_retired = Arc::new(AtomicBool::new(false));
        let mut opened = Vec::new();
        let mut writer_tasks = Vec::new();
        let mut initial_instances = Vec::new();
        for index in 0..2 {
            let (commands, mut receivers) = reliable_path_command_channels(32);
            let (frames_tx, frames_rx) = mpsc::channel(32);
            let member = test_opened_remote_stream(stream_id, index, commands, frames_rx);
            let instance = RelayPathInstance {
                key: RelayPathKey { underlay: UnderlayProtocol::Tcp, index },
                path_instance_id: member.path_instance_id(), attachment_id: index as u64,
            };
            context.install_relay_path_instance_for_test(instance);
            initial_instances.push(instance);
            opened.push(member);
            let received_request = received_request.clone();
            let request_ready = request_ready.clone();
            let retired = original_retired.clone();
            let writer = tokio::spawn(async move {
                while let Some(command) = recv_reliable_path_command(&mut receivers).await {
                    let charged = reliable_path_command_pending_bytes(&command);
                    let frame = match command {
                        ReliablePathCommand::PreparedOriginal(work) => {
                            let ready = receivers.writer_ready_boundary(work.path_instance_id()).unwrap();
                            match work.try_claim(ready) {
                                PreparedOriginalClaim::Claimed(frame) => {
                                    let charged = receivers.register_claimed_writer_frame(&frame);
                                    receivers.release_pending_command_bytes(charged);
                                    work.requeue();
                                    Some(frame)
                                }
                                PreparedOriginalClaim::Blocked(wait) => {
                                    receivers.defer_prepared_work(work, wait);
                                    None
                                }
                                PreparedOriginalClaim::Empty | PreparedOriginalClaim::RecoveryQueued => None,
                                PreparedOriginalClaim::Busy(wait) => {
                                    receivers.defer_prepared_work(work, wait);
                                    None
                                }
                                PreparedOriginalClaim::CarrierFailed(error) => panic!("live fixture writer failed: {error:?}"),
                            }
                        }
                        ReliablePathCommand::SendFrame(frame) => {
                            receivers.release_pending_command_bytes(charged);
                            Some(frame)
                        }
                        ReliablePathCommand::CloseStream(_) | ReliablePathCommand::ResetAndCloseStream { .. } => {
                            retired.store(true, Ordering::Release);
                            receivers.release_pending_command_bytes(charged);
                            None
                        }
                        _ => { receivers.release_pending_command_bytes(charged); None }
                    };
                    match frame {
                        Some(Frame::StreamData { stream_id: actual, offset, payload }) => {
                            assert_eq!(actual, stream_id);
                            let (acks, warm_response) = {
                                let mut request = received_request.lock().unwrap();
                                let before = request.1.len();
                                let outcome = request.0.receive_data(offset, payload).unwrap();
                                for bytes in outcome.delivered { request.1.extend_from_slice(&bytes); }
                                assert!(request.1.len() <= 128);
                                let warm_response = (before < 64 && request.1.len() >= 64)
                                    .then(|| Bytes::copy_from_slice(&request.1[..64]));
                                (request.0.ack_frames(), warm_response)
                            };
                            for ack in acks { frames_tx.send(Ok(ack)).await.unwrap(); }
                            if let Some(payload) = warm_response {
                                frames_tx.send(Ok(Frame::StreamData { stream_id, offset: 0, payload })).await.unwrap();
                            }
                            request_ready.notify_one();
                        }
                        Some(Frame::StreamDetach { .. } | Frame::StreamReset { .. } | Frame::SessionClose { .. }) => { retired.store(true, Ordering::Release); }
                        _ => {}
                    }
                }
            });
            tasks.aborts.push(writer.abort_handle());
            writer_tasks.push(writer);
        }

        let actual_opens = Arc::new(AtomicUsize::new(0));
        let request_replays = Arc::new(AtomicUsize::new(0));
        let release_reply = Arc::new(Notify::new());
        let (reply_held_tx, reply_held_rx) = tokio::sync::oneshot::channel();
        let (response_acked_tx, response_acked_rx) = tokio::sync::oneshot::channel();
        let peer_opens = actual_opens.clone();
        let peer_replays = request_replays.clone();
        let peer_reply = release_reply.clone();
        let peer_request = received_request.clone();
        let peer_target = target.clone();
        let session_id = context.session_id;
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut framed = EncryptedFramedStream::accept(socket,
                &crate::transport::encrypted::test_server_tls_config(), CodecLimits::default()).await?;
            let security = ServerSecurityConfig::for_test(
                SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).unwrap());
            let binding = framed.tcp_admission_binding()?;
            let encoded = framed.read_tcp_admission().await?;
            let authenticated = crate::runtime::path::tcp::admission::authenticate_prelude(
                &security, crate::runtime::path::authentication::ProductCredentialAdmission::from_security(&security),
                &encoded, &binding)?.expect("authenticated test prelude");
            let joined = authenticated.authenticate_path_join(UnderlayProtocol::Tcp, framed.read_frame().await?)?
                .expect("authenticated actual PATH_JOIN");
            assert_eq!(joined.session_id, session_id);
            let path_id = joined.path_id;
            assert!(matches!(framed.read_frame().await?, Frame::PathStatus { path_id: actual, sequence: 0, .. } if actual == path_id));
            framed.write_frames(&[Frame::SessionReady,
                Frame::PathStatus { path_id, sequence: 0, usage: PathUsage::Available }]).await?;
            framed.flush().await?;
            let mut peer_credit = 0;
            let mut open = false;
            let mut sent = false;
            let mut held = Some(reply_held_tx);
            let mut response_acked = Some(response_acked_tx);
            loop {
                if open && peer_credit >= 128 && !sent {
                    if let Some(held) = held.take() { let _ = held.send(()); }
                    if application_holds_reply { peer_reply.notified().await; }
                    {
                        let payload = {
                            let request = peer_request.lock().unwrap();
                            assert_eq!(request.1.len(), 128, "response uses actually received request bytes");
                            Bytes::copy_from_slice(&request.1[64..128])
                        };
                        framed.write_frame(&Frame::StreamData { stream_id, offset: 64, payload }).await?;
                        framed.flush().await?;
                        sent = true;
                    }
                }
                match framed.read_frame().await? {
                        Frame::OpenStream { stream_id: actual, target, return_plan, .. } => {
                            assert!(!open, "one ordinary recovery acquisition");
                            assert_eq!((actual, target), (stream_id, peer_target.clone()));
                            assert_eq!(return_plan.phase, StreamAttachmentPhase::Ordinary);
                            peer_opens.fetch_add(1, Ordering::Release);
                            open = true;
                            framed.write_frame(&Frame::StreamMaxData { stream_id, max_offset: limits.max_stream_window_bytes }).await?;
                            framed.flush().await?;
                        }
                        Frame::StreamMaxData { stream_id: actual, max_offset } => {
                            assert_eq!(actual, stream_id); peer_credit = peer_credit.max(max_offset);
                        }
                        Frame::StreamData { stream_id: actual, .. } => {
                            assert_eq!(actual, stream_id); peer_replays.fetch_add(1, Ordering::Release);
                        }
                        Frame::PathProofData { path_id: actual, proof_id, payload } => {
                            assert_eq!(actual, path_id);
                            framed.write_frame(&Frame::PathProofAck { path_id, proof_id, payload_bytes: u32::try_from(payload.len()).unwrap() }).await?;
                            framed.flush().await?;
                        }
                        Frame::Ping { nonce } => { framed.write_frame(&Frame::Pong { nonce }).await?; framed.flush().await?; }
                        Frame::StreamAck { stream_id: actual, ranges, .. } => {
                            assert_eq!(actual, stream_id);
                            if sent && ranges.iter().any(|range| range.start == 0 && range.end == 128)
                                && let Some(acked) = response_acked.take()
                            { let _ = acked.send(()); }
                        }
                        Frame::SessionClose { .. } => return Ok::<(), RuntimeError>(()),
                        Frame::PathMetrics { .. } | Frame::PathStatus { .. } | Frame::StreamReturnPlanFinal { .. }
                        | Frame::StreamFeedbackProbe { .. } | Frame::StreamFeedbackReceipt { .. } => {}
                        other => panic!("unexpected selected TCP peer input: {other:?}"),
                }
            }
        });
        tasks.aborts.push(peer.abort_handle());
        context.tcp_sessions[2].prepare_connection(tokio::time::Instant::now() + Duration::from_secs(2)).await.unwrap();
        let third_instance = context.tcp_sessions[2].connection_instance_id().expect("actual third carrier is ready");
        assert_eq!(actual_opens.load(Ordering::Acquire), 0, "carrier preparation creates no Product attachment");
        let mut third_committed = arm_client_relay_attachment_commits_for_test(third_instance, stream_id);
        let mut third_settlement = context.arm_reliable_tcp_settlement_test(2);
        let release_second = Arc::new(Notify::new());
        let (second_consumed_tx, second_consumed_rx) = tokio::sync::oneshot::channel();
        let second = opened.pop().unwrap();
        let initial = opened.pop().unwrap();
        let ingress = super::super::lifecycle::BlockedWriteOpenTestIngress {
            startup_ordinal: None,
            insert_pending_task: true,
            key: initial_instances[1].key, obsolete_generation: false, opened: Some(second),
            release: release_second.clone(), consumed: second_consumed_tx,
        };
        let (mut application, local) = duplex(4096);
        let (stall_tx, mut stall_rx) = mpsc::unbounded_channel();
        let relay_context = context.clone();
        let relay = tokio::spawn(observe_first_product_stall_for_test(stream_id, stall_tx, async move {
            relay_migrating_tcp_stream_active(local, &relay_context, MppPerformanceConfig::default(),
                ReliableRelayOpenSpec::new(target, TrafficClass::Latency), initial, None, Some(ingress)).await
        }));
        tasks.aborts.push(relay.abort_handle());
        release_second.notify_one();
        second_consumed_rx.await.expect("actual relay consumed second member setup");
        // Actual warm delivery places progress after setup's Recovery postaction;
        // no test clock or last-attempt mutation creates the first-stall premise.
        application.write_all(&[0x70; 64]).await.unwrap();
        let mut warm_response = [0u8; 64];
        application.read_exact(&mut warm_response).await.unwrap();
        assert_eq!(warm_response, [0x70; 64]);
        application.write_all(&[0x71; 64]).await.unwrap();
        loop {
            if received_request.lock().unwrap().1.len() == 128 { break; }
            request_ready.notified().await;
        }
        let first = stall_rx.recv().await.expect("actual first stall branch completed");
        assert_eq!(first.members, initial_instances, "first stall retains both exact attachment owners");
        assert_eq!((first.assigned, first.retained, first.receive_frontier, first.reorder_bytes), (128, 0, 64, 0),
            "actual request ACK cleared ownership; exact warm delivery established the response frontier");
        assert!(!first.queued_existing_tail, "no retained request authority licenses a replay");
        assert!(first.recovery_open_spawned,
            "observed first Product stall deferred acquisition despite two live non-delivering members and one ready nonmember: {first:?}");
        assert_eq!(first.pending, vec![RelayPathKey { underlay: UnderlayProtocol::Tcp, index: 2 }],
            "ordinary ranking created exactly one new pending open at the first stage");
        third_settlement.wait_reached().await;
        assert_eq!(actual_opens.load(Ordering::Acquire), 1, "ordinary authenticated OPEN reached the selected peer");
        third_settlement.release();
        let attached = third_committed.wait_committed().await;
        assert_eq!((attached.key.index, attached.path_instance_id), (2, third_instance));
        assert!(!original_retired.load(Ordering::Acquire));
        reply_held_rx.await.expect("peer consumed actual response credit");
        if application_holds_reply {
            let mut one = [0u8; 1];
            let mut buffer = ReadBuf::new(&mut one);
            std::future::poll_fn(|cx| {
                assert!(Pin::new(&mut application).poll_read(cx, &mut buffer).is_pending(),
                    "opening a carrier cannot manufacture an application-held reply");
                Poll::Ready(())
            }).await;
            assert_eq!(request_replays.load(Ordering::Acquire), 0);
            release_reply.notify_one();
        }
        let mut response = [0u8; 64];
        application.read_exact(&mut response).await.unwrap();
        assert_eq!(response, [0x71; 64]);
        response_acked_rx.await.expect("actual response ACK reaches the third peer");
        assert_eq!(request_replays.load(Ordering::Acquire), 0,
            "new membership did not replay fully ACKed request bytes");
        assert_eq!(actual_opens.load(Ordering::Acquire), 1);
        assert!(!original_retired.load(Ordering::Acquire), "existing owners remain intact through delivery");
        // The response ACK completed this peer's observation. Session retirement
        // cancels native work and does not promise an orderly TLS close exchange.
        // Join deliberate cancellation before retiring the fixture context;
        // an earlier peer failure must still fail the fixture.
        peer.abort();
        assert!(peer.await.expect_err("scripted peer stays live until explicit cleanup").is_cancelled());
        context.retire_session(CloseReason::Normal);
        relay.abort(); let _ = relay.await;
        for writer in writer_tasks { writer.abort(); let _ = writer.await; }
    }).await.expect("bounded first-stall actor/native-open fixture");
}

#[tokio::test]
async fn response_past_h_continues_while_alternate_startup_is_unfinished() {
    for initial_underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
        response_past_h_with_unfinished_alternate(initial_underlay).await;
    }
}

async fn response_past_h_with_unfinished_alternate(initial_underlay: UnderlayProtocol) {
    const H: usize = 58_400;
    const RESPONSE_BYTES: usize = 100_204;
    let stream_id = StreamId(58_401);
    let context = ClientPathContext::new(
        [
            "tcp://127.0.0.1:10906?max-tcp-carriers=1",
            "quic://127.0.0.1:10907",
        ]
        .into_iter()
        .map(|path| path.parse::<PathSpec>().expect("test path"))
        .collect(),
        test_security(),
        ResourceLimits::default(),
    )
    .expect("client context");
    let alternate_underlay = match initial_underlay {
        UnderlayProtocol::Tcp => UnderlayProtocol::Udp,
        UnderlayProtocol::Udp => UnderlayProtocol::Tcp,
    };
    let (commands, mut receivers) = reliable_path_command_channels(32);
    let (frames_tx, frames_rx) = mpsc::channel(2);
    let initial = test_opened_remote_stream_on(stream_id, 0, initial_underlay, commands, frames_rx);
    let initial_key = RelayPathKey {
        underlay: initial_underlay,
        index: 0,
    };
    let alternate_key = RelayPathKey {
        underlay: alternate_underlay,
        index: 0,
    };
    let plan = Arc::new(
        ReliableRelayReturnPlan::new(
            H as u64,
            PathUsage::Available,
            vec![
                (initial_key, Some(initial.path_instance_id())),
                (alternate_key, None),
            ],
        )
        .expect("two-candidate startup plan"),
    );
    let initial = initial.with_startup(plan, 0, Vec::new());
    let (consumed_tx, _consumed_rx) = tokio::sync::oneshot::channel();
    let ingress = super::super::lifecycle::BlockedWriteOpenTestIngress {
        key: alternate_key,
        obsolete_generation: false,
        startup_ordinal: Some(1),
        insert_pending_task: true,
        opened: None,
        // The alternate operation cannot finish on its own. Installation uses
        // the actual begin_candidate_for_open transition, before actor polling.
        release: Arc::new(Notify::new()),
        consumed: consumed_tx,
    };
    let (server_commands, _server_receivers) = reliable_path_command_channels(8);
    let binding = crate::runtime::stream::response::ResponseStreamBinding::new_with_limits(
        crate::protocol::SessionId(58_401),
        initial_underlay,
        PathId(0),
        server_commands,
        TrafficClass::Latency,
        context.mux_limits,
    );
    binding.install_unresolved_response_startup_for_test(
        H as u64,
        2,
        PathUsage::Available,
        crate::model::path::CarrierPathKey {
            underlay: initial_underlay,
            path_id: PathId(0),
        },
    );
    let mut peer_max = reliable_stream_initial_advertised_window_bytes(
        initial_underlay,
        TrafficClass::Latency,
        context.mux_limits,
    );
    assert!(peer_max >= H as u64);
    assert_eq!(
        binding.response_startup_fresh_data_limit(0, RESPONSE_BYTES),
        Some(H)
    );
    assert_eq!(
        binding.response_startup_fresh_data_limit(H as u64, RESPONSE_BYTES - H),
        None
    );
    let payload = Bytes::from(
        (0..RESPONSE_BYTES)
            .map(|n| (n % 251) as u8)
            .collect::<Vec<_>>(),
    );
    let expected = payload.clone();
    let (local, mut application) = duplex(RESPONSE_BYTES);
    let relay = tokio::spawn(async move {
        relay_migrating_tcp_stream_active(
            local,
            &context,
            MppPerformanceConfig::default(),
            ReliableRelayOpenSpec::new(
                TargetAddr::Ip("127.0.0.1:10908".parse().expect("test target")),
                TrafficClass::Latency,
            ),
            initial,
            None,
            Some(ingress),
        )
        .await
    });
    let producer = tokio::spawn(async move {
        frames_tx
            .send(Ok(Frame::StreamData {
                stream_id,
                offset: 0,
                payload: payload.slice(..H),
            }))
            .await
            .expect("send only the real prefix allowance");
        let mut finalized = false;
        while !finalized || peer_max < RESPONSE_BYTES as u64 {
            let command = recv_reliable_path_command(&mut receivers)
                .await
                .expect("client response control");
            receivers.release_pending_command_bytes(
                crate::runtime::path::commands::reliable_path_command_pending_bytes(&command),
            );
            match command {
                ReliablePathCommand::SendFrame(Frame::StreamReturnPlanFinal {
                    stream_id: id,
                    retained_ordinals,
                }) if id == stream_id => {
                    assert_eq!(
                        retained_ordinals,
                        [0],
                        "the unfinished alternate has no accepted membership"
                    );
                    binding
                        .finalize_response_startup_plan(&retained_ordinals)
                        .expect("apply actual emitted FINAL");
                    finalized = true;
                }
                ReliablePathCommand::SendFrame(Frame::StreamMaxData {
                    stream_id: id,
                    max_offset,
                }) if id == stream_id => peer_max = peer_max.max(max_offset),
                _ => {}
            }
        }
        assert_eq!(
            binding.response_startup_fresh_data_limit(H as u64, RESPONSE_BYTES - H),
            Some(RESPONSE_BYTES - H)
        );
        frames_tx
            .send(Ok(Frame::StreamData {
                stream_id,
                offset: H as u64,
                payload: payload.slice(H..),
            }))
            .await
            .expect("server tail obeys FINAL and receive credit");
        // Keep the attachment input alive until the application verifies its
        // exact response, rather than introducing unrelated stream detachment.
        std::future::pending::<()>().await;
    });
    let mut delivered = vec![0; RESPONSE_BYTES];
    let delivery = tokio::time::timeout(
        Duration::from_secs(2),
        application.read_exact(&mut delivered),
    )
    .await;
    relay.abort();
    if let Err(error) = relay.await {
        assert!(error.is_cancelled(), "client relay panicked: {error}");
    }
    producer.abort();
    let producer_result = producer.await;
    if let Err(error) = producer_result {
        assert!(error.is_cancelled(), "response producer panicked: {error}");
    }
    delivery
        .expect("FINAL must release the healthy response without alternate settlement")
        .expect("application response");
    assert_eq!(
        delivered, expected,
        "no missing or duplicated bytes for initial {initial_underlay:?}"
    );
}

#[tokio::test]
async fn omitted_startup_result_cannot_replace_disconnected_ordinary_generation() {
    let stream_id = StreamId(58_402);
    let initial_key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    let alternate_key = RelayPathKey {
        underlay: UnderlayProtocol::Udp,
        index: 0,
    };
    let (initial_commands, _initial_receivers) = reliable_path_command_channels(16);
    let (_initial_frames, initial_input) = mpsc::channel(1);
    let initial = test_opened_remote_stream(stream_id, 0, initial_commands, initial_input);
    let plan = Arc::new(
        ReliableRelayReturnPlan::new(
            58_400,
            PathUsage::Available,
            vec![
                (initial_key, Some(initial.path_instance_id())),
                (alternate_key, None),
            ],
        )
        .expect("two-candidate plan"),
    );
    let (mut remotes, _remote_input) = ReliableRelayRemoteSet::new(initial, 8);
    let initial_instance = remotes.paths[0].instance();
    let mut startup = ClientReliableReturnPlan::from_initial_open(
        crate::runtime::stream::ReliableRelayOpenedStartup {
            plan,
            opening_ordinal: 0,
            failed_ordinals: Vec::new(),
        },
        initial_instance,
    )
    .expect("client return plan");
    let mut state = ClientRelayState::new();
    let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
    let (tx, mut rx) = mpsc::channel(2);
    let (old_commands, _old_receivers) = reliable_path_command_channels(8);
    let (_old_frames, old_input) = mpsc::channel(1);
    let old_opened =
        test_opened_remote_stream_on(stream_id, 0, UnderlayProtocol::Udp, old_commands, old_input);
    let old_instance = old_opened.path_instance_id();
    let release_old = Arc::new(Notify::new());
    let (old_consumed, _old_consumed_rx) = tokio::sync::oneshot::channel();
    super::super::lifecycle::BlockedWriteOpenTestIngress {
        key: alternate_key,
        obsolete_generation: false,
        startup_ordinal: Some(1),
        insert_pending_task: true,
        opened: Some(old_opened),
        release: release_old.clone(),
        consumed: old_consumed,
    }
    .install(
        &mut startup,
        &mut state.recovery.pending_additional_path_opens,
        tx.clone(),
    );
    release_old.notify_one();
    let old_result = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("old open completes")
        .expect("old result");
    // Its native completion is deliberately retained across the real h
    // transition, before owner attachment settlement.
    drive_client_response_startup_control(
        &mut startup,
        stream_id,
        58_400,
        &mut remotes,
        &mut state,
    )
    .expect("h closes startup ownership");
    assert!(state.recovery.pending_additional_path_opens.is_empty());
    drop(
        remotes
            .remove_path_instance(initial_instance)
            .expect("current carrier disconnects"),
    );
    assert!(remotes.is_empty());

    let (new_commands, _new_receivers) = reliable_path_command_channels(8);
    let (_new_frames, new_input) = mpsc::channel(1);
    let new_opened =
        test_opened_remote_stream_on(stream_id, 0, UnderlayProtocol::Udp, new_commands, new_input);
    let new_instance = new_opened.path_instance_id();
    assert_ne!(old_instance, new_instance);
    let release_new = Arc::new(Notify::new());
    let (new_consumed, _new_consumed_rx) = tokio::sync::oneshot::channel();
    super::super::lifecycle::BlockedWriteOpenTestIngress {
        key: alternate_key,
        obsolete_generation: false,
        startup_ordinal: None,
        insert_pending_task: true,
        opened: Some(new_opened),
        release: release_new.clone(),
        consumed: new_consumed,
    }
    .install(
        &mut startup,
        &mut state.recovery.pending_additional_path_opens,
        tx,
    );
    assert_eq!(state.recovery.pending_additional_path_opens.len(), 1);
    assert!(
        settle_matching_client_additional_path_open(
            stream_id,
            &mut state,
            &mut startup,
            &mut remotes,
            &mut send_stream,
            TrafficClass::Latency,
            old_result,
            true,
        )
        .expect("old result is operation-local")
        .is_none()
    );
    assert!(
        remotes.is_empty(),
        "disconnection cannot admit the omitted generation"
    );
    assert_eq!(
        state.recovery.pending_additional_path_opens.len(),
        1,
        "old result cannot consume ordinary replacement"
    );

    release_new.notify_one();
    let new_result = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("ordinary open completes")
        .expect("ordinary result");
    assert!(matches!(
        settle_matching_client_additional_path_open(
            stream_id,
            &mut state,
            &mut startup,
            &mut remotes,
            &mut send_stream,
            TrafficClass::Latency,
            new_result,
            true,
        )
        .expect("ordinary owner commits"),
        Some(ReliableRelayAttachMode::Recovery)
    ));
    assert!(state.recovery.pending_additional_path_opens.is_empty());
    assert_eq!(
        remotes
            .path_instance_for_key(alternate_key)
            .expect("ordinary attachment")
            .path_instance_id,
        new_instance
    );
}
