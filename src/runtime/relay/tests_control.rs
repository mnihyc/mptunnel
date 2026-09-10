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
        if observation.open_started
            && observation.ack_frontier >= trigger_bytes
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
        restart_reset_during_blocked_product_write(obsolete_generation).await;
    }
}

async fn restart_reset_during_blocked_product_write(obsolete_generation: bool) {
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
        obsolete_generation,
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
        "terminal reset survives blocked output and obsolete generation={obsolete_generation}"
    );
    assert_eq!(control.accepted_bytes(), b"b");
}

#[tokio::test]
async fn response_startup_ack_open_and_final_progress_during_blocked_local_delivery() {
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
    assert_eq!(open_rounds_after_release, 1, "one frozen OPEN round");
    assert_eq!(
        observation.ack_frontier, STARTUP_TRIGGER_BYTES as u64,
        "the exact injected Product range must produce exact Data ACK [0,h)"
    );
    assert_eq!(
        observation.final_retained_ordinals,
        Some(vec![0]),
        "controlled failed ordinal 1 must produce exact FINAL [0]"
    );
    assert!(
        ack_frontier_while_blocked >= STARTUP_TRIGGER_BYTES as u64
            && open_while_blocked
            && final_while_blocked == Some(vec![0]),
        "SEEN-4: contiguous frontier h={STARTUP_TRIGGER_BYTES} reached the blocked Product sink, but independent progress stalled (ACK frontier={ack_frontier_while_blocked}, OPEN={open_while_blocked}, FINAL={final_while_blocked:?}); all three appeared only after local delivery release"
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
            path_snapshot: None,
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
            path_snapshot: None,
            relay_lane: TrafficClass::Throughput,
        },
        stream_id,
        Some(0),
        vec![OffsetRange { start: 0, end: 8 }],
    )
    .expect("exact assigned ACK commits");
    assert_eq!(released, 8);
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
                        PreparedOriginalClaim::Empty => {
                            // An exhausted or superseded weak notice owns no bytes.
                        }
                        PreparedOriginalClaim::Busy(_) | PreparedOriginalClaim::Blocked(_) => {
                            panic!("the sole ready healthy writer must claim admitted EOF source");
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
