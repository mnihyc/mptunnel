use super::reliable_path::ReliablePathStream;
use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

const RELIABLE_PATH_PRIORITY_HEADROOM_LANES: [FlowLane; 4] = [
    FlowLane::Control,
    FlowLane::Latency,
    FlowLane::RealtimeDatagram,
    FlowLane::Background,
];

#[derive(Clone)]
pub(super) struct ReliablePathCommandSender {
    control: mpsc::Sender<ReliablePathCommand>,
    priority: mpsc::Sender<ReliablePathCommand>,
    data: mpsc::Sender<ReliablePathCommand>,
    metrics: Arc<ReliablePathCommandQueueMetrics>,
}

pub(super) struct ReliablePathCommandReceivers {
    control: mpsc::Receiver<ReliablePathCommand>,
    priority: mpsc::Receiver<ReliablePathCommand>,
    data: mpsc::Receiver<ReliablePathCommand>,
    metrics: Arc<ReliablePathCommandQueueMetrics>,
}

#[derive(Default)]
struct ReliablePathCommandQueueMetrics {
    pending_bytes: AtomicU64,
    capacity_released: Arc<Notify>,
}

impl ReliablePathCommandQueueMetrics {
    fn add_pending_bytes(&self, bytes: usize) {
        self.pending_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    fn release_pending_bytes(&self, bytes: usize) {
        let bytes = bytes as u64;
        let _ = self
            .pending_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(bytes))
            });
        self.capacity_released.notify_waiters();
    }

    #[cfg_attr(not(any(test, feature = "lab-diagnostics")), allow(dead_code))]
    fn pending_bytes(&self) -> u64 {
        self.pending_bytes.load(Ordering::Relaxed)
    }
}

impl ReliablePathCommandReceivers {
    pub(super) fn release_pending_command_bytes(&self, bytes: usize) {
        self.metrics.release_pending_bytes(bytes);
    }

    #[cfg(feature = "lab-diagnostics")]
    pub(super) fn pending_bytes(&self) -> u64 {
        self.metrics.pending_bytes()
    }
}

impl ReliablePathCommandSender {
    pub(super) async fn send_control(
        &self,
        command: ReliablePathCommand,
    ) -> Result<(), mpsc::error::SendError<ReliablePathCommand>> {
        #[cfg(feature = "lab-diagnostics")]
        let command_kind = reliable_path_command_kind(&command);
        #[cfg(feature = "lab-diagnostics")]
        let stream_id = reliable_path_command_stream_id(&command);
        #[cfg(feature = "lab-diagnostics")]
        let started = Instant::now();
        let result = self.control.send(command).await;
        #[cfg(feature = "lab-diagnostics")]
        {
            let elapsed = started.elapsed();
            lab_perf_record("runtime.path_queue.control_send", elapsed, 0);
            lab_diagnostic(
                "path_command_queue_send",
                format_args!(
                    "queue=control command_kind={} stream_id={} pacing_bytes=0 wait_ms={} result={}",
                    command_kind,
                    stream_id.0,
                    elapsed.as_millis(),
                    if result.is_ok() { "queued" } else { "closed" },
                ),
            );
        }
        result
    }

    pub(super) async fn send_stream_ordered_close(
        &self,
        stream_id: StreamId,
        _lane: FlowLane,
    ) -> Result<(), mpsc::error::SendError<ReliablePathCommand>> {
        let command = ReliablePathCommand::CloseStream(stream_id);
        #[cfg(feature = "lab-diagnostics")]
        let ordered_lane = reliable_path_stream_ordered_queue_lane();
        let queue = &self.data;
        #[cfg(feature = "lab-diagnostics")]
        let started = Instant::now();
        let result = queue.send(command).await;
        #[cfg(feature = "lab-diagnostics")]
        {
            let elapsed = started.elapsed();
            lab_diagnostic(
                "path_command_queue_send",
                format_args!(
                    "queue=data command_kind=close_stream stream_id={} lane={:?} effective_lane={:?} pacing_bytes=0 wait_ms={} result={}",
                    stream_id.0,
                    _lane,
                    ordered_lane,
                    elapsed.as_millis(),
                    if result.is_ok() { "queued" } else { "closed" },
                ),
            );
        }
        result
    }

    pub(super) fn try_enqueue_admitted_frame(
        &self,
        frame: Frame,
        lane: FlowLane,
    ) -> Result<(), RuntimeError> {
        self.try_enqueue_admitted_frame_with_effective_lane(frame, lane, None)
    }

    pub(super) fn try_enqueue_stream_ordered_frame(
        &self,
        frame: Frame,
        lane: FlowLane,
    ) -> Result<(), RuntimeError> {
        self.try_enqueue_admitted_frame_with_effective_lane(
            frame,
            lane,
            Some(reliable_path_stream_ordered_queue_lane()),
        )
    }

    fn try_enqueue_admitted_frame_with_effective_lane(
        &self,
        frame: Frame,
        lane: FlowLane,
        effective_lane_override: Option<FlowLane>,
    ) -> Result<(), RuntimeError> {
        let bytes = frame_pacing_bytes(&frame);
        let effective_lane = effective_lane_override
            .unwrap_or_else(|| reliable_path_effective_frame_lane(&frame, lane));
        #[cfg(feature = "lab-diagnostics")]
        let frame_kind = reliable_path_frame_kind(&frame);
        #[cfg(feature = "lab-diagnostics")]
        let stream_id = reliable_path_frame_stream_id(&frame);
        let queue = if reliable_path_frame_uses_priority_queue(effective_lane) {
            &self.priority
        } else {
            &self.data
        };
        let result = match queue.try_reserve() {
            Ok(permit) => {
                self.metrics.add_pending_bytes(bytes);
                permit.send(ReliablePathCommand::SendFrame(frame));
                Ok(())
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                Err(RuntimeError::SenderServiceBlocked)
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                Err(RuntimeError::ReliablePathSessionClosed)
            }
        };
        #[cfg(feature = "lab-diagnostics")]
        {
            let queue_name = if reliable_path_frame_uses_priority_queue(effective_lane) {
                "priority"
            } else {
                "data"
            };
            lab_diagnostic(
                "path_command_queue_send",
                format_args!(
                    "queue={} frame_kind={} stream_id={} lane={:?} effective_lane={:?} pacing_bytes={} wait_ms=0 result={}",
                    queue_name,
                    frame_kind,
                    stream_id.0,
                    lane,
                    effective_lane,
                    bytes,
                    match &result {
                        Ok(()) => "queued",
                        Err(RuntimeError::SenderServiceBlocked) => "blocked",
                        Err(_) => "closed",
                    },
                ),
            );
        }
        result
    }

    pub(super) fn can_enqueue_frame_now(&self, frame: &Frame, lane: FlowLane) -> bool {
        let effective_lane = reliable_path_effective_frame_lane(frame, lane);
        self.can_enqueue_lane_now(effective_lane)
    }

    pub(super) fn can_enqueue_stream_ordered_frame_now(&self, lane: FlowLane) -> bool {
        let _ = lane;
        self.can_enqueue_lane_now(reliable_path_stream_ordered_queue_lane())
    }

    pub(super) fn can_enqueue_lane_now(&self, lane: FlowLane) -> bool {
        if reliable_path_frame_uses_priority_queue(lane) {
            self.priority.capacity() > 0
        } else {
            self.data.capacity() > 0
        }
    }

    pub(super) fn capacity_notify(&self) -> Arc<Notify> {
        self.metrics.capacity_released.clone()
    }

    #[cfg_attr(not(any(test, feature = "lab-diagnostics")), allow(dead_code))]
    pub(super) fn pending_bytes(&self) -> u64 {
        self.metrics.pending_bytes()
    }

    pub(super) fn is_closed(&self) -> bool {
        self.control.is_closed() && self.priority.is_closed() && self.data.is_closed()
    }

    pub(super) fn same_channel(&self, other: &Self) -> bool {
        self.control.same_channel(&other.control)
            && self.priority.same_channel(&other.priority)
            && self.data.same_channel(&other.data)
    }
}

pub(super) fn reliable_path_frame_uses_priority_queue(lane: FlowLane) -> bool {
    matches!(
        lane,
        FlowLane::Control | FlowLane::Latency | FlowLane::RealtimeDatagram
    )
}

pub(super) fn reliable_path_stream_ordered_queue_lane() -> FlowLane {
    FlowLane::Throughput
}

pub(super) fn reliable_path_effective_frame_lane(frame: &Frame, stream_lane: FlowLane) -> FlowLane {
    match frame {
        Frame::StreamData { .. } => stream_lane,
        Frame::DatagramData { .. } | Frame::DatagramFeedback { .. } => FlowLane::RealtimeDatagram,
        _ => FlowLane::Control,
    }
}

pub(super) fn reliable_path_command_channels(
    queue: usize,
) -> (ReliablePathCommandSender, ReliablePathCommandReceivers) {
    let queue = queue.max(1);
    let (control_tx, control_rx) = mpsc::channel(queue);
    let (priority_tx, priority_rx) = mpsc::channel(queue);
    let (data_tx, data_rx) = mpsc::channel(queue);
    let metrics = Arc::new(ReliablePathCommandQueueMetrics::default());
    (
        ReliablePathCommandSender {
            control: control_tx,
            priority: priority_tx,
            data: data_tx,
            metrics: metrics.clone(),
        },
        ReliablePathCommandReceivers {
            control: control_rx,
            priority: priority_rx,
            data: data_rx,
            metrics,
        },
    )
}

fn path_command_receiver_may_recv<T>(receiver: &mpsc::Receiver<T>) -> bool {
    !receiver.is_closed() || !receiver.is_empty()
}

pub(super) fn reliable_path_receivers_closed(receivers: &ReliablePathCommandReceivers) -> bool {
    !path_command_receiver_may_recv(&receivers.control)
        && !path_command_receiver_may_recv(&receivers.priority)
        && !path_command_receiver_may_recv(&receivers.data)
}

pub(super) async fn recv_reliable_path_command(
    receivers: &mut ReliablePathCommandReceivers,
) -> Option<ReliablePathCommand> {
    if let Some(command) = recv_ready_priority_command(receivers) {
        return Some(command);
    }
    let control_may_recv = path_command_receiver_may_recv(&receivers.control);
    let priority_may_recv = path_command_receiver_may_recv(&receivers.priority);
    let data_may_recv = path_command_receiver_may_recv(&receivers.data);
    match (control_may_recv, priority_may_recv, data_may_recv) {
        (true, true, true) => {
            tokio::select! {
                biased;
                command = receivers.control.recv() => command,
                command = receivers.priority.recv() => command,
                command = receivers.data.recv() => command,
            }
        }
        (true, true, false) => {
            tokio::select! {
                biased;
                command = receivers.control.recv() => command,
                command = receivers.priority.recv() => command,
            }
        }
        (true, false, true) => {
            tokio::select! {
                biased;
                command = receivers.control.recv() => command,
                command = receivers.data.recv() => command,
            }
        }
        (false, true, true) => {
            tokio::select! {
                biased;
                command = receivers.priority.recv() => command,
                command = receivers.data.recv() => command,
            }
        }
        (true, false, false) => receivers.control.recv().await,
        (false, true, false) => receivers.priority.recv().await,
        (false, false, true) => receivers.data.recv().await,
        (false, false, false) => None,
    }
}

pub(super) fn try_recv_reliable_path_command(
    receivers: &mut ReliablePathCommandReceivers,
) -> Option<ReliablePathCommand> {
    recv_ready_priority_command(receivers).or_else(|| receivers.data.try_recv().ok())
}

pub(super) fn try_recv_reliable_path_priority_command(
    receivers: &mut ReliablePathCommandReceivers,
) -> Option<ReliablePathCommand> {
    recv_ready_priority_command(receivers)
}

pub(super) fn reliable_path_writer_should_coalesce_partial_bulk_run(
    sent_items: usize,
    sent_bytes: usize,
    byte_budget: usize,
    item_budget: usize,
) -> bool {
    sent_items > 0 && sent_bytes > 0 && sent_bytes < byte_budget && sent_items < item_budget
}

pub(super) async fn try_coalesce_reliable_path_writer_run(
    receivers: &mut ReliablePathCommandReceivers,
    next_command: &mut Option<ReliablePathCommand>,
    sent_items: usize,
    sent_bytes: usize,
    byte_budget: usize,
    item_budget: usize,
) -> bool {
    if !reliable_path_writer_should_coalesce_partial_bulk_run(
        sent_items,
        sent_bytes,
        byte_budget,
        item_budget,
    ) {
        return false;
    }
    tokio::task::yield_now().await;
    if let Some(command) = try_recv_reliable_path_command(receivers) {
        *next_command = Some(command);
        return true;
    }
    false
}

pub(super) fn reliable_path_command_writer_run_budget_bytes(mux_limits: MuxLimits) -> usize {
    let frame_payload =
        reliable_relay_scheduler_quantum_cap(None, FlowLane::Throughput, mux_limits)
            .min(mux_limits.max_reliable_relay_chunk_bytes)
            .min(mux_limits.max_payload_bytes)
            .max(1);
    let stream_data_frame_overhead = crate::protocol::codec::FRAME_HEADER_LEN
        .saturating_add(8) // stream_id
        .saturating_add(8) // offset
        .saturating_add(1) // stream flags
        .saturating_add(4); // payload length
    let encoded_frame_bytes = frame_payload
        .saturating_add(stream_data_frame_overhead)
        .max(1);
    let frames_per_record = mux_limits
        .max_payload_bytes
        .checked_div(encoded_frame_bytes)
        .unwrap_or(0)
        .max(1);
    frame_payload
        .saturating_mul(frames_per_record)
        .max(reliable_relay_buffer_len(mux_limits))
        .max(1)
}

pub(super) fn reliable_path_command_writer_run_budget_items(mux_limits: MuxLimits) -> usize {
    reliable_path_command_queue(mux_limits).max(1)
}

pub(super) fn reliable_path_command_queue(mux_limits: MuxLimits) -> usize {
    let frame_payload = reliable_path_command_queue_payload(mux_limits);
    reliable_path_command_queue_for_payload(mux_limits, frame_payload)
}

pub(super) fn reliable_path_command_queue_for_payload(
    mux_limits: MuxLimits,
    frame_payload_bytes: usize,
) -> usize {
    let frame_payload = frame_payload_bytes.min(mux_limits.max_payload_bytes).max(1);
    let priority_headroom = reliable_path_priority_headroom_frames();
    let inflight_frames = mux_limits
        .max_path_flight_bytes
        .saturating_add(frame_payload - 1)
        / frame_payload;
    inflight_frames
        .saturating_add(priority_headroom)
        .max(priority_headroom)
        .min(reliable_path_writer_frame_queue_for_payload(
            mux_limits,
            frame_payload,
        ))
}

pub(super) fn reliable_path_writer_frame_queue(mux_limits: MuxLimits) -> usize {
    let frame_payload = reliable_path_command_queue_payload(mux_limits);
    reliable_path_writer_frame_queue_for_payload(mux_limits, frame_payload)
}

fn reliable_path_command_queue_payload(mux_limits: MuxLimits) -> usize {
    reliable_relay_scheduler_quantum_cap(None, FlowLane::Throughput, mux_limits)
        .min(mux_limits.max_reliable_relay_chunk_bytes)
        .min(mux_limits.max_payload_bytes)
        .max(1)
}

pub(super) fn reliable_path_writer_frame_queue_for_payload(
    mux_limits: MuxLimits,
    frame_payload_bytes: usize,
) -> usize {
    reliable_stream_frame_queue_for_payload(mux_limits, frame_payload_bytes)
        .saturating_mul(reliable_path_writer_lane_count())
        .max(reliable_path_writer_lane_count())
}

pub(super) fn reliable_stream_frame_queue(mux_limits: MuxLimits) -> usize {
    let frame_payload = mux_limits
        .max_reliable_relay_chunk_bytes
        .min(mux_limits.max_payload_bytes)
        .max(1);
    reliable_stream_frame_queue_for_payload(mux_limits, frame_payload)
}

pub(super) fn reliable_stream_frame_queue_for_payload(
    mux_limits: MuxLimits,
    frame_payload_bytes: usize,
) -> usize {
    let frame_payload = frame_payload_bytes.min(mux_limits.max_payload_bytes).max(1);
    let priority_headroom = reliable_path_priority_headroom_frames();
    (mux_limits.max_reorder_bytes / frame_payload)
        .saturating_add(priority_headroom)
        .max(priority_headroom)
}

pub(super) fn reliable_path_priority_headroom_frames() -> usize {
    RELIABLE_PATH_PRIORITY_HEADROOM_LANES.len()
}

fn reliable_path_writer_lane_count() -> usize {
    RELIABLE_PATH_PRIORITY_HEADROOM_LANES
        .len()
        .saturating_add(1)
}

fn recv_ready_priority_command(
    receivers: &mut ReliablePathCommandReceivers,
) -> Option<ReliablePathCommand> {
    if let Ok(command) = receivers.control.try_recv() {
        return Some(command);
    }
    receivers.priority.try_recv().ok()
}

pub(super) fn reliable_path_command_pending_bytes(command: &ReliablePathCommand) -> usize {
    match command {
        ReliablePathCommand::SendFrame(frame) => frame_pacing_bytes(frame),
        ReliablePathCommand::OpenStream { .. } | ReliablePathCommand::CloseStream(_) => 0,
    }
}

#[cfg(feature = "lab-diagnostics")]
fn reliable_path_command_stream_id(command: &ReliablePathCommand) -> StreamId {
    match command {
        ReliablePathCommand::SendFrame(frame) => reliable_path_frame_stream_id(frame),
        ReliablePathCommand::OpenStream { stream_id, .. }
        | ReliablePathCommand::CloseStream(stream_id) => *stream_id,
    }
}

#[cfg(feature = "lab-diagnostics")]
fn reliable_path_frame_stream_id(frame: &Frame) -> StreamId {
    match frame {
        Frame::OpenStream { stream_id, .. }
        | Frame::StreamData { stream_id, .. }
        | Frame::StreamAck { stream_id, .. }
        | Frame::StreamMaxData { stream_id, .. }
        | Frame::StreamFin { stream_id, .. }
        | Frame::StreamDetach { stream_id }
        | Frame::StreamReset { stream_id, .. } => *stream_id,
        _ => StreamId(0),
    }
}

pub(super) enum ReliablePathCommand {
    OpenStream {
        stream_id: StreamId,
        target: TargetAddr,
        ingress: IngressKind,
        lane: FlowLane,
        role: StreamOpenRole,
        session_commands: ReliablePathCommandSender,
        response: oneshot::Sender<Result<ReliablePathStream, RuntimeError>>,
    },
    SendFrame(Frame),
    CloseStream(StreamId),
}

#[cfg(feature = "lab-diagnostics")]
fn reliable_path_command_kind(command: &ReliablePathCommand) -> &'static str {
    match command {
        ReliablePathCommand::OpenStream { .. } => "open_stream",
        ReliablePathCommand::SendFrame(frame) => reliable_path_frame_kind(frame),
        ReliablePathCommand::CloseStream(_) => "close_stream",
    }
}

#[cfg(feature = "lab-diagnostics")]
fn reliable_path_frame_kind(frame: &Frame) -> &'static str {
    match frame {
        Frame::SessionHello { .. } => "session_hello",
        Frame::SessionAuth { .. } => "session_auth",
        Frame::SessionReady => "session_ready",
        Frame::SessionClose { .. } => "session_close",
        Frame::PathJoin { .. } => "path_join",
        Frame::PathJoinOk { .. } => "path_join_ok",
        Frame::PathChallenge { .. } => "path_challenge",
        Frame::PathResponse { .. } => "path_response",
        Frame::PathStatus { .. } => "path_status",
        Frame::PathDrain { .. } => "path_drain",
        Frame::PathClose { .. } => "path_close",
        Frame::PathMtuProbe { .. } => "path_mtu_probe",
        Frame::PathMtuAck { .. } => "path_mtu_ack",
        Frame::PathProofData { .. } => "path_proof_data",
        Frame::PathProofAck { .. } => "path_proof_ack",
        Frame::OpenStream { .. } => "open_stream",
        Frame::StreamData { .. } => "stream_data",
        Frame::StreamAck { .. } => "stream_ack",
        Frame::StreamMaxData { .. } => "stream_max_data",
        Frame::StreamFin { .. } => "stream_fin",
        Frame::StreamDetach { .. } => "stream_detach",
        Frame::StreamReset { .. } => "stream_reset",
        Frame::OpenDatagramFlow { .. } => "open_dgram_flow",
        Frame::DatagramData { .. } => "datagram_data",
        Frame::DatagramClose { .. } => "datagram_close",
        Frame::DatagramFeedback { .. } => "datagram_feedback",
        Frame::PathMetrics { .. } => "path_metrics",
        Frame::RxRateHint { .. } => "rx_rate_hint",
        Frame::MaxConnectionData { .. } => "max_connection_data",
        Frame::Ping { .. } => "ping",
        Frame::Pong { .. } => "pong",
    }
}
