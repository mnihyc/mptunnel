use super::*;
use crate::protocol::frame::reliable_path_frame_pacing_bytes;
use crate::runtime::stream::ReliablePathStream;
use crate::runtime::stream::response::{ServerCarrierPathInstanceId, TcpCapacityProbeSessionLease};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};

// Bounded, lane-separated handoff from carrier-neutral scheduling to TCP or
// QUIC writers. Control keeps independent capacity; a typed carrier probe remains
// one data-lane command so its token and frozen proof contract cannot split.

const RELIABLE_PATH_PRIORITY_HEADROOM_LANES: [FlowLane; 4] = [
    FlowLane::Control,
    FlowLane::Latency,
    FlowLane::RealtimeDatagram,
    FlowLane::Background,
];

#[derive(Clone)]
pub(in crate::runtime) struct ReliablePathCommandSender {
    control: mpsc::Sender<QueuedReliablePathCommand>,
    priority: mpsc::Sender<QueuedReliablePathCommand>,
    data: mpsc::Sender<QueuedReliablePathCommand>,
    metrics: Arc<ReliablePathCommandQueueMetrics>,
}

pub(in crate::runtime) struct ReliablePathCommandReceivers {
    control: mpsc::Receiver<QueuedReliablePathCommand>,
    priority: mpsc::Receiver<QueuedReliablePathCommand>,
    data: mpsc::Receiver<QueuedReliablePathCommand>,
    metrics: Arc<ReliablePathCommandQueueMetrics>,
    dequeued_unreleased_bytes: AtomicU64,
}

#[derive(Debug, Default)]
struct ReliablePathCommandQueueMetrics {
    pending_bytes: AtomicU64,
    capacity_released: Arc<Notify>,
    tcp_capacity_probe: TcpCapacityProbeLeaseState,
}

#[derive(Debug, Default)]
struct TcpCapacityProbeLeaseState {
    active: AtomicBool,
    attempts: AtomicU8,
}

#[derive(Debug)]
pub(in crate::runtime) struct TcpCapacityProbeLease {
    state: Arc<ReliablePathCommandQueueMetrics>,
}

impl Drop for TcpCapacityProbeLease {
    fn drop(&mut self) {
        self.state
            .tcp_capacity_probe
            .active
            .store(false, Ordering::Release);
        self.state.capacity_released.notify_waiters();
    }
}

struct QueuedReliablePathCommand {
    command: Option<ReliablePathCommand>,
    accounted_bytes: usize,
    metrics: Arc<ReliablePathCommandQueueMetrics>,
}

impl QueuedReliablePathCommand {
    fn new(
        command: ReliablePathCommand,
        accounted_bytes: usize,
        metrics: Arc<ReliablePathCommandQueueMetrics>,
    ) -> Self {
        Self {
            command: Some(command),
            accounted_bytes,
            metrics,
        }
    }

    fn into_parts(mut self) -> (ReliablePathCommand, usize) {
        // Dequeue transfers the byte charge to the receiver. It remains visible
        // until the writer hands the command to carrier/product accounting.
        let accounted_bytes = self.accounted_bytes;
        self.accounted_bytes = 0;
        (
            self.command.take().expect("queued reliable path command"),
            accounted_bytes,
        )
    }

    fn into_rejected_command(mut self) -> ReliablePathCommand {
        if self.accounted_bytes > 0 {
            self.metrics.release_pending_bytes(self.accounted_bytes);
        }
        self.accounted_bytes = 0;
        self.command.take().expect("queued reliable path command")
    }
}

impl Drop for QueuedReliablePathCommand {
    fn drop(&mut self) {
        if let Some(ReliablePathCommand::SendQuicCapacityProbe(probe)) = self.command.as_ref() {
            probe.ticket.cancel();
        }
        if self.accounted_bytes > 0 {
            self.metrics.release_pending_bytes(self.accounted_bytes);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum QuicCapacityProbeOwner {
    /// Client request discovery is scoped to the exact logical attachment.
    Request {
        stream_id: StreamId,
        path_instance: RelayPathInstance,
    },
    /// Server response discovery is scoped to the response binding instance.
    Response {
        binding_instance_id: u64,
        path_instance_id: ServerCarrierPathInstanceId,
    },
}

impl ReliablePathCommandQueueMetrics {
    fn add_pending_bytes(&self, bytes: usize) {
        self.pending_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    fn release_pending_bytes(&self, bytes: usize) {
        self.release_pending_bytes_u64(bytes as u64);
    }

    fn release_pending_bytes_u64(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
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

    fn try_reserve_tcp_capacity_probe(
        self: &Arc<Self>,
    ) -> Result<TcpCapacityProbeLease, RuntimeError> {
        if self
            .tcp_capacity_probe
            .active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(RuntimeError::SenderServiceBlocked);
        }
        let attempt = self.tcp_capacity_probe.attempts.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |attempts| (attempts == 0).then_some(1),
        );
        if attempt.is_err() {
            self.tcp_capacity_probe
                .active
                .store(false, Ordering::Release);
            return Err(RuntimeError::SenderServiceBlocked);
        }
        Ok(TcpCapacityProbeLease {
            state: Arc::clone(self),
        })
    }
}

impl ReliablePathCommandReceivers {
    fn take_queued_command(&self, command: QueuedReliablePathCommand) -> ReliablePathCommand {
        let (command, accounted_bytes) = command.into_parts();
        self.dequeued_unreleased_bytes
            .fetch_add(accounted_bytes as u64, Ordering::Relaxed);
        command
    }

    pub(in crate::runtime) fn release_pending_command_bytes(&self, bytes: usize) {
        let requested = bytes as u64;
        let previous = self
            .dequeued_unreleased_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(requested))
            })
            .expect("dequeued byte update always succeeds");
        self.metrics
            .release_pending_bytes_u64(previous.min(requested));
    }

    #[cfg(feature = "lab-diagnostics")]
    pub(in crate::runtime) fn pending_bytes(&self) -> u64 {
        self.metrics.pending_bytes()
    }
}

impl Drop for ReliablePathCommandReceivers {
    fn drop(&mut self) {
        // Queued envelopes reconcile themselves. This covers a command already
        // removed from mpsc when a writer exits through an async error path.
        let outstanding = self.dequeued_unreleased_bytes.swap(0, Ordering::Relaxed);
        self.metrics.release_pending_bytes_u64(outstanding);
    }
}

impl ReliablePathCommandSender {
    pub(in crate::runtime) async fn send_control(
        &self,
        command: ReliablePathCommand,
    ) -> Result<(), mpsc::error::SendError<ReliablePathCommand>> {
        #[cfg(feature = "lab-diagnostics")]
        let command_kind = reliable_path_command_kind(&command);
        #[cfg(feature = "lab-diagnostics")]
        let stream_id = reliable_path_command_stream_id(&command);
        #[cfg(feature = "lab-diagnostics")]
        let started = Instant::now();
        let result = self
            .control
            .send(QueuedReliablePathCommand::new(
                command,
                0,
                self.metrics.clone(),
            ))
            .await
            .map_err(|err| mpsc::error::SendError(err.0.into_rejected_command()));
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

    pub(in crate::runtime) async fn send_stream_ordered_close(
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
        let result = queue
            .send(QueuedReliablePathCommand::new(
                command,
                0,
                self.metrics.clone(),
            ))
            .await
            .map_err(|err| mpsc::error::SendError(err.0.into_rejected_command()));
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

    pub(in crate::runtime) fn try_enqueue_admitted_frame(
        &self,
        frame: Frame,
        lane: FlowLane,
    ) -> Result<(), RuntimeError> {
        self.try_enqueue_admitted_frame_with_effective_lane(frame, lane, None)
    }

    /// Admits one complete QUIC capacity epoch as one queue item.
    ///
    /// Keeping the frozen proof contract beside the train prevents ordinary
    /// frame batching from losing its token or mixing product data into it.
    pub(in crate::runtime) fn try_enqueue_quic_capacity_probe(
        &self,
        probe: QuicCapacityProbeCommand,
    ) -> Result<(), RuntimeError> {
        let pending_bytes = usize::try_from(probe.train_payload_bytes).unwrap_or(usize::MAX);
        #[cfg(feature = "lab-diagnostics")]
        let calibration_id = probe.calibration_id;
        let result = match self.data.try_reserve() {
            Ok(permit) => {
                self.metrics.add_pending_bytes(pending_bytes);
                permit.send(QueuedReliablePathCommand::new(
                    ReliablePathCommand::SendQuicCapacityProbe(probe),
                    pending_bytes,
                    self.metrics.clone(),
                ));
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
        lab_diagnostic(
            "path_command_queue_send",
            format_args!(
                "queue=data command_kind=quic_capacity_probe stream_id=0 lane={:?} effective_lane={:?} pacing_bytes={} wait_ms=0 result={} calibration_id={}",
                FlowLane::Throughput,
                FlowLane::Throughput,
                pending_bytes,
                match &result {
                    Ok(()) => "queued",
                    Err(RuntimeError::SenderServiceBlocked) => "blocked",
                    Err(_) => "closed",
                },
                calibration_id,
            ),
        );
        result
    }

    /// Reserves one offset-free capacity epoch for this exact TCP carrier.
    pub(in crate::runtime) fn try_enqueue_tcp_capacity_probe(
        &self,
        request: TcpCapacityProbeRequest,
        session_lease: TcpCapacityProbeSessionLease,
    ) -> Result<u64, RuntimeError> {
        let permit = match self.data.try_reserve() {
            Ok(permit) => permit,
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                return Err(RuntimeError::SenderServiceBlocked);
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                return Err(RuntimeError::ReliablePathSessionClosed);
            }
        };
        // Only admitted queue ownership spends this exact-carrier attempt.
        let lease = self.metrics.try_reserve_tcp_capacity_probe()?;
        let pending_bytes = usize::try_from(request.train_payload_bytes).unwrap_or(usize::MAX);
        let calibration_id = NEXT_TCP_CAPACITY_CALIBRATION_ID.fetch_add(1, Ordering::Relaxed);
        let probe = TcpCapacityProbeCommand {
            owner: TcpCapacityProbeOwner::Response {
                path_instance_id: request.path_instance_id,
            },
            path_id: request.path_id,
            calibration_id,
            train_payload_bytes: request.train_payload_bytes,
            sample_floor_bytes: request.sample_floor_bytes,
            warmup_carrier_bytes: 0,
            timing_slack_bytes: 0,
            required_timed_carrier_bytes: request.train_payload_bytes,
            baseline_expires_at: None,
            write_expires_at: None,
            expires_at: request.expires_at,
            _lease: lease,
            _session_lease: TcpCapacityProbeSessionLeaseOwner::Response(session_lease),
        };
        self.metrics.add_pending_bytes(pending_bytes);
        permit.send(QueuedReliablePathCommand::new(
            ReliablePathCommand::SendTcpCapacityProbe(probe),
            pending_bytes,
            self.metrics.clone(),
        ));
        #[cfg(feature = "lab-diagnostics")]
        lab_diagnostic(
            "path_command_queue_send",
            format_args!(
                "queue=data command_kind=tcp_capacity_probe stream_id=0 lane={:?} effective_lane={:?} pacing_bytes={} wait_ms=0 result=queued calibration_id={}",
                FlowLane::Throughput,
                FlowLane::Throughput,
                pending_bytes,
                calibration_id,
            ),
        );
        Ok(calibration_id)
    }

    /// Queues one client-to-server TCP carrier proof with no product offset.
    pub(in crate::runtime) fn try_enqueue_request_tcp_capacity_probe(
        &self,
        request: RequestTcpCapacityProbeRequest,
        session_lease: RequestTcpCapacityProbeLease,
    ) -> Result<(), RuntimeError> {
        let permit = match self.data.try_reserve() {
            Ok(permit) => permit,
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                return Err(RuntimeError::SenderServiceBlocked);
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                return Err(RuntimeError::ReliablePathSessionClosed);
            }
        };
        let lease = self.metrics.try_reserve_tcp_capacity_probe()?;
        let pending_bytes = usize::try_from(request.train_payload_bytes).unwrap_or(usize::MAX);
        let probe = TcpCapacityProbeCommand {
            owner: TcpCapacityProbeOwner::Request {
                stream_id: request.stream_id,
                path_instance: request.path_instance,
            },
            path_id: request.path_id,
            calibration_id: request.calibration_id,
            train_payload_bytes: request.train_payload_bytes,
            sample_floor_bytes: request.sample_floor_bytes,
            warmup_carrier_bytes: request.warmup_carrier_bytes,
            timing_slack_bytes: request.timing_slack_bytes,
            required_timed_carrier_bytes: request.required_timed_carrier_bytes,
            baseline_expires_at: Some(request.baseline_expires_at),
            write_expires_at: Some(request.write_expires_at),
            expires_at: request.expires_at,
            _session_lease: TcpCapacityProbeSessionLeaseOwner::Request(session_lease),
            _lease: lease,
        };
        self.metrics.add_pending_bytes(pending_bytes);
        permit.send(QueuedReliablePathCommand::new(
            ReliablePathCommand::SendTcpCapacityProbe(probe),
            pending_bytes,
            self.metrics.clone(),
        ));
        #[cfg(feature = "lab-diagnostics")]
        lab_diagnostic(
            "path_command_queue_send",
            format_args!(
                "queue=data command_kind=request_tcp_capacity_probe stream_id={} lane={:?} effective_lane={:?} pacing_bytes={} wait_ms=0 result=queued calibration_id={}",
                request.stream_id.0,
                FlowLane::Throughput,
                FlowLane::Throughput,
                pending_bytes,
                request.calibration_id,
            ),
        );
        Ok(())
    }

    pub(in crate::runtime) fn tcp_capacity_probe_attempted(&self) -> bool {
        self.metrics
            .tcp_capacity_probe
            .attempts
            .load(Ordering::Acquire)
            > 0
    }

    pub(in crate::runtime) fn tcp_capacity_probe_active(&self) -> bool {
        self.metrics
            .tcp_capacity_probe
            .active
            .load(Ordering::Acquire)
    }

    pub(in crate::runtime) fn try_enqueue_stream_ordered_frame(
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
        if reliable_path_frame_requires_capacity_command(&frame) {
            return Err(RuntimeError::Protocol(
                "PATH_CAPACITY_* requires an explicit typed carrier command",
            ));
        }
        let bytes = reliable_path_frame_pacing_bytes(&frame);
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
                permit.send(QueuedReliablePathCommand::new(
                    ReliablePathCommand::SendFrame(frame),
                    bytes,
                    self.metrics.clone(),
                ));
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

    pub(in crate::runtime) fn can_enqueue_frame_now(&self, frame: &Frame, lane: FlowLane) -> bool {
        let effective_lane = reliable_path_effective_frame_lane(frame, lane);
        self.can_enqueue_lane_now(effective_lane)
    }

    pub(in crate::runtime) fn can_enqueue_stream_ordered_frame_now(&self, lane: FlowLane) -> bool {
        let _ = lane;
        self.can_enqueue_lane_now(reliable_path_stream_ordered_queue_lane())
    }

    pub(in crate::runtime) fn can_enqueue_lane_now(&self, lane: FlowLane) -> bool {
        if reliable_path_frame_uses_priority_queue(lane) {
            self.priority.capacity() > 0
        } else {
            self.data.capacity() > 0
        }
    }

    pub(in crate::runtime) fn capacity_notify(&self) -> Arc<Notify> {
        self.metrics.capacity_released.clone()
    }

    #[cfg_attr(not(any(test, feature = "lab-diagnostics")), allow(dead_code))]
    pub(in crate::runtime) fn pending_bytes(&self) -> u64 {
        self.metrics.pending_bytes()
    }

    pub(in crate::runtime) fn is_closed(&self) -> bool {
        self.control.is_closed() && self.priority.is_closed() && self.data.is_closed()
    }

    pub(in crate::runtime) fn same_channel(&self, other: &Self) -> bool {
        self.control.same_channel(&other.control)
            && self.priority.same_channel(&other.priority)
            && self.data.same_channel(&other.data)
    }
}

pub(in crate::runtime) fn reliable_path_frame_uses_priority_queue(lane: FlowLane) -> bool {
    matches!(
        lane,
        FlowLane::Control | FlowLane::Latency | FlowLane::RealtimeDatagram
    )
}

pub(in crate::runtime) fn reliable_path_stream_ordered_queue_lane() -> FlowLane {
    FlowLane::Throughput
}

pub(in crate::runtime) fn reliable_path_effective_frame_lane(
    frame: &Frame,
    stream_lane: FlowLane,
) -> FlowLane {
    match frame {
        Frame::StreamData { .. }
        | Frame::PathCapacityData { .. }
        | Frame::PathCapacityFinish { .. } => stream_lane,
        Frame::DatagramData { .. } | Frame::DatagramFeedback { .. } => FlowLane::RealtimeDatagram,
        _ => FlowLane::Control,
    }
}

/// Capacity records use shared wire frames but carrier-specific commands and proof authority.
pub(in crate::runtime) fn reliable_path_frame_requires_capacity_command(frame: &Frame) -> bool {
    matches!(
        frame,
        Frame::PathCapacityData { .. }
            | Frame::PathCapacityFinish { .. }
            | Frame::PathCapacityReceipt { .. }
    )
}

pub(in crate::runtime) fn reliable_path_command_channels(
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
            dequeued_unreleased_bytes: AtomicU64::new(0),
        },
    )
}

fn path_command_receiver_may_recv<T>(receiver: &mpsc::Receiver<T>) -> bool {
    !receiver.is_closed() || !receiver.is_empty()
}

pub(in crate::runtime) fn reliable_path_receivers_closed(
    receivers: &ReliablePathCommandReceivers,
) -> bool {
    !path_command_receiver_may_recv(&receivers.control)
        && !path_command_receiver_may_recv(&receivers.priority)
        && !path_command_receiver_may_recv(&receivers.data)
}

pub(in crate::runtime) async fn recv_reliable_path_command(
    receivers: &mut ReliablePathCommandReceivers,
) -> Option<ReliablePathCommand> {
    if let Some(command) = recv_ready_priority_command(receivers) {
        return Some(command);
    }
    let control_may_recv = path_command_receiver_may_recv(&receivers.control);
    let priority_may_recv = path_command_receiver_may_recv(&receivers.priority);
    let data_may_recv = path_command_receiver_may_recv(&receivers.data);
    let command = match (control_may_recv, priority_may_recv, data_may_recv) {
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
    };
    command.map(|command| receivers.take_queued_command(command))
}

pub(in crate::runtime) fn try_recv_reliable_path_command(
    receivers: &mut ReliablePathCommandReceivers,
) -> Option<ReliablePathCommand> {
    recv_ready_priority_command(receivers).or_else(|| {
        receivers
            .data
            .try_recv()
            .ok()
            .map(|command| receivers.take_queued_command(command))
    })
}

pub(in crate::runtime) fn try_recv_reliable_path_priority_command(
    receivers: &mut ReliablePathCommandReceivers,
) -> Option<ReliablePathCommand> {
    recv_ready_priority_command(receivers)
}

pub(in crate::runtime) fn reliable_path_writer_should_coalesce_partial_bulk_run(
    sent_items: usize,
    sent_bytes: usize,
    byte_budget: usize,
    item_budget: usize,
) -> bool {
    sent_items > 0 && sent_bytes > 0 && sent_bytes < byte_budget && sent_items < item_budget
}

pub(in crate::runtime) async fn try_coalesce_reliable_path_writer_run(
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

pub(in crate::runtime) fn reliable_path_command_writer_run_budget_bytes(
    mux_limits: MuxLimits,
) -> usize {
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

pub(in crate::runtime) fn reliable_noninterlocked_tcp_writer_run_budget_bytes(
    mux_limits: MuxLimits,
) -> usize {
    BBR_MAX_SEND_QUANTUM_BYTES
        .min(reliable_relay_buffer_len(mux_limits))
        .min(mux_limits.max_payload_bytes)
        .max(1)
}

pub(in crate::runtime) fn reliable_path_command_writer_run_budget_items(
    mux_limits: MuxLimits,
) -> usize {
    reliable_path_command_queue(mux_limits).max(1)
}

pub(in crate::runtime) fn reliable_path_command_queue(mux_limits: MuxLimits) -> usize {
    let frame_payload = reliable_path_command_queue_payload(mux_limits);
    reliable_path_command_queue_for_payload(mux_limits, frame_payload)
}

pub(in crate::runtime) fn reliable_path_command_queue_for_payload(
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

pub(in crate::runtime) fn reliable_path_writer_frame_queue(mux_limits: MuxLimits) -> usize {
    let frame_payload = reliable_path_command_queue_payload(mux_limits);
    reliable_path_writer_frame_queue_for_payload(mux_limits, frame_payload)
}

fn reliable_path_command_queue_payload(mux_limits: MuxLimits) -> usize {
    reliable_relay_scheduler_quantum_cap(None, FlowLane::Throughput, mux_limits)
        .min(mux_limits.max_reliable_relay_chunk_bytes)
        .min(mux_limits.max_payload_bytes)
        .max(1)
}

pub(in crate::runtime) fn reliable_path_writer_frame_queue_for_payload(
    mux_limits: MuxLimits,
    frame_payload_bytes: usize,
) -> usize {
    reliable_stream_frame_queue_for_payload(mux_limits, frame_payload_bytes)
        .saturating_mul(reliable_path_writer_lane_count())
        .max(reliable_path_writer_lane_count())
}

pub(in crate::runtime) fn reliable_stream_frame_queue(mux_limits: MuxLimits) -> usize {
    let frame_payload = mux_limits
        .max_reliable_relay_chunk_bytes
        .min(mux_limits.max_payload_bytes)
        .max(1);
    reliable_stream_frame_queue_for_payload(mux_limits, frame_payload)
}

pub(in crate::runtime) fn reliable_stream_frame_queue_for_payload(
    mux_limits: MuxLimits,
    frame_payload_bytes: usize,
) -> usize {
    let frame_payload = frame_payload_bytes.min(mux_limits.max_payload_bytes).max(1);
    let priority_headroom = reliable_path_priority_headroom_frames();
    (mux_limits.max_reorder_bytes / frame_payload)
        .saturating_add(priority_headroom)
        .max(priority_headroom)
}

pub(in crate::runtime) fn reliable_path_priority_headroom_frames() -> usize {
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
        return Some(receivers.take_queued_command(command));
    }
    receivers
        .priority
        .try_recv()
        .ok()
        .map(|command| receivers.take_queued_command(command))
}

pub(in crate::runtime) fn reliable_path_command_pending_bytes(
    command: &ReliablePathCommand,
) -> usize {
    match command {
        ReliablePathCommand::SendFrame(frame) => reliable_path_frame_pacing_bytes(frame),
        ReliablePathCommand::SendQuicCapacityProbe(probe) => {
            usize::try_from(probe.train_payload_bytes).unwrap_or(usize::MAX)
        }
        ReliablePathCommand::SendTcpCapacityProbe(probe) => {
            usize::try_from(probe.train_payload_bytes).unwrap_or(usize::MAX)
        }
        ReliablePathCommand::OpenStream { .. }
        | ReliablePathCommand::CancelTcpOpen { .. }
        | ReliablePathCommand::CloseStream(_) => 0,
    }
}

pub(in crate::runtime) fn reliable_path_command_writer_run_bytes(
    command: &ReliablePathCommand,
) -> usize {
    match command {
        ReliablePathCommand::SendFrame(frame) => {
            crate::protocol::codec::encoded_frame_capacity_hint(frame).max(1)
        }
        ReliablePathCommand::SendQuicCapacityProbe(probe) => {
            usize::try_from(probe.train_payload_bytes)
                .unwrap_or(usize::MAX)
                .max(1)
        }
        ReliablePathCommand::SendTcpCapacityProbe(probe) => {
            usize::try_from(probe.train_payload_bytes)
                .unwrap_or(usize::MAX)
                .max(1)
        }
        ReliablePathCommand::OpenStream { .. }
        | ReliablePathCommand::CancelTcpOpen { .. }
        | ReliablePathCommand::CloseStream(_) => 1,
    }
}

#[cfg(feature = "lab-diagnostics")]
fn reliable_path_command_stream_id(command: &ReliablePathCommand) -> StreamId {
    match command {
        ReliablePathCommand::SendFrame(frame) => reliable_path_frame_stream_id(frame),
        ReliablePathCommand::SendQuicCapacityProbe(_) => StreamId(0),
        ReliablePathCommand::SendTcpCapacityProbe(_) => StreamId(0),
        ReliablePathCommand::OpenStream { stream_id, .. }
        | ReliablePathCommand::CancelTcpOpen { stream_id, .. }
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

#[derive(Debug)]
struct QuicCapacityProbeCommandTicketState {
    resolution: AtomicU8,
    resolved: tokio::sync::Notify,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum QuicCapacityProbeCommandResolution {
    Current = 0,
    Cancelled = 1,
    Published = 2,
}

#[derive(Debug, Clone)]
pub(in crate::runtime) struct QuicCapacityProbeCommandTicket {
    state: Arc<QuicCapacityProbeCommandTicketState>,
}

impl QuicCapacityProbeCommandTicket {
    pub(in crate::runtime) fn new() -> Self {
        Self {
            state: Arc::new(QuicCapacityProbeCommandTicketState {
                resolution: AtomicU8::new(QuicCapacityProbeCommandResolution::Current as u8),
                resolved: tokio::sync::Notify::new(),
            }),
        }
    }

    pub(in crate::runtime) fn resolution(&self) -> QuicCapacityProbeCommandResolution {
        match self.state.resolution.load(Ordering::Acquire) {
            value if value == QuicCapacityProbeCommandResolution::Current as u8 => {
                QuicCapacityProbeCommandResolution::Current
            }
            value if value == QuicCapacityProbeCommandResolution::Cancelled as u8 => {
                QuicCapacityProbeCommandResolution::Cancelled
            }
            value if value == QuicCapacityProbeCommandResolution::Published as u8 => {
                QuicCapacityProbeCommandResolution::Published
            }
            _ => unreachable!("invalid QUIC capacity command ticket resolution"),
        }
    }

    pub(in crate::runtime) fn is_current(&self) -> bool {
        self.resolution() == QuicCapacityProbeCommandResolution::Current
    }

    fn resolve(&self, resolution: QuicCapacityProbeCommandResolution) -> bool {
        debug_assert_ne!(resolution, QuicCapacityProbeCommandResolution::Current);
        let resolved = self
            .state
            .resolution
            .compare_exchange(
                QuicCapacityProbeCommandResolution::Current as u8,
                resolution as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok();
        if resolved {
            self.state.resolved.notify_waiters();
        }
        resolved
    }

    pub(in crate::runtime) fn cancel(&self) -> bool {
        self.resolve(QuicCapacityProbeCommandResolution::Cancelled)
    }

    pub(in crate::runtime) fn publish(&self) -> bool {
        self.resolve(QuicCapacityProbeCommandResolution::Published)
    }

    pub(in crate::runtime) async fn resolved(&self) -> QuicCapacityProbeCommandResolution {
        loop {
            let resolution = self.resolution();
            if resolution != QuicCapacityProbeCommandResolution::Current {
                return resolution;
            }
            let notified = self.state.resolved.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let resolution = self.resolution();
            if resolution != QuicCapacityProbeCommandResolution::Current {
                return resolution;
            }
            notified.await;
        }
    }

    pub(in crate::runtime) async fn cancelled(&self) {
        if self.resolved().await == QuicCapacityProbeCommandResolution::Cancelled {
            return;
        }
        std::future::pending::<()>().await;
    }
}

#[derive(Debug)]
pub(in crate::runtime) struct QuicCapacityProbeCommand {
    // The command channel is attachment-local. This identity is only a stale
    // ownership fence and lab correlation key; QUIC never interprets product
    // stream or response-binding semantics.
    pub(in crate::runtime) owner: QuicCapacityProbeOwner,
    pub(in crate::runtime) path_id: PathId,
    pub(in crate::runtime) calibration_id: u64,
    pub(in crate::runtime) train_payload_bytes: u64,
    pub(in crate::runtime) sample_floor_bytes: u64,
    pub(in crate::runtime) warmup_carrier_bytes: u64,
    pub(in crate::runtime) required_timed_carrier_bytes: u64,
    pub(in crate::runtime) proof_validity: std::time::Duration,
    pub(in crate::runtime) expires_at: std::time::Instant,
    pub(in crate::runtime) ticket: QuicCapacityProbeCommandTicket,
    pub(in crate::runtime) cancel_on_drop: bool,
}

static NEXT_TCP_CAPACITY_CALIBRATION_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct TcpCapacityProbeRequest {
    pub(in crate::runtime) path_id: PathId,
    pub(in crate::runtime) path_instance_id: ServerCarrierPathInstanceId,
    pub(in crate::runtime) train_payload_bytes: u64,
    pub(in crate::runtime) sample_floor_bytes: u64,
    pub(in crate::runtime) expires_at: Instant,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct RequestTcpCapacityProbeRequest {
    pub(in crate::runtime) stream_id: StreamId,
    pub(in crate::runtime) path_instance: RelayPathInstance,
    pub(in crate::runtime) path_id: PathId,
    pub(in crate::runtime) calibration_id: u64,
    pub(in crate::runtime) train_payload_bytes: u64,
    pub(in crate::runtime) sample_floor_bytes: u64,
    pub(in crate::runtime) warmup_carrier_bytes: u64,
    pub(in crate::runtime) timing_slack_bytes: u64,
    pub(in crate::runtime) required_timed_carrier_bytes: u64,
    pub(in crate::runtime) baseline_expires_at: Instant,
    pub(in crate::runtime) write_expires_at: Instant,
    pub(in crate::runtime) expires_at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum TcpCapacityProbeOwner {
    Request {
        stream_id: StreamId,
        path_instance: RelayPathInstance,
    },
    Response {
        path_instance_id: ServerCarrierPathInstanceId,
    },
}

#[derive(Debug)]
pub(in crate::runtime) enum TcpCapacityProbeSessionLeaseOwner {
    Request(RequestTcpCapacityProbeLease),
    // This field exists for drop order: the response session reservation lives
    // until the exact carrier transaction finishes even though it is not read.
    Response(#[allow(dead_code)] TcpCapacityProbeSessionLease),
}

impl TcpCapacityProbeSessionLeaseOwner {
    pub(in crate::runtime) fn request(&self) -> Option<&RequestTcpCapacityProbeLease> {
        match self {
            Self::Request(lease) => Some(lease),
            Self::Response(_) => None,
        }
    }
}

#[derive(Debug)]
pub(in crate::runtime) struct TcpCapacityProbeCommand {
    pub(in crate::runtime) owner: TcpCapacityProbeOwner,
    pub(in crate::runtime) path_id: PathId,
    pub(in crate::runtime) calibration_id: u64,
    pub(in crate::runtime) train_payload_bytes: u64,
    pub(in crate::runtime) sample_floor_bytes: u64,
    /// Request-side sizing; TCP publishes only the full-train receipt sample.
    pub(in crate::runtime) warmup_carrier_bytes: u64,
    pub(in crate::runtime) timing_slack_bytes: u64,
    pub(in crate::runtime) required_timed_carrier_bytes: u64,
    pub(in crate::runtime) baseline_expires_at: Option<Instant>,
    pub(in crate::runtime) write_expires_at: Option<Instant>,
    pub(in crate::runtime) expires_at: Instant,
    // Drop session ownership before the carrier lease wakes blocked planners.
    pub(in crate::runtime) _session_lease: TcpCapacityProbeSessionLeaseOwner,
    // Dropping the command or completed epoch releases exact-carrier admission.
    pub(in crate::runtime) _lease: TcpCapacityProbeLease,
}

impl TcpCapacityProbeCommand {
    pub(in crate::runtime) fn request_lease(&self) -> Option<&RequestTcpCapacityProbeLease> {
        self._session_lease.request()
    }

    pub(in crate::runtime) fn valid_request_tcp_train(&self) -> bool {
        if self.request_lease().is_none() {
            return false;
        }
        // TCP uses the whole continuous train as a conservative receipt sample.
        // The phase fields still size warmup and minimum evidence consistently
        // with QUIC policy, but no short ACK tail receives rate authority.
        let measurement_bytes = match self
            .timing_slack_bytes
            .checked_add(self.required_timed_carrier_bytes)
        {
            Some(bytes) => bytes,
            None => return false,
        };
        let train = self.warmup_carrier_bytes.checked_add(measurement_bytes);
        self.warmup_carrier_bytes > 0
            && self.timing_slack_bytes > 0
            && self.required_timed_carrier_bytes > 0
            && measurement_bytes >= self.sample_floor_bytes
            && train == Some(self.train_payload_bytes)
            && self
                .baseline_expires_at
                .zip(self.write_expires_at)
                .is_some_and(|(idle, write)| idle < write && write < self.expires_at)
    }
}

impl Drop for TcpCapacityProbeCommand {
    fn drop(&mut self) {
        if let Some(lease) = self.request_lease() {
            lease.cancel();
        }
    }
}

impl QuicCapacityProbeCommand {
    pub(in crate::runtime) fn disarm_drop_cancellation(&mut self) {
        self.cancel_on_drop = false;
    }
}

impl Drop for QuicCapacityProbeCommand {
    fn drop(&mut self) {
        if self.cancel_on_drop {
            self.ticket.cancel();
        }
    }
}

pub(in crate::runtime) enum ReliablePathCommand {
    OpenStream {
        stream_id: StreamId,
        attempt_id: ClientTcpOpenAttemptId,
        observed_carrier_generation: u64,
        target: TargetAddr,
        ingress: IngressKind,
        lane: FlowLane,
        role: StreamOpenRole,
        open_deadlines: ClientTcpOpenDeadlines,
        session_commands: ReliablePathCommandSender,
        response: oneshot::Sender<ClientTcpOpenResponse>,
    },
    CancelTcpOpen {
        stream_id: StreamId,
        attempt_id: ClientTcpOpenAttemptId,
    },
    SendFrame(Frame),
    // TCP writers reject this carrier-specific command. The shared scheduler
    // only owns admission; UDP/QUIC owns how the epoch is encoded and measured.
    SendQuicCapacityProbe(QuicCapacityProbeCommand),
    // TCP owns receipt timing and native socket evidence independently of QUIC.
    SendTcpCapacityProbe(TcpCapacityProbeCommand),
    CloseStream(StreamId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct ClientTcpOpenAttemptId(pub(in crate::runtime) u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct ClientTcpOpenDeadlines {
    pub(in crate::runtime) live: tokio::time::Instant,
    pub(in crate::runtime) setup: tokio::time::Instant,
}

impl ClientTcpOpenDeadlines {
    pub(in crate::runtime) fn fixed(deadline: tokio::time::Instant) -> Self {
        Self {
            live: deadline,
            setup: deadline,
        }
    }

    pub(in crate::runtime) fn from_timeouts(
        now: tokio::time::Instant,
        live: Duration,
        setup: Duration,
    ) -> Self {
        Self {
            live: now + live,
            setup: now + setup.max(live),
        }
    }

    pub(in crate::runtime) fn for_carrier_generation(
        self,
        observed_generation: u64,
        current_generation: u64,
    ) -> tokio::time::Instant {
        if observed_generation != 0 && observed_generation == current_generation {
            self.live
        } else {
            self.setup
        }
    }
}

pub(in crate::runtime) struct ClientTcpOpenedStream {
    pub(in crate::runtime) stream: ReliablePathStream,
    pub(in crate::runtime) open_deadline: tokio::time::Instant,
}

// A rejected duplicate never owned the wire stream, so its caller must not
// detach the earlier owner while unwinding the failed open attempt.
pub(in crate::runtime) enum ClientTcpOpenResponse {
    Opened(ClientTcpOpenedStream),
    RejectedWithoutOpen(RuntimeError),
    FailedAfterOpen(RuntimeError),
}

#[cfg(feature = "lab-diagnostics")]
fn reliable_path_command_kind(command: &ReliablePathCommand) -> &'static str {
    match command {
        ReliablePathCommand::OpenStream { .. } => "open_stream",
        ReliablePathCommand::CancelTcpOpen { .. } => "cancel_tcp_open",
        ReliablePathCommand::SendFrame(frame) => reliable_path_frame_kind(frame),
        ReliablePathCommand::SendQuicCapacityProbe(_) => "quic_capacity_probe",
        ReliablePathCommand::SendTcpCapacityProbe(_) => "tcp_capacity_probe",
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
        Frame::PathCapacityData { .. } => "path_capacity_data",
        Frame::PathCapacityFinish { .. } => "path_capacity_finish",
        Frame::PathCapacityReceipt { .. } => "path_capacity_receipt",
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

#[cfg(test)]
#[path = "commands_test.rs"]
mod tests;
