use super::commands::{
    QuicCapacityProbeCommand, ReliablePathCommand, RequestTcpCapacityProbeRequest,
    TcpCapacityProbeCommand,
};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::{lab_diagnostic, lab_perf_record};
use crate::model::capacity::{
    MAX_RELIABLE_SERVICE_QUANTUM_BYTES, reliable_relay_buffer_len,
    reliable_relay_scheduler_quantum_cap,
};
use crate::mux::MuxLimits;
use crate::protocol::frame::reliable_path_frame_pacing_bytes;
use crate::protocol::{Frame, ResetReason, StreamId};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::tcp::capacity::RequestTcpCapacityProbeLease;
use crate::scheduler::TrafficClass;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
};
#[cfg(feature = "lab-diagnostics")]
use std::time::Instant;
use tokio::sync::{Notify, mpsc};

// Bounded, traffic-class-separated transfer from carrier-neutral scheduling to TCP or
// QUIC writers. Control keeps independent capacity; a typed carrier probe remains
// one data-lane command so its token and frozen proof contract cannot split.

const RELIABLE_PATH_PRIORITY_HEADROOM_LANES: [TrafficClass; 4] = [
    TrafficClass::Control,
    TrafficClass::Latency,
    TrafficClass::RealtimeDatagram,
    TrafficClass::Background,
];

#[derive(Clone)]
pub(in crate::runtime) struct ReliablePathCommandSender {
    retirement: mpsc::UnboundedSender<ReliablePathRetirementCommand>,
    control: mpsc::Sender<QueuedReliablePathCommand>,
    priority: mpsc::Sender<QueuedReliablePathCommand>,
    data: mpsc::Sender<QueuedReliablePathCommand>,
    metrics: Arc<ReliablePathCommandQueueMetrics>,
}

pub(in crate::runtime) struct ReliablePathCommandReceivers {
    retirement: mpsc::UnboundedReceiver<ReliablePathRetirementCommand>,
    pending_retirement_close: Option<StreamId>,
    control: mpsc::Receiver<QueuedReliablePathCommand>,
    priority: mpsc::Receiver<QueuedReliablePathCommand>,
    data: mpsc::Receiver<QueuedReliablePathCommand>,
    metrics: Arc<ReliablePathCommandQueueMetrics>,
    dequeued_unreleased_bytes: AtomicU64,
}

/// Immutable queue readiness captured at an observe boundary. Policy may rank
/// this value, but only the command owner may resolve a sender and enqueue.
#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ReliablePathCommandQueueSnapshot {
    priority_ready: bool,
    data_ready: bool,
}

impl ReliablePathCommandQueueSnapshot {
    pub(in crate::runtime) fn can_enqueue_lane(self, lane: TrafficClass) -> bool {
        if reliable_path_frame_uses_priority_queue(lane) {
            self.priority_ready
        } else {
            self.data_ready
        }
    }

    pub(in crate::runtime) fn can_enqueue_frame(self, frame: &Frame, lane: TrafficClass) -> bool {
        self.can_enqueue_lane(reliable_path_effective_frame_lane(frame, lane))
    }

    pub(in crate::runtime) fn can_enqueue_stream_ordered_frame(self) -> bool {
        self.can_enqueue_lane(reliable_path_stream_ordered_queue_lane())
    }
}

/// A carrier-owned terminal transaction for an accepted stream whose product
/// attachment never committed. It is intentionally separate from bounded work:
/// queue pressure must not leak a peer binding or its local actor entry. The
/// carrier's accepted-stream limit bounds outstanding retirements.
#[derive(Debug, Clone, Copy)]
enum ReliablePathRetirementCommand {
    RetireAcceptedStream(StreamId),
}

/// Holds queue capacity without publishing a frame. Response transactions use
/// this to make their metadata visible before the carrier can dequeue work.
pub(in crate::runtime) struct ReliablePathFrameReservation<'a> {
    permit: mpsc::Permit<'a, QueuedReliablePathCommand>,
    frame: Frame,
    bytes: usize,
    metrics: Arc<ReliablePathCommandQueueMetrics>,
    #[cfg(feature = "lab-diagnostics")]
    lane: TrafficClass,
    #[cfg(feature = "lab-diagnostics")]
    effective_lane: TrafficClass,
}

impl ReliablePathFrameReservation<'_> {
    pub(in crate::runtime) fn commit(self) {
        #[cfg(feature = "lab-diagnostics")]
        let frame_kind = reliable_path_frame_kind(&self.frame);
        #[cfg(feature = "lab-diagnostics")]
        let stream_id = reliable_path_frame_stream_id(&self.frame);
        self.metrics.add_pending_bytes(self.bytes);
        self.permit.send(QueuedReliablePathCommand::new(
            ReliablePathCommand::SendFrame(self.frame),
            self.bytes,
            self.metrics,
        ));
        #[cfg(feature = "lab-diagnostics")]
        lab_diagnostic(
            "path_command_queue_send",
            format_args!(
                "queue={} frame_kind={} stream_id={} lane={:?} effective_lane={:?} pacing_bytes={} wait_ms=0 result=queued",
                if reliable_path_frame_uses_priority_queue(self.effective_lane) {
                    "priority"
                } else {
                    "data"
                },
                frame_kind,
                stream_id.0,
                self.lane,
                self.effective_lane,
                self.bytes,
            ),
        );
    }
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
    pub(in crate::runtime) fn queue_snapshot(&self) -> ReliablePathCommandQueueSnapshot {
        let priority_open = !self.priority.is_closed();
        let data_open = !self.data.is_closed();
        ReliablePathCommandQueueSnapshot {
            priority_ready: priority_open && self.priority.capacity() > 0,
            data_ready: data_open && self.data.capacity() > 0,
        }
    }

    pub(in crate::runtime) fn retire_accepted_stream(
        &self,
        stream_id: StreamId,
    ) -> Result<(), RuntimeError> {
        self.retirement
            .send(ReliablePathRetirementCommand::RetireAcceptedStream(
                stream_id,
            ))
            .map_err(|_| RuntimeError::ReliablePathSessionClosed)
    }

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

    pub(in crate::runtime) async fn send_stream_ordered_frame(
        &self,
        frame: Frame,
        lane: TrafficClass,
    ) -> Result<(), mpsc::error::SendError<ReliablePathCommand>> {
        self.send_stream_ordered_command(ReliablePathCommand::SendFrame(frame), lane)
            .await
    }

    pub(in crate::runtime) async fn send_stream_ordered_close(
        &self,
        stream_id: StreamId,
        lane: TrafficClass,
    ) -> Result<(), mpsc::error::SendError<ReliablePathCommand>> {
        self.send_stream_ordered_command(ReliablePathCommand::CloseStream(stream_id), lane)
            .await
    }

    pub(in crate::runtime) async fn send_stream_ordered_reset_and_close(
        &self,
        stream_id: StreamId,
        reason: ResetReason,
        lane: TrafficClass,
    ) -> Result<(), mpsc::error::SendError<ReliablePathCommand>> {
        self.send_stream_ordered_command(
            ReliablePathCommand::ResetAndCloseStream { stream_id, reason },
            lane,
        )
        .await
    }

    async fn send_stream_ordered_command(
        &self,
        command: ReliablePathCommand,
        _lane: TrafficClass,
    ) -> Result<(), mpsc::error::SendError<ReliablePathCommand>> {
        let pending_bytes = reliable_path_command_pending_bytes(&command);
        #[cfg(feature = "lab-diagnostics")]
        let command_kind = reliable_path_command_kind(&command);
        #[cfg(feature = "lab-diagnostics")]
        let stream_id = reliable_path_command_stream_id(&command);
        #[cfg(feature = "lab-diagnostics")]
        let ordered_lane = reliable_path_stream_ordered_queue_lane();
        let queue = &self.data;
        #[cfg(feature = "lab-diagnostics")]
        let started = Instant::now();
        self.metrics.add_pending_bytes(pending_bytes);
        let result = queue
            .send(QueuedReliablePathCommand::new(
                command,
                pending_bytes,
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
                    "queue=data command_kind={} stream_id={} lane={:?} effective_lane={:?} pacing_bytes={} wait_ms={} result={}",
                    command_kind,
                    stream_id.0,
                    _lane,
                    ordered_lane,
                    pending_bytes,
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
        lane: TrafficClass,
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
        let measurement_id = probe.measurement_id;
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
                "queue=data command_kind=quic_capacity_probe stream_id=0 lane={:?} effective_lane={:?} pacing_bytes={} wait_ms=0 result={} measurement_id={}",
                TrafficClass::Throughput,
                TrafficClass::Throughput,
                pending_bytes,
                match &result {
                    Ok(()) => "queued",
                    Err(RuntimeError::SenderServiceBlocked) => "blocked",
                    Err(_) => "closed",
                },
                measurement_id,
            ),
        );
        result
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
            stream_id: request.stream_id,
            path_instance: request.path_instance,
            path_id: request.path_id,
            measurement_id: request.measurement_id,
            train_payload_bytes: request.train_payload_bytes,
            sample_floor_bytes: request.sample_floor_bytes,
            warmup_carrier_bytes: request.warmup_carrier_bytes,
            timing_slack_bytes: request.timing_slack_bytes,
            required_timed_carrier_bytes: request.required_timed_carrier_bytes,
            baseline_expires_at: request.baseline_expires_at,
            write_expires_at: request.write_expires_at,
            expires_at: request.expires_at,
            request_lease: session_lease,
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
                "queue=data command_kind=request_tcp_capacity_probe stream_id={} lane={:?} effective_lane={:?} pacing_bytes={} wait_ms=0 result=queued measurement_id={}",
                request.stream_id.0,
                TrafficClass::Throughput,
                TrafficClass::Throughput,
                pending_bytes,
                request.measurement_id,
            ),
        );
        Ok(())
    }

    pub(in crate::runtime) fn try_enqueue_stream_ordered_frame(
        &self,
        frame: Frame,
        lane: TrafficClass,
    ) -> Result<(), RuntimeError> {
        let reservation = self.try_reserve_admitted_frame_with_effective_lane(
            frame,
            lane,
            Some(reliable_path_stream_ordered_queue_lane()),
        )?;
        reservation.commit();
        Ok(())
    }

    pub(in crate::runtime) fn try_reserve_stream_ordered_frame(
        &self,
        frame: Frame,
        lane: TrafficClass,
    ) -> Result<ReliablePathFrameReservation<'_>, RuntimeError> {
        self.try_reserve_admitted_frame_with_effective_lane(
            frame,
            lane,
            Some(reliable_path_stream_ordered_queue_lane()),
        )
    }

    fn try_enqueue_admitted_frame_with_effective_lane(
        &self,
        frame: Frame,
        lane: TrafficClass,
        effective_lane_override: Option<TrafficClass>,
    ) -> Result<(), RuntimeError> {
        let reservation = self.try_reserve_admitted_frame_with_effective_lane(
            frame,
            lane,
            effective_lane_override,
        )?;
        reservation.commit();
        Ok(())
    }

    fn try_reserve_admitted_frame_with_effective_lane(
        &self,
        frame: Frame,
        lane: TrafficClass,
        effective_lane_override: Option<TrafficClass>,
    ) -> Result<ReliablePathFrameReservation<'_>, RuntimeError> {
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
        let permit = match queue.try_reserve() {
            Ok(permit) => permit,
            Err(err) => {
                let error = match err {
                    tokio::sync::mpsc::error::TrySendError::Full(_) => {
                        RuntimeError::SenderServiceBlocked
                    }
                    tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                        RuntimeError::ReliablePathSessionClosed
                    }
                };
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "path_command_queue_send",
                    format_args!(
                        "queue={} frame_kind={} stream_id={} lane={:?} effective_lane={:?} pacing_bytes={} wait_ms=0 result={}",
                        if reliable_path_frame_uses_priority_queue(effective_lane) {
                            "priority"
                        } else {
                            "data"
                        },
                        frame_kind,
                        stream_id.0,
                        lane,
                        effective_lane,
                        bytes,
                        if matches!(&error, RuntimeError::SenderServiceBlocked) {
                            "blocked"
                        } else {
                            "closed"
                        },
                    ),
                );
                return Err(error);
            }
        };
        Ok(ReliablePathFrameReservation {
            permit,
            frame,
            bytes,
            metrics: self.metrics.clone(),
            #[cfg(feature = "lab-diagnostics")]
            lane,
            #[cfg(feature = "lab-diagnostics")]
            effective_lane,
        })
    }

    pub(in crate::runtime) fn can_enqueue_frame_now(
        &self,
        frame: &Frame,
        lane: TrafficClass,
    ) -> bool {
        let effective_lane = reliable_path_effective_frame_lane(frame, lane);
        self.can_enqueue_lane_now(effective_lane)
    }

    pub(in crate::runtime) fn can_enqueue_stream_ordered_frame_now(
        &self,
        lane: TrafficClass,
    ) -> bool {
        let _ = lane;
        self.can_enqueue_lane_now(reliable_path_stream_ordered_queue_lane())
    }

    pub(in crate::runtime) fn can_enqueue_lane_now(&self, lane: TrafficClass) -> bool {
        self.queue_snapshot().can_enqueue_lane(lane)
    }

    pub(in crate::runtime) fn capacity_notify(&self) -> Arc<Notify> {
        self.metrics.capacity_released.clone()
    }

    #[cfg_attr(not(any(test, feature = "lab-diagnostics")), allow(dead_code))]
    pub(in crate::runtime) fn pending_bytes(&self) -> u64 {
        self.metrics.pending_bytes()
    }

    pub(in crate::runtime) fn is_closed(&self) -> bool {
        self.retirement.is_closed()
            && self.control.is_closed()
            && self.priority.is_closed()
            && self.data.is_closed()
    }

    pub(in crate::runtime) fn same_channel(&self, other: &Self) -> bool {
        self.retirement.same_channel(&other.retirement)
            && self.control.same_channel(&other.control)
            && self.priority.same_channel(&other.priority)
            && self.data.same_channel(&other.data)
    }
}

pub(in crate::runtime) fn reliable_path_frame_uses_priority_queue(lane: TrafficClass) -> bool {
    lane.is_latency_sensitive()
}

pub(in crate::runtime) fn reliable_path_stream_ordered_queue_lane() -> TrafficClass {
    TrafficClass::Throughput
}

pub(in crate::runtime) fn reliable_path_effective_frame_lane(
    frame: &Frame,
    stream_lane: TrafficClass,
) -> TrafficClass {
    match frame {
        Frame::StreamData { .. }
        | Frame::PathCapacityData { .. }
        | Frame::PathCapacityFinish { .. } => stream_lane,
        Frame::DatagramData { .. } | Frame::DatagramFeedback { .. } => {
            TrafficClass::RealtimeDatagram
        }
        _ => TrafficClass::Control,
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
    let (retirement_tx, retirement_rx) = mpsc::unbounded_channel();
    let (control_tx, control_rx) = mpsc::channel(queue);
    let (priority_tx, priority_rx) = mpsc::channel(queue);
    let (data_tx, data_rx) = mpsc::channel(queue);
    let metrics = Arc::new(ReliablePathCommandQueueMetrics::default());
    (
        ReliablePathCommandSender {
            retirement: retirement_tx,
            control: control_tx,
            priority: priority_tx,
            data: data_tx,
            metrics: metrics.clone(),
        },
        ReliablePathCommandReceivers {
            retirement: retirement_rx,
            pending_retirement_close: None,
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

fn retirement_receiver_may_recv<T>(receiver: &mpsc::UnboundedReceiver<T>) -> bool {
    !receiver.is_closed() || !receiver.is_empty()
}

pub(in crate::runtime) fn reliable_path_receivers_closed(
    receivers: &ReliablePathCommandReceivers,
) -> bool {
    receivers.pending_retirement_close.is_none()
        && !retirement_receiver_may_recv(&receivers.retirement)
        && !path_command_receiver_may_recv(&receivers.control)
        && !path_command_receiver_may_recv(&receivers.priority)
        && !path_command_receiver_may_recv(&receivers.data)
}

pub(in crate::runtime) async fn recv_reliable_path_command(
    receivers: &mut ReliablePathCommandReceivers,
) -> Option<ReliablePathCommand> {
    enum ReceivedCommand {
        Retirement(Option<ReliablePathRetirementCommand>),
        Queued(Option<QueuedReliablePathCommand>),
    }

    loop {
        if let Some(command) = recv_ready_priority_command(receivers) {
            return Some(command);
        }
        let retirement_may_recv = retirement_receiver_may_recv(&receivers.retirement);
        let control_may_recv = path_command_receiver_may_recv(&receivers.control);
        let priority_may_recv = path_command_receiver_may_recv(&receivers.priority);
        let data_may_recv = path_command_receiver_may_recv(&receivers.data);
        let received = tokio::select! {
            biased;
            command = receivers.retirement.recv(), if retirement_may_recv => {
                ReceivedCommand::Retirement(command)
            }
            command = receivers.control.recv(), if control_may_recv => {
                ReceivedCommand::Queued(command)
            }
            command = receivers.priority.recv(), if priority_may_recv => {
                ReceivedCommand::Queued(command)
            }
            command = receivers.data.recv(), if data_may_recv => {
                ReceivedCommand::Queued(command)
            }
            else => return None,
        };
        match received {
            ReceivedCommand::Retirement(Some(command)) => {
                return Some(begin_reliable_path_retirement(receivers, command));
            }
            ReceivedCommand::Queued(Some(command)) => {
                return Some(receivers.take_queued_command(command));
            }
            ReceivedCommand::Retirement(None) | ReceivedCommand::Queued(None) => {}
        }
    }
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
        reliable_relay_scheduler_quantum_cap(None, TrafficClass::Throughput, mux_limits)
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
    MAX_RELIABLE_SERVICE_QUANTUM_BYTES
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
    reliable_relay_scheduler_quantum_cap(None, TrafficClass::Throughput, mux_limits)
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
    if let Some(stream_id) = receivers.pending_retirement_close.take() {
        return Some(ReliablePathCommand::CloseStream(stream_id));
    }
    if let Ok(command) = receivers.retirement.try_recv() {
        return Some(begin_reliable_path_retirement(receivers, command));
    }
    if let Ok(command) = receivers.control.try_recv() {
        return Some(receivers.take_queued_command(command));
    }
    receivers
        .priority
        .try_recv()
        .ok()
        .map(|command| receivers.take_queued_command(command))
}

fn begin_reliable_path_retirement(
    receivers: &mut ReliablePathCommandReceivers,
    command: ReliablePathRetirementCommand,
) -> ReliablePathCommand {
    match command {
        ReliablePathRetirementCommand::RetireAcceptedStream(stream_id) => {
            debug_assert!(receivers.pending_retirement_close.is_none());
            receivers.pending_retirement_close = Some(stream_id);
            ReliablePathCommand::SendFrame(Frame::StreamDetach { stream_id })
        }
    }
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
        ReliablePathCommand::ResetAndCloseStream { stream_id, reason } => {
            reliable_path_frame_pacing_bytes(&Frame::StreamReset {
                stream_id: *stream_id,
                reason: *reason,
            })
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
        ReliablePathCommand::ResetAndCloseStream { stream_id, reason } => {
            crate::protocol::codec::encoded_frame_capacity_hint(&Frame::StreamReset {
                stream_id: *stream_id,
                reason: *reason,
            })
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
        | ReliablePathCommand::ResetAndCloseStream { stream_id, .. }
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

#[cfg(feature = "lab-diagnostics")]
fn reliable_path_command_kind(command: &ReliablePathCommand) -> &'static str {
    match command {
        ReliablePathCommand::OpenStream { .. } => "open_stream",
        ReliablePathCommand::CancelTcpOpen { .. } => "cancel_tcp_open",
        ReliablePathCommand::SendFrame(frame) => reliable_path_frame_kind(frame),
        ReliablePathCommand::SendQuicCapacityProbe(_) => "quic_capacity_probe",
        ReliablePathCommand::SendTcpCapacityProbe(_) => "tcp_capacity_probe",
        ReliablePathCommand::ResetAndCloseStream { .. } => "reset_and_close_stream",
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
        Frame::PathStatus { .. } => "path_status",
        Frame::PathDrain { .. } => "path_drain",
        Frame::PathClose { .. } => "path_close",
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
        Frame::Ping { .. } => "ping",
        Frame::Pong { .. } => "pong",
        Frame::PeerStatusRequest { .. } => "peer_status_request",
        Frame::PeerStatusResponse { .. } => "peer_status_response",
    }
}

#[cfg(test)]
#[path = "queue_test.rs"]
mod tests;
