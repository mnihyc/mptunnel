use super::commands::{
    ReliablePathCommand, RequestTcpCapacityProbeRequest, TcpCapacityProbeCommand,
};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::{lab_diagnostic, lab_perf_record};
use crate::model::capacity::{reliable_relay_buffer_len, reliable_relay_scheduler_quantum_cap};
use crate::mux::MuxLimits;
use crate::protocol::frame::reliable_path_frame_pacing_bytes;
use crate::protocol::{Frame, ResetReason, StreamId};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::tcp::capacity::RequestTcpCapacityProbeLease;
use crate::runtime::recent_ids::RecentIdCache;
use crate::scheduler::TrafficClass;
use std::sync::{
    Arc, Mutex,
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
    reinjection: mpsc::Sender<QueuedReliablePathCommand>,
    data: mpsc::Sender<QueuedReliablePathCommand>,
    metrics: Arc<ReliablePathCommandQueueMetrics>,
}

pub(in crate::runtime) struct ReliablePathCommandReceivers {
    retirement: mpsc::UnboundedReceiver<ReliablePathRetirementCommand>,
    pending_retirement_close: Option<StreamId>,
    control: mpsc::Receiver<QueuedReliablePathCommand>,
    priority: mpsc::Receiver<QueuedReliablePathCommand>,
    reinjection: mpsc::Receiver<QueuedReliablePathCommand>,
    data: mpsc::Receiver<QueuedReliablePathCommand>,
    // A control close may overtake bounded data queues. Retain enough terminal
    // IDs to discard every older queued frame instead of writing stale bytes.
    closed_streams: RecentIdCache<StreamId>,
    metrics: Arc<ReliablePathCommandQueueMetrics>,
    dequeued_unreleased_bytes: AtomicU64,
    path_drain_phase: Option<ReliablePathCommandDrainPhase>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReliablePathCommandDrainPhase {
    Retirement,
    Control,
    Priority,
    Reinjection,
    Data,
    Complete,
}

/// Live logical-flow load for one ordered carrier writer queue.
///
/// TCP multiplexed streams share this registration domain. Native QUIC streams
/// use separate writer queues, so independent QUIC streams do not create false
/// head-of-line pressure for each other.
#[derive(Debug, Clone)]
pub(in crate::runtime) struct ReliablePathLoadRegistration {
    inner: Arc<ReliablePathLoadRegistrationInner>,
}

#[derive(Debug)]
struct ReliablePathLoadRegistrationInner {
    metrics: Arc<ReliablePathCommandQueueMetrics>,
    lane: Mutex<Option<TrafficClass>>,
}

impl ReliablePathLoadRegistration {
    pub(in crate::runtime) fn set_lane(&self, lane: TrafficClass) {
        let mut current = self.lane();
        let Some(previous) = *current else {
            return;
        };
        if previous == lane {
            return;
        }
        self.inner.metrics.change_flow_lane(previous, lane);
        *current = Some(lane);
    }

    pub(in crate::runtime) fn deactivate(&self) {
        let mut current = self.lane();
        if let Some(lane) = current.take() {
            self.inner.metrics.release_flow(lane);
        }
    }

    fn lane(&self) -> std::sync::MutexGuard<'_, Option<TrafficClass>> {
        self.inner
            .lane
            .lock()
            .expect("reliable path load registration lock")
    }
}

impl Drop for ReliablePathLoadRegistrationInner {
    fn drop(&mut self) {
        let lane = self
            .lane
            .get_mut()
            .expect("reliable path load registration lock")
            .take();
        if let Some(lane) = lane {
            self.metrics.release_flow(lane);
        }
    }
}

/// Immutable queue readiness captured at an observe boundary. Policy may rank
/// this value, but only the command owner may resolve a sender and enqueue.
#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ReliablePathCommandQueueSnapshot {
    priority_ready: bool,
    reinjection_ready: bool,
    data_ready: bool,
}

impl ReliablePathCommandQueueSnapshot {
    fn queue_ready(self, lane: TrafficClass) -> bool {
        if reliable_path_frame_uses_priority_queue(lane) {
            self.priority_ready
        } else {
            self.data_ready
        }
    }

    pub(in crate::runtime) fn can_enqueue_lane(self, lane: TrafficClass) -> bool {
        self.queue_ready(lane)
    }

    pub(in crate::runtime) fn can_enqueue_frame(self, frame: &Frame, lane: TrafficClass) -> bool {
        let effective_lane = reliable_path_effective_frame_lane(frame, lane);
        self.queue_ready(effective_lane)
    }

    pub(in crate::runtime) fn can_enqueue_stream_ordered_frame(self) -> bool {
        self.can_enqueue_lane(reliable_path_stream_ordered_queue_lane())
    }

    pub(in crate::runtime) fn can_enqueue_reinjection_frame(self, frame: &Frame) -> bool {
        let _ = frame;
        self.reinjection_ready
    }
}

/// A carrier-owned terminal transaction for an accepted stream whose product
/// attachment never committed. It is intentionally separate from bounded work:
/// queue pressure must not leak a peer binding or its local actor entry. The
/// carrier's accepted-stream limit bounds outstanding retirements.
#[derive(Debug, Clone, Copy)]
enum ReliablePathRetirementCommand {
    RetireAcceptedStream(StreamId),
    RetireDatagramAttachment(u64),
}

/// Holds queue capacity without publishing a frame. Response transactions use
/// this to make their metadata visible before the carrier can dequeue work.
pub(in crate::runtime) struct ReliablePathFrameReservation<'a> {
    permit: Option<mpsc::Permit<'a, QueuedReliablePathCommand>>,
    frame: Option<Frame>,
    bytes: usize,
    metrics: Arc<ReliablePathCommandQueueMetrics>,
    #[cfg(feature = "lab-diagnostics")]
    lane: TrafficClass,
    #[cfg(feature = "lab-diagnostics")]
    effective_lane: TrafficClass,
    #[cfg(feature = "lab-diagnostics")]
    queue_name: &'static str,
}

impl ReliablePathFrameReservation<'_> {
    pub(in crate::runtime) fn commit(mut self) {
        #[cfg(feature = "lab-diagnostics")]
        let frame_kind =
            reliable_path_frame_kind(self.frame.as_ref().expect("reserved reliable path frame"));
        #[cfg(feature = "lab-diagnostics")]
        let stream_id = reliable_path_frame_stream_id(
            self.frame.as_ref().expect("reserved reliable path frame"),
        )
        .unwrap_or(StreamId(0));
        self.metrics.add_pending_bytes(self.bytes);
        self.permit
            .take()
            .expect("reserved reliable path queue permit")
            .send(QueuedReliablePathCommand::new(
                ReliablePathCommand::SendFrame(
                    self.frame.take().expect("reserved reliable path frame"),
                ),
                self.bytes,
                self.metrics.clone(),
            ));
        #[cfg(feature = "lab-diagnostics")]
        lab_diagnostic(
            "path_command_queue_send",
            format_args!(
                "queue={} frame_kind={} stream_id={} lane={:?} effective_lane={:?} pacing_bytes={} wait_ms=0 result=queued",
                self.queue_name,
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
    writer_pending_bytes: AtomicU64,
    /// Upper/lower 32 bits hold total and latency-sensitive live flows.
    flow_counts: AtomicU64,
    capacity_released: Arc<Notify>,
    tcp_capacity_probe: TcpCapacityProbeLeaseState,
    lifecycle: ReliablePathCarrierLifecycle,
}

#[derive(Debug, Default)]
struct TcpCapacityProbeLeaseState {
    active: AtomicBool,
    attempts: AtomicU8,
}

/// One exact carrier-instance fence for new application work.
///
/// The lifecycle check after queue reservation is the admission boundary.
/// Work whose reservation crossed it before retirement remains owned by the
/// ordered queue and is drained; later work is rejected and can migrate.
#[derive(Debug)]
struct ReliablePathCarrierLifecycle {
    phase: AtomicU8,
    changed: Notify,
}

const RELIABLE_PATH_CARRIER_ACTIVE: u8 = 0;
const RELIABLE_PATH_CARRIER_DRAINING: u8 = 1;
const RELIABLE_PATH_CARRIER_TERMINAL: u8 = 2;

impl Default for ReliablePathCarrierLifecycle {
    fn default() -> Self {
        Self {
            phase: AtomicU8::new(RELIABLE_PATH_CARRIER_ACTIVE),
            changed: Notify::new(),
        }
    }
}

impl ReliablePathCarrierLifecycle {
    fn is_active(&self) -> bool {
        self.phase.load(Ordering::Acquire) == RELIABLE_PATH_CARRIER_ACTIVE
    }

    fn begin_drain(&self) {
        if self
            .phase
            .compare_exchange(
                RELIABLE_PATH_CARRIER_ACTIVE,
                RELIABLE_PATH_CARRIER_DRAINING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            self.changed.notify_waiters();
        }
    }

    fn finish(&self) {
        if self
            .phase
            .swap(RELIABLE_PATH_CARRIER_TERMINAL, Ordering::AcqRel)
            != RELIABLE_PATH_CARRIER_TERMINAL
        {
            self.changed.notify_waiters();
        }
    }

    async fn wait_for_drain(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if !self.is_active() {
                return;
            }
            changed.await;
        }
    }
}

#[derive(Clone)]
pub(in crate::runtime) struct ReliablePathDrainSignal {
    metrics: Arc<ReliablePathCommandQueueMetrics>,
}

impl ReliablePathDrainSignal {
    pub(in crate::runtime) async fn wait(&self) {
        self.metrics.lifecycle.wait_for_drain().await;
    }
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

    fn command(&self) -> &ReliablePathCommand {
        self.command.as_ref().expect("queued reliable path command")
    }

    fn into_rejected_command(mut self) -> ReliablePathCommand {
        if self.accounted_bytes > 0 {
            self.metrics
                .release_accounted_bytes(self.accounted_bytes as u64);
        }
        self.accounted_bytes = 0;
        self.command.take().expect("queued reliable path command")
    }
}

impl Drop for QueuedReliablePathCommand {
    fn drop(&mut self) {
        if self.accounted_bytes > 0 {
            self.metrics
                .release_accounted_bytes(self.accounted_bytes as u64);
        }
    }
}

impl ReliablePathCommandQueueMetrics {
    fn update_flow_counts(&self, update: impl Fn(u32, u32) -> (u32, u32)) {
        let _ = self
            .flow_counts
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                let active = (current >> 32) as u32;
                let latency = current as u32;
                let (active, latency) = update(active, latency);
                Some((u64::from(active) << 32) | u64::from(latency.min(active)))
            });
        self.capacity_released.notify_waiters();
    }

    fn register_flow(&self, lane: TrafficClass) {
        self.update_flow_counts(|active, latency| {
            (
                active.saturating_add(1),
                latency.saturating_add(u32::from(lane.is_latency_sensitive())),
            )
        });
    }

    fn release_flow(&self, lane: TrafficClass) {
        self.update_flow_counts(|active, latency| {
            (
                active.saturating_sub(1),
                latency.saturating_sub(u32::from(lane.is_latency_sensitive())),
            )
        });
    }

    fn change_flow_lane(&self, previous: TrafficClass, lane: TrafficClass) {
        self.update_flow_counts(|active, latency| {
            let latency = match (previous.is_latency_sensitive(), lane.is_latency_sensitive()) {
                (true, false) => latency.saturating_sub(1),
                (false, true) => latency.saturating_add(1),
                _ => latency,
            };
            (active, latency)
        });
    }

    fn flow_counts(&self) -> (u32, u32) {
        let counts = self.flow_counts.load(Ordering::Acquire);
        ((counts >> 32) as u32, counts as u32)
    }

    fn add_pending_bytes(&self, bytes: usize) {
        self.pending_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    fn release_accounted_bytes(&self, pending_bytes: u64) {
        if pending_bytes > 0 {
            let _ =
                self.pending_bytes
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                        Some(current.saturating_sub(pending_bytes))
                    });
        }
        self.capacity_released.notify_waiters();
    }

    fn add_writer_pending_bytes(&self, bytes: u64) {
        self.writer_pending_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }

    fn release_writer_pending_bytes(&self, bytes: u64) {
        if bytes > 0 {
            let _ = self.writer_pending_bytes.fetch_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |current| Some(current.saturating_sub(bytes)),
            );
        }
    }

    #[cfg_attr(not(any(test, feature = "lab-diagnostics")), allow(dead_code))]
    fn pending_bytes(&self) -> u64 {
        self.pending_bytes.load(Ordering::Relaxed)
    }

    fn writer_pending_bytes(&self) -> u64 {
        self.writer_pending_bytes.load(Ordering::Relaxed)
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
    pub(in crate::runtime) fn path_drain_signal(&self) -> ReliablePathDrainSignal {
        ReliablePathDrainSignal {
            metrics: self.metrics.clone(),
        }
    }

    /// Stops new admission while preserving every already reserved command.
    ///
    /// Tokio keeps a closed channel alive until outstanding permits resolve.
    /// `recv_reliable_path_command_during_drain` therefore remains the sole
    /// terminal test after this operation.
    pub(in crate::runtime) fn close_for_path_drain(&mut self) {
        if self.path_drain_phase.is_some() {
            return;
        }
        self.metrics.lifecycle.begin_drain();
        self.retirement.close();
        self.control.close();
        self.priority.close();
        self.reinjection.close();
        self.data.close();
        self.path_drain_phase = Some(ReliablePathCommandDrainPhase::Retirement);
    }

    fn take_queued_command(&self, command: QueuedReliablePathCommand) -> ReliablePathCommand {
        let (command, accounted_bytes) = command.into_parts();
        self.dequeued_unreleased_bytes
            .fetch_add(accounted_bytes as u64, Ordering::Relaxed);
        self.metrics
            .add_writer_pending_bytes(accounted_bytes as u64);
        command
    }

    fn take_live_queued_command(
        &mut self,
        queued: QueuedReliablePathCommand,
    ) -> Option<ReliablePathCommand> {
        if reliable_path_command_stream_id(queued.command())
            .is_some_and(|stream_id| self.closed_streams.contains(&stream_id))
        {
            // The envelope owns queue bytes until a writer accepts it, so
            // dropping the stale command reconciles the charge.
            return None;
        }
        let command = self.take_queued_command(queued);
        self.record_terminal_command(&command);
        Some(command)
    }

    fn record_terminal_command(&mut self, command: &ReliablePathCommand) {
        match command {
            ReliablePathCommand::ResetAndCloseStream { stream_id, .. }
            | ReliablePathCommand::CloseStream(stream_id) => {
                self.closed_streams.insert(*stream_id);
            }
            _ => {}
        }
    }

    pub(in crate::runtime) fn release_pending_command_bytes(&self, bytes: usize) {
        let requested_pending = bytes as u64;
        let previous_pending = self
            .dequeued_unreleased_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(requested_pending))
            })
            .expect("dequeued byte update always succeeds");
        self.metrics
            .release_writer_pending_bytes(previous_pending.min(requested_pending));
        self.metrics
            .release_accounted_bytes(previous_pending.min(requested_pending));
    }

    #[cfg(feature = "lab-diagnostics")]
    pub(in crate::runtime) fn pending_bytes(&self) -> u64 {
        self.metrics.pending_bytes()
    }
}

impl Drop for ReliablePathCommandReceivers {
    fn drop(&mut self) {
        self.metrics.lifecycle.finish();
        // Queued envelopes reconcile themselves. This covers a command already
        // removed from mpsc when a writer exits through an async error path.
        let outstanding = self.dequeued_unreleased_bytes.swap(0, Ordering::Relaxed);
        self.metrics.release_writer_pending_bytes(outstanding);
        self.metrics.release_accounted_bytes(outstanding);
    }
}

impl ReliablePathCommandSender {
    /// Stops fresh application work at the carrier-instance boundary without
    /// preventing ordered control needed to settle work already admitted.
    pub(in crate::runtime) fn begin_path_drain(&self) {
        self.metrics.lifecycle.begin_drain();
        self.metrics.capacity_released.notify_waiters();
    }

    pub(in crate::runtime) fn register_flow(
        &self,
        lane: TrafficClass,
    ) -> ReliablePathLoadRegistration {
        self.metrics.register_flow(lane);
        ReliablePathLoadRegistration {
            inner: Arc::new(ReliablePathLoadRegistrationInner {
                metrics: self.metrics.clone(),
                lane: Mutex::new(Some(lane)),
            }),
        }
    }

    pub(in crate::runtime) fn active_flow_counts(&self) -> (u32, u32) {
        self.metrics.flow_counts()
    }

    pub(in crate::runtime) fn queue_snapshot(&self) -> ReliablePathCommandQueueSnapshot {
        let product_open = self.metrics.lifecycle.is_active();
        let priority_open = !self.priority.is_closed();
        let data_open = !self.data.is_closed();
        ReliablePathCommandQueueSnapshot {
            priority_ready: product_open && priority_open && self.priority.capacity() > 0,
            reinjection_ready: product_open
                && !self.reinjection.is_closed()
                && self.reinjection.capacity() > 0,
            data_ready: product_open && data_open && self.data.capacity() > 0,
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

    pub(in crate::runtime) fn retire_datagram_attachment(
        &self,
        attachment_id: u64,
    ) -> Result<(), RuntimeError> {
        self.retirement
            .send(ReliablePathRetirementCommand::RetireDatagramAttachment(
                attachment_id,
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
        let stream_id = reliable_path_command_stream_id(&command).unwrap_or(StreamId(0));
        #[cfg(feature = "lab-diagnostics")]
        let started = Instant::now();
        let requires_product_admission = reliable_path_command_requires_product_admission(&command);
        let result = match self.control.reserve().await {
            Ok(permit) if !requires_product_admission || self.metrics.lifecycle.is_active() => {
                permit.send(QueuedReliablePathCommand::new(
                    command,
                    0,
                    self.metrics.clone(),
                ));
                Ok(())
            }
            Ok(_) | Err(_) => Err(mpsc::error::SendError(command)),
        };
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

    pub(in crate::runtime) async fn send_datagram_frame(
        &self,
        attachment_id: u64,
        frame: Frame,
        write_deadline: tokio::time::Instant,
        expires_at: Option<tokio::time::Instant>,
        response: tokio::sync::oneshot::Sender<Result<(), RuntimeError>>,
    ) -> Result<(), mpsc::error::SendError<ReliablePathCommand>> {
        let command = ReliablePathCommand::SendDatagramFrame {
            attachment_id,
            frame,
            write_deadline,
            expires_at,
            response,
        };
        let pending_bytes = reliable_path_command_pending_bytes(&command);
        let requires_product_admission = reliable_path_command_requires_product_admission(&command);
        self.metrics.add_pending_bytes(pending_bytes);
        let queued = QueuedReliablePathCommand::new(command, pending_bytes, self.metrics.clone());
        let permit = match self.priority.reserve().await {
            Ok(permit) => permit,
            Err(_) => {
                return Err(mpsc::error::SendError(queued.into_rejected_command()));
            }
        };
        if requires_product_admission && !self.metrics.lifecycle.is_active() {
            return Err(mpsc::error::SendError(queued.into_rejected_command()));
        }
        permit.send(queued);
        Ok(())
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
        let stream_id = reliable_path_command_stream_id(&command).unwrap_or(StreamId(0));
        #[cfg(feature = "lab-diagnostics")]
        let ordered_lane = reliable_path_stream_ordered_queue_lane();
        let queue = &self.data;
        #[cfg(feature = "lab-diagnostics")]
        let started = Instant::now();
        let requires_product_admission = reliable_path_command_requires_product_admission(&command);
        self.metrics.add_pending_bytes(pending_bytes);
        let queued = QueuedReliablePathCommand::new(command, pending_bytes, self.metrics.clone());
        let result = match queue.reserve().await {
            Ok(permit) if !requires_product_admission || self.metrics.lifecycle.is_active() => {
                permit.send(queued);
                Ok(())
            }
            Ok(_) | Err(_) => Err(mpsc::error::SendError(queued.into_rejected_command())),
        };
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
        let reservation = self.try_reserve_admitted_frame(frame, lane)?;
        reservation.commit();
        Ok(())
    }

    /// Reserves the frame's traffic-class queue before a response transaction
    /// publishes its matching Data Sequence ownership.
    pub(in crate::runtime) fn try_reserve_admitted_frame(
        &self,
        frame: Frame,
        lane: TrafficClass,
    ) -> Result<ReliablePathFrameReservation<'_>, RuntimeError> {
        self.try_reserve_admitted_frame_with_effective_lane(frame, lane, None)
    }

    /// Waits for bounded queue capacity without extending the product deadline.
    /// Registering the wakeup before each admission attempt prevents a queue
    /// release from being lost between the two.
    #[cfg(test)]
    pub(in crate::runtime) async fn enqueue_admitted_frame_until(
        &self,
        frame: Frame,
        lane: TrafficClass,
        deadline: tokio::time::Instant,
    ) -> Result<(), RuntimeError> {
        loop {
            let mut capacity_wait = Box::pin(self.capacity_notify().notified_owned());
            capacity_wait.as_mut().enable();

            match self.try_enqueue_admitted_frame(frame.clone(), lane) {
                Ok(()) => return Ok(()),
                Err(RuntimeError::SenderServiceBlocked) => {}
                Err(error) => return Err(error),
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(RuntimeError::SenderServiceBlocked);
            }

            tokio::select! {
                _ = capacity_wait => {}
                _ = tokio::time::sleep_until(deadline) => {
                    return Err(RuntimeError::SenderServiceBlocked);
                }
            }
        }
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
                if !self.metrics.lifecycle.is_active() {
                    return Err(RuntimeError::ReliablePathSessionClosed);
                }
                return Err(RuntimeError::SenderServiceBlocked);
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                return Err(RuntimeError::ReliablePathSessionClosed);
            }
        };
        if !self.metrics.lifecycle.is_active() {
            drop(permit);
            return Err(RuntimeError::ReliablePathSessionClosed);
        }
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

    /// Reserves carrier work for a repeated Data Sequence range. Reinjection is
    /// drained after latency/control traffic but before fresh bulk data.
    pub(in crate::runtime) fn try_reserve_reinjection_frame(
        &self,
        frame: Frame,
        lane: TrafficClass,
    ) -> Result<ReliablePathFrameReservation<'_>, RuntimeError> {
        let effective_lane = reliable_path_effective_frame_lane(&frame, lane);
        self.try_reserve_frame_on_queue(
            frame,
            lane,
            effective_lane,
            &self.reinjection,
            "reinjection",
        )
    }

    #[cfg(test)]
    pub(in crate::runtime) fn try_enqueue_reinjection_frame(
        &self,
        frame: Frame,
        lane: TrafficClass,
    ) -> Result<(), RuntimeError> {
        let reservation = self.try_reserve_reinjection_frame(frame, lane)?;
        reservation.commit();
        Ok(())
    }

    fn try_reserve_admitted_frame_with_effective_lane(
        &self,
        frame: Frame,
        lane: TrafficClass,
        effective_lane_override: Option<TrafficClass>,
    ) -> Result<ReliablePathFrameReservation<'_>, RuntimeError> {
        let effective_lane = effective_lane_override
            .unwrap_or_else(|| reliable_path_effective_frame_lane(&frame, lane));
        let (queue, queue_name) = if reliable_path_frame_uses_priority_queue(effective_lane) {
            (&self.priority, "priority")
        } else {
            (&self.data, "data")
        };
        self.try_reserve_frame_on_queue(frame, lane, effective_lane, queue, queue_name)
    }

    fn try_reserve_frame_on_queue<'a>(
        &'a self,
        frame: Frame,
        lane: TrafficClass,
        effective_lane: TrafficClass,
        queue: &'a mpsc::Sender<QueuedReliablePathCommand>,
        queue_name: &'static str,
    ) -> Result<ReliablePathFrameReservation<'a>, RuntimeError> {
        #[cfg(not(feature = "lab-diagnostics"))]
        let _ = (lane, effective_lane, queue_name);
        if reliable_path_frame_requires_capacity_command(&frame) {
            return Err(RuntimeError::Protocol(
                "PATH_CAPACITY_* requires an explicit typed carrier command",
            ));
        }
        let requires_product_admission = reliable_path_frame_requires_product_admission(&frame);
        let bytes = reliable_path_frame_pacing_bytes(&frame);
        #[cfg(feature = "lab-diagnostics")]
        let frame_kind = reliable_path_frame_kind(&frame);
        #[cfg(feature = "lab-diagnostics")]
        let stream_id = reliable_path_frame_stream_id(&frame).unwrap_or(StreamId(0));
        let permit = match queue.try_reserve() {
            Ok(permit) => permit,
            Err(err) => {
                let error = match err {
                    tokio::sync::mpsc::error::TrySendError::Full(_) => {
                        if requires_product_admission && !self.metrics.lifecycle.is_active() {
                            RuntimeError::ReliablePathSessionClosed
                        } else {
                            RuntimeError::SenderServiceBlocked
                        }
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
                        queue_name,
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
        if requires_product_admission && !self.metrics.lifecycle.is_active() {
            drop(permit);
            return Err(RuntimeError::ReliablePathSessionClosed);
        }
        Ok(ReliablePathFrameReservation {
            permit: Some(permit),
            frame: Some(frame),
            bytes,
            metrics: self.metrics.clone(),
            #[cfg(feature = "lab-diagnostics")]
            lane,
            #[cfg(feature = "lab-diagnostics")]
            effective_lane,
            #[cfg(feature = "lab-diagnostics")]
            queue_name,
        })
    }

    pub(in crate::runtime) fn can_enqueue_frame_now(
        &self,
        frame: &Frame,
        lane: TrafficClass,
    ) -> bool {
        self.queue_snapshot().can_enqueue_frame(frame, lane)
    }

    pub(in crate::runtime) fn can_enqueue_stream_ordered_frame_now(
        &self,
        lane: TrafficClass,
    ) -> bool {
        let _ = lane;
        self.can_enqueue_lane_now(reliable_path_stream_ordered_queue_lane())
    }

    pub(in crate::runtime) fn can_enqueue_reinjection_frame_now(&self, frame: &Frame) -> bool {
        self.queue_snapshot().can_enqueue_reinjection_frame(frame)
    }

    pub(in crate::runtime) fn can_enqueue_lane_now(&self, lane: TrafficClass) -> bool {
        self.queue_snapshot().can_enqueue_lane(lane)
    }

    pub(in crate::runtime) fn capacity_notify(&self) -> Arc<Notify> {
        self.metrics.capacity_released.clone()
    }

    pub(in crate::runtime) fn capacity_notifies(&self) -> Vec<Arc<Notify>> {
        vec![self.capacity_notify()]
    }

    #[cfg_attr(not(any(test, feature = "lab-diagnostics")), allow(dead_code))]
    pub(in crate::runtime) fn pending_bytes(&self) -> u64 {
        self.metrics.pending_bytes()
    }

    /// Bytes removed from the bounded queues but not yet released by the
    /// carrier writer. Queued fresh data is excluded because repair may still
    /// precede it in the priority queues.
    pub(in crate::runtime) fn writer_pending_bytes(&self) -> u64 {
        self.metrics.writer_pending_bytes()
    }

    pub(in crate::runtime) fn is_closed(&self) -> bool {
        self.retirement.is_closed()
            && self.control.is_closed()
            && self.priority.is_closed()
            && self.reinjection.is_closed()
            && self.data.is_closed()
    }

    pub(in crate::runtime) fn same_channel(&self, other: &Self) -> bool {
        self.retirement.same_channel(&other.retirement)
            && self.control.same_channel(&other.control)
            && self.priority.same_channel(&other.priority)
            && self.reinjection.same_channel(&other.reinjection)
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

fn reliable_path_frame_requires_product_admission(frame: &Frame) -> bool {
    matches!(
        frame,
        Frame::OpenStream { .. }
            | Frame::StreamData { .. }
            | Frame::StreamFin { .. }
            | Frame::OpenDatagramFlow { .. }
            | Frame::DatagramData { .. }
            | Frame::PathProofData { .. }
            | Frame::PathProofAck { .. }
            | Frame::PathCapacityData { .. }
            | Frame::PathCapacityFinish { .. }
            | Frame::PathCapacityReceipt { .. }
    )
}

fn reliable_path_command_requires_product_admission(command: &ReliablePathCommand) -> bool {
    match command {
        ReliablePathCommand::PrepareConnection { .. }
        | ReliablePathCommand::OpenStream { .. }
        | ReliablePathCommand::OpenDatagramAttachment { .. }
        | ReliablePathCommand::OpenDatagramFlow { .. }
        | ReliablePathCommand::SendTcpCapacityProbe(_) => true,
        ReliablePathCommand::SendDatagramFrame { frame, .. }
        | ReliablePathCommand::SendFrame(frame) => {
            reliable_path_frame_requires_product_admission(frame)
        }
        ReliablePathCommand::CancelTcpOpen { .. }
        | ReliablePathCommand::CloseDatagramAttachment { .. }
        | ReliablePathCommand::ResetAndCloseStream { .. }
        | ReliablePathCommand::CloseStream(_) => false,
    }
}

pub(in crate::runtime) fn reliable_path_command_channels(
    queue: usize,
) -> (ReliablePathCommandSender, ReliablePathCommandReceivers) {
    let queue = queue.max(1);
    let (retirement_tx, retirement_rx) = mpsc::unbounded_channel();
    let (control_tx, control_rx) = mpsc::channel(queue);
    let (priority_tx, priority_rx) = mpsc::channel(queue);
    let reinjection_queue = reliable_path_priority_headroom_frames().min(queue).max(1);
    let (reinjection_tx, reinjection_rx) = mpsc::channel(reinjection_queue);
    let (data_tx, data_rx) = mpsc::channel(queue);
    let metrics = Arc::new(ReliablePathCommandQueueMetrics::default());
    (
        ReliablePathCommandSender {
            retirement: retirement_tx,
            control: control_tx,
            priority: priority_tx,
            reinjection: reinjection_tx,
            data: data_tx,
            metrics: metrics.clone(),
        },
        ReliablePathCommandReceivers {
            retirement: retirement_rx,
            pending_retirement_close: None,
            control: control_rx,
            priority: priority_rx,
            reinjection: reinjection_rx,
            data: data_rx,
            closed_streams: RecentIdCache::new(
                queue
                    .saturating_mul(reliable_path_writer_lane_count())
                    .saturating_add(1),
            ),
            metrics,
            dequeued_unreleased_bytes: AtomicU64::new(0),
            path_drain_phase: None,
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
        && !path_command_receiver_may_recv(&receivers.reinjection)
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
        let reinjection_may_recv = path_command_receiver_may_recv(&receivers.reinjection);
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
            command = receivers.reinjection.recv(), if reinjection_may_recv => {
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
                if let Some(command) = receivers.take_live_queued_command(command) {
                    return Some(command);
                }
            }
            ReceivedCommand::Retirement(None) | ReceivedCommand::Queued(None) => {}
        }
    }
}

/// Drains every command and every outstanding queue reservation after path
/// admission has been closed.
pub(in crate::runtime) async fn recv_reliable_path_command_during_drain(
    receivers: &mut ReliablePathCommandReceivers,
) -> Option<ReliablePathCommand> {
    loop {
        match receivers
            .path_drain_phase
            .expect("path drain must close command admission first")
        {
            ReliablePathCommandDrainPhase::Retirement => {
                if let Some(stream_id) = receivers.pending_retirement_close.take() {
                    let command = ReliablePathCommand::CloseStream(stream_id);
                    receivers.record_terminal_command(&command);
                    return Some(command);
                }
                match receivers.retirement.recv().await {
                    Some(command) => {
                        return Some(begin_reliable_path_retirement(receivers, command));
                    }
                    None => {
                        receivers.path_drain_phase = Some(ReliablePathCommandDrainPhase::Control);
                    }
                }
            }
            ReliablePathCommandDrainPhase::Control => match receivers.control.recv().await {
                Some(command) => {
                    if let Some(command) = receivers.take_live_queued_command(command) {
                        return Some(command);
                    }
                }
                None => {
                    receivers.path_drain_phase = Some(ReliablePathCommandDrainPhase::Priority);
                }
            },
            ReliablePathCommandDrainPhase::Priority => match receivers.priority.recv().await {
                Some(command) => {
                    if let Some(command) = receivers.take_live_queued_command(command) {
                        return Some(command);
                    }
                }
                None => {
                    receivers.path_drain_phase = Some(ReliablePathCommandDrainPhase::Reinjection);
                }
            },
            ReliablePathCommandDrainPhase::Reinjection => {
                match receivers.reinjection.recv().await {
                    Some(command) => {
                        if let Some(command) = receivers.take_live_queued_command(command) {
                            return Some(command);
                        }
                    }
                    None => {
                        receivers.path_drain_phase = Some(ReliablePathCommandDrainPhase::Data);
                    }
                }
            }
            ReliablePathCommandDrainPhase::Data => match receivers.data.recv().await {
                Some(command) => {
                    if let Some(command) = receivers.take_live_queued_command(command) {
                        return Some(command);
                    }
                }
                None => {
                    receivers.path_drain_phase = Some(ReliablePathCommandDrainPhase::Complete);
                }
            },
            ReliablePathCommandDrainPhase::Complete => return None,
        }
    }
}

pub(in crate::runtime) fn try_recv_reliable_path_command(
    receivers: &mut ReliablePathCommandReceivers,
) -> Option<ReliablePathCommand> {
    loop {
        if let Some(command) = recv_ready_priority_command(receivers) {
            return Some(command);
        }
        let queued = receivers.data.try_recv().ok()?;
        if let Some(command) = receivers.take_live_queued_command(queued) {
            return Some(command);
        }
    }
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
    loop {
        if let Some(stream_id) = receivers.pending_retirement_close.take() {
            let command = ReliablePathCommand::CloseStream(stream_id);
            receivers.record_terminal_command(&command);
            return Some(command);
        }
        if let Ok(command) = receivers.retirement.try_recv() {
            return Some(begin_reliable_path_retirement(receivers, command));
        }
        let queued = receivers
            .control
            .try_recv()
            .ok()
            .or_else(|| receivers.priority.try_recv().ok())
            .or_else(|| receivers.reinjection.try_recv().ok())?;
        if let Some(command) = receivers.take_live_queued_command(queued) {
            return Some(command);
        }
    }
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
        ReliablePathRetirementCommand::RetireDatagramAttachment(attachment_id) => {
            ReliablePathCommand::CloseDatagramAttachment {
                attachment_id,
                response: None,
            }
        }
    }
}

pub(in crate::runtime) fn reliable_path_command_pending_bytes(
    command: &ReliablePathCommand,
) -> usize {
    match command {
        ReliablePathCommand::SendFrame(frame)
        | ReliablePathCommand::SendDatagramFrame { frame, .. } => {
            reliable_path_frame_pacing_bytes(frame)
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
        | ReliablePathCommand::PrepareConnection { .. }
        | ReliablePathCommand::CancelTcpOpen { .. }
        | ReliablePathCommand::OpenDatagramAttachment { .. }
        | ReliablePathCommand::OpenDatagramFlow { .. }
        | ReliablePathCommand::CloseDatagramAttachment { .. }
        | ReliablePathCommand::CloseStream(_) => 0,
    }
}

pub(in crate::runtime) fn reliable_path_command_writer_run_bytes(
    command: &ReliablePathCommand,
) -> usize {
    match command {
        ReliablePathCommand::SendFrame(frame)
        | ReliablePathCommand::SendDatagramFrame { frame, .. } => {
            crate::protocol::codec::encoded_frame_capacity_hint(frame).max(1)
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
        | ReliablePathCommand::PrepareConnection { .. }
        | ReliablePathCommand::CancelTcpOpen { .. }
        | ReliablePathCommand::OpenDatagramAttachment { .. }
        | ReliablePathCommand::OpenDatagramFlow { .. }
        | ReliablePathCommand::CloseDatagramAttachment { .. }
        | ReliablePathCommand::CloseStream(_) => 1,
    }
}

fn reliable_path_command_stream_id(command: &ReliablePathCommand) -> Option<StreamId> {
    match command {
        ReliablePathCommand::SendFrame(frame) => reliable_path_frame_stream_id(frame),
        ReliablePathCommand::SendDatagramFrame { .. }
        | ReliablePathCommand::OpenDatagramAttachment { .. }
        | ReliablePathCommand::OpenDatagramFlow { .. }
        | ReliablePathCommand::CloseDatagramAttachment { .. } => None,
        ReliablePathCommand::SendTcpCapacityProbe(probe) => Some(probe.stream_id),
        ReliablePathCommand::PrepareConnection { .. } => None,
        ReliablePathCommand::OpenStream { stream_id, .. }
        | ReliablePathCommand::CancelTcpOpen { stream_id, .. }
        | ReliablePathCommand::ResetAndCloseStream { stream_id, .. }
        | ReliablePathCommand::CloseStream(stream_id) => Some(*stream_id),
    }
}

fn reliable_path_frame_stream_id(frame: &Frame) -> Option<StreamId> {
    match frame {
        Frame::OpenStream { stream_id, .. }
        | Frame::StreamData { stream_id, .. }
        | Frame::StreamAck { stream_id, .. }
        | Frame::StreamMaxData { stream_id, .. }
        | Frame::StreamFin { stream_id, .. }
        | Frame::StreamDetach { stream_id }
        | Frame::StreamReset { stream_id, .. } => Some(*stream_id),
        _ => None,
    }
}

#[cfg(feature = "lab-diagnostics")]
fn reliable_path_command_kind(command: &ReliablePathCommand) -> &'static str {
    match command {
        ReliablePathCommand::PrepareConnection { .. } => "prepare_connection",
        ReliablePathCommand::OpenStream { .. } => "open_stream",
        ReliablePathCommand::CancelTcpOpen { .. } => "cancel_tcp_open",
        ReliablePathCommand::OpenDatagramAttachment { .. } => "open_datagram_attachment",
        ReliablePathCommand::OpenDatagramFlow { .. } => "open_datagram_flow",
        ReliablePathCommand::SendDatagramFrame { frame, .. } => reliable_path_frame_kind(frame),
        ReliablePathCommand::CloseDatagramAttachment { .. } => "close_datagram_attachment",
        ReliablePathCommand::SendFrame(frame) => reliable_path_frame_kind(frame),
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
        Frame::TcpCarrierDemand { .. } => "tcp_carrier_demand",
        Frame::TcpCarrierValidate { .. } => "tcp_carrier_validate",
        Frame::TcpCarrierResult { .. } => "tcp_carrier_result",
        Frame::TcpCarrierResultAck { .. } => "tcp_carrier_result_ack",
    }
}

#[cfg(test)]
#[path = "queue_test.rs"]
mod tests;
