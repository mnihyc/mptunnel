use super::ack_clock_policy::{
    TcpAckClockCalibrationOpportunity, reliable_tcp_ack_clock_calibration_opportunity,
};
#[cfg(test)]
use super::ack_clock_policy::{
    reliable_ack_clock_calibration_ceiling_bytes, reliable_ack_clock_calibration_limit_bytes,
};
#[cfg(feature = "lab-diagnostics")]
use super::bulk_admission::BulkExplorationCompletionProjection;
use super::bulk_admission::{
    BulkAdmissionCheck, BulkAdmissionRole, bulk_active_service_product_envelope_bytes,
    bulk_additional_admission_role, bulk_candidate_admission_suppression_with_ordering_debt,
    bulk_latency_pressure_service_feed_window_bytes, bulk_service_feed_reservoir_payload_bytes,
    bulk_service_horizon_payload_bytes,
};
use super::response_ownership::{
    ResponseCandidateTailDebt, ResponseOrderedTail, ResponseTcpReservoir,
};
use super::*;

// Ownership boundary:
// Sender services own product work before it reaches carrier command queues.
// Client relay sending and server response dispatch both use this module for
// queueing, path ranking, reservation intents, and diagnostics. Reliable-path
// bindings own exact range flight and atomic commit; final TCP/UDP emission
// still happens through carrier command senders.

// Local diagnostic naming helper.  `response_admission` has a private helper
// with the same purpose, but sender_service is a sibling module and must not
// depend on that module-private symbol when `lab-diagnostics` is enabled.
#[cfg(feature = "lab-diagnostics")]
fn sender_service_frame_kind(frame: &Frame) -> &'static str {
    match frame {
        Frame::StreamData { .. } => "stream_data",
        Frame::StreamAck { .. } => "stream_ack",
        Frame::StreamMaxData { .. } => "stream_max_data",
        Frame::StreamFin { .. } => "stream_fin",
        Frame::StreamReset { .. } => "stream_reset",
        Frame::StreamDetach { .. } => "stream_detach",
        Frame::DatagramData { .. } => "datagram_data",
        Frame::DatagramFeedback { .. } => "datagram_feedback",
        Frame::DatagramClose { .. } => "datagram_close",
        _ => "control",
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ClientRepairOutputIdentity {
    instance: RelayPathInstance,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ServerRepairOutputIdentity {
    key: CarrierPathKey,
    incarnation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PersistentClientAckGapBatch {
    target: ClientRepairOutputIdentity,
    expires_at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PersistentServerAckGapBatch {
    target: ServerRepairOutputIdentity,
    expires_at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RelaySendCause {
    StreamData,
    StreamFin,
    RecvProgress,
    RecvProgressRecovery,
    AckGapRepair,
    PersistentAckGapRepair,
    PersistentClientAckGapRepair(PersistentClientAckGapBatch),
    PersistentServerAckGapRepair(PersistentServerAckGapBatch),
    LiveOwnerTailRepair,
    PathFailureRepair,
}

impl RelaySendCause {
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::StreamData => "stream_data",
            Self::StreamFin => "stream_fin",
            Self::RecvProgress => "recv_progress",
            Self::RecvProgressRecovery => "recv_progress_recovery",
            Self::AckGapRepair => "ack_gap_repair",
            Self::PersistentAckGapRepair
            | Self::PersistentClientAckGapRepair(_)
            | Self::PersistentServerAckGapRepair(_) => "persistent_ack_gap_repair",
            Self::LiveOwnerTailRepair => "live_owner_tail_repair",
            Self::PathFailureRepair => "path_failure_repair",
        }
    }

    fn is_repair(self) -> bool {
        matches!(
            self,
            Self::AckGapRepair
                | Self::PersistentAckGapRepair
                | Self::PersistentClientAckGapRepair(_)
                | Self::PersistentServerAckGapRepair(_)
                | Self::LiveOwnerTailRepair
                | Self::PathFailureRepair
        )
    }

    fn is_recv_progress(self) -> bool {
        matches!(self, Self::RecvProgress | Self::RecvProgressRecovery)
    }

    fn is_persistent_ack_gap_repair(self) -> bool {
        matches!(
            self,
            Self::PersistentAckGapRepair
                | Self::PersistentClientAckGapRepair(_)
                | Self::PersistentServerAckGapRepair(_)
        )
    }

    fn persistent_client_target(self) -> Option<RelayPathInstance> {
        match self {
            Self::PersistentClientAckGapRepair(batch) => Some(batch.target.instance),
            _ => None,
        }
    }

    fn persistent_server_target(self) -> Option<ServerRepairOutputIdentity> {
        match self {
            Self::PersistentServerAckGapRepair(batch) => Some(batch.target),
            _ => None,
        }
    }

    fn persistent_ack_gap_repair_expired(self, now: Instant) -> bool {
        match self {
            Self::PersistentClientAckGapRepair(batch) => now >= batch.expires_at,
            Self::PersistentServerAckGapRepair(batch) => now >= batch.expires_at,
            _ => false,
        }
    }

    fn persistent_ack_gap_repair_deadline(self) -> Option<Instant> {
        match self {
            Self::PersistentClientAckGapRepair(batch) => Some(batch.expires_at),
            Self::PersistentServerAckGapRepair(batch) => Some(batch.expires_at),
            _ => None,
        }
    }

    pub(super) fn persistent_client_ack_gap_repair(
        target: ClientRepairOutputIdentity,
        snapshot: PathSnapshot,
        lane: FlowLane,
    ) -> Self {
        Self::PersistentClientAckGapRepair(PersistentClientAckGapBatch {
            target,
            expires_at: Instant::now() + reliable_ack_gap_repair_delay(Some(snapshot), lane),
        })
    }

    pub(super) fn persistent_server_ack_gap_repair(
        target: ServerRepairOutputIdentity,
        snapshot: PathSnapshot,
        lane: FlowLane,
    ) -> Self {
        Self::PersistentServerAckGapRepair(PersistentServerAckGapBatch {
            target,
            expires_at: Instant::now() + reliable_ack_gap_repair_delay(Some(snapshot), lane),
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct RelaySendOutcome {
    #[cfg_attr(not(any(test, feature = "lab-diagnostics")), allow(dead_code))]
    pub(super) path_key: RelayPathKey,
}

#[derive(Debug)]
struct RelayPathSendSelection {
    position: usize,
    data_role: Option<PathRuntimeRole>,
    request_subflow_rollback: Option<Option<FlowSubflowSet<RelayPathInstance>>>,
    request_attempted_rollback: Option<RelayPathInstance>,
    request_calibration_rollback: Option<(RelayPathInstance, Option<u64>)>,
}

impl RelayPathSendSelection {
    fn new(position: usize, data_role: Option<PathRuntimeRole>) -> Self {
        Self {
            position,
            data_role,
            request_subflow_rollback: None,
            request_attempted_rollback: None,
            request_calibration_rollback: None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct RequestPathRateEvidence {
    pending_bytes: u64,
    pending_first_sent_at: Instant,
    pending_latest_sent_at: Instant,
    previous_window_acked_at: Option<Instant>,
}

enum RequestPathRateEvidenceUpdate {
    Pending,
    Proven {
        sample: Option<PathRateSample>,
        first_window: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RequestOwnerAckProgress {
    pub(super) instance: RelayPathInstance,
    pub(super) bytes: usize,
}

impl RequestPathRateEvidence {
    fn new(first_sent_at: Instant) -> Self {
        Self {
            pending_bytes: 0,
            pending_first_sent_at: first_sent_at,
            pending_latest_sent_at: first_sent_at,
            previous_window_acked_at: None,
        }
    }

    fn observe(
        &mut self,
        bytes: u64,
        first_sent_at: Instant,
        latest_sent_at: Instant,
        acked_at: Instant,
    ) -> RequestPathRateEvidenceUpdate {
        if self.pending_bytes == 0 {
            self.pending_first_sent_at = first_sent_at;
            self.pending_latest_sent_at = latest_sent_at;
        } else {
            self.pending_first_sent_at = self.pending_first_sent_at.min(first_sent_at);
            self.pending_latest_sent_at = self.pending_latest_sent_at.max(latest_sent_at);
        }
        self.pending_bytes = self.pending_bytes.saturating_add(bytes);
        if self.pending_bytes < PATH_OPEN_SCORE_BYTES as u64 {
            return RequestPathRateEvidenceUpdate::Pending;
        }

        let sample_bytes = self.pending_bytes;
        let first_window = self.previous_window_acked_at.is_none();
        let sample_started_at = self
            .previous_window_acked_at
            .unwrap_or(self.pending_first_sent_at);
        // A later window is ACK-clocked only when every sampled byte was already
        // in flight at the ACK that starts the interval.
        let ack_clocked = first_window || self.pending_latest_sent_at <= sample_started_at;
        self.pending_bytes = 0;
        self.previous_window_acked_at = Some(acked_at);
        let sample = ack_clocked
            .then(|| {
                PathRateSample::new(
                    sample_bytes,
                    acked_at.saturating_duration_since(sample_started_at),
                )
            })
            .flatten();
        RequestPathRateEvidenceUpdate::Proven {
            sample,
            first_window,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) enum ClientQueuedDispatch {
    Data { payload_bytes: usize },
    Repair { payload_bytes: usize },
    RepairDeferred,
    PersistentRepairCancelled,
}

#[derive(Debug)]
pub(super) struct RelaySenderService {
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    stream_id: StreamId,
    flights: RelayPathFlightLedger,
    ordered_data_owner: Option<RelayPathKey>,
    ordered_data_owner_instance: Option<RelayPathInstance>,
    request_subflow_set: Option<FlowSubflowSet<RelayPathInstance>>,
    request_startup_acked_bytes: HashMap<RelayPathInstance, u64>,
    request_startup_first_sent_at: HashMap<RelayPathInstance, Instant>,
    request_startup_rate_evidence: HashSet<RelayPathInstance>,
    request_startup_receipt_proofs: HashMap<RelayPathInstance, (u64, u64)>,
    request_rate_evidence: HashMap<RelayPathInstance, RequestPathRateEvidence>,
    request_rate_proven_subflows: HashSet<RelayPathInstance>,
    request_ack_clock_proven_subflows: HashSet<RelayPathInstance>,
    request_ack_clock_calibration_bytes: HashMap<RelayPathInstance, u64>,
    request_graduated_subflows: HashSet<RelayPathInstance>,
    request_attempted_subflows: HashSet<RelayPathInstance>,
    request_membership_generation: Option<u64>,
    missing_owner_repair_attempts: HashMap<RelayPathInstance, Instant>,
    next_send_index: usize,
    performance: MppPerformanceConfig,
    extra_traffic: ExtraTrafficLedger,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct RelayRecvProgressSend {
    path: Option<PathSnapshot>,
    lane: FlowLane,
    force_max_data: bool,
    recover_stalled_service: bool,
}

impl RelayRecvProgressSend {
    pub(super) fn new(path: Option<PathSnapshot>, lane: FlowLane, force_max_data: bool) -> Self {
        Self {
            path,
            lane,
            force_max_data,
            recover_stalled_service: false,
        }
    }

    pub(super) fn recover_stalled_service(mut self) -> Self {
        self.recover_stalled_service = true;
        self
    }
}

#[derive(Debug, Clone)]
pub(super) enum ReliableRelayQueuedWorkKind {
    Control(Frame),
    Data(Bytes),
    Repair { frame: Frame, cause: RelaySendCause },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReliableRelayQueuedWorkLane {
    Control,
    Data,
    Repair,
}

#[derive(Debug, Clone)]
/// Byte-bounded queue for product reliable work awaiting sender admission.
///
/// This is above carrier paths: it is sized by product flow-control and repair
/// envelopes, not by TCP socket buffers or QUIC packet queues. Normal target
/// bytes remain raw bytes until dispatch, so the sender-service executor owns
/// the point where bytes become STREAM_DATA. Repair STREAM_DATA enters a
/// separate lane even though its wire frame kind is still STREAM_DATA.
pub(super) struct ReliableRelayQueuedWork {
    pub(super) kind: ReliableRelayQueuedWorkKind,
    pub(super) payload_bytes: usize,
    pub(super) data_lane: Option<FlowLane>,
    pub(super) stream_ordered_carrier_emit: bool,
    #[cfg(feature = "lab-diagnostics")]
    pub(super) enqueue_id: u64,
    #[cfg(feature = "lab-diagnostics")]
    pub(super) queued_at: Instant,
}

#[derive(Debug, Default)]
/// Lane staging queue used by the response sender service.
///
/// It owns queued product work and queue age before dispatch. Path command
/// queues must receive only already-admitted frames.
pub(super) struct ReliableRelaySenderQueue {
    control: VecDeque<ReliableRelayQueuedWork>,
    critical_repair: VecDeque<ReliableRelayQueuedWork>,
    repair: VecDeque<ReliableRelayQueuedWork>,
    data: VecDeque<ReliableRelayQueuedWork>,
    final_control: VecDeque<ReliableRelayQueuedWork>,
    bytes: usize,
    data_bytes: usize,
    #[cfg(feature = "lab-diagnostics")]
    next_enqueue_id: u64,
}

impl ReliableRelaySenderQueue {
    pub(super) fn is_empty(&self) -> bool {
        self.control.is_empty()
            && self.critical_repair.is_empty()
            && self.repair.is_empty()
            && self.data.is_empty()
            && self.final_control.is_empty()
    }

    pub(super) fn bytes(&self) -> usize {
        self.bytes
    }

    pub(super) fn data_bytes(&self) -> usize {
        self.data_bytes
    }

    pub(super) fn push_control(&mut self, frame: Frame) -> u64 {
        let payload_bytes = reliable_stream_frame_payload_bytes(&frame);
        self.push_work(
            ReliableRelayQueuedWorkLane::Control,
            ReliableRelayQueuedWorkKind::Control(frame),
            None,
            false,
            payload_bytes,
        )
    }

    pub(super) fn push_final_control(&mut self, frame: Frame) -> u64 {
        let payload_bytes = reliable_stream_frame_payload_bytes(&frame);
        self.push_work(
            ReliableRelayQueuedWorkLane::Control,
            ReliableRelayQueuedWorkKind::Control(frame),
            None,
            true,
            payload_bytes,
        )
    }

    pub(super) fn push_data(&mut self, payload: Bytes) -> u64 {
        self.push_data_for_lane(payload, FlowLane::Throughput)
    }

    pub(super) fn push_data_for_lane(&mut self, payload: Bytes, lane: FlowLane) -> u64 {
        let payload_bytes = payload.len();
        self.push_work(
            ReliableRelayQueuedWorkLane::Data,
            ReliableRelayQueuedWorkKind::Data(payload),
            Some(lane),
            false,
            payload_bytes,
        )
    }

    pub(super) fn push_repair(&mut self, frame: Frame) -> u64 {
        self.push_repair_with_cause(frame, RelaySendCause::AckGapRepair)
    }

    pub(super) fn push_repair_with_cause(&mut self, frame: Frame, cause: RelaySendCause) -> u64 {
        self.push_repair_with_priority(frame, cause, false)
    }

    pub(super) fn push_critical_repair_with_cause(
        &mut self,
        frame: Frame,
        cause: RelaySendCause,
    ) -> u64 {
        self.push_repair_with_priority(frame, cause, true)
    }

    fn push_repair_with_priority(
        &mut self,
        frame: Frame,
        cause: RelaySendCause,
        critical: bool,
    ) -> u64 {
        debug_assert!(cause.is_repair());
        let payload_bytes = reliable_stream_frame_payload_bytes(&frame);
        let enqueue_id = self.push_work(
            ReliableRelayQueuedWorkLane::Repair,
            ReliableRelayQueuedWorkKind::Repair { frame, cause },
            None,
            false,
            payload_bytes,
        );
        if critical {
            let work = self
                .repair
                .pop_back()
                .expect("newly pushed repair must exist");
            self.critical_repair.push_back(work);
        }
        enqueue_id
    }

    fn push_work(
        &mut self,
        lane: ReliableRelayQueuedWorkLane,
        kind: ReliableRelayQueuedWorkKind,
        data_lane: Option<FlowLane>,
        final_control: bool,
        payload_bytes: usize,
    ) -> u64 {
        #[cfg(feature = "lab-diagnostics")]
        let enqueue_id = {
            let enqueue_id = self.next_enqueue_id;
            self.next_enqueue_id = self.next_enqueue_id.saturating_add(1);
            enqueue_id
        };
        #[cfg(not(feature = "lab-diagnostics"))]
        let enqueue_id = 0;
        self.bytes = self.bytes.saturating_add(payload_bytes);
        if lane == ReliableRelayQueuedWorkLane::Data {
            self.data_bytes = self.data_bytes.saturating_add(payload_bytes);
        }
        let work = ReliableRelayQueuedWork {
            kind,
            payload_bytes,
            data_lane,
            stream_ordered_carrier_emit: final_control,
            #[cfg(feature = "lab-diagnostics")]
            enqueue_id,
            #[cfg(feature = "lab-diagnostics")]
            queued_at: Instant::now(),
        };
        match lane {
            ReliableRelayQueuedWorkLane::Control if final_control => {
                self.final_control.push_back(work);
            }
            ReliableRelayQueuedWorkLane::Control => self.control.push_back(work),
            ReliableRelayQueuedWorkLane::Data => self.data.push_back(work),
            ReliableRelayQueuedWorkLane::Repair => self.repair.push_back(work),
        }
        enqueue_id
    }

    pub(super) fn front(&self) -> Option<(ReliableRelayQueuedWorkLane, &ReliableRelayQueuedWork)> {
        if let Some(work) = self.control.front() {
            Some((ReliableRelayQueuedWorkLane::Control, work))
        } else if let Some(work) = self.critical_repair.front() {
            Some((ReliableRelayQueuedWorkLane::Repair, work))
        } else if let Some(work) = self.data.front() {
            Some((ReliableRelayQueuedWorkLane::Data, work))
        } else if let Some(work) = self.repair.front() {
            Some((ReliableRelayQueuedWorkLane::Repair, work))
        } else {
            self.final_control
                .front()
                .map(|work| (ReliableRelayQueuedWorkLane::Control, work))
        }
    }

    pub(super) fn front_lane(&self) -> Option<ReliableRelayQueuedWorkLane> {
        self.front().map(|(lane, _)| lane)
    }

    fn persistent_ack_gap_repair_deadline(&self) -> Option<Instant> {
        self.critical_repair
            .iter()
            .chain(self.repair.iter())
            .filter_map(|work| match &work.kind {
                ReliableRelayQueuedWorkKind::Repair { cause, .. } => {
                    cause.persistent_ack_gap_repair_deadline()
                }
                _ => None,
            })
            .min()
    }

    pub(super) fn has_queued_repair_overlap(&self, frame: &Frame) -> bool {
        let Some((start, end, _)) = reliable_stream_frame_extent(frame) else {
            return false;
        };
        self.critical_repair
            .iter()
            .chain(self.repair.iter())
            .any(|work| {
                let ReliableRelayQueuedWorkKind::Repair { frame: queued, .. } = &work.kind else {
                    return false;
                };
                let Some((queued_start, queued_end, _)) = reliable_stream_frame_extent(queued)
                else {
                    return false;
                };
                queued_start < end && start < queued_end
            })
    }

    pub(super) fn release_normalized_acked_repairs(&mut self, ranges: &[OffsetRange]) -> usize {
        if ranges.is_empty() {
            return 0;
        }
        let released = prune_acked_repair_queue(&mut self.critical_repair, ranges)
            .saturating_add(prune_acked_repair_queue(&mut self.repair, ranges));
        self.bytes = self.bytes.saturating_sub(released);
        released
    }

    pub(super) fn discard_unusable_live_owner_tail_repairs(
        &mut self,
        usable: impl Fn(&Frame) -> bool,
    ) -> usize {
        let released =
            discard_unusable_live_owner_tail_repair_queue(&mut self.critical_repair, &usable)
                .saturating_add(discard_unusable_live_owner_tail_repair_queue(
                    &mut self.repair,
                    &usable,
                ));
        self.bytes = self.bytes.saturating_sub(released);
        released
    }

    pub(super) fn discard_stale_persistent_ack_gap_repairs(
        &mut self,
        usable: impl Fn(RelaySendCause) -> bool,
    ) -> usize {
        let now = Instant::now();
        let released =
            discard_stale_persistent_ack_gap_repair_queue(&mut self.critical_repair, now, &usable)
                .saturating_add(discard_stale_persistent_ack_gap_repair_queue(
                    &mut self.repair,
                    now,
                    &usable,
                ));
        self.bytes = self.bytes.saturating_sub(released);
        released
    }

    fn discard_persistent_ack_gap_repair_batch(&mut self, cause: RelaySendCause) -> usize {
        if !matches!(
            cause,
            RelaySendCause::PersistentClientAckGapRepair(_)
                | RelaySendCause::PersistentServerAckGapRepair(_)
        ) {
            return 0;
        }
        let released =
            discard_persistent_ack_gap_repair_batch_from_queue(&mut self.critical_repair, cause)
                .saturating_add(discard_persistent_ack_gap_repair_batch_from_queue(
                    &mut self.repair,
                    cause,
                ));
        self.bytes = self.bytes.saturating_sub(released);
        released
    }

    pub(super) fn commit_front(
        &mut self,
    ) -> Option<(ReliableRelayQueuedWorkLane, ReliableRelayQueuedWork)> {
        let (lane, work) = if let Some(work) = self.control.pop_front() {
            (ReliableRelayQueuedWorkLane::Control, work)
        } else if let Some(work) = self.critical_repair.pop_front() {
            (ReliableRelayQueuedWorkLane::Repair, work)
        } else if let Some(work) = self.data.pop_front() {
            (ReliableRelayQueuedWorkLane::Data, work)
        } else if let Some(work) = self.repair.pop_front() {
            (ReliableRelayQueuedWorkLane::Repair, work)
        } else {
            (
                ReliableRelayQueuedWorkLane::Control,
                self.final_control.pop_front()?,
            )
        };
        self.bytes = self.bytes.saturating_sub(work.payload_bytes);
        if lane == ReliableRelayQueuedWorkLane::Data {
            self.data_bytes = self.data_bytes.saturating_sub(work.payload_bytes);
        }
        Some((lane, work))
    }

    fn commit_front_data_prefix(&mut self, prefix_len: usize) -> Option<ReliableRelayQueuedWork> {
        let work = self.data.front_mut()?;
        let ReliableRelayQueuedWorkKind::Data(payload) = &mut work.kind else {
            return None;
        };
        let prefix_len = prefix_len.min(payload.len()).max(1);
        if prefix_len >= payload.len() {
            let (_, work) = self.commit_front()?;
            return Some(work);
        }

        let prefix = payload.slice(..prefix_len);
        let remaining = payload.slice(prefix_len..);
        *payload = remaining;
        work.payload_bytes = work.payload_bytes.saturating_sub(prefix_len);
        self.bytes = self.bytes.saturating_sub(prefix_len);
        self.data_bytes = self.data_bytes.saturating_sub(prefix_len);

        Some(ReliableRelayQueuedWork {
            kind: ReliableRelayQueuedWorkKind::Data(prefix),
            payload_bytes: prefix_len,
            data_lane: work.data_lane,
            stream_ordered_carrier_emit: work.stream_ordered_carrier_emit,
            #[cfg(feature = "lab-diagnostics")]
            enqueue_id: work.enqueue_id,
            #[cfg(feature = "lab-diagnostics")]
            queued_at: work.queued_at,
        })
    }

    #[cfg(test)]
    pub(super) fn pop_front(
        &mut self,
    ) -> Option<(ReliableRelayQueuedWorkLane, ReliableRelayQueuedWork)> {
        self.commit_front()
    }
}

fn prune_acked_repair_queue(
    queue: &mut VecDeque<ReliableRelayQueuedWork>,
    ranges: &[OffsetRange],
) -> usize {
    let mut released = 0usize;
    let mut retained = VecDeque::with_capacity(queue.len());
    while let Some(work) = queue.pop_front() {
        let ReliableRelayQueuedWorkKind::Repair { frame, cause } = &work.kind else {
            retained.push_back(work);
            continue;
        };
        let slices = unacked_repair_frame_slices(frame, ranges);
        let retained_bytes = slices
            .iter()
            .map(reliable_stream_frame_payload_bytes)
            .sum::<usize>();
        released = released.saturating_add(work.payload_bytes.saturating_sub(retained_bytes));
        for frame in slices {
            let mut retained_work = work.clone();
            retained_work.payload_bytes = reliable_stream_frame_payload_bytes(&frame);
            retained_work.kind = ReliableRelayQueuedWorkKind::Repair {
                frame,
                cause: *cause,
            };
            retained.push_back(retained_work);
        }
    }
    *queue = retained;
    released
}

fn discard_unusable_live_owner_tail_repair_queue(
    queue: &mut VecDeque<ReliableRelayQueuedWork>,
    usable: &impl Fn(&Frame) -> bool,
) -> usize {
    let mut released = 0usize;
    queue.retain(|work| {
        let ReliableRelayQueuedWorkKind::Repair { frame, cause } = &work.kind else {
            return true;
        };
        let keep = *cause != RelaySendCause::LiveOwnerTailRepair || usable(frame);
        if !keep {
            released = released.saturating_add(work.payload_bytes);
        }
        keep
    });
    released
}

fn discard_stale_persistent_ack_gap_repair_queue(
    queue: &mut VecDeque<ReliableRelayQueuedWork>,
    now: Instant,
    usable: &impl Fn(RelaySendCause) -> bool,
) -> usize {
    let mut released = 0usize;
    queue.retain(|work| {
        let ReliableRelayQueuedWorkKind::Repair { cause, .. } = &work.kind else {
            return true;
        };
        let bound = matches!(
            cause,
            RelaySendCause::PersistentClientAckGapRepair(_)
                | RelaySendCause::PersistentServerAckGapRepair(_)
        );
        let keep = !bound || (!cause.persistent_ack_gap_repair_expired(now) && usable(*cause));
        if !keep {
            released = released.saturating_add(work.payload_bytes);
        }
        keep
    });
    released
}

fn discard_persistent_ack_gap_repair_batch_from_queue(
    queue: &mut VecDeque<ReliableRelayQueuedWork>,
    batch_cause: RelaySendCause,
) -> usize {
    let mut released = 0usize;
    queue.retain(|work| {
        let keep = !matches!(
            &work.kind,
            ReliableRelayQueuedWorkKind::Repair { cause, .. } if *cause == batch_cause
        );
        if !keep {
            released = released.saturating_add(work.payload_bytes);
        }
        keep
    });
    released
}

fn unacked_repair_frame_slices(frame: &Frame, ranges: &[OffsetRange]) -> Vec<Frame> {
    let Frame::StreamData {
        stream_id,
        offset,
        flags,
        payload,
    } = frame
    else {
        return vec![frame.clone()];
    };
    let frame_end = offset.saturating_add(payload.len() as u64);
    let mut remaining = vec![(*offset, frame_end)];
    for range in ranges {
        let mut next = Vec::with_capacity(remaining.len().saturating_add(1));
        for (start, end) in remaining {
            if range.end <= start || range.start >= end {
                next.push((start, end));
                continue;
            }
            if start < range.start {
                next.push((start, range.start.min(end)));
            }
            if range.end < end {
                next.push((range.end.max(start), end));
            }
        }
        remaining = next;
        if remaining.is_empty() {
            break;
        }
    }
    remaining
        .into_iter()
        .filter_map(|(start, end)| {
            let slice_start = usize::try_from(start.saturating_sub(*offset)).ok()?;
            let slice_end = usize::try_from(end.saturating_sub(*offset)).ok()?;
            (slice_start < slice_end && slice_end <= payload.len()).then(|| Frame::StreamData {
                stream_id: *stream_id,
                offset: start,
                flags: if end == frame_end {
                    *flags
                } else {
                    StreamFlags::NONE
                },
                payload: payload.slice(slice_start..slice_end),
            })
        })
        .collect()
}

pub(super) fn reliable_relay_sender_queue_limit(
    mux_limits: MuxLimits,
    inflight_limit: usize,
) -> usize {
    let stream_window = usize::try_from(mux_limits.max_stream_window_bytes).unwrap_or(usize::MAX);
    inflight_limit
        .max(reliable_relay_buffer_len(mux_limits))
        .min(mux_limits.max_repair_bytes)
        .min(stream_window)
        .min(mux_limits.max_path_flight_bytes)
        .max(1)
}

pub(super) fn reliable_relay_can_read_into_sender_queue(
    send_stream: &ReliableSendStream,
    sender_queue: &ReliableRelaySenderQueue,
    _mux_limits: MuxLimits,
    queue_limit: usize,
) -> bool {
    sender_queue.bytes() < queue_limit
        && sender_queue.data_bytes() < send_stream.send_credit_bytes()
}

pub(super) fn reliable_relay_can_read_product_source(
    local_open: bool,
    queued_send_blocked: bool,
    send_stream: &ReliableSendStream,
    sender_queue: &ReliableRelaySenderQueue,
    mux_limits: MuxLimits,
    queue_limit: usize,
) -> bool {
    local_open
        && !queued_send_blocked
        && reliable_relay_can_read_into_sender_queue(
            send_stream,
            sender_queue,
            mux_limits,
            queue_limit,
        )
}

pub(super) fn reliable_relay_sender_queue_read_budget(
    send_stream: &ReliableSendStream,
    sender_queue: &ReliableRelaySenderQueue,
    _mux_limits: MuxLimits,
    queue_limit: usize,
    buffer_len: usize,
) -> usize {
    queue_limit
        .saturating_sub(sender_queue.bytes())
        .min(
            send_stream
                .send_credit_bytes()
                .saturating_sub(sender_queue.data_bytes()),
        )
        .min(buffer_len)
}

#[derive(Debug)]
/// Current server response sender-service boundary.
///
/// Target reads enqueue STREAM_DATA here before any carrier path write. The
/// service owns queueing, path ranking, and reservation intents. The
/// `ReliablePathStream` binding revalidates and atomically commits exact ranges.
pub(super) struct ServerResponseSenderService {
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    session_id: SessionId,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    stream_id: StreamId,
    queue: ReliableRelaySenderQueue,
    performance: MppPerformanceConfig,
    extra_traffic: ExtraTrafficLedger,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ServerResponseDispatch {
    pub(super) payload_bytes: usize,
    pub(super) lane: ReliableRelayQueuedWorkLane,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(super) selected_path: Option<CarrierPathKey>,
}

fn carrier_path_key_order(left: CarrierPathKey, right: CarrierPathKey) -> std::cmp::Ordering {
    left.path_id.0.cmp(&right.path_id.0)
}

fn response_ordering_debt_bytes(
    lower_flights: &[CarrierPathFlightDebt],
    candidate: CarrierPathKey,
) -> u64 {
    lower_flights
        .iter()
        .filter_map(|flight| (flight.key != candidate).then_some(flight.bytes))
        .sum()
}

fn response_oldest_lower_flight_owner(
    lower_flights: &[CarrierPathFlightDebt],
) -> Option<CarrierPathKey> {
    lower_flights.first().map(|flight| flight.key)
}

#[derive(Debug, Clone, Copy)]
struct ResponseBulkLead {
    key: CarrierPathKey,
    snapshot: PathSnapshot,
    eta_ms: f64,
}

#[cfg(feature = "lab-diagnostics")]
#[derive(Debug, Clone, Copy)]
struct ResponseBulkCandidateDiag {
    lead: Option<ResponseBulkLead>,
    role: Option<BulkAdmissionRole>,
    ordering_debt: u64,
}

#[cfg(feature = "lab-diagnostics")]
fn lab_response_bulk_output_candidate(
    reason: &'static str,
    target: &ResponseSenderPathTarget,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    diag: ResponseBulkCandidateDiag,
) {
    if !lab_diagnostic_event_enabled("server_bulk_output_candidate") {
        return;
    }
    let (lead_underlay, lead_path_id, lead_eta_ms) = diag
        .lead
        .map(|lead| {
            (
                format!("{:?}", lead.key.underlay),
                lead.key.path_id.0.to_string(),
                lead.eta_ms,
            )
        })
        .unwrap_or_else(|| ("none".to_string(), "none".to_string(), 0.0));
    lab_diagnostic(
        "server_bulk_output_candidate",
        format_args!(
            "reason={} session_id={} binding_instance_id={} path_underlay={:?} path_id={} is_active={} sender_evidence={} bulk_rate_evidence={} role={} eta_ms={:.3} lead_underlay={} lead_path_id={} lead_eta_ms={:.3} stream_ordering_debt={} payload_bytes={} command_pending_bytes={} path_queue_bytes={} product_queue_bytes={} carrier_inflight_bytes={} product_inflight_bytes={} owner_data_inflight_bytes={} carrier_inflight_limit={} delivery_rate_mbps={:.3} pacing_mbps={:.3} srtt_ms={:.3} confidence={:.3} app_limited={} calibration_eligible={} calibration_proven={} calibration_active={} calibration_spent_bytes={} calibration_credit_bytes={} calibration_max_bytes={} mux_max_path_flight={} mux_max_reorder={}",
            reason,
            target.session_id.0,
            target.binding_instance_id,
            target.key.underlay,
            target.key.path_id.0,
            target.is_active,
            target.has_sender_evidence,
            target.has_bulk_rate_evidence,
            diag.role
                .map(|role| format!("{:?}", role))
                .unwrap_or_else(|| "none".to_string()),
            target.eta_ms,
            lead_underlay,
            lead_path_id,
            lead_eta_ms,
            diag.ordering_debt,
            payload_bytes,
            target.command_pending_bytes,
            target.snapshot.queue_bytes,
            target.snapshot.product_queue_bytes,
            target.snapshot.bytes_in_flight,
            target.snapshot.product_bytes_in_flight,
            target.owner_data_in_flight_bytes,
            target.snapshot.inflight_limit_bytes,
            target.snapshot.delivery_rate_bps / 1_000_000.0,
            target.snapshot.pacing_rate_bps / 1_000_000.0,
            target.snapshot.srtt_ms,
            target.snapshot.confidence,
            target.snapshot.app_limited,
            target.ack_clock_calibration_eligible,
            target.ack_clock_calibration_proven,
            target.ack_clock_calibration_active,
            target.ack_clock_calibration_spent_bytes,
            target.ack_clock_calibration_credit_limit_bytes,
            target.ack_clock_calibration_max_limit_bytes,
            mux_limits.max_path_flight_bytes,
            mux_limits.max_reorder_bytes,
        ),
    );
}

#[cfg(feature = "lab-diagnostics")]
fn lab_response_bulk_output_selected(
    reason: &'static str,
    selected: &ResponseSelectedDataTarget,
    payload_bytes: usize,
) {
    if !lab_diagnostic_event_enabled("server_bulk_output_selected") {
        return;
    }
    lab_diagnostic(
        "server_bulk_output_selected",
        format_args!(
            "reason={} session_id={} binding_instance_id={} path_underlay={:?} path_id={} role={:?} work={:?} payload_bytes={} command_pending_bytes={} product_inflight_bytes={} owner_data_inflight_bytes={} eta_ms={:.3} app_limited={} bulk_rate_evidence={} calibration_eligible={} calibration_proven={} calibration_active={} calibration_spent_bytes={} calibration_credit_bytes={} calibration_max_bytes={}",
            reason,
            selected.target.session_id.0,
            selected.target.binding_instance_id,
            selected.target.key.underlay,
            selected.target.key.path_id.0,
            selected.admission.role,
            selected.admission.work,
            payload_bytes,
            selected.target.command_pending_bytes,
            selected.target.snapshot.product_bytes_in_flight,
            selected.target.owner_data_in_flight_bytes,
            selected.target.eta_ms,
            selected.target.snapshot.app_limited,
            selected.target.has_bulk_rate_evidence,
            selected.target.ack_clock_calibration_eligible,
            selected.target.ack_clock_calibration_proven,
            selected.target.ack_clock_calibration_active,
            selected.target.ack_clock_calibration_spent_bytes,
            selected.target.ack_clock_calibration_credit_limit_bytes,
            selected.target.ack_clock_calibration_max_limit_bytes,
        ),
    );
}

#[cfg(feature = "lab-diagnostics")]
fn lab_response_ack_clock_calibration_admission(
    target: &ResponseSenderPathTarget,
    service: &ResponseSenderPathTarget,
    projection: BulkExplorationCompletionProjection,
    admitted: bool,
) {
    if !lab_diagnostic_event_enabled("response_ack_clock_calibration_admission") {
        return;
    }
    lab_diagnostic(
        "response_ack_clock_calibration_admission",
        format_args!(
            "session_id={} binding_instance_id={} path_underlay={:?} path_id={} service_underlay={:?} service_path_id={} admitted={} candidate_completion_ms={:.3} service_reservoir_horizon_ms={:.3} exploration_bytes={} service_followup_bytes={} candidate_eta_ms={:.3} service_eta_ms={:.3} candidate_rate_mbps={:.3} service_rate_mbps={:.3} candidate_srtt_ms={:.3} service_srtt_ms={:.3}",
            target.session_id.0,
            target.binding_instance_id,
            target.key.underlay,
            target.key.path_id.0,
            service.key.underlay,
            service.key.path_id.0,
            admitted,
            projection.candidate_completion_ms,
            projection.service_reservoir_horizon_ms,
            projection.exploration_bytes,
            projection.service_followup_bytes,
            target.eta_ms,
            service.eta_ms,
            target
                .snapshot
                .delivery_rate_bps
                .max(target.snapshot.pacing_rate_bps)
                / 1_000_000.0,
            service
                .snapshot
                .delivery_rate_bps
                .max(service.snapshot.pacing_rate_bps)
                / 1_000_000.0,
            target.snapshot.srtt_ms,
            service.snapshot.srtt_ms,
        ),
    );
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResponseCarrierEmitMode {
    Classified,
    StreamOrdered,
}

impl ResponseCarrierEmitMode {
    fn effective_lane(self, frame: &Frame, lane: FlowLane) -> FlowLane {
        match self {
            Self::Classified => reliable_path_effective_frame_lane(frame, lane),
            Self::StreamOrdered => reliable_path_stream_ordered_queue_lane(),
        }
    }
}

#[derive(Clone)]
enum ResponseDataDispatchTarget {
    Fixed(Arc<FixedReliablePathOutput>),
    Switchable {
        binding: Arc<ResponseStreamBinding>,
        target: ResponseSenderPathTarget,
        role: PathRuntimeRole,
        subflow_set_commit: Option<ResponseSubflowAdmissionCommit>,
        ack_clock_calibration_commit: Option<ResponseAckClockCalibrationCommit>,
    },
}

#[derive(Clone)]
struct ResponseDataDispatchPlan {
    primary: ResponseDataDispatchTarget,
}

impl ResponseDataDispatchPlan {
    #[cfg(test)]
    fn primary_key(&self) -> Option<CarrierPathKey> {
        match &self.primary {
            ResponseDataDispatchTarget::Fixed(fixed) => Some(fixed.key()),
            ResponseDataDispatchTarget::Switchable { target, .. } => Some(target.key),
        }
    }

    #[cfg(test)]
    fn primary_role(&self) -> PathRuntimeRole {
        match &self.primary {
            ResponseDataDispatchTarget::Fixed(_) => PathRuntimeRole::Service,
            ResponseDataDispatchTarget::Switchable { role, .. } => *role,
        }
    }
}

struct ResponseDataEmitOutcome {
    selected_path: Option<CarrierPathKey>,
}

#[derive(Clone, Copy)]
struct ResponseSubflowAdmissionCommit {
    planner_generation: u64,
    lane_generation: u64,
    service: CarrierPathKey,
    startup_owner_credit_bytes: usize,
    optional_overhead_budget_bytes: usize,
    max_read_gap_budget: Duration,
    input: SubflowAdmissionInput,
}

#[derive(Clone, Copy)]
struct ResponseAckClockCalibrationCommit {
    planner_generation: u64,
    lane_generation: u64,
    model_generation: u64,
    service: CarrierPathKey,
    service_incarnation: u64,
    service_pending_bytes: u64,
    target_pending_bytes: u64,
    limit_bytes: u64,
    requires_multi_flow_start: bool,
}

#[derive(Clone, Copy)]
struct ResponseAckClockCalibrationRetirementIntent {
    planner_generation: u64,
    lane_generation: u64,
    model_generation: u64,
    service: CarrierPathKey,
    service_incarnation: u64,
    service_pending_bytes: u64,
    target: CarrierPathKey,
    target_incarnation: u64,
    target_pending_bytes: u64,
    limit_bytes: u64,
}

#[derive(Clone)]
struct ResponseSelectedDataTarget {
    target: ResponseSenderPathTarget,
    admission: PathAdmission,
    subflow_set_commit: Option<ResponseSubflowAdmissionCommit>,
    ack_clock_calibration_commit: Option<ResponseAckClockCalibrationCommit>,
}

fn response_bulk_admission_role(
    service_key: CarrierPathKey,
    candidate: CarrierPathKey,
    lower_owner: Option<CarrierPathKey>,
    ordering_debt: u64,
) -> BulkAdmissionRole {
    if candidate == service_key && ordering_debt == 0 {
        BulkAdmissionRole::ActiveDataPath
    } else if let Some(owner) = lower_owner {
        // Continuing the existing lower-flight carrier does not introduce a
        // new carrier-family transition, even when Service uses another
        // underlay family. Runtime ownership still remains Subflow below.
        bulk_additional_admission_role(owner.underlay, candidate.underlay)
    } else {
        bulk_additional_admission_role(service_key.underlay, candidate.underlay)
    }
}

fn response_service_anchor_key(
    candidates: &[&ResponseSenderPathTarget],
    lower_owner: Option<CarrierPathKey>,
    ordered_data_owner: Option<CarrierPathKey>,
    fallback: CarrierPathKey,
) -> CarrierPathKey {
    ordered_data_owner
        .or_else(|| {
            candidates
                .iter()
                .copied()
                .find(|candidate| candidate.is_active)
                .map(|candidate| candidate.key)
        })
        .or(lower_owner)
        .unwrap_or(fallback)
}

fn response_unique_quic_data_would_expand_ordering_debt(
    lower_owner: Option<CarrierPathKey>,
    target: &ResponseSenderPathTarget,
    ordering_debt: u64,
) -> bool {
    matches!(
        lower_owner,
        Some(owner)
            if owner != target.key
                && owner.underlay == UnderlayProtocol::Udp
                && target.key.underlay == UnderlayProtocol::Udp
                && ordering_debt > 0
                && !target.has_bulk_rate_evidence
    )
}

fn response_target_has_service_anchor_rights(target: &ResponseSenderPathTarget) -> bool {
    target.is_active
}

fn response_target_is_plausible_unique_owner_candidate(target: &ResponseSenderPathTarget) -> bool {
    target.attachment_role != StreamOpenRole::Repair
        && (response_target_has_service_anchor_rights(target) || target.has_bulk_rate_evidence)
}

fn response_target_is_measured_same_underlay_subflow_candidate(
    service_key: CarrierPathKey,
    target: &ResponseSenderPathTarget,
) -> bool {
    target.attachment_role != StreamOpenRole::Repair
        && target.key != service_key
        && target.key.underlay == service_key.underlay
        && !target.is_active
        && target.has_bulk_rate_evidence
}

fn response_target_measured_admission_snapshot(target: &ResponseSenderPathTarget) -> PathSnapshot {
    let mut snapshot = target.snapshot;
    if target.has_bulk_rate_evidence {
        // An app-limited poll does not erase the retained path-scoped rate
        // model. Proven Subflows must continue to pass ECF completion math.
        snapshot.app_limited = false;
    }
    snapshot
}

fn response_target_is_startup_same_underlay_subflow_candidate(
    service_key: CarrierPathKey,
    service: &ResponseSenderPathTarget,
    target: &ResponseSenderPathTarget,
    ordered_tail_debt: u64,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> bool {
    let product_envelope = (mux_limits.max_path_flight_bytes as u64)
        .min(mux_limits.max_repair_bytes as u64)
        .min(mux_limits.max_reorder_bytes as u64)
        .min(mux_limits.max_stream_window_bytes)
        .max(payload_bytes as u64);
    let candidate_committed = response_target_assigned_product_bytes(target);
    // The ordered tail spans all unacknowledged product offsets, including
    // this candidate's assigned flight. The path snapshot is a fallback view.
    let projected_ordering_debt = ordered_tail_debt
        .max(candidate_committed)
        .saturating_add(payload_bytes as u64);

    service.key == service_key
        && service.is_active
        && service.has_bulk_rate_evidence
        && service.snapshot.active_latency_sensitive_flows == 0
        && service.snapshot.session_active_latency_sensitive_flows == 0
        && target.snapshot.active_latency_sensitive_flows == 0
        && target.snapshot.session_active_latency_sensitive_flows == 0
        && target.attachment_role == StreamOpenRole::Validation
        && target.key != service_key
        && target.key.underlay == service_key.underlay
        && !target.is_active
        && target.has_sender_evidence
        && !target.has_bulk_rate_evidence
        && projected_ordering_debt <= product_envelope
}

fn select_response_ack_clock_calibration_target(
    all_targets: &[ResponseSenderPathTarget],
    targets: &[&ResponseSenderPathTarget],
    lane: FlowLane,
    service_key: CarrierPathKey,
    ordered_owner_debt_bytes: usize,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    lower_flights: &[CarrierPathFlightDebt],
    subflow_set: Option<&FlowSubflowSet>,
    may_start_fresh_calibration: bool,
    retirement_intents: &mut Vec<ResponseAckClockCalibrationRetirementIntent>,
) -> Option<ResponseSelectedDataTarget> {
    if !lower_flights.is_empty()
        || subflow_set
            .and_then(FlowSubflowSet::startup_owner_key)
            .is_some()
    {
        return None;
    }
    let service = targets
        .iter()
        .copied()
        .find(|target| target.key == service_key)?;
    if !service.is_active
        || !service.has_bulk_rate_evidence
        || service.key.underlay != UnderlayProtocol::Tcp
        || service.snapshot.active_latency_sensitive_flows > 0
        || service.snapshot.session_active_latency_sensitive_flows > 0
    {
        return None;
    }

    let product_envelope = (mux_limits.max_path_flight_bytes as u64)
        .min(mux_limits.max_repair_bytes as u64)
        .min(mux_limits.max_reorder_bytes as u64)
        .min(mux_limits.max_stream_window_bytes)
        .max(payload_bytes as u64);
    let active_identity = all_targets
        .iter()
        .find(|target| target.ack_clock_calibration_active)
        .map(|target| (target.key, target.incarnation));

    targets
        .iter()
        .copied()
        .filter(|target| {
            active_identity.is_none_or(|identity| identity == (target.key, target.incarnation))
        })
        .filter(|target| {
            target.attachment_role == StreamOpenRole::Validation
                && target.key != service_key
                && target.key.underlay == service_key.underlay
                && !target.is_active
                && target.has_sender_evidence
                && target.has_bulk_rate_evidence
                && target.ack_clock_calibration_eligible
                && !target.ack_clock_calibration_proven
                && (may_start_fresh_calibration
                    || target.ack_clock_calibration_active
                    || target.ack_clock_calibration_spent_bytes > 0)
                && target.snapshot.active_latency_sensitive_flows == 0
                && target.snapshot.session_active_latency_sensitive_flows == 0
                && target.ack_clock_calibration_credit_limit_bytes > 0
                && target.ack_clock_calibration_credit_limit_bytes
                    <= target.ack_clock_calibration_max_limit_bytes
                && target
                    .ack_clock_calibration_spent_bytes
                    .saturating_add(payload_bytes as u64)
                    <= target.ack_clock_calibration_credit_limit_bytes
        })
        .filter(|target| {
            // Calibration spends unique OwnerData only. RepairData and carrier
            // queue copies remain real carrier pressure but never consume or
            // preserve this product-ownership fence.
            let candidate_debt = target.owner_data_in_flight_bytes;
            let projected_candidate_debt = candidate_debt.saturating_add(payload_bytes as u64);
            // Global ordered tail and per-candidate flight overlap; only the
            // newly assigned payload is outside both current views.
            projected_candidate_debt <= target.ack_clock_calibration_credit_limit_bytes
                && (ordered_owner_debt_bytes as u64)
                    .max(candidate_debt)
                    .saturating_add(payload_bytes as u64)
                    <= product_envelope
        })
        .filter(|target| {
            if target.ack_clock_calibration_active || target.ack_clock_calibration_spent_bytes > 0 {
                // Once exact calibration ownership exists, finish its authorized
                // stage. Reapplying an exploration gate could strand lower offsets.
                return true;
            }
            let exploration_bytes = target
                .ack_clock_calibration_credit_limit_bytes
                .saturating_sub(target.ack_clock_calibration_spent_bytes);
            let opportunity = reliable_tcp_ack_clock_calibration_opportunity(
                service.snapshot,
                service.eta_ms,
                target.snapshot,
                target.eta_ms,
                exploration_bytes,
                payload_bytes,
                mux_limits,
            );
            #[cfg(feature = "lab-diagnostics")]
            let projection = opportunity.projection();
            let admitted = opportunity.is_admitted();
            #[cfg(feature = "lab-diagnostics")]
            {
                lab_response_ack_clock_calibration_admission(target, service, projection, admitted);
                if !admitted {
                    lab_response_bulk_output_candidate(
                        "calibration_completion_horizon",
                        target,
                        payload_bytes,
                        mux_limits,
                        ResponseBulkCandidateDiag {
                            lead: Some(ResponseBulkLead {
                                key: service.key,
                                snapshot: service.snapshot,
                                eta_ms: service.eta_ms,
                            }),
                            role: Some(BulkAdmissionRole::AdditionalSameUnderlay),
                            ordering_debt: ordered_owner_debt_bytes as u64,
                        },
                    );
                }
            }
            if matches!(opportunity, TcpAckClockCalibrationOpportunity::Retire(_)) {
                retirement_intents.push(ResponseAckClockCalibrationRetirementIntent {
                    planner_generation: 0,
                    lane_generation: 0,
                    model_generation: 0,
                    service: service.key,
                    service_incarnation: service.incarnation,
                    service_pending_bytes: service.command_pending_bytes,
                    target: target.key,
                    target_incarnation: target.incarnation,
                    target_pending_bytes: target.command_pending_bytes,
                    limit_bytes: target.ack_clock_calibration_credit_limit_bytes,
                });
            }
            admitted
        })
        .filter(|target| {
            // RepairData cannot preserve the unique-owner fence, but it still
            // occupies the carrier/product pipe that the atomic commit checks.
            let carrier_pressure = target
                .snapshot
                .product_bytes_in_flight
                .max(target.command_pending_bytes);
            target.commands.can_enqueue_lane_now(lane)
                && carrier_pressure.saturating_add(payload_bytes as u64)
                    <= target.ack_clock_calibration_credit_limit_bytes
        })
        .min_by(|left, right| {
            right
                .ack_clock_calibration_active
                .cmp(&left.ack_clock_calibration_active)
                .then_with(|| {
                    (right.ack_clock_calibration_spent_bytes > 0)
                        .cmp(&(left.ack_clock_calibration_spent_bytes > 0))
                })
                .then_with(|| left.eta_ms.total_cmp(&right.eta_ms))
                .then_with(|| carrier_path_key_order(left.key, right.key))
                .then_with(|| left.incarnation.cmp(&right.incarnation))
        })
        .map(|target| ResponseSelectedDataTarget {
            target: target.clone(),
            admission: PathAdmission::subflow_owner(PathRuntimeRole::Subflow),
            subflow_set_commit: None,
            ack_clock_calibration_commit: Some(ResponseAckClockCalibrationCommit {
                planner_generation: 0,
                lane_generation: 0,
                model_generation: 0,
                service: service_key,
                service_incarnation: service.incarnation,
                service_pending_bytes: service.command_pending_bytes,
                target_pending_bytes: target.command_pending_bytes,
                limit_bytes: target.ack_clock_calibration_credit_limit_bytes,
                requires_multi_flow_start: !target.ack_clock_calibration_active
                    && target.ack_clock_calibration_spent_bytes == 0,
            }),
        })
}

fn response_ack_clock_calibration_pending(
    target: &ResponseSenderPathTarget,
    may_start_fresh_calibration: bool,
) -> bool {
    // Begun exact ownership serializes the binding. Fresh state does so only
    // while the session can actually start exploration; otherwise it is dormant.
    target.ack_clock_calibration_active
        || (!target.commands.is_closed()
            && target.ack_clock_calibration_eligible
            && !target.ack_clock_calibration_proven
            && (target.ack_clock_calibration_spent_bytes > 0
                || (may_start_fresh_calibration
                    && target.ack_clock_calibration_spent_bytes
                        < target.ack_clock_calibration_max_limit_bytes)))
}

fn response_ack_clock_calibration_blocks_generic_owner(target: &ResponseSenderPathTarget) -> bool {
    // Dormancy opens the binding reservoir, but this exact identity stays
    // excluded so ordinary OwnerData cannot contaminate later ACK calibration.
    !target.is_active
        && (target.ack_clock_calibration_active
            || (!target.commands.is_closed()
                && target.ack_clock_calibration_eligible
                && !target.ack_clock_calibration_proven
                && target.ack_clock_calibration_spent_bytes
                    < target.ack_clock_calibration_max_limit_bytes))
}

fn response_ack_clock_calibration_needs_opportunity_decision(
    target: &ResponseSenderPathTarget,
) -> bool {
    target.key.underlay == UnderlayProtocol::Tcp
        && target.ack_clock_calibration_eligible
        && !target.ack_clock_calibration_proven
        && !target.ack_clock_calibration_active
        && target.ack_clock_calibration_spent_bytes == 0
        && target.ack_clock_calibration_credit_limit_bytes > 0
}

struct ResponseOwnerAdmission {
    admission: PathAdmission,
    subflow_set_commit: Option<ResponseSubflowAdmissionCommit>,
    bulk_role: BulkAdmissionRole,
    model_suppression: Option<&'static str>,
}

fn response_owner_bulk_model_suppression(
    target: &ResponseSenderPathTarget,
    lead: ResponseBulkLead,
    lower_owner: Option<CarrierPathKey>,
    effective_ordering_debt: u64,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    role: BulkAdmissionRole,
) -> Option<&'static str> {
    if response_unique_quic_data_would_expand_ordering_debt(
        lower_owner,
        target,
        effective_ordering_debt,
    ) {
        return Some("quic_ordering_debt");
    }
    bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
        best_snapshot: lead.snapshot,
        best_eta_ms: lead.eta_ms,
        candidate_snapshot: response_target_measured_admission_snapshot(target),
        candidate_eta_ms: target.eta_ms,
        payload_bytes,
        mux_limits,
        role,
        stream_ordering_debt_bytes: effective_ordering_debt,
    })
}

#[cfg(test)]
fn response_target_unique_owner_admission(
    target: &ResponseSenderPathTarget,
    candidates: &[&ResponseSenderPathTarget],
    lead: ResponseBulkLead,
    lower_owner: Option<CarrierPathKey>,
    ordering_debt: u64,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> PathAdmission {
    response_target_unique_owner_admission_with_epoch(
        target,
        candidates,
        lead,
        lower_owner,
        None,
        ordering_debt,
        ResponseOrderedTail::new(None, 0).for_candidate(target.key),
        payload_bytes,
        mux_limits,
        None,
        true,
        false,
    )
    .admission
}

// Decides whether one candidate may own the next unique product byte range.
//
// The important split is:
// * Service: the current active owner, kept fed while healthy.
// * Subflow: an additional path admitted after path-scoped bulk-rate evidence,
//   or the one same-family Validation path consuming a bounded startup sample.
//
// Path proof, ACK-data visibility, and carrier attachment are evidence inputs,
// not implicit owner states. Startup ownership is explicit, bulk-only, and
// ledger-bounded.
fn response_target_unique_owner_admission_with_epoch(
    target: &ResponseSenderPathTarget,
    candidates: &[&ResponseSenderPathTarget],
    lead: ResponseBulkLead,
    lower_owner: Option<CarrierPathKey>,
    ordered_data_owner: Option<CarrierPathKey>,
    ordering_debt: u64,
    ordered_tail_debt: ResponseCandidateTailDebt,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    subflow_set: Option<&FlowSubflowSet>,
    startup_sampling_allowed: bool,
    allow_liveness_service_failover: bool,
) -> ResponseOwnerAdmission {
    let service_key =
        response_service_anchor_key(candidates, lower_owner, ordered_data_owner, lead.key);
    let candidate_tail_debt_bytes = ordered_tail_debt.external_bytes();
    let effective_ordering_debt = ordering_debt.max(candidate_tail_debt_bytes);
    let role = response_bulk_admission_role(
        service_key,
        target.key,
        lower_owner,
        effective_ordering_debt,
    );
    let result = |admission, subflow_set_commit, model_suppression| ResponseOwnerAdmission {
        admission,
        subflow_set_commit,
        bulk_role: role,
        model_suppression,
    };
    let direct_result = |admission: PathAdmission| {
        let owns_unique_data = matches!(
            admission.decision,
            PathAdmissionDecision::Service | PathAdmissionDecision::AdmitSubflow
        ) && admission.work == CarrierWorkKind::OwnerData
            && admission.role.may_own_unique_data();
        if !owns_unique_data {
            return result(admission, None, None);
        }
        let suppression = response_owner_bulk_model_suppression(
            target,
            lead,
            lower_owner,
            effective_ordering_debt,
            payload_bytes,
            mux_limits,
            role,
        );
        suppression.map_or_else(
            || result(admission, None, None),
            |reason| result(PathAdmission::standby(), None, Some(reason)),
        )
    };
    if target.attachment_role == StreamOpenRole::Repair {
        return result(PathAdmission::standby(), None, None);
    }
    let liveness_service_failover = allow_liveness_service_failover && target.key == service_key;
    let continues_lower_frontier = lower_owner == Some(target.key);
    if continues_lower_frontier && (target.key == service_key || target.is_active) {
        if ordering_debt > 0 {
            return result(PathAdmission::standby(), None, None);
        }
        return if target.is_active || target.has_bulk_rate_evidence {
            direct_result(PathAdmission::service())
        } else {
            result(PathAdmission::probe_only(), None, None)
        };
    }
    if continues_lower_frontier
        && target.key != service_key
        && (!target.has_bulk_rate_evidence || target.is_active)
    {
        // An authoritative ACK hole suspends startup sampling. Only an already
        // measured Subflow may continue its own lower frontier without changing
        // Service role, regardless of carrier family.
        return result(PathAdmission::standby(), None, None);
    }
    if lower_owner.is_some() && !continues_lower_frontier {
        return result(PathAdmission::standby(), None, None);
    }
    if target.key == service_key {
        if ordered_tail_debt.global_bytes() > 0
            && Some(target.key) != ordered_data_owner
            && !target.has_bulk_rate_evidence
            && !liveness_service_failover
        {
            return result(PathAdmission::standby(), None, None);
        }
        return if target.is_active || target.has_bulk_rate_evidence || liveness_service_failover {
            direct_result(PathAdmission::service())
        } else {
            result(PathAdmission::probe_only(), None, None)
        };
    }
    if target.is_active {
        return result(PathAdmission::standby(), None, None);
    }
    let startup_owner_allowed = startup_sampling_allowed
        && candidates
            .iter()
            .copied()
            .find(|candidate| candidate.key == service_key)
            .is_some_and(|service| {
                response_target_is_startup_same_underlay_subflow_candidate(
                    service_key,
                    service,
                    target,
                    candidate_tail_debt_bytes,
                    payload_bytes,
                    mux_limits,
                )
            });
    if candidate_tail_debt_bytes > 0
        && !continues_lower_frontier
        && !response_target_is_measured_same_underlay_subflow_candidate(service_key, target)
        && !startup_owner_allowed
    {
        return result(PathAdmission::standby(), None, None);
    }

    let model_suppression = response_owner_bulk_model_suppression(
        target,
        lead,
        lower_owner,
        effective_ordering_debt,
        payload_bytes,
        mux_limits,
        role,
    );
    let measured_model_allows_owner = model_suppression.is_none();
    // A candidate cannot produce a meaningful completion model until it has
    // received enough work to leave the app-limited startup state. The bounded
    // startup epoch therefore uses explicit role/pressure/resource guards and
    // does not compare the path against its own underfed rate prior.
    let model_allows_owner = startup_owner_allowed || measured_model_allows_owner;
    let completion_improves = measured_model_allows_owner && target.has_bulk_rate_evidence;
    let startup_owner_credit_bytes =
        usize::try_from(reliable_subflow_startup_sample_limit_bytes(mux_limits))
            .unwrap_or(usize::MAX)
            .max(payload_bytes);
    let input = SubflowAdmissionInput {
        key: target.key,
        bulk_rate_proven: target.has_bulk_rate_evidence,
        startup_owner_allowed,
        frontier_clear: model_allows_owner,
        completion_improves,
        observed_goodput_non_degrading: model_allows_owner,
        read_gap: Duration::ZERO,
        owner_bytes: payload_bytes,
        optional_overhead_bytes: 0,
    };
    let mut epoch = subflow_set
        .filter(|epoch| {
            epoch.matches_envelope(service_key, startup_owner_credit_bytes, 0, Duration::ZERO)
        })
        .cloned()
        .unwrap_or_else(|| {
            FlowSubflowSet::new(
                0,
                service_key,
                startup_owner_credit_bytes,
                0,
                Duration::ZERO,
            )
        });
    let admission = epoch.admit_subflow_owner(input);
    let commit = (admission.decision == PathAdmissionDecision::AdmitSubflow).then_some(
        ResponseSubflowAdmissionCommit {
            planner_generation: 0,
            lane_generation: 0,
            service: service_key,
            startup_owner_credit_bytes,
            optional_overhead_budget_bytes: 0,
            max_read_gap_budget: Duration::ZERO,
            input,
        },
    );
    result(
        admission,
        commit,
        if startup_owner_allowed {
            None
        } else {
            model_suppression
        },
    )
}

fn response_target_can_own_unique_bulk_data(
    target: &ResponseSenderPathTarget,
    candidates: &[&ResponseSenderPathTarget],
    lead: ResponseBulkLead,
    lower_owner: Option<CarrierPathKey>,
    ordering_debt: u64,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> bool {
    response_target_can_own_unique_bulk_data_with_epoch(
        target,
        candidates,
        lead,
        lower_owner,
        ordering_debt,
        payload_bytes,
        mux_limits,
        None,
    )
}

fn response_target_can_own_unique_bulk_data_with_epoch(
    target: &ResponseSenderPathTarget,
    candidates: &[&ResponseSenderPathTarget],
    lead: ResponseBulkLead,
    lower_owner: Option<CarrierPathKey>,
    ordering_debt: u64,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    subflow_set: Option<&FlowSubflowSet>,
) -> bool {
    let admission = response_target_unique_owner_admission_with_epoch(
        target,
        candidates,
        lead,
        lower_owner,
        None,
        ordering_debt,
        ResponseOrderedTail::new(None, 0).for_candidate(target.key),
        payload_bytes,
        mux_limits,
        subflow_set,
        true,
        false,
    )
    .admission;
    matches!(
        admission.decision,
        PathAdmissionDecision::Service | PathAdmissionDecision::AdmitSubflow
    ) && admission.work == CarrierWorkKind::OwnerData
        && admission.role.may_own_unique_data()
}

fn response_cross_underlay_owner_allowed(
    target: &ResponseSenderPathTarget,
    candidates: &[&ResponseSenderPathTarget],
    ordered_data_owner: Option<CarrierPathKey>,
    lower_flights: &[CarrierPathFlightDebt],
) -> bool {
    // Use the ordered owner as the family anchor, but assess safety from the
    // candidate's actual ordering debt. A lower-flight record owned by this
    // candidate is not a reason to block it; it means continuing the candidate
    // will not expand cross-path lower-byte debt.
    let current_owner = ordered_data_owner.or_else(|| {
        candidates
            .iter()
            .copied()
            .find(|entry| entry.is_active)
            .map(|entry| entry.key)
    });
    let current_owner_bulk_rate_proven = current_owner
        .and_then(|owner_key| {
            candidates
                .iter()
                .copied()
                .find(|entry| entry.key == owner_key)
        })
        .is_none_or(|owner| owner.has_bulk_rate_evidence);
    let candidate_continues_lower_frontier =
        response_oldest_lower_flight_owner(lower_flights) == Some(target.key);
    if candidate_continues_lower_frontier && (target.is_active || target.has_bulk_rate_evidence) {
        return true;
    }
    cross_family_reliable_owner_health(
        current_owner,
        current_owner_bulk_rate_proven,
        target.key,
        target.has_bulk_rate_evidence,
        candidate_continues_lower_frontier,
    )
    .reliable_owner_allowed()
}

fn response_ordered_owner_missing_under_debt(
    targets: &[ResponseSenderPathTarget],
    lower_flights: &[CarrierPathFlightDebt],
    ordered_data_owner: Option<CarrierPathKey>,
    ordered_owner_debt_bytes: usize,
) -> bool {
    if ordered_owner_debt_bytes == 0 || response_oldest_lower_flight_owner(lower_flights).is_some()
    {
        return false;
    }
    match ordered_data_owner {
        Some(owner) => {
            let live_owner = targets.iter().any(|target| target.key == owner);
            // A missing Service owner with unresolved tail debt normally blocks
            // later OwnerData. The only non-clear-frontier failover is a
            // sender-evidenced survivor in the same carrier family; RepairData
            // still never path-proves or transfers ownership.
            let same_underlay_sender_evidence_failover = targets
                .iter()
                .any(|target| target.key.underlay == owner.underlay && target.has_sender_evidence);
            !live_owner && !same_underlay_sender_evidence_failover
        }
        None => true,
    }
}

fn response_active_lead_suppression(
    target: &ResponseSenderPathTarget,
    mux_limits: MuxLimits,
    payload_bytes: usize,
    stream_ordering_debt_bytes: u64,
) -> Option<&'static str> {
    bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
        best_snapshot: target.snapshot,
        best_eta_ms: target.eta_ms,
        candidate_snapshot: target.snapshot,
        candidate_eta_ms: target.eta_ms,
        payload_bytes,
        mux_limits,
        role: BulkAdmissionRole::ActiveDataPath,
        stream_ordering_debt_bytes,
    })
}

fn choose_response_admissible_lead(
    candidate_targets: &[&ResponseSenderPathTarget],
    service_baseline: Option<&ResponseSenderPathTarget>,
    mux_limits: MuxLimits,
    payload_bytes: usize,
    lower_flights: &[CarrierPathFlightDebt],
    allow_liveness_service_failover: bool,
) -> Option<ResponseBulkLead> {
    let lower_owner = response_oldest_lower_flight_owner(lower_flights);
    if let Some(active) = service_baseline {
        // Service is the no-worse completion baseline even while its output is
        // temporarily backpressured. Candidate admission remains independent.
        return Some(ResponseBulkLead {
            key: active.key,
            snapshot: active.snapshot,
            eta_ms: active.eta_ms,
        });
    }

    if let Some(owner) = lower_owner {
        let owner_target = candidate_targets
            .iter()
            .copied()
            .find(|target| target.key == owner)?;
        if owner_target.is_active || owner_target.has_bulk_rate_evidence {
            let owner_cross_path_debt =
                response_ordering_debt_bytes(lower_flights, owner_target.key);
            return response_active_lead_suppression(
                owner_target,
                mux_limits,
                payload_bytes,
                owner_cross_path_debt,
            )
            .is_none()
            .then_some(ResponseBulkLead {
                key: owner_target.key,
                snapshot: owner_target.snapshot,
                eta_ms: owner_target.eta_ms,
            });
        }
    }

    let admissible = candidate_targets
        .iter()
        .copied()
        .filter(|target| {
            response_target_is_plausible_unique_owner_candidate(target)
                && response_active_lead_suppression(target, mux_limits, payload_bytes, 0).is_none()
        })
        .min_by(|left, right| {
            left.eta_ms
                .total_cmp(&right.eta_ms)
                .then_with(|| carrier_path_key_order(left.key, right.key))
        })
        .map(|target| ResponseBulkLead {
            key: target.key,
            snapshot: target.snapshot,
            eta_ms: target.eta_ms,
        });
    if admissible.is_some() {
        return admissible;
    }

    if lower_owner.is_none() && allow_liveness_service_failover {
        return candidate_targets
            .iter()
            .copied()
            .filter(|target| {
                response_active_lead_suppression(target, mux_limits, payload_bytes, 0).is_none()
            })
            .min_by(|left, right| {
                left.eta_ms
                    .total_cmp(&right.eta_ms)
                    .then_with(|| carrier_path_key_order(left.key, right.key))
            })
            .map(|target| ResponseBulkLead {
                key: target.key,
                snapshot: target.snapshot,
                eta_ms: target.eta_ms,
            });
    }

    if lower_owner.is_none() {
        return candidate_targets
            .iter()
            .copied()
            .filter(|target| target.has_bulk_rate_evidence)
            .min_by(|left, right| {
                left.eta_ms
                    .total_cmp(&right.eta_ms)
                    .then_with(|| carrier_path_key_order(left.key, right.key))
            })
            .map(|target| ResponseBulkLead {
                key: target.key,
                snapshot: target.snapshot,
                eta_ms: target.eta_ms,
            });
    }

    None
}

fn choose_lowest_eta_response_target(
    targets: &[ResponseSenderPathTarget],
    avoid_keys: &[CarrierPathKey],
    prefer_avoiding: bool,
) -> Option<ResponseSenderPathTarget> {
    targets
        .iter()
        .filter(|target| !prefer_avoiding || !avoid_keys.contains(&target.key))
        .min_by(|left, right| {
            left.eta_ms
                .total_cmp(&right.eta_ms)
                .then_with(|| carrier_path_key_order(left.key, right.key))
        })
        .cloned()
}

fn choose_same_family_sender_evidenced_response_target(
    targets: &[ResponseSenderPathTarget],
    avoid_keys: &[CarrierPathKey],
) -> Option<ResponseSenderPathTarget> {
    if avoid_keys.is_empty() {
        return None;
    }
    targets
        .iter()
        .filter(|target| {
            !avoid_keys.contains(&target.key)
                && target.has_sender_evidence
                && avoid_keys
                    .iter()
                    .any(|avoid_key| avoid_key.underlay == target.key.underlay)
        })
        .min_by(|left, right| {
            left.eta_ms
                .total_cmp(&right.eta_ms)
                .then_with(|| carrier_path_key_order(left.key, right.key))
        })
        .cloned()
}

fn response_target_has_ack_gap_repair_evidence(target: &ResponseSenderPathTarget) -> bool {
    target.is_active || target.has_bulk_rate_evidence
}

fn response_target_has_path_failure_repair_evidence(_target: &ResponseSenderPathTarget) -> bool {
    // A live carrier output is enough for bounded failover RepairData after the
    // original owner has disappeared or become unusable. The repair flight never
    // path-proves the carrier and never changes Service ownership.
    true
}

fn response_target_can_receive_repair(
    target: &ResponseSenderPathTarget,
    cause: RelaySendCause,
) -> bool {
    match cause {
        RelaySendCause::AckGapRepair => response_target_has_ack_gap_repair_evidence(target),
        RelaySendCause::PersistentAckGapRepair => target.has_bulk_rate_evidence,
        RelaySendCause::PersistentServerAckGapRepair(batch) => {
            target.key == batch.target.key
                && target.incarnation == batch.target.incarnation
                && target.has_bulk_rate_evidence
        }
        RelaySendCause::LiveOwnerTailRepair | RelaySendCause::PathFailureRepair => {
            response_target_has_path_failure_repair_evidence(target)
        }
        RelaySendCause::StreamData
        | RelaySendCause::StreamFin
        | RelaySendCause::RecvProgress
        | RelaySendCause::RecvProgressRecovery
        | RelaySendCause::PersistentClientAckGapRepair(_) => false,
    }
}

fn choose_response_repair_target(
    targets: &[ResponseSenderPathTarget],
    avoid_keys: &[CarrierPathKey],
    cause: RelaySendCause,
) -> Option<ResponseSenderPathTarget> {
    debug_assert!(PathRuntimeRole::RepairOnly.may_repair());
    debug_assert!(cause.is_repair());
    let repair_targets = targets
        .iter()
        .filter(|target| response_target_can_receive_repair(target, cause))
        .cloned()
        .collect::<Vec<_>>();
    if cause == RelaySendCause::PathFailureRepair
        && let Some(same_family_survivor) =
            choose_same_family_sender_evidenced_response_target(&repair_targets, avoid_keys)
    {
        return Some(same_family_survivor);
    }
    let distinct = choose_lowest_eta_response_target(&repair_targets, avoid_keys, true);
    if distinct.is_some()
        || matches!(
            cause,
            RelaySendCause::AckGapRepair
                | RelaySendCause::PersistentAckGapRepair
                | RelaySendCause::PersistentServerAckGapRepair(_)
                | RelaySendCause::LiveOwnerTailRepair
        )
    {
        return distinct;
    }
    choose_lowest_eta_response_target(&repair_targets, avoid_keys, false)
}

fn choose_response_service_or_proven_data_target(
    targets: &[ResponseSenderPathTarget],
    lower_flights: &[CarrierPathFlightDebt],
    avoid_keys: &[CarrierPathKey],
) -> Option<ResponseSenderPathTarget> {
    if let Some(lower_owner) = response_oldest_lower_flight_owner(lower_flights)
        && let Some(target) = targets
            .iter()
            .find(|target| target.key == lower_owner && !avoid_keys.contains(&target.key))
    {
        return Some(target.clone());
    }
    if let Some(active) = targets
        .iter()
        .find(|target| target.is_active && !avoid_keys.contains(&target.key))
    {
        return Some(active.clone());
    }
    let proven_targets = targets
        .iter()
        .filter(|target| target.has_bulk_rate_evidence)
        .cloned()
        .collect::<Vec<_>>();
    choose_lowest_eta_response_target(&proven_targets, avoid_keys, true)
        .or_else(|| choose_lowest_eta_response_target(&proven_targets, avoid_keys, false))
        .or_else(|| choose_lowest_eta_response_target(targets, avoid_keys, true))
        .or_else(|| choose_lowest_eta_response_target(targets, avoid_keys, false))
}

fn choose_response_sender_target(
    targets: &[ResponseSenderPathTarget],
    lane: FlowLane,
    frame: &Frame,
    emit_mode: ResponseCarrierEmitMode,
    mux_limits: MuxLimits,
    lower_flights: &[CarrierPathFlightDebt],
    avoid_keys: &[CarrierPathKey],
    repair_cause: Option<RelaySendCause>,
) -> Option<ResponseSenderPathTarget> {
    if targets.is_empty() {
        return None;
    }
    let active_service_baseline = targets.iter().find(|target| target.is_active);
    let repair = repair_cause.is_some();
    let path_failure_repair = matches!(repair_cause, Some(RelaySendCause::PathFailureRepair));
    let payload_bytes = reliable_stream_frame_payload_bytes(frame);
    if !repair
        && matches!(frame, Frame::StreamData { .. })
        && lower_flights
            .iter()
            .any(|flight| !targets.iter().any(|target| target.key == flight.key))
    {
        return None;
    }
    if !repair
        && emit_mode == ResponseCarrierEmitMode::StreamOrdered
        && !relay_frame_is_bulk_stream_data(frame, lane)
        && let Some(active) = targets
            .iter()
            .find(|target| target.is_active && !avoid_keys.contains(&target.key))
    {
        let effective_lane = emit_mode.effective_lane(frame, lane);
        return (response_target_can_enqueue_frame_now(active, frame, lane, emit_mode)
            && response_target_has_emission_credit(
                active,
                effective_lane,
                payload_bytes,
                mux_limits,
            ))
        .then_some(active.clone());
    }
    let capacity_targets = targets
        .iter()
        .filter(|target| {
            let effective_lane = emit_mode.effective_lane(frame, lane);
            response_target_can_enqueue_frame_now(target, frame, lane, emit_mode)
                && (path_failure_repair
                    || response_target_has_emission_credit(
                        target,
                        effective_lane,
                        payload_bytes,
                        mux_limits,
                    ))
        })
        .cloned()
        .collect::<Vec<_>>();
    if capacity_targets.is_empty() {
        return None;
    }
    let targets = capacity_targets.as_slice();
    if let Some(cause) = repair_cause {
        return choose_response_repair_target(targets, avoid_keys, cause);
    }
    if matches!(frame, Frame::StreamAck { .. })
        && let Some(active) = targets
            .iter()
            .find(|target| target.is_request_active && !avoid_keys.contains(&target.key))
    {
        // Request admission is clocked by ACKs returned on the current Active
        // carrier. Prefer that carrier while it has capacity, but retain the
        // normal fallback below so progress is not lost during backpressure.
        return Some(active.clone());
    }
    if !relay_frame_is_bulk_stream_data(frame, lane) {
        if matches!(frame, Frame::StreamData { .. }) {
            return choose_response_service_or_proven_data_target(
                targets,
                lower_flights,
                avoid_keys,
            );
        }
        return choose_lowest_eta_response_target(targets, avoid_keys, true)
            .or_else(|| choose_lowest_eta_response_target(targets, avoid_keys, false));
    }
    let lower_owner = response_oldest_lower_flight_owner(lower_flights);
    let service_baseline = lower_owner.and(active_service_baseline);
    let proven_targets = targets
        .iter()
        .filter(|target| target.is_active || target.has_sender_evidence)
        .collect::<Vec<_>>();
    let candidate_targets = if proven_targets.is_empty() {
        targets.iter().collect::<Vec<_>>()
    } else {
        proven_targets
    };
    let lead = choose_response_admissible_lead(
        &candidate_targets,
        service_baseline,
        mux_limits,
        payload_bytes,
        lower_flights,
        false,
    )?;
    let service_key = response_service_anchor_key(&candidate_targets, lower_owner, None, lead.key);
    let selected = candidate_targets
        .iter()
        .copied()
        .filter(|target| {
            let ordering_debt = response_ordering_debt_bytes(lower_flights, target.key);
            if !response_target_can_own_unique_bulk_data(
                target,
                &candidate_targets,
                lead,
                lower_owner,
                ordering_debt,
                payload_bytes,
                mux_limits,
            ) {
                return false;
            }
            let role =
                response_bulk_admission_role(service_key, target.key, lower_owner, ordering_debt);
            bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
                best_snapshot: lead.snapshot,
                best_eta_ms: lead.eta_ms,
                candidate_snapshot: target.snapshot,
                candidate_eta_ms: target.eta_ms,
                payload_bytes,
                mux_limits,
                role,
                stream_ordering_debt_bytes: ordering_debt,
            })
            .is_none()
        })
        .min_by(|left, right| {
            left.eta_ms
                .total_cmp(&right.eta_ms)
                .then_with(|| carrier_path_key_order(left.key, right.key))
        })
        .cloned();
    selected
}

fn response_target_can_enqueue_frame_now(
    target: &ResponseSenderPathTarget,
    frame: &Frame,
    lane: FlowLane,
    emit_mode: ResponseCarrierEmitMode,
) -> bool {
    match emit_mode {
        ResponseCarrierEmitMode::Classified => target.commands.can_enqueue_frame_now(frame, lane),
        ResponseCarrierEmitMode::StreamOrdered => {
            target.commands.can_enqueue_stream_ordered_frame_now(lane)
        }
    }
}

#[cfg(test)]
fn choose_response_sender_data_target(
    targets: &[ResponseSenderPathTarget],
    lane: FlowLane,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    lower_flights: &[CarrierPathFlightDebt],
    ordered_data_owner: Option<CarrierPathKey>,
) -> Option<ResponseSenderPathTarget> {
    choose_response_sender_data_target_with_ordered_debt(
        targets,
        lane,
        payload_bytes,
        mux_limits,
        lower_flights,
        ordered_data_owner,
        0,
    )
}

#[cfg(test)]
fn choose_response_sender_data_target_with_ordered_debt(
    targets: &[ResponseSenderPathTarget],
    lane: FlowLane,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    lower_flights: &[CarrierPathFlightDebt],
    ordered_data_owner: Option<CarrierPathKey>,
    ordered_owner_debt_bytes: usize,
) -> Option<ResponseSenderPathTarget> {
    choose_response_sender_data_target_with_ordered_debt_and_epoch(
        targets,
        lane,
        payload_bytes,
        mux_limits,
        lower_flights,
        ordered_data_owner,
        ordered_owner_debt_bytes,
        None,
    )
}

#[cfg(test)]
fn choose_response_sender_data_target_with_ordered_debt_and_epoch(
    targets: &[ResponseSenderPathTarget],
    lane: FlowLane,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    lower_flights: &[CarrierPathFlightDebt],
    ordered_data_owner: Option<CarrierPathKey>,
    ordered_owner_debt_bytes: usize,
    subflow_set: Option<&FlowSubflowSet>,
) -> Option<ResponseSenderPathTarget> {
    select_response_sender_data_target_with_ordered_debt_and_epoch(
        targets,
        lane,
        payload_bytes,
        mux_limits,
        lower_flights,
        ordered_data_owner,
        ordered_owner_debt_bytes,
        subflow_set,
    )
    .map(|selected| selected.target)
}

#[cfg(test)]
fn select_response_sender_data_target_with_ordered_debt_and_epoch(
    targets: &[ResponseSenderPathTarget],
    lane: FlowLane,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    lower_flights: &[CarrierPathFlightDebt],
    ordered_data_owner: Option<CarrierPathKey>,
    ordered_owner_debt_bytes: usize,
    subflow_set: Option<&FlowSubflowSet>,
) -> Option<ResponseSelectedDataTarget> {
    select_response_sender_data_target_with_ordered_debt_inner(
        targets,
        lane,
        payload_bytes,
        mux_limits,
        lower_flights,
        ordered_data_owner,
        ordered_owner_debt_bytes,
        subflow_set,
        true,
    )
}

#[derive(Debug)]
struct ResponseDataAdmissionPolicy {
    lead: ResponseBulkLead,
    lower_owner: Option<CarrierPathKey>,
    service_anchor: Option<CarrierPathKey>,
    lane: FlowLane,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    startup_sampling_allowed: bool,
    allow_liveness_service_failover: bool,
}

// Converts one scheduling snapshot into a reservation intent. Path ranking
// stays outside this helper, and `ResponseStreamBinding` revalidates the intent
// at commit; this keeps mutable ownership state out of speculative admission.
fn admit_response_data_target(
    target: &ResponseSenderPathTarget,
    candidates: &[&ResponseSenderPathTarget],
    subflow_set: Option<&FlowSubflowSet>,
    policy: &ResponseDataAdmissionPolicy,
    authoritative_ordering_debt: u64,
    ordered_tail_debt: ResponseCandidateTailDebt,
) -> Option<ResponseSelectedDataTarget> {
    let effective_ordering_debt =
        authoritative_ordering_debt.max(ordered_tail_debt.external_bytes());
    let ResponseOwnerAdmission {
        admission,
        subflow_set_commit,
        bulk_role: role,
        model_suppression,
    } = response_target_unique_owner_admission_with_epoch(
        target,
        candidates,
        policy.lead,
        policy.lower_owner,
        policy.service_anchor,
        authoritative_ordering_debt,
        ordered_tail_debt,
        policy.payload_bytes,
        policy.mux_limits,
        subflow_set,
        policy.startup_sampling_allowed,
        policy.allow_liveness_service_failover,
    );
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = (effective_ordering_debt, role, model_suppression);
    if !matches!(
        admission.decision,
        PathAdmissionDecision::Service | PathAdmissionDecision::AdmitSubflow
    ) || admission.work != CarrierWorkKind::OwnerData
        || !admission.role.may_own_unique_data()
    {
        #[cfg(feature = "lab-diagnostics")]
        lab_response_bulk_output_candidate(
            model_suppression.unwrap_or("not_owner_admission"),
            target,
            policy.payload_bytes,
            policy.mux_limits,
            ResponseBulkCandidateDiag {
                lead: Some(policy.lead),
                role: Some(role),
                ordering_debt: effective_ordering_debt,
            },
        );
        return None;
    }
    if admission.role == PathRuntimeRole::Service
        && !response_service_has_assigned_owner_credit(
            target,
            policy.lane,
            policy.payload_bytes,
            policy.mux_limits,
        )
    {
        #[cfg(feature = "lab-diagnostics")]
        lab_response_bulk_output_candidate(
            "assigned_owner_credit",
            target,
            policy.payload_bytes,
            policy.mux_limits,
            ResponseBulkCandidateDiag {
                lead: Some(policy.lead),
                role: Some(role),
                ordering_debt: effective_ordering_debt,
            },
        );
        return None;
    }
    if admission.role == PathRuntimeRole::Subflow
        && !response_target_has_emission_credit(
            target,
            policy.lane,
            policy.payload_bytes,
            policy.mux_limits,
        )
    {
        #[cfg(feature = "lab-diagnostics")]
        lab_response_bulk_output_candidate(
            "no_emission_credit",
            target,
            policy.payload_bytes,
            policy.mux_limits,
            ResponseBulkCandidateDiag {
                lead: Some(policy.lead),
                role: Some(role),
                ordering_debt: effective_ordering_debt,
            },
        );
        return None;
    }
    Some(ResponseSelectedDataTarget {
        target: target.clone(),
        admission,
        subflow_set_commit,
        ack_clock_calibration_commit: None,
    })
}

#[cfg(test)]
fn select_response_sender_data_target_with_ordered_debt_inner(
    targets: &[ResponseSenderPathTarget],
    lane: FlowLane,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    lower_flights: &[CarrierPathFlightDebt],
    ordered_data_owner: Option<CarrierPathKey>,
    ordered_owner_debt_bytes: usize,
    subflow_set: Option<&FlowSubflowSet>,
    startup_sampling_allowed: bool,
) -> Option<ResponseSelectedDataTarget> {
    let mut retirement_intents = Vec::new();
    select_response_sender_data_target_with_ordered_debt_inner_and_retirements(
        targets,
        lane,
        payload_bytes,
        mux_limits,
        lower_flights,
        ordered_data_owner,
        ordered_owner_debt_bytes,
        subflow_set,
        startup_sampling_allowed,
        &mut retirement_intents,
    )
}

fn select_response_sender_data_target_with_ordered_debt_inner_and_retirements(
    targets: &[ResponseSenderPathTarget],
    lane: FlowLane,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    lower_flights: &[CarrierPathFlightDebt],
    ordered_data_owner: Option<CarrierPathKey>,
    ordered_owner_debt_bytes: usize,
    subflow_set: Option<&FlowSubflowSet>,
    startup_sampling_allowed: bool,
    retirement_intents: &mut Vec<ResponseAckClockCalibrationRetirementIntent>,
) -> Option<ResponseSelectedDataTarget> {
    if targets.is_empty() {
        return None;
    }
    let mut capacity_targets = Vec::new();
    for target in targets {
        if target.attachment_role == StreamOpenRole::Repair {
            #[cfg(feature = "lab-diagnostics")]
            lab_response_bulk_output_candidate(
                "repair_attachment_owner_excluded",
                target,
                payload_bytes,
                mux_limits,
                ResponseBulkCandidateDiag {
                    lead: None,
                    role: None,
                    ordering_debt: 0,
                },
            );
            continue;
        }
        if !target.commands.can_enqueue_lane_now(lane)
            && !(startup_sampling_allowed
                && response_ack_clock_calibration_needs_opportunity_decision(target))
        {
            #[cfg(feature = "lab-diagnostics")]
            lab_response_bulk_output_candidate(
                "no_lane_capacity",
                target,
                payload_bytes,
                mux_limits,
                ResponseBulkCandidateDiag {
                    lead: None,
                    role: None,
                    ordering_debt: 0,
                },
            );
            continue;
        }
        capacity_targets.push(target.clone());
    }
    if capacity_targets.is_empty() {
        return None;
    }
    if lower_flights
        .iter()
        .any(|flight| !targets.iter().any(|target| target.key == flight.key))
    {
        return None;
    }
    if !lane.is_bulk() {
        return choose_response_service_or_proven_data_target(
            &capacity_targets,
            lower_flights,
            &[],
        )
        .map(|target| ResponseSelectedDataTarget {
            target,
            admission: PathAdmission::service(),
            subflow_set_commit: None,
            ack_clock_calibration_commit: None,
        });
    }

    let lower_owner = response_oldest_lower_flight_owner(lower_flights);
    if response_ordered_owner_missing_under_debt(
        targets,
        lower_flights,
        ordered_data_owner,
        ordered_owner_debt_bytes,
    ) {
        #[cfg(feature = "lab-diagnostics")]
        for target in &capacity_targets {
            lab_response_bulk_output_candidate(
                "missing_ordered_owner_debt",
                target,
                payload_bytes,
                mux_limits,
                ResponseBulkCandidateDiag {
                    lead: None,
                    role: None,
                    ordering_debt: ordered_owner_debt_bytes as u64,
                },
            );
        }
        return None;
    }
    let effective_lower_owner = lower_owner;
    let proven_targets = capacity_targets
        .iter()
        .filter(|target| target.is_active || target.has_sender_evidence)
        .collect::<Vec<_>>();
    #[cfg(feature = "lab-diagnostics")]
    if !proven_targets.is_empty() {
        for target in &capacity_targets {
            if !target.is_active && !target.has_sender_evidence {
                lab_response_bulk_output_candidate(
                    "no_sender_evidence",
                    target,
                    payload_bytes,
                    mux_limits,
                    ResponseBulkCandidateDiag {
                        lead: None,
                        role: None,
                        ordering_debt: 0,
                    },
                );
            }
        }
    }
    let mut candidate_targets = if proven_targets.is_empty() {
        capacity_targets.iter().collect::<Vec<_>>()
    } else {
        proven_targets
    };
    let ordered_owner_anchor = ordered_data_owner.filter(|owner| {
        targets.iter().any(|target| target.key == *owner)
            && (ordered_owner_debt_bytes > 0
                || capacity_targets.iter().any(|target| {
                    target.key == *owner && (target.is_active || target.has_bulk_rate_evidence)
                }))
    });
    let live_service_anchor = ordered_data_owner
        .filter(|owner| targets.iter().any(|target| target.key == *owner))
        .or_else(|| {
            targets
                .iter()
                .find(|target| target.is_active)
                .map(|target| target.key)
        });
    let service_anchor = if effective_lower_owner.is_some() {
        live_service_anchor
    } else {
        ordered_owner_anchor
    };
    if effective_lower_owner.is_some() && service_anchor.is_none() {
        // A surviving lower-flight owner cannot infer Service authority from a
        // missing anchor. Repair or ACK progress must clear the frontier first.
        return None;
    }
    if let Some(service_key) = ordered_owner_anchor
        && let Some(service) = targets.iter().find(|target| target.key == service_key)
    {
        if ordered_owner_debt_bytes > 0 && effective_lower_owner.is_none() {
            #[cfg(feature = "lab-diagnostics")]
            for target in &candidate_targets {
                if target.key != service_key
                    && !response_target_is_measured_same_underlay_subflow_candidate(
                        service_key,
                        target,
                    )
                    && !response_target_is_startup_same_underlay_subflow_candidate(
                        service_key,
                        service,
                        target,
                        ordered_owner_debt_bytes as u64,
                        payload_bytes,
                        mux_limits,
                    )
                {
                    lab_response_bulk_output_candidate(
                        "ordered_owner_tail_debt",
                        target,
                        payload_bytes,
                        mux_limits,
                        ResponseBulkCandidateDiag {
                            lead: None,
                            role: None,
                            ordering_debt: ordered_owner_debt_bytes as u64,
                        },
                    );
                }
            }
            candidate_targets.retain(|target| {
                target.key == service_key
                    || response_target_is_measured_same_underlay_subflow_candidate(
                        service_key,
                        target,
                    )
                    || response_target_is_startup_same_underlay_subflow_candidate(
                        service_key,
                        service,
                        target,
                        ordered_owner_debt_bytes as u64,
                        payload_bytes,
                        mux_limits,
                    )
            });
            if candidate_targets.is_empty() {
                return None;
            }
        }
        let service_has_capacity = candidate_targets
            .iter()
            .any(|target| target.key == service_key);
        let service_is_backpressured = !service_has_capacity
            || !response_service_has_assigned_owner_credit(
                service,
                lane,
                payload_bytes,
                mux_limits,
            )
            || response_active_lead_suppression(service, mux_limits, payload_bytes, 0).is_some();
        if service_is_backpressured {
            #[cfg(feature = "lab-diagnostics")]
            for target in &candidate_targets {
                if target.key != service_key && target.key.underlay != service_key.underlay {
                    lab_response_bulk_output_candidate(
                        "service_owner_backpressure",
                        target,
                        payload_bytes,
                        mux_limits,
                        ResponseBulkCandidateDiag {
                            lead: None,
                            role: None,
                            ordering_debt: 0,
                        },
                    );
                }
            }
            candidate_targets.retain(|target| {
                target.key == service_key || target.key.underlay == service_key.underlay
            });
            if candidate_targets.is_empty() {
                return None;
            }
        }
    }
    let mut missing_owner_same_underlay_failover = false;
    if effective_lower_owner.is_none()
        && ordered_owner_anchor.is_none()
        && ordered_owner_debt_bytes > 0
        && let Some(owner) = ordered_data_owner
    {
        let owner_underlay = owner.underlay;
        missing_owner_same_underlay_failover = candidate_targets
            .iter()
            .any(|target| target.key.underlay == owner_underlay && target.has_sender_evidence);
        if missing_owner_same_underlay_failover {
            #[cfg(feature = "lab-diagnostics")]
            for target in &candidate_targets {
                if target.key.underlay != owner_underlay || !target.has_sender_evidence {
                    lab_response_bulk_output_candidate(
                        "missing_owner_same_underlay_failover",
                        target,
                        payload_bytes,
                        mux_limits,
                        ResponseBulkCandidateDiag {
                            lead: None,
                            role: None,
                            ordering_debt: ordered_owner_debt_bytes as u64,
                        },
                    );
                }
            }
            candidate_targets.retain(|target| {
                target.key.underlay == owner_underlay && target.has_sender_evidence
            });
            if candidate_targets.is_empty() {
                return None;
            }
        }
    }
    let mixed_safe_targets = candidate_targets
        .iter()
        .copied()
        .filter(|target| {
            Some(target.key) == effective_lower_owner
                || response_cross_underlay_owner_allowed(
                    target,
                    &candidate_targets,
                    ordered_data_owner,
                    lower_flights,
                )
        })
        .collect::<Vec<_>>();
    #[cfg(feature = "lab-diagnostics")]
    if !mixed_safe_targets.is_empty() {
        for target in &candidate_targets {
            if !mixed_safe_targets.iter().any(|safe| safe.key == target.key) {
                lab_response_bulk_output_candidate(
                    "mixed_family_owner_unhealthy",
                    target,
                    payload_bytes,
                    mux_limits,
                    ResponseBulkCandidateDiag {
                        lead: None,
                        role: None,
                        ordering_debt: 0,
                    },
                );
            }
        }
    }
    let candidate_targets = if mixed_safe_targets.is_empty() {
        candidate_targets
    } else {
        mixed_safe_targets
    };
    let allow_liveness_service_failover = effective_lower_owner.is_none()
        && service_anchor.is_none()
        && (ordered_owner_debt_bytes == 0 || missing_owner_same_underlay_failover)
        && !candidate_targets.iter().any(|target| target.is_active);
    let service_baseline = service_anchor
        .and_then(|service_key| targets.iter().find(|target| target.key == service_key));
    // Begun TCP product-ACK calibration owns one binding tail. Fresh state does
    // so only while the two-flow start gate is open; dormant state blocks only
    // its exact target below. QUIC remains under its carrier ACK controller.
    let tcp_calibration_serialized = targets
        .iter()
        .any(|target| response_ack_clock_calibration_pending(target, startup_sampling_allowed));
    if let Some(service_key) = service_anchor
        && let Some(calibration) = select_response_ack_clock_calibration_target(
            targets,
            &candidate_targets,
            lane,
            service_key,
            ordered_owner_debt_bytes,
            payload_bytes,
            mux_limits,
            lower_flights,
            subflow_set,
            startup_sampling_allowed,
            retirement_intents,
        )
    {
        #[cfg(feature = "lab-diagnostics")]
        lab_response_bulk_output_selected("ack_clock_calibration", &calibration, payload_bytes);
        return Some(calibration);
    }
    let candidate_targets = candidate_targets
        .into_iter()
        .filter(|target| !response_ack_clock_calibration_blocks_generic_owner(target))
        .collect::<Vec<_>>();
    if candidate_targets.is_empty() {
        return None;
    }
    let Some(lead) = choose_response_admissible_lead(
        &candidate_targets,
        service_baseline,
        mux_limits,
        payload_bytes,
        lower_flights,
        allow_liveness_service_failover,
    ) else {
        #[cfg(feature = "lab-diagnostics")]
        for target in &candidate_targets {
            lab_response_bulk_output_candidate(
                "no_admissible_lead",
                target,
                payload_bytes,
                mux_limits,
                ResponseBulkCandidateDiag {
                    lead: None,
                    role: None,
                    ordering_debt: 0,
                },
            );
        }
        return None;
    };
    let service_key = response_service_anchor_key(
        &candidate_targets,
        effective_lower_owner,
        service_anchor,
        lead.key,
    );
    let ordered_tail = ResponseOrderedTail::new(service_anchor, ordered_owner_debt_bytes);
    let admission_policy = ResponseDataAdmissionPolicy {
        lead,
        lower_owner: effective_lower_owner,
        service_anchor,
        lane,
        payload_bytes,
        mux_limits,
        startup_sampling_allowed: startup_sampling_allowed && !tcp_calibration_serialized,
        allow_liveness_service_failover,
    };
    let service_target = candidate_targets
        .iter()
        .copied()
        .find(|target| target.key == service_key);
    let mut admitted = Vec::with_capacity(candidate_targets.len());
    if let Some(target) = service_target {
        let ordering_debt = response_ordering_debt_bytes(lower_flights, target.key);
        if let Some(selected) = admit_response_data_target(
            target,
            &candidate_targets,
            subflow_set,
            &admission_policy,
            ordering_debt,
            ordered_tail.for_candidate(target.key),
        ) {
            admitted.push(selected);
        }
    }
    // Service admission establishes the reservoir precondition. Each remaining
    // candidate produces one admission-model result with either ordinary debt
    // or the TCP-only ownership-aware view.
    // A calibration stage needs isolated product ACK coverage. Keep ordinary
    // TCP reservoir work out until its exact flights drain; Service remains the
    // fallback and the carrier-specific controller continues draining below.
    let tcp_reservoir = (!tcp_calibration_serialized && effective_lower_owner.is_none())
        .then(|| response_tcp_reservoir_policy(&admitted, ordered_tail, payload_bytes, mux_limits))
        .flatten();
    for target in candidate_targets
        .iter()
        .copied()
        .filter(|target| target.key != service_key)
    {
        let ordering_debt = response_ordering_debt_bytes(lower_flights, target.key);
        let candidate_debt = tcp_reservoir
            .filter(|reservoir| response_target_is_tcp_reservoir_candidate(*reservoir, target))
            .map_or_else(
                || ordered_tail.for_candidate(target.key),
                |reservoir| response_tcp_reservoir_candidate_debt(reservoir, target),
            );
        if let Some(selected) = admit_response_data_target(
            target,
            &candidate_targets,
            subflow_set,
            &admission_policy,
            ordering_debt,
            candidate_debt,
        ) {
            admitted.push(selected);
        }
    }
    if let Some(startup) = admitted
        .iter()
        .filter(|selected| {
            selected
                .subflow_set_commit
                .is_some_and(|commit| commit.input.startup_owner_allowed)
        })
        .min_by(|left, right| {
            left.target
                .eta_ms
                .total_cmp(&right.target.eta_ms)
                .then_with(|| carrier_path_key_order(left.target.key, right.target.key))
        })
        .cloned()
    {
        #[cfg(feature = "lab-diagnostics")]
        lab_response_bulk_output_selected("startup_sample", &startup, payload_bytes);
        return Some(startup);
    }
    if let Some(reservoir) = tcp_reservoir
        && let Some(subflow_target) = response_tcp_reservoir_subflow_target(&admitted, reservoir)
    {
        #[cfg(feature = "lab-diagnostics")]
        lab_response_bulk_output_selected("tcp_subflow_reservoir", &subflow_target, payload_bytes);
        return Some(subflow_target);
    }
    if let Some(service_target) = response_feedable_service_owner_target(&admitted) {
        #[cfg(feature = "lab-diagnostics")]
        lab_response_bulk_output_selected("service_first", &service_target, payload_bytes);
        return Some(service_target);
    }
    let best = admitted.iter().min_by(|left, right| {
        left.target
            .eta_ms
            .total_cmp(&right.target.eta_ms)
            .then_with(|| carrier_path_key_order(left.target.key, right.target.key))
    })?;
    if lower_owner.is_none()
        && let Some(lead_key) = ordered_data_owner
        && let Some(lead_target) = admitted
            .iter()
            .find(|selected| selected.target.key == lead_key)
        && response_target_within_adaptive_lead_hysteresis(
            &lead_target.target,
            &best.target,
            payload_bytes,
        )
    {
        #[cfg(feature = "lab-diagnostics")]
        lab_response_bulk_output_selected("hysteresis", lead_target, payload_bytes);
        return Some(lead_target.clone());
    }
    #[cfg(feature = "lab-diagnostics")]
    lab_response_bulk_output_selected("best_eta", best, payload_bytes);
    Some(best.clone())
}

fn response_extra_traffic_startup_floor_bytes(mux_limits: MuxLimits) -> usize {
    reliable_bulk_carrier_feed_quantum_bytes(mux_limits)
        .max(PATH_OPEN_SCORE_BYTES)
        .min(mux_limits.max_repair_bytes)
        .max(1)
}

fn response_repair_minimum_useful_attempt_bytes(mux_limits: MuxLimits) -> usize {
    PATH_OPEN_SCORE_BYTES
        .min(reliable_bulk_carrier_feed_quantum_bytes(mux_limits))
        .min(mux_limits.max_repair_bytes)
        .max(1)
}

fn response_target_within_adaptive_lead_hysteresis(
    old_lead: &ResponseSenderPathTarget,
    best: &ResponseSenderPathTarget,
    payload_bytes: usize,
) -> bool {
    if old_lead.key == best.key {
        return true;
    }
    path_within_adaptive_lead_hysteresis(
        old_lead.eta_ms,
        old_lead.snapshot,
        best.eta_ms,
        best.snapshot,
        payload_bytes,
    )
}

fn response_target_assigned_product_bytes(target: &ResponseSenderPathTarget) -> u64 {
    // Product flight includes frames still pending in the carrier command
    // pipe. Treat the ledger and queue snapshots as overlapping views so the
    // same OwnerData cannot consume calibration credit twice.
    target.snapshot.product_bytes_in_flight.max(
        target
            .snapshot
            .queue_bytes
            .max(target.commands.pending_bytes()),
    )
}

fn response_feedable_service_owner_target(
    admitted: &[ResponseSelectedDataTarget],
) -> Option<ResponseSelectedDataTarget> {
    admitted
        .iter()
        .filter(|selected| selected.admission.role == PathRuntimeRole::Service)
        .min_by(|left, right| {
            response_target_assigned_product_bytes(&left.target)
                .cmp(&response_target_assigned_product_bytes(&right.target))
                .then_with(|| left.target.eta_ms.total_cmp(&right.target.eta_ms))
                .then_with(|| carrier_path_key_order(left.target.key, right.target.key))
        })
        .cloned()
}

fn response_tcp_reservoir_policy(
    admitted: &[ResponseSelectedDataTarget],
    ordered_tail: ResponseOrderedTail,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> Option<ResponseTcpReservoir> {
    let service = response_feedable_service_owner_target(admitted)?;
    if service.target.key.underlay != UnderlayProtocol::Tcp
        || !service.target.is_active
        || !service.target.has_bulk_rate_evidence
        || service.target.snapshot.active_latency_sensitive_flows > 0
        || service
            .target
            .snapshot
            .session_active_latency_sensitive_flows
            > 0
    {
        return None;
    }
    let service_horizon = bulk_service_horizon_payload_bytes(payload_bytes, mux_limits);
    let service_assigned = service.target.owner_data_in_flight_bytes;
    let feed_reservoir = bulk_service_feed_reservoir_payload_bytes(payload_bytes, mux_limits);

    ResponseTcpReservoir::new(
        service.target.key,
        ordered_tail,
        service_assigned,
        service_horizon,
        feed_reservoir,
        payload_bytes,
    )
}

fn response_target_is_tcp_reservoir_candidate(
    reservoir: ResponseTcpReservoir,
    target: &ResponseSenderPathTarget,
) -> bool {
    target.key != reservoir.service()
        && target.key.underlay == UnderlayProtocol::Tcp
        && !target.is_active
        && target.has_bulk_rate_evidence
        && target.snapshot.active_latency_sensitive_flows == 0
        && target.snapshot.session_active_latency_sensitive_flows == 0
}

fn response_tcp_reservoir_candidate_debt(
    reservoir: ResponseTcpReservoir,
    target: &ResponseSenderPathTarget,
) -> ResponseCandidateTailDebt {
    // The global tail contains unique OwnerData. Subtract only this candidate's
    // unique share; generic carrier admission separately keeps every OwnerData
    // and RepairData copy charged as product flight.
    reservoir.for_candidate(target.key, target.owner_data_in_flight_bytes)
}

fn response_tcp_reservoir_subflow_target(
    admitted: &[ResponseSelectedDataTarget],
    reservoir: ResponseTcpReservoir,
) -> Option<ResponseSelectedDataTarget> {
    // The single-family source reader already caps total ordered tail plus raw
    // staging at the Service feed reservoir. Keep the first horizon on Service,
    // then let one already-admitted measured TCP Subflow use the remaining
    // reservoir instead of waiting for the full Service envelope to back up.
    admitted
        .iter()
        .filter(|selected| {
            selected.admission.role == PathRuntimeRole::Subflow
                && response_target_is_tcp_reservoir_candidate(reservoir, &selected.target)
                && selected
                    .subflow_set_commit
                    .is_some_and(|commit| commit.service == reservoir.service())
        })
        .min_by(|left, right| {
            left.target
                .eta_ms
                .total_cmp(&right.target.eta_ms)
                .then_with(|| {
                    response_target_assigned_product_bytes(&left.target)
                        .cmp(&response_target_assigned_product_bytes(&right.target))
                })
                .then_with(|| carrier_path_key_order(left.target.key, right.target.key))
        })
        .cloned()
}

fn response_target_has_emission_credit(
    target: &ResponseSenderPathTarget,
    lane: FlowLane,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> bool {
    if !lane.is_bulk() {
        return true;
    }
    let credit = response_target_emission_credit_bytes(target, lane, payload_bytes, mux_limits);
    target
        .commands
        .pending_bytes()
        .saturating_add(payload_bytes as u64)
        <= credit as u64
}

fn response_service_has_assigned_owner_credit(
    target: &ResponseSenderPathTarget,
    lane: FlowLane,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> bool {
    if !lane.is_bulk() {
        return true;
    }
    let credit = response_service_emission_credit_bytes(target, payload_bytes, mux_limits);
    // Product flight owns the offset range from carrier enqueue until
    // STREAM_ACK, including frames still pending in the carrier pipe. Retain
    // an independent queue-pressure fallback for incomplete/synthetic
    // snapshots, but use a union-style maximum so those views cannot charge
    // the same assigned OwnerData twice against hard Service credit.
    let assigned = target.snapshot.product_bytes_in_flight.max(
        target
            .snapshot
            .queue_bytes
            .max(target.commands.pending_bytes()),
    );
    assigned.saturating_add(payload_bytes as u64) <= credit as u64
}

fn response_service_emission_credit_bytes(
    target: &ResponseSenderPathTarget,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> usize {
    if !target.has_bulk_rate_evidence {
        return response_service_startup_emission_credit_bytes(payload_bytes, mux_limits);
    }
    if target.snapshot.active_latency_sensitive_flows > 0 {
        return usize::try_from(bulk_latency_pressure_service_feed_window_bytes(
            payload_bytes,
            mux_limits,
        ))
        .unwrap_or(usize::MAX)
        .max(payload_bytes)
        .max(1);
    }
    usize::try_from(bulk_active_service_product_envelope_bytes(
        target.snapshot,
        payload_bytes,
        mux_limits,
    ))
    .unwrap_or(usize::MAX)
    .max(payload_bytes)
    .max(1)
}

fn response_target_emission_credit_bytes(
    target: &ResponseSenderPathTarget,
    lane: FlowLane,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> usize {
    if lane.is_bulk() {
        if target.is_active {
            return response_service_emission_credit_bytes(target, payload_bytes, mux_limits);
        }
        if target.key.underlay == UnderlayProtocol::Udp {
            return response_quic_carrier_feed_credit_bytes(target, payload_bytes, mux_limits);
        }
    }
    adaptive_reliable_relay_inflight_bytes(Some(target.snapshot), lane, mux_limits)
        .max(reliable_relay_scheduler_quantum_cap(
            Some(target.snapshot),
            lane,
            mux_limits,
        ))
        .max(payload_bytes)
        .max(1)
}

fn response_service_startup_emission_credit_bytes(
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> usize {
    bulk_service_horizon_payload_bytes(payload_bytes, mux_limits)
}

fn response_quic_carrier_feed_credit_bytes(
    target: &ResponseSenderPathTarget,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> usize {
    let product_envelope = mux_limits
        .max_path_flight_bytes
        .min(mux_limits.max_repair_bytes)
        .min(mux_limits.max_reorder_bytes)
        .min(usize::try_from(mux_limits.max_stream_window_bytes).unwrap_or(usize::MAX))
        .max(payload_bytes)
        .max(1);
    let carrier_window = usize::try_from(target.snapshot.inflight_limit_bytes)
        .unwrap_or(usize::MAX)
        .max(reliable_bulk_carrier_feed_quantum_bytes(mux_limits));
    let live_carrier_debt = usize::try_from(
        target
            .snapshot
            .bytes_in_flight
            .saturating_add(target.snapshot.queue_bytes),
    )
    .unwrap_or(usize::MAX);
    product_envelope
        .min(carrier_window.saturating_add(live_carrier_debt))
        .max(reliable_bulk_carrier_feed_quantum_bytes(mux_limits))
        .max(payload_bytes)
}

#[cfg(test)]
fn plan_response_data_dispatch(
    stream: &ReliablePathStream,
    relay_lane: FlowLane,
    next_offset: u64,
    payload_bytes: usize,
) -> Result<ResponseDataDispatchPlan, RuntimeError> {
    plan_response_data_dispatch_with_ordered_debt_impl(
        stream,
        relay_lane,
        next_offset,
        payload_bytes,
        0,
    )
}

fn plan_response_data_dispatch_with_ordered_debt_impl(
    stream: &ReliablePathStream,
    relay_lane: FlowLane,
    next_offset: u64,
    payload_bytes: usize,
    ordered_owner_debt_bytes: usize,
) -> Result<ResponseDataDispatchPlan, RuntimeError> {
    match &stream.output {
        ReliablePathStreamOutput::Fixed(fixed) => {
            let lane =
                reliable_work_lane_to_carrier_lane(ReliableRelayQueuedWorkLane::Data, relay_lane);
            if fixed.commands().can_enqueue_lane_now(lane) {
                Ok(ResponseDataDispatchPlan {
                    primary: ResponseDataDispatchTarget::Fixed(fixed.clone()),
                })
            } else {
                Err(RuntimeError::SenderServiceBlocked)
            }
        }
        ReliablePathStreamOutput::Switchable(binding) => {
            let mut may_resnapshot_after_retirement = true;
            loop {
                let (planner_generation, subflow_set) = binding.subflow_state_snapshot();
                let (lane_generation, active_response_flows) =
                    binding.lane_generation_and_active_response_flows();
                let model_generation = binding.response_model_generation();
                let lower_flights = binding.lower_flights_before_offset(next_offset);
                let targets = binding.sender_path_targets(relay_lane, payload_bytes);
                let ordered_data_owner = binding.ordered_data_owner();
                let mut retirement_intents = Vec::new();
                let selected =
                    select_response_sender_data_target_with_ordered_debt_inner_and_retirements(
                        &targets,
                        relay_lane,
                        payload_bytes,
                        binding.mux_limits(),
                        &lower_flights,
                        ordered_data_owner,
                        ordered_owner_debt_bytes,
                        subflow_set.as_ref(),
                        active_response_flows >= 2,
                        &mut retirement_intents,
                    );
                let mut retired_any = false;
                if may_resnapshot_after_retirement {
                    for mut intent in retirement_intents {
                        intent.planner_generation = planner_generation;
                        intent.lane_generation = lane_generation;
                        intent.model_generation = model_generation;
                        retired_any |= binding.try_retire_tcp_ack_clock_calibration(
                            ResponseAckClockCalibrationRetirementRequest {
                                expected_planner_generation: intent.planner_generation,
                                expected_lane_generation: intent.lane_generation,
                                expected_model_generation: intent.model_generation,
                                service: intent.service,
                                service_incarnation: intent.service_incarnation,
                                service_pending_bytes: intent.service_pending_bytes,
                                target: intent.target,
                                target_incarnation: intent.target_incarnation,
                                target_pending_bytes: intent.target_pending_bytes,
                                limit_bytes: intent.limit_bytes,
                            },
                        );
                    }
                }
                if retired_any {
                    // Retirement invalidates the planner generation. Recompute
                    // once so the resulting Service/reservoir plan uses the tombstone.
                    may_resnapshot_after_retirement = false;
                    continue;
                }
                let Some(mut selected) = selected else {
                    return Err(RuntimeError::SenderServiceBlocked);
                };
                if let Some(commit) = selected.subflow_set_commit.as_mut() {
                    commit.planner_generation = planner_generation;
                    commit.lane_generation = lane_generation;
                }
                if let Some(commit) = selected.ack_clock_calibration_commit.as_mut() {
                    commit.planner_generation = planner_generation;
                    commit.lane_generation = lane_generation;
                    commit.model_generation = model_generation;
                }
                let target = selected.target;
                let role = selected.admission.role;
                debug_assert!(
                    role != PathRuntimeRole::Subflow
                        || target.has_bulk_rate_evidence
                        || selected
                            .subflow_set_commit
                            .is_some_and(|commit| commit.input.startup_owner_allowed),
                    "Subflow OwnerData requires bulk-rate evidence or explicit bounded startup admission: target={:?} role={:?} ordered_owner={:?} lower_owner={:?} is_active={} sender_evidence={} bulk_evidence={}",
                    target.key,
                    role,
                    ordered_data_owner,
                    response_oldest_lower_flight_owner(&lower_flights),
                    target.is_active,
                    target.has_sender_evidence,
                    target.has_bulk_rate_evidence,
                );
                return Ok(ResponseDataDispatchPlan {
                    primary: ResponseDataDispatchTarget::Switchable {
                        binding: binding.clone(),
                        target,
                        role,
                        subflow_set_commit: selected.subflow_set_commit,
                        ack_clock_calibration_commit: selected.ack_clock_calibration_commit,
                    },
                });
            }
        }
    }
}

fn response_plan_is_ack_clock_calibration(planned: &ResponseDataDispatchPlan) -> bool {
    matches!(
        &planned.primary,
        ResponseDataDispatchTarget::Switchable {
            ack_clock_calibration_commit: Some(_),
            ..
        }
    )
}

fn plan_response_data_payload_with_ordered_debt_impl(
    path_stream: &ReliablePathStream,
    relay_lane: FlowLane,
    next_offset: u64,
    payload_bytes: usize,
    ordered_owner_debt_bytes: usize,
) -> Result<(usize, ResponseDataDispatchPlan), RuntimeError> {
    let calibration_remaining = match &path_stream.output {
        ReliablePathStreamOutput::Switchable(binding) => {
            binding.active_tcp_ack_clock_calibration_remaining_bytes()
        }
        ReliablePathStreamOutput::Fixed(_) => None,
    };
    if let Some(remaining) = calibration_remaining {
        let calibration_payload_bytes = payload_bytes.min(remaining);
        match plan_response_data_dispatch_with_ordered_debt_impl(
            path_stream,
            relay_lane,
            next_offset,
            calibration_payload_bytes,
            ordered_owner_debt_bytes,
        ) {
            Ok(planned) if response_plan_is_ack_clock_calibration(&planned) => {
                return Ok((calibration_payload_bytes, planned));
            }
            Ok(planned) if calibration_payload_bytes == payload_bytes => {
                return Ok((payload_bytes, planned));
            }
            Err(err) if calibration_payload_bytes == payload_bytes => return Err(err),
            Ok(_) | Err(_) => {}
        }
    }

    plan_response_data_dispatch_with_ordered_debt_impl(
        path_stream,
        relay_lane,
        next_offset,
        payload_bytes,
        ordered_owner_debt_bytes,
    )
    .map(|planned| (payload_bytes, planned))
}

fn response_dispatch_payload_bytes(
    path_stream: &ReliablePathStream,
    send_stream: &ReliableSendStream,
    relay_lane: FlowLane,
    mux_limits: MuxLimits,
    queued_payload_bytes: usize,
) -> Option<usize> {
    let requires_repair_capacity_preflight = matches!(
        &path_stream.output,
        ReliablePathStreamOutput::Switchable(binding)
            if binding.may_have_mixed_owner_underlays()
    );
    let repair_credit = if requires_repair_capacity_preflight {
        mux_limits
            .max_repair_bytes
            .saturating_sub(send_stream.repair_bytes())
    } else {
        usize::MAX
    };
    if repair_credit == 0 {
        return None;
    }
    let snapshot = path_stream.send_path_snapshot(relay_lane, queued_payload_bytes);
    Some(
        adaptive_reliable_relay_chunk_bytes_with_frame_limit(
            snapshot,
            relay_lane,
            mux_limits,
            path_stream.max_frame_payload_bytes,
        )
        .min(queued_payload_bytes)
        .min(repair_credit)
        .max(1),
    )
}

fn response_repair_carrier_lane(frame: &Frame) -> FlowLane {
    if matches!(frame, Frame::StreamData { .. }) {
        reliable_path_stream_ordered_queue_lane()
    } else {
        FlowLane::Control
    }
}

fn response_frame_has_carrier_credit(
    stream: &ReliablePathStream,
    frame: &Frame,
    lane: FlowLane,
    emit_mode: ResponseCarrierEmitMode,
    repair_cause: Option<RelaySendCause>,
) -> bool {
    let repair = repair_cause.is_some();
    let lane = if repair {
        response_repair_carrier_lane(frame)
    } else {
        lane
    };
    match &stream.output {
        ReliablePathStreamOutput::Fixed(fixed) => match emit_mode {
            ResponseCarrierEmitMode::Classified => {
                fixed.commands().can_enqueue_frame_now(frame, lane)
            }
            ResponseCarrierEmitMode::StreamOrdered => {
                fixed.commands().can_enqueue_stream_ordered_frame_now(lane)
            }
        },
        ReliablePathStreamOutput::Switchable(binding) => {
            let payload_bytes = reliable_stream_frame_payload_bytes(frame);
            let lower_flights = if matches!(frame, Frame::StreamData { .. }) && !repair {
                binding.lower_flights_before_frame(frame)
            } else {
                Vec::new()
            };
            let avoid_keys = match repair_cause {
                Some(RelaySendCause::LiveOwnerTailRepair) => {
                    binding.owner_flight_keys_overlapping_frame(frame)
                }
                Some(_) => binding.flight_keys_overlapping_frame(frame),
                None => Vec::new(),
            };
            let targets = binding.sender_path_targets(lane, payload_bytes);
            choose_response_sender_target(
                &targets,
                lane,
                frame,
                emit_mode,
                binding.mux_limits(),
                &lower_flights,
                &avoid_keys,
                repair_cause,
            )
            .is_some()
        }
    }
}

async fn emit_planned_response_data_frame(
    stream: &ReliablePathStream,
    planned: ResponseDataDispatchPlan,
    frame: Frame,
    lane: FlowLane,
) -> Result<ResponseDataEmitOutcome, RuntimeError> {
    let ResponseDataDispatchPlan { primary } = planned;
    match primary {
        ResponseDataDispatchTarget::Fixed(fixed) => {
            send_sender_service_frame_to_carrier(
                fixed.commands(),
                frame.clone(),
                lane,
                ResponseCarrierEmitMode::StreamOrdered,
            )
            .await?;
            fixed.record_owner_flight(&frame);
            Ok(ResponseDataEmitOutcome {
                selected_path: Some(fixed.key()),
            })
        }
        ResponseDataDispatchTarget::Switchable {
            binding,
            target,
            role,
            subflow_set_commit,
            ack_clock_calibration_commit,
        } => {
            let subflow_request =
                subflow_set_commit.map(|commit| ResponseSubflowAdmissionRequest {
                    expected_planner_generation: commit.planner_generation,
                    expected_lane_generation: commit.lane_generation,
                    service: commit.service,
                    startup_owner_credit_bytes: commit.startup_owner_credit_bytes,
                    optional_overhead_budget_bytes: commit.optional_overhead_budget_bytes,
                    max_read_gap_budget: commit.max_read_gap_budget,
                    input: commit.input,
                });
            let calibration_request =
                ack_clock_calibration_commit.map(|commit| ResponseAckClockCalibrationRequest {
                    expected_planner_generation: commit.planner_generation,
                    expected_lane_generation: commit.lane_generation,
                    expected_model_generation: commit.model_generation,
                    service: commit.service,
                    service_incarnation: commit.service_incarnation,
                    service_pending_bytes: commit.service_pending_bytes,
                    target_pending_bytes: commit.target_pending_bytes,
                    limit_bytes: commit.limit_bytes,
                    requires_multi_flow_start: commit.requires_multi_flow_start,
                });
            let calibrating = calibration_request.is_some();
            match binding.try_enqueue_owner_frame_for_target(
                &target,
                &frame,
                lane,
                subflow_request,
                calibration_request,
            ) {
                Ok(_) => {}
                Err(RuntimeError::SenderServiceBlocked) => {
                    return Err(RuntimeError::SenderServiceBlocked);
                }
                Err(_) => {
                    binding.detach(target.key, &target.commands);
                    return Err(RuntimeError::SenderServiceBlocked);
                }
            }
            if role == PathRuntimeRole::Service {
                let _ = binding.commit_ordered_data_owner_for_target(&target);
            }
            let decision_reason = match role {
                PathRuntimeRole::Service => "data_service",
                PathRuntimeRole::Subflow if calibrating => "data_subflow_ack_clock_calibration",
                PathRuntimeRole::Subflow => "data_subflow",
                PathRuntimeRole::Probe
                | PathRuntimeRole::RepairOnly
                | PathRuntimeRole::Standby
                | PathRuntimeRole::Failed => "data",
            };
            record_server_sender_decision(
                binding.session_id(),
                stream.stream_id,
                target.key,
                &frame,
                lane,
                decision_reason,
                Some(target.has_bulk_rate_evidence),
            );
            Ok(ResponseDataEmitOutcome {
                selected_path: Some(target.key),
            })
        }
    }
}

async fn emit_response_frame_from_sender_service(
    stream: &ReliablePathStream,
    frame: Frame,
    lane: FlowLane,
    emit_mode: ResponseCarrierEmitMode,
    reason: &'static str,
    repair_cause: Option<RelaySendCause>,
) -> Result<Option<CarrierPathKey>, RuntimeError> {
    let repair = repair_cause.is_some();
    let lane = if repair {
        response_repair_carrier_lane(&frame)
    } else {
        lane
    };
    let emit_mode = if matches!(frame, Frame::StreamData { .. }) && !repair {
        ResponseCarrierEmitMode::StreamOrdered
    } else {
        emit_mode
    };
    match &stream.output {
        ReliablePathStreamOutput::Fixed(fixed) => {
            send_sender_service_frame_to_carrier(fixed.commands(), frame.clone(), lane, emit_mode)
                .await?;
            if matches!(frame, Frame::StreamData { .. }) {
                if repair {
                    fixed.record_repair_flight(&frame);
                } else {
                    fixed.record_owner_flight(&frame);
                }
            }
            Ok(Some(fixed.key()))
        }
        ReliablePathStreamOutput::Switchable(binding) => {
            let payload_bytes = reliable_stream_frame_payload_bytes(&frame);
            let lower_flights = if matches!(frame, Frame::StreamData { .. }) && !repair {
                binding.lower_flights_before_frame(&frame)
            } else {
                Vec::new()
            };
            let avoid_keys = match repair_cause {
                Some(RelaySendCause::LiveOwnerTailRepair) => {
                    binding.owner_flight_keys_overlapping_frame(&frame)
                }
                Some(_) => binding.flight_keys_overlapping_frame(&frame),
                None => Vec::new(),
            };
            let mut last_error = None;
            loop {
                let targets = binding.sender_path_targets(lane, payload_bytes);
                if targets.is_empty() {
                    let _ = last_error;
                    return Err(RuntimeError::SenderServiceBlocked);
                }
                let Some(target) = choose_response_sender_target(
                    &targets,
                    lane,
                    &frame,
                    emit_mode,
                    binding.mux_limits(),
                    &lower_flights,
                    &avoid_keys,
                    repair_cause,
                ) else {
                    return Err(RuntimeError::SenderServiceBlocked);
                };
                let send_result = if matches!(frame, Frame::StreamData { .. }) {
                    if repair {
                        binding
                            .try_enqueue_repair_frame_for_target(&target, &frame, lane)
                            .map(|()| None)
                    } else {
                        binding
                            .try_enqueue_owner_frame_for_target(&target, &frame, lane, None, None)
                    }
                } else {
                    send_sender_service_frame_to_carrier(
                        &target.commands,
                        frame.clone(),
                        lane,
                        emit_mode,
                    )
                    .await
                    .map(|()| None)
                };
                match send_result {
                    Ok(_) => {
                        if matches!(frame, Frame::StreamData { .. }) {
                            if !repair {
                                let _ = binding.commit_ordered_data_owner_for_target(&target);
                            }
                        }
                        record_server_sender_decision(
                            binding.session_id(),
                            stream.stream_id,
                            target.key,
                            &frame,
                            lane,
                            reason,
                            Some(target.has_bulk_rate_evidence),
                        );
                        return Ok(Some(target.key));
                    }
                    Err(RuntimeError::SenderServiceBlocked) => {
                        return Err(RuntimeError::SenderServiceBlocked);
                    }
                    Err(err) => {
                        last_error = Some(err);
                        binding.detach(target.key, &target.commands);
                    }
                }
            }
        }
    }
}

async fn send_sender_service_frame_to_carrier(
    commands: &ReliablePathCommandSender,
    frame: Frame,
    lane: FlowLane,
    emit_mode: ResponseCarrierEmitMode,
) -> Result<(), RuntimeError> {
    // Sender-service dispatch must not await a path queue permit; queue-full is
    // explicit backpressure so the owner can keep work queued and continue
    // polling ACK/control/path feedback.
    match emit_mode {
        ResponseCarrierEmitMode::Classified => commands.try_enqueue_admitted_frame(frame, lane),
        ResponseCarrierEmitMode::StreamOrdered => {
            commands.try_enqueue_stream_ordered_frame(frame, lane)
        }
    }
}

pub(super) async fn send_sender_service_control_frame(
    stream: &ReliablePathStream,
    frame: Frame,
) -> Result<Option<CarrierPathKey>, RuntimeError> {
    // Setup/attach control that is emitted outside a long-lived response queue
    // still uses the same sender-service carrier gate: no blocking path permit,
    // no path-local fairness decision, and queue-full remains explicit
    // sender-service backpressure.
    emit_response_frame_from_sender_service(
        stream,
        frame,
        FlowLane::Control,
        ResponseCarrierEmitMode::Classified,
        "control",
        None,
    )
    .await
}

async fn emit_relay_path_frame(
    stream: &ReliablePathStreamHandle,
    frame: Frame,
    lane: FlowLane,
) -> Result<(), RuntimeError> {
    emit_relay_path_frame_with_mode(stream, frame, lane, ResponseCarrierEmitMode::Classified).await
}

async fn emit_relay_path_frame_with_mode(
    stream: &ReliablePathStreamHandle,
    frame: Frame,
    lane: FlowLane,
    emit_mode: ResponseCarrierEmitMode,
) -> Result<(), RuntimeError> {
    match &stream.output {
        ReliablePathStreamOutput::Fixed(fixed) => {
            send_sender_service_frame_to_carrier(fixed.commands(), frame, lane, emit_mode).await
        }
        ReliablePathStreamOutput::Switchable(_) => {
            Err(RuntimeError::Protocol("request relay path is not fixed"))
        }
    }
}

fn relay_cursor_distance(position: usize, cursor: usize, len: usize) -> usize {
    if len == 0 {
        return 0;
    }
    position.wrapping_add(len).wrapping_sub(cursor % len) % len
}

impl ServerResponseSenderService {
    #[cfg(test)]
    pub(super) fn new(session_id: SessionId, stream_id: StreamId) -> Self {
        Self::new_with_performance(session_id, stream_id, MppPerformanceConfig::default())
    }

    pub(super) fn new_with_performance(
        session_id: SessionId,
        stream_id: StreamId,
        performance: MppPerformanceConfig,
    ) -> Self {
        Self {
            session_id,
            stream_id,
            queue: ReliableRelaySenderQueue::default(),
            performance,
            extra_traffic: ExtraTrafficLedger::default(),
        }
    }

    pub(super) fn ack_gap_repair_path_snapshot(
        &self,
        path_stream: &ReliablePathStream,
        send_stream: &ReliableSendStream,
        normalized_ranges: &[OffsetRange],
        preview_limit: usize,
    ) -> Option<(ServerRepairOutputIdentity, PathSnapshot)> {
        let preview = send_stream
            .retransmission_frames_for_normalized_ack_gaps(normalized_ranges, preview_limit.max(1))
            .into_iter()
            .next()?;
        let ReliablePathStreamOutput::Switchable(binding) = &path_stream.output else {
            return None;
        };
        let avoid_keys = binding.flight_keys_overlapping_frame(&preview);
        let lane = response_repair_carrier_lane(&preview);
        let targets =
            binding.sender_path_targets(lane, reliable_stream_frame_payload_bytes(&preview));
        choose_response_sender_target(
            &targets,
            lane,
            &preview,
            ResponseCarrierEmitMode::Classified,
            binding.mux_limits(),
            &[],
            &avoid_keys,
            Some(RelaySendCause::PersistentAckGapRepair),
        )
        .map(|target| {
            (
                ServerRepairOutputIdentity {
                    key: target.key,
                    incarnation: target.incarnation,
                },
                target.snapshot,
            )
        })
    }

    pub(super) fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(super) fn bytes(&self) -> usize {
        self.queue.bytes()
    }

    pub(super) fn data_bytes(&self) -> usize {
        self.queue.data_bytes()
    }

    pub(super) fn release_normalized_acked_repairs(&mut self, ranges: &[OffsetRange]) -> usize {
        self.queue.release_normalized_acked_repairs(ranges)
    }

    pub(super) fn discard_unusable_live_owner_tail_repairs(
        &mut self,
        path_stream: &ReliablePathStream,
    ) -> usize {
        self.queue
            .discard_unusable_live_owner_tail_repairs(|frame| {
                path_stream.has_live_owner_tail_repair_output_for_frame(frame)
            })
    }

    pub(super) fn discard_stale_persistent_ack_gap_repairs(
        &mut self,
        path_stream: &ReliablePathStream,
    ) -> usize {
        self.queue
            .discard_stale_persistent_ack_gap_repairs(|cause| {
                cause.persistent_server_target().is_none_or(|target| {
                    path_stream.has_output_incarnation(target.key, target.incarnation)
                }) && cause.persistent_client_target().is_none()
            })
    }

    pub(super) fn persistent_ack_gap_repair_deadline(&self) -> Option<Instant> {
        self.queue.persistent_ack_gap_repair_deadline()
    }

    pub(super) fn extra_traffic_budget_remaining(&self, mux_limits: MuxLimits) -> usize {
        self.extra_traffic
            .budget(
                response_extra_traffic_startup_floor_bytes(mux_limits),
                self.performance,
            )
            .remaining_bytes()
    }

    pub(super) fn repair_extra_budget_remaining(&self, mux_limits: MuxLimits) -> usize {
        self.extra_traffic_budget_remaining(mux_limits)
    }

    pub(super) fn repair_extra_event_budget_remaining(&self, mux_limits: MuxLimits) -> usize {
        let remaining = self.repair_extra_budget_remaining(mux_limits);
        if remaining < response_repair_minimum_useful_attempt_bytes(mux_limits) {
            0
        } else {
            remaining
        }
    }

    #[cfg(test)]
    pub(super) fn record_owner_progress_for_test(&mut self, bytes: usize) {
        self.record_owner_progress(bytes);
    }

    pub(super) fn record_owner_progress(&mut self, bytes: usize) {
        self.extra_traffic.record_owner_progress(bytes);
    }

    pub(super) fn publish_queue_bytes(&self, path_stream: &ReliablePathStream) {
        path_stream.set_sender_queue_bytes(self.queue.bytes());
    }

    pub(super) fn queued_send_ready(&self) -> bool {
        self.queue.front().is_some()
    }

    pub(super) fn front_has_carrier_credit_with_ordered_owner_debt(
        &self,
        path_stream: &ReliablePathStream,
        send_stream: &ReliableSendStream,
        relay_lane: FlowLane,
        mux_limits: MuxLimits,
        ordered_owner_debt_bytes: usize,
    ) -> bool {
        let Some((_, queued)) = self.queue.front() else {
            return false;
        };
        match &queued.kind {
            ReliableRelayQueuedWorkKind::Control(frame) => {
                let (carrier_lane, emit_mode) = if queued.stream_ordered_carrier_emit {
                    (relay_lane, ResponseCarrierEmitMode::StreamOrdered)
                } else {
                    (FlowLane::Control, ResponseCarrierEmitMode::Classified)
                };
                response_frame_has_carrier_credit(path_stream, frame, carrier_lane, emit_mode, None)
            }
            ReliableRelayQueuedWorkKind::Data(payload) => response_dispatch_payload_bytes(
                path_stream,
                send_stream,
                queued.data_lane.unwrap_or(relay_lane),
                mux_limits,
                payload.len(),
            )
            .is_some_and(|payload_bytes| {
                plan_response_data_payload_with_ordered_debt_impl(
                    path_stream,
                    queued.data_lane.unwrap_or(relay_lane),
                    send_stream.next_offset(),
                    payload_bytes,
                    ordered_owner_debt_bytes,
                )
                .is_ok()
            }),
            ReliableRelayQueuedWorkKind::Repair { frame, cause } => {
                response_frame_has_carrier_credit(
                    path_stream,
                    frame,
                    response_repair_carrier_lane(frame),
                    ResponseCarrierEmitMode::Classified,
                    Some(*cause),
                )
            }
        }
    }

    pub(super) fn can_read_product_source(
        &self,
        local_open: bool,
        queued_send_blocked: bool,
        send_stream: &ReliableSendStream,
        mux_limits: MuxLimits,
        queue_limit: usize,
    ) -> bool {
        reliable_relay_can_read_product_source(
            local_open,
            queued_send_blocked,
            send_stream,
            &self.queue,
            mux_limits,
            queue_limit,
        )
    }

    pub(super) fn read_budget(
        &self,
        send_stream: &ReliableSendStream,
        mux_limits: MuxLimits,
        queue_limit: usize,
        buffer_len: usize,
    ) -> usize {
        reliable_relay_sender_queue_read_budget(
            send_stream,
            &self.queue,
            mux_limits,
            queue_limit,
            buffer_len,
        )
    }

    pub(super) fn enqueue_data_for_lane(&mut self, payload: Bytes, lane: FlowLane) -> u64 {
        self.queue.push_data_for_lane(payload, lane)
    }

    pub(super) fn enqueue_control_frame(&mut self, frame: Frame) -> u64 {
        self.queue.push_control(frame)
    }

    pub(super) fn enqueue_final_control_frame(&mut self, frame: Frame) -> u64 {
        self.queue.push_final_control(frame)
    }

    pub(super) fn enqueue_repair_frame_with_priority(
        &mut self,
        frame: Frame,
        mux_limits: MuxLimits,
        critical_priority: bool,
    ) -> Option<u64> {
        let payload_bytes = reliable_stream_frame_payload_bytes(&frame);
        debug_assert!(CarrierWorkKind::RepairData.counts_against_sender_extra_budget());
        let budget = self.extra_traffic.budget(
            response_extra_traffic_startup_floor_bytes(mux_limits),
            self.performance,
        );
        if !budget.can_spend(payload_bytes) {
            return None;
        }
        self.extra_traffic
            .record_optional(ExtraTrafficKind::Repair, payload_bytes);
        Some(if critical_priority {
            self.queue
                .push_critical_repair_with_cause(frame, RelaySendCause::AckGapRepair)
        } else {
            self.queue.push_repair(frame)
        })
    }

    #[cfg(test)]
    pub(super) fn enqueue_critical_repair_frame(&mut self, frame: Frame) -> u64 {
        self.enqueue_critical_repair_frame_with_cause(frame, RelaySendCause::AckGapRepair)
    }

    pub(super) fn enqueue_critical_tail_repair_frame(&mut self, frame: Frame) -> Option<u64> {
        if self.has_queued_repair_overlap(&frame) {
            return None;
        }
        Some(
            self.enqueue_critical_repair_frame_with_cause(frame, RelaySendCause::PathFailureRepair),
        )
    }

    pub(super) fn enqueue_critical_repair_frame_with_cause(
        &mut self,
        frame: Frame,
        cause: RelaySendCause,
    ) -> u64 {
        debug_assert!(cause.is_repair());
        let payload_bytes = reliable_stream_frame_payload_bytes(&frame);
        debug_assert!(CarrierWorkKind::RepairData.counts_against_sender_extra_budget());
        self.extra_traffic
            .record_optional(ExtraTrafficKind::Repair, payload_bytes);
        self.queue.push_critical_repair_with_cause(frame, cause)
    }

    pub(super) fn has_queued_repair_overlap(&self, frame: &Frame) -> bool {
        self.queue.has_queued_repair_overlap(frame)
    }

    pub(super) async fn dispatch_next(
        &mut self,
        path_stream: &ReliablePathStream,
        send_stream: &mut ReliableSendStream,
        relay_lane: FlowLane,
        mux_limits: MuxLimits,
    ) -> Result<ServerResponseDispatch, RuntimeError> {
        self.dispatch_next_with_ordered_owner_debt(
            path_stream,
            send_stream,
            relay_lane,
            mux_limits,
            0,
        )
        .await
    }

    pub(super) async fn dispatch_next_with_ordered_owner_debt(
        &mut self,
        path_stream: &ReliablePathStream,
        send_stream: &mut ReliableSendStream,
        relay_lane: FlowLane,
        mux_limits: MuxLimits,
        ordered_owner_debt_bytes: usize,
    ) -> Result<ServerResponseDispatch, RuntimeError> {
        let (queued_lane, queued) = self
            .queue
            .front()
            .expect("queued_send_ready requires a queued frame");
        let enqueue_id = {
            #[cfg(feature = "lab-diagnostics")]
            {
                queued.enqueue_id
            }
            #[cfg(not(feature = "lab-diagnostics"))]
            {
                0
            }
        };
        let queue_delay_ms = {
            #[cfg(feature = "lab-diagnostics")]
            {
                queued.queued_at.elapsed().as_millis()
            }
            #[cfg(not(feature = "lab-diagnostics"))]
            {
                0
            }
        };
        let (frame, dispatch_lane_name, repair_cause) = match &queued.kind {
            ReliableRelayQueuedWorkKind::Control(frame) => (frame.clone(), "control", None),
            ReliableRelayQueuedWorkKind::Data(payload) => {
                let data_lane = queued.data_lane.unwrap_or(relay_lane);
                let dispatch_payload_bytes = response_dispatch_payload_bytes(
                    path_stream,
                    send_stream,
                    data_lane,
                    mux_limits,
                    payload.len(),
                )
                .ok_or(RuntimeError::SenderServiceBlocked)?;
                let (dispatch_payload_bytes, planned) =
                    plan_response_data_payload_with_ordered_debt_impl(
                        path_stream,
                        data_lane,
                        send_stream.next_offset(),
                        dispatch_payload_bytes,
                        ordered_owner_debt_bytes,
                    )?;
                let dispatch_payload = payload.slice(..dispatch_payload_bytes);
                #[cfg(feature = "lab-diagnostics")]
                let mux_started = Instant::now();
                let frame = send_stream.send_data(dispatch_payload, StreamFlags::NONE)?;
                #[cfg(feature = "lab-diagnostics")]
                lab_perf_record(
                    "mux.send_data",
                    mux_started.elapsed(),
                    dispatch_payload_bytes,
                );
                match emit_planned_response_data_frame(
                    path_stream,
                    planned,
                    frame.clone(),
                    reliable_path_effective_frame_lane(&frame, data_lane),
                )
                .await
                {
                    Ok(outcome) => {
                        let committed = self
                            .queue
                            .commit_front_data_prefix(dispatch_payload_bytes)
                            .expect("dispatched queued data must still be at queue front");
                        return self.finish_dispatched_work(
                            path_stream,
                            relay_lane,
                            queued_lane,
                            committed,
                            frame,
                            outcome.selected_path,
                            "data",
                            enqueue_id,
                            queue_delay_ms,
                        );
                    }
                    Err(err) => {
                        let _ = send_stream.rollback_committed_data(&frame);
                        return Err(err);
                    }
                }
            }
            ReliableRelayQueuedWorkKind::Repair { frame, cause } => {
                (frame.clone(), "repair", Some(*cause))
            }
        };
        let selected_path = match queued_lane {
            ReliableRelayQueuedWorkLane::Control => {
                let (carrier_lane, emit_mode) = if queued.stream_ordered_carrier_emit {
                    (relay_lane, ResponseCarrierEmitMode::StreamOrdered)
                } else {
                    (FlowLane::Control, ResponseCarrierEmitMode::Classified)
                };
                emit_response_frame_from_sender_service(
                    path_stream,
                    frame.clone(),
                    carrier_lane,
                    emit_mode,
                    "control",
                    None,
                )
                .await?
            }
            ReliableRelayQueuedWorkLane::Data => match emit_response_frame_from_sender_service(
                path_stream,
                frame.clone(),
                reliable_path_effective_frame_lane(&frame, relay_lane),
                ResponseCarrierEmitMode::Classified,
                "data",
                None,
            )
            .await
            {
                Ok(selected_path) => selected_path,
                Err(err) => {
                    let _ = send_stream.rollback_committed_data(&frame);
                    return Err(err);
                }
            },
            ReliableRelayQueuedWorkLane::Repair => {
                emit_response_frame_from_sender_service(
                    path_stream,
                    frame.clone(),
                    response_repair_carrier_lane(&frame),
                    ResponseCarrierEmitMode::Classified,
                    "tail_repair",
                    repair_cause,
                )
                .await?
            }
        };
        let (_, committed) = self
            .queue
            .commit_front()
            .expect("dispatched queued work must still be at queue front");
        self.finish_dispatched_work(
            path_stream,
            relay_lane,
            queued_lane,
            committed,
            frame,
            selected_path,
            dispatch_lane_name,
            enqueue_id,
            queue_delay_ms,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn finish_dispatched_work(
        &mut self,
        path_stream: &ReliablePathStream,
        relay_lane: FlowLane,
        queued_lane: ReliableRelayQueuedWorkLane,
        committed: ReliableRelayQueuedWork,
        frame: Frame,
        selected_path: Option<CarrierPathKey>,
        dispatch_lane_name: &'static str,
        enqueue_id: u64,
        queue_delay_ms: u128,
    ) -> Result<ServerResponseDispatch, RuntimeError> {
        #[cfg(feature = "lab-diagnostics")]
        let send_lane = match queued_lane {
            ReliableRelayQueuedWorkLane::Control => FlowLane::Control,
            ReliableRelayQueuedWorkLane::Repair => response_repair_carrier_lane(&frame),
            ReliableRelayQueuedWorkLane::Data => reliable_path_effective_frame_lane(
                &frame,
                committed.data_lane.unwrap_or(relay_lane),
            ),
        };
        #[cfg(feature = "lab-diagnostics")]
        let pacing_bytes = frame_pacing_bytes(&frame);
        #[cfg(feature = "lab-diagnostics")]
        let stream_extent = match &frame {
            Frame::StreamData {
                offset, payload, ..
            } => Some((*offset, payload.len())),
            _ => None,
        };
        #[cfg(feature = "lab-diagnostics")]
        if let Some((offset, payload_bytes)) = stream_extent {
            if queued_lane == ReliableRelayQueuedWorkLane::Data {
                lab_server_response_stream_data(
                    self.session_id.0,
                    self.stream_id.0,
                    offset,
                    payload_bytes,
                );
            }
            if selected_path.is_none() {
                lab_sender_service_decision(
                    "server",
                    Some(self.session_id.0),
                    self.stream_id.0,
                    dispatch_lane_name,
                    "stream_data",
                    payload_bytes,
                    None,
                    format_args!(
                        "path_underlay={:?} path_id=none lane={:?} pacing_bytes={} degenerate_single_path=true",
                        path_stream.underlay, send_lane, pacing_bytes,
                    ),
                );
            } else if queued_lane == ReliableRelayQueuedWorkLane::Data
                && matches!(&path_stream.output, ReliablePathStreamOutput::Fixed(_))
            {
                let selected_path =
                    selected_path.expect("selected fixed output path must be available");
                lab_sender_service_decision(
                    "server",
                    Some(self.session_id.0),
                    self.stream_id.0,
                    dispatch_lane_name,
                    "stream_data",
                    payload_bytes,
                    None,
                    format_args!(
                        "path_underlay={:?} path_id={} lane={:?} pacing_bytes={} fixed_output=true",
                        selected_path.underlay, selected_path.path_id.0, send_lane, pacing_bytes,
                    ),
                );
            }
            if lab_diagnostic_event_enabled("server_sender_dispatch") {
                let (selected_underlay, selected_path_id) = selected_path
                    .map(|path| (format!("{:?}", path.underlay), path.path_id.0.to_string()))
                    .unwrap_or_else(|| ("none".to_string(), "none".to_string()));
                lab_diagnostic(
                    "server_sender_dispatch",
                    format_args!(
                        "session_id={} stream_id={} enqueue_id={} offset={} payload_bytes={} lane={:?} work_lane={:?} queue_delay_ms={} sender_queue_bytes_after={} selected_path_underlay={} selected_path_id={} pacing_bytes={}",
                        self.session_id.0,
                        self.stream_id.0,
                        enqueue_id,
                        offset,
                        payload_bytes,
                        send_lane,
                        queued_lane,
                        queue_delay_ms,
                        self.queue.bytes(),
                        selected_underlay,
                        selected_path_id,
                        pacing_bytes,
                    ),
                );
            }
        }
        #[cfg(not(feature = "lab-diagnostics"))]
        let _ = (
            path_stream,
            relay_lane,
            &frame,
            dispatch_lane_name,
            enqueue_id,
            queue_delay_ms,
        );
        Ok(ServerResponseDispatch {
            payload_bytes: committed.payload_bytes,
            lane: queued_lane,
            selected_path,
        })
    }
}

impl RelaySenderService {
    pub(super) fn new(stream_id: StreamId) -> Self {
        Self::new_with_performance(stream_id, MppPerformanceConfig::default())
    }

    pub(super) fn new_with_performance(
        stream_id: StreamId,
        performance: MppPerformanceConfig,
    ) -> Self {
        Self {
            stream_id,
            flights: RelayPathFlightLedger::default(),
            ordered_data_owner: None,
            ordered_data_owner_instance: None,
            request_subflow_set: None,
            request_startup_acked_bytes: HashMap::new(),
            request_startup_first_sent_at: HashMap::new(),
            request_startup_rate_evidence: HashSet::new(),
            request_startup_receipt_proofs: HashMap::new(),
            request_rate_evidence: HashMap::new(),
            request_rate_proven_subflows: HashSet::new(),
            request_ack_clock_proven_subflows: HashSet::new(),
            request_ack_clock_calibration_bytes: HashMap::new(),
            request_graduated_subflows: HashSet::new(),
            request_attempted_subflows: HashSet::new(),
            request_membership_generation: None,
            missing_owner_repair_attempts: HashMap::new(),
            next_send_index: 0,
            performance,
            extra_traffic: ExtraTrafficLedger::default(),
        }
    }

    fn extra_traffic_budget_remaining(&self, mux_limits: MuxLimits) -> usize {
        self.extra_traffic
            .budget(
                response_extra_traffic_startup_floor_bytes(mux_limits),
                self.performance,
            )
            .remaining_bytes()
    }

    pub(super) fn repair_extra_event_budget_remaining(&self, mux_limits: MuxLimits) -> usize {
        let remaining = self.extra_traffic_budget_remaining(mux_limits);
        if remaining < response_repair_minimum_useful_attempt_bytes(mux_limits) {
            0
        } else {
            remaining
        }
    }

    pub(super) fn enqueue_repair_frame_with_priority(
        &mut self,
        sender_queue: &mut ReliableRelaySenderQueue,
        frame: Frame,
        cause: RelaySendCause,
        mux_limits: MuxLimits,
        critical_priority: bool,
    ) -> bool {
        debug_assert!(cause.is_repair());
        let payload_bytes = reliable_stream_frame_payload_bytes(&frame);
        let budget = self.extra_traffic.budget(
            response_extra_traffic_startup_floor_bytes(mux_limits),
            self.performance,
        );
        if !budget.can_spend(payload_bytes) {
            return false;
        }
        self.extra_traffic
            .record_optional(ExtraTrafficKind::Repair, payload_bytes);
        if critical_priority {
            sender_queue.push_critical_repair_with_cause(frame, cause);
        } else {
            sender_queue.push_repair_with_cause(frame, cause);
        }
        true
    }

    pub(super) fn enqueue_critical_repair_frame(
        &mut self,
        sender_queue: &mut ReliableRelaySenderQueue,
        frame: Frame,
        cause: RelaySendCause,
    ) {
        debug_assert!(cause.is_repair());
        let payload_bytes = reliable_stream_frame_payload_bytes(&frame);
        self.extra_traffic
            .record_optional(ExtraTrafficKind::Repair, payload_bytes);
        sender_queue.push_critical_repair_with_cause(frame, cause);
    }

    pub(super) fn enqueue_critical_tail_repair_frame(
        &mut self,
        sender_queue: &mut ReliableRelaySenderQueue,
        frame: Frame,
    ) -> bool {
        if sender_queue.has_queued_repair_overlap(&frame) {
            return false;
        }
        self.enqueue_critical_repair_frame(sender_queue, frame, RelaySendCause::PathFailureRepair);
        true
    }

    #[cfg(test)]
    fn record_owner_progress_for_test(&mut self, bytes: usize) {
        self.record_owner_progress(bytes);
    }

    pub(super) fn record_owner_progress(&mut self, bytes: usize) {
        self.extra_traffic.record_owner_progress(bytes);
    }

    pub(super) async fn send_stream_data(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        frame: Frame,
    ) -> Result<RelaySendOutcome, RuntimeError> {
        self.send_frame(context, remotes, frame, RelaySendCause::StreamData)
            .await
    }

    pub(super) async fn send_control_frame(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        frame: Frame,
        cause: RelaySendCause,
    ) -> Result<RelaySendOutcome, RuntimeError> {
        debug_assert!(!cause.is_repair());
        self.send_frame(context, remotes, frame, cause).await
    }

    pub(super) async fn send_repair_frame(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        frame: Frame,
        cause: RelaySendCause,
    ) -> Result<RelaySendOutcome, RuntimeError> {
        debug_assert!(cause.is_repair());
        self.send_frame(context, remotes, frame, cause).await
    }

    pub(super) fn ack_gap_repair_path_model(
        &self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        send_stream: &ReliableSendStream,
        normalized_ranges: &[OffsetRange],
        preview_limit: usize,
        lane: FlowLane,
    ) -> (
        Option<UnderlayProtocol>,
        Option<PathSnapshot>,
        Option<(ClientRepairOutputIdentity, PathSnapshot)>,
    ) {
        let Some(preview) = send_stream
            .retransmission_frames_for_normalized_ack_gaps(normalized_ranges, preview_limit.max(1))
            .into_iter()
            .next()
        else {
            return (None, None, None);
        };
        let owner_underlay = self.flights.ordering_owner_underlay_for_frame(&preview);
        let owner_timing_path = self
            .flights
            .ordering_owner_keys_for_frame_any_instance(&preview)
            .into_iter()
            .filter_map(|key| context.reliable_path_snapshot(key))
            .max_by(|left, right| {
                transport_pto_from_snapshot(Some(*left))
                    .cmp(&transport_pto_from_snapshot(Some(*right)))
            });
        let avoid_keys = self.flights.sent_keys_for_frame(&preview);
        let repair_path = self
            .choose_lowest_eta_relay_path(
                context,
                remotes,
                &preview,
                lane,
                RelaySendCause::PersistentAckGapRepair,
                &avoid_keys,
                false,
            )
            .ok()
            .and_then(|position| {
                let key = remotes.paths[position].key();
                let instance = remotes.paths[position].instance();
                context
                    .relay_path_has_bulk_model_evidence(key.underlay, key.index)
                    .then(|| {
                        context
                            .reliable_path_snapshot(key)
                            .map(|snapshot| (ClientRepairOutputIdentity { instance }, snapshot))
                    })
                    .flatten()
            });
        (owner_underlay, owner_timing_path, repair_path)
    }

    pub(super) async fn dispatch_client_queued_work(
        &mut self,
        context: &ClientPathContext,
        spec: &ReliableRelayOpenSpec,
        relay_lane: FlowLane,
        remotes: &mut ReliableRelayRemoteSet,
        send_stream: &mut ReliableSendStream,
        sender_queue: &mut ReliableRelaySenderQueue,
        local_open: bool,
        data_quantum_bytes: usize,
    ) -> Result<ClientQueuedDispatch, RuntimeError> {
        let queued_kind = sender_queue
            .front()
            .map(|(_, queued)| queued.kind.clone())
            .expect("queued_send_ready requires queued data");
        match queued_kind {
            ReliableRelayQueuedWorkKind::Control(_) => {
                Err(RuntimeError::Protocol("client sender queue control item"))
            }
            ReliableRelayQueuedWorkKind::Data(payload) => {
                self.dispatch_client_data_work(
                    context,
                    spec,
                    relay_lane,
                    remotes,
                    send_stream,
                    sender_queue,
                    local_open,
                    payload,
                    data_quantum_bytes,
                )
                .await
            }
            ReliableRelayQueuedWorkKind::Repair { frame, cause } => {
                self.dispatch_client_repair_work(
                    context,
                    spec,
                    relay_lane,
                    remotes,
                    send_stream,
                    sender_queue,
                    local_open,
                    frame,
                    cause,
                )
                .await
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn dispatch_client_data_work(
        &mut self,
        context: &ClientPathContext,
        spec: &ReliableRelayOpenSpec,
        relay_lane: FlowLane,
        remotes: &mut ReliableRelayRemoteSet,
        send_stream: &mut ReliableSendStream,
        sender_queue: &mut ReliableRelaySenderQueue,
        local_open: bool,
        payload: Bytes,
        data_quantum_bytes: usize,
    ) -> Result<ClientQueuedDispatch, RuntimeError> {
        let dispatch_payload_bytes = data_quantum_bytes.min(payload.len()).max(1);
        let dispatch_payload = payload.slice(..dispatch_payload_bytes);
        let frame = send_stream
            .send_data(dispatch_payload, StreamFlags::NONE)
            .map_err(RuntimeError::Stream)?;
        let retry_frame = frame.clone();
        match self.send_stream_data(context, remotes, frame.clone()).await {
            Ok(outcome) => {
                let committed = sender_queue
                    .commit_front_data_prefix(dispatch_payload_bytes)
                    .expect("sent queued data must still be at queue front");
                let _ = outcome;
                Ok(ClientQueuedDispatch::Data {
                    payload_bytes: committed.payload_bytes,
                })
            }
            Err(RuntimeError::SenderServiceBlocked) => {
                let _ = send_stream.rollback_committed_data(&frame);
                Err(RuntimeError::SenderServiceBlocked)
            }
            Err(err) if reliable_relay_error_is_migratable(&err) => {
                let _ = send_stream.rollback_committed_data(&frame);
                match attach_reliable_relay_paths(
                    context,
                    spec,
                    relay_lane,
                    remotes,
                    send_stream,
                    !local_open,
                    ReliableRelayAttachMode::Any,
                )
                .await
                {
                    Ok(attached) if attached > 0 => {
                        if let Err(err) = send_stream.commit_prepared_data(&frame) {
                            return Err(RuntimeError::Stream(err));
                        }
                        match self.send_stream_data(context, remotes, retry_frame).await {
                            Ok(outcome) => {
                                let committed = sender_queue
                                    .commit_front_data_prefix(dispatch_payload_bytes)
                                    .expect("sent queued data must still be at queue front");
                                let _ = outcome;
                                Ok(ClientQueuedDispatch::Data {
                                    payload_bytes: committed.payload_bytes,
                                })
                            }
                            Err(RuntimeError::SenderServiceBlocked) => {
                                let _ = send_stream.rollback_committed_data(&frame);
                                Err(RuntimeError::SenderServiceBlocked)
                            }
                            Err(err) if reliable_relay_error_is_migratable(&err) => {
                                let _ = send_stream.rollback_committed_data(&frame);
                                Err(err)
                            }
                            Err(err) => {
                                let _ = send_stream.rollback_committed_data(&frame);
                                Err(err)
                            }
                        }
                    }
                    Ok(_) => Err(err),
                    Err(err) => Err(err),
                }
            }
            Err(err) => {
                let _ = send_stream.rollback_committed_data(&frame);
                Err(err)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn dispatch_client_repair_work(
        &mut self,
        context: &ClientPathContext,
        spec: &ReliableRelayOpenSpec,
        relay_lane: FlowLane,
        remotes: &mut ReliableRelayRemoteSet,
        send_stream: &mut ReliableSendStream,
        sender_queue: &mut ReliableRelaySenderQueue,
        local_open: bool,
        frame: Frame,
        cause: RelaySendCause,
    ) -> Result<ClientQueuedDispatch, RuntimeError> {
        let retry_frame = frame.clone();
        match self.send_repair_frame(context, remotes, frame, cause).await {
            Ok(outcome) => {
                let (_, committed) = sender_queue
                    .commit_front()
                    .expect("sent queued repair must still be at queue front");
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "repair",
                    format_args!(
                        "stream_id={} path_underlay={:?} path_index={} cause={} queued_dispatch=true payload_bytes={}",
                        self.stream_id.0,
                        outcome.path_key.underlay,
                        outcome.path_key.index,
                        cause.as_str(),
                        committed.payload_bytes,
                    ),
                );
                #[cfg(not(feature = "lab-diagnostics"))]
                let _ = outcome;
                Ok(ClientQueuedDispatch::Repair {
                    payload_bytes: committed.payload_bytes,
                })
            }
            Err(RuntimeError::SenderServiceBlocked) => Err(RuntimeError::SenderServiceBlocked),
            Err(err)
                if matches!(cause, RelaySendCause::PersistentClientAckGapRepair(_))
                    && reliable_relay_error_is_migratable(&err) =>
            {
                let discarded = sender_queue.discard_persistent_ack_gap_repair_batch(cause);
                debug_assert!(discarded > 0);
                Ok(ClientQueuedDispatch::PersistentRepairCancelled)
            }
            Err(err)
                if cause == RelaySendCause::LiveOwnerTailRepair
                    && reliable_relay_error_is_migratable(&err) =>
            {
                let (_, _) = sender_queue
                    .commit_front()
                    .expect("deferred live-tail repair must still be at queue front");
                Ok(ClientQueuedDispatch::RepairDeferred)
            }
            Err(err) if reliable_relay_error_is_migratable(&err) => {
                match attach_reliable_relay_paths(
                    context,
                    spec,
                    relay_lane,
                    remotes,
                    send_stream,
                    !local_open,
                    ReliableRelayAttachMode::Any,
                )
                .await
                {
                    Ok(attached) if attached > 0 => {
                        match self
                            .send_repair_frame(context, remotes, retry_frame, cause)
                            .await
                        {
                            Ok(outcome) => {
                                let (_, committed) = sender_queue
                                    .commit_front()
                                    .expect("sent queued repair must still be at queue front");
                                #[cfg(feature = "lab-diagnostics")]
                                lab_diagnostic(
                                    "repair",
                                    format_args!(
                                        "stream_id={} path_underlay={:?} path_index={} cause={} queued_dispatch=true after_attach=true payload_bytes={}",
                                        self.stream_id.0,
                                        outcome.path_key.underlay,
                                        outcome.path_key.index,
                                        cause.as_str(),
                                        committed.payload_bytes,
                                    ),
                                );
                                #[cfg(not(feature = "lab-diagnostics"))]
                                let _ = outcome;
                                Ok(ClientQueuedDispatch::Repair {
                                    payload_bytes: committed.payload_bytes,
                                })
                            }
                            Err(RuntimeError::SenderServiceBlocked) => {
                                Err(RuntimeError::SenderServiceBlocked)
                            }
                            Err(err) => Err(err),
                        }
                    }
                    Ok(_) => Err(err),
                    Err(err) => Err(err),
                }
            }
            Err(err) => Err(err),
        }
    }

    async fn send_frame(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        frame: Frame,
        cause: RelaySendCause,
    ) -> Result<RelaySendOutcome, RuntimeError> {
        let sent_frame = frame.clone();
        let avoid_keys = match cause {
            RelaySendCause::LiveOwnerTailRepair => self.flights.live_owner_tail_repair_owner_keys(
                &sent_frame,
                &remotes.path_instances(),
                Duration::ZERO,
                Duration::ZERO,
            ),
            cause if cause.is_repair() => self.flights.sent_keys_for_frame(&sent_frame),
            _ => Vec::new(),
        };
        let instance = self
            .emit_relay_frame(context, remotes, frame, cause, &avoid_keys)
            .await?;
        let path_key = instance.key;
        let payload_bytes = if cause.is_repair() {
            self.flights
                .record_repair_frame_instance(instance, &sent_frame)
        } else {
            self.flights
                .record_owner_frame_instance(instance, &sent_frame)
        };
        self.record_decision(path_key, payload_bytes, &sent_frame, cause);
        Ok(RelaySendOutcome { path_key })
    }

    async fn emit_relay_frame(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        frame: Frame,
        cause: RelaySendCause,
        avoid_keys: &[RelayPathKey],
    ) -> Result<RelayPathInstance, RuntimeError> {
        let mut last_error = None;
        while !remotes.paths.is_empty() {
            let stream_lane = remotes
                .paths
                .last()
                .map(|path| path.stream.lane)
                .unwrap_or(FlowLane::Latency);
            if stream_lane.is_bulk()
                && matches!(frame, Frame::StreamData { .. })
                && !cause.is_repair()
            {
                remotes.retry_pending_path_proofs(context);
            }
            let selection = match self.choose_relay_path_position(
                context,
                remotes,
                &frame,
                stream_lane,
                cause,
                avoid_keys,
            ) {
                Ok(selection) => selection,
                Err(RuntimeError::SenderServiceBlocked) => {
                    return Err(RuntimeError::SenderServiceBlocked);
                }
                Err(err) => return Err(last_error.unwrap_or(err)),
            };
            let RelayPathSendSelection {
                position,
                data_role,
                request_subflow_rollback,
                request_attempted_rollback,
                request_calibration_rollback,
            } = selection;
            let instance = remotes.paths[position].instance();
            let (lane, emit_mode) = if matches!(cause, RelaySendCause::StreamFin) {
                (
                    remotes.paths[position].stream.lane,
                    ResponseCarrierEmitMode::StreamOrdered,
                )
            } else {
                (
                    reliable_path_effective_frame_lane(&frame, remotes.paths[position].stream.lane),
                    ResponseCarrierEmitMode::Classified,
                )
            };
            match emit_relay_path_frame_with_mode(
                &remotes.paths[position].stream,
                frame.clone(),
                lane,
                emit_mode,
            )
            .await
            {
                Ok(()) => {
                    if matches!(frame, Frame::StreamData { .. }) {
                        let sent_bytes = reliable_stream_frame_payload_bytes(&frame);
                        context.record_relay_path_send(
                            instance.key.underlay,
                            instance.key.index,
                            sent_bytes,
                        );
                        if data_role == Some(PathRuntimeRole::Service) {
                            self.ordered_data_owner = Some(instance.key);
                            self.ordered_data_owner_instance = Some(instance);
                        } else if data_role == Some(PathRuntimeRole::Subflow)
                            && self
                                .request_subflow_set
                                .as_ref()
                                .and_then(FlowSubflowSet::startup_owner_key)
                                == Some(instance)
                        {
                            self.request_startup_first_sent_at
                                .entry(instance)
                                .or_insert_with(Instant::now);
                            self.try_enqueue_request_startup_receipt_proof(
                                context, remotes, instance,
                            );
                        }
                    }
                    self.next_send_index = if remotes.paths.is_empty() {
                        0
                    } else {
                        (position + 1) % remotes.paths.len()
                    };
                    return Ok(instance);
                }
                Err(RuntimeError::SenderServiceBlocked) => {
                    if let Some(previous) = request_subflow_rollback {
                        self.request_subflow_set = previous;
                    }
                    if let Some(instance) = request_attempted_rollback {
                        self.request_attempted_subflows.remove(&instance);
                    }
                    self.rollback_request_ack_clock_calibration(request_calibration_rollback);
                    return Err(RuntimeError::SenderServiceBlocked);
                }
                Err(err) => {
                    if let Some(previous) = request_subflow_rollback {
                        self.request_subflow_set = previous;
                    }
                    if let Some(instance) = request_attempted_rollback {
                        self.request_attempted_subflows.remove(&instance);
                    }
                    self.rollback_request_ack_clock_calibration(request_calibration_rollback);
                    last_error = Some(err);
                    if self.ordered_data_owner_instance == Some(instance) {
                        self.ordered_data_owner = None;
                        self.ordered_data_owner_instance = None;
                        self.reset_request_subflow_epoch();
                    } else if self
                        .request_subflow_set
                        .as_ref()
                        .and_then(FlowSubflowSet::startup_owner_key)
                        == Some(instance)
                    {
                        self.request_subflow_set = None;
                        self.request_startup_acked_bytes.remove(&instance);
                        self.request_startup_first_sent_at.remove(&instance);
                        self.request_startup_rate_evidence.remove(&instance);
                        self.request_startup_receipt_proofs.remove(&instance);
                        self.request_graduated_subflows.remove(&instance);
                    }
                    remotes.fail_path_instance(context, instance).await;
                    if !remotes.paths.is_empty() {
                        self.next_send_index %= remotes.paths.len();
                    } else {
                        self.next_send_index = 0;
                    }
                }
            }
        }
        Err(last_error.unwrap_or(RuntimeError::ReliablePathSessionClosed))
    }

    fn choose_relay_path_position(
        &mut self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        frame: &Frame,
        lane: FlowLane,
        cause: RelaySendCause,
        avoid_keys: &[RelayPathKey],
    ) -> Result<RelayPathSendSelection, RuntimeError> {
        if remotes.paths.is_empty() {
            return Err(RuntimeError::ReliablePathSessionClosed);
        }
        self.retry_request_startup_receipt_proof(context, remotes);
        if !cause.is_repair()
            && let Some((offset, _, _)) = reliable_stream_frame_extent(frame)
            && self
                .flights
                .has_missing_ordering_owner_before_offset(offset, &remotes.path_instances())
        {
            return Err(RuntimeError::SenderServiceBlocked);
        }
        self.next_send_index %= remotes.paths.len();
        if self.ordered_data_owner_instance.is_none()
            && let Some(owner) = self.ordered_data_owner
        {
            self.ordered_data_owner_instance = remotes
                .paths
                .iter()
                .find(|path| path.key() == owner)
                .map(ReliableRelayRemotePath::instance);
        }
        if self
            .ordered_data_owner_instance
            .is_some_and(|owner| !remotes.contains_path_instance(owner))
        {
            self.ordered_data_owner = None;
            self.ordered_data_owner_instance = None;
            self.reset_request_subflow_epoch();
        }
        self.reconcile_request_subflow_set(context, remotes);
        if matches!(frame, Frame::StreamData { .. }) && !cause.is_repair() {
            let payload_bytes = reliable_stream_frame_payload_bytes(frame);
            let sealed_owner = self.request_subflow_set.as_mut().and_then(|epoch| {
                let owner = epoch.startup_owner_key()?;
                epoch
                    .seal_startup_owner_if_next_frame_exceeds_credit(owner, payload_bytes)
                    .then_some(owner)
            });
            if let Some(owner) = sealed_owner {
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "request_startup_receipt",
                    format_args!(
                        "phase=sealed stream_id={} underlay={:?} path_index={} instance_id={} next_payload_bytes={}",
                        self.stream_id.0,
                        owner.key.underlay,
                        owner.key.index,
                        owner.id,
                        payload_bytes,
                    ),
                );
                self.try_enqueue_request_startup_receipt_proof(context, remotes, owner);
            }
            match choose_bulk_relay_path_avoiding(BulkRelayFrameRequest {
                stream_id: remotes.stream_id(),
                context,
                paths: &remotes.paths,
                lane,
                frame,
                cursor: self.next_send_index,
                avoid_keys,
                path_flights: Some(&self.flights),
                ordered_data_owner: self.ordered_data_owner,
                subflow_set: self.request_subflow_set.as_ref(),
                proven_subflows: Some(&self.request_rate_proven_subflows),
                graduated_subflows: Some(&self.request_graduated_subflows),
                attempted_subflows: Some(&self.request_attempted_subflows),
                ack_clock_calibration: Some(RequestAckClockCalibration {
                    proven_subflows: &self.request_ack_clock_proven_subflows,
                    spent_bytes: &self.request_ack_clock_calibration_bytes,
                }),
            }) {
                BulkRelayPathChoice::Selected(position) => {
                    let key = remotes.paths[position].key();
                    let role = if self.ordered_data_owner.is_none_or(|owner| owner == key) {
                        PathRuntimeRole::Service
                    } else {
                        PathRuntimeRole::Subflow
                    };
                    return Ok(RelayPathSendSelection::new(position, Some(role)));
                }
                BulkRelayPathChoice::SelectedStartupSubflow {
                    position,
                    service,
                    candidate,
                } => {
                    let payload_bytes = reliable_stream_frame_payload_bytes(frame);
                    let (previous, newly_attempted) = self.reserve_request_startup_subflow(
                        context,
                        service,
                        candidate,
                        payload_bytes,
                    )?;
                    return Ok(RelayPathSendSelection {
                        position,
                        data_role: Some(PathRuntimeRole::Subflow),
                        request_subflow_rollback: Some(previous),
                        request_attempted_rollback: newly_attempted.then_some(candidate),
                        request_calibration_rollback: None,
                    });
                }
                BulkRelayPathChoice::SelectedAckClockCalibration {
                    position,
                    candidate,
                } => {
                    let payload_bytes = reliable_stream_frame_payload_bytes(frame) as u64;
                    let previous = self.request_ack_clock_calibration_bytes.insert(
                        candidate,
                        self.request_ack_clock_calibration_bytes
                            .get(&candidate)
                            .copied()
                            .unwrap_or(0)
                            .saturating_add(payload_bytes),
                    );
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "ack_clock_calibration",
                        format_args!(
                            "phase=selected stream_id={} underlay={:?} path_index={} instance_id={} payload_bytes={} spent_bytes={}",
                            self.stream_id.0,
                            candidate.key.underlay,
                            candidate.key.index,
                            candidate.id,
                            payload_bytes,
                            self.request_ack_clock_calibration_bytes[&candidate],
                        ),
                    );
                    return Ok(RelayPathSendSelection {
                        position,
                        data_role: Some(PathRuntimeRole::Subflow),
                        request_subflow_rollback: None,
                        request_attempted_rollback: None,
                        request_calibration_rollback: Some((candidate, previous)),
                    });
                }
                BulkRelayPathChoice::Blocked => return Err(RuntimeError::SenderServiceBlocked),
                BulkRelayPathChoice::NotApplicable => {}
            }
        }
        let position = self.choose_lowest_eta_relay_path(
            context,
            remotes,
            frame,
            lane,
            cause,
            avoid_keys,
            matches!(frame, Frame::StreamData { .. }) && !cause.is_repair(),
        )?;
        let data_role = if matches!(frame, Frame::StreamData { .. }) && !cause.is_repair() {
            Some(PathRuntimeRole::Service)
        } else {
            None
        };
        Ok(RelayPathSendSelection::new(position, data_role))
    }

    fn reset_request_subflow_epoch(&mut self) {
        self.request_subflow_set = None;
        self.request_startup_acked_bytes.clear();
        self.request_startup_first_sent_at.clear();
        self.request_startup_rate_evidence.clear();
        self.request_startup_receipt_proofs.clear();
    }

    fn rollback_request_ack_clock_calibration(
        &mut self,
        rollback: Option<(RelayPathInstance, Option<u64>)>,
    ) {
        let Some((instance, previous)) = rollback else {
            return;
        };
        if let Some(previous) = previous {
            self.request_ack_clock_calibration_bytes
                .insert(instance, previous);
        } else {
            self.request_ack_clock_calibration_bytes.remove(&instance);
        }
    }

    fn retry_request_startup_receipt_proof(
        &mut self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
    ) {
        let Some(owner) = self
            .request_subflow_set
            .as_ref()
            .and_then(FlowSubflowSet::startup_owner_key)
        else {
            return;
        };
        self.try_enqueue_request_startup_receipt_proof(context, remotes, owner);
    }

    fn try_enqueue_request_startup_receipt_proof(
        &mut self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        owner: RelayPathInstance,
    ) {
        let proof_generation = context
            .relay_path_proof_generation(owner.key.underlay, owner.key.index)
            .unwrap_or(0);
        if self
            .request_startup_receipt_proofs
            .get(&owner)
            .is_some_and(|(_, generation)| *generation == proof_generation)
            || !self
                .request_subflow_set
                .as_ref()
                .is_some_and(|epoch| epoch.startup_owner_sample_sealed(owner))
        {
            return;
        }
        let Some(path) = remotes.paths.iter().find(|path| {
            path.instance() == owner && path.placement == RelayPathPlacement::Validation
        }) else {
            return;
        };
        match path
            .stream
            .enqueue_stream_ordered_path_proof(path.stream.lane)
        {
            Ok(Some(proof_id)) => {
                self.request_startup_receipt_proofs
                    .insert(owner, (proof_id, proof_generation));
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "request_startup_receipt",
                    format_args!(
                        "phase=queued stream_id={} underlay={:?} path_index={} instance_id={} proof_id={} proof_generation={}",
                        self.stream_id.0,
                        owner.key.underlay,
                        owner.key.index,
                        owner.id,
                        proof_id,
                        proof_generation,
                    ),
                );
            }
            Ok(None) => {
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "request_startup_receipt",
                    format_args!(
                        "phase=unsupported stream_id={} underlay={:?} path_index={} instance_id={} proof_generation={}",
                        self.stream_id.0,
                        owner.key.underlay,
                        owner.key.index,
                        owner.id,
                        proof_generation,
                    ),
                );
            }
            Err(err) => {
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "request_startup_receipt",
                    format_args!(
                        "phase=queue_failed stream_id={} underlay={:?} path_index={} instance_id={} proof_generation={} error={}",
                        self.stream_id.0,
                        owner.key.underlay,
                        owner.key.index,
                        owner.id,
                        proof_generation,
                        err,
                    ),
                );
                #[cfg(not(feature = "lab-diagnostics"))]
                let _ = err;
            }
        }
    }

    fn reconcile_request_subflow_set(
        &mut self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
    ) {
        let membership_generation = remotes.membership_generation();
        if self.request_membership_generation != Some(membership_generation) {
            let live_instances = remotes.path_instances().into_iter().collect::<HashSet<_>>();
            self.request_attempted_subflows
                .retain(|instance| live_instances.contains(instance));
            self.request_graduated_subflows
                .retain(|instance| live_instances.contains(instance));
            self.request_rate_proven_subflows
                .retain(|instance| live_instances.contains(instance));
            self.request_ack_clock_proven_subflows
                .retain(|instance| live_instances.contains(instance));
            self.request_ack_clock_calibration_bytes
                .retain(|instance, _| live_instances.contains(instance));
            self.request_rate_evidence
                .retain(|instance, _| live_instances.contains(instance));
            self.request_startup_acked_bytes
                .retain(|instance, _| live_instances.contains(instance));
            self.request_startup_first_sent_at
                .retain(|instance, _| live_instances.contains(instance));
            self.request_startup_rate_evidence
                .retain(|instance| live_instances.contains(instance));
            self.request_startup_receipt_proofs
                .retain(|instance, _| live_instances.contains(instance));
            self.request_membership_generation = Some(membership_generation);
        }
        let service = self.ordered_data_owner_instance.filter(|owner| {
            remotes.paths.iter().any(|path| {
                path.instance() == *owner && path.placement == RelayPathPlacement::Active
            })
        });
        if self
            .request_subflow_set
            .as_ref()
            .is_some_and(|epoch| service.is_none_or(|service| epoch.service_key() != service))
        {
            self.reset_request_subflow_epoch();
            return;
        }
        let Some(owner) = self
            .request_subflow_set
            .as_ref()
            .and_then(FlowSubflowSet::startup_owner_key)
        else {
            return;
        };
        if !remotes.paths.iter().any(|path| {
            path.instance() == owner && path.placement == RelayPathPlacement::Validation
        }) {
            self.request_subflow_set = None;
            self.request_startup_acked_bytes.remove(&owner);
            self.request_startup_first_sent_at.remove(&owner);
            self.request_startup_rate_evidence.remove(&owner);
            self.request_startup_receipt_proofs.remove(&owner);
            return;
        }
        let required_evidence_bytes = self
            .request_subflow_set
            .as_ref()
            .and_then(|epoch| epoch.startup_owner_sealed_sample_bytes(owner))
            .unwrap_or(u64::MAX);
        let receipt_acked_at = self
            .request_startup_receipt_proofs
            .get(&owner)
            .copied()
            .and_then(|(proof_id, generation)| {
                (context.relay_path_proof_generation(owner.key.underlay, owner.key.index)
                    == Some(generation))
                .then_some(proof_id)
            })
            .and_then(|proof_id| {
                remotes
                    .paths
                    .iter()
                    .find(|path| path.instance() == owner)
                    .and_then(|path| {
                        context.relay_path_fresh_proof_acked_at(
                            owner.key.underlay,
                            owner.key.index,
                            proof_id,
                            path.attached_at,
                        )
                    })
            });
        if let Some(receipt_acked_at) = receipt_acked_at
            && !self.request_startup_rate_evidence.contains(&owner)
            && let Some(first_sent_at) = self.request_startup_first_sent_at.get(&owner).copied()
            && let Some(sample) = PathRateSample::new(
                required_evidence_bytes,
                receipt_acked_at.saturating_duration_since(first_sent_at),
            )
        {
            self.request_startup_rate_evidence.insert(owner);
            self.request_rate_proven_subflows.insert(owner);
            context.mark_relay_path_rate_sample(owner.key.underlay, owner.key.index, sample);
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "request_startup_receipt",
                format_args!(
                    "phase=rate_sample stream_id={} underlay={:?} path_index={} instance_id={} evidence_bytes={} elapsed_us={} rate_bps={}",
                    self.stream_id.0,
                    owner.key.underlay,
                    owner.key.index,
                    owner.id,
                    required_evidence_bytes,
                    receipt_acked_at
                        .saturating_duration_since(first_sent_at)
                        .as_micros(),
                    sample.rate_bps(),
                ),
            );
        }
        if self.request_startup_rate_evidence.contains(&owner)
            && !self.flights.has_ordering_owner_flights_for_instance(owner)
            && let Some(epoch) = self.request_subflow_set.as_mut()
        {
            let graduated = epoch.graduate_startup_owner(owner);
            debug_assert!(graduated);
            self.request_graduated_subflows.insert(owner);
            self.request_startup_receipt_proofs.remove(&owner);
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "request_startup_receipt",
                format_args!(
                    "phase=graduated stream_id={} underlay={:?} path_index={} instance_id={}",
                    self.stream_id.0, owner.key.underlay, owner.key.index, owner.id,
                ),
            );
        }
    }

    fn reserve_request_startup_subflow(
        &mut self,
        context: &ClientPathContext,
        service: RelayPathInstance,
        candidate: RelayPathInstance,
        payload_bytes: usize,
    ) -> Result<(Option<FlowSubflowSet<RelayPathInstance>>, bool), RuntimeError> {
        let startup_credit = usize::try_from(reliable_subflow_startup_sample_limit_bytes(
            context.mux_limits,
        ))
        .unwrap_or(usize::MAX);
        let previous = self.request_subflow_set.clone();
        let mut epoch = previous
            .as_ref()
            .filter(|epoch| epoch.matches_envelope(service, startup_credit, 0, Duration::ZERO))
            .cloned()
            .unwrap_or_else(|| FlowSubflowSet::new(0, service, startup_credit, 0, Duration::ZERO));
        let input = SubflowAdmissionInput {
            key: candidate,
            bulk_rate_proven: false,
            startup_owner_allowed: true,
            frontier_clear: true,
            completion_improves: false,
            observed_goodput_non_degrading: true,
            read_gap: Duration::ZERO,
            owner_bytes: payload_bytes,
            optional_overhead_bytes: 0,
        };
        if epoch.admit_subflow_owner(input).decision != PathAdmissionDecision::AdmitSubflow {
            return Err(RuntimeError::SenderServiceBlocked);
        }
        self.request_subflow_set = Some(epoch);
        let newly_attempted = self.request_attempted_subflows.insert(candidate);
        Ok((previous, newly_attempted))
    }

    fn choose_lowest_eta_relay_path(
        &self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        frame: &Frame,
        lane: FlowLane,
        cause: RelaySendCause,
        avoid_keys: &[RelayPathKey],
        ordinary_stream_data: bool,
    ) -> Result<usize, RuntimeError> {
        let persistent_ack_gap_repair = cause.is_persistent_ack_gap_repair();
        let required_persistent_target = cause.persistent_client_target();
        let invalid_persistent_target =
            matches!(cause, RelaySendCause::PersistentServerAckGapRepair(_));
        let requires_distinct_output =
            cause == RelaySendCause::LiveOwnerTailRepair || persistent_ack_gap_repair;
        let payload_bytes = reliable_stream_frame_payload_bytes(frame);
        if cause == RelaySendCause::RecvProgressRecovery
            && let Some(position) = choose_repair_recv_progress_path_position(remotes, frame, cause)
        {
            return Ok(position);
        }
        if cause.is_recv_progress() {
            if let Some(position) = choose_active_recv_progress_path_position(remotes, frame, cause)
            {
                return Ok(position);
            }
        }
        let has_active_path = remotes
            .paths
            .iter()
            .any(|path| path.placement == RelayPathPlacement::Active);
        let ordinary_path_allowed = |path: &ReliableRelayRemotePath| {
            (!ordinary_stream_data
                || !has_active_path
                || path.placement == RelayPathPlacement::Active)
                && (cause != RelaySendCause::RecvProgressRecovery
                    || path.placement != RelayPathPlacement::Validation)
                && (cause != RelaySendCause::LiveOwnerTailRepair
                    || path.placement != RelayPathPlacement::Validation)
                && !invalid_persistent_target
                && required_persistent_target.is_none_or(|required| path.instance() == required)
                && (!persistent_ack_gap_repair
                    || context
                        .relay_path_has_bulk_model_evidence(path.key().underlay, path.key().index))
        };
        let can_enqueue = |path: &ReliableRelayRemotePath| {
            relay_path_can_enqueue_frame_for_cause_now(path, frame, cause)
        };
        let choose = |prefer_avoiding: bool| {
            remotes
                .paths
                .iter()
                .enumerate()
                .filter(|(_, path)| !prefer_avoiding || !avoid_keys.contains(&path.key()))
                .filter(|(_, path)| ordinary_path_allowed(path))
                .filter(|(_, path)| can_enqueue(path))
                .filter_map(|(position, path)| {
                    let key = path.key();
                    let snapshot = context.reliable_path_snapshot(key)?;
                    let score = scheduler::score_path(
                        snapshot,
                        lane,
                        payload_bytes,
                        SchedulerPolicy::default(),
                    )?;
                    Some((
                        position,
                        score.eta_ms,
                        relay_cursor_distance(position, self.next_send_index, remotes.paths.len()),
                    ))
                })
                .min_by(|left, right| {
                    left.1
                        .total_cmp(&right.1)
                        .then_with(|| left.2.cmp(&right.2))
                })
                .map(|(position, _, _)| position)
        };
        let selected = if requires_distinct_output {
            choose(true)
        } else {
            choose(true).or_else(|| choose(false))
        };
        if let Some(position) = selected {
            return Ok(position);
        }
        let distinct_capacity_fallback = remotes
            .paths
            .iter()
            .enumerate()
            .filter(|(_, path)| ordinary_path_allowed(path))
            .filter(|(_, path)| can_enqueue(path))
            .map(|(position, _)| position)
            .find(|position| !avoid_keys.contains(&remotes.paths[*position].key()));
        let capacity_fallback = if requires_distinct_output {
            distinct_capacity_fallback
        } else {
            distinct_capacity_fallback.or_else(|| {
                remotes
                    .paths
                    .iter()
                    .enumerate()
                    .filter(|(_, path)| ordinary_path_allowed(path))
                    .filter(|(_, path)| can_enqueue(path))
                    .map(|(position, _)| position)
                    .next()
            })
        };
        if let Some(position) = capacity_fallback {
            return Ok(position);
        }
        let has_eligible_path = remotes.paths.iter().any(ordinary_path_allowed);
        let has_distinct_eligible_path = remotes
            .paths
            .iter()
            .any(|path| ordinary_path_allowed(path) && !avoid_keys.contains(&path.key()));
        if (requires_distinct_output && has_distinct_eligible_path)
            || (!requires_distinct_output && has_eligible_path)
        {
            Err(RuntimeError::SenderServiceBlocked)
        } else {
            Err(RuntimeError::ReliablePathSessionClosed)
        }
    }

    #[cfg(test)]
    pub(super) fn release_normalized_acked_ranges(
        &mut self,
        context: &ClientPathContext,
        ranges: &[OffsetRange],
    ) {
        let _ = self.release_normalized_acked_ranges_with_owner_progress(context, ranges);
    }

    pub(super) fn release_normalized_acked_ranges_with_owner_progress(
        &mut self,
        context: &ClientPathContext,
        ranges: &[OffsetRange],
    ) -> smallvec::SmallVec<[RequestOwnerAckProgress; 4]> {
        let startup_owner = self
            .request_subflow_set
            .as_ref()
            .and_then(FlowSubflowSet::startup_owner_key);
        let startup_required_bytes = self
            .request_subflow_set
            .as_ref()
            .and_then(|epoch| {
                startup_owner.and_then(|owner| epoch.startup_owner_sealed_sample_bytes(owner))
            })
            .unwrap_or(u64::MAX);
        let acked_at = Instant::now();
        let mut ordinary_owner_samples =
            HashMap::<RelayPathInstance, (u64, Instant, Instant)>::new();
        let mut owner_progress = smallvec::SmallVec::<[RequestOwnerAckProgress; 4]>::new();
        for release in self.flights.release_normalized_acked_ranges(ranges) {
            self.missing_owner_repair_attempts.remove(&release.instance);
            context.release_relay_path_inflight(
                release.key.underlay,
                release.key.index,
                release.bytes,
            );
            if release.path_proving && release.key.underlay == UnderlayProtocol::Tcp {
                if let Some(progress) = owner_progress
                    .iter_mut()
                    .find(|progress| progress.instance == release.instance)
                {
                    progress.bytes = progress.bytes.saturating_add(release.bytes);
                } else {
                    owner_progress.push(RequestOwnerAckProgress {
                        instance: release.instance,
                        bytes: release.bytes,
                    });
                }
            }
            if release.path_proving
                && release.key.underlay == UnderlayProtocol::Tcp
                && startup_owner == Some(release.instance)
            {
                let first_sent_at = self
                    .request_startup_first_sent_at
                    .entry(release.instance)
                    .or_insert(release.sent_at);
                *first_sent_at = (*first_sent_at).min(release.sent_at);
                let acked_bytes = self
                    .request_startup_acked_bytes
                    .entry(release.instance)
                    .or_default();
                *acked_bytes = acked_bytes.saturating_add(release.bytes as u64);
            }
            if release.path_proving && startup_owner != Some(release.instance) {
                let sample = ordinary_owner_samples.entry(release.instance).or_insert((
                    0,
                    release.sent_at,
                    release.sent_at,
                ));
                sample.0 = sample.0.saturating_add(release.bytes as u64);
                sample.1 = sample.1.min(release.sent_at);
                sample.2 = sample.2.max(release.sent_at);
            }
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "path_model",
                format_args!(
                    "stream_id={} path_underlay={:?} path_index={} path_instance={} released_bytes={} elapsed_ms={:.3} path_proving={} cause=stream_ack",
                    self.stream_id.0,
                    release.key.underlay,
                    release.key.index,
                    release.instance.id,
                    release.bytes,
                    release.elapsed.as_secs_f64() * 1000.0,
                    release.path_proving,
                ),
            );
        }
        if let Some(owner) = startup_owner
            && owner.key.underlay == UnderlayProtocol::Tcp
            && let (Some(acked_bytes), Some(first_sent_at)) = (
                self.request_startup_acked_bytes.get(&owner).copied(),
                self.request_startup_first_sent_at.get(&owner).copied(),
            )
            && acked_bytes >= startup_required_bytes
            && let Some(sample) = PathRateSample::new(
                acked_bytes,
                acked_at.saturating_duration_since(first_sent_at),
            )
            && self.request_startup_rate_evidence.insert(owner)
        {
            self.request_rate_proven_subflows.insert(owner);
            context.mark_relay_path_rate_sample(owner.key.underlay, owner.key.index, sample);
        }
        for (instance, (bytes, first_sent_at, latest_sent_at)) in ordinary_owner_samples {
            let update = self
                .request_rate_evidence
                .entry(instance)
                .or_insert_with(|| RequestPathRateEvidence::new(first_sent_at))
                .observe(bytes, first_sent_at, latest_sent_at, acked_at);
            if let RequestPathRateEvidenceUpdate::Proven {
                sample,
                first_window,
            } = update
            {
                let already_proven = self.request_rate_proven_subflows.contains(&instance);
                self.request_rate_proven_subflows.insert(instance);
                if instance.key.underlay == UnderlayProtocol::Tcp
                    && let Some(sample) = sample
                    && (!first_window || !already_proven)
                {
                    let replace_startup_rate =
                        !first_window && self.request_ack_clock_proven_subflows.insert(instance);
                    context.mark_relay_path_ack_clock_rate_sample(
                        instance.key.underlay,
                        instance.key.index,
                        sample,
                        replace_startup_rate,
                    );
                    #[cfg(feature = "lab-diagnostics")]
                    if !first_window && replace_startup_rate {
                        lab_diagnostic(
                            "ack_clock_calibration",
                            format_args!(
                                "phase=ack_clock_sample stream_id={} underlay={:?} path_index={} instance_id={} replace_startup_rate={} rate_bps={}",
                                self.stream_id.0,
                                instance.key.underlay,
                                instance.key.index,
                                instance.id,
                                replace_startup_rate,
                                sample.rate_bps(),
                            ),
                        );
                    }
                }
            }
        }
        owner_progress
    }

    pub(super) fn discard_unusable_live_owner_tail_repairs(
        &self,
        sender_queue: &mut ReliableRelaySenderQueue,
        remotes: &ReliableRelayRemoteSet,
    ) -> usize {
        let live_instances = self.request_owner_capable_instances(remotes);
        let live_keys = live_instances
            .iter()
            .map(|instance| instance.key)
            .collect::<Vec<_>>();
        sender_queue.discard_unusable_live_owner_tail_repairs(|frame| {
            let owner_keys = self
                .flights
                .ordering_owner_keys_for_frame(frame, &live_instances);
            !owner_keys.is_empty() && live_keys.iter().any(|key| !owner_keys.contains(key))
        })
    }

    pub(super) fn discard_stale_persistent_ack_gap_repairs(
        &self,
        sender_queue: &mut ReliableRelaySenderQueue,
        remotes: &ReliableRelayRemoteSet,
    ) -> usize {
        let live_instances = remotes.path_instances();
        sender_queue.discard_stale_persistent_ack_gap_repairs(|cause| {
            cause
                .persistent_client_target()
                .is_none_or(|target| live_instances.contains(&target))
                && cause.persistent_server_target().is_none()
        })
    }

    fn request_owner_capable_instances(
        &self,
        remotes: &ReliableRelayRemoteSet,
    ) -> Vec<RelayPathInstance> {
        let startup_owner = self
            .request_subflow_set
            .as_ref()
            .and_then(FlowSubflowSet::startup_owner_key);
        remotes
            .paths
            .iter()
            .filter(|path| {
                path.placement != RelayPathPlacement::Validation
                    || startup_owner == Some(path.instance())
                    || self.request_graduated_subflows.contains(&path.instance())
                    || self
                        .flights
                        .has_ordering_owner_flights_for_instance(path.instance())
            })
            .map(ReliableRelayRemotePath::instance)
            .collect()
    }

    pub(super) fn request_owner_ack_can_grow_window(
        &self,
        remotes: &ReliableRelayRemoteSet,
        instance: RelayPathInstance,
    ) -> bool {
        instance.key.underlay == UnderlayProtocol::Tcp
            && remotes.paths.iter().any(|path| {
                path.instance() == instance
                    && (path.placement == RelayPathPlacement::Active
                        || (self.request_graduated_subflows.contains(&instance)
                            && self.request_ack_clock_proven_subflows.contains(&instance)))
            })
    }

    pub(super) fn unreported_missing_owner_instances(
        &mut self,
        remotes: &ReliableRelayRemoteSet,
        retry_after: Duration,
    ) -> Vec<RelayPathInstance> {
        let owner_instances = self.flights.ordering_owner_instances();
        self.missing_owner_repair_attempts.retain(|instance, _| {
            owner_instances.contains(instance) && !remotes.contains_path_instance(*instance)
        });
        let now = Instant::now();
        owner_instances
            .into_iter()
            .filter(|instance| {
                !remotes.contains_path_instance(*instance)
                    && self
                        .missing_owner_repair_attempts
                        .get(instance)
                        .is_none_or(|attempt| {
                            now.saturating_duration_since(*attempt) >= retry_after
                        })
            })
            .collect()
    }

    #[cfg(test)]
    pub(super) fn unreported_missing_owner_keys(
        &mut self,
        remotes: &ReliableRelayRemoteSet,
        retry_after: Duration,
    ) -> Vec<RelayPathKey> {
        let mut keys = Vec::new();
        for instance in self.unreported_missing_owner_instances(remotes, retry_after) {
            if !keys.contains(&instance.key) {
                keys.push(instance.key);
            }
        }
        keys
    }

    pub(super) fn release_all(&mut self, context: &ClientPathContext) {
        for release in self.flights.drain_all() {
            context.release_relay_path_inflight(
                release.key.underlay,
                release.key.index,
                release.bytes,
            );
        }
    }

    #[cfg(test)]
    pub(super) fn age_product_flights_for_test(&mut self, age: Duration) {
        self.flights.age_product_flights_for_test(age);
    }

    #[cfg(test)]
    pub(super) fn record_owner_frame_for_test(
        &mut self,
        instance: RelayPathInstance,
        frame: &Frame,
    ) {
        self.flights.record_owner_frame_instance(instance, frame);
        self.ordered_data_owner = Some(instance.key);
        self.ordered_data_owner_instance = Some(instance);
    }

    #[cfg(test)]
    pub(super) fn ordered_data_owner_for_test(&self) -> Option<RelayPathKey> {
        self.ordered_data_owner
    }

    pub(super) async fn reannounce_active_path(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        spec: &ReliableRelayOpenSpec,
        lane: FlowLane,
    ) -> Result<(), RuntimeError> {
        let Some(position) = remotes
            .paths
            .iter()
            .rposition(|path| path.placement == RelayPathPlacement::Active)
        else {
            return Err(RuntimeError::ReliablePathSessionClosed);
        };
        let instance = remotes.paths[position].instance();
        remotes.paths[position].stream.lane = lane;
        let frame = Frame::OpenStream {
            stream_id: remotes.stream_id(),
            target: spec.target.clone(),
            ingress: spec.ingress,
            outbound: OutboundPolicy::Direct,
            demand: stream_demand_hint_for_lane(lane),
            role: StreamOpenRole::Active,
        };
        match emit_relay_path_frame(&remotes.paths[position].stream, frame, FlowLane::Control).await
        {
            Ok(()) => Ok(()),
            Err(err) => {
                remotes.fail_path_instance(context, instance).await;
                Err(err)
            }
        }
    }

    pub(super) async fn reannounce_path_instance_as_active(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        instance: RelayPathInstance,
        spec: &ReliableRelayOpenSpec,
        lane: FlowLane,
    ) -> Result<bool, RuntimeError> {
        let Some(position) = remotes
            .paths
            .iter()
            .position(|path| path.instance() == instance)
        else {
            return Ok(false);
        };
        if remotes.paths[position].placement == RelayPathPlacement::Active
            && position + 1 == remotes.paths.len()
        {
            return Ok(false);
        }
        let frame = Frame::OpenStream {
            stream_id: remotes.stream_id(),
            target: spec.target.clone(),
            ingress: spec.ingress,
            outbound: OutboundPolicy::Direct,
            demand: stream_demand_hint_for_lane(lane),
            role: StreamOpenRole::Active,
        };
        let emit_result = {
            let path = &mut remotes.paths[position];
            path.stream.lane = lane;
            emit_relay_path_frame(&path.stream, frame, FlowLane::Control).await
        };
        match emit_result {
            Ok(()) => {
                let activated = remotes.activate_path_instance_after_service_open(instance);
                if activated {
                    remotes.reserve_path_instance_load_if_needed(context, instance, lane);
                }
                Ok(activated)
            }
            Err(err) => {
                remotes.fail_path_instance(context, instance).await;
                Err(err)
            }
        }
    }

    pub(super) async fn send_attach_control_to_instance(
        &mut self,
        remotes: &mut ReliableRelayRemoteSet,
        instance: RelayPathInstance,
        send_stream: &ReliableSendStream,
        resend_fin: bool,
    ) -> Result<bool, RuntimeError> {
        let Some(position) = remotes
            .paths
            .iter()
            .position(|path| path.instance() == instance)
        else {
            return Ok(false);
        };
        if !resend_fin {
            return Ok(false);
        }
        emit_relay_path_frame_with_mode(
            &remotes.paths[position].stream,
            Frame::StreamFin {
                stream_id: remotes.stream_id(),
                final_offset: send_stream.next_offset(),
            },
            remotes.paths[position].stream.lane,
            ResponseCarrierEmitMode::StreamOrdered,
        )
        .await?;
        Ok(true)
    }

    pub(super) async fn send_recv_progress(
        &mut self,
        remotes: &mut ReliableRelayRemoteSet,
        context: &ClientPathContext,
        recv_stream: &ReliableRecvStream,
        progress: &mut ReliableRecvProgress,
        request: RelayRecvProgressSend,
    ) -> Result<bool, RuntimeError> {
        let mut sent_any = false;
        let cause = if request.recover_stalled_service {
            RelaySendCause::RecvProgressRecovery
        } else {
            RelaySendCause::RecvProgress
        };
        let ack_progress_before = progress.clone();
        if progress.should_send_ack(
            recv_stream,
            request.path,
            request.lane,
            context.mux_limits,
            request.force_max_data,
        ) {
            #[cfg(feature = "lab-diagnostics")]
            let ack_started = Instant::now();
            let ack_frames = recv_stream.ack_frames();
            #[cfg(feature = "lab-diagnostics")]
            lab_perf_record("mux.ack_frames", ack_started.elapsed(), ack_frames.len());
            for ack_frame in ack_frames {
                match self
                    .send_control_frame(context, remotes, ack_frame, cause)
                    .await
                {
                    Ok(_) => {
                        sent_any = true;
                    }
                    Err(RuntimeError::SenderServiceBlocked) => {
                        // Partial incomplete ACK chunks are safe to repeat. Put
                        // the ACK progress cursor back so the omitted chunks are
                        // sent on the next capacity notification instead of
                        // being inferred as sender-side loss.
                        *progress = ack_progress_before;
                        return Ok(sent_any);
                    }
                    Err(err) => return Err(err),
                }
            }
        }
        let max_data_progress_before = progress.clone();
        if progress.should_send_max_data(
            recv_stream,
            request.path,
            request.lane,
            context.mux_limits,
            request.force_max_data,
        ) {
            let advertised_window = reliable_stream_advertised_window_bytes(
                request.path,
                request.lane,
                context.mux_limits,
            );
            match self
                .send_control_frame(
                    context,
                    remotes,
                    recv_stream.max_data_frame_with_window(advertised_window),
                    cause,
                )
                .await
            {
                Ok(_) => {
                    sent_any = true;
                }
                Err(RuntimeError::SenderServiceBlocked) => {
                    *progress = max_data_progress_before;
                    return Ok(sent_any);
                }
                Err(err) => return Err(err),
            }
        }
        Ok(sent_any)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn enqueue_live_owner_tail_repair(
        &mut self,
        sender_queue: &mut ReliableRelaySenderQueue,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        send_stream: &ReliableSendStream,
        last_send_ack_ranges: &[OffsetRange],
        last_send_ack_complete: bool,
        last_send_ack_frontier: u64,
        lane: FlowLane,
    ) -> bool {
        if !last_send_ack_complete
            || last_send_ack_frontier == 0
            || last_send_ack_frontier >= send_stream.next_offset()
            || send_stream.repair_bytes() == 0
            || !matches!(
                last_send_ack_ranges,
                [range] if range.start == 0 && range.end == last_send_ack_frontier
            )
        {
            return false;
        }
        let live_instances = self.request_owner_capable_instances(remotes);
        let live_keys = live_instances
            .iter()
            .map(|instance| instance.key)
            .collect::<Vec<_>>();
        if live_keys.len() <= 1 {
            return false;
        }
        let repair_limit = reliable_critical_tail_repair_limit_bytes(
            live_keys
                .iter()
                .map(|key| {
                    adaptive_reliable_relay_repair_bytes(
                        context.reliable_path_snapshot(*key),
                        lane,
                        context.mux_limits,
                    )
                })
                .max()
                .unwrap_or(0),
            send_stream.repair_bytes(),
            context.mux_limits,
        );
        if repair_limit == 0 {
            return false;
        }
        let repair_frames = send_stream.retransmission_frames_for_ranges(
            &[OffsetRange {
                start: last_send_ack_frontier,
                end: send_stream.next_offset(),
            }],
            repair_limit,
        );
        let mut queued = false;
        for frame in repair_frames {
            let expected_owner_keys = self
                .flights
                .ordering_owner_keys_for_frame(&frame, &live_instances);
            if expected_owner_keys.is_empty()
                || !live_keys
                    .iter()
                    .any(|key| !expected_owner_keys.contains(key))
            {
                break;
            }
            let first_repair_after = expected_owner_keys
                .iter()
                .map(|key| {
                    reliable_relay_tail_repair_delay(context.reliable_path_snapshot(*key), lane)
                })
                .max()
                .unwrap_or_default();
            let repeat_repair_after =
                first_repair_after.saturating_mul(QUIC_PERSISTENT_CONGESTION_THRESHOLD);
            let owner_keys = self.flights.live_owner_tail_repair_owner_keys(
                &frame,
                &live_instances,
                first_repair_after,
                repeat_repair_after,
            );
            if owner_keys.len() != expected_owner_keys.len() {
                break;
            }
            if sender_queue.has_queued_repair_overlap(&frame) {
                continue;
            }
            let payload_bytes = reliable_stream_frame_payload_bytes(&frame);
            self.enqueue_critical_repair_frame(
                sender_queue,
                frame,
                RelaySendCause::LiveOwnerTailRepair,
            );
            queued = true;
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "repair",
                format_args!(
                    "stream_id={} owner_underlay={:?} owner_index={} cause=live_owner_tail queued=true payload_bytes={}",
                    self.stream_id.0, owner_keys[0].underlay, owner_keys[0].index, payload_bytes,
                ),
            );
            #[cfg(not(feature = "lab-diagnostics"))]
            let _ = payload_bytes;
        }
        queued
    }

    pub(super) fn enqueue_failed_path_instance_gap_repairs(
        &mut self,
        sender_queue: &mut ReliableRelaySenderQueue,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        send_stream: &ReliableSendStream,
        failed_instance: RelayPathInstance,
        lane: FlowLane,
    ) -> bool {
        let ranges = self
            .flights
            .latest_unacked_ranges_for_path_instance(failed_instance);
        self.enqueue_failed_path_gap_repairs_for_ranges(
            sender_queue,
            context,
            remotes,
            send_stream,
            failed_instance.key,
            &[failed_instance],
            ranges,
            lane,
        )
    }

    #[cfg(test)]
    pub(super) fn enqueue_failed_path_gap_repairs(
        &mut self,
        sender_queue: &mut ReliableRelaySenderQueue,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        send_stream: &ReliableSendStream,
        failed_key: RelayPathKey,
        lane: FlowLane,
    ) -> bool {
        let failed_instances = self
            .flights
            .ordering_owner_instances()
            .into_iter()
            .filter(|instance| instance.key == failed_key)
            .collect::<Vec<_>>();
        let ranges = self.flights.latest_unacked_ranges_for_path(failed_key);
        self.enqueue_failed_path_gap_repairs_for_ranges(
            sender_queue,
            context,
            remotes,
            send_stream,
            failed_key,
            &failed_instances,
            ranges,
            lane,
        )
    }

    #[allow(clippy::too_many_arguments)]
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(unused_variables))]
    fn enqueue_failed_path_gap_repairs_for_ranges(
        &mut self,
        sender_queue: &mut ReliableRelaySenderQueue,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        send_stream: &ReliableSendStream,
        failed_key: RelayPathKey,
        failed_instances: &[RelayPathInstance],
        ranges: Vec<OffsetRange>,
        lane: FlowLane,
    ) -> bool {
        if ranges.is_empty() {
            return false;
        }
        let repair_path = remotes
            .primary_path_key()
            .and_then(|key| context.reliable_path_snapshot(key));
        let repair_limit = reliable_critical_tail_repair_limit_bytes(
            adaptive_reliable_relay_repair_bytes(repair_path, lane, context.mux_limits),
            send_stream.repair_bytes(),
            context.mux_limits,
        );
        let repair_frames = send_stream.retransmission_frames_for_ranges(&ranges, repair_limit);
        if repair_frames.is_empty() {
            return false;
        }
        let mut queued = false;
        for frame in repair_frames {
            let queued_frame = if sender_queue.has_queued_repair_overlap(&frame) {
                false
            } else {
                self.enqueue_critical_repair_frame(
                    sender_queue,
                    frame,
                    RelaySendCause::PathFailureRepair,
                );
                true
            };
            queued |= queued_frame;
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "repair",
                format_args!(
                    "stream_id={} failed_underlay={:?} failed_index={} cause=path_failure queued={}",
                    self.stream_id.0, failed_key.underlay, failed_key.index, queued_frame,
                ),
            );
        }
        if queued {
            let now = Instant::now();
            for instance in failed_instances {
                self.missing_owner_repair_attempts.insert(*instance, now);
            }
        }
        queued
    }

    fn record_decision(
        &self,
        path_key: RelayPathKey,
        payload_bytes: usize,
        frame: &Frame,
        cause: RelaySendCause,
    ) {
        #[cfg(feature = "lab-diagnostics")]
        lab_sender_service_decision(
            "client",
            None,
            self.stream_id.0,
            "primary",
            sender_service_frame_kind(frame),
            payload_bytes,
            None,
            format_args!(
                "cause={} path_underlay={:?} path_index={} pacing_bytes={} repair={}",
                cause.as_str(),
                path_key.underlay,
                path_key.index,
                frame_pacing_bytes(frame),
                cause.is_repair(),
            ),
        );
        #[cfg(not(feature = "lab-diagnostics"))]
        {
            let _ = (path_key, payload_bytes, frame, cause);
        }
    }
}

fn choose_active_recv_progress_path_position(
    remotes: &ReliableRelayRemoteSet,
    frame: &Frame,
    cause: RelaySendCause,
) -> Option<usize> {
    remotes
        .paths
        .iter()
        .enumerate()
        .rev()
        .find(|(_, path)| {
            path.placement == RelayPathPlacement::Active
                && relay_path_can_enqueue_frame_for_cause_now(path, frame, cause)
        })
        .map(|(position, _)| position)
}

fn choose_repair_recv_progress_path_position(
    remotes: &ReliableRelayRemoteSet,
    frame: &Frame,
    cause: RelaySendCause,
) -> Option<usize> {
    remotes
        .paths
        .iter()
        .enumerate()
        .rev()
        .find(|(_, path)| {
            path.placement == RelayPathPlacement::Repair
                && relay_path_can_enqueue_frame_for_cause_now(path, frame, cause)
        })
        .map(|(position, _)| position)
}

fn relay_path_can_enqueue_frame_for_cause_now(
    path: &ReliableRelayRemotePath,
    frame: &Frame,
    cause: RelaySendCause,
) -> bool {
    if matches!(cause, RelaySendCause::StreamFin) {
        path.stream.output.can_enqueue_lane_now(path.stream.lane)
    } else {
        path.stream.can_enqueue_frame_now(frame, path.stream.lane)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SharedSecret;
    #[cfg(feature = "lab-diagnostics")]
    use crate::lab_diagnostics::{
        lab_assert_server_sender_service_balanced, lab_diag_test_guard,
        lab_sender_service_counts_for_test,
    };

    #[test]
    fn request_rate_evidence_uses_ack_clock_after_initial_provenance() {
        let started = Instant::now();
        let bytes = PATH_OPEN_SCORE_BYTES as u64;
        let mut evidence = RequestPathRateEvidence::new(started);

        let initial = match evidence.observe(
            bytes,
            started,
            started,
            started + Duration::from_millis(100),
        ) {
            RequestPathRateEvidenceUpdate::Proven {
                sample: Some(sample),
                ..
            } => sample.rate_bps(),
            _ => panic!("first complete window must publish conservative provenance"),
        };
        let ack_clocked = match evidence.observe(
            bytes,
            started + Duration::from_millis(90),
            started + Duration::from_millis(90),
            started + Duration::from_millis(101),
        ) {
            RequestPathRateEvidenceUpdate::Proven {
                sample: Some(sample),
                ..
            } => sample.rate_bps(),
            _ => panic!("pipelined bytes must use ACK-to-ACK delivery time"),
        };

        assert!(
            ack_clocked > initial * 50.0,
            "RTT must not be charged again once exact bytes were already in flight at the previous ACK"
        );
    }

    #[test]
    fn request_rate_evidence_ignores_app_limited_ack_window() {
        let started = Instant::now();
        let bytes = PATH_OPEN_SCORE_BYTES as u64;
        let mut evidence = RequestPathRateEvidence::new(started);
        assert!(matches!(
            evidence.observe(
                bytes,
                started,
                started,
                started + Duration::from_millis(100)
            ),
            RequestPathRateEvidenceUpdate::Proven {
                sample: Some(_),
                ..
            }
        ));

        assert!(matches!(
            evidence.observe(
                bytes,
                started + Duration::from_millis(200),
                started + Duration::from_millis(200),
                started + Duration::from_millis(300)
            ),
            RequestPathRateEvidenceUpdate::Proven { sample: None, .. }
        ));
        assert!(matches!(
            evidence.observe(
                bytes,
                started + Duration::from_millis(290),
                started + Duration::from_millis(290),
                started + Duration::from_millis(301)
            ),
            RequestPathRateEvidenceUpdate::Proven {
                sample: Some(_),
                ..
            }
        ));
    }

    #[test]
    fn request_rate_evidence_rejects_window_with_mostly_post_ack_bytes() {
        let started = Instant::now();
        let bytes = PATH_OPEN_SCORE_BYTES as u64;
        let previous_ack = started + Duration::from_millis(100);
        let mut evidence = RequestPathRateEvidence::new(started);
        assert!(matches!(
            evidence.observe(bytes, started, started, previous_ack),
            RequestPathRateEvidenceUpdate::Proven {
                sample: Some(_),
                ..
            }
        ));

        let old_byte_sent_at = started + Duration::from_millis(90);
        let new_bytes_sent_at = started + Duration::from_millis(101);
        assert!(matches!(
            evidence.observe(
                1,
                old_byte_sent_at,
                old_byte_sent_at,
                started + Duration::from_millis(110),
            ),
            RequestPathRateEvidenceUpdate::Pending
        ));
        assert!(matches!(
            evidence.observe(
                bytes - 1,
                new_bytes_sent_at,
                new_bytes_sent_at,
                started + Duration::from_millis(200),
            ),
            RequestPathRateEvidenceUpdate::Proven { sample: None, .. }
        ));
    }

    #[test]
    fn sender_queue_dispatches_owner_data_before_ordinary_repair() {
        let stream_id = StreamId(77);
        let mut queue = ReliableRelaySenderQueue::default();

        queue.push_data(Bytes::from_static(b"owner"));
        queue.push_repair(Frame::StreamData {
            stream_id,
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(b"repair"),
        });

        let (lane, work) = queue
            .pop_front()
            .expect("ordinary owner data should be queued");
        assert_eq!(
            lane,
            ReliableRelayQueuedWorkLane::Data,
            "ordinary RepairData must not preempt OwnerData; repair only preempts when explicitly critical"
        );
        assert_eq!(work.payload_bytes, 5);
    }

    #[test]
    fn sender_queue_dispatches_critical_repair_before_owner_data() {
        let stream_id = StreamId(78);
        let mut queue = ReliableRelaySenderQueue::default();

        queue.push_data(Bytes::from_static(b"owner"));
        queue.push_critical_repair_with_cause(
            Frame::StreamData {
                stream_id,
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"repair"),
            },
            RelaySendCause::AckGapRepair,
        );

        let (lane, work) = queue.pop_front().expect("critical repair should be queued");
        assert_eq!(
            lane,
            ReliableRelayQueuedWorkLane::Repair,
            "critical RepairData closes an active product hole and must preempt later OwnerData"
        );
        assert_eq!(work.payload_bytes, 6);
    }

    #[test]
    fn sender_queue_trims_and_releases_acked_live_tail_repair() {
        let stream_id = StreamId(80);
        let mut queue = ReliableRelaySenderQueue::default();
        queue.push_critical_repair_with_cause(
            Frame::StreamData {
                stream_id,
                offset: 128,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(&[0x5a; 64]),
            },
            RelaySendCause::LiveOwnerTailRepair,
        );

        assert_eq!(
            queue.release_normalized_acked_repairs(&[OffsetRange { start: 0, end: 160 }]),
            32,
        );
        assert_eq!(queue.bytes(), 32);
        assert!(matches!(
            queue.front().map(|(_, work)| &work.kind),
            Some(ReliableRelayQueuedWorkKind::Repair {
                frame: Frame::StreamData { offset: 160, payload, .. },
                cause: RelaySendCause::LiveOwnerTailRepair,
            }) if payload.len() == 32
        ));

        assert_eq!(
            queue.release_normalized_acked_repairs(&[OffsetRange { start: 0, end: 192 }]),
            32,
        );
        assert!(queue.is_empty());
        assert_eq!(queue.bytes(), 0);
    }

    #[test]
    fn sender_queue_discards_only_unusable_live_owner_tail_repair() {
        let stream_id = StreamId(81);
        let mut queue = ReliableRelaySenderQueue::default();
        for cause in [
            RelaySendCause::LiveOwnerTailRepair,
            RelaySendCause::PathFailureRepair,
        ] {
            queue.push_critical_repair_with_cause(
                Frame::StreamData {
                    stream_id,
                    offset: if cause == RelaySendCause::LiveOwnerTailRepair {
                        0
                    } else {
                        64
                    },
                    flags: StreamFlags::NONE,
                    payload: Bytes::from_static(&[0x5b; 64]),
                },
                cause,
            );
        }

        assert_eq!(
            queue.discard_unusable_live_owner_tail_repairs(|_| false),
            64,
        );
        assert_eq!(queue.bytes(), 64);
        assert!(matches!(
            queue.front().map(|(_, work)| &work.kind),
            Some(ReliableRelayQueuedWorkKind::Repair {
                cause: RelaySendCause::PathFailureRepair,
                ..
            })
        ));
    }

    #[test]
    fn sender_queue_discards_stale_bound_repair_without_touching_ordinary_repair() {
        let stream_id = StreamId(82);
        let mut queue = ReliableRelaySenderQueue::default();
        let cause = RelaySendCause::PersistentClientAckGapRepair(PersistentClientAckGapBatch {
            target: ClientRepairOutputIdentity {
                instance: RelayPathInstance {
                    key: RelayPathKey {
                        underlay: UnderlayProtocol::Tcp,
                        index: 2,
                    },
                    id: 7,
                },
            },
            expires_at: Instant::now() + Duration::from_secs(1),
        });
        for (offset, cause) in [(0, cause), (64, RelaySendCause::AckGapRepair)] {
            queue.push_critical_repair_with_cause(
                Frame::StreamData {
                    stream_id,
                    offset,
                    flags: StreamFlags::NONE,
                    payload: Bytes::from_static(&[0x5c; 64]),
                },
                cause,
            );
        }

        assert_eq!(
            queue.discard_stale_persistent_ack_gap_repairs(|_| false),
            64
        );
        assert_eq!(queue.bytes(), 64);
        assert!(matches!(
            queue.front().map(|(_, work)| &work.kind),
            Some(ReliableRelayQueuedWorkKind::Repair {
                cause: RelaySendCause::AckGapRepair,
                ..
            })
        ));
    }

    #[test]
    fn sender_queue_discards_expired_bound_repair_on_live_output() {
        let mut queue = ReliableRelaySenderQueue::default();
        queue.push_critical_repair_with_cause(
            Frame::StreamData {
                stream_id: StreamId(83),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(&[0x5d; 64]),
            },
            RelaySendCause::PersistentServerAckGapRepair(PersistentServerAckGapBatch {
                target: ServerRepairOutputIdentity {
                    key: CarrierPathKey {
                        underlay: UnderlayProtocol::Udp,
                        path_id: PathId(3),
                    },
                    incarnation: 9,
                },
                expires_at: Instant::now() - Duration::from_millis(1),
            }),
        );

        assert_eq!(queue.discard_stale_persistent_ack_gap_repairs(|_| true), 64);
        assert!(queue.is_empty());
        assert_eq!(queue.bytes(), 0);
    }

    #[test]
    fn budgeted_critical_repair_preempts_owner_data_and_debits_budget() {
        let mux_limits = MuxLimits::default();
        let stream_id = StreamId(79);
        let mut sender = ServerResponseSenderService::new_with_performance(
            SessionId(79),
            stream_id,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 1,
            },
        );
        let startup_floor = response_extra_traffic_startup_floor_bytes(mux_limits);

        sender.enqueue_data_for_lane(Bytes::from_static(b"owner"), FlowLane::Throughput);
        assert!(
            sender
                .enqueue_repair_frame_with_priority(
                    Frame::StreamData {
                        stream_id,
                        offset: 0,
                        flags: StreamFlags::NONE,
                        payload: Bytes::from(vec![0x7a; startup_floor]),
                    },
                    mux_limits,
                    true,
                )
                .is_some(),
            "startup repair floor should be spendable"
        );

        assert_eq!(
            sender.queue.front_lane(),
            Some(ReliableRelayQueuedWorkLane::Repair)
        );
        assert_eq!(
            sender.repair_extra_budget_remaining(mux_limits),
            0,
            "critical priority is not budget bypass"
        );
    }

    fn security() -> SecurityConfig {
        SecurityConfig::encrypted(
            SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
        )
    }

    #[test]
    fn stream_ack_releases_sender_service_flights_with_path_scoped_rate_sample() {
        let path = "tcp://127.0.0.1:10251".parse::<PathSpec>().expect("path");
        let context = ClientPathContext::new(vec![path], security(), ResourceLimits::default())
            .expect("context");
        let key = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        };
        let seeded = PathRateSample::new(4 * 1024 * 1024, Duration::from_millis(20))
            .expect("seed rate sample");
        context.mark_relay_path_rate_sample(key.underlay, key.index, seeded);

        let frame = Frame::StreamData {
            stream_id: StreamId(7),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![0u8; PATH_OPEN_SCORE_BYTES]),
        };
        context.record_relay_path_send(key.underlay, key.index, PATH_OPEN_SCORE_BYTES);
        let mut sender = RelaySenderService::new(StreamId(7));
        sender.flights.record_owner_frame(key, &frame);

        let before = context.tcp_path_snapshot(0).expect("before snapshot");
        assert_eq!(before.bytes_in_flight, PATH_OPEN_SCORE_BYTES as u64);
        let owner_progress = sender.release_normalized_acked_ranges_with_owner_progress(
            &context,
            &[OffsetRange::new(0, PATH_OPEN_SCORE_BYTES as u64).expect("ack range")],
        );
        let after = context.tcp_path_snapshot(0).expect("after snapshot");

        assert_eq!(after.bytes_in_flight, 0);
        assert_ne!(
            after.delivery_rate_bps, before.delivery_rate_bps,
            "an unambiguous owner STREAM_ACK is path-scoped delivery evidence"
        );
        assert_eq!(
            owner_progress.as_slice(),
            &[RequestOwnerAckProgress {
                instance: RelayPathInstance { key, id: 0 },
                bytes: PATH_OPEN_SCORE_BYTES,
            }],
            "request-window growth must use exact flight ownership, not the ACK carrier"
        );
    }

    #[test]
    fn cumulative_stream_ack_emits_one_aggregated_path_rate_sample() {
        let path = "tcp://127.0.0.1:10253".parse::<PathSpec>().expect("path");
        let context = ClientPathContext::new(vec![path], security(), ResourceLimits::default())
            .expect("context");
        let instance = RelayPathInstance {
            key: RelayPathKey {
                underlay: UnderlayProtocol::Tcp,
                index: 0,
            },
            id: 11,
        };
        let mut sender = RelaySenderService::new(StreamId(9));
        let frames = (0..4)
            .map(|index| {
                client_data_frame_for_test(
                    StreamId(9),
                    index * BBR_MAX_SEND_QUANTUM_BYTES as u64,
                    BBR_MAX_SEND_QUANTUM_BYTES,
                )
            })
            .collect::<Vec<_>>();
        for frame in &frames {
            context.record_relay_path_send(
                instance.key.underlay,
                instance.key.index,
                BBR_MAX_SEND_QUANTUM_BYTES,
            );
            sender.flights.record_owner_frame_instance(instance, frame);
        }

        sender.release_normalized_acked_ranges(
            &context,
            &[OffsetRange::new(0, (4 * BBR_MAX_SEND_QUANTUM_BYTES) as u64)
                .expect("cumulative ACK range")],
        );
        let delivery_samples =
            context.health.lock().expect("path health lock").tcp[0].delivery_samples;

        assert_eq!(
            context
                .tcp_path_snapshot(0)
                .expect("path snapshot")
                .bytes_in_flight,
            0
        );
        assert_eq!(
            delivery_samples, 1,
            "one cumulative STREAM_ACK must contribute one byte-aggregated path sample, not one tiny sample per frame"
        );
        assert!(
            context.relay_path_has_bulk_model_evidence(instance.key.underlay, instance.key.index,)
        );
    }

    #[test]
    fn fragmented_service_acks_accumulate_before_publishing_exact_evidence() {
        let path = "tcp://127.0.0.1:10254".parse::<PathSpec>().expect("path");
        let context = ClientPathContext::new(vec![path], security(), ResourceLimits::default())
            .expect("context");
        let instance = RelayPathInstance {
            key: RelayPathKey {
                underlay: UnderlayProtocol::Tcp,
                index: 0,
            },
            id: 12,
        };
        let mut sender = RelaySenderService::new(StreamId(10));
        let chunk = 8 * 1024;
        let first = client_data_frame_for_test(StreamId(10), 0, chunk);
        let second = client_data_frame_for_test(StreamId(10), chunk as u64, chunk);
        for frame in [&first, &second] {
            context.record_relay_path_send(instance.key.underlay, instance.key.index, chunk);
            sender.flights.record_owner_frame_instance(instance, frame);
        }

        sender.release_normalized_acked_ranges(
            &context,
            &[OffsetRange::new(0, chunk as u64).expect("first ACK range")],
        );
        assert!(!sender.request_rate_proven_subflows.contains(&instance));
        assert_eq!(
            context.health.lock().expect("path health lock").tcp[0].delivery_samples,
            0
        );

        sender.release_normalized_acked_ranges(
            &context,
            &[OffsetRange::new(chunk as u64, (2 * chunk) as u64).expect("second ACK range")],
        );
        let health = context.health.lock().expect("path health lock");
        assert!(sender.request_rate_proven_subflows.contains(&instance));
        assert_eq!(health.tcp[0].delivery_samples, 1);
        assert_eq!(
            health.tcp[0].product_delivery_sample_bytes,
            (2 * chunk) as u64
        );
    }

    #[test]
    fn duplicate_stream_ack_release_does_not_seed_sender_service_path_rate() {
        let path = "tcp://127.0.0.1:10252".parse::<PathSpec>().expect("path");
        let context = ClientPathContext::new(vec![path], security(), ResourceLimits::default())
            .expect("context");
        let owner = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        };
        let repair = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        };
        let frame = Frame::StreamData {
            stream_id: StreamId(8),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![0u8; PATH_OPEN_SCORE_BYTES]),
        };
        context.record_relay_path_send(owner.underlay, owner.index, PATH_OPEN_SCORE_BYTES);
        context.record_relay_path_send(repair.underlay, repair.index, PATH_OPEN_SCORE_BYTES);
        let mut sender = RelaySenderService::new(StreamId(8));
        sender.flights.record_owner_frame(owner, &frame);
        sender.flights.record_repair_frame(repair, &frame);

        let owner_progress = sender.release_normalized_acked_ranges_with_owner_progress(
            &context,
            &[OffsetRange::new(0, PATH_OPEN_SCORE_BYTES as u64).expect("ack range")],
        );
        let after = context.tcp_path_snapshot(0).expect("after snapshot");

        assert_eq!(after.bytes_in_flight, 0);
        assert!(
            !context.relay_path_has_bulk_model_evidence(owner.underlay, owner.index),
            "ACK of a duplicated request byte range releases inflight state but must not seed path evidence"
        );
        assert!(
            owner_progress.is_empty(),
            "ambiguous OwnerData/RepairData release must not grow request read-ahead"
        );
    }

    fn response_target(
        path_id: u16,
        underlay: UnderlayProtocol,
        eta_ms: f64,
        bytes_in_flight: u64,
        inflight_limit_bytes: u64,
        is_active: bool,
    ) -> ResponseSenderPathTarget {
        let (commands, _receivers) = reliable_path_command_channels(8);
        let mut snapshot =
            PathSnapshot::new(PathId(path_id), underlay, eta_ms.max(1.0), 500_000_000.0);
        snapshot.bytes_in_flight = bytes_in_flight;
        snapshot.product_bytes_in_flight = bytes_in_flight;
        snapshot.inflight_limit_bytes = inflight_limit_bytes;
        snapshot.confidence = 1.0;
        ResponseSenderPathTarget {
            #[cfg(feature = "lab-diagnostics")]
            session_id: SessionId(0),
            #[cfg(feature = "lab-diagnostics")]
            binding_instance_id: 0,
            key: CarrierPathKey {
                underlay,
                path_id: PathId(path_id),
            },
            incarnation: u64::from(path_id) + 1,
            commands,
            attachment_role: if is_active {
                StreamOpenRole::Active
            } else {
                StreamOpenRole::Validation
            },
            snapshot,
            owner_data_in_flight_bytes: bytes_in_flight,
            command_pending_bytes: 0,
            eta_ms,
            is_active,
            is_request_active: is_active,
            has_sender_evidence: true,
            has_bulk_rate_evidence: true,
            ack_clock_calibration_eligible: false,
            ack_clock_calibration_proven: false,
            ack_clock_calibration_spent_bytes: 0,
            ack_clock_calibration_credit_limit_bytes: 0,
            ack_clock_calibration_max_limit_bytes: 0,
            ack_clock_calibration_active: false,
        }
    }

    struct ResponseCalibrationDispatchFixture {
        binding: Arc<ResponseStreamBinding>,
        stream: ReliablePathStream,
        service: CarrierPathKey,
        candidate: CarrierPathKey,
        candidate_commands: ReliablePathCommandSender,
        service_receivers: ReliablePathCommandReceivers,
        candidate_receivers: ReliablePathCommandReceivers,
        second_binding: Option<Arc<ResponseStreamBinding>>,
    }

    fn response_calibration_dispatch_fixture(
        candidate_capacity: usize,
    ) -> ResponseCalibrationDispatchFixture {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let session_id = SessionId(191);
        let tracker = Arc::new(ServerPathLaneTracker::default());
        let service = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let candidate = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let (service_commands, service_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new_with_limits_and_tracker(
            session_id,
            service.underlay,
            service.path_id,
            service_commands,
            FlowLane::Throughput,
            mux_limits,
            tracker.clone(),
        );
        let (second_commands, _second_receivers) = reliable_path_command_channels(8);
        let second_binding = ResponseStreamBinding::new_with_limits_and_tracker(
            session_id,
            UnderlayProtocol::Tcp,
            PathId(9),
            second_commands,
            FlowLane::Throughput,
            mux_limits,
            tracker,
        );
        let (candidate_commands, mut candidate_receivers) =
            reliable_path_command_channels(candidate_capacity);
        assert_eq!(
            binding.attach(
                candidate.underlay,
                candidate.path_id,
                candidate_commands.clone(),
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                payload_bytes,
            ),
            ResponseStreamAttachOutcome::Attached
        );
        assert!(matches!(
            try_recv_reliable_path_command(&mut candidate_receivers),
            Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
        ));
        binding.mark_output_bulk_proven_for_test(service);
        binding.mark_output_bulk_proven_for_test(candidate);
        assert_eq!(
            binding
                .commit_subflow_owner_admission(
                    service,
                    payload_bytes,
                    0,
                    Duration::ZERO,
                    SubflowAdmissionInput {
                        key: candidate,
                        bulk_rate_proven: true,
                        startup_owner_allowed: false,
                        frontier_clear: true,
                        completion_improves: true,
                        observed_goodput_non_degrading: true,
                        read_gap: Duration::ZERO,
                        owner_bytes: payload_bytes,
                        optional_overhead_bytes: 0,
                    },
                )
                .decision,
            PathAdmissionDecision::AdmitSubflow
        );
        let stage_limit = reliable_ack_clock_calibration_limit_bytes(mux_limits);
        binding.install_tcp_ack_clock_calibration_for_test(
            candidate,
            stage_limit - 4032,
            stage_limit,
            reliable_ack_clock_calibration_ceiling_bytes(mux_limits),
            true,
        );
        let (_frames_tx, frames_rx) = mpsc::channel(1);
        let stream = ReliablePathStream {
            stream_id: StreamId(191),
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: UnderlayProtocol::Tcp,
            max_frame_payload_bytes: payload_bytes,
            output: ReliablePathStreamOutput::Switchable(binding.clone()),
            frames: frames_rx,
        };
        ResponseCalibrationDispatchFixture {
            binding,
            stream,
            service,
            candidate,
            candidate_commands,
            service_receivers,
            candidate_receivers,
            second_binding: Some(second_binding),
        }
    }

    fn install_slow_fresh_response_calibration(fixture: &ResponseCalibrationDispatchFixture) {
        fixture
            .binding
            .set_output_product_model_for_test(fixture.service, 47_429_000.0, 333.0);
        fixture
            .binding
            .set_output_product_model_for_test(fixture.candidate, 1_342_000.0, 891.787);
        fixture.binding.install_tcp_ack_clock_calibration_for_test(
            fixture.candidate,
            0,
            299_176,
            reliable_ack_clock_calibration_ceiling_bytes(MuxLimits::default()),
            false,
        );
    }

    fn response_calibration_retirement_request(
        fixture: &ResponseCalibrationDispatchFixture,
    ) -> ResponseAckClockCalibrationRetirementRequest {
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
        let targets = fixture
            .binding
            .sender_path_targets(FlowLane::Throughput, payload_bytes);
        let service = targets
            .iter()
            .find(|target| target.key == fixture.service)
            .expect("Service target");
        let candidate = targets
            .iter()
            .find(|target| target.key == fixture.candidate)
            .expect("calibration target");
        let (expected_planner_generation, _) = fixture.binding.subflow_state_snapshot();
        let expected_lane_generation = fixture
            .binding
            .lane_generation_and_active_response_flows()
            .0;
        ResponseAckClockCalibrationRetirementRequest {
            expected_planner_generation,
            expected_lane_generation,
            expected_model_generation: fixture.binding.response_model_generation(),
            service: service.key,
            service_incarnation: service.incarnation,
            service_pending_bytes: service.command_pending_bytes,
            target: candidate.key,
            target_incarnation: candidate.incarnation,
            target_pending_bytes: candidate.command_pending_bytes,
            limit_bytes: candidate.ack_clock_calibration_credit_limit_bytes,
        }
    }

    #[test]
    fn repair_target_requires_active_or_bulk_rate_evidence() {
        let mut proof_only = response_target(1, UnderlayProtocol::Udp, 25.0, 0, 1_000_000, false);
        proof_only.has_sender_evidence = true;
        proof_only.has_bulk_rate_evidence = false;
        let mut unevidenced = response_target(2, UnderlayProtocol::Tcp, 5.0, 0, 1_000_000, false);
        unevidenced.has_sender_evidence = false;
        unevidenced.has_bulk_rate_evidence = false;

        assert!(
            choose_response_repair_target(
                &[proof_only, unevidenced],
                &[],
                RelaySendCause::AckGapRepair,
            )
            .is_none(),
            "RepairData is correctness traffic, not path discovery; unproven outputs must not receive repair merely because no proven target is available"
        );
    }

    #[test]
    fn persistent_response_repair_stays_bound_to_modeled_output() {
        let modeled = response_target(1, UnderlayProtocol::Udp, 25.0, 0, 1_000_000, false);
        let alternate = response_target(2, UnderlayProtocol::Tcp, 5.0, 0, 1_000_000, false);
        let cause = RelaySendCause::persistent_server_ack_gap_repair(
            ServerRepairOutputIdentity {
                key: modeled.key,
                incarnation: modeled.incarnation,
            },
            modeled.snapshot,
            FlowLane::Throughput,
        );

        let selected =
            choose_response_repair_target(&[modeled.clone(), alternate.clone()], &[], cause)
                .expect("modeled output remains eligible");
        assert_eq!(selected.key, modeled.key);
        assert!(
            choose_response_repair_target(&[alternate], &[], cause).is_none(),
            "a queued BDP repair must pause instead of switching to a differently modeled output"
        );
        let mut replacement = modeled;
        replacement.incarnation = replacement.incarnation.saturating_add(1);
        assert!(
            choose_response_repair_target(&[replacement], &[], cause).is_none(),
            "a same-key replacement must not inherit a batch sized from the old output incarnation"
        );
    }

    #[test]
    fn persistent_response_repair_is_cancelled_when_output_incarnation_detaches() {
        let key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(7),
        };
        let (commands, _receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new(
            SessionId(84),
            key.underlay,
            key.path_id,
            commands.clone(),
            FlowLane::Throughput,
        );
        let target = binding
            .sender_path_targets(FlowLane::Throughput, 64)
            .into_iter()
            .next()
            .expect("initial response output");
        let (_frames_tx, frames) = mpsc::channel(1);
        let path_stream = ReliablePathStream {
            stream_id: StreamId(84),
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: key.underlay,
            max_frame_payload_bytes: MuxLimits::default().max_payload_bytes,
            output: ReliablePathStreamOutput::Switchable(binding.clone()),
            frames,
        };
        let mut sender = ServerResponseSenderService::new(SessionId(84), StreamId(84));
        sender.enqueue_critical_repair_frame_with_cause(
            Frame::StreamData {
                stream_id: StreamId(84),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(&[0x5e; 64]),
            },
            RelaySendCause::persistent_server_ack_gap_repair(
                ServerRepairOutputIdentity {
                    key,
                    incarnation: target.incarnation,
                },
                target.snapshot,
                FlowLane::Throughput,
            ),
        );

        binding.detach(key, &commands);
        assert_eq!(
            sender.discard_stale_persistent_ack_gap_repairs(&path_stream),
            64
        );
        assert!(sender.is_empty());
    }

    #[test]
    fn response_owner_data_waits_for_missing_lower_owner_debt() {
        let frame = Frame::StreamData {
            stream_id: StreamId(82),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(b"owner"),
        };
        let survivor = response_target(1, UnderlayProtocol::Udp, 10.0, 0, 1_000_000, false);
        let lower_flights = [
            CarrierPathFlightDebt {
                key: survivor.key,
                bytes: 64,
            },
            CarrierPathFlightDebt {
                key: CarrierPathKey {
                    underlay: UnderlayProtocol::Tcp,
                    path_id: PathId(9),
                },
                bytes: 64,
            },
        ];
        assert!(
            select_response_sender_data_target_with_ordered_debt_and_epoch(
                std::slice::from_ref(&survivor),
                FlowLane::Latency,
                reliable_stream_frame_payload_bytes(&frame),
                MuxLimits::default(),
                &lower_flights,
                None,
                128,
                None,
            )
            .is_none(),
            "a sole survivor must not receive later OwnerData while a missing lower owner still has debt"
        );
        assert!(
            select_response_sender_data_target_with_ordered_debt_and_epoch(
                &[survivor],
                FlowLane::Latency,
                reliable_stream_frame_payload_bytes(&frame),
                MuxLimits::default(),
                &[],
                None,
                0,
                None,
            )
            .is_some()
        );
    }

    #[test]
    fn repair_target_does_not_fallback_to_avoided_owner_path() {
        let owner = response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 1_000_000, true);
        let mut proof_only = response_target(2, UnderlayProtocol::Udp, 25.0, 0, 1_000_000, false);
        proof_only.has_sender_evidence = true;
        proof_only.has_bulk_rate_evidence = false;

        assert!(
            choose_response_repair_target(
                &[owner.clone(), proof_only],
                &[owner.key],
                RelaySendCause::AckGapRepair,
            )
            .is_none(),
            "RepairData must not retransmit an already-owned range on the same Service path when no distinct proven repair output exists"
        );
    }

    #[test]
    fn path_failure_repair_may_retry_stale_copy_when_all_outputs_are_avoided() {
        let owner = response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 1_000_000, true);
        let backup = response_target(2, UnderlayProtocol::Udp, 25.0, 0, 1_000_000, false);

        let selected = choose_response_repair_target(
            &[owner.clone(), backup.clone()],
            &[owner.key, backup.key],
            RelaySendCause::PathFailureRepair,
        )
        .expect("path-failure recovery may retry on a stale live output");

        assert_eq!(
            selected.key, owner.key,
            "PathFailureRepair should fall back by metrics when every live output already has a stale copy; this must not be available to ordinary AckGapRepair"
        );
        assert!(
            choose_response_repair_target(
                &[owner.clone(), backup.clone()],
                &[selected.key],
                RelaySendCause::AckGapRepair,
            )
            .is_some(),
            "ordinary ACK-gap repair still uses a distinct available output when one exists"
        );
        assert!(
            choose_response_repair_target(
                &[owner.clone(), backup.clone()],
                &[owner.key, backup.key],
                RelaySendCause::AckGapRepair,
            )
            .is_none(),
            "ordinary ACK-gap repair must not retry an already-owned or already-repaired range when every output is avoided"
        );
    }

    fn client_test_context() -> ClientPathContext {
        let path = "tcp://127.0.0.1:10251".parse::<PathSpec>().expect("path");
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("context")
    }

    fn client_test_context_with_paths(paths: &[&str]) -> ClientPathContext {
        ClientPathContext::new(
            paths
                .iter()
                .map(|path| path.parse::<PathSpec>().expect("path"))
                .collect(),
            security(),
            ResourceLimits::default(),
        )
        .expect("context")
    }

    fn opened_test_relay_stream(
        stream_id: StreamId,
        path_index: usize,
        commands: ReliablePathCommandSender,
    ) -> OpenedRemoteStream {
        opened_test_relay_stream_with_underlay(
            stream_id,
            UnderlayProtocol::Tcp,
            path_index,
            commands,
        )
    }

    fn opened_test_relay_stream_with_underlay(
        stream_id: StreamId,
        underlay: UnderlayProtocol,
        path_index: usize,
        commands: ReliablePathCommandSender,
    ) -> OpenedRemoteStream {
        let (_frame_tx, frame_rx) = mpsc::channel(1);
        OpenedRemoteStream {
            path_index,
            stream: ReliablePathStream {
                stream_id,
                max_offset: MuxLimits::default().max_stream_window_bytes,
                lane: FlowLane::Throughput,
                underlay,
                max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
                output: ReliablePathStreamOutput::fixed(
                    underlay,
                    PathId(path_index as u16),
                    commands,
                    MuxLimits::default(),
                ),
                frames: frame_rx,
            },
        }
    }

    fn client_data_frame_for_test(stream_id: StreamId, offset: u64, payload_bytes: usize) -> Frame {
        Frame::StreamData {
            stream_id,
            offset,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![0x5a; payload_bytes]),
        }
    }

    fn ack_client_frame_for_test(
        sender: &mut RelaySenderService,
        context: &ClientPathContext,
        frame: &Frame,
    ) {
        let (start, end, _) = reliable_stream_frame_extent(frame).expect("request data extent");
        sender.release_normalized_acked_ranges(
            context,
            &[OffsetRange::new(start, end).expect("request ACK range")],
        );
    }

    fn seed_client_bulk_evidence_for_test(context: &ClientPathContext, key: RelayPathKey) {
        context.mark_relay_path_rate_sample(
            key.underlay,
            key.index,
            PathRateSample::new(4 * 1024 * 1024, Duration::from_millis(20))
                .expect("bulk rate sample"),
        );
    }

    fn consume_client_validation_proof_for_test(receivers: &mut ReliablePathCommandReceivers) {
        assert!(matches!(
            try_recv_reliable_path_priority_command(receivers),
            Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
        ));
    }

    #[tokio::test]
    async fn client_ack_gap_model_separates_owner_transport_from_repair_output() {
        let stream_id = StreamId(90);
        let context = client_test_context_with_paths(&[
            "tcp://127.0.0.1:10260?srtt-ms=500&rate-mbps=400",
            "udp://127.0.0.1:10261?srtt-ms=40&rate-mbps=200",
            "udp://127.0.0.1:10262?srtt-ms=5&rate-mbps=500",
        ]);
        let (tcp_commands, _tcp_receivers) = reliable_path_command_channels(8);
        let (udp_commands, _udp_receivers) = reliable_path_command_channels(1);
        let (proof_only_commands, mut proof_only_receivers) = reliable_path_command_channels(8);
        let mut remotes = ReliableRelayRemoteSet::new(
            opened_test_relay_stream_with_underlay(
                stream_id,
                UnderlayProtocol::Udp,
                0,
                udp_commands.clone(),
            ),
            8,
        );
        remotes.attach_for_validation(opened_test_relay_stream_with_underlay(
            stream_id,
            UnderlayProtocol::Tcp,
            0,
            tcp_commands,
        ));
        remotes.attach_for_validation(opened_test_relay_stream_with_underlay(
            stream_id,
            UnderlayProtocol::Udp,
            1,
            proof_only_commands,
        ));
        consume_client_validation_proof_for_test(&mut proof_only_receivers);

        let limits = MuxLimits::default();
        let mut send_stream = ReliableSendStream::new(stream_id, limits);
        let blocked = send_stream
            .send_data(Bytes::from(vec![0x41; 4096]), StreamFlags::NONE)
            .expect("blocked owner data");
        send_stream
            .send_data(Bytes::from(vec![0x42; 4096]), StreamFlags::NONE)
            .expect("later delivered data");
        let mut sender = RelaySenderService::new(stream_id);
        sender.record_owner_frame_for_test(
            remotes
                .paths
                .iter()
                .find(|path| path.key().underlay == UnderlayProtocol::Tcp)
                .map(ReliableRelayRemotePath::instance)
                .expect("slow TCP validation owner"),
            &blocked,
        );
        let ranges = [OffsetRange {
            start: 4096,
            end: 8192,
        }];

        let (unproven_owner, owner_timing_path, unproven_repair_path) = sender
            .ack_gap_repair_path_model(
                &context,
                &remotes,
                &send_stream,
                &ranges,
                64 * 1024,
                FlowLane::Throughput,
            );
        assert_eq!(unproven_owner, Some(UnderlayProtocol::Tcp));
        assert_eq!(
            owner_timing_path.map(|snapshot| snapshot.srtt_ms),
            Some(500.0),
            "persistent-gap proof time follows the slow exact owner rather than the 40 ms Active repair output"
        );
        assert!(
            unproven_repair_path.is_none(),
            "a proof-only Validation output may carry a bounded repair quantum but must not authorize a BDP-sized burst from configured hints"
        );
        seed_client_bulk_evidence_for_test(
            &context,
            RelayPathKey {
                underlay: UnderlayProtocol::Udp,
                index: 0,
            },
        );
        let (owner_underlay, owner_timing_path, repair_path) = sender.ack_gap_repair_path_model(
            &context,
            &remotes,
            &send_stream,
            &ranges,
            64 * 1024,
            FlowLane::Throughput,
        );

        assert_eq!(owner_underlay, Some(UnderlayProtocol::Tcp));
        assert_eq!(
            owner_timing_path.map(|snapshot| snapshot.underlay),
            Some(UnderlayProtocol::Tcp)
        );
        assert_eq!(
            repair_path.map(|(_, snapshot)| snapshot.underlay),
            Some(UnderlayProtocol::Udp),
            "the exact ACK-gap selector must avoid the TCP owner and model the distinct QUIC repair output"
        );
        let (repair_target, repair_path) = repair_path.expect("distinct repair output");
        assert!(
            reliable_persistent_ack_gap_repair_limit_bytes(
                Some(repair_path),
                owner_underlay,
                FlowLane::Throughput,
                limits.max_repair_bytes,
                limits,
            ) > adaptive_reliable_relay_repair_bytes(
                Some(repair_path),
                FlowLane::Throughput,
                limits,
            ),
            "TCP owner persistence controls amplification even when QUIC carries the repair"
        );

        seed_client_bulk_evidence_for_test(
            &context,
            RelayPathKey {
                underlay: UnderlayProtocol::Udp,
                index: 1,
            },
        );

        udp_commands
            .try_enqueue_admitted_frame(
                Frame::StreamData {
                    stream_id: StreamId(91),
                    offset: 0,
                    flags: StreamFlags::NONE,
                    payload: Bytes::from_static(b"busy"),
                },
                FlowLane::Throughput,
            )
            .expect("fill the modeled repair output after sizing");
        let bound_cause = RelaySendCause::persistent_client_ack_gap_repair(
            repair_target,
            repair_path,
            FlowLane::Throughput,
        );
        assert!(matches!(
            sender
                .send_repair_frame(&context, &mut remotes, blocked.clone(), bound_cause,)
                .await,
            Err(RuntimeError::SenderServiceBlocked)
        ));
        assert!(
            try_recv_reliable_path_command(&mut proof_only_receivers).is_none(),
            "an amplified batch stays bound to the modeled output instead of switching to another proven output"
        );

        let replacement = remotes
            .paths
            .iter_mut()
            .find(|path| path.instance() == repair_target.instance)
            .expect("modeled repair attachment remains present");
        replacement.instance_id = replacement.instance_id.saturating_add(1);
        assert!(matches!(
            sender
                .send_repair_frame(&context, &mut remotes, blocked.clone(), bound_cause)
                .await,
            Err(RuntimeError::ReliablePathSessionClosed)
        ));
        let mut queue = ReliableRelaySenderQueue::default();
        queue.push_critical_repair_with_cause(blocked, bound_cause);
        let dispatch = sender
            .dispatch_client_queued_work(
                &context,
                &ReliableRelayOpenSpec {
                    target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80))),
                    ingress: IngressKind::Socks5,
                },
                FlowLane::Throughput,
                &mut remotes,
                &mut send_stream,
                &mut queue,
                true,
                4096,
            )
            .await
            .expect("stale bound repair is cancelled without aborting the stream");
        assert!(matches!(
            dispatch,
            ClientQueuedDispatch::PersistentRepairCancelled
        ));
        assert!(queue.is_empty());
    }

    fn mark_client_validation_proof_fresh_for_test(
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        instance: RelayPathInstance,
        elapsed: Duration,
    ) {
        let (attached_at, proof_id) = remotes
            .paths
            .iter()
            .find(|path| path.instance() == instance)
            .map(|path| {
                (
                    path.attached_at,
                    path.path_proof_id.expect("queued attachment proof"),
                )
            })
            .expect("attached validation instance");
        context.mark_relay_path_proof_observation(
            instance.key.underlay,
            instance.key.index,
            PathProofObservation {
                proof_id,
                bytes: PATH_OPEN_SCORE_BYTES as u64,
                elapsed,
                sent_at: Instant::now(),
            },
        );
        assert!(context.relay_path_has_fresh_proof(
            instance.key.underlay,
            instance.key.index,
            proof_id,
            attached_at,
        ));
    }

    #[tokio::test]
    async fn client_recv_progress_backpressure_is_retryable_not_stream_fatal() {
        let stream_id = StreamId(92);
        let context = client_test_context();
        let (commands, mut receivers) = reliable_path_command_channels(1);
        commands
            .try_enqueue_admitted_frame(
                Frame::StreamAck {
                    stream_id,
                    complete: false,
                    ranges: Vec::new(),
                },
                FlowLane::Control,
            )
            .expect("prefill priority queue");
        let mut remotes =
            ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, commands), 4);
        let mut recv_stream = ReliableRecvStream::new(stream_id, MuxLimits::default());
        recv_stream
            .receive_data(0, Bytes::from_static(b"reply"), StreamFlags::NONE)
            .expect("receive response bytes");
        let mut progress = ReliableRecvProgress::default();
        let mut sender = RelaySenderService::new(stream_id);

        let sent = sender
            .send_recv_progress(
                &mut remotes,
                &context,
                &recv_stream,
                &mut progress,
                RelayRecvProgressSend::new(None, FlowLane::Throughput, false),
            )
            .await
            .expect("recv progress backpressure should not close the product stream");

        assert!(!sent, "blocked advisory progress must report no frame sent");
        assert!(matches!(
            try_recv_reliable_path_priority_command(&mut receivers),
            Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
        ));

        let retried = sender
            .send_recv_progress(
                &mut remotes,
                &context,
                &recv_stream,
                &mut progress,
                RelayRecvProgressSend::new(None, FlowLane::Throughput, false),
            )
            .await
            .expect("recv progress should retry once queue capacity returns");

        assert!(
            retried,
            "progress watermark must roll back after a blocked enqueue"
        );
        assert!(matches!(
            try_recv_reliable_path_priority_command(&mut receivers),
            Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
        ));
    }

    #[tokio::test]
    async fn client_recv_progress_uses_available_control_queue_instead_of_full_low_eta_path() {
        let stream_id = StreamId(93);
        let first_path = "tcp://127.0.0.1:10251"
            .parse::<PathSpec>()
            .expect("first path");
        let second_path = "tcp://127.0.0.1:10252"
            .parse::<PathSpec>()
            .expect("second path");
        let context = ClientPathContext::new(
            vec![first_path, second_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("context");
        let (first_commands, mut first_rx) = reliable_path_command_channels(1);
        first_commands
            .try_enqueue_admitted_frame(
                Frame::StreamAck {
                    stream_id,
                    complete: false,
                    ranges: Vec::new(),
                },
                FlowLane::Control,
            )
            .expect("prefill first priority queue");
        let (second_commands, mut second_rx) = reliable_path_command_channels(1);
        let mut remotes =
            ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, first_commands), 4);
        remotes.attach(opened_test_relay_stream(stream_id, 1, second_commands));
        let mut recv_stream = ReliableRecvStream::new(stream_id, MuxLimits::default());
        recv_stream
            .receive_data(0, Bytes::from_static(b"reply"), StreamFlags::NONE)
            .expect("receive response bytes");
        let mut progress = ReliableRecvProgress::default();
        let mut sender = RelaySenderService::new(stream_id);

        let sent = sender
            .send_recv_progress(
                &mut remotes,
                &context,
                &recv_stream,
                &mut progress,
                RelayRecvProgressSend::new(None, FlowLane::Throughput, false),
            )
            .await
            .expect("available alternate control queue should accept recv progress");

        assert!(sent);
        assert!(matches!(
            try_recv_reliable_path_priority_command(&mut first_rx),
            Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
        ));
        assert!(matches!(
            try_recv_reliable_path_priority_command(&mut second_rx),
            Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
        ));
    }

    #[tokio::test]
    async fn client_recv_progress_prefers_active_service_path_over_validation_probe() {
        let stream_id = StreamId(96);
        let tcp_path = "tcp://127.0.0.1:10270?srtt-ms=500&rate-mbps=50"
            .parse::<PathSpec>()
            .expect("tcp path");
        let udp_path = "udp://127.0.0.1:10271?srtt-ms=5&rate-mbps=500"
            .parse::<PathSpec>()
            .expect("udp path");
        let context = ClientPathContext::new(
            vec![tcp_path, udp_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("context");
        let (tcp_commands, mut tcp_rx) = reliable_path_command_channels(8);
        let (udp_commands, _udp_rx) = reliable_path_command_channels(8);
        let mut remotes = ReliableRelayRemoteSet::new(
            opened_test_relay_stream_with_underlay(
                stream_id,
                UnderlayProtocol::Tcp,
                0,
                tcp_commands,
            ),
            8,
        );
        remotes.attach_for_validation(opened_test_relay_stream_with_underlay(
            stream_id,
            UnderlayProtocol::Udp,
            0,
            udp_commands,
        ));
        let mut recv_stream = ReliableRecvStream::new(stream_id, MuxLimits::default());
        recv_stream
            .receive_data(0, Bytes::from_static(b"reply"), StreamFlags::NONE)
            .expect("receive response bytes");
        let mut progress = ReliableRecvProgress::default();
        let mut sender = RelaySenderService::new(stream_id);

        let sent = sender
            .send_recv_progress(
                &mut remotes,
                &context,
                &recv_stream,
                &mut progress,
                RelayRecvProgressSend::new(None, FlowLane::Throughput, false),
            )
            .await
            .expect("recv progress should use the active service return path");

        assert!(sent);
        assert!(
            matches!(
                try_recv_reliable_path_priority_command(&mut tcp_rx),
                Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
            ),
            "STREAM_ACK for received OwnerData should prefer the Active Service path; a lower-ETA validation probe must not own the product ACK clock while the Service path is usable"
        );
    }

    #[tokio::test]
    async fn client_stall_recv_progress_prefers_accepted_repair_path() {
        let stream_id = StreamId(97);
        let tcp_path = "tcp://127.0.0.1:10272?srtt-ms=5&rate-mbps=500"
            .parse::<PathSpec>()
            .expect("tcp path");
        let udp_path = "udp://127.0.0.1:10273?srtt-ms=500&rate-mbps=50"
            .parse::<PathSpec>()
            .expect("udp path");
        let context = ClientPathContext::new(
            vec![tcp_path, udp_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("context");
        let (tcp_commands, mut tcp_rx) = reliable_path_command_channels(8);
        let (udp_commands, mut udp_rx) = reliable_path_command_channels(8);
        let mut remotes = ReliableRelayRemoteSet::new(
            opened_test_relay_stream_with_underlay(
                stream_id,
                UnderlayProtocol::Tcp,
                0,
                tcp_commands,
            ),
            8,
        );
        remotes.attach_for_repair(opened_test_relay_stream_with_underlay(
            stream_id,
            UnderlayProtocol::Udp,
            0,
            udp_commands,
        ));
        let mut recv_stream = ReliableRecvStream::new(stream_id, MuxLimits::default());
        recv_stream
            .receive_data(0, Bytes::from_static(b"reply"), StreamFlags::NONE)
            .expect("receive response bytes");
        let mut progress = ReliableRecvProgress::default();
        let mut sender = RelaySenderService::new(stream_id);

        let ordinary_sent = sender
            .send_recv_progress(
                &mut remotes,
                &context,
                &recv_stream,
                &mut progress,
                RelayRecvProgressSend::new(None, FlowLane::Latency, true),
            )
            .await
            .expect("ordinary receive progress should use Active");

        assert!(ordinary_sent);
        assert!(matches!(
            try_recv_reliable_path_priority_command(&mut tcp_rx),
            Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
        ));
        while try_recv_reliable_path_priority_command(&mut tcp_rx).is_some() {}
        assert!(try_recv_reliable_path_priority_command(&mut udp_rx).is_none());

        let mut progress = ReliableRecvProgress::default();
        let sent = sender
            .send_recv_progress(
                &mut remotes,
                &context,
                &recv_stream,
                &mut progress,
                RelayRecvProgressSend::new(None, FlowLane::Latency, true).recover_stalled_service(),
            )
            .await
            .expect("stall receive progress should use an accepted repair carrier");

        assert!(sent);
        assert!(
            try_recv_reliable_path_priority_command(&mut tcp_rx).is_none(),
            "the stalled Active path must not keep the recovery ACK when Repair is usable"
        );
        assert!(matches!(
            try_recv_reliable_path_priority_command(&mut udp_rx),
            Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
        ));
        assert_eq!(
            remotes.active_path_key(),
            Some(RelayPathKey {
                underlay: UnderlayProtocol::Tcp,
                index: 0,
            }),
            "routing recovery control over Repair must not promote it to Active"
        );
    }

    #[tokio::test]
    async fn client_stall_recv_progress_falls_back_to_active_when_repair_is_full() {
        let stream_id = StreamId(98);
        let context = client_test_context();
        let (active_commands, mut active_rx) = reliable_path_command_channels(1);
        let (repair_commands, mut repair_rx) = reliable_path_command_channels(1);
        repair_commands
            .try_enqueue_admitted_frame(
                Frame::StreamAck {
                    stream_id,
                    complete: false,
                    ranges: Vec::new(),
                },
                FlowLane::Control,
            )
            .expect("prefill repair control queue");
        let mut remotes = ReliableRelayRemoteSet::new(
            opened_test_relay_stream_with_underlay(
                stream_id,
                UnderlayProtocol::Tcp,
                0,
                active_commands,
            ),
            4,
        );
        remotes.attach_for_repair(opened_test_relay_stream_with_underlay(
            stream_id,
            UnderlayProtocol::Udp,
            0,
            repair_commands,
        ));
        let mut recv_stream = ReliableRecvStream::new(stream_id, MuxLimits::default());
        recv_stream
            .receive_data(0, Bytes::from_static(b"reply"), StreamFlags::NONE)
            .expect("receive response bytes");
        let mut progress = ReliableRecvProgress::default();
        let mut sender = RelaySenderService::new(stream_id);

        let sent = sender
            .send_recv_progress(
                &mut remotes,
                &context,
                &recv_stream,
                &mut progress,
                RelayRecvProgressSend::new(None, FlowLane::Latency, true).recover_stalled_service(),
            )
            .await
            .expect("a full repair queue should fall back to Active");

        assert!(sent);
        assert!(matches!(
            try_recv_reliable_path_priority_command(&mut active_rx),
            Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
        ));
        assert!(matches!(
            try_recv_reliable_path_priority_command(&mut repair_rx),
            Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
        ));
        assert!(try_recv_reliable_path_priority_command(&mut repair_rx).is_none());
    }

    #[tokio::test]
    async fn client_stall_recv_progress_never_uses_validation_path() {
        let stream_id = StreamId(99);
        let context = client_test_context();
        let (active_commands, mut active_rx) = reliable_path_command_channels(1);
        active_commands
            .try_enqueue_admitted_frame(
                Frame::StreamAck {
                    stream_id,
                    complete: false,
                    ranges: Vec::new(),
                },
                FlowLane::Control,
            )
            .expect("prefill active control queue");
        let (validation_commands, mut validation_rx) = reliable_path_command_channels(2);
        let mut remotes = ReliableRelayRemoteSet::new(
            opened_test_relay_stream_with_underlay(
                stream_id,
                UnderlayProtocol::Tcp,
                0,
                active_commands,
            ),
            4,
        );
        remotes.attach_for_validation(opened_test_relay_stream_with_underlay(
            stream_id,
            UnderlayProtocol::Udp,
            0,
            validation_commands,
        ));
        assert!(matches!(
            try_recv_reliable_path_priority_command(&mut validation_rx),
            Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
        ));
        let mut recv_stream = ReliableRecvStream::new(stream_id, MuxLimits::default());
        recv_stream
            .receive_data(0, Bytes::from_static(b"reply"), StreamFlags::NONE)
            .expect("receive response bytes");
        let mut progress = ReliableRecvProgress::default();
        let mut sender = RelaySenderService::new(stream_id);

        let sent = sender
            .send_recv_progress(
                &mut remotes,
                &context,
                &recv_stream,
                &mut progress,
                RelayRecvProgressSend::new(None, FlowLane::Latency, true).recover_stalled_service(),
            )
            .await
            .expect("blocked recovery feedback remains retryable");

        assert!(!sent);
        assert!(matches!(
            try_recv_reliable_path_priority_command(&mut active_rx),
            Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
        ));
        assert!(
            try_recv_reliable_path_priority_command(&mut validation_rx).is_none(),
            "Validation must remain product-ineligible during ACK recovery"
        );
    }

    #[tokio::test]
    async fn client_subflow_data_preserves_service_owner_after_frontier_clear_selection() {
        let stream_id = StreamId(94);
        let slow_path = "tcp://127.0.0.1:10261?srtt-ms=500&rate-mbps=50"
            .parse::<PathSpec>()
            .expect("slow path");
        let fast_path = "tcp://127.0.0.1:10262?srtt-ms=5&rate-mbps=500"
            .parse::<PathSpec>()
            .expect("fast path");
        let context = ClientPathContext::new(
            vec![slow_path, fast_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("context");
        let (slow_commands, _slow_rx) = reliable_path_command_channels(8);
        let (fast_commands, mut fast_rx) = reliable_path_command_channels(8);
        let mut remotes =
            ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, slow_commands), 8);
        remotes.attach(opened_test_relay_stream(stream_id, 1, fast_commands));
        let slow_key = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        };
        let fast_key = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 1,
        };
        let mut sender = RelaySenderService::new(stream_id);
        sender.ordered_data_owner = Some(slow_key);

        let frame = Frame::StreamData {
            stream_id,
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![0xab; 64 * 1024]),
        };
        let outcome = sender
            .send_stream_data(&context, &mut remotes, frame)
            .await
            .expect("frontier-clear owner data should migrate to the faster admitted active path");

        assert_eq!(outcome.path_key, fast_key);
        assert_eq!(
            sender.ordered_data_owner,
            Some(slow_key),
            "a selected Subflow owns its exact ranges without silently replacing the stable Service anchor"
        );
        assert!(matches!(
            try_recv_reliable_path_command(&mut fast_rx),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
        ));
    }

    #[tokio::test]
    async fn client_fresh_validation_proof_enables_startup_data_without_replacing_service() {
        let stream_id = StreamId(100);
        let context = client_test_context_with_paths(&[
            "tcp://127.0.0.1:10280?srtt-ms=20&rate-mbps=500",
            "tcp://127.0.0.1:10281?srtt-ms=10&rate-mbps=500",
        ]);
        let service_key = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        };
        let candidate_key = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 1,
        };
        seed_client_bulk_evidence_for_test(&context, service_key);

        let (service_commands, mut service_rx) = reliable_path_command_channels(8);
        let mut remotes = ReliableRelayRemoteSet::new(
            opened_test_relay_stream(stream_id, service_key.index, service_commands),
            8,
        );
        let mut sender = RelaySenderService::new(stream_id);
        let service_frame = client_data_frame_for_test(stream_id, 0, PATH_OPEN_SCORE_BYTES);
        let service_outcome = sender
            .send_stream_data(&context, &mut remotes, service_frame.clone())
            .await
            .expect("establish request Service owner");
        assert_eq!(service_outcome.path_key, service_key);
        assert!(matches!(
            try_recv_reliable_path_command(&mut service_rx),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
        ));
        ack_client_frame_for_test(&mut sender, &context, &service_frame);

        let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(8);
        remotes.attach_for_validation(opened_test_relay_stream(
            stream_id,
            candidate_key.index,
            candidate_commands,
        ));
        let candidate_instance = remotes
            .path_instance_for_key(candidate_key)
            .expect("validation instance");
        consume_client_validation_proof_for_test(&mut candidate_rx);
        mark_client_validation_proof_fresh_for_test(
            &context,
            &remotes,
            candidate_instance,
            Duration::from_millis(10),
        );

        let startup_frame =
            client_data_frame_for_test(stream_id, PATH_OPEN_SCORE_BYTES as u64, 8 * 1024);
        let startup_outcome = sender
            .send_stream_data(&context, &mut remotes, startup_frame)
            .await
            .expect("freshly proven Validation should receive bounded request data");

        assert_eq!(startup_outcome.path_key, candidate_key);
        assert!(matches!(
            try_recv_reliable_path_command(&mut candidate_rx),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
        ));
        assert!(try_recv_reliable_path_command(&mut service_rx).is_none());
        assert_eq!(sender.ordered_data_owner, Some(service_key));
        assert_eq!(remotes.active_path_key(), Some(service_key));
        assert_eq!(
            remotes
                .paths
                .iter()
                .find(|path| path.instance() == candidate_instance)
                .map(|path| path.placement),
            Some(RelayPathPlacement::Validation)
        );
        assert_eq!(
            sender
                .request_subflow_set
                .as_ref()
                .and_then(FlowSubflowSet::startup_owner_key),
            Some(candidate_instance)
        );
        assert!(
            !context
                .relay_path_has_bulk_model_evidence(candidate_key.underlay, candidate_key.index,),
            "PATH_PROOF enables only the bounded startup epoch"
        );
    }

    #[tokio::test]
    async fn client_startup_credit_is_cumulative_and_stream_acks_do_not_refill_it() {
        let stream_id = StreamId(101);
        let context = client_test_context_with_paths(&[
            "tcp://127.0.0.1:10282?srtt-ms=20&rate-mbps=500",
            "tcp://127.0.0.1:10283?srtt-ms=10&rate-mbps=500",
        ]);
        let service_key = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        };
        let candidate_key = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 1,
        };
        seed_client_bulk_evidence_for_test(&context, service_key);

        let (service_commands, mut service_rx) = reliable_path_command_channels(8);
        let mut remotes = ReliableRelayRemoteSet::new(
            opened_test_relay_stream(stream_id, service_key.index, service_commands),
            8,
        );
        let mut sender = RelaySenderService::new(stream_id);
        let mut offset = 0_u64;
        let service_frame = client_data_frame_for_test(stream_id, offset, PATH_OPEN_SCORE_BYTES);
        sender
            .send_stream_data(&context, &mut remotes, service_frame.clone())
            .await
            .expect("establish request Service owner");
        assert!(matches!(
            try_recv_reliable_path_command(&mut service_rx),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
        ));
        ack_client_frame_for_test(&mut sender, &context, &service_frame);
        offset = offset.saturating_add(PATH_OPEN_SCORE_BYTES as u64);

        let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(8);
        remotes.attach_for_validation(opened_test_relay_stream(
            stream_id,
            candidate_key.index,
            candidate_commands,
        ));
        let candidate_instance = remotes
            .path_instance_for_key(candidate_key)
            .expect("validation instance");
        consume_client_validation_proof_for_test(&mut candidate_rx);
        mark_client_validation_proof_fresh_for_test(
            &context,
            &remotes,
            candidate_instance,
            Duration::from_millis(10),
        );

        let startup_limit = usize::try_from(reliable_subflow_startup_sample_limit_bytes(
            context.mux_limits,
        ))
        .expect("startup limit");
        let ack_chunk = 8 * 1024;
        assert!(ack_chunk < PATH_OPEN_SCORE_BYTES);
        let mut startup_sent = 0_usize;
        while startup_sent < startup_limit {
            let payload_bytes = ack_chunk.min(startup_limit - startup_sent);
            let frame = client_data_frame_for_test(stream_id, offset, payload_bytes);
            let outcome = sender
                .send_stream_data(&context, &mut remotes, frame.clone())
                .await
                .expect("startup request sample within cumulative credit");
            assert_eq!(outcome.path_key, candidate_key);
            assert!(matches!(
                try_recv_reliable_path_command(&mut candidate_rx),
                Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
            ));
            ack_client_frame_for_test(&mut sender, &context, &frame);
            if startup_sent.saturating_add(payload_bytes) < startup_limit {
                assert!(
                    !context.relay_path_has_bulk_model_evidence(
                        candidate_key.underlay,
                        candidate_key.index,
                    ),
                    "fragmented ACKs must not create bulk evidence before cumulative startup evidence reaches its floor"
                );
            }
            startup_sent = startup_sent.saturating_add(payload_bytes);
            offset = offset.saturating_add(payload_bytes as u64);
        }

        let epoch = sender
            .request_subflow_set
            .as_ref()
            .expect("request startup epoch");
        let candidate_member = epoch
            .members()
            .iter()
            .find(|member| member.key == candidate_instance)
            .expect("startup candidate member");
        assert_eq!(candidate_member.owner_sent_bytes, startup_limit as u64);
        let (receipt_proof_id, _) = sender
            .request_startup_receipt_proofs
            .get(&candidate_instance)
            .copied()
            .expect("exhausted startup credit queues one ordered receipt proof");
        assert!(matches!(
            try_recv_reliable_path_command(&mut candidate_rx),
            Some(ReliablePathCommand::SendFrame(Frame::PathProofData {
                proof_id,
                ..
            })) if proof_id == receipt_proof_id
        ));

        let (delivery_samples, delivery_bytes) = {
            let health = context.health.lock().expect("path health lock");
            let candidate = &health.tcp[candidate_key.index];
            (
                candidate.delivery_samples,
                candidate.product_delivery_sample_bytes,
            )
        };
        sender.release_normalized_acked_ranges(&context, &[]);
        let health = context.health.lock().expect("path health lock");
        assert_eq!(
            health.tcp[candidate_key.index].delivery_samples,
            delivery_samples
        );
        assert_eq!(
            health.tcp[candidate_key.index].product_delivery_sample_bytes, delivery_bytes,
            "an unrelated ACK event must not republish a completed cumulative startup sample"
        );
        drop(health);

        let after_cap = client_data_frame_for_test(stream_id, offset, ack_chunk);
        let outcome = sender
            .send_stream_data(&context, &mut remotes, after_cap)
            .await
            .expect("graduated scheduling resumes after cumulative startup cap");
        match outcome.path_key {
            key if key == service_key => assert!(matches!(
                try_recv_reliable_path_command(&mut service_rx),
                Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
            )),
            key if key == candidate_key => assert!(matches!(
                try_recv_reliable_path_command(&mut candidate_rx),
                Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
            )),
            key => panic!("unexpected post-graduation path: {key:?}"),
        }
        let epoch = sender
            .request_subflow_set
            .as_ref()
            .expect("graduated request epoch");
        assert_eq!(epoch.startup_owner_key(), None);
        assert_eq!(
            epoch
                .members()
                .iter()
                .find(|member| member.key == candidate_instance)
                .expect("retained graduated member")
                .owner_sent_bytes,
            startup_limit as u64,
            "ACK release and ordinary measured sends must not refill or extend startup credit"
        );
        assert_eq!(sender.ordered_data_owner, Some(service_key));
    }

    #[tokio::test]
    async fn near_cap_startup_sample_seals_when_next_frame_cannot_fit() {
        let stream_id = StreamId(115);
        let context = client_test_context_with_paths(&[
            "tcp://127.0.0.1:10305?srtt-ms=20&rate-mbps=500",
            "tcp://127.0.0.1:10306?srtt-ms=10&rate-mbps=500",
        ]);
        let service_key = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        };
        let candidate_key = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 1,
        };
        seed_client_bulk_evidence_for_test(&context, service_key);

        let (service_commands, mut service_rx) = reliable_path_command_channels(16);
        let mut remotes = ReliableRelayRemoteSet::new(
            opened_test_relay_stream(stream_id, service_key.index, service_commands),
            16,
        );
        let mut sender = RelaySenderService::new(stream_id);
        let mut offset = 0_u64;
        let service_frame = client_data_frame_for_test(stream_id, offset, PATH_OPEN_SCORE_BYTES);
        sender
            .send_stream_data(&context, &mut remotes, service_frame.clone())
            .await
            .expect("establish request Service owner");
        assert!(matches!(
            try_recv_reliable_path_command(&mut service_rx),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
        ));
        ack_client_frame_for_test(&mut sender, &context, &service_frame);
        offset = offset.saturating_add(PATH_OPEN_SCORE_BYTES as u64);

        let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(16);
        remotes.attach_for_validation(opened_test_relay_stream(
            stream_id,
            candidate_key.index,
            candidate_commands,
        ));
        let candidate = remotes
            .path_instance_for_key(candidate_key)
            .expect("Validation candidate");
        consume_client_validation_proof_for_test(&mut candidate_rx);
        mark_client_validation_proof_fresh_for_test(
            &context,
            &remotes,
            candidate,
            Duration::from_millis(10),
        );

        let startup_limit = usize::try_from(reliable_subflow_startup_sample_limit_bytes(
            context.mux_limits,
        ))
        .expect("startup limit");
        let payload_bytes = 60 * 1024;
        let admitted_frames = startup_limit / payload_bytes;
        let admitted_bytes = admitted_frames * payload_bytes;
        assert!(admitted_frames > 0);
        assert!(admitted_bytes >= PATH_OPEN_SCORE_BYTES);
        assert!(startup_limit - admitted_bytes < payload_bytes);

        for _ in 0..admitted_frames {
            let frame = client_data_frame_for_test(stream_id, offset, payload_bytes);
            let outcome = sender
                .send_stream_data(&context, &mut remotes, frame.clone())
                .await
                .expect("near-cap startup sample frame");
            assert_eq!(outcome.path_key, candidate_key);
            assert!(matches!(
                try_recv_reliable_path_command(&mut candidate_rx),
                Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
            ));
            ack_client_frame_for_test(&mut sender, &context, &frame);
            offset = offset.saturating_add(payload_bytes as u64);
        }
        assert!(sender.request_startup_receipt_proofs.is_empty());
        assert_eq!(
            sender
                .request_subflow_set
                .as_ref()
                .and_then(|epoch| epoch.startup_owner_sealed_sample_bytes(candidate)),
            None
        );

        let next_frame = client_data_frame_for_test(stream_id, offset, payload_bytes);
        let outcome = sender
            .send_stream_data(&context, &mut remotes, next_frame)
            .await
            .expect("oversized remainder returns to Service after sealing the sample");
        assert_eq!(outcome.path_key, service_key);
        assert!(matches!(
            try_recv_reliable_path_command(&mut service_rx),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
        ));
        assert_eq!(
            sender
                .request_subflow_set
                .as_ref()
                .and_then(|epoch| epoch.startup_owner_sealed_sample_bytes(candidate)),
            Some(admitted_bytes as u64)
        );
        let (receipt_proof_id, _) = sender.request_startup_receipt_proofs[&candidate];
        assert!(matches!(
            try_recv_reliable_path_command(&mut candidate_rx),
            Some(ReliablePathCommand::SendFrame(Frame::PathProofData {
                proof_id,
                ..
            })) if proof_id == receipt_proof_id
        ));

        context.mark_relay_path_proof_observation(
            candidate_key.underlay,
            candidate_key.index,
            PathProofObservation {
                proof_id: receipt_proof_id,
                bytes: PATH_OPEN_SCORE_BYTES as u64,
                elapsed: Duration::from_millis(10),
                sent_at: Instant::now(),
            },
        );
        sender.reconcile_request_subflow_set(&context, &remotes);

        assert!(sender.request_graduated_subflows.contains(&candidate));
        let health = context.health.lock().expect("path health lock");
        assert_eq!(
            health.tcp[candidate_key.index].product_delivery_sample_bytes, admitted_bytes as u64,
            "receipt goodput must use only the bytes actually admitted before sealing"
        );
    }

    #[tokio::test]
    async fn graduated_candidate_calibration_produces_ack_clock_capacity_sample() {
        let stream_id = StreamId(116);
        let context = client_test_context_with_paths(&[
            "tcp://127.0.0.1:10307?srtt-ms=20&rate-mbps=500",
            "tcp://127.0.0.1:10308?srtt-ms=10&rate-mbps=500",
        ]);
        let service_key = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        };
        let candidate_key = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 1,
        };
        context.mark_relay_path_rate_sample(
            service_key.underlay,
            service_key.index,
            PathRateSample::new(4 * 1024 * 1024, Duration::from_millis(64)).expect("Service rate"),
        );
        context.mark_relay_path_rate_sample(
            candidate_key.underlay,
            candidate_key.index,
            PathRateSample::new(256 * 1024, Duration::from_secs(1)).expect("receipt rate"),
        );

        let (service_commands, mut service_rx) = reliable_path_command_channels(16);
        let mut remotes = ReliableRelayRemoteSet::new(
            opened_test_relay_stream(stream_id, service_key.index, service_commands),
            16,
        );
        let service = remotes
            .path_instance_for_key(service_key)
            .expect("Service instance");
        let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(16);
        remotes.attach_for_validation(opened_test_relay_stream(
            stream_id,
            candidate_key.index,
            candidate_commands,
        ));
        let candidate = remotes
            .path_instance_for_key(candidate_key)
            .expect("Validation candidate");
        consume_client_validation_proof_for_test(&mut candidate_rx);
        mark_client_validation_proof_fresh_for_test(
            &context,
            &remotes,
            candidate,
            Duration::from_millis(10),
        );

        let mut sender = RelaySenderService::new(stream_id);
        sender.ordered_data_owner = Some(service_key);
        sender.ordered_data_owner_instance = Some(service);
        sender.request_rate_proven_subflows.insert(service);
        sender.request_rate_proven_subflows.insert(candidate);
        sender.request_graduated_subflows.insert(candidate);
        assert!(!sender.request_owner_ack_can_grow_window(&remotes, candidate));

        let calibration_limit = usize::try_from(reliable_ack_clock_calibration_limit_bytes(
            context.mux_limits,
        ))
        .expect("calibration limit");
        assert_eq!(calibration_limit % BBR_MAX_SEND_QUANTUM_BYTES, 0);
        let calibration_frames = (0..(calibration_limit / BBR_MAX_SEND_QUANTUM_BYTES))
            .map(|index| {
                client_data_frame_for_test(
                    stream_id,
                    (index * BBR_MAX_SEND_QUANTUM_BYTES) as u64,
                    BBR_MAX_SEND_QUANTUM_BYTES,
                )
            })
            .collect::<Vec<_>>();
        for frame in &calibration_frames {
            let outcome = sender
                .send_stream_data(&context, &mut remotes, frame.clone())
                .await
                .expect("bounded ACK-clock calibration frame");
            assert_eq!(outcome.path_key, candidate_key);
            assert!(matches!(
                try_recv_reliable_path_command(&mut candidate_rx),
                Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
            ));
        }
        assert_eq!(
            sender.request_ack_clock_calibration_bytes[&candidate],
            calibration_limit as u64
        );

        ack_client_frame_for_test(&mut sender, &context, &calibration_frames[0]);
        assert!(
            !sender
                .request_ack_clock_proven_subflows
                .contains(&candidate)
        );
        ack_client_frame_for_test(&mut sender, &context, &calibration_frames[1]);
        assert!(
            sender
                .request_ack_clock_proven_subflows
                .contains(&candidate)
        );
        assert!(sender.request_owner_ack_can_grow_window(&remotes, service));
        assert!(
            sender.request_owner_ack_can_grow_window(&remotes, candidate),
            "a live graduated instance gains window-growth rights only after ACK-clock proof"
        );
        for frame in calibration_frames.iter().skip(2) {
            ack_client_frame_for_test(&mut sender, &context, frame);
        }
        let learned_rate = context
            .tcp_path_snapshot(candidate_key.index)
            .expect("candidate snapshot")
            .delivery_rate_bps;
        assert!(
            learned_rate > 100_000_000.0,
            "the first usable ACK-clock sample must replace the receipt-latency prior: {learned_rate}"
        );

        let third = client_data_frame_for_test(
            stream_id,
            calibration_limit as u64,
            BBR_MAX_SEND_QUANTUM_BYTES,
        );
        let outcome = sender
            .send_stream_data(&context, &mut remotes, third)
            .await
            .expect("ordinary scheduling after calibration");
        match outcome.path_key {
            key if key == candidate_key => assert!(matches!(
                try_recv_reliable_path_command(&mut candidate_rx),
                Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
            )),
            key if key == service_key => assert!(matches!(
                try_recv_reliable_path_command(&mut service_rx),
                Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
            )),
            key => panic!("unexpected post-calibration path: {key:?}"),
        }
        assert_eq!(
            sender.request_ack_clock_calibration_bytes[&candidate], calibration_limit as u64,
            "ACK release and ordinary scheduling must not refill calibration credit"
        );
    }

    #[tokio::test]
    async fn client_startup_graduation_advances_to_second_validation_instance() {
        let stream_id = StreamId(102);
        let context = client_test_context_with_paths(&[
            "tcp://127.0.0.1:10284?srtt-ms=20&rate-mbps=500",
            "tcp://127.0.0.1:10285?srtt-ms=5&rate-mbps=500",
            "tcp://127.0.0.1:10286?srtt-ms=40&rate-mbps=500",
        ]);
        let service_key = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        };
        let first_key = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 1,
        };
        let second_key = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 2,
        };
        seed_client_bulk_evidence_for_test(&context, service_key);

        let (service_commands, mut service_rx) = reliable_path_command_channels(8);
        let mut remotes = ReliableRelayRemoteSet::new(
            opened_test_relay_stream(stream_id, service_key.index, service_commands),
            8,
        );
        let mut sender = RelaySenderService::new(stream_id);
        let service_frame = client_data_frame_for_test(stream_id, 0, PATH_OPEN_SCORE_BYTES);
        sender
            .send_stream_data(&context, &mut remotes, service_frame.clone())
            .await
            .expect("establish request Service owner");
        assert!(matches!(
            try_recv_reliable_path_command(&mut service_rx),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
        ));
        ack_client_frame_for_test(&mut sender, &context, &service_frame);

        let (first_commands, mut first_rx) = reliable_path_command_channels(8);
        remotes.attach_for_validation(opened_test_relay_stream(
            stream_id,
            first_key.index,
            first_commands,
        ));
        let first_instance = remotes
            .path_instance_for_key(first_key)
            .expect("first validation instance");
        consume_client_validation_proof_for_test(&mut first_rx);

        let (second_commands, mut second_rx) = reliable_path_command_channels(8);
        remotes.attach_for_validation(opened_test_relay_stream(
            stream_id,
            second_key.index,
            second_commands,
        ));
        let second_instance = remotes
            .path_instance_for_key(second_key)
            .expect("second validation instance");
        consume_client_validation_proof_for_test(&mut second_rx);
        mark_client_validation_proof_fresh_for_test(
            &context,
            &remotes,
            first_instance,
            Duration::from_millis(5),
        );
        mark_client_validation_proof_fresh_for_test(
            &context,
            &remotes,
            second_instance,
            Duration::from_millis(40),
        );

        let startup_limit = usize::try_from(reliable_subflow_startup_sample_limit_bytes(
            context.mux_limits,
        ))
        .expect("startup limit");
        let mut first_sent = 0_usize;
        while first_sent < startup_limit {
            let payload_bytes = BBR_MAX_SEND_QUANTUM_BYTES.min(startup_limit - first_sent);
            let first_frame = client_data_frame_for_test(
                stream_id,
                PATH_OPEN_SCORE_BYTES as u64 + first_sent as u64,
                payload_bytes,
            );
            let first_outcome = sender
                .send_stream_data(&context, &mut remotes, first_frame.clone())
                .await
                .expect("first validation startup sample");
            assert_eq!(first_outcome.path_key, first_key);
            assert!(matches!(
                try_recv_reliable_path_command(&mut first_rx),
                Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
            ));
            ack_client_frame_for_test(&mut sender, &context, &first_frame);
            first_sent = first_sent.saturating_add(payload_bytes);
        }
        assert!(context.relay_path_has_bulk_model_evidence(first_key.underlay, first_key.index,));

        let second_offset = PATH_OPEN_SCORE_BYTES as u64 + startup_limit as u64;
        let second_frame = client_data_frame_for_test(stream_id, second_offset, 8 * 1024);
        let second_outcome = sender
            .send_stream_data(&context, &mut remotes, second_frame)
            .await
            .expect("second validation startup sample after first graduates");
        assert_eq!(second_outcome.path_key, second_key);
        assert!(matches!(
            try_recv_reliable_path_command(&mut second_rx),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
        ));

        let epoch = sender
            .request_subflow_set
            .as_ref()
            .expect("request startup epoch");
        assert_eq!(epoch.startup_owner_key(), Some(second_instance));
        assert!(
            epoch
                .members()
                .iter()
                .any(|member| member.key == first_instance)
        );
        assert!(
            epoch
                .members()
                .iter()
                .any(|member| member.key == second_instance)
        );
        assert_eq!(sender.ordered_data_owner, Some(service_key));
        assert_eq!(remotes.active_path_key(), Some(service_key));
    }

    #[tokio::test]
    async fn delayed_old_instance_ack_cannot_graduate_replacement_candidate() {
        let stream_id = StreamId(103);
        let context = client_test_context_with_paths(&[
            "tcp://127.0.0.1:10287?srtt-ms=20&rate-mbps=500",
            "tcp://127.0.0.1:10288?srtt-ms=10&rate-mbps=500",
        ]);
        let service_key = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        };
        let candidate_key = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 1,
        };
        let (service_commands, _service_rx) = reliable_path_command_channels(8);
        let mut remotes = ReliableRelayRemoteSet::new(
            opened_test_relay_stream(stream_id, service_key.index, service_commands),
            8,
        );
        let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(8);
        remotes.attach_for_validation(opened_test_relay_stream(
            stream_id,
            candidate_key.index,
            candidate_commands,
        ));
        consume_client_validation_proof_for_test(&mut candidate_rx);
        let service = remotes
            .path_instance_for_key(service_key)
            .expect("Service instance");
        let replacement = remotes
            .path_instance_for_key(candidate_key)
            .expect("replacement candidate instance");
        let stale = RelayPathInstance {
            key: candidate_key,
            id: replacement.id.wrapping_add(1000),
        };
        let startup_limit = usize::try_from(reliable_subflow_startup_sample_limit_bytes(
            context.mux_limits,
        ))
        .expect("startup limit");
        let mut epoch = FlowSubflowSet::new(0, service, startup_limit, 0, Duration::ZERO);
        assert_eq!(
            epoch
                .admit_subflow_owner(SubflowAdmissionInput {
                    key: replacement,
                    bulk_rate_proven: false,
                    startup_owner_allowed: true,
                    frontier_clear: true,
                    completion_improves: false,
                    observed_goodput_non_degrading: true,
                    read_gap: Duration::ZERO,
                    owner_bytes: BBR_MAX_SEND_QUANTUM_BYTES,
                    optional_overhead_bytes: 0,
                })
                .decision,
            PathAdmissionDecision::AdmitSubflow
        );
        let mut sender = RelaySenderService::new(stream_id);
        sender.ordered_data_owner = Some(service_key);
        sender.ordered_data_owner_instance = Some(service);
        sender.request_subflow_set = Some(epoch);
        sender.request_attempted_subflows.insert(replacement);
        let frame = client_data_frame_for_test(stream_id, 0, BBR_MAX_SEND_QUANTUM_BYTES);
        sender.flights.record_owner_frame_instance(stale, &frame);
        let owner_progress = sender.release_normalized_acked_ranges_with_owner_progress(
            &context,
            &[OffsetRange::new(0, BBR_MAX_SEND_QUANTUM_BYTES as u64).expect("ACK range")],
        );
        assert_eq!(owner_progress.len(), 1);
        assert_eq!(owner_progress[0].instance, stale);
        assert!(
            !sender.request_owner_ack_can_grow_window(&remotes, stale),
            "same-key progress from a detached instance must not grow the replacement epoch"
        );
        sender.reconcile_request_subflow_set(&context, &remotes);

        assert!(
            context
                .relay_path_has_bulk_model_evidence(candidate_key.underlay, candidate_key.index,)
        );
        assert_eq!(
            sender
                .request_subflow_set
                .as_ref()
                .and_then(FlowSubflowSet::startup_owner_key),
            Some(replacement),
            "logical-path evidence from an old attachment must not graduate the replacement"
        );
        assert!(!sender.request_graduated_subflows.contains(&replacement));
        assert!(
            !sender
                .request_startup_acked_bytes
                .contains_key(&replacement)
        );
    }

    #[tokio::test]
    async fn delayed_old_service_ack_cannot_authorize_replacement_service() {
        let stream_id = StreamId(109);
        let context = client_test_context_with_paths(&[
            "tcp://127.0.0.1:10293?srtt-ms=20&rate-mbps=500",
            "tcp://127.0.0.1:10294?srtt-ms=10&rate-mbps=500",
        ]);
        let service_key = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        };
        let candidate_key = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 1,
        };
        let (old_commands, mut old_rx) = reliable_path_command_channels(8);
        let mut remotes = ReliableRelayRemoteSet::new(
            opened_test_relay_stream(stream_id, service_key.index, old_commands),
            8,
        );
        let old_service = remotes
            .path_instance_for_key(service_key)
            .expect("old Service instance");
        let mut sender = RelaySenderService::new(stream_id);
        let stale_frame = client_data_frame_for_test(stream_id, 0, BBR_MAX_SEND_QUANTUM_BYTES);
        sender
            .send_stream_data(&context, &mut remotes, stale_frame.clone())
            .await
            .expect("send on old Service");
        assert!(matches!(
            try_recv_reliable_path_command(&mut old_rx),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
        ));
        let _removed = remotes
            .remove_path_instance(old_service)
            .expect("remove old Service attachment");

        let (replacement_commands, mut replacement_rx) = reliable_path_command_channels(8);
        remotes.attach(opened_test_relay_stream(
            stream_id,
            service_key.index,
            replacement_commands,
        ));
        let replacement_service = remotes
            .path_instance_for_key(service_key)
            .expect("replacement Service instance");
        assert_ne!(replacement_service, old_service);
        let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(8);
        remotes.attach_for_validation(opened_test_relay_stream(
            stream_id,
            candidate_key.index,
            candidate_commands,
        ));
        let candidate = remotes
            .path_instance_for_key(candidate_key)
            .expect("candidate instance");
        consume_client_validation_proof_for_test(&mut candidate_rx);
        mark_client_validation_proof_fresh_for_test(
            &context,
            &remotes,
            candidate,
            Duration::from_millis(10),
        );

        ack_client_frame_for_test(&mut sender, &context, &stale_frame);
        assert!(sender.request_rate_proven_subflows.contains(&old_service));
        assert!(
            !sender
                .request_rate_proven_subflows
                .contains(&replacement_service)
        );

        let replacement_frame = client_data_frame_for_test(
            stream_id,
            BBR_MAX_SEND_QUANTUM_BYTES as u64,
            BBR_MAX_SEND_QUANTUM_BYTES,
        );
        sender
            .send_stream_data(&context, &mut remotes, replacement_frame.clone())
            .await
            .expect("replacement must first establish itself as Service");
        assert!(matches!(
            try_recv_reliable_path_command(&mut replacement_rx),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
        ));
        assert!(try_recv_reliable_path_command(&mut candidate_rx).is_none());

        ack_client_frame_for_test(&mut sender, &context, &replacement_frame);
        assert!(
            sender
                .request_rate_proven_subflows
                .contains(&replacement_service)
        );
        let startup_frame = client_data_frame_for_test(
            stream_id,
            (2 * BBR_MAX_SEND_QUANTUM_BYTES) as u64,
            8 * 1024,
        );
        let outcome = sender
            .send_stream_data(&context, &mut remotes, startup_frame)
            .await
            .expect("current Service evidence may authorize bounded startup");
        assert_eq!(outcome.path_key, candidate_key);
        assert!(matches!(
            try_recv_reliable_path_command(&mut candidate_rx),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
        ));
    }

    #[tokio::test]
    async fn udp_product_stream_ack_does_not_create_quic_graduation_evidence() {
        let stream_id = StreamId(104);
        let context = client_test_context_with_paths(&[
            "udp://127.0.0.1:10289?srtt-ms=20&rate-mbps=500",
            "udp://127.0.0.1:10290?srtt-ms=10&rate-mbps=500",
        ]);
        let service_key = RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        };
        let candidate_key = RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 1,
        };
        let (service_commands, _service_rx) = reliable_path_command_channels(8);
        let mut remotes = ReliableRelayRemoteSet::new(
            opened_test_relay_stream_with_underlay(
                stream_id,
                UnderlayProtocol::Udp,
                service_key.index,
                service_commands,
            ),
            8,
        );
        let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(8);
        remotes.attach_for_validation(opened_test_relay_stream_with_underlay(
            stream_id,
            UnderlayProtocol::Udp,
            candidate_key.index,
            candidate_commands,
        ));
        consume_client_validation_proof_for_test(&mut candidate_rx);
        let service = remotes
            .path_instance_for_key(service_key)
            .expect("Service instance");
        let candidate = remotes
            .path_instance_for_key(candidate_key)
            .expect("candidate instance");
        let startup_limit = usize::try_from(reliable_subflow_startup_sample_limit_bytes(
            context.mux_limits,
        ))
        .expect("startup limit");
        let mut epoch = FlowSubflowSet::new(0, service, startup_limit, 0, Duration::ZERO);
        assert_eq!(
            epoch
                .admit_subflow_owner(SubflowAdmissionInput {
                    key: candidate,
                    bulk_rate_proven: false,
                    startup_owner_allowed: true,
                    frontier_clear: true,
                    completion_improves: false,
                    observed_goodput_non_degrading: true,
                    read_gap: Duration::ZERO,
                    owner_bytes: startup_limit,
                    optional_overhead_bytes: 0,
                })
                .decision,
            PathAdmissionDecision::AdmitSubflow
        );
        let mut sender = RelaySenderService::new(stream_id);
        sender.ordered_data_owner = Some(service_key);
        sender.ordered_data_owner_instance = Some(service);
        sender.request_subflow_set = Some(epoch);
        sender.request_attempted_subflows.insert(candidate);
        let frame = client_data_frame_for_test(stream_id, 0, startup_limit);
        sender
            .flights
            .record_owner_frame_instance(candidate, &frame);
        sender.release_normalized_acked_ranges(
            &context,
            &[OffsetRange::new(0, startup_limit as u64).expect("ACK range")],
        );
        sender.reconcile_request_subflow_set(&context, &remotes);

        assert!(
            !context
                .relay_path_has_bulk_model_evidence(candidate_key.underlay, candidate_key.index,)
        );
        assert!(!sender.request_startup_acked_bytes.contains_key(&candidate));
        assert!(!sender.request_graduated_subflows.contains(&candidate));
        assert_eq!(
            sender
                .request_subflow_set
                .as_ref()
                .and_then(FlowSubflowSet::startup_owner_key),
            Some(candidate),
            "QUIC requires direction-correct local carrier ACK evidence, not product STREAM_ACK timing"
        );
    }

    #[tokio::test]
    async fn ordered_receipt_proof_graduates_ambiguous_udp_startup_sample() {
        let stream_id = StreamId(110);
        let context = client_test_context_with_paths(&[
            "udp://127.0.0.1:10295?srtt-ms=20&rate-mbps=500",
            "udp://127.0.0.1:10296?srtt-ms=10&rate-mbps=500",
        ]);
        let service_key = RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        };
        let candidate_key = RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 1,
        };
        let (service_commands, _service_rx) = reliable_path_command_channels(8);
        let mut remotes = ReliableRelayRemoteSet::new(
            opened_test_relay_stream_with_underlay(
                stream_id,
                UnderlayProtocol::Udp,
                service_key.index,
                service_commands,
            ),
            8,
        );
        let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(8);
        remotes.attach_for_validation(opened_test_relay_stream_with_underlay(
            stream_id,
            UnderlayProtocol::Udp,
            candidate_key.index,
            candidate_commands,
        ));
        consume_client_validation_proof_for_test(&mut candidate_rx);
        let service = remotes
            .path_instance_for_key(service_key)
            .expect("Service instance");
        let candidate = remotes
            .path_instance_for_key(candidate_key)
            .expect("candidate instance");
        let startup_limit = usize::try_from(reliable_subflow_startup_sample_limit_bytes(
            context.mux_limits,
        ))
        .expect("startup limit");
        let mut epoch = FlowSubflowSet::new(0, service, startup_limit, 0, Duration::ZERO);
        assert_eq!(
            epoch
                .admit_subflow_owner(SubflowAdmissionInput {
                    key: candidate,
                    bulk_rate_proven: false,
                    startup_owner_allowed: true,
                    frontier_clear: true,
                    completion_improves: false,
                    observed_goodput_non_degrading: true,
                    read_gap: Duration::ZERO,
                    owner_bytes: startup_limit,
                    optional_overhead_bytes: 0,
                })
                .decision,
            PathAdmissionDecision::AdmitSubflow
        );
        let mut sender = RelaySenderService::new(stream_id);
        sender.ordered_data_owner = Some(service_key);
        sender.ordered_data_owner_instance = Some(service);
        sender.request_subflow_set = Some(epoch);
        sender.request_attempted_subflows.insert(candidate);
        let receipt_proof_id = 991;
        sender
            .request_startup_receipt_proofs
            .insert(candidate, (receipt_proof_id, 0));
        sender
            .request_startup_first_sent_at
            .insert(candidate, Instant::now());

        let frame = client_data_frame_for_test(stream_id, 0, startup_limit);
        sender
            .flights
            .record_owner_frame_instance(candidate, &frame);
        sender.flights.record_repair_frame_instance(service, &frame);
        sender.release_normalized_acked_ranges(
            &context,
            &[OffsetRange::new(0, startup_limit as u64).expect("ACK range")],
        );
        assert!(!sender.request_startup_acked_bytes.contains_key(&candidate));

        tokio::time::sleep(Duration::from_millis(10)).await;
        context.mark_relay_path_proof_observation(
            candidate_key.underlay,
            candidate_key.index,
            PathProofObservation {
                proof_id: receipt_proof_id + 1,
                bytes: PATH_OPEN_SCORE_BYTES as u64,
                elapsed: Duration::from_millis(10),
                sent_at: Instant::now(),
            },
        );
        sender.reconcile_request_subflow_set(&context, &remotes);
        assert_eq!(
            sender
                .request_subflow_set
                .as_ref()
                .and_then(FlowSubflowSet::startup_owner_key),
            Some(candidate),
            "a different proof on the shared path cannot graduate this attachment"
        );

        tokio::time::sleep(Duration::from_millis(10)).await;
        context.mark_relay_path_proof_observation(
            candidate_key.underlay,
            candidate_key.index,
            PathProofObservation {
                proof_id: receipt_proof_id,
                bytes: PATH_OPEN_SCORE_BYTES as u64,
                elapsed: Duration::from_millis(10),
                sent_at: Instant::now(),
            },
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        sender.reconcile_request_subflow_set(&context, &remotes);

        assert!(sender.request_graduated_subflows.contains(&candidate));
        assert_eq!(
            sender
                .request_subflow_set
                .as_ref()
                .and_then(FlowSubflowSet::startup_owner_key),
            None
        );
        let measured_rate = context.health.lock().expect("path health lock").udp
            [candidate_key.index]
            .measured_rate_bps
            .expect("receipt goodput sample");
        assert!(
            measured_rate > 60_000_000.0,
            "rate must use proof ACK completion, not a much later reconciliation poll: {measured_rate}"
        );
    }

    #[tokio::test]
    async fn udp_service_evidence_bootstraps_and_graduates_validation_candidate() {
        let stream_id = StreamId(114);
        let context = client_test_context_with_paths(&[
            "udp://127.0.0.1:10303?srtt-ms=20&rate-mbps=500",
            "udp://127.0.0.1:10304?srtt-ms=10&rate-mbps=500",
        ]);
        let service_key = RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        };
        let candidate_key = RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 1,
        };
        context.health.lock().expect("path health lock").udp[service_key.index]
            .mark_quic_path_metrics(UdpPathMetrics {
                direction: 1,
                srtt: Duration::from_millis(20),
                rttvar: Duration::from_millis(2),
                min_rtt: Duration::from_millis(18),
                min_rtt_observed: true,
                delivery_rate_bps: 500_000_000.0,
                pacing_rate_bps: 500_000_000.0,
                inflight_hi: 4 * 1024 * 1024,
                bytes_in_flight: 0,
                pending_bytes: 0,
                loss_ppm: Some(0),
                ecn_ppm: Some(0),
                app_limited: false,
                ack_derived_data_seen: true,
                delivery_sample_count: 1,
                delivery_sample_bytes: 4 * 1024 * 1024,
                last_delivery_sample_at: Some(Instant::now()),
            });
        let (service_commands, mut service_rx) = reliable_path_command_channels(16);
        let mut remotes = ReliableRelayRemoteSet::new(
            opened_test_relay_stream_with_underlay(
                stream_id,
                UnderlayProtocol::Udp,
                service_key.index,
                service_commands,
            ),
            16,
        );
        let service = remotes
            .path_instance_for_key(service_key)
            .expect("Service instance");
        let mut sender = RelaySenderService::new(stream_id);
        let service_frame = client_data_frame_for_test(stream_id, 0, BBR_MAX_SEND_QUANTUM_BYTES);
        sender
            .send_stream_data(&context, &mut remotes, service_frame.clone())
            .await
            .expect("send UDP Service evidence");
        assert!(matches!(
            try_recv_reliable_path_command(&mut service_rx),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
        ));
        ack_client_frame_for_test(&mut sender, &context, &service_frame);
        assert!(sender.request_rate_proven_subflows.contains(&service));
        assert!(
            context.relay_path_has_bulk_model_evidence(service_key.underlay, service_key.index,)
        );

        let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(16);
        remotes.attach_for_validation(opened_test_relay_stream_with_underlay(
            stream_id,
            UnderlayProtocol::Udp,
            candidate_key.index,
            candidate_commands,
        ));
        let candidate = remotes
            .path_instance_for_key(candidate_key)
            .expect("Validation candidate");
        consume_client_validation_proof_for_test(&mut candidate_rx);
        mark_client_validation_proof_fresh_for_test(
            &context,
            &remotes,
            candidate,
            Duration::from_millis(10),
        );
        let startup_limit = usize::try_from(reliable_subflow_startup_sample_limit_bytes(
            context.mux_limits,
        ))
        .expect("startup limit");
        let mut offset = BBR_MAX_SEND_QUANTUM_BYTES as u64;
        let mut sent = 0;
        while sent < startup_limit {
            let payload_bytes = BBR_MAX_SEND_QUANTUM_BYTES.min(startup_limit - sent);
            let frame = client_data_frame_for_test(stream_id, offset, payload_bytes);
            let outcome = sender
                .send_stream_data(&context, &mut remotes, frame.clone())
                .await
                .expect("bounded UDP startup sample");
            assert_eq!(outcome.path_key, candidate_key);
            assert!(matches!(
                try_recv_reliable_path_command(&mut candidate_rx),
                Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
            ));
            ack_client_frame_for_test(&mut sender, &context, &frame);
            sent += payload_bytes;
            offset += payload_bytes as u64;
        }
        let (receipt_proof_id, _) = sender.request_startup_receipt_proofs[&candidate];
        assert!(matches!(
            try_recv_reliable_path_command(&mut candidate_rx),
            Some(ReliablePathCommand::SendFrame(Frame::PathProofData {
                proof_id,
                ..
            })) if proof_id == receipt_proof_id
        ));
        context.mark_relay_path_proof_observation(
            candidate_key.underlay,
            candidate_key.index,
            PathProofObservation {
                proof_id: receipt_proof_id,
                bytes: PATH_OPEN_SCORE_BYTES as u64,
                elapsed: Duration::from_millis(10),
                sent_at: Instant::now(),
            },
        );
        sender.reconcile_request_subflow_set(&context, &remotes);

        assert!(sender.request_graduated_subflows.contains(&candidate));
        assert!(
            context
                .relay_path_has_bulk_model_evidence(candidate_key.underlay, candidate_key.index,)
        );
        assert_eq!(
            sender
                .request_subflow_set
                .as_ref()
                .and_then(FlowSubflowSet::startup_owner_key),
            None
        );
    }

    #[tokio::test]
    async fn startup_candidate_can_progress_when_service_command_queue_is_full() {
        let stream_id = StreamId(105);
        let context = client_test_context_with_paths(&[
            "tcp://127.0.0.1:10291?srtt-ms=20&rate-mbps=500",
            "tcp://127.0.0.1:10292?srtt-ms=10&rate-mbps=500",
        ]);
        let service_key = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        };
        let candidate_key = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 1,
        };
        seed_client_bulk_evidence_for_test(&context, service_key);
        let (service_commands, _service_rx) = reliable_path_command_channels(1);
        service_commands
            .try_enqueue_admitted_frame(
                client_data_frame_for_test(stream_id, 0, BBR_MAX_SEND_QUANTUM_BYTES),
                FlowLane::Throughput,
            )
            .expect("fill Service data queue");
        let mut remotes = ReliableRelayRemoteSet::new(
            opened_test_relay_stream(stream_id, service_key.index, service_commands),
            8,
        );
        let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(8);
        remotes.attach_for_validation(opened_test_relay_stream(
            stream_id,
            candidate_key.index,
            candidate_commands,
        ));
        consume_client_validation_proof_for_test(&mut candidate_rx);
        let candidate = remotes
            .path_instance_for_key(candidate_key)
            .expect("candidate instance");
        mark_client_validation_proof_fresh_for_test(
            &context,
            &remotes,
            candidate,
            Duration::from_millis(10),
        );
        let mut sender = RelaySenderService::new(stream_id);
        let service = remotes
            .path_instance_for_key(service_key)
            .expect("Service instance");
        sender.ordered_data_owner = Some(service_key);
        sender.ordered_data_owner_instance = Some(service);
        sender.request_rate_proven_subflows.insert(service);

        let outcome = sender
            .send_stream_data(
                &context,
                &mut remotes,
                client_data_frame_for_test(stream_id, 0, 8 * 1024),
            )
            .await
            .expect("fresh candidate should provide bounded overflow credit");

        assert_eq!(outcome.path_key, candidate_key);
        assert!(matches!(
            try_recv_reliable_path_command(&mut candidate_rx),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
        ));
        assert_eq!(sender.ordered_data_owner, Some(service_key));
    }

    #[tokio::test]
    async fn failed_path_proof_enqueue_retries_without_sticking_validation() {
        let stream_id = StreamId(106);
        let context = client_test_context();
        let (commands, mut receivers) = reliable_path_command_channels(1);
        commands
            .try_enqueue_admitted_frame(
                Frame::StreamAck {
                    stream_id,
                    complete: false,
                    ranges: Vec::new(),
                },
                FlowLane::Control,
            )
            .expect("fill priority queue");
        let mut remotes =
            ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, commands), 4);
        remotes.paths[0].placement = RelayPathPlacement::Validation;
        remotes.paths[0].path_proof_id = None;
        assert!(matches!(
            try_recv_reliable_path_priority_command(&mut receivers),
            Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
        ));

        remotes.retry_pending_path_proofs(&context);

        assert!(remotes.paths[0].path_proof_id.is_some());
        assert!(matches!(
            try_recv_reliable_path_priority_command(&mut receivers),
            Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
        ));
    }

    #[tokio::test]
    async fn queued_path_proof_keeps_one_identity_until_ack_or_path_failure() {
        let stream_id = StreamId(108);
        let context = client_test_context();
        let (commands, mut receivers) = reliable_path_command_channels(2);
        let mut remotes =
            ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, commands), 4);
        remotes.paths[0].placement = RelayPathPlacement::Validation;
        remotes.paths[0].path_proof_id = Some(41);

        remotes.retry_pending_path_proofs(&context);

        assert_eq!(remotes.paths[0].path_proof_id, Some(41));
        assert!(try_recv_reliable_path_priority_command(&mut receivers).is_none());

        context.health.lock().expect("path health lock").tcp[0].invalidate_path_proofs();
        remotes.retry_pending_path_proofs(&context);
        assert_ne!(remotes.paths[0].path_proof_id, Some(41));
        assert!(matches!(
            try_recv_reliable_path_priority_command(&mut receivers),
            Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
        ));
    }

    #[tokio::test]
    async fn invalidated_startup_receipt_proof_requeues_in_new_generation() {
        let stream_id = StreamId(113);
        let context = client_test_context_with_paths(&[
            "tcp://127.0.0.1:10301?srtt-ms=20&rate-mbps=500",
            "tcp://127.0.0.1:10302?srtt-ms=10&rate-mbps=500",
        ]);
        let (service_commands, _service_rx) = reliable_path_command_channels(8);
        let mut remotes = ReliableRelayRemoteSet::new(
            opened_test_relay_stream(stream_id, 0, service_commands),
            8,
        );
        let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(8);
        remotes.attach_for_validation(opened_test_relay_stream(stream_id, 1, candidate_commands));
        consume_client_validation_proof_for_test(&mut candidate_rx);
        let service = remotes
            .paths
            .iter()
            .find(|path| path.placement == RelayPathPlacement::Active)
            .expect("Active Service")
            .instance();
        let candidate_path = remotes
            .paths
            .iter()
            .find(|path| path.placement == RelayPathPlacement::Validation)
            .expect("Validation candidate");
        let candidate = candidate_path.instance();
        let attached_at = candidate_path.attached_at;
        let mut epoch = FlowSubflowSet::new(0, service, 64 * 1024, 0, Duration::ZERO);
        assert_eq!(
            epoch
                .admit_subflow_owner(SubflowAdmissionInput {
                    key: candidate,
                    bulk_rate_proven: false,
                    startup_owner_allowed: true,
                    frontier_clear: true,
                    completion_improves: false,
                    observed_goodput_non_degrading: true,
                    read_gap: Duration::ZERO,
                    owner_bytes: 64 * 1024,
                    optional_overhead_bytes: 0,
                })
                .decision,
            PathAdmissionDecision::AdmitSubflow
        );
        let mut sender = RelaySenderService::new(stream_id);
        sender.request_subflow_set = Some(epoch);
        sender.try_enqueue_request_startup_receipt_proof(&context, &remotes, candidate);
        let (old_proof_id, old_generation) = sender.request_startup_receipt_proofs[&candidate];
        assert_eq!(old_generation, 0);
        assert!(matches!(
            try_recv_reliable_path_command(&mut candidate_rx),
            Some(ReliablePathCommand::SendFrame(Frame::PathProofData {
                proof_id,
                ..
            })) if proof_id == old_proof_id
        ));
        let stale_sent_at = Instant::now();

        context.health.lock().expect("path health lock").tcp[1].invalidate_path_proofs();
        context.mark_relay_path_proof_observation(
            candidate.key.underlay,
            candidate.key.index,
            PathProofObservation {
                proof_id: old_proof_id,
                bytes: PATH_OPEN_SCORE_BYTES as u64,
                elapsed: Duration::from_millis(10),
                sent_at: stale_sent_at,
            },
        );
        assert!(!context.relay_path_has_fresh_proof(
            candidate.key.underlay,
            candidate.key.index,
            old_proof_id,
            attached_at,
        ));

        sender.try_enqueue_request_startup_receipt_proof(&context, &remotes, candidate);
        let (new_proof_id, new_generation) = sender.request_startup_receipt_proofs[&candidate];
        assert_eq!(new_generation, 1);
        assert_ne!(new_proof_id, old_proof_id);
        assert!(matches!(
            try_recv_reliable_path_command(&mut candidate_rx),
            Some(ReliablePathCommand::SendFrame(Frame::PathProofData {
                proof_id,
                ..
            })) if proof_id == new_proof_id
        ));
    }

    #[test]
    fn service_epoch_reset_retains_attempted_and_graduated_instance_tombstones() {
        let key = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 1,
        };
        let attempted = RelayPathInstance { key, id: 7 };
        let graduated = RelayPathInstance { key, id: 8 };
        let mut sender = RelaySenderService::new(StreamId(107));
        sender.request_attempted_subflows.insert(attempted);
        sender.request_attempted_subflows.insert(graduated);
        sender.request_graduated_subflows.insert(graduated);
        sender.request_subflow_set = Some(FlowSubflowSet::new(
            0,
            RelayPathInstance { key, id: 1 },
            256 * 1024,
            0,
            Duration::ZERO,
        ));

        sender.reset_request_subflow_epoch();

        assert!(sender.request_subflow_set.is_none());
        assert!(sender.request_attempted_subflows.contains(&attempted));
        assert!(sender.request_attempted_subflows.contains(&graduated));
        assert!(sender.request_graduated_subflows.contains(&graduated));
    }

    #[tokio::test]
    async fn startup_epoch_clears_when_candidate_is_no_longer_validation() {
        let stream_id = StreamId(111);
        let context = client_test_context_with_paths(&[
            "tcp://127.0.0.1:10297?srtt-ms=20&rate-mbps=500",
            "tcp://127.0.0.1:10298?srtt-ms=10&rate-mbps=500",
        ]);
        let (service_commands, _service_rx) = reliable_path_command_channels(8);
        let mut remotes = ReliableRelayRemoteSet::new(
            opened_test_relay_stream(stream_id, 0, service_commands),
            8,
        );
        let (candidate_commands, _candidate_rx) = reliable_path_command_channels(8);
        remotes.attach_for_validation(opened_test_relay_stream(stream_id, 1, candidate_commands));
        let service = remotes
            .paths
            .iter()
            .find(|path| path.placement == RelayPathPlacement::Active)
            .expect("Active Service")
            .instance();
        let candidate = remotes
            .paths
            .iter()
            .find(|path| path.placement == RelayPathPlacement::Validation)
            .expect("Validation candidate")
            .instance();
        let mut epoch = FlowSubflowSet::new(0, service, 256 * 1024, 0, Duration::ZERO);
        assert_eq!(
            epoch
                .admit_subflow_owner(SubflowAdmissionInput {
                    key: candidate,
                    bulk_rate_proven: false,
                    startup_owner_allowed: true,
                    frontier_clear: true,
                    completion_improves: false,
                    observed_goodput_non_degrading: true,
                    read_gap: Duration::ZERO,
                    owner_bytes: 64 * 1024,
                    optional_overhead_bytes: 0,
                })
                .decision,
            PathAdmissionDecision::AdmitSubflow
        );
        let mut sender = RelaySenderService::new(stream_id);
        sender.ordered_data_owner = Some(service.key);
        sender.ordered_data_owner_instance = Some(service);
        sender.request_subflow_set = Some(epoch);
        sender.request_attempted_subflows.insert(candidate);
        remotes
            .paths
            .iter_mut()
            .find(|path| path.instance() == candidate)
            .expect("candidate path")
            .placement = RelayPathPlacement::Active;

        sender.reconcile_request_subflow_set(&context, &remotes);

        assert!(sender.request_subflow_set.is_none());
        assert!(
            sender.request_attempted_subflows.contains(&candidate),
            "a live role change invalidates the epoch without minting fresh credit"
        );
    }

    #[tokio::test]
    async fn orphaned_validation_owner_tail_repairs_on_active_service() {
        let stream_id = StreamId(112);
        let limits = MuxLimits::default();
        let context = client_test_context_with_paths(&[
            "tcp://127.0.0.1:10299?srtt-ms=20&rate-mbps=500",
            "tcp://127.0.0.1:10300?srtt-ms=10&rate-mbps=500",
        ]);
        let (service_commands, mut service_rx) = reliable_path_command_channels(8);
        let mut remotes = ReliableRelayRemoteSet::new(
            opened_test_relay_stream(stream_id, 0, service_commands),
            8,
        );
        let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(8);
        remotes.attach_for_validation(opened_test_relay_stream(stream_id, 1, candidate_commands));
        consume_client_validation_proof_for_test(&mut candidate_rx);
        let service = remotes
            .paths
            .iter()
            .find(|path| path.placement == RelayPathPlacement::Active)
            .expect("Active Service")
            .instance();
        let candidate = remotes
            .paths
            .iter()
            .find(|path| path.placement == RelayPathPlacement::Validation)
            .expect("Validation candidate")
            .instance();
        let mut send_stream = ReliableSendStream::new(stream_id, limits);
        let _prefix = send_stream
            .send_data(Bytes::from(vec![0x31; 64]), StreamFlags::NONE)
            .expect("prefix");
        let candidate_tail = send_stream
            .send_data(Bytes::from(vec![0x32; 64]), StreamFlags::NONE)
            .expect("candidate tail");
        let ack_ranges = [OffsetRange::new(0, 64).expect("prefix ACK")];
        let _ = send_stream.apply_ack(&ack_ranges);

        let mut sender = RelaySenderService::new(stream_id);
        sender.ordered_data_owner = Some(service.key);
        sender.ordered_data_owner_instance = Some(service);
        sender
            .flights
            .record_owner_frame_instance(candidate, &candidate_tail);
        sender.age_product_flights_for_test(Duration::from_secs(10));
        sender.reset_request_subflow_epoch();
        let mut sender_queue = ReliableRelaySenderQueue::default();
        assert!(sender.enqueue_live_owner_tail_repair(
            &mut sender_queue,
            &context,
            &remotes,
            &send_stream,
            &ack_ranges,
            true,
            64,
            FlowLane::Throughput,
        ));
        assert_eq!(
            sender.discard_unusable_live_owner_tail_repairs(&mut sender_queue, &remotes),
            0,
            "ledger-owned Validation debt remains a live repair source after epoch reset"
        );
        let spec = ReliableRelayOpenSpec {
            target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80))),
            ingress: IngressKind::Socks5,
        };
        let dispatch = sender
            .dispatch_client_queued_work(
                &context,
                &spec,
                FlowLane::Throughput,
                &mut remotes,
                &mut send_stream,
                &mut sender_queue,
                true,
                64,
            )
            .await
            .expect("dispatch repair on Service");
        assert!(matches!(dispatch, ClientQueuedDispatch::Repair { .. }));
        assert!(matches!(
            try_recv_reliable_path_command(&mut service_rx),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData {
                offset: 64,
                ..
            }))
        ));
        assert!(try_recv_reliable_path_command(&mut candidate_rx).is_none());
    }

    #[test]
    fn response_repair_extra_budget_is_cumulative_not_per_event() {
        let mux_limits = MuxLimits::default();
        let stream_id = StreamId(91);
        let mut sender = ServerResponseSenderService::new_with_performance(
            SessionId(91),
            stream_id,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 1,
            },
        );
        let startup_floor = response_extra_traffic_startup_floor_bytes(mux_limits);
        let repair_payload = Bytes::from(vec![0x55; startup_floor]);

        assert_eq!(
            sender.repair_extra_budget_remaining(mux_limits),
            startup_floor
        );
        assert!(
            sender
                .enqueue_repair_frame_with_priority(
                    Frame::StreamData {
                        stream_id,
                        offset: 0,
                        flags: StreamFlags::NONE,
                        payload: repair_payload.clone(),
                    },
                    mux_limits,
                    false,
                )
                .is_some(),
            "startup repair floor should be spendable once"
        );
        assert_eq!(sender.repair_extra_budget_remaining(mux_limits), 0);
        assert!(
            sender
                .enqueue_repair_frame_with_priority(
                    Frame::StreamData {
                        stream_id,
                        offset: startup_floor as u64,
                        flags: StreamFlags::NONE,
                        payload: repair_payload.clone(),
                    },
                    mux_limits,
                    false,
                )
                .is_none(),
            "repair budget must be cumulative, not refreshed for every tail/ACK event"
        );

        let earned_data_bytes = startup_floor.saturating_mul(100);
        sender.record_owner_progress_for_test(earned_data_bytes);

        assert!(
            sender.repair_extra_budget_remaining(mux_limits) >= startup_floor,
            "ACK-released owner progress earns more bounded extra repair budget"
        );
        assert!(
            sender
                .enqueue_repair_frame_with_priority(
                    Frame::StreamData {
                        stream_id,
                        offset: (startup_floor * 2) as u64,
                        flags: StreamFlags::NONE,
                        payload: repair_payload,
                    },
                    mux_limits,
                    false,
                )
                .is_some()
        );
    }

    #[test]
    fn response_source_read_budget_is_separate_from_repair_cache_retention() {
        let stream_id = StreamId(93);
        let mux_limits = MuxLimits {
            max_repair_bytes: 4096,
            max_payload_bytes: 4096,
            max_stream_window_bytes: 64 * 1024,
            max_path_flight_bytes: 4096,
            ..MuxLimits::default()
        };
        let mut send_stream =
            ReliableSendStream::new_with_initial_max_offset(stream_id, mux_limits, u64::MAX);
        send_stream
            .send_data(
                Bytes::from(vec![0x5a; mux_limits.max_repair_bytes]),
                StreamFlags::NONE,
            )
            .expect("seed retained unacked OwnerData");
        assert_eq!(send_stream.repair_bytes(), mux_limits.max_repair_bytes);

        let sender_queue = ReliableRelaySenderQueue::default();
        assert!(
            reliable_relay_can_read_into_sender_queue(
                &send_stream,
                &sender_queue,
                mux_limits,
                mux_limits.max_repair_bytes,
            ),
            "repair cache retention is unacked OwnerData memory, not already-queued source bytes"
        );
        assert_eq!(
            reliable_relay_sender_queue_read_budget(
                &send_stream,
                &sender_queue,
                mux_limits,
                mux_limits.max_repair_bytes,
                mux_limits.max_repair_bytes,
            ),
            mux_limits.max_repair_bytes,
            "bounded product-source reads may continue while dispatch waits for repair-cache ACK release"
        );
    }

    #[test]
    fn mixed_response_dispatch_payload_is_bounded_by_remaining_repair_capacity() {
        let stream_id = StreamId(98);
        let mux_limits = MuxLimits {
            max_payload_bytes: 4096,
            max_repair_bytes: 4096,
            max_path_flight_bytes: 4096,
            max_reliable_relay_chunk_bytes: 4096,
            ..MuxLimits::default()
        };
        let (commands, _active_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new_with_limits(
            SessionId(98),
            UnderlayProtocol::Tcp,
            PathId(0),
            commands,
            FlowLane::Throughput,
            mux_limits,
        );
        let (validation_commands, _validation_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                UnderlayProtocol::Udp,
                PathId(1),
                validation_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                4096,
            ),
            ResponseStreamAttachOutcome::Attached
        );
        let (_frame_tx, frame_rx) = mpsc::channel(1);
        let path_stream = ReliablePathStream {
            stream_id,
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: UnderlayProtocol::Tcp,
            max_frame_payload_bytes: 4096,
            output: ReliablePathStreamOutput::Switchable(binding),
            frames: frame_rx,
        };
        let mut send_stream =
            ReliableSendStream::new_with_initial_max_offset(stream_id, mux_limits, u64::MAX);
        send_stream
            .send_data(Bytes::from(vec![0x5a; 3072]), StreamFlags::NONE)
            .expect("seed retained OwnerData");

        assert_eq!(
            response_dispatch_payload_bytes(
                &path_stream,
                &send_stream,
                FlowLane::Throughput,
                mux_limits,
                4096,
            ),
            Some(1024),
        );
        send_stream
            .send_data(Bytes::from(vec![0x5a; 1024]), StreamFlags::NONE)
            .expect("fill repair cache");
        assert_eq!(
            response_dispatch_payload_bytes(
                &path_stream,
                &send_stream,
                FlowLane::Throughput,
                mux_limits,
                4096,
            ),
            None,
        );
    }

    #[test]
    fn coupled_response_dispatch_keeps_the_authoritative_send_stream_check() {
        let stream_id = StreamId(97);
        let mux_limits = MuxLimits {
            max_payload_bytes: 4096,
            max_repair_bytes: 4096,
            max_path_flight_bytes: 4096,
            max_reliable_relay_chunk_bytes: 4096,
            ..MuxLimits::default()
        };
        let (commands, _receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new_with_limits(
            SessionId(97),
            UnderlayProtocol::Tcp,
            PathId(0),
            commands,
            FlowLane::Throughput,
            mux_limits,
        );
        let (_frame_tx, frame_rx) = mpsc::channel(1);
        let path_stream = ReliablePathStream {
            stream_id,
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: UnderlayProtocol::Tcp,
            max_frame_payload_bytes: 4096,
            output: ReliablePathStreamOutput::Switchable(binding),
            frames: frame_rx,
        };
        let mut send_stream =
            ReliableSendStream::new_with_initial_max_offset(stream_id, mux_limits, u64::MAX);
        send_stream
            .send_data(Bytes::from(vec![0x5a; 4096]), StreamFlags::NONE)
            .expect("fill repair cache");

        assert_eq!(
            response_dispatch_payload_bytes(
                &path_stream,
                &send_stream,
                FlowLane::Throughput,
                mux_limits,
                4096,
            ),
            Some(4096),
            "coupled paths retain the existing send-stream repair-capacity boundary"
        );
    }

    #[tokio::test]
    async fn formerly_mixed_response_retains_repair_preflight_after_family_detach() {
        let stream_id = StreamId(96);
        let mux_limits = MuxLimits {
            max_payload_bytes: 4096,
            max_repair_bytes: 4096,
            max_path_flight_bytes: 4096,
            max_reliable_relay_chunk_bytes: 4096,
            ..MuxLimits::default()
        };
        let (commands, _active_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new_with_limits(
            SessionId(96),
            UnderlayProtocol::Tcp,
            PathId(0),
            commands,
            FlowLane::Throughput,
            mux_limits,
        );
        let udp_key = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let (udp_commands, _udp_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                udp_key.underlay,
                udp_key.path_id,
                udp_commands.clone(),
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                4096,
            ),
            ResponseStreamAttachOutcome::Attached
        );
        assert!(binding.has_live_mixed_owner_underlays());
        binding.detach(udp_key, &udp_commands);
        assert!(!binding.has_live_mixed_owner_underlays());
        assert!(binding.may_have_mixed_owner_underlays());

        let (_frame_tx, frame_rx) = mpsc::channel(1);
        let path_stream = ReliablePathStream {
            stream_id,
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: UnderlayProtocol::Tcp,
            max_frame_payload_bytes: 4096,
            output: ReliablePathStreamOutput::Switchable(binding),
            frames: frame_rx,
        };
        let mut send_stream =
            ReliableSendStream::new_with_initial_max_offset(stream_id, mux_limits, u64::MAX);
        send_stream
            .send_data(Bytes::from(vec![0x5a; 3072]), StreamFlags::NONE)
            .expect("seed retained OwnerData");
        let mut sender = ServerResponseSenderService::new(SessionId(96), stream_id);
        sender.enqueue_data_for_lane(Bytes::from(vec![0x33; 4096]), FlowLane::Throughput);

        let first = sender
            .dispatch_next(
                &path_stream,
                &mut send_stream,
                FlowLane::Throughput,
                mux_limits,
            )
            .await
            .expect("formerly mixed raw bytes dispatch within remaining repair capacity");
        assert_eq!(first.payload_bytes, 1024);
        assert_eq!(send_stream.repair_bytes(), 4096);
        assert_eq!(sender.data_bytes(), 3072);
        assert!(matches!(
            sender
                .dispatch_next(
                    &path_stream,
                    &mut send_stream,
                    FlowLane::Throughput,
                    mux_limits,
                )
                .await,
            Err(RuntimeError::SenderServiceBlocked)
        ));
        assert_eq!(sender.data_bytes(), 3072);
    }

    #[tokio::test]
    async fn mixed_response_dispatch_waits_retryably_when_repair_cache_is_full() {
        let stream_id = StreamId(99);
        let mux_limits = MuxLimits {
            max_payload_bytes: 4096,
            max_repair_bytes: 4096,
            max_path_flight_bytes: 4096,
            max_reliable_relay_chunk_bytes: 4096,
            ..MuxLimits::default()
        };
        let (commands, _active_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new_with_limits(
            SessionId(99),
            UnderlayProtocol::Tcp,
            PathId(0),
            commands,
            FlowLane::Throughput,
            mux_limits,
        );
        let (validation_commands, _validation_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                UnderlayProtocol::Udp,
                PathId(1),
                validation_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                4096,
            ),
            ResponseStreamAttachOutcome::Attached
        );
        let (_frame_tx, frame_rx) = mpsc::channel(1);
        let path_stream = ReliablePathStream {
            stream_id,
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: UnderlayProtocol::Tcp,
            max_frame_payload_bytes: 4096,
            output: ReliablePathStreamOutput::Switchable(binding),
            frames: frame_rx,
        };
        let mut send_stream =
            ReliableSendStream::new_with_initial_max_offset(stream_id, mux_limits, u64::MAX);
        send_stream
            .send_data(Bytes::from(vec![0x5a; 4096]), StreamFlags::NONE)
            .expect("fill repair cache");
        let blocked_offset = send_stream.next_offset();
        let mut sender = ServerResponseSenderService::new(SessionId(99), stream_id);
        sender.enqueue_data_for_lane(Bytes::from_static(b"next"), FlowLane::Throughput);

        assert!(matches!(
            sender
                .dispatch_next(
                    &path_stream,
                    &mut send_stream,
                    FlowLane::Throughput,
                    mux_limits,
                )
                .await,
            Err(RuntimeError::SenderServiceBlocked)
        ));
        assert_eq!(send_stream.next_offset(), blocked_offset);
        assert_eq!(sender.data_bytes(), 4, "blocked raw bytes remain queued");

        send_stream.apply_ack(&[OffsetRange {
            start: 0,
            end: blocked_offset,
        }]);
        sender
            .dispatch_next(
                &path_stream,
                &mut send_stream,
                FlowLane::Throughput,
                mux_limits,
            )
            .await
            .expect("ACK release restores dispatch capacity");
        assert_eq!(sender.data_bytes(), 0);
    }

    #[test]
    fn response_repair_extra_budget_accumulates_until_useful_attempt() {
        let mux_limits = MuxLimits::default();
        let stream_id = StreamId(92);
        let mut sender = ServerResponseSenderService::new_with_performance(
            SessionId(92),
            stream_id,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 1,
            },
        );
        let startup_floor = response_extra_traffic_startup_floor_bytes(mux_limits);
        let min_attempt = response_repair_minimum_useful_attempt_bytes(mux_limits);

        assert!(sender.repair_extra_event_budget_remaining(mux_limits) >= min_attempt);
        assert!(
            sender
                .enqueue_repair_frame_with_priority(
                    Frame::StreamData {
                        stream_id,
                        offset: 0,
                        flags: StreamFlags::NONE,
                        payload: Bytes::from(vec![0x44; startup_floor]),
                    },
                    mux_limits,
                    false,
                )
                .is_some()
        );

        sender.record_owner_progress_for_test(startup_floor);
        assert!(
            sender.repair_extra_budget_remaining(mux_limits) > 0,
            "ACK-released owner progress earns fractional repair budget"
        );
        assert_eq!(
            sender.repair_extra_event_budget_remaining(mux_limits),
            0,
            "tiny earned repair crumbs should accumulate instead of emitting high-overhead repair frames"
        );

        sender.record_owner_progress_for_test(min_attempt.saturating_mul(100));
        assert!(
            sender.repair_extra_event_budget_remaining(mux_limits) >= min_attempt,
            "once enough owner bytes make ACK progress, repair can spend a useful attempt"
        );
    }

    #[tokio::test]
    async fn response_owner_dispatch_does_not_earn_repair_budget_before_ack_progress() {
        let mux_limits = MuxLimits::default();
        let stream_id = StreamId(96);
        let startup_floor = response_extra_traffic_startup_floor_bytes(mux_limits);
        let (commands, _receivers) = reliable_path_command_channels(8);
        let (_frame_tx, frame_rx) = mpsc::channel(1);
        let path_stream = ReliablePathStream {
            stream_id,
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: UnderlayProtocol::Tcp,
            max_frame_payload_bytes: reliable_relay_buffer_len(mux_limits),
            output: ReliablePathStreamOutput::fixed(
                UnderlayProtocol::Tcp,
                PathId(0),
                commands,
                mux_limits,
            ),
            frames: frame_rx,
        };
        let mut send_stream =
            ReliableSendStream::new_with_initial_max_offset(stream_id, mux_limits, u64::MAX);
        let mut sender = ServerResponseSenderService::new_with_performance(
            SessionId(96),
            stream_id,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 1,
            },
        );

        sender
            .extra_traffic
            .record_optional(ExtraTrafficKind::Repair, startup_floor);
        assert_eq!(sender.repair_extra_budget_remaining(mux_limits), 0);

        sender.enqueue_data_for_lane(
            Bytes::from(vec![0x96; startup_floor.saturating_mul(100)]),
            FlowLane::Throughput,
        );
        sender
            .dispatch_next(
                &path_stream,
                &mut send_stream,
                FlowLane::Throughput,
                mux_limits,
            )
            .await
            .expect("owner dispatch should not be blocked by exhausted repair budget");

        assert_eq!(
            sender.repair_extra_budget_remaining(mux_limits),
            0,
            "emitted OwnerData must not earn optional repair budget until ordered ACK progress releases it"
        );
    }

    #[cfg(feature = "lab-diagnostics")]
    #[tokio::test]
    async fn fixed_output_owner_data_records_sender_service_decision_for_conformance() {
        let _guard = lab_diag_test_guard();
        let mux_limits = MuxLimits::default();
        let session_id = SessionId(97);
        let stream_id = StreamId(97);
        let (commands, _receivers) = reliable_path_command_channels(8);
        let (_frame_tx, frame_rx) = mpsc::channel(1);
        let path_stream = ReliablePathStream {
            stream_id,
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: UnderlayProtocol::Tcp,
            max_frame_payload_bytes: reliable_relay_buffer_len(mux_limits),
            output: ReliablePathStreamOutput::fixed(
                UnderlayProtocol::Tcp,
                PathId(0),
                commands,
                mux_limits,
            ),
            frames: frame_rx,
        };
        let mut send_stream =
            ReliableSendStream::new_with_initial_max_offset(stream_id, mux_limits, u64::MAX);
        let mut sender = ServerResponseSenderService::new(session_id, stream_id);

        sender.enqueue_data_for_lane(Bytes::from_static(b"owner"), FlowLane::Throughput);
        sender
            .dispatch_next(
                &path_stream,
                &mut send_stream,
                FlowLane::Throughput,
                mux_limits,
            )
            .await
            .expect("fixed output OwnerData dispatch should succeed");

        assert_eq!(
            lab_sender_service_counts_for_test(session_id.0, stream_id.0),
            (1, 1),
            "fixed output OwnerData must be accounted as a sender-service decision"
        );
        lab_assert_server_sender_service_balanced(session_id.0, stream_id.0);
    }

    #[test]
    fn response_critical_repair_closes_tail_after_optional_budget_exhaustion() {
        let mux_limits = MuxLimits::default();
        let stream_id = StreamId(94);
        let mut sender = ServerResponseSenderService::new_with_performance(
            SessionId(94),
            stream_id,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 1,
            },
        );
        let startup_floor = response_extra_traffic_startup_floor_bytes(mux_limits);
        let frame = Frame::StreamData {
            stream_id,
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![0x44; startup_floor]),
        };
        assert!(
            sender
                .enqueue_repair_frame_with_priority(frame, mux_limits, false)
                .is_some()
        );

        let closure_frame = Frame::StreamData {
            stream_id,
            offset: startup_floor as u64,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(b"tail"),
        };
        assert!(
            sender
                .enqueue_repair_frame_with_priority(closure_frame.clone(), mux_limits, false)
                .is_none(),
            "ordinary optional repair budget should be exhausted"
        );

        sender.enqueue_critical_repair_frame(closure_frame);
        assert_eq!(sender.repair_extra_budget_remaining(mux_limits), 0);
    }

    #[test]
    fn response_critical_tail_repair_is_idempotent_while_range_is_queued() {
        let mux_limits = MuxLimits::default();
        let stream_id = StreamId(96);
        let mut sender = ServerResponseSenderService::new_with_performance(
            SessionId(96),
            stream_id,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 1,
            },
        );
        let first = Frame::StreamData {
            stream_id,
            offset: 128,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(&[0x44; 64]),
        };
        let duplicate = first.clone();

        assert!(sender.enqueue_critical_tail_repair_frame(first).is_some());
        let bytes_after_first = sender.bytes();
        let budget_after_first = sender.repair_extra_budget_remaining(mux_limits);

        assert!(
            sender
                .enqueue_critical_tail_repair_frame(duplicate)
                .is_none(),
            "final-tail RepairData is a one pending repair per offset range, not a repeatable owner-data substitute"
        );
        assert_eq!(sender.bytes(), bytes_after_first);
        assert_eq!(
            sender.repair_extra_budget_remaining(mux_limits),
            budget_after_first
        );
    }

    #[test]
    fn client_repair_extra_budget_is_cumulative_not_per_event() {
        let mux_limits = MuxLimits::default();
        let stream_id = StreamId(93);
        let mut sender = RelaySenderService::new_with_performance(
            stream_id,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 1,
            },
        );
        let mut sender_queue = ReliableRelaySenderQueue::default();
        let startup_floor = response_extra_traffic_startup_floor_bytes(mux_limits);
        let repair_payload = Bytes::from(vec![0x33; startup_floor]);

        assert!(sender.enqueue_repair_frame_with_priority(
            &mut sender_queue,
            Frame::StreamData {
                stream_id,
                offset: 0,
                flags: StreamFlags::NONE,
                payload: repair_payload.clone(),
            },
            RelaySendCause::AckGapRepair,
            mux_limits,
            false,
        ));
        assert!(!sender.enqueue_repair_frame_with_priority(
            &mut sender_queue,
            Frame::StreamData {
                stream_id,
                offset: startup_floor as u64,
                flags: StreamFlags::NONE,
                payload: repair_payload.clone(),
            },
            RelaySendCause::AckGapRepair,
            mux_limits,
            false,
        ));

        sender.record_owner_progress_for_test(startup_floor.saturating_mul(100));
        assert!(sender.enqueue_repair_frame_with_priority(
            &mut sender_queue,
            Frame::StreamData {
                stream_id,
                offset: (startup_floor * 2) as u64,
                flags: StreamFlags::NONE,
                payload: repair_payload,
            },
            RelaySendCause::PathFailureRepair,
            mux_limits,
            false,
        ));
    }

    #[test]
    fn client_critical_repair_closes_tail_after_optional_budget_exhaustion() {
        let mux_limits = MuxLimits::default();
        let stream_id = StreamId(95);
        let mut sender = RelaySenderService::new_with_performance(
            stream_id,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 1,
            },
        );
        let mut sender_queue = ReliableRelaySenderQueue::default();
        let startup_floor = response_extra_traffic_startup_floor_bytes(mux_limits);
        let frame = Frame::StreamData {
            stream_id,
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![0x33; startup_floor]),
        };
        assert!(sender.enqueue_repair_frame_with_priority(
            &mut sender_queue,
            frame,
            RelaySendCause::AckGapRepair,
            mux_limits,
            false,
        ));

        let closure_frame = Frame::StreamData {
            stream_id,
            offset: startup_floor as u64,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(b"tail"),
        };
        assert!(!sender.enqueue_repair_frame_with_priority(
            &mut sender_queue,
            closure_frame.clone(),
            RelaySendCause::AckGapRepair,
            mux_limits,
            false,
        ));

        sender.enqueue_critical_repair_frame(
            &mut sender_queue,
            closure_frame,
            RelaySendCause::AckGapRepair,
        );
        assert_eq!(sender.extra_traffic_budget_remaining(mux_limits), 0);
    }

    #[test]
    fn client_critical_tail_repair_is_idempotent_while_range_is_queued() {
        let mux_limits = MuxLimits::default();
        let stream_id = StreamId(97);
        let mut sender = RelaySenderService::new_with_performance(
            stream_id,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 1,
            },
        );
        let mut sender_queue = ReliableRelaySenderQueue::default();
        let first = Frame::StreamData {
            stream_id,
            offset: 128,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(&[0x55; 64]),
        };
        let duplicate = first.clone();

        assert!(sender.enqueue_critical_tail_repair_frame(&mut sender_queue, first));
        let bytes_after_first = sender_queue.bytes();
        let budget_after_first = sender.extra_traffic_budget_remaining(mux_limits);

        assert!(
            !sender.enqueue_critical_tail_repair_frame(&mut sender_queue, duplicate),
            "client final-tail RepairData must not stack duplicate pending ranges"
        );
        assert_eq!(sender_queue.bytes(), bytes_after_first);
        assert_eq!(
            sender.extra_traffic_budget_remaining(mux_limits),
            budget_after_first
        );
    }

    #[test]
    fn response_lead_must_be_admissible_not_lowest_raw_eta() {
        let mux_limits = MuxLimits::default();
        let mut saturated_low_eta =
            response_target(0, UnderlayProtocol::Udp, 1.0, 512 * 1024, 512 * 1024, true);
        saturated_low_eta.snapshot.product_bytes_in_flight =
            mux_limits.max_path_flight_bytes as u64;
        let admissible_higher_eta =
            response_target(1, UnderlayProtocol::Udp, 2.0, 0, 512 * 1024, false);
        let selected = choose_response_sender_target(
            &[saturated_low_eta, admissible_higher_eta.clone()],
            FlowLane::Throughput,
            &Frame::StreamData {
                stream_id: StreamId(7),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from(vec![0; 64 * 1024]),
            },
            ResponseCarrierEmitMode::Classified,
            mux_limits,
            &[],
            &[],
            None,
        )
        .expect("admissible higher ETA path should lead");

        assert_eq!(selected.key, admissible_higher_eta.key);
    }

    #[test]
    fn response_stream_ordered_final_control_stays_on_active_lead() {
        let active_data_owner =
            response_target(0, UnderlayProtocol::Tcp, 50.0, 0, 512 * 1024, true);
        let validation_lower_eta =
            response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 512 * 1024, false);

        let selected = choose_response_sender_target(
            &[active_data_owner.clone(), validation_lower_eta],
            FlowLane::Throughput,
            &Frame::StreamFin {
                stream_id: StreamId(7),
                final_offset: 2 * 1024 * 1024,
            },
            ResponseCarrierEmitMode::StreamOrdered,
            MuxLimits::default(),
            &[],
            &[],
            None,
        )
        .expect("stream-ordered final control should remain dispatchable");

        assert_eq!(
            selected.key, active_data_owner.key,
            "FIN/final-offset must not move to a validation path and overtake older data"
        );
    }

    #[test]
    fn response_stream_ack_prefers_request_active_over_response_owner() {
        let mut request_active =
            response_target(1, UnderlayProtocol::Tcp, 50.0, 0, 512 * 1024, false);
        request_active.is_request_active = true;
        let mut response_owner =
            response_target(0, UnderlayProtocol::Udp, 5.0, 0, 512 * 1024, true);
        response_owner.is_request_active = false;
        let selected = choose_response_sender_target(
            &[response_owner, request_active.clone()],
            FlowLane::Control,
            &Frame::StreamAck {
                stream_id: StreamId(7),
                complete: true,
                ranges: vec![OffsetRange { start: 0, end: 64 }],
            },
            ResponseCarrierEmitMode::Classified,
            MuxLimits::default(),
            &[],
            &[],
            None,
        )
        .expect("request Active ACK carrier should remain dispatchable");

        assert_eq!(selected.key, request_active.key);
    }

    #[test]
    fn response_stream_ordered_final_control_waits_for_backpressured_active_lead() {
        let (active_commands, _active_receivers) = reliable_path_command_channels(1);
        active_commands
            .try_enqueue_admitted_frame(
                Frame::StreamData {
                    stream_id: StreamId(7),
                    offset: 0,
                    flags: StreamFlags::NONE,
                    payload: Bytes::from_static(b"queued"),
                },
                FlowLane::Throughput,
            )
            .expect("fill active data queue");
        let mut active_data_owner =
            response_target(0, UnderlayProtocol::Tcp, 50.0, 0, 512 * 1024, true);
        active_data_owner.commands = active_commands;
        let validation_lower_eta =
            response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 512 * 1024, false);

        let selected = choose_response_sender_target(
            &[active_data_owner, validation_lower_eta],
            FlowLane::Throughput,
            &Frame::StreamFin {
                stream_id: StreamId(7),
                final_offset: 2 * 1024 * 1024,
            },
            ResponseCarrierEmitMode::StreamOrdered,
            MuxLimits::default(),
            &[],
            &[],
            None,
        );

        assert!(
            selected.is_none(),
            "stream-ordered FIN must wait behind older active-owner data instead of escaping to validation output"
        );
    }

    #[test]
    fn single_active_response_target_still_obeys_bulk_admission() {
        let mux_limits = MuxLimits::default();
        let mut saturated =
            response_target(0, UnderlayProtocol::Udp, 1.0, 512 * 1024, 512 * 1024, true);
        saturated.snapshot.product_bytes_in_flight = mux_limits.max_path_flight_bytes as u64;
        let candidates = [&saturated];
        let outcome = response_target_unique_owner_admission_with_epoch(
            &saturated,
            &candidates,
            ResponseBulkLead {
                key: saturated.key,
                snapshot: saturated.snapshot,
                eta_ms: saturated.eta_ms,
            },
            None,
            Some(saturated.key),
            0,
            ResponseOrderedTail::new(Some(saturated.key), 0).for_candidate(saturated.key),
            64 * 1024,
            mux_limits,
            None,
            true,
            false,
        );
        assert_eq!(outcome.admission.decision, PathAdmissionDecision::Standby);
        assert_eq!(outcome.model_suppression, Some("inflight_limit"));

        let selected = choose_response_sender_data_target(
            &[saturated],
            FlowLane::Throughput,
            64 * 1024,
            mux_limits,
            &[],
            None,
        );

        assert!(
            selected.is_none(),
            "a temporarily single attached output must not bypass product/carrier flight admission"
        );
    }

    #[test]
    fn response_data_admission_uses_writer_pending_bytes_not_only_slots() {
        let mux_limits = MuxLimits {
            max_path_flight_bytes: 512 * 1024,
            max_repair_bytes: 512 * 1024,
            max_reorder_bytes: 512 * 1024,
            max_stream_window_bytes: 512 * 1024,
            ..MuxLimits::default()
        };
        let payload_bytes = 8 * 1024;
        let (commands, _receivers) = reliable_path_command_channels(2048);
        let mut snapshot = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 1.0, 8_000_000.0);
        snapshot.confidence = 1.0;
        let saturated = ResponseSenderPathTarget {
            #[cfg(feature = "lab-diagnostics")]
            session_id: SessionId(0),
            #[cfg(feature = "lab-diagnostics")]
            binding_instance_id: 0,
            key: CarrierPathKey {
                underlay: UnderlayProtocol::Udp,
                path_id: PathId(0),
            },
            incarnation: 1,
            commands,
            attachment_role: StreamOpenRole::Active,
            snapshot,
            owner_data_in_flight_bytes: 0,
            command_pending_bytes: 0,
            eta_ms: 1.0,
            is_active: true,
            is_request_active: true,
            has_sender_evidence: true,
            has_bulk_rate_evidence: true,
            ack_clock_calibration_eligible: false,
            ack_clock_calibration_proven: false,
            ack_clock_calibration_spent_bytes: 0,
            ack_clock_calibration_credit_limit_bytes: 0,
            ack_clock_calibration_max_limit_bytes: 0,
            ack_clock_calibration_active: false,
        };
        let credit = response_target_emission_credit_bytes(
            &saturated,
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
        );
        while saturated.commands.pending_bytes() + payload_bytes as u64 <= credit as u64 {
            saturated
                .commands
                .try_enqueue_admitted_frame(
                    Frame::StreamData {
                        stream_id: StreamId(7),
                        offset: saturated.commands.pending_bytes(),
                        flags: StreamFlags::NONE,
                        payload: Bytes::from(vec![0; payload_bytes]),
                    },
                    FlowLane::Throughput,
                )
                .expect("prefill data pipe");
        }

        let admissible = response_target(1, UnderlayProtocol::Udp, 2.0, 0, 512 * 1024, false);
        let selected = choose_response_sender_data_target(
            &[saturated.clone(), admissible.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            None,
        )
        .expect("higher-ETA target with writer credit should be selected");

        assert_eq!(selected.key, admissible.key);
        assert!(
            saturated
                .commands
                .pending_bytes()
                .saturating_add(payload_bytes as u64)
                > credit as u64,
            "test must fill the low-ETA writer pipe until the next data frame would exceed byte credit"
        );
    }

    #[test]
    fn active_quic_response_owner_emission_credit_uses_product_envelope_not_carrier_cwnd() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut active =
            response_target(0, UnderlayProtocol::Udp, 5.0, 0, payload_bytes as u64, true);
        active.snapshot.inflight_limit_bytes = payload_bytes as u64;

        let credit = response_target_emission_credit_bytes(
            &active,
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
        );

        assert_eq!(
            credit,
            bulk_active_service_product_envelope_bytes(active.snapshot, payload_bytes, mux_limits)
                as usize,
            "active response owner must use the product envelope, not current carrier cwnd"
        );
        assert!(
            credit > payload_bytes,
            "the regression requires credit above one carrier quantum"
        );
    }

    #[test]
    fn active_tcp_response_owner_without_bulk_evidence_uses_startup_credit_not_full_envelope() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut active = response_target(0, UnderlayProtocol::Tcp, 80.0, 0, 16 * 1024 * 1024, true);
        active.has_sender_evidence = false;
        active.has_bulk_rate_evidence = false;

        let credit = response_target_emission_credit_bytes(
            &active,
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
        );

        assert_eq!(
            credit,
            bulk_service_horizon_payload_bytes(payload_bytes, mux_limits),
            "unproven active Service startup must be bounded until path-scoped bulk-rate evidence exists"
        );
        assert!(
            credit >= payload_bytes,
            "startup Service credit must still admit at least one bulk quantum"
        );

        active.snapshot.product_bytes_in_flight = credit as u64;
        assert!(
            !response_service_has_assigned_owner_credit(
                &active,
                FlowLane::Throughput,
                payload_bytes,
                mux_limits,
            ),
            "startup credit bounds cumulative assigned flight, not only the draining writer queue"
        );
    }

    #[test]
    fn response_quic_feed_credit_uses_live_carrier_debt_not_outdated_bdp() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = 64 * 1024usize;
        let mut loaded_quic = response_target(0, UnderlayProtocol::Udp, 250.0, 0, 64 * 1024, true);
        loaded_quic.snapshot.delivery_rate_bps = 351_000.0;
        loaded_quic.snapshot.pacing_rate_bps = 351_000.0;
        loaded_quic.snapshot.product_progress_rate_bps = Some(10_000_000.0);
        loaded_quic.snapshot.bytes_in_flight = 8 * 1024 * 1024;
        loaded_quic.snapshot.queue_bytes = 1024 * 1024;

        let quic_credit = response_target_emission_credit_bytes(
            &loaded_quic,
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
        );
        let outdated_bdp_credit = adaptive_reliable_relay_inflight_bytes(
            Some(loaded_quic.snapshot),
            FlowLane::Throughput,
            mux_limits,
        );

        assert_eq!(
            quic_credit,
            bulk_active_service_product_envelope_bytes(
                loaded_quic.snapshot,
                payload_bytes,
                mux_limits,
            ) as usize,
            "active QUIC Service feed credit must follow the product envelope, not live carrier debt"
        );
        assert!(
            quic_credit > outdated_bdp_credit,
            "app-limited BDP must not be the only active QUIC Service writer-feed ceiling"
        );

        let mut loaded_tcp = response_target(1, UnderlayProtocol::Tcp, 250.0, 0, 64 * 1024, true);
        loaded_tcp.snapshot.delivery_rate_bps = 351_000.0;
        loaded_tcp.snapshot.pacing_rate_bps = 351_000.0;
        loaded_tcp.snapshot.bytes_in_flight = 8 * 1024 * 1024;
        loaded_tcp.snapshot.queue_bytes = 1024 * 1024;
        loaded_tcp.snapshot.product_progress_rate_bps = Some(351_000.0);
        let tcp_credit = response_target_emission_credit_bytes(
            &loaded_tcp,
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
        );

        assert_eq!(
            tcp_credit,
            bulk_active_service_product_envelope_bytes(
                loaded_tcp.snapshot,
                payload_bytes,
                mux_limits,
            ) as usize,
            "active TCP owners use the same carrier-neutral product envelope as active QUIC owners"
        );

        let mut subflow_quic =
            response_target(2, UnderlayProtocol::Udp, 250.0, 0, 64 * 1024, false);
        subflow_quic.snapshot.delivery_rate_bps = 351_000.0;
        subflow_quic.snapshot.pacing_rate_bps = 351_000.0;
        subflow_quic.snapshot.bytes_in_flight = 8 * 1024 * 1024;
        subflow_quic.snapshot.queue_bytes = 1024 * 1024;
        let subflow_credit = response_target_emission_credit_bytes(
            &subflow_quic,
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
        );

        assert!(
            subflow_credit >= 8 * 1024 * 1024,
            "Subflow QUIC paths remain carrier-debt gated rather than borrowing the active owner envelope"
        );
    }

    #[test]
    fn quic_proof_success_path_gets_bounded_bulk_only_startup_sampling() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let active = response_target(
            0,
            UnderlayProtocol::Udp,
            1.0,
            0,
            4 * payload_bytes as u64,
            true,
        );
        let mut proof_success = response_target(1, UnderlayProtocol::Udp, 500.0, 0, 0, false);
        proof_success.snapshot.delivery_rate_bps = default_path_rate_bps(UnderlayProtocol::Udp);
        proof_success.snapshot.pacing_rate_bps = proof_success.snapshot.delivery_rate_bps;
        proof_success.snapshot.app_limited = true;
        proof_success.snapshot.confidence = 1.0;
        proof_success.has_bulk_rate_evidence = false;

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[active.clone(), proof_success.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(active.key),
            0,
            None,
        )
        .expect("QUIC Validation sampling should be dispatchable");

        assert_eq!(selected.target.key, proof_success.key);
        assert_eq!(selected.admission.role, PathRuntimeRole::Subflow);
        assert!(
            selected
                .subflow_set_commit
                .is_some_and(|commit| commit.input.startup_owner_allowed)
        );
    }

    #[test]
    fn proof_path_owner_sampling_is_explicit_subflow_not_service_migration() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let active = response_target(
            0,
            UnderlayProtocol::Udp,
            1.0,
            0,
            4 * payload_bytes as u64,
            true,
        );
        let mut proof_success = response_target(1, UnderlayProtocol::Udp, 500.0, 0, 0, false);
        proof_success.has_sender_evidence = true;
        proof_success.has_bulk_rate_evidence = false;

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[active.clone(), proof_success],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(active.key),
            0,
            None,
        )
        .expect("bounded startup sampling should be dispatchable");

        assert_ne!(selected.target.key, active.key);
        assert_eq!(selected.admission.role, PathRuntimeRole::Subflow);
        assert!(
            selected
                .subflow_set_commit
                .is_some_and(|commit| commit.input.startup_owner_allowed)
        );
    }

    #[test]
    fn measured_udp_bulk_path_remains_overflow_behind_feedable_udp_service() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let active_udp = response_target(
            0,
            UnderlayProtocol::Udp,
            150.0,
            0,
            4 * payload_bytes as u64,
            true,
        );
        let measured_udp = response_target(
            1,
            UnderlayProtocol::Udp,
            10.0,
            0,
            4 * payload_bytes as u64,
            false,
        );

        let selected = choose_response_sender_data_target(
            &[active_udp.clone(), measured_udp],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(CarrierPathKey {
                underlay: UnderlayProtocol::Udp,
                path_id: PathId(0),
            }),
        )
        .expect("the feedable UDP Service should remain eligible for ordinary bulk");

        assert_eq!(
            selected.key, active_udp.key,
            "a measured same-family Subflow is additive overflow and must not displace feedable Service"
        );
    }

    #[test]
    fn measured_udp_bulk_path_does_not_steal_tcp_owner_under_lower_debt() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let active_tcp = response_target(
            0,
            UnderlayProtocol::Tcp,
            150.0,
            0,
            4 * payload_bytes as u64,
            true,
        );
        let measured_udp = response_target(
            1,
            UnderlayProtocol::Udp,
            10.0,
            0,
            4 * payload_bytes as u64,
            false,
        );

        let selected = choose_response_sender_data_target(
            &[active_tcp.clone(), measured_udp],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[CarrierPathFlightDebt {
                key: active_tcp.key,
                bytes: payload_bytes as u64,
            }],
            Some(active_tcp.key),
        )
        .expect("current TCP primary remains eligible while it owns unresolved lower bytes");

        assert_eq!(
            selected.key, active_tcp.key,
            "mixed TCP/QUIC paths may probe or repair, but must not steal same-stream OwnerData under lower-owner debt"
        );
    }

    #[test]
    fn active_tcp_response_owner_uses_product_envelope() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut target = response_target(
            0,
            UnderlayProtocol::Tcp,
            50.0,
            0,
            payload_bytes as u64,
            true,
        );
        target.snapshot.product_progress_rate_bps = Some(10_000_000_000.0);

        assert_eq!(
            response_target_emission_credit_bytes(
                &target,
                FlowLane::Throughput,
                payload_bytes,
                mux_limits
            ),
            bulk_active_service_product_envelope_bytes(target.snapshot, payload_bytes, mux_limits)
                as usize,
            "active TCP and QUIC owners should use the same product envelope; transport pacing belongs below the sender service"
        );
    }

    #[test]
    fn measured_udp_alternate_does_not_replace_active_service_at_clear_frontier() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut active_unproven_udp = response_target(
            0,
            UnderlayProtocol::Udp,
            5.0,
            0,
            4 * payload_bytes as u64,
            true,
        );
        active_unproven_udp.has_bulk_rate_evidence = false;
        let measured_udp = response_target(
            1,
            UnderlayProtocol::Udp,
            10.0,
            0,
            4 * payload_bytes as u64,
            false,
        );

        let selected = choose_response_sender_data_target(
            &[active_unproven_udp, measured_udp.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(CarrierPathKey {
                underlay: UnderlayProtocol::Udp,
                path_id: PathId(0),
            }),
        )
        .expect("bulk-rate-proven UDP owner should be eligible at a clear frontier");

        assert_eq!(
            selected.key,
            CarrierPathKey {
                underlay: UnderlayProtocol::Udp,
                path_id: PathId(0),
            },
            "a measured alternate must not steal Service ownership merely by existing"
        );
    }

    #[test]
    fn proof_only_fallback_lead_cannot_become_response_service_owner() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut proof_only = response_target(
            1,
            UnderlayProtocol::Udp,
            5.0,
            0,
            4 * payload_bytes as u64,
            false,
        );
        proof_only.has_sender_evidence = true;
        proof_only.has_bulk_rate_evidence = false;
        let lead = ResponseBulkLead {
            key: proof_only.key,
            snapshot: proof_only.snapshot,
            eta_ms: proof_only.eta_ms,
        };

        let admission = response_target_unique_owner_admission(
            &proof_only,
            &[&proof_only],
            lead,
            None,
            0,
            payload_bytes,
            mux_limits,
        );

        assert_eq!(
            admission.decision,
            PathAdmissionDecision::ProbeOnly,
            "sender/proof evidence is not Service ownership; only an active anchor or bulk-rate-proven failover may own the Service role"
        );
        assert_eq!(admission.role, PathRuntimeRole::Probe);
    }

    #[test]
    fn clear_frontier_without_live_service_elects_liveness_service_failover() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut restart = response_target(
            1,
            UnderlayProtocol::Tcp,
            5.0,
            0,
            4 * payload_bytes as u64,
            false,
        );
        restart.has_sender_evidence = false;
        restart.has_bulk_rate_evidence = false;

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[restart.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            None,
            0,
            None,
        );

        let selected = selected.expect(
            "when the previous Service is gone and the ordered frontier is clear, the stream must elect a new Service failover path",
        );
        assert_eq!(
            selected.target.key, restart.key,
            "liveness from an attached output is enough for bounded Service failover only when no live Service owner remains"
        );
        assert_eq!(
            selected.admission.role,
            PathRuntimeRole::Service,
            "failover owner bytes are Service OwnerData, not optional Subflow exploration"
        );
    }

    #[test]
    fn repair_attachment_cannot_suppress_liveness_service_failover() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut repair = response_target(
            0,
            UnderlayProtocol::Tcp,
            1.0,
            0,
            4 * payload_bytes as u64,
            false,
        );
        repair.attachment_role = StreamOpenRole::Repair;
        let mut validation = response_target(
            1,
            UnderlayProtocol::Tcp,
            50.0,
            0,
            4 * payload_bytes as u64,
            false,
        );
        validation.has_sender_evidence = false;
        validation.has_bulk_rate_evidence = false;

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[repair, validation.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            None,
            0,
            None,
        )
        .expect("Repair output must not hide an eligible liveness Service survivor");

        assert_eq!(selected.target.key, validation.key);
        assert_eq!(selected.admission.role, PathRuntimeRole::Service);
    }

    #[test]
    fn unproven_liveness_service_failover_respects_startup_assigned_credit() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let startup_credit =
            response_service_startup_emission_credit_bytes(payload_bytes, mux_limits);
        let mut failover = response_target(
            1,
            UnderlayProtocol::Tcp,
            5.0,
            0,
            4 * payload_bytes as u64,
            false,
        );
        failover.has_bulk_rate_evidence = false;
        failover.snapshot.product_bytes_in_flight =
            startup_credit.saturating_sub(payload_bytes) as u64;

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[failover.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            None,
            0,
            None,
        )
        .expect("a prospective Service with startup credit remaining stays feedable");
        assert_eq!(selected.admission.role, PathRuntimeRole::Service);

        failover.snapshot.product_bytes_in_flight = startup_credit as u64;
        assert!(
            select_response_sender_data_target_with_ordered_debt_and_epoch(
                &[failover],
                FlowLane::Throughput,
                payload_bytes,
                mux_limits,
                &[],
                None,
                0,
                None,
            )
            .is_none(),
            "newly elected unproven Service must not exceed the cumulative startup horizon before becoming active"
        );
    }

    #[test]
    fn prospective_service_uses_service_credit_instead_of_optional_pipe_credit() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let (commands, _receivers) = reliable_path_command_channels(128);
        let mut failover = response_target(
            1,
            UnderlayProtocol::Tcp,
            5.0,
            0,
            payload_bytes as u64,
            false,
        );
        failover.commands = commands;
        failover.has_bulk_rate_evidence = false;
        failover.snapshot.delivery_rate_bps = 1.0;
        failover.snapshot.pacing_rate_bps = 1.0;
        let optional_credit = response_target_emission_credit_bytes(
            &failover,
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
        );
        let service_credit =
            response_service_emission_credit_bytes(&failover, payload_bytes, mux_limits);
        assert!(
            optional_credit < service_credit,
            "fixture requires optional-path credit below prospective Service credit"
        );
        while failover
            .commands
            .pending_bytes()
            .saturating_add(payload_bytes as u64)
            <= optional_credit as u64
        {
            failover
                .commands
                .try_enqueue_admitted_frame(
                    Frame::StreamData {
                        stream_id: StreamId(74),
                        offset: failover.commands.pending_bytes(),
                        flags: StreamFlags::NONE,
                        payload: Bytes::from(vec![0; payload_bytes]),
                    },
                    FlowLane::Throughput,
                )
                .expect("prefill prospective Service without exhausting queue slots");
        }
        assert!(
            failover.commands.can_enqueue_lane_now(FlowLane::Throughput),
            "fixture must retain a real writer queue slot"
        );
        assert!(
            !response_target_has_emission_credit(
                &failover,
                FlowLane::Throughput,
                payload_bytes,
                mux_limits,
            ),
            "fixture must exceed the optional-path pipe credit"
        );
        assert!(
            response_service_has_assigned_owner_credit(
                &failover,
                FlowLane::Throughput,
                payload_bytes,
                mux_limits,
            ),
            "the same assigned queue remains inside prospective Service credit"
        );

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[failover],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            None,
            0,
            None,
        )
        .expect("pre-role optional-path credit must not suppress Service failover");
        assert_eq!(selected.admission.role, PathRuntimeRole::Service);
    }

    #[test]
    fn mature_liveness_service_failover_uses_product_envelope() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut failover = response_target(
            1,
            UnderlayProtocol::Tcp,
            5.0,
            0,
            4 * payload_bytes as u64,
            false,
        );
        let mature_credit =
            response_service_emission_credit_bytes(&failover, payload_bytes, mux_limits);
        let full_envelope = usize::try_from(bulk_active_service_product_envelope_bytes(
            failover.snapshot,
            payload_bytes,
            mux_limits,
        ))
        .unwrap();
        assert!(
            mature_credit
                > response_service_startup_emission_credit_bytes(payload_bytes, mux_limits),
            "fixture requires a mature product envelope larger than startup credit"
        );
        assert_eq!(mature_credit, full_envelope);
        failover.snapshot.product_bytes_in_flight =
            mature_credit.saturating_sub(payload_bytes) as u64;

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[failover.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            None,
            0,
            None,
        )
        .expect("bulk-rate-proven prospective Service may use the product envelope");
        assert_eq!(selected.admission.role, PathRuntimeRole::Service);

        failover.snapshot.product_bytes_in_flight = mature_credit as u64;
        assert!(
            select_response_sender_data_target_with_ordered_debt_and_epoch(
                &[failover],
                FlowLane::Throughput,
                payload_bytes,
                mux_limits,
                &[],
                None,
                0,
                None,
            )
            .is_none(),
            "mature Service failover must stop at the product envelope"
        );
    }

    #[test]
    fn mixed_family_clear_frontier_service_failover_is_metric_first() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut tcp = response_target(
            1,
            UnderlayProtocol::Tcp,
            50.0,
            0,
            4 * payload_bytes as u64,
            false,
        );
        tcp.has_sender_evidence = true;
        tcp.has_bulk_rate_evidence = false;
        let mut udp = response_target(
            0,
            UnderlayProtocol::Udp,
            5.0,
            0,
            4 * payload_bytes as u64,
            false,
        );
        udp.has_sender_evidence = true;
        udp.has_bulk_rate_evidence = false;

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[tcp, udp.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            None,
            0,
            None,
        );

        let selected = selected
            .expect("Service failover must be carrier-neutral when no live ordered owner remains");
        assert_eq!(
            selected.target.key, udp.key,
            "clear-frontier Service failover is selected by path metrics, not by TCP/UDP family"
        );
        assert_eq!(
            selected.admission.role,
            PathRuntimeRole::Service,
            "the elected failover path becomes the new Service owner"
        );
    }

    #[test]
    fn clear_frontier_stale_owner_without_lane_capacity_elects_liveness_service_failover() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut stale_owner =
            response_target(1, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, true);
        let (owner_commands, _owner_rx) = reliable_path_command_channels(1);
        owner_commands
            .try_enqueue_admitted_frame(
                Frame::StreamData {
                    stream_id: StreamId(99),
                    offset: 0,
                    flags: StreamFlags::NONE,
                    payload: Bytes::from_static(b"queued"),
                },
                FlowLane::Throughput,
            )
            .expect("seed full owner data queue");
        stale_owner.commands = owner_commands;
        let mut failover = response_target(
            0,
            UnderlayProtocol::Udp,
            5.0,
            0,
            4 * payload_bytes as u64,
            false,
        );
        failover.has_sender_evidence = true;
        failover.has_bulk_rate_evidence = false;

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[stale_owner.clone(), failover.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(stale_owner.key),
            0,
            None,
        );

        let selected = selected.expect(
            "when the ordered frontier is clear and the old Service cannot enqueue, a validated survivor must become Service failover",
        );
        assert_eq!(
            selected.target.key, failover.key,
            "clear-frontier failover is metric-first and must not be trapped by the stale owner's carrier family"
        );
        assert_eq!(selected.admission.role, PathRuntimeRole::Service);
    }

    #[test]
    fn liveness_service_failover_waits_behind_live_owner_tail_guard() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut failover = response_target(
            1,
            UnderlayProtocol::Udp,
            5.0,
            0,
            4 * payload_bytes as u64,
            false,
        );
        failover.has_sender_evidence = true;
        failover.has_bulk_rate_evidence = false;

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[failover],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            None,
            payload_bytes,
            None,
        );

        assert!(
            selected.is_none(),
            "liveness Service failover can only own future bytes after the live lower owner frontier is clear"
        );
    }

    #[test]
    fn repair_prefers_bulk_proven_path_over_proof_only_low_eta_path() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let original_owner = response_target(
            0,
            UnderlayProtocol::Tcp,
            20.0,
            0,
            4 * payload_bytes as u64,
            true,
        );
        let proven_alternate = response_target(
            1,
            UnderlayProtocol::Tcp,
            50.0,
            0,
            4 * payload_bytes as u64,
            false,
        );
        let mut proof_only_udp = response_target(
            2,
            UnderlayProtocol::Udp,
            5.0,
            0,
            4 * payload_bytes as u64,
            false,
        );
        proof_only_udp.has_bulk_rate_evidence = false;

        let selected = choose_response_sender_target(
            &[
                original_owner.clone(),
                proven_alternate.clone(),
                proof_only_udp,
            ],
            FlowLane::Latency,
            &Frame::StreamData {
                stream_id: StreamId(7),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from(vec![0; payload_bytes]),
            },
            ResponseCarrierEmitMode::Classified,
            mux_limits,
            &[],
            &[original_owner.key],
            Some(RelaySendCause::AckGapRepair),
        )
        .expect("repair should remain dispatchable on the proven alternate");

        assert_eq!(
            selected.key, proven_alternate.key,
            "repair must not treat proof-only validation as bulk-capable just because it has lower ETA"
        );
    }

    #[test]
    fn repair_does_not_use_proof_only_path_when_no_proven_repair_path_exists() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let original_owner = response_target(
            0,
            UnderlayProtocol::Tcp,
            20.0,
            0,
            4 * payload_bytes as u64,
            true,
        );
        let mut proof_only_udp = response_target(
            1,
            UnderlayProtocol::Udp,
            5.0,
            0,
            4 * payload_bytes as u64,
            false,
        );
        proof_only_udp.has_bulk_rate_evidence = false;

        let selected = choose_response_sender_target(
            &[original_owner.clone(), proof_only_udp.clone()],
            FlowLane::Latency,
            &Frame::StreamData {
                stream_id: StreamId(7),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from(vec![0; payload_bytes]),
            },
            ResponseCarrierEmitMode::Classified,
            mux_limits,
            &[],
            &[original_owner.key],
            Some(RelaySendCause::AckGapRepair),
        );

        assert!(
            selected.is_none(),
            "RepairData must wait for an active or bulk-rate-proven alternate instead of turning proof-only validation into a repair path"
        );
    }

    #[test]
    fn path_failure_repair_can_use_live_liveness_survivor_without_path_proving_it() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let original_owner = response_target(
            0,
            UnderlayProtocol::Tcp,
            20.0,
            0,
            4 * payload_bytes as u64,
            true,
        );
        let mut liveness_survivor = response_target(
            1,
            UnderlayProtocol::Udp,
            5.0,
            0,
            4 * payload_bytes as u64,
            false,
        );
        liveness_survivor.has_sender_evidence = true;
        liveness_survivor.has_bulk_rate_evidence = false;

        let selected = choose_response_sender_target(
            &[original_owner.clone(), liveness_survivor.clone()],
            FlowLane::Latency,
            &Frame::StreamData {
                stream_id: StreamId(7),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from(vec![0; payload_bytes]),
            },
            ResponseCarrierEmitMode::Classified,
            mux_limits,
            &[],
            &[original_owner.key],
            Some(RelaySendCause::PathFailureRepair),
        )
        .expect("path-failure repair must be able to recover on a live non-owner output");

        assert_eq!(
            selected.key, liveness_survivor.key,
            "PathFailureRepair is bounded failover retransmission; it must not require bulk-rate proof because it never path-proves or changes Service ownership"
        );
    }

    #[test]
    fn path_failure_repair_prefers_same_family_survivor_before_cross_family_low_eta() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let original_owner = response_target(
            0,
            UnderlayProtocol::Tcp,
            20.0,
            0,
            4 * payload_bytes as u64,
            true,
        );
        let mut same_family_survivor = response_target(
            1,
            UnderlayProtocol::Tcp,
            50.0,
            0,
            4 * payload_bytes as u64,
            false,
        );
        same_family_survivor.has_sender_evidence = true;
        same_family_survivor.has_bulk_rate_evidence = false;
        let mut cross_family_low_eta = response_target(
            2,
            UnderlayProtocol::Udp,
            5.0,
            0,
            4 * payload_bytes as u64,
            false,
        );
        cross_family_low_eta.has_sender_evidence = true;
        cross_family_low_eta.has_bulk_rate_evidence = false;

        let selected = choose_response_sender_target(
            &[
                original_owner.clone(),
                same_family_survivor.clone(),
                cross_family_low_eta,
            ],
            FlowLane::Throughput,
            &Frame::StreamData {
                stream_id: StreamId(7),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from(vec![0; payload_bytes]),
            },
            ResponseCarrierEmitMode::Classified,
            mux_limits,
            &[],
            &[original_owner.key],
            Some(RelaySendCause::PathFailureRepair),
        )
        .expect("path-failure repair should remain dispatchable on a live survivor");

        assert_eq!(
            selected.key, same_family_survivor.key,
            "failed-owner RepairData should follow the same-family failover survivor before trying cross-family low-ETA repair"
        );
    }

    #[test]
    fn path_failure_repair_bypasses_stale_owner_emission_credit_but_not_queue_capacity() {
        let mux_limits = MuxLimits {
            max_path_flight_bytes: 64 * 1024,
            max_repair_bytes: 64 * 1024,
            max_reorder_bytes: 64 * 1024,
            max_stream_window_bytes: 64 * 1024,
            ..MuxLimits::default()
        };
        let payload_bytes = 8 * 1024;
        let (commands, _receivers) = reliable_path_command_channels(64);
        let mut survivor = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024, false);
        survivor.commands = commands.clone();
        survivor.has_sender_evidence = true;
        survivor.has_bulk_rate_evidence = false;

        let credit = response_target_emission_credit_bytes(
            &survivor,
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
        );
        while commands
            .pending_bytes()
            .saturating_add(payload_bytes as u64)
            <= credit as u64
        {
            commands
                .try_enqueue_admitted_frame(
                    Frame::StreamData {
                        stream_id: StreamId(72),
                        offset: commands.pending_bytes(),
                        flags: StreamFlags::NONE,
                        payload: Bytes::from(vec![0; payload_bytes]),
                    },
                    FlowLane::Throughput,
                )
                .expect("prefill survivor data queue without exhausting slots");
        }

        let repair_frame = Frame::StreamData {
            stream_id: StreamId(72),
            offset: 1024,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![7_u8; payload_bytes]),
        };
        assert!(
            survivor
                .commands
                .can_enqueue_frame_now(&repair_frame, FlowLane::Throughput),
            "test setup must leave a real queue slot for failover RepairData"
        );
        assert!(
            !response_target_has_emission_credit(
                &survivor,
                FlowLane::Throughput,
                payload_bytes,
                mux_limits,
            ),
            "test setup must exceed ordinary owner emission credit"
        );

        let selected = choose_response_sender_target(
            &[survivor.clone()],
            FlowLane::Throughput,
            &repair_frame,
            ResponseCarrierEmitMode::Classified,
            mux_limits,
            &[],
            &[],
            Some(RelaySendCause::PathFailureRepair),
        )
        .expect("path-failure RepairData must be admitted while a live queue slot exists");

        assert_eq!(
            selected.key, survivor.key,
            "failed-owner repair is bounded correctness traffic and must not be blocked by stale owner emission credit"
        );
    }

    #[test]
    fn path_failure_repair_stream_data_uses_data_queue_when_priority_is_full() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let stream_id = StreamId(71);
        let repair_frame = Frame::StreamData {
            stream_id,
            offset: 1024,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![7_u8; payload_bytes]),
        };
        let active_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let (active_commands, _active_rx) = reliable_path_command_channels(1);
        active_commands
            .try_enqueue_admitted_frame(
                Frame::StreamAck {
                    stream_id,
                    complete: false,
                    ranges: Vec::new(),
                },
                FlowLane::Control,
            )
            .expect("fill active priority queue");
        let binding = ResponseStreamBinding::new_with_limits(
            SessionId(71),
            active_key.underlay,
            active_key.path_id,
            active_commands,
            FlowLane::Throughput,
            mux_limits,
        );
        binding.record_owner_flight(active_key, &repair_frame);

        let (survivor_commands, _survivor_rx) = reliable_path_command_channels(1);
        assert_eq!(
            binding.attach(
                UnderlayProtocol::Udp,
                PathId(1),
                survivor_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                payload_bytes,
            ),
            ResponseStreamAttachOutcome::Attached
        );
        let (_frames_tx, frames_rx) = mpsc::channel(1);
        let path_stream = ReliablePathStream {
            stream_id,
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: UnderlayProtocol::Tcp,
            max_frame_payload_bytes: payload_bytes,
            output: ReliablePathStreamOutput::Switchable(binding),
            frames: frames_rx,
        };

        assert!(
            response_frame_has_carrier_credit(
                &path_stream,
                &repair_frame,
                FlowLane::Latency,
                ResponseCarrierEmitMode::Classified,
                Some(RelaySendCause::PathFailureRepair),
            ),
            "RepairData is product-critical stream data: carrier priority queues may be full, but an open stream-data queue must still admit failover repair"
        );
    }

    #[test]
    fn quic_ack_data_seen_path_does_not_own_unique_data_without_bulk_rate_proof() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let (active_commands, _active_rx) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new_with_limits(
            SessionId(81),
            UnderlayProtocol::Udp,
            PathId(0),
            active_commands,
            FlowLane::Throughput,
            mux_limits,
        );
        let validation_key = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let (validation_commands, _validation_rx) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                validation_key.underlay,
                validation_key.path_id,
                validation_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                payload_bytes,
            ),
            ResponseStreamAttachOutcome::Attached
        );
        binding.update_path_metrics(
            validation_key,
            PathMetrics {
                path_id: validation_key.path_id,
                underlay: validation_key.underlay,
                direction: PathMetricDirection::ServerToClient,
                metric_epoch: metric_epoch_now(),
                metric_age_us: 0,
                min_rtt_us: 5_000,
                srtt_us: 5_000,
                rttvar_us: 500,
                jitter_us: 500,
                delivery_rate_bps: 1_000_000,
                pacing_rate_bps: 1_000_000,
                loss_ppm: 0,
                ecn_ppm: 0,
                loss_observed: false,
                ecn_observed: false,
                bytes_in_flight: 0,
                queue_bytes: 0,
                inflight_limit_bytes: payload_bytes as u64,
                inflight_hi_bytes: payload_bytes as u64,
                confidence_ppm: 1,
                app_limited: true,
                has_ack_derived_data_sample: true,
                data_sample_count: 0,
                data_sample_bytes: 0,
            },
            ServerPathMetricsSource::LocalSender,
        );
        let (_frames_tx, frames_rx) = mpsc::channel(1);
        let stream = ReliablePathStream {
            stream_id: StreamId(7),
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: UnderlayProtocol::Udp,
            max_frame_payload_bytes: payload_bytes,
            output: ReliablePathStreamOutput::Switchable(binding),
            frames: frames_rx,
        };

        let plan = plan_response_data_dispatch(&stream, FlowLane::Throughput, 0, payload_bytes)
            .expect("active owner should remain dispatchable");

        assert_eq!(
            plan.primary_key(),
            Some(CarrierPathKey {
                underlay: UnderlayProtocol::Udp,
                path_id: PathId(0),
            }),
            "ACK-data evidence cannot create Subflow OwnerData before the candidate has bulk-rate evidence"
        );
        assert_eq!(
            plan.primary_role(),
            PathRuntimeRole::Service,
            "ACK-data-only paths must not become Service owners"
        );
    }

    #[test]
    fn ack_data_only_udp_path_cannot_own_unique_data_when_lower_owner_exists() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let active_key = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        };
        let mut active = response_target(
            0,
            UnderlayProtocol::Udp,
            50.0,
            payload_bytes as u64,
            4 * payload_bytes as u64,
            true,
        );
        active.has_bulk_rate_evidence = false;
        let mut ack_data_only_path = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 0, false);
        ack_data_only_path.has_bulk_rate_evidence = false;

        let selected = choose_response_sender_data_target(
            &[active.clone(), ack_data_only_path.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[CarrierPathFlightDebt {
                key: active_key,
                bytes: PATH_OPEN_SCORE_BYTES as u64,
            }],
            Some(active_key),
        )
        .expect("active owner should remain admissible while lower bytes are unresolved");

        assert_eq!(
            selected.key, active.key,
            "ACK-data-only QUIC paths must not own later ordered bytes while another path owns unresolved lower bytes"
        );
    }

    #[test]
    fn ack_data_quic_path_does_not_preempt_service_owner_under_lower_debt() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let active_key = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        };
        let mut active = response_target(
            0,
            UnderlayProtocol::Udp,
            5.0,
            payload_bytes as u64,
            16 * payload_bytes as u64,
            true,
        );
        active.has_bulk_rate_evidence = true;
        let mut ack_data_only_path = response_target(1, UnderlayProtocol::Udp, 500.0, 0, 0, false);
        ack_data_only_path.has_bulk_rate_evidence = false;
        ack_data_only_path.snapshot.delivery_rate_bps =
            default_path_rate_bps(UnderlayProtocol::Udp);
        ack_data_only_path.snapshot.pacing_rate_bps = ack_data_only_path.snapshot.delivery_rate_bps;
        ack_data_only_path.snapshot.app_limited = true;

        let selected = choose_response_sender_data_target(
            &[active.clone(), ack_data_only_path.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[CarrierPathFlightDebt {
                key: active_key,
                bytes: PATH_OPEN_SCORE_BYTES as u64,
            }],
            Some(active_key),
        )
        .expect("active owner should remain selected while it owns the lower frontier");

        assert_eq!(
            selected.key, active.key,
            "ACK-data-only paths must not preempt the service owner while lower-owner debt exists"
        );
    }

    #[test]
    fn quic_ack_data_seen_validation_path_bootstraps_as_bounded_subflow() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut active = response_target(
            0,
            UnderlayProtocol::Udp,
            50.0,
            0,
            4 * payload_bytes as u64,
            true,
        );
        active.has_bulk_rate_evidence = true;
        let mut ack_data_only = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 0, false);
        ack_data_only.has_bulk_rate_evidence = false;
        ack_data_only.has_sender_evidence = true;

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[active.clone(), ack_data_only.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(CarrierPathKey {
                underlay: UnderlayProtocol::Udp,
                path_id: PathId(0),
            }),
            0,
            None,
        )
        .expect("bulk-rate-proven Service should remain dispatchable");

        assert_eq!(
            selected.target.key, ack_data_only.key,
            "sender-evidenced same-family Validation may consume bounded startup sampling credit"
        );
        assert_eq!(
            selected.admission.role,
            PathRuntimeRole::Subflow,
            "startup sampling must not migrate the Service owner"
        );
        assert!(
            selected
                .subflow_set_commit
                .is_some_and(|commit| commit.input.startup_owner_allowed)
        );
    }

    #[test]
    fn measured_same_family_subflow_is_not_throttled_by_startup_credit() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let active_key = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        };
        let mut active = response_target(
            0,
            UnderlayProtocol::Udp,
            50.0,
            0,
            16 * payload_bytes as u64,
            true,
        );
        active.has_bulk_rate_evidence = true;
        let service_envelope =
            bulk_active_service_product_envelope_bytes(active.snapshot, payload_bytes, mux_limits);
        active.snapshot.product_bytes_in_flight = service_envelope;
        active.snapshot.queue_bytes = payload_bytes as u64;
        let mut bulk_rate_subflow = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 0, false);
        bulk_rate_subflow.has_sender_evidence = true;
        bulk_rate_subflow.has_bulk_rate_evidence = true;

        let first = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[active.clone(), bulk_rate_subflow.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(active_key),
            0,
            None,
        )
        .expect("first measured Subflow frame should be admitted");
        let commit = first
            .subflow_set_commit
            .expect("measured Subflow admission should carry commit state");
        assert_eq!(first.admission.role, PathRuntimeRole::Subflow);
        assert_eq!(
            commit.startup_owner_credit_bytes,
            usize::try_from(reliable_subflow_startup_sample_limit_bytes(mux_limits)).unwrap(),
            "the Subflow ledger keeps one stable startup sampling envelope across all decisions"
        );

        let mut subflow_set = FlowSubflowSet::new(
            0,
            commit.service,
            commit.startup_owner_credit_bytes,
            commit.optional_overhead_budget_bytes,
            commit.max_read_gap_budget,
        );
        assert_eq!(
            subflow_set.admit_subflow_owner(commit.input).decision,
            PathAdmissionDecision::AdmitSubflow
        );

        let second = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[active.clone(), bulk_rate_subflow.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(active_key),
            0,
            Some(&subflow_set),
        )
        .expect("measured Subflow should remain eligible if per-decision no-worse gates pass");
        assert_eq!(second.target.key, bulk_rate_subflow.key);
        assert_eq!(second.admission.role, PathRuntimeRole::Subflow);
    }

    #[test]
    fn mixed_dispatch_plan_does_not_carry_udp_product_duplicate_when_primary_is_tcp() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let (active_commands, _active_rx) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new_with_limits(
            SessionId(79),
            UnderlayProtocol::Tcp,
            PathId(0),
            active_commands,
            FlowLane::Throughput,
            mux_limits,
        );
        binding.update_path_metrics(
            CarrierPathKey {
                underlay: UnderlayProtocol::Tcp,
                path_id: PathId(0),
            },
            PathMetrics {
                path_id: PathId(0),
                underlay: UnderlayProtocol::Tcp,
                direction: PathMetricDirection::ServerToClient,
                metric_epoch: metric_epoch_now(),
                metric_age_us: 0,
                min_rtt_us: 50_000,
                srtt_us: 50_000,
                rttvar_us: 1_000,
                jitter_us: 1_000,
                delivery_rate_bps: 200_000_000,
                pacing_rate_bps: 200_000_000,
                loss_ppm: 0,
                ecn_ppm: 0,
                loss_observed: false,
                ecn_observed: false,
                bytes_in_flight: 0,
                queue_bytes: 0,
                inflight_limit_bytes: payload_bytes as u64,
                inflight_hi_bytes: payload_bytes as u64,
                confidence_ppm: 1_000_000,
                app_limited: false,
                has_ack_derived_data_sample: true,
                data_sample_count: 4,
                data_sample_bytes: payload_bytes as u64,
            },
            ServerPathMetricsSource::LocalSender,
        );
        let (validation_commands, _validation_rx) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                UnderlayProtocol::Udp,
                PathId(1),
                validation_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                payload_bytes,
            ),
            ResponseStreamAttachOutcome::Attached
        );
        binding.update_path_metrics(
            CarrierPathKey {
                underlay: UnderlayProtocol::Udp,
                path_id: PathId(1),
            },
            PathMetrics {
                path_id: PathId(1),
                underlay: UnderlayProtocol::Udp,
                direction: PathMetricDirection::ServerToClient,
                metric_epoch: metric_epoch_now(),
                metric_age_us: 0,
                min_rtt_us: 20_000,
                srtt_us: 20_000,
                rttvar_us: 1_000,
                jitter_us: 1_000,
                delivery_rate_bps: 1_000_000,
                pacing_rate_bps: 1_000_000,
                loss_ppm: 0,
                ecn_ppm: 0,
                loss_observed: false,
                ecn_observed: false,
                bytes_in_flight: 0,
                queue_bytes: 0,
                inflight_limit_bytes: payload_bytes as u64,
                inflight_hi_bytes: payload_bytes as u64,
                confidence_ppm: 1_000_000,
                app_limited: true,
                has_ack_derived_data_sample: true,
                data_sample_count: 0,
                data_sample_bytes: 0,
            },
            ServerPathMetricsSource::LocalSender,
        );
        let (_frames_tx, frames_rx) = mpsc::channel(1);
        let stream = ReliablePathStream {
            stream_id: StreamId(7),
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: UnderlayProtocol::Tcp,
            max_frame_payload_bytes: payload_bytes,
            output: ReliablePathStreamOutput::Switchable(binding),
            frames: frames_rx,
        };

        let plan = plan_response_data_dispatch(&stream, FlowLane::Throughput, 0, payload_bytes)
            .expect("TCP primary remains dispatchable");

        assert_eq!(
            plan.primary_key(),
            Some(CarrierPathKey {
                underlay: UnderlayProtocol::Tcp,
                path_id: PathId(0),
            })
        );
    }

    #[tokio::test]
    async fn stale_service_plan_cannot_enqueue_owner_data_after_repair_role_change() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let active = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let validation = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let (active_commands, _active_rx) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new_with_limits(
            SessionId(77),
            active.underlay,
            active.path_id,
            active_commands.clone(),
            FlowLane::Throughput,
            mux_limits,
        );
        let (validation_commands, mut validation_rx) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                validation.underlay,
                validation.path_id,
                validation_commands.clone(),
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                payload_bytes,
            ),
            ResponseStreamAttachOutcome::Attached
        );
        while try_recv_reliable_path_command(&mut validation_rx).is_some() {}
        binding.detach(active, &active_commands);
        assert_eq!(binding.ordered_data_owner(), None);

        let (_frames_tx, frames_rx) = mpsc::channel(1);
        let stream = ReliablePathStream {
            stream_id: StreamId(77),
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: UnderlayProtocol::Tcp,
            max_frame_payload_bytes: payload_bytes,
            output: ReliablePathStreamOutput::Switchable(binding.clone()),
            frames: frames_rx,
        };
        let plan = plan_response_data_dispatch(&stream, FlowLane::Throughput, 0, payload_bytes)
            .expect("liveness survivor may become the frontier-clear Service");
        assert_eq!(plan.primary_key(), Some(validation));
        assert_eq!(plan.primary_role(), PathRuntimeRole::Service);
        assert_eq!(
            binding.attach(
                validation.underlay,
                validation.path_id,
                validation_commands,
                FlowLane::Throughput,
                StreamOpenRole::Repair,
                payload_bytes,
            ),
            ResponseStreamAttachOutcome::RoleChanged
        );
        let frame = Frame::StreamData {
            stream_id: StreamId(77),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![0x77; payload_bytes]),
        };

        assert!(matches!(
            emit_planned_response_data_frame(&stream, plan, frame, FlowLane::Throughput).await,
            Err(RuntimeError::SenderServiceBlocked)
        ));
        assert!(
            try_recv_reliable_path_command(&mut validation_rx).is_none(),
            "a stale Service plan must not enqueue STREAM_DATA on a Repair attachment"
        );
        let target = binding
            .sender_path_targets(FlowLane::Throughput, payload_bytes)
            .into_iter()
            .find(|target| target.key == validation)
            .expect("Repair output remains attached");
        assert_eq!(target.attachment_role, StreamOpenRole::Repair);
        assert_eq!(target.snapshot.product_bytes_in_flight, 0);
        assert_eq!(target.commands.pending_bytes(), 0);
        assert_eq!(binding.ordered_data_owner(), None);
    }

    #[tokio::test]
    async fn passive_attach_preserves_one_bounded_exact_service_plan() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let service = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let (service_commands, mut service_rx) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new_with_limits(
            SessionId(109),
            service.underlay,
            service.path_id,
            service_commands,
            FlowLane::Throughput,
            mux_limits,
        );
        let (_frames_tx, frames_rx) = mpsc::channel(1);
        let stream = ReliablePathStream {
            stream_id: StreamId(109),
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: service.underlay,
            max_frame_payload_bytes: payload_bytes,
            output: ReliablePathStreamOutput::Switchable(binding.clone()),
            frames: frames_rx,
        };
        let plan = plan_response_data_dispatch(&stream, FlowLane::Throughput, 0, payload_bytes)
            .expect("live Service has a bounded owner plan");
        assert_eq!(plan.primary_key(), Some(service));
        assert_eq!(plan.primary_role(), PathRuntimeRole::Service);
        let planner_generation = binding.subflow_state_snapshot().0;

        let repair = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        };
        let (repair_commands, mut repair_rx) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                repair.underlay,
                repair.path_id,
                repair_commands,
                FlowLane::Throughput,
                StreamOpenRole::Repair,
                payload_bytes,
            ),
            ResponseStreamAttachOutcome::Attached
        );
        assert_ne!(binding.subflow_state_snapshot().0, planner_generation);

        let frame = Frame::StreamData {
            stream_id: StreamId(109),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![0x6d; payload_bytes]),
        };
        let outcome =
            emit_planned_response_data_frame(&stream, plan, frame.clone(), FlowLane::Throughput)
                .await
                .expect("passive growth does not revoke the exact live Service quantum");
        assert_eq!(outcome.selected_path, Some(service));
        assert!(matches!(
            try_recv_reliable_path_command(&mut service_rx),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
        ));
        assert!(try_recv_reliable_path_command(&mut repair_rx).is_none());
        assert_eq!(
            binding.owner_flight_keys_overlapping_frame(&frame),
            vec![service]
        );
    }

    #[tokio::test]
    async fn quic_probe_path_does_not_receive_product_duplicate_data() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let (active_commands, mut active_rx) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new_with_limits(
            SessionId(78),
            UnderlayProtocol::Udp,
            PathId(0),
            active_commands,
            FlowLane::Throughput,
            mux_limits,
        );
        let (validation_commands, mut validation_rx) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                UnderlayProtocol::Udp,
                PathId(1),
                validation_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                payload_bytes,
            ),
            ResponseStreamAttachOutcome::Attached
        );
        while try_recv_reliable_path_command(&mut validation_rx).is_some() {}
        let (_frames_tx, frames_rx) = mpsc::channel(1);
        let stream = ReliablePathStream {
            stream_id: StreamId(7),
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: UnderlayProtocol::Udp,
            max_frame_payload_bytes: payload_bytes,
            output: ReliablePathStreamOutput::Switchable(binding.clone()),
            frames: frames_rx,
        };
        let plan = plan_response_data_dispatch(&stream, FlowLane::Throughput, 0, payload_bytes)
            .expect("active path should remain dispatchable");
        let frame = Frame::StreamData {
            stream_id: StreamId(7),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![7_u8; payload_bytes]),
        };

        let outcome =
            emit_planned_response_data_frame(&stream, plan, frame.clone(), FlowLane::Throughput)
                .await
                .expect("primary data should emit");

        assert_eq!(
            outcome.selected_path,
            Some(CarrierPathKey {
                underlay: UnderlayProtocol::Udp,
                path_id: PathId(0),
            })
        );
        assert!(matches!(
            recv_reliable_path_command(&mut active_rx).await,
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
        ));
        assert!(
            try_recv_reliable_path_command(&mut validation_rx).is_none(),
            "Probe paths must not receive product STREAM_DATA"
        );
        let lower = binding.lower_flights_before_offset(payload_bytes as u64);
        assert!(
            lower.is_empty(),
            "plain unacked OwnerData stays in the flight ledger but is not ACK-hole ordering debt"
        );
    }

    #[tokio::test]
    async fn response_owner_data_keeps_fifo_order_across_lane_changes() {
        let (commands, mut receiver) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new(
            SessionId(108),
            UnderlayProtocol::Tcp,
            PathId(0),
            commands,
            FlowLane::Throughput,
        );
        let (_frames_tx, frames_rx) = mpsc::channel(1);
        let stream = ReliablePathStream {
            stream_id: StreamId(108),
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: UnderlayProtocol::Tcp,
            max_frame_payload_bytes: 4,
            output: ReliablePathStreamOutput::Switchable(binding.clone()),
            frames: frames_rx,
        };
        let target = binding
            .sender_path_targets(FlowLane::Throughput, 4)
            .into_iter()
            .next()
            .expect("binding has service target");

        let bulk_first = ResponseDataDispatchPlan {
            primary: ResponseDataDispatchTarget::Switchable {
                binding: binding.clone(),
                target: target.clone(),
                role: PathRuntimeRole::Service,
                subflow_set_commit: None,
                ack_clock_calibration_commit: None,
            },
        };
        let latency_second = ResponseDataDispatchPlan {
            primary: ResponseDataDispatchTarget::Switchable {
                binding,
                target,
                role: PathRuntimeRole::Service,
                subflow_set_commit: None,
                ack_clock_calibration_commit: None,
            },
        };

        emit_planned_response_data_frame(
            &stream,
            bulk_first,
            Frame::StreamData {
                stream_id: StreamId(108),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"aaaa"),
            },
            FlowLane::Throughput,
        )
        .await
        .expect("bulk owner data should enqueue");
        emit_planned_response_data_frame(
            &stream,
            latency_second,
            Frame::StreamData {
                stream_id: StreamId(108),
                offset: 4,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"bbbb"),
            },
            FlowLane::Latency,
        )
        .await
        .expect("latency owner data should enqueue");

        assert!(matches!(
            recv_reliable_path_command(&mut receiver).await,
            Some(ReliablePathCommand::SendFrame(Frame::StreamData {
                offset: 0,
                ..
            }))
        ));
        assert!(matches!(
            recv_reliable_path_command(&mut receiver).await,
            Some(ReliablePathCommand::SendFrame(Frame::StreamData {
                offset: 4,
                ..
            }))
        ));
    }

    #[test]
    fn sender_evidence_same_family_candidate_cannot_own_under_lower_owner_debt() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let active_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let active = response_target(
            active_key.path_id.0,
            active_key.underlay,
            100.0,
            0,
            4 * payload_bytes as u64,
            true,
        );
        let mut proof_only = response_target(
            1,
            UnderlayProtocol::Tcp,
            5.0,
            0,
            4 * payload_bytes as u64,
            false,
        );
        proof_only.has_bulk_rate_evidence = false;
        proof_only.has_sender_evidence = true;

        let selected = choose_response_sender_data_target(
            &[active.clone(), proof_only.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[CarrierPathFlightDebt {
                key: active_key,
                bytes: payload_bytes as u64,
            }],
            Some(active_key),
        )
        .expect("service path should remain dispatchable");

        assert_eq!(
            selected.key, active.key,
            "same-family sender evidence is not enough to assign later unique bytes while the Service owns unresolved lower bytes"
        );
    }

    #[test]
    fn bulk_rate_same_family_candidate_cannot_own_later_data_under_lower_owner_debt() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut owner = response_target(
            0,
            UnderlayProtocol::Udp,
            80.0,
            2 * 1024 * 1024,
            16 * 1024 * 1024,
            true,
        );
        owner.snapshot.product_progress_rate_bps = Some(500_000_000.0);
        let alternate = response_target(1, UnderlayProtocol::Udp, 7.0, 0, 16 * 1024 * 1024, false);
        let lower_flights = vec![CarrierPathFlightDebt {
            key: owner.key,
            bytes: 2 * 1024 * 1024,
        }];

        let selected = choose_response_sender_data_target(
            &[owner.clone(), alternate],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &lower_flights,
            Some(owner.key),
        )
        .expect("lower owner should remain dispatchable");

        assert_eq!(
            selected.key, owner.key,
            "bulk-rate evidence proves the alternate path is eligible at a clear frontier, not that it may extend an existing ordered receive hole"
        );
    }

    #[test]
    fn proof_only_validation_candidate_gets_explicit_startup_admission() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let active = response_target(
            0,
            UnderlayProtocol::Udp,
            100.0,
            0,
            4 * payload_bytes as u64,
            true,
        );
        let mut proof_only = response_target(
            1,
            UnderlayProtocol::Udp,
            50.0,
            0,
            4 * payload_bytes as u64,
            false,
        );
        proof_only.has_bulk_rate_evidence = false;
        proof_only.has_sender_evidence = true;
        let candidates = vec![&active, &proof_only];
        let lead = ResponseBulkLead {
            key: active.key,
            snapshot: active.snapshot,
            eta_ms: active.eta_ms,
        };

        let admission = response_target_unique_owner_admission(
            &proof_only,
            &candidates,
            lead,
            None,
            0,
            payload_bytes,
            mux_limits,
        );

        assert_eq!(admission.decision, PathAdmissionDecision::AdmitSubflow);
        assert_eq!(admission.role, PathRuntimeRole::Subflow);
    }

    #[test]
    fn frontier_clear_bulk_rate_candidate_is_subflow_not_service() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let active = response_target(0, UnderlayProtocol::Udp, 80.0, 0, 16 * 1024 * 1024, true);
        let alternate = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
        let candidates = vec![&active, &alternate];
        let lead = ResponseBulkLead {
            key: alternate.key,
            snapshot: alternate.snapshot,
            eta_ms: alternate.eta_ms,
        };

        let admission = response_target_unique_owner_admission(
            &alternate,
            &candidates,
            lead,
            None,
            0,
            payload_bytes,
            mux_limits,
        );

        assert_eq!(admission.decision, PathAdmissionDecision::AdmitSubflow);
        assert_eq!(admission.role, PathRuntimeRole::Subflow);
    }

    #[tokio::test]
    async fn response_planning_bounds_app_limited_validation_sampling_before_service_resumes() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let lane_tracker = Arc::new(ServerPathLaneTracker::default());
        let (active_commands, mut active_rx) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new_with_limits_and_tracker(
            SessionId(88),
            UnderlayProtocol::Udp,
            PathId(0),
            active_commands,
            FlowLane::Throughput,
            mux_limits,
            lane_tracker.clone(),
        );
        let service = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        };
        binding.update_path_metrics(
            service,
            PathMetrics {
                path_id: service.path_id,
                underlay: service.underlay,
                direction: PathMetricDirection::ServerToClient,
                metric_epoch: metric_epoch_now(),
                metric_age_us: 0,
                min_rtt_us: 50_000,
                srtt_us: 50_000,
                rttvar_us: 1_000,
                jitter_us: 1_000,
                delivery_rate_bps: 200_000_000,
                pacing_rate_bps: 200_000_000,
                loss_ppm: 0,
                ecn_ppm: 0,
                loss_observed: false,
                ecn_observed: false,
                bytes_in_flight: 0,
                queue_bytes: 0,
                inflight_limit_bytes: (payload_bytes * 8) as u64,
                inflight_hi_bytes: (payload_bytes * 8) as u64,
                confidence_ppm: 1_000_000,
                app_limited: false,
                has_ack_derived_data_sample: true,
                data_sample_count: 4,
                data_sample_bytes: (payload_bytes * 8) as u64,
            },
            ServerPathMetricsSource::LocalSender,
        );
        let optional = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let (optional_commands, mut optional_rx) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                optional.underlay,
                optional.path_id,
                optional_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                payload_bytes,
            ),
            ResponseStreamAttachOutcome::Attached
        );
        binding.update_path_metrics(
            optional,
            PathMetrics {
                path_id: optional.path_id,
                underlay: optional.underlay,
                direction: PathMetricDirection::ServerToClient,
                metric_epoch: metric_epoch_now(),
                metric_age_us: 0,
                min_rtt_us: 5_000,
                srtt_us: 5_000,
                rttvar_us: 500,
                jitter_us: 500,
                delivery_rate_bps: 500_000_000,
                pacing_rate_bps: 500_000_000,
                loss_ppm: 0,
                ecn_ppm: 0,
                loss_observed: false,
                ecn_observed: false,
                bytes_in_flight: 0,
                queue_bytes: 0,
                inflight_limit_bytes: (payload_bytes * 8) as u64,
                inflight_hi_bytes: (payload_bytes * 8) as u64,
                confidence_ppm: 900_000,
                app_limited: true,
                has_ack_derived_data_sample: true,
                data_sample_count: 0,
                data_sample_bytes: 0,
            },
            ServerPathMetricsSource::LocalSender,
        );
        binding.update_path_metrics(
            optional,
            PathMetrics {
                path_id: optional.path_id,
                underlay: optional.underlay,
                direction: PathMetricDirection::ServerToClient,
                metric_epoch: metric_epoch_now(),
                metric_age_us: 0,
                min_rtt_us: 5_000,
                srtt_us: 5_000,
                rttvar_us: 500,
                jitter_us: 500,
                delivery_rate_bps: 500_000_000,
                pacing_rate_bps: 500_000_000,
                loss_ppm: 0,
                ecn_ppm: 0,
                loss_observed: false,
                ecn_observed: false,
                bytes_in_flight: 0,
                queue_bytes: 0,
                inflight_limit_bytes: (payload_bytes * 8) as u64,
                inflight_hi_bytes: (payload_bytes * 8) as u64,
                confidence_ppm: 0,
                app_limited: false,
                has_ack_derived_data_sample: false,
                data_sample_count: 0,
                data_sample_bytes: 0,
            },
            ServerPathMetricsSource::PeerHint,
        );

        let (_frames_tx, frames_rx) = mpsc::channel(1);
        let stream = ReliablePathStream {
            stream_id: StreamId(88),
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: UnderlayProtocol::Udp,
            max_frame_payload_bytes: payload_bytes,
            output: ReliablePathStreamOutput::Switchable(binding.clone()),
            frames: frames_rx,
        };
        let single_flow_plan =
            plan_response_data_dispatch(&stream, FlowLane::Throughput, 0, payload_bytes)
                .expect("the one-flow Service should remain dispatchable");
        assert_eq!(single_flow_plan.primary_key(), Some(service));
        assert_eq!(single_flow_plan.primary_role(), PathRuntimeRole::Service);
        drop(single_flow_plan);

        let (second_flow_commands, _second_flow_receivers) = reliable_path_command_channels(8);
        let _second_flow = ResponseStreamBinding::new_with_limits_and_tracker(
            SessionId(88),
            UnderlayProtocol::Udp,
            PathId(2),
            second_flow_commands,
            FlowLane::Throughput,
            mux_limits,
            lane_tracker,
        );
        let startup_limit =
            usize::try_from(reliable_subflow_startup_sample_limit_bytes(mux_limits)).unwrap();
        assert_eq!(startup_limit % payload_bytes, 0);
        for quantum in 0..(startup_limit / payload_bytes) {
            let offset = (quantum * payload_bytes) as u64;
            let plan =
                plan_response_data_dispatch(&stream, FlowLane::Throughput, offset, payload_bytes)
                    .expect("bounded Validation sampling should be dispatchable");
            assert_eq!(plan.primary_key(), Some(optional));
            assert_eq!(plan.primary_role(), PathRuntimeRole::Subflow);

            let frame = Frame::StreamData {
                stream_id: StreamId(88),
                offset,
                flags: StreamFlags::NONE,
                payload: Bytes::from(vec![9_u8; payload_bytes]),
            };
            let outcome =
                emit_planned_response_data_frame(&stream, plan, frame, FlowLane::Throughput)
                    .await
                    .expect("bounded startup Subflow OwnerData should emit");
            assert_eq!(outcome.selected_path, Some(optional));
            assert!(try_recv_reliable_path_command(&mut optional_rx).is_some());
            assert_eq!(
                binding.ordered_data_owner(),
                Some(service),
                "startup sampling must not migrate Service ownership"
            );
        }

        let service_offset = startup_limit as u64;
        let plan = plan_response_data_dispatch(
            &stream,
            FlowLane::Throughput,
            service_offset,
            payload_bytes,
        )
        .expect("Service should resume after the startup sample cap");
        assert_eq!(plan.primary_key(), Some(service));
        assert_eq!(plan.primary_role(), PathRuntimeRole::Service);
        let frame = Frame::StreamData {
            stream_id: StreamId(88),
            offset: service_offset,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![7_u8; payload_bytes]),
        };
        let outcome = emit_planned_response_data_frame(&stream, plan, frame, FlowLane::Throughput)
            .await
            .expect("Service OwnerData should emit after bounded sampling");
        assert_eq!(outcome.selected_path, Some(service));
        assert!(try_recv_reliable_path_command(&mut active_rx).is_some());
    }

    #[tokio::test]
    async fn blocked_path_queue_rolls_back_unemitted_startup_credit() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let service = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let candidate = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let (service_commands, _service_rx) = reliable_path_command_channels(1);
        let binding = ResponseStreamBinding::new_with_limits(
            SessionId(89),
            service.underlay,
            service.path_id,
            service_commands,
            FlowLane::Throughput,
            mux_limits,
        );
        let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(1);
        assert_eq!(
            binding.attach(
                candidate.underlay,
                candidate.path_id,
                candidate_commands.clone(),
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                payload_bytes,
            ),
            ResponseStreamAttachOutcome::Attached
        );
        assert!(matches!(
            try_recv_reliable_path_command(&mut candidate_rx),
            Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
        ));
        let target = binding
            .sender_path_targets(FlowLane::Throughput, payload_bytes)
            .into_iter()
            .find(|target| target.key == candidate)
            .expect("candidate output is attached");
        let (planner_generation, _) = binding.subflow_state_snapshot();
        let commit = ResponseSubflowAdmissionCommit {
            planner_generation,
            lane_generation: binding.lane_generation(),
            service,
            startup_owner_credit_bytes: payload_bytes,
            optional_overhead_budget_bytes: 0,
            max_read_gap_budget: Duration::ZERO,
            input: SubflowAdmissionInput {
                key: candidate,
                bulk_rate_proven: false,
                startup_owner_allowed: true,
                frontier_clear: true,
                completion_improves: false,
                observed_goodput_non_degrading: true,
                read_gap: Duration::ZERO,
                owner_bytes: payload_bytes,
                optional_overhead_bytes: 0,
            },
        };
        let (_frames_tx, frames_rx) = mpsc::channel(1);
        let stream = ReliablePathStream {
            stream_id: StreamId(89),
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: service.underlay,
            max_frame_payload_bytes: payload_bytes,
            output: ReliablePathStreamOutput::Switchable(binding.clone()),
            frames: frames_rx,
        };
        let frame = Frame::StreamData {
            stream_id: StreamId(89),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![5_u8; payload_bytes]),
        };
        candidate_commands
            .try_enqueue_stream_ordered_frame(frame.clone(), FlowLane::Throughput)
            .expect("fill the candidate data queue after planning");
        let blocked = emit_planned_response_data_frame(
            &stream,
            ResponseDataDispatchPlan {
                primary: ResponseDataDispatchTarget::Switchable {
                    binding: binding.clone(),
                    target: target.clone(),
                    role: PathRuntimeRole::Subflow,
                    subflow_set_commit: Some(commit),
                    ack_clock_calibration_commit: None,
                },
            },
            frame.clone(),
            FlowLane::Throughput,
        )
        .await;
        assert!(matches!(blocked, Err(RuntimeError::SenderServiceBlocked)));
        assert!(try_recv_reliable_path_command(&mut candidate_rx).is_some());

        let emitted = emit_planned_response_data_frame(
            &stream,
            ResponseDataDispatchPlan {
                primary: ResponseDataDispatchTarget::Switchable {
                    binding,
                    target,
                    role: PathRuntimeRole::Subflow,
                    subflow_set_commit: Some(commit),
                    ack_clock_calibration_commit: None,
                },
            },
            frame,
            FlowLane::Throughput,
        )
        .await
        .expect("the rolled-back startup quantum remains admissible");
        assert_eq!(emitted.selected_path, Some(candidate));
    }

    #[tokio::test]
    async fn stale_passive_topology_plan_blocks_subflow_reservation_and_enqueue() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let service = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let candidate = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let unrelated = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(2),
        };
        let (service_commands, _service_rx) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new_with_limits(
            SessionId(90),
            service.underlay,
            service.path_id,
            service_commands,
            FlowLane::Throughput,
            mux_limits,
        );
        let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                candidate.underlay,
                candidate.path_id,
                candidate_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                payload_bytes,
            ),
            ResponseStreamAttachOutcome::Attached
        );
        assert!(matches!(
            try_recv_reliable_path_command(&mut candidate_rx),
            Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
        ));
        let target = binding
            .sender_path_targets(FlowLane::Throughput, payload_bytes)
            .into_iter()
            .find(|target| target.key == candidate)
            .expect("candidate output is attached");
        let (stale_planner_generation, _) = binding.subflow_state_snapshot();
        let lane_generation = binding.lane_generation();
        let input = SubflowAdmissionInput {
            key: candidate,
            bulk_rate_proven: false,
            startup_owner_allowed: true,
            frontier_clear: true,
            completion_improves: false,
            observed_goodput_non_degrading: true,
            read_gap: Duration::ZERO,
            owner_bytes: payload_bytes,
            optional_overhead_bytes: 0,
        };
        let stale_commit = ResponseSubflowAdmissionCommit {
            planner_generation: stale_planner_generation,
            lane_generation,
            service,
            startup_owner_credit_bytes: payload_bytes,
            optional_overhead_budget_bytes: 0,
            max_read_gap_budget: Duration::ZERO,
            input,
        };
        let (unrelated_commands, _unrelated_rx) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                unrelated.underlay,
                unrelated.path_id,
                unrelated_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                payload_bytes,
            ),
            ResponseStreamAttachOutcome::Attached
        );
        let (fresh_planner_generation, _) = binding.subflow_state_snapshot();
        assert_ne!(fresh_planner_generation, stale_planner_generation);

        let (_frames_tx, frames_rx) = mpsc::channel(1);
        let stream = ReliablePathStream {
            stream_id: StreamId(90),
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: service.underlay,
            max_frame_payload_bytes: payload_bytes,
            output: ReliablePathStreamOutput::Switchable(binding.clone()),
            frames: frames_rx,
        };
        let frame = Frame::StreamData {
            stream_id: StreamId(90),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![0x55; payload_bytes]),
        };
        let stale = emit_planned_response_data_frame(
            &stream,
            ResponseDataDispatchPlan {
                primary: ResponseDataDispatchTarget::Switchable {
                    binding: binding.clone(),
                    target: target.clone(),
                    role: PathRuntimeRole::Subflow,
                    subflow_set_commit: Some(stale_commit),
                    ack_clock_calibration_commit: None,
                },
            },
            frame.clone(),
            FlowLane::Throughput,
        )
        .await;
        assert!(matches!(stale, Err(RuntimeError::SenderServiceBlocked)));
        assert!(
            try_recv_reliable_path_command(&mut candidate_rx).is_none(),
            "planner invalidation must fence both reservation and owner enqueue"
        );

        let fresh = emit_planned_response_data_frame(
            &stream,
            ResponseDataDispatchPlan {
                primary: ResponseDataDispatchTarget::Switchable {
                    binding,
                    target,
                    role: PathRuntimeRole::Subflow,
                    subflow_set_commit: Some(ResponseSubflowAdmissionCommit {
                        planner_generation: fresh_planner_generation,
                        ..stale_commit
                    }),
                    ack_clock_calibration_commit: None,
                },
            },
            frame,
            FlowLane::Throughput,
        )
        .await
        .expect("fresh generation may reserve and enqueue the startup quantum");
        assert_eq!(fresh.selected_path, Some(candidate));
        assert!(matches!(
            try_recv_reliable_path_command(&mut candidate_rx),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
        ));
    }

    #[tokio::test]
    async fn normal_repair_cache_retention_does_not_create_authoritative_owner_debt() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let active_key = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        };
        let alternate_key = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let (active_commands, mut active_rx) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new_with_limits(
            SessionId(83),
            UnderlayProtocol::Udp,
            active_key.path_id,
            active_commands,
            FlowLane::Throughput,
            mux_limits,
        );
        binding.set_ordered_data_owner(active_key);
        binding.update_path_metrics(
            active_key,
            PathMetrics {
                path_id: active_key.path_id,
                underlay: active_key.underlay,
                direction: PathMetricDirection::ServerToClient,
                metric_epoch: metric_epoch_now(),
                metric_age_us: 0,
                min_rtt_us: 50_000,
                srtt_us: 50_000,
                rttvar_us: 1_000,
                jitter_us: 1_000,
                delivery_rate_bps: 200_000_000,
                pacing_rate_bps: 200_000_000,
                loss_ppm: 0,
                ecn_ppm: 0,
                loss_observed: false,
                ecn_observed: false,
                bytes_in_flight: 0,
                queue_bytes: 0,
                inflight_limit_bytes: (16 * payload_bytes) as u64,
                inflight_hi_bytes: (16 * payload_bytes) as u64,
                confidence_ppm: 1_000_000,
                app_limited: false,
                has_ack_derived_data_sample: true,
                data_sample_count: 4,
                data_sample_bytes: (16 * payload_bytes) as u64,
            },
            ServerPathMetricsSource::LocalSender,
        );
        let (alternate_commands, mut alternate_rx) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                UnderlayProtocol::Udp,
                alternate_key.path_id,
                alternate_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                payload_bytes,
            ),
            ResponseStreamAttachOutcome::Attached
        );
        binding.update_path_metrics(
            alternate_key,
            PathMetrics {
                path_id: alternate_key.path_id,
                underlay: alternate_key.underlay,
                direction: PathMetricDirection::ServerToClient,
                metric_epoch: metric_epoch_now(),
                metric_age_us: 0,
                min_rtt_us: 5_000,
                srtt_us: 5_000,
                rttvar_us: 500,
                jitter_us: 500,
                delivery_rate_bps: 1_000_000_000,
                pacing_rate_bps: 1_000_000_000,
                loss_ppm: 0,
                ecn_ppm: 0,
                loss_observed: false,
                ecn_observed: false,
                bytes_in_flight: 0,
                queue_bytes: 0,
                inflight_limit_bytes: (16 * payload_bytes) as u64,
                inflight_hi_bytes: (16 * payload_bytes) as u64,
                confidence_ppm: 1_000_000,
                app_limited: false,
                has_ack_derived_data_sample: true,
                data_sample_count: 4,
                data_sample_bytes: (16 * payload_bytes) as u64,
            },
            ServerPathMetricsSource::LocalSender,
        );
        let (_frames_tx, frames_rx) = mpsc::channel(1);
        let stream = ReliablePathStream {
            stream_id: StreamId(7),
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: UnderlayProtocol::Udp,
            max_frame_payload_bytes: payload_bytes,
            output: ReliablePathStreamOutput::Switchable(binding.clone()),
            frames: frames_rx,
        };
        let owner_tail_guard_bytes = payload_bytes.saturating_mul(2);
        let mut send_stream = ReliableSendStream::new(StreamId(7), mux_limits);
        let mut retained_unacked_bytes = owner_tail_guard_bytes.saturating_add(payload_bytes);
        while retained_unacked_bytes > 0 {
            let chunk = retained_unacked_bytes.min(payload_bytes);
            let _unacked = send_stream
                .send_data(Bytes::from(vec![1_u8; chunk]), StreamFlags::NONE)
                .expect("seed normal retained unacked OwnerData above the synthetic tail guard");
            retained_unacked_bytes -= chunk;
        }
        assert!(send_stream.repair_bytes() > owner_tail_guard_bytes);
        assert!(
            binding
                .lower_flights_before_offset(send_stream.next_offset())
                .is_empty(),
            "this regression isolates repair-cache retention from authoritative path-flight debt"
        );
        while let Some(_setup_command) = try_recv_reliable_path_command(&mut alternate_rx) {}

        let mut sender = ServerResponseSenderService::new(SessionId(83), StreamId(7));
        sender.enqueue_data_for_lane(Bytes::from(vec![2_u8; payload_bytes]), FlowLane::Throughput);
        let dispatch = sender
            .dispatch_next(&stream, &mut send_stream, FlowLane::Throughput, mux_limits)
            .await
            .expect("normal repair-cache retention must not block Service OwnerData");

        assert_eq!(dispatch.selected_path, Some(active_key));
        assert_eq!(
            binding.ordered_data_owner(),
            Some(active_key),
            "normal repair-cache retention must not rewrite the Service owner hint"
        );
        assert!(matches!(
            recv_reliable_path_command(&mut active_rx).await,
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
        ));
        assert!(
            try_recv_reliable_path_command(&mut alternate_rx).is_none(),
            "retained repair-cache bytes are not authoritative debt and must not displace feedable Service"
        );
    }

    #[tokio::test]
    async fn response_owner_tail_guard_admits_measured_subflow_when_service_is_backpressured() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let active_key = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        };
        let alternate_key = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let (active_commands, mut active_rx) = reliable_path_command_channels(1);
        let active_commands_for_backpressure = active_commands.clone();
        let binding = ResponseStreamBinding::new_with_limits(
            SessionId(82),
            UnderlayProtocol::Udp,
            active_key.path_id,
            active_commands,
            FlowLane::Throughput,
            mux_limits,
        );
        binding.set_ordered_data_owner(active_key);
        binding.update_path_metrics(
            active_key,
            PathMetrics {
                path_id: active_key.path_id,
                underlay: active_key.underlay,
                direction: PathMetricDirection::ServerToClient,
                metric_epoch: metric_epoch_now(),
                metric_age_us: 0,
                min_rtt_us: 50_000,
                srtt_us: 50_000,
                rttvar_us: 1_000,
                jitter_us: 1_000,
                delivery_rate_bps: 200_000_000,
                pacing_rate_bps: 200_000_000,
                loss_ppm: 0,
                ecn_ppm: 0,
                loss_observed: false,
                ecn_observed: false,
                bytes_in_flight: 0,
                queue_bytes: 0,
                inflight_limit_bytes: (16 * payload_bytes) as u64,
                inflight_hi_bytes: (16 * payload_bytes) as u64,
                confidence_ppm: 1_000_000,
                app_limited: false,
                has_ack_derived_data_sample: true,
                data_sample_count: 4,
                data_sample_bytes: (16 * payload_bytes) as u64,
            },
            ServerPathMetricsSource::LocalSender,
        );
        let (alternate_commands, mut alternate_rx) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                UnderlayProtocol::Udp,
                alternate_key.path_id,
                alternate_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                payload_bytes,
            ),
            ResponseStreamAttachOutcome::Attached
        );
        binding.update_path_metrics(
            alternate_key,
            PathMetrics {
                path_id: alternate_key.path_id,
                underlay: alternate_key.underlay,
                direction: PathMetricDirection::ServerToClient,
                metric_epoch: metric_epoch_now(),
                metric_age_us: 0,
                min_rtt_us: 5_000,
                srtt_us: 5_000,
                rttvar_us: 500,
                jitter_us: 500,
                delivery_rate_bps: 1_000_000_000,
                pacing_rate_bps: 1_000_000_000,
                loss_ppm: 0,
                ecn_ppm: 0,
                loss_observed: false,
                ecn_observed: false,
                bytes_in_flight: 0,
                queue_bytes: 0,
                inflight_limit_bytes: (16 * payload_bytes) as u64,
                inflight_hi_bytes: (16 * payload_bytes) as u64,
                confidence_ppm: 1_000_000,
                app_limited: false,
                has_ack_derived_data_sample: true,
                data_sample_count: 4,
                data_sample_bytes: (16 * payload_bytes) as u64,
            },
            ServerPathMetricsSource::LocalSender,
        );
        let (_frames_tx, frames_rx) = mpsc::channel(1);
        let stream = ReliablePathStream {
            stream_id: StreamId(7),
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: UnderlayProtocol::Udp,
            max_frame_payload_bytes: payload_bytes,
            output: ReliablePathStreamOutput::Switchable(binding.clone()),
            frames: frames_rx,
        };
        let owner_tail_guard_bytes = payload_bytes.saturating_mul(2);
        let mut send_stream = ReliableSendStream::new(StreamId(7), mux_limits);
        let mut remaining_owner_debt = owner_tail_guard_bytes.saturating_add(payload_bytes);
        while remaining_owner_debt > 0 {
            let chunk = remaining_owner_debt.min(payload_bytes);
            let _unacked = send_stream
                .send_data(Bytes::from(vec![1_u8; chunk]), StreamFlags::NONE)
                .expect("seed unacked ordered-owner tail guard");
            remaining_owner_debt -= chunk;
        }
        assert!(send_stream.repair_bytes() > owner_tail_guard_bytes);
        while let Some(_setup_command) = try_recv_reliable_path_command(&mut alternate_rx) {}
        active_commands_for_backpressure
            .try_enqueue_admitted_frame(
                Frame::StreamData {
                    stream_id: StreamId(7),
                    offset: 0,
                    flags: StreamFlags::NONE,
                    payload: Bytes::from_static(b"queued"),
                },
                FlowLane::Throughput,
            )
            .expect("seed full Service data queue");

        let mut sender = ServerResponseSenderService::new(SessionId(82), StreamId(7));
        sender.enqueue_data_for_lane(Bytes::from(vec![2_u8; payload_bytes]), FlowLane::Throughput);
        let ordered_owner_debt_bytes = send_stream.repair_bytes();
        let dispatch = sender
            .dispatch_next_with_ordered_owner_debt(
                &stream,
                &mut send_stream,
                FlowLane::Throughput,
                mux_limits,
                ordered_owner_debt_bytes,
            )
            .await;

        let dispatch =
            dispatch.expect("measured same-underlay Subflow should pass no-worse tail admission");
        assert_eq!(dispatch.selected_path, Some(alternate_key));
        assert_eq!(binding.ordered_data_owner(), Some(active_key));
        assert!(matches!(
            try_recv_reliable_path_command(&mut alternate_rx),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
        ));
        assert!(matches!(
            try_recv_reliable_path_command(&mut active_rx),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { payload, .. }))
                if payload == Bytes::from_static(b"queued")
        ));
        assert!(try_recv_reliable_path_command(&mut active_rx).is_none());
    }

    #[tokio::test]
    async fn quic_ack_data_path_does_not_own_range_under_lower_owner_debt() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let active_key = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        };
        let (active_commands, _active_rx) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new_with_limits(
            SessionId(88),
            active_key.underlay,
            active_key.path_id,
            active_commands,
            FlowLane::Throughput,
            mux_limits,
        );
        binding.set_ordered_data_owner(active_key);
        let active_frame = Frame::StreamData {
            stream_id: StreamId(7),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![3_u8; payload_bytes]),
        };
        binding.record_owner_flight(active_key, &active_frame);

        let (ack_data_path_commands, mut ack_data_path_rx) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                UnderlayProtocol::Udp,
                PathId(1),
                ack_data_path_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                payload_bytes,
            ),
            ResponseStreamAttachOutcome::Attached
        );
        let ack_data_path_key = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        binding.update_path_metrics(
            ack_data_path_key,
            PathMetrics {
                path_id: ack_data_path_key.path_id,
                underlay: ack_data_path_key.underlay,
                direction: PathMetricDirection::ServerToClient,
                metric_epoch: metric_epoch_now(),
                metric_age_us: 0,
                min_rtt_us: 20_000,
                srtt_us: 20_000,
                rttvar_us: 1_000,
                jitter_us: 1_000,
                delivery_rate_bps: default_path_rate_bps(UnderlayProtocol::Udp).round() as u64,
                pacing_rate_bps: default_path_rate_bps(UnderlayProtocol::Udp).round() as u64,
                loss_ppm: 0,
                ecn_ppm: 0,
                loss_observed: false,
                ecn_observed: false,
                bytes_in_flight: 0,
                queue_bytes: 0,
                inflight_limit_bytes: payload_bytes as u64,
                inflight_hi_bytes: payload_bytes as u64,
                confidence_ppm: 0,
                app_limited: true,
                has_ack_derived_data_sample: true,
                data_sample_count: 0,
                data_sample_bytes: 0,
            },
            ServerPathMetricsSource::LocalSender,
        );
        let (_frames_tx, frames_rx) = mpsc::channel(1);
        let stream = ReliablePathStream {
            stream_id: StreamId(7),
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: UnderlayProtocol::Udp,
            max_frame_payload_bytes: payload_bytes,
            output: ReliablePathStreamOutput::Switchable(binding.clone()),
            frames: frames_rx,
        };
        let plan = plan_response_data_dispatch(
            &stream,
            FlowLane::Throughput,
            payload_bytes as u64,
            payload_bytes,
        )
        .expect("active owner should remain dispatchable");
        assert_eq!(plan.primary_key(), Some(active_key));
        assert_eq!(
            plan.primary_role(),
            PathRuntimeRole::Service,
            "validation paths must not receive unique owner data while lower bytes are unresolved"
        );

        let service_frame = Frame::StreamData {
            stream_id: StreamId(7),
            offset: payload_bytes as u64,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![4_u8; payload_bytes]),
        };
        let outcome =
            emit_planned_response_data_frame(&stream, plan, service_frame, FlowLane::Throughput)
                .await
                .expect("service owner data should emit");

        assert_eq!(outcome.selected_path, Some(active_key));
        assert_eq!(
            binding.ordered_data_owner(),
            Some(active_key),
            "service owner remains the ordinary lead"
        );
        while let Some(_command) = try_recv_reliable_path_command(&mut ack_data_path_rx) {}
        let lower = binding.lower_flights_before_offset((payload_bytes * 2) as u64);
        assert!(!lower.iter().any(|flight| flight.key == ack_data_path_key));
    }

    #[test]
    fn single_response_carrier_uses_sliding_window_not_multipath_ordering_debt() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let assigned_bytes = bulk_service_feed_reservoir_payload_bytes(payload_bytes, mux_limits)
            .saturating_sub(payload_bytes);
        let mut target = response_target(
            0,
            UnderlayProtocol::Tcp,
            5.0,
            assigned_bytes as u64,
            16 * 1024 * 1024,
            true,
        );
        target.snapshot.product_progress_rate_bps = Some(10_000_000_000.0);
        let lower_flights = vec![CarrierPathFlightDebt {
            key: target.key,
            bytes: assigned_bytes as u64,
        }];

        let selected = choose_response_sender_data_target(
            std::slice::from_ref(&target),
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &lower_flights,
            Some(target.key),
        )
        .expect("single carrier lower flight is normal sliding-window debt");

        assert_eq!(selected.key, target.key);
    }

    #[test]
    fn proven_udp_candidate_cannot_overtake_large_lower_owner() {
        let mut owner = response_target(
            0,
            UnderlayProtocol::Udp,
            80.0,
            2 * 1024 * 1024,
            16 * 1024 * 1024,
            true,
        );
        owner.snapshot.product_progress_rate_bps = Some(500_000_000.0);
        let alternate = response_target(1, UnderlayProtocol::Udp, 7.0, 0, 16 * 1024 * 1024, false);
        let lower_flights = vec![CarrierPathFlightDebt {
            key: owner.key,
            bytes: 2 * 1024 * 1024,
        }];

        let selected = choose_response_sender_data_target(
            &[owner.clone(), alternate.clone()],
            FlowLane::Throughput,
            64 * 1024,
            MuxLimits::default(),
            &lower_flights,
            Some(owner.key),
        )
        .expect("lower owner should remain eligible while it owns unresolved lower bytes");

        assert_eq!(selected.key, owner.key);
    }

    #[test]
    fn proven_udp_candidate_waits_even_when_lower_owner_debt_is_within_reorder_budget() {
        let mut owner = response_target(
            0,
            UnderlayProtocol::Udp,
            80.0,
            2 * 1024 * 1024,
            16 * 1024 * 1024,
            true,
        );
        owner.snapshot.product_progress_rate_bps = Some(500_000_000.0);
        let lower_eta_alternate =
            response_target(1, UnderlayProtocol::Udp, 7.0, 0, 16 * 1024 * 1024, false);
        let lower_flights = vec![CarrierPathFlightDebt {
            key: owner.key,
            bytes: 64 * 1024,
        }];

        let selected = choose_response_sender_data_target(
            &[owner.clone(), lower_eta_alternate],
            FlowLane::Throughput,
            64 * 1024,
            MuxLimits::default(),
            &lower_flights,
            Some(owner.key),
        )
        .expect("lower owner should remain eligible while the frontier is not clear");

        assert_eq!(selected.key.path_id, PathId(0));
    }

    #[test]
    fn proof_only_udp_candidate_is_blocked_from_unique_data_with_lower_udp_owner() {
        let mut owner = response_target(
            0,
            UnderlayProtocol::Udp,
            80.0,
            2 * 1024 * 1024,
            16 * 1024 * 1024,
            true,
        );
        owner.snapshot.product_progress_rate_bps = Some(500_000_000.0);
        let mut proof_only =
            response_target(1, UnderlayProtocol::Udp, 7.0, 0, 16 * 1024 * 1024, false);
        proof_only.has_bulk_rate_evidence = false;
        let lower_flights = vec![CarrierPathFlightDebt {
            key: owner.key,
            bytes: 64 * 1024,
        }];

        let selected = choose_response_sender_data_target(
            &[owner.clone(), proof_only],
            FlowLane::Throughput,
            64 * 1024,
            MuxLimits::default(),
            &lower_flights,
            Some(owner.key),
        )
        .expect("proof-only path should not own unique later bytes");

        assert_eq!(selected.key, owner.key);
    }

    #[test]
    fn proof_only_tcp_candidate_does_not_displace_bulk_rate_proven_udp() {
        let bulk_proven_udp =
            response_target(0, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, true);
        let mut proof_only_tcp =
            response_target(0, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);
        proof_only_tcp.has_sender_evidence = true;
        proof_only_tcp.has_bulk_rate_evidence = false;

        let selected = choose_response_sender_data_target(
            &[bulk_proven_udp.clone(), proof_only_tcp],
            FlowLane::Throughput,
            64 * 1024,
            MuxLimits::default(),
            &[],
            Some(bulk_proven_udp.key),
        )
        .expect("bulk-rate-proven path should remain unique ordered owner");

        assert_eq!(selected.key, bulk_proven_udp.key);
    }

    #[test]
    fn response_clear_frontier_keeps_feedable_service_ahead_of_lower_eta_subflow() {
        let lead = response_target(0, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, true);
        let lower_eta_alternate =
            response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);

        let selected = choose_response_sender_data_target(
            &[lead.clone(), lower_eta_alternate.clone()],
            FlowLane::Throughput,
            64 * 1024,
            MuxLimits::default(),
            &[],
            Some(lead.key),
        )
        .expect("feedable Service should remain selected");

        assert_eq!(selected.key, lead.key);
    }

    #[test]
    fn feedable_service_precedes_lower_eta_same_family_subflow() {
        let service = response_target(0, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, true);
        let lower_eta_subflow =
            response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), lower_eta_subflow.clone()],
            FlowLane::Throughput,
            64 * 1024,
            MuxLimits::default(),
            &[],
            Some(service.key),
            0,
            None,
        )
        .expect("feedable Service should remain selected ahead of admitted overflow");

        assert_eq!(selected.target.key, service.key);
        assert_eq!(
            selected.admission.role,
            PathRuntimeRole::Service,
            "a lower-ETA Subflow remains eligible overflow and does not displace feedable Service"
        );
    }

    #[test]
    fn same_family_lower_frontier_owner_remains_subflow() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);

        for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
            let service = response_target(1, underlay, 50.0, 0, 16 * 1024 * 1024, true);
            let lower_owner = response_target(0, underlay, 5.0, 0, 16 * 1024 * 1024, false);
            let lower_flights = [CarrierPathFlightDebt {
                key: lower_owner.key,
                bytes: payload_bytes as u64,
            }];

            let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
                &[service.clone(), lower_owner.clone()],
                FlowLane::Throughput,
                payload_bytes,
                mux_limits,
                &lower_flights,
                Some(service.key),
                payload_bytes.saturating_mul(2),
                None,
            )
            .expect("measured lower-frontier owner should remain dispatchable as a Subflow");

            assert_eq!(selected.target.key, lower_owner.key, "{underlay:?}");
            assert_eq!(selected.admission.role, PathRuntimeRole::Subflow);
            assert_eq!(
                selected.subflow_set_commit.map(|commit| commit.service),
                Some(service.key),
                "{underlay:?} lower-frontier continuation must retain the Service anchor"
            );
        }
    }

    #[test]
    fn cross_family_lower_frontier_owner_remains_subflow() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let service = response_target(1, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, true);
        let lower_owner =
            response_target(0, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
        let lower_flights = [CarrierPathFlightDebt {
            key: lower_owner.key,
            bytes: payload_bytes as u64,
        }];

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), lower_owner.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &lower_flights,
            Some(service.key),
            payload_bytes.saturating_mul(2),
            None,
        )
        .expect("measured cross-family lower-frontier owner should remain dispatchable");

        assert_eq!(selected.target.key, lower_owner.key);
        assert_eq!(selected.admission.role, PathRuntimeRole::Subflow);
        assert_eq!(
            selected.subflow_set_commit.map(|commit| commit.service),
            Some(service.key),
            "cross-family continuation must not commit an implicit Service migration"
        );
    }

    #[test]
    fn authoritative_lower_frontier_suspends_unmeasured_startup_sampling() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);

        for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
            let service = response_target(1, underlay, 50.0, 0, 16 * 1024 * 1024, true);
            let mut proof_only = response_target(0, underlay, 5.0, 0, 16 * 1024 * 1024, false);
            proof_only.has_bulk_rate_evidence = false;
            let lower_flights = [CarrierPathFlightDebt {
                key: proof_only.key,
                bytes: payload_bytes as u64,
            }];

            let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
                &[service.clone(), proof_only],
                FlowLane::Throughput,
                payload_bytes,
                mux_limits,
                &lower_flights,
                Some(service.key),
                payload_bytes.saturating_mul(2),
                None,
            );

            assert!(
                selected.is_none(),
                "{underlay:?} sender evidence alone must not extend an ACK hole"
            );
        }
    }

    #[test]
    fn slow_measured_lower_frontier_cannot_borrow_service_admission() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);

        for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
            let service = response_target(1, underlay, 5.0, 0, 16 * 1024 * 1024, true);
            let mut slow_lower_owner =
                response_target(0, underlay, 500.0, 0, 16 * 1024 * 1024, false);
            slow_lower_owner.snapshot.delivery_rate_bps = 20_000_000.0;
            slow_lower_owner.snapshot.pacing_rate_bps = 20_000_000.0;
            slow_lower_owner.snapshot.product_progress_rate_bps = Some(20_000_000.0);
            let lower_flights = [CarrierPathFlightDebt {
                key: slow_lower_owner.key,
                bytes: payload_bytes as u64,
            }];

            let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
                &[service.clone(), slow_lower_owner],
                FlowLane::Throughput,
                payload_bytes,
                mux_limits,
                &lower_flights,
                Some(service.key),
                payload_bytes.saturating_mul(2),
                None,
            );

            assert!(
                selected.is_none(),
                "{underlay:?} lower ownership is not permission to borrow Service admission"
            );
        }
    }

    #[test]
    fn backpressured_service_remains_lower_frontier_completion_baseline() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut service =
            response_target(1, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, true);
        let (service_commands, _service_receivers) = reliable_path_command_channels(1);
        service_commands
            .try_enqueue_stream_ordered_frame(
                Frame::StreamData {
                    stream_id: StreamId(901),
                    offset: 0,
                    flags: StreamFlags::NONE,
                    payload: Bytes::from_static(b"queued"),
                },
                FlowLane::Throughput,
            )
            .expect("test setup should fill the Service data queue");
        service.commands = service_commands;
        let lower_owner =
            response_target(0, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
        let lower_flights = [CarrierPathFlightDebt {
            key: lower_owner.key,
            bytes: payload_bytes as u64,
        }];

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), lower_owner.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &lower_flights,
            Some(service.key),
            payload_bytes.saturating_mul(2),
            None,
        )
        .expect("measured lower-frontier Subflow should be evaluated against queued Service");

        assert_eq!(selected.target.key, lower_owner.key);
        assert_eq!(selected.admission.role, PathRuntimeRole::Subflow);
        assert_eq!(
            selected.subflow_set_commit.map(|commit| commit.service),
            Some(service.key)
        );
    }

    #[test]
    fn detached_service_with_lower_frontier_waits_for_repair_or_ack_clear() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let lower_owner =
            response_target(0, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
        let lower_flights = [CarrierPathFlightDebt {
            key: lower_owner.key,
            bytes: payload_bytes as u64,
        }];

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            std::slice::from_ref(&lower_owner),
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &lower_flights,
            None,
            payload_bytes.saturating_mul(2),
            None,
        );

        assert!(
            selected.is_none(),
            "a lower-hole owner cannot infer Service authority after the anchor detaches"
        );
    }

    #[test]
    fn clear_frontier_unavailable_ordered_owner_reanchors_service_to_bulk_proven_path() {
        let (service_commands, _service_receivers) = reliable_path_command_channels(1);
        let mut service_snapshot =
            PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 50.0, 500_000_000.0);
        service_snapshot.inflight_limit_bytes = 16 * 1024 * 1024;
        service_snapshot.confidence = 1.0;
        let service = ResponseSenderPathTarget {
            #[cfg(feature = "lab-diagnostics")]
            session_id: SessionId(0),
            #[cfg(feature = "lab-diagnostics")]
            binding_instance_id: 0,
            key: CarrierPathKey {
                underlay: UnderlayProtocol::Tcp,
                path_id: PathId(1),
            },
            incarnation: 1,
            commands: service_commands,
            attachment_role: StreamOpenRole::Active,
            snapshot: service_snapshot,
            owner_data_in_flight_bytes: 0,
            command_pending_bytes: 0,
            eta_ms: 50.0,
            is_active: true,
            is_request_active: true,
            has_sender_evidence: true,
            has_bulk_rate_evidence: true,
            ack_clock_calibration_eligible: false,
            ack_clock_calibration_proven: false,
            ack_clock_calibration_spent_bytes: 0,
            ack_clock_calibration_credit_limit_bytes: 0,
            ack_clock_calibration_max_limit_bytes: 0,
            ack_clock_calibration_active: false,
        };
        service
            .commands
            .try_enqueue_stream_ordered_frame(
                Frame::StreamData {
                    stream_id: StreamId(900),
                    offset: 0,
                    flags: StreamFlags::NONE,
                    payload: Bytes::from_static(b"x"),
                },
                FlowLane::Throughput,
            )
            .expect("test setup should fill the service data queue");
        let lower_eta_subflow =
            response_target(2, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), lower_eta_subflow.clone()],
            FlowLane::Throughput,
            64 * 1024,
            MuxLimits::default(),
            &[],
            Some(service.key),
            0,
            None,
        )
        .expect("bulk-rate-proven alternate should become Service when the prior clear-frontier owner is not dispatchable");

        assert_eq!(selected.target.key, lower_eta_subflow.key);
        assert_eq!(
            selected.admission.role,
            PathRuntimeRole::Service,
            "a clear-frontier owner hint is not a permanent Service anchor when that output cannot enqueue owner bytes"
        );
    }

    #[test]
    fn lower_eta_same_family_subflow_does_not_borrow_active_service_envelope() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let service = response_target(0, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, true);
        let mut saturated_subflow =
            response_target(1, UnderlayProtocol::Udp, 5.0, 0, 512 * 1024, false);
        saturated_subflow.snapshot.product_bytes_in_flight =
            RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES;
        saturated_subflow.snapshot.bytes_in_flight = RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES;

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), saturated_subflow],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service.key),
            0,
            None,
        )
        .expect("Service should remain eligible when the lower-ETA Subflow is out of credit");

        assert_eq!(
            selected.target.key, service.key,
            "non-active Subflow admission must use additional-path gates instead of the active Service envelope"
        );
        assert_eq!(selected.admission.role, PathRuntimeRole::Service);
    }

    #[test]
    fn response_ordinary_bulk_keeps_lead_only_inside_measured_hysteresis() {
        let mut lead = response_target(0, UnderlayProtocol::Udp, 5.1, 0, 16 * 1024 * 1024, true);
        let mut lower_eta_alternate =
            response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
        lead.snapshot.jitter_ms = 0.2;
        lower_eta_alternate.snapshot.jitter_ms = 0.1;

        let selected = choose_response_sender_data_target(
            &[lead.clone(), lower_eta_alternate],
            FlowLane::Throughput,
            64 * 1024,
            MuxLimits::default(),
            &[],
            Some(lead.key),
        )
        .expect("near-tie lead should remain selected inside observed jitter");

        assert_eq!(selected.key, lead.key);
    }

    #[test]
    fn active_service_remains_admissible_lead_when_subflow_is_not_admissible() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut service =
            response_target(0, UnderlayProtocol::Tcp, 25.0, 0, 16 * 1024 * 1024, true);
        service.has_bulk_rate_evidence = false;
        let mut subflow = response_target(
            1,
            UnderlayProtocol::Tcp,
            5.0,
            mux_limits.max_path_flight_bytes as u64,
            16 * 1024 * 1024,
            false,
        );
        subflow.has_bulk_rate_evidence = true;
        let candidates = [&service, &subflow];

        let lead = choose_response_admissible_lead(
            &candidates,
            Some(&service),
            mux_limits,
            payload_bytes,
            &[],
            false,
        )
        .expect("active Service must remain a lead candidate when optional Subflow is blocked");

        assert_eq!(
            lead.key, service.key,
            "optional bulk-rate evidence must not hide the current Service owner"
        );
    }

    #[test]
    fn active_service_remains_lead_when_measured_subflow_has_lower_eta() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let service = response_target(0, UnderlayProtocol::Tcp, 25.0, 0, 16 * 1024 * 1024, true);
        let measured_subflow =
            response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);
        let candidates = [&service, &measured_subflow];

        let lead = choose_response_admissible_lead(
            &candidates,
            Some(&service),
            mux_limits,
            payload_bytes,
            &[],
            false,
        )
        .expect("active Service should remain the lead anchor");

        assert_eq!(
            lead.key, service.key,
            "a lower-ETA same-family Subflow must not redefine Service ownership"
        );
    }

    #[test]
    fn feedable_service_owner_is_selected_before_lower_eta_same_family_subflow() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut service =
            response_target(0, UnderlayProtocol::Udp, 25.0, 0, 16 * 1024 * 1024, true);
        service.snapshot.product_progress_rate_bps = Some(80_000_000.0);
        service.has_sender_evidence = true;
        service.has_bulk_rate_evidence = true;

        let mut measured_subflow =
            response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
        measured_subflow.snapshot.product_progress_rate_bps = Some(120_000_000.0);
        measured_subflow.snapshot.app_limited = false;
        measured_subflow.has_sender_evidence = true;
        measured_subflow.has_bulk_rate_evidence = true;

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), measured_subflow],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service.key),
            0,
            None,
        )
        .expect("feedable Service owner should remain dispatchable");

        assert_eq!(
            selected.target.key, service.key,
            "same-family Subflow OwnerData is additive; it must not replace a feedable Service quantum just because its instantaneous ETA is lower"
        );
        assert_eq!(selected.admission.role, PathRuntimeRole::Service);
    }

    #[test]
    fn measured_tcp_subflow_uses_only_the_reservoir_beyond_service_horizon() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let service_horizon = bulk_service_horizon_payload_bytes(payload_bytes, mux_limits);
        let mut service = response_target(
            0,
            UnderlayProtocol::Tcp,
            25.0,
            service_horizon.saturating_sub(payload_bytes) as u64,
            mux_limits.max_path_flight_bytes as u64,
            true,
        );
        service.snapshot.product_progress_rate_bps = Some(80_000_000.0);
        let mut measured_subflow = response_target(
            1,
            UnderlayProtocol::Tcp,
            5.0,
            0,
            mux_limits.max_path_flight_bytes as u64,
            false,
        );
        measured_subflow.snapshot.product_progress_rate_bps = Some(120_000_000.0);
        measured_subflow.snapshot.srtt_ms = 80.0;
        measured_subflow.snapshot.min_rtt_ms = 80.0;
        measured_subflow.snapshot.delivery_rate_bps = 200_000_000.0;
        measured_subflow.snapshot.pacing_rate_bps = 200_000_000.0;
        measured_subflow.snapshot.app_limited = false;

        let below_horizon = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), measured_subflow.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service.key),
            service_horizon.saturating_sub(payload_bytes),
            None,
        )
        .expect("Service should fill its protected horizon first");
        assert_eq!(below_horizon.target.key, service.key);
        assert_eq!(below_horizon.admission.role, PathRuntimeRole::Service);

        service.snapshot.product_bytes_in_flight = service_horizon as u64;
        service.owner_data_in_flight_bytes = service_horizon as u64;
        let reservoir_subflow = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), measured_subflow.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service.key),
            service_horizon,
            None,
        )
        .expect("measured TCP Subflow should use the remaining source reservoir");
        assert_eq!(reservoir_subflow.target.key, measured_subflow.key);
        assert_eq!(reservoir_subflow.admission.role, PathRuntimeRole::Subflow);
        assert_eq!(
            reservoir_subflow
                .subflow_set_commit
                .map(|commit| commit.service),
            Some(service.key),
            "overflow must remain bound to the exact current Service epoch"
        );

        let feed_reservoir = bulk_service_feed_reservoir_payload_bytes(payload_bytes, mux_limits);
        let exhausted_reservoir = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), measured_subflow],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service.key),
            feed_reservoir,
            None,
        )
        .expect("Service remains the liveness fallback at the reservoir boundary");
        assert_eq!(exhausted_reservoir.target.key, service.key);
        assert_eq!(exhausted_reservoir.admission.role, PathRuntimeRole::Service);
    }

    #[test]
    fn tcp_reservoir_does_not_charge_service_horizon_to_low_bdp_subflow() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let service_horizon = bulk_service_horizon_payload_bytes(payload_bytes, mux_limits);
        let mut service = response_target(
            0,
            UnderlayProtocol::Tcp,
            25.0,
            service_horizon as u64,
            mux_limits.max_path_flight_bytes as u64,
            true,
        );
        service.snapshot.product_progress_rate_bps = Some(80_000_000.0);

        let mut low_bdp_subflow = response_target(
            1,
            UnderlayProtocol::Tcp,
            5.0,
            0,
            mux_limits.max_path_flight_bytes as u64,
            false,
        );
        low_bdp_subflow.snapshot.product_progress_rate_bps = Some(54_016_000.0);
        low_bdp_subflow.snapshot.delivery_rate_bps = 54_016_000.0;
        low_bdp_subflow.snapshot.pacing_rate_bps = 54_016_000.0;
        low_bdp_subflow.snapshot.srtt_ms = 137.968;
        low_bdp_subflow.snapshot.min_rtt_ms = 137.968;
        low_bdp_subflow.snapshot.app_limited = false;

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), low_bdp_subflow.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service.key),
            service_horizon,
            None,
        )
        .expect("Service or its measured TCP Subflow must remain feedable");

        assert_eq!(
            selected.target.key, low_bdp_subflow.key,
            "the connection-level Service horizon consumes global reservoir credit once; it is not candidate-local BDP flight"
        );
        assert_eq!(selected.admission.role, PathRuntimeRole::Subflow);
    }

    #[test]
    fn tcp_reservoir_subtracts_only_unique_owner_not_queue_or_repair() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let service_horizon = bulk_service_horizon_payload_bytes(payload_bytes, mux_limits);
        let overflow = 1024 * 1024;
        let candidate_owner_bytes = 128 * 1024;
        let candidate_product_copies = 2 * 1024 * 1024;
        let service = response_target(
            0,
            UnderlayProtocol::Tcp,
            25.0,
            service_horizon as u64,
            mux_limits.max_path_flight_bytes as u64,
            true,
        );
        let mut candidate = response_target(
            1,
            UnderlayProtocol::Tcp,
            5.0,
            candidate_product_copies,
            mux_limits.max_path_flight_bytes as u64,
            false,
        );
        candidate.owner_data_in_flight_bytes = candidate_owner_bytes;
        candidate.snapshot.queue_bytes = (3 * 1024 * 1024) as u64;
        let tail = ResponseOrderedTail::new(Some(service.key), service_horizon + overflow);
        let reservoir = ResponseTcpReservoir::new(
            service.key,
            tail,
            service_horizon as u64,
            service_horizon,
            bulk_service_feed_reservoir_payload_bytes(payload_bytes, mux_limits),
            payload_bytes,
        )
        .expect("global reservoir has credit");

        let debt = response_tcp_reservoir_candidate_debt(reservoir, &candidate);
        assert_eq!(
            debt.external_bytes(),
            (overflow - candidate_owner_bytes as usize) as u64
        );
        assert_eq!(
            debt.external_bytes() + candidate.snapshot.product_bytes_in_flight,
            (overflow + candidate_product_copies as usize - candidate_owner_bytes as usize) as u64,
            "shared queue pressure and duplicate RepairData cannot erase unique tail exposure"
        );
    }

    #[test]
    fn tcp_reservoir_requires_unique_service_owner_horizon() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let service_horizon = bulk_service_horizon_payload_bytes(payload_bytes, mux_limits);
        let mut service = response_target(
            0,
            UnderlayProtocol::Tcp,
            25.0,
            service_horizon as u64,
            mux_limits.max_path_flight_bytes as u64,
            true,
        );
        service.owner_data_in_flight_bytes = payload_bytes as u64;
        service.snapshot.queue_bytes = service_horizon as u64;
        service.snapshot.product_progress_rate_bps = Some(80_000_000.0);

        let mut measured_subflow = response_target(
            1,
            UnderlayProtocol::Tcp,
            5.0,
            0,
            mux_limits.max_path_flight_bytes as u64,
            false,
        );
        measured_subflow.snapshot.product_progress_rate_bps = Some(200_000_000.0);
        measured_subflow.snapshot.delivery_rate_bps = 200_000_000.0;
        measured_subflow.snapshot.pacing_rate_bps = 200_000_000.0;
        measured_subflow.snapshot.app_limited = false;

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), measured_subflow],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service.key),
            service_horizon,
            None,
        )
        .expect("Service remains the fallback until its unique quota is assigned");

        assert_eq!(selected.target.key, service.key);
        assert_eq!(selected.admission.role, PathRuntimeRole::Service);
    }

    #[test]
    fn tcp_reservoir_split_derives_reduced_resource_geometry() {
        let mut mux_limits = MuxLimits::default();
        let resource_limit = 4 * 1024 * 1024;
        mux_limits.max_path_flight_bytes = resource_limit;
        mux_limits.max_repair_bytes = resource_limit;
        mux_limits.max_reorder_bytes = resource_limit;
        mux_limits.max_stream_window_bytes = resource_limit as u64;
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let service_horizon = bulk_service_horizon_payload_bytes(payload_bytes, mux_limits);
        let feed_reservoir = bulk_service_feed_reservoir_payload_bytes(payload_bytes, mux_limits);
        assert!(
            service_horizon
                < bulk_service_horizon_payload_bytes(payload_bytes, MuxLimits::default())
        );
        assert!(feed_reservoir <= resource_limit);

        let service = response_target(
            0,
            UnderlayProtocol::Tcp,
            25.0,
            service_horizon as u64,
            resource_limit as u64,
            true,
        );
        let mut measured_subflow = response_target(
            1,
            UnderlayProtocol::Tcp,
            5.0,
            0,
            resource_limit as u64,
            false,
        );
        measured_subflow.snapshot.srtt_ms = 80.0;
        measured_subflow.snapshot.min_rtt_ms = 80.0;
        measured_subflow.snapshot.delivery_rate_bps = 200_000_000.0;
        measured_subflow.snapshot.pacing_rate_bps = 200_000_000.0;
        measured_subflow.snapshot.app_limited = false;

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), measured_subflow.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service.key),
            service_horizon,
            None,
        )
        .expect("reduced valid resources should retain the derived TCP split");
        assert_eq!(selected.target.key, measured_subflow.key);
        assert_eq!(selected.admission.role, PathRuntimeRole::Subflow);
    }

    #[test]
    fn tcp_reservoir_split_yields_to_latency_and_calibration_fences() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let service_horizon = bulk_service_horizon_payload_bytes(payload_bytes, mux_limits);
        let mut service = response_target(
            0,
            UnderlayProtocol::Tcp,
            25.0,
            service_horizon as u64,
            mux_limits.max_path_flight_bytes as u64,
            true,
        );
        let mut measured_subflow = response_target(
            1,
            UnderlayProtocol::Tcp,
            5.0,
            0,
            mux_limits.max_path_flight_bytes as u64,
            false,
        );
        measured_subflow.snapshot.srtt_ms = 80.0;
        measured_subflow.snapshot.min_rtt_ms = 80.0;
        measured_subflow.snapshot.delivery_rate_bps = 200_000_000.0;
        measured_subflow.snapshot.pacing_rate_bps = 200_000_000.0;
        measured_subflow.snapshot.app_limited = false;

        service.snapshot.active_latency_sensitive_flows = 1;
        let path_pressure = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), measured_subflow.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service.key),
            service_horizon,
            None,
        )
        .expect("Service stays live under path-local latency pressure");
        assert_eq!(path_pressure.target.key, service.key);

        service.snapshot.active_latency_sensitive_flows = 0;
        service.snapshot.session_active_latency_sensitive_flows = 1;
        let session_pressure = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), measured_subflow.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service.key),
            service_horizon,
            None,
        )
        .expect("Service stays live under session latency pressure");
        assert_eq!(session_pressure.target.key, service.key);

        service.snapshot.session_active_latency_sensitive_flows = 0;
        measured_subflow.ack_clock_calibration_eligible = true;
        measured_subflow.ack_clock_calibration_active = true;
        measured_subflow.ack_clock_calibration_proven = true;
        measured_subflow.ack_clock_calibration_spent_bytes =
            reliable_ack_clock_calibration_limit_bytes(mux_limits);
        measured_subflow.ack_clock_calibration_credit_limit_bytes =
            measured_subflow.ack_clock_calibration_spent_bytes;
        measured_subflow.ack_clock_calibration_max_limit_bytes =
            measured_subflow.ack_clock_calibration_spent_bytes;
        let calibration_fence = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), measured_subflow],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service.key),
            service_horizon,
            None,
        )
        .expect("Service remains available while exact calibration flights drain");
        assert_eq!(calibration_fence.target.key, service.key);
        assert_eq!(calibration_fence.admission.role, PathRuntimeRole::Service);
    }

    #[test]
    fn tcp_reservoir_waits_for_binding_calibration_tail() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let service_horizon = bulk_service_horizon_payload_bytes(payload_bytes, mux_limits);
        let mut service = response_target(
            0,
            UnderlayProtocol::Tcp,
            25.0,
            service_horizon as u64,
            mux_limits.max_path_flight_bytes as u64,
            true,
        );
        service.snapshot.product_progress_rate_bps = Some(80_000_000.0);

        let mut proven = response_target(
            1,
            UnderlayProtocol::Tcp,
            5.0,
            0,
            mux_limits.max_path_flight_bytes as u64,
            false,
        );
        proven.snapshot.product_progress_rate_bps = Some(200_000_000.0);
        proven.snapshot.delivery_rate_bps = 200_000_000.0;
        proven.snapshot.pacing_rate_bps = 200_000_000.0;
        proven.snapshot.app_limited = false;

        let mut calibrating = response_target(
            2,
            UnderlayProtocol::Tcp,
            10.0,
            0,
            mux_limits.max_path_flight_bytes as u64,
            false,
        );
        let stage = reliable_ack_clock_calibration_limit_bytes(mux_limits);
        calibrating.ack_clock_calibration_eligible = true;
        calibrating.ack_clock_calibration_active = true;
        calibrating.ack_clock_calibration_spent_bytes = stage;
        calibrating.ack_clock_calibration_credit_limit_bytes = stage;
        calibrating.ack_clock_calibration_max_limit_bytes = 2 * stage;

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), proven, calibrating],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service.key),
            service_horizon,
            None,
        )
        .expect("Service remains available while calibration waits for ACK evidence");

        assert_eq!(selected.target.key, service.key);
        assert_eq!(selected.admission.role, PathRuntimeRole::Service);
    }

    #[test]
    fn udp_service_remains_first_after_its_service_horizon() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let service_horizon = bulk_service_horizon_payload_bytes(payload_bytes, mux_limits);
        let service = response_target(
            0,
            UnderlayProtocol::Udp,
            25.0,
            service_horizon as u64,
            mux_limits.max_path_flight_bytes as u64,
            true,
        );
        let measured_subflow = response_target(
            1,
            UnderlayProtocol::Udp,
            5.0,
            0,
            mux_limits.max_path_flight_bytes as u64,
            false,
        );

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), measured_subflow],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service.key),
            service_horizon,
            None,
        )
        .expect("UDP Service remains the packet-controller owner policy");
        assert_eq!(selected.target.key, service.key);
        assert_eq!(selected.admission.role, PathRuntimeRole::Service);
    }

    #[test]
    fn unproven_service_bootstraps_before_app_limited_proven_subflow() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut service =
            response_target(0, UnderlayProtocol::Udp, 25.0, 0, 16 * 1024 * 1024, true);
        service.snapshot.product_queue_bytes = (2 * payload_bytes) as u64;
        service.snapshot.app_limited = true;
        service.has_bulk_rate_evidence = false;

        let mut proven_subflow =
            response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
        proven_subflow.snapshot.app_limited = true;
        proven_subflow.has_bulk_rate_evidence = true;

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), proven_subflow],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service.key),
            0,
            None,
        )
        .expect("the unproven live Service remains feedable");

        assert_eq!(selected.target.key, service.key);
        assert_eq!(selected.admission.role, PathRuntimeRole::Service);
    }

    #[test]
    fn feedable_service_precedes_less_committed_app_limited_subflow() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut service =
            response_target(0, UnderlayProtocol::Udp, 25.0, 0, 16 * 1024 * 1024, true);
        service.snapshot.product_queue_bytes = (2 * payload_bytes) as u64;

        let mut underloaded =
            response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
        underloaded.snapshot.app_limited = true;
        underloaded.has_bulk_rate_evidence = true;

        let mut overloaded =
            response_target(2, UnderlayProtocol::Udp, 1.0, 0, 16 * 1024 * 1024, false);
        overloaded.snapshot.product_queue_bytes = (4 * payload_bytes) as u64;
        overloaded.snapshot.app_limited = true;
        overloaded.has_bulk_rate_evidence = true;

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), underloaded, overloaded],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(CarrierPathKey {
                underlay: UnderlayProtocol::Udp,
                path_id: PathId(0),
            }),
            0,
            None,
        )
        .expect("feedable Service remains selected despite more committed work");

        assert_eq!(selected.target.key, service.key);
        assert_eq!(selected.admission.role, PathRuntimeRole::Service);
    }

    #[test]
    fn app_limited_bulk_proven_slow_subflow_still_requires_completion_gain() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut service = response_target(0, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, true);
        service.snapshot.product_progress_rate_bps = Some(120_000_000.0);
        let mut slow_subflow =
            response_target(1, UnderlayProtocol::Udp, 500.0, 0, 16 * 1024 * 1024, false);
        slow_subflow.snapshot.product_progress_rate_bps = Some(20_000_000.0);
        slow_subflow.snapshot.app_limited = true;
        slow_subflow.has_bulk_rate_evidence = true;
        let candidates = [&service, &slow_subflow];
        let lead = ResponseBulkLead {
            key: service.key,
            snapshot: service.snapshot,
            eta_ms: service.eta_ms,
        };

        let admission = response_target_unique_owner_admission(
            &slow_subflow,
            &candidates,
            lead,
            None,
            0,
            payload_bytes,
            mux_limits,
        );

        assert_eq!(admission.decision, PathAdmissionDecision::Standby);
    }

    #[test]
    fn tcp_ack_clock_calibration_rejects_seed_beyond_service_reservoir() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let service = response_target(0, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, true);
        let mut candidate = response_target(
            1,
            UnderlayProtocol::Tcp,
            1_500.0,
            0,
            16 * 1024 * 1024,
            false,
        );
        candidate.snapshot.delivery_rate_bps = 2_000_000.0;
        candidate.snapshot.product_progress_rate_bps = Some(2_000_000.0);
        candidate.snapshot.app_limited = true;
        candidate.ack_clock_calibration_eligible = true;
        let initial_limit = reliable_ack_clock_calibration_limit_bytes(mux_limits);
        candidate.ack_clock_calibration_credit_limit_bytes = initial_limit;
        candidate.ack_clock_calibration_max_limit_bytes = initial_limit.saturating_mul(4);
        let candidates = [&service, &candidate];
        let lead = ResponseBulkLead {
            key: service.key,
            snapshot: service.snapshot,
            eta_ms: service.eta_ms,
        };
        assert_eq!(
            response_target_unique_owner_admission(
                &candidate,
                &candidates,
                lead,
                None,
                0,
                payload_bytes,
                mux_limits,
            )
            .decision,
            PathAdmissionDecision::Standby,
            "the provisional first-RTT rate remains too slow for ordinary ECF admission"
        );

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), candidate.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service.key),
            0,
            None,
        )
        .expect("Service remains available when exploration would create an ordering stall");
        assert_eq!(selected.target.key, service.key);
        assert!(selected.ack_clock_calibration_commit.is_none());
    }

    #[test]
    fn fresh_tcp_calibration_is_dormant_when_multi_flow_start_is_closed() {
        let mut candidate =
            response_target(1, UnderlayProtocol::Tcp, 500.0, 0, 16 * 1024 * 1024, false);
        let (commands, _receivers) = reliable_path_command_channels(8);
        candidate.commands = commands;
        candidate.ack_clock_calibration_eligible = true;
        candidate.ack_clock_calibration_credit_limit_bytes = 256 * 1024;
        candidate.ack_clock_calibration_max_limit_bytes = 64 * 1024 * 1024;

        assert!(response_ack_clock_calibration_pending(&candidate, true));
        assert!(!response_ack_clock_calibration_pending(&candidate, false));
        assert!(response_ack_clock_calibration_blocks_generic_owner(
            &candidate
        ));
    }

    #[test]
    fn tcp_ack_clock_calibration_explores_within_service_reservoir() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut service = response_target(
            0,
            UnderlayProtocol::Tcp,
            1_098.657,
            0,
            16 * 1024 * 1024,
            true,
        );
        service.snapshot.delivery_rate_bps = 18_561_000.0;
        service.snapshot.pacing_rate_bps = 18_561_000.0;
        service.snapshot.srtt_ms = 333.0;
        service.snapshot.min_rtt_ms = 333.0;

        let mut candidate = response_target(
            1,
            UnderlayProtocol::Tcp,
            1_406.704,
            0,
            16 * 1024 * 1024,
            false,
        );
        candidate.snapshot.delivery_rate_bps = 1_007_000.0;
        candidate.snapshot.pacing_rate_bps = 1_007_000.0;
        candidate.snapshot.product_progress_rate_bps = Some(1_007_000.0);
        candidate.snapshot.srtt_ms = 730.287;
        candidate.snapshot.min_rtt_ms = 730.287;
        candidate.snapshot.app_limited = true;
        candidate.ack_clock_calibration_eligible = true;
        let initial_limit = 183_802;
        candidate.ack_clock_calibration_credit_limit_bytes = initial_limit;
        candidate.ack_clock_calibration_max_limit_bytes = 64 * 1024 * 1024;
        let candidates = [&service, &candidate];
        let lead = ResponseBulkLead {
            key: service.key,
            snapshot: service.snapshot,
            eta_ms: service.eta_ms,
        };
        assert_eq!(
            response_target_unique_owner_admission(
                &candidate,
                &candidates,
                lead,
                None,
                0,
                payload_bytes,
                mux_limits,
            )
            .decision,
            PathAdmissionDecision::Standby,
            "the provisional model still cannot claim ordinary ownership"
        );

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), candidate.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service.key),
            0,
            None,
        )
        .expect("bounded exploration should fit behind the Service reservoir");
        assert_eq!(selected.target.key, candidate.key);
        assert_eq!(selected.admission.role, PathRuntimeRole::Subflow);
        assert!(selected.ack_clock_calibration_commit.is_some());

        candidate.ack_clock_calibration_active = true;
        candidate.ack_clock_calibration_spent_bytes = initial_limit;
        candidate.ack_clock_calibration_credit_limit_bytes = initial_limit.saturating_mul(2);
        let grown = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), candidate.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service.key),
            0,
            None,
        )
        .expect("a causally authorized stage continues calibration");
        assert_eq!(grown.target.key, candidate.key);
        assert_eq!(
            grown
                .ack_clock_calibration_commit
                .expect("staged calibration commit")
                .limit_bytes,
            initial_limit.saturating_mul(2)
        );

        candidate.ack_clock_calibration_spent_bytes = initial_limit.saturating_mul(2);
        let awaiting_evidence = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), candidate],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service.key),
            0,
            None,
        )
        .expect("a stage awaiting new ACK evidence returns to Service");
        assert_eq!(awaiting_evidence.target.key, service.key);
        assert!(awaiting_evidence.ack_clock_calibration_commit.is_none());
    }

    #[test]
    fn safe_tcp_calibration_waits_for_repair_carrier_headroom() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let service = response_target(0, UnderlayProtocol::Tcp, 5_000.0, 0, 16 * 1024 * 1024, true);
        let mut candidate =
            response_target(1, UnderlayProtocol::Tcp, 100.0, 0, 16 * 1024 * 1024, false);
        candidate.ack_clock_calibration_eligible = true;
        candidate.ack_clock_calibration_credit_limit_bytes = 256 * 1024;
        candidate.ack_clock_calibration_max_limit_bytes = 64 * 1024 * 1024;
        candidate.snapshot.product_bytes_in_flight = 256 * 1024;
        candidate.owner_data_in_flight_bytes = 0;

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), candidate],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service.key),
            0,
            None,
        )
        .expect("Service remains available while RepairData occupies candidate headroom");

        assert_eq!(selected.target.key, service.key);
        assert!(selected.ack_clock_calibration_commit.is_none());
    }

    #[test]
    fn tcp_ack_clock_calibration_retirement_releases_binding_fences() {
        let fixture = response_calibration_dispatch_fixture(8);
        install_slow_fresh_response_calibration(&fixture);
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
        let generation_before = fixture.binding.subflow_state_snapshot().0;

        let plan =
            plan_response_data_dispatch(&fixture.stream, FlowLane::Throughput, 0, payload_bytes)
                .expect("Service remains available after retiring unsafe exploration");

        assert_eq!(plan.primary_key(), Some(fixture.service));
        assert_ne!(
            fixture.binding.subflow_state_snapshot().0,
            generation_before
        );
        let candidate = fixture
            .binding
            .sender_path_targets(FlowLane::Throughput, payload_bytes)
            .into_iter()
            .find(|target| target.key == fixture.candidate)
            .expect("retired candidate remains attached");
        assert_eq!(candidate.ack_clock_calibration_spent_bytes, 0);
        assert_eq!(candidate.ack_clock_calibration_credit_limit_bytes, 0);
        assert_eq!(candidate.ack_clock_calibration_max_limit_bytes, 0);
        assert!(!candidate.ack_clock_calibration_active);
        assert!(!response_ack_clock_calibration_blocks_generic_owner(
            &candidate
        ));
    }

    #[test]
    fn tcp_ack_clock_calibration_retirement_ignores_repair_only_carrier_debt() {
        let fixture = response_calibration_dispatch_fixture(1);
        install_slow_fresh_response_calibration(&fixture);
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
        let repair = Frame::StreamData {
            stream_id: fixture.stream.stream_id,
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(b"repair-only"),
        };
        fixture
            .candidate_commands
            .try_enqueue_stream_ordered_frame(repair.clone(), FlowLane::Throughput)
            .expect("fill the candidate lane with RepairData");
        fixture
            .binding
            .record_repair_flight(fixture.candidate, &repair);

        let plan = plan_response_data_dispatch(
            &fixture.stream,
            FlowLane::Throughput,
            reliable_stream_frame_payload_bytes(&repair) as u64,
            payload_bytes,
        )
        .expect("RepairData must not preserve a unique-owner calibration fence");

        assert_eq!(plan.primary_key(), Some(fixture.service));
        let candidate = fixture
            .binding
            .sender_path_targets(FlowLane::Throughput, payload_bytes)
            .into_iter()
            .find(|target| target.key == fixture.candidate)
            .expect("candidate remains attached");
        assert_eq!(candidate.owner_data_in_flight_bytes, 0);
        assert!(candidate.snapshot.product_bytes_in_flight > 0);
        assert_eq!(candidate.ack_clock_calibration_credit_limit_bytes, 0);
        assert_eq!(candidate.ack_clock_calibration_max_limit_bytes, 0);
        assert!(!response_ack_clock_calibration_blocks_generic_owner(
            &candidate
        ));
    }

    #[test]
    fn tcp_ack_clock_calibration_retirement_refuses_exact_owner_flight() {
        let fixture = response_calibration_dispatch_fixture(8);
        install_slow_fresh_response_calibration(&fixture);
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
        let candidate = fixture
            .binding
            .sender_path_targets(FlowLane::Throughput, payload_bytes)
            .into_iter()
            .find(|target| target.key == fixture.candidate)
            .expect("fresh calibration candidate");
        fixture.binding.record_owner_flight_for_target(
            &candidate,
            &Frame::StreamData {
                stream_id: fixture.stream.stream_id,
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"stale-owner"),
            },
        );

        let plan =
            plan_response_data_dispatch(&fixture.stream, FlowLane::Throughput, 0, payload_bytes)
                .expect("stale calibration state must fall back without erasing exact flight");

        assert_eq!(plan.primary_key(), Some(fixture.service));
        let candidate = fixture
            .binding
            .sender_path_targets(FlowLane::Throughput, payload_bytes)
            .into_iter()
            .find(|target| target.key == fixture.candidate)
            .expect("candidate remains attached");
        assert!(candidate.ack_clock_calibration_credit_limit_bytes > 0);
        assert!(candidate.ack_clock_calibration_max_limit_bytes > 0);
        assert!(response_ack_clock_calibration_blocks_generic_owner(
            &candidate
        ));
    }

    #[test]
    fn tcp_ack_clock_calibration_retirement_rejects_stale_path_model() {
        let fixture = response_calibration_dispatch_fixture(8);
        install_slow_fresh_response_calibration(&fixture);
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
        let request = response_calibration_retirement_request(&fixture);
        fixture
            .binding
            .set_output_product_model_for_test(fixture.candidate, 500_000_000.0, 10.0);

        assert!(
            !fixture
                .binding
                .try_retire_tcp_ack_clock_calibration(request)
        );
        let candidate = fixture
            .binding
            .sender_path_targets(FlowLane::Throughput, payload_bytes)
            .into_iter()
            .find(|target| target.key == fixture.candidate)
            .expect("candidate remains attached");
        assert!(candidate.ack_clock_calibration_credit_limit_bytes > 0);
    }

    #[test]
    fn tcp_ack_clock_calibration_retirement_rejects_stale_pending_snapshots() {
        let mut fixture = response_calibration_dispatch_fixture(8);
        install_slow_fresh_response_calibration(&fixture);
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());

        let stale_candidate = response_calibration_retirement_request(&fixture);
        fixture
            .candidate_commands
            .try_enqueue_stream_ordered_frame(
                Frame::StreamData {
                    stream_id: fixture.stream.stream_id,
                    offset: 0,
                    flags: StreamFlags::NONE,
                    payload: Bytes::from_static(b"candidate-pending"),
                },
                FlowLane::Throughput,
            )
            .expect("change candidate pending bytes");
        let candidate_command = try_recv_reliable_path_command(&mut fixture.candidate_receivers)
            .expect("drain candidate queue without releasing pending bytes");
        let candidate_pending_bytes = reliable_path_command_pending_bytes(&candidate_command);
        assert!(
            !fixture
                .binding
                .try_retire_tcp_ack_clock_calibration(stale_candidate)
        );
        fixture
            .candidate_receivers
            .release_pending_command_bytes(candidate_pending_bytes);

        let stale_service = response_calibration_retirement_request(&fixture);
        let service = fixture
            .binding
            .sender_path_targets(FlowLane::Throughput, payload_bytes)
            .into_iter()
            .find(|target| target.key == fixture.service)
            .expect("Service target");
        service
            .commands
            .try_enqueue_stream_ordered_frame(
                Frame::StreamData {
                    stream_id: fixture.stream.stream_id,
                    offset: payload_bytes as u64,
                    flags: StreamFlags::NONE,
                    payload: Bytes::from_static(b"service-pending"),
                },
                FlowLane::Throughput,
            )
            .expect("change Service pending bytes");
        let service_command = try_recv_reliable_path_command(&mut fixture.service_receivers)
            .expect("drain Service queue without releasing pending bytes");
        let service_pending_bytes = reliable_path_command_pending_bytes(&service_command);
        assert!(
            !fixture
                .binding
                .try_retire_tcp_ack_clock_calibration(stale_service)
        );
        fixture
            .service_receivers
            .release_pending_command_bytes(service_pending_bytes);

        assert!(fixture.binding.try_retire_tcp_ack_clock_calibration(
            response_calibration_retirement_request(&fixture)
        ));
    }

    #[test]
    fn tcp_response_calibration_does_not_double_count_pending_owner_flight() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let service = response_target(0, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, true);
        let mut candidate =
            response_target(1, UnderlayProtocol::Tcp, 500.0, 0, 16 * 1024 * 1024, false);
        let (commands, _receivers) = reliable_path_command_channels(8);
        candidate.commands = commands;
        let initial_limit = reliable_ack_clock_calibration_limit_bytes(mux_limits);
        let committed = initial_limit - payload_bytes as u64;
        candidate.snapshot.product_bytes_in_flight = committed;
        candidate.ack_clock_calibration_eligible = true;
        candidate.ack_clock_calibration_active = true;
        candidate.ack_clock_calibration_spent_bytes = committed;
        candidate.ack_clock_calibration_credit_limit_bytes = initial_limit;
        candidate.ack_clock_calibration_max_limit_bytes =
            reliable_ack_clock_calibration_ceiling_bytes(mux_limits);
        candidate
            .commands
            .try_enqueue_stream_ordered_frame(
                Frame::StreamData {
                    stream_id: StreamId(991),
                    offset: 0,
                    flags: StreamFlags::NONE,
                    payload: Bytes::from(vec![0x5a; committed as usize]),
                },
                FlowLane::Throughput,
            )
            .expect("mirror the product flight in the carrier queue");
        assert_eq!(candidate.commands.pending_bytes(), committed);
        assert_eq!(
            response_target_assigned_product_bytes(&candidate),
            committed
        );
        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), candidate.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service.key),
            0,
            None,
        )
        .expect("overlapping flight and queue views count as one debt");

        assert_eq!(selected.target.key, candidate.key);
        assert_eq!(selected.admission.role, PathRuntimeRole::Subflow);
        assert_eq!(
            selected
                .ack_clock_calibration_commit
                .expect("calibration commit")
                .limit_bytes,
            initial_limit
        );
    }

    #[test]
    fn tcp_response_calibration_does_not_double_count_global_ordered_tail() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let service = response_target(0, UnderlayProtocol::Tcp, 5.0, 0, 64 * 1024 * 1024, true);
        let mut candidate =
            response_target(1, UnderlayProtocol::Tcp, 500.0, 0, 64 * 1024 * 1024, false);
        let ceiling = reliable_ack_clock_calibration_ceiling_bytes(mux_limits);
        let committed = ceiling - payload_bytes as u64;
        candidate.snapshot.product_bytes_in_flight = committed;
        candidate.ack_clock_calibration_eligible = true;
        candidate.ack_clock_calibration_active = true;
        candidate.ack_clock_calibration_spent_bytes = committed;
        candidate.ack_clock_calibration_credit_limit_bytes = ceiling;
        candidate.ack_clock_calibration_max_limit_bytes = ceiling;

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), candidate.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service.key),
            committed as usize,
            None,
        )
        .expect("the global tail and candidate flight are the same product debt");

        assert_eq!(selected.target.key, candidate.key);
        assert_eq!(
            selected
                .ack_clock_calibration_commit
                .expect("calibration commit")
                .limit_bytes,
            ceiling
        );
    }

    #[test]
    fn tcp_response_startup_does_not_double_count_global_ordered_tail() {
        let mut mux_limits = MuxLimits::default();
        mux_limits.max_path_flight_bytes = 2 * 1024 * 1024;
        mux_limits.max_repair_bytes = 2 * 1024 * 1024;
        mux_limits.max_reorder_bytes = 2 * 1024 * 1024;
        mux_limits.max_stream_window_bytes = 2 * 1024 * 1024;
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let service = response_target(0, UnderlayProtocol::Tcp, 5.0, 0, 2 * 1024 * 1024, true);
        let mut candidate =
            response_target(1, UnderlayProtocol::Tcp, 500.0, 0, 2 * 1024 * 1024, false);
        let committed = 2 * 1024 * 1024 - payload_bytes as u64;
        candidate.snapshot.product_bytes_in_flight = committed;
        candidate.has_bulk_rate_evidence = false;

        assert!(response_target_is_startup_same_underlay_subflow_candidate(
            service.key,
            &service,
            &candidate,
            committed,
            payload_bytes,
            mux_limits,
        ));
    }

    #[tokio::test]
    async fn tcp_response_calibration_dispatch_restores_credit_after_exact_remainder() {
        let mux_limits = MuxLimits::default();
        let normal_payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut fixture = response_calibration_dispatch_fixture(8);
        let mut sender = ServerResponseSenderService::new(SessionId(191), fixture.stream.stream_id);
        sender.enqueue_data_for_lane(
            Bytes::from(vec![0x5a; normal_payload_bytes]),
            FlowLane::Throughput,
        );
        let mut send_stream = ReliableSendStream::new_with_initial_max_offset(
            fixture.stream.stream_id,
            mux_limits,
            u64::MAX,
        );

        let dispatch = sender
            .dispatch_next(
                &fixture.stream,
                &mut send_stream,
                FlowLane::Throughput,
                mux_limits,
            )
            .await
            .expect("the exact residual remains spendable");

        assert_eq!(dispatch.selected_path, Some(fixture.candidate));
        assert_eq!(dispatch.payload_bytes, 4032);
        assert_eq!(fixture.binding.ordered_data_owner(), Some(fixture.service));
        assert!(try_recv_reliable_path_command(&mut fixture.service_receivers).is_none());
        assert!(matches!(
            try_recv_reliable_path_command(&mut fixture.candidate_receivers),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { payload, .. }))
                if payload.len() == 4032
        ));
        let target = fixture
            .binding
            .sender_path_targets(FlowLane::Throughput, normal_payload_bytes)
            .into_iter()
            .find(|target| target.key == fixture.candidate)
            .expect("calibration target");
        assert_eq!(
            target.ack_clock_calibration_spent_bytes,
            target.ack_clock_calibration_credit_limit_bytes
        );
        assert_eq!(sender.data_bytes(), normal_payload_bytes - 4032);

        fixture
            .binding
            .release_normalized_acked_ranges(&[OffsetRange {
                start: 0,
                end: 4032,
            }]);
        let drained = fixture
            .binding
            .sender_path_targets(FlowLane::Throughput, normal_payload_bytes)
            .into_iter()
            .find(|target| target.key == fixture.candidate)
            .expect("drained calibration target");
        assert!(drained.ack_clock_calibration_active);
        assert!(
            drained.ack_clock_calibration_credit_limit_bytes
                > drained.ack_clock_calibration_spent_bytes,
            "exact drain restores bounded credit when no representative strict window was reachable"
        );
        assert!(
            drained.ack_clock_calibration_credit_limit_bytes
                <= drained.ack_clock_calibration_max_limit_bytes
        );
    }

    #[tokio::test]
    async fn active_tcp_calibration_finishes_after_response_flow_count_drops() {
        let mux_limits = MuxLimits::default();
        let normal_payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut fixture = response_calibration_dispatch_fixture(8);
        drop(fixture.second_binding.take());
        assert_eq!(
            fixture
                .binding
                .lane_generation_and_active_response_flows()
                .1,
            1
        );
        let mut sender = ServerResponseSenderService::new(SessionId(191), fixture.stream.stream_id);
        sender.enqueue_data_for_lane(
            Bytes::from(vec![0x5a; normal_payload_bytes]),
            FlowLane::Throughput,
        );
        let mut send_stream = ReliableSendStream::new_with_initial_max_offset(
            fixture.stream.stream_id,
            mux_limits,
            u64::MAX,
        );

        let dispatch = sender
            .dispatch_next(
                &fixture.stream,
                &mut send_stream,
                FlowLane::Throughput,
                mux_limits,
            )
            .await
            .expect("an exact active calibration may finish after the start gate closes");

        assert_eq!(dispatch.selected_path, Some(fixture.candidate));
        assert_eq!(dispatch.payload_bytes, 4032);
        assert!(matches!(
            try_recv_reliable_path_command(&mut fixture.candidate_receivers),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { payload, .. }))
                if payload.len() == 4032
        ));
    }

    #[tokio::test]
    async fn tcp_response_calibration_dispatch_treats_pending_flight_as_one_debt() {
        let mux_limits = MuxLimits::default();
        let normal_payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let stage_limit = reliable_ack_clock_calibration_limit_bytes(mux_limits);
        let committed = stage_limit - 4032;
        let mut fixture = response_calibration_dispatch_fixture(8);
        let overlap = Frame::StreamData {
            stream_id: fixture.stream.stream_id,
            offset: normal_payload_bytes as u64,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![0x5a; committed as usize]),
        };
        fixture
            .binding
            .record_owner_flight(fixture.candidate, &overlap);
        fixture
            .candidate_commands
            .try_enqueue_stream_ordered_frame(overlap, FlowLane::Throughput)
            .expect("mirror the assigned product flight in the carrier queue");

        let mut sender = ServerResponseSenderService::new(SessionId(191), fixture.stream.stream_id);
        sender.enqueue_data_for_lane(
            Bytes::from(vec![0x5a; normal_payload_bytes]),
            FlowLane::Throughput,
        );
        let mut send_stream = ReliableSendStream::new_with_initial_max_offset(
            fixture.stream.stream_id,
            mux_limits,
            u64::MAX,
        );
        let dispatch = sender
            .dispatch_next(
                &fixture.stream,
                &mut send_stream,
                FlowLane::Throughput,
                mux_limits,
            )
            .await
            .expect("overlapping ledger and queue views leave the residual spendable");

        assert_eq!(dispatch.selected_path, Some(fixture.candidate));
        assert_eq!(dispatch.payload_bytes, 4032);
        assert!(matches!(
            try_recv_reliable_path_command(&mut fixture.candidate_receivers),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { payload, .. }))
                if payload.len() == committed as usize
        ));
        assert!(matches!(
            try_recv_reliable_path_command(&mut fixture.candidate_receivers),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { payload, .. }))
                if payload.len() == 4032
        ));
        let target = fixture
            .binding
            .sender_path_targets(FlowLane::Throughput, normal_payload_bytes)
            .into_iter()
            .find(|target| target.key == fixture.candidate)
            .expect("calibration target");
        assert_eq!(
            target.ack_clock_calibration_spent_bytes,
            target.ack_clock_calibration_credit_limit_bytes
        );
    }

    #[tokio::test]
    async fn blocked_tcp_calibration_remainder_keeps_normal_service_chunk() {
        let mux_limits = MuxLimits::default();
        let normal_payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut fixture = response_calibration_dispatch_fixture(1);
        fixture
            .candidate_commands
            .try_enqueue_stream_ordered_frame(
                Frame::StreamData {
                    stream_id: fixture.stream.stream_id,
                    offset: 0,
                    flags: StreamFlags::NONE,
                    payload: Bytes::from_static(b"blocked"),
                },
                FlowLane::Throughput,
            )
            .expect("fill exact calibration candidate queue");
        let mut sender = ServerResponseSenderService::new(SessionId(191), fixture.stream.stream_id);
        sender.enqueue_data_for_lane(
            Bytes::from(vec![0x5a; normal_payload_bytes]),
            FlowLane::Throughput,
        );
        let mut send_stream = ReliableSendStream::new_with_initial_max_offset(
            fixture.stream.stream_id,
            mux_limits,
            u64::MAX,
        );

        let dispatch = sender
            .dispatch_next(
                &fixture.stream,
                &mut send_stream,
                FlowLane::Throughput,
                mux_limits,
            )
            .await
            .expect("blocked calibration falls back to normal Service emission");

        assert_eq!(dispatch.selected_path, Some(fixture.service));
        assert_eq!(dispatch.payload_bytes, normal_payload_bytes);
        assert!(matches!(
            try_recv_reliable_path_command(&mut fixture.service_receivers),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { payload, .. }))
                if payload.len() == normal_payload_bytes
        ));
        assert_eq!(
            fixture
                .binding
                .active_tcp_ack_clock_calibration_remaining_bytes(),
            Some(4032),
            "Service fallback must not spend or repeatedly fragment the candidate's residual credit"
        );
    }

    #[test]
    fn blocked_active_ack_clock_candidate_does_not_select_another_calibration_owner() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let service = response_target(0, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, true);
        let initial_limit = reliable_ack_clock_calibration_limit_bytes(mux_limits);

        let mut active_candidate =
            response_target(1, UnderlayProtocol::Tcp, 500.0, 0, 16 * 1024 * 1024, false);
        let (blocked_commands, _blocked_receivers) = reliable_path_command_channels(1);
        blocked_commands
            .try_enqueue_stream_ordered_frame(
                Frame::StreamData {
                    stream_id: StreamId(901),
                    offset: 0,
                    flags: StreamFlags::NONE,
                    payload: Bytes::from_static(b"x"),
                },
                FlowLane::Throughput,
            )
            .expect("fill active calibration candidate queue");
        active_candidate.commands = blocked_commands;
        active_candidate.ack_clock_calibration_eligible = true;
        active_candidate.ack_clock_calibration_active = true;
        active_candidate.ack_clock_calibration_credit_limit_bytes = initial_limit;
        active_candidate.ack_clock_calibration_max_limit_bytes = initial_limit.saturating_mul(2);

        let mut other_candidate = response_target(
            2,
            UnderlayProtocol::Tcp,
            1_500.0,
            0,
            16 * 1024 * 1024,
            false,
        );
        other_candidate.snapshot.delivery_rate_bps = 2_000_000.0;
        other_candidate.snapshot.product_progress_rate_bps = Some(2_000_000.0);
        other_candidate.snapshot.app_limited = true;
        other_candidate.ack_clock_calibration_eligible = true;
        other_candidate.ack_clock_calibration_credit_limit_bytes = initial_limit;
        other_candidate.ack_clock_calibration_max_limit_bytes = initial_limit.saturating_mul(2);

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), active_candidate, other_candidate],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service.key),
            0,
            None,
        )
        .expect("Service remains feedable while the active calibration path is blocked");
        assert_eq!(selected.target.key, service.key);
        assert!(selected.ack_clock_calibration_commit.is_none());
    }

    #[test]
    fn exhausted_active_calibration_cannot_bypass_saturated_service_via_generic_subflow() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut service = response_target(0, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, true);
        let (blocked_service_commands, _blocked_service_receivers) =
            reliable_path_command_channels(1);
        blocked_service_commands
            .try_enqueue_stream_ordered_frame(
                Frame::StreamData {
                    stream_id: StreamId(902),
                    offset: 0,
                    flags: StreamFlags::NONE,
                    payload: Bytes::from_static(b"x"),
                },
                FlowLane::Throughput,
            )
            .expect("fill Service queue");
        service.commands = blocked_service_commands;

        let initial_limit = reliable_ack_clock_calibration_limit_bytes(mux_limits);
        let mut candidate =
            response_target(1, UnderlayProtocol::Tcp, 1.0, 0, 16 * 1024 * 1024, false);
        candidate.ack_clock_calibration_eligible = true;
        candidate.ack_clock_calibration_active = true;
        candidate.ack_clock_calibration_spent_bytes = initial_limit;
        candidate.ack_clock_calibration_credit_limit_bytes = initial_limit;
        candidate.ack_clock_calibration_max_limit_bytes = initial_limit.saturating_mul(2);

        assert!(
            select_response_sender_data_target_with_ordered_debt_and_epoch(
                &[service.clone(), candidate],
                FlowLane::Throughput,
                payload_bytes,
                mux_limits,
                &[],
                Some(service.key),
                0,
                None,
            )
            .is_none(),
            "generic Subflow selection must not bypass staged credit while Service is blocked"
        );
    }

    #[test]
    fn proven_active_calibration_cannot_reenter_generic_ownership_before_drain() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut service = response_target(0, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, true);
        let (blocked_service_commands, _blocked_service_receivers) =
            reliable_path_command_channels(1);
        blocked_service_commands
            .try_enqueue_stream_ordered_frame(
                Frame::StreamData {
                    stream_id: StreamId(903),
                    offset: 0,
                    flags: StreamFlags::NONE,
                    payload: Bytes::from_static(b"x"),
                },
                FlowLane::Throughput,
            )
            .expect("fill Service queue");
        service.commands = blocked_service_commands;

        let initial_limit = reliable_ack_clock_calibration_limit_bytes(mux_limits);
        let mut candidate =
            response_target(1, UnderlayProtocol::Tcp, 1.0, 0, 16 * 1024 * 1024, false);
        candidate.ack_clock_calibration_eligible = true;
        candidate.ack_clock_calibration_active = true;
        candidate.ack_clock_calibration_proven = true;
        candidate.ack_clock_calibration_spent_bytes = initial_limit;
        candidate.ack_clock_calibration_credit_limit_bytes = initial_limit;
        candidate.ack_clock_calibration_max_limit_bytes = initial_limit;

        assert!(response_ack_clock_calibration_blocks_generic_owner(
            &candidate
        ));
        assert!(
            select_response_sender_data_target_with_ordered_debt_and_epoch(
                &[service.clone(), candidate.clone()],
                FlowLane::Throughput,
                payload_bytes,
                mux_limits,
                &[],
                Some(service.key),
                0,
                None,
            )
            .is_none(),
            "the exact active fence must drain before proven capacity becomes ordinary ownership"
        );

        candidate.ack_clock_calibration_active = false;
        assert!(!response_ack_clock_calibration_blocks_generic_owner(
            &candidate
        ));
    }

    #[test]
    fn closed_active_calibration_drain_fence_blocks_next_startup_owner() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut service = response_target(0, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, true);
        let (service_commands, _service_receivers) = reliable_path_command_channels(8);
        service.commands = service_commands;

        let initial_limit = reliable_ack_clock_calibration_limit_bytes(mux_limits);
        let mut draining =
            response_target(1, UnderlayProtocol::Tcp, 500.0, 0, 16 * 1024 * 1024, false);
        let (closed_commands, closed_receivers) = reliable_path_command_channels(8);
        drop(closed_receivers);
        draining.commands = closed_commands;
        draining.ack_clock_calibration_eligible = true;
        draining.ack_clock_calibration_active = true;
        draining.ack_clock_calibration_spent_bytes = initial_limit;
        draining.ack_clock_calibration_credit_limit_bytes = initial_limit;
        draining.ack_clock_calibration_max_limit_bytes = initial_limit.saturating_mul(2);
        assert!(draining.commands.is_closed());

        let mut next_startup =
            response_target(2, UnderlayProtocol::Tcp, 1.0, 0, 16 * 1024 * 1024, false);
        let (startup_commands, _startup_receivers) = reliable_path_command_channels(8);
        next_startup.commands = startup_commands;
        next_startup.has_bulk_rate_evidence = false;

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), draining, next_startup],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service.key),
            0,
            None,
        )
        .expect("Service remains available during exact-flight drain");
        assert_eq!(selected.target.key, service.key);
        assert!(selected.subflow_set_commit.is_none());
    }

    #[test]
    fn app_limited_bulk_proven_fast_subflow_can_still_improve_completion() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut service =
            response_target(0, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, true);
        service.snapshot.product_progress_rate_bps = Some(20_000_000.0);
        let mut fast_subflow =
            response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
        fast_subflow.snapshot.product_progress_rate_bps = Some(120_000_000.0);
        fast_subflow.snapshot.app_limited = true;
        fast_subflow.has_bulk_rate_evidence = true;
        let candidates = [&service, &fast_subflow];
        let lead = ResponseBulkLead {
            key: service.key,
            snapshot: service.snapshot,
            eta_ms: service.eta_ms,
        };

        let admission = response_target_unique_owner_admission(
            &fast_subflow,
            &candidates,
            lead,
            None,
            0,
            payload_bytes,
            mux_limits,
        );

        assert_eq!(admission.decision, PathAdmissionDecision::AdmitSubflow);
    }

    #[test]
    fn measured_same_family_alternate_is_subflow_when_service_is_not_feedable() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut service =
            response_target(0, UnderlayProtocol::Tcp, 25.0, 0, 16 * 1024 * 1024, true);
        let service_envelope =
            bulk_active_service_product_envelope_bytes(service.snapshot, payload_bytes, mux_limits);
        service.snapshot.product_bytes_in_flight = service_envelope;
        service.snapshot.queue_bytes = payload_bytes as u64;
        let measured_subflow =
            response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), measured_subflow.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service.key),
            0,
            None,
        )
        .expect("measured same-family path should remain an admissible Subflow when Service is not feedable");

        assert_eq!(selected.target.key, measured_subflow.key);
        assert_eq!(
            selected.admission.role,
            PathRuntimeRole::Subflow,
            "additional same-family owner bytes must be labeled Subflow, not Service"
        );
        assert!(
            selected.subflow_set_commit.is_some(),
            "Subflow OwnerData must be committed through the Subflow admission ledger"
        );
    }

    #[test]
    fn active_attachment_without_bulk_evidence_remains_service_anchor_when_measured_subflow_exists()
    {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut active_attachment =
            response_target(0, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, true);
        active_attachment.has_bulk_rate_evidence = false;
        let measured_lead =
            response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);
        let candidates = vec![&active_attachment, &measured_lead];
        let lead = ResponseBulkLead {
            key: measured_lead.key,
            snapshot: measured_lead.snapshot,
            eta_ms: measured_lead.eta_ms,
        };

        let admission = response_target_unique_owner_admission(
            &active_attachment,
            &candidates,
            lead,
            None,
            0,
            payload_bytes,
            mux_limits,
        );

        assert_eq!(
            admission.decision,
            PathAdmissionDecision::Service,
            "the active attachment remains the Service anchor; measured alternates are Subflows"
        );
    }

    #[test]
    fn saturated_service_may_admit_one_startup_same_underlay_subflow_owner() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut service = response_target(
            0,
            UnderlayProtocol::Tcp,
            25.0,
            mux_limits.max_path_flight_bytes as u64,
            16 * 1024 * 1024,
            true,
        );
        service.snapshot.product_progress_rate_bps = Some(180_000_000.0);
        service.has_sender_evidence = true;
        service.has_bulk_rate_evidence = true;
        let mut startup_subflow =
            response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);
        startup_subflow.has_sender_evidence = true;
        startup_subflow.has_bulk_rate_evidence = false;
        startup_subflow.snapshot.product_queue_bytes = mux_limits.max_path_flight_bytes as u64;

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), startup_subflow.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service.key),
            0,
            None,
        );

        let selected =
            selected.expect("startup same-underlay Subflow should receive one owner quantum");
        assert_eq!(selected.target.key, startup_subflow.key);
        assert_eq!(selected.admission.role, PathRuntimeRole::Subflow);
        assert!(
            selected
                .subflow_set_commit
                .is_some_and(|commit| commit.input.startup_owner_allowed),
            "sender evidence permits only explicit bounded startup Subflow admission"
        );
    }

    #[test]
    fn bulk_only_live_service_tail_admits_bounded_same_underlay_startup_sampling() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
            let mut service = response_target(0, underlay, 25.0, 0, 16 * 1024 * 1024, true);
            service.snapshot.product_progress_rate_bps = Some(180_000_000.0);
            service.has_sender_evidence = true;
            service.has_bulk_rate_evidence = true;
            let mut startup_subflow = response_target(1, underlay, 5.0, 0, 16 * 1024 * 1024, false);
            startup_subflow.has_sender_evidence = true;
            startup_subflow.has_bulk_rate_evidence = false;

            let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
                &[service.clone(), startup_subflow.clone()],
                FlowLane::Throughput,
                payload_bytes,
                mux_limits,
                &[],
                Some(service.key),
                payload_bytes,
                None,
            )
            .expect("bulk-only startup sampling should remain dispatchable behind a live Service suffix");

            assert_eq!(selected.target.key, startup_subflow.key, "{underlay:?}");
            assert_eq!(selected.admission.role, PathRuntimeRole::Subflow);
            assert!(
                selected
                    .subflow_set_commit
                    .is_some_and(|commit| commit.input.startup_owner_allowed),
                "{underlay:?} startup sampling must be explicit and ledger-bounded"
            );
        }
    }

    #[test]
    fn latency_pressure_keeps_unmeasured_validation_path_out_of_owner_sampling() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut service =
            response_target(0, UnderlayProtocol::Udp, 25.0, 0, 16 * 1024 * 1024, true);
        service.snapshot.product_progress_rate_bps = Some(180_000_000.0);
        service.snapshot.session_active_latency_sensitive_flows = 1;
        let mut validation =
            response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
        validation.has_bulk_rate_evidence = false;

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), validation.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service.key),
            payload_bytes,
            None,
        )
        .expect("the Service path should remain dispatchable under latency pressure");

        assert_eq!(selected.target.key, service.key);
        assert_eq!(selected.admission.role, PathRuntimeRole::Service);
        assert!(selected.subflow_set_commit.is_none());
    }

    #[test]
    fn repair_attachment_never_receives_startup_owner_sampling() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut service =
            response_target(0, UnderlayProtocol::Tcp, 25.0, 0, 16 * 1024 * 1024, true);
        service.snapshot.product_progress_rate_bps = Some(180_000_000.0);
        let mut repair = response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);
        repair.attachment_role = StreamOpenRole::Repair;
        repair.has_bulk_rate_evidence = true;

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), repair],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service.key),
            payload_bytes,
            None,
        )
        .expect("the Service path should remain dispatchable with a proven Repair attachment");

        assert_eq!(selected.target.key, service.key);
        assert_eq!(selected.admission.role, PathRuntimeRole::Service);
    }

    #[test]
    fn single_active_flow_keeps_unmeasured_validation_out_of_owner_data() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut service =
            response_target(0, UnderlayProtocol::Udp, 25.0, 0, 16 * 1024 * 1024, true);
        service.snapshot.product_progress_rate_bps = Some(180_000_000.0);
        let service_key = service.key;
        let mut validation =
            response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
        validation.has_bulk_rate_evidence = false;

        let single_flow = select_response_sender_data_target_with_ordered_debt_inner(
            &[service.clone(), validation.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service_key),
            0,
            None,
            false,
        )
        .expect("the one-flow Service must remain dispatchable");
        assert_eq!(single_flow.target.key, service.key);
        assert_eq!(single_flow.admission.role, PathRuntimeRole::Service);
        assert!(single_flow.subflow_set_commit.is_none());

        let multi_flow = select_response_sender_data_target_with_ordered_debt_inner(
            &[service, validation.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service_key),
            0,
            None,
            true,
        )
        .expect("multi-flow state may spend the existing bounded startup sample");
        assert_eq!(multi_flow.target.key, validation.key);
        assert!(
            multi_flow
                .subflow_set_commit
                .is_some_and(|commit| commit.input.startup_owner_allowed)
        );
    }

    #[test]
    fn startup_sample_cap_returns_dispatch_to_service() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let startup_credit =
            usize::try_from(reliable_subflow_startup_sample_limit_bytes(mux_limits)).unwrap();
        assert_eq!(startup_credit % payload_bytes, 0);

        let mut service =
            response_target(0, UnderlayProtocol::Udp, 25.0, 0, 16 * 1024 * 1024, true);
        service.snapshot.product_progress_rate_bps = Some(180_000_000.0);
        let mut validation =
            response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
        validation.has_bulk_rate_evidence = false;
        let candidates = [&service, &validation];
        let lead = ResponseBulkLead {
            key: service.key,
            snapshot: service.snapshot,
            eta_ms: service.eta_ms,
        };
        let outcome = response_target_unique_owner_admission_with_epoch(
            &validation,
            &candidates,
            lead,
            None,
            Some(service.key),
            0,
            ResponseOrderedTail::new(Some(service.key), payload_bytes)
                .for_candidate(validation.key),
            payload_bytes,
            mux_limits,
            None,
            true,
            false,
        );
        let input = outcome
            .subflow_set_commit
            .expect("first sample quantum should be admitted")
            .input;
        let mut epoch = FlowSubflowSet::new(0, service.key, startup_credit, 0, Duration::ZERO);
        for _ in 0..(startup_credit / payload_bytes) {
            assert_eq!(
                epoch.admit_subflow_owner(input).decision,
                PathAdmissionDecision::AdmitSubflow
            );
        }

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), validation],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service.key),
            payload_bytes,
            Some(&epoch),
        )
        .expect("Service should resume once startup sampling credit is exhausted");

        assert_eq!(selected.target.key, service.key);
        assert_eq!(selected.admission.role, PathRuntimeRole::Service);
        assert!(selected.subflow_set_commit.is_none());
    }

    #[test]
    fn feedable_service_precedes_measured_subflow_under_bounded_tail_debt() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut service =
            response_target(0, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, true);
        service.has_sender_evidence = true;
        service.has_bulk_rate_evidence = true;
        let mut measured_subflow =
            response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);
        measured_subflow.has_sender_evidence = true;
        measured_subflow.has_bulk_rate_evidence = true;
        measured_subflow.snapshot.app_limited = false;

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), measured_subflow.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service.key),
            payload_bytes.saturating_mul(2),
            None,
        )
        .expect("feedable Service should remain selected under bounded tail debt");

        assert_eq!(selected.target.key, service.key);
        assert_eq!(
            selected.admission.role,
            PathRuntimeRole::Service,
            "measured Subflow remains overflow while Service has capacity"
        );
    }

    #[test]
    fn response_owner_tail_guard_keeps_service_owner_feedable_under_pressure() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let owner = response_target(0, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, true);
        let alternate = response_target(1, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, false);
        let owner_key = owner.key;
        let owner_tail_guard_bytes = payload_bytes.saturating_mul(2);

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[owner, alternate],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(owner_key),
            owner_tail_guard_bytes,
            None,
        )
        .expect("live Service owner must remain feedable under contiguous owner-tail guard");

        assert_eq!(
            selected.target.key, owner_key,
            "contiguous owner-tail guard blocks alternates but must not starve the current Service owner"
        );
        assert_eq!(selected.admission.role, PathRuntimeRole::Service);
    }

    #[test]
    fn response_owner_tail_guard_uses_measured_same_underlay_when_service_queue_is_full() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut owner = response_target(0, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, true);
        let alternate = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
        let (owner_commands, _owner_rx) = reliable_path_command_channels(1);
        owner_commands
            .try_enqueue_admitted_frame(
                Frame::StreamData {
                    stream_id: StreamId(99),
                    offset: 0,
                    flags: StreamFlags::NONE,
                    payload: Bytes::from_static(b"queued"),
                },
                FlowLane::Throughput,
            )
            .expect("seed full owner data queue");
        owner.commands = owner_commands;

        let owner_tail_guard_bytes = payload_bytes.saturating_mul(2);

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[owner.clone(), alternate.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(owner.key),
            owner_tail_guard_bytes,
            None,
        );
        let selected = selected
            .expect("measured same-underlay Subflow should remain eligible under tail debt");
        assert_eq!(selected.target.key, alternate.key);
        assert_eq!(
            selected.admission.role,
            PathRuntimeRole::Subflow,
            "queue backpressure on Service does not promote a new Service; it admits a measured same-underlay Subflow"
        );
    }

    #[test]
    fn ordered_owner_debt_admits_measured_same_underlay_subflow_when_service_is_backpressured() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut service =
            response_target(0, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, true);
        let (service_commands, _service_rx) = reliable_path_command_channels(1);
        service_commands
            .try_enqueue_admitted_frame(
                Frame::StreamData {
                    stream_id: StreamId(199),
                    offset: 0,
                    flags: StreamFlags::NONE,
                    payload: Bytes::from_static(b"queued"),
                },
                FlowLane::Throughput,
            )
            .expect("seed full stale Service data queue");
        service.commands = service_commands;
        let survivor = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
        let owner_tail_guard_bytes = payload_bytes.saturating_mul(2);

        let selected = select_response_sender_data_target_with_ordered_debt_inner(
            &[service.clone(), survivor.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service.key),
            owner_tail_guard_bytes,
            None,
            true,
        );

        let selected =
            selected.expect("measured same-underlay Subflow should pass tail-debt admission");
        assert_eq!(selected.target.key, survivor.key);
        assert_eq!(
            selected.admission.role,
            PathRuntimeRole::Subflow,
            "queue backpressure on a live Service owner is not Service failure; measured same-underlay work remains Subflow OwnerData"
        );
    }

    #[test]
    fn ordered_owner_debt_keeps_live_service_owner_when_it_has_capacity() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut service =
            response_target(0, UnderlayProtocol::Tcp, 333.0, 0, 16 * 1024 * 1024, true);
        service.has_sender_evidence = true;
        service.has_bulk_rate_evidence = true;
        service.snapshot.product_progress_rate_bps = Some(1_121_000.0);
        let survivor = response_target(1, UnderlayProtocol::Tcp, 712.0, 0, 16 * 1024 * 1024, false);
        let owner_tail_guard_bytes = payload_bytes.saturating_mul(58);

        let selected = select_response_sender_data_target_with_ordered_debt_inner(
            &[service.clone(), survivor],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service.key),
            owner_tail_guard_bytes,
            None,
            true,
        )
        .expect("ordered-owner debt must not suppress a live Service owner with emission credit");

        assert_eq!(
            selected.target.key, service.key,
            "ordered-owner debt must not eject a live owner and create no_admissible_lead"
        );
        assert_eq!(selected.admission.role, PathRuntimeRole::Service);
    }

    #[test]
    fn unresolved_ordered_owner_debt_does_not_grant_owner_bytes_to_unmeasured_survivor() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut stale_service =
            response_target(0, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, true);
        let (service_commands, _service_rx) = reliable_path_command_channels(1);
        service_commands
            .try_enqueue_admitted_frame(
                Frame::StreamData {
                    stream_id: StreamId(200),
                    offset: 0,
                    flags: StreamFlags::NONE,
                    payload: Bytes::from_static(b"queued"),
                },
                FlowLane::Throughput,
            )
            .expect("seed full stale Service data queue");
        stale_service.commands = service_commands;
        let mut proof_only =
            response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);
        proof_only.has_sender_evidence = true;
        proof_only.has_bulk_rate_evidence = false;
        let owner_tail_guard_bytes = payload_bytes.saturating_mul(2);

        let selected = select_response_sender_data_target_with_ordered_debt_inner(
            &[stale_service.clone(), proof_only],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(stale_service.key),
            owner_tail_guard_bytes,
            None,
            true,
        );

        assert!(
            selected.is_none(),
            "ordered-owner debt is not a proof shortcut; an unmeasured survivor remains Probe/Standby until path-scoped bulk evidence exists"
        );
    }

    #[test]
    fn unresolved_ordered_owner_debt_blocks_active_liveness_survivor() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let stale_owner = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(2),
        };
        let mut active_validation =
            response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, true);
        active_validation.has_sender_evidence = true;
        active_validation.has_bulk_rate_evidence = false;
        let owner_tail_guard_bytes = payload_bytes.saturating_mul(2);

        let selected = select_response_sender_data_target_with_ordered_debt_inner(
            &[active_validation],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(stale_owner),
            owner_tail_guard_bytes,
            None,
            true,
        );

        assert!(
            selected.is_none(),
            "unresolved prior Service bytes block active validation/liveness from becoming Service OwnerData"
        );
    }

    #[test]
    fn clear_frontier_stale_owner_hint_does_not_block_liveness_service_failover() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut stale_owner =
            response_target(2, UnderlayProtocol::Tcp, 500.0, 0, 16 * 1024 * 1024, false);
        stale_owner.has_sender_evidence = true;
        stale_owner.has_bulk_rate_evidence = false;
        let mut survivor =
            response_target(3, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, false);
        survivor.has_sender_evidence = true;
        survivor.has_bulk_rate_evidence = false;

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[stale_owner.clone(), survivor.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(stale_owner.key),
            0,
            None,
        )
        .expect("with no active Service and a clear frontier, sender-evidence survivors may elect exactly one liveness Service");

        assert_eq!(
            selected.target.key, survivor.key,
            "a stale ordered-owner hint without unresolved bytes must not pin Service ownership to a worse proof-only path"
        );
        assert_eq!(
            selected.admission.role,
            PathRuntimeRole::Service,
            "liveness failover elects one Service owner; it must not admit optional Subflow ownership"
        );
    }

    #[test]
    fn clear_frontier_ownerless_stream_elects_measured_service() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let survivor = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);

        let selected = select_response_sender_data_target_with_ordered_debt_inner(
            std::slice::from_ref(&survivor),
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            None,
            0,
            None,
            true,
        )
        .expect("frontier-clear ownerless stream may elect a measured survivor as Service");

        assert_eq!(selected.target.key, survivor.key);
        assert_eq!(
            selected.admission.role,
            PathRuntimeRole::Service,
            "ownerless failover elects a new Service, not an optional Subflow behind missing-owner debt"
        );
    }

    #[test]
    fn response_owner_tail_guard_admits_measured_same_underlay_when_service_over_budget() {
        let mux_limits = MuxLimits {
            max_path_flight_bytes: 64 * 1024 * 1024,
            max_reorder_bytes: 64 * 1024 * 1024,
            ..MuxLimits::default()
        };
        let payload_bytes = 64 * 1024usize;
        let mut owner = response_target(0, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, true);
        let service_envelope =
            bulk_active_service_product_envelope_bytes(owner.snapshot, payload_bytes, mux_limits);
        owner.snapshot.product_bytes_in_flight = service_envelope;
        owner.snapshot.queue_bytes = payload_bytes as u64;
        let alternate = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
        let owner_tail_guard_bytes = payload_bytes.saturating_mul(2);

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[owner.clone(), alternate],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(owner.key),
            owner_tail_guard_bytes,
            None,
        )
        .expect("measured same-underlay Subflow should remain eligible under bounded tail debt");
        assert_eq!(
            selected.admission.role,
            PathRuntimeRole::Subflow,
            "owner-tail debt is accounted as ordering risk, not an absolute same-underlay Subflow ban"
        );
    }

    #[test]
    fn response_owner_tail_guard_blocks_cross_underlay_when_owner_queue_is_full() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut owner = response_target(0, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, true);
        let alternate = response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);
        let (owner_commands, _owner_rx) = reliable_path_command_channels(1);
        owner_commands
            .try_enqueue_admitted_frame(
                Frame::StreamData {
                    stream_id: StreamId(99),
                    offset: 0,
                    flags: StreamFlags::NONE,
                    payload: Bytes::from_static(b"queued"),
                },
                FlowLane::Throughput,
            )
            .expect("seed full owner data queue");
        owner.commands = owner_commands;

        let owner_tail_guard_bytes = payload_bytes.saturating_mul(2);

        assert!(
            select_response_sender_data_target_with_ordered_debt_and_epoch(
                &[owner.clone(), alternate],
                FlowLane::Throughput,
                payload_bytes,
                mux_limits,
                &[],
                Some(owner.key),
                owner_tail_guard_bytes,
                None,
            )
            .is_none(),
            "owner-debt fallback must not migrate ordered bytes across TCP/QUIC families"
        );
    }

    #[test]
    fn cross_underlay_alternate_waits_when_service_owner_is_backpressured() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let assigned_bytes = bulk_service_feed_reservoir_payload_bytes(payload_bytes, mux_limits)
            .saturating_sub(payload_bytes);
        let owner = response_target(
            1,
            UnderlayProtocol::Tcp,
            50.0,
            assigned_bytes as u64,
            0,
            true,
        );
        let alternate = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);

        let owner_tail_guard_bytes = payload_bytes.saturating_mul(2);
        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[owner.clone(), alternate],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(owner.key),
            owner_tail_guard_bytes,
            None,
        );

        let selected =
            selected.expect("feedable Service owner should remain selected under tail debt");
        assert_eq!(
            selected.target.key, owner.key,
            "a cross-underlay alternate must not own later bytes while the current Service owner has unresolved contiguous tail"
        );
    }

    #[test]
    fn response_owner_tail_guard_blocks_proof_only_same_family_subflow() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut owner = response_target(0, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, true);
        let mut alternate =
            response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
        alternate.has_sender_evidence = true;
        alternate.has_bulk_rate_evidence = false;
        let (owner_commands, _owner_rx) = reliable_path_command_channels(1);
        owner_commands
            .try_enqueue_admitted_frame(
                Frame::StreamData {
                    stream_id: StreamId(99),
                    offset: 0,
                    flags: StreamFlags::NONE,
                    payload: Bytes::from_static(b"queued"),
                },
                FlowLane::Throughput,
            )
            .expect("seed full owner data queue");
        owner.commands = owner_commands;

        let owner_tail_guard_bytes = payload_bytes.saturating_mul(2);

        assert!(
            select_response_sender_data_target_with_ordered_debt_and_epoch(
                &[owner.clone(), alternate],
                FlowLane::Throughput,
                payload_bytes,
                mux_limits,
                &[],
                Some(owner.key),
                owner_tail_guard_bytes,
                None,
            )
            .is_none(),
            "proof-only paths must stay Probe/Standby while older owner debt is unresolved"
        );
    }

    #[test]
    fn response_small_owner_debt_keeps_feedable_service_ahead_of_measured_subflow() {
        let owner = response_target(0, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, true);
        let lower_eta_alternate =
            response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[owner.clone(), lower_eta_alternate.clone()],
            FlowLane::Throughput,
            64 * 1024,
            MuxLimits::default(),
            &[],
            Some(owner.key),
            64 * 1024,
            None,
        )
        .expect("feedable Service should pass bounded tail-debt admission");

        assert_eq!(
            selected.target.key, owner.key,
            "small Service-tail debt must not displace a feedable Service with optional same-underlay work"
        );
        assert_eq!(
            selected.admission.role,
            PathRuntimeRole::Service,
            "the lower-ETA same-underlay path remains Subflow overflow"
        );
    }

    #[test]
    fn small_ordered_owner_debt_blocks_cross_underlay_service_migration() {
        let owner = response_target(1, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, true);
        let active_cross_underlay =
            response_target(0, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, true);

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[owner.clone(), active_cross_underlay],
            FlowLane::Throughput,
            64 * 1024,
            MuxLimits::default(),
            &[],
            Some(owner.key),
            64 * 1024,
            None,
        );

        assert!(
            selected.is_none()
                || selected
                    .as_ref()
                    .is_some_and(|selected| selected.target.key == owner.key),
            "any unresolved ordered-owner tail must block TCP/QUIC Service migration until the frontier clears or the candidate already owns the lower range"
        );
    }

    #[test]
    fn ordered_owner_debt_blocks_fallback_service_when_owner_target_is_absent() {
        let missing_owner = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let active_cross_underlay =
            response_target(0, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, true);

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[active_cross_underlay],
            FlowLane::Throughput,
            64 * 1024,
            MuxLimits::default(),
            &[],
            Some(missing_owner),
            64 * 1024,
            None,
        );

        assert!(
            selected.is_none(),
            "an absent ordered owner with unresolved lower bytes must trigger repair/failover handling, not make another underlay the Service owner for later bytes"
        );
    }

    #[test]
    fn missing_same_underlay_owner_debt_admits_measured_service_failover() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let missing_owner = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        };
        let measured_survivor =
            response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            std::slice::from_ref(&measured_survivor),
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(missing_owner),
            payload_bytes.saturating_mul(2),
            None,
        )
        .expect("a bulk-rate-proven same-underlay survivor should elect Service failover when the previous Service output is gone and no lower-flight owner remains");

        assert_eq!(selected.target.key, measured_survivor.key);
        assert_eq!(
            selected.admission.role,
            PathRuntimeRole::Service,
            "same-underlay failover resumes Service OwnerData; it is not optional Subflow exploration and does not credit RepairData as proof"
        );
    }

    #[test]
    fn missing_same_underlay_service_failover_respects_path_latency_window() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let missing_owner = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let mut measured_survivor = response_target(
            1,
            UnderlayProtocol::Tcp,
            5.0,
            0,
            mux_limits.max_path_flight_bytes as u64,
            false,
        );
        measured_survivor.snapshot.delivery_rate_bps = 10_000_000_000.0;
        measured_survivor.snapshot.pacing_rate_bps = 10_000_000_000.0;
        measured_survivor.snapshot.active_latency_sensitive_flows = 1;
        let latency_credit = usize::try_from(bulk_latency_pressure_service_feed_window_bytes(
            payload_bytes,
            mux_limits,
        ))
        .unwrap();
        measured_survivor.snapshot.product_bytes_in_flight =
            latency_credit.saturating_sub(payload_bytes) as u64;

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            std::slice::from_ref(&measured_survivor),
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(missing_owner),
            payload_bytes.saturating_mul(2),
            None,
        )
        .expect(
            "mature same-underlay Service failover may consume remaining latency-window credit",
        );
        assert_eq!(selected.admission.role, PathRuntimeRole::Service);

        measured_survivor.snapshot.product_bytes_in_flight = latency_credit as u64;
        assert!(
            select_response_sender_data_target_with_ordered_debt_and_epoch(
                &[measured_survivor],
                FlowLane::Throughput,
                payload_bytes,
                mux_limits,
                &[],
                Some(missing_owner),
                payload_bytes.saturating_mul(2),
                None,
            )
            .is_none(),
            "runtime Service failover must stop at the same path-local latency window even when its bulk role is AdditionalSameUnderlay"
        );
    }

    #[test]
    fn missing_same_underlay_owner_debt_admits_sender_evidence_service_failover() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let missing_owner = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let mut liveness_survivor =
            response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);
        liveness_survivor.has_sender_evidence = true;
        liveness_survivor.has_bulk_rate_evidence = false;

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            std::slice::from_ref(&liveness_survivor),
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(missing_owner),
            payload_bytes.saturating_mul(2),
            None,
        )
        .expect("a same-underlay sender-evidenced survivor should receive bounded Service failover when the previous Service output is gone and no lower-flight owner remains");

        assert_eq!(selected.target.key, liveness_survivor.key);
        assert_eq!(
            selected.admission.role,
            PathRuntimeRole::Service,
            "same-underlay failover is Service continuation, not Subflow aggregation"
        );
        assert!(
            selected.subflow_set_commit.is_none(),
            "failover Service election must not spend Subflow owner credit"
        );
    }

    #[test]
    fn ordered_owner_debt_without_owner_hint_blocks_active_fallback_service() {
        let active_cross_underlay =
            response_target(0, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, true);

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[active_cross_underlay],
            FlowLane::Throughput,
            64 * 1024,
            MuxLimits::default(),
            &[],
            None,
            64 * 1024,
            None,
        );

        assert!(
            selected.is_none(),
            "ordered-owner debt without an owner hint must not fall back to the active path as Service"
        );
    }

    #[test]
    fn proof_only_active_service_can_continue_under_its_own_tail_guard() {
        let mut active_fallback =
            response_target(0, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, true);
        active_fallback.has_sender_evidence = true;
        active_fallback.has_bulk_rate_evidence = false;

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[active_fallback.clone()],
            FlowLane::Throughput,
            64 * 1024,
            MuxLimits::default(),
            &[],
            Some(active_fallback.key),
            315_680,
            None,
        );

        let selected =
            selected.expect("the live active Service owner may continue under its own tail guard");
        assert_eq!(selected.target.key, active_fallback.key);
        assert_eq!(
            selected.admission.role,
            PathRuntimeRole::Service,
            "tail guard must not turn active Service OwnerData into Subflow exploration"
        );
    }

    #[test]
    fn bulk_only_tcp_sender_evidence_admits_startup_subflow_not_service() {
        let owner = response_target(0, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, true);
        let mut lower_eta_alternate =
            response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);
        lower_eta_alternate.has_sender_evidence = true;
        lower_eta_alternate.has_bulk_rate_evidence = false;

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[owner.clone(), lower_eta_alternate.clone()],
            FlowLane::Throughput,
            64 * 1024,
            MuxLimits::default(),
            &[],
            Some(owner.key),
            0,
            None,
        )
        .expect("current Service owner should remain eligible");

        assert_eq!(
            selected.target.key, lower_eta_alternate.key,
            "sender evidence may start one bounded same-underlay Subflow sampling epoch"
        );
        assert_eq!(
            selected.admission.role,
            PathRuntimeRole::Subflow,
            "startup owner bytes are Subflow OwnerData and must not migrate Service ownership"
        );
        assert!(
            selected
                .subflow_set_commit
                .is_some_and(|commit| commit.input.startup_owner_allowed),
            "startup Subflow admission must be explicit and bounded"
        );
    }

    #[test]
    fn cross_underlay_candidate_does_not_displace_owner_without_bulk_rate() {
        let owner = response_target(0, UnderlayProtocol::Tcp, 80.0, 0, 16 * 1024 * 1024, true);
        let mut candidate =
            response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
        candidate.has_bulk_rate_evidence = false;

        let selected = choose_response_sender_data_target(
            &[owner.clone(), candidate],
            FlowLane::Throughput,
            64 * 1024,
            MuxLimits::default(),
            &[],
            Some(owner.key),
        )
        .expect(
            "current service owner should remain eligible while cross-family candidate is unproven",
        );

        assert_eq!(selected.key, owner.key);
    }

    #[test]
    fn cross_underlay_bulk_rate_candidate_does_not_become_service_at_clear_frontier() {
        let owner = response_target(0, UnderlayProtocol::Tcp, 80.0, 0, 16 * 1024 * 1024, true);
        let candidate = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);

        let selected = choose_response_sender_data_target(
            &[owner.clone(), candidate.clone()],
            FlowLane::Throughput,
            64 * 1024,
            MuxLimits::default(),
            &[],
            Some(owner.key),
        )
        .expect("current Service owner should remain eligible at a clear frontier");

        assert_eq!(
            selected.key, owner.key,
            "mixed-family Service migration must be explicit; lower-ETA cross-underlay candidates do not become Service through per-quantum selection"
        );
    }

    #[test]
    fn cross_underlay_candidate_does_not_become_service_when_owner_hint_is_missing() {
        let owner = response_target(0, UnderlayProtocol::Tcp, 80.0, 0, 16 * 1024 * 1024, true);
        let candidate = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);

        let selected = choose_response_sender_data_target(
            &[owner.clone(), candidate.clone()],
            FlowLane::Throughput,
            64 * 1024,
            MuxLimits::default(),
            &[],
            None,
        )
        .expect("active Service output should anchor family ownership even if the owner hint was cleared");

        assert_eq!(
            selected.key, owner.key,
            "a missing ordered-owner hint is not permission for implicit cross-family Service migration while an active Service output is live"
        );
    }

    #[test]
    fn cross_underlay_bulk_rate_candidate_that_owns_lower_flight_remains_eligible() {
        let service = response_target(0, UnderlayProtocol::Tcp, 80.0, 0, 16 * 1024 * 1024, true);
        let candidate = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
        let lower_flights = vec![CarrierPathFlightDebt {
            key: candidate.key,
            bytes: 64 * 1024,
        }];

        let selected = choose_response_sender_data_target(
            &[service.clone(), candidate.clone()],
            FlowLane::Throughput,
            64 * 1024,
            MuxLimits::default(),
            &lower_flights,
            Some(service.key),
        )
        .expect("candidate owning the lower flight should remain eligible");

        assert_eq!(
            selected.key, candidate.key,
            "a bulk-rate-proven path that already owns the lower range must not be blocked by a stale cross-family frontier check"
        );
    }

    #[test]
    fn active_cross_underlay_path_that_owns_lower_flight_remains_service_candidate() {
        let mut old_service =
            response_target(0, UnderlayProtocol::Tcp, 80.0, 0, 16 * 1024 * 1024, false);
        old_service.has_bulk_rate_evidence = true;
        let mut lower_active =
            response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, true);
        lower_active.has_sender_evidence = true;
        lower_active.has_bulk_rate_evidence = false;
        let lower_flights = vec![CarrierPathFlightDebt {
            key: lower_active.key,
            bytes: 64 * 1024,
        }];

        let selected = choose_response_sender_data_target(
            &[old_service.clone(), lower_active.clone()],
            FlowLane::Throughput,
            64 * 1024,
            MuxLimits::default(),
            &lower_flights,
            Some(old_service.key),
        )
        .expect("active lower-owner path must remain eligible to advance its own frontier");

        assert_eq!(
            selected.key, lower_active.key,
            "mixed-family health gates must not remove the active path that already owns unresolved lower bytes"
        );
    }

    #[test]
    fn owner_tail_guard_keeps_cross_underlay_candidate_that_owns_lower_flight() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let service = response_target(0, UnderlayProtocol::Tcp, 80.0, 0, 16 * 1024 * 1024, true);
        let candidate = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
        let lower_flights = vec![CarrierPathFlightDebt {
            key: candidate.key,
            bytes: payload_bytes as u64,
        }];
        let owner_tail_guard_bytes = payload_bytes.saturating_mul(2);

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), candidate.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &lower_flights,
            Some(service.key),
            owner_tail_guard_bytes,
            None,
        )
        .expect("candidate owning the lower flight should survive tail-guard filtering");

        assert_eq!(
            selected.target.key, candidate.key,
            "tail guard must filter by candidate ordering safety, not by carrier family alone"
        );
    }
}
