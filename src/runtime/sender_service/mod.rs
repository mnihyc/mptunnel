#[cfg(feature = "lab-diagnostics")]
mod response_service_handoff_diagnostics;

#[cfg(feature = "lab-diagnostics")]
use self::response_service_handoff_diagnostics::lab_response_service_handoff_evaluation;
use super::model::ack_clock::{
    TcpAckClockCalibrationOpportunity, reliable_ack_clock_calibration_rate_coverage_floor_bytes,
    reliable_tcp_ack_clock_calibration_opportunity,
};
#[cfg(test)]
use super::model::ack_clock::{
    reliable_ack_clock_calibration_ceiling_bytes, reliable_ack_clock_calibration_limit_bytes,
    reliable_request_ack_clock_calibration_target_bytes,
};
#[cfg(feature = "lab-diagnostics")]
use super::model::admission::BulkExplorationCompletionProjection;
use super::model::admission::{
    BulkAdmissionCheck, BulkAdmissionRole, bulk_active_service_product_envelope_bytes,
    bulk_additional_admission_role, bulk_candidate_admission_suppression_with_completion_backlog,
    bulk_candidate_admission_suppression_with_ordering_debt, bulk_candidate_pipe_bytes,
    bulk_exploration_completion_projection, bulk_latency_pressure_service_feed_window_bytes,
    bulk_service_feed_reservoir_payload_bytes, bulk_service_horizon_payload_bytes,
    bulk_service_product_envelope_payload_bytes,
};
use super::model::{ResponseCandidateTailDebt, ResponseOrderedTail, ResponseSameFamilyReservoir};
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
    request_calibration_commit: Option<RequestAckClockCalibrationCommit>,
    request_load_expectation: Option<(RelayPathKey, u32, u32)>,
}

#[derive(Debug, Clone, Copy)]
enum RequestAckClockCalibrationCommit {
    ServiceFence {
        service: RelayPathInstance,
        candidate: RelayPathInstance,
        entry_offset: u64,
        foreign_optional_ranges: usize,
        foreign_optional_bytes: u64,
    },
    OwnerData {
        candidate: RelayPathInstance,
        target_bytes: u64,
        payload_bytes: u64,
        entry_offset: u64,
        foreign_optional_ranges: usize,
        foreign_optional_bytes: u64,
    },
}

#[derive(Debug)]
struct RequestQuicCapacityCalibration {
    target: RelayPathInstance,
    token: u64,
    publication_expires_at: Instant,
    graduated: bool,
    ticket: QuicCapacityProbeCommandTicket,
    _lease: RequestQuicCapacityProbeLease,
}

#[derive(Debug)]
struct RequestTcpCapacityCalibration {
    target: RelayPathInstance,
    token: u64,
    publication_expires_at: Instant,
    proof_expires_at: Option<Instant>,
    graduated: bool,
    lease: RequestTcpCapacityProbeLease,
}

fn request_tcp_carrier_authority_expired_naturally(
    published: bool,
    proof_expires_at: Option<Instant>,
    now: Instant,
) -> bool {
    published && proof_expires_at.is_some_and(|expires_at| now >= expires_at)
}

impl Drop for RequestTcpCapacityCalibration {
    fn drop(&mut self) {
        self.lease.cancel();
    }
}

impl Drop for RequestQuicCapacityCalibration {
    fn drop(&mut self) {
        self.ticket.cancel();
    }
}

impl RelayPathSendSelection {
    fn new(position: usize, data_role: Option<PathRuntimeRole>) -> Self {
        Self {
            position,
            data_role,
            request_subflow_rollback: None,
            request_attempted_rollback: None,
            request_calibration_commit: None,
            request_load_expectation: None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct RequestPathRateEvidence {
    exact_attributed_bytes: u64,
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

#[derive(Debug, Clone, Copy)]
pub(super) struct RequestPerFlowRateModel {
    pub(super) rate_bps: f64,
    pub(super) delivery_samples: u32,
}

#[derive(Debug, Clone, Copy)]
struct RequestTcpAckTurnoverModel {
    turnover_bytes: f64,
    sampled_at: Instant,
    sample_pto: Duration,
}

impl RequestTcpAckTurnoverModel {
    fn observe(
        previous: Option<Self>,
        sample: PathRateSample,
        sample_pto: Duration,
        sampled_at: Instant,
    ) -> Option<Self> {
        let sample_turnover = sample.rate_bps().max(0.0) / 8.0 * sample_pto.as_secs_f64();
        if !sample_turnover.is_finite() {
            return None;
        }
        let turnover_bytes = previous
            .filter(|previous| previous.is_fresh_at(sampled_at))
            .map_or(sample_turnover, |previous| {
                previous
                    .turnover_bytes
                    .mul_add(0.75, sample_turnover * 0.25)
            });
        Some(Self {
            turnover_bytes,
            sampled_at,
            sample_pto,
        })
    }

    fn is_fresh_at(self, now: Instant) -> bool {
        now.saturating_duration_since(self.sampled_at)
            < self
                .sample_pto
                .saturating_mul(QUIC_PERSISTENT_CONGESTION_THRESHOLD)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RequestOwnerAckProgress {
    pub(super) instance: RelayPathInstance,
    pub(super) bytes: usize,
}

impl RequestPathRateEvidence {
    fn new(first_sent_at: Instant) -> Self {
        Self {
            exact_attributed_bytes: 0,
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
        coverage_floor_bytes: u64,
        require_post_boundary_send: bool,
    ) -> RequestPathRateEvidenceUpdate {
        self.exact_attributed_bytes = self.exact_attributed_bytes.saturating_add(bytes);
        if self.pending_bytes == 0 {
            self.pending_first_sent_at = first_sent_at;
            self.pending_latest_sent_at = latest_sent_at;
        } else {
            self.pending_first_sent_at = self.pending_first_sent_at.min(first_sent_at);
            self.pending_latest_sent_at = self.pending_latest_sent_at.max(latest_sent_at);
        }
        self.pending_bytes = self.pending_bytes.saturating_add(bytes);
        let coverage_floor_bytes = coverage_floor_bytes.max(PATH_OPEN_SCORE_BYTES as u64);
        if self.pending_bytes < coverage_floor_bytes {
            return RequestPathRateEvidenceUpdate::Pending;
        }

        let sample_bytes = self.pending_bytes;
        let first_window = self.previous_window_acked_at.is_none();
        let sample_started_at = self
            .previous_window_acked_at
            .unwrap_or(self.pending_first_sent_at);
        // A later staged window is causal only when every sampled byte was sent
        // at or after the ACK that starts the interval. Charging the full
        // ACK-to-ACK gap is conservative when the sender was briefly idle.
        let ack_clocked = first_window
            || !require_post_boundary_send
            || self.pending_first_sent_at >= sample_started_at;
        self.pending_bytes = 0;
        self.previous_window_acked_at = Some(acked_at);
        let ack_elapsed = acked_at.saturating_duration_since(sample_started_at);
        let send_elapsed = self
            .pending_latest_sent_at
            .saturating_duration_since(self.pending_first_sent_at);
        // Product ACKs can arrive in compressed batches. As in BBR delivery
        // sampling, use the slower of the send and ACK clocks so ACK timing
        // alone cannot claim a rate above the observed sender rate.
        let sample = ack_clocked
            .then(|| PathRateSample::new(sample_bytes, ack_elapsed.max(send_elapsed)))
            .flatten();
        RequestPathRateEvidenceUpdate::Proven {
            sample,
            first_window,
        }
    }

    fn has_exact_path_provenance(&self) -> bool {
        self.exact_attributed_bytes >= PATH_OPEN_SCORE_BYTES as u64
    }

    fn exact_attributed_bytes(&self) -> u64 {
        self.exact_attributed_bytes
    }

    fn seed_ack_boundary(&mut self, acked_at: Instant) {
        self.pending_bytes = 0;
        self.previous_window_acked_at = Some(acked_at);
    }
}

fn request_path_rate_coverage_floor_bytes(
    instance: RelayPathInstance,
    ordered_service: Option<RelayPathInstance>,
    calibration_target: Option<u64>,
    mux_limits: MuxLimits,
) -> u64 {
    match instance.key.underlay {
        UnderlayProtocol::Tcp if Some(instance) != ordered_service => calibration_target
            .unwrap_or_else(|| {
                reliable_ack_clock_calibration_rate_coverage_floor_bytes(mux_limits)
            }),
        // Service has continuous feed; it does not need a new pipe-sized train.
        UnderlayProtocol::Tcp => {
            reliable_ack_clock_calibration_rate_coverage_floor_bytes(mux_limits)
        }
        UnderlayProtocol::Udp => PATH_OPEN_SCORE_BYTES as u64,
    }
}

fn request_tcp_candidate_turnover_authorized(
    exact_acked_bytes: u64,
    calibration_target_bytes: u64,
    ordinary_coverage_bytes: u64,
) -> bool {
    // The bounded calibration window proves ownership and one rate point. A
    // second exact window proves that ordinary product traffic can sustain it.
    exact_acked_bytes >= calibration_target_bytes.saturating_add(ordinary_coverage_bytes.max(1))
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
    request_per_flow_rate_bps: HashMap<RelayPathInstance, RequestPerFlowRateModel>,
    request_tcp_ack_turnover: HashMap<RelayPathInstance, RequestTcpAckTurnoverModel>,
    request_rate_proven_subflows: HashSet<RelayPathInstance>,
    request_ack_clock_first_window_subflows: HashSet<RelayPathInstance>,
    request_ack_clock_proven_subflows: HashSet<RelayPathInstance>,
    request_window_turnover_proven_subflows: HashSet<RelayPathInstance>,
    request_ack_clock_calibration_bytes: HashMap<RelayPathInstance, u64>,
    request_ack_clock_calibration_targets: HashMap<RelayPathInstance, u64>,
    request_ack_clock_calibration_owner: Option<RequestAckClockCalibrationOwner>,
    request_ack_clock_calibration_pending: Option<RequestAckClockCalibrationPending>,
    request_tcp_capacity_calibrations: HashMap<RelayPathInstance, RequestTcpCapacityCalibration>,
    request_tcp_capacity_attempted_paths: HashSet<usize>,
    request_tcp_capacity_proven_subflows: HashSet<RelayPathInstance>,
    request_tcp_capacity_campaign: Arc<RequestCapacityProbeCampaignBudget>,
    #[cfg(feature = "lab-diagnostics")]
    request_tcp_capacity_last_gate: Option<(Option<RelayPathInstance>, &'static str)>,
    request_quic_capacity_calibration: Option<RequestQuicCapacityCalibration>,
    request_quic_capacity_attempted_paths: HashSet<usize>,
    request_quic_capacity_campaign: Arc<RequestCapacityProbeCampaignBudget>,
    request_graduated_subflows: HashSet<RelayPathInstance>,
    request_attempted_subflows: HashSet<RelayPathInstance>,
    request_membership_generation: Option<u64>,
    request_bulk_flow_registration: Option<ReliableTcpRequestBulkFlowRegistration>,
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
            ReliableWorkClass::Control,
            ReliableRelayQueuedWorkKind::Control(frame),
            None,
            false,
            payload_bytes,
        )
    }

    pub(super) fn push_final_control(&mut self, frame: Frame) -> u64 {
        let payload_bytes = reliable_stream_frame_payload_bytes(&frame);
        self.push_work(
            ReliableWorkClass::Control,
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
            ReliableWorkClass::Data,
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
            ReliableWorkClass::Repair,
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
        lane: ReliableWorkClass,
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
        if lane == ReliableWorkClass::Data {
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
            ReliableWorkClass::Control if final_control => {
                self.final_control.push_back(work);
            }
            ReliableWorkClass::Control => self.control.push_back(work),
            ReliableWorkClass::Data => self.data.push_back(work),
            ReliableWorkClass::Repair => self.repair.push_back(work),
        }
        enqueue_id
    }

    pub(super) fn front(&self) -> Option<(ReliableWorkClass, &ReliableRelayQueuedWork)> {
        if let Some(work) = self.control.front() {
            Some((ReliableWorkClass::Control, work))
        } else if let Some(work) = self.critical_repair.front() {
            Some((ReliableWorkClass::Repair, work))
        } else if let Some(work) = self.data.front() {
            Some((ReliableWorkClass::Data, work))
        } else if let Some(work) = self.repair.front() {
            Some((ReliableWorkClass::Repair, work))
        } else {
            self.final_control
                .front()
                .map(|work| (ReliableWorkClass::Control, work))
        }
    }

    pub(super) fn front_lane(&self) -> Option<ReliableWorkClass> {
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

    pub(super) fn commit_front(&mut self) -> Option<(ReliableWorkClass, ReliableRelayQueuedWork)> {
        let (lane, work) = if let Some(work) = self.control.pop_front() {
            (ReliableWorkClass::Control, work)
        } else if let Some(work) = self.critical_repair.pop_front() {
            (ReliableWorkClass::Repair, work)
        } else if let Some(work) = self.data.pop_front() {
            (ReliableWorkClass::Data, work)
        } else if let Some(work) = self.repair.pop_front() {
            (ReliableWorkClass::Repair, work)
        } else {
            (ReliableWorkClass::Control, self.final_control.pop_front()?)
        };
        self.bytes = self.bytes.saturating_sub(work.payload_bytes);
        if lane == ReliableWorkClass::Data {
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
    pub(super) fn pop_front(&mut self) -> Option<(ReliableWorkClass, ReliableRelayQueuedWork)> {
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
    pub(super) lane: ReliableWorkClass,
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
    static EVENT_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let ordinal = EVENT_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if ordinal >= 512 && ordinal % 512 != 0 {
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
            "ordinal={} reason={} session_id={} binding_instance_id={} path_underlay={:?} path_id={} is_active={} sender_evidence={} bulk_rate_evidence={} role={} eta_ms={:.3} lead_underlay={} lead_path_id={} lead_eta_ms={:.3} stream_ordering_debt={} payload_bytes={} command_pending_bytes={} path_queue_bytes={} product_queue_bytes={} carrier_inflight_bytes={} product_inflight_bytes={} owner_data_inflight_bytes={} carrier_inflight_limit={} delivery_rate_mbps={:.3} pacing_mbps={:.3} srtt_ms={:.3} confidence={:.3} app_limited={} calibration_eligible={} calibration_proven={} calibration_active={} calibration_spent_bytes={} calibration_credit_bytes={} calibration_max_bytes={} mux_max_path_flight={} mux_max_reorder={}",
            ordinal + 1,
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
    static EVENT_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let ordinal = EVENT_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if ordinal >= 1024 && ordinal % 128 != 0 {
        return;
    }
    lab_diagnostic(
        "server_bulk_output_selected",
        format_args!(
            "ordinal={} reason={} session_id={} binding_instance_id={} path_underlay={:?} path_id={} role={:?} work={:?} payload_bytes={} command_pending_bytes={} product_inflight_bytes={} owner_data_inflight_bytes={} eta_ms={:.3} app_limited={} bulk_rate_evidence={} calibration_eligible={} calibration_proven={} calibration_active={} calibration_spent_bytes={} calibration_credit_bytes={} calibration_max_bytes={}",
            ordinal + 1,
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
    candidate_snapshot: PathSnapshot,
    candidate_eta_ms: f64,
    uses_service_prior: bool,
    projection: BulkExplorationCompletionProjection,
    admitted: bool,
) {
    if !lab_diagnostic_event_enabled("response_ack_clock_calibration_admission") {
        return;
    }
    lab_diagnostic(
        "response_ack_clock_calibration_admission",
        format_args!(
            "session_id={} binding_instance_id={} path_underlay={:?} path_id={} service_underlay={:?} service_path_id={} admitted={} uses_service_prior={} candidate_completion_ms={:.3} service_reservoir_horizon_ms={:.3} exploration_bytes={} service_followup_bytes={} candidate_eta_ms={:.3} service_eta_ms={:.3} candidate_rate_mbps={:.3} service_rate_mbps={:.3} candidate_srtt_ms={:.3} service_srtt_ms={:.3}",
            target.session_id.0,
            target.binding_instance_id,
            target.key.underlay,
            target.key.path_id.0,
            service.key.underlay,
            service.key.path_id.0,
            admitted,
            uses_service_prior,
            projection.candidate_completion_ms,
            projection.service_reservoir_horizon_ms,
            projection.exploration_bytes,
            projection.service_followup_bytes,
            candidate_eta_ms,
            service.eta_ms,
            candidate_snapshot
                .delivery_rate_bps
                .max(candidate_snapshot.pacing_rate_bps)
                / 1_000_000.0,
            service
                .snapshot
                .delivery_rate_bps
                .max(service.snapshot.pacing_rate_bps)
                / 1_000_000.0,
            candidate_snapshot.srtt_ms,
            service.snapshot.srtt_ms,
        ),
    );
}

fn response_tcp_calibration_opportunity_candidate(
    service: &ResponseSenderPathTarget,
    target: &ResponseSenderPathTarget,
    lane: FlowLane,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> (PathSnapshot, f64, bool) {
    let mut snapshot = target.snapshot;
    let service_rate_bps = service.snapshot.delivery_rate_bps.max(1.0);
    let uses_service_prior = target.endpoint_only_service_prior_eligible
        && service_rate_bps > snapshot.delivery_rate_bps;
    if !uses_service_prior {
        return (snapshot, target.eta_ms, false);
    }

    // This prior makes a bounded measurement reachable; it is not candidate
    // evidence and never leaves this completion-opportunity calculation.
    snapshot.delivery_rate_bps = service_rate_bps;
    snapshot.pacing_rate_bps = snapshot.pacing_rate_bps.max(service_rate_bps);
    snapshot.rate_scope = ResponseRateScope::PathCapacity;
    snapshot.inflight_limit_bytes = snapshot
        .inflight_limit_bytes
        .max(bulk_candidate_pipe_bytes(snapshot));
    let eta_ms = server_bulk_output_eta_ms(
        target.key,
        snapshot,
        Some(service.key),
        lane,
        payload_bytes,
        mux_limits,
    );
    (snapshot, eta_ms, true)
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
        target: ResponseDispatchTarget,
        role: PathRuntimeRole,
        service_handoff_commit: Option<ResponseServiceHandoffCommit>,
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
struct ResponseServiceHandoffCommit {
    planner_generation: u64,
    lane_generation: u64,
    model_generation: u64,
    handoff_frontier: u64,
    service: CarrierPathKey,
    service_path_instance_id: ServerCarrierPathInstanceId,
    service_incarnation: u64,
    target_path_instance_id: ServerCarrierPathInstanceId,
    mode: ResponseServiceHandoffMode,
    target_command_pending_limit_bytes: u64,
    capacity_proof: Option<QuicCapacityProofCandidate>,
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
    requires_active_response_start: bool,
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
    service_handoff_commit: Option<ResponseServiceHandoffCommit>,
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
    let service_bulk_flows = service
        .snapshot
        .active_flows
        .saturating_sub(service.snapshot.active_latency_sensitive_flows);
    let target_bulk_flows = target
        .snapshot
        .active_flows
        .saturating_sub(target.snapshot.active_latency_sensitive_flows);

    service.key == service_key
        && service.is_active
        && service.has_bulk_rate_evidence
        // One sustained response is real demand. The candidate must still be
        // less occupied than Service; flow count never substitutes for the
        // bounded epoch, sender evidence, or product-debt guards below.
        && service_bulk_flows > target_bulk_flows
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

fn response_startup_sample_has_completion_opportunity(
    candidates: &[&ResponseSenderPathTarget],
    service: &ResponseSenderPathTarget,
    target: &ResponseSenderPathTarget,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> bool {
    let measured_same_family_subflow_exists = candidates.iter().copied().any(|candidate| {
        candidate.key != target.key
            && response_target_is_measured_same_underlay_subflow_candidate(service.key, candidate)
    });
    if !measured_same_family_subflow_exists {
        // The first bounded candidate is the bootstrap that makes an optional
        // path measurable. Latency pressure and resource/debt guards still
        // apply; requiring a preexisting completion model would be circular.
        return true;
    }
    // Once one optional path is measured, another candidate must justify its
    // own ordering risk; serially probing every cold path starves capacity that
    // the binding has already discovered.
    let candidate_snapshot = target.snapshot;
    let candidate_eta_ms = target.eta_ms;
    bulk_exploration_completion_projection(
        service.snapshot,
        service.eta_ms,
        candidate_snapshot,
        candidate_eta_ms,
        reliable_subflow_startup_sample_limit_bytes(mux_limits),
        payload_bytes,
        mux_limits,
    )
    .completes_within_service_reservoir()
}

#[derive(Debug, Clone, Copy)]
struct ResponseQuicCapacityCalibrationGeometry {
    train_bytes: usize,
    fits_session_envelope: bool,
    sample_floor_bytes: u64,
    accounting_slack_bytes: u64,
    fresh_strict_window_bytes: u64,
    carrier_window_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RequestQuicCapacityCalibrationGeometry {
    train_bytes: u64,
    sample_floor_bytes: u64,
    accounting_slack_bytes: u64,
    timing_slack_bytes: u64,
    desired_warmup_carrier_bytes: u64,
    warmup_carrier_bytes: u64,
    required_timed_carrier_bytes: u64,
    service_rate_bps: u64,
    candidate_carrier_flight_bytes: u64,
}

#[cfg(target_os = "linux")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RequestTcpCapacityCalibrationGeometry {
    train_bytes: u64,
    sample_floor_bytes: u64,
    accounting_slack_bytes: u64,
    timing_slack_bytes: u64,
    warmup_carrier_bytes: u64,
    required_timed_carrier_bytes: u64,
    service_rate_bps: u64,
    candidate_carrier_flight_bytes: u64,
}

fn capacity_timing_slack_bytes() -> u64 {
    // The measurement epoch carries zero-span callback bytes forward; one
    // pacing quantum still keeps distinct packet timestamps behind its floor.
    BBR_MAX_SEND_QUANTUM_BYTES as u64
}

fn request_quic_capacity_calibration_geometry(
    candidate: PathSnapshot,
    service_rate_bps: f64,
    mux_limits: MuxLimits,
    train_envelope_bytes: u64,
) -> Option<RequestQuicCapacityCalibrationGeometry> {
    // A cold QUIC carrier must grow far enough to compete with the live
    // Service before its final timed window can represent bulk capacity.
    if !service_rate_bps.is_finite() || service_rate_bps <= 0.0 {
        return None;
    }
    let sample_floor = reliable_subflow_startup_sample_limit_bytes(mux_limits);
    let accounting_slack = (PATH_OPEN_SCORE_BYTES as u64).min(sample_floor / 8);
    let required_timed_carrier_bytes = sample_floor.saturating_sub(accounting_slack).max(1);
    let timing_slack_bytes = capacity_timing_slack_bytes();
    let measurement_window_bytes = required_timed_carrier_bytes.checked_add(timing_slack_bytes)?;
    let competing_rate_bdp =
        (service_rate_bps / 8.0 * candidate.srtt_ms.max(1.0) / 1_000.0).ceil() as u64;
    let competing_rate_pipe = ((competing_rate_bdp as f64) * BBR_DEFAULT_CWND_GAIN).ceil() as u64;
    // Request snapshots expose total relay-plus-carrier flight. Subtract the
    // separately tracked product flight before sizing this carrier epoch.
    let candidate_carrier_flight_bytes = candidate
        .bytes_in_flight
        .saturating_sub(candidate.product_bytes_in_flight);
    // Carrier warmup competes with the effective rate/RTT pipe. Product flight is
    // shared ordering debt and may belong to other paths, so it cannot size one
    // candidate's native congestion-control transaction.
    let desired_warmup_carrier_bytes = candidate
        .inflight_limit_bytes
        .max(candidate_carrier_flight_bytes)
        .max(competing_rate_pipe);
    let train_envelope_bytes = train_envelope_bytes.min(
        reliable_quic_capacity_calibration_session_limit_bytes(mux_limits),
    );
    let warmup_carrier_bytes = desired_warmup_carrier_bytes
        .min(train_envelope_bytes.checked_sub(measurement_window_bytes)?);
    if warmup_carrier_bytes
        < candidate
            .inflight_limit_bytes
            .max(candidate_carrier_flight_bytes)
    {
        return None;
    }
    // Keep one pacing quantum of trailing measurement credit. The application
    // receipt can race the final delayed transport ACK; untimed/late bytes must
    // not consume the strict window before earlier timed ACKs reach the poller.
    let train_bytes = warmup_carrier_bytes.checked_add(measurement_window_bytes)?;
    if train_bytes > train_envelope_bytes {
        return None;
    }
    Some(RequestQuicCapacityCalibrationGeometry {
        train_bytes: train_bytes.max(sample_floor),
        sample_floor_bytes: sample_floor,
        accounting_slack_bytes: accounting_slack,
        timing_slack_bytes,
        desired_warmup_carrier_bytes,
        warmup_carrier_bytes,
        required_timed_carrier_bytes,
        service_rate_bps: service_rate_bps.ceil() as u64,
        candidate_carrier_flight_bytes,
    })
}

fn request_capacity_stable_candidate_share_bytes(
    mux_limits: MuxLimits,
    eligible_candidates: usize,
) -> u64 {
    // Divide once from configured policy eligibility. Attempt order and unused
    // earlier shares must not make a later path's calibration more expensive.
    let divisor = u64::try_from(eligible_candidates.max(1)).unwrap_or(u64::MAX);
    reliable_quic_capacity_calibration_session_limit_bytes(mux_limits) / divisor
}

#[cfg(target_os = "linux")]
fn request_tcp_capacity_calibration_geometry(
    candidate: PathSnapshot,
    service_model: RequestPerFlowRateModel,
    mux_limits: MuxLimits,
    train_envelope_bytes: u64,
) -> Option<RequestTcpCapacityCalibrationGeometry> {
    // TCP and QUIC share competing-pipe sizing, but not proof mechanics: TCP
    // seeds from a full receiver-confirmed train and never truncates warmup.
    if candidate.underlay != UnderlayProtocol::Tcp
        || !product_delivery_samples_override_startup_prior(service_model.delivery_samples)
        || !service_model.rate_bps.is_finite()
        || service_model.rate_bps <= 0.0
    {
        return None;
    }
    let sample_floor_bytes = reliable_subflow_startup_sample_limit_bytes(mux_limits);
    let accounting_slack_bytes = (PATH_OPEN_SCORE_BYTES as u64).min(sample_floor_bytes / 8);
    let required_timed_carrier_bytes = sample_floor_bytes
        .saturating_sub(accounting_slack_bytes)
        .max(1);
    let timing_slack_bytes = capacity_timing_slack_bytes();
    let candidate_carrier_flight_bytes = candidate
        .bytes_in_flight
        .saturating_sub(candidate.product_bytes_in_flight);
    let competing_rate_bdp =
        (service_model.rate_bps / 8.0 * candidate.srtt_ms.max(1.0) / 1_000.0).ceil() as u64;
    let competing_rate_pipe = ((competing_rate_bdp as f64) * BBR_DEFAULT_CWND_GAIN).ceil() as u64;
    // A larger configured/startup cwnd is not native evidence. The exact flight
    // and the Service-rate pipe are the only warmup authorities available here.
    let warmup_carrier_bytes = candidate_carrier_flight_bytes
        .max(competing_rate_pipe)
        .max(PATH_OPEN_SCORE_BYTES as u64);
    let train_bytes = warmup_carrier_bytes
        .checked_add(timing_slack_bytes)?
        .checked_add(required_timed_carrier_bytes)?;
    let train_envelope_bytes = train_envelope_bytes.min(
        reliable_quic_capacity_calibration_session_limit_bytes(mux_limits),
    );
    if train_bytes > train_envelope_bytes {
        return None;
    }
    Some(RequestTcpCapacityCalibrationGeometry {
        train_bytes,
        sample_floor_bytes,
        accounting_slack_bytes,
        timing_slack_bytes,
        warmup_carrier_bytes,
        required_timed_carrier_bytes,
        service_rate_bps: service_model.rate_bps.ceil() as u64,
        candidate_carrier_flight_bytes,
    })
}

#[cfg(target_os = "linux")]
fn request_tcp_capacity_candidate_can_start_receipt(candidate: PathSnapshot) -> bool {
    // Product and unsent queue debt cannot enter a capacity epoch. Stale
    // control flight may remain locally unacknowledged: TCP ordering plus the
    // full typed receipt makes that delay conservative rather than ambiguous.
    candidate.queue_bytes == 0
        && candidate.product_bytes_in_flight == 0
        && candidate.product_queue_bytes == 0
        && candidate.active_latency_sensitive_flows == 0
        && candidate.session_active_latency_sensitive_flows == 0
}

fn request_quic_capacity_slow_start_rounds(train_bytes: u64) -> u32 {
    let mut rounds = 1_u32;
    let mut window_bytes = PATH_OPEN_SCORE_BYTES as u64;
    let mut cumulative_bytes = window_bytes;
    while cumulative_bytes < train_bytes {
        window_bytes = window_bytes.saturating_mul(2);
        cumulative_bytes = cumulative_bytes.saturating_add(window_bytes);
        rounds = rounds.saturating_add(1);
        if cumulative_bytes == u64::MAX {
            break;
        }
    }
    rounds
}

#[cfg(target_os = "linux")]
fn request_tcp_capacity_calibration_lease(
    candidate: PathSnapshot,
    train_bytes: u64,
    service_rate_bps: u64,
) -> Duration {
    let pto = transport_pto_from_snapshot(Some(candidate));
    // Ordinary loss can delay any cold congestion-growth round. Budget each
    // modeled round with the candidate PTO instead of assuming lossless
    // SRTT-paced doubling; this remains a deadline, so success finishes early.
    let growth = pto.saturating_mul(request_quic_capacity_slow_start_rounds(train_bytes));
    let service_transfer =
        Duration::from_secs_f64(train_bytes as f64 * 8.0 / service_rate_bps.max(1) as f64);
    // One PTO lets prior unsent control drain; the trailing PTO covers the
    // final typed receipt and ordinary recovery without a fixed margin.
    pto.saturating_add(growth.max(service_transfer))
        .saturating_add(pto)
        .max(Duration::from_secs(1))
}

fn request_quic_capacity_calibration_lease(candidate: PathSnapshot, train_bytes: u64) -> Duration {
    let pto = transport_pto_from_snapshot(Some(candidate));
    let srtt = Duration::from_secs_f64(candidate.srtt_ms.max(1.0) / 1_000.0);
    // The train intentionally starts on a cold QUIC congestion controller.
    // Budget the ACK-clock rounds needed to grow the RFC 9002 initial window;
    // half a PTO protects an RTT estimate that still comes from path proof.
    let modeled_round_trip = srtt.max(pto.div_f64(2.0));
    modeled_round_trip
        .saturating_mul(request_quic_capacity_slow_start_rounds(train_bytes))
        .saturating_add(pto)
        .max(Duration::from_secs(1))
}

fn response_quic_capacity_calibration_geometry(
    target: &ResponseSenderPathTarget,
    mux_limits: MuxLimits,
) -> ResponseQuicCapacityCalibrationGeometry {
    let sample_floor = reliable_subflow_startup_sample_limit_bytes(mux_limits);
    let packet_accounting_slack = (PATH_OPEN_SCORE_BYTES as u64).min(sample_floor / 8);
    let fresh_strict_window = sample_floor.saturating_sub(packet_accounting_slack).max(1);
    let timing_slack = capacity_timing_slack_bytes();
    let carrier_window = target
        .snapshot
        .inflight_limit_bytes
        .max(target.snapshot.bytes_in_flight);
    let session_envelope = reliable_quic_capacity_calibration_session_limit_bytes(mux_limits);
    let required_train = carrier_window
        .checked_add(fresh_strict_window)
        .and_then(|bytes| bytes.checked_add(timing_slack));
    let fits_session_envelope = required_train
        .map(|bytes| bytes.max(sample_floor))
        .is_some_and(|bytes| bytes <= session_envelope);
    let train_bytes = usize::try_from(
        required_train
            .unwrap_or(u64::MAX)
            .max(sample_floor)
            .min(session_envelope),
    )
    .unwrap_or(usize::MAX)
    .max(1);
    ResponseQuicCapacityCalibrationGeometry {
        train_bytes,
        fits_session_envelope,
        sample_floor_bytes: sample_floor,
        accounting_slack_bytes: packet_accounting_slack,
        fresh_strict_window_bytes: fresh_strict_window,
        carrier_window_bytes: carrier_window,
    }
}

#[cfg(test)]
fn response_quic_capacity_calibration_train_bytes(
    target: &ResponseSenderPathTarget,
    mux_limits: MuxLimits,
) -> usize {
    response_quic_capacity_calibration_geometry(target, mux_limits).train_bytes
}

fn response_quic_capacity_calibration_lease(
    target: &ResponseSenderPathTarget,
    train_bytes: usize,
) -> Duration {
    let pto = transport_pto_from_snapshot(Some(target.snapshot));
    let pacing_rate_bps = target
        .snapshot
        .pacing_rate_bps
        .max(target.snapshot.delivery_rate_bps)
        .max(1.0);
    let transmit_eta = Duration::from_secs_f64(train_bytes as f64 * 8.0 / pacing_rate_bps);
    // A healthy BBR startup grows within the persistent-congestion horizon.
    // Waiting longer would serialize useful retries behind a stale cold rate;
    // one additional PTO covers ACK/recovery after the bounded feed horizon.
    transmit_eta
        .min(pto.saturating_mul(QUIC_PERSISTENT_CONGESTION_THRESHOLD))
        .saturating_add(pto)
        .max(Duration::from_secs(1))
}

fn response_quic_capacity_proof_validity(target: &ResponseSenderPathTarget) -> Duration {
    let srtt = Duration::from_secs_f64((target.snapshot.srtt_ms.max(1.0)) / 1_000.0);
    let rttvar = Duration::from_secs_f64((target.snapshot.jitter_ms.max(1.0)) / 1_000.0);
    quic_bulk_proof_freshness_horizon(srtt, rttvar)
}

#[cfg(any(test, feature = "lab-diagnostics"))]
fn response_service_handoff_preserves_fair_share(
    service: &ResponseSenderPathTarget,
    target: &ResponseSenderPathTarget,
) -> bool {
    // Sticky placement compares one moved flow; only aggregate carrier rates
    // are divided because TCP product ACK clocks already measure a flow share.
    response_service_fair_share_bps(service, false) <= response_service_fair_share_bps(target, true)
}

fn response_service_fair_share_bps(target: &ResponseSenderPathTarget, adds_flow: bool) -> f64 {
    response_rate_fair_share_bps(target.snapshot, target.snapshot.rate_scope, adds_flow)
}

fn response_service_handoff_mode_for_targets(
    service: &ResponseSenderPathTarget,
    target: &ResponseSenderPathTarget,
    family_loads: ResponseServiceFamilyLoads,
) -> Option<ResponseServiceHandoffMode> {
    response_service_handoff_mode(
        service.key.underlay,
        response_service_fair_share_bps(service, false),
        target.key.underlay,
        response_service_fair_share_bps(target, true),
        family_loads,
    )
}

fn response_service_handoff_target_view(
    target: &ResponseSenderPathTarget,
    service_key: CarrierPathKey,
    lane: FlowLane,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    reservation: Option<ResponseServiceHandoffDrainReservation>,
    now: Instant,
) -> Option<ResponseSenderPathTarget> {
    let mut target = target.clone();
    let Some(reservation) = reservation else {
        return Some(target);
    };
    if now >= reservation.expires_at
        || reservation.target != target.key
        || reservation.target_path_instance_id != target.path_instance_id
        || reservation.target_incarnation != target.incarnation
    {
        return None;
    }
    let raw_capacity_proof = target.quic_capacity_proof;
    // A drain freezes the authority chosen at reservation time. Clear an
    // unrelated raw marker when this transaction deliberately uses generic
    // carrier evidence instead of receipt authority.
    target.quic_capacity_proof = reservation.capacity_proof;
    if let Some(proof) = reservation.capacity_proof {
        if target.key.underlay != UnderlayProtocol::Udp {
            return None;
        }
        if !quic_capacity_proof_pin_matches_marker(proof, raw_capacity_proof, now) {
            return None;
        }
        // The ordinary marker still expires; only this transaction view retains it.
        target.has_bulk_rate_evidence = true;
        target.snapshot.delivery_rate_bps = proof.rate_bps.max(1) as f64;
        target.snapshot.rate_scope = ResponseRateScope::PathCapacity;
        target.snapshot.confidence = target.snapshot.confidence.max(
            (proof.received_bytes as f64 / proof.sample_floor_bytes.max(1) as f64).clamp(0.0, 1.0),
        );
        target.eta_ms = server_bulk_output_eta_ms(
            target.key,
            target.snapshot,
            Some(service_key),
            lane,
            payload_bytes,
            mux_limits,
        );
    }
    Some(target)
}

fn response_service_handoff_start_capacity_proof(
    target: &ResponseSenderPathTarget,
    now: Instant,
) -> Option<QuicCapacityProofCandidate> {
    (target.key.underlay == UnderlayProtocol::Udp)
        .then_some(target.quic_capacity_proof)
        .flatten()
        .filter(|proof| valid_quic_capacity_proof_candidate_at(*proof, now))
}

#[derive(Clone)]
struct ResponseServiceHandoffCandidate {
    service: ResponseSenderPathTarget,
    target: ResponseSenderPathTarget,
    mode: ResponseServiceHandoffMode,
}

#[allow(clippy::too_many_arguments)]
fn select_response_service_handoff_candidate(
    targets: &[ResponseSenderPathTarget],
    lane: FlowLane,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    ordered_data_owner: Option<CarrierPathKey>,
    service_family_loads: ResponseServiceFamilyLoads,
    required_reservation: Option<ResponseServiceHandoffDrainReservation>,
) -> Option<ResponseServiceHandoffCandidate> {
    if !lane.is_bulk() {
        return None;
    }
    let service_key = ordered_data_owner?;
    let service = targets.iter().find(|target| target.key == service_key)?;
    if required_reservation.is_some_and(|reservation| {
        reservation.service != service.key
            || reservation.service_path_instance_id != service.path_instance_id
            || reservation.service_incarnation != service.incarnation
    }) {
        return None;
    }
    if !service.is_active
        || !service.has_bulk_rate_evidence
        || service.snapshot.active_latency_sensitive_flows > 0
        || service.snapshot.session_active_latency_sensitive_flows > 0
    {
        return None;
    }
    let now = Instant::now();
    let target = targets
        .iter()
        .filter_map(|target| {
            response_service_handoff_target_view(
                target,
                service.key,
                lane,
                payload_bytes,
                mux_limits,
                required_reservation,
                now,
            )
        })
        .filter(|target| {
            target.key.underlay != service.key.underlay
                && target.attachment_role == StreamOpenRole::Validation
                && !target.is_active
                && target.has_bulk_rate_evidence
                && target.owner_data_in_flight_bytes == 0
                && target.snapshot.product_bytes_in_flight == 0
                && target.snapshot.active_latency_sensitive_flows == 0
                && target.snapshot.session_active_latency_sensitive_flows == 0
                && response_service_handoff_mode_for_targets(service, target, service_family_loads)
                    .is_some()
                && target.commands.can_enqueue_lane_now(lane)
                && response_owner_bulk_model_suppression(
                    target,
                    ResponseBulkLead {
                        key: service.key,
                        snapshot: service.snapshot,
                        eta_ms: service.eta_ms,
                    },
                    None,
                    0,
                    0,
                    payload_bytes,
                    mux_limits,
                    BulkAdmissionRole::AdditionalCrossUnderlay,
                )
                .is_none()
                && response_target_has_emission_credit(target, lane, payload_bytes, mux_limits)
        })
        .min_by(|left, right| {
            left.eta_ms
                .total_cmp(&right.eta_ms)
                .then_with(|| carrier_path_key_order(left.key, right.key))
                .then_with(|| left.incarnation.cmp(&right.incarnation))
        })?;
    let mode = response_service_handoff_mode_for_targets(service, &target, service_family_loads)?;
    Some(ResponseServiceHandoffCandidate {
        service: service.clone(),
        target,
        mode,
    })
}

fn select_response_quic_capacity_calibration_target(
    targets: &[ResponseSenderPathTarget],
    lane: FlowLane,
    ordered_data_owner: Option<CarrierPathKey>,
    service_family_loads: ResponseServiceFamilyLoads,
    mux_limits: MuxLimits,
    remaining_probe_bytes: u64,
) -> Option<ResponseSenderPathTarget> {
    if !lane.is_bulk() {
        return None;
    }
    let service_key = ordered_data_owner?;
    let service = targets.iter().find(|target| target.key == service_key)?;
    if service.key.underlay != UnderlayProtocol::Tcp
        || !service.is_active
        || !service.has_bulk_rate_evidence
        || service.snapshot.active_latency_sensitive_flows > 0
        || service.snapshot.session_active_latency_sensitive_flows > 0
        || service_family_loads.for_underlay(UnderlayProtocol::Tcp)
            < service_family_loads
                .for_underlay(UnderlayProtocol::Udp)
                .saturating_add(2)
    {
        return None;
    }
    if targets.iter().any(|target| {
        target.key.underlay == UnderlayProtocol::Udp
            && target.has_bulk_rate_evidence
            && response_service_handoff_mode_for_targets(service, target, service_family_loads)
                .is_some()
    }) {
        // A measured target that already clears the placement gate should drain
        // toward handoff; probing a second path would add optional traffic only.
        return None;
    }
    targets
        .iter()
        .filter(|target| {
            target.key.underlay == UnderlayProtocol::Udp
                && target.attachment_role == StreamOpenRole::Validation
                && !target.is_active
                && target.has_sender_evidence
                && !target.has_bulk_rate_evidence
                && target.quic_capacity_calibration_attempts
                    < MAX_RESPONSE_QUIC_CAPACITY_CALIBRATION_ATTEMPTS_PER_PATH
                && target.command_pending_bytes == 0
                && target.snapshot.queue_bytes == 0
                && target.snapshot.bytes_in_flight == 0
                && target.snapshot.active_latency_sensitive_flows == 0
                && target.snapshot.session_active_latency_sensitive_flows == 0
                && target.commands.can_enqueue_lane_now(FlowLane::Throughput)
                && {
                    let geometry = response_quic_capacity_calibration_geometry(target, mux_limits);
                    geometry.fits_session_envelope
                        && geometry.train_bytes as u64 <= remaining_probe_bytes
                }
        })
        // Attachment order must not consume discovery opportunity: sample each
        // exact reachable path once before spending a second attempt on one.
        .min_by(|left, right| {
            (left.quic_capacity_calibration_attempts != 0)
                .cmp(&(right.quic_capacity_calibration_attempts != 0))
                .then_with(|| left.eta_ms.total_cmp(&right.eta_ms))
                .then_with(|| carrier_path_key_order(left.key, right.key))
                .then_with(|| left.incarnation.cmp(&right.incarnation))
        })
        .cloned()
}

#[cfg(target_os = "linux")]
fn select_response_tcp_capacity_probe_target(
    targets: &[ResponseSenderPathTarget],
    lane: FlowLane,
    ordered_data_owner: Option<CarrierPathKey>,
    service_family_loads: ResponseServiceFamilyLoads,
    mux_limits: MuxLimits,
) -> Option<(ResponseSenderPathTarget, u64)> {
    if !lane.is_bulk() {
        return None;
    }
    let service_key = ordered_data_owner?;
    let service = targets.iter().find(|target| target.key == service_key)?;
    if service.key.underlay != UnderlayProtocol::Tcp
        || !service.is_active
        || !service.has_bulk_rate_evidence
        || service.snapshot.active_latency_sensitive_flows > 0
        || service.snapshot.session_active_latency_sensitive_flows > 0
    {
        return None;
    }
    if targets.iter().any(|target| {
        target.key.underlay == UnderlayProtocol::Udp
            && target.has_bulk_rate_evidence
            && response_service_handoff_mode_for_targets(service, target, service_family_loads)
                .is_some()
    }) {
        // A measured cross-family target that can take Service outranks
        // optional same-family discovery on the shared product session.
        return None;
    }
    // This train owns no product offset. Requiring a product Subflow first
    // serializes two independent discovery mechanisms and delays cold paths.
    let envelope = reliable_quic_capacity_calibration_session_limit_bytes(mux_limits);
    let sample_floor = reliable_subflow_startup_sample_limit_bytes(mux_limits);
    let train_bytes = (2 * 1024 * 1024u64).min(envelope).max(sample_floor).max(1);
    targets
        .iter()
        .filter(|target| {
            target.key != service_key
                && target.key.underlay == UnderlayProtocol::Tcp
                && target.attachment_role == StreamOpenRole::Validation
                && !target.is_active
                && target.has_sender_evidence
                && !target.has_bulk_rate_evidence
                && !target.commands.tcp_capacity_probe_attempted()
                && !target.commands.tcp_capacity_probe_active()
                && target.command_pending_bytes == 0
                && target.snapshot.queue_bytes == 0
                && target.snapshot.bytes_in_flight == 0
                && target.snapshot.active_latency_sensitive_flows == 0
                && target.snapshot.session_active_latency_sensitive_flows == 0
                && target.commands.can_enqueue_lane_now(FlowLane::Throughput)
        })
        .min_by(|left, right| {
            left.eta_ms
                .total_cmp(&right.eta_ms)
                .then_with(|| carrier_path_key_order(left.key, right.key))
                .then_with(|| left.incarnation.cmp(&right.incarnation))
        })
        .cloned()
        .map(|target| (target, train_bytes))
}

#[allow(clippy::too_many_arguments)]
fn select_response_service_handoff_target(
    targets: &[ResponseSenderPathTarget],
    lane: FlowLane,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    lower_flights: &[CarrierPathFlightDebt],
    ordered_data_owner: Option<CarrierPathKey>,
    ordered_owner_debt_bytes: usize,
    service_family_loads: ResponseServiceFamilyLoads,
    handoff_frontier: u64,
    required_reservation: Option<ResponseServiceHandoffDrainReservation>,
) -> Option<ResponseSelectedDataTarget> {
    if !lane.is_bulk() || ordered_owner_debt_bytes > 0 || !lower_flights.is_empty() {
        return None;
    }
    let candidate = select_response_service_handoff_candidate(
        targets,
        lane,
        payload_bytes,
        mux_limits,
        ordered_data_owner,
        service_family_loads,
        required_reservation,
    )?;
    let service = candidate.service;
    let target = candidate.target;
    let target_command_pending_limit_bytes = u64::try_from(
        response_target_emission_credit_bytes(&target, lane, payload_bytes, mux_limits)
            .saturating_sub(payload_bytes),
    )
    .unwrap_or(u64::MAX);
    debug_assert!(target.commands.pending_bytes() <= target_command_pending_limit_bytes);

    Some(ResponseSelectedDataTarget {
        target: target.clone(),
        admission: PathAdmission::service(),
        service_handoff_commit: Some(ResponseServiceHandoffCommit {
            planner_generation: 0,
            lane_generation: 0,
            model_generation: 0,
            handoff_frontier,
            service: service.key,
            service_path_instance_id: service.path_instance_id,
            service_incarnation: service.incarnation,
            target_path_instance_id: target.path_instance_id,
            mode: candidate.mode,
            target_command_pending_limit_bytes,
            capacity_proof: required_reservation
                .map(|reservation| reservation.capacity_proof)
                .unwrap_or_else(|| {
                    response_service_handoff_start_capacity_proof(&target, Instant::now())
                }),
        }),
        subflow_set_commit: None,
        ack_clock_calibration_commit: None,
    })
}

fn response_service_handoff_drain_matches_candidate(
    binding_instance_id: u64,
    reservation: ResponseServiceHandoffDrainReservation,
    candidate: &ResponseServiceHandoffCandidate,
) -> bool {
    reservation.binding_instance_id == binding_instance_id
        && reservation.service == candidate.service.key
        && reservation.service_path_instance_id == candidate.service.path_instance_id
        && reservation.service_incarnation == candidate.service.incarnation
        && reservation.target == candidate.target.key
        && reservation.target_path_instance_id == candidate.target.path_instance_id
        && reservation.target_incarnation == candidate.target.incarnation
        && reservation.capacity_proof == candidate.target.quic_capacity_proof
}

fn response_service_handoff_drain_matches_selection(
    binding_instance_id: u64,
    reservation: ResponseServiceHandoffDrainReservation,
    selection: &ResponseSelectedDataTarget,
) -> bool {
    let Some(commit) = selection.service_handoff_commit else {
        return false;
    };
    reservation.binding_instance_id == binding_instance_id
        && reservation.service == commit.service
        && reservation.service_path_instance_id == commit.service_path_instance_id
        && reservation.service_incarnation == commit.service_incarnation
        && reservation.target == selection.target.key
        && reservation.target_path_instance_id == commit.target_path_instance_id
        && reservation.target_incarnation == selection.target.incarnation
        && reservation.capacity_proof == commit.capacity_proof
}

fn response_service_handoff_drain_lease(
    service: &ResponseSenderPathTarget,
    outstanding_owner_bytes: u64,
) -> Duration {
    let rate_bps = response_service_fair_share_bps(service, false)
        .max(default_path_rate_bps(service.key.underlay))
        .max(1.0);
    let transmit_seconds = outstanding_owner_bytes as f64 * 8.0 / rate_bps;
    let transmit_eta = Duration::from_secs_f64(transmit_seconds);
    let recovery_margin = transport_pto_from_snapshot(Some(service.snapshot))
        .saturating_mul(QUIC_PERSISTENT_CONGESTION_THRESHOLD);
    // Fresh assignment pauses while already-owned bytes continue draining. Size
    // the lease from this binding's share; a five-second cap made a default
    // 2 MiB window impossible to move on a healthy 1 Mbit/s path.
    transmit_eta
        .saturating_add(recovery_margin)
        .max(Duration::from_secs(1))
        .min(Duration::from_secs(60))
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
            let (candidate_snapshot, candidate_eta_ms, _uses_service_prior) =
                response_tcp_calibration_opportunity_candidate(
                    service,
                    target,
                    lane,
                    payload_bytes,
                    mux_limits,
                );
            let opportunity = reliable_tcp_ack_clock_calibration_opportunity(
                service.snapshot,
                service.eta_ms,
                candidate_snapshot,
                candidate_eta_ms,
                exploration_bytes,
                payload_bytes,
                mux_limits,
            );
            #[cfg(feature = "lab-diagnostics")]
            let projection = opportunity.projection();
            let admitted = opportunity.is_admitted();
            #[cfg(feature = "lab-diagnostics")]
            {
                lab_response_ack_clock_calibration_admission(
                    target,
                    service,
                    candidate_snapshot,
                    candidate_eta_ms,
                    _uses_service_prior,
                    projection,
                    admitted,
                );
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
            service_handoff_commit: None,
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
                requires_active_response_start: !target.ack_clock_calibration_active
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

fn response_calibration_service_reservoir_has_credit(
    ordered_owner_debt_bytes: usize,
    calibration_prefix_limit_bytes: u64,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> bool {
    let product_envelope = bulk_service_product_envelope_payload_bytes(payload_bytes, mux_limits);
    let calibration_prefix_limit = usize::try_from(calibration_prefix_limit_bytes)
        .unwrap_or(usize::MAX)
        .min(product_envelope);
    let reservoir = bulk_service_feed_reservoir_payload_bytes(payload_bytes, mux_limits)
        .saturating_add(calibration_prefix_limit)
        .min(product_envelope);
    ordered_owner_debt_bytes.saturating_add(payload_bytes) <= reservoir
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
    completion_backlog_bytes: u64,
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
    bulk_candidate_admission_suppression_with_completion_backlog(
        BulkAdmissionCheck {
            best_snapshot: lead.snapshot,
            best_eta_ms: lead.eta_ms,
            candidate_snapshot: response_target_measured_admission_snapshot(target),
            candidate_eta_ms: target.eta_ms,
            payload_bytes,
            mux_limits,
            role,
            stream_ordering_debt_bytes: effective_ordering_debt,
        },
        completion_backlog_bytes,
    )
}

fn response_fallback_bulk_model_suppression(
    target: &ResponseSenderPathTarget,
    lead: ResponseBulkLead,
    ordering_debt: u64,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    role: BulkAdmissionRole,
) -> Option<&'static str> {
    // This is response-owned lower flight, so it is real Service completion
    // backlog. Request receive holes carry no such authority.
    bulk_candidate_admission_suppression_with_completion_backlog(
        BulkAdmissionCheck {
            best_snapshot: lead.snapshot,
            best_eta_ms: lead.eta_ms,
            candidate_snapshot: target.snapshot,
            candidate_eta_ms: target.eta_ms,
            payload_bytes,
            mux_limits,
            role,
            stream_ordering_debt_bytes: ordering_debt,
        },
        ordering_debt,
    )
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
    let completion_backlog_bytes = ordering_debt.max(ordered_tail_debt.global_bytes());
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
            completion_backlog_bytes,
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
    let current_startup_owner_continues_lower_frontier = startup_sampling_allowed
        && continues_lower_frontier
        && target.key != service_key
        && !target.has_bulk_rate_evidence
        && subflow_set.is_some_and(|epoch| {
            epoch.service_key() == service_key && epoch.startup_owner_key() == Some(target.key)
        })
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
        && !current_startup_owner_continues_lower_frontier
    {
        // Only the exact bounded startup owner or an already measured Subflow
        // may continue its own authoritative lower frontier.
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
    let existing_startup_owner = subflow_set.is_some_and(|epoch| {
        epoch.service_key() == service_key && epoch.startup_owner_key() == Some(target.key)
    });
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
                ) && (existing_startup_owner
                    || response_startup_sample_has_completion_opportunity(
                        candidates,
                        service,
                        target,
                        payload_bytes,
                        mux_limits,
                    ))
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
        completion_backlog_bytes,
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
            response_fallback_bulk_model_suppression(
                target,
                lead,
                ordering_debt,
                payload_bytes,
                mux_limits,
                role,
            )
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
        service_handoff_commit: None,
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
            service_handoff_commit: None,
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
    // so only while the active-response start gate is open; dormant state blocks
    // only its exact target below. QUIC remains under its carrier ACK controller.
    let tcp_calibration_reservoir_prefix_bytes = targets
        .iter()
        .filter(|target| response_ack_clock_calibration_pending(target, startup_sampling_allowed))
        .map(|target| target.ack_clock_calibration_credit_limit_bytes)
        .max();
    let tcp_calibration_serialized = tcp_calibration_reservoir_prefix_bytes.is_some();
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
    // or the same-family ownership-aware view.
    // A calibration stage needs isolated product ACK coverage. Keep ordinary
    // same-family reservoir work out until its exact flights drain; Service
    // remains the fallback and each carrier controller continues below.
    let same_family_reservoir = (!tcp_calibration_serialized && effective_lower_owner.is_none())
        .then(|| {
            response_same_family_reservoir_policy(
                &admitted,
                ordered_tail,
                payload_bytes,
                mux_limits,
            )
        })
        .flatten();
    for target in candidate_targets
        .iter()
        .copied()
        .filter(|target| target.key != service_key)
    {
        let ordering_debt = response_ordering_debt_bytes(lower_flights, target.key);
        let candidate_debt = same_family_reservoir
            .filter(|reservoir| {
                response_target_is_same_family_reservoir_candidate(*reservoir, target)
            })
            .map_or_else(
                || ordered_tail.for_candidate(target.key),
                |reservoir| response_same_family_reservoir_candidate_debt(reservoir, target),
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
    if let Some(reservoir) = same_family_reservoir
        && let Some(subflow_target) =
            response_same_family_reservoir_subflow_target(&admitted, reservoir)
    {
        #[cfg(feature = "lab-diagnostics")]
        lab_response_bulk_output_selected(
            "same_family_subflow_reservoir",
            &subflow_target,
            payload_bytes,
        );
        return Some(subflow_target);
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
    if tcp_calibration_serialized
        && !response_calibration_service_reservoir_has_credit(
            ordered_owner_debt_bytes,
            tcp_calibration_reservoir_prefix_bytes.unwrap_or(0),
            payload_bytes,
            mux_limits,
        )
    {
        // The calibration opportunity projected only this much Service work
        // behind the candidate prefix. Stop assigning offsets at that boundary
        // until exact ACK progress shrinks the ordered tail.
        return None;
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

fn response_same_family_reservoir_policy(
    admitted: &[ResponseSelectedDataTarget],
    ordered_tail: ResponseOrderedTail,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> Option<ResponseSameFamilyReservoir> {
    let service = response_feedable_service_owner_target(admitted)?;
    if !service.target.is_active
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
    // Same-family proven paths may drain a bulk backlog concurrently, but a
    // full resource envelope can become tens of MiB of receiver-prefix debt.
    // The BBR-shaped feed reservoir preserves aggregation headroom while
    // keeping cross-path ownership close enough for latency-sensitive takeover.
    let ordered_reservoir = bulk_service_feed_reservoir_payload_bytes(payload_bytes, mux_limits);

    ResponseSameFamilyReservoir::new(
        service.target.key,
        ordered_tail,
        service_assigned,
        service_horizon,
        ordered_reservoir,
        payload_bytes,
    )
}

fn response_target_is_same_family_reservoir_candidate(
    reservoir: ResponseSameFamilyReservoir,
    target: &ResponseSenderPathTarget,
) -> bool {
    target.key != reservoir.service()
        && target.key.underlay == reservoir.service().underlay
        && !target.is_active
        && target.has_bulk_rate_evidence
        && target.snapshot.active_latency_sensitive_flows == 0
        && target.snapshot.session_active_latency_sensitive_flows == 0
}

fn response_same_family_reservoir_candidate_debt(
    reservoir: ResponseSameFamilyReservoir,
    target: &ResponseSenderPathTarget,
) -> ResponseCandidateTailDebt {
    // The global tail contains unique OwnerData. Subtract only this candidate's
    // unique share; generic carrier admission separately keeps every OwnerData
    // and RepairData copy charged as product flight.
    reservoir.for_candidate(target.key, target.owner_data_in_flight_bytes)
}

fn response_same_family_reservoir_subflow_target(
    admitted: &[ResponseSelectedDataTarget],
    reservoir: ResponseSameFamilyReservoir,
) -> Option<ResponseSelectedDataTarget> {
    // This reservoir independently bounds cross-path ordering exposure inside
    // the larger source envelope. Keep the first horizon on Service, then let
    // one measured same-family Subflow use the remaining bounded partition.
    let service = admitted
        .iter()
        .find(|selected| selected.target.key == reservoir.service())?;
    admitted
        .iter()
        .filter(|selected| {
            selected.admission.role == PathRuntimeRole::Subflow
                && response_target_is_same_family_reservoir_candidate(reservoir, &selected.target)
                // Separate QUIC connections own independent congestion
                // controllers. Crossing into an equally loaded connection
                // only creates product reordering; require real load relief.
                && (selected.target.key.underlay != UnderlayProtocol::Udp
                    || response_target_active_bulk_flows(&service.target)
                        > response_target_active_bulk_flows(&selected.target))
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

fn response_target_active_bulk_flows(target: &ResponseSenderPathTarget) -> u32 {
    target
        .snapshot
        .active_flows
        .saturating_sub(target.snapshot.active_latency_sensitive_flows)
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
    if !target.has_service_feed_evidence {
        return response_service_startup_emission_credit_bytes(
            target.key.underlay,
            payload_bytes,
            mux_limits,
        );
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
    underlay: UnderlayProtocol,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> usize {
    if underlay == UnderlayProtocol::Udp {
        bulk_service_feed_reservoir_payload_bytes(payload_bytes, mux_limits)
    } else {
        bulk_service_horizon_payload_bytes(payload_bytes, mux_limits)
    }
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
            let lane = reliable_work_lane_to_carrier_lane(ReliableWorkClass::Data, relay_lane);
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
                let session_scheduling = binding.response_scheduling_snapshot();
                let lane_generation = session_scheduling.generation;
                let active_response_flows = session_scheduling.active_response_flows;
                let model_generation = binding.response_model_generation();
                let lower_flights = binding.lower_flights_before_offset(next_offset);
                let targets = binding.sender_path_targets(relay_lane, payload_bytes);
                let ordered_data_owner = binding.ordered_data_owner();
                #[cfg(target_os = "linux")]
                if !session_scheduling.tcp_capacity_probe_reserved
                    && let Some((target, train_bytes)) = select_response_tcp_capacity_probe_target(
                        &targets,
                        relay_lane,
                        ordered_data_owner,
                        session_scheduling.service_family_loads,
                        binding.mux_limits(),
                    )
                    && let Some(expires_at) = Instant::now().checked_add(Duration::from_secs(20))
                    && let Some(session_lease) =
                        binding.try_reserve_tcp_capacity_probe(lane_generation)
                {
                    let calibration_id = match target.commands.try_enqueue_tcp_capacity_probe(
                        TcpCapacityProbeRequest {
                            path_id: target.key.path_id,
                            path_instance_id: target.path_instance_id,
                            train_payload_bytes: train_bytes,
                            sample_floor_bytes: reliable_subflow_startup_sample_limit_bytes(
                                binding.mux_limits(),
                            ),
                            expires_at,
                        },
                        session_lease,
                    ) {
                        Ok(calibration_id) => calibration_id,
                        Err(err) => return Err(err),
                    };
                    #[cfg(not(feature = "lab-diagnostics"))]
                    let _ = calibration_id;
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "response_tcp_capacity_probe",
                        format_args!(
                            "phase=started session_id={} binding_instance_id={} path_id={} path_instance_id={} incarnation={} calibration_id={} train_bytes={}",
                            target.session_id.0,
                            target.binding_instance_id,
                            target.key.path_id.0,
                            target.path_instance_id.as_u64(),
                            target.incarnation,
                            calibration_id,
                            train_bytes,
                        ),
                    );
                    // Reservation advances the shared generation; resnapshot
                    // before any product decision.
                    continue;
                }
                let capacity_session_limit =
                    reliable_quic_capacity_calibration_session_limit_bytes(binding.mux_limits());
                let remaining_capacity_probe_bytes = capacity_session_limit
                    .saturating_sub(session_scheduling.quic_capacity_calibration_spent_bytes);
                if !session_scheduling.quic_capacity_calibration_reserved
                    && session_scheduling.response_service_handoff_drain.is_none()
                    && session_scheduling
                        .service_family_loads
                        .needs_diversification()
                    && let Some(target) = select_response_quic_capacity_calibration_target(
                        &targets,
                        relay_lane,
                        ordered_data_owner,
                        session_scheduling.service_family_loads,
                        binding.mux_limits(),
                        remaining_capacity_probe_bytes,
                    )
                    && {
                        let geometry = response_quic_capacity_calibration_geometry(
                            &target,
                            binding.mux_limits(),
                        );
                        let train_bytes = geometry.train_bytes;
                        let lease = response_quic_capacity_calibration_lease(&target, train_bytes);
                        binding.try_start_quic_capacity_calibration(
                            &target,
                            ResponseQuicCapacityCalibrationRequest {
                                expected_planner_generation: planner_generation,
                                expected_lane_generation: lane_generation,
                                expected_model_generation: model_generation,
                                target: target.key,
                                target_path_instance_id: target.path_instance_id,
                                target_incarnation: target.incarnation,
                                target_pending_bytes: target.command_pending_bytes,
                                train_bytes,
                                sample_floor_bytes: geometry.sample_floor_bytes,
                                accounting_slack_bytes: geometry.accounting_slack_bytes,
                                fresh_strict_window_bytes: geometry.fresh_strict_window_bytes,
                                carrier_window_bytes: geometry.carrier_window_bytes,
                                proof_validity: response_quic_capacity_proof_validity(&target),
                                lease,
                            },
                        )
                    }
                {
                    // Reservation and command admission change the session and
                    // response-model generations. Replan ordinary OwnerData.
                    continue;
                }
                let binding_instance_id = binding.binding_instance_id();
                let current_drain = session_scheduling
                    .response_service_handoff_drain
                    .filter(|reservation| reservation.binding_instance_id == binding_instance_id);
                let another_binding_is_draining = session_scheduling
                    .response_service_handoff_drain
                    .is_some_and(|reservation| {
                        reservation.binding_instance_id != binding_instance_id
                    });
                let handoff_open = binding.response_service_handoff_open();
                let startup_owner_active = subflow_set
                    .as_ref()
                    .and_then(FlowSubflowSet::startup_owner_key)
                    .is_some();
                let calibration_active = targets
                    .iter()
                    .any(|target| target.ack_clock_calibration_active);
                let handoff_context_ready =
                    handoff_open && !startup_owner_active && !calibration_active;
                #[cfg(feature = "lab-diagnostics")]
                lab_response_service_handoff_evaluation(
                    binding,
                    &targets,
                    relay_lane,
                    payload_bytes,
                    binding.mux_limits(),
                    &lower_flights,
                    ordered_data_owner,
                    ordered_owner_debt_bytes,
                    session_scheduling.service_family_loads,
                    current_drain,
                    handoff_open,
                    startup_owner_active,
                    calibration_active,
                    another_binding_is_draining,
                    planner_generation,
                    lane_generation,
                    model_generation,
                );
                if handoff_context_ready
                    && !another_binding_is_draining
                    && let Some(mut selected) = select_response_service_handoff_target(
                        &targets,
                        relay_lane,
                        payload_bytes,
                        binding.mux_limits(),
                        &lower_flights,
                        ordered_data_owner,
                        ordered_owner_debt_bytes,
                        session_scheduling.service_family_loads,
                        next_offset,
                        current_drain,
                    )
                {
                    debug_assert!(current_drain.is_none_or(|reservation| {
                        response_service_handoff_drain_matches_selection(
                            binding_instance_id,
                            reservation,
                            &selected,
                        )
                    }));
                    let commit = selected
                        .service_handoff_commit
                        .as_mut()
                        .expect("response Service handoff selection has a commit");
                    commit.planner_generation = planner_generation;
                    commit.lane_generation = lane_generation;
                    commit.model_generation = model_generation;
                    #[cfg(feature = "lab-diagnostics")]
                    lab_response_bulk_output_selected("service_handoff", &selected, payload_bytes);
                    return Ok(ResponseDataDispatchPlan {
                        primary: ResponseDataDispatchTarget::Switchable {
                            binding: binding.clone(),
                            target: selected.target.into(),
                            role: PathRuntimeRole::Service,
                            service_handoff_commit: selected.service_handoff_commit,
                            subflow_set_commit: None,
                            ack_clock_calibration_commit: None,
                        },
                    });
                }
                let handoff_candidate = (handoff_context_ready && !another_binding_is_draining)
                    .then(|| {
                        select_response_service_handoff_candidate(
                            &targets,
                            relay_lane,
                            payload_bytes,
                            binding.mux_limits(),
                            ordered_data_owner,
                            session_scheduling.service_family_loads,
                            current_drain,
                        )
                    })
                    .flatten();
                if let Some(reservation) = current_drain {
                    if handoff_candidate.as_ref().is_some_and(|candidate| {
                        response_service_handoff_drain_matches_candidate(
                            binding_instance_id,
                            reservation,
                            candidate,
                        )
                    }) {
                        // Only this binding pauses fresh OwnerData. Control and
                        // critical repair still preempt the blocked data lane.
                        return Err(RuntimeError::SenderServiceBlocked);
                    }
                    binding.cancel_response_service_handoff_drain("eligibility_regressed");
                    continue;
                }
                if let Some(candidate) = handoff_candidate {
                    let lower_flight_bytes = lower_flights
                        .iter()
                        .fold(0u64, |total, flight| total.saturating_add(flight.bytes));
                    let outstanding_owner_bytes = u64::try_from(ordered_owner_debt_bytes)
                        .unwrap_or(u64::MAX)
                        .max(lower_flight_bytes)
                        .max(candidate.service.owner_data_in_flight_bytes);
                    let lease = response_service_handoff_drain_lease(
                        &candidate.service,
                        outstanding_owner_bytes,
                    );
                    if binding.try_start_response_service_handoff_drain(
                        &candidate.service,
                        &candidate.target,
                        relay_lane,
                        ResponseServiceHandoffDrainRequest {
                            expected_planner_generation: planner_generation,
                            expected_lane_generation: lane_generation,
                            expected_model_generation: model_generation,
                            service: candidate.service.key,
                            service_path_instance_id: candidate.service.path_instance_id,
                            service_incarnation: candidate.service.incarnation,
                            target: candidate.target.key,
                            target_path_instance_id: candidate.target.path_instance_id,
                            target_incarnation: candidate.target.incarnation,
                            mode: candidate.mode,
                            capacity_proof: response_service_handoff_start_capacity_proof(
                                &candidate.target,
                                Instant::now(),
                            ),
                            outstanding_owner_bytes,
                            lease,
                        },
                    ) {
                        return Err(RuntimeError::SenderServiceBlocked);
                    }
                }
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
                        active_response_flows
                            >= MIN_ACTIVE_RESPONSE_FLOWS_FOR_SAME_FAMILY_DISCOVERY,
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
                        target: target.into(),
                        role,
                        service_handoff_commit: selected.service_handoff_commit,
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
            service_handoff_commit,
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
                    requires_active_response_start: commit.requires_active_response_start,
                });
            let calibrating = calibration_request.is_some();
            let handoff = service_handoff_commit.is_some();
            let enqueue_result = if let Some(commit) = service_handoff_commit {
                binding
                    .try_enqueue_response_service_handoff_for_dispatch(
                        &target,
                        &frame,
                        lane,
                        ResponseServiceHandoffRequest {
                            expected_planner_generation: commit.planner_generation,
                            expected_lane_generation: commit.lane_generation,
                            expected_model_generation: commit.model_generation,
                            handoff_frontier: commit.handoff_frontier,
                            service: commit.service,
                            service_path_instance_id: commit.service_path_instance_id,
                            service_incarnation: commit.service_incarnation,
                            target: target.key,
                            target_path_instance_id: commit.target_path_instance_id,
                            target_incarnation: target.incarnation,
                            mode: commit.mode,
                            target_command_pending_limit_bytes: commit
                                .target_command_pending_limit_bytes,
                            capacity_proof: commit.capacity_proof,
                        },
                    )
                    .map(|()| None)
            } else {
                binding.try_enqueue_owner_frame_for_dispatch_target(
                    &target,
                    &frame,
                    lane,
                    subflow_request,
                    calibration_request,
                )
            };
            match enqueue_result {
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
                let _ = binding.commit_ordered_data_owner_for_dispatch_target(&target);
            }
            let decision_reason = match role {
                PathRuntimeRole::Service if handoff => "data_service_handoff",
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
                let dispatch_target = ResponseDispatchTarget::from(&target);
                let send_result = if matches!(frame, Frame::StreamData { .. }) {
                    if repair {
                        binding
                            .try_enqueue_repair_frame_for_target(&target, &frame, lane)
                            .map(|()| None)
                    } else {
                        binding.try_enqueue_owner_frame_for_dispatch_target(
                            &dispatch_target,
                            &frame,
                            lane,
                            None,
                            None,
                        )
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
                                let _ = binding.commit_ordered_data_owner_for_dispatch_target(
                                    &dispatch_target,
                                );
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

    pub(super) fn front_is_data(&self) -> bool {
        self.queue
            .front()
            .is_some_and(|(_, work)| matches!(&work.kind, ReliableRelayQueuedWorkKind::Data(_)))
    }

    pub(super) fn drain_allows_bounded_source_staging(
        &self,
        path_stream: &ReliablePathStream,
        queued_send_blocked: bool,
    ) -> bool {
        queued_send_blocked
            && self.front_is_data()
            && path_stream.response_service_handoff_drain_active()
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
            ReliableWorkClass::Control => {
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
            ReliableWorkClass::Data => match emit_response_frame_from_sender_service(
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
            ReliableWorkClass::Repair => {
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
        queued_lane: ReliableWorkClass,
        committed: ReliableRelayQueuedWork,
        frame: Frame,
        selected_path: Option<CarrierPathKey>,
        dispatch_lane_name: &'static str,
        enqueue_id: u64,
        queue_delay_ms: u128,
    ) -> Result<ServerResponseDispatch, RuntimeError> {
        #[cfg(feature = "lab-diagnostics")]
        let send_lane = match queued_lane {
            ReliableWorkClass::Control => FlowLane::Control,
            ReliableWorkClass::Repair => response_repair_carrier_lane(&frame),
            ReliableWorkClass::Data => reliable_path_effective_frame_lane(
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
            if queued_lane == ReliableWorkClass::Data {
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
            } else if queued_lane == ReliableWorkClass::Data
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
            request_per_flow_rate_bps: HashMap::new(),
            request_tcp_ack_turnover: HashMap::new(),
            request_rate_proven_subflows: HashSet::new(),
            request_ack_clock_first_window_subflows: HashSet::new(),
            request_ack_clock_proven_subflows: HashSet::new(),
            request_window_turnover_proven_subflows: HashSet::new(),
            request_ack_clock_calibration_bytes: HashMap::new(),
            request_ack_clock_calibration_targets: HashMap::new(),
            request_ack_clock_calibration_owner: None,
            request_ack_clock_calibration_pending: None,
            request_tcp_capacity_calibrations: HashMap::new(),
            request_tcp_capacity_attempted_paths: HashSet::new(),
            request_tcp_capacity_proven_subflows: HashSet::new(),
            request_tcp_capacity_campaign: Arc::new(RequestCapacityProbeCampaignBudget::default()),
            #[cfg(feature = "lab-diagnostics")]
            request_tcp_capacity_last_gate: None,
            request_quic_capacity_calibration: None,
            request_quic_capacity_attempted_paths: HashSet::new(),
            request_quic_capacity_campaign: Arc::new(RequestCapacityProbeCampaignBudget::default()),
            request_graduated_subflows: HashSet::new(),
            request_attempted_subflows: HashSet::new(),
            request_membership_generation: None,
            request_bulk_flow_registration: None,
            missing_owner_repair_attempts: HashMap::new(),
            next_send_index: 0,
            performance,
            extra_traffic: ExtraTrafficLedger::default(),
        }
    }

    pub(super) fn bind_request_bulk_flow_registration(
        &mut self,
        registration: ReliableTcpRequestBulkFlowRegistration,
    ) {
        self.request_bulk_flow_registration = Some(registration);
    }

    pub(super) async fn fail_client_path_instance(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        instance: RelayPathInstance,
    ) -> bool {
        // Removal cleanup can await a full carrier queue. Only losing an
        // active Service invalidates logical contention; an optional
        // Validation failure must not hide the still-live Service meanwhile.
        let removes_active_service = remotes.paths.iter().any(|path| {
            path.instance() == instance && path.placement == RelayPathPlacement::Active
        });
        if removes_active_service && let Some(registration) = &self.request_bulk_flow_registration {
            registration.update(false, None);
        }
        remotes.fail_path_instance(context, instance).await
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

    fn record_request_per_flow_rate_sample(
        &mut self,
        instance: RelayPathInstance,
        sample: PathRateSample,
        replace: bool,
    ) {
        if instance.key.underlay != UnderlayProtocol::Tcp {
            return;
        }
        let sample_bps = sample.rate_bps();
        let previous = self.request_per_flow_rate_bps.get(&instance).copied();
        let model = if replace {
            RequestPerFlowRateModel {
                rate_bps: sample_bps,
                delivery_samples: 1,
            }
        } else {
            RequestPerFlowRateModel {
                rate_bps: previous.map_or(sample_bps, |previous| {
                    previous.rate_bps.mul_add(0.75, sample_bps * 0.25)
                }),
                delivery_samples: previous
                    .map_or(1, |previous| previous.delivery_samples.saturating_add(1)),
            }
        };
        self.request_per_flow_rate_bps.insert(instance, model);
    }

    fn record_request_tcp_ack_turnover_sample(
        &mut self,
        context: &ClientPathContext,
        instance: RelayPathInstance,
        sample: PathRateSample,
        sampled_at: Instant,
        candidate_sample: bool,
    ) {
        if instance.key.underlay != UnderlayProtocol::Tcp
            || (self.ordered_data_owner_instance != Some(instance) && !candidate_sample)
        {
            return;
        }
        let Some(snapshot) = context.reliable_path_snapshot(instance.key) else {
            return;
        };
        let pto = transport_pto_from_snapshot(Some(snapshot));
        let previous = self.request_tcp_ack_turnover.get(&instance).copied();
        if let Some(model) = RequestTcpAckTurnoverModel::observe(previous, sample, pto, sampled_at)
        {
            self.request_tcp_ack_turnover.insert(instance, model);
        }
    }

    #[cfg(test)]
    pub(super) async fn send_stream_data(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        frame: Frame,
    ) -> Result<RelaySendOutcome, RuntimeError> {
        self.send_frame(context, remotes, frame, RelaySendCause::StreamData, None)
            .await
    }

    async fn send_stream_data_for_request_lane(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        frame: Frame,
        request_lane: FlowLane,
    ) -> Result<RelaySendOutcome, RuntimeError> {
        self.send_frame(
            context,
            remotes,
            frame,
            RelaySendCause::StreamData,
            Some(request_lane),
        )
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
        self.send_frame(context, remotes, frame, cause, None).await
    }

    pub(super) async fn send_repair_frame(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        frame: Frame,
        cause: RelaySendCause,
    ) -> Result<RelaySendOutcome, RuntimeError> {
        debug_assert!(cause.is_repair());
        self.send_frame(context, remotes, frame, cause, None).await
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
        request_lane: FlowLane,
        remotes: &mut ReliableRelayRemoteSet,
        send_stream: &mut ReliableSendStream,
        sender_queue: &mut ReliableRelaySenderQueue,
        local_open: bool,
        inflight_path_claims: &HashSet<RelayPathKey>,
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
                    request_lane,
                    remotes,
                    send_stream,
                    sender_queue,
                    local_open,
                    inflight_path_claims,
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
                    inflight_path_claims,
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
        request_lane: FlowLane,
        remotes: &mut ReliableRelayRemoteSet,
        send_stream: &mut ReliableSendStream,
        sender_queue: &mut ReliableRelaySenderQueue,
        local_open: bool,
        inflight_path_claims: &HashSet<RelayPathKey>,
        payload: Bytes,
        data_quantum_bytes: usize,
    ) -> Result<ClientQueuedDispatch, RuntimeError> {
        let dispatch_payload_bytes = data_quantum_bytes.min(payload.len()).max(1);
        let dispatch_payload = payload.slice(..dispatch_payload_bytes);
        let frame = send_stream
            .send_data(dispatch_payload, StreamFlags::NONE)
            .map_err(RuntimeError::Stream)?;
        let retry_frame = frame.clone();
        // Queue priority stays duplex-aware, but request exploration must not
        // borrow bulk classification from reverse-direction response bytes.
        match self
            .send_stream_data_for_request_lane(context, remotes, frame.clone(), request_lane)
            .await
        {
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
                    inflight_path_claims,
                )
                .await
                {
                    Ok(attached) if attached > 0 => {
                        if let Err(err) = send_stream.commit_prepared_data(&frame) {
                            return Err(RuntimeError::Stream(err));
                        }
                        match self
                            .send_stream_data_for_request_lane(
                                context,
                                remotes,
                                retry_frame,
                                request_lane,
                            )
                            .await
                        {
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
        inflight_path_claims: &HashSet<RelayPathKey>,
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
                    inflight_path_claims,
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
        request_lane: Option<FlowLane>,
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
            .emit_relay_frame(context, remotes, frame, cause, &avoid_keys, request_lane)
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
        request_lane: Option<FlowLane>,
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
            let selection_lane = request_lane.unwrap_or(stream_lane);
            let selection = match self.choose_relay_path_position(
                context,
                remotes,
                &frame,
                selection_lane,
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
                request_calibration_commit,
                request_load_expectation,
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
            let request_load_claim =
                if let Some((key, active, latency_sensitive)) = request_load_expectation {
                    let Some(claim) = context.try_reserve_relay_path_load_if_unchanged(
                        key,
                        selection_lane,
                        active,
                        latency_sensitive,
                    ) else {
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "request_startup_selection",
                            format_args!(
                                "phase=claim_stale stream_id={} path_index={} instance_id={}",
                                self.stream_id.0, instance.key.index, instance.id,
                            ),
                        );
                        if let Some(previous) = request_subflow_rollback {
                            self.request_subflow_set = previous;
                        }
                        if let Some(instance) = request_attempted_rollback {
                            // No product byte was emitted, so this was not an
                            // attempt. The occupied-path filter prevents an
                            // immediate unsafe retry while the winner owns it.
                            self.request_attempted_subflows.remove(&instance);
                        }
                        return Err(RuntimeError::SenderServiceBlocked);
                    };
                    Some(claim)
                } else {
                    None
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
                    self.commit_request_ack_clock_calibration(request_calibration_commit);
                    let claimed_load = request_load_claim.is_some();
                    if let Some(claim) = request_load_claim {
                        // The exact path owns the lease after carrier enqueue;
                        // path removal or relay cancellation releases it.
                        let _ = remotes.commit_path_instance_load_claim(instance, claim);
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "request_startup_selection",
                            format_args!(
                                "phase=claim_committed stream_id={} path_index={} instance_id={}",
                                self.stream_id.0, instance.key.index, instance.id,
                            ),
                        );
                    }
                    if matches!(frame, Frame::StreamData { .. }) {
                        let sent_bytes = reliable_stream_frame_payload_bytes(&frame);
                        if data_role.is_some() && !claimed_load {
                            // Validation attachment is not demand. Its first
                            // unique OwnerData commits this flow's carrier load
                            // so concurrent flows divide capacity and explore a
                            // different idle Subflow when one exists.
                            remotes.reserve_path_instance_load_if_needed(
                                context,
                                instance,
                                selection_lane,
                            );
                        }
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
                    return Err(RuntimeError::SenderServiceBlocked);
                }
                Err(err) => {
                    if let Some(previous) = request_subflow_rollback {
                        self.request_subflow_set = previous;
                    }
                    if let Some(instance) = request_attempted_rollback {
                        self.request_attempted_subflows.remove(&instance);
                    }
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
                    self.fail_client_path_instance(context, remotes, instance)
                        .await;
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
            #[cfg(target_os = "linux")]
            self.try_start_request_tcp_capacity_calibration(context, remotes, lane);
            self.try_start_request_quic_capacity_calibration(context, remotes, lane);
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
                    owner: self.request_ack_clock_calibration_owner,
                    pending: self.request_ack_clock_calibration_pending,
                    proven_subflows: &self.request_ack_clock_proven_subflows,
                    first_window_acked_subflows: &self.request_ack_clock_first_window_subflows,
                    spent_bytes: &self.request_ack_clock_calibration_bytes,
                    tcp_carrier_proven_candidates: Some(&self.request_tcp_capacity_proven_subflows),
                }),
                request_per_flow_rate_bps: Some(&self.request_per_flow_rate_bps),
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
                    load_expectation,
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
                        request_calibration_commit: None,
                        request_load_expectation: load_expectation
                            .map(|(active, latency)| (candidate.key, active, latency)),
                    });
                }
                BulkRelayPathChoice::SelectedAckClockCalibration {
                    position,
                    candidate,
                    target_bytes,
                } => {
                    let payload_bytes = reliable_stream_frame_payload_bytes(frame) as u64;
                    let entry_offset = reliable_stream_frame_extent(frame)
                        .map(|(offset, _, _)| offset)
                        .unwrap_or(0);
                    let service_key = self
                        .ordered_data_owner_instance
                        .map(|service| service.key)
                        .unwrap_or(candidate.key);
                    let (foreign_optional_ranges, foreign_optional_bytes) = if self
                        .request_ack_clock_calibration_owner
                        .is_none()
                    {
                        self.flights
                            .foreign_ordering_owner_debt_before_offset(entry_offset, &[service_key])
                    } else {
                        (0, 0)
                    };
                    return Ok(RelayPathSendSelection {
                        position,
                        data_role: Some(PathRuntimeRole::Subflow),
                        request_subflow_rollback: None,
                        request_attempted_rollback: None,
                        request_calibration_commit: Some(
                            RequestAckClockCalibrationCommit::OwnerData {
                                candidate,
                                target_bytes,
                                payload_bytes,
                                entry_offset,
                                foreign_optional_ranges,
                                foreign_optional_bytes,
                            },
                        ),
                        request_load_expectation: None,
                    });
                }
                BulkRelayPathChoice::SelectedAckClockCalibrationFence {
                    position,
                    candidate,
                } => {
                    let service = remotes.paths[position].instance();
                    debug_assert_eq!(Some(service), self.ordered_data_owner_instance);
                    let entry_offset = reliable_stream_frame_extent(frame)
                        .map(|(offset, _, _)| offset)
                        .unwrap_or(0);
                    let (foreign_optional_ranges, foreign_optional_bytes) = if self
                        .request_ack_clock_calibration_owner
                        .is_none()
                        && self.request_ack_clock_calibration_pending.is_none()
                    {
                        self.flights
                            .foreign_ordering_owner_debt_before_offset(entry_offset, &[service.key])
                    } else {
                        (0, 0)
                    };
                    return Ok(RelayPathSendSelection {
                        position,
                        data_role: Some(PathRuntimeRole::Service),
                        request_subflow_rollback: None,
                        request_attempted_rollback: None,
                        request_calibration_commit: Some(
                            RequestAckClockCalibrationCommit::ServiceFence {
                                service,
                                candidate,
                                entry_offset,
                                foreign_optional_ranges,
                                foreign_optional_bytes,
                            },
                        ),
                        request_load_expectation: None,
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
        // Pending entry owns no candidate bytes and is valid only for the exact
        // Service epoch that drained its lower optional frontier.
        self.request_ack_clock_calibration_pending = None;
    }

    fn commit_request_ack_clock_calibration(
        &mut self,
        commit: Option<RequestAckClockCalibrationCommit>,
    ) {
        let Some(commit) = commit else {
            return;
        };
        match commit {
            RequestAckClockCalibrationCommit::ServiceFence {
                service,
                candidate,
                entry_offset,
                foreign_optional_ranges,
                foreign_optional_bytes,
            } => {
                #[cfg(not(feature = "lab-diagnostics"))]
                let _ = (
                    entry_offset,
                    foreign_optional_ranges,
                    foreign_optional_bytes,
                );
                if let Some(owner) = self.request_ack_clock_calibration_owner {
                    debug_assert_eq!(owner.candidate, candidate);
                    return;
                }
                debug_assert_eq!(self.ordered_data_owner_instance, Some(service));
                let pending = RequestAckClockCalibrationPending { service, candidate };
                if self.request_ack_clock_calibration_pending == Some(pending) {
                    return;
                }
                self.request_ack_clock_calibration_pending = Some(pending);
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "ack_clock_calibration",
                    format_args!(
                        "phase=pending_started stream_id={} service_underlay={:?} service_index={} service_instance={} candidate_index={} candidate_instance={} entry_offset={} foreign_optional_ranges={} foreign_optional_bytes={}",
                        self.stream_id.0,
                        service.key.underlay,
                        service.key.index,
                        service.id,
                        candidate.key.index,
                        candidate.id,
                        entry_offset,
                        foreign_optional_ranges,
                        foreign_optional_bytes,
                    ),
                );
            }
            RequestAckClockCalibrationCommit::OwnerData {
                candidate,
                target_bytes,
                payload_bytes,
                entry_offset,
                foreign_optional_ranges,
                foreign_optional_bytes,
            } => {
                #[cfg(not(feature = "lab-diagnostics"))]
                let _ = (
                    entry_offset,
                    foreign_optional_ranges,
                    foreign_optional_bytes,
                );
                if self
                    .request_ack_clock_calibration_owner
                    .is_some_and(|owner| owner.candidate != candidate)
                {
                    debug_assert!(false, "calibration owner changed before enqueue commit");
                    return;
                }
                if let Some(pending) = self.request_ack_clock_calibration_pending {
                    debug_assert_eq!(pending.candidate, candidate);
                    debug_assert_eq!(Some(pending.service), self.ordered_data_owner_instance);
                }
                let beginning = self.request_ack_clock_calibration_owner.is_none();
                let target_bytes = self
                    .request_ack_clock_calibration_owner
                    .map_or(target_bytes, |owner| owner.target_bytes);
                let previous_bytes = if beginning {
                    0
                } else {
                    self.request_ack_clock_calibration_bytes
                        .get(&candidate)
                        .copied()
                        .unwrap_or(0)
                };
                let spent_bytes = previous_bytes.saturating_add(payload_bytes);
                self.request_ack_clock_calibration_pending = None;
                self.request_ack_clock_calibration_owner = Some(RequestAckClockCalibrationOwner {
                    candidate,
                    target_bytes,
                });
                self.request_ack_clock_calibration_targets
                    .insert(candidate, target_bytes);
                self.request_ack_clock_calibration_bytes
                    .insert(candidate, spent_bytes);
                #[cfg(feature = "lab-diagnostics")]
                {
                    if beginning {
                        lab_diagnostic(
                            "ack_clock_calibration",
                            format_args!(
                                "phase=owner_started stream_id={} underlay={:?} path_index={} instance_id={} payload_bytes={} target_bytes={} entry_offset={} foreign_optional_ranges={} foreign_optional_bytes={}",
                                self.stream_id.0,
                                candidate.key.underlay,
                                candidate.key.index,
                                candidate.id,
                                payload_bytes,
                                target_bytes,
                                entry_offset,
                                foreign_optional_ranges,
                                foreign_optional_bytes,
                            ),
                        );
                    }
                    if previous_bytes < target_bytes && spent_bytes >= target_bytes {
                        lab_diagnostic(
                            "ack_clock_calibration",
                            format_args!(
                                "phase=target_spent stream_id={} underlay={:?} path_index={} instance_id={} spent_bytes={} target_bytes={}",
                                self.stream_id.0,
                                candidate.key.underlay,
                                candidate.key.index,
                                candidate.id,
                                spent_bytes,
                                target_bytes,
                            ),
                        );
                    }
                    static TRACE_COUNT: std::sync::atomic::AtomicU64 =
                        std::sync::atomic::AtomicU64::new(0);
                    let count = TRACE_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if count < 16 || count % 256 == 0 {
                        lab_diagnostic(
                            "ack_clock_calibration",
                            format_args!(
                                "phase=selected stream_id={} underlay={:?} path_index={} instance_id={} payload_bytes={} spent_bytes={} target_bytes={}",
                                self.stream_id.0,
                                candidate.key.underlay,
                                candidate.key.index,
                                candidate.id,
                                payload_bytes,
                                spent_bytes,
                                target_bytes,
                            ),
                        );
                    }
                }
            }
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
        if owner.key.underlay != UnderlayProtocol::Tcp {
            return;
        }
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

    fn request_ack_clock_calibration_target_is_sealed(&self, target: RelayPathInstance) -> bool {
        self.request_ack_clock_calibration_owner
            .filter(|owner| owner.candidate == target)
            .is_some_and(|owner| {
                self.request_ack_clock_calibration_bytes
                    .get(&target)
                    .is_some_and(|spent| *spent >= owner.target_bytes)
            })
    }

    fn revoke_request_tcp_capacity_calibration(
        &mut self,
        target: RelayPathInstance,
        preserve_committed_product: bool,
    ) -> bool {
        self.request_tcp_capacity_proven_subflows.remove(&target);
        self.request_tcp_capacity_calibrations.remove(&target);
        let product_transaction_preserved = preserve_committed_product
            && self.request_ack_clock_calibration_target_is_sealed(target);
        if product_transaction_preserved {
            // Carrier freshness admits a bounded product transaction but does
            // not own it. Once the fixed target is sealed, keep its exact ACK
            // evidence until product proof or a real path lifecycle change.
            return true;
        }
        self.request_graduated_subflows.remove(&target);
        self.request_rate_proven_subflows.remove(&target);
        self.request_ack_clock_first_window_subflows.remove(&target);
        self.request_ack_clock_proven_subflows.remove(&target);
        self.request_window_turnover_proven_subflows.remove(&target);
        self.request_rate_evidence.remove(&target);
        self.request_tcp_ack_turnover.remove(&target);
        self.request_ack_clock_calibration_bytes.remove(&target);
        self.request_ack_clock_calibration_targets.remove(&target);
        if self
            .request_ack_clock_calibration_owner
            .is_some_and(|owner| owner.candidate == target)
        {
            self.request_ack_clock_calibration_owner = None;
        }
        if self
            .request_ack_clock_calibration_pending
            .is_some_and(|pending| pending.candidate == target)
        {
            self.request_ack_clock_calibration_pending = None;
        }
        false
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
            self.request_ack_clock_first_window_subflows
                .retain(|instance| live_instances.contains(instance));
            self.request_ack_clock_proven_subflows
                .retain(|instance| live_instances.contains(instance));
            self.request_window_turnover_proven_subflows
                .retain(|instance| live_instances.contains(instance));
            self.request_tcp_capacity_proven_subflows
                .retain(|instance| live_instances.contains(instance));
            self.request_ack_clock_calibration_bytes
                .retain(|instance, _| live_instances.contains(instance));
            self.request_ack_clock_calibration_targets
                .retain(|instance, _| live_instances.contains(instance));
            self.request_rate_evidence
                .retain(|instance, _| live_instances.contains(instance));
            self.request_per_flow_rate_bps
                .retain(|instance, _| live_instances.contains(instance));
            self.request_tcp_ack_turnover
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
        let now = Instant::now();
        let detached_tcp_capacity_targets = self
            .request_tcp_capacity_calibrations
            .keys()
            .copied()
            .filter(|target| !remotes.contains_path_instance(*target))
            .collect::<Vec<_>>();
        for target in detached_tcp_capacity_targets {
            self.revoke_request_tcp_capacity_calibration(target, false);
        }
        let tcp_capacities = self
            .request_tcp_capacity_calibrations
            .values()
            .map(|calibration| {
                let proof = context.request_tcp_capacity_probe_proof(
                    self.stream_id,
                    calibration.target.key.index,
                    calibration.target,
                    calibration.token,
                );
                (
                    calibration.target,
                    calibration.token,
                    calibration.graduated,
                    calibration.publication_expires_at,
                    calibration.proof_expires_at,
                    calibration.lease.is_current(),
                    calibration.lease.is_published(),
                    proof,
                )
            })
            .collect::<Vec<_>>();
        for (
            target,
            _token,
            graduated,
            publication_expires_at,
            proof_expires_at,
            current,
            published,
            proof,
        ) in tcp_capacities
        {
            if self.request_ack_clock_proven_subflows.contains(&target)
                && self.request_per_flow_rate_bps.contains_key(&target)
            {
                self.request_tcp_capacity_proven_subflows.remove(&target);
                self.request_tcp_capacity_calibrations.remove(&target);
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "request_tcp_capacity_calibration",
                    format_args!(
                        "phase=handoff_complete stream_id={} path_index={} instance_id={} calibration_id={}",
                        self.stream_id.0, target.key.index, target.id, _token,
                    ),
                );
                continue;
            }
            if !graduated {
                if let Some(proof) = proof {
                    if let Some(calibration) =
                        self.request_tcp_capacity_calibrations.get_mut(&target)
                    {
                        calibration.graduated = true;
                        calibration.proof_expires_at = Some(proof.expires_at);
                    }
                    self.request_tcp_capacity_proven_subflows.insert(target);
                    self.request_graduated_subflows.insert(target);
                    self.request_rate_evidence
                        .entry(target)
                        .or_insert_with(|| RequestPathRateEvidence::new(proof.accepted_at))
                        .seed_ack_boundary(proof.accepted_at);
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "request_tcp_capacity_calibration",
                        format_args!(
                            "phase=carrier_proven stream_id={} path_index={} instance_id={} calibration_id={} train_bytes={} rate_mbps={:.3} proof_ms={}",
                            self.stream_id.0,
                            target.key.index,
                            target.id,
                            _token,
                            proof.train_bytes,
                            proof.rate_bps as f64 / 1_000_000.0,
                            proof
                                .expires_at
                                .saturating_duration_since(proof.accepted_at)
                                .as_millis(),
                        ),
                    );
                } else if now >= publication_expires_at || !current {
                    self.revoke_request_tcp_capacity_calibration(target, false);
                }
            } else if request_tcp_carrier_authority_expired_naturally(
                published,
                proof_expires_at,
                now,
            ) {
                let _product_transaction_preserved =
                    self.revoke_request_tcp_capacity_calibration(target, true);
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "request_tcp_capacity_calibration",
                    format_args!(
                        "phase=carrier_authority_expired stream_id={} path_index={} instance_id={} calibration_id={} product_transaction_preserved={}",
                        self.stream_id.0,
                        target.key.index,
                        target.id,
                        _token,
                        _product_transaction_preserved,
                    ),
                );
            } else if proof.is_none() || !published {
                self.revoke_request_tcp_capacity_calibration(target, false);
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "request_tcp_capacity_calibration",
                    format_args!(
                        "phase=revoked stream_id={} path_index={} instance_id={} calibration_id={} reason=carrier_authority_lost",
                        self.stream_id.0, target.key.index, target.id, _token,
                    ),
                );
            }
        }
        if self
            .request_quic_capacity_calibration
            .as_ref()
            .is_some_and(|calibration| !remotes.contains_path_instance(calibration.target))
        {
            self.request_quic_capacity_calibration = None;
        }
        // A proof accepted at the train deadline remains authoritative for its
        // own proof lifetime, even if sender reconciliation runs just after it.
        let graduated = self
            .request_quic_capacity_calibration
            .as_ref()
            .filter(|calibration| !calibration.graduated)
            .filter(|calibration| {
                context.request_quic_capacity_probe_proven(
                    calibration.target.key.index,
                    calibration.token,
                )
            })
            .map(|calibration| (calibration.target, calibration.token));
        if let Some((_target, _token)) = graduated {
            if let Some(calibration) = self.request_quic_capacity_calibration.as_mut() {
                calibration.graduated = true;
            }
            self.request_graduated_subflows.insert(_target);
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "request_quic_capacity_calibration",
                format_args!(
                    "phase=graduated stream_id={} path_index={} instance_id={} calibration_id={}",
                    self.stream_id.0, _target.key.index, _target.id, _token,
                ),
            );
        }
        if self
            .request_quic_capacity_calibration
            .as_ref()
            .is_some_and(|calibration| {
                !calibration.graduated && now >= calibration.publication_expires_at
            })
        {
            self.request_quic_capacity_calibration = None;
        }
        let handoff = self
            .request_quic_capacity_calibration
            .as_ref()
            .filter(|calibration| calibration.graduated)
            .map(|calibration| {
                (
                    calibration.target,
                    calibration.token,
                    context.request_quic_capacity_product_handoff_state(
                        calibration.target.key.index,
                        calibration.token,
                    ),
                )
            });
        if let Some((_target, _token, state)) = handoff
            && state != RequestQuicCapacityProductHandoffState::Pending
        {
            if state == RequestQuicCapacityProductHandoffState::Absent {
                // Ephemeral proof credit is transactional: an incomplete or
                // failed handoff cannot leave the Validation path owner-capable.
                self.request_graduated_subflows.remove(&_target);
            }
            self.request_quic_capacity_calibration = None;
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "request_quic_capacity_calibration",
                format_args!(
                    "phase={} stream_id={} path_index={} instance_id={} calibration_id={}",
                    match state {
                        RequestQuicCapacityProductHandoffState::Complete => "handoff_complete",
                        RequestQuicCapacityProductHandoffState::Absent => "handoff_expired",
                        RequestQuicCapacityProductHandoffState::Pending => unreachable!(),
                    },
                    self.stream_id.0,
                    _target.key.index,
                    _target.id,
                    _token,
                ),
            );
        }
        if self
            .request_ack_clock_calibration_pending
            .is_some_and(|pending| {
                self.ordered_data_owner_instance != Some(pending.service)
                    || pending.service.key.underlay != UnderlayProtocol::Tcp
                    || self
                        .request_ack_clock_proven_subflows
                        .contains(&pending.candidate)
                    || !self.request_graduated_subflows.contains(&pending.candidate)
                    || !(self
                        .request_ack_clock_first_window_subflows
                        .contains(&pending.candidate)
                        || self
                            .request_tcp_capacity_proven_subflows
                            .contains(&pending.candidate))
                    || !remotes.paths.iter().any(|path| {
                        path.instance() == pending.service
                            && path.placement == RelayPathPlacement::Active
                    })
                    || !remotes.paths.iter().any(|path| {
                        path.instance() == pending.candidate
                            && path.placement == RelayPathPlacement::Validation
                            && path.key().underlay == UnderlayProtocol::Tcp
                            && path.key().underlay == pending.service.key.underlay
                    })
            })
        {
            self.request_ack_clock_calibration_pending = None;
        }
        if let Some(owner) = self.request_ack_clock_calibration_owner {
            if self
                .request_ack_clock_proven_subflows
                .contains(&owner.candidate)
            {
                self.request_ack_clock_calibration_owner = None;
            } else {
                let placement_valid = self.request_graduated_subflows.contains(&owner.candidate)
                    && remotes.paths.iter().any(|path| {
                        path.instance() == owner.candidate
                            && path.placement == RelayPathPlacement::Validation
                    });
                let transaction_authorized = self
                    .request_ack_clock_calibration_target_is_sealed(owner.candidate)
                    || self
                        .request_ack_clock_first_window_subflows
                        .contains(&owner.candidate)
                    || self
                        .request_tcp_capacity_proven_subflows
                        .contains(&owner.candidate);
                if !placement_valid || !transaction_authorized {
                    // A sealed AwaitingAck target remains exact-instance state.
                    // Real placement loss or a partial transaction without its
                    // entry proof performs the full abort cleanup.
                    self.revoke_request_tcp_capacity_calibration(owner.candidate, false);
                }
            }
        }
        if self.ordered_data_owner_instance.is_some_and(|service| {
            service.key.underlay == UnderlayProtocol::Udp && remotes.contains_path_instance(service)
        }) {
            for path in &remotes.paths {
                let instance = path.instance();
                let native_evidence = path.placement == RelayPathPlacement::Validation
                    && instance.key.underlay == UnderlayProtocol::Udp
                    && path.path_proof_id.is_some_and(|proof_id| {
                        context.relay_path_has_fresh_proof(
                            instance.key.underlay,
                            instance.key.index,
                            proof_id,
                            path.attached_at,
                        )
                    })
                    && context.relay_path_has_native_bulk_model_evidence_since(
                        instance.key.underlay,
                        instance.key.index,
                        path.attached_at,
                    );
                if native_evidence {
                    // QUIC graduates directly from its fresh, non-app-limited
                    // packet-ACK model. No ordered product sample or receipt
                    // marker participates in this carrier decision.
                    self.request_graduated_subflows.insert(instance);
                }
            }
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
        if owner.key.underlay != UnderlayProtocol::Tcp {
            self.reset_request_subflow_epoch();
            return;
        }
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
            self.record_request_per_flow_rate_sample(owner, sample, false);
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
        if let Some(receipt_acked_at) = receipt_acked_at
            && self.request_startup_rate_evidence.contains(&owner)
            && self.request_ack_clock_first_window_subflows.insert(owner)
        {
            // The ordered receipt follows the sealed startup sample on this
            // exact TCP attachment. Once product flight also drains below, it
            // is the causal boundary for the first calibration window.
            self.request_rate_evidence
                .entry(owner)
                .or_insert_with(|| RequestPathRateEvidence::new(receipt_acked_at))
                .seed_ack_boundary(receipt_acked_at);
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

    #[cfg(all(target_os = "linux", feature = "lab-diagnostics"))]
    fn diagnose_request_tcp_capacity_gate(
        &mut self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        lane: FlowLane,
    ) {
        let active_tcp_service_flows = context.active_tcp_service_request_bulk_flows();
        let latency_pressure = context.reliable_relay_has_latency_pressure();
        let service_path = self.ordered_data_owner_instance.and_then(|service| {
            (service.key.underlay == UnderlayProtocol::Tcp)
                .then(|| {
                    remotes.paths.iter().find(|path| {
                        path.instance() == service && path.placement == RelayPathPlacement::Active
                    })
                })
                .flatten()
        });
        let service_instance = service_path.map(ReliableRelayRemotePath::instance);
        let service_snapshot =
            service_instance.and_then(|instance| context.reliable_path_snapshot(instance.key));
        let service_bulk_evidence = service_instance.is_some_and(|instance| {
            context.relay_path_has_bulk_model_evidence(instance.key.underlay, instance.key.index)
        });
        let service_model = service_instance
            .and_then(|instance| self.request_per_flow_rate_bps.get(&instance).copied());
        let candidate_path = remotes
            .paths
            .iter()
            .filter(|path| {
                path.placement == RelayPathPlacement::Validation
                    && path.key().underlay == UnderlayProtocol::Tcp
                    && context.relay_path_allows_automatic_bulk_use(path.key())
                    && !self
                        .request_tcp_capacity_attempted_paths
                        .contains(&path.key().index)
            })
            .min_by_key(|path| context.relay_path_config_ordinal(path.key()));
        let candidate_instance = candidate_path.map(ReliableRelayRemotePath::instance);
        let candidate_snapshot =
            candidate_instance.and_then(|instance| context.reliable_path_snapshot(instance.key));
        let eligible_candidates = context.automatic_bulk_path_count(
            UnderlayProtocol::Tcp,
            service_instance.map(|instance| instance.key.index),
        );
        let proposed_candidate_share =
            request_capacity_stable_candidate_share_bytes(context.mux_limits, eligible_candidates);
        let stable_candidate_share =
            context.request_tcp_capacity_probe_candidate_share_bytes(proposed_candidate_share);
        let campaign_remaining_bytes = self
            .request_tcp_capacity_campaign
            .remaining_bytes(stable_candidate_share);
        let session_remaining_bytes = context.request_tcp_capacity_probe_remaining_bytes();
        let train_envelope_bytes = candidate_instance.map_or(0, |instance| {
            session_remaining_bytes.min(campaign_remaining_bytes).min(
                context.request_tcp_capacity_probe_path_remaining_bytes(
                    instance.key.index,
                    stable_candidate_share,
                ),
            )
        });
        let geometry = candidate_snapshot
            .zip(service_model)
            .and_then(|(candidate, service)| {
                request_tcp_capacity_calibration_geometry(
                    candidate,
                    service,
                    context.mux_limits,
                    train_envelope_bytes,
                )
            });
        let candidate_bulk_evidence = candidate_instance.is_some_and(|instance| {
            context.relay_path_has_bulk_model_evidence(instance.key.underlay, instance.key.index)
        });
        let path_proof_fresh = candidate_path.is_some_and(|path| {
            let instance = path.instance();
            path.path_proof_id.is_some_and(|proof_id| {
                context.relay_path_has_fresh_proof(
                    instance.key.underlay,
                    instance.key.index,
                    proof_id,
                    path.attached_at,
                )
            })
        });
        let can_enqueue = candidate_path.is_some_and(|path| {
            path.stream
                .can_enqueue_work_lane_now(ReliableWorkClass::Data, lane)
        });
        let gate = if !lane.is_bulk() {
            "non_bulk_lane"
        } else if active_tcp_service_flows != 1 {
            "tcp_service_flow_count"
        } else if latency_pressure {
            "session_latency_pressure"
        } else if service_path.is_none() {
            "active_tcp_service_missing"
        } else if service_snapshot.is_none() {
            "service_snapshot_missing"
        } else if !service_bulk_evidence {
            "service_bulk_evidence_missing"
        } else if service_snapshot.is_some_and(|snapshot| {
            snapshot.active_latency_sensitive_flows > 0
                || snapshot.session_active_latency_sensitive_flows > 0
        }) {
            "service_latency_pressure"
        } else if service_model.is_none() {
            "service_flow_model_missing"
        } else if service_model.is_some_and(|model| {
            !product_delivery_samples_override_startup_prior(model.delivery_samples)
        }) {
            "service_flow_model_immature"
        } else if candidate_path.is_none() {
            "validation_tcp_missing"
        } else if candidate_instance
            .is_some_and(|instance| self.request_graduated_subflows.contains(&instance))
        {
            "candidate_already_graduated"
        } else if candidate_instance.is_some_and(|instance| {
            self.request_rate_evidence.contains_key(&instance)
                || self.request_per_flow_rate_bps.contains_key(&instance)
                || self.request_rate_proven_subflows.contains(&instance)
                || self.request_ack_clock_proven_subflows.contains(&instance)
        }) {
            "candidate_product_evidence_present"
        } else if candidate_path.is_some_and(|path| path.path_proof_id.is_none()) {
            "candidate_path_proof_missing"
        } else if !path_proof_fresh {
            "candidate_path_proof_stale"
        } else if candidate_bulk_evidence {
            "candidate_bulk_evidence_present"
        } else if candidate_snapshot.is_none() {
            "candidate_snapshot_missing"
        } else if geometry.is_none() {
            "train_geometry_unavailable"
        } else if candidate_snapshot.is_some_and(|snapshot| snapshot.queue_bytes > 0) {
            "candidate_carrier_queue"
        } else if candidate_snapshot.is_some_and(|snapshot| snapshot.product_bytes_in_flight > 0) {
            "candidate_product_inflight"
        } else if candidate_snapshot.is_some_and(|snapshot| snapshot.product_queue_bytes > 0) {
            "candidate_product_queue"
        } else if candidate_snapshot.is_some_and(|snapshot| {
            snapshot.active_latency_sensitive_flows > 0
                || snapshot.session_active_latency_sensitive_flows > 0
        }) {
            "candidate_latency_pressure"
        } else if !can_enqueue {
            "candidate_queue_credit_missing"
        } else {
            "eligible"
        };
        let signature = (candidate_instance, gate);
        if self.request_tcp_capacity_last_gate == Some(signature) {
            return;
        }
        self.request_tcp_capacity_last_gate = Some(signature);
        lab_diagnostic(
            "request_tcp_capacity_gate",
            format_args!(
                "stream_id={} first_failed_gate={} lane={:?} active_tcp_service_flows={} latency_pressure={} service_path_index={} service_bulk_evidence={} service_carrier_bif={} service_product_bif={} service_rate_mbps={:.3} service_delivery_samples={} candidate_path_index={} candidate_proof_id={} candidate_proof_fresh={} candidate_bulk_evidence={} candidate_carrier_bif={} candidate_queue_bytes={} candidate_product_bif={} candidate_product_queue_bytes={} can_enqueue={} train_bytes={} stable_candidate_share_bytes={} campaign_remaining_bytes={} train_envelope_bytes={} session_remaining_bytes={}",
                self.stream_id.0,
                gate,
                lane,
                active_tcp_service_flows,
                latency_pressure,
                service_instance.map_or(-1, |instance| instance.key.index as i64),
                service_bulk_evidence,
                service_snapshot.map_or(0, |snapshot| snapshot.bytes_in_flight),
                service_snapshot.map_or(0, |snapshot| snapshot.product_bytes_in_flight),
                service_model.map_or(0.0, |model| model.rate_bps / 1_000_000.0),
                service_model.map_or(0, |model| model.delivery_samples),
                candidate_instance.map_or(-1, |instance| instance.key.index as i64),
                candidate_path
                    .and_then(|path| path.path_proof_id)
                    .unwrap_or(0),
                path_proof_fresh,
                candidate_bulk_evidence,
                candidate_snapshot.map_or(0, |snapshot| snapshot.bytes_in_flight),
                candidate_snapshot.map_or(0, |snapshot| snapshot.queue_bytes),
                candidate_snapshot.map_or(0, |snapshot| snapshot.product_bytes_in_flight),
                candidate_snapshot.map_or(0, |snapshot| snapshot.product_queue_bytes),
                can_enqueue,
                geometry.map_or(0, |geometry| geometry.train_bytes),
                stable_candidate_share,
                campaign_remaining_bytes,
                train_envelope_bytes,
                context.request_tcp_capacity_probe_remaining_bytes(),
            ),
        );
    }

    #[cfg(target_os = "linux")]
    fn try_start_request_tcp_capacity_calibration(
        &mut self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        lane: FlowLane,
    ) {
        #[cfg(feature = "lab-diagnostics")]
        self.diagnose_request_tcp_capacity_gate(context, remotes, lane);
        if !lane.is_bulk()
            || context.active_tcp_service_request_bulk_flows() != 1
            || context.reliable_relay_has_latency_pressure()
        {
            return;
        }
        let Some(service_path) = self.ordered_data_owner_instance.and_then(|service| {
            (service.key.underlay == UnderlayProtocol::Tcp).then(|| {
                remotes.paths.iter().find(|path| {
                    path.instance() == service && path.placement == RelayPathPlacement::Active
                })
            })?
        }) else {
            return;
        };
        let service = service_path.instance();
        let Some(service_snapshot) = context.reliable_path_snapshot(service.key) else {
            return;
        };
        if !context.relay_path_has_bulk_model_evidence(service.key.underlay, service.key.index)
            || service_snapshot.active_latency_sensitive_flows > 0
            || service_snapshot.session_active_latency_sensitive_flows > 0
        {
            return;
        }
        let Some(service_model) = self
            .request_per_flow_rate_bps
            .get(&service)
            .copied()
            .filter(|model| {
                product_delivery_samples_override_startup_prior(model.delivery_samples)
            })
        else {
            return;
        };
        // The Service model prices only train geometry. The candidate's full
        // receiver-confirmed train remains the sole cold startup-rate authority.
        // Freeze one fair share per configured eligible candidate. A late
        // path must not inherit the unused campaign budget of earlier paths.
        let eligible_candidates =
            context.automatic_bulk_path_count(UnderlayProtocol::Tcp, Some(service.key.index));
        let proposed_candidate_share =
            request_capacity_stable_candidate_share_bytes(context.mux_limits, eligible_candidates);
        let stable_candidate_share =
            context.request_tcp_capacity_probe_candidate_share_bytes(proposed_candidate_share);
        let session_remaining_bytes = context.request_tcp_capacity_probe_remaining_bytes();
        let mut candidates = remotes
            .paths
            .iter()
            .filter(|path| {
                let instance = path.instance();
                let snapshot = context.reliable_path_snapshot(instance.key);
                path.placement == RelayPathPlacement::Validation
                    && instance.key.underlay == UnderlayProtocol::Tcp
                    && context.relay_path_allows_automatic_bulk_use(instance.key)
                    && !self
                        .request_tcp_capacity_attempted_paths
                        .contains(&instance.key.index)
                    && !self.request_graduated_subflows.contains(&instance)
                    && !self.request_rate_evidence.contains_key(&instance)
                    && !self.request_per_flow_rate_bps.contains_key(&instance)
                    && !self.request_rate_proven_subflows.contains(&instance)
                    && !self.request_ack_clock_proven_subflows.contains(&instance)
                    && path.path_proof_id.is_some_and(|proof_id| {
                        context.relay_path_has_fresh_proof(
                            instance.key.underlay,
                            instance.key.index,
                            proof_id,
                            path.attached_at,
                        )
                    })
                    && !context.relay_path_has_bulk_model_evidence(
                        instance.key.underlay,
                        instance.key.index,
                    )
                    && snapshot.is_some_and(request_tcp_capacity_candidate_can_start_receipt)
                    && path
                        .stream
                        .can_enqueue_work_lane_now(ReliableWorkClass::Data, lane)
            })
            .filter_map(|path| {
                let candidate_snapshot = context.reliable_path_snapshot(path.key())?;
                let campaign_remaining_bytes = self
                    .request_tcp_capacity_campaign
                    .remaining_bytes(stable_candidate_share);
                let train_envelope_bytes = session_remaining_bytes
                    .min(campaign_remaining_bytes)
                    .min(context.request_tcp_capacity_probe_path_remaining_bytes(
                        path.key().index,
                        stable_candidate_share,
                    ));
                let geometry = request_tcp_capacity_calibration_geometry(
                    candidate_snapshot,
                    service_model,
                    context.mux_limits,
                    train_envelope_bytes,
                )?;
                Some((path, candidate_snapshot, geometry, train_envelope_bytes))
            })
            .collect::<Vec<_>>();
        candidates.sort_by_key(|(path, _, _, _)| context.relay_path_config_ordinal(path.key()));
        if candidates.is_empty() {
            return;
        }
        static NEXT_REQUEST_TCP_CAPACITY_ID: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(1);
        for (path, candidate_snapshot, geometry, _train_envelope_bytes) in candidates {
            let instance = path.instance();
            let train_payload_bytes = geometry.train_bytes;
            let token =
                NEXT_REQUEST_TCP_CAPACITY_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let ticket = QuicCapacityProbeCommandTicket::new();
            let now = Instant::now();
            let baseline_budget = transport_pto_from_snapshot(Some(candidate_snapshot));
            let lease_duration = request_tcp_capacity_calibration_lease(
                candidate_snapshot,
                train_payload_bytes,
                geometry.service_rate_bps,
            );
            let Some(baseline_expires_at) = now.checked_add(baseline_budget) else {
                continue;
            };
            let Some(expires_at) = now.checked_add(lease_duration) else {
                continue;
            };
            let Some(write_expires_at) = expires_at.checked_sub(baseline_budget) else {
                continue;
            };
            let Some(lease) = context.try_reserve_request_tcp_capacity_probe(
                self.stream_id,
                instance.key.index,
                instance,
                token,
                train_payload_bytes,
                stable_candidate_share,
                self.request_tcp_capacity_campaign.clone(),
                geometry.required_timed_carrier_bytes,
                path.attached_at,
                expires_at,
                ticket,
            ) else {
                continue;
            };
            let request = RequestTcpCapacityProbeRequest {
                stream_id: self.stream_id,
                path_instance: instance,
                path_id: PathId(instance.key.index as u16),
                calibration_id: token,
                train_payload_bytes,
                sample_floor_bytes: geometry.sample_floor_bytes,
                warmup_carrier_bytes: geometry.warmup_carrier_bytes,
                timing_slack_bytes: geometry.timing_slack_bytes,
                required_timed_carrier_bytes: geometry.required_timed_carrier_bytes,
                baseline_expires_at,
                write_expires_at,
                expires_at,
            };
            if path
                .stream
                .try_enqueue_request_tcp_capacity_probe(request, lease.clone())
                .is_err()
            {
                continue;
            }
            self.request_tcp_capacity_attempted_paths
                .insert(instance.key.index);
            if !lease.commit() {
                // The exact carrier dequeued and rejected this one-shot attempt
                // before planner commit without putting any train byte on wire.
                continue;
            }
            let previous = self.request_tcp_capacity_calibrations.insert(
                instance,
                RequestTcpCapacityCalibration {
                    target: instance,
                    token,
                    publication_expires_at: expires_at,
                    proof_expires_at: None,
                    graduated: false,
                    lease,
                },
            );
            debug_assert!(previous.is_none());
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "request_tcp_capacity_calibration",
                format_args!(
                    "phase=started stream_id={} path_index={} instance_id={} calibration_id={} train_bytes={} train_envelope_bytes={} sample_floor_bytes={} accounting_slack_bytes={} timing_slack_bytes={} warmup_bytes={} required_timed_bytes={} candidate_carrier_flight_bytes={} candidate_srtt_ms={:.3} candidate_jitter_ms={:.3} service_rate_mbps={:.3} service_delivery_samples={} baseline_budget_ms={} write_deadline_after_ms={} final_budget_ms={} lease_ms={}",
                    self.stream_id.0,
                    instance.key.index,
                    instance.id,
                    token,
                    train_payload_bytes,
                    _train_envelope_bytes,
                    geometry.sample_floor_bytes,
                    geometry.accounting_slack_bytes,
                    geometry.timing_slack_bytes,
                    geometry.warmup_carrier_bytes,
                    geometry.required_timed_carrier_bytes,
                    geometry.candidate_carrier_flight_bytes,
                    candidate_snapshot.srtt_ms,
                    candidate_snapshot.jitter_ms,
                    geometry.service_rate_bps as f64 / 1_000_000.0,
                    service_model.delivery_samples,
                    baseline_budget.as_millis(),
                    lease_duration.saturating_sub(baseline_budget).as_millis(),
                    baseline_budget.as_millis(),
                    lease_duration.as_millis(),
                ),
            );
        }
    }

    fn try_start_request_quic_capacity_calibration(
        &mut self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        lane: FlowLane,
    ) {
        if !lane.is_bulk() || self.request_quic_capacity_calibration.is_some() {
            return;
        }
        let has_unattempted_udp_candidate = remotes.paths.iter().any(|path| {
            let instance = path.instance();
            path.placement == RelayPathPlacement::Validation
                && instance.key.underlay == UnderlayProtocol::Udp
                && context.relay_path_allows_automatic_bulk_use(instance.key)
                && !self
                    .request_quic_capacity_attempted_paths
                    .contains(&instance.key.index)
                && !self.request_graduated_subflows.contains(&instance)
        });
        if !has_unattempted_udp_candidate || context.reliable_relay_has_latency_pressure() {
            // Bulk sends call this repeatedly. Topology can reject the common
            // single-path/completed case without touching session health.
            return;
        }
        let Some(service_path) = self.ordered_data_owner_instance.and_then(|service| {
            (service.key.underlay == UnderlayProtocol::Udp).then(|| {
                remotes.paths.iter().find(|path| {
                    path.instance() == service && path.placement == RelayPathPlacement::Active
                })
            })?
        }) else {
            return;
        };
        let service = service_path.instance();
        let Some(service_snapshot) = context.reliable_path_snapshot(service.key) else {
            return;
        };
        if service_snapshot.active_latency_sensitive_flows > 0
            || service_snapshot.session_active_latency_sensitive_flows > 0
        {
            return;
        }
        if service_snapshot.product_bytes_in_flight
            < reliable_subflow_startup_sample_limit_bytes(context.mux_limits)
        {
            return;
        }
        // QUIC keeps its native packet-ACK proof transaction, but shares TCP's
        // topology-stable budget policy. Attempt order cannot enlarge a train.
        let eligible_candidates =
            context.automatic_bulk_path_count(UnderlayProtocol::Udp, Some(service.key.index));
        let proposed_candidate_share =
            request_capacity_stable_candidate_share_bytes(context.mux_limits, eligible_candidates);
        let stable_candidate_share =
            context.request_quic_capacity_probe_candidate_share_bytes(proposed_candidate_share);
        let session_remaining_bytes = context.request_quic_capacity_probe_remaining_bytes();
        let Some((path, snapshot, geometry, _train_envelope_bytes)) = remotes
            .paths
            .iter()
            .filter(|path| {
                let instance = path.instance();
                let snapshot = context.reliable_path_snapshot(instance.key);
                path.placement == RelayPathPlacement::Validation
                    && instance.key.underlay == UnderlayProtocol::Udp
                    && context.relay_path_allows_automatic_bulk_use(instance.key)
                    && !self
                        .request_quic_capacity_attempted_paths
                        .contains(&instance.key.index)
                    && !self.request_graduated_subflows.contains(&instance)
                    && path.path_proof_id.is_some_and(|proof_id| {
                        context.relay_path_has_fresh_proof(
                            instance.key.underlay,
                            instance.key.index,
                            proof_id,
                            path.attached_at,
                        )
                    })
                    && !context.relay_path_has_native_bulk_model_evidence_since(
                        instance.key.underlay,
                        instance.key.index,
                        path.attached_at,
                    )
                    && snapshot.is_some_and(|snapshot| {
                        snapshot.bytes_in_flight == 0
                            && snapshot.queue_bytes == 0
                            && snapshot.product_bytes_in_flight == 0
                            && snapshot.product_queue_bytes == 0
                    })
                    && path
                        .stream
                        .can_enqueue_work_lane_now(ReliableWorkClass::Data, lane)
            })
            .filter_map(|path| {
                let snapshot = context.reliable_path_snapshot(path.key())?;
                let campaign_remaining_bytes = self
                    .request_quic_capacity_campaign
                    .remaining_bytes(stable_candidate_share);
                let train_envelope_bytes = session_remaining_bytes
                    .min(campaign_remaining_bytes)
                    .min(context.request_quic_capacity_probe_path_remaining_bytes(
                        path.key().index,
                        stable_candidate_share,
                    ));
                let geometry = request_quic_capacity_calibration_geometry(
                    snapshot,
                    service_snapshot.delivery_rate_bps,
                    context.mux_limits,
                    train_envelope_bytes,
                )?;
                Some((path, snapshot, geometry, train_envelope_bytes))
            })
            .min_by_key(|(path, _, _, _)| context.relay_path_config_ordinal(path.key()))
        else {
            return;
        };
        let train_payload_bytes = geometry.train_bytes;
        static NEXT_REQUEST_QUIC_CAPACITY_ID: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(1);
        let token =
            NEXT_REQUEST_QUIC_CAPACITY_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let ticket = QuicCapacityProbeCommandTicket::new();
        let lease_duration = request_quic_capacity_calibration_lease(snapshot, train_payload_bytes);
        let Some(expires_at) = Instant::now().checked_add(lease_duration) else {
            return;
        };
        let proof_validity = quic_bulk_proof_freshness_horizon(
            Duration::from_secs_f64(snapshot.srtt_ms.max(1.0) / 1_000.0),
            Duration::from_secs_f64(snapshot.jitter_ms.max(1.0) / 1_000.0),
        );
        let Some(publication_expires_at) = expires_at.checked_add(proof_validity) else {
            return;
        };
        let instance = path.instance();
        let Some(mut lease) = context.try_reserve_request_quic_capacity_probe(
            self.stream_id,
            instance.key.index,
            instance,
            token,
            train_payload_bytes,
            stable_candidate_share,
            self.request_quic_capacity_campaign.clone(),
            path.attached_at,
            expires_at,
            proof_validity,
            ticket.clone(),
        ) else {
            return;
        };
        let probe = QuicCapacityProbeCommand {
            owner: QuicCapacityProbeOwner::Request {
                stream_id: self.stream_id,
                path_instance: instance,
            },
            path_id: PathId(instance.key.index as u16),
            calibration_id: token,
            train_payload_bytes,
            sample_floor_bytes: geometry.sample_floor_bytes,
            warmup_carrier_bytes: geometry.warmup_carrier_bytes,
            required_timed_carrier_bytes: geometry.required_timed_carrier_bytes,
            proof_validity,
            expires_at,
            ticket: ticket.clone(),
            cancel_on_drop: true,
        };
        if path
            .stream
            .try_enqueue_request_quic_capacity_probe(probe)
            .is_err()
        {
            return;
        }
        lease.commit();
        self.request_quic_capacity_attempted_paths
            .insert(instance.key.index);
        self.request_quic_capacity_calibration = Some(RequestQuicCapacityCalibration {
            target: instance,
            token,
            publication_expires_at,
            graduated: false,
            ticket,
            _lease: lease,
        });
        #[cfg(feature = "lab-diagnostics")]
        lab_diagnostic(
            "request_quic_capacity_calibration",
            format_args!(
                "phase=started stream_id={} path_index={} instance_id={} calibration_id={} train_bytes={} stable_candidate_share_bytes={} train_envelope_bytes={} sample_floor_bytes={} accounting_slack_bytes={} timing_slack_bytes={} desired_warmup_bytes={} warmup_bytes={} required_proof_bytes={} candidate_carrier_flight_bytes={} service_rate_bps={} service_rate_scope={:?} slow_start_rounds={} lease_ms={}",
                self.stream_id.0,
                instance.key.index,
                instance.id,
                token,
                train_payload_bytes,
                stable_candidate_share,
                _train_envelope_bytes,
                geometry.sample_floor_bytes,
                geometry.accounting_slack_bytes,
                geometry.timing_slack_bytes,
                geometry.desired_warmup_carrier_bytes,
                geometry.warmup_carrier_bytes,
                geometry.required_timed_carrier_bytes,
                geometry.candidate_carrier_flight_bytes,
                geometry.service_rate_bps,
                service_snapshot.rate_scope,
                request_quic_capacity_slow_start_rounds(train_payload_bytes),
                lease_duration.as_millis(),
            ),
        );
    }

    fn reserve_request_startup_subflow(
        &mut self,
        context: &ClientPathContext,
        service: RelayPathInstance,
        candidate: RelayPathInstance,
        payload_bytes: usize,
    ) -> Result<(Option<FlowSubflowSet<RelayPathInstance>>, bool), RuntimeError> {
        if service.key.underlay != UnderlayProtocol::Tcp
            || candidate.key.underlay != UnderlayProtocol::Tcp
        {
            return Err(RuntimeError::SenderServiceBlocked);
        }
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
            if release.path_proving {
                context.record_relay_path_product_ack(
                    self.stream_id,
                    release.instance,
                    release.bytes,
                    release.sent_at,
                    acked_at,
                );
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
            self.record_request_per_flow_rate_sample(owner, sample, false);
            context.mark_relay_path_rate_sample(owner.key.underlay, owner.key.index, sample);
            if self.request_ack_clock_first_window_subflows.insert(owner) {
                // The exact product ACK that completes the sealed TCP startup
                // owner window is also a causal boundary: every calibration
                // byte selected after this point is post-boundary by
                // construction. The explicit path receipt remains an
                // equivalent boundary when it arrives first.
                self.request_rate_evidence
                    .entry(owner)
                    .or_insert_with(|| RequestPathRateEvidence::new(acked_at))
                    .seed_ack_boundary(acked_at);
            }
        }
        for (instance, (bytes, first_sent_at, latest_sent_at)) in ordinary_owner_samples {
            // TCP lacks carrier-native delivery telemetry, so its product ACK
            // fallback needs a representative window. QUIC keeps its existing
            // small product-provenance threshold; carrier ACKs own its rate.
            let coverage_floor_bytes = request_path_rate_coverage_floor_bytes(
                instance,
                self.ordered_data_owner_instance,
                self.request_ack_clock_calibration_targets
                    .get(&instance)
                    .copied(),
                context.mux_limits,
            );
            let is_ordered_service = self.ordered_data_owner_instance == Some(instance);
            let (update, has_exact_path_provenance, exact_attributed_bytes) = {
                let evidence = self
                    .request_rate_evidence
                    .entry(instance)
                    .or_insert_with(|| RequestPathRateEvidence::new(first_sent_at));
                let update = evidence.observe(
                    bytes,
                    first_sent_at,
                    latest_sent_at,
                    acked_at,
                    coverage_floor_bytes,
                    !is_ordered_service,
                );
                (
                    update,
                    evidence.has_exact_path_provenance(),
                    evidence.exact_attributed_bytes(),
                )
            };
            if has_exact_path_provenance {
                // Exact ownership is enough to establish that this flow used
                // the path. It is not enough to publish a rate sample.
                self.request_rate_proven_subflows.insert(instance);
            }
            if let RequestPathRateEvidenceUpdate::Proven {
                sample,
                first_window,
            } = update
            {
                if instance.key.underlay == UnderlayProtocol::Tcp
                    && is_ordered_service
                    && let Some(sample) = sample
                {
                    if !first_window {
                        self.record_request_tcp_ack_turnover_sample(
                            context, instance, sample, acked_at, false,
                        );
                    }
                    context.mark_relay_path_rate_sample(
                        instance.key.underlay,
                        instance.key.index,
                        sample,
                    );
                    if !first_window {
                        self.record_request_per_flow_rate_sample(instance, sample, false);
                    }
                } else if instance.key.underlay == UnderlayProtocol::Tcp && first_window {
                    self.request_ack_clock_first_window_subflows
                        .insert(instance);
                } else if instance.key.underlay == UnderlayProtocol::Tcp
                    && let Some(sample) = sample
                {
                    let replace_startup_rate =
                        self.request_ack_clock_proven_subflows.insert(instance);
                    let turnover_authorized = !replace_startup_rate
                        && self
                            .request_ack_clock_calibration_targets
                            .get(&instance)
                            .copied()
                            .is_some_and(|target_bytes| {
                                request_tcp_candidate_turnover_authorized(
                                    exact_attributed_bytes,
                                    target_bytes,
                                    coverage_floor_bytes,
                                )
                            });
                    if turnover_authorized {
                        self.request_window_turnover_proven_subflows
                            .insert(instance);
                    }
                    if self
                        .request_ack_clock_calibration_owner
                        .is_some_and(|owner| owner.candidate == instance)
                    {
                        self.request_ack_clock_calibration_owner = None;
                    }
                    if self
                        .request_ack_clock_calibration_pending
                        .is_some_and(|pending| pending.candidate == instance)
                    {
                        self.request_ack_clock_calibration_pending = None;
                    }
                    self.record_request_per_flow_rate_sample(
                        instance,
                        sample,
                        replace_startup_rate,
                    );
                    self.record_request_tcp_ack_turnover_sample(
                        context, instance, sample, acked_at, true,
                    );
                    context.mark_relay_path_ack_clock_rate_sample(
                        instance.key.underlay,
                        instance.key.index,
                        sample,
                        replace_startup_rate,
                    );
                    #[cfg(feature = "lab-diagnostics")]
                    {
                        lab_diagnostic(
                            "ack_clock_calibration",
                            format_args!(
                                "phase=ack_clock_sample stream_id={} underlay={:?} path_index={} instance_id={} evidence_bytes={} sample_elapsed_us={} replace_startup_rate={} rate_bps={}",
                                self.stream_id.0,
                                instance.key.underlay,
                                instance.key.index,
                                instance.id,
                                sample.bytes(),
                                sample.elapsed().as_micros(),
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

    pub(super) fn request_ordered_service_instance(&self) -> Option<RelayPathInstance> {
        self.ordered_data_owner_instance
    }

    pub(super) fn request_owner_ack_can_grow_window(
        &self,
        remotes: &ReliableRelayRemoteSet,
        service_instance: Option<RelayPathInstance>,
        instance: RelayPathInstance,
    ) -> bool {
        let Some(service) = service_instance else {
            return false;
        };
        if self.ordered_data_owner_instance != Some(service)
            || !remotes.contains_path_instance(service)
            || service.key.underlay != instance.key.underlay
        {
            return false;
        }
        remotes.paths.iter().any(|path| {
            path.instance() == instance
                && (instance == service
                    || (self.request_graduated_subflows.contains(&instance)
                        && (instance.key.underlay == UnderlayProtocol::Udp
                            || self.request_ack_clock_proven_subflows.contains(&instance))))
        })
    }

    pub(super) fn request_tcp_owner_ack_turnover_bytes(
        &self,
        remotes: &ReliableRelayRemoteSet,
        service_instance: Option<RelayPathInstance>,
        now: Instant,
    ) -> usize {
        let Some(service) = service_instance.filter(|service| {
            service.key.underlay == UnderlayProtocol::Tcp
                && self.ordered_data_owner_instance == Some(*service)
                && remotes.contains_path_instance(*service)
        }) else {
            return 0;
        };
        remotes
            .paths
            .iter()
            .filter_map(|path| {
                let instance = path.instance();
                if !self.request_owner_ack_can_grow_window(remotes, Some(service), instance) {
                    return None;
                }
                if instance != service
                    && !self
                        .request_window_turnover_proven_subflows
                        .contains(&instance)
                {
                    return None;
                }
                self.request_tcp_ack_turnover
                    .get(&instance)
                    .copied()
                    .filter(|model| model.is_fresh_at(now))
                    .map(|model| model.turnover_bytes)
            })
            .sum::<f64>()
            .ceil() as usize
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
                self.fail_client_path_instance(context, remotes, instance)
                    .await;
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
                self.fail_client_path_instance(context, remotes, instance)
                    .await;
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
                #[cfg(feature = "lab-diagnostics")]
                let (ack_complete, ack_ranges, ack_frontier, ack_largest_end) = match &ack_frame {
                    Frame::StreamAck {
                        complete, ranges, ..
                    } => (
                        *complete,
                        ranges.len(),
                        stream_ack_contiguous_frontier(*complete, ranges),
                        ranges.last().map_or(0, |range| range.end),
                    ),
                    _ => unreachable!("ack_frames only returns STREAM_ACK"),
                };
                match self
                    .send_control_frame(context, remotes, ack_frame, cause)
                    .await
                {
                    Ok(outcome) => {
                        sent_any = true;
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "recv_progress_ack_emit",
                            format_args!(
                                "stream_id={} complete={} ranges={} frontier={} largest_end={} recv_next_offset={} recv_reorder_bytes={} cause={} path_underlay={:?} path_index={}",
                                self.stream_id.0,
                                ack_complete,
                                ack_ranges,
                                ack_frontier,
                                ack_largest_end,
                                recv_stream.next_offset(),
                                recv_stream.reorder_bytes(),
                                cause.as_str(),
                                outcome.path_key.underlay,
                                outcome.path_key.index,
                            ),
                        );
                        #[cfg(not(feature = "lab-diagnostics"))]
                        let _ = outcome;
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
        {
            let (frame_offset, frame_end_offset) = match frame {
                Frame::StreamData {
                    offset, payload, ..
                } => (*offset, offset.saturating_add(payload.len() as u64)),
                _ => (0, 0),
            };
            lab_sender_service_decision(
                "client",
                None,
                self.stream_id.0,
                "primary",
                sender_service_frame_kind(frame),
                payload_bytes,
                None,
                format_args!(
                    "cause={} path_underlay={:?} path_index={} pacing_bytes={} repair={} frame_offset={} frame_end_offset={}",
                    cause.as_str(),
                    path_key.underlay,
                    path_key.index,
                    frame_pacing_bytes(frame),
                    cause.is_repair(),
                    frame_offset,
                    frame_end_offset,
                ),
            );
        }
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
mod tests;
