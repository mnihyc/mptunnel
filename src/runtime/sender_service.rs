use super::bulk_admission::{
    BulkAdmissionCheck, BulkAdmissionRole, bulk_active_service_product_envelope_bytes,
    bulk_additional_admission_role, bulk_candidate_admission_suppression_with_ordering_debt,
};
use super::*;

// Ownership boundary:
// Sender services own product work before it reaches carrier command queues.
// Client relay sending and server response dispatch both use this module for
// queueing, product flight ledgers, stream-ACK release, and diagnostics. Final
// TCP/UDP emission still happens through carrier command senders.

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
pub(super) enum RelaySendCause {
    StreamData,
    StreamFin,
    RecvProgress,
    AckGapRepair,
    PathFailureRepair,
}

impl RelaySendCause {
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::StreamData => "stream_data",
            Self::StreamFin => "stream_fin",
            Self::RecvProgress => "recv_progress",
            Self::AckGapRepair => "ack_gap_repair",
            Self::PathFailureRepair => "path_failure_repair",
        }
    }

    fn is_repair(self) -> bool {
        matches!(self, Self::AckGapRepair | Self::PathFailureRepair)
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct RelaySendOutcome {
    #[cfg_attr(not(any(test, feature = "lab-diagnostics")), allow(dead_code))]
    pub(super) path_key: RelayPathKey,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum ClientQueuedDispatch {
    Data { payload_bytes: usize },
    Repair { payload_bytes: usize },
}

#[derive(Debug)]
pub(super) struct RelaySenderService {
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    stream_id: StreamId,
    flights: RelayPathFlightLedger,
    ordered_data_owner: Option<RelayPathKey>,
    next_send_index: usize,
    performance: MppPerformanceConfig,
    extra_traffic: ExtraTrafficLedger,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct RelayRecvProgressSend {
    path: Option<PathSnapshot>,
    lane: FlowLane,
    force_max_data: bool,
}

impl RelayRecvProgressSend {
    pub(super) fn new(path: Option<PathSnapshot>, lane: FlowLane, force_max_data: bool) -> Self {
        Self {
            path,
            lane,
            force_max_data,
        }
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

#[derive(Debug)]
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
/// service owns queue accounting and dispatch diagnostics, while the
/// `ReliablePathStream` binding owns the current carrier-neutral path choice.
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
            "reason={} path_underlay={:?} path_id={} is_active={} sender_evidence={} bulk_rate_evidence={} role={} eta_ms={:.3} lead_underlay={} lead_path_id={} lead_eta_ms={:.3} stream_ordering_debt={} payload_bytes={} command_pending_bytes={} path_queue_bytes={} product_queue_bytes={} carrier_inflight_bytes={} product_inflight_bytes={} carrier_inflight_limit={} delivery_rate_mbps={:.3} pacing_mbps={:.3} srtt_ms={:.3} confidence={:.3} app_limited={} mux_max_path_flight={} mux_max_reorder={}",
            reason,
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
            target.commands.pending_bytes(),
            target.snapshot.queue_bytes,
            target.snapshot.product_queue_bytes,
            target.snapshot.bytes_in_flight,
            target.snapshot.product_bytes_in_flight,
            target.snapshot.inflight_limit_bytes,
            target.snapshot.delivery_rate_bps / 1_000_000.0,
            target.snapshot.pacing_rate_bps / 1_000_000.0,
            target.snapshot.srtt_ms,
            target.snapshot.confidence,
            target.snapshot.app_limited,
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
    lab_diagnostic(
        "server_bulk_output_selected",
        format_args!(
            "reason={} path_underlay={:?} path_id={} role={:?} work={:?} payload_bytes={} command_pending_bytes={} eta_ms={:.3} app_limited={} bulk_rate_evidence={}",
            reason,
            selected.target.key.underlay,
            selected.target.key.path_id.0,
            selected.admission.role,
            selected.admission.work,
            payload_bytes,
            selected.target.commands.pending_bytes(),
            selected.target.eta_ms,
            selected.target.snapshot.app_limited,
            selected.target.has_bulk_rate_evidence,
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
    service: CarrierPathKey,
    owner_credit_bytes: usize,
    optional_overhead_budget_bytes: usize,
    max_read_gap_budget: Duration,
    input: SubflowAdmissionInput,
}

#[derive(Clone)]
struct ResponseSelectedDataTarget {
    target: ResponseSenderPathTarget,
    admission: PathAdmission,
    subflow_set_commit: Option<ResponseSubflowAdmissionCommit>,
}

fn response_bulk_admission_role(
    service_key: CarrierPathKey,
    candidate: CarrierPathKey,
    lower_owner: Option<CarrierPathKey>,
    ordering_debt: u64,
) -> BulkAdmissionRole {
    if lower_owner == Some(candidate) || (candidate == service_key && ordering_debt == 0) {
        BulkAdmissionRole::ActiveDataPath
    } else if let Some(owner) = lower_owner {
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
    lower_owner
        .or(ordered_data_owner)
        .or_else(|| {
            candidates
                .iter()
                .copied()
                .find(|candidate| candidate.is_active)
                .map(|candidate| candidate.key)
        })
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
    response_target_has_service_anchor_rights(target) || target.has_bulk_rate_evidence
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
        0,
        payload_bytes,
        mux_limits,
        None,
        false,
    )
    .0
}

// Decides whether one candidate may own the next unique product byte range.
//
// The important split is:
// * Service: the current active/lower-frontier owner, kept fed while healthy.
// * Subflow: additional path admitted for owner bytes only after path-scoped
//   bulk-rate evidence and candidate-specific ordering safety.
//
// Path proof, ACK-data visibility, and carrier attachment are intentionally not
// owner states. They are evidence inputs for this decision.
fn response_target_unique_owner_admission_with_epoch(
    target: &ResponseSenderPathTarget,
    candidates: &[&ResponseSenderPathTarget],
    lead: ResponseBulkLead,
    lower_owner: Option<CarrierPathKey>,
    ordered_data_owner: Option<CarrierPathKey>,
    ordering_debt: u64,
    _ordered_owner_debt_bytes: usize,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    subflow_set: Option<&FlowSubflowSet>,
    allow_sender_evidence_service_failover: bool,
) -> (PathAdmission, Option<ResponseSubflowAdmissionCommit>) {
    let service_key =
        response_service_anchor_key(candidates, lower_owner, ordered_data_owner, lead.key);
    let sender_evidence_service_failover = allow_sender_evidence_service_failover
        && target.key == service_key
        && target.has_sender_evidence;
    if lower_owner == Some(target.key) {
        if ordering_debt > 0 {
            return (PathAdmission::standby(), None);
        }
        return if target.is_active || target.has_bulk_rate_evidence {
            (PathAdmission::service(), None)
        } else {
            (PathAdmission::probe_only(), None)
        };
    }
    if lower_owner.is_some() {
        return (PathAdmission::standby(), None);
    }
    if target.key == service_key {
        return if target.is_active
            || target.has_bulk_rate_evidence
            || sender_evidence_service_failover
        {
            (PathAdmission::service(), None)
        } else {
            (PathAdmission::probe_only(), None)
        };
    }
    if target.is_active {
        return (PathAdmission::standby(), None);
    }

    let role = response_bulk_admission_role(service_key, target.key, lower_owner, ordering_debt);
    let model_allows_owner =
        !response_unique_quic_data_would_expand_ordering_debt(lower_owner, target, ordering_debt)
            && bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
                best_snapshot: lead.snapshot,
                best_eta_ms: lead.eta_ms,
                candidate_snapshot: target.snapshot,
                candidate_eta_ms: target.eta_ms,
                payload_bytes,
                mux_limits,
                role,
                stream_ordering_debt_bytes: ordering_debt,
            })
            .is_none();
    let completion_improves = model_allows_owner && target.has_bulk_rate_evidence;
    let owner_credit_bytes = payload_bytes;
    let input = SubflowAdmissionInput {
        key: target.key,
        bulk_rate_proven: target.has_bulk_rate_evidence,
        frontier_clear: model_allows_owner,
        completion_improves,
        observed_goodput_non_degrading: model_allows_owner,
        read_gap: Duration::ZERO,
        owner_bytes: payload_bytes,
        optional_overhead_bytes: 0,
    };
    let mut epoch = subflow_set
        .filter(|epoch| epoch.matches_envelope(service_key, owner_credit_bytes, 0, Duration::ZERO))
        .cloned()
        .unwrap_or_else(|| {
            FlowSubflowSet::new(0, service_key, owner_credit_bytes, 0, Duration::ZERO)
        });
    let admission = epoch.admit_subflow_owner(input);
    let commit = (admission.decision == PathAdmissionDecision::AdmitSubflow).then_some(
        ResponseSubflowAdmissionCommit {
            service: service_key,
            owner_credit_bytes,
            optional_overhead_budget_bytes: 0,
            max_read_gap_budget: Duration::ZERO,
            input,
        },
    );
    (admission, commit)
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
        0,
        payload_bytes,
        mux_limits,
        subflow_set,
        false,
    )
    .0;
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

fn response_ordered_owner_debt_pressure_bytes(
    owner: &ResponseSenderPathTarget,
    lane: FlowLane,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> usize {
    let ack_step = usize::try_from(reliable_stream_ack_update_bytes(
        Some(owner.snapshot),
        lane,
        mux_limits,
    ))
    .unwrap_or(usize::MAX);
    ack_step
        .max(payload_bytes)
        .max(reliable_bulk_carrier_feed_quantum_bytes(mux_limits))
        .min(mux_limits.max_repair_bytes.max(payload_bytes))
        .max(1)
}

fn response_ordered_owner_debt_exceeds_pressure(
    owner: &ResponseSenderPathTarget,
    lane: FlowLane,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    ordered_owner_debt_bytes: usize,
) -> bool {
    ordered_owner_debt_bytes
        > response_ordered_owner_debt_pressure_bytes(owner, lane, payload_bytes, mux_limits)
}

fn response_owner_debt_pressure(
    all_targets: &[ResponseSenderPathTarget],
    lane: FlowLane,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    ordered_data_owner: Option<CarrierPathKey>,
    ordered_owner_debt_bytes: usize,
) -> Option<CarrierPathKey> {
    if ordered_owner_debt_bytes == 0 {
        return None;
    }
    let owner_key = ordered_data_owner?;
    let owner_for_pressure = all_targets.iter().find(|target| target.key == owner_key)?;
    if !response_ordered_owner_debt_exceeds_pressure(
        owner_for_pressure,
        lane,
        payload_bytes,
        mux_limits,
        ordered_owner_debt_bytes,
    ) {
        return None;
    }

    // Ordered-owner scheduling debt is an evidence/ordering-safety filter, not a
    // Service priority bypass. The current Service owner and measured Subflows
    // that do not expand cross-path ordering debt compete through the normal
    // no-worse admission checks; proof-only and debt-expanding cross-family
    // candidates remain Probe/Standby/RepairOnly.
    Some(owner_key)
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
        Some(owner) => !targets.iter().any(|target| target.key == owner),
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
    mux_limits: MuxLimits,
    payload_bytes: usize,
    lower_flights: &[CarrierPathFlightDebt],
    allow_sender_evidence_service_failover: bool,
) -> Option<ResponseBulkLead> {
    let lower_owner = response_oldest_lower_flight_owner(lower_flights);
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

    if let Some(active) = candidate_targets
        .iter()
        .copied()
        .find(|target| target.is_active)
        && response_active_lead_suppression(active, mux_limits, payload_bytes, 0).is_none()
    {
        return Some(ResponseBulkLead {
            key: active.key,
            snapshot: active.snapshot,
            eta_ms: active.eta_ms,
        });
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

    if lower_owner.is_none() && allow_sender_evidence_service_failover {
        return candidate_targets
            .iter()
            .copied()
            .filter(|target| target.has_sender_evidence)
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

fn response_target_has_repair_evidence(target: &ResponseSenderPathTarget) -> bool {
    target.is_active || target.has_bulk_rate_evidence
}

fn choose_response_repair_target(
    targets: &[ResponseSenderPathTarget],
    avoid_keys: &[CarrierPathKey],
) -> Option<ResponseSenderPathTarget> {
    debug_assert!(PathRuntimeRole::RepairOnly.may_repair());
    let proven_targets = targets
        .iter()
        .filter(|target| response_target_has_repair_evidence(target))
        .cloned()
        .collect::<Vec<_>>();
    choose_lowest_eta_response_target(&proven_targets, avoid_keys, true)
        .or_else(|| choose_lowest_eta_response_target(targets, avoid_keys, true))
        .or_else(|| choose_lowest_eta_response_target(&proven_targets, avoid_keys, false))
        .or_else(|| choose_lowest_eta_response_target(targets, avoid_keys, false))
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
    repair: bool,
) -> Option<ResponseSenderPathTarget> {
    if targets.is_empty() {
        return None;
    }
    let payload_bytes = reliable_stream_frame_payload_bytes(frame);
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
                && response_target_has_emission_credit(
                    target,
                    effective_lane,
                    payload_bytes,
                    mux_limits,
                )
        })
        .cloned()
        .collect::<Vec<_>>();
    if capacity_targets.is_empty() {
        return None;
    }
    let targets = capacity_targets.as_slice();
    if repair {
        return choose_response_repair_target(targets, avoid_keys);
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
    if targets.is_empty() {
        return None;
    }
    let mut capacity_targets = Vec::new();
    for target in targets {
        if !target.commands.can_enqueue_lane_now(lane) {
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
        if !response_target_has_emission_credit(target, lane, payload_bytes, mux_limits) {
            #[cfg(feature = "lab-diagnostics")]
            lab_response_bulk_output_candidate(
                "no_emission_credit",
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
    if !relay_lane_is_bulk(lane) {
        return choose_response_service_or_proven_data_target(
            &capacity_targets,
            lower_flights,
            &[],
        )
        .map(|target| ResponseSelectedDataTarget {
            target,
            admission: PathAdmission::service(),
            subflow_set_commit: None,
        });
    }

    let debt_pressure = response_owner_debt_pressure(
        targets,
        lane,
        payload_bytes,
        mux_limits,
        ordered_data_owner,
        ordered_owner_debt_bytes,
    );
    let debt_pressure_owner = debt_pressure;

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
    let effective_lower_owner = lower_owner.or(debt_pressure_owner);
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
    let ordered_owner_anchor =
        ordered_data_owner.filter(|owner| targets.iter().any(|target| target.key == *owner));
    if let Some(service_key) = ordered_owner_anchor
        && let Some(service) = targets.iter().find(|target| target.key == service_key)
    {
        let service_has_capacity = candidate_targets
            .iter()
            .any(|target| target.key == service_key);
        let service_is_backpressured = !service_has_capacity
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
    if let Some(owner_key) = debt_pressure_owner {
        #[cfg(feature = "lab-diagnostics")]
        for target in &candidate_targets {
            if target.key != owner_key
                && !(target.has_bulk_rate_evidence
                    && (target.key.underlay == owner_key.underlay
                        || response_ordering_debt_bytes(lower_flights, target.key) == 0))
            {
                lab_response_bulk_output_candidate(
                    "debt_pressure_filter",
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
            target.key == owner_key
                || (target.has_bulk_rate_evidence
                    && (target.key.underlay == owner_key.underlay
                        || response_ordering_debt_bytes(lower_flights, target.key) == 0))
        });
        if candidate_targets.is_empty() {
            return None;
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
    let allow_sender_evidence_service_failover = effective_lower_owner.is_none()
        && ordered_owner_anchor.is_none()
        && ordered_owner_debt_bytes == 0
        && !candidate_targets.iter().any(|target| target.is_active);
    let Some(lead) = choose_response_admissible_lead(
        &candidate_targets,
        mux_limits,
        payload_bytes,
        lower_flights,
        allow_sender_evidence_service_failover,
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
        ordered_owner_anchor,
        lead.key,
    );
    let admitted = candidate_targets
        .iter()
        .copied()
        .filter_map(|target| {
            let ordering_debt = if debt_pressure_owner.is_some() {
                if lower_owner.is_none() {
                    ordered_owner_debt_bytes as u64
                } else {
                    response_ordering_debt_bytes(lower_flights, target.key)
                }
            } else {
                response_ordering_debt_bytes(lower_flights, target.key)
            };
            let (admission, subflow_set_commit) = response_target_unique_owner_admission_with_epoch(
                target,
                &candidate_targets,
                lead,
                effective_lower_owner,
                ordered_owner_anchor,
                ordering_debt,
                ordered_owner_debt_bytes,
                payload_bytes,
                mux_limits,
                subflow_set,
                allow_sender_evidence_service_failover,
            );
            if !matches!(
                admission.decision,
                PathAdmissionDecision::Service | PathAdmissionDecision::AdmitSubflow
            ) || admission.work != CarrierWorkKind::OwnerData
                || !admission.role.may_own_unique_data()
            {
                #[cfg(feature = "lab-diagnostics")]
                lab_response_bulk_output_candidate(
                    "not_owner_admission",
                    target,
                    payload_bytes,
                    mux_limits,
                    ResponseBulkCandidateDiag {
                        lead: Some(lead),
                        role: Some(response_bulk_admission_role(
                            service_key,
                            target.key,
                            effective_lower_owner,
                            ordering_debt,
                        )),
                        ordering_debt,
                    },
                );
                return None;
            }
            let role = response_bulk_admission_role(
                service_key,
                target.key,
                effective_lower_owner,
                ordering_debt,
            );
            let suppression =
                bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
                    best_snapshot: lead.snapshot,
                    best_eta_ms: lead.eta_ms,
                    candidate_snapshot: target.snapshot,
                    candidate_eta_ms: target.eta_ms,
                    payload_bytes,
                    mux_limits,
                    role,
                    stream_ordering_debt_bytes: ordering_debt,
                });
            if let Some(_reason) = suppression {
                #[cfg(feature = "lab-diagnostics")]
                lab_response_bulk_output_candidate(
                    _reason,
                    target,
                    payload_bytes,
                    mux_limits,
                    ResponseBulkCandidateDiag {
                        lead: Some(lead),
                        role: Some(role),
                        ordering_debt,
                    },
                );
                return None;
            }
            Some(ResponseSelectedDataTarget {
                target: target.clone(),
                admission,
                subflow_set_commit,
            })
        })
        .collect::<Vec<_>>();
    if let Some(service_target) =
        response_feedable_service_owner_target_before_app_limited_subflows(&admitted)
    {
        #[cfg(feature = "lab-diagnostics")]
        lab_response_bulk_output_selected("service_feed", &service_target, payload_bytes);
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

fn response_repair_minimum_useful_burst_bytes(mux_limits: MuxLimits) -> usize {
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
    let jitter_hysteresis_ms = old_lead.snapshot.jitter_ms.max(best.snapshot.jitter_ms);
    let queue_hysteresis_bytes = payload_bytes as u64;
    old_lead.eta_ms <= best.eta_ms + jitter_hysteresis_ms
        && old_lead.snapshot.queue_bytes <= best.snapshot.queue_bytes + queue_hysteresis_bytes
}

fn response_target_committed_product_bytes(target: &ResponseSenderPathTarget) -> u64 {
    target
        .snapshot
        .product_bytes_in_flight
        .saturating_add(target.snapshot.product_queue_bytes)
        .saturating_add(target.snapshot.queue_bytes)
}

fn response_feedable_service_owner_target_before_app_limited_subflows(
    admitted: &[ResponseSelectedDataTarget],
) -> Option<ResponseSelectedDataTarget> {
    let service = admitted
        .iter()
        .filter(|selected| selected.admission.role == PathRuntimeRole::Service)
        .min_by(|left, right| {
            response_target_committed_product_bytes(&left.target)
                .cmp(&response_target_committed_product_bytes(&right.target))
                .then_with(|| left.target.eta_ms.total_cmp(&right.target.eta_ms))
                .then_with(|| carrier_path_key_order(left.target.key, right.target.key))
        })
        .cloned()?;
    let non_app_limited_subflow = admitted.iter().any(|selected| {
        selected.admission.role == PathRuntimeRole::Subflow && !selected.target.snapshot.app_limited
    });
    (!non_app_limited_subflow).then_some(service)
}

fn response_target_has_emission_credit(
    target: &ResponseSenderPathTarget,
    lane: FlowLane,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> bool {
    if !relay_lane_is_bulk(lane) {
        return true;
    }
    let credit = response_target_emission_credit_bytes(target, lane, payload_bytes, mux_limits);
    target
        .commands
        .pending_bytes()
        .saturating_add(payload_bytes as u64)
        <= credit as u64
}

fn response_target_emission_credit_bytes(
    target: &ResponseSenderPathTarget,
    lane: FlowLane,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> usize {
    if relay_lane_is_bulk(lane) {
        if target.is_active {
            return usize::try_from(bulk_active_service_product_envelope_bytes(
                target.snapshot,
                payload_bytes,
                mux_limits,
            ))
            .unwrap_or(usize::MAX)
            .max(payload_bytes)
            .max(1);
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
    plan_response_data_dispatch_with_product_debt(stream, relay_lane, next_offset, payload_bytes, 0)
}

fn plan_response_data_dispatch_with_product_debt(
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
            let lower_flights = binding.lower_flights_before_offset(next_offset);
            let targets = binding.sender_path_targets(relay_lane, payload_bytes);
            let ordered_data_owner = binding.ordered_data_owner();
            let subflow_set = binding.subflow_set_snapshot();
            let Some(selected) = select_response_sender_data_target_with_ordered_debt_and_epoch(
                &targets,
                relay_lane,
                payload_bytes,
                binding.mux_limits(),
                &lower_flights,
                ordered_data_owner,
                ordered_owner_debt_bytes,
                subflow_set.as_ref(),
            ) else {
                return Err(RuntimeError::SenderServiceBlocked);
            };
            let target = selected.target;
            let role = selected.admission.role;
            debug_assert!(
                role != PathRuntimeRole::Subflow || target.has_bulk_rate_evidence,
                "Subflow OwnerData requires bulk-rate evidence: target={:?} role={:?} ordered_owner={:?} lower_owner={:?} is_active={} sender_evidence={} bulk_evidence={}",
                target.key,
                role,
                ordered_data_owner,
                response_oldest_lower_flight_owner(&lower_flights),
                target.is_active,
                target.has_sender_evidence,
                target.has_bulk_rate_evidence,
            );
            Ok(ResponseDataDispatchPlan {
                primary: ResponseDataDispatchTarget::Switchable {
                    binding: binding.clone(),
                    target,
                    role,
                    subflow_set_commit: selected.subflow_set_commit,
                },
            })
        }
    }
}

fn response_dispatch_payload_bytes(
    path_stream: &ReliablePathStream,
    relay_lane: FlowLane,
    mux_limits: MuxLimits,
    queued_payload_bytes: usize,
) -> usize {
    let snapshot = path_stream.send_path_snapshot(relay_lane, queued_payload_bytes);
    adaptive_reliable_relay_chunk_bytes_with_frame_limit(
        snapshot,
        relay_lane,
        mux_limits,
        path_stream.max_frame_payload_bytes,
    )
    .min(queued_payload_bytes)
    .max(1)
}

fn response_frame_has_carrier_credit(
    stream: &ReliablePathStream,
    frame: &Frame,
    lane: FlowLane,
    emit_mode: ResponseCarrierEmitMode,
    repair: bool,
) -> bool {
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
            let lower_flights = if relay_frame_is_bulk_stream_data(frame, lane) && !repair {
                binding.lower_flights_before_frame(frame)
            } else {
                Vec::new()
            };
            let avoid_keys = if repair {
                binding.flight_keys_overlapping_frame(frame)
            } else {
                Vec::new()
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
                repair,
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
        } => {
            match send_sender_service_frame_to_carrier(
                &target.commands,
                frame.clone(),
                lane,
                ResponseCarrierEmitMode::StreamOrdered,
            )
            .await
            {
                Ok(()) => {}
                Err(RuntimeError::SenderServiceBlocked) => {
                    return Err(RuntimeError::SenderServiceBlocked);
                }
                Err(_) => {
                    binding.detach(target.key, &target.commands);
                    return Err(RuntimeError::SenderServiceBlocked);
                }
            }
            binding.record_owner_flight(target.key, &frame);
            if let Some(commit) = subflow_set_commit {
                let _ = binding.commit_subflow_owner_admission(
                    commit.service,
                    commit.owner_credit_bytes,
                    commit.optional_overhead_budget_bytes,
                    commit.max_read_gap_budget,
                    commit.input,
                );
            }
            if role == PathRuntimeRole::Service {
                binding.set_ordered_data_owner(target.key);
            }
            let decision_reason = match role {
                PathRuntimeRole::Service => "data_service",
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
    repair: bool,
) -> Result<Option<CarrierPathKey>, RuntimeError> {
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
            let lower_flights = if relay_frame_is_bulk_stream_data(&frame, lane) && !repair {
                binding.lower_flights_before_frame(&frame)
            } else {
                Vec::new()
            };
            let avoid_keys = if repair {
                binding.flight_keys_overlapping_frame(&frame)
            } else {
                Vec::new()
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
                    repair,
                ) else {
                    return Err(RuntimeError::SenderServiceBlocked);
                };
                match send_sender_service_frame_to_carrier(
                    &target.commands,
                    frame.clone(),
                    lane,
                    emit_mode,
                )
                .await
                {
                    Ok(()) => {
                        if matches!(frame, Frame::StreamData { .. }) {
                            if repair {
                                binding.record_repair_flight(target.key, &frame);
                            } else {
                                binding.record_owner_flight(target.key, &frame);
                            }
                            if !repair {
                                binding.set_ordered_data_owner(target.key);
                            }
                        }
                        record_server_sender_decision(
                            binding.session_id(),
                            stream.stream_id,
                            target.key,
                            &frame,
                            lane,
                            reason,
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
        false,
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
        if remaining < response_repair_minimum_useful_burst_bytes(mux_limits) {
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
                response_frame_has_carrier_credit(
                    path_stream,
                    frame,
                    carrier_lane,
                    emit_mode,
                    false,
                )
            }
            ReliableRelayQueuedWorkKind::Data(payload) => {
                plan_response_data_dispatch_with_product_debt(
                    path_stream,
                    queued.data_lane.unwrap_or(relay_lane),
                    send_stream.next_offset(),
                    response_dispatch_payload_bytes(
                        path_stream,
                        queued.data_lane.unwrap_or(relay_lane),
                        mux_limits,
                        payload.len(),
                    ),
                    ordered_owner_debt_bytes,
                )
                .is_ok()
            }
            ReliableRelayQueuedWorkKind::Repair { frame, .. } => response_frame_has_carrier_credit(
                path_stream,
                frame,
                FlowLane::Latency,
                ResponseCarrierEmitMode::Classified,
                true,
            ),
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

    pub(super) fn enqueue_critical_repair_frame(&mut self, frame: Frame) -> u64 {
        let payload_bytes = reliable_stream_frame_payload_bytes(&frame);
        debug_assert!(CarrierWorkKind::RepairData.counts_against_sender_extra_budget());
        self.extra_traffic
            .record_optional(ExtraTrafficKind::Repair, payload_bytes);
        self.queue
            .push_critical_repair_with_cause(frame, RelaySendCause::AckGapRepair)
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
        let (frame, dispatch_lane_name) = match &queued.kind {
            ReliableRelayQueuedWorkKind::Control(frame) => (frame.clone(), "control"),
            ReliableRelayQueuedWorkKind::Data(payload) => {
                let data_lane = queued.data_lane.unwrap_or(relay_lane);
                let dispatch_payload_bytes = response_dispatch_payload_bytes(
                    path_stream,
                    data_lane,
                    mux_limits,
                    payload.len(),
                );
                let dispatch_payload = payload.slice(..dispatch_payload_bytes);
                let planned = plan_response_data_dispatch_with_product_debt(
                    path_stream,
                    data_lane,
                    send_stream.next_offset(),
                    dispatch_payload.len(),
                    ordered_owner_debt_bytes,
                )?;
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
            ReliableRelayQueuedWorkKind::Repair { frame, .. } => (frame.clone(), "repair"),
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
                    false,
                )
                .await?
            }
            ReliableRelayQueuedWorkLane::Data => match emit_response_frame_from_sender_service(
                path_stream,
                frame.clone(),
                reliable_path_effective_frame_lane(&frame, relay_lane),
                ResponseCarrierEmitMode::Classified,
                "data",
                false,
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
                    FlowLane::Latency,
                    ResponseCarrierEmitMode::Classified,
                    "tail_repair",
                    true,
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
            ReliableRelayQueuedWorkLane::Repair => FlowLane::Latency,
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
                    format_args!(
                        "path_underlay={:?} path_id={} lane={:?} pacing_bytes={} fixed_output=true",
                        selected_path.underlay, selected_path.path_id.0, send_lane, pacing_bytes,
                    ),
                );
            }
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
        if remaining < response_repair_minimum_useful_burst_bytes(mux_limits) {
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
        let avoid_keys = if cause.is_repair() {
            self.flights.sent_keys_for_frame(&sent_frame)
        } else {
            Vec::new()
        };
        let path_key = self
            .emit_relay_frame(context, remotes, frame, cause, &avoid_keys)
            .await?;
        let payload_bytes = if cause.is_repair() {
            self.flights.record_repair_frame(path_key, &sent_frame)
        } else {
            self.flights.record_owner_frame(path_key, &sent_frame)
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
    ) -> Result<RelayPathKey, RuntimeError> {
        let mut last_error = None;
        while !remotes.paths.is_empty() {
            let stream_lane = remotes
                .paths
                .last()
                .map(|path| path.stream.lane)
                .unwrap_or(FlowLane::Latency);
            let position = match self.choose_relay_path_position(
                context,
                remotes,
                &frame,
                stream_lane,
                cause,
                avoid_keys,
            ) {
                Ok(position) => position,
                Err(RuntimeError::SenderServiceBlocked) => {
                    return Err(RuntimeError::SenderServiceBlocked);
                }
                Err(err) => return Err(last_error.unwrap_or(err)),
            };
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
                        if !cause.is_repair() {
                            self.ordered_data_owner = Some(instance.key);
                        }
                    }
                    self.next_send_index = if remotes.paths.is_empty() {
                        0
                    } else {
                        (position + 1) % remotes.paths.len()
                    };
                    return Ok(instance.key);
                }
                Err(RuntimeError::SenderServiceBlocked) => {
                    return Err(RuntimeError::SenderServiceBlocked);
                }
                Err(err) => {
                    last_error = Some(err);
                    if self.ordered_data_owner == Some(instance.key) {
                        self.ordered_data_owner = None;
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
    ) -> Result<usize, RuntimeError> {
        if remotes.paths.is_empty() {
            return Err(RuntimeError::ReliablePathSessionClosed);
        }
        self.next_send_index %= remotes.paths.len();
        if self
            .ordered_data_owner
            .is_some_and(|lead| !remotes.contains_path_key(lead))
        {
            self.ordered_data_owner = None;
        }
        if matches!(frame, Frame::StreamData { .. }) && !cause.is_repair() {
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
            }) {
                BulkRelayPathChoice::Selected(position) => return Ok(position),
                BulkRelayPathChoice::Blocked => return Err(RuntimeError::SenderServiceBlocked),
                BulkRelayPathChoice::NotApplicable => {}
            }
        }
        self.choose_lowest_eta_relay_path(
            context,
            remotes,
            frame,
            lane,
            cause,
            avoid_keys,
            matches!(frame, Frame::StreamData { .. }) && !cause.is_repair(),
        )
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
        let payload_bytes = reliable_stream_frame_payload_bytes(frame);
        if matches!(cause, RelaySendCause::RecvProgress)
            && let Some(position) = choose_active_recv_progress_path_position(remotes, frame, cause)
        {
            return Ok(position);
        }
        let has_active_path = remotes
            .paths
            .iter()
            .any(|path| path.placement == RelayPathPlacement::Active);
        let ordinary_path_allowed = |path: &ReliableRelayRemotePath| {
            !ordinary_stream_data
                || !has_active_path
                || path.placement == RelayPathPlacement::Active
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
        if let Some(position) = choose(true).or_else(|| choose(false)) {
            return Ok(position);
        }
        let capacity_fallback = remotes
            .paths
            .iter()
            .enumerate()
            .filter(|(_, path)| ordinary_path_allowed(path))
            .filter(|(_, path)| can_enqueue(path))
            .map(|(position, _)| position)
            .find(|position| !avoid_keys.contains(&remotes.paths[*position].key()))
            .or_else(|| {
                remotes
                    .paths
                    .iter()
                    .enumerate()
                    .filter(|(_, path)| ordinary_path_allowed(path))
                    .filter(|(_, path)| can_enqueue(path))
                    .map(|(position, _)| position)
                    .next()
            });
        if let Some(position) = capacity_fallback {
            return Ok(position);
        }
        if remotes.paths.iter().any(ordinary_path_allowed) {
            Err(RuntimeError::SenderServiceBlocked)
        } else {
            Err(RuntimeError::ReliablePathSessionClosed)
        }
    }

    pub(super) fn release_normalized_acked_ranges(
        &mut self,
        context: &ClientPathContext,
        ranges: &[OffsetRange],
    ) {
        for release in self.flights.release_normalized_acked_ranges(ranges) {
            context.release_relay_path_inflight(
                release.key.underlay,
                release.key.index,
                release.bytes,
            );
            if release.path_proving
                && let Some(sample) = PathRateSample::new(release.bytes as u64, release.elapsed)
            {
                context.mark_relay_path_rate_sample(
                    release.key.underlay,
                    release.key.index,
                    sample,
                );
            }
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "path_model",
                format_args!(
                    "stream_id={} path_underlay={:?} path_index={} released_bytes={} elapsed_ms={:.3} path_proving={} cause=stream_ack",
                    self.stream_id.0,
                    release.key.underlay,
                    release.key.index,
                    release.bytes,
                    release.elapsed.as_secs_f64() * 1000.0,
                    release.path_proving,
                ),
            );
        }
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

    pub(super) async fn reannounce_active_path(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        spec: &ReliableRelayOpenSpec,
        lane: FlowLane,
    ) -> Result<(), RuntimeError> {
        let Some(position) = remotes.paths.len().checked_sub(1) else {
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
                    .send_control_frame(context, remotes, ack_frame, RelaySendCause::RecvProgress)
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
                    RelaySendCause::RecvProgress,
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

    pub(super) fn enqueue_failed_path_gap_repairs(
        &mut self,
        sender_queue: &mut ReliableRelaySenderQueue,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        send_stream: &ReliableSendStream,
        failed_key: RelayPathKey,
        lane: FlowLane,
    ) -> bool {
        let ranges = self.flights.latest_unacked_ranges_for_path(failed_key);
        if ranges.is_empty() {
            return false;
        }
        let repair_path = remotes
            .primary_path_key()
            .and_then(|key| context.reliable_path_snapshot(key));
        let repair_limit =
            adaptive_reliable_relay_repair_bytes(repair_path, lane, context.mux_limits)
                .min(self.repair_extra_event_budget_remaining(context.mux_limits));
        let repair_frames = send_stream.retransmission_frames_for_ranges(&ranges, repair_limit);
        if repair_frames.is_empty() {
            return false;
        }
        let mut queued = false;
        for frame in repair_frames {
            let queued_frame = self.enqueue_repair_frame_with_priority(
                sender_queue,
                frame,
                RelaySendCause::PathFailureRepair,
                context.mux_limits,
                true,
            );
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
    use crate::runtime::bulk_admission::bulk_startup_service_horizon_payload_bytes;

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
        sender.release_normalized_acked_ranges(
            &context,
            &[OffsetRange::new(0, PATH_OPEN_SCORE_BYTES as u64).expect("ack range")],
        );
        let after = context.tcp_path_snapshot(0).expect("after snapshot");

        assert_eq!(after.bytes_in_flight, 0);
        assert_ne!(
            after.delivery_rate_bps, before.delivery_rate_bps,
            "an unambiguous owner STREAM_ACK is path-scoped delivery evidence"
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

        sender.release_normalized_acked_ranges(
            &context,
            &[OffsetRange::new(0, PATH_OPEN_SCORE_BYTES as u64).expect("ack range")],
        );
        let after = context.tcp_path_snapshot(0).expect("after snapshot");

        assert_eq!(after.bytes_in_flight, 0);
        assert!(
            !context.relay_path_has_bulk_model_evidence(owner.underlay, owner.index),
            "ACK of a duplicated request byte range releases inflight state but must not seed path evidence"
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
            key: CarrierPathKey {
                underlay,
                path_id: PathId(path_id),
            },
            commands,
            snapshot,
            eta_ms,
            is_active,
            has_sender_evidence: true,
            has_bulk_rate_evidence: true,
        }
    }

    fn client_test_context() -> ClientPathContext {
        let path = "tcp://127.0.0.1:10251".parse::<PathSpec>().expect("path");
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("context")
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
    async fn client_owner_data_updates_ordered_owner_after_frontier_clear_migration() {
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
            Some(fast_key),
            "the client-side service owner marker must follow the path that actually owns new bytes"
        );
        assert!(matches!(
            try_recv_reliable_path_command(&mut fast_rx),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
        ));
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
    fn response_repair_extra_budget_accumulates_until_useful_burst() {
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
        let min_burst = response_repair_minimum_useful_burst_bytes(mux_limits);

        assert!(sender.repair_extra_event_budget_remaining(mux_limits) >= min_burst);
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

        sender.record_owner_progress_for_test(min_burst.saturating_mul(100));
        assert!(
            sender.repair_extra_event_budget_remaining(mux_limits) >= min_burst,
            "once enough owner bytes make ACK progress, repair can spend a useful burst"
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
            false,
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
            false,
        )
        .expect("stream-ordered final control should remain dispatchable");

        assert_eq!(
            selected.key, active_data_owner.key,
            "FIN/final-offset must not move to a validation path and overtake older data"
        );
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
            false,
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
            key: CarrierPathKey {
                underlay: UnderlayProtocol::Udp,
                path_id: PathId(0),
            },
            commands,
            snapshot,
            eta_ms: 1.0,
            is_active: true,
            has_sender_evidence: true,
            has_bulk_rate_evidence: true,
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
            saturated.commands.pending_bytes() >= credit as u64,
            "test must fill the low-ETA writer pipe enough to exercise byte credit"
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
            usize::try_from(bulk_active_service_product_envelope_bytes(
                active.snapshot,
                payload_bytes,
                mux_limits,
            ))
            .unwrap(),
            "active response owner must be fed by the product envelope, not current carrier cwnd"
        );
        assert!(
            credit > payload_bytes,
            "the regression requires credit above one carrier quantum"
        );
    }

    #[test]
    fn active_tcp_response_owner_without_product_progress_uses_startup_feedback_credit() {
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
            bulk_startup_service_horizon_payload_bytes(payload_bytes, mux_limits),
            "unproven Service startup uses bounded startup-feedback credit, not a tiny carrier quantum and not the full geometric Service horizon"
        );
        assert!(
            credit >= payload_bytes,
            "startup Service credit must still admit at least one bulk quantum"
        );
    }

    #[test]
    fn response_quic_feed_credit_uses_live_carrier_debt_not_outdated_bdp() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = 64 * 1024;
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
            usize::try_from(bulk_active_service_product_envelope_bytes(
                loaded_quic.snapshot,
                payload_bytes,
                mux_limits,
            ))
            .unwrap(),
            "active QUIC Service feed credit must follow the product Service envelope, not live carrier debt"
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
            usize::try_from(bulk_active_service_product_envelope_bytes(
                loaded_tcp.snapshot,
                payload_bytes,
                mux_limits,
            ))
            .unwrap(),
            "active TCP owners use the same carrier-neutral product Service envelope as active QUIC owners"
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
    fn quic_proof_success_path_does_not_become_unique_validation_owner() {
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

        let selected = choose_response_sender_data_target(
            &[active.clone(), proof_success.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(active.key),
        )
        .expect("active path should remain the unique owner");

        assert_eq!(
            selected.key, active.key,
            "proof-success QUIC validation paths must use duplicate/non-owner validation, not unique owner data"
        );

        let mut validation_in_flight = proof_success;
        validation_in_flight.snapshot.product_bytes_in_flight = payload_bytes as u64;
        let selected_after_credit = choose_response_sender_data_target(
            &[active.clone(), validation_in_flight.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(active.key),
        )
        .expect("active path remains available while validation flight is outstanding");

        assert_eq!(
            selected_after_credit.key, active.key,
            "proof-only validation is one bounded quantum until ACK-derived data evidence arrives"
        );
    }

    #[test]
    fn proof_paths_are_not_product_data_dispatch_targets() {
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

        let selected = choose_response_sender_data_target(
            &[active.clone(), proof_success],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(active.key),
        )
        .expect("service owner remains dispatchable");

        assert_eq!(
            selected.key, active.key,
            "Probe paths must use path-scoped control-plane proof, not product STREAM_DATA"
        );
    }

    #[test]
    fn measured_udp_bulk_path_beats_poor_udp_active_path() {
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
            &[active_udp, measured_udp.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(CarrierPathKey {
                underlay: UnderlayProtocol::Udp,
                path_id: PathId(0),
            }),
        )
        .expect("measured same-family UDP path should be eligible for ordinary bulk");

        assert_eq!(
            selected.key, measured_udp.key,
            "within one carrier family, live metrics must override active-path stickiness"
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
    fn active_tcp_response_owner_uses_product_feed_envelope() {
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
            usize::try_from(bulk_active_service_product_envelope_bytes(
                target.snapshot,
                payload_bytes,
                mux_limits,
            ))
            .unwrap(),
            "active TCP and QUIC owners should use the same product Service envelope; transport pacing belongs below the sender service"
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
    fn clear_frontier_without_live_service_elects_sender_evidence_service_failover() {
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
        restart.has_sender_evidence = true;
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
            "path-scoped sender evidence is enough for Service failover only when no live Service owner remains"
        );
        assert_eq!(
            selected.admission.role,
            PathRuntimeRole::Service,
            "failover owner bytes are Service OwnerData, not optional Subflow exploration"
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
    fn sender_evidence_service_failover_waits_behind_live_ordered_owner_debt() {
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
            "sender-evidenced Service failover can only own future bytes after the live lower owner frontier is clear"
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
            true,
        )
        .expect("repair should remain dispatchable on the proven alternate");

        assert_eq!(
            selected.key, proven_alternate.key,
            "repair must not treat proof-only validation as bulk-capable just because it has lower ETA"
        );
    }

    #[test]
    fn repair_can_use_proof_only_path_when_no_proven_repair_path_exists() {
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
            true,
        )
        .expect("repair may fall back to the only non-owner path");

        assert_eq!(
            selected.key, proof_only_udp.key,
            "proof-only validation remains a bounded fallback when no proven repair path exists"
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
    fn quic_ack_data_seen_path_keeps_bulk_rate_proven_service_owner() {
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
            selected.target.key, active.key,
            "ACK-data-seen path must not receive ordered owner bytes until it has bulk-rate proof"
        );
        assert_eq!(
            selected.admission.role,
            PathRuntimeRole::Service,
            "ACK-data-only evidence is not a Subflow owner state"
        );
    }

    #[test]
    fn same_family_bulk_rate_subflow_admission_is_per_decision_not_startup_credit() {
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
            commit.owner_credit_bytes, payload_bytes,
            "measured Subflow decisions use the assigned owner range, not startup sampling credit"
        );

        let mut subflow_set = FlowSubflowSet::new(
            0,
            commit.service,
            commit.owner_credit_bytes,
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
            },
        };
        let latency_second = ResponseDataDispatchPlan {
            primary: ResponseDataDispatchTarget::Switchable {
                binding,
                target,
                role: PathRuntimeRole::Service,
                subflow_set_commit: None,
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
    fn proof_only_candidate_admission_is_probe_only() {
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

        assert_eq!(admission.decision, PathAdmissionDecision::ProbeOnly);
        assert_eq!(admission.role, PathRuntimeRole::Probe);
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
    async fn response_planning_keeps_service_before_app_limited_subflow_candidate() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let (active_commands, mut active_rx) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new_with_limits(
            SessionId(88),
            UnderlayProtocol::Udp,
            PathId(0),
            active_commands,
            FlowLane::Throughput,
            mux_limits,
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
        let (optional_commands, _optional_rx) = reliable_path_command_channels(8);
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
        let plan = plan_response_data_dispatch(&stream, FlowLane::Throughput, 0, payload_bytes)
            .expect("feedable Service should be dispatchable");

        assert_eq!(
            plan.primary_key(),
            Some(service),
            "app-limited ACK-data evidence must not displace the Service owner"
        );
        assert_eq!(
            plan.primary_role(),
            PathRuntimeRole::Service,
            "Subflow OwnerData requires bulk-rate proof"
        );

        let frame = Frame::StreamData {
            stream_id: StreamId(88),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![9_u8; payload_bytes]),
        };
        let outcome = emit_planned_response_data_frame(&stream, plan, frame, FlowLane::Throughput)
            .await
            .expect("Service OwnerData should emit");

        assert_eq!(outcome.selected_path, Some(service));
        assert_eq!(
            binding.ordered_data_owner(),
            Some(service),
            "Service OwnerData keeps the Service owner hint"
        );
        assert!(
            try_recv_reliable_path_command(&mut active_rx).is_some(),
            "Service quantum must be emitted on the Service path"
        );

        let followup_plan = plan_response_data_dispatch(
            &stream,
            FlowLane::Throughput,
            payload_bytes as u64,
            payload_bytes,
        )
        .expect("plain unacked Service OwnerData must not block the next Service feed");
        assert_eq!(
            followup_plan.primary_role(),
            PathRuntimeRole::Service,
            "normal Service tail remains on Service instead of creating Subflow ordered debt"
        );
        assert_eq!(
            followup_plan.primary_key(),
            Some(service),
            "the Service path remains responsible for the ordered tail"
        );
    }

    #[tokio::test]
    async fn normal_repair_cache_retention_does_not_create_owner_debt_pressure() {
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
        let active_target = binding
            .sender_path_targets(FlowLane::Throughput, payload_bytes)
            .into_iter()
            .find(|target| target.key == active_key)
            .expect("active target should be visible to sender planning");
        let owner_debt_pressure = response_ordered_owner_debt_pressure_bytes(
            &active_target,
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
        );
        let mut send_stream = ReliableSendStream::new(StreamId(7), mux_limits);
        let mut retained_unacked_bytes = owner_debt_pressure.saturating_add(payload_bytes);
        while retained_unacked_bytes > 0 {
            let chunk = retained_unacked_bytes.min(payload_bytes);
            let _unacked = send_stream
                .send_data(Bytes::from(vec![1_u8; chunk]), StreamFlags::NONE)
                .expect("seed normal retained unacked OwnerData above pressure threshold");
            retained_unacked_bytes -= chunk;
        }
        assert!(send_stream.repair_bytes() > owner_debt_pressure);
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
            .expect("normal repair-cache retention must not block measured Subflow OwnerData");

        assert_eq!(dispatch.selected_path, Some(alternate_key));
        assert_eq!(
            binding.ordered_data_owner(),
            Some(active_key),
            "Subflow OwnerData must not rewrite the Service owner hint"
        );
        assert!(
            try_recv_reliable_path_command(&mut active_rx).is_none(),
            "normal repair-cache retention is not an owner-debt reason to pin data to Service"
        );
        assert!(matches!(
            recv_reliable_path_command(&mut alternate_rx).await,
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
        ));
    }

    #[tokio::test]
    async fn response_owner_data_waits_under_ordered_owner_debt() {
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
        let active_target = binding
            .sender_path_targets(FlowLane::Throughput, payload_bytes)
            .into_iter()
            .find(|target| target.key == active_key)
            .expect("active target should be visible to sender planning");
        let owner_debt_pressure = response_ordered_owner_debt_pressure_bytes(
            &active_target,
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
        );
        let mut send_stream = ReliableSendStream::new(StreamId(7), mux_limits);
        let mut remaining_owner_debt = owner_debt_pressure.saturating_add(payload_bytes);
        while remaining_owner_debt > 0 {
            let chunk = remaining_owner_debt.min(payload_bytes);
            let _unacked = send_stream
                .send_data(Bytes::from(vec![1_u8; chunk]), StreamFlags::NONE)
                .expect("seed unacked ordered-owner scheduling debt above pressure threshold");
            remaining_owner_debt -= chunk;
        }
        assert!(send_stream.repair_bytes() > owner_debt_pressure);
        while let Some(_setup_command) = try_recv_reliable_path_command(&mut alternate_rx) {}

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

        assert!(
            matches!(dispatch, Err(RuntimeError::SenderServiceBlocked)),
            "ordered-owner scheduling debt must backpressure later OwnerData until the frontier is safe"
        );
        assert_eq!(binding.ordered_data_owner(), Some(active_key));
        assert!(try_recv_reliable_path_command(&mut active_rx).is_none());
        assert!(
            try_recv_reliable_path_command(&mut alternate_rx).is_none(),
            "ordered-owner scheduling debt must not move later OwnerData onto another Subflow"
        );
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
        let mut target = response_target(
            0,
            UnderlayProtocol::Tcp,
            5.0,
            8 * 1024 * 1024,
            16 * 1024 * 1024,
            true,
        );
        target.snapshot.product_progress_rate_bps = Some(10_000_000_000.0);
        let lower_flights = vec![CarrierPathFlightDebt {
            key: target.key,
            bytes: 8 * 1024 * 1024,
        }];

        let selected = choose_response_sender_data_target(
            std::slice::from_ref(&target),
            FlowLane::Throughput,
            64 * 1024,
            MuxLimits::default(),
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
    fn response_ordinary_bulk_uses_lower_eta_when_frontier_is_clear() {
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
        .expect("lower ETA path should be selected");

        assert_eq!(selected.key, lower_eta_alternate.key);
    }

    #[test]
    fn lower_eta_same_family_path_is_subflow_not_service_owner() {
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
        .expect("lower ETA same-family path should be eligible as a Subflow");

        assert_eq!(selected.target.key, lower_eta_subflow.key);
        assert_eq!(
            selected.admission.role,
            PathRuntimeRole::Subflow,
            "non-active same-family OwnerData must not rewrite the Service owner solely because it has the lowest ETA"
        );
    }

    #[test]
    fn ordered_owner_outside_dispatchable_set_still_anchors_service_role() {
        let (service_commands, _service_receivers) = reliable_path_command_channels(1);
        let mut service_snapshot =
            PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 50.0, 500_000_000.0);
        service_snapshot.inflight_limit_bytes = 16 * 1024 * 1024;
        service_snapshot.confidence = 1.0;
        let service = ResponseSenderPathTarget {
            key: CarrierPathKey {
                underlay: UnderlayProtocol::Tcp,
                path_id: PathId(1),
            },
            commands: service_commands,
            snapshot: service_snapshot,
            eta_ms: 50.0,
            is_active: true,
            has_sender_evidence: true,
            has_bulk_rate_evidence: true,
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
        .expect("bulk-rate-proven alternate should remain eligible as a Subflow");

        assert_eq!(selected.target.key, lower_eta_subflow.key);
        assert_eq!(
            selected.admission.role,
            PathRuntimeRole::Subflow,
            "a dispatchable alternate must not become Service merely because the current ordered owner is temporarily not dispatchable"
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

        let lead =
            choose_response_admissible_lead(&candidates, mux_limits, payload_bytes, &[], false)
                .expect(
                    "active Service must remain a lead candidate when optional Subflow is blocked",
                );

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

        let lead =
            choose_response_admissible_lead(&candidates, mux_limits, payload_bytes, &[], false)
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
        measured_subflow.snapshot.app_limited = true;
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
    fn measured_same_family_alternate_is_subflow_when_service_is_not_feedable() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut service =
            response_target(0, UnderlayProtocol::Tcp, 25.0, 0, 16 * 1024 * 1024, true);
        let service_envelope =
            bulk_active_service_product_envelope_bytes(service.snapshot, payload_bytes, mux_limits);
        service.snapshot.product_queue_bytes = service_envelope;
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
    fn saturated_service_does_not_admit_unproven_subflow_owner() {
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

        assert!(
            selected.is_none()
                || selected
                    .as_ref()
                    .is_some_and(|selected| selected.target.key != startup_subflow.key),
            "sender-evidence-only paths must not receive OwnerData even when Service is saturated"
        );
    }

    #[test]
    fn proof_only_startup_subflow_waits_behind_live_service_tail() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut service = response_target(
            0,
            UnderlayProtocol::Udp,
            25.0,
            mux_limits.max_path_flight_bytes as u64,
            16 * 1024 * 1024,
            true,
        );
        service.snapshot.product_progress_rate_bps = Some(180_000_000.0);
        service.has_sender_evidence = true;
        service.has_bulk_rate_evidence = true;
        let mut startup_subflow =
            response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
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
        );

        assert!(
            selected.is_none()
                || selected.as_ref().is_some_and(|selected| {
                    selected.target.key != startup_subflow.key
                        || selected.target.has_bulk_rate_evidence
                }),
            "proof-only Subflow OwnerData must wait behind a live lower Service tail"
        );
    }

    #[test]
    fn response_owner_debt_pressure_stops_later_service_owner_feed() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let owner = response_target(0, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, true);
        let alternate = response_target(1, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, false);
        let owner_key = owner.key;
        let owner_debt_pressure = response_ordered_owner_debt_pressure_bytes(
            &owner,
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
        );

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[owner, alternate],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(owner_key),
            owner_debt_pressure.saturating_add(payload_bytes),
            None,
        );

        assert!(
            selected.is_none(),
            "ordered-owner scheduling debt must stop later Service OwnerData instead of growing the receive hole"
        );
    }

    #[test]
    fn response_owner_debt_pressure_waits_when_lower_owner_queue_is_full() {
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

        let owner_debt_pressure = response_ordered_owner_debt_pressure_bytes(
            &owner,
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
        );

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[owner.clone(), alternate],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(owner.key),
            owner_debt_pressure.saturating_add(payload_bytes),
            None,
        );
        assert!(
            selected.is_none(),
            "when the lower owner is full, sender must wait instead of expanding the ordered receive hole on a Subflow"
        );
    }

    #[test]
    fn response_owner_debt_pressure_waits_when_lower_owner_is_over_budget() {
        let mux_limits = MuxLimits {
            max_path_flight_bytes: 64 * 1024 * 1024,
            max_reorder_bytes: 64 * 1024 * 1024,
            ..MuxLimits::default()
        };
        let payload_bytes = 64 * 1024;
        let mut owner = response_target(0, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, true);
        owner.snapshot.product_progress_rate_bps = Some(10_000_000.0);
        owner.snapshot.product_bytes_in_flight = 8 * 1024 * 1024;
        owner.snapshot.pacing_rate_bps = 2_000_000_000.0;
        let alternate = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
        let owner_debt_pressure = response_ordered_owner_debt_pressure_bytes(
            &owner,
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
        );

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[owner.clone(), alternate],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(owner.key),
            owner_debt_pressure.saturating_add(payload_bytes),
            None,
        );
        assert!(
            selected.is_none(),
            "an over-budget lower owner should create backpressure, not later-offset Subflow ownership"
        );
    }

    #[test]
    fn response_owner_debt_pressure_blocks_cross_underlay_when_owner_queue_is_full() {
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

        let owner_debt_pressure = response_ordered_owner_debt_pressure_bytes(
            &owner,
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
        );

        assert!(
            select_response_sender_data_target_with_ordered_debt_and_epoch(
                &[owner.clone(), alternate],
                FlowLane::Throughput,
                payload_bytes,
                mux_limits,
                &[],
                Some(owner.key),
                owner_debt_pressure.saturating_add(payload_bytes),
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
        let owner = response_target(1, UnderlayProtocol::Tcp, 50.0, 4 * 1024 * 1024, 0, true);
        let alternate = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[owner.clone(), alternate],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(owner.key),
            0,
            None,
        );

        assert!(
            selected.is_none(),
            "a cross-underlay alternate must not own later bytes while the current Service owner is backpressured by unresolved contiguous tail"
        );
    }

    #[test]
    fn response_owner_debt_pressure_blocks_proof_only_same_family_subflow() {
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

        let owner_debt_pressure = response_ordered_owner_debt_pressure_bytes(
            &owner,
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
        );

        assert!(
            select_response_sender_data_target_with_ordered_debt_and_epoch(
                &[owner.clone(), alternate],
                FlowLane::Throughput,
                payload_bytes,
                mux_limits,
                &[],
                Some(owner.key),
                owner_debt_pressure.saturating_add(payload_bytes),
                None,
            )
            .is_none(),
            "proof-only paths must stay Probe/Standby while older owner debt is unresolved"
        );
    }

    #[test]
    fn response_small_owner_debt_does_not_pin_bulk_to_current_service_path() {
        let owner = response_target(0, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, true);
        let lower_eta_alternate =
            response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);

        let selected = choose_response_sender_data_target_with_ordered_debt(
            &[owner.clone(), lower_eta_alternate.clone()],
            FlowLane::Throughput,
            64 * 1024,
            MuxLimits::default(),
            &[],
            Some(owner.key),
            64 * 1024,
        )
        .expect("small normal owner debt should not block better bulk service selection");

        assert_eq!(selected.key, lower_eta_alternate.key);
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
    fn proof_only_active_fallback_cannot_extend_unresolved_ordered_debt() {
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

        assert!(
            selected.is_none(),
            "a reachability/proof-only fallback path must repair or wait under unresolved ordered debt; it must not become Service OwnerData for later offsets"
        );
    }

    #[test]
    fn frontier_clear_same_family_sender_evidence_remains_service_not_subflow() {
        let owner = response_target(0, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, true);
        let mut lower_eta_alternate =
            response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
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
            selected.target.key, owner.key,
            "sender evidence alone must not create Subflow owner credit at a clear same-family frontier"
        );
        assert_eq!(
            selected.admission.role,
            PathRuntimeRole::Service,
            "unproven alternates remain Probe/Standby instead of becoming Subflow owners"
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
    fn owner_debt_pressure_keeps_cross_underlay_candidate_that_owns_lower_flight() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let service = response_target(0, UnderlayProtocol::Tcp, 80.0, 0, 16 * 1024 * 1024, true);
        let candidate = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
        let lower_flights = vec![CarrierPathFlightDebt {
            key: candidate.key,
            bytes: payload_bytes as u64,
        }];
        let owner_debt_pressure = response_ordered_owner_debt_pressure_bytes(
            &service,
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
        );

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), candidate.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &lower_flights,
            Some(service.key),
            owner_debt_pressure.saturating_add(payload_bytes),
            None,
        )
        .expect("candidate owning the lower flight should survive debt-pressure filtering");

        assert_eq!(
            selected.target.key, candidate.key,
            "owner-debt pressure must filter by candidate ordering safety, not by carrier family alone"
        );
    }
}
