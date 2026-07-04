use super::bulk_admission::{
    BulkAdmissionCheck, BulkAdmissionRole, bulk_additional_admission_role,
    bulk_candidate_admission_suppression_with_ordering_debt,
};
use super::*;

// Ownership boundary:
// Sender services own product work before it reaches carrier command queues.
// Client relay sending and server response dispatch both use this module for
// queueing, product flight ledgers, stream-ACK release, and diagnostics. Final
// TCP/UDP emission still happens through carrier command senders.

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
    pub(super) path_key: RelayPathKey,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum ClientQueuedDispatch {
    Data {
        path_key: RelayPathKey,
        payload_bytes: usize,
    },
    Repair {
        path_key: RelayPathKey,
        payload_bytes: usize,
    },
}

#[derive(Debug)]
pub(super) struct RelaySenderService {
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    stream_id: StreamId,
    flights: RelayPathFlightLedger,
    ordered_data_owner: Option<RelayPathKey>,
    next_send_index: usize,
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
        debug_assert!(cause.is_repair());
        let payload_bytes = reliable_stream_frame_payload_bytes(&frame);
        self.push_work(
            ReliableRelayQueuedWorkLane::Repair,
            ReliableRelayQueuedWorkKind::Repair { frame, cause },
            None,
            false,
            payload_bytes,
        )
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
        } else if let Some(work) = self.repair.front() {
            Some((ReliableRelayQueuedWorkLane::Repair, work))
        } else {
            self.data
                .front()
                .map(|work| (ReliableRelayQueuedWorkLane::Data, work))
                .or_else(|| {
                    self.final_control
                        .front()
                        .map(|work| (ReliableRelayQueuedWorkLane::Control, work))
                })
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
        } else if let Some(work) = self.repair.pop_front() {
            (ReliableRelayQueuedWorkLane::Repair, work)
        } else if let Some(work) = self.data.pop_front() {
            (ReliableRelayQueuedWorkLane::Data, work)
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
    mux_limits: MuxLimits,
    queue_limit: usize,
) -> bool {
    sender_queue.bytes() < queue_limit
        && sender_queue.data_bytes() < send_stream.send_credit_bytes()
        && send_stream
            .repair_bytes()
            .saturating_add(sender_queue.data_bytes())
            < mux_limits.max_repair_bytes
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
    mux_limits: MuxLimits,
    queue_limit: usize,
    buffer_len: usize,
) -> usize {
    queue_limit
        .saturating_sub(sender_queue.bytes())
        .min(
            mux_limits
                .max_repair_bytes
                .saturating_sub(send_stream.repair_bytes())
                .saturating_sub(sender_queue.data_bytes()),
        )
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
    ordinary_data_dispatched_bytes: u64,
    extra_repair_queued_bytes: u64,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResponseCarrierEmitMode {
    Classified,
    StreamOrdered,
}

impl ResponseCarrierEmitMode {
    fn effective_lane(self, frame: &Frame, lane: FlowLane) -> FlowLane {
        match self {
            Self::Classified => reliable_path_effective_frame_lane(frame, lane),
            Self::StreamOrdered => lane,
        }
    }
}

#[derive(Clone)]
enum ResponseDataDispatchTarget {
    Fixed(Arc<FixedReliablePathOutput>),
    Switchable {
        binding: Arc<ResponseStreamBinding>,
        target: ResponseSenderPathTarget,
        bulk_discovery: bool,
    },
}

#[derive(Clone)]
struct ResponseDataDispatchPlan {
    primary: ResponseDataDispatchTarget,
    duplicate_discovery: smallvec::SmallVec<[ResponseSenderPathTarget; 2]>,
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
    fn primary_is_bulk_discovery(&self) -> bool {
        match &self.primary {
            ResponseDataDispatchTarget::Fixed(_) => false,
            ResponseDataDispatchTarget::Switchable { bulk_discovery, .. } => *bulk_discovery,
        }
    }
}

fn response_bulk_admission_role(
    lead_key: CarrierPathKey,
    candidate: CarrierPathKey,
    lower_owner: Option<CarrierPathKey>,
    ordering_debt: u64,
) -> BulkAdmissionRole {
    if lower_owner == Some(candidate) || (candidate == lead_key && ordering_debt == 0) {
        BulkAdmissionRole::ActiveDataPath
    } else if let Some(owner) = lower_owner {
        bulk_additional_admission_role(owner.underlay, candidate.underlay)
    } else {
        bulk_additional_admission_role(lead_key.underlay, candidate.underlay)
    }
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
                && !target.has_ack_data_evidence
    )
}

fn response_target_can_own_unique_bulk_data(
    target: &ResponseSenderPathTarget,
    candidates: &[&ResponseSenderPathTarget],
    lower_owner: Option<CarrierPathKey>,
    ordinary_data_dispatched_bytes: u64,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    performance: MppPerformanceConfig,
) -> bool {
    if target.has_bulk_rate_evidence {
        return true;
    }
    if response_target_can_own_quic_ack_data_trial(
        target,
        candidates,
        lower_owner,
        ordinary_data_dispatched_bytes,
        payload_bytes,
        mux_limits,
        performance,
    ) {
        return true;
    }
    target.is_active
        && !candidates
            .iter()
            .any(|candidate| candidate.key != target.key && candidate.has_bulk_rate_evidence)
}

fn response_target_can_own_quic_ack_data_trial(
    target: &ResponseSenderPathTarget,
    candidates: &[&ResponseSenderPathTarget],
    _lower_owner: Option<CarrierPathKey>,
    ordinary_data_dispatched_bytes: u64,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    performance: MppPerformanceConfig,
) -> bool {
    target.key.underlay == UnderlayProtocol::Udp
        && !target.is_active
        && target.has_ack_data_evidence
        && !target.has_bulk_rate_evidence
        && !candidates
            .iter()
            .any(|candidate| candidate.key != target.key && candidate.has_bulk_rate_evidence)
        && response_target_has_quic_unique_trial_credit(
            target,
            ordinary_data_dispatched_bytes,
            payload_bytes,
            mux_limits,
            performance,
        )
}

fn response_target_is_quic_ack_data_unique_trial(
    target: &ResponseSenderPathTarget,
    targets: &[ResponseSenderPathTarget],
    lower_owner: Option<CarrierPathKey>,
    ordinary_data_dispatched_bytes: u64,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    performance: MppPerformanceConfig,
) -> bool {
    let candidates = targets.iter().collect::<Vec<_>>();
    response_target_can_own_quic_ack_data_trial(
        target,
        &candidates,
        lower_owner,
        ordinary_data_dispatched_bytes,
        payload_bytes,
        mux_limits,
        performance,
    )
}

fn response_target_is_quic_ack_data_exploration(
    target: &ResponseSenderPathTarget,
    lower_owner: Option<CarrierPathKey>,
    ordinary_data_dispatched_bytes: u64,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    performance: MppPerformanceConfig,
) -> bool {
    target.key.underlay == UnderlayProtocol::Udp
        && Some(target.key) != lower_owner
        && lower_owner.is_some()
        && !target.is_active
        && target.has_ack_data_evidence
        && !target.has_bulk_rate_evidence
        && response_target_has_quic_unique_trial_credit(
            target,
            ordinary_data_dispatched_bytes,
            payload_bytes,
            mux_limits,
            performance,
        )
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
    performance: MppPerformanceConfig,
    ordinary_data_dispatched_bytes: u64,
) -> Option<ResponseBulkLead> {
    let lower_owner = response_oldest_lower_flight_owner(lower_flights);
    if let Some(owner) = lower_owner {
        let owner_target = candidate_targets
            .iter()
            .copied()
            .find(|target| target.key == owner)?;
        let owner_cross_path_debt = response_ordering_debt_bytes(lower_flights, owner_target.key);
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

    candidate_targets
        .iter()
        .copied()
        .filter(|target| {
            response_target_can_own_unique_bulk_data(
                target,
                candidate_targets,
                lower_owner,
                ordinary_data_dispatched_bytes,
                payload_bytes,
                mux_limits,
                performance,
            ) && response_active_lead_suppression(target, mux_limits, payload_bytes, 0).is_none()
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
        })
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

fn choose_response_sender_target(
    targets: &[ResponseSenderPathTarget],
    lane: FlowLane,
    frame: &Frame,
    emit_mode: ResponseCarrierEmitMode,
    mux_limits: MuxLimits,
    performance: MppPerformanceConfig,
    lower_flights: &[CarrierPathFlightDebt],
    avoid_keys: &[CarrierPathKey],
    ordinary_data_dispatched_bytes: u64,
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
        performance,
        ordinary_data_dispatched_bytes,
    )?;
    let selected = candidate_targets
        .iter()
        .copied()
        .filter(|target| {
            let ordering_debt = response_ordering_debt_bytes(lower_flights, target.key);
            if !response_target_can_own_unique_bulk_data(
                target,
                &candidate_targets,
                lower_owner,
                ordinary_data_dispatched_bytes,
                payload_bytes,
                mux_limits,
                performance,
            ) || response_unique_quic_data_would_expand_ordering_debt(
                lower_owner,
                target,
                ordering_debt,
            ) {
                return false;
            }
            let role =
                response_bulk_admission_role(lead.key, target.key, lower_owner, ordering_debt);
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

fn choose_response_sender_data_target(
    targets: &[ResponseSenderPathTarget],
    lane: FlowLane,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    performance: MppPerformanceConfig,
    ordinary_data_dispatched_bytes: u64,
    lower_flights: &[CarrierPathFlightDebt],
    ordered_data_owner: Option<CarrierPathKey>,
) -> Option<ResponseSenderPathTarget> {
    if targets.is_empty() {
        return None;
    }
    let capacity_targets = targets
        .iter()
        .filter(|target| {
            target.commands.can_enqueue_lane_now(lane)
                && response_target_has_emission_credit(target, lane, payload_bytes, mux_limits)
        })
        .cloned()
        .collect::<Vec<_>>();
    if capacity_targets.is_empty() {
        return None;
    }
    if !relay_lane_is_bulk(lane) {
        return capacity_targets.into_iter().min_by(|left, right| {
            left.eta_ms
                .total_cmp(&right.eta_ms)
                .then_with(|| carrier_path_key_order(left.key, right.key))
        });
    }

    let lower_owner = response_oldest_lower_flight_owner(lower_flights);
    let proven_targets = capacity_targets
        .iter()
        .filter(|target| target.is_active || target.has_sender_evidence)
        .collect::<Vec<_>>();
    let candidate_targets = if proven_targets.is_empty() {
        capacity_targets.iter().collect::<Vec<_>>()
    } else {
        proven_targets
    };
    if lower_owner.is_some()
        && let Some(trial) = candidate_targets
            .iter()
            .copied()
            .filter(|target| {
                response_target_is_quic_ack_data_exploration(
                    target,
                    lower_owner,
                    ordinary_data_dispatched_bytes,
                    payload_bytes,
                    mux_limits,
                    performance,
                )
            })
            .min_by(|left, right| {
                left.eta_ms
                    .total_cmp(&right.eta_ms)
                    .then_with(|| carrier_path_key_order(left.key, right.key))
            })
    {
        return Some(trial.clone());
    }
    let lead = choose_response_admissible_lead(
        &candidate_targets,
        mux_limits,
        payload_bytes,
        lower_flights,
        performance,
        ordinary_data_dispatched_bytes,
    )?;
    let admitted = candidate_targets
        .iter()
        .copied()
        .filter(|target| {
            let ordering_debt = response_ordering_debt_bytes(lower_flights, target.key);
            if !response_target_can_own_unique_bulk_data(
                target,
                &candidate_targets,
                lower_owner,
                ordinary_data_dispatched_bytes,
                payload_bytes,
                mux_limits,
                performance,
            ) || response_unique_quic_data_would_expand_ordering_debt(
                lower_owner,
                target,
                ordering_debt,
            ) {
                return false;
            }
            let role =
                response_bulk_admission_role(lead.key, target.key, lower_owner, ordering_debt);
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
        .collect::<Vec<_>>();
    if lower_owner.is_none()
        && let Some(discovery) = admitted
            .iter()
            .copied()
            .filter(|target| {
                response_target_needs_same_underlay_bulk_discovery(
                    target,
                    lead.key,
                    payload_bytes,
                    mux_limits,
                    performance,
                )
            })
            .min_by(|left, right| {
                left.eta_ms
                    .total_cmp(&right.eta_ms)
                    .then_with(|| carrier_path_key_order(left.key, right.key))
            })
    {
        return Some(discovery.clone());
    }
    let best = admitted.iter().copied().min_by(|left, right| {
        left.eta_ms
            .total_cmp(&right.eta_ms)
            .then_with(|| carrier_path_key_order(left.key, right.key))
    })?;
    if lower_owner.is_none()
        && let Some(lead_key) = ordered_data_owner
        && let Some(lead_target) = admitted
            .iter()
            .copied()
            .find(|target| target.key == lead_key)
        && response_target_within_adaptive_lead_hysteresis(lead_target, best, payload_bytes)
    {
        return Some(lead_target.clone());
    }
    Some(best.clone())
}

fn response_target_needs_same_underlay_bulk_discovery(
    target: &ResponseSenderPathTarget,
    lead_key: CarrierPathKey,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    performance: MppPerformanceConfig,
) -> bool {
    if target.is_active
        || !target.has_sender_evidence
        || target.has_bulk_rate_evidence
        || target.key.underlay == UnderlayProtocol::Udp
        || target.key.underlay != lead_key.underlay
        || target.key == lead_key
    {
        return false;
    }
    response_target_has_discovery_credit(target, payload_bytes, mux_limits, performance)
}

fn response_target_needs_quic_duplicate_bulk_discovery(
    target: &ResponseSenderPathTarget,
    primary_key: CarrierPathKey,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    performance: MppPerformanceConfig,
) -> bool {
    target.key.underlay == UnderlayProtocol::Udp
        && target.key != primary_key
        && !target.is_active
        && !target.has_bulk_rate_evidence
        && target.commands.can_enqueue_lane_now(FlowLane::Throughput)
        && response_target_has_emission_credit(
            target,
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
        )
        && response_target_has_discovery_credit(target, payload_bytes, mux_limits, performance)
}

fn choose_quic_duplicate_discovery_targets(
    targets: &[ResponseSenderPathTarget],
    primary_key: CarrierPathKey,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    performance: MppPerformanceConfig,
) -> smallvec::SmallVec<[ResponseSenderPathTarget; 2]> {
    let mut selected = targets
        .iter()
        .filter(|target| {
            response_target_needs_quic_duplicate_bulk_discovery(
                target,
                primary_key,
                payload_bytes,
                mux_limits,
                performance,
            )
        })
        .cloned()
        .collect::<Vec<_>>();
    selected.sort_by(|left, right| {
        left.eta_ms
            .total_cmp(&right.eta_ms)
            .then_with(|| carrier_path_key_order(left.key, right.key))
    });
    selected
        .into_iter()
        .take(2)
        .collect::<smallvec::SmallVec<[_; 2]>>()
}

fn response_target_has_discovery_credit(
    target: &ResponseSenderPathTarget,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    performance: MppPerformanceConfig,
) -> bool {
    let discovery_credit =
        response_bulk_discovery_credit_bytes(payload_bytes, mux_limits, performance) as u64;
    target.bulk_discovery_sent_bytes < discovery_credit
        && response_target_discovery_debt_bytes(target) < discovery_credit
}

fn response_target_has_quic_unique_trial_credit(
    target: &ResponseSenderPathTarget,
    ordinary_data_dispatched_bytes: u64,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    performance: MppPerformanceConfig,
) -> bool {
    let discovery_credit =
        response_bulk_discovery_credit_bytes(payload_bytes, mux_limits, performance) as u64;
    let min_pipe_trial_credit = reliable_bulk_carrier_feed_quantum_bytes(mux_limits)
        .saturating_mul(BBR_MIN_PIPE_CWND_PACKETS) as u64;
    let product_envelope = mux_limits
        .max_path_flight_bytes
        .min(mux_limits.max_repair_bytes)
        .min(mux_limits.max_reorder_bytes)
        .min(usize::try_from(mux_limits.max_stream_window_bytes).unwrap_or(usize::MAX))
        .max(payload_bytes)
        .max(min_pipe_trial_credit as usize)
        .max(1) as u64;
    let carrier_trial_credit = target
        .snapshot
        .inflight_limit_bytes
        .min(product_envelope)
        .max(min_pipe_trial_credit);
    let earned_trial_credit = response_extra_traffic_budget_bytes(
        ordinary_data_dispatched_bytes,
        performance,
        mux_limits,
    )
    .max(discovery_credit.saturating_mul(2));
    target.bulk_discovery_sent_bytes < earned_trial_credit
        && response_target_discovery_debt_bytes(target) < carrier_trial_credit
}

fn response_bulk_discovery_credit_bytes(
    payload_bytes: usize,
    mux_limits: MuxLimits,
    performance: MppPerformanceConfig,
) -> usize {
    let hint = performance.extra_traffic_hint_percent as usize;
    if hint == 0 {
        return 0;
    }
    let service_quantum = reliable_bulk_carrier_feed_quantum_bytes(mux_limits)
        .max(payload_bytes)
        .max(1);
    let percent_budget = service_quantum.saturating_mul(hint) / 100;
    let startup_floor = payload_bytes
        .max(PATH_OPEN_SCORE_BYTES)
        .min(service_quantum)
        .max(1);
    percent_budget.max(startup_floor)
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

fn response_extra_traffic_budget_bytes(
    ordinary_data_dispatched_bytes: u64,
    performance: MppPerformanceConfig,
    mux_limits: MuxLimits,
) -> u64 {
    let hint = performance.extra_traffic_hint_percent as u64;
    if hint == 0 {
        return 0;
    }
    let startup_floor = response_extra_traffic_startup_floor_bytes(mux_limits) as u64;
    let earned = ordinary_data_dispatched_bytes.saturating_mul(hint) / 100;
    startup_floor.saturating_add(earned)
}

fn response_target_discovery_debt_bytes(target: &ResponseSenderPathTarget) -> u64 {
    target
        .snapshot
        .product_bytes_in_flight
        .saturating_add(target.snapshot.bytes_in_flight)
        .saturating_add(target.snapshot.queue_bytes)
        .saturating_add(target.snapshot.product_queue_bytes)
        .saturating_add(target.commands.pending_bytes())
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
    if relay_lane_is_bulk(lane) && target.key.underlay == UnderlayProtocol::Udp {
        return response_quic_carrier_feed_credit_bytes(target, payload_bytes, mux_limits);
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

fn plan_response_data_dispatch(
    stream: &ReliablePathStream,
    relay_lane: FlowLane,
    next_offset: u64,
    payload_bytes: usize,
    performance: MppPerformanceConfig,
    ordinary_data_dispatched_bytes: u64,
) -> Result<ResponseDataDispatchPlan, RuntimeError> {
    match &stream.output {
        ReliablePathStreamOutput::Fixed(fixed) => {
            let lane =
                reliable_work_lane_to_carrier_lane(ReliableRelayQueuedWorkLane::Data, relay_lane);
            if fixed.commands().can_enqueue_lane_now(lane) {
                Ok(ResponseDataDispatchPlan {
                    primary: ResponseDataDispatchTarget::Fixed(fixed.clone()),
                    duplicate_discovery: smallvec::SmallVec::new(),
                })
            } else {
                Err(RuntimeError::SenderServiceBlocked)
            }
        }
        ReliablePathStreamOutput::Switchable(binding) => {
            let lower_flights = binding.lower_flights_before_offset(next_offset);
            let targets = binding.sender_path_targets(relay_lane, payload_bytes);
            let ordered_data_owner = binding.ordered_data_owner();
            let Some(target) = choose_response_sender_data_target(
                &targets,
                relay_lane,
                payload_bytes,
                binding.mux_limits(),
                performance,
                ordinary_data_dispatched_bytes,
                &lower_flights,
                ordered_data_owner,
            ) else {
                return Err(RuntimeError::SenderServiceBlocked);
            };
            let bulk_discovery = ordered_data_owner.is_some_and(|lead_key| {
                response_target_needs_same_underlay_bulk_discovery(
                    &target,
                    lead_key,
                    payload_bytes,
                    binding.mux_limits(),
                    performance,
                )
            }) || response_target_is_quic_ack_data_unique_trial(
                &target,
                &targets,
                response_oldest_lower_flight_owner(&lower_flights),
                ordinary_data_dispatched_bytes,
                payload_bytes,
                binding.mux_limits(),
                performance,
            ) || response_target_is_quic_ack_data_exploration(
                &target,
                response_oldest_lower_flight_owner(&lower_flights),
                ordinary_data_dispatched_bytes,
                payload_bytes,
                binding.mux_limits(),
                performance,
            );
            let duplicate_discovery = choose_quic_duplicate_discovery_targets(
                &targets,
                target.key,
                payload_bytes,
                binding.mux_limits(),
                performance,
            );
            Ok(ResponseDataDispatchPlan {
                primary: ResponseDataDispatchTarget::Switchable {
                    binding: binding.clone(),
                    target,
                    bulk_discovery,
                },
                duplicate_discovery,
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
    performance: MppPerformanceConfig,
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
                performance,
                &lower_flights,
                &avoid_keys,
                0,
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
) -> Result<Option<CarrierPathKey>, RuntimeError> {
    let ResponseDataDispatchPlan {
        primary,
        duplicate_discovery,
    } = planned;
    match primary {
        ResponseDataDispatchTarget::Fixed(fixed) => {
            send_sender_service_frame_to_carrier(
                fixed.commands(),
                frame.clone(),
                lane,
                ResponseCarrierEmitMode::Classified,
            )
            .await?;
            fixed.record_flight(&frame, true);
            Ok(Some(fixed.key()))
        }
        ResponseDataDispatchTarget::Switchable {
            binding,
            target,
            bulk_discovery,
        } => {
            match send_sender_service_frame_to_carrier(
                &target.commands,
                frame.clone(),
                lane,
                ResponseCarrierEmitMode::Classified,
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
            binding.record_flight(target.key, &frame, true);
            if bulk_discovery {
                binding.record_bulk_discovery_bytes(
                    target.key,
                    reliable_stream_frame_payload_bytes(&frame),
                );
            }
            if !bulk_discovery {
                binding.set_ordered_data_owner(target.key);
            }
            let decision_reason = if bulk_discovery {
                "bulk_discovery"
            } else {
                "data"
            };
            record_server_sender_decision(
                binding.session_id(),
                stream.stream_id,
                target.key,
                &frame,
                lane,
                decision_reason,
            );
            for duplicate in duplicate_discovery {
                match send_sender_service_frame_to_carrier(
                    &duplicate.commands,
                    frame.clone(),
                    lane,
                    ResponseCarrierEmitMode::Classified,
                )
                .await
                {
                    Ok(()) => {
                        binding.record_flight_with_ordering_owner(
                            duplicate.key,
                            &frame,
                            false,
                            false,
                        );
                        binding.record_bulk_discovery_bytes(
                            duplicate.key,
                            reliable_stream_frame_payload_bytes(&frame),
                        );
                        record_server_sender_decision(
                            binding.session_id(),
                            stream.stream_id,
                            duplicate.key,
                            &frame,
                            lane,
                            "duplicate_discovery",
                        );
                    }
                    Err(RuntimeError::SenderServiceBlocked) => {}
                    Err(_) => {
                        binding.detach(duplicate.key, &duplicate.commands);
                    }
                }
            }
            Ok(Some(target.key))
        }
    }
}

async fn emit_response_frame_from_sender_service(
    stream: &ReliablePathStream,
    frame: Frame,
    lane: FlowLane,
    emit_mode: ResponseCarrierEmitMode,
    reason: &'static str,
    performance: MppPerformanceConfig,
    repair: bool,
) -> Result<Option<CarrierPathKey>, RuntimeError> {
    match &stream.output {
        ReliablePathStreamOutput::Fixed(fixed) => {
            send_sender_service_frame_to_carrier(fixed.commands(), frame.clone(), lane, emit_mode)
                .await?;
            if matches!(frame, Frame::StreamData { .. }) {
                fixed.record_flight(&frame, !repair);
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
                    performance,
                    &lower_flights,
                    &avoid_keys,
                    0,
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
                            binding.record_flight(target.key, &frame, !repair);
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
        MppPerformanceConfig::default(),
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
            ordinary_data_dispatched_bytes: 0,
            extra_repair_queued_bytes: 0,
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

    pub(super) fn repair_extra_budget_remaining(&self, mux_limits: MuxLimits) -> usize {
        response_extra_traffic_budget_bytes(
            self.ordinary_data_dispatched_bytes,
            self.performance,
            mux_limits,
        )
        .saturating_sub(self.extra_repair_queued_bytes)
        .min(usize::MAX as u64) as usize
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
    pub(super) fn record_ordinary_data_dispatched_for_test(&mut self, bytes: usize) {
        self.ordinary_data_dispatched_bytes = self
            .ordinary_data_dispatched_bytes
            .saturating_add(bytes as u64);
    }

    pub(super) fn publish_queue_bytes(&self, path_stream: &ReliablePathStream) {
        path_stream.set_sender_queue_bytes(self.queue.bytes());
    }

    pub(super) fn queued_send_ready(&self) -> bool {
        self.queue.front().is_some()
    }

    pub(super) fn front_has_carrier_credit(
        &self,
        path_stream: &ReliablePathStream,
        send_stream: &ReliableSendStream,
        relay_lane: FlowLane,
        mux_limits: MuxLimits,
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
                    self.performance,
                    false,
                )
            }
            ReliableRelayQueuedWorkKind::Data(payload) => plan_response_data_dispatch(
                path_stream,
                queued.data_lane.unwrap_or(relay_lane),
                send_stream.next_offset(),
                response_dispatch_payload_bytes(
                    path_stream,
                    queued.data_lane.unwrap_or(relay_lane),
                    mux_limits,
                    payload.len(),
                ),
                self.performance,
                self.ordinary_data_dispatched_bytes,
            )
            .is_ok(),
            ReliableRelayQueuedWorkKind::Repair { frame, .. } => response_frame_has_carrier_credit(
                path_stream,
                frame,
                FlowLane::Latency,
                ResponseCarrierEmitMode::Classified,
                self.performance,
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

    pub(super) fn enqueue_repair_frame(
        &mut self,
        frame: Frame,
        mux_limits: MuxLimits,
    ) -> Option<u64> {
        let payload_bytes = reliable_stream_frame_payload_bytes(&frame);
        if payload_bytes > self.repair_extra_budget_remaining(mux_limits) {
            return None;
        }
        self.extra_repair_queued_bytes = self
            .extra_repair_queued_bytes
            .saturating_add(payload_bytes as u64);
        Some(self.queue.push_repair(frame))
    }

    pub(super) async fn dispatch_next(
        &mut self,
        path_stream: &ReliablePathStream,
        send_stream: &mut ReliableSendStream,
        relay_lane: FlowLane,
        mux_limits: MuxLimits,
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
                let planned = plan_response_data_dispatch(
                    path_stream,
                    data_lane,
                    send_stream.next_offset(),
                    dispatch_payload.len(),
                    self.performance,
                    self.ordinary_data_dispatched_bytes,
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
                    Ok(selected_path) => {
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
                            selected_path,
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
                    self.performance,
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
                self.performance,
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
                    self.performance,
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
        if queued_lane == ReliableRelayQueuedWorkLane::Data {
            self.ordinary_data_dispatched_bytes = self
                .ordinary_data_dispatched_bytes
                .saturating_add(committed.payload_bytes as u64);
        }
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
        Self {
            stream_id,
            flights: RelayPathFlightLedger::default(),
            ordered_data_owner: None,
            next_send_index: 0,
        }
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
                Ok(ClientQueuedDispatch::Data {
                    path_key: outcome.path_key,
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
                                Ok(ClientQueuedDispatch::Data {
                                    path_key: outcome.path_key,
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
                Ok(ClientQueuedDispatch::Repair {
                    path_key: outcome.path_key,
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
                                Ok(ClientQueuedDispatch::Repair {
                                    path_key: outcome.path_key,
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
        let payload_bytes = self.flights.record_frame(path_key, &sent_frame);
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
                        if !cause.is_repair() && self.ordered_data_owner.is_none() {
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
            avoid_keys,
            matches!(frame, Frame::StreamData { .. }) && !cause.is_repair(),
        )
        .ok_or(RuntimeError::ReliablePathSessionClosed)
    }

    fn choose_lowest_eta_relay_path(
        &self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        frame: &Frame,
        lane: FlowLane,
        avoid_keys: &[RelayPathKey],
        ordinary_stream_data: bool,
    ) -> Option<usize> {
        let payload_bytes = reliable_stream_frame_payload_bytes(frame);
        let has_active_path = remotes
            .paths
            .iter()
            .any(|path| path.placement == RelayPathPlacement::Active);
        let choose = |prefer_avoiding: bool| {
            remotes
                .paths
                .iter()
                .enumerate()
                .filter(|(_, path)| !prefer_avoiding || !avoid_keys.contains(&path.key()))
                .filter(|(_, path)| {
                    !ordinary_stream_data
                        || !has_active_path
                        || path.placement == RelayPathPlacement::Active
                })
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
        choose(true).or_else(|| choose(false)).or_else(|| {
            remotes
                .paths
                .iter()
                .enumerate()
                .filter(|(_, path)| {
                    !ordinary_stream_data
                        || !has_active_path
                        || path.placement == RelayPathPlacement::Active
                })
                .map(|(position, _)| position)
                .find(|position| !avoid_keys.contains(&remotes.paths[*position].key()))
                .or_else(|| {
                    remotes
                        .paths
                        .iter()
                        .position(|path| !avoid_keys.contains(&path.key()))
                })
                .or_else(|| remotes.paths.first().map(|_| 0))
        })
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
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "path_model",
                format_args!(
                    "stream_id={} path_underlay={:?} path_index={} released_bytes={} elapsed_ms={:.3} cause=stream_ack",
                    self.stream_id.0,
                    release.key.underlay,
                    release.key.index,
                    release.bytes,
                    release.elapsed.as_secs_f64() * 1000.0,
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
        if progress.should_send_ack(
            recv_stream,
            request.path,
            request.lane,
            context.mux_limits,
            request.force_max_data,
        ) {
            #[cfg(feature = "lab-diagnostics")]
            let ack_started = Instant::now();
            let ack_frame = recv_stream.ack_frame();
            #[cfg(feature = "lab-diagnostics")]
            lab_perf_record("mux.ack_frames", ack_started.elapsed(), 1);
            self.send_control_frame(context, remotes, ack_frame, RelaySendCause::RecvProgress)
                .await?;
            sent_any = true;
        }
        if progress.should_send_max_data(recv_stream, context.mux_limits, request.force_max_data) {
            self.send_control_frame(
                context,
                remotes,
                recv_stream.max_data_frame(),
                RelaySendCause::RecvProgress,
            )
            .await?;
            sent_any = true;
        }
        Ok(sent_any)
    }

    pub(super) fn enqueue_failed_path_gap_repairs(
        &self,
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
            adaptive_reliable_relay_repair_bytes(repair_path, lane, context.mux_limits);
        let repair_frames = send_stream.retransmission_frames_for_ranges(&ranges, repair_limit);
        if repair_frames.is_empty() {
            return false;
        }
        let mut queued = false;
        for frame in repair_frames {
            sender_queue.push_repair_with_cause(frame, RelaySendCause::PathFailureRepair);
            queued = true;
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "repair",
                format_args!(
                    "stream_id={} failed_underlay={:?} failed_index={} cause=path_failure queued=true",
                    self.stream_id.0, failed_key.underlay, failed_key.index,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SharedSecret;

    fn security() -> SecurityConfig {
        SecurityConfig::encrypted(
            SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
        )
    }

    #[test]
    fn stream_ack_releases_sender_service_flights_without_lowering_delivery_rate() {
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
        sender.flights.record_frame(key, &frame);

        let before = context.tcp_path_snapshot(0).expect("before snapshot");
        assert_eq!(before.bytes_in_flight, PATH_OPEN_SCORE_BYTES as u64);
        sender.release_normalized_acked_ranges(
            &context,
            &[OffsetRange::new(0, PATH_OPEN_SCORE_BYTES as u64).expect("ack range")],
        );
        let after = context.tcp_path_snapshot(0).expect("after snapshot");

        assert_eq!(after.bytes_in_flight, 0);
        assert_eq!(after.delivery_rate_bps, before.delivery_rate_bps);
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
            has_ack_data_evidence: true,
            has_bulk_rate_evidence: true,
            bulk_discovery_sent_bytes: 0,
        }
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
                .enqueue_repair_frame(
                    Frame::StreamData {
                        stream_id,
                        offset: 0,
                        flags: StreamFlags::NONE,
                        payload: repair_payload.clone(),
                    },
                    mux_limits,
                )
                .is_some(),
            "startup repair floor should be spendable once"
        );
        assert_eq!(sender.repair_extra_budget_remaining(mux_limits), 0);
        assert!(
            sender
                .enqueue_repair_frame(
                    Frame::StreamData {
                        stream_id,
                        offset: startup_floor as u64,
                        flags: StreamFlags::NONE,
                        payload: repair_payload.clone(),
                    },
                    mux_limits,
                )
                .is_none(),
            "repair budget must be cumulative, not refreshed for every tail/ACK event"
        );

        let earned_data_bytes = startup_floor.saturating_mul(100);
        sender.record_ordinary_data_dispatched_for_test(earned_data_bytes);

        assert!(
            sender.repair_extra_budget_remaining(mux_limits) >= startup_floor,
            "ordinary owner bytes earn more bounded extra repair budget"
        );
        assert!(
            sender
                .enqueue_repair_frame(
                    Frame::StreamData {
                        stream_id,
                        offset: (startup_floor * 2) as u64,
                        flags: StreamFlags::NONE,
                        payload: repair_payload,
                    },
                    mux_limits,
                )
                .is_some()
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
                .enqueue_repair_frame(
                    Frame::StreamData {
                        stream_id,
                        offset: 0,
                        flags: StreamFlags::NONE,
                        payload: Bytes::from(vec![0x44; startup_floor]),
                    },
                    mux_limits,
                )
                .is_some()
        );

        sender.record_ordinary_data_dispatched_for_test(startup_floor);
        assert!(
            sender.repair_extra_budget_remaining(mux_limits) > 0,
            "ordinary data earns fractional repair budget"
        );
        assert_eq!(
            sender.repair_extra_event_budget_remaining(mux_limits),
            0,
            "tiny earned repair crumbs should accumulate instead of emitting high-overhead repair frames"
        );

        sender.record_ordinary_data_dispatched_for_test(min_burst.saturating_mul(100));
        assert!(
            sender.repair_extra_event_budget_remaining(mux_limits) >= min_burst,
            "once enough owner bytes are sent, repair can spend a useful burst"
        );
    }

    #[test]
    fn response_lead_must_be_admissible_not_lowest_raw_eta() {
        let saturated_low_eta =
            response_target(0, UnderlayProtocol::Udp, 1.0, 512 * 1024, 512 * 1024, true);
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
            MuxLimits::default(),
            MppPerformanceConfig::default(),
            &[],
            &[],
            0,
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
            MppPerformanceConfig::default(),
            &[],
            &[],
            0,
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
            MppPerformanceConfig::default(),
            &[],
            &[],
            0,
            false,
        );

        assert!(
            selected.is_none(),
            "stream-ordered FIN must wait behind older active-owner data instead of escaping to validation output"
        );
    }

    #[test]
    fn single_active_response_target_still_obeys_bulk_admission() {
        let saturated =
            response_target(0, UnderlayProtocol::Udp, 1.0, 512 * 1024, 512 * 1024, true);

        let selected = choose_response_sender_data_target(
            &[saturated],
            FlowLane::Throughput,
            64 * 1024,
            MuxLimits::default(),
            MppPerformanceConfig::default(),
            0,
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
        let mux_limits = MuxLimits::default();
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
            has_ack_data_evidence: true,
            has_bulk_rate_evidence: true,
            bulk_discovery_sent_bytes: 0,
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
            MppPerformanceConfig::default(),
            0,
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
    fn response_quic_feed_credit_uses_live_carrier_debt_not_outdated_bdp() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = 64 * 1024;
        let mut loaded_quic = response_target(0, UnderlayProtocol::Udp, 250.0, 0, 64 * 1024, true);
        loaded_quic.snapshot.delivery_rate_bps = 351_000.0;
        loaded_quic.snapshot.pacing_rate_bps = 351_000.0;
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

        assert!(
            quic_credit >= 8 * 1024 * 1024,
            "QUIC feed credit must follow live carrier debt so the product sender keeps QUIC fed"
        );
        assert!(
            quic_credit > outdated_bdp_credit,
            "app-limited BDP must not be the only QUIC writer-feed ceiling"
        );

        let mut loaded_tcp = response_target(1, UnderlayProtocol::Tcp, 250.0, 0, 64 * 1024, true);
        loaded_tcp.snapshot.delivery_rate_bps = 351_000.0;
        loaded_tcp.snapshot.pacing_rate_bps = 351_000.0;
        loaded_tcp.snapshot.bytes_in_flight = 8 * 1024 * 1024;
        loaded_tcp.snapshot.queue_bytes = 1024 * 1024;
        let tcp_credit = response_target_emission_credit_bytes(
            &loaded_tcp,
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
        );

        assert_eq!(
            tcp_credit, outdated_bdp_credit,
            "TCP product credit remains model-gated; only QUIC delegates packet pacing to QUIC"
        );
    }

    #[test]
    fn quic_proof_success_path_does_not_become_unique_discovery_owner() {
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
            MppPerformanceConfig::default(),
            0,
            &[],
            Some(active.key),
        )
        .expect("active path should remain the unique owner");

        assert_eq!(
            selected.key, active.key,
            "proof-success QUIC validation paths must use duplicate/non-owner discovery, not unique owner data"
        );

        let mut discovery_in_flight = proof_success;
        discovery_in_flight.snapshot.product_bytes_in_flight = payload_bytes as u64;
        let selected_after_credit = choose_response_sender_data_target(
            &[active.clone(), discovery_in_flight.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            MppPerformanceConfig::default(),
            0,
            &[],
            Some(active.key),
        )
        .expect("active path remains available while discovery flight is outstanding");

        assert_eq!(
            selected_after_credit.key, active.key,
            "proof-only discovery is one bounded quantum until ACK-derived data evidence arrives"
        );
    }

    #[test]
    fn quic_proof_success_path_gets_duplicate_non_owner_discovery() {
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
        proof_success.has_bulk_rate_evidence = false;

        let duplicates = choose_quic_duplicate_discovery_targets(
            &[active.clone(), proof_success.clone()],
            active.key,
            payload_bytes,
            mux_limits,
            MppPerformanceConfig::default(),
        );

        assert_eq!(duplicates.len(), 1);
        assert_eq!(duplicates[0].key, proof_success.key);
    }

    #[test]
    fn quic_attached_path_without_sender_evidence_gets_bounded_duplicate_discovery() {
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
        let mut attached = response_target(1, UnderlayProtocol::Udp, 500.0, 0, 0, false);
        attached.has_sender_evidence = false;
        attached.has_ack_data_evidence = false;
        attached.has_bulk_rate_evidence = false;

        let duplicates = choose_quic_duplicate_discovery_targets(
            &[active.clone(), attached.clone()],
            active.key,
            payload_bytes,
            mux_limits,
            MppPerformanceConfig::default(),
        );

        assert_eq!(
            duplicates.len(),
            1,
            "attached QUIC validation paths can be bootstrapped by bounded duplicate data because the primary path owns the byte range"
        );
        assert_eq!(duplicates[0].key, attached.key);
    }

    #[test]
    fn tcp_primary_can_duplicate_discover_proof_success_udp_path() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let primary = response_target(
            0,
            UnderlayProtocol::Tcp,
            50.0,
            0,
            4 * payload_bytes as u64,
            true,
        );
        let mut proof_success = response_target(1, UnderlayProtocol::Udp, 10.0, 0, 0, false);
        proof_success.has_bulk_rate_evidence = false;

        let duplicates = choose_quic_duplicate_discovery_targets(
            &[primary.clone(), proof_success.clone()],
            primary.key,
            payload_bytes,
            mux_limits,
            MppPerformanceConfig::default(),
        );

        assert_eq!(duplicates.len(), 1);
        assert_eq!(
            duplicates[0].key, proof_success.key,
            "a TCP primary must be allowed to send bounded duplicate STREAM_DATA on proof-success UDP paths so mixed mode can obtain QUIC bulk-rate evidence without moving ownership"
        );
    }

    #[test]
    fn measured_udp_bulk_path_beats_poor_tcp_active_path() {
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
            &[active_tcp, measured_udp.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            MppPerformanceConfig::default(),
            0,
            &[],
            Some(CarrierPathKey {
                underlay: UnderlayProtocol::Tcp,
                path_id: PathId(0),
            }),
        )
        .expect("measured UDP path should be eligible for ordinary bulk");

        assert_eq!(
            selected.key, measured_udp.key,
            "carrier family must not override link metrics once UDP has bulk-rate evidence"
        );
    }

    #[test]
    fn measured_udp_bulk_path_beats_unproven_active_udp_path() {
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
            MppPerformanceConfig::default(),
            0,
            &[],
            Some(CarrierPathKey {
                underlay: UnderlayProtocol::Udp,
                path_id: PathId(0),
            }),
        )
        .expect("measured UDP path should be eligible for ordinary bulk");

        assert_eq!(
            selected.key, measured_udp.key,
            "active UDP bootstrap must yield to a bulk-rate-proven UDP path"
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
            MppPerformanceConfig::default(),
            &[],
            &[original_owner.key],
            0,
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
            MppPerformanceConfig::default(),
            &[],
            &[original_owner.key],
            0,
            true,
        )
        .expect("repair may fall back to the only non-owner path");

        assert_eq!(
            selected.key, proof_only_udp.key,
            "proof-only validation remains a bounded fallback when no proven repair path exists"
        );
    }

    #[test]
    fn quic_duplicate_discovery_uses_total_credit_not_only_outstanding_debt() {
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
        let mut spent = response_target(1, UnderlayProtocol::Udp, 500.0, 0, 0, false);
        spent.has_bulk_rate_evidence = false;
        spent.bulk_discovery_sent_bytes = payload_bytes as u64;

        let duplicates = choose_quic_duplicate_discovery_targets(
            &[active.clone(), spent],
            active.key,
            payload_bytes,
            mux_limits,
            MppPerformanceConfig::default(),
        );

        assert!(
            duplicates.is_empty(),
            "a proof-success QUIC path gets one bounded duplicate-discovery credit until real bulk evidence arrives"
        );

        let mut generous = response_target(1, UnderlayProtocol::Udp, 500.0, 0, 0, false);
        generous.has_bulk_rate_evidence = false;
        generous.bulk_discovery_sent_bytes = payload_bytes as u64;
        let duplicates = choose_quic_duplicate_discovery_targets(
            &[active.clone(), generous],
            active.key,
            payload_bytes,
            mux_limits,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 200,
            },
        );

        assert_eq!(
            duplicates.len(),
            1,
            "aggressive extra-traffic mode intentionally permits additional bounded duplicate discovery"
        );
    }

    #[test]
    fn response_dispatch_plan_carries_quic_duplicate_discovery_without_changing_primary() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let (active_commands, _active_rx) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new_with_limits(
            SessionId(77),
            UnderlayProtocol::Udp,
            PathId(0),
            active_commands,
            FlowLane::Throughput,
            mux_limits,
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
                has_ack_derived_data_sample: false,
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

        let plan = plan_response_data_dispatch(
            &stream,
            FlowLane::Throughput,
            0,
            payload_bytes,
            MppPerformanceConfig::default(),
            0,
        )
        .expect("active path should remain dispatchable");

        assert_eq!(
            plan.primary_key(),
            Some(CarrierPathKey {
                underlay: UnderlayProtocol::Udp,
                path_id: PathId(0),
            })
        );
        assert_eq!(plan.duplicate_discovery.len(), 1);
        assert_eq!(
            plan.duplicate_discovery[0].key,
            CarrierPathKey {
                underlay: UnderlayProtocol::Udp,
                path_id: PathId(1),
            }
        );
    }

    #[test]
    fn quic_ack_data_seen_path_gets_bounded_unique_trial_when_frontier_clear() {
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
        binding.record_bulk_discovery_bytes(validation_key, payload_bytes);
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
            output: ReliablePathStreamOutput::Switchable(binding),
            frames: frames_rx,
        };

        let plan = plan_response_data_dispatch(
            &stream,
            FlowLane::Throughput,
            0,
            payload_bytes,
            MppPerformanceConfig::default(),
            0,
        )
        .expect("ACK-data-seen validation path should receive a bounded unique trial");

        assert_eq!(
            plan.primary_key(),
            Some(CarrierPathKey {
                underlay: UnderlayProtocol::Udp,
                path_id: PathId(1),
            }),
            "ACK-derived carrier data should permit a bounded unique trial when the ordered frontier is clear"
        );
        assert!(
            plan.primary_is_bulk_discovery(),
            "ACK-data unique trials must debit the bounded discovery ledger"
        );
        assert!(plan.duplicate_discovery.is_empty());
    }

    #[test]
    fn ack_data_quic_trial_can_own_bounded_data_with_lower_owner_debt() {
        let mux_limits = MuxLimits::default();
        let performance = MppPerformanceConfig::default();
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
        let mut trial = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 0, false);
        trial.has_bulk_rate_evidence = false;
        trial.has_ack_data_evidence = true;

        let selected = choose_response_sender_data_target(
            &[active, trial.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            performance,
            0,
            &[CarrierPathFlightDebt {
                key: active_key,
                bytes: PATH_OPEN_SCORE_BYTES as u64,
            }],
            Some(active_key),
        )
        .expect("ACK-data QUIC trial should be admissible under bounded lower-owner debt");

        assert_eq!(
            selected.key, trial.key,
            "ACK-data evidence is path-scoped enough for one bounded ECF/BLEST-governed trial; proof-only paths remain blocked elsewhere"
        );
    }

    #[test]
    fn ack_data_quic_trial_gets_exploration_quantum_despite_conservative_eta() {
        let mux_limits = MuxLimits::default();
        let performance = MppPerformanceConfig::default();
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
        let mut trial = response_target(1, UnderlayProtocol::Udp, 500.0, 0, 0, false);
        trial.has_bulk_rate_evidence = false;
        trial.has_ack_data_evidence = true;
        trial.snapshot.delivery_rate_bps = default_path_rate_bps(UnderlayProtocol::Udp);
        trial.snapshot.pacing_rate_bps = trial.snapshot.delivery_rate_bps;
        trial.snapshot.app_limited = true;

        let selected = choose_response_sender_data_target(
            &[active, trial.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            performance,
            0,
            &[CarrierPathFlightDebt {
                key: active_key,
                bytes: PATH_OPEN_SCORE_BYTES as u64,
            }],
            Some(active_key),
        )
        .expect("ACK-data QUIC path should receive a bounded exploration quantum");

        assert_eq!(
            selected.key, trial.key,
            "ACK-data validation needs a bounded trial even while its app-limited ETA is conservative; otherwise it cannot graduate to bulk-rate evidence"
        );
    }

    #[test]
    fn quic_ack_data_unique_trial_credit_covers_minimum_pipe() {
        let mux_limits = MuxLimits::default();
        let performance = MppPerformanceConfig::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut trial = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 0, false);
        trial.has_bulk_rate_evidence = false;
        trial.has_ack_data_evidence = true;

        let one_discovery_credit =
            response_bulk_discovery_credit_bytes(payload_bytes, mux_limits, performance) as u64;
        trial.bulk_discovery_sent_bytes = one_discovery_credit.saturating_mul(2);

        assert!(
            !response_target_has_quic_unique_trial_credit(
                &trial,
                0,
                payload_bytes,
                mux_limits,
                performance
            ),
            "ACK-data unique exploration must not outrun the startup trial share before ordinary owner progress earns more"
        );

        let ordinary_progress_bytes = one_discovery_credit.saturating_mul(200);
        assert!(
            response_target_has_quic_unique_trial_credit(
                &trial,
                ordinary_progress_bytes,
                payload_bytes,
                mux_limits,
                performance
            ),
            "ordinary app-byte progress refills ACK-data unique exploration instead of permanently exhausting the path"
        );

        trial.snapshot.product_bytes_in_flight =
            (reliable_bulk_carrier_feed_quantum_bytes(mux_limits) * BBR_MIN_PIPE_CWND_PACKETS)
                as u64;
        assert!(
            !response_target_has_quic_unique_trial_credit(
                &trial,
                ordinary_progress_bytes,
                payload_bytes,
                mux_limits,
                performance
            ),
            "ACK-data unique trial remains bounded by outstanding minimum-pipe debt"
        );
    }

    #[test]
    fn quic_ack_data_unique_trial_credit_uses_carrier_inflight_limit() {
        let mux_limits = MuxLimits::default();
        let performance = MppPerformanceConfig::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let mut trial = response_target(
            1,
            UnderlayProtocol::Udp,
            5.0,
            0,
            16 * payload_bytes as u64,
            false,
        );
        trial.has_bulk_rate_evidence = false;
        trial.has_ack_data_evidence = true;
        trial.bulk_discovery_sent_bytes = (reliable_bulk_carrier_feed_quantum_bytes(mux_limits)
            * BBR_MIN_PIPE_CWND_PACKETS) as u64;
        let mut ordinary_progress_bytes = trial.bulk_discovery_sent_bytes.saturating_mul(100);

        assert!(
            response_target_has_quic_unique_trial_credit(
                &trial,
                ordinary_progress_bytes,
                payload_bytes,
                mux_limits,
                performance
            ),
            "ACK-data exploration carries real app bytes and should be bounded by path-local QUIC carrier credit, not only the duplicate-discovery floor"
        );

        trial.bulk_discovery_sent_bytes = trial.snapshot.inflight_limit_bytes;
        ordinary_progress_bytes = trial.bulk_discovery_sent_bytes.saturating_mul(100);
        assert!(
            response_target_has_quic_unique_trial_credit(
                &trial,
                ordinary_progress_bytes,
                payload_bytes,
                mux_limits,
                performance
            ),
            "ACKed unique exploration carries app bytes; cumulative sent bytes must not exhaust the path forever"
        );

        trial.snapshot.product_bytes_in_flight = trial.snapshot.inflight_limit_bytes;
        assert!(
            !response_target_has_quic_unique_trial_credit(
                &trial,
                ordinary_progress_bytes,
                payload_bytes,
                mux_limits,
                performance
            ),
            "the carrier-credit trial envelope remains finite"
        );
    }

    #[test]
    fn quic_ack_data_seen_path_does_not_displace_bulk_rate_proven_owner() {
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
        let mut trial = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 0, false);
        trial.has_bulk_rate_evidence = false;
        trial.bulk_discovery_sent_bytes = response_bulk_discovery_credit_bytes(
            payload_bytes,
            mux_limits,
            MppPerformanceConfig::default(),
        ) as u64
            + payload_bytes as u64;

        let selected = choose_response_sender_data_target(
            &[active.clone(), trial.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            MppPerformanceConfig::default(),
            0,
            &[],
            Some(CarrierPathKey {
                underlay: UnderlayProtocol::Udp,
                path_id: PathId(0),
            }),
        )
        .expect("bulk-rate-proven active path should remain eligible");

        assert_eq!(
            selected.key, active.key,
            "ACK-data-seen without non-app-limited bulk-rate evidence must not displace the current ordered owner"
        );
    }

    #[test]
    fn mixed_dispatch_plan_carries_udp_duplicate_discovery_when_primary_is_tcp() {
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
                has_ack_derived_data_sample: false,
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

        let plan = plan_response_data_dispatch(
            &stream,
            FlowLane::Throughput,
            0,
            payload_bytes,
            MppPerformanceConfig::default(),
            0,
        )
        .expect("TCP primary remains dispatchable");

        assert_eq!(
            plan.primary_key(),
            Some(CarrierPathKey {
                underlay: UnderlayProtocol::Tcp,
                path_id: PathId(0),
            })
        );
        assert_eq!(plan.duplicate_discovery.len(), 1);
        assert_eq!(
            plan.duplicate_discovery[0].key,
            CarrierPathKey {
                underlay: UnderlayProtocol::Udp,
                path_id: PathId(1),
            }
        );
    }

    #[tokio::test]
    async fn quic_duplicate_discovery_emits_non_owner_copy_without_migrating_lead() {
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
                has_ack_derived_data_sample: false,
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
            0,
            payload_bytes,
            MppPerformanceConfig::default(),
            0,
        )
        .expect("active path should remain dispatchable");
        let frame = Frame::StreamData {
            stream_id: StreamId(7),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![7_u8; payload_bytes]),
        };

        let selected =
            emit_planned_response_data_frame(&stream, plan, frame.clone(), FlowLane::Throughput)
                .await
                .expect("primary data and duplicate discovery should emit");

        assert_eq!(
            selected,
            Some(CarrierPathKey {
                underlay: UnderlayProtocol::Udp,
                path_id: PathId(0),
            })
        );
        assert_eq!(
            binding.ordered_data_owner(),
            Some(CarrierPathKey {
                underlay: UnderlayProtocol::Udp,
                path_id: PathId(0),
            })
        );
        assert!(matches!(
            recv_reliable_path_command(&mut active_rx).await,
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
        ));
        assert!(matches!(
            recv_reliable_path_command(&mut validation_rx).await,
            Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
        ));
        assert!(matches!(
            recv_reliable_path_command(&mut validation_rx).await,
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
        ));
        let lower = binding.lower_flights_before_offset(payload_bytes as u64);
        assert_eq!(lower.len(), 1);
        assert_eq!(
            lower[0].key,
            CarrierPathKey {
                underlay: UnderlayProtocol::Udp,
                path_id: PathId(0),
            }
        );
    }

    #[tokio::test]
    async fn quic_ack_data_exploration_owns_range_without_migrating_ordinary_lead() {
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
        binding.record_flight(active_key, &active_frame, true);

        let (trial_commands, mut trial_rx) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                UnderlayProtocol::Udp,
                PathId(1),
                trial_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                payload_bytes,
            ),
            ResponseStreamAttachOutcome::Attached
        );
        let trial_key = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        binding.update_path_metrics(
            trial_key,
            PathMetrics {
                path_id: trial_key.path_id,
                underlay: trial_key.underlay,
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
            MppPerformanceConfig::default(),
            (payload_bytes as u64).saturating_mul(200),
        )
        .expect("ACK-data exploration path should receive bounded unique trial");
        assert_eq!(plan.primary_key(), Some(trial_key));
        assert!(plan.primary_is_bulk_discovery());

        let trial_frame = Frame::StreamData {
            stream_id: StreamId(7),
            offset: payload_bytes as u64,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![4_u8; payload_bytes]),
        };
        let selected =
            emit_planned_response_data_frame(&stream, plan, trial_frame, FlowLane::Throughput)
                .await
                .expect("trial data should emit");

        assert_eq!(selected, Some(trial_key));
        assert_eq!(
            binding.ordered_data_owner(),
            Some(active_key),
            "bounded exploration owns its byte range but must not become the ordinary lead"
        );
        assert!(matches!(
            recv_reliable_path_command(&mut trial_rx).await,
            Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
        ));
        assert!(matches!(
            recv_reliable_path_command(&mut trial_rx).await,
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
        ));
        let lower = binding.lower_flights_before_offset((payload_bytes * 2) as u64);
        assert!(
            lower.iter().any(|flight| flight.key == trial_key),
            "the exploration range remains path-owned for ordering debt until ACKed"
        );
    }

    #[test]
    fn single_response_carrier_uses_sliding_window_not_multipath_ordering_debt() {
        let target = response_target(
            0,
            UnderlayProtocol::Tcp,
            5.0,
            8 * 1024 * 1024,
            16 * 1024 * 1024,
            true,
        );
        let lower_flights = vec![CarrierPathFlightDebt {
            key: target.key,
            bytes: 8 * 1024 * 1024,
        }];

        let selected = choose_response_sender_data_target(
            std::slice::from_ref(&target),
            FlowLane::Throughput,
            64 * 1024,
            MuxLimits::default(),
            MppPerformanceConfig::default(),
            0,
            &lower_flights,
            Some(target.key),
        )
        .expect("single carrier lower flight is normal sliding-window debt");

        assert_eq!(selected.key, target.key);
    }

    #[test]
    fn proven_udp_candidate_can_overtake_large_lower_owner_when_completion_model_allows() {
        let owner = response_target(
            0,
            UnderlayProtocol::Udp,
            80.0,
            2 * 1024 * 1024,
            16 * 1024 * 1024,
            true,
        );
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
            MppPerformanceConfig::default(),
            0,
            &lower_flights,
            Some(owner.key),
        )
        .expect("bulk-rate-proven same-underlay candidate should remain eligible under ECF/BLEST");

        assert_eq!(selected.key, alternate.key);
    }

    #[test]
    fn proven_udp_candidate_is_not_blocked_by_lower_udp_owner_when_within_reorder_budget() {
        let owner = response_target(
            0,
            UnderlayProtocol::Udp,
            80.0,
            2 * 1024 * 1024,
            16 * 1024 * 1024,
            true,
        );
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
            MppPerformanceConfig::default(),
            0,
            &lower_flights,
            Some(owner.key),
        )
        .expect("bulk-rate-proven QUIC candidate should be admitted by completion/reorder math");

        assert_eq!(selected.key.path_id, PathId(1));
    }

    #[test]
    fn proof_only_udp_candidate_is_blocked_from_unique_data_with_lower_udp_owner() {
        let owner = response_target(
            0,
            UnderlayProtocol::Udp,
            80.0,
            2 * 1024 * 1024,
            16 * 1024 * 1024,
            true,
        );
        let mut proof_only =
            response_target(1, UnderlayProtocol::Udp, 7.0, 0, 16 * 1024 * 1024, false);
        proof_only.has_ack_data_evidence = false;
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
            MppPerformanceConfig::default(),
            0,
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
            MppPerformanceConfig::default(),
            0,
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
            MppPerformanceConfig::default(),
            0,
            &[],
            Some(lead.key),
        )
        .expect("lower ETA path should be selected");

        assert_eq!(selected.key, lower_eta_alternate.key);
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
            MppPerformanceConfig::default(),
            0,
            &[],
            Some(lead.key),
        )
        .expect("near-tie lead should remain selected inside observed jitter");

        assert_eq!(selected.key, lead.key);
    }
}
