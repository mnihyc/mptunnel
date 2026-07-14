//! Response-direction planning, reservation, and dispatch service.
//!
//! The planner ranks immutable binding snapshots. The reliable-path binding
//! remains the authority that revalidates generations and commits exact ranges.

#[cfg(test)]
mod tests;

use super::*;
use crate::model::ack_clock::{
    TcpAckClockCalibrationOpportunity, reliable_tcp_ack_clock_calibration_opportunity,
};
#[cfg(feature = "lab-diagnostics")]
use crate::model::admission::BulkExplorationCompletionProjection;
use crate::model::admission::{
    BulkAdmissionCheck, BulkAdmissionRole, bulk_active_service_product_envelope_bytes,
    bulk_additional_admission_role, bulk_candidate_admission_suppression_with_completion_backlog,
    bulk_candidate_admission_suppression_with_ordering_debt, bulk_candidate_pipe_bytes,
    bulk_exploration_completion_projection, bulk_latency_pressure_service_feed_window_bytes,
    bulk_service_feed_reservoir_payload_bytes, bulk_service_horizon_payload_bytes,
    bulk_service_product_envelope_payload_bytes,
};
use crate::model::{ResponseCandidateTailDebt, ResponseOrderedTail, ResponseSameFamilyReservoir};
pub(super) fn carrier_path_key_order(
    left: CarrierPathKey,
    right: CarrierPathKey,
) -> std::cmp::Ordering {
    (left.path_id, left.underlay).cmp(&(right.path_id, right.underlay))
}

pub(super) fn response_ordering_debt_bytes(
    lower_flights: &[CarrierPathFlightDebt],
    candidate: CarrierPathKey,
) -> u64 {
    lower_flights
        .iter()
        .filter_map(|flight| (flight.key != candidate).then_some(flight.bytes))
        .sum()
}

pub(super) fn response_oldest_lower_flight_owner(
    lower_flights: &[CarrierPathFlightDebt],
) -> Option<CarrierPathKey> {
    lower_flights.first().map(|flight| flight.key)
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ResponseBulkLead {
    pub(super) key: CarrierPathKey,
    pub(super) snapshot: PathSnapshot,
    pub(super) eta_ms: f64,
}

#[cfg(feature = "lab-diagnostics")]
#[derive(Debug, Clone, Copy)]
pub(super) struct ResponseBulkCandidateDiag {
    lead: Option<ResponseBulkLead>,
    role: Option<BulkAdmissionRole>,
    ordering_debt: u64,
}

#[cfg(feature = "lab-diagnostics")]
pub(super) fn lab_response_bulk_output_candidate(
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
pub(super) fn lab_response_bulk_output_selected(
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
pub(super) fn lab_response_ack_clock_calibration_admission(
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

pub(super) fn response_tcp_calibration_opportunity_candidate(
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

#[derive(Clone)]
pub(super) enum ResponseDataDispatchTarget {
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
pub(super) struct ResponseDataDispatchPlan {
    pub(super) primary: ResponseDataDispatchTarget,
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

pub(super) struct ResponseDataEmitOutcome {
    pub(super) selected_path: Option<CarrierPathKey>,
}

#[derive(Clone, Copy)]
pub(super) struct ResponseSubflowAdmissionCommit {
    pub(super) planner_generation: u64,
    pub(super) lane_generation: u64,
    pub(super) service: CarrierPathKey,
    pub(super) startup_owner_credit_bytes: usize,
    pub(super) optional_overhead_budget_bytes: usize,
    pub(super) max_read_gap_budget: Duration,
    pub(super) input: SubflowAdmissionInput,
}

#[derive(Clone, Copy)]
pub(super) struct ResponseServiceHandoffCommit {
    pub(super) planner_generation: u64,
    pub(super) lane_generation: u64,
    pub(super) model_generation: u64,
    pub(super) handoff_frontier: u64,
    pub(super) service: CarrierPathKey,
    pub(super) service_path_instance_id: ServerCarrierPathInstanceId,
    pub(super) service_incarnation: u64,
    pub(super) target_path_instance_id: ServerCarrierPathInstanceId,
    pub(super) mode: ResponseServiceHandoffMode,
    pub(super) target_command_pending_limit_bytes: u64,
    pub(super) capacity_proof: Option<QuicCapacityProofCandidate>,
}

#[derive(Clone, Copy)]
pub(super) struct ResponseAckClockCalibrationCommit {
    pub(super) planner_generation: u64,
    pub(super) lane_generation: u64,
    pub(super) model_generation: u64,
    pub(super) service: CarrierPathKey,
    pub(super) service_incarnation: u64,
    pub(super) service_pending_bytes: u64,
    pub(super) target_pending_bytes: u64,
    pub(super) limit_bytes: u64,
    pub(super) requires_active_response_start: bool,
}

#[derive(Clone, Copy)]
pub(super) struct ResponseAckClockCalibrationRetirementIntent {
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
pub(super) struct ResponseSelectedDataTarget {
    target: ResponseSenderPathTarget,
    admission: PathAdmission,
    service_handoff_commit: Option<ResponseServiceHandoffCommit>,
    subflow_set_commit: Option<ResponseSubflowAdmissionCommit>,
    ack_clock_calibration_commit: Option<ResponseAckClockCalibrationCommit>,
}

pub(super) fn response_bulk_admission_role(
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

pub(super) fn response_service_anchor_key(
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

pub(super) fn response_unique_quic_data_would_expand_ordering_debt(
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

pub(super) fn response_target_has_service_anchor_rights(target: &ResponseSenderPathTarget) -> bool {
    target.is_active
}

pub(super) fn response_target_is_plausible_unique_owner_candidate(
    target: &ResponseSenderPathTarget,
) -> bool {
    target.attachment_role != StreamOpenRole::Repair
        && (response_target_has_service_anchor_rights(target) || target.has_bulk_rate_evidence)
}

pub(super) fn response_target_is_measured_same_underlay_subflow_candidate(
    service_key: CarrierPathKey,
    target: &ResponseSenderPathTarget,
) -> bool {
    target.attachment_role != StreamOpenRole::Repair
        && target.key != service_key
        && target.key.underlay == service_key.underlay
        && !target.is_active
        && target.has_bulk_rate_evidence
}

pub(super) fn response_target_measured_admission_snapshot(
    target: &ResponseSenderPathTarget,
) -> PathSnapshot {
    let mut snapshot = target.snapshot;
    if target.has_bulk_rate_evidence {
        // An app-limited poll does not erase the retained path-scoped rate
        // model. Proven Subflows must continue to pass ECF completion math.
        snapshot.app_limited = false;
    }
    snapshot
}

pub(super) fn response_target_is_startup_same_underlay_subflow_candidate(
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

pub(super) fn response_startup_sample_has_completion_opportunity(
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
pub(super) struct ResponseQuicCapacityCalibrationGeometry {
    train_bytes: usize,
    fits_session_envelope: bool,
    sample_floor_bytes: u64,
    accounting_slack_bytes: u64,
    fresh_strict_window_bytes: u64,
    carrier_window_bytes: u64,
}

pub(super) fn response_quic_capacity_calibration_geometry(
    target: &ResponseSenderPathTarget,
    mux_limits: MuxLimits,
) -> ResponseQuicCapacityCalibrationGeometry {
    let sample_floor = reliable_subflow_startup_sample_limit_bytes(mux_limits);
    let packet_accounting_slack = (PATH_OPEN_SCORE_BYTES as u64).min(sample_floor / 8);
    let fresh_strict_window = sample_floor.saturating_sub(packet_accounting_slack).max(1);
    let timing_slack = CAPACITY_TIMING_SLACK_BYTES;
    let carrier_window = target
        .snapshot
        .inflight_limit_bytes
        .max(target.snapshot.bytes_in_flight);
    let session_envelope = reliable_capacity_calibration_session_limit_bytes(mux_limits);
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
pub(super) fn response_quic_capacity_calibration_train_bytes(
    target: &ResponseSenderPathTarget,
    mux_limits: MuxLimits,
) -> usize {
    response_quic_capacity_calibration_geometry(target, mux_limits).train_bytes
}

pub(super) fn response_quic_capacity_calibration_lease(
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

pub(super) fn response_quic_capacity_proof_validity(target: &ResponseSenderPathTarget) -> Duration {
    let srtt = Duration::from_secs_f64((target.snapshot.srtt_ms.max(1.0)) / 1_000.0);
    let rttvar = Duration::from_secs_f64((target.snapshot.jitter_ms.max(1.0)) / 1_000.0);
    quic_bulk_proof_freshness_horizon(srtt, rttvar)
}

#[cfg(any(test, feature = "lab-diagnostics"))]
pub(super) fn response_service_handoff_preserves_fair_share(
    service: &ResponseSenderPathTarget,
    target: &ResponseSenderPathTarget,
) -> bool {
    // Sticky placement compares one moved flow; only aggregate carrier rates
    // are divided because TCP product ACK clocks already measure a flow share.
    response_service_fair_share_bps(service, false) <= response_service_fair_share_bps(target, true)
}

pub(super) fn response_service_fair_share_bps(
    target: &ResponseSenderPathTarget,
    adds_flow: bool,
) -> f64 {
    response_rate_fair_share_bps(target.snapshot, target.snapshot.rate_scope, adds_flow)
}

pub(super) fn response_service_handoff_mode_for_targets(
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

pub(super) fn response_service_handoff_target_view(
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

pub(super) fn response_service_handoff_start_capacity_proof(
    target: &ResponseSenderPathTarget,
    now: Instant,
) -> Option<QuicCapacityProofCandidate> {
    (target.key.underlay == UnderlayProtocol::Udp)
        .then_some(target.quic_capacity_proof)
        .flatten()
        .filter(|proof| valid_quic_capacity_proof_candidate_at(*proof, now))
}

#[derive(Clone)]
pub(super) struct ResponseServiceHandoffCandidate {
    service: ResponseSenderPathTarget,
    target: ResponseSenderPathTarget,
    mode: ResponseServiceHandoffMode,
}

#[allow(clippy::too_many_arguments)]
pub(super) fn select_response_service_handoff_candidate(
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

pub(super) fn select_response_quic_capacity_calibration_target(
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
pub(super) fn select_response_tcp_capacity_probe_target(
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
    let envelope = reliable_capacity_calibration_session_limit_bytes(mux_limits);
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
pub(super) fn select_response_service_handoff_target(
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

pub(super) fn response_service_handoff_drain_matches_candidate(
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

pub(super) fn response_service_handoff_drain_matches_selection(
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

pub(super) fn response_service_handoff_drain_lease(
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

pub(super) fn select_response_ack_clock_calibration_target(
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

pub(super) fn response_ack_clock_calibration_pending(
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

pub(super) fn response_ack_clock_calibration_blocks_generic_owner(
    target: &ResponseSenderPathTarget,
) -> bool {
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

pub(super) fn response_calibration_service_reservoir_has_credit(
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

pub(super) fn response_ack_clock_calibration_needs_opportunity_decision(
    target: &ResponseSenderPathTarget,
) -> bool {
    target.key.underlay == UnderlayProtocol::Tcp
        && target.ack_clock_calibration_eligible
        && !target.ack_clock_calibration_proven
        && !target.ack_clock_calibration_active
        && target.ack_clock_calibration_spent_bytes == 0
        && target.ack_clock_calibration_credit_limit_bytes > 0
}

pub(super) struct ResponseOwnerAdmission {
    admission: PathAdmission,
    subflow_set_commit: Option<ResponseSubflowAdmissionCommit>,
    bulk_role: BulkAdmissionRole,
    model_suppression: Option<&'static str>,
}

pub(super) fn response_owner_bulk_model_suppression(
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

pub(super) fn response_fallback_bulk_model_suppression(
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
pub(super) fn response_target_unique_owner_admission(
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
pub(super) fn response_target_unique_owner_admission_with_epoch(
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

pub(super) fn response_target_can_own_unique_bulk_data(
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

pub(super) fn response_target_can_own_unique_bulk_data_with_epoch(
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

pub(super) fn response_cross_underlay_owner_allowed(
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

pub(super) fn response_ordered_owner_missing_under_debt(
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

pub(super) fn response_active_lead_suppression(
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

pub(super) fn choose_response_admissible_lead(
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

pub(super) fn choose_lowest_eta_response_target(
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

pub(super) fn choose_same_family_sender_evidenced_response_target(
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

pub(super) fn response_target_has_ack_gap_repair_evidence(
    target: &ResponseSenderPathTarget,
) -> bool {
    target.is_active || target.has_bulk_rate_evidence
}

pub(super) fn response_target_has_path_failure_repair_evidence(
    _target: &ResponseSenderPathTarget,
) -> bool {
    // A live carrier output is enough for bounded failover RepairData after the
    // original owner has disappeared or become unusable. The repair flight never
    // path-proves the carrier and never changes Service ownership.
    true
}

pub(super) fn response_target_can_receive_repair(
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

pub(super) fn choose_response_repair_target(
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

pub(super) fn choose_response_service_or_proven_data_target(
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

pub(super) fn choose_response_sender_target(
    targets: &[ResponseSenderPathTarget],
    lane: FlowLane,
    frame: &Frame,
    emit_mode: CarrierEmitMode,
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
        && emit_mode == CarrierEmitMode::StreamOrdered
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

pub(super) fn response_target_can_enqueue_frame_now(
    target: &ResponseSenderPathTarget,
    frame: &Frame,
    lane: FlowLane,
    emit_mode: CarrierEmitMode,
) -> bool {
    match emit_mode {
        CarrierEmitMode::Classified => target.commands.can_enqueue_frame_now(frame, lane),
        CarrierEmitMode::StreamOrdered => {
            target.commands.can_enqueue_stream_ordered_frame_now(lane)
        }
    }
}

#[cfg(test)]
pub(super) fn choose_response_sender_data_target(
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
pub(super) fn choose_response_sender_data_target_with_ordered_debt(
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
pub(super) fn choose_response_sender_data_target_with_ordered_debt_and_epoch(
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
pub(super) fn select_response_sender_data_target_with_ordered_debt_and_epoch(
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
pub(super) struct ResponseDataAdmissionPolicy {
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
pub(super) fn admit_response_data_target(
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
pub(super) fn select_response_sender_data_target_with_ordered_debt_inner(
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

pub(super) fn select_response_sender_data_target_with_ordered_debt_inner_and_retirements(
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

pub(super) fn response_target_within_adaptive_lead_hysteresis(
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

pub(super) fn response_target_assigned_product_bytes(target: &ResponseSenderPathTarget) -> u64 {
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

pub(super) fn response_feedable_service_owner_target(
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

pub(super) fn response_same_family_reservoir_policy(
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

pub(super) fn response_target_is_same_family_reservoir_candidate(
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

pub(super) fn response_same_family_reservoir_candidate_debt(
    reservoir: ResponseSameFamilyReservoir,
    target: &ResponseSenderPathTarget,
) -> ResponseCandidateTailDebt {
    // The global tail contains unique OwnerData. Subtract only this candidate's
    // unique share; generic carrier admission separately keeps every OwnerData
    // and RepairData copy charged as product flight.
    reservoir.for_candidate(target.key, target.owner_data_in_flight_bytes)
}

pub(super) fn response_same_family_reservoir_subflow_target(
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

pub(super) fn response_target_active_bulk_flows(target: &ResponseSenderPathTarget) -> u32 {
    target
        .snapshot
        .active_flows
        .saturating_sub(target.snapshot.active_latency_sensitive_flows)
}

pub(super) fn response_target_has_emission_credit(
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

pub(super) fn response_service_has_assigned_owner_credit(
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

pub(super) fn response_service_emission_credit_bytes(
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

pub(super) fn response_target_emission_credit_bytes(
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

pub(super) fn response_service_startup_emission_credit_bytes(
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

pub(super) fn response_quic_carrier_feed_credit_bytes(
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
pub(super) fn plan_response_data_dispatch(
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

pub(super) fn plan_response_data_dispatch_with_ordered_debt_impl(
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
                    reliable_capacity_calibration_session_limit_bytes(binding.mux_limits());
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

pub(super) fn response_plan_is_ack_clock_calibration(planned: &ResponseDataDispatchPlan) -> bool {
    matches!(
        &planned.primary,
        ResponseDataDispatchTarget::Switchable {
            ack_clock_calibration_commit: Some(_),
            ..
        }
    )
}

pub(super) fn plan_response_data_payload_with_ordered_debt_impl(
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

pub(super) fn response_dispatch_payload_bytes(
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
