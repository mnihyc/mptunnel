//! Response-direction planning, reservation, and dispatch service.
//!
//! The planner ranks immutable binding snapshots. The reliable-path binding
//! remains the authority that revalidates generations and commits exact ranges.

use super::admission::{
    ResponseSubflowAdmissionCommit, response_bulk_admission_role,
    response_fallback_bulk_model_suppression, response_owner_bulk_model_suppression,
    response_same_family_reservoir_candidate_debt, response_same_family_reservoir_for_service,
    response_service_anchor_key, response_service_has_assigned_owner_credit,
    response_target_assigned_product_bytes, response_target_can_own_unique_bulk_data,
    response_target_emission_credit_bytes, response_target_has_emission_credit,
    response_target_is_measured_same_underlay_subflow_candidate,
    response_target_is_plausible_unique_owner_candidate,
    response_target_is_same_family_reservoir_candidate,
    response_target_is_startup_same_underlay_subflow_candidate,
    response_target_unique_owner_admission_with_epoch,
};
#[cfg(feature = "lab-diagnostics")]
use super::diagnostics::{
    ResponseBulkCandidateDiag, lab_response_bulk_output_candidate,
    lab_response_bulk_output_selected, lab_response_service_handoff_evaluation,
};
use super::quic_capacity::{
    select_response_quic_capacity_calibration_start, try_start_response_quic_capacity_calibration,
};
use super::tcp_capacity::{
    ResponseAckClockCalibrationCommit, ResponseAckClockCalibrationRetirementIntent,
    response_ack_clock_calibration_blocks_generic_owner,
    response_ack_clock_calibration_needs_opportunity_decision,
    response_ack_clock_calibration_pending, response_calibration_service_reservoir_has_credit,
    select_response_ack_clock_calibration_target, select_response_tcp_capacity_probe_start,
    try_start_response_tcp_capacity_probe,
};
#[cfg(test)]
use super::*;
use crate::model::admission::{
    BulkAdmissionCheck, BulkAdmissionRole, bulk_candidate_admission_suppression_with_ordering_debt,
};
use crate::model::capacity::{
    QUIC_PERSISTENT_CONGESTION_THRESHOLD, QuicCapacityProofCandidate,
    adaptive_reliable_relay_chunk_bytes_with_frame_limit,
};
use crate::model::multipath::{
    FlowSubflowSet, PathAdmission, PathAdmissionDecision, PathRuntimeRole,
    cross_family_reliable_owner_health,
};
use crate::model::path::{CarrierPathInstanceId, CarrierPathKey, carrier_path_key_order};
use crate::model::response::{
    CarrierPathFlightDebt, ResponseBulkLead, ResponseCandidateTailDebt, ResponseOrderedTail,
    ResponseSameFamilyReservoir, ResponseServiceFamilyLoads, ResponseServiceHandoffMode,
    response_oldest_lower_flight_owner, response_ordering_debt_bytes, response_rate_fair_share_bps,
    response_snapshot_handoff_mode,
};
use crate::model::timing::transport_pto_from_snapshot;
use crate::model::work::{CarrierWorkKind, ReliableWorkClass};
use crate::mux::MuxLimits;
use crate::mux::stream::ReliableSendStream;
use crate::protocol::frame::reliable_stream_frame_accounted_bytes;
use crate::protocol::{Frame, StreamOpenRole, UnderlayProtocol};
use crate::runtime::RuntimeError;
use crate::runtime::path::model::{default_path_rate_bps, path_within_adaptive_lead_hysteresis};
use crate::runtime::relay_striping::relay_frame_is_bulk_stream_data;
use crate::runtime::sender::{CarrierEmitMode, RelaySendCause};
use crate::runtime::stream::response::{
    MIN_ACTIVE_RESPONSE_FLOWS_FOR_SAME_FAMILY_DISCOVERY,
    ResponseAckClockCalibrationRetirementRequest, ResponseDispatchTarget, ResponseSenderPathTarget,
    ResponseServiceHandoffDrainRequest, ResponseServiceHandoffDrainReservation,
    ResponseStreamBinding, quic_capacity_proof_pin_matches_marker, server_bulk_output_eta_ms,
    valid_quic_capacity_proof_candidate_at,
};
use crate::runtime::stream::{
    FixedReliablePathOutput, ReliablePathStream, ReliablePathStreamOutput,
    reliable_work_lane_to_carrier_lane,
};
use crate::scheduler::{FlowLane, PathRateScope};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[cfg(test)]
#[path = "planner_test.rs"]
mod tests;

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

#[derive(Clone, Copy, PartialEq, Eq)]
enum ResponsePlanningMode {
    /// Reports whether dispatch or an apply transition can make progress.
    Preview,
    /// Owns expiry, discovery start, drain, and calibration retirement.
    Apply,
}

enum ResponseDataPlanningOutcome {
    Dispatch(ResponseDataDispatchPlan),
    ApplyRequired,
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
pub(super) struct ResponseServiceHandoffCommit {
    pub(super) planner_generation: u64,
    pub(super) lane_generation: u64,
    pub(super) model_generation: u64,
    pub(super) handoff_frontier: u64,
    pub(super) service: CarrierPathKey,
    pub(super) service_path_instance_id: CarrierPathInstanceId,
    pub(super) service_incarnation: u64,
    pub(super) target_path_instance_id: CarrierPathInstanceId,
    pub(super) mode: ResponseServiceHandoffMode,
    pub(super) target_command_pending_limit_bytes: u64,
    pub(super) capacity_proof: Option<QuicCapacityProofCandidate>,
}

#[derive(Clone)]
pub(super) struct ResponseSelectedDataTarget {
    target: ResponseSenderPathTarget,
    admission: PathAdmission,
    service_handoff_commit: Option<ResponseServiceHandoffCommit>,
    subflow_set_commit: Option<ResponseSubflowAdmissionCommit>,
    ack_clock_calibration_commit: Option<ResponseAckClockCalibrationCommit>,
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
    response_rate_fair_share_bps(
        target.observation.snapshot,
        target.observation.snapshot.rate_scope,
        adds_flow,
    )
}

pub(super) fn response_service_handoff_mode_for_targets(
    service: &ResponseSenderPathTarget,
    target: &ResponseSenderPathTarget,
    family_loads: ResponseServiceFamilyLoads,
) -> Option<ResponseServiceHandoffMode> {
    response_snapshot_handoff_mode(
        service.observation.key.underlay,
        service.observation.snapshot,
        target.observation.key.underlay,
        target.observation.snapshot,
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
        || reservation.target != target.observation.key
        || reservation.target_path_instance_id != target.observation.path_instance_id
        || reservation.target_incarnation != target.observation.incarnation
    {
        return None;
    }
    let raw_capacity_proof = target.quic_capacity_proof;
    // A drain freezes the authority chosen at reservation time. Clear an
    // unrelated raw marker when this transaction deliberately uses generic
    // carrier evidence instead of receipt authority.
    target.quic_capacity_proof = reservation.capacity_proof;
    if let Some(proof) = reservation.capacity_proof {
        if target.observation.key.underlay != UnderlayProtocol::Udp {
            return None;
        }
        if !quic_capacity_proof_pin_matches_marker(proof, raw_capacity_proof, now) {
            return None;
        }
        // The ordinary marker still expires; only this transaction view retains it.
        target.observation.has_bulk_rate_evidence = true;
        target.observation.snapshot.delivery_rate_bps = proof.rate_bps.max(1) as f64;
        target.observation.snapshot.rate_scope = PathRateScope::PathCapacity;
        target.observation.snapshot.confidence = target.observation.snapshot.confidence.max(
            (proof.received_bytes as f64 / proof.sample_floor_bytes.max(1) as f64).clamp(0.0, 1.0),
        );
        target.observation.eta_ms = server_bulk_output_eta_ms(
            target.observation.key,
            target.observation.snapshot,
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
    (target.observation.key.underlay == UnderlayProtocol::Udp)
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
    let service = targets
        .iter()
        .find(|target| target.observation.key == service_key)?;
    if required_reservation.is_some_and(|reservation| {
        reservation.service != service.observation.key
            || reservation.service_path_instance_id != service.observation.path_instance_id
            || reservation.service_incarnation != service.observation.incarnation
    }) {
        return None;
    }
    if !service.observation.is_service
        || !service.observation.has_bulk_rate_evidence
        || service.observation.snapshot.active_latency_sensitive_flows > 0
        || service
            .observation
            .snapshot
            .session_active_latency_sensitive_flows
            > 0
    {
        return None;
    }
    let now = Instant::now();
    let target = targets
        .iter()
        .filter_map(|target| {
            response_service_handoff_target_view(
                target,
                service.observation.key,
                lane,
                payload_bytes,
                mux_limits,
                required_reservation,
                now,
            )
        })
        .filter(|target| {
            target.observation.key.underlay != service.observation.key.underlay
                && target.observation.attachment_role == StreamOpenRole::Validation
                && !target.observation.is_service
                && target.observation.has_bulk_rate_evidence
                && target.observation.owner_data_in_flight_bytes == 0
                && target.observation.snapshot.product_bytes_in_flight == 0
                && target.observation.snapshot.active_latency_sensitive_flows == 0
                && target
                    .observation
                    .snapshot
                    .session_active_latency_sensitive_flows
                    == 0
                && response_service_handoff_mode_for_targets(service, target, service_family_loads)
                    .is_some()
                && target.commands.can_enqueue_lane_now(lane)
                && response_owner_bulk_model_suppression(
                    target,
                    ResponseBulkLead {
                        key: service.observation.key,
                        snapshot: service.observation.snapshot,
                        eta_ms: service.observation.eta_ms,
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
            left.observation
                .eta_ms
                .total_cmp(&right.observation.eta_ms)
                .then_with(|| carrier_path_key_order(left.observation.key, right.observation.key))
                .then_with(|| {
                    left.observation
                        .incarnation
                        .cmp(&right.observation.incarnation)
                })
        })?;
    let mode = response_service_handoff_mode_for_targets(service, &target, service_family_loads)?;
    Some(ResponseServiceHandoffCandidate {
        service: service.clone(),
        target,
        mode,
    })
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
            service: service.observation.key,
            service_path_instance_id: service.observation.path_instance_id,
            service_incarnation: service.observation.incarnation,
            target_path_instance_id: target.observation.path_instance_id,
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
        && reservation.service == candidate.service.observation.key
        && reservation.service_path_instance_id == candidate.service.observation.path_instance_id
        && reservation.service_incarnation == candidate.service.observation.incarnation
        && reservation.target == candidate.target.observation.key
        && reservation.target_path_instance_id == candidate.target.observation.path_instance_id
        && reservation.target_incarnation == candidate.target.observation.incarnation
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
        && reservation.target == selection.target.observation.key
        && reservation.target_path_instance_id == commit.target_path_instance_id
        && reservation.target_incarnation == selection.target.observation.incarnation
        && reservation.capacity_proof == commit.capacity_proof
}

pub(super) fn response_service_handoff_drain_lease(
    service: &ResponseSenderPathTarget,
    outstanding_owner_bytes: u64,
) -> Duration {
    let rate_bps = response_service_fair_share_bps(service, false)
        .max(default_path_rate_bps(service.observation.key.underlay))
        .max(1.0);
    let transmit_seconds = outstanding_owner_bytes as f64 * 8.0 / rate_bps;
    let transmit_eta = Duration::from_secs_f64(transmit_seconds);
    let recovery_margin = transport_pto_from_snapshot(Some(service.observation.snapshot))
        .saturating_mul(QUIC_PERSISTENT_CONGESTION_THRESHOLD);
    // Fresh assignment pauses while already-owned bytes continue draining. Size
    // the lease from this binding's share; a five-second cap made a default
    // 2 MiB window impossible to move on a healthy 1 Mbit/s path.
    transmit_eta
        .saturating_add(recovery_margin)
        .max(Duration::from_secs(1))
        .min(Duration::from_secs(60))
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
            .find(|entry| entry.observation.is_service)
            .map(|entry| entry.observation.key)
    });
    let current_owner_bulk_rate_proven = current_owner
        .and_then(|owner_key| {
            candidates
                .iter()
                .copied()
                .find(|entry| entry.observation.key == owner_key)
        })
        .is_none_or(|owner| owner.observation.has_bulk_rate_evidence);
    let candidate_continues_lower_frontier =
        response_oldest_lower_flight_owner(lower_flights) == Some(target.observation.key);
    if candidate_continues_lower_frontier
        && (target.observation.is_service || target.observation.has_bulk_rate_evidence)
    {
        return true;
    }
    cross_family_reliable_owner_health(
        current_owner,
        current_owner_bulk_rate_proven,
        target.observation.key,
        target.observation.has_bulk_rate_evidence,
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
            let live_owner = targets.iter().any(|target| target.observation.key == owner);
            // A missing Service owner with unresolved tail debt normally blocks
            // later OwnerData. The only non-clear-frontier failover is a
            // sender-evidenced survivor in the same carrier family; RepairData
            // still never path-proves or transfers ownership.
            let same_underlay_sender_evidence_failover = targets.iter().any(|target| {
                target.observation.key.underlay == owner.underlay
                    && target.observation.has_sender_evidence
            });
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
        best_snapshot: target.observation.snapshot,
        best_eta_ms: target.observation.eta_ms,
        candidate_snapshot: target.observation.snapshot,
        candidate_eta_ms: target.observation.eta_ms,
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
            key: active.observation.key,
            snapshot: active.observation.snapshot,
            eta_ms: active.observation.eta_ms,
        });
    }

    if let Some(owner) = lower_owner {
        let owner_target = candidate_targets
            .iter()
            .copied()
            .find(|target| target.observation.key == owner)?;
        if owner_target.observation.is_service || owner_target.observation.has_bulk_rate_evidence {
            let owner_cross_path_debt =
                response_ordering_debt_bytes(lower_flights, owner_target.observation.key);
            return response_active_lead_suppression(
                owner_target,
                mux_limits,
                payload_bytes,
                owner_cross_path_debt,
            )
            .is_none()
            .then_some(ResponseBulkLead {
                key: owner_target.observation.key,
                snapshot: owner_target.observation.snapshot,
                eta_ms: owner_target.observation.eta_ms,
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
            left.observation
                .eta_ms
                .total_cmp(&right.observation.eta_ms)
                .then_with(|| carrier_path_key_order(left.observation.key, right.observation.key))
        })
        .map(|target| ResponseBulkLead {
            key: target.observation.key,
            snapshot: target.observation.snapshot,
            eta_ms: target.observation.eta_ms,
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
                left.observation
                    .eta_ms
                    .total_cmp(&right.observation.eta_ms)
                    .then_with(|| {
                        carrier_path_key_order(left.observation.key, right.observation.key)
                    })
            })
            .map(|target| ResponseBulkLead {
                key: target.observation.key,
                snapshot: target.observation.snapshot,
                eta_ms: target.observation.eta_ms,
            });
    }

    if lower_owner.is_none() {
        return candidate_targets
            .iter()
            .copied()
            .filter(|target| target.observation.has_bulk_rate_evidence)
            .min_by(|left, right| {
                left.observation
                    .eta_ms
                    .total_cmp(&right.observation.eta_ms)
                    .then_with(|| {
                        carrier_path_key_order(left.observation.key, right.observation.key)
                    })
            })
            .map(|target| ResponseBulkLead {
                key: target.observation.key,
                snapshot: target.observation.snapshot,
                eta_ms: target.observation.eta_ms,
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
        .filter(|target| !prefer_avoiding || !avoid_keys.contains(&target.observation.key))
        .min_by(|left, right| {
            left.observation
                .eta_ms
                .total_cmp(&right.observation.eta_ms)
                .then_with(|| carrier_path_key_order(left.observation.key, right.observation.key))
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
            !avoid_keys.contains(&target.observation.key)
                && target.observation.has_sender_evidence
                && avoid_keys
                    .iter()
                    .any(|avoid_key| avoid_key.underlay == target.observation.key.underlay)
        })
        .min_by(|left, right| {
            left.observation
                .eta_ms
                .total_cmp(&right.observation.eta_ms)
                .then_with(|| carrier_path_key_order(left.observation.key, right.observation.key))
        })
        .cloned()
}

pub(super) fn response_target_has_ack_gap_repair_evidence(
    target: &ResponseSenderPathTarget,
) -> bool {
    target.observation.is_service || target.observation.has_bulk_rate_evidence
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
        RelaySendCause::PersistentAckGapRepair => target.observation.has_bulk_rate_evidence,
        RelaySendCause::PersistentServerAckGapRepair(batch) => {
            target.observation.key == batch.target.key
                && target.observation.incarnation == batch.target.incarnation
                && target.observation.has_bulk_rate_evidence
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
        && let Some(target) = targets.iter().find(|target| {
            target.observation.key == lower_owner && !avoid_keys.contains(&target.observation.key)
        })
    {
        return Some(target.clone());
    }
    if let Some(active) = targets.iter().find(|target| {
        target.observation.is_service && !avoid_keys.contains(&target.observation.key)
    }) {
        return Some(active.clone());
    }
    let proven_targets = targets
        .iter()
        .filter(|target| target.observation.has_bulk_rate_evidence)
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
    let active_service_baseline = targets.iter().find(|target| target.observation.is_service);
    let repair = repair_cause.is_some();
    let path_failure_repair = matches!(repair_cause, Some(RelaySendCause::PathFailureRepair));
    let payload_bytes = reliable_stream_frame_accounted_bytes(frame);
    if !repair
        && matches!(frame, Frame::StreamData { .. })
        && lower_flights.iter().any(|flight| {
            !targets
                .iter()
                .any(|target| target.observation.key == flight.key)
        })
    {
        return None;
    }
    if !repair
        && emit_mode == CarrierEmitMode::StreamOrdered
        && !relay_frame_is_bulk_stream_data(frame, lane)
        && let Some(active) = targets.iter().find(|target| {
            target.observation.is_service && !avoid_keys.contains(&target.observation.key)
        })
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
        && let Some(active) = targets.iter().find(|target| {
            target.observation.is_request_active && !avoid_keys.contains(&target.observation.key)
        })
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
        .filter(|target| target.observation.is_service || target.observation.has_sender_evidence)
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
            let ordering_debt = response_ordering_debt_bytes(lower_flights, target.observation.key);
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
            let role = response_bulk_admission_role(
                service_key,
                target.observation.key,
                lower_owner,
                ordering_debt,
            );
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
            left.observation
                .eta_ms
                .total_cmp(&right.observation.eta_ms)
                .then_with(|| carrier_path_key_order(left.observation.key, right.observation.key))
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
    let (admission, subflow_set_commit, role, model_suppression) =
        response_target_unique_owner_admission_with_epoch(
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
        )
        .into_parts();
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
        if target.observation.attachment_role == StreamOpenRole::Repair {
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
    if lower_flights.iter().any(|flight| {
        !targets
            .iter()
            .any(|target| target.observation.key == flight.key)
    }) {
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
        .filter(|target| target.observation.is_service || target.observation.has_sender_evidence)
        .collect::<Vec<_>>();
    #[cfg(feature = "lab-diagnostics")]
    if !proven_targets.is_empty() {
        for target in &capacity_targets {
            if !target.observation.is_service && !target.observation.has_sender_evidence {
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
        targets
            .iter()
            .any(|target| target.observation.key == *owner)
            && (ordered_owner_debt_bytes > 0
                || capacity_targets.iter().any(|target| {
                    target.observation.key == *owner
                        && (target.observation.is_service
                            || target.observation.has_bulk_rate_evidence)
                }))
    });
    let live_service_anchor = ordered_data_owner
        .filter(|owner| {
            targets
                .iter()
                .any(|target| target.observation.key == *owner)
        })
        .or_else(|| {
            targets
                .iter()
                .find(|target| target.observation.is_service)
                .map(|target| target.observation.key)
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
        && let Some(service) = targets
            .iter()
            .find(|target| target.observation.key == service_key)
    {
        if ordered_owner_debt_bytes > 0 && effective_lower_owner.is_none() {
            #[cfg(feature = "lab-diagnostics")]
            for target in &candidate_targets {
                if target.observation.key != service_key
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
                target.observation.key == service_key
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
            .any(|target| target.observation.key == service_key);
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
                if target.observation.key != service_key
                    && target.observation.key.underlay != service_key.underlay
                {
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
                target.observation.key == service_key
                    || target.observation.key.underlay == service_key.underlay
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
        missing_owner_same_underlay_failover = candidate_targets.iter().any(|target| {
            target.observation.key.underlay == owner_underlay
                && target.observation.has_sender_evidence
        });
        if missing_owner_same_underlay_failover {
            #[cfg(feature = "lab-diagnostics")]
            for target in &candidate_targets {
                if target.observation.key.underlay != owner_underlay
                    || !target.observation.has_sender_evidence
                {
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
                target.observation.key.underlay == owner_underlay
                    && target.observation.has_sender_evidence
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
            Some(target.observation.key) == effective_lower_owner
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
            if !mixed_safe_targets
                .iter()
                .any(|safe| safe.observation.key == target.observation.key)
            {
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
        && !candidate_targets
            .iter()
            .any(|target| target.observation.is_service);
    let service_baseline = service_anchor.and_then(|service_key| {
        targets
            .iter()
            .find(|target| target.observation.key == service_key)
    });
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
        // TCP supplies its protocol-specific target and commit; the planner
        // adds only the shared ownership metadata used by dispatch.
        let calibration = ResponseSelectedDataTarget {
            target: calibration.target,
            admission: PathAdmission::subflow_owner(PathRuntimeRole::Subflow),
            service_handoff_commit: None,
            subflow_set_commit: None,
            ack_clock_calibration_commit: Some(calibration.commit),
        };
        #[cfg(feature = "lab-diagnostics")]
        lab_response_bulk_output_selected(
            "ack_clock_calibration",
            &calibration.target,
            calibration.admission,
            payload_bytes,
        );
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
        .find(|target| target.observation.key == service_key);
    let mut admitted = Vec::with_capacity(candidate_targets.len());
    if let Some(target) = service_target {
        let ordering_debt = response_ordering_debt_bytes(lower_flights, target.observation.key);
        if let Some(selected) = admit_response_data_target(
            target,
            &candidate_targets,
            subflow_set,
            &admission_policy,
            ordering_debt,
            ordered_tail.for_candidate(target.observation.key),
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
            response_feedable_service_owner_target(&admitted).and_then(|service| {
                response_same_family_reservoir_for_service(
                    &service.target,
                    ordered_tail,
                    payload_bytes,
                    mux_limits,
                )
            })
        })
        .flatten();
    for target in candidate_targets
        .iter()
        .copied()
        .filter(|target| target.observation.key != service_key)
    {
        let ordering_debt = response_ordering_debt_bytes(lower_flights, target.observation.key);
        let candidate_debt = same_family_reservoir
            .filter(|reservoir| {
                response_target_is_same_family_reservoir_candidate(*reservoir, target)
            })
            .map_or_else(
                || ordered_tail.for_candidate(target.observation.key),
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
            &subflow_target.target,
            subflow_target.admission,
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
                .observation
                .eta_ms
                .total_cmp(&right.target.observation.eta_ms)
                .then_with(|| {
                    carrier_path_key_order(
                        left.target.observation.key,
                        right.target.observation.key,
                    )
                })
        })
        .cloned()
    {
        #[cfg(feature = "lab-diagnostics")]
        lab_response_bulk_output_selected(
            "startup_sample",
            &startup.target,
            startup.admission,
            payload_bytes,
        );
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
        lab_response_bulk_output_selected(
            "service_first",
            &service_target.target,
            service_target.admission,
            payload_bytes,
        );
        return Some(service_target);
    }
    let best = admitted.iter().min_by(|left, right| {
        left.target
            .observation
            .eta_ms
            .total_cmp(&right.target.observation.eta_ms)
            .then_with(|| {
                carrier_path_key_order(left.target.observation.key, right.target.observation.key)
            })
    })?;
    if lower_owner.is_none()
        && let Some(lead_key) = ordered_data_owner
        && let Some(lead_target) = admitted
            .iter()
            .find(|selected| selected.target.observation.key == lead_key)
        && response_target_within_adaptive_lead_hysteresis(
            &lead_target.target,
            &best.target,
            payload_bytes,
        )
    {
        #[cfg(feature = "lab-diagnostics")]
        lab_response_bulk_output_selected(
            "hysteresis",
            &lead_target.target,
            lead_target.admission,
            payload_bytes,
        );
        return Some(lead_target.clone());
    }
    #[cfg(feature = "lab-diagnostics")]
    lab_response_bulk_output_selected("best_eta", &best.target, best.admission, payload_bytes);
    Some(best.clone())
}

pub(super) fn response_target_within_adaptive_lead_hysteresis(
    old_lead: &ResponseSenderPathTarget,
    best: &ResponseSenderPathTarget,
    payload_bytes: usize,
) -> bool {
    if old_lead.observation.key == best.observation.key {
        return true;
    }
    path_within_adaptive_lead_hysteresis(
        old_lead.observation.eta_ms,
        old_lead.observation.snapshot,
        best.observation.eta_ms,
        best.observation.snapshot,
        payload_bytes,
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
                .then_with(|| {
                    left.target
                        .observation
                        .eta_ms
                        .total_cmp(&right.target.observation.eta_ms)
                })
                .then_with(|| {
                    carrier_path_key_order(
                        left.target.observation.key,
                        right.target.observation.key,
                    )
                })
        })
        .cloned()
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
        .find(|selected| selected.target.observation.key == reservoir.service())?;
    admitted
        .iter()
        .filter(|selected| {
            selected.admission.role == PathRuntimeRole::Subflow
                && response_target_is_same_family_reservoir_candidate(reservoir, &selected.target)
                // Separate QUIC connections own independent congestion
                // controllers. Crossing into an equally loaded connection
                // only creates product reordering; require real load relief.
                && (selected.target.observation.key.underlay != UnderlayProtocol::Udp
                    || response_target_active_bulk_flows(&service.target)
                        > response_target_active_bulk_flows(&selected.target))
                && selected
                    .subflow_set_commit
                    .is_some_and(|commit| commit.service == reservoir.service())
        })
        .min_by(|left, right| {
            left.target
                .observation
                .eta_ms
                .total_cmp(&right.target.observation.eta_ms)
                .then_with(|| {
                    response_target_assigned_product_bytes(&left.target)
                        .cmp(&response_target_assigned_product_bytes(&right.target))
                })
                .then_with(|| {
                    carrier_path_key_order(
                        left.target.observation.key,
                        right.target.observation.key,
                    )
                })
        })
        .cloned()
}

pub(super) fn response_target_active_bulk_flows(target: &ResponseSenderPathTarget) -> u32 {
    target
        .observation
        .snapshot
        .active_flows
        .saturating_sub(target.observation.snapshot.active_latency_sensitive_flows)
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
    match evaluate_response_data_dispatch_with_ordered_debt(
        stream,
        relay_lane,
        next_offset,
        payload_bytes,
        ordered_owner_debt_bytes,
        ResponsePlanningMode::Apply,
    )? {
        ResponseDataPlanningOutcome::Dispatch(planned) => Ok(planned),
        ResponseDataPlanningOutcome::ApplyRequired => {
            unreachable!("apply mode resolves maintenance before returning")
        }
    }
}

fn evaluate_response_data_dispatch_with_ordered_debt(
    stream: &ReliablePathStream,
    relay_lane: FlowLane,
    next_offset: u64,
    payload_bytes: usize,
    ordered_owner_debt_bytes: usize,
    mode: ResponsePlanningMode,
) -> Result<ResponseDataPlanningOutcome, RuntimeError> {
    match &stream.output {
        ReliablePathStreamOutput::Fixed(fixed) => {
            let lane = reliable_work_lane_to_carrier_lane(ReliableWorkClass::Data, relay_lane);
            if fixed.commands().can_enqueue_lane_now(lane) {
                Ok(ResponseDataPlanningOutcome::Dispatch(
                    ResponseDataDispatchPlan {
                        primary: ResponseDataDispatchTarget::Fixed(fixed.clone()),
                    },
                ))
            } else {
                Err(RuntimeError::SenderServiceBlocked)
            }
        }
        ReliablePathStreamOutput::Switchable(binding) => {
            let mut may_resnapshot_after_retirement = true;
            loop {
                if mode == ResponsePlanningMode::Apply
                    && binding.maintain_response_session_operations()
                {
                    continue;
                }
                let (planner_generation, subflow_set) = binding.subflow_state_snapshot();
                let session_scheduling = binding.response_scheduling_snapshot();
                if mode == ResponsePlanningMode::Preview
                    && session_scheduling.operation_maintenance_due
                {
                    return Ok(ResponseDataPlanningOutcome::ApplyRequired);
                }
                let lane_generation = session_scheduling.generation;
                let active_response_flows = session_scheduling.active_response_flows;
                let model_generation = binding.response_model_generation();
                let lower_flights = binding.lower_flights_before_offset(next_offset);
                let targets = binding.sender_path_targets(relay_lane, payload_bytes);
                let ordered_data_owner = binding.ordered_data_owner();
                if mode == ResponsePlanningMode::Preview
                    && select_response_tcp_capacity_probe_start(
                        &targets,
                        relay_lane,
                        ordered_data_owner,
                        session_scheduling.service_family_loads,
                        binding.mux_limits(),
                        session_scheduling.tcp_capacity_probe_reserved,
                    )
                    .is_some()
                {
                    return Ok(ResponseDataPlanningOutcome::ApplyRequired);
                }
                if mode == ResponsePlanningMode::Apply
                    && try_start_response_tcp_capacity_probe(
                        binding,
                        &targets,
                        relay_lane,
                        ordered_data_owner,
                        session_scheduling.service_family_loads,
                        lane_generation,
                        session_scheduling.tcp_capacity_probe_reserved,
                    )?
                {
                    // Reservation advances the shared generation; resnapshot
                    // before any product decision.
                    continue;
                }
                if mode == ResponsePlanningMode::Preview
                    && select_response_quic_capacity_calibration_start(
                        &targets,
                        relay_lane,
                        ordered_data_owner,
                        session_scheduling.service_family_loads,
                        binding.mux_limits(),
                        active_response_flows,
                        session_scheduling.quic_capacity_calibration_reserved,
                        session_scheduling.quic_capacity_calibration_spent_bytes,
                        session_scheduling.response_service_handoff_drain.is_some(),
                    )
                    .is_some()
                {
                    return Ok(ResponseDataPlanningOutcome::ApplyRequired);
                }
                if mode == ResponsePlanningMode::Apply
                    && try_start_response_quic_capacity_calibration(
                        binding,
                        &targets,
                        relay_lane,
                        ordered_data_owner,
                        session_scheduling.service_family_loads,
                        active_response_flows,
                        planner_generation,
                        lane_generation,
                        model_generation,
                        session_scheduling.quic_capacity_calibration_reserved,
                        session_scheduling.quic_capacity_calibration_spent_bytes,
                        session_scheduling.response_service_handoff_drain.is_some(),
                    )
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
                    lab_response_bulk_output_selected(
                        "service_handoff",
                        &selected.target,
                        selected.admission,
                        payload_bytes,
                    );
                    return Ok(ResponseDataPlanningOutcome::Dispatch(
                        ResponseDataDispatchPlan {
                            primary: ResponseDataDispatchTarget::Switchable {
                                binding: binding.clone(),
                                target: selected.target.into(),
                                role: PathRuntimeRole::Service,
                                service_handoff_commit: selected.service_handoff_commit,
                                subflow_set_commit: None,
                                ack_clock_calibration_commit: None,
                            },
                        },
                    ));
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
                    if mode == ResponsePlanningMode::Preview {
                        return Ok(ResponseDataPlanningOutcome::ApplyRequired);
                    }
                    binding.cancel_response_service_handoff_drain("eligibility_regressed");
                    continue;
                }
                if let Some(candidate) = handoff_candidate {
                    if mode == ResponsePlanningMode::Preview {
                        return Ok(ResponseDataPlanningOutcome::ApplyRequired);
                    }
                    let lower_flight_bytes = lower_flights
                        .iter()
                        .fold(0u64, |total, flight| total.saturating_add(flight.bytes));
                    let outstanding_owner_bytes = u64::try_from(ordered_owner_debt_bytes)
                        .unwrap_or(u64::MAX)
                        .max(lower_flight_bytes)
                        .max(candidate.service.observation.owner_data_in_flight_bytes);
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
                            service: candidate.service.observation.key,
                            service_path_instance_id: candidate
                                .service
                                .observation
                                .path_instance_id,
                            service_incarnation: candidate.service.observation.incarnation,
                            target: candidate.target.observation.key,
                            target_path_instance_id: candidate.target.observation.path_instance_id,
                            target_incarnation: candidate.target.observation.incarnation,
                            mode: candidate.mode,
                            capacity_proof: response_service_handoff_start_capacity_proof(
                                &candidate.target,
                                session_scheduling.observed_at,
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
                if mode == ResponsePlanningMode::Preview && !retirement_intents.is_empty() {
                    return Ok(ResponseDataPlanningOutcome::ApplyRequired);
                }
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
                        || target.observation.has_bulk_rate_evidence
                        || selected
                            .subflow_set_commit
                            .is_some_and(|commit| commit.input.startup_owner_allowed),
                    "Subflow OwnerData requires bulk-rate evidence or explicit bounded startup admission: target={:?} role={:?} ordered_owner={:?} lower_owner={:?} is_active={} sender_evidence={} bulk_evidence={}",
                    target.observation.key,
                    role,
                    ordered_data_owner,
                    response_oldest_lower_flight_owner(&lower_flights),
                    target.observation.is_service,
                    target.observation.has_sender_evidence,
                    target.observation.has_bulk_rate_evidence,
                );
                return Ok(ResponseDataPlanningOutcome::Dispatch(
                    ResponseDataDispatchPlan {
                        primary: ResponseDataDispatchTarget::Switchable {
                            binding: binding.clone(),
                            target: target.into(),
                            role,
                            service_handoff_commit: selected.service_handoff_commit,
                            subflow_set_commit: selected.subflow_set_commit,
                            ack_clock_calibration_commit: selected.ack_clock_calibration_commit,
                        },
                    },
                ));
            }
        }
    }
}

/// Readiness must not advance protocol generations or reserve carrier work.
pub(super) fn preview_response_data_payload_with_ordered_debt(
    path_stream: &ReliablePathStream,
    relay_lane: FlowLane,
    next_offset: u64,
    payload_bytes: usize,
    ordered_owner_debt_bytes: usize,
) -> bool {
    let calibration_remaining = match &path_stream.output {
        ReliablePathStreamOutput::Switchable(binding) => {
            binding.active_tcp_ack_clock_calibration_remaining_bytes()
        }
        ReliablePathStreamOutput::Fixed(_) => None,
    };
    if let Some(remaining) = calibration_remaining {
        let calibration_payload_bytes = payload_bytes.min(remaining);
        match evaluate_response_data_dispatch_with_ordered_debt(
            path_stream,
            relay_lane,
            next_offset,
            calibration_payload_bytes,
            ordered_owner_debt_bytes,
            ResponsePlanningMode::Preview,
        ) {
            Ok(ResponseDataPlanningOutcome::ApplyRequired) => return true,
            Ok(ResponseDataPlanningOutcome::Dispatch(planned))
                if response_plan_is_ack_clock_calibration(&planned) =>
            {
                return true;
            }
            Ok(ResponseDataPlanningOutcome::Dispatch(_))
                if calibration_payload_bytes == payload_bytes =>
            {
                return true;
            }
            Err(_) if calibration_payload_bytes == payload_bytes => return false,
            Ok(ResponseDataPlanningOutcome::Dispatch(_)) | Err(_) => {}
        }
    }

    evaluate_response_data_dispatch_with_ordered_debt(
        path_stream,
        relay_lane,
        next_offset,
        payload_bytes,
        ordered_owner_debt_bytes,
        ResponsePlanningMode::Preview,
    )
    .is_ok()
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
