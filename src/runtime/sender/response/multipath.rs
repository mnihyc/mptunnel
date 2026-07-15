//! Serialized response multipath lifecycle transaction.
//!
//! Durable state stays in the response binding, session, and concrete carrier
//! controllers. This module owns the ordered maintain/observe/apply loop that
//! turns one immutable ranking result into an executable dispatch plan.

#[cfg(feature = "lab-diagnostics")]
use super::diagnostics::{
    lab_response_bulk_output_selected, lab_response_service_handoff_evaluation,
};
use super::planner::{
    ResponseDataSelectionIntent, ResponseSelectedDataTarget, ResponseServiceHandoffSelection,
    response_service_handoff_drain_lease, response_service_handoff_drain_matches_candidate,
    response_service_handoff_drain_matches_selection,
    response_service_handoff_start_capacity_proof,
    select_response_sender_data_target_with_ordered_debt_inner_and_retirements,
    select_response_service_handoff_candidate, select_response_service_handoff_target,
};
use super::quic_capacity::{
    select_response_quic_capacity_calibration_start, try_start_response_quic_capacity_calibration,
};
use super::tcp_capacity::{
    ResponseAckClockCalibrationRetirementSelection, select_response_tcp_capacity_probe_start,
    try_start_response_tcp_capacity_probe,
};
use crate::model::multipath::{FlowSubflowSet, PathRuntimeRole};
use crate::model::path::CarrierPathKey;
use crate::model::response::response_oldest_lower_flight_owner;
use crate::model::work::ReliableWorkClass;
use crate::runtime::RuntimeError;
use crate::runtime::stream::response::{
    MIN_ACTIVE_RESPONSE_FLOWS_FOR_SAME_FAMILY_DISCOVERY, ResponseAckClockCalibrationRequest,
    ResponseAckClockCalibrationRetirementRequest, ResponseDispatchTarget,
    ResponseServiceHandoffDrainRequest, ResponseServiceHandoffRequest,
    ResponseSubflowAdmissionRequest,
};
use crate::runtime::stream::{
    ReliablePathStream, ReliablePathStreamOutput, reliable_work_lane_to_carrier_lane,
};
use crate::scheduler::FlowLane;

/// Optimistic-concurrency fence associated with one planning pass. Each owner
/// revalidates its generation, so this is not an atomic state snapshot.
#[derive(Clone, Copy)]
struct ResponsePlanningEpoch {
    planner_generation: u64,
    lane_generation: u64,
    model_generation: u64,
}

/// Executable response ownership transition. Reservation and handoff variants
/// carry their generation fences; plain Service is re-elected during apply.
pub(super) enum ResponseDataDispatchIntent {
    Service,
    SubflowAdmission(ResponseSubflowAdmissionRequest),
    AckClockCalibration(ResponseAckClockCalibrationRequest),
    ServiceHandoff(ResponseStampedServiceHandoff),
}

pub(super) struct ResponseStampedServiceHandoff {
    // Preserve the planner's rare allocation; dispatch projects the complete
    // request on its stack instead of freeing and boxing it a second time.
    epoch: ResponsePlanningEpoch,
    selection: Box<ResponseServiceHandoffSelection>,
}

impl ResponseStampedServiceHandoff {
    pub(super) fn into_request(
        self,
        target: &ResponseDispatchTarget,
    ) -> ResponseServiceHandoffRequest {
        ResponseServiceHandoffRequest {
            expected_planner_generation: self.epoch.planner_generation,
            expected_lane_generation: self.epoch.lane_generation,
            expected_model_generation: self.epoch.model_generation,
            handoff_frontier: self.selection.handoff_frontier,
            service: self.selection.service,
            service_path_instance_id: self.selection.service_path_instance_id,
            service_incarnation: self.selection.service_incarnation,
            target: target.key,
            target_path_instance_id: target.path_instance_id,
            target_incarnation: target.incarnation,
            mode: self.selection.mode,
            target_command_pending_limit_bytes: self.selection.target_command_pending_limit_bytes,
            capacity_proof: self.selection.capacity_proof,
        }
    }
}

impl ResponseDataDispatchIntent {
    pub(super) fn role(&self) -> PathRuntimeRole {
        match self {
            Self::Service | Self::ServiceHandoff(_) => PathRuntimeRole::Service,
            Self::SubflowAdmission(_) | Self::AckClockCalibration(_) => PathRuntimeRole::Subflow,
        }
    }
}

fn stamp_response_data_selection(
    selected: ResponseSelectedDataTarget,
    epoch: ResponsePlanningEpoch,
) -> (ResponseDispatchTarget, ResponseDataDispatchIntent) {
    let (target, selection) = selected.into_parts();
    let intent = match selection {
        ResponseDataSelectionIntent::Service => ResponseDataDispatchIntent::Service,
        ResponseDataSelectionIntent::SubflowAdmission(selection) => {
            ResponseDataDispatchIntent::SubflowAdmission(ResponseSubflowAdmissionRequest {
                expected_planner_generation: epoch.planner_generation,
                expected_lane_generation: epoch.lane_generation,
                service: selection.service,
                startup_owner_credit_bytes: selection.startup_owner_credit_bytes,
                optional_overhead_budget_bytes: selection.optional_overhead_budget_bytes,
                max_read_gap_budget: selection.max_read_gap_budget,
                input: selection.input,
            })
        }
        ResponseDataSelectionIntent::AckClockCalibration(selection) => {
            ResponseDataDispatchIntent::AckClockCalibration(ResponseAckClockCalibrationRequest {
                expected_planner_generation: epoch.planner_generation,
                expected_lane_generation: epoch.lane_generation,
                expected_model_generation: epoch.model_generation,
                service: selection.service,
                service_incarnation: selection.service_incarnation,
                service_pending_bytes: selection.service_pending_bytes,
                target_pending_bytes: selection.target_pending_bytes,
                limit_bytes: selection.limit_bytes,
                requires_active_response_start: selection.requires_active_response_start,
            })
        }
        ResponseDataSelectionIntent::ServiceHandoff(selection) => {
            ResponseDataDispatchIntent::ServiceHandoff(ResponseStampedServiceHandoff {
                epoch,
                selection,
            })
        }
    };
    (target.into(), intent)
}

fn stamp_response_calibration_retirement(
    selection: ResponseAckClockCalibrationRetirementSelection,
    epoch: ResponsePlanningEpoch,
) -> ResponseAckClockCalibrationRetirementRequest {
    ResponseAckClockCalibrationRetirementRequest {
        expected_planner_generation: epoch.planner_generation,
        expected_lane_generation: epoch.lane_generation,
        expected_model_generation: epoch.model_generation,
        service: selection.service,
        service_incarnation: selection.service_incarnation,
        service_pending_bytes: selection.service_pending_bytes,
        target: selection.target,
        target_incarnation: selection.target_incarnation,
        target_pending_bytes: selection.target_pending_bytes,
        limit_bytes: selection.limit_bytes,
    }
}

pub(super) enum ResponseDataDispatchTarget {
    Fixed {
        key: CarrierPathKey,
    },
    Switchable {
        target: ResponseDispatchTarget,
        intent: ResponseDataDispatchIntent,
    },
}

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
    pub(super) fn primary_key(&self) -> Option<CarrierPathKey> {
        match &self.primary {
            ResponseDataDispatchTarget::Fixed { key } => Some(*key),
            ResponseDataDispatchTarget::Switchable { target, .. } => Some(target.key),
        }
    }

    #[cfg(test)]
    pub(super) fn primary_role(&self) -> PathRuntimeRole {
        match &self.primary {
            ResponseDataDispatchTarget::Fixed { .. } => PathRuntimeRole::Service,
            ResponseDataDispatchTarget::Switchable { intent, .. } => intent.role(),
        }
    }
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
                        primary: ResponseDataDispatchTarget::Fixed { key: fixed.key() },
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
                let planning_epoch = ResponsePlanningEpoch {
                    planner_generation,
                    lane_generation,
                    model_generation,
                };
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
                    && let Some(selected) = select_response_service_handoff_target(
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
                        session_scheduling.observed_at,
                    )
                {
                    debug_assert!(current_drain.is_none_or(|reservation| {
                        response_service_handoff_drain_matches_selection(
                            binding_instance_id,
                            reservation,
                            &selected,
                        )
                    }));
                    #[cfg(feature = "lab-diagnostics")]
                    lab_response_bulk_output_selected(
                        "service_handoff",
                        selected.target(),
                        selected.admission(),
                        payload_bytes,
                    );
                    let (target, intent) = stamp_response_data_selection(selected, planning_epoch);
                    debug_assert!(matches!(
                        &intent,
                        ResponseDataDispatchIntent::ServiceHandoff(_)
                    ));
                    return Ok(ResponseDataPlanningOutcome::Dispatch(
                        ResponseDataDispatchPlan {
                            primary: ResponseDataDispatchTarget::Switchable { target, intent },
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
                            session_scheduling.observed_at,
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
                        .max(candidate.service().observation.owner_data_in_flight_bytes);
                    let lease = response_service_handoff_drain_lease(
                        candidate.service(),
                        outstanding_owner_bytes,
                    );
                    if binding.try_start_response_service_handoff_drain(
                        candidate.service(),
                        candidate.target(),
                        relay_lane,
                        ResponseServiceHandoffDrainRequest {
                            expected_planner_generation: planner_generation,
                            expected_lane_generation: lane_generation,
                            expected_model_generation: model_generation,
                            service: candidate.service().observation.key,
                            service_path_instance_id: candidate
                                .service()
                                .observation
                                .path_instance_id,
                            service_incarnation: candidate.service().observation.incarnation,
                            target: candidate.target().observation.key,
                            target_path_instance_id: candidate
                                .target()
                                .observation
                                .path_instance_id,
                            target_incarnation: candidate.target().observation.incarnation,
                            mode: candidate.mode(),
                            capacity_proof: response_service_handoff_start_capacity_proof(
                                candidate.target(),
                                session_scheduling.observed_at,
                            ),
                            outstanding_owner_bytes,
                            lease,
                        },
                    ) {
                        return Err(RuntimeError::SenderServiceBlocked);
                    }
                }
                let mut retirement_selections = Vec::new();
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
                        &mut retirement_selections,
                    );
                let mut retired_any = false;
                if mode == ResponsePlanningMode::Preview && !retirement_selections.is_empty() {
                    return Ok(ResponseDataPlanningOutcome::ApplyRequired);
                }
                if may_resnapshot_after_retirement {
                    for selection in retirement_selections {
                        retired_any |= binding.try_retire_tcp_ack_clock_calibration(
                            stamp_response_calibration_retirement(selection, planning_epoch),
                        );
                    }
                }
                if retired_any {
                    // Retirement invalidates the planner generation. Recompute
                    // once so the resulting Service/reservoir plan uses the tombstone.
                    may_resnapshot_after_retirement = false;
                    continue;
                }
                let Some(selected) = selected else {
                    return Err(RuntimeError::SenderServiceBlocked);
                };
                let role = selected.admission().role;
                let target = selected.target();
                debug_assert!(
                    role != PathRuntimeRole::Subflow
                        || target.observation.has_bulk_rate_evidence
                        || selected
                            .subflow_admission_selection()
                            .is_some_and(|selection| selection.input.startup_owner_allowed),
                    "Subflow OwnerData requires bulk-rate evidence or explicit bounded startup admission: target={:?} role={:?} ordered_owner={:?} lower_owner={:?} is_active={} sender_evidence={} bulk_evidence={}",
                    target.observation.key,
                    role,
                    ordered_data_owner,
                    response_oldest_lower_flight_owner(&lower_flights),
                    target.observation.is_service,
                    target.observation.has_sender_evidence,
                    target.observation.has_bulk_rate_evidence,
                );
                if selected.service_handoff_selection().is_some() {
                    return Err(RuntimeError::SenderServiceBlocked);
                }
                let (target, intent) = stamp_response_data_selection(selected, planning_epoch);
                return Ok(ResponseDataPlanningOutcome::Dispatch(
                    ResponseDataDispatchPlan {
                        primary: ResponseDataDispatchTarget::Switchable { target, intent },
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
            intent: ResponseDataDispatchIntent::AckClockCalibration(_),
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

#[cfg(test)]
#[path = "multipath_test.rs"]
mod tests;
