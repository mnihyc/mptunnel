use super::*;
use crate::model::admission::BulkAdmissionRole;
use crate::runtime::stream::response::{
    CarrierPathFlightDebt, ResponseSenderPathTarget, ResponseServiceFamilyLoads,
    ResponseServiceHandoffDrainReservation, ResponseStreamBinding,
    valid_quic_capacity_proof_candidate_at, well_formed_quic_capacity_proof_candidate,
};

// Why separate: causal lab observability mirrors product gates, but it must
// remain an observer rather than inflate or become a second hot-path policy.

#[derive(Clone, Copy)]
struct ResponseServiceHandoffTargetEvaluation<'a> {
    target: &'a ResponseSenderPathTarget,
    gate_stage: u8,
    first_failed_gate: &'static str,
    model_suppression: Option<&'static str>,
}

#[derive(Clone, Copy)]
pub(super) struct ResponseServiceHandoffEvaluation<'a> {
    pub(super) service: Option<&'a ResponseSenderPathTarget>,
    pub(super) target: Option<&'a ResponseSenderPathTarget>,
    pub(super) first_failed_gate: &'static str,
    model_suppression: Option<&'static str>,
}

fn response_quic_capacity_marker_state(
    target: &ResponseSenderPathTarget,
    now: Instant,
) -> &'static str {
    if target.key.underlay != UnderlayProtocol::Udp {
        return "not_udp";
    }
    match target.quic_capacity_proof {
        None => "missing",
        Some(proof) if !well_formed_quic_capacity_proof_candidate(proof) => "invalid",
        Some(proof) if now >= proof.expires_at => "expired",
        Some(_) => "current",
    }
}

pub(super) fn response_service_handoff_diagnostic_target_view(
    service: &ResponseSenderPathTarget,
    target: &ResponseSenderPathTarget,
    lane: FlowLane,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    required_reservation: Option<ResponseServiceHandoffDrainReservation>,
    now: Instant,
) -> Option<ResponseSenderPathTarget> {
    response_service_handoff_target_view(
        target,
        service.key,
        lane,
        payload_bytes,
        mux_limits,
        required_reservation,
        now,
    )
}

fn evaluate_response_service_handoff_target<'a>(
    target: &'a ResponseSenderPathTarget,
    service: &ResponseSenderPathTarget,
    lane: FlowLane,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    service_family_loads: ResponseServiceFamilyLoads,
    required_reservation: Option<ResponseServiceHandoffDrainReservation>,
    now: Instant,
) -> ResponseServiceHandoffTargetEvaluation<'a> {
    let failed = |gate_stage, first_failed_gate| ResponseServiceHandoffTargetEvaluation {
        target,
        gate_stage,
        first_failed_gate,
        model_suppression: None,
    };
    let Some(target_view) = response_service_handoff_target_view(
        target,
        service.key,
        lane,
        payload_bytes,
        mux_limits,
        required_reservation,
        now,
    ) else {
        return failed(0, "drain_target_changed");
    };
    let effective_target = &target_view;
    if effective_target.key.underlay == service.key.underlay {
        return failed(1, "same_carrier_family");
    }
    if effective_target.attachment_role != StreamOpenRole::Validation {
        return failed(2, "target_role");
    }
    if effective_target.is_active {
        return failed(3, "target_is_service");
    }
    if !effective_target.has_bulk_rate_evidence {
        let gate = match response_quic_capacity_marker_state(target, now) {
            "expired" => "target_proof_expired",
            "invalid" => "target_proof_invalid",
            _ => "target_proof_missing",
        };
        return failed(4, gate);
    }
    if effective_target.owner_data_in_flight_bytes != 0 {
        return failed(5, "target_owner_flight");
    }
    if effective_target.snapshot.product_bytes_in_flight != 0 {
        return failed(6, "target_product_flight");
    }
    if effective_target.snapshot.active_latency_sensitive_flows != 0 {
        return failed(7, "target_path_latency_load");
    }
    if effective_target
        .snapshot
        .session_active_latency_sensitive_flows
        != 0
    {
        return failed(8, "target_session_latency_load");
    }
    if response_service_handoff_mode_for_targets(service, effective_target, service_family_loads)
        .is_none()
    {
        return failed(9, "family_or_gain");
    }
    if !effective_target.commands.can_enqueue_lane_now(lane) {
        return failed(10, "target_queue_slot");
    }
    let model_suppression = response_owner_bulk_model_suppression(
        effective_target,
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
    );
    if let Some(reason) = model_suppression {
        return ResponseServiceHandoffTargetEvaluation {
            target,
            gate_stage: 11,
            first_failed_gate: "model_suppression",
            model_suppression: Some(reason),
        };
    }
    if !response_target_has_emission_credit(effective_target, lane, payload_bytes, mux_limits) {
        return failed(12, "emission_credit");
    }
    ResponseServiceHandoffTargetEvaluation {
        target,
        gate_stage: 13,
        first_failed_gate: "eligible",
        model_suppression: None,
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn evaluate_response_service_handoff<'a>(
    targets: &'a [ResponseSenderPathTarget],
    lane: FlowLane,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    lower_flights: &[CarrierPathFlightDebt],
    ordered_data_owner: Option<CarrierPathKey>,
    ordered_owner_debt_bytes: usize,
    service_family_loads: ResponseServiceFamilyLoads,
    required_reservation: Option<ResponseServiceHandoffDrainReservation>,
    handoff_open: bool,
    startup_owner_active: bool,
    calibration_active: bool,
    another_binding_is_draining: bool,
    now: Instant,
) -> ResponseServiceHandoffEvaluation<'a> {
    let service =
        ordered_data_owner.and_then(|key| targets.iter().find(|target| target.key == key));
    let preferred_target = service.and_then(|service| {
        targets
            .iter()
            .filter(|target| target.key.underlay != service.key.underlay)
            .min_by(|left, right| {
                let marker_rank =
                    |target: &ResponseSenderPathTarget| match response_quic_capacity_marker_state(
                        target, now,
                    ) {
                        "current" => 0,
                        "expired" => 1,
                        "invalid" => 2,
                        _ => 3,
                    };
                marker_rank(left)
                    .cmp(&marker_rank(right))
                    .then_with(|| left.eta_ms.total_cmp(&right.eta_ms))
                    .then_with(|| carrier_path_key_order(left.key, right.key))
            })
    });
    let blocked = |first_failed_gate| ResponseServiceHandoffEvaluation {
        service,
        target: preferred_target,
        first_failed_gate,
        model_suppression: None,
    };
    if !handoff_open {
        return blocked("handoff_closed");
    }
    if startup_owner_active {
        return blocked("startup_owner_active");
    }
    if calibration_active {
        return blocked("calibration_active");
    }
    if another_binding_is_draining {
        return blocked("another_binding_draining");
    }
    if !lane.is_bulk() {
        return blocked("non_bulk_lane");
    }
    let Some(service_key) = ordered_data_owner else {
        return blocked("no_frontier");
    };
    let Some(service) = targets.iter().find(|target| target.key == service_key) else {
        return blocked("service_missing");
    };
    if required_reservation.is_some_and(|reservation| {
        reservation.service != service.key
            || reservation.service_path_instance_id != service.path_instance_id
            || reservation.service_incarnation != service.incarnation
    }) {
        return blocked("drain_service_changed");
    }
    if !service.is_active {
        return blocked("service_not_active");
    }
    if !service.has_bulk_rate_evidence {
        return blocked("service_proof_missing");
    }
    if service.snapshot.active_latency_sensitive_flows != 0 {
        return blocked("service_path_latency_load");
    }
    if service.snapshot.session_active_latency_sensitive_flows != 0 {
        return blocked("service_session_latency_load");
    }

    let mut best_failed = None::<ResponseServiceHandoffTargetEvaluation<'_>>;
    let mut best_eligible = None::<ResponseServiceHandoffTargetEvaluation<'_>>;
    for target in targets {
        let evaluation = evaluate_response_service_handoff_target(
            target,
            service,
            lane,
            payload_bytes,
            mux_limits,
            service_family_loads,
            required_reservation,
            now,
        );
        if evaluation.first_failed_gate == "eligible" {
            if best_eligible.is_none_or(|current| {
                evaluation
                    .target
                    .eta_ms
                    .total_cmp(&current.target.eta_ms)
                    .then_with(|| carrier_path_key_order(evaluation.target.key, current.target.key))
                    .is_lt()
            }) {
                best_eligible = Some(evaluation);
            }
        } else if best_failed.is_none_or(|current| {
            evaluation.gate_stage > current.gate_stage
                || (evaluation.gate_stage == current.gate_stage
                    && evaluation.target.quic_capacity_proof.is_some()
                    && current.target.quic_capacity_proof.is_none())
        }) {
            best_failed = Some(evaluation);
        }
    }
    let Some(candidate) = best_eligible.or(best_failed) else {
        return blocked("target_missing");
    };
    if candidate.first_failed_gate != "eligible" {
        return ResponseServiceHandoffEvaluation {
            service: Some(service),
            target: Some(candidate.target),
            first_failed_gate: candidate.first_failed_gate,
            model_suppression: candidate.model_suppression,
        };
    }
    if ordered_owner_debt_bytes != 0 || !lower_flights.is_empty() {
        return ResponseServiceHandoffEvaluation {
            service: Some(service),
            target: Some(candidate.target),
            first_failed_gate: "frontier_not_clear",
            model_suppression: None,
        };
    }
    ResponseServiceHandoffEvaluation {
        service: Some(service),
        target: Some(candidate.target),
        first_failed_gate: "eligible",
        model_suppression: None,
    }
}

fn response_service_handoff_capacity_marker_signature(
    targets: &[ResponseSenderPathTarget],
    now: Instant,
) -> u64 {
    use std::hash::{Hash, Hasher};

    let mut signature = std::collections::hash_map::DefaultHasher::new();
    for target in targets {
        let Some(proof) = target.quic_capacity_proof else {
            continue;
        };
        target.key.path_id.0.hash(&mut signature);
        (target.key.underlay == UnderlayProtocol::Udp).hash(&mut signature);
        target.path_instance_id.as_u64().hash(&mut signature);
        proof.token.hash(&mut signature);
        (now >= proof.expires_at).hash(&mut signature);
    }
    signature.finish()
}

pub(super) fn response_service_handoff_evaluation_signature(
    evaluation: ResponseServiceHandoffEvaluation<'_>,
    service_family_loads: ResponseServiceFamilyLoads,
) -> u64 {
    use std::hash::{Hash, Hasher};

    let mut signature = std::collections::hash_map::DefaultHasher::new();
    evaluation.first_failed_gate.hash(&mut signature);
    evaluation.model_suppression.hash(&mut signature);
    // The gate name is the causal state. Hash identities and family policy, but
    // leave quantitative queue/rate churn to the bounded one-second refresh.
    // This observer must not become per-frame work while an upstream gate holds.
    service_family_loads
        .for_underlay(UnderlayProtocol::Tcp)
        .hash(&mut signature);
    service_family_loads
        .for_underlay(UnderlayProtocol::Udp)
        .hash(&mut signature);
    if let Some(service) = evaluation.service {
        service.key.path_id.0.hash(&mut signature);
        (service.key.underlay == UnderlayProtocol::Udp).hash(&mut signature);
        service.path_instance_id.as_u64().hash(&mut signature);
        service.incarnation.hash(&mut signature);
    }
    if let Some(target) = evaluation.target {
        target.key.path_id.0.hash(&mut signature);
        (target.key.underlay == UnderlayProtocol::Udp).hash(&mut signature);
        target.path_instance_id.as_u64().hash(&mut signature);
        target.incarnation.hash(&mut signature);
    }
    signature.finish()
}

#[allow(clippy::too_many_arguments)]
pub(super) fn lab_response_service_handoff_evaluation(
    binding: &ResponseStreamBinding,
    targets: &[ResponseSenderPathTarget],
    lane: FlowLane,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    lower_flights: &[CarrierPathFlightDebt],
    ordered_data_owner: Option<CarrierPathKey>,
    ordered_owner_debt_bytes: usize,
    service_family_loads: ResponseServiceFamilyLoads,
    required_reservation: Option<ResponseServiceHandoffDrainReservation>,
    handoff_open: bool,
    startup_owner_active: bool,
    calibration_active: bool,
    another_binding_is_draining: bool,
    planner_generation: u64,
    lane_generation: u64,
    model_generation: u64,
) {
    if !lab_diagnostic_event_enabled("response_service_handoff") {
        return;
    }
    let now = Instant::now();
    let evaluation = evaluate_response_service_handoff(
        targets,
        lane,
        payload_bytes,
        mux_limits,
        lower_flights,
        ordered_data_owner,
        ordered_owner_debt_bytes,
        service_family_loads,
        required_reservation,
        handoff_open,
        startup_owner_active,
        calibration_active,
        another_binding_is_draining,
        now,
    );
    let marker_signature = response_service_handoff_capacity_marker_signature(targets, now);
    let evaluation_signature =
        response_service_handoff_evaluation_signature(evaluation, service_family_loads);
    if !binding.should_emit_response_service_handoff_diagnostic(
        model_generation,
        evaluation_signature,
        marker_signature,
        now,
    ) {
        return;
    }

    let unknown = || "unknown".to_string();
    let service = evaluation.service;
    let raw_target = evaluation.target;
    // Keep marker fields raw, but derive every placement field from the same
    // transaction view used by selection. This makes an expired pinned marker
    // visible without falsely reporting that its bounded drain lost authority.
    let effective_target_storage = service.zip(raw_target).and_then(|(service, target)| {
        response_service_handoff_diagnostic_target_view(
            service,
            target,
            lane,
            payload_bytes,
            mux_limits,
            required_reservation,
            now,
        )
    });
    let target = effective_target_storage.as_ref().or(raw_target);
    let service_bulk_flows = service.map(|service| {
        service
            .snapshot
            .active_flows
            .saturating_sub(service.snapshot.active_latency_sensitive_flows)
            .max(1)
    });
    let target_bulk_flows = target.map(|target| {
        target
            .snapshot
            .active_flows
            .saturating_sub(target.snapshot.active_latency_sensitive_flows)
            .saturating_add(1)
            .max(1)
    });
    let current_share_bps =
        service.map(|service| response_service_fair_share_bps(service, false).round() as u64);
    let projected_share_bps =
        target.map(|target| response_service_fair_share_bps(target, true).round() as u64);
    let handoff_mode = service
        .zip(target)
        .and_then(|(service, target)| {
            response_service_handoff_mode_for_targets(service, target, service_family_loads)
        })
        .map(|mode| format!("{mode:?}"));
    let target_credit = target.map(|target| {
        response_target_emission_credit_bytes(target, lane, payload_bytes, mux_limits)
    });
    let target_pending = target.map(|target| target.commands.pending_bytes());
    let raw_proof = raw_target.and_then(|target| target.quic_capacity_proof);
    let proof_state = raw_target
        .map(|target| response_quic_capacity_marker_state(target, now))
        .unwrap_or("unknown");
    let proof_remaining_us = raw_proof.map_or_else(unknown, |proof| {
        if now >= proof.expires_at {
            "expired".to_string()
        } else {
            proof.expires_at.duration_since(now).as_micros().to_string()
        }
    });
    let pinned_proof = required_reservation.and_then(|reservation| reservation.capacity_proof);
    let effective_proof = target
        .and_then(|target| target.quic_capacity_proof)
        .filter(|proof| {
            pinned_proof == Some(*proof) || valid_quic_capacity_proof_candidate_at(*proof, now)
        });
    let effective_proof_state = if pinned_proof.is_some() && effective_proof == pinned_proof {
        "pinned"
    } else if effective_proof.is_some() {
        "current"
    } else if target.is_some_and(|target| target.has_bulk_rate_evidence) {
        "generic"
    } else {
        "none"
    };
    let lower_flight_bytes = lower_flights
        .iter()
        .fold(0u64, |total, flight| total.saturating_add(flight.bytes));

    lab_diagnostic(
        "response_service_handoff",
        format_args!(
            "phase=evaluation session_id={} binding_instance_id={} planner_generation={} lane_generation={} model_generation={} first_failed_gate={} handoff_mode={} handoff_open={} startup_owner_active={} calibration_active={} another_binding_draining={} ordered_owner_debt_bytes={} lower_flight_count={} lower_flight_bytes={} service_underlay={} service_path_id={} service_path_instance_id={} service_role={} service_command_pending_bytes={} service_queue_bytes={} service_carrier_bif_bytes={} service_owner_bif_bytes={} service_rate_bps={} service_rate_scope={} service_bulk_flows={} current_fair_share_bps={} target_underlay={} target_path_id={} target_path_instance_id={} target_role={} target_active={} target_bulk_evidence={} capacity_marker_state={} capacity_marker_token={} capacity_marker_remaining_us={} capacity_effective_state={} capacity_effective_token={} target_command_pending_snapshot_bytes={} target_command_pending_live_bytes={} target_queue_bytes={} target_carrier_bif_bytes={} target_rate_bps={} target_rate_scope={} target_bulk_flows={} projected_fair_share_bps={} fair_share_preserved={} service_family_count={} target_family_count={} model_suppression={} emission_credit_bytes={} emission_pending_after_bytes={} emission_credit_available={}",
            binding.session_id().0,
            binding.binding_instance_id(),
            planner_generation,
            lane_generation,
            model_generation,
            evaluation.first_failed_gate,
            handoff_mode.unwrap_or_else(unknown),
            handoff_open,
            startup_owner_active,
            calibration_active,
            another_binding_is_draining,
            ordered_owner_debt_bytes,
            lower_flights.len(),
            lower_flight_bytes,
            service.map_or_else(unknown, |service| format!("{:?}", service.key.underlay)),
            service.map_or_else(unknown, |service| service.key.path_id.0.to_string()),
            service.map_or_else(unknown, |service| service
                .path_instance_id
                .as_u64()
                .to_string()),
            service.map_or_else(unknown, |service| format!("{:?}", service.attachment_role)),
            service.map_or_else(unknown, |service| service.command_pending_bytes.to_string()),
            service.map_or_else(unknown, |service| service.snapshot.queue_bytes.to_string()),
            service.map_or_else(unknown, |service| service
                .snapshot
                .bytes_in_flight
                .to_string()),
            service.map_or_else(unknown, |service| service
                .owner_data_in_flight_bytes
                .to_string()),
            service.map_or_else(unknown, |service| service
                .snapshot
                .delivery_rate_bps
                .round()
                .to_string()),
            service.map_or_else(unknown, |service| {
                format!("{:?}", service.snapshot.rate_scope)
            }),
            service_bulk_flows.map_or_else(unknown, |flows| flows.to_string()),
            current_share_bps.map_or_else(unknown, |share| share.to_string()),
            target.map_or_else(unknown, |target| format!("{:?}", target.key.underlay)),
            target.map_or_else(unknown, |target| target.key.path_id.0.to_string()),
            target.map_or_else(unknown, |target| target
                .path_instance_id
                .as_u64()
                .to_string()),
            target.map_or_else(unknown, |target| format!("{:?}", target.attachment_role)),
            target.map_or_else(unknown, |target| target.is_active.to_string()),
            target.map_or_else(unknown, |target| target.has_bulk_rate_evidence.to_string()),
            proof_state,
            raw_proof.map_or_else(unknown, |proof| proof.token.to_string()),
            proof_remaining_us,
            effective_proof_state,
            effective_proof.map_or_else(unknown, |proof| proof.token.to_string()),
            target.map_or_else(unknown, |target| target.command_pending_bytes.to_string()),
            target_pending.map_or_else(unknown, |pending| pending.to_string()),
            target.map_or_else(unknown, |target| target.snapshot.queue_bytes.to_string()),
            target.map_or_else(unknown, |target| target
                .snapshot
                .bytes_in_flight
                .to_string()),
            target.map_or_else(unknown, |target| target
                .snapshot
                .delivery_rate_bps
                .round()
                .to_string()),
            target.map_or_else(unknown, |target| {
                format!("{:?}", target.snapshot.rate_scope)
            }),
            target_bulk_flows.map_or_else(unknown, |flows| flows.to_string()),
            projected_share_bps.map_or_else(unknown, |share| share.to_string()),
            service
                .zip(target)
                .map_or_else(unknown, |(service, target)| {
                    response_service_handoff_preserves_fair_share(service, target).to_string()
                }),
            service.map_or_else(unknown, |service| service_family_loads
                .for_underlay(service.key.underlay)
                .to_string()),
            target.map_or_else(unknown, |target| service_family_loads
                .for_underlay(target.key.underlay)
                .to_string()),
            evaluation.model_suppression.unwrap_or("none"),
            target_credit.map_or_else(unknown, |credit| credit.to_string()),
            target_pending.map_or_else(unknown, |pending| pending
                .saturating_add(payload_bytes as u64)
                .to_string()),
            target
                .zip(target_credit)
                .map_or_else(unknown, |(target, credit)| {
                    response_target_has_emission_credit(target, lane, payload_bytes, mux_limits)
                        .then_some(credit)
                        .is_some()
                        .to_string()
                }),
        ),
    );
}
