use super::admission::{
    response_owner_bulk_model_suppression, response_target_emission_credit_bytes,
    response_target_has_emission_credit,
};
use super::planner::response_service_handoff_target_view;
use crate::lab_diagnostics::{lab_diagnostic, lab_diagnostic_event_enabled};
use crate::model::admission::{BulkAdmissionRole, BulkExplorationCompletionProjection};
use crate::model::multipath::PathAdmission;
use crate::model::path::{CarrierPathKey, carrier_path_key_order};
use crate::model::response::{
    CarrierPathFlightDebt, ResponseBulkLead, ResponseServiceFamilyLoads,
    response_service_fair_share_bps, response_service_handoff_mode_for_observations,
    response_service_handoff_preserves_fair_share,
};
use crate::mux::MuxLimits;
use crate::protocol::{StreamOpenRole, UnderlayProtocol};
use crate::runtime::stream::response::{
    ResponseSenderPathTarget, ResponseServiceHandoffDrainReservation, ResponseStreamBinding,
    valid_quic_capacity_proof_candidate_at, well_formed_quic_capacity_proof_candidate,
};
use crate::scheduler::{FlowLane, PathSnapshot};
use std::time::Instant;

// Why separate: causal lab observability mirrors product gates, but it must
// remain an observer rather than inflate or become a second hot-path policy.

#[derive(Debug, Clone, Copy)]
pub(super) struct ResponseBulkCandidateDiag {
    pub(super) lead: Option<ResponseBulkLead>,
    pub(super) role: Option<BulkAdmissionRole>,
    pub(super) ordering_debt: u64,
}

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
            target.observation.key.underlay,
            target.observation.key.path_id.0,
            target.observation.is_service,
            target.observation.has_sender_evidence,
            target.observation.has_bulk_rate_evidence,
            diag.role
                .map(|role| format!("{:?}", role))
                .unwrap_or_else(|| "none".to_string()),
            target.observation.eta_ms,
            lead_underlay,
            lead_path_id,
            lead_eta_ms,
            diag.ordering_debt,
            payload_bytes,
            target.observation.command_pending_bytes,
            target.observation.snapshot.queue_bytes,
            target.observation.snapshot.product_queue_bytes,
            target.observation.snapshot.bytes_in_flight,
            target.observation.snapshot.product_bytes_in_flight,
            target.observation.owner_data_in_flight_bytes,
            target.observation.snapshot.inflight_limit_bytes,
            target.observation.snapshot.delivery_rate_bps / 1_000_000.0,
            target.observation.snapshot.pacing_rate_bps / 1_000_000.0,
            target.observation.snapshot.srtt_ms,
            target.observation.snapshot.confidence,
            target.observation.snapshot.app_limited,
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

pub(super) fn lab_response_bulk_output_selected(
    reason: &'static str,
    target: &ResponseSenderPathTarget,
    admission: PathAdmission,
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
            target.session_id.0,
            target.binding_instance_id,
            target.observation.key.underlay,
            target.observation.key.path_id.0,
            admission.role,
            admission.work,
            payload_bytes,
            target.observation.command_pending_bytes,
            target.observation.snapshot.product_bytes_in_flight,
            target.observation.owner_data_in_flight_bytes,
            target.observation.eta_ms,
            target.observation.snapshot.app_limited,
            target.observation.has_bulk_rate_evidence,
            target.ack_clock_calibration_eligible,
            target.ack_clock_calibration_proven,
            target.ack_clock_calibration_active,
            target.ack_clock_calibration_spent_bytes,
            target.ack_clock_calibration_credit_limit_bytes,
            target.ack_clock_calibration_max_limit_bytes,
        ),
    );
}

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
            target.observation.key.underlay,
            target.observation.key.path_id.0,
            service.observation.key.underlay,
            service.observation.key.path_id.0,
            admitted,
            uses_service_prior,
            projection.candidate_completion_ms,
            projection.service_reservoir_horizon_ms,
            projection.exploration_bytes,
            projection.service_followup_bytes,
            candidate_eta_ms,
            service.observation.eta_ms,
            candidate_snapshot
                .delivery_rate_bps
                .max(candidate_snapshot.pacing_rate_bps)
                / 1_000_000.0,
            service
                .observation
                .snapshot
                .delivery_rate_bps
                .max(service.observation.snapshot.pacing_rate_bps)
                / 1_000_000.0,
            candidate_snapshot.srtt_ms,
            service.observation.snapshot.srtt_ms,
        ),
    );
}

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
    if target.observation.key.underlay != UnderlayProtocol::Udp {
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
        service.observation.key,
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
        service.observation.key,
        lane,
        payload_bytes,
        mux_limits,
        required_reservation,
        now,
    ) else {
        return failed(0, "drain_target_changed");
    };
    let effective_target = &target_view;
    if effective_target.observation.key.underlay == service.observation.key.underlay {
        return failed(1, "same_carrier_family");
    }
    if effective_target.observation.attachment_role != StreamOpenRole::Validation {
        return failed(2, "target_role");
    }
    if effective_target.observation.is_service {
        return failed(3, "target_is_service");
    }
    if !effective_target.observation.has_bulk_rate_evidence {
        let gate = match response_quic_capacity_marker_state(target, now) {
            "expired" => "target_proof_expired",
            "invalid" => "target_proof_invalid",
            _ => "target_proof_missing",
        };
        return failed(4, gate);
    }
    if effective_target.observation.owner_data_in_flight_bytes != 0 {
        return failed(5, "target_owner_flight");
    }
    if effective_target
        .observation
        .snapshot
        .product_bytes_in_flight
        != 0
    {
        return failed(6, "target_product_flight");
    }
    if effective_target
        .observation
        .snapshot
        .active_latency_sensitive_flows
        != 0
    {
        return failed(7, "target_path_latency_load");
    }
    if effective_target
        .observation
        .snapshot
        .session_active_latency_sensitive_flows
        != 0
    {
        return failed(8, "target_session_latency_load");
    }
    if response_service_handoff_mode_for_observations(
        &service.observation,
        &effective_target.observation,
        service_family_loads,
    )
    .is_none()
    {
        return failed(9, "family_or_gain");
    }
    if !effective_target.can_enqueue_lane(lane) {
        return failed(10, "target_queue_slot");
    }
    let model_suppression = response_owner_bulk_model_suppression(
        effective_target,
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
    let service = ordered_data_owner
        .and_then(|key| targets.iter().find(|target| target.observation.key == key));
    let preferred_target = service.and_then(|service| {
        targets
            .iter()
            .filter(|target| target.observation.key.underlay != service.observation.key.underlay)
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
                    .then_with(|| left.observation.eta_ms.total_cmp(&right.observation.eta_ms))
                    .then_with(|| {
                        carrier_path_key_order(left.observation.key, right.observation.key)
                    })
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
    let Some(service) = targets
        .iter()
        .find(|target| target.observation.key == service_key)
    else {
        return blocked("service_missing");
    };
    if required_reservation.is_some_and(|reservation| {
        reservation.service != service.observation.key
            || reservation.service_path_instance_id != service.observation.path_instance_id
            || reservation.service_incarnation != service.observation.incarnation
    }) {
        return blocked("drain_service_changed");
    }
    if !service.observation.is_service {
        return blocked("service_not_active");
    }
    if !service.observation.has_bulk_rate_evidence {
        return blocked("service_proof_missing");
    }
    if service.observation.snapshot.active_latency_sensitive_flows != 0 {
        return blocked("service_path_latency_load");
    }
    if service
        .observation
        .snapshot
        .session_active_latency_sensitive_flows
        != 0
    {
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
                    .observation
                    .eta_ms
                    .total_cmp(&current.target.observation.eta_ms)
                    .then_with(|| {
                        carrier_path_key_order(
                            evaluation.target.observation.key,
                            current.target.observation.key,
                        )
                    })
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
        target.observation.key.path_id.0.hash(&mut signature);
        (target.observation.key.underlay == UnderlayProtocol::Udp).hash(&mut signature);
        target
            .observation
            .path_instance_id
            .as_u64()
            .hash(&mut signature);
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
        service.observation.key.path_id.0.hash(&mut signature);
        (service.observation.key.underlay == UnderlayProtocol::Udp).hash(&mut signature);
        service
            .observation
            .path_instance_id
            .as_u64()
            .hash(&mut signature);
        service.observation.incarnation.hash(&mut signature);
    }
    if let Some(target) = evaluation.target {
        target.observation.key.path_id.0.hash(&mut signature);
        (target.observation.key.underlay == UnderlayProtocol::Udp).hash(&mut signature);
        target
            .observation
            .path_instance_id
            .as_u64()
            .hash(&mut signature);
        target.observation.incarnation.hash(&mut signature);
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
            .observation
            .snapshot
            .active_flows
            .saturating_sub(service.observation.snapshot.active_latency_sensitive_flows)
            .max(1)
    });
    let target_bulk_flows = target.map(|target| {
        target
            .observation
            .snapshot
            .active_flows
            .saturating_sub(target.observation.snapshot.active_latency_sensitive_flows)
            .saturating_add(1)
            .max(1)
    });
    let current_share_bps = service
        .map(|service| response_service_fair_share_bps(&service.observation, false).round() as u64);
    let projected_share_bps = target
        .map(|target| response_service_fair_share_bps(&target.observation, true).round() as u64);
    let handoff_mode = service
        .zip(target)
        .and_then(|(service, target)| {
            response_service_handoff_mode_for_observations(
                &service.observation,
                &target.observation,
                service_family_loads,
            )
        })
        .map(|mode| format!("{mode:?}"));
    let target_credit = target.map(|target| {
        response_target_emission_credit_bytes(target, lane, payload_bytes, mux_limits)
    });
    let target_pending = target.map(|target| target.observation.command_pending_bytes);
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
    } else if target.is_some_and(|target| target.observation.has_bulk_rate_evidence) {
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
            service.map_or_else(unknown, |service| format!(
                "{:?}",
                service.observation.key.underlay
            )),
            service.map_or_else(unknown, |service| service
                .observation
                .key
                .path_id
                .0
                .to_string()),
            service.map_or_else(unknown, |service| service
                .observation
                .path_instance_id
                .as_u64()
                .to_string()),
            service.map_or_else(unknown, |service| format!(
                "{:?}",
                service.observation.attachment_role
            )),
            service.map_or_else(unknown, |service| service
                .observation
                .command_pending_bytes
                .to_string()),
            service.map_or_else(unknown, |service| service
                .observation
                .snapshot
                .queue_bytes
                .to_string()),
            service.map_or_else(unknown, |service| service
                .observation
                .snapshot
                .bytes_in_flight
                .to_string()),
            service.map_or_else(unknown, |service| service
                .observation
                .owner_data_in_flight_bytes
                .to_string()),
            service.map_or_else(unknown, |service| service
                .observation
                .snapshot
                .delivery_rate_bps
                .round()
                .to_string()),
            service.map_or_else(unknown, |service| {
                format!("{:?}", service.observation.snapshot.rate_scope)
            }),
            service_bulk_flows.map_or_else(unknown, |flows| flows.to_string()),
            current_share_bps.map_or_else(unknown, |share| share.to_string()),
            target.map_or_else(unknown, |target| format!(
                "{:?}",
                target.observation.key.underlay
            )),
            target.map_or_else(unknown, |target| target
                .observation
                .key
                .path_id
                .0
                .to_string()),
            target.map_or_else(unknown, |target| target
                .observation
                .path_instance_id
                .as_u64()
                .to_string()),
            target.map_or_else(unknown, |target| format!(
                "{:?}",
                target.observation.attachment_role
            )),
            target.map_or_else(unknown, |target| target.observation.is_service.to_string()),
            target.map_or_else(unknown, |target| target
                .observation
                .has_bulk_rate_evidence
                .to_string()),
            proof_state,
            raw_proof.map_or_else(unknown, |proof| proof.token.to_string()),
            proof_remaining_us,
            effective_proof_state,
            effective_proof.map_or_else(unknown, |proof| proof.token.to_string()),
            target.map_or_else(unknown, |target| target
                .observation
                .command_pending_bytes
                .to_string()),
            target_pending.map_or_else(unknown, |pending| pending.to_string()),
            target.map_or_else(unknown, |target| target
                .observation
                .snapshot
                .queue_bytes
                .to_string()),
            target.map_or_else(unknown, |target| target
                .observation
                .snapshot
                .bytes_in_flight
                .to_string()),
            target.map_or_else(unknown, |target| target
                .observation
                .snapshot
                .delivery_rate_bps
                .round()
                .to_string()),
            target.map_or_else(unknown, |target| {
                format!("{:?}", target.observation.snapshot.rate_scope)
            }),
            target_bulk_flows.map_or_else(unknown, |flows| flows.to_string()),
            projected_share_bps.map_or_else(unknown, |share| share.to_string()),
            service
                .zip(target)
                .map_or_else(unknown, |(service, target)| {
                    response_service_handoff_preserves_fair_share(
                        &service.observation,
                        &target.observation,
                    )
                    .to_string()
                }),
            service.map_or_else(unknown, |service| service_family_loads
                .for_underlay(service.observation.key.underlay)
                .to_string()),
            target.map_or_else(unknown, |target| service_family_loads
                .for_underlay(target.observation.key.underlay)
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
