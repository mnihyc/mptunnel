//! Response TCP capacity discovery and product calibration.
//!
//! TCP owns two independent evidence loops: offset-free socket probing discovers
//! carrier capacity, while bounded product offsets calibrate the response ACK
//! clock. They share TCP policy, not mutable state, and remain separate from QUIC.

#[cfg(feature = "lab-diagnostics")]
use super::diagnostics::{
    ResponseBulkCandidateDiag, lab_response_ack_clock_calibration_admission,
    lab_response_bulk_output_candidate,
};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::ack_clock::{
    TcpAckClockCalibrationOpportunity, reliable_tcp_ack_clock_calibration_opportunity,
};
#[cfg(feature = "lab-diagnostics")]
use crate::model::admission::BulkAdmissionRole;
use crate::model::admission::{
    bulk_candidate_pipe_bytes, bulk_service_feed_reservoir_payload_bytes,
    bulk_service_product_envelope_payload_bytes,
};
use crate::model::capacity::{
    reliable_capacity_calibration_session_limit_bytes, reliable_subflow_startup_sample_limit_bytes,
};
use crate::model::multipath::FlowSubflowSet;
use crate::model::path::{CarrierPathKey, carrier_path_key_order};
#[cfg(feature = "lab-diagnostics")]
use crate::model::response::ResponseBulkLead;
use crate::model::response::{
    CarrierPathFlightDebt, ResponseServiceFamilyLoads, response_snapshot_handoff_mode,
};
use crate::mux::MuxLimits;
use crate::protocol::{StreamOpenRole, UnderlayProtocol};
use crate::runtime::RuntimeError;
use crate::runtime::path::commands::TcpCapacityProbeRequest;
use crate::runtime::stream::response::{
    ResponseSenderPathTarget, ResponseStreamBinding, server_bulk_output_eta_ms,
};
use crate::scheduler::{FlowLane, PathRateScope, PathSnapshot};
use std::time::{Duration, Instant};

// These values preserve the established policy during the ownership move.
// High-BDP labs must validate them before a separate behavior change.
const RESPONSE_TCP_CAPACITY_PROBE_BYTES: u64 = 2 * 1024 * 1024;
const RESPONSE_TCP_CAPACITY_PROBE_DEADLINE: Duration = Duration::from_secs(20);

#[derive(Clone, Copy)]
pub(super) struct ResponseAckClockCalibrationSelection {
    pub(super) service: CarrierPathKey,
    pub(super) service_incarnation: u64,
    pub(super) service_pending_bytes: u64,
    pub(super) target_pending_bytes: u64,
    pub(super) limit_bytes: u64,
    pub(super) requires_active_response_start: bool,
}

#[derive(Clone, Copy)]
pub(super) struct ResponseAckClockCalibrationRetirementSelection {
    pub(super) service: CarrierPathKey,
    pub(super) service_incarnation: u64,
    pub(super) service_pending_bytes: u64,
    pub(super) target: CarrierPathKey,
    pub(super) target_incarnation: u64,
    pub(super) target_pending_bytes: u64,
    pub(super) limit_bytes: u64,
}

// Cross-protocol scheduling metadata stays in the planner.
#[derive(Clone)]
pub(super) struct ResponseTcpAckClockCalibrationSelection {
    pub(super) target: ResponseSenderPathTarget,
    pub(super) selection: ResponseAckClockCalibrationSelection,
}

pub(super) fn response_tcp_calibration_opportunity_candidate(
    service: &ResponseSenderPathTarget,
    target: &ResponseSenderPathTarget,
    lane: FlowLane,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> (PathSnapshot, f64, bool) {
    let mut snapshot = target.observation.snapshot;
    let service_rate_bps = service.observation.snapshot.delivery_rate_bps.max(1.0);
    let uses_service_prior = target.endpoint_only_service_prior_eligible
        && service_rate_bps > snapshot.delivery_rate_bps;
    if !uses_service_prior {
        return (snapshot, target.observation.eta_ms, false);
    }

    // This prior makes a bounded measurement reachable; it is not candidate
    // evidence and never leaves this completion-opportunity calculation.
    snapshot.delivery_rate_bps = service_rate_bps;
    snapshot.pacing_rate_bps = snapshot.pacing_rate_bps.max(service_rate_bps);
    snapshot.rate_scope = PathRateScope::PathCapacity;
    snapshot.inflight_limit_bytes = snapshot
        .inflight_limit_bytes
        .max(bulk_candidate_pipe_bytes(snapshot));
    let eta_ms = server_bulk_output_eta_ms(
        target.observation.key,
        snapshot,
        Some(service.observation.key),
        lane,
        payload_bytes,
        mux_limits,
    );
    (snapshot, eta_ms, true)
}

pub(super) fn try_start_response_tcp_capacity_probe(
    binding: &ResponseStreamBinding,
    targets: &[ResponseSenderPathTarget],
    lane: FlowLane,
    ordered_data_owner: Option<CarrierPathKey>,
    service_family_loads: ResponseServiceFamilyLoads,
    lane_generation: u64,
    already_reserved: bool,
) -> Result<bool, RuntimeError> {
    let Some((target, train_bytes)) = select_response_tcp_capacity_probe_start(
        targets,
        lane,
        ordered_data_owner,
        service_family_loads,
        binding.mux_limits(),
        already_reserved,
    ) else {
        return Ok(false);
    };
    let Some(expires_at) = Instant::now().checked_add(RESPONSE_TCP_CAPACITY_PROBE_DEADLINE) else {
        return Ok(false);
    };
    let Some(session_lease) = binding.try_reserve_tcp_capacity_probe(lane_generation) else {
        return Ok(false);
    };
    let calibration_id = target.commands.try_enqueue_tcp_capacity_probe(
        TcpCapacityProbeRequest {
            path_id: target.observation.key.path_id,
            path_instance_id: target.observation.path_instance_id,
            train_payload_bytes: train_bytes,
            sample_floor_bytes: reliable_subflow_startup_sample_limit_bytes(binding.mux_limits()),
            expires_at,
        },
        session_lease,
    )?;
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = calibration_id;
    #[cfg(feature = "lab-diagnostics")]
    lab_diagnostic(
        "response_tcp_capacity_probe",
        format_args!(
            "phase=started session_id={} binding_instance_id={} path_id={} path_instance_id={} incarnation={} calibration_id={} train_bytes={}",
            target.session_id.0,
            target.binding_instance_id,
            target.observation.key.path_id.0,
            target.observation.path_instance_id.as_u64(),
            target.observation.incarnation,
            calibration_id,
            train_bytes,
        ),
    );
    Ok(true)
}

/// Pure start decision shared by readiness preview and the mutating apply path.
pub(super) fn select_response_tcp_capacity_probe_start(
    targets: &[ResponseSenderPathTarget],
    lane: FlowLane,
    ordered_data_owner: Option<CarrierPathKey>,
    service_family_loads: ResponseServiceFamilyLoads,
    mux_limits: MuxLimits,
    already_reserved: bool,
) -> Option<(ResponseSenderPathTarget, u64)> {
    if already_reserved {
        return None;
    }
    select_response_tcp_capacity_probe_target(
        targets,
        lane,
        ordered_data_owner,
        service_family_loads,
        mux_limits,
    )
}

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
    let service = targets
        .iter()
        .find(|target| target.observation.key == service_key)?;
    if service.observation.key.underlay != UnderlayProtocol::Tcp
        || !service.observation.is_service
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
    if targets.iter().any(|target| {
        target.observation.key.underlay == UnderlayProtocol::Udp
            && target.observation.has_bulk_rate_evidence
            && response_snapshot_handoff_mode(
                service.observation.key.underlay,
                service.observation.snapshot,
                target.observation.key.underlay,
                target.observation.snapshot,
                service_family_loads,
            )
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
    let train_bytes = RESPONSE_TCP_CAPACITY_PROBE_BYTES
        .min(envelope)
        .max(sample_floor)
        .max(1);
    targets
        .iter()
        .filter(|target| {
            target.observation.key != service_key
                && target.observation.key.underlay == UnderlayProtocol::Tcp
                && target.observation.attachment_role == StreamOpenRole::Validation
                && !target.observation.is_service
                && target.observation.has_sender_evidence
                && !target.observation.has_bulk_rate_evidence
                && !target.commands.tcp_capacity_probe_attempted()
                && !target.commands.tcp_capacity_probe_active()
                && target.observation.command_pending_bytes == 0
                && target.observation.snapshot.queue_bytes == 0
                && target.observation.snapshot.bytes_in_flight == 0
                && target.observation.snapshot.active_latency_sensitive_flows == 0
                && target
                    .observation
                    .snapshot
                    .session_active_latency_sensitive_flows
                    == 0
                && target.commands.can_enqueue_lane_now(FlowLane::Throughput)
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
        })
        .cloned()
        .map(|target| (target, train_bytes))
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
    retirement_selections: &mut Vec<ResponseAckClockCalibrationRetirementSelection>,
) -> Option<ResponseTcpAckClockCalibrationSelection> {
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
        .find(|target| target.observation.key == service_key)?;
    if !service.observation.is_service
        || !service.observation.has_bulk_rate_evidence
        || service.observation.key.underlay != UnderlayProtocol::Tcp
        || service.observation.snapshot.active_latency_sensitive_flows > 0
        || service
            .observation
            .snapshot
            .session_active_latency_sensitive_flows
            > 0
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
        .map(|target| (target.observation.key, target.observation.incarnation));

    targets
        .iter()
        .copied()
        .filter(|target| {
            active_identity.is_none_or(|identity| {
                identity == (target.observation.key, target.observation.incarnation)
            })
        })
        .filter(|target| {
            target.observation.attachment_role == StreamOpenRole::Validation
                && target.observation.key != service_key
                && target.observation.key.underlay == service_key.underlay
                && !target.observation.is_service
                && target.observation.has_sender_evidence
                && target.observation.has_bulk_rate_evidence
                && target.ack_clock_calibration_eligible
                && !target.ack_clock_calibration_proven
                && (may_start_fresh_calibration
                    || target.ack_clock_calibration_active
                    || target.ack_clock_calibration_spent_bytes > 0)
                && target.observation.snapshot.active_latency_sensitive_flows == 0
                && target
                    .observation
                    .snapshot
                    .session_active_latency_sensitive_flows
                    == 0
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
            let candidate_debt = target.observation.owner_data_in_flight_bytes;
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
                service.observation.snapshot,
                service.observation.eta_ms,
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
                                key: service.observation.key,
                                snapshot: service.observation.snapshot,
                                eta_ms: service.observation.eta_ms,
                            }),
                            role: Some(BulkAdmissionRole::AdditionalSameUnderlay),
                            ordering_debt: ordered_owner_debt_bytes as u64,
                        },
                    );
                }
            }
            if matches!(opportunity, TcpAckClockCalibrationOpportunity::Retire(_)) {
                retirement_selections.push(ResponseAckClockCalibrationRetirementSelection {
                    service: service.observation.key,
                    service_incarnation: service.observation.incarnation,
                    service_pending_bytes: service.observation.command_pending_bytes,
                    target: target.observation.key,
                    target_incarnation: target.observation.incarnation,
                    target_pending_bytes: target.observation.command_pending_bytes,
                    limit_bytes: target.ack_clock_calibration_credit_limit_bytes,
                });
            }
            admitted
        })
        .filter(|target| {
            // RepairData cannot preserve the unique-owner fence, but it still
            // occupies the carrier/product pipe that the atomic commit checks.
            let carrier_pressure = target
                .observation
                .snapshot
                .product_bytes_in_flight
                .max(target.observation.command_pending_bytes);
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
                .then_with(|| left.observation.eta_ms.total_cmp(&right.observation.eta_ms))
                .then_with(|| carrier_path_key_order(left.observation.key, right.observation.key))
                .then_with(|| {
                    left.observation
                        .incarnation
                        .cmp(&right.observation.incarnation)
                })
        })
        .map(|target| ResponseTcpAckClockCalibrationSelection {
            target: target.clone(),
            selection: ResponseAckClockCalibrationSelection {
                service: service_key,
                service_incarnation: service.observation.incarnation,
                service_pending_bytes: service.observation.command_pending_bytes,
                target_pending_bytes: target.observation.command_pending_bytes,
                limit_bytes: target.ack_clock_calibration_credit_limit_bytes,
                requires_active_response_start: !target.ack_clock_calibration_active
                    && target.ack_clock_calibration_spent_bytes == 0,
            },
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
    !target.observation.is_service
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
    target.observation.key.underlay == UnderlayProtocol::Tcp
        && target.ack_clock_calibration_eligible
        && !target.ack_clock_calibration_proven
        && !target.ack_clock_calibration_active
        && target.ack_clock_calibration_spent_bytes == 0
        && target.ack_clock_calibration_credit_limit_bytes > 0
}

#[cfg(test)]
#[path = "tcp_capacity_test.rs"]
mod tests;
