//! Receipt-authorized response QUIC carrier calibration.
//!
//! This owner computes one immutable QUIC train and starts its exact-instance
//! transaction. Stream state validates and reserves it; Quinn owns packet ACK,
//! congestion, pacing, recovery, and the receipt-derived capacity evidence.

use crate::model::capacity::{
    CAPACITY_TIMING_SLACK_BYTES, PATH_OPEN_SCORE_BYTES, QUIC_PERSISTENT_CONGESTION_THRESHOLD,
    reliable_capacity_calibration_session_limit_bytes, reliable_subflow_startup_sample_limit_bytes,
    valid_quic_capacity_proof_geometry,
};
use crate::model::path::{CarrierPathKey, carrier_path_key_order};
use crate::model::response::{ResponseServiceFamilyLoads, response_snapshot_handoff_mode};
use crate::model::timing::{quic_bulk_proof_freshness_horizon, transport_pto_from_snapshot};
use crate::mux::MuxLimits;
use crate::protocol::{StreamOpenRole, UnderlayProtocol};
use crate::runtime::stream::response::{
    MAX_RESPONSE_QUIC_CAPACITY_CALIBRATION_ATTEMPTS_PER_PATH,
    MIN_ACTIVE_RESPONSE_FLOWS_FOR_SAME_FAMILY_DISCOVERY, ResponseQuicCapacityCalibrationRequest,
    ResponseSenderPathTarget, ResponseStreamBinding,
};
use crate::scheduler::FlowLane;
use std::time::Duration;

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
    let geometry = ResponseQuicCapacityCalibrationGeometry {
        train_bytes,
        fits_session_envelope,
        sample_floor_bytes: sample_floor,
        accounting_slack_bytes: packet_accounting_slack,
        fresh_strict_window_bytes: fresh_strict_window,
        carrier_window_bytes: carrier_window,
    };
    debug_assert!(
        !geometry.fits_session_envelope
            || valid_quic_capacity_proof_geometry(
                geometry.train_bytes as u64,
                geometry.sample_floor_bytes,
                geometry.accounting_slack_bytes,
                geometry.carrier_window_bytes,
                geometry.fresh_strict_window_bytes,
            )
    );
    geometry
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
    // One additional PTO covers ACK/recovery after the bounded feed horizon.
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

#[allow(clippy::too_many_arguments)]
pub(super) fn try_start_response_quic_capacity_calibration(
    binding: &ResponseStreamBinding,
    targets: &[ResponseSenderPathTarget],
    lane: FlowLane,
    ordered_data_owner: Option<CarrierPathKey>,
    service_family_loads: ResponseServiceFamilyLoads,
    active_response_flows: u32,
    planner_generation: u64,
    lane_generation: u64,
    model_generation: u64,
    already_reserved: bool,
    spent_bytes: u64,
    handoff_drain_active: bool,
) -> bool {
    let Some(target) = select_response_quic_capacity_calibration_start(
        targets,
        lane,
        ordered_data_owner,
        service_family_loads,
        binding.mux_limits(),
        active_response_flows,
        already_reserved,
        spent_bytes,
        handoff_drain_active,
    ) else {
        return false;
    };
    let geometry = response_quic_capacity_calibration_geometry(&target, binding.mux_limits());
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

/// Pure start decision shared by readiness preview and the mutating apply path.
pub(super) fn select_response_quic_capacity_calibration_start(
    targets: &[ResponseSenderPathTarget],
    lane: FlowLane,
    ordered_data_owner: Option<CarrierPathKey>,
    service_family_loads: ResponseServiceFamilyLoads,
    mux_limits: MuxLimits,
    active_response_flows: u32,
    already_reserved: bool,
    spent_bytes: u64,
    handoff_drain_active: bool,
) -> Option<ResponseSenderPathTarget> {
    if already_reserved
        || handoff_drain_active
        || active_response_flows < MIN_ACTIVE_RESPONSE_FLOWS_FOR_SAME_FAMILY_DISCOVERY
        || !service_family_loads.needs_diversification()
    {
        return None;
    }
    let remaining_probe_bytes =
        reliable_capacity_calibration_session_limit_bytes(mux_limits).saturating_sub(spent_bytes);
    select_response_quic_capacity_calibration_target(
        targets,
        lane,
        ordered_data_owner,
        service_family_loads,
        mux_limits,
        remaining_probe_bytes,
    )
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
            && response_snapshot_handoff_mode(
                service.key.underlay,
                service.snapshot,
                target.key.underlay,
                target.snapshot,
                service_family_loads,
            )
            .is_some()
    }) {
        // A measured target that clears placement should drain toward handoff;
        // another carrier-only train would add optional traffic without value.
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
        // exact reachable path once before spending a retry on one attachment.
        .min_by(|left, right| {
            (left.quic_capacity_calibration_attempts != 0)
                .cmp(&(right.quic_capacity_calibration_attempts != 0))
                .then_with(|| left.eta_ms.total_cmp(&right.eta_ms))
                .then_with(|| carrier_path_key_order(left.key, right.key))
                .then_with(|| left.incarnation.cmp(&right.incarnation))
        })
        .cloned()
}

#[cfg(test)]
#[path = "quic_capacity_test.rs"]
mod tests;
