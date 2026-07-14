//! Offset-free response TCP carrier discovery.
//!
//! TCP owns its socket probe and receipt lifecycle. This sender owner decides
//! when a response may start that probe and performs the typed queue handoff;
//! it never spends product offsets or shares mutable state with QUIC discovery.

#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::capacity::{
    reliable_capacity_calibration_session_limit_bytes, reliable_subflow_startup_sample_limit_bytes,
};
use crate::model::path::{CarrierPathKey, carrier_path_key_order};
use crate::model::response::{ResponseServiceFamilyLoads, response_snapshot_handoff_mode};
use crate::mux::MuxLimits;
use crate::protocol::{StreamOpenRole, UnderlayProtocol};
use crate::runtime::RuntimeError;
use crate::runtime::path::commands::TcpCapacityProbeRequest;
use crate::runtime::stream::response::{ResponseSenderPathTarget, ResponseStreamBinding};
use crate::scheduler::FlowLane;
use std::time::{Duration, Instant};

// These values preserve the established policy during the ownership move.
// High-BDP labs must validate them before a separate behavior change.
const RESPONSE_TCP_CAPACITY_PROBE_BYTES: u64 = 2 * 1024 * 1024;
const RESPONSE_TCP_CAPACITY_PROBE_DEADLINE: Duration = Duration::from_secs(20);

pub(super) fn try_start_response_tcp_capacity_probe(
    binding: &ResponseStreamBinding,
    targets: &[ResponseSenderPathTarget],
    lane: FlowLane,
    ordered_data_owner: Option<CarrierPathKey>,
    service_family_loads: ResponseServiceFamilyLoads,
    lane_generation: u64,
    already_reserved: bool,
) -> Result<bool, RuntimeError> {
    if already_reserved {
        return Ok(false);
    }
    let Some((target, train_bytes)) = select_response_tcp_capacity_probe_target(
        targets,
        lane,
        ordered_data_owner,
        service_family_loads,
        binding.mux_limits(),
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
            path_id: target.key.path_id,
            path_instance_id: target.path_instance_id,
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
            target.key.path_id.0,
            target.path_instance_id.as_u64(),
            target.incarnation,
            calibration_id,
            train_bytes,
        ),
    );
    Ok(true)
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
            && response_snapshot_handoff_mode(
                service.key.underlay,
                service.snapshot,
                target.key.underlay,
                target.snapshot,
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

#[cfg(test)]
#[path = "tcp_capacity_test.rs"]
mod tests;
