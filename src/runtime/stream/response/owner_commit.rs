//! Optimistic response owner apply and ACK-clock retirement transaction.
//! Outputs is the primary lock; validation and reservation precede carrier
//! enqueue, which must precede exact flight commit.
//! Ordinary Service admission orders locks as lane, outputs, ordered owner,
//! flights, Subflow state, Service registration, then the session lane tracker.

#[cfg(test)]
use super::attachment::ResponseSenderPathTarget;
use super::attachment::{ResponseDispatchTarget, ResponseStreamOutputEntry};
use super::subflow::{
    ResponseSubflowAdmissionRequest, server_output_has_bulk_rate_evidence_with_limits,
};
use super::{MIN_ACTIVE_RESPONSE_FLOWS_FOR_SAME_FAMILY_DISCOVERY, ResponseStreamBinding};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::ack_clock::reliable_ack_clock_calibration_ceiling_bytes;
use crate::model::multipath::PathAdmissionDecision;
use crate::model::path::CarrierPathKey;
use crate::protocol::frame::reliable_stream_frame_extent;
use crate::protocol::{Frame, StreamOpenRole, UnderlayProtocol};
use crate::runtime::RuntimeError;
use crate::scheduler::FlowLane;
use std::sync::atomic::Ordering;

#[derive(Debug, Clone, Copy)]
/// Optimistic calibration reservation. Generations fence product/path model
/// changes; pending values fence the exact queue-pressure projection.
pub(in crate::runtime) struct ResponseAckClockCalibrationRequest {
    pub(in crate::runtime) expected_planner_generation: u64,
    pub(in crate::runtime) expected_lane_generation: u64,
    pub(in crate::runtime) expected_model_generation: u64,
    pub(in crate::runtime) service: CarrierPathKey,
    pub(in crate::runtime) service_incarnation: u64,
    pub(in crate::runtime) service_pending_bytes: u64,
    pub(in crate::runtime) target_pending_bytes: u64,
    pub(in crate::runtime) limit_bytes: u64,
    /// Fresh work requires active response demand; exact begun work may finish.
    pub(in crate::runtime) requires_active_response_start: bool,
}

#[derive(Debug, Clone, Copy)]
/// Zero-spend retirement uses the same coherent planner/model snapshot as Admit.
pub(in crate::runtime) struct ResponseAckClockCalibrationRetirementRequest {
    pub(in crate::runtime) expected_planner_generation: u64,
    pub(in crate::runtime) expected_lane_generation: u64,
    pub(in crate::runtime) expected_model_generation: u64,
    pub(in crate::runtime) service: CarrierPathKey,
    pub(in crate::runtime) service_incarnation: u64,
    pub(in crate::runtime) service_pending_bytes: u64,
    pub(in crate::runtime) target: CarrierPathKey,
    pub(in crate::runtime) target_incarnation: u64,
    pub(in crate::runtime) target_pending_bytes: u64,
    pub(in crate::runtime) limit_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
/// Exact ownership transition authorized for one OwnerData enqueue. Keeping
/// Service, established Subflow, new Subflow, and calibration distinct makes
/// impossible combinations unrepresentable at the binding commit boundary.
pub(in crate::runtime) enum ResponseOwnerEnqueueAdmission {
    Service,
    ExistingSubflow,
    NewSubflow(ResponseSubflowAdmissionRequest),
    AckClockCalibration(ResponseAckClockCalibrationRequest),
}

impl ResponseStreamBinding {
    pub(in crate::runtime) fn try_retire_tcp_ack_clock_calibration(
        &self,
        request: ResponseAckClockCalibrationRetirementRequest,
    ) -> bool {
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let mut retired = None;
        let applied = self
            .lane_tracker
            .with_matching_generation_and_min_active_response_flows(
                self.session_id,
                request.expected_lane_generation,
                MIN_ACTIVE_RESPONSE_FLOWS_FOR_SAME_FAMILY_DISCOVERY,
                || {
                    if self.response_model_generation.load(Ordering::Acquire)
                        != request.expected_model_generation
                    {
                        return false;
                    }
                    let mut subflow_state = self
                        .subflow_set
                        .lock()
                        .expect("server reliable stream subflow set lock");
                    if subflow_state.planner_generation != request.expected_planner_generation
                        || subflow_state.set.as_ref().is_none_or(|epoch| {
                            epoch.service_key() != request.service
                                || epoch.startup_owner_key().is_some()
                        })
                    {
                        return false;
                    }
                    let service_is_exact_and_proven = outputs.entries.iter().any(|entry| {
                        entry.key == request.service
                            && entry.incarnation == request.service_incarnation
                            && entry.role != StreamOpenRole::Repair
                            && !entry.commands.is_closed()
                            && entry.commands.pending_bytes() == request.service_pending_bytes
                            && entry.key.underlay == UnderlayProtocol::Tcp
                            && server_output_has_bulk_rate_evidence_with_limits(
                                entry,
                                self.mux_limits,
                            )
                    });
                    let target_is_exact_and_drained = outputs.entries.iter().any(|entry| {
                        entry.key == request.target
                            && entry.incarnation == request.target_incarnation
                            && entry.role == StreamOpenRole::Validation
                            && !entry.commands.is_closed()
                            && entry.commands.pending_bytes() == request.target_pending_bytes
                            && entry.key.underlay == UnderlayProtocol::Tcp
                            && entry.key.underlay == request.service.underlay
                            // RepairData may remain as carrier pressure, but it
                            // cannot preserve a unique OwnerData policy fence.
                            && entry.owner_data_in_flight_bytes == 0
                    });
                    let identity = (request.target, request.target_incarnation);
                    if !service_is_exact_and_proven
                        || !target_is_exact_and_drained
                        || outputs.active_ack_clock_calibration.is_some()
                    {
                        return false;
                    }
                    let flights = self
                        .flights
                        .lock()
                        .expect("server reliable stream flight lock");
                    let has_exact_owner_flight = flights.values().flatten().any(|flight| {
                        flight.key == request.target
                            && flight.output_incarnation == request.target_incarnation
                            && flight.kind.is_ordering_owner()
                    });
                    if has_exact_owner_flight {
                        return false;
                    }
                    drop(flights);

                    let Some(calibration) = outputs.ack_clock_calibrations.get_mut(&identity)
                    else {
                        return false;
                    };
                    if calibration.proven
                        || calibration.retired
                        || calibration.spent_bytes != 0
                        || calibration.credit_limit_bytes != request.limit_bytes
                        || calibration.credit_limit_bytes > calibration.max_limit_bytes
                    {
                        return false;
                    }
                    calibration.retire();
                    retired = Some(*calibration);
                    subflow_state.planner_generation =
                        subflow_state.planner_generation.wrapping_add(1);
                    true
                },
            )
            .unwrap_or(false);
        drop(outputs);
        if !applied {
            return false;
        }
        #[cfg(feature = "lab-diagnostics")]
        if let Some(calibration) = retired {
            lab_diagnostic(
                "response_ack_clock_calibration",
                format_args!(
                    "phase=terminal session_id={} binding_instance_id={} underlay={:?} path_id={} incarnation={} reason=completion_horizon active_owner_flights=false calibrated_rate_ready=false calibrated_rate_bps=0 spent_bytes={} previous_credit_limit_bytes={} credit_limit_bytes={} max_limit_bytes={} stage_authorized_spent_bytes={} stage_credit_bytes={} stage_strict_capacity_bytes={} stage_evidence_bytes={} stage_rate_ineligible_bytes={} proven={} retired={}",
                    self.session_id.0,
                    self.binding_instance_id,
                    request.target.underlay,
                    request.target.path_id.0,
                    request.target_incarnation,
                    calibration.spent_bytes,
                    request.limit_bytes,
                    calibration.credit_limit_bytes,
                    calibration.max_limit_bytes,
                    calibration.stage_authorized_spent_bytes,
                    calibration.stage_credit_bytes(),
                    calibration.stage_strict_capacity_bytes(),
                    calibration.stage_rate_evidence_bytes,
                    calibration.stage_rate_ineligible_bytes,
                    calibration.proven,
                    calibration.retired,
                ),
            );
        }
        #[cfg(not(feature = "lab-diagnostics"))]
        let _ = retired;
        self.notify_update();
        true
    }

    #[cfg(test)]
    pub(in crate::runtime) fn try_enqueue_owner_frame_for_target(
        &self,
        target: &ResponseSenderPathTarget,
        frame: &Frame,
        lane: FlowLane,
        admission: ResponseOwnerEnqueueAdmission,
    ) -> Result<Option<u64>, RuntimeError> {
        self.try_enqueue_owner_frame_for_dispatch_target(&target.into(), frame, lane, admission)
    }

    pub(in crate::runtime) fn try_enqueue_owner_frame_for_dispatch_target(
        &self,
        target: &ResponseDispatchTarget,
        frame: &Frame,
        lane: FlowLane,
        admission: ResponseOwnerEnqueueAdmission,
    ) -> Result<Option<u64>, RuntimeError> {
        self.try_enqueue_owner_frame_for_target_inner(target, frame, lane, admission, || {})
    }

    pub(super) fn try_enqueue_owner_frame_for_target_inner(
        &self,
        target: &ResponseDispatchTarget,
        frame: &Frame,
        lane: FlowLane,
        admission: ResponseOwnerEnqueueAdmission,
        after_subflow_reservation: impl FnOnce(),
    ) -> Result<Option<u64>, RuntimeError> {
        if !self.response_stream_open.load(Ordering::Acquire) {
            return Err(RuntimeError::SenderServiceBlocked);
        }
        let Some((_, _, payload_bytes)) = reliable_stream_frame_extent(frame) else {
            return Err(RuntimeError::SenderServiceBlocked);
        };
        // Service registration follows the binding's flow lane, not the
        // command's effective carrier queue lane. Keep this guard through the
        // outputs transaction so a concurrent lane change cannot make it stale.
        let service_lane = match admission {
            ResponseOwnerEnqueueAdmission::Service => {
                Some(self.lane.lock().expect("server reliable stream lane lock"))
            }
            _ => None,
        };
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        if !self.response_stream_open.load(Ordering::Acquire) {
            return Err(RuntimeError::SenderServiceBlocked);
        }
        let target_matches = |entry: &ResponseStreamOutputEntry| {
            entry.key == target.key
                && entry.path_instance_id == target.path_instance_id
                && entry.incarnation == target.incarnation
                && entry.commands.same_channel(&target.commands)
                && entry.role == target.attachment_role
                && entry.role != StreamOpenRole::Repair
        };
        let target_index = outputs
            .entries
            .last()
            .filter(|entry| target_matches(entry))
            .map(|_| outputs.entries.len() - 1)
            .or_else(|| outputs.entries.iter().position(target_matches));
        let Some(target_index) = target_index else {
            return Err(RuntimeError::SenderServiceBlocked);
        };
        if let ResponseOwnerEnqueueAdmission::AckClockCalibration(request) = admission {
            let calibration_ceiling = reliable_ack_clock_calibration_ceiling_bytes(self.mux_limits);
            let calibration_limit = request.limit_bytes.min(calibration_ceiling);
            return self
                .lane_tracker
                .with_matching_generation_and_min_active_response_flows(
                    self.session_id,
                    request.expected_lane_generation,
                    if request.requires_active_response_start {
                        MIN_ACTIVE_RESPONSE_FLOWS_FOR_SAME_FAMILY_DISCOVERY
                    } else {
                        0
                    },
                    || {
                    {
                        if self.response_model_generation.load(Ordering::Acquire)
                            != request.expected_model_generation
                        {
                            return Err(RuntimeError::SenderServiceBlocked);
                        }
                        let state = self
                            .subflow_set
                            .lock()
                            .expect("server reliable stream subflow set lock");
                        if state.planner_generation != request.expected_planner_generation
                            || state.set.as_ref().is_none_or(|epoch| {
                                epoch.service_key() != request.service
                                    || epoch.startup_owner_key().is_some()
                            })
                        {
                            return Err(RuntimeError::SenderServiceBlocked);
                        }
                    }
                    let service_is_exact_and_proven = outputs.entries.iter().any(|entry| {
                        entry.key == request.service
                            && entry.incarnation == request.service_incarnation
                            && entry.role != StreamOpenRole::Repair
                            && !entry.commands.is_closed()
                            && entry.commands.pending_bytes() == request.service_pending_bytes
                            && entry.key.underlay == UnderlayProtocol::Tcp
                            && server_output_has_bulk_rate_evidence_with_limits(
                                entry,
                                self.mux_limits,
                            )
                    });
                    let target_entry = &outputs.entries[target_index];
                    let identity = (target_entry.key, target_entry.incarnation);
                    let target_is_tcp_validation = target_entry.role == StreamOpenRole::Validation
                        && target_entry.key.underlay == UnderlayProtocol::Tcp
                        && target_entry.key.underlay == request.service.underlay
                        && !target_entry.commands.is_closed()
                        && target_entry.commands.pending_bytes() == request.target_pending_bytes;
                    // The product-flight ledger already includes frames that
                    // remain pending in the carrier command pipe.
                    let target_has_calibration_headroom = target_entry
                        .bytes_in_flight
                        .max(target_entry.commands.pending_bytes())
                        .saturating_add(payload_bytes as u64)
                        <= calibration_limit;
                    let active_matches = outputs
                        .active_ack_clock_calibration
                        .is_none_or(|active| active == identity);
                    let calibration_is_available = outputs
                        .ack_clock_calibrations
                        .get(&identity)
                        .is_some_and(|calibration| {
                            !calibration.proven
                                && request.limit_bytes == calibration.credit_limit_bytes
                                && calibration.credit_limit_bytes <= calibration.max_limit_bytes
                                && calibration.max_limit_bytes <= calibration_ceiling
                                && calibration
                                    .spent_bytes
                                    .saturating_add(payload_bytes as u64)
                                    <= calibration_limit
                        });
                    if !service_is_exact_and_proven
                        || !target_is_tcp_validation
                        || !target_has_calibration_headroom
                        || !active_matches
                        || !calibration_is_available
                    {
                        return Err(RuntimeError::SenderServiceBlocked);
                    }

                    let previous_active = outputs.active_ack_clock_calibration;
                    let previous_calibration = *outputs
                        .ack_clock_calibrations
                        .get(&identity)
                        .expect("validated response calibration identity");
                    let reserved_calibration = {
                        let calibration = outputs
                        .ack_clock_calibrations
                        .get_mut(&identity)
                        .expect("validated response calibration identity");
                        calibration.spent_bytes = calibration
                            .spent_bytes
                            .saturating_add(payload_bytes as u64);
                        *calibration
                    };
                    #[cfg(not(feature = "lab-diagnostics"))]
                    let _ = reserved_calibration;
                    outputs.active_ack_clock_calibration = Some(identity);
                    if let Err(err) = target
                        .commands
                        .try_enqueue_stream_ordered_frame(frame.clone(), lane)
                    {
                        *outputs
                            .ack_clock_calibrations
                            .get_mut(&identity)
                            .expect("reserved response calibration identity") =
                            previous_calibration;
                        outputs.active_ack_clock_calibration = previous_active;
                        return Err(err);
                    }
                    self.record_validated_owner_flight_with_outputs(
                        &mut outputs,
                        target_index,
                        frame,
                    );
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "response_ack_clock_calibration",
                        format_args!(
                            "phase=selected session_id={} binding_instance_id={} underlay={:?} path_id={} incarnation={} payload_bytes={} spent_bytes={} credit_limit_bytes={} max_limit_bytes={} proven={}",
                            self.session_id.0,
                            self.binding_instance_id,
                            identity.0.underlay,
                            identity.0.path_id.0,
                            identity.1,
                            payload_bytes,
                            reserved_calibration.spent_bytes,
                            reserved_calibration.credit_limit_bytes,
                            reserved_calibration.max_limit_bytes,
                            reserved_calibration.proven,
                        ),
                    );
                    Ok(None)
                    },
                )
                .unwrap_or(Err(RuntimeError::SenderServiceBlocked));
        }
        if let ResponseOwnerEnqueueAdmission::NewSubflow(request) = admission {
            return self
                .lane_tracker
                .with_matching_generation(self.session_id, request.expected_lane_generation, || {
                    let reservation = self.reserve_subflow_owner_admission_for_request(request);
                    if reservation.admission.decision != PathAdmissionDecision::AdmitSubflow {
                        return Err(RuntimeError::SenderServiceBlocked);
                    }
                    after_subflow_reservation();
                    if let Err(err) = target
                        .commands
                        .try_enqueue_stream_ordered_frame(frame.clone(), lane)
                    {
                        if let Some(epoch_generation) = reservation.epoch_generation {
                            self.rollback_subflow_owner_admission_for_epoch(
                                epoch_generation,
                                request.input,
                            );
                        }
                        return Err(err);
                    }
                    self.record_validated_owner_flight_with_outputs(
                        &mut outputs,
                        target_index,
                        frame,
                    );
                    Ok(reservation.epoch_generation)
                })
                .unwrap_or(Err(RuntimeError::SenderServiceBlocked));
        }
        match admission {
            ResponseOwnerEnqueueAdmission::Service => {
                let mut service = self
                    .ordered_data_owner
                    .lock()
                    .expect("server reliable stream ordered data owner lock");
                if !self.response_stream_open.load(Ordering::Acquire) {
                    return Err(RuntimeError::SenderServiceBlocked);
                }
                let changed = *service != Some(target.key);

                // Slot reservation is the only fallible operation. Publish
                // the command only after its owner and exact flight exist, so
                // the carrier cannot dequeue work ahead of response metadata.
                let command = target
                    .commands
                    .try_reserve_stream_ordered_frame(frame.clone(), lane)?;
                self.record_validated_owner_flight_with_outputs(&mut outputs, target_index, frame);
                if changed {
                    *service = Some(target.key);
                    outputs
                        .ack_clock_calibrations
                        .remove(&(target.key, target.incarnation));
                    if outputs.active_ack_clock_calibration
                        == Some((target.key, target.incarnation))
                    {
                        outputs.active_ack_clock_calibration = None;
                    }
                    self.reset_subflow_set_with_outputs(&mut outputs);
                    self.response_flow_registration.set_service(Some((
                        target.key,
                        **service_lane
                            .as_ref()
                            .expect("Service admission holds the binding lane"),
                    )));
                }
                command.commit();
                drop(service);
                drop(outputs);
                if changed {
                    self.notify_update();
                }
                Ok(None)
            }
            ResponseOwnerEnqueueAdmission::ExistingSubflow => {
                target
                    .commands
                    .try_enqueue_stream_ordered_frame(frame.clone(), lane)?;
                self.record_validated_owner_flight_with_outputs(&mut outputs, target_index, frame);
                Ok(None)
            }
            ResponseOwnerEnqueueAdmission::NewSubflow(_)
            | ResponseOwnerEnqueueAdmission::AckClockCalibration(_) => {
                unreachable!("typed response admission was handled above")
            }
        }
    }
}

#[cfg(test)]
#[path = "owner_commit_test.rs"]
mod tests;
