//! Response intent preflight and carrier command dispatch.
//!
//! This module never ranks paths. It resolves a planned identity, asks the
//! binding to revalidate and commit, and enqueues one carrier command.

use super::multipath::{
    ResponseDataDispatchIntent, ResponseDataDispatchPlan, ResponseDataDispatchTarget,
};
use super::planner::choose_response_sender_target;
#[cfg(test)]
use super::*;
use crate::model::path::CarrierPathKey;
use crate::protocol::Frame;
use crate::protocol::frame::reliable_stream_frame_accounted_bytes;
use crate::runtime::RuntimeError;
use crate::runtime::path::commands::reliable_path_stream_ordered_queue_lane;
use crate::runtime::sender::{CarrierEmitMode, RelaySendCause};
use crate::runtime::stream::response::{
    ResponseDispatchTarget, ResponseOwnerEnqueueAdmission, record_server_sender_decision,
};
use crate::runtime::stream::{ReliablePathStream, ReliablePathStreamOutput};
use crate::scheduler::FlowLane;

pub(super) struct ResponseDataEmitOutcome {
    pub(super) selected_path: Option<CarrierPathKey>,
}

pub(super) fn response_repair_carrier_lane(frame: &Frame) -> FlowLane {
    if matches!(frame, Frame::StreamData { .. }) {
        reliable_path_stream_ordered_queue_lane()
    } else {
        FlowLane::Control
    }
}

pub(super) fn response_frame_has_carrier_credit(
    stream: &ReliablePathStream,
    frame: &Frame,
    lane: FlowLane,
    emit_mode: CarrierEmitMode,
    repair_cause: Option<RelaySendCause>,
) -> bool {
    let repair = repair_cause.is_some();
    let lane = if repair {
        response_repair_carrier_lane(frame)
    } else {
        lane
    };
    match &stream.output {
        ReliablePathStreamOutput::Fixed(fixed) => match emit_mode {
            CarrierEmitMode::Classified => fixed.commands().can_enqueue_frame_now(frame, lane),
            CarrierEmitMode::StreamOrdered => {
                fixed.commands().can_enqueue_stream_ordered_frame_now(lane)
            }
        },
        ReliablePathStreamOutput::Switchable(binding) => {
            let payload_bytes = reliable_stream_frame_accounted_bytes(frame);
            let lower_flights = if matches!(frame, Frame::StreamData { .. }) && !repair {
                binding.lower_flights_before_frame(frame)
            } else {
                Vec::new()
            };
            let avoid_keys = match repair_cause {
                Some(RelaySendCause::LiveOwnerTailRepair) => {
                    binding.owner_flight_keys_overlapping_frame(frame)
                }
                Some(_) => binding.flight_keys_overlapping_frame(frame),
                None => Vec::new(),
            };
            let targets = binding.sender_path_targets(lane, payload_bytes);
            choose_response_sender_target(
                &targets,
                lane,
                frame,
                emit_mode,
                binding.mux_limits(),
                &lower_flights,
                &avoid_keys,
                repair_cause,
            )
            .is_some()
        }
    }
}

pub(super) fn emit_planned_response_data_frame(
    stream: &ReliablePathStream,
    planned: ResponseDataDispatchPlan,
    frame: Frame,
    lane: FlowLane,
) -> Result<ResponseDataEmitOutcome, RuntimeError> {
    let ResponseDataDispatchPlan { primary } = planned;
    match primary {
        ResponseDataDispatchTarget::Fixed { key } => {
            let ReliablePathStreamOutput::Fixed(fixed) = &stream.output else {
                return Err(RuntimeError::SenderServiceBlocked);
            };
            if fixed.key() != key {
                return Err(RuntimeError::SenderServiceBlocked);
            }
            CarrierEmitMode::StreamOrdered.try_enqueue_frame(
                fixed.commands(),
                frame.clone(),
                lane,
            )?;
            fixed.record_owner_flight(&frame);
            Ok(ResponseDataEmitOutcome {
                selected_path: Some(fixed.key()),
            })
        }
        ResponseDataDispatchTarget::Switchable { target, intent } => {
            let ReliablePathStreamOutput::Switchable(binding) = &stream.output else {
                return Err(RuntimeError::SenderServiceBlocked);
            };
            let decision_reason = match &intent {
                ResponseDataDispatchIntent::Service => "data_service",
                ResponseDataDispatchIntent::SubflowAdmission(_) => "data_subflow",
                ResponseDataDispatchIntent::AckClockCalibration(_) => {
                    "data_subflow_ack_clock_calibration"
                }
                ResponseDataDispatchIntent::ServiceHandoff(_) => "data_service_handoff",
            };
            let enqueue_result = match intent {
                ResponseDataDispatchIntent::Service => binding
                    .try_enqueue_owner_frame_for_dispatch_target(
                        &target,
                        &frame,
                        lane,
                        ResponseOwnerEnqueueAdmission::Service,
                    ),
                ResponseDataDispatchIntent::SubflowAdmission(request) => binding
                    .try_enqueue_owner_frame_for_dispatch_target(
                        &target,
                        &frame,
                        lane,
                        ResponseOwnerEnqueueAdmission::SubflowAdmission(request),
                    ),
                ResponseDataDispatchIntent::AckClockCalibration(request) => binding
                    .try_enqueue_owner_frame_for_dispatch_target(
                        &target,
                        &frame,
                        lane,
                        ResponseOwnerEnqueueAdmission::AckClockCalibration(request),
                    ),
                ResponseDataDispatchIntent::ServiceHandoff(handoff) => binding
                    .try_enqueue_response_service_handoff_for_dispatch(
                        &target,
                        &frame,
                        lane,
                        handoff.into_request(&target),
                    )
                    .map(|()| None),
            };
            match enqueue_result {
                Ok(_) => {}
                Err(RuntimeError::SenderServiceBlocked) => {
                    return Err(RuntimeError::SenderServiceBlocked);
                }
                Err(_) => {
                    binding.detach_path_instance(target.key, target.path_instance_id);
                    return Err(RuntimeError::SenderServiceBlocked);
                }
            }
            record_server_sender_decision(
                binding.session_id(),
                stream.stream_id,
                target.key,
                &frame,
                lane,
                decision_reason,
                Some(target.has_bulk_rate_evidence),
            );
            Ok(ResponseDataEmitOutcome {
                selected_path: Some(target.key),
            })
        }
    }
}

pub(super) fn emit_response_frame_from_sender_service(
    stream: &ReliablePathStream,
    frame: Frame,
    lane: FlowLane,
    emit_mode: CarrierEmitMode,
    reason: &'static str,
    repair_cause: Option<RelaySendCause>,
) -> Result<Option<CarrierPathKey>, RuntimeError> {
    let repair = repair_cause.is_some();
    let lane = if repair {
        response_repair_carrier_lane(&frame)
    } else {
        lane
    };
    let emit_mode = if matches!(frame, Frame::StreamData { .. }) && !repair {
        CarrierEmitMode::StreamOrdered
    } else {
        emit_mode
    };
    match &stream.output {
        ReliablePathStreamOutput::Fixed(fixed) => {
            emit_mode.try_enqueue_frame(fixed.commands(), frame.clone(), lane)?;
            if matches!(frame, Frame::StreamData { .. }) {
                if repair {
                    fixed.record_repair_flight(&frame);
                } else {
                    fixed.record_owner_flight(&frame);
                }
            }
            Ok(Some(fixed.key()))
        }
        ReliablePathStreamOutput::Switchable(binding) => {
            let payload_bytes = reliable_stream_frame_accounted_bytes(&frame);
            let lower_flights = if matches!(frame, Frame::StreamData { .. }) && !repair {
                binding.lower_flights_before_frame(&frame)
            } else {
                Vec::new()
            };
            let avoid_keys = match repair_cause {
                Some(RelaySendCause::LiveOwnerTailRepair) => {
                    binding.owner_flight_keys_overlapping_frame(&frame)
                }
                Some(_) => binding.flight_keys_overlapping_frame(&frame),
                None => Vec::new(),
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
                    &lower_flights,
                    &avoid_keys,
                    repair_cause,
                ) else {
                    return Err(RuntimeError::SenderServiceBlocked);
                };
                let dispatch_target = ResponseDispatchTarget::from(&target);
                let send_result = if matches!(frame, Frame::StreamData { .. }) {
                    if repair {
                        binding
                            .try_enqueue_repair_frame_for_target(&dispatch_target, &frame, lane)
                            .map(|()| None)
                    } else {
                        binding.try_enqueue_owner_frame_for_dispatch_target(
                            &dispatch_target,
                            &frame,
                            lane,
                            ResponseOwnerEnqueueAdmission::Service,
                        )
                    }
                } else {
                    match emit_mode {
                        CarrierEmitMode::Classified => binding
                            .try_enqueue_classified_frame_for_target(
                                &dispatch_target,
                                frame.clone(),
                                lane,
                            ),
                        CarrierEmitMode::StreamOrdered => binding
                            .try_enqueue_stream_ordered_frame_for_target(
                                &dispatch_target,
                                frame.clone(),
                                lane,
                            ),
                    }
                    .map(|()| None)
                };
                match send_result {
                    Ok(_) => {
                        record_server_sender_decision(
                            binding.session_id(),
                            stream.stream_id,
                            target.observation.key,
                            &frame,
                            lane,
                            reason,
                            Some(target.observation.has_bulk_rate_evidence),
                        );
                        return Ok(Some(target.observation.key));
                    }
                    Err(RuntimeError::SenderServiceBlocked) => {
                        return Err(RuntimeError::SenderServiceBlocked);
                    }
                    Err(err) => {
                        last_error = Some(err);
                        binding.detach_path_instance(
                            target.observation.key,
                            target.observation.path_instance_id,
                        );
                    }
                }
            }
        }
    }
}

pub(in crate::runtime) fn emit_response_control_frame(
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
        CarrierEmitMode::Classified,
        "control",
        None,
    )
}

#[cfg(test)]
#[path = "dispatch_test.rs"]
mod tests;
