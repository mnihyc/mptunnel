//! Response intent preflight and carrier command dispatch.
//!
//! This module never ranks paths. It resolves a planned identity, asks the
//! binding to revalidate and commit, and enqueues one carrier command.

use super::multipath::ResponseDataDispatchTarget;
use super::response_reinjection_avoid_outputs;
use super::scheduling::select_response_frame_path;
use crate::model::path::CarrierPathKey;
use crate::protocol::Frame;
use crate::protocol::frame::reliable_stream_frame_accounted_bytes;
use crate::runtime::RuntimeError;
use crate::runtime::sender::{CarrierEmitMode, RelaySendCause};
use crate::runtime::stream::response::{ResponseDispatchTarget, record_server_sender_decision};
use crate::runtime::stream::{ReliablePathStream, ReliablePathStreamOutput};
use crate::scheduler::TrafficClass;

pub(super) fn response_frame_has_carrier_credit(
    stream: &ReliablePathStream,
    frame: &Frame,
    lane: TrafficClass,
    emit_mode: CarrierEmitMode,
    reinjection_cause: Option<RelaySendCause>,
) -> bool {
    let reinjection = reinjection_cause.is_some();
    match &stream.output {
        ReliablePathStreamOutput::Fixed(fixed) => {
            if reinjection {
                fixed.commands().can_enqueue_reinjection_frame_now(frame)
            } else {
                match emit_mode {
                    CarrierEmitMode::Classified => {
                        fixed.commands().can_enqueue_frame_now(frame, lane)
                    }
                    CarrierEmitMode::StreamOrdered => {
                        fixed.commands().can_enqueue_stream_ordered_frame_now(lane)
                    }
                }
            }
        }
        ReliablePathStreamOutput::Switchable(binding) => {
            let payload_bytes = reliable_stream_frame_accounted_bytes(frame);
            let avoid_outputs = reinjection_cause.map_or_else(Vec::new, |cause| {
                response_reinjection_avoid_outputs(binding, frame, cause)
            });
            let targets = binding.sender_path_targets(lane, payload_bytes);
            select_response_frame_path(
                &targets,
                lane,
                frame,
                emit_mode,
                &avoid_outputs,
                reinjection_cause,
            )
            .is_some()
        }
    }
}

pub(super) fn emit_planned_response_data_frame(
    stream: &ReliablePathStream,
    target: ResponseDataDispatchTarget,
    frame: Frame,
    lane: TrafficClass,
) -> Result<Option<CarrierPathKey>, RuntimeError> {
    match target {
        ResponseDataDispatchTarget::Fixed { key } => {
            let ReliablePathStreamOutput::Fixed(fixed) = &stream.output else {
                return Err(RuntimeError::SenderServiceBlocked);
            };
            if fixed.key() != key {
                return Err(RuntimeError::SenderServiceBlocked);
            }
            let command = fixed
                .commands()
                .try_reserve_admitted_frame(frame.clone(), lane)?;
            fixed.record_original_flight(&frame);
            command.commit();
            Ok(Some(fixed.key()))
        }
        ResponseDataDispatchTarget::Switchable {
            target,
            expected_model_generation,
        } => {
            let ReliablePathStreamOutput::Switchable(binding) = &stream.output else {
                return Err(RuntimeError::SenderServiceBlocked);
            };
            let tcp_service = stream.tcp_service_coordinator();
            let enqueue_result = if let Some(coordinator) = tcp_service.as_ref() {
                let mut transaction = coordinator.lock();
                binding.try_enqueue_data_frame_for_dispatch_target_with_tcp_service(
                    &target,
                    &frame,
                    lane,
                    expected_model_generation,
                    &mut transaction,
                )
            } else {
                binding.try_enqueue_data_frame_for_dispatch_target(
                    &target,
                    &frame,
                    lane,
                    expected_model_generation,
                )
            };
            match enqueue_result {
                Ok(_) => {}
                Err(RuntimeError::SenderServiceBlocked) => {
                    return Err(RuntimeError::SenderServiceBlocked);
                }
                // Carrier registration retirement owns ordered detach. Removing
                // a closed queue here could overtake ACKs already accepted by
                // the reliable-stream actor.
                Err(_) => return Err(RuntimeError::SenderServiceBlocked),
            }
            record_server_sender_decision(
                binding.session_id(),
                stream.stream_id,
                target.key,
                &frame,
                lane,
                "data_completion_time",
                Some(target.has_bulk_rate_evidence),
            );
            Ok(Some(target.key))
        }
    }
}

pub(super) fn emit_response_frame_from_sender_service(
    stream: &ReliablePathStream,
    frame: Frame,
    lane: TrafficClass,
    emit_mode: CarrierEmitMode,
    reason: &'static str,
    reinjection_cause: Option<RelaySendCause>,
) -> Result<Option<CarrierPathKey>, RuntimeError> {
    let reinjection = reinjection_cause.is_some();
    if matches!(frame, Frame::StreamData { .. }) && !reinjection {
        return Err(RuntimeError::Protocol(
            "new response data requires a generation-fenced dispatch plan",
        ));
    }
    let emit_mode = if matches!(frame, Frame::StreamData { .. }) && !reinjection {
        CarrierEmitMode::StreamOrdered
    } else {
        emit_mode
    };
    match &stream.output {
        ReliablePathStreamOutput::Fixed(fixed) => {
            if matches!(frame, Frame::StreamData { .. }) {
                let command = if reinjection {
                    fixed
                        .commands()
                        .try_reserve_reinjection_frame(frame.clone(), lane)?
                } else {
                    fixed
                        .commands()
                        .try_reserve_stream_ordered_frame(frame.clone(), lane)?
                };
                if reinjection {
                    fixed.record_reinjected_flight(&frame);
                } else {
                    fixed.record_original_flight(&frame);
                }
                command.commit();
            } else {
                emit_mode.try_enqueue_frame(fixed.commands(), frame, lane)?;
            }
            Ok(Some(fixed.key()))
        }
        ReliablePathStreamOutput::Switchable(binding) => {
            let payload_bytes = reliable_stream_frame_accounted_bytes(&frame);
            let avoid_outputs = reinjection_cause.map_or_else(Vec::new, |cause| {
                response_reinjection_avoid_outputs(binding, &frame, cause)
            });
            let mut last_error = None;
            loop {
                let targets = binding.sender_path_targets(lane, payload_bytes);
                if targets.is_empty() {
                    let _ = last_error;
                    return Err(RuntimeError::SenderServiceBlocked);
                }
                let Some(target) = select_response_frame_path(
                    &targets,
                    lane,
                    &frame,
                    emit_mode,
                    &avoid_outputs,
                    reinjection_cause,
                ) else {
                    return Err(RuntimeError::SenderServiceBlocked);
                };
                let dispatch_target = ResponseDispatchTarget::from(&target);
                let send_result = if matches!(frame, Frame::StreamData { .. }) {
                    binding.try_enqueue_reinjected_frame_for_target(&dispatch_target, &frame, lane)
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
                };
                match send_result {
                    Ok(()) => {
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
                        // A closed command queue is already unschedulable.
                        // Carrier retirement orders its detach behind accepted
                        // stream input; dispatch must not mutate ownership.
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
        TrafficClass::Control,
        CarrierEmitMode::Classified,
        "control",
        None,
    )
}

#[cfg(test)]
#[path = "dispatch_test.rs"]
mod tests;
