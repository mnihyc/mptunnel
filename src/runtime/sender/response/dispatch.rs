//! Response intent preflight and carrier command dispatch.
//!
//! This module never ranks paths. It resolves a planned identity, asks the
//! binding to revalidate and commit, and enqueues one carrier command.

use super::ResponseOutputIdentity;
use super::multipath::ResponseDataDispatchTarget;
use super::response_reinjection_avoid_outputs;
use super::scheduling::{response_completion_snapshot, select_response_frame_path};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::path::CarrierPathKey;
use crate::model::work::{ReliableReinjectionTargetWork, reliable_reinjection_service_limit_bytes};
use crate::protocol::Frame;
use crate::protocol::frame::reliable_stream_frame_accounted_bytes;
#[cfg(feature = "lab-diagnostics")]
use crate::protocol::frame::reliable_stream_frame_extent;
use crate::runtime::RuntimeError;
use crate::runtime::sender::{
    CarrierEmitMode, RelaySendCause, ReliableRelaySenderQueue, ServerReinjectionOutputIdentity,
};
use crate::runtime::stream::response::{
    ResponseDispatchTarget, ResponseSenderPathTarget, record_server_sender_decision,
};
use crate::runtime::stream::{ReliablePathStream, ReliablePathStreamOutput};
use crate::scheduler::TrafficClass;
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ResponseFrameEmitOutcome {
    pub(super) selected_path: Option<CarrierPathKey>,
    pub(super) accepted_copy_deadline: Option<Instant>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ResponseReinjectionServiceModel<'a> {
    pub(super) queue: &'a ReliableRelaySenderQueue,
    pub(super) exclude_front_work: bool,
    pub(super) reinjection_debt_bytes: usize,
}

fn response_reinjection_target_has_service_credit(
    stream: &ReliablePathStream,
    target: &ResponseSenderPathTarget,
    service_model: ResponseReinjectionServiceModel<'_>,
    payload_bytes: usize,
) -> bool {
    let ReliablePathStreamOutput::Switchable(binding) = &stream.output else {
        return true;
    };
    let identity = ServerReinjectionOutputIdentity {
        key: target.observation.key,
        incarnation: target.observation.incarnation,
    };
    reliable_reinjection_service_limit_bytes(
        ReliableReinjectionTargetWork::new(
            Some(response_completion_snapshot(target)),
            service_model
                .queue
                .response_target_queued_reinjection_bytes(
                    identity,
                    service_model.exclude_front_work,
                ),
            binding.accepted_reinjected_data_in_flight_bytes_at(identity),
        ),
        payload_bytes.min(service_model.reinjection_debt_bytes),
        binding.mux_limits(),
    ) >= payload_bytes
}

pub(super) fn select_switchable_response_target(
    stream: &ReliablePathStream,
    lane: TrafficClass,
    frame: &Frame,
    emit_mode: CarrierEmitMode,
    avoid_outputs: &[ResponseOutputIdentity],
    reinjection_cause: Option<RelaySendCause>,
    reinjection_service_model: Option<ResponseReinjectionServiceModel<'_>>,
) -> Option<ResponseSenderPathTarget> {
    let ReliablePathStreamOutput::Switchable(binding) = &stream.output else {
        return None;
    };
    let payload_bytes = reliable_stream_frame_accounted_bytes(frame);
    let mut exhausted_outputs = Vec::<ResponseOutputIdentity>::new();
    loop {
        let targets = binding.sender_path_targets(lane, payload_bytes);
        if targets.is_empty() {
            return None;
        }
        let targets = targets
            .into_iter()
            .filter(|target| {
                !exhausted_outputs
                    .contains(&(target.observation.key, target.observation.incarnation))
            })
            .collect::<Vec<_>>();
        if targets.is_empty() {
            return None;
        }
        let target = select_response_frame_path(
            &targets,
            lane,
            frame,
            emit_mode,
            avoid_outputs,
            reinjection_cause,
        )?;
        if matches!(frame, Frame::StreamData { .. })
            && let Some(service_model) = reinjection_service_model
            && !response_reinjection_target_has_service_credit(
                stream,
                &target,
                service_model,
                payload_bytes,
            )
        {
            exhausted_outputs.push((target.observation.key, target.observation.incarnation));
            continue;
        }
        return Some(target);
    }
}

#[cfg(feature = "lab-diagnostics")]
#[allow(clippy::too_many_arguments)]
fn lab_server_repair_carrier_accept(
    session_id: Option<u64>,
    stream_id: u64,
    frame: &Frame,
    cause: RelaySendCause,
    target: CarrierPathKey,
    target_incarnation: Option<u64>,
    accepted_copy_deadline: Option<Instant>,
) {
    let Some((offset, end, payload_bytes)) = reliable_stream_frame_extent(frame) else {
        return;
    };
    let (owner_underlay, owner_path_id, owner_incarnation) = match cause {
        RelaySendCause::StaleResponsePathReinjection(owner) => (
            format!("{:?}", owner.key.underlay),
            owner.key.path_id.0.to_string(),
            owner.incarnation.to_string(),
        ),
        _ => ("none".to_string(), "none".to_string(), "none".to_string()),
    };
    let deadline_in_us = accepted_copy_deadline
        .map(|deadline| {
            deadline
                .saturating_duration_since(Instant::now())
                .as_micros()
                .to_string()
        })
        .unwrap_or_else(|| "none".to_string());
    lab_diagnostic(
        "server_repair_carrier_accept",
        format_args!(
            "session_id={} stream_id={} cause={} offset={} end={} payload_bytes={} target_underlay={:?} target_path_id={} target_incarnation={} owner_underlay={} owner_path_id={} owner_incarnation={} accepted_copy_deadline_in_us={}",
            session_id
                .map(|session_id| session_id.to_string())
                .unwrap_or_else(|| "none".to_string()),
            stream_id,
            cause.as_str(),
            offset,
            end,
            payload_bytes,
            target.underlay,
            target.path_id.0,
            target_incarnation
                .map(|incarnation| incarnation.to_string())
                .unwrap_or_else(|| "none".to_string()),
            owner_underlay,
            owner_path_id,
            owner_incarnation,
            deadline_in_us,
        ),
    );
}

pub(super) fn response_frame_has_carrier_credit(
    stream: &ReliablePathStream,
    frame: &Frame,
    lane: TrafficClass,
    emit_mode: CarrierEmitMode,
    reinjection_cause: Option<RelaySendCause>,
    reinjection_service_model: Option<ResponseReinjectionServiceModel<'_>>,
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
            let avoid_outputs = reinjection_cause.map_or_else(Vec::new, |cause| {
                response_reinjection_avoid_outputs(binding, frame, cause)
            });
            select_switchable_response_target(
                stream,
                lane,
                frame,
                emit_mode,
                &avoid_outputs,
                reinjection_cause,
                reinjection_service_model,
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
            fixed.try_enqueue_original_data_frame(&frame, lane)?;
            Ok(Some(fixed.key()))
        }
        ResponseDataDispatchTarget::Switchable {
            target,
            expected_model_generation,
            position,
        } => {
            let ReliablePathStreamOutput::Switchable(binding) = &stream.output else {
                return Err(RuntimeError::SenderServiceBlocked);
            };
            let enqueue_result = binding.try_enqueue_data_frame_for_dispatch_target(
                &target,
                &frame,
                lane,
                expected_model_generation,
                position,
            );
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
    reinjection_service_model: Option<ResponseReinjectionServiceModel<'_>>,
) -> Result<ResponseFrameEmitOutcome, RuntimeError> {
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
            let accepted_copy_deadline = if matches!(frame, Frame::StreamData { .. }) {
                let accepted_copy_deadline = if reinjection {
                    let Some(service_model) = reinjection_service_model else {
                        return Err(RuntimeError::SenderServiceBlocked);
                    };
                    let identity = fixed.reinjection_output_identity();
                    let queued_reinjection_bytes = service_model
                        .queue
                        .response_target_queued_reinjection_bytes(
                            identity,
                            service_model.exclude_front_work,
                        );
                    Some(fixed.try_enqueue_reinjected_frame(
                        &frame,
                        lane,
                        queued_reinjection_bytes,
                        service_model.reinjection_debt_bytes,
                    )?)
                } else {
                    let command = fixed
                        .commands()
                        .try_reserve_stream_ordered_frame(frame.clone(), lane)?;
                    fixed.record_original_flight(&frame);
                    command.commit();
                    None
                };
                #[cfg(feature = "lab-diagnostics")]
                if let Some(cause) = reinjection_cause {
                    lab_server_repair_carrier_accept(
                        None,
                        stream.stream_id.0,
                        &frame,
                        cause,
                        fixed.key(),
                        None,
                        accepted_copy_deadline,
                    );
                }
                accepted_copy_deadline
            } else {
                emit_mode.try_enqueue_frame(fixed.commands(), frame, lane)?;
                None
            };
            Ok(ResponseFrameEmitOutcome {
                selected_path: Some(fixed.key()),
                accepted_copy_deadline,
            })
        }
        ReliablePathStreamOutput::Switchable(binding) => {
            let avoid_outputs = reinjection_cause.map_or_else(Vec::new, |cause| {
                response_reinjection_avoid_outputs(binding, &frame, cause)
            });
            let mut last_error = None;
            loop {
                let Some(target) = select_switchable_response_target(
                    stream,
                    lane,
                    &frame,
                    emit_mode,
                    &avoid_outputs,
                    reinjection_cause,
                    reinjection_service_model,
                ) else {
                    let _ = last_error;
                    return Err(RuntimeError::SenderServiceBlocked);
                };
                let dispatch_target = ResponseDispatchTarget::from(&target);
                let send_result = if matches!(frame, Frame::StreamData { .. }) {
                    let Some(service_model) = reinjection_service_model else {
                        return Err(RuntimeError::SenderServiceBlocked);
                    };
                    let identity = ServerReinjectionOutputIdentity {
                        key: target.observation.key,
                        incarnation: target.observation.incarnation,
                    };
                    let queued_reinjection_bytes = service_model
                        .queue
                        .response_target_queued_reinjection_bytes(
                            identity,
                            service_model.exclude_front_work,
                        );
                    binding
                        .try_enqueue_reinjected_frame_for_target(
                            &dispatch_target,
                            &frame,
                            lane,
                            queued_reinjection_bytes,
                            service_model.reinjection_debt_bytes,
                        )
                        .map(Some)
                } else {
                    match emit_mode {
                        CarrierEmitMode::Classified => binding
                            .try_enqueue_classified_frame_for_target(
                                &dispatch_target,
                                frame.clone(),
                                lane,
                            )
                            .map(|()| None),
                        CarrierEmitMode::StreamOrdered => binding
                            .try_enqueue_stream_ordered_frame_for_target(
                                &dispatch_target,
                                frame.clone(),
                                lane,
                            )
                            .map(|()| None),
                    }
                };
                match send_result {
                    Ok(accepted_copy_deadline) => {
                        record_server_sender_decision(
                            binding.session_id(),
                            stream.stream_id,
                            target.observation.key,
                            &frame,
                            lane,
                            reason,
                            Some(target.observation.has_bulk_rate_evidence),
                        );
                        #[cfg(feature = "lab-diagnostics")]
                        if let Some(cause) = reinjection_cause {
                            lab_server_repair_carrier_accept(
                                Some(binding.session_id().0),
                                stream.stream_id.0,
                                &frame,
                                cause,
                                target.observation.key,
                                Some(target.observation.incarnation),
                                accepted_copy_deadline,
                            );
                        }
                        return Ok(ResponseFrameEmitOutcome {
                            selected_path: Some(target.observation.key),
                            accepted_copy_deadline,
                        });
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

#[cfg(test)]
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
        None,
    )
    .map(|outcome| outcome.selected_path)
}

#[cfg(test)]
#[path = "tests_dispatch.rs"]
mod tests;
