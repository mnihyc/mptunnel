//! Response intent preflight and carrier command dispatch.
//!
//! This module never ranks paths. It resolves a planned identity, asks the
//! binding to revalidate and commit, and enqueues one carrier command.

use super::*;

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
    emit_mode: ResponseCarrierEmitMode,
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
            ResponseCarrierEmitMode::Classified => {
                fixed.commands().can_enqueue_frame_now(frame, lane)
            }
            ResponseCarrierEmitMode::StreamOrdered => {
                fixed.commands().can_enqueue_stream_ordered_frame_now(lane)
            }
        },
        ReliablePathStreamOutput::Switchable(binding) => {
            let payload_bytes = reliable_stream_frame_payload_bytes(frame);
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

pub(super) async fn emit_planned_response_data_frame(
    stream: &ReliablePathStream,
    planned: ResponseDataDispatchPlan,
    frame: Frame,
    lane: FlowLane,
) -> Result<ResponseDataEmitOutcome, RuntimeError> {
    let ResponseDataDispatchPlan { primary } = planned;
    match primary {
        ResponseDataDispatchTarget::Fixed(fixed) => {
            send_sender_service_frame_to_carrier(
                fixed.commands(),
                frame.clone(),
                lane,
                ResponseCarrierEmitMode::StreamOrdered,
            )
            .await?;
            fixed.record_owner_flight(&frame);
            Ok(ResponseDataEmitOutcome {
                selected_path: Some(fixed.key()),
            })
        }
        ResponseDataDispatchTarget::Switchable {
            binding,
            target,
            role,
            service_handoff_commit,
            subflow_set_commit,
            ack_clock_calibration_commit,
        } => {
            let subflow_request =
                subflow_set_commit.map(|commit| ResponseSubflowAdmissionRequest {
                    expected_planner_generation: commit.planner_generation,
                    expected_lane_generation: commit.lane_generation,
                    service: commit.service,
                    startup_owner_credit_bytes: commit.startup_owner_credit_bytes,
                    optional_overhead_budget_bytes: commit.optional_overhead_budget_bytes,
                    max_read_gap_budget: commit.max_read_gap_budget,
                    input: commit.input,
                });
            let calibration_request =
                ack_clock_calibration_commit.map(|commit| ResponseAckClockCalibrationRequest {
                    expected_planner_generation: commit.planner_generation,
                    expected_lane_generation: commit.lane_generation,
                    expected_model_generation: commit.model_generation,
                    service: commit.service,
                    service_incarnation: commit.service_incarnation,
                    service_pending_bytes: commit.service_pending_bytes,
                    target_pending_bytes: commit.target_pending_bytes,
                    limit_bytes: commit.limit_bytes,
                    requires_active_response_start: commit.requires_active_response_start,
                });
            let calibrating = calibration_request.is_some();
            let handoff = service_handoff_commit.is_some();
            let enqueue_result = if let Some(commit) = service_handoff_commit {
                binding
                    .try_enqueue_response_service_handoff_for_dispatch(
                        &target,
                        &frame,
                        lane,
                        ResponseServiceHandoffRequest {
                            expected_planner_generation: commit.planner_generation,
                            expected_lane_generation: commit.lane_generation,
                            expected_model_generation: commit.model_generation,
                            handoff_frontier: commit.handoff_frontier,
                            service: commit.service,
                            service_path_instance_id: commit.service_path_instance_id,
                            service_incarnation: commit.service_incarnation,
                            target: target.key,
                            target_path_instance_id: commit.target_path_instance_id,
                            target_incarnation: target.incarnation,
                            mode: commit.mode,
                            target_command_pending_limit_bytes: commit
                                .target_command_pending_limit_bytes,
                            capacity_proof: commit.capacity_proof,
                        },
                    )
                    .map(|()| None)
            } else {
                binding.try_enqueue_owner_frame_for_dispatch_target(
                    &target,
                    &frame,
                    lane,
                    subflow_request,
                    calibration_request,
                )
            };
            match enqueue_result {
                Ok(_) => {}
                Err(RuntimeError::SenderServiceBlocked) => {
                    return Err(RuntimeError::SenderServiceBlocked);
                }
                Err(_) => {
                    binding.detach(target.key, &target.commands);
                    return Err(RuntimeError::SenderServiceBlocked);
                }
            }
            if role == PathRuntimeRole::Service {
                let _ = binding.commit_ordered_data_owner_for_dispatch_target(&target);
            }
            let decision_reason = match role {
                PathRuntimeRole::Service if handoff => "data_service_handoff",
                PathRuntimeRole::Service => "data_service",
                PathRuntimeRole::Subflow if calibrating => "data_subflow_ack_clock_calibration",
                PathRuntimeRole::Subflow => "data_subflow",
                PathRuntimeRole::Probe
                | PathRuntimeRole::RepairOnly
                | PathRuntimeRole::Standby
                | PathRuntimeRole::Failed => "data",
            };
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

pub(super) async fn emit_response_frame_from_sender_service(
    stream: &ReliablePathStream,
    frame: Frame,
    lane: FlowLane,
    emit_mode: ResponseCarrierEmitMode,
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
        ResponseCarrierEmitMode::StreamOrdered
    } else {
        emit_mode
    };
    match &stream.output {
        ReliablePathStreamOutput::Fixed(fixed) => {
            send_sender_service_frame_to_carrier(fixed.commands(), frame.clone(), lane, emit_mode)
                .await?;
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
            let payload_bytes = reliable_stream_frame_payload_bytes(&frame);
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
                            .try_enqueue_repair_frame_for_target(&target, &frame, lane)
                            .map(|()| None)
                    } else {
                        binding.try_enqueue_owner_frame_for_dispatch_target(
                            &dispatch_target,
                            &frame,
                            lane,
                            None,
                            None,
                        )
                    }
                } else {
                    send_sender_service_frame_to_carrier(
                        &target.commands,
                        frame.clone(),
                        lane,
                        emit_mode,
                    )
                    .await
                    .map(|()| None)
                };
                match send_result {
                    Ok(_) => {
                        if matches!(frame, Frame::StreamData { .. }) {
                            if !repair {
                                let _ = binding.commit_ordered_data_owner_for_dispatch_target(
                                    &dispatch_target,
                                );
                            }
                        }
                        record_server_sender_decision(
                            binding.session_id(),
                            stream.stream_id,
                            target.key,
                            &frame,
                            lane,
                            reason,
                            Some(target.has_bulk_rate_evidence),
                        );
                        return Ok(Some(target.key));
                    }
                    Err(RuntimeError::SenderServiceBlocked) => {
                        return Err(RuntimeError::SenderServiceBlocked);
                    }
                    Err(err) => {
                        last_error = Some(err);
                        binding.detach(target.key, &target.commands);
                    }
                }
            }
        }
    }
}

async fn send_sender_service_frame_to_carrier(
    commands: &ReliablePathCommandSender,
    frame: Frame,
    lane: FlowLane,
    emit_mode: ResponseCarrierEmitMode,
) -> Result<(), RuntimeError> {
    // Sender-service dispatch must not await a path queue permit; queue-full is
    // explicit backpressure so the owner can keep work queued and continue
    // polling ACK/control/path feedback.
    match emit_mode {
        ResponseCarrierEmitMode::Classified => commands.try_enqueue_admitted_frame(frame, lane),
        ResponseCarrierEmitMode::StreamOrdered => {
            commands.try_enqueue_stream_ordered_frame(frame, lane)
        }
    }
}

pub(in crate::runtime) async fn send_sender_service_control_frame(
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
        ResponseCarrierEmitMode::Classified,
        "control",
        None,
    )
    .await
}

pub(in crate::runtime) async fn emit_relay_path_frame(
    stream: &ReliablePathStreamHandle,
    frame: Frame,
    lane: FlowLane,
) -> Result<(), RuntimeError> {
    emit_relay_path_frame_with_mode(stream, frame, lane, ResponseCarrierEmitMode::Classified).await
}

pub(in crate::runtime) async fn emit_relay_path_frame_with_mode(
    stream: &ReliablePathStreamHandle,
    frame: Frame,
    lane: FlowLane,
    emit_mode: ResponseCarrierEmitMode,
) -> Result<(), RuntimeError> {
    match &stream.output {
        ReliablePathStreamOutput::Fixed(fixed) => {
            send_sender_service_frame_to_carrier(fixed.commands(), frame, lane, emit_mode).await
        }
        ReliablePathStreamOutput::Switchable(_) => {
            Err(RuntimeError::Protocol("request relay path is not fixed"))
        }
    }
}

pub(in crate::runtime) fn relay_cursor_distance(
    position: usize,
    cursor: usize,
    len: usize,
) -> usize {
    if len == 0 {
        return 0;
    }
    position.wrapping_add(len).wrapping_sub(cursor % len) % len
}
