//! Server response sender queue and orchestration facade.
//!
//! The service owns queued product work. Planning and carrier dispatch remain
//! separate so queue mutation cannot silently become path-selection policy.

use super::dispatch::{
    emit_planned_response_data_frame, emit_response_frame_from_sender_service,
    response_frame_has_carrier_credit,
};
use super::multipath::{
    ResponseMultipathPlanError, plan_response_data_payload_with_data_ack_outstanding_impl,
};
use super::response_reinjection_avoid_outputs;
use super::scheduling::{
    ResponseOrdinarySaturationObservation, response_completion_snapshot, select_response_frame_path,
};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::{
    lab_diagnostic, lab_diagnostic_event_enabled, lab_perf_record, lab_sender_service_decision,
    lab_server_response_stream_data,
};
use crate::model::capacity::adaptive_reliable_relay_chunk_bytes_with_frame_limit;
use crate::model::multipath::ExtraTrafficLedger;
use crate::model::path::CarrierPathKey;
use crate::model::tcp_carrier::TcpCarrierStableGenerations;
use crate::model::timing::reliable_data_retransmission_interval;
use crate::model::work::ReliableWorkClass;
use crate::model::work::reliable_failed_original_reinjection_limit_bytes;
use crate::mux::MuxLimits;
use crate::mux::stream::ReliableSendStream;
use crate::performance::MppPerformanceConfig;
#[cfg(feature = "lab-diagnostics")]
use crate::protocol::frame::reliable_path_frame_pacing_bytes;
use crate::protocol::frame::reliable_stream_frame_accounted_bytes;
use crate::protocol::{Frame, OffsetRange, SessionId, StreamId};
use crate::runtime::RuntimeError;
use crate::runtime::path::commands::reliable_path_effective_frame_lane;
use crate::runtime::sender::{
    CarrierEmitMode, RelaySendCause, ReliableRelayQueuedWork, ReliableRelayQueuedWorkKind,
    ReliableRelaySenderQueue, ServerReinjectionOutputIdentity,
    reliable_relay_can_read_product_source, reliable_relay_sender_queue_read_budget,
    sender_extra_traffic_startup_floor_bytes, sender_reinjection_minimum_useful_attempt_bytes,
};
use crate::runtime::stream::response::ResponseSenderPathTarget;
use crate::runtime::stream::{ReliablePathStream, ReliablePathStreamOutput};
use crate::scheduler::{self, PathSnapshot, TrafficClass};
use bytes::Bytes;
use std::time::{Duration, Instant};

fn response_data_dispatch_lane(
    queued_lane: Option<TrafficClass>,
    current_lane: TrafficClass,
) -> TrafficClass {
    // Promotion is current flow evidence, so staged startup bytes must not keep
    // the stream trapped behind the conservative latency-lane carrier prior.
    match (queued_lane, current_lane) {
        (Some(TrafficClass::Throughput), _) | (_, TrafficClass::Throughput) => {
            TrafficClass::Throughput
        }
        (Some(TrafficClass::Background), _) | (_, TrafficClass::Background) => {
            TrafficClass::Background
        }
        (Some(queued_lane), _) => queued_lane,
        (None, current_lane) => current_lane,
    }
}

fn response_dispatch_payload_bytes(
    path_stream: &ReliablePathStream,
    send_stream: &ReliableSendStream,
    relay_lane: TrafficClass,
    mux_limits: MuxLimits,
    queued_payload_bytes: usize,
) -> Option<usize> {
    let reinjection_credit = mux_limits
        .max_repair_bytes
        .saturating_sub(send_stream.reinjection_bytes());
    if reinjection_credit == 0 {
        return None;
    }
    let snapshot = path_stream.send_path_snapshot(relay_lane, queued_payload_bytes);
    Some(
        adaptive_reliable_relay_chunk_bytes_with_frame_limit(
            snapshot,
            relay_lane,
            mux_limits,
            path_stream.max_frame_payload_bytes,
        )
        .min(queued_payload_bytes)
        .min(reinjection_credit)
        .max(1),
    )
}

#[derive(Debug)]
/// Current server response sender-service boundary.
///
/// Target reads enqueue STREAM_DATA here before any carrier path write. The
/// service owns queueing and source-stream mutation. The multipath transaction
/// plans path work; the binding revalidates and atomically commits exact ranges.
pub(in crate::runtime) struct ServerResponseSenderService {
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(in crate::runtime::sender) session_id: SessionId,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(in crate::runtime::sender) stream_id: StreamId,
    pub(in crate::runtime::sender) queue: ReliableRelaySenderQueue,
    pub(in crate::runtime::sender) performance: MppPerformanceConfig,
    pub(in crate::runtime::sender) extra_traffic: ExtraTrafficLedger,
    stale_response_recovery_generation: u64,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ServerResponseDispatch {
    pub(in crate::runtime) payload_bytes: usize,
    pub(in crate::runtime) lane: ReliableWorkClass,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(in crate::runtime) selected_path: Option<CarrierPathKey>,
    pub(in crate::runtime) tcp_carrier_stable: Option<TcpCarrierStableGenerations>,
}

pub(in crate::runtime) enum ServerQueuedDispatch {
    Dispatched(ServerResponseDispatch),
    OrdinarySaturation(Box<ResponseOrdinarySaturationObservation>),
}

pub(in crate::runtime) enum ServerCarrierReadiness {
    Ready,
    OrdinarySaturation(Box<ResponseOrdinarySaturationObservation>),
    Blocked,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ServerAckGapReinjectionTarget {
    pub(in crate::runtime) identity: ServerReinjectionOutputIdentity,
    pub(in crate::runtime) snapshot: PathSnapshot,
    pub(in crate::runtime) completion: Duration,
}

#[derive(Debug, Default)]
pub(in crate::runtime) struct StaleResponseRecoveryOutcome {
    pub(in crate::runtime) queued: bool,
    pub(in crate::runtime) retry_deadline: Option<Instant>,
    pub(in crate::runtime) blocked_for_carrier_capacity: bool,
}

impl ServerResponseSenderService {
    #[cfg(test)]
    pub(in crate::runtime) fn new(session_id: SessionId, stream_id: StreamId) -> Self {
        Self::new_with_performance(session_id, stream_id, MppPerformanceConfig::default())
    }

    pub(in crate::runtime) fn new_with_performance(
        session_id: SessionId,
        stream_id: StreamId,
        performance: MppPerformanceConfig,
    ) -> Self {
        Self {
            session_id,
            stream_id,
            queue: ReliableRelaySenderQueue::default(),
            performance,
            extra_traffic: ExtraTrafficLedger::default(),
            stale_response_recovery_generation: 0,
        }
    }

    pub(in crate::runtime) fn stale_response_recovery_generation(&self) -> u64 {
        self.stale_response_recovery_generation
    }

    pub(in crate::runtime) fn ack_gap_reinjection_path_snapshot(
        &self,
        path_stream: &ReliablePathStream,
        send_stream: &ReliableSendStream,
        normalized_ranges: &[OffsetRange],
        preview_limit: usize,
    ) -> Option<ServerAckGapReinjectionTarget> {
        let preview = send_stream
            .retransmission_frames_for_normalized_ack_gaps(normalized_ranges, preview_limit.max(1))
            .into_iter()
            .next()?;
        let target = self.reinjection_path_target_for_frame(
            path_stream,
            &preview,
            RelaySendCause::PersistentAckGapReinjection,
        )?;
        let lane = path_stream.current_lane();
        let score = scheduler::score_path(
            response_completion_snapshot(&target),
            lane,
            reliable_stream_frame_accounted_bytes(&preview),
        )?;
        if !score.eta_ms.is_finite() {
            return None;
        }
        Some(ServerAckGapReinjectionTarget {
            identity: ServerReinjectionOutputIdentity {
                key: target.observation.key,
                incarnation: target.observation.incarnation,
            },
            snapshot: target.observation.snapshot,
            completion: Duration::from_secs_f64(score.eta_ms.max(0.0) / 1000.0),
        })
    }

    pub(in crate::runtime) fn reinjection_path_snapshot_for_frame(
        &self,
        path_stream: &ReliablePathStream,
        preview: &Frame,
        cause: RelaySendCause,
    ) -> Option<(ServerReinjectionOutputIdentity, PathSnapshot)> {
        self.reinjection_path_target_for_frame(path_stream, preview, cause)
            .map(|target| {
                (
                    ServerReinjectionOutputIdentity {
                        key: target.observation.key,
                        incarnation: target.observation.incarnation,
                    },
                    target.observation.snapshot,
                )
            })
    }

    fn reinjection_path_target_for_frame(
        &self,
        path_stream: &ReliablePathStream,
        preview: &Frame,
        cause: RelaySendCause,
    ) -> Option<ResponseSenderPathTarget> {
        let ReliablePathStreamOutput::Switchable(binding) = &path_stream.output else {
            return None;
        };
        let avoid_outputs = response_reinjection_avoid_outputs(binding, preview, cause);
        let lane = path_stream.current_lane();
        let targets =
            binding.sender_path_targets(lane, reliable_stream_frame_accounted_bytes(preview));
        select_response_frame_path(
            &targets,
            lane,
            preview,
            CarrierEmitMode::Classified,
            &avoid_outputs,
            Some(cause),
        )
    }

    pub(in crate::runtime) fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(in crate::runtime) fn bytes(&self) -> usize {
        self.queue.bytes()
    }

    pub(in crate::runtime) fn data_bytes(&self) -> usize {
        self.queue.data_bytes()
    }

    pub(in crate::runtime) fn release_normalized_acked_reinjections(
        &mut self,
        ranges: &[OffsetRange],
    ) -> usize {
        self.queue.release_normalized_acked_reinjections(ranges)
    }

    pub(in crate::runtime) fn discard_unusable_tail_reinjections(
        &mut self,
        path_stream: &ReliablePathStream,
    ) -> usize {
        self.queue.discard_unusable_tail_reinjections(|frame| {
            path_stream.has_tail_reinjection_output_for_frame(frame)
        })
    }

    pub(in crate::runtime) fn discard_stale_persistent_ack_gap_reinjections(
        &mut self,
        path_stream: &ReliablePathStream,
    ) -> usize {
        self.queue
            .discard_stale_persistent_ack_gap_reinjections(|cause| {
                cause.persistent_server_target().is_none_or(|target| {
                    path_stream.has_output_incarnation(target.key, target.incarnation)
                }) && cause.persistent_client_target().is_none()
            })
    }

    pub(in crate::runtime) fn discard_resolved_stale_output_reinjections(
        &mut self,
        path_stream: &ReliablePathStream,
    ) -> usize {
        let active = path_stream.stale_response_original_outputs();
        self.queue
            .discard_resolved_stale_response_path_reinjections(|identity| {
                active.contains(&identity)
            })
    }

    pub(in crate::runtime) fn persistent_ack_gap_reinjection_deadline(&self) -> Option<Instant> {
        self.queue.persistent_ack_gap_reinjection_deadline()
    }

    pub(in crate::runtime) fn extra_traffic_budget_remaining(
        &self,
        mux_limits: MuxLimits,
    ) -> usize {
        self.extra_traffic
            .budget(
                sender_extra_traffic_startup_floor_bytes(mux_limits),
                self.performance,
            )
            .remaining_bytes()
    }

    pub(in crate::runtime) fn reinjection_extra_budget_remaining(
        &self,
        mux_limits: MuxLimits,
    ) -> usize {
        self.extra_traffic_budget_remaining(mux_limits)
    }

    pub(in crate::runtime) fn reinjection_extra_event_budget_remaining(
        &self,
        mux_limits: MuxLimits,
    ) -> usize {
        let remaining = self.reinjection_extra_budget_remaining(mux_limits);
        if remaining < sender_reinjection_minimum_useful_attempt_bytes(mux_limits) {
            0
        } else {
            remaining
        }
    }

    pub(in crate::runtime) fn record_delivered_data(&mut self, bytes: usize) {
        self.extra_traffic.record_delivered_data(bytes);
    }

    pub(in crate::runtime) fn publish_queue_bytes(&self, path_stream: &ReliablePathStream) {
        path_stream.set_sender_queue_bytes(self.queue.bytes());
    }

    pub(in crate::runtime) fn queued_send_ready(&self) -> bool {
        self.queue.front().is_some()
    }

    pub(in crate::runtime) fn front_carrier_readiness_with_tcp_observation(
        &self,
        path_stream: &ReliablePathStream,
        send_stream: &ReliableSendStream,
        relay_lane: TrafficClass,
        mux_limits: MuxLimits,
        data_ack_outstanding_bytes: usize,
    ) -> ServerCarrierReadiness {
        let Some((_, queued)) = self.queue.front() else {
            return ServerCarrierReadiness::Blocked;
        };
        match &queued.kind {
            ReliableRelayQueuedWorkKind::Control(frame) => {
                let (carrier_lane, emit_mode) = if queued.stream_ordered_carrier_emit {
                    (relay_lane, CarrierEmitMode::StreamOrdered)
                } else {
                    (TrafficClass::Control, CarrierEmitMode::Classified)
                };
                if response_frame_has_carrier_credit(
                    path_stream,
                    frame,
                    carrier_lane,
                    emit_mode,
                    None,
                ) {
                    ServerCarrierReadiness::Ready
                } else {
                    ServerCarrierReadiness::Blocked
                }
            }
            ReliableRelayQueuedWorkKind::Data(payload) => {
                let data_lane = response_data_dispatch_lane(queued.data_lane, relay_lane);
                let Some(payload_bytes) = response_dispatch_payload_bytes(
                    path_stream,
                    send_stream,
                    data_lane,
                    mux_limits,
                    payload.len(),
                ) else {
                    return ServerCarrierReadiness::Blocked;
                };
                match plan_response_data_payload_with_data_ack_outstanding_impl(
                    path_stream,
                    data_lane,
                    send_stream.next_offset(),
                    payload_bytes,
                    data_ack_outstanding_bytes,
                ) {
                    Ok(_) => ServerCarrierReadiness::Ready,
                    Err(ResponseMultipathPlanError::OrdinarySaturation(saturation)) => {
                        ServerCarrierReadiness::OrdinarySaturation(saturation)
                    }
                    Err(ResponseMultipathPlanError::Runtime(_)) => ServerCarrierReadiness::Blocked,
                }
            }
            ReliableRelayQueuedWorkKind::Reinjection { frame, cause } => {
                if response_frame_has_carrier_credit(
                    path_stream,
                    frame,
                    relay_lane,
                    CarrierEmitMode::Classified,
                    Some(*cause),
                ) {
                    ServerCarrierReadiness::Ready
                } else {
                    ServerCarrierReadiness::Blocked
                }
            }
        }
    }

    pub(in crate::runtime) fn can_read_product_source(
        &self,
        local_open: bool,
        queued_send_blocked: bool,
        send_stream: &ReliableSendStream,
        queue_limit: usize,
    ) -> bool {
        reliable_relay_can_read_product_source(
            local_open,
            queued_send_blocked,
            send_stream,
            &self.queue,
            queue_limit,
        )
    }

    pub(in crate::runtime) fn read_budget(
        &self,
        send_stream: &ReliableSendStream,
        queue_limit: usize,
        buffer_len: usize,
    ) -> usize {
        reliable_relay_sender_queue_read_budget(send_stream, &self.queue, queue_limit, buffer_len)
    }

    pub(in crate::runtime) fn enqueue_data_for_lane(
        &mut self,
        payload: Bytes,
        lane: TrafficClass,
    ) -> u64 {
        self.queue.push_data_for_lane(payload, lane)
    }

    pub(in crate::runtime) fn enqueue_final_control_frame(&mut self, frame: Frame) -> u64 {
        self.queue.push_final_control(frame)
    }

    pub(in crate::runtime) fn enqueue_reinjection_frame_with_priority(
        &mut self,
        frame: Frame,
        mux_limits: MuxLimits,
        critical_priority: bool,
    ) -> Option<u64> {
        let payload_bytes = reliable_stream_frame_accounted_bytes(&frame);
        let budget = self.extra_traffic.budget(
            sender_extra_traffic_startup_floor_bytes(mux_limits),
            self.performance,
        );
        if !budget.can_spend(payload_bytes) {
            return None;
        }
        self.extra_traffic.record_reinjection(payload_bytes);
        Some(if critical_priority {
            self.queue
                .push_critical_reinjection_with_cause(frame, RelaySendCause::AckGapReinjection)
        } else {
            self.queue.push_reinjection(frame)
        })
    }

    pub(in crate::runtime) fn enqueue_critical_tail_reinjection_frame(
        &mut self,
        frame: Frame,
    ) -> Option<u64> {
        if self.has_queued_reinjection_overlap(&frame) {
            return None;
        }
        Some(self.enqueue_critical_reinjection_frame_with_cause(
            frame,
            RelaySendCause::PathFailureReinjection,
        ))
    }

    pub(in crate::runtime) fn enqueue_critical_reinjection_frame_with_cause(
        &mut self,
        frame: Frame,
        cause: RelaySendCause,
    ) -> u64 {
        debug_assert!(cause.is_reinjection());
        let payload_bytes = reliable_stream_frame_accounted_bytes(&frame);
        self.extra_traffic.record_reinjection(payload_bytes);
        self.queue
            .push_critical_reinjection_with_cause(frame, cause)
    }

    /// Reinjects exact OriginalData owned by a connection-level stale output.
    /// The native TCP/QUIC sender remains alive; retained product ranges,
    /// alternate carrier credit, queue bounds, and the owner's recovery clock
    /// constrain this work.
    pub(in crate::runtime) fn drive_stale_output_recovery(
        &mut self,
        path_stream: &ReliablePathStream,
        send_stream: &ReliableSendStream,
        mux_limits: MuxLimits,
    ) -> StaleResponseRecoveryOutcome {
        let mut outcome = StaleResponseRecoveryOutcome::default();
        for identity in path_stream.stale_response_original_outputs() {
            let owner_path =
                path_stream.response_output_snapshot(identity, path_stream.current_lane());
            let retry_after =
                reliable_data_retransmission_interval(Some(identity.key.underlay), owner_path);
            let recovery = path_stream.stale_original_recovery_state(identity, retry_after);
            outcome.retry_deadline = match (outcome.retry_deadline, recovery.retry_deadline) {
                (Some(current), Some(deadline)) => Some(current.min(deadline)),
                (None, deadline) => deadline,
                (current, None) => current,
            };
            if recovery.uncovered_ranges.is_empty() {
                continue;
            }

            let cause = RelaySendCause::StaleResponsePathReinjection(identity);
            let preview = send_stream
                .retransmission_frames_for_ranges(
                    &recovery.uncovered_ranges,
                    mux_limits.max_repair_bytes.max(1),
                )
                .into_iter()
                .find(|frame| !self.has_queued_reinjection_overlap(frame));
            let Some(preview) = preview else {
                continue;
            };
            let Some((_, reinjection_path)) =
                self.reinjection_path_snapshot_for_frame(path_stream, &preview, cause)
            else {
                outcome.blocked_for_carrier_capacity = true;
                continue;
            };
            let reinjection_limit = reliable_failed_original_reinjection_limit_bytes(
                Some(reinjection_path),
                send_stream.reinjection_bytes(),
                mux_limits,
            );
            for frame in send_stream
                .retransmission_frames_for_ranges(&recovery.uncovered_ranges, reinjection_limit)
            {
                if self.has_queued_reinjection_overlap(&frame) {
                    continue;
                }
                self.enqueue_critical_reinjection_frame_with_cause(frame, cause);
                outcome.queued = true;
            }
        }
        outcome
    }

    pub(in crate::runtime) fn has_queued_reinjection_overlap(&self, frame: &Frame) -> bool {
        self.queue.has_queued_reinjection_overlap(frame)
    }

    pub(in crate::runtime) fn dispatch_next(
        &mut self,
        path_stream: &ReliablePathStream,
        send_stream: &mut ReliableSendStream,
        relay_lane: TrafficClass,
        mux_limits: MuxLimits,
    ) -> Result<ServerResponseDispatch, RuntimeError> {
        self.dispatch_next_with_data_ack_outstanding(
            path_stream,
            send_stream,
            relay_lane,
            mux_limits,
            0,
        )
    }

    pub(in crate::runtime) fn dispatch_next_with_data_ack_outstanding(
        &mut self,
        path_stream: &ReliablePathStream,
        send_stream: &mut ReliableSendStream,
        relay_lane: TrafficClass,
        mux_limits: MuxLimits,
        data_ack_outstanding_bytes: usize,
    ) -> Result<ServerResponseDispatch, RuntimeError> {
        match self.dispatch_next_attempt_with_data_ack_outstanding(
            path_stream,
            send_stream,
            relay_lane,
            mux_limits,
            data_ack_outstanding_bytes,
        )? {
            ServerQueuedDispatch::Dispatched(dispatch) => Ok(dispatch),
            ServerQueuedDispatch::OrdinarySaturation(_) => Err(RuntimeError::SenderServiceBlocked),
        }
    }

    pub(in crate::runtime) fn dispatch_next_with_tcp_carrier_observation(
        &mut self,
        path_stream: &ReliablePathStream,
        send_stream: &mut ReliableSendStream,
        relay_lane: TrafficClass,
        mux_limits: MuxLimits,
        data_ack_outstanding_bytes: usize,
    ) -> Result<ServerQueuedDispatch, RuntimeError> {
        self.dispatch_next_attempt_with_data_ack_outstanding(
            path_stream,
            send_stream,
            relay_lane,
            mux_limits,
            data_ack_outstanding_bytes,
        )
    }

    fn dispatch_next_attempt_with_data_ack_outstanding(
        &mut self,
        path_stream: &ReliablePathStream,
        send_stream: &mut ReliableSendStream,
        relay_lane: TrafficClass,
        mux_limits: MuxLimits,
        data_ack_outstanding_bytes: usize,
    ) -> Result<ServerQueuedDispatch, RuntimeError> {
        let (queued_lane, queued) = self
            .queue
            .front()
            .expect("queued_send_ready requires a queued frame");
        let enqueue_id = {
            #[cfg(feature = "lab-diagnostics")]
            {
                queued.enqueue_id
            }
            #[cfg(not(feature = "lab-diagnostics"))]
            {
                0
            }
        };
        let queue_delay_ms = {
            #[cfg(feature = "lab-diagnostics")]
            {
                queued.queued_at.elapsed().as_millis()
            }
            #[cfg(not(feature = "lab-diagnostics"))]
            {
                0
            }
        };
        let (frame, dispatch_lane_name, reinjection_cause) = match &queued.kind {
            ReliableRelayQueuedWorkKind::Control(frame) => (frame.clone(), "control", None),
            ReliableRelayQueuedWorkKind::Data(payload) => {
                let data_lane = response_data_dispatch_lane(queued.data_lane, relay_lane);
                let dispatch_payload_bytes = response_dispatch_payload_bytes(
                    path_stream,
                    send_stream,
                    data_lane,
                    mux_limits,
                    payload.len(),
                )
                .ok_or(RuntimeError::SenderServiceBlocked)?;
                let (dispatch_payload_bytes, planned, tcp_carrier_stable) =
                    match plan_response_data_payload_with_data_ack_outstanding_impl(
                        path_stream,
                        data_lane,
                        send_stream.next_offset(),
                        dispatch_payload_bytes,
                        data_ack_outstanding_bytes,
                    ) {
                        Ok(plan) => plan,
                        Err(ResponseMultipathPlanError::OrdinarySaturation(saturation)) => {
                            return Ok(ServerQueuedDispatch::OrdinarySaturation(saturation));
                        }
                        Err(ResponseMultipathPlanError::Runtime(error)) => return Err(error),
                    };
                let dispatch_payload = payload.slice(..dispatch_payload_bytes);
                #[cfg(feature = "lab-diagnostics")]
                let mux_started = Instant::now();
                let frame = send_stream.send_data(dispatch_payload)?;
                #[cfg(feature = "lab-diagnostics")]
                lab_perf_record(
                    "mux.send_data",
                    mux_started.elapsed(),
                    dispatch_payload_bytes,
                );
                match emit_planned_response_data_frame(
                    path_stream,
                    planned,
                    frame.clone(),
                    reliable_path_effective_frame_lane(&frame, data_lane),
                ) {
                    Ok(selected_path) => {
                        let committed = self
                            .queue
                            .commit_front_data_prefix(dispatch_payload_bytes)
                            .expect("dispatched queued data must still be at queue front");
                        return self
                            .finish_dispatched_work(
                                path_stream,
                                relay_lane,
                                queued_lane,
                                committed,
                                frame,
                                selected_path,
                                "data",
                                enqueue_id,
                                queue_delay_ms,
                                tcp_carrier_stable,
                            )
                            .map(ServerQueuedDispatch::Dispatched);
                    }
                    Err(err) => {
                        let _ = send_stream.rollback_committed_data(&frame);
                        return Err(err);
                    }
                }
            }
            ReliableRelayQueuedWorkKind::Reinjection { frame, cause } => {
                (frame.clone(), "reinjection", Some(*cause))
            }
        };
        let selected_path = match queued_lane {
            ReliableWorkClass::Control => {
                let (carrier_lane, emit_mode) = if queued.stream_ordered_carrier_emit {
                    (relay_lane, CarrierEmitMode::StreamOrdered)
                } else {
                    (TrafficClass::Control, CarrierEmitMode::Classified)
                };
                emit_response_frame_from_sender_service(
                    path_stream,
                    frame.clone(),
                    carrier_lane,
                    emit_mode,
                    "control",
                    None,
                )?
            }
            ReliableWorkClass::Data => match emit_response_frame_from_sender_service(
                path_stream,
                frame.clone(),
                reliable_path_effective_frame_lane(&frame, relay_lane),
                CarrierEmitMode::Classified,
                "data",
                None,
            ) {
                Ok(selected_path) => selected_path,
                Err(err) => {
                    let _ = send_stream.rollback_committed_data(&frame);
                    return Err(err);
                }
            },
            ReliableWorkClass::Reinjection => emit_response_frame_from_sender_service(
                path_stream,
                frame.clone(),
                relay_lane,
                CarrierEmitMode::Classified,
                "tail_reinjection",
                reinjection_cause,
            )?,
        };
        let (_, committed) = self
            .queue
            .commit_front()
            .expect("dispatched queued work must still be at queue front");
        self.finish_dispatched_work(
            path_stream,
            relay_lane,
            queued_lane,
            committed,
            frame,
            selected_path,
            dispatch_lane_name,
            enqueue_id,
            queue_delay_ms,
            None,
        )
        .map(ServerQueuedDispatch::Dispatched)
    }

    #[allow(clippy::too_many_arguments)]
    fn finish_dispatched_work(
        &mut self,
        path_stream: &ReliablePathStream,
        relay_lane: TrafficClass,
        queued_lane: ReliableWorkClass,
        committed: ReliableRelayQueuedWork,
        frame: Frame,
        selected_path: Option<CarrierPathKey>,
        dispatch_lane_name: &'static str,
        enqueue_id: u64,
        queue_delay_ms: u128,
        tcp_carrier_stable: Option<TcpCarrierStableGenerations>,
    ) -> Result<ServerResponseDispatch, RuntimeError> {
        if matches!(
            &committed.kind,
            ReliableRelayQueuedWorkKind::Reinjection {
                cause: RelaySendCause::StaleResponsePathReinjection(_),
                ..
            }
        ) {
            self.stale_response_recovery_generation =
                self.stale_response_recovery_generation.wrapping_add(1);
        }
        #[cfg(feature = "lab-diagnostics")]
        let send_lane = match queued_lane {
            ReliableWorkClass::Control => TrafficClass::Control,
            ReliableWorkClass::Reinjection => relay_lane,
            ReliableWorkClass::Data => reliable_path_effective_frame_lane(
                &frame,
                response_data_dispatch_lane(committed.data_lane, relay_lane),
            ),
        };
        #[cfg(feature = "lab-diagnostics")]
        let pacing_bytes = reliable_path_frame_pacing_bytes(&frame);
        #[cfg(feature = "lab-diagnostics")]
        let stream_extent = match &frame {
            Frame::StreamData {
                offset, payload, ..
            } => Some((*offset, payload.len())),
            _ => None,
        };
        #[cfg(feature = "lab-diagnostics")]
        if let Some((offset, payload_bytes)) = stream_extent {
            if queued_lane == ReliableWorkClass::Data {
                lab_server_response_stream_data(
                    self.session_id.0,
                    self.stream_id.0,
                    offset,
                    payload_bytes,
                );
            }
            if selected_path.is_none() {
                lab_sender_service_decision(
                    "server",
                    Some(self.session_id.0),
                    self.stream_id.0,
                    dispatch_lane_name,
                    "stream_data",
                    payload_bytes,
                    None,
                    format_args!(
                        "path_underlay={:?} path_id=none lane={:?} pacing_bytes={} degenerate_single_path=true",
                        path_stream.underlay, send_lane, pacing_bytes,
                    ),
                );
            } else if let Some(selected_path) = selected_path
                && queued_lane == ReliableWorkClass::Data
                && matches!(&path_stream.output, ReliablePathStreamOutput::Fixed(_))
            {
                lab_sender_service_decision(
                    "server",
                    Some(self.session_id.0),
                    self.stream_id.0,
                    dispatch_lane_name,
                    "stream_data",
                    payload_bytes,
                    None,
                    format_args!(
                        "path_underlay={:?} path_id={} lane={:?} pacing_bytes={} fixed_output=true",
                        selected_path.underlay, selected_path.path_id.0, send_lane, pacing_bytes,
                    ),
                );
            }
            if lab_diagnostic_event_enabled("server_sender_dispatch") {
                let (selected_underlay, selected_path_id) = selected_path
                    .map(|path| (format!("{:?}", path.underlay), path.path_id.0.to_string()))
                    .unwrap_or_else(|| ("none".to_string(), "none".to_string()));
                lab_diagnostic(
                    "server_sender_dispatch",
                    format_args!(
                        "session_id={} stream_id={} enqueue_id={} offset={} payload_bytes={} lane={:?} work_lane={:?} queue_delay_ms={} sender_queue_bytes_after={} selected_path_underlay={} selected_path_id={} pacing_bytes={}",
                        self.session_id.0,
                        self.stream_id.0,
                        enqueue_id,
                        offset,
                        payload_bytes,
                        send_lane,
                        queued_lane,
                        queue_delay_ms,
                        self.queue.bytes(),
                        selected_underlay,
                        selected_path_id,
                        pacing_bytes,
                    ),
                );
            }
        }
        #[cfg(not(feature = "lab-diagnostics"))]
        let _ = (
            path_stream,
            relay_lane,
            &frame,
            dispatch_lane_name,
            enqueue_id,
            queue_delay_ms,
        );
        Ok(ServerResponseDispatch {
            payload_bytes: committed.payload_bytes,
            lane: queued_lane,
            selected_path,
            tcp_carrier_stable,
        })
    }
}

#[cfg(test)]
#[path = "service_test.rs"]
mod tests;
