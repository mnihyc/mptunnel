//! Server response sender queue and orchestration facade.
//!
//! The service owns queued product work. Planning and carrier dispatch remain
//! separate so queue mutation cannot silently become path-selection policy.

use super::dispatch::{
    emit_planned_response_data_frame, emit_response_frame_from_sender_service,
    response_frame_has_carrier_credit, response_repair_carrier_lane,
};
use super::multipath::{
    plan_response_data_payload_with_ordered_debt_impl,
    preview_response_data_payload_with_ordered_debt,
};
use super::planner::{choose_response_sender_target, response_dispatch_payload_bytes};
#[cfg(test)]
use super::*;
use crate::config::MppPerformanceConfig;
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::{
    lab_diagnostic, lab_diagnostic_event_enabled, lab_perf_record, lab_sender_service_decision,
    lab_server_response_stream_data,
};
use crate::model::multipath::{ExtraTrafficKind, ExtraTrafficLedger};
use crate::model::path::CarrierPathKey;
use crate::model::work::{CarrierWorkKind, ReliableWorkClass};
use crate::mux::MuxLimits;
use crate::mux::stream::ReliableSendStream;
#[cfg(feature = "lab-diagnostics")]
use crate::protocol::frame::reliable_path_frame_pacing_bytes;
use crate::protocol::frame::reliable_stream_frame_accounted_bytes;
use crate::protocol::{Frame, OffsetRange, SessionId, StreamFlags, StreamId};
use crate::runtime::RuntimeError;
use crate::runtime::path::commands::reliable_path_effective_frame_lane;
use crate::runtime::sender::{
    CarrierEmitMode, RelaySendCause, ReliableRelayQueuedWork, ReliableRelayQueuedWorkKind,
    ReliableRelaySenderQueue, ServerRepairOutputIdentity, reliable_relay_can_read_product_source,
    reliable_relay_sender_queue_read_budget, sender_extra_traffic_startup_floor_bytes,
    sender_repair_minimum_useful_attempt_bytes,
};
use crate::runtime::stream::{ReliablePathStream, ReliablePathStreamOutput};
use crate::scheduler::{FlowLane, PathSnapshot};
use bytes::Bytes;
use std::time::Instant;

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
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ServerResponseDispatch {
    pub(in crate::runtime) payload_bytes: usize,
    pub(in crate::runtime) lane: ReliableWorkClass,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(in crate::runtime) selected_path: Option<CarrierPathKey>,
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
        }
    }

    pub(in crate::runtime) fn ack_gap_repair_path_snapshot(
        &self,
        path_stream: &ReliablePathStream,
        send_stream: &ReliableSendStream,
        normalized_ranges: &[OffsetRange],
        preview_limit: usize,
    ) -> Option<(ServerRepairOutputIdentity, PathSnapshot)> {
        let preview = send_stream
            .retransmission_frames_for_normalized_ack_gaps(normalized_ranges, preview_limit.max(1))
            .into_iter()
            .next()?;
        let ReliablePathStreamOutput::Switchable(binding) = &path_stream.output else {
            return None;
        };
        let avoid_keys = binding.flight_keys_overlapping_frame(&preview);
        let lane = response_repair_carrier_lane(&preview);
        let targets =
            binding.sender_path_targets(lane, reliable_stream_frame_accounted_bytes(&preview));
        choose_response_sender_target(
            &targets,
            lane,
            &preview,
            CarrierEmitMode::Classified,
            binding.mux_limits(),
            &[],
            &avoid_keys,
            Some(RelaySendCause::PersistentAckGapRepair),
        )
        .map(|target| {
            (
                ServerRepairOutputIdentity {
                    key: target.observation.key,
                    incarnation: target.observation.incarnation,
                },
                target.observation.snapshot,
            )
        })
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

    pub(in crate::runtime) fn release_normalized_acked_repairs(
        &mut self,
        ranges: &[OffsetRange],
    ) -> usize {
        self.queue.release_normalized_acked_repairs(ranges)
    }

    pub(in crate::runtime) fn discard_unusable_live_owner_tail_repairs(
        &mut self,
        path_stream: &ReliablePathStream,
    ) -> usize {
        self.queue
            .discard_unusable_live_owner_tail_repairs(|frame| {
                path_stream.has_live_owner_tail_repair_output_for_frame(frame)
            })
    }

    pub(in crate::runtime) fn discard_stale_persistent_ack_gap_repairs(
        &mut self,
        path_stream: &ReliablePathStream,
    ) -> usize {
        self.queue
            .discard_stale_persistent_ack_gap_repairs(|cause| {
                cause.persistent_server_target().is_none_or(|target| {
                    path_stream.has_output_incarnation(target.key, target.incarnation)
                }) && cause.persistent_client_target().is_none()
            })
    }

    pub(in crate::runtime) fn persistent_ack_gap_repair_deadline(&self) -> Option<Instant> {
        self.queue.persistent_ack_gap_repair_deadline()
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

    pub(in crate::runtime) fn repair_extra_budget_remaining(&self, mux_limits: MuxLimits) -> usize {
        self.extra_traffic_budget_remaining(mux_limits)
    }

    pub(in crate::runtime) fn repair_extra_event_budget_remaining(
        &self,
        mux_limits: MuxLimits,
    ) -> usize {
        let remaining = self.repair_extra_budget_remaining(mux_limits);
        if remaining < sender_repair_minimum_useful_attempt_bytes(mux_limits) {
            0
        } else {
            remaining
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn record_owner_progress_for_test(&mut self, bytes: usize) {
        self.record_owner_progress(bytes);
    }

    pub(in crate::runtime) fn record_owner_progress(&mut self, bytes: usize) {
        self.extra_traffic.record_owner_progress(bytes);
    }

    pub(in crate::runtime) fn publish_queue_bytes(&self, path_stream: &ReliablePathStream) {
        path_stream.set_sender_queue_bytes(self.queue.bytes());
    }

    pub(in crate::runtime) fn queued_send_ready(&self) -> bool {
        self.queue.front().is_some()
    }

    pub(in crate::runtime) fn front_is_data(&self) -> bool {
        self.queue
            .front()
            .is_some_and(|(_, work)| matches!(&work.kind, ReliableRelayQueuedWorkKind::Data(_)))
    }

    pub(in crate::runtime) fn drain_allows_bounded_source_staging(
        &self,
        path_stream: &ReliablePathStream,
        queued_send_blocked: bool,
    ) -> bool {
        queued_send_blocked
            && self.front_is_data()
            && path_stream.response_service_handoff_drain_active()
    }

    pub(in crate::runtime) fn front_has_carrier_credit_with_ordered_owner_debt(
        &self,
        path_stream: &ReliablePathStream,
        send_stream: &ReliableSendStream,
        relay_lane: FlowLane,
        mux_limits: MuxLimits,
        ordered_owner_debt_bytes: usize,
    ) -> bool {
        let Some((_, queued)) = self.queue.front() else {
            return false;
        };
        match &queued.kind {
            ReliableRelayQueuedWorkKind::Control(frame) => {
                let (carrier_lane, emit_mode) = if queued.stream_ordered_carrier_emit {
                    (relay_lane, CarrierEmitMode::StreamOrdered)
                } else {
                    (FlowLane::Control, CarrierEmitMode::Classified)
                };
                response_frame_has_carrier_credit(path_stream, frame, carrier_lane, emit_mode, None)
            }
            ReliableRelayQueuedWorkKind::Data(payload) => response_dispatch_payload_bytes(
                path_stream,
                send_stream,
                queued.data_lane.unwrap_or(relay_lane),
                mux_limits,
                payload.len(),
            )
            .is_some_and(|payload_bytes| {
                preview_response_data_payload_with_ordered_debt(
                    path_stream,
                    queued.data_lane.unwrap_or(relay_lane),
                    send_stream.next_offset(),
                    payload_bytes,
                    ordered_owner_debt_bytes,
                )
            }),
            ReliableRelayQueuedWorkKind::Repair { frame, cause } => {
                response_frame_has_carrier_credit(
                    path_stream,
                    frame,
                    response_repair_carrier_lane(frame),
                    CarrierEmitMode::Classified,
                    Some(*cause),
                )
            }
        }
    }

    pub(in crate::runtime) fn can_read_product_source(
        &self,
        local_open: bool,
        queued_send_blocked: bool,
        send_stream: &ReliableSendStream,
        mux_limits: MuxLimits,
        queue_limit: usize,
    ) -> bool {
        reliable_relay_can_read_product_source(
            local_open,
            queued_send_blocked,
            send_stream,
            &self.queue,
            mux_limits,
            queue_limit,
        )
    }

    pub(in crate::runtime) fn read_budget(
        &self,
        send_stream: &ReliableSendStream,
        mux_limits: MuxLimits,
        queue_limit: usize,
        buffer_len: usize,
    ) -> usize {
        reliable_relay_sender_queue_read_budget(
            send_stream,
            &self.queue,
            mux_limits,
            queue_limit,
            buffer_len,
        )
    }

    pub(in crate::runtime) fn enqueue_data_for_lane(
        &mut self,
        payload: Bytes,
        lane: FlowLane,
    ) -> u64 {
        self.queue.push_data_for_lane(payload, lane)
    }

    pub(in crate::runtime) fn enqueue_control_frame(&mut self, frame: Frame) -> u64 {
        self.queue.push_control(frame)
    }

    pub(in crate::runtime) fn enqueue_final_control_frame(&mut self, frame: Frame) -> u64 {
        self.queue.push_final_control(frame)
    }

    pub(in crate::runtime) fn enqueue_repair_frame_with_priority(
        &mut self,
        frame: Frame,
        mux_limits: MuxLimits,
        critical_priority: bool,
    ) -> Option<u64> {
        let payload_bytes = reliable_stream_frame_accounted_bytes(&frame);
        debug_assert!(CarrierWorkKind::RepairData.counts_against_sender_extra_budget());
        let budget = self.extra_traffic.budget(
            sender_extra_traffic_startup_floor_bytes(mux_limits),
            self.performance,
        );
        if !budget.can_spend(payload_bytes) {
            return None;
        }
        self.extra_traffic
            .record_optional(ExtraTrafficKind::Repair, payload_bytes);
        Some(if critical_priority {
            self.queue
                .push_critical_repair_with_cause(frame, RelaySendCause::AckGapRepair)
        } else {
            self.queue.push_repair(frame)
        })
    }

    #[cfg(test)]
    pub(in crate::runtime) fn enqueue_critical_repair_frame(&mut self, frame: Frame) -> u64 {
        self.enqueue_critical_repair_frame_with_cause(frame, RelaySendCause::AckGapRepair)
    }

    pub(in crate::runtime) fn enqueue_critical_tail_repair_frame(
        &mut self,
        frame: Frame,
    ) -> Option<u64> {
        if self.has_queued_repair_overlap(&frame) {
            return None;
        }
        Some(
            self.enqueue_critical_repair_frame_with_cause(frame, RelaySendCause::PathFailureRepair),
        )
    }

    pub(in crate::runtime) fn enqueue_critical_repair_frame_with_cause(
        &mut self,
        frame: Frame,
        cause: RelaySendCause,
    ) -> u64 {
        debug_assert!(cause.is_repair());
        let payload_bytes = reliable_stream_frame_accounted_bytes(&frame);
        debug_assert!(CarrierWorkKind::RepairData.counts_against_sender_extra_budget());
        self.extra_traffic
            .record_optional(ExtraTrafficKind::Repair, payload_bytes);
        self.queue.push_critical_repair_with_cause(frame, cause)
    }

    pub(in crate::runtime) fn has_queued_repair_overlap(&self, frame: &Frame) -> bool {
        self.queue.has_queued_repair_overlap(frame)
    }

    pub(in crate::runtime) fn dispatch_next(
        &mut self,
        path_stream: &ReliablePathStream,
        send_stream: &mut ReliableSendStream,
        relay_lane: FlowLane,
        mux_limits: MuxLimits,
    ) -> Result<ServerResponseDispatch, RuntimeError> {
        self.dispatch_next_with_ordered_owner_debt(
            path_stream,
            send_stream,
            relay_lane,
            mux_limits,
            0,
        )
    }

    pub(in crate::runtime) fn dispatch_next_with_ordered_owner_debt(
        &mut self,
        path_stream: &ReliablePathStream,
        send_stream: &mut ReliableSendStream,
        relay_lane: FlowLane,
        mux_limits: MuxLimits,
        ordered_owner_debt_bytes: usize,
    ) -> Result<ServerResponseDispatch, RuntimeError> {
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
        let (frame, dispatch_lane_name, repair_cause) = match &queued.kind {
            ReliableRelayQueuedWorkKind::Control(frame) => (frame.clone(), "control", None),
            ReliableRelayQueuedWorkKind::Data(payload) => {
                let data_lane = queued.data_lane.unwrap_or(relay_lane);
                let dispatch_payload_bytes = response_dispatch_payload_bytes(
                    path_stream,
                    send_stream,
                    data_lane,
                    mux_limits,
                    payload.len(),
                )
                .ok_or(RuntimeError::SenderServiceBlocked)?;
                let (dispatch_payload_bytes, planned) =
                    plan_response_data_payload_with_ordered_debt_impl(
                        path_stream,
                        data_lane,
                        send_stream.next_offset(),
                        dispatch_payload_bytes,
                        ordered_owner_debt_bytes,
                    )?;
                let dispatch_payload = payload.slice(..dispatch_payload_bytes);
                #[cfg(feature = "lab-diagnostics")]
                let mux_started = Instant::now();
                let frame = send_stream.send_data(dispatch_payload, StreamFlags::NONE)?;
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
                    Ok(outcome) => {
                        let committed = self
                            .queue
                            .commit_front_data_prefix(dispatch_payload_bytes)
                            .expect("dispatched queued data must still be at queue front");
                        return self.finish_dispatched_work(
                            path_stream,
                            relay_lane,
                            queued_lane,
                            committed,
                            frame,
                            outcome.selected_path,
                            "data",
                            enqueue_id,
                            queue_delay_ms,
                        );
                    }
                    Err(err) => {
                        let _ = send_stream.rollback_committed_data(&frame);
                        return Err(err);
                    }
                }
            }
            ReliableRelayQueuedWorkKind::Repair { frame, cause } => {
                (frame.clone(), "repair", Some(*cause))
            }
        };
        let selected_path = match queued_lane {
            ReliableWorkClass::Control => {
                let (carrier_lane, emit_mode) = if queued.stream_ordered_carrier_emit {
                    (relay_lane, CarrierEmitMode::StreamOrdered)
                } else {
                    (FlowLane::Control, CarrierEmitMode::Classified)
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
            ReliableWorkClass::Repair => emit_response_frame_from_sender_service(
                path_stream,
                frame.clone(),
                response_repair_carrier_lane(&frame),
                CarrierEmitMode::Classified,
                "tail_repair",
                repair_cause,
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
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn finish_dispatched_work(
        &mut self,
        path_stream: &ReliablePathStream,
        relay_lane: FlowLane,
        queued_lane: ReliableWorkClass,
        committed: ReliableRelayQueuedWork,
        frame: Frame,
        selected_path: Option<CarrierPathKey>,
        dispatch_lane_name: &'static str,
        enqueue_id: u64,
        queue_delay_ms: u128,
    ) -> Result<ServerResponseDispatch, RuntimeError> {
        #[cfg(feature = "lab-diagnostics")]
        let send_lane = match queued_lane {
            ReliableWorkClass::Control => FlowLane::Control,
            ReliableWorkClass::Repair => response_repair_carrier_lane(&frame),
            ReliableWorkClass::Data => reliable_path_effective_frame_lane(
                &frame,
                committed.data_lane.unwrap_or(relay_lane),
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
        })
    }
}

#[cfg(test)]
#[path = "service_test.rs"]
mod tests;
