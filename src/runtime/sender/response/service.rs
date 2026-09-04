//! Server response sender queue and orchestration facade.
//!
//! The service owns queued product work. Planning and carrier dispatch remain
//! separate so queue mutation cannot silently become path-selection policy.

use super::dispatch::{
    ResponseReinjectionServiceModel, emit_planned_response_data_frame,
    emit_response_frame_from_sender_service, response_frame_has_carrier_credit,
    select_observed_switchable_response_target_for_extent, select_switchable_response_target,
    select_switchable_response_target_for_extent,
};
use super::multipath::plan_response_data_payload_with_data_ack_outstanding_impl;
use super::response_reinjection_avoid_outputs;
use super::scheduling::response_completion_snapshot;
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::{
    lab_diagnostic, lab_diagnostic_event_enabled, lab_perf_record, lab_sender_service_decision,
    lab_server_response_stream_data,
};
use crate::model::admission::ReliableDataAckFrontierState;
use crate::model::capacity::adaptive_reliable_relay_chunk_bytes_with_frame_limit;
use crate::model::multipath::{
    LiveOwnerFallbackEpoch, LiveOwnerFrontierFloorEpoch, OptionalReinjectionLedger,
};
use crate::model::path::CarrierPathKey;
use crate::model::timing::{
    ReliableDataAckGapTiming, reliable_data_ack_gap_timing_for_assignments,
};
use crate::model::work::{
    ReliableReinjectionTargetWork, ReliableWorkClass, flight_interval_bytes,
    reliable_reinjection_service_limit_bytes,
};
use crate::mux::MuxLimits;
use crate::mux::stream::ReliableSendStream;
use crate::performance::MppPerformanceConfig;
use crate::protocol::frame::reliable_stream_frame_accounted_bytes;
#[cfg(feature = "lab-diagnostics")]
use crate::protocol::frame::{reliable_path_frame_pacing_bytes, reliable_stream_frame_extent};
use crate::protocol::{Frame, OffsetRange, SessionId, StreamId};
use crate::runtime::RuntimeError;
use crate::runtime::path::commands::reliable_path_effective_frame_lane;
use crate::runtime::relay::io::{
    exact_contiguous_retransmission_frames, normalized_stream_ack_first_gap,
};
use crate::runtime::sender::{
    CarrierEmitMode, RelaySendCause, ReliableRelayQueuedWork, ReliableRelayQueuedWorkKind,
    ReliableRelaySenderQueue, ServerReinjectionOutputIdentity,
    reliable_relay_can_read_product_source, reliable_relay_sender_queue_read_budget,
    sender_optional_reinjection_startup_floor_bytes,
    sender_reinjection_minimum_useful_attempt_bytes,
};
use crate::runtime::stream::response::ResponseSenderPathTarget;
use crate::runtime::stream::{
    ReliablePathStream, ReliablePathStreamOutput, RequalificationAttempt,
};
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
        (Some(queued_lane), _) => queued_lane,
        (None, current_lane) => current_lane,
    }
}

#[cfg(feature = "lab-diagnostics")]
#[allow(clippy::too_many_arguments)]
fn lab_server_stale_output_recovery(
    session_id: SessionId,
    stream_id: StreamId,
    owner: ServerReinjectionOutputIdentity,
    lowest: Option<OffsetRange>,
    preview: Option<&Frame>,
    target: Option<ServerReinjectionOutputIdentity>,
    owner_age: Option<Duration>,
    retry_deadline: Option<Instant>,
    observed_at: Instant,
    queued_frames: usize,
    first_queued: Option<(u64, u64)>,
    blocked: bool,
    disposition: &'static str,
) {
    let (lowest_start, lowest_end) = lowest
        .map(|range| (range.start.to_string(), range.end.to_string()))
        .unwrap_or_else(|| ("none".to_string(), "none".to_string()));
    let (preview_start, preview_end) = preview
        .and_then(reliable_stream_frame_extent)
        .map(|(start, end, _)| (start.to_string(), end.to_string()))
        .unwrap_or_else(|| ("none".to_string(), "none".to_string()));
    let (target_underlay, target_path_id, target_incarnation) = target
        .map(|target| {
            (
                format!("{:?}", target.key.underlay),
                target.key.path_id.0.to_string(),
                target.incarnation.to_string(),
            )
        })
        .unwrap_or_else(|| ("none".to_string(), "none".to_string(), "none".to_string()));
    let retry_due = retry_deadline.is_some_and(|deadline| deadline <= observed_at);
    let retry_distance_us = retry_deadline
        .map(|deadline| {
            if deadline >= observed_at {
                deadline.duration_since(observed_at).as_micros().to_string()
            } else {
                format!("-{}", observed_at.duration_since(deadline).as_micros())
            }
        })
        .unwrap_or_else(|| "none".to_string());
    let (first_queued_start, first_queued_end) = first_queued
        .map(|(start, end)| (start.to_string(), end.to_string()))
        .unwrap_or_else(|| ("none".to_string(), "none".to_string()));
    lab_diagnostic(
        "server_stale_output_recovery",
        format_args!(
            "session_id={} stream_id={} owner_underlay={:?} owner_path_id={} owner_incarnation={} owner_age_us={} lowest_start={} lowest_end={} preview_start={} preview_end={} target_underlay={} target_path_id={} target_incarnation={} retry_due={} retry_distance_us={} queued_frames={} first_queued_start={} first_queued_end={} blocked={} disposition={}",
            session_id.0,
            stream_id.0,
            owner.key.underlay,
            owner.key.path_id.0,
            owner.incarnation,
            owner_age
                .map(|age| age.as_micros().to_string())
                .unwrap_or_else(|| "none".to_string()),
            lowest_start,
            lowest_end,
            preview_start,
            preview_end,
            target_underlay,
            target_path_id,
            target_incarnation,
            retry_due,
            retry_distance_us,
            queued_frames,
            first_queued_start,
            first_queued_end,
            blocked,
            disposition,
        ),
    );
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

/// Applies the requester's one-shot return-topology ceiling without granting
/// any carrier credit or changing ordinary scheduling. Fixed/singleton output
/// retains the exact prior sequence.
fn response_startup_dispatch_payload_bytes(
    path_stream: &ReliablePathStream,
    send_stream: &ReliableSendStream,
    proposed_bytes: usize,
) -> Option<usize> {
    match &path_stream.output {
        ReliablePathStreamOutput::Fixed(_) => Some(proposed_bytes),
        ReliablePathStreamOutput::Switchable(binding) => {
            binding.response_startup_fresh_data_limit(send_stream.next_offset(), proposed_bytes)
        }
    }
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
    pub(in crate::runtime::sender) optional_reinjection: OptionalReinjectionLedger,
    live_owner_frontier_floor: LiveOwnerFrontierFloorEpoch,
    completion_tail_owner_fallback: LiveOwnerFallbackEpoch<ServerReinjectionOutputIdentity>,
    stale_response_recovery_generation: u64,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ServerResponseDispatch {
    pub(in crate::runtime) payload_bytes: usize,
    pub(in crate::runtime) lane: ReliableWorkClass,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(in crate::runtime) selected_path: Option<CarrierPathKey>,
    pub(in crate::runtime) accepted_copy_deadline: Option<Instant>,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ServerAckGapReinjectionTarget {
    pub(in crate::runtime) identity: ServerReinjectionOutputIdentity,
    pub(in crate::runtime) snapshot: PathSnapshot,
    pub(in crate::runtime) completion: Duration,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ServerAckGapReinjectionObservation {
    pub(in crate::runtime) uniform_frontier_extent_bytes: usize,
    pub(in crate::runtime) owner_recovery_timing: ReliableDataAckGapTiming,
    #[cfg(test)]
    pub(in crate::runtime) owner_snapshot: Option<crate::scheduler::PathSnapshot>,
    pub(in crate::runtime) owner_completion: Option<Duration>,
    pub(in crate::runtime) target: Option<ServerAckGapReinjectionTarget>,
}

#[derive(Debug, Default)]
pub(in crate::runtime) struct StaleResponseRecoveryOutcome {
    pub(in crate::runtime) queued: bool,
    pub(in crate::runtime) retry_deadline: Option<Instant>,
    pub(in crate::runtime) blocked_for_carrier_capacity: bool,
}

impl ServerResponseSenderService {
    fn reinjection_service_model(
        &self,
        send_stream: &ReliableSendStream,
        exclude_front_work: bool,
        require_full_frame: bool,
    ) -> ResponseReinjectionServiceModel<'_> {
        ResponseReinjectionServiceModel {
            queue: &self.queue,
            exclude_front_work,
            reinjection_debt_bytes: send_stream.reinjection_bytes(),
            require_full_frame,
        }
    }

    pub(in crate::runtime) fn reinjection_service_limit_for_target(
        &self,
        path_stream: &ReliablePathStream,
        send_stream: &ReliableSendStream,
        identity: ServerReinjectionOutputIdentity,
        snapshot: PathSnapshot,
        exclude_front_work: bool,
        mux_limits: MuxLimits,
    ) -> usize {
        let accepted_reinjection_bytes = match &path_stream.output {
            ReliablePathStreamOutput::Switchable(binding) => {
                binding.accepted_reinjected_data_in_flight_bytes_at(identity)
            }
            ReliablePathStreamOutput::Fixed(fixed) => {
                if identity != fixed.reinjection_output_identity() {
                    return 0;
                }
                fixed.accepted_reinjected_data_in_flight_bytes_at(identity)
            }
        };
        reliable_reinjection_service_limit_bytes(
            ReliableReinjectionTargetWork::new(
                Some(snapshot),
                self.queue
                    .response_target_queued_reinjection_bytes(identity, exclude_front_work),
                accepted_reinjection_bytes,
            ),
            send_stream.reinjection_bytes(),
            mux_limits,
        )
    }

    pub(in crate::runtime) fn try_send_requalification_probe(
        &mut self,
        path_stream: &ReliablePathStream,
        send_stream: &ReliableSendStream,
        lane: TrafficClass,
        mux_limits: MuxLimits,
    ) -> Result<RequalificationAttempt<ServerReinjectionOutputIdentity>, RuntimeError> {
        let budget = self.optional_reinjection.budget(
            sender_optional_reinjection_startup_floor_bytes(mux_limits),
            self.performance,
        );
        let minimum = sender_reinjection_minimum_useful_attempt_bytes(mux_limits);
        let byte_limit = minimum.min(budget.remaining_bytes().max(minimum));
        let attempt = path_stream.try_enqueue_response_requalification_probe(
            send_stream,
            lane,
            byte_limit,
        )?;
        if let Some(bytes) = attempt.published_payload_bytes() {
            self.optional_reinjection.record_reinjection(bytes);
        }
        Ok(attempt)
    }

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
            optional_reinjection: OptionalReinjectionLedger::default(),
            live_owner_frontier_floor: LiveOwnerFrontierFloorEpoch::default(),
            completion_tail_owner_fallback: LiveOwnerFallbackEpoch::default(),
            stale_response_recovery_generation: 0,
        }
    }

    pub(in crate::runtime) fn stale_response_recovery_generation(&self) -> u64 {
        self.stale_response_recovery_generation
    }

    pub(in crate::runtime) fn observe_completion_tail_owner_fallback(
        &mut self,
        range: OffsetRange,
        owners: &[ServerReinjectionOutputIdentity],
        timing: ReliableDataAckGapTiming,
    ) -> Instant {
        self.completion_tail_owner_fallback
            .observe(range, owners, timing)
    }

    pub(in crate::runtime) fn completion_tail_owner_fallback_deadline(&self) -> Option<Instant> {
        self.completion_tail_owner_fallback.deadline()
    }

    pub(in crate::runtime) fn ack_gap_reinjection_path_snapshot(
        &self,
        path_stream: &ReliablePathStream,
        send_stream: &ReliableSendStream,
        normalized_ranges: &[OffsetRange],
        preview_limit: usize,
    ) -> Option<ServerAckGapReinjectionObservation> {
        let (frontier, horizon) = normalized_stream_ack_first_gap(normalized_ranges)?;
        let ReliablePathStreamOutput::Switchable(binding) = &path_stream.output else {
            return None;
        };
        let uniform_frontier = binding.live_owner_uniform_frontier(OffsetRange {
            start: frontier,
            end: horizon,
        })?;
        if uniform_frontier.owners.len() != 1 {
            return None;
        }
        let uniform_frontier_extent_bytes =
            flight_interval_bytes(uniform_frontier.range.start, uniform_frontier.range.end);
        let scoring_payload_bytes = preview_limit.min(uniform_frontier_extent_bytes);
        if scoring_payload_bytes == 0 {
            return None;
        }
        let scoring_range = OffsetRange {
            start: frontier,
            end: frontier.saturating_add(scoring_payload_bytes as u64),
        };
        let scoring_frontier = binding.live_owner_uniform_frontier(scoring_range)?;
        if scoring_frontier.range != scoring_range
            || scoring_frontier.owners != uniform_frontier.owners
            || scoring_frontier.avoid != uniform_frontier.avoid
        {
            return None;
        }
        let scoring_frames = exact_contiguous_retransmission_frames(send_stream, scoring_range)?;
        let preview = scoring_frames
            .first()
            .expect("non-empty exact cache prefix");
        let lane = path_stream.current_lane();
        let targets = binding.sender_path_targets(lane, scoring_payload_bytes);
        let target = select_observed_switchable_response_target_for_extent(
            path_stream,
            lane,
            preview,
            CarrierEmitMode::Classified,
            &response_reinjection_avoid_outputs(
                binding,
                preview,
                RelaySendCause::PersistentAckGapReinjection,
            ),
            Some(RelaySendCause::PersistentAckGapReinjection),
            Some(self.reinjection_service_model(send_stream, false, false)),
            scoring_payload_bytes,
            &targets,
        );
        let exact_owner = uniform_frontier.owners[0];
        let owner_snapshot = targets
            .iter()
            .find(|candidate| {
                candidate.observation.key == exact_owner.key
                    && candidate.observation.incarnation == exact_owner.incarnation
            })
            .map(response_completion_snapshot);
        let owner_completion = owner_snapshot
            // The retained frontier is already included in this output's
            // exact OriginalData debt; it is not a second new payload.
            .and_then(|snapshot| scheduler::score_path(snapshot, lane, 0))
            .filter(|score| score.eta_ms.is_finite())
            .map(|score| Duration::from_secs_f64(score.eta_ms.max(0.0) / 1000.0));
        let owner_recovery_timing = reliable_data_ack_gap_timing_for_assignments(
            &scoring_frontier.owner_assignments,
            |owner| {
                (
                    owner.key.underlay,
                    (owner == exact_owner).then_some(owner_snapshot).flatten(),
                )
            },
        )?;
        let target = target.and_then(|target| {
            let identity = ServerReinjectionOutputIdentity {
                key: target.observation.key,
                incarnation: target.observation.incarnation,
            };
            if uniform_frontier.avoid.contains(&identity) {
                return None;
            }
            let snapshot = response_completion_snapshot(&target);
            let score = scheduler::score_path(snapshot, lane, scoring_payload_bytes)?;
            score
                .eta_ms
                .is_finite()
                .then_some(ServerAckGapReinjectionTarget {
                    identity,
                    snapshot,
                    completion: Duration::from_secs_f64(score.eta_ms.max(0.0) / 1000.0),
                })
        });
        Some(ServerAckGapReinjectionObservation {
            uniform_frontier_extent_bytes,
            owner_recovery_timing,
            #[cfg(test)]
            owner_snapshot,
            owner_completion,
            target,
        })
    }

    pub(in crate::runtime) fn reinjection_path_snapshot_for_frame(
        &self,
        path_stream: &ReliablePathStream,
        send_stream: &ReliableSendStream,
        preview: &Frame,
        cause: RelaySendCause,
    ) -> Option<(ServerReinjectionOutputIdentity, PathSnapshot)> {
        self.reinjection_path_target_for_frame(
            path_stream,
            preview,
            cause,
            Some(self.reinjection_service_model(send_stream, false, true)),
        )
        .map(|target| {
            let identity = ServerReinjectionOutputIdentity {
                key: target.observation.key,
                incarnation: target.observation.incarnation,
            };
            (identity, response_completion_snapshot(&target))
        })
    }

    /// Selects a positive-service witness for a common frontier preview.
    /// Apply must shrink the queued prefix to this exact witness's K/A limits;
    /// ordinary preflight above continues to require the whole frame.
    pub(in crate::runtime) fn reinjection_frontier_preview_target_for_extent(
        &self,
        path_stream: &ReliablePathStream,
        send_stream: &ReliableSendStream,
        preview: &Frame,
        cause: RelaySendCause,
        scoring_payload_bytes: usize,
    ) -> Option<(ServerReinjectionOutputIdentity, PathSnapshot)> {
        let ReliablePathStreamOutput::Switchable(binding) = &path_stream.output else {
            return None;
        };
        let avoid_outputs = response_reinjection_avoid_outputs(binding, preview, cause);
        select_switchable_response_target_for_extent(
            path_stream,
            path_stream.current_lane(),
            preview,
            CarrierEmitMode::Classified,
            &avoid_outputs,
            Some(cause),
            Some(self.reinjection_service_model(send_stream, false, false)),
            scoring_payload_bytes,
        )
        .map(|target| {
            let identity = ServerReinjectionOutputIdentity {
                key: target.observation.key,
                incarnation: target.observation.incarnation,
            };
            (identity, response_completion_snapshot(&target))
        })
    }

    fn reinjection_path_target_for_frame(
        &self,
        path_stream: &ReliablePathStream,
        preview: &Frame,
        cause: RelaySendCause,
        service_model: Option<ResponseReinjectionServiceModel<'_>>,
    ) -> Option<ResponseSenderPathTarget> {
        let ReliablePathStreamOutput::Switchable(binding) = &path_stream.output else {
            return None;
        };
        let avoid_outputs = response_reinjection_avoid_outputs(binding, preview, cause);
        let lane = path_stream.current_lane();
        select_switchable_response_target(
            path_stream,
            lane,
            preview,
            CarrierEmitMode::Classified,
            &avoid_outputs,
            Some(cause),
            service_model,
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

    pub(in crate::runtime) fn discard_stale_bound_reinjections(
        &mut self,
        path_stream: &ReliablePathStream,
    ) -> usize {
        self.queue.discard_stale_bound_reinjections(|cause| {
            cause.server_bound_target().is_none_or(|target| {
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

    pub(in crate::runtime) fn bound_reinjection_deadline(&self) -> Option<Instant> {
        self.queue.bound_reinjection_deadline()
    }

    pub(in crate::runtime) fn live_owner_frontier_floor_ready(&self, observed_at: Instant) -> bool {
        self.live_owner_frontier_floor.attempt_ready(observed_at)
    }

    pub(in crate::runtime) fn live_owner_frontier_floor_deadline(&self) -> Option<Instant> {
        self.live_owner_frontier_floor.next_attempt_at()
    }

    pub(in crate::runtime) fn record_live_owner_frontier_floor_attempt(
        &mut self,
        observed_at: Instant,
        recovery_interval: Duration,
    ) {
        self.live_owner_frontier_floor
            .record_accepted_attempt(observed_at, recovery_interval);
    }

    pub(in crate::runtime) fn record_live_owner_data_ack_frontier_progress(
        &mut self,
        observed_at: Instant,
    ) {
        self.live_owner_frontier_floor
            .record_data_ack_progress(observed_at);
    }

    pub(in crate::runtime) fn record_delivered_data(&mut self, bytes: usize) {
        self.optional_reinjection.record_delivered_data(bytes);
    }

    #[cfg(test)]
    pub(in crate::runtime) fn record_reinjection_for_test(&mut self, bytes: usize) {
        self.optional_reinjection.record_reinjection(bytes);
    }

    #[cfg(test)]
    pub(in crate::runtime) fn clear_queued_work_for_test(&mut self) {
        while self.queue.pop_front().is_some() {}
    }

    pub(in crate::runtime) fn publish_queue_bytes(&self, path_stream: &ReliablePathStream) {
        path_stream.set_sender_queue_bytes(self.queue.bytes());
    }

    pub(in crate::runtime) fn queued_send_ready(&self) -> bool {
        self.queue.front().is_some()
    }

    pub(in crate::runtime) fn front_has_carrier_credit_at_frontier(
        &mut self,
        path_stream: &ReliablePathStream,
        send_stream: &ReliableSendStream,
        relay_lane: TrafficClass,
        mux_limits: MuxLimits,
        data_ack_outstanding_bytes: usize,
        frontier_state: ReliableDataAckFrontierState,
    ) -> bool {
        let Some((_, queued)) = self.queue.front() else {
            return false;
        };
        match &queued.kind {
            ReliableRelayQueuedWorkKind::Control(frame) => {
                let (carrier_lane, emit_mode) = if queued.stream_ordered_carrier_emit {
                    (relay_lane, CarrierEmitMode::StreamOrdered)
                } else {
                    (TrafficClass::Control, CarrierEmitMode::Classified)
                };
                response_frame_has_carrier_credit(
                    path_stream,
                    frame,
                    carrier_lane,
                    emit_mode,
                    None,
                    None,
                )
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
                    return false;
                };
                let Some(payload_bytes) = response_startup_dispatch_payload_bytes(
                    path_stream,
                    send_stream,
                    payload_bytes,
                ) else {
                    return false;
                };
                plan_response_data_payload_with_data_ack_outstanding_impl(
                    path_stream,
                    data_lane,
                    send_stream.next_offset(),
                    payload_bytes,
                    data_ack_outstanding_bytes,
                    frontier_state,
                )
                .is_ok()
            }
            ReliableRelayQueuedWorkKind::Reinjection { frame, cause } => {
                response_frame_has_carrier_credit(
                    path_stream,
                    frame,
                    relay_lane,
                    CarrierEmitMode::Classified,
                    Some(*cause),
                    Some(self.reinjection_service_model(send_stream, true, true)),
                )
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
        critical_priority: bool,
    ) -> u64 {
        self.enqueue_reinjection_frame_with_cause_and_priority(
            frame,
            RelaySendCause::AckGapReinjection,
            critical_priority,
        )
    }

    pub(in crate::runtime) fn enqueue_reinjection_frame_with_cause_and_priority(
        &mut self,
        frame: Frame,
        cause: RelaySendCause,
        critical_priority: bool,
    ) -> u64 {
        debug_assert!(cause.is_reinjection());
        let payload_bytes = reliable_stream_frame_accounted_bytes(&frame);
        self.optional_reinjection.record_reinjection(payload_bytes);
        if critical_priority {
            self.queue
                .push_critical_reinjection_with_cause(frame, cause)
        } else {
            self.queue.push_reinjection_with_cause(frame, cause)
        }
    }

    pub(in crate::runtime) fn enqueue_critical_reinjection_frame_with_cause(
        &mut self,
        frame: Frame,
        cause: RelaySendCause,
    ) -> u64 {
        debug_assert!(cause.is_reinjection());
        let payload_bytes = reliable_stream_frame_accounted_bytes(&frame);
        self.optional_reinjection.record_reinjection(payload_bytes);
        self.queue
            .push_critical_reinjection_with_cause(frame, cause)
    }

    /// Reinjects exact OriginalData owned by a connection-level stale output.
    /// The native TCP/QUIC sender remains alive; retained product ranges,
    /// alternate carrier credit, queue bounds, and the owner's recovery clock
    /// constrain this work.
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(unused_variables))]
    pub(in crate::runtime) fn drive_stale_output_recovery(
        &mut self,
        path_stream: &ReliablePathStream,
        send_stream: &ReliableSendStream,
        mux_limits: MuxLimits,
    ) -> StaleResponseRecoveryOutcome {
        let mut outcome = StaleResponseRecoveryOutcome::default();
        for identity in path_stream.stale_response_original_outputs() {
            let recovery = path_stream.stale_original_recovery_state(identity);
            #[cfg(feature = "lab-diagnostics")]
            let observed_at = Instant::now();
            #[cfg(feature = "lab-diagnostics")]
            let lowest = recovery.uncovered_ranges.first().copied();
            #[cfg(feature = "lab-diagnostics")]
            let owner_age = lowest
                .and_then(|range| path_stream.data_ack_recovery_candidate(range.start))
                .filter(|candidate| {
                    candidate.key == identity.key
                        && candidate.output_incarnation == identity.incarnation
                })
                .map(|candidate| observed_at.saturating_duration_since(candidate.sent_at));
            outcome.retry_deadline = match (outcome.retry_deadline, recovery.retry_deadline) {
                (Some(current), Some(deadline)) => Some(current.min(deadline)),
                (None, deadline) => deadline,
                (current, None) => current,
            };
            if recovery.uncovered_ranges.is_empty() {
                #[cfg(feature = "lab-diagnostics")]
                lab_server_stale_output_recovery(
                    self.session_id,
                    self.stream_id,
                    identity,
                    lowest,
                    None,
                    None,
                    owner_age,
                    recovery.retry_deadline,
                    observed_at,
                    0,
                    None,
                    false,
                    "no_uncovered_ranges",
                );
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
                #[cfg(feature = "lab-diagnostics")]
                lab_server_stale_output_recovery(
                    self.session_id,
                    self.stream_id,
                    identity,
                    lowest,
                    None,
                    None,
                    owner_age,
                    recovery.retry_deadline,
                    observed_at,
                    0,
                    None,
                    false,
                    "overlap_suppressed",
                );
                continue;
            };
            let Some((reinjection_target, reinjection_path)) =
                self.reinjection_path_snapshot_for_frame(path_stream, send_stream, &preview, cause)
            else {
                outcome.blocked_for_carrier_capacity = true;
                #[cfg(feature = "lab-diagnostics")]
                lab_server_stale_output_recovery(
                    self.session_id,
                    self.stream_id,
                    identity,
                    lowest,
                    Some(&preview),
                    None,
                    owner_age,
                    recovery.retry_deadline,
                    observed_at,
                    0,
                    None,
                    true,
                    "no_target",
                );
                continue;
            };
            let reinjection_limit = self.reinjection_service_limit_for_target(
                path_stream,
                send_stream,
                reinjection_target,
                reinjection_path,
                false,
                mux_limits,
            );
            if reinjection_limit == 0 {
                outcome.blocked_for_carrier_capacity = true;
                #[cfg(feature = "lab-diagnostics")]
                lab_server_stale_output_recovery(
                    self.session_id,
                    self.stream_id,
                    identity,
                    lowest,
                    Some(&preview),
                    Some(reinjection_target),
                    owner_age,
                    recovery.retry_deadline,
                    observed_at,
                    0,
                    None,
                    true,
                    "target_service_exhausted",
                );
                continue;
            }
            #[cfg(feature = "lab-diagnostics")]
            let mut queued_frames = 0usize;
            #[cfg(feature = "lab-diagnostics")]
            let mut first_queued = None;
            for frame in send_stream
                .retransmission_frames_for_ranges(&recovery.uncovered_ranges, reinjection_limit)
            {
                if self.has_queued_reinjection_overlap(&frame) {
                    continue;
                }
                #[cfg(feature = "lab-diagnostics")]
                {
                    queued_frames = queued_frames.saturating_add(1);
                    if first_queued.is_none() {
                        first_queued = reliable_stream_frame_extent(&frame)
                            .map(|(start, end, _)| (start, end));
                    }
                }
                self.enqueue_critical_reinjection_frame_with_cause(frame, cause);
                outcome.queued = true;
            }
            #[cfg(feature = "lab-diagnostics")]
            lab_server_stale_output_recovery(
                self.session_id,
                self.stream_id,
                identity,
                lowest,
                Some(&preview),
                Some(reinjection_target),
                owner_age,
                recovery.retry_deadline,
                observed_at,
                queued_frames,
                first_queued,
                false,
                if queued_frames > 0 {
                    "queued"
                } else {
                    "no_frames"
                },
            );
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
        self.dispatch_next_at_frontier(
            path_stream,
            send_stream,
            relay_lane,
            mux_limits,
            data_ack_outstanding_bytes,
            ReliableDataAckFrontierState::Live,
        )
    }

    pub(in crate::runtime) fn dispatch_next_at_frontier(
        &mut self,
        path_stream: &ReliablePathStream,
        send_stream: &mut ReliableSendStream,
        relay_lane: TrafficClass,
        mux_limits: MuxLimits,
        data_ack_outstanding_bytes: usize,
        frontier_state: ReliableDataAckFrontierState,
    ) -> Result<ServerResponseDispatch, RuntimeError> {
        self.dispatch_next_attempt_with_data_ack_outstanding(
            path_stream,
            send_stream,
            relay_lane,
            mux_limits,
            data_ack_outstanding_bytes,
            frontier_state,
        )
    }

    fn dispatch_next_attempt_with_data_ack_outstanding(
        &mut self,
        path_stream: &ReliablePathStream,
        send_stream: &mut ReliableSendStream,
        relay_lane: TrafficClass,
        mux_limits: MuxLimits,
        data_ack_outstanding_bytes: usize,
        frontier_state: ReliableDataAckFrontierState,
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
                // Re-evaluate the non-refilling startup coordinate at apply;
                // the earlier readiness preview is advisory and may race FINAL.
                let dispatch_payload_bytes = response_startup_dispatch_payload_bytes(
                    path_stream,
                    send_stream,
                    dispatch_payload_bytes,
                )
                .ok_or(RuntimeError::SenderServiceBlocked)?;
                let (dispatch_payload_bytes, planned) =
                    plan_response_data_payload_with_data_ack_outstanding_impl(
                        path_stream,
                        data_lane,
                        send_stream.next_offset(),
                        dispatch_payload_bytes,
                        data_ack_outstanding_bytes,
                        frontier_state,
                    )?;
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
                        return self.finish_dispatched_work(
                            path_stream,
                            relay_lane,
                            queued_lane,
                            committed,
                            frame,
                            selected_path,
                            None,
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
            ReliableRelayQueuedWorkKind::Reinjection { frame, cause } => {
                (frame.clone(), "reinjection", Some(*cause))
            }
        };
        let emit_outcome = match queued_lane {
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
                None,
            ) {
                Ok(outcome) => outcome,
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
                Some(self.reinjection_service_model(send_stream, true, true)),
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
            emit_outcome.selected_path,
            emit_outcome.accepted_copy_deadline,
            dispatch_lane_name,
            enqueue_id,
            queue_delay_ms,
        )
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
        accepted_copy_deadline: Option<Instant>,
        dispatch_lane_name: &'static str,
        enqueue_id: u64,
        queue_delay_ms: u128,
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
        let traffic_class = match queued_lane {
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
                        path_stream.underlay, traffic_class, pacing_bytes,
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
                        selected_path.underlay,
                        selected_path.path_id.0,
                        traffic_class,
                        pacing_bytes,
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
                        traffic_class,
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
            accepted_copy_deadline,
        })
    }
}

#[cfg(test)]
#[path = "tests_service.rs"]
mod tests;
