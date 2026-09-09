//! Request-direction sender ownership.
//!
//! One serialized service owns request offsets, exact-path evidence, and the
//! observe/intent/apply scheduling cycle. TCP keeps its portable fallback;
//! QUIC path use is gated by validation and native writer backpressure.

use self::multipath::{
    RequestMultipathController, RequestMultipathPlan, RequestMultipathPlanError,
    RequestRelayNativeCapture, RequestRelayNativeInputs,
};
use super::queue::{ReliableRelayQueuedWorkKind, ReliableRelaySenderQueue};
use super::work::{
    CarrierEmitMode, ClientReinjectionOutputIdentity, RelaySendCause, RelaySendOutcome,
    sender_optional_reinjection_startup_floor_bytes,
    sender_reinjection_minimum_useful_attempt_bytes,
};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::{lab_diagnostic, lab_perf_record, lab_sender_service_decision};
use crate::model::admission::ReliableDataAckFrontierState;
use crate::model::capacity::{
    ReliableStreamSourceAdmission, adaptive_reliable_relay_reinjection_bytes,
    reliable_stream_advertised_window_bytes,
};
use crate::model::multipath::{
    LiveOwnerFallbackEpoch, LiveOwnerFrontierFloorEpoch, OptionalReinjectionLedger,
    include_live_owner_recovery_interval,
};
use crate::model::path::{RelayPathInstance, RelayPathKey};
use crate::model::requalification::StreamRequalificationProbe;
use crate::model::timing::{
    ReliableDataAckGapTiming, reliable_data_ack_gap_timing_for_assignments,
    reliable_data_retransmission_interval,
};
use crate::model::work::{
    ReliableReinjectionTargetWork, flight_interval_bytes,
    reliable_live_frontier_reinjection_limit_bytes, reliable_live_gap_reinjection_authority,
    reliable_reinjection_service_limit_bytes,
};
#[cfg(test)]
use crate::mux::MuxLimits;
use crate::mux::stream::{
    AckOutcome, ReliableRecvStream, ReliableSendStream, StreamError, ValidatedStreamAck,
};
use crate::performance::MppPerformanceConfig;
use crate::protocol::frame::{normalize_offset_ranges, reliable_stream_frame_accounted_bytes};
#[cfg(feature = "lab-diagnostics")]
use crate::protocol::frame::{reliable_path_frame_pacing_bytes, stream_ack_contiguous_frontier};
use crate::protocol::{Frame, OffsetRange, StreamId, UnderlayProtocol};
use crate::runtime::error::{RuntimeError, reliable_path_error_is_migratable};
use crate::runtime::path::commands::{
    ReliablePathCommandSender, ReliablePathFrameReservation, reliable_path_effective_frame_lane,
};
use crate::runtime::path::{ClientPathContext, RelayPathLoadLease};
use crate::runtime::relay::io::{
    AuthoritativeStreamAckSnapshot, exact_contiguous_retransmission_frames, first_proven_ack_gap,
    preserve_reinjection_frontier_quantum,
};
#[cfg(test)]
use crate::runtime::stream::ReliablePathStreamHandle;
use crate::runtime::stream::{
    ReliablePathStreamOutput, ReliableRecvProgress, ReliableRelayRemoteSet, RequalificationAttempt,
};
use crate::scheduler::{PathSnapshot, TrafficClass};
#[cfg(test)]
use bytes::Bytes;
use std::time::{Duration, Instant};

#[cfg(test)]
struct RequestAfterFrameReservationHook(Option<Box<dyn FnOnce() + Send>>);

#[cfg(test)]
impl std::fmt::Debug for RequestAfterFrameReservationHook {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("RequestAfterFrameReservationHook")
            .field(&self.0.is_some())
            .finish()
    }
}

#[cfg(test)]
impl RequestAfterFrameReservationHook {
    fn run(&mut self) {
        if let Some(hook) = self.0.take() {
            hook();
        }
    }
}

mod multipath;
mod owner;
mod prepared;
mod scheduling;
pub(in crate::runtime) use owner::{SharedRequestProduct, WeakSharedRequestProduct};
pub(in crate::runtime) use prepared::{RequestPreparedSource, claim_prepared_request_data};
#[cfg(test)]
mod tcp_capacity;
#[cfg(test)]
#[path = "request/tests_test_support.rs"]
pub(super) mod test_support;

// Ownership boundary:
// Sender services own product work before it reaches carrier command queues.
// Client relay sending and server response dispatch both use this module for
// queueing, reservation intents, and diagnostics. The request multipath owner
// serializes exact flight and product commits; final TCP/UDP emission still
// happens through carrier command senders.

// Request-sender diagnostics keep frame naming local to their event owner.
#[cfg(feature = "lab-diagnostics")]
fn sender_service_frame_kind(frame: &Frame) -> &'static str {
    match frame {
        Frame::StreamData { .. } => "stream_data",
        Frame::StreamAck { .. } => "stream_ack",
        Frame::StreamRequalifyData { .. } => "stream_requalify_data",
        Frame::StreamRequalifyAck { .. } => "stream_requalify_ack",
        Frame::StreamMaxData { .. } => "stream_max_data",
        Frame::StreamFin { .. } => "stream_fin",
        Frame::StreamReset { .. } => "stream_reset",
        Frame::StreamDetach { .. } => "stream_detach",
        Frame::DatagramData { .. } => "datagram_data",
        Frame::DatagramFeedback { .. } => "datagram_feedback",
        Frame::DatagramClose { .. } => "datagram_close",
        _ => "control",
    }
}

#[derive(Debug)]
pub(in crate::runtime) enum ClientQueuedDispatch {
    Reinjection {
        payload_bytes: usize,
        accepted_copy_deadline: Instant,
    },
    ReinjectionDeferred,
    PersistentReinjectionCancelled,
    PathAttachmentRequired(RuntimeError),
}

pub(in crate::runtime) struct RequestProductAckOutcome {
    pub(in crate::runtime) mux: AckOutcome,
    pub(in crate::runtime) data_ack_progress_paths: smallvec::SmallVec<[RelayPathInstance; 4]>,
    pub(in crate::runtime) idle_original_data_instances: smallvec::SmallVec<[RelayPathInstance; 4]>,
}

#[derive(Debug, Default)]
/// A single serialized Dispatch transaction, never retained across an ACK,
/// queued-copy dispatch, or qualification change. Collection subtracts current
/// accepted-copy deadlines; successful direct copies advance disjoint ranges.
pub(in crate::runtime) struct RequestPathRecoveryBatch {
    ranges: Vec<RequestPathRecoveryRange>,
    next_range: usize,
    rejected_targets: Vec<RelayPathInstance>,
    pub(in crate::runtime) retry_deadline: Option<Instant>,
    pub(in crate::runtime) blocked_for_carrier_capacity: bool,
}

impl RequestPathRecoveryBatch {
    pub(in crate::runtime) fn has_pending(&self) -> bool {
        self.next_range < self.ranges.len()
    }
}

#[derive(Debug, Clone)]
struct RequestPathRecoveryRange {
    owner: RelayPathInstance,
    range: OffsetRange,
    copy_owners: Vec<RelayPathInstance>,
    queued: bool,
}

#[derive(Clone, Copy)]
struct RequestReinjectionQueueContext<'a> {
    queue: &'a ReliableRelaySenderQueue,
    // Only queued publication consumes the queue's current front. A direct
    // structural copy must charge every independently queued live intent.
    exclude_front: bool,
}

/// Borrowed source ownership for one queued Original publication. No offset or
/// queue bytes are committed until the selected Native fence accepts Apply.
struct RequestQueuedSourceCommit<'a> {
    send_stream: &'a mut ReliableSendStream,
    sender_queue: &'a mut ReliableRelaySenderQueue,
}

impl RequestQueuedSourceCommit<'_> {
    fn validate(&self, frame: &Frame) -> Result<(), RequestFrameAdmissionError> {
        let Frame::StreamData { payload, .. } = frame else {
            return Err(RequestFrameAdmissionError::Source(
                StreamError::InvalidPreparedFrame,
            ));
        };
        let Some((_, queued)) = self.sender_queue.front() else {
            return Err(RequestFrameAdmissionError::SourceChanged);
        };
        let ReliableRelayQueuedWorkKind::Data(current) = &queued.kind else {
            return Err(RequestFrameAdmissionError::SourceChanged);
        };
        // Preparation slices this exact shared Bytes allocation. Comparing
        // its prefix identity is constant work and cannot accept a different
        // source item merely because it contains equal bytes. Checking the
        // global front also preserves critical-repair priority at commit.
        if payload.is_empty()
            || payload.len() > current.len()
            || payload.as_ptr() != current.as_ptr()
        {
            return Err(RequestFrameAdmissionError::SourceChanged);
        }
        Ok(())
    }
}

/// The exact admitted frame's Product commit inputs, borrowed only for the
/// synchronous fenced transaction. These are not a second mutable owner.
struct RequestFrameProductCommit<'a> {
    plan: &'a RequestMultipathPlan,
    frame: &'a Frame,
    cause: RelaySendCause,
    position: usize,
    path_count: usize,
    reinjection_target_snapshot: Option<PathSnapshot>,
    request_load_claim: Option<RelayPathLoadLease>,
}

#[derive(Debug, Clone, Copy, Default)]
pub(in crate::runtime) struct RequestCompletionTailEnqueueOutcome {
    pub(in crate::runtime) queued: bool,
    pub(in crate::runtime) blocked_for_carrier_capacity: bool,
    pub(in crate::runtime) waiting_for_path_model_publication: bool,
}

#[derive(Debug, Clone, Copy, Default)]
struct RequestTailReinjectionOutcome {
    queued: bool,
    completion_tail: Option<RequestCompletionTailEnqueueOutcome>,
}

/// Immutable evidence used to decide whether one Data ACK gap may be
/// reinjected on a different live path.
#[derive(Debug, Clone, Copy, Default)]
pub(in crate::runtime) struct RequestDataAckGapObservation {
    pub(in crate::runtime) has_live_original_path: bool,
    pub(in crate::runtime) original_assignment_at: Option<Instant>,
    pub(in crate::runtime) original_underlay: Option<UnderlayProtocol>,
    pub(in crate::runtime) original_path_timing: Option<PathSnapshot>,
    pub(in crate::runtime) reinjection_target:
        Option<(ClientReinjectionOutputIdentity, PathSnapshot)>,
    pub(in crate::runtime) reinjection_target_flight_bytes: usize,
    pub(in crate::runtime) reinjection_completion: Option<Duration>,
    pub(in crate::runtime) owner_completion: Option<Duration>,
    pub(in crate::runtime) target_service_exhausted: bool,
    pub(in crate::runtime) target_model_pending: bool,
    pub(in crate::runtime) uniform_frontier_extent_bytes: usize,
    pub(in crate::runtime) owner_recovery_timing: Option<ReliableDataAckGapTiming>,
}

/// Immutable target authority captured by one completion-tail Decide step.
///
/// Apply may shrink the ranked frontier against this exact snapshot and
/// service headroom, but must not resample a replacement target before it
/// records the shared live-owner recovery interval.
#[derive(Debug, Clone, Copy)]
struct RequestCompletionTailTarget {
    identity: ClientReinjectionOutputIdentity,
    snapshot: PathSnapshot,
    service_limit_bytes: usize,
    recovery_interval: Duration,
}

#[derive(Debug, Clone, Copy, Default)]
struct RequestCompletionTailTargetObservation {
    target: Option<RequestCompletionTailTarget>,
    target_service_exhausted: bool,
    waiting_for_path_model_publication: bool,
}

#[derive(Debug)]
pub(in crate::runtime) struct RequestSenderService {
    multipath: RequestMultipathController,
    performance: MppPerformanceConfig,
    optional_reinjection: OptionalReinjectionLedger,
    live_owner_frontier_floor: LiveOwnerFrontierFloorEpoch,
    completion_tail_owner_fallback: LiveOwnerFallbackEpoch<RelayPathInstance>,
    #[cfg(test)]
    after_frame_reservation: RequestAfterFrameReservationHook,
}

/// One owner for request source, claimed bytes and exact output membership.
///
/// The relay still serializes this aggregate; it is not shared with writers
/// yet. Merged receive I/O and asynchronous open/close execution stay outside.
/// Keep queue/service/cache teardown before dropping output membership, as in
/// the previous independently declared actor fields. No mirrored admission
/// state is needed when the native-claim transaction moves into this owner.
pub(in crate::runtime) struct RequestProductState {
    pub(in crate::runtime) sender_queue: ReliableRelaySenderQueue,
    pub(in crate::runtime) sender: RequestSenderService,
    pub(in crate::runtime) send_stream: ReliableSendStream,
    pub(in crate::runtime) last_send_ack: AuthoritativeStreamAckSnapshot,
    pub(in crate::runtime) remotes: ReliableRelayRemoteSet,
    pub(in crate::runtime) prepared: RequestPreparedSource,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct RelayRecvProgressSend {
    path: Option<PathSnapshot>,
    lane: TrafficClass,
    force_ack: bool,
    publish_max_data: bool,
    force_max_data: bool,
}

impl RelayRecvProgressSend {
    pub(in crate::runtime) fn new(
        path: Option<PathSnapshot>,
        lane: TrafficClass,
        force_max_data: bool,
    ) -> Self {
        Self {
            path,
            lane,
            force_ack: force_max_data,
            publish_max_data: true,
            force_max_data,
        }
    }

    pub(in crate::runtime) fn final_ack(path: Option<PathSnapshot>, lane: TrafficClass) -> Self {
        Self {
            path,
            lane,
            force_ack: true,
            publish_max_data: false,
            // Once the final receive offset is contiguous, new receive credit
            // has no consumer and must not precede the terminal Data ACK.
            force_max_data: false,
        }
    }

    pub(in crate::runtime) fn ack_only(path: Option<PathSnapshot>, lane: TrafficClass) -> Self {
        Self {
            path,
            lane,
            force_ack: true,
            publish_max_data: false,
            force_max_data: false,
        }
    }
}

impl RequestSenderService {
    #[cfg(test)]
    pub(in crate::runtime) fn new(stream_id: StreamId) -> Self {
        Self::new_with_performance(stream_id, MppPerformanceConfig::default())
    }

    pub(in crate::runtime) fn new_with_performance(
        stream_id: StreamId,
        performance: MppPerformanceConfig,
    ) -> Self {
        Self {
            multipath: RequestMultipathController::new(stream_id),
            performance,
            optional_reinjection: OptionalReinjectionLedger::default(),
            live_owner_frontier_floor: LiveOwnerFrontierFloorEpoch::default(),
            completion_tail_owner_fallback: LiveOwnerFallbackEpoch::default(),
            #[cfg(test)]
            after_frame_reservation: RequestAfterFrameReservationHook(None),
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn set_after_frame_reservation_for_test(
        &mut self,
        hook: impl FnOnce() + Send + 'static,
    ) {
        self.after_frame_reservation = RequestAfterFrameReservationHook(Some(Box::new(hook)));
    }

    pub(in crate::runtime) fn completion_tail_owner_fallback_deadline(&self) -> Option<Instant> {
        self.completion_tail_owner_fallback.deadline()
    }

    pub(in crate::runtime) fn fail_client_path_instance(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        instance: RelayPathInstance,
    ) -> bool {
        remotes.fail_path_instance(context, instance)
    }

    #[cfg(test)]
    fn optional_reinjection_budget_remaining(&self, mux_limits: MuxLimits) -> usize {
        self.optional_reinjection
            .budget(
                sender_optional_reinjection_startup_floor_bytes(mux_limits),
                self.performance,
            )
            .remaining_bytes()
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

    pub(in crate::runtime) fn enqueue_reinjection_frame_with_priority(
        &mut self,
        sender_queue: &mut ReliableRelaySenderQueue,
        frame: Frame,
        cause: RelaySendCause,
        critical_priority: bool,
    ) {
        debug_assert!(cause.is_reinjection());
        let payload_bytes = reliable_stream_frame_accounted_bytes(&frame);
        self.optional_reinjection.record_reinjection(payload_bytes);
        if critical_priority {
            sender_queue.push_critical_reinjection_with_cause(frame, cause);
        } else {
            sender_queue.push_reinjection_with_cause(frame, cause);
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn enqueue_critical_reinjection_frame(
        &mut self,
        sender_queue: &mut ReliableRelaySenderQueue,
        frame: Frame,
        cause: RelaySendCause,
    ) {
        self.enqueue_reinjection_frame_with_priority(sender_queue, frame, cause, true);
    }

    #[cfg(test)]
    fn record_delivered_data_for_test(&mut self, bytes: usize) {
        self.record_delivered_data(bytes);
    }

    pub(in crate::runtime) fn record_delivered_data(&mut self, bytes: usize) {
        self.optional_reinjection.record_delivered_data(bytes);
    }

    #[cfg(test)]
    pub(in crate::runtime) fn record_reinjection_for_test(&mut self, bytes: usize) {
        self.optional_reinjection.record_reinjection(bytes);
    }

    /// Advances the complete request product-ACK transaction once.
    ///
    /// Unique mux bytes, every transmitted flight copy, and exact OriginalData
    /// evidence remain separate accounting domains across this composition.
    pub(in crate::runtime) fn apply_request_product_ack(
        &mut self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        send_stream: &mut ReliableSendStream,
        ack: &ValidatedStreamAck,
    ) -> Result<RequestProductAckOutcome, StreamError> {
        #[cfg(feature = "lab-diagnostics")]
        let mux_started = Instant::now();
        let mux = send_stream.apply_validated_ack(ack)?;
        #[cfg(feature = "lab-diagnostics")]
        lab_perf_record("mux.apply_ack", mux_started.elapsed(), mux.released_bytes);
        if mux.released_bytes > 0 {
            self.record_delivered_data(mux.released_bytes);
        }
        let acked_at = Instant::now();
        let data_ack_release =
            self.multipath
                .apply_product_ack(context, remotes, ack.ranges(), acked_at);
        Ok(RequestProductAckOutcome {
            mux,
            data_ack_progress_paths: data_ack_release.data_ack_progress_paths,
            idle_original_data_instances: data_ack_release.idle_original_data_instances,
        })
    }

    pub(in crate::runtime) fn discard_unusable_tail_reinjections(
        &self,
        sender_queue: &mut ReliableRelaySenderQueue,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        lane: TrafficClass,
    ) -> usize {
        self.multipath
            .discard_unusable_tail_reinjections(sender_queue, context, remotes, lane)
    }

    pub(in crate::runtime) fn has_multipath_reinjection_alternative(
        &self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        lane: TrafficClass,
    ) -> bool {
        self.multipath
            .owner_capable_instances(context, remotes, lane)
            .len()
            > 1
    }

    pub(in crate::runtime) fn discard_stale_bound_reinjections(
        &self,
        sender_queue: &mut ReliableRelaySenderQueue,
        remotes: &ReliableRelayRemoteSet,
    ) -> usize {
        self.multipath
            .discard_stale_bound_reinjections(sender_queue, remotes)
    }

    pub(in crate::runtime) fn mark_request_path_stale(
        &mut self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        instance: RelayPathInstance,
        lane: TrafficClass,
    ) -> bool {
        if !self
            .multipath
            .has_reinjection_path(context, remotes, instance, lane)
        {
            return false;
        }
        self.multipath.mark_path_stale(instance)
    }

    #[cfg(test)]
    pub(in crate::runtime) fn request_path_is_stale(&self, instance: RelayPathInstance) -> bool {
        self.multipath.path_is_stale(instance)
    }

    pub(in crate::runtime) fn reliable_stream_source_admission(
        &self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        lane: TrafficClass,
        payload_bytes: usize,
    ) -> ReliableStreamSourceAdmission {
        self.multipath
            .reliable_stream_source_admission(context, remotes, lane, payload_bytes)
    }

    pub(in crate::runtime) fn requalification_deadline(&self) -> Option<Instant> {
        self.multipath.requalification_deadline()
    }

    pub(in crate::runtime) fn earliest_reinjection_suppression_deadline(
        &self,
        remotes: &ReliableRelayRemoteSet,
    ) -> Option<Instant> {
        self.multipath
            .earliest_reinjection_suppression_deadline(remotes)
    }

    pub(in crate::runtime) fn reinjection_suppression_deadline_for_frame(
        &self,
        frame: &Frame,
        remotes: &ReliableRelayRemoteSet,
    ) -> Option<Instant> {
        self.multipath
            .reinjection_suppression_deadline_for_frame(frame, remotes)
    }

    #[cfg(test)]
    pub(in crate::runtime) fn accepted_reinjected_data_bytes_for_test(
        &self,
        instance: RelayPathInstance,
    ) -> usize {
        self.multipath.accepted_reinjected_data_bytes(instance)
    }

    #[cfg(test)]
    pub(in crate::runtime) fn request_reinjection_target_snapshot_for_test(
        &self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        instance: RelayPathInstance,
    ) -> Option<PathSnapshot> {
        let path = remotes
            .paths
            .iter()
            .find(|path| path.instance() == instance)?;
        self.multipath
            .request_reinjection_target_snapshot(context, remotes, path)
    }

    pub(in crate::runtime) fn try_send_requalification_probe(
        &mut self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        send_stream: &ReliableSendStream,
        lane: TrafficClass,
    ) -> Result<RequalificationAttempt<RelayPathInstance>, RuntimeError> {
        let budget = self.optional_reinjection.budget(
            sender_optional_reinjection_startup_floor_bytes(context.mux_limits),
            self.performance,
        );
        let minimum = sender_reinjection_minimum_useful_attempt_bytes(context.mux_limits);
        // Requalification normally consumes optional credit. Once exhausted,
        // one minimum quantum per exact stale interval remains critical
        // liveness authority and is still charged as debt.
        let byte_limit = minimum.min(budget.remaining_bytes().max(minimum));
        let attempt = self.multipath.try_enqueue_requalification_probe(
            context,
            remotes,
            send_stream,
            lane,
            byte_limit,
        )?;
        if let Some(bytes) = attempt.published_payload_bytes() {
            self.optional_reinjection.record_reinjection(bytes);
        }
        Ok(attempt)
    }

    pub(in crate::runtime) fn acknowledge_requalification_probe(
        &mut self,
        instance: RelayPathInstance,
        probe_id: u64,
        offset: u64,
        payload_bytes: u32,
    ) -> bool {
        self.multipath.acknowledge_requalification_probe(
            instance,
            StreamRequalificationProbe {
                id: probe_id,
                offset,
                payload_bytes,
            },
        )
    }

    pub(in crate::runtime) fn unacked_original_paths_for_gaps(
        &self,
        remotes: &ReliableRelayRemoteSet,
        gaps: &[OffsetRange],
    ) -> smallvec::SmallVec<[RelayPathInstance; 4]> {
        self.multipath
            .unacked_original_paths_for_gaps(remotes, gaps)
    }

    pub(in crate::runtime) fn request_path_has_reinjection_path(
        &self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        candidate: RelayPathInstance,
        lane: TrafficClass,
    ) -> bool {
        self.multipath
            .has_reinjection_path(context, remotes, candidate, lane)
    }

    pub(in crate::runtime) fn release_all(&mut self, context: &ClientPathContext) {
        self.multipath.release_all(context);
    }

    #[cfg(test)]
    pub(in crate::runtime) fn record_original_frame_for_test(
        &mut self,
        instance: RelayPathInstance,
        frame: &Frame,
    ) {
        self.multipath
            .record_original_frame_for_test(instance, frame);
    }

    #[cfg(test)]
    pub(in crate::runtime) fn record_reinjected_frame_for_test(
        &mut self,
        instance: RelayPathInstance,
        frame: &Frame,
    ) {
        self.multipath
            .record_reinjected_frame_for_test(instance, frame);
    }

    pub(in crate::runtime) fn send_control_frame(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        frame: Frame,
        cause: RelaySendCause,
    ) -> Result<RelaySendOutcome, RuntimeError> {
        debug_assert!(!cause.is_reinjection());
        self.send_frame(context, remotes, frame, cause, None)
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::runtime) fn data_ack_gap_reinjection_model(
        &self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        send_stream: &ReliableSendStream,
        sender_queue: &ReliableRelaySenderQueue,
        normalized_ranges: &[OffsetRange],
        preview_limit: usize,
        lane: TrafficClass,
    ) -> RequestDataAckGapObservation {
        let Some((frontier, horizon)) = first_proven_ack_gap(normalized_ranges) else {
            return RequestDataAckGapObservation::default();
        };
        let live_instances = self
            .multipath
            .owner_capable_instances(context, remotes, lane);
        let Some(uniform_frontier) = self.multipath.live_owner_uniform_frontier(
            OffsetRange {
                start: frontier,
                end: horizon,
            },
            &live_instances,
        ) else {
            return RequestDataAckGapObservation::default();
        };
        if uniform_frontier.owners.len() != 1 {
            return RequestDataAckGapObservation::default();
        }
        let uniform_frontier_extent_bytes =
            flight_interval_bytes(uniform_frontier.range.start, uniform_frontier.range.end);
        let scoring_payload_bytes = preview_limit.min(uniform_frontier_extent_bytes);
        if scoring_payload_bytes == 0 {
            return RequestDataAckGapObservation::default();
        }
        let scoring_range = OffsetRange {
            start: frontier,
            end: frontier.saturating_add(scoring_payload_bytes as u64),
        };
        let Some(scoring_frontier) = self
            .multipath
            .live_owner_uniform_frontier(scoring_range, &live_instances)
        else {
            return RequestDataAckGapObservation::default();
        };
        if scoring_frontier.range != scoring_range
            || scoring_frontier.owners != uniform_frontier.owners
            || scoring_frontier.avoid != uniform_frontier.avoid
        {
            return RequestDataAckGapObservation::default();
        }
        let Some(scoring_frames) =
            exact_contiguous_retransmission_frames(send_stream, scoring_range)
        else {
            return RequestDataAckGapObservation::default();
        };
        let preview = scoring_frames
            .first()
            .expect("non-empty exact cache prefix");
        let mut model = self
            .multipath
            .data_ack_gap_reinjection_service_model_for_extent(
                context,
                remotes,
                preview,
                lane,
                sender_queue,
                send_stream.reinjection_bytes(),
                context.mux_limits,
                scoring_payload_bytes,
            );
        let exact_owner = uniform_frontier.owners[0];
        let owner_snapshot = model.original_path_timing;
        let owner_recovery_timing = reliable_data_ack_gap_timing_for_assignments(
            &scoring_frontier.owner_assignments,
            |instance| {
                (
                    instance.key.underlay,
                    (instance == exact_owner)
                        .then_some(owner_snapshot)
                        .flatten(),
                )
            },
        );
        if model
            .reinjection_target
            .is_some_and(|(target, _)| uniform_frontier.avoid.contains(&target.instance))
        {
            return RequestDataAckGapObservation::default();
        }
        model.original_assignment_at = scoring_frontier
            .owner_assignments
            .iter()
            .map(|(_, sent_at)| *sent_at)
            .max();
        model.uniform_frontier_extent_bytes = uniform_frontier_extent_bytes;
        model.owner_recovery_timing = owner_recovery_timing;
        model
    }

    /// Prepared Original source belongs to native claimants. The Product
    /// actor still publishes the same exact repair work, without removing or
    /// assigning any bytes from the prepared Data lane.
    pub(in crate::runtime) fn dispatch_client_repair_work(
        &mut self,
        context: &ClientPathContext,
        request_lane: TrafficClass,
        remotes: &mut ReliableRelayRemoteSet,
        sender_queue: &mut ReliableRelaySenderQueue,
    ) -> Result<Option<ClientQueuedDispatch>, RuntimeError> {
        let Some(queued) = sender_queue.front_reinjection() else {
            return Ok(None);
        };
        let ReliableRelayQueuedWorkKind::Reinjection { frame, cause } = queued.kind.clone() else {
            return Err(RuntimeError::Protocol(
                "request repair queue contains non-repair work",
            ));
        };
        self.dispatch_client_reinjection_work(
            context,
            request_lane,
            remotes,
            sender_queue,
            frame,
            cause,
        )
        .map(Some)
    }

    #[allow(clippy::too_many_arguments)]
    fn dispatch_client_reinjection_work(
        &mut self,
        context: &ClientPathContext,
        request_lane: TrafficClass,
        remotes: &mut ReliableRelayRemoteSet,
        sender_queue: &mut ReliableRelaySenderQueue,
        frame: Frame,
        cause: RelaySendCause,
    ) -> Result<ClientQueuedDispatch, RuntimeError> {
        let dispatch = self.send_frame_at_frontier(
            context,
            remotes,
            frame,
            cause,
            matches!(cause, RelaySendCause::CompletionTailReinjection(_)).then_some(request_lane),
            ReliableDataAckFrontierState::Live,
            Some(RequestReinjectionQueueContext {
                queue: sender_queue,
                exclude_front: true,
            }),
        );
        match dispatch {
            Ok(outcome) => {
                let committed = sender_queue
                    .commit_front_reinjection()
                    .expect("sent queued reinjection must still be at queue front");
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "reinjection",
                    format_args!(
                        "stream_id={} path_underlay={:?} path_index={} cause={} queued_dispatch=true payload_bytes={}",
                        self.multipath.stream_id().0,
                        outcome.path_key.underlay,
                        outcome.path_key.index,
                        cause.as_str(),
                        committed.payload_bytes,
                    ),
                );
                #[cfg(not(feature = "lab-diagnostics"))]
                let _ = outcome;
                Ok(ClientQueuedDispatch::Reinjection {
                    payload_bytes: committed.payload_bytes,
                    accepted_copy_deadline: outcome
                        .accepted_copy_deadline
                        .expect("reinjection commitment must publish its immutable deadline"),
                })
            }
            Err(RuntimeError::SenderServiceBlocked) => Err(RuntimeError::SenderServiceBlocked),
            Err(err)
                if matches!(cause, RelaySendCause::PersistentClientAckGapReinjection(_))
                    && reliable_path_error_is_migratable(&err) =>
            {
                let discarded = sender_queue.discard_persistent_ack_gap_reinjection_batch(cause);
                debug_assert!(discarded > 0);
                Ok(ClientQueuedDispatch::PersistentReinjectionCancelled)
            }
            Err(err)
                if matches!(
                    cause,
                    RelaySendCause::TailReinjection | RelaySendCause::CompletionTailReinjection(_)
                ) && reliable_path_error_is_migratable(&err) =>
            {
                let _ = sender_queue
                    .commit_front_reinjection()
                    .expect("deferred live-tail reinjection must still be at queue front");
                Ok(ClientQueuedDispatch::ReinjectionDeferred)
            }
            Err(err) if reliable_path_error_is_migratable(&err) => {
                Ok(ClientQueuedDispatch::PathAttachmentRequired(err))
            }
            Err(err) => Err(err),
        }
    }

    fn send_frame(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        frame: Frame,
        cause: RelaySendCause,
        request_lane: Option<TrafficClass>,
    ) -> Result<RelaySendOutcome, RuntimeError> {
        self.send_frame_at_frontier(
            context,
            remotes,
            frame,
            cause,
            request_lane,
            ReliableDataAckFrontierState::Live,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn send_frame_at_frontier(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        frame: Frame,
        cause: RelaySendCause,
        request_lane: Option<TrafficClass>,
        frontier_state: ReliableDataAckFrontierState,
        reinjection_queue: Option<RequestReinjectionQueueContext<'_>>,
    ) -> Result<RelaySendOutcome, RuntimeError> {
        let sent_frame = frame.clone();
        let avoid_instances =
            self.multipath
                .reinjection_avoid_instances(&sent_frame, cause, remotes);
        let (instance, payload_bytes, accepted_copy_deadline) = self.emit_relay_frame(
            context,
            remotes,
            frame,
            cause,
            &avoid_instances,
            request_lane,
            frontier_state,
            reinjection_queue,
        )?;
        let path_key = instance.key;
        self.record_decision(path_key, payload_bytes, &sent_frame, cause);
        Ok(RelaySendOutcome {
            path_key,
            accepted_copy_deadline,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn emit_relay_frame(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        frame: Frame,
        cause: RelaySendCause,
        avoid_instances: &[RelayPathInstance],
        request_lane: Option<TrafficClass>,
        frontier_state: ReliableDataAckFrontierState,
        reinjection_queue: Option<RequestReinjectionQueueContext<'_>>,
    ) -> Result<(RelayPathInstance, usize, Option<Instant>), RuntimeError> {
        let mut last_error = None;
        let mut rejected_bulk_original_targets =
            smallvec::SmallVec::<[RelayPathInstance; 8]>::new();
        while !remotes.paths.is_empty() {
            let stream_lane = remotes
                .paths
                .last()
                .map(|path| path.stream.lane)
                .unwrap_or(TrafficClass::Latency);
            let selection_lane = request_lane.unwrap_or(stream_lane);
            let mut decision_avoid_instances =
                smallvec::SmallVec::<[RelayPathInstance; 8]>::from_slice(avoid_instances);
            for rejected in &rejected_bulk_original_targets {
                if !decision_avoid_instances.contains(rejected) {
                    decision_avoid_instances.push(*rejected);
                }
            }
            let plan = match self.multipath.plan_relay_path_send_at_frontier(
                context,
                remotes,
                &frame,
                selection_lane,
                cause,
                &decision_avoid_instances,
                frontier_state,
            ) {
                Ok(plan) => plan,
                Err(
                    RequestMultipathPlanError::ServiceBlocked
                    | RequestMultipathPlanError::OrderedTerminalPending,
                ) => {
                    return Err(RuntimeError::SenderServiceBlocked);
                }
                Err(RequestMultipathPlanError::OutputUnavailable) => {
                    return Err(last_error.unwrap_or(RuntimeError::ReliablePathSessionClosed));
                }
            };
            let (_, instance) = plan.target();
            let Some(position) = plan.target_position_for_apply(remotes, selection_lane) else {
                if plan.reject_failed_bulk_original_target(
                    selection_lane,
                    &mut rejected_bulk_original_targets,
                ) {
                    continue;
                }
                return Err(RuntimeError::SenderServiceBlocked);
            };
            if plan.proof_expectation().is_some_and(|proof| {
                !context.relay_path_proof_epoch_is_current(instance.key, proof)
            }) {
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "request_path_proof",
                    format_args!(
                        "phase=apply_stale stream_id={} underlay={:?} path_index={} instance_id={}",
                        self.multipath.stream_id().0,
                        instance.key.underlay,
                        instance.key.index,
                        instance.attachment_id,
                    ),
                );
                if plan.reject_failed_bulk_original_target(
                    selection_lane,
                    &mut rejected_bulk_original_targets,
                ) {
                    continue;
                }
                return Err(RuntimeError::SenderServiceBlocked);
            }
            let (lane, emit_mode) = if matches!(cause, RelaySendCause::StreamFin) {
                (
                    remotes.paths[position].stream.lane,
                    CarrierEmitMode::StreamOrdered,
                )
            } else {
                (
                    reliable_path_effective_frame_lane(&frame, remotes.paths[position].stream.lane),
                    CarrierEmitMode::Classified,
                )
            };
            let request_load_claim =
                if let Some((key, active, latency_sensitive)) = plan.load_expectation() {
                    debug_assert_eq!(key, instance.key);
                    let Some(claim) = context.try_reserve_relay_path_load_if_unchanged(
                        instance,
                        selection_lane,
                        active,
                        latency_sensitive,
                    ) else {
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "request_startup_selection",
                            format_args!(
                                "phase=claim_stale stream_id={} path_index={} instance_id={}",
                                self.multipath.stream_id().0,
                                instance.key.index,
                                instance.attachment_id,
                            ),
                        );
                        if plan.reject_failed_bulk_original_target(
                            selection_lane,
                            &mut rejected_bulk_original_targets,
                        ) {
                            continue;
                        }
                        return Err(RuntimeError::SenderServiceBlocked);
                    };
                    Some(claim)
                } else {
                    None
                };
            let path_count = remotes.paths.len();
            // TCP has no named Native shape adapter, so its existing exact
            // target snapshot remains the immutable apply input. QUIC must
            // instead rebuild this snapshot from the current shape supplied
            // while its Native fence is held below.
            let tcp_reinjection_authority = (cause.is_reinjection()
                && instance.key.underlay == UnderlayProtocol::Tcp)
                .then(|| {
                    self.multipath.request_reinjection_target_snapshot(
                        context,
                        remotes,
                        &remotes.paths[position],
                    )
                });
            // The reservation borrows this local clone rather than the remote
            // entry, leaving the complete exact output set observable during
            // the post-reservation Product transaction.
            let commands =
                match fixed_request_output_commands(&remotes.paths[position].stream.output) {
                    Ok(commands) => commands.clone(),
                    Err(error) => {
                        if plan.reject_failed_bulk_original_target(
                            selection_lane,
                            &mut rejected_bulk_original_targets,
                        ) {
                            continue;
                        }
                        return Err(error);
                    }
                };
            let publish_result = {
                match reserve_request_frame_with_mode(
                    &commands,
                    frame.clone(),
                    lane,
                    emit_mode,
                    cause.is_reinjection(),
                ) {
                    Ok(command) => {
                        #[cfg(test)]
                        self.after_frame_reservation.run();
                        let bulk_original_apply =
                            plan.assigns_original_data() && selection_lane.is_bulk();
                        if !bulk_original_apply
                            && !plan.target_retains_exact_eligibility(context, selection_lane)
                        {
                            #[cfg(feature = "lab-diagnostics")]
                            lab_diagnostic(
                                "request_path_apply",
                                format_args!(
                                    "phase=exact_eligibility_stale stream_id={} underlay={:?} path_index={} instance_id={}",
                                    self.multipath.stream_id().0,
                                    instance.key.underlay,
                                    instance.key.index,
                                    instance.attachment_id,
                                ),
                            );
                            if plan.reject_failed_bulk_original_target(
                                selection_lane,
                                &mut rejected_bulk_original_targets,
                            ) {
                                continue;
                            }
                            return Err(RuntimeError::SenderServiceBlocked);
                        }
                        if !bulk_original_apply
                            && !self.multipath.plan_retains_exact_product_headroom(&plan)
                        {
                            #[cfg(feature = "lab-diagnostics")]
                            lab_diagnostic(
                                "request_product_admission",
                                format_args!(
                                    "phase=exact_product_window_exhausted stream_id={} underlay={:?} path_index={} instance_id={}",
                                    self.multipath.stream_id().0,
                                    instance.key.underlay,
                                    instance.key.index,
                                    instance.attachment_id,
                                ),
                            );
                            if plan.reject_failed_bulk_original_target(
                                selection_lane,
                                &mut rejected_bulk_original_targets,
                            ) {
                                continue;
                            }
                            return Err(RuntimeError::SenderServiceBlocked);
                        }
                        // TCP's Product calculation can observe attached QUIC
                        // authorities too. Resolve that advisory input before
                        // entering the Apply closure; it must never hide a
                        // Product -> Native read when ownership is shared.
                        let tcp_original_native_inputs = (bulk_original_apply
                            && instance.key.underlay == UnderlayProtocol::Tcp)
                            .then(|| {
                                RequestRelayNativeCapture::new(
                                    remotes.membership_generation(),
                                    &remotes.paths,
                                )
                                .resolve()
                            });
                        // The exact Native stamp now guards the complete first
                        // irreversible publication: Product flight, load
                        // ownership, Product receipt/cursor, and carrier queue.
                        // A stale stamp drops the still-local command and load
                        // claim, then replans this same path without treating
                        // the authority transition as a path failure.
                        plan.commit_with_current_native_shape(&commands, |native_shape| {
                            let reinjection_target_snapshot = if cause.is_reinjection() {
                                match instance.key.underlay {
                                    UnderlayProtocol::Udp => native_shape.and_then(|shape| {
                                        self.multipath
                                            .request_reinjection_target_snapshot_with_native_shape(
                                                context, remotes, instance, shape,
                                            )
                                    }),
                                    UnderlayProtocol::Tcp => {
                                        tcp_reinjection_authority.flatten()
                                    }
                                }
                            } else {
                                None
                            };
                            if cause.is_reinjection() {
                                let Some(snapshot) = reinjection_target_snapshot else {
                                    return Err(RequestFrameAdmissionError::ServiceBlocked);
                                };
                                let queued_reinjection = reinjection_queue.map_or(0, |queued| {
                                    queued.queue.request_target_queued_reinjection_bytes(
                                        instance,
                                        queued.exclude_front,
                                    )
                                });
                                let accepted_reinjection =
                                    self.multipath.accepted_reinjected_data_bytes(instance);
                                let payload_bytes = reliable_stream_frame_accounted_bytes(&frame);
                                let exact_service = reliable_reinjection_service_limit_bytes(
                                    ReliableReinjectionTargetWork::new(
                                        Some(snapshot),
                                        queued_reinjection,
                                        accepted_reinjection,
                                    ),
                                    payload_bytes,
                                    context.mux_limits,
                                );
                                if exact_service < payload_bytes {
                                    #[cfg(feature = "lab-diagnostics")]
                                    lab_diagnostic(
                                        "request_reinjection_admission",
                                        format_args!(
                                            "phase=exact_target_exhausted stream_id={} underlay={:?} path_index={} instance_id={} payload_bytes={} service_bytes={}",
                                            self.multipath.stream_id().0,
                                            instance.key.underlay,
                                            instance.key.index,
                                            instance.attachment_id,
                                            payload_bytes,
                                            exact_service,
                                        ),
                                    );
                                    return Err(RequestFrameAdmissionError::ServiceBlocked);
                                }
                            }
                            if bulk_original_apply {
                                let native_inputs = match native_shape {
                                    Some(shape) => RequestRelayNativeInputs::for_fenced_target(
                                        remotes.membership_generation(),
                                        &remotes.paths,
                                        instance,
                                        shape,
                                    ),
                                    None => tcp_original_native_inputs
                                        .expect("TCP Apply requires resolved Native inputs"),
                                };
                                let authority = self
                                    .multipath
                                    .bulk_original_data_apply_authority_from_native_inputs(
                                        context,
                                        remotes,
                                        &plan,
                                        &frame,
                                        selection_lane,
                                        frontier_state,
                                        request_load_claim.is_some(),
                                        native_inputs,
                                    );
                                if authority.is_none_or(|authority| !authority.has_headroom()) {
                                    #[cfg(feature = "lab-diagnostics")]
                                    lab_diagnostic(
                                        "request_product_admission",
                                        format_args!(
                                            "phase=exact_bulk_authority_exhausted stream_id={} underlay={:?} path_index={} instance_id={}",
                                            self.multipath.stream_id().0,
                                            instance.key.underlay,
                                            instance.key.index,
                                            instance.attachment_id,
                                        ),
                                    );
                                    return Err(RequestFrameAdmissionError::ServiceBlocked);
                                }
                            }
                            let (payload_bytes, accepted_copy_deadline) =
                                self.commit_fenced_frame_product(
                                    context,
                                    remotes,
                                    RequestFrameProductCommit {
                                        plan: &plan,
                                        frame: &frame,
                                        cause,
                                        position,
                                        path_count,
                                        reinjection_target_snapshot,
                                        request_load_claim,
                                    },
                                    None,
                                )?;
                            command.commit();
                            Ok((payload_bytes, accepted_copy_deadline))
                        })
                    }
                    Err(error) => Some(Err(error)),
                }
            };
            let Some(publish_result) = publish_result else {
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "request_native_authority",
                    format_args!(
                        "phase=apply_stale stream_id={} underlay={:?} path_index={} instance_id={}",
                        self.multipath.stream_id().0,
                        instance.key.underlay,
                        instance.key.index,
                        instance.attachment_id,
                    ),
                );
                continue;
            };
            match publish_result {
                Ok((payload_bytes, accepted_copy_deadline)) => {
                    return Ok((instance, payload_bytes, accepted_copy_deadline));
                }
                Err(
                    RequestFrameAdmissionError::ServiceBlocked
                    | RequestFrameAdmissionError::OrderedTerminalPending,
                ) => {
                    if plan.reject_failed_bulk_original_target(
                        selection_lane,
                        &mut rejected_bulk_original_targets,
                    ) {
                        continue;
                    }
                    return Err(RuntimeError::SenderServiceBlocked);
                }
                Err(RequestFrameAdmissionError::SourceChanged) => {
                    return Err(RuntimeError::SenderServiceBlocked);
                }
                Err(RequestFrameAdmissionError::Source(error)) => {
                    return Err(RuntimeError::Stream(error));
                }
                Err(RequestFrameAdmissionError::Runtime(err)) => {
                    last_error = Some(err);
                    self.fail_client_path_instance(context, remotes, instance);
                    self.multipath.normalize_cursor(remotes.paths.len());
                }
            }
        }
        Err(last_error.unwrap_or(RuntimeError::ReliablePathSessionClosed))
    }

    /// Commits already validated Product ownership inside the selected Native
    /// fence. Publication must follow synchronously; this helper does not read
    /// Native, reserve a future writer, or validate a stale decision by itself.
    fn commit_fenced_frame_product(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        commit: RequestFrameProductCommit<'_>,
        mut source_commit: Option<&mut RequestQueuedSourceCommit<'_>>,
    ) -> Result<(usize, Option<Instant>), RequestFrameAdmissionError> {
        let RequestFrameProductCommit {
            plan,
            frame,
            cause,
            position,
            path_count,
            reinjection_target_snapshot,
            request_load_claim,
        } = commit;
        let (_, instance) = plan.target();
        if let Some(source) = source_commit.as_mut() {
            if !plan.assigns_original_data() {
                return Err(RequestFrameAdmissionError::Source(
                    StreamError::InvalidPreparedFrame,
                ));
            }
            source.validate(frame)?;
            source
                .send_stream
                .commit_prepared_data(frame)
                .map_err(RequestFrameAdmissionError::Source)?;
        }
        let (payload_bytes, accepted_copy_deadline) = match self.multipath.record_emitted_frame(
            context,
            instance,
            frame,
            cause,
            reinjection_target_snapshot,
        ) {
            Ok(recorded) => recorded,
            Err(_) => {
                // Qualification refusal installs neither a tag nor flight.
                // Undo only this just-committed mux prefix before leaving the
                // same synchronous fence; queued source remains untouched.
                if let Some(source) = source_commit.as_mut() {
                    source
                        .send_stream
                        .rollback_committed_data(frame)
                        .map_err(RequestFrameAdmissionError::Source)?;
                }
                return Err(RequestFrameAdmissionError::ServiceBlocked);
            }
        };
        if let Some(source) = source_commit {
            let committed = source
                .sender_queue
                .commit_front_data_prefix(payload_bytes)
                .expect("validated queued source remains the exact Data front during commit");
            assert_eq!(committed.payload_bytes, payload_bytes);
        }
        if let Some(claim) = request_load_claim {
            let remote = &mut remotes.paths[position];
            // The exact path owns the lease after queue
            // reservation and before carrier publication;
            // path removal or relay cancellation releases it.
            assert!(
                remote.load_lease.is_none(),
                "conditionally claimed path load must remain unowned before transfer"
            );
            remote.load_lease = Some(claim);
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "request_startup_selection",
                format_args!(
                    "phase=claim_committed stream_id={} path_index={} instance_id={}",
                    self.multipath.stream_id().0,
                    instance.key.index,
                    instance.attachment_id,
                ),
            );
        }
        // Exact Product ownership and its receipt precede
        // the final queue publication under the same fence.
        self.multipath
            .commit_enqueued_request_product_send(context, frame, plan, position, path_count);
        Ok((payload_bytes, accepted_copy_deadline))
    }

    pub(in crate::runtime) fn send_recv_progress(
        &mut self,
        remotes: &mut ReliableRelayRemoteSet,
        context: &ClientPathContext,
        recv_stream: &mut ReliableRecvStream,
        progress: &mut ReliableRecvProgress,
        request: RelayRecvProgressSend,
    ) -> Result<bool, RuntimeError> {
        if !remotes.has_receive_feedback_output() {
            // Closed command admission is not attachment-removal authority.
            // Preserve cumulative feedback until the ordered carrier terminal
            // event removes this exact attachment or a successor accepts it.
            return Ok(false);
        }

        let mut sent_any = false;
        let ack_generation_before = progress.ack_generation();
        if progress.should_send_ack(
            recv_stream,
            request.path,
            request.lane,
            context.mux_limits,
            request.force_ack,
        ) {
            let generation = progress.ack_generation();
            let publication = if generation == ack_generation_before {
                remotes.retry_pending_stream_ack()
            } else {
                #[cfg(feature = "lab-diagnostics")]
                let ack_started = Instant::now();
                let ack_frames = recv_stream.ack_frames();
                let update_frames = recv_stream.take_ack_update();
                #[cfg(feature = "lab-diagnostics")]
                {
                    lab_perf_record("mux.ack_frames", ack_started.elapsed(), ack_frames.len());
                    if let Some(ack_frame) = ack_frames.last() {
                        let (ack_scope_start, ack_ranges, ack_frontier, ack_largest_end) =
                            match ack_frame {
                                Frame::StreamAck {
                                    scope_start,
                                    ranges,
                                    ..
                                } => (
                                    *scope_start,
                                    ranges.len(),
                                    stream_ack_contiguous_frontier(ranges),
                                    ranges.last().map_or(0, |range| range.end),
                                ),
                                _ => unreachable!("ack_frames only returns STREAM_ACK"),
                            };
                        lab_diagnostic(
                            "recv_progress_ack_state",
                            format_args!(
                                "stream_id={} scope_start={:?} ranges={} frontier={} largest_end={} recv_next_offset={} recv_reorder_bytes={} generation={}",
                                self.multipath.stream_id().0,
                                ack_scope_start,
                                ack_ranges,
                                ack_frontier,
                                ack_largest_end,
                                recv_stream.next_offset(),
                                recv_stream.reorder_bytes(),
                                generation,
                            ),
                        );
                    }
                }
                remotes.publish_stream_ack(generation, update_frames, ack_frames)
            };
            sent_any |= publication.published;
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "recv_progress_ack_emit",
                format_args!(
                    "stream_id={} cause={} generation={} state_changed={} published={} pending={}",
                    self.multipath.stream_id().0,
                    "recv_progress",
                    generation,
                    generation != ack_generation_before,
                    publication.published,
                    publication.pending,
                ),
            );
        }
        if request.publish_max_data
            && progress.should_send_max_data(
                recv_stream,
                request.path,
                request.lane,
                context.mux_limits,
                request.force_max_data,
            )
        {
            let advertised_window = reliable_stream_advertised_window_bytes(
                request.path,
                request.lane,
                context.mux_limits,
            );
            let max_offset = recv_stream.max_data_offset_with_window(advertised_window);
            let publication = remotes.publish_max_data(max_offset);
            if let Some(published_offset) = publication.published_offset {
                recv_stream.commit_max_data(published_offset);
                sent_any = true;
            }
        }
        Ok(sent_any)
    }

    pub(in crate::runtime) fn enqueue_tail_reinjection(
        &mut self,
        sender_queue: &mut ReliableRelaySenderQueue,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        send_stream: &ReliableSendStream,
        lane: TrafficClass,
    ) -> bool {
        self.enqueue_tail_reinjection_inner(
            sender_queue,
            context,
            remotes,
            send_stream,
            lane,
            false,
        )
        .queued
    }

    /// Recovers the exact retained frontier on a measured alternate after the
    /// original owner's immutable fallback, during sending or final drain.
    /// Positive ACK/cache ownership, not a complete negative ACK snapshot,
    /// identifies this work. Exact copy and service bounds still govern it.
    pub(in crate::runtime) fn enqueue_retained_frontier_reinjection(
        &mut self,
        sender_queue: &mut ReliableRelaySenderQueue,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        send_stream: &ReliableSendStream,
        lane: TrafficClass,
    ) -> RequestCompletionTailEnqueueOutcome {
        self.enqueue_tail_reinjection_inner(sender_queue, context, remotes, send_stream, lane, true)
            .completion_tail
            .unwrap_or_default()
    }

    #[allow(clippy::too_many_arguments)]
    fn enqueue_tail_reinjection_inner(
        &mut self,
        sender_queue: &mut ReliableRelaySenderQueue,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        send_stream: &ReliableSendStream,
        lane: TrafficClass,
        completion_tail_fallback: bool,
    ) -> RequestTailReinjectionOutcome {
        let last_send_ack_frontier = send_stream.data_ack_frontier();
        let reinjection_horizon = send_stream.next_offset();
        if (!completion_tail_fallback && last_send_ack_frontier == 0)
            || last_send_ack_frontier >= reinjection_horizon
            || send_stream.reinjection_bytes() == 0
        {
            return RequestTailReinjectionOutcome::default();
        }
        let live_instances = self
            .multipath
            .owner_capable_instances(context, remotes, lane);
        let live_keys = live_instances
            .iter()
            .map(|instance| instance.key)
            .collect::<Vec<_>>();
        if live_keys.len() <= 1 {
            return RequestTailReinjectionOutcome::default();
        }
        let selection_reinjection_quantum = live_instances
            .iter()
            .map(|instance| {
                adaptive_reliable_relay_reinjection_bytes(
                    context.reliable_path_snapshot_for_instance(*instance),
                    lane,
                    context.mux_limits,
                )
            })
            .max()
            .unwrap_or(0);
        let exact_frontier_extent = usize::try_from(
            reinjection_horizon
                .min(send_stream.next_offset())
                .saturating_sub(last_send_ack_frontier),
        )
        .unwrap_or(usize::MAX);
        let observed_at = Instant::now();
        if completion_tail_fallback {
            return RequestTailReinjectionOutcome {
                queued: false,
                completion_tail: Some(self.enqueue_completion_tail_reinjection_inner(
                    sender_queue,
                    context,
                    remotes,
                    send_stream,
                    last_send_ack_frontier,
                    reinjection_horizon,
                    lane,
                    &live_instances,
                    selection_reinjection_quantum,
                    exact_frontier_extent,
                    observed_at,
                )),
            };
        }
        let reinjection_limit = reliable_live_frontier_reinjection_limit_bytes(
            selection_reinjection_quantum,
            selection_reinjection_quantum,
            exact_frontier_extent,
            send_stream.reinjection_bytes(),
            context.mux_limits,
        );
        let reinjection_limit =
            reliable_live_gap_reinjection_authority(reinjection_limit, reinjection_limit, true);
        if reinjection_limit == 0 {
            return RequestTailReinjectionOutcome::default();
        }
        let reinjection_frames = send_stream.retransmission_frames_for_ranges(
            &[OffsetRange {
                start: last_send_ack_frontier,
                end: reinjection_horizon.min(send_stream.next_offset()),
            }],
            reinjection_limit,
        );
        let mut queued = false;
        let mut accepted_recovery_interval = None::<Duration>;
        for frame in reinjection_frames {
            let expected_owner_instances = self
                .multipath
                .original_transmission_instances_for_frame(&frame, &live_instances);
            let expected_owner_keys = expected_owner_instances
                .iter()
                .map(|instance| instance.key)
                .collect::<Vec<_>>();
            if expected_owner_keys.is_empty()
                || !live_keys
                    .iter()
                    .any(|key| !expected_owner_keys.contains(key))
            {
                break;
            }
            let first_reinjection_after = expected_owner_instances
                .iter()
                .map(|instance| {
                    reliable_data_retransmission_interval(
                        Some(instance.key.underlay),
                        context.reliable_path_snapshot_for_instance(*instance),
                    )
                })
                .max()
                .unwrap_or_default();
            let owner_keys = self.multipath.tail_reinjection_owner_keys(
                &frame,
                &live_instances,
                first_reinjection_after,
            );
            if owner_keys.len() != expected_owner_keys.len() {
                break;
            }
            if sender_queue.has_queued_reinjection_overlap(&frame) {
                break;
            }
            let attempt_recovery_interval = first_reinjection_after;
            let payload_bytes = reliable_stream_frame_accounted_bytes(&frame);
            let cause = RelaySendCause::TailReinjection;
            self.enqueue_reinjection_frame_with_priority(sender_queue, frame, cause, true);
            queued = true;
            accepted_recovery_interval = Some(include_live_owner_recovery_interval(
                accepted_recovery_interval,
                attempt_recovery_interval,
            ));
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "reinjection",
                format_args!(
                    "stream_id={} original_underlay={:?} owner_index={} cause=live_original_tail queued=true payload_bytes={}",
                    self.multipath.stream_id().0,
                    owner_keys[0].underlay,
                    owner_keys[0].index,
                    payload_bytes,
                ),
            );
            #[cfg(not(feature = "lab-diagnostics"))]
            let _ = payload_bytes;
        }
        if let Some(recovery_interval) = accepted_recovery_interval {
            // Owner age was proven above for every accepted frame. Linearize
            // the retained successor observation at the end of the successful
            // batch without making it authority for this due cause.
            let accepted_at = Instant::now();
            if self.live_owner_frontier_floor_ready(accepted_at) {
                self.record_live_owner_frontier_floor_attempt(accepted_at, recovery_interval);
            }
        }
        RequestTailReinjectionOutcome {
            queued,
            completion_tail: None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn enqueue_completion_tail_reinjection_inner(
        &mut self,
        sender_queue: &mut ReliableRelaySenderQueue,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        send_stream: &ReliableSendStream,
        frontier: u64,
        horizon: u64,
        lane: TrafficClass,
        live_instances: &[RelayPathInstance],
        selection_reinjection_quantum: usize,
        exact_frontier_extent: usize,
        observed_at: Instant,
    ) -> RequestCompletionTailEnqueueOutcome {
        let selection_limit = reliable_live_frontier_reinjection_limit_bytes(
            selection_reinjection_quantum,
            selection_reinjection_quantum,
            exact_frontier_extent,
            send_stream.reinjection_bytes(),
            context.mux_limits,
        );
        // Only the ranked prefix can influence this transaction. Restrict the
        // owner sweep before collecting/sorting flights, not after discovering
        // an arbitrarily long uniform suffix and querying the prefix again.
        let range = OffsetRange {
            start: frontier,
            end: horizon
                .min(send_stream.next_offset())
                .min(frontier.saturating_add(selection_limit as u64)),
        };
        let Some(scoring_frontier) = self
            .multipath
            .live_owner_uniform_frontier(range, live_instances)
        else {
            return RequestCompletionTailEnqueueOutcome::default();
        };
        if scoring_frontier.owners.len() != 1 {
            return RequestCompletionTailEnqueueOutcome::default();
        }
        // An earlier owner/copy boundary supplies a shorter valid frontier;
        // requiring the whole requested quantum would change admission.
        let scoring_range = scoring_frontier.range;
        let scoring_extent = flight_interval_bytes(scoring_range.start, scoring_range.end);
        if scoring_extent == 0 {
            return RequestCompletionTailEnqueueOutcome::default();
        }
        let Some(scoring_frames) =
            exact_contiguous_retransmission_frames(send_stream, scoring_range)
        else {
            return RequestCompletionTailEnqueueOutcome::default();
        };
        let preview = scoring_frames
            .first()
            .expect("non-empty exact cache prefix")
            .clone();

        if !live_instances
            .iter()
            .any(|instance| !scoring_frontier.owners.contains(instance))
        {
            return RequestCompletionTailEnqueueOutcome::default();
        }
        let Some(owner_recovery_timing) = reliable_data_ack_gap_timing_for_assignments(
            &scoring_frontier.owner_assignments,
            |instance| {
                (
                    instance.key.underlay,
                    context.reliable_path_snapshot_for_instance(instance),
                )
            },
        ) else {
            return RequestCompletionTailEnqueueOutcome::default();
        };
        let owner_recovery_deadline = self.completion_tail_owner_fallback.observe(
            scoring_range,
            &scoring_frontier.owners,
            owner_recovery_timing,
        );
        if observed_at < owner_recovery_deadline {
            return RequestCompletionTailEnqueueOutcome::default();
        }

        // A copy on any still-attached output owns the same-range repeat
        // delay, even if that output is no longer eligible for new work.
        // Merely excluding its target would allow an immediate copy on C.
        // Preserve the existing accepted-copy deadline/wake and stop at the
        // retained prefix instead of skipping to unsuppressed suffix data.
        if scoring_frames.iter().any(|frame| {
            self.reinjection_suppression_deadline_for_frame(frame, remotes)
                .is_some()
        }) {
            return RequestCompletionTailEnqueueOutcome::default();
        }

        let target_observation = self
            .multipath
            .tail_reinjection_fallback_service_target_observation_for_extent(
                context,
                remotes,
                &preview,
                lane,
                sender_queue,
                send_stream.reinjection_bytes(),
                context.mux_limits,
                scoring_extent,
            );
        let Some(target) = target_observation.target else {
            return RequestCompletionTailEnqueueOutcome {
                blocked_for_carrier_capacity: target_observation.target_service_exhausted,
                waiting_for_path_model_publication: target_observation
                    .waiting_for_path_model_publication,
                ..RequestCompletionTailEnqueueOutcome::default()
            };
        };
        if scoring_frontier.avoid.contains(&target.identity.instance) {
            return RequestCompletionTailEnqueueOutcome::default();
        }
        let target_reinjection_quantum = adaptive_reliable_relay_reinjection_bytes(
            Some(target.snapshot),
            lane,
            context.mux_limits,
        );
        let frontier_limit = reliable_live_frontier_reinjection_limit_bytes(
            target_reinjection_quantum,
            selection_reinjection_quantum,
            scoring_extent,
            send_stream.reinjection_bytes(),
            context.mux_limits,
        )
        .min(target.service_limit_bytes);
        let service_limit = reliable_live_gap_reinjection_authority(
            target.service_limit_bytes,
            frontier_limit,
            true,
        );
        if service_limit == 0 {
            return RequestCompletionTailEnqueueOutcome {
                blocked_for_carrier_capacity: target.service_limit_bytes == 0,
                ..RequestCompletionTailEnqueueOutcome::default()
            };
        }
        let applied_extent = service_limit.min(scoring_extent);
        let apply_range = OffsetRange {
            start: frontier,
            end: frontier.saturating_add(applied_extent as u64),
        };
        let Some(frames) = exact_contiguous_retransmission_frames(send_stream, apply_range) else {
            return RequestCompletionTailEnqueueOutcome::default();
        };
        let frames = preserve_reinjection_frontier_quantum(frames, frontier_limit);
        let cause = RelaySendCause::CompletionTailReinjection(target.identity);
        let mut queued = false;
        let mut accepted_recovery_interval = None::<Duration>;
        for frame in frames {
            if sender_queue.has_queued_reinjection_overlap(&frame) {
                break;
            }
            self.enqueue_reinjection_frame_with_priority(sender_queue, frame, cause, true);
            queued = true;
            accepted_recovery_interval = Some(include_live_owner_recovery_interval(
                accepted_recovery_interval,
                target.recovery_interval,
            ));
        }
        if let Some(recovery_interval) = accepted_recovery_interval {
            // The exact original-owner age was proven before target binding.
            // Retain one non-accumulating successor observation for this batch.
            let accepted_at = Instant::now();
            if self.live_owner_frontier_floor_ready(accepted_at) {
                self.record_live_owner_frontier_floor_attempt(accepted_at, recovery_interval);
            }
        }
        RequestCompletionTailEnqueueOutcome {
            queued,
            blocked_for_carrier_capacity: false,
            waiting_for_path_model_publication: false,
        }
    }

    /// Observe obligations once per cooperative Dispatch batch. These are
    /// offsets and exact owners, not queued payload or reserved target service.
    pub(in crate::runtime) fn collect_request_path_recovery(
        &self,
        remotes: &ReliableRelayRemoteSet,
        sender_queue: &ReliableRelaySenderQueue,
    ) -> RequestPathRecoveryBatch {
        let mut batch = RequestPathRecoveryBatch::default();
        let owners = self.multipath.request_recovery_original_paths(remotes);
        if owners.is_empty() {
            return batch;
        }
        let mut copy_ranges = self.multipath.recovery_copy_ranges();
        copy_ranges.retain(|(instance, _)| remotes.contains_path_instance(*instance));
        let queued_ranges =
            normalize_offset_ranges(sender_queue.queued_reinjection_ranges().collect());
        let mut boundaries = copy_ranges
            .iter()
            .flat_map(|(_, ranges)| ranges.iter().flat_map(|range| [range.start, range.end]))
            .collect::<Vec<_>>();
        boundaries.extend(
            queued_ranges
                .iter()
                .flat_map(|range| [range.start, range.end]),
        );
        boundaries.sort_unstable();
        boundaries.dedup();
        for original_instance in owners {
            let recovery = self
                .multipath
                .path_recovery_state(remotes, original_instance);
            batch.retry_deadline = match (batch.retry_deadline, recovery.retry_deadline) {
                (Some(current), Some(deadline)) => Some(current.min(deadline)),
                (None, deadline) => deadline,
                (current, None) => current,
            };
            for range in recovery.uncovered_ranges {
                let mut start = range.start;
                let first = boundaries.partition_point(|boundary| *boundary <= start);
                for end in boundaries[first..]
                    .iter()
                    .copied()
                    .take_while(|end| *end < range.end)
                    .chain(std::iter::once(range.end))
                {
                    let queued_index = queued_ranges.partition_point(|range| range.end <= start);
                    batch.ranges.push(RequestPathRecoveryRange {
                        owner: original_instance,
                        range: OffsetRange { start, end },
                        queued: queued_ranges
                            .get(queued_index)
                            .is_some_and(|range| range.start < end),
                        copy_owners: copy_ranges
                            .iter()
                            .filter_map(|(instance, ranges)| {
                                let index = ranges.partition_point(|range| range.end <= start);
                                ranges
                                    .get(index)
                                    .is_some_and(|range| range.start < end)
                                    .then_some(*instance)
                            })
                            .collect(),
                    });
                    start = end;
                }
            }
        }
        batch.ranges.sort_by_key(|entry| entry.range.start);
        batch
    }

    /// Select the lowest currently serviceable due range before assigning any
    /// target credit. A rejected target/range remains in the flight ledger;
    /// only this finite batch cursor advances so independent targets can run.
    pub(in crate::runtime) fn dispatch_next_request_path_recovery(
        &mut self,
        batch: &mut RequestPathRecoveryBatch,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        send_stream: &ReliableSendStream,
        sender_queue: &ReliableRelaySenderQueue,
    ) -> Result<Option<ClientQueuedDispatch>, RuntimeError> {
        while let Some(entry) = batch.ranges.get(batch.next_range).cloned() {
            if entry.queued {
                // The queue union is immutable for this serialized batch and
                // this entire range lies within it. Native credit/cache slices
                // cannot change overlap; no per-frame queue scan is needed.
                // Queue removal owns the actor's subsequent rediscovery wake.
                batch.next_range += 1;
                batch.rejected_targets.clear();
                continue;
            }
            let Some(preview) =
                send_stream.first_retransmission_frame_for_range(entry.range, usize::MAX)
            else {
                batch.next_range += 1;
                batch.rejected_targets.clear();
                continue;
            };
            let unbound_cause = if remotes.contains_path_instance(entry.owner) {
                RelaySendCause::StalePathReinjection(entry.owner)
            } else {
                RelaySendCause::PathFailureReinjection
            };
            let mut excluded_targets =
                self.multipath
                    .reinjection_avoid_instances(&preview, unbound_cause, remotes);
            if !excluded_targets.contains(&entry.owner) {
                excluded_targets.push(entry.owner);
            }
            for instance in &entry.copy_owners {
                if !excluded_targets.contains(instance) {
                    excluded_targets.push(*instance);
                }
            }
            for instance in &batch.rejected_targets {
                if !excluded_targets.contains(instance) {
                    excluded_targets.push(*instance);
                }
            }
            let (target, _) = self.multipath.reinjection_path_snapshot(
                context,
                remotes,
                &excluded_targets,
                sender_queue,
                send_stream.reinjection_bytes(),
                context.mux_limits,
            );
            let Some((instance, _, limit)) = target else {
                batch.blocked_for_carrier_capacity = true;
                batch.next_range += 1;
                batch.rejected_targets.clear();
                continue;
            };
            let frame = send_stream
                .first_retransmission_frame_for_range(entry.range, limit)
                .expect("positive exact target service and retained preview");
            let Frame::StreamData {
                offset,
                ref payload,
                ..
            } = frame
            else {
                unreachable!("retained cache contains only stream data")
            };
            let payload_bytes = payload.len();
            let end = offset.saturating_add(payload_bytes as u64);
            let target = ClientReinjectionOutputIdentity { instance };
            let cause = match unbound_cause {
                RelaySendCause::StalePathReinjection(owner) => {
                    RelaySendCause::ClientStalePathReinjection { owner, target }
                }
                _ => RelaySendCause::ClientPathFailureReinjection(target),
            };
            match self.send_frame_at_frontier(
                context,
                remotes,
                frame,
                cause,
                None,
                ReliableDataAckFrontierState::Live,
                Some(RequestReinjectionQueueContext {
                    queue: sender_queue,
                    exclude_front: false,
                }),
            ) {
                Ok(outcome) => {
                    // Discovery and failed Apply attempts consume no optional
                    // traffic account. An actual committed copy counts once.
                    self.optional_reinjection.record_reinjection(payload_bytes);
                    let deadline = outcome
                        .accepted_copy_deadline
                        .expect("committed structural copy owns its exact deadline");
                    batch.retry_deadline = Some(
                        batch
                            .retry_deadline
                            .map_or(deadline, |old| old.min(deadline)),
                    );
                    batch.ranges[batch.next_range].range.start = end;
                    if end >= entry.range.end {
                        batch.next_range += 1;
                        batch.rejected_targets.clear();
                    }
                    return Ok(Some(ClientQueuedDispatch::Reinjection {
                        payload_bytes,
                        accepted_copy_deadline: deadline,
                    }));
                }
                Err(error)
                    if matches!(error, RuntimeError::SenderServiceBlocked)
                        || reliable_path_error_is_migratable(&error) =>
                {
                    batch.rejected_targets.push(instance);
                    // Retry this same lowest range on another exact target;
                    // only after all are unavailable can a later range run.
                }
                Err(error) => return Err(error),
            }
        }
        Ok(None)
    }

    fn record_decision(
        &self,
        path_key: RelayPathKey,
        payload_bytes: usize,
        frame: &Frame,
        cause: RelaySendCause,
    ) {
        #[cfg(feature = "lab-diagnostics")]
        {
            let (frame_offset, frame_end_offset) = match frame {
                Frame::StreamData {
                    offset, payload, ..
                } => (*offset, offset.saturating_add(payload.len() as u64)),
                _ => (0, 0),
            };
            lab_sender_service_decision(
                "client",
                None,
                self.multipath.stream_id().0,
                "primary",
                sender_service_frame_kind(frame),
                payload_bytes,
                None,
                format_args!(
                    "cause={} path_underlay={:?} path_index={} pacing_bytes={} reinjection={} frame_offset={} frame_end_offset={}",
                    cause.as_str(),
                    path_key.underlay,
                    path_key.index,
                    reliable_path_frame_pacing_bytes(frame),
                    cause.is_reinjection(),
                    frame_offset,
                    frame_end_offset,
                ),
            );
        }
        #[cfg(not(feature = "lab-diagnostics"))]
        {
            let _ = (path_key, payload_bytes, frame, cause);
        }
    }
}

#[cfg(test)]
fn emit_request_frame_with_mode(
    stream: &ReliablePathStreamHandle,
    frame: Frame,
    lane: TrafficClass,
    emit_mode: CarrierEmitMode,
    reinjection: bool,
) -> Result<(), RuntimeError> {
    let commands = fixed_request_output_commands(&stream.output)?;
    let reservation =
        reserve_request_frame_with_mode(commands, frame, lane, emit_mode, reinjection)
            .map_err(RequestFrameAdmissionError::into_runtime)?;
    reservation.commit();
    Ok(())
}

fn fixed_request_output_commands(
    output: &ReliablePathStreamOutput,
) -> Result<&ReliablePathCommandSender, RuntimeError> {
    match output {
        ReliablePathStreamOutput::Fixed(fixed) => Ok(fixed.commands()),
        ReliablePathStreamOutput::Switchable(_) => {
            Err(RuntimeError::Protocol("request relay path is not fixed"))
        }
    }
}

fn reserve_request_frame_with_mode<'a>(
    commands: &'a ReliablePathCommandSender,
    frame: Frame,
    lane: TrafficClass,
    emit_mode: CarrierEmitMode,
    reinjection: bool,
) -> Result<ReliablePathFrameReservation<'a>, RequestFrameAdmissionError> {
    let result = if reinjection {
        commands.try_reserve_reinjection_frame(frame, lane)
    } else {
        emit_mode.try_reserve_frame(commands, frame, lane)
    };
    result.map_err(RequestFrameAdmissionError::from_runtime)
}

/// Request-local classification of synchronous command admission.
///
/// At this boundary the caller still owns a generation-fenced exact attachment,
/// so a closed command pipe means its ordered terminal is pending rather than
/// that Product membership is already absent.
#[derive(Debug)]
enum RequestFrameAdmissionError {
    ServiceBlocked,
    OrderedTerminalPending,
    // Source/claim failures do not implicate the selected carrier. In
    // particular they must never enter the Runtime path-retirement branch.
    SourceChanged,
    Source(StreamError),
    Runtime(RuntimeError),
}

impl RequestFrameAdmissionError {
    fn from_runtime(error: RuntimeError) -> Self {
        match error {
            RuntimeError::SenderServiceBlocked => Self::ServiceBlocked,
            RuntimeError::ReliablePathSessionClosed => Self::OrderedTerminalPending,
            error => Self::Runtime(error),
        }
    }

    #[cfg(test)]
    fn into_runtime(self) -> RuntimeError {
        match self {
            Self::ServiceBlocked => RuntimeError::SenderServiceBlocked,
            Self::OrderedTerminalPending => RuntimeError::ReliablePathSessionClosed,
            Self::SourceChanged => RuntimeError::SenderServiceBlocked,
            Self::Source(error) => RuntimeError::Stream(error),
            Self::Runtime(error) => error,
        }
    }
}

#[cfg(test)]
#[path = "tests_request.rs"]
mod tests;

#[cfg(test)]
#[path = "tests_c3_common.rs"]
mod tests_c3_common;
