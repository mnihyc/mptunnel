//! Request-direction sender ownership.
//!
//! One serialized service owns request offsets, exact-path evidence, and the
//! observe/intent/apply scheduling cycle. TCP keeps its portable fallback;
//! QUIC path use is gated by validation and native writer backpressure.

use self::multipath::RequestMultipathController;
use super::queue::{ReliableRelayQueuedWorkKind, ReliableRelaySenderQueue};
use super::work::{
    CarrierEmitMode, ClientReinjectionOutputIdentity, RelaySendCause, RelaySendOutcome,
    sender_extra_traffic_startup_floor_bytes, sender_reinjection_minimum_useful_attempt_bytes,
};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::{lab_diagnostic, lab_perf_record, lab_sender_service_decision};
use crate::model::capacity::{
    QUIC_PERSISTENT_CONGESTION_THRESHOLD, adaptive_reliable_relay_reinjection_bytes,
    reliable_stream_advertised_window_bytes,
};
use crate::model::multipath::ExtraTrafficLedger;
use crate::model::path::{RelayPathInstance, RelayPathKey};
use crate::model::timing::{
    reliable_data_retransmission_interval, reliable_relay_tail_reinjection_delay,
};
use crate::model::work::{
    reliable_critical_tail_reinjection_limit_bytes, reliable_reinjection_service_limit_bytes,
};
use crate::mux::MuxLimits;
use crate::mux::stream::{
    AckOutcome, ReliableRecvStream, ReliableSendStream, StreamError, ValidatedStreamAck,
};
use crate::performance::MppPerformanceConfig;
use crate::protocol::frame::reliable_stream_frame_accounted_bytes;
#[cfg(feature = "lab-diagnostics")]
use crate::protocol::frame::{reliable_path_frame_pacing_bytes, stream_ack_contiguous_frontier};
use crate::protocol::{Frame, OffsetRange, StreamId, UnderlayProtocol};
use crate::runtime::error::{RuntimeError, reliable_path_error_is_migratable};
use crate::runtime::path::ClientPathContext;
use crate::runtime::path::commands::{
    ReliablePathCommandSender, ReliablePathFrameReservation, reliable_path_effective_frame_lane,
};
#[cfg(test)]
use crate::runtime::stream::ReliablePathStreamHandle;
use crate::runtime::stream::{
    ReliablePathStreamOutput, ReliableRecvProgress, ReliableRelayRemoteSet,
};
use crate::scheduler::{PathSnapshot, TrafficClass};
use bytes::Bytes;
use std::time::{Duration, Instant};

mod multipath;
mod scheduling;
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
    Data { payload_bytes: usize },
    Reinjection { payload_bytes: usize },
    ReinjectionDeferred,
    PersistentReinjectionCancelled,
    PathAttachmentRequired(RuntimeError),
}

pub(in crate::runtime) struct RequestProductAckOutcome {
    pub(in crate::runtime) mux: AckOutcome,
    pub(in crate::runtime) data_ack_progress_paths: smallvec::SmallVec<[RelayPathInstance; 4]>,
}

#[derive(Debug, Default)]
pub(in crate::runtime) struct RequestPathRecoveryOutcome {
    pub(in crate::runtime) queued: bool,
    pub(in crate::runtime) retry_deadline: Option<Instant>,
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
    pub(in crate::runtime) reinjection_completion: Option<Duration>,
}

#[derive(Debug)]
pub(in crate::runtime) struct RequestSenderService {
    multipath: RequestMultipathController,
    performance: MppPerformanceConfig,
    extra_traffic: ExtraTrafficLedger,
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
            extra_traffic: ExtraTrafficLedger::default(),
        }
    }

    pub(in crate::runtime) async fn fail_client_path_instance(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        instance: RelayPathInstance,
    ) -> bool {
        remotes.fail_path_instance(context, instance).await
    }

    fn extra_traffic_budget_remaining(&self, mux_limits: MuxLimits) -> usize {
        self.extra_traffic
            .budget(
                sender_extra_traffic_startup_floor_bytes(mux_limits),
                self.performance,
            )
            .remaining_bytes()
    }

    pub(in crate::runtime) fn reinjection_extra_event_budget_remaining(
        &self,
        mux_limits: MuxLimits,
    ) -> usize {
        let remaining = self.extra_traffic_budget_remaining(mux_limits);
        if remaining < sender_reinjection_minimum_useful_attempt_bytes(mux_limits) {
            0
        } else {
            remaining
        }
    }

    pub(in crate::runtime) fn enqueue_reinjection_frame_with_priority(
        &mut self,
        sender_queue: &mut ReliableRelaySenderQueue,
        frame: Frame,
        cause: RelaySendCause,
        mux_limits: MuxLimits,
        critical_priority: bool,
    ) -> bool {
        debug_assert!(cause.is_reinjection());
        let payload_bytes = reliable_stream_frame_accounted_bytes(&frame);
        let budget = self.extra_traffic.budget(
            sender_extra_traffic_startup_floor_bytes(mux_limits),
            self.performance,
        );
        if !budget.can_spend(payload_bytes) {
            return false;
        }
        self.extra_traffic.record_reinjection(payload_bytes);
        if critical_priority {
            sender_queue.push_critical_reinjection_with_cause(frame, cause);
        } else {
            sender_queue.push_reinjection_with_cause(frame, cause);
        }
        true
    }

    pub(in crate::runtime) fn enqueue_critical_reinjection_frame(
        &mut self,
        sender_queue: &mut ReliableRelaySenderQueue,
        frame: Frame,
        cause: RelaySendCause,
    ) {
        debug_assert!(cause.is_reinjection());
        let payload_bytes = reliable_stream_frame_accounted_bytes(&frame);
        self.extra_traffic.record_reinjection(payload_bytes);
        sender_queue.push_critical_reinjection_with_cause(frame, cause);
    }

    #[cfg(test)]
    fn record_delivered_data_for_test(&mut self, bytes: usize) {
        self.record_delivered_data(bytes);
    }

    pub(in crate::runtime) fn record_delivered_data(&mut self, bytes: usize) {
        self.extra_traffic.record_delivered_data(bytes);
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
        let data_ack_progress_paths =
            self.multipath
                .apply_product_ack(context, remotes, ack.ranges(), acked_at);
        Ok(RequestProductAckOutcome {
            mux,
            data_ack_progress_paths,
        })
    }

    pub(in crate::runtime) fn discard_unusable_tail_reinjections(
        &self,
        sender_queue: &mut ReliableRelaySenderQueue,
        remotes: &ReliableRelayRemoteSet,
    ) -> usize {
        self.multipath
            .discard_unusable_tail_reinjections(sender_queue, remotes)
    }

    pub(in crate::runtime) fn discard_stale_persistent_ack_gap_reinjections(
        &self,
        sender_queue: &mut ReliableRelaySenderQueue,
        remotes: &ReliableRelayRemoteSet,
    ) -> usize {
        self.multipath
            .discard_stale_persistent_ack_gap_reinjections(sender_queue, remotes)
    }

    pub(in crate::runtime) fn discard_resolved_stale_path_reinjections(
        &self,
        sender_queue: &mut ReliableRelaySenderQueue,
        remotes: &ReliableRelayRemoteSet,
    ) -> usize {
        self.multipath
            .discard_resolved_stale_path_reinjections(sender_queue, remotes)
    }

    pub(in crate::runtime) fn mark_request_path_stale(
        &mut self,
        remotes: &ReliableRelayRemoteSet,
        instance: RelayPathInstance,
    ) -> bool {
        if !self.multipath.has_reinjection_path(remotes, instance) {
            return false;
        }
        self.multipath.mark_path_stale(instance)
    }

    pub(in crate::runtime) fn request_path_is_stale(&self, instance: RelayPathInstance) -> bool {
        self.multipath.path_is_stale(instance)
    }

    pub(in crate::runtime) fn unacked_original_paths_before(
        &self,
        remotes: &ReliableRelayRemoteSet,
        horizon: u64,
    ) -> smallvec::SmallVec<[RelayPathInstance; 4]> {
        self.multipath
            .unacked_original_paths_before(remotes, horizon)
    }

    pub(in crate::runtime) fn request_path_has_reinjection_path(
        &self,
        remotes: &ReliableRelayRemoteSet,
        candidate: RelayPathInstance,
    ) -> bool {
        self.multipath.has_reinjection_path(remotes, candidate)
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

    async fn send_stream_data_for_request_lane(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        frame: Frame,
        request_lane: TrafficClass,
    ) -> Result<RelaySendOutcome, RuntimeError> {
        self.send_frame(
            context,
            remotes,
            frame,
            RelaySendCause::StreamData,
            Some(request_lane),
        )
        .await
    }

    pub(in crate::runtime) async fn send_control_frame(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        frame: Frame,
        cause: RelaySendCause,
    ) -> Result<RelaySendOutcome, RuntimeError> {
        debug_assert!(!cause.is_reinjection());
        self.send_frame(context, remotes, frame, cause, None).await
    }

    pub(in crate::runtime) async fn send_reinjection_frame(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        frame: Frame,
        cause: RelaySendCause,
    ) -> Result<RelaySendOutcome, RuntimeError> {
        debug_assert!(cause.is_reinjection());
        self.send_frame(context, remotes, frame, cause, None).await
    }

    pub(in crate::runtime) fn data_ack_gap_reinjection_model(
        &self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        send_stream: &ReliableSendStream,
        normalized_ranges: &[OffsetRange],
        preview_limit: usize,
        lane: TrafficClass,
    ) -> RequestDataAckGapObservation {
        let Some(preview) = send_stream
            .retransmission_frames_for_normalized_ack_gaps(normalized_ranges, preview_limit.max(1))
            .into_iter()
            .next()
        else {
            return RequestDataAckGapObservation::default();
        };
        self.multipath
            .data_ack_gap_reinjection_model(context, remotes, &preview, lane)
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::runtime) async fn dispatch_client_queued_work(
        &mut self,
        context: &ClientPathContext,
        request_lane: TrafficClass,
        remotes: &mut ReliableRelayRemoteSet,
        send_stream: &mut ReliableSendStream,
        sender_queue: &mut ReliableRelaySenderQueue,
        data_quantum_bytes: usize,
    ) -> Result<ClientQueuedDispatch, RuntimeError> {
        let queued_kind = sender_queue
            .front()
            .map(|(_, queued)| queued.kind.clone())
            .expect("queued_send_ready requires queued data");
        match queued_kind {
            ReliableRelayQueuedWorkKind::Control(_) => {
                Err(RuntimeError::Protocol("client sender queue control item"))
            }
            ReliableRelayQueuedWorkKind::Data(payload) => {
                self.dispatch_client_data_work(
                    context,
                    request_lane,
                    remotes,
                    send_stream,
                    sender_queue,
                    payload,
                    data_quantum_bytes,
                )
                .await
            }
            ReliableRelayQueuedWorkKind::Reinjection { frame, cause } => {
                self.dispatch_client_reinjection_work(
                    context,
                    request_lane,
                    remotes,
                    sender_queue,
                    frame,
                    cause,
                )
                .await
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn dispatch_client_data_work(
        &mut self,
        context: &ClientPathContext,
        request_lane: TrafficClass,
        remotes: &mut ReliableRelayRemoteSet,
        send_stream: &mut ReliableSendStream,
        sender_queue: &mut ReliableRelaySenderQueue,
        payload: Bytes,
        data_quantum_bytes: usize,
    ) -> Result<ClientQueuedDispatch, RuntimeError> {
        let dispatch_payload_bytes = data_quantum_bytes.min(payload.len()).max(1);
        let dispatch_payload = payload.slice(..dispatch_payload_bytes);
        let frame = send_stream
            .send_data(dispatch_payload)
            .map_err(RuntimeError::Stream)?;
        // Queue priority stays duplex-aware, but request exploration must not
        // borrow bulk classification from reverse-direction response bytes.
        match self
            .send_stream_data_for_request_lane(context, remotes, frame.clone(), request_lane)
            .await
        {
            Ok(_outcome) => {
                let committed = sender_queue
                    .commit_front_data_prefix(dispatch_payload_bytes)
                    .expect("sent queued data must still be at queue front");
                Ok(ClientQueuedDispatch::Data {
                    payload_bytes: committed.payload_bytes,
                })
            }
            Err(RuntimeError::SenderServiceBlocked) => {
                let _ = send_stream.rollback_committed_data(&frame);
                Err(RuntimeError::SenderServiceBlocked)
            }
            Err(err) if reliable_path_error_is_migratable(&err) => {
                let _ = send_stream.rollback_committed_data(&frame);
                Ok(ClientQueuedDispatch::PathAttachmentRequired(err))
            }
            Err(err) => {
                let _ = send_stream.rollback_committed_data(&frame);
                Err(err)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn dispatch_client_reinjection_work(
        &mut self,
        context: &ClientPathContext,
        request_lane: TrafficClass,
        remotes: &mut ReliableRelayRemoteSet,
        sender_queue: &mut ReliableRelaySenderQueue,
        frame: Frame,
        cause: RelaySendCause,
    ) -> Result<ClientQueuedDispatch, RuntimeError> {
        let dispatch = if matches!(cause, RelaySendCause::CompletionTailReinjection(_)) {
            self.send_frame(context, remotes, frame, cause, Some(request_lane))
                .await
        } else {
            self.send_reinjection_frame(context, remotes, frame, cause)
                .await
        };
        match dispatch {
            Ok(outcome) => {
                let (_, committed) = sender_queue
                    .commit_front()
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
                let (_, _) = sender_queue
                    .commit_front()
                    .expect("deferred live-tail reinjection must still be at queue front");
                Ok(ClientQueuedDispatch::ReinjectionDeferred)
            }
            Err(err) if reliable_path_error_is_migratable(&err) => {
                Ok(ClientQueuedDispatch::PathAttachmentRequired(err))
            }
            Err(err) => Err(err),
        }
    }

    async fn send_frame(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        frame: Frame,
        cause: RelaySendCause,
        request_lane: Option<TrafficClass>,
    ) -> Result<RelaySendOutcome, RuntimeError> {
        let sent_frame = frame.clone();
        let avoid_instances =
            self.multipath
                .reinjection_avoid_instances(&sent_frame, cause, remotes);
        let (instance, payload_bytes) = self
            .emit_relay_frame(
                context,
                remotes,
                frame,
                cause,
                &avoid_instances,
                request_lane,
            )
            .await?;
        let path_key = instance.key;
        self.record_decision(path_key, payload_bytes, &sent_frame, cause);
        Ok(RelaySendOutcome { path_key })
    }

    async fn emit_relay_frame(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        frame: Frame,
        cause: RelaySendCause,
        avoid_instances: &[RelayPathInstance],
        request_lane: Option<TrafficClass>,
    ) -> Result<(RelayPathInstance, usize), RuntimeError> {
        let mut last_error = None;
        while !remotes.paths.is_empty() {
            if let Some(instance) = remotes
                .paths
                .iter()
                .find(|path| path.stream.output_is_terminally_closed())
                .map(|path| path.instance())
            {
                // Capacity and closure are different scheduling facts. Retire
                // a dead carrier before fallback can hide its owner debt.
                last_error = Some(RuntimeError::ReliablePathSessionClosed);
                self.fail_client_path_instance(context, remotes, instance)
                    .await;
                self.multipath.normalize_cursor(remotes.paths.len());
                continue;
            }
            let stream_lane = remotes
                .paths
                .last()
                .map(|path| path.stream.lane)
                .unwrap_or(TrafficClass::Latency);
            let selection_lane = request_lane.unwrap_or(stream_lane);
            let plan = match self.multipath.plan_relay_path_send(
                context,
                remotes,
                &frame,
                selection_lane,
                cause,
                avoid_instances,
            ) {
                Ok(plan) => plan,
                Err(RuntimeError::SenderServiceBlocked) => {
                    return Err(RuntimeError::SenderServiceBlocked);
                }
                Err(err) => return Err(last_error.unwrap_or(err)),
            };
            let (membership_generation, instance) = plan.target();
            let Some(position) =
                remotes.path_position_at_generation(membership_generation, instance)
            else {
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
                return Err(RuntimeError::SenderServiceBlocked);
            }
            if cause.is_reinjection()
                && !cause.permits_busy_carrier_recovery()
                && !self.multipath.ordered_reinjection_carrier_ready(
                    context,
                    &remotes.paths[position],
                    &frame,
                    cause,
                )
            {
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
                    let Some(claim) = context.try_reserve_relay_path_load_if_unchanged(
                        key,
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
                        return Err(RuntimeError::SenderServiceBlocked);
                    };
                    Some(claim)
                } else {
                    None
                };
            let path_count = remotes.paths.len();
            let publish_result = {
                let remote = &mut remotes.paths[position];
                let commands = fixed_request_output_commands(&remote.stream.output)?;
                match reserve_request_frame_with_mode(
                    commands,
                    frame.clone(),
                    lane,
                    emit_mode,
                    cause.is_reinjection(),
                ) {
                    Ok(command) => {
                        if let Some(claim) = request_load_claim {
                            // The exact path owns the lease after queue
                            // reservation and before carrier publication; path
                            // removal or relay cancellation releases it.
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
                        self.multipath.commit_enqueued_request_product_send(
                            context, &frame, plan, position, path_count,
                        );
                        let payload_bytes =
                            self.multipath.record_emitted_frame(instance, &frame, cause);
                        command.commit();
                        Ok(payload_bytes)
                    }
                    Err(error) => Err(error),
                }
            };
            match publish_result {
                Ok(payload_bytes) => {
                    return Ok((instance, payload_bytes));
                }
                Err(RuntimeError::SenderServiceBlocked) => {
                    return Err(RuntimeError::SenderServiceBlocked);
                }
                Err(err) => {
                    last_error = Some(err);
                    self.fail_client_path_instance(context, remotes, instance)
                        .await;
                    self.multipath.normalize_cursor(remotes.paths.len());
                }
            }
        }
        Err(last_error.unwrap_or(RuntimeError::ReliablePathSessionClosed))
    }

    pub(in crate::runtime) async fn send_recv_progress(
        &mut self,
        remotes: &mut ReliableRelayRemoteSet,
        context: &ClientPathContext,
        recv_stream: &mut ReliableRecvStream,
        progress: &mut ReliableRecvProgress,
        request: RelayRecvProgressSend,
    ) -> Result<bool, RuntimeError> {
        let closed_outputs = remotes
            .paths
            .iter()
            .filter(|path| path.stream.request_control_frame_queue_is_closed())
            .map(|path| path.instance())
            .collect::<Vec<_>>();
        for instance in closed_outputs {
            self.fail_client_path_instance(context, remotes, instance)
                .await;
        }
        if !remotes.has_receive_feedback_output() {
            return Err(RuntimeError::ReliablePathSessionClosed);
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
                #[cfg(feature = "lab-diagnostics")]
                {
                    lab_perf_record("mux.ack_frames", ack_started.elapsed(), ack_frames.len());
                    if let Some(ack_frame) = ack_frames.last() {
                        let (ack_complete, ack_ranges, ack_frontier, ack_largest_end) =
                            match ack_frame {
                                Frame::StreamAck {
                                    complete, ranges, ..
                                } => (
                                    *complete,
                                    ranges.len(),
                                    stream_ack_contiguous_frontier(ranges),
                                    ranges.last().map_or(0, |range| range.end),
                                ),
                                _ => unreachable!("ack_frames only returns STREAM_ACK"),
                            };
                        lab_diagnostic(
                            "recv_progress_ack_state",
                            format_args!(
                                "stream_id={} complete={} ranges={} frontier={} largest_end={} recv_next_offset={} recv_reorder_bytes={} generation={}",
                                self.multipath.stream_id().0,
                                ack_complete,
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
                remotes.publish_stream_ack(generation, ack_frames)
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

    #[allow(clippy::too_many_arguments)]
    pub(in crate::runtime) fn enqueue_tail_reinjection(
        &mut self,
        sender_queue: &mut ReliableRelaySenderQueue,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        send_stream: &ReliableSendStream,
        last_send_ack_ranges: &[OffsetRange],
        last_send_ack_complete: bool,
        reinjection_horizon: Option<u64>,
        last_send_ack_frontier: u64,
        lane: TrafficClass,
    ) -> bool {
        self.enqueue_tail_reinjection_inner(
            sender_queue,
            context,
            remotes,
            send_stream,
            last_send_ack_ranges,
            last_send_ack_complete,
            reinjection_horizon,
            last_send_ack_frontier,
            lane,
            false,
        )
    }

    /// Races only a finite retained tail whose measured alternate is expected
    /// to complete earlier than its still-live original owner. The existing
    /// recovery interval, exact-range repeat suppression, and repair envelope
    /// remain the authority for when and how much can be copied.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::runtime) fn enqueue_completion_tail_reinjection(
        &mut self,
        sender_queue: &mut ReliableRelaySenderQueue,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        send_stream: &ReliableSendStream,
        last_send_ack_ranges: &[OffsetRange],
        last_send_ack_complete: bool,
        last_send_ack_frontier: u64,
        lane: TrafficClass,
    ) -> bool {
        self.enqueue_tail_reinjection_inner(
            sender_queue,
            context,
            remotes,
            send_stream,
            last_send_ack_ranges,
            last_send_ack_complete,
            Some(send_stream.next_offset()),
            last_send_ack_frontier,
            lane,
            true,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn enqueue_tail_reinjection_inner(
        &mut self,
        sender_queue: &mut ReliableRelaySenderQueue,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        send_stream: &ReliableSendStream,
        last_send_ack_ranges: &[OffsetRange],
        last_send_ack_complete: bool,
        reinjection_horizon: Option<u64>,
        last_send_ack_frontier: u64,
        lane: TrafficClass,
        require_earlier_completion: bool,
    ) -> bool {
        let Some(reinjection_horizon) = reinjection_horizon else {
            return false;
        };
        if !last_send_ack_complete
            || (!require_earlier_completion && last_send_ack_frontier == 0)
            || last_send_ack_frontier >= reinjection_horizon
            || send_stream.reinjection_bytes() == 0
            || !(matches!(
                last_send_ack_ranges,
                [range] if range.start == 0 && range.end == last_send_ack_frontier
            ) || (last_send_ack_frontier == 0 && last_send_ack_ranges.is_empty()))
        {
            return false;
        }
        let live_instances = self.multipath.owner_capable_instances(remotes);
        let live_keys = live_instances
            .iter()
            .map(|instance| instance.key)
            .collect::<Vec<_>>();
        if live_keys.len() <= 1 {
            return false;
        }
        let reinjection_limit = reliable_critical_tail_reinjection_limit_bytes(
            live_keys
                .iter()
                .map(|key| {
                    adaptive_reliable_relay_reinjection_bytes(
                        context.reliable_path_snapshot(*key),
                        lane,
                        context.mux_limits,
                    )
                })
                .max()
                .unwrap_or(0),
            send_stream.reinjection_bytes(),
            context.mux_limits,
        );
        if reinjection_limit == 0 {
            return false;
        }
        let reinjection_frames = send_stream.retransmission_frames_for_ranges(
            &[OffsetRange {
                start: last_send_ack_frontier,
                end: reinjection_horizon.min(send_stream.next_offset()),
            }],
            reinjection_limit,
        );
        let mut queued = false;
        for frame in reinjection_frames {
            let expected_owner_keys = self
                .multipath
                .original_transmission_keys_for_frame(&frame, &live_instances);
            if expected_owner_keys.is_empty()
                || !live_keys
                    .iter()
                    .any(|key| !expected_owner_keys.contains(key))
            {
                break;
            }
            let first_reinjection_after = expected_owner_keys
                .iter()
                .map(|key| {
                    reliable_relay_tail_reinjection_delay(context.reliable_path_snapshot(*key))
                })
                .max()
                .unwrap_or_default();
            let repeat_reinjection_after =
                first_reinjection_after.saturating_mul(QUIC_PERSISTENT_CONGESTION_THRESHOLD);
            let owner_keys = self.multipath.tail_reinjection_owner_keys(
                &frame,
                &live_instances,
                first_reinjection_after,
                repeat_reinjection_after,
            );
            if owner_keys.len() != expected_owner_keys.len() {
                break;
            }
            if sender_queue.has_queued_reinjection_overlap(&frame) {
                continue;
            }
            let completion_target = if require_earlier_completion {
                let Some(target) = self
                    .multipath
                    .tail_reinjection_earlier_completion_target(context, remotes, &frame, lane)
                else {
                    break;
                };
                Some(target)
            } else {
                None
            };
            let payload_bytes = reliable_stream_frame_accounted_bytes(&frame);
            let cause = if let Some(target) = completion_target {
                RelaySendCause::CompletionTailReinjection(target)
            } else {
                RelaySendCause::TailReinjection
            };
            self.enqueue_critical_reinjection_frame(sender_queue, frame, cause);
            queued = true;
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
        queued
    }

    pub(in crate::runtime) fn drive_request_path_recovery(
        &mut self,
        sender_queue: &mut ReliableRelaySenderQueue,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        send_stream: &ReliableSendStream,
    ) -> RequestPathRecoveryOutcome {
        let mut outcome = RequestPathRecoveryOutcome::default();
        for original_instance in self.multipath.request_recovery_original_paths(remotes) {
            let original_snapshot = if remotes.contains_path_instance(original_instance) {
                context.reliable_path_snapshot(original_instance.key)
            } else {
                context.reliable_path_snapshot_for_instance(original_instance)
            };
            let retry_after = reliable_data_retransmission_interval(
                Some(original_instance.key.underlay),
                original_snapshot,
            );
            let recovery =
                self.multipath
                    .path_recovery_state(remotes, original_instance, retry_after);
            outcome.retry_deadline = match (outcome.retry_deadline, recovery.retry_deadline) {
                (Some(current), Some(deadline)) => Some(current.min(deadline)),
                (None, deadline) => deadline,
                (current, None) => current,
            };
            let cause = if remotes.contains_path_instance(original_instance) {
                RelaySendCause::StalePathReinjection(original_instance)
            } else {
                RelaySendCause::PathFailureReinjection
            };
            outcome.queued |= self.enqueue_path_data_for_reinjection(
                sender_queue,
                context,
                remotes,
                send_stream,
                original_instance.key,
                &[original_instance],
                recovery.uncovered_ranges,
                cause,
            );
        }
        outcome
    }

    #[allow(clippy::too_many_arguments)]
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(unused_variables))]
    fn enqueue_path_data_for_reinjection(
        &mut self,
        sender_queue: &mut ReliableRelaySenderQueue,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        send_stream: &ReliableSendStream,
        failed_key: RelayPathKey,
        failed_instances: &[RelayPathInstance],
        ranges: Vec<OffsetRange>,
        cause: RelaySendCause,
    ) -> bool {
        if ranges.is_empty() {
            return false;
        }
        let reinjection_path =
            self.multipath
                .reinjection_path_snapshot(context, remotes, failed_instances);
        let reinjection_limit = reliable_reinjection_service_limit_bytes(
            reinjection_path,
            sender_queue.bytes(),
            send_stream.reinjection_bytes(),
            context.mux_limits,
        );
        let reinjection_frames =
            send_stream.retransmission_frames_for_ranges(&ranges, reinjection_limit);
        if reinjection_frames.is_empty() {
            return false;
        }
        let mut queued = false;
        for frame in reinjection_frames {
            let queued_frame = if sender_queue.has_queued_reinjection_overlap(&frame) {
                false
            } else {
                self.enqueue_critical_reinjection_frame(sender_queue, frame, cause);
                true
            };
            queued |= queued_frame;
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "reinjection",
                format_args!(
                    "stream_id={} path_underlay={:?} path_index={} cause={} queued={}",
                    self.multipath.stream_id().0,
                    failed_key.underlay,
                    failed_key.index,
                    cause.as_str(),
                    queued_frame,
                ),
            );
        }
        queued
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
        reserve_request_frame_with_mode(commands, frame, lane, emit_mode, reinjection)?;
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
) -> Result<ReliablePathFrameReservation<'a>, RuntimeError> {
    if reinjection {
        commands.try_reserve_reinjection_frame(frame, lane)
    } else {
        emit_mode.try_reserve_frame(commands, frame, lane)
    }
}

#[cfg(test)]
#[path = "tests_request.rs"]
mod tests;
