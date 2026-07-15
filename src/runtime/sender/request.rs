//! Request-direction sender ownership.
//!
//! One serialized service owns request offsets, exact-path evidence, and the
//! observe/intent/apply scheduling cycle. TCP receipt calibration and QUIC
//! packet-ACK calibration remain distinct mechanisms below that product state.

use self::multipath::RequestMultipathController;
use super::queue::{ReliableRelayQueuedWorkKind, ReliableRelaySenderQueue};
use super::work::{
    CarrierEmitMode, ClientRepairOutputIdentity, RelaySendCause, RelaySendOutcome,
    sender_extra_traffic_startup_floor_bytes, sender_repair_minimum_useful_attempt_bytes,
};
use crate::config::MppPerformanceConfig;
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::{lab_diagnostic, lab_perf_record, lab_sender_service_decision};
use crate::model::capacity::{
    QUIC_PERSISTENT_CONGESTION_THRESHOLD, adaptive_reliable_relay_repair_bytes,
    reliable_stream_advertised_window_bytes,
};
use crate::model::multipath::{ExtraTrafficKind, ExtraTrafficLedger};
use crate::model::path::{RelayPathInstance, RelayPathKey, RelayPathPlacement};
use crate::model::request::evidence::RequestWindowGrowthEvidence;
use crate::mux::MuxLimits;
use crate::mux::stream::{AckOutcome, ReliableRecvStream, ReliableSendStream};
use crate::protocol::frame::reliable_stream_frame_accounted_bytes;
#[cfg(feature = "lab-diagnostics")]
use crate::protocol::frame::{reliable_path_frame_pacing_bytes, stream_ack_contiguous_frontier};
use crate::protocol::{
    Frame, OffsetRange, OutboundPolicy, StreamFlags, StreamId, StreamOpenRole, UnderlayProtocol,
};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::reliable_path_effective_frame_lane;
use crate::runtime::path::{ClientPathContext, ReliableTcpRequestBulkFlowRegistration};
use crate::runtime::relay::io::{
    ReliableRecvProgress, reliable_critical_tail_repair_limit_bytes,
    reliable_relay_error_is_migratable, reliable_relay_tail_repair_delay,
};
use crate::runtime::relay::open::ReliableRelayOpenSpec;
use crate::runtime::relay::remote::{
    ReliableRelayAttachMode, ReliableRelayRemoteSet, attach_reliable_relay_paths,
};
use crate::runtime::stream::{ReliablePathStreamHandle, ReliablePathStreamOutput};
use crate::scheduler::{FlowLane, PathSnapshot, stream_demand_hint_for_lane};
use bytes::Bytes;
use std::collections::HashSet;
use std::time::{Duration, Instant};

mod multipath;
mod quic_capacity;
mod scheduling;
mod tcp_capacity;
#[cfg(test)]
pub(super) mod test_support;

// Ownership boundary:
// Sender services own product work before it reaches carrier command queues.
// Client relay sending and server response dispatch both use this module for
// queueing, reservation intents, and diagnostics. The request multipath owner
// serializes exact flight and product commits; final TCP/UDP emission still
// happens through carrier command senders.

// Local diagnostic naming helper. The response `admission` owner has a private helper
// with the same purpose, but sender is a sibling module and must not
// depend on that module-private symbol when `lab-diagnostics` is enabled.
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

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) enum ClientQueuedDispatch {
    Data { payload_bytes: usize },
    Repair { payload_bytes: usize },
    RepairDeferred,
    PersistentRepairCancelled,
}

pub(in crate::runtime) struct RequestProductAckOutcome {
    pub(in crate::runtime) mux: AckOutcome,
    pub(in crate::runtime) window: RequestWindowGrowthEvidence<RelayPathInstance>,
}

#[derive(Debug)]
pub(in crate::runtime) struct RequestSenderService {
    multipath: RequestMultipathController,
    request_bulk_flow_registration: Option<ReliableTcpRequestBulkFlowRegistration>,
    performance: MppPerformanceConfig,
    extra_traffic: ExtraTrafficLedger,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct RelayRecvProgressSend {
    path: Option<PathSnapshot>,
    lane: FlowLane,
    force_max_data: bool,
    recover_stalled_service: bool,
}

impl RelayRecvProgressSend {
    pub(in crate::runtime) fn new(
        path: Option<PathSnapshot>,
        lane: FlowLane,
        force_max_data: bool,
    ) -> Self {
        Self {
            path,
            lane,
            force_max_data,
            recover_stalled_service: false,
        }
    }

    pub(in crate::runtime) fn recover_stalled_service(mut self) -> Self {
        self.recover_stalled_service = true;
        self
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
            request_bulk_flow_registration: None,
            performance,
            extra_traffic: ExtraTrafficLedger::default(),
        }
    }

    pub(in crate::runtime) fn bind_request_bulk_flow_registration(
        &mut self,
        registration: ReliableTcpRequestBulkFlowRegistration,
    ) {
        self.request_bulk_flow_registration = Some(registration);
    }

    pub(in crate::runtime) async fn fail_client_path_instance(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        instance: RelayPathInstance,
    ) -> bool {
        // Removal cleanup can await a full carrier queue. Only losing an
        // active Service invalidates logical contention; an optional
        // Validation failure must not hide the still-live Service meanwhile.
        let removes_active_service = remotes.paths.iter().any(|path| {
            path.instance() == instance && path.placement == RelayPathPlacement::Active
        });
        if removes_active_service {
            if let Some(registration) = &self.request_bulk_flow_registration {
                registration.update(false, None);
            }
        }
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

    pub(in crate::runtime) fn repair_extra_event_budget_remaining(
        &self,
        mux_limits: MuxLimits,
    ) -> usize {
        let remaining = self.extra_traffic_budget_remaining(mux_limits);
        if remaining < sender_repair_minimum_useful_attempt_bytes(mux_limits) {
            0
        } else {
            remaining
        }
    }

    pub(in crate::runtime) fn enqueue_repair_frame_with_priority(
        &mut self,
        sender_queue: &mut ReliableRelaySenderQueue,
        frame: Frame,
        cause: RelaySendCause,
        mux_limits: MuxLimits,
        critical_priority: bool,
    ) -> bool {
        debug_assert!(cause.is_repair());
        let payload_bytes = reliable_stream_frame_accounted_bytes(&frame);
        let budget = self.extra_traffic.budget(
            sender_extra_traffic_startup_floor_bytes(mux_limits),
            self.performance,
        );
        if !budget.can_spend(payload_bytes) {
            return false;
        }
        self.extra_traffic
            .record_optional(ExtraTrafficKind::Repair, payload_bytes);
        if critical_priority {
            sender_queue.push_critical_repair_with_cause(frame, cause);
        } else {
            sender_queue.push_repair_with_cause(frame, cause);
        }
        true
    }

    pub(in crate::runtime) fn enqueue_critical_repair_frame(
        &mut self,
        sender_queue: &mut ReliableRelaySenderQueue,
        frame: Frame,
        cause: RelaySendCause,
    ) {
        debug_assert!(cause.is_repair());
        let payload_bytes = reliable_stream_frame_accounted_bytes(&frame);
        self.extra_traffic
            .record_optional(ExtraTrafficKind::Repair, payload_bytes);
        sender_queue.push_critical_repair_with_cause(frame, cause);
    }

    pub(in crate::runtime) fn enqueue_critical_tail_repair_frame(
        &mut self,
        sender_queue: &mut ReliableRelaySenderQueue,
        frame: Frame,
    ) -> bool {
        if sender_queue.has_queued_repair_overlap(&frame) {
            return false;
        }
        self.enqueue_critical_repair_frame(sender_queue, frame, RelaySendCause::PathFailureRepair);
        true
    }

    #[cfg(test)]
    fn record_owner_progress_for_test(&mut self, bytes: usize) {
        self.record_owner_progress(bytes);
    }

    pub(in crate::runtime) fn record_owner_progress(&mut self, bytes: usize) {
        self.extra_traffic.record_owner_progress(bytes);
    }

    /// Advances the complete request product-ACK transaction once.
    ///
    /// Unique mux bytes, every transmitted flight copy, and exact OwnerData
    /// evidence remain separate accounting domains across this composition.
    pub(in crate::runtime) fn apply_request_product_ack(
        &mut self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        send_stream: &mut ReliableSendStream,
        ranges: &[OffsetRange],
    ) -> RequestProductAckOutcome {
        #[cfg(feature = "lab-diagnostics")]
        let mux_started = Instant::now();
        let mux = send_stream.apply_normalized_ack(ranges);
        #[cfg(feature = "lab-diagnostics")]
        lab_perf_record("mux.apply_ack", mux_started.elapsed(), mux.released_bytes);
        if mux.released_bytes > 0 {
            self.record_owner_progress(mux.released_bytes);
        }
        let acked_at = Instant::now();
        let window = self
            .multipath
            .apply_product_ack(context, remotes, ranges, acked_at);
        RequestProductAckOutcome { mux, window }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn release_normalized_acked_ranges(
        &mut self,
        context: &ClientPathContext,
        ranges: &[OffsetRange],
    ) {
        self.multipath
            .release_normalized_acked_ranges(context, ranges);
    }

    pub(in crate::runtime) fn discard_unusable_live_owner_tail_repairs(
        &self,
        sender_queue: &mut ReliableRelaySenderQueue,
        remotes: &ReliableRelayRemoteSet,
    ) -> usize {
        self.multipath
            .discard_unusable_live_owner_tail_repairs(sender_queue, remotes)
    }

    pub(in crate::runtime) fn discard_stale_persistent_ack_gap_repairs(
        &self,
        sender_queue: &mut ReliableRelaySenderQueue,
        remotes: &ReliableRelayRemoteSet,
    ) -> usize {
        self.multipath
            .discard_stale_persistent_ack_gap_repairs(sender_queue, remotes)
    }

    pub(in crate::runtime) fn request_ordered_service_instance(&self) -> Option<RelayPathInstance> {
        self.multipath.request_ordered_service_instance()
    }

    pub(in crate::runtime) fn unreported_missing_owner_instances(
        &mut self,
        remotes: &ReliableRelayRemoteSet,
        retry_after: Duration,
    ) -> Vec<RelayPathInstance> {
        self.multipath
            .unreported_missing_owner_instances(remotes, retry_after)
    }

    #[cfg(test)]
    pub(in crate::runtime) fn unreported_missing_owner_keys(
        &mut self,
        remotes: &ReliableRelayRemoteSet,
        retry_after: Duration,
    ) -> Vec<RelayPathKey> {
        self.multipath
            .unreported_missing_owner_keys(remotes, retry_after)
    }

    pub(in crate::runtime) fn release_all(&mut self, context: &ClientPathContext) {
        self.multipath.release_all(context);
    }

    #[cfg(test)]
    pub(in crate::runtime) fn age_product_flights_for_test(&mut self, age: Duration) {
        self.multipath.age_product_flights_for_test(age);
    }

    #[cfg(test)]
    pub(in crate::runtime) fn record_owner_frame_for_test(
        &mut self,
        instance: RelayPathInstance,
        frame: &Frame,
    ) {
        self.multipath.record_owner_frame_for_test(instance, frame);
    }

    #[cfg(test)]
    pub(in crate::runtime) fn ordered_data_owner_for_test(&self) -> Option<RelayPathKey> {
        self.multipath.ordered_data_owner_for_test()
    }

    #[cfg(test)]
    pub(in crate::runtime) async fn send_stream_data(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        frame: Frame,
    ) -> Result<RelaySendOutcome, RuntimeError> {
        self.send_frame(context, remotes, frame, RelaySendCause::StreamData, None)
            .await
    }

    async fn send_stream_data_for_request_lane(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        frame: Frame,
        request_lane: FlowLane,
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
        debug_assert!(!cause.is_repair());
        self.send_frame(context, remotes, frame, cause, None).await
    }

    pub(in crate::runtime) async fn send_repair_frame(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        frame: Frame,
        cause: RelaySendCause,
    ) -> Result<RelaySendOutcome, RuntimeError> {
        debug_assert!(cause.is_repair());
        self.send_frame(context, remotes, frame, cause, None).await
    }

    pub(in crate::runtime) fn ack_gap_repair_path_model(
        &self,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        send_stream: &ReliableSendStream,
        normalized_ranges: &[OffsetRange],
        preview_limit: usize,
        lane: FlowLane,
    ) -> (
        Option<UnderlayProtocol>,
        Option<PathSnapshot>,
        Option<(ClientRepairOutputIdentity, PathSnapshot)>,
    ) {
        let Some(preview) = send_stream
            .retransmission_frames_for_normalized_ack_gaps(normalized_ranges, preview_limit.max(1))
            .into_iter()
            .next()
        else {
            return (None, None, None);
        };
        self.multipath
            .ack_gap_repair_path_model(context, remotes, &preview, lane)
    }

    pub(in crate::runtime) async fn dispatch_client_queued_work(
        &mut self,
        context: &ClientPathContext,
        spec: &ReliableRelayOpenSpec,
        relay_lane: FlowLane,
        request_lane: FlowLane,
        remotes: &mut ReliableRelayRemoteSet,
        send_stream: &mut ReliableSendStream,
        sender_queue: &mut ReliableRelaySenderQueue,
        local_open: bool,
        inflight_path_claims: &HashSet<RelayPathKey>,
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
                    spec,
                    relay_lane,
                    request_lane,
                    remotes,
                    send_stream,
                    sender_queue,
                    local_open,
                    inflight_path_claims,
                    payload,
                    data_quantum_bytes,
                )
                .await
            }
            ReliableRelayQueuedWorkKind::Repair { frame, cause } => {
                self.dispatch_client_repair_work(
                    context,
                    spec,
                    relay_lane,
                    remotes,
                    send_stream,
                    sender_queue,
                    local_open,
                    inflight_path_claims,
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
        spec: &ReliableRelayOpenSpec,
        relay_lane: FlowLane,
        request_lane: FlowLane,
        remotes: &mut ReliableRelayRemoteSet,
        send_stream: &mut ReliableSendStream,
        sender_queue: &mut ReliableRelaySenderQueue,
        local_open: bool,
        inflight_path_claims: &HashSet<RelayPathKey>,
        payload: Bytes,
        data_quantum_bytes: usize,
    ) -> Result<ClientQueuedDispatch, RuntimeError> {
        let dispatch_payload_bytes = data_quantum_bytes.min(payload.len()).max(1);
        let dispatch_payload = payload.slice(..dispatch_payload_bytes);
        let frame = send_stream
            .send_data(dispatch_payload, StreamFlags::NONE)
            .map_err(RuntimeError::Stream)?;
        let retry_frame = frame.clone();
        // Queue priority stays duplex-aware, but request exploration must not
        // borrow bulk classification from reverse-direction response bytes.
        match self
            .send_stream_data_for_request_lane(context, remotes, frame.clone(), request_lane)
            .await
        {
            Ok(outcome) => {
                let committed = sender_queue
                    .commit_front_data_prefix(dispatch_payload_bytes)
                    .expect("sent queued data must still be at queue front");
                let _ = outcome;
                Ok(ClientQueuedDispatch::Data {
                    payload_bytes: committed.payload_bytes,
                })
            }
            Err(RuntimeError::SenderServiceBlocked) => {
                let _ = send_stream.rollback_committed_data(&frame);
                Err(RuntimeError::SenderServiceBlocked)
            }
            Err(err) if reliable_relay_error_is_migratable(&err) => {
                let _ = send_stream.rollback_committed_data(&frame);
                match attach_reliable_relay_paths(
                    context,
                    spec,
                    relay_lane,
                    remotes,
                    send_stream,
                    !local_open,
                    ReliableRelayAttachMode::Any,
                    inflight_path_claims,
                )
                .await
                {
                    Ok(attached) if attached > 0 => {
                        if let Err(err) = send_stream.commit_prepared_data(&frame) {
                            return Err(RuntimeError::Stream(err));
                        }
                        match self
                            .send_stream_data_for_request_lane(
                                context,
                                remotes,
                                retry_frame,
                                request_lane,
                            )
                            .await
                        {
                            Ok(outcome) => {
                                let committed = sender_queue
                                    .commit_front_data_prefix(dispatch_payload_bytes)
                                    .expect("sent queued data must still be at queue front");
                                let _ = outcome;
                                Ok(ClientQueuedDispatch::Data {
                                    payload_bytes: committed.payload_bytes,
                                })
                            }
                            Err(RuntimeError::SenderServiceBlocked) => {
                                let _ = send_stream.rollback_committed_data(&frame);
                                Err(RuntimeError::SenderServiceBlocked)
                            }
                            Err(err) if reliable_relay_error_is_migratable(&err) => {
                                let _ = send_stream.rollback_committed_data(&frame);
                                Err(err)
                            }
                            Err(err) => {
                                let _ = send_stream.rollback_committed_data(&frame);
                                Err(err)
                            }
                        }
                    }
                    Ok(_) => Err(err),
                    Err(err) => Err(err),
                }
            }
            Err(err) => {
                let _ = send_stream.rollback_committed_data(&frame);
                Err(err)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn dispatch_client_repair_work(
        &mut self,
        context: &ClientPathContext,
        spec: &ReliableRelayOpenSpec,
        relay_lane: FlowLane,
        remotes: &mut ReliableRelayRemoteSet,
        send_stream: &mut ReliableSendStream,
        sender_queue: &mut ReliableRelaySenderQueue,
        local_open: bool,
        inflight_path_claims: &HashSet<RelayPathKey>,
        frame: Frame,
        cause: RelaySendCause,
    ) -> Result<ClientQueuedDispatch, RuntimeError> {
        let retry_frame = frame.clone();
        match self.send_repair_frame(context, remotes, frame, cause).await {
            Ok(outcome) => {
                let (_, committed) = sender_queue
                    .commit_front()
                    .expect("sent queued repair must still be at queue front");
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "repair",
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
                Ok(ClientQueuedDispatch::Repair {
                    payload_bytes: committed.payload_bytes,
                })
            }
            Err(RuntimeError::SenderServiceBlocked) => Err(RuntimeError::SenderServiceBlocked),
            Err(err)
                if matches!(cause, RelaySendCause::PersistentClientAckGapRepair(_))
                    && reliable_relay_error_is_migratable(&err) =>
            {
                let discarded = sender_queue.discard_persistent_ack_gap_repair_batch(cause);
                debug_assert!(discarded > 0);
                Ok(ClientQueuedDispatch::PersistentRepairCancelled)
            }
            Err(err)
                if cause == RelaySendCause::LiveOwnerTailRepair
                    && reliable_relay_error_is_migratable(&err) =>
            {
                let (_, _) = sender_queue
                    .commit_front()
                    .expect("deferred live-tail repair must still be at queue front");
                Ok(ClientQueuedDispatch::RepairDeferred)
            }
            Err(err) if reliable_relay_error_is_migratable(&err) => {
                match attach_reliable_relay_paths(
                    context,
                    spec,
                    relay_lane,
                    remotes,
                    send_stream,
                    !local_open,
                    ReliableRelayAttachMode::Any,
                    inflight_path_claims,
                )
                .await
                {
                    Ok(attached) if attached > 0 => {
                        match self
                            .send_repair_frame(context, remotes, retry_frame, cause)
                            .await
                        {
                            Ok(outcome) => {
                                let (_, committed) = sender_queue
                                    .commit_front()
                                    .expect("sent queued repair must still be at queue front");
                                #[cfg(feature = "lab-diagnostics")]
                                lab_diagnostic(
                                    "repair",
                                    format_args!(
                                        "stream_id={} path_underlay={:?} path_index={} cause={} queued_dispatch=true after_attach=true payload_bytes={}",
                                        self.multipath.stream_id().0,
                                        outcome.path_key.underlay,
                                        outcome.path_key.index,
                                        cause.as_str(),
                                        committed.payload_bytes,
                                    ),
                                );
                                #[cfg(not(feature = "lab-diagnostics"))]
                                let _ = outcome;
                                Ok(ClientQueuedDispatch::Repair {
                                    payload_bytes: committed.payload_bytes,
                                })
                            }
                            Err(RuntimeError::SenderServiceBlocked) => {
                                Err(RuntimeError::SenderServiceBlocked)
                            }
                            Err(err) => Err(err),
                        }
                    }
                    Ok(_) => Err(err),
                    Err(err) => Err(err),
                }
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
        request_lane: Option<FlowLane>,
    ) -> Result<RelaySendOutcome, RuntimeError> {
        let sent_frame = frame.clone();
        let avoid_keys = self
            .multipath
            .repair_avoid_keys(&sent_frame, cause, remotes);
        let instance = self
            .emit_relay_frame(context, remotes, frame, cause, &avoid_keys, request_lane)
            .await?;
        let path_key = instance.key;
        let payload_bytes = self
            .multipath
            .record_emitted_frame(instance, &sent_frame, cause);
        self.record_decision(path_key, payload_bytes, &sent_frame, cause);
        Ok(RelaySendOutcome { path_key })
    }

    async fn emit_relay_frame(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        frame: Frame,
        cause: RelaySendCause,
        avoid_keys: &[RelayPathKey],
        request_lane: Option<FlowLane>,
    ) -> Result<RelayPathInstance, RuntimeError> {
        let mut last_error = None;
        while !remotes.paths.is_empty() {
            let stream_lane = remotes
                .paths
                .last()
                .map(|path| path.stream.lane)
                .unwrap_or(FlowLane::Latency);
            let selection_lane = request_lane.unwrap_or(stream_lane);
            let plan = match self.multipath.plan_relay_path_send(
                context,
                remotes,
                &frame,
                selection_lane,
                cause,
                avoid_keys,
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
                        instance.id,
                    ),
                );
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
                                instance.id,
                            ),
                        );
                        return Err(RuntimeError::SenderServiceBlocked);
                    };
                    Some(claim)
                } else {
                    None
                };
            match emit_request_frame_with_mode(
                &remotes.paths[position].stream,
                frame.clone(),
                lane,
                emit_mode,
            ) {
                Ok(()) => {
                    if let Some(claim) = request_load_claim {
                        // The exact path owns the lease after carrier enqueue;
                        // path removal or relay cancellation releases it.
                        remotes.commit_path_instance_load_claim(instance, claim);
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "request_startup_selection",
                            format_args!(
                                "phase=claim_committed stream_id={} path_index={} instance_id={}",
                                self.multipath.stream_id().0,
                                instance.key.index,
                                instance.id,
                            ),
                        );
                    }
                    self.multipath.commit_enqueued_request_product_send(
                        context,
                        remotes,
                        &frame,
                        plan,
                        position,
                        remotes.paths.len(),
                    );
                    return Ok(instance);
                }
                Err(RuntimeError::SenderServiceBlocked) => {
                    return Err(RuntimeError::SenderServiceBlocked);
                }
                Err(err) => {
                    last_error = Some(err);
                    self.multipath.record_emit_failure(instance);
                    self.fail_client_path_instance(context, remotes, instance)
                        .await;
                    self.multipath.normalize_cursor(remotes.paths.len());
                }
            }
        }
        Err(last_error.unwrap_or(RuntimeError::ReliablePathSessionClosed))
    }

    pub(in crate::runtime) async fn reannounce_active_path(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        spec: &ReliableRelayOpenSpec,
        lane: FlowLane,
    ) -> Result<(), RuntimeError> {
        let Some(position) = remotes
            .paths
            .iter()
            .rposition(|path| path.placement == RelayPathPlacement::Active)
        else {
            return Err(RuntimeError::ReliablePathSessionClosed);
        };
        let instance = remotes.paths[position].instance();
        remotes.paths[position].stream.lane = lane;
        let frame = Frame::OpenStream {
            stream_id: remotes.stream_id(),
            target: spec.target.clone(),
            ingress: spec.ingress,
            outbound: OutboundPolicy::Direct,
            demand: stream_demand_hint_for_lane(lane),
            role: StreamOpenRole::Active,
        };
        match emit_request_frame(&remotes.paths[position].stream, frame, FlowLane::Control) {
            Ok(()) => Ok(()),
            Err(err) => {
                self.fail_client_path_instance(context, remotes, instance)
                    .await;
                Err(err)
            }
        }
    }

    pub(in crate::runtime) async fn reannounce_path_instance_as_active(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut ReliableRelayRemoteSet,
        instance: RelayPathInstance,
        spec: &ReliableRelayOpenSpec,
        lane: FlowLane,
    ) -> Result<bool, RuntimeError> {
        let Some(position) = remotes
            .paths
            .iter()
            .position(|path| path.instance() == instance)
        else {
            return Ok(false);
        };
        if remotes.paths[position].placement == RelayPathPlacement::Active
            && position + 1 == remotes.paths.len()
        {
            return Ok(false);
        }
        let frame = Frame::OpenStream {
            stream_id: remotes.stream_id(),
            target: spec.target.clone(),
            ingress: spec.ingress,
            outbound: OutboundPolicy::Direct,
            demand: stream_demand_hint_for_lane(lane),
            role: StreamOpenRole::Active,
        };
        let emit_result = {
            let path = &mut remotes.paths[position];
            path.stream.lane = lane;
            emit_request_frame(&path.stream, frame, FlowLane::Control)
        };
        match emit_result {
            Ok(()) => {
                let activated = remotes.activate_path_instance_after_service_open(instance);
                if activated {
                    remotes.reserve_path_instance_load_if_needed(context, instance, lane);
                }
                Ok(activated)
            }
            Err(err) => {
                self.fail_client_path_instance(context, remotes, instance)
                    .await;
                Err(err)
            }
        }
    }

    pub(in crate::runtime) async fn send_attach_control_to_instance(
        &mut self,
        remotes: &mut ReliableRelayRemoteSet,
        instance: RelayPathInstance,
        send_stream: &ReliableSendStream,
        resend_fin: bool,
    ) -> Result<bool, RuntimeError> {
        let Some(position) = remotes
            .paths
            .iter()
            .position(|path| path.instance() == instance)
        else {
            return Ok(false);
        };
        if !resend_fin {
            return Ok(false);
        }
        emit_request_frame_with_mode(
            &remotes.paths[position].stream,
            Frame::StreamFin {
                stream_id: remotes.stream_id(),
                final_offset: send_stream.next_offset(),
            },
            remotes.paths[position].stream.lane,
            CarrierEmitMode::StreamOrdered,
        )?;
        Ok(true)
    }

    pub(in crate::runtime) async fn send_recv_progress(
        &mut self,
        remotes: &mut ReliableRelayRemoteSet,
        context: &ClientPathContext,
        recv_stream: &ReliableRecvStream,
        progress: &mut ReliableRecvProgress,
        request: RelayRecvProgressSend,
    ) -> Result<bool, RuntimeError> {
        let mut sent_any = false;
        let cause = if request.recover_stalled_service {
            RelaySendCause::RecvProgressRecovery
        } else {
            RelaySendCause::RecvProgress
        };
        let ack_progress_before = progress.clone();
        if progress.should_send_ack(
            recv_stream,
            request.path,
            request.lane,
            context.mux_limits,
            request.force_max_data,
        ) {
            #[cfg(feature = "lab-diagnostics")]
            let ack_started = Instant::now();
            let ack_frames = recv_stream.ack_frames();
            #[cfg(feature = "lab-diagnostics")]
            lab_perf_record("mux.ack_frames", ack_started.elapsed(), ack_frames.len());
            for ack_frame in ack_frames {
                #[cfg(feature = "lab-diagnostics")]
                let (ack_complete, ack_ranges, ack_frontier, ack_largest_end) = match &ack_frame {
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
                match self
                    .send_control_frame(context, remotes, ack_frame, cause)
                    .await
                {
                    Ok(outcome) => {
                        sent_any = true;
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "recv_progress_ack_emit",
                            format_args!(
                                "stream_id={} complete={} ranges={} frontier={} largest_end={} recv_next_offset={} recv_reorder_bytes={} cause={} path_underlay={:?} path_index={}",
                                self.multipath.stream_id().0,
                                ack_complete,
                                ack_ranges,
                                ack_frontier,
                                ack_largest_end,
                                recv_stream.next_offset(),
                                recv_stream.reorder_bytes(),
                                cause.as_str(),
                                outcome.path_key.underlay,
                                outcome.path_key.index,
                            ),
                        );
                        #[cfg(not(feature = "lab-diagnostics"))]
                        let _ = outcome;
                    }
                    Err(RuntimeError::SenderServiceBlocked) => {
                        // Partial incomplete ACK chunks are safe to repeat. Put
                        // the ACK progress cursor back so the omitted chunks are
                        // sent on the next capacity notification instead of
                        // being inferred as sender-side loss.
                        *progress = ack_progress_before;
                        return Ok(sent_any);
                    }
                    Err(err) => return Err(err),
                }
            }
        }
        let max_data_progress_before = progress.clone();
        if progress.should_send_max_data(
            recv_stream,
            request.path,
            request.lane,
            context.mux_limits,
            request.force_max_data,
        ) {
            let advertised_window = reliable_stream_advertised_window_bytes(
                request.path,
                request.lane,
                context.mux_limits,
            );
            match self
                .send_control_frame(
                    context,
                    remotes,
                    recv_stream.max_data_frame_with_window(advertised_window),
                    cause,
                )
                .await
            {
                Ok(_) => {
                    sent_any = true;
                }
                Err(RuntimeError::SenderServiceBlocked) => {
                    *progress = max_data_progress_before;
                    return Ok(sent_any);
                }
                Err(err) => return Err(err),
            }
        }
        Ok(sent_any)
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::runtime) fn enqueue_live_owner_tail_repair(
        &mut self,
        sender_queue: &mut ReliableRelaySenderQueue,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        send_stream: &ReliableSendStream,
        last_send_ack_ranges: &[OffsetRange],
        last_send_ack_complete: bool,
        last_send_ack_frontier: u64,
        lane: FlowLane,
    ) -> bool {
        if !last_send_ack_complete
            || last_send_ack_frontier == 0
            || last_send_ack_frontier >= send_stream.next_offset()
            || send_stream.repair_bytes() == 0
            || !matches!(
                last_send_ack_ranges,
                [range] if range.start == 0 && range.end == last_send_ack_frontier
            )
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
        let repair_limit = reliable_critical_tail_repair_limit_bytes(
            live_keys
                .iter()
                .map(|key| {
                    adaptive_reliable_relay_repair_bytes(
                        context.reliable_path_snapshot(*key),
                        lane,
                        context.mux_limits,
                    )
                })
                .max()
                .unwrap_or(0),
            send_stream.repair_bytes(),
            context.mux_limits,
        );
        if repair_limit == 0 {
            return false;
        }
        let repair_frames = send_stream.retransmission_frames_for_ranges(
            &[OffsetRange {
                start: last_send_ack_frontier,
                end: send_stream.next_offset(),
            }],
            repair_limit,
        );
        let mut queued = false;
        for frame in repair_frames {
            let expected_owner_keys = self
                .multipath
                .ordering_owner_keys_for_frame(&frame, &live_instances);
            if expected_owner_keys.is_empty()
                || !live_keys
                    .iter()
                    .any(|key| !expected_owner_keys.contains(key))
            {
                break;
            }
            let first_repair_after = expected_owner_keys
                .iter()
                .map(|key| reliable_relay_tail_repair_delay(context.reliable_path_snapshot(*key)))
                .max()
                .unwrap_or_default();
            let repeat_repair_after =
                first_repair_after.saturating_mul(QUIC_PERSISTENT_CONGESTION_THRESHOLD);
            let owner_keys = self.multipath.live_owner_tail_repair_owner_keys(
                &frame,
                &live_instances,
                first_repair_after,
                repeat_repair_after,
            );
            if owner_keys.len() != expected_owner_keys.len() {
                break;
            }
            if sender_queue.has_queued_repair_overlap(&frame) {
                continue;
            }
            let payload_bytes = reliable_stream_frame_accounted_bytes(&frame);
            self.enqueue_critical_repair_frame(
                sender_queue,
                frame,
                RelaySendCause::LiveOwnerTailRepair,
            );
            queued = true;
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "repair",
                format_args!(
                    "stream_id={} owner_underlay={:?} owner_index={} cause=live_owner_tail queued=true payload_bytes={}",
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

    pub(in crate::runtime) fn enqueue_failed_path_instance_gap_repairs(
        &mut self,
        sender_queue: &mut ReliableRelaySenderQueue,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        send_stream: &ReliableSendStream,
        failed_instance: RelayPathInstance,
        lane: FlowLane,
    ) -> bool {
        let ranges = self
            .multipath
            .latest_unacked_ranges_for_path_instance(failed_instance);
        self.enqueue_failed_path_gap_repairs_for_ranges(
            sender_queue,
            context,
            remotes,
            send_stream,
            failed_instance.key,
            &[failed_instance],
            ranges,
            lane,
        )
    }

    #[cfg(test)]
    pub(in crate::runtime) fn enqueue_failed_path_gap_repairs(
        &mut self,
        sender_queue: &mut ReliableRelaySenderQueue,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        send_stream: &ReliableSendStream,
        failed_key: RelayPathKey,
        lane: FlowLane,
    ) -> bool {
        let (failed_instances, ranges) = self.multipath.failed_path_gap_parts(failed_key);
        self.enqueue_failed_path_gap_repairs_for_ranges(
            sender_queue,
            context,
            remotes,
            send_stream,
            failed_key,
            &failed_instances,
            ranges,
            lane,
        )
    }

    #[allow(clippy::too_many_arguments)]
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(unused_variables))]
    fn enqueue_failed_path_gap_repairs_for_ranges(
        &mut self,
        sender_queue: &mut ReliableRelaySenderQueue,
        context: &ClientPathContext,
        remotes: &ReliableRelayRemoteSet,
        send_stream: &ReliableSendStream,
        failed_key: RelayPathKey,
        failed_instances: &[RelayPathInstance],
        ranges: Vec<OffsetRange>,
        lane: FlowLane,
    ) -> bool {
        if ranges.is_empty() {
            return false;
        }
        let repair_path = remotes
            .primary_path_key()
            .and_then(|key| context.reliable_path_snapshot(key));
        let repair_limit = reliable_critical_tail_repair_limit_bytes(
            adaptive_reliable_relay_repair_bytes(repair_path, lane, context.mux_limits),
            send_stream.repair_bytes(),
            context.mux_limits,
        );
        let repair_frames = send_stream.retransmission_frames_for_ranges(&ranges, repair_limit);
        if repair_frames.is_empty() {
            return false;
        }
        let mut queued = false;
        for frame in repair_frames {
            let queued_frame = if sender_queue.has_queued_repair_overlap(&frame) {
                false
            } else {
                self.enqueue_critical_repair_frame(
                    sender_queue,
                    frame,
                    RelaySendCause::PathFailureRepair,
                );
                true
            };
            queued |= queued_frame;
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "repair",
                format_args!(
                    "stream_id={} failed_underlay={:?} failed_index={} cause=path_failure queued={}",
                    self.multipath.stream_id().0,
                    failed_key.underlay,
                    failed_key.index,
                    queued_frame,
                ),
            );
        }
        if queued {
            self.multipath
                .record_missing_owner_repair_attempts(failed_instances, Instant::now());
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
                    "cause={} path_underlay={:?} path_index={} pacing_bytes={} repair={} frame_offset={} frame_end_offset={}",
                    cause.as_str(),
                    path_key.underlay,
                    path_key.index,
                    reliable_path_frame_pacing_bytes(frame),
                    cause.is_repair(),
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

fn emit_request_frame(
    stream: &ReliablePathStreamHandle,
    frame: Frame,
    lane: FlowLane,
) -> Result<(), RuntimeError> {
    emit_request_frame_with_mode(stream, frame, lane, CarrierEmitMode::Classified)
}

fn emit_request_frame_with_mode(
    stream: &ReliablePathStreamHandle,
    frame: Frame,
    lane: FlowLane,
    emit_mode: CarrierEmitMode,
) -> Result<(), RuntimeError> {
    emit_fixed_request_output(&stream.output, frame, lane, emit_mode)
}

fn emit_fixed_request_output(
    output: &ReliablePathStreamOutput,
    frame: Frame,
    lane: FlowLane,
    emit_mode: CarrierEmitMode,
) -> Result<(), RuntimeError> {
    match output {
        ReliablePathStreamOutput::Fixed(fixed) => {
            emit_mode.try_enqueue_frame(fixed.commands(), frame, lane)
        }
        ReliablePathStreamOutput::Switchable(_) => {
            Err(RuntimeError::Protocol("request relay path is not fixed"))
        }
    }
}

#[cfg(test)]
#[path = "request_test.rs"]
mod tests;
