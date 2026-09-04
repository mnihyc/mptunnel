use super::feedback::{StreamAckPublication, StreamMaxDataPublication};
use super::response::{
    CarrierPathFlight, ResponseDataAckRecoveryCandidate, ResponseDataAckRelease,
    ResponseStreamBinding, product_flights_have_recent_reinjection_overlap,
    release_carrier_path_flight_ranges,
};
use crate::model::capacity::{
    PathRateSample, RELIABLE_INITIAL_WINDOW_PACKETS, ReliableOriginalDataOutput,
    product_delivery_samples_override_startup_prior, reliable_path_startup_sample_limit_bytes,
    reliable_product_feedback_window_bytes, reliable_stream_source_admission,
};
use crate::model::carrier_rate_authority::CarrierRateAuthorityScope;
use crate::model::path::{CarrierPathInstanceId, CarrierPathKey};
use crate::model::service_rate::{DirectionalServiceRate, DirectionalServiceRateScope};
use crate::model::timing::{
    reliable_data_retransmission_interval, transport_rate_sample_freshness_horizon,
};
use crate::model::work::{
    CarrierWorkKind, RangeRecoveryState, ReliableReinjectionTargetWork, ReliableWorkClass,
    reliable_reinjection_service_limit_bytes,
};
use crate::mux::MuxLimits;
use crate::mux::stream::ReliableSendStream;
#[cfg(test)]
use crate::protocol::PathId;
use crate::protocol::frame::{normalize_offset_ranges, reliable_stream_frame_extent};
use crate::protocol::{
    Frame, OffsetRange, PathMetricDirection, PathMetrics, ResetReason, StreamId, UnderlayProtocol,
};
use crate::runtime::RuntimeError;
use crate::runtime::path::authority::NativeCarrierSchedulingShapeSnapshot;
use crate::runtime::path::commands::{
    ReliablePathCarrierTerminalSignal, ReliablePathCommand, ReliablePathCommandSender,
    ReliablePathFrameReservation, RequestTcpCapacityProbeRequest,
};
#[cfg(test)]
use crate::runtime::path::model::{default_path_rate_bps, default_path_srtt_ms};
use crate::runtime::path::proof::enqueue_path_proof_frame;
use crate::runtime::path::{
    CarrierNativeWindowSample, OpenedReliableCarrierStream, RequestTcpCapacityProbeLease,
};
use crate::runtime::sender::ServerReinjectionOutputIdentity;
use crate::scheduler::{PathRateScope, PathSnapshot, TrafficClass};
use smallvec::SmallVec;
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{Notify, mpsc, watch};

/// Product reliable stream handle after an OPEN_STREAM has been accepted.
///
/// The handle owns the stream ID, current traffic class, product frame receive queue, and
/// response output binding for this stream. The carrier is only the emission
/// target; product offsets and reinjection semantics stay above TCP/UDP engines.
pub(in crate::runtime) struct ReliablePathStream {
    pub(in crate::runtime) stream_id: StreamId,
    pub(in crate::runtime) max_offset: u64,
    pub(in crate::runtime) lane: TrafficClass,
    pub(in crate::runtime) underlay: UnderlayProtocol,
    pub(in crate::runtime) max_frame_payload_bytes: usize,
    pub(in crate::runtime) output: ReliablePathStreamOutput,
    pub(in crate::runtime) frames: ReliablePathStreamInput,
}

/// Ordered input consumed by one product-stream actor.
///
/// Client streams receive carrier frames directly. Server streams share one
/// queue for frames and attachment lifecycle so a following path detach cannot
/// overtake ACK processing already accepted from that carrier.
pub(in crate::runtime) enum ReliablePathStreamInput {
    Carrier(mpsc::Receiver<Result<Frame, RuntimeError>>),
    Server {
        events: mpsc::Receiver<ServerReliableStreamEvent>,
        pending: VecDeque<ServerReliableStreamEvent>,
    },
}

pub(in crate::runtime) enum ServerReliableStreamEvent {
    Frame(Frame),
    PathDetached {
        key: CarrierPathKey,
        path_instance_id: CarrierPathInstanceId,
        output_incarnation: u64,
    },
}

impl From<mpsc::Receiver<Result<Frame, RuntimeError>>> for ReliablePathStreamInput {
    fn from(frames: mpsc::Receiver<Result<Frame, RuntimeError>>) -> Self {
        Self::Carrier(frames)
    }
}

impl ReliablePathStreamInput {
    pub(in crate::runtime::stream) fn server(
        events: mpsc::Receiver<ServerReliableStreamEvent>,
    ) -> Self {
        Self::Server {
            events,
            pending: VecDeque::new(),
        }
    }

    fn into_carrier_frames(self) -> mpsc::Receiver<Result<Frame, RuntimeError>> {
        match self {
            Self::Carrier(frames) => frames,
            Self::Server { .. } => {
                unreachable!("server stream input cannot become a fixed carrier receiver")
            }
        }
    }
}

/// Coalesces only state-like feedback before the next data or lifecycle event.
///
/// Complete Data ACK snapshots can arrive out of order on different paths, so
/// their monotonic received ranges are unioned instead of choosing by arrival
/// order. Deltas remain explicitly incomplete. MAX_DATA is monotonic, so only
/// its greatest advertised offset needs to reach the actor.
#[derive(Debug, Default)]
struct ServerFeedbackBatch {
    complete_stream_id: Option<StreamId>,
    complete_ranges: Vec<OffsetRange>,
    delta_stream_id: Option<StreamId>,
    delta_ranges: Vec<OffsetRange>,
    max_data: Option<(StreamId, u64)>,
}

impl ServerFeedbackBatch {
    fn accepts(frame: &Frame) -> bool {
        matches!(frame, Frame::StreamAck { .. } | Frame::StreamMaxData { .. })
    }

    fn push(&mut self, frame: Frame) {
        match frame {
            Frame::StreamAck {
                stream_id,
                complete: true,
                ranges,
            } => {
                self.complete_stream_id.get_or_insert(stream_id);
                self.complete_ranges.extend(ranges);
            }
            Frame::StreamAck {
                stream_id,
                complete: false,
                ranges,
            } => {
                self.delta_stream_id.get_or_insert(stream_id);
                self.delta_ranges.extend(ranges);
            }
            Frame::StreamMaxData {
                stream_id,
                max_offset,
            } => {
                if self
                    .max_data
                    .is_none_or(|(_, current)| max_offset >= current)
                {
                    self.max_data = Some((stream_id, max_offset));
                }
            }
            _ => unreachable!("feedback batch accepts only ACK and MAX_DATA"),
        }
    }

    fn into_frames(self) -> VecDeque<Frame> {
        let mut frames = VecDeque::with_capacity(3);
        // Positive delta coverage is released before the complete union drives
        // gap inference, so one batch cannot infer against bytes it also ACKs.
        if let Some(stream_id) = self.delta_stream_id {
            frames.push_back(Frame::StreamAck {
                stream_id,
                complete: false,
                ranges: normalize_offset_ranges(self.delta_ranges),
            });
        }
        if let Some(stream_id) = self.complete_stream_id {
            frames.push_back(Frame::StreamAck {
                stream_id,
                complete: true,
                ranges: normalize_offset_ranges(self.complete_ranges),
            });
        }
        if let Some((stream_id, max_offset)) = self.max_data {
            frames.push_back(Frame::StreamMaxData {
                stream_id,
                max_offset,
            });
        }
        frames
    }
}

impl ReliablePathStream {
    /// Promotes accepted carrier state into product offset and output ownership.
    pub(in crate::runtime) fn from_opened_carrier(opened: OpenedReliableCarrierStream) -> Self {
        Self {
            stream_id: opened.stream_id,
            max_offset: opened.max_offset,
            lane: opened.lane,
            underlay: opened.underlay,
            max_frame_payload_bytes: opened.max_frame_payload_bytes,
            output: ReliablePathStreamOutput::fixed_with_snapshot_and_path_instance(
                opened.startup,
                opened.portable_startup,
                opened.path_instance_id,
                opened.startup_native_window,
                opened.startup_metrics,
                opened.commands,
                opened.mux_limits,
            ),
            frames: opened.frames.into(),
        }
    }

    pub(in crate::runtime) fn into_handle_and_frames(
        self,
    ) -> (
        ReliablePathStreamHandle,
        mpsc::Receiver<Result<Frame, RuntimeError>>,
        ReliablePathCarrierTerminalSignal,
    ) {
        let terminal = self
            .output
            .terminal_signal()
            .expect("accepted remote stream has one fixed carrier lifecycle");
        (
            ReliablePathStreamHandle {
                stream_id: self.stream_id,
                max_offset: self.max_offset,
                lane: self.lane,
                underlay: self.underlay,
                max_frame_payload_bytes: self.max_frame_payload_bytes,
                output: self.output,
            },
            self.frames.into_carrier_frames(),
            terminal,
        )
    }

    pub(in crate::runtime) async fn recv_frame(&mut self) -> Result<Frame, RuntimeError> {
        loop {
            match &mut self.frames {
                ReliablePathStreamInput::Carrier(frames) => {
                    return match frames.recv().await {
                        Some(Ok(frame)) => Ok(frame),
                        Some(Err(err)) => Err(err),
                        None => Err(RuntimeError::ReliablePathSessionClosed),
                    };
                }
                ReliablePathStreamInput::Server { events, pending } => {
                    let (event, received_from_channel) = match pending.pop_front() {
                        Some(event) => (Some(event), false),
                        None => (events.recv().await, true),
                    };
                    match event {
                        Some(ServerReliableStreamEvent::Frame(frame))
                            if received_from_channel && ServerFeedbackBatch::accepts(&frame) =>
                        {
                            let mut batch = ServerFeedbackBatch::default();
                            batch.push(frame);
                            // Collapse only the backlog visible at entry. Producers
                            // may continue publishing on other runtime workers; an
                            // unbounded try-recv loop must not become actor work.
                            let queued_feedback = events.len();
                            for _ in 0..queued_feedback {
                                match events.try_recv() {
                                    Ok(ServerReliableStreamEvent::Frame(frame))
                                        if ServerFeedbackBatch::accepts(&frame) =>
                                    {
                                        batch.push(frame);
                                    }
                                    Ok(boundary) => {
                                        pending.push_back(boundary);
                                        break;
                                    }
                                    Err(mpsc::error::TryRecvError::Empty)
                                    | Err(mpsc::error::TryRecvError::Disconnected) => break,
                                }
                            }
                            let mut frames = batch.into_frames();
                            let first = frames
                                .pop_front()
                                .expect("a feedback batch contains at least one frame");
                            while let Some(frame) = frames.pop_back() {
                                pending.push_front(ServerReliableStreamEvent::Frame(frame));
                            }
                            return Ok(first);
                        }
                        Some(ServerReliableStreamEvent::Frame(frame)) => return Ok(frame),
                        Some(ServerReliableStreamEvent::PathDetached {
                            key,
                            path_instance_id,
                            output_incarnation,
                        }) => self.output.complete_path_detach(
                            key,
                            path_instance_id,
                            output_incarnation,
                        ),
                        None => return Err(RuntimeError::ReliablePathSessionClosed),
                    }
                }
            }
        }
    }

    /// Returns the ordered input backlog visible at this instant.
    ///
    /// Callers must snapshot this before a ready-only drain. The value is an
    /// item bound, not permission to cross a lifecycle event.
    pub(in crate::runtime) fn ready_frame_count(&self) -> usize {
        match &self.frames {
            ReliablePathStreamInput::Carrier(frames) => frames.len(),
            ReliablePathStreamInput::Server { events, pending } => {
                pending.len().saturating_add(events.len())
            }
        }
    }

    /// Takes one already-queued product frame without waiting.
    ///
    /// A server path-detach event is retained as an ordering boundary for the
    /// ordinary `recv_frame` path; data batching must never process through it.
    pub(in crate::runtime) fn try_recv_frame(&mut self) -> Option<Result<Frame, RuntimeError>> {
        match &mut self.frames {
            ReliablePathStreamInput::Carrier(frames) => frames.try_recv().ok(),
            ReliablePathStreamInput::Server { events, pending } => {
                if matches!(
                    pending.front(),
                    Some(ServerReliableStreamEvent::PathDetached { .. })
                ) {
                    return None;
                }
                if let Some(ServerReliableStreamEvent::Frame(frame)) = pending.pop_front() {
                    return Some(Ok(frame));
                }
                match events.try_recv() {
                    Ok(ServerReliableStreamEvent::Frame(frame)) => Some(Ok(frame)),
                    Ok(boundary @ ServerReliableStreamEvent::PathDetached { .. }) => {
                        pending.push_back(boundary);
                        None
                    }
                    Err(mpsc::error::TryRecvError::Empty)
                    | Err(mpsc::error::TryRecvError::Disconnected) => None,
                }
            }
        }
    }

    /// Client request control remains bound to the carrier that opened it;
    /// switchable response output must use response placement instead.
    pub(in crate::runtime) fn try_enqueue_request_control_frame(
        &self,
        frame: Frame,
    ) -> Result<(), RuntimeError> {
        match &self.output {
            ReliablePathStreamOutput::Fixed(fixed) => fixed
                .commands()
                .try_enqueue_admitted_frame(frame, TrafficClass::Control),
            ReliablePathStreamOutput::Switchable(_) => {
                Err(RuntimeError::Protocol("request relay path is not fixed"))
            }
        }
    }

    pub(in crate::runtime) fn current_lane(&self) -> TrafficClass {
        self.output.current_lane(self.lane)
    }

    pub(in crate::runtime) fn send_path_snapshot(
        &self,
        lane: TrafficClass,
        payload_bytes: usize,
    ) -> Option<PathSnapshot> {
        self.output.send_path_snapshot(lane, payload_bytes)
    }

    pub(in crate::runtime) fn send_path_snapshot_and_source_window(
        &self,
        lane: TrafficClass,
        payload_bytes: usize,
    ) -> (Option<PathSnapshot>, usize) {
        self.output
            .send_path_snapshot_and_source_window(lane, payload_bytes)
    }

    pub(in crate::runtime) fn tail_reinjection_snapshot(
        &self,
        ack_frontier: u64,
        lane: TrafficClass,
        payload_bytes: usize,
    ) -> Option<PathSnapshot> {
        self.output
            .tail_reinjection_snapshot(ack_frontier, lane)
            .or_else(|| self.send_path_snapshot(lane, payload_bytes))
    }

    pub(in crate::runtime) fn tail_reinjection_original_underlay(
        &self,
        ack_frontier: u64,
    ) -> Option<UnderlayProtocol> {
        self.output.tail_reinjection_original_underlay(ack_frontier)
    }

    pub(in crate::runtime) fn data_ack_recovery_candidate(
        &self,
        ack_frontier: u64,
    ) -> Option<ResponseDataAckRecoveryCandidate> {
        self.output.data_ack_recovery_candidate(ack_frontier)
    }

    pub(in crate::runtime) fn data_ack_recovery_candidates(
        &self,
        authoritative_horizon: u64,
        lane: TrafficClass,
    ) -> SmallVec<[ResponseDataAckRecoveryCandidate; 4]> {
        self.output
            .data_ack_recovery_candidates(authoritative_horizon, lane)
    }

    pub(in crate::runtime) fn response_output_snapshot(
        &self,
        identity: ServerReinjectionOutputIdentity,
        lane: TrafficClass,
    ) -> Option<PathSnapshot> {
        self.output.response_output_snapshot(identity, lane)
    }

    pub(in crate::runtime) fn request_feedback_underlay(&self) -> Option<UnderlayProtocol> {
        self.output.request_feedback_underlay()
    }

    pub(in crate::runtime) fn request_feedback_path_snapshot(
        &self,
        lane: TrafficClass,
    ) -> Option<PathSnapshot> {
        self.output.request_feedback_path_snapshot(lane)
    }

    pub(in crate::runtime) fn has_output_incarnation(
        &self,
        key: CarrierPathKey,
        incarnation: u64,
    ) -> bool {
        self.output.has_output_incarnation(key, incarnation)
    }

    pub(in crate::runtime) fn set_sender_queue_bytes(&self, bytes: usize) {
        self.output.set_sender_queue_bytes(bytes);
    }

    pub(in crate::runtime) fn subscribe_output_updates(&self) -> Option<watch::Receiver<u64>> {
        self.output.subscribe_updates()
    }

    pub(in crate::runtime) fn has_live_output(&self) -> bool {
        self.output.has_live_output()
    }

    pub(in crate::runtime) fn capacity_notifies(&self) -> Vec<Arc<Notify>> {
        self.output.capacity_notifies()
    }

    pub(in crate::runtime) fn output_membership_generation(&self) -> u64 {
        match &self.output {
            ReliablePathStreamOutput::Switchable(binding) => binding.output_membership_generation(),
            ReliablePathStreamOutput::Fixed(_) => 0,
        }
    }

    pub(in crate::runtime) fn publish_ack(
        &self,
        generation: u64,
        update_frames: &[Frame],
        cumulative_frames: &[Frame],
    ) -> StreamAckPublication {
        match &self.output {
            ReliablePathStreamOutput::Switchable(binding) => {
                binding.publish_ack(generation, update_frames, cumulative_frames)
            }
            ReliablePathStreamOutput::Fixed(_) => StreamAckPublication::default(),
        }
    }

    pub(in crate::runtime) fn retry_pending_ack(
        &self,
        generation: u64,
        cumulative_frames: &[Frame],
    ) -> StreamAckPublication {
        match &self.output {
            ReliablePathStreamOutput::Switchable(binding) => {
                binding.retry_pending_ack(generation, cumulative_frames)
            }
            ReliablePathStreamOutput::Fixed(_) => StreamAckPublication::default(),
        }
    }

    pub(in crate::runtime) fn pending_ack_capacity_notifies(
        &self,
        generation: u64,
    ) -> Vec<Arc<Notify>> {
        match &self.output {
            ReliablePathStreamOutput::Switchable(binding) => {
                binding.pending_ack_capacity_notifies(generation)
            }
            ReliablePathStreamOutput::Fixed(_) => Vec::new(),
        }
    }

    pub(in crate::runtime) fn publish_max_data(&self, max_offset: u64) -> StreamMaxDataPublication {
        match &self.output {
            ReliablePathStreamOutput::Switchable(binding) => {
                binding.publish_max_data(self.stream_id, max_offset)
            }
            ReliablePathStreamOutput::Fixed(_) => StreamMaxDataPublication::default(),
        }
    }

    pub(in crate::runtime) fn retry_pending_max_data(&self) -> StreamMaxDataPublication {
        match &self.output {
            ReliablePathStreamOutput::Switchable(binding) => {
                binding.retry_pending_max_data(self.stream_id)
            }
            ReliablePathStreamOutput::Fixed(_) => StreamMaxDataPublication::default(),
        }
    }

    pub(in crate::runtime) fn has_pending_max_data_publication(&self) -> bool {
        match &self.output {
            ReliablePathStreamOutput::Switchable(binding) => {
                binding.has_pending_max_data_publication()
            }
            ReliablePathStreamOutput::Fixed(_) => false,
        }
    }

    pub(in crate::runtime) fn pending_max_data_capacity_notifies(&self) -> Vec<Arc<Notify>> {
        match &self.output {
            ReliablePathStreamOutput::Switchable(binding) => {
                binding.pending_max_data_capacity_notifies()
            }
            ReliablePathStreamOutput::Fixed(_) => Vec::new(),
        }
    }

    pub(in crate::runtime) fn has_pending_request_requalification_ack(&self) -> bool {
        match &self.output {
            ReliablePathStreamOutput::Switchable(binding) => {
                binding.has_pending_request_requalification_ack()
            }
            ReliablePathStreamOutput::Fixed(_) => false,
        }
    }

    pub(in crate::runtime) fn pending_request_requalification_ack_capacity_notifies(
        &self,
    ) -> Vec<Arc<Notify>> {
        match &self.output {
            ReliablePathStreamOutput::Switchable(binding) => {
                binding.pending_request_requalification_ack_capacity_notifies()
            }
            ReliablePathStreamOutput::Fixed(_) => Vec::new(),
        }
    }

    pub(in crate::runtime) fn retry_pending_request_requalification_ack(
        &self,
    ) -> Result<bool, RuntimeError> {
        match &self.output {
            ReliablePathStreamOutput::Switchable(binding) => {
                binding.retry_pending_request_requalification_ack()
            }
            ReliablePathStreamOutput::Fixed(_) => Ok(false),
        }
    }

    pub(in crate::runtime) fn response_recovery_capacity_notifies(&self) -> Vec<Arc<Notify>> {
        match &self.output {
            ReliablePathStreamOutput::Switchable(binding) => {
                binding.response_recovery_capacity_notifies()
            }
            ReliablePathStreamOutput::Fixed(_) => Vec::new(),
        }
    }

    pub(in crate::runtime) fn try_enqueue_response_requalification_probe(
        &self,
        send_stream: &ReliableSendStream,
        lane: TrafficClass,
        byte_limit: usize,
    ) -> Result<RequalificationAttempt<ServerReinjectionOutputIdentity>, RuntimeError> {
        match &self.output {
            ReliablePathStreamOutput::Switchable(binding) => {
                binding.try_enqueue_response_requalification_probe(send_stream, lane, byte_limit)
            }
            ReliablePathStreamOutput::Fixed(_) => Ok(RequalificationAttempt::Idle),
        }
    }

    pub(in crate::runtime) fn response_requalification_deadline(&self) -> Option<Instant> {
        match &self.output {
            ReliablePathStreamOutput::Switchable(binding) => {
                binding.response_requalification_deadline()
            }
            ReliablePathStreamOutput::Fixed(_) => None,
        }
    }

    pub(in crate::runtime) fn set_lane(&mut self, lane: TrafficClass) {
        self.lane = lane;
        self.output.set_lane(lane);
    }

    pub(in crate::runtime) fn release_normalized_acked_ranges(
        &self,
        ranges: &[OffsetRange],
    ) -> ResponseDataAckRelease {
        self.output.release_normalized_acked_ranges(ranges)
    }

    pub(in crate::runtime) fn has_recent_reinjection_overlap(&self, frame: &Frame) -> bool {
        self.output.has_recent_reinjection_overlap(frame)
    }

    pub(in crate::runtime) fn earliest_reinjection_suppression_deadline(&self) -> Option<Instant> {
        self.output.earliest_reinjection_suppression_deadline()
    }

    pub(in crate::runtime) fn has_multipath_reinjection_alternative(&self) -> bool {
        self.output.has_multipath_reinjection_alternative()
    }

    pub(in crate::runtime) fn has_reinjection_path_for_frame(&self, frame: &Frame) -> bool {
        self.output.has_reinjection_path_for_frame(frame)
    }

    pub(in crate::runtime) fn has_tail_reinjection_output_for_frame(&self, frame: &Frame) -> bool {
        self.output.has_tail_reinjection_output_for_frame(frame)
    }

    pub(in crate::runtime) fn failed_original_recovery_state(&self) -> RangeRecoveryState {
        self.output.failed_original_recovery_state()
    }

    pub(in crate::runtime) fn has_nonstale_reinjection_alternative(
        &self,
        candidate: ServerReinjectionOutputIdentity,
        lane: TrafficClass,
    ) -> bool {
        self.output
            .has_nonstale_reinjection_alternative(candidate, lane)
    }

    pub(in crate::runtime) fn mark_response_output_stale(
        &self,
        identity: ServerReinjectionOutputIdentity,
        lane: TrafficClass,
    ) -> bool {
        self.output.mark_response_output_stale(identity, lane)
    }

    pub(in crate::runtime) fn stale_response_original_outputs(
        &self,
    ) -> Vec<ServerReinjectionOutputIdentity> {
        self.output
            .stale_response_original_outputs(self.current_lane())
    }

    pub(in crate::runtime) fn stale_original_recovery_state(
        &self,
        identity: ServerReinjectionOutputIdentity,
    ) -> RangeRecoveryState {
        self.output
            .stale_original_recovery_state(identity, self.current_lane())
    }

    pub(in crate::runtime) fn has_untracked_data_reinjection_path_for_frame(
        &self,
        frame: &Frame,
    ) -> bool {
        self.output
            .has_untracked_data_reinjection_path_for_frame(frame)
    }

    pub(in crate::runtime) async fn send_detach(&self) {
        self.output.send_stream_detach(self.stream_id).await;
    }

    pub(in crate::runtime) async fn close(&self) {
        self.output.close_stream(self.stream_id).await;
    }

    pub(in crate::runtime) async fn close_ordered(&self, lane: TrafficClass) {
        self.output.close_stream_ordered(self.stream_id, lane).await;
    }

    pub(in crate::runtime) async fn reset_and_close_ordered(
        &self,
        reason: ResetReason,
        lane: TrafficClass,
    ) {
        self.output
            .reset_and_close_stream_ordered(self.stream_id, reason, lane)
            .await;
    }

    /// Transfers timeout reset ownership to the carrier retirement lane before
    /// the Product owner is released. This never waits for bounded queues.
    pub(in crate::runtime) fn retire_with_reset(&self, reason: ResetReason) {
        self.output.retire_stream_with_reset(self.stream_id, reason);
    }

    /// Retires a client-opened stream that never transferred into product
    /// ownership. The carrier mailbox makes this cancellation-safe for Drop.
    pub(in crate::runtime) fn retire_uncommitted(self) -> Result<(), RuntimeError> {
        self.output.retire_accepted_stream(self.stream_id)
    }
}

pub(in crate::runtime) struct ReliablePathStreamHandle {
    pub(in crate::runtime) stream_id: StreamId,
    pub(in crate::runtime) max_offset: u64,
    pub(in crate::runtime) lane: TrafficClass,
    pub(in crate::runtime) underlay: UnderlayProtocol,
    pub(in crate::runtime) max_frame_payload_bytes: usize,
    pub(in crate::runtime) output: ReliablePathStreamOutput,
}

impl ReliablePathStreamHandle {
    pub(in crate::runtime) fn try_enqueue_request_control_frame(
        &self,
        frame: Frame,
    ) -> Result<(), RuntimeError> {
        match &self.output {
            ReliablePathStreamOutput::Fixed(fixed) => fixed
                .commands()
                .try_enqueue_admitted_frame(frame, TrafficClass::Control),
            ReliablePathStreamOutput::Switchable(_) => {
                Err(RuntimeError::Protocol("request relay path is not fixed"))
            }
        }
    }

    /// Enqueues a data-bearing, non-DSN-owning requalification probe on this
    /// exact fixed attachment. It shares the bounded reinjection lane but is
    /// deliberately absent from Product flight ledgers.
    pub(in crate::runtime) fn try_enqueue_requalification_frame(
        &self,
        frame: Frame,
        lane: TrafficClass,
    ) -> Result<(), RuntimeError> {
        match &self.output {
            ReliablePathStreamOutput::Fixed(fixed) => fixed
                .commands()
                .try_reserve_reinjection_frame(frame, lane)
                .map(|reservation| reservation.commit()),
            ReliablePathStreamOutput::Switchable(_) => Err(RuntimeError::Protocol(
                "request requalification requires a fixed attachment",
            )),
        }
    }

    pub(in crate::runtime) fn request_control_frame_admission_is_closed(&self) -> bool {
        match &self.output {
            ReliablePathStreamOutput::Fixed(fixed) => {
                fixed.commands().control_frame_admission_is_closed()
            }
            ReliablePathStreamOutput::Switchable(_) => true,
        }
    }

    pub(in crate::runtime) fn request_control_capacity_notify(&self) -> Option<Arc<Notify>> {
        match &self.output {
            ReliablePathStreamOutput::Fixed(fixed)
                if !fixed.commands().control_frame_admission_is_closed() =>
            {
                Some(fixed.commands().capacity_notify())
            }
            _ => None,
        }
    }

    pub(in crate::runtime) async fn send_detach(&self) {
        self.output.send_stream_detach(self.stream_id).await;
    }

    /// Removes an attached stream through the carrier-owned retirement lane.
    /// This preserves detach-before-close ordering without waiting for a
    /// bounded Product command queue on the carrier being removed.
    pub(in crate::runtime) fn retire_attachment(self) -> Result<(), RuntimeError> {
        self.output.retire_accepted_stream(self.stream_id)
    }

    /// Preserve STREAM_FIN ordering during successful product retirement.
    pub(in crate::runtime) async fn detach_and_close_ordered(&self) {
        match &self.output {
            ReliablePathStreamOutput::Fixed(fixed) => {
                let _ = fixed
                    .commands()
                    .send_stream_ordered_frame(
                        Frame::StreamDetach {
                            stream_id: self.stream_id,
                        },
                        self.lane,
                    )
                    .await;
                let _ = fixed
                    .commands()
                    .send_stream_ordered_close(self.stream_id, self.lane)
                    .await;
            }
            ReliablePathStreamOutput::Switchable(binding) => {
                binding
                    .close_stream_ordered(self.stream_id, self.lane)
                    .await;
            }
        }
    }

    /// A failed product endpoint is terminal; reset it ahead of queued payload
    /// so the peer does not retain or reinject a stream that cannot recover.
    pub(in crate::runtime) async fn reset_and_close(&self, reason: ResetReason) {
        self.output
            .reset_and_close_stream(self.stream_id, reason)
            .await;
    }

    /// Transfers timeout reset ownership without waiting for a bounded carrier
    /// queue; the caller may release Product membership immediately afterward.
    pub(in crate::runtime) fn retire_with_reset(&self, reason: ResetReason) {
        self.output.retire_stream_with_reset(self.stream_id, reason);
    }

    pub(in crate::runtime) fn enqueue_path_proof(&self) -> Result<Option<u64>, RuntimeError> {
        self.output.enqueue_path_proof()
    }

    pub(in crate::runtime) fn try_enqueue_request_tcp_capacity_probe(
        &self,
        request: RequestTcpCapacityProbeRequest,
        lease: RequestTcpCapacityProbeLease,
    ) -> Result<(), RuntimeError> {
        if self.underlay != UnderlayProtocol::Tcp {
            return Err(RuntimeError::Protocol(
                "request TCP capacity probe requires a TCP output",
            ));
        }
        match &self.output {
            ReliablePathStreamOutput::Fixed(fixed) => fixed
                .commands()
                .try_enqueue_request_tcp_capacity_probe(request, lease),
            ReliablePathStreamOutput::Switchable(_) => Err(RuntimeError::Protocol(
                "request TCP capacity probe requires a fixed client output",
            )),
        }
    }

    pub(in crate::runtime) fn can_enqueue_frame_now(
        &self,
        frame: &Frame,
        lane: TrafficClass,
    ) -> bool {
        self.output.can_enqueue_frame_now(frame, lane)
    }

    pub(in crate::runtime) fn can_enqueue_reinjection_frame_now(&self, frame: &Frame) -> bool {
        self.output.can_enqueue_reinjection_frame_now(frame)
    }

    /// Exact command work accepted by this request attachment but not yet
    /// released by its ordered carrier writer.
    pub(in crate::runtime) fn carrier_pending_bytes(&self) -> Option<u64> {
        match &self.output {
            ReliablePathStreamOutput::Fixed(fixed) => Some(fixed.commands().pending_bytes()),
            ReliablePathStreamOutput::Switchable(_) => None,
        }
    }

    pub(in crate::runtime) fn can_enqueue_work_lane_now(
        &self,
        work_lane: ReliableWorkClass,
        relay_lane: TrafficClass,
    ) -> bool {
        self.output
            .can_enqueue_lane_now(reliable_work_lane_to_carrier_lane(work_lane, relay_lane))
    }

    pub(in crate::runtime) fn capacity_notifies(&self) -> Vec<Arc<Notify>> {
        self.output.capacity_notifies()
    }

    /// Compatibility probe for pre-model red/control fixtures. The post-model
    /// answer comes from the authoritative ordered lifecycle, not channel
    /// ownership, so planned drain remains non-terminal until retirement.
    #[cfg(test)]
    pub(in crate::runtime) fn output_is_terminally_closed(&self) -> bool {
        self.output
            .terminal_signal()
            .and_then(|signal| signal.cause())
            .is_some()
    }

    pub(in crate::runtime) fn product_admission_active(&self) -> bool {
        self.output.product_admission_active()
    }

    pub(in crate::runtime) async fn close(&self) {
        self.output.close_stream(self.stream_id).await;
    }
}

#[derive(Clone)]
pub(in crate::runtime) enum ReliablePathStreamOutput {
    /// A fixed carrier command pipe, used by client-side paths where the stream
    /// is bound to the currently opened carrier association.
    Fixed(Arc<FixedReliablePathOutput>),
    /// A switchable response binding, used by server-side streams that may send
    /// later response bytes or reinjection ranges over another joined carrier path.
    Switchable(Arc<ResponseStreamBinding>),
}

pub(in crate::runtime) struct FixedReliablePathOutput {
    key: CarrierPathKey,
    startup: PathSnapshot,
    portable_startup: PathSnapshot,
    service_rate_scope: DirectionalServiceRateScope,
    native_authority_scope: Option<CarrierRateAuthorityScope>,
    native_window_epoch: Option<CarrierNativeWindowSample>,
    native_rate_epoch: Option<FixedNativeRateEpoch>,
    commands: ReliablePathCommandSender,
    mux_limits: MuxLimits,
    model: Mutex<FixedReliablePathModel>,
}

#[derive(Debug, Clone, Copy)]
enum FixedRateDecision {
    Legacy,
    Native(NativeCarrierSchedulingShapeSnapshot),
}

#[derive(Default)]
struct FixedReliablePathModel {
    original_data_in_flight_bytes: u64,
    carrier_work_in_flight_bytes: u64,
    data_level_queue_bytes: u64,
    product_progress_bytes: u64,
    product_rate_epoch: Option<FixedProductRateEpoch>,
    srtt_ms: Option<f64>,
    delivery_samples: u32,
    flights: BTreeMap<u64, Vec<CarrierPathFlight>>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct FixedNativeRateEpoch {
    rate_bps: f64,
    observed_at: Instant,
    expires_at: Instant,
}

impl FixedNativeRateEpoch {
    fn from_snapshot_at(snapshot: PathSnapshot, observed_at: Instant) -> Option<Self> {
        let rate_bps = snapshot
            .carrier_delivery_rate_bps
            .filter(|rate| rate.is_finite() && *rate > 0.0)?;
        let horizon = fixed_rate_freshness_horizon(snapshot.srtt_ms, snapshot.jitter_ms);
        observed_at.checked_add(horizon).map(|expires_at| Self {
            rate_bps,
            observed_at,
            expires_at,
        })
    }

    fn from_path_metrics_at(
        snapshot: PathSnapshot,
        metrics: PathMetrics,
        captured_at: Instant,
    ) -> Option<Self> {
        let rate_bps = (metrics.rate_observed && metrics.rate_valid_for_us > 0)
            .then_some(snapshot.carrier_delivery_rate_bps)
            .flatten()
            .filter(|rate| rate.is_finite() && *rate > 0.0)?;
        let observed_at = captured_at
            .checked_sub(Duration::from_micros(u64::from(metrics.metric_age_us)))
            .unwrap_or(captured_at);
        let expires_at =
            captured_at.checked_add(Duration::from_micros(metrics.rate_valid_for_us))?;
        Some(Self {
            rate_bps,
            observed_at,
            expires_at,
        })
    }

    fn is_fresh_at(self, now: Instant) -> bool {
        self.observed_at <= now && now < self.expires_at
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct FixedProductRateEpoch {
    rate_bps: f64,
    sample_count: u32,
    sample_bytes: u64,
    observed_at: Instant,
    expires_at: Instant,
}

impl FixedProductRateEpoch {
    fn new(
        rate_bps: f64,
        sample_count: u32,
        sample_bytes: u64,
        observed_at: Instant,
        freshness_horizon: Duration,
    ) -> Option<Self> {
        (rate_bps.is_finite() && rate_bps > 0.0)
            .then(|| observed_at.checked_add(freshness_horizon))
            .flatten()
            .map(|expires_at| Self {
                rate_bps,
                sample_count,
                sample_bytes,
                observed_at,
                expires_at,
            })
    }

    fn fresh_rate_at(self, now: Instant) -> Option<f64> {
        (self.observed_at <= now && now < self.expires_at).then_some(self.rate_bps)
    }

    fn qualified_completion_rate_at(
        self,
        now: Instant,
        product_progress_bytes: u64,
        mux_limits: MuxLimits,
    ) -> Option<f64> {
        let sample_floor = reliable_path_startup_sample_limit_bytes(mux_limits);
        let persistent_exact_samples =
            product_delivery_samples_override_startup_prior(self.sample_count);
        let full_exact_byte_window = self.sample_count > 0
            && self.sample_bytes >= sample_floor
            && product_progress_bytes >= sample_floor;
        (persistent_exact_samples || full_exact_byte_window)
            .then(|| self.fresh_rate_at(now))
            .flatten()
    }
}

fn fixed_rate_freshness_horizon(srtt_ms: f64, jitter_ms: f64) -> Duration {
    let srtt = Duration::from_secs_f64(srtt_ms.max(0.001) / 1000.0);
    let rttvar = Duration::from_secs_f64(jitter_ms.max(0.0) / 1000.0);
    transport_rate_sample_freshness_horizon(srtt, rttvar)
}

impl FixedReliablePathOutput {
    #[cfg(test)]
    pub(in crate::runtime) fn new(
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: ReliablePathCommandSender,
        mux_limits: MuxLimits,
    ) -> Arc<Self> {
        let startup = PathSnapshot::new(
            path_id,
            underlay,
            default_path_srtt_ms(),
            default_path_rate_bps(),
        );
        Self::new_with_snapshot(startup, commands, mux_limits)
    }

    #[cfg(test)]
    pub(in crate::runtime) fn new_with_snapshot(
        startup: PathSnapshot,
        commands: ReliablePathCommandSender,
        mux_limits: MuxLimits,
    ) -> Arc<Self> {
        let observed_at = Instant::now();
        let native_window_epoch = CarrierNativeWindowSample::new(
            startup.carrier_inflight_limit_bytes,
            observed_at,
            fixed_rate_freshness_horizon(startup.srtt_ms, startup.jitter_ms),
        );
        let native_rate_epoch = FixedNativeRateEpoch::from_snapshot_at(startup, observed_at);
        Self::new_with_snapshot_and_path_instance(
            startup,
            startup,
            CarrierPathInstanceId::from_raw(u64::from(startup.id.0)),
            native_window_epoch,
            native_rate_epoch,
            commands,
            mux_limits,
        )
    }

    fn new_with_snapshot_and_path_instance(
        startup: PathSnapshot,
        portable_startup: PathSnapshot,
        path_instance_id: CarrierPathInstanceId,
        native_window_epoch: Option<CarrierNativeWindowSample>,
        native_rate_epoch: Option<FixedNativeRateEpoch>,
        commands: ReliablePathCommandSender,
        mux_limits: MuxLimits,
    ) -> Arc<Self> {
        Arc::new(Self {
            key: CarrierPathKey {
                underlay: startup.underlay,
                path_id: startup.id,
            },
            startup,
            portable_startup,
            service_rate_scope: DirectionalServiceRateScope::new(
                path_instance_id,
                PathMetricDirection::ClientToServer,
            ),
            native_authority_scope: (startup.underlay == UnderlayProtocol::Udp).then_some(
                CarrierRateAuthorityScope::new(
                    path_instance_id,
                    PathMetricDirection::ClientToServer,
                ),
            ),
            native_window_epoch,
            native_rate_epoch,
            commands,
            mux_limits,
            model: Mutex::new(FixedReliablePathModel::default()),
        })
    }

    pub(in crate::runtime) fn key(&self) -> CarrierPathKey {
        self.key
    }

    pub(in crate::runtime) fn reinjection_output_identity(
        &self,
    ) -> ServerReinjectionOutputIdentity {
        ServerReinjectionOutputIdentity {
            key: self.key,
            // Fixed outputs never detach and reattach within one Product
            // stream. Their frozen flight identity therefore uses incarnation
            // zero for the lifetime of that stream.
            incarnation: 0,
        }
    }

    pub(in crate::runtime) fn commands(&self) -> &ReliablePathCommandSender {
        &self.commands
    }

    fn enqueue_path_proof(&self) -> Result<u64, RuntimeError> {
        enqueue_path_proof_frame(&self.commands, self.key.path_id, self.mux_limits)
    }

    fn current_rate_decision(&self) -> Option<FixedRateDecision> {
        let Some(scope) = self.native_authority_scope else {
            return Some(FixedRateDecision::Legacy);
        };
        let decision = self
            .commands
            .native_rate_authority()?
            .scheduling_shape_snapshot(scope)
            .ok()?;
        debug_assert_eq!(decision.stamp().scope(), scope);
        Some(FixedRateDecision::Native(decision))
    }

    /// Runs one Product ownership transfer against the exact rate decision
    /// that authorized it. TCP remains Receipt/legacy-owned and therefore
    /// executes directly. QUIC re-reads the full current scheduling shape and
    /// executes only while the transport activation fence and central
    /// `(scope, A, I, G)` stamp remain current.
    fn commit_rate_decision<R>(
        &self,
        decision: FixedRateDecision,
        transfer_ownership: impl FnOnce(FixedRateDecision) -> Result<R, RuntimeError>,
    ) -> Result<R, RuntimeError> {
        match decision {
            FixedRateDecision::Legacy => transfer_ownership(FixedRateDecision::Legacy),
            FixedRateDecision::Native(decision) => self
                .commands
                .native_rate_authority()
                .ok_or(RuntimeError::SenderServiceBlocked)?
                .commit_with_current_scheduling_shape(
                    decision.decision().stamp(),
                    |current_shape| transfer_ownership(FixedRateDecision::Native(current_shape)),
                )
                .map_err(|_| RuntimeError::SenderServiceBlocked)?,
        }
    }

    fn try_send_path_snapshot(&self) -> Option<PathSnapshot> {
        self.try_send_path_snapshot_at(TrafficClass::Throughput, Instant::now())
    }

    fn try_send_path_snapshot_at(&self, lane: TrafficClass, now: Instant) -> Option<PathSnapshot> {
        let rate_decision = self.current_rate_decision()?;
        let model = self.model.lock().expect("fixed reliable path model lock");
        Some(self.send_path_snapshot_with_model_at(&model, lane, now, rate_decision))
    }

    #[cfg(test)]
    fn send_path_snapshot(&self) -> PathSnapshot {
        self.try_send_path_snapshot()
            .expect("fixed test output has a valid rate authority")
    }

    #[cfg(test)]
    fn send_path_snapshot_at(&self, lane: TrafficClass, now: Instant) -> PathSnapshot {
        self.try_send_path_snapshot_at(lane, now)
            .expect("fixed test output has a valid rate authority")
    }

    fn send_path_snapshot_with_model_at(
        &self,
        model: &FixedReliablePathModel,
        lane: TrafficClass,
        now: Instant,
        rate_decision: FixedRateDecision,
    ) -> PathSnapshot {
        let startup_service_rate = self
            .startup
            .scheduling_service_rate()
            .filter(|rate| rate.scope() == self.service_rate_scope);
        let product_rate_epoch = model
            .product_rate_epoch
            .filter(|epoch| epoch.fresh_rate_at(now).is_some());
        let raw_product_rate_bps = product_rate_epoch.map(|epoch| epoch.rate_bps);
        let (native_window_epoch, product_rate_bps, carrier_diagnostic_rate_bps, service_rate) =
            match rate_decision {
                FixedRateDecision::Legacy => {
                    let native_window_epoch =
                        self.native_window_epoch.filter(|epoch| epoch.fresh_at(now));
                    let product_rate_bps = product_rate_epoch.and_then(|epoch| {
                        epoch.qualified_completion_rate_at(
                            now,
                            model.product_progress_bytes,
                            self.mux_limits,
                        )
                    });
                    let native_rate_bps = self
                        .native_rate_epoch
                        .filter(|epoch| epoch.is_fresh_at(now))
                        .map(|epoch| epoch.rate_bps);
                    (
                        native_window_epoch,
                        product_rate_bps,
                        native_rate_bps,
                        startup_service_rate,
                    )
                }
                FixedRateDecision::Native(shape) => (
                    None,
                    None,
                    shape.finite_rate_bps().map(|rate| rate as f64),
                    Some(shape.service_rate()),
                ),
            };
        // The deployed scorer still consumes this legacy scalar. Preserve its
        // complete pre-typed projection until scorer migration is atomic:
        // Legacy uses fresh attached carrier evidence or its startup fallback,
        // then lets qualified Product completion raise (never lower) that
        // baseline. Native uses only its exact typed value; Unlimited retains
        // the startup scalar solely as a compatibility projection.
        let scalar_startup_rate_bps = self.portable_startup.delivery_rate_bps.max(1.0);
        let scalar_carrier_rate_bps = match rate_decision {
            FixedRateDecision::Legacy => {
                carrier_diagnostic_rate_bps.unwrap_or(scalar_startup_rate_bps)
            }
            FixedRateDecision::Native(_) => service_rate
                .and_then(DirectionalServiceRate::finite_rate_bps)
                .map_or(scalar_startup_rate_bps, |rate| rate as f64),
        };
        let (delivery_rate_bps, rate_scope) = match (rate_decision, product_rate_bps) {
            (FixedRateDecision::Legacy, Some(rate)) if rate > scalar_carrier_rate_bps => {
                (rate, PathRateScope::PerFlowGoodput)
            }
            _ => (scalar_carrier_rate_bps, PathRateScope::PathCapacity),
        };
        let delivery_rate_bps = delivery_rate_bps.max(1.0);
        let srtt_ms = match rate_decision {
            FixedRateDecision::Native(shape) => {
                if shape.srtt().is_zero() {
                    self.portable_startup.srtt_ms
                } else {
                    shape.srtt().as_secs_f64() * 1000.0
                }
            }
            FixedRateDecision::Legacy => raw_product_rate_bps
                .and(model.srtt_ms)
                .unwrap_or(self.portable_startup.srtt_ms),
        };
        let mut snapshot = self.startup;
        snapshot.srtt_ms = srtt_ms;
        snapshot.delivery_rate_bps = delivery_rate_bps;
        snapshot.scheduling_service_rate = service_rate;
        snapshot.rate_scope = rate_scope;
        snapshot.carrier_delivery_rate_bps = carrier_diagnostic_rate_bps;
        snapshot.product_progress_rate_bps = product_rate_bps;
        snapshot.has_durable_product_progress = product_rate_bps.is_some()
            && model.product_progress_bytes
                >= reliable_path_startup_sample_limit_bytes(self.mux_limits);
        snapshot.pacing_rate_bps = delivery_rate_bps.max(1.0);
        snapshot.carrier_inflight_limit_bytes =
            native_window_epoch.map_or(0, |epoch| epoch.inflight_limit_bytes);
        snapshot.queue_bytes = self.commands.pending_bytes();
        snapshot.data_level_queue_bytes = model.data_level_queue_bytes;
        snapshot.bytes_in_flight = 0;
        snapshot.data_level_bytes_in_flight = model.original_data_in_flight_bytes;
        snapshot.data_level_limit_bytes =
            reliable_product_feedback_window_bytes(Some(snapshot), lane, self.mux_limits) as u64;
        if let FixedRateDecision::Native(shape) = rate_decision {
            snapshot.jitter_ms = shape.rttvar().as_secs_f64() * 1000.0;
            snapshot.pacing_rate_bps = shape
                .pacing_rate_bps()
                .map_or(delivery_rate_bps, |rate| rate.max(1) as f64);
            snapshot.carrier_inflight_limit_bytes = shape
                .congestion_window()
                .max(u64::from(shape.current_mtu()));
            snapshot.bytes_in_flight = shape.bytes_in_flight();
            snapshot.app_limited = shape.app_limited();
            // Shared lineage loss is diagnostic-only and cannot be fused with
            // this activation-stamped Native scheduling bundle.
            snapshot.loss_rate = 0.0;
        }
        if let Some(epoch) = product_rate_epoch.filter(|_| {
            matches!(rate_decision, FixedRateDecision::Legacy) && product_rate_bps.is_some()
        }) {
            let learned_confidence = (f64::from(epoch.sample_count)
                / f64::from(RELIABLE_INITIAL_WINDOW_PACKETS as u32))
            .clamp(0.0, 1.0);
            snapshot.confidence = snapshot.confidence.max(learned_confidence);
        } else {
            snapshot.confidence = self.portable_startup.confidence;
        }
        snapshot.app_limited = model.carrier_work_in_flight_bytes == 0
            && model.data_level_queue_bytes == 0
            && self.commands.pending_bytes() == 0;
        snapshot
    }

    fn set_sender_queue_bytes(&self, bytes: usize) {
        let mut model = self.model.lock().expect("fixed reliable path model lock");
        model.data_level_queue_bytes = bytes as u64;
    }

    pub(in crate::runtime) fn record_original_flight(&self, frame: &Frame) {
        let _ = self.record_product_flight(frame, CarrierWorkKind::OriginalData);
    }

    #[cfg(test)]
    pub(in crate::runtime) fn record_reinjected_flight(&self, frame: &Frame) -> Option<Instant> {
        self.record_product_flight(frame, CarrierWorkKind::ReinjectedData)
    }

    pub(in crate::runtime) fn accepted_reinjected_data_in_flight_bytes_at(
        &self,
        identity: ServerReinjectionOutputIdentity,
    ) -> usize {
        if identity != self.reinjection_output_identity() {
            return 0;
        }
        let model = self.model.lock().expect("fixed reliable path model lock");
        Self::retained_reinjected_data_bytes_with_model(&model)
    }

    fn retained_reinjected_data_bytes_with_model(model: &FixedReliablePathModel) -> usize {
        model
            .flights
            .values()
            .flat_map(|flights| flights.iter())
            .filter_map(|flight| flight.reinjected_data_bytes())
            .fold(0usize, usize::saturating_add)
    }

    pub(in crate::runtime) fn try_enqueue_reinjected_frame(
        &self,
        frame: &Frame,
        lane: TrafficClass,
        queued_reinjection_bytes: usize,
        reinjection_debt_bytes: usize,
    ) -> Result<Instant, RuntimeError> {
        let Some((offset, end, bytes)) = reliable_stream_frame_extent(frame) else {
            return Err(RuntimeError::SenderServiceBlocked);
        };
        // The writer reservation is the exact native admission authority. K
        // is re-read only after this reservation so a successful Product
        // decision cannot race carrier capacity publication.
        let command = self
            .commands
            .try_reserve_reinjection_frame(frame.clone(), lane)?;
        let Some(rate_decision) = self.current_rate_decision() else {
            return Err(RuntimeError::SenderServiceBlocked);
        };
        self.commit_reserved_reinjected_frame(
            command,
            rate_decision,
            offset,
            end,
            bytes,
            lane,
            queued_reinjection_bytes,
            reinjection_debt_bytes,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn commit_reserved_reinjected_frame(
        &self,
        command: ReliablePathFrameReservation<'_>,
        rate_decision: FixedRateDecision,
        offset: u64,
        end: u64,
        bytes: usize,
        lane: TrafficClass,
        queued_reinjection_bytes: usize,
        reinjection_debt_bytes: usize,
    ) -> Result<Instant, RuntimeError> {
        self.commit_rate_decision(rate_decision, |current_rate_decision| {
            // Lock order is Quinn activation fence -> authority coordinator ->
            // current shape -> Product model. Nothing in this closure calls
            // back into Quinn.
            let mut model = self.model.lock().expect("fixed reliable path model lock");
            let accepted_at = Instant::now();
            let snapshot = self.send_path_snapshot_with_model_at(
                &model,
                lane,
                accepted_at,
                current_rate_decision,
            );
            let accepted_reinjection_bytes =
                Self::retained_reinjected_data_bytes_with_model(&model);
            let exact_service = reliable_reinjection_service_limit_bytes(
                ReliableReinjectionTargetWork::new(
                    Some(snapshot),
                    queued_reinjection_bytes,
                    accepted_reinjection_bytes,
                ),
                bytes.min(reinjection_debt_bytes),
                self.mux_limits,
            );
            if exact_service < bytes {
                // Dropping the uncommitted reservation returns writer
                // capacity; Product flight ownership has not changed.
                return Err(RuntimeError::SenderServiceBlocked);
            }
            let suppression_interval =
                reliable_data_retransmission_interval(Some(self.key.underlay), Some(snapshot));
            self.record_product_flight_with_model(
                &mut model,
                offset,
                end,
                bytes,
                accepted_at,
                CarrierWorkKind::ReinjectedData,
                Some(suppression_interval),
            );
            // Permit publication is synchronous and non-fallible. Product
            // ownership is therefore visible before the writer can dequeue.
            command.commit();
            Ok(accepted_at
                .checked_add(suppression_interval)
                .unwrap_or(accepted_at))
        })
    }

    pub(in crate::runtime) fn can_assign_original_data(&self, lane: TrafficClass) -> bool {
        self.try_send_path_snapshot_at(lane, Instant::now())
            .is_some_and(crate::model::admission::original_data_assignment_has_product_headroom)
    }

    pub(in crate::runtime) fn try_enqueue_original_data_frame(
        &self,
        frame: &Frame,
        lane: TrafficClass,
    ) -> Result<(), RuntimeError> {
        let Some((offset, end, bytes)) = reliable_stream_frame_extent(frame) else {
            return Err(RuntimeError::SenderServiceBlocked);
        };
        // The real writer permit is reserved before any Product ownership.
        // Every following rejection drops it and refunds pending-byte charge.
        let command = self
            .commands
            .try_reserve_admitted_frame(frame.clone(), lane)?;
        let Some(rate_decision) = self.current_rate_decision() else {
            return Err(RuntimeError::SenderServiceBlocked);
        };
        self.commit_reserved_original_data_frame(command, rate_decision, offset, end, bytes, lane)
    }

    fn commit_reserved_original_data_frame(
        &self,
        command: ReliablePathFrameReservation<'_>,
        rate_decision: FixedRateDecision,
        offset: u64,
        end: u64,
        bytes: usize,
        lane: TrafficClass,
    ) -> Result<(), RuntimeError> {
        self.commit_rate_decision(rate_decision, |current_rate_decision| {
            // Lock order is Quinn activation fence -> authority coordinator ->
            // current shape -> Product model. Nothing in this closure calls
            // back into Quinn.
            let mut model = self.model.lock().expect("fixed reliable path model lock");
            let accepted_at = Instant::now();
            let snapshot = self.send_path_snapshot_with_model_at(
                &model,
                lane,
                accepted_at,
                current_rate_decision,
            );
            if !crate::model::admission::original_data_assignment_has_product_headroom(snapshot) {
                return Err(RuntimeError::SenderServiceBlocked);
            }
            self.record_product_flight_with_model(
                &mut model,
                offset,
                end,
                bytes,
                accepted_at,
                CarrierWorkKind::OriginalData,
                None,
            );
            // Permit publication is synchronous and non-fallible. Product
            // ownership is therefore visible before the writer can dequeue.
            command.commit();
            Ok(())
        })
    }

    fn record_product_flight(&self, frame: &Frame, kind: CarrierWorkKind) -> Option<Instant> {
        let (offset, end, bytes) = reliable_stream_frame_extent(frame)?;
        let accepted_at = Instant::now();
        let suppression_interval = (kind == CarrierWorkKind::ReinjectedData).then(|| {
            reliable_data_retransmission_interval(
                Some(self.key.underlay),
                self.try_send_path_snapshot(),
            )
        });
        let mut model = self.model.lock().expect("fixed reliable path model lock");
        self.record_product_flight_with_model(
            &mut model,
            offset,
            end,
            bytes,
            accepted_at,
            kind,
            suppression_interval,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn record_product_flight_with_model(
        &self,
        model: &mut FixedReliablePathModel,
        offset: u64,
        end: u64,
        bytes: usize,
        accepted_at: Instant,
        kind: CarrierWorkKind,
        suppression_interval: Option<Duration>,
    ) -> Option<Instant> {
        model.carrier_work_in_flight_bytes = model
            .carrier_work_in_flight_bytes
            .saturating_add(bytes as u64);
        if kind == CarrierWorkKind::OriginalData {
            model.original_data_in_flight_bytes = model
                .original_data_in_flight_bytes
                .saturating_add(bytes as u64);
        }
        let accepted_copy_deadline = suppression_interval
            .and_then(|interval| accepted_at.checked_add(interval))
            .or(suppression_interval.map(|_| accepted_at));
        model
            .flights
            .entry(offset)
            .or_default()
            .push(CarrierPathFlight::fixed_output(
                self.key,
                end,
                bytes,
                accepted_at,
                kind,
                suppression_interval,
            ));
        accepted_copy_deadline
    }

    fn release_normalized_acked_ranges(&self, ranges: &[OffsetRange]) {
        self.release_normalized_acked_ranges_at(ranges, Instant::now());
    }

    fn release_normalized_acked_ranges_at(&self, ranges: &[OffsetRange], now: Instant) {
        if ranges.is_empty() {
            return;
        }
        let mut model = self.model.lock().expect("fixed reliable path model lock");
        let released = release_carrier_path_flight_ranges(&mut model.flights, ranges);
        if released.is_empty() {
            return;
        }
        let mut sample_bytes = 0_u64;
        let mut sample_start = now;
        let mut released_proven_flights = 0_u32;
        for (_, release) in released {
            let (bytes, sent_at, kind, path_proving) = release.fixed_output_sample();
            model.carrier_work_in_flight_bytes = model
                .carrier_work_in_flight_bytes
                .saturating_sub(bytes as u64);
            if kind.is_original_transmission() {
                // Product debt is ownership, not evidence. An overlapping
                // repair makes the ACK ambiguous for rate attribution, but it
                // still DataACKs and releases the unique OriginalData range.
                model.original_data_in_flight_bytes = model
                    .original_data_in_flight_bytes
                    .saturating_sub(bytes as u64);
            }
            if path_proving {
                sample_bytes = sample_bytes.saturating_add(bytes as u64);
                sample_start = sample_start.min(sent_at);
                released_proven_flights = released_proven_flights.saturating_add(1);
            }
        }
        model.product_progress_bytes = model.product_progress_bytes.saturating_add(sample_bytes);
        if let Some(sample) =
            PathRateSample::new(sample_bytes, now.saturating_duration_since(sample_start))
        {
            let sample_bps = sample.rate_bps();
            let fresh_prior_epoch = model
                .product_rate_epoch
                .filter(|epoch| epoch.fresh_rate_at(now).is_some());
            let rate_bps = match fresh_prior_epoch {
                Some(previous) => previous.rate_bps.mul_add(0.75, sample_bps * 0.25),
                None => sample_bps,
            };
            let sample_rtt_ms = now.saturating_duration_since(sample_start).as_secs_f64() * 1000.0;
            model.srtt_ms = Some(match fresh_prior_epoch.and(model.srtt_ms) {
                Some(previous) => previous.mul_add(0.875, sample_rtt_ms * 0.125),
                None => sample_rtt_ms,
            });
            let next_delivery_samples = fresh_prior_epoch
                .map_or(released_proven_flights, |epoch| {
                    epoch.sample_count.saturating_add(released_proven_flights)
                });
            let next_sample_bytes = fresh_prior_epoch.map_or(sample_bytes, |epoch| {
                epoch.sample_bytes.saturating_add(sample_bytes)
            });
            model.product_rate_epoch = FixedProductRateEpoch::new(
                rate_bps,
                next_delivery_samples,
                next_sample_bytes,
                now,
                fixed_rate_freshness_horizon(
                    model.srtt_ms.unwrap_or(self.startup.srtt_ms),
                    self.startup.jitter_ms,
                ),
            );
        }
        model.delivery_samples = model
            .delivery_samples
            .saturating_add(released_proven_flights);
    }

    fn reinjection_suppression_deadline(&self, frame: &Frame) -> Option<Instant> {
        let (start, end, _) = reliable_stream_frame_extent(frame)?;
        let model = self.model.lock().expect("fixed reliable path model lock");
        product_flights_have_recent_reinjection_overlap(
            &model.flights,
            start,
            end,
            Instant::now(),
            |_, _| true,
        )
    }

    fn earliest_reinjection_suppression_deadline(&self) -> Option<Instant> {
        let model = self.model.lock().expect("fixed reliable path model lock");
        product_flights_have_recent_reinjection_overlap(
            &model.flights,
            0,
            u64::MAX,
            Instant::now(),
            |_, _| true,
        )
    }
}

impl ReliablePathStreamOutput {
    fn has_live_output(&self) -> bool {
        match self {
            Self::Fixed(fixed) => !fixed.commands().is_closed(),
            Self::Switchable(binding) => binding.has_live_output(),
        }
    }

    fn product_admission_active(&self) -> bool {
        match self {
            Self::Fixed(fixed) => fixed.commands().product_admission_active(),
            Self::Switchable(binding) => binding.has_product_output(),
        }
    }

    fn terminal_signal(&self) -> Option<ReliablePathCarrierTerminalSignal> {
        match self {
            Self::Fixed(fixed) => Some(fixed.commands().terminal_signal()),
            Self::Switchable(_) => None,
        }
    }

    fn retire_accepted_stream(&self, stream_id: StreamId) -> Result<(), RuntimeError> {
        match self {
            Self::Fixed(fixed) => fixed.commands().retire_accepted_stream(stream_id),
            Self::Switchable(_) => Err(RuntimeError::Protocol(
                "accepted remote stream unexpectedly has switchable output",
            )),
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn fixed(
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: ReliablePathCommandSender,
        mux_limits: MuxLimits,
    ) -> Self {
        Self::Fixed(FixedReliablePathOutput::new(
            underlay, path_id, commands, mux_limits,
        ))
    }

    #[cfg(test)]
    pub(in crate::runtime) fn fixed_with_snapshot(
        startup: PathSnapshot,
        commands: ReliablePathCommandSender,
        mux_limits: MuxLimits,
    ) -> Self {
        Self::Fixed(FixedReliablePathOutput::new_with_snapshot(
            startup, commands, mux_limits,
        ))
    }

    fn fixed_with_snapshot_and_path_instance(
        startup: PathSnapshot,
        portable_startup: PathSnapshot,
        path_instance_id: CarrierPathInstanceId,
        startup_native_window: Option<CarrierNativeWindowSample>,
        startup_metrics: Option<PathMetrics>,
        commands: ReliablePathCommandSender,
        mux_limits: MuxLimits,
    ) -> Self {
        let captured_at = Instant::now();
        let native_window_epoch = startup_native_window.or_else(|| {
            CarrierNativeWindowSample::new(
                startup.carrier_inflight_limit_bytes,
                captured_at,
                fixed_rate_freshness_horizon(startup.srtt_ms, startup.jitter_ms),
            )
        });
        let native_rate_epoch = startup_metrics.map_or_else(
            || FixedNativeRateEpoch::from_snapshot_at(startup, captured_at),
            |metrics| FixedNativeRateEpoch::from_path_metrics_at(startup, metrics, captured_at),
        );
        Self::Fixed(
            FixedReliablePathOutput::new_with_snapshot_and_path_instance(
                startup,
                portable_startup,
                path_instance_id,
                native_window_epoch,
                native_rate_epoch,
                commands,
                mux_limits,
            ),
        )
    }

    pub(in crate::runtime) fn can_enqueue_frame_now(
        &self,
        frame: &Frame,
        lane: TrafficClass,
    ) -> bool {
        match self {
            Self::Fixed(fixed) => fixed.commands().can_enqueue_frame_now(frame, lane),
            Self::Switchable(_) => true,
        }
    }

    pub(in crate::runtime) fn can_enqueue_reinjection_frame_now(&self, frame: &Frame) -> bool {
        match self {
            Self::Fixed(fixed) => fixed.commands().can_enqueue_reinjection_frame_now(frame),
            Self::Switchable(_) => true,
        }
    }

    fn enqueue_path_proof(&self) -> Result<Option<u64>, RuntimeError> {
        match self {
            Self::Fixed(fixed) => fixed.enqueue_path_proof().map(Some),
            Self::Switchable(_) => Ok(None),
        }
    }

    pub(in crate::runtime) fn can_enqueue_lane_now(&self, lane: TrafficClass) -> bool {
        match self {
            Self::Fixed(fixed) => fixed.commands().can_enqueue_lane_now(lane),
            Self::Switchable(_) => false,
        }
    }

    pub(in crate::runtime) fn capacity_notifies(&self) -> Vec<Arc<Notify>> {
        match self {
            Self::Fixed(fixed) if !fixed.commands().is_closed() => {
                fixed.commands().capacity_notifies()
            }
            Self::Fixed(_) => Vec::new(),
            Self::Switchable(binding) => binding.capacity_notifies(),
        }
    }

    pub(in crate::runtime) async fn send_stream_detach(&self, stream_id: StreamId) {
        if let Self::Fixed(fixed) = self {
            let _ = fixed
                .commands()
                .send_control(ReliablePathCommand::SendFrame(Frame::StreamDetach {
                    stream_id,
                }))
                .await;
        }
    }

    pub(in crate::runtime) async fn close_stream(&self, stream_id: StreamId) {
        match self {
            Self::Fixed(fixed) => {
                let _ = fixed
                    .commands()
                    .send_control(ReliablePathCommand::CloseStream(stream_id))
                    .await;
            }
            Self::Switchable(binding) => binding.close_stream(stream_id).await,
        }
    }

    pub(in crate::runtime) async fn reset_and_close_stream(
        &self,
        stream_id: StreamId,
        reason: ResetReason,
    ) {
        match self {
            Self::Fixed(fixed) => {
                let _ = fixed
                    .commands()
                    .send_control(ReliablePathCommand::ResetAndCloseStream { stream_id, reason })
                    .await;
            }
            Self::Switchable(binding) => {
                binding.reset_and_close_stream(stream_id, reason).await;
            }
        }
    }

    pub(in crate::runtime) fn retire_stream_with_reset(
        &self,
        stream_id: StreamId,
        reason: ResetReason,
    ) {
        match self {
            Self::Fixed(fixed) => {
                let _ = fixed.commands().reset_accepted_stream(stream_id, reason);
            }
            Self::Switchable(binding) => {
                binding.retire_stream_with_reset(stream_id, reason);
            }
        }
    }

    pub(in crate::runtime) async fn close_stream_ordered(
        &self,
        stream_id: StreamId,
        lane: TrafficClass,
    ) {
        match self {
            Self::Fixed(fixed) => {
                let _ = fixed
                    .commands()
                    .send_stream_ordered_close(stream_id, lane)
                    .await;
            }
            Self::Switchable(binding) => binding.close_stream_ordered(stream_id, lane).await,
        }
    }

    pub(in crate::runtime) async fn reset_and_close_stream_ordered(
        &self,
        stream_id: StreamId,
        reason: ResetReason,
        lane: TrafficClass,
    ) {
        match self {
            Self::Fixed(fixed) => {
                let _ = fixed
                    .commands()
                    .send_stream_ordered_reset_and_close(stream_id, reason, lane)
                    .await;
            }
            Self::Switchable(binding) => {
                binding
                    .reset_and_close_stream_ordered(stream_id, reason, lane)
                    .await;
            }
        }
    }

    pub(in crate::runtime) fn current_lane(&self, fallback: TrafficClass) -> TrafficClass {
        match self {
            Self::Fixed(_) => fallback,
            Self::Switchable(binding) => binding.lane(),
        }
    }

    pub(in crate::runtime) fn send_path_snapshot(
        &self,
        lane: TrafficClass,
        payload_bytes: usize,
    ) -> Option<PathSnapshot> {
        match self {
            Self::Fixed(fixed) => {
                let _ = payload_bytes;
                fixed.try_send_path_snapshot_at(lane, Instant::now())
            }
            Self::Switchable(binding) => binding.send_path_snapshot(lane, payload_bytes),
        }
    }

    pub(in crate::runtime) fn send_path_snapshot_and_source_window(
        &self,
        lane: TrafficClass,
        payload_bytes: usize,
    ) -> (Option<PathSnapshot>, usize) {
        match self {
            Self::Fixed(fixed) => {
                let snapshot = fixed.try_send_path_snapshot_at(lane, Instant::now());
                let outputs = snapshot.and_then(|snapshot| {
                    fixed.commands().product_admission_active().then_some(
                        ReliableOriginalDataOutput {
                            snapshot,
                            stale: false,
                        },
                    )
                });
                let admission = reliable_stream_source_admission(
                    outputs,
                    lane,
                    payload_bytes,
                    fixed.mux_limits,
                );
                (admission.selected_path, admission.window_bytes)
            }
            Self::Switchable(binding) => {
                binding.send_path_snapshot_and_source_window(lane, payload_bytes)
            }
        }
    }

    pub(in crate::runtime) fn tail_reinjection_snapshot(
        &self,
        ack_frontier: u64,
        lane: TrafficClass,
    ) -> Option<PathSnapshot> {
        match self {
            Self::Fixed(fixed) => {
                let _ = ack_frontier;
                fixed.try_send_path_snapshot_at(lane, Instant::now())
            }
            Self::Switchable(binding) => binding.tail_reinjection_snapshot(ack_frontier, lane),
        }
    }

    pub(in crate::runtime) fn tail_reinjection_original_underlay(
        &self,
        ack_frontier: u64,
    ) -> Option<UnderlayProtocol> {
        match self {
            Self::Fixed(fixed) => Some(fixed.key().underlay),
            Self::Switchable(binding) => binding.tail_reinjection_original_underlay(ack_frontier),
        }
    }

    pub(in crate::runtime) fn data_ack_recovery_candidate(
        &self,
        ack_frontier: u64,
    ) -> Option<ResponseDataAckRecoveryCandidate> {
        match self {
            Self::Fixed(_) => None,
            Self::Switchable(binding) => binding.data_ack_recovery_candidate(ack_frontier),
        }
    }

    pub(in crate::runtime) fn data_ack_recovery_candidates(
        &self,
        authoritative_horizon: u64,
        lane: TrafficClass,
    ) -> SmallVec<[ResponseDataAckRecoveryCandidate; 4]> {
        match self {
            Self::Fixed(_) => SmallVec::new(),
            Self::Switchable(binding) => {
                binding.data_ack_recovery_candidates(authoritative_horizon, lane)
            }
        }
    }

    pub(in crate::runtime) fn response_output_snapshot(
        &self,
        identity: ServerReinjectionOutputIdentity,
        lane: TrafficClass,
    ) -> Option<PathSnapshot> {
        match self {
            Self::Fixed(_) => None,
            Self::Switchable(binding) => binding.response_output_snapshot(identity, lane),
        }
    }

    pub(in crate::runtime) fn request_feedback_underlay(&self) -> Option<UnderlayProtocol> {
        match self {
            Self::Fixed(fixed) => Some(fixed.key().underlay),
            Self::Switchable(binding) => binding.request_feedback_underlay(),
        }
    }

    pub(in crate::runtime) fn request_feedback_path_snapshot(
        &self,
        lane: TrafficClass,
    ) -> Option<PathSnapshot> {
        match self {
            Self::Fixed(fixed) => fixed.try_send_path_snapshot_at(lane, Instant::now()),
            Self::Switchable(binding) => binding.request_feedback_path_snapshot(lane),
        }
    }

    pub(in crate::runtime) fn has_output_incarnation(
        &self,
        key: CarrierPathKey,
        incarnation: u64,
    ) -> bool {
        match self {
            Self::Fixed(_) => false,
            Self::Switchable(binding) => binding.has_output_incarnation(key, incarnation),
        }
    }

    fn complete_path_detach(
        &self,
        key: CarrierPathKey,
        path_instance_id: CarrierPathInstanceId,
        output_incarnation: u64,
    ) {
        if let Self::Switchable(binding) = self {
            binding.complete_path_detach(key, path_instance_id, output_incarnation);
        }
    }

    pub(in crate::runtime) fn set_sender_queue_bytes(&self, bytes: usize) {
        match self {
            Self::Fixed(fixed) => fixed.set_sender_queue_bytes(bytes),
            Self::Switchable(binding) => binding.set_sender_queue_bytes(bytes),
        }
    }

    pub(in crate::runtime) fn subscribe_updates(&self) -> Option<watch::Receiver<u64>> {
        match self {
            Self::Fixed(_) => None,
            Self::Switchable(binding) => Some(binding.subscribe_updates()),
        }
    }

    pub(in crate::runtime) fn set_lane(&self, lane: TrafficClass) {
        if let Self::Switchable(binding) = self {
            binding.set_lane(lane);
        }
    }

    pub(in crate::runtime) fn release_normalized_acked_ranges(
        &self,
        ranges: &[OffsetRange],
    ) -> ResponseDataAckRelease {
        match self {
            Self::Fixed(fixed) => {
                fixed.release_normalized_acked_ranges(ranges);
                ResponseDataAckRelease::default()
            }
            Self::Switchable(binding) => binding.release_normalized_acked_ranges(ranges),
        }
    }

    pub(in crate::runtime) fn has_recent_reinjection_overlap(&self, frame: &Frame) -> bool {
        self.reinjection_suppression_deadline(frame).is_some()
    }

    pub(in crate::runtime) fn reinjection_suppression_deadline(
        &self,
        frame: &Frame,
    ) -> Option<Instant> {
        match self {
            Self::Fixed(fixed) => fixed.reinjection_suppression_deadline(frame),
            Self::Switchable(binding) => binding.reinjection_suppression_deadline(frame),
        }
    }

    pub(in crate::runtime) fn earliest_reinjection_suppression_deadline(&self) -> Option<Instant> {
        match self {
            Self::Fixed(fixed) => fixed.earliest_reinjection_suppression_deadline(),
            Self::Switchable(binding) => binding.earliest_reinjection_suppression_deadline(),
        }
    }

    pub(in crate::runtime) fn has_multipath_reinjection_alternative(&self) -> bool {
        match self {
            Self::Fixed(_) => false,
            Self::Switchable(binding) => binding.has_multipath_reinjection_alternative(),
        }
    }

    pub(in crate::runtime) fn has_reinjection_path_for_frame(&self, frame: &Frame) -> bool {
        match self {
            Self::Fixed(_) => true,
            Self::Switchable(binding) => binding.has_reinjection_path_for_frame(frame),
        }
    }

    pub(in crate::runtime) fn has_tail_reinjection_output_for_frame(&self, frame: &Frame) -> bool {
        match self {
            Self::Fixed(_) => false,
            Self::Switchable(binding) => binding.has_tail_reinjection_output_for_frame(frame),
        }
    }

    pub(in crate::runtime) fn failed_original_recovery_state(&self) -> RangeRecoveryState {
        match self {
            Self::Fixed(_) => RangeRecoveryState::default(),
            Self::Switchable(binding) => binding.failed_original_recovery_state(),
        }
    }

    pub(in crate::runtime) fn has_nonstale_reinjection_alternative(
        &self,
        candidate: ServerReinjectionOutputIdentity,
        lane: TrafficClass,
    ) -> bool {
        match self {
            Self::Fixed(_) => false,
            Self::Switchable(binding) => {
                binding.has_nonstale_reinjection_alternative(candidate, lane)
            }
        }
    }

    pub(in crate::runtime) fn mark_response_output_stale(
        &self,
        identity: ServerReinjectionOutputIdentity,
        lane: TrafficClass,
    ) -> bool {
        match self {
            Self::Fixed(_) => false,
            Self::Switchable(binding) => binding.mark_output_stale(identity, lane),
        }
    }

    pub(in crate::runtime) fn stale_response_original_outputs(
        &self,
        lane: TrafficClass,
    ) -> Vec<ServerReinjectionOutputIdentity> {
        match self {
            Self::Fixed(_) => Vec::new(),
            Self::Switchable(binding) => binding.stale_original_outputs(lane),
        }
    }

    pub(in crate::runtime) fn stale_original_recovery_state(
        &self,
        identity: ServerReinjectionOutputIdentity,
        lane: TrafficClass,
    ) -> RangeRecoveryState {
        match self {
            Self::Fixed(_) => RangeRecoveryState::default(),
            Self::Switchable(binding) => binding.stale_original_recovery_state(identity, lane),
        }
    }

    pub(in crate::runtime) fn has_untracked_data_reinjection_path_for_frame(
        &self,
        frame: &Frame,
    ) -> bool {
        match self {
            Self::Fixed(_) => false,
            Self::Switchable(binding) => {
                binding.has_untracked_data_reinjection_path_for_frame(frame)
            }
        }
    }
}

pub(in crate::runtime) fn reliable_work_lane_to_carrier_lane(
    work_lane: ReliableWorkClass,
    relay_lane: TrafficClass,
) -> TrafficClass {
    match work_lane {
        ReliableWorkClass::Control => TrafficClass::Control,
        ReliableWorkClass::Data | ReliableWorkClass::Reinjection => relay_lane,
    }
}

pub(in crate::runtime) async fn wait_for_carrier_capacity_notifies(notifies: Vec<Arc<Notify>>) {
    if notifies.is_empty() {
        tokio::task::yield_now().await;
        return;
    }
    let waits = notifies
        .into_iter()
        .map(|notify| Box::pin(async move { notify.notified().await }))
        .collect::<Vec<_>>();
    let _ = futures::future::select_all(waits).await;
}

pub(in crate::runtime) type ArmedCarrierCapacityWait = Pin<Box<dyn Future<Output = ()> + Send>>;

/// One exact carrier queue edge armed before maintenance inspects capacity.
///
/// `Notify::notify_waiters` is not retained, so a target-local maintenance
/// pass must carry this already-enabled wait out to its actor when the exact
/// queue is full. The identity is diagnostic/ownership context only; it grants
/// no Product, queue, or carrier authority.
pub(in crate::runtime) struct TargetCarrierCapacityWait<T> {
    pub(in crate::runtime) target: T,
    wait: ArmedCarrierCapacityWait,
}

impl<T> TargetCarrierCapacityWait<T> {
    pub(in crate::runtime) fn arm(target: T, notify: Arc<Notify>) -> Self {
        Self::arm_all(target, vec![notify]).expect("one exact carrier capacity notification")
    }

    pub(in crate::runtime) fn arm_all(target: T, notifies: Vec<Arc<Notify>>) -> Option<Self> {
        let wait = arm_carrier_capacity_notifies(notifies)?;
        Some(Self { target, wait })
    }
}

/// Result of one finite, lowest-priority requalification pass.
///
/// Capacity blockage is maintenance-local. It never aliases the ordinary
/// sender retry state and therefore cannot suppress useful work on a sibling
/// writer.
pub(in crate::runtime) enum RequalificationAttempt<T> {
    Idle,
    Published {
        target: T,
        payload_bytes: usize,
    },
    CapacityBlocked {
        targets: Vec<TargetCarrierCapacityWait<T>>,
    },
}

impl<T> RequalificationAttempt<T> {
    pub(in crate::runtime) fn published_payload_bytes(&self) -> Option<usize> {
        match self {
            Self::Published {
                target,
                payload_bytes,
            } => {
                let _ = target;
                Some(*payload_bytes)
            }
            Self::Idle | Self::CapacityBlocked { .. } => None,
        }
    }

    pub(in crate::runtime) fn is_capacity_blocked(&self) -> bool {
        matches!(self, Self::CapacityBlocked { .. })
    }

    pub(in crate::runtime) fn into_capacity_wait(self) -> Option<ArmedCarrierCapacityWait>
    where
        T: Send + 'static,
    {
        let Self::CapacityBlocked { targets } = self else {
            return None;
        };
        let waits = targets
            .into_iter()
            .map(|target| {
                let _ = target.target;
                target.wait
            })
            .collect::<Vec<_>>();
        debug_assert!(!waits.is_empty());
        Some(Box::pin(async move {
            let _ = futures::future::select_all(waits).await;
        }))
    }
}

/// Arms capacity notifications before the caller retries a failed queue
/// reservation. `Notify::notify_waiters` is not retained for a future waiter,
/// so creating the waiter after retry would leave a lost-wake window.
pub(in crate::runtime) fn arm_carrier_capacity_notifies(
    notifies: Vec<Arc<Notify>>,
) -> Option<ArmedCarrierCapacityWait> {
    if notifies.is_empty() {
        return None;
    }
    let waits = notifies
        .into_iter()
        .map(|notify| {
            let mut wait = Box::pin(notify.notified_owned());
            wait.as_mut().enable();
            wait
        })
        .collect::<Vec<_>>();
    Some(Box::pin(async move {
        let _ = futures::future::select_all(waits).await;
    }))
}

#[cfg(test)]
#[path = "tests_handle.rs"]
mod tests;
