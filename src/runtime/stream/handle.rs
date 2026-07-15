use super::response::{
    CarrierPathFlight, ResponseStreamBinding, product_flights_have_recent_repair_overlap,
    release_carrier_path_flight_ranges,
};
use crate::model::capacity::{
    PATH_OPEN_SCORE_BYTES, PathRateSample, RELIABLE_INITIAL_WINDOW_PACKETS,
    bbr_inflight_target_bytes, product_delivery_samples_override_startup_prior,
    reliable_subflow_startup_sample_limit_bytes,
};
use crate::model::path::CarrierPathKey;
use crate::model::work::{CarrierWorkKind, ReliableWorkClass};
use crate::mux::MuxLimits;
#[cfg(test)]
use crate::protocol::PathId;
use crate::protocol::frame::reliable_stream_frame_extent;
use crate::protocol::{Frame, OffsetRange, ResetReason, StreamId, UnderlayProtocol};
use crate::runtime::RuntimeError;
use crate::runtime::path::commands::{
    QuicCapacityProbeCommand, ReliablePathCommand, ReliablePathCommandSender,
    RequestTcpCapacityProbeRequest, reliable_path_stream_ordered_queue_lane,
};
#[cfg(test)]
use crate::runtime::path::model::{default_path_rate_bps, default_path_srtt_ms};
use crate::runtime::path::proof::{
    enqueue_path_proof_frame, enqueue_stream_ordered_path_proof_frame,
};
use crate::runtime::path::{OpenedReliableCarrierStream, RequestTcpCapacityProbeLease};
use crate::scheduler::{FlowLane, PathRateScope, PathSnapshot};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{Notify, mpsc, watch};

/// Product reliable stream handle after an OPEN_STREAM has been accepted.
///
/// The handle owns the stream ID, flow lane, product frame receive queue, and
/// response output binding for this stream. The carrier is only the emission
/// target; product offsets and repair semantics stay above TCP/UDP engines.
pub(in crate::runtime) struct ReliablePathStream {
    pub(in crate::runtime) stream_id: StreamId,
    pub(in crate::runtime) max_offset: u64,
    pub(in crate::runtime) lane: FlowLane,
    pub(in crate::runtime) underlay: UnderlayProtocol,
    pub(in crate::runtime) max_frame_payload_bytes: usize,
    pub(in crate::runtime) output: ReliablePathStreamOutput,
    pub(in crate::runtime) frames: mpsc::Receiver<Result<Frame, RuntimeError>>,
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
            output: ReliablePathStreamOutput::fixed_with_snapshot(
                opened.startup,
                opened.commands,
                opened.mux_limits,
            ),
            frames: opened.frames,
        }
    }

    pub(in crate::runtime) fn into_handle_and_frames(
        self,
    ) -> (
        ReliablePathStreamHandle,
        mpsc::Receiver<Result<Frame, RuntimeError>>,
    ) {
        (
            ReliablePathStreamHandle {
                stream_id: self.stream_id,
                max_offset: self.max_offset,
                lane: self.lane,
                underlay: self.underlay,
                max_frame_payload_bytes: self.max_frame_payload_bytes,
                output: self.output,
            },
            self.frames,
        )
    }

    pub(in crate::runtime) async fn recv_frame(&mut self) -> Result<Frame, RuntimeError> {
        match self.frames.recv().await {
            Some(Ok(frame)) => Ok(frame),
            Some(Err(err)) => Err(err),
            None => Err(RuntimeError::ReliablePathSessionClosed),
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
                .try_enqueue_admitted_frame(frame, FlowLane::Control),
            ReliablePathStreamOutput::Switchable(_) => {
                Err(RuntimeError::Protocol("request relay path is not fixed"))
            }
        }
    }

    pub(in crate::runtime) fn current_lane(&self) -> FlowLane {
        self.output.current_lane(self.lane)
    }

    pub(in crate::runtime) fn send_path_snapshot(
        &self,
        lane: FlowLane,
        payload_bytes: usize,
    ) -> Option<PathSnapshot> {
        self.output.send_path_snapshot(lane, payload_bytes)
    }

    pub(in crate::runtime) fn tail_repair_snapshot(
        &self,
        ack_frontier: u64,
        lane: FlowLane,
        payload_bytes: usize,
    ) -> Option<PathSnapshot> {
        self.output
            .tail_repair_snapshot(ack_frontier, lane)
            .or_else(|| self.send_path_snapshot(lane, payload_bytes))
    }

    pub(in crate::runtime) fn tail_repair_owner_underlay(
        &self,
        ack_frontier: u64,
    ) -> Option<UnderlayProtocol> {
        self.output.tail_repair_owner_underlay(ack_frontier)
    }

    pub(in crate::runtime) fn request_active_underlay(&self) -> Option<UnderlayProtocol> {
        self.output.request_active_underlay()
    }

    pub(in crate::runtime) fn request_active_path_snapshot(
        &self,
        lane: FlowLane,
    ) -> Option<PathSnapshot> {
        self.output.request_active_path_snapshot(lane)
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

    pub(in crate::runtime) fn capacity_notifies(&self) -> Vec<Arc<Notify>> {
        self.output.capacity_notifies()
    }

    pub(in crate::runtime) fn response_service_handoff_drain_active(&self) -> bool {
        self.output.response_service_handoff_drain_active()
    }

    pub(in crate::runtime) fn set_lane(&mut self, lane: FlowLane) {
        self.lane = lane;
        self.output.set_lane(lane);
    }

    pub(in crate::runtime) fn release_normalized_acked_ranges(&self, ranges: &[OffsetRange]) {
        self.output.release_normalized_acked_ranges(ranges);
    }

    pub(in crate::runtime) fn has_recent_live_repair_flight_overlap(
        &self,
        frame: &Frame,
        retry_after: Duration,
    ) -> bool {
        self.output
            .has_recent_live_repair_flight_overlap(frame, retry_after)
    }

    pub(in crate::runtime) fn has_multipath_repair_alternative(&self) -> bool {
        self.output.has_multipath_repair_alternative()
    }

    pub(in crate::runtime) fn has_repair_output_for_frame(&self, frame: &Frame) -> bool {
        self.output.has_repair_output_for_frame(frame)
    }

    pub(in crate::runtime) fn has_live_owner_tail_repair_output_for_frame(
        &self,
        frame: &Frame,
    ) -> bool {
        self.output
            .has_live_owner_tail_repair_output_for_frame(frame)
    }

    pub(in crate::runtime) fn has_failed_owner_repair_output_for_frame(
        &self,
        frame: &Frame,
    ) -> bool {
        self.output.has_failed_owner_repair_output_for_frame(frame)
    }

    pub(in crate::runtime) fn has_unknown_owner_repair_output_for_frame(
        &self,
        frame: &Frame,
    ) -> bool {
        self.output.has_unknown_owner_repair_output_for_frame(frame)
    }

    pub(in crate::runtime) fn can_attempt_failed_owner_tail_repair(&self) -> bool {
        matches!(self.output, ReliablePathStreamOutput::Switchable(_))
    }

    pub(in crate::runtime) async fn send_detach(&self) {
        self.output.send_stream_detach(self.stream_id).await;
    }

    pub(in crate::runtime) async fn close(&self) {
        self.output.close_stream(self.stream_id).await;
    }

    pub(in crate::runtime) async fn close_ordered(&self, lane: FlowLane) {
        self.output.close_stream_ordered(self.stream_id, lane).await;
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
    pub(in crate::runtime) lane: FlowLane,
    pub(in crate::runtime) underlay: UnderlayProtocol,
    pub(in crate::runtime) max_frame_payload_bytes: usize,
    pub(in crate::runtime) output: ReliablePathStreamOutput,
}

impl ReliablePathStreamHandle {
    pub(in crate::runtime) async fn send_detach(&self) {
        self.output.send_stream_detach(self.stream_id).await;
    }

    pub(in crate::runtime) fn enqueue_path_proof(&self) -> Result<Option<u64>, RuntimeError> {
        self.output.enqueue_path_proof()
    }

    pub(in crate::runtime) fn enqueue_stream_ordered_path_proof(
        &self,
        lane: FlowLane,
    ) -> Result<Option<u64>, RuntimeError> {
        self.output.enqueue_stream_ordered_path_proof(lane)
    }

    pub(in crate::runtime) fn try_enqueue_request_quic_capacity_probe(
        &self,
        probe: QuicCapacityProbeCommand,
    ) -> Result<(), RuntimeError> {
        match &self.output {
            ReliablePathStreamOutput::Fixed(fixed) => {
                fixed.commands().try_enqueue_quic_capacity_probe(probe)
            }
            ReliablePathStreamOutput::Switchable(_) => Err(RuntimeError::Protocol(
                "request QUIC capacity probe requires a fixed client output",
            )),
        }
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

    pub(in crate::runtime) fn can_enqueue_frame_now(&self, frame: &Frame, lane: FlowLane) -> bool {
        self.output.can_enqueue_frame_now(frame, lane)
    }

    pub(in crate::runtime) fn can_enqueue_work_lane_now(
        &self,
        work_lane: ReliableWorkClass,
        relay_lane: FlowLane,
    ) -> bool {
        self.output
            .can_enqueue_lane_now(reliable_work_lane_to_carrier_lane(work_lane, relay_lane))
    }

    pub(in crate::runtime) fn capacity_notifies(&self) -> Vec<Arc<Notify>> {
        self.output.capacity_notifies()
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
    /// later response bytes or repair ranges over another joined carrier path.
    Switchable(Arc<ResponseStreamBinding>),
}

pub(in crate::runtime) struct FixedReliablePathOutput {
    key: CarrierPathKey,
    startup: PathSnapshot,
    commands: ReliablePathCommandSender,
    mux_limits: MuxLimits,
    model: Mutex<FixedReliablePathModel>,
}

#[derive(Default)]
struct FixedReliablePathModel {
    bytes_in_flight: u64,
    product_queue_bytes: u64,
    product_progress_bytes: u64,
    product_progress_rate_bps: Option<f64>,
    delivery_rate_bps: Option<f64>,
    srtt_ms: Option<f64>,
    delivery_samples: u32,
    flights: BTreeMap<u64, Vec<CarrierPathFlight>>,
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
            default_path_srtt_ms(underlay),
            default_path_rate_bps(underlay),
        );
        Self::new_with_snapshot(startup, commands, mux_limits)
    }

    pub(in crate::runtime) fn new_with_snapshot(
        startup: PathSnapshot,
        commands: ReliablePathCommandSender,
        mux_limits: MuxLimits,
    ) -> Arc<Self> {
        Arc::new(Self {
            key: CarrierPathKey {
                underlay: startup.underlay,
                path_id: startup.id,
            },
            startup,
            commands,
            mux_limits,
            model: Mutex::new(FixedReliablePathModel::default()),
        })
    }

    pub(in crate::runtime) fn key(&self) -> CarrierPathKey {
        self.key
    }

    pub(in crate::runtime) fn commands(&self) -> &ReliablePathCommandSender {
        &self.commands
    }

    fn enqueue_path_proof(&self) -> Result<u64, RuntimeError> {
        enqueue_path_proof_frame(&self.commands, self.key.path_id, self.mux_limits)
    }

    fn enqueue_stream_ordered_path_proof(&self, lane: FlowLane) -> Result<u64, RuntimeError> {
        enqueue_stream_ordered_path_proof_frame(
            &self.commands,
            self.key.path_id,
            self.mux_limits,
            lane,
        )
    }

    fn send_path_snapshot(&self) -> PathSnapshot {
        let model = self.model.lock().expect("fixed reliable path model lock");
        let prior_rate_bps = self.startup.delivery_rate_bps.max(1.0);
        let (delivery_rate_bps, rate_scope) = match (self.key.underlay, model.delivery_rate_bps) {
            (UnderlayProtocol::Tcp, Some(rate))
                if !product_delivery_samples_override_startup_prior(model.delivery_samples) =>
            {
                if rate >= prior_rate_bps {
                    (rate, PathRateScope::PerFlowGoodput)
                } else {
                    (prior_rate_bps, PathRateScope::PathCapacity)
                }
            }
            (_, Some(rate)) => (rate, PathRateScope::PerFlowGoodput),
            (_, None) => (prior_rate_bps, PathRateScope::PathCapacity),
        };
        let delivery_rate_bps = delivery_rate_bps.max(1.0);
        let srtt_ms = model.srtt_ms.unwrap_or(self.startup.srtt_ms);
        let mut snapshot = self.startup;
        snapshot.srtt_ms = srtt_ms;
        snapshot.delivery_rate_bps = delivery_rate_bps;
        snapshot.rate_scope = rate_scope;
        snapshot.product_progress_rate_bps = model.product_progress_rate_bps;
        snapshot.has_durable_product_progress = model.product_progress_bytes
            >= reliable_subflow_startup_sample_limit_bytes(self.mux_limits);
        snapshot.pacing_rate_bps = delivery_rate_bps
            .max(model.product_progress_rate_bps.unwrap_or(0.0))
            .max(1.0);
        snapshot.queue_bytes = self.commands.pending_bytes();
        snapshot.product_queue_bytes = model.product_queue_bytes;
        snapshot.bytes_in_flight = 0;
        snapshot.product_bytes_in_flight = model.bytes_in_flight;
        snapshot.inflight_limit_bytes = snapshot.inflight_limit_bytes.max(
            bbr_inflight_target_bytes(snapshot, FlowLane::Throughput, self.mux_limits)
                .ceil()
                .max(PATH_OPEN_SCORE_BYTES as f64) as u64,
        );
        let learned_confidence = (f64::from(model.delivery_samples)
            / f64::from(RELIABLE_INITIAL_WINDOW_PACKETS as u32))
        .clamp(0.0, 1.0);
        snapshot.confidence = snapshot.confidence.max(learned_confidence);
        snapshot.app_limited = model.bytes_in_flight == 0
            && model.product_queue_bytes == 0
            && self.commands.pending_bytes() == 0;
        snapshot
    }

    fn set_sender_queue_bytes(&self, bytes: usize) {
        let mut model = self.model.lock().expect("fixed reliable path model lock");
        model.product_queue_bytes = bytes as u64;
    }

    pub(in crate::runtime) fn record_owner_flight(&self, frame: &Frame) {
        self.record_product_flight(frame, CarrierWorkKind::OwnerData)
    }

    pub(in crate::runtime) fn record_repair_flight(&self, frame: &Frame) {
        self.record_product_flight(frame, CarrierWorkKind::RepairData)
    }

    fn record_product_flight(&self, frame: &Frame, kind: CarrierWorkKind) {
        debug_assert!(kind.carries_product_offsets());
        let Some((offset, end, bytes)) = reliable_stream_frame_extent(frame) else {
            return;
        };
        let mut model = self.model.lock().expect("fixed reliable path model lock");
        model.bytes_in_flight = model.bytes_in_flight.saturating_add(bytes as u64);
        model
            .flights
            .entry(offset)
            .or_default()
            .push(CarrierPathFlight::fixed_output(
                self.key,
                end,
                bytes,
                Instant::now(),
                kind,
            ));
    }

    fn release_normalized_acked_ranges(&self, ranges: &[OffsetRange]) {
        if ranges.is_empty() {
            return;
        }
        let mut model = self.model.lock().expect("fixed reliable path model lock");
        let released = release_carrier_path_flight_ranges(&mut model.flights, ranges);
        if released.is_empty() {
            return;
        }
        let now = Instant::now();
        let mut sample_bytes = 0_u64;
        let mut sample_start = now;
        let mut released_proven_flights = 0_u32;
        for (_, release) in released {
            let (bytes, sent_at, path_proving) = release.fixed_output_sample();
            model.bytes_in_flight = model.bytes_in_flight.saturating_sub(bytes as u64);
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
            model.product_progress_rate_bps = Some(match model.product_progress_rate_bps {
                Some(previous) => previous.mul_add(0.75, sample_bps * 0.25),
                None => sample_bps,
            });
            model.delivery_rate_bps = Some(match model.delivery_rate_bps {
                Some(previous) => previous.mul_add(0.75, sample_bps * 0.25),
                None => sample_bps,
            });
            let sample_rtt_ms = now.saturating_duration_since(sample_start).as_secs_f64() * 1000.0;
            model.srtt_ms = Some(match model.srtt_ms {
                Some(previous) => previous.mul_add(0.875, sample_rtt_ms * 0.125),
                None => sample_rtt_ms,
            });
        }
        model.delivery_samples = model
            .delivery_samples
            .saturating_add(released_proven_flights);
    }

    fn has_recent_repair_flight_overlap(&self, frame: &Frame, retry_after: Duration) -> bool {
        let Some((start, end, _)) = reliable_stream_frame_extent(frame) else {
            return false;
        };
        let model = self.model.lock().expect("fixed reliable path model lock");
        product_flights_have_recent_repair_overlap(
            &model.flights,
            start,
            end,
            Instant::now(),
            retry_after,
            |_| true,
        )
    }
}

impl ReliablePathStreamOutput {
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

    pub(in crate::runtime) fn fixed_with_snapshot(
        startup: PathSnapshot,
        commands: ReliablePathCommandSender,
        mux_limits: MuxLimits,
    ) -> Self {
        Self::Fixed(FixedReliablePathOutput::new_with_snapshot(
            startup, commands, mux_limits,
        ))
    }

    pub(in crate::runtime) fn can_enqueue_frame_now(&self, frame: &Frame, lane: FlowLane) -> bool {
        match self {
            Self::Fixed(fixed) => fixed.commands().can_enqueue_frame_now(frame, lane),
            Self::Switchable(_) => true,
        }
    }

    fn response_service_handoff_drain_active(&self) -> bool {
        match self {
            Self::Fixed(_) => false,
            Self::Switchable(binding) => binding.response_service_handoff_drain_active(),
        }
    }

    fn enqueue_path_proof(&self) -> Result<Option<u64>, RuntimeError> {
        match self {
            Self::Fixed(fixed) => fixed.enqueue_path_proof().map(Some),
            Self::Switchable(_) => Ok(None),
        }
    }

    fn enqueue_stream_ordered_path_proof(
        &self,
        lane: FlowLane,
    ) -> Result<Option<u64>, RuntimeError> {
        match self {
            Self::Fixed(fixed) => fixed.enqueue_stream_ordered_path_proof(lane).map(Some),
            Self::Switchable(_) => Ok(None),
        }
    }

    pub(in crate::runtime) fn can_enqueue_lane_now(&self, lane: FlowLane) -> bool {
        match self {
            Self::Fixed(fixed) => fixed.commands().can_enqueue_lane_now(lane),
            Self::Switchable(_) => false,
        }
    }

    pub(in crate::runtime) fn capacity_notifies(&self) -> Vec<Arc<Notify>> {
        match self {
            Self::Fixed(fixed) if !fixed.commands().is_closed() => {
                vec![fixed.commands().capacity_notify()]
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

    pub(in crate::runtime) async fn close_stream_ordered(
        &self,
        stream_id: StreamId,
        lane: FlowLane,
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
        lane: FlowLane,
    ) {
        match self {
            Self::Fixed(fixed) => {
                let _ = fixed
                    .commands()
                    .send_stream_ordered_frame(Frame::StreamReset { stream_id, reason }, lane)
                    .await;
                let _ = fixed
                    .commands()
                    .send_stream_ordered_close(stream_id, lane)
                    .await;
            }
            Self::Switchable(binding) => {
                binding
                    .reset_and_close_stream_ordered(stream_id, reason, lane)
                    .await;
            }
        }
    }

    pub(in crate::runtime) fn current_lane(&self, fallback: FlowLane) -> FlowLane {
        match self {
            Self::Fixed(_) => fallback,
            Self::Switchable(binding) => binding.lane(),
        }
    }

    pub(in crate::runtime) fn send_path_snapshot(
        &self,
        lane: FlowLane,
        payload_bytes: usize,
    ) -> Option<PathSnapshot> {
        match self {
            Self::Fixed(fixed) => {
                let _ = lane;
                let _ = payload_bytes;
                Some(fixed.send_path_snapshot())
            }
            Self::Switchable(binding) => binding.send_path_snapshot(lane, payload_bytes),
        }
    }

    pub(in crate::runtime) fn tail_repair_snapshot(
        &self,
        ack_frontier: u64,
        lane: FlowLane,
    ) -> Option<PathSnapshot> {
        match self {
            Self::Fixed(fixed) => {
                let _ = (ack_frontier, lane);
                Some(fixed.send_path_snapshot())
            }
            Self::Switchable(binding) => binding.tail_repair_snapshot(ack_frontier, lane),
        }
    }

    pub(in crate::runtime) fn tail_repair_owner_underlay(
        &self,
        ack_frontier: u64,
    ) -> Option<UnderlayProtocol> {
        match self {
            Self::Fixed(fixed) => Some(fixed.key().underlay),
            Self::Switchable(binding) => binding.tail_repair_owner_underlay(ack_frontier),
        }
    }

    pub(in crate::runtime) fn request_active_underlay(&self) -> Option<UnderlayProtocol> {
        match self {
            Self::Fixed(fixed) => Some(fixed.key().underlay),
            Self::Switchable(binding) => binding.request_active_underlay(),
        }
    }

    pub(in crate::runtime) fn request_active_path_snapshot(
        &self,
        lane: FlowLane,
    ) -> Option<PathSnapshot> {
        match self {
            Self::Fixed(fixed) => {
                let _ = lane;
                Some(fixed.send_path_snapshot())
            }
            Self::Switchable(binding) => binding.request_active_path_snapshot(lane),
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

    pub(in crate::runtime) fn set_lane(&self, lane: FlowLane) {
        if let Self::Switchable(binding) = self {
            binding.set_lane(lane);
        }
    }

    pub(in crate::runtime) fn release_normalized_acked_ranges(&self, ranges: &[OffsetRange]) {
        match self {
            Self::Fixed(fixed) => fixed.release_normalized_acked_ranges(ranges),
            Self::Switchable(binding) => binding.release_normalized_acked_ranges(ranges),
        }
    }

    pub(in crate::runtime) fn has_recent_live_repair_flight_overlap(
        &self,
        frame: &Frame,
        retry_after: Duration,
    ) -> bool {
        match self {
            Self::Fixed(fixed) => fixed.has_recent_repair_flight_overlap(frame, retry_after),
            Self::Switchable(binding) => {
                binding.has_recent_live_repair_flight_overlap(frame, retry_after)
            }
        }
    }

    pub(in crate::runtime) fn has_multipath_repair_alternative(&self) -> bool {
        match self {
            Self::Fixed(_) => false,
            Self::Switchable(binding) => binding.has_multipath_repair_alternative(),
        }
    }

    pub(in crate::runtime) fn has_repair_output_for_frame(&self, frame: &Frame) -> bool {
        match self {
            Self::Fixed(_) => true,
            Self::Switchable(binding) => binding.has_repair_output_for_frame(frame),
        }
    }

    pub(in crate::runtime) fn has_live_owner_tail_repair_output_for_frame(
        &self,
        frame: &Frame,
    ) -> bool {
        match self {
            Self::Fixed(_) => false,
            Self::Switchable(binding) => binding.has_live_owner_tail_repair_output_for_frame(frame),
        }
    }

    pub(in crate::runtime) fn has_failed_owner_repair_output_for_frame(
        &self,
        frame: &Frame,
    ) -> bool {
        match self {
            Self::Fixed(_) => false,
            Self::Switchable(binding) => binding.has_failed_owner_repair_output_for_frame(frame),
        }
    }

    pub(in crate::runtime) fn has_unknown_owner_repair_output_for_frame(
        &self,
        frame: &Frame,
    ) -> bool {
        match self {
            Self::Fixed(_) => false,
            Self::Switchable(binding) => binding.has_unknown_owner_repair_output_for_frame(frame),
        }
    }
}

pub(in crate::runtime) fn reliable_work_lane_to_carrier_lane(
    work_lane: ReliableWorkClass,
    relay_lane: FlowLane,
) -> FlowLane {
    match work_lane {
        ReliableWorkClass::Control => FlowLane::Control,
        ReliableWorkClass::Repair => reliable_path_stream_ordered_queue_lane(),
        ReliableWorkClass::Data => relay_lane,
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

#[cfg(test)]
#[path = "handle_test.rs"]
mod tests;
