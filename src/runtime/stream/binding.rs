use super::*;
#[cfg(test)]
use crate::model::ack_clock::reliable_ack_clock_calibration_limit_bytes;
use crate::model::ack_clock::{
    reliable_ack_clock_calibration_ceiling_bytes,
    reliable_ack_clock_calibration_rate_coverage_floor_bytes,
    reliable_tcp_ack_clock_calibration_initial_limit_bytes,
};
#[cfg(feature = "lab-diagnostics")]
use crate::model::admission::{
    bulk_active_service_product_envelope_bytes, bulk_latency_pressure_service_feed_window_bytes,
    bulk_service_feed_reservoir_payload_bytes, bulk_service_horizon_payload_bytes,
};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};

// Reliable-path bindings own attachment instances, exact range flights,
// evidence, and atomic commit. Sender services rank immutable snapshots.

static NEXT_SERVER_CARRIER_PATH_INSTANCE_ID: AtomicU64 = AtomicU64::new(1);
pub(in crate::runtime) const MAX_RESPONSE_QUIC_CAPACITY_CALIBRATION_ATTEMPTS_PER_PATH: u8 = 2;
// One sustained response stream is real demand. Same-family discovery stays
// single-owner and bounded; requiring a second stream prevents large one-flow
// transfers from ever measuring spare paths.
pub(in crate::runtime) const MIN_ACTIVE_RESPONSE_FLOWS_FOR_SAME_FAMILY_DISCOVERY: u32 = 1;
static NEXT_RESPONSE_STREAM_BINDING_INSTANCE_ID: AtomicU64 = AtomicU64::new(1);
const RESPONSE_OWNER_TCP_SEEN: u8 = 1 << 0;
const RESPONSE_OWNER_UDP_SEEN: u8 = 1 << 1;
const RESPONSE_OWNER_MIXED_SEEN: u8 = RESPONSE_OWNER_TCP_SEEN | RESPONSE_OWNER_UDP_SEEN;

fn response_owner_underlay_seen_bit(underlay: UnderlayProtocol) -> u8 {
    match underlay {
        UnderlayProtocol::Tcp => RESPONSE_OWNER_TCP_SEEN,
        UnderlayProtocol::Udp => RESPONSE_OWNER_UDP_SEEN,
    }
}

mod quic_capacity_probe;
mod response_admission;
mod response_service_handoff;
pub(super) mod response_session;

pub(in crate::runtime) use crate::runtime::path::quic::metrics::QuicCapacityProofCandidate;
pub(in crate::runtime) use quic_capacity_probe::ResponseQuicCapacityCalibrationRequest;
pub(in crate::runtime) use response_admission::*;
pub(in crate::runtime) use response_service_handoff::{
    ResponseServiceHandoffDrainRequest, ResponseServiceHandoffRequest,
};
#[cfg(test)]
use response_session::ServerQuicCapacityCalibrationPhase;
use response_session::ServerResponseFlowRegistration;
pub(in crate::runtime) use response_session::{
    ResponseServiceFamilyLoads, ResponseServiceHandoffDrainReservation,
    ResponseSessionSchedulingSnapshot, ServerPathLaneTracker, ServerRealtimeFlowRegistration,
    TcpCapacityProbeSessionLease, quic_capacity_proof_pin_matches_marker,
    quic_capacity_receipt_rate_bps, valid_quic_capacity_proof_candidate_at,
    well_formed_quic_capacity_proof_candidate,
};

// Ownership boundary:
// This module owns carrier-neutral reliable stream bindings on the response
// side. It tracks which carrier path carried each product byte range, records
// ordering debt and stream-ACK release. It must not choose among joined carrier
// paths for response frames; dispatch belongs to the sender service. It must
// not implement TCP framing, QUIC packet recovery, or target socket I/O; those
// belong to carrier and outbound modules.

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
            .push(CarrierPathFlight {
                key: self.key,
                output_incarnation: 0,
                end,
                bytes,
                sent_at: Instant::now(),
                kind,
                evidence_eligible: true,
            });
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
            let flight = release.flight;
            model.bytes_in_flight = model.bytes_in_flight.saturating_sub(flight.bytes as u64);
            if release.path_proving {
                sample_bytes = sample_bytes.saturating_add(flight.bytes as u64);
                sample_start = sample_start.min(flight.sent_at);
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
            Self::Fixed(fixed) => vec![fixed.commands().capacity_notify()],
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

/// Server-side response owner for one product reliable stream.
///
/// This binding owns the stream's attached carrier outputs, product byte flight
/// ledger, stream-ACK ordering state, lane tracking, and path-metric hints used
/// for response scheduling. It does not own the target socket and does not own
/// TCP/QUIC packet recovery.
#[derive(Default)]
struct ResponseSubflowSetState {
    planner_generation: u64,
    epoch_generation: u64,
    set: Option<FlowSubflowSet>,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ResponseSubflowAdmissionReservation {
    pub(in crate::runtime) admission: PathAdmission,
    pub(in crate::runtime) epoch_generation: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ResponseSubflowAdmissionRequest {
    pub(in crate::runtime) expected_planner_generation: u64,
    pub(in crate::runtime) expected_lane_generation: u64,
    pub(in crate::runtime) service: CarrierPathKey,
    pub(in crate::runtime) startup_owner_credit_bytes: usize,
    pub(in crate::runtime) optional_overhead_budget_bytes: usize,
    pub(in crate::runtime) max_read_gap_budget: Duration,
    pub(in crate::runtime) input: SubflowAdmissionInput,
}

#[derive(Debug, Clone, Copy)]
/// Optimistic calibration reservation. Generations fence product/path model
/// changes; pending values fence the exact queue-pressure projection.
pub(in crate::runtime) struct ResponseAckClockCalibrationRequest {
    pub(in crate::runtime) expected_planner_generation: u64,
    pub(in crate::runtime) expected_lane_generation: u64,
    pub(in crate::runtime) expected_model_generation: u64,
    pub(in crate::runtime) service: CarrierPathKey,
    pub(in crate::runtime) service_incarnation: u64,
    pub(in crate::runtime) service_pending_bytes: u64,
    pub(in crate::runtime) target_pending_bytes: u64,
    pub(in crate::runtime) limit_bytes: u64,
    /// Fresh work requires active response demand; exact begun work may finish.
    pub(in crate::runtime) requires_active_response_start: bool,
}

#[derive(Debug, Clone, Copy)]
/// Zero-spend retirement uses the same coherent planner/model snapshot as Admit.
pub(in crate::runtime) struct ResponseAckClockCalibrationRetirementRequest {
    pub(in crate::runtime) expected_planner_generation: u64,
    pub(in crate::runtime) expected_lane_generation: u64,
    pub(in crate::runtime) expected_model_generation: u64,
    pub(in crate::runtime) service: CarrierPathKey,
    pub(in crate::runtime) service_incarnation: u64,
    pub(in crate::runtime) service_pending_bytes: u64,
    pub(in crate::runtime) target: CarrierPathKey,
    pub(in crate::runtime) target_incarnation: u64,
    pub(in crate::runtime) target_pending_bytes: u64,
    pub(in crate::runtime) limit_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ResponseSourceServiceSnapshot {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::runtime) key: CarrierPathKey,
    pub(in crate::runtime) active_latency_sensitive_flows: u32,
    pub(in crate::runtime) has_service_feed_evidence: bool,
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::runtime) has_bulk_rate_evidence: bool,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ResponseRelayReadSnapshot {
    pub(in crate::runtime) send_path: Option<PathSnapshot>,
    pub(in crate::runtime) source_service: Option<ResponseSourceServiceSnapshot>,
    pub(in crate::runtime) independent_source_staging: bool,
}

#[cfg(feature = "lab-diagnostics")]
#[derive(Clone, Copy)]
struct ResponseServiceHandoffDiagnosticState {
    model_generation: u64,
    evaluation_signature: u64,
    capacity_marker_signature: u64,
    emitted_at: Instant,
}

#[cfg(feature = "lab-diagnostics")]
#[derive(Clone, Copy, PartialEq, Eq)]
struct ResponseServiceFeedDiagnosticState {
    path_instance_id: ServerCarrierPathInstanceId,
    attachment_role: StreamOpenRole,
    is_active: bool,
    has_bulk_rate_evidence: bool,
    has_service_feed_evidence: bool,
    owner_progress_bucket: u8,
    product_rate_available: bool,
    carrier_sample_bucket: u8,
    carrier_sample_available: bool,
    carrier_app_limited: bool,
    latency_pressure: bool,
    source_limit_bytes: u64,
    emission_limit_bytes: u64,
}

pub(in crate::runtime) struct ResponseStreamBinding {
    session_id: SessionId,
    binding_instance_id: u64,
    lane: Mutex<FlowLane>,
    mux_limits: MuxLimits,
    lane_tracker: Arc<ServerPathLaneTracker>,
    response_flow_registration: ServerResponseFlowRegistration,
    next_output_incarnation: AtomicU64,
    // Publishes coherent path evidence, exact flights, ACK ordering, and queue
    // inputs so calibration cannot commit a mixture of old and new views.
    response_model_generation: AtomicU64,
    owner_underlay_history: AtomicU8,
    // Close publishes before carrier commands so no later scheduler commit can
    // resurrect response Service ownership after stream retirement begins.
    response_stream_open: AtomicBool,
    // A successful carrier-family Service handoff is sticky. Reopening this
    // decision would turn whole-flow placement into per-epoch path hopping.
    response_service_handoff_open: AtomicBool,
    // A failed bounded drain is not retried for this flow; repeated pauses
    // would convert an optional placement optimization into periodic stalls.
    response_service_handoff_drain_attempted: AtomicBool,
    // Lab evaluation is transition/interval scoped so a hot sender loop does
    // not turn one failed placement gate into one event per product frame.
    #[cfg(feature = "lab-diagnostics")]
    response_service_handoff_diagnostic: Mutex<Option<ResponseServiceHandoffDiagnosticState>>,
    #[cfg(feature = "lab-diagnostics")]
    response_service_feed_diagnostic:
        Mutex<HashMap<(CarrierPathKey, u64), ResponseServiceFeedDiagnosticState>>,
    outputs: Mutex<ResponseStreamOutputs>,
    request_active_owner: Mutex<Option<CarrierPathKey>>,
    // Historical name: this is the persistent response Service anchor, not
    // exclusive ownership of every range. `flights` owns exact byte identity.
    ordered_data_owner: Mutex<Option<CarrierPathKey>>,
    flights: Mutex<BTreeMap<u64, Vec<CarrierPathFlight>>>,
    ack_ordering: Mutex<ResponseAckOrderingState>,
    subflow_set: Mutex<ResponseSubflowSetState>,
    version: watch::Sender<u64>,
}

impl Drop for ResponseStreamBinding {
    fn drop(&mut self) {
        self.response_flow_registration.set_active(false);
        self.lane_tracker
            .clear_response_service_handoff_drain_for_binding(
                self.session_id,
                self.binding_instance_id,
            );
        let lane = *self
            .lane
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let outputs = self
            .outputs
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for entry in outputs.entries.drain(..) {
            if response_stream_role_reserves_flow_load(entry.role) {
                self.lane_tracker.detach(self.session_id, entry.key, lane);
            }
            self.lane_tracker.clear_quic_capacity_calibration(
                self.session_id,
                self.binding_instance_id,
                entry.key,
                entry.path_instance_id,
            );
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum ResponseStreamAttachOutcome {
    Attached,
    RoleChanged,
    ReplacedClosedOutput,
    RejectedDuplicateLiveOutput,
}

impl ResponseStreamBinding {
    #[cfg(test)]
    pub(in crate::runtime) fn new(
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: ReliablePathCommandSender,
        lane: FlowLane,
    ) -> Arc<Self> {
        Self::new_with_limits(
            session_id,
            underlay,
            path_id,
            commands,
            lane,
            MuxLimits::default(),
        )
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::runtime) fn new_with_limits(
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: ReliablePathCommandSender,
        lane: FlowLane,
        mux_limits: MuxLimits,
    ) -> Arc<Self> {
        Self::new_with_limits_and_tracker(
            session_id,
            underlay,
            path_id,
            commands,
            lane,
            mux_limits,
            Arc::new(ServerPathLaneTracker::default()),
        )
    }

    pub(in crate::runtime) fn new_with_limits_and_tracker(
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: ReliablePathCommandSender,
        lane: FlowLane,
        mux_limits: MuxLimits,
        lane_tracker: Arc<ServerPathLaneTracker>,
    ) -> Arc<Self> {
        Self::new_with_limits_tracker_and_path_instance(
            session_id,
            underlay,
            path_id,
            commands,
            lane,
            mux_limits,
            lane_tracker,
            next_server_carrier_path_instance_id(),
        )
    }

    pub(super) fn new_with_limits_tracker_and_path_instance(
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: ReliablePathCommandSender,
        lane: FlowLane,
        mux_limits: MuxLimits,
        lane_tracker: Arc<ServerPathLaneTracker>,
        path_instance_id: ServerCarrierPathInstanceId,
    ) -> Arc<Self> {
        let (version, _) = watch::channel(0);
        let key = CarrierPathKey { underlay, path_id };
        let response_flow_registration =
            ServerResponseFlowRegistration::new(lane_tracker.clone(), session_id, key, lane);
        lane_tracker.attach(session_id, key, lane);
        response_flow_registration.set_active(true);
        Arc::new(Self {
            session_id,
            binding_instance_id: NEXT_RESPONSE_STREAM_BINDING_INSTANCE_ID
                .fetch_add(1, Ordering::AcqRel),
            lane: Mutex::new(lane),
            mux_limits,
            lane_tracker,
            response_flow_registration,
            next_output_incarnation: AtomicU64::new(2),
            response_model_generation: AtomicU64::new(0),
            owner_underlay_history: AtomicU8::new(response_owner_underlay_seen_bit(underlay)),
            response_stream_open: AtomicBool::new(true),
            response_service_handoff_open: AtomicBool::new(true),
            response_service_handoff_drain_attempted: AtomicBool::new(false),
            #[cfg(feature = "lab-diagnostics")]
            response_service_handoff_diagnostic: Mutex::new(None),
            #[cfg(feature = "lab-diagnostics")]
            response_service_feed_diagnostic: Mutex::new(HashMap::new()),
            outputs: Mutex::new(ResponseStreamOutputs {
                entries: vec![ResponseStreamOutputEntry {
                    key,
                    path_instance_id,
                    incarnation: 1,
                    commands,
                    role: StreamOpenRole::Active,
                    owner_data_in_flight_bytes: 0,
                    bytes_in_flight: 0,
                    product_queue_bytes: 0,
                    product_progress_rate_bps: None,
                    delivery_rate_bps: None,
                    tcp_ack_clock_rate_bps: None,
                    tcp_product_rate_evidence: None,
                    tcp_capacity_prior: None,
                    srtt_ms: None,
                    delivery_samples: 0,
                    owner_data_acked_bytes: 0,
                    local_path_metrics: None,
                    peer_path_metrics: None,
                }],
                ack_clock_calibrations: HashMap::new(),
                active_ack_clock_calibration: None,
            }),
            request_active_owner: Mutex::new(Some(key)),
            ordered_data_owner: Mutex::new(Some(key)),
            flights: Mutex::new(BTreeMap::new()),
            ack_ordering: Mutex::new(ResponseAckOrderingState::default()),
            subflow_set: Mutex::new(ResponseSubflowSetState::default()),
            version,
        })
    }

    fn allocate_output_incarnation(&self) -> u64 {
        self.next_output_incarnation.fetch_add(1, Ordering::AcqRel)
    }

    fn response_flow_is_active(outputs: &ResponseStreamOutputs) -> bool {
        outputs
            .entries
            .iter()
            .any(|entry| response_stream_role_reserves_flow_load(entry.role))
    }

    fn sync_response_flow_activity(&self, outputs: &ResponseStreamOutputs) {
        // Deactivation calls this before path-load removal; activation calls it
        // after path-load registration so every visible generation is conservative.
        self.response_flow_registration
            .set_active(Self::response_flow_is_active(outputs));
    }

    #[cfg(test)]
    pub(in crate::runtime) fn attach(
        &self,
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: ReliablePathCommandSender,
        lane: FlowLane,
        role: StreamOpenRole,
        max_frame_payload_bytes: usize,
    ) -> ResponseStreamAttachOutcome {
        let key = CarrierPathKey { underlay, path_id };
        let path_instance_id = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock")
            .entries
            .iter()
            .find(|entry| entry.key == key && entry.commands.same_channel(&commands))
            .map_or_else(next_server_carrier_path_instance_id, |entry| {
                entry.path_instance_id
            });
        self.attach_with_path_instance(
            underlay,
            path_id,
            path_instance_id,
            commands,
            lane,
            role,
            max_frame_payload_bytes,
        )
    }

    pub(super) fn attach_with_path_instance(
        &self,
        underlay: UnderlayProtocol,
        path_id: PathId,
        path_instance_id: ServerCarrierPathInstanceId,
        commands: ReliablePathCommandSender,
        lane: FlowLane,
        role: StreamOpenRole,
        _max_frame_payload_bytes: usize,
    ) -> ResponseStreamAttachOutcome {
        let mut current_lane = self.lane.lock().expect("server reliable stream lane lock");
        let previous_lane = *current_lane;
        let proof_commands = commands.clone();
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let response_flow_was_active = Self::response_flow_is_active(&outputs);
        let key = CarrierPathKey { underlay, path_id };
        let mut was_active = false;
        let mut previous_load_registered = false;
        let mut replaced_closed = false;
        let mut replaced_incarnation = None;
        let mut replaced_path_instance_id = None;
        let existing_position = outputs.entries.iter().position(|entry| entry.key == key);
        if let Some(position) = existing_position {
            let entry = &mut outputs.entries[position];
            if !entry.commands.is_closed() {
                let same_channel = entry.commands.same_channel(&commands);
                #[cfg(feature = "lab-diagnostics")]
                let attach_result = match same_channel {
                    true => "same_channel_role_update",
                    false => "duplicate_live",
                };
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "server_stream_output_attach",
                    format_args!(
                        "session_id={} path_underlay={:?} path_id={} role={:?} lane={:?} result={} same_channel={}",
                        self.session_id.0,
                        underlay,
                        path_id.0,
                        role,
                        lane,
                        attach_result,
                        same_channel,
                    ),
                );
                if same_channel {
                    let previous_role = entry.role;
                    let previous_load_registered =
                        response_stream_role_reserves_flow_load(previous_role);
                    entry.role = response_stream_live_role_update(entry.role, role);
                    let role_changed = entry.role != previous_role;
                    let response_state_invalidated =
                        response_stream_role_change_invalidates_response_state(
                            previous_role,
                            entry.role,
                        );
                    let response_state_incarnations = response_state_invalidated.then(|| {
                        let previous = entry.incarnation;
                        let current = self.allocate_output_incarnation();
                        (previous, current)
                    });
                    if response_state_invalidated {
                        entry.incarnation = response_state_incarnations
                            .expect("response eligibility change allocates an incarnation")
                            .1;
                        entry.product_progress_rate_bps = None;
                        entry.delivery_rate_bps = None;
                        entry.tcp_ack_clock_rate_bps = None;
                        entry.tcp_product_rate_evidence = None;
                        entry.tcp_capacity_prior = None;
                        entry.srtt_ms = None;
                        entry.delivery_samples = 0;
                        entry.owner_data_acked_bytes = 0;
                        entry.local_path_metrics = None;
                        entry.peer_path_metrics = None;
                    }
                    let updated_role = entry.role;
                    let updated_load_registered =
                        response_stream_role_reserves_flow_load(updated_role);
                    let lane_registered_keys = outputs
                        .entries
                        .iter()
                        .filter(|entry| {
                            response_stream_role_reserves_flow_load(entry.role)
                                && (entry.key != key || previous_load_registered)
                        })
                        .map(|entry| entry.key)
                        .collect::<Vec<_>>();
                    let response_flow_is_active = Self::response_flow_is_active(&outputs);
                    if response_flow_was_active && !response_flow_is_active {
                        self.sync_response_flow_activity(&outputs);
                    }
                    if let Some((previous_incarnation, current_incarnation)) =
                        response_state_incarnations
                    {
                        outputs
                            .ack_clock_calibrations
                            .remove(&(key, previous_incarnation));
                        if outputs.active_ack_clock_calibration == Some((key, previous_incarnation))
                        {
                            outputs.active_ack_clock_calibration = None;
                        }
                        self.rebind_path_flights_after_live_role_change(
                            key,
                            previous_incarnation,
                            current_incarnation,
                        );
                    }
                    if role_changed && updated_role != StreamOpenRole::Active {
                        self.clear_ordered_data_owner_if(key);
                    }
                    if previous_load_registered && !updated_load_registered {
                        self.lane_tracker
                            .detach(self.session_id, key, previous_lane);
                    }
                    *current_lane = lane;
                    if previous_lane != lane {
                        self.lane_tracker.change_lanes(
                            self.session_id,
                            &lane_registered_keys,
                            previous_lane,
                            lane,
                        );
                    }
                    if !previous_load_registered && updated_load_registered {
                        self.lane_tracker.attach(self.session_id, key, lane);
                    }
                    if !response_flow_was_active && response_flow_is_active {
                        self.sync_response_flow_activity(&outputs);
                    }
                    if response_state_invalidated {
                        // Crossing Repair changes response ownership
                        // eligibility, so publish the role and reset Subflow
                        // identities at one outputs-lock linearization point.
                        self.reset_subflow_set_with_outputs(&mut outputs);
                    }
                    if updated_role != StreamOpenRole::Repair {
                        self.owner_underlay_history
                            .fetch_or(response_owner_underlay_seen_bit(underlay), Ordering::AcqRel);
                    }
                    if role == StreamOpenRole::Active {
                        self.set_request_active_owner(key);
                    }
                    drop(outputs);
                    drop(current_lane);
                    self.notify_update();
                    return if role_changed {
                        ResponseStreamAttachOutcome::RoleChanged
                    } else {
                        ResponseStreamAttachOutcome::Attached
                    };
                }
                return ResponseStreamAttachOutcome::RejectedDuplicateLiveOutput;
            }
        }
        let entry = if let Some(position) = existing_position {
            was_active = position + 1 == outputs.entries.len();
            let mut entry = outputs.entries.remove(position);
            if entry.commands.is_closed() {
                previous_load_registered = response_stream_role_reserves_flow_load(entry.role);
                replaced_incarnation = Some(entry.incarnation);
                replaced_path_instance_id = Some(entry.path_instance_id);
                entry.path_instance_id = path_instance_id;
                entry.incarnation = self.allocate_output_incarnation();
                entry.commands = commands;
                entry.role = role;
                entry.owner_data_in_flight_bytes = 0;
                entry.bytes_in_flight = 0;
                entry.product_queue_bytes = 0;
                entry.product_progress_rate_bps = None;
                entry.delivery_rate_bps = None;
                entry.tcp_ack_clock_rate_bps = None;
                entry.tcp_product_rate_evidence = None;
                entry.tcp_capacity_prior = None;
                entry.srtt_ms = None;
                entry.delivery_samples = 0;
                entry.owner_data_acked_bytes = 0;
                entry.local_path_metrics = None;
                entry.peer_path_metrics = None;
                replaced_closed = true;
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "server_stream_output_attach",
                    format_args!(
                        "session_id={} path_underlay={:?} path_id={} role={:?} lane={:?} result=replace_closed",
                        self.session_id.0, underlay, path_id.0, role, lane,
                    ),
                );
            } else {
                #[cfg(feature = "lab-diagnostics")]
                {
                    let same_channel = entry.commands.same_channel(&commands);
                    lab_diagnostic(
                        "server_stream_output_attach",
                        format_args!(
                            "session_id={} path_underlay={:?} path_id={} role={:?} lane={:?} result=duplicate_live same_channel={}",
                            self.session_id.0, underlay, path_id.0, role, lane, same_channel,
                        ),
                    );
                }
            }
            entry
        } else {
            ResponseStreamOutputEntry {
                key,
                path_instance_id,
                incarnation: self.allocate_output_incarnation(),
                commands,
                role,
                owner_data_in_flight_bytes: 0,
                bytes_in_flight: 0,
                product_queue_bytes: 0,
                product_progress_rate_bps: None,
                delivery_rate_bps: None,
                tcp_ack_clock_rate_bps: None,
                tcp_product_rate_evidence: None,
                tcp_capacity_prior: None,
                srtt_ms: None,
                delivery_samples: 0,
                owner_data_acked_bytes: 0,
                local_path_metrics: None,
                peer_path_metrics: None,
            }
        };
        let promote_or_keep_active_slot = was_active || outputs.entries.is_empty();
        if promote_or_keep_active_slot {
            outputs.entries.push(entry);
        } else {
            let insert_at = outputs.entries.len().saturating_sub(1);
            outputs.entries.insert(insert_at, entry);
        }
        let updated_load_registered = response_stream_role_reserves_flow_load(role);
        let lane_registered_keys = outputs
            .entries
            .iter()
            .filter(|entry| {
                response_stream_role_reserves_flow_load(entry.role)
                    && (entry.key != key || previous_load_registered)
            })
            .map(|entry| entry.key)
            .collect::<Vec<_>>();
        let response_flow_is_active = Self::response_flow_is_active(&outputs);
        if response_flow_was_active && !response_flow_is_active {
            self.sync_response_flow_activity(&outputs);
        }
        if let Some(incarnation) = replaced_incarnation {
            outputs.ack_clock_calibrations.remove(&(key, incarnation));
            if outputs.active_ack_clock_calibration == Some((key, incarnation)) {
                outputs.active_ack_clock_calibration = None;
            }
            self.invalidate_path_flight_evidence(key, incarnation);
        }
        if let Some(replaced_path_instance_id) = replaced_path_instance_id {
            // Output replacement retires this binding's queue, not the shared
            // carrier instance. Only registry path-registration drop may reset
            // an exact carrier's bounded attempt count.
            self.lane_tracker.clear_quic_capacity_calibration(
                self.session_id,
                self.binding_instance_id,
                key,
                replaced_path_instance_id,
            );
            self.lane_tracker
                .clear_response_service_handoff_drain_for_path(
                    self.session_id,
                    self.binding_instance_id,
                    key,
                    replaced_path_instance_id,
                );
        }
        if replaced_closed && role != StreamOpenRole::Active {
            self.clear_ordered_data_owner_if(key);
        }
        if previous_load_registered && !updated_load_registered {
            self.lane_tracker
                .detach(self.session_id, key, previous_lane);
        }
        *current_lane = lane;
        if previous_lane != lane {
            self.lane_tracker.change_lanes(
                self.session_id,
                &lane_registered_keys,
                previous_lane,
                lane,
            );
        }
        if !previous_load_registered && updated_load_registered {
            self.lane_tracker.attach(self.session_id, key, lane);
        }
        if !response_flow_was_active && response_flow_is_active {
            self.sync_response_flow_activity(&outputs);
        }
        // A planner may snapshot the old generation before blocking on outputs,
        // but it cannot observe new membership before this invalidation completes.
        // Passive growth does not recreate cumulative startup sampling credit.
        if replaced_closed || role == StreamOpenRole::Active {
            self.reset_subflow_set_with_outputs(&mut outputs);
        } else {
            self.invalidate_subflow_plan();
        }
        if role != StreamOpenRole::Repair {
            self.owner_underlay_history
                .fetch_or(response_owner_underlay_seen_bit(underlay), Ordering::AcqRel);
        }
        if replaced_closed && role != StreamOpenRole::Active {
            self.clear_request_active_owner_if(key);
        } else if role == StreamOpenRole::Active {
            self.set_request_active_owner(key);
        }
        drop(outputs);
        drop(current_lane);
        if role == StreamOpenRole::Validation {
            let _ = enqueue_path_proof_frame(&proof_commands, path_id, self.mux_limits);
        }
        self.notify_update();
        if replaced_closed {
            ResponseStreamAttachOutcome::ReplacedClosedOutput
        } else {
            ResponseStreamAttachOutcome::Attached
        }
    }

    pub(in crate::runtime) fn lane(&self) -> FlowLane {
        *self.lane.lock().expect("server reliable stream lane lock")
    }

    pub(in crate::runtime) fn subscribe_updates(&self) -> watch::Receiver<u64> {
        self.version.subscribe()
    }

    pub(in crate::runtime) fn send_path_snapshot(
        &self,
        lane: FlowLane,
        payload_bytes: usize,
    ) -> Option<PathSnapshot> {
        self.relay_read_snapshot(lane, payload_bytes).send_path
    }

    pub(in crate::runtime) fn relay_read_snapshot(
        &self,
        lane: FlowLane,
        payload_bytes: usize,
    ) -> ResponseRelayReadSnapshot {
        let may_have_mixed_owner_underlays = self.may_have_mixed_owner_underlays();
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let stored_service_key = *self
            .ordered_data_owner
            .lock()
            .expect("server reliable stream ordered data owner lock");
        outputs.relay_read_snapshot(
            stored_service_key,
            may_have_mixed_owner_underlays,
            self.session_id,
            &self.lane_tracker,
            lane,
            payload_bytes,
            self.mux_limits,
        )
    }

    pub(in crate::runtime) fn tail_repair_snapshot(
        &self,
        ack_frontier: u64,
        lane: FlowLane,
    ) -> Option<PathSnapshot> {
        let owner_key = self
            .blocking_owner_key_at_or_after(ack_frontier)
            .or_else(|| self.ordered_data_owner());
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        owner_key.and_then(|key| {
            outputs.snapshot_for_key(
                key,
                self.session_id,
                &self.lane_tracker,
                lane,
                self.mux_limits,
            )
        })
    }

    pub(in crate::runtime) fn tail_repair_owner_underlay(
        &self,
        ack_frontier: u64,
    ) -> Option<UnderlayProtocol> {
        self.blocking_owner_key_at_or_after(ack_frontier)
            .or_else(|| self.ordered_data_owner())
            .map(|key| key.underlay)
    }

    #[cfg(test)]
    pub(in crate::runtime) fn has_live_mixed_owner_underlays(&self) -> bool {
        if !self.may_have_mixed_owner_underlays() {
            return false;
        }
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        response_outputs_have_live_mixed_owner_underlays(&outputs.entries)
    }

    pub(in crate::runtime) fn may_have_mixed_owner_underlays(&self) -> bool {
        self.owner_underlay_history.load(Ordering::Acquire) & RESPONSE_OWNER_MIXED_SEEN
            == RESPONSE_OWNER_MIXED_SEEN
    }

    fn set_sender_queue_bytes(&self, bytes: usize) {
        let bytes = bytes as u64;
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let mut changed = false;
        for entry in &mut outputs.entries {
            if entry.product_queue_bytes != bytes {
                entry.product_queue_bytes = bytes;
                changed = true;
            }
        }
        if changed {
            self.response_model_generation
                .fetch_add(1, Ordering::AcqRel);
        }
        drop(outputs);
        if changed {
            self.notify_update();
        }
    }

    fn capacity_notifies(&self) -> Vec<Arc<Notify>> {
        self.outputs
            .lock()
            .expect("server reliable stream binding lock")
            .entries
            .iter()
            .map(|entry| entry.commands.capacity_notify())
            .collect()
    }

    pub(in crate::runtime) fn set_lane(&self, lane: FlowLane) {
        let mut current_lane = self.lane.lock().expect("server reliable stream lane lock");
        let previous_lane = *current_lane;
        if previous_lane != lane {
            let outputs = self
                .outputs
                .lock()
                .expect("server reliable stream binding lock");
            let attached_keys = outputs
                .entries
                .iter()
                .filter(|entry| response_stream_role_reserves_flow_load(entry.role))
                .map(|entry| entry.key)
                .collect::<Vec<_>>();
            *current_lane = lane;
            self.lane_tracker
                .change_lanes(self.session_id, &attached_keys, previous_lane, lane);
            self.response_flow_registration
                .change_lane_if_present(previous_lane, lane);
            drop(outputs);
        }
        drop(current_lane);
        self.notify_update();
    }

    pub(in crate::runtime) fn has_multipath_repair_alternative(&self) -> bool {
        self.outputs
            .lock()
            .expect("server reliable stream binding lock")
            .entries
            .len()
            > 1
    }

    pub(in crate::runtime) fn has_repair_output_for_frame(&self, frame: &Frame) -> bool {
        let avoid_keys = self.flight_keys_overlapping_frame(frame);
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        outputs
            .entries
            .iter()
            .any(|entry| !avoid_keys.contains(&entry.key))
    }

    pub(in crate::runtime) fn has_live_owner_tail_repair_output_for_frame(
        &self,
        frame: &Frame,
    ) -> bool {
        let owner_keys = self.owner_flight_keys_overlapping_frame(frame);
        if owner_keys.is_empty() {
            return false;
        }
        self.outputs
            .lock()
            .expect("server reliable stream binding lock")
            .entries
            .iter()
            .any(|entry| !owner_keys.contains(&entry.key))
    }

    pub(in crate::runtime) fn has_recent_live_repair_flight_overlap(
        &self,
        frame: &Frame,
        retry_after: Duration,
    ) -> bool {
        let Some((start, end, _)) = reliable_stream_frame_extent(frame) else {
            return false;
        };
        let now = Instant::now();
        let live_keys = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock")
            .entries
            .iter()
            .map(|entry| entry.key)
            .collect::<Vec<_>>();
        let flights = self
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        product_flights_have_recent_repair_overlap(&flights, start, end, now, retry_after, |key| {
            live_keys.contains(&key)
        })
    }

    pub(in crate::runtime) fn has_failed_owner_repair_output_for_frame(
        &self,
        frame: &Frame,
    ) -> bool {
        let avoid_keys = self.flight_keys_overlapping_frame(frame);
        if avoid_keys.is_empty() {
            return false;
        }
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let recorded_output_still_live = outputs
            .entries
            .iter()
            .any(|entry| avoid_keys.contains(&entry.key));
        !recorded_output_still_live
            && outputs
                .entries
                .iter()
                .any(|entry| !avoid_keys.contains(&entry.key))
    }

    pub(in crate::runtime) fn has_unknown_owner_repair_output_for_frame(
        &self,
        frame: &Frame,
    ) -> bool {
        if !self.flight_keys_overlapping_frame(frame).is_empty()
            || self.ordered_data_owner().is_some()
        {
            return false;
        }
        !self
            .outputs
            .lock()
            .expect("server reliable stream binding lock")
            .entries
            .is_empty()
    }

    pub(in crate::runtime) fn detach(
        &self,
        key: CarrierPathKey,
        commands: &ReliablePathCommandSender,
    ) {
        self.detach_matching_output(key, |entry| entry.commands.same_channel(commands));
    }

    pub(super) fn detach_path_instance(
        &self,
        key: CarrierPathKey,
        path_instance_id: ServerCarrierPathInstanceId,
    ) {
        self.detach_matching_output(key, |entry| entry.path_instance_id == path_instance_id);
    }

    fn detach_matching_output(
        &self,
        key: CarrierPathKey,
        matches: impl Fn(&ResponseStreamOutputEntry) -> bool,
    ) {
        let current_lane = self.lane.lock().expect("server reliable stream lane lock");
        let lane = *current_lane;
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let response_flow_was_active = Self::response_flow_is_active(&outputs);
        let removed = outputs
            .entries
            .iter()
            .find(|entry| entry.key == key && matches(entry))
            .map(|entry| {
                (
                    entry.incarnation,
                    entry.path_instance_id,
                    response_stream_role_reserves_flow_load(entry.role),
                )
            });
        outputs
            .entries
            .retain(|entry| entry.key != key || !matches(entry));
        if let Some((incarnation, path_instance_id, load_registered)) = removed {
            if response_flow_was_active && !Self::response_flow_is_active(&outputs) {
                self.sync_response_flow_activity(&outputs);
            }
            self.invalidate_path_flight_evidence(key, incarnation);
            outputs.ack_clock_calibrations.remove(&(key, incarnation));
            if outputs.active_ack_clock_calibration == Some((key, incarnation)) {
                outputs.active_ack_clock_calibration = None;
            }
            if load_registered {
                self.lane_tracker.detach(self.session_id, key, lane);
            }
            self.lane_tracker.clear_quic_capacity_calibration(
                self.session_id,
                self.binding_instance_id,
                key,
                path_instance_id,
            );
            self.lane_tracker
                .clear_response_service_handoff_drain_for_path(
                    self.session_id,
                    self.binding_instance_id,
                    key,
                    path_instance_id,
                );
            self.repair_ordered_data_owner_after_output_change(&outputs.entries);
            self.reset_subflow_set_with_outputs(&mut outputs);
            self.clear_request_active_owner_if(key);
            drop(outputs);
            drop(current_lane);
            self.notify_update();
        }
    }

    pub(in crate::runtime) fn release_normalized_acked_ranges(&self, ranges: &[OffsetRange]) {
        self.release_normalized_acked_ranges_at(ranges, Instant::now());
    }

    fn release_normalized_acked_ranges_at(&self, ranges: &[OffsetRange], now: Instant) {
        if ranges.is_empty() {
            return;
        }
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let mut flights = self
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        let released = release_carrier_path_flight_ranges(&mut flights, ranges);
        if released.is_empty() {
            drop(flights);
            let ordering_update = self
                .ack_ordering
                .lock()
                .expect("server response ACK ordering lock")
                .apply_normalized_ack(ranges, &[]);
            if ordering_update.changed {
                // Publish the generation only after the coherent ordering view
                // exists. Duplicate ACKs need no fence or shared atomic write.
                self.response_model_generation
                    .fetch_add(1, Ordering::AcqRel);
            }
            drop(outputs);
            if ordering_update.changed {
                self.notify_update();
            }
            return;
        }
        let active_calibration_has_owner_flights = outputs
            .active_ack_clock_calibration
            .is_some_and(|(active_key, active_incarnation)| {
                flights.values().flatten().any(|flight| {
                    flight.key == active_key
                        && flight.output_incarnation == active_incarnation
                        && flight.kind.is_ordering_owner()
                })
            });
        drop(flights);

        let ordering_update = {
            let mut ordering = self
                .ack_ordering
                .lock()
                .expect("server response ACK ordering lock");
            ordering.apply_normalized_ack(ranges, &released)
        };
        #[cfg(feature = "lab-diagnostics")]
        if ordering_update.acked_hole_bytes > 0 {
            lab_diagnostic(
                "server_ack_ordering_state",
                format_args!(
                    "session_id={} contiguous_frontier={} acked_hole_bytes={} released_flights={}",
                    self.session_id.0,
                    ordering_update.contiguous_frontier,
                    ordering_update.acked_hole_bytes,
                    released.len(),
                ),
            );
        }

        let mut changed = false;
        let mut path_samples =
            HashMap::<(CarrierPathKey, u64), (u64, u64, Instant, Instant)>::new();
        for (_, release) in released {
            let flight = release.flight;
            let identity = (flight.key, flight.output_incarnation);
            let stage_authorized_at = outputs
                .ack_clock_calibrations
                .get(&identity)
                .map(|calibration| calibration.stage_authorized_at);
            if let Some(entry) = outputs.entries.iter_mut().find(|entry| {
                entry.key == flight.key && entry.incarnation == flight.output_incarnation
            }) {
                entry.bytes_in_flight = entry.bytes_in_flight.saturating_sub(flight.bytes as u64);
                if flight.kind.is_ordering_owner() {
                    entry.owner_data_in_flight_bytes = entry
                        .owner_data_in_flight_bytes
                        .saturating_sub(flight.bytes as u64);
                }
                if release.path_proving {
                    entry.owner_data_acked_bytes = entry
                        .owner_data_acked_bytes
                        .saturating_add(flight.bytes as u64);
                    let sample = path_samples.entry(identity).or_insert((
                        0_u64,
                        0_u64,
                        flight.sent_at,
                        flight.sent_at,
                    ));
                    sample.0 = sample.0.saturating_add(flight.bytes as u64);
                    if stage_authorized_at
                        .is_some_and(|authorized_at| flight.sent_at >= authorized_at)
                    {
                        sample.1 = sample.1.saturating_add(flight.bytes as u64);
                    }
                    sample.2 = sample.2.min(flight.sent_at);
                    sample.3 = sample.3.max(flight.sent_at);
                }
                changed = true;
            }
        }
        for ((key, output_incarnation), (bytes, fresh_bytes, first_sent_at, last_sent_at)) in
            path_samples
        {
            let identity = (key, output_incarnation);
            let ack_clock_update = if outputs.active_ack_clock_calibration == Some(identity) {
                outputs
                    .ack_clock_calibrations
                    .get_mut(&identity)
                    .filter(|calibration| {
                        calibration.spent_bytes > 0 && !calibration.proven && !calibration.retired
                    })
                    .map(|calibration| {
                        calibration
                            .rate_evidence
                            .get_or_insert_with(|| ResponseAckClockRateEvidence::new(first_sent_at))
                            .observe_with_fresh_bytes(
                                bytes,
                                fresh_bytes,
                                first_sent_at,
                                last_sent_at,
                                now,
                            )
                    })
            } else {
                None
            };
            let ack_clock_window = match ack_clock_update {
                Some(ResponseAckClockRateEvidenceUpdate::Proven {
                    sample,
                    bytes,
                    fresh_bytes,
                    first_window,
                    earliest_sent_at,
                    previous_window_acked_at,
                    latest_sent_at,
                }) => Some((
                    if first_window { None } else { sample },
                    bytes,
                    fresh_bytes,
                    first_window,
                    earliest_sent_at,
                    previous_window_acked_at,
                    latest_sent_at,
                )),
                _ => None,
            };
            let calibration_update = ack_clock_window.and_then(
                |(
                    strict_rate_sample,
                    window_bytes,
                    fresh_window_bytes,
                    first_window,
                    earliest_sent_at,
                    previous_ack_at,
                    latest_sent_at,
                )| {
                    outputs
                        .ack_clock_calibrations
                        .get_mut(&identity)
                        .map(|calibration| {
                            let sample_bps = strict_rate_sample
                                .map(PathRateSample::rate_bps)
                                .unwrap_or(0.0);
                            let sample_elapsed = strict_rate_sample
                                .map(PathRateSample::elapsed)
                                .unwrap_or(Duration::ZERO);
                            let previous_credit = calibration.credit_limit_bytes;
                            let stage_authorized_at = calibration.stage_authorized_at;
                            let stage_authorized_spent_bytes =
                                calibration.stage_authorized_spent_bytes;
                            let stage_credit_bytes = calibration.stage_credit_bytes();
                            let stage_window_eligible = earliest_sent_at >= stage_authorized_at;
                            let stage_rate_evidence_accepted = stage_window_eligible
                                && strict_rate_sample.is_some()
                                && fresh_window_bytes == window_bytes;
                            let stage_evidence_bytes = if stage_rate_evidence_accepted {
                                calibration
                                    .stage_rate_evidence_bytes
                                    .saturating_add(window_bytes)
                            } else {
                                calibration.stage_rate_evidence_bytes
                            };
                            let stage_evidence_elapsed = if stage_rate_evidence_accepted {
                                calibration
                                    .stage_rate_evidence_elapsed
                                    .saturating_add(sample_elapsed)
                            } else {
                                calibration.stage_rate_evidence_elapsed
                            };
                            let stage_rate_ineligible_bytes =
                                if fresh_window_bytes > 0 && !stage_rate_evidence_accepted {
                                    calibration
                                        .stage_rate_ineligible_bytes
                                        .saturating_add(fresh_window_bytes)
                                } else {
                                    calibration.stage_rate_ineligible_bytes
                                };
                            let stage_fully_spent =
                                calibration.spent_bytes >= calibration.credit_limit_bytes;
                            let stage_strict_capacity_bytes =
                                stage_credit_bytes.saturating_sub(stage_rate_ineligible_bytes);
                            let previous_stage_rate_sample_count =
                                calibration.stage_rate_sample_count();
                            let aggregate_rate_bps = (stage_fully_spent
                                && stage_strict_capacity_bytes
                                    >= calibration.stage_rate_coverage_floor_bytes
                                && stage_evidence_bytes
                                    >= calibration.stage_rate_coverage_floor_bytes)
                                .then(|| {
                                    stage_evidence_bytes as f64 * 8.0
                                        / stage_evidence_elapsed
                                            .max(TRANSPORT_TIMER_GRANULARITY)
                                            .as_secs_f64()
                                })
                                .unwrap_or(0.0);
                            let credit_grew = calibration.record_ack_clock_window(
                                strict_rate_sample,
                                window_bytes,
                                fresh_window_bytes,
                                earliest_sent_at,
                                now,
                            );
                            debug_assert_eq!(
                                credit_grew,
                                calibration.credit_limit_bytes > previous_credit
                            );
                            let stage_rate_sample_accepted = calibration.stage_rate_sample_count()
                                > previous_stage_rate_sample_count;
                            (
                                sample_bps,
                                *calibration,
                                credit_grew,
                                first_window,
                                strict_rate_sample.is_some(),
                                stage_window_eligible,
                                stage_rate_evidence_accepted,
                                stage_fully_spent,
                                stage_rate_sample_accepted,
                                window_bytes,
                                fresh_window_bytes,
                                sample_elapsed,
                                stage_evidence_bytes,
                                stage_evidence_elapsed,
                                stage_rate_ineligible_bytes,
                                calibration.stage_rate_coverage_floor_bytes,
                                stage_authorized_spent_bytes,
                                stage_credit_bytes,
                                stage_strict_capacity_bytes,
                                aggregate_rate_bps,
                                stage_authorized_at,
                                earliest_sent_at,
                                previous_ack_at,
                                latest_sent_at,
                            )
                        })
                },
            );
            let calibration_snapshot = outputs.ack_clock_calibrations.get(&identity).copied();
            let calibration_identity_active =
                outputs.active_ack_clock_calibration == Some(identity);
            if let Some(entry) = outputs
                .entries
                .iter_mut()
                .find(|entry| entry.key == key && entry.incarnation == output_incarnation)
            {
                let udp_assignment_sample = (entry.key.underlay == UnderlayProtocol::Udp)
                    .then(|| {
                        PathRateSample::new(bytes, now.saturating_duration_since(first_sent_at))
                    })
                    .flatten();
                // Flight timestamps mark scheduler assignment, not TCP kernel
                // dispatch. The first exact ACK establishes the clock; later
                // binding-local OwnerData bytes use continuous ACK wall time so
                // callback compression cannot discard the preceding silence.
                let (tcp_ack_clock_sample, tcp_ack_clock_window_complete) =
                    if entry.key.underlay == UnderlayProtocol::Tcp {
                        let evidence = entry.tcp_product_rate_evidence.get_or_insert_with(|| {
                            ResponseAckClockRateEvidence::new(first_sent_at)
                        });
                        let update = evidence.observe_with_fresh_bytes(
                            bytes,
                            bytes,
                            first_sent_at,
                            last_sent_at,
                            now,
                        );
                        (
                            evidence.goodput_sample(),
                            matches!(update, ResponseAckClockRateEvidenceUpdate::Proven { .. }),
                        )
                    } else {
                        (None, false)
                    };
                let carrier_app_limited = entry
                    .local_path_metrics
                    .is_some_and(|metrics| metrics.metrics.app_limited);
                let calibrated_rate_bps =
                    calibration_snapshot.and_then(|calibration| calibration.calibrated_rate_bps);
                let mut ordinary_tcp_rate_replaces_capacity_prior = false;
                if entry.key.underlay == UnderlayProtocol::Tcp
                    && !calibration_identity_active
                    && calibration_update.is_none()
                    && tcp_ack_clock_window_complete
                    && let Some(prior) = entry.tcp_capacity_prior.as_mut()
                {
                    prior.ordinary_windows = prior.ordinary_windows.saturating_add(1);
                    ordinary_tcp_rate_replaces_capacity_prior =
                        product_delivery_samples_override_startup_prior(prior.ordinary_windows)
                            && tcp_ack_clock_sample.is_some();
                }
                if ordinary_tcp_rate_replaces_capacity_prior {
                    entry.tcp_capacity_prior = None;
                }
                // A terminal calibration ACK remains exclusive, while either
                // capacity-prior source stays temporary until a fresh ordinary
                // exact-ACK epoch reaches the normal evidence threshold.
                let tcp_measurement_owns_rate = entry.key.underlay == UnderlayProtocol::Tcp
                    && (calibration_identity_active
                        || calibration_update.is_some()
                        || entry.tcp_capacity_prior.is_some()
                        || calibration_snapshot.is_some_and(|calibration| {
                            !calibration.proven && !calibration.retired
                        }));
                if !tcp_measurement_owns_rate {
                    match (
                        entry.key.underlay,
                        tcp_ack_clock_sample,
                        udp_assignment_sample,
                    ) {
                        (UnderlayProtocol::Tcp, Some(sample), _) => {
                            // The sample already smooths a bounded byte/time
                            // epoch. Averaging point rates would restore the ACK
                            // compression bias this ratio removes.
                            let rate_bps = sample.rate_bps();
                            entry.tcp_ack_clock_rate_bps = Some(rate_bps);
                            entry.product_progress_rate_bps = Some(rate_bps);
                            entry.delivery_rate_bps = Some(rate_bps);
                        }
                        (UnderlayProtocol::Udp, _, Some(sample)) => {
                            let sample_bps = sample.rate_bps();
                            entry.product_progress_rate_bps =
                                Some(match entry.product_progress_rate_bps {
                                    Some(previous) if carrier_app_limited => {
                                        previous.max(sample_bps)
                                    }
                                    Some(previous) => previous.mul_add(0.75, sample_bps * 0.25),
                                    None => sample_bps,
                                });
                        }
                        _ => {}
                    }
                }
                if entry.key.underlay == UnderlayProtocol::Tcp
                    && calibration_identity_active
                    && let Some(calibrated_rate_bps) = calibrated_rate_bps
                {
                    entry.product_progress_rate_bps = Some(calibrated_rate_bps);
                    entry.delivery_rate_bps = Some(calibrated_rate_bps);
                }
            }
            if let Some((
                sample_bps,
                calibration,
                credit_grew,
                first_window,
                strict_rate_window,
                stage_window_eligible,
                stage_rate_evidence_accepted,
                stage_fully_spent,
                stage_rate_sample_accepted,
                sample_bytes,
                fresh_sample_bytes,
                sample_elapsed,
                stage_evidence_bytes,
                stage_evidence_elapsed,
                stage_rate_ineligible_bytes,
                stage_rate_coverage_floor_bytes,
                stage_authorized_spent_bytes,
                stage_credit_bytes,
                stage_strict_capacity_bytes,
                aggregate_rate_bps,
                stage_authorized_at,
                earliest_sent_at,
                previous_ack_at,
                latest_sent_at,
            )) = calibration_update
            {
                #[cfg(not(feature = "lab-diagnostics"))]
                let _ = (
                    sample_bps,
                    calibration,
                    credit_grew,
                    first_window,
                    strict_rate_window,
                    stage_window_eligible,
                    stage_rate_evidence_accepted,
                    stage_fully_spent,
                    stage_rate_sample_accepted,
                    sample_bytes,
                    fresh_sample_bytes,
                    sample_elapsed,
                    stage_evidence_bytes,
                    stage_evidence_elapsed,
                    stage_rate_ineligible_bytes,
                    stage_rate_coverage_floor_bytes,
                    stage_authorized_spent_bytes,
                    stage_credit_bytes,
                    stage_strict_capacity_bytes,
                    aggregate_rate_bps,
                    stage_authorized_at,
                    earliest_sent_at,
                    previous_ack_at,
                    latest_sent_at,
                );
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "response_ack_clock_calibration",
                    format_args!(
                        "phase=ack_clock_window session_id={} binding_instance_id={} underlay={:?} path_id={} incarnation={} rate_bps={} sample_bytes={} fresh_sample_bytes={} sample_elapsed_us={} calibrated_rate_bps={} calibrated_rate_ready={} first_window={} strict_rate_window={} stage_window_eligible={} stage_rate_evidence_accepted={} stage_fully_spent={} stage_rate_sample_accepted={} stage_evidence_bytes={} stage_evidence_elapsed_us={} stage_rate_ineligible_bytes={} stage_rate_coverage_floor_bytes={} stage_authorized_spent_bytes={} stage_credit_bytes={} stage_strict_capacity_bytes={} aggregate_rate_bps={} spent_bytes={} credit_limit_bytes={} max_limit_bytes={} credit_grew={} proven={} stage_authorized_age_us={} earliest_sent_age_us={} previous_ack_age_us={} latest_sent_age_us={} stage_provenance_slack_us={} causal_slack_us={}",
                        self.session_id.0,
                        self.binding_instance_id,
                        key.underlay,
                        key.path_id.0,
                        output_incarnation,
                        sample_bps,
                        sample_bytes,
                        fresh_sample_bytes,
                        sample_elapsed.as_micros(),
                        calibration.calibrated_rate_bps.unwrap_or(0.0),
                        calibration.calibrated_rate_bps.is_some(),
                        first_window,
                        strict_rate_window,
                        stage_window_eligible,
                        stage_rate_evidence_accepted,
                        stage_fully_spent,
                        stage_rate_sample_accepted,
                        stage_evidence_bytes,
                        stage_evidence_elapsed.as_micros(),
                        stage_rate_ineligible_bytes,
                        stage_rate_coverage_floor_bytes,
                        stage_authorized_spent_bytes,
                        stage_credit_bytes,
                        stage_strict_capacity_bytes,
                        aggregate_rate_bps,
                        calibration.spent_bytes,
                        calibration.credit_limit_bytes,
                        calibration.max_limit_bytes,
                        credit_grew,
                        calibration.proven,
                        now.saturating_duration_since(stage_authorized_at)
                            .as_micros(),
                        now.saturating_duration_since(earliest_sent_at).as_micros(),
                        previous_ack_at.map_or(0, |acked_at| {
                            now.saturating_duration_since(acked_at).as_micros()
                        }),
                        now.saturating_duration_since(latest_sent_at).as_micros(),
                        earliest_sent_at
                            .saturating_duration_since(stage_authorized_at)
                            .as_micros(),
                        previous_ack_at.map_or(0, |acked_at| {
                            acked_at
                                .saturating_duration_since(latest_sent_at)
                                .as_micros()
                        }),
                    ),
                );
            }
        }
        if !active_calibration_has_owner_flights
            && let Some(identity) = outputs.active_ack_clock_calibration
        {
            let previous_credit = outputs
                .ack_clock_calibrations
                .get(&identity)
                .map_or(0, |calibration| calibration.credit_limit_bytes);
            let mut transition_snapshot = None;
            let (clear_active, terminal_reason) =
                match outputs.ack_clock_calibrations.get_mut(&identity) {
                    None => (true, "missing_state"),
                    Some(calibration) => {
                        if calibration.proven {
                            transition_snapshot = Some(*calibration);
                            (
                                true,
                                if calibration.calibrated_rate_bps.is_some() {
                                    "robust_rate"
                                } else {
                                    "hard_ceiling_no_rate"
                                },
                            )
                        } else if calibration.retired {
                            transition_snapshot = Some(*calibration);
                            (true, "retired_drain")
                        } else {
                            let previous_stage_rate_samples = calibration.stage_rate_sample_count();
                            if calibration.advance_drained_stage(now) {
                                let accepted_stage = calibration.stage_rate_sample_count()
                                    > previous_stage_rate_samples;
                                transition_snapshot = Some(*calibration);
                                (
                                    false,
                                    if accepted_stage {
                                        "drain_stage_advance"
                                    } else {
                                        "drain_reachability_topup"
                                    },
                                )
                            } else if calibration.proven {
                                transition_snapshot = Some(*calibration);
                                (
                                    true,
                                    if calibration.calibrated_rate_bps.is_some() {
                                        "robust_rate"
                                    } else {
                                        "hard_ceiling_no_rate"
                                    },
                                )
                            } else if calibration.spent_bytes >= calibration.max_limit_bytes {
                                transition_snapshot = Some(*calibration);
                                calibration.retire();
                                (true, "hard_ceiling_drain")
                            } else if calibration.spent_bytes >= calibration.credit_limit_bytes {
                                transition_snapshot = Some(*calibration);
                                calibration.retire();
                                (true, "under_covered_drain")
                            } else {
                                (false, "credit_remaining")
                            }
                        }
                    }
                };
            if clear_active || terminal_reason != "credit_remaining" {
                let terminal = transition_snapshot;
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "response_ack_clock_calibration",
                    format_args!(
                        "phase={} session_id={} binding_instance_id={} underlay={:?} path_id={} incarnation={} reason={} active_owner_flights=false calibrated_rate_ready={} calibrated_rate_bps={} spent_bytes={} previous_credit_limit_bytes={} credit_limit_bytes={} max_limit_bytes={} stage_authorized_spent_bytes={} stage_credit_bytes={} stage_strict_capacity_bytes={} stage_evidence_bytes={} stage_rate_ineligible_bytes={} proven={} retired={}",
                        if clear_active {
                            "terminal"
                        } else {
                            "drain_transition"
                        },
                        self.session_id.0,
                        self.binding_instance_id,
                        identity.0.underlay,
                        identity.0.path_id.0,
                        identity.1,
                        terminal_reason,
                        terminal.is_some_and(|state| state.calibrated_rate_bps.is_some()),
                        terminal
                            .and_then(|state| state.calibrated_rate_bps)
                            .unwrap_or(0.0),
                        terminal.map_or(0, |state| state.spent_bytes),
                        previous_credit,
                        terminal.map_or(0, |state| state.credit_limit_bytes),
                        terminal.map_or(0, |state| state.max_limit_bytes),
                        terminal.map_or(0, |state| state.stage_authorized_spent_bytes),
                        terminal.map_or(0, |state| state.stage_credit_bytes()),
                        terminal.map_or(0, |state| state.stage_strict_capacity_bytes()),
                        terminal.map_or(0, |state| state.stage_rate_evidence_bytes),
                        terminal.map_or(0, |state| state.stage_rate_ineligible_bytes),
                        terminal.is_some_and(|state| state.proven),
                        terminal.is_some_and(|state| state.retired),
                    ),
                );
                #[cfg(not(feature = "lab-diagnostics"))]
                let _ = (terminal_reason, terminal, previous_credit);
            };
            if clear_active {
                outputs.active_ack_clock_calibration = None;
                if let Some(entry) = outputs
                    .entries
                    .iter_mut()
                    .find(|entry| entry.key == identity.0 && entry.incarnation == identity.1)
                {
                    // Exclude bounded calibration traffic from the continuous
                    // ACK clock that will eventually replace its startup prior.
                    entry.tcp_product_rate_evidence = None;
                    entry.tcp_ack_clock_rate_bps = None;
                    entry.tcp_capacity_prior = transition_snapshot
                        .and_then(|state| state.calibrated_rate_bps)
                        .map(|rate_bps| TcpResponseCapacityPrior {
                            rate_bps,
                            ordinary_windows: 0,
                        });
                    if let Some(prior) = entry.tcp_capacity_prior {
                        entry.product_progress_rate_bps = Some(prior.rate_bps);
                        entry.delivery_rate_bps = Some(prior.rate_bps);
                    }
                }
            }
        }
        for hole in ordering_update.newly_contiguous {
            if !hole.path_proving {
                continue;
            }
            if let Some(entry) = outputs
                .entries
                .iter_mut()
                .find(|entry| entry.key == hole.key && entry.incarnation == hole.output_incarnation)
            {
                if hole.end <= ordering_update.contiguous_frontier {
                    entry.delivery_samples = entry.delivery_samples.saturating_add(1);
                    changed = true;
                }
            }
        }
        // A planner captures this before reading lower flights and path
        // snapshots. Publish it only after both ledgers describe the ACK.
        self.response_model_generation
            .fetch_add(1, Ordering::AcqRel);
        drop(outputs);
        if changed || ordering_update.changed {
            self.graduate_completed_response_startup_owner();
            // ACK progress updates path evidence and ordering, but Subflow
            // admission credit is epoch state. Recreate it only on a semantic
            // reset or admission-envelope change, not passive membership growth.
            self.notify_update();
        }
    }

    pub(in crate::runtime) fn ordered_data_owner(&self) -> Option<CarrierPathKey> {
        *self
            .ordered_data_owner
            .lock()
            .expect("server reliable stream ordered data owner lock")
    }

    #[cfg(test)]
    pub(in crate::runtime) fn request_active_owner(&self) -> Option<CarrierPathKey> {
        *self
            .request_active_owner
            .lock()
            .expect("server reliable stream request active owner lock")
    }

    pub(in crate::runtime) fn request_active_underlay(&self) -> Option<UnderlayProtocol> {
        self.request_active_owner
            .lock()
            .expect("server reliable stream request active owner lock")
            .map(|key| key.underlay)
    }

    pub(in crate::runtime) fn request_active_path_snapshot(
        &self,
        lane: FlowLane,
    ) -> Option<PathSnapshot> {
        // Attach and detach take these locks in this order before changing the
        // request-side Active identity. Keep the identity and its metrics in a
        // single coherent snapshot without reversing that order.
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let active_key = *self
            .request_active_owner
            .lock()
            .expect("server reliable stream request active owner lock");
        active_key.and_then(|key| {
            outputs.snapshot_for_key(
                key,
                self.session_id,
                &self.lane_tracker,
                lane,
                self.mux_limits,
            )
        })
    }

    pub(in crate::runtime) fn has_output_incarnation(
        &self,
        key: CarrierPathKey,
        incarnation: u64,
    ) -> bool {
        self.outputs
            .lock()
            .expect("server reliable stream binding lock")
            .entries
            .iter()
            .any(|entry| entry.key == key && entry.incarnation == incarnation)
    }

    fn set_request_active_owner(&self, key: CarrierPathKey) {
        *self
            .request_active_owner
            .lock()
            .expect("server reliable stream request active owner lock") = Some(key);
    }

    fn clear_request_active_owner_if(&self, key: CarrierPathKey) {
        let mut active = self
            .request_active_owner
            .lock()
            .expect("server reliable stream request active owner lock");
        if *active == Some(key) {
            *active = None;
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn set_ordered_data_owner(&self, key: CarrierPathKey) {
        let lane = self.lane();
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let mut lead = self
            .ordered_data_owner
            .lock()
            .expect("server reliable stream ordered data owner lock");
        if *lead != Some(key) {
            *lead = Some(key);
            self.reset_subflow_set_with_outputs(&mut outputs);
            self.response_flow_registration
                .set_service(Some((key, lane)));
            drop(lead);
            drop(outputs);
            self.notify_update();
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn commit_ordered_data_owner_for_target(
        &self,
        target: &ResponseSenderPathTarget,
    ) -> bool {
        self.commit_ordered_data_owner_for_dispatch_target(&target.into())
    }

    pub(in crate::runtime) fn commit_ordered_data_owner_for_dispatch_target(
        &self,
        target: &ResponseDispatchTarget,
    ) -> bool {
        let lane = self.lane();
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let target_is_live = outputs.entries.iter().any(|entry| {
            entry.key == target.key
                && entry.incarnation == target.incarnation
                && entry.commands.same_channel(&target.commands)
                && !entry.commands.is_closed()
        });
        if !target_is_live {
            return false;
        }
        let mut lead = self
            .ordered_data_owner
            .lock()
            .expect("server reliable stream ordered data owner lock");
        if !self.response_stream_open.load(Ordering::Acquire) {
            return false;
        }
        let changed = *lead != Some(target.key);
        if changed {
            *lead = Some(target.key);
            outputs
                .ack_clock_calibrations
                .remove(&(target.key, target.incarnation));
            if outputs.active_ack_clock_calibration == Some((target.key, target.incarnation)) {
                outputs.active_ack_clock_calibration = None;
            }
            self.reset_subflow_set_with_outputs(&mut outputs);
            self.response_flow_registration
                .set_service(Some((target.key, lane)));
        }
        drop(lead);
        drop(outputs);
        if changed {
            self.notify_update();
        }
        true
    }

    fn clear_ordered_data_owner_if(&self, key: CarrierPathKey) {
        let mut lead = self
            .ordered_data_owner
            .lock()
            .expect("server reliable stream ordered data owner lock");
        let changed = *lead == Some(key);
        if changed {
            *lead = None;
            self.response_flow_registration.set_service(None);
        }
        drop(lead);
    }

    fn subflow_set_for(
        current: Option<FlowSubflowSet>,
        epoch_generation: u64,
        service: CarrierPathKey,
        startup_owner_credit_bytes: usize,
        optional_overhead_budget_bytes: usize,
        max_read_gap_budget: Duration,
    ) -> FlowSubflowSet {
        current
            .filter(|epoch| {
                epoch.matches_envelope(
                    service,
                    startup_owner_credit_bytes,
                    optional_overhead_budget_bytes,
                    max_read_gap_budget,
                )
            })
            .unwrap_or_else(|| {
                FlowSubflowSet::new(
                    epoch_generation,
                    service,
                    startup_owner_credit_bytes,
                    optional_overhead_budget_bytes,
                    max_read_gap_budget,
                )
            })
    }

    #[cfg(test)]
    pub(in crate::runtime) fn subflow_set_snapshot(&self) -> Option<FlowSubflowSet> {
        self.subflow_set
            .lock()
            .expect("server reliable stream subflow set lock")
            .set
            .clone()
    }

    pub(in crate::runtime) fn subflow_state_snapshot(&self) -> (u64, Option<FlowSubflowSet>) {
        let state = self
            .subflow_set
            .lock()
            .expect("server reliable stream subflow set lock");
        (state.planner_generation, state.set.clone())
    }

    pub(in crate::runtime) fn response_model_generation(&self) -> u64 {
        self.response_model_generation.load(Ordering::Acquire)
    }

    #[cfg(feature = "lab-diagnostics")]
    pub(in crate::runtime) fn should_emit_response_service_handoff_diagnostic(
        &self,
        model_generation: u64,
        evaluation_signature: u64,
        capacity_marker_signature: u64,
        now: Instant,
    ) -> bool {
        const REFRESH_INTERVAL: Duration = Duration::from_secs(1);

        let mut previous = self
            .response_service_handoff_diagnostic
            .lock()
            .expect("response Service handoff diagnostic lock");
        let should_emit = previous.is_none_or(|previous| {
            previous.evaluation_signature != evaluation_signature
                || previous.capacity_marker_signature != capacity_marker_signature
                || (previous.model_generation != model_generation
                    && now.saturating_duration_since(previous.emitted_at) >= REFRESH_INTERVAL)
        });
        if should_emit {
            *previous = Some(ResponseServiceHandoffDiagnosticState {
                model_generation,
                evaluation_signature,
                capacity_marker_signature,
                emitted_at: now,
            });
        }
        should_emit
    }

    #[cfg(test)]
    pub(in crate::runtime) fn lane_generation(&self) -> u64 {
        self.lane_tracker.generation(self.session_id)
    }

    #[cfg(test)]
    pub(in crate::runtime) fn lane_generation_and_active_response_flows(&self) -> (u64, u32) {
        self.lane_tracker
            .generation_and_active_response_flows(self.session_id)
    }

    pub(in crate::runtime) fn response_scheduling_snapshot(
        &self,
    ) -> ResponseSessionSchedulingSnapshot {
        self.lane_tracker
            .response_scheduling_snapshot(self.session_id)
    }

    pub(in crate::runtime) fn try_reserve_tcp_capacity_probe(
        &self,
        expected_generation: u64,
    ) -> Option<TcpCapacityProbeSessionLease> {
        self.lane_tracker
            .try_reserve_tcp_capacity_probe(self.session_id, expected_generation)
    }

    #[cfg(test)]
    pub(in crate::runtime) fn preview_subflow_owner_admission(
        &self,
        service: CarrierPathKey,
        startup_owner_credit_bytes: usize,
        optional_overhead_budget_bytes: usize,
        max_read_gap_budget: Duration,
        input: SubflowAdmissionInput,
    ) -> PathAdmission {
        let state = self
            .subflow_set
            .lock()
            .expect("server reliable stream subflow set lock");
        let epoch_generation = state.epoch_generation;
        let current = state.set.clone();
        drop(state);
        let mut epoch = Self::subflow_set_for(
            current,
            epoch_generation,
            service,
            startup_owner_credit_bytes,
            optional_overhead_budget_bytes,
            max_read_gap_budget,
        );
        epoch.admit_subflow_owner(input)
    }

    #[cfg(test)]
    pub(in crate::runtime) fn commit_subflow_owner_admission(
        &self,
        service: CarrierPathKey,
        startup_owner_credit_bytes: usize,
        optional_overhead_budget_bytes: usize,
        max_read_gap_budget: Duration,
        input: SubflowAdmissionInput,
    ) -> PathAdmission {
        let (generation, _) = self.subflow_state_snapshot();
        self.commit_subflow_owner_admission_for_planner_generation(
            generation,
            self.lane_generation(),
            service,
            startup_owner_credit_bytes,
            optional_overhead_budget_bytes,
            max_read_gap_budget,
            input,
        )
    }

    #[cfg(test)]
    pub(in crate::runtime) fn commit_subflow_owner_admission_for_planner_generation(
        &self,
        expected_planner_generation: u64,
        expected_lane_generation: u64,
        service: CarrierPathKey,
        startup_owner_credit_bytes: usize,
        optional_overhead_budget_bytes: usize,
        max_read_gap_budget: Duration,
        input: SubflowAdmissionInput,
    ) -> PathAdmission {
        self.reserve_subflow_owner_admission_for_planner_generation(
            expected_planner_generation,
            expected_lane_generation,
            service,
            startup_owner_credit_bytes,
            optional_overhead_budget_bytes,
            max_read_gap_budget,
            input,
        )
        .admission
    }

    #[cfg(test)]
    pub(in crate::runtime) fn reserve_subflow_owner_admission_for_planner_generation(
        &self,
        expected_planner_generation: u64,
        expected_lane_generation: u64,
        service: CarrierPathKey,
        startup_owner_credit_bytes: usize,
        optional_overhead_budget_bytes: usize,
        max_read_gap_budget: Duration,
        input: SubflowAdmissionInput,
    ) -> ResponseSubflowAdmissionReservation {
        let request = ResponseSubflowAdmissionRequest {
            expected_planner_generation,
            expected_lane_generation,
            service,
            startup_owner_credit_bytes,
            optional_overhead_budget_bytes,
            max_read_gap_budget,
            input,
        };
        let standby = || ResponseSubflowAdmissionReservation {
            admission: PathAdmission::standby(),
            epoch_generation: None,
        };
        self.lane_tracker
            .with_matching_generation(self.session_id, expected_lane_generation, || {
                self.reserve_subflow_owner_admission_for_request(request)
            })
            .unwrap_or_else(standby)
    }

    fn reserve_subflow_owner_admission_for_request(
        &self,
        request: ResponseSubflowAdmissionRequest,
    ) -> ResponseSubflowAdmissionReservation {
        let standby = || ResponseSubflowAdmissionReservation {
            admission: PathAdmission::standby(),
            epoch_generation: None,
        };
        let mut state = self
            .subflow_set
            .lock()
            .expect("server reliable stream subflow set lock");
        if state.planner_generation != request.expected_planner_generation {
            return standby();
        }
        let envelope_changed = state.set.as_ref().is_some_and(|epoch| {
            !epoch.matches_envelope(
                request.service,
                request.startup_owner_credit_bytes,
                request.optional_overhead_budget_bytes,
                request.max_read_gap_budget,
            )
        });
        if envelope_changed {
            state.planner_generation = state.planner_generation.wrapping_add(1);
            state.epoch_generation = state.epoch_generation.wrapping_add(1);
            state.set = None;
        }
        let current = state.set.take();
        let mut epoch = Self::subflow_set_for(
            current,
            state.epoch_generation,
            request.service,
            request.startup_owner_credit_bytes,
            request.optional_overhead_budget_bytes,
            request.max_read_gap_budget,
        );
        let admission = epoch.admit_subflow_owner(request.input);
        state.set = epoch.has_members().then_some(epoch);
        ResponseSubflowAdmissionReservation {
            epoch_generation: (admission.decision == PathAdmissionDecision::AdmitSubflow)
                .then_some(state.epoch_generation),
            admission,
        }
    }

    pub(in crate::runtime) fn rollback_subflow_owner_admission_for_epoch(
        &self,
        expected_epoch_generation: u64,
        input: SubflowAdmissionInput,
    ) {
        let mut state = self
            .subflow_set
            .lock()
            .expect("server reliable stream subflow set lock");
        if state.epoch_generation == expected_epoch_generation
            && let Some(epoch) = state.set.as_mut()
        {
            epoch.rollback_subflow_owner(input);
        }
    }

    fn graduate_completed_response_startup_owner(&self) -> bool {
        let startup = self
            .subflow_set
            .lock()
            .expect("server reliable stream subflow set lock")
            .set
            .as_ref()
            .and_then(|epoch| {
                let owner = epoch.startup_owner_key()?;
                Some((owner, epoch.startup_owner_sealed_sample_bytes(owner)))
            });
        let Some((owner, sealed_sample_bytes)) = startup else {
            return false;
        };
        let lane = self.lane();

        // Owner enqueue holds the outputs lock from Subflow reservation through
        // flight recording. Keep it here so the no-flight proof and graduation
        // are one transition with respect to new response OwnerData.
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let owner_position = outputs.entries.iter().position(|entry| {
            if entry.key != owner
                || entry.role != StreamOpenRole::Validation
                || entry.commands.is_closed()
            {
                return false;
            }
            match entry.key.underlay {
                // Scheduler assignment is not a TCP send clock. Completing the
                // finite startup sample proves ownership/reachability and opens
                // prior selection or fallback measurement; only an exact ACK
                // clock may replace either temporary capacity value.
                UnderlayProtocol::Tcp => {
                    sealed_sample_bytes.is_some_and(|bytes| entry.owner_data_acked_bytes >= bytes)
                }
                // QUIC capacity is carrier-scoped and cannot be inferred from
                // product STREAM_ACK timing.
                UnderlayProtocol::Udp => {
                    server_output_has_bulk_rate_evidence_with_limits(entry, self.mux_limits)
                }
            }
        });
        let Some(owner_position) = owner_position else {
            return false;
        };
        let flights = self
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        if flights
            .values()
            .flatten()
            .any(|flight| flight.key == owner && flight.kind.is_ordering_owner())
        {
            return false;
        }

        let mut state = self
            .subflow_set
            .lock()
            .expect("server reliable stream subflow set lock");
        let service_key = state.set.as_ref().map(FlowSubflowSet::service_key);
        let graduated = state
            .set
            .as_mut()
            .is_some_and(|epoch| epoch.graduate_startup_owner(owner));
        if graduated {
            let owner_identity = (
                outputs.entries[owner_position].key,
                outputs.entries[owner_position].incarnation,
            );
            if owner_identity.0.underlay == UnderlayProtocol::Tcp {
                let service_capacity_prior_bps = if server_output_accepts_service_capacity_prior(
                    &outputs.entries[owner_position],
                ) {
                    service_key.and_then(|service_key| {
                        outputs
                            .entries
                            .iter()
                            .find(|entry| {
                                entry.key == service_key
                                    && entry.key.underlay == owner_identity.0.underlay
                                    && entry.role != StreamOpenRole::Repair
                                    && !entry.commands.is_closed()
                                    && server_output_has_bulk_rate_evidence_with_limits(
                                        entry,
                                        self.mux_limits,
                                    )
                            })
                            .map(|service| {
                                server_bulk_output_snapshot(
                                    service,
                                    self.session_id,
                                    lane,
                                    &self.lane_tracker,
                                    self.mux_limits,
                                    Instant::now(),
                                )
                                .delivery_rate_bps
                            })
                    })
                } else {
                    None
                };
                if let Some(rate_bps) = service_capacity_prior_bps {
                    // The exact startup sample already proved reachability and
                    // bounded ownership. Endpoint-only TCP has no independent
                    // carrier hint to preserve, so ordinary shared work can use
                    // the same typed Service opportunity that admitted a
                    // calibration seed. A fresh exact-ACK epoch replaces it.
                    let entry = &mut outputs.entries[owner_position];
                    entry.tcp_product_rate_evidence = None;
                    entry.tcp_ack_clock_rate_bps = None;
                    entry.tcp_capacity_prior = Some(TcpResponseCapacityPrior {
                        rate_bps,
                        ordinary_windows: 0,
                    });
                    entry.product_progress_rate_bps = Some(rate_bps);
                    entry.delivery_rate_bps = Some(rate_bps);
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "response_tcp_capacity_prior",
                        format_args!(
                            "phase=service_opportunity session_id={} binding_instance_id={} underlay={:?} path_id={} incarnation={} rate_bps={}",
                            self.session_id.0,
                            self.binding_instance_id,
                            owner_identity.0.underlay,
                            owner_identity.0.path_id.0,
                            owner_identity.1,
                            rate_bps,
                        ),
                    );
                } else {
                    let calibration_snapshot = server_bulk_output_snapshot(
                        &outputs.entries[owner_position],
                        self.session_id,
                        lane,
                        &self.lane_tracker,
                        self.mux_limits,
                        Instant::now(),
                    );
                    let initial_limit = reliable_tcp_ack_clock_calibration_initial_limit_bytes(
                        calibration_snapshot,
                        self.mux_limits,
                    );
                    let max_limit = reliable_ack_clock_calibration_ceiling_bytes(self.mux_limits);
                    if initial_limit > 0 && max_limit >= initial_limit {
                        let coverage_floor =
                            reliable_ack_clock_calibration_rate_coverage_floor_bytes(
                                self.mux_limits,
                            );
                        outputs
                            .ack_clock_calibrations
                            .entry(owner_identity)
                            .or_insert_with(|| {
                                ResponseAckClockCalibrationState::new_with_rate_coverage_floor(
                                    initial_limit,
                                    max_limit,
                                    coverage_floor,
                                )
                            });
                    }
                }
            }
            // Preserve the epoch and its measured members, but invalidate any
            // planner snapshot that still treats this output as the exclusive
            // unproven startup owner.
            state.planner_generation = state.planner_generation.wrapping_add(1);
        }
        graduated
    }

    fn reset_subflow_set_state(&self) {
        let mut state = self
            .subflow_set
            .lock()
            .expect("server reliable stream subflow set lock");
        state.planner_generation = state.planner_generation.wrapping_add(1);
        state.epoch_generation = state.epoch_generation.wrapping_add(1);
        state.set = None;
    }

    fn reset_subflow_set_with_outputs(&self, outputs: &mut ResponseStreamOutputs) {
        let active_calibration_has_owner_flights = outputs
            .active_ack_clock_calibration
            .is_some_and(|(active_key, active_incarnation)| {
                outputs
                    .ack_clock_calibrations
                    .contains_key(&(active_key, active_incarnation))
                    && outputs.entries.iter().any(|entry| {
                        entry.key == active_key && entry.incarnation == active_incarnation
                    })
                    && self
                        .flights
                        .lock()
                        .expect("server reliable stream flight lock")
                        .values()
                        .flatten()
                        .any(|flight| {
                            flight.key == active_key
                                && flight.output_incarnation == active_incarnation
                                && flight.kind.is_ordering_owner()
                        })
            });
        for calibration in outputs.ack_clock_calibrations.values_mut() {
            if !calibration.proven {
                calibration.retire();
            }
        }
        if !active_calibration_has_owner_flights {
            outputs.active_ack_clock_calibration = None;
        }
        self.reset_subflow_set_state();
    }

    #[cfg(test)]
    pub(in crate::runtime) fn reset_subflow_set(&self) {
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        self.reset_subflow_set_with_outputs(&mut outputs);
    }

    fn invalidate_subflow_plan(&self) {
        let mut state = self
            .subflow_set
            .lock()
            .expect("server reliable stream subflow set lock");
        state.planner_generation = state.planner_generation.wrapping_add(1);
    }

    fn repair_ordered_data_owner_after_output_change(
        &self,
        live_entries: &[ResponseStreamOutputEntry],
    ) {
        let mut lead = self
            .ordered_data_owner
            .lock()
            .expect("server reliable stream ordered data owner lock");
        let live_lead = lead.is_some_and(|key| live_entries.iter().any(|entry| entry.key == key));
        let cleared = !live_lead && lead.is_some();
        if !live_lead {
            *lead = None;
        }
        if cleared {
            self.response_flow_registration.set_service(None);
        }
        drop(lead);
    }

    #[cfg(test)]
    pub(in crate::runtime) fn record_owner_flight_for_target(
        &self,
        target: &ResponseSenderPathTarget,
        frame: &Frame,
    ) {
        self.record_product_flight(
            target.key,
            target.incarnation,
            target.attachment_role,
            &target.commands,
            frame,
            CarrierWorkKind::OwnerData,
        )
    }

    pub(in crate::runtime) fn try_retire_tcp_ack_clock_calibration(
        &self,
        request: ResponseAckClockCalibrationRetirementRequest,
    ) -> bool {
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let mut retired = None;
        let applied = self
            .lane_tracker
            .with_matching_generation_and_min_active_response_flows(
                self.session_id,
                request.expected_lane_generation,
                MIN_ACTIVE_RESPONSE_FLOWS_FOR_SAME_FAMILY_DISCOVERY,
                || {
                    if self.response_model_generation.load(Ordering::Acquire)
                        != request.expected_model_generation
                    {
                        return false;
                    }
                    let mut subflow_state = self
                        .subflow_set
                        .lock()
                        .expect("server reliable stream subflow set lock");
                    if subflow_state.planner_generation != request.expected_planner_generation
                        || subflow_state.set.as_ref().is_none_or(|epoch| {
                            epoch.service_key() != request.service
                                || epoch.startup_owner_key().is_some()
                        })
                    {
                        return false;
                    }
                    let service_is_exact_and_proven = outputs.entries.iter().any(|entry| {
                        entry.key == request.service
                            && entry.incarnation == request.service_incarnation
                            && entry.role != StreamOpenRole::Repair
                            && !entry.commands.is_closed()
                            && entry.commands.pending_bytes() == request.service_pending_bytes
                            && entry.key.underlay == UnderlayProtocol::Tcp
                            && server_output_has_bulk_rate_evidence_with_limits(
                                entry,
                                self.mux_limits,
                            )
                    });
                    let target_is_exact_and_drained = outputs.entries.iter().any(|entry| {
                        entry.key == request.target
                            && entry.incarnation == request.target_incarnation
                            && entry.role == StreamOpenRole::Validation
                            && !entry.commands.is_closed()
                            && entry.commands.pending_bytes() == request.target_pending_bytes
                            && entry.key.underlay == UnderlayProtocol::Tcp
                            && entry.key.underlay == request.service.underlay
                            // RepairData may remain as carrier pressure, but it
                            // cannot preserve a unique OwnerData policy fence.
                            && entry.owner_data_in_flight_bytes == 0
                    });
                    let identity = (request.target, request.target_incarnation);
                    if !service_is_exact_and_proven
                        || !target_is_exact_and_drained
                        || outputs.active_ack_clock_calibration.is_some()
                    {
                        return false;
                    }
                    let flights = self
                        .flights
                        .lock()
                        .expect("server reliable stream flight lock");
                    let has_exact_owner_flight = flights.values().flatten().any(|flight| {
                        flight.key == request.target
                            && flight.output_incarnation == request.target_incarnation
                            && flight.kind.is_ordering_owner()
                    });
                    if has_exact_owner_flight {
                        return false;
                    }
                    drop(flights);

                    let Some(calibration) = outputs.ack_clock_calibrations.get_mut(&identity)
                    else {
                        return false;
                    };
                    if calibration.proven
                        || calibration.retired
                        || calibration.spent_bytes != 0
                        || calibration.credit_limit_bytes != request.limit_bytes
                        || calibration.credit_limit_bytes > calibration.max_limit_bytes
                    {
                        return false;
                    }
                    calibration.retire();
                    retired = Some(*calibration);
                    subflow_state.planner_generation =
                        subflow_state.planner_generation.wrapping_add(1);
                    true
                },
            )
            .unwrap_or(false);
        drop(outputs);
        if !applied {
            return false;
        }
        #[cfg(feature = "lab-diagnostics")]
        if let Some(calibration) = retired {
            lab_diagnostic(
                "response_ack_clock_calibration",
                format_args!(
                    "phase=terminal session_id={} binding_instance_id={} underlay={:?} path_id={} incarnation={} reason=completion_horizon active_owner_flights=false calibrated_rate_ready=false calibrated_rate_bps=0 spent_bytes={} previous_credit_limit_bytes={} credit_limit_bytes={} max_limit_bytes={} stage_authorized_spent_bytes={} stage_credit_bytes={} stage_strict_capacity_bytes={} stage_evidence_bytes={} stage_rate_ineligible_bytes={} proven={} retired={}",
                    self.session_id.0,
                    self.binding_instance_id,
                    request.target.underlay,
                    request.target.path_id.0,
                    request.target_incarnation,
                    calibration.spent_bytes,
                    request.limit_bytes,
                    calibration.credit_limit_bytes,
                    calibration.max_limit_bytes,
                    calibration.stage_authorized_spent_bytes,
                    calibration.stage_credit_bytes(),
                    calibration.stage_strict_capacity_bytes(),
                    calibration.stage_rate_evidence_bytes,
                    calibration.stage_rate_ineligible_bytes,
                    calibration.proven,
                    calibration.retired,
                ),
            );
        }
        #[cfg(not(feature = "lab-diagnostics"))]
        let _ = retired;
        self.notify_update();
        true
    }

    #[cfg(test)]
    pub(in crate::runtime) fn try_enqueue_owner_frame_for_target(
        &self,
        target: &ResponseSenderPathTarget,
        frame: &Frame,
        lane: FlowLane,
        subflow_request: Option<ResponseSubflowAdmissionRequest>,
        calibration_request: Option<ResponseAckClockCalibrationRequest>,
    ) -> Result<Option<u64>, RuntimeError> {
        self.try_enqueue_owner_frame_for_dispatch_target(
            &target.into(),
            frame,
            lane,
            subflow_request,
            calibration_request,
        )
    }

    pub(in crate::runtime) fn try_enqueue_owner_frame_for_dispatch_target(
        &self,
        target: &ResponseDispatchTarget,
        frame: &Frame,
        lane: FlowLane,
        subflow_request: Option<ResponseSubflowAdmissionRequest>,
        calibration_request: Option<ResponseAckClockCalibrationRequest>,
    ) -> Result<Option<u64>, RuntimeError> {
        self.try_enqueue_owner_frame_for_target_inner(
            target,
            frame,
            lane,
            subflow_request,
            calibration_request,
            || {},
        )
    }

    fn try_enqueue_owner_frame_for_target_inner(
        &self,
        target: &ResponseDispatchTarget,
        frame: &Frame,
        lane: FlowLane,
        subflow_request: Option<ResponseSubflowAdmissionRequest>,
        calibration_request: Option<ResponseAckClockCalibrationRequest>,
        after_subflow_reservation: impl FnOnce(),
    ) -> Result<Option<u64>, RuntimeError> {
        if !self.response_stream_open.load(Ordering::Acquire) {
            return Err(RuntimeError::SenderServiceBlocked);
        }
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        if !self.response_stream_open.load(Ordering::Acquire) {
            return Err(RuntimeError::SenderServiceBlocked);
        }
        let target_matches = |entry: &ResponseStreamOutputEntry| {
            entry.key == target.key
                && entry.incarnation == target.incarnation
                && entry.commands.same_channel(&target.commands)
                && entry.role == target.attachment_role
                && entry.role != StreamOpenRole::Repair
        };
        let target_index = outputs
            .entries
            .last()
            .filter(|entry| target_matches(entry))
            .map(|_| outputs.entries.len() - 1)
            .or_else(|| outputs.entries.iter().position(target_matches));
        let Some(target_index) = target_index else {
            return Err(RuntimeError::SenderServiceBlocked);
        };
        if subflow_request.is_some() && calibration_request.is_some() {
            return Err(RuntimeError::SenderServiceBlocked);
        }
        if let Some(request) = calibration_request {
            let Some((_, _, payload_bytes)) = reliable_stream_frame_extent(frame) else {
                return Err(RuntimeError::SenderServiceBlocked);
            };
            let calibration_ceiling = reliable_ack_clock_calibration_ceiling_bytes(self.mux_limits);
            let calibration_limit = request.limit_bytes.min(calibration_ceiling);
            return self
                .lane_tracker
                .with_matching_generation_and_min_active_response_flows(
                    self.session_id,
                    request.expected_lane_generation,
                    if request.requires_active_response_start {
                        MIN_ACTIVE_RESPONSE_FLOWS_FOR_SAME_FAMILY_DISCOVERY
                    } else {
                        0
                    },
                    || {
                    {
                        if self.response_model_generation.load(Ordering::Acquire)
                            != request.expected_model_generation
                        {
                            return Err(RuntimeError::SenderServiceBlocked);
                        }
                        let state = self
                            .subflow_set
                            .lock()
                            .expect("server reliable stream subflow set lock");
                        if state.planner_generation != request.expected_planner_generation
                            || state.set.as_ref().is_none_or(|epoch| {
                                epoch.service_key() != request.service
                                    || epoch.startup_owner_key().is_some()
                            })
                        {
                            return Err(RuntimeError::SenderServiceBlocked);
                        }
                    }
                    let service_is_exact_and_proven = outputs.entries.iter().any(|entry| {
                        entry.key == request.service
                            && entry.incarnation == request.service_incarnation
                            && entry.role != StreamOpenRole::Repair
                            && !entry.commands.is_closed()
                            && entry.commands.pending_bytes() == request.service_pending_bytes
                            && entry.key.underlay == UnderlayProtocol::Tcp
                            && server_output_has_bulk_rate_evidence_with_limits(
                                entry,
                                self.mux_limits,
                            )
                    });
                    let target_entry = &outputs.entries[target_index];
                    let identity = (target_entry.key, target_entry.incarnation);
                    let target_is_tcp_validation = target_entry.role == StreamOpenRole::Validation
                        && target_entry.key.underlay == UnderlayProtocol::Tcp
                        && target_entry.key.underlay == request.service.underlay
                        && !target_entry.commands.is_closed()
                        && target_entry.commands.pending_bytes() == request.target_pending_bytes;
                    // The product-flight ledger already includes frames that
                    // remain pending in the carrier command pipe.
                    let target_has_calibration_headroom = target_entry
                        .bytes_in_flight
                        .max(target_entry.commands.pending_bytes())
                        .saturating_add(payload_bytes as u64)
                        <= calibration_limit;
                    let active_matches = outputs
                        .active_ack_clock_calibration
                        .is_none_or(|active| active == identity);
                    let calibration_is_available = outputs
                        .ack_clock_calibrations
                        .get(&identity)
                        .is_some_and(|calibration| {
                            !calibration.proven
                                && request.limit_bytes == calibration.credit_limit_bytes
                                && calibration.credit_limit_bytes <= calibration.max_limit_bytes
                                && calibration.max_limit_bytes <= calibration_ceiling
                                && calibration
                                    .spent_bytes
                                    .saturating_add(payload_bytes as u64)
                                    <= calibration_limit
                        });
                    if !service_is_exact_and_proven
                        || !target_is_tcp_validation
                        || !target_has_calibration_headroom
                        || !active_matches
                        || !calibration_is_available
                    {
                        return Err(RuntimeError::SenderServiceBlocked);
                    }

                    let previous_active = outputs.active_ack_clock_calibration;
                    let previous_calibration = *outputs
                        .ack_clock_calibrations
                        .get(&identity)
                        .expect("validated response calibration identity");
                    let reserved_calibration = {
                        let calibration = outputs
                        .ack_clock_calibrations
                        .get_mut(&identity)
                        .expect("validated response calibration identity");
                        calibration.spent_bytes = calibration
                            .spent_bytes
                            .saturating_add(payload_bytes as u64);
                        *calibration
                    };
                    #[cfg(not(feature = "lab-diagnostics"))]
                    let _ = reserved_calibration;
                    outputs.active_ack_clock_calibration = Some(identity);
                    if let Err(err) = target
                        .commands
                        .try_enqueue_stream_ordered_frame(frame.clone(), lane)
                    {
                        *outputs
                            .ack_clock_calibrations
                            .get_mut(&identity)
                            .expect("reserved response calibration identity") =
                            previous_calibration;
                        outputs.active_ack_clock_calibration = previous_active;
                        return Err(err);
                    }
                    self.record_validated_owner_flight_with_outputs(
                        &mut outputs,
                        target_index,
                        frame,
                    );
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "response_ack_clock_calibration",
                        format_args!(
                            "phase=selected session_id={} binding_instance_id={} underlay={:?} path_id={} incarnation={} payload_bytes={} spent_bytes={} credit_limit_bytes={} max_limit_bytes={} proven={}",
                            self.session_id.0,
                            self.binding_instance_id,
                            identity.0.underlay,
                            identity.0.path_id.0,
                            identity.1,
                            payload_bytes,
                            reserved_calibration.spent_bytes,
                            reserved_calibration.credit_limit_bytes,
                            reserved_calibration.max_limit_bytes,
                            reserved_calibration.proven,
                        ),
                    );
                    Ok(None)
                    },
                )
                .unwrap_or(Err(RuntimeError::SenderServiceBlocked));
        }
        if let Some(request) = subflow_request {
            return self
                .lane_tracker
                .with_matching_generation(self.session_id, request.expected_lane_generation, || {
                    let reservation = self.reserve_subflow_owner_admission_for_request(request);
                    if reservation.admission.decision != PathAdmissionDecision::AdmitSubflow {
                        return Err(RuntimeError::SenderServiceBlocked);
                    }
                    after_subflow_reservation();
                    if let Err(err) = target
                        .commands
                        .try_enqueue_stream_ordered_frame(frame.clone(), lane)
                    {
                        if let Some(epoch_generation) = reservation.epoch_generation {
                            self.rollback_subflow_owner_admission_for_epoch(
                                epoch_generation,
                                request.input,
                            );
                        }
                        return Err(err);
                    }
                    self.record_validated_owner_flight_with_outputs(
                        &mut outputs,
                        target_index,
                        frame,
                    );
                    Ok(reservation.epoch_generation)
                })
                .unwrap_or(Err(RuntimeError::SenderServiceBlocked));
        }
        target
            .commands
            .try_enqueue_stream_ordered_frame(frame.clone(), lane)?;
        self.record_validated_owner_flight_with_outputs(&mut outputs, target_index, frame);
        Ok(None)
    }

    fn record_validated_owner_flight_with_outputs(
        &self,
        outputs: &mut ResponseStreamOutputs,
        target_index: usize,
        frame: &Frame,
    ) {
        let Some((offset, end, bytes)) = reliable_stream_frame_extent(frame) else {
            return;
        };
        let (key, output_incarnation) = {
            let entry = outputs
                .entries
                .get_mut(target_index)
                .expect("validated response output index");
            debug_assert_ne!(entry.role, StreamOpenRole::Repair);
            entry.owner_data_in_flight_bytes = entry
                .owner_data_in_flight_bytes
                .saturating_add(bytes as u64);
            entry.bytes_in_flight = entry.bytes_in_flight.saturating_add(bytes as u64);
            (entry.key, entry.incarnation)
        };
        self.flights
            .lock()
            .expect("server reliable stream flight lock")
            .entry(offset)
            .or_default()
            .push(CarrierPathFlight {
                key,
                output_incarnation,
                end,
                bytes,
                sent_at: Instant::now(),
                kind: CarrierWorkKind::OwnerData,
                evidence_eligible: true,
            });
        // Keep path counters and the exact range ledger in one published model
        // generation so a concurrent calibration plan cannot mix the views.
        self.response_model_generation
            .fetch_add(1, Ordering::AcqRel);
    }

    pub(in crate::runtime) fn try_enqueue_repair_frame_for_target(
        &self,
        target: &ResponseSenderPathTarget,
        frame: &Frame,
        lane: FlowLane,
    ) -> Result<(), RuntimeError> {
        if !self.response_stream_open.load(Ordering::Acquire) {
            return Err(RuntimeError::SenderServiceBlocked);
        }
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        if !self.response_stream_open.load(Ordering::Acquire) {
            return Err(RuntimeError::SenderServiceBlocked);
        }
        let target_matches = outputs.entries.iter().any(|entry| {
            entry.key == target.key
                && entry.incarnation == target.incarnation
                && entry.commands.same_channel(&target.commands)
                && entry.role == target.attachment_role
        });
        if !target_matches {
            return Err(RuntimeError::SenderServiceBlocked);
        }
        target
            .commands
            .try_enqueue_admitted_frame(frame.clone(), lane)?;
        self.record_product_flight_with_outputs(
            &mut outputs,
            target.key,
            target.incarnation,
            target.attachment_role,
            &target.commands,
            frame,
            CarrierWorkKind::RepairData,
        );
        Ok(())
    }

    #[cfg(test)]
    pub(in crate::runtime) fn record_owner_flight(&self, key: CarrierPathKey, frame: &Frame) {
        let (incarnation, role, commands) = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock")
            .entries
            .iter()
            .find(|entry| entry.key == key)
            .map(|entry| (entry.incarnation, entry.role, entry.commands.clone()))
            .expect("test owner output must be attached");
        self.record_product_flight(
            key,
            incarnation,
            role,
            &commands,
            frame,
            CarrierWorkKind::OwnerData,
        )
    }

    #[cfg(test)]
    pub(in crate::runtime) fn record_repair_flight(&self, key: CarrierPathKey, frame: &Frame) {
        let (incarnation, role, commands) = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock")
            .entries
            .iter()
            .find(|entry| entry.key == key)
            .map(|entry| (entry.incarnation, entry.role, entry.commands.clone()))
            .expect("test repair output must be attached");
        self.record_product_flight(
            key,
            incarnation,
            role,
            &commands,
            frame,
            CarrierWorkKind::RepairData,
        )
    }

    #[cfg(test)]
    pub(in crate::runtime) fn age_repair_flights_for_test(&self, age: Duration) {
        let sent_at = Instant::now().checked_sub(age).unwrap_or_else(Instant::now);
        let mut flights = self
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        for path_flights in flights.values_mut() {
            for flight in path_flights {
                if flight.kind == CarrierWorkKind::RepairData {
                    flight.sent_at = sent_at;
                }
            }
        }
    }

    #[cfg(test)]
    fn record_product_flight(
        &self,
        key: CarrierPathKey,
        output_incarnation: u64,
        planned_role: StreamOpenRole,
        planned_commands: &ReliablePathCommandSender,
        frame: &Frame,
        kind: CarrierWorkKind,
    ) {
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        self.record_product_flight_with_outputs(
            &mut outputs,
            key,
            output_incarnation,
            planned_role,
            planned_commands,
            frame,
            kind,
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn record_product_flight_with_outputs(
        &self,
        outputs: &mut ResponseStreamOutputs,
        key: CarrierPathKey,
        output_incarnation: u64,
        planned_role: StreamOpenRole,
        planned_commands: &ReliablePathCommandSender,
        frame: &Frame,
        kind: CarrierWorkKind,
    ) {
        debug_assert!(kind.carries_product_offsets());
        let Some((offset, end, bytes)) = reliable_stream_frame_extent(frame) else {
            return;
        };
        let (recorded_incarnation, evidence_eligible) = if let Some(entry) = outputs
            .entries
            .iter_mut()
            .find(|entry| entry.key == key && entry.commands.same_channel(planned_commands))
        {
            let incarnation_matches = entry.incarnation == output_incarnation;
            let role_matches = entry.role == planned_role;
            if kind.is_ordering_owner() {
                entry.owner_data_in_flight_bytes = entry
                    .owner_data_in_flight_bytes
                    .saturating_add(bytes as u64);
            }
            entry.bytes_in_flight = entry.bytes_in_flight.saturating_add(bytes as u64);
            (
                entry.incarnation,
                incarnation_matches && role_matches && planned_role != StreamOpenRole::Repair,
            )
        } else {
            (output_incarnation, false)
        };
        self.flights
            .lock()
            .expect("server reliable stream flight lock")
            .entry(offset)
            .or_default()
            .push(CarrierPathFlight {
                key,
                output_incarnation: recorded_incarnation,
                end,
                bytes,
                sent_at: Instant::now(),
                kind,
                evidence_eligible,
            });
        // The generation becomes visible only after the matching exact range.
        self.response_model_generation
            .fetch_add(1, Ordering::AcqRel);
    }

    fn invalidate_path_flight_evidence(&self, key: CarrierPathKey, output_incarnation: u64) {
        let mut flights = self
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        for path_flights in flights.values_mut() {
            for flight in path_flights.iter_mut().filter(|flight| {
                flight.key == key && flight.output_incarnation == output_incarnation
            }) {
                flight.evidence_eligible = false;
            }
        }
    }

    fn rebind_path_flights_after_live_role_change(
        &self,
        key: CarrierPathKey,
        previous_incarnation: u64,
        current_incarnation: u64,
    ) {
        let mut flights = self
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        for path_flights in flights.values_mut() {
            for flight in path_flights.iter_mut().filter(|flight| {
                flight.key == key && flight.output_incarnation == previous_incarnation
            }) {
                flight.output_incarnation = current_incarnation;
                flight.evidence_eligible = false;
            }
        }
    }

    pub(in crate::runtime) fn lower_flights_before_frame(
        &self,
        frame: &Frame,
    ) -> Vec<CarrierPathFlightDebt> {
        let Some((offset, _, _)) = reliable_stream_frame_extent(frame) else {
            return Vec::new();
        };
        self.lower_flights_before_offset(offset)
    }

    pub(in crate::runtime) fn flight_keys_overlapping_frame(
        &self,
        frame: &Frame,
    ) -> Vec<CarrierPathKey> {
        let Some((start, end, _)) = reliable_stream_frame_extent(frame) else {
            return Vec::new();
        };
        let flights = self
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        let mut keys = Vec::new();
        for (_, path_flights) in flights.range(..end) {
            for flight in path_flights {
                if flight.end <= start || keys.contains(&flight.key) {
                    continue;
                }
                keys.push(flight.key);
            }
        }
        keys
    }

    pub(in crate::runtime) fn owner_flight_keys_overlapping_frame(
        &self,
        frame: &Frame,
    ) -> Vec<CarrierPathKey> {
        let Some((start, end, _)) = reliable_stream_frame_extent(frame) else {
            return Vec::new();
        };
        let flights = self
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        let mut keys = Vec::new();
        for (_, path_flights) in flights.range(..end) {
            for flight in path_flights {
                if flight.end <= start
                    || !flight.kind.is_ordering_owner()
                    || keys.contains(&flight.key)
                {
                    continue;
                }
                keys.push(flight.key);
            }
        }
        keys
    }

    pub(in crate::runtime) fn lower_flights_before_offset(
        &self,
        offset: u64,
    ) -> Vec<CarrierPathFlightDebt> {
        let mut debts = BTreeMap::<u64, CarrierPathFlightDebt>::new();
        {
            let ack_ordering = self
                .ack_ordering
                .lock()
                .expect("server response ACK ordering lock");
            for (hole_offset, holes) in ack_ordering.acked_holes.range(..offset) {
                if let Some(latest) = response_latest_ordering_hole(holes) {
                    debts.insert(
                        *hole_offset,
                        CarrierPathFlightDebt {
                            key: latest.key,
                            bytes: latest.bytes,
                        },
                    );
                }
            }
        }
        debts.into_values().collect()
    }

    fn blocking_owner_key_at_or_after(&self, offset: u64) -> Option<CarrierPathKey> {
        let flights = self
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        for path_flights in flights.values() {
            for flight in path_flights {
                if flight.kind.is_ordering_owner() && flight.end > offset {
                    return Some(flight.key);
                }
            }
        }
        None
    }

    #[cfg(feature = "lab-diagnostics")]
    fn lab_response_service_feed_state(
        &self,
        entry: &ResponseStreamOutputEntry,
        snapshot: PathSnapshot,
        lane: FlowLane,
        is_active: bool,
        has_bulk_rate_evidence: bool,
        has_service_feed_evidence: bool,
        command_pending_bytes: u64,
    ) {
        if !lab_diagnostic_event_enabled("response_service_feed_state") {
            return;
        }

        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(self.mux_limits);
        let startup_floor = reliable_subflow_startup_sample_limit_bytes(self.mux_limits);
        let service_floor =
            bulk_service_horizon_payload_bytes(payload_bytes, self.mux_limits) as u64;
        let progress_bucket = |bytes: u64| {
            if bytes == 0 {
                0
            } else if bytes < startup_floor {
                1
            } else if bytes < service_floor {
                2
            } else {
                3
            }
        };
        let local_metrics = entry
            .local_path_metrics
            .as_ref()
            .filter(|metrics| metrics.source == ServerPathMetricsSource::LocalSender)
            .map(|metrics| metrics.metrics);
        let carrier_sample_bytes = local_metrics.map_or(0, |metrics| metrics.data_sample_bytes);
        let carrier_sample_count = local_metrics.map_or(0, |metrics| metrics.data_sample_count);
        let carrier_sample_available =
            local_metrics.is_some_and(|metrics| metrics.has_ack_derived_data_sample);
        let carrier_app_limited = local_metrics.is_none_or(|metrics| metrics.app_limited);
        let latency_pressure = snapshot.active_latency_sensitive_flows > 0
            || snapshot.session_active_latency_sensitive_flows > 0;
        let source_limit_bytes = if latency_pressure {
            service_floor
        } else if has_service_feed_evidence {
            bulk_active_service_product_envelope_bytes(snapshot, payload_bytes, self.mux_limits)
        } else {
            bulk_service_feed_reservoir_payload_bytes(payload_bytes, self.mux_limits) as u64
        };
        let emission_limit_bytes = if !has_service_feed_evidence {
            if entry.key.underlay == UnderlayProtocol::Udp {
                bulk_service_feed_reservoir_payload_bytes(payload_bytes, self.mux_limits) as u64
            } else {
                service_floor
            }
        } else if latency_pressure {
            bulk_latency_pressure_service_feed_window_bytes(payload_bytes, self.mux_limits)
        } else {
            bulk_active_service_product_envelope_bytes(snapshot, payload_bytes, self.mux_limits)
        };
        let state = ResponseServiceFeedDiagnosticState {
            path_instance_id: entry.path_instance_id,
            attachment_role: entry.role,
            is_active,
            has_bulk_rate_evidence,
            has_service_feed_evidence,
            owner_progress_bucket: progress_bucket(entry.owner_data_acked_bytes),
            product_rate_available: entry.product_progress_rate_bps.is_some(),
            carrier_sample_bucket: progress_bucket(carrier_sample_bytes),
            carrier_sample_available,
            carrier_app_limited,
            latency_pressure,
            source_limit_bytes,
            emission_limit_bytes,
        };
        let identity = (entry.key, entry.incarnation);
        let mut previous = self
            .response_service_feed_diagnostic
            .lock()
            .expect("response Service-feed diagnostic lock");
        if previous.get(&identity) == Some(&state) {
            return;
        }
        previous.insert(identity, state);
        drop(previous);

        lab_diagnostic(
            "response_service_feed_state",
            format_args!(
                "session_id={} binding_instance_id={} path_underlay={:?} path_id={} path_instance_id={} incarnation={} attachment_role={:?} lane={:?} is_active={} latency_pressure={} owner_data_acked_bytes={} owner_progress_bucket={} product_progress_rate_mbps={:.3} product_rate_available={} carrier_sample_bytes={} carrier_sample_count={} carrier_sample_available={} carrier_app_limited={} startup_floor_bytes={} service_floor_bytes={} bulk_rate_evidence={} service_feed_evidence={} source_limit_bytes={} emission_limit_bytes={} command_pending_bytes={} owner_data_inflight_bytes={} product_inflight_bytes={} product_queue_bytes={} path_queue_bytes={}",
                self.session_id.0,
                self.binding_instance_id,
                entry.key.underlay,
                entry.key.path_id.0,
                entry.path_instance_id.as_u64(),
                entry.incarnation,
                entry.role,
                lane,
                is_active,
                latency_pressure,
                entry.owner_data_acked_bytes,
                state.owner_progress_bucket,
                entry.product_progress_rate_bps.unwrap_or(0.0) / 1_000_000.0,
                state.product_rate_available,
                carrier_sample_bytes,
                carrier_sample_count,
                carrier_sample_available,
                carrier_app_limited,
                startup_floor,
                service_floor,
                has_bulk_rate_evidence,
                has_service_feed_evidence,
                source_limit_bytes,
                emission_limit_bytes,
                command_pending_bytes,
                entry.owner_data_in_flight_bytes,
                snapshot.product_bytes_in_flight,
                snapshot.product_queue_bytes,
                snapshot.queue_bytes,
            ),
        );
    }

    pub(in crate::runtime) fn sender_path_targets(
        &self,
        lane: FlowLane,
        payload_bytes: usize,
    ) -> Vec<ResponseSenderPathTarget> {
        let stored_active_key = self.ordered_data_owner();
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let request_active_key = *self
            .request_active_owner
            .lock()
            .expect("server reliable stream request active owner lock");
        let active_key = response_live_ordered_data_owner(stored_active_key, &outputs.entries);
        let now = Instant::now();
        let response_scheduling = self.lane_tracker.response_path_scheduling_snapshots(
            self.session_id,
            outputs
                .entries
                .iter()
                .map(|entry| (entry.key, entry.path_instance_id)),
        );
        outputs
            .entries
            .iter()
            .zip(response_scheduling)
            .map(|(entry, response_scheduling)| {
                let command_pending_bytes = entry.commands.pending_bytes();
                let calibration_identity = (entry.key, entry.incarnation);
                let calibration = outputs
                    .ack_clock_calibrations
                    .get(&calibration_identity)
                    .copied();
                let response_snapshot = server_bulk_output_snapshot_with_scheduling(
                    entry,
                    lane,
                    self.mux_limits,
                    now,
                    command_pending_bytes,
                    response_scheduling,
                );
                let snapshot = response_snapshot.path;
                let is_active = Some(entry.key) == active_key;
                let has_bulk_rate_evidence =
                    server_output_has_bulk_rate_evidence_with_limits(entry, self.mux_limits);
                let has_service_feed_evidence = has_bulk_rate_evidence
                    || (is_active
                        && server_output_has_service_feed_evidence_with_limits(
                            entry,
                            self.mux_limits,
                        ));
                let endpoint_only_service_prior_eligible =
                    server_output_accepts_service_capacity_prior(entry);
                #[cfg(feature = "lab-diagnostics")]
                self.lab_response_service_feed_state(
                    entry,
                    snapshot,
                    lane,
                    is_active,
                    has_bulk_rate_evidence,
                    has_service_feed_evidence,
                    command_pending_bytes,
                );
                ResponseSenderPathTarget {
                    #[cfg(feature = "lab-diagnostics")]
                    session_id: self.session_id,
                    #[cfg(feature = "lab-diagnostics")]
                    binding_instance_id: self.binding_instance_id,
                    key: entry.key,
                    path_instance_id: entry.path_instance_id,
                    incarnation: entry.incarnation,
                    commands: entry.commands.clone(),
                    attachment_role: entry.role,
                    snapshot,
                    owner_data_in_flight_bytes: entry.owner_data_in_flight_bytes,
                    command_pending_bytes,
                    eta_ms: server_bulk_output_eta_ms(
                        entry.key,
                        snapshot,
                        active_key,
                        lane,
                        payload_bytes,
                        self.mux_limits,
                    ),
                    is_active,
                    is_request_active: Some(entry.key) == request_active_key,
                    has_sender_evidence: server_output_has_sender_evidence(entry),
                    has_service_feed_evidence,
                    has_bulk_rate_evidence,
                    endpoint_only_service_prior_eligible,
                    quic_capacity_proof: server_output_quic_capacity_proof_marker(entry),
                    quic_capacity_calibration_attempts: response_snapshot
                        .quic_capacity_calibration_attempts,
                    ack_clock_calibration_eligible: calibration.is_some(),
                    ack_clock_calibration_proven: calibration
                        .is_some_and(|calibration| calibration.proven),
                    ack_clock_calibration_spent_bytes: calibration
                        .map_or(0, |calibration| calibration.spent_bytes),
                    ack_clock_calibration_credit_limit_bytes: calibration
                        .map_or(0, |calibration| calibration.credit_limit_bytes),
                    ack_clock_calibration_max_limit_bytes: calibration
                        .map_or(0, |calibration| calibration.max_limit_bytes),
                    ack_clock_calibration_active: outputs.active_ack_clock_calibration
                        == Some(calibration_identity),
                }
            })
            .collect()
    }

    pub(in crate::runtime) fn mux_limits(&self) -> MuxLimits {
        self.mux_limits
    }

    pub(in crate::runtime) fn active_tcp_ack_clock_calibration_remaining_bytes(
        &self,
    ) -> Option<usize> {
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let identity = outputs.active_ack_clock_calibration?;
        if identity.0.underlay != UnderlayProtocol::Tcp {
            return None;
        }
        let calibration = outputs.ack_clock_calibrations.get(&identity)?;
        if calibration.proven || calibration.retired {
            return None;
        }
        let remaining = calibration
            .credit_limit_bytes
            .saturating_sub(calibration.spent_bytes);
        (remaining > 0).then(|| usize::try_from(remaining).unwrap_or(usize::MAX))
    }

    #[cfg(test)]
    pub(in crate::runtime) fn mark_output_bulk_proven_for_test(&self, key: CarrierPathKey) {
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let entry = outputs
            .entries
            .iter_mut()
            .find(|entry| entry.key == key)
            .expect("test bulk-proven output");
        entry.product_progress_rate_bps = Some(100_000_000.0);
        entry.delivery_rate_bps = Some(100_000_000.0);
        entry.delivery_samples = 1;
        entry.owner_data_acked_bytes = reliable_subflow_startup_sample_limit_bytes(self.mux_limits);
        self.response_model_generation
            .fetch_add(1, Ordering::AcqRel);
    }

    #[cfg(test)]
    pub(in crate::runtime) fn set_output_product_model_for_test(
        &self,
        key: CarrierPathKey,
        rate_bps: f64,
        srtt_ms: f64,
    ) {
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let entry = outputs
            .entries
            .iter_mut()
            .find(|entry| entry.key == key)
            .expect("test modeled output");
        entry.product_progress_rate_bps = Some(rate_bps.max(1.0));
        entry.delivery_rate_bps = Some(rate_bps.max(1.0));
        entry.srtt_ms = Some(srtt_ms.max(1.0));
        self.response_model_generation
            .fetch_add(1, Ordering::AcqRel);
    }

    #[cfg(test)]
    pub(in crate::runtime) fn install_tcp_ack_clock_calibration_for_test(
        &self,
        key: CarrierPathKey,
        spent_bytes: u64,
        credit_limit_bytes: u64,
        max_limit_bytes: u64,
        active: bool,
    ) {
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let entry = outputs
            .entries
            .iter()
            .find(|entry| entry.key == key)
            .expect("test calibration output");
        assert_eq!(entry.key.underlay, UnderlayProtocol::Tcp);
        let identity = (entry.key, entry.incarnation);
        let mut calibration =
            ResponseAckClockCalibrationState::new(credit_limit_bytes, max_limit_bytes);
        calibration.spent_bytes = spent_bytes;
        outputs.ack_clock_calibrations.insert(identity, calibration);
        if active {
            outputs.active_ack_clock_calibration = Some(identity);
        } else if outputs.active_ack_clock_calibration == Some(identity) {
            outputs.active_ack_clock_calibration = None;
        }
    }

    pub(in crate::runtime) fn session_id(&self) -> SessionId {
        self.session_id
    }

    pub(in crate::runtime) async fn close_stream(&self, stream_id: StreamId) {
        let outputs = {
            let outputs = self
                .outputs
                .lock()
                .expect("server reliable stream binding lock");
            self.response_stream_open.store(false, Ordering::Release);
            outputs.entries.clone()
        };
        self.response_flow_registration.set_active(false);
        self.lane_tracker
            .clear_quic_capacity_calibration_for_binding(self.session_id, self.binding_instance_id);
        self.lane_tracker
            .clear_response_service_handoff_drain_for_binding(
                self.session_id,
                self.binding_instance_id,
            );
        for entry in outputs {
            let _ = entry
                .commands
                .send_control(ReliablePathCommand::CloseStream(stream_id))
                .await;
        }
        let mut lead = self
            .ordered_data_owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *lead = None;
        self.response_flow_registration.set_service(None);
    }

    pub(in crate::runtime) async fn close_stream_ordered(
        &self,
        stream_id: StreamId,
        lane: FlowLane,
    ) {
        let outputs = {
            let outputs = self
                .outputs
                .lock()
                .expect("server reliable stream binding lock");
            self.response_stream_open.store(false, Ordering::Release);
            outputs.entries.clone()
        };
        self.response_flow_registration.set_active(false);
        self.lane_tracker
            .clear_quic_capacity_calibration_for_binding(self.session_id, self.binding_instance_id);
        self.lane_tracker
            .clear_response_service_handoff_drain_for_binding(
                self.session_id,
                self.binding_instance_id,
            );
        for entry in outputs {
            let _ = entry
                .commands
                .send_stream_ordered_close(stream_id, lane)
                .await;
        }
        let mut lead = self
            .ordered_data_owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *lead = None;
        self.response_flow_registration.set_service(None);
    }

    fn notify_update(&self) {
        let current = *self.version.borrow();
        let _ = self.version.send(current.wrapping_add(1));
    }

    pub(in crate::runtime) fn update_path_metrics_for_instance(
        &self,
        key: CarrierPathKey,
        path_instance_id: ServerCarrierPathInstanceId,
        metrics: PathMetrics,
        source: ServerPathMetricsSource,
    ) {
        self.update_path_metrics_matching(key, Some(path_instance_id), metrics, source);
    }

    pub(in crate::runtime) fn install_quic_capacity_proof_for_instance(
        &self,
        key: CarrierPathKey,
        path_instance_id: ServerCarrierPathInstanceId,
        metrics: PathMetrics,
        candidate: QuicCapacityProofCandidate,
    ) -> bool {
        self.install_path_metrics_entry_matching(
            key,
            Some(path_instance_id),
            ServerPathMetricsEntry {
                metrics,
                source: ServerPathMetricsSource::LocalSender,
                recorded_at: Instant::now(),
                capacity_proof: Some(candidate),
                tcp_capacity_proof: None,
            },
            false,
        )
        .0
    }

    pub(in crate::runtime) fn install_tcp_capacity_proof_for_instance(
        &self,
        key: CarrierPathKey,
        path_instance_id: ServerCarrierPathInstanceId,
        metrics: PathMetrics,
        candidate: TcpCapacityProofCandidate,
    ) -> bool {
        self.install_path_metrics_entry_matching(
            key,
            Some(path_instance_id),
            ServerPathMetricsEntry {
                metrics,
                source: ServerPathMetricsSource::LocalSender,
                recorded_at: Instant::now(),
                capacity_proof: None,
                tcp_capacity_proof: Some(candidate),
            },
            false,
        )
        .0
    }

    pub(in crate::runtime) fn install_stored_path_metrics_for_instance(
        &self,
        key: CarrierPathKey,
        path_instance_id: ServerCarrierPathInstanceId,
        path_metrics: ServerPathMetricsEntry,
    ) {
        self.install_path_metrics_entry_matching(key, Some(path_instance_id), path_metrics, true);
    }

    pub(in crate::runtime) fn notify_installed_path_metrics(&self) {
        self.graduate_completed_response_startup_owner();
        self.notify_update();
    }

    #[cfg(test)]
    pub(in crate::runtime) fn update_path_metrics(
        &self,
        key: CarrierPathKey,
        metrics: PathMetrics,
        source: ServerPathMetricsSource,
    ) {
        self.update_path_metrics_matching(key, None, metrics, source);
    }

    fn update_path_metrics_matching(
        &self,
        key: CarrierPathKey,
        path_instance_id: Option<ServerCarrierPathInstanceId>,
        metrics: PathMetrics,
        source: ServerPathMetricsSource,
    ) {
        let (_, changed) = self.install_path_metrics_entry_matching(
            key,
            path_instance_id,
            ServerPathMetricsEntry {
                metrics,
                source,
                recorded_at: Instant::now(),
                capacity_proof: None,
                tcp_capacity_proof: None,
            },
            true,
        );
        if changed {
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "server_response_path_metrics_attached",
                format_args!(
                    "session_id={} underlay={:?} path_id={} source={:?} direction={:?} rate_mbps={:.3} pacing_mbps={:.3} srtt_ms={:.3} confidence_ppm={} app_limited={} ack_sample={} sample_count={} sample_bytes={}",
                    self.session_id.0,
                    key.underlay,
                    key.path_id.0,
                    source,
                    metrics.direction,
                    metrics.delivery_rate_bps as f64 / 1_000_000.0,
                    metrics.pacing_rate_bps as f64 / 1_000_000.0,
                    metrics.srtt_us as f64 / 1000.0,
                    metrics.confidence_ppm,
                    metrics.app_limited,
                    metrics.has_ack_derived_data_sample,
                    metrics.data_sample_count,
                    metrics.data_sample_bytes,
                ),
            );
        }
    }

    fn install_path_metrics_entry_matching(
        &self,
        key: CarrierPathKey,
        path_instance_id: Option<ServerCarrierPathInstanceId>,
        mut path_metrics: ServerPathMetricsEntry,
        notify: bool,
    ) -> (bool, bool) {
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let now = Instant::now();
        let source = path_metrics.source;
        let metrics = path_metrics.metrics;
        let explicit_quic_capacity_proof = path_metrics.capacity_proof.is_some();
        let explicit_tcp_capacity_proof = path_metrics.tcp_capacity_proof.is_some();
        let mut matched = false;
        let mut changed = false;
        for entry in &mut outputs.entries {
            if entry.key == key
                && path_instance_id.is_none_or(|instance| entry.path_instance_id == instance)
            {
                matched = true;
                let current = match source {
                    ServerPathMetricsSource::LocalSender => &mut entry.local_path_metrics,
                    ServerPathMetricsSource::PeerHint => &mut entry.peer_path_metrics,
                };
                if !explicit_quic_capacity_proof {
                    path_metrics.capacity_proof = current
                        .and_then(|previous| previous.capacity_proof)
                        .filter(|proof| proof.expires_at > now);
                }
                if !explicit_tcp_capacity_proof {
                    path_metrics.tcp_capacity_proof = current
                        .and_then(|previous| previous.tcp_capacity_proof)
                        .filter(|proof| proof.expires_at > now);
                }
                let scheduling_changed = current.is_none_or(|previous| {
                    previous.source != source
                        || !server_path_metrics_scheduling_equivalent(previous.metrics, metrics)
                        || previous.capacity_proof != path_metrics.capacity_proof
                        || previous.tcp_capacity_proof != path_metrics.tcp_capacity_proof
                });
                *current = Some(path_metrics);
                changed |= scheduling_changed;
            }
        }
        if changed {
            self.response_model_generation
                .fetch_add(1, Ordering::AcqRel);
        }
        drop(outputs);
        if changed && notify {
            self.graduate_completed_response_startup_owner();
            self.notify_update();
        }
        (matched, changed)
    }
}

fn server_path_metrics_scheduling_equivalent(
    mut left: PathMetrics,
    mut right: PathMetrics,
) -> bool {
    // Epoch and age refresh evidence lifetime but do not change a ranking or
    // admission input. Suppressing that no-op update avoids waking every bound
    // response stream on each idle QUIC metrics poll.
    left.metric_epoch = 0;
    left.metric_age_us = 0;
    right.metric_epoch = 0;
    right.metric_age_us = 0;
    left == right
}

fn response_stream_live_role_update(
    current: StreamOpenRole,
    requested: StreamOpenRole,
) -> StreamOpenRole {
    match (current, requested) {
        (StreamOpenRole::Active, _) => StreamOpenRole::Active,
        (_, StreamOpenRole::Active) => StreamOpenRole::Active,
        (StreamOpenRole::Repair, _) | (_, StreamOpenRole::Repair) => StreamOpenRole::Repair,
        _ => current,
    }
}

fn response_stream_role_change_invalidates_response_state(
    previous: StreamOpenRole,
    current: StreamOpenRole,
) -> bool {
    previous != current
        && ((previous == StreamOpenRole::Repair) != (current == StreamOpenRole::Repair))
}

fn response_stream_role_reserves_flow_load(role: StreamOpenRole) -> bool {
    role == StreamOpenRole::Active
}

fn release_carrier_path_flight_ranges(
    flights: &mut BTreeMap<u64, Vec<CarrierPathFlight>>,
    ranges: &[OffsetRange],
) -> Vec<(u64, CarrierPathReleasedFlight)> {
    if ranges.is_empty() || flights.is_empty() {
        return Vec::new();
    }

    let original_flights = std::mem::take(flights)
        .into_iter()
        .flat_map(|(start, path_flights)| {
            path_flights.into_iter().map(move |flight| (start, flight))
        })
        .collect::<Vec<_>>();
    let ambiguous_intervals = carrier_path_ambiguous_flight_intervals(&original_flights);
    let mut released = Vec::new();
    for (start, flight) in original_flights.iter().copied() {
        let split = split_carrier_flight_interval_by_ack(start, flight.end, ranges);
        for (acked_start, acked_end) in split.acked {
            let bytes = carrier_flight_interval_bytes(acked_start, acked_end);
            if bytes == 0 {
                continue;
            }
            released.push((
                acked_start,
                CarrierPathReleasedFlight {
                    flight: CarrierPathFlight {
                        end: acked_end,
                        bytes,
                        ..flight
                    },
                    path_proving: flight.evidence_eligible
                        && flight.kind.is_ordering_owner()
                        && !carrier_flight_intervals_overlap(
                            &ambiguous_intervals,
                            acked_start,
                            acked_end,
                        ),
                },
            ));
        }
        for (retained_start, retained_end) in split.retained {
            let bytes = carrier_flight_interval_bytes(retained_start, retained_end);
            if bytes == 0 {
                continue;
            }
            flights
                .entry(retained_start)
                .or_default()
                .push(CarrierPathFlight {
                    end: retained_end,
                    bytes,
                    ..flight
                });
        }
    }
    released
}

fn carrier_path_ambiguous_flight_intervals(
    flights: &[(u64, CarrierPathFlight)],
) -> Vec<(u64, u64)> {
    let mut events = BTreeMap::<u64, i64>::new();
    for (start, flight) in flights {
        *events.entry(*start).or_default() += 1;
        *events.entry(flight.end).or_default() -= 1;
    }
    let mut intervals = Vec::new();
    let mut active = 0_i64;
    let mut previous = None;
    for (position, delta) in events {
        if let Some(previous) = previous
            && previous < position
            && active > 1
        {
            intervals.push((previous, position));
        }
        active += delta;
        previous = Some(position);
    }
    intervals
}

fn carrier_flight_intervals_overlap(intervals: &[(u64, u64)], start: u64, end: u64) -> bool {
    let position = intervals.partition_point(|(_, interval_end)| *interval_end <= start);
    intervals
        .get(position)
        .is_some_and(|(interval_start, _)| *interval_start < end)
}

struct CarrierFlightIntervalSplit {
    acked: Vec<(u64, u64)>,
    retained: Vec<(u64, u64)>,
}

fn split_carrier_flight_interval_by_ack(
    start: u64,
    end: u64,
    ranges: &[OffsetRange],
) -> CarrierFlightIntervalSplit {
    let mut acked = Vec::new();
    let mut retained = Vec::new();
    let mut cursor = start;
    for range in ranges {
        if range.end <= cursor {
            continue;
        }
        if range.start >= end {
            break;
        }
        let ack_start = cursor.max(range.start);
        if cursor < ack_start {
            retained.push((cursor, ack_start));
        }
        let ack_end = end.min(range.end);
        if ack_start < ack_end {
            acked.push((ack_start, ack_end));
            cursor = ack_end;
        }
        if cursor >= end {
            break;
        }
    }
    if cursor < end {
        retained.push((cursor, end));
    }
    CarrierFlightIntervalSplit { acked, retained }
}

fn carrier_flight_interval_bytes(start: u64, end: u64) -> usize {
    usize::try_from(end.saturating_sub(start)).unwrap_or(usize::MAX)
}

fn product_flights_have_recent_repair_overlap(
    flights: &BTreeMap<u64, Vec<CarrierPathFlight>>,
    start: u64,
    end: u64,
    now: Instant,
    retry_after: Duration,
    mut live: impl FnMut(CarrierPathKey) -> bool,
) -> bool {
    if start >= end {
        return false;
    }
    for (&offset, path_flights) in flights.range(..end) {
        for flight in path_flights {
            if offset >= end || flight.end <= start {
                continue;
            }
            if flight.kind != CarrierWorkKind::RepairData || !live(flight.key) {
                continue;
            }
            if now.saturating_duration_since(flight.sent_at) < retry_after {
                return true;
            }
        }
    }
    false
}

fn response_live_ordered_data_owner(
    stored: Option<CarrierPathKey>,
    entries: &[ResponseStreamOutputEntry],
) -> Option<CarrierPathKey> {
    stored.filter(|key| entries.iter().any(|entry| entry.key == *key))
}

fn response_outputs_have_live_mixed_owner_underlays(entries: &[ResponseStreamOutputEntry]) -> bool {
    let mut first_underlay = None;
    for entry in entries
        .iter()
        .filter(|entry| entry.role != StreamOpenRole::Repair && !entry.commands.is_closed())
    {
        match first_underlay {
            Some(underlay) if underlay != entry.key.underlay => return true,
            Some(_) => {}
            None => first_underlay = Some(entry.key.underlay),
        }
    }
    false
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(in crate::runtime) struct ServerCarrierPathInstanceId(u64);

impl ServerCarrierPathInstanceId {
    #[cfg(feature = "lab-diagnostics")]
    pub(in crate::runtime) fn as_u64(self) -> u64 {
        self.0
    }
}

pub(in crate::runtime) fn next_server_carrier_path_instance_id() -> ServerCarrierPathInstanceId {
    ServerCarrierPathInstanceId(NEXT_SERVER_CARRIER_PATH_INSTANCE_ID.fetch_add(1, Ordering::AcqRel))
}

#[cfg(test)]
#[path = "binding_test.rs"]
mod tests;
