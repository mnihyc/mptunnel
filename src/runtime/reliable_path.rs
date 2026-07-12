#[cfg(test)]
use super::ack_clock_policy::reliable_ack_clock_calibration_limit_bytes;
use super::ack_clock_policy::{
    reliable_ack_clock_calibration_ceiling_bytes,
    reliable_ack_clock_calibration_rate_coverage_floor_bytes,
    reliable_tcp_ack_clock_calibration_initial_limit_bytes,
};
#[cfg(feature = "lab-diagnostics")]
use super::bulk_admission::{
    bulk_active_service_product_envelope_bytes, bulk_latency_pressure_service_feed_window_bytes,
    bulk_service_feed_reservoir_payload_bytes, bulk_service_horizon_payload_bytes,
};
use super::*;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};

// Reliable-path bindings own attachment instances, exact range flights,
// evidence, and atomic commit. Sender services rank immutable snapshots.

static NEXT_SERVER_CARRIER_PATH_INSTANCE_ID: AtomicU64 = AtomicU64::new(1);
pub(in crate::runtime) const MAX_RESPONSE_QUIC_CAPACITY_CALIBRATION_ATTEMPTS_PER_PATH: u8 = 2;
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
mod registry;
mod response_admission;
mod response_placement;
mod response_service_handoff;
mod response_session;

pub(in crate::runtime) use quic_capacity_probe::ResponseQuicCapacityCalibrationRequest;
pub(in crate::runtime) use registry::*;
pub(super) use response_admission::*;
pub(in crate::runtime) use response_placement::*;
pub(in crate::runtime) use response_service_handoff::{
    ResponseServiceHandoffDrainRequest, ResponseServiceHandoffRequest,
};
#[cfg(test)]
use response_session::ServerQuicCapacityCalibrationPhase;
use response_session::ServerResponseFlowRegistration;
pub(in crate::runtime) use response_session::{
    QuicCapacityProofCandidate, ResponseServiceFamilyLoads, ResponseServiceHandoffDrainReservation,
    ResponseSessionSchedulingSnapshot, ServerPathLaneTracker, ServerRealtimeFlowRegistration,
    quic_capacity_proof_pin_matches_marker, quic_capacity_receipt_rate_bps,
    valid_quic_capacity_proof_candidate_at, well_formed_quic_capacity_proof_candidate,
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
pub(super) struct ReliablePathStream {
    pub(super) stream_id: StreamId,
    pub(super) max_offset: u64,
    pub(super) lane: FlowLane,
    pub(super) underlay: UnderlayProtocol,
    pub(super) max_frame_payload_bytes: usize,
    pub(super) output: ReliablePathStreamOutput,
    pub(super) frames: mpsc::Receiver<Result<Frame, RuntimeError>>,
}

impl ReliablePathStream {
    pub(super) fn into_handle_and_frames(
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

    pub(super) async fn recv_frame(&mut self) -> Result<Frame, RuntimeError> {
        match self.frames.recv().await {
            Some(Ok(frame)) => Ok(frame),
            Some(Err(err)) => Err(err),
            None => Err(RuntimeError::ReliablePathSessionClosed),
        }
    }

    pub(super) fn current_lane(&self) -> FlowLane {
        self.output.current_lane(self.lane)
    }

    pub(super) fn send_path_snapshot(
        &self,
        lane: FlowLane,
        payload_bytes: usize,
    ) -> Option<PathSnapshot> {
        self.output.send_path_snapshot(lane, payload_bytes)
    }

    pub(super) fn tail_repair_snapshot(
        &self,
        ack_frontier: u64,
        lane: FlowLane,
        payload_bytes: usize,
    ) -> Option<PathSnapshot> {
        self.output
            .tail_repair_snapshot(ack_frontier, lane)
            .or_else(|| self.send_path_snapshot(lane, payload_bytes))
    }

    pub(super) fn tail_repair_owner_underlay(&self, ack_frontier: u64) -> Option<UnderlayProtocol> {
        self.output.tail_repair_owner_underlay(ack_frontier)
    }

    pub(super) fn request_active_underlay(&self) -> Option<UnderlayProtocol> {
        self.output.request_active_underlay()
    }

    pub(super) fn request_active_path_snapshot(&self, lane: FlowLane) -> Option<PathSnapshot> {
        self.output.request_active_path_snapshot(lane)
    }

    pub(super) fn has_output_incarnation(&self, key: CarrierPathKey, incarnation: u64) -> bool {
        self.output.has_output_incarnation(key, incarnation)
    }

    pub(super) fn set_sender_queue_bytes(&self, bytes: usize) {
        self.output.set_sender_queue_bytes(bytes);
    }

    pub(super) fn subscribe_output_updates(&self) -> Option<watch::Receiver<u64>> {
        self.output.subscribe_updates()
    }

    pub(super) fn capacity_notifies(&self) -> Vec<Arc<Notify>> {
        self.output.capacity_notifies()
    }

    pub(super) fn response_service_handoff_drain_active(&self) -> bool {
        self.output.response_service_handoff_drain_active()
    }

    pub(super) fn set_lane(&mut self, lane: FlowLane) {
        self.lane = lane;
        self.output.set_lane(lane);
    }

    pub(super) fn release_normalized_acked_ranges(&self, ranges: &[OffsetRange]) {
        self.output.release_normalized_acked_ranges(ranges);
    }

    pub(super) fn has_recent_live_repair_flight_overlap(
        &self,
        frame: &Frame,
        retry_after: Duration,
    ) -> bool {
        self.output
            .has_recent_live_repair_flight_overlap(frame, retry_after)
    }

    pub(super) fn has_multipath_repair_alternative(&self) -> bool {
        self.output.has_multipath_repair_alternative()
    }

    pub(super) fn has_repair_output_for_frame(&self, frame: &Frame) -> bool {
        self.output.has_repair_output_for_frame(frame)
    }

    pub(super) fn has_live_owner_tail_repair_output_for_frame(&self, frame: &Frame) -> bool {
        self.output
            .has_live_owner_tail_repair_output_for_frame(frame)
    }

    pub(super) fn has_failed_owner_repair_output_for_frame(&self, frame: &Frame) -> bool {
        self.output.has_failed_owner_repair_output_for_frame(frame)
    }

    pub(super) fn has_unknown_owner_repair_output_for_frame(&self, frame: &Frame) -> bool {
        self.output.has_unknown_owner_repair_output_for_frame(frame)
    }

    pub(super) fn can_attempt_failed_owner_tail_repair(&self) -> bool {
        matches!(self.output, ReliablePathStreamOutput::Switchable(_))
    }

    pub(super) async fn close(&self) {
        self.output.close_stream(self.stream_id).await;
    }

    pub(super) async fn close_ordered(&self, lane: FlowLane) {
        self.output.close_stream_ordered(self.stream_id, lane).await;
    }
}

pub(super) struct ReliablePathStreamHandle {
    pub(super) stream_id: StreamId,
    pub(super) max_offset: u64,
    pub(super) lane: FlowLane,
    pub(super) underlay: UnderlayProtocol,
    pub(super) max_frame_payload_bytes: usize,
    pub(super) output: ReliablePathStreamOutput,
}

impl ReliablePathStreamHandle {
    pub(super) async fn send_detach(&self) {
        self.output.send_stream_detach(self.stream_id).await;
    }

    pub(super) fn enqueue_path_proof(&self) -> Result<Option<u64>, RuntimeError> {
        self.output.enqueue_path_proof()
    }

    pub(super) fn enqueue_stream_ordered_path_proof(
        &self,
        lane: FlowLane,
    ) -> Result<Option<u64>, RuntimeError> {
        self.output.enqueue_stream_ordered_path_proof(lane)
    }

    pub(super) fn can_enqueue_frame_now(&self, frame: &Frame, lane: FlowLane) -> bool {
        self.output.can_enqueue_frame_now(frame, lane)
    }

    pub(super) fn can_enqueue_work_lane_now(
        &self,
        work_lane: ReliableRelayQueuedWorkLane,
        relay_lane: FlowLane,
    ) -> bool {
        self.output
            .can_enqueue_lane_now(reliable_work_lane_to_carrier_lane(work_lane, relay_lane))
    }

    pub(super) fn capacity_notifies(&self) -> Vec<Arc<Notify>> {
        self.output.capacity_notifies()
    }

    pub(super) async fn close(&self) {
        self.output.close_stream(self.stream_id).await;
    }
}

#[derive(Clone)]
pub(super) enum ReliablePathStreamOutput {
    /// A fixed carrier command pipe, used by client-side paths where the stream
    /// is bound to the currently opened carrier association.
    Fixed(Arc<FixedReliablePathOutput>),
    /// A switchable response binding, used by server-side streams that may send
    /// later response bytes or repair ranges over another joined carrier path.
    Switchable(Arc<ResponseStreamBinding>),
}

pub(super) struct FixedReliablePathOutput {
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

pub(in crate::runtime) fn product_delivery_samples_override_startup_prior(
    delivery_samples: u32,
) -> bool {
    delivery_samples >= RELIABLE_INITIAL_WINDOW_PACKETS as u32
}

impl FixedReliablePathOutput {
    #[cfg(test)]
    pub(super) fn new(
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

    pub(super) fn new_with_snapshot(
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

    pub(super) fn key(&self) -> CarrierPathKey {
        self.key
    }

    pub(super) fn commands(&self) -> &ReliablePathCommandSender {
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
        let delivery_rate_bps = match (self.key.underlay, model.delivery_rate_bps) {
            (UnderlayProtocol::Tcp, Some(rate))
                if !product_delivery_samples_override_startup_prior(model.delivery_samples) =>
            {
                rate.max(prior_rate_bps)
            }
            (_, Some(rate)) => rate,
            (_, None) => prior_rate_bps,
        }
        .max(1.0);
        let srtt_ms = model.srtt_ms.unwrap_or(self.startup.srtt_ms);
        let mut snapshot = self.startup;
        snapshot.srtt_ms = srtt_ms;
        snapshot.delivery_rate_bps = delivery_rate_bps;
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

    pub(super) fn record_owner_flight(&self, frame: &Frame) {
        self.record_product_flight(frame, CarrierWorkKind::OwnerData)
    }

    pub(super) fn record_repair_flight(&self, frame: &Frame) {
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
    pub(super) fn fixed(
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: ReliablePathCommandSender,
        mux_limits: MuxLimits,
    ) -> Self {
        Self::Fixed(FixedReliablePathOutput::new(
            underlay, path_id, commands, mux_limits,
        ))
    }

    pub(super) fn fixed_with_snapshot(
        startup: PathSnapshot,
        commands: ReliablePathCommandSender,
        mux_limits: MuxLimits,
    ) -> Self {
        Self::Fixed(FixedReliablePathOutput::new_with_snapshot(
            startup, commands, mux_limits,
        ))
    }

    pub(super) fn can_enqueue_frame_now(&self, frame: &Frame, lane: FlowLane) -> bool {
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

    pub(super) fn can_enqueue_lane_now(&self, lane: FlowLane) -> bool {
        match self {
            Self::Fixed(fixed) => fixed.commands().can_enqueue_lane_now(lane),
            Self::Switchable(_) => false,
        }
    }

    pub(super) fn capacity_notifies(&self) -> Vec<Arc<Notify>> {
        match self {
            Self::Fixed(fixed) => vec![fixed.commands().capacity_notify()],
            Self::Switchable(binding) => binding.capacity_notifies(),
        }
    }

    pub(super) async fn send_stream_detach(&self, stream_id: StreamId) {
        if let Self::Fixed(fixed) = self {
            let _ = fixed
                .commands()
                .send_control(ReliablePathCommand::SendFrame(Frame::StreamDetach {
                    stream_id,
                }))
                .await;
        }
    }

    pub(super) async fn close_stream(&self, stream_id: StreamId) {
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

    pub(super) async fn close_stream_ordered(&self, stream_id: StreamId, lane: FlowLane) {
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

    pub(super) fn current_lane(&self, fallback: FlowLane) -> FlowLane {
        match self {
            Self::Fixed(_) => fallback,
            Self::Switchable(binding) => binding.lane(),
        }
    }

    pub(super) fn send_path_snapshot(
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

    pub(super) fn tail_repair_snapshot(
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

    pub(super) fn tail_repair_owner_underlay(&self, ack_frontier: u64) -> Option<UnderlayProtocol> {
        match self {
            Self::Fixed(fixed) => Some(fixed.key().underlay),
            Self::Switchable(binding) => binding.tail_repair_owner_underlay(ack_frontier),
        }
    }

    pub(super) fn request_active_underlay(&self) -> Option<UnderlayProtocol> {
        match self {
            Self::Fixed(fixed) => Some(fixed.key().underlay),
            Self::Switchable(binding) => binding.request_active_underlay(),
        }
    }

    pub(super) fn request_active_path_snapshot(&self, lane: FlowLane) -> Option<PathSnapshot> {
        match self {
            Self::Fixed(fixed) => {
                let _ = lane;
                Some(fixed.send_path_snapshot())
            }
            Self::Switchable(binding) => binding.request_active_path_snapshot(lane),
        }
    }

    pub(super) fn has_output_incarnation(&self, key: CarrierPathKey, incarnation: u64) -> bool {
        match self {
            Self::Fixed(_) => false,
            Self::Switchable(binding) => binding.has_output_incarnation(key, incarnation),
        }
    }

    pub(super) fn set_sender_queue_bytes(&self, bytes: usize) {
        match self {
            Self::Fixed(fixed) => fixed.set_sender_queue_bytes(bytes),
            Self::Switchable(binding) => binding.set_sender_queue_bytes(bytes),
        }
    }

    pub(super) fn subscribe_updates(&self) -> Option<watch::Receiver<u64>> {
        match self {
            Self::Fixed(_) => None,
            Self::Switchable(binding) => Some(binding.subscribe_updates()),
        }
    }

    pub(super) fn set_lane(&self, lane: FlowLane) {
        if let Self::Switchable(binding) = self {
            binding.set_lane(lane);
        }
    }

    pub(super) fn release_normalized_acked_ranges(&self, ranges: &[OffsetRange]) {
        match self {
            Self::Fixed(fixed) => fixed.release_normalized_acked_ranges(ranges),
            Self::Switchable(binding) => binding.release_normalized_acked_ranges(ranges),
        }
    }

    pub(super) fn has_recent_live_repair_flight_overlap(
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

    pub(super) fn has_multipath_repair_alternative(&self) -> bool {
        match self {
            Self::Fixed(_) => false,
            Self::Switchable(binding) => binding.has_multipath_repair_alternative(),
        }
    }

    pub(super) fn has_repair_output_for_frame(&self, frame: &Frame) -> bool {
        match self {
            Self::Fixed(_) => true,
            Self::Switchable(binding) => binding.has_repair_output_for_frame(frame),
        }
    }

    pub(super) fn has_live_owner_tail_repair_output_for_frame(&self, frame: &Frame) -> bool {
        match self {
            Self::Fixed(_) => false,
            Self::Switchable(binding) => binding.has_live_owner_tail_repair_output_for_frame(frame),
        }
    }

    pub(super) fn has_failed_owner_repair_output_for_frame(&self, frame: &Frame) -> bool {
        match self {
            Self::Fixed(_) => false,
            Self::Switchable(binding) => binding.has_failed_owner_repair_output_for_frame(frame),
        }
    }

    pub(super) fn has_unknown_owner_repair_output_for_frame(&self, frame: &Frame) -> bool {
        match self {
            Self::Fixed(_) => false,
            Self::Switchable(binding) => binding.has_unknown_owner_repair_output_for_frame(frame),
        }
    }
}

pub(super) fn reliable_work_lane_to_carrier_lane(
    work_lane: ReliableRelayQueuedWorkLane,
    relay_lane: FlowLane,
) -> FlowLane {
    match work_lane {
        ReliableRelayQueuedWorkLane::Control => FlowLane::Control,
        ReliableRelayQueuedWorkLane::Repair => reliable_path_stream_ordered_queue_lane(),
        ReliableRelayQueuedWorkLane::Data => relay_lane,
    }
}

pub(super) async fn wait_for_carrier_capacity_notifies(notifies: Vec<Arc<Notify>>) {
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
pub(super) struct ResponseSubflowAdmissionReservation {
    pub(super) admission: PathAdmission,
    pub(super) epoch_generation: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ResponseSubflowAdmissionRequest {
    pub(super) expected_planner_generation: u64,
    pub(super) expected_lane_generation: u64,
    pub(super) service: CarrierPathKey,
    pub(super) startup_owner_credit_bytes: usize,
    pub(super) optional_overhead_budget_bytes: usize,
    pub(super) max_read_gap_budget: Duration,
    pub(super) input: SubflowAdmissionInput,
}

#[derive(Debug, Clone, Copy)]
/// Optimistic calibration reservation. Generations fence product/path model
/// changes; pending values fence the exact queue-pressure projection.
pub(super) struct ResponseAckClockCalibrationRequest {
    pub(super) expected_planner_generation: u64,
    pub(super) expected_lane_generation: u64,
    pub(super) expected_model_generation: u64,
    pub(super) service: CarrierPathKey,
    pub(super) service_incarnation: u64,
    pub(super) service_pending_bytes: u64,
    pub(super) target_pending_bytes: u64,
    pub(super) limit_bytes: u64,
    /// Two response flows are required to start, not to finish exact begun work.
    pub(super) requires_multi_flow_start: bool,
}

#[derive(Debug, Clone, Copy)]
/// Zero-spend retirement uses the same coherent planner/model snapshot as Admit.
pub(super) struct ResponseAckClockCalibrationRetirementRequest {
    pub(super) expected_planner_generation: u64,
    pub(super) expected_lane_generation: u64,
    pub(super) expected_model_generation: u64,
    pub(super) service: CarrierPathKey,
    pub(super) service_incarnation: u64,
    pub(super) service_pending_bytes: u64,
    pub(super) target: CarrierPathKey,
    pub(super) target_incarnation: u64,
    pub(super) target_pending_bytes: u64,
    pub(super) limit_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ResponseSourceServiceSnapshot {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) key: CarrierPathKey,
    pub(super) active_latency_sensitive_flows: u32,
    pub(super) has_service_feed_evidence: bool,
    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) has_bulk_rate_evidence: bool,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ResponseRelayReadSnapshot {
    pub(super) send_path: Option<PathSnapshot>,
    pub(super) source_service: Option<ResponseSourceServiceSnapshot>,
    pub(super) independent_source_staging: bool,
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

pub(super) struct ResponseStreamBinding {
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
pub(super) enum ResponseStreamAttachOutcome {
    Attached,
    RoleChanged,
    ReplacedClosedOutput,
    RejectedDuplicateLiveOutput,
}

impl ResponseStreamBinding {
    #[cfg(test)]
    pub(super) fn new(
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
    pub(super) fn new_with_limits(
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

    pub(super) fn new_with_limits_and_tracker(
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

    fn new_with_limits_tracker_and_path_instance(
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
    pub(super) fn attach(
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

    fn attach_with_path_instance(
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

    pub(super) fn lane(&self) -> FlowLane {
        *self.lane.lock().expect("server reliable stream lane lock")
    }

    pub(super) fn subscribe_updates(&self) -> watch::Receiver<u64> {
        self.version.subscribe()
    }

    pub(super) fn send_path_snapshot(
        &self,
        lane: FlowLane,
        payload_bytes: usize,
    ) -> Option<PathSnapshot> {
        self.relay_read_snapshot(lane, payload_bytes).send_path
    }

    pub(super) fn relay_read_snapshot(
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

    pub(super) fn tail_repair_snapshot(
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

    pub(super) fn tail_repair_owner_underlay(&self, ack_frontier: u64) -> Option<UnderlayProtocol> {
        self.blocking_owner_key_at_or_after(ack_frontier)
            .or_else(|| self.ordered_data_owner())
            .map(|key| key.underlay)
    }

    #[cfg(test)]
    pub(super) fn has_live_mixed_owner_underlays(&self) -> bool {
        if !self.may_have_mixed_owner_underlays() {
            return false;
        }
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        response_outputs_have_live_mixed_owner_underlays(&outputs.entries)
    }

    pub(super) fn may_have_mixed_owner_underlays(&self) -> bool {
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

    pub(super) fn set_lane(&self, lane: FlowLane) {
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

    pub(super) fn has_multipath_repair_alternative(&self) -> bool {
        self.outputs
            .lock()
            .expect("server reliable stream binding lock")
            .entries
            .len()
            > 1
    }

    pub(super) fn has_repair_output_for_frame(&self, frame: &Frame) -> bool {
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

    pub(super) fn has_live_owner_tail_repair_output_for_frame(&self, frame: &Frame) -> bool {
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

    pub(super) fn has_recent_live_repair_flight_overlap(
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

    pub(super) fn has_failed_owner_repair_output_for_frame(&self, frame: &Frame) -> bool {
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

    pub(super) fn has_unknown_owner_repair_output_for_frame(&self, frame: &Frame) -> bool {
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

    pub(super) fn detach(&self, key: CarrierPathKey, commands: &ReliablePathCommandSender) {
        self.detach_matching_output(key, |entry| entry.commands.same_channel(commands));
    }

    fn detach_path_instance(
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

    pub(super) fn release_normalized_acked_ranges(&self, ranges: &[OffsetRange]) {
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
                let tcp_ack_clock_sample = if entry.key.underlay == UnderlayProtocol::Tcp {
                    let evidence = entry
                        .tcp_product_rate_evidence
                        .get_or_insert_with(|| ResponseAckClockRateEvidence::new(first_sent_at));
                    let _ = evidence.observe_with_fresh_bytes(
                        bytes,
                        bytes,
                        first_sent_at,
                        last_sent_at,
                        now,
                    );
                    evidence.goodput_sample()
                } else {
                    None
                };
                let carrier_app_limited = entry
                    .local_path_metrics
                    .is_some_and(|metrics| metrics.metrics.app_limited);
                // The terminal ACK stays calibration-owned, but a no-rate
                // tombstone must not freeze later ordinary TCP evidence.
                let tcp_calibration_owns_rate = entry.key.underlay == UnderlayProtocol::Tcp
                    && (calibration_identity_active
                        || calibration_update.is_some()
                        || calibration_snapshot.is_some_and(|calibration| {
                            calibration.calibrated_rate_bps.is_some()
                                || (!calibration.proven && !calibration.retired)
                        }));
                if !tcp_calibration_owns_rate {
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
                    && let Some(calibrated_rate_bps) =
                        calibration_snapshot.and_then(|calibration| calibration.calibrated_rate_bps)
                {
                    entry.product_progress_rate_bps = Some(calibrated_rate_bps);
                    entry.delivery_rate_bps = Some(calibrated_rate_bps);
                    entry.tcp_ack_clock_rate_bps = Some(calibrated_rate_bps);
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

    pub(super) fn ordered_data_owner(&self) -> Option<CarrierPathKey> {
        *self
            .ordered_data_owner
            .lock()
            .expect("server reliable stream ordered data owner lock")
    }

    #[cfg(test)]
    pub(super) fn request_active_owner(&self) -> Option<CarrierPathKey> {
        *self
            .request_active_owner
            .lock()
            .expect("server reliable stream request active owner lock")
    }

    pub(super) fn request_active_underlay(&self) -> Option<UnderlayProtocol> {
        self.request_active_owner
            .lock()
            .expect("server reliable stream request active owner lock")
            .map(|key| key.underlay)
    }

    pub(super) fn request_active_path_snapshot(&self, lane: FlowLane) -> Option<PathSnapshot> {
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

    pub(super) fn has_output_incarnation(&self, key: CarrierPathKey, incarnation: u64) -> bool {
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
    pub(super) fn set_ordered_data_owner(&self, key: CarrierPathKey) {
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
    pub(super) fn commit_ordered_data_owner_for_target(
        &self,
        target: &ResponseSenderPathTarget,
    ) -> bool {
        self.commit_ordered_data_owner_for_dispatch_target(&target.into())
    }

    pub(super) fn commit_ordered_data_owner_for_dispatch_target(
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
    pub(super) fn subflow_set_snapshot(&self) -> Option<FlowSubflowSet> {
        self.subflow_set
            .lock()
            .expect("server reliable stream subflow set lock")
            .set
            .clone()
    }

    pub(super) fn subflow_state_snapshot(&self) -> (u64, Option<FlowSubflowSet>) {
        let state = self
            .subflow_set
            .lock()
            .expect("server reliable stream subflow set lock");
        (state.planner_generation, state.set.clone())
    }

    pub(super) fn response_model_generation(&self) -> u64 {
        self.response_model_generation.load(Ordering::Acquire)
    }

    #[cfg(feature = "lab-diagnostics")]
    pub(super) fn should_emit_response_service_handoff_diagnostic(
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
    pub(super) fn lane_generation(&self) -> u64 {
        self.lane_tracker.generation(self.session_id)
    }

    #[cfg(test)]
    pub(super) fn lane_generation_and_active_response_flows(&self) -> (u64, u32) {
        self.lane_tracker
            .generation_and_active_response_flows(self.session_id)
    }

    pub(super) fn response_scheduling_snapshot(&self) -> ResponseSessionSchedulingSnapshot {
        self.lane_tracker
            .response_scheduling_snapshot(self.session_id)
    }

    #[cfg(test)]
    pub(super) fn preview_subflow_owner_admission(
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
    pub(super) fn commit_subflow_owner_admission(
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
    pub(super) fn commit_subflow_owner_admission_for_planner_generation(
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
    pub(super) fn reserve_subflow_owner_admission_for_planner_generation(
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

    pub(super) fn rollback_subflow_owner_admission_for_epoch(
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
                // calibration; only a later ACK-to-ACK window publishes rate.
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
                        reliable_ack_clock_calibration_rate_coverage_floor_bytes(self.mux_limits);
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
    pub(super) fn reset_subflow_set(&self) {
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
    pub(super) fn record_owner_flight_for_target(
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

    pub(super) fn try_retire_tcp_ack_clock_calibration(
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
                2,
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
    pub(super) fn try_enqueue_owner_frame_for_target(
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

    pub(super) fn try_enqueue_owner_frame_for_dispatch_target(
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
                    if request.requires_multi_flow_start { 2 } else { 0 },
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

    pub(super) fn try_enqueue_repair_frame_for_target(
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
    pub(super) fn record_owner_flight(&self, key: CarrierPathKey, frame: &Frame) {
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
    pub(super) fn record_repair_flight(&self, key: CarrierPathKey, frame: &Frame) {
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
    pub(super) fn age_repair_flights_for_test(&self, age: Duration) {
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

    pub(super) fn lower_flights_before_frame(&self, frame: &Frame) -> Vec<CarrierPathFlightDebt> {
        let Some((offset, _, _)) = reliable_stream_frame_extent(frame) else {
            return Vec::new();
        };
        self.lower_flights_before_offset(offset)
    }

    pub(super) fn flight_keys_overlapping_frame(&self, frame: &Frame) -> Vec<CarrierPathKey> {
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

    pub(super) fn owner_flight_keys_overlapping_frame(&self, frame: &Frame) -> Vec<CarrierPathKey> {
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

    pub(super) fn lower_flights_before_offset(&self, offset: u64) -> Vec<CarrierPathFlightDebt> {
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

    pub(super) fn sender_path_targets(
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
                    rate_scope: response_snapshot.rate_scope,
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

    pub(super) fn mux_limits(&self) -> MuxLimits {
        self.mux_limits
    }

    pub(super) fn active_tcp_ack_clock_calibration_remaining_bytes(&self) -> Option<usize> {
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
    pub(super) fn mark_output_bulk_proven_for_test(&self, key: CarrierPathKey) {
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
    pub(super) fn set_output_product_model_for_test(
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
    pub(super) fn install_tcp_ack_clock_calibration_for_test(
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

    pub(super) fn session_id(&self) -> SessionId {
        self.session_id
    }

    pub(super) async fn close_stream(&self, stream_id: StreamId) {
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

    pub(super) async fn close_stream_ordered(&self, stream_id: StreamId, lane: FlowLane) {
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

    pub(super) fn update_path_metrics_for_instance(
        &self,
        key: CarrierPathKey,
        path_instance_id: ServerCarrierPathInstanceId,
        metrics: PathMetrics,
        source: ServerPathMetricsSource,
    ) {
        self.update_path_metrics_matching(key, Some(path_instance_id), metrics, source);
    }

    pub(super) fn install_quic_capacity_proof_for_instance(
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
            },
            false,
        )
        .0
    }

    pub(super) fn install_stored_path_metrics_for_instance(
        &self,
        key: CarrierPathKey,
        path_instance_id: ServerCarrierPathInstanceId,
        path_metrics: ServerPathMetricsEntry,
    ) {
        self.install_path_metrics_entry_matching(key, Some(path_instance_id), path_metrics, true);
    }

    pub(super) fn notify_installed_path_metrics(&self) {
        self.graduate_completed_response_startup_owner();
        self.notify_update();
    }

    #[cfg(test)]
    pub(super) fn update_path_metrics(
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
        let explicit_capacity_proof = path_metrics.capacity_proof.is_some();
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
                if !explicit_capacity_proof {
                    path_metrics.capacity_proof = current
                        .and_then(|previous| previous.capacity_proof)
                        .filter(|proof| proof.expires_at > now);
                }
                let scheduling_changed = current.is_none_or(|previous| {
                    previous.source != source
                        || !server_path_metrics_scheduling_equivalent(previous.metrics, metrics)
                        || previous.capacity_proof != path_metrics.capacity_proof
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
/// Stable identity for one live carrier path inside a session.
///
/// The same product stream can have flights on several carrier paths; this key
/// names the carrier path without making the path own product bytes.
pub(super) struct CarrierPathKey {
    pub(super) underlay: UnderlayProtocol,
    pub(super) path_id: PathId,
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
mod tests {
    use super::*;
    use crate::runtime::bulk_admission::{
        ReliableSourceServiceStagingContext, ReliableSourceStagingContext,
        bulk_service_feed_reservoir_payload_bytes,
        reliable_relay_source_staging_owner_tail_headroom,
    };
    use crate::runtime::relay_io::reliable_stream_recv_progress_interval;
    use bytes::Bytes;
    use std::sync::mpsc as std_mpsc;
    use std::time::Duration;

    fn binding_for_underlay(
        underlay: UnderlayProtocol,
    ) -> (Arc<ResponseStreamBinding>, CarrierPathKey) {
        let (commands, _receivers) = reliable_path_command_channels(8);
        let key = CarrierPathKey {
            underlay,
            path_id: PathId(0),
        };
        let binding = ResponseStreamBinding::new(
            SessionId(42),
            underlay,
            key.path_id,
            commands,
            FlowLane::Throughput,
        );
        (binding, key)
    }

    fn stream_data_frame(payload_len: usize) -> Frame {
        stream_data_frame_at(0, payload_len)
    }

    fn stream_data_frame_at(offset: u64, payload_len: usize) -> Frame {
        Frame::StreamData {
            stream_id: StreamId(7),
            offset,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![0x5a; payload_len]),
        }
    }

    fn test_ack_clock_rate_sample(bytes: u64, rate_bps: f64) -> PathRateSample {
        PathRateSample::new(
            bytes,
            Duration::from_secs_f64(bytes as f64 * 8.0 / rate_bps),
        )
        .expect("valid ACK-clock rate sample")
    }

    fn assert_test_rate_close(actual: Option<f64>, expected: f64) {
        let actual = actual.expect("calibrated rate");
        assert!((actual - expected).abs() / expected.max(1.0) < 1e-6);
    }

    fn first_output_entry(binding: &ResponseStreamBinding) -> ResponseStreamOutputEntry {
        binding
            .outputs
            .lock()
            .expect("test response outputs lock")
            .entries
            .first()
            .expect("test response binding has output")
            .clone()
    }

    fn mark_test_response_output_bulk_proven(
        entry: &mut ResponseStreamOutputEntry,
        mux_limits: MuxLimits,
    ) {
        entry.product_progress_rate_bps = Some(100_000_000.0);
        entry.delivery_rate_bps = Some(100_000_000.0);
        entry.delivery_samples = 1;
        entry.owner_data_acked_bytes = reliable_subflow_startup_sample_limit_bytes(mux_limits);
    }

    fn mark_test_quic_output_carrier_bulk_proven(
        entry: &mut ResponseStreamOutputEntry,
        mux_limits: MuxLimits,
    ) {
        let sample_bytes = reliable_subflow_startup_sample_limit_bytes(mux_limits);
        entry.local_path_metrics = Some(ServerPathMetricsEntry {
            source: ServerPathMetricsSource::LocalSender,
            recorded_at: Instant::now(),
            capacity_proof: None,
            metrics: PathMetrics {
                path_id: entry.key.path_id,
                underlay: UnderlayProtocol::Udp,
                direction: PathMetricDirection::ServerToClient,
                metric_epoch: 1,
                metric_age_us: 0,
                min_rtt_us: 10_000,
                srtt_us: 12_000,
                rttvar_us: 1_000,
                jitter_us: 1_000,
                delivery_rate_bps: 500_000_000,
                pacing_rate_bps: 500_000_000,
                loss_ppm: 0,
                ecn_ppm: 0,
                loss_observed: false,
                ecn_observed: false,
                bytes_in_flight: 0,
                queue_bytes: 0,
                inflight_limit_bytes: sample_bytes,
                inflight_hi_bytes: sample_bytes,
                confidence_ppm: 1_000_000,
                app_limited: false,
                has_ack_derived_data_sample: true,
                data_sample_count: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
                data_sample_bytes: sample_bytes,
            },
        });
    }

    fn test_quic_capacity_proof(
        mux_limits: MuxLimits,
        token: u64,
        proof_validity: Duration,
    ) -> QuicCapacityProofCandidate {
        let proof_bytes = reliable_subflow_startup_sample_limit_bytes(mux_limits);
        let accounting_slack_bytes = (PATH_OPEN_SCORE_BYTES as u64).min(proof_bytes / 8);
        let proof_elapsed = Duration::from_millis(2);
        let accepted_at = Instant::now();
        QuicCapacityProofCandidate {
            token,
            train_bytes: proof_bytes,
            sample_floor_bytes: proof_bytes,
            accounting_slack_bytes,
            warmup_bytes: 0,
            required_proof_bytes: proof_bytes - accounting_slack_bytes,
            written_bytes: proof_bytes,
            written_data_frame_count: 1,
            receipt_confirmed: true,
            received_bytes: proof_bytes,
            proof_elapsed,
            rate_bps: quic_capacity_receipt_rate_bps(proof_bytes, proof_elapsed)
                .expect("test receipt rate"),
            accepted_at,
            expires_at: accepted_at + proof_validity,
            proof_validity,
        }
    }

    fn mark_test_quic_output_receipt_bulk_proven(
        entry: &mut ResponseStreamOutputEntry,
        mux_limits: MuxLimits,
        token: u64,
        proof_validity: Duration,
    ) -> QuicCapacityProofCandidate {
        mark_test_quic_output_carrier_bulk_proven(entry, mux_limits);
        let proof = test_quic_capacity_proof(mux_limits, token, proof_validity);
        let path_metrics = entry
            .local_path_metrics
            .as_mut()
            .expect("test QUIC metrics");
        // Keep receipt proof as the only bulk authority so expiry is observable.
        path_metrics.metrics.app_limited = true;
        path_metrics.metrics.has_ack_derived_data_sample = false;
        path_metrics.metrics.confidence_ppm = 0;
        path_metrics.metrics.data_sample_count = 0;
        path_metrics.metrics.data_sample_bytes = 0;
        path_metrics.capacity_proof = Some(proof);
        proof
    }

    #[test]
    fn quic_capacity_calibration_uses_carrier_bytes_without_product_flight() {
        let mux_limits = MuxLimits::default();
        let session_id = SessionId(510);
        let tracker = Arc::new(ServerPathLaneTracker::default());
        let (service_commands, _service_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new_with_limits_and_tracker(
            session_id,
            UnderlayProtocol::Tcp,
            PathId(0),
            service_commands,
            FlowLane::Throughput,
            mux_limits,
            tracker.clone(),
        );
        let (second_commands, _second_receivers) = reliable_path_command_channels(8);
        let _second_flow = ResponseStreamBinding::new_with_limits_and_tracker(
            session_id,
            UnderlayProtocol::Tcp,
            PathId(2),
            second_commands,
            FlowLane::Throughput,
            mux_limits,
            tracker,
        );
        let candidate = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let (candidate_commands, mut candidate_receivers) = reliable_path_command_channels(8);
        binding.attach(
            candidate.underlay,
            candidate.path_id,
            candidate_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            mux_limits.max_payload_bytes,
        );
        assert!(matches!(
            try_recv_reliable_path_command(&mut candidate_receivers),
            Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
        ));

        let (planner_generation, _) = binding.subflow_state_snapshot();
        let scheduling = binding.response_scheduling_snapshot();
        let model_generation = binding.response_model_generation();
        let target = binding
            .sender_path_targets(FlowLane::Throughput, 64 * 1024)
            .into_iter()
            .find(|target| target.key == candidate)
            .expect("UDP Validation target");
        let train_bytes = mux_limits
            .max_payload_bytes
            .saturating_add(PATH_OPEN_SCORE_BYTES);
        let sample_floor_bytes = train_bytes as u64;
        let accounting_slack_bytes = (PATH_OPEN_SCORE_BYTES as u64).min(sample_floor_bytes / 8);
        let required_proof_bytes = sample_floor_bytes - accounting_slack_bytes;
        assert!(binding.try_start_quic_capacity_calibration(
            &target,
            ResponseQuicCapacityCalibrationRequest {
                expected_planner_generation: planner_generation,
                expected_lane_generation: scheduling.generation,
                expected_model_generation: model_generation,
                target: candidate,
                target_path_instance_id: target.path_instance_id,
                target_incarnation: target.incarnation,
                target_pending_bytes: target.command_pending_bytes,
                train_bytes,
                sample_floor_bytes,
                accounting_slack_bytes,
                fresh_strict_window_bytes: required_proof_bytes,
                carrier_window_bytes: 0,
                lease: Duration::from_secs(1),
                proof_validity: Duration::from_secs(3),
            },
        ));
        let probe = match try_recv_reliable_path_command(&mut candidate_receivers)
            .expect("capacity probe command")
        {
            ReliablePathCommand::SendQuicCapacityProbe(probe) => probe,
            _ => panic!("expected typed QUIC capacity probe"),
        };
        assert_ne!(probe.calibration_id, 0);
        assert_eq!(probe.path_id, candidate.path_id);
        assert_eq!(probe.train_payload_bytes, train_bytes as u64);
        assert_eq!(probe.sample_floor_bytes, sample_floor_bytes);
        assert_eq!(probe.warmup_carrier_bytes, 0);
        assert_eq!(probe.required_timed_carrier_bytes, required_proof_bytes);
        assert!(probe.expires_at > Instant::now());
        assert!(
            binding
                .flights
                .lock()
                .expect("test response flight lock")
                .is_empty(),
            "carrier capacity bytes must not enter product ownership"
        );
        assert_eq!(
            binding.ordered_data_owner(),
            Some(CarrierPathKey {
                underlay: UnderlayProtocol::Tcp,
                path_id: PathId(0),
            })
        );
        assert!(
            binding
                .response_scheduling_snapshot()
                .quic_capacity_calibration_reserved
        );
        let generic_bulk_metrics = {
            let mut entry = binding
                .outputs
                .lock()
                .expect("test response outputs lock")
                .entries
                .iter()
                .find(|entry| entry.key == candidate)
                .expect("UDP capacity candidate")
                .clone();
            mark_test_quic_output_carrier_bulk_proven(&mut entry, mux_limits);
            entry
                .local_path_metrics
                .expect("generic local bulk metrics")
                .metrics
        };
        binding.update_path_metrics(
            candidate,
            generic_bulk_metrics,
            ServerPathMetricsSource::LocalSender,
        );
        assert!(
            binding
                .response_scheduling_snapshot()
                .quic_capacity_calibration_reserved,
            "generic path metrics cannot complete a token-owned capacity train"
        );
    }

    #[test]
    fn generic_metrics_preserve_but_do_not_extend_fixed_capacity_proof_deadline() {
        let mux_limits = MuxLimits::default();
        let (commands, _receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new_with_limits(
            SessionId(521),
            UnderlayProtocol::Udp,
            PathId(6),
            commands,
            FlowLane::Throughput,
            mux_limits,
        );
        let mut output = first_output_entry(&binding);
        mark_test_quic_output_carrier_bulk_proven(&mut output, mux_limits);
        let metrics = output
            .local_path_metrics
            .expect("test QUIC metrics")
            .metrics;
        let accepted_at = Instant::now();
        let expires_at = accepted_at + Duration::from_millis(20);
        let proof_bytes = reliable_subflow_startup_sample_limit_bytes(mux_limits);
        let accounting_slack = (PATH_OPEN_SCORE_BYTES as u64).min(proof_bytes / 8);
        let required_proof_bytes = proof_bytes - accounting_slack;
        let proof_elapsed = Duration::from_millis(10);
        let proof = QuicCapacityProofCandidate {
            token: 77,
            train_bytes: proof_bytes,
            sample_floor_bytes: proof_bytes,
            accounting_slack_bytes: accounting_slack,
            warmup_bytes: 0,
            required_proof_bytes,
            written_bytes: proof_bytes,
            written_data_frame_count: RELIABLE_INITIAL_WINDOW_PACKETS as u64,
            receipt_confirmed: true,
            received_bytes: proof_bytes,
            proof_elapsed,
            rate_bps: quic_capacity_receipt_rate_bps(proof_bytes, proof_elapsed)
                .expect("valid receipt rate"),
            accepted_at,
            expires_at,
            proof_validity: Duration::from_millis(20),
        };
        assert!(binding.install_quic_capacity_proof_for_instance(
            output.key,
            output.path_instance_id,
            metrics,
            proof,
        ));
        binding.update_path_metrics(
            output.key,
            PathMetrics {
                delivery_rate_bps: metrics.delivery_rate_bps / 2,
                ..metrics
            },
            ServerPathMetricsSource::LocalSender,
        );
        assert_eq!(
            first_output_entry(&binding)
                .local_path_metrics
                .and_then(|entry| entry.capacity_proof)
                .map(|proof| proof.expires_at),
            Some(expires_at)
        );

        std::thread::sleep(Duration::from_millis(25));
        binding.update_path_metrics(
            output.key,
            PathMetrics {
                delivery_rate_bps: metrics.delivery_rate_bps / 3,
                ..metrics
            },
            ServerPathMetricsSource::LocalSender,
        );
        assert!(
            first_output_entry(&binding)
                .local_path_metrics
                .is_some_and(|entry| entry.capacity_proof.is_none()),
            "an expired fixed proof cannot be resurrected by a generic refresh"
        );
    }

    #[test]
    fn quic_capacity_lease_deadline_is_created_after_admission_and_failure_propagates() {
        let mux_limits = MuxLimits::default();
        let session_id = SessionId(519);
        let tracker = Arc::new(ServerPathLaneTracker::default());
        let (service_commands, _service_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new_with_limits_and_tracker(
            session_id,
            UnderlayProtocol::Tcp,
            PathId(0),
            service_commands,
            FlowLane::Throughput,
            mux_limits,
            tracker.clone(),
        );
        let (second_commands, _second_receivers) = reliable_path_command_channels(8);
        let _second_flow = ResponseStreamBinding::new_with_limits_and_tracker(
            session_id,
            UnderlayProtocol::Tcp,
            PathId(2),
            second_commands,
            FlowLane::Throughput,
            mux_limits,
            tracker,
        );
        let candidate = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let (candidate_commands, mut candidate_receivers) = reliable_path_command_channels(8);
        let candidate_queue = candidate_commands.clone();
        binding.attach(
            candidate.underlay,
            candidate.path_id,
            candidate_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            mux_limits.max_payload_bytes,
        );
        assert!(matches!(
            try_recv_reliable_path_command(&mut candidate_receivers),
            Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
        ));

        let (planner_generation, _) = binding.subflow_state_snapshot();
        let scheduling = binding.response_scheduling_snapshot();
        let target = binding
            .sender_path_targets(FlowLane::Throughput, 64 * 1024)
            .into_iter()
            .find(|target| target.key == candidate)
            .expect("UDP Validation target");
        let pending_before_train = candidate_queue.pending_bytes();
        let train_bytes = mux_limits.max_payload_bytes / 2;
        let sample_floor_bytes = train_bytes as u64;
        let accounting_slack_bytes = (PATH_OPEN_SCORE_BYTES as u64).min(sample_floor_bytes / 8);
        let required_proof_bytes = sample_floor_bytes - accounting_slack_bytes;
        let mut deadline_observed_admitted_train = false;
        assert!(!binding.try_start_quic_capacity_calibration_with_lease(
            &target,
            ResponseQuicCapacityCalibrationRequest {
                expected_planner_generation: planner_generation,
                expected_lane_generation: scheduling.generation,
                expected_model_generation: binding.response_model_generation(),
                target: candidate,
                target_path_instance_id: target.path_instance_id,
                target_incarnation: target.incarnation,
                target_pending_bytes: target.command_pending_bytes,
                train_bytes,
                sample_floor_bytes,
                accounting_slack_bytes,
                fresh_strict_window_bytes: required_proof_bytes,
                carrier_window_bytes: 0,
                lease: Duration::from_secs(1),
                proof_validity: Duration::from_secs(3),
            },
            |_| {
                deadline_observed_admitted_train =
                    candidate_queue.pending_bytes() > pending_before_train;
                Duration::ZERO
            },
        ));
        assert!(deadline_observed_admitted_train);
        let after_failed_commit = binding.response_scheduling_snapshot();
        assert!(!after_failed_commit.quic_capacity_calibration_reserved);
        assert_eq!(
            after_failed_commit.quic_capacity_calibration_spent_bytes, train_bytes as u64,
            "an admitted train remains charged even when its lease cannot commit"
        );
        assert_eq!(
            binding
                .lane_tracker
                .response_path_scheduling_snapshot(session_id, candidate, target.path_instance_id,)
                .quic_capacity_calibration_attempts,
            1
        );
        assert!(matches!(
            try_recv_reliable_path_command(&mut candidate_receivers),
            Some(ReliablePathCommand::SendQuicCapacityProbe(_))
        ));
    }

    #[test]
    fn quic_capacity_reservation_expires_and_completion_releases_probe_slot() {
        let tracker = ServerPathLaneTracker::default();
        let session_id = SessionId(510);
        let path = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(9),
        };
        let binding_instance_id = 77;
        let path_instance_id = next_server_carrier_path_instance_id();
        let train_bytes = 100;
        let session_byte_limit = 1_000;
        tracker.attach_session(session_id);
        tracker.set_response_flow_active(session_id, true);
        tracker.set_response_flow_active(session_id, true);

        let first_generation = tracker.generation(session_id);
        assert!(tracker.try_reserve_test_quic_capacity_calibration(
            session_id,
            first_generation,
            binding_instance_id,
            path,
            path_instance_id,
            train_bytes,
            session_byte_limit,
            1,
        ));
        tracker.clear_quic_capacity_calibration(
            session_id,
            binding_instance_id + 1,
            path,
            path_instance_id,
        );
        assert!(
            tracker
                .response_scheduling_snapshot(session_id)
                .quic_capacity_calibration_reserved,
            "an unrelated binding on the shared carrier path cannot clear the lease"
        );
        tracker
            .state
            .lock()
            .expect("test lane tracker lock")
            .quic_capacity_calibrations
            .get_mut(&session_id)
            .expect("first reservation")
            .phase = ServerQuicCapacityCalibrationPhase::Active {
            expires_at: Instant::now() - Duration::from_millis(1),
        };
        let expired = tracker.response_scheduling_snapshot(session_id);
        assert!(!expired.quic_capacity_calibration_reserved);

        assert!(tracker.try_reserve_test_quic_capacity_calibration(
            session_id,
            expired.generation,
            binding_instance_id,
            path,
            path_instance_id,
            train_bytes,
            session_byte_limit,
            2,
        ));
        assert!(tracker.commit_test_quic_capacity_calibration(
            session_id,
            binding_instance_id,
            path,
            path_instance_id,
            Duration::from_secs(1),
            2,
        ));
        assert!(tracker.complete_test_quic_capacity_calibration(
            session_id,
            binding_instance_id,
            path,
            path_instance_id,
        ));
        let completed = tracker.response_scheduling_snapshot(session_id);
        assert!(
            !completed.quic_capacity_calibration_reserved,
            "measured evidence releases serialization for a different candidate"
        );

        tracker.clear_quic_capacity_calibration(
            session_id,
            binding_instance_id,
            path,
            path_instance_id,
        );
        let cleared = tracker.response_scheduling_snapshot(session_id);
        assert!(
            !tracker.try_reserve_test_quic_capacity_calibration(
                session_id,
                cleared.generation,
                binding_instance_id,
                path,
                path_instance_id,
                train_bytes,
                session_byte_limit,
                3,
            ),
            "completion releases the slot but not the exact path's two-attempt budget"
        );
        let alternate_path_instance_id = next_server_carrier_path_instance_id();
        assert!(tracker.try_reserve_test_quic_capacity_calibration(
            session_id,
            cleared.generation,
            binding_instance_id,
            path,
            alternate_path_instance_id,
            train_bytes,
            session_byte_limit,
            4,
        ));
        let alternate = tracker.response_scheduling_snapshot(session_id);
        assert_eq!(
            alternate.quic_capacity_calibration_spent_bytes,
            3 * train_bytes
        );
    }

    #[test]
    fn quic_capacity_attempts_are_path_instance_scoped_but_session_bytes_are_shared() {
        let tracker = ServerPathLaneTracker::default();
        let session_id = SessionId(518);
        let path = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(9),
        };
        let path_instance_id = next_server_carrier_path_instance_id();
        let replacement_path_instance_id = next_server_carrier_path_instance_id();
        let session_byte_limit = 250;
        tracker.set_response_flow_active(session_id, true);
        tracker.set_response_flow_active(session_id, true);

        for (binding_instance_id, token) in [(71, 1), (72, 2)] {
            assert!(tracker.try_reserve_test_quic_capacity_calibration(
                session_id,
                tracker.generation(session_id),
                binding_instance_id,
                path,
                path_instance_id,
                100,
                session_byte_limit,
                token,
            ));
            assert!(tracker.commit_test_quic_capacity_calibration(
                session_id,
                binding_instance_id,
                path,
                path_instance_id,
                Duration::from_secs(1),
                token,
            ));
            assert!(tracker.complete_test_quic_capacity_calibration(
                session_id,
                binding_instance_id,
                path,
                path_instance_id,
            ));
        }

        let shared_path =
            tracker.response_path_scheduling_snapshot(session_id, path, path_instance_id);
        assert_eq!(shared_path.quic_capacity_calibration_attempts, 2);
        let exhausted_generation = tracker.generation(session_id);
        assert!(!tracker.try_reserve_test_quic_capacity_calibration(
            session_id,
            exhausted_generation,
            73,
            path,
            path_instance_id,
            1,
            session_byte_limit,
            3,
        ));

        let replacement = tracker.response_path_scheduling_snapshot(
            session_id,
            path,
            replacement_path_instance_id,
        );
        assert_eq!(replacement.quic_capacity_calibration_attempts, 0);
        let before_budget_rejection = tracker.response_scheduling_snapshot(session_id);
        assert_eq!(
            before_budget_rejection.quic_capacity_calibration_spent_bytes,
            200
        );
        assert!(!before_budget_rejection.quic_capacity_calibration_reserved);
        assert!(!tracker.try_reserve_test_quic_capacity_calibration(
            session_id,
            before_budget_rejection.generation,
            73,
            path,
            replacement_path_instance_id,
            51,
            session_byte_limit,
            4,
        ));

        let after_budget_rejection = tracker.response_scheduling_snapshot(session_id);
        assert_eq!(
            after_budget_rejection.generation,
            before_budget_rejection.generation
        );
        assert_eq!(
            after_budget_rejection.quic_capacity_calibration_spent_bytes,
            before_budget_rejection.quic_capacity_calibration_spent_bytes
        );
        assert!(!after_budget_rejection.quic_capacity_calibration_reserved);
        assert_eq!(
            tracker
                .response_path_scheduling_snapshot(session_id, path, replacement_path_instance_id,)
                .quic_capacity_calibration_attempts,
            0,
            "a byte-budget rejection must not consume the replacement path's first attempt"
        );
        assert_eq!(
            tracker
                .response_path_scheduling_snapshot(session_id, path, path_instance_id)
                .quic_capacity_calibration_attempts,
            2
        );
    }

    #[test]
    fn quic_capacity_retirement_bounds_flapping_attempt_keys_without_refunding_spend() {
        let tracker = ServerPathLaneTracker::default();
        let session_id = SessionId(520);
        let path = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(9),
        };
        tracker.set_response_flow_active(session_id, true);
        tracker.set_response_flow_active(session_id, true);

        for token in 1..=32 {
            let path_instance_id = next_server_carrier_path_instance_id();
            assert!(tracker.try_reserve_test_quic_capacity_calibration(
                session_id,
                tracker.generation(session_id),
                71,
                path,
                path_instance_id,
                10,
                1_000,
                token,
            ));
            assert!(tracker.commit_test_quic_capacity_calibration(
                session_id,
                71,
                path,
                path_instance_id,
                Duration::from_secs(1),
                token,
            ));
            if token < 32 {
                assert!(tracker.complete_test_quic_capacity_calibration(
                    session_id,
                    71,
                    path,
                    path_instance_id,
                ));
            }
            assert_eq!(
                tracker
                    .response_path_scheduling_snapshot(session_id, path, path_instance_id)
                    .quic_capacity_calibration_attempts,
                1
            );
            tracker.retire_quic_capacity_calibration_path_instance(
                session_id,
                path,
                path_instance_id,
            );
            assert!(
                !tracker
                    .response_scheduling_snapshot(session_id)
                    .quic_capacity_calibration_reserved
            );
            assert_eq!(
                tracker
                    .response_path_scheduling_snapshot(session_id, path, path_instance_id)
                    .quic_capacity_calibration_attempts,
                0
            );
        }

        let state = tracker.state.lock().expect("test lane tracker lock");
        assert!(
            state
                .quic_capacity_calibration_attempts
                .keys()
                .all(|key| key.session_id != session_id)
        );
        assert_eq!(
            state.quic_capacity_calibration_bytes.get(&session_id),
            Some(&320),
            "carrier-instance retirement cannot refill the session envelope"
        );
    }

    #[test]
    fn quic_capacity_replacement_only_resets_a_distinct_retired_instance() {
        let mux_limits = MuxLimits::default();
        let session_id = SessionId(521);
        let tracker = Arc::new(ServerPathLaneTracker::default());
        let (service_commands, _service_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new_with_limits_and_tracker(
            session_id,
            UnderlayProtocol::Tcp,
            PathId(0),
            service_commands,
            FlowLane::Throughput,
            mux_limits,
            tracker.clone(),
        );
        let (second_commands, _second_receivers) = reliable_path_command_channels(8);
        let _second_flow = ResponseStreamBinding::new_with_limits_and_tracker(
            session_id,
            UnderlayProtocol::Tcp,
            PathId(2),
            second_commands,
            FlowLane::Throughput,
            mux_limits,
            tracker.clone(),
        );
        let candidate = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let old_instance = next_server_carrier_path_instance_id();
        let (old_commands, old_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach_with_path_instance(
                candidate.underlay,
                candidate.path_id,
                old_instance,
                old_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                mux_limits.max_payload_bytes,
            ),
            ResponseStreamAttachOutcome::Attached
        );
        assert!(tracker.try_reserve_test_quic_capacity_calibration(
            session_id,
            tracker.generation(session_id),
            binding.binding_instance_id,
            candidate,
            old_instance,
            10,
            100,
            1,
        ));
        assert!(tracker.commit_test_quic_capacity_calibration(
            session_id,
            binding.binding_instance_id,
            candidate,
            old_instance,
            Duration::from_secs(1),
            1,
        ));
        drop(old_receivers);

        let (same_instance_commands, same_instance_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach_with_path_instance(
                candidate.underlay,
                candidate.path_id,
                old_instance,
                same_instance_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                mux_limits.max_payload_bytes,
            ),
            ResponseStreamAttachOutcome::ReplacedClosedOutput
        );
        assert_eq!(
            tracker
                .response_path_scheduling_snapshot(session_id, candidate, old_instance)
                .quic_capacity_calibration_attempts,
            1,
            "reopening commands for the same carrier instance cannot reset its allowance"
        );
        assert!(
            !tracker
                .response_scheduling_snapshot(session_id)
                .quic_capacity_calibration_reserved,
            "replacing a dead command queue must release its active serialization lease"
        );
        drop(same_instance_receivers);

        let replacement_instance = next_server_carrier_path_instance_id();
        let (replacement_commands, _replacement_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach_with_path_instance(
                candidate.underlay,
                candidate.path_id,
                replacement_instance,
                replacement_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                mux_limits.max_payload_bytes,
            ),
            ResponseStreamAttachOutcome::ReplacedClosedOutput
        );
        assert_eq!(
            tracker
                .response_path_scheduling_snapshot(session_id, candidate, old_instance)
                .quic_capacity_calibration_attempts,
            1,
            "binding replacement cannot retire a carrier shared by other streams"
        );
        tracker.retire_quic_capacity_calibration_path_instance(session_id, candidate, old_instance);
        assert_eq!(
            tracker
                .response_path_scheduling_snapshot(session_id, candidate, old_instance)
                .quic_capacity_calibration_attempts,
            0,
            "exact carrier retirement releases its instance-scoped attempt key"
        );
        let scheduling = tracker.response_scheduling_snapshot(session_id);
        assert_eq!(scheduling.quic_capacity_calibration_spent_bytes, 10);
        assert_eq!(
            tracker
                .response_path_scheduling_snapshot(session_id, candidate, replacement_instance)
                .quic_capacity_calibration_attempts,
            0
        );
    }

    #[test]
    fn quic_capacity_rollback_is_provisional_token_exact_and_reclaim_clears_ledgers() {
        let tracker = ServerPathLaneTracker::default();
        let session_id = SessionId(512);
        let path = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(7),
        };
        let path_instance_id = next_server_carrier_path_instance_id();
        let binding_instance_id = 41;
        tracker.attach_session(session_id);
        tracker.set_response_flow_active(session_id, true);
        tracker.set_response_flow_active(session_id, true);

        let generation = tracker.generation(session_id);
        assert!(tracker.try_reserve_test_quic_capacity_calibration(
            session_id,
            generation,
            binding_instance_id,
            path,
            path_instance_id,
            100,
            1_000,
            10,
        ));
        tracker.rollback_quic_capacity_calibration(
            session_id,
            binding_instance_id,
            path,
            path_instance_id,
            9,
        );
        let stale_rollback = tracker.response_scheduling_snapshot(session_id);
        assert!(stale_rollback.quic_capacity_calibration_reserved);
        assert_eq!(stale_rollback.quic_capacity_calibration_spent_bytes, 100);

        tracker.rollback_quic_capacity_calibration(
            session_id,
            binding_instance_id,
            path,
            path_instance_id,
            10,
        );
        let rolled_back = tracker.response_scheduling_snapshot(session_id);
        assert!(!rolled_back.quic_capacity_calibration_reserved);
        assert_eq!(rolled_back.quic_capacity_calibration_spent_bytes, 0);
        assert_eq!(
            tracker
                .response_path_scheduling_snapshot(session_id, path, path_instance_id)
                .quic_capacity_calibration_attempts,
            0
        );

        assert!(tracker.try_reserve_test_quic_capacity_calibration(
            session_id,
            rolled_back.generation,
            binding_instance_id,
            path,
            path_instance_id,
            100,
            1_000,
            11,
        ));
        assert!(tracker.commit_test_quic_capacity_calibration(
            session_id,
            binding_instance_id,
            path,
            path_instance_id,
            Duration::from_secs(1),
            11,
        ));
        tracker.rollback_quic_capacity_calibration(
            session_id,
            binding_instance_id,
            path,
            path_instance_id,
            11,
        );
        let admitted = tracker.response_scheduling_snapshot(session_id);
        assert!(admitted.quic_capacity_calibration_reserved);
        assert_eq!(admitted.quic_capacity_calibration_spent_bytes, 100);
        assert!(tracker.complete_test_quic_capacity_calibration(
            session_id,
            binding_instance_id,
            path,
            path_instance_id,
        ));
        assert_eq!(
            tracker
                .response_scheduling_snapshot(session_id)
                .quic_capacity_calibration_spent_bytes,
            100,
            "admitted carrier bytes remain charged after proof"
        );

        tracker.set_response_flow_active(session_id, false);
        tracker.set_response_flow_active(session_id, false);
        tracker.detach_session(session_id);
        let state = tracker.state.lock().expect("test lane tracker lock");
        assert!(
            !state
                .quic_capacity_calibration_bytes
                .contains_key(&session_id)
        );
        assert!(
            state
                .quic_capacity_calibration_attempts
                .keys()
                .all(|key| key.session_id != session_id)
        );
    }

    #[test]
    fn response_service_handoff_drain_is_session_serialized() {
        let tracker = ServerPathLaneTracker::default();
        let session_id = SessionId(513);
        let service = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let target = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let service_instance = next_server_carrier_path_instance_id();
        let target_instance = next_server_carrier_path_instance_id();
        tracker.set_response_flow_active(session_id, true);
        tracker.set_response_flow_active(session_id, true);
        tracker.attach_response_service(session_id, service, FlowLane::Throughput);
        tracker.attach_response_service(session_id, service, FlowLane::Throughput);

        let generation = tracker.generation(session_id);
        assert!(tracker.try_reserve_response_service_handoff_drain(
            session_id,
            generation,
            1,
            service,
            service_instance,
            10,
            target,
            target_instance,
            20,
            None,
            Instant::now() + Duration::from_secs(1),
        ));
        let reserved = tracker.response_scheduling_snapshot(session_id);
        assert!(!tracker.try_reserve_response_service_handoff_drain(
            session_id,
            reserved.generation,
            2,
            service,
            service_instance,
            11,
            target,
            target_instance,
            21,
            None,
            Instant::now() + Duration::from_secs(1),
        ));
        assert!(!tracker.clear_response_service_handoff_drain_for_binding(session_id, 2));
        assert!(tracker.clear_response_service_handoff_drain_for_binding(session_id, 1));
    }

    #[test]
    fn expired_response_service_handoff_drain_rejects_move_without_changing_loads() {
        let tracker = ServerPathLaneTracker::default();
        let session_id = SessionId(514);
        let service = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let target = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let service_instance = next_server_carrier_path_instance_id();
        let target_instance = next_server_carrier_path_instance_id();
        tracker.set_response_flow_active(session_id, true);
        tracker.set_response_flow_active(session_id, true);
        tracker.attach_response_service(session_id, service, FlowLane::Throughput);
        tracker.attach_response_service(session_id, service, FlowLane::Throughput);

        assert!(tracker.try_reserve_response_service_handoff_drain(
            session_id,
            tracker.generation(session_id),
            1,
            service,
            service_instance,
            10,
            target,
            target_instance,
            20,
            None,
            Instant::now() + Duration::from_secs(1),
        ));
        let move_generation = tracker.generation(session_id);
        tracker
            .state
            .lock()
            .expect("test lane tracker lock")
            .response_service_handoff_drains
            .get_mut(&session_id)
            .expect("reserved handoff drain")
            .expires_at = Instant::now() - Duration::from_millis(1);

        assert!(!tracker.try_move_response_service_handoff(
            session_id,
            move_generation,
            1,
            service,
            service_instance,
            10,
            target,
            target_instance,
            20,
            None,
            FlowLane::Throughput,
        ));
        let scheduling = tracker.response_scheduling_snapshot(session_id);
        assert_eq!(
            scheduling.service_family_loads,
            ResponseServiceFamilyLoads::new(2, 0)
        );
        assert!(scheduling.response_service_handoff_drain.is_none());
        assert_eq!(
            tracker
                .response_service_snapshot(session_id, service)
                .active_flows,
            2
        );
        assert_eq!(
            tracker
                .response_service_snapshot(session_id, target)
                .active_flows,
            0
        );
    }

    #[test]
    fn direct_response_service_handoff_rejects_proof_that_expired_before_atomic_move() {
        let tracker = ServerPathLaneTracker::default();
        let session_id = SessionId(518);
        let service = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let target = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let service_instance = next_server_carrier_path_instance_id();
        let target_instance = next_server_carrier_path_instance_id();
        tracker.set_response_flow_active(session_id, true);
        tracker.set_response_flow_active(session_id, true);
        tracker.attach_response_service(session_id, service, FlowLane::Throughput);
        tracker.attach_response_service(session_id, service, FlowLane::Throughput);
        let mut proof = test_quic_capacity_proof(MuxLimits::default(), 518, Duration::from_secs(1));
        proof.accepted_at = Instant::now() - Duration::from_secs(2);
        proof.expires_at = proof.accepted_at + proof.proof_validity;

        assert!(!tracker.try_move_response_service_handoff(
            session_id,
            tracker.generation(session_id),
            1,
            service,
            service_instance,
            10,
            target,
            target_instance,
            20,
            Some(proof),
            FlowLane::Throughput,
        ));
        let scheduling = tracker.response_scheduling_snapshot(session_id);
        assert_eq!(
            scheduling.service_family_loads,
            ResponseServiceFamilyLoads::new(2, 0)
        );
        assert_eq!(
            tracker
                .response_service_snapshot(session_id, service)
                .active_flows,
            2
        );
        assert_eq!(
            tracker
                .response_service_snapshot(session_id, target)
                .active_flows,
            0
        );
    }

    #[test]
    fn response_service_handoff_drain_requires_every_reserved_identity() {
        let tracker = ServerPathLaneTracker::default();
        let session_id = SessionId(515);
        let service = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let target = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let service_instance = next_server_carrier_path_instance_id();
        let target_instance = next_server_carrier_path_instance_id();
        tracker.set_response_flow_active(session_id, true);
        tracker.set_response_flow_active(session_id, true);
        tracker.attach_response_service(session_id, service, FlowLane::Throughput);
        tracker.attach_response_service(session_id, service, FlowLane::Throughput);
        let proof = test_quic_capacity_proof(MuxLimits::default(), 515, Duration::from_secs(1));

        assert!(tracker.try_reserve_response_service_handoff_drain(
            session_id,
            tracker.generation(session_id),
            1,
            service,
            service_instance,
            10,
            target,
            target_instance,
            20,
            Some(proof),
            Instant::now() + Duration::from_secs(1),
        ));
        let generation = tracker.generation(session_id);
        let wrong_service_instance = next_server_carrier_path_instance_id();
        let wrong_target_instance = next_server_carrier_path_instance_id();

        for (binding, from_instance, from_incarnation, to_instance, to_incarnation) in [
            (2, service_instance, 10, target_instance, 20),
            (1, wrong_service_instance, 10, target_instance, 20),
            (1, service_instance, 11, target_instance, 20),
            (1, service_instance, 10, wrong_target_instance, 20),
            (1, service_instance, 10, target_instance, 21),
        ] {
            assert!(!tracker.try_move_response_service_handoff(
                session_id,
                generation,
                binding,
                service,
                from_instance,
                from_incarnation,
                target,
                to_instance,
                to_incarnation,
                Some(proof),
                FlowLane::Throughput,
            ));
        }
        assert!(!tracker.try_move_response_service_handoff(
            session_id,
            generation,
            1,
            service,
            service_instance,
            10,
            target,
            target_instance,
            20,
            Some(QuicCapacityProofCandidate {
                token: proof.token.wrapping_add(1),
                ..proof
            }),
            FlowLane::Throughput,
        ));

        let scheduling = tracker.response_scheduling_snapshot(session_id);
        assert_eq!(
            scheduling.service_family_loads,
            ResponseServiceFamilyLoads::new(2, 0)
        );
        assert_eq!(
            scheduling
                .response_service_handoff_drain
                .expect("identity mismatch must preserve drain")
                .binding_instance_id,
            1
        );
    }

    #[test]
    fn matching_response_service_handoff_drain_moves_one_flow_and_is_consumed() {
        let tracker = ServerPathLaneTracker::default();
        let session_id = SessionId(516);
        let service = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let target = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let service_instance = next_server_carrier_path_instance_id();
        let target_instance = next_server_carrier_path_instance_id();
        tracker.set_response_flow_active(session_id, true);
        tracker.set_response_flow_active(session_id, true);
        tracker.attach_response_service(session_id, service, FlowLane::Throughput);
        tracker.attach_response_service(session_id, service, FlowLane::Throughput);

        assert!(tracker.try_reserve_response_service_handoff_drain(
            session_id,
            tracker.generation(session_id),
            1,
            service,
            service_instance,
            10,
            target,
            target_instance,
            20,
            None,
            Instant::now() + Duration::from_secs(1),
        ));
        assert!(tracker.try_move_response_service_handoff(
            session_id,
            tracker.generation(session_id),
            1,
            service,
            service_instance,
            10,
            target,
            target_instance,
            20,
            None,
            FlowLane::Throughput,
        ));

        let scheduling = tracker.response_scheduling_snapshot(session_id);
        assert_eq!(
            scheduling.service_family_loads,
            ResponseServiceFamilyLoads::new(1, 1)
        );
        assert!(scheduling.response_service_handoff_drain.is_none());
        assert_eq!(
            tracker
                .response_service_snapshot(session_id, service)
                .active_flows,
            1
        );
        assert_eq!(
            tracker
                .response_service_snapshot(session_id, target)
                .active_flows,
            1
        );
    }

    #[test]
    fn clearing_response_service_handoff_drain_requires_exact_target_path() {
        let tracker = ServerPathLaneTracker::default();
        let session_id = SessionId(517);
        let service = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let target = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let service_instance = next_server_carrier_path_instance_id();
        let target_instance = next_server_carrier_path_instance_id();
        tracker.set_response_flow_active(session_id, true);
        tracker.set_response_flow_active(session_id, true);
        tracker.attach_response_service(session_id, service, FlowLane::Throughput);
        tracker.attach_response_service(session_id, service, FlowLane::Throughput);

        assert!(tracker.try_reserve_response_service_handoff_drain(
            session_id,
            tracker.generation(session_id),
            1,
            service,
            service_instance,
            10,
            target,
            target_instance,
            20,
            None,
            Instant::now() + Duration::from_secs(1),
        ));
        assert!(!tracker.clear_response_service_handoff_drain_for_path(
            session_id,
            2,
            target,
            target_instance,
        ));
        assert!(!tracker.clear_response_service_handoff_drain_for_path(
            session_id,
            1,
            target,
            next_server_carrier_path_instance_id(),
        ));
        assert!(
            tracker
                .response_scheduling_snapshot(session_id)
                .response_service_handoff_drain
                .is_some()
        );
        assert!(tracker.clear_response_service_handoff_drain_for_path(
            session_id,
            1,
            target,
            target_instance,
        ));
        let scheduling = tracker.response_scheduling_snapshot(session_id);
        assert!(scheduling.response_service_handoff_drain.is_none());
        assert_eq!(
            scheduling.service_family_loads,
            ResponseServiceFamilyLoads::new(2, 0)
        );
    }

    #[test]
    fn exact_clear_frontier_handoff_pins_quic_proof_through_marker_expiry() {
        let mux_limits = MuxLimits::default();
        let session_id = SessionId(511);
        let tracker = Arc::new(ServerPathLaneTracker::default());
        let service = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let candidate = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let (service_commands, _service_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new_with_limits_and_tracker(
            session_id,
            service.underlay,
            service.path_id,
            service_commands,
            FlowLane::Throughput,
            mux_limits,
            tracker.clone(),
        );
        let (second_commands, _second_receivers) = reliable_path_command_channels(8);
        let _second_flow = ResponseStreamBinding::new_with_limits_and_tracker(
            session_id,
            UnderlayProtocol::Tcp,
            PathId(2),
            second_commands,
            FlowLane::Throughput,
            mux_limits,
            tracker,
        );
        let (candidate_commands, mut candidate_receivers) = reliable_path_command_channels(8);
        binding.attach(
            candidate.underlay,
            candidate.path_id,
            candidate_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            mux_limits.max_payload_bytes,
        );
        let _ = try_recv_reliable_path_command(&mut candidate_receivers);
        let proof = {
            let mut outputs = binding.outputs.lock().expect("test response outputs lock");
            let mut proof = None;
            for entry in &mut outputs.entries {
                if entry.key == service {
                    mark_test_response_output_bulk_proven(entry, mux_limits);
                } else if entry.key == candidate {
                    proof = Some(mark_test_quic_output_receipt_bulk_proven(
                        entry,
                        mux_limits,
                        511,
                        Duration::from_millis(250),
                    ));
                }
            }
            proof.expect("installed QUIC receipt proof")
        };
        let frontier = 4096;
        binding
            .ack_ordering
            .lock()
            .expect("test response ACK ordering lock")
            .contiguous_frontier = frontier;
        let (planner_generation, _) = binding.subflow_state_snapshot();
        let scheduling = binding.response_scheduling_snapshot();
        assert_eq!(
            scheduling.service_family_loads,
            ResponseServiceFamilyLoads::new(2, 0)
        );
        let model_generation = binding.response_model_generation();
        let targets = binding.sender_path_targets(FlowLane::Throughput, 64 * 1024);
        let service_target = targets
            .iter()
            .find(|target| target.key == service)
            .expect("TCP Service target")
            .clone();
        let candidate_target = targets
            .iter()
            .find(|target| target.key == candidate)
            .expect("measured QUIC target")
            .clone();
        let frame = stream_data_frame_at(frontier, 64 * 1024);
        let request = ResponseServiceHandoffRequest {
            expected_planner_generation: planner_generation,
            expected_lane_generation: scheduling.generation,
            expected_model_generation: model_generation,
            handoff_frontier: frontier,
            service,
            service_path_instance_id: service_target.path_instance_id,
            service_incarnation: service_target.incarnation,
            target: candidate,
            target_path_instance_id: candidate_target.path_instance_id,
            target_incarnation: candidate_target.incarnation,
            mode: ResponseServiceHandoffMode::Diversification,
            target_command_pending_limit_bytes: u64::MAX,
            capacity_proof: Some(proof),
        };
        assert!(matches!(
            binding.try_enqueue_response_service_handoff(
                &candidate_target,
                &frame,
                FlowLane::Throughput,
                ResponseServiceHandoffRequest {
                    expected_model_generation: model_generation.wrapping_sub(1),
                    ..request
                },
            ),
            Err(RuntimeError::SenderServiceBlocked)
        ));
        assert_eq!(binding.ordered_data_owner(), Some(service));
        assert_eq!(
            binding.response_scheduling_snapshot().service_family_loads,
            ResponseServiceFamilyLoads::new(2, 0),
            "a stale handoff must not reserve or move session Service load"
        );
        assert!(binding.try_start_response_service_handoff_drain(
            &service_target,
            &candidate_target,
            FlowLane::Throughput,
            ResponseServiceHandoffDrainRequest {
                expected_planner_generation: planner_generation,
                expected_lane_generation: request.expected_lane_generation,
                expected_model_generation: model_generation,
                service,
                service_path_instance_id: service_target.path_instance_id,
                service_incarnation: service_target.incarnation,
                target: candidate,
                target_path_instance_id: candidate_target.path_instance_id,
                target_incarnation: candidate_target.incarnation,
                mode: ResponseServiceHandoffMode::Diversification,
                capacity_proof: Some(proof),
                outstanding_owner_bytes: 64 * 1024,
                lease: Duration::from_secs(1),
            },
        ));
        let drained_scheduling = binding.response_scheduling_snapshot();
        assert!(drained_scheduling.response_service_handoff_drain.is_some());
        std::thread::sleep(
            proof
                .expires_at
                .saturating_duration_since(Instant::now())
                .saturating_add(Duration::from_millis(10)),
        );
        assert!(
            binding
                .outputs
                .lock()
                .expect("test response outputs lock")
                .entries
                .iter()
                .find(|entry| entry.key == candidate)
                .is_some_and(|entry| {
                    server_output_quic_capacity_proof_marker(entry) == Some(proof)
                        && server_output_fresh_quic_capacity_proof(entry).is_none()
                }),
            "the raw marker remains observable after ordinary authority expires"
        );
        let candidate_target = binding
            .sender_path_targets(FlowLane::Throughput, 64 * 1024)
            .into_iter()
            .find(|target| target.key == candidate)
            .expect("reserved QUIC target after marker expiry");
        assert!(!candidate_target.has_bulk_rate_evidence);
        binding
            .try_enqueue_response_service_handoff(
                &candidate_target,
                &frame,
                FlowLane::Throughput,
                ResponseServiceHandoffRequest {
                    expected_lane_generation: drained_scheduling.generation,
                    ..request
                },
            )
            .expect("exact drained frontier should commit one sticky handoff");

        assert_eq!(binding.ordered_data_owner(), Some(candidate));
        assert!(!binding.response_service_handoff_open());
        assert!(
            binding
                .response_scheduling_snapshot()
                .response_service_handoff_drain
                .is_none(),
            "the matching drain intent must be consumed with the Service move"
        );
        assert_eq!(
            binding.response_scheduling_snapshot().service_family_loads,
            ResponseServiceFamilyLoads::new(1, 1)
        );
        assert_eq!(
            binding
                .lane_tracker
                .response_service_snapshot(session_id, service)
                .active_flows,
            0,
            "the old Active attachment must not retain response Service pressure"
        );
        assert_eq!(
            binding
                .lane_tracker
                .response_service_snapshot(session_id, candidate)
                .active_flows,
            1
        );
        let moved_targets = binding.sender_path_targets(FlowLane::Throughput, 64 * 1024);
        assert_eq!(
            moved_targets
                .iter()
                .find(|target| target.key == service)
                .expect("old TCP attachment")
                .snapshot
                .active_flows,
            0
        );
        assert_eq!(
            moved_targets
                .iter()
                .find(|target| target.key == candidate)
                .expect("new QUIC Service")
                .snapshot
                .active_flows,
            1
        );
        assert!(matches!(
            try_recv_reliable_path_command(&mut candidate_receivers),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData {
                offset,
                ..
            })) if offset == frontier
        ));
    }

    #[test]
    fn fixed_stream_ordered_path_proof_follows_earlier_stream_data() {
        let mux_limits = MuxLimits::default();
        let path_id = PathId(3);
        let (commands, mut receivers) = reliable_path_command_channels(4);
        commands
            .try_enqueue_admitted_frame(stream_data_frame(32), FlowLane::Throughput)
            .expect("queue earlier stream data");
        let stream = ReliablePathStreamHandle {
            stream_id: StreamId(7),
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: UnderlayProtocol::Tcp,
            max_frame_payload_bytes: mux_limits.max_payload_bytes,
            output: ReliablePathStreamOutput::fixed(
                UnderlayProtocol::Tcp,
                path_id,
                commands,
                mux_limits,
            ),
        };

        let proof_id = stream
            .enqueue_stream_ordered_path_proof(FlowLane::Throughput)
            .expect("queue stream-ordered path proof")
            .expect("fixed output has a carrier path");

        assert!(
            try_recv_reliable_path_priority_command(&mut receivers).is_none(),
            "stream-ordered proof must not enter the priority queue"
        );
        assert!(matches!(
            try_recv_reliable_path_command(&mut receivers),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
        ));
        let proof_frame = match try_recv_reliable_path_command(&mut receivers) {
            Some(ReliablePathCommand::SendFrame(frame)) => {
                let Frame::PathProofData {
                    path_id: queued_path_id,
                    proof_id: queued_proof_id,
                    payload,
                } = &frame
                else {
                    panic!("stream-ordered proof must follow earlier product data");
                };
                assert_eq!(*queued_path_id, path_id);
                assert_eq!(*queued_proof_id, proof_id);
                assert!(!payload.is_empty());
                frame
            }
            _ => panic!("stream-ordered proof must follow earlier product data"),
        };
        let payload_len = match &proof_frame {
            Frame::PathProofData { payload, .. } => payload.len(),
            _ => unreachable!("matched path proof frame above"),
        };
        let mut tracker = PathProofTracker::default();
        tracker.record_sent_frame(&proof_frame);
        let observation = tracker
            .acknowledge(
                path_id,
                proof_id,
                u32::try_from(payload_len).expect("test proof payload length fits u32"),
            )
            .expect("consumed ordered proof is tracked for acknowledgement");
        assert_eq!(observation.proof_id, proof_id);
        assert_eq!(observation.bytes, payload_len as u64);
    }

    #[test]
    fn fixed_priority_path_proof_preserves_attachment_liveness_ordering() {
        let mux_limits = MuxLimits::default();
        let path_id = PathId(4);
        let (commands, mut receivers) = reliable_path_command_channels(4);
        commands
            .try_enqueue_admitted_frame(stream_data_frame(32), FlowLane::Throughput)
            .expect("queue earlier stream data");
        let stream = ReliablePathStreamHandle {
            stream_id: StreamId(7),
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: UnderlayProtocol::Tcp,
            max_frame_payload_bytes: mux_limits.max_payload_bytes,
            output: ReliablePathStreamOutput::fixed(
                UnderlayProtocol::Tcp,
                path_id,
                commands,
                mux_limits,
            ),
        };

        let proof_id = stream
            .enqueue_path_proof()
            .expect("queue priority path proof")
            .expect("fixed output has a carrier path");

        match try_recv_reliable_path_priority_command(&mut receivers) {
            Some(ReliablePathCommand::SendFrame(Frame::PathProofData {
                path_id: queued_path_id,
                proof_id: queued_proof_id,
                ..
            })) => {
                assert_eq!(queued_path_id, path_id);
                assert_eq!(queued_proof_id, proof_id);
            }
            _ => panic!("attachment-liveness proof must retain priority ordering"),
        }
        assert!(matches!(
            try_recv_reliable_path_command(&mut receivers),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
        ));
    }

    #[test]
    fn switchable_stream_ordered_path_proof_keeps_no_fixed_carrier_semantics() {
        let (binding, key) = binding_for_underlay(UnderlayProtocol::Tcp);
        let stream = ReliablePathStreamHandle {
            stream_id: StreamId(7),
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: key.underlay,
            max_frame_payload_bytes: MuxLimits::default().max_payload_bytes,
            output: ReliablePathStreamOutput::Switchable(binding),
        };

        assert_eq!(
            stream
                .enqueue_stream_ordered_path_proof(FlowLane::Throughput)
                .expect("switchable output is a successful no-op"),
            None
        );
    }

    #[test]
    fn response_validation_attach_adds_output_without_promoting_lead() {
        let (binding, active) = binding_for_underlay(UnderlayProtocol::Udp);
        let validation = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let (validation_commands, mut validation_receivers) = reliable_path_command_channels(8);

        assert_eq!(binding.ordered_data_owner(), Some(active));
        assert_eq!(
            binding.attach(
                validation.underlay,
                validation.path_id,
                validation_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                reliable_relay_buffer_len(MuxLimits::default()),
            ),
            ResponseStreamAttachOutcome::Attached
        );

        let outputs = binding.outputs.lock().expect("test response outputs lock");
        assert_eq!(outputs.entries.len(), 2);
        assert!(outputs.entries.iter().any(|entry| entry.key == validation));
        drop(outputs);
        assert_eq!(
            binding.ordered_data_owner(),
            Some(active),
            "validation attachment opens a carrier output but is not scheduler ownership"
        );
        match try_recv_reliable_path_priority_command(&mut validation_receivers) {
            Some(ReliablePathCommand::SendFrame(Frame::PathProofData {
                path_id, payload, ..
            })) => {
                assert_eq!(path_id, validation.path_id);
                assert!(!payload.is_empty());
            }
            _ => panic!("validation attach must enqueue carrier path proof"),
        }
    }

    #[test]
    fn independent_source_staging_requires_live_mixed_owner_underlays() {
        let (active_commands, active_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new(
            SessionId(42),
            UnderlayProtocol::Tcp,
            PathId(0),
            active_commands,
            FlowLane::Throughput,
        );
        let mut receivers = vec![active_receivers];
        assert!(!binding.has_live_mixed_owner_underlays());
        assert!(
            !binding
                .relay_read_snapshot(FlowLane::Throughput, PATH_OPEN_SCORE_BYTES)
                .independent_source_staging
        );
        assert_eq!(
            binding.owner_underlay_history.load(Ordering::Acquire),
            RESPONSE_OWNER_TCP_SEEN
        );

        for (path_id, underlay, role, expected, expected_history) in [
            (
                1,
                UnderlayProtocol::Tcp,
                StreamOpenRole::Validation,
                false,
                RESPONSE_OWNER_TCP_SEEN,
            ),
            (
                2,
                UnderlayProtocol::Udp,
                StreamOpenRole::Repair,
                false,
                RESPONSE_OWNER_TCP_SEEN,
            ),
            (
                3,
                UnderlayProtocol::Udp,
                StreamOpenRole::Validation,
                true,
                RESPONSE_OWNER_MIXED_SEEN,
            ),
        ] {
            let (commands, output_receivers) = reliable_path_command_channels(8);
            assert_eq!(
                binding.attach(
                    underlay,
                    PathId(path_id),
                    commands,
                    FlowLane::Throughput,
                    role,
                    reliable_relay_buffer_len(MuxLimits::default()),
                ),
                ResponseStreamAttachOutcome::Attached,
            );
            assert_eq!(
                binding.has_live_mixed_owner_underlays(),
                expected,
                "only a live owner-capable cross-underlay output enables independent raw staging",
            );
            assert_eq!(
                binding
                    .relay_read_snapshot(FlowLane::Throughput, PATH_OPEN_SCORE_BYTES)
                    .independent_source_staging,
                expected,
                "the composite relay snapshot must use the same live-family policy",
            );
            assert_eq!(
                binding.owner_underlay_history.load(Ordering::Acquire),
                expected_history,
                "Repair-only attachments must retain the single-family fast path",
            );
            receivers.push(output_receivers);
        }
    }

    #[test]
    fn response_relay_read_snapshot_keeps_source_evidence_on_the_ordered_service() {
        let session_id = SessionId(42);
        let lane_tracker = Arc::new(ServerPathLaneTracker::default());
        let service = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let (service_commands, _service_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new_with_limits_and_tracker(
            session_id,
            service.underlay,
            service.path_id,
            service_commands,
            FlowLane::Throughput,
            MuxLimits::default(),
            lane_tracker.clone(),
        );
        let alternate = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let (alternate_commands, _alternate_receivers) = reliable_path_command_channels(8);
        let alternate_commands_for_detach = alternate_commands.clone();
        assert_eq!(
            binding.attach(
                alternate.underlay,
                alternate.path_id,
                alternate_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                reliable_relay_buffer_len(MuxLimits::default()),
            ),
            ResponseStreamAttachOutcome::Attached,
        );
        let (latency_commands, _latency_receivers) = reliable_path_command_channels(8);
        let _alternate_latency_flow = ResponseStreamBinding::new_with_limits_and_tracker(
            session_id,
            alternate.underlay,
            alternate.path_id,
            latency_commands,
            FlowLane::Latency,
            MuxLimits::default(),
            lane_tracker,
        );
        {
            let mut outputs = binding.outputs.lock().expect("test response outputs lock");
            let service_entry = outputs
                .entries
                .iter_mut()
                .find(|entry| entry.key == service)
                .expect("ordered Service output");
            service_entry.delivery_rate_bps = Some(1_000_000.0);
            service_entry.srtt_ms = Some(500.0);
            service_entry.delivery_samples = 1;
        }
        binding.update_path_metrics(
            alternate,
            PathMetrics {
                path_id: alternate.path_id,
                underlay: alternate.underlay,
                direction: PathMetricDirection::ServerToClient,
                metric_epoch: metric_epoch_now(),
                metric_age_us: 0,
                min_rtt_us: 5_000,
                srtt_us: 5_000,
                rttvar_us: 500,
                jitter_us: 500,
                delivery_rate_bps: 1_000_000_000,
                pacing_rate_bps: 1_000_000_000,
                loss_ppm: 0,
                ecn_ppm: 0,
                loss_observed: false,
                ecn_observed: false,
                bytes_in_flight: 0,
                queue_bytes: 0,
                inflight_limit_bytes: PATH_OPEN_SCORE_BYTES as u64,
                inflight_hi_bytes: PATH_OPEN_SCORE_BYTES as u64,
                confidence_ppm: 1_000_000,
                app_limited: false,
                has_ack_derived_data_sample: true,
                data_sample_count: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
                data_sample_bytes: RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES,
            },
            ServerPathMetricsSource::LocalSender,
        );

        let before = binding.relay_read_snapshot(FlowLane::Throughput, PATH_OPEN_SCORE_BYTES);
        assert!(before.send_path.is_some_and(|path| {
            path.id == alternate.path_id && path.underlay == alternate.underlay
        }));
        assert_eq!(
            before
                .send_path
                .expect("faster alternate send path")
                .active_latency_sensitive_flows,
            1
        );
        let source = before
            .source_service
            .expect("live ordered Service snapshot");
        assert_eq!(source.key, service);
        assert!(!source.has_bulk_rate_evidence);
        assert!(before.independent_source_staging);

        {
            let mut outputs = binding.outputs.lock().expect("test response outputs lock");
            let service_entry = outputs
                .entries
                .iter_mut()
                .find(|entry| entry.key == service)
                .expect("ordered Service output");
            service_entry.product_progress_rate_bps = Some(1_000_000.0);
            service_entry.owner_data_acked_bytes =
                reliable_subflow_startup_sample_limit_bytes(binding.mux_limits());
        }
        let after = binding.relay_read_snapshot(FlowLane::Throughput, PATH_OPEN_SCORE_BYTES);
        let source = after.source_service.expect("live ordered Service snapshot");
        assert!(source.has_bulk_rate_evidence);
        assert_eq!(source.active_latency_sensitive_flows, 0);
        assert_eq!(
            reliable_relay_source_staging_owner_tail_headroom(
                ReliableSourceStagingContext {
                    independent: after.independent_source_staging,
                    service: Some(ReliableSourceServiceStagingContext {
                        allows_product_envelope: true,
                        has_latency_pressure: source.active_latency_sensitive_flows > 0,
                        has_feed_evidence: source.has_service_feed_evidence,
                    }),
                },
                FlowLane::Throughput,
                0,
                0,
                reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default()),
                MuxLimits::default(),
            ),
            bulk_service_feed_reservoir_payload_bytes(
                reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default()),
                MuxLimits::default(),
            ),
            "alternate-path latency pressure must not narrow exact-Service source staging"
        );
        let service_target = binding
            .sender_path_targets(FlowLane::Throughput, PATH_OPEN_SCORE_BYTES)
            .into_iter()
            .find(|target| target.key == service)
            .expect("ordered Service sender target");
        assert_eq!(
            source.has_bulk_rate_evidence, service_target.has_bulk_rate_evidence,
            "source staging and sender admission must consume the same Service proof"
        );

        binding.detach(alternate, &alternate_commands_for_detach);
        assert!(
            !binding
                .relay_read_snapshot(FlowLane::Throughput, PATH_OPEN_SCORE_BYTES)
                .independent_source_staging,
            "mixed-family source staging must end when the alternate family detaches"
        );
    }

    #[test]
    fn udp_product_progress_matures_only_current_service_feed() {
        let service = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        };
        let (service_commands, _service_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new(
            SessionId(42),
            service.underlay,
            service.path_id,
            service_commands,
            FlowLane::Throughput,
        );
        {
            let mut outputs = binding.outputs.lock().expect("test response outputs lock");
            let service_entry = outputs
                .entries
                .iter_mut()
                .find(|entry| entry.key == service)
                .expect("ordered Service output");
            service_entry.product_progress_rate_bps = Some(100_000_000.0);
            service_entry.delivery_rate_bps = Some(100_000_000.0);
            service_entry.srtt_ms = Some(20.0);
            service_entry.delivery_samples = u32::MAX;
            service_entry.owner_data_acked_bytes = u64::MAX;
        }

        let read = binding.relay_read_snapshot(FlowLane::Throughput, PATH_OPEN_SCORE_BYTES);
        let source = read.source_service.expect("live ordered Service snapshot");
        assert_eq!(source.key, service);
        let send_path = read.send_path.expect("single live Service send snapshot");
        assert_eq!(send_path.confidence, 1.0);
        assert!(send_path.product_progress_rate_bps.is_some());
        assert!(
            source.has_service_feed_evidence,
            "substantial uniquely owned product ACKs may release current-Service staging"
        );
        assert!(
            !source.has_bulk_rate_evidence,
            "product ACK timing must not mint optional QUIC placement authority"
        );
    }

    #[test]
    fn udp_app_limited_carrier_progress_feeds_only_the_current_service() {
        let mux_limits = MuxLimits::default();
        let service = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        };
        let (service_commands, _service_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new(
            SessionId(42),
            service.underlay,
            service.path_id,
            service_commands,
            FlowLane::Throughput,
        );
        let mut entry = first_output_entry(&binding);
        mark_test_quic_output_carrier_bulk_proven(&mut entry, mux_limits);
        let metrics = PathMetrics {
            app_limited: true,
            ..entry
                .local_path_metrics
                .expect("test QUIC sender metrics")
                .metrics
        };
        binding.update_path_metrics(service, metrics, ServerPathMetricsSource::LocalSender);

        let read = binding.relay_read_snapshot(FlowLane::Throughput, PATH_OPEN_SCORE_BYTES);
        let source = read.source_service.expect("current response Service");
        assert!(source.has_service_feed_evidence);
        assert!(
            !source.has_bulk_rate_evidence,
            "an app-limited sample must not authorize optional placement"
        );

        let target = binding
            .sender_path_targets(FlowLane::Throughput, PATH_OPEN_SCORE_BYTES)
            .into_iter()
            .find(|target| target.key == service)
            .expect("current Service sender target");
        assert!(target.is_active);
        assert!(target.has_service_feed_evidence);
        assert!(!target.has_bulk_rate_evidence);

        let alternate = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let (alternate_commands, _alternate_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                alternate.underlay,
                alternate.path_id,
                alternate_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                reliable_relay_buffer_len(mux_limits),
            ),
            ResponseStreamAttachOutcome::Attached
        );
        let mut alternate_entry = binding
            .outputs
            .lock()
            .expect("test response outputs lock")
            .entries
            .iter()
            .find(|entry| entry.key == alternate)
            .expect("Validation output")
            .clone();
        mark_test_quic_output_carrier_bulk_proven(&mut alternate_entry, mux_limits);
        let alternate_metrics = PathMetrics {
            app_limited: true,
            ..alternate_entry
                .local_path_metrics
                .expect("alternate QUIC sender metrics")
                .metrics
        };
        binding.update_path_metrics(
            alternate,
            alternate_metrics,
            ServerPathMetricsSource::LocalSender,
        );
        let alternate_target = binding
            .sender_path_targets(FlowLane::Throughput, PATH_OPEN_SCORE_BYTES)
            .into_iter()
            .find(|target| target.key == alternate)
            .expect("Validation sender target");
        assert!(!alternate_target.is_active);
        assert!(!alternate_target.has_service_feed_evidence);
        assert!(!alternate_target.has_bulk_rate_evidence);
    }

    #[test]
    fn response_repair_output_requires_explicit_active_reannounce() {
        let (binding, active) = binding_for_underlay(UnderlayProtocol::Tcp);
        let repair = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let (repair_commands, _repair_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.request_active_underlay(),
            Some(UnderlayProtocol::Tcp)
        );

        assert_eq!(
            binding.attach(
                repair.underlay,
                repair.path_id,
                repair_commands.clone(),
                FlowLane::Latency,
                StreamOpenRole::Repair,
                reliable_relay_buffer_len(MuxLimits::default()),
            ),
            ResponseStreamAttachOutcome::Attached
        );
        assert_eq!(
            binding.owner_underlay_history.load(Ordering::Acquire),
            RESPONSE_OWNER_TCP_SEEN,
            "Repair attachment must not disable the single-family fast path"
        );

        assert_eq!(
            binding.attach(
                repair.underlay,
                repair.path_id,
                repair_commands.clone(),
                FlowLane::Latency,
                StreamOpenRole::Validation,
                reliable_relay_buffer_len(MuxLimits::default()),
            ),
            ResponseStreamAttachOutcome::Attached,
            "same-channel Validation cannot weaken an existing Repair role"
        );
        assert_eq!(
            binding.owner_underlay_history.load(Ordering::Acquire),
            RESPONSE_OWNER_TCP_SEEN,
            "an ineffective Validation request must not poison family history"
        );

        assert_eq!(
            binding.attach(
                repair.underlay,
                repair.path_id,
                repair_commands,
                FlowLane::Latency,
                StreamOpenRole::Active,
                reliable_relay_buffer_len(MuxLimits::default()),
            ),
            ResponseStreamAttachOutcome::RoleChanged,
            "explicit Active reannounce may promote future work without changing old repair-flight semantics"
        );

        let mut outputs = binding.outputs.lock().expect("test response outputs lock");
        let repair_entry = outputs
            .entries
            .iter_mut()
            .find(|entry| entry.key == repair)
            .expect("repair output remains attached");
        assert_eq!(repair_entry.role, StreamOpenRole::Active);
        repair_entry.srtt_ms = Some(40.0);
        outputs
            .entries
            .iter_mut()
            .find(|entry| entry.key == active)
            .expect("response Service output remains attached")
            .srtt_ms = Some(500.0);
        drop(outputs);
        assert_eq!(binding.ordered_data_owner(), Some(active));
        assert_eq!(
            binding.request_active_owner(),
            Some(repair),
            "request Active reannounce must not depend on the response data owner"
        );
        assert_eq!(
            binding.request_active_underlay(),
            Some(UnderlayProtocol::Udp),
            "server receive-progress policy follows the current request Active family"
        );
        let request_active_snapshot = binding
            .request_active_path_snapshot(FlowLane::Throughput)
            .expect("request Active output remains attached");
        let response_service_snapshot = binding
            .send_path_snapshot(FlowLane::Throughput, PATH_OPEN_SCORE_BYTES)
            .expect("response Service output remains attached");
        assert_eq!(request_active_snapshot.id, repair.path_id);
        assert_eq!(request_active_snapshot.underlay, UnderlayProtocol::Udp);
        assert_eq!(response_service_snapshot.id, active.path_id);
        assert_eq!(response_service_snapshot.underlay, UnderlayProtocol::Tcp);
        assert!(
            reliable_stream_recv_progress_interval(
                Some(request_active_snapshot),
                FlowLane::Throughput,
            ) < reliable_stream_recv_progress_interval(
                Some(response_service_snapshot),
                FlowLane::Throughput,
            ),
            "receive-progress cadence must follow the request Active PTO rather than the response Service PTO"
        );
        assert_eq!(
            binding.owner_underlay_history.load(Ordering::Acquire),
            RESPONSE_OWNER_MIXED_SEEN
        );
    }

    #[test]
    fn response_repair_enqueue_rejects_detached_output_incarnation() {
        let key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(6),
        };
        let (commands, mut receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new(
            SessionId(42),
            key.underlay,
            key.path_id,
            commands.clone(),
            FlowLane::Throughput,
        );
        let stale_target = binding
            .sender_path_targets(FlowLane::Throughput, 64)
            .into_iter()
            .next()
            .expect("initial response output");
        binding.detach(key, &commands);
        let (replacement_commands, mut replacement_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                key.underlay,
                key.path_id,
                replacement_commands,
                FlowLane::Throughput,
                StreamOpenRole::Active,
                reliable_relay_buffer_len(MuxLimits::default()),
            ),
            ResponseStreamAttachOutcome::Attached
        );

        assert!(matches!(
            binding.try_enqueue_repair_frame_for_target(
                &stale_target,
                &stream_data_frame(64),
                FlowLane::Throughput,
            ),
            Err(RuntimeError::SenderServiceBlocked)
        ));
        assert!(try_recv_reliable_path_command(&mut receivers).is_none());
        assert!(try_recv_reliable_path_command(&mut replacement_receivers).is_none());
    }

    #[test]
    fn response_sender_targets_active_path_follows_ordered_data_owner_not_output_tail() {
        let (binding, active) = binding_for_underlay(UnderlayProtocol::Udp);
        let validation = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let (validation_commands, _validation_receivers) = reliable_path_command_channels(8);

        assert_eq!(
            binding.attach(
                validation.underlay,
                validation.path_id,
                validation_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                reliable_relay_buffer_len(MuxLimits::default()),
            ),
            ResponseStreamAttachOutcome::Attached
        );

        let targets = binding.sender_path_targets(FlowLane::Throughput, 4096);
        assert!(
            targets
                .iter()
                .find(|target| target.key == active)
                .is_some_and(|target| target.is_active && target.is_request_active),
            "the initial active output remains the scheduler-active target"
        );
        assert!(
            targets
                .iter()
                .find(|target| target.key == validation)
                .is_some_and(|target| !target.is_active),
            "validation output must not be active before lead migration"
        );

        binding.set_ordered_data_owner(validation);

        let targets = binding.sender_path_targets(FlowLane::Throughput, 4096);
        assert!(
            targets
                .iter()
                .find(|target| target.key == validation)
                .is_some_and(|target| target.is_active && !target.is_request_active),
            "scheduler-active target must follow ordered_data_owner after migration"
        );
        assert!(
            targets
                .iter()
                .find(|target| target.key == active)
                .is_some_and(|target| !target.is_active && target.is_request_active),
            "response owner migration must not overwrite the request Active identity"
        );
    }

    #[test]
    fn response_duplicate_active_attach_with_different_channel_is_rejected() {
        let (binding, active) = binding_for_underlay(UnderlayProtocol::Udp);
        let validation = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let (validation_commands, _validation_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                validation.underlay,
                validation.path_id,
                validation_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                reliable_relay_buffer_len(MuxLimits::default()),
            ),
            ResponseStreamAttachOutcome::Attached
        );
        let before = {
            let outputs = binding.outputs.lock().expect("test response outputs lock");
            outputs
                .entries
                .iter()
                .map(|entry| entry.key)
                .collect::<Vec<_>>()
        };
        let (duplicate_commands, _duplicate_receivers) = reliable_path_command_channels(8);

        assert_eq!(
            binding.attach(
                validation.underlay,
                validation.path_id,
                duplicate_commands,
                FlowLane::Throughput,
                StreamOpenRole::Active,
                reliable_relay_buffer_len(MuxLimits::default()),
            ),
            ResponseStreamAttachOutcome::RejectedDuplicateLiveOutput
        );

        let after = {
            let outputs = binding.outputs.lock().expect("test response outputs lock");
            outputs
                .entries
                .iter()
                .map(|entry| entry.key)
                .collect::<Vec<_>>()
        };
        assert_eq!(after, before);
        assert_eq!(binding.ordered_data_owner(), Some(active));
    }

    #[test]
    fn response_validation_same_channel_active_attach_does_not_promote_service_owner() {
        let (binding, active) = binding_for_underlay(UnderlayProtocol::Tcp);
        let validation = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let (validation_commands, _validation_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                validation.underlay,
                validation.path_id,
                validation_commands.clone(),
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                reliable_relay_buffer_len(MuxLimits::default()),
            ),
            ResponseStreamAttachOutcome::Attached
        );
        assert_eq!(binding.ordered_data_owner(), Some(active));

        assert_eq!(
            binding.attach(
                validation.underlay,
                validation.path_id,
                validation_commands,
                FlowLane::Throughput,
                StreamOpenRole::Active,
                reliable_relay_buffer_len(MuxLimits::default()),
            ),
            ResponseStreamAttachOutcome::RoleChanged
        );

        let outputs = binding.outputs.lock().expect("test response outputs lock");
        assert_eq!(
            outputs
                .entries
                .iter()
                .filter(|entry| entry.key == validation)
                .count(),
            1,
            "same-channel Active reannouncement updates the existing output instead of opening a duplicate"
        );
        drop(outputs);
        assert_eq!(
            binding.ordered_data_owner(),
            Some(active),
            "Active reannouncement is attachment state, not Service ownership"
        );
    }

    #[test]
    fn response_detaching_service_owner_does_not_promote_probe_only_survivor_to_service() {
        let (binding, active) = binding_for_underlay(UnderlayProtocol::Tcp);
        let survivor = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let (survivor_commands, _survivor_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                survivor.underlay,
                survivor.path_id,
                survivor_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                reliable_relay_buffer_len(MuxLimits::default()),
            ),
            ResponseStreamAttachOutcome::Attached
        );
        binding.update_path_metrics(
            survivor,
            PathMetrics {
                path_id: survivor.path_id,
                underlay: survivor.underlay,
                direction: PathMetricDirection::ServerToClient,
                metric_epoch: metric_epoch_now(),
                metric_age_us: 0,
                min_rtt_us: 20_000,
                srtt_us: 20_000,
                rttvar_us: 1_000,
                jitter_us: 1_000,
                delivery_rate_bps: 200_000_000,
                pacing_rate_bps: 200_000_000,
                loss_ppm: 0,
                ecn_ppm: 0,
                loss_observed: false,
                ecn_observed: false,
                bytes_in_flight: 0,
                queue_bytes: 0,
                inflight_limit_bytes: 0,
                inflight_hi_bytes: 0,
                confidence_ppm: 1_000_000,
                app_limited: true,
                has_ack_derived_data_sample: false,
                data_sample_count: 0,
                data_sample_bytes: 0,
            },
            ServerPathMetricsSource::LocalSender,
        );

        let active_commands = {
            let outputs = binding.outputs.lock().expect("test response outputs lock");
            outputs
                .entries
                .iter()
                .find(|entry| entry.key == active)
                .expect("active output exists")
                .commands
                .clone()
        };
        binding.detach(active, &active_commands);

        assert_eq!(
            binding.ordered_data_owner(),
            None,
            "proof/liveness evidence is not enough to promote a failover Service owner"
        );
        let targets = binding.sender_path_targets(FlowLane::Throughput, 4096);
        assert!(
            targets
                .iter()
                .find(|target| target.key == survivor)
                .is_some_and(|target| !target.is_active && target.has_sender_evidence),
            "probe-only survivor stays attached for validation but is not scheduler-active"
        );
    }

    #[test]
    fn response_detaching_service_owner_does_not_promote_ack_data_survivor() {
        let (binding, active) = binding_for_underlay(UnderlayProtocol::Tcp);
        let survivor = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let (survivor_commands, _survivor_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                survivor.underlay,
                survivor.path_id,
                survivor_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                reliable_relay_buffer_len(MuxLimits::default()),
            ),
            ResponseStreamAttachOutcome::Attached
        );
        binding.update_path_metrics(
            survivor,
            PathMetrics {
                path_id: survivor.path_id,
                underlay: survivor.underlay,
                direction: PathMetricDirection::ServerToClient,
                metric_epoch: metric_epoch_now(),
                metric_age_us: 0,
                min_rtt_us: 20_000,
                srtt_us: 20_000,
                rttvar_us: 1_000,
                jitter_us: 1_000,
                delivery_rate_bps: 614_000,
                pacing_rate_bps: 1_000_000,
                loss_ppm: 0,
                ecn_ppm: 0,
                loss_observed: false,
                ecn_observed: false,
                bytes_in_flight: PATH_OPEN_SCORE_BYTES as u64,
                queue_bytes: 0,
                inflight_limit_bytes: PATH_OPEN_SCORE_BYTES as u64,
                inflight_hi_bytes: PATH_OPEN_SCORE_BYTES as u64,
                confidence_ppm: 1_000_000,
                app_limited: true,
                has_ack_derived_data_sample: true,
                data_sample_count: 1,
                data_sample_bytes: 1,
            },
            ServerPathMetricsSource::LocalSender,
        );

        let active_commands = {
            let outputs = binding.outputs.lock().expect("test response outputs lock");
            outputs
                .entries
                .iter()
                .find(|entry| entry.key == active)
                .expect("active output exists")
                .commands
                .clone()
        };
        binding.detach(active, &active_commands);

        assert_eq!(
            binding.ordered_data_owner(),
            None,
            "carrier output detachment is not a Service ownership transfer; later OwnerData must wait for frontier-clear admission or repair"
        );
        let targets = binding.sender_path_targets(FlowLane::Throughput, 4096);
        assert!(
            targets
                .iter()
                .find(|target| target.key == survivor)
                .is_some_and(|target| !target.is_active && target.has_sender_evidence),
            "ACK-data survivor remains attached evidence, not the scheduler-active Service"
        );
    }

    #[test]
    fn response_service_detach_does_not_pick_measured_survivor_by_output_tail() {
        let (binding, active) = binding_for_underlay(UnderlayProtocol::Tcp);
        let measured = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let probe_only_tail = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        };
        let (measured_commands, _measured_receivers) = reliable_path_command_channels(8);
        let (probe_commands, _probe_receivers) = reliable_path_command_channels(8);

        assert_eq!(
            binding.attach(
                measured.underlay,
                measured.path_id,
                measured_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                reliable_relay_buffer_len(MuxLimits::default()),
            ),
            ResponseStreamAttachOutcome::Attached
        );
        binding.update_path_metrics(
            measured,
            PathMetrics {
                path_id: measured.path_id,
                underlay: measured.underlay,
                direction: PathMetricDirection::ServerToClient,
                metric_epoch: metric_epoch_now(),
                metric_age_us: 0,
                min_rtt_us: 20_000,
                srtt_us: 20_000,
                rttvar_us: 1_000,
                jitter_us: 1_000,
                delivery_rate_bps: 200_000_000,
                pacing_rate_bps: 200_000_000,
                loss_ppm: 0,
                ecn_ppm: 0,
                loss_observed: false,
                ecn_observed: false,
                bytes_in_flight: 0,
                queue_bytes: 0,
                inflight_limit_bytes: 0,
                inflight_hi_bytes: 0,
                confidence_ppm: 1_000_000,
                app_limited: false,
                has_ack_derived_data_sample: true,
                data_sample_count: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
                data_sample_bytes: MIN_RATE_SAMPLE_BYTES,
            },
            ServerPathMetricsSource::LocalSender,
        );
        assert_eq!(
            binding.attach(
                probe_only_tail.underlay,
                probe_only_tail.path_id,
                probe_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                reliable_relay_buffer_len(MuxLimits::default()),
            ),
            ResponseStreamAttachOutcome::Attached
        );

        let active_commands = {
            let outputs = binding.outputs.lock().expect("test response outputs lock");
            outputs
                .entries
                .iter()
                .find(|entry| entry.key == active)
                .expect("active output exists")
                .commands
                .clone()
        };
        binding.detach(active, &active_commands);

        assert_eq!(
            binding.ordered_data_owner(),
            None,
            "output membership changes are not Service admission; measured survivors compete only when ordered debt is clear"
        );
    }

    #[test]
    fn udp_stream_ack_releases_product_flight_without_seeding_carrier_rate() {
        let (binding, key) = binding_for_underlay(UnderlayProtocol::Udp);
        let frame = stream_data_frame(MIN_RATE_SAMPLE_BYTES as usize);

        binding.record_owner_flight(key, &frame);
        std::thread::sleep(Duration::from_millis(1));
        binding.release_normalized_acked_ranges(&[OffsetRange {
            start: 0,
            end: reliable_stream_frame_payload_bytes(&frame) as u64,
        }]);

        let entry = first_output_entry(&binding);
        assert_eq!(entry.bytes_in_flight, 0);
        assert_eq!(entry.delivery_samples, 1);
        assert_eq!(entry.owner_data_acked_bytes, MIN_RATE_SAMPLE_BYTES);
        assert!(entry.product_progress_rate_bps.is_some());
        assert!(entry.delivery_rate_bps.is_none());
        assert!(entry.srtt_ms.is_none());
    }

    #[test]
    fn tcp_first_stream_ack_is_progress_but_not_a_capacity_clock() {
        let (binding, key) = binding_for_underlay(UnderlayProtocol::Tcp);
        let frame = stream_data_frame(MIN_RATE_SAMPLE_BYTES as usize);

        binding.record_owner_flight(key, &frame);
        std::thread::sleep(Duration::from_millis(1));
        binding.release_normalized_acked_ranges(&[OffsetRange {
            start: 0,
            end: reliable_stream_frame_payload_bytes(&frame) as u64,
        }]);

        let entry = first_output_entry(&binding);
        assert_eq!(entry.bytes_in_flight, 0);
        assert_eq!(entry.delivery_samples, 1);
        assert_eq!(entry.owner_data_acked_bytes, MIN_RATE_SAMPLE_BYTES);
        assert!(entry.product_progress_rate_bps.is_none());
        assert!(entry.delivery_rate_bps.is_none());
        assert!(entry.tcp_ack_clock_rate_bps.is_none());
        assert!(entry.srtt_ms.is_none());
    }

    #[test]
    fn tcp_ordinary_ack_clock_excludes_assignment_queue_residence() {
        let (binding, key) = binding_for_underlay(UnderlayProtocol::Tcp);
        let window_bytes = PATH_OPEN_SCORE_BYTES;
        binding.record_owner_flight(key, &stream_data_frame_at(0, window_bytes));
        binding.record_owner_flight(
            key,
            &stream_data_frame_at(window_bytes as u64, window_bytes),
        );
        let clock = Instant::now();
        let first_ack_at = clock + Duration::from_secs(1);
        binding.release_normalized_acked_ranges_at(
            &[OffsetRange {
                start: 0,
                end: window_bytes as u64,
            }],
            first_ack_at,
        );
        let provisional = first_output_entry(&binding);
        assert!(provisional.tcp_ack_clock_rate_bps.is_none());
        assert!(provisional.product_progress_rate_bps.is_none());
        assert!(provisional.delivery_rate_bps.is_none());
        assert!(provisional.srtt_ms.is_none());

        let second_ack_at = first_ack_at + Duration::from_millis(100);
        binding.release_normalized_acked_ranges_at(
            &[OffsetRange {
                start: window_bytes as u64,
                end: (2 * window_bytes) as u64,
            }],
            second_ack_at,
        );
        let expected_rate = window_bytes as f64 * 8.0 / 0.1;
        let measured = first_output_entry(&binding);
        assert_test_rate_close(measured.tcp_ack_clock_rate_bps, expected_rate);
        assert_test_rate_close(measured.product_progress_rate_bps, expected_rate);
        assert_test_rate_close(measured.delivery_rate_bps, expected_rate);

        let late_offset = (2 * window_bytes) as u64;
        binding.record_owner_flight(key, &stream_data_frame_at(late_offset, window_bytes));
        {
            let mut flights = binding
                .flights
                .lock()
                .expect("server reliable stream flight lock");
            flights
                .get_mut(&late_offset)
                .expect("late flight")
                .iter_mut()
                .for_each(|flight| flight.sent_at = second_ack_at + Duration::from_millis(1));
        }
        binding.release_normalized_acked_ranges_at(
            &[OffsetRange {
                start: late_offset,
                end: late_offset + window_bytes as u64,
            }],
            second_ack_at + Duration::from_millis(100),
        );
        let after_late_assignment = first_output_entry(&binding);
        assert_test_rate_close(after_late_assignment.tcp_ack_clock_rate_bps, expected_rate);
        assert_test_rate_close(after_late_assignment.delivery_rate_bps, expected_rate);

        let late_ack_at = second_ack_at + Duration::from_millis(100);
        let recovery_offset = late_offset + window_bytes as u64;
        binding.record_owner_flight(key, &stream_data_frame_at(recovery_offset, window_bytes));
        {
            let mut flights = binding
                .flights
                .lock()
                .expect("server reliable stream flight lock");
            flights
                .get_mut(&recovery_offset)
                .expect("recovery flight")
                .iter_mut()
                .for_each(|flight| flight.sent_at = late_ack_at - Duration::from_millis(1));
        }
        binding.release_normalized_acked_ranges_at(
            &[OffsetRange {
                start: recovery_offset,
                end: recovery_offset + window_bytes as u64,
            }],
            late_ack_at + Duration::from_millis(50),
        );
        let recovered_rate = (3 * window_bytes) as f64 * 8.0 / 0.25;
        let recovered = first_output_entry(&binding);
        assert_test_rate_close(recovered.tcp_ack_clock_rate_bps, recovered_rate);
        assert_test_rate_close(recovered.delivery_rate_bps, recovered_rate);
    }

    #[test]
    fn tcp_ack_clock_can_reduce_rate_while_carrier_snapshot_is_app_limited() {
        let (binding, key) = binding_for_underlay(UnderlayProtocol::Tcp);
        let window_bytes = PATH_OPEN_SCORE_BYTES;
        binding.update_path_metrics(
            key,
            PathMetrics {
                path_id: key.path_id,
                underlay: key.underlay,
                direction: PathMetricDirection::ServerToClient,
                metric_epoch: metric_epoch_now(),
                metric_age_us: 0,
                min_rtt_us: 20_000,
                srtt_us: 20_000,
                rttvar_us: 1_000,
                jitter_us: 1_000,
                delivery_rate_bps: 100_000_000,
                pacing_rate_bps: 100_000_000,
                loss_ppm: 0,
                ecn_ppm: 0,
                loss_observed: false,
                ecn_observed: false,
                bytes_in_flight: 0,
                queue_bytes: 0,
                inflight_limit_bytes: window_bytes as u64,
                inflight_hi_bytes: window_bytes as u64,
                confidence_ppm: 1_000_000,
                app_limited: true,
                has_ack_derived_data_sample: false,
                data_sample_count: 0,
                data_sample_bytes: 0,
            },
            ServerPathMetricsSource::LocalSender,
        );
        {
            let mut outputs = binding.outputs.lock().expect("test response outputs lock");
            let entry = outputs.entries.first_mut().expect("TCP output");
            entry.tcp_ack_clock_rate_bps = Some(100_000_000.0);
            entry.product_progress_rate_bps = Some(100_000_000.0);
            entry.delivery_rate_bps = Some(100_000_000.0);
        }
        binding.record_owner_flight(key, &stream_data_frame_at(0, window_bytes));
        binding.record_owner_flight(
            key,
            &stream_data_frame_at(window_bytes as u64, window_bytes),
        );
        let first_ack_at = Instant::now() + Duration::from_millis(100);
        binding.release_normalized_acked_ranges_at(
            &[OffsetRange {
                start: 0,
                end: window_bytes as u64,
            }],
            first_ack_at,
        );
        binding.release_normalized_acked_ranges_at(
            &[OffsetRange {
                start: window_bytes as u64,
                end: (2 * window_bytes) as u64,
            }],
            first_ack_at + Duration::from_millis(100),
        );

        let entry = first_output_entry(&binding);
        assert!(
            entry
                .tcp_ack_clock_rate_bps
                .is_some_and(|rate| rate < 100_000_000.0),
            "per-flow TCP ACK evidence must not inherit QUIC's app-limited max filter"
        );
    }

    #[test]
    fn tcp_ack_clock_is_independent_from_global_contiguous_frontier() {
        let (binding, key) = binding_for_underlay(UnderlayProtocol::Tcp);
        let window_bytes = PATH_OPEN_SCORE_BYTES;
        binding.record_owner_flight(key, &stream_data_frame_at(0, window_bytes));
        binding.record_owner_flight(
            key,
            &stream_data_frame_at(window_bytes as u64, window_bytes),
        );
        let first_ack_at = Instant::now() + Duration::from_secs(1);
        binding.release_normalized_acked_ranges_at(
            &[OffsetRange {
                start: window_bytes as u64,
                end: (2 * window_bytes) as u64,
            }],
            first_ack_at,
        );
        let hole = first_output_entry(&binding);
        assert!(hole.tcp_ack_clock_rate_bps.is_none());

        binding.release_normalized_acked_ranges_at(
            &[OffsetRange {
                start: 0,
                end: window_bytes as u64,
            }],
            first_ack_at + Duration::from_millis(100),
        );
        let expected_rate = window_bytes as f64 * 8.0 / 0.1;
        let measured = first_output_entry(&binding);
        assert_test_rate_close(measured.tcp_ack_clock_rate_bps, expected_rate);
    }

    #[test]
    fn tcp_response_single_stage_ack_clock_sample_preserves_startup_rate() {
        let (binding, key) = binding_for_underlay(UnderlayProtocol::Tcp);
        let window_bytes = PATH_OPEN_SCORE_BYTES;
        let identity = {
            let mut outputs = binding.outputs.lock().expect("test response outputs lock");
            let entry = outputs.entries.first_mut().expect("TCP output");
            entry.product_progress_rate_bps = Some(1.0);
            entry.delivery_rate_bps = Some(1.0);
            let identity = (entry.key, entry.incarnation);
            let mut calibration = ResponseAckClockCalibrationState::new(
                (2 * window_bytes) as u64,
                (2 * window_bytes) as u64,
            );
            calibration.spent_bytes = (2 * window_bytes) as u64;
            outputs.ack_clock_calibrations.insert(identity, calibration);
            outputs.active_ack_clock_calibration = Some(identity);
            identity
        };
        binding.record_owner_flight(key, &stream_data_frame_at(0, window_bytes));
        binding.record_owner_flight(
            key,
            &stream_data_frame_at(window_bytes as u64, window_bytes),
        );
        std::thread::sleep(Duration::from_millis(2));
        binding.release_normalized_acked_ranges(&[OffsetRange {
            start: 0,
            end: window_bytes as u64,
        }]);
        {
            let outputs = binding.outputs.lock().expect("test response outputs lock");
            assert!(
                !outputs
                    .ack_clock_calibrations
                    .get(&identity)
                    .expect("calibration state")
                    .proven,
                "the first send-to-ACK window remains provisional"
            );
            assert_eq!(outputs.active_ack_clock_calibration, Some(identity));
        }

        std::thread::sleep(Duration::from_millis(2));
        binding.release_normalized_acked_ranges(&[OffsetRange {
            start: window_bytes as u64,
            end: (2 * window_bytes) as u64,
        }]);
        let outputs = binding.outputs.lock().expect("test response outputs lock");
        let entry = outputs.entries.first().expect("TCP output");
        assert!(
            outputs
                .ack_clock_calibrations
                .get(&identity)
                .expect("calibration state")
                .proven,
            "the later window was already in flight at the previous ACK"
        );
        assert_eq!(entry.delivery_rate_bps, Some(1.0));
        assert_eq!(entry.product_progress_rate_bps, Some(1.0));
        assert_eq!(
            outputs
                .ack_clock_calibrations
                .get(&identity)
                .expect("calibration state")
                .calibrated_rate_bps,
            None,
            "one compressed stage sample cannot replace the startup rate"
        );
        assert_eq!(outputs.active_ack_clock_calibration, None);
        drop(outputs);

        let next_offset = (2 * window_bytes) as u64;
        binding.record_owner_flight(
            key,
            &stream_data_frame_at(next_offset, MIN_RATE_SAMPLE_BYTES as usize),
        );
        binding.record_owner_flight(
            key,
            &stream_data_frame_at(
                next_offset + MIN_RATE_SAMPLE_BYTES,
                MIN_RATE_SAMPLE_BYTES as usize,
            ),
        );
        let ordinary_ack_at = Instant::now() + RESPONSE_ACK_CLOCK_GOODPUT_MIN_ELAPSED;
        binding.release_normalized_acked_ranges_at(
            &[OffsetRange {
                start: next_offset,
                end: next_offset + MIN_RATE_SAMPLE_BYTES,
            }],
            ordinary_ack_at,
        );
        binding.release_normalized_acked_ranges_at(
            &[OffsetRange {
                start: next_offset + MIN_RATE_SAMPLE_BYTES,
                end: next_offset + 2 * MIN_RATE_SAMPLE_BYTES,
            }],
            ordinary_ack_at + RESPONSE_ACK_CLOCK_GOODPUT_MIN_ELAPSED,
        );
        let entry = first_output_entry(&binding);
        assert!(
            entry.delivery_rate_bps.is_some_and(|rate| rate > 1.0),
            "a terminal calibration without a robust rate must not freeze later ordinary TCP evidence"
        );
    }

    #[test]
    fn tcp_response_robust_calibration_replaces_poisoned_rate_without_fake_rtt() {
        let (binding, key) = binding_for_underlay(UnderlayProtocol::Tcp);
        let identity = {
            let mut outputs = binding.outputs.lock().expect("test response outputs lock");
            let entry = outputs.entries.first_mut().expect("TCP output");
            entry.product_progress_rate_bps = Some(7_000_000_000.0);
            entry.delivery_rate_bps = Some(7_000_000_000.0);
            entry.srtt_ms = Some(10.0);
            let identity = (entry.key, entry.incarnation);
            let initial = PATH_OPEN_SCORE_BYTES as u64;
            let mut calibration = ResponseAckClockCalibrationState::new(initial, 4 * initial);
            for sample_bps in [90_000_000.0, 7_000_000_000.0, 110_000_000.0] {
                calibration.spent_bytes = calibration.credit_limit_bytes;
                let stage_authorized_at = calibration.stage_authorized_at;
                let sample = test_ack_clock_rate_sample(
                    calibration.stage_rate_coverage_floor_bytes,
                    sample_bps,
                );
                let _ = calibration.record_ack_clock_sample(
                    sample,
                    stage_authorized_at + Duration::from_millis(1),
                    stage_authorized_at + Duration::from_millis(10),
                );
            }
            assert_test_rate_close(calibration.calibrated_rate_bps, 110_000_000.0);
            outputs.ack_clock_calibrations.insert(identity, calibration);
            identity
        };
        let sample_bytes = 4096;
        binding.record_owner_flight(key, &stream_data_frame_at(0, sample_bytes));
        std::thread::sleep(Duration::from_millis(1));
        binding.release_normalized_acked_ranges(&[OffsetRange {
            start: 0,
            end: sample_bytes as u64,
        }]);

        let outputs = binding.outputs.lock().expect("test response outputs lock");
        let entry = outputs
            .entries
            .iter()
            .find(|entry| (entry.key, entry.incarnation) == identity)
            .expect("TCP output");
        assert_test_rate_close(entry.product_progress_rate_bps, 110_000_000.0);
        assert_test_rate_close(entry.delivery_rate_bps, 110_000_000.0);
        assert_test_rate_close(entry.tcp_ack_clock_rate_bps, 110_000_000.0);
        assert_eq!(entry.srtt_ms, Some(10.0));
        drop(outputs);

        let rtt_sample_bytes = PATH_OPEN_SCORE_BYTES;
        binding.record_owner_flight(
            key,
            &stream_data_frame_at(sample_bytes as u64, rtt_sample_bytes),
        );
        std::thread::sleep(Duration::from_millis(1));
        binding.release_normalized_acked_ranges(&[OffsetRange {
            start: sample_bytes as u64,
            end: sample_bytes as u64 + rtt_sample_bytes as u64,
        }]);
        let outputs = binding.outputs.lock().expect("test response outputs lock");
        let entry = outputs
            .entries
            .iter()
            .find(|entry| (entry.key, entry.incarnation) == identity)
            .expect("TCP output");
        assert_test_rate_close(entry.product_progress_rate_bps, 110_000_000.0);
        assert_test_rate_close(entry.delivery_rate_bps, 110_000_000.0);
        assert_eq!(
            entry.srtt_ms,
            Some(10.0),
            "scheduler assignment time is not a TCP dispatch or RTT timestamp"
        );
    }

    #[test]
    fn tcp_response_active_calibration_remainder_honors_state_boundaries() {
        let (binding, _key) = binding_for_underlay(UnderlayProtocol::Tcp);
        let stage_limit = (2 * 1024 * 1024) as u64;
        let residual = 4032_u64;
        let identity = {
            let mut outputs = binding.outputs.lock().expect("test response outputs lock");
            let entry = outputs.entries.first().expect("TCP output");
            let identity = (entry.key, entry.incarnation);
            let mut calibration =
                ResponseAckClockCalibrationState::new(stage_limit, 4 * stage_limit);
            calibration.spent_bytes = stage_limit - residual;
            outputs.ack_clock_calibrations.insert(identity, calibration);
            outputs.active_ack_clock_calibration = Some(identity);
            identity
        };

        assert_eq!(
            binding.active_tcp_ack_clock_calibration_remaining_bytes(),
            Some(residual as usize),
        );

        {
            let mut outputs = binding.outputs.lock().expect("test response outputs lock");
            outputs
                .ack_clock_calibrations
                .get_mut(&identity)
                .expect("calibration state")
                .spent_bytes = stage_limit;
        }
        assert_eq!(
            binding.active_tcp_ack_clock_calibration_remaining_bytes(),
            None,
            "an exhausted stage returns to Service while it awaits ACK evidence",
        );

        {
            let mut outputs = binding.outputs.lock().expect("test response outputs lock");
            let calibration = outputs
                .ack_clock_calibrations
                .get_mut(&identity)
                .expect("calibration state");
            calibration.spent_bytes = stage_limit - 1;
        }
        assert_eq!(
            binding.active_tcp_ack_clock_calibration_remaining_bytes(),
            Some(1),
            "a one-byte residual must not be expanded to a minimum quantum",
        );

        {
            let mut outputs = binding.outputs.lock().expect("test response outputs lock");
            outputs
                .ack_clock_calibrations
                .get_mut(&identity)
                .expect("calibration state")
                .proven = true;
        }
        assert_eq!(
            binding.active_tcp_ack_clock_calibration_remaining_bytes(),
            None
        );
        {
            let mut outputs = binding.outputs.lock().expect("test response outputs lock");
            let calibration = outputs
                .ack_clock_calibrations
                .get_mut(&identity)
                .expect("calibration state");
            calibration.proven = false;
            calibration.retired = true;
        }
        assert_eq!(
            binding.active_tcp_ack_clock_calibration_remaining_bytes(),
            None
        );
        {
            let mut outputs = binding.outputs.lock().expect("test response outputs lock");
            outputs.active_ack_clock_calibration = Some((
                CarrierPathKey {
                    underlay: UnderlayProtocol::Tcp,
                    path_id: PathId(99),
                },
                99,
            ));
        }
        assert_eq!(
            binding.active_tcp_ack_clock_calibration_remaining_bytes(),
            None
        );

        let (udp_binding, _udp_key) = binding_for_underlay(UnderlayProtocol::Udp);
        {
            let mut outputs = udp_binding
                .outputs
                .lock()
                .expect("test response outputs lock");
            let entry = outputs.entries.first().expect("UDP output");
            let identity = (entry.key, entry.incarnation);
            outputs.ack_clock_calibrations.insert(
                identity,
                ResponseAckClockCalibrationState::new(stage_limit, 4 * stage_limit),
            );
            outputs.active_ack_clock_calibration = Some(identity);
        }
        assert_eq!(
            udp_binding.active_tcp_ack_clock_calibration_remaining_bytes(),
            None,
            "QUIC/UDP product frames stay under the carrier-local controller",
        );
    }

    #[test]
    fn tcp_response_mixed_window_consumes_fresh_capacity_without_publishing_rate() {
        let (binding, key) = binding_for_underlay(UnderlayProtocol::Tcp);
        let window_bytes = PATH_OPEN_SCORE_BYTES;
        let identity = {
            let mut outputs = binding.outputs.lock().expect("test response outputs lock");
            let entry = outputs.entries.first().expect("TCP output");
            let identity = (entry.key, entry.incarnation);
            let mut calibration = ResponseAckClockCalibrationState::new(
                (2 * window_bytes) as u64,
                (2 * window_bytes) as u64,
            );
            calibration.spent_bytes = (2 * window_bytes) as u64;
            outputs.ack_clock_calibrations.insert(identity, calibration);
            outputs.active_ack_clock_calibration = Some(identity);
            identity
        };
        binding.record_owner_flight(key, &stream_data_frame_at(0, window_bytes));
        binding.record_owner_flight(key, &stream_data_frame_at(window_bytes as u64, 1));
        binding.release_normalized_acked_ranges(&[OffsetRange {
            start: 0,
            end: window_bytes as u64,
        }]);

        binding.record_owner_flight(
            key,
            &stream_data_frame_at(window_bytes as u64 + 1, window_bytes.saturating_sub(1)),
        );
        binding.release_normalized_acked_ranges(&[OffsetRange {
            start: window_bytes as u64,
            end: (2 * window_bytes) as u64,
        }]);

        let outputs = binding.outputs.lock().expect("test response outputs lock");
        let calibration = outputs
            .ack_clock_calibrations
            .get(&identity)
            .expect("calibration state");
        assert_eq!(calibration.calibrated_rate_bps, None);
        assert!(
            calibration.proven,
            "the hard-capped stage cannot recover a representative strict window"
        );
        assert_eq!(
            outputs.active_ack_clock_calibration, None,
            "a terminal stage without causal evidence retires after exact flights drain"
        );
    }

    #[test]
    fn later_owner_ack_window_proves_tcp_but_not_udp_without_carrier_evidence() {
        let sample_bytes = reliable_subflow_startup_sample_limit_bytes(MuxLimits::default());
        let frame_bytes = BBR_MAX_SEND_QUANTUM_BYTES as u64;
        assert_eq!(sample_bytes % frame_bytes, 0);

        for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
            let (binding, key) = binding_for_underlay(underlay);
            for offset in (0..2 * sample_bytes).step_by(BBR_MAX_SEND_QUANTUM_BYTES) {
                binding.record_owner_flight(
                    key,
                    &stream_data_frame_at(offset, BBR_MAX_SEND_QUANTUM_BYTES),
                );
            }
            let first_ack = Instant::now() + RESPONSE_ACK_CLOCK_GOODPUT_MIN_ELAPSED;
            binding.release_normalized_acked_ranges_at(
                &[OffsetRange {
                    start: 0,
                    end: sample_bytes,
                }],
                first_ack,
            );
            binding.release_normalized_acked_ranges_at(
                &[OffsetRange {
                    start: sample_bytes,
                    end: 2 * sample_bytes,
                }],
                first_ack + RESPONSE_ACK_CLOCK_GOODPUT_MIN_ELAPSED,
            );

            let entry = first_output_entry(&binding);
            assert_eq!(
                entry.owner_data_acked_bytes,
                2 * sample_bytes,
                "{underlay:?}"
            );
            assert!(entry.product_progress_rate_bps.is_some(), "{underlay:?}");
            assert_eq!(
                server_output_has_bulk_rate_evidence(&entry),
                underlay == UnderlayProtocol::Tcp,
                "TCP may use product owner ACKs; QUIC requires local carrier bulk evidence"
            );
        }
    }

    #[test]
    fn tcp_response_startup_ack_graduates_epoch_and_admits_next_candidate() {
        let limits = MuxLimits::default();
        let sample_bytes = reliable_subflow_startup_sample_limit_bytes(limits) as usize;
        let (binding, service) = binding_for_underlay(UnderlayProtocol::Tcp);
        let first = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let second = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(2),
        };
        let (first_commands, _first_receivers) = reliable_path_command_channels(8);
        let (second_commands, _second_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                first.underlay,
                first.path_id,
                first_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                reliable_relay_buffer_len(limits),
            ),
            ResponseStreamAttachOutcome::Attached
        );
        assert_eq!(
            binding.attach(
                second.underlay,
                second.path_id,
                second_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                reliable_relay_buffer_len(limits),
            ),
            ResponseStreamAttachOutcome::Attached
        );
        let startup_input = |key| SubflowAdmissionInput {
            key,
            bulk_rate_proven: false,
            startup_owner_allowed: true,
            frontier_clear: true,
            completion_improves: false,
            observed_goodput_non_degrading: true,
            read_gap: Duration::ZERO,
            owner_bytes: sample_bytes,
            optional_overhead_bytes: 0,
        };

        assert_eq!(
            binding
                .commit_subflow_owner_admission(
                    service,
                    sample_bytes,
                    0,
                    Duration::ZERO,
                    startup_input(first),
                )
                .decision,
            PathAdmissionDecision::AdmitSubflow
        );
        assert_eq!(
            binding
                .commit_subflow_owner_admission(
                    service,
                    sample_bytes,
                    0,
                    Duration::ZERO,
                    startup_input(second),
                )
                .decision,
            PathAdmissionDecision::ProbeOnly,
            "only one unproven response candidate may own startup bytes"
        );
        let generation_before_ack = binding.subflow_state_snapshot().0;
        binding.record_owner_flight(first, &stream_data_frame(sample_bytes));
        std::thread::sleep(Duration::from_millis(1));
        binding.release_normalized_acked_ranges(&[OffsetRange {
            start: 0,
            end: sample_bytes as u64,
        }]);

        let (generation_after_ack, epoch) = binding.subflow_state_snapshot();
        assert_ne!(generation_after_ack, generation_before_ack);
        assert_eq!(
            epoch.as_ref().and_then(FlowSubflowSet::startup_owner_key),
            None,
            "exact TCP OwnerData ACK evidence should graduate the sampled response path"
        );
        {
            let outputs = binding.outputs.lock().expect("test response outputs lock");
            let entry = outputs
                .entries
                .iter()
                .find(|entry| entry.key == first)
                .expect("graduated TCP output remains attached");
            assert!(
                outputs
                    .ack_clock_calibrations
                    .contains_key(&(entry.key, entry.incarnation)),
                "TCP graduation creates an exact-incarnation ACK-clock phase"
            );
        }
        assert_eq!(
            binding
                .commit_subflow_owner_admission(
                    service,
                    sample_bytes,
                    0,
                    Duration::ZERO,
                    startup_input(second),
                )
                .decision,
            PathAdmissionDecision::AdmitSubflow
        );
        assert_eq!(
            binding
                .subflow_state_snapshot()
                .1
                .and_then(|epoch| epoch.startup_owner_key()),
            Some(second)
        );
    }

    #[test]
    fn tcp_response_graduation_skips_calibration_below_ack_sample_resource_floor() {
        let limits = MuxLimits {
            max_path_flight_bytes: PATH_OPEN_SCORE_BYTES - 1,
            ..MuxLimits::default()
        };
        let sample_bytes = reliable_subflow_startup_sample_limit_bytes(limits) as usize;
        let service = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let candidate = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let (service_commands, _service_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new_with_limits(
            SessionId(43),
            service.underlay,
            service.path_id,
            service_commands,
            FlowLane::Throughput,
            limits,
        );
        let (candidate_commands, _candidate_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                candidate.underlay,
                candidate.path_id,
                candidate_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                reliable_relay_buffer_len(limits),
            ),
            ResponseStreamAttachOutcome::Attached
        );
        assert_eq!(
            binding
                .commit_subflow_owner_admission(
                    service,
                    sample_bytes,
                    0,
                    Duration::ZERO,
                    SubflowAdmissionInput {
                        key: candidate,
                        bulk_rate_proven: false,
                        startup_owner_allowed: true,
                        frontier_clear: true,
                        completion_improves: false,
                        observed_goodput_non_degrading: true,
                        read_gap: Duration::ZERO,
                        owner_bytes: sample_bytes,
                        optional_overhead_bytes: 0,
                    },
                )
                .decision,
            PathAdmissionDecision::AdmitSubflow
        );
        binding.record_owner_flight(candidate, &stream_data_frame(sample_bytes));
        binding.release_normalized_acked_ranges(&[OffsetRange {
            start: 0,
            end: sample_bytes as u64,
        }]);
        assert_eq!(
            binding
                .subflow_state_snapshot()
                .1
                .and_then(|epoch| epoch.startup_owner_key()),
            None,
            "a fully ACKed TCP startup sample may graduate without inventing a rate"
        );
        let candidate_target = binding
            .sender_path_targets(FlowLane::Throughput, 1)
            .into_iter()
            .find(|target| target.key == candidate)
            .expect("graduated candidate target");
        assert!(!candidate_target.ack_clock_calibration_eligible);
    }

    #[test]
    fn udp_response_startup_requires_local_carrier_bulk_evidence_to_graduate() {
        let limits = MuxLimits::default();
        let sample_bytes = reliable_subflow_startup_sample_limit_bytes(limits) as usize;
        let (binding, service) = binding_for_underlay(UnderlayProtocol::Udp);
        let first = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let second = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(2),
        };
        let (first_commands, _first_receivers) = reliable_path_command_channels(8);
        let (second_commands, _second_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                first.underlay,
                first.path_id,
                first_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                reliable_relay_buffer_len(limits),
            ),
            ResponseStreamAttachOutcome::Attached
        );
        assert_eq!(
            binding.attach(
                second.underlay,
                second.path_id,
                second_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                reliable_relay_buffer_len(limits),
            ),
            ResponseStreamAttachOutcome::Attached
        );
        let startup_input = |key| SubflowAdmissionInput {
            key,
            bulk_rate_proven: false,
            startup_owner_allowed: true,
            frontier_clear: true,
            completion_improves: false,
            observed_goodput_non_degrading: true,
            read_gap: Duration::ZERO,
            owner_bytes: sample_bytes,
            optional_overhead_bytes: 0,
        };

        assert_eq!(
            binding
                .commit_subflow_owner_admission(
                    service,
                    sample_bytes,
                    0,
                    Duration::ZERO,
                    startup_input(first),
                )
                .decision,
            PathAdmissionDecision::AdmitSubflow
        );
        let generation_before_ack = binding.subflow_state_snapshot().0;
        binding.record_owner_flight(first, &stream_data_frame(sample_bytes));
        std::thread::sleep(Duration::from_millis(1));
        binding.release_normalized_acked_ranges(&[OffsetRange {
            start: 0,
            end: sample_bytes as u64,
        }]);

        let (generation_after_ack, epoch) = binding.subflow_state_snapshot();
        assert_eq!(generation_after_ack, generation_before_ack);
        assert_eq!(
            epoch.as_ref().and_then(FlowSubflowSet::startup_owner_key),
            Some(first),
            "UDP product ACKs alone must not graduate a QUIC response Subflow"
        );
        assert_eq!(
            binding
                .commit_subflow_owner_admission(
                    service,
                    sample_bytes,
                    0,
                    Duration::ZERO,
                    startup_input(second),
                )
                .decision,
            PathAdmissionDecision::ProbeOnly
        );

        binding.update_path_metrics(
            first,
            PathMetrics {
                path_id: first.path_id,
                underlay: first.underlay,
                direction: PathMetricDirection::ServerToClient,
                metric_epoch: metric_epoch_now(),
                metric_age_us: 0,
                min_rtt_us: 80_000,
                srtt_us: 80_000,
                rttvar_us: 5_000,
                jitter_us: 5_000,
                delivery_rate_bps: 200_000_000,
                pacing_rate_bps: 200_000_000,
                loss_ppm: 0,
                ecn_ppm: 0,
                loss_observed: false,
                ecn_observed: false,
                bytes_in_flight: 0,
                queue_bytes: 0,
                inflight_limit_bytes: sample_bytes as u64,
                inflight_hi_bytes: sample_bytes as u64,
                confidence_ppm: 1_000_000,
                app_limited: false,
                has_ack_derived_data_sample: true,
                data_sample_count: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
                data_sample_bytes: sample_bytes as u64,
            },
            ServerPathMetricsSource::LocalSender,
        );

        let (generation_after_carrier_proof, epoch) = binding.subflow_state_snapshot();
        assert_ne!(generation_after_carrier_proof, generation_after_ack);
        assert_eq!(
            epoch.as_ref().and_then(FlowSubflowSet::startup_owner_key),
            None
        );
        assert!(
            binding
                .outputs
                .lock()
                .expect("test response outputs lock")
                .ack_clock_calibrations
                .is_empty(),
            "UDP/QUIC graduation remains carrier-owned and never enters TCP calibration"
        );
        assert_eq!(
            binding
                .commit_subflow_owner_admission(
                    service,
                    sample_bytes,
                    0,
                    Duration::ZERO,
                    startup_input(second),
                )
                .decision,
            PathAdmissionDecision::AdmitSubflow
        );
    }

    #[test]
    fn duplicate_response_validation_copy_does_not_become_ordering_owner() {
        let (binding, owner) = binding_for_underlay(UnderlayProtocol::Tcp);
        let duplicate = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let (duplicate_commands, _duplicate_receivers) = reliable_path_command_channels(8);
        binding.attach(
            duplicate.underlay,
            duplicate.path_id,
            duplicate_commands,
            FlowLane::Throughput,
            StreamOpenRole::Repair,
            reliable_relay_buffer_len(MuxLimits::default()),
        );
        let frame = stream_data_frame_at(0, 4096);

        binding.record_owner_flight(owner, &frame);
        binding.record_repair_flight(duplicate, &frame);
        let owner_identity = {
            let mut outputs = binding.outputs.lock().expect("test response outputs lock");
            let entry = outputs
                .entries
                .iter()
                .find(|entry| entry.key == owner)
                .expect("owner output exists");
            let identity = (entry.key, entry.incarnation);
            let mut calibration = ResponseAckClockCalibrationState::new(
                PATH_OPEN_SCORE_BYTES as u64,
                PATH_OPEN_SCORE_BYTES as u64,
            );
            calibration.spent_bytes = PATH_OPEN_SCORE_BYTES as u64;
            outputs.ack_clock_calibrations.insert(identity, calibration);
            identity
        };

        let lower = binding.lower_flights_before_offset(4096);
        assert!(
            lower.is_empty(),
            "plain unacked owner flight is recovery state, not authoritative ordering debt"
        );

        binding.release_normalized_acked_ranges(&[OffsetRange {
            start: 0,
            end: 4096,
        }]);
        let entries = binding.outputs.lock().expect("test response outputs lock");
        let owner_entry = entries
            .entries
            .iter()
            .find(|entry| entry.key == owner)
            .expect("owner output exists");
        let duplicate_entry = entries
            .entries
            .iter()
            .find(|entry| entry.key == duplicate)
            .expect("duplicate output exists");
        assert_eq!(owner_entry.bytes_in_flight, 0);
        assert_eq!(duplicate_entry.bytes_in_flight, 0);
        assert_eq!(
            owner_entry.delivery_samples, 0,
            "ACK of a duplicated byte range is not path-scoped proof for the owner path"
        );
        assert_eq!(
            duplicate_entry.delivery_samples, 0,
            "repair duplicate STREAM_ACK must not become response bulk evidence"
        );
        assert_eq!(owner_entry.owner_data_acked_bytes, 0);
        assert_eq!(duplicate_entry.owner_data_acked_bytes, 0);
        assert!(owner_entry.tcp_product_rate_evidence.is_none());
        assert!(
            entries
                .ack_clock_calibrations
                .get(&owner_identity)
                .expect("owner calibration state")
                .rate_evidence
                .is_none(),
            "ambiguous OwnerData/RepairData ACKs cannot advance the TCP ACK clock"
        );
    }

    #[test]
    fn partial_same_start_response_ack_releases_each_copy_and_retains_owner_suffix() {
        let (binding, owner) = binding_for_underlay(UnderlayProtocol::Tcp);
        let repair = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let (repair_commands, _repair_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                repair.underlay,
                repair.path_id,
                repair_commands,
                FlowLane::Latency,
                StreamOpenRole::Repair,
                reliable_relay_buffer_len(MuxLimits::default()),
            ),
            ResponseStreamAttachOutcome::Attached,
        );
        binding.record_owner_flight(owner, &stream_data_frame_at(0, 4096));
        binding.record_repair_flight(repair, &stream_data_frame_at(0, 1024));

        {
            let outputs = binding.outputs.lock().expect("test response outputs lock");
            let owner_entry = outputs
                .entries
                .iter()
                .find(|entry| entry.key == owner)
                .expect("owner output exists");
            let repair_entry = outputs
                .entries
                .iter()
                .find(|entry| entry.key == repair)
                .expect("repair output exists");
            assert_eq!(owner_entry.owner_data_in_flight_bytes, 4096);
            assert_eq!(owner_entry.bytes_in_flight, 4096);
            assert_eq!(repair_entry.owner_data_in_flight_bytes, 0);
            assert_eq!(repair_entry.bytes_in_flight, 1024);
        }

        binding.release_normalized_acked_ranges(&[OffsetRange {
            start: 0,
            end: 1024,
        }]);
        {
            let outputs = binding.outputs.lock().expect("test response outputs lock");
            let owner_entry = outputs
                .entries
                .iter()
                .find(|entry| entry.key == owner)
                .expect("owner output exists");
            let repair_entry = outputs
                .entries
                .iter()
                .find(|entry| entry.key == repair)
                .expect("repair output exists");
            assert_eq!(owner_entry.bytes_in_flight, 3072);
            assert_eq!(repair_entry.bytes_in_flight, 0);
            assert_eq!(owner_entry.owner_data_in_flight_bytes, 3072);
            assert_eq!(repair_entry.owner_data_in_flight_bytes, 0);
            assert_eq!(
                owner_entry.delivery_samples, 0,
                "the duplicated prefix ACK is not path-scoped owner evidence"
            );
            assert_eq!(repair_entry.delivery_samples, 0);
            assert_eq!(owner_entry.owner_data_acked_bytes, 0);
            assert_eq!(repair_entry.owner_data_acked_bytes, 0);
        }
        let owner_suffix = stream_data_frame_at(1024, 3072);
        assert_eq!(
            binding.owner_flight_keys_overlapping_frame(&owner_suffix),
            vec![owner],
            "the longer owner flight must survive after its shorter same-start repair copy is released"
        );
        assert_eq!(
            binding.flight_keys_overlapping_frame(&owner_suffix),
            vec![owner]
        );

        binding.release_normalized_acked_ranges(&[OffsetRange {
            start: 1024,
            end: 4096,
        }]);
        let outputs = binding.outputs.lock().expect("test response outputs lock");
        let owner_entry = outputs
            .entries
            .iter()
            .find(|entry| entry.key == owner)
            .expect("owner output exists");
        let repair_entry = outputs
            .entries
            .iter()
            .find(|entry| entry.key == repair)
            .expect("repair output exists");
        assert_eq!(owner_entry.bytes_in_flight, 0);
        assert_eq!(repair_entry.bytes_in_flight, 0);
        assert_eq!(owner_entry.owner_data_in_flight_bytes, 0);
        assert_eq!(repair_entry.owner_data_in_flight_bytes, 0);
        assert_eq!(
            owner_entry.delivery_samples, 1,
            "the later owner-only suffix ACK may become path-scoped evidence"
        );
        assert_eq!(repair_entry.delivery_samples, 0);
        assert_eq!(owner_entry.owner_data_acked_bytes, 3072);
        assert_eq!(repair_entry.owner_data_acked_bytes, 0);
    }

    #[test]
    fn lower_flight_debt_ignores_plain_unacked_owner_data_until_ack_hole() {
        let (binding, owner) = binding_for_underlay(UnderlayProtocol::Tcp);
        binding.record_owner_flight(owner, &stream_data_frame_at(0, 1024));
        binding.record_owner_flight(owner, &stream_data_frame_at(1024, 2048));

        assert!(
            binding.lower_flights_before_offset(3072).is_empty(),
            "ordinary unacked owner flight is recovery state, not authoritative ordering debt"
        );

        binding.release_normalized_acked_ranges(&[OffsetRange {
            start: 1024,
            end: 3072,
        }]);

        let lower = binding.lower_flights_before_offset(3072);
        assert_eq!(lower.len(), 1);
        assert_eq!(lower[0].key, owner);
        assert_eq!(
            lower[0].bytes, 2048,
            "ACK-hole evidence remains ordering debt until the frontier becomes contiguous"
        );
    }

    #[test]
    fn repair_stream_ack_progress_does_not_promote_repair_output() {
        let (binding, owner) = binding_for_underlay(UnderlayProtocol::Udp);
        let repair = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let (repair_commands, _repair_receivers) = reliable_path_command_channels(8);
        binding.attach(
            repair.underlay,
            repair.path_id,
            repair_commands,
            FlowLane::Throughput,
            StreamOpenRole::Repair,
            reliable_relay_buffer_len(MuxLimits::default()),
        );
        let frame = stream_data_frame_at(0, 4096);

        let before_order = binding
            .outputs
            .lock()
            .expect("test response outputs lock")
            .entries
            .iter()
            .map(|entry| entry.key)
            .collect::<Vec<_>>();

        binding.record_repair_flight(repair, &frame);
        binding.release_normalized_acked_ranges(&[OffsetRange {
            start: 0,
            end: 4096,
        }]);

        let outputs = binding.outputs.lock().expect("test response outputs lock");
        let after_order = outputs
            .entries
            .iter()
            .map(|entry| entry.key)
            .collect::<Vec<_>>();
        let owner_entry = outputs
            .entries
            .iter()
            .find(|entry| entry.key == owner)
            .expect("owner output exists");
        let repair_entry = outputs
            .entries
            .iter()
            .find(|entry| entry.key == repair)
            .expect("repair output exists");

        assert_eq!(after_order, before_order);
        assert_eq!(owner_entry.delivery_samples, 0);
        assert_eq!(repair_entry.delivery_samples, 0);
        assert_eq!(repair_entry.bytes_in_flight, 0);
        assert_eq!(binding.ordered_data_owner(), Some(owner));
    }

    #[test]
    fn repair_flight_kind_never_owns_ordering_or_delivery_evidence() {
        let (binding, owner) = binding_for_underlay(UnderlayProtocol::Udp);
        let repair = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let (repair_commands, _repair_receivers) = reliable_path_command_channels(8);
        binding.attach(
            repair.underlay,
            repair.path_id,
            repair_commands,
            FlowLane::Throughput,
            StreamOpenRole::Repair,
            reliable_relay_buffer_len(MuxLimits::default()),
        );

        let owner_frame = stream_data_frame_at(0, 1024);
        let repair_frame = stream_data_frame_at(1024, 1024);
        binding.record_owner_flight(owner, &owner_frame);
        binding.record_repair_flight(repair, &repair_frame);

        let lower = binding.lower_flights_before_offset(2048);
        assert!(
            lower.is_empty(),
            "plain owner flight and repair-only flight must not become admission ordering debt"
        );

        binding.release_normalized_acked_ranges(&[OffsetRange {
            start: 1024,
            end: 2048,
        }]);

        let outputs = binding.outputs.lock().expect("test response outputs lock");
        let repair_entry = outputs
            .entries
            .iter()
            .find(|entry| entry.key == repair)
            .expect("repair output exists");
        assert_eq!(repair_entry.bytes_in_flight, 0);
        assert_eq!(
            repair_entry.delivery_samples, 0,
            "RepairData ACKs release product flight but never become path delivery evidence"
        );
    }

    #[test]
    fn response_subflow_set_allows_repeated_measured_subflow_admission() {
        let (binding, service) = binding_for_underlay(UnderlayProtocol::Udp);
        let optional = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
        let input = SubflowAdmissionInput {
            key: optional,
            bulk_rate_proven: true,
            startup_owner_allowed: false,
            frontier_clear: true,
            completion_improves: true,
            observed_goodput_non_degrading: true,
            read_gap: Duration::ZERO,
            owner_bytes: payload_bytes,
            optional_overhead_bytes: 0,
        };

        let first = binding.preview_subflow_owner_admission(
            service,
            payload_bytes,
            0,
            Duration::ZERO,
            input,
        );
        assert_eq!(first.decision, PathAdmissionDecision::AdmitSubflow);

        let committed = binding.commit_subflow_owner_admission(
            service,
            payload_bytes,
            0,
            Duration::ZERO,
            input,
        );
        assert_eq!(committed.decision, PathAdmissionDecision::AdmitSubflow);

        let second = binding.preview_subflow_owner_admission(
            service,
            payload_bytes,
            0,
            Duration::ZERO,
            input,
        );
        assert_eq!(
            second.decision,
            PathAdmissionDecision::AdmitSubflow,
            "measured subflows are paced by inflight/completion/reorder gates, not by a startup quantum"
        );

        binding.reset_subflow_set();
        let after_reset = binding.preview_subflow_owner_admission(
            service,
            payload_bytes,
            0,
            Duration::ZERO,
            input,
        );
        assert_eq!(after_reset.decision, PathAdmissionDecision::AdmitSubflow);
    }

    #[test]
    fn response_semantic_reset_retires_partial_ack_clock_credit_without_refill() {
        let mux_limits = MuxLimits::default();
        let (binding, _service) = binding_for_underlay(UnderlayProtocol::Tcp);
        let candidate = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let (candidate_commands, _candidate_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                candidate.underlay,
                candidate.path_id,
                candidate_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                reliable_relay_buffer_len(mux_limits),
            ),
            ResponseStreamAttachOutcome::Attached
        );
        let initial_limit = reliable_ack_clock_calibration_limit_bytes(mux_limits);
        let spent_bytes = initial_limit / 2;
        let identity = {
            let mut outputs = binding.outputs.lock().expect("test response outputs lock");
            let entry = outputs
                .entries
                .iter()
                .find(|entry| entry.key == candidate)
                .expect("candidate output");
            let identity = (entry.key, entry.incarnation);
            let mut calibration = ResponseAckClockCalibrationState::new(
                initial_limit,
                reliable_ack_clock_calibration_ceiling_bytes(mux_limits),
            );
            calibration.spent_bytes = spent_bytes;
            outputs.ack_clock_calibrations.insert(identity, calibration);
            outputs.active_ack_clock_calibration = Some(identity);
            identity
        };

        binding.reset_subflow_set();

        let outputs = binding.outputs.lock().expect("test response outputs lock");
        let calibration = outputs
            .ack_clock_calibrations
            .get(&identity)
            .expect("retired calibration tombstone");
        assert_eq!(calibration.spent_bytes, spent_bytes);
        assert_eq!(calibration.credit_limit_bytes, spent_bytes);
        assert_eq!(calibration.max_limit_bytes, spent_bytes);
        assert_eq!(outputs.active_ack_clock_calibration, None);
        drop(outputs);

        let target = binding
            .sender_path_targets(FlowLane::Throughput, 1)
            .into_iter()
            .find(|target| target.key == candidate)
            .expect("retired candidate target");
        assert_eq!(
            target.ack_clock_calibration_spent_bytes, target.ack_clock_calibration_max_limit_bytes,
            "selection sees an exhausted tombstone instead of refilled credit"
        );
    }

    #[test]
    fn response_semantic_reset_keeps_retired_active_identity_until_owner_flight_drains() {
        let mux_limits = MuxLimits::default();
        let (binding, _service) = binding_for_underlay(UnderlayProtocol::Tcp);
        let candidate = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let (candidate_commands, _candidate_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                candidate.underlay,
                candidate.path_id,
                candidate_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                reliable_relay_buffer_len(mux_limits),
            ),
            ResponseStreamAttachOutcome::Attached
        );
        let frame = stream_data_frame_at(0, 4096);
        let identity = {
            let mut outputs = binding.outputs.lock().expect("test response outputs lock");
            let entry = outputs
                .entries
                .iter_mut()
                .find(|entry| entry.key == candidate)
                .expect("candidate output");
            entry.product_progress_rate_bps = Some(1.0);
            entry.delivery_rate_bps = Some(1.0);
            let identity = (entry.key, entry.incarnation);
            let mut calibration = ResponseAckClockCalibrationState::new(
                reliable_ack_clock_calibration_limit_bytes(mux_limits),
                reliable_ack_clock_calibration_ceiling_bytes(mux_limits),
            );
            calibration.spent_bytes = 4096;
            outputs.ack_clock_calibrations.insert(identity, calibration);
            outputs.active_ack_clock_calibration = Some(identity);
            identity
        };
        binding.record_owner_flight(candidate, &frame);

        binding.reset_subflow_set();

        {
            let outputs = binding.outputs.lock().expect("test response outputs lock");
            assert_eq!(outputs.active_ack_clock_calibration, Some(identity));
            let calibration = outputs
                .ack_clock_calibrations
                .get(&identity)
                .expect("retired calibration state");
            assert_eq!(calibration.spent_bytes, calibration.max_limit_bytes);
        }
        let target = binding
            .sender_path_targets(FlowLane::Throughput, 1)
            .into_iter()
            .find(|target| target.key == candidate)
            .expect("retired candidate target");
        assert!(target.ack_clock_calibration_active);

        std::thread::sleep(Duration::from_millis(1));
        binding.release_normalized_acked_ranges(&[OffsetRange {
            start: 0,
            end: 4096,
        }]);
        assert_eq!(
            binding
                .outputs
                .lock()
                .expect("test response outputs lock")
                .active_ack_clock_calibration,
            None
        );
        {
            let outputs = binding.outputs.lock().expect("test response outputs lock");
            let entry = outputs
                .entries
                .iter()
                .find(|entry| entry.key == candidate)
                .expect("candidate output after calibration drain");
            assert_eq!(entry.delivery_rate_bps, Some(1.0));
        }

        let ordinary = stream_data_frame_at(4096, MIN_RATE_SAMPLE_BYTES as usize);
        let later =
            stream_data_frame_at(4096 + MIN_RATE_SAMPLE_BYTES, MIN_RATE_SAMPLE_BYTES as usize);
        binding.record_owner_flight(candidate, &ordinary);
        binding.record_owner_flight(candidate, &later);
        let first_ack = Instant::now() + RESPONSE_ACK_CLOCK_GOODPUT_MIN_ELAPSED;
        binding.release_normalized_acked_ranges_at(
            &[OffsetRange {
                start: 4096,
                end: 4096 + MIN_RATE_SAMPLE_BYTES,
            }],
            first_ack,
        );
        binding.release_normalized_acked_ranges_at(
            &[OffsetRange {
                start: 4096 + MIN_RATE_SAMPLE_BYTES,
                end: 4096 + 2 * MIN_RATE_SAMPLE_BYTES,
            }],
            first_ack + RESPONSE_ACK_CLOCK_GOODPUT_MIN_ELAPSED,
        );
        let outputs = binding.outputs.lock().expect("test response outputs lock");
        let entry = outputs
            .entries
            .iter()
            .find(|entry| entry.key == candidate)
            .expect("candidate output after ordinary ACK");
        assert!(entry.delivery_rate_bps.is_some_and(|rate| rate > 1.0));
    }

    #[test]
    fn response_subflow_set_rejects_unproven_owner_without_bulk_rate() {
        let (binding, service) = binding_for_underlay(UnderlayProtocol::Udp);
        let optional = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
        let input = SubflowAdmissionInput {
            key: optional,
            bulk_rate_proven: false,
            startup_owner_allowed: false,
            frontier_clear: true,
            completion_improves: true,
            observed_goodput_non_degrading: true,
            read_gap: Duration::ZERO,
            owner_bytes: payload_bytes,
            optional_overhead_bytes: 0,
        };

        let committed = binding.commit_subflow_owner_admission(
            service,
            payload_bytes,
            0,
            Duration::ZERO,
            input,
        );
        assert_eq!(
            committed.decision,
            PathAdmissionDecision::ProbeOnly,
            "sender/proof/ACK-data evidence is not enough to enter the owner Subflow set"
        );
        assert!(binding.subflow_set_snapshot().is_none());

        let second = binding.preview_subflow_owner_admission(
            service,
            payload_bytes,
            0,
            Duration::ZERO,
            input,
        );
        assert_eq!(
            second.decision,
            PathAdmissionDecision::ProbeOnly,
            "unproven Subflows remain Probe until they have bulk-rate evidence"
        );

        binding.reset_subflow_set();
        let after_reset = binding.preview_subflow_owner_admission(
            service,
            payload_bytes,
            0,
            Duration::ZERO,
            input,
        );
        assert_eq!(after_reset.decision, PathAdmissionDecision::ProbeOnly);
    }

    #[test]
    fn response_subflow_unproven_probe_state_survives_ack_progress() {
        let (binding, service) = binding_for_underlay(UnderlayProtocol::Udp);
        let optional = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
        let input = SubflowAdmissionInput {
            key: optional,
            bulk_rate_proven: false,
            startup_owner_allowed: false,
            frontier_clear: true,
            completion_improves: true,
            observed_goodput_non_degrading: true,
            read_gap: Duration::ZERO,
            owner_bytes: payload_bytes,
            optional_overhead_bytes: 0,
        };

        assert_eq!(
            binding
                .commit_subflow_owner_admission(service, payload_bytes, 0, Duration::ZERO, input)
                .decision,
            PathAdmissionDecision::ProbeOnly
        );
        assert_eq!(
            binding
                .preview_subflow_owner_admission(service, payload_bytes, 0, Duration::ZERO, input)
                .decision,
            PathAdmissionDecision::ProbeOnly
        );

        let service_frame = stream_data_frame(payload_bytes);
        binding.record_owner_flight(service, &service_frame);
        binding.release_normalized_acked_ranges(&[OffsetRange {
            start: 0,
            end: payload_bytes as u64,
        }]);

        assert_eq!(
            binding
                .preview_subflow_owner_admission(service, payload_bytes, 0, Duration::ZERO, input)
                .decision,
            PathAdmissionDecision::ProbeOnly,
            "ordinary ACK progress must not convert an unproven path into a Subflow owner"
        );
    }

    #[test]
    fn response_subflow_epoch_survives_passive_growth_but_resets_on_detach() {
        let (binding, service) = binding_for_underlay(UnderlayProtocol::Udp);
        let optional = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let (commands, _receivers) = reliable_path_command_channels(8);
        binding.attach(
            optional.underlay,
            optional.path_id,
            commands.clone(),
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            reliable_relay_buffer_len(MuxLimits::default()),
        );

        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
        let input = SubflowAdmissionInput {
            key: optional,
            bulk_rate_proven: true,
            startup_owner_allowed: false,
            frontier_clear: true,
            completion_improves: true,
            observed_goodput_non_degrading: true,
            read_gap: Duration::ZERO,
            owner_bytes: payload_bytes,
            optional_overhead_bytes: 0,
        };

        assert_eq!(
            binding
                .commit_subflow_owner_admission(service, payload_bytes, 0, Duration::ZERO, input)
                .decision,
            PathAdmissionDecision::AdmitSubflow
        );
        assert!(binding.subflow_set_snapshot().is_some());

        let (stale_generation, _) = binding.subflow_state_snapshot();
        let stale_lane_generation = binding.lane_generation();
        let added = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(2),
        };
        let (added_commands, _added_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                added.underlay,
                added.path_id,
                added_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                reliable_relay_buffer_len(MuxLimits::default()),
            ),
            ResponseStreamAttachOutcome::Attached
        );
        assert!(
            binding.subflow_set_snapshot().is_some(),
            "passive output growth must preserve the current Subflow epoch"
        );
        assert_eq!(
            binding
                .commit_subflow_owner_admission_for_planner_generation(
                    stale_generation,
                    stale_lane_generation,
                    service,
                    payload_bytes,
                    0,
                    Duration::ZERO,
                    input,
                )
                .decision,
            PathAdmissionDecision::Standby,
            "a plan made before passive membership changed must not commit afterward"
        );
        assert_eq!(
            binding
                .commit_subflow_owner_admission(service, payload_bytes, 0, Duration::ZERO, input,)
                .decision,
            PathAdmissionDecision::AdmitSubflow
        );
        assert!(binding.subflow_set_snapshot().is_some());

        binding.detach(optional, &commands);

        assert!(
            binding.subflow_set_snapshot().is_none(),
            "carrier output detach resets the Subflow set"
        );
    }

    #[test]
    fn passive_cross_family_attach_does_not_refill_or_transfer_startup_epoch() {
        let (binding, service) = binding_for_underlay(UnderlayProtocol::Tcp);
        let candidate = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let (candidate_commands, _candidate_receivers) = reliable_path_command_channels(8);
        binding.attach(
            candidate.underlay,
            candidate.path_id,
            candidate_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            reliable_relay_buffer_len(MuxLimits::default()),
        );

        let quantum = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
        let startup_credit = quantum * 4;
        let input = SubflowAdmissionInput {
            key: candidate,
            bulk_rate_proven: false,
            startup_owner_allowed: true,
            frontier_clear: true,
            completion_improves: false,
            observed_goodput_non_degrading: true,
            read_gap: Duration::ZERO,
            owner_bytes: quantum,
            optional_overhead_bytes: 0,
        };
        for _ in 0..4 {
            assert_eq!(
                binding
                    .commit_subflow_owner_admission(
                        service,
                        startup_credit,
                        0,
                        Duration::ZERO,
                        input,
                    )
                    .decision,
                PathAdmissionDecision::AdmitSubflow
            );
        }
        assert_eq!(
            binding
                .preview_subflow_owner_admission(
                    service,
                    startup_credit,
                    0,
                    Duration::ZERO,
                    SubflowAdmissionInput {
                        owner_bytes: 1,
                        ..input
                    },
                )
                .decision,
            PathAdmissionDecision::ProbeOnly,
            "the initial candidate has spent the cumulative startup cap"
        );

        let (stale_generation, _) = binding.subflow_state_snapshot();
        let stale_lane_generation = binding.lane_generation();
        let added = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        };
        let (added_commands, _added_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                added.underlay,
                added.path_id,
                added_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                reliable_relay_buffer_len(MuxLimits::default()),
            ),
            ResponseStreamAttachOutcome::Attached
        );

        let (current_generation, epoch) = binding.subflow_state_snapshot();
        assert_ne!(current_generation, stale_generation);
        let epoch = epoch.expect("passive attachment preserves startup epoch");
        assert_eq!(epoch.members().len(), 1);
        assert_eq!(epoch.members()[0].key, candidate);
        assert_eq!(epoch.members()[0].owner_sent_bytes, startup_credit as u64);
        assert_eq!(
            binding
                .commit_subflow_owner_admission_for_planner_generation(
                    stale_generation,
                    stale_lane_generation,
                    service,
                    startup_credit,
                    0,
                    Duration::ZERO,
                    input,
                )
                .decision,
            PathAdmissionDecision::Standby,
            "a plan made before passive growth must not commit afterward"
        );
        assert_eq!(
            binding
                .commit_subflow_owner_admission(
                    service,
                    startup_credit,
                    0,
                    Duration::ZERO,
                    SubflowAdmissionInput {
                        owner_bytes: 1,
                        ..input
                    },
                )
                .decision,
            PathAdmissionDecision::ProbeOnly,
            "passive growth must not refill the selected candidate's startup credit"
        );
        assert_eq!(
            binding
                .commit_subflow_owner_admission(
                    service,
                    startup_credit,
                    0,
                    Duration::ZERO,
                    SubflowAdmissionInput {
                        key: added,
                        owner_bytes: 1,
                        ..input
                    },
                )
                .decision,
            PathAdmissionDecision::ProbeOnly,
            "passive growth must not transfer startup ownership to the new output"
        );
    }

    #[test]
    fn passive_attach_after_reservation_preserves_unemitted_credit_rollback() {
        for passive_role in [StreamOpenRole::Validation, StreamOpenRole::Repair] {
            let (binding, service) = binding_for_underlay(UnderlayProtocol::Tcp);
            let candidate = CarrierPathKey {
                underlay: UnderlayProtocol::Tcp,
                path_id: PathId(1),
            };
            let (candidate_commands, _candidate_receivers) = reliable_path_command_channels(8);
            binding.attach(
                candidate.underlay,
                candidate.path_id,
                candidate_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                reliable_relay_buffer_len(MuxLimits::default()),
            );
            let quantum = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
            let optional_bytes = 1024;
            let input = SubflowAdmissionInput {
                key: candidate,
                bulk_rate_proven: false,
                startup_owner_allowed: true,
                frontier_clear: true,
                completion_improves: false,
                observed_goodput_non_degrading: true,
                read_gap: Duration::ZERO,
                owner_bytes: quantum,
                optional_overhead_bytes: optional_bytes,
            };
            let (planner_generation, _) = binding.subflow_state_snapshot();
            let reservation = binding.reserve_subflow_owner_admission_for_planner_generation(
                planner_generation,
                binding.lane_generation(),
                service,
                quantum,
                optional_bytes,
                Duration::ZERO,
                input,
            );
            assert_eq!(
                reservation.admission.decision,
                PathAdmissionDecision::AdmitSubflow
            );
            let epoch_generation = reservation
                .epoch_generation
                .expect("admitted Subflow reservation has an epoch token");

            let passive = CarrierPathKey {
                underlay: UnderlayProtocol::Udp,
                path_id: PathId(0),
            };
            let (passive_commands, _passive_receivers) = reliable_path_command_channels(8);
            assert_eq!(
                binding.attach(
                    passive.underlay,
                    passive.path_id,
                    passive_commands,
                    FlowLane::Throughput,
                    passive_role,
                    reliable_relay_buffer_len(MuxLimits::default()),
                ),
                ResponseStreamAttachOutcome::Attached
            );
            binding.rollback_subflow_owner_admission_for_epoch(epoch_generation, input);

            assert_eq!(
                binding
                    .commit_subflow_owner_admission(
                        service,
                        quantum,
                        optional_bytes,
                        Duration::ZERO,
                        input,
                    )
                    .decision,
                PathAdmissionDecision::AdmitSubflow,
                "{passive_role:?} planner invalidation must not block refund of unemitted bytes"
            );
        }
    }

    #[test]
    fn tcp_calibration_commit_fences_generations_and_rolls_back_blocked_enqueue() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = PATH_OPEN_SCORE_BYTES;
        let session_id = SessionId(190);
        let tracker = Arc::new(ServerPathLaneTracker::default());
        let service = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let candidate = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let (service_commands, mut service_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new_with_limits_and_tracker(
            session_id,
            service.underlay,
            service.path_id,
            service_commands,
            FlowLane::Throughput,
            mux_limits,
            tracker.clone(),
        );
        let (second_commands, _second_receivers) = reliable_path_command_channels(8);
        let mut second_flow = Some(ResponseStreamBinding::new_with_limits_and_tracker(
            session_id,
            UnderlayProtocol::Tcp,
            PathId(9),
            second_commands,
            FlowLane::Throughput,
            mux_limits,
            tracker.clone(),
        ));
        assert_eq!(binding.lane_generation_and_active_response_flows().1, 2);

        let (candidate_commands, mut candidate_receivers) = reliable_path_command_channels(1);
        assert_eq!(
            binding.attach(
                candidate.underlay,
                candidate.path_id,
                candidate_commands.clone(),
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                payload_bytes,
            ),
            ResponseStreamAttachOutcome::Attached
        );
        assert!(matches!(
            try_recv_reliable_path_command(&mut candidate_receivers),
            Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
        ));

        let (service_incarnation, candidate_incarnation) = {
            let mut outputs = binding.outputs.lock().expect("test response outputs lock");
            for entry in &mut outputs.entries {
                if entry.key == service || entry.key == candidate {
                    mark_test_response_output_bulk_proven(entry, mux_limits);
                }
            }
            let service_incarnation = outputs
                .entries
                .iter()
                .find(|entry| entry.key == service)
                .expect("service output")
                .incarnation;
            let candidate_incarnation = outputs
                .entries
                .iter()
                .find(|entry| entry.key == candidate)
                .expect("candidate output")
                .incarnation;
            outputs.ack_clock_calibrations.insert(
                (candidate, candidate_incarnation),
                ResponseAckClockCalibrationState::new(
                    reliable_ack_clock_calibration_limit_bytes(mux_limits),
                    reliable_ack_clock_calibration_ceiling_bytes(mux_limits),
                ),
            );
            (service_incarnation, candidate_incarnation)
        };
        assert_eq!(
            binding
                .commit_subflow_owner_admission(
                    service,
                    payload_bytes,
                    0,
                    Duration::ZERO,
                    SubflowAdmissionInput {
                        key: candidate,
                        bulk_rate_proven: true,
                        startup_owner_allowed: false,
                        frontier_clear: true,
                        completion_improves: true,
                        observed_goodput_non_degrading: true,
                        read_gap: Duration::ZERO,
                        owner_bytes: payload_bytes,
                        optional_overhead_bytes: 0,
                    },
                )
                .decision,
            PathAdmissionDecision::AdmitSubflow
        );
        let target = binding
            .sender_path_targets(FlowLane::Throughput, payload_bytes)
            .into_iter()
            .find(|target| target.key == candidate)
            .expect("candidate target");
        let service_target = binding
            .sender_path_targets(FlowLane::Throughput, payload_bytes)
            .into_iter()
            .find(|target| target.key == service)
            .expect("service target");
        let request_for = |binding: &ResponseStreamBinding| {
            let (expected_planner_generation, _) = binding.subflow_state_snapshot();
            ResponseAckClockCalibrationRequest {
                expected_planner_generation,
                expected_lane_generation: binding.lane_generation(),
                expected_model_generation: binding.response_model_generation(),
                service,
                service_incarnation,
                service_pending_bytes: 0,
                target_pending_bytes: target.commands.pending_bytes(),
                limit_bytes: reliable_ack_clock_calibration_limit_bytes(mux_limits),
                requires_multi_flow_start: true,
            }
        };
        let frame = stream_data_frame(payload_bytes);

        let stale_model = request_for(&binding);
        binding.set_output_product_model_for_test(candidate, 500_000_000.0, 10.0);
        assert!(matches!(
            binding.try_enqueue_owner_frame_for_target(
                &target,
                &frame,
                FlowLane::Throughput,
                None,
                Some(stale_model),
            ),
            Err(RuntimeError::SenderServiceBlocked)
        ));

        let stale = request_for(&binding);
        binding.invalidate_subflow_plan();
        assert!(matches!(
            binding.try_enqueue_owner_frame_for_target(
                &target,
                &frame,
                FlowLane::Throughput,
                None,
                Some(stale),
            ),
            Err(RuntimeError::SenderServiceBlocked)
        ));

        let stale_lane = request_for(&binding);
        drop(second_flow.take());
        assert_eq!(binding.lane_generation_and_active_response_flows().1, 1);
        assert!(matches!(
            binding.try_enqueue_owner_frame_for_target(
                &target,
                &frame,
                FlowLane::Throughput,
                None,
                Some(stale_lane),
            ),
            Err(RuntimeError::SenderServiceBlocked)
        ));
        let (replacement_commands, _replacement_receivers) = reliable_path_command_channels(8);
        second_flow = Some(ResponseStreamBinding::new_with_limits_and_tracker(
            session_id,
            UnderlayProtocol::Tcp,
            PathId(9),
            replacement_commands,
            FlowLane::Throughput,
            mux_limits,
            tracker,
        ));
        assert_eq!(binding.lane_generation_and_active_response_flows().1, 2);

        let stale_stage = request_for(&binding);
        {
            let mut outputs = binding.outputs.lock().expect("test response outputs lock");
            let calibration = outputs
                .ack_clock_calibrations
                .get_mut(&(candidate, candidate_incarnation))
                .expect("candidate calibration state");
            calibration.spent_bytes = calibration.credit_limit_bytes;
            let stage_authorized_at = calibration.stage_authorized_at;
            let sample = test_ack_clock_rate_sample(
                calibration.stage_rate_coverage_floor_bytes,
                10_000_000.0,
            );
            assert!(calibration.record_ack_clock_sample(
                sample,
                stage_authorized_at,
                stage_authorized_at + Duration::from_millis(1),
            ));
        }
        assert!(matches!(
            binding.try_enqueue_owner_frame_for_target(
                &target,
                &frame,
                FlowLane::Throughput,
                None,
                Some(stale_stage),
            ),
            Err(RuntimeError::SenderServiceBlocked)
        ));
        {
            let mut outputs = binding.outputs.lock().expect("test response outputs lock");
            outputs.ack_clock_calibrations.insert(
                (candidate, candidate_incarnation),
                ResponseAckClockCalibrationState::new(
                    reliable_ack_clock_calibration_limit_bytes(mux_limits),
                    reliable_ack_clock_calibration_ceiling_bytes(mux_limits),
                ),
            );
        }

        let stale_target_pending = request_for(&binding);
        candidate_commands
            .try_enqueue_stream_ordered_frame(
                stream_data_frame_at(payload_bytes as u64, payload_bytes),
                FlowLane::Throughput,
            )
            .expect("change candidate pending bytes");
        let candidate_pending_command = try_recv_reliable_path_command(&mut candidate_receivers)
            .expect("drain candidate queue without releasing pending bytes");
        let candidate_pending_bytes =
            reliable_path_command_pending_bytes(&candidate_pending_command);
        assert!(matches!(
            binding.try_enqueue_owner_frame_for_target(
                &target,
                &frame,
                FlowLane::Throughput,
                None,
                Some(stale_target_pending),
            ),
            Err(RuntimeError::SenderServiceBlocked)
        ));
        candidate_receivers.release_pending_command_bytes(candidate_pending_bytes);

        let stale_service_pending = request_for(&binding);
        service_target
            .commands
            .try_enqueue_stream_ordered_frame(
                stream_data_frame_at(payload_bytes as u64, payload_bytes),
                FlowLane::Throughput,
            )
            .expect("change service pending bytes");
        let service_pending_command = try_recv_reliable_path_command(&mut service_receivers)
            .expect("drain service queue without releasing pending bytes");
        let service_pending_bytes = reliable_path_command_pending_bytes(&service_pending_command);
        assert!(matches!(
            binding.try_enqueue_owner_frame_for_target(
                &target,
                &frame,
                FlowLane::Throughput,
                None,
                Some(stale_service_pending),
            ),
            Err(RuntimeError::SenderServiceBlocked)
        ));
        service_receivers.release_pending_command_bytes(service_pending_bytes);

        candidate_commands
            .try_enqueue_stream_ordered_frame(
                stream_data_frame_at(payload_bytes as u64, payload_bytes),
                FlowLane::Throughput,
            )
            .expect("fill candidate queue");
        let fresh = request_for(&binding);
        assert!(matches!(
            binding.try_enqueue_owner_frame_for_target(
                &target,
                &frame,
                FlowLane::Throughput,
                None,
                Some(fresh),
            ),
            Err(RuntimeError::SenderServiceBlocked)
        ));
        {
            let outputs = binding.outputs.lock().expect("test response outputs lock");
            assert_eq!(
                outputs
                    .ack_clock_calibrations
                    .get(&(candidate, candidate_incarnation))
                    .expect("candidate calibration state")
                    .spent_bytes,
                0,
                "blocked enqueue restores cumulative calibration credit"
            );
            assert_eq!(outputs.active_ack_clock_calibration, None);
        }
        assert!(try_recv_reliable_path_command(&mut candidate_receivers).is_some());

        binding
            .try_enqueue_owner_frame_for_target(
                &target,
                &frame,
                FlowLane::Throughput,
                None,
                Some(request_for(&binding)),
            )
            .expect("fresh exact calibration reservation enqueues");
        {
            let outputs = binding.outputs.lock().expect("test response outputs lock");
            assert_eq!(
                outputs
                    .ack_clock_calibrations
                    .get(&(candidate, candidate_incarnation))
                    .expect("candidate calibration state")
                    .spent_bytes,
                payload_bytes as u64
            );
            assert_eq!(
                outputs.active_ack_clock_calibration,
                Some((candidate, candidate_incarnation))
            );
        }

        binding.detach(candidate, &candidate_commands);
        assert!(matches!(
            binding.try_enqueue_owner_frame_for_target(
                &target,
                &frame,
                FlowLane::Throughput,
                None,
                Some(request_for(&binding)),
            ),
            Err(RuntimeError::SenderServiceBlocked)
        ));
        assert!(
            binding
                .outputs
                .lock()
                .expect("test response outputs lock")
                .ack_clock_calibrations
                .get(&(candidate, candidate_incarnation))
                .is_none(),
            "detach removes exact-incarnation calibration state"
        );
        drop(second_flow);
    }

    #[test]
    fn subflow_reservation_and_enqueue_linearize_before_topology_reset() {
        let mux_limits = MuxLimits::default();
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let service = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let candidate = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let unrelated = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(2),
        };
        let (service_commands, _service_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new_with_limits(
            SessionId(91),
            service.underlay,
            service.path_id,
            service_commands,
            FlowLane::Throughput,
            mux_limits,
        );
        let (candidate_commands, mut candidate_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                candidate.underlay,
                candidate.path_id,
                candidate_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                payload_bytes,
            ),
            ResponseStreamAttachOutcome::Attached
        );
        assert!(matches!(
            try_recv_reliable_path_command(&mut candidate_receivers),
            Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
        ));
        let (unrelated_commands, mut unrelated_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                unrelated.underlay,
                unrelated.path_id,
                unrelated_commands.clone(),
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                payload_bytes,
            ),
            ResponseStreamAttachOutcome::Attached
        );
        assert!(matches!(
            try_recv_reliable_path_command(&mut unrelated_receivers),
            Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
        ));

        let target = binding
            .sender_path_targets(FlowLane::Throughput, payload_bytes)
            .into_iter()
            .find(|target| target.key == candidate)
            .expect("candidate output is attached");
        let (planner_generation, _) = binding.subflow_state_snapshot();
        let request = ResponseSubflowAdmissionRequest {
            expected_planner_generation: planner_generation,
            expected_lane_generation: binding.lane_generation(),
            service,
            startup_owner_credit_bytes: payload_bytes,
            optional_overhead_budget_bytes: 0,
            max_read_gap_budget: Duration::ZERO,
            input: SubflowAdmissionInput {
                key: candidate,
                bulk_rate_proven: false,
                startup_owner_allowed: true,
                frontier_clear: true,
                completion_improves: false,
                observed_goodput_non_degrading: true,
                read_gap: Duration::ZERO,
                owner_bytes: payload_bytes,
                optional_overhead_bytes: 0,
            },
        };
        let frame = stream_data_frame(payload_bytes);
        let frame_for_sender = frame.clone();
        let binding_for_sender = binding.clone();
        let (reserved_tx, reserved_rx) = std_mpsc::sync_channel(0);
        let (resume_tx, resume_rx) = std_mpsc::sync_channel(0);
        let sender = std::thread::spawn(move || {
            binding_for_sender.try_enqueue_owner_frame_for_target_inner(
                &ResponseDispatchTarget::from(&target),
                &frame_for_sender,
                FlowLane::Throughput,
                Some(request),
                None,
                || {
                    reserved_tx
                        .send(())
                        .expect("reservation observer remains live");
                    resume_rx.recv().expect("reservation test resumes enqueue");
                },
            )
        });
        reserved_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("Subflow reservation reaches the pre-enqueue barrier");

        let outputs_locked_across_reservation = matches!(
            binding.outputs.try_lock(),
            Err(std::sync::TryLockError::WouldBlock)
        );
        let binding_for_detach = binding.clone();
        let (detach_started_tx, detach_started_rx) = std_mpsc::sync_channel(0);
        let (detach_done_tx, detach_done_rx) = std_mpsc::channel();
        let detacher = std::thread::spawn(move || {
            detach_started_tx
                .send(())
                .expect("detach observer remains live");
            binding_for_detach.detach(unrelated, &unrelated_commands);
            detach_done_tx
                .send(())
                .expect("detach completion observer remains live");
        });
        detach_started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("detach attempt starts while enqueue is paused");
        let generation_while_paused = binding.subflow_state_snapshot().0;
        let detach_completed_while_paused = detach_done_rx
            .recv_timeout(Duration::from_millis(50))
            .is_ok();

        resume_tx
            .send(())
            .expect("paused reservation remains ready to enqueue");
        let reservation_epoch = sender
            .join()
            .expect("sender thread does not panic")
            .expect("generation-fenced reservation enqueues");
        detacher.join().expect("detach thread does not panic");

        assert!(
            outputs_locked_across_reservation,
            "outputs must remain locked from Subflow reservation through owner enqueue"
        );
        assert_eq!(generation_while_paused, planner_generation);
        assert!(
            !detach_completed_while_paused,
            "topology reset must not linearize between reservation and enqueue"
        );
        assert!(reservation_epoch.is_some());
        assert_ne!(binding.subflow_state_snapshot().0, planner_generation);
        assert!(matches!(
            try_recv_reliable_path_command(&mut candidate_receivers),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
        ));
        assert_eq!(
            binding.owner_flight_keys_overlapping_frame(&frame),
            vec![candidate],
            "owner flight must be recorded before the topology reset"
        );
    }

    #[test]
    fn full_reset_rejects_stale_epoch_rollback() {
        let (binding, service) = binding_for_underlay(UnderlayProtocol::Tcp);
        let candidate = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let quantum = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
        let input = SubflowAdmissionInput {
            key: candidate,
            bulk_rate_proven: false,
            startup_owner_allowed: true,
            frontier_clear: true,
            completion_improves: false,
            observed_goodput_non_degrading: true,
            read_gap: Duration::ZERO,
            owner_bytes: quantum,
            optional_overhead_bytes: 0,
        };
        let (planner_generation, _) = binding.subflow_state_snapshot();
        let reservation = binding.reserve_subflow_owner_admission_for_planner_generation(
            planner_generation,
            binding.lane_generation(),
            service,
            quantum,
            0,
            Duration::ZERO,
            input,
        );
        let stale_epoch_generation = reservation
            .epoch_generation
            .expect("initial reservation has an epoch token");

        binding.reset_subflow_set();
        assert_eq!(
            binding
                .commit_subflow_owner_admission(service, quantum, 0, Duration::ZERO, input,)
                .decision,
            PathAdmissionDecision::AdmitSubflow
        );
        binding.rollback_subflow_owner_admission_for_epoch(stale_epoch_generation, input);

        assert_eq!(
            binding
                .preview_subflow_owner_admission(
                    service,
                    quantum,
                    0,
                    Duration::ZERO,
                    SubflowAdmissionInput {
                        owner_bytes: 1,
                        ..input
                    },
                )
                .decision,
            PathAdmissionDecision::ProbeOnly,
            "a stale refund must not debit a replacement epoch"
        );
    }

    #[test]
    fn every_envelope_change_replaces_epoch_and_invalidates_competing_plans() {
        let base_service = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let changed_service = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(4),
        };
        let candidate = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let quantum = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
        let base_credit = quantum * 2;
        let base_overhead = 1024;
        let base_gap = Duration::from_millis(10);
        let variants = [
            (changed_service, base_credit, base_overhead, base_gap),
            (base_service, quantum, base_overhead, base_gap),
            (base_service, base_credit, base_overhead * 2, base_gap),
            (
                base_service,
                base_credit,
                base_overhead,
                Duration::from_millis(20),
            ),
        ];

        for (service, credit, overhead, max_gap) in variants {
            let (binding, _) = binding_for_underlay(UnderlayProtocol::Tcp);
            let input = SubflowAdmissionInput {
                key: candidate,
                bulk_rate_proven: false,
                startup_owner_allowed: true,
                frontier_clear: true,
                completion_improves: false,
                observed_goodput_non_degrading: true,
                read_gap: Duration::ZERO,
                owner_bytes: quantum,
                optional_overhead_bytes: 0,
            };
            let (initial_planner_generation, _) = binding.subflow_state_snapshot();
            let initial = binding.reserve_subflow_owner_admission_for_planner_generation(
                initial_planner_generation,
                binding.lane_generation(),
                base_service,
                base_credit,
                base_overhead,
                base_gap,
                input,
            );
            let stale_epoch_generation = initial
                .epoch_generation
                .expect("base envelope reservation has an epoch token");
            let (stale_planner_generation, _) = binding.subflow_state_snapshot();

            let replacement = binding.reserve_subflow_owner_admission_for_planner_generation(
                stale_planner_generation,
                binding.lane_generation(),
                service,
                credit,
                overhead,
                max_gap,
                input,
            );
            assert_eq!(
                replacement.admission.decision,
                PathAdmissionDecision::AdmitSubflow
            );
            assert_ne!(
                replacement.epoch_generation,
                Some(stale_epoch_generation),
                "each envelope field owns a new epoch identity"
            );
            let (current_planner_generation, _) = binding.subflow_state_snapshot();
            assert_ne!(current_planner_generation, stale_planner_generation);
            assert_eq!(
                binding
                    .commit_subflow_owner_admission_for_planner_generation(
                        stale_planner_generation,
                        binding.lane_generation(),
                        service,
                        credit,
                        overhead,
                        max_gap,
                        input,
                    )
                    .decision,
                PathAdmissionDecision::Standby,
                "a competing plan for the replaced envelope must be stale"
            );

            binding.rollback_subflow_owner_admission_for_epoch(stale_epoch_generation, input);
            let epoch = binding
                .subflow_set_snapshot()
                .expect("replacement epoch remains present");
            assert_eq!(epoch.members().len(), 1);
            assert_eq!(epoch.members()[0].owner_sent_bytes, quantum as u64);
        }
    }

    #[test]
    fn stale_subflow_commit_is_rejected_after_reset_or_realtime_pressure() {
        let session_id = SessionId(91);
        let service = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        };
        let candidate = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let (commands, _receivers) = reliable_path_command_channels(8);
        let lane_tracker = Arc::new(ServerPathLaneTracker::default());
        let binding = ResponseStreamBinding::new_with_limits_and_tracker(
            session_id,
            service.underlay,
            service.path_id,
            commands,
            FlowLane::Throughput,
            MuxLimits::default(),
            lane_tracker.clone(),
        );
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
        let input = SubflowAdmissionInput {
            key: candidate,
            bulk_rate_proven: false,
            startup_owner_allowed: true,
            frontier_clear: true,
            completion_improves: false,
            observed_goodput_non_degrading: true,
            read_gap: Duration::ZERO,
            owner_bytes: payload_bytes,
            optional_overhead_bytes: 0,
        };

        let (stale_generation, _) = binding.subflow_state_snapshot();
        let stale_lane_generation = binding.lane_generation();
        binding.reset_subflow_set();
        assert_eq!(
            binding
                .commit_subflow_owner_admission_for_planner_generation(
                    stale_generation,
                    stale_lane_generation,
                    service,
                    payload_bytes * 4,
                    0,
                    Duration::ZERO,
                    input,
                )
                .decision,
            PathAdmissionDecision::Standby,
            "a reset must invalidate an already-planned startup commit"
        );

        let (current_generation, _) = binding.subflow_state_snapshot();
        let pre_pressure_lane_generation = binding.lane_generation();
        let realtime = ServerRealtimeFlowRegistration::new(lane_tracker.clone(), session_id);
        assert_eq!(
            lane_tracker
                .session_snapshot(session_id)
                .active_latency_sensitive_flows,
            1
        );
        assert_eq!(
            binding
                .commit_subflow_owner_admission_for_planner_generation(
                    current_generation,
                    pre_pressure_lane_generation,
                    service,
                    payload_bytes * 4,
                    0,
                    Duration::ZERO,
                    input,
                )
                .decision,
            PathAdmissionDecision::Standby,
            "new realtime pressure must invalidate an already-planned startup commit"
        );
        drop(realtime);
        assert_eq!(
            lane_tracker
                .session_snapshot(session_id)
                .active_latency_sensitive_flows,
            0
        );
    }

    #[test]
    fn startup_commit_rechecks_multi_flow_lane_generation() {
        let session_id = SessionId(92);
        let service = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        };
        let candidate = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let lane_tracker = Arc::new(ServerPathLaneTracker::default());
        let (commands, _receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new_with_limits_and_tracker(
            session_id,
            service.underlay,
            service.path_id,
            commands,
            FlowLane::Throughput,
            MuxLimits::default(),
            lane_tracker.clone(),
        );
        let (second_commands, _second_receivers) = reliable_path_command_channels(8);
        let second_flow = ResponseStreamBinding::new_with_limits_and_tracker(
            session_id,
            UnderlayProtocol::Udp,
            PathId(2),
            second_commands,
            FlowLane::Throughput,
            MuxLimits::default(),
            lane_tracker.clone(),
        );
        let (planner_generation, _) = binding.subflow_state_snapshot();
        let (multi_flow_generation, active_response_flows) =
            binding.lane_generation_and_active_response_flows();
        assert_eq!(active_response_flows, 2);

        drop(second_flow);
        assert_eq!(lane_tracker.session_snapshot(session_id).active_flows, 1);
        assert_eq!(binding.lane_generation_and_active_response_flows().1, 1);
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
        let admission = binding.commit_subflow_owner_admission_for_planner_generation(
            planner_generation,
            multi_flow_generation,
            service,
            payload_bytes * 4,
            0,
            Duration::ZERO,
            SubflowAdmissionInput {
                key: candidate,
                bulk_rate_proven: false,
                startup_owner_allowed: true,
                frontier_clear: true,
                completion_improves: false,
                observed_goodput_non_degrading: true,
                read_gap: Duration::ZERO,
                owner_bytes: payload_bytes,
                optional_overhead_bytes: 0,
            },
        );
        assert_eq!(
            admission.decision,
            PathAdmissionDecision::Standby,
            "closing the second active flow must invalidate a planned startup sample before commit"
        );
    }

    #[test]
    fn unrelated_session_churn_does_not_invalidate_subflow_commit() {
        let session_id = SessionId(93);
        let other_session_id = SessionId(94);
        let service = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let candidate = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let (commands, _receivers) = reliable_path_command_channels(8);
        let lane_tracker = Arc::new(ServerPathLaneTracker::default());
        let binding = ResponseStreamBinding::new_with_limits_and_tracker(
            session_id,
            service.underlay,
            service.path_id,
            commands,
            FlowLane::Throughput,
            MuxLimits::default(),
            lane_tracker.clone(),
        );
        let (planner_generation, _) = binding.subflow_state_snapshot();
        let lane_generation = binding.lane_generation();

        let realtime = ServerRealtimeFlowRegistration::new(lane_tracker.clone(), other_session_id);
        let other_path = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(9),
        };
        lane_tracker.attach(other_session_id, other_path, FlowLane::Latency);
        lane_tracker.detach(other_session_id, other_path, FlowLane::Latency);

        assert_eq!(binding.lane_generation(), lane_generation);
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
        assert_eq!(
            binding
                .commit_subflow_owner_admission_for_planner_generation(
                    planner_generation,
                    lane_generation,
                    service,
                    payload_bytes * 4,
                    0,
                    Duration::ZERO,
                    SubflowAdmissionInput {
                        key: candidate,
                        bulk_rate_proven: false,
                        startup_owner_allowed: true,
                        frontier_clear: true,
                        completion_improves: false,
                        observed_goodput_non_degrading: true,
                        read_gap: Duration::ZERO,
                        owner_bytes: payload_bytes,
                        optional_overhead_bytes: 0,
                    },
                )
                .decision,
            PathAdmissionDecision::AdmitSubflow,
            "lane and realtime churn in another session must not reject this session's commit"
        );
        drop(realtime);
        assert_eq!(binding.lane_generation(), lane_generation);
    }

    #[test]
    fn lane_tracker_reclaims_session_state_when_last_binding_drops() {
        let session_id = SessionId(95);
        let lane_tracker = Arc::new(ServerPathLaneTracker::default());
        let (commands, _receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new_with_limits_and_tracker(
            session_id,
            UnderlayProtocol::Tcp,
            PathId(0),
            commands,
            FlowLane::Throughput,
            MuxLimits::default(),
            lane_tracker.clone(),
        );

        {
            let state = lane_tracker
                .state
                .lock()
                .expect("server path lane tracker lock");
            assert_eq!(state.session_references.get(&session_id), Some(&1));
            assert_eq!(state.active_response_flows.get(&session_id), Some(&1));
            assert!(state.session_generations.contains_key(&session_id));
            assert!(state.loads.keys().any(|key| key.session_id == session_id));
        }

        drop(binding);

        let state = lane_tracker
            .state
            .lock()
            .expect("server path lane tracker lock");
        assert!(!state.session_references.contains_key(&session_id));
        assert!(!state.session_generations.contains_key(&session_id));
        assert!(!state.realtime_flows.contains_key(&session_id));
        assert!(!state.active_response_flows.contains_key(&session_id));
        assert!(!state.loads.keys().any(|key| key.session_id == session_id));
    }

    #[test]
    fn active_response_flow_count_is_per_binding_not_per_attachment() {
        let session_id = SessionId(99);
        let service = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        };
        let alternate = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let lane_tracker = Arc::new(ServerPathLaneTracker::default());
        let (service_commands, _service_receivers) = reliable_path_command_channels(8);
        let service_commands_for_detach = service_commands.clone();
        let binding = ResponseStreamBinding::new_with_limits_and_tracker(
            session_id,
            service.underlay,
            service.path_id,
            service_commands,
            FlowLane::Throughput,
            MuxLimits::default(),
            lane_tracker.clone(),
        );
        let (alternate_commands, _alternate_receivers) = reliable_path_command_channels(8);
        let alternate_commands_for_detach = alternate_commands.clone();
        assert_eq!(
            binding.attach(
                alternate.underlay,
                alternate.path_id,
                alternate_commands,
                FlowLane::Throughput,
                StreamOpenRole::Active,
                reliable_relay_buffer_len(MuxLimits::default()),
            ),
            ResponseStreamAttachOutcome::Attached
        );
        assert_eq!(lane_tracker.session_snapshot(session_id).active_flows, 2);
        assert_eq!(
            binding.lane_generation_and_active_response_flows().1,
            1,
            "one response stream must contribute one flow despite two Active attachments"
        );

        binding.detach(service, &service_commands_for_detach);
        assert_eq!(lane_tracker.session_snapshot(session_id).active_flows, 1);
        assert_eq!(binding.lane_generation_and_active_response_flows().1, 1);
        binding.detach(alternate, &alternate_commands_for_detach);
        assert_eq!(lane_tracker.session_snapshot(session_id).active_flows, 0);
        assert_eq!(
            binding.lane_generation_and_active_response_flows().1,
            0,
            "a response stream with no Active attachment must not satisfy the gate"
        );
    }

    #[test]
    fn passive_attachments_do_not_consume_or_release_shared_flow_load() {
        let session_id = SessionId(97);
        let service_key = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        };
        let shared_key = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let lane_tracker = Arc::new(ServerPathLaneTracker::default());
        let (service_commands, _service_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new_with_limits_and_tracker(
            session_id,
            service_key.underlay,
            service_key.path_id,
            service_commands,
            FlowLane::Throughput,
            MuxLimits::default(),
            lane_tracker.clone(),
        );
        let (shared_commands, _shared_receivers) = reliable_path_command_channels(8);
        let shared_binding = ResponseStreamBinding::new_with_limits_and_tracker(
            session_id,
            shared_key.underlay,
            shared_key.path_id,
            shared_commands,
            FlowLane::Throughput,
            MuxLimits::default(),
            lane_tracker.clone(),
        );

        let (repair_commands, _repair_receivers) = reliable_path_command_channels(8);
        let repair_commands_for_detach = repair_commands.clone();
        assert_eq!(
            binding.attach(
                shared_key.underlay,
                shared_key.path_id,
                repair_commands,
                FlowLane::Throughput,
                StreamOpenRole::Repair,
                reliable_relay_buffer_len(MuxLimits::default()),
            ),
            ResponseStreamAttachOutcome::Attached
        );
        assert_eq!(
            lane_tracker.snapshot(session_id, shared_key).active_flows,
            1
        );
        binding.detach(shared_key, &repair_commands_for_detach);
        assert_eq!(
            lane_tracker.snapshot(session_id, shared_key).active_flows,
            1,
            "detaching passive Repair must not debit another stream's share"
        );

        let (validation_commands, _validation_receivers) = reliable_path_command_channels(8);
        let validation_commands_for_promotion = validation_commands.clone();
        let validation_commands_for_repeat = validation_commands.clone();
        let validation_commands_for_detach = validation_commands.clone();
        assert_eq!(
            binding.attach(
                shared_key.underlay,
                shared_key.path_id,
                validation_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                reliable_relay_buffer_len(MuxLimits::default()),
            ),
            ResponseStreamAttachOutcome::Attached
        );
        assert_eq!(
            lane_tracker.snapshot(session_id, shared_key).active_flows,
            1
        );
        assert_eq!(lane_tracker.session_snapshot(session_id).active_flows, 2);

        assert_eq!(
            binding.attach(
                shared_key.underlay,
                shared_key.path_id,
                validation_commands_for_promotion,
                FlowLane::Latency,
                StreamOpenRole::Active,
                reliable_relay_buffer_len(MuxLimits::default()),
            ),
            ResponseStreamAttachOutcome::RoleChanged
        );
        let service_load = lane_tracker.snapshot(session_id, service_key);
        assert_eq!(service_load.active_flows, 1);
        assert_eq!(service_load.active_latency_sensitive_flows, 1);
        let shared_load = lane_tracker.snapshot(session_id, shared_key);
        assert_eq!(shared_load.active_flows, 2);
        assert_eq!(
            shared_load.active_latency_sensitive_flows, 1,
            "promotion must add this stream in its new lane without moving the other stream"
        );

        assert_eq!(
            binding.attach(
                shared_key.underlay,
                shared_key.path_id,
                validation_commands_for_repeat,
                FlowLane::Latency,
                StreamOpenRole::Active,
                reliable_relay_buffer_len(MuxLimits::default()),
            ),
            ResponseStreamAttachOutcome::Attached
        );
        let repeated_shared_load = lane_tracker.snapshot(session_id, shared_key);
        assert_eq!(repeated_shared_load.active_flows, shared_load.active_flows);
        assert_eq!(
            repeated_shared_load.active_latency_sensitive_flows,
            shared_load.active_latency_sensitive_flows
        );

        binding.detach(shared_key, &validation_commands_for_detach);
        let remaining_shared_load = lane_tracker.snapshot(session_id, shared_key);
        assert_eq!(remaining_shared_load.active_flows, 1);
        assert_eq!(remaining_shared_load.active_latency_sensitive_flows, 0);
        drop(binding);
        assert_eq!(
            lane_tracker.snapshot(session_id, service_key).active_flows,
            0
        );
        assert_eq!(
            lane_tracker.snapshot(session_id, shared_key).active_flows,
            1
        );
        drop(shared_binding);
        assert_eq!(
            lane_tracker.snapshot(session_id, shared_key).active_flows,
            0
        );
    }

    #[test]
    fn closed_output_replacement_reconciles_role_flow_load() {
        let session_id = SessionId(98);
        let key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let lane_tracker = Arc::new(ServerPathLaneTracker::default());
        let (active_commands, active_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new_with_limits_and_tracker(
            session_id,
            key.underlay,
            key.path_id,
            active_commands,
            FlowLane::Throughput,
            MuxLimits::default(),
            lane_tracker.clone(),
        );
        assert_eq!(lane_tracker.snapshot(session_id, key).active_flows, 1);
        assert_eq!(binding.lane_generation_and_active_response_flows().1, 1);
        drop(active_receivers);

        let (validation_commands, validation_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                key.underlay,
                key.path_id,
                validation_commands,
                FlowLane::Latency,
                StreamOpenRole::Validation,
                reliable_relay_buffer_len(MuxLimits::default()),
            ),
            ResponseStreamAttachOutcome::ReplacedClosedOutput
        );
        assert_eq!(lane_tracker.snapshot(session_id, key).active_flows, 0);
        assert_eq!(binding.lane_generation_and_active_response_flows().1, 0);
        drop(validation_receivers);

        let (replacement_commands, _replacement_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                key.underlay,
                key.path_id,
                replacement_commands,
                FlowLane::Throughput,
                StreamOpenRole::Active,
                reliable_relay_buffer_len(MuxLimits::default()),
            ),
            ResponseStreamAttachOutcome::ReplacedClosedOutput
        );
        let replacement_load = lane_tracker.snapshot(session_id, key);
        assert_eq!(replacement_load.active_flows, 1);
        assert_eq!(replacement_load.active_latency_sensitive_flows, 0);
        assert_eq!(binding.lane_generation_and_active_response_flows().1, 1);
        drop(binding);
        assert_eq!(lane_tracker.snapshot(session_id, key).active_flows, 0);
    }

    #[tokio::test]
    async fn close_command_detaches_shared_lane_load_exactly_once() {
        let session_id = SessionId(96);
        let key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let lane_tracker = Arc::new(ServerPathLaneTracker::default());
        let (first_commands, _first_receivers) = reliable_path_command_channels(8);
        let first_commands_for_detach = first_commands.clone();
        let first = ResponseStreamBinding::new_with_limits_and_tracker(
            session_id,
            key.underlay,
            key.path_id,
            first_commands,
            FlowLane::Throughput,
            MuxLimits::default(),
            lane_tracker.clone(),
        );
        let (second_commands, _second_receivers) = reliable_path_command_channels(8);
        let second = ResponseStreamBinding::new_with_limits_and_tracker(
            session_id,
            key.underlay,
            key.path_id,
            second_commands,
            FlowLane::Throughput,
            MuxLimits::default(),
            lane_tracker.clone(),
        );
        assert_eq!(lane_tracker.snapshot(session_id, key).active_flows, 2);
        let stale_target = first
            .sender_path_targets(FlowLane::Throughput, 64 * 1024)
            .into_iter()
            .find(|target| target.key == key)
            .expect("first response Service target");

        first.close_stream(StreamId(10)).await;
        assert_eq!(
            lane_tracker.snapshot(session_id, key).active_flows,
            2,
            "enqueuing close does not complete carrier detachment"
        );
        assert_eq!(
            first.response_scheduling_snapshot().service_family_loads,
            ResponseServiceFamilyLoads::new(1, 0),
            "close retires product Service ownership independently of attachment cleanup"
        );
        assert!(!first.commit_ordered_data_owner_for_target(&stale_target));
        first.set_lane(FlowLane::Latency);
        assert_eq!(
            first.response_scheduling_snapshot().service_family_loads,
            ResponseServiceFamilyLoads::new(1, 0),
            "a stale owner commit or lane change cannot resurrect closed Service load"
        );

        first.detach(key, &first_commands_for_detach);
        first.detach(key, &first_commands_for_detach);
        assert_eq!(
            lane_tracker.snapshot(session_id, key).active_flows,
            1,
            "command handling and repeated cleanup must leave the other stream counted"
        );

        drop(first);
        assert_eq!(lane_tracker.snapshot(session_id, key).active_flows, 1);
        drop(second);
        assert_eq!(lane_tracker.snapshot(session_id, key).active_flows, 0);
    }

    #[test]
    fn old_flight_ack_does_not_debit_or_prove_replaced_output() {
        let session_id = SessionId(92);
        let key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let (old_commands, old_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new(
            session_id,
            key.underlay,
            key.path_id,
            old_commands,
            FlowLane::Throughput,
        );
        let frame = stream_data_frame(BBR_MAX_SEND_QUANTUM_BYTES);
        binding.record_owner_flight(key, &frame);
        drop(old_receivers);

        let (new_commands, _new_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                key.underlay,
                key.path_id,
                new_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                reliable_relay_buffer_len(MuxLimits::default()),
            ),
            ResponseStreamAttachOutcome::ReplacedClosedOutput
        );
        assert_eq!(
            binding.ordered_data_owner(),
            None,
            "a fresh Validation incarnation must not inherit the closed Service owner"
        );
        let replacement = binding
            .sender_path_targets(FlowLane::Throughput, BBR_MAX_SEND_QUANTUM_BYTES)
            .into_iter()
            .find(|target| target.key == key)
            .expect("replacement target remains attached");
        assert!(!replacement.is_active);
        let replacement_frame = stream_data_frame_at(
            BBR_MAX_SEND_QUANTUM_BYTES as u64,
            BBR_MAX_SEND_QUANTUM_BYTES,
        );
        binding.record_owner_flight_for_target(&replacement, &replacement_frame);
        assert_eq!(
            first_output_entry(&binding).bytes_in_flight,
            BBR_MAX_SEND_QUANTUM_BYTES as u64
        );
        binding.release_normalized_acked_ranges(&[OffsetRange {
            start: 0,
            end: BBR_MAX_SEND_QUANTUM_BYTES as u64,
        }]);

        let entry = first_output_entry(&binding);
        assert_eq!(
            entry.bytes_in_flight, BBR_MAX_SEND_QUANTUM_BYTES as u64,
            "an old output ACK must not debit replacement flight accounting"
        );
        assert_eq!(entry.owner_data_acked_bytes, 0);
        assert_eq!(entry.delivery_samples, 0);
        assert!(entry.product_progress_rate_bps.is_none());
    }

    #[test]
    fn late_old_output_record_cannot_account_or_prove_replacement() {
        let session_id = SessionId(95);
        let key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let (old_commands, old_receivers) = reliable_path_command_channels(8);
        let old_commands_for_detach = old_commands.clone();
        let binding = ResponseStreamBinding::new(
            session_id,
            key.underlay,
            key.path_id,
            old_commands,
            FlowLane::Throughput,
        );
        let stale_target = binding
            .sender_path_targets(FlowLane::Throughput, BBR_MAX_SEND_QUANTUM_BYTES)
            .into_iter()
            .next()
            .expect("initial target exists");
        drop(old_receivers);
        binding.detach(key, &old_commands_for_detach);
        assert_eq!(binding.ordered_data_owner(), None);

        let (new_commands, _new_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                key.underlay,
                key.path_id,
                new_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                reliable_relay_buffer_len(MuxLimits::default()),
            ),
            ResponseStreamAttachOutcome::Attached
        );

        let frame = stream_data_frame(BBR_MAX_SEND_QUANTUM_BYTES);
        binding.record_owner_flight_for_target(&stale_target, &frame);
        assert!(
            !binding.commit_ordered_data_owner_for_target(&stale_target),
            "a stale plan must not restore ownership after detach"
        );
        assert_eq!(binding.ordered_data_owner(), None);
        assert!(
            binding
                .sender_path_targets(FlowLane::Throughput, BBR_MAX_SEND_QUANTUM_BYTES)
                .iter()
                .all(|target| !target.is_active),
            "a same-key Validation replacement must not inherit stale Service ownership"
        );
        assert_eq!(first_output_entry(&binding).bytes_in_flight, 0);
        binding.release_normalized_acked_ranges(&[OffsetRange {
            start: 0,
            end: BBR_MAX_SEND_QUANTUM_BYTES as u64,
        }]);

        let entry = first_output_entry(&binding);
        assert_eq!(entry.bytes_in_flight, 0);
        assert_eq!(entry.owner_data_acked_bytes, 0);
        assert_eq!(entry.delivery_samples, 0);
        assert!(entry.product_progress_rate_bps.is_none());
    }

    #[test]
    fn old_acked_hole_cannot_prove_replacement_when_frontier_advances() {
        let session_id = SessionId(96);
        let key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let (old_commands, old_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new(
            session_id,
            key.underlay,
            key.path_id,
            old_commands,
            FlowLane::Throughput,
        );
        binding.record_owner_flight(key, &stream_data_frame_at(0, 1024));
        binding.record_owner_flight(key, &stream_data_frame_at(1024, 1024));
        binding.release_normalized_acked_ranges(&[OffsetRange {
            start: 1024,
            end: 2048,
        }]);
        drop(old_receivers);

        let (new_commands, _new_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                key.underlay,
                key.path_id,
                new_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                reliable_relay_buffer_len(MuxLimits::default()),
            ),
            ResponseStreamAttachOutcome::ReplacedClosedOutput
        );
        binding.release_normalized_acked_ranges(&[OffsetRange {
            start: 0,
            end: 1024,
        }]);

        let entry = first_output_entry(&binding);
        assert_eq!(entry.owner_data_acked_bytes, 0);
        assert_eq!(entry.delivery_samples, 0);
        assert!(entry.product_progress_rate_bps.is_none());
    }

    #[test]
    fn live_role_change_clears_evidence_and_invalidates_old_flights() {
        let (binding, _) = binding_for_underlay(UnderlayProtocol::Tcp);
        let key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let (commands, _receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                key.underlay,
                key.path_id,
                commands.clone(),
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                reliable_relay_buffer_len(MuxLimits::default()),
            ),
            ResponseStreamAttachOutcome::Attached
        );
        let frame = stream_data_frame(BBR_MAX_SEND_QUANTUM_BYTES);
        binding.record_owner_flight(key, &frame);
        let before_role_change = first_output_entry(&binding);
        assert_eq!(
            before_role_change.bytes_in_flight,
            BBR_MAX_SEND_QUANTUM_BYTES as u64
        );
        let previous_incarnation = before_role_change.incarnation;
        assert_eq!(
            binding.attach(
                key.underlay,
                key.path_id,
                commands,
                FlowLane::Throughput,
                StreamOpenRole::Repair,
                reliable_relay_buffer_len(MuxLimits::default()),
            ),
            ResponseStreamAttachOutcome::RoleChanged
        );
        let after_role_change = first_output_entry(&binding);
        assert_ne!(after_role_change.incarnation, previous_incarnation);
        assert_eq!(
            after_role_change.bytes_in_flight, BBR_MAX_SEND_QUANTUM_BYTES as u64,
            "live role change must preserve actual outstanding product debt"
        );
        binding.release_normalized_acked_ranges(&[OffsetRange {
            start: 0,
            end: BBR_MAX_SEND_QUANTUM_BYTES as u64,
        }]);

        let outputs = binding.outputs.lock().expect("test response outputs lock");
        let entry = outputs
            .entries
            .iter()
            .find(|entry| entry.key == key)
            .expect("role-changed output remains attached");
        assert_eq!(entry.role, StreamOpenRole::Repair);
        assert_eq!(entry.bytes_in_flight, 0);
        assert_eq!(entry.owner_data_acked_bytes, 0);
        assert_eq!(entry.delivery_samples, 0);
        assert!(entry.product_progress_rate_bps.is_none());
    }

    #[test]
    fn validation_to_active_preserves_response_identity_evidence_and_subflow_epoch() {
        let limits = MuxLimits::default();
        let sample_bytes = reliable_subflow_startup_sample_limit_bytes(limits) as usize;
        let (binding, service) = binding_for_underlay(UnderlayProtocol::Udp);
        let candidate = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let (commands, _receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                candidate.underlay,
                candidate.path_id,
                commands.clone(),
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                reliable_relay_buffer_len(limits),
            ),
            ResponseStreamAttachOutcome::Attached
        );
        let startup_input = SubflowAdmissionInput {
            key: candidate,
            bulk_rate_proven: false,
            startup_owner_allowed: true,
            frontier_clear: true,
            completion_improves: false,
            observed_goodput_non_degrading: true,
            read_gap: Duration::ZERO,
            owner_bytes: sample_bytes,
            optional_overhead_bytes: 0,
        };
        assert_eq!(
            binding
                .commit_subflow_owner_admission(
                    service,
                    sample_bytes,
                    0,
                    Duration::ZERO,
                    startup_input,
                )
                .decision,
            PathAdmissionDecision::AdmitSubflow
        );
        let incarnation = {
            let mut outputs = binding.outputs.lock().expect("test response outputs lock");
            let entry = outputs
                .entries
                .iter_mut()
                .find(|entry| entry.key == candidate)
                .expect("Validation output");
            mark_test_quic_output_carrier_bulk_proven(entry, limits);
            entry.incarnation
        };
        let (planner_generation, epoch) = binding.subflow_state_snapshot();
        assert_eq!(
            epoch.as_ref().and_then(FlowSubflowSet::startup_owner_key),
            Some(candidate)
        );

        assert_eq!(
            binding.attach(
                candidate.underlay,
                candidate.path_id,
                commands,
                FlowLane::Throughput,
                StreamOpenRole::Active,
                reliable_relay_buffer_len(limits),
            ),
            ResponseStreamAttachOutcome::RoleChanged
        );

        let target = binding
            .sender_path_targets(FlowLane::Throughput, BBR_MAX_SEND_QUANTUM_BYTES)
            .into_iter()
            .find(|target| target.key == candidate)
            .expect("promoted response output");
        assert_eq!(target.incarnation, incarnation);
        assert!(target.has_bulk_rate_evidence);
        let (after_generation, after_epoch) = binding.subflow_state_snapshot();
        assert_eq!(after_generation, planner_generation);
        assert_eq!(
            after_epoch.and_then(|epoch| epoch.startup_owner_key()),
            Some(candidate),
            "request-role promotion cannot erase paid-for response membership"
        );
    }

    #[test]
    fn late_record_from_pre_role_change_plan_is_not_path_proving() {
        let (binding, _) = binding_for_underlay(UnderlayProtocol::Tcp);
        let key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let (commands, _receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                key.underlay,
                key.path_id,
                commands.clone(),
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                reliable_relay_buffer_len(MuxLimits::default()),
            ),
            ResponseStreamAttachOutcome::Attached
        );
        let stale_target = binding
            .sender_path_targets(FlowLane::Throughput, BBR_MAX_SEND_QUANTUM_BYTES)
            .into_iter()
            .find(|target| target.key == key)
            .expect("validation target is attached");
        assert_eq!(
            binding.attach(
                key.underlay,
                key.path_id,
                commands,
                FlowLane::Throughput,
                StreamOpenRole::Repair,
                reliable_relay_buffer_len(MuxLimits::default()),
            ),
            ResponseStreamAttachOutcome::RoleChanged
        );

        let frame = stream_data_frame(BBR_MAX_SEND_QUANTUM_BYTES);
        binding.record_owner_flight_for_target(&stale_target, &frame);
        assert_eq!(
            first_output_entry(&binding).bytes_in_flight,
            BBR_MAX_SEND_QUANTUM_BYTES as u64,
            "a late record on the same live channel must follow the new incarnation as non-proving debt"
        );
        binding.release_normalized_acked_ranges(&[OffsetRange {
            start: 0,
            end: BBR_MAX_SEND_QUANTUM_BYTES as u64,
        }]);

        let outputs = binding.outputs.lock().expect("test response outputs lock");
        let entry = outputs
            .entries
            .iter()
            .find(|entry| entry.key == key)
            .expect("role-changed output remains attached");
        assert_eq!(entry.bytes_in_flight, 0);
        assert_eq!(entry.owner_data_acked_bytes, 0);
        assert_eq!(entry.delivery_samples, 0);
        assert!(entry.product_progress_rate_bps.is_none());
    }

    #[test]
    fn pre_role_change_acked_hole_cannot_restore_delivery_evidence() {
        let (binding, _) = binding_for_underlay(UnderlayProtocol::Tcp);
        let key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let (commands, _receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                key.underlay,
                key.path_id,
                commands.clone(),
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                reliable_relay_buffer_len(MuxLimits::default()),
            ),
            ResponseStreamAttachOutcome::Attached
        );
        binding.record_owner_flight(key, &stream_data_frame_at(0, 1024));
        binding.record_owner_flight(key, &stream_data_frame_at(1024, 1024));
        binding.release_normalized_acked_ranges(&[OffsetRange {
            start: 1024,
            end: 2048,
        }]);

        assert_eq!(
            binding.attach(
                key.underlay,
                key.path_id,
                commands,
                FlowLane::Throughput,
                StreamOpenRole::Repair,
                reliable_relay_buffer_len(MuxLimits::default()),
            ),
            ResponseStreamAttachOutcome::RoleChanged
        );
        binding.release_normalized_acked_ranges(&[OffsetRange {
            start: 0,
            end: 1024,
        }]);

        let outputs = binding.outputs.lock().expect("test response outputs lock");
        let entry = outputs
            .entries
            .iter()
            .find(|entry| entry.key == key)
            .expect("role-changed output remains attached");
        assert_eq!(entry.owner_data_acked_bytes, 0);
        assert_eq!(entry.delivery_samples, 0);
        assert!(entry.product_progress_rate_bps.is_none());
    }

    #[test]
    fn response_acked_hole_debt_counts_unique_ordering_owner_only() {
        let (binding, owner) = binding_for_underlay(UnderlayProtocol::Udp);
        let duplicate = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let (duplicate_commands, _duplicate_receivers) = reliable_path_command_channels(8);
        binding.attach(
            duplicate.underlay,
            duplicate.path_id,
            duplicate_commands,
            FlowLane::Throughput,
            StreamOpenRole::Repair,
            reliable_relay_buffer_len(MuxLimits::default()),
        );
        let lower_missing = stream_data_frame_at(0, 1024);
        let later = stream_data_frame_at(1024, 4096);
        binding.record_owner_flight(owner, &lower_missing);
        binding.record_owner_flight(owner, &later);
        binding.record_repair_flight(duplicate, &later);

        binding.release_normalized_acked_ranges(&[OffsetRange {
            start: 1024,
            end: 5120,
        }]);

        let lower = binding.lower_flights_before_offset(5120);
        assert_eq!(lower.len(), 1);
        assert_eq!(lower[0].key, owner);
        assert_eq!(
            lower[0].bytes, 4096,
            "acked hole debt must not double-count repair duplicate copies"
        );
        let ordering = binding
            .ack_ordering
            .lock()
            .expect("server response ACK ordering lock");
        assert_eq!(ordering.acked_hole_bytes(), 4096);
    }

    #[test]
    fn peer_app_limited_metrics_do_not_seed_response_bulk_rate_or_envelope() {
        for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
            let (binding, key) = binding_for_underlay(underlay);
            let metrics = PathMetrics {
                path_id: key.path_id,
                underlay: key.underlay,
                direction: PathMetricDirection::ServerToClient,
                metric_epoch: metric_epoch_now(),
                metric_age_us: 0,
                min_rtt_us: 20_000,
                srtt_us: 20_000,
                rttvar_us: 1_000,
                jitter_us: 1_000,
                delivery_rate_bps: 614_000,
                pacing_rate_bps: 614_000,
                loss_ppm: 0,
                ecn_ppm: 0,
                loss_observed: false,
                ecn_observed: false,
                bytes_in_flight: PATH_OPEN_SCORE_BYTES as u64,
                queue_bytes: PATH_OPEN_SCORE_BYTES as u64,
                inflight_limit_bytes: PATH_OPEN_SCORE_BYTES as u64,
                inflight_hi_bytes: PATH_OPEN_SCORE_BYTES as u64,
                confidence_ppm: 900_000,
                app_limited: true,
                has_ack_derived_data_sample: true,
                data_sample_count: 142,
                data_sample_bytes: 0,
            };
            binding.update_path_metrics(key, metrics, ServerPathMetricsSource::PeerHint);

            let snapshot = binding
                .send_path_snapshot(FlowLane::Throughput, MIN_RATE_SAMPLE_BYTES as usize)
                .expect("peer metrics remain validation hints");
            assert_eq!(snapshot.delivery_rate_bps, default_path_rate_bps(underlay));
            assert_eq!(snapshot.pacing_rate_bps, snapshot.delivery_rate_bps);
            assert_eq!(snapshot.inflight_limit_bytes, 0);
            assert_eq!(snapshot.bytes_in_flight, 0);
            assert_eq!(snapshot.confidence, 0.0);
            assert!(snapshot.app_limited);
        }
    }

    #[test]
    fn response_peer_hint_yields_to_durable_local_quic_estimate() {
        let (binding, key) = binding_for_underlay(UnderlayProtocol::Udp);
        let mut peer_hint = PathMetrics {
            path_id: key.path_id,
            underlay: key.underlay,
            direction: PathMetricDirection::ClientToServer,
            metric_epoch: metric_epoch_now(),
            metric_age_us: 0,
            min_rtt_us: 200_000,
            srtt_us: 200_000,
            rttvar_us: 10_000,
            jitter_us: 10_000,
            delivery_rate_bps: 200_000_000,
            pacing_rate_bps: 200_000_000,
            loss_ppm: 0,
            ecn_ppm: 0,
            loss_observed: false,
            ecn_observed: false,
            bytes_in_flight: 0,
            queue_bytes: 0,
            inflight_limit_bytes: 0,
            inflight_hi_bytes: 0,
            confidence_ppm: 100_000,
            app_limited: false,
            has_ack_derived_data_sample: false,
            data_sample_count: 0,
            data_sample_bytes: 0,
        };
        binding.update_path_metrics(key, peer_hint, ServerPathMetricsSource::PeerHint);

        let local_proof = PathMetrics {
            direction: PathMetricDirection::ServerToClient,
            min_rtt_us: 20_000,
            srtt_us: 20_000,
            rttvar_us: 1_000,
            jitter_us: 1_000,
            delivery_rate_bps: 500_000,
            pacing_rate_bps: 500_000,
            confidence_ppm: 1_000_000,
            app_limited: true,
            ..peer_hint
        };
        binding.update_path_metrics(key, local_proof, ServerPathMetricsSource::LocalSender);

        let snapshot = binding
            .send_path_snapshot(FlowLane::Throughput, MIN_RATE_SAMPLE_BYTES as usize)
            .expect("path remains attached");

        assert_eq!(snapshot.delivery_rate_bps, 200_000_000.0);
        assert_eq!(snapshot.srtt_ms, 20.0);
        assert!(snapshot.app_limited);

        peer_hint.delivery_rate_bps = 300_000_000;
        binding.update_path_metrics(key, peer_hint, ServerPathMetricsSource::PeerHint);
        let updated = binding
            .send_path_snapshot(FlowLane::Throughput, MIN_RATE_SAMPLE_BYTES as usize)
            .expect("path remains attached");
        assert_eq!(updated.delivery_rate_bps, 300_000_000.0);
        assert_eq!(
            updated.srtt_ms, 20.0,
            "local liveness RTT must not be erased by peer hint refresh"
        );

        let durable_local = PathMetrics {
            metric_epoch: metric_epoch_now(),
            delivery_rate_bps: 500_000,
            pacing_rate_bps: 500_000,
            app_limited: true,
            has_ack_derived_data_sample: true,
            data_sample_count: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
            data_sample_bytes: reliable_subflow_startup_sample_limit_bytes(MuxLimits::default()),
            ..local_proof
        };
        binding.update_path_metrics(key, durable_local, ServerPathMetricsSource::LocalSender);
        let local_estimate = binding
            .send_path_snapshot(FlowLane::Throughput, MIN_RATE_SAMPLE_BYTES as usize)
            .expect("path remains attached");
        assert_eq!(local_estimate.delivery_rate_bps, 500_000.0);
        assert!(local_estimate.app_limited);
        let entry = first_output_entry(&binding);
        assert!(!server_output_has_bulk_rate_evidence(&entry));
    }

    #[test]
    fn tcp_local_sender_metrics_remain_send_quantum_prior_after_low_product_sample() {
        let (binding, key) = binding_for_underlay(UnderlayProtocol::Tcp);
        let mux_limits = binding.mux_limits();
        let metrics = PathMetrics {
            path_id: key.path_id,
            underlay: key.underlay,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: metric_epoch_now(),
            metric_age_us: 0,
            min_rtt_us: 20_000,
            srtt_us: 20_000,
            rttvar_us: 1_000,
            jitter_us: 1_000,
            delivery_rate_bps: 500_000_000,
            pacing_rate_bps: 500_000_000,
            loss_ppm: 0,
            ecn_ppm: 0,
            loss_observed: true,
            ecn_observed: false,
            bytes_in_flight: 0,
            queue_bytes: 0,
            inflight_limit_bytes: 0,
            inflight_hi_bytes: 0,
            confidence_ppm: 0,
            app_limited: false,
            has_ack_derived_data_sample: true,
            data_sample_count: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
            data_sample_bytes: MIN_RATE_SAMPLE_BYTES,
        };
        binding.update_path_metrics(key, metrics, ServerPathMetricsSource::LocalSender);

        let before_ack = binding
            .send_path_snapshot(FlowLane::Throughput, MIN_RATE_SAMPLE_BYTES as usize)
            .expect("path metrics seed response path snapshot");
        assert_eq!(before_ack.delivery_rate_bps, 500_000_000.0);

        let frame = stream_data_frame(MIN_RATE_SAMPLE_BYTES as usize);
        let later = stream_data_frame_at(MIN_RATE_SAMPLE_BYTES, MIN_RATE_SAMPLE_BYTES as usize);
        binding.record_owner_flight(key, &frame);
        binding.record_owner_flight(key, &later);
        let first_ack = Instant::now() + RESPONSE_ACK_CLOCK_GOODPUT_MIN_ELAPSED;
        binding.release_normalized_acked_ranges_at(
            &[OffsetRange {
                start: 0,
                end: reliable_stream_frame_payload_bytes(&frame) as u64,
            }],
            first_ack,
        );
        binding.release_normalized_acked_ranges_at(
            &[OffsetRange {
                start: MIN_RATE_SAMPLE_BYTES,
                end: 2 * MIN_RATE_SAMPLE_BYTES,
            }],
            first_ack + RESPONSE_ACK_CLOCK_GOODPUT_MIN_ELAPSED,
        );

        let entry = first_output_entry(&binding);
        assert_eq!(entry.delivery_samples, 2);
        assert!(
            entry.delivery_rate_bps.unwrap_or(f64::INFINITY) < 500_000_000.0,
            "the test must create a low product progress sample"
        );
        let after_ack = binding
            .send_path_snapshot(FlowLane::Throughput, MIN_RATE_SAMPLE_BYTES as usize)
            .expect("peer rate prior remains available after product ACK sample");
        assert_eq!(after_ack.delivery_rate_bps, 500_000_000.0);
        assert!(
            adaptive_reliable_relay_chunk_bytes(Some(after_ack), FlowLane::Throughput, mux_limits)
                > bbr_min_send_quantum_bytes(mux_limits),
            "a low product ACK sample must not collapse TCP send quantum below the path-rate prior"
        );
    }

    #[test]
    fn tcp_fixed_output_startup_prior_yields_after_persistent_local_delivery_samples() {
        let mux_limits = MuxLimits::default();
        let (commands, _receivers) = reliable_path_command_channels(64);
        let startup_rate = 500_000_000.0;
        let startup = PathSnapshot::new(PathId(8), UnderlayProtocol::Tcp, 20.0, startup_rate);
        let output = ReliablePathStreamOutput::fixed_with_snapshot(startup, commands, mux_limits);
        let ReliablePathStreamOutput::Fixed(fixed) = &output else {
            panic!("expected fixed output");
        };
        let mut offset = 0_u64;

        for _ in 0..RELIABLE_INITIAL_WINDOW_PACKETS {
            let frame = stream_data_frame_at(offset, MIN_RATE_SAMPLE_BYTES as usize);
            let end = offset + reliable_stream_frame_payload_bytes(&frame) as u64;
            fixed.record_owner_flight(&frame);
            std::thread::sleep(Duration::from_millis(20));
            fixed.release_normalized_acked_ranges(&[OffsetRange { start: offset, end }]);
            offset = end;
        }

        let learned_rate = fixed
            .model
            .lock()
            .expect("fixed output model lock")
            .delivery_rate_bps
            .expect("persistent samples produce a delivery model");
        assert!(learned_rate < startup_rate * 0.5);

        let snapshot = output
            .send_path_snapshot(FlowLane::Throughput, MIN_RATE_SAMPLE_BYTES as usize)
            .expect("response binding exposes learned path model");
        assert!(
            snapshot.delivery_rate_bps < startup_rate * 0.5,
            "startup/default rate is only a hint; persistent local delivery samples must correct it downward"
        );
    }

    #[test]
    fn fixed_output_request_active_snapshot_preserves_send_path_timing() {
        let mux_limits = MuxLimits::default();
        let (commands, _receivers) = reliable_path_command_channels(8);
        let startup = PathSnapshot::new(PathId(9), UnderlayProtocol::Tcp, 123.0, 8_000_000.0);
        let output = ReliablePathStreamOutput::fixed_with_snapshot(startup, commands, mux_limits);
        let send_snapshot = output
            .send_path_snapshot(FlowLane::Latency, PATH_OPEN_SCORE_BYTES)
            .expect("fixed output has a send path snapshot");
        let request_active_snapshot = output
            .request_active_path_snapshot(FlowLane::Latency)
            .expect("fixed output has a request Active path snapshot");

        assert_eq!(request_active_snapshot.id, send_snapshot.id);
        assert_eq!(request_active_snapshot.underlay, send_snapshot.underlay);
        assert_eq!(request_active_snapshot.srtt_ms, send_snapshot.srtt_ms);
        assert_eq!(
            reliable_stream_recv_progress_interval(
                Some(request_active_snapshot),
                FlowLane::Latency,
            ),
            reliable_stream_recv_progress_interval(Some(send_snapshot), FlowLane::Latency),
            "fixed-path replay cadence must remain unchanged"
        );
    }
}
