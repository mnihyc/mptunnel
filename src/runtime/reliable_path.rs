use super::*;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

static NEXT_SERVER_CARRIER_PATH_INSTANCE_ID: AtomicU64 = AtomicU64::new(1);
const RESPONSE_OWNER_TCP_SEEN: u8 = 1 << 0;
const RESPONSE_OWNER_UDP_SEEN: u8 = 1 << 1;
const RESPONSE_OWNER_MIXED_SEEN: u8 = RESPONSE_OWNER_TCP_SEEN | RESPONSE_OWNER_UDP_SEEN;

fn response_owner_underlay_seen_bit(underlay: UnderlayProtocol) -> u8 {
    match underlay {
        UnderlayProtocol::Tcp => RESPONSE_OWNER_TCP_SEEN,
        UnderlayProtocol::Udp => RESPONSE_OWNER_UDP_SEEN,
    }
}

mod registry;
mod response_admission;

pub(in crate::runtime) use registry::*;
pub(super) use response_admission::*;

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

    pub(super) fn set_sender_queue_bytes(&self, bytes: usize) {
        self.output.set_sender_queue_bytes(bytes);
    }

    pub(super) fn subscribe_output_updates(&self) -> Option<watch::Receiver<u64>> {
        self.output.subscribe_updates()
    }

    pub(super) fn capacity_notifies(&self) -> Vec<Arc<Notify>> {
        self.output.capacity_notifies()
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

    pub(super) fn enqueue_path_proof(&self) -> Result<(), RuntimeError> {
        self.output.enqueue_path_proof()
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

    fn enqueue_path_proof(&self) -> Result<(), RuntimeError> {
        enqueue_path_proof_frame(&self.commands, self.key.path_id, self.mux_limits)
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

    fn enqueue_path_proof(&self) -> Result<(), RuntimeError> {
        match self {
            Self::Fixed(fixed) => fixed.enqueue_path_proof(),
            Self::Switchable(_) => Ok(()),
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
pub(super) struct ResponseSourceServiceSnapshot {
    pub(super) key: CarrierPathKey,
    pub(super) active_latency_sensitive_flows: u32,
    pub(super) has_bulk_rate_evidence: bool,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ResponseRelayReadSnapshot {
    pub(super) send_path: Option<PathSnapshot>,
    pub(super) source_service: Option<ResponseSourceServiceSnapshot>,
    pub(super) independent_source_staging: bool,
}

pub(super) struct ResponseStreamBinding {
    session_id: SessionId,
    lane: Mutex<FlowLane>,
    mux_limits: MuxLimits,
    lane_tracker: Arc<ServerPathLaneTracker>,
    response_flow_registration: ServerResponseFlowRegistration,
    next_output_incarnation: AtomicU64,
    owner_underlay_history: AtomicU8,
    outputs: Mutex<ResponseStreamOutputs>,
    request_active_owner: Mutex<Option<CarrierPathKey>>,
    ordered_data_owner: Mutex<Option<CarrierPathKey>>,
    flights: Mutex<BTreeMap<u64, Vec<CarrierPathFlight>>>,
    ack_ordering: Mutex<ResponseAckOrderingState>,
    subflow_set: Mutex<ResponseSubflowSetState>,
    version: watch::Sender<u64>,
}

impl Drop for ResponseStreamBinding {
    fn drop(&mut self) {
        self.response_flow_registration.set_active(false);
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
            ServerResponseFlowRegistration::new(lane_tracker.clone(), session_id);
        lane_tracker.attach(session_id, key, lane);
        response_flow_registration.set_active(true);
        Arc::new(Self {
            session_id,
            lane: Mutex::new(lane),
            mux_limits,
            lane_tracker,
            response_flow_registration,
            next_output_incarnation: AtomicU64::new(2),
            owner_underlay_history: AtomicU8::new(response_owner_underlay_seen_bit(underlay)),
            outputs: Mutex::new(ResponseStreamOutputs {
                entries: vec![ResponseStreamOutputEntry {
                    key,
                    path_instance_id,
                    incarnation: 1,
                    commands,
                    role: StreamOpenRole::Active,
                    bytes_in_flight: 0,
                    product_queue_bytes: 0,
                    product_progress_rate_bps: None,
                    delivery_rate_bps: None,
                    srtt_ms: None,
                    delivery_samples: 0,
                    owner_data_acked_bytes: 0,
                    last_delivery_at: None,
                    local_path_metrics: None,
                    peer_path_metrics: None,
                }],
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
                    let role_changed_incarnations = role_changed.then(|| {
                        let previous = entry.incarnation;
                        let current = self.allocate_output_incarnation();
                        (previous, current)
                    });
                    if role_changed {
                        entry.incarnation = role_changed_incarnations
                            .expect("role change allocates an output incarnation")
                            .1;
                        entry.product_progress_rate_bps = None;
                        entry.delivery_rate_bps = None;
                        entry.srtt_ms = None;
                        entry.delivery_samples = 0;
                        entry.owner_data_acked_bytes = 0;
                        entry.last_delivery_at = None;
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
                        role_changed_incarnations
                    {
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
                    if role_changed {
                        // Publish the new output role and reset both Subflow
                        // identities at one outputs-lock linearization point.
                        self.reset_subflow_set();
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
                entry.path_instance_id = path_instance_id;
                entry.incarnation = self.allocate_output_incarnation();
                entry.commands = commands;
                entry.role = role;
                entry.bytes_in_flight = 0;
                entry.product_queue_bytes = 0;
                entry.product_progress_rate_bps = None;
                entry.delivery_rate_bps = None;
                entry.srtt_ms = None;
                entry.delivery_samples = 0;
                entry.owner_data_acked_bytes = 0;
                entry.last_delivery_at = None;
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
                bytes_in_flight: 0,
                product_queue_bytes: 0,
                product_progress_rate_bps: None,
                delivery_rate_bps: None,
                srtt_ms: None,
                delivery_samples: 0,
                owner_data_acked_bytes: 0,
                last_delivery_at: None,
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
            self.invalidate_path_flight_evidence(key, incarnation);
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
            self.reset_subflow_set();
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
                    response_stream_role_reserves_flow_load(entry.role),
                )
            });
        outputs
            .entries
            .retain(|entry| entry.key != key || !matches(entry));
        if let Some((incarnation, load_registered)) = removed {
            if response_flow_was_active && !Self::response_flow_is_active(&outputs) {
                self.sync_response_flow_activity(&outputs);
            }
            self.invalidate_path_flight_evidence(key, incarnation);
            if load_registered {
                self.lane_tracker.detach(self.session_id, key, lane);
            }
            self.repair_ordered_data_owner_after_output_change(&outputs.entries);
            self.reset_subflow_set();
            self.clear_request_active_owner_if(key);
            drop(outputs);
            drop(current_lane);
            self.notify_update();
        }
    }

    pub(super) fn release_normalized_acked_ranges(&self, ranges: &[OffsetRange]) {
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
            drop(outputs);
            let ordering_update = self
                .ack_ordering
                .lock()
                .expect("server response ACK ordering lock")
                .apply_normalized_ack(ranges, &[]);
            if ordering_update.changed {
                self.notify_update();
            }
            return;
        }
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

        let now = Instant::now();
        let mut changed = false;
        let mut path_samples = HashMap::<(CarrierPathKey, u64), (u64, Instant)>::new();
        for (_, release) in released {
            let flight = release.flight;
            if let Some(entry) = outputs.entries.iter_mut().find(|entry| {
                entry.key == flight.key && entry.incarnation == flight.output_incarnation
            }) {
                entry.bytes_in_flight = entry.bytes_in_flight.saturating_sub(flight.bytes as u64);
                if release.path_proving {
                    entry.owner_data_acked_bytes = entry
                        .owner_data_acked_bytes
                        .saturating_add(flight.bytes as u64);
                    let sample = path_samples
                        .entry((flight.key, flight.output_incarnation))
                        .or_insert((0_u64, flight.sent_at));
                    sample.0 = sample.0.saturating_add(flight.bytes as u64);
                    sample.1 = sample.1.min(flight.sent_at);
                }
                changed = true;
            }
        }
        for ((key, output_incarnation), (bytes, first_sent_at)) in path_samples {
            if let Some(entry) = outputs
                .entries
                .iter_mut()
                .find(|entry| entry.key == key && entry.incarnation == output_incarnation)
                && let Some(sample) =
                    PathRateSample::new(bytes, now.saturating_duration_since(first_sent_at))
            {
                let sample_bps = sample.rate_bps();
                let carrier_app_limited = entry
                    .local_path_metrics
                    .is_some_and(|metrics| metrics.metrics.app_limited);
                entry.product_progress_rate_bps = Some(match entry.product_progress_rate_bps {
                    Some(previous) if carrier_app_limited => previous.max(sample_bps),
                    Some(previous) => previous.mul_add(0.75, sample_bps * 0.25),
                    None => sample_bps,
                });
                if entry.key.underlay == UnderlayProtocol::Tcp {
                    entry.delivery_rate_bps = Some(match entry.delivery_rate_bps {
                        Some(previous) if carrier_app_limited => previous.max(sample_bps),
                        Some(previous) => previous.mul_add(0.75, sample_bps * 0.25),
                        None => sample_bps,
                    });
                    let sample_rtt_ms =
                        now.saturating_duration_since(first_sent_at).as_secs_f64() * 1000.0;
                    entry.srtt_ms = Some(match entry.srtt_ms {
                        Some(previous) => previous.mul_add(0.875, sample_rtt_ms * 0.125),
                        None => sample_rtt_ms,
                    });
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
                    entry.last_delivery_at = Some(now);
                    changed = true;
                }
            }
        }
        drop(outputs);
        if changed || ordering_update.changed {
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
        let mut lead = self
            .ordered_data_owner
            .lock()
            .expect("server reliable stream ordered data owner lock");
        if *lead != Some(key) {
            *lead = Some(key);
            self.reset_subflow_set();
            drop(lead);
            self.notify_update();
        }
    }

    pub(super) fn commit_ordered_data_owner_for_target(
        &self,
        target: &ResponseSenderPathTarget,
    ) -> bool {
        let outputs = self
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
        let changed = *lead != Some(target.key);
        if changed {
            *lead = Some(target.key);
            self.reset_subflow_set();
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
        if *lead == Some(key) {
            *lead = None;
        }
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

    #[cfg(test)]
    pub(super) fn lane_generation(&self) -> u64 {
        self.lane_tracker.generation(self.session_id)
    }

    pub(super) fn lane_generation_and_active_response_flows(&self) -> (u64, u32) {
        self.lane_tracker
            .generation_and_active_response_flows(self.session_id)
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

    pub(super) fn reset_subflow_set(&self) {
        let mut state = self
            .subflow_set
            .lock()
            .expect("server reliable stream subflow set lock");
        state.planner_generation = state.planner_generation.wrapping_add(1);
        state.epoch_generation = state.epoch_generation.wrapping_add(1);
        state.set = None;
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
        if !live_lead {
            *lead = None;
        }
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

    pub(super) fn try_enqueue_owner_frame_for_target(
        &self,
        target: &ResponseSenderPathTarget,
        frame: &Frame,
        lane: FlowLane,
        subflow_request: Option<ResponseSubflowAdmissionRequest>,
    ) -> Result<Option<u64>, RuntimeError> {
        self.try_enqueue_owner_frame_for_target_inner(target, frame, lane, subflow_request, || {})
    }

    fn try_enqueue_owner_frame_for_target_inner(
        &self,
        target: &ResponseSenderPathTarget,
        frame: &Frame,
        lane: FlowLane,
        subflow_request: Option<ResponseSubflowAdmissionRequest>,
        after_subflow_reservation: impl FnOnce(),
    ) -> Result<Option<u64>, RuntimeError> {
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
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
    }

    pub(super) fn record_repair_flight_for_target(
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
            CarrierWorkKind::RepairData,
        )
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
        outputs
            .entries
            .iter()
            .map(|entry| {
                let snapshot = server_bulk_output_snapshot(
                    entry,
                    self.session_id,
                    lane,
                    &self.lane_tracker,
                    self.mux_limits,
                    now,
                );
                ResponseSenderPathTarget {
                    key: entry.key,
                    incarnation: entry.incarnation,
                    commands: entry.commands.clone(),
                    attachment_role: entry.role,
                    snapshot,
                    eta_ms: server_bulk_output_eta_ms(
                        entry.key,
                        snapshot,
                        active_key,
                        lane,
                        payload_bytes,
                        self.mux_limits,
                    ),
                    is_active: Some(entry.key) == active_key,
                    is_request_active: Some(entry.key) == request_active_key,
                    has_sender_evidence: server_output_has_sender_evidence(entry),
                    has_bulk_rate_evidence: server_output_has_bulk_rate_evidence_with_limits(
                        entry,
                        self.mux_limits,
                    ),
                }
            })
            .collect()
    }

    pub(super) fn mux_limits(&self) -> MuxLimits {
        self.mux_limits
    }

    pub(super) fn session_id(&self) -> SessionId {
        self.session_id
    }

    pub(super) async fn close_stream(&self, stream_id: StreamId) {
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock")
            .entries
            .clone();
        for entry in outputs {
            let _ = entry
                .commands
                .send_control(ReliablePathCommand::CloseStream(stream_id))
                .await;
        }
        if let Ok(mut lead) = self.ordered_data_owner.lock() {
            *lead = None;
        }
    }

    pub(super) async fn close_stream_ordered(&self, stream_id: StreamId, lane: FlowLane) {
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock")
            .entries
            .clone();
        for entry in outputs {
            let _ = entry
                .commands
                .send_stream_ordered_close(stream_id, lane)
                .await;
        }
        if let Ok(mut lead) = self.ordered_data_owner.lock() {
            *lead = None;
        }
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
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let mut changed = false;
        for entry in &mut outputs.entries {
            if entry.key == key
                && path_instance_id.is_none_or(|instance| entry.path_instance_id == instance)
            {
                let path_metrics = Some(ServerPathMetricsEntry { metrics, source });
                match source {
                    ServerPathMetricsSource::LocalSender => {
                        entry.local_path_metrics = path_metrics;
                    }
                    ServerPathMetricsSource::PeerHint => {
                        entry.peer_path_metrics = path_metrics;
                    }
                }
                changed = true;
            }
        }
        drop(outputs);
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
            self.notify_update();
        }
    }
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

pub(in crate::runtime) fn next_server_carrier_path_instance_id() -> ServerCarrierPathInstanceId {
    ServerCarrierPathInstanceId(NEXT_SERVER_CARRIER_PATH_INSTANCE_ID.fetch_add(1, Ordering::AcqRel))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ServerPathLoadKey {
    session_id: SessionId,
    path: CarrierPathKey,
}

#[derive(Debug, Clone, Copy, Default)]
struct ServerPathLaneLoad {
    active_flows: u32,
    active_latency_sensitive_flows: u32,
}

impl ServerPathLaneLoad {
    fn add(&mut self, lane: FlowLane) {
        self.active_flows = self.active_flows.saturating_add(1);
        if reliable_relay_expects_interactive_response(lane) {
            self.active_latency_sensitive_flows =
                self.active_latency_sensitive_flows.saturating_add(1);
        }
    }

    fn remove(&mut self, lane: FlowLane) {
        self.active_flows = self.active_flows.saturating_sub(1);
        if reliable_relay_expects_interactive_response(lane) {
            self.active_latency_sensitive_flows =
                self.active_latency_sensitive_flows.saturating_sub(1);
        }
    }
}

#[derive(Debug, Default)]
/// Per-session lane load snapshot for Active response-stream attachments.
///
/// The tracker informs scheduling and diagnostics only. It is not a path queue
/// and cannot reorder product frames after sender-service admission.
pub(super) struct ServerPathLaneTracker {
    state: Mutex<ServerPathLaneTrackerState>,
}

#[derive(Debug, Default)]
struct ServerPathLaneTrackerState {
    loads: HashMap<ServerPathLoadKey, ServerPathLaneLoad>,
    realtime_flows: HashMap<SessionId, u32>,
    active_response_flows: HashMap<SessionId, u32>,
    session_references: HashMap<SessionId, u32>,
    session_generations: HashMap<SessionId, u64>,
}

impl ServerPathLaneTrackerState {
    fn bump_generation(&mut self, session_id: SessionId) {
        let generation = self.session_generations.entry(session_id).or_default();
        *generation = generation.wrapping_add(1);
    }

    fn maybe_reclaim_session(&mut self, session_id: SessionId) {
        let has_references = self
            .session_references
            .get(&session_id)
            .is_some_and(|count| *count > 0);
        let has_realtime = self
            .realtime_flows
            .get(&session_id)
            .is_some_and(|count| *count > 0);
        let has_active_response_flows = self
            .active_response_flows
            .get(&session_id)
            .is_some_and(|count| *count > 0);
        let has_loads = self.loads.keys().any(|key| key.session_id == session_id);
        if !has_references && !has_realtime && !has_active_response_flows && !has_loads {
            self.session_generations.remove(&session_id);
        }
    }
}

impl ServerPathLaneTracker {
    fn attach_session(&self, session_id: SessionId) {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let references = state.session_references.entry(session_id).or_default();
        *references = references.saturating_add(1);
    }

    fn detach_session(&self, session_id: SessionId) {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        if let Some(references) = state.session_references.get_mut(&session_id) {
            *references = references.saturating_sub(1);
            if *references == 0 {
                state.session_references.remove(&session_id);
            }
        }
        state.maybe_reclaim_session(session_id);
    }

    #[cfg(test)]
    fn generation(&self, session_id: SessionId) -> u64 {
        self.state
            .lock()
            .expect("server path lane tracker lock")
            .session_generations
            .get(&session_id)
            .copied()
            .unwrap_or(0)
    }

    fn generation_and_active_response_flows(&self, session_id: SessionId) -> (u64, u32) {
        let state = self.state.lock().expect("server path lane tracker lock");
        let generation = state
            .session_generations
            .get(&session_id)
            .copied()
            .unwrap_or(0);
        let active_response_flows = state
            .active_response_flows
            .get(&session_id)
            .copied()
            .unwrap_or(0);
        (generation, active_response_flows)
    }

    fn with_matching_generation<R>(
        &self,
        session_id: SessionId,
        expected_generation: u64,
        apply: impl FnOnce() -> R,
    ) -> Option<R> {
        let state = self.state.lock().expect("server path lane tracker lock");
        let generation = state
            .session_generations
            .get(&session_id)
            .copied()
            .unwrap_or(0);
        if generation != expected_generation {
            return None;
        }
        let result = apply();
        drop(state);
        Some(result)
    }

    fn attach(&self, session_id: SessionId, path: CarrierPathKey, lane: FlowLane) {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        state
            .loads
            .entry(ServerPathLoadKey { session_id, path })
            .or_default()
            .add(lane);
        state.bump_generation(session_id);
    }

    fn detach(&self, session_id: SessionId, path: CarrierPathKey, lane: FlowLane) {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let key = ServerPathLoadKey { session_id, path };
        let changed = if let Some(load) = state.loads.get_mut(&key) {
            load.remove(lane);
            if load.active_flows == 0 {
                state.loads.remove(&key);
            }
            true
        } else {
            false
        };
        if changed {
            state.bump_generation(session_id);
        }
        state.maybe_reclaim_session(session_id);
    }

    fn change_lanes(
        &self,
        session_id: SessionId,
        paths: &[CarrierPathKey],
        from: FlowLane,
        to: FlowLane,
    ) {
        if from == to {
            return;
        }
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let mut changed = false;
        for path in paths {
            if let Some(load) = state.loads.get_mut(&ServerPathLoadKey {
                session_id,
                path: *path,
            }) {
                load.remove(from);
                load.add(to);
                changed = true;
            }
        }
        if changed {
            state.bump_generation(session_id);
        }
        state.maybe_reclaim_session(session_id);
    }

    fn attach_realtime_flow(&self, session_id: SessionId) {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let count = state.realtime_flows.entry(session_id).or_default();
        *count = count.saturating_add(1);
        state.bump_generation(session_id);
    }

    fn set_response_flow_active(&self, session_id: SessionId, active: bool) {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        if active {
            let count = state.active_response_flows.entry(session_id).or_default();
            *count = count.saturating_add(1);
            state.bump_generation(session_id);
            return;
        }

        let changed = if let Some(count) = state.active_response_flows.get_mut(&session_id) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                state.active_response_flows.remove(&session_id);
            }
            true
        } else {
            false
        };
        if changed {
            state.bump_generation(session_id);
        }
        state.maybe_reclaim_session(session_id);
    }

    fn detach_realtime_flow(&self, session_id: SessionId) {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let changed = if let Some(count) = state.realtime_flows.get_mut(&session_id) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                state.realtime_flows.remove(&session_id);
            }
            true
        } else {
            false
        };
        if changed {
            state.bump_generation(session_id);
        }
        state.maybe_reclaim_session(session_id);
    }

    fn snapshot(&self, session_id: SessionId, path: CarrierPathKey) -> ServerPathLaneLoad {
        self.state
            .lock()
            .expect("server path lane tracker lock")
            .loads
            .get(&ServerPathLoadKey { session_id, path })
            .copied()
            .unwrap_or_default()
    }

    fn session_snapshot(&self, session_id: SessionId) -> ServerPathLaneLoad {
        let state = self.state.lock().expect("server path lane tracker lock");
        let mut total = state
            .loads
            .iter()
            .filter(|(key, _)| key.session_id == session_id)
            .fold(ServerPathLaneLoad::default(), |mut total, (_, load)| {
                total.active_flows = total.active_flows.saturating_add(load.active_flows);
                total.active_latency_sensitive_flows = total
                    .active_latency_sensitive_flows
                    .saturating_add(load.active_latency_sensitive_flows);
                total
            });
        let realtime_flows = state.realtime_flows.get(&session_id).copied().unwrap_or(0);
        total.active_flows = total.active_flows.saturating_add(realtime_flows);
        total.active_latency_sensitive_flows = total
            .active_latency_sensitive_flows
            .saturating_add(realtime_flows);
        total
    }
}

struct ServerResponseFlowRegistration {
    lane_tracker: Arc<ServerPathLaneTracker>,
    session_id: SessionId,
    active: Mutex<bool>,
}

impl ServerResponseFlowRegistration {
    fn new(lane_tracker: Arc<ServerPathLaneTracker>, session_id: SessionId) -> Self {
        lane_tracker.attach_session(session_id);
        Self {
            lane_tracker,
            session_id,
            active: Mutex::new(false),
        }
    }

    fn set_active(&self, active: bool) {
        let mut current = self
            .active
            .lock()
            .expect("server response flow registration lock");
        if *current == active {
            return;
        }
        self.lane_tracker
            .set_response_flow_active(self.session_id, active);
        *current = active;
    }
}

impl Drop for ServerResponseFlowRegistration {
    fn drop(&mut self) {
        self.set_active(false);
        self.lane_tracker.detach_session(self.session_id);
    }
}

pub(super) struct ServerRealtimeFlowRegistration {
    lane_tracker: Arc<ServerPathLaneTracker>,
    session_id: SessionId,
}

impl ServerRealtimeFlowRegistration {
    fn new(lane_tracker: Arc<ServerPathLaneTracker>, session_id: SessionId) -> Self {
        lane_tracker.attach_realtime_flow(session_id);
        Self {
            lane_tracker,
            session_id,
        }
    }
}

impl Drop for ServerRealtimeFlowRegistration {
    fn drop(&mut self) {
        self.lane_tracker.detach_realtime_flow(self.session_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::bulk_admission::bulk_service_feed_reservoir_payload_bytes;
    use crate::runtime::relay_io::reliable_relay_source_staging_owner_tail_headroom;
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
                response_subflow_startup_sample_limit_bytes(binding.mux_limits());
        }
        let after = binding.relay_read_snapshot(FlowLane::Throughput, PATH_OPEN_SCORE_BYTES);
        let source = after.source_service.expect("live ordered Service snapshot");
        assert!(source.has_bulk_rate_evidence);
        assert_eq!(source.active_latency_sensitive_flows, 0);
        assert_eq!(
            reliable_relay_source_staging_owner_tail_headroom(
                after.independent_source_staging,
                FlowLane::Throughput,
                true,
                source.active_latency_sensitive_flows > 0,
                source.has_bulk_rate_evidence,
                0,
                0,
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
    fn udp_product_confidence_does_not_mature_response_source_staging() {
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
            !source.has_bulk_rate_evidence,
            "UDP source staging requires local carrier ACK-derived bulk evidence"
        );
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

        let outputs = binding.outputs.lock().expect("test response outputs lock");
        let repair_entry = outputs
            .entries
            .iter()
            .find(|entry| entry.key == repair)
            .expect("repair output remains attached");
        assert_eq!(repair_entry.role, StreamOpenRole::Active);
        drop(outputs);
        assert_eq!(binding.ordered_data_owner(), Some(active));
        assert_eq!(
            binding.request_active_owner(),
            Some(repair),
            "request Active reannounce must not depend on the response data owner"
        );
        assert_eq!(
            binding.owner_underlay_history.load(Ordering::Acquire),
            RESPONSE_OWNER_MIXED_SEEN
        );
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
    fn tcp_stream_ack_can_seed_response_path_rate_when_no_packet_carrier_exists() {
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
        assert!(entry.delivery_rate_bps.is_some());
        assert!(entry.srtt_ms.is_some());
    }

    #[test]
    fn cumulative_unique_owner_acks_graduate_tcp_but_not_udp_without_carrier_evidence() {
        let sample_bytes = response_subflow_startup_sample_limit_bytes(MuxLimits::default());
        let frame_bytes = BBR_MAX_SEND_QUANTUM_BYTES as u64;
        assert_eq!(sample_bytes % frame_bytes, 0);

        for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
            let (binding, key) = binding_for_underlay(underlay);
            for offset in (0..sample_bytes).step_by(BBR_MAX_SEND_QUANTUM_BYTES) {
                binding.record_owner_flight(
                    key,
                    &stream_data_frame_at(offset, BBR_MAX_SEND_QUANTUM_BYTES),
                );
            }
            std::thread::sleep(Duration::from_millis(1));
            binding.release_normalized_acked_ranges(&[OffsetRange {
                start: 0,
                end: sample_bytes,
            }]);

            let entry = first_output_entry(&binding);
            assert_eq!(entry.owner_data_acked_bytes, sample_bytes, "{underlay:?}");
            assert!(entry.product_progress_rate_bps.is_some(), "{underlay:?}");
            assert_eq!(
                server_output_has_bulk_rate_evidence(&entry),
                underlay == UnderlayProtocol::Tcp,
                "TCP may use product owner ACKs; QUIC requires local carrier bulk evidence"
            );
        }
    }

    #[test]
    fn duplicate_response_validation_copy_does_not_become_ordering_owner() {
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
        let frame = stream_data_frame_at(0, 4096);

        binding.record_owner_flight(owner, &frame);
        binding.record_repair_flight(duplicate, &frame);

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
                &target,
                &frame_for_sender,
                FlowLane::Throughput,
                Some(request),
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

        first.close_stream(StreamId(10)).await;
        assert_eq!(
            lane_tracker.snapshot(session_id, key).active_flows,
            2,
            "enqueuing close does not complete carrier detachment"
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
    fn response_peer_hint_rate_survives_local_proof_liveness_metrics() {
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
        binding.record_owner_flight(key, &frame);
        std::thread::sleep(Duration::from_millis(20));
        binding.release_normalized_acked_ranges(&[OffsetRange {
            start: 0,
            end: reliable_stream_frame_payload_bytes(&frame) as u64,
        }]);

        let entry = first_output_entry(&binding);
        assert_eq!(entry.delivery_samples, 1);
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
}
