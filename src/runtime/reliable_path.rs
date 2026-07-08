use super::*;
use std::collections::{BTreeMap, HashMap};

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

    pub(super) fn has_multipath_repair_alternative(&self) -> bool {
        self.output.has_multipath_repair_alternative()
    }

    pub(super) fn has_repair_output_for_frame(&self, frame: &Frame) -> bool {
        self.output.has_repair_output_for_frame(frame)
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
                end,
                bytes,
                sent_at: Instant::now(),
                kind,
            });
    }

    fn release_normalized_acked_ranges(&self, ranges: &[OffsetRange]) {
        if ranges.is_empty() {
            return;
        }
        let mut model = self.model.lock().expect("fixed reliable path model lock");
        let acked_offsets = model
            .flights
            .iter()
            .filter_map(|(offset, path_flights)| {
                path_flights
                    .iter()
                    .any(|flight| {
                        ranges
                            .iter()
                            .any(|range| range.start <= *offset && range.end >= flight.end)
                    })
                    .then_some(*offset)
            })
            .collect::<Vec<_>>();
        if acked_offsets.is_empty() {
            return;
        }
        let now = Instant::now();
        let mut sample_bytes = 0_u64;
        let mut sample_start = now;
        let mut released_proven_flights = 0_u32;
        for offset in acked_offsets {
            if let Some(path_flights) = model.flights.remove(&offset) {
                let path_proving = carrier_path_flights_have_unambiguous_owner_ack(&path_flights);
                for flight in path_flights {
                    model.bytes_in_flight =
                        model.bytes_in_flight.saturating_sub(flight.bytes as u64);
                    if path_proving && flight.kind.is_ordering_owner() {
                        sample_bytes = sample_bytes.saturating_add(flight.bytes as u64);
                        sample_start = sample_start.min(flight.sent_at);
                        released_proven_flights = released_proven_flights.saturating_add(1);
                    }
                }
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
}

pub(super) fn reliable_work_lane_to_carrier_lane(
    work_lane: ReliableRelayQueuedWorkLane,
    relay_lane: FlowLane,
) -> FlowLane {
    match work_lane {
        ReliableRelayQueuedWorkLane::Control => FlowLane::Control,
        ReliableRelayQueuedWorkLane::Repair => FlowLane::Latency,
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
pub(super) struct ResponseStreamBinding {
    session_id: SessionId,
    lane: Mutex<FlowLane>,
    mux_limits: MuxLimits,
    lane_tracker: Arc<ServerPathLaneTracker>,
    outputs: Mutex<ResponseStreamOutputs>,
    ordered_data_owner: Mutex<Option<CarrierPathKey>>,
    flights: Mutex<BTreeMap<u64, Vec<CarrierPathFlight>>>,
    ack_ordering: Mutex<ResponseAckOrderingState>,
    subflow_set: Mutex<Option<FlowSubflowSet>>,
    version: watch::Sender<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ResponseStreamAttachOutcome {
    Attached,
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

    fn new_with_limits_and_tracker(
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: ReliablePathCommandSender,
        lane: FlowLane,
        mux_limits: MuxLimits,
        lane_tracker: Arc<ServerPathLaneTracker>,
    ) -> Arc<Self> {
        let (version, _) = watch::channel(0);
        let key = CarrierPathKey { underlay, path_id };
        lane_tracker.attach(session_id, key, lane);
        Arc::new(Self {
            session_id,
            lane: Mutex::new(lane),
            mux_limits,
            lane_tracker,
            outputs: Mutex::new(ResponseStreamOutputs {
                entries: vec![ResponseStreamOutputEntry {
                    key,
                    commands,
                    bytes_in_flight: 0,
                    product_queue_bytes: 0,
                    product_progress_rate_bps: None,
                    delivery_rate_bps: None,
                    srtt_ms: None,
                    delivery_samples: 0,
                    last_delivery_at: None,
                    local_path_metrics: None,
                    peer_path_metrics: None,
                }],
            }),
            ordered_data_owner: Mutex::new(Some(key)),
            flights: Mutex::new(BTreeMap::new()),
            ack_ordering: Mutex::new(ResponseAckOrderingState::default()),
            subflow_set: Mutex::new(None),
            version,
        })
    }

    pub(super) fn attach(
        &self,
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: ReliablePathCommandSender,
        lane: FlowLane,
        role: StreamOpenRole,
        _max_frame_payload_bytes: usize,
    ) -> ResponseStreamAttachOutcome {
        let previous_lane = *self.lane.lock().expect("server reliable stream lane lock");
        let proof_commands = commands.clone();
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let key = CarrierPathKey { underlay, path_id };
        let mut was_active = false;
        let mut already_attached = false;
        let existing_position = outputs.entries.iter().position(|entry| entry.key == key);
        if let Some(position) = existing_position {
            let entry = &outputs.entries[position];
            if !entry.commands.is_closed() {
                let same_channel = entry.commands.same_channel(&commands);
                #[cfg(feature = "lab-diagnostics")]
                let attach_result = if same_channel {
                    "same_channel_role_update"
                } else {
                    "duplicate_live"
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
                    let attached_keys = outputs
                        .entries
                        .iter()
                        .map(|entry| entry.key)
                        .collect::<Vec<_>>();
                    drop(outputs);
                    let previous_lane =
                        *self.lane.lock().expect("server reliable stream lane lock");
                    {
                        let mut current_lane =
                            self.lane.lock().expect("server reliable stream lane lock");
                        *current_lane = lane;
                    }
                    if previous_lane != lane {
                        self.lane_tracker.change_lanes(
                            self.session_id,
                            &attached_keys,
                            previous_lane,
                            lane,
                        );
                    }
                    self.notify_update();
                    return ResponseStreamAttachOutcome::Attached;
                }
                return ResponseStreamAttachOutcome::RejectedDuplicateLiveOutput;
            }
        }
        let entry = if let Some(position) = existing_position {
            was_active = position + 1 == outputs.entries.len();
            already_attached = true;
            let mut entry = outputs.entries.remove(position);
            if entry.commands.is_closed() {
                entry.commands = commands;
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
                commands,
                bytes_in_flight: 0,
                product_queue_bytes: 0,
                product_progress_rate_bps: None,
                delivery_rate_bps: None,
                srtt_ms: None,
                delivery_samples: 0,
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
        let attached_keys = outputs
            .entries
            .iter()
            .map(|entry| entry.key)
            .collect::<Vec<_>>();
        drop(outputs);
        {
            let mut current_lane = self.lane.lock().expect("server reliable stream lane lock");
            *current_lane = lane;
        }
        if previous_lane != lane {
            self.lane_tracker
                .change_lanes(self.session_id, &attached_keys, previous_lane, lane);
        }
        if !already_attached {
            self.lane_tracker.attach(self.session_id, key, lane);
        }
        if role == StreamOpenRole::Validation {
            let _ = enqueue_path_proof_frame(&proof_commands, path_id, self.mux_limits);
        }
        self.notify_update();
        ResponseStreamAttachOutcome::Attached
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
        let stored_active_key = self.ordered_data_owner();
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let active_key = response_live_ordered_data_owner(stored_active_key, &outputs.entries);
        outputs.read_backpressure_snapshot(
            active_key,
            self.session_id,
            &self.lane_tracker,
            lane,
            payload_bytes,
            self.mux_limits,
        )
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
        let previous_lane = {
            let mut current_lane = self.lane.lock().expect("server reliable stream lane lock");
            let previous = *current_lane;
            *current_lane = lane;
            previous
        };
        if previous_lane != lane {
            let outputs = self
                .outputs
                .lock()
                .expect("server reliable stream binding lock");
            let attached_keys = outputs
                .entries
                .iter()
                .map(|entry| entry.key)
                .collect::<Vec<_>>();
            drop(outputs);
            self.lane_tracker
                .change_lanes(self.session_id, &attached_keys, previous_lane, lane);
        }
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

    pub(super) fn detach(&self, key: CarrierPathKey, commands: &ReliablePathCommandSender) {
        let lane = self.lane();
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let before = outputs.entries.len();
        outputs
            .entries
            .retain(|entry| entry.key != key || !entry.commands.same_channel(commands));
        if outputs.entries.len() != before {
            let live_entries = outputs.entries.clone();
            drop(outputs);
            self.lane_tracker.detach(self.session_id, key, lane);
            self.repair_ordered_data_owner_after_output_change(&live_entries);
            self.reset_subflow_set();
            self.notify_update();
        }
    }

    pub(super) fn release_normalized_acked_ranges(&self, ranges: &[OffsetRange]) {
        if ranges.is_empty() {
            return;
        }
        let mut flights = self
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        let acked_offsets = flights
            .iter()
            .filter_map(|(offset, path_flights)| {
                path_flights
                    .iter()
                    .any(|flight| {
                        ranges
                            .iter()
                            .any(|range| range.start <= *offset && range.end >= flight.end)
                    })
                    .then_some(*offset)
            })
            .collect::<Vec<_>>();
        if acked_offsets.is_empty() {
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
        let mut released = Vec::new();
        for offset in acked_offsets {
            if let Some(path_flights) = flights.remove(&offset) {
                let path_proving = carrier_path_flights_have_unambiguous_owner_ack(&path_flights);
                released.extend(path_flights.into_iter().map(|flight| {
                    (
                        offset,
                        CarrierPathReleasedFlight {
                            flight,
                            path_proving: path_proving && flight.kind.is_ordering_owner(),
                        },
                    )
                }));
            }
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

        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let now = Instant::now();
        let mut changed = false;
        let mut path_samples = HashMap::<CarrierPathKey, (u64, Instant)>::new();
        for (_, release) in released {
            let flight = release.flight;
            if let Some(entry) = outputs
                .entries
                .iter_mut()
                .find(|entry| entry.key == flight.key)
            {
                entry.bytes_in_flight = entry.bytes_in_flight.saturating_sub(flight.bytes as u64);
                if release.path_proving {
                    let sample = path_samples
                        .entry(flight.key)
                        .or_insert((0_u64, flight.sent_at));
                    sample.0 = sample.0.saturating_add(flight.bytes as u64);
                    sample.1 = sample.1.min(flight.sent_at);
                }
                changed = true;
            }
        }
        for (key, (bytes, first_sent_at)) in path_samples {
            if let Some(entry) = outputs.entries.iter_mut().find(|entry| entry.key == key)
                && let Some(sample) =
                    PathRateSample::new(bytes, now.saturating_duration_since(first_sent_at))
            {
                let sample_bps = sample.rate_bps();
                entry.product_progress_rate_bps = Some(match entry.product_progress_rate_bps {
                    Some(previous) => previous.mul_add(0.75, sample_bps * 0.25),
                    None => sample_bps,
                });
                if entry.key.underlay == UnderlayProtocol::Tcp {
                    entry.delivery_rate_bps = Some(match entry.delivery_rate_bps {
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
                .find(|entry| entry.key == hole.key)
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
            // admission credit is epoch state. Recreate it only when membership
            // or the admission envelope changes.
            self.notify_update();
        }
    }

    pub(super) fn ordered_data_owner(&self) -> Option<CarrierPathKey> {
        *self
            .ordered_data_owner
            .lock()
            .expect("server reliable stream ordered data owner lock")
    }

    pub(super) fn set_ordered_data_owner(&self, key: CarrierPathKey) {
        let mut lead = self
            .ordered_data_owner
            .lock()
            .expect("server reliable stream ordered data owner lock");
        if *lead != Some(key) {
            *lead = Some(key);
            drop(lead);
            self.notify_update();
        }
    }

    fn subflow_set_for(
        current: Option<FlowSubflowSet>,
        service: CarrierPathKey,
        owner_credit_bytes: usize,
        optional_overhead_budget_bytes: usize,
        max_read_gap_budget: Duration,
    ) -> FlowSubflowSet {
        current
            .filter(|epoch| {
                epoch.matches_envelope(
                    service,
                    owner_credit_bytes,
                    optional_overhead_budget_bytes,
                    max_read_gap_budget,
                )
            })
            .unwrap_or_else(|| {
                FlowSubflowSet::new(
                    0,
                    service,
                    owner_credit_bytes,
                    optional_overhead_budget_bytes,
                    max_read_gap_budget,
                )
            })
    }

    pub(super) fn subflow_set_snapshot(&self) -> Option<FlowSubflowSet> {
        self.subflow_set
            .lock()
            .expect("server reliable stream subflow set lock")
            .clone()
    }

    #[cfg(test)]
    pub(super) fn preview_subflow_owner_admission(
        &self,
        service: CarrierPathKey,
        owner_credit_bytes: usize,
        optional_overhead_budget_bytes: usize,
        max_read_gap_budget: Duration,
        input: SubflowAdmissionInput,
    ) -> PathAdmission {
        let current = self
            .subflow_set
            .lock()
            .expect("server reliable stream subflow set lock")
            .clone();
        let mut epoch = Self::subflow_set_for(
            current,
            service,
            owner_credit_bytes,
            optional_overhead_budget_bytes,
            max_read_gap_budget,
        );
        epoch.admit_subflow_owner(input)
    }

    pub(super) fn commit_subflow_owner_admission(
        &self,
        service: CarrierPathKey,
        owner_credit_bytes: usize,
        optional_overhead_budget_bytes: usize,
        max_read_gap_budget: Duration,
        input: SubflowAdmissionInput,
    ) -> PathAdmission {
        let mut guard = self
            .subflow_set
            .lock()
            .expect("server reliable stream subflow set lock");
        let current = guard.take();
        let mut epoch = Self::subflow_set_for(
            current,
            service,
            owner_credit_bytes,
            optional_overhead_budget_bytes,
            max_read_gap_budget,
        );
        let admission = epoch.admit_subflow_owner(input);
        *guard = Some(epoch);
        admission
    }

    pub(super) fn reset_subflow_set(&self) {
        *self
            .subflow_set
            .lock()
            .expect("server reliable stream subflow set lock") = None;
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
            *lead = response_best_failover_ordered_data_owner(live_entries);
        }
    }

    pub(super) fn record_owner_flight(&self, key: CarrierPathKey, frame: &Frame) {
        self.record_product_flight(key, frame, CarrierWorkKind::OwnerData)
    }

    pub(super) fn record_repair_flight(&self, key: CarrierPathKey, frame: &Frame) {
        self.record_product_flight(key, frame, CarrierWorkKind::RepairData)
    }

    fn record_product_flight(&self, key: CarrierPathKey, frame: &Frame, kind: CarrierWorkKind) {
        debug_assert!(kind.carries_product_offsets());
        let Some((offset, end, bytes)) = reliable_stream_frame_extent(frame) else {
            return;
        };
        {
            let mut outputs = self
                .outputs
                .lock()
                .expect("server reliable stream binding lock");
            if let Some(entry) = outputs.entries.iter_mut().find(|entry| entry.key == key) {
                entry.bytes_in_flight = entry.bytes_in_flight.saturating_add(bytes as u64);
            }
        }
        self.flights
            .lock()
            .expect("server reliable stream flight lock")
            .entry(offset)
            .or_default()
            .push(CarrierPathFlight {
                key,
                end,
                bytes,
                sent_at: Instant::now(),
                kind,
            });
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
                    commands: entry.commands.clone(),
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
                    has_sender_evidence: server_output_has_sender_evidence(entry),
                    has_bulk_rate_evidence: server_output_has_bulk_rate_evidence(entry),
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
        let lane = self.lane();
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
            self.lane_tracker.detach(self.session_id, entry.key, lane);
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
            self.lane_tracker.detach(self.session_id, entry.key, lane);
        }
        if let Ok(mut lead) = self.ordered_data_owner.lock() {
            *lead = None;
        }
    }

    fn notify_update(&self) {
        let current = *self.version.borrow();
        let _ = self.version.send(current.wrapping_add(1));
    }

    pub(super) fn update_path_metrics(
        &self,
        key: CarrierPathKey,
        metrics: PathMetrics,
        source: ServerPathMetricsSource,
    ) {
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let mut changed = false;
        for entry in &mut outputs.entries {
            if entry.key == key {
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

fn carrier_path_flights_have_unambiguous_owner_ack(flights: &[CarrierPathFlight]) -> bool {
    matches!(
        flights,
        [flight] if flight.kind.is_ordering_owner()
    )
}

fn response_live_ordered_data_owner(
    stored: Option<CarrierPathKey>,
    entries: &[ResponseStreamOutputEntry],
) -> Option<CarrierPathKey> {
    stored
        .filter(|key| entries.iter().any(|entry| entry.key == *key))
        .or_else(|| response_best_failover_ordered_data_owner(entries))
}

fn response_best_failover_ordered_data_owner(
    entries: &[ResponseStreamOutputEntry],
) -> Option<CarrierPathKey> {
    entries
        .iter()
        .enumerate()
        .filter(|(_, entry)| {
            !entry.commands.is_closed() && server_output_has_bulk_rate_evidence(entry)
        })
        .max_by_key(|(index, entry)| {
            (
                response_failover_rate_bps(entry),
                entry.delivery_samples,
                usize::MAX.saturating_sub(*index),
            )
        })
        .map(|(_, entry)| entry.key)
}

fn response_failover_rate_bps(entry: &ResponseStreamOutputEntry) -> u64 {
    entry
        .local_path_metrics
        .map(|path_metrics| path_metrics.metrics.delivery_rate_bps.max(1))
        .or_else(|| entry.delivery_rate_bps.and_then(f64_to_positive_u64))
        .or_else(|| {
            entry
                .product_progress_rate_bps
                .and_then(f64_to_positive_u64)
        })
        .or_else(|| {
            entry
                .peer_path_metrics
                .map(|path_metrics| path_metrics.metrics.delivery_rate_bps.max(1))
        })
        .unwrap_or(1)
}

fn f64_to_positive_u64(value: f64) -> Option<u64> {
    value.is_finite().then_some(value.max(1.0) as u64)
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
/// Per-session lane load snapshot for joined carrier paths.
///
/// The tracker informs scheduling and diagnostics only. It is not a path queue
/// and cannot reorder product frames after sender-service admission.
struct ServerPathLaneTracker {
    loads: Mutex<HashMap<ServerPathLoadKey, ServerPathLaneLoad>>,
}

impl ServerPathLaneTracker {
    fn attach(&self, session_id: SessionId, path: CarrierPathKey, lane: FlowLane) {
        self.loads
            .lock()
            .expect("server path lane tracker lock")
            .entry(ServerPathLoadKey { session_id, path })
            .or_default()
            .add(lane);
    }

    fn detach(&self, session_id: SessionId, path: CarrierPathKey, lane: FlowLane) {
        let mut loads = self.loads.lock().expect("server path lane tracker lock");
        let key = ServerPathLoadKey { session_id, path };
        if let Some(load) = loads.get_mut(&key) {
            load.remove(lane);
            if load.active_flows == 0 {
                loads.remove(&key);
            }
        }
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
        let mut loads = self.loads.lock().expect("server path lane tracker lock");
        for path in paths {
            if let Some(load) = loads.get_mut(&ServerPathLoadKey {
                session_id,
                path: *path,
            }) {
                load.remove(from);
                load.add(to);
            }
        }
    }

    fn snapshot(&self, session_id: SessionId, path: CarrierPathKey) -> ServerPathLaneLoad {
        self.loads
            .lock()
            .expect("server path lane tracker lock")
            .get(&ServerPathLoadKey { session_id, path })
            .copied()
            .unwrap_or_default()
    }

    fn session_snapshot(&self, session_id: SessionId) -> ServerPathLaneLoad {
        self.loads
            .lock()
            .expect("server path lane tracker lock")
            .iter()
            .filter(|(key, _)| key.session_id == session_id)
            .fold(ServerPathLaneLoad::default(), |mut total, (_, load)| {
                total.active_flows = total.active_flows.saturating_add(load.active_flows);
                total.active_latency_sensitive_flows = total
                    .active_latency_sensitive_flows
                    .saturating_add(load.active_latency_sensitive_flows);
                total
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
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
                .is_some_and(|target| target.is_active),
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
                .is_some_and(|target| target.is_active),
            "scheduler-active target must follow ordered_data_owner after migration"
        );
        assert!(
            targets
                .iter()
                .find(|target| target.key == active)
                .is_some_and(|target| !target.is_active),
            "output list tail must not override ordered_data_owner"
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
            ResponseStreamAttachOutcome::Attached
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
    fn response_service_failover_prefers_measured_survivor_over_output_tail() {
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
            Some(measured),
            "Service failover must be evidence-based; output-list tail is not an ownership signal"
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
        assert!(entry.delivery_rate_bps.is_some());
        assert!(entry.srtt_ms.is_some());
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
            sender_evidence: true,
            bulk_rate_proven: true,
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
    fn response_subflow_set_rejects_unproven_startup_after_credit_is_spent() {
        let (binding, service) = binding_for_underlay(UnderlayProtocol::Udp);
        let optional = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
        let input = SubflowAdmissionInput {
            key: optional,
            sender_evidence: true,
            bulk_rate_proven: false,
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
            PathAdmissionDecision::AdmitSubflow,
            "same-family startup Subflow owner windows are unique payload bytes that produce real delivery evidence"
        );

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
            "unproven Subflows return to Probe after bounded startup owner credit is spent"
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
    fn response_subflow_startup_credit_survives_ack_progress() {
        let (binding, service) = binding_for_underlay(UnderlayProtocol::Udp);
        let optional = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
        let input = SubflowAdmissionInput {
            key: optional,
            sender_evidence: true,
            bulk_rate_proven: false,
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
            "ordinary ACK progress must not recreate startup Subflow owner credit"
        );
    }

    #[test]
    fn response_subflow_startup_credit_resets_when_output_detaches() {
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
            sender_evidence: true,
            bulk_rate_proven: false,
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

        binding.detach(optional, &commands);

        assert!(
            binding.subflow_set_snapshot().is_none(),
            "carrier output membership changes invalidate the Subflow set"
        );
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
