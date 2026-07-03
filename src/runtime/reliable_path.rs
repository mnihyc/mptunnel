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

    pub(super) fn mark_repair_path_delivery_and_promote(&self, key: CarrierPathKey) -> bool {
        self.output.mark_repair_path_delivery_and_promote(key)
    }

    pub(super) async fn recv_frame(&mut self) -> Result<Frame, RuntimeError> {
        match self.frames.recv().await {
            Some(Ok(frame)) => Ok(frame),
            Some(Err(err)) => Err(err),
            None => Err(RuntimeError::TcpPathSessionClosed),
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

    pub(super) fn release_acked_ranges(&self, ranges: &[OffsetRange]) {
        self.output.release_acked_ranges(ranges);
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
    commands: TcpPathSessionCommandSender,
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

impl FixedReliablePathOutput {
    #[cfg(test)]
    pub(super) fn new(
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: TcpPathSessionCommandSender,
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
        commands: TcpPathSessionCommandSender,
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

    pub(super) fn commands(&self) -> &TcpPathSessionCommandSender {
        &self.commands
    }

    fn send_path_snapshot(&self) -> PathSnapshot {
        let model = self.model.lock().expect("fixed reliable path model lock");
        let prior_rate_bps = self.startup.delivery_rate_bps.max(1.0);
        let delivery_rate_bps = match self.key.underlay {
            UnderlayProtocol::Tcp => model
                .delivery_rate_bps
                .map(|rate| rate.max(prior_rate_bps))
                .unwrap_or(prior_rate_bps),
            UnderlayProtocol::Udp => model.delivery_rate_bps.unwrap_or(prior_rate_bps),
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
            / f64::from(QUIC_INITIAL_WINDOW_PACKETS as u32))
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

    pub(super) fn record_flight(&self, frame: &Frame, stream_ack_proves_path: bool) {
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
                stream_ack_proves_path,
                owns_ordering_frontier: true,
            });
    }

    fn release_acked_ranges(&self, ranges: &[OffsetRange]) {
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
                for flight in path_flights {
                    model.bytes_in_flight =
                        model.bytes_in_flight.saturating_sub(flight.bytes as u64);
                    if flight.stream_ack_proves_path {
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
        commands: TcpPathSessionCommandSender,
        mux_limits: MuxLimits,
    ) -> Self {
        Self::Fixed(FixedReliablePathOutput::new(
            underlay, path_id, commands, mux_limits,
        ))
    }

    pub(super) fn fixed_with_snapshot(
        startup: PathSnapshot,
        commands: TcpPathSessionCommandSender,
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
                .send_control(TcpPathSessionCommand::SendFrame(Frame::StreamDetach {
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
                    .send_control(TcpPathSessionCommand::CloseStream(stream_id))
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

    pub(super) fn mark_repair_path_delivery_and_promote(&self, key: CarrierPathKey) -> bool {
        match self {
            Self::Fixed(_) => false,
            Self::Switchable(binding) => binding.mark_repair_path_delivery_and_promote(key),
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

    pub(super) fn release_acked_ranges(&self, ranges: &[OffsetRange]) {
        match self {
            Self::Fixed(fixed) => fixed.release_acked_ranges(ranges),
            Self::Switchable(binding) => binding.release_acked_ranges(ranges),
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
    ordinary_lead: Mutex<Option<CarrierPathKey>>,
    flights: Mutex<BTreeMap<u64, Vec<CarrierPathFlight>>>,
    ack_ordering: Mutex<ResponseAckOrderingState>,
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
        commands: TcpPathSessionCommandSender,
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
        commands: TcpPathSessionCommandSender,
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
        commands: TcpPathSessionCommandSender,
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
                    path_metrics: None,
                }],
            }),
            ordinary_lead: Mutex::new(Some(key)),
            flights: Mutex::new(BTreeMap::new()),
            ack_ordering: Mutex::new(ResponseAckOrderingState::default()),
            version,
        })
    }

    pub(super) fn attach(
        &self,
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: TcpPathSessionCommandSender,
        lane: FlowLane,
        role: StreamOpenRole,
        _max_frame_payload_bytes: usize,
    ) -> ResponseStreamAttachOutcome {
        let previous_lane = *self.lane.lock().expect("server reliable stream lane lock");
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
                lab_diagnostic(
                    "server_stream_output_attach",
                    format_args!(
                        "session_id={} path_underlay={:?} path_id={} role={:?} lane={:?} result=duplicate_live same_channel={}",
                        self.session_id.0, underlay, path_id.0, role, lane, same_channel,
                    ),
                );
                if role == StreamOpenRole::Active || same_channel {
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
                path_metrics: None,
            }
        };
        let promote_or_keep_active_slot = server_stream_open_role_promotes_data_path(role)
            || was_active
            || outputs.entries.is_empty();
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
        if server_stream_open_role_promotes_data_path(role) && self.can_migrate_ordinary_lead() {
            self.set_ordinary_lead(key);
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
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        outputs.read_backpressure_snapshot(
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

    pub(super) fn detach(&self, key: CarrierPathKey, commands: &TcpPathSessionCommandSender) {
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
            drop(outputs);
            self.lane_tracker.detach(self.session_id, key, lane);
            self.clear_ordinary_lead_if(key);
            self.notify_update();
        }
    }

    fn mark_repair_path_delivery_and_promote(&self, key: CarrierPathKey) -> bool {
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let Some(position) = outputs.entries.iter().position(|entry| entry.key == key) else {
            return false;
        };
        let was_active = position + 1 == outputs.entries.len();
        let now = Instant::now();
        outputs.entries[position].delivery_samples =
            outputs.entries[position].delivery_samples.saturating_add(1);
        outputs.entries[position].last_delivery_at = Some(now);
        if !was_active {
            let entry = outputs.entries.remove(position);
            outputs.entries.push(entry);
        }
        drop(outputs);
        self.notify_update();
        !was_active
    }

    pub(super) fn release_acked_ranges(&self, ranges: &[OffsetRange]) {
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
                .apply_ack(ranges, &[]);
            if ordering_update.changed {
                self.notify_update();
            }
            return;
        }
        let mut released = Vec::new();
        for offset in acked_offsets {
            if let Some(path_flights) = flights.remove(&offset) {
                released.extend(path_flights.into_iter().map(|flight| (offset, flight)));
            }
        }
        drop(flights);

        let ordering_update = {
            let mut ordering = self
                .ack_ordering
                .lock()
                .expect("server response ACK ordering lock");
            ordering.apply_ack(ranges, &released)
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
        for (_, flight) in released {
            if let Some(entry) = outputs
                .entries
                .iter_mut()
                .find(|entry| entry.key == flight.key)
            {
                entry.bytes_in_flight = entry.bytes_in_flight.saturating_sub(flight.bytes as u64);
                if flight.stream_ack_proves_path {
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
            if !hole.stream_ack_proves_path {
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
            self.notify_update();
        }
    }

    pub(super) fn ordinary_lead(&self) -> Option<CarrierPathKey> {
        *self
            .ordinary_lead
            .lock()
            .expect("server reliable stream ordinary lead lock")
    }

    pub(super) fn set_ordinary_lead(&self, key: CarrierPathKey) {
        let mut lead = self
            .ordinary_lead
            .lock()
            .expect("server reliable stream ordinary lead lock");
        if *lead != Some(key) {
            *lead = Some(key);
            drop(lead);
            self.notify_update();
        }
    }

    fn clear_ordinary_lead_if(&self, key: CarrierPathKey) {
        let mut lead = self
            .ordinary_lead
            .lock()
            .expect("server reliable stream ordinary lead lock");
        if *lead == Some(key) {
            *lead = None;
        }
    }

    fn can_migrate_ordinary_lead(&self) -> bool {
        self.flights
            .lock()
            .expect("server reliable stream flight lock")
            .is_empty()
            && self
                .ack_ordering
                .lock()
                .expect("server response ACK ordering lock")
                .acked_holes
                .is_empty()
    }

    pub(super) fn record_flight(
        &self,
        key: CarrierPathKey,
        frame: &Frame,
        stream_ack_proves_path: bool,
    ) {
        self.record_flight_with_ordering_owner(key, frame, stream_ack_proves_path, true)
    }

    pub(super) fn record_flight_with_ordering_owner(
        &self,
        key: CarrierPathKey,
        frame: &Frame,
        stream_ack_proves_path: bool,
        owns_ordering_frontier: bool,
    ) {
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
                stream_ack_proves_path,
                owns_ordering_frontier,
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
            let flights = self
                .flights
                .lock()
                .expect("server reliable stream flight lock");
            for (flight_offset, path_flights) in flights.range(..offset) {
                if let Some(latest) = response_latest_ordering_flight(path_flights) {
                    debts.insert(
                        *flight_offset,
                        CarrierPathFlightDebt {
                            key: latest.key,
                            bytes: latest.bytes as u64,
                        },
                    );
                }
            }
        }
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
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let active_key = outputs.entries.last().map(|entry| entry.key);
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
                .send_control(TcpPathSessionCommand::CloseStream(stream_id))
                .await;
            self.lane_tracker.detach(self.session_id, entry.key, lane);
        }
        if let Ok(mut lead) = self.ordinary_lead.lock() {
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
        if let Ok(mut lead) = self.ordinary_lead.lock() {
            *lead = None;
        }
    }

    fn notify_update(&self) {
        let current = *self.version.borrow();
        let _ = self.version.send(current.wrapping_add(1));
    }

    fn update_path_metrics(
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
                entry.path_metrics = Some(ServerPathMetricsEntry { metrics, source });
                changed = true;
            }
        }
        drop(outputs);
        if changed {
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "server_response_path_metrics_attached",
                format_args!(
                    "session_id={} underlay={:?} path_id={} source={:?} direction={:?} rate_mbps={:.3} pacing_mbps={:.3} srtt_ms={:.3} confidence_ppm={} app_limited={} ack_sample={} sample_count={}",
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
                ),
            );
            self.notify_update();
        }
    }
}

fn server_stream_open_role_promotes_data_path(role: StreamOpenRole) -> bool {
    role == StreamOpenRole::Active
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
        let (commands, _receivers) = tcp_path_session_command_channels(8);
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
        let (validation_commands, _validation_receivers) = tcp_path_session_command_channels(8);

        assert_eq!(binding.ordinary_lead(), Some(active));
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
            binding.ordinary_lead(),
            Some(active),
            "validation attachment opens a carrier output but is not scheduler ownership"
        );
    }

    #[test]
    fn response_duplicate_active_attach_is_idempotent_for_live_output() {
        let (binding, active) = binding_for_underlay(UnderlayProtocol::Udp);
        let validation = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let (validation_commands, _validation_receivers) = tcp_path_session_command_channels(8);
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
        let (duplicate_commands, _duplicate_receivers) = tcp_path_session_command_channels(8);

        assert_eq!(
            binding.attach(
                validation.underlay,
                validation.path_id,
                duplicate_commands,
                FlowLane::Throughput,
                StreamOpenRole::Active,
                reliable_relay_buffer_len(MuxLimits::default()),
            ),
            ResponseStreamAttachOutcome::Attached
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
        assert_eq!(binding.ordinary_lead(), Some(active));
    }

    #[test]
    fn udp_stream_ack_releases_product_flight_without_seeding_carrier_rate() {
        let (binding, key) = binding_for_underlay(UnderlayProtocol::Udp);
        let frame = stream_data_frame(MIN_RATE_SAMPLE_BYTES as usize);

        binding.record_flight(key, &frame, true);
        std::thread::sleep(Duration::from_millis(1));
        binding.release_acked_ranges(&[OffsetRange {
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

        binding.record_flight(key, &frame, true);
        std::thread::sleep(Duration::from_millis(1));
        binding.release_acked_ranges(&[OffsetRange {
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
        let (duplicate_commands, _duplicate_receivers) = tcp_path_session_command_channels(8);
        binding.attach(
            duplicate.underlay,
            duplicate.path_id,
            duplicate_commands,
            FlowLane::Throughput,
            StreamOpenRole::Repair,
            reliable_relay_buffer_len(MuxLimits::default()),
        );
        let frame = stream_data_frame_at(0, 4096);

        binding.record_flight(owner, &frame, true);
        binding.record_flight_with_ordering_owner(duplicate, &frame, false, false);

        let lower = binding.lower_flights_before_offset(4096);
        assert_eq!(lower.len(), 1);
        assert_eq!(lower[0].key, owner);
        assert_eq!(lower[0].bytes, 4096);

        binding.release_acked_ranges(&[OffsetRange {
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
        assert_eq!(owner_entry.delivery_samples, 1);
        assert_eq!(
            duplicate_entry.delivery_samples, 0,
            "duplicate validation STREAM_ACK must not become response bulk evidence"
        );
    }

    #[test]
    fn response_acked_hole_debt_counts_unique_ordering_owner_only() {
        let (binding, owner) = binding_for_underlay(UnderlayProtocol::Udp);
        let duplicate = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let (duplicate_commands, _duplicate_receivers) = tcp_path_session_command_channels(8);
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
        binding.record_flight(owner, &lower_missing, true);
        binding.record_flight(owner, &later, true);
        binding.record_flight_with_ordering_owner(duplicate, &later, false, false);

        binding.release_acked_ranges(&[OffsetRange {
            start: 1024,
            end: 5120,
        }]);

        let lower = binding.lower_flights_before_offset(5120);
        assert_eq!(lower.len(), 2);
        assert_eq!(lower[0].key, owner);
        assert_eq!(lower[0].bytes, 1024);
        assert_eq!(lower[1].key, owner);
        assert_eq!(
            lower[1].bytes, 4096,
            "acked hole debt must not double-count duplicate validation copies"
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
            };
            binding.update_path_metrics(key, metrics, ServerPathMetricsSource::PeerHint);

            let snapshot = binding
                .send_path_snapshot(FlowLane::Throughput, MIN_RATE_SAMPLE_BYTES as usize)
                .expect("peer metrics remain validation hints");
            assert_eq!(snapshot.delivery_rate_bps, default_path_rate_bps(underlay));
            assert_eq!(snapshot.pacing_rate_bps, snapshot.delivery_rate_bps);
            assert_eq!(snapshot.inflight_limit_bytes, 0);
            assert_eq!(snapshot.bytes_in_flight, 0);
            assert!(snapshot.app_limited);
        }
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
            data_sample_count: QUIC_INITIAL_WINDOW_PACKETS as u32,
        };
        binding.update_path_metrics(key, metrics, ServerPathMetricsSource::LocalSender);

        let before_ack = binding
            .send_path_snapshot(FlowLane::Throughput, MIN_RATE_SAMPLE_BYTES as usize)
            .expect("path metrics seed response path snapshot");
        assert_eq!(before_ack.delivery_rate_bps, 500_000_000.0);

        let frame = stream_data_frame(MIN_RATE_SAMPLE_BYTES as usize);
        binding.record_flight(key, &frame, true);
        std::thread::sleep(Duration::from_millis(20));
        binding.release_acked_ranges(&[OffsetRange {
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
}
