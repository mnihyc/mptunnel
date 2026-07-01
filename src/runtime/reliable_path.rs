use super::bulk_admission::{
    BulkAdmissionCheck, BulkAdmissionRole, bulk_additional_admission_role,
    bulk_candidate_admission_suppression, bulk_candidate_admission_suppression_with_ordering_debt,
    bulk_service_horizon_payload_bytes,
};
use super::*;
use std::collections::{BTreeMap, HashMap};

mod registry;
mod response_admission;

pub(in crate::runtime) use registry::*;
use response_admission::*;

// Ownership boundary:
// This module owns carrier-neutral reliable stream bindings on the response
// side. It tracks which carrier path carried each product byte range, records
// ordering debt and stream-ACK release, and chooses among already joined carrier
// paths for response frames. It must not implement TCP framing, QUIC packet
// recovery, or target socket I/O; those belong to carrier and outbound modules.

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

    pub(super) async fn send_frame(&self, frame: Frame) -> Result<(), RuntimeError> {
        self.send_frame_tracked(frame).await.map(|_| ())
    }

    pub(super) async fn send_frame_with_lane(
        &self,
        frame: Frame,
        lane: FlowLane,
    ) -> Result<(), RuntimeError> {
        self.output
            .send_frame(self.stream_id, lane, frame)
            .await
            .map(|_| ())
    }

    pub(super) async fn send_repair_frame(
        &self,
        frame: Frame,
    ) -> Result<Option<CarrierPathKey>, RuntimeError> {
        self.output.send_repair_frame(self.stream_id, frame).await
    }

    pub(super) fn mark_repair_path_delivery_and_promote(&self, key: CarrierPathKey) -> bool {
        self.output.mark_repair_path_delivery_and_promote(key)
    }

    pub(super) async fn send_frame_tracked(
        &self,
        frame: Frame,
    ) -> Result<Option<CarrierPathKey>, RuntimeError> {
        self.output
            .send_frame(
                self.stream_id,
                tcp_path_effective_frame_lane(&frame, self.lane),
                frame,
            )
            .await
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

    pub(super) fn can_send_stream_data_extent(
        &self,
        lane: FlowLane,
        offset: u64,
        payload_bytes: usize,
    ) -> bool {
        self.output
            .can_send_stream_data_extent(lane, offset, payload_bytes)
    }

    pub(super) fn subscribe_output_updates(&self) -> Option<watch::Receiver<u64>> {
        self.output.subscribe_updates()
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

    pub(super) async fn close(&self) {
        self.output.close_stream(self.stream_id).await;
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
    pub(super) async fn send_frame(&self, frame: Frame) -> Result<(), RuntimeError> {
        self.output
            .send_frame(
                self.stream_id,
                tcp_path_effective_frame_lane(&frame, self.lane),
                frame,
            )
            .await
            .map(|_| ())
    }

    pub(super) async fn close(&self) {
        self.output.close_stream(self.stream_id).await;
    }
}

#[derive(Clone)]
pub(super) enum ReliablePathStreamOutput {
    /// A fixed carrier command pipe, used by client-side paths where the stream
    /// is bound to the currently opened carrier association.
    Fixed(TcpPathSessionCommandSender),
    /// A switchable response binding, used by server-side streams that may send
    /// later response bytes or repair ranges over another joined carrier path.
    Switchable(Arc<ResponseStreamBinding>),
}

impl ReliablePathStreamOutput {
    pub(super) async fn send_frame(
        &self,
        stream_id: StreamId,
        lane: FlowLane,
        frame: Frame,
    ) -> Result<Option<CarrierPathKey>, RuntimeError> {
        match self {
            Self::Fixed(commands) => {
                commands.send_frame(frame, lane).await?;
                Ok(None)
            }
            Self::Switchable(binding) => binding.send_frame(stream_id, lane, frame).await,
        }
    }

    pub(super) async fn send_repair_frame(
        &self,
        stream_id: StreamId,
        frame: Frame,
    ) -> Result<Option<CarrierPathKey>, RuntimeError> {
        match self {
            Self::Fixed(commands) => {
                commands.send_frame(frame, FlowLane::Latency).await?;
                Ok(None)
            }
            Self::Switchable(binding) => binding.send_repair_frame(stream_id, frame).await,
        }
    }

    pub(super) async fn close_stream(&self, stream_id: StreamId) {
        match self {
            Self::Fixed(commands) => {
                let _ = commands
                    .send_frame(Frame::StreamDetach { stream_id }, FlowLane::Control)
                    .await;
                let _ = commands
                    .send_control(TcpPathSessionCommand::CloseStream(stream_id))
                    .await;
            }
            Self::Switchable(binding) => binding.close_stream(stream_id).await,
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
            Self::Fixed(_) => None,
            Self::Switchable(binding) => binding.send_path_snapshot(lane, payload_bytes),
        }
    }

    pub(super) fn set_sender_queue_bytes(&self, bytes: usize) {
        if let Self::Switchable(binding) = self {
            binding.set_sender_queue_bytes(bytes);
        }
    }

    pub(super) fn can_send_stream_data_extent(
        &self,
        lane: FlowLane,
        offset: u64,
        payload_bytes: usize,
    ) -> bool {
        match self {
            Self::Fixed(_) => true,
            Self::Switchable(binding) => {
                binding.can_send_stream_data_extent(lane, offset, payload_bytes)
            }
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
        if let Self::Switchable(binding) = self {
            binding.release_acked_ranges(ranges);
        }
    }

    pub(super) fn has_multipath_repair_alternative(&self) -> bool {
        match self {
            Self::Fixed(_) => false,
            Self::Switchable(binding) => binding.has_multipath_repair_alternative(),
        }
    }
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
    flights: Mutex<BTreeMap<u64, Vec<CarrierPathFlight>>>,
    ack_ordering: Mutex<ResponseAckOrderingState>,
    lead_path: Mutex<Option<CarrierPathKey>>,
    version: watch::Sender<u64>,
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
                next_index: 0,
                entries: vec![ResponseStreamOutputEntry {
                    key,
                    commands,
                    bytes_in_flight: 0,
                    product_queue_bytes: 0,
                    delivery_samples: 0,
                    last_delivery_at: None,
                    validation_credit_bytes: 0,
                    path_metrics: None,
                }],
            }),
            flights: Mutex::new(BTreeMap::new()),
            ack_ordering: Mutex::new(ResponseAckOrderingState::default()),
            lead_path: Mutex::new(Some(key)),
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
        max_frame_payload_bytes: usize,
    ) {
        let previous_lane = {
            let mut current_lane = self.lane.lock().expect("server reliable stream lane lock");
            let previous = *current_lane;
            *current_lane = lane;
            previous
        };
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let key = CarrierPathKey { underlay, path_id };
        let mut was_active = false;
        let mut already_attached = false;
        let mut entry =
            if let Some(position) = outputs.entries.iter().position(|entry| entry.key == key) {
                was_active = position + 1 == outputs.entries.len();
                already_attached = true;
                let mut entry = outputs.entries.remove(position);
                entry.commands = commands;
                entry
            } else {
                ResponseStreamOutputEntry {
                    key,
                    commands,
                    bytes_in_flight: 0,
                    product_queue_bytes: 0,
                    delivery_samples: 0,
                    last_delivery_at: None,
                    validation_credit_bytes: if role == StreamOpenRole::Validation {
                        max_frame_payload_bytes.saturating_mul(2) as u64
                    } else {
                        0
                    },
                    path_metrics: None,
                }
            };
        if role == StreamOpenRole::Validation {
            let validation_limit = max_frame_payload_bytes.saturating_mul(2) as u64;
            entry.validation_credit_bytes = entry.validation_credit_bytes.max(validation_limit);
        } else {
            entry.validation_credit_bytes = 0;
        }
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
        outputs.next_index %= outputs.entries.len().max(1);
        drop(outputs);
        if previous_lane != lane {
            self.lane_tracker
                .change_lanes(self.session_id, &attached_keys, previous_lane, lane);
        }
        if !already_attached {
            self.lane_tracker.attach(self.session_id, key, lane);
        }
        if server_stream_open_role_promotes_data_path(role) {
            *self
                .lead_path
                .lock()
                .expect("server reliable stream lead path lock") = Some(key);
        }
        self.notify_update();
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

    pub(super) fn can_send_stream_data_extent(
        &self,
        lane: FlowLane,
        offset: u64,
        payload_bytes: usize,
    ) -> bool {
        if !relay_lane_is_bulk(lane) || payload_bytes == 0 {
            return true;
        }
        let lower_flights = self.lower_flights_before_offset(offset);
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        outputs.bulk_send_ready(
            self.session_id,
            &self.lane_tracker,
            lane,
            payload_bytes,
            self.mux_limits,
            &lower_flights,
            *self
                .lead_path
                .lock()
                .expect("server reliable stream lead path lock"),
        )
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

    fn detach(&self, key: CarrierPathKey, commands: &TcpPathSessionCommandSender) {
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
            outputs.next_index %= outputs.entries.len().max(1);
            let still_attached = outputs
                .entries
                .iter()
                .any(|entry| Some(entry.key) == *self.lead_path.lock().expect("lead path lock"));
            drop(outputs);
            if !still_attached {
                *self
                    .lead_path
                    .lock()
                    .expect("server reliable stream lead path lock") = None;
            }
            self.lane_tracker.detach(self.session_id, key, lane);
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
        outputs.entries[position].validation_credit_bytes = 0;
        if !was_active {
            let entry = outputs.entries.remove(position);
            outputs.entries.push(entry);
            outputs.next_index %= outputs.entries.len().max(1);
        }
        drop(outputs);
        self.notify_update();
        !was_active
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) async fn send_frame(
        &self,
        stream_id: StreamId,
        _lane: FlowLane,
        frame: Frame,
    ) -> Result<Option<CarrierPathKey>, RuntimeError> {
        let mut updates = self.version.subscribe();
        loop {
            let stream_lane = self.lane();
            let lower_flights = if relay_frame_is_bulk_stream_data(&frame, stream_lane) {
                self.lower_flights_before_frame(&frame)
            } else {
                Vec::new()
            };
            let selected = {
                let mut outputs = self
                    .outputs
                    .lock()
                    .expect("server reliable stream binding lock");
                if server_frame_prefers_current_data_path(&frame, stream_lane) {
                    outputs.data_commands().map(CarrierPathSendChoice::Single)
                } else if relay_frame_is_bulk_stream_data(&frame, stream_lane) {
                    outputs
                        .bulk_commands(
                            self.session_id,
                            &self.lane_tracker,
                            stream_lane,
                            reliable_stream_frame_payload_bytes(&frame),
                            self.mux_limits,
                            &lower_flights,
                            *self
                                .lead_path
                                .lock()
                                .expect("server reliable stream lead path lock"),
                        )
                        .map(CarrierPathSendChoice::Bulk)
                } else {
                    outputs.next_commands().map(CarrierPathSendChoice::Single)
                }
            };
            if let Some(choice) = selected {
                let send_lane = tcp_path_effective_frame_lane(&frame, stream_lane);
                let primary = match &choice {
                    CarrierPathSendChoice::Single(target) => target,
                    CarrierPathSendChoice::Bulk(choice) => &choice.primary,
                };
                tokio::select! {
                    result = primary.commands.send_frame(frame.clone(), send_lane) => {
                        match result {
                            Ok(()) => {
                                let mut primary_stream_ack_proves_path = true;
                                if let CarrierPathSendChoice::Bulk(choice) = &choice
                                    && let Some(duplicate) = &choice.validation_duplicate
                                {
                                    match duplicate.commands.try_send_frame(frame.clone(), send_lane) {
                                        Ok(true) => {
                                            primary_stream_ack_proves_path = false;
                                            record_server_sender_decision(
                                                self.session_id,
                                                stream_id,
                                                duplicate.key,
                                                &frame,
                                                send_lane,
                                                "validation_duplicate",
                                            );
                                            self.record_flight(
                                                duplicate.key,
                                                &frame,
                                                false,
                                            );
                                        }
                                        Ok(false) => {
                                            #[cfg(feature = "lab-diagnostics")]
                                            lab_diagnostic(
                                                "server_bulk_output_validation_duplicate_skipped",
                                                format_args!(
                                                    "path_underlay={:?} path_id={} reason=queue_full",
                                                    duplicate.key.underlay,
                                                    duplicate.key.path_id.0,
                                                ),
                                            );
                                        }
                                        Err(_) => self.detach(duplicate.key, &duplicate.commands),
                                    }
                                }
                                if relay_frame_is_bulk_stream_data(&frame, stream_lane) {
                                    *self
                                        .lead_path
                                        .lock()
                                        .expect("server reliable stream lead path lock") =
                                        Some(primary.key);
                                    self.record_flight(
                                        primary.key,
                                        &frame,
                                        primary_stream_ack_proves_path,
                                    );
                                }
                                record_server_sender_decision(
                                    self.session_id,
                                    stream_id,
                                    primary.key,
                                    &frame,
                                    send_lane,
                                    "primary",
                                );
                                return Ok(Some(primary.key));
                            }
                            Err(_) => self.detach(primary.key, &primary.commands),
                        }
                    }
                    changed = updates.changed() => {
                        changed.map_err(|_| RuntimeError::TcpPathSessionClosed)?;
                    }
                }
            } else {
                updates
                    .changed()
                    .await
                    .map_err(|_| RuntimeError::TcpPathSessionClosed)?;
            }
        }
    }

    pub(super) async fn send_repair_frame(
        &self,
        stream_id: StreamId,
        frame: Frame,
    ) -> Result<Option<CarrierPathKey>, RuntimeError> {
        let mut updates = self.version.subscribe();
        loop {
            let avoid_keys = self.flight_keys_overlapping_frame(&frame);
            let selected = {
                let outputs = self
                    .outputs
                    .lock()
                    .expect("server reliable stream binding lock");
                outputs.repair_commands(
                    self.session_id,
                    &self.lane_tracker,
                    &avoid_keys,
                    reliable_stream_frame_payload_bytes(&frame),
                    self.mux_limits,
                )
            };
            if let Some(target) = selected {
                tokio::select! {
                    result = target.commands.send_frame(frame.clone(), FlowLane::Latency) => {
                        match result {
                            Ok(()) => {
                                self.record_flight(target.key, &frame, false);
                                record_server_sender_decision(
                                    self.session_id,
                                    stream_id,
                                    target.key,
                                    &frame,
                                    FlowLane::Latency,
                                    "tail_repair",
                                );
                                #[cfg(feature = "lab-diagnostics")]
                                lab_diagnostic(
                                    "repair",
                                    format_args!(
                                        "stream_id={} path_underlay={:?} path_id={} cause=tail_stall avoided_paths={}",
                                        stream_id.0,
                                        target.key.underlay,
                                        target.key.path_id.0,
                                        avoid_keys.len(),
                                    ),
                                );
                                return Ok(Some(target.key));
                            }
                            Err(_) => self.detach(target.key, &target.commands),
                        }
                    }
                    changed = updates.changed() => {
                        changed.map_err(|_| RuntimeError::TcpPathSessionClosed)?;
                    }
                }
            } else {
                updates
                    .changed()
                    .await
                    .map_err(|_| RuntimeError::TcpPathSessionClosed)?;
            }
        }
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
        for (_, flight) in released {
            if let Some(entry) = outputs
                .entries
                .iter_mut()
                .find(|entry| entry.key == flight.key)
            {
                entry.bytes_in_flight = entry.bytes_in_flight.saturating_sub(flight.bytes as u64);
                changed = true;
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

    fn record_flight(&self, key: CarrierPathKey, frame: &Frame, stream_ack_proves_path: bool) {
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
                entry.validation_credit_bytes =
                    entry.validation_credit_bytes.saturating_sub(bytes as u64);
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
                stream_ack_proves_path,
            });
    }

    fn lower_flights_before_frame(&self, frame: &Frame) -> Vec<CarrierPathFlightDebt> {
        let Some((offset, _, _)) = reliable_stream_frame_extent(frame) else {
            return Vec::new();
        };
        self.lower_flights_before_offset(offset)
    }

    fn flight_keys_overlapping_frame(&self, frame: &Frame) -> Vec<CarrierPathKey> {
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

    fn lower_flights_before_offset(&self, offset: u64) -> Vec<CarrierPathFlightDebt> {
        let mut debts = BTreeMap::<u64, CarrierPathFlightDebt>::new();
        {
            let flights = self
                .flights
                .lock()
                .expect("server reliable stream flight lock");
            for (flight_offset, path_flights) in flights.range(..offset) {
                if let Some(latest) = path_flights.last() {
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
                if let Some(latest) = holes.last() {
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
            self.notify_update();
        }
    }

    #[cfg(test)]
    pub(super) fn update_path_metrics_for_test(
        &self,
        underlay: UnderlayProtocol,
        path_id: PathId,
        metrics: PathMetrics,
    ) {
        self.update_path_metrics(
            CarrierPathKey { underlay, path_id },
            metrics,
            ServerPathMetricsSource::LocalSender,
        );
    }

    #[cfg(test)]
    pub(super) fn update_peer_path_metrics_for_test(
        &self,
        underlay: UnderlayProtocol,
        path_id: PathId,
        metrics: PathMetrics,
    ) {
        self.update_path_metrics(
            CarrierPathKey { underlay, path_id },
            metrics,
            ServerPathMetricsSource::PeerHint,
        );
    }

    #[cfg(test)]
    pub(super) fn output_snapshot_for_test(
        &self,
        underlay: UnderlayProtocol,
        path_id: PathId,
    ) -> Option<PathSnapshot> {
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        outputs
            .entries
            .iter()
            .find(|entry| entry.key == CarrierPathKey { underlay, path_id })
            .map(|entry| {
                server_bulk_output_snapshot(
                    entry,
                    self.session_id,
                    FlowLane::Throughput,
                    &self.lane_tracker,
                    self.mux_limits,
                    Instant::now(),
                )
            })
    }

    #[cfg(test)]
    pub(super) fn bulk_choice_key_for_test(&self, payload_bytes: usize) -> Option<CarrierPathKey> {
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        outputs
            .bulk_commands(
                self.session_id,
                &self.lane_tracker,
                FlowLane::Throughput,
                payload_bytes,
                self.mux_limits,
                &[],
                *self
                    .lead_path
                    .lock()
                    .expect("server reliable stream lead path lock"),
            )
            .map(|choice| choice.primary.key)
    }

    #[cfg(test)]
    pub(super) fn output_has_sender_evidence_for_test(
        &self,
        underlay: UnderlayProtocol,
        path_id: PathId,
    ) -> bool {
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        outputs
            .entries
            .iter()
            .find(|entry| entry.key == CarrierPathKey { underlay, path_id })
            .is_some_and(server_output_has_sender_evidence)
    }

    #[cfg(test)]
    pub(super) fn output_eta_ms_for_test(
        &self,
        underlay: UnderlayProtocol,
        path_id: PathId,
        payload_bytes: usize,
    ) -> Option<f64> {
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let active_key = outputs.entries.last().map(|entry| entry.key);
        outputs
            .entries
            .iter()
            .find(|entry| entry.key == CarrierPathKey { underlay, path_id })
            .map(|entry| {
                server_bulk_output_eta_ms(
                    entry.key,
                    server_bulk_output_snapshot(
                        entry,
                        self.session_id,
                        FlowLane::Throughput,
                        &self.lane_tracker,
                        self.mux_limits,
                        Instant::now(),
                    ),
                    active_key,
                    FlowLane::Throughput,
                    payload_bytes,
                    self.mux_limits,
                )
            })
    }

    #[cfg(test)]
    pub(super) fn record_flight_for_test(
        &self,
        underlay: UnderlayProtocol,
        path_id: PathId,
        offset: u64,
        payload_bytes: usize,
        stream_ack_proves_path: bool,
    ) {
        self.record_flight(
            CarrierPathKey { underlay, path_id },
            &Frame::StreamData {
                stream_id: StreamId(7),
                offset,
                flags: StreamFlags::NONE,
                payload: Bytes::from(vec![0u8; payload_bytes]),
            },
            stream_ack_proves_path,
        );
    }

    #[cfg(test)]
    pub(super) fn lower_flight_debt_keys_for_test(
        &self,
        offset: u64,
    ) -> Vec<(UnderlayProtocol, PathId, u64)> {
        self.lower_flights_before_offset(offset)
            .into_iter()
            .map(|debt| (debt.key.underlay, debt.key.path_id, debt.bytes))
            .collect()
    }

    #[cfg(test)]
    pub(super) fn ack_ordering_state_for_test(&self) -> (u64, u64) {
        let ordering = self
            .ack_ordering
            .lock()
            .expect("server response ACK ordering lock");
        (ordering.contiguous_frontier, ordering.acked_hole_bytes())
    }
}

pub(super) fn server_frame_prefers_current_data_path(frame: &Frame, lane: FlowLane) -> bool {
    matches!(frame, Frame::StreamData { .. }) && !relay_lane_is_bulk(lane)
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
