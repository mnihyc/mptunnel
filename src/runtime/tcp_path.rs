use super::bulk_admission::{
    BulkAdmissionCheck, BulkAdmissionRole, bulk_additional_admission_role,
    bulk_candidate_admission_suppression, bulk_candidate_admission_suppression_with_ordering_debt,
    bulk_service_horizon_payload_bytes,
};
use super::*;
use std::collections::{BTreeMap, HashMap};

pub(super) struct TcpPathStream {
    pub(super) stream_id: StreamId,
    pub(super) max_offset: u64,
    pub(super) lane: FlowLane,
    pub(super) underlay: UnderlayProtocol,
    pub(super) max_frame_payload_bytes: usize,
    pub(super) output: TcpPathStreamOutput,
    pub(super) frames: mpsc::Receiver<Result<Frame, RuntimeError>>,
}

impl TcpPathStream {
    pub(super) fn into_handle_and_frames(
        self,
    ) -> (
        TcpPathStreamHandle,
        mpsc::Receiver<Result<Frame, RuntimeError>>,
    ) {
        (
            TcpPathStreamHandle {
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
    ) -> Result<Option<ServerTcpPathKey>, RuntimeError> {
        self.output.send_repair_frame(self.stream_id, frame).await
    }

    pub(super) fn mark_repair_path_delivery_and_promote(&self, key: ServerTcpPathKey) -> bool {
        self.output.mark_repair_path_delivery_and_promote(key)
    }

    pub(super) async fn send_frame_tracked(
        &self,
        frame: Frame,
    ) -> Result<Option<ServerTcpPathKey>, RuntimeError> {
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

pub(super) struct TcpPathStreamHandle {
    pub(super) stream_id: StreamId,
    pub(super) max_offset: u64,
    pub(super) lane: FlowLane,
    pub(super) underlay: UnderlayProtocol,
    pub(super) max_frame_payload_bytes: usize,
    pub(super) output: TcpPathStreamOutput,
}

impl TcpPathStreamHandle {
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
pub(super) enum TcpPathStreamOutput {
    Fixed(TcpPathSessionCommandSender),
    Switchable(Arc<ServerTcpStreamBinding>),
}

impl TcpPathStreamOutput {
    pub(super) async fn send_frame(
        &self,
        stream_id: StreamId,
        lane: FlowLane,
        frame: Frame,
    ) -> Result<Option<ServerTcpPathKey>, RuntimeError> {
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
    ) -> Result<Option<ServerTcpPathKey>, RuntimeError> {
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

    pub(super) fn mark_repair_path_delivery_and_promote(&self, key: ServerTcpPathKey) -> bool {
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

pub(super) struct ServerTcpStreamBinding {
    session_id: SessionId,
    lane: Mutex<FlowLane>,
    mux_limits: MuxLimits,
    lane_tracker: Arc<ServerPathLaneTracker>,
    outputs: Mutex<ServerTcpStreamOutputs>,
    flights: Mutex<BTreeMap<u64, Vec<ServerTcpPathFlight>>>,
    ack_ordering: Mutex<ServerTcpAckOrderingState>,
    version: watch::Sender<u64>,
}

impl ServerTcpStreamBinding {
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
        let key = ServerTcpPathKey { underlay, path_id };
        lane_tracker.attach(session_id, key, lane);
        Arc::new(Self {
            session_id,
            lane: Mutex::new(lane),
            mux_limits,
            lane_tracker,
            outputs: Mutex::new(ServerTcpStreamOutputs {
                next_index: 0,
                entries: vec![ServerTcpStreamOutputEntry {
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
            ack_ordering: Mutex::new(ServerTcpAckOrderingState::default()),
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
            let mut current_lane = self.lane.lock().expect("server TCP stream lane lock");
            let previous = *current_lane;
            *current_lane = lane;
            previous
        };
        let mut outputs = self.outputs.lock().expect("server TCP stream binding lock");
        let key = ServerTcpPathKey { underlay, path_id };
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
                ServerTcpStreamOutputEntry {
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
        self.notify_update();
    }

    pub(super) fn lane(&self) -> FlowLane {
        *self.lane.lock().expect("server TCP stream lane lock")
    }

    pub(super) fn subscribe_updates(&self) -> watch::Receiver<u64> {
        self.version.subscribe()
    }

    pub(super) fn send_path_snapshot(
        &self,
        lane: FlowLane,
        payload_bytes: usize,
    ) -> Option<PathSnapshot> {
        let outputs = self.outputs.lock().expect("server TCP stream binding lock");
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
        let mut outputs = self.outputs.lock().expect("server TCP stream binding lock");
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
        let outputs = self.outputs.lock().expect("server TCP stream binding lock");
        outputs.bulk_send_ready(
            self.session_id,
            &self.lane_tracker,
            lane,
            payload_bytes,
            self.mux_limits,
            &lower_flights,
        )
    }

    pub(super) fn set_lane(&self, lane: FlowLane) {
        let previous_lane = {
            let mut current_lane = self.lane.lock().expect("server TCP stream lane lock");
            let previous = *current_lane;
            *current_lane = lane;
            previous
        };
        if previous_lane != lane {
            let outputs = self.outputs.lock().expect("server TCP stream binding lock");
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
            .expect("server TCP stream binding lock")
            .entries
            .len()
            > 1
    }

    fn detach(&self, key: ServerTcpPathKey, commands: &TcpPathSessionCommandSender) {
        let lane = self.lane();
        let mut outputs = self.outputs.lock().expect("server TCP stream binding lock");
        let before = outputs.entries.len();
        outputs
            .entries
            .retain(|entry| entry.key != key || !entry.commands.same_channel(commands));
        if outputs.entries.len() != before {
            outputs.next_index %= outputs.entries.len().max(1);
            drop(outputs);
            self.lane_tracker.detach(self.session_id, key, lane);
            self.notify_update();
        }
    }

    fn mark_repair_path_delivery_and_promote(&self, key: ServerTcpPathKey) -> bool {
        let mut outputs = self.outputs.lock().expect("server TCP stream binding lock");
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
    ) -> Result<Option<ServerTcpPathKey>, RuntimeError> {
        let mut updates = self.version.subscribe();
        loop {
            let stream_lane = self.lane();
            let lower_flights = if relay_frame_is_bulk_stream_data(&frame, stream_lane) {
                self.lower_flights_before_frame(&frame)
            } else {
                Vec::new()
            };
            let selected = {
                let mut outputs = self.outputs.lock().expect("server TCP stream binding lock");
                if server_frame_prefers_current_data_path(&frame, stream_lane) {
                    outputs.data_commands().map(ServerTcpPathSendChoice::Single)
                } else if relay_frame_is_bulk_stream_data(&frame, stream_lane) {
                    outputs
                        .bulk_commands(
                            self.session_id,
                            &self.lane_tracker,
                            stream_lane,
                            reliable_stream_frame_payload_bytes(&frame),
                            self.mux_limits,
                            &lower_flights,
                        )
                        .map(ServerTcpPathSendChoice::Bulk)
                } else {
                    outputs.next_commands().map(ServerTcpPathSendChoice::Single)
                }
            };
            if let Some(choice) = selected {
                let send_lane = tcp_path_effective_frame_lane(&frame, stream_lane);
                let primary = match &choice {
                    ServerTcpPathSendChoice::Single(target) => target,
                    ServerTcpPathSendChoice::Bulk(choice) => &choice.primary,
                };
                tokio::select! {
                    result = primary.commands.send_frame(frame.clone(), send_lane) => {
                        match result {
                            Ok(()) => {
                                let mut primary_stream_ack_proves_path = true;
                                if let ServerTcpPathSendChoice::Bulk(choice) = &choice
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
    ) -> Result<Option<ServerTcpPathKey>, RuntimeError> {
        let mut updates = self.version.subscribe();
        loop {
            let avoid_keys = self.flight_keys_overlapping_frame(&frame);
            let selected = {
                let outputs = self.outputs.lock().expect("server TCP stream binding lock");
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
        let mut flights = self.flights.lock().expect("server TCP stream flight lock");
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
                .expect("server TCP ACK ordering lock")
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
                .expect("server TCP ACK ordering lock");
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

        let mut outputs = self.outputs.lock().expect("server TCP stream binding lock");
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

    fn record_flight(&self, key: ServerTcpPathKey, frame: &Frame, stream_ack_proves_path: bool) {
        let Some((offset, end, bytes)) = reliable_stream_frame_extent(frame) else {
            return;
        };
        {
            let mut outputs = self.outputs.lock().expect("server TCP stream binding lock");
            if let Some(entry) = outputs.entries.iter_mut().find(|entry| entry.key == key) {
                entry.bytes_in_flight = entry.bytes_in_flight.saturating_add(bytes as u64);
                entry.validation_credit_bytes =
                    entry.validation_credit_bytes.saturating_sub(bytes as u64);
            }
        }
        self.flights
            .lock()
            .expect("server TCP stream flight lock")
            .entry(offset)
            .or_default()
            .push(ServerTcpPathFlight {
                key,
                end,
                bytes,
                stream_ack_proves_path,
            });
    }

    fn lower_flights_before_frame(&self, frame: &Frame) -> Vec<ServerTcpPathFlightDebt> {
        let Some((offset, _, _)) = reliable_stream_frame_extent(frame) else {
            return Vec::new();
        };
        self.lower_flights_before_offset(offset)
    }

    fn flight_keys_overlapping_frame(&self, frame: &Frame) -> Vec<ServerTcpPathKey> {
        let Some((start, end, _)) = reliable_stream_frame_extent(frame) else {
            return Vec::new();
        };
        let flights = self.flights.lock().expect("server TCP stream flight lock");
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

    fn lower_flights_before_offset(&self, offset: u64) -> Vec<ServerTcpPathFlightDebt> {
        let mut debts = BTreeMap::<u64, ServerTcpPathFlightDebt>::new();
        {
            let flights = self.flights.lock().expect("server TCP stream flight lock");
            for (flight_offset, path_flights) in flights.range(..offset) {
                if let Some(latest) = path_flights.last() {
                    debts.insert(
                        *flight_offset,
                        ServerTcpPathFlightDebt {
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
                .expect("server TCP ACK ordering lock");
            for (hole_offset, holes) in ack_ordering.acked_holes.range(..offset) {
                if let Some(latest) = holes.last() {
                    debts.insert(
                        *hole_offset,
                        ServerTcpPathFlightDebt {
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
            .expect("server TCP stream binding lock")
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
        key: ServerTcpPathKey,
        metrics: PathMetrics,
        source: ServerPathMetricsSource,
    ) {
        let mut outputs = self.outputs.lock().expect("server TCP stream binding lock");
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
            ServerTcpPathKey { underlay, path_id },
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
            ServerTcpPathKey { underlay, path_id },
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
        let outputs = self.outputs.lock().expect("server TCP stream binding lock");
        outputs
            .entries
            .iter()
            .find(|entry| entry.key == ServerTcpPathKey { underlay, path_id })
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
    pub(super) fn bulk_choice_key_for_test(
        &self,
        payload_bytes: usize,
    ) -> Option<ServerTcpPathKey> {
        let mut outputs = self.outputs.lock().expect("server TCP stream binding lock");
        outputs
            .bulk_commands(
                self.session_id,
                &self.lane_tracker,
                FlowLane::Throughput,
                payload_bytes,
                self.mux_limits,
                &[],
            )
            .map(|choice| choice.primary.key)
    }

    #[cfg(test)]
    pub(super) fn output_has_sender_evidence_for_test(
        &self,
        underlay: UnderlayProtocol,
        path_id: PathId,
    ) -> bool {
        let outputs = self.outputs.lock().expect("server TCP stream binding lock");
        outputs
            .entries
            .iter()
            .find(|entry| entry.key == ServerTcpPathKey { underlay, path_id })
            .is_some_and(server_output_has_sender_evidence)
    }

    #[cfg(test)]
    pub(super) fn output_eta_ms_for_test(
        &self,
        underlay: UnderlayProtocol,
        path_id: PathId,
        payload_bytes: usize,
    ) -> Option<f64> {
        let outputs = self.outputs.lock().expect("server TCP stream binding lock");
        let active_key = outputs.entries.last().map(|entry| entry.key);
        outputs
            .entries
            .iter()
            .find(|entry| entry.key == ServerTcpPathKey { underlay, path_id })
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
            ServerTcpPathKey { underlay, path_id },
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
            .expect("server TCP ACK ordering lock");
        (ordering.contiguous_frontier, ordering.acked_hole_bytes())
    }
}

fn server_frame_prefers_current_data_path(frame: &Frame, lane: FlowLane) -> bool {
    matches!(frame, Frame::StreamData { .. }) && !relay_lane_is_bulk(lane)
}

fn server_stream_open_role_promotes_data_path(role: StreamOpenRole) -> bool {
    role == StreamOpenRole::Active
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct ServerTcpPathKey {
    pub(super) underlay: UnderlayProtocol,
    pub(super) path_id: PathId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ServerPathLoadKey {
    session_id: SessionId,
    path: ServerTcpPathKey,
}

#[derive(Debug, Clone, Copy, Default)]
struct ServerPathLaneLoad {
    active_flows: u32,
    active_latency_sensitive_flows: u32,
}

impl ServerPathLaneLoad {
    fn add(&mut self, lane: FlowLane) {
        self.active_flows = self.active_flows.saturating_add(1);
        if tcp_relay_expects_interactive_response(lane) {
            self.active_latency_sensitive_flows =
                self.active_latency_sensitive_flows.saturating_add(1);
        }
    }

    fn remove(&mut self, lane: FlowLane) {
        self.active_flows = self.active_flows.saturating_sub(1);
        if tcp_relay_expects_interactive_response(lane) {
            self.active_latency_sensitive_flows =
                self.active_latency_sensitive_flows.saturating_sub(1);
        }
    }
}

#[derive(Debug, Default)]
struct ServerPathLaneTracker {
    loads: Mutex<HashMap<ServerPathLoadKey, ServerPathLaneLoad>>,
}

impl ServerPathLaneTracker {
    fn attach(&self, session_id: SessionId, path: ServerTcpPathKey, lane: FlowLane) {
        self.loads
            .lock()
            .expect("server path lane tracker lock")
            .entry(ServerPathLoadKey { session_id, path })
            .or_default()
            .add(lane);
    }

    fn detach(&self, session_id: SessionId, path: ServerTcpPathKey, lane: FlowLane) {
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
        paths: &[ServerTcpPathKey],
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

    fn snapshot(&self, session_id: SessionId, path: ServerTcpPathKey) -> ServerPathLaneLoad {
        self.loads
            .lock()
            .expect("server path lane tracker lock")
            .get(&ServerPathLoadKey { session_id, path })
            .copied()
            .unwrap_or_default()
    }
}

#[derive(Clone)]
struct ServerTcpStreamOutputEntry {
    key: ServerTcpPathKey,
    commands: TcpPathSessionCommandSender,
    bytes_in_flight: u64,
    product_queue_bytes: u64,
    delivery_samples: u32,
    last_delivery_at: Option<Instant>,
    validation_credit_bytes: u64,
    path_metrics: Option<ServerPathMetricsEntry>,
}

struct ServerTcpStreamOutputs {
    entries: Vec<ServerTcpStreamOutputEntry>,
    next_index: usize,
}

#[derive(Debug, Clone, Copy)]
struct ServerTcpPathFlight {
    key: ServerTcpPathKey,
    end: u64,
    bytes: usize,
    stream_ack_proves_path: bool,
}

#[derive(Debug, Clone, Copy)]
struct ServerTcpPathFlightDebt {
    key: ServerTcpPathKey,
    bytes: u64,
}

#[derive(Debug, Clone, Copy)]
struct ServerTcpPathAckedHole {
    key: ServerTcpPathKey,
    end: u64,
    bytes: u64,
    stream_ack_proves_path: bool,
}

#[derive(Debug, Default)]
struct ServerTcpAckOrderingState {
    contiguous_frontier: u64,
    acked_holes: BTreeMap<u64, Vec<ServerTcpPathAckedHole>>,
}

struct ServerTcpAckOrderingUpdate {
    changed: bool,
    contiguous_frontier: u64,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    acked_hole_bytes: u64,
    newly_contiguous: Vec<ServerTcpPathAckedHole>,
}

impl ServerTcpAckOrderingState {
    fn apply_ack(
        &mut self,
        ranges: &[OffsetRange],
        released: &[(u64, ServerTcpPathFlight)],
    ) -> ServerTcpAckOrderingUpdate {
        let previous_frontier = self.contiguous_frontier;
        let previous_hole_bytes = self.acked_hole_bytes();
        let mut newly_contiguous = Vec::new();

        for (offset, flight) in released {
            let hole = ServerTcpPathAckedHole {
                key: flight.key,
                end: flight.end,
                bytes: flight.bytes as u64,
                stream_ack_proves_path: flight.stream_ack_proves_path,
            };
            if hole.end <= self.contiguous_frontier {
                newly_contiguous.push(hole);
            } else {
                self.acked_holes.entry(*offset).or_default().push(hole);
            }
        }

        self.advance_contiguous_frontier(ranges);
        let frontier = self.contiguous_frontier;
        self.acked_holes.retain(|_, holes| {
            holes.retain(|hole| {
                if hole.end <= frontier {
                    newly_contiguous.push(*hole);
                    false
                } else {
                    true
                }
            });
            !holes.is_empty()
        });
        let acked_hole_bytes = self.acked_hole_bytes();

        ServerTcpAckOrderingUpdate {
            changed: previous_frontier != self.contiguous_frontier
                || previous_hole_bytes != acked_hole_bytes
                || !newly_contiguous.is_empty(),
            contiguous_frontier: self.contiguous_frontier,
            acked_hole_bytes,
            newly_contiguous,
        }
    }

    fn advance_contiguous_frontier(&mut self, ranges: &[OffsetRange]) {
        let ranges = normalized_offset_ranges(ranges);
        loop {
            let mut next_frontier = self.contiguous_frontier;
            for range in &ranges {
                if range.start > next_frontier {
                    break;
                }
                if range.end > next_frontier {
                    next_frontier = range.end;
                }
            }
            for (offset, holes) in self.acked_holes.range(..=next_frontier) {
                if *offset > next_frontier {
                    break;
                }
                for hole in holes {
                    if hole.end > next_frontier {
                        next_frontier = hole.end;
                    }
                }
            }
            if next_frontier == self.contiguous_frontier {
                break;
            }
            self.contiguous_frontier = next_frontier;
        }
    }

    fn acked_hole_bytes(&self) -> u64 {
        self.acked_holes
            .values()
            .flat_map(|holes| holes.iter())
            .map(|hole| hole.bytes)
            .sum()
    }
}

fn server_stream_ordering_debt_bytes(
    lower_flights: &[ServerTcpPathFlightDebt],
    candidate: ServerTcpPathKey,
) -> u64 {
    lower_flights
        .iter()
        .filter_map(|flight| (flight.key != candidate).then_some(flight.bytes))
        .sum()
}

fn server_total_lower_flight_debt_bytes(lower_flights: &[ServerTcpPathFlightDebt]) -> u64 {
    lower_flights.iter().map(|flight| flight.bytes).sum()
}

fn server_admission_ordering_debt_bytes(
    lower_flights: &[ServerTcpPathFlightDebt],
    candidate: ServerTcpPathKey,
    role: BulkAdmissionRole,
) -> u64 {
    if role == BulkAdmissionRole::ActiveDataPath {
        server_total_lower_flight_debt_bytes(lower_flights)
    } else {
        server_stream_ordering_debt_bytes(lower_flights, candidate)
    }
}

fn server_oldest_lower_flight_owner(
    lower_flights: &[ServerTcpPathFlightDebt],
) -> Option<ServerTcpPathKey> {
    lower_flights.first().map(|flight| flight.key)
}

fn server_bulk_admission_role(
    lead_key: ServerTcpPathKey,
    candidate: ServerTcpPathKey,
    lower_flight_owner: Option<ServerTcpPathKey>,
    ordering_debt: u64,
) -> BulkAdmissionRole {
    if lower_flight_owner == Some(candidate) || (candidate == lead_key && ordering_debt == 0) {
        BulkAdmissionRole::ActiveDataPath
    } else if let Some(owner) = lower_flight_owner {
        bulk_additional_admission_role(owner.underlay, candidate.underlay)
    } else {
        bulk_additional_admission_role(lead_key.underlay, candidate.underlay)
    }
}

fn server_bulk_lead_candidate_suppression(
    key: ServerTcpPathKey,
    snapshot: PathSnapshot,
    eta_ms: f64,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    lower_flights: &[ServerTcpPathFlightDebt],
) -> Option<&'static str> {
    let lower_flight_owner = server_oldest_lower_flight_owner(lower_flights);
    let role = if lower_flight_owner.is_none() || lower_flight_owner == Some(key) {
        BulkAdmissionRole::ActiveDataPath
    } else {
        bulk_additional_admission_role(lower_flight_owner.expect("checked").underlay, key.underlay)
    };
    let ordering_debt = if role == BulkAdmissionRole::ActiveDataPath {
        server_total_lower_flight_debt_bytes(lower_flights)
    } else {
        server_stream_ordering_debt_bytes(lower_flights, key)
    };
    bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
        best_snapshot: snapshot,
        best_eta_ms: eta_ms,
        candidate_snapshot: snapshot,
        candidate_eta_ms: eta_ms,
        payload_bytes,
        mux_limits,
        role,
        stream_ordering_debt_bytes: ordering_debt,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ServerPathMetricsSource {
    PeerHint,
    LocalSender,
}

#[derive(Debug, Clone, Copy)]
struct ServerPathMetricsEntry {
    metrics: PathMetrics,
    source: ServerPathMetricsSource,
}

#[derive(Clone)]
struct ServerTcpPathSendTarget {
    key: ServerTcpPathKey,
    commands: TcpPathSessionCommandSender,
}

struct ServerTcpPathBulkChoice {
    primary: ServerTcpPathSendTarget,
    validation_duplicate: Option<ServerTcpPathSendTarget>,
}

enum ServerTcpPathSendChoice {
    Single(ServerTcpPathSendTarget),
    Bulk(ServerTcpPathBulkChoice),
}

impl ServerTcpStreamOutputs {
    fn read_backpressure_snapshot(
        &self,
        session_id: SessionId,
        lane_tracker: &ServerPathLaneTracker,
        lane: FlowLane,
        payload_bytes: usize,
        mux_limits: MuxLimits,
    ) -> Option<PathSnapshot> {
        let now = Instant::now();
        if !relay_lane_is_bulk(lane) {
            return self.entries.last().map(|entry| {
                server_bulk_output_snapshot(entry, session_id, lane, lane_tracker, mux_limits, now)
            });
        }
        let active_key = self.entries.last().map(|entry| entry.key);
        self.entries
            .iter()
            .filter(|entry| {
                Some(entry.key) == active_key || server_output_has_sender_evidence(entry)
            })
            .map(|entry| {
                let snapshot = server_bulk_output_snapshot(
                    entry,
                    session_id,
                    lane,
                    lane_tracker,
                    mux_limits,
                    now,
                );
                let eta_ms = server_bulk_output_eta_ms(
                    entry.key,
                    snapshot,
                    active_key,
                    lane,
                    payload_bytes,
                    mux_limits,
                );
                (eta_ms, snapshot)
            })
            .min_by(|left, right| left.0.total_cmp(&right.0))
            .map(|(_, snapshot)| snapshot)
    }

    fn next_commands(&mut self) -> Option<ServerTcpPathSendTarget> {
        if self.entries.is_empty() {
            return None;
        }
        self.next_index %= self.entries.len();
        let entry = self.entries[self.next_index].clone();
        self.next_index = (self.next_index + 1) % self.entries.len();
        Some(ServerTcpPathSendTarget {
            key: entry.key,
            commands: entry.commands,
        })
    }

    fn data_commands(&self) -> Option<ServerTcpPathSendTarget> {
        self.entries
            .last()
            .cloned()
            .map(|entry| ServerTcpPathSendTarget {
                key: entry.key,
                commands: entry.commands,
            })
    }

    fn repair_commands(
        &self,
        session_id: SessionId,
        lane_tracker: &ServerPathLaneTracker,
        avoid_keys: &[ServerTcpPathKey],
        payload_bytes: usize,
        mux_limits: MuxLimits,
    ) -> Option<ServerTcpPathSendTarget> {
        let now = Instant::now();
        let active_key = self.entries.last().map(|entry| entry.key);
        let choose = |prefer_avoiding: bool| {
            self.entries
                .iter()
                .filter(|entry| !prefer_avoiding || !avoid_keys.contains(&entry.key))
                .map(|entry| {
                    let snapshot = server_bulk_output_snapshot(
                        entry,
                        session_id,
                        FlowLane::Latency,
                        lane_tracker,
                        mux_limits,
                        now,
                    );
                    let eta_ms = server_bulk_output_eta_ms(
                        entry.key,
                        snapshot,
                        active_key,
                        FlowLane::Latency,
                        payload_bytes,
                        mux_limits,
                    );
                    (eta_ms, entry)
                })
                .min_by(|left, right| left.0.total_cmp(&right.0))
                .map(|(_, entry)| ServerTcpPathSendTarget {
                    key: entry.key,
                    commands: entry.commands.clone(),
                })
        };
        choose(true).or_else(|| choose(false))
    }

    fn bulk_send_ready(
        &self,
        session_id: SessionId,
        lane_tracker: &ServerPathLaneTracker,
        lane: FlowLane,
        payload_bytes: usize,
        mux_limits: MuxLimits,
        lower_flights: &[ServerTcpPathFlightDebt],
    ) -> bool {
        self.select_bulk_output(
            session_id,
            lane_tracker,
            lane,
            payload_bytes,
            mux_limits,
            lower_flights,
            Instant::now(),
        )
        .is_some()
    }

    fn bulk_commands(
        &mut self,
        session_id: SessionId,
        lane_tracker: &ServerPathLaneTracker,
        lane: FlowLane,
        payload_bytes: usize,
        mux_limits: MuxLimits,
        lower_flights: &[ServerTcpPathFlightDebt],
    ) -> Option<ServerTcpPathBulkChoice> {
        let now = Instant::now();
        #[cfg(feature = "lab-diagnostics")]
        {
            let active_key = self.entries.last().map(|entry| entry.key);
            let lead_candidate = self.bulk_lead_candidate(
                session_id,
                lane_tracker,
                lane,
                payload_bytes,
                mux_limits,
                now,
                active_key,
                lower_flights,
            );
            self.log_bulk_candidates(
                session_id,
                lane_tracker,
                lane,
                active_key,
                lead_candidate,
                payload_bytes,
                mux_limits,
                now,
                lower_flights,
            );
        }
        let (position, primary_eta_ms, primary_snapshot) = self.select_bulk_output(
            session_id,
            lane_tracker,
            lane,
            payload_bytes,
            mux_limits,
            lower_flights,
            now,
        )?;
        let entry = self.entries[position].clone();
        #[cfg(feature = "lab-diagnostics")]
        let snapshot =
            server_bulk_output_snapshot(&entry, session_id, lane, lane_tracker, mux_limits, now);
        self.next_index = (position + 1) % self.entries.len().max(1);
        #[cfg(feature = "lab-diagnostics")]
        lab_diagnostic(
            "server_bulk_output_selected",
            format_args!(
                "path_underlay={:?} path_id={} reason=admitted payload_bytes={} scoring_payload_bytes={} delivery_samples={} validation_credit_bytes={} product_bytes_in_flight={} carrier_bytes_in_flight={} queue_bytes={} inflight_limit={} active_flows={} active_latency_sensitive_flows={}",
                entry.key.underlay,
                entry.key.path_id.0,
                payload_bytes,
                bulk_service_horizon_payload_bytes(payload_bytes, mux_limits),
                entry.delivery_samples,
                entry.validation_credit_bytes,
                entry.bytes_in_flight,
                snapshot.bytes_in_flight,
                snapshot.queue_bytes,
                snapshot.inflight_limit_bytes,
                snapshot.active_flows,
                snapshot.active_latency_sensitive_flows,
            ),
        );
        let validation_duplicate = self.validation_duplicate_for_bulk_choice(
            &entry,
            session_id,
            lane_tracker,
            lane,
            primary_eta_ms,
            primary_snapshot,
            payload_bytes,
            mux_limits,
            lower_flights,
            now,
        );
        Some(ServerTcpPathBulkChoice {
            primary: ServerTcpPathSendTarget {
                key: entry.key,
                commands: entry.commands,
            },
            validation_duplicate,
        })
    }

    fn select_bulk_output(
        &self,
        session_id: SessionId,
        lane_tracker: &ServerPathLaneTracker,
        lane: FlowLane,
        payload_bytes: usize,
        mux_limits: MuxLimits,
        lower_flights: &[ServerTcpPathFlightDebt],
        now: Instant,
    ) -> Option<(usize, f64, PathSnapshot)> {
        let active_key = self.entries.last().map(|entry| entry.key);
        let lower_flight_owner = server_oldest_lower_flight_owner(lower_flights);
        let attached_lower_flight_owner =
            lower_flight_owner.filter(|owner| self.entries.iter().any(|entry| entry.key == *owner));
        if let Some(owner) = attached_lower_flight_owner
            && self
                .lower_frontier_owner_service_suppression(
                    session_id,
                    lane_tracker,
                    lane,
                    owner,
                    payload_bytes,
                    mux_limits,
                    lower_flights,
                    now,
                )
                .is_some()
        {
            return None;
        }
        let has_sender_evidence_candidate =
            self.entries.iter().any(server_output_has_sender_evidence);
        let normal_candidates = self
            .entries
            .iter()
            .enumerate()
            .filter(|(_, entry)| {
                server_output_can_carry_primary_bulk(
                    entry,
                    active_key,
                    payload_bytes,
                    lower_flights,
                    attached_lower_flight_owner,
                    has_sender_evidence_candidate,
                )
            })
            .map(|(position, entry)| {
                let snapshot = server_bulk_output_snapshot(
                    entry,
                    session_id,
                    lane,
                    lane_tracker,
                    mux_limits,
                    now,
                );
                let eta_ms = server_bulk_output_eta_ms(
                    entry.key,
                    snapshot,
                    active_key,
                    lane,
                    payload_bytes,
                    mux_limits,
                );
                (position, eta_ms, snapshot)
            })
            .collect::<Vec<_>>();
        let lead_candidate = normal_candidates
            .iter()
            .filter(|(position, eta_ms, snapshot)| {
                let key = self.entries[*position].key;
                server_bulk_lead_candidate_suppression(
                    key,
                    *snapshot,
                    *eta_ms,
                    payload_bytes,
                    mux_limits,
                    lower_flights,
                )
                .is_none()
            })
            .min_by(|left, right| left.1.total_cmp(&right.1))
            .map(|(position, eta_ms, snapshot)| (self.entries[*position].key, *eta_ms, *snapshot));
        normal_candidates
            .into_iter()
            .filter(|(position, eta_ms, snapshot)| {
                lead_candidate.is_some_and(|(lead_key, best_eta_ms, best_snapshot)| {
                    let key = self.entries[*position].key;
                    let cross_path_ordering_debt =
                        server_stream_ordering_debt_bytes(lower_flights, key);
                    let owns_lower_frontier = lower_flight_owner == Some(key);
                    let role = server_bulk_admission_role(
                        lead_key,
                        key,
                        lower_flight_owner,
                        cross_path_ordering_debt,
                    );
                    let admission_ordering_debt =
                        server_admission_ordering_debt_bytes(lower_flights, key, role);
                    let (baseline_snapshot, baseline_eta_ms) =
                        if owns_lower_frontier && role == BulkAdmissionRole::ActiveDataPath {
                            (*snapshot, *eta_ms)
                        } else {
                            (best_snapshot, best_eta_ms)
                        };
                    bulk_candidate_admission_suppression(
                        baseline_snapshot,
                        baseline_eta_ms,
                        *snapshot,
                        *eta_ms,
                        payload_bytes,
                        mux_limits,
                        role,
                    )
                    .or_else(|| {
                        bulk_candidate_admission_suppression_with_ordering_debt(
                            BulkAdmissionCheck {
                                best_snapshot: baseline_snapshot,
                                best_eta_ms: baseline_eta_ms,
                                candidate_snapshot: *snapshot,
                                candidate_eta_ms: *eta_ms,
                                payload_bytes,
                                mux_limits,
                                role,
                                stream_ordering_debt_bytes: admission_ordering_debt,
                            },
                        )
                    })
                    .is_none()
                })
            })
            .min_by(|left, right| left.1.total_cmp(&right.1))
    }

    fn lower_frontier_owner_service_suppression(
        &self,
        session_id: SessionId,
        lane_tracker: &ServerPathLaneTracker,
        lane: FlowLane,
        owner: ServerTcpPathKey,
        payload_bytes: usize,
        mux_limits: MuxLimits,
        lower_flights: &[ServerTcpPathFlightDebt],
        now: Instant,
    ) -> Option<&'static str> {
        let active_key = self.entries.last().map(|entry| entry.key);
        let owner_entry = self.entries.iter().find(|entry| entry.key == owner)?;
        let owner_snapshot = server_bulk_output_snapshot(
            owner_entry,
            session_id,
            lane,
            lane_tracker,
            mux_limits,
            now,
        );
        let owner_eta_ms = server_bulk_output_eta_ms(
            owner,
            owner_snapshot,
            active_key,
            lane,
            payload_bytes,
            mux_limits,
        );
        let alternate = self
            .entries
            .iter()
            .filter(|entry| entry.key != owner && server_output_has_sender_evidence(entry))
            .map(|entry| {
                let snapshot = server_bulk_output_snapshot(
                    entry,
                    session_id,
                    lane,
                    lane_tracker,
                    mux_limits,
                    now,
                );
                let eta_ms = server_bulk_output_eta_ms(
                    entry.key,
                    snapshot,
                    active_key,
                    lane,
                    payload_bytes,
                    mux_limits,
                );
                (entry.key, eta_ms, snapshot)
            })
            .filter(|(_, eta_ms, snapshot)| {
                bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
                    best_snapshot: *snapshot,
                    best_eta_ms: *eta_ms,
                    candidate_snapshot: *snapshot,
                    candidate_eta_ms: *eta_ms,
                    payload_bytes,
                    mux_limits,
                    role: BulkAdmissionRole::ActiveDataPath,
                    stream_ordering_debt_bytes: 0,
                })
                .is_none()
            })
            .min_by(|left, right| left.1.total_cmp(&right.1));
        let (_, alternate_eta_ms, alternate_snapshot) = alternate?;
        bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
            best_snapshot: alternate_snapshot,
            best_eta_ms: alternate_eta_ms,
            candidate_snapshot: owner_snapshot,
            candidate_eta_ms: owner_eta_ms,
            payload_bytes,
            mux_limits,
            role: BulkAdmissionRole::ActiveDataPath,
            stream_ordering_debt_bytes: server_total_lower_flight_debt_bytes(lower_flights),
        })
    }

    fn validation_duplicate_for_bulk_choice(
        &self,
        entry: &ServerTcpStreamOutputEntry,
        session_id: SessionId,
        lane_tracker: &ServerPathLaneTracker,
        lane: FlowLane,
        primary_eta_ms: f64,
        primary_snapshot: PathSnapshot,
        payload_bytes: usize,
        mux_limits: MuxLimits,
        lower_flights: &[ServerTcpPathFlightDebt],
        now: Instant,
    ) -> Option<ServerTcpPathSendTarget> {
        let active_key = self.entries.last().map(|entry| entry.key);
        let lower_flight_owner = server_oldest_lower_flight_owner(lower_flights);
        self
            .entries
            .iter()
            .enumerate()
            .filter(|(_, validation)| {
                validation.key != entry.key
                    && validation.key.underlay == UnderlayProtocol::Udp
                    && !server_output_has_sender_evidence(validation)
                    && validation.validation_credit_bytes >= payload_bytes as u64
            })
            .map(|(validation_position, validation)| {
                let validation_snapshot = server_bulk_output_snapshot(
                    validation,
                    session_id,
                    lane,
                    lane_tracker,
                    mux_limits,
                    now,
                );
                (
                    validation_position,
                    validation.key,
                    server_bulk_output_eta_ms(
                        validation.key,
                        validation_snapshot,
                        active_key,
                        lane,
                        payload_bytes,
                        mux_limits,
                    ),
                    validation_snapshot,
                )
            })
            .filter(|(_, validation_key, validation_eta_ms, validation_snapshot)| {
                let cross_path_ordering_debt =
                    server_stream_ordering_debt_bytes(lower_flights, *validation_key);
                let owns_lower_frontier = lower_flight_owner == Some(*validation_key);
                let role = server_bulk_admission_role(
                    entry.key,
                    *validation_key,
                    lower_flight_owner,
                    cross_path_ordering_debt,
                );
                let admission_ordering_debt =
                    server_admission_ordering_debt_bytes(lower_flights, *validation_key, role);
                let (baseline_snapshot, baseline_eta_ms) =
                    if owns_lower_frontier && role == BulkAdmissionRole::ActiveDataPath {
                        (*validation_snapshot, *validation_eta_ms)
                    } else {
                        (primary_snapshot, primary_eta_ms)
                    };
                bulk_candidate_admission_suppression(
                    baseline_snapshot,
                    baseline_eta_ms,
                    *validation_snapshot,
                    *validation_eta_ms,
                    payload_bytes,
                    mux_limits,
                    role,
                )
                .or_else(|| {
                    bulk_candidate_admission_suppression_with_ordering_debt(
                        BulkAdmissionCheck {
                            best_snapshot: baseline_snapshot,
                            best_eta_ms: baseline_eta_ms,
                            candidate_snapshot: *validation_snapshot,
                            candidate_eta_ms: *validation_eta_ms,
                            payload_bytes,
                            mux_limits,
                            role,
                            stream_ordering_debt_bytes: admission_ordering_debt,
                        },
                    )
                })
                .is_none()
            })
            .min_by(|left, right| left.2.total_cmp(&right.2))
            .map(|(validation_position, _, _, _)| {
                let validation = self.entries[validation_position].clone();
                #[cfg(feature = "lab-diagnostics")]
                {
                    let validation_snapshot = server_bulk_output_snapshot(
                        &validation,
                        session_id,
                        lane,
                        lane_tracker,
                        mux_limits,
                        now,
                    );
                    lab_diagnostic(
                        "server_bulk_output_selected",
                        format_args!(
                            "path_underlay={:?} path_id={} reason=validation_duplicate payload_bytes={} scoring_payload_bytes={} delivery_samples={} validation_credit_bytes={} product_bytes_in_flight={} carrier_bytes_in_flight={} queue_bytes={} inflight_limit={} active_flows={} active_latency_sensitive_flows={}",
                            validation.key.underlay,
                            validation.key.path_id.0,
                            payload_bytes,
                            bulk_service_horizon_payload_bytes(payload_bytes, mux_limits),
                            validation.delivery_samples,
                            validation.validation_credit_bytes,
                            validation.bytes_in_flight,
                            validation_snapshot.bytes_in_flight,
                            validation_snapshot.queue_bytes,
                            validation_snapshot.inflight_limit_bytes,
                            validation_snapshot.active_flows,
                            validation_snapshot.active_latency_sensitive_flows,
                        ),
                    );
                }
                ServerTcpPathSendTarget {
                    key: validation.key,
                    commands: validation.commands,
                }
            })
    }

    #[cfg(feature = "lab-diagnostics")]
    fn bulk_lead_candidate(
        &self,
        session_id: SessionId,
        lane_tracker: &ServerPathLaneTracker,
        lane: FlowLane,
        payload_bytes: usize,
        mux_limits: MuxLimits,
        now: Instant,
        active_key: Option<ServerTcpPathKey>,
        lower_flights: &[ServerTcpPathFlightDebt],
    ) -> Option<(ServerTcpPathKey, f64, PathSnapshot)> {
        let has_sender_evidence_candidate =
            self.entries.iter().any(server_output_has_sender_evidence);
        let attached_lower_flight_owner = server_oldest_lower_flight_owner(lower_flights)
            .filter(|owner| self.entries.iter().any(|entry| entry.key == *owner));
        self.entries
            .iter()
            .filter(|entry| {
                server_output_can_carry_primary_bulk(
                    entry,
                    active_key,
                    payload_bytes,
                    lower_flights,
                    attached_lower_flight_owner,
                    has_sender_evidence_candidate,
                )
            })
            .map(|entry| {
                let snapshot = server_bulk_output_snapshot(
                    entry,
                    session_id,
                    lane,
                    lane_tracker,
                    mux_limits,
                    now,
                );
                let eta_ms = server_bulk_output_eta_ms(
                    entry.key,
                    snapshot,
                    active_key,
                    lane,
                    payload_bytes,
                    mux_limits,
                );
                (entry.key, eta_ms, snapshot)
            })
            .filter(|(key, eta_ms, snapshot)| {
                server_bulk_lead_candidate_suppression(
                    *key,
                    *snapshot,
                    *eta_ms,
                    payload_bytes,
                    mux_limits,
                    lower_flights,
                )
                .is_none()
            })
            .min_by(|left, right| left.1.total_cmp(&right.1))
    }

    #[cfg(feature = "lab-diagnostics")]
    fn log_bulk_candidates(
        &self,
        session_id: SessionId,
        lane_tracker: &ServerPathLaneTracker,
        lane: FlowLane,
        active_key: Option<ServerTcpPathKey>,
        lead_candidate: Option<(ServerTcpPathKey, f64, PathSnapshot)>,
        payload_bytes: usize,
        mux_limits: MuxLimits,
        now: Instant,
        lower_flights: &[ServerTcpPathFlightDebt],
    ) {
        let attached_lower_flight_owner = server_oldest_lower_flight_owner(lower_flights)
            .filter(|owner| self.entries.iter().any(|entry| entry.key == *owner));
        for entry in &self.entries {
            let snapshot =
                server_bulk_output_snapshot(entry, session_id, lane, lane_tracker, mux_limits, now);
            let eta_ms = server_bulk_output_eta_ms(
                entry.key,
                snapshot,
                active_key,
                lane,
                payload_bytes,
                mux_limits,
            );
            let validation_ordering_debt =
                server_stream_ordering_debt_bytes(lower_flights, entry.key);
            let has_sender_evidence_candidate =
                self.entries.iter().any(server_output_has_sender_evidence);
            let reason = if Some(entry.key) != active_key
                && attached_lower_flight_owner.is_some_and(|owner| owner != entry.key)
            {
                "waiting_for_lower_frontier_owner"
            } else if Some(entry.key) != active_key
                && !server_output_has_sender_evidence(entry)
                && !server_output_has_primary_validation_credit(entry, payload_bytes)
            {
                "validation_credit_exhausted"
            } else if Some(entry.key) != active_key
                && !server_output_has_sender_evidence(entry)
                && has_sender_evidence_candidate
            {
                "validation_without_sender_evidence"
            } else if Some(entry.key) != active_key
                && !server_output_has_sender_evidence(entry)
                && validation_ordering_debt > 0
            {
                "validation_would_expand_ordering_debt"
            } else if let Some((lead_key, best_eta_ms, best_snapshot)) = lead_candidate {
                let cross_path_ordering_debt =
                    server_stream_ordering_debt_bytes(lower_flights, entry.key);
                let role = server_bulk_admission_role(
                    lead_key,
                    entry.key,
                    server_oldest_lower_flight_owner(lower_flights),
                    cross_path_ordering_debt,
                );
                let admission_ordering_debt =
                    server_admission_ordering_debt_bytes(lower_flights, entry.key, role);
                let owns_lower_frontier =
                    server_oldest_lower_flight_owner(lower_flights) == Some(entry.key);
                let (baseline_snapshot, baseline_eta_ms) =
                    if owns_lower_frontier && role == BulkAdmissionRole::ActiveDataPath {
                        (snapshot, eta_ms)
                    } else {
                        (best_snapshot, best_eta_ms)
                    };
                if let Some(suppression) =
                    bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
                        best_snapshot: baseline_snapshot,
                        best_eta_ms: baseline_eta_ms,
                        candidate_snapshot: snapshot,
                        candidate_eta_ms: eta_ms,
                        payload_bytes,
                        mux_limits,
                        role,
                        stream_ordering_debt_bytes: admission_ordering_debt,
                    })
                {
                    suppression
                } else if entry.key == lead_key
                    && server_output_has_primary_validation_credit(entry, payload_bytes)
                    && !server_output_has_sender_evidence(entry)
                {
                    "validation_lead_admitted"
                } else if entry.key == lead_key {
                    "lead_admitted"
                } else if server_output_has_sender_evidence(entry) {
                    "delivery_evidence_admitted"
                } else {
                    "validation_candidate"
                }
            } else {
                "validation_candidate_no_admitted_baseline"
            };
            lab_diagnostic(
                "server_bulk_output_candidate",
                format_args!(
                    "path_underlay={:?} path_id={} active={} reason={} payload_bytes={} scoring_payload_bytes={} eta_ms={:.3} confidence={:.3} delivery_samples={} validation_credit_bytes={} product_bytes_in_flight={} carrier_bytes_in_flight={} stream_ordering_debt={} queue_bytes={} command_pending_bytes={} inflight_limit={} active_flows={} active_latency_sensitive_flows={} srtt_ms={:.3} delivery_rate_mbps={:.3}",
                    entry.key.underlay,
                    entry.key.path_id.0,
                    Some(entry.key) == active_key,
                    reason,
                    payload_bytes,
                    bulk_service_horizon_payload_bytes(payload_bytes, mux_limits),
                    eta_ms,
                    snapshot.confidence,
                    entry.delivery_samples,
                    entry.validation_credit_bytes,
                    entry.bytes_in_flight,
                    snapshot.bytes_in_flight,
                    server_admission_ordering_debt_bytes(
                        lower_flights,
                        entry.key,
                        server_bulk_admission_role(
                            lead_candidate
                                .map(|candidate| candidate.0)
                                .unwrap_or(entry.key),
                            entry.key,
                            server_oldest_lower_flight_owner(lower_flights),
                            server_stream_ordering_debt_bytes(lower_flights, entry.key),
                        )
                    ),
                    snapshot.queue_bytes,
                    entry.commands.pending_bytes(),
                    snapshot.inflight_limit_bytes,
                    snapshot.active_flows,
                    snapshot.active_latency_sensitive_flows,
                    snapshot.srtt_ms,
                    snapshot.delivery_rate_bps / 1_000_000.0,
                ),
            );
        }
    }
}

fn server_bulk_output_snapshot(
    entry: &ServerTcpStreamOutputEntry,
    session_id: SessionId,
    lane: FlowLane,
    lane_tracker: &ServerPathLaneTracker,
    mux_limits: MuxLimits,
    now: Instant,
) -> PathSnapshot {
    let local_sender_metrics = entry.path_metrics.and_then(|path_metrics| {
        (path_metrics.source == ServerPathMetricsSource::LocalSender).then_some(path_metrics)
    });
    let validation_hint_metrics = entry
        .path_metrics
        .and_then(|path_metrics| (entry.delivery_samples == 0).then_some(path_metrics));
    let model_metrics = local_sender_metrics.or(validation_hint_metrics);
    let srtt_ms = model_metrics.map_or_else(
        || default_path_srtt_ms(entry.key.underlay),
        |path_metrics| f64::from(path_metrics.metrics.srtt_us.max(1)) / 1000.0,
    );
    let jitter_ms = model_metrics.map_or(0.0, |path_metrics| {
        f64::from(path_metrics.metrics.jitter_us) / 1000.0
    });
    let loss_rate = model_metrics
        .map_or(0.0, |path_metrics| {
            f64::from(path_metrics.metrics.loss_ppm) / 1_000_000.0
        })
        .clamp(0.0, 1.0);
    let model_rate_bps = model_metrics.map(server_path_metrics_rate_bps);
    let local_sender_rate_bps = local_sender_metrics.map(server_path_metrics_rate_bps);
    let rate_bps = match entry.key.underlay {
        UnderlayProtocol::Udp => local_sender_rate_bps,
        UnderlayProtocol::Tcp => model_rate_bps,
    }
    .unwrap_or_else(|| default_path_rate_bps(entry.key.underlay))
    .max(1.0);
    let mut snapshot = PathSnapshot::new(entry.key.path_id, entry.key.underlay, srtt_ms, rate_bps);
    snapshot.jitter_ms = jitter_ms;
    snapshot.loss_rate = loss_rate;
    if let Some(path_metrics) = model_metrics {
        snapshot.pacing_rate_bps =
            (path_metrics.metrics.pacing_rate_bps.max(1) as f64).max(snapshot.delivery_rate_bps);
        snapshot.app_limited = path_metrics.metrics.app_limited;
    }
    let metric_queue_bytes =
        model_metrics.map_or(0, |path_metrics| path_metrics.metrics.queue_bytes);
    snapshot.queue_bytes = metric_queue_bytes.saturating_add(entry.commands.pending_bytes());
    snapshot.product_queue_bytes = entry.product_queue_bytes;
    snapshot.bytes_in_flight = match entry.key.underlay {
        UnderlayProtocol::Udp => {
            local_sender_metrics.map_or(0, |path_metrics| path_metrics.metrics.bytes_in_flight)
        }
        UnderlayProtocol::Tcp => entry.bytes_in_flight,
    };
    snapshot.product_bytes_in_flight = entry.bytes_in_flight;
    snapshot.inflight_limit_bytes =
        model_metrics.map_or(0, |path_metrics| path_metrics.metrics.inflight_limit_bytes);
    snapshot.confidence = server_output_confidence(entry, now);
    let lane_load = lane_tracker.snapshot(session_id, entry.key);
    snapshot.active_flows = lane_load.active_flows;
    snapshot.active_latency_sensitive_flows = lane_load.active_latency_sensitive_flows;
    let known_bulk_flows = lane_load
        .active_flows
        .saturating_sub(lane_load.active_latency_sensitive_flows);
    if relay_lane_is_bulk(lane)
        && lane_load.active_latency_sensitive_flows > 0
        && known_bulk_flows > 0
    {
        let latency_headroom =
            adaptive_tcp_relay_inflight_bytes(Some(snapshot), FlowLane::Latency, mux_limits) as u64;
        let protected_queue =
            latency_headroom.saturating_mul(u64::from(lane_load.active_latency_sensitive_flows));
        snapshot.queue_bytes = snapshot.queue_bytes.saturating_add(protected_queue);
    }
    snapshot
}

fn server_bulk_output_eta_ms(
    key: ServerTcpPathKey,
    snapshot: PathSnapshot,
    active_key: Option<ServerTcpPathKey>,
    lane: FlowLane,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> f64 {
    let queued_bits = snapshot
        .queue_bytes
        .saturating_add(snapshot.product_queue_bytes)
        .saturating_add(snapshot.bytes_in_flight)
        .saturating_mul(8) as f64;
    let scoring_payload_bytes = if relay_lane_is_bulk(lane) {
        bulk_service_horizon_payload_bytes(payload_bytes, mux_limits)
    } else {
        payload_bytes
    };
    let payload_bits = scoring_payload_bytes as f64 * 8.0;
    let mut eta_ms = snapshot.srtt_ms / 2.0;
    let effective_rate_bps = if relay_lane_is_bulk(lane) {
        snapshot
            .delivery_rate_bps
            .max(snapshot.pacing_rate_bps)
            .max(1.0)
    } else {
        snapshot.delivery_rate_bps.max(1.0)
    };
    eta_ms += (queued_bits + payload_bits) / effective_rate_bps * 1000.0;
    eta_ms += snapshot.jitter_ms;
    eta_ms += snapshot.loss_rate.clamp(0.0, 1.0) * 500.0;
    if key.underlay == UnderlayProtocol::Udp && relay_lane_is_bulk(lane) {
        eta_ms += udp_reliable_stream_loss_repair_penalty_ms(snapshot, payload_bytes);
    }
    eta_ms += (1.0 - snapshot.confidence.clamp(0.0, 1.0)) * snapshot.srtt_ms;
    if Some(key) != active_key && snapshot.confidence < 0.5 {
        eta_ms += snapshot.srtt_ms;
        if snapshot.bytes_in_flight > 0 {
            eta_ms += snapshot.srtt_ms;
        }
    }
    eta_ms
}

fn server_output_confidence(entry: &ServerTcpStreamOutputEntry, now: Instant) -> f64 {
    let delivery_confidence = (f64::from(entry.delivery_samples) / 8.0).clamp(0.0, 1.0);
    let metric_confidence = match entry.path_metrics {
        Some(ServerPathMetricsEntry {
            source: ServerPathMetricsSource::LocalSender,
            metrics,
        }) if metrics.has_ack_derived_data_sample => {
            let source_confidence =
                f64::from(metrics.confidence_ppm).clamp(0.0, 1_000_000.0) / 1_000_000.0;
            let sample_confidence = (f64::from(metrics.data_sample_count) / 8.0).clamp(0.0, 1.0);
            source_confidence * sample_confidence
        }
        Some(ServerPathMetricsEntry {
            source: ServerPathMetricsSource::PeerHint,
            ..
        }) => 0.1,
        _ => 0.0,
    };
    let freshness_confidence = entry
        .last_delivery_at
        .map(|seen| {
            let age = now.saturating_duration_since(seen).as_secs_f64();
            (1.0 - age / 30.0).clamp(0.0, 1.0) * 0.25
        })
        .unwrap_or(0.0);
    delivery_confidence
        .max(metric_confidence)
        .max(freshness_confidence)
        .clamp(0.1, 1.0)
}

fn server_path_metrics_rate_bps(path_metrics: ServerPathMetricsEntry) -> f64 {
    let delivery_rate_bps = path_metrics.metrics.delivery_rate_bps.max(1) as f64;
    let pacing_rate_bps = path_metrics.metrics.pacing_rate_bps.max(1) as f64;
    if path_metrics.source == ServerPathMetricsSource::LocalSender
        && path_metrics.metrics.app_limited
    {
        delivery_rate_bps.max(pacing_rate_bps)
    } else {
        delivery_rate_bps
    }
}

fn server_output_has_sender_evidence(entry: &ServerTcpStreamOutputEntry) -> bool {
    entry.delivery_samples > 0
        || matches!(
            entry.path_metrics,
            Some(ServerPathMetricsEntry {
                source: ServerPathMetricsSource::LocalSender,
                metrics: PathMetrics {
                    delivery_rate_bps: 1..,
                    has_ack_derived_data_sample: true,
                    ..
                },
            })
        )
}

fn server_output_has_primary_validation_credit(
    entry: &ServerTcpStreamOutputEntry,
    payload_bytes: usize,
) -> bool {
    entry.validation_credit_bytes >= payload_bytes as u64
}

fn server_output_can_carry_primary_bulk(
    entry: &ServerTcpStreamOutputEntry,
    active_key: Option<ServerTcpPathKey>,
    payload_bytes: usize,
    lower_flights: &[ServerTcpPathFlightDebt],
    attached_lower_flight_owner: Option<ServerTcpPathKey>,
    has_sender_evidence_candidate: bool,
) -> bool {
    if let Some(owner) = attached_lower_flight_owner
        && entry.key != owner
    {
        return false;
    }
    if Some(entry.key) == active_key || server_output_has_sender_evidence(entry) {
        return true;
    }
    if has_sender_evidence_candidate {
        return false;
    }
    server_output_has_primary_validation_credit(entry, payload_bytes)
        && server_stream_ordering_debt_bytes(lower_flights, entry.key) == 0
}

fn record_server_sender_decision(
    session_id: SessionId,
    stream_id: StreamId,
    key: ServerTcpPathKey,
    frame: &Frame,
    lane: FlowLane,
    reason: &'static str,
) {
    #[cfg(feature = "lab-diagnostics")]
    lab_sender_service_decision(
        "server",
        Some(session_id.0),
        stream_id.0,
        reason,
        sender_service_frame_kind(frame),
        reliable_stream_frame_payload_bytes(frame),
        format_args!(
            "path_underlay={:?} path_id={} lane={:?} pacing_bytes={}",
            key.underlay,
            key.path_id.0,
            lane,
            frame_pacing_bytes(frame),
        ),
    );
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = (session_id, stream_id, key, frame, lane, reason);
}

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

pub(super) struct ServerTcpStreamRegistry {
    streams: Mutex<HashMap<(SessionId, StreamId), ServerTcpStreamEntry>>,
    path_metrics: Mutex<HashMap<(SessionId, UnderlayProtocol, PathId), ServerPathMetricsEntry>>,
    closed_streams: Mutex<RecentIdCache<(SessionId, StreamId)>>,
    lane_tracker: Arc<ServerPathLaneTracker>,
}

impl std::fmt::Debug for ServerTcpStreamRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerTcpStreamRegistry")
            .finish_non_exhaustive()
    }
}

struct ServerTcpStreamEntry {
    target: TargetAddr,
    lane: FlowLane,
    frames: mpsc::Sender<Result<Frame, RuntimeError>>,
    binding: Arc<ServerTcpStreamBinding>,
}

pub(super) struct ServerTcpPathAttachment {
    pub(super) path_id: PathId,
    pub(super) underlay: UnderlayProtocol,
    pub(super) commands: TcpPathSessionCommandSender,
    pub(super) max_frame_payload_bytes: usize,
    pub(super) role: StreamOpenRole,
}

pub(super) struct ServerTcpStreamOpenRequest<'a> {
    pub(super) session_id: SessionId,
    pub(super) stream_id: StreamId,
    pub(super) target: &'a TargetAddr,
    pub(super) lane: FlowLane,
    pub(super) attachment: ServerTcpPathAttachment,
}

pub(super) enum ServerTcpStreamOpen {
    New(TcpPathStream),
    Existing,
}

pub(super) struct ServerTcpRegistryManagementSnapshot {
    pub(super) active_streams: usize,
    pub(super) path_metrics: Vec<ServerTcpPathMetricSnapshot>,
}

pub(super) struct ServerTcpPathMetricSnapshot {
    pub(super) session_id: SessionId,
    pub(super) underlay: UnderlayProtocol,
    pub(super) path_id: PathId,
    pub(super) metrics: PathMetrics,
    pub(super) source: &'static str,
}

impl ServerTcpStreamRegistry {
    pub(super) fn new(max_streams: usize) -> Self {
        Self {
            streams: Mutex::new(HashMap::new()),
            path_metrics: Mutex::new(HashMap::new()),
            closed_streams: Mutex::new(RecentIdCache::new(tcp_closed_stream_cache_capacity(
                max_streams,
            ))),
            lane_tracker: Arc::new(ServerPathLaneTracker::default()),
        }
    }

    pub(super) fn management_snapshot(&self) -> ServerTcpRegistryManagementSnapshot {
        let active_streams = self.streams.lock().expect("server stream lock").len();
        let path_metrics = self
            .path_metrics
            .lock()
            .expect("server path metrics lock")
            .iter()
            .map(
                |((session_id, underlay, path_id), entry)| ServerTcpPathMetricSnapshot {
                    session_id: *session_id,
                    underlay: *underlay,
                    path_id: *path_id,
                    metrics: entry.metrics,
                    source: match entry.source {
                        ServerPathMetricsSource::PeerHint => "peer_hint",
                        ServerPathMetricsSource::LocalSender => "local_sender",
                    },
                },
            )
            .collect();
        ServerTcpRegistryManagementSnapshot {
            active_streams,
            path_metrics,
        }
    }

    pub(super) fn open_or_attach(
        &self,
        request: ServerTcpStreamOpenRequest<'_>,
        mux_limits: MuxLimits,
        max_streams: usize,
    ) -> Result<ServerTcpStreamOpen, RuntimeError> {
        let ServerTcpStreamOpenRequest {
            session_id,
            stream_id,
            target,
            lane,
            attachment,
        } = request;
        let max_frame_payload_bytes = attachment.max_frame_payload_bytes;
        let underlay = attachment.underlay;
        let path_id = attachment.path_id;
        let role = attachment.role;
        let initial_metrics = self.stored_path_metrics(session_id, underlay, path_id);
        let mut streams = self
            .streams
            .lock()
            .expect("server TCP stream registry lock");
        if let Some(entry) = streams.get_mut(&(session_id, stream_id)) {
            if entry.target != *target {
                return Err(RuntimeError::Protocol(
                    "TCP stream migration target does not match original stream",
                ));
            }
            entry.lane = lane;
            entry.binding.attach(
                underlay,
                path_id,
                attachment.commands,
                lane,
                role,
                max_frame_payload_bytes,
            );
            if let Some(metrics) = initial_metrics {
                entry.binding.update_path_metrics(
                    ServerTcpPathKey { underlay, path_id },
                    metrics.metrics,
                    metrics.source,
                );
            }
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "server_stream_open",
                format_args!(
                    "session_id={} stream_id={} path_underlay={:?} path_id={} role={:?} lane={:?} result=existing",
                    session_id.0, stream_id.0, underlay, path_id.0, role, lane,
                ),
            );
            return Ok(ServerTcpStreamOpen::Existing);
        }

        if streams.len() >= max_streams {
            return Err(RuntimeError::Protocol("server TCP stream limit reached"));
        }

        let (frames_tx, frames_rx) = mpsc::channel(tcp_stream_frame_queue_for_payload(
            mux_limits,
            max_frame_payload_bytes,
        ));
        let binding = ServerTcpStreamBinding::new_with_limits_and_tracker(
            session_id,
            underlay,
            path_id,
            attachment.commands,
            lane,
            mux_limits,
            self.lane_tracker.clone(),
        );
        if let Some(metrics) = initial_metrics {
            binding.update_path_metrics(
                ServerTcpPathKey { underlay, path_id },
                metrics.metrics,
                metrics.source,
            );
        }
        streams.insert(
            (session_id, stream_id),
            ServerTcpStreamEntry {
                target: target.clone(),
                lane,
                frames: frames_tx,
                binding: binding.clone(),
            },
        );
        #[cfg(feature = "lab-diagnostics")]
        lab_diagnostic(
            "server_stream_open",
            format_args!(
                "session_id={} stream_id={} path_underlay={:?} path_id={} role={:?} lane={:?} result=new",
                session_id.0, stream_id.0, underlay, path_id.0, role, lane,
            ),
        );
        Ok(ServerTcpStreamOpen::New(TcpPathStream {
            stream_id,
            max_offset: mux_limits.max_stream_window_bytes,
            lane,
            underlay,
            max_frame_payload_bytes,
            output: TcpPathStreamOutput::Switchable(binding),
            frames: frames_rx,
        }))
    }

    pub(super) fn record_path_metrics(
        &self,
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        metrics: PathMetrics,
    ) {
        self.record_path_metrics_with_source(
            session_id,
            underlay,
            path_id,
            metrics,
            ServerPathMetricsSource::PeerHint,
        );
    }

    pub(super) fn record_local_path_metrics(
        &self,
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        metrics: PathMetrics,
    ) {
        self.record_path_metrics_with_source(
            session_id,
            underlay,
            path_id,
            metrics,
            ServerPathMetricsSource::LocalSender,
        );
    }

    fn record_path_metrics_with_source(
        &self,
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        metrics: PathMetrics,
        source: ServerPathMetricsSource,
    ) {
        let metrics = PathMetrics { path_id, ..metrics };
        let entry = ServerPathMetricsEntry { metrics, source };
        self.path_metrics
            .lock()
            .expect("server path metrics lock")
            .insert((session_id, underlay, path_id), entry);
        let bindings = {
            let streams = self
                .streams
                .lock()
                .expect("server TCP stream registry lock");
            streams
                .iter()
                .filter_map(|((entry_session_id, _), entry)| {
                    (*entry_session_id == session_id).then_some(entry.binding.clone())
                })
                .collect::<Vec<_>>()
        };
        let key = ServerTcpPathKey { underlay, path_id };
        for binding in bindings {
            binding.update_path_metrics(key, metrics, source);
        }
    }

    fn stored_path_metrics(
        &self,
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
    ) -> Option<ServerPathMetricsEntry> {
        self.path_metrics
            .lock()
            .expect("server path metrics lock")
            .get(&(session_id, underlay, path_id))
            .copied()
    }

    pub(super) fn detach_path(
        &self,
        session_id: SessionId,
        stream_id: StreamId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: &TcpPathSessionCommandSender,
    ) {
        if let Some(binding) = self
            .streams
            .lock()
            .expect("server TCP stream registry lock")
            .get(&(session_id, stream_id))
            .map(|entry| entry.binding.clone())
        {
            binding.detach(ServerTcpPathKey { underlay, path_id }, commands);
        }
    }

    pub(super) async fn route_frame(
        &self,
        session_id: SessionId,
        stream_id: StreamId,
        frame: Frame,
    ) -> Result<(), RuntimeError> {
        #[cfg(feature = "lab-diagnostics")]
        let bytes = frame_pacing_bytes(&frame);
        let stream = {
            let streams = self
                .streams
                .lock()
                .expect("server TCP stream registry lock");
            streams
                .get(&(session_id, stream_id))
                .map(|entry| entry.frames.clone())
        };
        let Some(stream) = stream else {
            let closed_key = (session_id, stream_id);
            if self
                .closed_streams
                .lock()
                .expect("server TCP stream closed cache lock")
                .contains(&closed_key)
            {
                return Ok(());
            }
            return Err(RuntimeError::Protocol(
                "frame for unknown server TCP stream",
            ));
        };
        #[cfg(feature = "lab-diagnostics")]
        let started = Instant::now();
        let result = stream
            .send(Ok(frame))
            .await
            .map_err(|_| RuntimeError::TcpPathSessionClosed);
        #[cfg(feature = "lab-diagnostics")]
        lab_perf_record(
            "runtime.server_stream.route_frame",
            started.elapsed(),
            bytes,
        );
        result
    }

    pub(super) fn close(&self, session_id: SessionId, stream_id: StreamId) {
        let removed = self
            .streams
            .lock()
            .expect("server TCP stream registry lock")
            .remove(&(session_id, stream_id))
            .is_some();
        if removed {
            self.closed_streams
                .lock()
                .expect("server TCP stream closed cache lock")
                .insert((session_id, stream_id));
        }
    }
}

impl Default for ServerTcpStreamRegistry {
    fn default() -> Self {
        Self::new(ResourceLimits::default().max_streams)
    }
}

pub(super) struct ClientTcpPathSessionHandle {
    runtime: ClientTcpPathSessionRuntime,
    commands: Arc<Mutex<Option<TcpPathSessionCommandSender>>>,
    latency_commands: Arc<Mutex<Option<TcpPathSessionCommandSender>>>,
}

impl std::fmt::Debug for ClientTcpPathSessionHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientTcpPathSessionHandle")
            .finish_non_exhaustive()
    }
}

impl Clone for ClientTcpPathSessionHandle {
    fn clone(&self) -> Self {
        Self {
            runtime: self.runtime.clone(),
            commands: self.commands.clone(),
            latency_commands: self.latency_commands.clone(),
        }
    }
}

impl ClientTcpPathSessionHandle {
    pub(super) fn new(runtime: ClientTcpPathSessionRuntime) -> Self {
        Self {
            runtime,
            commands: Arc::new(Mutex::new(None)),
            latency_commands: Arc::new(Mutex::new(None)),
        }
    }

    #[cfg(test)]
    pub(super) fn session_id(&self) -> SessionId {
        self.runtime.session_id
    }

    pub(super) async fn open_stream(
        &self,
        stream_id: StreamId,
        target: TargetAddr,
        ingress: IngressKind,
        lane: FlowLane,
        role: StreamOpenRole,
    ) -> Result<TcpPathStream, RuntimeError> {
        let commands = self.ensure_session(lane);
        let (response_tx, response_rx) = oneshot::channel();
        commands
            .send_control(TcpPathSessionCommand::OpenStream {
                stream_id,
                target,
                ingress,
                lane,
                role,
                session_commands: commands.clone(),
                response: response_tx,
            })
            .await
            .map_err(|_| RuntimeError::TcpPathSessionClosed)?;
        response_rx
            .await
            .map_err(|_| RuntimeError::TcpPathSessionClosed)?
    }

    pub(super) async fn cancel_stream_open(&self, lane: FlowLane, stream_id: StreamId) {
        let commands =
            if tcp_path_lane_uses_dedicated_session(lane) && !self.runtime.reuse_latency_session {
                None
            } else if tcp_path_lane_uses_dedicated_session(lane) {
                self.latency_commands
                    .lock()
                    .expect("TCP path session lock")
                    .clone()
            } else {
                self.commands.lock().expect("TCP path session lock").clone()
            };
        if let Some(commands) = commands
            && !commands.is_closed()
        {
            let _ = commands
                .send_control(TcpPathSessionCommand::CloseStream(stream_id))
                .await;
        }
    }

    pub(super) fn ensure_session(&self, lane: FlowLane) -> TcpPathSessionCommandSender {
        if tcp_path_lane_uses_dedicated_session(lane) && !self.runtime.reuse_latency_session {
            let (commands, receivers) =
                tcp_path_session_command_channels(self.runtime.command_queue);
            tokio::spawn(run_client_tcp_path_session(self.runtime.clone(), receivers));
            return commands;
        }

        let lane = if tcp_path_lane_uses_dedicated_session(lane) {
            &self.latency_commands
        } else {
            &self.commands
        };
        let mut current = lane.lock().expect("TCP path session lock");
        if let Some(commands) = current.as_ref()
            && !commands.is_closed()
        {
            return commands.clone();
        }

        let (commands, receivers) = tcp_path_session_command_channels(self.runtime.command_queue);
        tokio::spawn(run_client_tcp_path_session(self.runtime.clone(), receivers));
        *current = Some(commands.clone());
        commands
    }
}

pub(super) fn tcp_path_lane_uses_dedicated_session(lane: FlowLane) -> bool {
    matches!(
        lane,
        FlowLane::Control | FlowLane::Latency | FlowLane::RealtimeDatagram
    )
}

pub(super) struct ClientTcpPathConnection {
    pub(super) writer: EncryptedTcpWriter,
    pub(super) frames: mpsc::Receiver<Result<Frame, EncryptedFramedTransportError>>,
    pub(super) heartbeat_interval: Duration,
    pub(super) next_heartbeat_at: tokio::time::Instant,
    pub(super) pending_heartbeat: Option<(u64, tokio::time::Instant)>,
}

pub(super) type EncryptedTcpReader = EncryptedFramedReader<tokio::io::ReadHalf<TcpStream>>;
pub(super) type EncryptedTcpWriter = EncryptedFramedWriter<tokio::io::WriteHalf<TcpStream>>;

pub(super) struct ClientTcpPathStreamState {
    pub(super) frames: mpsc::Sender<Result<Frame, RuntimeError>>,
    pub(super) pending_open: Option<ClientTcpPendingOpen>,
}

pub(super) struct ClientTcpPendingOpen {
    response: oneshot::Sender<Result<TcpPathStream, RuntimeError>>,
    frames: Option<mpsc::Receiver<Result<Frame, RuntimeError>>>,
    session_commands: TcpPathSessionCommandSender,
    lane: FlowLane,
}

#[derive(Clone)]
pub(super) struct ClientTcpPathSessionRuntime {
    pub(super) path: PathSpec,
    pub(super) path_index: usize,
    pub(super) session_id: SessionId,
    pub(super) security: SecurityConfig,
    pub(super) codec_limits: CodecLimits,
    pub(super) mux_limits: MuxLimits,
    pub(super) command_queue: usize,
    pub(super) stream_frame_queue: usize,
    pub(super) closed_stream_cache_capacity: usize,
    pub(super) reuse_latency_session: bool,
}

struct ClientTcpPathSessionState {
    connection: Option<ClientTcpPathConnection>,
    streams: HashMap<StreamId, ClientTcpPathStreamState>,
    closed_streams: RecentIdCache<StreamId>,
}

struct ClientTcpOpenStreamRequest {
    stream_id: StreamId,
    target: TargetAddr,
    ingress: IngressKind,
    lane: FlowLane,
    role: StreamOpenRole,
    session_commands: TcpPathSessionCommandSender,
    response: oneshot::Sender<Result<TcpPathStream, RuntimeError>>,
}

async fn run_client_tcp_path_session(
    runtime: ClientTcpPathSessionRuntime,
    mut commands: TcpPathSessionCommandReceivers,
) {
    let mut state = ClientTcpPathSessionState {
        connection: None,
        streams: HashMap::new(),
        closed_streams: RecentIdCache::new(runtime.closed_stream_cache_capacity),
    };

    loop {
        if state.connection.is_none() {
            match recv_tcp_path_command(&mut commands).await {
                Some(command) => {
                    handle_disconnected_client_tcp_command(command, &runtime, &mut state).await;
                }
                None => return,
            }
            continue;
        }

        let heartbeat_at = {
            let connection_ref = state
                .connection
                .as_ref()
                .expect("checked connected TCP path session");
            connection_ref
                .pending_heartbeat
                .as_ref()
                .map(|(_, deadline)| *deadline)
                .unwrap_or(connection_ref.next_heartbeat_at)
        };
        let heartbeat_timer = tokio::time::sleep_until(heartbeat_at);
        tokio::pin!(heartbeat_timer);

        let command_may_recv = !tcp_path_receivers_closed(&commands);
        if !command_may_recv {
            if let Some(connection_ref) = state.connection.as_mut() {
                let _ = close_client_tcp_path(
                    connection_ref,
                    PathId(runtime.path_index as u16),
                    !state.streams.is_empty(),
                )
                .await;
            }
            return;
        }

        let mut drop_connection = false;
        tokio::select! {
            biased;
            command = recv_tcp_path_command(&mut commands), if command_may_recv => {
                match command {
                    Some(command) => {
                        if let Err(err) = handle_connected_client_tcp_command(
                            command,
                            state.connection.as_mut().expect("checked connected TCP path session"),
                            &mut state.streams,
                            &mut state.closed_streams,
                            runtime.stream_frame_queue,
                            runtime.mux_limits,
                        )
                        .await
                        {
                            fail_client_tcp_streams(&mut state.streams, &err);
                            eprintln!("warning: TCP path session command failed: {err}");
                            drop_connection = true;
                        }
                    }
                    None => {
                        if tcp_path_receivers_closed(&commands) {
                            if let Some(connection_ref) = state.connection.as_mut() {
                                let _ = close_client_tcp_path(
                                    connection_ref,
                                    PathId(runtime.path_index as u16),
                                    !state.streams.is_empty(),
                                )
                                .await;
                            }
                            return;
                        }
                    }
                }
            }
            frame = state.connection.as_mut().expect("checked connected TCP path session").frames.recv() => {
                match frame {
                    Some(Ok(frame)) => {
                        if let Err(err) = handle_client_tcp_path_frame(
                            frame,
                            state.connection.as_mut().expect("checked connected TCP path session"),
                            &mut state.streams,
                            &mut state.closed_streams,
                            runtime.mux_limits,
                        )
                        .await
                        {
                            fail_client_tcp_streams(&mut state.streams, &err);
                            eprintln!("warning: TCP path session frame handling failed: {err}");
                            drop_connection = true;
                        }
                    }
                    Some(Err(err)) => {
                        let err = RuntimeError::Encrypted(err);
                        fail_client_tcp_streams(&mut state.streams, &err);
                        eprintln!("warning: TCP path session read failed: {err}");
                        drop_connection = true;
                    }
                    None => {
                        let err = RuntimeError::TcpPathSessionClosed;
                        fail_client_tcp_streams(&mut state.streams, &err);
                        drop_connection = true;
                    }
                }
            }
            _ = &mut heartbeat_timer => {
                if let Err(err) = tick_client_tcp_path_heartbeat(
                    state.connection.as_mut().expect("checked connected TCP path session"),
                    runtime.mux_limits,
                    !state.streams.is_empty(),
                )
                .await
                {
                    fail_client_tcp_streams(&mut state.streams, &err);
                    eprintln!("warning: TCP path heartbeat failed: {err}");
                    drop_connection = true;
                }
            }
        }

        if drop_connection {
            state.connection = None;
        }
    }
}

async fn handle_disconnected_client_tcp_command(
    command: TcpPathSessionCommand,
    runtime: &ClientTcpPathSessionRuntime,
    state: &mut ClientTcpPathSessionState,
) {
    match command {
        TcpPathSessionCommand::OpenStream {
            stream_id,
            target,
            ingress,
            lane,
            role,
            session_commands,
            response,
        } => match connect_client_tcp_path(
            &runtime.path,
            runtime.path_index,
            runtime.session_id,
            &runtime.security,
            runtime.codec_limits,
            runtime.mux_limits,
        )
        .await
        {
            Ok(mut connected) => {
                let open = ClientTcpOpenStreamRequest {
                    stream_id,
                    target,
                    ingress,
                    lane,
                    role,
                    session_commands,
                    response,
                };
                let result = open_client_tcp_stream_on_connection(
                    &mut connected,
                    open,
                    &mut state.streams,
                    runtime.stream_frame_queue,
                )
                .await;
                if result.is_ok() {
                    state.connection = Some(connected);
                } else if let Err(err) = result {
                    eprintln!("warning: TCP stream open on new path session failed: {err}");
                    fail_client_tcp_streams(&mut state.streams, &err);
                }
            }
            Err(err) => {
                let _ = response.send(Err(err));
            }
        },
        TcpPathSessionCommand::SendFrame(_) | TcpPathSessionCommand::CloseStream(_) => {}
    }
}

async fn handle_connected_client_tcp_command(
    command: TcpPathSessionCommand,
    connection: &mut ClientTcpPathConnection,
    streams: &mut HashMap<StreamId, ClientTcpPathStreamState>,
    closed_streams: &mut RecentIdCache<StreamId>,
    stream_frame_queue: usize,
    mux_limits: MuxLimits,
) -> Result<(), RuntimeError> {
    match command {
        TcpPathSessionCommand::OpenStream {
            stream_id,
            target,
            ingress,
            lane,
            role,
            session_commands,
            response,
        } => {
            let open = ClientTcpOpenStreamRequest {
                stream_id,
                target,
                ingress,
                lane,
                role,
                session_commands,
                response,
            };
            open_client_tcp_stream_on_connection(connection, open, streams, stream_frame_queue)
                .await?;
            record_client_tcp_path_outbound_activity(connection, mux_limits);
            Ok(())
        }
        TcpPathSessionCommand::SendFrame(frame) => {
            connection.writer.write_frame(&frame).await?;
            connection.writer.flush().await?;
            record_client_tcp_path_outbound_activity(connection, mux_limits);
            Ok(())
        }
        TcpPathSessionCommand::CloseStream(stream_id) => {
            if streams.remove(&stream_id).is_some() {
                closed_streams.insert(stream_id);
            }
            Ok(())
        }
    }
}

pub(super) async fn connect_client_tcp_path(
    path: &PathSpec,
    path_index: usize,
    session_id: SessionId,
    security: &SecurityConfig,
    codec_limits: CodecLimits,
    mux_limits: MuxLimits,
) -> Result<ClientTcpPathConnection, RuntimeError> {
    let tcp_stream = tcp::connect_path(path, TcpConnectOptions::default()).await?;
    let mut framed = EncryptedFramedStream::with_cipher_suite(
        tcp_stream,
        security.secret.as_bytes(),
        PeerRole::Client,
        codec_limits,
        security.cipher,
    );
    let path_id = PathId(path_index as u16);
    let (session_hello, session_auth, path_join) = authenticated_path_join_frames_for_session(
        security,
        path,
        path_id,
        UnderlayProtocol::Tcp,
        session_id,
    )?;

    framed.write_frame(&session_hello).await?;
    framed.write_frame(&session_auth).await?;
    framed.write_frame(&path_join).await?;
    framed.flush().await?;

    let mut session_ready = false;
    let mut path_active = false;
    while !session_ready || !path_active {
        match framed.read_frame().await? {
            Frame::SessionReady => session_ready = true,
            Frame::PathStatus {
                status: crate::protocol::PathStatus::Active,
                ..
            } => path_active = true,
            Frame::PathStatus { .. } => {
                return Err(RuntimeError::Protocol(
                    "TCP path session did not become active",
                ));
            }
            Frame::SessionClose { reason } => return Err(RuntimeError::RemoteClosed(reason)),
            _ => {
                return Err(RuntimeError::Protocol(
                    "unexpected TCP path handshake frame",
                ));
            }
        }
    }

    let (reader, writer) = framed.split();
    let now = tokio::time::Instant::now();
    Ok(ClientTcpPathConnection {
        writer,
        frames: spawn_encrypted_tcp_reader(reader, tcp_path_session_frame_queue(mux_limits)),
        heartbeat_interval: mux_limits.tcp_path_heartbeat_interval,
        next_heartbeat_at: now + mux_limits.tcp_path_heartbeat_interval,
        pending_heartbeat: None,
    })
}

async fn open_client_tcp_stream_on_connection(
    connection: &mut ClientTcpPathConnection,
    open: ClientTcpOpenStreamRequest,
    streams: &mut HashMap<StreamId, ClientTcpPathStreamState>,
    stream_frame_queue: usize,
) -> Result<(), RuntimeError> {
    let stream_id = open.stream_id;
    let (frames_tx, frames_rx) = mpsc::channel(stream_frame_queue);
    streams.insert(
        stream_id,
        ClientTcpPathStreamState {
            frames: frames_tx,
            pending_open: Some(ClientTcpPendingOpen {
                response: open.response,
                frames: Some(frames_rx),
                session_commands: open.session_commands,
                lane: open.lane,
            }),
        },
    );
    connection
        .writer
        .write_frame(&Frame::OpenStream {
            stream_id,
            target: open.target,
            ingress: open.ingress,
            outbound: OutboundPolicy::Direct,
            demand: stream_demand_hint_for_lane(open.lane),
            role: open.role,
        })
        .await?;
    connection.writer.flush().await?;
    connection.next_heartbeat_at = tokio::time::Instant::now() + connection.heartbeat_interval;
    Ok(())
}

async fn handle_client_tcp_path_frame(
    frame: Frame,
    connection: &mut ClientTcpPathConnection,
    streams: &mut HashMap<StreamId, ClientTcpPathStreamState>,
    closed_streams: &mut RecentIdCache<StreamId>,
    mux_limits: MuxLimits,
) -> Result<(), RuntimeError> {
    refresh_client_tcp_path_liveness(connection, mux_limits);
    match frame {
        Frame::StreamMaxData {
            stream_id,
            max_offset,
        } => {
            if let Some(state) = streams.get_mut(&stream_id)
                && let Some(mut pending) = state.pending_open.take()
            {
                let frames = pending
                    .frames
                    .take()
                    .ok_or(RuntimeError::Protocol("missing TCP stream frame receiver"))?;
                let stream = TcpPathStream {
                    stream_id,
                    max_offset,
                    lane: pending.lane,
                    underlay: UnderlayProtocol::Tcp,
                    max_frame_payload_bytes: tcp_relay_buffer_len(mux_limits),
                    output: TcpPathStreamOutput::Fixed(pending.session_commands),
                    frames,
                };
                let _ = pending.response.send(Ok(stream));
                return Ok(());
            }
            route_client_tcp_stream_frame(
                streams,
                closed_streams,
                stream_id,
                Frame::StreamMaxData {
                    stream_id,
                    max_offset,
                },
            )
            .await
        }
        Frame::StreamReset { stream_id, reason } => {
            if let Some(mut state) = streams.remove(&stream_id)
                && let Some(pending) = state.pending_open.take()
            {
                closed_streams.insert(stream_id);
                let _ = pending
                    .response
                    .send(Err(RuntimeError::RemoteReset(reason)));
                return Ok(());
            }
            route_client_tcp_stream_frame(
                streams,
                closed_streams,
                stream_id,
                Frame::StreamReset { stream_id, reason },
            )
            .await
        }
        Frame::StreamData {
            stream_id,
            offset,
            flags,
            payload,
        } => {
            route_client_tcp_stream_frame(
                streams,
                closed_streams,
                stream_id,
                Frame::StreamData {
                    stream_id,
                    offset,
                    flags,
                    payload,
                },
            )
            .await
        }
        Frame::StreamAck {
            stream_id,
            complete,
            ranges,
        } => {
            route_client_tcp_stream_frame(
                streams,
                closed_streams,
                stream_id,
                Frame::StreamAck {
                    stream_id,
                    complete,
                    ranges,
                },
            )
            .await
        }
        Frame::StreamFin {
            stream_id,
            final_offset,
        } => {
            route_client_tcp_stream_frame(
                streams,
                closed_streams,
                stream_id,
                Frame::StreamFin {
                    stream_id,
                    final_offset,
                },
            )
            .await
        }
        Frame::Ping { nonce } => {
            connection
                .writer
                .write_frame(&Frame::Pong { nonce })
                .await?;
            connection.writer.flush().await?;
            Ok(())
        }
        Frame::Pong { nonce } => {
            let Some((pending_nonce, _)) = connection.pending_heartbeat.as_ref() else {
                return Err(RuntimeError::Protocol(
                    "unexpected TCP path heartbeat response",
                ));
            };
            if *pending_nonce != nonce {
                return Err(RuntimeError::Protocol(
                    "unexpected TCP path heartbeat response",
                ));
            }
            connection.pending_heartbeat = None;
            connection.next_heartbeat_at =
                tokio::time::Instant::now() + connection.heartbeat_interval;
            Ok(())
        }
        Frame::PathStatus {
            status: crate::protocol::PathStatus::Draining | crate::protocol::PathStatus::Failed,
            ..
        } => Err(RuntimeError::TcpPathSessionClosed),
        Frame::PathStatus { .. } => Ok(()),
        Frame::SessionClose { reason } => Err(RuntimeError::RemoteClosed(reason)),
        Frame::PathDrain { .. } | Frame::PathClose { .. } => {
            Err(RuntimeError::TcpPathSessionClosed)
        }
        _ => Err(RuntimeError::Protocol("unexpected TCP path session frame")),
    }
}

pub(super) fn refresh_client_tcp_path_liveness(
    connection: &mut ClientTcpPathConnection,
    mux_limits: MuxLimits,
) {
    refresh_client_tcp_path_liveness_state(
        &mut connection.next_heartbeat_at,
        connection.heartbeat_interval,
        &mut connection.pending_heartbeat,
        mux_limits.tcp_path_heartbeat_timeout,
    );
}

fn record_client_tcp_path_outbound_activity(
    connection: &mut ClientTcpPathConnection,
    mux_limits: MuxLimits,
) {
    refresh_client_tcp_path_liveness(connection, mux_limits);
}

pub(super) fn refresh_client_tcp_path_liveness_state(
    next_heartbeat_at: &mut tokio::time::Instant,
    heartbeat_interval: Duration,
    pending_heartbeat: &mut Option<(u64, tokio::time::Instant)>,
    heartbeat_timeout: Duration,
) {
    let now = tokio::time::Instant::now();
    *next_heartbeat_at = now + heartbeat_interval;
    if let Some((_, deadline)) = pending_heartbeat.as_mut() {
        *deadline = now + heartbeat_timeout;
    }
}

pub(super) async fn route_client_tcp_stream_frame(
    streams: &mut HashMap<StreamId, ClientTcpPathStreamState>,
    closed_streams: &mut RecentIdCache<StreamId>,
    stream_id: StreamId,
    frame: Frame,
) -> Result<(), RuntimeError> {
    let Some(state) = streams.get_mut(&stream_id) else {
        if closed_streams.contains(&stream_id) {
            return Ok(());
        }
        return Err(RuntimeError::Protocol("frame for unknown TCP stream"));
    };
    #[cfg(feature = "lab-diagnostics")]
    let bytes = frame_pacing_bytes(&frame);
    #[cfg(feature = "lab-diagnostics")]
    let started = Instant::now();
    if state.frames.send(Ok(frame)).await.is_err() {
        streams.remove(&stream_id);
        closed_streams.insert(stream_id);
    }
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record("runtime.tcp_stream.route_frame", started.elapsed(), bytes);
    Ok(())
}

pub(super) async fn tick_client_tcp_path_heartbeat(
    connection: &mut ClientTcpPathConnection,
    mux_limits: MuxLimits,
    has_active_streams: bool,
) -> Result<(), RuntimeError> {
    let now = tokio::time::Instant::now();
    if let Some((_, deadline)) = connection.pending_heartbeat.as_ref()
        && now >= *deadline
    {
        if has_active_streams {
            connection.pending_heartbeat = None;
            connection.next_heartbeat_at = now + connection.heartbeat_interval;
            return Ok(());
        }
        return Err(RuntimeError::PathHeartbeatTimeout);
    }
    if connection.pending_heartbeat.is_none() && now >= connection.next_heartbeat_at {
        let nonce = random_u64()?;
        connection
            .writer
            .write_frame(&Frame::Ping { nonce })
            .await?;
        connection.writer.flush().await?;
        connection.pending_heartbeat = Some((nonce, now + mux_limits.tcp_path_heartbeat_timeout));
    }
    Ok(())
}

pub(super) async fn close_client_tcp_path(
    connection: &mut ClientTcpPathConnection,
    path_id: PathId,
    drain: bool,
) -> Result<(), RuntimeError> {
    if drain {
        connection
            .writer
            .write_frame(&Frame::PathDrain { path_id })
            .await?;
    }
    connection
        .writer
        .write_frame(&Frame::PathClose {
            path_id,
            reason: CloseReason::Normal,
        })
        .await?;
    connection
        .writer
        .write_frame(&Frame::SessionClose {
            reason: CloseReason::Normal,
        })
        .await?;
    connection.writer.flush().await?;
    Ok(())
}

fn fail_client_tcp_streams(
    streams: &mut HashMap<StreamId, ClientTcpPathStreamState>,
    reason: &RuntimeError,
) {
    for (_, mut state) in streams.drain() {
        if let Some(pending) = state.pending_open.take() {
            let _ = pending.response.send(Err(tcp_path_stream_error(reason)));
        } else {
            let _ = state.frames.try_send(Err(tcp_path_stream_error(reason)));
        }
    }
}

fn tcp_path_stream_error(reason: &RuntimeError) -> RuntimeError {
    match reason {
        RuntimeError::PathHeartbeatTimeout => RuntimeError::PathHeartbeatTimeout,
        RuntimeError::PathOpenTimedOut => RuntimeError::PathOpenTimedOut,
        RuntimeError::TcpPathSessionClosed => RuntimeError::TcpPathSessionClosed,
        RuntimeError::RemoteReset(reason) => RuntimeError::RemoteReset(*reason),
        RuntimeError::RemoteClosed(reason) => RuntimeError::RemoteClosed(*reason),
        RuntimeError::Protocol(message) => RuntimeError::Protocol(message),
        _ => RuntimeError::TcpPathSessionClosed,
    }
}

pub(super) fn spawn_encrypted_tcp_reader(
    mut reader: EncryptedTcpReader,
    queue_size: usize,
) -> mpsc::Receiver<Result<Frame, EncryptedFramedTransportError>> {
    let (frames_tx, frames_rx) = mpsc::channel(queue_size);
    tokio::spawn(async move {
        loop {
            let frame = reader.read_frame().await;
            let done = frame.is_err();
            #[cfg(feature = "lab-diagnostics")]
            let bytes = frame.as_ref().ok().map(frame_pacing_bytes).unwrap_or(0);
            #[cfg(feature = "lab-diagnostics")]
            let started = Instant::now();
            let send_result = frames_tx.send(frame).await;
            #[cfg(feature = "lab-diagnostics")]
            lab_perf_record("runtime.tcp_reader.queue_send", started.elapsed(), bytes);
            if send_result.is_err() || done {
                break;
            }
        }
    });
    frames_rx
}

pub(super) fn tcp_session_command_queue(resources: ResourceLimits) -> usize {
    tcp_path_command_queue(resources.into())
}

pub(super) fn tcp_path_command_queue(mux_limits: MuxLimits) -> usize {
    let frame_payload = mux_limits
        .max_tcp_relay_chunk_bytes
        .min(mux_limits.max_payload_bytes)
        .max(1);
    tcp_path_command_queue_for_payload(mux_limits, frame_payload)
}

pub(super) fn tcp_path_command_queue_for_payload(
    mux_limits: MuxLimits,
    frame_payload_bytes: usize,
) -> usize {
    let frame_payload = frame_payload_bytes.min(mux_limits.max_payload_bytes).max(1);
    let inflight_frames = mux_limits
        .max_tcp_path_inflight_bytes
        .saturating_add(frame_payload - 1)
        / frame_payload;
    inflight_frames.saturating_add(4).clamp(
        4,
        tcp_path_session_frame_queue_for_payload(mux_limits, frame_payload).max(4),
    )
}

pub(super) fn tcp_path_session_frame_queue(mux_limits: MuxLimits) -> usize {
    let frame_payload = mux_limits
        .max_tcp_relay_chunk_bytes
        .min(mux_limits.max_payload_bytes)
        .max(1);
    tcp_path_session_frame_queue_for_payload(mux_limits, frame_payload)
}

pub(super) fn tcp_path_session_frame_queue_for_payload(
    mux_limits: MuxLimits,
    frame_payload_bytes: usize,
) -> usize {
    tcp_stream_frame_queue_for_payload(mux_limits, frame_payload_bytes)
        .saturating_mul(4)
        .clamp(16, 4096)
}

pub(super) fn tcp_stream_frame_queue(mux_limits: MuxLimits) -> usize {
    let frame_payload = mux_limits
        .max_tcp_relay_chunk_bytes
        .min(mux_limits.max_payload_bytes)
        .max(1);
    tcp_stream_frame_queue_for_payload(mux_limits, frame_payload)
}

pub(super) fn tcp_stream_frame_queue_for_payload(
    mux_limits: MuxLimits,
    frame_payload_bytes: usize,
) -> usize {
    let frame_payload = frame_payload_bytes.min(mux_limits.max_payload_bytes).max(1);
    (mux_limits.max_reorder_bytes / frame_payload)
        .saturating_add(4)
        .clamp(4, 1024)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_stream_fin_is_control_not_current_data_path() {
        let frame = Frame::StreamFin {
            stream_id: StreamId(1),
            final_offset: 64,
        };

        assert!(!server_frame_prefers_current_data_path(
            &frame,
            FlowLane::Throughput
        ));
    }

    #[test]
    fn control_and_ack_frames_never_use_throughput_lane() {
        let priority_frames = [
            (
                Frame::StreamAck {
                    stream_id: StreamId(1),
                    complete: false,
                    ranges: vec![],
                },
                FlowLane::Control,
            ),
            (
                Frame::StreamMaxData {
                    stream_id: StreamId(1),
                    max_offset: 1024,
                },
                FlowLane::Control,
            ),
            (
                Frame::StreamFin {
                    stream_id: StreamId(1),
                    final_offset: 64,
                },
                FlowLane::Control,
            ),
            (
                Frame::StreamReset {
                    stream_id: StreamId(1),
                    reason: ResetReason::RemoteClosed,
                },
                FlowLane::Control,
            ),
            (
                Frame::StreamDetach {
                    stream_id: StreamId(1),
                },
                FlowLane::Control,
            ),
            (
                Frame::DatagramFeedback {
                    flow_id: DatagramFlowId(1),
                    received: vec![],
                },
                FlowLane::RealtimeDatagram,
            ),
            (
                Frame::DatagramClose {
                    flow_id: DatagramFlowId(1),
                },
                FlowLane::Control,
            ),
        ];

        for (frame, expected_lane) in priority_frames {
            let effective_lane = tcp_path_effective_frame_lane(&frame, FlowLane::Throughput);
            assert_eq!(effective_lane, expected_lane);
            assert!(tcp_path_frame_uses_priority_queue(effective_lane));
            if !matches!(frame, Frame::StreamFin { .. }) {
                assert!(!tcp_relay_frame_prefers_current_data_path(
                    &frame,
                    FlowLane::Throughput
                ));
            }
        }
    }
}
