use super::bulk_admission::{
    BulkAdmissionCheck, BulkAdmissionRole, bulk_additional_admission_role,
    bulk_candidate_admission_suppression, bulk_candidate_admission_suppression_with_ordering_debt,
};
use super::*;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

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

    pub(super) fn current_lane(&self, fallback: FlowLane) -> FlowLane {
        match self {
            Self::Fixed(_) => fallback,
            Self::Switchable(binding) => binding.lane(),
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
    outputs: Mutex<ServerTcpStreamOutputs>,
    flights: Mutex<BTreeMap<u64, Vec<ServerTcpPathFlight>>>,
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

    pub(super) fn new_with_limits(
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: TcpPathSessionCommandSender,
        lane: FlowLane,
        mux_limits: MuxLimits,
    ) -> Arc<Self> {
        let (version, _) = watch::channel(0);
        Arc::new(Self {
            session_id,
            lane: Mutex::new(lane),
            mux_limits,
            outputs: Mutex::new(ServerTcpStreamOutputs {
                next_index: 0,
                entries: vec![ServerTcpStreamOutputEntry {
                    key: ServerTcpPathKey { underlay, path_id },
                    commands,
                    bytes_in_flight: 0,
                    delivery_rate_bps: None,
                    delivery_samples: 0,
                    last_delivery_at: None,
                    validation_credit_bytes: 0,
                    path_metrics: None,
                }],
            }),
            flights: Mutex::new(BTreeMap::new()),
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
        *self.lane.lock().expect("server TCP stream lane lock") = lane;
        let mut outputs = self.outputs.lock().expect("server TCP stream binding lock");
        let key = ServerTcpPathKey { underlay, path_id };
        let mut was_active = false;
        let mut entry =
            if let Some(position) = outputs.entries.iter().position(|entry| entry.key == key) {
                was_active = position + 1 == outputs.entries.len();
                let mut entry = outputs.entries.remove(position);
                entry.commands = commands;
                entry
            } else {
                ServerTcpStreamOutputEntry {
                    key,
                    commands,
                    bytes_in_flight: 0,
                    delivery_rate_bps: None,
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
        outputs.next_index %= outputs.entries.len().max(1);
        drop(outputs);
        self.notify_update();
    }

    pub(super) fn lane(&self) -> FlowLane {
        *self.lane.lock().expect("server TCP stream lane lock")
    }

    pub(super) fn set_lane(&self, lane: FlowLane) {
        *self.lane.lock().expect("server TCP stream lane lock") = lane;
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
        let mut outputs = self.outputs.lock().expect("server TCP stream binding lock");
        let before = outputs.entries.len();
        outputs
            .entries
            .retain(|entry| entry.key != key || !entry.commands.same_channel(commands));
        if outputs.entries.len() != before {
            outputs.next_index %= outputs.entries.len().max(1);
            drop(outputs);
            self.notify_update();
        }
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
            return;
        }
        let mut released = Vec::new();
        for offset in acked_offsets {
            if let Some(path_flights) = flights.remove(&offset) {
                released.extend(path_flights);
            }
        }
        drop(flights);

        let mut outputs = self.outputs.lock().expect("server TCP stream binding lock");
        let now = Instant::now();
        let mut changed = false;
        for flight in released {
            if let Some(entry) = outputs
                .entries
                .iter_mut()
                .find(|entry| entry.key == flight.key)
            {
                entry.bytes_in_flight = entry.bytes_in_flight.saturating_sub(flight.bytes as u64);
                if flight.stream_ack_proves_path {
                    entry.delivery_samples = entry.delivery_samples.saturating_add(1);
                    entry.last_delivery_at = Some(now);
                    let elapsed = now.saturating_duration_since(flight.sent_at).as_secs_f64();
                    if elapsed > f64::EPSILON {
                        let sample = flight.bytes as f64 * 8.0 / elapsed;
                        entry.delivery_rate_bps = Some(match entry.delivery_rate_bps {
                            Some(previous) => previous.mul_add(0.75, sample * 0.25),
                            None => sample,
                        });
                    }
                }
                changed = true;
            }
        }
        drop(outputs);
        if changed {
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
                sent_at: Instant::now(),
                stream_ack_proves_path,
            });
    }

    fn lower_flights_before_frame(&self, frame: &Frame) -> Vec<ServerTcpPathFlightDebt> {
        let Some((offset, _, _)) = reliable_stream_frame_extent(frame) else {
            return Vec::new();
        };
        let flights = self.flights.lock().expect("server TCP stream flight lock");
        flights
            .range(..offset)
            .filter_map(|(_, path_flights)| {
                let latest = path_flights.last()?;
                Some(ServerTcpPathFlightDebt {
                    key: latest.key,
                    bytes: latest.bytes as u64,
                })
            })
            .collect()
    }

    pub(super) async fn close_stream(&self, stream_id: StreamId) {
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

#[derive(Clone)]
struct ServerTcpStreamOutputEntry {
    key: ServerTcpPathKey,
    commands: TcpPathSessionCommandSender,
    bytes_in_flight: u64,
    delivery_rate_bps: Option<f64>,
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
    sent_at: Instant,
    stream_ack_proves_path: bool,
}

#[derive(Debug, Clone, Copy)]
struct ServerTcpPathFlightDebt {
    key: ServerTcpPathKey,
    bytes: u64,
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

    fn bulk_commands(
        &mut self,
        payload_bytes: usize,
        mux_limits: MuxLimits,
        lower_flights: &[ServerTcpPathFlightDebt],
    ) -> Option<ServerTcpPathBulkChoice> {
        let active_key = self.entries.last().map(|entry| entry.key);
        let now = Instant::now();
        let normal_candidates = self
            .entries
            .iter()
            .enumerate()
            .filter(|(_, entry)| {
                Some(entry.key) == active_key || server_output_has_sender_evidence(entry)
            })
            .map(|(position, entry)| {
                let snapshot = server_bulk_output_snapshot(entry, mux_limits, now);
                let eta_ms =
                    server_bulk_output_eta_ms(entry.key, snapshot, active_key, payload_bytes);
                (position, eta_ms, snapshot)
            })
            .collect::<Vec<_>>();
        let lead_candidate = normal_candidates
            .iter()
            .min_by(|left, right| left.1.total_cmp(&right.1))
            .map(|(position, eta_ms, snapshot)| (self.entries[*position].key, *eta_ms, *snapshot));
        #[cfg(feature = "lab-diagnostics")]
        self.log_bulk_candidates(
            active_key,
            lead_candidate,
            payload_bytes,
            mux_limits,
            now,
            lower_flights,
        );
        let (position, primary_eta_ms, primary_snapshot) = normal_candidates
            .into_iter()
            .filter(|(position, eta_ms, snapshot)| {
                lead_candidate.is_some_and(|(lead_key, best_eta_ms, best_snapshot)| {
                    let key = self.entries[*position].key;
                    let role = if key == lead_key {
                        BulkAdmissionRole::ActiveDataPath
                    } else {
                        bulk_additional_admission_role(lead_key.underlay, key.underlay)
                    };
                    bulk_candidate_admission_suppression(
                        best_snapshot,
                        best_eta_ms,
                        *snapshot,
                        *eta_ms,
                        payload_bytes,
                        mux_limits,
                        role,
                    )
                    .or_else(|| {
                        let ordering_debt = server_stream_ordering_debt_bytes(
                            lower_flights,
                            self.entries[*position].key,
                        );
                        bulk_candidate_admission_suppression_with_ordering_debt(
                            BulkAdmissionCheck {
                                best_snapshot,
                                best_eta_ms,
                                candidate_snapshot: *snapshot,
                                candidate_eta_ms: *eta_ms,
                                payload_bytes,
                                mux_limits,
                                role,
                                stream_ordering_debt_bytes: ordering_debt,
                            },
                        )
                    })
                    .is_none()
                })
            })
            .min_by(|left, right| left.1.total_cmp(&right.1))?;
        let entry = self.entries[position].clone();
        #[cfg(feature = "lab-diagnostics")]
        let snapshot = server_bulk_output_snapshot(&entry, mux_limits, now);
        self.next_index = (position + 1) % self.entries.len().max(1);
        #[cfg(feature = "lab-diagnostics")]
        lab_diagnostic(
            "server_bulk_output_selected",
            format_args!(
                "path_underlay={:?} path_id={} reason=admitted payload_bytes={} delivery_samples={} validation_credit_bytes={} product_bytes_in_flight={} carrier_bytes_in_flight={} queue_bytes={} inflight_limit={}",
                entry.key.underlay,
                entry.key.path_id.0,
                payload_bytes,
                entry.delivery_samples,
                entry.validation_credit_bytes,
                entry.bytes_in_flight,
                snapshot.bytes_in_flight,
                snapshot.queue_bytes,
                snapshot.inflight_limit_bytes,
            ),
        );
        let validation_duplicate = self
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
                let validation_snapshot =
                    server_bulk_output_snapshot(validation, mux_limits, now);
                (
                    validation_position,
                    validation.key,
                    server_bulk_output_eta_ms(
                        validation.key,
                        validation_snapshot,
                        active_key,
                        payload_bytes,
                    ),
                    validation_snapshot,
                )
            })
            .filter(|(_, validation_key, validation_eta_ms, validation_snapshot)| {
                bulk_candidate_admission_suppression(
                    primary_snapshot,
                    primary_eta_ms,
                    *validation_snapshot,
                    *validation_eta_ms,
                    payload_bytes,
                    mux_limits,
                    bulk_additional_admission_role(entry.key.underlay, validation_key.underlay),
                )
                .or_else(|| {
                    let ordering_debt =
                        server_stream_ordering_debt_bytes(lower_flights, *validation_key);
                    bulk_candidate_admission_suppression_with_ordering_debt(
                        BulkAdmissionCheck {
                            best_snapshot: primary_snapshot,
                            best_eta_ms: primary_eta_ms,
                            candidate_snapshot: *validation_snapshot,
                            candidate_eta_ms: *validation_eta_ms,
                            payload_bytes,
                            mux_limits,
                            role: bulk_additional_admission_role(
                                entry.key.underlay,
                                validation_key.underlay,
                            ),
                            stream_ordering_debt_bytes: ordering_debt,
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
                    let validation_snapshot =
                        server_bulk_output_snapshot(&validation, mux_limits, now);
                    lab_diagnostic(
                        "server_bulk_output_selected",
                        format_args!(
                            "path_underlay={:?} path_id={} reason=validation_duplicate payload_bytes={} delivery_samples={} validation_credit_bytes={} product_bytes_in_flight={} carrier_bytes_in_flight={} queue_bytes={} inflight_limit={}",
                            validation.key.underlay,
                            validation.key.path_id.0,
                            payload_bytes,
                            validation.delivery_samples,
                            validation.validation_credit_bytes,
                            validation.bytes_in_flight,
                            validation_snapshot.bytes_in_flight,
                            validation_snapshot.queue_bytes,
                            validation_snapshot.inflight_limit_bytes,
                        ),
                    );
                }
                ServerTcpPathSendTarget {
                    key: validation.key,
                    commands: validation.commands,
                }
            });
        Some(ServerTcpPathBulkChoice {
            primary: ServerTcpPathSendTarget {
                key: entry.key,
                commands: entry.commands,
            },
            validation_duplicate,
        })
    }

    #[cfg(feature = "lab-diagnostics")]
    fn log_bulk_candidates(
        &self,
        active_key: Option<ServerTcpPathKey>,
        lead_candidate: Option<(ServerTcpPathKey, f64, PathSnapshot)>,
        payload_bytes: usize,
        mux_limits: MuxLimits,
        now: Instant,
        lower_flights: &[ServerTcpPathFlightDebt],
    ) {
        for entry in &self.entries {
            let snapshot = server_bulk_output_snapshot(entry, mux_limits, now);
            let eta_ms = server_bulk_output_eta_ms(entry.key, snapshot, active_key, payload_bytes);
            let reason = if entry.validation_credit_bytes < payload_bytes as u64
                && Some(entry.key) != active_key
                && !server_output_has_sender_evidence(entry)
            {
                "validation_credit_exhausted"
            } else if let Some((lead_key, best_eta_ms, best_snapshot)) = lead_candidate {
                let role = if entry.key == lead_key {
                    BulkAdmissionRole::ActiveDataPath
                } else {
                    bulk_additional_admission_role(lead_key.underlay, entry.key.underlay)
                };
                let ordering_debt = server_stream_ordering_debt_bytes(lower_flights, entry.key);
                if let Some(suppression) =
                    bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
                        best_snapshot,
                        best_eta_ms,
                        candidate_snapshot: snapshot,
                        candidate_eta_ms: eta_ms,
                        payload_bytes,
                        mux_limits,
                        role,
                        stream_ordering_debt_bytes: ordering_debt,
                    })
                {
                    suppression
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
                    "path_underlay={:?} path_id={} active={} reason={} payload_bytes={} eta_ms={:.3} confidence={:.3} delivery_samples={} validation_credit_bytes={} product_bytes_in_flight={} carrier_bytes_in_flight={} stream_ordering_debt={} queue_bytes={} command_pending_bytes={} inflight_limit={} srtt_ms={:.3} delivery_rate_mbps={:.3}",
                    entry.key.underlay,
                    entry.key.path_id.0,
                    Some(entry.key) == active_key,
                    reason,
                    payload_bytes,
                    eta_ms,
                    snapshot.confidence,
                    entry.delivery_samples,
                    entry.validation_credit_bytes,
                    entry.bytes_in_flight,
                    snapshot.bytes_in_flight,
                    server_stream_ordering_debt_bytes(lower_flights, entry.key),
                    snapshot.queue_bytes,
                    entry.commands.pending_bytes(),
                    snapshot.inflight_limit_bytes,
                    snapshot.srtt_ms,
                    snapshot.delivery_rate_bps / 1_000_000.0,
                ),
            );
        }
    }
}

fn server_bulk_output_snapshot(
    entry: &ServerTcpStreamOutputEntry,
    _mux_limits: MuxLimits,
    now: Instant,
) -> PathSnapshot {
    let srtt_ms = entry.path_metrics.map_or_else(
        || default_path_srtt_ms(entry.key.underlay),
        |path_metrics| f64::from(path_metrics.metrics.srtt_us.max(1)) / 1000.0,
    );
    let jitter_ms = entry.path_metrics.map_or(0.0, |path_metrics| {
        f64::from(path_metrics.metrics.jitter_us) / 1000.0
    });
    let loss_rate = entry
        .path_metrics
        .map_or(0.0, |path_metrics| {
            f64::from(path_metrics.metrics.loss_ppm) / 1_000_000.0
        })
        .clamp(0.0, 1.0);
    let metric_rate_bps = entry
        .path_metrics
        .map(|path_metrics| path_metrics.metrics.delivery_rate_bps as f64);
    let rate_bps = match entry.key.underlay {
        UnderlayProtocol::Udp => metric_rate_bps.or(entry.delivery_rate_bps),
        UnderlayProtocol::Tcp => entry.delivery_rate_bps.or(metric_rate_bps),
    }
    .unwrap_or_else(|| default_path_rate_bps(entry.key.underlay))
    .max(1.0);
    let mut snapshot = PathSnapshot::new(entry.key.path_id, entry.key.underlay, srtt_ms, rate_bps);
    snapshot.jitter_ms = jitter_ms;
    snapshot.loss_rate = loss_rate;
    let local_sender_metrics = entry.path_metrics.and_then(|path_metrics| {
        (path_metrics.source == ServerPathMetricsSource::LocalSender).then_some(path_metrics)
    });
    snapshot.queue_bytes = if entry.key.underlay == UnderlayProtocol::Udp {
        local_sender_metrics
            .map_or(0, |path_metrics| path_metrics.metrics.queue_bytes)
            .saturating_add(entry.commands.pending_bytes())
    } else {
        entry
            .path_metrics
            .map_or(0, |path_metrics| path_metrics.metrics.queue_bytes)
    };
    snapshot.bytes_in_flight = if entry.key.underlay == UnderlayProtocol::Udp {
        local_sender_metrics.map_or(0, |path_metrics| path_metrics.metrics.bytes_in_flight)
    } else {
        entry.bytes_in_flight
    };
    snapshot.product_bytes_in_flight = entry.bytes_in_flight;
    snapshot.inflight_limit_bytes = entry
        .path_metrics
        .map_or(0, |path_metrics| path_metrics.metrics.inflight_limit_bytes);
    snapshot.confidence = server_output_confidence(entry, now);
    snapshot
}

fn server_bulk_output_eta_ms(
    key: ServerTcpPathKey,
    snapshot: PathSnapshot,
    active_key: Option<ServerTcpPathKey>,
    payload_bytes: usize,
) -> f64 {
    let queued_bits = snapshot
        .queue_bytes
        .saturating_add(snapshot.bytes_in_flight)
        .saturating_mul(8) as f64;
    let payload_bits = payload_bytes as f64 * 8.0;
    let mut eta_ms = snapshot.srtt_ms / 2.0;
    eta_ms += (queued_bits + payload_bits) / snapshot.delivery_rate_bps.max(1.0) * 1000.0;
    eta_ms += snapshot.jitter_ms;
    eta_ms += snapshot.loss_rate.clamp(0.0, 1.0) * 500.0;
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
            f64::from(metrics.confidence_ppm).clamp(0.0, 1_000_000.0) / 1_000_000.0 * 0.35
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
    (delivery_confidence + metric_confidence + freshness_confidence).clamp(0.1, 1.0)
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
        let binding = ServerTcpStreamBinding::new_with_limits(
            session_id,
            underlay,
            path_id,
            attachment.commands,
            lane,
            mux_limits,
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

#[derive(Clone)]
pub(super) struct TcpPathSessionCommandSender {
    control: mpsc::Sender<TcpPathSessionCommand>,
    priority: mpsc::Sender<TcpPathSessionCommand>,
    data: mpsc::Sender<TcpPathSessionCommand>,
    metrics: Arc<TcpPathSessionCommandQueueMetrics>,
}

pub(super) struct TcpPathSessionCommandReceivers {
    control: mpsc::Receiver<TcpPathSessionCommand>,
    priority: mpsc::Receiver<TcpPathSessionCommand>,
    data: mpsc::Receiver<TcpPathSessionCommand>,
    metrics: Arc<TcpPathSessionCommandQueueMetrics>,
}

#[derive(Default)]
struct TcpPathSessionCommandQueueMetrics {
    pending_bytes: AtomicU64,
}

impl TcpPathSessionCommandQueueMetrics {
    fn add_pending_bytes(&self, bytes: usize) {
        self.pending_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    fn release_pending_bytes(&self, bytes: usize) {
        let bytes = bytes as u64;
        let _ = self
            .pending_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(bytes))
            });
    }

    #[cfg_attr(not(any(test, feature = "lab-diagnostics")), allow(dead_code))]
    fn pending_bytes(&self) -> u64 {
        self.pending_bytes.load(Ordering::Relaxed)
    }
}

impl TcpPathSessionCommandSender {
    pub(super) async fn send_control(
        &self,
        command: TcpPathSessionCommand,
    ) -> Result<(), mpsc::error::SendError<TcpPathSessionCommand>> {
        #[cfg(feature = "lab-diagnostics")]
        let started = Instant::now();
        let result = self.control.send(command).await;
        #[cfg(feature = "lab-diagnostics")]
        lab_perf_record("runtime.path_queue.control_send", started.elapsed(), 0);
        result
    }

    pub(super) async fn send_frame(
        &self,
        frame: Frame,
        lane: FlowLane,
    ) -> Result<(), RuntimeError> {
        let bytes = frame_pacing_bytes(&frame);
        let effective_lane = tcp_path_effective_frame_lane(&frame, lane);
        let queue = if tcp_path_frame_uses_priority_queue(effective_lane) {
            &self.priority
        } else {
            &self.data
        };
        #[cfg(feature = "lab-diagnostics")]
        let started = Instant::now();
        let result = match queue.reserve().await {
            Ok(permit) => {
                self.metrics.add_pending_bytes(bytes);
                permit.send(TcpPathSessionCommand::SendFrame(frame));
                Ok(())
            }
            Err(_) => Err(RuntimeError::TcpPathSessionClosed),
        };
        #[cfg(feature = "lab-diagnostics")]
        lab_perf_record(
            if tcp_path_frame_uses_priority_queue(effective_lane) {
                "runtime.path_queue.priority_send"
            } else {
                "runtime.path_queue.data_send"
            },
            started.elapsed(),
            bytes,
        );
        result
    }

    pub(super) fn try_send_frame(
        &self,
        frame: Frame,
        lane: FlowLane,
    ) -> Result<bool, RuntimeError> {
        let bytes = frame_pacing_bytes(&frame);
        let effective_lane = tcp_path_effective_frame_lane(&frame, lane);
        let queue = if tcp_path_frame_uses_priority_queue(effective_lane) {
            &self.priority
        } else {
            &self.data
        };
        match queue.try_reserve() {
            Ok(permit) => {
                self.metrics.add_pending_bytes(bytes);
                permit.send(TcpPathSessionCommand::SendFrame(frame));
                Ok(true)
            }
            Err(mpsc::error::TrySendError::Full(_)) => Ok(false),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(RuntimeError::TcpPathSessionClosed),
        }
    }

    #[cfg_attr(not(any(test, feature = "lab-diagnostics")), allow(dead_code))]
    pub(super) fn pending_bytes(&self) -> u64 {
        self.metrics.pending_bytes()
    }

    pub(super) fn is_closed(&self) -> bool {
        self.control.is_closed() && self.priority.is_closed() && self.data.is_closed()
    }

    pub(super) fn same_channel(&self, other: &Self) -> bool {
        self.control.same_channel(&other.control)
            && self.priority.same_channel(&other.priority)
            && self.data.same_channel(&other.data)
    }
}

pub(super) fn tcp_path_frame_uses_priority_queue(lane: FlowLane) -> bool {
    matches!(
        lane,
        FlowLane::Control | FlowLane::Latency | FlowLane::RealtimeDatagram
    )
}

pub(super) fn tcp_path_effective_frame_lane(frame: &Frame, stream_lane: FlowLane) -> FlowLane {
    match frame {
        Frame::StreamData { .. } => stream_lane,
        Frame::DatagramData { .. } | Frame::DatagramFeedback { .. } => FlowLane::RealtimeDatagram,
        _ => FlowLane::Control,
    }
}

pub(super) fn tcp_path_session_command_channels(
    queue: usize,
) -> (TcpPathSessionCommandSender, TcpPathSessionCommandReceivers) {
    let queue = queue.max(1);
    let (control_tx, control_rx) = mpsc::channel(queue);
    let (priority_tx, priority_rx) = mpsc::channel(queue);
    let (data_tx, data_rx) = mpsc::channel(queue);
    let metrics = Arc::new(TcpPathSessionCommandQueueMetrics::default());
    (
        TcpPathSessionCommandSender {
            control: control_tx,
            priority: priority_tx,
            data: data_tx,
            metrics: metrics.clone(),
        },
        TcpPathSessionCommandReceivers {
            control: control_rx,
            priority: priority_rx,
            data: data_rx,
            metrics,
        },
    )
}

fn tcp_receiver_may_recv<T>(receiver: &mpsc::Receiver<T>) -> bool {
    !receiver.is_closed() || !receiver.is_empty()
}

pub(super) fn tcp_path_receivers_closed(receivers: &TcpPathSessionCommandReceivers) -> bool {
    !tcp_receiver_may_recv(&receivers.control)
        && !tcp_receiver_may_recv(&receivers.priority)
        && !tcp_receiver_may_recv(&receivers.data)
}

pub(super) async fn recv_tcp_path_command(
    receivers: &mut TcpPathSessionCommandReceivers,
) -> Option<TcpPathSessionCommand> {
    if let Some(command) = recv_ready_priority_command(receivers) {
        release_tcp_path_command_pending_bytes(receivers, &command);
        return Some(command);
    }
    let control_may_recv = tcp_receiver_may_recv(&receivers.control);
    let priority_may_recv = tcp_receiver_may_recv(&receivers.priority);
    let data_may_recv = tcp_receiver_may_recv(&receivers.data);
    let command = match (control_may_recv, priority_may_recv, data_may_recv) {
        (true, true, true) => {
            tokio::select! {
                biased;
                command = receivers.control.recv() => command,
                command = receivers.priority.recv() => command,
                command = receivers.data.recv() => command,
            }
        }
        (true, true, false) => {
            tokio::select! {
                biased;
                command = receivers.control.recv() => command,
                command = receivers.priority.recv() => command,
            }
        }
        (true, false, true) => {
            tokio::select! {
                biased;
                command = receivers.control.recv() => command,
                command = receivers.data.recv() => command,
            }
        }
        (false, true, true) => {
            tokio::select! {
                biased;
                command = receivers.priority.recv() => command,
                command = receivers.data.recv() => command,
            }
        }
        (true, false, false) => receivers.control.recv().await,
        (false, true, false) => receivers.priority.recv().await,
        (false, false, true) => receivers.data.recv().await,
        (false, false, false) => None,
    };
    if let Some(command) = &command {
        release_tcp_path_command_pending_bytes(receivers, command);
    }
    command
}

fn recv_ready_priority_command(
    receivers: &mut TcpPathSessionCommandReceivers,
) -> Option<TcpPathSessionCommand> {
    if let Ok(command) = receivers.control.try_recv() {
        return Some(command);
    }
    receivers.priority.try_recv().ok()
}

fn release_tcp_path_command_pending_bytes(
    receivers: &TcpPathSessionCommandReceivers,
    command: &TcpPathSessionCommand,
) {
    receivers
        .metrics
        .release_pending_bytes(tcp_path_command_pacing_bytes(command));
}

fn tcp_path_command_pacing_bytes(command: &TcpPathSessionCommand) -> usize {
    match command {
        TcpPathSessionCommand::SendFrame(frame) => frame_pacing_bytes(frame),
        TcpPathSessionCommand::OpenStream { .. } | TcpPathSessionCommand::CloseStream(_) => 0,
    }
}

pub(super) enum TcpPathSessionCommand {
    OpenStream {
        stream_id: StreamId,
        target: TargetAddr,
        ingress: IngressKind,
        lane: FlowLane,
        role: StreamOpenRole,
        session_commands: TcpPathSessionCommandSender,
        response: oneshot::Sender<Result<TcpPathStream, RuntimeError>>,
    },
    SendFrame(Frame),
    CloseStream(StreamId),
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
