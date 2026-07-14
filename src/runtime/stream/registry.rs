use super::handle::{ReliablePathStream, ReliablePathStreamOutput};
use super::response::{
    ResponseStreamAttachOutcome, ResponseStreamBinding, ServerPathLaneTracker,
    ServerPathMetricsEntry, ServerPathMetricsSource, ServerRealtimeFlowRegistration,
    next_server_carrier_path_instance_id,
};
use crate::config::ResourceLimits;
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::{lab_diagnostic, lab_perf_record};
use crate::model::capacity::{
    QuicCapacityProofCandidate, reliable_stream_initial_advertised_window_bytes,
};
use crate::model::path::{CarrierPathInstanceId, CarrierPathKey};
use crate::mux::MuxLimits;
#[cfg(feature = "lab-diagnostics")]
use crate::protocol::frame::reliable_path_frame_pacing_bytes;
use crate::protocol::{
    Frame, PathId, PathMetricDirection, PathMetrics, SessionId, StreamId, StreamOpenRole,
    TargetAddr, UnderlayProtocol,
};
use crate::runtime::RuntimeError;
use crate::runtime::path::commands::{
    ReliablePathCommandSender, reliable_stream_frame_queue_for_payload,
};
use crate::runtime::path::tcp::capacity::{
    TcpCapacityProofCandidate, valid_tcp_capacity_proof_candidate_at,
};
use crate::runtime::recent_ids::{RecentIdCache, reliable_closed_stream_cache_capacity};
use crate::scheduler::FlowLane;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::mpsc;

/// Session-wide registry for server-side product reliable streams.
///
/// The registry owns stream lookup, target consistency, recent closed-stream
/// filtering, and peer/local path metrics for response scheduling. It does not
/// own target sockets or carrier packet state.
pub(in crate::runtime) struct ServerReliableStreamRegistry {
    streams: Mutex<HashMap<(SessionId, StreamId), ServerReliableStreamEntry>>,
    path_metrics: Mutex<
        HashMap<
            (SessionId, UnderlayProtocol, PathId, CarrierPathInstanceId),
            ServerPathMetricsEntry,
        >,
    >,
    active_path_instances:
        Mutex<HashSet<(SessionId, UnderlayProtocol, PathId, CarrierPathInstanceId)>>,
    closed_streams: Mutex<RecentIdCache<(SessionId, StreamId)>>,
    lane_tracker: Arc<ServerPathLaneTracker>,
}

impl std::fmt::Debug for ServerReliableStreamRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerReliableStreamRegistry")
            .finish_non_exhaustive()
    }
}

struct ServerReliableStreamEntry {
    target: TargetAddr,
    lane: FlowLane,
    frames: mpsc::Sender<Result<Frame, RuntimeError>>,
    binding: Arc<ResponseStreamBinding>,
}

pub(in crate::runtime) struct ServerReliablePathAttachment {
    pub(in crate::runtime) path_registration: ServerCarrierPathRegistration,
    pub(in crate::runtime) commands: ReliablePathCommandSender,
    pub(in crate::runtime) max_frame_payload_bytes: usize,
    pub(in crate::runtime) role: StreamOpenRole,
    pub(in crate::runtime) initial_metrics: Option<PathMetrics>,
}

/// Request to open or attach a carrier path to a product reliable stream.
///
/// The attachment carries carrier command access; the registry decides whether
/// this is a new product stream or an additional path for an existing stream.
pub(in crate::runtime) struct ServerReliableStreamOpenRequest<'a> {
    pub(in crate::runtime) session_id: SessionId,
    pub(in crate::runtime) stream_id: StreamId,
    pub(in crate::runtime) target: &'a TargetAddr,
    pub(in crate::runtime) lane: FlowLane,
    pub(in crate::runtime) attachment: ServerReliablePathAttachment,
}

pub(in crate::runtime) enum ServerReliableStreamOpen {
    New(ReliablePathStream),
    Existing,
    DuplicateLiveIgnored,
    Rejected,
}

pub(in crate::runtime) struct ServerReliableRegistryManagementSnapshot {
    pub(in crate::runtime) active_streams: usize,
    pub(in crate::runtime) path_metrics: Vec<ServerCarrierPathMetricSnapshot>,
}

pub(in crate::runtime) struct ServerCarrierPathMetricSnapshot {
    pub(in crate::runtime) session_id: SessionId,
    pub(in crate::runtime) underlay: UnderlayProtocol,
    pub(in crate::runtime) path_id: PathId,
    pub(in crate::runtime) metrics: PathMetrics,
    pub(in crate::runtime) source: &'static str,
}

#[derive(Clone)]
pub(in crate::runtime) struct ServerCarrierPathRegistration {
    inner: Arc<ServerCarrierPathRegistrationInner>,
}

struct ServerCarrierPathRegistrationInner {
    registry: Arc<ServerReliableStreamRegistry>,
    session_id: SessionId,
    underlay: UnderlayProtocol,
    path_id: PathId,
    path_instance_id: CarrierPathInstanceId,
}

impl ServerCarrierPathRegistration {
    pub(in crate::runtime) fn path_instance_id(&self) -> CarrierPathInstanceId {
        self.inner.path_instance_id
    }

    pub(in crate::runtime) fn session_id(&self) -> SessionId {
        self.inner.session_id
    }

    fn underlay(&self) -> UnderlayProtocol {
        self.inner.underlay
    }

    pub(in crate::runtime) fn path_id(&self) -> PathId {
        self.inner.path_id
    }

    fn belongs_to(&self, registry: &ServerReliableStreamRegistry) -> bool {
        std::ptr::eq(Arc::as_ptr(&self.inner.registry), registry)
    }
}

impl Drop for ServerCarrierPathRegistrationInner {
    fn drop(&mut self) {
        self.registry.retire_carrier_path_instance(
            self.session_id,
            self.underlay,
            self.path_id,
            self.path_instance_id,
        );
        self.registry.lane_tracker.detach_session(self.session_id);
    }
}

impl ServerReliableStreamRegistry {
    pub(in crate::runtime) fn new(max_streams: usize) -> Self {
        Self {
            streams: Mutex::new(HashMap::new()),
            path_metrics: Mutex::new(HashMap::new()),
            active_path_instances: Mutex::new(HashSet::new()),
            closed_streams: Mutex::new(RecentIdCache::new(reliable_closed_stream_cache_capacity(
                max_streams,
            ))),
            lane_tracker: Arc::new(ServerPathLaneTracker::default()),
        }
    }

    pub(in crate::runtime) fn management_snapshot(
        &self,
    ) -> ServerReliableRegistryManagementSnapshot {
        let active_streams = self.streams.lock().expect("server stream lock").len();
        let path_metrics = self
            .path_metrics
            .lock()
            .expect("server path metrics lock")
            .iter()
            .map(
                |((session_id, underlay, path_id, _path_instance_id), entry)| {
                    ServerCarrierPathMetricSnapshot {
                        session_id: *session_id,
                        underlay: *underlay,
                        path_id: *path_id,
                        metrics: entry.metrics,
                        source: match entry.source {
                            ServerPathMetricsSource::PeerHint => "peer_hint",
                            ServerPathMetricsSource::LocalSender => "local_sender",
                        },
                    }
                },
            )
            .collect();
        ServerReliableRegistryManagementSnapshot {
            active_streams,
            path_metrics,
        }
    }

    pub(in crate::runtime) fn register_realtime_flow(
        &self,
        session_id: SessionId,
    ) -> ServerRealtimeFlowRegistration {
        ServerRealtimeFlowRegistration::new(self.lane_tracker.clone(), session_id)
    }

    pub(in crate::runtime) fn register_carrier_path(
        self: &Arc<Self>,
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
    ) -> ServerCarrierPathRegistration {
        let path_instance_id = next_server_carrier_path_instance_id();
        // Carrier lifetime, not response-stream occupancy, bounds cumulative
        // per-session discovery spend across zero-stream gaps.
        self.lane_tracker.attach_session(session_id);
        self.active_path_instances
            .lock()
            .expect("server active path instance lock")
            .insert((session_id, underlay, path_id, path_instance_id));
        ServerCarrierPathRegistration {
            inner: Arc::new(ServerCarrierPathRegistrationInner {
                registry: self.clone(),
                session_id,
                underlay,
                path_id,
                path_instance_id,
            }),
        }
    }

    fn retire_carrier_path_instance(
        &self,
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        path_instance_id: CarrierPathInstanceId,
    ) {
        self.active_path_instances
            .lock()
            .expect("server active path instance lock")
            .remove(&(session_id, underlay, path_id, path_instance_id));
        self.path_metrics
            .lock()
            .expect("server path metrics lock")
            .remove(&(session_id, underlay, path_id, path_instance_id));
        let key = CarrierPathKey { underlay, path_id };
        // Retire the carrier-scoped reservation before stream detach can consume
        // it as a generic binding cancellation.
        self.lane_tracker
            .retire_quic_capacity_calibration_path_instance(session_id, key, path_instance_id);
        let bindings = {
            let streams = self
                .streams
                .lock()
                .expect("server reliable stream registry lock");
            streams
                .iter()
                .filter_map(|((entry_session_id, _), entry)| {
                    (*entry_session_id == session_id).then_some(entry.binding.clone())
                })
                .collect::<Vec<_>>()
        };
        for binding in bindings {
            binding.detach_path_instance(key, path_instance_id);
        }
    }

    pub(in crate::runtime) fn open_or_attach(
        &self,
        request: ServerReliableStreamOpenRequest<'_>,
        mux_limits: MuxLimits,
        max_streams: usize,
    ) -> Result<ServerReliableStreamOpen, RuntimeError> {
        let ServerReliableStreamOpenRequest {
            session_id,
            stream_id,
            target,
            lane,
            attachment,
        } = request;
        let ServerReliablePathAttachment {
            path_registration,
            commands,
            max_frame_payload_bytes,
            role,
            initial_metrics,
        } = attachment;
        if !path_registration.belongs_to(self) || path_registration.session_id() != session_id {
            return Err(RuntimeError::Protocol(
                "reliable path registration does not match stream registry or session",
            ));
        }
        let underlay = path_registration.underlay();
        let path_id = path_registration.path_id();
        let path_instance_id = path_registration.path_instance_id();
        let mut streams = self
            .streams
            .lock()
            .expect("server reliable stream registry lock");
        // Resolve stored proof after taking the stream membership lock. A path
        // proof published while this opener waited must be inherited rather
        // than missed by a stale pre-lock metrics copy.
        let initial_metrics = self.initial_path_metrics(
            session_id,
            underlay,
            path_id,
            path_instance_id,
            initial_metrics,
        );
        if let Some(entry) = streams.get_mut(&(session_id, stream_id)) {
            if entry.target != *target {
                return Err(RuntimeError::Protocol(
                    "reliable stream migration target does not match original stream",
                ));
            }
            entry.lane = lane;
            let attach_outcome = entry.binding.attach_with_path_instance(
                underlay,
                path_id,
                path_instance_id,
                commands,
                lane,
                role,
                max_frame_payload_bytes,
            );
            if matches!(
                attach_outcome,
                ResponseStreamAttachOutcome::RejectedDuplicateLiveOutput
            ) {
                #[cfg(feature = "lab-diagnostics")]
                let result = match attach_outcome {
                    ResponseStreamAttachOutcome::RejectedDuplicateLiveOutput => {
                        "rejected_duplicate_live_output"
                    }
                    ResponseStreamAttachOutcome::Attached => "attached",
                    ResponseStreamAttachOutcome::RoleChanged => "role_changed",
                    ResponseStreamAttachOutcome::ReplacedClosedOutput => "replaced_closed_output",
                };
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "server_stream_open",
                    format_args!(
                        "session_id={} stream_id={} path_underlay={:?} path_id={} role={:?} lane={:?} result={}",
                        session_id.0, stream_id.0, underlay, path_id.0, role, lane, result,
                    ),
                );
                return Ok(ServerReliableStreamOpen::DuplicateLiveIgnored);
            }
            if !matches!(
                attach_outcome,
                ResponseStreamAttachOutcome::RejectedDuplicateLiveOutput
            ) && let Some(metrics) = initial_metrics
            {
                entry.binding.install_stored_path_metrics_for_instance(
                    CarrierPathKey { underlay, path_id },
                    path_instance_id,
                    metrics,
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
            return Ok(ServerReliableStreamOpen::Existing);
        }

        if self
            .closed_streams
            .lock()
            .expect("server reliable stream closed cache lock")
            .contains(&(session_id, stream_id))
        {
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "server_stream_open",
                format_args!(
                    "session_id={} stream_id={} path_underlay={:?} path_id={} role={:?} lane={:?} result=rejected_closed_stream",
                    session_id.0, stream_id.0, underlay, path_id.0, role, lane,
                ),
            );
            return Ok(ServerReliableStreamOpen::Rejected);
        }

        if role != StreamOpenRole::Active {
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "server_stream_open",
                format_args!(
                    "session_id={} stream_id={} path_underlay={:?} path_id={} role={:?} lane={:?} result=rejected_attach_only_unknown",
                    session_id.0, stream_id.0, underlay, path_id.0, role, lane,
                ),
            );
            return Ok(ServerReliableStreamOpen::Rejected);
        }

        if streams.len() >= max_streams {
            return Err(RuntimeError::Protocol(
                "server reliable stream limit reached",
            ));
        }

        let (frames_tx, frames_rx) = mpsc::channel(reliable_stream_frame_queue_for_payload(
            mux_limits,
            max_frame_payload_bytes,
        ));
        let binding = ResponseStreamBinding::new_with_limits_tracker_and_path_instance(
            session_id,
            underlay,
            path_id,
            commands,
            lane,
            mux_limits,
            self.lane_tracker.clone(),
            path_instance_id,
        );
        if let Some(metrics) = initial_metrics {
            binding.install_stored_path_metrics_for_instance(
                CarrierPathKey { underlay, path_id },
                path_instance_id,
                metrics,
            );
        }
        streams.insert(
            (session_id, stream_id),
            ServerReliableStreamEntry {
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
        let initial_max_offset =
            reliable_stream_initial_advertised_window_bytes(underlay, lane, mux_limits);
        Ok(ServerReliableStreamOpen::New(ReliablePathStream {
            stream_id,
            max_offset: initial_max_offset,
            lane,
            underlay,
            max_frame_payload_bytes,
            output: ReliablePathStreamOutput::Switchable(binding),
            frames: frames_rx,
        }))
    }

    pub(in crate::runtime) fn record_path_metrics(
        &self,
        path_registration: &ServerCarrierPathRegistration,
        metrics: PathMetrics,
    ) {
        self.record_path_metrics_with_source(
            path_registration,
            metrics,
            ServerPathMetricsSource::PeerHint,
        );
    }

    pub(in crate::runtime) fn record_local_path_metrics(
        &self,
        path_registration: &ServerCarrierPathRegistration,
        metrics: PathMetrics,
    ) {
        self.record_path_metrics_with_source(
            path_registration,
            metrics,
            ServerPathMetricsSource::LocalSender,
        );
    }

    /// Publishes a carrier proof only after its exact session reservation has
    /// accepted the frozen train specification. Generic metrics cannot call it.
    pub(in crate::runtime) fn record_local_quic_capacity_proof(
        &self,
        path_registration: &ServerCarrierPathRegistration,
        mut metrics: PathMetrics,
        candidate: QuicCapacityProofCandidate,
    ) -> bool {
        if !path_registration.belongs_to(self)
            || path_registration.underlay() != UnderlayProtocol::Udp
            || metrics.underlay != UnderlayProtocol::Udp
            || metrics.direction != PathMetricDirection::ServerToClient
        {
            return false;
        }
        let session_id = path_registration.session_id();
        let path_id = path_registration.path_id();
        let path_instance_id = path_registration.path_instance_id();
        let instance_key = (session_id, UnderlayProtocol::Udp, path_id, path_instance_id);
        // Holding instance membership fences carrier retirement across the
        // accept/commit/publish/finalize transaction without holding lane state.
        let active_path_instances = self
            .active_path_instances
            .lock()
            .expect("server active path instance lock");
        if !active_path_instances.contains(&instance_key) {
            return false;
        }
        let key = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id,
        };
        let Some(ticket) = self.lane_tracker.try_accept_quic_capacity_proof(
            session_id,
            key,
            path_instance_id,
            candidate,
        ) else {
            return false;
        };
        if self
            .lane_tracker
            .commit_quic_capacity_proof(ticket)
            .is_none()
        {
            return false;
        }

        metrics.path_id = path_id;
        let entry = ServerPathMetricsEntry {
            metrics,
            source: ServerPathMetricsSource::LocalSender,
            recorded_at: Instant::now(),
            capacity_proof: Some(candidate),
            tcp_capacity_proof: None,
        };
        self.path_metrics
            .lock()
            .expect("server path metrics lock")
            .insert(instance_key, entry);

        let bindings = {
            let streams = self
                .streams
                .lock()
                .expect("server reliable stream registry lock");
            streams
                .iter()
                .filter_map(|((entry_session_id, _), entry)| {
                    (*entry_session_id == session_id).then_some(entry.binding.clone())
                })
                .collect::<Vec<_>>()
        };
        let installed = bindings
            .iter()
            .filter_map(|binding| {
                binding
                    .install_quic_capacity_proof_for_instance(
                        key,
                        path_instance_id,
                        metrics,
                        candidate,
                    )
                    .then_some(binding.clone())
            })
            .collect::<Vec<_>>();

        self.lane_tracker
            .finish_quic_capacity_proof_publication(ticket)
            .expect("committed capacity proof publication must finish");
        drop(active_path_instances);
        for binding in installed {
            binding.notify_installed_path_metrics();
        }
        true
    }

    /// Publishes one receiver-confirmed TCP train for the exact live socket.
    pub(in crate::runtime) fn record_local_tcp_capacity_proof(
        &self,
        path_registration: &ServerCarrierPathRegistration,
        mut metrics: PathMetrics,
        candidate: TcpCapacityProofCandidate,
    ) -> bool {
        if !path_registration.belongs_to(self)
            || path_registration.underlay() != UnderlayProtocol::Tcp
            || metrics.underlay != UnderlayProtocol::Tcp
            || metrics.direction != PathMetricDirection::ServerToClient
            || !valid_tcp_capacity_proof_candidate_at(candidate, Instant::now())
        {
            return false;
        }
        let session_id = path_registration.session_id();
        let path_id = path_registration.path_id();
        let path_instance_id = path_registration.path_instance_id();
        let instance_key = (session_id, UnderlayProtocol::Tcp, path_id, path_instance_id);
        let active_path_instances = self
            .active_path_instances
            .lock()
            .expect("server active path instance lock");
        if !active_path_instances.contains(&instance_key) {
            return false;
        }
        metrics.path_id = path_id;
        let entry = ServerPathMetricsEntry {
            metrics,
            source: ServerPathMetricsSource::LocalSender,
            recorded_at: Instant::now(),
            capacity_proof: None,
            tcp_capacity_proof: Some(candidate),
        };
        self.path_metrics
            .lock()
            .expect("server path metrics lock")
            .insert(instance_key, entry);
        drop(active_path_instances);

        let key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id,
        };
        let bindings = {
            let streams = self
                .streams
                .lock()
                .expect("server reliable stream registry lock");
            streams
                .iter()
                .filter_map(|((entry_session_id, _), entry)| {
                    (*entry_session_id == session_id).then_some(entry.binding.clone())
                })
                .collect::<Vec<_>>()
        };
        let installed = bindings
            .iter()
            .filter(|binding| {
                binding.install_tcp_capacity_proof_for_instance(
                    key,
                    path_instance_id,
                    metrics,
                    candidate,
                )
            })
            .count();
        #[cfg(not(feature = "lab-diagnostics"))]
        let _ = installed;
        for binding in bindings {
            binding.notify_installed_path_metrics();
        }
        #[cfg(feature = "lab-diagnostics")]
        lab_diagnostic(
            "response_tcp_capacity_probe",
            format_args!(
                "phase=published session_id={} path_id={} path_instance_id={} calibration_id={} train_bytes={} receipt_rate_mbps={:.3} rate_mbps={:.3} bindings={}",
                session_id.0,
                path_id.0,
                path_instance_id.as_u64(),
                candidate.token,
                candidate.train_bytes,
                candidate.receipt_rate_bps as f64 / 1_000_000.0,
                candidate.rate_bps as f64 / 1_000_000.0,
                installed,
            ),
        );
        true
    }

    fn record_path_metrics_with_source(
        &self,
        path_registration: &ServerCarrierPathRegistration,
        metrics: PathMetrics,
        source: ServerPathMetricsSource,
    ) {
        if !path_registration.belongs_to(self) {
            return;
        }
        let session_id = path_registration.session_id();
        let underlay = path_registration.underlay();
        let path_id = path_registration.path_id();
        let path_instance_id = path_registration.path_instance_id();
        let instance_key = (session_id, underlay, path_id, path_instance_id);
        let active_path_instances = self
            .active_path_instances
            .lock()
            .expect("server active path instance lock");
        if !active_path_instances.contains(&instance_key) {
            return;
        }
        let metrics = PathMetrics { path_id, ..metrics };
        let mut path_metrics = self.path_metrics.lock().expect("server path metrics lock");
        let previous = path_metrics.get(&instance_key).copied();
        let capacity_proof = previous
            .and_then(|previous| previous.capacity_proof)
            .filter(|proof| proof.expires_at > Instant::now());
        let tcp_capacity_proof = previous
            .and_then(|previous| previous.tcp_capacity_proof)
            .filter(|proof| proof.expires_at > Instant::now());
        let entry = ServerPathMetricsEntry {
            metrics,
            source,
            recorded_at: Instant::now(),
            capacity_proof,
            tcp_capacity_proof,
        };
        // One registry slot cannot represent both directions. Preserve local
        // sender authority for future attachments; peer hints still update each
        // live binding's independent peer slot below.
        if source == ServerPathMetricsSource::LocalSender
            || previous.is_none_or(|entry| entry.source != ServerPathMetricsSource::LocalSender)
        {
            path_metrics.insert(instance_key, entry);
        }
        drop(path_metrics);
        drop(active_path_instances);
        #[cfg(feature = "lab-diagnostics")]
        lab_diagnostic(
            "server_path_metrics_recorded",
            format_args!(
                "session_id={} underlay={:?} path_id={} source={:?} direction={:?} rate_mbps={:.3} pacing_mbps={:.3} srtt_ms={:.3} confidence_ppm={} app_limited={} ack_sample={} sample_count={} sample_bytes={}",
                session_id.0,
                underlay,
                path_id.0,
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
        let bindings = {
            let streams = self
                .streams
                .lock()
                .expect("server reliable stream registry lock");
            streams
                .iter()
                .filter_map(|((entry_session_id, _), entry)| {
                    (*entry_session_id == session_id).then_some(entry.binding.clone())
                })
                .collect::<Vec<_>>()
        };
        let key = CarrierPathKey { underlay, path_id };
        for binding in bindings {
            binding.update_path_metrics_for_instance(key, path_instance_id, metrics, source);
        }
    }

    fn initial_path_metrics(
        &self,
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        path_instance_id: CarrierPathInstanceId,
        initial_metrics: Option<PathMetrics>,
    ) -> Option<ServerPathMetricsEntry> {
        let stored = self.stored_path_metrics(session_id, underlay, path_id, path_instance_id);
        match initial_metrics {
            Some(metrics) => Some(ServerPathMetricsEntry {
                metrics: PathMetrics { path_id, ..metrics },
                source: ServerPathMetricsSource::LocalSender,
                recorded_at: Instant::now(),
                capacity_proof: stored.and_then(|entry| entry.capacity_proof),
                tcp_capacity_proof: stored.and_then(|entry| entry.tcp_capacity_proof),
            }),
            None => stored,
        }
    }

    fn stored_path_metrics(
        &self,
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        path_instance_id: CarrierPathInstanceId,
    ) -> Option<ServerPathMetricsEntry> {
        self.path_metrics
            .lock()
            .expect("server path metrics lock")
            .get(&(session_id, underlay, path_id, path_instance_id))
            .copied()
            .map(|mut entry| {
                if entry
                    .capacity_proof
                    .is_some_and(|proof| proof.expires_at <= Instant::now())
                {
                    entry.capacity_proof = None;
                }
                if entry
                    .tcp_capacity_proof
                    .is_some_and(|proof| proof.expires_at <= Instant::now())
                {
                    entry.tcp_capacity_proof = None;
                }
                entry
            })
    }

    pub(in crate::runtime) fn detach_path(
        &self,
        session_id: SessionId,
        stream_id: StreamId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: &ReliablePathCommandSender,
    ) {
        if let Some(binding) = self
            .streams
            .lock()
            .expect("server reliable stream registry lock")
            .get(&(session_id, stream_id))
            .map(|entry| entry.binding.clone())
        {
            binding.detach(CarrierPathKey { underlay, path_id }, commands);
        }
    }

    pub(in crate::runtime) async fn route_frame(
        &self,
        session_id: SessionId,
        stream_id: StreamId,
        frame: Frame,
    ) -> Result<(), RuntimeError> {
        #[cfg(feature = "lab-diagnostics")]
        let bytes = reliable_path_frame_pacing_bytes(&frame);
        let stream = {
            let streams = self
                .streams
                .lock()
                .expect("server reliable stream registry lock");
            streams
                .get(&(session_id, stream_id))
                .map(|entry| entry.frames.clone())
        };
        let Some(stream) = stream else {
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "server_stream_unknown_frame_drop",
                format_args!(
                    "session_id={} stream_id={} frame_kind={}",
                    session_id.0,
                    stream_id.0,
                    frame.kind_name(),
                ),
            );
            return Ok(());
        };
        #[cfg(feature = "lab-diagnostics")]
        let started = Instant::now();
        let result = stream
            .send(Ok(frame))
            .await
            .map_err(|_| RuntimeError::ReliablePathSessionClosed);
        #[cfg(feature = "lab-diagnostics")]
        lab_perf_record(
            "runtime.server_stream.route_frame",
            started.elapsed(),
            bytes,
        );
        result
    }

    pub(in crate::runtime) fn close(&self, session_id: SessionId, stream_id: StreamId) {
        let mut streams = self
            .streams
            .lock()
            .expect("server reliable stream registry lock");
        let removed = streams.remove(&(session_id, stream_id)).is_some();
        if removed {
            self.closed_streams
                .lock()
                .expect("server reliable stream closed cache lock")
                .insert((session_id, stream_id));
        }
        drop(streams);
    }
}

impl Default for ServerReliableStreamRegistry {
    fn default() -> Self {
        Self::new(ResourceLimits::default().max_streams)
    }
}

#[cfg(test)]
#[path = "registry_test.rs"]
mod tests;
