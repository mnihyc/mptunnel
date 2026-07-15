use super::handle::{ReliablePathStream, ReliablePathStreamOutput};
use super::response::{
    ResponseStreamAttachOutcome, ResponseStreamBinding, ServerPathLaneTracker,
    ServerPathMetricsEntry, ServerPathMetricsSource, ServerRealtimeFlowRegistration,
};
#[cfg(test)]
use crate::config::ResourceLimits;
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::{lab_diagnostic, lab_perf_record};
use crate::model::capacity::{
    QuicCapacityProofCandidate, reliable_stream_initial_advertised_window_bytes,
};
use crate::model::path::{CarrierPathInstanceId, CarrierPathKey};
#[cfg(feature = "lab-diagnostics")]
use crate::protocol::frame::reliable_path_frame_pacing_bytes;
use crate::protocol::{
    Frame, PathId, PathMetricDirection, PathMetrics, ResetReason, SessionId, StreamId,
    StreamOpenRole, TargetAddr, UnderlayProtocol,
};
use crate::runtime::RuntimeError;
#[cfg(test)]
use crate::runtime::path::ServerCarrierPathRegistration;
use crate::runtime::path::commands::{
    ReliablePathCommandSender, reliable_stream_frame_queue_for_payload,
};
use crate::runtime::path::tcp::capacity::{
    TcpCapacityProofCandidate, valid_tcp_capacity_proof_candidate_at,
};
use crate::runtime::path::{
    ServerCarrierPathIdentity, ServerCarrierPathMetricSnapshot, ServerNewStreamPolicy,
    ServerRealtimeFlowLease, ServerStreamManagementSnapshot, ServerStreamOpenOutcome,
    ServerStreamOpenRequest, ServerStreamPort, ServerStreamPortBackend,
};
use crate::runtime::recent_ids::{RecentIdCache, reliable_closed_stream_cache_capacity};
use crate::scheduler::FlowLane;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::{Mutex as AsyncMutex, mpsc};

/// Session-wide registry for server-side product reliable streams.
///
/// The registry owns stream lookup, target consistency, recent closed-stream
/// filtering, and peer/local path metrics for response scheduling. It does not
/// own target sockets or carrier packet state.
pub(in crate::runtime) struct ServerReliableStreamRegistry {
    max_streams: usize,
    accepted: mpsc::UnboundedSender<AcceptedServerReliableStream>,
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

pub(in crate::runtime) enum ServerReliableStreamOpen {
    New(AcceptedServerReliableStream),
    Existing,
    DuplicateLiveIgnored,
    Rejected,
}

/// One registry-admitted product stream awaiting or running its target relay.
///
/// Its retirement lease keeps registry membership alive until carrier output
/// has closed. The relay supervisor retains that lease across task aborts.
pub(in crate::runtime) struct AcceptedServerReliableStream {
    session_id: SessionId,
    target: TargetAddr,
    stream: Option<ReliablePathStream>,
    retirement: Arc<AcceptedServerReliableStreamRetirementInner>,
    supervised: bool,
}

struct AcceptedServerReliableStreamRetirementInner {
    registry: Arc<ServerReliableStreamRegistry>,
    session_id: SessionId,
    stream_id: StreamId,
    close_output: ReliablePathStreamOutput,
    state: AsyncMutex<AcceptedServerReliableStreamRetirementState>,
    registry_retired: AtomicBool,
}

struct AcceptedServerReliableStreamRetirementState {
    close_required: bool,
    mode: AcceptedServerReliableStreamCloseMode,
}

#[derive(Clone, Copy)]
enum AcceptedServerReliableStreamCloseMode {
    Unordered,
    Reset { reason: ResetReason, lane: FlowLane },
}

/// Supervisor-owned retirement for one accepted relay task.
pub(in crate::runtime) struct AcceptedServerReliableStreamRetirement {
    inner: Arc<AcceptedServerReliableStreamRetirementInner>,
    armed: bool,
}

struct ScheduledAcceptedServerReliableStreamRetirement {
    inner: Arc<AcceptedServerReliableStreamRetirementInner>,
}

impl AcceptedServerReliableStreamRetirementInner {
    async fn close_output(&self, state: &mut AcceptedServerReliableStreamRetirementState) {
        if !state.close_required {
            return;
        }
        match state.mode {
            AcceptedServerReliableStreamCloseMode::Unordered => {
                self.close_output.close_stream(self.stream_id).await;
            }
            AcceptedServerReliableStreamCloseMode::Reset { reason, lane } => {
                self.close_output
                    .reset_and_close_stream_ordered(self.stream_id, reason, lane)
                    .await;
            }
        }
        state.close_required = false;
    }

    async fn retire(&self, mode: AcceptedServerReliableStreamCloseMode) {
        // This lifecycle mutex may span the close await: cancellation drops
        // the guard with close_required intact, allowing the supervisor retry.
        let mut state = self.state.lock().await;
        if self.registry_retired.load(Ordering::Acquire) {
            return;
        }
        if matches!(mode, AcceptedServerReliableStreamCloseMode::Reset { .. }) {
            // Preserve terminal delivery across cancellation so a supervisor
            // retry cannot replace ordered Reset+Close with control-only close.
            state.mode = mode;
        }
        self.close_output(&mut state).await;
        self.retire_registry();
    }

    async fn mark_output_closed(&self) {
        self.state.lock().await.close_required = false;
    }

    fn retire_registry(&self) {
        if !self.registry_retired.swap(true, Ordering::AcqRel) {
            self.registry.close(self.session_id, self.stream_id);
        }
    }
}

impl Drop for ScheduledAcceptedServerReliableStreamRetirement {
    fn drop(&mut self) {
        // A runtime may discard a just-spawned future without polling it. The
        // carrier close is then impossible, but registry membership must not leak.
        self.inner.retire_registry();
    }
}

fn schedule_accepted_stream_retirement(
    retirement: Arc<AcceptedServerReliableStreamRetirementInner>,
) {
    if let Ok(runtime) = tokio::runtime::Handle::try_current() {
        let scheduled = ScheduledAcceptedServerReliableStreamRetirement { inner: retirement };
        runtime.spawn(async move {
            scheduled
                .inner
                .retire(AcceptedServerReliableStreamCloseMode::Unordered)
                .await;
        });
    } else {
        // No async executor remains to close carrier output. Removing registry
        // membership is the only teardown still possible at process/runtime drop.
        retirement.retire_registry();
    }
}

impl AcceptedServerReliableStream {
    pub(in crate::runtime) fn session_id(&self) -> SessionId {
        self.session_id
    }

    pub(in crate::runtime) fn target(&self) -> &TargetAddr {
        &self.target
    }

    pub(in crate::runtime) fn stream(&self) -> &ReliablePathStream {
        self.stream.as_ref().expect("accepted reliable stream")
    }

    pub(in crate::runtime) fn take_stream(&mut self) -> ReliablePathStream {
        self.stream.take().expect("accepted reliable stream")
    }

    pub(in crate::runtime) fn supervise(&mut self) -> AcceptedServerReliableStreamRetirement {
        debug_assert!(!self.supervised, "accepted stream already supervised");
        self.supervised = true;
        AcceptedServerReliableStreamRetirement {
            inner: self.retirement.clone(),
            armed: true,
        }
    }

    pub(in crate::runtime) async fn mark_closed(&mut self) {
        self.retirement.mark_output_closed().await;
    }

    pub(in crate::runtime) async fn close(mut self) {
        self.retirement
            .retire(AcceptedServerReliableStreamCloseMode::Unordered)
            .await;
        self.supervised = true;
    }

    pub(in crate::runtime) async fn reject(mut self, reason: ResetReason, lane: FlowLane) {
        self.retirement
            .retire(AcceptedServerReliableStreamCloseMode::Reset { reason, lane })
            .await;
        self.supervised = true;
    }
}

impl Drop for AcceptedServerReliableStream {
    fn drop(&mut self) {
        if !self.supervised {
            schedule_accepted_stream_retirement(self.retirement.clone());
        }
    }
}

impl AcceptedServerReliableStreamRetirement {
    pub(in crate::runtime) async fn retire(mut self) {
        self.inner
            .retire(AcceptedServerReliableStreamCloseMode::Unordered)
            .await;
        self.armed = false;
    }
}

impl Drop for AcceptedServerReliableStreamRetirement {
    fn drop(&mut self) {
        if self.armed {
            schedule_accepted_stream_retirement(self.inner.clone());
        }
    }
}

impl ServerReliableStreamRegistry {
    #[cfg(test)]
    pub(in crate::runtime) fn new(max_streams: usize) -> Self {
        let (accepted, _receiver) = mpsc::unbounded_channel();
        Self::with_accept_sender(max_streams, accepted)
    }

    /// Creates the registry and its uniquely paired relay receiver together.
    pub(in crate::runtime) fn new_accepting(
        max_streams: usize,
    ) -> (
        Arc<Self>,
        mpsc::UnboundedReceiver<AcceptedServerReliableStream>,
    ) {
        let (accepted, receiver) = mpsc::unbounded_channel();
        (
            Arc::new(Self::with_accept_sender(max_streams, accepted)),
            receiver,
        )
    }

    /// Adapts product registry ownership to the carrier-facing service contract.
    pub(in crate::runtime) fn path_port(self: &Arc<Self>) -> ServerStreamPort {
        ServerStreamPort::new(Arc::new(ServerReliableStreamPortBackend {
            registry: self.clone(),
        }))
    }

    #[cfg(test)]
    pub(in crate::runtime) fn register_carrier_path(
        self: &Arc<Self>,
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
    ) -> ServerCarrierPathRegistration {
        self.path_port()
            .register_carrier_path(session_id, underlay, path_id)
    }

    fn with_accept_sender(
        max_streams: usize,
        accepted: mpsc::UnboundedSender<AcceptedServerReliableStream>,
    ) -> Self {
        Self {
            max_streams,
            accepted,
            streams: Mutex::new(HashMap::new()),
            path_metrics: Mutex::new(HashMap::new()),
            active_path_instances: Mutex::new(HashSet::new()),
            closed_streams: Mutex::new(RecentIdCache::new(reliable_closed_stream_cache_capacity(
                max_streams,
            ))),
            lane_tracker: Arc::new(ServerPathLaneTracker::default()),
        }
    }

    /// Hands an admitted stream to this registry's paired target-relay service.
    pub(in crate::runtime) fn submit_accepted(
        &self,
        accepted: AcceptedServerReliableStream,
    ) -> Result<(), AcceptedServerReliableStream> {
        self.accepted.send(accepted).map_err(|error| error.0)
    }

    pub(in crate::runtime) fn management_snapshot(&self) -> ServerStreamManagementSnapshot {
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
        ServerStreamManagementSnapshot {
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

    fn accepted_stream(
        self: &Arc<Self>,
        session_id: SessionId,
        target: TargetAddr,
        stream: ReliablePathStream,
    ) -> AcceptedServerReliableStream {
        let stream_id = stream.stream_id;
        let close_output = stream.output.clone();
        AcceptedServerReliableStream {
            session_id,
            target,
            stream: Some(stream),
            retirement: Arc::new(AcceptedServerReliableStreamRetirementInner {
                registry: self.clone(),
                session_id,
                stream_id,
                close_output,
                state: AsyncMutex::new(AcceptedServerReliableStreamRetirementState {
                    close_required: true,
                    mode: AcceptedServerReliableStreamCloseMode::Unordered,
                }),
                registry_retired: AtomicBool::new(false),
            }),
            supervised: false,
        }
    }

    fn activate_carrier_path(&self, identity: ServerCarrierPathIdentity) {
        let ServerCarrierPathIdentity {
            session_id,
            underlay,
            path_id,
            path_instance_id,
        } = identity;
        // Carrier lifetime, not response-stream occupancy, bounds cumulative
        // per-session discovery spend across zero-stream gaps.
        self.lane_tracker.attach_session(session_id);
        self.active_path_instances
            .lock()
            .expect("server active path instance lock")
            .insert((session_id, underlay, path_id, path_instance_id));
    }

    fn retire_carrier_path(&self, identity: ServerCarrierPathIdentity) {
        let ServerCarrierPathIdentity {
            session_id,
            underlay,
            path_id,
            path_instance_id,
        } = identity;
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
        self.lane_tracker.detach_session(session_id);
    }

    pub(in crate::runtime) fn open_or_attach(
        self: &Arc<Self>,
        request: ServerStreamOpenRequest,
    ) -> Result<ServerReliableStreamOpen, RuntimeError> {
        let ServerStreamOpenRequest {
            session_id,
            stream_id,
            target,
            lane,
            attachment,
            mux_limits,
        } = request;
        let crate::runtime::path::ServerStreamPathAttachment {
            path_registration,
            commands,
            max_frame_payload_bytes,
            role,
            initial_metrics,
        } = attachment;
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
            if entry.target != target {
                return Err(RuntimeError::Protocol(
                    "reliable stream migration target does not match original stream",
                ));
            }
            let attach_outcome = entry.binding.attach_with_path_instance(
                underlay,
                path_id,
                path_instance_id,
                commands,
                lane,
                role,
            );
            if matches!(
                attach_outcome,
                ResponseStreamAttachOutcome::RejectedClosedStream
            ) {
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "server_stream_open",
                    format_args!(
                        "session_id={} stream_id={} path_underlay={:?} path_id={} role={:?} lane={:?} result=rejected_closing_stream",
                        session_id.0, stream_id.0, underlay, path_id.0, role, lane,
                    ),
                );
                return Ok(ServerReliableStreamOpen::Rejected);
            }
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
                    ResponseStreamAttachOutcome::RejectedClosedStream => "rejected_closed_stream",
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
            entry.lane = lane;
            if !matches!(
                attach_outcome,
                ResponseStreamAttachOutcome::RejectedDuplicateLiveOutput
                    | ResponseStreamAttachOutcome::RejectedClosedStream
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

        if streams.len() >= self.max_streams {
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
        let stream = ReliablePathStream {
            stream_id,
            max_offset: initial_max_offset,
            lane,
            underlay,
            max_frame_payload_bytes,
            output: ReliablePathStreamOutput::Switchable(binding),
            frames: frames_rx,
        };
        Ok(ServerReliableStreamOpen::New(
            self.accepted_stream(session_id, target, stream),
        ))
    }

    pub(in crate::runtime) fn record_path_metrics(
        &self,
        identity: ServerCarrierPathIdentity,
        metrics: PathMetrics,
    ) {
        self.record_path_metrics_with_source(identity, metrics, ServerPathMetricsSource::PeerHint);
    }

    pub(in crate::runtime) fn record_local_path_metrics(
        &self,
        identity: ServerCarrierPathIdentity,
        metrics: PathMetrics,
    ) {
        self.record_path_metrics_with_source(
            identity,
            metrics,
            ServerPathMetricsSource::LocalSender,
        );
    }

    /// Publishes a carrier proof only after its exact session reservation has
    /// accepted the frozen train specification. Generic metrics cannot call it.
    pub(in crate::runtime) fn record_local_quic_capacity_proof(
        &self,
        identity: ServerCarrierPathIdentity,
        mut metrics: PathMetrics,
        candidate: QuicCapacityProofCandidate,
    ) -> bool {
        if identity.underlay != UnderlayProtocol::Udp
            || metrics.underlay != UnderlayProtocol::Udp
            || metrics.direction != PathMetricDirection::ServerToClient
        {
            return false;
        }
        let ServerCarrierPathIdentity {
            session_id,
            path_id,
            path_instance_id,
            ..
        } = identity;
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
        identity: ServerCarrierPathIdentity,
        mut metrics: PathMetrics,
        candidate: TcpCapacityProofCandidate,
    ) -> bool {
        if identity.underlay != UnderlayProtocol::Tcp
            || metrics.underlay != UnderlayProtocol::Tcp
            || metrics.direction != PathMetricDirection::ServerToClient
            || !valid_tcp_capacity_proof_candidate_at(candidate, Instant::now())
        {
            return false;
        }
        let ServerCarrierPathIdentity {
            session_id,
            path_id,
            path_instance_id,
            ..
        } = identity;
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
        identity: ServerCarrierPathIdentity,
        metrics: PathMetrics,
        source: ServerPathMetricsSource,
    ) {
        let ServerCarrierPathIdentity {
            session_id,
            underlay,
            path_id,
            path_instance_id,
        } = identity;
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

struct ServerReliableStreamPortBackend {
    registry: Arc<ServerReliableStreamRegistry>,
}

impl ServerStreamPortBackend for ServerReliableStreamPortBackend {
    fn owner_token(&self) -> usize {
        Arc::as_ptr(&self.registry) as usize
    }

    fn activate_carrier_path(&self, identity: ServerCarrierPathIdentity) {
        self.registry.activate_carrier_path(identity);
    }

    fn retire_carrier_path(&self, identity: ServerCarrierPathIdentity) {
        self.registry.retire_carrier_path(identity);
    }

    fn register_realtime_flow(&self, session_id: SessionId) -> ServerRealtimeFlowLease {
        ServerRealtimeFlowLease::hold(self.registry.register_realtime_flow(session_id))
    }

    fn open_or_attach<'a>(
        &'a self,
        request: ServerStreamOpenRequest,
        new_stream_policy: ServerNewStreamPolicy,
    ) -> Pin<Box<dyn Future<Output = Result<ServerStreamOpenOutcome, RuntimeError>> + Send + 'a>>
    {
        let registry = self.registry.clone();
        Box::pin(async move {
            match registry.open_or_attach(request)? {
                ServerReliableStreamOpen::New(accepted) => {
                    match new_stream_policy {
                        ServerNewStreamPolicy::Submit => {
                            if let Err(accepted) = registry.submit_accepted(accepted) {
                                accepted.close().await;
                                return Err(RuntimeError::Protocol(
                                    "server reliable stream service closed",
                                ));
                            }
                        }
                        ServerNewStreamPolicy::Reject => accepted.close().await,
                    }
                    Ok(ServerStreamOpenOutcome::New)
                }
                ServerReliableStreamOpen::Existing => Ok(ServerStreamOpenOutcome::Existing),
                ServerReliableStreamOpen::DuplicateLiveIgnored => {
                    Ok(ServerStreamOpenOutcome::DuplicateLiveIgnored)
                }
                ServerReliableStreamOpen::Rejected => Ok(ServerStreamOpenOutcome::Rejected),
            }
        })
    }

    fn route_frame<'a>(
        &'a self,
        session_id: SessionId,
        stream_id: StreamId,
        frame: Frame,
    ) -> Pin<Box<dyn Future<Output = Result<(), RuntimeError>> + Send + 'a>> {
        Box::pin(self.registry.route_frame(session_id, stream_id, frame))
    }

    fn detach_path(
        &self,
        session_id: SessionId,
        stream_id: StreamId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: &ReliablePathCommandSender,
    ) {
        self.registry
            .detach_path(session_id, stream_id, underlay, path_id, commands);
    }

    fn record_peer_path_metrics(&self, identity: ServerCarrierPathIdentity, metrics: PathMetrics) {
        self.registry.record_path_metrics(identity, metrics);
    }

    fn record_local_path_metrics(&self, identity: ServerCarrierPathIdentity, metrics: PathMetrics) {
        self.registry.record_local_path_metrics(identity, metrics);
    }

    fn record_local_quic_capacity_proof(
        &self,
        identity: ServerCarrierPathIdentity,
        metrics: PathMetrics,
        candidate: QuicCapacityProofCandidate,
    ) -> bool {
        self.registry
            .record_local_quic_capacity_proof(identity, metrics, candidate)
    }

    fn record_local_tcp_capacity_proof(
        &self,
        identity: ServerCarrierPathIdentity,
        metrics: PathMetrics,
        candidate: TcpCapacityProofCandidate,
    ) -> bool {
        self.registry
            .record_local_tcp_capacity_proof(identity, metrics, candidate)
    }

    fn management_snapshot(&self) -> ServerStreamManagementSnapshot {
        self.registry.management_snapshot()
    }
}

#[cfg(test)]
impl Default for ServerReliableStreamRegistry {
    fn default() -> Self {
        Self::new(ResourceLimits::default().max_streams)
    }
}

#[cfg(test)]
#[path = "registry_test.rs"]
mod tests;
