use super::handle::{ReliablePathStream, ReliablePathStreamOutput};
use super::response::{
    ResponseStreamAttachOutcome, ResponseStreamBinding, ServerPathMetricsEntry,
    ServerPathMetricsSource, ServerSessionRegistration, ServerSessionTracker,
};
#[cfg(test)]
use crate::config::ResourceLimits;
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::{lab_diagnostic, lab_perf_record};
use crate::model::capacity::reliable_stream_initial_advertised_window_bytes;
use crate::model::path::{CarrierPathInstanceId, CarrierPathKey};
#[cfg(feature = "lab-diagnostics")]
use crate::protocol::frame::reliable_path_frame_pacing_bytes;
use crate::protocol::{
    Frame, PathId, PathMetrics, PathUsage, PeerPathState, PeerPathStatus, ResetReason, SessionId,
    StreamId, TargetAddr, UnderlayProtocol,
};
use crate::runtime::RuntimeError;
#[cfg(test)]
use crate::runtime::path::ServerCarrierPathRegistration;
use crate::runtime::path::commands::{
    ReliablePathCommandSender, reliable_stream_frame_queue_for_payload,
};
use crate::runtime::path::{
    ServerCarrierPathIdentity, ServerCarrierPathStatusSnapshot, ServerNewStreamPolicy,
    ServerRealtimeFlowLease, ServerSessionManagementSnapshot, ServerStreamManagementSnapshot,
    ServerStreamOpenOutcome, ServerStreamOpenRequest, ServerStreamPort, ServerStreamPortBackend,
};
use crate::runtime::recent_ids::{RecentIdCache, reliable_closed_stream_cache_capacity};
use crate::scheduler::TrafficClass;
use std::collections::HashMap;
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
    path_usage: Mutex<
        HashMap<(SessionId, UnderlayProtocol, PathId, CarrierPathInstanceId), ServerPathUsageEntry>,
    >,
    registered_path_instances: Mutex<
        HashMap<(SessionId, UnderlayProtocol, PathId, CarrierPathInstanceId), ServerRegisteredPath>,
    >,
    closed_streams: Mutex<RecentIdCache<(SessionId, StreamId)>>,
    session_tracker: Arc<ServerSessionTracker>,
}

impl std::fmt::Debug for ServerReliableStreamRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerReliableStreamRegistry")
            .finish_non_exhaustive()
    }
}

struct ServerReliableStreamEntry {
    target: TargetAddr,
    frames: mpsc::Sender<Result<Frame, RuntimeError>>,
    binding: Arc<ResponseStreamBinding>,
}

#[derive(Debug, Clone, Copy)]
struct ServerPathUsageEntry {
    sequence: u64,
    usage: PathUsage,
}

#[derive(Debug, Clone, Copy)]
struct ServerRegisteredPath {
    local: crate::runtime::path::ServerLocalPathProperties,
    state: PeerPathState,
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
    Reset {
        reason: ResetReason,
        lane: TrafficClass,
    },
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

    pub(in crate::runtime) async fn reject(mut self, reason: ResetReason, lane: TrafficClass) {
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
        self.path_port().register_carrier_path(
            session_id,
            underlay,
            path_id,
            crate::runtime::path::ServerLocalPathProperties::default(),
        )
    }

    #[cfg(test)]
    pub(in crate::runtime) fn register_carrier_path_with_local_properties(
        self: &Arc<Self>,
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        local: crate::runtime::path::ServerLocalPathProperties,
    ) -> ServerCarrierPathRegistration {
        self.path_port()
            .register_carrier_path(session_id, underlay, path_id, local)
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
            path_usage: Mutex::new(HashMap::new()),
            registered_path_instances: Mutex::new(HashMap::new()),
            closed_streams: Mutex::new(RecentIdCache::new(reliable_closed_stream_cache_capacity(
                max_streams,
            ))),
            session_tracker: Arc::new(ServerSessionTracker::default()),
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
        #[cfg(test)]
        let active_streams = self.streams.lock().expect("server stream lock").len();
        let now = Instant::now();
        let registered = self
            .registered_path_instances
            .lock()
            .expect("server active path instance lock")
            .iter()
            .map(|(identity, path)| (*identity, *path))
            .collect::<Vec<_>>();
        let path_metrics = self.path_metrics.lock().expect("server path metrics lock");
        let path_usage = self.path_usage.lock().expect("server path usage lock");
        let mut paths = registered
            .into_iter()
            .map(
                |((session_id, underlay, path_id, path_instance_id), registered)| {
                    let identity = (session_id, underlay, path_id, path_instance_id);
                    let (metrics, source) = path_metrics.get(&identity).map_or_else(
                        || {
                            (
                                registered.local.initial_metrics,
                                registered.local.initial_metrics.map(|_| "startup"),
                            )
                        },
                        |entry| {
                            let residence_us =
                                now.saturating_duration_since(entry.recorded_at).as_micros();
                            (
                                Some(PathMetrics {
                                    metric_age_us: entry.metrics.metric_age_us.saturating_add(
                                        u32::try_from(residence_us).unwrap_or(u32::MAX),
                                    ),
                                    ..entry.metrics
                                }),
                                Some(match entry.source {
                                    ServerPathMetricsSource::PeerHint => "peer_hint",
                                    ServerPathMetricsSource::LocalSender => "local_sender",
                                }),
                            )
                        },
                    );
                    ServerCarrierPathStatusSnapshot {
                        session_id,
                        underlay,
                        path_id,
                        path_instance_id,
                        configured_index: registered.local.config_ordinal,
                        policy: registered.local.policy,
                        state: registered.state,
                        usage: path_usage.get(&identity).map(|entry| entry.usage),
                        metrics,
                        source,
                    }
                },
            )
            .collect::<Vec<_>>();
        paths.sort_unstable_by_key(|path| {
            (
                path.session_id,
                path.underlay,
                path.path_id,
                path.path_instance_id.as_u64(),
            )
        });
        let sessions = self
            .session_tracker
            .management_snapshot()
            .into_iter()
            .map(
                |(session_id, reference_count)| ServerSessionManagementSnapshot {
                    session_id,
                    reference_count,
                },
            )
            .collect();
        ServerStreamManagementSnapshot {
            #[cfg(test)]
            active_streams,
            paths,
            sessions,
        }
    }

    pub(in crate::runtime) fn peer_status_snapshot(
        &self,
        session_id: SessionId,
    ) -> Vec<PeerPathStatus> {
        let now = Instant::now();
        let registered = self
            .registered_path_instances
            .lock()
            .expect("server active path instance lock");
        let metrics = self.path_metrics.lock().expect("server path metrics lock");
        let mut paths = HashMap::<(UnderlayProtocol, PathId), PeerPathStatus>::new();
        for ((entry_session, underlay, path_id, path_instance_id), registered_path) in
            registered.iter()
        {
            if *entry_session != session_id {
                continue;
            }
            let instance_key = (*entry_session, *underlay, *path_id, *path_instance_id);
            let current = metrics
                .get(&instance_key)
                .filter(|entry| entry.source == ServerPathMetricsSource::LocalSender)
                .map(|entry| {
                    let residence_us = now.saturating_duration_since(entry.recorded_at).as_micros();
                    PathMetrics {
                        metric_age_us: entry
                            .metrics
                            .metric_age_us
                            .saturating_add(u32::try_from(residence_us).unwrap_or(u32::MAX)),
                        ..entry.metrics
                    }
                })
                .or(registered_path.local.initial_metrics);
            let Some(metrics) = current else {
                continue;
            };
            let candidate = PeerPathStatus {
                state: registered_path.state,
                usage: if registered_path.local.policy.backup {
                    PathUsage::Backup
                } else {
                    PathUsage::Available
                },
                metrics: PathMetrics {
                    path_id: *path_id,
                    underlay: *underlay,
                    ..metrics
                },
            };
            paths
                .entry((*underlay, *path_id))
                .and_modify(|current| {
                    if candidate.metrics.metric_epoch >= current.metrics.metric_epoch {
                        *current = candidate;
                    }
                })
                .or_insert(candidate);
        }
        let mut paths = paths.into_iter().collect::<Vec<_>>();
        paths.sort_unstable_by_key(|((underlay, path_id), _)| (*underlay, *path_id));
        paths.into_iter().map(|(_, status)| status).collect()
    }

    pub(in crate::runtime) fn register_realtime_flow(
        &self,
        session_id: SessionId,
    ) -> ServerSessionRegistration {
        ServerSessionRegistration::new(self.session_tracker.clone(), session_id)
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

    fn activate_carrier_path(
        &self,
        identity: ServerCarrierPathIdentity,
        local: crate::runtime::path::ServerLocalPathProperties,
    ) {
        let ServerCarrierPathIdentity {
            session_id,
            underlay,
            path_id,
            path_instance_id,
        } = identity;
        let inserted = self
            .registered_path_instances
            .lock()
            .expect("server active path instance lock")
            .insert(
                (session_id, underlay, path_id, path_instance_id),
                ServerRegisteredPath {
                    local,
                    state: PeerPathState::Active,
                },
            )
            .is_none();
        if inserted {
            self.session_tracker.attach_session(session_id);
        }
    }

    fn set_carrier_path_state(&self, identity: ServerCarrierPathIdentity, state: PeerPathState) {
        let key = (
            identity.session_id,
            identity.underlay,
            identity.path_id,
            identity.path_instance_id,
        );
        if let Some(path) = self
            .registered_path_instances
            .lock()
            .expect("server active path instance lock")
            .get_mut(&key)
        {
            path.state = state;
        }
    }

    fn retire_carrier_path(&self, identity: ServerCarrierPathIdentity) {
        let ServerCarrierPathIdentity {
            session_id,
            underlay,
            path_id,
            path_instance_id,
        } = identity;
        let removed = self
            .registered_path_instances
            .lock()
            .expect("server active path instance lock")
            .remove(&(session_id, underlay, path_id, path_instance_id));
        self.path_metrics
            .lock()
            .expect("server path metrics lock")
            .remove(&(session_id, underlay, path_id, path_instance_id));
        self.path_usage
            .lock()
            .expect("server path usage lock")
            .remove(&(session_id, underlay, path_id, path_instance_id));
        let key = CarrierPathKey { underlay, path_id };
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
        if removed.is_some() {
            self.session_tracker.detach_session(session_id);
        }
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
        } = attachment;
        let initial_metrics = path_registration.initial_metrics();
        let local_policy = path_registration.local_policy();
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
        let initial_usage = self.stored_path_usage(session_id, underlay, path_id, path_instance_id);
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
                local_policy,
                commands,
                lane,
            );
            if matches!(
                attach_outcome,
                ResponseStreamAttachOutcome::RejectedClosedStream
            ) {
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "server_stream_open",
                    format_args!(
                        "session_id={} stream_id={} path_underlay={:?} path_id={} lane={:?} result=rejected_closing_stream",
                        session_id.0, stream_id.0, underlay, path_id.0, lane,
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
                    ResponseStreamAttachOutcome::ReplacedClosedOutput => "replaced_closed_output",
                    ResponseStreamAttachOutcome::RejectedClosedStream => "rejected_closed_stream",
                };
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "server_stream_open",
                    format_args!(
                        "session_id={} stream_id={} path_underlay={:?} path_id={} lane={:?} result={}",
                        session_id.0, stream_id.0, underlay, path_id.0, lane, result,
                    ),
                );
                return Ok(ServerReliableStreamOpen::DuplicateLiveIgnored);
            }
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
            if !matches!(
                attach_outcome,
                ResponseStreamAttachOutcome::RejectedDuplicateLiveOutput
                    | ResponseStreamAttachOutcome::RejectedClosedStream
            ) && let Some(usage) = initial_usage
            {
                entry.binding.update_peer_path_usage_for_instance(
                    CarrierPathKey { underlay, path_id },
                    path_instance_id,
                    usage.sequence,
                    usage.usage,
                );
            }
            entry.binding.record_request_feedback_ingress(
                CarrierPathKey { underlay, path_id },
                path_instance_id,
            );
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "server_stream_open",
                format_args!(
                    "session_id={} stream_id={} path_underlay={:?} path_id={} lane={:?} result=existing",
                    session_id.0, stream_id.0, underlay, path_id.0, lane,
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
                    "session_id={} stream_id={} path_underlay={:?} path_id={} lane={:?} result=rejected_closed_stream",
                    session_id.0, stream_id.0, underlay, path_id.0, lane,
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
            self.session_tracker.clone(),
            path_instance_id,
            local_policy,
        );
        if let Some(metrics) = initial_metrics {
            binding.install_stored_path_metrics_for_instance(
                CarrierPathKey { underlay, path_id },
                path_instance_id,
                metrics,
            );
        }
        if let Some(usage) = initial_usage {
            binding.update_peer_path_usage_for_instance(
                CarrierPathKey { underlay, path_id },
                path_instance_id,
                usage.sequence,
                usage.usage,
            );
        }
        streams.insert(
            (session_id, stream_id),
            ServerReliableStreamEntry {
                target: target.clone(),
                frames: frames_tx,
                binding: binding.clone(),
            },
        );
        #[cfg(feature = "lab-diagnostics")]
        lab_diagnostic(
            "server_stream_open",
            format_args!(
                "session_id={} stream_id={} path_underlay={:?} path_id={} lane={:?} result=new",
                session_id.0, stream_id.0, underlay, path_id.0, lane,
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

    pub(in crate::runtime) fn record_peer_path_usage(
        &self,
        identity: ServerCarrierPathIdentity,
        sequence: u64,
        usage: PathUsage,
    ) {
        let ServerCarrierPathIdentity {
            session_id,
            underlay,
            path_id,
            path_instance_id,
        } = identity;
        let instance_key = (session_id, underlay, path_id, path_instance_id);
        if !self
            .registered_path_instances
            .lock()
            .expect("server active path instance lock")
            .contains_key(&instance_key)
        {
            return;
        }
        let changed = {
            let mut path_usage = self.path_usage.lock().expect("server path usage lock");
            if path_usage
                .get(&instance_key)
                .is_some_and(|current| sequence <= current.sequence)
            {
                false
            } else {
                path_usage.insert(instance_key, ServerPathUsageEntry { sequence, usage });
                true
            }
        };
        if !changed {
            return;
        }
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
            binding.update_peer_path_usage_for_instance(key, path_instance_id, sequence, usage);
        }
    }

    fn stored_path_usage(
        &self,
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        path_instance_id: CarrierPathInstanceId,
    ) -> Option<ServerPathUsageEntry> {
        self.path_usage
            .lock()
            .expect("server path usage lock")
            .get(&(session_id, underlay, path_id, path_instance_id))
            .copied()
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
        let registered_path_instances = self
            .registered_path_instances
            .lock()
            .expect("server active path instance lock");
        if !registered_path_instances.contains_key(&instance_key) {
            return;
        }
        let metrics = PathMetrics { path_id, ..metrics };
        let mut path_metrics = self.path_metrics.lock().expect("server path metrics lock");
        let previous = path_metrics.get(&instance_key).copied();
        let capacity_proof = previous
            .and_then(|previous| previous.capacity_proof)
            .filter(|proof| proof.expires_at > Instant::now());
        let entry = ServerPathMetricsEntry {
            metrics,
            source,
            recorded_at: Instant::now(),
            capacity_proof,
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
        drop(registered_path_instances);
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

    async fn route_frame_from_path(
        &self,
        identity: ServerCarrierPathIdentity,
        stream_id: StreamId,
        frame: Frame,
    ) -> Result<(), RuntimeError> {
        let ServerCarrierPathIdentity {
            session_id,
            underlay,
            path_id,
            path_instance_id,
        } = identity;
        if matches!(&frame, Frame::StreamData { .. } | Frame::StreamFin { .. })
            && let Some(binding) = self
                .streams
                .lock()
                .expect("server reliable stream registry lock")
                .get(&(session_id, stream_id))
                .map(|entry| entry.binding.clone())
        {
            // Connection-level feedback may return on any carrier. Remember
            // ingress only as a return-path hint, never as fixed path ownership.
            binding.record_request_feedback_ingress(
                CarrierPathKey { underlay, path_id },
                path_instance_id,
            );
        }
        self.route_frame(session_id, stream_id, frame).await
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

    fn activate_carrier_path(
        &self,
        identity: ServerCarrierPathIdentity,
        local: crate::runtime::path::ServerLocalPathProperties,
    ) {
        self.registry.activate_carrier_path(identity, local);
    }

    fn retire_carrier_path(&self, identity: ServerCarrierPathIdentity) {
        self.registry.retire_carrier_path(identity);
    }

    fn set_carrier_path_state(&self, identity: ServerCarrierPathIdentity, state: PeerPathState) {
        self.registry.set_carrier_path_state(identity, state);
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
        identity: ServerCarrierPathIdentity,
        stream_id: StreamId,
        frame: Frame,
    ) -> Pin<Box<dyn Future<Output = Result<(), RuntimeError>> + Send + 'a>> {
        Box::pin(
            self.registry
                .route_frame_from_path(identity, stream_id, frame),
        )
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

    fn record_peer_path_usage(
        &self,
        identity: ServerCarrierPathIdentity,
        sequence: u64,
        usage: PathUsage,
    ) {
        self.registry
            .record_peer_path_usage(identity, sequence, usage);
    }

    fn record_local_path_metrics(&self, identity: ServerCarrierPathIdentity, metrics: PathMetrics) {
        self.registry.record_local_path_metrics(identity, metrics);
    }

    fn peer_status_snapshot(&self, session_id: SessionId) -> Vec<PeerPathStatus> {
        self.registry.peer_status_snapshot(session_id)
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
