use super::handle::{
    ReliablePathStream, ReliablePathStreamInput, ReliablePathStreamOutput,
    ServerReliableStreamEvent,
};
use super::response::{
    ResponseOutputAttachment, ResponseOutputAttachmentState, ResponsePathDetachOutcome,
    ResponseStartupFinalOutcome, ResponseStreamAttachOutcome, ResponseStreamBinding,
    ServerPathMetricsEntry, ServerPathMetricsSource, ServerSessionRegistration,
    ServerSessionTracker,
};
use super::send_buffer::SessionSendBuffer;
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::{lab_diagnostic, lab_perf_record};
use crate::model::path::{CarrierPathInstanceId, CarrierPathKey};
use crate::mux::MuxLimits;
#[cfg(test)]
use crate::performance::ResourceLimits;
use crate::product::PrincipalPermit;
#[cfg(feature = "lab-diagnostics")]
use crate::protocol::frame::reliable_path_frame_pacing_bytes;
use crate::protocol::{
    ConfiguredMemberSlot, Frame, PathId, PathMetrics, PathUsage, PeerPathState, PeerPathStatus,
    ResetReason, SessionId, StreamDemandHint, StreamId, TargetAddr, UnderlayProtocol,
};
use crate::runtime::RuntimeError;
#[cfg(test)]
use crate::runtime::path::ServerCarrierPathRegistration;
use crate::runtime::path::authority::NativeCarrierSchedulingShapeSnapshot;
#[cfg(test)]
use crate::runtime::path::commands::ReliablePathCommand;
use crate::runtime::path::commands::{
    ReliablePathCommandSender, reliable_stream_frame_queue_for_payload,
};
use crate::runtime::path::proof::PathProofObservation;
use crate::runtime::path::{
    CarrierDeliveryRateSample, CarrierNativeWindowSample, ServerCarrierPathApplyAuthority,
    ServerCarrierPathIdentity, ServerCarrierPathRetirement, ServerCarrierPathStatusSnapshot,
    ServerMppIngress, ServerNewStreamPolicy, ServerPathValidation, ServerRealtimeFlowLease,
    ServerSessionManagementSnapshot, ServerStreamFrameRoute, ServerStreamManagementSnapshot,
    ServerStreamOpenOutcome, ServerStreamOpenRequest, ServerStreamPort, ServerStreamPortBackend,
};
use crate::runtime::recent_ids::{RecentIdCache, reliable_closed_stream_cache_capacity};
use crate::scheduler::{TrafficClass, traffic_class_from_stream_demand_hint};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex as AsyncMutex, mpsc, watch};

/// Session-wide registry for server-side product reliable streams.
///
/// The registry owns stream lookup, target consistency, recent closed-stream
/// filtering, and peer/local path metrics for response scheduling. It does not
/// own target sockets or carrier packet state.
pub(in crate::runtime) struct ServerReliableStreamRegistry {
    max_streams: usize,
    max_paths_per_session: usize,
    max_carrier_paths: usize,
    accepted: mpsc::UnboundedSender<AcceptedServerReliableStream>,
    streams: Mutex<HashMap<(SessionId, StreamId), ServerReliableStreamEntry>>,
    path_metrics: Mutex<
        HashMap<
            (SessionId, UnderlayProtocol, PathId, CarrierPathInstanceId),
            ServerPathMetricsEntry,
        >,
    >,
    registered_path_instances: Mutex<ServerCarrierPathRegistry>,
    closed_streams: Mutex<RecentIdCache<(SessionId, StreamId)>>,
    session_tracker: Arc<ServerSessionTracker>,
    #[cfg(test)]
    carrier_activation_after_session_attach_hook:
        Mutex<Option<Arc<dyn Fn() + Send + Sync + 'static>>>,
}

impl std::fmt::Debug for ServerReliableStreamRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerReliableStreamRegistry")
            .finish_non_exhaustive()
    }
}

struct ServerReliableStreamEntry {
    target: TargetAddr,
    initial_demand: StreamDemandHint,
    events: mpsc::Sender<ServerReliableStreamEvent>,
    binding: Arc<ResponseStreamBinding>,
}

struct ServerStreamFrameRouteTarget {
    events: mpsc::Sender<ServerReliableStreamEvent>,
    binding: Arc<ResponseStreamBinding>,
}

#[derive(Debug, Clone, Copy)]
struct ServerPathUsageEntry {
    sequence: u64,
    usage: PathUsage,
}

#[derive(Clone)]
struct ServerRegisteredPath {
    /// Authenticated peer configuration identity, stable across a physical
    /// replacement which receives a new PathId and instance identity.
    configured_slot: ConfiguredMemberSlot,
    local: crate::runtime::path::ServerLocalPathProperties,
    state: PeerPathState,
    peer_usage: Option<ServerPathUsageEntry>,
    /// Exact local congestion-controller/network-path lifetime. TCP uses zero
    /// because physical socket replacement already changes `path_instance_id`;
    /// QUIC publishes Quinn's path epoch independently of optional rate data.
    native_capacity_epoch: u64,
    /// Single registry-owned structural/Native apply fence shared with packet
    /// attachments. It contains no ACK/Product-derived rate.
    apply_authority: ServerCarrierPathApplyAuthority,
    path_proof: Option<PathProofObservation>,
    retirement_started: bool,
    retirement_completion: watch::Sender<bool>,
}

#[derive(Debug, Clone, Copy)]
struct ServerCarrierPathStatusBasis {
    identity: ServerCarrierPathIdentity,
    local: crate::runtime::path::ServerLocalPathProperties,
    state: PeerPathState,
    usage: Option<PathUsage>,
    eligibility_epoch: Option<u64>,
    native_scheduling_shape: Option<NativeCarrierSchedulingShapeSnapshot>,
}

impl ServerCarrierPathStatusBasis {
    /// Capture structural state and its apply epoch under the registry owner.
    /// Callers must hold `registered_path_instances` while invoking this.
    fn capture(identity: ServerCarrierPathIdentity, path: &ServerRegisteredPath) -> Self {
        let apply = path.apply_authority.snapshot();
        Self {
            identity,
            local: path.local,
            state: path.state,
            usage: path.peer_usage.map(|entry| entry.usage),
            eligibility_epoch: apply.eligibility_epoch,
            native_scheduling_shape: apply.native_scheduling_shape,
        }
    }
}

impl ServerRegisteredPath {
    fn advance_eligibility_epoch(&mut self) {
        self.apply_authority.advance_eligibility_epoch();
    }

    fn set_state(&mut self, state: PeerPathState) -> bool {
        if self.state == state {
            return false;
        }
        self.advance_eligibility_epoch();
        self.state = state;
        true
    }

    fn update_peer_usage(&mut self, sequence: u64, usage: PathUsage) -> bool {
        if self
            .peer_usage
            .is_some_and(|current| sequence <= current.sequence)
        {
            return false;
        }
        let eligibility_changed = self.peer_usage.map(|current| current.usage) != Some(usage);
        if eligibility_changed {
            self.advance_eligibility_epoch();
        }
        self.peer_usage = Some(ServerPathUsageEntry { sequence, usage });
        true
    }

    fn begin_retirement(&mut self) -> bool {
        if self.retirement_started {
            return false;
        }
        self.advance_eligibility_epoch();
        self.retirement_started = true;
        self.state = PeerPathState::Draining;
        true
    }

    /// Accepts one observation from the exact local QUIC controller lifetime.
    /// A late publication from an older path epoch cannot restore obsolete
    /// native capacity or overwrite the current path's diagnostics.
    fn observe_native_capacity_epoch(&mut self, epoch: u64) -> bool {
        if epoch < self.native_capacity_epoch {
            return false;
        }
        self.native_capacity_epoch = epoch;
        true
    }
}

type ServerLogicalPathKey = (SessionId, UnderlayProtocol, PathId);
type ServerPhysicalPathKey = (SessionId, UnderlayProtocol, PathId, CarrierPathInstanceId);

#[derive(Default)]
struct ServerCarrierPathRegistry {
    instances: HashMap<ServerPhysicalPathKey, ServerRegisteredPath>,
    logical_instances: HashMap<ServerLogicalPathKey, CarrierPathInstanceId>,
    session_path_counts: HashMap<SessionId, usize>,
}

fn server_physical_path_key(identity: ServerCarrierPathIdentity) -> ServerPhysicalPathKey {
    (
        identity.session_id,
        identity.underlay,
        identity.path_id,
        identity.path_instance_id,
    )
}

fn server_logical_path_key(identity: ServerCarrierPathIdentity) -> ServerLogicalPathKey {
    (identity.session_id, identity.underlay, identity.path_id)
}

fn decrement_session_path_count(
    session_path_counts: &mut HashMap<SessionId, usize>,
    session_id: SessionId,
) {
    let Some(count) = session_path_counts.get_mut(&session_id) else {
        debug_assert!(false, "missing server session path count");
        return;
    };
    debug_assert!(*count > 0, "server session path count underflow");
    *count = count.saturating_sub(1);
    if *count == 0 {
        session_path_counts.remove(&session_id);
    }
}

struct PendingOrderedPathDetach {
    events: mpsc::Sender<ServerReliableStreamEvent>,
    binding: Arc<ResponseStreamBinding>,
    key: CarrierPathKey,
    path_instance_id: CarrierPathInstanceId,
    output_incarnation: u64,
    event: ServerReliableStreamEvent,
}

struct ExistingOrderedPathDetach {
    events: mpsc::Sender<ServerReliableStreamEvent>,
    binding: Arc<ResponseStreamBinding>,
    key: CarrierPathKey,
    path_instance_id: CarrierPathInstanceId,
    output_incarnation: u64,
}

enum OrderedPathDetachCompletion {
    Pending(PendingOrderedPathDetach),
    Existing(ExistingOrderedPathDetach),
}

impl OrderedPathDetachCompletion {
    async fn wait(self) {
        match self {
            Self::Pending(detach) => detach.send().await,
            Self::Existing(detach) => detach.wait().await,
        }
    }
}

impl ExistingOrderedPathDetach {
    async fn wait(self) {
        let mut updates = self.binding.subscribe_updates();
        loop {
            if !self
                .binding
                .has_output_incarnation(self.key, self.output_incarnation)
            {
                return;
            }
            tokio::select! {
                changed = updates.changed() => {
                    if changed.is_err() {
                        self.binding.complete_path_detach(
                            self.key,
                            self.path_instance_id,
                            self.output_incarnation,
                        );
                        return;
                    }
                }
                _ = self.events.closed() => {
                    self.binding.complete_path_detach(
                        self.key,
                        self.path_instance_id,
                        self.output_incarnation,
                    );
                    return;
                }
            }
        }
    }

    fn finish_without_runtime(self) {
        // Synchronous detach callers block until their lifecycle event owns a
        // slot in this bounded queue. If its receiver has already disappeared,
        // no actor remains to complete the binding transition.
        if self.events.is_closed() {
            self.binding.complete_path_detach(
                self.key,
                self.path_instance_id,
                self.output_incarnation,
            );
        }
    }
}

impl PendingOrderedPathDetach {
    async fn send(self) {
        let Self {
            events,
            binding,
            key,
            path_instance_id,
            output_incarnation,
            event,
        } = self;
        if events.send(event).await.is_err() {
            binding.complete_path_detach(key, path_instance_id, output_incarnation);
            return;
        }
        ExistingOrderedPathDetach {
            events,
            binding,
            key,
            path_instance_id,
            output_incarnation,
        }
        .wait()
        .await;
    }

    fn blocking_send(self) {
        if self.events.blocking_send(self.event).is_err() {
            self.binding.complete_path_detach(
                self.key,
                self.path_instance_id,
                self.output_incarnation,
            );
        }
    }
}

fn try_queue_ordered_path_detach(
    events: mpsc::Sender<ServerReliableStreamEvent>,
    binding: Arc<ResponseStreamBinding>,
    key: CarrierPathKey,
    path_instance_id: CarrierPathInstanceId,
    output_incarnation: u64,
) -> Option<PendingOrderedPathDetach> {
    let event = ServerReliableStreamEvent::PathDetached {
        key,
        path_instance_id,
        output_incarnation,
    };
    match events.try_send(event) {
        Ok(()) => None,
        Err(mpsc::error::TrySendError::Full(event)) => Some(PendingOrderedPathDetach {
            events,
            binding,
            key,
            path_instance_id,
            output_incarnation,
            event,
        }),
        Err(mpsc::error::TrySendError::Closed(_)) => {
            binding.complete_path_detach(key, path_instance_id, output_incarnation);
            None
        }
    }
}

fn queue_ordered_path_detach(
    events: mpsc::Sender<ServerReliableStreamEvent>,
    binding: Arc<ResponseStreamBinding>,
    key: CarrierPathKey,
    path_instance_id: CarrierPathInstanceId,
    output_incarnation: u64,
) {
    let Some(pending) =
        try_queue_ordered_path_detach(events, binding, key, path_instance_id, output_incarnation)
    else {
        return;
    };
    if let Ok(runtime) = tokio::runtime::Handle::try_current() {
        runtime.spawn(pending.send());
    } else {
        // With no async caller available, blocking preserves FIFO. A closed
        // receiver has no actor left to observe the transition.
        pending.blocking_send();
    }
}

pub(in crate::runtime) enum ServerReliableStreamOpen {
    New(Box<AcceptedServerReliableStream>, TrafficClass),
    Existing(TrafficClass),
    DuplicateLiveIgnored,
    Rejected,
}

/// One registry-admitted product stream awaiting or running its target relay.
///
/// Its retirement lease keeps registry membership alive until carrier output
/// has closed. The relay supervisor retains that lease across task aborts.
pub(in crate::runtime) struct AcceptedServerReliableStream {
    session_id: SessionId,
    principal_permit: PrincipalPermit,
    target: TargetAddr,
    ingress: Option<crate::runtime::path::ServerMppIngress>,
    stream: Option<ReliablePathStream>,
    opening: AcceptedServerReliableStreamOpening,
    session_send_buffer: SessionSendBuffer,
    retirement: Arc<AcceptedServerReliableStreamRetirementInner>,
    supervised: bool,
}

struct AcceptedServerReliableStreamIdentity {
    session_id: SessionId,
    principal_permit: PrincipalPermit,
    target: TargetAddr,
    ingress: Option<ServerMppIngress>,
}

struct AcceptedServerReliableStreamOpening {
    path_validation: ServerPathValidation,
    commands: ReliablePathCommandSender,
    mux_limits: MuxLimits,
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

    pub(in crate::runtime) fn session_retirement(
        &self,
    ) -> Result<crate::runtime::path::ServerSessionRetirement, RuntimeError> {
        self.retirement.registry.session_retirement(self.session_id)
    }

    pub(in crate::runtime) fn target(&self) -> &TargetAddr {
        &self.target
    }

    pub(in crate::runtime) fn principal_permit(&self) -> &PrincipalPermit {
        &self.principal_permit
    }

    pub(in crate::runtime) fn ingress(&self) -> Option<&crate::runtime::path::ServerMppIngress> {
        self.ingress.as_ref()
    }

    pub(in crate::runtime) fn stream(&self) -> &ReliablePathStream {
        self.stream.as_ref().expect("accepted reliable stream")
    }

    pub(in crate::runtime) fn take_stream(&mut self) -> ReliablePathStream {
        self.stream.take().expect("accepted reliable stream")
    }

    pub(in crate::runtime) fn session_send_buffer(&self) -> SessionSendBuffer {
        self.session_send_buffer.clone()
    }

    /// Publishes carrier admission before target establishment is submitted.
    /// Zero credit keeps an initial logical OPEN pending without charging
    /// remote routing, DNS, or target connect time to this carrier.
    pub(in crate::runtime) fn admit_opening_path(&self) -> Result<(), RuntimeError> {
        let stream = self.stream();
        self.opening.commands.try_enqueue_admitted_frame(
            Frame::StreamMaxData {
                stream_id: stream.stream_id,
                max_offset: 0,
            },
            TrafficClass::Control,
        )
    }

    /// Queues the optional opening-carrier proof from the independent target
    /// task. Once zero-credit admission committed, proof backpressure or
    /// carrier loss cannot revoke the logical owner or create a second target.
    pub(in crate::runtime) async fn publish_opening_path_validation(
        &self,
    ) -> Result<(), RuntimeError> {
        let Some(challenge) = self
            .opening
            .path_validation
            .challenge(self.opening.mux_limits)
        else {
            return Ok(());
        };
        self.opening
            .commands
            .enqueue_admitted_frame(challenge, TrafficClass::Control)
            .await
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
        Self::with_accept_sender(
            max_streams,
            crate::performance::ResourceLimits::default().max_paths,
            accepted,
            MuxLimits::default(),
        )
    }

    /// Creates the registry and its uniquely paired relay receiver together.
    #[cfg(test)]
    pub(in crate::runtime) fn new_accepting(
        max_streams: usize,
    ) -> (
        Arc<Self>,
        mpsc::UnboundedReceiver<AcceptedServerReliableStream>,
    ) {
        let (accepted, receiver) = mpsc::unbounded_channel();
        (
            Arc::new(Self::with_accept_sender(
                max_streams,
                crate::performance::ResourceLimits::default().max_paths,
                accepted,
                MuxLimits::default(),
            )),
            receiver,
        )
    }

    /// Production constructor whose session buffer follows configured limits.
    #[cfg(test)]
    pub(in crate::runtime) fn new_accepting_with_limits(
        limits: MuxLimits,
        max_paths_per_session: usize,
    ) -> (
        Arc<Self>,
        mpsc::UnboundedReceiver<AcceptedServerReliableStream>,
    ) {
        let (accepted, receiver) = mpsc::unbounded_channel();
        (
            Arc::new(Self::with_accept_sender(
                limits.max_streams,
                max_paths_per_session,
                accepted,
                limits,
            )),
            receiver,
        )
    }

    pub(in crate::runtime) fn new_accepting_with_limits_and_retention(
        limits: MuxLimits,
        max_paths_per_session: usize,
        session_retention_timeout: std::time::Duration,
    ) -> (
        Arc<Self>,
        mpsc::UnboundedReceiver<AcceptedServerReliableStream>,
    ) {
        let (accepted, receiver) = mpsc::unbounded_channel();
        (
            Arc::new(Self::with_accept_sender_and_retention(
                limits.max_streams,
                max_paths_per_session,
                accepted,
                limits,
                session_retention_timeout,
            )),
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
            .register_carrier_path(
                session_id,
                underlay,
                path_id,
                crate::runtime::path::ServerLocalPathProperties::default(),
                PrincipalPermit::for_test("test-peer"),
            )
            .expect("register test carrier")
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
            .register_carrier_path(
                session_id,
                underlay,
                path_id,
                local,
                PrincipalPermit::for_test("test-peer"),
            )
            .expect("register test carrier")
    }

    #[cfg(test)]
    fn with_accept_sender(
        max_streams: usize,
        max_paths_per_session: usize,
        accepted: mpsc::UnboundedSender<AcceptedServerReliableStream>,
        limits: MuxLimits,
    ) -> Self {
        Self::with_accept_sender_and_retention(
            max_streams,
            max_paths_per_session,
            accepted,
            limits,
            crate::config::DEFAULT_SESSION_RETENTION_TIMEOUT,
        )
    }

    fn with_accept_sender_and_retention(
        max_streams: usize,
        max_paths_per_session: usize,
        accepted: mpsc::UnboundedSender<AcceptedServerReliableStream>,
        limits: MuxLimits,
        session_retention_timeout: std::time::Duration,
    ) -> Self {
        Self {
            max_streams,
            max_paths_per_session,
            // A server must be able to admit one fully populated multipath
            // session even when its reliable-stream budget is intentionally
            // small. Beyond that, the existing global stream envelope is the
            // finite process-wide carrier ceiling; multiplying both limits
            // would turn the default 65,536 x 64 into an impractical metadata
            // allowance.
            max_carrier_paths: max_streams.max(max_paths_per_session),
            accepted,
            streams: Mutex::new(HashMap::new()),
            path_metrics: Mutex::new(HashMap::new()),
            registered_path_instances: Mutex::new(ServerCarrierPathRegistry::default()),
            closed_streams: Mutex::new(RecentIdCache::new(reliable_closed_stream_cache_capacity(
                max_streams,
            ))),
            session_tracker: Arc::new(ServerSessionTracker::from_limits_and_retention(
                limits,
                max_streams,
                session_retention_timeout,
            )),
            #[cfg(test)]
            carrier_activation_after_session_attach_hook: Mutex::new(None),
        }
    }

    #[cfg(test)]
    fn set_carrier_activation_after_session_attach_hook(
        &self,
        hook: Option<Arc<dyn Fn() + Send + Sync + 'static>>,
    ) {
        *self
            .carrier_activation_after_session_attach_hook
            .lock()
            .expect("server carrier activation hook lock") = hook;
    }

    /// Hands an admitted stream to this registry's paired target-relay service.
    // Channel rejection returns the admitted stream without allocating or losing ownership.
    #[allow(clippy::result_large_err)]
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
        let registered = {
            let registered = self
                .registered_path_instances
                .lock()
                .expect("server active path instance lock");
            registered
                .instances
                .iter()
                .map(
                    |(&(session_id, underlay, path_id, path_instance_id), path)| {
                        ServerCarrierPathStatusBasis::capture(
                            ServerCarrierPathIdentity {
                                session_id,
                                underlay,
                                path_id,
                                path_instance_id,
                            },
                            path,
                        )
                    },
                )
                .collect::<Vec<_>>()
        };
        let mut paths = self.project_carrier_path_statuses(registered, now);
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

    /// Project only the exact carrier instances requested by a packet owner.
    /// Unlike `management_snapshot`, this is O(requested paths), does not scan
    /// unrelated sessions, and still captures structural fields with their
    /// apply epoch in one registry transaction.
    pub(in crate::runtime) fn carrier_path_statuses(
        &self,
        identities: &[ServerCarrierPathIdentity],
    ) -> Vec<Option<ServerCarrierPathStatusSnapshot>> {
        let now = Instant::now();
        let registered = {
            let registered = self
                .registered_path_instances
                .lock()
                .expect("server active path instance lock");
            identities
                .iter()
                .map(|identity| {
                    registered
                        .instances
                        .get(&server_physical_path_key(*identity))
                        .map(|path| ServerCarrierPathStatusBasis::capture(*identity, path))
                })
                .collect::<Vec<_>>()
        };
        let path_metrics = self.path_metrics.lock().expect("server path metrics lock");
        registered
            .into_iter()
            .map(|basis| {
                basis.map(|basis| {
                    project_carrier_path_status(
                        basis,
                        path_metrics
                            .get(&server_physical_path_key(basis.identity))
                            .copied(),
                        now,
                    )
                })
            })
            .collect()
    }

    fn project_carrier_path_statuses(
        &self,
        registered: Vec<ServerCarrierPathStatusBasis>,
        now: Instant,
    ) -> Vec<ServerCarrierPathStatusSnapshot> {
        let path_metrics = self.path_metrics.lock().expect("server path metrics lock");
        registered
            .into_iter()
            .map(|basis| {
                project_carrier_path_status(
                    basis,
                    path_metrics
                        .get(&server_physical_path_key(basis.identity))
                        .copied(),
                    now,
                )
            })
            .collect()
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
            registered.instances.iter()
        {
            if *entry_session != session_id {
                continue;
            }
            let logical_key = (*entry_session, *underlay, *path_id);
            if registered.logical_instances.get(&logical_key) != Some(path_instance_id) {
                // Peer status describes the current logical carrier, never a
                // non-current exact physical lifecycle record.
                continue;
            }
            let instance_key = (*entry_session, *underlay, *path_id, *path_instance_id);
            let current = metrics
                .get(&instance_key)
                .filter(|entry| entry.source == ServerPathMetricsSource::LocalSender)
                .map(|entry| {
                    path_metrics_after_residence(
                        entry.metrics,
                        now.saturating_duration_since(entry.recorded_at),
                    )
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
    ) -> Result<ServerSessionRegistration, RuntimeError> {
        ServerSessionRegistration::try_new(self.session_tracker.clone(), session_id)
    }

    pub(in crate::runtime) fn session_retirement(
        &self,
        session_id: SessionId,
    ) -> Result<crate::runtime::path::ServerSessionRetirement, RuntimeError> {
        self.session_tracker.session_retirement(session_id)
    }

    pub(in crate::runtime) fn retire_session(
        self: &Arc<Self>,
        session_id: SessionId,
        reason: crate::protocol::CloseReason,
    ) -> crate::protocol::CloseReason {
        let _first_publication = self.session_tracker.retire_session(session_id, reason);
        let reason = self
            .session_tracker
            .session_retirement(session_id)
            .ok()
            .and_then(|retirement| retirement.reason())
            .unwrap_or(reason);
        // Publication is first-writer-wins, but the owner sweep is deliberately
        // repeatable. An activation that linearized before publication is
        // either already visible here or still holds the owner lock below.
        let identities = self
            .registered_path_instances
            .lock()
            .expect("server active path instance lock")
            .instances
            .keys()
            .filter_map(
                |(registered_session_id, underlay, path_id, path_instance_id)| {
                    (*registered_session_id == session_id).then_some(ServerCarrierPathIdentity {
                        session_id,
                        underlay: *underlay,
                        path_id: *path_id,
                        path_instance_id: *path_instance_id,
                    })
                },
            )
            .collect::<Vec<_>>();
        for identity in identities {
            let _ = self.retire_carrier_path(identity);
        }
        reason
    }

    fn accepted_stream(
        self: &Arc<Self>,
        identity: AcceptedServerReliableStreamIdentity,
        stream: ReliablePathStream,
        opening: AcceptedServerReliableStreamOpening,
        session_send_buffer: SessionSendBuffer,
    ) -> AcceptedServerReliableStream {
        let session_id = identity.session_id;
        let stream_id = stream.stream_id;
        let close_output = stream.output.clone();
        AcceptedServerReliableStream {
            session_id,
            principal_permit: identity.principal_permit,
            target: identity.target,
            ingress: identity.ingress,
            stream: Some(stream),
            opening,
            session_send_buffer,
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
        configured_slot: ConfiguredMemberSlot,
        local: crate::runtime::path::ServerLocalPathProperties,
        initial_peer_usage: Option<PathUsage>,
        native_capacity_epoch: u64,
        apply_authority: ServerCarrierPathApplyAuthority,
        principal_permit: PrincipalPermit,
        retirement_completion: watch::Sender<bool>,
    ) -> Result<crate::runtime::path::ServerSessionRetirement, RuntimeError> {
        let ServerCarrierPathIdentity {
            session_id,
            underlay: _,
            path_id: _,
            path_instance_id,
        } = identity;
        let logical_key = server_logical_path_key(identity);
        let mut paths = self
            .registered_path_instances
            .lock()
            .expect("server active path instance lock");
        if paths.logical_instances.contains_key(&logical_key) {
            return Err(RuntimeError::Protocol(
                "duplicate server logical carrier path",
            ));
        }
        if paths.logical_instances.len() >= self.max_carrier_paths {
            return Err(RuntimeError::Protocol(
                "server global carrier path limit reached",
            ));
        }
        let session_path_count = paths
            .session_path_counts
            .get(&session_id)
            .copied()
            .unwrap_or(0);
        if session_path_count >= self.max_paths_per_session {
            return Err(RuntimeError::Protocol(
                "server session carrier path limit reached",
            ));
        }
        // The path-owner lock spans tracker attachment and physical
        // publication. Retirement publishes under the tracker lock and then
        // takes this lock to scan, so exactly one transition linearizes first:
        // a retired attach is rejected, or a successful attach is visible to
        // that scan. Neither lock is held by tracker callbacks.
        let (_, session_retirement) = self
            .session_tracker
            .attach_authenticated_session(session_id, &principal_permit)?;

        #[cfg(test)]
        if let Some(hook) = self
            .carrier_activation_after_session_attach_hook
            .lock()
            .expect("server carrier activation hook lock")
            .clone()
        {
            hook();
        }

        // Publication can race the short interval after tracker attachment.
        // Abort before publishing either logical or physical ownership; if it
        // races after this read, retirement is waiting on `paths` and its scan
        // necessarily observes the instance inserted below.
        if let Some(reason) = session_retirement.reason() {
            drop(paths);
            self.session_tracker.detach_session(session_id);
            return Err(RuntimeError::RemoteClosed(reason));
        }

        paths
            .logical_instances
            .insert(logical_key, path_instance_id);
        paths
            .session_path_counts
            .insert(session_id, session_path_count + 1);
        if paths
            .instances
            .insert(
                server_physical_path_key(identity),
                ServerRegisteredPath {
                    configured_slot,
                    local,
                    state: PeerPathState::Active,
                    peer_usage: initial_peer_usage
                        .map(|usage| ServerPathUsageEntry { sequence: 0, usage }),
                    native_capacity_epoch,
                    apply_authority,
                    path_proof: None,
                    retirement_started: false,
                    retirement_completion,
                },
            )
            .is_some()
        {
            paths.logical_instances.remove(&logical_key);
            decrement_session_path_count(&mut paths.session_path_counts, session_id);
            drop(paths);
            self.session_tracker.detach_session(session_id);
            return Err(RuntimeError::Protocol(
                "duplicate server physical carrier path",
            ));
        }
        drop(paths);
        Ok(session_retirement)
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
            .instances
            .get_mut(&key)
        {
            path.set_state(state);
        }
    }

    fn carrier_path_attachment_slot(
        &self,
        identity: ServerCarrierPathIdentity,
    ) -> Option<ConfiguredMemberSlot> {
        let paths = self
            .registered_path_instances
            .lock()
            .expect("server active path instance lock");
        (paths
            .logical_instances
            .get(&server_logical_path_key(identity))
            == Some(&identity.path_instance_id))
        .then(|| {
            paths
                .instances
                .get(&server_physical_path_key(identity))
                .filter(|path| !path.retirement_started)
                .map(|path| path.configured_slot)
        })
        .flatten()
    }

    fn retire_carrier_path(
        self: &Arc<Self>,
        identity: ServerCarrierPathIdentity,
    ) -> ServerCarrierPathRetirement {
        let ServerCarrierPathIdentity {
            session_id,
            underlay,
            path_id,
            path_instance_id,
        } = identity;
        let (retirement, retirement_started) = {
            let physical_key = server_physical_path_key(identity);
            let mut paths = self
                .registered_path_instances
                .lock()
                .expect("server active path instance lock");
            let Some(path) = paths.instances.get_mut(&physical_key) else {
                return ServerCarrierPathRetirement::complete();
            };
            let retirement =
                ServerCarrierPathRetirement::pending(path.retirement_completion.subscribe());
            let retirement_started = path.begin_retirement();
            (retirement, retirement_started)
        };
        if !retirement_started {
            return retirement;
        }
        let key = CarrierPathKey { underlay, path_id };
        let stream_inputs = {
            let streams = self
                .streams
                .lock()
                .expect("server reliable stream registry lock");
            streams
                .iter()
                .filter_map(|((entry_session_id, _), entry)| {
                    (*entry_session_id == session_id)
                        .then_some((entry.events.clone(), entry.binding.clone()))
                })
                .collect::<Vec<_>>()
        };
        let mut pending = Vec::new();
        let mut existing = Vec::new();
        for (events, binding) in stream_inputs {
            let Some(outcome) = binding.begin_path_detach(key, path_instance_id) else {
                continue;
            };
            match outcome {
                ResponsePathDetachOutcome::Begun(output_incarnation) => {
                    if let Some(detach) = try_queue_ordered_path_detach(
                        events.clone(),
                        binding.clone(),
                        key,
                        path_instance_id,
                        output_incarnation,
                    ) {
                        pending.push(detach);
                    } else {
                        // A successful queue send is not the lifecycle
                        // boundary. The stream actor must apply the detach
                        // before aggregate carrier retirement can complete.
                        existing.push(ExistingOrderedPathDetach {
                            events,
                            binding,
                            key,
                            path_instance_id,
                            output_incarnation,
                        });
                    }
                }
                ResponsePathDetachOutcome::Pending(output_incarnation) => {
                    existing.push(ExistingOrderedPathDetach {
                        events,
                        binding,
                        key,
                        path_instance_id,
                        output_incarnation,
                    });
                }
            }
        }
        let retired_path = {
            let logical_key = server_logical_path_key(identity);
            let physical_key = server_physical_path_key(identity);
            let mut paths = self
                .registered_path_instances
                .lock()
                .expect("server active path instance lock");
            if paths.logical_instances.get(&logical_key) == Some(&path_instance_id) {
                // `begin_path_detach` already removed every extant attachment
                // from scheduling. Release logical admission at that same
                // lifecycle boundary; exact physical cleanup may finish later
                // without withholding a same-key successor.
                paths.logical_instances.remove(&logical_key);
                decrement_session_path_count(&mut paths.session_path_counts, session_id);
            }
            paths.instances.remove(&physical_key)
        };
        let Some(retired_path) = retired_path else {
            return retirement;
        };
        self.path_metrics
            .lock()
            .expect("server path metrics lock")
            .remove(&(session_id, underlay, path_id, path_instance_id));
        // The carrier's session reference ends with its physical ownership.
        // Each surviving Product binding independently retains the session
        // while its exact ordered detach remains actor-owned below.
        self.session_tracker.detach_session(session_id);
        if pending.is_empty() && existing.is_empty() {
            retired_path.retirement_completion.send_replace(true);
            return retirement;
        }
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                // Each stream owns an independent bounded actor queue. Publish
                // every detach concurrently so a full dormant stream cannot
                // withhold the lifecycle boundary from runnable siblings;
                // aggregate retirement still joins every actor-applied detach.
                let completions = pending
                    .into_iter()
                    .map(OrderedPathDetachCompletion::Pending)
                    .chain(
                        existing
                            .into_iter()
                            .map(OrderedPathDetachCompletion::Existing),
                    );
                futures::future::join_all(completions.map(OrderedPathDetachCompletion::wait)).await;
                retired_path.retirement_completion.send_replace(true);
            });
        } else {
            for detach in pending {
                detach.blocking_send();
            }
            for detach in existing {
                detach.finish_without_runtime();
            }
            retired_path.retirement_completion.send_replace(true);
        }
        retirement
    }

    #[cfg(test)]
    pub(in crate::runtime) fn open_or_attach(
        self: &Arc<Self>,
        request: ServerStreamOpenRequest,
    ) -> Result<ServerReliableStreamOpen, RuntimeError> {
        let opening_ingress = request.attachment.path_registration.mpp_ingress();
        self.open_or_attach_with_ingress(request, opening_ingress)
    }

    fn open_or_attach_with_ingress(
        self: &Arc<Self>,
        request: ServerStreamOpenRequest,
        opening_ingress: Option<ServerMppIngress>,
    ) -> Result<ServerReliableStreamOpen, RuntimeError> {
        let ServerStreamOpenRequest {
            session_id,
            stream_id,
            target,
            initial_demand,
            return_plan,
            attachment,
            mux_limits,
        } = request;
        let initial_lane = traffic_class_from_stream_demand_hint(initial_demand);
        let crate::runtime::path::ServerStreamPathAttachment {
            path_registration,
            commands,
            max_frame_payload_bytes,
        } = attachment;
        let initial_metrics = path_registration.initial_metrics();
        let startup_rate_prior = path_registration.startup_rate_prior();
        let local_policy = path_registration.local_policy();
        let underlay = path_registration.underlay();
        let path_id = path_registration.path_id();
        let path_instance_id = path_registration.path_instance_id();
        let mut streams = self
            .streams
            .lock()
            .expect("server reliable stream registry lock");
        let Some(configured_slot) = self.carrier_path_attachment_slot(ServerCarrierPathIdentity {
            session_id,
            underlay,
            path_id,
            path_instance_id,
        }) else {
            // Retirement marks the exact physical owner before taking the
            // stream-membership lock. An opener that linearized first is
            // included in its detach scan; a later opener cannot resurrect the
            // predecessor after scheduler withdrawal.
            return Ok(ServerReliableStreamOpen::Rejected);
        };
        // Resolve stored evidence after taking the stream membership lock. An
        // update published while this opener waited must either be inherited
        // here or observe the newly published binding afterward.
        let initial_metrics = self.initial_path_metrics(
            session_id,
            underlay,
            path_id,
            path_instance_id,
            initial_metrics,
        );
        let initial_usage = self.stored_path_usage(session_id, underlay, path_id, path_instance_id);
        let initial_path_proof =
            self.initial_path_proof(session_id, underlay, path_id, path_instance_id);
        let initial_native_scheduling_shape =
            self.initial_native_scheduling_shape(session_id, underlay, path_id, path_instance_id);
        if let Some(entry) = streams.get_mut(&(session_id, stream_id)) {
            if entry.target != target || entry.initial_demand != initial_demand {
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "server_stream_open",
                    format_args!(
                        "session_id={} stream_id={} path_underlay={:?} path_id={} initial_demand={:?} result=rejected_open_identity_mismatch",
                        session_id.0, stream_id.0, underlay, path_id.0, initial_demand,
                    ),
                );
                return Ok(ServerReliableStreamOpen::Rejected);
            }
            let attach_outcome = match entry
                .binding
                .attach_output_with_return_plan_if_session_active(
                    ResponseOutputAttachment {
                        key: CarrierPathKey { underlay, path_id },
                        path_instance_id,
                        configured_slot,
                        local_policy,
                        startup_rate_prior,
                        commands,
                        state: ResponseOutputAttachmentState {
                            metrics: initial_metrics,
                            native_scheduling_shape: initial_native_scheduling_shape,
                            peer_usage: initial_usage.map(|usage| (usage.sequence, usage.usage)),
                            path_proof: initial_path_proof,
                        },
                    },
                    return_plan,
                ) {
                Ok(outcome) => outcome,
                Err(RuntimeError::RemoteClosed(_) | RuntimeError::ReliablePathSessionClosed) => {
                    return Ok(ServerReliableStreamOpen::Rejected);
                }
                Err(error) => return Err(error),
            };
            let response_lane = entry.binding.lane();
            if matches!(
                attach_outcome,
                ResponseStreamAttachOutcome::RejectedClosedStream
            ) {
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "server_stream_open",
                    format_args!(
                        "session_id={} stream_id={} path_underlay={:?} path_id={} response_lane={:?} result=rejected_closing_stream",
                        session_id.0, stream_id.0, underlay, path_id.0, response_lane,
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
                        "session_id={} stream_id={} path_underlay={:?} path_id={} response_lane={:?} result={}",
                        session_id.0, stream_id.0, underlay, path_id.0, response_lane, result,
                    ),
                );
                return Ok(ServerReliableStreamOpen::DuplicateLiveIgnored);
            }
            entry.binding.record_request_feedback_ingress(
                CarrierPathKey { underlay, path_id },
                path_instance_id,
            );
            // Attachment acceptance remains zero-credit and carrier-local.
            // If the target is already established, immediately replay the
            // retained logical grant on this newly attached output as a later
            // cumulative update. The carrier actor writes its direct zero ACK
            // before polling this ordered command queue.
            entry.binding.retry_pending_max_data(stream_id);
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "server_stream_open",
                format_args!(
                    "session_id={} stream_id={} path_underlay={:?} path_id={} response_lane={:?} result=existing",
                    session_id.0, stream_id.0, underlay, path_id.0, response_lane,
                ),
            );
            return Ok(ServerReliableStreamOpen::Existing(response_lane));
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
                    "session_id={} stream_id={} path_underlay={:?} path_id={} initial_demand={:?} result=rejected_closed_stream",
                    session_id.0, stream_id.0, underlay, path_id.0, initial_demand,
                ),
            );
            return Ok(ServerReliableStreamOpen::Rejected);
        }

        if streams.len() >= self.max_streams {
            return Err(RuntimeError::Protocol(
                "server reliable stream limit reached",
            ));
        }

        let (events_tx, events_rx) = mpsc::channel(reliable_stream_frame_queue_for_payload(
            mux_limits,
            max_frame_payload_bytes,
        ));
        let opening_path_validation = path_registration.path_validation();
        let opening_commands = commands.clone();
        let binding = ResponseStreamBinding::new_with_limits_tracker_path_instance_and_return_plan(
            session_id,
            underlay,
            path_id,
            commands,
            initial_lane,
            mux_limits,
            self.session_tracker.clone(),
            path_instance_id,
            configured_slot,
            local_policy,
            startup_rate_prior,
            return_plan,
        )?;
        let session_send_buffer = binding.session_send_buffer();
        if let Some(metrics) = initial_metrics {
            binding.install_stored_path_metrics_for_instance(
                CarrierPathKey { underlay, path_id },
                path_instance_id,
                metrics,
            );
        }
        if let Some(shape) = initial_native_scheduling_shape {
            binding.install_native_scheduling_shape_for_instance(
                CarrierPathKey { underlay, path_id },
                path_instance_id,
                shape,
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
        if let Some(observation) = initial_path_proof {
            binding.mark_path_proof_success_for_instance(
                CarrierPathKey { underlay, path_id },
                path_instance_id,
                observation,
            );
        }
        streams.insert(
            (session_id, stream_id),
            ServerReliableStreamEntry {
                target: target.clone(),
                initial_demand,
                events: events_tx,
                binding: binding.clone(),
            },
        );
        #[cfg(feature = "lab-diagnostics")]
        lab_diagnostic(
            "server_stream_open",
            format_args!(
                "session_id={} stream_id={} path_underlay={:?} path_id={} initial_demand={:?} response_lane={:?} result=new",
                session_id.0, stream_id.0, underlay, path_id.0, initial_demand, initial_lane,
            ),
        );
        let stream = ReliablePathStream {
            stream_id,
            // OPEN_STREAM carries no response-direction credit. The client
            // publishes its window with STREAM_MAX_DATA in the same open
            // flight; until that event is consumed the server send side must
            // remain blocked at offset zero.
            max_offset: 0,
            lane: initial_lane,
            underlay,
            max_frame_payload_bytes,
            output: ReliablePathStreamOutput::Switchable(binding),
            frames: ReliablePathStreamInput::server(events_rx),
        };
        Ok(ServerReliableStreamOpen::New(
            Box::new(self.accepted_stream(
                AcceptedServerReliableStreamIdentity {
                    session_id,
                    principal_permit: path_registration.principal_permit().clone(),
                    target,
                    ingress: opening_ingress,
                },
                stream,
                AcceptedServerReliableStreamOpening {
                    path_validation: opening_path_validation,
                    commands: opening_commands,
                    mux_limits,
                },
                session_send_buffer,
            )),
            initial_lane,
        ))
    }

    pub(in crate::runtime) fn record_path_metrics(
        &self,
        identity: ServerCarrierPathIdentity,
        metrics: PathMetrics,
    ) {
        self.record_path_metrics_with_source(
            identity,
            metrics,
            ServerPathMetricsSource::PeerHint,
            false,
            None,
            None,
            None,
        );
    }

    #[cfg(test)]
    pub(in crate::runtime) fn record_local_path_metrics(
        &self,
        identity: ServerCarrierPathIdentity,
        metrics: PathMetrics,
        native_drain_observed: bool,
    ) {
        let native_window_sample =
            CarrierNativeWindowSample::from_path_metrics_at(metrics, Instant::now());
        self.record_local_path_metrics_with_delivery_rate_sample(
            identity,
            metrics,
            native_drain_observed,
            None,
            native_window_sample,
            None,
        );
    }

    fn record_local_path_metrics_with_delivery_rate_sample(
        &self,
        identity: ServerCarrierPathIdentity,
        metrics: PathMetrics,
        native_drain_observed: bool,
        native_capacity_epoch: Option<u64>,
        native_window_sample: Option<CarrierNativeWindowSample>,
        delivery_rate_sample: Option<CarrierDeliveryRateSample>,
    ) {
        self.record_path_metrics_with_source(
            identity,
            metrics,
            ServerPathMetricsSource::LocalSender,
            native_drain_observed,
            native_capacity_epoch,
            native_window_sample,
            delivery_rate_sample,
        );
    }

    pub(in crate::runtime) fn record_path_proof_success(
        &self,
        identity: ServerCarrierPathIdentity,
        observation: PathProofObservation,
    ) {
        let ServerCarrierPathIdentity {
            session_id,
            underlay,
            path_id,
            path_instance_id,
        } = identity;
        let instance_key = (session_id, underlay, path_id, path_instance_id);
        let changed = {
            let mut registered = self
                .registered_path_instances
                .lock()
                .expect("server active path instance lock");
            let Some(path) = registered.instances.get_mut(&instance_key) else {
                return;
            };
            if path.path_proof.is_some() {
                false
            } else {
                path.path_proof = Some(observation);
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
            binding.mark_path_proof_success_for_instance(key, path_instance_id, observation);
        }
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
        let changed = self
            .registered_path_instances
            .lock()
            .expect("server active path instance lock")
            .instances
            .get_mut(&instance_key)
            .is_some_and(|path| path.update_peer_usage(sequence, usage));
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
        self.registered_path_instances
            .lock()
            .expect("server active path instance lock")
            .instances
            .get(&(session_id, underlay, path_id, path_instance_id))
            .and_then(|path| path.peer_usage)
    }

    fn initial_path_proof(
        &self,
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        path_instance_id: CarrierPathInstanceId,
    ) -> Option<PathProofObservation> {
        self.registered_path_instances
            .lock()
            .expect("server active path instance lock")
            .instances
            .get(&(session_id, underlay, path_id, path_instance_id))
            .and_then(|path| path.path_proof)
    }

    fn initial_native_scheduling_shape(
        &self,
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        path_instance_id: CarrierPathInstanceId,
    ) -> Option<NativeCarrierSchedulingShapeSnapshot> {
        self.registered_path_instances
            .lock()
            .expect("server active path instance lock")
            .instances
            .get(&(session_id, underlay, path_id, path_instance_id))
            .and_then(|path| path.apply_authority.snapshot().native_scheduling_shape)
    }

    fn stage_native_scheduling_shape(
        &self,
        identity: ServerCarrierPathIdentity,
        shape: NativeCarrierSchedulingShapeSnapshot,
    ) -> bool {
        let ServerCarrierPathIdentity {
            session_id,
            underlay,
            path_id,
            path_instance_id,
        } = identity;
        let scope = shape.stamp().scope();
        if underlay != UnderlayProtocol::Udp
            || scope.carrier_instance_id() != path_instance_id
            || scope.direction() != crate::protocol::PathMetricDirection::ServerToClient
        {
            return false;
        }
        let instance_key = (session_id, underlay, path_id, path_instance_id);
        let changed = {
            let mut registered = self
                .registered_path_instances
                .lock()
                .expect("server active path instance lock");
            let Some(path) = registered.instances.get_mut(&instance_key) else {
                return false;
            };
            path.apply_authority.stage_native_scheduling_shape(shape)
        };
        changed
    }

    fn fanout_native_scheduling_shape(
        &self,
        identity: ServerCarrierPathIdentity,
        shape: NativeCarrierSchedulingShapeSnapshot,
    ) {
        let ServerCarrierPathIdentity {
            session_id,
            underlay,
            path_id,
            path_instance_id,
        } = identity;
        let instance_key = (session_id, underlay, path_id, path_instance_id);
        let remains_current = self
            .registered_path_instances
            .lock()
            .expect("server active path instance lock")
            .instances
            .get(&instance_key)
            .is_some_and(|path| {
                path.apply_authority.snapshot().native_scheduling_shape == Some(shape)
            });
        if !remains_current {
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
            binding.install_native_scheduling_shape_for_instance(key, path_instance_id, shape);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn record_path_metrics_with_source(
        &self,
        identity: ServerCarrierPathIdentity,
        metrics: PathMetrics,
        source: ServerPathMetricsSource,
        native_drain_observed: bool,
        native_capacity_epoch: Option<u64>,
        native_window_sample: Option<CarrierNativeWindowSample>,
        delivery_rate_sample: Option<CarrierDeliveryRateSample>,
    ) {
        let ServerCarrierPathIdentity {
            session_id,
            underlay,
            path_id,
            path_instance_id,
        } = identity;
        let instance_key = (session_id, underlay, path_id, path_instance_id);
        let mut registered_path_instances = self
            .registered_path_instances
            .lock()
            .expect("server active path instance lock");
        let Some(registered_path) = registered_path_instances.instances.get_mut(&instance_key)
        else {
            return;
        };
        let metrics = PathMetrics { path_id, ..metrics };
        if source == ServerPathMetricsSource::LocalSender
            && underlay == UnderlayProtocol::Udp
            && let Some(native_capacity_epoch) = native_capacity_epoch
            && !registered_path.observe_native_capacity_epoch(native_capacity_epoch)
        {
            return;
        }
        let mut path_metrics = self.path_metrics.lock().expect("server path metrics lock");
        let previous = path_metrics.get(&instance_key).copied();
        let entry = ServerPathMetricsEntry {
            metrics,
            source,
            native_drain_observed,
            carrier_native_window_sample: native_window_sample,
            carrier_delivery_rate_sample: delivery_rate_sample,
            recorded_at: Instant::now(),
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
            binding.install_stored_path_metrics_for_instance(key, path_instance_id, entry);
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
        let initial = initial_metrics.map(|metrics| {
            let recorded_at = Instant::now();
            let metrics = PathMetrics { path_id, ..metrics };
            ServerPathMetricsEntry {
                metrics,
                source: ServerPathMetricsSource::LocalSender,
                native_drain_observed: false,
                carrier_native_window_sample: CarrierNativeWindowSample::from_path_metrics_at(
                    metrics,
                    recorded_at,
                ),
                carrier_delivery_rate_sample: None,
                recorded_at,
            }
        });
        match stored {
            Some(metrics) if metrics.source == ServerPathMetricsSource::LocalSender => {
                Some(metrics)
            }
            _ => initial.or(stored),
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
    }

    pub(in crate::runtime) fn detach_path(
        &self,
        identity: ServerCarrierPathIdentity,
        stream_id: StreamId,
    ) -> Result<(), RuntimeError> {
        let input = {
            let streams = self
                .streams
                .lock()
                .expect("server reliable stream registry lock");
            streams
                .get(&(identity.session_id, stream_id))
                .map(|entry| (entry.events.clone(), entry.binding.clone()))
        };
        let Some((events, binding)) = input else {
            return Ok(());
        };
        // Path lifecycle shares the actor queue with frames. This preserves
        // ACK-before-detach order while retaining immediate genuine failover.
        let key = CarrierPathKey {
            underlay: identity.underlay,
            path_id: identity.path_id,
        };
        let Some(outcome) = binding.begin_path_detach(key, identity.path_instance_id) else {
            return Ok(());
        };
        let ResponsePathDetachOutcome::Begun(output_incarnation) = outcome else {
            // Replayed detach frames share the exact carrier incarnation's
            // existing lifecycle event and cannot create additional waiters.
            return Ok(());
        };
        queue_ordered_path_detach(
            events,
            binding,
            key,
            identity.path_instance_id,
            output_incarnation,
        );
        Ok(())
    }

    fn stream_frame_route_target(
        &self,
        session_id: SessionId,
        stream_id: StreamId,
    ) -> Option<ServerStreamFrameRouteTarget> {
        self.streams
            .lock()
            .expect("server reliable stream registry lock")
            .get(&(session_id, stream_id))
            .map(|entry| ServerStreamFrameRouteTarget {
                events: entry.events.clone(),
                binding: entry.binding.clone(),
            })
    }

    async fn route_frame_to_target(
        frame: Frame,
        target: ServerStreamFrameRouteTarget,
    ) -> Result<(), RuntimeError> {
        #[cfg(feature = "lab-diagnostics")]
        let bytes = reliable_path_frame_pacing_bytes(&frame);
        #[cfg(feature = "lab-diagnostics")]
        let started = Instant::now();
        // The relay supervisor retires registry membership after the per-stream
        // receiver closes. A frame in that interval is stream-local teardown,
        // not failure of the multiplexed carrier that delivered it.
        let _ = target
            .events
            .send(ServerReliableStreamEvent::Frame(frame))
            .await;
        #[cfg(feature = "lab-diagnostics")]
        lab_perf_record(
            "runtime.server_stream.route_frame",
            started.elapsed(),
            bytes,
        );
        Ok(())
    }

    fn try_route_frame_to_target(
        frame: Frame,
        target: ServerStreamFrameRouteTarget,
    ) -> Result<ServerStreamFrameRoute, RuntimeError> {
        match target
            .events
            .try_send(ServerReliableStreamEvent::Frame(frame))
        {
            Ok(()) => Ok(ServerStreamFrameRoute::Routed),
            Err(mpsc::error::TrySendError::Full(ServerReliableStreamEvent::Frame(frame))) => {
                Ok(ServerStreamFrameRoute::Backpressured(frame))
            }
            // See `route_frame`: retirement owns this short closed-receiver
            // interval, and one finished stream must not close its carrier.
            Err(mpsc::error::TrySendError::Closed(_)) => Ok(ServerStreamFrameRoute::Routed),
            Err(mpsc::error::TrySendError::Full(ServerReliableStreamEvent::PathDetached {
                ..
            })) => {
                unreachable!("server frame routing only sends frame events")
            }
        }
    }

    fn record_request_feedback_ingress_from_path_target(
        identity: ServerCarrierPathIdentity,
        frame: &Frame,
        target: &ServerStreamFrameRouteTarget,
    ) {
        let ServerCarrierPathIdentity {
            underlay,
            path_id,
            path_instance_id,
            ..
        } = identity;
        if matches!(frame, Frame::StreamData { .. } | Frame::StreamFin { .. }) {
            // Connection-level feedback may return on any carrier. Remember
            // ingress only as a return-path hint, never as fixed path ownership.
            target.binding.record_request_feedback_ingress(
                CarrierPathKey { underlay, path_id },
                path_instance_id,
            );
        }
    }

    fn apply_response_startup_final(
        target: &ServerStreamFrameRouteTarget,
        retained_ordinals: &[u8],
    ) -> Result<(), RuntimeError> {
        let outcome = target
            .binding
            .finalize_response_startup_plan(retained_ordinals)?;
        let ResponseStartupFinalOutcome::Finalized { withdrawn_outputs } = outcome else {
            return Ok(());
        };
        for output in withdrawn_outputs {
            queue_ordered_path_detach(
                target.events.clone(),
                target.binding.clone(),
                output.key,
                output.path_instance_id,
                output.incarnation,
            );
        }
        Ok(())
    }

    async fn route_frame_from_path(
        &self,
        identity: ServerCarrierPathIdentity,
        stream_id: StreamId,
        frame: Frame,
    ) -> Result<(), RuntimeError> {
        let Some(target) = self.stream_frame_route_target(identity.session_id, stream_id) else {
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "server_stream_unknown_frame_drop",
                format_args!(
                    "session_id={} stream_id={} frame_kind={}",
                    identity.session_id.0,
                    stream_id.0,
                    frame.kind_name(),
                ),
            );
            return Ok(());
        };
        let key = CarrierPathKey {
            underlay: identity.underlay,
            path_id: identity.path_id,
        };
        match &frame {
            Frame::StreamReturnPlanFinal {
                retained_ordinals, ..
            } => {
                Self::apply_response_startup_final(&target, retained_ordinals)?;
                return Ok(());
            }
            Frame::StreamRequalifyData {
                probe_id,
                offset,
                payload,
                ..
            } => {
                let payload_bytes = u32::try_from(payload.len())
                    .map_err(|_| RuntimeError::Protocol("requalification payload overflow"))?;
                return target.binding.accept_request_requalification_probe(
                    key,
                    identity.path_instance_id,
                    stream_id,
                    crate::model::requalification::StreamRequalificationProbe {
                        id: *probe_id,
                        offset: *offset,
                        payload_bytes,
                    },
                );
            }
            Frame::StreamRequalifyAck {
                probe_id,
                offset,
                payload_bytes,
                ..
            } => {
                target.binding.acknowledge_response_requalification_probe(
                    key,
                    identity.path_instance_id,
                    crate::model::requalification::StreamRequalificationProbe {
                        id: *probe_id,
                        offset: *offset,
                        payload_bytes: *payload_bytes,
                    },
                );
                return Ok(());
            }
            _ => {}
        }
        Self::record_request_feedback_ingress_from_path_target(identity, &frame, &target);
        Self::route_frame_to_target(frame, target).await
    }

    fn try_route_frame_from_path(
        &self,
        identity: ServerCarrierPathIdentity,
        stream_id: StreamId,
        frame: Frame,
    ) -> Result<ServerStreamFrameRoute, RuntimeError> {
        let Some(target) = self.stream_frame_route_target(identity.session_id, stream_id) else {
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "server_stream_unknown_frame_drop",
                format_args!(
                    "session_id={} stream_id={} frame_kind={}",
                    identity.session_id.0,
                    stream_id.0,
                    frame.kind_name(),
                ),
            );
            return Ok(ServerStreamFrameRoute::Routed);
        };
        let key = CarrierPathKey {
            underlay: identity.underlay,
            path_id: identity.path_id,
        };
        match &frame {
            Frame::StreamReturnPlanFinal {
                retained_ordinals, ..
            } => {
                Self::apply_response_startup_final(&target, retained_ordinals)?;
                return Ok(ServerStreamFrameRoute::Routed);
            }
            Frame::StreamRequalifyData {
                probe_id,
                offset,
                payload,
                ..
            } => {
                let payload_bytes = u32::try_from(payload.len())
                    .map_err(|_| RuntimeError::Protocol("requalification payload overflow"))?;
                return match target.binding.accept_request_requalification_probe(
                    key,
                    identity.path_instance_id,
                    stream_id,
                    crate::model::requalification::StreamRequalificationProbe {
                        id: *probe_id,
                        offset: *offset,
                        payload_bytes,
                    },
                ) {
                    Ok(()) => Ok(ServerStreamFrameRoute::Routed),
                    Err(RuntimeError::SenderServiceBlocked) => {
                        Ok(ServerStreamFrameRoute::Backpressured(frame))
                    }
                    Err(error) => Err(error),
                };
            }
            Frame::StreamRequalifyAck {
                probe_id,
                offset,
                payload_bytes,
                ..
            } => {
                target.binding.acknowledge_response_requalification_probe(
                    key,
                    identity.path_instance_id,
                    crate::model::requalification::StreamRequalificationProbe {
                        id: *probe_id,
                        offset: *offset,
                        payload_bytes: *payload_bytes,
                    },
                );
                return Ok(ServerStreamFrameRoute::Routed);
            }
            _ => {}
        }
        Self::record_request_feedback_ingress_from_path_target(identity, &frame, &target);
        Self::try_route_frame_to_target(frame, target)
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
        configured_slot: ConfiguredMemberSlot,
        local: crate::runtime::path::ServerLocalPathProperties,
        initial_peer_usage: Option<PathUsage>,
        native_capacity_epoch: u64,
        apply_authority: ServerCarrierPathApplyAuthority,
        principal_permit: PrincipalPermit,
        retirement_completion: watch::Sender<bool>,
    ) -> Result<crate::runtime::path::ServerSessionRetirement, RuntimeError> {
        self.registry.activate_carrier_path(
            identity,
            configured_slot,
            local,
            initial_peer_usage,
            native_capacity_epoch,
            apply_authority,
            principal_permit,
            retirement_completion,
        )
    }

    fn retire_carrier_path(
        &self,
        identity: ServerCarrierPathIdentity,
    ) -> ServerCarrierPathRetirement {
        self.registry.retire_carrier_path(identity)
    }

    fn session_retirement(
        &self,
        session_id: SessionId,
    ) -> Result<crate::runtime::path::ServerSessionRetirement, RuntimeError> {
        self.registry.session_retirement(session_id)
    }

    fn retire_session(
        &self,
        session_id: SessionId,
        reason: crate::protocol::CloseReason,
    ) -> crate::protocol::CloseReason {
        self.registry.retire_session(session_id, reason)
    }

    fn set_carrier_path_state(&self, identity: ServerCarrierPathIdentity, state: PeerPathState) {
        self.registry.set_carrier_path_state(identity, state);
    }

    fn register_realtime_flow(
        &self,
        session_id: SessionId,
    ) -> Result<ServerRealtimeFlowLease, RuntimeError> {
        let registration = self.registry.register_realtime_flow(session_id)?;
        let retirement = registration.retirement();
        Ok(ServerRealtimeFlowLease::hold(registration, retirement))
    }

    fn open_or_attach<'a>(
        &'a self,
        request: ServerStreamOpenRequest,
        new_stream_policy: ServerNewStreamPolicy,
        opening_ingress: Option<ServerMppIngress>,
    ) -> Pin<Box<dyn Future<Output = Result<ServerStreamOpenOutcome, RuntimeError>> + Send + 'a>>
    {
        let registry = self.registry.clone();
        Box::pin(async move {
            match registry.open_or_attach_with_ingress(request, opening_ingress)? {
                ServerReliableStreamOpen::New(accepted, response_lane) => {
                    let accepted = *accepted;
                    match new_stream_policy {
                        ServerNewStreamPolicy::Submit => {
                            if let Err(error) = accepted.admit_opening_path() {
                                // This backend may execute inside the carrier
                                // actor that owns the command receiver. Drop
                                // schedules cleanup without awaiting its own
                                // full queue.
                                drop(accepted);
                                return Err(error);
                            }
                            if let Err(accepted) = registry.submit_accepted(accepted) {
                                accepted.close().await;
                                return Err(RuntimeError::Protocol(
                                    "server reliable stream service closed",
                                ));
                            }
                        }
                        ServerNewStreamPolicy::Reject => accepted.close().await,
                    }
                    Ok(ServerStreamOpenOutcome::New(response_lane))
                }
                ServerReliableStreamOpen::Existing(response_lane) => {
                    Ok(ServerStreamOpenOutcome::Existing(response_lane))
                }
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

    fn try_route_frame(
        &self,
        identity: ServerCarrierPathIdentity,
        stream_id: StreamId,
        frame: Frame,
    ) -> Result<ServerStreamFrameRoute, RuntimeError> {
        self.registry
            .try_route_frame_from_path(identity, stream_id, frame)
    }

    fn detach_path(
        &self,
        identity: ServerCarrierPathIdentity,
        stream_id: StreamId,
    ) -> Result<(), RuntimeError> {
        self.registry.detach_path(identity, stream_id)
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

    fn record_local_path_metrics(
        &self,
        identity: ServerCarrierPathIdentity,
        metrics: PathMetrics,
        native_drain_observed: bool,
        native_capacity_epoch: Option<u64>,
        native_window_sample: Option<CarrierNativeWindowSample>,
        delivery_rate_sample: Option<CarrierDeliveryRateSample>,
    ) {
        self.registry
            .record_local_path_metrics_with_delivery_rate_sample(
                identity,
                metrics,
                native_drain_observed,
                native_capacity_epoch,
                native_window_sample,
                delivery_rate_sample,
            );
    }

    fn stage_native_scheduling_shape(
        &self,
        identity: ServerCarrierPathIdentity,
        shape: NativeCarrierSchedulingShapeSnapshot,
    ) -> bool {
        self.registry.stage_native_scheduling_shape(identity, shape)
    }

    fn fanout_native_scheduling_shape(
        &self,
        identity: ServerCarrierPathIdentity,
        shape: NativeCarrierSchedulingShapeSnapshot,
    ) {
        self.registry
            .fanout_native_scheduling_shape(identity, shape);
    }

    fn record_path_proof_success(
        &self,
        identity: ServerCarrierPathIdentity,
        observation: PathProofObservation,
    ) {
        self.registry
            .record_path_proof_success(identity, observation);
    }

    fn peer_status_snapshot(&self, session_id: SessionId) -> Vec<PeerPathStatus> {
        self.registry.peer_status_snapshot(session_id)
    }

    fn carrier_path_statuses(
        &self,
        identities: &[ServerCarrierPathIdentity],
    ) -> Vec<Option<ServerCarrierPathStatusSnapshot>> {
        self.registry.carrier_path_statuses(identities)
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

fn path_metrics_after_residence(mut metrics: PathMetrics, residence: Duration) -> PathMetrics {
    let residence_us = u64::try_from(residence.as_micros()).unwrap_or(u64::MAX);
    metrics.metric_age_us = metrics
        .metric_age_us
        .saturating_add(u32::try_from(residence_us).unwrap_or(u32::MAX));
    metrics.rate_valid_for_us = metrics.rate_valid_for_us.saturating_sub(residence_us);
    metrics
}

fn project_carrier_path_status(
    basis: ServerCarrierPathStatusBasis,
    metrics_entry: Option<ServerPathMetricsEntry>,
    now: Instant,
) -> ServerCarrierPathStatusSnapshot {
    let ServerCarrierPathIdentity {
        session_id,
        underlay,
        path_id,
        path_instance_id,
    } = basis.identity;
    let (metrics, source, carrier_delivery_rate_sample) = metrics_entry.map_or_else(
        || {
            (
                basis.local.initial_metrics,
                basis.local.initial_metrics.map(|_| "startup"),
                None,
            )
        },
        |entry| {
            let residence = now.saturating_duration_since(entry.recorded_at);
            (
                Some(path_metrics_after_residence(entry.metrics, residence)),
                Some(match entry.source {
                    ServerPathMetricsSource::PeerHint => "peer_hint",
                    ServerPathMetricsSource::LocalSender => "local_sender",
                }),
                entry.carrier_delivery_rate_sample,
            )
        },
    );
    ServerCarrierPathStatusSnapshot {
        session_id,
        underlay,
        path_id,
        path_instance_id,
        configured_index: basis.local.config_ordinal,
        policy: basis.local.policy,
        state: basis.state,
        usage: basis.usage,
        metrics,
        carrier_delivery_rate_sample,
        eligibility_epoch: basis.eligibility_epoch,
        native_scheduling_shape: basis.native_scheduling_shape,
        source,
    }
}

#[cfg(test)]
#[path = "tests_registry.rs"]
mod tests;
