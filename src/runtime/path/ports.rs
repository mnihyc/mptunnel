//! Contracts crossing between carrier paths and product-stream ownership.
//!
//! Carriers publish accepted transport state here without constructing stream
//! policy objects. The stream layer consumes these values and owns offsets,
//! reinjection, and attachment behavior.

use super::authority::NativeCarrierSchedulingShapeSnapshot;
use super::commands::ReliablePathCommandSender;
use crate::model::carrier_rate_authority::CarrierRateAuthorityStamp;
use crate::model::path::{CarrierPathInstanceId, PathPolicy, try_next_carrier_path_instance_id};
use crate::mux::MuxLimits;
use crate::product::PrincipalPermit;
use crate::protocol::{
    CloseReason, Frame, OffsetRange, PathId, PathMetrics, PathUsage, PeerPathState, PeerPathStatus,
    SessionId, StreamDemandHint, StreamId, StreamReturnPlan, TargetAddr, UnderlayProtocol,
};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::proof::{PathProofObservation, allocated_path_proof_data_frame};
use crate::scheduler::{PathSnapshot, TrafficClass};
use crate::transport::RateHint;
use bytes::Bytes;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::{mpsc, oneshot, watch};

/// A positive-ACK, non-application-limited delivery sample for one carrier.
///
/// Product ACK evidence remains stream-owned. This sample only describes the
/// transport capacity observed by the exact carrier instance that published it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct CarrierDeliveryRateSample {
    pub(in crate::runtime) delivery_rate_bps: u64,
    /// Native pacing sampled in the same ACK epoch, when exposed by the host.
    pub(in crate::runtime) pacing_rate_bps: Option<u64>,
    pub(in crate::runtime) sample_count: u32,
    pub(in crate::runtime) sample_bytes: u64,
    pub(in crate::runtime) delivery_window_covered: bool,
    /// Exact local epoch of the newest qualified native ACK in this sample.
    /// Registry refreshes must preserve this provenance unchanged.
    pub(in crate::runtime) observed_at: std::time::Instant,
    /// Three-PTO expiry frozen from the transport timing observed in the same
    /// ACK epoch. Later app-limited RTT polls cannot rewrite this lifetime.
    pub(in crate::runtime) expires_at: std::time::Instant,
}

/// One immutable native carrier-window observation.
///
/// The raw carrier metric may remain available for diagnostics after this
/// sample expires. Product admission consumes only this frozen authority: a
/// later RTT observation cannot extend or shorten its lifetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct CarrierNativeWindowSample {
    pub(in crate::runtime) inflight_limit_bytes: u64,
    pub(in crate::runtime) observed_at: std::time::Instant,
    pub(in crate::runtime) expires_at: std::time::Instant,
}

impl CarrierNativeWindowSample {
    pub(in crate::runtime) fn new(
        inflight_limit_bytes: u64,
        observed_at: std::time::Instant,
        freshness_horizon: std::time::Duration,
    ) -> Option<Self> {
        (inflight_limit_bytes > 0)
            .then(|| observed_at.checked_add(freshness_horizon))
            .flatten()
            .map(|expires_at| Self {
                inflight_limit_bytes,
                observed_at,
                expires_at,
            })
    }

    pub(in crate::runtime) fn fresh_at(self, now: std::time::Instant) -> bool {
        self.observed_at <= now && now < self.expires_at
    }

    pub(in crate::runtime) fn from_path_metrics_at(
        metrics: PathMetrics,
        observed_at: std::time::Instant,
    ) -> Option<Self> {
        Self::new(
            metrics.inflight_limit_bytes,
            observed_at,
            crate::model::timing::transport_rate_sample_freshness_horizon(
                std::time::Duration::from_micros(u64::from(metrics.srtt_us.max(1))),
                std::time::Duration::from_micros(u64::from(metrics.rttvar_us)),
            ),
        )
    }
}

/// Stable notification that the complete authenticated MPP session became
/// terminal. Native carrier loss and exact-path drain never publish here.
#[derive(Clone)]
pub(in crate::runtime) struct ServerSessionRetirement {
    reason: watch::Receiver<Option<CloseReason>>,
}

impl ServerSessionRetirement {
    pub(in crate::runtime) fn pending(reason: watch::Receiver<Option<CloseReason>>) -> Self {
        Self { reason }
    }

    pub(in crate::runtime) fn reason(&self) -> Option<CloseReason> {
        *self.reason.borrow()
    }

    pub(in crate::runtime) fn is_retired(&self) -> bool {
        self.reason().is_some()
    }

    pub(in crate::runtime) async fn wait(mut self) -> CloseReason {
        loop {
            if let Some(reason) = *self.reason.borrow_and_update() {
                return reason;
            }
            if self.reason.changed().await.is_err() {
                std::future::pending::<CloseReason>().await;
            }
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn active_for_test() -> Self {
        Self::pending(watch::channel(None).1)
    }
}

/// Runs the transport readiness publication under the same sticky terminal
/// fence carried by the admitted path. A close already published for this
/// SessionId always wins, even when the transport is immediately writable.
pub(in crate::runtime) async fn fence_server_carrier_readiness<T>(
    retirement: ServerSessionRetirement,
    readiness: impl Future<Output = Result<T, RuntimeError>>,
) -> Result<T, RuntimeError> {
    let terminal = retirement.wait();
    tokio::pin!(terminal);
    tokio::pin!(readiness);
    tokio::select! {
        biased;
        reason = &mut terminal => Err(RuntimeError::RemoteClosed(reason)),
        result = &mut readiness => result,
    }
}

impl std::fmt::Debug for ServerSessionRetirement {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ServerSessionRetirement")
            .field("reason", &self.reason())
            .finish()
    }
}

/// Keeps one carrier-independent Product owner attached to the authenticated
/// server session. Datagram workers and retained logical IP tunnels use this
/// lease so carrier loss cannot discard principal or terminal-session state.
pub(in crate::runtime) struct ServerRealtimeFlowLease {
    _guard: Box<dyn Send + Sync>,
    retirement: ServerSessionRetirement,
}

impl ServerRealtimeFlowLease {
    pub(in crate::runtime) fn hold<T>(guard: T, retirement: ServerSessionRetirement) -> Self
    where
        T: Send + Sync + 'static,
    {
        Self {
            _guard: Box::new(guard),
            retirement,
        }
    }

    pub(in crate::runtime) fn retirement(&self) -> ServerSessionRetirement {
        self.retirement.clone()
    }

    pub(in crate::runtime) fn is_retired(&self) -> bool {
        self.retirement.is_retired()
    }
}

impl std::fmt::Debug for ServerRealtimeFlowLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ServerRealtimeFlowLease")
            .field("retirement", &self.retirement)
            .finish_non_exhaustive()
    }
}

/// One target-bound datagram handed from a carrier to the product worker.
pub(in crate::runtime) struct ServerDatagramRequest {
    pub(in crate::runtime) datagram_id: crate::protocol::DatagramId,
    pub(in crate::runtime) ttl_ms: u32,
    pub(in crate::runtime) payload: Bytes,
}

pub(in crate::runtime) enum ServerDatagramWorkerMessage {
    Attach {
        commands: ReliablePathCommandSender,
        attachment: Weak<()>,
        attached: oneshot::Sender<()>,
    },
    Request {
        request: ServerDatagramRequest,
        commands: ReliablePathCommandSender,
        attachment: Weak<()>,
        admission: oneshot::Sender<Result<ServerDatagramSendOutcome, RuntimeError>>,
    },
    ResponseFeedback {
        received: Vec<OffsetRange>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum ServerDatagramSendOutcome {
    Accepted,
    Full,
    Closed,
}

/// Stable result retained for a denied datagram flow ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum ServerDatagramTombstone {
    Reject,
    Drop,
    CapacityReject,
}

/// A carrier-local, independently bounded LRU of non-accepted datagram opens.
///
/// Tombstones never occupy accepted-flow slots. Remembering them makes a
/// retransmitted OPEN deterministic while keeping attacker-controlled flow
/// IDs bounded by the same configured per-session cardinality.
pub(in crate::runtime) struct ServerDatagramTombstoneCache {
    capacity: usize,
    entries: HashMap<crate::protocol::DatagramFlowId, ServerDatagramTombstone>,
    lru: VecDeque<crate::protocol::DatagramFlowId>,
}

impl ServerDatagramTombstoneCache {
    pub(in crate::runtime) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: HashMap::new(),
            lru: VecDeque::new(),
        }
    }

    pub(in crate::runtime) fn get(
        &mut self,
        flow_id: crate::protocol::DatagramFlowId,
    ) -> Option<ServerDatagramTombstone> {
        let tombstone = self.entries.get(&flow_id).copied()?;
        self.lru.retain(|current| *current != flow_id);
        self.lru.push_back(flow_id);
        Some(tombstone)
    }

    pub(in crate::runtime) fn insert(
        &mut self,
        flow_id: crate::protocol::DatagramFlowId,
        tombstone: ServerDatagramTombstone,
    ) {
        let _ = self.insert_with_eviction(flow_id, tombstone);
    }

    pub(in crate::runtime) fn insert_with_eviction(
        &mut self,
        flow_id: crate::protocol::DatagramFlowId,
        tombstone: ServerDatagramTombstone,
    ) -> Option<crate::protocol::DatagramFlowId> {
        if self.capacity == 0 {
            return Some(flow_id);
        }
        if self.entries.insert(flow_id, tombstone).is_some() {
            self.lru.retain(|current| *current != flow_id);
            self.lru.push_back(flow_id);
            return None;
        }
        self.lru.push_back(flow_id);
        let mut evicted = None;
        while self.entries.len() > self.capacity {
            let Some(evicted_id) = self.lru.pop_front() else {
                break;
            };
            if self.entries.remove(&evicted_id).is_some() {
                debug_assert!(evicted_id != flow_id);
                evicted = Some(evicted_id);
            }
        }
        evicted
    }

    pub(in crate::runtime) fn remove(&mut self, flow_id: crate::protocol::DatagramFlowId) {
        self.entries.remove(&flow_id);
        self.lru.retain(|current| *current != flow_id);
    }

    pub(in crate::runtime) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[cfg(test)]
    pub(in crate::runtime) fn len(&self) -> usize {
        self.entries.len()
    }
}

/// Accepted target worker plus the higher-layer accounting it keeps alive.
pub(in crate::runtime) struct AcceptedServerDatagramFlow {
    flow_id: crate::protocol::DatagramFlowId,
    requests: mpsc::Sender<ServerDatagramWorkerMessage>,
    commands: ReliablePathCommandSender,
    route_lifetime: Arc<()>,
    retired: Arc<AtomicBool>,
    session_retirement: ServerSessionRetirement,
    _attachment: Box<dyn Send + Sync>,
}

impl AcceptedServerDatagramFlow {
    pub(in crate::runtime) fn holding(
        flow_id: crate::protocol::DatagramFlowId,
        requests: mpsc::Sender<ServerDatagramWorkerMessage>,
        commands: ReliablePathCommandSender,
        route_lifetime: Arc<()>,
        retired: Arc<AtomicBool>,
        session_retirement: ServerSessionRetirement,
        attachment: impl Send + Sync + 'static,
    ) -> Self {
        Self {
            flow_id,
            requests,
            commands,
            route_lifetime,
            retired,
            session_retirement,
            _attachment: Box::new(attachment),
        }
    }

    pub(in crate::runtime) fn flow_id(&self) -> crate::protocol::DatagramFlowId {
        self.flow_id
    }

    pub(in crate::runtime) async fn send(
        &self,
        request: ServerDatagramRequest,
    ) -> Result<ServerDatagramSendOutcome, RuntimeError> {
        if self.retired.load(Ordering::Acquire) || self.session_retirement.is_retired() {
            return Ok(ServerDatagramSendOutcome::Closed);
        }
        let (admission, admitted) = oneshot::channel();
        match self
            .requests
            .try_send(ServerDatagramWorkerMessage::Request {
                request,
                commands: self.commands.clone(),
                attachment: Arc::downgrade(&self.route_lifetime),
                admission,
            }) {
            Ok(()) => match admitted.await {
                Ok(result) => result,
                Err(_) => Ok(ServerDatagramSendOutcome::Closed),
            },
            Err(mpsc::error::TrySendError::Full(_)) => Ok(ServerDatagramSendOutcome::Full),
            Err(mpsc::error::TrySendError::Closed(_)) => Ok(ServerDatagramSendOutcome::Closed),
        }
    }

    pub(in crate::runtime) fn acknowledge_response(&self, received: Vec<OffsetRange>) {
        if self.retired.load(Ordering::Acquire) || self.session_retirement.is_retired() {
            return;
        }
        let _ = self
            .requests
            .try_send(ServerDatagramWorkerMessage::ResponseFeedback { received });
    }
}

impl std::fmt::Debug for AcceptedServerDatagramFlow {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AcceptedServerDatagramFlow")
            .field("flow_id", &self.flow_id)
            .finish_non_exhaustive()
    }
}

/// Target-open failure plus any registered flow accounting awaiting close.
pub(in crate::runtime) struct ServerDatagramOpenError {
    failure: ServerDatagramOpenFailure,
}

pub(in crate::runtime) enum ServerDatagramOpenFailure {
    Capacity,
    Runtime(RuntimeError),
}

impl ServerDatagramOpenError {
    pub(in crate::runtime) fn new(error: RuntimeError) -> Self {
        Self {
            failure: ServerDatagramOpenFailure::Runtime(error),
        }
    }

    pub(in crate::runtime) fn capacity() -> Self {
        Self {
            failure: ServerDatagramOpenFailure::Capacity,
        }
    }

    pub(in crate::runtime) fn into_failure(self) -> ServerDatagramOpenFailure {
        self.failure
    }
}

impl std::fmt::Debug for ServerDatagramOpenError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ServerDatagramOpenError")
            .field(
                "kind",
                &match &self.failure {
                    ServerDatagramOpenFailure::Capacity => "capacity",
                    ServerDatagramOpenFailure::Runtime(_) => "runtime",
                },
            )
            .finish_non_exhaustive()
    }
}

pub(in crate::runtime) struct ServerDatagramOpenRequest {
    pub(in crate::runtime) session_id: SessionId,
    pub(in crate::runtime) principal_permit: PrincipalPermit,
    pub(in crate::runtime) flow_id: crate::protocol::DatagramFlowId,
    pub(in crate::runtime) target: TargetAddr,
    pub(in crate::runtime) commands: ReliablePathCommandSender,
    pub(in crate::runtime) ingress: ServerMppIngress,
}

type ServerDatagramPortFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<AcceptedServerDatagramFlow, ServerDatagramOpenError>>
            + Send
            + 'a,
    >,
>;

/// Target-side datagram service implemented above TCP and QUIC carriers.
pub(in crate::runtime) trait ServerDatagramPortBackend: Send + Sync {
    fn open<'a>(&'a self, request: ServerDatagramOpenRequest) -> ServerDatagramPortFuture<'a>;

    fn retire_session(&self, _session_id: SessionId) {}
}

#[derive(Clone)]
pub(in crate::runtime) struct ServerDatagramPort {
    backend: Arc<dyn ServerDatagramPortBackend>,
}

impl ServerDatagramPort {
    pub(in crate::runtime) fn new(backend: Arc<dyn ServerDatagramPortBackend>) -> Self {
        Self { backend }
    }

    pub(in crate::runtime) async fn open(
        &self,
        request: ServerDatagramOpenRequest,
    ) -> Result<AcceptedServerDatagramFlow, ServerDatagramOpenError> {
        self.backend.open(request).await
    }

    pub(in crate::runtime) fn retire_session(&self, session_id: SessionId) {
        self.backend.retire_session(session_id);
    }
}

impl std::fmt::Debug for ServerDatagramPort {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ServerDatagramPort")
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ServerCarrierPathStatusSnapshot {
    pub(in crate::runtime) session_id: SessionId,
    pub(in crate::runtime) underlay: UnderlayProtocol,
    pub(in crate::runtime) path_id: PathId,
    pub(in crate::runtime) path_instance_id: CarrierPathInstanceId,
    pub(in crate::runtime) configured_index: usize,
    pub(in crate::runtime) policy: PathPolicy,
    pub(in crate::runtime) state: PeerPathState,
    pub(in crate::runtime) usage: Option<PathUsage>,
    pub(in crate::runtime) metrics: Option<PathMetrics>,
    pub(in crate::runtime) carrier_delivery_rate_sample: Option<CarrierDeliveryRateSample>,
    /// Exact structural generation for state/usage/retirement qualification.
    /// `None` is the fail-closed exhausted state.
    pub(in crate::runtime) eligibility_epoch: Option<u64>,
    /// Endpoint-local NativeMode authority and activation-coherent shape.
    /// This never crosses the peer wire; lineage ACK diagnostics above do not
    /// alter it.
    pub(in crate::runtime) native_scheduling_shape: Option<NativeCarrierSchedulingShapeSnapshot>,
    pub(in crate::runtime) source: Option<&'static str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct ServerCarrierPathApplySnapshot {
    pub(in crate::runtime) eligibility_epoch: Option<u64>,
    pub(in crate::runtime) native_scheduling_shape: Option<NativeCarrierSchedulingShapeSnapshot>,
}

#[derive(Debug)]
struct ServerCarrierPathApplyState {
    eligibility_epoch: Option<u64>,
    native_scheduling_shape: Option<NativeCarrierSchedulingShapeSnapshot>,
}

/// Exact per-carrier structural/Native publication fence.
///
/// Registry transitions publish here while holding their owner lock. Packet
/// publication holds this fence through its first irreversible queue mutation,
/// so structural eligibility cannot change after validation. This owns the
/// registry's one shape copy; it is not another rate/controller authority.
#[derive(Debug, Clone)]
pub(in crate::runtime) struct ServerCarrierPathApplyAuthority {
    inner: Arc<Mutex<ServerCarrierPathApplyState>>,
}

impl ServerCarrierPathApplyAuthority {
    pub(in crate::runtime) fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(ServerCarrierPathApplyState {
                eligibility_epoch: Some(1),
                native_scheduling_shape: None,
            })),
        }
    }

    pub(in crate::runtime) fn snapshot(&self) -> ServerCarrierPathApplySnapshot {
        let state = self
            .inner
            .lock()
            .expect("server carrier apply-authority lock");
        ServerCarrierPathApplySnapshot {
            eligibility_epoch: state.eligibility_epoch,
            native_scheduling_shape: state.native_scheduling_shape,
        }
    }

    pub(in crate::runtime) fn advance_eligibility_epoch(&self) {
        let mut state = self
            .inner
            .lock()
            .expect("server carrier apply-authority lock");
        state.eligibility_epoch = state
            .eligibility_epoch
            .and_then(|epoch| epoch.checked_add(1));
    }

    #[cfg(test)]
    pub(in crate::runtime) fn set_eligibility_epoch_for_test(&self, epoch: Option<u64>) {
        self.inner
            .lock()
            .expect("server carrier apply-authority lock")
            .eligibility_epoch = epoch;
    }

    pub(in crate::runtime) fn stage_native_scheduling_shape(
        &self,
        shape: NativeCarrierSchedulingShapeSnapshot,
    ) -> bool {
        let mut state = self
            .inner
            .lock()
            .expect("server carrier apply-authority lock");
        if let Some(previous) = state.native_scheduling_shape {
            if shape.stamp().revision() < previous.stamp().revision()
                || (shape.stamp().revision() == previous.stamp().revision()
                    && shape.stamp() != previous.stamp())
            {
                return false;
            }
        }
        if state.native_scheduling_shape == Some(shape) {
            false
        } else {
            state.native_scheduling_shape = Some(shape);
            true
        }
    }

    /// Hold structural identity and the registry's current shape through one
    /// Product apply. Native callers acquire their rate-authority fence first.
    pub(in crate::runtime) fn commit_if_current<R>(
        &self,
        expected_eligibility_epoch: u64,
        expected_native_stamp: Option<CarrierRateAuthorityStamp>,
        commit: impl FnOnce(Option<NativeCarrierSchedulingShapeSnapshot>) -> R,
    ) -> Option<R> {
        let state = self
            .inner
            .lock()
            .expect("server carrier apply-authority lock");
        if state.eligibility_epoch != Some(expected_eligibility_epoch) {
            return None;
        }
        if let Some(expected) = expected_native_stamp
            && !state
                .native_scheduling_shape
                .is_some_and(|shape| shape.stamp() == expected)
        {
            return None;
        }
        Some(commit(state.native_scheduling_shape))
    }
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ServerSessionManagementSnapshot {
    pub(in crate::runtime) session_id: SessionId,
    pub(in crate::runtime) reference_count: u32,
}

#[derive(Debug)]
pub(in crate::runtime) struct ServerStreamManagementSnapshot {
    #[cfg(test)]
    pub(in crate::runtime) active_streams: usize,
    pub(in crate::runtime) paths: Vec<ServerCarrierPathStatusSnapshot>,
    pub(in crate::runtime) sessions: Vec<ServerSessionManagementSnapshot>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct ServerCarrierPathIdentity {
    pub(in crate::runtime) session_id: SessionId,
    pub(in crate::runtime) underlay: UnderlayProtocol,
    pub(in crate::runtime) path_id: PathId,
    pub(in crate::runtime) path_instance_id: CarrierPathInstanceId,
}

/// Endpoint-local properties bound to one accepted carrier instance.
/// These values never cross the wire and are not derived from peer `PathId`.
#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ServerLocalPathProperties {
    pub(in crate::runtime) config_ordinal: usize,
    pub(in crate::runtime) policy: PathPolicy,
    /// Immutable endpoint-local rate prior for this accepted carrier.
    pub(in crate::runtime) startup_rate_prior: RateHint,
    pub(in crate::runtime) initial_metrics: Option<PathMetrics>,
}

impl Default for ServerLocalPathProperties {
    fn default() -> Self {
        Self {
            config_ordinal: 0,
            policy: PathPolicy::default(),
            startup_rate_prior: RateHint::Unknown,
            initial_metrics: None,
        }
    }
}

#[derive(Clone)]
pub(in crate::runtime) struct ServerCarrierPathRegistration {
    inner: Arc<ServerCarrierPathRegistrationInner>,
}

/// Non-owning exact-carrier state publication for authenticated I/O observers.
///
/// Decode tasks may publish a peer's ordered drain intent before bounded actor
/// delivery without extending the carrier registration lifetime themselves.
#[derive(Clone)]
pub(in crate::runtime) struct ServerCarrierPathStateHandle {
    inner: Weak<ServerCarrierPathRegistrationInner>,
}

#[derive(Clone)]
pub(in crate::runtime) struct ServerCarrierPeer {
    observe: Arc<dyn Fn() -> SocketAddr + Send + Sync>,
}

impl ServerCarrierPeer {
    pub(in crate::runtime) fn fixed(peer: SocketAddr) -> Self {
        Self {
            observe: Arc::new(move || peer),
        }
    }

    pub(in crate::runtime) fn observed(
        observe: impl Fn() -> SocketAddr + Send + Sync + 'static,
    ) -> Self {
        Self {
            observe: Arc::new(observe),
        }
    }

    fn current(&self) -> SocketAddr {
        (self.observe)()
    }
}

impl std::fmt::Debug for ServerCarrierPeer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("ServerCarrierPeer")
            .field(&self.current())
            .finish()
    }
}

/// Authenticated opening-carrier identity used for endpoint-local diagnostics.
/// `peer` is the transport endpoint observed by this server, not an end-client
/// address forwarded by the MPP peer and never routing source evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime) struct ServerMppIngress {
    session_id: SessionId,
    peer: SocketAddr,
    underlay: UnderlayProtocol,
    configured_path: Option<Arc<str>>,
    path_id: PathId,
    path_instance_id: CarrierPathInstanceId,
}

/// Lightweight observational authority detached from carrier-registration
/// lifetime. Persistent QUIC datagram streams retain this to snapshot the
/// current migrated peer at each logical flow open without keeping a retired
/// carrier registered merely for diagnostics.
#[derive(Debug, Clone)]
pub(in crate::runtime) struct ServerMppIngressObserver {
    session_id: SessionId,
    peer: ServerCarrierPeer,
    underlay: UnderlayProtocol,
    configured_path: Option<Arc<str>>,
    path_id: PathId,
    path_instance_id: CarrierPathInstanceId,
}

impl ServerMppIngressObserver {
    pub(in crate::runtime) fn snapshot(&self) -> ServerMppIngress {
        ServerMppIngress {
            session_id: self.session_id,
            peer: self.peer.current(),
            underlay: self.underlay,
            configured_path: self.configured_path.clone(),
            path_id: self.path_id,
            path_instance_id: self.path_instance_id,
        }
    }
}

impl ServerMppIngress {
    #[cfg(test)]
    pub(in crate::runtime) fn for_test(
        session_id: SessionId,
        peer: SocketAddr,
        underlay: UnderlayProtocol,
        configured_path: Option<&str>,
        path_id: PathId,
        path_instance_id: CarrierPathInstanceId,
    ) -> Self {
        Self {
            session_id,
            peer,
            underlay,
            configured_path: configured_path.map(Arc::from),
            path_id,
            path_instance_id,
        }
    }

    pub(in crate::runtime) fn session_id(&self) -> SessionId {
        self.session_id
    }

    pub(in crate::runtime) fn peer(&self) -> SocketAddr {
        self.peer
    }

    pub(in crate::runtime) fn underlay(&self) -> UnderlayProtocol {
        self.underlay
    }

    pub(in crate::runtime) fn configured_path(&self) -> Option<&str> {
        self.configured_path.as_deref()
    }

    pub(in crate::runtime) fn path_id(&self) -> PathId {
        self.path_id
    }

    pub(in crate::runtime) fn path_instance_id(&self) -> CarrierPathInstanceId {
        self.path_instance_id
    }
}

/// Completion authority for one exact carrier's ordered attachment retirement.
#[derive(Clone)]
pub(in crate::runtime) struct ServerCarrierPathRetirement {
    completed: watch::Receiver<bool>,
}

impl ServerCarrierPathRetirement {
    pub(in crate::runtime) fn pending(completed: watch::Receiver<bool>) -> Self {
        Self { completed }
    }

    pub(in crate::runtime) fn complete() -> Self {
        let (_completion, completed) = watch::channel(true);
        Self { completed }
    }

    pub(in crate::runtime) async fn wait(mut self) {
        while !*self.completed.borrow_and_update() {
            if self.completed.changed().await.is_err() {
                break;
            }
        }
    }
}

struct ServerCarrierPathRegistrationInner {
    backend: Arc<dyn ServerStreamPortBackend>,
    owner_token: usize,
    identity: ServerCarrierPathIdentity,
    local: ServerLocalPathProperties,
    principal_permit: PrincipalPermit,
    observed_ingress: Option<ServerCarrierObservedIngress>,
    session_retirement: ServerSessionRetirement,
    apply_authority: ServerCarrierPathApplyAuthority,
    validation: Arc<AtomicBool>,
    retirement: ServerCarrierPathRetirement,
}

#[derive(Debug, Clone)]
struct ServerCarrierObservedIngress {
    peer: ServerCarrierPeer,
    configured_path: Option<Arc<str>>,
}

/// Validation authority detached from carrier-registration lifetime.
#[derive(Clone)]
pub(in crate::runtime) struct ServerPathValidation {
    path_id: PathId,
    validated: Arc<AtomicBool>,
}

impl ServerPathValidation {
    pub(in crate::runtime) fn challenge(&self, mux_limits: MuxLimits) -> Option<Frame> {
        if self.validated.load(Ordering::Acquire) {
            return None;
        }
        let (_, frame) = allocated_path_proof_data_frame(self.path_id, mux_limits);
        Some(frame)
    }
}

impl ServerCarrierPathRegistration {
    pub(in crate::runtime) fn path_instance_id(&self) -> CarrierPathInstanceId {
        self.inner.identity.path_instance_id
    }

    pub(in crate::runtime) fn session_id(&self) -> SessionId {
        self.inner.identity.session_id
    }

    pub(in crate::runtime) fn underlay(&self) -> UnderlayProtocol {
        self.inner.identity.underlay
    }

    pub(in crate::runtime) fn path_id(&self) -> PathId {
        self.inner.identity.path_id
    }

    pub(in crate::runtime) fn local_policy(&self) -> PathPolicy {
        self.inner.local.policy
    }

    pub(in crate::runtime) fn local_config_ordinal(&self) -> usize {
        self.inner.local.config_ordinal
    }

    pub(in crate::runtime) fn initial_metrics(&self) -> Option<PathMetrics> {
        self.inner.local.initial_metrics
    }

    pub(in crate::runtime) fn startup_rate_prior(&self) -> RateHint {
        self.inner.local.startup_rate_prior
    }

    pub(in crate::runtime) fn principal_permit(&self) -> &PrincipalPermit {
        &self.inner.principal_permit
    }

    pub(in crate::runtime) fn mpp_ingress(&self) -> Option<ServerMppIngress> {
        self.mpp_ingress_observer()
            .map(|observer| observer.snapshot())
    }

    pub(in crate::runtime) fn mpp_ingress_observer(&self) -> Option<ServerMppIngressObserver> {
        let observed = self.inner.observed_ingress.as_ref()?;
        Some(ServerMppIngressObserver {
            session_id: self.session_id(),
            peer: observed.peer.clone(),
            underlay: self.underlay(),
            configured_path: observed.configured_path.clone(),
            path_id: self.path_id(),
            path_instance_id: self.path_instance_id(),
        })
    }

    pub(in crate::runtime) fn session_retirement(&self) -> ServerSessionRetirement {
        self.inner.session_retirement.clone()
    }

    pub(in crate::runtime) fn apply_authority(&self) -> ServerCarrierPathApplyAuthority {
        self.inner.apply_authority.clone()
    }

    /// Returns a fresh challenge until this carrier instance is validated.
    pub(in crate::runtime) fn path_validation_challenge(
        &self,
        mux_limits: MuxLimits,
    ) -> Option<Frame> {
        self.path_validation().challenge(mux_limits)
    }

    pub(in crate::runtime) fn path_validation(&self) -> ServerPathValidation {
        ServerPathValidation {
            path_id: self.path_id(),
            validated: self.inner.validation.clone(),
        }
    }

    fn mark_path_validated(&self) {
        self.inner.validation.store(true, Ordering::Release);
    }

    pub(in crate::runtime) fn set_state(&self, state: PeerPathState) {
        self.inner
            .backend
            .set_carrier_path_state(self.inner.identity, state);
    }

    pub(in crate::runtime) fn state_handle(&self) -> ServerCarrierPathStateHandle {
        ServerCarrierPathStateHandle {
            inner: Arc::downgrade(&self.inner),
        }
    }

    pub(in crate::runtime) fn begin_retirement(&self) -> ServerCarrierPathRetirement {
        let _ = self.inner.backend.retire_carrier_path(self.inner.identity);
        self.inner.retirement.clone()
    }

    fn belongs_to(&self, port: &ServerStreamPort) -> bool {
        self.inner.owner_token == port.owner_token
    }
}

impl ServerCarrierPathStateHandle {
    pub(in crate::runtime) fn set_state(&self, state: PeerPathState) {
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        inner.backend.set_carrier_path_state(inner.identity, state);
    }
}

impl std::fmt::Debug for ServerCarrierPathRegistration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ServerCarrierPathRegistration")
            .field("session_id", &self.session_id())
            .field("underlay", &self.underlay())
            .field("path_id", &self.path_id())
            .field("path_instance_id", &self.path_instance_id())
            .field("local_config_ordinal", &self.local_config_ordinal())
            .finish()
    }
}

impl Drop for ServerCarrierPathRegistrationInner {
    fn drop(&mut self) {
        let _ = self.backend.retire_carrier_path(self.identity);
    }
}

pub(in crate::runtime) struct ServerStreamPathAttachment {
    pub(in crate::runtime) path_registration: ServerCarrierPathRegistration,
    pub(in crate::runtime) commands: ReliablePathCommandSender,
    pub(in crate::runtime) max_frame_payload_bytes: usize,
}

/// Carrier request to create or join one server-side product stream.
pub(in crate::runtime) struct ServerStreamOpenRequest {
    pub(in crate::runtime) session_id: SessionId,
    pub(in crate::runtime) stream_id: StreamId,
    pub(in crate::runtime) target: TargetAddr,
    pub(in crate::runtime) initial_demand: StreamDemandHint,
    pub(in crate::runtime) return_plan: StreamReturnPlan,
    pub(in crate::runtime) attachment: ServerStreamPathAttachment,
    pub(in crate::runtime) mux_limits: MuxLimits,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum ServerStreamOpenOutcome {
    New(TrafficClass),
    Existing(TrafficClass),
    DuplicateLiveIgnored,
    Rejected,
    Dropped,
}

/// Policy result for one logical Product target.
///
/// Unlike a runtime or protocol error, rejection and silent drop are scoped
/// to the requested flow and must never fail its shared carrier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum ServerTargetAdmission {
    Allow,
    Reject,
    Drop,
}

pub(in crate::runtime) enum ServerStreamFrameRoute {
    Routed,
    Backpressured(Frame),
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) enum ServerNewStreamPolicy {
    Submit,
    Reject,
}

type ServerStreamPortFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, RuntimeError>> + Send + 'a>>;

/// Product-stream service implemented above the carrier layer.
///
/// TCP and QUIC keep distinct I/O and proof mechanics; this neutral port only
/// transports carrier identity, lifecycle, frames, and validated evidence.
pub(in crate::runtime) trait ServerStreamPortBackend: Send + Sync {
    fn owner_token(&self) -> usize;

    fn activate_carrier_path(
        &self,
        identity: ServerCarrierPathIdentity,
        local: ServerLocalPathProperties,
        initial_peer_usage: Option<PathUsage>,
        native_capacity_epoch: u64,
        apply_authority: ServerCarrierPathApplyAuthority,
        principal_permit: PrincipalPermit,
        retirement_completion: watch::Sender<bool>,
    ) -> Result<ServerSessionRetirement, RuntimeError>;

    fn retire_carrier_path(
        &self,
        identity: ServerCarrierPathIdentity,
    ) -> ServerCarrierPathRetirement;

    fn session_retirement(
        &self,
        session_id: SessionId,
    ) -> Result<ServerSessionRetirement, RuntimeError>;

    fn retire_session(&self, session_id: SessionId, reason: CloseReason) -> CloseReason;

    fn set_carrier_path_state(&self, identity: ServerCarrierPathIdentity, state: PeerPathState);

    fn register_realtime_flow(
        &self,
        session_id: SessionId,
    ) -> Result<ServerRealtimeFlowLease, RuntimeError>;

    fn open_or_attach<'a>(
        &'a self,
        request: ServerStreamOpenRequest,
        new_stream_policy: ServerNewStreamPolicy,
        opening_ingress: Option<ServerMppIngress>,
    ) -> ServerStreamPortFuture<'a, ServerStreamOpenOutcome>;

    fn route_frame<'a>(
        &'a self,
        identity: ServerCarrierPathIdentity,
        stream_id: StreamId,
        frame: Frame,
    ) -> ServerStreamPortFuture<'a, ()>;

    fn try_route_frame(
        &self,
        identity: ServerCarrierPathIdentity,
        stream_id: StreamId,
        frame: Frame,
    ) -> Result<ServerStreamFrameRoute, RuntimeError>;

    fn detach_path(
        &self,
        identity: ServerCarrierPathIdentity,
        stream_id: StreamId,
    ) -> Result<(), RuntimeError>;

    fn record_peer_path_metrics(&self, identity: ServerCarrierPathIdentity, metrics: PathMetrics);

    fn record_peer_path_usage(
        &self,
        identity: ServerCarrierPathIdentity,
        sequence: u64,
        usage: PathUsage,
    );

    fn record_local_path_metrics(
        &self,
        identity: ServerCarrierPathIdentity,
        metrics: PathMetrics,
        native_drain_observed: bool,
        native_capacity_epoch: Option<u64>,
        native_window_sample: Option<CarrierNativeWindowSample>,
        delivery_rate_sample: Option<CarrierDeliveryRateSample>,
    );

    fn stage_native_scheduling_shape(
        &self,
        identity: ServerCarrierPathIdentity,
        shape: NativeCarrierSchedulingShapeSnapshot,
    ) -> bool;

    fn fanout_native_scheduling_shape(
        &self,
        identity: ServerCarrierPathIdentity,
        shape: NativeCarrierSchedulingShapeSnapshot,
    );

    fn record_path_proof_success(
        &self,
        identity: ServerCarrierPathIdentity,
        observation: PathProofObservation,
    );

    fn peer_status_snapshot(&self, session_id: SessionId) -> Vec<PeerPathStatus>;

    fn carrier_path_statuses(
        &self,
        identities: &[ServerCarrierPathIdentity],
    ) -> Vec<Option<ServerCarrierPathStatusSnapshot>>;

    fn management_snapshot(&self) -> ServerStreamManagementSnapshot;
}

#[derive(Clone)]
pub(in crate::runtime) struct ServerStreamPort {
    backend: Arc<dyn ServerStreamPortBackend>,
    owner_token: usize,
    target_admission: Option<Arc<ServerStreamTargetAdmission>>,
}

pub(in crate::runtime) type ServerStreamTargetAdmission = dyn Fn(
        &PrincipalPermit,
        &ServerMppIngress,
        &TargetAddr,
    ) -> Result<ServerTargetAdmission, RuntimeError>
    + Send
    + Sync;

impl std::fmt::Debug for ServerStreamPort {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ServerStreamPort")
            .finish_non_exhaustive()
    }
}

impl ServerStreamPort {
    pub(in crate::runtime) fn new(backend: Arc<dyn ServerStreamPortBackend>) -> Self {
        let owner_token = backend.owner_token();
        Self {
            backend,
            owner_token,
            target_admission: None,
        }
    }

    /// Installs composition-owned target policy without exposing it to carriers.
    pub(in crate::runtime) fn with_target_admission(
        mut self,
        target_admission: Arc<ServerStreamTargetAdmission>,
    ) -> Self {
        self.target_admission = Some(target_admission);
        self
    }

    fn validate_target_with_ingress(
        &self,
        path_registration: &ServerCarrierPathRegistration,
        target: &TargetAddr,
        opening_ingress: Option<&ServerMppIngress>,
    ) -> Result<ServerTargetAdmission, RuntimeError> {
        if !path_registration.belongs_to(self) {
            return Err(RuntimeError::Protocol(
                "reliable path registration does not match stream service",
            ));
        }
        let Some(target_admission) = &self.target_admission else {
            return Ok(ServerTargetAdmission::Allow);
        };
        let ingress = opening_ingress.ok_or(RuntimeError::Protocol(
            "authenticated MPP carrier is missing its observed peer",
        ))?;
        (target_admission)(path_registration.principal_permit(), ingress, target)
    }

    #[cfg(test)]
    pub(in crate::runtime) fn register_carrier_path(
        &self,
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        local: ServerLocalPathProperties,
        principal_permit: PrincipalPermit,
    ) -> Result<ServerCarrierPathRegistration, RuntimeError> {
        self.register_carrier_path_with_observed_ingress(
            session_id,
            underlay,
            path_id,
            local,
            None,
            0,
            principal_permit,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::runtime) fn register_carrier_path_with_observed_peer_and_authority(
        &self,
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        local: ServerLocalPathProperties,
        peer_usage: PathUsage,
        native_capacity_epoch: u64,
        principal_permit: PrincipalPermit,
        peer: ServerCarrierPeer,
        configured_path: Option<Arc<str>>,
    ) -> Result<ServerCarrierPathRegistration, RuntimeError> {
        self.register_carrier_path_with_observed_ingress(
            session_id,
            underlay,
            path_id,
            local,
            Some(peer_usage),
            native_capacity_epoch,
            principal_permit,
            Some(ServerCarrierObservedIngress {
                peer,
                configured_path,
            }),
        )
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(in crate::runtime) fn register_carrier_path_with_observed_peer(
        &self,
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        local: ServerLocalPathProperties,
        principal_permit: PrincipalPermit,
        peer: ServerCarrierPeer,
        configured_path: Option<Arc<str>>,
    ) -> Result<ServerCarrierPathRegistration, RuntimeError> {
        self.register_carrier_path_with_observed_ingress(
            session_id,
            underlay,
            path_id,
            local,
            None,
            0,
            principal_permit,
            Some(ServerCarrierObservedIngress {
                peer,
                configured_path,
            }),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn register_carrier_path_with_observed_ingress(
        &self,
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        local: ServerLocalPathProperties,
        initial_peer_usage: Option<PathUsage>,
        native_capacity_epoch: u64,
        principal_permit: PrincipalPermit,
        observed_ingress: Option<ServerCarrierObservedIngress>,
    ) -> Result<ServerCarrierPathRegistration, RuntimeError> {
        let identity = ServerCarrierPathIdentity {
            session_id,
            underlay,
            path_id,
            path_instance_id: try_next_carrier_path_instance_id()
                .ok_or(RuntimeError::ExactIdentityExhausted)?,
        };
        let (retirement_completion, retirement) = watch::channel(false);
        let apply_authority = ServerCarrierPathApplyAuthority::new();
        let session_retirement = self.backend.activate_carrier_path(
            identity,
            local,
            initial_peer_usage,
            native_capacity_epoch,
            apply_authority.clone(),
            principal_permit.clone(),
            retirement_completion,
        )?;
        Ok(ServerCarrierPathRegistration {
            inner: Arc::new(ServerCarrierPathRegistrationInner {
                backend: self.backend.clone(),
                owner_token: self.owner_token,
                identity,
                local,
                principal_permit,
                observed_ingress,
                session_retirement,
                apply_authority,
                validation: Arc::new(AtomicBool::new(false)),
                retirement: ServerCarrierPathRetirement::pending(retirement),
            }),
        })
    }

    #[cfg(test)]
    pub(in crate::runtime) fn register_test_carrier_path(
        &self,
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        local: ServerLocalPathProperties,
    ) -> ServerCarrierPathRegistration {
        self.register_carrier_path_with_observed_peer(
            session_id,
            underlay,
            path_id,
            local,
            PrincipalPermit::for_test("test-peer"),
            ServerCarrierPeer::fixed(
                "203.0.113.7:51000"
                    .parse()
                    .expect("authenticated test carrier peer"),
            ),
            None,
        )
        .expect("register test carrier path")
    }

    pub(in crate::runtime) fn register_realtime_flow(
        &self,
        session_id: SessionId,
    ) -> Result<ServerRealtimeFlowLease, RuntimeError> {
        self.backend.register_realtime_flow(session_id)
    }

    pub(in crate::runtime) fn session_retirement(
        &self,
        session_id: SessionId,
    ) -> Result<ServerSessionRetirement, RuntimeError> {
        self.backend.session_retirement(session_id)
    }

    pub(in crate::runtime) fn retire_session(
        &self,
        session_id: SessionId,
        reason: CloseReason,
    ) -> CloseReason {
        self.backend.retire_session(session_id, reason)
    }

    pub(in crate::runtime) async fn open_or_attach(
        &self,
        request: ServerStreamOpenRequest,
    ) -> Result<ServerStreamOpenOutcome, RuntimeError> {
        self.open_with_policy(request, ServerNewStreamPolicy::Submit)
            .await
    }

    pub(in crate::runtime) async fn attach_existing(
        &self,
        request: ServerStreamOpenRequest,
    ) -> Result<ServerStreamOpenOutcome, RuntimeError> {
        self.open_with_policy(request, ServerNewStreamPolicy::Reject)
            .await
    }

    async fn open_with_policy(
        &self,
        request: ServerStreamOpenRequest,
        new_stream_policy: ServerNewStreamPolicy,
    ) -> Result<ServerStreamOpenOutcome, RuntimeError> {
        if !request.attachment.path_registration.belongs_to(self)
            || request.attachment.path_registration.session_id() != request.session_id
        {
            return Err(RuntimeError::Protocol(
                "reliable path registration does not match stream service or session",
            ));
        }
        // The observed QUIC peer can migrate. Snapshot once at logical OPEN so
        // preflight policy, debug logs, and accepted-flow telemetry all use
        // one causal opening identity rather than separate observer reads.
        let opening_ingress = request.attachment.path_registration.mpp_ingress();
        match self.validate_target_with_ingress(
            &request.attachment.path_registration,
            &request.target,
            opening_ingress.as_ref(),
        )? {
            ServerTargetAdmission::Allow => {}
            ServerTargetAdmission::Reject => return Ok(ServerStreamOpenOutcome::Rejected),
            ServerTargetAdmission::Drop => return Ok(ServerStreamOpenOutcome::Dropped),
        }
        self.backend
            .open_or_attach(request, new_stream_policy, opening_ingress)
            .await
    }

    pub(in crate::runtime) async fn route_frame(
        &self,
        path_registration: &ServerCarrierPathRegistration,
        stream_id: StreamId,
        frame: Frame,
    ) -> Result<(), RuntimeError> {
        if !path_registration.belongs_to(self) {
            return Err(RuntimeError::Protocol(
                "reliable path registration does not match stream service",
            ));
        }
        self.backend
            .route_frame(path_registration.inner.identity, stream_id, frame)
            .await
    }

    pub(in crate::runtime) fn try_route_frame(
        &self,
        path_registration: &ServerCarrierPathRegistration,
        stream_id: StreamId,
        frame: Frame,
    ) -> Result<ServerStreamFrameRoute, RuntimeError> {
        if !path_registration.belongs_to(self) {
            return Err(RuntimeError::Protocol(
                "reliable path registration does not match stream service",
            ));
        }
        self.backend
            .try_route_frame(path_registration.inner.identity, stream_id, frame)
    }

    pub(in crate::runtime) fn detach_path(
        &self,
        path_registration: &ServerCarrierPathRegistration,
        stream_id: StreamId,
    ) -> Result<(), RuntimeError> {
        if !path_registration.belongs_to(self) {
            return Err(RuntimeError::Protocol(
                "reliable path registration does not match stream service",
            ));
        }
        self.backend
            .detach_path(path_registration.inner.identity, stream_id)
    }

    pub(in crate::runtime) fn record_peer_path_metrics(
        &self,
        registration: &ServerCarrierPathRegistration,
        metrics: PathMetrics,
    ) {
        if registration.belongs_to(self) {
            self.backend
                .record_peer_path_metrics(registration.inner.identity, metrics);
        }
    }

    pub(in crate::runtime) fn record_peer_path_usage(
        &self,
        registration: &ServerCarrierPathRegistration,
        sequence: u64,
        usage: PathUsage,
    ) {
        if registration.belongs_to(self) {
            self.backend
                .record_peer_path_usage(registration.inner.identity, sequence, usage);
        }
    }

    pub(in crate::runtime) fn record_local_path_metrics(
        &self,
        registration: &ServerCarrierPathRegistration,
        metrics: PathMetrics,
        native_drain_observed: bool,
    ) {
        self.record_local_path_metrics_with_delivery_rate_sample(
            registration,
            metrics,
            native_drain_observed,
            None,
        );
    }

    pub(in crate::runtime) fn record_local_path_metrics_with_delivery_rate_sample(
        &self,
        registration: &ServerCarrierPathRegistration,
        metrics: PathMetrics,
        native_drain_observed: bool,
        delivery_rate_sample: Option<CarrierDeliveryRateSample>,
    ) {
        self.record_local_path_metrics_with_native_epoch(
            registration,
            metrics,
            native_drain_observed,
            None,
            delivery_rate_sample,
        );
    }

    pub(in crate::runtime) fn record_local_path_metrics_with_native_epoch(
        &self,
        registration: &ServerCarrierPathRegistration,
        metrics: PathMetrics,
        native_drain_observed: bool,
        native_capacity_epoch: Option<u64>,
        delivery_rate_sample: Option<CarrierDeliveryRateSample>,
    ) {
        let native_window_sample =
            CarrierNativeWindowSample::from_path_metrics_at(metrics, std::time::Instant::now());
        self.record_local_path_metrics_with_native_evidence(
            registration,
            metrics,
            native_drain_observed,
            native_capacity_epoch,
            native_window_sample,
            delivery_rate_sample,
        );
    }

    pub(in crate::runtime) fn record_local_path_metrics_with_native_evidence(
        &self,
        registration: &ServerCarrierPathRegistration,
        metrics: PathMetrics,
        native_drain_observed: bool,
        native_capacity_epoch: Option<u64>,
        native_window_sample: Option<CarrierNativeWindowSample>,
        delivery_rate_sample: Option<CarrierDeliveryRateSample>,
    ) {
        if registration.belongs_to(self) {
            self.backend.record_local_path_metrics(
                registration.inner.identity,
                metrics,
                native_drain_observed,
                native_capacity_epoch,
                native_window_sample,
                delivery_rate_sample,
            );
        }
    }

    pub(in crate::runtime) fn stage_native_scheduling_shape(
        &self,
        registration: &ServerCarrierPathRegistration,
        shape: NativeCarrierSchedulingShapeSnapshot,
    ) -> bool {
        if registration.belongs_to(self) {
            self.backend
                .stage_native_scheduling_shape(registration.inner.identity, shape)
        } else {
            false
        }
    }

    pub(in crate::runtime) fn fanout_native_scheduling_shape(
        &self,
        registration: &ServerCarrierPathRegistration,
        shape: NativeCarrierSchedulingShapeSnapshot,
    ) {
        if registration.belongs_to(self) {
            self.backend
                .fanout_native_scheduling_shape(registration.inner.identity, shape);
        }
    }

    pub(in crate::runtime) fn record_path_proof_success(
        &self,
        registration: &ServerCarrierPathRegistration,
        observation: PathProofObservation,
    ) {
        if registration.belongs_to(self) {
            registration.mark_path_validated();
            self.backend
                .record_path_proof_success(registration.inner.identity, observation);
        }
    }

    pub(in crate::runtime) fn peer_status_snapshot(
        &self,
        session_id: SessionId,
    ) -> Vec<PeerPathStatus> {
        self.backend.peer_status_snapshot(session_id)
    }

    pub(in crate::runtime) fn carrier_path_statuses(
        &self,
        identities: &[ServerCarrierPathIdentity],
    ) -> Vec<Option<ServerCarrierPathStatusSnapshot>> {
        self.backend.carrier_path_statuses(identities)
    }

    pub(in crate::runtime) fn management_snapshot(&self) -> ServerStreamManagementSnapshot {
        self.backend.management_snapshot()
    }
}

/// Accepted client carrier state before product-stream ownership begins.
pub(in crate::runtime) struct OpenedReliableCarrierStream {
    pub(in crate::runtime) stream_id: StreamId,
    pub(in crate::runtime) path_instance_id: CarrierPathInstanceId,
    pub(in crate::runtime) max_offset: u64,
    pub(in crate::runtime) lane: TrafficClass,
    pub(in crate::runtime) underlay: UnderlayProtocol,
    pub(in crate::runtime) max_frame_payload_bytes: usize,
    /// Non-evidentiary configured/startup prior retained when every measured
    /// attachment epoch has expired.
    pub(in crate::runtime) portable_startup: PathSnapshot,
    pub(in crate::runtime) startup: PathSnapshot,
    /// Native window captured with the attachment's carrier observation.
    /// Its lifetime is independent from `startup_metrics` delivery-rate age.
    pub(in crate::runtime) startup_native_window: Option<CarrierNativeWindowSample>,
    /// Attachment-time carrier metrics retain the immutable native evidence
    /// lifetime. Fixed Product outputs must not turn their scalar snapshot into
    /// permanent rate or congestion-window authority.
    pub(in crate::runtime) startup_metrics: Option<PathMetrics>,
    pub(in crate::runtime) commands: ReliablePathCommandSender,
    pub(in crate::runtime) mux_limits: MuxLimits,
    pub(in crate::runtime) frames: mpsc::Receiver<Result<Frame, RuntimeError>>,
}

impl OpenedReliableCarrierStream {
    /// Retires a peer-accepted client stream that lost exact carrier ownership
    /// before Product attachment commit.
    pub(in crate::runtime) fn retire_uncommitted(self) -> Result<(), RuntimeError> {
        self.commands.retire_accepted_stream(self.stream_id)
    }
}

#[cfg(test)]
mod session_retirement_tests {
    use super::*;

    #[tokio::test]
    async fn sticky_session_retirement_wins_even_when_readiness_is_immediately_ready() {
        let (_reason, retirement) = watch::channel(Some(CloseReason::PolicyRejected));
        let readiness_polled = Arc::new(AtomicBool::new(false));
        let observed = readiness_polled.clone();

        let result = fence_server_carrier_readiness(
            ServerSessionRetirement::pending(retirement),
            async move {
                observed.store(true, Ordering::Release);
                Ok::<(), RuntimeError>(())
            },
        )
        .await;

        assert!(matches!(
            result,
            Err(RuntimeError::RemoteClosed(CloseReason::PolicyRejected))
        ));
        assert!(
            !readiness_polled.load(Ordering::Acquire),
            "the biased terminal fence must win before readiness is polled",
        );
    }
}
