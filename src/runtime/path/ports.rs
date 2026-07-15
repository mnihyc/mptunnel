//! Contracts crossing between carrier paths and product-stream ownership.
//!
//! Carriers publish accepted transport state here without constructing stream
//! policy objects. The stream layer consumes these values and owns offsets,
//! repair, and attachment behavior.

use super::commands::ReliablePathCommandSender;
use super::tcp::capacity::TcpCapacityProofCandidate;
use crate::model::capacity::QuicCapacityProofCandidate;
use crate::model::path::{CarrierPathInstanceId, next_carrier_path_instance_id};
use crate::mux::MuxLimits;
use crate::protocol::{
    Frame, PathId, PathMetrics, SessionId, StreamId, StreamOpenRole, TargetAddr, UnderlayProtocol,
};
use crate::runtime::error::RuntimeError;
use crate::scheduler::{FlowLane, PathSnapshot};
use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Keeps a higher-layer reservation alive for exactly one queued carrier command.
///
/// The carrier only owns the lifetime contract; the reservation's policy and
/// release behavior remain in the layer that created the guard.
pub(in crate::runtime) struct CarrierCommandLease {
    _guard: Box<dyn Send + Sync>,
}

impl CarrierCommandLease {
    pub(in crate::runtime) fn hold<T>(guard: T) -> Self
    where
        T: Send + Sync + 'static,
    {
        Self {
            _guard: Box::new(guard),
        }
    }
}

impl std::fmt::Debug for CarrierCommandLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CarrierCommandLease")
            .finish_non_exhaustive()
    }
}

/// Keeps response-lane accounting attached to one target-side datagram flow.
pub(in crate::runtime) struct ServerRealtimeFlowLease {
    _guard: Box<dyn Send + Sync>,
}

impl ServerRealtimeFlowLease {
    pub(in crate::runtime) fn hold<T>(guard: T) -> Self
    where
        T: Send + Sync + 'static,
    {
        Self {
            _guard: Box::new(guard),
        }
    }
}

/// One target-bound datagram handed from a carrier to the product worker.
pub(in crate::runtime) struct ServerDatagramRequest {
    pub(in crate::runtime) datagram_id: crate::protocol::DatagramId,
    pub(in crate::runtime) ttl_ms: u32,
    pub(in crate::runtime) payload: Bytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum ServerDatagramSendOutcome {
    Accepted,
    Full,
    Closed,
}

/// Accepted target worker plus the higher-layer accounting it keeps alive.
pub(in crate::runtime) struct AcceptedServerDatagramFlow {
    flow_id: crate::protocol::DatagramFlowId,
    requests: mpsc::Sender<ServerDatagramRequest>,
    _realtime_registration: ServerRealtimeFlowLease,
}

impl AcceptedServerDatagramFlow {
    pub(in crate::runtime) fn holding(
        flow_id: crate::protocol::DatagramFlowId,
        requests: mpsc::Sender<ServerDatagramRequest>,
        realtime_registration: ServerRealtimeFlowLease,
    ) -> Self {
        Self {
            flow_id,
            requests,
            _realtime_registration: realtime_registration,
        }
    }

    pub(in crate::runtime) fn flow_id(&self) -> crate::protocol::DatagramFlowId {
        self.flow_id
    }

    pub(in crate::runtime) fn try_send(
        &self,
        request: ServerDatagramRequest,
    ) -> ServerDatagramSendOutcome {
        match self.requests.try_send(request) {
            Ok(()) => ServerDatagramSendOutcome::Accepted,
            Err(mpsc::error::TrySendError::Full(_)) => ServerDatagramSendOutcome::Full,
            Err(mpsc::error::TrySendError::Closed(_)) => ServerDatagramSendOutcome::Closed,
        }
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
    error: RuntimeError,
    _realtime_registration: Option<ServerRealtimeFlowLease>,
}

impl ServerDatagramOpenError {
    pub(in crate::runtime) fn new(error: RuntimeError) -> Self {
        Self {
            error,
            _realtime_registration: None,
        }
    }

    pub(in crate::runtime) fn holding(
        error: RuntimeError,
        realtime_registration: ServerRealtimeFlowLease,
    ) -> Self {
        Self {
            error,
            _realtime_registration: Some(realtime_registration),
        }
    }

    pub(in crate::runtime) fn into_error(self) -> RuntimeError {
        self.error
    }

    /// A registered flow must publish its close before releasing accounting.
    pub(in crate::runtime) fn requires_close(&self) -> bool {
        self._realtime_registration.is_some()
    }
}

impl std::fmt::Debug for ServerDatagramOpenError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ServerDatagramOpenError")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

pub(in crate::runtime) struct ServerDatagramOpenRequest {
    pub(in crate::runtime) session_id: SessionId,
    pub(in crate::runtime) flow_id: crate::protocol::DatagramFlowId,
    pub(in crate::runtime) target: TargetAddr,
    pub(in crate::runtime) commands: ReliablePathCommandSender,
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
}

impl std::fmt::Debug for ServerDatagramPort {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ServerDatagramPort")
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ServerCarrierPathMetricSnapshot {
    pub(in crate::runtime) session_id: SessionId,
    pub(in crate::runtime) underlay: UnderlayProtocol,
    pub(in crate::runtime) path_id: PathId,
    pub(in crate::runtime) metrics: PathMetrics,
    pub(in crate::runtime) source: &'static str,
}

#[derive(Debug)]
pub(in crate::runtime) struct ServerStreamManagementSnapshot {
    pub(in crate::runtime) active_streams: usize,
    pub(in crate::runtime) path_metrics: Vec<ServerCarrierPathMetricSnapshot>,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ServerCarrierPathIdentity {
    pub(in crate::runtime) session_id: SessionId,
    pub(in crate::runtime) underlay: UnderlayProtocol,
    pub(in crate::runtime) path_id: PathId,
    pub(in crate::runtime) path_instance_id: CarrierPathInstanceId,
}

#[derive(Clone)]
pub(in crate::runtime) struct ServerCarrierPathRegistration {
    inner: Arc<ServerCarrierPathRegistrationInner>,
}

struct ServerCarrierPathRegistrationInner {
    backend: Arc<dyn ServerStreamPortBackend>,
    owner_token: usize,
    identity: ServerCarrierPathIdentity,
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

    fn belongs_to(&self, port: &ServerStreamPort) -> bool {
        self.inner.owner_token == port.owner_token
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
            .finish()
    }
}

impl Drop for ServerCarrierPathRegistrationInner {
    fn drop(&mut self) {
        self.backend.retire_carrier_path(self.identity);
    }
}

pub(in crate::runtime) struct ServerStreamPathAttachment {
    pub(in crate::runtime) path_registration: ServerCarrierPathRegistration,
    pub(in crate::runtime) commands: ReliablePathCommandSender,
    pub(in crate::runtime) max_frame_payload_bytes: usize,
    pub(in crate::runtime) role: StreamOpenRole,
    pub(in crate::runtime) initial_metrics: Option<PathMetrics>,
}

/// Carrier request to create or join one server-side product stream.
pub(in crate::runtime) struct ServerStreamOpenRequest {
    pub(in crate::runtime) session_id: SessionId,
    pub(in crate::runtime) stream_id: StreamId,
    pub(in crate::runtime) target: TargetAddr,
    pub(in crate::runtime) lane: FlowLane,
    pub(in crate::runtime) attachment: ServerStreamPathAttachment,
    pub(in crate::runtime) mux_limits: MuxLimits,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum ServerStreamOpenOutcome {
    New,
    Existing,
    DuplicateLiveIgnored,
    Rejected,
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

    fn activate_carrier_path(&self, identity: ServerCarrierPathIdentity);

    fn retire_carrier_path(&self, identity: ServerCarrierPathIdentity);

    fn register_realtime_flow(&self, session_id: SessionId) -> ServerRealtimeFlowLease;

    fn open_or_attach<'a>(
        &'a self,
        request: ServerStreamOpenRequest,
        new_stream_policy: ServerNewStreamPolicy,
    ) -> ServerStreamPortFuture<'a, ServerStreamOpenOutcome>;

    fn route_frame<'a>(
        &'a self,
        session_id: SessionId,
        stream_id: StreamId,
        frame: Frame,
    ) -> ServerStreamPortFuture<'a, ()>;

    fn detach_path(
        &self,
        session_id: SessionId,
        stream_id: StreamId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: &ReliablePathCommandSender,
    );

    fn record_peer_path_metrics(&self, identity: ServerCarrierPathIdentity, metrics: PathMetrics);

    fn record_local_path_metrics(&self, identity: ServerCarrierPathIdentity, metrics: PathMetrics);

    fn record_local_quic_capacity_proof(
        &self,
        identity: ServerCarrierPathIdentity,
        metrics: PathMetrics,
        candidate: QuicCapacityProofCandidate,
    ) -> bool;

    fn record_local_tcp_capacity_proof(
        &self,
        identity: ServerCarrierPathIdentity,
        metrics: PathMetrics,
        candidate: TcpCapacityProofCandidate,
    ) -> bool;

    fn management_snapshot(&self) -> ServerStreamManagementSnapshot;
}

#[derive(Clone)]
pub(in crate::runtime) struct ServerStreamPort {
    backend: Arc<dyn ServerStreamPortBackend>,
    owner_token: usize,
    target_admission: Arc<ServerStreamTargetAdmission>,
}

pub(in crate::runtime) type ServerStreamTargetAdmission =
    dyn Fn(&TargetAddr) -> Result<(), RuntimeError> + Send + Sync;

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
            target_admission: Arc::new(|_| Ok(())),
        }
    }

    /// Installs composition-owned target policy without exposing it to carriers.
    pub(in crate::runtime) fn with_target_admission(
        mut self,
        target_admission: Arc<ServerStreamTargetAdmission>,
    ) -> Self {
        self.target_admission = target_admission;
        self
    }

    pub(in crate::runtime) fn validate_target(
        &self,
        target: &TargetAddr,
    ) -> Result<(), RuntimeError> {
        (self.target_admission)(target)
    }

    pub(in crate::runtime) fn register_carrier_path(
        &self,
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
    ) -> ServerCarrierPathRegistration {
        let identity = ServerCarrierPathIdentity {
            session_id,
            underlay,
            path_id,
            path_instance_id: next_carrier_path_instance_id(),
        };
        self.backend.activate_carrier_path(identity);
        ServerCarrierPathRegistration {
            inner: Arc::new(ServerCarrierPathRegistrationInner {
                backend: self.backend.clone(),
                owner_token: self.owner_token,
                identity,
            }),
        }
    }

    pub(in crate::runtime) fn register_realtime_flow(
        &self,
        session_id: SessionId,
    ) -> ServerRealtimeFlowLease {
        self.backend.register_realtime_flow(session_id)
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
        self.backend
            .open_or_attach(request, new_stream_policy)
            .await
    }

    pub(in crate::runtime) async fn route_frame(
        &self,
        session_id: SessionId,
        stream_id: StreamId,
        frame: Frame,
    ) -> Result<(), RuntimeError> {
        self.backend.route_frame(session_id, stream_id, frame).await
    }

    pub(in crate::runtime) fn detach_path(
        &self,
        session_id: SessionId,
        stream_id: StreamId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: &ReliablePathCommandSender,
    ) {
        self.backend
            .detach_path(session_id, stream_id, underlay, path_id, commands);
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

    pub(in crate::runtime) fn record_local_path_metrics(
        &self,
        registration: &ServerCarrierPathRegistration,
        metrics: PathMetrics,
    ) {
        if registration.belongs_to(self) {
            self.backend
                .record_local_path_metrics(registration.inner.identity, metrics);
        }
    }

    pub(in crate::runtime) fn record_local_quic_capacity_proof(
        &self,
        registration: &ServerCarrierPathRegistration,
        metrics: PathMetrics,
        candidate: QuicCapacityProofCandidate,
    ) -> bool {
        registration.belongs_to(self)
            && self.backend.record_local_quic_capacity_proof(
                registration.inner.identity,
                metrics,
                candidate,
            )
    }

    pub(in crate::runtime) fn record_local_tcp_capacity_proof(
        &self,
        registration: &ServerCarrierPathRegistration,
        metrics: PathMetrics,
        candidate: TcpCapacityProofCandidate,
    ) -> bool {
        registration.belongs_to(self)
            && self.backend.record_local_tcp_capacity_proof(
                registration.inner.identity,
                metrics,
                candidate,
            )
    }

    pub(in crate::runtime) fn management_snapshot(&self) -> ServerStreamManagementSnapshot {
        self.backend.management_snapshot()
    }
}

/// Accepted client carrier state before product-stream ownership begins.
pub(in crate::runtime) struct OpenedReliableCarrierStream {
    pub(in crate::runtime) stream_id: StreamId,
    pub(in crate::runtime) max_offset: u64,
    pub(in crate::runtime) lane: FlowLane,
    pub(in crate::runtime) underlay: UnderlayProtocol,
    pub(in crate::runtime) max_frame_payload_bytes: usize,
    pub(in crate::runtime) startup: PathSnapshot,
    pub(in crate::runtime) commands: ReliablePathCommandSender,
    pub(in crate::runtime) mux_limits: MuxLimits,
    pub(in crate::runtime) frames: mpsc::Receiver<Result<Frame, RuntimeError>>,
}

/// QUIC-specific wire-open behavior selected by the relay open transaction.
#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct UdpStreamOpenOptions {
    pub(in crate::runtime) wait_for_accept: bool,
    pub(in crate::runtime) role: StreamOpenRole,
}

impl UdpStreamOpenOptions {
    pub(in crate::runtime) const ACTIVE_WAIT: Self = Self {
        wait_for_accept: true,
        role: StreamOpenRole::Active,
    };
}
