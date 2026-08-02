//! Contracts crossing between carrier paths and product-stream ownership.
//!
//! Carriers publish accepted transport state here without constructing stream
//! policy objects. The stream layer consumes these values and owns offsets,
//! reinjection, and attachment behavior.

use super::commands::ReliablePathCommandSender;
use super::tcp::server::service::ServerTcpCarrierDemandSubscription;
use crate::model::path::{CarrierPathInstanceId, PathPolicy, next_carrier_path_instance_id};
use crate::mux::MuxLimits;
use crate::product::PrincipalPermit;
use crate::protocol::{
    Frame, OffsetRange, PathId, PathMetricDirection, PathMetrics, PathPurpose, PathUsage,
    PeerPathState, PeerPathStatus, SessionId, StreamId, TargetAddr, UnderlayProtocol,
};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::proof::{PathProofObservation, allocated_path_proof_data_frame};
use crate::runtime::stream::response::ServerTcpValidationOutput;
use crate::scheduler::{PathSnapshot, TrafficClass};
use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use tokio::sync::{mpsc, oneshot, watch};

/// A positive-ACK, non-application-limited delivery sample for one carrier.
///
/// Product ACK evidence remains stream-owned. This sample only describes the
/// transport capacity observed by the exact carrier instance that published it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct CarrierDeliveryRateSample {
    pub(in crate::runtime) delivery_rate_bps: u64,
    pub(in crate::runtime) sample_count: u32,
    pub(in crate::runtime) sample_bytes: u64,
    pub(in crate::runtime) delivery_window_covered: bool,
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

/// Accepted target worker plus the higher-layer accounting it keeps alive.
pub(in crate::runtime) struct AcceptedServerDatagramFlow {
    flow_id: crate::protocol::DatagramFlowId,
    requests: mpsc::Sender<ServerDatagramWorkerMessage>,
    commands: ReliablePathCommandSender,
    route_lifetime: Arc<()>,
    _attachment: Box<dyn Send + Sync>,
}

impl AcceptedServerDatagramFlow {
    pub(in crate::runtime) fn holding(
        flow_id: crate::protocol::DatagramFlowId,
        requests: mpsc::Sender<ServerDatagramWorkerMessage>,
        commands: ReliablePathCommandSender,
        route_lifetime: Arc<()>,
        attachment: impl Send + Sync + 'static,
    ) -> Self {
        Self {
            flow_id,
            requests,
            commands,
            route_lifetime,
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
    error: RuntimeError,
}

impl ServerDatagramOpenError {
    pub(in crate::runtime) fn new(error: RuntimeError) -> Self {
        Self { error }
    }

    pub(in crate::runtime) fn into_error(self) -> RuntimeError {
        self.error
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
    pub(in crate::runtime) principal_permit: PrincipalPermit,
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
    pub(in crate::runtime) source: Option<&'static str>,
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
#[derive(Debug, Clone, Copy, Default)]
pub(in crate::runtime) struct ServerLocalPathProperties {
    pub(in crate::runtime) config_ordinal: usize,
    pub(in crate::runtime) policy: PathPolicy,
    pub(in crate::runtime) initial_metrics: Option<PathMetrics>,
}

#[derive(Clone)]
pub(in crate::runtime) struct ServerCarrierPathRegistration {
    inner: Arc<ServerCarrierPathRegistrationInner>,
}

/// Exact session-owned authority for one directional TCP-carrier validation.
///
/// The lease is deliberately non-cloneable. Dropping it withdraws only the
/// exact active transaction; retaining consumes it and commits authority in
/// the same neutral registry transaction that releases the session slot.
pub(in crate::runtime) struct ServerTcpCarrierValidationLease {
    backend: Arc<dyn ServerStreamPortBackend>,
    identity: ServerCarrierPathIdentity,
    direction: PathMetricDirection,
    lease_id: u64,
    active: bool,
}

/// Completion authority for one exact carrier's ordered attachment retirement.
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
    purpose: PathPurpose,
    local: ServerLocalPathProperties,
    principal_permit: PrincipalPermit,
    validation: Arc<AtomicBool>,
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

    pub(in crate::runtime) fn purpose(&self) -> PathPurpose {
        self.inner.purpose
    }

    pub(in crate::runtime) fn local_policy(&self) -> PathPolicy {
        self.inner.local.policy
    }

    /// Directional usage advertised by this receiver for peer Product work.
    /// Validation admission and RETAIN commitment recheck this exact local
    /// value instead of trusting that the peer honored the readiness frame.
    pub(in crate::runtime) fn local_usage(&self) -> PathUsage {
        if self.local_policy().backup {
            PathUsage::Backup
        } else {
            PathUsage::Available
        }
    }

    pub(in crate::runtime) fn local_config_ordinal(&self) -> usize {
        self.inner.local.config_ordinal
    }

    pub(in crate::runtime) fn initial_metrics(&self) -> Option<PathMetrics> {
        self.inner.local.initial_metrics
    }

    pub(in crate::runtime) fn principal_permit(&self) -> &PrincipalPermit {
        &self.inner.principal_permit
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

    pub(in crate::runtime) fn begin_retirement(&self) -> ServerCarrierPathRetirement {
        self.inner.backend.retire_carrier_path(self.inner.identity)
    }

    /// Reserves the one active directional validation slot for this session.
    /// Ordinary carriers never acquire this authority.
    pub(in crate::runtime) fn begin_tcp_carrier_validation(
        &self,
        direction: PathMetricDirection,
    ) -> Result<ServerTcpCarrierValidationLease, RuntimeError> {
        if self.purpose() != PathPurpose::Validation {
            return Err(RuntimeError::Protocol(
                "ordinary carrier cannot begin TCP carrier validation",
            ));
        }
        let lease_id = self
            .inner
            .backend
            .begin_tcp_carrier_validation(self.inner.identity, direction)?;
        Ok(ServerTcpCarrierValidationLease {
            backend: self.inner.backend.clone(),
            identity: self.inner.identity,
            direction,
            lease_id,
            active: true,
        })
    }

    pub(in crate::runtime) fn tcp_carrier_direction_authorized(
        &self,
        direction: PathMetricDirection,
    ) -> bool {
        self.inner
            .backend
            .tcp_carrier_direction_authorized(self.inner.identity, direction)
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
            .field("purpose", &self.purpose())
            .field("local_config_ordinal", &self.local_config_ordinal())
            .finish()
    }
}

impl ServerTcpCarrierValidationLease {
    pub(in crate::runtime) fn path_instance_id(&self) -> CarrierPathInstanceId {
        self.identity.path_instance_id
    }

    pub(in crate::runtime) fn direction(&self) -> PathMetricDirection {
        self.direction
    }

    /// Settles a complete negative or withdrawn validation. An unretained
    /// carrier keeps its admission reservation until exact retirement.
    pub(in crate::runtime) fn settle_without_retain(mut self) -> Result<(), RuntimeError> {
        self.backend.finish_tcp_carrier_validation(
            self.identity,
            self.direction,
            self.lease_id,
            false,
        )?;
        self.active = false;
        Ok(())
    }

    /// Commits receiver-side directional authority at RETAIN acknowledgment
    /// serialization. Authority is carrier state, not stream-binding state.
    pub(in crate::runtime) fn commit_retain(mut self) -> Result<(), RuntimeError> {
        self.backend.finish_tcp_carrier_validation(
            self.identity,
            self.direction,
            self.lease_id,
            true,
        )?;
        self.active = false;
        Ok(())
    }
}

impl std::fmt::Debug for ServerTcpCarrierValidationLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ServerTcpCarrierValidationLease")
            .field("session_id", &self.identity.session_id)
            .field("path_instance_id", &self.identity.path_instance_id)
            .field("direction", &self.direction)
            .field("active", &self.active)
            .finish()
    }
}

impl Drop for ServerTcpCarrierValidationLease {
    fn drop(&mut self) {
        if self.active {
            self.backend.abandon_tcp_carrier_validation(
                self.identity,
                self.direction,
                self.lease_id,
            );
        }
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
    pub(in crate::runtime) lane: TrafficClass,
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

pub(in crate::runtime) enum ServerStreamFrameRoute {
    Routed,
    Backpressured(Frame),
}

/// Exact receive-side binding used while one existing throughput stream is
/// validating a carrier. It deliberately owns no response-output command
/// channel, so creating it cannot publish ordinary server-to-client authority.
#[derive(Clone)]
pub(in crate::runtime) struct ServerValidationStreamBinding {
    inner: Arc<dyn ServerValidationStreamBindingBackend>,
}

impl std::fmt::Debug for ServerValidationStreamBinding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ServerValidationStreamBinding")
            .field("session_id", &self.session_id())
            .field("stream_id", &self.stream_id())
            .field("path_instance_id", &self.path_instance_id())
            .finish()
    }
}

impl ServerValidationStreamBinding {
    pub(in crate::runtime) fn new(inner: Arc<dyn ServerValidationStreamBindingBackend>) -> Self {
        Self { inner }
    }

    pub(in crate::runtime) fn session_id(&self) -> SessionId {
        self.inner.session_id()
    }

    pub(in crate::runtime) fn stream_id(&self) -> StreamId {
        self.inner.stream_id()
    }

    pub(in crate::runtime) fn path_instance_id(&self) -> CarrierPathInstanceId {
        self.inner.path_instance_id()
    }

    /// Revalidates both the exact physical carrier and exact Product-stream
    /// lifetime. A lane change ends throughput-validation eligibility.
    pub(in crate::runtime) fn is_current(&self) -> bool {
        self.inner.is_current()
    }

    pub(in crate::runtime) async fn route_frame(&self, frame: Frame) -> Result<(), RuntimeError> {
        validate_validation_stream_frame(self.stream_id(), &frame)?;
        self.inner.route_frame(frame).await
    }

    pub(in crate::runtime) fn try_route_frame(
        &self,
        frame: Frame,
    ) -> Result<ServerStreamFrameRoute, RuntimeError> {
        validate_validation_stream_frame(self.stream_id(), &frame)?;
        self.inner.try_route_frame(frame)
    }

    /// Stops new input on this exact attachment and orders its completion
    /// after every frame already accepted into the Product stream actor.
    pub(in crate::runtime) fn begin_detach(&self) {
        self.inner.begin_detach();
    }
}

fn validate_validation_stream_frame(
    stream_id: StreamId,
    frame: &Frame,
) -> Result<(), RuntimeError> {
    let frame_stream_id = match frame {
        Frame::StreamData { stream_id, .. }
        | Frame::StreamAck { stream_id, .. }
        | Frame::StreamMaxData { stream_id, .. }
        | Frame::StreamFin { stream_id, .. }
        | Frame::StreamReset { stream_id, .. }
        | Frame::StreamDetach { stream_id } => *stream_id,
        _ => {
            return Err(RuntimeError::Protocol(
                "validation stream binding received non-stream frame",
            ));
        }
    };
    if frame_stream_id != stream_id {
        return Err(RuntimeError::Protocol(
            "validation stream binding frame stream mismatch",
        ));
    }
    Ok(())
}

pub(in crate::runtime) trait ServerValidationStreamBindingBackend:
    Send + Sync
{
    fn session_id(&self) -> SessionId;
    fn stream_id(&self) -> StreamId;
    fn path_instance_id(&self) -> CarrierPathInstanceId;
    fn is_current(&self) -> bool;
    fn route_frame<'a>(&'a self, frame: Frame) -> ServerStreamPortFuture<'a, ()>;
    fn try_route_frame(&self, frame: Frame) -> Result<ServerStreamFrameRoute, RuntimeError>;
    fn begin_detach(&self);
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
        purpose: PathPurpose,
        local: ServerLocalPathProperties,
        principal_permit: PrincipalPermit,
    ) -> Result<(), RuntimeError>;

    fn begin_tcp_carrier_validation(
        &self,
        identity: ServerCarrierPathIdentity,
        direction: PathMetricDirection,
    ) -> Result<u64, RuntimeError>;

    fn finish_tcp_carrier_validation(
        &self,
        identity: ServerCarrierPathIdentity,
        direction: PathMetricDirection,
        lease_id: u64,
        retain: bool,
    ) -> Result<(), RuntimeError>;

    fn abandon_tcp_carrier_validation(
        &self,
        identity: ServerCarrierPathIdentity,
        direction: PathMetricDirection,
        lease_id: u64,
    );

    fn tcp_carrier_direction_authorized(
        &self,
        identity: ServerCarrierPathIdentity,
        direction: PathMetricDirection,
    ) -> bool;

    fn subscribe_tcp_carrier_demands(
        &self,
        identity: ServerCarrierPathIdentity,
    ) -> Result<ServerTcpCarrierDemandSubscription, RuntimeError>;

    fn retire_carrier_path(
        &self,
        identity: ServerCarrierPathIdentity,
    ) -> ServerCarrierPathRetirement;

    fn set_carrier_path_state(&self, identity: ServerCarrierPathIdentity, state: PeerPathState);

    fn register_realtime_flow(&self, session_id: SessionId) -> ServerRealtimeFlowLease;

    fn open_or_attach<'a>(
        &'a self,
        request: ServerStreamOpenRequest,
        new_stream_policy: ServerNewStreamPolicy,
    ) -> ServerStreamPortFuture<'a, ServerStreamOpenOutcome>;

    fn bind_validation_input_existing(
        &self,
        identity: ServerCarrierPathIdentity,
        stream_id: StreamId,
    ) -> Result<Option<ServerValidationStreamBinding>, RuntimeError>;

    fn bind_validation_output_existing(
        &self,
        identity: ServerCarrierPathIdentity,
        stream_id: StreamId,
        commands: ReliablePathCommandSender,
    ) -> Result<Option<ServerTcpValidationOutput>, RuntimeError>;

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
        delivery_rate_sample: Option<CarrierDeliveryRateSample>,
    );

    fn record_path_proof_success(
        &self,
        identity: ServerCarrierPathIdentity,
        observation: PathProofObservation,
    );

    fn peer_status_snapshot(&self, session_id: SessionId) -> Vec<PeerPathStatus>;

    fn management_snapshot(&self) -> ServerStreamManagementSnapshot;
}

#[derive(Clone)]
pub(in crate::runtime) struct ServerStreamPort {
    backend: Arc<dyn ServerStreamPortBackend>,
    owner_token: usize,
    target_admission: Arc<ServerStreamTargetAdmission>,
}

pub(in crate::runtime) type ServerStreamTargetAdmission =
    dyn Fn(&PrincipalPermit, &TargetAddr) -> Result<(), RuntimeError> + Send + Sync;

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
            target_admission: Arc::new(|_, _| Ok(())),
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
        path_registration: &ServerCarrierPathRegistration,
        target: &TargetAddr,
    ) -> Result<(), RuntimeError> {
        if !path_registration.belongs_to(self) {
            return Err(RuntimeError::Protocol(
                "reliable path registration does not match stream service",
            ));
        }
        (self.target_admission)(path_registration.principal_permit(), target)
    }

    pub(in crate::runtime) fn register_carrier_path(
        &self,
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        local: ServerLocalPathProperties,
        principal_permit: PrincipalPermit,
    ) -> Result<ServerCarrierPathRegistration, RuntimeError> {
        self.register_carrier_path_with_purpose(
            session_id,
            underlay,
            path_id,
            PathPurpose::Ordinary,
            local,
            principal_permit,
        )
    }

    /// Registers one validation-purpose TCP carrier and atomically reserves
    /// the session's sole unretained-candidate slot.
    pub(in crate::runtime) fn register_validation_carrier_path(
        &self,
        session_id: SessionId,
        path_id: PathId,
        local: ServerLocalPathProperties,
        principal_permit: PrincipalPermit,
    ) -> Result<ServerCarrierPathRegistration, RuntimeError> {
        self.register_carrier_path_with_purpose(
            session_id,
            UnderlayProtocol::Tcp,
            path_id,
            PathPurpose::Validation,
            local,
            principal_permit,
        )
    }

    fn register_carrier_path_with_purpose(
        &self,
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        purpose: PathPurpose,
        local: ServerLocalPathProperties,
        principal_permit: PrincipalPermit,
    ) -> Result<ServerCarrierPathRegistration, RuntimeError> {
        if purpose == PathPurpose::Validation && underlay != UnderlayProtocol::Tcp {
            return Err(RuntimeError::Protocol(
                "validation-purpose carrier requires TCP underlay",
            ));
        }
        let identity = ServerCarrierPathIdentity {
            session_id,
            underlay,
            path_id,
            path_instance_id: next_carrier_path_instance_id(),
        };
        self.backend
            .activate_carrier_path(identity, purpose, local, principal_permit.clone())?;
        Ok(ServerCarrierPathRegistration {
            inner: Arc::new(ServerCarrierPathRegistrationInner {
                backend: self.backend.clone(),
                owner_token: self.owner_token,
                identity,
                purpose,
                local,
                principal_permit,
                validation: Arc::new(AtomicBool::new(false)),
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
        self.register_carrier_path(
            session_id,
            underlay,
            path_id,
            local,
            PrincipalPermit::for_test("test-peer"),
        )
        .expect("register test carrier path")
    }

    #[cfg(test)]
    pub(in crate::runtime) fn register_test_validation_carrier_path(
        &self,
        session_id: SessionId,
        path_id: PathId,
        local: ServerLocalPathProperties,
    ) -> Result<ServerCarrierPathRegistration, RuntimeError> {
        self.register_validation_carrier_path(
            session_id,
            path_id,
            local,
            PrincipalPermit::for_test("test-peer"),
        )
    }

    pub(in crate::runtime) fn register_realtime_flow(
        &self,
        session_id: SessionId,
    ) -> ServerRealtimeFlowLease {
        self.backend.register_realtime_flow(session_id)
    }

    pub(in crate::runtime) fn subscribe_tcp_carrier_demands(
        &self,
        registration: &ServerCarrierPathRegistration,
    ) -> Result<ServerTcpCarrierDemandSubscription, RuntimeError> {
        if !registration.belongs_to(self) {
            return Err(RuntimeError::Protocol(
                "reliable path registration does not match stream service",
            ));
        }
        self.backend
            .subscribe_tcp_carrier_demands(registration.inner.identity)
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

    /// Binds an exact live carrier to the receive side of one already-existing
    /// throughput stream. This neither creates a stream nor changes response
    /// output membership.
    pub(in crate::runtime) fn bind_validation_input_existing(
        &self,
        path_registration: &ServerCarrierPathRegistration,
        stream_id: StreamId,
    ) -> Result<Option<ServerValidationStreamBinding>, RuntimeError> {
        if !path_registration.belongs_to(self) {
            return Err(RuntimeError::Protocol(
                "reliable path registration does not match stream service",
            ));
        }
        self.backend
            .bind_validation_input_existing(path_registration.inner.identity, stream_id)
    }

    /// Binds one exact unpublished response output for S2C validation. It is
    /// deliberately absent from ordinary output membership until promotion.
    pub(in crate::runtime) fn bind_validation_output_existing(
        &self,
        path_registration: &ServerCarrierPathRegistration,
        stream_id: StreamId,
        commands: ReliablePathCommandSender,
    ) -> Result<Option<ServerTcpValidationOutput>, RuntimeError> {
        if !path_registration.belongs_to(self) {
            return Err(RuntimeError::Protocol(
                "reliable path registration does not match stream service",
            ));
        }
        self.backend.bind_validation_output_existing(
            path_registration.inner.identity,
            stream_id,
            commands,
        )
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
        if registration.belongs_to(self) {
            self.backend.record_local_path_metrics(
                registration.inner.identity,
                metrics,
                native_drain_observed,
                delivery_rate_sample,
            );
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
    pub(in crate::runtime) startup: PathSnapshot,
    pub(in crate::runtime) commands: ReliablePathCommandSender,
    pub(in crate::runtime) mux_limits: MuxLimits,
    pub(in crate::runtime) frames: mpsc::Receiver<Result<Frame, RuntimeError>>,
}
