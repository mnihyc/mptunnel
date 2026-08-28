//! Client QUIC path sessions and reliable stream lifecycle.

use super::client_stream::{apply_client_udp_path_status, run_client_udp_stream};
use super::estimator::UdpPathMetricTracker;
use super::io::{
    UdpPathConnection, UdpPathEndpoint, UdpPathRecvStream, UdpPathSendStream,
    spawn_quic_path_reader, udp_path_command_queue, udp_path_max_stream_payload_bytes,
    udp_path_read_frame, udp_path_write_frame, udp_reliable_stream_frame_queue,
    usable_udp_path_socket_addrs, warn_unexpected_udp_runtime_error,
};
use super::ip_tunnel::open_client_udp_ip_tunnel;
#[cfg(feature = "lab-diagnostics")]
use super::metrics::log_quic_ack_poll_diagnostics;
use super::metrics::{quic_path_metrics_ack_interval, quic_path_metrics_poll_interval};
use crate::config::ClientSecurityConfig;
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::path::{CarrierPathInstanceId, RelayPathKey, next_carrier_path_instance_id};
use crate::mux::MuxLimits;
use crate::protocol::codec::CodecLimits;
use crate::protocol::{
    Frame, PathId, PathMetricDirection, PathUsage, SessionId, StreamDemandHint, StreamId,
    TargetAddr, UnderlayProtocol,
};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::authentication::ClientPathAuthenticationFrames;
use crate::runtime::path::commands::reliable_path_command_channels;
use crate::runtime::path::model::path_startup_snapshot;
use crate::runtime::path::ports::OpenedReliableCarrierStream;
use crate::runtime::path::state::ClientPathState;
use crate::runtime::peer_status::{
    PeerStatusBroker, PeerStatusCarrier, PeerStatusPathMetadataHandle, PeerStatusSnapshotSource,
};
use crate::scheduler::TrafficClass;
use crate::transport::encrypted::TcpClientTlsConfig;
use crate::transport::quic::{QuicCandidateSelector, QuicCarrierError};
use crate::transport::{
    CarrierNetworkProvider, CarrierPathIdentity, CarrierResolutionRequest, CarrierSocketRequest,
    PathSpec, interleave_socket_addr_families,
};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
#[cfg(feature = "lab-diagnostics")]
use std::time::Instant;
use tokio::sync::Mutex as AsyncMutex;

// RFC 8305's default keeps a blackholed family from monopolizing setup without
// opening every resolver answer in one socket/TLS burst.
const QUIC_ADDRESS_ATTEMPT_DELAY: Duration = Duration::from_millis(250);
const MAX_QUIC_ADDRESS_ATTEMPTS: usize = 8;
pub(in crate::runtime) const MAX_CLIENT_UDP_EXACT_OPEN_ATTEMPTS: usize = 2;
use tokio::sync::mpsc;

/// Authority carried by an error observed after a client QUIC Product open
/// has selected an established connection.
///
/// Session shutdown is terminal for the authenticated MPP session. A
/// carrier-lifetime failure may retire only the exact physical owner and use
/// the released two-attempt reconnect budget. Operation failures retire only
/// the affected Product attachment and never reconnect the carrier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum ClientUdpErrorDisposition {
    Session,
    CarrierLifetime,
    Operation,
}

pub(in crate::runtime) fn client_udp_error_disposition(
    source: &RuntimeError,
) -> ClientUdpErrorDisposition {
    match source {
        RuntimeError::RemoteClosed(_) => ClientUdpErrorDisposition::Session,
        RuntimeError::QuicCarrier(error)
            if quic_product_error_has_carrier_lifetime_authority(error) =>
        {
            ClientUdpErrorDisposition::CarrierLifetime
        }
        _ => ClientUdpErrorDisposition::Operation,
    }
}

/// Classifies only evidence that identifies the established physical QUIC
/// connection as failed or unable to accept another Product request.
///
/// `QuicCarrierError::is_path_lifetime_failure` has the broader historical
/// meaning "the logical reliable path may migrate". In particular, request
/// stream reset/finish is sufficient to migrate that Product attachment but
/// is not authority to retire sibling requests sharing the QUIC connection.
fn quic_product_error_has_carrier_lifetime_authority(source: &QuicCarrierError) -> bool {
    match source {
        QuicCarrierError::Io(_)
        | QuicCarrierError::Connection(_)
        | QuicCarrierError::H3Connection(_)
        | QuicCarrierError::H3DriverClosed
        | QuicCarrierError::Write(quinn::WriteError::ConnectionLost(_))
        | QuicCarrierError::Read(quinn::ReadError::ConnectionLost(_))
        | QuicCarrierError::NativeDatagram(quinn::SendDatagramError::ConnectionLost(_)) => true,
        QuicCarrierError::H3Stream(error) => matches!(
            error,
            h3::error::StreamError::ConnectionError(_) | h3::error::StreamError::RemoteClosing
        ),
        _ => false,
    }
}

fn client_udp_endpoint_error_has_health_authority(source: &RuntimeError) -> bool {
    matches!(
        source,
        RuntimeError::Io(_)
            | RuntimeError::Udp(_)
            | RuntimeError::PathOpenTimedOut
            // In endpoint-establishment context this is emitted only when an
            // authenticated connection closes before owner publication. The
            // same generic error observed on a Product request remains
            // operation-local in `client_udp_error_disposition`.
            | RuntimeError::ReliablePathSessionClosed
            | RuntimeError::QuicCarrier(
                QuicCarrierError::Io(_)
                    | QuicCarrierError::Connection(_)
                    | QuicCarrierError::H3Connection(_)
                    | QuicCarrierError::H3Stream(_)
                    | QuicCarrierError::H3DriverClosed
                    | QuicCarrierError::H3StreamFinished
                    | QuicCarrierError::StreamFinished
                    | QuicCarrierError::UnexpectedEnd
                    | QuicCarrierError::ClosedStream(_)
            )
    )
}

fn client_udp_native_close_authorizes_retry(
    connection_closed: bool,
    source: &RuntimeError,
) -> bool {
    connection_closed && !matches!(source, RuntimeError::RemoteClosed(_))
}

fn quic_address_attempt_delay(remaining: Duration, unstarted: usize) -> Duration {
    debug_assert!(unstarted > 0 && unstarted < u32::MAX as usize);
    let slots = unstarted as u32 + 1;
    (remaining / slots).min(QUIC_ADDRESS_ATTEMPT_DELAY)
}

fn next_quic_address_attempt_at(
    open_deadline: tokio::time::Instant,
    unstarted: usize,
) -> tokio::time::Instant {
    let now = tokio::time::Instant::now();
    now + quic_address_attempt_delay(open_deadline.saturating_duration_since(now), unstarted)
}

#[derive(Clone)]
pub(in crate::runtime) struct ClientUdpPathSessionHandle {
    runtime: ClientUdpPathSessionRuntime,
    connection: Arc<AsyncMutex<Option<ClientUdpPathConnection>>>,
    #[cfg(test)]
    retryable_open_failure_hook:
        Arc<std::sync::Mutex<Option<ClientUdpRetryableOpenFailureTestHook>>>,
    #[cfg(test)]
    accepted_open_hook: Arc<std::sync::Mutex<Option<ClientUdpAcceptedOpenTestHook>>>,
}

#[cfg(test)]
#[derive(Clone)]
struct ClientUdpRetryableOpenFailureTestHook {
    reached: mpsc::UnboundedSender<CarrierPathInstanceId>,
    resume: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClientUdpAcceptedOpenKind {
    Reliable,
    Datagram,
}

#[cfg(test)]
struct ClientUdpAcceptedOpenTestHook {
    reached: mpsc::UnboundedSender<(ClientUdpAcceptedOpenKind, CarrierPathInstanceId)>,
    resume: Arc<tokio::sync::Notify>,
}

impl std::fmt::Debug for ClientUdpPathSessionHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientUdpPathSessionHandle")
            .finish_non_exhaustive()
    }
}

impl ClientUdpPathSessionHandle {
    pub(in crate::runtime) fn new(runtime: ClientUdpPathSessionRuntime) -> Self {
        Self {
            runtime,
            connection: Arc::new(AsyncMutex::new(None)),
            #[cfg(test)]
            retryable_open_failure_hook: Arc::new(std::sync::Mutex::new(None)),
            #[cfg(test)]
            accepted_open_hook: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) async fn prepare_connection(
        &self,
        open_deadline: tokio::time::Instant,
    ) -> Result<Option<Duration>, RuntimeError> {
        Ok(self
            .prepare_connection_for_probe(open_deadline)
            .await?
            .map(|(_, elapsed)| elapsed))
    }

    pub(in crate::runtime) async fn prepare_connection_for_probe(
        &self,
        open_deadline: tokio::time::Instant,
    ) -> Result<Option<(CarrierPathInstanceId, Duration)>, RuntimeError> {
        let (carrier, newly_connected) = self.ensure_connection_with_status(open_deadline).await?;
        Ok(newly_connected.then(|| (carrier.path_instance_id, carrier.connection.rtt())))
    }

    pub(in crate::runtime) async fn open_stream(
        &self,
        stream_id: StreamId,
        target: TargetAddr,
        lane: TrafficClass,
        initial_demand: StreamDemandHint,
        open_deadline: tokio::time::Instant,
        advertised_recv_max_offset: u64,
    ) -> Result<OpenedReliableCarrierStream, RuntimeError> {
        for attempt in 0..MAX_CLIENT_UDP_EXACT_OPEN_ATTEMPTS {
            let expected_path_instance_id = self
                .runtime
                .state
                .path_instance_id(UnderlayProtocol::Udp, self.runtime.path_index);
            let connection =
                match tokio::time::timeout_at(open_deadline, self.ensure_connection(open_deadline))
                    .await
                    .map_err(|_| RuntimeError::PathOpenTimedOut)
                    .and_then(|result| result)
                {
                    Ok(connection) => connection,
                    Err(source) => {
                        if client_udp_endpoint_error_has_health_authority(&source) {
                            self.runtime
                                .state
                                .mark_udp_path_establishment_failure_if_current(
                                    self.runtime.path_index,
                                    expected_path_instance_id,
                                );
                        }
                        return Err(source);
                    }
                };
            let path_instance_id = connection.path_instance_id;
            let result = tokio::time::timeout_at(
                open_deadline,
                open_client_udp_stream_on_connection(
                    connection,
                    stream_id,
                    target.clone(),
                    lane,
                    initial_demand,
                    advertised_recv_max_offset,
                    self.runtime.clone(),
                ),
            )
            .await
            .map_err(|_| RuntimeError::PathOpenTimedOut)
            .and_then(|result| result);
            match result {
                Ok(stream) => {
                    #[cfg(test)]
                    self.pause_accepted_open_for_test(
                        ClientUdpAcceptedOpenKind::Reliable,
                        path_instance_id,
                    )
                    .await;
                    let committed = tokio::time::timeout_at(
                        open_deadline,
                        self.try_commit_opened_instance(path_instance_id),
                    )
                    .await;
                    let commit_timed_out = match committed {
                        Ok(true) => return Ok(stream),
                        Ok(false) => false,
                        Err(_) => true,
                    };
                    let _ = stream.retire_uncommitted();
                    if commit_timed_out {
                        return Err(RuntimeError::PathOpenTimedOut);
                    }
                    if attempt + 1 < MAX_CLIENT_UDP_EXACT_OPEN_ATTEMPTS {
                        continue;
                    }
                    return Err(RuntimeError::ReliablePathRetired);
                }
                Err(source) => {
                    #[cfg(test)]
                    if client_udp_error_disposition(&source)
                        == ClientUdpErrorDisposition::CarrierLifetime
                    {
                        self.pause_retryable_open_failure_for_test(path_instance_id)
                            .await;
                    }
                    let disposition = self
                        .settle_established_error(path_instance_id, &source)
                        .await;
                    if disposition == ClientUdpErrorDisposition::CarrierLifetime
                        && attempt + 1 < MAX_CLIENT_UDP_EXACT_OPEN_ATTEMPTS
                    {
                        continue;
                    }
                    return Err(source);
                }
            }
        }
        unreachable!("bounded QUIC Product stream-open attempts return from the loop")
    }

    pub(in crate::runtime) async fn open_datagram_stream(
        &self,
        open_deadline: tokio::time::Instant,
    ) -> Result<ClientUdpDatagramStream, RuntimeError> {
        for attempt in 0..MAX_CLIENT_UDP_EXACT_OPEN_ATTEMPTS {
            let expected_path_instance_id = self
                .runtime
                .state
                .path_instance_id(UnderlayProtocol::Udp, self.runtime.path_index);
            let connection =
                match tokio::time::timeout_at(open_deadline, self.ensure_connection(open_deadline))
                    .await
                    .map_err(|_| RuntimeError::PathOpenTimedOut)
                    .and_then(|result| result)
                {
                    Ok(connection) => connection,
                    Err(source) => {
                        if client_udp_endpoint_error_has_health_authority(&source) {
                            self.runtime
                                .state
                                .mark_udp_path_establishment_failure_if_current(
                                    self.runtime.path_index,
                                    expected_path_instance_id,
                                );
                        }
                        return Err(source);
                    }
                };
            let path_instance_id = connection.path_instance_id;
            let result = tokio::time::timeout_at(
                open_deadline,
                open_client_udp_datagram_stream(connection, self.runtime.clone()),
            )
            .await
            .map_err(|_| RuntimeError::PathOpenTimedOut)
            .and_then(|result| result);
            match result {
                Ok(stream) => {
                    #[cfg(test)]
                    self.pause_accepted_open_for_test(
                        ClientUdpAcceptedOpenKind::Datagram,
                        path_instance_id,
                    )
                    .await;
                    let committed = tokio::time::timeout_at(
                        open_deadline,
                        self.try_commit_opened_instance(path_instance_id),
                    )
                    .await;
                    let commit_timed_out = match committed {
                        Ok(true) => return Ok(stream),
                        Ok(false) => false,
                        Err(_) => true,
                    };
                    stream.retire_uncommitted();
                    if commit_timed_out {
                        return Err(RuntimeError::PathOpenTimedOut);
                    }
                    if attempt + 1 < MAX_CLIENT_UDP_EXACT_OPEN_ATTEMPTS {
                        continue;
                    }
                    return Err(RuntimeError::ReliablePathRetired);
                }
                Err(source) => {
                    let disposition = self
                        .settle_established_error(path_instance_id, &source)
                        .await;
                    if disposition == ClientUdpErrorDisposition::CarrierLifetime
                        && attempt + 1 < MAX_CLIENT_UDP_EXACT_OPEN_ATTEMPTS
                    {
                        continue;
                    }
                    return Err(source);
                }
            }
        }
        unreachable!("bounded QUIC Product datagram-open attempts return from the loop")
    }

    pub(in crate::runtime) async fn open_ip_tunnel_attachment(
        &self,
        tunnel_id: crate::protocol::IpTunnelId,
        open_deadline: tokio::time::Instant,
    ) -> Result<super::ip_tunnel::ClientUdpIpTunnelOpenOutcome, RuntimeError> {
        let open = async {
            let connection = self.ensure_connection(open_deadline).await?;
            let path_instance_id = connection.path_instance_id;
            let connection_lifetime = connection.connection.clone();
            match open_client_udp_ip_tunnel(connection, self.runtime.clone(), tunnel_id).await {
                Ok(attachment) => Ok(attachment),
                Err(err)
                    if client_udp_native_close_authorizes_retry(
                        connection_lifetime.is_closed(),
                        &err,
                    ) =>
                {
                    self.drop_failed_connection_instance(path_instance_id).await;
                    let connection = self.ensure_connection(open_deadline).await?;
                    open_client_udp_ip_tunnel(connection, self.runtime.clone(), tunnel_id).await
                }
                Err(err) => Err(err),
            }
        };
        tokio::time::timeout_at(open_deadline, open)
            .await
            .map_err(|_| RuntimeError::PathOpenTimedOut)?
    }

    async fn ensure_connection(
        &self,
        open_deadline: tokio::time::Instant,
    ) -> Result<ClientUdpCarrierInstance, RuntimeError> {
        Ok(self.ensure_connection_with_status(open_deadline).await?.0)
    }

    async fn ensure_connection_with_status(
        &self,
        open_deadline: tokio::time::Instant,
    ) -> Result<(ClientUdpCarrierInstance, bool), RuntimeError> {
        self.runtime.state.session_lifecycle().ensure_active()?;
        let mut current = self.connection.lock().await;
        self.runtime.state.session_lifecycle().ensure_active()?;
        if let Some(failed) = current
            .as_ref()
            .filter(|connection| connection.carrier.connection.is_closed())
            .map(|connection| connection.carrier.path_instance_id)
        {
            self.retire_connection_owner_locked(&mut current, failed);
        }
        if let Some(connection) = current.as_ref() {
            return self
                .runtime
                .state
                .session_lifecycle()
                .commit_if_active(|| (connection.carrier.clone(), false))
                .map_err(RuntimeError::RemoteClosed);
        }
        let mut pending = Some(connect_client_udp_path(&self.runtime, open_deadline).await?);
        let carrier = pending
            .as_ref()
            .expect("new QUIC carrier awaits owner publication")
            .carrier
            .clone();
        let peer_usage = pending
            .as_ref()
            .expect("new QUIC carrier awaits health publication")
            .peer_usage;
        let published = self
            .runtime
            .state
            .session_lifecycle()
            .commit_if_active(|| {
                self.runtime.state.publish_udp_peer_path_usage_committed(
                    self.runtime.path_index,
                    carrier.path_instance_id,
                    0,
                    peer_usage,
                    || !carrier.connection.is_closed(),
                    || {
                        let mut connection = pending
                            .take()
                            .expect("new QUIC carrier awaits owner publication");
                        connection._authenticated_carrier =
                            Some(self.runtime.authenticated_carriers.register());
                        *current = Some(connection);
                    },
                )
            })
            .map_err(RuntimeError::RemoteClosed)?;
        if !published {
            return Err(RuntimeError::ReliablePathSessionClosed);
        }
        current
            .as_mut()
            .expect("published QUIC carrier has one physical owner")
            .start_background_tasks(&self.runtime);
        Ok((carrier, true))
    }

    fn retire_connection_owner_locked(
        &self,
        current: &mut Option<ClientUdpPathConnection>,
        failed: CarrierPathInstanceId,
    ) -> bool {
        if !current
            .as_ref()
            .is_some_and(|connection| connection.carrier.path_instance_id == failed)
        {
            return false;
        }
        let mut retired = None;
        self.runtime.state.settle_udp_path_instance_failure(
            self.runtime.path_index,
            failed,
            || {
                retired = current.take();
            },
        );
        if let Some(connection) = retired {
            connection.carrier.connection.close();
            true
        } else {
            false
        }
    }

    async fn retire_failed_connection_instance(&self, failed: CarrierPathInstanceId) -> bool {
        let mut current = self.connection.lock().await;
        self.retire_connection_owner_locked(&mut current, failed)
    }

    /// Compatibility path for IP-tunnel open, whose settlement remains
    /// outside C4. It retains the released exact-instance health publication.
    async fn drop_failed_connection_instance(&self, failed: CarrierPathInstanceId) {
        self.retire_failed_connection_instance(failed).await;
    }

    pub(in crate::runtime) async fn settle_established_error(
        &self,
        path_instance_id: CarrierPathInstanceId,
        source: &RuntimeError,
    ) -> ClientUdpErrorDisposition {
        let disposition = client_udp_error_disposition(source);
        self.settle_established_disposition(path_instance_id, disposition)
            .await
    }

    pub(in crate::runtime) async fn settle_established_disposition(
        &self,
        path_instance_id: CarrierPathInstanceId,
        disposition: ClientUdpErrorDisposition,
    ) -> ClientUdpErrorDisposition {
        match disposition {
            ClientUdpErrorDisposition::Session => {}
            ClientUdpErrorDisposition::CarrierLifetime => {
                self.retire_failed_connection_instance(path_instance_id)
                    .await;
            }
            ClientUdpErrorDisposition::Operation => {
                let physically_closed = {
                    let current = self.connection.lock().await;
                    current.as_ref().is_some_and(|connection| {
                        connection.carrier.path_instance_id == path_instance_id
                            && connection.carrier.connection.is_closed()
                    })
                };
                if physically_closed {
                    // Physical closure is independent exact-instance
                    // evidence. It may retire N, but it cannot change this
                    // operation-local Product disposition or authorize retry.
                    self.retire_failed_connection_instance(path_instance_id)
                        .await;
                }
            }
        }
        disposition
    }

    async fn try_commit_opened_instance(&self, path_instance_id: CarrierPathInstanceId) -> bool {
        let current = self.connection.lock().await;
        let Some(connection) = current.as_ref().filter(|connection| {
            connection.carrier.path_instance_id == path_instance_id
                && !connection.carrier.connection.is_closed()
        }) else {
            return false;
        };
        debug_assert_eq!(connection.carrier.path_instance_id, path_instance_id);
        self.runtime.state.try_commit_path_instance(
            RelayPathKey {
                underlay: UnderlayProtocol::Udp,
                index: self.runtime.path_index,
            },
            path_instance_id,
        )
    }

    #[cfg(test)]
    fn set_retryable_open_failure_hook(&self, hook: Option<ClientUdpRetryableOpenFailureTestHook>) {
        *self
            .retryable_open_failure_hook
            .lock()
            .expect("client QUIC open-failure hook lock") = hook;
    }

    #[cfg(test)]
    async fn pause_retryable_open_failure_for_test(&self, opened: CarrierPathInstanceId) {
        let hook = self
            .retryable_open_failure_hook
            .lock()
            .expect("client QUIC open-failure hook lock")
            .clone();
        if let Some(hook) = hook {
            let _ = hook.reached.send(opened);
            hook.resume.notified().await;
        }
    }

    #[cfg(test)]
    fn set_accepted_open_hook(&self, hook: Option<ClientUdpAcceptedOpenTestHook>) {
        *self
            .accepted_open_hook
            .lock()
            .expect("client QUIC accepted-open hook lock") = hook;
    }

    #[cfg(test)]
    async fn pause_accepted_open_for_test(
        &self,
        kind: ClientUdpAcceptedOpenKind,
        path_instance_id: CarrierPathInstanceId,
    ) {
        let hook = self
            .accepted_open_hook
            .lock()
            .expect("client QUIC accepted-open hook lock")
            .take();
        if let Some(hook) = hook {
            let _ = hook.reached.send((kind, path_instance_id));
            hook.resume.notified().await;
        }
    }

    pub(in crate::runtime) async fn wait_for_connection_instance_change(
        &self,
        previous: CarrierPathInstanceId,
    ) {
        let connection = {
            let current = self.connection.lock().await;
            match current.as_ref() {
                Some(connection) if connection.carrier.path_instance_id == previous => {
                    connection.carrier.connection.clone()
                }
                _ => return,
            }
        };
        connection.wait_closed().await;
    }
}

#[derive(Clone)]
pub(in crate::runtime) struct ClientUdpPathSessionRuntime {
    pub(in crate::runtime) paths: Arc<Vec<PathSpec>>,
    pub(in crate::runtime) config_index: usize,
    pub(in crate::runtime) path_index: usize,
    pub(in crate::runtime) carrier_identity: CarrierPathIdentity,
    pub(in crate::runtime) session_id: SessionId,
    pub(in crate::runtime) security: Arc<Vec<ClientSecurityConfig>>,
    pub(in crate::runtime) candidate_selector: QuicCandidateSelector,
    pub(in crate::runtime) tls: Arc<Vec<TcpClientTlsConfig>>,
    pub(in crate::runtime) codec_limits: CodecLimits,
    pub(in crate::runtime) mux_limits: MuxLimits,
    pub(in crate::runtime) stream_frame_queue: usize,
    pub(in crate::runtime) state: Arc<ClientPathState>,
    pub(in crate::runtime) carrier_network: Arc<dyn CarrierNetworkProvider>,
    pub(in crate::runtime) peer_status: PeerStatusBroker,
    pub(in crate::runtime) peer_status_snapshot: PeerStatusSnapshotSource,
    pub(in crate::runtime) authenticated_carriers:
        crate::runtime::path::AuthenticatedCarrierInventory,
    pub(in crate::runtime) ip_tunnels: crate::runtime::tun_l3::ClientIpTunnelHub,
}

impl ClientUdpPathSessionRuntime {
    pub(in crate::runtime) fn path(&self) -> &PathSpec {
        self.paths
            .get(self.config_index)
            .expect("UDP session path inventory matches its index")
    }

    pub(in crate::runtime) fn security(&self) -> &ClientSecurityConfig {
        self.security
            .get(self.config_index)
            .expect("UDP session security inventory matches its index")
    }

    pub(in crate::runtime) fn tls(&self) -> &TcpClientTlsConfig {
        self.tls
            .get(self.config_index)
            .expect("UDP session TLS identity inventory matches its index")
    }
}

#[derive(Clone)]
pub(super) struct ClientUdpCarrierInstance {
    pub(super) connection: UdpPathConnection,
    pub(super) path_instance_id: CarrierPathInstanceId,
}

struct ClientUdpPathConnection {
    endpoint: UdpPathEndpoint,
    carrier: ClientUdpCarrierInstance,
    peer_usage: PathUsage,
    _authenticated_carrier: Option<crate::runtime::path::AuthenticatedCarrierRegistration>,
    startup: Option<ClientUdpPathConnectionStartup>,
    lifecycle_task: Option<tokio::task::JoinHandle<()>>,
    metrics_task: Option<tokio::task::JoinHandle<()>>,
    control_task: Option<tokio::task::JoinHandle<()>>,
    port_migration_task: Option<tokio::task::JoinHandle<()>>,
}

struct ClientUdpPathConnectionStartup {
    control_send: UdpPathSendStream,
    control_recv: UdpPathRecvStream,
    canonical_remote: std::net::SocketAddr,
}

impl ClientUdpPathConnection {
    /// Starts exact-instance observers only after the physical owner and its
    /// health identity are atomically visible. The caller still holds the
    /// connection-owner mutex, so no Product open can overtake startup.
    fn start_background_tasks(&mut self, runtime: &ClientUdpPathSessionRuntime) {
        let ClientUdpPathConnectionStartup {
            control_send,
            control_recv,
            canonical_remote,
        } = self
            .startup
            .take()
            .expect("new QUIC carrier starts its observers exactly once");
        let path_instance_id = self.carrier.path_instance_id;
        let connection = self.carrier.connection.clone();
        let authenticated_carrier = self
            ._authenticated_carrier
            .take()
            .expect("published QUIC carrier owns one authenticated registration");
        let retirement = runtime.state.session_retirement();
        let lifecycle_connection = connection.clone();
        let lifecycle_state = runtime.state.clone();
        let path_index = runtime.path_index;
        self.lifecycle_task = Some(tokio::spawn(async move {
            tokio::select! {
                biased;
                _reason = retirement.wait() => {
                    lifecycle_state.mark_path_instance_data_plane_failure(
                        RelayPathKey {
                            underlay: UnderlayProtocol::Udp,
                            index: path_index,
                        },
                        path_instance_id,
                    );
                    lifecycle_connection.close();
                }
                _ = lifecycle_connection.wait_closed() => {}
            }
            drop(authenticated_carrier);
        }));
        self.metrics_task = Some(spawn_client_udp_path_metrics(
            runtime.clone(),
            connection.clone(),
            path_instance_id,
        ));
        let peer_status = runtime.peer_status.register_path(
            runtime.session_id,
            UnderlayProtocol::Udp,
            PathId(runtime.path_index as u16),
            runtime.config_index,
            Some(connection.remote_address().port()),
        );
        let path_metadata = peer_status
            .path_metadata_handle()
            .expect("QUIC peer-status registration has path metadata");
        let control_connection = connection.clone();
        let control_runtime = runtime.clone();
        self.control_task = Some(tokio::spawn(async move {
            if let Err(err) = run_client_udp_control_stream(
                control_send,
                control_recv,
                peer_status,
                control_runtime,
            )
            .await
            {
                warn_unexpected_udp_runtime_error("client QUIC control stream failed", &err);
                control_connection.close();
            }
        }));
        self.port_migration_task = runtime.path().port_hop_interval().map(|interval| {
            spawn_client_udp_port_migration(
                runtime.clone(),
                self.endpoint.clone(),
                connection,
                canonical_remote,
                interval,
                path_metadata,
            )
        });
    }
}

// The metrics loop holds a carrier clone, so the session must retire it explicitly.
impl Drop for ClientUdpPathConnection {
    fn drop(&mut self) {
        self.carrier.connection.close();
        if let Some(task) = self.lifecycle_task.take() {
            task.abort();
        }
        if let Some(task) = self.metrics_task.take() {
            task.abort();
        }
        if let Some(task) = self.control_task.take() {
            task.abort();
        }
        if let Some(task) = self.port_migration_task.take() {
            task.abort();
        }
    }
}

fn spawn_client_udp_path_metrics(
    runtime: ClientUdpPathSessionRuntime,
    connection: UdpPathConnection,
    path_instance_id: CarrierPathInstanceId,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tracker = UdpPathMetricTracker::default();
        #[cfg(feature = "lab-diagnostics")]
        let mut last_metrics_poll_at = None;
        let delivery_activity = connection.delivery_activity_notify();
        loop {
            let activity_started = delivery_activity.notified();
            tokio::pin!(activity_started);
            activity_started.as_mut().enable();
            if connection.is_closed() {
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "quic_carrier_closed",
                    format_args!(
                        "session_id={} path_index={} path_instance_id={:?} locally_closed={} reason={}",
                        runtime.session_id.0,
                        runtime.path_index,
                        path_instance_id,
                        connection.is_locally_closed(),
                        connection
                            .close_reason()
                            .unwrap_or_else(|| "unknown".to_string()),
                    ),
                );
                if !connection.is_locally_closed() {
                    runtime.state.mark_path_instance_data_plane_failure(
                        RelayPathKey {
                            underlay: UnderlayProtocol::Udp,
                            index: runtime.path_index,
                        },
                        path_instance_id,
                    );
                }
                return;
            }
            let metrics = connection.tx_metrics(&mut tracker, PathMetricDirection::ClientToServer);
            #[cfg(feature = "lab-diagnostics")]
            let metrics_poll_at = Instant::now();
            #[cfg(feature = "lab-diagnostics")]
            let poll_elapsed = last_metrics_poll_at
                .replace(metrics_poll_at)
                .map(|previous| metrics_poll_at.saturating_duration_since(previous))
                .unwrap_or_default();
            #[cfg(feature = "lab-diagnostics")]
            log_quic_ack_poll_diagnostics(
                runtime.session_id,
                PathId(runtime.path_index as u16),
                path_instance_id.as_u64(),
                metrics,
                poll_elapsed,
            );
            let _ = runtime.state.mutate_path_eligibility(
                crate::model::path::RelayPathKey {
                    underlay: crate::protocol::UnderlayProtocol::Udp,
                    index: runtime.path_index,
                },
                |record| {
                    record.mark_quic_path_metrics(path_instance_id, metrics);
                },
            );
            tokio::select! {
                _ = tokio::time::sleep(quic_path_metrics_poll_interval(metrics)) => {}
                _ = &mut activity_started => {
                    tokio::time::sleep(quic_path_metrics_ack_interval(metrics)).await;
                }
            }
        }
    })
}

pub(in crate::runtime) struct ClientUdpDatagramStream {
    pub(in crate::runtime) send: UdpPathSendStream,
    pub(in crate::runtime) frames: mpsc::Receiver<Result<Frame, RuntimeError>>,
    pub(in crate::runtime) runtime: ClientUdpPathSessionRuntime,
    pub(in crate::runtime) path_id: PathId,
    pub(in crate::runtime) path_instance_id: CarrierPathInstanceId,
}

impl ClientUdpDatagramStream {
    fn retire_uncommitted(self) {
        // No MPP datagram flow owns this request yet. Dropping both HTTP/3
        // halves closes only this request, and the receive-half drop retires
        // its native-datagram route; sibling requests retain the connection.
        drop(self);
    }
}

async fn connect_client_udp_path(
    runtime: &ClientUdpPathSessionRuntime,
    open_deadline: tokio::time::Instant,
) -> Result<ClientUdpPathConnection, RuntimeError> {
    runtime.state.session_lifecycle().ensure_active()?;
    let connect = async {
        let remote_port = runtime.path().endpoint.ports().select().map_err(|error| {
            RuntimeError::Io(std::io::Error::other(format!(
                "could not select a carrier port for {}: {error}",
                runtime.path().endpoint.authority()
            )))
        })?;
        let resolved = runtime
            .carrier_network
            .resolve(CarrierResolutionRequest {
                path: runtime.path(),
                identity: runtime.carrier_identity,
                remote_port,
            })
            .await?;
        let resolved = crate::transport::validate_carrier_resolution_port(resolved, remote_port)?;
        let resolved = usable_udp_path_socket_addrs(runtime.path(), resolved)?;
        let mut remote_addrs = interleave_socket_addr_families(resolved)
            .into_iter()
            .take(MAX_QUIC_ADDRESS_ATTEMPTS)
            .collect::<VecDeque<_>>();
        let mut attempts = FuturesUnordered::new();
        let first_addr = remote_addrs
            .pop_front()
            .expect("resolver rejects an empty address set");
        attempts.push(connect_client_udp_addr(runtime, first_addr));
        let mut next_attempt_at = (!remote_addrs.is_empty())
            .then(|| next_quic_address_attempt_at(open_deadline, remote_addrs.len()));

        // A blackholed first DNS record must not consume the whole path budget.
        // Race establishment only; dropping the remaining futures closes losers.
        let mut last_error = None;
        let established = loop {
            let completed = if remote_addrs.is_empty() {
                attempts.next().await
            } else {
                tokio::select! {
                    biased;
                    completed = attempts.next() => completed,
                    _ = tokio::time::sleep_until(
                        next_attempt_at.expect("unstarted addresses have a launch time")
                    ) => {
                        if tokio::time::Instant::now() >= open_deadline {
                            return Err(RuntimeError::PathOpenTimedOut);
                        }
                        let remote_addr = remote_addrs
                            .pop_front()
                            .expect("address availability checked before stagger timer");
                        attempts.push(connect_client_udp_addr(runtime, remote_addr));
                        next_attempt_at = (!remote_addrs.is_empty()).then(|| {
                            next_quic_address_attempt_at(open_deadline, remote_addrs.len())
                        });
                        continue;
                    }
                }
            };
            match completed {
                Some(Ok(connection)) => break connection,
                Some(Err(err)) => {
                    last_error = Some(err);
                    tokio::task::yield_now().await;
                    if tokio::time::Instant::now() >= open_deadline {
                        return Err(RuntimeError::PathOpenTimedOut);
                    }
                    // A hard failure does not need the blackhole stagger.
                    if attempts.is_empty()
                        && let Some(remote_addr) = remote_addrs.pop_front()
                    {
                        attempts.push(connect_client_udp_addr(runtime, remote_addr));
                        next_attempt_at = (!remote_addrs.is_empty()).then(|| {
                            next_quic_address_attempt_at(open_deadline, remote_addrs.len())
                        });
                    }
                }
                None => {
                    return Err(last_error.unwrap_or(RuntimeError::Protocol(
                        "QUIC UDP path exhausted resolved socket addresses",
                    )));
                }
            }
        };
        drop(attempts);
        let EstablishedClientUdpPath {
            endpoint,
            connection,
            canonical_remote,
        } = established;

        // Address retry owns only carrier establishment. Authenticate exactly
        // once so a rejected MPP identity is never retried as a DNS decision.
        let (peer_usage, control_send, control_recv) =
            match perform_client_udp_path_handshake(&connection, runtime).await {
                Ok(handshake) => handshake,
                Err(RuntimeError::RemoteClosed(reason)) => {
                    let reason = runtime.state.session_lifecycle().retire(reason);
                    return Err(RuntimeError::RemoteClosed(reason));
                }
                Err(error) => return Err(error),
            };
        let path_instance_id = next_carrier_path_instance_id();
        Ok(ClientUdpPathConnection {
            endpoint,
            carrier: ClientUdpCarrierInstance {
                connection,
                path_instance_id,
            },
            peer_usage,
            _authenticated_carrier: None,
            startup: Some(ClientUdpPathConnectionStartup {
                control_send,
                control_recv,
                canonical_remote,
            }),
            lifecycle_task: None,
            metrics_task: None,
            control_task: None,
            port_migration_task: None,
        })
    };
    let retirement = runtime.state.session_retirement().wait();
    tokio::pin!(retirement);
    let connect = tokio::time::timeout_at(open_deadline, connect);
    tokio::pin!(connect);
    tokio::select! {
        biased;
        reason = &mut retirement => Err(RuntimeError::RemoteClosed(reason)),
        result = &mut connect => {
            result.map_err(|_| RuntimeError::PathOpenTimedOut)?
        }
    }
}

struct EstablishedClientUdpPath {
    endpoint: UdpPathEndpoint,
    connection: UdpPathConnection,
    canonical_remote: std::net::SocketAddr,
}

async fn connect_client_udp_addr(
    runtime: &ClientUdpPathSessionRuntime,
    remote_addr: std::net::SocketAddr,
) -> Result<EstablishedClientUdpPath, RuntimeError> {
    // Each attempt needs its own family-correct, host-protected socket.
    let carrier = runtime
        .carrier_network
        .create_socket(CarrierSocketRequest {
            path: runtime.path(),
            identity: runtime.carrier_identity,
            remote_addr,
        })?;
    let endpoint = UdpPathEndpoint::bind_client(carrier, runtime).await?;
    let connection = endpoint.connect(remote_addr).await?;
    Ok(EstablishedClientUdpPath {
        endpoint,
        connection,
        canonical_remote: remote_addr,
    })
}

fn spawn_client_udp_port_migration(
    runtime: ClientUdpPathSessionRuntime,
    endpoint: UdpPathEndpoint,
    connection: UdpPathConnection,
    canonical_remote: std::net::SocketAddr,
    interval: Duration,
    path_metadata: PeerStatusPathMetadataHandle,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut current_port = canonical_remote.port();
        loop {
            tokio::select! {
                _ = tokio::time::sleep(interval) => {}
                _ = connection.wait_closed() => return,
            }
            if connection.is_closed() {
                return;
            }

            let selected_port = match runtime.path().endpoint.ports().select_other(current_port) {
                Ok(port) => port,
                Err(err) => {
                    crate::observability::process_event!(
                        Warn,
                        "quic",
                        "carrier_port_migration_failed",
                        "QUIC carrier destination-port migration could not obtain OS entropy; \
                         group={}, path={}: {err}",
                        runtime.carrier_identity.group_ordinal,
                        runtime.carrier_identity.path_ordinal
                    );
                    continue;
                }
            };
            let selected_remote = std::net::SocketAddr::new(canonical_remote.ip(), selected_port);
            let socket = match runtime.carrier_network.create_socket(CarrierSocketRequest {
                path: runtime.path(),
                identity: runtime.carrier_identity,
                remote_addr: selected_remote,
            }) {
                Ok(socket) => socket,
                Err(err) => {
                    crate::observability::process_event!(
                        Warn,
                        "quic",
                        "carrier_port_migration_failed",
                        "QUIC carrier destination-port migration could not create a host-protected \
                         socket; group={}, path={}: {err}",
                        runtime.carrier_identity.group_ordinal,
                        runtime.carrier_identity.path_ordinal
                    );
                    continue;
                }
            };
            let migration = match endpoint.migrate_destination_port(
                socket,
                canonical_remote,
                selected_remote,
            ) {
                Ok(migration) => migration,
                Err(err) => {
                    crate::observability::process_event!(
                        Warn,
                        "quic",
                        "carrier_port_migration_failed",
                        "QUIC carrier destination-port migration failed; group={}, path={}: {err}",
                        runtime.carrier_identity.group_ordinal,
                        runtime.carrier_identity.path_ordinal
                    );
                    continue;
                }
            };
            tokio::select! {
                _ = migration => {}
                _ = connection.wait_closed() => return,
            }
            current_port = selected_port;
            let _ = path_metadata.set_active_port(selected_port);
            crate::observability::process_event!(
                Info,
                "quic",
                "carrier_port_migrated",
                "QUIC carrier changed destination port without changing carrier identity; \
                 group={}, path={}",
                runtime.carrier_identity.group_ordinal,
                runtime.carrier_identity.path_ordinal
            );
        }
    })
}

async fn perform_client_udp_path_handshake(
    connection: &UdpPathConnection,
    runtime: &ClientUdpPathSessionRuntime,
) -> Result<(PathUsage, UdpPathSendStream, UdpPathRecvStream), RuntimeError> {
    let (mut send, mut recv) = connection.open_bi().await?;
    send.set_traffic_class(TrafficClass::Control)?;
    let path_id = PathId(runtime.path_index as u16);
    let [session_hello, session_auth, path_join] = ClientPathAuthenticationFrames::for_session(
        runtime.security(),
        path_id,
        UnderlayProtocol::Udp,
        runtime.session_id,
    )?
    .into_array();
    udp_path_write_frame(&mut send, &session_hello, runtime.codec_limits).await?;
    udp_path_write_frame(&mut send, &session_auth, runtime.codec_limits).await?;
    udp_path_write_frame(&mut send, &path_join, runtime.codec_limits).await?;
    udp_path_write_frame(
        &mut send,
        &Frame::PathStatus {
            path_id,
            sequence: 0,
            usage: if runtime.path().metadata.policy.backup {
                PathUsage::Backup
            } else {
                PathUsage::Available
            },
        },
        runtime.codec_limits,
    )
    .await?;
    let mut session_ready = false;
    let mut peer_usage = None;
    while !session_ready || peer_usage.is_none() {
        match udp_path_read_frame(&mut recv, runtime.codec_limits).await? {
            Frame::SessionReady => session_ready = true,
            Frame::PathStatus {
                path_id: status_path_id,
                sequence: 0,
                usage,
            } if status_path_id == path_id => peer_usage = Some(usage),
            Frame::PathStatus { .. } => {
                return Err(RuntimeError::Protocol(
                    "invalid UDP path usage advertisement",
                ));
            }
            Frame::SessionClose { reason } => {
                let reason = runtime.state.session_lifecycle().retire(reason);
                return Err(RuntimeError::RemoteClosed(reason));
            }
            _ => {
                return Err(RuntimeError::Protocol(
                    "unexpected UDP path handshake frame",
                ));
            }
        }
    }
    Ok((
        peer_usage.expect("path usage checked before handshake completion"),
        send,
        recv,
    ))
}

enum ClientUdpControlEvent {
    Frame(Result<Frame, RuntimeError>),
    Request(Option<u64>),
}

async fn run_client_udp_control_stream(
    mut send: UdpPathSendStream,
    mut recv: UdpPathRecvStream,
    mut peer_status: PeerStatusCarrier,
    runtime: ClientUdpPathSessionRuntime,
) -> Result<(), RuntimeError> {
    loop {
        let event = tokio::select! {
            frame = udp_path_read_frame(&mut recv, runtime.codec_limits) => {
                ClientUdpControlEvent::Frame(frame)
            }
            request_id = peer_status.recv_request() => {
                ClientUdpControlEvent::Request(request_id)
            }
        };
        let outgoing = match event {
            ClientUdpControlEvent::Frame(Ok(Frame::PeerStatusRequest { request_id })) => Some(
                peer_status.response_frame(request_id, runtime.codec_limits, || {
                    runtime.peer_status_snapshot.snapshot()
                }),
            ),
            ClientUdpControlEvent::Frame(Ok(Frame::PeerStatusResponse {
                request_id,
                code,
                paths,
            })) => {
                let _ = peer_status.receive_response(request_id, code, paths);
                None
            }
            ClientUdpControlEvent::Frame(Ok(Frame::SessionClose { reason })) => {
                let reason = runtime.state.session_lifecycle().retire(reason);
                return Err(RuntimeError::RemoteClosed(reason));
            }
            ClientUdpControlEvent::Frame(Ok(_)) => {
                return Err(RuntimeError::Protocol(
                    "unexpected QUIC UDP control stream frame",
                ));
            }
            // Pre-control peers finish the handshake stream; keep their product
            // connection usable and simply withdraw this diagnostic carrier.
            ClientUdpControlEvent::Frame(Err(RuntimeError::QuicCarrier(
                QuicCarrierError::StreamFinished,
            ))) => return Ok(()),
            ClientUdpControlEvent::Frame(Err(err)) => return Err(err),
            ClientUdpControlEvent::Request(Some(request_id)) => {
                Some(Frame::PeerStatusRequest { request_id })
            }
            ClientUdpControlEvent::Request(None) => return Ok(()),
        };
        if let Some(frame) = outgoing {
            udp_path_write_frame(&mut send, &frame, runtime.codec_limits).await?;
        }
    }
}

async fn open_client_udp_stream_on_connection(
    carrier: ClientUdpCarrierInstance,
    stream_id: StreamId,
    target: TargetAddr,
    lane: TrafficClass,
    initial_demand: StreamDemandHint,
    advertised_recv_max_offset: u64,
    runtime: ClientUdpPathSessionRuntime,
) -> Result<OpenedReliableCarrierStream, RuntimeError> {
    let (mut send, mut recv) = carrier.connection.open_bi().await?;
    send.set_traffic_class(lane)?;
    let open = Frame::OpenStream {
        stream_id,
        target,
        demand: initial_demand,
    };
    udp_path_write_frame(&mut send, &open, runtime.codec_limits).await?;
    // Initial opens publish the logical receive owner's starting credit.
    // Attachments pass zero so accepting another carrier cannot widen the one
    // shared receive window.
    udp_path_write_frame(
        &mut send,
        &Frame::StreamMaxData {
            stream_id,
            max_offset: advertised_recv_max_offset,
        },
        runtime.codec_limits,
    )
    .await?;
    let path_id = PathId(runtime.path_index as u16);
    let max_offset = read_client_udp_stream_open_accept(
        &mut recv,
        stream_id,
        runtime.path_index,
        carrier.path_instance_id,
        &runtime.state,
        path_id,
        runtime.codec_limits,
    )
    .await?;
    let (commands, receivers) = reliable_path_command_channels(udp_path_command_queue(
        runtime.mux_limits,
        runtime.codec_limits,
    ));
    let stream_frame_queue =
        udp_reliable_stream_frame_queue(runtime.codec_limits, runtime.mux_limits);
    let (frames_tx, frames_rx) = mpsc::channel(stream_frame_queue);
    tokio::spawn(run_client_udp_stream(
        send,
        recv,
        stream_id,
        runtime.path_index,
        carrier.path_instance_id,
        runtime.codec_limits,
        runtime.mux_limits,
        stream_frame_queue,
        runtime.state.clone(),
        receivers,
        frames_tx,
    ));
    let mut startup = path_startup_snapshot(runtime.path(), PathId(runtime.path_index as u16));
    startup.peer_usage = runtime
        .state
        .peer_path_usage(UnderlayProtocol::Udp, runtime.path_index);
    Ok(OpenedReliableCarrierStream {
        stream_id,
        path_instance_id: carrier.path_instance_id,
        max_offset,
        lane,
        underlay: UnderlayProtocol::Udp,
        max_frame_payload_bytes: udp_path_max_stream_payload_bytes(
            runtime.codec_limits,
            runtime.mux_limits,
        ),
        startup,
        commands,
        mux_limits: runtime.mux_limits,
        frames: frames_rx,
    })
}

async fn read_client_udp_stream_open_accept(
    recv: &mut UdpPathRecvStream,
    stream_id: StreamId,
    path_index: usize,
    path_instance_id: CarrierPathInstanceId,
    state: &ClientPathState,
    path_id: PathId,
    codec_limits: CodecLimits,
) -> Result<u64, RuntimeError> {
    loop {
        match udp_path_read_frame(recv, codec_limits).await? {
            Frame::StreamMaxData {
                stream_id: max_stream_id,
                max_offset,
            } if max_stream_id == stream_id => return Ok(max_offset),
            Frame::StreamReset {
                stream_id: reset_stream_id,
                reason,
            } if reset_stream_id == stream_id => return Err(RuntimeError::RemoteReset(reason)),
            Frame::StreamDetach {
                stream_id: detached_stream_id,
            } if detached_stream_id == stream_id => {
                return Err(RuntimeError::ReliablePathAttachmentRefused);
            }
            Frame::PathStatus {
                path_id: status_path_id,
                sequence,
                usage,
            } => {
                let _ = apply_client_udp_path_status(
                    state,
                    path_index,
                    path_instance_id,
                    path_id,
                    status_path_id,
                    sequence,
                    usage,
                )?;
            }
            Frame::SessionReady => {}
            Frame::SessionClose { reason } => {
                let reason = state.session_lifecycle().retire(reason);
                return Err(RuntimeError::RemoteClosed(reason));
            }
            _ => {
                return Err(RuntimeError::Protocol(
                    "unexpected QUIC UDP path stream open frame",
                ));
            }
        }
    }
}

#[cfg(test)]
#[path = "tests_client.rs"]
mod tests;

async fn open_client_udp_datagram_stream(
    carrier: ClientUdpCarrierInstance,
    runtime: ClientUdpPathSessionRuntime,
) -> Result<ClientUdpDatagramStream, RuntimeError> {
    let (mut send, recv) = carrier.connection.open_bi().await?;
    send.set_traffic_class(TrafficClass::RealtimeDatagram)?;
    let frames = spawn_quic_path_reader(recv, runtime.codec_limits, runtime.stream_frame_queue);
    Ok(ClientUdpDatagramStream {
        send,
        frames,
        path_id: PathId(runtime.path_index as u16),
        path_instance_id: carrier.path_instance_id,
        runtime,
    })
}
