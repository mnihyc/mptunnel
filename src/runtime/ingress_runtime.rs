//! Client ingress listeners and protocol handshakes.
//!
//! Each listener owns active connections so shutdown cannot leave stale streams.

use crate::ingress::http_connect::{self, HttpConnectError, HttpStatus};
use crate::ingress::socks5::{self, Socks5Error, Socks5Reply};
use crate::ingress::{
    LocalIngressAdmissionConfig, ProxyAuthConfig, TcpForwardConfig, UdpForwardConfig,
};
use crate::model::path::CarrierPathInstanceId;
use crate::mux::MuxLimits;
#[cfg(test)]
use crate::performance::MppPerformanceConfig;
use crate::product::{InboundId, PrincipalId};
use crate::protocol::TargetAddr;
use crate::runtime::datagram::{
    UdpEdgeCompletion, UdpEdgeLane, UdpEdgeRequest, close_udp_edge_lanes,
    dispatch_udp_edge_request_with_idle_timeout, finish_udp_edge_completion,
    reap_finished_udp_edge_lane_instance, remove_udp_edge_lane, udp_edge_completion_queue,
};
use crate::runtime::error::RuntimeError;
use crate::runtime::outbound_registry::relay_opened_tcp;
use crate::runtime::path::ClientPathContext;
use crate::runtime::product_lifecycle::ProductFlowActivity;
use crate::runtime::product_policy::{ClientIngressRouter, ClientPolicyDisposition, ClientRoute};
use crate::runtime::readiness::RequiredServiceReadiness;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, hash_map::Entry};
use std::future::pending;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{Semaphore, mpsc};

const MAX_HTTP_CONNECT_HEADER_BYTES: usize = 64 * 1024;
const ROUTE_DROP_HOLD_TIMEOUT: Duration = Duration::from_secs(10);
const ROUTE_DROP_READ_BUFFER_BYTES: usize = 4 * 1024;

#[derive(Clone)]
struct LocalIngressAdmission {
    limits: LocalIngressAdmissionConfig,
    flow_idle_timeout: Option<Duration>,
    state: Arc<Mutex<LocalIngressAdmissionState>>,
}

#[derive(Default)]
struct LocalIngressAdmissionState {
    connections: usize,
    sources: HashMap<IpAddr, usize>,
    principals: HashMap<PrincipalId, usize>,
}

pub(in crate::runtime) struct LocalIngressAdmissionPermit {
    admission: LocalIngressAdmission,
    source: IpAddr,
    principal: Option<PrincipalId>,
}

#[cfg(test)]
pub(in crate::runtime) fn local_admission_permit_for_test(
    source: IpAddr,
) -> LocalIngressAdmissionPermit {
    LocalIngressAdmission::new(
        LocalIngressAdmissionConfig::default(),
        crate::config::ProductFlowConfig::default().idle_timeout,
    )
    .try_admit_source(source)
    .expect("default test local admission")
}

impl LocalIngressAdmission {
    fn new(limits: LocalIngressAdmissionConfig, flow_idle_timeout: Option<Duration>) -> Self {
        Self {
            limits,
            flow_idle_timeout,
            state: Arc::new(Mutex::new(LocalIngressAdmissionState::default())),
        }
    }

    fn try_admit_source(
        &self,
        source: IpAddr,
    ) -> Result<LocalIngressAdmissionPermit, &'static str> {
        let mut state = self.state.lock().expect("local admission lock");
        if state.connections >= self.limits.max_connections() {
            return Err("local listener connection limit reached");
        }
        if state.sources.get(&source).copied().unwrap_or(0)
            >= self.limits.max_connections_per_source()
        {
            return Err("local source connection limit reached");
        }
        state.connections += 1;
        *state.sources.entry(source).or_default() += 1;
        Ok(LocalIngressAdmissionPermit {
            admission: self.clone(),
            source,
            principal: None,
        })
    }
}

impl LocalIngressAdmissionPermit {
    fn admit_principal(&mut self, principal: &PrincipalId) -> Result<(), &'static str> {
        if self.principal.is_some() {
            return Err("local principal admission was already completed");
        }
        let mut state = self.admission.state.lock().expect("local admission lock");
        if state.principals.get(principal).copied().unwrap_or(0)
            >= self.admission.limits.max_connections_per_principal()
        {
            return Err("local principal connection limit reached");
        }
        *state.principals.entry(principal.clone()).or_default() += 1;
        self.principal = Some(principal.clone());
        Ok(())
    }
}

impl Drop for LocalIngressAdmissionPermit {
    fn drop(&mut self) {
        let mut state = self.admission.state.lock().expect("local admission lock");
        state.connections = state.connections.saturating_sub(1);
        decrement_admission_key(&mut state.sources, &self.source);
        if let Some(principal) = &self.principal {
            decrement_admission_key(&mut state.principals, principal);
        }
    }
}

fn decrement_admission_key<K>(counts: &mut HashMap<K, usize>, key: &K)
where
    K: Clone + Eq + std::hash::Hash,
{
    match counts.entry(key.clone()) {
        Entry::Occupied(mut entry) if *entry.get() > 1 => {
            *entry.get_mut() -= 1;
        }
        Entry::Occupied(entry) => {
            entry.remove();
        }
        Entry::Vacant(_) => {}
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the listener composition boundary transfers each independent Product owner"
)]
pub(super) async fn spawn_socks5_client_ingress(
    listen: Vec<SocketAddr>,
    mux_limits: MuxLimits,
    router: ClientIngressRouter,
    inbound: InboundId,
    proxy_auth: ProxyAuthConfig,
    admission: LocalIngressAdmissionConfig,
    flow_idle_timeout: Option<Duration>,
    readiness: RequiredServiceReadiness,
    services: &mut tokio::task::JoinSet<Result<(), RuntimeError>>,
) -> Result<(), RuntimeError> {
    let mut bound = Vec::with_capacity(listen.len());
    for addr in listen {
        bound.push(TcpListener::bind(addr).await?);
    }
    if bound.is_empty() {
        return Err(RuntimeError::Protocol(
            "SOCKS5 ingress has no listener tasks",
        ));
    }
    let admission = LocalIngressAdmission::new(admission, flow_idle_timeout);
    for listener in bound {
        let local_address = listener.local_addr()?;
        crate::observability::emit_lifecycle(
            crate::config::LogLevel::Info,
            "inbound",
            "listening",
            format_args!("{inbound}: SOCKS5 listening on {local_address}"),
        );
        let router = router.clone();
        let inbound = inbound.clone();
        let proxy_auth = proxy_auth.clone();
        let admission = admission.clone();
        services.spawn(async move {
            run_socks5_client_listener(listener, mux_limits, router, inbound, proxy_auth, admission)
                .await
        });
    }
    readiness.ready();
    Ok(())
}

async fn run_socks5_client_listener(
    listener: TcpListener,
    mux_limits: MuxLimits,
    router: ClientIngressRouter,
    inbound: InboundId,
    proxy_auth: ProxyAuthConfig,
    admission: LocalIngressAdmission,
) -> Result<(), RuntimeError> {
    let mut connections = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, source) = accepted?;
                stream.set_nodelay(true)?;
                let permit = match admission.try_admit_source(source.ip()) {
                    Ok(permit) => permit,
                    Err(_) => {
                        drop(stream);
                        continue;
                    }
                };
                let udp_relay_ip = stream.local_addr()?.ip();
                let router = router.clone();
                let inbound = inbound.clone();
                let proxy_auth = proxy_auth.clone();
                connections.spawn(async move {
                    if let Err(err) =
                        handle_socks5_client_stream_with_auth(
                        stream,
                        mux_limits,
                            router,
                            inbound,
                            source,
                            proxy_auth,
                            udp_relay_ip,
                            permit,
                        )
                        .await
                    {
                        crate::observability::process_event!(
                            Warn,
                            "socks5",
                            "client_failed",
                            "SOCKS5 client handler failed: {err}"
                        );
                    }
                });
            }
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                if let Err(err) = result {
                    crate::observability::process_event!(
                        Warn,
                        "socks5",
                        "client_task_failed",
                        "SOCKS5 client handler task failed: {err}"
                    );
                }
            }
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the listener composition boundary transfers each independent Product owner"
)]
pub(super) async fn spawn_mixed_client_ingress(
    listen: Vec<SocketAddr>,
    mux_limits: MuxLimits,
    router: ClientIngressRouter,
    inbound: InboundId,
    proxy_auth: ProxyAuthConfig,
    admission: LocalIngressAdmissionConfig,
    flow_idle_timeout: Option<Duration>,
    readiness: RequiredServiceReadiness,
    services: &mut tokio::task::JoinSet<Result<(), RuntimeError>>,
) -> Result<(), RuntimeError> {
    let mut bound = Vec::with_capacity(listen.len());
    for addr in listen {
        bound.push(TcpListener::bind(addr).await?);
    }
    if bound.is_empty() {
        return Err(RuntimeError::Protocol(
            "mixed proxy ingress has no listener tasks",
        ));
    }
    let admission = LocalIngressAdmission::new(admission, flow_idle_timeout);
    for listener in bound {
        let local_address = listener.local_addr()?;
        crate::observability::emit_lifecycle(
            crate::config::LogLevel::Info,
            "inbound",
            "listening",
            format_args!("{inbound}: mixed SOCKS5/HTTP CONNECT listening on {local_address}"),
        );
        let router = router.clone();
        let inbound = inbound.clone();
        let proxy_auth = proxy_auth.clone();
        let admission = admission.clone();
        services.spawn(async move {
            run_mixed_client_listener(listener, mux_limits, router, inbound, proxy_auth, admission)
                .await
        });
    }
    readiness.ready();
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MixedIngressProtocol {
    Socks5,
    HttpConnect,
}

fn classify_mixed_ingress_byte(first: u8) -> Result<MixedIngressProtocol, RuntimeError> {
    match first {
        0x05 => Ok(MixedIngressProtocol::Socks5),
        b'C' => Ok(MixedIngressProtocol::HttpConnect),
        _ => Err(RuntimeError::Protocol(
            "mixed proxy connection uses an unsupported protocol",
        )),
    }
}

async fn detect_mixed_ingress_protocol(
    stream: &TcpStream,
    handshake_deadline: tokio::time::Instant,
) -> Result<MixedIngressProtocol, RuntimeError> {
    let mut first = [0_u8; 1];
    let received = tokio::time::timeout_at(handshake_deadline, stream.peek(&mut first))
        .await
        .map_err(|_| RuntimeError::Protocol("mixed proxy protocol detection timed out"))??;
    if received == 0 {
        return Err(RuntimeError::Protocol(
            "mixed proxy connection closed before protocol detection",
        ));
    }
    classify_mixed_ingress_byte(first[0])
}

async fn run_mixed_client_listener(
    listener: TcpListener,
    mux_limits: MuxLimits,
    router: ClientIngressRouter,
    inbound: InboundId,
    proxy_auth: ProxyAuthConfig,
    admission: LocalIngressAdmission,
) -> Result<(), RuntimeError> {
    let mut connections = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, source) = accepted?;
                stream.set_nodelay(true)?;
                let permit = match admission.try_admit_source(source.ip()) {
                    Ok(permit) => permit,
                    Err(_) => {
                        drop(stream);
                        continue;
                    }
                };
                let router = router.clone();
                let inbound = inbound.clone();
                let proxy_auth = proxy_auth.clone();
                connections.spawn(async move {
                    if let Err(err) = handle_mixed_client_stream(
                        stream,
                        mux_limits,
                        router,
                        inbound,
                        source,
                        proxy_auth,
                        permit,
                    )
                    .await
                    {
                        crate::observability::process_event!(
                            Warn,
                            "mixed",
                            "client_failed",
                            "mixed proxy client handler failed: {err}"
                        );
                    }
                });
            }
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                if let Err(err) = result {
                    crate::observability::process_event!(
                        Warn,
                        "mixed",
                        "client_task_failed",
                        "mixed proxy client handler task failed: {err}"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
pub(in crate::runtime) async fn run_mixed_client_listener_for_test(
    listener: TcpListener,
    mux_limits: MuxLimits,
    router: ClientIngressRouter,
    inbound: InboundId,
    proxy_auth: ProxyAuthConfig,
    admission: LocalIngressAdmissionConfig,
) -> Result<(), RuntimeError> {
    run_mixed_client_listener(
        listener,
        mux_limits,
        router,
        inbound,
        proxy_auth,
        LocalIngressAdmission::new(
            admission,
            crate::config::ProductFlowConfig::default().idle_timeout,
        ),
    )
    .await
}

async fn handle_mixed_client_stream(
    stream: TcpStream,
    mux_limits: MuxLimits,
    router: ClientIngressRouter,
    inbound: InboundId,
    source: SocketAddr,
    proxy_auth: ProxyAuthConfig,
    permit: LocalIngressAdmissionPermit,
) -> Result<(), RuntimeError> {
    let handshake_deadline =
        tokio::time::Instant::now() + permit.admission.limits.handshake_timeout();
    let protocol = detect_mixed_ingress_protocol(&stream, handshake_deadline).await?;
    match protocol {
        MixedIngressProtocol::Socks5 => {
            let udp_relay_ip = stream.local_addr()?.ip();
            handle_socks5_client_stream_until(
                stream,
                mux_limits,
                router,
                inbound,
                source,
                proxy_auth,
                udp_relay_ip,
                permit,
                handshake_deadline,
            )
            .await
        }
        MixedIngressProtocol::HttpConnect => {
            handle_http_connect_client_stream_until(
                stream,
                router,
                inbound,
                source,
                proxy_auth,
                permit,
                handshake_deadline,
            )
            .await
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the listener composition boundary transfers each independent Product owner"
)]
pub(super) async fn spawn_http_connect_client_ingress(
    listen: Vec<SocketAddr>,
    router: ClientIngressRouter,
    inbound: InboundId,
    proxy_auth: ProxyAuthConfig,
    admission: LocalIngressAdmissionConfig,
    flow_idle_timeout: Option<Duration>,
    readiness: RequiredServiceReadiness,
    services: &mut tokio::task::JoinSet<Result<(), RuntimeError>>,
) -> Result<(), RuntimeError> {
    let mut bound = Vec::with_capacity(listen.len());
    for addr in listen {
        bound.push(TcpListener::bind(addr).await?);
    }
    if bound.is_empty() {
        return Err(RuntimeError::Protocol(
            "HTTP CONNECT ingress has no listener tasks",
        ));
    }
    let admission = LocalIngressAdmission::new(admission, flow_idle_timeout);
    for listener in bound {
        let local_address = listener.local_addr()?;
        crate::observability::emit_lifecycle(
            crate::config::LogLevel::Info,
            "inbound",
            "listening",
            format_args!("{inbound}: HTTP CONNECT listening on {local_address}"),
        );
        let router = router.clone();
        let inbound = inbound.clone();
        let proxy_auth = proxy_auth.clone();
        let admission = admission.clone();
        services.spawn(async move {
            run_http_connect_client_listener(listener, router, inbound, proxy_auth, admission).await
        });
    }
    readiness.ready();
    Ok(())
}

async fn run_http_connect_client_listener(
    listener: TcpListener,
    router: ClientIngressRouter,
    inbound: InboundId,
    proxy_auth: ProxyAuthConfig,
    admission: LocalIngressAdmission,
) -> Result<(), RuntimeError> {
    let mut connections = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, source) = accepted?;
                stream.set_nodelay(true)?;
                let permit = match admission.try_admit_source(source.ip()) {
                    Ok(permit) => permit,
                    Err(_) => {
                        drop(stream);
                        continue;
                    }
                };
                let router = router.clone();
                let inbound = inbound.clone();
                let proxy_auth = proxy_auth.clone();
                connections.spawn(async move {
                    if let Err(err) =
                        handle_http_connect_client_stream_with_auth(
                            stream,
                            router,
                            inbound,
                            source,
                            proxy_auth,
                            permit,
                        )
                        .await
                    {
                        crate::observability::process_event!(
                            Warn,
                            "http_connect",
                            "client_failed",
                            "HTTP CONNECT client handler failed: {err}"
                        );
                    }
                });
            }
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                if let Err(err) = result {
                    crate::observability::process_event!(
                        Warn,
                        "http_connect",
                        "client_task_failed",
                        "HTTP CONNECT client handler task failed: {err}"
                    );
                }
            }
        }
    }
}

pub(super) async fn spawn_tcp_forward_client_ingress(
    config: TcpForwardConfig,
    router: ClientIngressRouter,
    inbound: InboundId,
    flow_idle_timeout: Option<Duration>,
    readiness: RequiredServiceReadiness,
    services: &mut tokio::task::JoinSet<Result<(), RuntimeError>>,
) -> Result<(), RuntimeError> {
    let (listen, target, max_connections) = config.into_parts();
    let mut bound = Vec::with_capacity(listen.len());
    for address in listen {
        bound.push(TcpListener::bind(address).await?);
    }
    if bound.is_empty() {
        return Err(RuntimeError::Protocol(
            "TCP port-forward ingress has no listener tasks",
        ));
    }

    let target = Arc::new(target.into_target());
    let connection_slots = Arc::new(Semaphore::new(max_connections));
    for listener in bound {
        let local_address = listener.local_addr()?;
        crate::observability::emit_lifecycle(
            crate::config::LogLevel::Info,
            "inbound",
            "listening",
            format_args!("{inbound}: TCP forwarding listening on {local_address}"),
        );
        services.spawn(run_tcp_forward_client_listener(
            listener,
            target.clone(),
            router.clone(),
            inbound.clone(),
            connection_slots.clone(),
            flow_idle_timeout,
        ));
    }
    readiness.ready();
    Ok(())
}

pub(super) async fn run_tcp_forward_client_listener(
    listener: TcpListener,
    target: Arc<TargetAddr>,
    router: ClientIngressRouter,
    inbound: InboundId,
    connection_slots: Arc<Semaphore>,
    flow_idle_timeout: Option<Duration>,
) -> Result<(), RuntimeError> {
    let mut connections = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (mut stream, source) = accepted?;
                stream.set_nodelay(true)?;
                let permit = match connection_slots.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(tokio::sync::TryAcquireError::NoPermits) => {
                        let _ = stream.shutdown().await;
                        continue;
                    }
                    Err(tokio::sync::TryAcquireError::Closed) => {
                        return Err(RuntimeError::Protocol(
                            "TCP port-forward connection limiter closed"
                        ));
                    }
                };
                let target = target.clone();
                let router = router.clone();
                let inbound = inbound.clone();
                connections.spawn(async move {
                    let _permit = permit;
                    if let Err(error) =
                        handle_tcp_forward_client_stream(
                            stream,
                            target,
                            router,
                            inbound,
                            source,
                            flow_idle_timeout,
                        )
                            .await
                    {
                        crate::observability::process_event!(
                            Warn,
                            "tcp_forward",
                            "client_failed",
                            "TCP port-forward client handler failed: {error}"
                        );
                    }
                });
            }
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                if let Err(error) = result {
                    crate::observability::process_event!(
                        Warn,
                        "tcp_forward",
                        "client_task_failed",
                        "TCP port-forward client handler task failed: {error}"
                    );
                }
            }
        }
    }
}

pub(super) async fn handle_tcp_forward_client_stream<S>(
    mut stream: S,
    target: Arc<TargetAddr>,
    router: ClientIngressRouter,
    inbound: InboundId,
    source: SocketAddr,
    flow_idle_timeout: Option<Duration>,
) -> Result<(), RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let principal = PrincipalId::parse("anonymous")
        .map_err(|_| RuntimeError::Protocol("invalid fixed port-forward principal"))?;
    let plan = match router.route_tcp(target.as_ref(), source, principal, inbound)? {
        ClientRoute::Open(plan) => plan,
        ClientRoute::Deny(ClientPolicyDisposition::Reject) => {
            return Ok(());
        }
        ClientRoute::Deny(ClientPolicyDisposition::Drop) => {
            hold_silent_route_drop(&mut stream).await;
            return Ok(());
        }
    };
    let opened = match plan.open_tcp(target.as_ref()).await {
        Ok(opened) => opened,
        Err(RuntimeError::RouteRejected) => return Ok(()),
        Err(RuntimeError::RouteDropped) => {
            hold_silent_route_drop(&mut stream).await;
            return Ok(());
        }
        Err(RuntimeError::OutboundUnavailable(_)) => return Ok(()),
        Err(error) => return Err(error),
    };
    relay_opened_tcp(stream, opened, flow_idle_timeout).await
}

pub(super) async fn spawn_udp_forward_client_ingress(
    config: UdpForwardConfig,
    mux_limits: MuxLimits,
    router: ClientIngressRouter,
    inbound: InboundId,
    flow_idle_timeout: Option<Duration>,
    readiness: RequiredServiceReadiness,
    services: &mut tokio::task::JoinSet<Result<(), RuntimeError>>,
) -> Result<(), RuntimeError> {
    let (listen, target, max_associations, idle_timeout, datagram_ttl_ms) = config.into_parts();
    let mut bound = Vec::with_capacity(listen.len());
    for address in listen {
        bound.push(UdpSocket::bind(address).await?);
    }
    if bound.is_empty() {
        return Err(RuntimeError::Protocol(
            "UDP port-forward ingress has no listener tasks",
        ));
    }

    let target = Arc::new(target.into_target());
    let association_slots = Arc::new(Semaphore::new(max_associations));
    for socket in bound {
        let local_address = socket.local_addr()?;
        crate::observability::emit_lifecycle(
            crate::config::LogLevel::Info,
            "inbound",
            "listening",
            format_args!("{inbound}: UDP forwarding listening on {local_address}"),
        );
        services.spawn(run_udp_forward_client_socket(
            socket,
            target.clone(),
            mux_limits,
            router.clone(),
            inbound.clone(),
            association_slots.clone(),
            idle_timeout,
            datagram_ttl_ms,
            flow_idle_timeout,
        ));
    }
    readiness.ready();
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct UdpForwardLaneKey {
    peer: SocketAddr,
    generation: u64,
}

struct UdpForwardAssociation {
    generation: u64,
    last_activity: tokio::time::Instant,
    scheduled_expiry: tokio::time::Instant,
    plan: Option<crate::runtime::product_policy::ClientOutboundPlan>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_udp_forward_client_socket(
    socket: UdpSocket,
    target: Arc<TargetAddr>,
    mux_limits: MuxLimits,
    router: ClientIngressRouter,
    inbound: InboundId,
    association_slots: Arc<Semaphore>,
    idle_timeout: Duration,
    datagram_ttl_ms: u32,
    flow_idle_timeout: Option<Duration>,
) -> Result<(), RuntimeError> {
    const UDP_PACKET_BUFFER_BYTES: usize = u16::MAX as usize;

    let principal = PrincipalId::parse("anonymous")
        .map_err(|_| RuntimeError::Protocol("invalid fixed port-forward principal"))?;
    let mut packet = vec![0u8; UDP_PACKET_BUFFER_BYTES];
    let (completion_tx, mut completion_rx) = mpsc::channel::<UdpEdgeCompletion<UdpForwardLaneKey>>(
        udp_edge_completion_queue(mux_limits),
    );
    let mut lanes = Vec::<UdpEdgeLane<UdpForwardLaneKey>>::new();
    let mut associations = HashMap::<SocketAddr, UdpForwardAssociation>::new();
    let mut expirations = BinaryHeap::<Reverse<(tokio::time::Instant, SocketAddr, u64)>>::new();
    let mut next_lane_id = 0usize;
    let mut next_generation = 0u64;
    let mut route_error_reported = false;
    let mut last_send_warning = None::<tokio::time::Instant>;

    let result = loop {
        let next_expiry = expirations.peek().map(|entry| entry.0.0);
        tokio::select! {
            received = socket.recv_from(&mut packet) => {
                let (length, peer) = match received {
                    Ok(received) => received,
                    Err(error) => break Err(RuntimeError::Io(error)),
                };
                let now = tokio::time::Instant::now();
                expire_udp_forward_associations(
                    &mut associations,
                    &mut expirations,
                    &mut lanes,
                    now,
                    idle_timeout,
                );
                if length > mux_limits.max_payload_bytes {
                    continue;
                }

                let association = match associations.entry(peer) {
                    Entry::Occupied(mut occupied) => {
                        occupied.get_mut().last_activity = now;
                        occupied.into_mut()
                    }
                    Entry::Vacant(vacant) => {
                        let Ok(permit) = association_slots.clone().try_acquire_owned() else {
                            continue;
                        };
                        next_generation = match next_generation.checked_add(1) {
                            Some(generation) => generation,
                            None => break Err(RuntimeError::Protocol(
                                "UDP port-forward association generation exhausted",
                            )),
                        };
                        let plan = match router.route_udp(
                            target.as_ref(),
                            peer,
                            principal.clone(),
                            inbound.clone(),
                        ) {
                            Ok(ClientRoute::Open(plan)) => Some(plan),
                            Ok(ClientRoute::Deny(_)) => None,
                            Err(error) => {
                                if !route_error_reported {
                                    crate::observability::process_event!(
                                        Warn,
                                        "udp_forward",
                                        "route_failed",
                                        "UDP port-forward route for source {peer} failed: {error}"
                                    );
                                    route_error_reported = true;
                                }
                                None
                            }
                        };
                        let scheduled_expiry = now + idle_timeout;
                        expirations.push(Reverse((scheduled_expiry, peer, next_generation)));
                        vacant.insert(UdpForwardAssociation {
                            generation: next_generation,
                            last_activity: now,
                            scheduled_expiry,
                            plan,
                            _permit: permit,
                        })
                    }
                };
                let Some(plan) = association.plan.as_ref() else {
                    continue;
                };
                let lane_key = UdpForwardLaneKey {
                    peer,
                    generation: association.generation,
                };
                let _ = dispatch_udp_edge_request_with_idle_timeout(
                    &mut lanes,
                    &mut next_lane_id,
                    plan,
                    mux_limits,
                    flow_idle_timeout,
                    &completion_tx,
                    UdpEdgeRequest {
                        target: target.as_ref().clone(),
                        payload: bytes::Bytes::copy_from_slice(&packet[..length]),
                        ttl_ms: datagram_ttl_ms,
                        metadata: lane_key,
                    },
                );
            }
            completion = completion_rx.recv() => {
                let Some(completion) = completion else {
                    break Err(RuntimeError::Protocol(
                        "UDP port-forward completion channel closed",
                    ));
                };
                finish_udp_edge_completion(&mut lanes, &completion);
                match completion {
                    UdpEdgeCompletion::Received { target: received_target, metadata, payload } => {
                        let is_current = associations
                            .get_mut(&metadata.peer)
                            .is_some_and(|association| {
                                let current = association.generation == metadata.generation;
                                if current && received_target == *target {
                                    association.last_activity = tokio::time::Instant::now();
                                }
                                current
                            });
                        if is_current
                            && received_target == *target
                            && let Err(error) = socket.send_to(&payload, metadata.peer).await
                        {
                            break Err(RuntimeError::Io(error));
                        }
                    }
                    UdpEdgeCompletion::Sent {
                        lane_id,
                        result: Err(error),
                        ..
                    } if matches!(error.as_ref(), RuntimeError::OutboundUnavailable(_)) => {
                        let _ = reap_finished_udp_edge_lane_instance(&mut lanes, lane_id);
                    }
                    UdpEdgeCompletion::Sent {
                        metadata,
                        result: Err(error),
                        ..
                    } => {
                        let is_current = associations
                            .get(&metadata.peer)
                            .is_some_and(|association| {
                                association.generation == metadata.generation
                            });
                        let now = tokio::time::Instant::now();
                        let warning_due = last_send_warning.is_none_or(|previous| {
                            now.saturating_duration_since(previous) >= Duration::from_secs(10)
                        });
                        if is_current && warning_due {
                            crate::observability::process_event!(
                                Warn,
                                "udp_forward",
                                "datagram_failed",
                                "UDP port-forward datagram from {} failed: {error}",
                                metadata.peer
                            );
                            last_send_warning = Some(now);
                        }
                    }
                    UdpEdgeCompletion::Sent { result: Ok(()), .. } => {}
                    UdpEdgeCompletion::Discarded { .. } => {}
                }
            }
            _ = async {
                match next_expiry {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => pending::<()>().await,
                }
            } => {
                expire_udp_forward_associations(
                    &mut associations,
                    &mut expirations,
                    &mut lanes,
                    tokio::time::Instant::now(),
                    idle_timeout,
                );
            }
        }
    };

    drop(completion_tx);
    close_udp_edge_lanes(lanes).await;
    result
}

fn expire_udp_forward_associations(
    associations: &mut HashMap<SocketAddr, UdpForwardAssociation>,
    expirations: &mut BinaryHeap<Reverse<(tokio::time::Instant, SocketAddr, u64)>>,
    lanes: &mut Vec<UdpEdgeLane<UdpForwardLaneKey>>,
    now: tokio::time::Instant,
    idle_timeout: Duration,
) {
    while let Some(Reverse((scheduled_expiry, peer, generation))) = expirations.peek().copied() {
        if scheduled_expiry > now {
            break;
        }
        expirations.pop();
        let Some(association) = associations.get_mut(&peer) else {
            continue;
        };
        if association.generation != generation || association.scheduled_expiry != scheduled_expiry
        {
            continue;
        }
        let actual_expiry = association.last_activity + idle_timeout;
        if actual_expiry > now {
            association.scheduled_expiry = actual_expiry;
            expirations.push(Reverse((actual_expiry, peer, generation)));
            continue;
        }

        associations.remove(&peer);
        remove_udp_edge_lane(lanes, &UdpForwardLaneKey { peer, generation });
    }
}

#[cfg(test)]
pub(super) async fn handle_socks5_client_stream<S>(
    stream: S,
    context: ClientPathContext,
) -> Result<(), RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let proxy_auth = context.proxy_auth.clone();
    let router =
        ClientIngressRouter::single_for_test(context.clone(), MppPerformanceConfig::default())?;
    handle_socks5_client_stream_with_auth(
        stream,
        context.mux_limits,
        router,
        InboundId::parse("socks-test")
            .map_err(|_| RuntimeError::Protocol("invalid test inbound"))?,
        SocketAddr::from(([127, 0, 0, 1], 40_000)),
        proxy_auth,
        IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        local_admission_permit_for_test(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
    )
    .await
}

#[allow(
    clippy::too_many_arguments,
    reason = "the accepted-connection actor keeps immutable ingress identity and admission ownership explicit"
)]
pub(super) async fn handle_socks5_client_stream_with_auth<S>(
    stream: S,
    mux_limits: MuxLimits,
    router: ClientIngressRouter,
    inbound: InboundId,
    source: SocketAddr,
    proxy_auth: ProxyAuthConfig,
    udp_relay_ip: IpAddr,
    admission_permit: LocalIngressAdmissionPermit,
) -> Result<(), RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let handshake_deadline =
        tokio::time::Instant::now() + admission_permit.admission.limits.handshake_timeout();
    handle_socks5_client_stream_until(
        stream,
        mux_limits,
        router,
        inbound,
        source,
        proxy_auth,
        udp_relay_ip,
        admission_permit,
        handshake_deadline,
    )
    .await
}

#[allow(
    clippy::too_many_arguments,
    reason = "the accepted-connection actor keeps immutable ingress identity, admission, and its absolute handshake deadline explicit"
)]
async fn handle_socks5_client_stream_until<S>(
    mut stream: S,
    mux_limits: MuxLimits,
    router: ClientIngressRouter,
    inbound: InboundId,
    source: SocketAddr,
    proxy_auth: ProxyAuthConfig,
    udp_relay_ip: IpAddr,
    mut admission_permit: LocalIngressAdmissionPermit,
    handshake_deadline: tokio::time::Instant,
) -> Result<(), RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let principal = tokio::time::timeout_at(
        handshake_deadline,
        authenticate_socks5_client(&mut stream, &proxy_auth),
    )
    .await
    .map_err(|_| RuntimeError::Protocol("SOCKS5 authentication timed out"))??;
    admission_permit
        .admit_principal(&principal)
        .map_err(RuntimeError::Protocol)?;
    let request = tokio::time::timeout_at(handshake_deadline, read_socks5_command(&mut stream))
        .await
        .map_err(|_| RuntimeError::Protocol("SOCKS5 request header timed out"))??;
    match request.command {
        socks5::Socks5Command::Connect => {
            let target = request.target;
            let route = router.route_tcp(&target, source, principal, inbound)?;
            let plan = match route {
                ClientRoute::Open(plan) => plan,
                ClientRoute::Deny(disposition) => {
                    return apply_socks5_policy_disposition(&mut stream, disposition).await;
                }
            };
            let opened = match plan.open_tcp(&target).await {
                Ok(opened) => opened,
                Err(RuntimeError::RouteRejected) => {
                    return apply_socks5_policy_disposition(
                        &mut stream,
                        ClientPolicyDisposition::Reject,
                    )
                    .await;
                }
                Err(RuntimeError::RouteDropped) => {
                    return apply_socks5_policy_disposition(
                        &mut stream,
                        ClientPolicyDisposition::Drop,
                    )
                    .await;
                }
                Err(err @ RuntimeError::OutboundUnavailable(_)) => {
                    stream
                        .write_all(&socks5::connect_reply(
                            Socks5Reply::NetworkUnreachable,
                            SocketAddr::from(([0, 0, 0, 0], 0)),
                        ))
                        .await?;
                    return Err(err);
                }
                Err(err) => {
                    stream
                        .write_all(&socks5::connect_reply(
                            Socks5Reply::GeneralFailure,
                            SocketAddr::from(([0, 0, 0, 0], 0)),
                        ))
                        .await?;
                    return Err(err);
                }
            };
            let result = async {
                stream
                    .write_all(&socks5::connect_reply(
                        Socks5Reply::Succeeded,
                        SocketAddr::from(([0, 0, 0, 0], 0)),
                    ))
                    .await?;
                stream.flush().await?;
                relay_opened_tcp(stream, opened, admission_permit.admission.flow_idle_timeout).await
            }
            .await;
            result.map(|_| ())
        }
        socks5::Socks5Command::UdpAssociate => {
            let policy = SocksUdpAssociationPolicy {
                router,
                inbound,
                source,
                principal,
                idle_timeout: admission_permit.admission.flow_idle_timeout,
            };
            handle_socks5_udp_associate(
                &mut stream,
                mux_limits,
                policy,
                socks5::UdpAssociateRequest {
                    client_endpoint: request.target,
                },
                udp_relay_ip,
            )
            .await
        }
    }
}

#[cfg(test)]
pub(super) async fn handle_http_connect_client_stream<S>(
    stream: S,
    context: ClientPathContext,
) -> Result<(), RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let proxy_auth = context.proxy_auth.clone();
    let router = ClientIngressRouter::single_for_test(context, MppPerformanceConfig::default())?;
    handle_http_connect_client_stream_with_auth(
        stream,
        router,
        InboundId::parse("http-test")
            .map_err(|_| RuntimeError::Protocol("invalid test inbound"))?,
        SocketAddr::from(([127, 0, 0, 1], 40_000)),
        proxy_auth,
        local_admission_permit_for_test(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
    )
    .await
}

pub(super) async fn handle_http_connect_client_stream_with_auth<S>(
    stream: S,
    router: ClientIngressRouter,
    inbound: InboundId,
    source: SocketAddr,
    proxy_auth: ProxyAuthConfig,
    admission_permit: LocalIngressAdmissionPermit,
) -> Result<(), RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let handshake_deadline =
        tokio::time::Instant::now() + admission_permit.admission.limits.handshake_timeout();
    handle_http_connect_client_stream_until(
        stream,
        router,
        inbound,
        source,
        proxy_auth,
        admission_permit,
        handshake_deadline,
    )
    .await
}

#[allow(
    clippy::too_many_arguments,
    reason = "the accepted-connection actor keeps immutable ingress identity, admission, and its absolute handshake deadline explicit"
)]
async fn handle_http_connect_client_stream_until<S>(
    mut stream: S,
    router: ClientIngressRouter,
    inbound: InboundId,
    source: SocketAddr,
    proxy_auth: ProxyAuthConfig,
    mut admission_permit: LocalIngressAdmissionPermit,
    handshake_deadline: tokio::time::Instant,
) -> Result<(), RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let request = tokio::time::timeout_at(handshake_deadline, read_http_connect(&mut stream))
        .await
        .map_err(|_| RuntimeError::Protocol("HTTP CONNECT request header timed out"))??;
    let principal = if proxy_auth.is_required() {
        match proxy_auth.authenticate_basic_header(request.proxy_authorization.as_deref()) {
            Some(principal) => principal,
            None => {
                stream
                    .write_all(http_connect::error_response(
                        HttpStatus::ProxyAuthenticationRequired,
                    ))
                    .await?;
                return Err(RuntimeError::Protocol("HTTP proxy authentication failed"));
            }
        }
    } else {
        PrincipalId::parse("anonymous")
            .map_err(|_| RuntimeError::Protocol("invalid anonymous principal"))?
    };
    admission_permit
        .admit_principal(&principal)
        .map_err(RuntimeError::Protocol)?;
    let target = request.target;
    let route = router.route_tcp(&target, source, principal, inbound)?;
    let plan = match route {
        ClientRoute::Open(plan) => plan,
        ClientRoute::Deny(disposition) => {
            return apply_http_policy_disposition(&mut stream, disposition).await;
        }
    };
    let opened = match plan.open_tcp(&target).await {
        Ok(opened) => opened,
        Err(RuntimeError::RouteRejected) => {
            return apply_http_policy_disposition(&mut stream, ClientPolicyDisposition::Reject)
                .await;
        }
        Err(RuntimeError::RouteDropped) => {
            return apply_http_policy_disposition(&mut stream, ClientPolicyDisposition::Drop).await;
        }
        Err(err @ RuntimeError::OutboundUnavailable(_)) => {
            stream
                .write_all(http_connect::error_response(HttpStatus::ServiceUnavailable))
                .await?;
            return Err(err);
        }
        Err(err) => {
            stream
                .write_all(http_connect::error_response(HttpStatus::BadGateway))
                .await?;
            return Err(err);
        }
    };
    let result = async {
        stream.write_all(http_connect::success_response()).await?;
        stream.flush().await?;
        relay_opened_tcp(stream, opened, admission_permit.admission.flow_idle_timeout).await
    }
    .await;
    result.map(|_| ())
}

async fn apply_socks5_policy_disposition<S>(
    stream: &mut S,
    disposition: ClientPolicyDisposition,
) -> Result<(), RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match disposition {
        ClientPolicyDisposition::Reject => {
            stream
                .write_all(&socks5::connect_reply(
                    Socks5Reply::ConnectionNotAllowed,
                    SocketAddr::from(([0, 0, 0, 0], 0)),
                ))
                .await?;
            stream.flush().await?;
        }
        ClientPolicyDisposition::Drop => hold_silent_route_drop(stream).await,
    }
    Ok(())
}

async fn apply_http_policy_disposition<S>(
    stream: &mut S,
    disposition: ClientPolicyDisposition,
) -> Result<(), RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match disposition {
        ClientPolicyDisposition::Reject => {
            stream
                .write_all(http_connect::error_response(HttpStatus::Forbidden))
                .await?;
            stream.flush().await?;
        }
        ClientPolicyDisposition::Drop => hold_silent_route_drop(stream).await,
    }
    Ok(())
}

pub(super) async fn hold_silent_route_drop<S>(stream: &mut S)
where
    S: AsyncRead + Unpin,
{
    let discard = async {
        let mut buffer = [0u8; ROUTE_DROP_READ_BUFFER_BYTES];
        loop {
            match stream.read(&mut buffer).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
        }
    };
    let _ = tokio::time::timeout(ROUTE_DROP_HOLD_TIMEOUT, discard).await;
}

pub(super) const DEFAULT_SOCKS5_UDP_TTL_MS: u32 = 30_000;

#[derive(Debug, Clone, PartialEq, Eq)]
struct SocksUdpLaneKey {
    peer: SocketAddr,
    // Stable index into the bounded target route cache; avoids cloning domain
    // names on the per-payload path while keeping target lanes isolated.
    target_slot: usize,
}

struct SocksUdpTargetRoute {
    target: TargetAddr,
    binding: Option<SocksUdpRouteBinding>,
}

struct SocksUdpRouteBinding {
    plan: crate::runtime::product_policy::ClientOutboundPlan,
}

struct SocksUdpAssociationPolicy {
    router: ClientIngressRouter,
    inbound: InboundId,
    source: SocketAddr,
    principal: PrincipalId,
    idle_timeout: Option<Duration>,
}

fn resolve_socks_udp_target_route(
    routes: &mut Vec<SocksUdpTargetRoute>,
    route_limit: usize,
    target: &TargetAddr,
    policy: &SocksUdpAssociationPolicy,
) -> Result<Option<usize>, RuntimeError> {
    if let Some(position) = routes.iter().position(|entry| entry.target == *target) {
        return Ok(Some(position));
    }
    if routes.len() >= route_limit {
        return Ok(None);
    }
    let binding = match policy.router.route_udp(
        target,
        policy.source,
        policy.principal.clone(),
        policy.inbound.clone(),
    ) {
        Ok(ClientRoute::Open(plan)) => Some(SocksUdpRouteBinding { plan }),
        Ok(ClientRoute::Deny(_)) | Err(RuntimeError::DestinationDenied(_)) => None,
        Err(error) => return Err(error),
    };
    routes.push(SocksUdpTargetRoute {
        target: target.clone(),
        binding,
    });
    Ok(Some(routes.len() - 1))
}

#[cfg(test)]
#[path = "tests_ingress_runtime.rs"]
mod tests;
async fn handle_socks5_udp_associate<S>(
    stream: &mut S,
    mux_limits: MuxLimits,
    policy: SocksUdpAssociationPolicy,
    request: socks5::UdpAssociateRequest,
    relay_ip: IpAddr,
) -> Result<(), RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut peer_binding =
        match Socks5UdpPeerBinding::new(policy.source.ip(), &request.client_endpoint) {
            Ok(binding) => binding,
            Err(reason) => {
                stream
                    .write_all(&socks5::connect_reply(
                        Socks5Reply::ConnectionNotAllowed,
                        SocketAddr::new(relay_ip, 0),
                    ))
                    .await?;
                stream.flush().await?;
                return Err(RuntimeError::Protocol(reason));
            }
        };
    let relay_socket = UdpSocket::bind(socks5_udp_relay_bind_addr(relay_ip)).await?;
    let relay_addr = relay_socket.local_addr()?;
    stream
        .write_all(&socks5::connect_reply(Socks5Reply::Succeeded, relay_addr))
        .await?;
    stream.flush().await?;

    let mut packet = vec![0u8; local_udp_buffer_len(mux_limits)];
    let mut control_probe = [0u8; 1];
    let (completion_tx, mut completion_rx) =
        mpsc::channel::<UdpEdgeCompletion<SocksUdpLaneKey>>(udp_edge_completion_queue(mux_limits));
    let mut lanes = Vec::<UdpEdgeLane<SocksUdpLaneKey>>::new();
    let mut next_lane_id = 0usize;
    let route_limit = mux_limits
        .max_streams
        .min(crate::runtime::datagram::udp_edge_queue_slots(mux_limits))
        .max(1);
    let mut routes = Vec::<SocksUdpTargetRoute>::new();
    let activity = ProductFlowActivity::new();
    let idle = activity.wait_until_idle(policy.idle_timeout);
    tokio::pin!(idle);
    let result = loop {
        tokio::select! {
            read = stream.read(&mut control_probe) => {
                let read = match read {
                    Ok(read) => read,
                    Err(err) => break Err(RuntimeError::Io(err)),
                };
                if read == 0 {
                    break Ok(());
                }
                break Err(RuntimeError::Protocol("unexpected data on SOCKS5 UDP control stream"));
            }
            received = relay_socket.recv_from(&mut packet) => {
                let (len, peer) = match received {
                    Ok(received) => received,
                    Err(err) => break Err(RuntimeError::Io(err)),
                };
                if !peer_binding.accept(peer) {
                    // A UDP relay can be reachable by more than the associated
                    // client. Foreign packets are untrusted noise: they cannot
                    // bind the wildcard port or tear down the authenticated
                    // TCP-owned association.
                    continue;
                }
                let (datagram, consumed) = match socks5::parse_udp_datagram(&packet[..len]) {
                    Ok(parsed) => parsed,
                    Err(err) => break Err(RuntimeError::Socks5(err)),
                };
                if consumed != len {
                    break Err(RuntimeError::Protocol("trailing SOCKS5 UDP datagram bytes"));
                }
                activity.record();
                let socks5::UdpDatagram { target, payload } = datagram;
                let route_position = match resolve_socks_udp_target_route(
                    &mut routes,
                    route_limit,
                    &target,
                    &policy,
                ) {
                    Ok(Some(position)) => position,
                    Ok(None) => {
                        crate::observability::process_event!(
                            Warn,
                            "socks5_udp",
                            "target_limit",
                            "SOCKS5 UDP target limit reached; dropping datagram from {peer}"
                        );
                        continue;
                    }
                    Err(error) => break Err(error),
                };
                let Some(binding) = routes[route_position].binding.as_mut() else {
                    // SOCKS UDP is connectionless: every Product deny outcome is a silent drop.
                    continue;
                };
                let lane_key = SocksUdpLaneKey {
                    peer,
                    target_slot: route_position,
                };
                if dispatch_udp_edge_request_with_idle_timeout(
                    &mut lanes,
                    &mut next_lane_id,
                    &binding.plan,
                    mux_limits,
                    policy.idle_timeout,
                    &completion_tx,
                    UdpEdgeRequest {
                        target,
                        payload,
                        ttl_ms: DEFAULT_SOCKS5_UDP_TTL_MS,
                        metadata: lane_key,
                    },
                )
                .is_err()
                {
                    crate::observability::process_event!(
                        Warn,
                        "socks5_udp",
                        "queue_full",
                        "SOCKS5 UDP lane queue full; dropping datagram from {peer}"
                    );
                }
            }
            completion = completion_rx.recv() => {
                let Some(completion) = completion else {
                    break Err(RuntimeError::Protocol("SOCKS5 UDP completion channel closed"));
                };
                finish_udp_edge_completion(&mut lanes, &completion);
                match completion {
                    UdpEdgeCompletion::Received { target, metadata, payload } => {
                        activity.record();
                        let response_packet = match socks5::udp_datagram(&target, &payload) {
                            Ok(packet) => packet,
                            Err(err) => break Err(RuntimeError::Socks5(err)),
                        };
                        if let Err(err) = relay_socket.send_to(&response_packet, metadata.peer).await {
                            break Err(RuntimeError::Io(err));
                        }
                    }
                    UdpEdgeCompletion::Sent {
                        lane_id,
                        result: Err(error),
                        ..
                    } if matches!(error.as_ref(), RuntimeError::OutboundUnavailable(_)) => {
                        let _ = reap_finished_udp_edge_lane_instance(&mut lanes, lane_id);
                    }
                    UdpEdgeCompletion::Sent {
                        target,
                        result: Err(err),
                        ..
                    } => {
                        crate::observability::process_event!(
                            Warn,
                            "socks5_udp",
                            "datagram_failed",
                            "SOCKS5 UDP datagram to {:?} failed: {err}",
                            target
                        );
                    }
                    UdpEdgeCompletion::Sent { result: Ok(()), .. } => {}
                    UdpEdgeCompletion::Discarded { .. } => {}
                }
            }
            () = &mut idle => break Ok(()),
        }
    };
    drop(completion_tx);
    close_udp_edge_lanes(lanes).await;
    result
}

pub(super) fn socks5_udp_relay_bind_addr(control_ip: IpAddr) -> SocketAddr {
    // The accepted TCP local address identifies the interface and family the
    // client can already reach; UDP association must preserve that boundary.
    SocketAddr::new(control_ip, 0)
}

pub(super) fn local_udp_buffer_len(mux_limits: MuxLimits) -> usize {
    const SOCKS5_UDP_HEADER_BUDGET_BYTES: usize = 512;
    const SOCKS5_UDP_PACKET_BUFFER_BYTES: usize = u16::MAX as usize;
    mux_limits
        .max_payload_bytes
        .saturating_add(SOCKS5_UDP_HEADER_BUDGET_BYTES)
        .clamp(
            SOCKS5_UDP_HEADER_BUDGET_BYTES,
            SOCKS5_UDP_PACKET_BUFFER_BYTES,
        )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Socks5UdpPeerBinding {
    control_ip: IpAddr,
    port: Option<u16>,
}

impl Socks5UdpPeerBinding {
    pub(super) fn new(
        control_ip: IpAddr,
        requested_endpoint: &TargetAddr,
    ) -> Result<Self, &'static str> {
        let TargetAddr::Ip(requested) = requested_endpoint else {
            return Err("SOCKS5 UDP client endpoint must be an IP address");
        };
        if !requested.ip().is_unspecified() && requested.ip() != control_ip {
            return Err("SOCKS5 UDP client endpoint does not match the TCP control peer");
        }
        Ok(Self {
            control_ip,
            port: (requested.port() != 0).then_some(requested.port()),
        })
    }

    pub(super) fn accept(&mut self, peer: SocketAddr) -> bool {
        if peer.ip() != self.control_ip {
            return false;
        }
        match self.port {
            Some(port) => peer.port() == port,
            None => {
                self.port = Some(peer.port());
                true
            }
        }
    }
}

#[cfg(test)]
pub(super) async fn probe_tcp_client_path(
    context: &ClientPathContext,
    path_index: usize,
    timeout: Duration,
) {
    let Some(durable_path_session) = context.tcp_sessions.get(path_index) else {
        return;
    };
    let probe_deadline = tokio::time::Instant::now() + timeout;
    // The durable carrier owns native liveness and heartbeat evidence.
    // Preparing an absent carrier is useful; opening another socket must not
    // change the health of a different live carrier instance.
    let _ = durable_path_session
        .prepare_connection(probe_deadline)
        .await;
}

pub(super) async fn probe_udp_client_path(
    context: &ClientPathContext,
    path_index: usize,
    timeout: Duration,
) -> Result<Option<(CarrierPathInstanceId, Duration)>, RuntimeError> {
    let durable_path_session = context
        .udp_sessions
        .get(path_index)
        .ok_or(RuntimeError::NoSchedulableUdpPath)?;
    let probe_deadline = tokio::time::Instant::now() + timeout;
    // Cold validation prepares the authenticated Product carrier. Once live,
    // QUIC's own connection RTT and liveness remain the only carrier evidence.
    durable_path_session
        .prepare_connection_for_probe(probe_deadline)
        .await
}

pub(super) async fn read_socks5_auth<S>(stream: &mut S) -> Result<socks5::AuthRequest, RuntimeError>
where
    S: AsyncRead + Unpin,
{
    let mut prefix = [0u8; 2];
    stream.read_exact(&mut prefix).await?;
    let method_count = prefix[1] as usize;
    let mut request = Vec::with_capacity(2 + method_count);
    request.extend_from_slice(&prefix);
    request.resize(2 + method_count, 0);
    stream.read_exact(&mut request[2..]).await?;
    let (auth, consumed) = socks5::parse_auth_request(&request)?;
    if consumed != request.len() {
        return Err(RuntimeError::Protocol("trailing SOCKS5 auth bytes"));
    }
    Ok(auth)
}

pub(super) async fn authenticate_socks5_client<S>(
    stream: &mut S,
    proxy_auth: &ProxyAuthConfig,
) -> Result<PrincipalId, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let auth = read_socks5_auth(stream).await?;
    if proxy_auth.is_required() {
        if !auth.supports_username_password() {
            stream
                .write_all(&socks5::no_acceptable_methods_response())
                .await?;
            return Err(RuntimeError::Protocol(
                "SOCKS5 client did not offer username/password auth",
            ));
        }
        stream
            .write_all(&socks5::username_password_method_response())
            .await?;
        let credentials = read_socks5_username_password_auth(stream).await?;
        let principal = proxy_auth.authenticate(&credentials.username, &credentials.password);
        let accepted = principal.is_some();
        stream
            .write_all(&socks5::username_password_auth_response(accepted))
            .await?;
        if !accepted {
            return Err(RuntimeError::Protocol("SOCKS5 proxy authentication failed"));
        }
        return Ok(principal.expect("accepted local proxy authentication has principal"));
    }

    if !auth.supports_no_auth() {
        stream
            .write_all(&socks5::no_acceptable_methods_response())
            .await?;
        return Err(RuntimeError::Socks5(Socks5Error::UnsupportedCommand(0)));
    }
    stream.write_all(&socks5::no_auth_response()).await?;
    PrincipalId::parse("anonymous")
        .map_err(|_| RuntimeError::Protocol("invalid anonymous principal"))
}

pub(super) async fn read_socks5_username_password_auth<S>(
    stream: &mut S,
) -> Result<socks5::UsernamePasswordAuthRequest, RuntimeError>
where
    S: AsyncRead + Unpin,
{
    let mut prefix = [0u8; 2];
    stream.read_exact(&mut prefix).await?;
    let username_len = prefix[1] as usize;
    let mut request = Vec::with_capacity(2 + username_len + 1);
    request.extend_from_slice(&prefix);
    request.resize(2 + username_len + 1, 0);
    stream.read_exact(&mut request[2..]).await?;
    let password_len = *request
        .last()
        .ok_or(RuntimeError::Protocol("missing SOCKS5 password length"))?
        as usize;
    let current_len = request.len();
    request.resize(
        current_len
            .checked_add(password_len)
            .ok_or(RuntimeError::Protocol("SOCKS5 auth message too long"))?,
        0,
    );
    stream.read_exact(&mut request[current_len..]).await?;
    let (auth, consumed) = socks5::parse_username_password_auth_request(&request)?;
    if consumed != request.len() {
        return Err(RuntimeError::Protocol("trailing SOCKS5 auth bytes"));
    }
    Ok(auth)
}

pub(super) async fn read_socks5_command<S>(
    stream: &mut S,
) -> Result<socks5::CommandRequest, RuntimeError>
where
    S: AsyncRead + Unpin,
{
    let mut prefix = [0u8; 4];
    stream.read_exact(&mut prefix).await?;
    let remaining = match prefix[3] {
        0x01 => 4 + 2,
        0x04 => 16 + 2,
        0x03 => {
            let mut len = [0u8; 1];
            stream.read_exact(&mut len).await?;
            let host_len = len[0] as usize;
            let mut request = Vec::with_capacity(5 + host_len + 2);
            request.extend_from_slice(&prefix);
            request.push(len[0]);
            request.resize(5 + host_len + 2, 0);
            stream.read_exact(&mut request[5..]).await?;
            let (command, consumed) = socks5::parse_command_request(&request)?;
            if consumed != request.len() {
                return Err(RuntimeError::Protocol("trailing SOCKS5 command bytes"));
            }
            return Ok(command);
        }
        _ => {
            return Err(RuntimeError::Socks5(Socks5Error::UnsupportedAddressType(
                prefix[3],
            )));
        }
    };
    let mut request = Vec::with_capacity(4 + remaining);
    request.extend_from_slice(&prefix);
    request.resize(4 + remaining, 0);
    stream.read_exact(&mut request[4..]).await?;
    let (command, consumed) = socks5::parse_command_request(&request)?;
    if consumed != request.len() {
        return Err(RuntimeError::Protocol("trailing SOCKS5 command bytes"));
    }
    Ok(command)
}

pub(super) async fn read_http_connect<S>(
    stream: &mut S,
) -> Result<http_connect::ConnectRequest, RuntimeError>
where
    S: AsyncRead + Unpin,
{
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        if buf.len() >= MAX_HTTP_CONNECT_HEADER_BYTES {
            return Err(RuntimeError::HttpConnect(HttpConnectError::HeaderTooLarge));
        }
        stream.read_exact(&mut byte).await?;
        buf.push(byte[0]);
        if buf.ends_with(b"\r\n\r\n") {
            return Ok(http_connect::parse_connect_request(&buf)?);
        }
    }
}
