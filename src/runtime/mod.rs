use crate::config::{
    AppConfig, ClientConfig, CommandConfig, ResourceLimits, SecurityConfig, TcpTrafficClass,
    TrafficPolicy,
};
use crate::ingress::IngressConfig;
use crate::ingress::http_connect::{self, HttpConnectError, HttpStatus};
use crate::ingress::socks5::{self, Socks5Error, Socks5Reply};
use crate::ingress::tun::TunL4Config;
use crate::mux::MuxLimits;
use crate::mux::datagram::{DatagramError, DatagramFlow};
use crate::mux::stream::{ReliableRecvStream, ReliableSendStream, StreamError};
use crate::outbound::{self, DnsConfig, OutboundConfig, TargetProtocol};
use crate::platform;
use crate::protocol::RateHint;
use crate::protocol::auth::{AuthError, SessionAuthenticator};
use crate::protocol::codec::CodecLimits;
use crate::protocol::{
    AuthNonce, CloseReason, DatagramFlowId, DatagramId, Frame, IngressKind, OffsetRange,
    OutboundPolicy, PathId, ResetReason, SessionId, StreamFlags, StreamId, TargetAddr,
    TrafficClass, UnderlayProtocol,
};
use crate::scheduler::{self, PathSnapshot, PathState as SchedulerPathState, SchedulerPolicy};
use crate::transport::encrypted::{
    EncryptedFramedReader, EncryptedFramedStream, EncryptedFramedTransportError,
    EncryptedFramedWriter, PeerRole,
};
use crate::transport::encrypted_udp::{EncryptedUdpSocket, EncryptedUdpTransportError};
use crate::transport::tcp::{self, TcpConnectOptions, TcpTransportError};
use crate::transport::udp::{self, UdpTransportError};
use crate::transport::{PathSpec, PathSpecParseError};
use bytes::{Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
use netstack_smoltcp::{StackBuilder, TcpListener as TunTcpListener, UdpSocket as TunUdpSocket};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{mpsc, oneshot, watch};
use tun_rs::DeviceBuilder;
use tun_rs::async_framed::{BytesCodec, DeviceFramed};

const MAX_HTTP_CONNECT_HEADER_BYTES: usize = 64 * 1024;
const PATH_OPEN_SCORE_BYTES: usize = 4 * 1024;
const UDP_PATH_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(2);
const PATH_FAILURE_COOLDOWN: Duration = Duration::from_secs(5);
const TCP_STREAM_LOAD_BYTES: u64 = 256 * 1024;
const UDP_SESSION_LOAD_BYTES: u64 = 64 * 1024;
const MIN_RATE_SAMPLE_BYTES: u64 = PATH_OPEN_SCORE_BYTES as u64;
const MIN_RATE_SAMPLE_DURATION: Duration = Duration::from_millis(1);
const TCP_STREAM_STALL_MIN_TIMEOUT: Duration = Duration::from_millis(350);
const TCP_STREAM_STALL_MAX_TIMEOUT: Duration = Duration::from_secs(2);
const UDP_DATAGRAM_MIN_TTL_FIT_RATIO: f64 = 0.9;
const UDP_BBR_PACING_GAIN: f64 = 1.25;
const UDP_MIN_PACING_RATE_BPS: f64 = 64_000.0;
const UDP_MAX_RESPONSE_TIMEOUT: Duration = Duration::from_secs(1);
const UDP_MIN_RESPONSE_TIMEOUT: Duration = Duration::from_millis(50);
const UDP_DEFAULT_MTU_PAYLOAD_BYTES: usize = 1200;
const UDP_MIN_MTU_PAYLOAD_BYTES: usize = 512;
const UDP_MAX_MTU_PAYLOAD_BYTES: usize = 65_000;
const TUN_UDP_FLOW_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

pub async fn run(config: AppConfig) -> Result<(), RuntimeError> {
    match config.command {
        CommandConfig::Client(client) => {
            run_client(client, config.security, config.resources).await
        }
        CommandConfig::Server(server) => {
            run_server(
                server.bind_paths,
                server.outbound,
                server.outbound_dns,
                config.security,
                config.resources,
            )
            .await
        }
    }
}

async fn run_client(
    client: ClientConfig,
    security: SecurityConfig,
    resources: ResourceLimits,
) -> Result<(), RuntimeError> {
    let path_probe_interval = client.path_probe_interval;
    let path_probe_timeout = client.path_probe_timeout;
    let context = ClientPathContext::new_with_policy(
        client.paths,
        client.traffic_policy,
        security,
        resources,
    )?;
    match client.ingress {
        IngressConfig::Socks5 { listen } => {
            run_socks5_client_ingress(listen, context, path_probe_interval, path_probe_timeout)
                .await
        }
        IngressConfig::HttpConnect { listen } => {
            run_http_connect_client_ingress(
                listen,
                context,
                path_probe_interval,
                path_probe_timeout,
            )
            .await
        }
        IngressConfig::TunL4(tun) => {
            start_client_path_probes(context.clone(), path_probe_interval, path_probe_timeout);
            run_tun_l4_client(tun, context).await
        }
    }
}

async fn run_socks5_client_ingress(
    listen: Vec<SocketAddr>,
    context: ClientPathContext,
    path_probe_interval: Duration,
    path_probe_timeout: Duration,
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
    let mut listeners = tokio::task::JoinSet::new();
    for listener in bound {
        let context = context.clone();
        listeners.spawn(async move { run_socks5_client_listener(listener, context).await });
    }
    start_client_path_probes(context, path_probe_interval, path_probe_timeout);
    wait_for_ingress_listener_failure(listeners, "SOCKS5").await
}

async fn run_socks5_client_listener(
    listener: TcpListener,
    context: ClientPathContext,
) -> Result<(), RuntimeError> {
    loop {
        let (stream, _) = listener.accept().await?;
        let context = context.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_socks5_client_stream(stream, context).await {
                eprintln!("warning: SOCKS5 client handler failed: {err}");
            }
        });
    }
}

async fn run_http_connect_client_ingress(
    listen: Vec<SocketAddr>,
    context: ClientPathContext,
    path_probe_interval: Duration,
    path_probe_timeout: Duration,
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
    let mut listeners = tokio::task::JoinSet::new();
    for listener in bound {
        let context = context.clone();
        listeners.spawn(async move { run_http_connect_client_listener(listener, context).await });
    }
    start_client_path_probes(context, path_probe_interval, path_probe_timeout);
    wait_for_ingress_listener_failure(listeners, "HTTP CONNECT").await
}

async fn run_http_connect_client_listener(
    listener: TcpListener,
    context: ClientPathContext,
) -> Result<(), RuntimeError> {
    loop {
        let (stream, _) = listener.accept().await?;
        let context = context.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_http_connect_client_stream(stream, context).await {
                eprintln!("warning: HTTP CONNECT client handler failed: {err}");
            }
        });
    }
}

async fn wait_for_ingress_listener_failure(
    mut listeners: tokio::task::JoinSet<Result<(), RuntimeError>>,
    ingress: &'static str,
) -> Result<(), RuntimeError> {
    if let Some(result) = listeners.join_next().await {
        match result {
            Ok(Ok(())) => return Err(RuntimeError::Protocol("client ingress listener exited")),
            Ok(Err(err)) => return Err(err),
            Err(err) => return Err(RuntimeError::TaskJoin(err)),
        }
    }
    Err(RuntimeError::Protocol(match ingress {
        "SOCKS5" => "SOCKS5 ingress has no listener tasks",
        "HTTP CONNECT" => "HTTP CONNECT ingress has no listener tasks",
        _ => "client ingress has no listener tasks",
    }))
}

fn start_client_path_probes(context: ClientPathContext, interval: Duration, timeout: Duration) {
    tokio::spawn(async move {
        run_client_path_probes(context, interval, timeout).await;
    });
}

async fn run_client_path_probes(context: ClientPathContext, interval: Duration, timeout: Duration) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        probe_client_paths(&context, timeout).await;
    }
}

async fn probe_client_paths(context: &ClientPathContext, timeout: Duration) {
    let mut probes = tokio::task::JoinSet::new();
    for path_index in 0..context.tcp_paths.len() {
        let context = context.clone();
        probes.spawn(async move {
            (
                UnderlayProtocol::Tcp,
                path_index,
                probe_tcp_client_path(&context, path_index, timeout).await,
            )
        });
    }
    for path_index in 0..context.udp_paths.len() {
        let context = context.clone();
        probes.spawn(async move {
            (
                UnderlayProtocol::Udp,
                path_index,
                probe_udp_client_path(&context, path_index, timeout).await,
            )
        });
    }

    while let Some(result) = probes.join_next().await {
        match result {
            Ok((UnderlayProtocol::Tcp, path_index, Ok(elapsed))) => {
                context.mark_tcp_path_probe_success(path_index, elapsed);
            }
            Ok((UnderlayProtocol::Tcp, path_index, Err(_))) => {
                context.mark_tcp_path_failure(path_index);
            }
            Ok((UnderlayProtocol::Udp, path_index, Ok(elapsed))) => {
                context.mark_udp_path_probe_success(path_index, elapsed);
            }
            Ok((UnderlayProtocol::Udp, path_index, Err(_))) => {
                context.mark_udp_path_failure(path_index);
            }
            Err(err) => {
                eprintln!("warning: path probe task failed: {err}");
            }
        }
    }
}

async fn run_tun_l4_client(
    tun: TunL4Config,
    context: ClientPathContext,
) -> Result<(), RuntimeError> {
    let device = build_tun_device(&tun)?;
    let framed = DeviceFramed::new(device, BytesCodec::new());
    let (mut tun_sink, mut tun_stream) = framed.split();

    let (stack, runner, udp_socket, tcp_listener) = StackBuilder::default()
        .enable_tcp(true)
        .enable_udp(true)
        .enable_icmp(tun.enable_icmp)
        .mtu(usize::from(tun.mtu))
        .build()?;
    let runner = runner.ok_or(RuntimeError::Protocol("TUN stack runner is unavailable"))?;
    let udp_socket = udp_socket.ok_or(RuntimeError::Protocol("TUN UDP socket is unavailable"))?;
    let tcp_listener =
        tcp_listener.ok_or(RuntimeError::Protocol("TUN TCP listener is unavailable"))?;
    let (mut stack_sink, mut stack_stream) = stack.split();

    let stack_to_tun = async move {
        while let Some(packet) = stack_stream.next().await {
            let packet = packet?;
            tun_sink.send(BytesMut::from(packet.as_slice())).await?;
        }
        Ok::<(), RuntimeError>(())
    };
    let tun_to_stack = async move {
        while let Some(packet) = tun_stream.next().await {
            let packet = packet?;
            stack_sink.send(packet.to_vec()).await?;
        }
        Ok::<(), RuntimeError>(())
    };
    let stack_runner = async move { runner.await.map_err(RuntimeError::Io) };

    tokio::try_join!(
        stack_runner,
        stack_to_tun,
        tun_to_stack,
        run_tun_tcp_listener(tcp_listener, context.clone()),
        run_tun_udp_socket(udp_socket, context, tun)
    )?;
    Ok(())
}

fn build_tun_device(tun: &TunL4Config) -> Result<tun_rs::AsyncDevice, RuntimeError> {
    let mut builder = DeviceBuilder::new().mtu(tun.mtu);
    if let Some(name) = &tun.name {
        builder = builder.name(name.clone());
    }
    if let Some(ipv4) = tun.ipv4 {
        builder = builder.ipv4(ipv4, tun.ipv4_prefix, tun.ipv4_gateway);
    }
    if let Some(ipv6) = tun.ipv6 {
        builder = builder.ipv6(ipv6, tun.ipv6_prefix);
    }
    builder.build_async().map_err(RuntimeError::TunDevice)
}

async fn run_tun_tcp_listener(
    mut listener: TunTcpListener,
    context: ClientPathContext,
) -> Result<(), RuntimeError> {
    while let Some((stream, local, remote)) = listener.next().await {
        let context = context.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_tun_tcp_stream(stream, local, remote, context).await {
                eprintln!("warning: TUN TCP flow {local} -> {remote} failed: {err}");
            }
        });
    }
    Ok(())
}

async fn handle_tun_tcp_stream<S>(
    stream: S,
    _local: SocketAddr,
    remote: SocketAddr,
    context: ClientPathContext,
) -> Result<(), RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let target = TargetAddr::Ip(remote);
    outbound::validate_target(&target)?;
    let class_policy = context.classify_tcp_target(&target);
    let remote = open_remote_stream(
        &context,
        target.clone(),
        IngressKind::TunTcp,
        class_policy.initial_class(),
    )
    .await?;
    relay_migrating_tcp_stream(
        stream,
        &context,
        TcpRelayOpenSpec {
            target,
            ingress: IngressKind::TunTcp,
            class_policy,
        },
        remote,
    )
    .await?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct TunUdpFlowKey {
    local: SocketAddr,
    remote: SocketAddr,
}

struct TunUdpResponse {
    payload: Vec<u8>,
    source: SocketAddr,
    destination: SocketAddr,
}

async fn run_tun_udp_socket(
    udp_socket: TunUdpSocket,
    context: ClientPathContext,
    tun: TunL4Config,
) -> Result<(), RuntimeError> {
    let (mut read_half, mut write_half) = udp_socket.split();
    let mut flows: HashMap<TunUdpFlowKey, mpsc::Sender<Vec<u8>>> = HashMap::new();
    let flow_limit = tun_udp_flow_limit(&context);
    let flow_queue = tun_udp_flow_queue(&context);
    let response_queue = tun_udp_response_queue(&context);
    let done_queue = flow_limit.clamp(1, 1024);
    let (response_tx, mut response_rx) = mpsc::channel::<TunUdpResponse>(response_queue);
    let (done_tx, mut done_rx) = mpsc::channel::<TunUdpFlowKey>(done_queue);

    loop {
        tokio::select! {
            received = read_half.next() => {
                let Some((payload, local, remote)) = received else {
                    return Ok(());
                };
                let key = TunUdpFlowKey { local, remote };
                if !flows.contains_key(&key) {
                    if flows.len() >= flow_limit {
                        eprintln!("warning: TUN UDP flow limit reached; dropping datagram from {local} to {remote}");
                        continue;
                    }
                    let (tx, rx) = mpsc::channel(flow_queue);
                    let flow_context = context.clone();
                    let flow_tun = tun.clone();
                    let flow_responses = response_tx.clone();
                    let flow_done = done_tx.clone();
                    tokio::spawn(async move {
                        let result =
                            handle_tun_udp_flow(key, flow_context, flow_tun, rx, flow_responses)
                                .await;
                        let _ = flow_done.send(key).await;
                        if let Err(err) = result {
                            eprintln!(
                                "warning: TUN UDP flow {} -> {} failed: {err}",
                                key.local, key.remote
                            );
                        }
                    });
                    flows.insert(key, tx);
                }
                let send_result = flows
                    .get(&key)
                    .ok_or(RuntimeError::Protocol("missing TUN UDP flow"))?
                    .try_send(payload);
                match send_result {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        eprintln!("warning: TUN UDP flow queue full; dropping datagram from {local} to {remote}");
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        flows.remove(&key);
                    }
                }
            }
            response = response_rx.recv() => {
                let Some(response) = response else {
                    return Ok(());
                };
                write_half
                    .send((response.payload, response.source, response.destination))
                    .await?;
            }
            done = done_rx.recv() => {
                if let Some(key) = done {
                    flows.remove(&key);
                }
            }
        }
    }
}

async fn handle_tun_udp_flow(
    key: TunUdpFlowKey,
    context: ClientPathContext,
    tun: TunL4Config,
    mut datagrams: mpsc::Receiver<Vec<u8>>,
    responses: mpsc::Sender<TunUdpResponse>,
) -> Result<(), RuntimeError> {
    let mut association = UdpDatagramClientAssociation::new(context)?;
    let result = loop {
        let payload = match tokio::time::timeout(TUN_UDP_FLOW_IDLE_TIMEOUT, datagrams.recv()).await
        {
            Ok(Some(payload)) => payload,
            Ok(None) | Err(_) => break Ok(()),
        };
        let target = tun_udp_target_for_remote(key.remote, &tun);
        let ttl_ms = tun_udp_ttl_ms(key.remote, &tun);
        let response = association
            .send_to(TargetAddr::Ip(target), Bytes::from(payload), ttl_ms)
            .await?;
        responses
            .send(TunUdpResponse {
                payload: response.to_vec(),
                source: key.remote,
                destination: key.local,
            })
            .await
            .map_err(|_| RuntimeError::Protocol("TUN UDP response channel closed"))?;
    };
    let close_result = association.close().await;
    if result.is_ok() {
        close_result?;
    }
    result
}

fn tun_udp_target_for_remote(remote: SocketAddr, tun: &TunL4Config) -> SocketAddr {
    if remote.port() != 53 || tun.dns_resolvers.is_empty() {
        return remote;
    }
    tun.dns_resolvers
        .iter()
        .copied()
        .find(|resolver| resolver.ip().is_ipv4() == remote.ip().is_ipv4())
        .unwrap_or(tun.dns_resolvers[0])
}

fn tun_udp_ttl_ms(remote: SocketAddr, tun: &TunL4Config) -> u32 {
    if remote.port() == 53 {
        tun.dns_ttl_ms
    } else {
        DEFAULT_SOCKS5_UDP_TTL_MS
    }
}

fn tun_udp_flow_limit(context: &ClientPathContext) -> usize {
    let payload = context.mux_limits.max_payload_bytes.max(1);
    (context.mux_limits.max_datagram_queue_bytes / payload).clamp(1, 4096)
}

fn tun_udp_flow_queue(context: &ClientPathContext) -> usize {
    let payload = context.mux_limits.max_payload_bytes.max(1);
    (context.mux_limits.max_datagram_queue_bytes / payload).clamp(1, 256)
}

fn tun_udp_response_queue(context: &ClientPathContext) -> usize {
    let payload = context.mux_limits.max_payload_bytes.max(1);
    (context.mux_limits.max_datagram_queue_bytes / payload).clamp(1, 1024)
}

async fn run_server(
    bind_paths: Vec<PathSpec>,
    outbound: OutboundConfig,
    outbound_dns: DnsConfig,
    security: SecurityConfig,
    resources: ResourceLimits,
) -> Result<(), RuntimeError> {
    let context = ServerPathContext {
        outbound,
        outbound_dns,
        codec_limits: resources.into(),
        mux_limits: resources.into(),
        security,
        tcp_streams: Arc::new(ServerTcpStreamRegistry::default()),
        max_tcp_streams: resources.max_streams,
        max_udp_sessions: resources.max_streams,
        max_udp_flows_per_session: resources.max_streams,
    };
    let mut bound = Vec::with_capacity(bind_paths.len());
    for path in bind_paths {
        match path.underlay {
            UnderlayProtocol::Tcp => {
                let listener = tcp::bind_listener(&path).await?;
                bound.push(BoundServerPath::Tcp(listener));
            }
            UnderlayProtocol::Udp => {
                let socket = udp::bind_socket(&path).await?;
                bound.push(BoundServerPath::Udp(socket));
            }
        }
    }
    let mut listeners = tokio::task::JoinSet::new();
    for bound_path in bound {
        match bound_path {
            BoundServerPath::Tcp(listener) => {
                let context = context.clone();
                listeners.spawn(async move { run_server_tcp_listener(listener, context).await });
            }
            BoundServerPath::Udp(socket) => {
                let context = context.clone();
                listeners.spawn(async move { run_server_udp_listener(socket, context).await });
            }
        }
    }
    if let Some(result) = listeners.join_next().await {
        match result {
            Ok(Ok(())) => return Err(RuntimeError::Protocol("server listener exited")),
            Ok(Err(err)) => return Err(err),
            Err(err) => return Err(RuntimeError::TaskJoin(err)),
        }
    }
    Err(RuntimeError::Protocol("server has no listener tasks"))
}

enum BoundServerPath {
    Tcp(TcpListener),
    Udp(UdpSocket),
}

async fn run_server_tcp_listener(
    listener: TcpListener,
    context: ServerPathContext,
) -> Result<(), RuntimeError> {
    loop {
        let (stream, _) = listener.accept().await?;
        let context = context.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_server_path(stream, context).await {
                eprintln!("warning: server path handler failed: {err}");
            }
        });
    }
}

async fn run_server_udp_listener(
    socket: UdpSocket,
    context: ServerPathContext,
) -> Result<(), RuntimeError> {
    let socket = Arc::new(socket);
    let probe = EncryptedUdpSocket::from_shared(
        socket.clone(),
        context.security.secret.as_bytes(),
        PeerRole::Server,
        context.codec_limits,
    );
    let mut buffer = vec![0u8; probe.max_datagram_bytes()?];
    let mut sessions: HashMap<SocketAddr, mpsc::Sender<Bytes>> = HashMap::new();
    let (done_tx, mut done_rx) = mpsc::channel::<SocketAddr>(udp_session_done_queue(&context));
    loop {
        tokio::select! {
            received = socket.recv_from(&mut buffer) => {
                let (len, peer) = received?;
                if !sessions.contains_key(&peer) {
                    if sessions.len() >= context.max_udp_sessions {
                        eprintln!(
                            "warning: UDP server session limit reached; dropping datagram from {peer}"
                        );
                        continue;
                    }
                    let (tx, rx) = mpsc::channel(udp_session_datagram_queue(&context));
                    let session_socket = socket.clone();
                    let session_context = context.clone();
                    let session_done = done_tx.clone();
                    tokio::spawn(async move {
                        if let Err(err) =
                            run_server_udp_peer_session(session_socket, peer, session_context, rx).await
                        {
                            eprintln!("warning: UDP server path session for {peer} failed: {err}");
                        }
                        let _ = session_done.send(peer).await;
                    });
                    sessions.insert(peer, tx);
                }
                let datagram = Bytes::copy_from_slice(&buffer[..len]);
                let send_result = sessions
                    .get(&peer)
                    .ok_or(RuntimeError::Protocol("missing UDP peer session"))?
                    .try_send(datagram);
                match send_result {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        eprintln!("warning: UDP server peer queue full; dropping datagram from {peer}");
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        sessions.remove(&peer);
                    }
                }
            }
            completed = done_rx.recv() => {
                if let Some(peer) = completed {
                    sessions.remove(&peer);
                }
            }
        }
    }
}

async fn run_server_udp_peer_session(
    socket: Arc<UdpSocket>,
    peer: SocketAddr,
    context: ServerPathContext,
    mut datagrams: mpsc::Receiver<Bytes>,
) -> Result<(), RuntimeError> {
    let mut session = ServerUdpPathSession::new(socket, peer, context)?;
    while let Some(datagram) = datagrams.recv().await {
        let frame = session.open_frame(&datagram)?;
        match session.handle_frame(frame).await? {
            ServerUdpSessionOutcome::Active => {}
            ServerUdpSessionOutcome::Closed => return Ok(()),
        }
    }
    Ok(())
}

fn udp_session_datagram_queue(context: &ServerPathContext) -> usize {
    let payload = context.mux_limits.max_payload_bytes.max(1);
    (context.mux_limits.max_datagram_queue_bytes / payload).clamp(1, 1024)
}

fn udp_session_done_queue(context: &ServerPathContext) -> usize {
    context.max_udp_sessions.clamp(1, 1024)
}

#[derive(Debug, Clone)]
pub struct ClientPathContext {
    tcp_paths: Arc<Vec<PathSpec>>,
    udp_paths: Arc<Vec<PathSpec>>,
    tcp_sessions: Arc<Vec<ClientTcpPathSessionHandle>>,
    next_tcp_stream_id: Arc<Mutex<u64>>,
    health: Arc<Mutex<ClientPathHealth>>,
    traffic_policy: TrafficPolicy,
    codec_limits: CodecLimits,
    mux_limits: MuxLimits,
    security: SecurityConfig,
}

#[derive(Debug)]
struct ClientPathHealth {
    tcp: Vec<ClientPathHealthRecord>,
    udp: Vec<ClientPathHealthRecord>,
}

#[derive(Debug, Clone)]
struct ClientPathHealthRecord {
    state: SchedulerPathState,
    consecutive_failures: u32,
    measured_srtt_ms: Option<f64>,
    measured_jitter_ms: Option<f64>,
    measured_rate_bps: Option<f64>,
    measured_loss_rate: Option<f64>,
    measured_mtu_payload_bytes: Option<usize>,
    failed_until: Option<Instant>,
    active_flows: u32,
    load_bytes: u64,
}

impl Default for ClientPathHealthRecord {
    fn default() -> Self {
        Self {
            state: SchedulerPathState::Active,
            consecutive_failures: 0,
            measured_srtt_ms: None,
            measured_jitter_ms: None,
            measured_rate_bps: None,
            measured_loss_rate: None,
            measured_mtu_payload_bytes: None,
            failed_until: None,
            active_flows: 0,
            load_bytes: 0,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ClientPathObservation {
    state: SchedulerPathState,
    measured_srtt_ms: Option<f64>,
    measured_jitter_ms: Option<f64>,
    measured_rate_bps: Option<f64>,
    measured_loss_rate: Option<f64>,
    measured_mtu_payload_bytes: Option<usize>,
    active_flows: u32,
    load_bytes: u64,
}

impl ClientPathHealthRecord {
    fn observe(&mut self, now: Instant) -> ClientPathObservation {
        if self.state == SchedulerPathState::Failed
            && self.failed_until.is_some_and(|deadline| now >= deadline)
        {
            self.state = SchedulerPathState::Suspect;
            self.failed_until = None;
        }
        ClientPathObservation {
            state: self.state,
            measured_srtt_ms: self.measured_srtt_ms,
            measured_jitter_ms: self.measured_jitter_ms,
            measured_rate_bps: self.measured_rate_bps,
            measured_loss_rate: self.measured_loss_rate,
            measured_mtu_payload_bytes: self.measured_mtu_payload_bytes,
            active_flows: self.active_flows,
            load_bytes: self.load_bytes,
        }
    }

    fn mark_success(&mut self, elapsed: Duration) {
        self.state = SchedulerPathState::Active;
        self.consecutive_failures = 0;
        self.failed_until = None;
        let sample_ms = elapsed.as_secs_f64() * 1000.0;
        self.measured_srtt_ms = Some(match self.measured_srtt_ms {
            Some(previous) => previous.mul_add(0.875, sample_ms * 0.125),
            None => sample_ms,
        });
    }

    fn mark_open_success(&mut self, elapsed: Duration, load_bytes: u64) {
        self.mark_success(elapsed);
        self.active_flows = self.active_flows.saturating_add(1);
        self.load_bytes = self.load_bytes.saturating_add(load_bytes);
    }

    fn release_load(&mut self, load_bytes: u64) {
        self.active_flows = self.active_flows.saturating_sub(1);
        self.load_bytes = self.load_bytes.saturating_sub(load_bytes);
    }

    fn mark_delivery(&mut self, sample: PathRateSample) {
        self.state = SchedulerPathState::Active;
        self.consecutive_failures = 0;
        self.failed_until = None;
        let sample_bps = sample.rate_bps();
        self.measured_rate_bps = Some(match self.measured_rate_bps {
            Some(previous) => previous.mul_add(0.75, sample_bps * 0.25),
            None => sample_bps,
        });
    }

    fn mark_udp_datagram_feedback(&mut self, observation: UdpDatagramPathObservation) {
        self.mark_success(observation.rtt);
        if let Some(sample) = observation.rate_sample {
            self.mark_delivery(sample);
        }
        let sample_jitter_ms = observation.jitter.as_secs_f64() * 1000.0;
        self.measured_jitter_ms = Some(match self.measured_jitter_ms {
            Some(previous) => previous.mul_add(0.875, sample_jitter_ms * 0.125),
            None => sample_jitter_ms,
        });
        self.measured_loss_rate = Some(match self.measured_loss_rate {
            Some(previous) => previous.mul_add(0.875, observation.loss_rate * 0.125),
            None => observation.loss_rate,
        });
    }

    fn mark_udp_mtu(&mut self, payload_bytes: usize) {
        self.measured_mtu_payload_bytes = Some(payload_bytes);
    }

    fn mark_failure(&mut self, now: Instant) {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        self.state = SchedulerPathState::Failed;
        self.failed_until = Some(now + PATH_FAILURE_COOLDOWN);
    }
}

#[derive(Debug, Clone, Copy)]
struct PathRateSample {
    bytes: u64,
    elapsed: Duration,
}

impl PathRateSample {
    fn new(bytes: u64, elapsed: Duration) -> Option<Self> {
        if bytes < MIN_RATE_SAMPLE_BYTES {
            return None;
        }
        Some(Self {
            bytes,
            elapsed: elapsed.max(MIN_RATE_SAMPLE_DURATION),
        })
    }

    fn rate_bps(self) -> f64 {
        self.bytes as f64 * 8.0 / self.elapsed.as_secs_f64()
    }
}

#[derive(Debug, Clone, Copy)]
struct UdpDatagramPathObservation {
    rtt: Duration,
    jitter: Duration,
    loss_rate: f64,
    rate_sample: Option<PathRateSample>,
}

#[derive(Debug, Clone, Copy, Default)]
struct PathDeliveryStats {
    payload_bytes: u64,
    first_payload_at: Option<Instant>,
    last_payload_at: Option<Instant>,
}

impl PathDeliveryStats {
    fn record_payload_bytes(&mut self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let now = Instant::now();
        self.payload_bytes = self.payload_bytes.saturating_add(bytes as u64);
        if self.first_payload_at.is_none() {
            self.first_payload_at = Some(now);
        }
        self.last_payload_at = Some(now);
    }

    fn rate_sample(self) -> Option<PathRateSample> {
        let first = self.first_payload_at?;
        let last = self.last_payload_at.unwrap_or(first);
        PathRateSample::new(self.payload_bytes, last.duration_since(first))
    }
}

struct TcpPathStream {
    stream_id: StreamId,
    max_offset: u64,
    output: TcpPathStreamOutput,
    frames: mpsc::Receiver<Result<Frame, RuntimeError>>,
}

impl TcpPathStream {
    fn into_handle_and_frames(
        self,
    ) -> (
        TcpPathStreamHandle,
        mpsc::Receiver<Result<Frame, RuntimeError>>,
    ) {
        (
            TcpPathStreamHandle {
                stream_id: self.stream_id,
                max_offset: self.max_offset,
                output: self.output,
            },
            self.frames,
        )
    }

    async fn send_frame(&self, frame: Frame) -> Result<(), RuntimeError> {
        self.output.send_frame(self.stream_id, frame).await
    }

    async fn recv_frame(&mut self) -> Result<Frame, RuntimeError> {
        match self.frames.recv().await {
            Some(Ok(frame)) => Ok(frame),
            Some(Err(err)) => Err(err),
            None => Err(RuntimeError::TcpPathSessionClosed),
        }
    }

    async fn close(&self) {
        self.output.close_stream(self.stream_id).await;
    }
}

struct TcpPathStreamHandle {
    stream_id: StreamId,
    max_offset: u64,
    output: TcpPathStreamOutput,
}

impl TcpPathStreamHandle {
    async fn send_frame(&self, frame: Frame) -> Result<(), RuntimeError> {
        self.output.send_frame(self.stream_id, frame).await
    }

    async fn close(&self) {
        self.output.close_stream(self.stream_id).await;
    }
}

#[derive(Clone)]
enum TcpPathStreamOutput {
    Fixed(mpsc::Sender<TcpPathSessionCommand>),
    Switchable(Arc<ServerTcpStreamBinding>),
}

impl TcpPathStreamOutput {
    async fn send_frame(&self, stream_id: StreamId, frame: Frame) -> Result<(), RuntimeError> {
        match self {
            Self::Fixed(commands) => commands
                .send(TcpPathSessionCommand::SendFrame(frame))
                .await
                .map_err(|_| RuntimeError::TcpPathSessionClosed),
            Self::Switchable(binding) => binding.send_frame(stream_id, frame).await,
        }
    }

    async fn close_stream(&self, stream_id: StreamId) {
        match self {
            Self::Fixed(commands) => {
                let _ = commands
                    .send(TcpPathSessionCommand::SendFrame(Frame::StreamDetach {
                        stream_id,
                    }))
                    .await;
                let _ = commands
                    .send(TcpPathSessionCommand::CloseStream(stream_id))
                    .await;
            }
            Self::Switchable(binding) => binding.close_stream(stream_id).await,
        }
    }
}

struct ServerTcpStreamBinding {
    outputs: Mutex<ServerTcpStreamOutputs>,
    version: watch::Sender<u64>,
}

impl ServerTcpStreamBinding {
    fn new(path_id: PathId, commands: mpsc::Sender<TcpPathSessionCommand>) -> Arc<Self> {
        let (version, _) = watch::channel(0);
        Arc::new(Self {
            outputs: Mutex::new(ServerTcpStreamOutputs {
                next_index: 0,
                entries: vec![ServerTcpStreamOutputEntry { path_id, commands }],
            }),
            version,
        })
    }

    fn attach(&self, path_id: PathId, commands: mpsc::Sender<TcpPathSessionCommand>) {
        let mut outputs = self.outputs.lock().expect("server TCP stream binding lock");
        if let Some(entry) = outputs
            .entries
            .iter_mut()
            .find(|entry| entry.path_id == path_id)
        {
            entry.commands = commands;
        } else {
            outputs
                .entries
                .push(ServerTcpStreamOutputEntry { path_id, commands });
        }
        outputs.next_index %= outputs.entries.len().max(1);
        drop(outputs);
        self.notify_update();
    }

    fn detach(&self, path_id: PathId, commands: &mpsc::Sender<TcpPathSessionCommand>) {
        let mut outputs = self.outputs.lock().expect("server TCP stream binding lock");
        let before = outputs.entries.len();
        outputs
            .entries
            .retain(|entry| entry.path_id != path_id || !entry.commands.same_channel(commands));
        if outputs.entries.len() != before {
            outputs.next_index %= outputs.entries.len().max(1);
            drop(outputs);
            self.notify_update();
        }
    }

    fn next_commands(&self) -> Option<(PathId, mpsc::Sender<TcpPathSessionCommand>)> {
        let mut outputs = self.outputs.lock().expect("server TCP stream binding lock");
        if outputs.entries.is_empty() {
            return None;
        }
        outputs.next_index %= outputs.entries.len();
        let entry = outputs.entries[outputs.next_index].clone();
        outputs.next_index = (outputs.next_index + 1) % outputs.entries.len();
        Some((entry.path_id, entry.commands))
    }

    fn data_commands(&self) -> Option<(PathId, mpsc::Sender<TcpPathSessionCommand>)> {
        self.outputs
            .lock()
            .expect("server TCP stream binding lock")
            .entries
            .last()
            .cloned()
            .map(|entry| (entry.path_id, entry.commands))
    }

    async fn send_frame(&self, _stream_id: StreamId, frame: Frame) -> Result<(), RuntimeError> {
        let mut updates = self.version.subscribe();
        loop {
            let selected = if server_frame_prefers_current_data_path(&frame) {
                self.data_commands()
            } else {
                self.next_commands()
            };
            if let Some((path_id, commands)) = selected {
                tokio::select! {
                    result = commands.send(TcpPathSessionCommand::SendFrame(frame.clone())) => {
                        match result {
                            Ok(()) => return Ok(()),
                            Err(_) => self.detach(path_id, &commands),
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

    async fn close_stream(&self, stream_id: StreamId) {
        let outputs = self
            .outputs
            .lock()
            .expect("server TCP stream binding lock")
            .entries
            .clone();
        for entry in outputs {
            let _ = entry
                .commands
                .send(TcpPathSessionCommand::CloseStream(stream_id))
                .await;
        }
    }

    fn notify_update(&self) {
        let current = *self.version.borrow();
        let _ = self.version.send(current.wrapping_add(1));
    }
}

fn server_frame_prefers_current_data_path(frame: &Frame) -> bool {
    matches!(frame, Frame::StreamData { .. } | Frame::StreamFin { .. })
}

#[derive(Clone)]
struct ServerTcpStreamOutputEntry {
    path_id: PathId,
    commands: mpsc::Sender<TcpPathSessionCommand>,
}

struct ServerTcpStreamOutputs {
    entries: Vec<ServerTcpStreamOutputEntry>,
    next_index: usize,
}

#[derive(Default)]
struct ServerTcpStreamRegistry {
    streams: Mutex<HashMap<(SessionId, StreamId), ServerTcpStreamEntry>>,
}

impl std::fmt::Debug for ServerTcpStreamRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerTcpStreamRegistry")
            .finish_non_exhaustive()
    }
}

struct ServerTcpStreamEntry {
    target: TargetAddr,
    frames: mpsc::Sender<Result<Frame, RuntimeError>>,
    binding: Arc<ServerTcpStreamBinding>,
}

struct ServerTcpPathAttachment {
    path_id: PathId,
    commands: mpsc::Sender<TcpPathSessionCommand>,
}

enum ServerTcpStreamOpen {
    New(TcpPathStream),
    Existing,
}

impl ServerTcpStreamRegistry {
    fn open_or_attach(
        &self,
        session_id: SessionId,
        stream_id: StreamId,
        target: &TargetAddr,
        attachment: ServerTcpPathAttachment,
        mux_limits: MuxLimits,
        max_streams: usize,
    ) -> Result<ServerTcpStreamOpen, RuntimeError> {
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
            entry
                .binding
                .attach(attachment.path_id, attachment.commands);
            return Ok(ServerTcpStreamOpen::Existing);
        }

        if streams.len() >= max_streams {
            return Err(RuntimeError::Protocol("server TCP stream limit reached"));
        }

        let (frames_tx, frames_rx) = mpsc::channel(tcp_stream_frame_queue(mux_limits));
        let binding = ServerTcpStreamBinding::new(attachment.path_id, attachment.commands);
        streams.insert(
            (session_id, stream_id),
            ServerTcpStreamEntry {
                target: target.clone(),
                frames: frames_tx,
                binding: binding.clone(),
            },
        );
        Ok(ServerTcpStreamOpen::New(TcpPathStream {
            stream_id,
            max_offset: mux_limits.max_stream_window_bytes,
            output: TcpPathStreamOutput::Switchable(binding),
            frames: frames_rx,
        }))
    }

    fn detach_path(
        &self,
        session_id: SessionId,
        stream_id: StreamId,
        path_id: PathId,
        commands: &mpsc::Sender<TcpPathSessionCommand>,
    ) {
        if let Some(binding) = self
            .streams
            .lock()
            .expect("server TCP stream registry lock")
            .get(&(session_id, stream_id))
            .map(|entry| entry.binding.clone())
        {
            binding.detach(path_id, commands);
        }
    }

    async fn route_frame(
        &self,
        session_id: SessionId,
        stream_id: StreamId,
        frame: Frame,
    ) -> Result<(), RuntimeError> {
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
            return Err(RuntimeError::Protocol(
                "frame for unknown server TCP stream",
            ));
        };
        stream
            .send(Ok(frame))
            .await
            .map_err(|_| RuntimeError::TcpPathSessionClosed)
    }

    fn close(&self, session_id: SessionId, stream_id: StreamId) {
        self.streams
            .lock()
            .expect("server TCP stream registry lock")
            .remove(&(session_id, stream_id));
    }
}

struct ClientTcpPathSessionHandle {
    runtime: ClientTcpPathSessionRuntime,
    commands: Arc<Mutex<Option<mpsc::Sender<TcpPathSessionCommand>>>>,
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
        }
    }
}

impl ClientTcpPathSessionHandle {
    fn new(runtime: ClientTcpPathSessionRuntime) -> Self {
        Self {
            runtime,
            commands: Arc::new(Mutex::new(None)),
        }
    }

    async fn open_stream(
        &self,
        stream_id: StreamId,
        target: TargetAddr,
        ingress: IngressKind,
        class: TrafficClass,
    ) -> Result<TcpPathStream, RuntimeError> {
        let commands = self.ensure_session();
        let (response_tx, response_rx) = oneshot::channel();
        commands
            .send(TcpPathSessionCommand::OpenStream {
                stream_id,
                target,
                ingress,
                class,
                session_commands: commands.clone(),
                response: response_tx,
            })
            .await
            .map_err(|_| RuntimeError::TcpPathSessionClosed)?;
        response_rx
            .await
            .map_err(|_| RuntimeError::TcpPathSessionClosed)?
    }

    fn ensure_session(&self) -> mpsc::Sender<TcpPathSessionCommand> {
        let mut current = self.commands.lock().expect("TCP path session lock");
        if let Some(commands) = current.as_ref()
            && !commands.is_closed()
        {
            return commands.clone();
        }

        let (commands, receiver) = mpsc::channel(self.runtime.command_queue);
        tokio::spawn(run_client_tcp_path_session(self.runtime.clone(), receiver));
        *current = Some(commands.clone());
        commands
    }
}

enum TcpPathSessionCommand {
    OpenStream {
        stream_id: StreamId,
        target: TargetAddr,
        ingress: IngressKind,
        class: TrafficClass,
        session_commands: mpsc::Sender<TcpPathSessionCommand>,
        response: oneshot::Sender<Result<TcpPathStream, RuntimeError>>,
    },
    SendFrame(Frame),
    CloseStream(StreamId),
}

struct ClientTcpPathConnection {
    writer: EncryptedTcpWriter,
    frames: mpsc::Receiver<Result<Frame, EncryptedFramedTransportError>>,
    heartbeat_interval: Duration,
    next_heartbeat_at: tokio::time::Instant,
    pending_heartbeat: Option<(u64, tokio::time::Instant)>,
}

type EncryptedTcpReader = EncryptedFramedReader<tokio::io::ReadHalf<TcpStream>>;
type EncryptedTcpWriter = EncryptedFramedWriter<tokio::io::WriteHalf<TcpStream>>;

struct ClientTcpPathStreamState {
    frames: mpsc::Sender<Result<Frame, RuntimeError>>,
    pending_open: Option<ClientTcpPendingOpen>,
}

struct ClientTcpPendingOpen {
    response: oneshot::Sender<Result<TcpPathStream, RuntimeError>>,
    frames: Option<mpsc::Receiver<Result<Frame, RuntimeError>>>,
    session_commands: mpsc::Sender<TcpPathSessionCommand>,
}

#[derive(Clone)]
struct ClientTcpPathSessionRuntime {
    path: PathSpec,
    path_index: usize,
    session_id: SessionId,
    security: SecurityConfig,
    codec_limits: CodecLimits,
    mux_limits: MuxLimits,
    command_queue: usize,
    stream_frame_queue: usize,
}

struct ClientTcpPathSessionState {
    connection: Option<ClientTcpPathConnection>,
    streams: HashMap<StreamId, ClientTcpPathStreamState>,
}

struct ClientTcpOpenStreamRequest {
    stream_id: StreamId,
    target: TargetAddr,
    ingress: IngressKind,
    class: TrafficClass,
    session_commands: mpsc::Sender<TcpPathSessionCommand>,
    response: oneshot::Sender<Result<TcpPathStream, RuntimeError>>,
}

async fn run_client_tcp_path_session(
    runtime: ClientTcpPathSessionRuntime,
    mut commands: mpsc::Receiver<TcpPathSessionCommand>,
) {
    let mut state = ClientTcpPathSessionState {
        connection: None,
        streams: HashMap::new(),
    };

    loop {
        if state.connection.is_none() {
            match commands.recv().await {
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

        let mut drop_connection = false;
        tokio::select! {
            biased;
            command = commands.recv() => {
                match command {
                    Some(command) => {
                        if let Err(err) = handle_connected_client_tcp_command(
                            command,
                            state.connection.as_mut().expect("checked connected TCP path session"),
                            &mut state.streams,
                            runtime.stream_frame_queue,
                        )
                        .await
                        {
                            fail_client_tcp_streams(&mut state.streams, &err);
                            eprintln!("warning: TCP path session command failed: {err}");
                            drop_connection = true;
                        }
                    }
                    None => {
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
            frame = state.connection.as_mut().expect("checked connected TCP path session").frames.recv() => {
                match frame {
                    Some(Ok(frame)) => {
                        if let Err(err) = handle_client_tcp_path_frame(
                            frame,
                            state.connection.as_mut().expect("checked connected TCP path session"),
                            &mut state.streams,
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
            class,
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
                    class,
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
    stream_frame_queue: usize,
) -> Result<(), RuntimeError> {
    match command {
        TcpPathSessionCommand::OpenStream {
            stream_id,
            target,
            ingress,
            class,
            session_commands,
            response,
        } => {
            let open = ClientTcpOpenStreamRequest {
                stream_id,
                target,
                ingress,
                class,
                session_commands,
                response,
            };
            open_client_tcp_stream_on_connection(connection, open, streams, stream_frame_queue)
                .await
        }
        TcpPathSessionCommand::SendFrame(frame) => {
            connection.writer.write_frame(&frame).await?;
            connection.writer.flush().await?;
            Ok(())
        }
        TcpPathSessionCommand::CloseStream(stream_id) => {
            streams.remove(&stream_id);
            Ok(())
        }
    }
}

async fn connect_client_tcp_path(
    path: &PathSpec,
    path_index: usize,
    session_id: SessionId,
    security: &SecurityConfig,
    codec_limits: CodecLimits,
    mux_limits: MuxLimits,
) -> Result<ClientTcpPathConnection, RuntimeError> {
    let tcp_stream = tcp::connect_path(path, TcpConnectOptions::default()).await?;
    let mut framed = EncryptedFramedStream::new(
        tcp_stream,
        security.secret.as_bytes(),
        PeerRole::Client,
        codec_limits,
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
    Ok(ClientTcpPathConnection {
        writer,
        frames: spawn_encrypted_tcp_reader(reader, tcp_path_session_frame_queue(mux_limits)),
        heartbeat_interval: mux_limits.tcp_path_heartbeat_interval,
        next_heartbeat_at: tokio::time::Instant::now() + mux_limits.tcp_path_heartbeat_interval,
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
            class: open.class,
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
                    output: TcpPathStreamOutput::Fixed(pending.session_commands),
                    frames,
                };
                let _ = pending.response.send(Ok(stream));
                return Ok(());
            }
            route_client_tcp_stream_frame(
                streams,
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
                let _ = pending
                    .response
                    .send(Err(RuntimeError::RemoteReset(reason)));
                return Ok(());
            }
            route_client_tcp_stream_frame(
                streams,
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
        Frame::StreamAck { stream_id, ranges } => {
            route_client_tcp_stream_frame(
                streams,
                stream_id,
                Frame::StreamAck { stream_id, ranges },
            )
            .await
        }
        Frame::StreamFin { stream_id } => {
            route_client_tcp_stream_frame(streams, stream_id, Frame::StreamFin { stream_id }).await
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

fn refresh_client_tcp_path_liveness(
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

fn refresh_client_tcp_path_liveness_state(
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

async fn route_client_tcp_stream_frame(
    streams: &mut HashMap<StreamId, ClientTcpPathStreamState>,
    stream_id: StreamId,
    frame: Frame,
) -> Result<(), RuntimeError> {
    let Some(state) = streams.get_mut(&stream_id) else {
        return Err(RuntimeError::Protocol("frame for unknown TCP stream"));
    };
    state
        .frames
        .send(Ok(frame))
        .await
        .map_err(|_| RuntimeError::TcpPathSessionClosed)
}

async fn tick_client_tcp_path_heartbeat(
    connection: &mut ClientTcpPathConnection,
    mux_limits: MuxLimits,
) -> Result<(), RuntimeError> {
    let now = tokio::time::Instant::now();
    if let Some((_, deadline)) = connection.pending_heartbeat.as_ref()
        && now >= *deadline
    {
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

async fn close_client_tcp_path(
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
        RuntimeError::TcpPathSessionClosed => RuntimeError::TcpPathSessionClosed,
        RuntimeError::RemoteReset(reason) => RuntimeError::RemoteReset(*reason),
        RuntimeError::RemoteClosed(reason) => RuntimeError::RemoteClosed(*reason),
        RuntimeError::Protocol(message) => RuntimeError::Protocol(message),
        _ => RuntimeError::TcpPathSessionClosed,
    }
}

fn spawn_encrypted_tcp_reader(
    mut reader: EncryptedTcpReader,
    queue_size: usize,
) -> mpsc::Receiver<Result<Frame, EncryptedFramedTransportError>> {
    let (frames_tx, frames_rx) = mpsc::channel(queue_size);
    tokio::spawn(async move {
        loop {
            let frame = reader.read_frame().await;
            let done = frame.is_err();
            if frames_tx.send(frame).await.is_err() || done {
                break;
            }
        }
    });
    frames_rx
}

fn tcp_session_command_queue(resources: ResourceLimits) -> usize {
    resources.max_streams.clamp(1, 1024)
}

fn tcp_path_session_frame_queue(mux_limits: MuxLimits) -> usize {
    tcp_stream_frame_queue(mux_limits)
        .saturating_mul(4)
        .clamp(16, 4096)
}

fn tcp_stream_frame_queue(mux_limits: MuxLimits) -> usize {
    let frame_payload = mux_limits
        .max_tcp_relay_chunk_bytes
        .min(mux_limits.max_payload_bytes)
        .max(1);
    (mux_limits.max_reorder_bytes / frame_payload)
        .saturating_add(4)
        .clamp(4, 1024)
}

impl ClientPathContext {
    pub fn new(
        paths: Vec<PathSpec>,
        security: SecurityConfig,
        resources: ResourceLimits,
    ) -> Result<Self, RuntimeError> {
        Self::new_with_policy(paths, TrafficPolicy::default(), security, resources)
    }

    pub fn new_with_policy(
        paths: Vec<PathSpec>,
        traffic_policy: TrafficPolicy,
        security: SecurityConfig,
        resources: ResourceLimits,
    ) -> Result<Self, RuntimeError> {
        if paths.len() > u16::MAX as usize {
            return Err(RuntimeError::PathIdOverflow);
        }
        let tcp_paths = paths
            .iter()
            .filter(|path| path.underlay == UnderlayProtocol::Tcp)
            .cloned()
            .collect::<Vec<_>>();
        let udp_paths = paths
            .into_iter()
            .filter(|path| path.underlay == UnderlayProtocol::Udp)
            .collect::<Vec<_>>();
        let health = ClientPathHealth {
            tcp: vec![ClientPathHealthRecord::default(); tcp_paths.len()],
            udp: vec![ClientPathHealthRecord::default(); udp_paths.len()],
        };
        let codec_limits = resources.into();
        let mux_limits = resources.into();
        let tcp_session_id = random_session_id()?;
        let tcp_sessions = tcp_paths
            .iter()
            .cloned()
            .enumerate()
            .map(|(path_index, path)| {
                ClientTcpPathSessionHandle::new(ClientTcpPathSessionRuntime {
                    path,
                    path_index,
                    session_id: tcp_session_id,
                    security: security.clone(),
                    codec_limits,
                    mux_limits,
                    command_queue: tcp_session_command_queue(resources),
                    stream_frame_queue: tcp_stream_frame_queue(mux_limits),
                })
            })
            .collect::<Vec<_>>();
        Ok(Self {
            tcp_paths: Arc::new(tcp_paths),
            udp_paths: Arc::new(udp_paths),
            tcp_sessions: Arc::new(tcp_sessions),
            next_tcp_stream_id: Arc::new(Mutex::new(0)),
            health: Arc::new(Mutex::new(health)),
            traffic_policy,
            codec_limits,
            mux_limits,
            security,
        })
    }

    fn classify_tcp_target(&self, target: &TargetAddr) -> TcpTrafficClass {
        self.traffic_policy.classify_tcp_target(target)
    }

    fn allocate_tcp_stream_id(&self) -> Result<StreamId, RuntimeError> {
        let mut next = self
            .next_tcp_stream_id
            .lock()
            .expect("client TCP stream ID lock");
        let stream_id = StreamId(*next);
        *next = next
            .checked_add(1)
            .ok_or(RuntimeError::Protocol("TCP stream ID overflow"))?;
        Ok(stream_id)
    }

    fn ordered_tcp_path_indices(&self, class: TrafficClass, payload_bytes: usize) -> Vec<usize> {
        let observations =
            health_observations(&mut self.health.lock().expect("client path health lock").tcp);
        ordered_path_indices(&self.tcp_paths, &observations, class, payload_bytes)
    }

    fn ordered_tcp_auto_bulk_discovery_indices(
        &self,
        current_path_index: Option<usize>,
        payload_bytes: usize,
    ) -> Vec<usize> {
        let observations =
            health_observations(&mut self.health.lock().expect("client path health lock").tcp);
        let scores = ordered_path_scores(
            &self.tcp_paths,
            &observations,
            TrafficClass::Bulk,
            payload_bytes,
        );
        let current_eta = current_path_index.and_then(|current_path_index| {
            scores
                .iter()
                .find_map(|(index, eta)| (*index == current_path_index).then_some(*eta))
        });
        let improves_current = |index: usize, eta: f64| {
            Some(index) != current_path_index && current_eta.is_none_or(|current| eta < current)
        };
        let measured = scores
            .iter()
            .copied()
            .filter(|(index, eta)| {
                improves_current(*index, *eta)
                    && observations
                        .get(*index)
                        .is_some_and(|observation| observation.measured_rate_bps.is_some())
            })
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        if !measured.is_empty() {
            return measured;
        }
        scores
            .into_iter()
            .filter(|(index, eta)| {
                improves_current(*index, *eta)
                    && self
                        .tcp_paths
                        .get(*index)
                        .is_some_and(tcp_path_can_be_auto_discovered)
            })
            .map(|(index, _)| index)
            .collect()
    }

    fn tcp_path_snapshot(&self, index: usize) -> Option<PathSnapshot> {
        let path = self.tcp_paths.get(index)?;
        let observation = self
            .health
            .lock()
            .expect("client path health lock")
            .tcp
            .get_mut(index)?
            .observe(Instant::now());
        Some(path_snapshot(path, index, observation))
    }

    fn ordered_udp_path_indices_for_ttl(&self, payload_bytes: usize, ttl_ms: u32) -> Vec<usize> {
        if ttl_ms == 0 {
            return Vec::new();
        }
        let observations =
            health_observations(&mut self.health.lock().expect("client path health lock").udp);
        ordered_path_indices_for_ttl(
            &self.udp_paths,
            &observations,
            TrafficClass::RealtimeDatagram,
            payload_bytes,
            ttl_ms,
        )
    }

    fn udp_path_runtime_model(&self, index: usize, ttl_ms: u32) -> Option<UdpPathRuntimeModel> {
        if ttl_ms == 0 {
            return None;
        }
        let path = self.udp_paths.get(index)?;
        let observation = self
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)?
            .observe(Instant::now());
        let snapshot = path_snapshot(path, index, observation);
        scheduler::score_path(
            snapshot,
            TrafficClass::RealtimeDatagram,
            1,
            SchedulerPolicy::default(),
        )?;
        Some(UdpPathRuntimeModel::from_snapshot(
            snapshot,
            ttl_ms,
            udp_mtu_payload_bytes(path, observation, self.mux_limits.max_payload_bytes),
            observation.measured_mtu_payload_bytes.is_some(),
            udp_probe_ceiling_payload_bytes(self.mux_limits.max_payload_bytes),
        ))
    }

    fn mark_tcp_path_open_success(&self, index: usize, elapsed: Duration) {
        if let Some(current) = self
            .health
            .lock()
            .expect("client path health lock")
            .tcp
            .get_mut(index)
        {
            current.mark_open_success(elapsed, TCP_STREAM_LOAD_BYTES);
        }
    }

    fn mark_tcp_path_probe_success(&self, index: usize, elapsed: Duration) {
        if let Some(current) = self
            .health
            .lock()
            .expect("client path health lock")
            .tcp
            .get_mut(index)
        {
            current.mark_success(elapsed);
        }
    }

    fn release_tcp_path_load(&self, index: usize) {
        if let Some(current) = self
            .health
            .lock()
            .expect("client path health lock")
            .tcp
            .get_mut(index)
        {
            current.release_load(TCP_STREAM_LOAD_BYTES);
        }
    }

    fn mark_tcp_path_delivery(&self, index: usize, stats: PathDeliveryStats) {
        let Some(sample) = stats.rate_sample() else {
            return;
        };
        if let Some(current) = self
            .health
            .lock()
            .expect("client path health lock")
            .tcp
            .get_mut(index)
        {
            current.mark_delivery(sample);
        }
    }

    fn mark_tcp_path_failure(&self, index: usize) {
        if let Some(current) = self
            .health
            .lock()
            .expect("client path health lock")
            .tcp
            .get_mut(index)
        {
            current.mark_failure(Instant::now());
        }
    }

    fn mark_udp_path_open_success(&self, index: usize, elapsed: Duration) {
        if let Some(current) = self
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
        {
            current.mark_open_success(elapsed, UDP_SESSION_LOAD_BYTES);
        }
    }

    fn mark_udp_path_probe_success(&self, index: usize, elapsed: Duration) {
        if let Some(current) = self
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
        {
            current.mark_success(elapsed);
        }
    }

    fn release_udp_path_load(&self, index: usize) {
        if let Some(current) = self
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
        {
            current.release_load(UDP_SESSION_LOAD_BYTES);
        }
    }

    fn mark_udp_path_delivery(&self, index: usize, stats: PathDeliveryStats) {
        let Some(sample) = stats.rate_sample() else {
            return;
        };
        if let Some(current) = self
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
        {
            current.mark_delivery(sample);
        }
    }

    fn mark_udp_path_feedback(&self, index: usize, observation: UdpDatagramPathObservation) {
        if let Some(current) = self
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
        {
            current.mark_udp_datagram_feedback(observation);
        }
    }

    fn mark_udp_path_mtu(&self, index: usize, payload_bytes: usize) {
        if let Some(current) = self
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
        {
            current.mark_udp_mtu(payload_bytes);
        }
    }

    fn mark_udp_path_failure(&self, index: usize) {
        if let Some(current) = self
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
        {
            current.mark_failure(Instant::now());
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct UdpPathRuntimeModel {
    pacing_rate_bps: f64,
    response_timeout: Duration,
    mtu_payload_bytes: usize,
    mtu_is_measured: bool,
    mtu_probe_ceiling_payload_bytes: usize,
}

impl UdpPathRuntimeModel {
    fn from_snapshot(
        snapshot: PathSnapshot,
        ttl_ms: u32,
        mtu_payload_bytes: usize,
        mtu_is_measured: bool,
        mtu_probe_ceiling_payload_bytes: usize,
    ) -> Self {
        let loss_backoff = (1.0 - snapshot.loss_rate.clamp(0.0, 1.0)).clamp(0.25, 1.0);
        let pacing_rate_bps = (snapshot.delivery_rate_bps * UDP_BBR_PACING_GAIN * loss_backoff)
            .max(UDP_MIN_PACING_RATE_BPS);
        let model_timeout = Duration::from_secs_f64(
            ((snapshot.srtt_ms + snapshot.jitter_ms.mul_add(4.0, 25.0)) / 1000.0)
                .max(UDP_MIN_RESPONSE_TIMEOUT.as_secs_f64()),
        );
        let ttl_timeout = Duration::from_millis(u64::from(ttl_ms));
        let response_timeout = model_timeout.min(UDP_MAX_RESPONSE_TIMEOUT).min(ttl_timeout);
        Self {
            pacing_rate_bps,
            response_timeout,
            mtu_payload_bytes,
            mtu_is_measured,
            mtu_probe_ceiling_payload_bytes,
        }
    }

    fn accepts_or_can_probe(self, payload_bytes: usize) -> bool {
        payload_bytes <= self.mtu_payload_bytes
            || (!self.mtu_is_measured && payload_bytes <= self.mtu_probe_ceiling_payload_bytes)
    }

    fn pacing_interval(self, payload_bytes: usize) -> Duration {
        if payload_bytes == 0 {
            return Duration::ZERO;
        }
        Duration::from_secs_f64(payload_bytes as f64 * 8.0 / self.pacing_rate_bps)
    }
}

fn udp_mtu_payload_bytes(
    path: &PathSpec,
    observation: ClientPathObservation,
    max_payload_bytes: usize,
) -> usize {
    let seeded = observation
        .measured_mtu_payload_bytes
        .or(path.metadata.initial_mtu_payload_bytes)
        .unwrap_or(UDP_DEFAULT_MTU_PAYLOAD_BYTES);
    seeded.clamp(
        UDP_MIN_MTU_PAYLOAD_BYTES,
        udp_probe_ceiling_payload_bytes(max_payload_bytes),
    )
}

fn udp_probe_ceiling_payload_bytes(max_payload_bytes: usize) -> usize {
    max_payload_bytes.clamp(UDP_MIN_MTU_PAYLOAD_BYTES, UDP_MAX_MTU_PAYLOAD_BYTES)
}

fn health_observations(records: &mut [ClientPathHealthRecord]) -> Vec<ClientPathObservation> {
    let now = Instant::now();
    records
        .iter_mut()
        .map(|record| record.observe(now))
        .collect()
}

fn ordered_path_indices(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    class: TrafficClass,
    payload_bytes: usize,
) -> Vec<usize> {
    ordered_path_scores(paths, observations, class, payload_bytes)
        .into_iter()
        .map(|(index, _)| index)
        .collect()
}

fn ordered_path_indices_for_ttl(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    class: TrafficClass,
    payload_bytes: usize,
    ttl_ms: u32,
) -> Vec<usize> {
    let scores = ordered_path_scores(paths, observations, class, payload_bytes);
    let freshness_budget_ms = f64::from(ttl_ms) * UDP_DATAGRAM_MIN_TTL_FIT_RATIO;
    scores
        .iter()
        .copied()
        .filter(|(_, eta_ms)| *eta_ms <= freshness_budget_ms)
        .map(|(index, _)| index)
        .collect::<Vec<_>>()
}

fn ordered_path_scores(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    class: TrafficClass,
    payload_bytes: usize,
) -> Vec<(usize, f64)> {
    let mut scores = paths
        .iter()
        .enumerate()
        .filter_map(|(index, path)| {
            let observation = observations
                .get(index)
                .copied()
                .unwrap_or(ClientPathObservation {
                    state: SchedulerPathState::Suspect,
                    measured_srtt_ms: None,
                    measured_jitter_ms: None,
                    measured_rate_bps: None,
                    measured_loss_rate: None,
                    measured_mtu_payload_bytes: None,
                    active_flows: 0,
                    load_bytes: 0,
                });
            scheduler::score_path(
                path_snapshot(path, index, observation),
                class,
                payload_bytes,
                SchedulerPolicy::default(),
            )
            .map(|score| (index, score.eta_ms))
        })
        .collect::<Vec<_>>();
    scores.sort_by(|left, right| left.1.total_cmp(&right.1));
    scores
}

fn path_snapshot(
    path: &PathSpec,
    index: usize,
    observation: ClientPathObservation,
) -> PathSnapshot {
    let hinted_delivery_rate_bps = match path.metadata.initial_rate {
        RateHint::Unknown => default_path_rate_bps(path.underlay),
        RateHint::Unlimited => 1_000_000_000_000.0,
        RateHint::BitsPerSecond(rate) => rate.max(1) as f64,
    };
    let delivery_rate_bps = observation
        .measured_rate_bps
        .unwrap_or(hinted_delivery_rate_bps)
        .max(1.0);
    PathSnapshot {
        id: PathId(index as u16),
        underlay: path.underlay,
        state: observation.state,
        flags: path.metadata.capabilities.into(),
        srtt_ms: observation.measured_srtt_ms.unwrap_or_else(|| {
            path.metadata
                .initial_srtt_ms
                .map_or_else(|| default_path_srtt_ms(path.underlay), f64::from)
        }),
        jitter_ms: observation
            .measured_jitter_ms
            .unwrap_or_else(|| f64::from(path.metadata.initial_jitter_ms.unwrap_or(0))),
        delivery_rate_bps,
        loss_rate: observation.measured_loss_rate.unwrap_or(0.0),
        queue_bytes: observation.load_bytes,
        bytes_in_flight: u64::from(observation.active_flows) * PATH_OPEN_SCORE_BYTES as u64,
    }
}

fn tcp_path_can_be_auto_discovered(path: &PathSpec) -> bool {
    !path.metadata.capabilities.expensive
        && !path.metadata.capabilities.backup
        && !path.metadata.capabilities.probe_only
        && path.metadata.capabilities.bulk_allowed
}

fn default_path_srtt_ms(underlay: UnderlayProtocol) -> f64 {
    match underlay {
        UnderlayProtocol::Tcp => 50.0,
        UnderlayProtocol::Udp => 40.0,
    }
}

fn default_path_rate_bps(underlay: UnderlayProtocol) -> f64 {
    match underlay {
        UnderlayProtocol::Tcp | UnderlayProtocol::Udp => 100_000_000.0,
    }
}

#[derive(Debug, Clone)]
pub struct ServerPathContext {
    outbound: OutboundConfig,
    outbound_dns: DnsConfig,
    codec_limits: CodecLimits,
    mux_limits: MuxLimits,
    security: SecurityConfig,
    tcp_streams: Arc<ServerTcpStreamRegistry>,
    max_tcp_streams: usize,
    max_udp_sessions: usize,
    max_udp_flows_per_session: usize,
}

pub async fn handle_socks5_client_stream<S>(
    mut stream: S,
    context: ClientPathContext,
) -> Result<(), RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let auth = read_socks5_auth(&mut stream).await?;
    if !auth.supports_no_auth() {
        stream
            .write_all(&socks5::no_acceptable_methods_response())
            .await?;
        return Err(RuntimeError::Socks5(Socks5Error::UnsupportedCommand(0)));
    }
    stream.write_all(&socks5::no_auth_response()).await?;
    let request = read_socks5_command(&mut stream).await?;
    match request.command {
        socks5::Socks5Command::Connect => {
            let target = request.target;
            let class_policy = context.classify_tcp_target(&target);
            let remote = match open_remote_stream(
                &context,
                target.clone(),
                IngressKind::Socks5,
                class_policy.initial_class(),
            )
            .await
            {
                Ok(remote) => remote,
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
                relay_migrating_tcp_stream(
                    stream,
                    &context,
                    TcpRelayOpenSpec {
                        target,
                        ingress: IngressKind::Socks5,
                        class_policy,
                    },
                    remote,
                )
                .await
            }
            .await;
            result.map(|_| ())
        }
        socks5::Socks5Command::UdpAssociate => {
            handle_socks5_udp_associate(
                &mut stream,
                context,
                socks5::UdpAssociateRequest {
                    client_endpoint: request.target,
                },
            )
            .await
        }
    }
}

pub async fn handle_http_connect_client_stream<S>(
    mut stream: S,
    context: ClientPathContext,
) -> Result<(), RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let request = read_http_connect(&mut stream).await?;
    let target = request.target;
    let class_policy = context.classify_tcp_target(&target);
    let remote = match open_remote_stream(
        &context,
        target.clone(),
        IngressKind::HttpConnect,
        class_policy.initial_class(),
    )
    .await
    {
        Ok(remote) => remote,
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
        relay_migrating_tcp_stream(
            stream,
            &context,
            TcpRelayOpenSpec {
                target,
                ingress: IngressKind::HttpConnect,
                class_policy,
            },
            remote,
        )
        .await
    }
    .await;
    result.map(|_| ())
}

struct OpenedRemoteStream {
    stream: TcpPathStream,
    path_index: usize,
}

struct TcpRelayRemotePath {
    path_index: usize,
    stream: TcpPathStreamHandle,
}

struct TcpRelayRemoteFrame {
    path_index: usize,
    frame: Result<Frame, RuntimeError>,
}

struct TcpRelayRemoteSet {
    stream_id: StreamId,
    paths: Vec<TcpRelayRemotePath>,
    frames_tx: mpsc::Sender<TcpRelayRemoteFrame>,
    frames_rx: mpsc::Receiver<TcpRelayRemoteFrame>,
    next_send_index: usize,
}

impl TcpRelayRemoteSet {
    fn new(opened: OpenedRemoteStream, frame_queue: usize) -> Self {
        let stream_id = opened.stream.stream_id;
        let (frames_tx, frames_rx) = mpsc::channel(frame_queue);
        let mut set = Self {
            stream_id,
            paths: Vec::new(),
            frames_tx,
            frames_rx,
            next_send_index: 0,
        };
        set.attach(opened);
        set
    }

    fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    fn primary_path_index(&self) -> Option<usize> {
        self.paths.first().map(|path| path.path_index)
    }

    fn active_path_index(&self) -> Option<usize> {
        self.paths.last().map(|path| path.path_index)
    }

    fn contains_path(&self, path_index: usize) -> bool {
        self.paths.iter().any(|path| path.path_index == path_index)
    }

    fn path_indices(&self) -> Vec<usize> {
        self.paths.iter().map(|path| path.path_index).collect()
    }

    fn is_empty(&self) -> bool {
        self.paths.is_empty()
    }

    fn max_offset(&self) -> u64 {
        self.paths
            .iter()
            .map(|path| path.stream.max_offset)
            .max()
            .unwrap_or(0)
    }

    fn attach(&mut self, opened: OpenedRemoteStream) {
        let path_index = opened.path_index;
        if self.contains_path(path_index) {
            return;
        }
        let (stream, mut frames) = opened.stream.into_handle_and_frames();
        let frames_tx = self.frames_tx.clone();
        tokio::spawn(async move {
            while let Some(frame) = frames.recv().await {
                let done = frame.is_err();
                if frames_tx
                    .send(TcpRelayRemoteFrame { path_index, frame })
                    .await
                    .is_err()
                    || done
                {
                    return;
                }
            }
            let _ = frames_tx
                .send(TcpRelayRemoteFrame {
                    path_index,
                    frame: Err(RuntimeError::TcpPathSessionClosed),
                })
                .await;
        });
        self.paths.push(TcpRelayRemotePath { path_index, stream });
    }

    async fn recv_frame(&mut self) -> Result<TcpRelayRemoteFrame, RuntimeError> {
        self.frames_rx
            .recv()
            .await
            .ok_or(RuntimeError::TcpPathSessionClosed)
    }

    async fn send_frame(
        &mut self,
        context: &ClientPathContext,
        frame: Frame,
    ) -> Result<usize, RuntimeError> {
        let mut last_error = None;
        while !self.paths.is_empty() {
            self.next_send_index %= self.paths.len();
            let path_index = self.paths[self.next_send_index].path_index;
            match self.paths[self.next_send_index]
                .stream
                .send_frame(frame.clone())
                .await
            {
                Ok(()) => {
                    self.next_send_index = (self.next_send_index + 1) % self.paths.len();
                    return Ok(path_index);
                }
                Err(err) => {
                    last_error = Some(err);
                    self.fail_path(context, path_index).await;
                }
            }
        }
        Err(last_error.unwrap_or(RuntimeError::TcpPathSessionClosed))
    }

    async fn close_all(&mut self) {
        let paths = std::mem::take(&mut self.paths);
        for path in paths {
            path.stream.close().await;
        }
        self.next_send_index = 0;
    }

    async fn fail_path(&mut self, context: &ClientPathContext, path_index: usize) -> bool {
        let Some(path) = self.remove_path(path_index) else {
            return false;
        };
        context.mark_tcp_path_failure(path.path_index);
        context.release_tcp_path_load(path.path_index);
        path.stream.close().await;
        true
    }

    fn remove_path(&mut self, path_index: usize) -> Option<TcpRelayRemotePath> {
        let position = self
            .paths
            .iter()
            .position(|path| path.path_index == path_index)?;
        let path = self.paths.remove(position);
        if self.paths.is_empty() {
            self.next_send_index = 0;
        } else {
            self.next_send_index %= self.paths.len();
        }
        Some(path)
    }
}

#[derive(Clone)]
struct TcpRelayOpenSpec {
    target: TargetAddr,
    ingress: IngressKind,
    class_policy: TcpTrafficClass,
}

#[derive(Debug, Clone, Copy)]
enum TcpRelayAttachMode {
    Any,
    AutoBulkDiscovery,
}

async fn open_remote_stream(
    context: &ClientPathContext,
    target: TargetAddr,
    ingress: IngressKind,
    class: TrafficClass,
) -> Result<OpenedRemoteStream, RuntimeError> {
    let stream_id = context.allocate_tcp_stream_id()?;
    open_remote_stream_with_id(context, stream_id, target, ingress, class).await
}

async fn open_remote_stream_with_id(
    context: &ClientPathContext,
    stream_id: StreamId,
    target: TargetAddr,
    ingress: IngressKind,
    class: TrafficClass,
) -> Result<OpenedRemoteStream, RuntimeError> {
    if context.tcp_paths.is_empty() {
        return Err(RuntimeError::NoTcpPath);
    }
    let candidates = context.ordered_tcp_path_indices(class, PATH_OPEN_SCORE_BYTES);
    if candidates.is_empty() {
        return Err(RuntimeError::NoSchedulableTcpPath);
    }
    let mut last_retryable_error = None;
    for path_index in candidates {
        match open_remote_stream_on_path(
            context,
            stream_id,
            target.clone(),
            ingress,
            class,
            path_index,
        )
        .await
        {
            Ok(opened) => return Ok(opened),
            Err(err) if stream_open_error_is_path_retryable(&err) => {
                context.mark_tcp_path_failure(path_index);
                last_retryable_error = Some(err);
            }
            Err(err) => return Err(err),
        }
    }
    Err(last_retryable_error.unwrap_or(RuntimeError::NoSchedulableTcpPath))
}

async fn open_remote_stream_on_path(
    context: &ClientPathContext,
    stream_id: StreamId,
    target: TargetAddr,
    ingress: IngressKind,
    class: TrafficClass,
    path_index: usize,
) -> Result<OpenedRemoteStream, RuntimeError> {
    let started_at = Instant::now();
    let stream = context
        .tcp_sessions
        .get(path_index)
        .ok_or(RuntimeError::NoSchedulableTcpPath)?
        .open_stream(stream_id, target, ingress, class)
        .await?;
    context.mark_tcp_path_open_success(path_index, started_at.elapsed());
    Ok(OpenedRemoteStream { stream, path_index })
}

fn authenticated_path_join_frames(
    security: &SecurityConfig,
    path: &PathSpec,
    path_id: PathId,
    underlay: UnderlayProtocol,
) -> Result<(Frame, Frame, Frame), RuntimeError> {
    let session_id = random_session_id()?;
    authenticated_path_join_frames_for_session(security, path, path_id, underlay, session_id)
}

fn authenticated_path_join_frames_for_session(
    security: &SecurityConfig,
    path: &PathSpec,
    path_id: PathId,
    underlay: UnderlayProtocol,
    session_id: SessionId,
) -> Result<(Frame, Frame, Frame), RuntimeError> {
    let authenticator = SessionAuthenticator::new(security.secret.as_bytes())?;
    let session_nonce = random_nonce()?;
    let session_tag = authenticator.session_auth_tag(session_id, session_nonce);
    let path_nonce = random_nonce()?;
    let capabilities = path.metadata.capabilities;
    let path_tag =
        authenticator.path_join_tag(session_id, path_id, underlay, path_nonce, capabilities);
    Ok((
        Frame::SessionHello { session_id },
        Frame::SessionAuth {
            session_id,
            nonce: session_nonce,
            auth_tag: session_tag,
        },
        Frame::PathJoin {
            session_id,
            path_id,
            underlay,
            nonce: path_nonce,
            capabilities,
            auth_tag: path_tag,
        },
    ))
}

fn stream_open_error_is_path_retryable(err: &RuntimeError) -> bool {
    matches!(
        err,
        RuntimeError::Io(_)
            | RuntimeError::Tcp(_)
            | RuntimeError::Encrypted(_)
            | RuntimeError::Auth(_)
            | RuntimeError::RemoteClosed(_)
            | RuntimeError::TcpPathSessionClosed
            | RuntimeError::PathHeartbeatTimeout
            | RuntimeError::Protocol(_)
    )
}

fn relay_error_is_tcp_path_failure<T>(result: &Result<T, RuntimeError>) -> bool {
    matches!(
        result,
        Err(RuntimeError::PathHeartbeatTimeout)
            | Err(RuntimeError::TcpPathSessionClosed)
            | Err(RuntimeError::Tcp(_))
            | Err(RuntimeError::Encrypted(_))
            | Err(RuntimeError::RemoteClosed(_))
            | Err(RuntimeError::Protocol(_))
    )
}

const DEFAULT_SOCKS5_UDP_TTL_MS: u32 = 30_000;

async fn handle_socks5_udp_associate<S>(
    stream: &mut S,
    context: ClientPathContext,
    request: socks5::UdpAssociateRequest,
) -> Result<(), RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if context.udp_paths.is_empty() {
        return Err(RuntimeError::NoUdpPath);
    }
    let client_endpoint = request.client_endpoint;
    let relay_socket = UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
    let relay_addr = relay_socket.local_addr()?;
    stream
        .write_all(&socks5::connect_reply(Socks5Reply::Succeeded, relay_addr))
        .await?;
    stream.flush().await?;

    let mut packet = vec![0u8; local_udp_buffer_len(context.mux_limits)];
    let mut control_probe = [0u8; 1];
    let mut udp_association = UdpDatagramClientAssociation::new(context.clone())?;
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
                if !socks5_udp_peer_allowed(&client_endpoint, peer) {
                    break Err(RuntimeError::Protocol("SOCKS5 UDP peer does not match association"));
                }
                let (datagram, consumed) = match socks5::parse_udp_datagram(&packet[..len]) {
                    Ok(parsed) => parsed,
                    Err(err) => break Err(RuntimeError::Socks5(err)),
                };
                if consumed != len {
                    break Err(RuntimeError::Protocol("trailing SOCKS5 UDP datagram bytes"));
                }
                let target = datagram.target.clone();
                let response = udp_association
                    .send_to(target.clone(), datagram.payload, DEFAULT_SOCKS5_UDP_TTL_MS)
                    .await;
                let response = match response {
                    Ok(response) => response,
                    Err(err) => break Err(err),
                };
                let response_packet = match socks5::udp_datagram(&target, &response) {
                    Ok(packet) => packet,
                    Err(err) => break Err(RuntimeError::Socks5(err)),
                };
                if let Err(err) = relay_socket.send_to(&response_packet, peer).await {
                    break Err(RuntimeError::Io(err));
                }
            }
        }
    };
    let close_result = udp_association.close().await;
    if result.is_ok() {
        close_result?;
    }
    result
}

fn local_udp_buffer_len(mux_limits: MuxLimits) -> usize {
    mux_limits
        .max_payload_bytes
        .saturating_add(512)
        .clamp(512, 65_535)
}

fn socks5_udp_peer_allowed(client_endpoint: &TargetAddr, peer: SocketAddr) -> bool {
    match client_endpoint {
        TargetAddr::Ip(addr) => {
            let ip_matches = addr.ip().is_unspecified() || addr.ip() == peer.ip();
            let port_matches = addr.port() == 0 || addr.port() == peer.port();
            ip_matches && port_matches
        }
        TargetAddr::Domain { port, .. } => *port == 0 || *port == peer.port(),
    }
}

async fn open_udp_datagram_session_on_path(
    context: &ClientPathContext,
    path_index: usize,
    session_id: SessionId,
) -> Result<UdpDatagramClientSession, RuntimeError> {
    let path = context
        .udp_paths
        .get(path_index)
        .ok_or(RuntimeError::NoSchedulableUdpPath)?;
    let started_at = Instant::now();
    let session = UdpDatagramClientSession::open_for_session(
        path,
        path_index,
        session_id,
        context.security.clone(),
        context.codec_limits,
        context.mux_limits,
        UDP_PATH_HANDSHAKE_TIMEOUT,
    )
    .await?;
    context.mark_udp_path_open_success(path_index, started_at.elapsed());
    Ok(session)
}

async fn probe_tcp_client_path(
    context: &ClientPathContext,
    path_index: usize,
    timeout: Duration,
) -> Result<Duration, RuntimeError> {
    let path = context
        .tcp_paths
        .get(path_index)
        .ok_or(RuntimeError::NoSchedulableTcpPath)?;
    let started_at = Instant::now();
    tokio::time::timeout(timeout, async {
        let tcp_stream = tcp::connect_path(
            path,
            TcpConnectOptions {
                timeout,
                ..TcpConnectOptions::default()
            },
        )
        .await?;
        let mut framed = EncryptedFramedStream::new(
            tcp_stream,
            context.security.secret.as_bytes(),
            PeerRole::Client,
            context.codec_limits,
        );
        let path_id = PathId(path_index as u16);
        let (session_hello, session_auth, path_join) = authenticated_path_join_frames(
            &context.security,
            path,
            path_id,
            UnderlayProtocol::Tcp,
        )?;
        let nonce = random_u64()?;

        framed.write_frame(&session_hello).await?;
        framed.write_frame(&session_auth).await?;
        framed.write_frame(&path_join).await?;
        framed.write_frame(&Frame::Ping { nonce }).await?;
        framed.flush().await?;

        let mut session_ready = false;
        let mut path_active = false;
        let mut pong_received = false;
        while !session_ready || !path_active || !pong_received {
            match framed.read_frame().await? {
                Frame::SessionReady => session_ready = true,
                Frame::PathStatus {
                    status: crate::protocol::PathStatus::Active,
                    ..
                } => path_active = true,
                Frame::PathStatus { .. } => {
                    return Err(RuntimeError::Protocol(
                        "TCP path probe did not return active path status",
                    ));
                }
                Frame::Pong {
                    nonce: received_nonce,
                } if received_nonce == nonce => pong_received = true,
                Frame::SessionClose { reason } => return Err(RuntimeError::RemoteClosed(reason)),
                _ => return Err(RuntimeError::Protocol("unexpected TCP path probe frame")),
            }
        }

        framed
            .write_frame(&Frame::SessionClose {
                reason: CloseReason::Normal,
            })
            .await?;
        framed.flush().await?;
        Ok(())
    })
    .await
    .map_err(|_| RuntimeError::Protocol("TCP path probe timed out"))??;
    Ok(started_at.elapsed())
}

async fn probe_udp_client_path(
    context: &ClientPathContext,
    path_index: usize,
    timeout: Duration,
) -> Result<Duration, RuntimeError> {
    let path = context
        .udp_paths
        .get(path_index)
        .ok_or(RuntimeError::NoSchedulableUdpPath)?;
    let started_at = Instant::now();
    tokio::time::timeout(timeout, async {
        let mut session = UdpDatagramClientSession::open(
            path,
            path_index,
            context.security.clone(),
            context.codec_limits,
            context.mux_limits,
            timeout,
        )
        .await?;
        session.ping(timeout).await?;
        session.close_session().await?;
        Ok::<(), RuntimeError>(())
    })
    .await
    .map_err(|_| RuntimeError::Protocol("UDP path probe timed out"))??;
    Ok(started_at.elapsed())
}

async fn handle_server_path(
    stream: TcpStream,
    context: ServerPathContext,
) -> Result<(), RuntimeError> {
    let mut framed = EncryptedFramedStream::new(
        stream,
        context.security.secret.as_bytes(),
        PeerRole::Server,
        context.codec_limits,
    );
    let session_id = match framed.read_frame().await? {
        Frame::SessionHello { session_id } => session_id,
        _ => return Err(RuntimeError::Protocol("expected SESSION_HELLO")),
    };
    let authenticator = SessionAuthenticator::new(context.security.secret.as_bytes())?;
    match framed.read_frame().await? {
        Frame::SessionAuth {
            session_id: auth_session_id,
            nonce,
            auth_tag,
        } if auth_session_id == session_id
            && authenticator.verify_session_auth(session_id, nonce, auth_tag) => {}
        _ => return Err(RuntimeError::Protocol("invalid SESSION_AUTH")),
    }
    let (path_id, path_capabilities) = match framed.read_frame().await? {
        Frame::PathJoin {
            session_id: join_session_id,
            path_id,
            underlay,
            nonce,
            capabilities,
            auth_tag,
        } if join_session_id == session_id
            && underlay == UnderlayProtocol::Tcp
            && authenticator.verify_path_join(
                session_id,
                path_id,
                underlay,
                nonce,
                capabilities,
                auth_tag,
            ) =>
        {
            (path_id, capabilities)
        }
        _ => return Err(RuntimeError::Protocol("invalid PATH_JOIN")),
    };
    framed.write_frame(&Frame::SessionReady).await?;
    framed
        .write_frame(&Frame::PathStatus {
            path_id,
            status: crate::protocol::PathStatus::Active,
            capabilities: path_capabilities,
        })
        .await?;
    if let Err(err) = framed.flush().await {
        if encrypted_framed_peer_closed(&err) {
            return Ok(());
        }
        return Err(RuntimeError::Encrypted(err));
    }

    let (reader, mut writer) = framed.split();
    let mut path_frames =
        spawn_encrypted_tcp_reader(reader, tcp_path_session_frame_queue(context.mux_limits));
    let (commands_tx, mut commands_rx) =
        mpsc::channel::<TcpPathSessionCommand>(tcp_server_session_command_queue(&context));
    let mut attached_streams = HashSet::new();
    let mut draining = false;

    loop {
        tokio::select! {
            biased;
            frame = path_frames.recv() => {
                match frame.ok_or(RuntimeError::TcpPathSessionClosed)?? {
                    Frame::OpenStream {
                        stream_id,
                        target,
                        ..
                    } if !draining => {
                        outbound::validate_target(&target)?;
                        context.outbound.ensure_supports(TargetProtocol::Tcp)?;
                        match context.tcp_streams.open_or_attach(
                            session_id,
                            stream_id,
                            &target,
                            ServerTcpPathAttachment {
                                path_id,
                                commands: commands_tx.clone(),
                            },
                            context.mux_limits,
                            context.max_tcp_streams,
                        )? {
                            ServerTcpStreamOpen::New(stream) => {
                                attached_streams.insert(stream_id);
                                let stream_context = context.clone();
                                tokio::spawn(async move {
                                    if let Err(err) =
                                        run_server_tcp_stream(
                                            stream_context,
                                            session_id,
                                            stream,
                                            target,
                                        )
                                        .await
                                    {
                                        eprintln!("warning: server TCP stream failed: {err}");
                                    }
                                });
                            }
                            ServerTcpStreamOpen::Existing => {
                                attached_streams.insert(stream_id);
                                context
                                    .tcp_streams
                                    .route_frame(
                                        session_id,
                                        stream_id,
                                        Frame::PathStatus {
                                            path_id,
                                            status: crate::protocol::PathStatus::Active,
                                            capabilities: path_capabilities,
                                        },
                                    )
                                    .await?;
                                if !server_write_tcp_path_frame(
                                    &mut writer,
                                    &Frame::StreamMaxData {
                                        stream_id,
                                        max_offset: context.mux_limits.max_stream_window_bytes,
                                    },
                                )
                                .await?
                                {
                                    return Ok(());
                                }
                            }
                        }
                    }
                    Frame::OpenStream { stream_id, .. } => {
                        if !server_write_tcp_path_frame(
                            &mut writer,
                            &Frame::StreamReset {
                                stream_id,
                                reason: ResetReason::Refused,
                            },
                        )
                        .await?
                        {
                            return Ok(());
                        }
                    }
                    Frame::StreamData { stream_id, offset, flags, payload } => {
                        context.tcp_streams.route_frame(
                            session_id,
                            stream_id,
                            Frame::StreamData {
                                stream_id,
                                offset,
                                flags,
                                payload,
                            },
                        )
                        .await?;
                    }
                    Frame::StreamAck { stream_id, ranges } => {
                        context.tcp_streams.route_frame(
                            session_id,
                            stream_id,
                            Frame::StreamAck { stream_id, ranges },
                        )
                        .await?;
                    }
                    Frame::StreamMaxData {
                        stream_id,
                        max_offset,
                    } => {
                        context.tcp_streams.route_frame(
                            session_id,
                            stream_id,
                            Frame::StreamMaxData {
                                stream_id,
                                max_offset,
                            },
                        )
                        .await?;
                    }
                    Frame::StreamFin { stream_id } => {
                        context.tcp_streams.route_frame(
                            session_id,
                            stream_id,
                            Frame::StreamFin { stream_id },
                        )
                        .await?;
                    }
                    Frame::StreamDetach { stream_id } => {
                        attached_streams.remove(&stream_id);
                        context
                            .tcp_streams
                            .detach_path(session_id, stream_id, path_id, &commands_tx);
                        if draining && attached_streams.is_empty() {
                            if !server_write_tcp_path_frame(
                                &mut writer,
                                &Frame::PathClose {
                                    path_id,
                                    reason: CloseReason::Normal,
                                },
                            )
                            .await?
                            {
                                return Ok(());
                            }
                            return Ok(());
                        }
                    }
                    Frame::StreamReset { stream_id, reason } => {
                        context.tcp_streams.route_frame(
                            session_id,
                            stream_id,
                            Frame::StreamReset { stream_id, reason },
                        )
                        .await?;
                    }
                    Frame::Ping { nonce } => {
                        if !server_write_tcp_path_frame(&mut writer, &Frame::Pong { nonce }).await? {
                            return Ok(());
                        }
                    }
                    Frame::PathDrain { path_id: drain_path_id } if drain_path_id == path_id => {
                        draining = true;
                        if !server_write_tcp_path_frame(
                            &mut writer,
                            &Frame::PathStatus {
                                path_id,
                                status: crate::protocol::PathStatus::Draining,
                                capabilities: path_capabilities,
                            },
                        )
                        .await?
                        {
                            return Ok(());
                        }
                        if attached_streams.is_empty() {
                            return Ok(());
                        }
                    }
                    Frame::PathClose {
                        path_id: close_path_id,
                        ..
                    } if close_path_id == path_id => return Ok(()),
                    Frame::SessionClose { .. } => return Ok(()),
                    _ => return Err(RuntimeError::Protocol("unexpected TCP path session frame")),
                }
            }
            command = commands_rx.recv() => {
                match command {
                    Some(TcpPathSessionCommand::SendFrame(frame)) => {
                        if !server_write_tcp_path_frame(&mut writer, &frame).await? {
                            return Ok(());
                        }
                    }
                    Some(TcpPathSessionCommand::CloseStream(stream_id)) => {
                        attached_streams.remove(&stream_id);
                        context
                            .tcp_streams
                            .detach_path(session_id, stream_id, path_id, &commands_tx);
                        if draining && attached_streams.is_empty() {
                            if !server_write_tcp_path_frame(
                                &mut writer,
                                &Frame::PathClose {
                                    path_id,
                                    reason: CloseReason::Normal,
                                },
                            )
                            .await?
                            {
                                return Ok(());
                            }
                            return Ok(());
                        }
                    }
                    Some(TcpPathSessionCommand::OpenStream { .. }) => {
                        return Err(RuntimeError::Protocol("server TCP path received client open command"));
                    }
                    None => return Ok(()),
                }
            }
        }
    }
}

async fn server_write_tcp_path_frame(
    framed: &mut EncryptedTcpWriter,
    frame: &Frame,
) -> Result<bool, RuntimeError> {
    match framed.write_frame(frame).await {
        Ok(()) => {}
        Err(err) if encrypted_framed_peer_closed(&err) => return Ok(false),
        Err(err) => return Err(RuntimeError::Encrypted(err)),
    }
    match framed.flush().await {
        Ok(()) => Ok(true),
        Err(err) if encrypted_framed_peer_closed(&err) => Ok(false),
        Err(err) => Err(RuntimeError::Encrypted(err)),
    }
}

fn encrypted_framed_peer_closed(err: &EncryptedFramedTransportError) -> bool {
    matches!(
        err,
        EncryptedFramedTransportError::Io(io)
            if matches!(
                io.kind(),
                std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::UnexpectedEof
            )
    )
}

async fn run_server_tcp_stream(
    context: ServerPathContext,
    session_id: SessionId,
    stream: TcpPathStream,
    target: TargetAddr,
) -> Result<(), RuntimeError> {
    let stream_id = stream.stream_id;
    let result = async {
        let outbound_stream = match outbound::connect_tcp(
            &context.outbound,
            &context.outbound_dns,
            &target,
            Duration::from_secs(10),
        )
        .await
        {
            Ok(stream) => stream,
            Err(err) => {
                stream
                    .send_frame(Frame::StreamReset {
                        stream_id,
                        reason: ResetReason::Refused,
                    })
                    .await?;
                stream.close().await;
                return Err(RuntimeError::OutboundConnect(err));
            }
        };
        stream
            .send_frame(Frame::StreamMaxData {
                stream_id,
                max_offset: context.mux_limits.max_stream_window_bytes,
            })
            .await?;
        relay_tcp_stream(outbound_stream, stream, context.mux_limits)
            .await
            .map(|_| ())
    }
    .await;
    context.tcp_streams.close(session_id, stream_id);
    result
}

fn tcp_server_session_command_queue(context: &ServerPathContext) -> usize {
    context.max_tcp_streams.clamp(1, 1024)
}

#[derive(Debug, Clone, Copy)]
struct TcpRelayClassState {
    policy: TcpTrafficClass,
    current: TrafficClass,
    rebalance_attempted: bool,
}

impl TcpRelayClassState {
    fn new(policy: TcpTrafficClass) -> Self {
        Self {
            policy,
            current: policy.initial_class(),
            rebalance_attempted: false,
        }
    }

    fn refresh(
        &mut self,
        path: Option<PathSnapshot>,
        sent_offset: u64,
        received_offset: u64,
        repair_bytes: usize,
        mux_limits: MuxLimits,
    ) -> TcpRelayClassUpdate {
        if !self.policy.is_auto() {
            self.current = self.policy.initial_class();
            return TcpRelayClassUpdate {
                class: self.current,
                promoted_to_bulk: false,
            };
        }

        let observed_bytes = sent_offset
            .max(received_offset)
            .saturating_add(repair_bytes as u64);
        let previous = self.current;
        self.current = if observed_bytes >= tcp_auto_bulk_threshold_bytes(path, mux_limits) {
            TrafficClass::Bulk
        } else {
            TrafficClass::Interactive
        };
        TcpRelayClassUpdate {
            class: self.current,
            promoted_to_bulk: previous != TrafficClass::Bulk && self.current == TrafficClass::Bulk,
        }
    }

    fn should_rebalance(self, update: TcpRelayClassUpdate) -> bool {
        self.policy.is_auto() && update.promoted_to_bulk && !self.rebalance_attempted
    }

    fn mark_rebalance_attempted(&mut self) {
        self.rebalance_attempted = true;
    }
}

#[derive(Debug, Clone, Copy)]
struct TcpRelayClassUpdate {
    class: TrafficClass,
    promoted_to_bulk: bool,
}

fn tcp_auto_bulk_threshold_bytes(path: Option<PathSnapshot>, mux_limits: MuxLimits) -> u64 {
    let relay_chunk = tcp_relay_buffer_len(mux_limits) as u64;
    let window = mux_limits.max_stream_window_bytes.max(relay_chunk);
    let bdp_bytes = path.map_or(relay_chunk, |path| {
        ((path.delivery_rate_bps.max(1.0) / 8.0) * (path.srtt_ms.max(1.0) / 1000.0)).ceil() as u64
    });
    let ramp_floor = relay_chunk.saturating_mul(4).min(window);
    let ramp_bdp = bdp_bytes.saturating_div(4).max(relay_chunk).max(ramp_floor);
    ramp_bdp.min(window)
}

async fn relay_migrating_tcp_stream<S>(
    mut local: S,
    context: &ClientPathContext,
    spec: TcpRelayOpenSpec,
    remote: OpenedRemoteStream,
) -> Result<PathDeliveryStats, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut remotes = TcpRelayRemoteSet::new(remote, tcp_stream_frame_queue(context.mux_limits));
    let stream_id = remotes.stream_id();
    let mut send_stream = ReliableSendStream::new(stream_id, context.mux_limits);
    send_stream.update_max_offset(remotes.max_offset());
    let mut recv_stream = ReliableRecvStream::new(stream_id, context.mux_limits);
    let chunk_size = tcp_relay_buffer_len(context.mux_limits);
    let mut buf = vec![0u8; chunk_size];
    let mut local_open = true;
    let mut remote_open = true;
    let mut pending_remote_fin = false;
    let mut stats = PathDeliveryStats::default();
    let mut path_stats = HashMap::<usize, PathDeliveryStats>::new();
    let mut class_state = TcpRelayClassState::new(spec.class_policy);
    let mut last_stream_progress_at = Instant::now();

    let result = loop {
        if !local_open && !remote_open && send_stream.repair_bytes() == 0 {
            break Ok(stats);
        }
        let path_snapshot = remotes
            .primary_path_index()
            .and_then(|path_index| context.tcp_path_snapshot(path_index));
        let class_update = class_state.refresh(
            path_snapshot,
            send_stream.next_offset(),
            recv_stream.next_offset(),
            send_stream.repair_bytes(),
            context.mux_limits,
        );
        let relay_class = class_update.class;
        if class_state.should_rebalance(class_update) {
            class_state.mark_rebalance_attempted();
            if let Err(err) = switch_tcp_relay_to_best_path(
                context,
                &spec,
                relay_class,
                &mut remotes,
                &send_stream,
                !local_open,
                TcpRelayAttachMode::AutoBulkDiscovery,
            )
            .await
            {
                eprintln!("warning: TCP auto path attachment failed: {err}");
            } else {
                last_stream_progress_at = Instant::now();
            }
            send_stream.update_max_offset(remotes.max_offset());
        }
        let adaptive_chunk =
            adaptive_tcp_relay_chunk_bytes(path_snapshot, relay_class, context.mux_limits);
        let adaptive_inflight =
            adaptive_tcp_relay_inflight_bytes(path_snapshot, relay_class, context.mux_limits);
        let stall_watch_active = tcp_relay_stall_watch_active(
            &send_stream,
            &recv_stream,
            remote_open,
            relay_class,
            context.mux_limits,
        );
        let stall_deadline =
            tcp_relay_stall_deadline(last_stream_progress_at, path_snapshot, relay_class);

        tokio::select! {
            _ = tokio::time::sleep_until(stall_deadline), if stall_watch_active => {
                if let Some(path_index) = remotes.active_path_index() {
                    remotes.fail_path(context, path_index).await;
                }
                match attach_tcp_relay_paths(
                    context,
                    &spec,
                    relay_class,
                    &mut remotes,
                    &send_stream,
                    !local_open,
                    TcpRelayAttachMode::Any,
                )
                .await
                {
                    Ok(attached) if attached > 0 => {
                        send_stream.update_max_offset(remotes.max_offset());
                        last_stream_progress_at = Instant::now();
                        continue;
                    }
                    Ok(_) => {
                        last_stream_progress_at = Instant::now();
                    }
                    Err(err) if remotes.is_empty() => break Err(err),
                    Err(err) => {
                        eprintln!("warning: TCP stream stall repair failed: {err}");
                        last_stream_progress_at = Instant::now();
                    }
                }
            }
            read = async {
                let read_budget = tcp_relay_read_budget_with_limit(
                    &send_stream,
                    context.mux_limits,
                    adaptive_inflight,
                    adaptive_chunk.min(buf.len()),
                );
                local.read(&mut buf[..read_budget]).await
            }, if local_open && tcp_relay_can_read_with_limit(&send_stream, adaptive_inflight) => {
                let read = match read {
                    Ok(read) => read,
                    Err(err) => break Err(RuntimeError::Io(err)),
                };
                if read == 0 {
                    local_open = false;
                    match remotes
                        .send_frame(context, Frame::StreamFin { stream_id })
                        .await
                    {
                        Ok(_) => {
                            last_stream_progress_at = Instant::now();
                        }
                        Err(err) if tcp_relay_error_is_migratable(&err) => {
                            if let Err(err) = attach_tcp_relay_paths(
                                context,
                                &spec,
                                relay_class,
                                &mut remotes,
                                &send_stream,
                                !local_open,
                                TcpRelayAttachMode::Any,
                            )
                            .await
                            {
                                break Err(err);
                            }
                            last_stream_progress_at = Instant::now();
                        }
                        Err(err) => break Err(err),
                    }
                } else {
                    let frame = match send_stream.send_data(
                        Bytes::copy_from_slice(&buf[..read]),
                        StreamFlags::NONE,
                    ) {
                        Ok(frame) => frame,
                        Err(err) => break Err(RuntimeError::Stream(err)),
                    };
                    match remotes.send_frame(context, frame).await {
                        Ok(path_index) => {
                            last_stream_progress_at = Instant::now();
                            stats.record_payload_bytes(read);
                            path_stats
                                .entry(path_index)
                                .or_default()
                                .record_payload_bytes(read);
                        }
                        Err(err) if tcp_relay_error_is_migratable(&err) => {
                            match attach_tcp_relay_paths(
                                context,
                                &spec,
                                relay_class,
                                &mut remotes,
                                &send_stream,
                                !local_open,
                                TcpRelayAttachMode::Any,
                            )
                            .await
                            {
                                Ok(attached) if attached > 0 => {
                                    last_stream_progress_at = Instant::now();
                                    stats.record_payload_bytes(read);
                                }
                                Ok(_) => break Err(err),
                                Err(err) => break Err(err),
                            }
                        }
                        Err(err) => break Err(err),
                    }
                }
            }
            frame = remotes.recv_frame(), if remote_open || send_stream.repair_bytes() > 0 => {
                let TcpRelayRemoteFrame { path_index, frame } = match frame {
                    Ok(frame) => frame,
                    Err(err) if tcp_relay_error_is_migratable(&err) => {
                        match attach_tcp_relay_paths(
                            context,
                            &spec,
                            relay_class,
                            &mut remotes,
                            &send_stream,
                            !local_open,
                            TcpRelayAttachMode::Any,
                        )
                        .await
                        {
                            Ok(attached) if attached > 0 => {
                                last_stream_progress_at = Instant::now();
                                continue;
                            }
                            Ok(_) => break Err(err),
                            Err(_) => break Err(err),
                        }
                    }
                    Err(err) => break Err(err),
                };
                let frame = match frame {
                    Ok(frame) => frame,
                    Err(err) if tcp_relay_error_is_migratable(&err) => {
                        remotes.fail_path(context, path_index).await;
                        if remotes.is_empty() {
                            match attach_tcp_relay_paths(
                                context,
                                &spec,
                                relay_class,
                                &mut remotes,
                                &send_stream,
                                !local_open,
                                TcpRelayAttachMode::Any,
                            )
                            .await
                            {
                                Ok(attached) if attached > 0 => {
                                    send_stream.update_max_offset(remotes.max_offset());
                                    last_stream_progress_at = Instant::now();
                                    continue;
                                }
                                Ok(_) => break Err(err),
                                Err(_) => break Err(err),
                            }
                        }
                        continue;
                    }
                    Err(err) => break Err(err),
                };
                match frame {
                    Frame::StreamData {
                        stream_id: received_stream_id,
                        offset,
                        flags,
                        payload,
                    } if received_stream_id == stream_id && remote_open => {
                        let outcome = match recv_stream.receive_data(offset, payload, flags) {
                            Ok(outcome) => outcome,
                            Err(err) => break Err(RuntimeError::Stream(err)),
                        };
                        last_stream_progress_at = Instant::now();
                        let mut write_error = None;
                        for chunk in outcome.delivered {
                            stats.record_payload_bytes(chunk.len());
                            path_stats
                                .entry(path_index)
                                .or_default()
                                .record_payload_bytes(chunk.len());
                            if let Err(err) = local.write_all(&chunk).await {
                                write_error = Some(err);
                                break;
                            }
                        }
                        if let Some(err) = write_error {
                            break Err(RuntimeError::Io(err));
                        }
                        if let Err(err) = local.flush().await {
                            break Err(RuntimeError::Io(err));
                        }
                        match send_tcp_recv_progress_remote_set(&mut remotes, context, &recv_stream).await {
                            Ok(()) => {}
                            Err(err) if tcp_relay_error_is_migratable(&err) => {
                                match attach_tcp_relay_paths(
                                    context,
                                    &spec,
                                    relay_class,
                                    &mut remotes,
                                    &send_stream,
                                    !local_open,
                                    TcpRelayAttachMode::Any,
                                )
                            .await
                            {
                                    Ok(attached) if attached > 0 => {
                                        last_stream_progress_at = Instant::now();
                                    }
                                    Ok(_) => break Err(err),
                                    Err(err) => break Err(err),
                                }
                            }
                            Err(err) => break Err(err),
                        }
                        if outcome.fin || (pending_remote_fin && recv_stream.reorder_bytes() == 0) {
                            if let Err(err) = local.shutdown().await {
                                break Err(RuntimeError::Io(err));
                            }
                            remote_open = false;
                            pending_remote_fin = false;
                        }
                    }
                    Frame::StreamAck {
                        stream_id: ack_stream_id,
                        ranges,
                    } if ack_stream_id == stream_id => {
                        send_stream.apply_ack(&ranges);
                        last_stream_progress_at = Instant::now();
                    }
                    Frame::StreamMaxData {
                        stream_id: max_stream_id,
                        max_offset,
                    } if max_stream_id == stream_id => {
                        send_stream.update_max_offset(max_offset);
                        last_stream_progress_at = Instant::now();
                    }
                    Frame::StreamFin { stream_id: fin_stream_id } if fin_stream_id == stream_id => {
                        last_stream_progress_at = Instant::now();
                        if recv_stream.reorder_bytes() == 0 {
                            if let Err(err) = local.shutdown().await {
                                break Err(RuntimeError::Io(err));
                            }
                            remote_open = false;
                        } else {
                            pending_remote_fin = true;
                        }
                    }
                    Frame::StreamReset {
                        stream_id: reset_stream_id,
                        reason,
                    } if reset_stream_id == stream_id => break Err(RuntimeError::RemoteReset(reason)),
                    _ => break Err(RuntimeError::Protocol("unexpected stream relay frame")),
                }
            }
            else => break Ok(stats),
        }
    };

    let remaining_paths = remotes.path_indices();
    if result.is_ok() {
        for (path_index, stats) in path_stats {
            context.mark_tcp_path_delivery(path_index, stats);
        }
    }
    if result.is_ok() {
        remotes.close_all().await;
    }
    for path_index in remaining_paths {
        if relay_error_is_tcp_path_failure(&result) {
            context.mark_tcp_path_failure(path_index);
        }
        context.release_tcp_path_load(path_index);
    }
    result
}

async fn switch_tcp_relay_to_best_path(
    context: &ClientPathContext,
    spec: &TcpRelayOpenSpec,
    class: TrafficClass,
    remotes: &mut TcpRelayRemoteSet,
    send_stream: &ReliableSendStream,
    resend_fin: bool,
    mode: TcpRelayAttachMode,
) -> Result<bool, RuntimeError> {
    let previous_paths = remotes.path_indices();
    let attached =
        attach_tcp_relay_paths(context, spec, class, remotes, send_stream, resend_fin, mode)
            .await?;
    if attached == 0 {
        return Ok(false);
    }
    let Some(active_path) = remotes.path_indices().last().copied() else {
        return Ok(false);
    };
    for path_index in previous_paths {
        if path_index == active_path {
            continue;
        }
        if let Some(path) = remotes.remove_path(path_index) {
            path.stream.close().await;
            context.release_tcp_path_load(path.path_index);
        }
    }
    Ok(true)
}

async fn attach_tcp_relay_paths(
    context: &ClientPathContext,
    spec: &TcpRelayOpenSpec,
    class: TrafficClass,
    remotes: &mut TcpRelayRemoteSet,
    send_stream: &ReliableSendStream,
    resend_fin: bool,
    mode: TcpRelayAttachMode,
) -> Result<usize, RuntimeError> {
    let stream_id = remotes.stream_id();
    let payload_bytes = match mode {
        TcpRelayAttachMode::Any => tcp_relay_attach_payload_bytes(send_stream, context.mux_limits),
        TcpRelayAttachMode::AutoBulkDiscovery => {
            tcp_relay_auto_bulk_discovery_payload_bytes(send_stream, context.mux_limits)
        }
    };
    let candidates = match mode {
        TcpRelayAttachMode::Any => context.ordered_tcp_path_indices(class, payload_bytes),
        TcpRelayAttachMode::AutoBulkDiscovery => context
            .ordered_tcp_auto_bulk_discovery_indices(remotes.active_path_index(), payload_bytes),
    };
    let mut last_retryable_error = None;
    let mut attached = 0usize;

    for path_index in candidates {
        if remotes.contains_path(path_index) {
            continue;
        }
        match open_remote_stream_on_path(
            context,
            stream_id,
            spec.target.clone(),
            spec.ingress,
            class,
            path_index,
        )
        .await
        {
            Ok(opened) => {
                match replay_tcp_repair_cache(&opened.stream, send_stream, resend_fin).await {
                    Ok(()) => {
                        remotes.attach(opened);
                        attached += 1;
                        return Ok(attached);
                    }
                    Err(err) if tcp_relay_error_is_migratable(&err) => {
                        context.mark_tcp_path_failure(path_index);
                        context.release_tcp_path_load(path_index);
                        last_retryable_error = Some(err);
                    }
                    Err(err) => {
                        context.release_tcp_path_load(path_index);
                        return Err(err);
                    }
                }
            }
            Err(err) if stream_open_error_is_path_retryable(&err) => {
                context.mark_tcp_path_failure(path_index);
                last_retryable_error = Some(err);
            }
            Err(err) => return Err(err),
        }
    }
    if attached > 0 {
        Ok(attached)
    } else if remotes.is_empty() {
        Err(last_retryable_error.unwrap_or(RuntimeError::NoSchedulableTcpPath))
    } else {
        Ok(0)
    }
}

fn tcp_relay_attach_payload_bytes(
    send_stream: &ReliableSendStream,
    mux_limits: MuxLimits,
) -> usize {
    let repair_bytes = send_stream
        .repair_bytes()
        .max(tcp_relay_buffer_len(mux_limits));
    let stream_window = usize::try_from(mux_limits.max_stream_window_bytes).unwrap_or(usize::MAX);
    repair_bytes.min(stream_window)
}

fn tcp_relay_auto_bulk_discovery_payload_bytes(
    send_stream: &ReliableSendStream,
    mux_limits: MuxLimits,
) -> usize {
    let attach_payload = tcp_relay_attach_payload_bytes(send_stream, mux_limits);
    let stream_window = usize::try_from(mux_limits.max_stream_window_bytes).unwrap_or(usize::MAX);
    attach_payload.max(mux_limits.max_tcp_path_inflight_bytes.min(stream_window))
}

fn tcp_relay_stall_watch_active(
    send_stream: &ReliableSendStream,
    recv_stream: &ReliableRecvStream,
    remote_open: bool,
    class: TrafficClass,
    mux_limits: MuxLimits,
) -> bool {
    send_stream.repair_bytes() > 0
        || (remote_open
            && (matches!(class, TrafficClass::Bulk | TrafficClass::Background)
                || recv_stream.next_offset() >= tcp_relay_response_stall_watch_bytes(mux_limits))
            && recv_stream.next_offset() > 0)
}

fn tcp_relay_response_stall_watch_bytes(mux_limits: MuxLimits) -> u64 {
    (tcp_relay_buffer_len(mux_limits) as u64)
        .saturating_mul(4)
        .min(mux_limits.max_stream_window_bytes)
}

fn tcp_relay_stall_deadline(
    last_progress_at: Instant,
    path: Option<PathSnapshot>,
    class: TrafficClass,
) -> tokio::time::Instant {
    tokio::time::Instant::from_std(last_progress_at + tcp_relay_stall_timeout(path, class))
}

fn tcp_relay_stall_timeout(path: Option<PathSnapshot>, class: TrafficClass) -> Duration {
    let (srtt_ms, jitter_ms) = path.map_or((250.0, 50.0), |path| {
        (path.srtt_ms.max(1.0), path.jitter_ms.max(0.0))
    });
    let rtt_gain = match class {
        TrafficClass::Control | TrafficClass::RealtimeDatagram => 1.5,
        TrafficClass::Interactive => 2.0,
        TrafficClass::Bulk => 1.5,
        TrafficClass::Background => 3.0,
    };
    Duration::from_secs_f64(
        ((srtt_ms * rtt_gain + jitter_ms * 4.0 + 100.0) / 1000.0).clamp(
            TCP_STREAM_STALL_MIN_TIMEOUT.as_secs_f64(),
            TCP_STREAM_STALL_MAX_TIMEOUT.as_secs_f64(),
        ),
    )
}

async fn replay_tcp_repair_cache(
    path_stream: &TcpPathStream,
    send_stream: &ReliableSendStream,
    resend_fin: bool,
) -> Result<(), RuntimeError> {
    for frame in send_stream.retransmission_frames() {
        path_stream.send_frame(frame).await?;
    }
    if resend_fin {
        path_stream
            .send_frame(Frame::StreamFin {
                stream_id: path_stream.stream_id,
            })
            .await?;
    }
    Ok(())
}

fn tcp_relay_error_is_migratable(err: &RuntimeError) -> bool {
    matches!(
        err,
        RuntimeError::PathHeartbeatTimeout
            | RuntimeError::TcpPathSessionClosed
            | RuntimeError::Tcp(_)
            | RuntimeError::Encrypted(_)
            | RuntimeError::RemoteClosed(_)
            | RuntimeError::Protocol(_)
    )
}

async fn send_tcp_recv_progress(
    path_stream: &TcpPathStream,
    recv_stream: &ReliableRecvStream,
) -> Result<(), RuntimeError> {
    path_stream.send_frame(recv_stream.ack_frame()).await?;
    path_stream.send_frame(recv_stream.max_data_frame()).await?;
    Ok(())
}

async fn send_tcp_recv_progress_remote_set(
    remotes: &mut TcpRelayRemoteSet,
    context: &ClientPathContext,
    recv_stream: &ReliableRecvStream,
) -> Result<(), RuntimeError> {
    remotes.send_frame(context, recv_stream.ack_frame()).await?;
    remotes
        .send_frame(context, recv_stream.max_data_frame())
        .await?;
    Ok(())
}

fn tcp_relay_buffer_len(mux_limits: MuxLimits) -> usize {
    mux_limits
        .max_tcp_relay_chunk_bytes
        .min(mux_limits.max_payload_bytes)
        .min(mux_limits.max_tcp_path_inflight_bytes)
        .max(1)
}

fn adaptive_tcp_relay_chunk_bytes(
    path: Option<PathSnapshot>,
    class: TrafficClass,
    mux_limits: MuxLimits,
) -> usize {
    let cap = tcp_relay_buffer_len(mux_limits);
    let Some(path) = path else {
        return cap;
    };

    let bdp_bytes = tcp_path_bdp_bytes(path);
    let class_gain = tcp_class_chunk_gain(class);
    let stability = tcp_path_stability_factor(path);
    let queue_factor = tcp_path_queue_factor(path, bdp_bytes);
    let target = (bdp_bytes * class_gain * stability * queue_factor).ceil() as usize;
    target.clamp(1, cap)
}

fn adaptive_tcp_relay_inflight_bytes(
    path: Option<PathSnapshot>,
    class: TrafficClass,
    mux_limits: MuxLimits,
) -> usize {
    let cap = mux_limits.max_tcp_path_inflight_bytes.max(1);
    let floor = tcp_relay_buffer_len(mux_limits).min(cap).max(1);
    let Some(path) = path else {
        return cap;
    };

    let bdp_bytes = tcp_path_bdp_bytes(path);
    let target = bdp_bytes
        * tcp_class_inflight_gain(class)
        * tcp_path_stability_factor(path)
        * tcp_path_queue_factor(path, bdp_bytes);
    (target.ceil() as usize).clamp(floor, cap)
}

fn tcp_path_bdp_bytes(path: PathSnapshot) -> f64 {
    (path.delivery_rate_bps.max(1.0) / 8.0) * (path.srtt_ms.max(1.0) / 1000.0)
}

fn tcp_class_chunk_gain(class: TrafficClass) -> f64 {
    match class {
        TrafficClass::Control | TrafficClass::RealtimeDatagram => 1.0 / 64.0,
        TrafficClass::Interactive => 1.0 / 16.0,
        TrafficClass::Bulk => 1.0 / 4.0,
        TrafficClass::Background => 1.0 / 8.0,
    }
}

fn tcp_class_inflight_gain(class: TrafficClass) -> f64 {
    match class {
        TrafficClass::Control | TrafficClass::RealtimeDatagram => 0.5,
        TrafficClass::Interactive => 1.0,
        TrafficClass::Bulk => 2.0,
        TrafficClass::Background => 1.0,
    }
}

fn tcp_path_stability_factor(path: PathSnapshot) -> f64 {
    let loss_factor = (1.0 - path.loss_rate.clamp(0.0, 1.0)).clamp(0.125, 1.0);
    let srtt = path.srtt_ms.max(1.0);
    let jitter_factor = (srtt / (srtt + path.jitter_ms.max(0.0))).clamp(0.125, 1.0);
    loss_factor * jitter_factor
}

fn tcp_path_queue_factor(path: PathSnapshot, bdp_bytes: f64) -> f64 {
    let queued = path.queue_bytes.saturating_add(path.bytes_in_flight) as f64;
    (bdp_bytes / (bdp_bytes + queued.max(0.0))).clamp(0.125, 1.0)
}

async fn relay_tcp_stream<S>(
    mut local: S,
    mut path_stream: TcpPathStream,
    mux_limits: MuxLimits,
) -> Result<PathDeliveryStats, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let stream_id = path_stream.stream_id;
    let mut send_stream = ReliableSendStream::new(stream_id, mux_limits);
    send_stream.update_max_offset(path_stream.max_offset);
    let mut recv_stream = ReliableRecvStream::new(stream_id, mux_limits);
    let chunk_size = tcp_relay_buffer_len(mux_limits);
    let mut buf = vec![0u8; chunk_size];
    let mut local_open = true;
    let mut remote_open = true;
    let mut stats = PathDeliveryStats::default();
    let mut close_sent = false;

    let result = loop {
        if !local_open && !remote_open && send_stream.repair_bytes() == 0 {
            break Ok(stats);
        }

        tokio::select! {
            read = async {
                let read_budget = tcp_relay_read_budget(&send_stream, mux_limits, buf.len());
                local.read(&mut buf[..read_budget]).await
            }, if local_open && tcp_relay_can_read(&send_stream, mux_limits) => {
                let read = read?;
                if read == 0 {
                    path_stream.send_frame(Frame::StreamFin { stream_id }).await?;
                    close_sent = true;
                    local_open = false;
                } else {
                    let frame = send_stream.send_data(
                        Bytes::copy_from_slice(&buf[..read]),
                        StreamFlags::NONE,
                    )?;
                    path_stream.send_frame(frame).await?;
                    stats.record_payload_bytes(read);
                }
            }
            frame = path_stream.recv_frame(), if remote_open || send_stream.repair_bytes() > 0 => {
                match frame? {
                    Frame::StreamData {
                        stream_id: received_stream_id,
                        offset,
                        flags,
                        payload,
                    } if received_stream_id == stream_id && remote_open => {
                        let outcome = recv_stream.receive_data(offset, payload, flags)?;
                        for chunk in outcome.delivered {
                            stats.record_payload_bytes(chunk.len());
                            local.write_all(&chunk).await?;
                        }
                        local.flush().await?;
                        send_tcp_recv_progress(&path_stream, &recv_stream).await?;
                        if outcome.fin {
                            local.shutdown().await?;
                            remote_open = false;
                        }
                    }
                    Frame::StreamAck {
                        stream_id: ack_stream_id,
                        ranges,
                    } if ack_stream_id == stream_id => {
                        send_stream.apply_ack(&ranges);
                    }
                    Frame::StreamMaxData {
                        stream_id: max_stream_id,
                        max_offset,
                    } if max_stream_id == stream_id => {
                        send_stream.update_max_offset(max_offset);
                    }
                    Frame::PathStatus {
                        status: crate::protocol::PathStatus::Active,
                        ..
                    } => {
                        replay_tcp_repair_cache(&path_stream, &send_stream, false).await?;
                    }
                    Frame::StreamFin { stream_id: fin_stream_id } if fin_stream_id == stream_id => {
                        local.shutdown().await?;
                        remote_open = false;
                    }
                    Frame::StreamReset {
                        stream_id: reset_stream_id,
                        reason,
                    } if reset_stream_id == stream_id => return Err(RuntimeError::RemoteReset(reason)),
                    _ => return Err(RuntimeError::Protocol("unexpected stream relay frame")),
                }
            }
            else => break Ok(stats),
        }
    };

    if !close_sent {
        path_stream.close().await;
    }
    result
}

fn tcp_relay_can_read(send_stream: &ReliableSendStream, mux_limits: MuxLimits) -> bool {
    tcp_relay_can_read_with_limit(send_stream, mux_limits.max_tcp_path_inflight_bytes)
}

fn tcp_relay_can_read_with_limit(send_stream: &ReliableSendStream, inflight_limit: usize) -> bool {
    send_stream.repair_bytes() < inflight_limit.max(1)
}

fn tcp_relay_read_budget(
    send_stream: &ReliableSendStream,
    mux_limits: MuxLimits,
    buffer_len: usize,
) -> usize {
    tcp_relay_read_budget_with_limit(
        send_stream,
        mux_limits,
        mux_limits.max_tcp_path_inflight_bytes,
        buffer_len,
    )
}

fn tcp_relay_read_budget_with_limit(
    send_stream: &ReliableSendStream,
    mux_limits: MuxLimits,
    inflight_limit: usize,
    buffer_len: usize,
) -> usize {
    inflight_limit
        .max(1)
        .min(mux_limits.max_tcp_path_inflight_bytes)
        .saturating_sub(send_stream.repair_bytes())
        .min(buffer_len)
}

pub async fn client_udp_datagram_round_trip(
    path: &PathSpec,
    security: SecurityConfig,
    resources: ResourceLimits,
    target: TargetAddr,
    payload: Bytes,
    ttl_ms: u32,
) -> Result<Bytes, RuntimeError> {
    client_udp_datagram_round_trip_with_limits(
        path,
        security,
        resources.into(),
        resources.into(),
        target,
        payload,
        ttl_ms,
    )
    .await
}

async fn client_udp_datagram_round_trip_with_limits(
    path: &PathSpec,
    security: SecurityConfig,
    codec_limits: CodecLimits,
    mux_limits: MuxLimits,
    target: TargetAddr,
    payload: Bytes,
    ttl_ms: u32,
) -> Result<Bytes, RuntimeError> {
    let mut session = UdpDatagramClientSession::open(
        path,
        0,
        security,
        codec_limits,
        mux_limits,
        UDP_PATH_HANDSHAKE_TIMEOUT,
    )
    .await?;
    let response = session.send_to(target, payload, ttl_ms).await?;
    session.close().await?;
    Ok(response)
}

struct UdpDatagramClientAssociation {
    context: ClientPathContext,
    session_id: SessionId,
    paths: Vec<UdpDatagramAssociationPath>,
}

struct UdpDatagramAssociationPath {
    session: UdpDatagramClientSession,
    pacer: UdpDatagramPacer,
}

#[derive(Debug, Clone, Copy)]
struct UdpDatagramPacer {
    next_send_at: Instant,
}

impl UdpDatagramPacer {
    fn new() -> Self {
        Self {
            next_send_at: Instant::now(),
        }
    }

    fn ready_at(self) -> Instant {
        self.next_send_at
    }

    async fn wait_for_send(&mut self, model: UdpPathRuntimeModel, payload_bytes: usize) {
        let now = Instant::now();
        if self.next_send_at > now {
            tokio::time::sleep(self.next_send_at.duration_since(now)).await;
        }
        self.next_send_at = Instant::now() + model.pacing_interval(payload_bytes);
    }
}

enum UdpPathSendError {
    MtuExceeded {
        limit: usize,
    },
    Timeout {
        path_was_acked: bool,
        response_timeout: Duration,
    },
    Runtime(RuntimeError),
}

impl UdpDatagramClientAssociation {
    fn new(context: ClientPathContext) -> Result<Self, RuntimeError> {
        Ok(Self {
            context,
            session_id: random_session_id()?,
            paths: Vec::new(),
        })
    }

    async fn send_to(
        &mut self,
        target: TargetAddr,
        payload: Bytes,
        ttl_ms: u32,
    ) -> Result<Bytes, RuntimeError> {
        if payload.len() > self.context.mux_limits.max_payload_bytes {
            return Err(RuntimeError::Datagram(DatagramError::PayloadTooLarge {
                actual: payload.len(),
                limit: self.context.mux_limits.max_payload_bytes,
            }));
        }
        let candidates = self
            .context
            .ordered_udp_path_indices_for_ttl(payload.len(), ttl_ms);
        if candidates.is_empty() {
            return Err(RuntimeError::NoSchedulableUdpPath);
        }

        let mut attempted = HashSet::new();
        let mut last_retryable_error = None;
        while let Some(path_index) =
            self.select_path_candidate(&candidates, &attempted, payload.len(), ttl_ms)
        {
            attempted.insert(path_index);
            match self
                .send_to_path(path_index, target.clone(), payload.clone(), ttl_ms)
                .await
            {
                Ok(response) => return Ok(response),
                Err(UdpPathSendError::MtuExceeded { limit }) => {
                    last_retryable_error =
                        Some(RuntimeError::Datagram(DatagramError::PayloadTooLarge {
                            actual: payload.len(),
                            limit,
                        }));
                }
                Err(UdpPathSendError::Timeout {
                    path_was_acked,
                    response_timeout,
                }) => {
                    self.remove_path(path_index).await;
                    if !path_was_acked {
                        self.context.mark_udp_path_failure(path_index);
                    } else {
                        self.context.mark_udp_path_feedback(
                            path_index,
                            UdpDatagramPathObservation {
                                rtt: response_timeout,
                                jitter: Duration::ZERO,
                                loss_rate: 1.0,
                                rate_sample: None,
                            },
                        );
                    }
                    last_retryable_error =
                        Some(RuntimeError::Protocol("UDP datagram response timed out"));
                }
                Err(UdpPathSendError::Runtime(err))
                    if udp_datagram_error_is_path_retryable(&err) =>
                {
                    self.remove_path(path_index).await;
                    self.context.mark_udp_path_failure(path_index);
                    last_retryable_error = Some(err);
                }
                Err(UdpPathSendError::Runtime(err)) => return Err(err),
            }
        }
        Err(last_retryable_error.unwrap_or(RuntimeError::NoSchedulableUdpPath))
    }

    async fn close(&mut self) -> Result<(), RuntimeError> {
        let mut close_error = None;
        while let Some(mut path) = self.paths.pop() {
            let close_result = path.session.close().await;
            self.context
                .mark_udp_path_delivery(path.session.path_index, path.session.delivery_stats());
            self.context.release_udp_path_load(path.session.path_index);
            if close_error.is_none()
                && let Err(err) = close_result
            {
                close_error = Some(err);
            }
        }
        match close_error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    fn select_path_candidate(
        &self,
        candidates: &[usize],
        attempted: &HashSet<usize>,
        payload_bytes: usize,
        ttl_ms: u32,
    ) -> Option<usize> {
        if let Some(path_index) = candidates
            .iter()
            .copied()
            .filter(|path_index| !attempted.contains(path_index))
            .find(|path_index| {
                self.paths
                    .iter()
                    .all(|path| path.session.path_index != *path_index)
                    && self
                        .context
                        .udp_path_runtime_model(*path_index, ttl_ms)
                        .is_some_and(|model| model.accepts_or_can_probe(payload_bytes))
            })
        {
            return Some(path_index);
        }

        let now = Instant::now();
        candidates
            .iter()
            .enumerate()
            .filter(|(_, path_index)| !attempted.contains(path_index))
            .filter_map(|(rank, path_index)| {
                let model = self.context.udp_path_runtime_model(*path_index, ttl_ms)?;
                if !model.accepts_or_can_probe(payload_bytes) {
                    return None;
                }
                let ready_at = self
                    .paths
                    .iter()
                    .find(|path| path.session.path_index == *path_index)
                    .map(|path| path.pacer.ready_at())
                    .unwrap_or(now);
                Some((*path_index, ready_at, rank))
            })
            .min_by(|left, right| left.1.cmp(&right.1).then_with(|| left.2.cmp(&right.2)))
            .map(|(path_index, _, _)| path_index)
    }

    async fn send_to_path(
        &mut self,
        path_index: usize,
        target: TargetAddr,
        payload: Bytes,
        ttl_ms: u32,
    ) -> Result<Bytes, UdpPathSendError> {
        let model = self
            .context
            .udp_path_runtime_model(path_index, ttl_ms)
            .ok_or(UdpPathSendError::Runtime(
                RuntimeError::NoSchedulableUdpPath,
            ))?;
        if !model.accepts_or_can_probe(payload.len()) {
            return Err(UdpPathSendError::MtuExceeded {
                limit: model.mtu_payload_bytes,
            });
        }
        let position = self
            .ensure_path_session(path_index)
            .await
            .map_err(UdpPathSendError::Runtime)?;
        let current_mtu = self
            .paths
            .get(position)
            .ok_or(UdpPathSendError::Runtime(
                RuntimeError::NoSchedulableUdpPath,
            ))?
            .session
            .mtu_payload_bytes();
        if payload.len() > current_mtu {
            let probe_result = {
                let path = self
                    .paths
                    .get_mut(position)
                    .ok_or(UdpPathSendError::Runtime(
                        RuntimeError::NoSchedulableUdpPath,
                    ))?;
                tokio::time::timeout(
                    model.response_timeout,
                    path.session.probe_mtu(payload.len()),
                )
                .await
            };
            match probe_result {
                Ok(Ok(probed_mtu)) => {
                    self.context.mark_udp_path_mtu(path_index, probed_mtu);
                }
                Ok(Err(err)) if udp_datagram_error_is_path_retryable(&err) => {
                    self.context.mark_udp_path_mtu(path_index, current_mtu);
                    return Err(UdpPathSendError::MtuExceeded { limit: current_mtu });
                }
                Ok(Err(err)) => return Err(UdpPathSendError::Runtime(err)),
                Err(_) => {
                    self.context.mark_udp_path_mtu(path_index, current_mtu);
                    return Err(UdpPathSendError::MtuExceeded { limit: current_mtu });
                }
            }
        }
        let (path_was_acked, observation_path_index, observation, result) = {
            let path = self
                .paths
                .get_mut(position)
                .ok_or(UdpPathSendError::Runtime(
                    RuntimeError::NoSchedulableUdpPath,
                ))?;
            path.pacer.wait_for_send(model, payload.len()).await;
            let result = tokio::time::timeout(
                model.response_timeout,
                path.session.send_to(target, payload, ttl_ms),
            )
            .await;
            let observation = path.session.take_feedback_observation();
            let path_was_acked = observation.is_some();
            (path_was_acked, path.session.path_index, observation, result)
        };
        if let Some(observation) = observation {
            self.context
                .mark_udp_path_feedback(observation_path_index, observation);
        }

        match result {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(err)) => Err(UdpPathSendError::Runtime(err)),
            Err(_) => Err(UdpPathSendError::Timeout {
                path_was_acked,
                response_timeout: model.response_timeout,
            }),
        }
    }

    async fn ensure_path_session(&mut self, path_index: usize) -> Result<usize, RuntimeError> {
        if let Some(position) = self
            .paths
            .iter()
            .position(|path| path.session.path_index == path_index)
        {
            return Ok(position);
        }
        let session =
            open_udp_datagram_session_on_path(&self.context, path_index, self.session_id).await?;
        self.paths.push(UdpDatagramAssociationPath {
            session,
            pacer: UdpDatagramPacer::new(),
        });
        Ok(self.paths.len() - 1)
    }

    async fn remove_path(&mut self, path_index: usize) {
        let Some(position) = self
            .paths
            .iter()
            .position(|path| path.session.path_index == path_index)
        else {
            return;
        };
        let mut path = self.paths.swap_remove(position);
        let _ = path.session.close().await;
        self.context
            .mark_udp_path_delivery(path.session.path_index, path.session.delivery_stats());
        self.context.release_udp_path_load(path.session.path_index);
    }
}

fn udp_datagram_error_is_path_retryable(err: &RuntimeError) -> bool {
    matches!(
        err,
        RuntimeError::Io(_)
            | RuntimeError::Udp(_)
            | RuntimeError::EncryptedUdp(_)
            | RuntimeError::Auth(_)
            | RuntimeError::RemoteClosed(_)
            | RuntimeError::Protocol(_)
    )
}

struct UdpDatagramClientSession {
    encrypted: EncryptedUdpSocket,
    buffer: Vec<u8>,
    flows: Vec<UdpDatagramClientFlow>,
    next_flow_id: u64,
    mux_limits: MuxLimits,
    path_index: usize,
    path_id: PathId,
    stats: PathDeliveryStats,
    sent_datagrams: HashMap<(DatagramFlowId, DatagramId), UdpSentDatagram>,
    last_datagram_rtt: Option<Duration>,
    last_feedback_observation: Option<UdpDatagramPathObservation>,
    mtu_payload_bytes: usize,
}

struct UdpDatagramClientFlow {
    target: TargetAddr,
    flow: DatagramFlow,
    flow_id: DatagramFlowId,
}

#[derive(Debug, Clone, Copy)]
struct UdpSentDatagram {
    sent_at: Instant,
    bytes: usize,
    ttl: Duration,
}

impl UdpDatagramClientSession {
    async fn open(
        path: &PathSpec,
        path_index: usize,
        security: SecurityConfig,
        codec_limits: CodecLimits,
        mux_limits: MuxLimits,
        handshake_timeout: Duration,
    ) -> Result<Self, RuntimeError> {
        let session_id = random_session_id()?;
        Self::open_for_session(
            path,
            path_index,
            session_id,
            security,
            codec_limits,
            mux_limits,
            handshake_timeout,
        )
        .await
    }

    async fn open_for_session(
        path: &PathSpec,
        path_index: usize,
        session_id: SessionId,
        security: SecurityConfig,
        codec_limits: CodecLimits,
        mux_limits: MuxLimits,
        handshake_timeout: Duration,
    ) -> Result<Self, RuntimeError> {
        let socket = udp::connect_path(
            path,
            crate::transport::udp::UdpConnectOptions {
                timeout: handshake_timeout,
                ..crate::transport::udp::UdpConnectOptions::default()
            },
        )
        .await?;
        let mut encrypted = EncryptedUdpSocket::new(
            socket,
            security.secret.as_bytes(),
            PeerRole::Client,
            codec_limits,
        );
        let path_id = PathId(path_index as u16);
        let (session_hello, session_auth, path_join) = authenticated_path_join_frames_for_session(
            &security,
            path,
            path_id,
            UnderlayProtocol::Udp,
            session_id,
        )?;

        encrypted.send_frame(&session_hello).await?;
        encrypted.send_frame(&session_auth).await?;
        encrypted.send_frame(&path_join).await?;

        let mut buffer = vec![0u8; encrypted.max_datagram_bytes()?];
        let mut session_ready = false;
        let mut path_active = false;
        while !session_ready || !path_active {
            match tokio::time::timeout(handshake_timeout, encrypted.recv_frame(&mut buffer))
                .await
                .map_err(|_| RuntimeError::Protocol("UDP path handshake timed out"))??
            {
                Frame::SessionReady => session_ready = true,
                Frame::PathStatus { .. } => path_active = true,
                Frame::SessionClose { reason } => return Err(RuntimeError::RemoteClosed(reason)),
                _ => return Err(RuntimeError::Protocol("unexpected UDP handshake frame")),
            }
        }

        Ok(Self {
            encrypted,
            buffer,
            flows: Vec::new(),
            next_flow_id: 0,
            mux_limits,
            path_index,
            path_id,
            stats: PathDeliveryStats::default(),
            sent_datagrams: HashMap::new(),
            last_datagram_rtt: None,
            last_feedback_observation: None,
            mtu_payload_bytes: UDP_DEFAULT_MTU_PAYLOAD_BYTES,
        })
    }

    async fn send_to(
        &mut self,
        target: TargetAddr,
        payload: Bytes,
        ttl_ms: u32,
    ) -> Result<Bytes, RuntimeError> {
        let flow_id = self.ensure_flow(target).await?;
        let frame = {
            let flow = self
                .flows
                .iter_mut()
                .find(|flow| flow.flow_id == flow_id)
                .ok_or(RuntimeError::Protocol("missing UDP datagram flow"))?;
            flow.flow.enqueue(0, ttl_ms, payload)?;
            flow.flow
                .pop_frame(0)
                .ok_or(RuntimeError::Protocol("datagram expired before send"))?
        };
        let (request_datagram_id, request_len) = match &frame {
            Frame::DatagramData {
                datagram_id,
                payload,
                ..
            } => (*datagram_id, payload.len()),
            _ => return Err(RuntimeError::Protocol("unexpected queued datagram frame")),
        };
        self.sent_datagrams.insert(
            (flow_id, request_datagram_id),
            UdpSentDatagram {
                sent_at: Instant::now(),
                bytes: request_len,
                ttl: Duration::from_millis(u64::from(ttl_ms)),
            },
        );
        self.encrypted.send_frame(&frame).await?;

        loop {
            match self.encrypted.recv_frame(&mut self.buffer).await? {
                Frame::DatagramFeedback { flow_id, received } => {
                    self.handle_datagram_feedback(flow_id, &received)?;
                }
                Frame::DatagramData {
                    flow_id: response_flow_id,
                    datagram_id,
                    payload,
                    ..
                } if response_flow_id == flow_id => {
                    let request_ack = datagram_ack_range(request_datagram_id)?;
                    self.handle_datagram_feedback(flow_id, &[request_ack])?;
                    self.encrypted
                        .send_frame(&Frame::DatagramFeedback {
                            flow_id,
                            received: vec![datagram_ack_range(datagram_id)?],
                        })
                        .await?;
                    self.stats.record_payload_bytes(request_len);
                    self.stats.record_payload_bytes(payload.len());
                    return Ok(payload);
                }
                Frame::PathMetrics { metrics } if metrics.path_id == self.path_id => {
                    self.observe_remote_path_metrics(metrics);
                }
                Frame::RxRateHint { path_id, .. } if path_id == self.path_id => {}
                Frame::DatagramClose {
                    flow_id: closed_flow_id,
                } if closed_flow_id == flow_id => {
                    return Err(RuntimeError::Protocol("datagram flow closed"));
                }
                Frame::SessionClose { reason } => return Err(RuntimeError::RemoteClosed(reason)),
                _ => return Err(RuntimeError::Protocol("unexpected UDP datagram frame")),
            }
        }
    }

    async fn ensure_flow(&mut self, target: TargetAddr) -> Result<DatagramFlowId, RuntimeError> {
        if let Some(flow) = self.flows.iter().find(|flow| flow.target == target) {
            return Ok(flow.flow_id);
        }
        let flow_id = DatagramFlowId(self.next_flow_id);
        self.next_flow_id = self
            .next_flow_id
            .checked_add(1)
            .ok_or(RuntimeError::Protocol("UDP datagram flow id overflow"))?;
        self.encrypted
            .send_frame(&Frame::OpenDatagramFlow {
                flow_id,
                target: target.clone(),
                ingress: IngressKind::Socks5,
                outbound: OutboundPolicy::Direct,
                class: TrafficClass::RealtimeDatagram,
            })
            .await?;
        self.flows.push(UdpDatagramClientFlow {
            target,
            flow: DatagramFlow::new(flow_id, self.mux_limits),
            flow_id,
        });
        Ok(flow_id)
    }

    async fn close(&mut self) -> Result<(), RuntimeError> {
        for flow in &self.flows {
            self.encrypted
                .send_frame(&Frame::DatagramClose {
                    flow_id: flow.flow_id,
                })
                .await?;
        }
        self.flows.clear();
        Ok(())
    }

    async fn ping(&mut self, probe_timeout: Duration) -> Result<(), RuntimeError> {
        let nonce = random_u64()?;
        self.encrypted.send_frame(&Frame::Ping { nonce }).await?;
        match tokio::time::timeout(probe_timeout, self.encrypted.recv_frame(&mut self.buffer))
            .await
            .map_err(|_| RuntimeError::Protocol("UDP path probe ping timed out"))??
        {
            Frame::Pong {
                nonce: received_nonce,
            } if received_nonce == nonce => Ok(()),
            Frame::SessionClose { reason } => Err(RuntimeError::RemoteClosed(reason)),
            _ => Err(RuntimeError::Protocol("unexpected UDP path probe frame")),
        }
    }

    async fn close_session(&mut self) -> Result<(), RuntimeError> {
        self.encrypted
            .send_frame(&Frame::SessionClose {
                reason: CloseReason::Normal,
            })
            .await?;
        Ok(())
    }

    fn delivery_stats(&self) -> PathDeliveryStats {
        self.stats
    }

    fn mtu_payload_bytes(&self) -> usize {
        self.mtu_payload_bytes
    }

    async fn probe_mtu(&mut self, payload_bytes: usize) -> Result<usize, RuntimeError> {
        if payload_bytes <= self.mtu_payload_bytes {
            return Ok(self.mtu_payload_bytes);
        }
        if payload_bytes > self.mux_limits.max_payload_bytes {
            return Err(RuntimeError::Datagram(DatagramError::PayloadTooLarge {
                actual: payload_bytes,
                limit: self.mux_limits.max_payload_bytes,
            }));
        }
        let probe_id = random_u64()?;
        self.encrypted
            .send_frame(&Frame::PathMtuProbe {
                path_id: self.path_id,
                probe_id,
                payload: Bytes::from(vec![0u8; payload_bytes]),
            })
            .await?;
        loop {
            match self.encrypted.recv_frame(&mut self.buffer).await? {
                Frame::PathMtuAck {
                    path_id,
                    probe_id: received_probe_id,
                    payload_bytes: received_payload_bytes,
                } if path_id == self.path_id && received_probe_id == probe_id => {
                    let payload_bytes = received_payload_bytes as usize;
                    self.mtu_payload_bytes = payload_bytes;
                    return Ok(payload_bytes);
                }
                Frame::PathMetrics { metrics } if metrics.path_id == self.path_id => {
                    self.observe_remote_path_metrics(metrics);
                }
                Frame::RxRateHint { path_id, .. } if path_id == self.path_id => {}
                Frame::SessionClose { reason } => return Err(RuntimeError::RemoteClosed(reason)),
                _ => return Err(RuntimeError::Protocol("unexpected UDP MTU probe frame")),
            }
        }
    }

    fn take_feedback_observation(&mut self) -> Option<UdpDatagramPathObservation> {
        self.last_feedback_observation.take()
    }

    fn handle_datagram_feedback(
        &mut self,
        flow_id: DatagramFlowId,
        ranges: &[OffsetRange],
    ) -> Result<(), RuntimeError> {
        let now = Instant::now();
        let lost = self.expire_unacked_datagrams(now);
        let acked_keys = self
            .sent_datagrams
            .keys()
            .copied()
            .filter(|(pending_flow_id, datagram_id)| {
                *pending_flow_id == flow_id && datagram_id_is_in_ranges(*datagram_id, ranges)
            })
            .collect::<Vec<_>>();

        for key in acked_keys {
            if let Some(sent) = self.sent_datagrams.remove(&key) {
                self.observe_datagram_ack(sent, now, lost);
            }
        }
        Ok(())
    }

    fn observe_remote_path_metrics(&mut self, metrics: crate::protocol::PathMetrics) {
        self.last_feedback_observation = Some(UdpDatagramPathObservation {
            rtt: Duration::from_micros(u64::from(metrics.srtt_us)),
            jitter: Duration::from_micros(u64::from(metrics.jitter_us)),
            loss_rate: (f64::from(metrics.loss_ppm) / 1_000_000.0).clamp(0.0, 1.0),
            rate_sample: PathRateSample::new(
                metrics.delivery_rate_bps.max(8) / 8,
                Duration::from_secs(1),
            ),
        });
    }

    fn expire_unacked_datagrams(&mut self, now: Instant) -> u64 {
        let expired = self
            .sent_datagrams
            .iter()
            .filter_map(|(key, sent)| {
                (now.duration_since(sent.sent_at) >= sent.ttl).then_some(*key)
            })
            .collect::<Vec<_>>();
        let lost = expired.len() as u64;
        for key in expired {
            self.sent_datagrams.remove(&key);
        }
        lost
    }

    fn observe_datagram_ack(&mut self, sent: UdpSentDatagram, now: Instant, lost: u64) {
        let rtt = now
            .duration_since(sent.sent_at)
            .max(MIN_RATE_SAMPLE_DURATION);
        let jitter = self
            .last_datagram_rtt
            .map(|previous| previous.abs_diff(rtt))
            .unwrap_or(Duration::ZERO);
        self.last_datagram_rtt = Some(rtt);
        let delivered = 1_u64;
        let total = delivered.saturating_add(lost).max(1);
        self.last_feedback_observation = Some(UdpDatagramPathObservation {
            rtt,
            jitter,
            loss_rate: lost as f64 / total as f64,
            rate_sample: PathRateSample::new(sent.bytes as u64, rtt),
        });
    }
}

fn datagram_ack_range(datagram_id: DatagramId) -> Result<OffsetRange, RuntimeError> {
    let end = datagram_id
        .0
        .checked_add(1)
        .ok_or(RuntimeError::Protocol("datagram ACK range overflow"))?;
    OffsetRange::new(datagram_id.0, end).ok_or(RuntimeError::Protocol("invalid datagram ACK range"))
}

fn datagram_id_is_in_ranges(datagram_id: DatagramId, ranges: &[OffsetRange]) -> bool {
    ranges
        .iter()
        .any(|range| datagram_id.0 >= range.start && datagram_id.0 < range.end)
}

pub async fn handle_server_udp_datagram_path_session(
    socket: UdpSocket,
    context: ServerPathContext,
) -> Result<(), RuntimeError> {
    let socket = Arc::new(socket);
    let probe = EncryptedUdpSocket::from_shared(
        socket.clone(),
        context.security.secret.as_bytes(),
        PeerRole::Server,
        context.codec_limits,
    );
    let mut buffer = vec![0u8; probe.max_datagram_bytes()?];
    let mut session = None;
    loop {
        let (len, peer) = socket.recv_from(&mut buffer).await?;
        if session.is_none() {
            session = Some(ServerUdpPathSession::new(
                socket.clone(),
                peer,
                context.clone(),
            )?);
        }
        let session_ref = session
            .as_mut()
            .ok_or(RuntimeError::Protocol("missing UDP path session"))?;
        if session_ref.peer != peer {
            return Err(RuntimeError::Protocol(
                "UDP datagram arrived from unexpected peer",
            ));
        }
        let frame = session_ref.open_frame(&buffer[..len])?;
        match session_ref.handle_frame(frame).await? {
            ServerUdpSessionOutcome::Active => {}
            ServerUdpSessionOutcome::Closed => return Ok(()),
        }
    }
}

struct ServerUdpDatagramFlow {
    flow_id: DatagramFlowId,
    outbound_socket: outbound::OutboundUdpSocket,
    response_flow: DatagramFlow,
}

struct ServerUdpPathSession {
    peer: SocketAddr,
    encrypted: EncryptedUdpSocket,
    context: ServerPathContext,
    authenticator: SessionAuthenticator,
    state: ServerUdpPathState,
    flows: Vec<ServerUdpDatagramFlow>,
}

enum ServerUdpPathState {
    AwaitSessionHello,
    AwaitSessionAuth { session_id: SessionId },
    AwaitPathJoin { session_id: SessionId },
    Established,
}

enum ServerUdpSessionOutcome {
    Active,
    Closed,
}

impl ServerUdpPathSession {
    fn new(
        socket: Arc<UdpSocket>,
        peer: SocketAddr,
        context: ServerPathContext,
    ) -> Result<Self, RuntimeError> {
        let authenticator = SessionAuthenticator::new(context.security.secret.as_bytes())?;
        let encrypted = EncryptedUdpSocket::from_shared(
            socket,
            context.security.secret.as_bytes(),
            PeerRole::Server,
            context.codec_limits,
        );
        Ok(Self {
            peer,
            encrypted,
            context,
            authenticator,
            state: ServerUdpPathState::AwaitSessionHello,
            flows: Vec::new(),
        })
    }

    fn open_frame(&mut self, datagram: &[u8]) -> Result<Frame, RuntimeError> {
        Ok(self.encrypted.open_frame_datagram(datagram)?)
    }

    async fn handle_frame(
        &mut self,
        frame: Frame,
    ) -> Result<ServerUdpSessionOutcome, RuntimeError> {
        match (&self.state, frame) {
            (ServerUdpPathState::AwaitSessionHello, Frame::SessionHello { session_id }) => {
                self.state = ServerUdpPathState::AwaitSessionAuth { session_id };
                Ok(ServerUdpSessionOutcome::Active)
            }
            (
                ServerUdpPathState::AwaitSessionAuth { session_id },
                Frame::SessionAuth {
                    session_id: auth_session_id,
                    nonce,
                    auth_tag,
                },
            ) if auth_session_id == *session_id
                && self
                    .authenticator
                    .verify_session_auth(*session_id, nonce, auth_tag) =>
            {
                self.state = ServerUdpPathState::AwaitPathJoin {
                    session_id: *session_id,
                };
                Ok(ServerUdpSessionOutcome::Active)
            }
            (
                ServerUdpPathState::AwaitPathJoin { session_id },
                Frame::PathJoin {
                    session_id: join_session_id,
                    path_id,
                    underlay,
                    nonce,
                    capabilities,
                    auth_tag,
                },
            ) if join_session_id == *session_id
                && underlay == UnderlayProtocol::Udp
                && self.authenticator.verify_path_join(
                    *session_id,
                    path_id,
                    underlay,
                    nonce,
                    capabilities,
                    auth_tag,
                ) =>
            {
                self.encrypted
                    .send_frame_to(&Frame::SessionReady, self.peer)
                    .await?;
                self.encrypted
                    .send_frame_to(
                        &Frame::PathStatus {
                            path_id,
                            status: crate::protocol::PathStatus::Active,
                            capabilities,
                        },
                        self.peer,
                    )
                    .await?;
                self.state = ServerUdpPathState::Established;
                Ok(ServerUdpSessionOutcome::Active)
            }
            (ServerUdpPathState::Established, Frame::Ping { nonce }) => {
                self.encrypted
                    .send_frame_to(&Frame::Pong { nonce }, self.peer)
                    .await?;
                Ok(ServerUdpSessionOutcome::Active)
            }
            (
                ServerUdpPathState::Established,
                Frame::PathMtuProbe {
                    path_id,
                    probe_id,
                    payload,
                },
            ) => {
                self.encrypted
                    .send_frame_to(
                        &Frame::PathMtuAck {
                            path_id,
                            probe_id,
                            payload_bytes: payload.len() as u32,
                        },
                        self.peer,
                    )
                    .await?;
                Ok(ServerUdpSessionOutcome::Active)
            }
            (
                ServerUdpPathState::Established,
                Frame::OpenDatagramFlow {
                    flow_id, target, ..
                },
            ) => {
                if self.flows.iter().any(|flow| flow.flow_id == flow_id) {
                    return Err(RuntimeError::Protocol("duplicate UDP datagram flow"));
                }
                if self.flows.len() >= self.context.max_udp_flows_per_session {
                    self.encrypted
                        .send_frame_to(&Frame::DatagramClose { flow_id }, self.peer)
                        .await?;
                    return Ok(ServerUdpSessionOutcome::Active);
                }
                outbound::validate_target(&target)?;
                self.context.outbound.ensure_supports(TargetProtocol::Udp)?;
                let outbound_socket = match outbound::connect_udp(
                    &self.context.outbound,
                    &self.context.outbound_dns,
                    &target,
                    Duration::from_secs(10),
                )
                .await
                {
                    Ok(socket) => socket,
                    Err(err) => {
                        self.encrypted
                            .send_frame_to(&Frame::DatagramClose { flow_id }, self.peer)
                            .await?;
                        return Err(RuntimeError::OutboundConnect(err));
                    }
                };
                self.flows.push(ServerUdpDatagramFlow {
                    flow_id,
                    outbound_socket,
                    response_flow: DatagramFlow::new(flow_id, self.context.mux_limits),
                });
                Ok(ServerUdpSessionOutcome::Active)
            }
            (
                ServerUdpPathState::Established,
                Frame::DatagramData {
                    flow_id,
                    datagram_id,
                    ttl_ms,
                    payload,
                },
            ) => {
                if ttl_ms == 0 {
                    return Err(RuntimeError::Protocol("expired datagram received"));
                }
                let flow_index = self
                    .flows
                    .iter()
                    .position(|flow| flow.flow_id == flow_id)
                    .ok_or(RuntimeError::Protocol("unknown UDP datagram flow"))?;
                self.encrypted
                    .send_frame_to(
                        &Frame::DatagramFeedback {
                            flow_id,
                            received: vec![datagram_ack_range(datagram_id)?],
                        },
                        self.peer,
                    )
                    .await?;
                let flow = self
                    .flows
                    .get_mut(flow_index)
                    .ok_or(RuntimeError::Protocol("unknown UDP datagram flow"))?;
                flow.outbound_socket.send(&payload).await?;
                let mut response =
                    vec![0u8; self.context.mux_limits.max_payload_bytes.min(64 * 1024)];
                let len = tokio::time::timeout(
                    Duration::from_secs(1),
                    flow.outbound_socket.recv(&mut response),
                )
                .await
                .map_err(|_| RuntimeError::Protocol("UDP outbound response timed out"))??;
                response.truncate(len);
                flow.response_flow
                    .enqueue(0, ttl_ms, Bytes::from(response))?;
                let frame = flow
                    .response_flow
                    .pop_frame(0)
                    .ok_or(RuntimeError::Protocol("UDP response expired before send"))?;
                self.encrypted.send_frame_to(&frame, self.peer).await?;
                Ok(ServerUdpSessionOutcome::Active)
            }
            (ServerUdpPathState::Established, Frame::DatagramFeedback { .. }) => {
                Ok(ServerUdpSessionOutcome::Active)
            }
            (ServerUdpPathState::Established, Frame::DatagramClose { flow_id }) => {
                self.flows.retain(|flow| flow.flow_id != flow_id);
                if self.flows.is_empty() {
                    Ok(ServerUdpSessionOutcome::Closed)
                } else {
                    Ok(ServerUdpSessionOutcome::Active)
                }
            }
            (_, Frame::SessionClose { .. }) => Ok(ServerUdpSessionOutcome::Closed),
            _ => Err(RuntimeError::Protocol("unexpected UDP datagram path frame")),
        }
    }
}

async fn read_socks5_auth<S>(stream: &mut S) -> Result<socks5::AuthRequest, RuntimeError>
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

async fn read_socks5_command<S>(stream: &mut S) -> Result<socks5::CommandRequest, RuntimeError>
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

async fn read_http_connect<S>(stream: &mut S) -> Result<http_connect::ConnectRequest, RuntimeError>
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

fn random_session_id() -> Result<SessionId, RuntimeError> {
    Ok(SessionId(random_u64()?))
}

fn random_u64() -> Result<u64, RuntimeError> {
    let mut bytes = [0u8; 8];
    getrandom::getrandom(&mut bytes).map_err(RuntimeError::Random)?;
    Ok(u64::from_be_bytes(bytes))
}

fn random_nonce() -> Result<AuthNonce, RuntimeError> {
    let mut bytes = [0u8; 16];
    getrandom::getrandom(&mut bytes).map_err(RuntimeError::Random)?;
    Ok(AuthNonce(bytes))
}

#[derive(Debug)]
pub enum RuntimeError {
    Io(std::io::Error),
    Tcp(TcpTransportError),
    Udp(UdpTransportError),
    Encrypted(EncryptedFramedTransportError),
    EncryptedUdp(EncryptedUdpTransportError),
    Auth(AuthError),
    Random(getrandom::Error),
    Socks5(Socks5Error),
    HttpConnect(HttpConnectError),
    Outbound(outbound::OutboundError),
    OutboundConnect(outbound::OutboundConnectError),
    Stream(StreamError),
    Datagram(DatagramError),
    PathSpec(PathSpecParseError),
    TunDevice(std::io::Error),
    TaskJoin(tokio::task::JoinError),
    NoTcpPath,
    NoUdpPath,
    NoSchedulableTcpPath,
    NoSchedulableUdpPath,
    PathIdOverflow,
    PathHeartbeatTimeout,
    TcpPathSessionClosed,
    RemoteReset(ResetReason),
    RemoteClosed(CloseReason),
    Protocol(&'static str),
}

impl From<std::io::Error> for RuntimeError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<TcpTransportError> for RuntimeError {
    fn from(value: TcpTransportError) -> Self {
        Self::Tcp(value)
    }
}

impl From<UdpTransportError> for RuntimeError {
    fn from(value: UdpTransportError) -> Self {
        Self::Udp(value)
    }
}

impl From<EncryptedFramedTransportError> for RuntimeError {
    fn from(value: EncryptedFramedTransportError) -> Self {
        Self::Encrypted(value)
    }
}

impl From<EncryptedUdpTransportError> for RuntimeError {
    fn from(value: EncryptedUdpTransportError) -> Self {
        Self::EncryptedUdp(value)
    }
}

impl From<AuthError> for RuntimeError {
    fn from(value: AuthError) -> Self {
        Self::Auth(value)
    }
}

impl From<Socks5Error> for RuntimeError {
    fn from(value: Socks5Error) -> Self {
        Self::Socks5(value)
    }
}

impl From<HttpConnectError> for RuntimeError {
    fn from(value: HttpConnectError) -> Self {
        Self::HttpConnect(value)
    }
}

impl From<outbound::OutboundError> for RuntimeError {
    fn from(value: outbound::OutboundError) -> Self {
        Self::Outbound(value)
    }
}

impl From<outbound::OutboundConnectError> for RuntimeError {
    fn from(value: outbound::OutboundConnectError) -> Self {
        Self::OutboundConnect(value)
    }
}

impl From<StreamError> for RuntimeError {
    fn from(value: StreamError) -> Self {
        Self::Stream(value)
    }
}

impl From<DatagramError> for RuntimeError {
    fn from(value: DatagramError) -> Self {
        Self::Datagram(value)
    }
}

impl From<PathSpecParseError> for RuntimeError {
    fn from(value: PathSpecParseError) -> Self {
        Self::PathSpec(value)
    }
}

impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "{err}"),
            Self::Tcp(err) => write!(f, "{err}"),
            Self::Udp(err) => write!(f, "{err}"),
            Self::Encrypted(err) => write!(f, "{err}"),
            Self::EncryptedUdp(err) => write!(f, "{err}"),
            Self::Auth(err) => write!(f, "{err}"),
            Self::Random(err) => write!(f, "random source failed: {err}"),
            Self::Socks5(err) => write!(f, "{err}"),
            Self::HttpConnect(err) => write!(f, "{err}"),
            Self::Outbound(err) => write!(f, "{err}"),
            Self::OutboundConnect(err) => write!(f, "{err}"),
            Self::Stream(err) => write!(f, "{err}"),
            Self::Datagram(err) => write!(f, "{err}"),
            Self::PathSpec(err) => write!(f, "{err}"),
            Self::TunDevice(err) => write!(
                f,
                "failed to create TUN device: {err}; {}",
                platform::tun_privilege_hint()
            ),
            Self::TaskJoin(err) => write!(f, "runtime task failed: {err}"),
            Self::NoTcpPath => write!(f, "runtime operation requires at least one TCP path"),
            Self::NoUdpPath => write!(f, "runtime operation requires at least one UDP path"),
            Self::NoSchedulableTcpPath => {
                write!(f, "no configured TCP path is schedulable for this flow")
            }
            Self::NoSchedulableUdpPath => {
                write!(
                    f,
                    "no configured UDP path is schedulable for this datagram flow"
                )
            }
            Self::PathIdOverflow => write!(f, "configured paths exceed protocol path ID space"),
            Self::PathHeartbeatTimeout => write!(f, "TCP path heartbeat timed out"),
            Self::TcpPathSessionClosed => write!(f, "TCP path session closed"),
            Self::RemoteReset(reason) => write!(f, "remote reset stream: {reason:?}"),
            Self::RemoteClosed(reason) => write!(f, "remote closed session: {reason:?}"),
            Self::Protocol(message) => write!(f, "protocol error: {message}"),
        }
    }
}

impl std::error::Error for RuntimeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            Self::Tcp(err) => Some(err),
            Self::Udp(err) => Some(err),
            Self::Encrypted(err) => Some(err),
            Self::EncryptedUdp(err) => Some(err),
            Self::Auth(err) => Some(err),
            Self::Random(_) => None,
            Self::Socks5(err) => Some(err),
            Self::HttpConnect(err) => Some(err),
            Self::Outbound(err) => Some(err),
            Self::OutboundConnect(err) => Some(err),
            Self::Stream(err) => Some(err),
            Self::Datagram(err) => Some(err),
            Self::PathSpec(err) => Some(err),
            Self::TunDevice(err) => Some(err),
            Self::TaskJoin(err) => Some(err),
            Self::NoTcpPath
            | Self::NoUdpPath
            | Self::NoSchedulableTcpPath
            | Self::NoSchedulableUdpPath
            | Self::PathIdOverflow
            | Self::PathHeartbeatTimeout
            | Self::TcpPathSessionClosed
            | Self::RemoteReset(_)
            | Self::RemoteClosed(_)
            | Self::Protocol(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{SharedSecret, TcpPortClassRule, TcpTrafficClass, TrafficPolicy};
    use crate::transport::Endpoint;
    use crate::transport::tcp::bind_listener;
    use tokio::io::duplex;

    fn security() -> SecurityConfig {
        SecurityConfig::encrypted(SharedSecret::new(b"0123456789abcdef".to_vec()).expect("secret"))
    }

    fn server_context(outbound: OutboundConfig) -> ServerPathContext {
        let resources = ResourceLimits::default();
        ServerPathContext {
            outbound,
            outbound_dns: DnsConfig::default(),
            codec_limits: resources.into(),
            mux_limits: resources.into(),
            security: security(),
            tcp_streams: Arc::new(ServerTcpStreamRegistry::default()),
            max_tcp_streams: resources.max_streams,
            max_udp_sessions: resources.max_streams,
            max_udp_flows_per_session: resources.max_streams,
        }
    }

    #[test]
    fn tun_udp_dns_target_uses_configured_matching_resolver() {
        let tun = TunL4Config {
            dns_resolvers: vec![
                "[2606:4700:4700::1111]:5353".parse().expect("resolver"),
                "1.1.1.1:5353".parse().expect("resolver"),
            ],
            ..TunL4Config::default()
        };

        assert_eq!(
            tun_udp_target_for_remote("8.8.8.8:53".parse().expect("remote"), &tun),
            "1.1.1.1:5353".parse().expect("resolver")
        );
        assert_eq!(
            tun_udp_target_for_remote("[2001:4860:4860::8888]:53".parse().expect("remote"), &tun),
            "[2606:4700:4700::1111]:5353".parse().expect("resolver")
        );
        assert_eq!(
            tun_udp_target_for_remote("8.8.8.8:443".parse().expect("remote"), &tun),
            "8.8.8.8:443".parse().expect("remote")
        );
    }

    async fn reserve_tcp_path() -> PathSpec {
        let probe = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("reserve port");
        let port = probe.local_addr().expect("reserved addr").port();
        drop(probe);
        format!("tcp://127.0.0.1:{port}").parse().expect("path")
    }

    async fn reserve_tcp_path_with_query(query: &str) -> PathSpec {
        let probe = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("reserve port");
        let port = probe.local_addr().expect("reserved addr").port();
        drop(probe);
        format!("tcp://127.0.0.1:{port}?{query}")
            .parse()
            .expect("path")
    }

    async fn spawn_echo_target() -> (SocketAddr, tokio::task::JoinHandle<()>) {
        spawn_echo_target_count(1).await
    }

    async fn spawn_echo_target_count(count: usize) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("target bind");
        let addr = listener.local_addr().expect("target addr");
        let handle = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            for _ in 0..count {
                let (mut stream, _) = listener.accept().await.expect("target accept");
                connections.spawn(async move {
                    let mut buf = [0u8; 4];
                    stream.read_exact(&mut buf).await.expect("target read");
                    assert_eq!(&buf, b"ping");
                    stream.write_all(b"pong").await.expect("target write");
                    stream.shutdown().await.expect("target shutdown");
                });
            }
            while let Some(connection) = connections.join_next().await {
                connection.expect("target connection");
            }
        });
        (addr, handle)
    }

    async fn spawn_udp_echo_target() -> (SocketAddr, tokio::task::JoinHandle<()>) {
        spawn_udp_echo_target_count(1).await
    }

    async fn spawn_udp_echo_target_count(
        count: usize,
    ) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let socket = UdpSocket::bind("127.0.0.1:0").await.expect("target bind");
        let addr = socket.local_addr().expect("target addr");
        let handle = tokio::spawn(async move {
            let mut buf = [0u8; 16];
            for _ in 0..count {
                let (len, peer) = socket.recv_from(&mut buf).await.expect("target recv");
                assert_eq!(&buf[..len], b"ping");
                socket.send_to(b"pong", peer).await.expect("target send");
            }
        });
        (addr, handle)
    }

    async fn spawn_udp_payload_target(
        expected: Bytes,
        response: Bytes,
    ) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let socket = UdpSocket::bind("127.0.0.1:0").await.expect("target bind");
        let addr = socket.local_addr().expect("target addr");
        let handle = tokio::spawn(async move {
            let mut buf = vec![0u8; expected.len().max(16)];
            let (len, peer) = socket.recv_from(&mut buf).await.expect("target recv");
            assert_eq!(&buf[..len], expected.as_ref());
            socket
                .send_to(response.as_ref(), peer)
                .await
                .expect("target send");
        });
        (addr, handle)
    }

    async fn spawn_socks5_udp_proxy_once() -> (Endpoint, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("proxy bind");
        let proxy: Endpoint = listener
            .local_addr()
            .expect("proxy addr")
            .to_string()
            .parse()
            .expect("proxy endpoint");
        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("proxy accept");
            let mut greeting = [0u8; 3];
            stream
                .read_exact(&mut greeting)
                .await
                .expect("proxy greeting");
            assert_eq!(greeting, crate::outbound::socks5::no_auth_greeting());
            stream.write_all(&[0x05, 0x00]).await.expect("proxy method");

            let mut request = [0u8; 10];
            stream
                .read_exact(&mut request)
                .await
                .expect("udp associate request");
            assert_eq!(
                request.as_slice(),
                crate::outbound::socks5::udp_associate_request(
                    "0.0.0.0:0".parse().expect("client endpoint")
                )
                .expect("expected request")
            );

            let relay = UdpSocket::bind("127.0.0.1:0")
                .await
                .expect("udp relay bind");
            let relay_addr = relay.local_addr().expect("relay addr");
            stream
                .write_all(&socks5::connect_reply(Socks5Reply::Succeeded, relay_addr))
                .await
                .expect("associate reply");

            let mut packet = [0u8; 512];
            let (len, peer) = relay.recv_from(&mut packet).await.expect("udp relay recv");
            let (datagram, consumed) =
                socks5::parse_udp_datagram(&packet[..len]).expect("udp relay packet");
            assert_eq!(consumed, len);
            assert_eq!(
                datagram.target,
                TargetAddr::Domain {
                    host: "example.com".to_string(),
                    port: 53,
                }
            );
            assert_eq!(datagram.payload, Bytes::from_static(b"ping"));
            let response =
                socks5::udp_datagram(&datagram.target, b"pong").expect("udp relay response");
            relay
                .send_to(&response, peer)
                .await
                .expect("udp relay send");
        });
        (proxy, handle)
    }

    async fn spawn_server_path(
        outbound: OutboundConfig,
    ) -> (PathSpec, tokio::task::JoinHandle<Result<(), RuntimeError>>) {
        let path = reserve_tcp_path().await;
        let listener = bind_listener(&path).await.expect("bind");
        let handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            handle_server_path(stream, server_context(outbound)).await
        });
        (path, handle)
    }

    async fn spawn_tcp_relay_heartbeat_blackhole(
        hold_after_ping: Duration,
    ) -> (PathSpec, tokio::task::JoinHandle<Result<(), RuntimeError>>) {
        let path = reserve_tcp_path().await;
        let listener = bind_listener(&path).await.expect("bind");
        let handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let security = security();
            let mut framed = EncryptedFramedStream::new(
                stream,
                security.secret.as_bytes(),
                PeerRole::Server,
                CodecLimits::default(),
            );
            let session_id = match framed.read_frame().await? {
                Frame::SessionHello { session_id } => session_id,
                _ => return Err(RuntimeError::Protocol("expected SESSION_HELLO")),
            };
            let authenticator = SessionAuthenticator::new(security.secret.as_bytes())?;
            match framed.read_frame().await? {
                Frame::SessionAuth {
                    session_id: auth_session_id,
                    nonce,
                    auth_tag,
                } if auth_session_id == session_id
                    && authenticator.verify_session_auth(session_id, nonce, auth_tag) => {}
                _ => return Err(RuntimeError::Protocol("invalid SESSION_AUTH")),
            }
            let (path_id, capabilities) = match framed.read_frame().await? {
                Frame::PathJoin {
                    session_id: join_session_id,
                    path_id,
                    underlay,
                    nonce,
                    capabilities,
                    auth_tag,
                } if join_session_id == session_id
                    && underlay == UnderlayProtocol::Tcp
                    && authenticator.verify_path_join(
                        session_id,
                        path_id,
                        underlay,
                        nonce,
                        capabilities,
                        auth_tag,
                    ) =>
                {
                    (path_id, capabilities)
                }
                _ => return Err(RuntimeError::Protocol("invalid PATH_JOIN")),
            };
            let resources = ResourceLimits::default();
            framed.write_frame(&Frame::SessionReady).await?;
            framed
                .write_frame(&Frame::PathStatus {
                    path_id,
                    status: crate::protocol::PathStatus::Active,
                    capabilities,
                })
                .await?;
            framed.flush().await?;

            let stream_id = match framed.read_frame().await? {
                Frame::OpenStream { stream_id, .. } => stream_id,
                _ => return Err(RuntimeError::Protocol("expected OPEN_STREAM")),
            };

            framed
                .write_frame(&Frame::StreamMaxData {
                    stream_id,
                    max_offset: resources.max_stream_window_bytes,
                })
                .await?;
            framed.flush().await?;

            loop {
                match framed.read_frame().await? {
                    Frame::Ping { .. } => {
                        tokio::time::sleep(hold_after_ping).await;
                        return Ok(());
                    }
                    Frame::StreamAck {
                        stream_id: ack_stream_id,
                        ..
                    }
                    | Frame::StreamData {
                        stream_id: ack_stream_id,
                        ..
                    }
                    | Frame::StreamFin {
                        stream_id: ack_stream_id,
                    } if ack_stream_id == stream_id => {}
                    Frame::SessionClose { .. } => return Ok(()),
                    _ => return Err(RuntimeError::Protocol("unexpected heartbeat test frame")),
                }
            }
        });
        (path, handle)
    }

    async fn spawn_notified_server_path(
        path: PathSpec,
        marker: u8,
        outbound: OutboundConfig,
        accepted: mpsc::Sender<u8>,
    ) -> tokio::task::JoinHandle<Result<(), RuntimeError>> {
        let listener = bind_listener(&path).await.expect("bind");
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let _ = accepted.send(marker).await;
            handle_server_path(stream, server_context(outbound)).await
        })
    }

    async fn reserve_udp_path() -> PathSpec {
        let probe = UdpSocket::bind("127.0.0.1:0").await.expect("reserve port");
        let port = probe.local_addr().expect("reserved addr").port();
        drop(probe);
        format!("udp://127.0.0.1:{port}").parse().expect("path")
    }

    async fn reserve_udp_path_with_query(query: &str) -> PathSpec {
        let probe = UdpSocket::bind("127.0.0.1:0").await.expect("reserve port");
        let port = probe.local_addr().expect("reserved addr").port();
        drop(probe);
        format!("udp://127.0.0.1:{port}?{query}")
            .parse()
            .expect("path")
    }

    async fn spawn_udp_datagram_blackhole_path(
        path: PathSpec,
    ) -> tokio::task::JoinHandle<Result<(), RuntimeError>> {
        let socket = udp::bind_socket(&path).await.expect("bind udp path");
        tokio::spawn(async move {
            let socket = Arc::new(socket);
            let probe = EncryptedUdpSocket::from_shared(
                socket.clone(),
                security().secret.as_bytes(),
                PeerRole::Server,
                CodecLimits::default(),
            );
            let mut buffer = vec![0u8; probe.max_datagram_bytes()?];
            let mut session = None;
            loop {
                let (len, peer) = socket.recv_from(&mut buffer).await?;
                if session.is_none() {
                    session = Some(ServerUdpPathSession::new(
                        socket.clone(),
                        peer,
                        server_context(OutboundConfig::Direct),
                    )?);
                }
                let session_ref = session
                    .as_mut()
                    .ok_or(RuntimeError::Protocol("missing UDP path session"))?;
                if session_ref.peer != peer {
                    return Err(RuntimeError::Protocol(
                        "UDP datagram arrived from unexpected peer",
                    ));
                }
                let frame = session_ref.open_frame(&buffer[..len])?;
                if matches!(frame, Frame::DatagramData { .. }) {
                    return Ok(());
                }
                match session_ref.handle_frame(frame).await? {
                    ServerUdpSessionOutcome::Active => {}
                    ServerUdpSessionOutcome::Closed => return Ok(()),
                }
            }
        })
    }

    async fn spawn_udp_datagram_ack_then_drop_path(
        path: PathSpec,
    ) -> tokio::task::JoinHandle<Result<(), RuntimeError>> {
        let socket = udp::bind_socket(&path).await.expect("bind udp path");
        tokio::spawn(async move {
            let socket = Arc::new(socket);
            let probe = EncryptedUdpSocket::from_shared(
                socket.clone(),
                security().secret.as_bytes(),
                PeerRole::Server,
                CodecLimits::default(),
            );
            let mut buffer = vec![0u8; probe.max_datagram_bytes()?];
            let mut session = None;
            loop {
                let (len, peer) = socket.recv_from(&mut buffer).await?;
                if session.is_none() {
                    session = Some(ServerUdpPathSession::new(
                        socket.clone(),
                        peer,
                        server_context(OutboundConfig::Direct),
                    )?);
                }
                let session_ref = session
                    .as_mut()
                    .ok_or(RuntimeError::Protocol("missing UDP path session"))?;
                if session_ref.peer != peer {
                    return Err(RuntimeError::Protocol(
                        "UDP datagram arrived from unexpected peer",
                    ));
                }
                let frame = session_ref.open_frame(&buffer[..len])?;
                match frame {
                    Frame::DatagramData {
                        flow_id,
                        datagram_id,
                        ..
                    } => {
                        session_ref
                            .encrypted
                            .send_frame_to(
                                &Frame::DatagramFeedback {
                                    flow_id,
                                    received: vec![datagram_ack_range(datagram_id)?],
                                },
                                session_ref.peer,
                            )
                            .await?;
                        return Ok(());
                    }
                    frame => match session_ref.handle_frame(frame).await? {
                        ServerUdpSessionOutcome::Active => {}
                        ServerUdpSessionOutcome::Closed => return Ok(()),
                    },
                }
            }
        })
    }

    async fn drive_socks5_echo_client<S>(client: &mut S, target_addr: SocketAddr)
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        client
            .write_all(&[0x05, 0x01, 0x00])
            .await
            .expect("auth request");
        let mut auth_response = [0u8; 2];
        tokio::time::timeout(
            Duration::from_secs(2),
            client.read_exact(&mut auth_response),
        )
        .await
        .expect("auth timeout")
        .expect("auth response");
        assert_eq!(auth_response, [0x05, 0x00]);

        let mut connect = vec![0x05, 0x01, 0x00, 0x01];
        match target_addr {
            SocketAddr::V4(addr) => {
                connect.extend_from_slice(&addr.ip().octets());
                connect.extend_from_slice(&addr.port().to_be_bytes());
            }
            SocketAddr::V6(_) => panic!("expected IPv4 test target"),
        }
        client.write_all(&connect).await.expect("connect");

        let mut response = [0u8; 10];
        client.read_exact(&mut response).await.expect("reply");
        assert_eq!(response[1], Socks5Reply::Succeeded as u8);

        client.write_all(b"ping").await.expect("payload write");
        client.shutdown().await.expect("client shutdown");
        let mut payload = [0u8; 4];
        client.read_exact(&mut payload).await.expect("payload read");
        assert_eq!(&payload, b"pong");
    }

    #[test]
    fn tcp_relay_read_budget_is_ack_gated() {
        let mut mux_limits = MuxLimits {
            max_payload_bytes: 64 * 1024,
            max_ack_ranges: 256,
            max_stream_window_bytes: 1024 * 1024,
            max_repair_bytes: 1024 * 1024,
            max_reorder_bytes: 1024 * 1024,
            max_datagram_queue_bytes: 1024 * 1024,
            max_tcp_path_inflight_bytes: 32 * 1024,
            max_tcp_relay_chunk_bytes: 32 * 1024,
            tcp_path_heartbeat_interval: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_INTERVAL,
            tcp_path_heartbeat_timeout: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_TIMEOUT,
        };
        let mut send_stream = ReliableSendStream::new(StreamId(9), mux_limits);

        assert!(tcp_relay_can_read(&send_stream, mux_limits));
        assert_eq!(
            tcp_relay_read_budget(&send_stream, mux_limits, 64 * 1024),
            32 * 1024
        );

        send_stream
            .send_data(Bytes::from(vec![0u8; 8 * 1024]), StreamFlags::NONE)
            .expect("first send");
        assert_eq!(
            tcp_relay_read_budget(&send_stream, mux_limits, 64 * 1024),
            24 * 1024
        );

        send_stream
            .send_data(Bytes::from(vec![0u8; 24 * 1024]), StreamFlags::NONE)
            .expect("second send");
        assert!(!tcp_relay_can_read(&send_stream, mux_limits));
        assert_eq!(
            tcp_relay_read_budget(&send_stream, mux_limits, 64 * 1024),
            0
        );

        send_stream.apply_ack(&[crate::protocol::OffsetRange {
            start: 0,
            end: 8 * 1024,
        }]);
        assert!(tcp_relay_can_read(&send_stream, mux_limits));
        assert_eq!(
            tcp_relay_read_budget(&send_stream, mux_limits, 64 * 1024),
            8 * 1024
        );

        mux_limits.max_tcp_path_inflight_bytes = 64 * 1024;
        assert_eq!(
            tcp_relay_read_budget(&send_stream, mux_limits, 16 * 1024),
            16 * 1024
        );
    }

    #[test]
    fn tcp_stream_frame_queue_tracks_relay_chunk_byte_budget() {
        let mux_limits = MuxLimits {
            max_payload_bytes: 1024 * 1024,
            max_ack_ranges: 256,
            max_stream_window_bytes: 16 * 1024 * 1024,
            max_repair_bytes: 16 * 1024 * 1024,
            max_reorder_bytes: 16 * 1024 * 1024,
            max_datagram_queue_bytes: 4 * 1024 * 1024,
            max_tcp_path_inflight_bytes: 4 * 1024 * 1024,
            max_tcp_relay_chunk_bytes: 256 * 1024,
            tcp_path_heartbeat_interval: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_INTERVAL,
            tcp_path_heartbeat_timeout: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_TIMEOUT,
        };

        assert_eq!(
            tcp_stream_frame_queue(mux_limits),
            (mux_limits.max_reorder_bytes / mux_limits.max_tcp_relay_chunk_bytes) + 4
        );
    }

    #[test]
    fn auto_tcp_class_promotes_after_runtime_bdp_threshold() {
        let mux_limits = MuxLimits::default();
        let path = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 20.0, 30_000_000.0);
        let threshold = tcp_auto_bulk_threshold_bytes(Some(path), mux_limits);
        let high_bdp_path =
            PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 180.0, 300_000_000.0);
        let high_bdp_threshold = tcp_auto_bulk_threshold_bytes(Some(high_bdp_path), mux_limits);
        let high_bdp = ((high_bdp_path.delivery_rate_bps / 8.0) * (high_bdp_path.srtt_ms / 1000.0))
            .ceil() as u64;
        let mut state = TcpRelayClassState::new(TcpTrafficClass::Auto);

        assert!(threshold >= (tcp_relay_buffer_len(mux_limits) as u64).saturating_mul(4));
        assert!(high_bdp_threshold < high_bdp);
        assert!(high_bdp_threshold >= high_bdp / 4);

        let before = state.refresh(Some(path), threshold.saturating_sub(1), 0, 0, mux_limits);
        assert_eq!(before.class, TrafficClass::Interactive);
        assert!(!before.promoted_to_bulk);

        let after = state.refresh(Some(path), threshold, 0, 0, mux_limits);
        assert_eq!(after.class, TrafficClass::Bulk);
        assert!(after.promoted_to_bulk);

        let steady = state.refresh(Some(path), threshold.saturating_mul(2), 0, 0, mux_limits);
        assert_eq!(steady.class, TrafficClass::Bulk);
        assert!(!steady.promoted_to_bulk);
    }

    #[test]
    fn adaptive_tcp_budgets_expand_for_bulk_and_shrink_under_instability() {
        let mux_limits = MuxLimits {
            max_tcp_relay_chunk_bytes: 1024 * 1024,
            ..MuxLimits::default()
        };
        let stable = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 120.0, 300_000_000.0);
        let mut unstable = stable;
        unstable.loss_rate = 0.25;
        unstable.jitter_ms = 120.0;
        unstable.queue_bytes = 8 * 1024 * 1024;

        let interactive_chunk =
            adaptive_tcp_relay_chunk_bytes(Some(stable), TrafficClass::Interactive, mux_limits);
        let bulk_chunk =
            adaptive_tcp_relay_chunk_bytes(Some(stable), TrafficClass::Bulk, mux_limits);
        let unstable_bulk_chunk =
            adaptive_tcp_relay_chunk_bytes(Some(unstable), TrafficClass::Bulk, mux_limits);
        assert!(bulk_chunk > interactive_chunk);
        assert!(unstable_bulk_chunk < bulk_chunk);

        let interactive_inflight =
            adaptive_tcp_relay_inflight_bytes(Some(stable), TrafficClass::Interactive, mux_limits);
        let bulk_inflight =
            adaptive_tcp_relay_inflight_bytes(Some(stable), TrafficClass::Bulk, mux_limits);
        let unstable_bulk_inflight =
            adaptive_tcp_relay_inflight_bytes(Some(unstable), TrafficClass::Bulk, mux_limits);
        assert!(bulk_inflight >= interactive_inflight);
        assert!(unstable_bulk_inflight < bulk_inflight);
    }

    #[test]
    fn tcp_relay_stall_timeout_is_adaptive_and_bounded_for_fluent_failover() {
        let low_latency = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 20.0, 30_000_000.0);
        let mut cross_continent =
            PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 900.0, 300_000_000.0);
        cross_continent.jitter_ms = 400.0;

        assert_eq!(
            tcp_relay_stall_timeout(Some(low_latency), TrafficClass::Interactive),
            TCP_STREAM_STALL_MIN_TIMEOUT
        );
        assert!(
            tcp_relay_stall_timeout(Some(cross_continent), TrafficClass::Bulk)
                <= TCP_STREAM_STALL_MAX_TIMEOUT
        );
        assert!(TCP_STREAM_STALL_MAX_TIMEOUT < Duration::from_secs(5));
    }

    #[test]
    fn tcp_relay_stall_watch_ignores_idle_streams_and_tracks_repairable_work() {
        let mux_limits = MuxLimits::default();
        let mut send_stream = ReliableSendStream::new(StreamId(11), mux_limits);
        let mut recv_stream = ReliableRecvStream::new(StreamId(11), mux_limits);

        assert!(!tcp_relay_stall_watch_active(
            &send_stream,
            &recv_stream,
            false,
            TrafficClass::Interactive,
            mux_limits
        ));
        assert!(!tcp_relay_stall_watch_active(
            &send_stream,
            &recv_stream,
            true,
            TrafficClass::Interactive,
            mux_limits
        ));

        send_stream
            .send_data(Bytes::from_static(b"request"), StreamFlags::NONE)
            .expect("request data");
        assert!(tcp_relay_stall_watch_active(
            &send_stream,
            &recv_stream,
            true,
            TrafficClass::Interactive,
            mux_limits
        ));
        send_stream.apply_ack(&[crate::protocol::OffsetRange { start: 0, end: 7 }]);
        assert!(!tcp_relay_stall_watch_active(
            &send_stream,
            &recv_stream,
            true,
            TrafficClass::Interactive,
            mux_limits
        ));

        recv_stream
            .receive_data(0, Bytes::from_static(b"response"), StreamFlags::NONE)
            .expect("response data");
        assert!(!tcp_relay_stall_watch_active(
            &send_stream,
            &recv_stream,
            true,
            TrafficClass::Interactive,
            mux_limits
        ));
        assert!(tcp_relay_stall_watch_active(
            &send_stream,
            &recv_stream,
            true,
            TrafficClass::Bulk,
            mux_limits
        ));
        assert!(!tcp_relay_stall_watch_active(
            &send_stream,
            &recv_stream,
            false,
            TrafficClass::Bulk,
            mux_limits
        ));

        let response_watch_bytes = tcp_relay_response_stall_watch_bytes(mux_limits);
        let current_offset = recv_stream.next_offset();
        let fill_bytes = response_watch_bytes.saturating_sub(current_offset);
        let first_fill = fill_bytes.min(mux_limits.max_payload_bytes as u64) as usize;
        recv_stream
            .receive_data(
                current_offset,
                Bytes::from(vec![0u8; first_fill]),
                StreamFlags::NONE,
            )
            .expect("first sustained response data");
        let remaining = response_watch_bytes.saturating_sub(recv_stream.next_offset());
        if remaining > 0 {
            recv_stream
                .receive_data(
                    recv_stream.next_offset(),
                    Bytes::from(vec![0u8; remaining as usize]),
                    StreamFlags::NONE,
                )
                .expect("second sustained response data");
        }
        assert!(tcp_relay_stall_watch_active(
            &send_stream,
            &recv_stream,
            true,
            TrafficClass::Interactive,
            mux_limits
        ));
    }

    #[tokio::test]
    async fn server_tcp_binding_reselects_blocked_data_send_after_path_update() {
        let (old_tx, _old_rx) = mpsc::channel(1);
        old_tx
            .send(TcpPathSessionCommand::SendFrame(Frame::Ping { nonce: 1 }))
            .await
            .expect("fill old path command queue");
        let binding = ServerTcpStreamBinding::new(PathId(0), old_tx);
        let send_binding = binding.clone();
        let send_task = tokio::spawn(async move {
            send_binding
                .send_frame(
                    StreamId(7),
                    Frame::StreamData {
                        stream_id: StreamId(7),
                        offset: 0,
                        flags: StreamFlags::NONE,
                        payload: Bytes::from_static(b"bulk"),
                    },
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!send_task.is_finished());

        let (new_tx, mut new_rx) = mpsc::channel(1);
        binding.attach(PathId(1), new_tx);
        send_task
            .await
            .expect("binding send join")
            .expect("binding send");
        match new_rx.recv().await.expect("new path command") {
            TcpPathSessionCommand::SendFrame(Frame::StreamData {
                stream_id, payload, ..
            }) => {
                assert_eq!(stream_id, StreamId(7));
                assert_eq!(&payload[..], b"bulk");
            }
            _ => panic!("expected stream data on reselected path"),
        }
    }

    #[tokio::test]
    async fn server_tcp_relay_replays_response_repair_cache_on_path_reattach() {
        let mux_limits = MuxLimits::default();
        let stream_id = StreamId(42);
        let (mut target_peer, target_side) = duplex(4096);
        let (commands_tx, mut commands_rx) = mpsc::channel(8);
        let (frames_tx, frames_rx) = mpsc::channel(8);
        let relay = tokio::spawn(relay_tcp_stream(
            target_side,
            TcpPathStream {
                stream_id,
                max_offset: mux_limits.max_stream_window_bytes,
                output: TcpPathStreamOutput::Fixed(commands_tx),
                frames: frames_rx,
            },
            mux_limits,
        ));

        target_peer
            .write_all(b"response")
            .await
            .expect("target write");
        let first = tokio::time::timeout(Duration::from_secs(1), commands_rx.recv())
            .await
            .expect("first relay frame timeout")
            .expect("first relay frame");
        match first {
            TcpPathSessionCommand::SendFrame(Frame::StreamData {
                stream_id: received_stream_id,
                offset,
                payload,
                ..
            }) => {
                assert_eq!(received_stream_id, stream_id);
                assert_eq!(offset, 0);
                assert_eq!(&payload[..], b"response");
            }
            _ => panic!("expected first response stream data"),
        }

        frames_tx
            .send(Ok(Frame::PathStatus {
                path_id: PathId(1),
                status: crate::protocol::PathStatus::Active,
                capabilities: Default::default(),
            }))
            .await
            .expect("reattach signal");
        let replay = tokio::time::timeout(Duration::from_secs(1), commands_rx.recv())
            .await
            .expect("replay frame timeout")
            .expect("replay frame");
        match replay {
            TcpPathSessionCommand::SendFrame(Frame::StreamData {
                stream_id: received_stream_id,
                offset,
                payload,
                ..
            }) => {
                assert_eq!(received_stream_id, stream_id);
                assert_eq!(offset, 0);
                assert_eq!(&payload[..], b"response");
            }
            _ => panic!("expected replayed response stream data"),
        }

        relay.abort();
        let _ = relay.await;
    }

    #[test]
    fn client_path_health_suppresses_failed_paths_until_cooldown() {
        let fast_path = "tcp://127.0.0.1:10001?srtt-ms=5&rate-mbps=100"
            .parse::<PathSpec>()
            .expect("fast path");
        let slow_path = "tcp://127.0.0.1:10002?srtt-ms=200&rate-mbps=100"
            .parse::<PathSpec>()
            .expect("slow path");
        let context = ClientPathContext::new(
            vec![fast_path, slow_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("context");

        assert_eq!(
            context
                .ordered_tcp_path_indices(TrafficClass::Interactive, 512)
                .first()
                .copied(),
            Some(0)
        );
        context.mark_tcp_path_failure(0);
        let failed_order = context.ordered_tcp_path_indices(TrafficClass::Interactive, 512);
        assert_eq!(failed_order, vec![1]);

        {
            let mut health = context.health.lock().expect("health lock");
            health.tcp[0].failed_until = Some(Instant::now() - Duration::from_millis(1));
        }
        let recovered_order = context.ordered_tcp_path_indices(TrafficClass::Interactive, 512);
        assert!(recovered_order.contains(&0));
    }

    #[test]
    fn measured_path_latency_updates_next_scheduling_order() {
        let first_path = "tcp://127.0.0.1:10011?srtt-ms=50&rate-mbps=100"
            .parse::<PathSpec>()
            .expect("first path");
        let second_path = "tcp://127.0.0.1:10012?srtt-ms=50&rate-mbps=100"
            .parse::<PathSpec>()
            .expect("second path");
        let context = ClientPathContext::new(
            vec![first_path, second_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("context");

        context.mark_tcp_path_open_success(0, Duration::from_millis(120));
        context.mark_tcp_path_open_success(1, Duration::from_millis(5));

        assert_eq!(
            context
                .ordered_tcp_path_indices(TrafficClass::Interactive, 512)
                .first()
                .copied(),
            Some(1)
        );
    }

    #[test]
    fn measured_tcp_delivery_rate_updates_next_bulk_order() {
        let hinted_slow_path = "tcp://127.0.0.1:10013?srtt-ms=20&rate-mbps=10"
            .parse::<PathSpec>()
            .expect("hinted slow path");
        let hinted_fast_path = "tcp://127.0.0.1:10014?srtt-ms=20&rate-mbps=100"
            .parse::<PathSpec>()
            .expect("hinted fast path");
        let context = ClientPathContext::new(
            vec![hinted_slow_path, hinted_fast_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("context");

        assert_eq!(
            context
                .ordered_tcp_path_indices(TrafficClass::Bulk, 4 * 1024 * 1024)
                .first()
                .copied(),
            Some(1)
        );

        context.mark_tcp_path_delivery(
            0,
            PathDeliveryStats {
                payload_bytes: 4 * 1024 * 1024,
                first_payload_at: Some(Instant::now()),
                last_payload_at: Some(Instant::now() + Duration::from_millis(40)),
            },
        );

        assert_eq!(
            context
                .ordered_tcp_path_indices(TrafficClass::Bulk, 4 * 1024 * 1024)
                .first()
                .copied(),
            Some(0)
        );
    }

    #[test]
    fn auto_bulk_discovery_uses_bulk_horizon_for_unmeasured_high_bandwidth_path() {
        let low_latency_path = "tcp://127.0.0.1:10015?srtt-ms=20&rate-mbps=30&low-latency=true"
            .parse::<PathSpec>()
            .expect("low-latency path");
        let high_bandwidth_path = "tcp://127.0.0.1:10016?srtt-ms=180&rate-mbps=300"
            .parse::<PathSpec>()
            .expect("high-bandwidth path");
        let context = ClientPathContext::new(
            vec![low_latency_path, high_bandwidth_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("context");

        assert_eq!(
            context
                .ordered_tcp_auto_bulk_discovery_indices(
                    Some(0),
                    MuxLimits::default().max_tcp_path_inflight_bytes,
                )
                .first()
                .copied(),
            Some(1)
        );
    }

    #[test]
    fn auto_bulk_discovery_skips_unmeasured_expensive_path() {
        let low_latency_path = "tcp://127.0.0.1:10017?srtt-ms=20&rate-mbps=30&low-latency=true"
            .parse::<PathSpec>()
            .expect("low-latency path");
        let expensive_path = "tcp://127.0.0.1:10018?srtt-ms=80&rate-mbps=500&expensive=true"
            .parse::<PathSpec>()
            .expect("expensive path");
        let context = ClientPathContext::new(
            vec![low_latency_path, expensive_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("context");

        assert!(
            context
                .ordered_tcp_auto_bulk_discovery_indices(
                    Some(0),
                    MuxLimits::default().max_tcp_path_inflight_bytes,
                )
                .is_empty()
        );
    }

    #[test]
    fn measured_udp_delivery_rate_updates_next_datagram_order() {
        let hinted_slow_path = "udp://127.0.0.1:10019?srtt-ms=20&rate-mbps=10"
            .parse::<PathSpec>()
            .expect("hinted slow path");
        let hinted_fast_path = "udp://127.0.0.1:10020?srtt-ms=20&rate-mbps=100"
            .parse::<PathSpec>()
            .expect("hinted fast path");
        let context = ClientPathContext::new(
            vec![hinted_slow_path, hinted_fast_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("context");

        assert_eq!(
            context
                .ordered_udp_path_indices_for_ttl(1024 * 1024, DEFAULT_SOCKS5_UDP_TTL_MS)
                .first()
                .copied(),
            Some(1)
        );

        context.mark_udp_path_delivery(
            0,
            PathDeliveryStats {
                payload_bytes: 1024 * 1024,
                first_payload_at: Some(Instant::now()),
                last_payload_at: Some(Instant::now() + Duration::from_millis(10)),
            },
        );

        assert_eq!(
            context
                .ordered_udp_path_indices_for_ttl(1024 * 1024, DEFAULT_SOCKS5_UDP_TTL_MS)
                .first()
                .copied(),
            Some(0)
        );
    }

    #[test]
    fn udp_datagram_feedback_updates_scheduler_health() {
        let stale_path = "udp://127.0.0.1:10021?srtt-ms=250&rate-mbps=1"
            .parse::<PathSpec>()
            .expect("stale path");
        let observed_path = "udp://127.0.0.1:10022?srtt-ms=250&rate-mbps=1"
            .parse::<PathSpec>()
            .expect("observed path");
        let context = ClientPathContext::new(
            vec![stale_path, observed_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("context");

        context.mark_udp_path_feedback(
            1,
            UdpDatagramPathObservation {
                rtt: Duration::from_millis(8),
                jitter: Duration::from_millis(1),
                loss_rate: 0.02,
                rate_sample: PathRateSample::new(1024 * 1024, Duration::from_millis(20)),
            },
        );

        assert_eq!(
            context
                .ordered_udp_path_indices_for_ttl(4096, DEFAULT_SOCKS5_UDP_TTL_MS)
                .first()
                .copied(),
            Some(1)
        );
        let health = context.health.lock().expect("health lock");
        assert_eq!(health.udp[1].state, SchedulerPathState::Active);
        assert!(health.udp[1].measured_srtt_ms.is_some());
        assert!(health.udp[1].measured_jitter_ms.is_some());
        assert!(health.udp[1].measured_rate_bps.is_some());
        assert_eq!(health.udp[1].measured_loss_rate, Some(0.02));
    }

    #[test]
    fn udp_freshness_filter_rejects_paths_that_cannot_fit_ttl() {
        let high_latency_path = "udp://127.0.0.1:10023?srtt-ms=1000&rate-mbps=1"
            .parse::<PathSpec>()
            .expect("high latency path");
        let context = ClientPathContext::new(
            vec![high_latency_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("context");

        assert!(
            context
                .ordered_udp_path_indices_for_ttl(1024, 10)
                .is_empty()
        );
    }

    #[test]
    fn active_tcp_load_spreads_new_streams_and_releases_on_close() {
        let first_path = "tcp://127.0.0.1:10021?srtt-ms=10&rate-mbps=10"
            .parse::<PathSpec>()
            .expect("first path");
        let second_path = "tcp://127.0.0.1:10022?srtt-ms=10&rate-mbps=10"
            .parse::<PathSpec>()
            .expect("second path");
        let context = ClientPathContext::new(
            vec![first_path, second_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("context");

        context.mark_tcp_path_open_success(0, Duration::from_millis(1));
        assert_eq!(
            context
                .ordered_tcp_path_indices(TrafficClass::Interactive, 512)
                .first()
                .copied(),
            Some(1)
        );

        context.release_tcp_path_load(0);
        assert_eq!(
            context
                .ordered_tcp_path_indices(TrafficClass::Interactive, 512)
                .first()
                .copied(),
            Some(0)
        );
        let health = context.health.lock().expect("health lock");
        assert_eq!(health.tcp[0].active_flows, 0);
        assert_eq!(health.tcp[0].load_bytes, 0);
    }

    #[test]
    fn active_udp_load_spreads_new_associations_and_releases_on_close() {
        let first_path = "udp://127.0.0.1:10031?srtt-ms=10&rate-mbps=10"
            .parse::<PathSpec>()
            .expect("first path");
        let second_path = "udp://127.0.0.1:10032?srtt-ms=10&rate-mbps=10"
            .parse::<PathSpec>()
            .expect("second path");
        let context = ClientPathContext::new(
            vec![first_path, second_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("context");

        context.mark_udp_path_open_success(0, Duration::from_millis(1));
        assert_eq!(
            context
                .ordered_udp_path_indices_for_ttl(512, DEFAULT_SOCKS5_UDP_TTL_MS)
                .first()
                .copied(),
            Some(1)
        );

        context.release_udp_path_load(0);
        assert_eq!(
            context
                .ordered_udp_path_indices_for_ttl(512, DEFAULT_SOCKS5_UDP_TTL_MS)
                .first()
                .copied(),
            Some(0)
        );
        let health = context.health.lock().expect("health lock");
        assert_eq!(health.udp[0].active_flows, 0);
        assert_eq!(health.udp[0].load_bytes, 0);
    }

    #[tokio::test]
    async fn path_probe_refreshes_tcp_health_without_stream_load() {
        let (path, server) = spawn_server_path(OutboundConfig::Direct).await;
        let context =
            ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("ctx");

        probe_client_paths(&context, Duration::from_secs(1)).await;

        server.await.expect("server join").expect("server probe");
        let health = context.health.lock().expect("health lock");
        assert_eq!(health.tcp[0].state, SchedulerPathState::Active);
        assert!(health.tcp[0].measured_srtt_ms.is_some());
        assert_eq!(health.tcp[0].active_flows, 0);
        assert_eq!(health.tcp[0].load_bytes, 0);
    }

    #[tokio::test]
    async fn path_probe_refreshes_udp_health_without_association_load() {
        let path = reserve_udp_path().await;
        let socket = udp::bind_socket(&path).await.expect("bind udp path");
        let server = tokio::spawn(handle_server_udp_datagram_path_session(
            socket,
            server_context(OutboundConfig::Direct),
        ));
        let context =
            ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("ctx");

        probe_client_paths(&context, Duration::from_secs(1)).await;

        server.await.expect("server join").expect("server probe");
        let health = context.health.lock().expect("health lock");
        assert_eq!(health.udp[0].state, SchedulerPathState::Active);
        assert!(health.udp[0].measured_srtt_ms.is_some());
        assert_eq!(health.udp[0].active_flows, 0);
        assert_eq!(health.udp[0].load_bytes, 0);
    }

    #[tokio::test]
    async fn path_probe_failure_suppresses_unreachable_tcp_path() {
        let path = reserve_tcp_path().await;
        let context =
            ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("ctx");

        probe_client_paths(&context, Duration::from_millis(50)).await;

        let health = context.health.lock().expect("health lock");
        assert_eq!(health.tcp[0].state, SchedulerPathState::Failed);
        assert_eq!(health.tcp[0].consecutive_failures, 1);
        assert!(health.tcp[0].failed_until.is_some());
        drop(health);
        assert!(
            context
                .ordered_tcp_path_indices(TrafficClass::Interactive, 512)
                .is_empty()
        );
    }

    #[tokio::test]
    async fn socks5_ingress_relays_tcp_payload_over_encrypted_internal_stream() {
        let (target_addr, target) = spawn_echo_target().await;
        let (path, server_path) = spawn_server_path(OutboundConfig::Direct).await;
        let context =
            ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("ctx");
        let (mut client, server) = duplex(4096);
        let handler = tokio::spawn(handle_socks5_client_stream(server, context));

        client
            .write_all(&[0x05, 0x01, 0x00])
            .await
            .expect("auth request");
        let mut auth_response = [0u8; 2];
        tokio::time::timeout(
            Duration::from_secs(2),
            client.read_exact(&mut auth_response),
        )
        .await
        .expect("auth timeout")
        .expect("auth response");
        assert_eq!(auth_response, [0x05, 0x00]);

        let mut connect = vec![0x05, 0x01, 0x00, 0x01];
        match target_addr {
            SocketAddr::V4(addr) => {
                connect.extend_from_slice(&addr.ip().octets());
                connect.extend_from_slice(&addr.port().to_be_bytes());
            }
            SocketAddr::V6(_) => panic!("expected IPv4 test target"),
        }
        client.write_all(&connect).await.expect("connect");

        let mut response = [0u8; 10];
        client.read_exact(&mut response).await.expect("reply");
        assert_eq!(response[1], Socks5Reply::Succeeded as u8);

        client.write_all(b"ping").await.expect("payload write");
        client.shutdown().await.expect("client shutdown");
        let mut payload = [0u8; 4];
        client.read_exact(&mut payload).await.expect("payload read");
        assert_eq!(&payload, b"pong");

        handler.await.expect("join").expect("handler");
        server_path
            .await
            .expect("server join")
            .expect("server path");
        target.await.expect("target join");
    }

    #[tokio::test]
    async fn tcp_path_session_multiplexes_multiple_ingress_streams() {
        let (target_addr, target) = spawn_echo_target_count(2).await;
        let (path, server_path) = spawn_server_path(OutboundConfig::Direct).await;
        let context =
            ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("ctx");
        let (mut first_client, first_server) = duplex(4096);
        let (mut second_client, second_server) = duplex(4096);
        let first_handler =
            tokio::spawn(handle_socks5_client_stream(first_server, context.clone()));
        let second_handler = tokio::spawn(handle_socks5_client_stream(second_server, context));

        let first_client_task = tokio::spawn(async move {
            drive_socks5_echo_client(&mut first_client, target_addr).await;
        });
        let second_client_task = tokio::spawn(async move {
            drive_socks5_echo_client(&mut second_client, target_addr).await;
        });

        first_client_task.await.expect("first client");
        second_client_task.await.expect("second client");
        first_handler
            .await
            .expect("first join")
            .expect("first handler");
        second_handler
            .await
            .expect("second join")
            .expect("second handler");
        server_path
            .await
            .expect("server join")
            .expect("server path");
        target.await.expect("target join");
    }

    #[tokio::test]
    async fn auto_bulk_tcp_stream_attaches_measured_path_for_large_response() {
        let payload = vec![0x5au8; 2 * 1024 * 1024];
        let expected_payload = payload.clone();
        let target_listener = TcpListener::bind("127.0.0.1:0").await.expect("target bind");
        let target_addr = target_listener.local_addr().expect("target addr");
        let target = tokio::spawn(async move {
            let (mut stream, _) = target_listener.accept().await.expect("target accept");
            let mut request = [0u8; 4];
            stream
                .read_exact(&mut request)
                .await
                .expect("target request");
            assert_eq!(&request, b"ping");
            stream.write_all(&payload).await.expect("target response");
            stream.shutdown().await.expect("target shutdown");
        });

        let low_latency_path =
            reserve_tcp_path_with_query("srtt-ms=10&rate-mbps=20&low-latency=true").await;
        let high_bandwidth_path = reserve_tcp_path_with_query("srtt-ms=120&rate-mbps=300").await;
        let low_latency_listener = bind_listener(&low_latency_path)
            .await
            .expect("low-latency bind");
        let high_bandwidth_listener = bind_listener(&high_bandwidth_path)
            .await
            .expect("high-bandwidth bind");
        let server_context = server_context(OutboundConfig::Direct);
        let (accepted_tx, mut accepted_rx) = mpsc::channel(2);
        let low_latency_context = server_context.clone();
        let low_latency_accepted_tx = accepted_tx.clone();
        let low_latency_server = tokio::spawn(async move {
            let (stream, _) = low_latency_listener
                .accept()
                .await
                .expect("low-latency accept");
            low_latency_accepted_tx
                .send(0usize)
                .await
                .expect("accepted low latency");
            handle_server_path(stream, low_latency_context).await
        });
        let high_bandwidth_context = server_context.clone();
        let high_bandwidth_server = tokio::spawn(async move {
            let (stream, _) = high_bandwidth_listener
                .accept()
                .await
                .expect("high-bandwidth accept");
            accepted_tx
                .send(1usize)
                .await
                .expect("accepted high bandwidth");
            handle_server_path(stream, high_bandwidth_context).await
        });

        let context = ClientPathContext::new(
            vec![low_latency_path, high_bandwidth_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("ctx");
        context.mark_tcp_path_delivery(
            1,
            PathDeliveryStats {
                payload_bytes: 4 * 1024 * 1024,
                first_payload_at: Some(Instant::now()),
                last_payload_at: Some(Instant::now() + Duration::from_millis(100)),
            },
        );
        let health_context = context.clone();
        let ingress_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("ingress bind");
        let ingress_addr = ingress_listener.local_addr().expect("ingress addr");
        let handler = tokio::spawn(async move {
            let (server, _) = ingress_listener.accept().await.expect("ingress accept");
            handle_socks5_client_stream(server, context).await
        });
        let mut client = TcpStream::connect(ingress_addr)
            .await
            .expect("ingress client");

        client
            .write_all(&[0x05, 0x01, 0x00])
            .await
            .expect("auth request");
        let mut auth_response = [0u8; 2];
        tokio::time::timeout(
            Duration::from_secs(2),
            client.read_exact(&mut auth_response),
        )
        .await
        .expect("auth timeout")
        .expect("auth response");
        assert_eq!(auth_response, [0x05, 0x00]);
        let mut connect = vec![0x05, 0x01, 0x00, 0x01];
        match target_addr {
            SocketAddr::V4(addr) => {
                connect.extend_from_slice(&addr.ip().octets());
                connect.extend_from_slice(&addr.port().to_be_bytes());
            }
            SocketAddr::V6(_) => panic!("expected IPv4 test target"),
        }
        client.write_all(&connect).await.expect("connect");
        let mut response = [0u8; 10];
        tokio::time::timeout(Duration::from_secs(2), client.read_exact(&mut response))
            .await
            .expect("reply timeout")
            .expect("reply");
        assert_eq!(response[1], Socks5Reply::Succeeded as u8);

        client.write_all(b"ping").await.expect("payload write");
        client.shutdown().await.expect("client shutdown");
        let mut received = vec![0u8; expected_payload.len()];
        tokio::time::timeout(Duration::from_secs(5), client.read_exact(&mut received))
            .await
            .expect("response timeout")
            .expect("payload read");
        assert_eq!(received, expected_payload);

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), accepted_rx.recv())
                .await
                .expect("first accept timeout"),
            Some(0)
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), accepted_rx.recv())
                .await
                .expect("second accept timeout"),
            Some(1)
        );

        handler.await.expect("handler join").expect("handler");
        {
            let health = health_context.health.lock().expect("health lock");
            assert_eq!(health.tcp[0].active_flows, 0);
            assert_eq!(health.tcp[1].active_flows, 0);
        }
        drop(health_context);
        low_latency_server
            .await
            .expect("low-latency server join")
            .expect("low-latency server");
        high_bandwidth_server
            .await
            .expect("high-bandwidth server join")
            .expect("high-bandwidth server");
        target.await.expect("target join");
    }

    #[tokio::test]
    async fn tcp_stream_migrates_to_survivor_path_after_active_path_failure() {
        let target_listener = TcpListener::bind("127.0.0.1:0").await.expect("target bind");
        let target_addr = target_listener.local_addr().expect("target addr");
        let (first_payload_tx, first_payload_rx) = oneshot::channel();
        let target = tokio::spawn(async move {
            let (mut stream, _) = target_listener.accept().await.expect("target accept");
            let mut first = [0u8; 4];
            stream
                .read_exact(&mut first)
                .await
                .expect("target first read");
            assert_eq!(&first, b"ping");
            let _ = first_payload_tx.send(());
            let mut second = [0u8; 4];
            stream
                .read_exact(&mut second)
                .await
                .expect("target second read");
            assert_eq!(&second, b"pong");
            stream.write_all(b"done").await.expect("target write");
            stream.shutdown().await.expect("target shutdown");
        });

        let first_path = reserve_tcp_path().await;
        let second_path = reserve_tcp_path().await;
        let first_listener = bind_listener(&first_path).await.expect("first bind");
        let second_listener = bind_listener(&second_path).await.expect("second bind");
        let server_context = server_context(OutboundConfig::Direct);
        let first_server_context = server_context.clone();
        let first_server = tokio::spawn(async move {
            let (stream, _) = first_listener.accept().await.expect("first accept");
            handle_server_path(stream, first_server_context).await
        });
        let second_server_context = server_context.clone();
        let second_server = tokio::spawn(async move {
            let (stream, _) = second_listener.accept().await.expect("second accept");
            handle_server_path(stream, second_server_context).await
        });

        let resources = ResourceLimits {
            tcp_path_heartbeat_interval: Duration::from_secs(60),
            tcp_path_heartbeat_timeout: Duration::from_secs(60),
            ..ResourceLimits::default()
        };
        let context = ClientPathContext::new(vec![first_path, second_path], security(), resources)
            .expect("ctx");
        let health_context = context.clone();
        let ingress_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("ingress bind");
        let ingress_addr = ingress_listener.local_addr().expect("ingress addr");
        let handler = tokio::spawn(async move {
            let (server, _) = ingress_listener.accept().await.expect("ingress accept");
            handle_socks5_client_stream(server, context.clone()).await
        });
        let mut client = TcpStream::connect(ingress_addr)
            .await
            .expect("ingress client");

        client
            .write_all(&[0x05, 0x01, 0x00])
            .await
            .expect("auth request");
        let mut auth_response = [0u8; 2];
        client
            .read_exact(&mut auth_response)
            .await
            .expect("auth response");
        assert_eq!(auth_response, [0x05, 0x00]);
        let mut connect = vec![0x05, 0x01, 0x00, 0x01];
        match target_addr {
            SocketAddr::V4(addr) => {
                connect.extend_from_slice(&addr.ip().octets());
                connect.extend_from_slice(&addr.port().to_be_bytes());
            }
            SocketAddr::V6(_) => panic!("expected IPv4 test target"),
        }
        client.write_all(&connect).await.expect("connect");
        let mut response = [0u8; 10];
        client.read_exact(&mut response).await.expect("reply");
        assert_eq!(response[1], Socks5Reply::Succeeded as u8);

        client.write_all(b"ping").await.expect("first payload");
        first_payload_rx.await.expect("first payload observed");
        first_server.abort();
        let _ = first_server.await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        client.write_all(b"pong").await.expect("second payload");

        let mut payload = [0u8; 4];
        client.read_exact(&mut payload).await.expect("payload read");
        assert_eq!(&payload, b"done");
        client.shutdown().await.expect("client shutdown");
        handler.await.expect("handler join").expect("handler");
        {
            let health = health_context.health.lock().expect("health lock");
            assert_eq!(health.tcp[0].state, SchedulerPathState::Failed);
            assert_eq!(health.tcp[0].active_flows, 0);
            assert_eq!(health.tcp[1].active_flows, 0);
        }
        drop(health_context);
        second_server
            .await
            .expect("second server join")
            .expect("second server");
        target.await.expect("target join");
    }

    #[tokio::test]
    async fn tcp_relay_heartbeat_timeout_marks_path_failed_and_releases_load() {
        let (path, server_path) =
            spawn_tcp_relay_heartbeat_blackhole(Duration::from_millis(100)).await;
        let resources = ResourceLimits {
            tcp_path_heartbeat_interval: Duration::from_millis(10),
            tcp_path_heartbeat_timeout: Duration::from_millis(30),
            ..ResourceLimits::default()
        };
        let context = ClientPathContext::new(vec![path], security(), resources).expect("ctx");
        let health_context = context.clone();
        let (mut client, server) = duplex(4096);
        let handler = tokio::spawn(handle_socks5_client_stream(server, context));

        client
            .write_all(&[0x05, 0x01, 0x00])
            .await
            .expect("auth request");
        let mut auth_response = [0u8; 2];
        client
            .read_exact(&mut auth_response)
            .await
            .expect("auth response");
        assert_eq!(auth_response, [0x05, 0x00]);

        client
            .write_all(&[0x05, 0x01, 0x00, 0x01, 203, 0, 113, 1, 0x01, 0xbb])
            .await
            .expect("connect");
        let mut response = [0u8; 10];
        client.read_exact(&mut response).await.expect("reply");
        assert_eq!(response[1], Socks5Reply::Succeeded as u8);

        let err = tokio::time::timeout(Duration::from_secs(2), handler)
            .await
            .expect("handler timeout")
            .expect("handler join")
            .expect_err("heartbeat timeout");
        assert!(matches!(err, RuntimeError::PathHeartbeatTimeout));

        {
            let health = health_context.health.lock().expect("health lock");
            assert_eq!(health.tcp[0].state, SchedulerPathState::Failed);
            assert_eq!(health.tcp[0].consecutive_failures, 1);
            assert_eq!(health.tcp[0].active_flows, 0);
            assert_eq!(health.tcp[0].load_bytes, 0);
        }

        server_path
            .await
            .expect("server join")
            .expect("heartbeat test server");
    }

    #[test]
    fn tcp_path_activity_extends_pending_heartbeat_deadline() {
        let before = tokio::time::Instant::now();
        let mut next_heartbeat_at = before;
        let old_deadline = before + Duration::from_millis(1);
        let mut pending = Some((42, old_deadline));

        refresh_client_tcp_path_liveness_state(
            &mut next_heartbeat_at,
            Duration::from_secs(10),
            &mut pending,
            Duration::from_secs(30),
        );

        assert!(next_heartbeat_at >= before + Duration::from_secs(10));
        let Some((nonce, deadline)) = pending else {
            panic!("heartbeat should remain pending");
        };
        assert_eq!(nonce, 42);
        assert!(deadline >= before + Duration::from_secs(30));
        assert!(deadline > old_deadline);
    }

    #[tokio::test]
    async fn socks5_ingress_schedules_tcp_stream_to_best_configured_path() {
        let (target_addr, target) = spawn_echo_target().await;
        let high_latency_path = reserve_tcp_path_with_query("srtt-ms=200&rate-mbps=1000").await;
        let low_latency_path =
            reserve_tcp_path_with_query("srtt-ms=10&rate-mbps=50&low-latency=true").await;
        let (accepted_tx, mut accepted_rx) = mpsc::channel(2);
        let high_latency_server = spawn_notified_server_path(
            high_latency_path.clone(),
            0,
            OutboundConfig::Direct,
            accepted_tx.clone(),
        )
        .await;
        let low_latency_server = spawn_notified_server_path(
            low_latency_path.clone(),
            1,
            OutboundConfig::Direct,
            accepted_tx,
        )
        .await;
        let context = ClientPathContext::new(
            vec![high_latency_path, low_latency_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("ctx");
        let (mut client, server) = duplex(4096);
        let handler = tokio::spawn(handle_socks5_client_stream(server, context));

        client
            .write_all(&[0x05, 0x01, 0x00])
            .await
            .expect("auth request");
        let mut auth_response = [0u8; 2];
        client
            .read_exact(&mut auth_response)
            .await
            .expect("auth response");
        assert_eq!(auth_response, [0x05, 0x00]);
        let mut connect = vec![0x05, 0x01, 0x00, 0x01];
        match target_addr {
            SocketAddr::V4(addr) => {
                connect.extend_from_slice(&addr.ip().octets());
                connect.extend_from_slice(&addr.port().to_be_bytes());
            }
            SocketAddr::V6(_) => panic!("expected IPv4 test target"),
        }
        client.write_all(&connect).await.expect("connect");
        let mut response = [0u8; 10];
        client.read_exact(&mut response).await.expect("reply");
        assert_eq!(response[1], Socks5Reply::Succeeded as u8);
        client.write_all(b"ping").await.expect("payload write");
        client.shutdown().await.expect("client shutdown");
        let mut payload = [0u8; 4];
        client.read_exact(&mut payload).await.expect("payload read");
        assert_eq!(&payload, b"pong");

        assert_eq!(accepted_rx.recv().await, Some(1));
        handler.await.expect("join").expect("handler");
        low_latency_server
            .await
            .expect("low latency server join")
            .expect("low latency server");
        high_latency_server.abort();
        let _ = high_latency_server.await;
        target.await.expect("target join");
    }

    #[tokio::test]
    async fn socks5_ingress_applies_tcp_class_policy_before_scheduling() {
        let (target_addr, target) = spawn_echo_target().await;
        let no_bulk_low_latency_path =
            reserve_tcp_path_with_query("srtt-ms=10&rate-mbps=1000&no-bulk").await;
        let bulk_allowed_path = reserve_tcp_path_with_query("srtt-ms=120&rate-mbps=100").await;
        let (accepted_tx, mut accepted_rx) = mpsc::channel(2);
        let low_latency_server = spawn_notified_server_path(
            no_bulk_low_latency_path.clone(),
            0,
            OutboundConfig::Direct,
            accepted_tx.clone(),
        )
        .await;
        let bulk_allowed_server = spawn_notified_server_path(
            bulk_allowed_path.clone(),
            1,
            OutboundConfig::Direct,
            accepted_tx,
        )
        .await;
        let traffic_policy = TrafficPolicy {
            default_tcp_class: TcpTrafficClass::Fixed(TrafficClass::Interactive),
            tcp_port_rules: vec![TcpPortClassRule {
                port: target_addr.port(),
                class: TcpTrafficClass::Fixed(TrafficClass::Bulk),
            }],
        };
        let context = ClientPathContext::new_with_policy(
            vec![no_bulk_low_latency_path, bulk_allowed_path],
            traffic_policy,
            security(),
            ResourceLimits::default(),
        )
        .expect("ctx");
        let (mut client, server) = duplex(4096);
        let handler = tokio::spawn(handle_socks5_client_stream(server, context));

        client
            .write_all(&[0x05, 0x01, 0x00])
            .await
            .expect("auth request");
        let mut auth_response = [0u8; 2];
        client
            .read_exact(&mut auth_response)
            .await
            .expect("auth response");
        assert_eq!(auth_response, [0x05, 0x00]);
        let mut connect = vec![0x05, 0x01, 0x00, 0x01];
        match target_addr {
            SocketAddr::V4(addr) => {
                connect.extend_from_slice(&addr.ip().octets());
                connect.extend_from_slice(&addr.port().to_be_bytes());
            }
            SocketAddr::V6(_) => panic!("expected IPv4 test target"),
        }
        client.write_all(&connect).await.expect("connect");
        let mut response = [0u8; 10];
        client.read_exact(&mut response).await.expect("reply");
        assert_eq!(response[1], Socks5Reply::Succeeded as u8);
        client.write_all(b"ping").await.expect("payload write");
        client.shutdown().await.expect("client shutdown");
        let mut payload = [0u8; 4];
        client.read_exact(&mut payload).await.expect("payload read");
        assert_eq!(&payload, b"pong");

        assert_eq!(accepted_rx.recv().await, Some(1));
        handler.await.expect("join").expect("handler");
        bulk_allowed_server
            .await
            .expect("bulk server join")
            .expect("bulk server");
        low_latency_server.abort();
        let _ = low_latency_server.await;
        target.await.expect("target join");
    }

    #[tokio::test]
    async fn socks5_ingress_retries_next_tcp_path_after_connect_failure() {
        let (target_addr, target) = spawn_echo_target().await;
        let failed_path = reserve_tcp_path().await;
        let (working_path, server_path) = spawn_server_path(OutboundConfig::Direct).await;
        let context = ClientPathContext::new(
            vec![failed_path, working_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("ctx");
        let (mut client, server) = duplex(4096);
        let handler = tokio::spawn(handle_socks5_client_stream(server, context));

        client
            .write_all(&[0x05, 0x01, 0x00])
            .await
            .expect("auth request");
        let mut auth_response = [0u8; 2];
        client
            .read_exact(&mut auth_response)
            .await
            .expect("auth response");
        assert_eq!(auth_response, [0x05, 0x00]);
        let mut connect = vec![0x05, 0x01, 0x00, 0x01];
        match target_addr {
            SocketAddr::V4(addr) => {
                connect.extend_from_slice(&addr.ip().octets());
                connect.extend_from_slice(&addr.port().to_be_bytes());
            }
            SocketAddr::V6(_) => panic!("expected IPv4 test target"),
        }
        client.write_all(&connect).await.expect("connect");
        let mut response = [0u8; 10];
        client.read_exact(&mut response).await.expect("reply");
        assert_eq!(response[1], Socks5Reply::Succeeded as u8);
        client.write_all(b"ping").await.expect("payload write");
        client.shutdown().await.expect("client shutdown");
        let mut payload = [0u8; 4];
        client.read_exact(&mut payload).await.expect("payload read");
        assert_eq!(&payload, b"pong");

        handler.await.expect("join").expect("handler");
        server_path
            .await
            .expect("server join")
            .expect("server path");
        target.await.expect("target join");
    }

    #[tokio::test]
    async fn http_connect_ingress_relays_tcp_payload_over_encrypted_internal_stream() {
        let (target_addr, target) = spawn_echo_target().await;
        let (path, server_path) = spawn_server_path(OutboundConfig::Direct).await;
        let context =
            ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("ctx");
        let (mut client, server) = duplex(4096);
        let handler = tokio::spawn(handle_http_connect_client_stream(server, context));

        client
            .write_all(
                format!("CONNECT {target_addr} HTTP/1.1\r\nHost: {target_addr}\r\n\r\n").as_bytes(),
            )
            .await
            .expect("request");
        let mut response = vec![0u8; http_connect::success_response().len()];
        client.read_exact(&mut response).await.expect("response");
        assert_eq!(response, http_connect::success_response());

        client.write_all(b"ping").await.expect("payload write");
        client.shutdown().await.expect("client shutdown");
        let mut payload = [0u8; 4];
        client.read_exact(&mut payload).await.expect("payload read");
        assert_eq!(&payload, b"pong");

        handler.await.expect("join").expect("handler");
        server_path
            .await
            .expect("server join")
            .expect("server path");
        target.await.expect("target join");
    }

    #[tokio::test]
    async fn encrypted_udp_datagram_path_relays_direct_udp_target() {
        let (target_addr, target) = spawn_udp_echo_target().await;
        let path = reserve_udp_path().await;
        let socket = udp::bind_socket(&path).await.expect("bind udp path");
        let server = tokio::spawn(handle_server_udp_datagram_path_session(
            socket,
            server_context(OutboundConfig::Direct),
        ));

        let response = client_udp_datagram_round_trip(
            &path,
            security(),
            ResourceLimits::default(),
            TargetAddr::Ip(target_addr),
            Bytes::from_static(b"ping"),
            1000,
        )
        .await
        .expect("round trip");

        assert_eq!(response, Bytes::from_static(b"pong"));
        server.await.expect("server join").expect("server");
        target.await.expect("target join");
    }

    #[tokio::test]
    async fn encrypted_udp_datagram_path_relays_upstream_socks5_udp_target() {
        let (proxy, proxy_task) = spawn_socks5_udp_proxy_once().await;
        let path = reserve_udp_path().await;
        let socket = udp::bind_socket(&path).await.expect("bind udp path");
        let server = tokio::spawn(handle_server_udp_datagram_path_session(
            socket,
            server_context(OutboundConfig::Socks5 { proxy }),
        ));

        let response = client_udp_datagram_round_trip(
            &path,
            security(),
            ResourceLimits::default(),
            TargetAddr::Domain {
                host: "example.com".to_string(),
                port: 53,
            },
            Bytes::from_static(b"ping"),
            1000,
        )
        .await
        .expect("round trip");

        assert_eq!(response, Bytes::from_static(b"pong"));
        server.await.expect("server join").expect("server");
        proxy_task.await.expect("proxy join");
    }

    #[tokio::test]
    async fn server_runtime_binds_udp_path_and_relays_direct_udp_datagram() {
        let (target_addr, target) = spawn_udp_echo_target().await;
        let path = reserve_udp_path().await;
        let server = tokio::spawn(run_server(
            vec![path.clone()],
            OutboundConfig::Direct,
            DnsConfig::default(),
            security(),
            ResourceLimits::default(),
        ));
        tokio::time::sleep(Duration::from_millis(10)).await;

        let response = client_udp_datagram_round_trip(
            &path,
            security(),
            ResourceLimits::default(),
            TargetAddr::Ip(target_addr),
            Bytes::from_static(b"ping"),
            1000,
        )
        .await
        .expect("round trip");

        assert_eq!(response, Bytes::from_static(b"pong"));
        server.abort();
        let _ = server.await;
        target.await.expect("target join");
    }

    #[tokio::test]
    async fn server_runtime_demuxes_concurrent_udp_peers_on_one_bind_path() {
        let (first_target_addr, first_target) = spawn_udp_echo_target().await;
        let (second_target_addr, second_target) = spawn_udp_echo_target().await;
        let path = reserve_udp_path().await;
        let server = tokio::spawn(run_server(
            vec![path.clone()],
            OutboundConfig::Direct,
            DnsConfig::default(),
            security(),
            ResourceLimits::default(),
        ));
        tokio::time::sleep(Duration::from_millis(10)).await;

        let first = client_udp_datagram_round_trip(
            &path,
            security(),
            ResourceLimits::default(),
            TargetAddr::Ip(first_target_addr),
            Bytes::from_static(b"ping"),
            1000,
        );
        let second = client_udp_datagram_round_trip(
            &path,
            security(),
            ResourceLimits::default(),
            TargetAddr::Ip(second_target_addr),
            Bytes::from_static(b"ping"),
            1000,
        );
        let (first_response, second_response) = tokio::join!(first, second);

        assert_eq!(
            first_response.expect("first response"),
            Bytes::from_static(b"pong")
        );
        assert_eq!(
            second_response.expect("second response"),
            Bytes::from_static(b"pong")
        );
        server.abort();
        let _ = server.await;
        first_target.await.expect("first target join");
        second_target.await.expect("second target join");
    }

    #[tokio::test]
    async fn socks5_udp_associate_relays_datagram_over_encrypted_udp_path() {
        let (target_addr, target) = spawn_udp_echo_target_count(2).await;
        let path = reserve_udp_path().await;
        let socket = udp::bind_socket(&path).await.expect("bind udp path");
        let server = tokio::spawn(handle_server_udp_datagram_path_session(
            socket,
            server_context(OutboundConfig::Direct),
        ));
        let context =
            ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("ctx");
        let health_context = context.clone();
        let (mut control_client, control_server) = duplex(4096);
        let handler = tokio::spawn(handle_socks5_client_stream(control_server, context));

        control_client
            .write_all(&[0x05, 0x01, 0x00])
            .await
            .expect("auth request");
        let mut auth_response = [0u8; 2];
        control_client
            .read_exact(&mut auth_response)
            .await
            .expect("auth response");
        assert_eq!(auth_response, [0x05, 0x00]);

        control_client
            .write_all(&[0x05, 0x03, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
            .await
            .expect("udp associate");
        let mut associate_response = [0u8; 10];
        control_client
            .read_exact(&mut associate_response)
            .await
            .expect("associate response");
        assert_eq!(associate_response[0], 0x05);
        assert_eq!(associate_response[1], Socks5Reply::Succeeded as u8);
        assert_eq!(associate_response[3], 0x01);
        let relay_addr = SocketAddr::from((
            [
                associate_response[4],
                associate_response[5],
                associate_response[6],
                associate_response[7],
            ],
            u16::from_be_bytes([associate_response[8], associate_response[9]]),
        ));

        let udp_client = UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("udp client bind");
        let request =
            socks5::udp_datagram(&TargetAddr::Ip(target_addr), b"ping").expect("udp request");
        for _ in 0..2 {
            udp_client
                .send_to(&request, relay_addr)
                .await
                .expect("send udp request");
            let mut response = [0u8; 128];
            let (len, _) = udp_client
                .recv_from(&mut response)
                .await
                .expect("recv udp response");
            let (datagram, consumed) =
                socks5::parse_udp_datagram(&response[..len]).expect("datagram");
            assert_eq!(consumed, len);
            assert_eq!(datagram.target, TargetAddr::Ip(target_addr));
            assert_eq!(datagram.payload, Bytes::from_static(b"pong"));
        }
        control_client.shutdown().await.expect("control shutdown");

        handler.await.expect("handler join").expect("handler");
        {
            let health = health_context.health.lock().expect("health lock");
            assert_eq!(health.udp[0].state, SchedulerPathState::Active);
            assert!(health.udp[0].measured_srtt_ms.is_some());
            assert!(health.udp[0].measured_jitter_ms.is_some());
            assert_eq!(health.udp[0].measured_loss_rate, Some(0.0));
        }
        server.await.expect("server join").expect("server");
        target.await.expect("target join");
    }

    #[tokio::test]
    async fn socks5_udp_associate_keeps_multiple_udp_paths_active() {
        let (target_addr, target) = spawn_udp_echo_target_count(2).await;
        let first_path = reserve_udp_path_with_query("srtt-ms=10&rate-mbps=10").await;
        let second_path = reserve_udp_path_with_query("srtt-ms=10&rate-mbps=10").await;
        let first_socket = udp::bind_socket(&first_path)
            .await
            .expect("bind first udp path");
        let second_socket = udp::bind_socket(&second_path)
            .await
            .expect("bind second udp path");
        let first_server = tokio::spawn(handle_server_udp_datagram_path_session(
            first_socket,
            server_context(OutboundConfig::Direct),
        ));
        let second_server = tokio::spawn(handle_server_udp_datagram_path_session(
            second_socket,
            server_context(OutboundConfig::Direct),
        ));
        let context = ClientPathContext::new(
            vec![first_path, second_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("ctx");
        let health_context = context.clone();
        let (mut control_client, control_server) = duplex(4096);
        let handler = tokio::spawn(handle_socks5_client_stream(control_server, context));

        control_client
            .write_all(&[0x05, 0x01, 0x00])
            .await
            .expect("auth request");
        let mut auth_response = [0u8; 2];
        control_client
            .read_exact(&mut auth_response)
            .await
            .expect("auth response");
        assert_eq!(auth_response, [0x05, 0x00]);
        control_client
            .write_all(&[0x05, 0x03, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
            .await
            .expect("udp associate");
        let mut associate_response = [0u8; 10];
        control_client
            .read_exact(&mut associate_response)
            .await
            .expect("associate response");
        let relay_addr = SocketAddr::from((
            [
                associate_response[4],
                associate_response[5],
                associate_response[6],
                associate_response[7],
            ],
            u16::from_be_bytes([associate_response[8], associate_response[9]]),
        ));

        let udp_client = UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("udp client bind");
        let request =
            socks5::udp_datagram(&TargetAddr::Ip(target_addr), b"ping").expect("udp request");
        for _ in 0..2 {
            udp_client
                .send_to(&request, relay_addr)
                .await
                .expect("send udp request");
            let mut response = [0u8; 128];
            let (len, _) = udp_client
                .recv_from(&mut response)
                .await
                .expect("recv udp response");
            let (datagram, consumed) =
                socks5::parse_udp_datagram(&response[..len]).expect("datagram");
            assert_eq!(consumed, len);
            assert_eq!(datagram.target, TargetAddr::Ip(target_addr));
            assert_eq!(datagram.payload, Bytes::from_static(b"pong"));
        }
        {
            let health = health_context.health.lock().expect("health lock");
            assert_eq!(health.udp[0].active_flows, 1);
            assert_eq!(health.udp[1].active_flows, 1);
        }
        control_client.shutdown().await.expect("control shutdown");

        handler.await.expect("handler join").expect("handler");
        {
            let health = health_context.health.lock().expect("health lock");
            assert_eq!(health.udp[0].active_flows, 0);
            assert_eq!(health.udp[1].active_flows, 0);
        }
        first_server
            .await
            .expect("first server join")
            .expect("first server");
        second_server
            .await
            .expect("second server join")
            .expect("second server");
        target.await.expect("target join");
    }

    #[tokio::test]
    async fn udp_association_retries_datagram_on_survivor_path_after_timeout() {
        let (target_addr, target) = spawn_udp_echo_target().await;
        let blackhole_path = reserve_udp_path_with_query("srtt-ms=5&rate-mbps=100").await;
        let survivor_path = reserve_udp_path_with_query("srtt-ms=20&rate-mbps=100").await;
        let blackhole = spawn_udp_datagram_blackhole_path(blackhole_path.clone()).await;
        let survivor_socket = udp::bind_socket(&survivor_path)
            .await
            .expect("bind survivor udp path");
        let survivor = tokio::spawn(handle_server_udp_datagram_path_session(
            survivor_socket,
            server_context(OutboundConfig::Direct),
        ));
        let context = ClientPathContext::new(
            vec![blackhole_path, survivor_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("ctx");
        let mut association = UdpDatagramClientAssociation::new(context.clone()).expect("assoc");

        let response = association
            .send_to(
                TargetAddr::Ip(target_addr),
                Bytes::from_static(b"ping"),
                1000,
            )
            .await
            .expect("retry response");

        assert_eq!(response, Bytes::from_static(b"pong"));
        {
            let health = context.health.lock().expect("health lock");
            assert_eq!(health.udp[0].state, SchedulerPathState::Failed);
            assert_eq!(health.udp[0].active_flows, 0);
            assert_eq!(health.udp[1].state, SchedulerPathState::Active);
            assert_eq!(health.udp[1].active_flows, 1);
        }
        association.close().await.expect("close association");
        blackhole
            .await
            .expect("blackhole join")
            .expect("blackhole path");
        survivor
            .await
            .expect("survivor join")
            .expect("survivor path");
        target.await.expect("target join");
    }

    #[tokio::test]
    async fn udp_association_probes_mtu_before_large_datagram() {
        let payload = Bytes::from(vec![0x5a; UDP_DEFAULT_MTU_PAYLOAD_BYTES + 256]);
        let (target_addr, target) =
            spawn_udp_payload_target(payload.clone(), Bytes::from_static(b"pong")).await;
        let path = reserve_udp_path().await;
        let socket = udp::bind_socket(&path).await.expect("bind udp path");
        let server = tokio::spawn(handle_server_udp_datagram_path_session(
            socket,
            server_context(OutboundConfig::Direct),
        ));
        let context =
            ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("ctx");
        let mut association = UdpDatagramClientAssociation::new(context.clone()).expect("assoc");

        let response = association
            .send_to(TargetAddr::Ip(target_addr), payload.clone(), 1000)
            .await
            .expect("large datagram");

        assert_eq!(response, Bytes::from_static(b"pong"));
        {
            let health = context.health.lock().expect("health lock");
            assert_eq!(
                health.udp[0].measured_mtu_payload_bytes,
                Some(payload.len())
            );
        }
        association.close().await.expect("close association");
        server.await.expect("server join").expect("server");
        target.await.expect("target join");
    }

    #[test]
    fn udp_measured_mtu_skips_oversized_path_candidate() {
        let low_mtu_path = "udp://127.0.0.1:12001?srtt-ms=5&rate-mbps=100"
            .parse::<PathSpec>()
            .expect("low mtu path");
        let probeable_path = "udp://127.0.0.1:12002?srtt-ms=20&rate-mbps=100"
            .parse::<PathSpec>()
            .expect("probeable path");
        let context = ClientPathContext::new(
            vec![low_mtu_path, probeable_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("ctx");
        context.mark_udp_path_mtu(0, UDP_DEFAULT_MTU_PAYLOAD_BYTES);
        let association = UdpDatagramClientAssociation::new(context).expect("assoc");

        assert_eq!(
            association.select_path_candidate(
                &[0, 1],
                &HashSet::new(),
                UDP_DEFAULT_MTU_PAYLOAD_BYTES + 256,
                1000,
            ),
            Some(1)
        );
    }

    #[tokio::test]
    async fn udp_association_retries_after_acked_response_loss_without_failing_path() {
        let (target_addr, target) = spawn_udp_echo_target().await;
        let drop_path = reserve_udp_path_with_query("srtt-ms=5&rate-mbps=100").await;
        let survivor_path = reserve_udp_path_with_query("srtt-ms=20&rate-mbps=100").await;
        let drop_server = spawn_udp_datagram_ack_then_drop_path(drop_path.clone()).await;
        let survivor_socket = udp::bind_socket(&survivor_path)
            .await
            .expect("bind survivor udp path");
        let survivor = tokio::spawn(handle_server_udp_datagram_path_session(
            survivor_socket,
            server_context(OutboundConfig::Direct),
        ));
        let context = ClientPathContext::new(
            vec![drop_path, survivor_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("ctx");
        let mut association = UdpDatagramClientAssociation::new(context.clone()).expect("assoc");

        let response = association
            .send_to(
                TargetAddr::Ip(target_addr),
                Bytes::from_static(b"ping"),
                1000,
            )
            .await
            .expect("retry response");

        assert_eq!(response, Bytes::from_static(b"pong"));
        {
            let health = context.health.lock().expect("health lock");
            assert_eq!(health.udp[0].state, SchedulerPathState::Active);
            assert!(
                health.udp[0]
                    .measured_loss_rate
                    .is_some_and(|loss| loss > 0.0)
            );
            assert_eq!(health.udp[1].state, SchedulerPathState::Active);
        }
        association.close().await.expect("close association");
        drop_server
            .await
            .expect("drop server join")
            .expect("drop server");
        survivor
            .await
            .expect("survivor join")
            .expect("survivor path");
        target.await.expect("target join");
    }

    #[tokio::test]
    async fn server_verifies_auth_sequence_and_rejects_wrong_secret() {
        let path = reserve_tcp_path().await;
        let listener = bind_listener(&path).await.expect("bind");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            handle_server_path(
                stream,
                ServerPathContext {
                    outbound: OutboundConfig::Direct,
                    outbound_dns: DnsConfig::default(),
                    codec_limits: CodecLimits::default(),
                    mux_limits: ResourceLimits::default().into(),
                    security: SecurityConfig::encrypted(
                        SharedSecret::new(b"fedcba9876543210".to_vec()).expect("secret"),
                    ),
                    tcp_streams: Arc::new(ServerTcpStreamRegistry::default()),
                    max_tcp_streams: ResourceLimits::default().max_streams,
                    max_udp_sessions: ResourceLimits::default().max_streams,
                    max_udp_flows_per_session: ResourceLimits::default().max_streams,
                },
            )
            .await
        });

        let stream = tcp::connect_path(&path, TcpConnectOptions::default())
            .await
            .expect("connect");
        let mut client = EncryptedFramedStream::new(
            stream,
            b"0123456789abcdef",
            PeerRole::Client,
            CodecLimits::default(),
        );
        client
            .write_frame(&Frame::SessionHello {
                session_id: SessionId(1),
            })
            .await
            .expect("write");
        client.flush().await.expect("flush");

        assert!(matches!(
            server.await.expect("join"),
            Err(RuntimeError::Encrypted(
                EncryptedFramedTransportError::Crypto
            ))
        ));
    }
}
