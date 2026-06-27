use crate::config::{AppConfig, ClientConfig, CommandConfig, ResourceLimits, SecurityConfig};
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
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::Hash;
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
const TCP_STREAM_STALL_MAX_TIMEOUT: Duration = Duration::from_millis(1500);
const UDP_DATAGRAM_MIN_TTL_FIT_RATIO: f64 = 0.9;
const UDP_BBR_PACING_GAIN: f64 = 1.25;
const UDP_FIRST_OPEN_RTT_MULTIPLIER: f64 = 8.0;
const UDP_MIN_PACING_RATE_BPS: f64 = 64_000.0;
const UDP_MAX_RESPONSE_TIMEOUT: Duration = Duration::from_secs(1);
const UDP_MIN_RESPONSE_TIMEOUT: Duration = Duration::from_millis(50);
const UDP_MIN_RETRY_BUDGET: Duration = Duration::from_millis(250);
const UDP_MAX_RETRY_BUDGET: Duration = Duration::from_millis(500);
const UDP_MIN_PATH_SUPPRESSION: Duration = Duration::from_millis(250);
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
    let context = ClientPathContext::new(client.paths, security, resources)?;
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
    let remote = open_remote_stream(
        &context,
        target.clone(),
        IngressKind::TunTcp,
        TrafficClass::Interactive,
    )
    .await?;
    relay_migrating_tcp_stream(
        stream,
        &context,
        TcpRelayOpenSpec {
            target,
            ingress: IngressKind::TunTcp,
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

struct UdpEdgeRequest<M> {
    target: TargetAddr,
    payload: Bytes,
    ttl_ms: u32,
    metadata: M,
}

struct UdpEdgeCompletion<M> {
    lane_id: usize,
    target: TargetAddr,
    metadata: M,
    result: Result<Bytes, RuntimeError>,
}

struct UdpEdgeLane<M> {
    lane_id: usize,
    pending: usize,
    successful_completions: usize,
    requests: mpsc::Sender<UdpEdgeRequest<M>>,
    handle: tokio::task::JoinHandle<()>,
}

fn udp_edge_queue_slots(context: &ClientPathContext) -> usize {
    let payload = context.mux_limits.max_payload_bytes.max(1);
    (context.mux_limits.max_datagram_queue_bytes / payload).max(1)
}

fn udp_edge_lane_limit(context: &ClientPathContext) -> usize {
    let path_parallelism = context.udp_paths.len().max(1).saturating_mul(2);
    udp_edge_queue_slots(context).min(path_parallelism.max(1))
}

fn udp_edge_startup_lane_limit(context: &ClientPathContext) -> usize {
    let queue_slots = udp_edge_queue_slots(context);
    let hedge_lane = usize::from(queue_slots > 1 && !context.udp_paths.is_empty());
    udp_edge_lane_limit(context)
        .min(queue_slots)
        .min(1usize.saturating_add(hedge_lane))
        .max(1)
}

fn udp_edge_lane_spawn_allowed(
    lane_count: usize,
    successful_lane_count: usize,
    context: &ClientPathContext,
) -> bool {
    if lane_count < udp_edge_startup_lane_limit(context) {
        return true;
    }
    successful_lane_count > 0
}

fn udp_edge_lane_queue(context: &ClientPathContext) -> usize {
    let lanes = udp_edge_lane_limit(context).max(1);
    (udp_edge_queue_slots(context) / lanes).max(1)
}

fn udp_edge_completion_queue(context: &ClientPathContext) -> usize {
    udp_edge_lane_limit(context)
        .saturating_mul(udp_edge_lane_queue(context))
        .max(1)
}

fn spawn_udp_edge_lane<M: Send + 'static>(
    lane_id: usize,
    context: ClientPathContext,
    lane_queue: usize,
    completions: mpsc::Sender<UdpEdgeCompletion<M>>,
) -> UdpEdgeLane<M> {
    let (requests, rx) = mpsc::channel(lane_queue);
    let handle = tokio::spawn(run_udp_edge_lane(lane_id, context, rx, completions));
    UdpEdgeLane {
        lane_id,
        pending: 0,
        successful_completions: 0,
        requests,
        handle,
    }
}

async fn run_udp_edge_lane<M: Send + 'static>(
    lane_id: usize,
    context: ClientPathContext,
    mut requests: mpsc::Receiver<UdpEdgeRequest<M>>,
    completions: mpsc::Sender<UdpEdgeCompletion<M>>,
) {
    let mut association = match UdpDatagramClientAssociation::new(context) {
        Ok(association) => association,
        Err(err) => {
            eprintln!("warning: UDP edge lane could not start: {err}");
            return;
        }
    };
    while let Some(request) = requests.recv().await {
        let UdpEdgeRequest {
            target,
            payload,
            ttl_ms,
            metadata,
        } = request;
        let result = association
            .send_to_with_adaptive_retries(target.clone(), payload, ttl_ms)
            .await;
        if completions
            .send(UdpEdgeCompletion {
                lane_id,
                target,
                metadata,
                result,
            })
            .await
            .is_err()
        {
            break;
        }
    }
    if let Err(err) = association.close().await {
        eprintln!("warning: UDP edge lane close failed: {err}");
    }
}

fn dispatch_udp_edge_request<M: Send + 'static>(
    lanes: &mut Vec<UdpEdgeLane<M>>,
    next_lane_id: &mut usize,
    context: &ClientPathContext,
    completions: &mpsc::Sender<UdpEdgeCompletion<M>>,
    request: UdpEdgeRequest<M>,
) -> Result<(), UdpEdgeRequest<M>> {
    let lane_limit = udp_edge_lane_limit(context);
    let lane_queue = udp_edge_lane_queue(context);
    let successful_lane_count = lanes
        .iter()
        .filter(|lane| lane.successful_completions > 0)
        .count();
    if lanes.is_empty()
        || (lanes.len() < lane_limit
            && lanes.iter().all(|lane| lane.pending > 0)
            && udp_edge_lane_spawn_allowed(lanes.len(), successful_lane_count, context))
    {
        let lane_id = *next_lane_id;
        *next_lane_id = next_lane_id.saturating_add(1);
        lanes.push(spawn_udp_edge_lane(
            lane_id,
            context.clone(),
            lane_queue,
            completions.clone(),
        ));
    }

    let Some((position, _)) = lanes
        .iter()
        .enumerate()
        .min_by_key(|(_, lane)| (lane.pending, lane.lane_id))
    else {
        return Err(request);
    };

    match lanes[position].requests.try_send(request) {
        Ok(()) => {
            lanes[position].pending = lanes[position].pending.saturating_add(1);
            Ok(())
        }
        Err(mpsc::error::TrySendError::Full(request)) => Err(request),
        Err(mpsc::error::TrySendError::Closed(request)) => {
            lanes.swap_remove(position);
            Err(request)
        }
    }
}

fn finish_udp_edge_completion<M>(lanes: &mut [UdpEdgeLane<M>], completion: &UdpEdgeCompletion<M>) {
    if let Some(lane) = lanes
        .iter_mut()
        .find(|lane| lane.lane_id == completion.lane_id)
    {
        lane.pending = lane.pending.saturating_sub(1);
        if completion.result.is_ok() {
            lane.successful_completions = lane.successful_completions.saturating_add(1);
        }
    }
}

async fn close_udp_edge_lanes<M>(lanes: Vec<UdpEdgeLane<M>>) {
    let handles = lanes
        .into_iter()
        .map(|lane| lane.handle)
        .collect::<Vec<_>>();
    for handle in handles {
        if let Err(err) = handle.await {
            eprintln!("warning: UDP edge lane task failed: {err}");
        }
    }
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
    let (completion_tx, mut completion_rx) =
        mpsc::channel::<UdpEdgeCompletion<TunUdpFlowKey>>(udp_edge_completion_queue(&context));
    let mut lanes = Vec::<UdpEdgeLane<TunUdpFlowKey>>::new();
    let mut next_lane_id = 0usize;
    let target = TargetAddr::Ip(tun_udp_target_for_remote(key.remote, &tun));
    let ttl_ms = tun_udp_ttl_ms(key.remote, &tun);
    let result = loop {
        tokio::select! {
            payload = tokio::time::timeout(TUN_UDP_FLOW_IDLE_TIMEOUT, datagrams.recv()) => {
                let payload = match payload {
                    Ok(Some(payload)) => payload,
                    Ok(None) | Err(_) => break Ok(()),
                };
                if dispatch_udp_edge_request(
                    &mut lanes,
                    &mut next_lane_id,
                    &context,
                    &completion_tx,
                    UdpEdgeRequest {
                        target: target.clone(),
                        payload: Bytes::from(payload),
                        ttl_ms,
                        metadata: key,
                    },
                )
                .is_err()
                {
                    eprintln!("warning: TUN UDP lane queue full; dropping datagram from {} to {}", key.local, key.remote);
                }
            }
            completion = completion_rx.recv() => {
                let Some(completion) = completion else {
                    break Err(RuntimeError::Protocol("TUN UDP completion channel closed"));
                };
                finish_udp_edge_completion(&mut lanes, &completion);
                match completion.result {
                    Ok(response) => {
                        responses
                            .send(TunUdpResponse {
                                payload: response.to_vec(),
                                source: completion.metadata.remote,
                                destination: completion.metadata.local,
                            })
                            .await
                            .map_err(|_| RuntimeError::Protocol("TUN UDP response channel closed"))?;
                    }
                    Err(err) => {
                        eprintln!(
                            "warning: TUN UDP datagram {} -> {} failed: {err}",
                            completion.metadata.local, completion.metadata.remote
                        );
                    }
                }
            }
            else => break Ok(()),
        }
    };
    drop(completion_tx);
    close_udp_edge_lanes(lanes).await;
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
        tcp_streams: Arc::new(ServerTcpStreamRegistry::new(resources.max_streams)),
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
    loop {
        let command_may_recv = !tcp_path_receivers_closed(&session.commands_rx);
        tokio::select! {
            datagram = datagrams.recv() => {
                let Some(datagram) = datagram else {
                    return Ok(());
                };
                let frame = match session.open_frame(&datagram) {
                    Ok(frame) => frame,
                    Err(err) if udp_runtime_error_is_ignorable(&err) => continue,
                    Err(err) => return Err(err),
                };
                match session.handle_frame(frame).await? {
                    ServerUdpSessionOutcome::Active => {}
                    ServerUdpSessionOutcome::Closed => return Ok(()),
                }
            }
            command = recv_tcp_path_command(&mut session.commands_rx), if command_may_recv => {
                if let Some(command) = command {
                    match session.handle_command(command).await? {
                        ServerUdpSessionOutcome::Active => {}
                        ServerUdpSessionOutcome::Closed => return Ok(()),
                    }
                }
            }
        }
    }
}

fn udp_session_datagram_queue(context: &ServerPathContext) -> usize {
    let datagram_payload = context.mux_limits.max_payload_bytes.max(1);
    let stream_payload = udp_stream_frame_payload_bytes(context.mux_limits).max(1);
    let queue_bytes = context
        .mux_limits
        .max_datagram_queue_bytes
        .max(datagram_payload);
    (queue_bytes / datagram_payload)
        .max(queue_bytes / stream_payload)
        .max(1)
}

fn udp_session_done_queue(context: &ServerPathContext) -> usize {
    context.max_udp_sessions.max(1)
}

fn udp_runtime_error_is_ignorable(err: &RuntimeError) -> bool {
    matches!(
        err,
        RuntimeError::EncryptedUdp(EncryptedUdpTransportError::Replay)
    )
}

fn encrypted_udp_error_is_ignorable(err: &EncryptedUdpTransportError) -> bool {
    matches!(err, EncryptedUdpTransportError::Replay)
}

#[derive(Debug, Clone)]
pub struct ClientPathContext {
    tcp_paths: Arc<Vec<PathSpec>>,
    udp_paths: Arc<Vec<PathSpec>>,
    tcp_sessions: Arc<Vec<ClientTcpPathSessionHandle>>,
    udp_stream_session_id: SessionId,
    next_tcp_stream_id: Arc<Mutex<u64>>,
    health: Arc<Mutex<ClientPathHealth>>,
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
    active_latency_sensitive_flows: u32,
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
            active_latency_sensitive_flows: 0,
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
    active_latency_sensitive_flows: u32,
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
            active_latency_sensitive_flows: self.active_latency_sensitive_flows,
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

    fn mark_open_success(&mut self, elapsed: Duration, load_bytes: u64, class: TrafficClass) {
        self.mark_success(elapsed);
        self.active_flows = self.active_flows.saturating_add(1);
        if tcp_relay_expects_interactive_response(class) {
            self.active_latency_sensitive_flows =
                self.active_latency_sensitive_flows.saturating_add(1);
        }
        self.load_bytes = self.load_bytes.saturating_add(load_bytes);
    }

    fn release_load(&mut self, load_bytes: u64, class: TrafficClass) {
        self.active_flows = self.active_flows.saturating_sub(1);
        if tcp_relay_expects_interactive_response(class) {
            self.active_latency_sensitive_flows =
                self.active_latency_sensitive_flows.saturating_sub(1);
        }
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
        if self.consecutive_failures == 1 {
            self.state = SchedulerPathState::Suspect;
            self.failed_until = None;
        } else {
            self.state = SchedulerPathState::Failed;
            self.failed_until = Some(now + PATH_FAILURE_COOLDOWN);
        }
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

#[derive(Debug)]
struct RecentIdCache<T>
where
    T: Copy + Eq + Hash,
{
    capacity: usize,
    order: VecDeque<T>,
    set: HashSet<T>,
}

impl<T> RecentIdCache<T>
where
    T: Copy + Eq + Hash,
{
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            order: VecDeque::with_capacity(capacity.min(1024)),
            set: HashSet::new(),
        }
    }

    fn insert(&mut self, id: T) {
        if self.set.contains(&id) {
            return;
        }
        self.order.push_back(id);
        self.set.insert(id);
        while self.order.len() > self.capacity {
            if let Some(expired) = self.order.pop_front() {
                self.set.remove(&expired);
            }
        }
    }

    fn contains(&self, id: &T) -> bool {
        self.set.contains(id)
    }
}

fn tcp_closed_stream_cache_capacity(max_streams: usize) -> usize {
    max_streams.saturating_mul(2).clamp(128, 65_536)
}

struct TcpPathStream {
    stream_id: StreamId,
    max_offset: u64,
    class: TrafficClass,
    underlay: UnderlayProtocol,
    max_frame_payload_bytes: usize,
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
                class: self.class,
                underlay: self.underlay,
                max_frame_payload_bytes: self.max_frame_payload_bytes,
                output: self.output,
            },
            self.frames,
        )
    }

    async fn send_frame(&self, frame: Frame) -> Result<(), RuntimeError> {
        self.output
            .send_frame(self.stream_id, self.class, frame)
            .await
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
    class: TrafficClass,
    underlay: UnderlayProtocol,
    max_frame_payload_bytes: usize,
    output: TcpPathStreamOutput,
}

impl TcpPathStreamHandle {
    async fn send_frame(&self, frame: Frame) -> Result<(), RuntimeError> {
        self.output
            .send_frame(self.stream_id, self.class, frame)
            .await
    }

    async fn close(&self) {
        self.output.close_stream(self.stream_id).await;
    }
}

#[derive(Clone)]
enum TcpPathStreamOutput {
    Fixed(TcpPathSessionCommandSender),
    Switchable(Arc<ServerTcpStreamBinding>),
}

impl TcpPathStreamOutput {
    async fn send_frame(
        &self,
        stream_id: StreamId,
        class: TrafficClass,
        frame: Frame,
    ) -> Result<(), RuntimeError> {
        match self {
            Self::Fixed(commands) => commands.send_frame(frame, class).await,
            Self::Switchable(binding) => binding.send_frame(stream_id, class, frame).await,
        }
    }

    async fn close_stream(&self, stream_id: StreamId) {
        match self {
            Self::Fixed(commands) => {
                let _ = commands
                    .send_frame(Frame::StreamDetach { stream_id }, TrafficClass::Control)
                    .await;
                let _ = commands
                    .send_control(TcpPathSessionCommand::CloseStream(stream_id))
                    .await;
            }
            Self::Switchable(binding) => binding.close_stream(stream_id).await,
        }
    }
}

struct ServerTcpStreamBinding {
    class: Mutex<TrafficClass>,
    outputs: Mutex<ServerTcpStreamOutputs>,
    version: watch::Sender<u64>,
}

impl ServerTcpStreamBinding {
    fn new(
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: TcpPathSessionCommandSender,
        class: TrafficClass,
    ) -> Arc<Self> {
        let (version, _) = watch::channel(0);
        Arc::new(Self {
            class: Mutex::new(class),
            outputs: Mutex::new(ServerTcpStreamOutputs {
                next_index: 0,
                entries: vec![ServerTcpStreamOutputEntry {
                    key: ServerTcpPathKey { underlay, path_id },
                    commands,
                }],
            }),
            version,
        })
    }

    fn attach(
        &self,
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: TcpPathSessionCommandSender,
        class: TrafficClass,
    ) {
        *self.class.lock().expect("server TCP stream class lock") = class;
        let mut outputs = self.outputs.lock().expect("server TCP stream binding lock");
        let key = ServerTcpPathKey { underlay, path_id };
        if let Some(position) = outputs.entries.iter().position(|entry| entry.key == key) {
            let mut entry = outputs.entries.remove(position);
            entry.commands = commands;
            outputs.entries.push(entry);
        } else {
            outputs
                .entries
                .push(ServerTcpStreamOutputEntry { key, commands });
        }
        outputs.next_index %= outputs.entries.len().max(1);
        drop(outputs);
        self.notify_update();
    }

    fn class(&self) -> TrafficClass {
        *self.class.lock().expect("server TCP stream class lock")
    }

    fn detach(&self, key: ServerTcpPathKey, commands: &TcpPathSessionCommandSender) {
        let mut outputs = self.outputs.lock().expect("server TCP stream binding lock");
        let before = outputs.entries.len();
        outputs
            .entries
            .retain(|entry| entry.key != key || !entry.commands.same_channel(commands));
        if outputs.entries.len() != before {
            outputs.next_index %= outputs.entries.len().max(1);
            drop(outputs);
            self.notify_update();
        }
    }

    fn next_commands(&self) -> Option<(ServerTcpPathKey, TcpPathSessionCommandSender)> {
        let mut outputs = self.outputs.lock().expect("server TCP stream binding lock");
        if outputs.entries.is_empty() {
            return None;
        }
        outputs.next_index %= outputs.entries.len();
        let entry = outputs.entries[outputs.next_index].clone();
        outputs.next_index = (outputs.next_index + 1) % outputs.entries.len();
        Some((entry.key, entry.commands))
    }

    fn data_commands(&self) -> Option<(ServerTcpPathKey, TcpPathSessionCommandSender)> {
        self.outputs
            .lock()
            .expect("server TCP stream binding lock")
            .entries
            .last()
            .cloned()
            .map(|entry| (entry.key, entry.commands))
    }

    async fn send_frame(
        &self,
        _stream_id: StreamId,
        _class: TrafficClass,
        frame: Frame,
    ) -> Result<(), RuntimeError> {
        let mut updates = self.version.subscribe();
        loop {
            let selected = if server_frame_prefers_current_data_path(&frame) {
                self.data_commands()
            } else {
                self.next_commands()
            };
            if let Some((key, commands)) = selected {
                let class = self.class();
                tokio::select! {
                    result = commands.send_frame(frame.clone(), class) => {
                        match result {
                            Ok(()) => return Ok(()),
                            Err(_) => self.detach(key, &commands),
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
                .send_control(TcpPathSessionCommand::CloseStream(stream_id))
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ServerTcpPathKey {
    underlay: UnderlayProtocol,
    path_id: PathId,
}

#[derive(Clone)]
struct ServerTcpStreamOutputEntry {
    key: ServerTcpPathKey,
    commands: TcpPathSessionCommandSender,
}

struct ServerTcpStreamOutputs {
    entries: Vec<ServerTcpStreamOutputEntry>,
    next_index: usize,
}

struct ServerTcpStreamRegistry {
    streams: Mutex<HashMap<(SessionId, StreamId), ServerTcpStreamEntry>>,
    closed_streams: Mutex<RecentIdCache<(SessionId, StreamId)>>,
}

impl std::fmt::Debug for ServerTcpStreamRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerTcpStreamRegistry")
            .finish_non_exhaustive()
    }
}

struct ServerTcpStreamEntry {
    target: TargetAddr,
    class: TrafficClass,
    frames: mpsc::Sender<Result<Frame, RuntimeError>>,
    binding: Arc<ServerTcpStreamBinding>,
}

struct ServerTcpPathAttachment {
    path_id: PathId,
    underlay: UnderlayProtocol,
    commands: TcpPathSessionCommandSender,
    max_frame_payload_bytes: usize,
}

struct ServerTcpStreamOpenRequest<'a> {
    session_id: SessionId,
    stream_id: StreamId,
    target: &'a TargetAddr,
    class: TrafficClass,
    attachment: ServerTcpPathAttachment,
}

enum ServerTcpStreamOpen {
    New(TcpPathStream),
    Existing,
}

impl ServerTcpStreamRegistry {
    fn new(max_streams: usize) -> Self {
        Self {
            streams: Mutex::new(HashMap::new()),
            closed_streams: Mutex::new(RecentIdCache::new(tcp_closed_stream_cache_capacity(
                max_streams,
            ))),
        }
    }

    fn open_or_attach(
        &self,
        request: ServerTcpStreamOpenRequest<'_>,
        mux_limits: MuxLimits,
        max_streams: usize,
    ) -> Result<ServerTcpStreamOpen, RuntimeError> {
        let ServerTcpStreamOpenRequest {
            session_id,
            stream_id,
            target,
            class,
            attachment,
        } = request;
        let max_frame_payload_bytes = attachment.max_frame_payload_bytes;
        let underlay = attachment.underlay;
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
            entry.class = class;
            entry
                .binding
                .attach(underlay, attachment.path_id, attachment.commands, class);
            return Ok(ServerTcpStreamOpen::Existing);
        }

        if streams.len() >= max_streams {
            return Err(RuntimeError::Protocol("server TCP stream limit reached"));
        }

        let (frames_tx, frames_rx) = mpsc::channel(tcp_stream_frame_queue(mux_limits));
        let binding =
            ServerTcpStreamBinding::new(underlay, attachment.path_id, attachment.commands, class);
        streams.insert(
            (session_id, stream_id),
            ServerTcpStreamEntry {
                target: target.clone(),
                class,
                frames: frames_tx,
                binding: binding.clone(),
            },
        );
        Ok(ServerTcpStreamOpen::New(TcpPathStream {
            stream_id,
            max_offset: mux_limits.max_stream_window_bytes,
            class,
            underlay,
            max_frame_payload_bytes,
            output: TcpPathStreamOutput::Switchable(binding),
            frames: frames_rx,
        }))
    }

    fn detach_path(
        &self,
        session_id: SessionId,
        stream_id: StreamId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: &TcpPathSessionCommandSender,
    ) {
        if let Some(binding) = self
            .streams
            .lock()
            .expect("server TCP stream registry lock")
            .get(&(session_id, stream_id))
            .map(|entry| entry.binding.clone())
        {
            binding.detach(ServerTcpPathKey { underlay, path_id }, commands);
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
            let closed_key = (session_id, stream_id);
            if self
                .closed_streams
                .lock()
                .expect("server TCP stream closed cache lock")
                .contains(&closed_key)
            {
                return Ok(());
            }
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
        let removed = self
            .streams
            .lock()
            .expect("server TCP stream registry lock")
            .remove(&(session_id, stream_id))
            .is_some();
        if removed {
            self.closed_streams
                .lock()
                .expect("server TCP stream closed cache lock")
                .insert((session_id, stream_id));
        }
    }
}

impl Default for ServerTcpStreamRegistry {
    fn default() -> Self {
        Self::new(ResourceLimits::default().max_streams)
    }
}

struct ClientTcpPathSessionHandle {
    runtime: ClientTcpPathSessionRuntime,
    commands: Arc<Mutex<Option<TcpPathSessionCommandSender>>>,
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
        let commands = self.ensure_session(class);
        let (response_tx, response_rx) = oneshot::channel();
        commands
            .send_control(TcpPathSessionCommand::OpenStream {
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

    fn ensure_session(&self, class: TrafficClass) -> TcpPathSessionCommandSender {
        if tcp_path_class_uses_dedicated_session(class) {
            let (commands, receivers) =
                tcp_path_session_command_channels(self.runtime.command_queue);
            tokio::spawn(run_client_tcp_path_session(self.runtime.clone(), receivers));
            return commands;
        }

        let mut current = self.commands.lock().expect("TCP path session lock");
        if let Some(commands) = current.as_ref()
            && !commands.is_closed()
        {
            return commands.clone();
        }

        let (commands, receivers) = tcp_path_session_command_channels(self.runtime.command_queue);
        tokio::spawn(run_client_tcp_path_session(self.runtime.clone(), receivers));
        *current = Some(commands.clone());
        commands
    }
}

fn tcp_path_class_uses_dedicated_session(class: TrafficClass) -> bool {
    matches!(class, TrafficClass::Control | TrafficClass::Interactive)
}

#[derive(Clone)]
struct TcpPathSessionCommandSender {
    control: mpsc::Sender<TcpPathSessionCommand>,
    priority: mpsc::Sender<TcpPathSessionCommand>,
    data: mpsc::Sender<TcpPathSessionCommand>,
}

struct TcpPathSessionCommandReceivers {
    control: mpsc::Receiver<TcpPathSessionCommand>,
    priority: mpsc::Receiver<TcpPathSessionCommand>,
    data: mpsc::Receiver<TcpPathSessionCommand>,
}

impl TcpPathSessionCommandSender {
    async fn send_control(
        &self,
        command: TcpPathSessionCommand,
    ) -> Result<(), mpsc::error::SendError<TcpPathSessionCommand>> {
        self.control.send(command).await
    }

    async fn send_frame(&self, frame: Frame, class: TrafficClass) -> Result<(), RuntimeError> {
        let queue = if tcp_path_frame_uses_priority_queue(class) {
            &self.priority
        } else {
            &self.data
        };
        queue
            .send(TcpPathSessionCommand::SendFrame(frame))
            .await
            .map_err(|_| RuntimeError::TcpPathSessionClosed)
    }

    fn is_closed(&self) -> bool {
        self.control.is_closed() && self.priority.is_closed() && self.data.is_closed()
    }

    fn same_channel(&self, other: &Self) -> bool {
        self.control.same_channel(&other.control)
            && self.priority.same_channel(&other.priority)
            && self.data.same_channel(&other.data)
    }
}

fn tcp_path_frame_uses_priority_queue(class: TrafficClass) -> bool {
    matches!(
        class,
        TrafficClass::Control | TrafficClass::Interactive | TrafficClass::RealtimeDatagram
    )
}

fn tcp_path_session_command_channels(
    queue: usize,
) -> (TcpPathSessionCommandSender, TcpPathSessionCommandReceivers) {
    let queue = queue.max(1);
    let (control_tx, control_rx) = mpsc::channel(queue);
    let (priority_tx, priority_rx) = mpsc::channel(queue);
    let (data_tx, data_rx) = mpsc::channel(queue);
    (
        TcpPathSessionCommandSender {
            control: control_tx,
            priority: priority_tx,
            data: data_tx,
        },
        TcpPathSessionCommandReceivers {
            control: control_rx,
            priority: priority_rx,
            data: data_rx,
        },
    )
}

fn tcp_receiver_may_recv<T>(receiver: &mpsc::Receiver<T>) -> bool {
    !receiver.is_closed() || !receiver.is_empty()
}

fn tcp_path_receivers_closed(receivers: &TcpPathSessionCommandReceivers) -> bool {
    !tcp_receiver_may_recv(&receivers.control)
        && !tcp_receiver_may_recv(&receivers.priority)
        && !tcp_receiver_may_recv(&receivers.data)
}

async fn recv_tcp_path_command(
    receivers: &mut TcpPathSessionCommandReceivers,
) -> Option<TcpPathSessionCommand> {
    let control_may_recv = tcp_receiver_may_recv(&receivers.control);
    let priority_may_recv = tcp_receiver_may_recv(&receivers.priority);
    let data_may_recv = tcp_receiver_may_recv(&receivers.data);
    match (control_may_recv, priority_may_recv, data_may_recv) {
        (true, true, true) => {
            tokio::select! {
                biased;
                command = receivers.control.recv() => command,
                command = receivers.priority.recv() => command,
                command = receivers.data.recv() => command,
            }
        }
        (true, true, false) => {
            tokio::select! {
                biased;
                command = receivers.control.recv() => command,
                command = receivers.priority.recv() => command,
            }
        }
        (true, false, true) => {
            tokio::select! {
                biased;
                command = receivers.control.recv() => command,
                command = receivers.data.recv() => command,
            }
        }
        (false, true, true) => {
            tokio::select! {
                biased;
                command = receivers.priority.recv() => command,
                command = receivers.data.recv() => command,
            }
        }
        (true, false, false) => receivers.control.recv().await,
        (false, true, false) => receivers.priority.recv().await,
        (false, false, true) => receivers.data.recv().await,
        (false, false, false) => None,
    }
}

enum TcpPathSessionCommand {
    OpenStream {
        stream_id: StreamId,
        target: TargetAddr,
        ingress: IngressKind,
        class: TrafficClass,
        session_commands: TcpPathSessionCommandSender,
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
    session_commands: TcpPathSessionCommandSender,
    class: TrafficClass,
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
    closed_stream_cache_capacity: usize,
}

struct ClientTcpPathSessionState {
    connection: Option<ClientTcpPathConnection>,
    streams: HashMap<StreamId, ClientTcpPathStreamState>,
    closed_streams: RecentIdCache<StreamId>,
}

struct ClientTcpOpenStreamRequest {
    stream_id: StreamId,
    target: TargetAddr,
    ingress: IngressKind,
    class: TrafficClass,
    session_commands: TcpPathSessionCommandSender,
    response: oneshot::Sender<Result<TcpPathStream, RuntimeError>>,
}

async fn run_client_tcp_path_session(
    runtime: ClientTcpPathSessionRuntime,
    mut commands: TcpPathSessionCommandReceivers,
) {
    let mut state = ClientTcpPathSessionState {
        connection: None,
        streams: HashMap::new(),
        closed_streams: RecentIdCache::new(runtime.closed_stream_cache_capacity),
    };

    loop {
        if state.connection.is_none() {
            match recv_tcp_path_command(&mut commands).await {
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

        let command_may_recv = !tcp_path_receivers_closed(&commands);
        if !command_may_recv {
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

        let mut drop_connection = false;
        tokio::select! {
            biased;
            command = recv_tcp_path_command(&mut commands), if command_may_recv => {
                match command {
                    Some(command) => {
                        if let Err(err) = handle_connected_client_tcp_command(
                            command,
                            state.connection.as_mut().expect("checked connected TCP path session"),
                            &mut state.streams,
                            &mut state.closed_streams,
                            runtime.stream_frame_queue,
                            runtime.mux_limits,
                        )
                        .await
                        {
                            fail_client_tcp_streams(&mut state.streams, &err);
                            eprintln!("warning: TCP path session command failed: {err}");
                            drop_connection = true;
                        }
                    }
                    None => {
                        if tcp_path_receivers_closed(&commands) {
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
            }
            frame = state.connection.as_mut().expect("checked connected TCP path session").frames.recv() => {
                match frame {
                    Some(Ok(frame)) => {
                        if let Err(err) = handle_client_tcp_path_frame(
                            frame,
                            state.connection.as_mut().expect("checked connected TCP path session"),
                            &mut state.streams,
                            &mut state.closed_streams,
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
                    !state.streams.is_empty(),
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
    closed_streams: &mut RecentIdCache<StreamId>,
    stream_frame_queue: usize,
    mux_limits: MuxLimits,
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
                .await?;
            record_client_tcp_path_outbound_activity(connection, mux_limits);
            Ok(())
        }
        TcpPathSessionCommand::SendFrame(frame) => {
            connection.writer.write_frame(&frame).await?;
            connection.writer.flush().await?;
            record_client_tcp_path_outbound_activity(connection, mux_limits);
            Ok(())
        }
        TcpPathSessionCommand::CloseStream(stream_id) => {
            if streams.remove(&stream_id).is_some() {
                closed_streams.insert(stream_id);
            }
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
    let now = tokio::time::Instant::now();
    Ok(ClientTcpPathConnection {
        writer,
        frames: spawn_encrypted_tcp_reader(reader, tcp_path_session_frame_queue(mux_limits)),
        heartbeat_interval: mux_limits.tcp_path_heartbeat_interval,
        next_heartbeat_at: now + mux_limits.tcp_path_heartbeat_interval,
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
                class: open.class,
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
    closed_streams: &mut RecentIdCache<StreamId>,
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
                    class: pending.class,
                    underlay: UnderlayProtocol::Tcp,
                    max_frame_payload_bytes: tcp_relay_buffer_len(mux_limits),
                    output: TcpPathStreamOutput::Fixed(pending.session_commands),
                    frames,
                };
                let _ = pending.response.send(Ok(stream));
                return Ok(());
            }
            route_client_tcp_stream_frame(
                streams,
                closed_streams,
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
                closed_streams.insert(stream_id);
                let _ = pending
                    .response
                    .send(Err(RuntimeError::RemoteReset(reason)));
                return Ok(());
            }
            route_client_tcp_stream_frame(
                streams,
                closed_streams,
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
                closed_streams,
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
                closed_streams,
                stream_id,
                Frame::StreamAck { stream_id, ranges },
            )
            .await
        }
        Frame::StreamFin { stream_id } => {
            route_client_tcp_stream_frame(
                streams,
                closed_streams,
                stream_id,
                Frame::StreamFin { stream_id },
            )
            .await
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

fn record_client_tcp_path_outbound_activity(
    connection: &mut ClientTcpPathConnection,
    mux_limits: MuxLimits,
) {
    refresh_client_tcp_path_liveness(connection, mux_limits);
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
    closed_streams: &mut RecentIdCache<StreamId>,
    stream_id: StreamId,
    frame: Frame,
) -> Result<(), RuntimeError> {
    let Some(state) = streams.get_mut(&stream_id) else {
        if closed_streams.contains(&stream_id) {
            return Ok(());
        }
        return Err(RuntimeError::Protocol("frame for unknown TCP stream"));
    };
    if state.frames.send(Ok(frame)).await.is_err() {
        streams.remove(&stream_id);
        closed_streams.insert(stream_id);
    }
    Ok(())
}

async fn tick_client_tcp_path_heartbeat(
    connection: &mut ClientTcpPathConnection,
    mux_limits: MuxLimits,
    has_active_streams: bool,
) -> Result<(), RuntimeError> {
    let now = tokio::time::Instant::now();
    if let Some((_, deadline)) = connection.pending_heartbeat.as_ref()
        && now >= *deadline
    {
        if has_active_streams {
            connection.pending_heartbeat = None;
            connection.next_heartbeat_at = now + connection.heartbeat_interval;
            return Ok(());
        }
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
    tcp_path_command_queue(resources.into())
}

fn tcp_path_command_queue(mux_limits: MuxLimits) -> usize {
    let frame_payload = mux_limits
        .max_tcp_relay_chunk_bytes
        .min(mux_limits.max_payload_bytes)
        .max(1);
    let inflight_frames = mux_limits
        .max_tcp_path_inflight_bytes
        .saturating_add(frame_payload - 1)
        / frame_payload;
    inflight_frames
        .saturating_add(4)
        .clamp(4, tcp_path_session_frame_queue(mux_limits).max(4))
}

fn udp_stream_path_command_queue(mux_limits: MuxLimits) -> usize {
    let frame_payload = udp_stream_frame_payload_bytes(mux_limits).max(1);
    let inflight_frames = mux_limits
        .max_tcp_path_inflight_bytes
        .saturating_add(frame_payload - 1)
        / frame_payload;
    inflight_frames
        .saturating_add(4)
        .clamp(16, tcp_path_session_frame_queue(mux_limits).max(16))
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
        let udp_stream_session_id = random_session_id()?;
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
                    closed_stream_cache_capacity: tcp_closed_stream_cache_capacity(
                        resources.max_streams,
                    ),
                })
            })
            .collect::<Vec<_>>();
        Ok(Self {
            tcp_paths: Arc::new(tcp_paths),
            udp_paths: Arc::new(udp_paths),
            tcp_sessions: Arc::new(tcp_sessions),
            udp_stream_session_id,
            next_tcp_stream_id: Arc::new(Mutex::new(0)),
            health: Arc::new(Mutex::new(health)),
            codec_limits,
            mux_limits,
            security,
        })
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
        let observations = self.tcp_health_observations_for_class(class);
        if reliable_stream_latency_startup_should_use_configured_order(
            &self.tcp_paths,
            &observations,
            class,
        ) {
            return configured_order_path_indices(
                &self.tcp_paths,
                &observations,
                class,
                payload_bytes,
            );
        }
        ordered_path_indices(&self.tcp_paths, &observations, class, payload_bytes)
    }

    fn ordered_udp_stream_path_indices(
        &self,
        class: TrafficClass,
        payload_bytes: usize,
    ) -> Vec<usize> {
        let observations =
            health_observations(&mut self.health.lock().expect("client path health lock").udp);
        if reliable_stream_latency_startup_should_use_configured_order(
            &self.udp_paths,
            &observations,
            class,
        ) {
            return configured_order_path_indices(
                &self.udp_paths,
                &observations,
                class,
                payload_bytes,
            );
        }
        ordered_path_indices(&self.udp_paths, &observations, class, payload_bytes)
    }

    fn ordered_tcp_repair_path_indices(
        &self,
        current_path_index: Option<usize>,
        class: TrafficClass,
        payload_bytes: usize,
    ) -> Vec<usize> {
        let scores = ordered_path_scores(
            &self.tcp_paths,
            &self.tcp_health_observations_for_class(class),
            class,
            payload_bytes,
        );
        if !matches!(class, TrafficClass::Bulk | TrafficClass::Background) {
            return scores.into_iter().map(|(index, _)| index).collect();
        }
        let current_eta = current_path_index.and_then(|current_path_index| {
            scores
                .iter()
                .find_map(|(index, eta)| (*index == current_path_index).then_some(*eta))
        });
        scores
            .into_iter()
            .filter(|(index, eta)| {
                Some(*index) != current_path_index
                    && current_eta.is_none_or(|current| *eta < current)
            })
            .map(|(index, _)| index)
            .collect()
    }

    fn ordered_udp_stream_auto_bulk_discovery_indices(
        &self,
        current_path_index: Option<usize>,
        payload_bytes: usize,
    ) -> Vec<usize> {
        self.ordered_udp_stream_auto_bulk_discovery_scores(current_path_index, payload_bytes)
            .into_iter()
            .map(|(index, _)| index)
            .collect()
    }

    fn ordered_udp_stream_repair_path_indices(
        &self,
        current_path_index: Option<usize>,
        class: TrafficClass,
        payload_bytes: usize,
        require_delivery_evidence: bool,
    ) -> Vec<usize> {
        let observations =
            health_observations(&mut self.health.lock().expect("client path health lock").udp);
        let scores = if reliable_stream_latency_startup_should_use_configured_order(
            &self.udp_paths,
            &observations,
            class,
        ) {
            configured_order_path_scores(&self.udp_paths, &observations, class, payload_bytes)
        } else {
            ordered_path_scores(&self.udp_paths, &observations, class, payload_bytes)
        };
        scores
            .into_iter()
            .filter(|(index, _)| Some(*index) != current_path_index)
            .filter(|(index, _)| {
                if !require_delivery_evidence {
                    return true;
                }
                let Some(path) = self.udp_paths.get(*index) else {
                    return false;
                };
                let observation =
                    observations
                        .get(*index)
                        .copied()
                        .unwrap_or(ClientPathObservation {
                            state: SchedulerPathState::Suspect,
                            measured_srtt_ms: None,
                            measured_jitter_ms: None,
                            measured_rate_bps: None,
                            measured_loss_rate: None,
                            measured_mtu_payload_bytes: None,
                            active_flows: 0,
                            active_latency_sensitive_flows: 0,
                            load_bytes: 0,
                        });
                udp_stream_path_can_be_auto_discovered(path, observation)
            })
            .map(|(index, _)| index)
            .collect()
    }

    fn ordered_reliable_auto_bulk_discovery_path_keys(
        &self,
        current_tcp_path_index: Option<usize>,
        current_udp_path_index: Option<usize>,
        payload_bytes: usize,
    ) -> Vec<RelayPathKey> {
        let mut candidates = self
            .ordered_tcp_auto_bulk_discovery_scores(current_tcp_path_index, payload_bytes)
            .into_iter()
            .map(|(index, eta_ms)| {
                (
                    RelayPathKey {
                        underlay: UnderlayProtocol::Tcp,
                        index,
                    },
                    eta_ms,
                )
            })
            .chain(
                self.ordered_udp_stream_auto_bulk_discovery_scores(
                    current_udp_path_index,
                    payload_bytes,
                )
                .into_iter()
                .map(|(index, eta_ms)| {
                    (
                        RelayPathKey {
                            underlay: UnderlayProtocol::Udp,
                            index,
                        },
                        eta_ms,
                    )
                }),
            )
            .collect::<Vec<_>>();
        if let Some(current_eta_ms) = self.reliable_stream_current_eta_ms(
            current_tcp_path_index,
            current_udp_path_index,
            payload_bytes,
        ) {
            candidates.retain(|(_, eta_ms)| *eta_ms < current_eta_ms);
        }
        candidates.sort_by(|left, right| {
            left.1
                .total_cmp(&right.1)
                .then_with(|| relay_path_key_order(left.0, right.0))
        });
        candidates.into_iter().map(|(key, _)| key).collect()
    }

    fn reliable_stream_current_eta_ms(
        &self,
        current_tcp_path_index: Option<usize>,
        current_udp_path_index: Option<usize>,
        payload_bytes: usize,
    ) -> Option<f64> {
        [
            current_tcp_path_index.map(|index| RelayPathKey {
                underlay: UnderlayProtocol::Tcp,
                index,
            }),
            current_udp_path_index.map(|index| RelayPathKey {
                underlay: UnderlayProtocol::Udp,
                index,
            }),
        ]
        .into_iter()
        .flatten()
        .filter_map(|key| {
            relay_path_snapshot(self, key).and_then(|snapshot| {
                scheduler::score_path(
                    snapshot,
                    TrafficClass::Bulk,
                    payload_bytes,
                    SchedulerPolicy::default(),
                )
                .map(|score| score.eta_ms)
            })
        })
        .min_by(|left, right| left.total_cmp(right))
    }

    fn ordered_tcp_auto_bulk_discovery_scores(
        &self,
        current_path_index: Option<usize>,
        payload_bytes: usize,
    ) -> Vec<(usize, f64)> {
        let observations = self.tcp_health_observations_for_class(TrafficClass::Bulk);
        let any_measured_delivery = observations
            .iter()
            .any(|observation| observation.measured_rate_bps.is_some());
        if !any_measured_delivery && self.tcp_paths.iter().all(path_is_endpoint_only) {
            return Vec::new();
        }
        let scores = ordered_path_scores(
            &self.tcp_paths,
            &observations,
            TrafficClass::Bulk,
            payload_bytes,
        );
        reliable_auto_bulk_discovery_scores(
            &self.tcp_paths,
            &observations,
            scores,
            current_path_index,
            path_can_be_auto_discovered,
        )
    }

    fn ordered_udp_stream_auto_bulk_discovery_scores(
        &self,
        current_path_index: Option<usize>,
        payload_bytes: usize,
    ) -> Vec<(usize, f64)> {
        let observations =
            health_observations(&mut self.health.lock().expect("client path health lock").udp);
        let any_measured_delivery = observations
            .iter()
            .any(|observation| observation.measured_rate_bps.is_some());
        if !any_measured_delivery && self.udp_paths.iter().all(path_is_endpoint_only) {
            return Vec::new();
        }
        let scores = ordered_path_scores(
            &self.udp_paths,
            &observations,
            TrafficClass::Bulk,
            payload_bytes,
        );
        reliable_auto_bulk_discovery_scores(
            &self.udp_paths,
            &observations,
            scores,
            current_path_index,
            udp_stream_path_can_be_auto_discovered,
        )
    }

    fn tcp_health_observations_for_class(&self, class: TrafficClass) -> Vec<ClientPathObservation> {
        let mut observations =
            health_observations(&mut self.health.lock().expect("client path health lock").tcp);
        apply_tcp_bulk_isolation(&mut observations, class, self.mux_limits);
        observations
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

    fn udp_path_snapshot(&self, index: usize) -> Option<PathSnapshot> {
        let path = self.udp_paths.get(index)?;
        let observation = self
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)?
            .observe(Instant::now());
        Some(path_snapshot(path, index, observation))
    }

    fn ordered_udp_path_candidates_for_ttl(
        &self,
        payload_bytes: usize,
        ttl_ms: u32,
    ) -> Vec<UdpPathCandidate> {
        if ttl_ms == 0 {
            return Vec::new();
        }
        let observations =
            health_observations(&mut self.health.lock().expect("client path health lock").udp);
        if self.udp_paths.iter().all(path_is_endpoint_only)
            && !observations
                .iter()
                .any(udp_observation_has_datagram_feedback)
        {
            let freshness_budget_ms = f64::from(ttl_ms) * UDP_DATAGRAM_MIN_TTL_FIT_RATIO;
            return configured_order_path_indices(
                &self.udp_paths,
                &observations,
                TrafficClass::RealtimeDatagram,
                payload_bytes,
            )
            .into_iter()
            .find_map(|path_index| {
                let path = self.udp_paths.get(path_index)?;
                let observation = observations.get(path_index).copied()?;
                let eta_ms = scheduler::score_path(
                    path_snapshot(path, path_index, observation),
                    TrafficClass::RealtimeDatagram,
                    payload_bytes,
                    SchedulerPolicy::default(),
                )?
                .eta_ms;
                (eta_ms <= freshness_budget_ms).then_some(UdpPathCandidate { path_index, eta_ms })
            })
            .into_iter()
            .collect();
        }
        let mut candidates = ordered_path_scores_for_ttl(
            &self.udp_paths,
            &observations,
            TrafficClass::RealtimeDatagram,
            payload_bytes,
            ttl_ms,
        )
        .into_iter()
        .map(|(path_index, eta_ms)| UdpPathCandidate { path_index, eta_ms })
        .collect::<Vec<_>>();
        if candidates
            .iter()
            .any(|candidate| self.udp_path_candidate_has_realtime_model(*candidate, &observations))
        {
            candidates.retain(|candidate| {
                self.udp_path_candidate_has_realtime_model(*candidate, &observations)
            });
        }
        candidates
    }

    fn udp_path_candidate_has_realtime_model(
        &self,
        candidate: UdpPathCandidate,
        observations: &[ClientPathObservation],
    ) -> bool {
        let Some(path) = self.udp_paths.get(candidate.path_index) else {
            return false;
        };
        observations
            .get(candidate.path_index)
            .copied()
            .is_some_and(|observation| udp_path_has_realtime_model(path, observation))
    }

    fn udp_path_eta_for_ttl(
        &self,
        index: usize,
        payload_bytes: usize,
        ttl_ms: u32,
        discount_open_udp_session: bool,
    ) -> Option<f64> {
        if ttl_ms == 0 {
            return None;
        }
        let path = self.udp_paths.get(index)?;
        let mut observation = self
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)?
            .observe(Instant::now());
        if discount_open_udp_session {
            observation.active_flows = observation.active_flows.saturating_sub(1);
            observation.load_bytes = observation
                .load_bytes
                .saturating_sub(UDP_SESSION_LOAD_BYTES);
        }
        let score = scheduler::score_path(
            path_snapshot(path, index, observation),
            TrafficClass::RealtimeDatagram,
            payload_bytes,
            SchedulerPolicy::default(),
        )?;
        let freshness_budget_ms = f64::from(ttl_ms) * UDP_DATAGRAM_MIN_TTL_FIT_RATIO;
        (score.eta_ms <= freshness_budget_ms).then_some(score.eta_ms)
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

    fn mark_tcp_path_open_success(&self, index: usize, elapsed: Duration, class: TrafficClass) {
        if let Some(current) = self
            .health
            .lock()
            .expect("client path health lock")
            .tcp
            .get_mut(index)
        {
            current.mark_open_success(elapsed, TCP_STREAM_LOAD_BYTES, class);
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

    fn release_tcp_path_load(&self, index: usize, class: TrafficClass) {
        if let Some(current) = self
            .health
            .lock()
            .expect("client path health lock")
            .tcp
            .get_mut(index)
        {
            current.release_load(TCP_STREAM_LOAD_BYTES, class);
        }
    }

    fn mark_udp_stream_path_open_success(
        &self,
        index: usize,
        elapsed: Duration,
        class: TrafficClass,
    ) {
        if let Some(current) = self
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
        {
            current.mark_open_success(elapsed, TCP_STREAM_LOAD_BYTES, class);
        }
    }

    fn release_udp_stream_path_load(&self, index: usize, class: TrafficClass) {
        if let Some(current) = self
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(index)
        {
            current.release_load(TCP_STREAM_LOAD_BYTES, class);
        }
    }

    fn mark_relay_path_failure(&self, underlay: UnderlayProtocol, index: usize) {
        match underlay {
            UnderlayProtocol::Tcp => self.mark_tcp_path_failure(index),
            UnderlayProtocol::Udp => self.mark_udp_path_failure(index),
        }
    }

    fn release_relay_path_load(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        class: TrafficClass,
    ) {
        match underlay {
            UnderlayProtocol::Tcp => self.release_tcp_path_load(index, class),
            UnderlayProtocol::Udp => self.release_udp_stream_path_load(index, class),
        }
    }

    fn mark_relay_path_delivery(
        &self,
        underlay: UnderlayProtocol,
        index: usize,
        stats: PathDeliveryStats,
    ) {
        match underlay {
            UnderlayProtocol::Tcp => self.mark_tcp_path_delivery(index, stats),
            UnderlayProtocol::Udp => self.mark_udp_path_delivery(index, stats),
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
            current.mark_open_success(
                elapsed,
                UDP_SESSION_LOAD_BYTES,
                TrafficClass::RealtimeDatagram,
            );
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
            current.release_load(UDP_SESSION_LOAD_BYTES, TrafficClass::RealtimeDatagram);
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
        let timeout_loss_gain = 1.0 + snapshot.loss_rate.clamp(0.0, 1.0);
        let model_timeout = Duration::from_secs_f64(
            (((snapshot.srtt_ms + snapshot.jitter_ms.mul_add(4.0, 25.0)) * timeout_loss_gain)
                / 1000.0)
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

fn apply_tcp_bulk_isolation(
    observations: &mut [ClientPathObservation],
    class: TrafficClass,
    mux_limits: MuxLimits,
) {
    if !matches!(class, TrafficClass::Bulk | TrafficClass::Background) {
        return;
    }
    if !observations
        .iter()
        .any(|observation| observation.measured_rate_bps.is_some())
    {
        return;
    }
    let isolation_bytes = mux_limits.max_tcp_path_inflight_bytes as u64;
    for observation in observations {
        let latency_flows = u64::from(observation.active_latency_sensitive_flows);
        observation.load_bytes = observation
            .load_bytes
            .saturating_add(latency_flows.saturating_mul(isolation_bytes));
    }
}

fn reliable_stream_latency_startup_should_use_configured_order(
    paths: &[PathSpec],
    _observations: &[ClientPathObservation],
    class: TrafficClass,
) -> bool {
    tcp_relay_expects_interactive_response(class) && paths.iter().all(path_is_endpoint_only)
}

fn path_is_endpoint_only(path: &PathSpec) -> bool {
    path.metadata.initial_srtt_ms.is_none()
        && path.metadata.initial_jitter_ms.is_none()
        && path.metadata.initial_rate == RateHint::Unknown
        && path.metadata.capabilities == crate::protocol::PathCapabilities::default()
}

fn configured_order_path_indices(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    class: TrafficClass,
    payload_bytes: usize,
) -> Vec<usize> {
    configured_order_path_scores(paths, observations, class, payload_bytes)
        .into_iter()
        .map(|(index, _)| index)
        .collect()
}

fn configured_order_path_scores(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    class: TrafficClass,
    payload_bytes: usize,
) -> Vec<(usize, f64)> {
    paths
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
                    active_latency_sensitive_flows: 0,
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

fn ordered_path_scores_for_ttl(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    class: TrafficClass,
    payload_bytes: usize,
    ttl_ms: u32,
) -> Vec<(usize, f64)> {
    let scores = ordered_path_scores(paths, observations, class, payload_bytes);
    let freshness_budget_ms = f64::from(ttl_ms) * UDP_DATAGRAM_MIN_TTL_FIT_RATIO;
    scores
        .iter()
        .copied()
        .filter(|(_, eta_ms)| *eta_ms <= freshness_budget_ms)
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
                    active_latency_sensitive_flows: 0,
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
    scores.sort_by(|left, right| {
        left.1
            .total_cmp(&right.1)
            .then_with(|| left.0.cmp(&right.0))
    });
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

fn udp_path_has_realtime_model(path: &PathSpec, observation: ClientPathObservation) -> bool {
    observation.measured_srtt_ms.is_some()
        || observation.measured_jitter_ms.is_some()
        || observation.measured_rate_bps.is_some()
        || observation.measured_loss_rate.is_some()
        || path.metadata.initial_srtt_ms.is_some()
        || path.metadata.initial_jitter_ms.is_some()
        || path.metadata.initial_rate != RateHint::Unknown
}

fn udp_observation_has_datagram_feedback(observation: &ClientPathObservation) -> bool {
    observation.measured_jitter_ms.is_some()
        || observation.measured_loss_rate.is_some()
        || observation.measured_rate_bps.is_some()
        || observation.measured_mtu_payload_bytes.is_some()
}

fn reliable_auto_bulk_discovery_scores(
    paths: &[PathSpec],
    observations: &[ClientPathObservation],
    scores: Vec<(usize, f64)>,
    current_path_index: Option<usize>,
    candidate_is_allowed: fn(&PathSpec, ClientPathObservation) -> bool,
) -> Vec<(usize, f64)> {
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
        .collect::<Vec<_>>();
    if !measured.is_empty() {
        return measured;
    }
    scores
        .into_iter()
        .filter(|(index, eta)| {
            let Some(path) = paths.get(*index) else {
                return false;
            };
            let observation = observations
                .get(*index)
                .copied()
                .unwrap_or(ClientPathObservation {
                    state: SchedulerPathState::Suspect,
                    measured_srtt_ms: None,
                    measured_jitter_ms: None,
                    measured_rate_bps: None,
                    measured_loss_rate: None,
                    measured_mtu_payload_bytes: None,
                    active_flows: 0,
                    active_latency_sensitive_flows: 0,
                    load_bytes: 0,
                });
            improves_current(*index, *eta) && candidate_is_allowed(path, observation)
        })
        .collect()
}

fn path_can_be_auto_discovered(path: &PathSpec, _observation: ClientPathObservation) -> bool {
    !path.metadata.capabilities.expensive
        && !path.metadata.capabilities.backup
        && !path.metadata.capabilities.probe_only
        && path.metadata.capabilities.bulk_allowed
}

fn udp_stream_path_can_be_auto_discovered(
    path: &PathSpec,
    observation: ClientPathObservation,
) -> bool {
    path_can_be_auto_discovered(path, observation)
        && (observation.measured_rate_bps.is_some()
            || path.metadata.initial_rate != RateHint::Unknown)
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
            let remote = match open_remote_stream(
                &context,
                target.clone(),
                IngressKind::Socks5,
                TrafficClass::Interactive,
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
    let remote = match open_remote_stream(
        &context,
        target.clone(),
        IngressKind::HttpConnect,
        TrafficClass::Interactive,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct RelayPathKey {
    underlay: UnderlayProtocol,
    index: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct RelayPathInstance {
    key: RelayPathKey,
    id: u64,
}

struct TcpRelayRemotePath {
    path_index: usize,
    instance_id: u64,
    stream: TcpPathStreamHandle,
}

impl TcpRelayRemotePath {
    fn key(&self) -> RelayPathKey {
        RelayPathKey {
            underlay: self.stream.underlay,
            index: self.path_index,
        }
    }

    fn instance(&self) -> RelayPathInstance {
        RelayPathInstance {
            key: self.key(),
            id: self.instance_id,
        }
    }
}

struct TcpRelayRemoteFrame {
    instance: RelayPathInstance,
    frame: Result<Frame, RuntimeError>,
}

struct TcpRelayRemoteSet {
    stream_id: StreamId,
    paths: Vec<TcpRelayRemotePath>,
    frames_tx: mpsc::Sender<TcpRelayRemoteFrame>,
    frames_rx: mpsc::Receiver<TcpRelayRemoteFrame>,
    next_send_index: usize,
    next_instance_id: u64,
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
            next_instance_id: 0,
        };
        set.attach(opened);
        set
    }

    fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    fn primary_path_key(&self) -> Option<RelayPathKey> {
        self.paths.first().map(|path| path.key())
    }

    fn active_path_instance(&self) -> Option<RelayPathInstance> {
        self.paths.last().map(TcpRelayRemotePath::instance)
    }

    fn active_path_index_for(&self, underlay: UnderlayProtocol) -> Option<usize> {
        self.paths
            .iter()
            .rev()
            .find(|path| path.stream.underlay == underlay)
            .map(|path| path.path_index)
    }

    fn active_carrier_underlay(&self) -> Option<UnderlayProtocol> {
        self.paths.last().map(|path| path.stream.underlay)
    }

    fn contains_path_key(&self, key: RelayPathKey) -> bool {
        self.paths.iter().any(|path| path.key() == key)
    }

    fn path_keys(&self) -> Vec<RelayPathKey> {
        self.paths.iter().map(TcpRelayRemotePath::key).collect()
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

    fn max_frame_payload_bytes(&self, mux_limits: MuxLimits) -> usize {
        self.paths
            .iter()
            .map(|path| path.stream.max_frame_payload_bytes)
            .min()
            .unwrap_or_else(|| tcp_relay_buffer_len(mux_limits))
            .max(1)
    }

    fn fin_requires_repair_drain(&self) -> bool {
        self.paths
            .iter()
            .any(|path| path.stream.underlay == UnderlayProtocol::Udp)
    }

    fn attach(&mut self, opened: OpenedRemoteStream) {
        let path_index = opened.path_index;
        let underlay = opened.stream.underlay;
        let key = RelayPathKey {
            underlay,
            index: path_index,
        };
        if self.contains_path_key(key) {
            return;
        }
        let instance_id = self.next_instance_id;
        self.next_instance_id = self.next_instance_id.wrapping_add(1);
        let instance = RelayPathInstance {
            key,
            id: instance_id,
        };
        let (stream, mut frames) = opened.stream.into_handle_and_frames();
        let frames_tx = self.frames_tx.clone();
        tokio::spawn(async move {
            while let Some(frame) = frames.recv().await {
                let done = frame.is_err();
                if frames_tx
                    .send(TcpRelayRemoteFrame { instance, frame })
                    .await
                    .is_err()
                    || done
                {
                    return;
                }
            }
            let _ = frames_tx
                .send(TcpRelayRemoteFrame {
                    instance,
                    frame: Err(RuntimeError::TcpPathSessionClosed),
                })
                .await;
        });
        self.paths.push(TcpRelayRemotePath {
            path_index,
            instance_id,
            stream,
        });
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
    ) -> Result<RelayPathKey, RuntimeError> {
        let mut last_error = None;
        let prefer_current_data_path = tcp_relay_frame_prefers_current_data_path(&frame);
        while !self.paths.is_empty() {
            if prefer_current_data_path
                || self
                    .paths
                    .last()
                    .is_some_and(|path| tcp_path_frame_uses_priority_queue(path.stream.class))
            {
                self.next_send_index = self.paths.len() - 1;
            }
            self.next_send_index %= self.paths.len();
            let instance = self.paths[self.next_send_index].instance();
            match self.paths[self.next_send_index]
                .stream
                .send_frame(frame.clone())
                .await
            {
                Ok(()) => {
                    if !prefer_current_data_path
                        && !tcp_path_frame_uses_priority_queue(
                            self.paths[self.next_send_index].stream.class,
                        )
                    {
                        self.next_send_index = (self.next_send_index + 1) % self.paths.len();
                    }
                    return Ok(instance.key);
                }
                Err(err) => {
                    last_error = Some(err);
                    self.fail_path_instance(context, instance).await;
                }
            }
        }
        Err(last_error.unwrap_or(RuntimeError::TcpPathSessionClosed))
    }

    async fn reannounce_active_path(
        &mut self,
        context: &ClientPathContext,
        spec: &TcpRelayOpenSpec,
        class: TrafficClass,
    ) -> Result<(), RuntimeError> {
        let Some(position) = self.paths.len().checked_sub(1) else {
            return Err(RuntimeError::TcpPathSessionClosed);
        };
        let instance = self.paths[position].instance();
        let output = self.paths[position].stream.output.clone();
        self.paths[position].stream.class = class;
        let frame = Frame::OpenStream {
            stream_id: self.stream_id,
            target: spec.target.clone(),
            ingress: spec.ingress,
            outbound: OutboundPolicy::Direct,
            class,
        };
        match output
            .send_frame(self.stream_id, TrafficClass::Control, frame)
            .await
        {
            Ok(()) => Ok(()),
            Err(err) => {
                self.fail_path_instance(context, instance).await;
                Err(err)
            }
        }
    }

    async fn close_all(&mut self) {
        let paths = std::mem::take(&mut self.paths);
        for path in paths {
            path.stream.close().await;
        }
        self.next_send_index = 0;
    }

    async fn fail_path_instance(
        &mut self,
        context: &ClientPathContext,
        instance: RelayPathInstance,
    ) -> bool {
        let Some(path) = self.remove_path_instance(instance) else {
            return false;
        };
        context.mark_relay_path_failure(path.stream.underlay, path.path_index);
        context.release_relay_path_load(path.stream.underlay, path.path_index, path.stream.class);
        path.stream.close().await;
        true
    }

    async fn fail_path_key(&mut self, context: &ClientPathContext, key: RelayPathKey) -> bool {
        let Some(path) = self.remove_path_key(key) else {
            return false;
        };
        context.mark_relay_path_failure(path.stream.underlay, path.path_index);
        context.release_relay_path_load(path.stream.underlay, path.path_index, path.stream.class);
        path.stream.close().await;
        true
    }

    fn remove_path_instance(&mut self, instance: RelayPathInstance) -> Option<TcpRelayRemotePath> {
        let position = self
            .paths
            .iter()
            .position(|path| path.instance() == instance)?;
        self.remove_path_at(position)
    }

    fn remove_path_key(&mut self, key: RelayPathKey) -> Option<TcpRelayRemotePath> {
        let position = self.paths.iter().position(|path| path.key() == key)?;
        self.remove_path_at(position)
    }

    fn remove_path_at(&mut self, position: usize) -> Option<TcpRelayRemotePath> {
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
        return open_remote_stream_with_id_over_udp(context, stream_id, target, ingress, class)
            .await;
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
    context.mark_tcp_path_open_success(path_index, started_at.elapsed(), class);
    Ok(OpenedRemoteStream { stream, path_index })
}

async fn open_remote_stream_with_id_over_udp(
    context: &ClientPathContext,
    stream_id: StreamId,
    target: TargetAddr,
    ingress: IngressKind,
    class: TrafficClass,
) -> Result<OpenedRemoteStream, RuntimeError> {
    if context.udp_paths.is_empty() {
        return Err(RuntimeError::NoTcpPath);
    }
    let candidates = context.ordered_udp_stream_path_indices(class, PATH_OPEN_SCORE_BYTES);
    if candidates.is_empty() {
        return Err(RuntimeError::NoSchedulableUdpPath);
    }
    let mut last_retryable_error = None;
    for path_index in candidates {
        match open_remote_stream_on_udp_path(
            context,
            stream_id,
            target.clone(),
            ingress,
            class,
            path_index,
            true,
        )
        .await
        {
            Ok(opened) => return Ok(opened),
            Err(err) if udp_stream_open_error_is_path_retryable(&err) => {
                context.mark_udp_path_failure(path_index);
                last_retryable_error = Some(err);
            }
            Err(err) => return Err(err),
        }
    }
    Err(last_retryable_error.unwrap_or(RuntimeError::NoSchedulableUdpPath))
}

async fn open_remote_stream_on_udp_path(
    context: &ClientPathContext,
    stream_id: StreamId,
    target: TargetAddr,
    ingress: IngressKind,
    class: TrafficClass,
    path_index: usize,
    wait_for_accept: bool,
) -> Result<OpenedRemoteStream, RuntimeError> {
    let path = context
        .udp_paths
        .get(path_index)
        .ok_or(RuntimeError::NoSchedulableUdpPath)?;
    let started_at = Instant::now();
    let socket = udp::connect_path(
        path,
        crate::transport::udp::UdpConnectOptions {
            timeout: UDP_PATH_HANDSHAKE_TIMEOUT,
            ..crate::transport::udp::UdpConnectOptions::default()
        },
    )
    .await?;
    let mut encrypted = EncryptedUdpSocket::new(
        socket,
        context.security.secret.as_bytes(),
        PeerRole::Client,
        context.codec_limits,
    );
    let path_id = PathId(path_index as u16);
    let handshake_frames = {
        let (session_hello, session_auth, path_join) = authenticated_path_join_frames_for_session(
            &context.security,
            path,
            path_id,
            UnderlayProtocol::Udp,
            context.udp_stream_session_id,
        )?;
        [session_hello, session_auth, path_join]
    };

    for frame in &handshake_frames {
        encrypted.send_frame(frame).await?;
    }

    let mut buffer = vec![0u8; encrypted.max_datagram_bytes()?];
    let control_retry_interval = udp_stream_control_retry_interval(context, path_index);
    let handshake_started_at = Instant::now();
    let mut session_ready = false;
    let mut path_active = false;
    while !session_ready || !path_active {
        let elapsed = handshake_started_at.elapsed();
        if elapsed >= UDP_PATH_HANDSHAKE_TIMEOUT {
            return Err(RuntimeError::Protocol(
                "UDP stream path handshake timed out",
            ));
        }
        let remaining = UDP_PATH_HANDSHAKE_TIMEOUT.saturating_sub(elapsed);
        match tokio::time::timeout(
            control_retry_interval.min(remaining),
            encrypted.recv_frame(&mut buffer),
        )
        .await
        {
            Err(_) => {
                for frame in &handshake_frames {
                    encrypted.send_frame(frame).await?;
                }
                continue;
            }
            Ok(Err(err)) if encrypted_udp_error_is_ignorable(&err) => continue,
            Ok(Err(err)) => return Err(RuntimeError::EncryptedUdp(err)),
            Ok(Ok(Frame::SessionReady)) => session_ready = true,
            Ok(Ok(Frame::PathStatus {
                status: crate::protocol::PathStatus::Active,
                ..
            })) => path_active = true,
            Ok(Ok(Frame::PathStatus { .. })) => {
                return Err(RuntimeError::Protocol(
                    "UDP stream path did not become active",
                ));
            }
            Ok(Ok(Frame::SessionClose { reason })) => {
                return Err(RuntimeError::RemoteClosed(reason));
            }
            Ok(Ok(_)) => {
                return Err(RuntimeError::Protocol(
                    "unexpected UDP stream handshake frame",
                ));
            }
        }
    }

    let open_frame = Frame::OpenStream {
        stream_id,
        target,
        ingress,
        outbound: OutboundPolicy::Direct,
        class,
    };
    encrypted.send_frame(&open_frame).await?;

    let open_started_at = Instant::now();
    let open_retry_interval = control_retry_interval;
    let mut pending_open_retry = None;
    let max_offset = if wait_for_accept {
        loop {
            let elapsed = open_started_at.elapsed();
            if elapsed >= UDP_PATH_HANDSHAKE_TIMEOUT {
                return Err(RuntimeError::Protocol("UDP stream open timed out"));
            }
            let remaining = UDP_PATH_HANDSHAKE_TIMEOUT.saturating_sub(elapsed);
            match tokio::time::timeout(
                open_retry_interval.min(remaining),
                encrypted.recv_frame(&mut buffer),
            )
            .await
            {
                Err(_) => {
                    encrypted.send_frame(&open_frame).await?;
                    continue;
                }
                Ok(Err(err)) if encrypted_udp_error_is_ignorable(&err) => continue,
                Ok(Err(err)) => return Err(RuntimeError::EncryptedUdp(err)),
                Ok(Ok(frame)) => match frame {
                    Frame::StreamMaxData {
                        stream_id: max_stream_id,
                        max_offset,
                    } if max_stream_id == stream_id => break max_offset,
                    Frame::StreamReset {
                        stream_id: reset_stream_id,
                        reason,
                    } if reset_stream_id == stream_id => {
                        return Err(RuntimeError::RemoteReset(reason));
                    }
                    Frame::SessionClose { reason } => {
                        return Err(RuntimeError::RemoteClosed(reason));
                    }
                    Frame::PathStatus { .. } => {}
                    _ => return Err(RuntimeError::Protocol("unexpected UDP stream open frame")),
                },
            }
        }
    } else {
        pending_open_retry = Some((open_frame.clone(), open_retry_interval));
        context.mux_limits.max_stream_window_bytes
    };

    let (commands, receivers) =
        tcp_path_session_command_channels(udp_stream_path_command_queue(context.mux_limits));
    let (frames_tx, frames_rx) = mpsc::channel(tcp_stream_frame_queue(context.mux_limits));
    tokio::spawn(run_client_udp_stream_path_session(
        encrypted,
        buffer,
        stream_id,
        path_id,
        receivers,
        frames_tx,
        pending_open_retry,
    ));
    context.mark_udp_stream_path_open_success(path_index, started_at.elapsed(), class);
    Ok(OpenedRemoteStream {
        stream: TcpPathStream {
            stream_id,
            max_offset,
            class,
            underlay: UnderlayProtocol::Udp,
            max_frame_payload_bytes: udp_stream_frame_payload_bytes(context.mux_limits),
            output: TcpPathStreamOutput::Fixed(commands),
            frames: frames_rx,
        },
        path_index,
    })
}

async fn run_client_udp_stream_path_session(
    mut encrypted: EncryptedUdpSocket,
    mut buffer: Vec<u8>,
    stream_id: StreamId,
    _path_id: PathId,
    mut commands: TcpPathSessionCommandReceivers,
    frames: mpsc::Sender<Result<Frame, RuntimeError>>,
    pending_open_retry: Option<(Frame, Duration)>,
) {
    let mut pending_open_retry = pending_open_retry
        .map(|(frame, interval)| (frame, interval, tokio::time::Instant::now() + interval));
    loop {
        let command_may_recv = !tcp_path_receivers_closed(&commands);
        if !command_may_recv {
            let _ = encrypted
                .send_frame(&Frame::SessionClose {
                    reason: CloseReason::Normal,
                })
                .await;
            return;
        }
        tokio::select! {
            biased;
            _ = async {
                if let Some((_, _, deadline)) = &pending_open_retry {
                    tokio::time::sleep_until(*deadline).await;
                }
            }, if pending_open_retry.is_some() => {
                if let Some((frame, interval, deadline)) = &mut pending_open_retry
                    && tokio::time::Instant::now() >= *deadline
                {
                    if let Err(err) = encrypted.send_frame(frame).await {
                        let _ = frames.send(Err(RuntimeError::EncryptedUdp(err))).await;
                        return;
                    }
                    *deadline = tokio::time::Instant::now() + *interval;
                }
            }
            frame = encrypted.recv_frame(&mut buffer) => {
                match frame {
                    Ok(Frame::Ping { nonce }) => {
                        if let Err(err) = encrypted.send_frame(&Frame::Pong { nonce }).await {
                            let _ = frames.send(Err(RuntimeError::EncryptedUdp(err))).await;
                            return;
                        }
                    }
                    Ok(frame @ (Frame::StreamData { stream_id: received_stream_id, .. }
                        | Frame::StreamAck { stream_id: received_stream_id, .. }
                        | Frame::StreamMaxData { stream_id: received_stream_id, .. }
                        | Frame::StreamFin { stream_id: received_stream_id }
                        | Frame::StreamReset { stream_id: received_stream_id, .. }))
                        if received_stream_id == stream_id =>
                    {
                        pending_open_retry = None;
                        if frames.send(Ok(frame)).await.is_err() {
                            return;
                        }
                    }
                    Ok(frame @ Frame::PathStatus { .. }) => {
                        if frames.send(Ok(frame)).await.is_err() {
                            return;
                        }
                    }
                    Ok(Frame::SessionClose { reason }) => {
                        let _ = frames.send(Err(RuntimeError::RemoteClosed(reason))).await;
                        return;
                    }
                    Ok(_) => {
                        let _ = frames
                            .send(Err(RuntimeError::Protocol(
                                "unexpected UDP reliable stream frame",
                            )))
                            .await;
                        return;
                    }
                    Err(err) if encrypted_udp_error_is_ignorable(&err) => {}
                    Err(err) => {
                        let _ = frames.send(Err(RuntimeError::EncryptedUdp(err))).await;
                        return;
                    }
                }
            }
            command = recv_tcp_path_command(&mut commands), if command_may_recv => {
                match command {
                    Some(TcpPathSessionCommand::SendFrame(frame)) => {
                        if let Err(err) = encrypted.send_frame(&frame).await {
                            let _ = frames.send(Err(RuntimeError::EncryptedUdp(err))).await;
                            return;
                        }
                    }
                    Some(TcpPathSessionCommand::CloseStream(close_stream_id)) => {
                        if close_stream_id == stream_id {
                            let _ = encrypted
                                .send_frame(&Frame::SessionClose {
                                    reason: CloseReason::Normal,
                                })
                                .await;
                            return;
                        }
                    }
                    Some(TcpPathSessionCommand::OpenStream { .. }) => {
                        let _ = frames
                            .send(Err(RuntimeError::Protocol(
                                "client UDP stream path received open command",
                            )))
                            .await;
                        return;
                    }
                    None => {}
                }
            }
        }
    }
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

fn udp_stream_open_error_is_path_retryable(err: &RuntimeError) -> bool {
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

fn udp_stream_control_retry_interval(context: &ClientPathContext, path_index: usize) -> Duration {
    let max_retry = UDP_PATH_HANDSHAKE_TIMEOUT.mul_f64(0.5);
    let Some(snapshot) = context.udp_path_snapshot(path_index) else {
        return UDP_MIN_PATH_SUPPRESSION.min(max_retry);
    };
    let modeled_ms = snapshot.srtt_ms.max(1.0) * 2.0 + snapshot.jitter_ms.max(0.0) * 4.0 + 10.0;
    Duration::from_secs_f64(modeled_ms / 1000.0)
        .max(UDP_MIN_RESPONSE_TIMEOUT)
        .min(max_retry)
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
    let (completion_tx, mut completion_rx) =
        mpsc::channel::<UdpEdgeCompletion<SocketAddr>>(udp_edge_completion_queue(&context));
    let mut lanes = Vec::<UdpEdgeLane<SocketAddr>>::new();
    let mut next_lane_id = 0usize;
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
                if dispatch_udp_edge_request(
                    &mut lanes,
                    &mut next_lane_id,
                    &context,
                    &completion_tx,
                    UdpEdgeRequest {
                        target,
                        payload: datagram.payload,
                        ttl_ms: DEFAULT_SOCKS5_UDP_TTL_MS,
                        metadata: peer,
                    },
                )
                .is_err()
                {
                    eprintln!("warning: SOCKS5 UDP lane queue full; dropping datagram from {peer}");
                }
            }
            completion = completion_rx.recv() => {
                let Some(completion) = completion else {
                    break Err(RuntimeError::Protocol("SOCKS5 UDP completion channel closed"));
                };
                finish_udp_edge_completion(&mut lanes, &completion);
                match completion.result {
                    Ok(response) => {
                        let response_packet = match socks5::udp_datagram(&completion.target, &response) {
                            Ok(packet) => packet,
                            Err(err) => break Err(RuntimeError::Socks5(err)),
                        };
                        if let Err(err) = relay_socket.send_to(&response_packet, completion.metadata).await {
                            break Err(RuntimeError::Io(err));
                        }
                    }
                    Err(err) => {
                        eprintln!(
                            "warning: SOCKS5 UDP datagram to {:?} failed: {err}",
                            completion.target
                        );
                    }
                }
            }
        }
    };
    drop(completion_tx);
    close_udp_edge_lanes(lanes).await;
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
    handshake_timeout: Duration,
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
        handshake_timeout,
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
        tcp_path_session_command_channels(tcp_server_session_command_queue(&context));
    let mut attached_streams = HashSet::new();
    let mut draining = false;

    loop {
        let Some(event) = recv_server_tcp_path_event(&mut path_frames, &mut commands_rx).await?
        else {
            return Ok(());
        };
        match event {
            ServerTcpPathEvent::Command(command) => {
                if !handle_server_tcp_path_command(
                    command,
                    &mut writer,
                    &context,
                    &mut attached_streams,
                    ServerTcpPathCommandContext {
                        session_id,
                        path_id,
                        commands_tx: &commands_tx,
                        draining,
                    },
                )
                .await?
                {
                    return Ok(());
                }
            }
            ServerTcpPathEvent::Frame(frame) => match frame {
                Frame::OpenStream {
                    stream_id,
                    target,
                    class,
                    ..
                } if !draining => {
                    outbound::validate_target(&target)?;
                    context.outbound.ensure_supports(TargetProtocol::Tcp)?;
                    match context.tcp_streams.open_or_attach(
                        ServerTcpStreamOpenRequest {
                            session_id,
                            stream_id,
                            target: &target,
                            class,
                            attachment: ServerTcpPathAttachment {
                                path_id,
                                underlay: UnderlayProtocol::Tcp,
                                commands: commands_tx.clone(),
                                max_frame_payload_bytes: tcp_relay_buffer_len(context.mux_limits),
                            },
                        },
                        context.mux_limits,
                        context.max_tcp_streams,
                    )? {
                        ServerTcpStreamOpen::New(stream) => {
                            attached_streams.insert(stream_id);
                            let stream_context = context.clone();
                            tokio::spawn(async move {
                                if let Err(err) = run_server_tcp_stream(
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
                Frame::StreamData {
                    stream_id,
                    offset,
                    flags,
                    payload,
                } => {
                    context
                        .tcp_streams
                        .route_frame(
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
                    context
                        .tcp_streams
                        .route_frame(
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
                    context
                        .tcp_streams
                        .route_frame(
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
                    context
                        .tcp_streams
                        .route_frame(session_id, stream_id, Frame::StreamFin { stream_id })
                        .await?;
                }
                Frame::StreamDetach { stream_id } => {
                    attached_streams.remove(&stream_id);
                    context.tcp_streams.detach_path(
                        session_id,
                        stream_id,
                        UnderlayProtocol::Tcp,
                        path_id,
                        &commands_tx,
                    );
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
                    context
                        .tcp_streams
                        .route_frame(
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
                Frame::PathDrain {
                    path_id: drain_path_id,
                } if drain_path_id == path_id => {
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
            },
        }
    }
}

struct ServerTcpPathCommandContext<'a> {
    session_id: SessionId,
    path_id: PathId,
    commands_tx: &'a TcpPathSessionCommandSender,
    draining: bool,
}

async fn handle_server_tcp_path_command(
    command: TcpPathSessionCommand,
    writer: &mut EncryptedTcpWriter,
    context: &ServerPathContext,
    attached_streams: &mut HashSet<StreamId>,
    command_context: ServerTcpPathCommandContext<'_>,
) -> Result<bool, RuntimeError> {
    match command {
        TcpPathSessionCommand::SendFrame(frame) => {
            server_write_tcp_path_frame(writer, &frame).await
        }
        TcpPathSessionCommand::CloseStream(stream_id) => {
            attached_streams.remove(&stream_id);
            context.tcp_streams.detach_path(
                command_context.session_id,
                stream_id,
                UnderlayProtocol::Tcp,
                command_context.path_id,
                command_context.commands_tx,
            );
            if command_context.draining && attached_streams.is_empty() {
                let _ = server_write_tcp_path_frame(
                    writer,
                    &Frame::PathClose {
                        path_id: command_context.path_id,
                        reason: CloseReason::Normal,
                    },
                )
                .await?;
                return Ok(false);
            }
            Ok(true)
        }
        TcpPathSessionCommand::OpenStream { .. } => Err(RuntimeError::Protocol(
            "server TCP path received client open command",
        )),
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
    tcp_path_command_queue(context.mux_limits)
}

#[derive(Debug, Clone, Copy)]
struct TcpRelayClassState {
    current: TrafficClass,
    rebalance_attempted: bool,
}

impl TcpRelayClassState {
    fn new() -> Self {
        Self {
            current: TrafficClass::Interactive,
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
        update.promoted_to_bulk && !self.rebalance_attempted
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
    let ramp_floor = relay_chunk.saturating_mul(2).min(window);
    let ramp_bdp = bdp_bytes.saturating_div(8).max(relay_chunk).max(ramp_floor);
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
    let initial_key = RelayPathKey {
        underlay: remote.stream.underlay,
        index: remote.path_index,
    };
    let mut remotes = TcpRelayRemoteSet::new(remote, tcp_stream_frame_queue(context.mux_limits));
    let stream_id = remotes.stream_id();
    let mut send_stream = ReliableSendStream::new(stream_id, context.mux_limits);
    send_stream.update_max_offset(remotes.max_offset());
    let mut recv_stream = ReliableRecvStream::new(stream_id, context.mux_limits);
    let chunk_size = tcp_relay_buffer_len(context.mux_limits);
    let mut buf = vec![0u8; chunk_size];
    let mut local_open = true;
    let mut remote_open = true;
    let mut pending_local_fin = false;
    let mut pending_remote_fin = false;
    let mut stats = PathDeliveryStats::default();
    let mut path_stats = HashMap::<RelayPathKey, PathDeliveryStats>::new();
    let mut class_state = TcpRelayClassState::new();
    let mut last_stream_progress_at = Instant::now();
    let mut last_delivery_progress_at = Instant::now();
    let mut last_response_stall_repair_at = Instant::now();
    let mut response_stall_reannounce_attempts = 0_u32;
    let mut last_receive_hole_repair_at = Instant::now();
    let mut receive_hole_repair_attempts = 0_u32;
    let mut path_last_delivery_at = HashMap::from([(initial_key, Instant::now())]);
    let mut interactive_response_pending = false;
    let mut recv_progress = ReliableRecvProgress::default();
    let mut last_recv_progress_sent_at = Instant::now();

    let result = loop {
        if !local_open && !remote_open && send_stream.repair_bytes() == 0 {
            break Ok(stats);
        }
        let path_snapshot = remotes
            .primary_path_key()
            .and_then(|key| relay_path_snapshot(context, key));
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
            adaptive_tcp_relay_chunk_bytes(path_snapshot, relay_class, context.mux_limits)
                .min(remotes.max_frame_payload_bytes(context.mux_limits));
        let adaptive_inflight =
            adaptive_tcp_relay_inflight_bytes(path_snapshot, relay_class, context.mux_limits);
        let stall_watch_active = tcp_relay_stall_watch_active(
            &send_stream,
            &recv_stream,
            remote_open,
            relay_class,
            interactive_response_pending,
            context.mux_limits,
        );
        let stall_progress_anchor = tcp_relay_stall_progress_anchor(
            last_stream_progress_at,
            last_delivery_progress_at,
            last_response_stall_repair_at,
            &recv_stream,
            remote_open,
            relay_class,
            context.mux_limits,
        );
        let receive_hole_repair_active =
            tcp_relay_receive_hole_repair_active(&recv_stream, remote_open);
        let receive_hole_repair_deadline = tcp_relay_receive_hole_repair_deadline(
            last_delivery_progress_at,
            last_receive_hole_repair_at,
            path_snapshot,
            relay_class,
        );
        let stall_deadline =
            tcp_relay_stall_deadline(stall_progress_anchor, path_snapshot, relay_class);
        let recv_progress_deadline = tokio::time::Instant::from_std(
            last_recv_progress_sent_at
                + reliable_stream_recv_progress_interval(path_snapshot, relay_class),
        );

        tokio::select! {
            _ = tokio::time::sleep_until(receive_hole_repair_deadline), if receive_hole_repair_active => {
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
                        last_receive_hole_repair_at = Instant::now();
                        receive_hole_repair_attempts = 0;
                        tcp_relay_refresh_path_tracking(
                            &mut path_last_delivery_at,
                            &remotes.path_keys(),
                            Instant::now(),
                        );
                        continue;
                    }
                    Ok(_) => {
                        receive_hole_repair_attempts =
                            receive_hole_repair_attempts.saturating_add(1);
                        if receive_hole_repair_attempts >= tcp_relay_receive_hole_failure_attempts(relay_class) {
                            let path_keys = remotes.path_keys();
                            if let Some(path_key) = tcp_relay_receive_hole_victim(
                                context,
                                &path_keys,
                                relay_class,
                                recv_stream.reorder_bytes().max(1),
                                &path_last_delivery_at,
                            ) && remotes.fail_path_key(context, path_key).await
                            {
                                path_last_delivery_at.remove(&path_key);
                                if !remotes.is_empty()
                                    && let Err(err) = remotes
                                        .reannounce_active_path(context, &spec, relay_class)
                                        .await
                                {
                                    eprintln!(
                                        "warning: TCP receive-hole survivor reannounce failed: {err}"
                                    );
                                }
                                send_stream.update_max_offset(remotes.max_offset());
                                last_stream_progress_at = Instant::now();
                                last_receive_hole_repair_at = Instant::now();
                                receive_hole_repair_attempts = 0;
                                continue;
                            }
                            if !remotes.is_empty()
                                && let Err(err) = remotes
                                    .reannounce_active_path(context, &spec, relay_class)
                                    .await
                            {
                                eprintln!(
                                    "warning: TCP receive-hole sole-survivor reannounce failed: {err}"
                                );
                            }
                        }
                        last_receive_hole_repair_at = Instant::now();
                    }
                    Err(err) if remotes.is_empty() => break Err(err),
                    Err(err) => {
                        eprintln!("warning: TCP receive-hole repair failed: {err}");
                        last_receive_hole_repair_at = Instant::now();
                    }
                }
            }
            _ = tokio::time::sleep_until(stall_deadline), if stall_watch_active => {
                if remotes.path_keys().len() <= 1 {
                    let reannounce_budget = tcp_relay_sole_survivor_reannounce_attempts(
                        tcp_relay_stall_timeout(path_snapshot, relay_class),
                    );
                    if response_stall_reannounce_attempts
                        < reannounce_budget
                    {
                        response_stall_reannounce_attempts =
                            response_stall_reannounce_attempts.saturating_add(1);
                        match remotes
                            .reannounce_active_path(context, &spec, relay_class)
                            .await
                        {
                            Ok(()) => {
                                send_stream.update_max_offset(remotes.max_offset());
                                last_stream_progress_at = Instant::now();
                                last_response_stall_repair_at = Instant::now();
                                tcp_relay_refresh_path_tracking(
                                    &mut path_last_delivery_at,
                                    &remotes.path_keys(),
                                    Instant::now(),
                                );
                                continue;
                            }
                            Err(err) => {
                                eprintln!(
                                    "warning: TCP stall sole-survivor reannounce failed: {err}"
                                );
                            }
                        }
                    } else {
                        response_stall_reannounce_attempts = 0;
                    }
                }
                if let Some(instance) = remotes.active_path_instance() {
                    remotes.fail_path_instance(context, instance).await;
                }
                if !remotes.is_empty() {
                    match remotes
                        .reannounce_active_path(context, &spec, relay_class)
                        .await
                    {
                        Ok(()) => {
                            send_stream.update_max_offset(remotes.max_offset());
                            last_stream_progress_at = Instant::now();
                            last_response_stall_repair_at = Instant::now();
                            tcp_relay_refresh_path_tracking(
                                &mut path_last_delivery_at,
                                &remotes.path_keys(),
                                Instant::now(),
                            );
                            continue;
                        }
                        Err(err) => {
                            eprintln!("warning: TCP stall survivor reannounce failed: {err}");
                        }
                    }
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
                            last_response_stall_repair_at = Instant::now();
                            tcp_relay_refresh_path_tracking(
                                &mut path_last_delivery_at,
                                &remotes.path_keys(),
                                Instant::now(),
                            );
                            continue;
                        }
                    Ok(_) => {
                        last_stream_progress_at = Instant::now();
                        last_response_stall_repair_at = Instant::now();
                    }
                    Err(err) if remotes.is_empty() => break Err(err),
                    Err(err) => {
                        eprintln!("warning: TCP stream stall repair failed: {err}");
                        last_stream_progress_at = Instant::now();
                        last_response_stall_repair_at = Instant::now();
                    }
                }
            }
            _ = tokio::time::sleep_until(recv_progress_deadline), if remotes.path_keys().len() > 1
                && tcp_relay_recv_progress_resend_active(&recv_stream, remote_open) => {
                match send_tcp_recv_progress_remote_set(
                    &mut remotes,
                    context,
                    &recv_stream,
                    &mut recv_progress,
                    true,
                )
                .await
                {
                    Ok(()) => {
                        last_stream_progress_at = Instant::now();
                        last_recv_progress_sent_at = Instant::now();
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
                                send_stream.update_max_offset(remotes.max_offset());
                                last_stream_progress_at = Instant::now();
                                last_recv_progress_sent_at = Instant::now();
                            }
                            Ok(_) => break Err(err),
                            Err(err) => break Err(err),
                        }
                    }
                    Err(err) => break Err(err),
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
                    if remotes.fin_requires_repair_drain() && send_stream.repair_bytes() > 0 {
                        pending_local_fin = true;
                    } else {
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
                    }
                } else {
                    if tcp_relay_expects_interactive_response(relay_class) && remote_open {
                        interactive_response_pending = true;
                    }
                    let frame = match send_stream.send_data(
                        Bytes::copy_from_slice(&buf[..read]),
                        StreamFlags::NONE,
                    ) {
                        Ok(frame) => frame,
                        Err(err) => break Err(RuntimeError::Stream(err)),
                    };
                    match remotes.send_frame(context, frame).await {
                        Ok(path_key) => {
                            last_stream_progress_at = Instant::now();
                            stats.record_payload_bytes(read);
                            path_stats
                                .entry(path_key)
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
                let TcpRelayRemoteFrame { instance, frame } = match frame {
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
                let path_key = instance.key;
                let frame = match frame {
                    Ok(frame) => frame,
                    Err(err) if tcp_relay_error_is_migratable(&err) => {
                        remotes.fail_path_instance(context, instance).await;
                        if !remotes.is_empty()
                            && let Err(err) = remotes
                                .reannounce_active_path(context, &spec, relay_class)
                                .await
                        {
                            eprintln!("warning: TCP path-error survivor reannounce failed: {err}");
                        } else if !remotes.is_empty() {
                            send_stream.update_max_offset(remotes.max_offset());
                            last_stream_progress_at = Instant::now();
                            last_response_stall_repair_at = Instant::now();
                            tcp_relay_refresh_path_tracking(
                                &mut path_last_delivery_at,
                                &remotes.path_keys(),
                                Instant::now(),
                            );
                        }
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
                                    tcp_relay_refresh_path_tracking(
                                        &mut path_last_delivery_at,
                                        &remotes.path_keys(),
                                        Instant::now(),
                                    );
                                    continue;
                                }
                                Ok(_) => break Err(err),
                                Err(_) => break Err(err),
                            }
                        }
                        path_last_delivery_at.remove(&path_key);
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
                        let previous_remote_offset = recv_stream.next_offset();
                        let outcome = match recv_stream.receive_data(offset, payload, flags) {
                            Ok(outcome) => outcome,
                            Err(err) => break Err(RuntimeError::Stream(err)),
                        };
                        last_stream_progress_at = Instant::now();
                        let delivered_progress = recv_stream.next_offset() > previous_remote_offset;
                        if delivered_progress {
                            last_delivery_progress_at = Instant::now();
                            receive_hole_repair_attempts = 0;
                            response_stall_reannounce_attempts = 0;
                            path_last_delivery_at.insert(path_key, Instant::now());
                        }
                        let mut write_error = None;
                        for chunk in outcome.delivered {
                            stats.record_payload_bytes(chunk.len());
                            path_stats
                                .entry(path_key)
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
                        if delivered_progress {
                            interactive_response_pending = false;
                        }
                        match send_tcp_recv_progress_remote_set(
                            &mut remotes,
                            context,
                            &recv_stream,
                            &mut recv_progress,
                            false,
                        )
                        .await
                        {
                            Ok(()) => {
                                last_recv_progress_sent_at = Instant::now();
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
                            interactive_response_pending = false;
                        }
                    }
                    Frame::StreamAck {
                        stream_id: ack_stream_id,
                        ranges,
                    } if ack_stream_id == stream_id => {
                        send_stream.apply_ack(&ranges);
                        last_stream_progress_at = Instant::now();
                        if pending_local_fin && send_stream.repair_bytes() == 0 {
                            match remotes
                                .send_frame(context, Frame::StreamFin { stream_id })
                                .await
                            {
                                Ok(_) => {
                                    pending_local_fin = false;
                                    last_stream_progress_at = Instant::now();
                                }
                                Err(err) if tcp_relay_error_is_migratable(&err) => {
                                    match attach_tcp_relay_paths(
                                        context,
                                        &spec,
                                        relay_class,
                                        &mut remotes,
                                        &send_stream,
                                        true,
                                        TcpRelayAttachMode::Any,
                                    )
                                    .await
                                    {
                                        Ok(attached) if attached > 0 => {
                                            pending_local_fin = false;
                                            last_stream_progress_at = Instant::now();
                                        }
                                        Ok(_) => break Err(err),
                                        Err(err) => break Err(err),
                                    }
                                }
                                Err(err) => break Err(err),
                            }
                        }
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
                            last_delivery_progress_at = Instant::now();
                            if let Err(err) = local.shutdown().await {
                                break Err(RuntimeError::Io(err));
                            }
                            remote_open = false;
                            interactive_response_pending = false;
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

    let remaining_paths = remotes
        .paths
        .iter()
        .map(|path| (path.key(), path.stream.class))
        .collect::<Vec<_>>();
    if result.is_ok() {
        for (key, stats) in path_stats {
            context.mark_relay_path_delivery(key.underlay, key.index, stats);
        }
    }
    if result.is_ok() {
        remotes.close_all().await;
    }
    for (key, class) in remaining_paths {
        if relay_error_is_tcp_path_failure(&result) {
            context.mark_relay_path_failure(key.underlay, key.index);
        }
        context.release_relay_path_load(key.underlay, key.index, class);
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
    let attached =
        attach_tcp_relay_paths(context, spec, class, remotes, send_stream, resend_fin, mode)
            .await?;
    if attached == 0 {
        return Ok(false);
    }
    Ok(true)
}

fn tcp_relay_frame_prefers_current_data_path(frame: &Frame) -> bool {
    matches!(frame, Frame::StreamData { .. } | Frame::StreamFin { .. })
}

struct RelayPathAttachRequest<'a> {
    spec: &'a TcpRelayOpenSpec,
    class: TrafficClass,
    send_stream: &'a ReliableSendStream,
    resend_fin: bool,
    candidates: Vec<RelayPathKey>,
    race_repair: bool,
    allow_mixed_carrier: bool,
}

async fn attach_relay_path_candidates(
    context: &ClientPathContext,
    remotes: &mut TcpRelayRemoteSet,
    request: RelayPathAttachRequest<'_>,
) -> Result<usize, RuntimeError> {
    let stream_id = remotes.stream_id();
    let mut last_retryable_error = None;
    let mut attached = 0usize;
    let active_underlay = remotes.active_carrier_underlay();
    let candidates = if request.allow_mixed_carrier {
        request.candidates
    } else {
        relay_path_candidates_for_active_carrier(request.candidates, active_underlay)
    };

    for key in candidates {
        if remotes.contains_path_key(key) {
            continue;
        }
        match open_remote_stream_for_relay_path(
            context,
            stream_id,
            request.spec.target.clone(),
            request.spec.ingress,
            request.class,
            key,
        )
        .await
        {
            Ok(opened) => {
                match replay_tcp_repair_cache(
                    &opened.stream,
                    request.send_stream,
                    request.resend_fin,
                )
                .await
                {
                    Ok(()) => {
                        remotes.attach(opened);
                        attached += 1;
                        if !request.race_repair {
                            return Ok(attached);
                        }
                    }
                    Err(err) if tcp_relay_error_is_migratable(&err) => {
                        context.mark_relay_path_failure(key.underlay, key.index);
                        context.release_relay_path_load(key.underlay, key.index, request.class);
                        last_retryable_error = Some(err);
                    }
                    Err(err) => {
                        context.release_relay_path_load(key.underlay, key.index, request.class);
                        return Err(err);
                    }
                }
            }
            Err(err) if relay_path_open_error_is_retryable(key.underlay, &err) => {
                context.mark_relay_path_failure(key.underlay, key.index);
                last_retryable_error = Some(err);
            }
            Err(err) => return Err(err),
        }
    }
    if attached > 0 {
        Ok(attached)
    } else if remotes.is_empty() {
        Err(last_retryable_error.unwrap_or_else(|| no_schedulable_reliable_path_error(context)))
    } else {
        Ok(0)
    }
}

async fn open_remote_stream_for_relay_path(
    context: &ClientPathContext,
    stream_id: StreamId,
    target: TargetAddr,
    ingress: IngressKind,
    class: TrafficClass,
    key: RelayPathKey,
) -> Result<OpenedRemoteStream, RuntimeError> {
    match key.underlay {
        UnderlayProtocol::Tcp => {
            open_remote_stream_on_path(context, stream_id, target, ingress, class, key.index).await
        }
        UnderlayProtocol::Udp => {
            open_remote_stream_on_udp_path(
                context, stream_id, target, ingress, class, key.index, false,
            )
            .await
        }
    }
}

fn relay_path_open_error_is_retryable(underlay: UnderlayProtocol, err: &RuntimeError) -> bool {
    match underlay {
        UnderlayProtocol::Tcp => stream_open_error_is_path_retryable(err),
        UnderlayProtocol::Udp => udp_stream_open_error_is_path_retryable(err),
    }
}

fn no_schedulable_reliable_path_error(context: &ClientPathContext) -> RuntimeError {
    if !context.tcp_paths.is_empty() {
        RuntimeError::NoSchedulableTcpPath
    } else {
        RuntimeError::NoSchedulableUdpPath
    }
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
    let payload_bytes = match mode {
        TcpRelayAttachMode::Any => {
            tcp_relay_attach_payload_bytes(send_stream, class, context.mux_limits)
        }
        TcpRelayAttachMode::AutoBulkDiscovery => {
            tcp_relay_auto_bulk_discovery_payload_bytes(send_stream, context.mux_limits)
        }
    };
    if matches!(mode, TcpRelayAttachMode::AutoBulkDiscovery) {
        let candidates = context.ordered_reliable_auto_bulk_discovery_path_keys(
            remotes.active_path_index_for(UnderlayProtocol::Tcp),
            remotes.active_path_index_for(UnderlayProtocol::Udp),
            payload_bytes,
        );
        return attach_relay_path_candidates(
            context,
            remotes,
            RelayPathAttachRequest {
                spec,
                class,
                send_stream,
                resend_fin,
                candidates,
                race_repair: false,
                allow_mixed_carrier: true,
            },
        )
        .await;
    }
    if context.tcp_paths.is_empty() {
        return attach_udp_relay_paths(
            context,
            spec,
            class,
            remotes,
            send_stream,
            resend_fin,
            mode,
        )
        .await;
    }
    if remotes.active_carrier_underlay() == Some(UnderlayProtocol::Udp) {
        return attach_udp_relay_paths(
            context,
            spec,
            class,
            remotes,
            send_stream,
            resend_fin,
            mode,
        )
        .await;
    }
    let candidates = context.ordered_tcp_repair_path_indices(
        remotes.active_path_index_for(UnderlayProtocol::Tcp),
        class,
        payload_bytes,
    );
    let race_repair = tcp_relay_should_race_repair(class, send_stream, resend_fin, mode);
    let attached = attach_relay_path_candidates(
        context,
        remotes,
        RelayPathAttachRequest {
            spec,
            class,
            send_stream,
            resend_fin,
            candidates: candidates
                .into_iter()
                .map(|index| RelayPathKey {
                    underlay: UnderlayProtocol::Tcp,
                    index,
                })
                .collect(),
            race_repair,
            allow_mixed_carrier: false,
        },
    )
    .await?;
    if attached > 0 {
        return Ok(attached);
    }
    if !context.udp_paths.is_empty() && remotes.is_empty() {
        return attach_udp_relay_paths(
            context,
            spec,
            class,
            remotes,
            send_stream,
            resend_fin,
            mode,
        )
        .await;
    }
    Ok(0)
}

async fn attach_udp_relay_paths(
    context: &ClientPathContext,
    spec: &TcpRelayOpenSpec,
    class: TrafficClass,
    remotes: &mut TcpRelayRemoteSet,
    send_stream: &ReliableSendStream,
    resend_fin: bool,
    mode: TcpRelayAttachMode,
) -> Result<usize, RuntimeError> {
    if remotes.active_carrier_underlay() == Some(UnderlayProtocol::Tcp) {
        return Ok(0);
    }
    let stream_id = remotes.stream_id();
    let payload_bytes = match mode {
        TcpRelayAttachMode::Any => {
            tcp_relay_attach_payload_bytes(send_stream, class, context.mux_limits)
        }
        TcpRelayAttachMode::AutoBulkDiscovery => {
            tcp_relay_auto_bulk_discovery_payload_bytes(send_stream, context.mux_limits)
        }
    };
    let mut candidates = match mode {
        TcpRelayAttachMode::Any => {
            let require_delivery_evidence =
                matches!(class, TrafficClass::Bulk | TrafficClass::Background)
                    && !remotes.is_empty();
            context.ordered_udp_stream_repair_path_indices(
                remotes.active_path_index_for(UnderlayProtocol::Udp),
                class,
                payload_bytes,
                require_delivery_evidence,
            )
        }
        TcpRelayAttachMode::AutoBulkDiscovery => context
            .ordered_udp_stream_auto_bulk_discovery_indices(
                remotes.active_path_index_for(UnderlayProtocol::Udp),
                payload_bytes,
            ),
    };
    if candidates.is_empty() && remotes.is_empty() {
        candidates = (0..context.udp_paths.len()).collect();
    }
    if matches!(mode, TcpRelayAttachMode::AutoBulkDiscovery) {
        candidates.retain(|path_index| {
            !remotes.contains_path_key(RelayPathKey {
                underlay: UnderlayProtocol::Udp,
                index: *path_index,
            })
        });
    }
    let race_repair = tcp_relay_should_race_repair(class, send_stream, resend_fin, mode);
    let mut last_retryable_error = None;
    let mut attached = 0usize;

    for path_index in candidates {
        let key = RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: path_index,
        };
        if remotes.contains_path_key(key) {
            continue;
        }
        match open_remote_stream_on_udp_path(
            context,
            stream_id,
            spec.target.clone(),
            spec.ingress,
            class,
            path_index,
            false,
        )
        .await
        {
            Ok(opened) => {
                match replay_tcp_repair_cache(&opened.stream, send_stream, resend_fin).await {
                    Ok(()) => {
                        remotes.attach(opened);
                        attached += 1;
                        if !race_repair {
                            return Ok(attached);
                        }
                    }
                    Err(err) if tcp_relay_error_is_migratable(&err) => {
                        context.mark_udp_path_failure(path_index);
                        context.release_udp_stream_path_load(path_index, class);
                        last_retryable_error = Some(err);
                    }
                    Err(err) => {
                        context.release_udp_stream_path_load(path_index, class);
                        return Err(err);
                    }
                }
            }
            Err(err) if udp_stream_open_error_is_path_retryable(&err) => {
                context.mark_udp_path_failure(path_index);
                last_retryable_error = Some(err);
            }
            Err(err) => return Err(err),
        }
    }
    if attached > 0 {
        Ok(attached)
    } else if remotes.is_empty() {
        Err(last_retryable_error.unwrap_or(RuntimeError::NoSchedulableUdpPath))
    } else {
        Ok(0)
    }
}

fn relay_path_candidates_for_active_carrier(
    candidates: Vec<RelayPathKey>,
    active_underlay: Option<UnderlayProtocol>,
) -> Vec<RelayPathKey> {
    let Some(active_underlay) = active_underlay else {
        return candidates;
    };
    candidates
        .into_iter()
        .filter(|candidate| candidate.underlay == active_underlay)
        .collect()
}

fn tcp_relay_should_race_repair(
    class: TrafficClass,
    send_stream: &ReliableSendStream,
    resend_fin: bool,
    mode: TcpRelayAttachMode,
) -> bool {
    matches!(mode, TcpRelayAttachMode::Any)
        && !resend_fin
        && tcp_relay_expects_interactive_response(class)
        && send_stream.repair_bytes() <= PATH_OPEN_SCORE_BYTES
}

fn tcp_relay_attach_payload_bytes(
    send_stream: &ReliableSendStream,
    class: TrafficClass,
    mux_limits: MuxLimits,
) -> usize {
    let floor = if tcp_relay_expects_interactive_response(class) {
        PATH_OPEN_SCORE_BYTES
    } else {
        tcp_relay_buffer_len(mux_limits)
    };
    let repair_bytes = send_stream.repair_bytes().max(floor);
    let stream_window = usize::try_from(mux_limits.max_stream_window_bytes).unwrap_or(usize::MAX);
    repair_bytes.min(stream_window)
}

fn tcp_relay_auto_bulk_discovery_payload_bytes(
    send_stream: &ReliableSendStream,
    mux_limits: MuxLimits,
) -> usize {
    let attach_payload =
        tcp_relay_attach_payload_bytes(send_stream, TrafficClass::Bulk, mux_limits);
    let stream_window = usize::try_from(mux_limits.max_stream_window_bytes).unwrap_or(usize::MAX);
    attach_payload.max(mux_limits.max_tcp_path_inflight_bytes.min(stream_window))
}

#[derive(Debug)]
struct UdpStreamCongestion {
    cwnd_bytes: usize,
    ssthresh_bytes: usize,
    max_bytes: usize,
    mss_bytes: usize,
    srtt: Option<Duration>,
    pending_samples: VecDeque<UdpStreamSentSample>,
}

#[derive(Debug)]
struct UdpStreamSentSample {
    sent_at: Instant,
    bytes: usize,
}

impl UdpStreamCongestion {
    fn new(mux_limits: MuxLimits) -> Self {
        let mss_bytes = udp_stream_frame_payload_bytes(mux_limits).max(1);
        let max_bytes = mux_limits
            .max_tcp_path_inflight_bytes
            .min(
                mux_limits
                    .max_tcp_relay_chunk_bytes
                    .max(mss_bytes.saturating_mul(10)),
            )
            .max(mss_bytes);
        let initial_bytes = udp_stream_initial_cwnd_bytes(mss_bytes, max_bytes);
        Self {
            cwnd_bytes: initial_bytes,
            ssthresh_bytes: max_bytes,
            max_bytes,
            mss_bytes,
            srtt: None,
            pending_samples: VecDeque::new(),
        }
    }

    fn inflight_limit(&self) -> usize {
        self.cwnd_bytes.clamp(self.mss_bytes, self.max_bytes)
    }

    fn repair_budget(&self, repair_bytes: usize) -> usize {
        if repair_bytes == 0 {
            return 0;
        }
        repair_bytes.min(self.inflight_limit()).max(self.mss_bytes)
    }

    fn on_send(&mut self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        self.pending_samples.push_back(UdpStreamSentSample {
            sent_at: Instant::now(),
            bytes,
        });
    }

    fn on_ack(&mut self, released_bytes: usize) {
        if released_bytes == 0 {
            return;
        }
        if let Some(sample) = self.ack_sample(released_bytes) {
            self.observe_rtt(sample);
        }
        if self.cwnd_bytes < self.ssthresh_bytes {
            self.cwnd_bytes = self.cwnd_bytes.saturating_add(released_bytes);
        } else {
            let additive = ((self.mss_bytes as u64 * released_bytes as u64)
                / self.cwnd_bytes.max(1) as u64)
                .max(1) as usize;
            self.cwnd_bytes = self.cwnd_bytes.saturating_add(additive);
        }
        self.cwnd_bytes = self.cwnd_bytes.clamp(self.mss_bytes, self.max_bytes);
    }

    fn on_repair_timeout(&mut self) {
        let reduced = self.cwnd_bytes.saturating_sub((self.cwnd_bytes / 4).max(1));
        self.ssthresh_bytes = reduced
            .max(udp_stream_min_cwnd_bytes(self.mss_bytes))
            .min(self.max_bytes);
        self.cwnd_bytes = self.ssthresh_bytes;
    }

    fn repair_replay_interval(&self, repair_bytes: usize, mux_limits: MuxLimits) -> Duration {
        let fallback = udp_stream_repair_replay_interval(repair_bytes, mux_limits);
        let Some(srtt) = self.srtt else {
            return fallback;
        };
        Duration::from_secs_f64(srtt.as_secs_f64().mul_add(2.0, 0.025))
            .max(fallback)
            .min(TCP_STREAM_STALL_MAX_TIMEOUT)
    }

    fn ack_sample(&mut self, released_bytes: usize) -> Option<Duration> {
        let now = Instant::now();
        let mut remaining = released_bytes;
        let mut sample = None;
        while remaining > 0 {
            let Some(front) = self.pending_samples.front_mut() else {
                break;
            };
            sample.get_or_insert_with(|| now.saturating_duration_since(front.sent_at));
            let consumed = remaining.min(front.bytes);
            remaining -= consumed;
            front.bytes -= consumed;
            if front.bytes == 0 {
                self.pending_samples.pop_front();
            }
        }
        sample
    }

    fn observe_rtt(&mut self, sample: Duration) {
        let sample = sample.max(MIN_RATE_SAMPLE_DURATION);
        self.srtt = Some(match self.srtt {
            Some(previous) => Duration::from_secs_f64(
                previous
                    .as_secs_f64()
                    .mul_add(0.875, sample.as_secs_f64() * 0.125),
            ),
            None => sample,
        });
    }
}

fn udp_stream_initial_cwnd_bytes(mss_bytes: usize, max_bytes: usize) -> usize {
    let initial = mss_bytes.saturating_mul(10);
    initial.clamp(mss_bytes, max_bytes)
}

fn udp_stream_min_cwnd_bytes(mss_bytes: usize) -> usize {
    mss_bytes.saturating_mul(2).max(mss_bytes)
}

fn tcp_relay_stall_watch_active(
    send_stream: &ReliableSendStream,
    recv_stream: &ReliableRecvStream,
    remote_open: bool,
    class: TrafficClass,
    interactive_response_pending: bool,
    mux_limits: MuxLimits,
) -> bool {
    send_stream.repair_bytes() > 0
        || (remote_open
            && interactive_response_pending
            && tcp_relay_expects_interactive_response(class))
        || tcp_relay_response_stall_watch_active(recv_stream, remote_open, class, mux_limits)
}

fn tcp_relay_response_stall_watch_active(
    recv_stream: &ReliableRecvStream,
    remote_open: bool,
    class: TrafficClass,
    mux_limits: MuxLimits,
) -> bool {
    remote_open
        && recv_stream.next_offset() > 0
        && (matches!(class, TrafficClass::Bulk | TrafficClass::Background)
            || recv_stream.next_offset() >= tcp_relay_response_stall_watch_bytes(mux_limits))
}

fn tcp_relay_stall_progress_anchor(
    last_stream_progress_at: Instant,
    last_delivery_progress_at: Instant,
    last_response_stall_repair_at: Instant,
    recv_stream: &ReliableRecvStream,
    remote_open: bool,
    class: TrafficClass,
    mux_limits: MuxLimits,
) -> Instant {
    if tcp_relay_response_stall_watch_active(recv_stream, remote_open, class, mux_limits) {
        last_delivery_progress_at.max(last_response_stall_repair_at)
    } else {
        last_stream_progress_at
    }
}

fn tcp_relay_receive_hole_repair_active(
    recv_stream: &ReliableRecvStream,
    remote_open: bool,
) -> bool {
    remote_open && recv_stream.next_offset() > 0 && recv_stream.reorder_bytes() > 0
}

fn tcp_relay_receive_hole_repair_deadline(
    last_delivery_progress_at: Instant,
    last_receive_hole_repair_at: Instant,
    path: Option<PathSnapshot>,
    class: TrafficClass,
) -> tokio::time::Instant {
    let anchor = if last_delivery_progress_at > last_receive_hole_repair_at {
        last_delivery_progress_at
    } else {
        last_receive_hole_repair_at
    };
    tokio::time::Instant::from_std(anchor + tcp_relay_stall_timeout(path, class))
}

fn tcp_relay_receive_hole_failure_attempts(_class: TrafficClass) -> u32 {
    1
}

fn tcp_relay_sole_survivor_reannounce_attempts(stall_timeout: Duration) -> u32 {
    const FLUENT_REPAIR_BUDGET: Duration = Duration::from_millis(4500);
    let timeout = stall_timeout.max(TCP_STREAM_STALL_MIN_TIMEOUT);
    (FLUENT_REPAIR_BUDGET.as_secs_f64() / timeout.as_secs_f64())
        .floor()
        .clamp(1.0, 16.0) as u32
}

fn tcp_relay_refresh_path_tracking(
    path_last_delivery_at: &mut HashMap<RelayPathKey, Instant>,
    path_keys: &[RelayPathKey],
    now: Instant,
) {
    let live_paths = path_keys.iter().copied().collect::<HashSet<_>>();
    path_last_delivery_at.retain(|path_key, _| live_paths.contains(path_key));
    for path_key in path_keys {
        path_last_delivery_at.entry(*path_key).or_insert(now);
    }
}

fn tcp_relay_receive_hole_victim(
    context: &ClientPathContext,
    path_keys: &[RelayPathKey],
    class: TrafficClass,
    payload_bytes: usize,
    path_last_delivery_at: &HashMap<RelayPathKey, Instant>,
) -> Option<RelayPathKey> {
    if path_keys.len() <= 1 {
        return None;
    }
    path_keys.iter().copied().max_by(|left, right| {
        let left_score = tcp_relay_receive_hole_victim_score(context, *left, class, payload_bytes);
        let right_score =
            tcp_relay_receive_hole_victim_score(context, *right, class, payload_bytes);
        left_score
            .total_cmp(&right_score)
            .then_with(|| tcp_relay_stale_delivery_order(*left, *right, path_last_delivery_at))
    })
}

fn tcp_relay_receive_hole_victim_score(
    context: &ClientPathContext,
    key: RelayPathKey,
    class: TrafficClass,
    payload_bytes: usize,
) -> f64 {
    relay_path_snapshot(context, key)
        .and_then(|snapshot| {
            scheduler::score_path(snapshot, class, payload_bytes, SchedulerPolicy::default())
                .map(|score| score.eta_ms)
        })
        .unwrap_or(f64::INFINITY)
}

fn tcp_relay_stale_delivery_order(
    left: RelayPathKey,
    right: RelayPathKey,
    path_last_delivery_at: &HashMap<RelayPathKey, Instant>,
) -> std::cmp::Ordering {
    match (
        path_last_delivery_at.get(&left),
        path_last_delivery_at.get(&right),
    ) {
        (Some(left_seen), Some(right_seen)) => right_seen
            .cmp(left_seen)
            .then_with(|| relay_path_key_order(right, left)),
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, None) => relay_path_key_order(right, left),
    }
}

fn relay_path_snapshot(context: &ClientPathContext, key: RelayPathKey) -> Option<PathSnapshot> {
    match key.underlay {
        UnderlayProtocol::Tcp => context.tcp_path_snapshot(key.index),
        UnderlayProtocol::Udp => context.udp_path_snapshot(key.index),
    }
}

fn relay_path_key_order(left: RelayPathKey, right: RelayPathKey) -> std::cmp::Ordering {
    relay_underlay_order(left.underlay)
        .cmp(&relay_underlay_order(right.underlay))
        .then_with(|| left.index.cmp(&right.index))
}

fn relay_underlay_order(underlay: UnderlayProtocol) -> u8 {
    match underlay {
        UnderlayProtocol::Tcp => 0,
        UnderlayProtocol::Udp => 1,
    }
}

fn tcp_relay_expects_interactive_response(class: TrafficClass) -> bool {
    matches!(class, TrafficClass::Control | TrafficClass::Interactive)
}

fn tcp_relay_response_stall_watch_bytes(mux_limits: MuxLimits) -> u64 {
    (tcp_relay_buffer_len(mux_limits) as u64).min(mux_limits.max_stream_window_bytes)
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

async fn replay_tcp_repair_cache_limited(
    path_stream: &TcpPathStream,
    send_stream: &ReliableSendStream,
    resend_fin: bool,
    byte_limit: usize,
) -> Result<(), RuntimeError> {
    for frame in send_stream.retransmission_frames_limited(byte_limit) {
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

#[derive(Debug, Default)]
struct ReliableRecvProgress {
    last_max_data_offset: u64,
}

impl ReliableRecvProgress {
    fn should_send_max_data(
        &mut self,
        recv_stream: &ReliableRecvStream,
        mux_limits: MuxLimits,
        force: bool,
    ) -> bool {
        let max_offset = recv_stream.max_data_offset();
        if force
            || self.last_max_data_offset == 0
            || max_offset.saturating_sub(self.last_max_data_offset)
                >= reliable_stream_max_data_update_bytes(mux_limits)
        {
            self.last_max_data_offset = max_offset;
            true
        } else {
            false
        }
    }
}

fn reliable_stream_max_data_update_bytes(mux_limits: MuxLimits) -> u64 {
    let window_step = mux_limits.max_stream_window_bytes.saturating_div(4).max(1);
    let payload_step = tcp_relay_buffer_len(mux_limits) as u64;
    window_step
        .max(payload_step)
        .min(mux_limits.max_stream_window_bytes)
}

async fn send_tcp_recv_progress(
    path_stream: &TcpPathStream,
    recv_stream: &ReliableRecvStream,
    progress: &mut ReliableRecvProgress,
    mux_limits: MuxLimits,
    force_max_data: bool,
) -> Result<(), RuntimeError> {
    for frame in recv_stream.ack_frames() {
        path_stream.send_frame(frame).await?;
    }
    if progress.should_send_max_data(recv_stream, mux_limits, force_max_data) {
        path_stream.send_frame(recv_stream.max_data_frame()).await?;
    }
    Ok(())
}

fn tcp_relay_recv_progress_resend_active(
    recv_stream: &ReliableRecvStream,
    remote_open: bool,
) -> bool {
    remote_open && (recv_stream.next_offset() > 0 || recv_stream.reorder_bytes() > 0)
}

fn reliable_stream_recv_progress_interval(
    path: Option<PathSnapshot>,
    class: TrafficClass,
) -> Duration {
    tcp_relay_stall_timeout(path, class)
        .div_f64(2.0)
        .max(UDP_MIN_RESPONSE_TIMEOUT)
        .min(TCP_STREAM_STALL_MIN_TIMEOUT)
}

async fn send_tcp_recv_progress_remote_set(
    remotes: &mut TcpRelayRemoteSet,
    context: &ClientPathContext,
    recv_stream: &ReliableRecvStream,
    progress: &mut ReliableRecvProgress,
    force_max_data: bool,
) -> Result<(), RuntimeError> {
    for frame in recv_stream.ack_frames() {
        remotes.send_frame(context, frame).await?;
    }
    if progress.should_send_max_data(recv_stream, context.mux_limits, force_max_data) {
        remotes
            .send_frame(context, recv_stream.max_data_frame())
            .await?;
    }
    Ok(())
}

fn tcp_relay_buffer_len(mux_limits: MuxLimits) -> usize {
    mux_limits
        .max_tcp_relay_chunk_bytes
        .min(mux_limits.max_payload_bytes)
        .min(mux_limits.max_tcp_path_inflight_bytes)
        .max(1)
}

fn udp_stream_frame_payload_bytes(mux_limits: MuxLimits) -> usize {
    UDP_DEFAULT_MTU_PAYLOAD_BYTES
        .saturating_sub(128)
        .clamp(UDP_MIN_MTU_PAYLOAD_BYTES, UDP_DEFAULT_MTU_PAYLOAD_BYTES)
        .min(tcp_relay_buffer_len(mux_limits))
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
    let chunk_size = tcp_relay_buffer_len(mux_limits)
        .min(path_stream.max_frame_payload_bytes)
        .max(1);
    let mut buf = vec![0u8; chunk_size];
    let mut local_open = true;
    let mut remote_open = true;
    let mut stats = PathDeliveryStats::default();
    let mut close_sent = false;
    let mut pending_local_fin = false;
    let mut last_repair_replay_at = Instant::now();
    let mut udp_congestion = (path_stream.underlay == UnderlayProtocol::Udp)
        .then(|| UdpStreamCongestion::new(mux_limits));
    let mut recv_progress = ReliableRecvProgress::default();
    let mut last_recv_progress_sent_at = Instant::now();

    let result = loop {
        if !local_open && !remote_open && send_stream.repair_bytes() == 0 {
            break Ok(stats);
        }
        let repair_replay_interval = if let Some(congestion) = &udp_congestion {
            congestion.repair_replay_interval(send_stream.repair_bytes(), mux_limits)
        } else {
            tcp_relay_repair_replay_interval(send_stream.repair_bytes(), mux_limits)
        };
        let repair_replay_deadline =
            tokio::time::Instant::from_std(last_repair_replay_at + repair_replay_interval);
        let recv_progress_deadline = tokio::time::Instant::from_std(
            last_recv_progress_sent_at
                + reliable_stream_recv_progress_interval(None, path_stream.class),
        );
        let inflight_limit = udp_congestion
            .as_ref()
            .map(|congestion| congestion.inflight_limit())
            .unwrap_or(mux_limits.max_tcp_path_inflight_bytes);
        let can_read_local =
            local_open && tcp_relay_can_read_with_limit(&send_stream, inflight_limit);
        let read_budget = if can_read_local {
            tcp_relay_read_budget_with_limit(&send_stream, mux_limits, inflight_limit, buf.len())
        } else {
            0
        };

        tokio::select! {
            biased;
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
                        send_tcp_recv_progress(
                            &path_stream,
                            &recv_stream,
                            &mut recv_progress,
                            mux_limits,
                            false,
                        )
                        .await?;
                        last_recv_progress_sent_at = Instant::now();
                        if outcome.fin {
                            local.shutdown().await?;
                            remote_open = false;
                        }
                    }
                    Frame::StreamAck {
                        stream_id: ack_stream_id,
                        ranges,
                    } if ack_stream_id == stream_id => {
                        let previous_repair_bytes = send_stream.repair_bytes();
                        let ack = send_stream.apply_ack(&ranges);
                        if let Some(congestion) = &mut udp_congestion {
                            congestion.on_ack(ack.released_bytes);
                        }
                        if send_stream.repair_bytes() < previous_repair_bytes {
                            last_repair_replay_at = Instant::now();
                        }
                        if pending_local_fin && send_stream.repair_bytes() == 0 {
                            path_stream
                                .send_frame(Frame::StreamFin { stream_id })
                                .await?;
                            close_sent = true;
                            pending_local_fin = false;
                        }
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
                        if path_stream.underlay == UnderlayProtocol::Udp {
                            let budget = udp_congestion
                                .as_ref()
                                .map(|congestion| {
                                    congestion.repair_budget(send_stream.repair_bytes())
                                })
                                .unwrap_or_else(|| send_stream.repair_bytes());
                            replay_tcp_repair_cache_limited(
                                &path_stream,
                                &send_stream,
                                false,
                                budget,
                            )
                            .await?;
                        } else {
                            replay_tcp_repair_cache(&path_stream, &send_stream, false).await?;
                        }
                        last_repair_replay_at = Instant::now();
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
            _ = tokio::time::sleep_until(repair_replay_deadline), if send_stream.repair_bytes() > 0 => {
                if let Some(congestion) = &mut udp_congestion {
                    congestion.on_repair_timeout();
                }
                let budget = udp_congestion
                    .as_ref()
                    .map(|congestion| congestion.repair_budget(send_stream.repair_bytes()))
                    .unwrap_or_else(|| send_stream.repair_bytes());
                replay_tcp_repair_cache_limited(&path_stream, &send_stream, false, budget).await?;
                last_repair_replay_at = Instant::now();
            }
            _ = tokio::time::sleep_until(recv_progress_deadline), if path_stream.underlay == UnderlayProtocol::Udp
                && tcp_relay_recv_progress_resend_active(&recv_stream, remote_open) => {
                send_tcp_recv_progress(
                    &path_stream,
                    &recv_stream,
                    &mut recv_progress,
                    mux_limits,
                    true,
                )
                .await?;
                last_recv_progress_sent_at = Instant::now();
            }
            read = local.read(&mut buf[..read_budget]), if can_read_local => {
                let read = read?;
                if read == 0 {
                    if path_stream.underlay == UnderlayProtocol::Udp
                        && send_stream.repair_bytes() > 0
                    {
                        pending_local_fin = true;
                    } else {
                        path_stream
                            .send_frame(Frame::StreamFin { stream_id })
                            .await?;
                        close_sent = true;
                    }
                    local_open = false;
                } else {
                    let frame = send_stream.send_data(
                        Bytes::copy_from_slice(&buf[..read]),
                        StreamFlags::NONE,
                    )?;
                    path_stream.send_frame(frame).await?;
                    if let Some(congestion) = &mut udp_congestion {
                        congestion.on_send(read);
                    }
                    stats.record_payload_bytes(read);
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

fn tcp_relay_repair_replay_interval(repair_bytes: usize, mux_limits: MuxLimits) -> Duration {
    if repair_bytes == 0 {
        return TCP_STREAM_STALL_MAX_TIMEOUT;
    }
    let inflight = mux_limits.max_tcp_path_inflight_bytes.max(1) as f64;
    let pressure = (repair_bytes as f64 / inflight).clamp(0.0, 1.0);
    let min = TCP_STREAM_STALL_MIN_TIMEOUT.as_secs_f64();
    let max = TCP_STREAM_STALL_MAX_TIMEOUT.as_secs_f64();
    Duration::from_secs_f64(min + (max - min) * pressure)
}

fn udp_stream_repair_replay_interval(repair_bytes: usize, mux_limits: MuxLimits) -> Duration {
    tcp_relay_repair_replay_interval(repair_bytes, mux_limits)
        .min(TCP_STREAM_STALL_MIN_TIMEOUT)
        .max(UDP_MIN_RESPONSE_TIMEOUT)
}

fn tcp_relay_can_read_with_limit(send_stream: &ReliableSendStream, inflight_limit: usize) -> bool {
    send_stream.repair_bytes() < inflight_limit.max(1)
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
    let payload_len = payload.len();
    let mut session = UdpDatagramClientSession::open(
        path,
        0,
        security,
        codec_limits,
        mux_limits,
        UDP_PATH_HANDSHAKE_TIMEOUT,
    )
    .await?;
    let response = session
        .send_to(target, payload, ttl_ms, UDP_MAX_RESPONSE_TIMEOUT)
        .await
        .map_err(|err| match err {
            UdpPathSendError::Runtime(err) => err,
            UdpPathSendError::MtuExceeded { limit } => {
                RuntimeError::Datagram(DatagramError::PayloadTooLarge {
                    actual: payload_len,
                    limit,
                })
            }
            UdpPathSendError::Timeout { .. } => {
                RuntimeError::Protocol("UDP datagram response timed out")
            }
        })?;
    session.close().await?;
    Ok(response)
}

struct UdpDatagramClientAssociation {
    context: ClientPathContext,
    session_id: SessionId,
    paths: Vec<UdpDatagramAssociationPath>,
    suppressed_paths: HashMap<usize, Instant>,
    last_successful_path: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct UdpPathCandidate {
    path_index: usize,
    eta_ms: f64,
}

#[derive(Debug, Clone, Copy)]
struct UdpAssociationCandidateScore {
    path_index: usize,
    completion_ms: f64,
    eta_ms: f64,
    opens_new_session: bool,
    rank: usize,
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
            suppressed_paths: HashMap::new(),
            last_successful_path: None,
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
            .ordered_udp_path_candidates_for_ttl(payload.len(), ttl_ms);
        if candidates.is_empty() {
            return Err(RuntimeError::NoSchedulableUdpPath);
        }

        self.prune_suppressed_paths();
        let mut attempted = HashSet::new();
        let mut retried_acked_timeout = HashSet::new();
        let mut last_retryable_error = None;
        while let Some(path_index) =
            self.select_path_candidate(&candidates, &attempted, payload.len(), ttl_ms)
        {
            attempted.insert(path_index);
            let has_unattempted_alternative = candidates
                .iter()
                .any(|candidate| !attempted.contains(&candidate.path_index));
            match self
                .send_to_path(
                    path_index,
                    target.clone(),
                    payload.clone(),
                    ttl_ms,
                    has_unattempted_alternative,
                )
                .await
            {
                Ok(response) => {
                    self.last_successful_path = Some(path_index);
                    return Ok(response);
                }
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
                    if path_was_acked
                        && retried_acked_timeout.insert(path_index)
                        && self.path_session_is_open(path_index)
                    {
                        self.context.mark_udp_path_feedback(
                            path_index,
                            UdpDatagramPathObservation {
                                rtt: response_timeout,
                                jitter: Duration::ZERO,
                                loss_rate: 1.0,
                                rate_sample: None,
                            },
                        );
                        attempted.remove(&path_index);
                        last_retryable_error =
                            Some(RuntimeError::Protocol("UDP datagram response timed out"));
                        continue;
                    }
                    if path_was_acked
                        && self.path_session_is_open(path_index)
                        && !self.has_validated_udp_retry_alternative(
                            &candidates,
                            &attempted,
                            path_index,
                        )
                    {
                        self.context.mark_udp_path_feedback(
                            path_index,
                            UdpDatagramPathObservation {
                                rtt: response_timeout,
                                jitter: Duration::ZERO,
                                loss_rate: 1.0,
                                rate_sample: None,
                            },
                        );
                        return Err(RuntimeError::Protocol("UDP datagram response timed out"));
                    }
                    self.remove_path(path_index).await;
                    self.suppress_path_after_timeout(path_index, response_timeout, ttl_ms);
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
                    self.suppress_path_after_timeout(path_index, UDP_MIN_RESPONSE_TIMEOUT, ttl_ms);
                    self.context.mark_udp_path_failure(path_index);
                    last_retryable_error = Some(err);
                }
                Err(UdpPathSendError::Runtime(err)) => return Err(err),
            }
        }
        Err(last_retryable_error.unwrap_or(RuntimeError::NoSchedulableUdpPath))
    }

    async fn send_to_with_adaptive_retries(
        &mut self,
        target: TargetAddr,
        payload: Bytes,
        ttl_ms: u32,
    ) -> Result<Bytes, RuntimeError> {
        let started_at = Instant::now();
        loop {
            match self.send_to(target.clone(), payload.clone(), ttl_ms).await {
                Ok(response) => return Ok(response),
                Err(err) if udp_datagram_error_is_path_retryable(&err) => {
                    if started_at.elapsed() >= self.adaptive_retry_budget(payload.len(), ttl_ms) {
                        return Err(err);
                    }
                }
                Err(err) => return Err(err),
            }
        }
    }

    fn adaptive_retry_budget(&self, payload_bytes: usize, ttl_ms: u32) -> Duration {
        let ttl = Duration::from_millis(u64::from(ttl_ms));
        let response_timeout = self
            .context
            .ordered_udp_path_candidates_for_ttl(payload_bytes, ttl_ms)
            .into_iter()
            .filter_map(|candidate| {
                self.context
                    .udp_path_runtime_model(candidate.path_index, ttl_ms)
                    .map(|model| model.response_timeout)
            })
            .min()
            .unwrap_or(UDP_MAX_RESPONSE_TIMEOUT);
        Duration::from_secs_f64(response_timeout.as_secs_f64() * 4.0)
            .max(UDP_MIN_RETRY_BUDGET)
            .min(UDP_MAX_RETRY_BUDGET)
            .min(ttl)
    }

    fn path_session_is_open(&self, path_index: usize) -> bool {
        self.paths
            .iter()
            .any(|path| path.session.path_index == path_index)
    }

    fn has_validated_udp_retry_alternative(
        &self,
        candidates: &[UdpPathCandidate],
        attempted: &HashSet<usize>,
        current_path_index: usize,
    ) -> bool {
        candidates.iter().any(|candidate| {
            candidate.path_index != current_path_index
                && !attempted.contains(&candidate.path_index)
                && self.path_has_datagram_feedback_or_hint(candidate.path_index)
        })
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
        candidates: &[UdpPathCandidate],
        attempted: &HashSet<usize>,
        payload_bytes: usize,
        ttl_ms: u32,
    ) -> Option<usize> {
        let now = Instant::now();
        let freshness_budget_ms = f64::from(ttl_ms) * UDP_DATAGRAM_MIN_TTL_FIT_RATIO;
        let mut viable = candidates
            .iter()
            .enumerate()
            .filter(|(_, candidate)| !attempted.contains(&candidate.path_index))
            .filter_map(|(rank, candidate)| {
                let open_ready_at = self
                    .paths
                    .iter()
                    .find(|path| path.session.path_index == candidate.path_index)
                    .map(|path| path.pacer.ready_at());
                let has_open_session = open_ready_at.is_some();
                let eta_ms = self.context.udp_path_eta_for_ttl(
                    candidate.path_index,
                    payload_bytes,
                    ttl_ms,
                    has_open_session,
                )?;
                let model = self
                    .context
                    .udp_path_runtime_model(candidate.path_index, ttl_ms)?;
                if !model.accepts_or_can_probe(payload_bytes) {
                    return None;
                }
                let ready_at = open_ready_at.unwrap_or(now);
                let ready_delay_ms = ready_at.saturating_duration_since(now).as_secs_f64() * 1000.0;
                let completion_ms = eta_ms + ready_delay_ms;
                (completion_ms <= freshness_budget_ms).then_some(UdpAssociationCandidateScore {
                    path_index: candidate.path_index,
                    completion_ms,
                    eta_ms,
                    opens_new_session: !has_open_session,
                    rank,
                })
            })
            .collect::<Vec<_>>();
        if viable.iter().any(|candidate| {
            self.path_has_datagram_feedback_or_hint(candidate.path_index)
                && !self.path_is_temporarily_suppressed(candidate.path_index, now)
        }) {
            viable
                .retain(|candidate| self.path_has_datagram_feedback_or_hint(candidate.path_index));
        }
        if self.context.udp_paths.iter().all(path_is_endpoint_only)
            && let Some(candidate) = viable
                .iter()
                .filter(|candidate| !self.path_is_temporarily_suppressed(candidate.path_index, now))
                .min_by(|left, right| left.path_index.cmp(&right.path_index))
        {
            return Some(candidate.path_index);
        }
        if let Some(path_index) = self.last_successful_path
            && let Some(candidate) = viable.iter().find(|candidate| {
                candidate.path_index == path_index
                    && !self.path_is_temporarily_suppressed(candidate.path_index, now)
            })
        {
            return Some(candidate.path_index);
        }
        let has_unsuppressed = viable
            .iter()
            .any(|candidate| !self.path_is_temporarily_suppressed(candidate.path_index, now));
        if has_unsuppressed {
            viable.retain(|candidate| {
                !self.path_is_temporarily_suppressed(candidate.path_index, now)
            });
        }
        viable
            .into_iter()
            .min_by(|left, right| {
                left.completion_ms
                    .total_cmp(&right.completion_ms)
                    .then_with(|| left.eta_ms.total_cmp(&right.eta_ms))
                    .then_with(|| left.opens_new_session.cmp(&right.opens_new_session))
                    .then_with(|| left.rank.cmp(&right.rank))
            })
            .map(|candidate| candidate.path_index)
    }

    fn suppress_path_after_timeout(
        &mut self,
        path_index: usize,
        response_timeout: Duration,
        ttl_ms: u32,
    ) {
        let ttl = Duration::from_millis(u64::from(ttl_ms));
        let adaptive = Duration::from_secs_f64(response_timeout.as_secs_f64() * 4.0)
            .max(UDP_MIN_PATH_SUPPRESSION);
        let duration = adaptive.min(PATH_FAILURE_COOLDOWN).min(ttl);
        self.suppressed_paths
            .insert(path_index, Instant::now() + duration);
    }

    fn prune_suppressed_paths(&mut self) {
        let now = Instant::now();
        self.suppressed_paths
            .retain(|_, suppressed_until| *suppressed_until > now);
    }

    fn path_is_temporarily_suppressed(&self, path_index: usize, now: Instant) -> bool {
        self.suppressed_paths
            .get(&path_index)
            .is_some_and(|suppressed_until| *suppressed_until > now)
    }

    fn path_has_datagram_feedback_or_hint(&self, path_index: usize) -> bool {
        let Some(path) = self.context.udp_paths.get(path_index) else {
            return false;
        };
        let Some(observation) = self
            .context
            .health
            .lock()
            .expect("client path health lock")
            .udp
            .get_mut(path_index)
            .map(|record| record.observe(Instant::now()))
        else {
            return false;
        };
        udp_observation_has_datagram_feedback(&observation)
            || path.metadata.initial_srtt_ms.is_some()
            || path.metadata.initial_jitter_ms.is_some()
            || path.metadata.initial_rate != RateHint::Unknown
    }

    async fn send_to_path(
        &mut self,
        path_index: usize,
        target: TargetAddr,
        payload: Bytes,
        ttl_ms: u32,
        has_unattempted_alternative: bool,
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
        let handshake_timeout = udp_datagram_path_open_timeout(
            !self.paths.is_empty(),
            has_unattempted_alternative,
            model,
            ttl_ms,
        );
        let position = self
            .ensure_path_session(path_index, handshake_timeout)
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
            let result = path
                .session
                .send_to(target, payload, ttl_ms, model.response_timeout)
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
            Ok(response) => Ok(response),
            Err(UdpPathSendError::Timeout {
                path_was_acked: _,
                response_timeout,
            }) => Err(UdpPathSendError::Timeout {
                path_was_acked,
                response_timeout,
            }),
            Err(err) => Err(err),
        }
    }

    async fn ensure_path_session(
        &mut self,
        path_index: usize,
        handshake_timeout: Duration,
    ) -> Result<usize, RuntimeError> {
        if let Some(position) = self
            .paths
            .iter()
            .position(|path| path.session.path_index == path_index)
        {
            return Ok(position);
        }
        let session = open_udp_datagram_session_on_path(
            &self.context,
            path_index,
            self.session_id,
            handshake_timeout,
        )
        .await?;
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

fn udp_datagram_path_open_timeout(
    association_has_open_path: bool,
    has_unattempted_alternative: bool,
    model: UdpPathRuntimeModel,
    ttl_ms: u32,
) -> Duration {
    let ttl_timeout = Duration::from_millis(u64::from(ttl_ms));
    if !association_has_open_path && !has_unattempted_alternative {
        return UDP_PATH_HANDSHAKE_TIMEOUT.min(ttl_timeout);
    }
    let response_timeout = if association_has_open_path {
        model.response_timeout
    } else {
        Duration::from_secs_f64(
            model.response_timeout.as_secs_f64() * UDP_FIRST_OPEN_RTT_MULTIPLIER,
        )
    };
    response_timeout
        .max(UDP_MIN_RESPONSE_TIMEOUT)
        .min(UDP_PATH_HANDSHAKE_TIMEOUT)
        .min(ttl_timeout)
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
        response_timeout: Duration,
    ) -> Result<Bytes, UdpPathSendError> {
        let flow_id = self
            .ensure_flow(target)
            .await
            .map_err(UdpPathSendError::Runtime)?;
        let frame = {
            let flow = self
                .flows
                .iter_mut()
                .find(|flow| flow.flow_id == flow_id)
                .ok_or(UdpPathSendError::Runtime(RuntimeError::Protocol(
                    "missing UDP datagram flow",
                )))?;
            flow.flow
                .enqueue(0, ttl_ms, payload)
                .map_err(|err| UdpPathSendError::Runtime(RuntimeError::Datagram(err)))?;
            flow.flow
                .pop_frame(0)
                .ok_or(UdpPathSendError::Runtime(RuntimeError::Protocol(
                    "datagram expired before send",
                )))?
        };
        let (request_datagram_id, request_len) = match &frame {
            Frame::DatagramData {
                datagram_id,
                payload,
                ..
            } => (*datagram_id, payload.len()),
            _ => {
                return Err(UdpPathSendError::Runtime(RuntimeError::Protocol(
                    "unexpected queued datagram frame",
                )));
            }
        };
        let request_key = (flow_id, request_datagram_id);
        let mut retransmitted = false;
        let mut observed_response_timeout = false;

        loop {
            self.sent_datagrams.insert(
                request_key,
                UdpSentDatagram {
                    sent_at: Instant::now(),
                    bytes: request_len,
                    ttl: Duration::from_millis(u64::from(ttl_ms)),
                },
            );
            self.encrypted
                .send_frame(&frame)
                .await
                .map_err(|err| UdpPathSendError::Runtime(RuntimeError::EncryptedUdp(err)))?;
            loop {
                let received = match tokio::time::timeout(
                    response_timeout,
                    self.encrypted.recv_frame(&mut self.buffer),
                )
                .await
                {
                    Ok(Ok(frame)) => frame,
                    Ok(Err(err)) => {
                        return Err(UdpPathSendError::Runtime(RuntimeError::EncryptedUdp(err)));
                    }
                    Err(_) if !retransmitted && self.last_feedback_observation.is_some() => {
                        observed_response_timeout = true;
                        retransmitted = true;
                        break;
                    }
                    Err(_) => {
                        return Err(UdpPathSendError::Timeout {
                            path_was_acked: self.last_feedback_observation.is_some(),
                            response_timeout,
                        });
                    }
                };
                match received {
                    Frame::DatagramFeedback { flow_id, received } => {
                        self.handle_datagram_feedback(flow_id, &received)
                            .map_err(UdpPathSendError::Runtime)?;
                    }
                    Frame::DatagramData {
                        flow_id: response_flow_id,
                        datagram_id,
                        payload,
                        ..
                    } if response_flow_id == flow_id && datagram_id == request_datagram_id => {
                        let request_ack = datagram_ack_range(request_datagram_id)
                            .map_err(UdpPathSendError::Runtime)?;
                        self.handle_datagram_feedback(flow_id, &[request_ack])
                            .map_err(UdpPathSendError::Runtime)?;
                        self.encrypted
                            .send_frame(&Frame::DatagramFeedback {
                                flow_id,
                                received: vec![
                                    datagram_ack_range(datagram_id)
                                        .map_err(UdpPathSendError::Runtime)?,
                                ],
                            })
                            .await
                            .map_err(|err| {
                                UdpPathSendError::Runtime(RuntimeError::EncryptedUdp(err))
                            })?;
                        self.stats.record_payload_bytes(request_len);
                        self.stats.record_payload_bytes(payload.len());
                        if observed_response_timeout {
                            self.last_feedback_observation = Some(UdpDatagramPathObservation {
                                rtt: response_timeout,
                                jitter: Duration::ZERO,
                                loss_rate: 1.0,
                                rate_sample: None,
                            });
                        }
                        return Ok(payload);
                    }
                    Frame::DatagramData {
                        flow_id: response_flow_id,
                        datagram_id,
                        ..
                    } if response_flow_id == flow_id => {
                        self.encrypted
                            .send_frame(&Frame::DatagramFeedback {
                                flow_id,
                                received: vec![
                                    datagram_ack_range(datagram_id)
                                        .map_err(UdpPathSendError::Runtime)?,
                                ],
                            })
                            .await
                            .map_err(|err| {
                                UdpPathSendError::Runtime(RuntimeError::EncryptedUdp(err))
                            })?;
                    }
                    Frame::PathMetrics { metrics } if metrics.path_id == self.path_id => {
                        self.observe_remote_path_metrics(metrics);
                    }
                    Frame::RxRateHint { path_id, .. } if path_id == self.path_id => {}
                    Frame::DatagramClose {
                        flow_id: closed_flow_id,
                    } if closed_flow_id == flow_id => {
                        return Err(UdpPathSendError::Runtime(RuntimeError::Protocol(
                            "datagram flow closed",
                        )));
                    }
                    Frame::SessionClose { reason } => {
                        return Err(UdpPathSendError::Runtime(RuntimeError::RemoteClosed(
                            reason,
                        )));
                    }
                    _ => {
                        return Err(UdpPathSendError::Runtime(RuntimeError::Protocol(
                            "unexpected UDP datagram frame",
                        )));
                    }
                }
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
        if session.is_none() {
            let (len, peer) = socket.recv_from(&mut buffer).await?;
            session = Some(ServerUdpPathSession::new(
                socket.clone(),
                peer,
                context.clone(),
            )?);
            let session_ref = session
                .as_mut()
                .ok_or(RuntimeError::Protocol("missing UDP path session"))?;
            let frame = match session_ref.open_frame(&buffer[..len]) {
                Ok(frame) => frame,
                Err(err) if udp_runtime_error_is_ignorable(&err) => continue,
                Err(err) => return Err(err),
            };
            match session_ref.handle_frame(frame).await? {
                ServerUdpSessionOutcome::Active => {}
                ServerUdpSessionOutcome::Closed => return Ok(()),
            }
            continue;
        }

        let session_ref = session
            .as_mut()
            .ok_or(RuntimeError::Protocol("missing UDP path session"))?;
        let command_may_recv = !tcp_path_receivers_closed(&session_ref.commands_rx);
        tokio::select! {
            received = socket.recv_from(&mut buffer) => {
                let (len, peer) = received?;
                if session_ref.peer != peer {
                    return Err(RuntimeError::Protocol(
                        "UDP datagram arrived from unexpected peer",
                    ));
                }
                let frame = match session_ref.open_frame(&buffer[..len]) {
                    Ok(frame) => frame,
                    Err(err) if udp_runtime_error_is_ignorable(&err) => continue,
                    Err(err) => return Err(err),
                };
                match session_ref.handle_frame(frame).await? {
                    ServerUdpSessionOutcome::Active => {}
                    ServerUdpSessionOutcome::Closed => return Ok(()),
                }
            }
            command = recv_tcp_path_command(&mut session_ref.commands_rx), if command_may_recv => {
                if let Some(command) = command {
                    match session_ref.handle_command(command).await? {
                        ServerUdpSessionOutcome::Active => {}
                        ServerUdpSessionOutcome::Closed => return Ok(()),
                    }
                }
            }
        }
    }
}

struct ServerUdpDatagramFlow {
    flow_id: DatagramFlowId,
    requests: mpsc::Sender<ServerUdpDatagramRequest>,
}

struct ServerUdpDatagramRequest {
    datagram_id: DatagramId,
    ttl_ms: u32,
    payload: Bytes,
}

fn server_udp_datagram_request_queue_len(mux_limits: MuxLimits) -> usize {
    let unit = mux_limits.max_payload_bytes.max(1);
    mux_limits
        .max_datagram_queue_bytes
        .saturating_div(unit)
        .clamp(1, 1024)
}

fn spawn_server_udp_datagram_flow_worker(
    flow_id: DatagramFlowId,
    mut outbound_socket: outbound::OutboundUdpSocket,
    commands: TcpPathSessionCommandSender,
    mux_limits: MuxLimits,
) -> mpsc::Sender<ServerUdpDatagramRequest> {
    let (requests_tx, mut requests_rx) = mpsc::channel::<ServerUdpDatagramRequest>(
        server_udp_datagram_request_queue_len(mux_limits),
    );
    tokio::spawn(async move {
        let mut response_buffer = vec![0u8; mux_limits.max_payload_bytes.min(64 * 1024)];
        let mut pending_ttls = VecDeque::<(Instant, u32, DatagramId)>::new();
        loop {
            prune_server_udp_pending_ttls(&mut pending_ttls);
            tokio::select! {
                request = requests_rx.recv() => {
                    let Some(request) = request else {
                        break;
                    };
                    if request.ttl_ms == 0 {
                        continue;
                    }
                    match outbound_socket.send(&request.payload).await {
                        Ok(_) => {
                            pending_ttls.push_back((
                                Instant::now() + Duration::from_millis(u64::from(request.ttl_ms)),
                                request.ttl_ms,
                                request.datagram_id,
                            ));
                        }
                        Err(err) => {
                            eprintln!("warning: UDP outbound send failed: {err}");
                        }
                    }
                }
                received = outbound_socket.recv(&mut response_buffer) => {
                    let len = match received {
                        Ok(len) => len,
                        Err(err) => {
                            eprintln!("warning: UDP outbound receive failed: {err}");
                            let _ = commands
                                .send_frame(Frame::DatagramClose { flow_id }, TrafficClass::RealtimeDatagram)
                                .await;
                            break;
                        }
                    };
                    let Some((ttl_ms, datagram_id)) =
                        server_udp_next_response_ttl(&mut pending_ttls)
                    else {
                        continue;
                    };
                    let frame = Frame::DatagramData {
                        flow_id,
                        datagram_id,
                        ttl_ms,
                        payload: Bytes::copy_from_slice(&response_buffer[..len]),
                    };
                    if commands
                        .send_frame(frame, TrafficClass::RealtimeDatagram)
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            }
        }
    });
    requests_tx
}

fn prune_server_udp_pending_ttls(pending_ttls: &mut VecDeque<(Instant, u32, DatagramId)>) {
    let now = Instant::now();
    while pending_ttls
        .front()
        .is_some_and(|(deadline, _, _)| *deadline <= now)
    {
        pending_ttls.pop_front();
    }
}

fn server_udp_next_response_ttl(
    pending_ttls: &mut VecDeque<(Instant, u32, DatagramId)>,
) -> Option<(u32, DatagramId)> {
    prune_server_udp_pending_ttls(pending_ttls);
    pending_ttls
        .pop_front()
        .map(|(_, ttl_ms, datagram_id)| (ttl_ms, datagram_id))
}

struct ServerUdpPathSession {
    peer: SocketAddr,
    encrypted: EncryptedUdpSocket,
    context: ServerPathContext,
    authenticator: SessionAuthenticator,
    state: ServerUdpPathState,
    flows: Vec<ServerUdpDatagramFlow>,
    commands_tx: TcpPathSessionCommandSender,
    commands_rx: TcpPathSessionCommandReceivers,
    attached_streams: HashSet<StreamId>,
    session_id: Option<SessionId>,
    path_id: Option<PathId>,
    path_capabilities: Option<crate::protocol::PathCapabilities>,
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

enum ServerTcpPathEvent {
    Frame(Frame),
    Command(TcpPathSessionCommand),
}

async fn recv_server_tcp_path_event(
    path_frames: &mut mpsc::Receiver<Result<Frame, EncryptedFramedTransportError>>,
    commands_rx: &mut TcpPathSessionCommandReceivers,
) -> Result<Option<ServerTcpPathEvent>, RuntimeError> {
    loop {
        let command_may_recv = !tcp_path_receivers_closed(commands_rx);
        tokio::select! {
            biased;
            frame = path_frames.recv() => {
                return match frame {
                    Some(Ok(frame)) => Ok(Some(ServerTcpPathEvent::Frame(frame))),
                    Some(Err(err)) => Err(RuntimeError::Encrypted(err)),
                    None => Err(RuntimeError::TcpPathSessionClosed),
                };
            }
            command = recv_tcp_path_command(commands_rx), if command_may_recv => {
                match command {
                    Some(command) => return Ok(Some(ServerTcpPathEvent::Command(command))),
                    None if tcp_path_receivers_closed(commands_rx) => return Ok(None),
                    None => continue,
                }
            }
        }
    }
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
        let (commands_tx, commands_rx) =
            tcp_path_session_command_channels(udp_stream_path_command_queue(context.mux_limits));
        Ok(Self {
            peer,
            encrypted,
            context,
            authenticator,
            state: ServerUdpPathState::AwaitSessionHello,
            flows: Vec::new(),
            commands_tx,
            commands_rx,
            attached_streams: HashSet::new(),
            session_id: None,
            path_id: None,
            path_capabilities: None,
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
            (ServerUdpPathState::Established, Frame::SessionHello { session_id })
                if Some(session_id) == self.session_id =>
            {
                self.send_established_udp_path_ready().await?;
                Ok(ServerUdpSessionOutcome::Active)
            }
            (_, Frame::SessionHello { session_id }) => {
                self.flows.clear();
                self.attached_streams.clear();
                self.session_id = None;
                self.path_id = None;
                self.path_capabilities = None;
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
                ServerUdpPathState::Established,
                Frame::SessionAuth {
                    session_id: auth_session_id,
                    nonce,
                    auth_tag,
                },
            ) if Some(auth_session_id) == self.session_id
                && self
                    .authenticator
                    .verify_session_auth(auth_session_id, nonce, auth_tag) =>
            {
                self.send_established_udp_path_ready().await?;
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
                self.session_id = Some(*session_id);
                self.path_id = Some(path_id);
                self.path_capabilities = Some(capabilities);
                self.state = ServerUdpPathState::Established;
                Ok(ServerUdpSessionOutcome::Active)
            }
            (
                ServerUdpPathState::Established,
                Frame::PathJoin {
                    session_id: join_session_id,
                    path_id,
                    underlay,
                    nonce,
                    capabilities,
                    auth_tag,
                },
            ) if Some(join_session_id) == self.session_id
                && Some(path_id) == self.path_id
                && underlay == UnderlayProtocol::Udp
                && self.authenticator.verify_path_join(
                    join_session_id,
                    path_id,
                    underlay,
                    nonce,
                    capabilities,
                    auth_tag,
                ) =>
            {
                self.path_capabilities = Some(capabilities);
                self.send_established_udp_path_ready().await?;
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
                Frame::OpenStream {
                    stream_id,
                    target,
                    class,
                    ..
                },
            ) => {
                let (session_id, path_id, capabilities) = self.established_stream_context()?;
                outbound::validate_target(&target)?;
                self.context.outbound.ensure_supports(TargetProtocol::Tcp)?;
                match self.context.tcp_streams.open_or_attach(
                    ServerTcpStreamOpenRequest {
                        session_id,
                        stream_id,
                        target: &target,
                        class,
                        attachment: ServerTcpPathAttachment {
                            path_id,
                            underlay: UnderlayProtocol::Udp,
                            commands: self.commands_tx.clone(),
                            max_frame_payload_bytes: udp_stream_frame_payload_bytes(
                                self.context.mux_limits,
                            ),
                        },
                    },
                    self.context.mux_limits,
                    self.context.max_tcp_streams,
                )? {
                    ServerTcpStreamOpen::New(stream) => {
                        self.attached_streams.insert(stream_id);
                        let stream_context = self.context.clone();
                        tokio::spawn(async move {
                            if let Err(err) =
                                run_server_tcp_stream(stream_context, session_id, stream, target)
                                    .await
                            {
                                eprintln!("warning: server TCP stream failed: {err}");
                            }
                        });
                    }
                    ServerTcpStreamOpen::Existing => {
                        self.attached_streams.insert(stream_id);
                        self.context
                            .tcp_streams
                            .route_frame(
                                session_id,
                                stream_id,
                                Frame::PathStatus {
                                    path_id,
                                    status: crate::protocol::PathStatus::Active,
                                    capabilities,
                                },
                            )
                            .await?;
                        self.encrypted
                            .send_frame_to(
                                &Frame::StreamMaxData {
                                    stream_id,
                                    max_offset: self.context.mux_limits.max_stream_window_bytes,
                                },
                                self.peer,
                            )
                            .await?;
                    }
                }
                Ok(ServerUdpSessionOutcome::Active)
            }
            (
                ServerUdpPathState::Established,
                Frame::StreamData {
                    stream_id,
                    offset,
                    flags,
                    payload,
                },
            ) => {
                let (session_id, _, _) = self.established_stream_context()?;
                self.context
                    .tcp_streams
                    .route_frame(
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
                Ok(ServerUdpSessionOutcome::Active)
            }
            (ServerUdpPathState::Established, Frame::StreamAck { stream_id, ranges }) => {
                let (session_id, _, _) = self.established_stream_context()?;
                self.context
                    .tcp_streams
                    .route_frame(
                        session_id,
                        stream_id,
                        Frame::StreamAck { stream_id, ranges },
                    )
                    .await?;
                Ok(ServerUdpSessionOutcome::Active)
            }
            (
                ServerUdpPathState::Established,
                Frame::StreamMaxData {
                    stream_id,
                    max_offset,
                },
            ) => {
                let (session_id, _, _) = self.established_stream_context()?;
                self.context
                    .tcp_streams
                    .route_frame(
                        session_id,
                        stream_id,
                        Frame::StreamMaxData {
                            stream_id,
                            max_offset,
                        },
                    )
                    .await?;
                Ok(ServerUdpSessionOutcome::Active)
            }
            (ServerUdpPathState::Established, Frame::StreamFin { stream_id }) => {
                let (session_id, _, _) = self.established_stream_context()?;
                self.context
                    .tcp_streams
                    .route_frame(session_id, stream_id, Frame::StreamFin { stream_id })
                    .await?;
                Ok(ServerUdpSessionOutcome::Active)
            }
            (ServerUdpPathState::Established, Frame::StreamReset { stream_id, reason }) => {
                let (session_id, _, _) = self.established_stream_context()?;
                self.context
                    .tcp_streams
                    .route_frame(
                        session_id,
                        stream_id,
                        Frame::StreamReset { stream_id, reason },
                    )
                    .await?;
                Ok(ServerUdpSessionOutcome::Active)
            }
            (ServerUdpPathState::Established, Frame::StreamDetach { stream_id }) => {
                let (session_id, path_id, _) = self.established_stream_context()?;
                self.attached_streams.remove(&stream_id);
                self.context.tcp_streams.detach_path(
                    session_id,
                    stream_id,
                    UnderlayProtocol::Udp,
                    path_id,
                    &self.commands_tx,
                );
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
                let requests = spawn_server_udp_datagram_flow_worker(
                    flow_id,
                    outbound_socket,
                    self.commands_tx.clone(),
                    self.context.mux_limits,
                );
                self.flows.push(ServerUdpDatagramFlow { flow_id, requests });
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
                let requests = self
                    .flows
                    .get(flow_index)
                    .ok_or(RuntimeError::Protocol("unknown UDP datagram flow"))?
                    .requests
                    .clone();
                match requests.try_send(ServerUdpDatagramRequest {
                    datagram_id,
                    ttl_ms,
                    payload,
                }) {
                    Ok(()) => {
                        self.encrypted
                            .send_frame_to(
                                &Frame::DatagramFeedback {
                                    flow_id,
                                    received: vec![datagram_ack_range(datagram_id)?],
                                },
                                self.peer,
                            )
                            .await?;
                    }
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        eprintln!("warning: UDP datagram worker queue full; dropping request");
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        self.flows.retain(|flow| flow.flow_id != flow_id);
                        self.encrypted
                            .send_frame_to(&Frame::DatagramClose { flow_id }, self.peer)
                            .await?;
                    }
                }
                Ok(ServerUdpSessionOutcome::Active)
            }
            (ServerUdpPathState::Established, Frame::DatagramFeedback { .. }) => {
                Ok(ServerUdpSessionOutcome::Active)
            }
            (ServerUdpPathState::Established, Frame::DatagramClose { flow_id }) => {
                self.flows.retain(|flow| flow.flow_id != flow_id);
                if self.flows.is_empty() && self.attached_streams.is_empty() {
                    Ok(ServerUdpSessionOutcome::Closed)
                } else {
                    Ok(ServerUdpSessionOutcome::Active)
                }
            }
            (_, Frame::SessionClose { .. }) => Ok(ServerUdpSessionOutcome::Closed),
            _ => Err(RuntimeError::Protocol("unexpected UDP datagram path frame")),
        }
    }

    async fn handle_command(
        &mut self,
        command: TcpPathSessionCommand,
    ) -> Result<ServerUdpSessionOutcome, RuntimeError> {
        match command {
            TcpPathSessionCommand::SendFrame(frame) => {
                self.encrypted.send_frame_to(&frame, self.peer).await?;
                Ok(ServerUdpSessionOutcome::Active)
            }
            TcpPathSessionCommand::CloseStream(stream_id) => {
                let (session_id, path_id, _) = self.established_stream_context()?;
                self.attached_streams.remove(&stream_id);
                self.context.tcp_streams.detach_path(
                    session_id,
                    stream_id,
                    UnderlayProtocol::Udp,
                    path_id,
                    &self.commands_tx,
                );
                if self.flows.is_empty() && self.attached_streams.is_empty() {
                    Ok(ServerUdpSessionOutcome::Closed)
                } else {
                    Ok(ServerUdpSessionOutcome::Active)
                }
            }
            TcpPathSessionCommand::OpenStream { .. } => Err(RuntimeError::Protocol(
                "server UDP path received client open command",
            )),
        }
    }

    fn established_stream_context(
        &self,
    ) -> Result<(SessionId, PathId, crate::protocol::PathCapabilities), RuntimeError> {
        let session_id = self
            .session_id
            .ok_or(RuntimeError::Protocol("UDP stream path missing session id"))?;
        let path_id = self
            .path_id
            .ok_or(RuntimeError::Protocol("UDP stream path missing path id"))?;
        let capabilities = self.path_capabilities.ok_or(RuntimeError::Protocol(
            "UDP stream path missing path capabilities",
        ))?;
        Ok((session_id, path_id, capabilities))
    }

    async fn send_established_udp_path_ready(&mut self) -> Result<(), RuntimeError> {
        let (_, path_id, capabilities) = self.established_stream_context()?;
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
        Ok(())
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
    use crate::config::SharedSecret;
    use crate::transport::Endpoint;
    use crate::transport::tcp::bind_listener;
    use tokio::io::duplex;

    fn security() -> SecurityConfig {
        SecurityConfig::encrypted(SharedSecret::new(b"0123456789abcdef".to_vec()).expect("secret"))
    }

    fn udp_candidate_indices(
        context: &ClientPathContext,
        payload_bytes: usize,
        ttl_ms: u32,
    ) -> Vec<usize> {
        context
            .ordered_udp_path_candidates_for_ttl(payload_bytes, ttl_ms)
            .into_iter()
            .map(|candidate| candidate.path_index)
            .collect()
    }

    fn tcp_auto_bulk_discovery_indices(
        context: &ClientPathContext,
        current_path_index: Option<usize>,
        payload_bytes: usize,
    ) -> Vec<usize> {
        context
            .ordered_tcp_auto_bulk_discovery_scores(current_path_index, payload_bytes)
            .into_iter()
            .map(|(index, _)| index)
            .collect()
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

    async fn spawn_udp_reordered_echo_target() -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.expect("target bind"));
        let addr = socket.local_addr().expect("target addr");
        let handle = tokio::spawn(async move {
            let mut delayed = tokio::task::JoinSet::new();
            let mut buf = [0u8; 16];
            for _ in 0..2 {
                let (len, peer) = socket.recv_from(&mut buf).await.expect("target recv");
                match &buf[..len] {
                    b"slow" => {
                        let socket = socket.clone();
                        delayed.spawn(async move {
                            tokio::time::sleep(Duration::from_millis(500)).await;
                            socket
                                .send_to(b"slow-pong", peer)
                                .await
                                .expect("target delayed send");
                        });
                    }
                    b"fast" => {
                        socket
                            .send_to(b"fast-pong", peer)
                            .await
                            .expect("target fast send");
                    }
                    payload => panic!("unexpected UDP payload: {payload:?}"),
                }
            }
            while let Some(result) = delayed.join_next().await {
                result.expect("delayed target response");
            }
        });
        (addr, handle)
    }

    async fn spawn_udp_drop_first_echo_target() -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let socket = UdpSocket::bind("127.0.0.1:0").await.expect("target bind");
        let addr = socket.local_addr().expect("target addr");
        let handle = tokio::spawn(async move {
            let mut buf = [0u8; 16];
            let (len, _peer) = socket.recv_from(&mut buf).await.expect("first recv");
            assert_eq!(&buf[..len], b"ping");
            let (len, peer) = socket.recv_from(&mut buf).await.expect("retry recv");
            assert_eq!(&buf[..len], b"ping");
            socket.send_to(b"pong", peer).await.expect("target send");
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
        spawn_server_path_count(outbound, 1).await
    }

    async fn spawn_server_path_count(
        outbound: OutboundConfig,
        count: usize,
    ) -> (PathSpec, tokio::task::JoinHandle<Result<(), RuntimeError>>) {
        let path = reserve_tcp_path().await;
        let listener = bind_listener(&path).await.expect("bind");
        let handle = tokio::spawn(async move {
            let context = server_context(outbound);
            let mut sessions = tokio::task::JoinSet::new();
            for _ in 0..count {
                let (stream, _) = listener.accept().await.expect("accept");
                let session_context = context.clone();
                sessions.spawn(async move { handle_server_path(stream, session_context).await });
            }
            while let Some(session) = sessions.join_next().await {
                session.map_err(RuntimeError::TaskJoin)??;
            }
            Ok(())
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
                    }
                    frame => match session_ref.handle_frame(frame).await? {
                        ServerUdpSessionOutcome::Active => {}
                        ServerUdpSessionOutcome::Closed => return Ok(()),
                    },
                }
            }
        })
    }

    async fn spawn_udp_datagram_stale_then_matching_response_path(
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
            let mut sent_response_pair = false;
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
                    } if !sent_response_pair => {
                        let stale_datagram_id = DatagramId(if datagram_id.0 == u64::MAX {
                            datagram_id.0 - 1
                        } else {
                            datagram_id.0 + 1
                        });
                        sent_response_pair = true;
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
                        session_ref
                            .encrypted
                            .send_frame_to(
                                &Frame::DatagramData {
                                    flow_id,
                                    datagram_id: stale_datagram_id,
                                    ttl_ms: DEFAULT_SOCKS5_UDP_TTL_MS,
                                    payload: Bytes::from_static(b"stale"),
                                },
                                session_ref.peer,
                            )
                            .await?;
                        session_ref
                            .encrypted
                            .send_frame_to(
                                &Frame::DatagramData {
                                    flow_id,
                                    datagram_id,
                                    ttl_ms: DEFAULT_SOCKS5_UDP_TTL_MS,
                                    payload: Bytes::from_static(b"pong"),
                                },
                                session_ref.peer,
                            )
                            .await?;
                    }
                    frame => match session_ref.handle_frame(frame).await? {
                        ServerUdpSessionOutcome::Active => {}
                        ServerUdpSessionOutcome::Closed => return Ok(()),
                    },
                }
            }
        })
    }

    #[tokio::test]
    async fn server_udp_path_tolerates_duplicate_established_handshake_frames() {
        let socket = Arc::new(
            UdpSocket::bind("127.0.0.1:0")
                .await
                .expect("server udp bind"),
        );
        let peer = UdpSocket::bind("127.0.0.1:0").await.expect("peer udp bind");
        let peer_addr = peer.local_addr().expect("peer addr");
        let context = server_context(OutboundConfig::Direct);
        let mut session =
            ServerUdpPathSession::new(socket, peer_addr, context).expect("server udp path session");
        let path = "udp://127.0.0.1:7443".parse::<PathSpec>().expect("path");
        let session_id = SessionId(77);
        let path_id = PathId(2);
        let (hello, auth, join) = authenticated_path_join_frames_for_session(
            &security(),
            &path,
            path_id,
            UnderlayProtocol::Udp,
            session_id,
        )
        .expect("auth frames");

        session.handle_frame(hello.clone()).await.expect("hello");
        session.handle_frame(auth.clone()).await.expect("auth");
        session.handle_frame(join.clone()).await.expect("join");
        assert!(matches!(session.state, ServerUdpPathState::Established));
        assert_eq!(session.session_id, Some(session_id));
        assert_eq!(session.path_id, Some(path_id));

        session
            .handle_frame(hello)
            .await
            .expect("duplicate hello should be idempotent");
        session
            .handle_frame(auth)
            .await
            .expect("duplicate auth should be idempotent");
        session
            .handle_frame(join)
            .await
            .expect("duplicate join should be idempotent");
        assert!(matches!(session.state, ServerUdpPathState::Established));
        assert_eq!(session.session_id, Some(session_id));
        assert_eq!(session.path_id, Some(path_id));
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

        assert!(tcp_relay_can_read_with_limit(
            &send_stream,
            mux_limits.max_tcp_path_inflight_bytes
        ));
        assert_eq!(
            tcp_relay_read_budget_with_limit(
                &send_stream,
                mux_limits,
                mux_limits.max_tcp_path_inflight_bytes,
                64 * 1024
            ),
            32 * 1024
        );

        send_stream
            .send_data(Bytes::from(vec![0u8; 8 * 1024]), StreamFlags::NONE)
            .expect("first send");
        assert_eq!(
            tcp_relay_read_budget_with_limit(
                &send_stream,
                mux_limits,
                mux_limits.max_tcp_path_inflight_bytes,
                64 * 1024
            ),
            24 * 1024
        );

        send_stream
            .send_data(Bytes::from(vec![0u8; 24 * 1024]), StreamFlags::NONE)
            .expect("second send");
        assert!(!tcp_relay_can_read_with_limit(
            &send_stream,
            mux_limits.max_tcp_path_inflight_bytes
        ));
        assert_eq!(
            tcp_relay_read_budget_with_limit(
                &send_stream,
                mux_limits,
                mux_limits.max_tcp_path_inflight_bytes,
                64 * 1024
            ),
            0
        );

        send_stream.apply_ack(&[crate::protocol::OffsetRange {
            start: 0,
            end: 8 * 1024,
        }]);
        assert!(tcp_relay_can_read_with_limit(
            &send_stream,
            mux_limits.max_tcp_path_inflight_bytes
        ));
        assert_eq!(
            tcp_relay_read_budget_with_limit(
                &send_stream,
                mux_limits,
                mux_limits.max_tcp_path_inflight_bytes,
                64 * 1024
            ),
            8 * 1024
        );

        mux_limits.max_tcp_path_inflight_bytes = 64 * 1024;
        assert_eq!(
            tcp_relay_read_budget_with_limit(
                &send_stream,
                mux_limits,
                mux_limits.max_tcp_path_inflight_bytes,
                16 * 1024
            ),
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
    fn tcp_path_command_queue_tracks_inflight_budget_not_stream_limit() {
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
        assert_eq!(tcp_path_command_queue(mux_limits), 20);

        let resources = ResourceLimits {
            max_streams: 65_536,
            max_tcp_path_inflight_bytes: mux_limits.max_tcp_path_inflight_bytes,
            max_tcp_relay_chunk_bytes: mux_limits.max_tcp_relay_chunk_bytes,
            ..ResourceLimits::default()
        };
        assert_eq!(tcp_session_command_queue(resources), 20);
    }

    #[test]
    fn udp_stream_path_command_queue_tracks_udp_frame_budget() {
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
        let udp_queue = udp_stream_path_command_queue(mux_limits);

        assert!(udp_queue > tcp_path_command_queue(mux_limits));
        assert_eq!(udp_queue, tcp_path_session_frame_queue(mux_limits));
        assert!(
            udp_queue * udp_stream_frame_payload_bytes(mux_limits)
                >= mux_limits.max_tcp_relay_chunk_bytes
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
        let mut state = TcpRelayClassState::new();

        assert!(threshold >= (tcp_relay_buffer_len(mux_limits) as u64).saturating_mul(2));
        assert!(high_bdp_threshold < high_bdp / 4);
        assert!(high_bdp_threshold >= high_bdp / 8);

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
    fn reliable_stream_recv_progress_resend_tracks_received_state() {
        let mux_limits = MuxLimits::default();
        let mut recv_stream = ReliableRecvStream::new(StreamId(21), mux_limits);
        let low_latency = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 20.0, 30_000_000.0);
        let cross_continent =
            PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 900.0, 300_000_000.0);

        assert!(!tcp_relay_recv_progress_resend_active(&recv_stream, true));

        recv_stream
            .receive_data(1024, Bytes::from_static(b"late"), StreamFlags::NONE)
            .expect("out-of-order data");
        assert!(tcp_relay_recv_progress_resend_active(&recv_stream, true));
        assert!(!tcp_relay_recv_progress_resend_active(&recv_stream, false));

        let low_interval =
            reliable_stream_recv_progress_interval(Some(low_latency), TrafficClass::Interactive);
        let high_interval =
            reliable_stream_recv_progress_interval(Some(cross_continent), TrafficClass::Bulk);
        assert!(low_interval >= UDP_MIN_RESPONSE_TIMEOUT);
        assert!(low_interval <= TCP_STREAM_STALL_MIN_TIMEOUT);
        assert!(high_interval >= low_interval);
        assert!(high_interval <= TCP_STREAM_STALL_MIN_TIMEOUT);
    }

    #[test]
    fn reliable_recv_progress_batches_max_data_updates() {
        let mux_limits = MuxLimits {
            max_payload_bytes: 1024,
            max_tcp_relay_chunk_bytes: 1024,
            max_tcp_path_inflight_bytes: 4096,
            max_stream_window_bytes: 4096,
            max_repair_bytes: 4096,
            max_reorder_bytes: 4096,
            ..MuxLimits::default()
        };
        let mut recv_stream = ReliableRecvStream::new(StreamId(22), mux_limits);
        let mut progress = ReliableRecvProgress::default();
        let step = reliable_stream_max_data_update_bytes(mux_limits);

        assert_eq!(step, 1024);
        assert!(progress.should_send_max_data(&recv_stream, mux_limits, false));
        assert!(!progress.should_send_max_data(&recv_stream, mux_limits, false));

        recv_stream
            .receive_data(0, Bytes::from(vec![0x11; 512]), StreamFlags::NONE)
            .expect("half-step data");
        assert!(!progress.should_send_max_data(&recv_stream, mux_limits, false));

        recv_stream
            .receive_data(512, Bytes::from(vec![0x22; 512]), StreamFlags::NONE)
            .expect("full-step data");
        assert!(progress.should_send_max_data(&recv_stream, mux_limits, false));
        assert!(progress.should_send_max_data(&recv_stream, mux_limits, true));
    }

    #[test]
    fn tcp_relay_repair_replay_interval_tracks_inflight_pressure() {
        let mux_limits = MuxLimits::default();
        let light = tcp_relay_repair_replay_interval(PATH_OPEN_SCORE_BYTES, mux_limits);
        let full =
            tcp_relay_repair_replay_interval(mux_limits.max_tcp_path_inflight_bytes, mux_limits);
        let udp_full =
            udp_stream_repair_replay_interval(mux_limits.max_tcp_path_inflight_bytes, mux_limits);

        assert!(light >= TCP_STREAM_STALL_MIN_TIMEOUT);
        assert!(light < full);
        assert_eq!(full, TCP_STREAM_STALL_MAX_TIMEOUT);
        assert_eq!(udp_full, TCP_STREAM_STALL_MIN_TIMEOUT);
        assert!(full < Duration::from_secs(5));
    }

    #[test]
    fn udp_stream_congestion_self_clocks_and_cuts_back_on_repair_timeout() {
        let mux_limits = MuxLimits {
            max_tcp_path_inflight_bytes: 64 * 1024,
            max_tcp_relay_chunk_bytes: 64 * 1024,
            max_payload_bytes: 64 * 1024,
            ..MuxLimits::default()
        };
        let mss = udp_stream_frame_payload_bytes(mux_limits);
        let mut congestion = UdpStreamCongestion::new(mux_limits);
        let initial = congestion.inflight_limit();

        assert_eq!(initial, mss.saturating_mul(10).min(64 * 1024));
        assert_eq!(congestion.repair_budget(0), 0);
        assert_eq!(congestion.repair_budget(mss / 2), mss);

        congestion.on_ack(mss * 4);
        assert!(congestion.inflight_limit() > initial);

        for _ in 0..32 {
            congestion.on_ack(64 * 1024);
        }
        assert_eq!(congestion.inflight_limit(), 64 * 1024);

        congestion.on_repair_timeout();
        assert!(congestion.inflight_limit() < 64 * 1024);
        assert!(congestion.inflight_limit() >= udp_stream_min_cwnd_bytes(mss).min(64 * 1024));
    }

    #[test]
    fn udp_stream_repair_replay_uses_measured_ack_rtt() {
        let mux_limits = MuxLimits::default();
        let mss = udp_stream_frame_payload_bytes(mux_limits);
        let mut congestion = UdpStreamCongestion::new(mux_limits);
        let fallback =
            udp_stream_repair_replay_interval(mux_limits.max_tcp_path_inflight_bytes, mux_limits);

        assert_eq!(
            congestion.repair_replay_interval(mux_limits.max_tcp_path_inflight_bytes, mux_limits),
            fallback
        );

        congestion.on_send(mss);
        let sample = congestion
            .pending_samples
            .front_mut()
            .expect("pending sample");
        sample.sent_at = sample
            .sent_at
            .checked_sub(Duration::from_millis(360))
            .expect("past sample");
        congestion.on_ack(mss);

        let high_rtt_interval =
            congestion.repair_replay_interval(mux_limits.max_tcp_path_inflight_bytes, mux_limits);
        assert!(high_rtt_interval > fallback);
        assert!(high_rtt_interval <= TCP_STREAM_STALL_MAX_TIMEOUT);
    }

    #[test]
    fn tcp_sole_survivor_reannounce_budget_stays_within_fluency_window() {
        let low_latency_budget =
            tcp_relay_sole_survivor_reannounce_attempts(TCP_STREAM_STALL_MIN_TIMEOUT);
        let max_timeout_budget =
            tcp_relay_sole_survivor_reannounce_attempts(TCP_STREAM_STALL_MAX_TIMEOUT);
        assert!(
            low_latency_budget > max_timeout_budget,
            "low-latency paths should get more quick repair probes"
        );
        assert!(TCP_STREAM_STALL_MAX_TIMEOUT * max_timeout_budget <= Duration::from_millis(4500));
        assert!(low_latency_budget <= 16);
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
            false,
            mux_limits
        ));
        assert!(!tcp_relay_stall_watch_active(
            &send_stream,
            &recv_stream,
            true,
            TrafficClass::Interactive,
            false,
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
            false,
            mux_limits
        ));
        send_stream.apply_ack(&[crate::protocol::OffsetRange { start: 0, end: 7 }]);
        assert!(!tcp_relay_stall_watch_active(
            &send_stream,
            &recv_stream,
            true,
            TrafficClass::Interactive,
            false,
            mux_limits
        ));
        assert!(tcp_relay_stall_watch_active(
            &send_stream,
            &recv_stream,
            true,
            TrafficClass::Interactive,
            true,
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
            false,
            mux_limits
        ));
        assert!(tcp_relay_stall_watch_active(
            &send_stream,
            &recv_stream,
            true,
            TrafficClass::Bulk,
            false,
            mux_limits
        ));
        assert!(!tcp_relay_stall_watch_active(
            &send_stream,
            &recv_stream,
            false,
            TrafficClass::Bulk,
            true,
            mux_limits
        ));

        let response_watch_bytes = tcp_relay_response_stall_watch_bytes(mux_limits);
        assert_eq!(
            response_watch_bytes,
            tcp_relay_buffer_len(mux_limits) as u64
        );
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
            false,
            mux_limits
        ));
    }

    #[test]
    fn tcp_response_stall_anchor_uses_delivery_progress_not_control_progress() {
        let mux_limits = MuxLimits::default();
        let mut recv_stream = ReliableRecvStream::new(StreamId(12), mux_limits);
        let last_delivery = Instant::now();
        let control_progress = last_delivery + Duration::from_secs(30);

        assert_eq!(
            tcp_relay_stall_progress_anchor(
                control_progress,
                last_delivery,
                last_delivery,
                &recv_stream,
                true,
                TrafficClass::Interactive,
                mux_limits,
            ),
            control_progress
        );

        let response_watch_bytes = tcp_relay_response_stall_watch_bytes(mux_limits);
        recv_stream
            .receive_data(
                0,
                Bytes::from(vec![0u8; response_watch_bytes as usize]),
                StreamFlags::NONE,
            )
            .expect("sustained response data");

        assert_eq!(
            tcp_relay_stall_progress_anchor(
                control_progress,
                last_delivery,
                last_delivery,
                &recv_stream,
                true,
                TrafficClass::Interactive,
                mux_limits,
            ),
            last_delivery
        );

        let repair_progress = control_progress + Duration::from_secs(1);
        assert_eq!(
            tcp_relay_stall_progress_anchor(
                control_progress,
                last_delivery,
                repair_progress,
                &recv_stream,
                true,
                TrafficClass::Interactive,
                mux_limits,
            ),
            repair_progress
        );
    }

    #[test]
    fn tcp_receive_hole_repair_tracks_buffered_ordering_gap() {
        let mux_limits = MuxLimits::default();
        let mut recv_stream = ReliableRecvStream::new(StreamId(14), mux_limits);

        assert!(!tcp_relay_receive_hole_repair_active(&recv_stream, true));
        recv_stream
            .receive_data(0, Bytes::from_static(b"head"), StreamFlags::NONE)
            .expect("initial response data");
        assert!(!tcp_relay_receive_hole_repair_active(&recv_stream, true));

        let out_of_order = recv_stream
            .receive_data(8, Bytes::from_static(b"tail"), StreamFlags::NONE)
            .expect("out-of-order response data");
        assert!(out_of_order.delivered.is_empty());
        assert!(tcp_relay_receive_hole_repair_active(&recv_stream, true));
        assert!(!tcp_relay_receive_hole_repair_active(&recv_stream, false));

        let hole_fill = recv_stream
            .receive_data(4, Bytes::from_static(b"gap!"), StreamFlags::NONE)
            .expect("hole fill response data");
        assert_eq!(hole_fill.delivered.len(), 2);
        assert!(!tcp_relay_receive_hole_repair_active(&recv_stream, true));
    }

    #[test]
    fn tcp_receive_hole_victim_prefers_worst_score_then_stale_delivery() {
        let now = Instant::now();
        let low_latency_path = "tcp://127.0.0.1:10028?srtt-ms=5&rate-mbps=100"
            .parse::<PathSpec>()
            .expect("low latency path");
        let stale_but_fast_path = "tcp://127.0.0.1:10029?srtt-ms=10&rate-mbps=100"
            .parse::<PathSpec>()
            .expect("stale but fast path");
        let slow_path = "tcp://127.0.0.1:10030?srtt-ms=300&rate-mbps=5"
            .parse::<PathSpec>()
            .expect("slow path");
        let context = ClientPathContext::new(
            vec![low_latency_path, stale_but_fast_path, slow_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("ctx");
        let key = |index| RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index,
        };
        let mut path_last_delivery_at = HashMap::from([
            (key(0), now - Duration::from_secs(1)),
            (key(1), now - Duration::from_secs(3)),
            (key(2), now - Duration::from_secs(2)),
        ]);

        assert_eq!(
            tcp_relay_receive_hole_victim(
                &context,
                &[key(0), key(1), key(2)],
                TrafficClass::Bulk,
                64 * 1024,
                &path_last_delivery_at
            ),
            Some(key(2))
        );

        tcp_relay_refresh_path_tracking(&mut path_last_delivery_at, &[key(0), key(2), key(3)], now);
        assert!(!path_last_delivery_at.contains_key(&key(1)));
        assert_eq!(path_last_delivery_at.get(&key(3)), Some(&now));
        assert_eq!(
            tcp_relay_receive_hole_victim(
                &context,
                &[key(0), key(1)],
                TrafficClass::Bulk,
                64 * 1024,
                &path_last_delivery_at
            ),
            Some(key(1))
        );
        assert_eq!(
            tcp_relay_receive_hole_victim(
                &context,
                &[key(3)],
                TrafficClass::Bulk,
                64 * 1024,
                &path_last_delivery_at
            ),
            None
        );
    }

    #[test]
    fn tcp_relay_attach_scoring_keeps_interactive_repairs_small() {
        let mux_limits = MuxLimits::default();
        let send_stream = ReliableSendStream::new(StreamId(12), mux_limits);

        assert_eq!(
            tcp_relay_attach_payload_bytes(&send_stream, TrafficClass::Interactive, mux_limits),
            PATH_OPEN_SCORE_BYTES
        );
        assert_eq!(
            tcp_relay_attach_payload_bytes(&send_stream, TrafficClass::Bulk, mux_limits),
            tcp_relay_buffer_len(mux_limits)
        );
    }

    #[test]
    fn tcp_path_sessions_are_dedicated_for_latency_sensitive_classes() {
        assert!(tcp_path_class_uses_dedicated_session(
            TrafficClass::Interactive
        ));
        assert!(tcp_path_class_uses_dedicated_session(TrafficClass::Control));
        assert!(!tcp_path_class_uses_dedicated_session(TrafficClass::Bulk));
        assert!(!tcp_path_class_uses_dedicated_session(
            TrafficClass::Background
        ));
    }

    #[tokio::test]
    async fn server_tcp_binding_reselects_blocked_data_send_after_path_update() {
        let (old_tx, _old_rx) = tcp_path_session_command_channels(1);
        old_tx
            .send_frame(
                Frame::StreamData {
                    stream_id: StreamId(99),
                    offset: 0,
                    flags: StreamFlags::NONE,
                    payload: Bytes::from_static(b"fill"),
                },
                TrafficClass::Interactive,
            )
            .await
            .expect("fill old path priority command queue");
        let binding = ServerTcpStreamBinding::new(
            UnderlayProtocol::Tcp,
            PathId(0),
            old_tx,
            TrafficClass::Interactive,
        );
        let send_binding = binding.clone();
        let send_task = tokio::spawn(async move {
            send_binding
                .send_frame(
                    StreamId(7),
                    TrafficClass::Bulk,
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

        let (new_tx, mut new_rx) = tcp_path_session_command_channels(1);
        binding.attach(UnderlayProtocol::Tcp, PathId(1), new_tx, TrafficClass::Bulk);
        assert_eq!(binding.class(), TrafficClass::Bulk);
        send_task
            .await
            .expect("binding send join")
            .expect("binding send");
        match recv_tcp_path_command(&mut new_rx)
            .await
            .expect("new path command")
        {
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
    async fn server_tcp_binding_reattach_promotes_existing_path_for_data() {
        let (path0_initial_tx, _path0_initial_rx) = tcp_path_session_command_channels(1);
        let binding = ServerTcpStreamBinding::new(
            UnderlayProtocol::Tcp,
            PathId(0),
            path0_initial_tx,
            TrafficClass::Interactive,
        );
        let (path1_tx, mut path1_rx) = tcp_path_session_command_channels(1);
        binding.attach(
            UnderlayProtocol::Tcp,
            PathId(1),
            path1_tx,
            TrafficClass::Bulk,
        );
        let (path0_repair_tx, mut path0_repair_rx) = tcp_path_session_command_channels(1);
        binding.attach(
            UnderlayProtocol::Tcp,
            PathId(0),
            path0_repair_tx,
            TrafficClass::Bulk,
        );

        binding
            .send_frame(
                StreamId(7),
                TrafficClass::Bulk,
                Frame::StreamData {
                    stream_id: StreamId(7),
                    offset: 0,
                    flags: StreamFlags::NONE,
                    payload: Bytes::from_static(b"repair"),
                },
            )
            .await
            .expect("send on promoted repair path");

        match recv_tcp_path_command(&mut path0_repair_rx)
            .await
            .expect("path0 repair command")
        {
            TcpPathSessionCommand::SendFrame(Frame::StreamData { payload, .. }) => {
                assert_eq!(&payload[..], b"repair");
            }
            _ => panic!("expected data on promoted repair path"),
        }
        assert!(
            tokio::time::timeout(
                Duration::from_millis(20),
                recv_tcp_path_command(&mut path1_rx)
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn server_tcp_binding_keeps_tcp_and_udp_paths_with_same_id_separate() {
        let (tcp_tx, mut tcp_rx) = tcp_path_session_command_channels(4);
        let binding = ServerTcpStreamBinding::new(
            UnderlayProtocol::Tcp,
            PathId(0),
            tcp_tx,
            TrafficClass::Interactive,
        );
        let (udp_tx, mut udp_rx) = tcp_path_session_command_channels(4);
        binding.attach(UnderlayProtocol::Udp, PathId(0), udp_tx, TrafficClass::Bulk);

        binding.close_stream(StreamId(7)).await;

        match recv_tcp_path_command(&mut tcp_rx)
            .await
            .expect("tcp close command")
        {
            TcpPathSessionCommand::CloseStream(stream_id) => assert_eq!(stream_id, StreamId(7)),
            _ => panic!("expected TCP close stream command"),
        }
        match recv_tcp_path_command(&mut udp_rx)
            .await
            .expect("udp close command")
        {
            TcpPathSessionCommand::CloseStream(stream_id) => assert_eq!(stream_id, StreamId(7)),
            _ => panic!("expected UDP close stream command"),
        }
    }

    #[tokio::test]
    async fn tcp_path_control_command_bypasses_saturated_data_queue() {
        let (tx, mut rx) = tcp_path_session_command_channels(1);
        tx.send_frame(
            Frame::StreamData {
                stream_id: StreamId(3),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"queued-data"),
            },
            TrafficClass::Bulk,
        )
        .await
        .expect("fill data queue");

        tokio::time::timeout(
            Duration::from_millis(50),
            tx.send_control(TcpPathSessionCommand::CloseStream(StreamId(3))),
        )
        .await
        .expect("control send should not wait for data queue")
        .expect("control send");

        match recv_tcp_path_command(&mut rx).await.expect("first command") {
            TcpPathSessionCommand::CloseStream(stream_id) => assert_eq!(stream_id, StreamId(3)),
            _ => panic!("expected prioritized close stream control"),
        }
    }

    #[tokio::test]
    async fn tcp_path_interactive_frame_bypasses_saturated_bulk_queue() {
        let (tx, mut rx) = tcp_path_session_command_channels(1);
        tx.send_frame(
            Frame::StreamData {
                stream_id: StreamId(10),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"bulk"),
            },
            TrafficClass::Bulk,
        )
        .await
        .expect("fill bulk data queue");

        tokio::time::timeout(
            Duration::from_millis(50),
            tx.send_frame(
                Frame::StreamData {
                    stream_id: StreamId(11),
                    offset: 0,
                    flags: StreamFlags::NONE,
                    payload: Bytes::from_static(b"i"),
                },
                TrafficClass::Interactive,
            ),
        )
        .await
        .expect("interactive send should not wait for bulk queue")
        .expect("interactive send");

        match recv_tcp_path_command(&mut rx).await.expect("first command") {
            TcpPathSessionCommand::SendFrame(Frame::StreamData {
                stream_id, payload, ..
            }) => {
                assert_eq!(stream_id, StreamId(11));
                assert_eq!(&payload[..], b"i");
            }
            _ => panic!("expected prioritized interactive stream data"),
        }
    }

    #[tokio::test]
    async fn server_tcp_path_input_frame_bypasses_queued_bulk_output() {
        let (tx, mut commands_rx) = tcp_path_session_command_channels(1);
        tx.send_frame(
            Frame::StreamData {
                stream_id: StreamId(10),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"bulk"),
            },
            TrafficClass::Bulk,
        )
        .await
        .expect("fill bulk output command queue");
        let (frame_tx, mut path_frames) = mpsc::channel(1);
        frame_tx
            .send(Ok(Frame::Ping { nonce: 7 }))
            .await
            .expect("queue inbound ping");

        match recv_server_tcp_path_event(&mut path_frames, &mut commands_rx)
            .await
            .expect("server path event")
            .expect("event")
        {
            ServerTcpPathEvent::Frame(Frame::Ping { nonce }) => assert_eq!(nonce, 7),
            _ => panic!("expected inbound frame before queued bulk output"),
        }
    }

    #[tokio::test]
    async fn client_tcp_path_ignores_late_frames_for_recently_closed_stream() {
        let stream_id = StreamId(7);
        let (frames_tx, frames_rx) = mpsc::channel(1);
        let mut streams = HashMap::new();
        streams.insert(
            stream_id,
            ClientTcpPathStreamState {
                frames: frames_tx,
                pending_open: None,
            },
        );
        let mut closed_streams = RecentIdCache::new(8);
        drop(frames_rx);

        route_client_tcp_stream_frame(
            &mut streams,
            &mut closed_streams,
            stream_id,
            Frame::StreamFin { stream_id },
        )
        .await
        .expect("receiver close should mark stream drained");
        assert!(!streams.contains_key(&stream_id));
        assert!(closed_streams.contains(&stream_id));

        route_client_tcp_stream_frame(
            &mut streams,
            &mut closed_streams,
            stream_id,
            Frame::StreamAck {
                stream_id,
                ranges: Vec::new(),
            },
        )
        .await
        .expect("late frame for closed stream should be ignored");

        let unknown = StreamId(99);
        let err = route_client_tcp_stream_frame(
            &mut streams,
            &mut closed_streams,
            unknown,
            Frame::StreamFin { stream_id: unknown },
        )
        .await
        .expect_err("unknown stream should remain a protocol error");
        assert!(matches!(err, RuntimeError::Protocol(_)));
    }

    #[tokio::test]
    async fn server_tcp_registry_ignores_late_frames_for_recently_closed_stream() {
        let registry = ServerTcpStreamRegistry::new(8);
        let session_id = SessionId(11);
        let stream_id = StreamId(5);
        let (commands, _receivers) = tcp_path_session_command_channels(4);
        let target = TargetAddr::Domain {
            host: "example.com".to_string(),
            port: 443,
        };

        let opened = registry
            .open_or_attach(
                ServerTcpStreamOpenRequest {
                    session_id,
                    stream_id,
                    target: &target,
                    class: TrafficClass::Interactive,
                    attachment: ServerTcpPathAttachment {
                        path_id: PathId(0),
                        underlay: UnderlayProtocol::Tcp,
                        commands,
                        max_frame_payload_bytes: tcp_relay_buffer_len(MuxLimits::default()),
                    },
                },
                MuxLimits::default(),
                8,
            )
            .expect("server stream open");
        assert!(matches!(opened, ServerTcpStreamOpen::New(_)));

        registry.close(session_id, stream_id);
        registry
            .route_frame(session_id, stream_id, Frame::StreamFin { stream_id })
            .await
            .expect("late server stream frame should be ignored");

        let unknown = StreamId(99);
        let err = registry
            .route_frame(session_id, unknown, Frame::StreamFin { stream_id: unknown })
            .await
            .expect_err("unknown server stream should remain a protocol error");
        assert!(matches!(err, RuntimeError::Protocol(_)));
    }

    #[tokio::test]
    async fn server_tcp_relay_replays_response_repair_cache_on_path_reattach() {
        let mux_limits = MuxLimits::default();
        let stream_id = StreamId(42);
        let (mut target_peer, target_side) = duplex(4096);
        let (commands_tx, mut commands_rx) = tcp_path_session_command_channels(8);
        let (frames_tx, frames_rx) = mpsc::channel(8);
        let relay = tokio::spawn(relay_tcp_stream(
            target_side,
            TcpPathStream {
                stream_id,
                max_offset: mux_limits.max_stream_window_bytes,
                class: TrafficClass::Interactive,
                underlay: UnderlayProtocol::Tcp,
                max_frame_payload_bytes: tcp_relay_buffer_len(mux_limits),
                output: TcpPathStreamOutput::Fixed(commands_tx),
                frames: frames_rx,
            },
            mux_limits,
        ));

        target_peer
            .write_all(b"response")
            .await
            .expect("target write");
        let first = tokio::time::timeout(
            Duration::from_secs(1),
            recv_tcp_path_command(&mut commands_rx),
        )
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
        let replay = tokio::time::timeout(
            Duration::from_secs(1),
            recv_tcp_path_command(&mut commands_rx),
        )
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
        let suspect_order = context.ordered_tcp_path_indices(TrafficClass::Interactive, 512);
        assert_eq!(suspect_order, vec![0, 1]);
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

        context.mark_tcp_path_open_success(
            0,
            Duration::from_millis(120),
            TrafficClass::Interactive,
        );
        context.mark_tcp_path_open_success(1, Duration::from_millis(5), TrafficClass::Interactive);

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
            tcp_auto_bulk_discovery_indices(
                &context,
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
            tcp_auto_bulk_discovery_indices(
                &context,
                Some(0),
                MuxLimits::default().max_tcp_path_inflight_bytes,
            )
            .is_empty()
        );
    }

    #[test]
    fn bulk_repair_does_not_attach_worse_path_when_current_path_is_best() {
        let low_latency_path = "tcp://127.0.0.1:10128?srtt-ms=20&rate-mbps=30"
            .parse::<PathSpec>()
            .expect("low-latency path");
        let poor_path = "tcp://127.0.0.1:10129?srtt-ms=420&jitter-ms=120&rate-mbps=8"
            .parse::<PathSpec>()
            .expect("poor path");
        let context = ClientPathContext::new(
            vec![low_latency_path, poor_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("context");

        assert!(
            context
                .ordered_tcp_repair_path_indices(Some(0), TrafficClass::Bulk, 4 * 1024 * 1024)
                .is_empty()
        );
        assert_eq!(
            context.ordered_tcp_repair_path_indices(Some(1), TrafficClass::Bulk, 4 * 1024 * 1024),
            vec![0]
        );
        assert_eq!(
            context
                .ordered_tcp_repair_path_indices(Some(0), TrafficClass::Interactive, 512)
                .first()
                .copied(),
            Some(0)
        );
    }

    #[test]
    fn endpoint_only_tcp_bulk_discovery_waits_for_delivery_evidence_before_probe_noise() {
        let low_latency_path = "tcp://127.0.0.1:10132"
            .parse::<PathSpec>()
            .expect("low latency path");
        let high_bandwidth_path = "tcp://127.0.0.1:10133"
            .parse::<PathSpec>()
            .expect("high bandwidth path");
        let poor_path = "tcp://127.0.0.1:10134"
            .parse::<PathSpec>()
            .expect("poor path");
        let context = ClientPathContext::new(
            vec![low_latency_path, high_bandwidth_path, poor_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("context");

        context.mark_tcp_path_open_success(0, Duration::from_millis(20), TrafficClass::Interactive);
        context.mark_tcp_path_probe_success(2, Duration::from_millis(1));

        assert!(
            tcp_auto_bulk_discovery_indices(
                &context,
                Some(0),
                MuxLimits::default().max_tcp_path_inflight_bytes
            )
            .is_empty()
        );

        let now = Instant::now();
        context.mark_tcp_path_delivery(
            1,
            PathDeliveryStats {
                payload_bytes: 4 * 1024 * 1024,
                first_payload_at: Some(now),
                last_payload_at: Some(now + Duration::from_millis(40)),
            },
        );

        assert_eq!(
            tcp_auto_bulk_discovery_indices(
                &context,
                Some(0),
                MuxLimits::default().max_tcp_path_inflight_bytes,
            ),
            vec![1]
        );
    }

    #[test]
    fn endpoint_only_tcp_bulk_discovery_requires_delivery_under_concurrent_latency_demand() {
        let low_latency_path = "tcp://127.0.0.1:10146"
            .parse::<PathSpec>()
            .expect("low latency path");
        let high_bandwidth_path = "tcp://127.0.0.1:10147"
            .parse::<PathSpec>()
            .expect("high bandwidth path");
        let context = ClientPathContext::new(
            vec![low_latency_path, high_bandwidth_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("context");

        context.mark_tcp_path_open_success(0, Duration::from_millis(20), TrafficClass::Interactive);
        assert!(
            tcp_auto_bulk_discovery_indices(
                &context,
                Some(0),
                MuxLimits::default().max_tcp_path_inflight_bytes,
            )
            .is_empty()
        );

        context.mark_tcp_path_open_success(0, Duration::from_millis(20), TrafficClass::Interactive);
        let now = Instant::now();
        context.mark_tcp_path_delivery(
            1,
            PathDeliveryStats {
                payload_bytes: 4 * 1024 * 1024,
                first_payload_at: Some(now),
                last_payload_at: Some(now + Duration::from_millis(40)),
            },
        );
        assert_eq!(
            tcp_auto_bulk_discovery_indices(
                &context,
                Some(0),
                MuxLimits::default().max_tcp_path_inflight_bytes,
            ),
            vec![1]
        );
    }

    #[test]
    fn endpoint_only_udp_stream_startup_preserves_configured_order_on_probe_noise() {
        let first_path = "udp://127.0.0.1:10135"
            .parse::<PathSpec>()
            .expect("first path");
        let second_path = "udp://127.0.0.1:10136"
            .parse::<PathSpec>()
            .expect("second path");
        let context = ClientPathContext::new(
            vec![first_path, second_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("context");

        context.mark_udp_path_failure(0);
        context.mark_udp_path_probe_success(1, Duration::from_millis(1));

        assert_eq!(
            context
                .ordered_udp_stream_path_indices(TrafficClass::Interactive, PATH_OPEN_SCORE_BYTES),
            vec![0, 1]
        );

        context.mark_udp_path_failure(0);
        assert_eq!(
            context
                .ordered_udp_stream_path_indices(TrafficClass::Interactive, PATH_OPEN_SCORE_BYTES),
            vec![1]
        );
    }

    #[test]
    fn endpoint_only_udp_stream_auto_bulk_discovery_waits_for_delivery_evidence() {
        let low_latency_path = "udp://127.0.0.1:10137"
            .parse::<PathSpec>()
            .expect("low latency path");
        let high_bandwidth_path = "udp://127.0.0.1:10138"
            .parse::<PathSpec>()
            .expect("high bandwidth path");
        let poor_path = "udp://127.0.0.1:10139"
            .parse::<PathSpec>()
            .expect("poor path");
        let context = ClientPathContext::new(
            vec![low_latency_path, high_bandwidth_path, poor_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("context");

        context.mark_udp_path_probe_success(1, Duration::from_millis(1));
        assert!(
            context
                .ordered_udp_stream_auto_bulk_discovery_indices(
                    Some(0),
                    MuxLimits::default().max_tcp_path_inflight_bytes,
                )
                .is_empty()
        );

        let now = Instant::now();
        context.mark_udp_path_delivery(
            1,
            PathDeliveryStats {
                payload_bytes: 4 * 1024 * 1024,
                first_payload_at: Some(now),
                last_payload_at: Some(now + Duration::from_millis(40)),
            },
        );

        assert_eq!(
            context.ordered_udp_stream_auto_bulk_discovery_indices(
                Some(0),
                MuxLimits::default().max_tcp_path_inflight_bytes,
            ),
            vec![1]
        );
    }

    #[test]
    fn mixed_udp_repair_waits_for_delivery_evidence_on_active_tcp_stream() {
        let tcp_path = "tcp://127.0.0.1:10157"
            .parse::<PathSpec>()
            .expect("tcp path");
        let udp_low_latency_path = "udp://127.0.0.1:10158"
            .parse::<PathSpec>()
            .expect("udp low latency path");
        let udp_probe_only_path = "udp://127.0.0.1:10159"
            .parse::<PathSpec>()
            .expect("udp probe path");
        let context = ClientPathContext::new(
            vec![tcp_path, udp_low_latency_path, udp_probe_only_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("context");

        context.mark_udp_path_probe_success(1, Duration::from_millis(1));
        assert!(
            context
                .ordered_udp_stream_repair_path_indices(
                    None,
                    TrafficClass::Bulk,
                    MuxLimits::default().max_tcp_path_inflight_bytes,
                    true,
                )
                .is_empty()
        );
        assert_eq!(
            context
                .ordered_udp_stream_repair_path_indices(
                    None,
                    TrafficClass::Bulk,
                    MuxLimits::default().max_tcp_path_inflight_bytes,
                    false,
                )
                .first()
                .copied(),
            Some(1)
        );

        let now = Instant::now();
        context.mark_udp_path_delivery(
            1,
            PathDeliveryStats {
                payload_bytes: 4 * 1024 * 1024,
                first_payload_at: Some(now),
                last_payload_at: Some(now + Duration::from_millis(40)),
            },
        );

        assert_eq!(
            context.ordered_udp_stream_repair_path_indices(
                None,
                TrafficClass::Bulk,
                MuxLimits::default().max_tcp_path_inflight_bytes,
                true,
            ),
            vec![1]
        );
    }

    #[test]
    fn udp_repair_waits_for_delivery_evidence_on_active_endpoint_only_stream() {
        let udp_low_latency_path = "udp://127.0.0.1:10160"
            .parse::<PathSpec>()
            .expect("udp low latency path");
        let udp_probe_path = "udp://127.0.0.1:10161"
            .parse::<PathSpec>()
            .expect("udp probe path");
        let context = ClientPathContext::new(
            vec![udp_low_latency_path, udp_probe_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("context");

        context.mark_udp_path_probe_success(1, Duration::from_millis(1));
        assert!(
            context
                .ordered_udp_stream_repair_path_indices(
                    Some(0),
                    TrafficClass::Bulk,
                    MuxLimits::default().max_tcp_path_inflight_bytes,
                    true,
                )
                .is_empty()
        );
        assert_eq!(
            context
                .ordered_udp_stream_repair_path_indices(
                    Some(0),
                    TrafficClass::Bulk,
                    MuxLimits::default().max_tcp_path_inflight_bytes,
                    false,
                )
                .first()
                .copied(),
            Some(1)
        );

        let now = Instant::now();
        context.mark_udp_path_delivery(
            1,
            PathDeliveryStats {
                payload_bytes: 4 * 1024 * 1024,
                first_payload_at: Some(now),
                last_payload_at: Some(now + Duration::from_millis(40)),
            },
        );

        assert_eq!(
            context.ordered_udp_stream_repair_path_indices(
                Some(0),
                TrafficClass::Bulk,
                MuxLimits::default().max_tcp_path_inflight_bytes,
                true,
            ),
            vec![1]
        );
    }

    #[test]
    fn mixed_auto_bulk_discovery_can_cross_to_better_udp_carrier() {
        let tcp_path = "tcp://127.0.0.1:10140?srtt-ms=20&rate-mbps=30"
            .parse::<PathSpec>()
            .expect("tcp path");
        let udp_path = "udp://127.0.0.1:10141?srtt-ms=40&rate-mbps=300"
            .parse::<PathSpec>()
            .expect("udp path");
        let context = ClientPathContext::new(
            vec![tcp_path, udp_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("context");

        assert_eq!(
            context.ordered_reliable_auto_bulk_discovery_path_keys(
                Some(0),
                None,
                MuxLimits::default().max_tcp_path_inflight_bytes,
            ),
            vec![RelayPathKey {
                underlay: UnderlayProtocol::Udp,
                index: 0,
            }]
        );
    }

    #[test]
    fn mixed_auto_bulk_discovery_rejects_worse_udp_carrier() {
        let tcp_path = "tcp://127.0.0.1:10140?srtt-ms=20&rate-mbps=300"
            .parse::<PathSpec>()
            .expect("tcp path");
        let udp_path = "udp://127.0.0.1:10141?srtt-ms=180&rate-mbps=30"
            .parse::<PathSpec>()
            .expect("udp path");
        let context = ClientPathContext::new(
            vec![tcp_path, udp_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("context");

        assert_eq!(
            context.ordered_reliable_auto_bulk_discovery_path_keys(
                Some(0),
                None,
                MuxLimits::default().max_tcp_path_inflight_bytes,
            ),
            Vec::<RelayPathKey>::new()
        );
    }

    #[test]
    fn mixed_auto_bulk_discovery_can_choose_best_carrier_without_active_cohort() {
        let tcp_path = "tcp://127.0.0.1:10144?srtt-ms=20&rate-mbps=30"
            .parse::<PathSpec>()
            .expect("tcp path");
        let udp_path = "udp://127.0.0.1:10145?srtt-ms=40&rate-mbps=300"
            .parse::<PathSpec>()
            .expect("udp path");
        let context = ClientPathContext::new(
            vec![tcp_path, udp_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("context");

        assert_eq!(
            context
                .ordered_reliable_auto_bulk_discovery_path_keys(
                    None,
                    None,
                    MuxLimits::default().max_tcp_path_inflight_bytes,
                )
                .first()
                .copied(),
            Some(RelayPathKey {
                underlay: UnderlayProtocol::Udp,
                index: 0,
            })
        );
    }

    #[test]
    fn relay_candidate_filter_preserves_current_carrier_cohort() {
        let tcp = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 1,
        };
        let udp = RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 2,
        };

        assert_eq!(
            relay_path_candidates_for_active_carrier(vec![udp, tcp], Some(UnderlayProtocol::Tcp)),
            vec![tcp]
        );
        assert_eq!(
            relay_path_candidates_for_active_carrier(vec![tcp, udp], Some(UnderlayProtocol::Udp)),
            vec![udp]
        );
        assert_eq!(
            relay_path_candidates_for_active_carrier(vec![tcp, udp], None),
            vec![tcp, udp]
        );
    }

    #[tokio::test]
    async fn mixed_relay_current_carrier_tracks_latest_data_path() {
        fn opened_relay_stream_for_test(
            underlay: UnderlayProtocol,
            path_index: usize,
        ) -> (
            OpenedRemoteStream,
            TcpPathSessionCommandReceivers,
            mpsc::Sender<Result<Frame, RuntimeError>>,
        ) {
            let (commands, command_rx) = tcp_path_session_command_channels(4);
            let (frames_tx, frames_rx) = mpsc::channel(4);
            (
                OpenedRemoteStream {
                    path_index,
                    stream: TcpPathStream {
                        stream_id: StreamId(44),
                        max_offset: MuxLimits::default().max_stream_window_bytes,
                        class: TrafficClass::Bulk,
                        underlay,
                        max_frame_payload_bytes: tcp_relay_buffer_len(MuxLimits::default()),
                        output: TcpPathStreamOutput::Fixed(commands),
                        frames: frames_rx,
                    },
                },
                command_rx,
                frames_tx,
            )
        }

        let (tcp_stream, _tcp_commands, _tcp_frames) =
            opened_relay_stream_for_test(UnderlayProtocol::Tcp, 0);
        let mut remotes = TcpRelayRemoteSet::new(tcp_stream, 4);
        assert_eq!(
            remotes.active_carrier_underlay(),
            Some(UnderlayProtocol::Tcp)
        );

        let (udp_stream, _udp_commands, _udp_frames) =
            opened_relay_stream_for_test(UnderlayProtocol::Udp, 1);
        remotes.attach(udp_stream);
        assert_eq!(
            remotes.active_carrier_underlay(),
            Some(UnderlayProtocol::Udp)
        );

        assert_eq!(
            relay_path_candidates_for_active_carrier(
                vec![
                    RelayPathKey {
                        underlay: UnderlayProtocol::Tcp,
                        index: 0,
                    },
                    RelayPathKey {
                        underlay: UnderlayProtocol::Udp,
                        index: 2,
                    },
                ],
                remotes.active_carrier_underlay(),
            ),
            vec![RelayPathKey {
                underlay: UnderlayProtocol::Udp,
                index: 2,
            }]
        );
    }

    #[test]
    fn mixed_auto_bulk_discovery_does_not_attach_unmeasured_endpoint_only_udp() {
        let tcp_path = "tcp://127.0.0.1:10142"
            .parse::<PathSpec>()
            .expect("tcp path");
        let udp_path = "udp://127.0.0.1:10143"
            .parse::<PathSpec>()
            .expect("udp path");
        let context = ClientPathContext::new(
            vec![tcp_path, udp_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("context");

        context.mark_udp_path_probe_success(0, Duration::from_millis(1));

        assert!(
            context
                .ordered_reliable_auto_bulk_discovery_path_keys(
                    Some(0),
                    None,
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
            udp_candidate_indices(&context, 1024 * 1024, DEFAULT_SOCKS5_UDP_TTL_MS)
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
            udp_candidate_indices(&context, 1024 * 1024, DEFAULT_SOCKS5_UDP_TTL_MS)
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
            udp_candidate_indices(&context, 4096, DEFAULT_SOCKS5_UDP_TTL_MS)
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
    fn realtime_udp_datagram_feedback_beats_probe_only_paths() {
        let feedback_path = "udp://127.0.0.1:10144"
            .parse::<PathSpec>()
            .expect("feedback path");
        let probe_only_path = "udp://127.0.0.1:10145"
            .parse::<PathSpec>()
            .expect("probe-only path");
        let context = ClientPathContext::new(
            vec![feedback_path, probe_only_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("context");

        context.mark_udp_path_feedback(
            0,
            UdpDatagramPathObservation {
                rtt: Duration::from_millis(40),
                jitter: Duration::from_millis(4),
                loss_rate: 0.0,
                rate_sample: PathRateSample::new(1024 * 1024, Duration::from_millis(10)),
            },
        );
        context.mark_udp_path_probe_success(1, Duration::from_millis(1));
        context.mark_udp_path_feedback(
            1,
            UdpDatagramPathObservation {
                rtt: Duration::from_millis(20),
                jitter: Duration::from_millis(2),
                loss_rate: 0.0,
                rate_sample: PathRateSample::new(1024 * 1024, Duration::from_millis(10)),
            },
        );

        let association = UdpDatagramClientAssociation::new(context.clone()).expect("assoc");
        let candidates =
            context.ordered_udp_path_candidates_for_ttl(512, DEFAULT_SOCKS5_UDP_TTL_MS);
        assert_eq!(
            association.select_path_candidate(
                &candidates,
                &HashSet::new(),
                512,
                DEFAULT_SOCKS5_UDP_TTL_MS,
            ),
            Some(0)
        );
        assert_eq!(
            association.select_path_candidate(
                &candidates,
                &HashSet::from([0]),
                512,
                DEFAULT_SOCKS5_UDP_TTL_MS,
            ),
            Some(1)
        );
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

        assert!(udp_candidate_indices(&context, 1024, 10).is_empty());
    }

    #[test]
    fn realtime_udp_prefers_measured_model_before_unmeasured_startup_paths() {
        let first_path = "udp://127.0.0.1:10024"
            .parse::<PathSpec>()
            .expect("first path");
        let second_path = "udp://127.0.0.1:10025"
            .parse::<PathSpec>()
            .expect("second path");
        let context = ClientPathContext::new(
            vec![first_path, second_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("context");

        assert_eq!(
            udp_candidate_indices(&context, 512, DEFAULT_SOCKS5_UDP_TTL_MS),
            vec![0]
        );

        context.mark_udp_path_probe_success(0, Duration::from_millis(20));

        assert_eq!(
            udp_candidate_indices(&context, 512, DEFAULT_SOCKS5_UDP_TTL_MS),
            vec![0]
        );
    }

    #[test]
    fn udp_association_suppression_prefers_survivor_without_dead_ending() {
        let blackhole_path = "udp://127.0.0.1:10026?srtt-ms=5&rate-mbps=100"
            .parse::<PathSpec>()
            .expect("blackhole path");
        let survivor_path = "udp://127.0.0.1:10027?srtt-ms=20&rate-mbps=100"
            .parse::<PathSpec>()
            .expect("survivor path");
        let context = ClientPathContext::new(
            vec![blackhole_path, survivor_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("ctx");
        let mut association = UdpDatagramClientAssociation::new(context).expect("assoc");
        let candidates = [
            UdpPathCandidate {
                path_index: 0,
                eta_ms: 5.0,
            },
            UdpPathCandidate {
                path_index: 1,
                eta_ms: 20.0,
            },
        ];

        assert_eq!(
            association.select_path_candidate(&candidates, &HashSet::new(), 512, 1000),
            Some(0)
        );

        association.suppress_path_after_timeout(0, Duration::from_millis(250), 1000);
        assert_eq!(
            association.select_path_candidate(&candidates, &HashSet::new(), 512, 1000),
            Some(1)
        );

        association.suppress_path_after_timeout(1, Duration::from_millis(250), 1000);
        assert_eq!(
            association.select_path_candidate(&candidates, &HashSet::new(), 512, 1000),
            Some(0)
        );
    }

    #[test]
    fn udp_association_sticks_to_successful_path_until_suppressed() {
        let steady_path = "udp://127.0.0.1:10031?srtt-ms=20&rate-mbps=100"
            .parse::<PathSpec>()
            .expect("steady path");
        let lower_eta_path = "udp://127.0.0.1:10032?srtt-ms=5&rate-mbps=100"
            .parse::<PathSpec>()
            .expect("lower eta path");
        let context = ClientPathContext::new(
            vec![steady_path, lower_eta_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("ctx");
        let mut association = UdpDatagramClientAssociation::new(context).expect("assoc");
        let candidates = [
            UdpPathCandidate {
                path_index: 1,
                eta_ms: 5.0,
            },
            UdpPathCandidate {
                path_index: 0,
                eta_ms: 20.0,
            },
        ];

        association.last_successful_path = Some(0);
        assert_eq!(
            association.select_path_candidate(&candidates, &HashSet::new(), 512, 1000),
            Some(0)
        );

        association.suppress_path_after_timeout(0, Duration::from_millis(250), 1000);
        assert_eq!(
            association.select_path_candidate(&candidates, &HashSet::new(), 512, 1000),
            Some(1)
        );
    }

    #[test]
    fn udp_acked_timeout_migration_requires_validated_alternative() {
        let proven_path = "udp://127.0.0.1:10033"
            .parse::<PathSpec>()
            .expect("proven path");
        let endpoint_only_alternative = "udp://127.0.0.1:10034"
            .parse::<PathSpec>()
            .expect("endpoint-only alternative");
        let hinted_alternative = "udp://127.0.0.1:10035?srtt-ms=80&rate-mbps=30"
            .parse::<PathSpec>()
            .expect("hinted alternative");
        let context = ClientPathContext::new(
            vec![proven_path, endpoint_only_alternative, hinted_alternative],
            security(),
            ResourceLimits::default(),
        )
        .expect("ctx");
        context.mark_udp_path_feedback(
            0,
            UdpDatagramPathObservation {
                rtt: Duration::from_millis(40),
                jitter: Duration::from_millis(4),
                loss_rate: 0.0,
                rate_sample: None,
            },
        );
        let association = UdpDatagramClientAssociation::new(context).expect("assoc");
        let attempted = HashSet::from([0]);

        assert!(!association.has_validated_udp_retry_alternative(
            &[
                UdpPathCandidate {
                    path_index: 0,
                    eta_ms: 40.0,
                },
                UdpPathCandidate {
                    path_index: 1,
                    eta_ms: 80.0,
                },
            ],
            &attempted,
            0,
        ));
        assert!(association.has_validated_udp_retry_alternative(
            &[
                UdpPathCandidate {
                    path_index: 0,
                    eta_ms: 40.0,
                },
                UdpPathCandidate {
                    path_index: 2,
                    eta_ms: 80.0,
                },
            ],
            &attempted,
            0,
        ));
    }

    #[test]
    fn udp_path_open_timeout_uses_adaptive_multipath_startup_budget() {
        let mut model = UdpPathRuntimeModel {
            pacing_rate_bps: UDP_MIN_PACING_RATE_BPS,
            response_timeout: Duration::from_millis(300),
            mtu_payload_bytes: UDP_DEFAULT_MTU_PAYLOAD_BYTES,
            mtu_is_measured: false,
            mtu_probe_ceiling_payload_bytes: UDP_MAX_MTU_PAYLOAD_BYTES,
        };

        assert_eq!(
            udp_datagram_path_open_timeout(false, false, model, DEFAULT_SOCKS5_UDP_TTL_MS),
            UDP_PATH_HANDSHAKE_TIMEOUT
        );
        assert_eq!(
            udp_datagram_path_open_timeout(true, true, model, DEFAULT_SOCKS5_UDP_TTL_MS),
            Duration::from_millis(300)
        );
        assert_eq!(
            udp_datagram_path_open_timeout(false, true, model, DEFAULT_SOCKS5_UDP_TTL_MS),
            UDP_PATH_HANDSHAKE_TIMEOUT
        );

        model.response_timeout = Duration::from_millis(1);
        assert_eq!(
            udp_datagram_path_open_timeout(true, true, model, DEFAULT_SOCKS5_UDP_TTL_MS),
            UDP_MIN_RESPONSE_TIMEOUT
        );
        assert_eq!(
            udp_datagram_path_open_timeout(false, true, model, DEFAULT_SOCKS5_UDP_TTL_MS),
            UDP_MIN_RESPONSE_TIMEOUT
        );

        model.response_timeout = Duration::from_millis(65);
        assert_eq!(
            udp_datagram_path_open_timeout(false, true, model, DEFAULT_SOCKS5_UDP_TTL_MS),
            Duration::from_millis(520)
        );

        model.response_timeout = UDP_PATH_HANDSHAKE_TIMEOUT + Duration::from_secs(1);
        assert_eq!(
            udp_datagram_path_open_timeout(true, true, model, DEFAULT_SOCKS5_UDP_TTL_MS),
            UDP_PATH_HANDSHAKE_TIMEOUT
        );
        assert_eq!(
            udp_datagram_path_open_timeout(false, false, model, 250),
            Duration::from_millis(250)
        );
    }

    #[test]
    fn udp_runtime_model_backs_off_response_timeout_after_loss() {
        let stable = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 80.0, 30_000_000.0);
        let mut lossy = stable;
        lossy.loss_rate = 0.5;

        let stable_model = UdpPathRuntimeModel::from_snapshot(
            stable,
            DEFAULT_SOCKS5_UDP_TTL_MS,
            UDP_DEFAULT_MTU_PAYLOAD_BYTES,
            true,
            UDP_MAX_MTU_PAYLOAD_BYTES,
        );
        let lossy_model = UdpPathRuntimeModel::from_snapshot(
            lossy,
            DEFAULT_SOCKS5_UDP_TTL_MS,
            UDP_DEFAULT_MTU_PAYLOAD_BYTES,
            true,
            UDP_MAX_MTU_PAYLOAD_BYTES,
        );

        assert!(lossy_model.response_timeout > stable_model.response_timeout);
        assert!(lossy_model.response_timeout <= UDP_MAX_RESPONSE_TIMEOUT);
        assert!(lossy_model.pacing_rate_bps < stable_model.pacing_rate_bps);
    }

    #[test]
    fn udp_association_retry_budget_tracks_live_loss_model() {
        let path = "udp://127.0.0.1:10036?srtt-ms=80&rate-mbps=30"
            .parse::<PathSpec>()
            .expect("path");
        let context =
            ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("ctx");
        let association = UdpDatagramClientAssociation::new(context.clone()).expect("assoc");
        let stable_budget = association.adaptive_retry_budget(512, DEFAULT_SOCKS5_UDP_TTL_MS);
        assert!(stable_budget >= UDP_MIN_RETRY_BUDGET);
        assert!(stable_budget <= UDP_MAX_RETRY_BUDGET);

        context.mark_udp_path_feedback(
            0,
            UdpDatagramPathObservation {
                rtt: Duration::from_millis(120),
                jitter: Duration::from_millis(0),
                loss_rate: 1.0,
                rate_sample: None,
            },
        );

        let lossy_budget = association.adaptive_retry_budget(512, DEFAULT_SOCKS5_UDP_TTL_MS);
        assert!(lossy_budget > stable_budget);
        assert!(lossy_budget <= UDP_MAX_RETRY_BUDGET);
    }

    #[test]
    fn udp_edge_lane_startup_ramps_after_success_feedback() {
        let paths = vec![
            "udp://127.0.0.1:10180".parse().expect("first path"),
            "udp://127.0.0.1:10181".parse().expect("second path"),
            "udp://127.0.0.1:10182".parse().expect("third path"),
        ];
        let context =
            ClientPathContext::new(paths, security(), ResourceLimits::default()).expect("context");

        assert!(udp_edge_lane_limit(&context) > udp_edge_startup_lane_limit(&context));
        assert_eq!(udp_edge_startup_lane_limit(&context), 2);
        assert!(udp_edge_lane_spawn_allowed(0, 0, &context));
        assert!(udp_edge_lane_spawn_allowed(1, 0, &context));
        assert!(!udp_edge_lane_spawn_allowed(2, 0, &context));
        assert!(udp_edge_lane_spawn_allowed(2, 1, &context));
    }

    #[test]
    fn udp_edge_lane_startup_respects_queue_capacity() {
        let path = "udp://127.0.0.1:10183".parse().expect("path");
        let resources = ResourceLimits {
            max_datagram_queue_bytes: ResourceLimits::default().max_payload_bytes,
            ..ResourceLimits::default()
        };
        let context = ClientPathContext::new(vec![path], security(), resources).expect("context");

        assert_eq!(udp_edge_queue_slots(&context), 1);
        assert_eq!(udp_edge_startup_lane_limit(&context), 1);
        assert!(udp_edge_lane_spawn_allowed(0, 0, &context));
        assert!(!udp_edge_lane_spawn_allowed(1, 0, &context));
        assert!(udp_edge_lane_spawn_allowed(1, 1, &context));
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

        context.mark_tcp_path_open_success(0, Duration::from_millis(1), TrafficClass::Interactive);
        assert_eq!(
            context
                .ordered_tcp_path_indices(TrafficClass::Interactive, 512)
                .first()
                .copied(),
            Some(1)
        );

        context.release_tcp_path_load(0, TrafficClass::Interactive);
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
    fn active_interactive_tcp_flow_pushes_bulk_to_other_path() {
        let low_latency_path = "tcp://127.0.0.1:10123?srtt-ms=20&rate-mbps=100"
            .parse::<PathSpec>()
            .expect("low latency path");
        let bulk_candidate_path = "tcp://127.0.0.1:10124?srtt-ms=180&rate-mbps=100"
            .parse::<PathSpec>()
            .expect("bulk candidate path");
        let context = ClientPathContext::new(
            vec![low_latency_path, bulk_candidate_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("context");

        context.mark_tcp_path_open_success(0, Duration::from_millis(20), TrafficClass::Interactive);
        context.mark_tcp_path_delivery(
            1,
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
            Some(1)
        );
        assert_eq!(
            context
                .ordered_tcp_path_indices(TrafficClass::Interactive, PATH_OPEN_SCORE_BYTES)
                .first()
                .copied(),
            Some(0)
        );
    }

    #[test]
    fn endpoint_only_tcp_startup_preserves_configured_order_on_equal_scores() {
        let first_path = "tcp://127.0.0.1:10121"
            .parse::<PathSpec>()
            .expect("first path");
        let second_path = "tcp://127.0.0.1:10122"
            .parse::<PathSpec>()
            .expect("second path");
        let context = ClientPathContext::new(
            vec![first_path, second_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("context");

        assert_eq!(
            context
                .ordered_tcp_path_indices(TrafficClass::Interactive, PATH_OPEN_SCORE_BYTES)
                .first()
                .copied(),
            Some(0)
        );
    }

    #[test]
    fn endpoint_only_tcp_startup_validates_order_before_noisy_probe_scores() {
        let first_path = "tcp://127.0.0.1:10125"
            .parse::<PathSpec>()
            .expect("first path");
        let second_path = "tcp://127.0.0.1:10126"
            .parse::<PathSpec>()
            .expect("second path");
        let context = ClientPathContext::new(
            vec![first_path, second_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("context");

        context.mark_tcp_path_failure(0);
        context.mark_tcp_path_probe_success(1, Duration::from_millis(1));

        assert_eq!(
            context.ordered_tcp_path_indices(TrafficClass::Interactive, PATH_OPEN_SCORE_BYTES),
            vec![0, 1]
        );

        context.mark_tcp_path_failure(0);
        assert_eq!(
            context.ordered_tcp_path_indices(TrafficClass::Interactive, PATH_OPEN_SCORE_BYTES),
            vec![1]
        );
    }

    #[test]
    fn endpoint_only_tcp_interactive_opens_stay_latency_first_under_active_flow() {
        let low_latency_path = "tcp://127.0.0.1:10129"
            .parse::<PathSpec>()
            .expect("low latency path");
        let high_latency_path = "tcp://127.0.0.1:10130"
            .parse::<PathSpec>()
            .expect("high latency path");
        let poor_path = "tcp://127.0.0.1:10131"
            .parse::<PathSpec>()
            .expect("poor path");
        let context = ClientPathContext::new(
            vec![low_latency_path, high_latency_path, poor_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("context");

        context.mark_tcp_path_open_success(0, Duration::from_millis(20), TrafficClass::Interactive);
        context.mark_tcp_path_probe_success(2, Duration::from_millis(1));

        assert_eq!(
            context.ordered_tcp_path_indices(TrafficClass::Interactive, PATH_OPEN_SCORE_BYTES),
            vec![0, 1, 2]
        );
    }

    #[test]
    fn hinted_tcp_startup_uses_configured_metrics_before_order() {
        let high_latency_path = "tcp://127.0.0.1:10127?srtt-ms=200&rate-mbps=100"
            .parse::<PathSpec>()
            .expect("high latency path");
        let low_latency_path = "tcp://127.0.0.1:10128?srtt-ms=10&rate-mbps=100"
            .parse::<PathSpec>()
            .expect("low latency path");
        let context = ClientPathContext::new(
            vec![high_latency_path, low_latency_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("context");

        assert_eq!(
            context
                .ordered_tcp_path_indices(TrafficClass::Interactive, PATH_OPEN_SCORE_BYTES)
                .first()
                .copied(),
            Some(1)
        );
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

        context.mark_udp_path_probe_success(1, Duration::from_millis(1));
        context.mark_udp_path_open_success(0, Duration::from_millis(1));
        assert_eq!(
            udp_candidate_indices(&context, 512, DEFAULT_SOCKS5_UDP_TTL_MS)
                .first()
                .copied(),
            Some(1)
        );

        context.release_udp_path_load(0);
        assert_eq!(
            udp_candidate_indices(&context, 512, DEFAULT_SOCKS5_UDP_TTL_MS)
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
    async fn repeated_path_probe_failure_suppresses_unreachable_tcp_path() {
        let path = reserve_tcp_path().await;
        let context =
            ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("ctx");

        probe_client_paths(&context, Duration::from_millis(50)).await;

        {
            let health = context.health.lock().expect("health lock");
            assert_eq!(health.tcp[0].state, SchedulerPathState::Suspect);
            assert_eq!(health.tcp[0].consecutive_failures, 1);
            assert!(health.tcp[0].failed_until.is_none());
        }
        assert_eq!(
            context
                .ordered_tcp_path_indices(TrafficClass::Interactive, 512)
                .first()
                .copied(),
            Some(0)
        );

        probe_client_paths(&context, Duration::from_millis(50)).await;

        {
            let health = context.health.lock().expect("health lock");
            assert_eq!(health.tcp[0].state, SchedulerPathState::Failed);
            assert_eq!(health.tcp[0].consecutive_failures, 2);
            assert!(health.tcp[0].failed_until.is_some());
        }
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
    async fn socks5_ingress_relays_tcp_payload_over_encrypted_udp_stream_path() {
        let (target_addr, target) = spawn_echo_target().await;
        let path = reserve_udp_path().await;
        let socket = udp::bind_socket(&path).await.expect("bind udp path");
        let server_path = tokio::spawn(handle_server_udp_datagram_path_session(
            socket,
            server_context(OutboundConfig::Direct),
        ));
        let context =
            ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("ctx");
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
    async fn tcp_path_sessions_handle_multiple_dedicated_interactive_streams() {
        let (target_addr, target) = spawn_echo_target_count(2).await;
        let (path, server_path) = spawn_server_path_count(OutboundConfig::Direct, 2).await;
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
            assert!(matches!(
                health.tcp[0].state,
                SchedulerPathState::Suspect | SchedulerPathState::Failed
            ));
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
    async fn tcp_relay_active_stream_heartbeat_timeout_does_not_abort_stream() {
        let (path, server_path) =
            spawn_tcp_relay_heartbeat_blackhole(Duration::from_millis(500)).await;
        let resources = ResourceLimits {
            tcp_path_heartbeat_interval: Duration::from_millis(10),
            tcp_path_heartbeat_timeout: Duration::from_millis(30),
            ..ResourceLimits::default()
        };
        let context = ClientPathContext::new(vec![path], security(), resources).expect("ctx");
        let (mut client, server) = duplex(4096);
        let mut handler = tokio::spawn(handle_socks5_client_stream(server, context));

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

        tokio::select! {
            result = &mut handler => {
                panic!("active stream should not be aborted by heartbeat timeout: {result:?}");
            }
            _ = tokio::time::sleep(Duration::from_millis(150)) => {}
        }

        handler.abort();
        let _ = handler.await;
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
    async fn socks5_ingress_starts_tcp_auto_latency_first() {
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
        let context = ClientPathContext::new(
            vec![no_bulk_low_latency_path, bulk_allowed_path],
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

        assert_eq!(accepted_rx.recv().await, Some(0));
        handler.await.expect("join").expect("handler");
        low_latency_server
            .await
            .expect("low latency server join")
            .expect("low latency server");
        bulk_allowed_server.abort();
        let _ = bulk_allowed_server.await;
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
    async fn http_connect_ingress_relays_tcp_payload_over_encrypted_udp_stream_path() {
        let (target_addr, target) = spawn_echo_target().await;
        let path = reserve_udp_path().await;
        let socket = udp::bind_socket(&path).await.expect("bind udp path");
        let server_path = tokio::spawn(handle_server_udp_datagram_path_session(
            socket,
            server_context(OutboundConfig::Direct),
        ));
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
    async fn socks5_udp_associate_does_not_block_fast_datagram_behind_slow_response() {
        let (target_addr, target) = spawn_udp_reordered_echo_target().await;
        let path = reserve_udp_path().await;
        let server = tokio::spawn(run_server(
            vec![path.clone()],
            OutboundConfig::Direct,
            DnsConfig::default(),
            security(),
            ResourceLimits::default(),
        ));
        tokio::time::sleep(Duration::from_millis(10)).await;
        let context =
            ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("ctx");
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
        let slow =
            socks5::udp_datagram(&TargetAddr::Ip(target_addr), b"slow").expect("slow request");
        let fast =
            socks5::udp_datagram(&TargetAddr::Ip(target_addr), b"fast").expect("fast request");
        udp_client
            .send_to(&slow, relay_addr)
            .await
            .expect("send slow request");
        tokio::time::sleep(Duration::from_millis(50)).await;
        udp_client
            .send_to(&fast, relay_addr)
            .await
            .expect("send fast request");

        let mut response = [0u8; 128];
        let (len, _) = tokio::time::timeout(
            Duration::from_millis(400),
            udp_client.recv_from(&mut response),
        )
        .await
        .expect("fast response should not wait for slow response")
        .expect("fast recv");
        let (datagram, consumed) = socks5::parse_udp_datagram(&response[..len]).expect("datagram");
        assert_eq!(consumed, len);
        assert_eq!(datagram.target, TargetAddr::Ip(target_addr));
        assert_eq!(datagram.payload, Bytes::from_static(b"fast-pong"));

        control_client.shutdown().await.expect("control shutdown");
        handler.await.expect("handler join").expect("handler");
        server.abort();
        let _ = server.await;
        target.await.expect("target join");
    }

    #[tokio::test]
    async fn socks5_udp_associate_prefers_ready_low_latency_path() {
        let (target_addr, target) = spawn_udp_echo_target_count(2).await;
        let first_path = reserve_udp_path_with_query("srtt-ms=10&rate-mbps=10").await;
        let second_path = reserve_udp_path_with_query("srtt-ms=10&rate-mbps=10").await;
        let first_socket = udp::bind_socket(&first_path)
            .await
            .expect("bind first udp path");
        let first_server = tokio::spawn(handle_server_udp_datagram_path_session(
            first_socket,
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
            assert_eq!(health.udp[1].active_flows, 0);
            assert_eq!(health.udp[1].state, SchedulerPathState::Active);
            assert_eq!(health.udp[1].consecutive_failures, 0);
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
        target.await.expect("target join");
    }

    #[tokio::test]
    async fn udp_association_scores_pacer_delay_against_path_eta() {
        let (target_addr, target) = spawn_udp_echo_target().await;
        let low_latency_path = reserve_udp_path_with_query("srtt-ms=10&rate-mbps=100").await;
        let slower_path = reserve_udp_path_with_query("srtt-ms=120&rate-mbps=100").await;
        let low_latency_socket = udp::bind_socket(&low_latency_path)
            .await
            .expect("bind low latency udp path");
        let low_latency_server = tokio::spawn(handle_server_udp_datagram_path_session(
            low_latency_socket,
            server_context(OutboundConfig::Direct),
        ));
        let context = ClientPathContext::new(
            vec![low_latency_path, slower_path],
            security(),
            ResourceLimits::default(),
        )
        .expect("ctx");
        let observed_context = context.clone();
        let mut association = UdpDatagramClientAssociation::new(context).expect("assoc");

        let response = association
            .send_to(
                TargetAddr::Ip(target_addr),
                Bytes::from_static(b"ping"),
                1000,
            )
            .await
            .expect("initial response");

        assert_eq!(response, Bytes::from_static(b"pong"));
        association
            .paths
            .iter_mut()
            .find(|path| path.session.path_index == 0)
            .expect("low latency path session")
            .pacer
            .next_send_at = Instant::now() + Duration::from_millis(30);

        assert_eq!(
            association.select_path_candidate(
                &[
                    UdpPathCandidate {
                        path_index: 0,
                        eta_ms: 10.0,
                    },
                    UdpPathCandidate {
                        path_index: 1,
                        eta_ms: 120.0,
                    },
                ],
                &HashSet::new(),
                512,
                1000,
            ),
            Some(0)
        );
        observed_context.mark_udp_path_probe_success(1, Duration::from_millis(20));
        assert_eq!(
            association.select_path_candidate(
                &[
                    UdpPathCandidate {
                        path_index: 0,
                        eta_ms: 10.0,
                    },
                    UdpPathCandidate {
                        path_index: 1,
                        eta_ms: 25.0,
                    },
                ],
                &HashSet::new(),
                512,
                1000,
            ),
            Some(0)
        );

        association.suppress_path_after_timeout(0, Duration::from_millis(250), 1000);
        assert_eq!(
            association.select_path_candidate(
                &[
                    UdpPathCandidate {
                        path_index: 0,
                        eta_ms: 10.0,
                    },
                    UdpPathCandidate {
                        path_index: 1,
                        eta_ms: 25.0,
                    },
                ],
                &HashSet::new(),
                512,
                1000,
            ),
            Some(1)
        );

        association.close().await.expect("close association");
        low_latency_server
            .await
            .expect("low latency server join")
            .expect("low latency server");
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
            assert_eq!(health.udp[0].state, SchedulerPathState::Suspect);
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
                &[
                    UdpPathCandidate {
                        path_index: 0,
                        eta_ms: 5.0,
                    },
                    UdpPathCandidate {
                        path_index: 1,
                        eta_ms: 20.0,
                    },
                ],
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
    async fn udp_association_retries_acked_timeout_on_same_open_path() {
        let (target_addr, target) = spawn_udp_drop_first_echo_target().await;
        let path = reserve_udp_path_with_query("srtt-ms=5&rate-mbps=100").await;
        let socket = udp::bind_socket(&path).await.expect("bind udp path");
        let server = tokio::spawn(handle_server_udp_datagram_path_session(
            socket,
            server_context(OutboundConfig::Direct),
        ));
        let context =
            ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("ctx");
        let mut association = UdpDatagramClientAssociation::new(context.clone()).expect("assoc");

        let response = association
            .send_to(
                TargetAddr::Ip(target_addr),
                Bytes::from_static(b"ping"),
                1000,
            )
            .await
            .expect("same path retry response");

        assert_eq!(response, Bytes::from_static(b"pong"));
        {
            let health = context.health.lock().expect("health lock");
            assert_eq!(health.udp[0].state, SchedulerPathState::Active);
            assert_eq!(health.udp[0].active_flows, 1);
            assert!(
                health.udp[0]
                    .measured_loss_rate
                    .is_some_and(|loss| loss > 0.0)
            );
        }
        association.close().await.expect("close association");
        server.await.expect("server join").expect("server");
        target.await.expect("target join");
    }

    #[tokio::test]
    async fn udp_association_ignores_stale_response_datagram_id() {
        let target_socket = UdpSocket::bind("127.0.0.1:0").await.expect("target bind");
        let target_addr = target_socket.local_addr().expect("target addr");
        let path = reserve_udp_path_with_query("srtt-ms=5&rate-mbps=100").await;
        let server = spawn_udp_datagram_stale_then_matching_response_path(path.clone()).await;
        let context =
            ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("ctx");
        let mut association = UdpDatagramClientAssociation::new(context).expect("assoc");

        let response = association
            .send_to(
                TargetAddr::Ip(target_addr),
                Bytes::from_static(b"ping"),
                1000,
            )
            .await
            .expect("matched response");

        assert_eq!(response, Bytes::from_static(b"pong"));
        association.close().await.expect("close association");
        server.await.expect("server join").expect("server");
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
