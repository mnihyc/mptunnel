use crate::config::{AppConfig, ClientConfig, CommandConfig, ResourceLimits, SecurityConfig};
use crate::ingress::IngressConfig;
use crate::ingress::http_connect::{self, HttpConnectError, HttpStatus};
use crate::ingress::socks5::{self, Socks5Error, Socks5Reply};
use crate::mux::MuxLimits;
use crate::mux::datagram::{DatagramError, DatagramFlow};
use crate::mux::stream::{ReliableRecvStream, ReliableSendStream, StreamError};
use crate::outbound::{self, OutboundConfig, TargetProtocol};
use crate::protocol::RateHint;
use crate::protocol::auth::{AuthError, SessionAuthenticator};
use crate::protocol::codec::CodecLimits;
use crate::protocol::{
    AuthNonce, CloseReason, DatagramFlowId, Frame, IngressKind, OutboundPolicy, PathId,
    ResetReason, SessionId, StreamFlags, StreamId, TargetAddr, TrafficClass, UnderlayProtocol,
};
use crate::scheduler::{self, PathSnapshot, PathState as SchedulerPathState, SchedulerPolicy};
use crate::transport::encrypted::{EncryptedFramedStream, EncryptedFramedTransportError, PeerRole};
use crate::transport::encrypted_udp::{EncryptedUdpSocket, EncryptedUdpTransportError};
use crate::transport::tcp::{self, TcpConnectOptions, TcpTransportError};
use crate::transport::udp::{self, UdpTransportError};
use crate::transport::{PathSpec, PathSpecParseError};
use bytes::Bytes;
use std::collections::HashMap;
use std::future;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::mpsc;

const MAX_HTTP_CONNECT_HEADER_BYTES: usize = 64 * 1024;
const PATH_OPEN_SCORE_BYTES: usize = 4 * 1024;
const UDP_PATH_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(2);
const PATH_FAILURE_COOLDOWN: Duration = Duration::from_secs(5);
const TCP_STREAM_LOAD_BYTES: u64 = 256 * 1024;
const UDP_SESSION_LOAD_BYTES: u64 = 64 * 1024;

pub async fn run(config: AppConfig) -> Result<(), RuntimeError> {
    match config.command {
        CommandConfig::Client(client) => {
            run_client(client, config.security, config.resources).await
        }
        CommandConfig::Server(server) => {
            run_server(
                server.bind_paths,
                server.outbound,
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
            let listener = TcpListener::bind(listen).await?;
            start_client_path_probes(context.clone(), path_probe_interval, path_probe_timeout);
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
        IngressConfig::HttpConnect { listen } => {
            let listener = TcpListener::bind(listen).await?;
            start_client_path_probes(context.clone(), path_probe_interval, path_probe_timeout);
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
    }
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

async fn run_server(
    bind_paths: Vec<PathSpec>,
    outbound: OutboundConfig,
    security: SecurityConfig,
    resources: ResourceLimits,
) -> Result<(), RuntimeError> {
    let context = ServerPathContext {
        outbound,
        codec_limits: resources.into(),
        mux_limits: resources.into(),
        security,
        max_udp_sessions: resources.max_streams,
        max_udp_flows_per_session: resources.max_streams,
    };
    for path in bind_paths {
        match path.underlay {
            UnderlayProtocol::Tcp => {
                let listener = tcp::bind_listener(&path).await?;
                let context = context.clone();
                tokio::spawn(async move {
                    if let Err(err) = run_server_tcp_listener(listener, context).await {
                        eprintln!("warning: TCP server listener failed: {err}");
                    }
                });
            }
            UnderlayProtocol::Udp => {
                let socket = udp::bind_socket(&path).await?;
                let context = context.clone();
                tokio::spawn(async move {
                    if let Err(err) = run_server_udp_listener(socket, context).await {
                        eprintln!("warning: UDP server listener failed: {err}");
                    }
                });
            }
        }
    }
    future::pending::<()>().await;
    Ok(())
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

    fn mark_failure(&mut self, now: Instant) {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        self.state = SchedulerPathState::Failed;
        self.failed_until = Some(now + PATH_FAILURE_COOLDOWN);
    }
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
        Ok(Self {
            tcp_paths: Arc::new(tcp_paths),
            udp_paths: Arc::new(udp_paths),
            health: Arc::new(Mutex::new(health)),
            codec_limits: resources.into(),
            mux_limits: resources.into(),
            security,
        })
    }

    fn ordered_tcp_path_indices(&self, class: TrafficClass, payload_bytes: usize) -> Vec<usize> {
        let observations =
            health_observations(&mut self.health.lock().expect("client path health lock").tcp);
        ordered_path_indices(&self.tcp_paths, &observations, class, payload_bytes)
    }

    fn ordered_udp_path_indices(&self, payload_bytes: usize) -> Vec<usize> {
        let observations =
            health_observations(&mut self.health.lock().expect("client path health lock").udp);
        ordered_path_indices(
            &self.udp_paths,
            &observations,
            TrafficClass::RealtimeDatagram,
            payload_bytes,
        )
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
    scores.into_iter().map(|(index, _)| index).collect()
}

fn path_snapshot(
    path: &PathSpec,
    index: usize,
    observation: ClientPathObservation,
) -> PathSnapshot {
    let delivery_rate_bps = match path.metadata.initial_rate {
        RateHint::Unknown => default_path_rate_bps(path.underlay),
        RateHint::Unlimited => 1_000_000_000_000.0,
        RateHint::BitsPerSecond(rate) => rate.max(1) as f64,
    };
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
        jitter_ms: f64::from(path.metadata.initial_jitter_ms.unwrap_or(0)),
        delivery_rate_bps,
        loss_rate: 0.0,
        queue_bytes: observation.load_bytes,
        bytes_in_flight: u64::from(observation.active_flows) * PATH_OPEN_SCORE_BYTES as u64,
    }
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
    codec_limits: CodecLimits,
    mux_limits: MuxLimits,
    security: SecurityConfig,
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
            let remote = match open_remote_stream(
                &context,
                request.target,
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
            let path_index = remote.path_index;
            let result = async {
                stream
                    .write_all(&socks5::connect_reply(
                        Socks5Reply::Succeeded,
                        SocketAddr::from(([0, 0, 0, 0], 0)),
                    ))
                    .await?;
                stream.flush().await?;
                relay_tcp_stream(
                    stream,
                    remote.framed,
                    remote.stream_id,
                    context.mux_limits,
                    remote.max_offset,
                )
                .await
            }
            .await;
            context.release_tcp_path_load(path_index);
            result
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
    let remote = match open_remote_stream(
        &context,
        request.target,
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
    let path_index = remote.path_index;
    let result = async {
        stream.write_all(http_connect::success_response()).await?;
        stream.flush().await?;
        relay_tcp_stream(
            stream,
            remote.framed,
            remote.stream_id,
            context.mux_limits,
            remote.max_offset,
        )
        .await
    }
    .await;
    context.release_tcp_path_load(path_index);
    result
}

struct OpenedRemoteStream {
    framed: EncryptedFramedStream<TcpStream>,
    stream_id: StreamId,
    max_offset: u64,
    path_index: usize,
}

async fn open_remote_stream(
    context: &ClientPathContext,
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
        let started_at = Instant::now();
        match open_remote_stream_on_path(context, path_index, target.clone(), ingress, class).await
        {
            Ok(opened) => {
                context.mark_tcp_path_open_success(path_index, started_at.elapsed());
                return Ok(opened);
            }
            Err(err) if stream_open_error_is_path_retryable(&err) => {
                context.mark_tcp_path_failure(path_index);
                last_retryable_error = Some(err);
            }
            Err(err) => return Err(err),
        }
    }
    Err(last_retryable_error.unwrap_or(RuntimeError::NoSchedulableTcpPath))
}

fn authenticated_path_join_frames(
    security: &SecurityConfig,
    path: &PathSpec,
    path_id: PathId,
    underlay: UnderlayProtocol,
) -> Result<(Frame, Frame, Frame), RuntimeError> {
    let session_id = random_session_id()?;
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

async fn open_remote_stream_on_path(
    context: &ClientPathContext,
    path_index: usize,
    target: TargetAddr,
    ingress: IngressKind,
    class: TrafficClass,
) -> Result<OpenedRemoteStream, RuntimeError> {
    let path = context
        .tcp_paths
        .get(path_index)
        .ok_or(RuntimeError::NoSchedulableTcpPath)?;
    let tcp_stream = tcp::connect_path(path, TcpConnectOptions::default()).await?;
    let mut framed = EncryptedFramedStream::new(
        tcp_stream,
        context.security.secret.as_bytes(),
        PeerRole::Client,
        context.codec_limits,
    );
    let path_id = PathId(path_index as u16);
    let (session_hello, session_auth, path_join) =
        authenticated_path_join_frames(&context.security, path, path_id, UnderlayProtocol::Tcp)?;

    framed.write_frame(&session_hello).await?;
    framed.write_frame(&session_auth).await?;
    framed.write_frame(&path_join).await?;
    let stream_id = StreamId(0);
    framed
        .write_frame(&Frame::OpenStream {
            stream_id,
            target,
            ingress,
            outbound: OutboundPolicy::Direct,
            class,
        })
        .await?;
    framed.flush().await?;
    let mut session_ready = false;
    loop {
        match framed.read_frame().await? {
            Frame::SessionReady => session_ready = true,
            Frame::PathStatus { .. } => {}
            Frame::StreamMaxData {
                stream_id: accepted_stream_id,
                max_offset,
            } if accepted_stream_id == stream_id && session_ready => {
                return Ok(OpenedRemoteStream {
                    framed,
                    stream_id,
                    max_offset,
                    path_index,
                });
            }
            Frame::StreamReset {
                stream_id: reset_stream_id,
                reason,
            } if reset_stream_id == stream_id => {
                return Err(RuntimeError::RemoteReset(reason));
            }
            Frame::SessionClose { reason } => return Err(RuntimeError::RemoteClosed(reason)),
            _ => {
                return Err(RuntimeError::Protocol(
                    "unexpected frame while opening stream",
                ));
            }
        }
    }
}

fn stream_open_error_is_path_retryable(err: &RuntimeError) -> bool {
    matches!(
        err,
        RuntimeError::Io(_)
            | RuntimeError::Tcp(_)
            | RuntimeError::Encrypted(_)
            | RuntimeError::Auth(_)
            | RuntimeError::RemoteClosed(_)
            | RuntimeError::Protocol(_)
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
    let mut udp_session: Option<UdpDatagramClientSession> = None;
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
                if udp_session.is_none() {
                    match open_udp_datagram_session(&context).await {
                        Ok(session) => udp_session = Some(session),
                        Err(err) => break Err(err),
                    }
                }
                let response = match udp_session
                    .as_mut()
                    .ok_or(RuntimeError::Protocol("missing UDP datagram session"))
                {
                    Ok(session) => session,
                    Err(err) => break Err(err),
                }
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
    if let Some(session) = udp_session.as_mut() {
        let close_result = session.close().await;
        context.release_udp_path_load(session.path_index);
        if result.is_ok() {
            close_result?;
        }
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

async fn open_udp_datagram_session(
    context: &ClientPathContext,
) -> Result<UdpDatagramClientSession, RuntimeError> {
    let candidates = context.ordered_udp_path_indices(PATH_OPEN_SCORE_BYTES);
    if candidates.is_empty() {
        return Err(RuntimeError::NoSchedulableUdpPath);
    }
    let mut last_retryable_error = None;
    for path_index in candidates {
        let path = context
            .udp_paths
            .get(path_index)
            .ok_or(RuntimeError::NoSchedulableUdpPath)?;
        let started_at = Instant::now();
        match UdpDatagramClientSession::open(
            path,
            path_index,
            PathId(path_index as u16),
            context.security.clone(),
            context.codec_limits,
            context.mux_limits,
            UDP_PATH_HANDSHAKE_TIMEOUT,
        )
        .await
        {
            Ok(session) => {
                context.mark_udp_path_open_success(path_index, started_at.elapsed());
                return Ok(session);
            }
            Err(err) if udp_open_error_is_path_retryable(&err) => {
                context.mark_udp_path_failure(path_index);
                last_retryable_error = Some(err);
            }
            Err(err) => return Err(err),
        }
    }
    Err(last_retryable_error.unwrap_or(RuntimeError::NoSchedulableUdpPath))
}

fn udp_open_error_is_path_retryable(err: &RuntimeError) -> bool {
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
            PathId(path_index as u16),
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
    let (stream_id, target) = match framed.read_frame().await? {
        Frame::OpenStream {
            stream_id, target, ..
        } => {
            outbound::validate_target(&target)?;
            context.outbound.ensure_supports(TargetProtocol::Tcp)?;
            (stream_id, target)
        }
        Frame::Ping { nonce } => {
            return handle_server_tcp_probe(framed, path_id, path_capabilities, nonce).await;
        }
        Frame::SessionClose { .. } => return Ok(()),
        _ => return Err(RuntimeError::Protocol("expected OPEN_STREAM or PING")),
    };
    let outbound_stream =
        match outbound::connect_tcp(&context.outbound, &target, Duration::from_secs(10)).await {
            Ok(stream) => stream,
            Err(err) => {
                framed
                    .write_frame(&Frame::StreamReset {
                        stream_id,
                        reason: ResetReason::Refused,
                    })
                    .await?;
                framed.flush().await?;
                return Err(RuntimeError::OutboundConnect(err));
            }
        };
    framed.write_frame(&Frame::SessionReady).await?;
    framed
        .write_frame(&Frame::PathStatus {
            path_id,
            status: crate::protocol::PathStatus::Active,
            capabilities: path_capabilities,
        })
        .await?;
    framed
        .write_frame(&Frame::StreamMaxData {
            stream_id,
            max_offset: context.mux_limits.max_stream_window_bytes,
        })
        .await?;
    framed.flush().await?;
    relay_tcp_stream(
        outbound_stream,
        framed,
        stream_id,
        context.mux_limits,
        context.mux_limits.max_stream_window_bytes,
    )
    .await
}

async fn handle_server_tcp_probe(
    mut framed: EncryptedFramedStream<TcpStream>,
    path_id: PathId,
    path_capabilities: crate::protocol::PathCapabilities,
    nonce: u64,
) -> Result<(), RuntimeError> {
    framed.write_frame(&Frame::SessionReady).await?;
    framed
        .write_frame(&Frame::PathStatus {
            path_id,
            status: crate::protocol::PathStatus::Active,
            capabilities: path_capabilities,
        })
        .await?;
    framed.write_frame(&Frame::Pong { nonce }).await?;
    framed.flush().await?;

    loop {
        match framed.read_frame().await? {
            Frame::Ping { nonce } => {
                framed.write_frame(&Frame::Pong { nonce }).await?;
                framed.flush().await?;
            }
            Frame::SessionClose { .. } => return Ok(()),
            Frame::PathClose {
                path_id: close_path_id,
                ..
            } if close_path_id == path_id => return Ok(()),
            _ => return Err(RuntimeError::Protocol("unexpected TCP path probe frame")),
        }
    }
}

async fn relay_tcp_stream<S>(
    mut local: S,
    mut framed: EncryptedFramedStream<TcpStream>,
    stream_id: StreamId,
    mux_limits: MuxLimits,
    initial_max_offset: u64,
) -> Result<(), RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut send_stream = ReliableSendStream::new(stream_id, mux_limits);
    send_stream.update_max_offset(initial_max_offset);
    let mut recv_stream = ReliableRecvStream::new(stream_id, mux_limits);
    let chunk_size = mux_limits.max_payload_bytes.clamp(1, 16 * 1024);
    let mut buf = vec![0u8; chunk_size];
    let mut local_open = true;
    let mut remote_open = true;

    loop {
        if !local_open && !remote_open && send_stream.repair_bytes() == 0 {
            break;
        }

        tokio::select! {
            read = local.read(&mut buf), if local_open => {
                let read = read?;
                if read == 0 {
                    framed.write_frame(&Frame::StreamFin { stream_id }).await?;
                    framed.flush().await?;
                    local_open = false;
                } else {
                    let frame = send_stream.send_data(
                        Bytes::copy_from_slice(&buf[..read]),
                        StreamFlags::NONE,
                    )?;
                    framed.write_frame(&frame).await?;
                    framed.flush().await?;
                }
            }
            frame = framed.read_frame(), if remote_open || send_stream.repair_bytes() > 0 => {
                match frame? {
                    Frame::StreamData {
                        stream_id: received_stream_id,
                        offset,
                        flags,
                        payload,
                    } if received_stream_id == stream_id && remote_open => {
                        let outcome = recv_stream.receive_data(offset, payload, flags)?;
                        for chunk in outcome.delivered {
                            local.write_all(&chunk).await?;
                        }
                        framed.write_frame(&recv_stream.ack_frame()).await?;
                        framed.flush().await?;
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
                    Frame::StreamFin { stream_id: fin_stream_id } if fin_stream_id == stream_id => {
                        local.shutdown().await?;
                        remote_open = false;
                    }
                    Frame::StreamReset {
                        stream_id: reset_stream_id,
                        reason,
                    } if reset_stream_id == stream_id => return Err(RuntimeError::RemoteReset(reason)),
                    Frame::SessionClose { reason } => return Err(RuntimeError::RemoteClosed(reason)),
                    _ => return Err(RuntimeError::Protocol("unexpected stream relay frame")),
                }
            }
            else => break,
        }
    }

    Ok(())
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
        PathId(0),
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

struct UdpDatagramClientSession {
    encrypted: EncryptedUdpSocket,
    buffer: Vec<u8>,
    flows: Vec<UdpDatagramClientFlow>,
    next_flow_id: u64,
    mux_limits: MuxLimits,
    path_index: usize,
}

struct UdpDatagramClientFlow {
    target: TargetAddr,
    flow: DatagramFlow,
    flow_id: DatagramFlowId,
}

impl UdpDatagramClientSession {
    async fn open(
        path: &PathSpec,
        path_index: usize,
        path_id: PathId,
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
        let (session_hello, session_auth, path_join) =
            authenticated_path_join_frames(&security, path, path_id, UnderlayProtocol::Udp)?;

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
        })
    }

    async fn send_to(
        &mut self,
        target: TargetAddr,
        payload: Bytes,
        ttl_ms: u32,
    ) -> Result<Bytes, RuntimeError> {
        let flow_id = self.ensure_flow(target).await?;
        let flow = self
            .flows
            .iter_mut()
            .find(|flow| flow.flow_id == flow_id)
            .ok_or(RuntimeError::Protocol("missing UDP datagram flow"))?;
        flow.flow.enqueue(0, ttl_ms, payload)?;
        let frame = self
            .flows
            .iter_mut()
            .find(|flow| flow.flow_id == flow_id)
            .ok_or(RuntimeError::Protocol("missing UDP datagram flow"))?
            .flow
            .pop_frame(0)
            .ok_or(RuntimeError::Protocol("datagram expired before send"))?;
        self.encrypted.send_frame(&frame).await?;

        match self.encrypted.recv_frame(&mut self.buffer).await? {
            Frame::DatagramData {
                flow_id: response_flow_id,
                payload,
                ..
            } if response_flow_id == flow_id => Ok(payload),
            Frame::DatagramClose {
                flow_id: closed_flow_id,
            } if closed_flow_id == flow_id => Err(RuntimeError::Protocol("datagram flow closed")),
            Frame::SessionClose { reason } => Err(RuntimeError::RemoteClosed(reason)),
            _ => Err(RuntimeError::Protocol("unexpected UDP datagram frame")),
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
                });
                Ok(ServerUdpSessionOutcome::Active)
            }
            (
                ServerUdpPathState::Established,
                Frame::DatagramData {
                    flow_id,
                    ttl_ms,
                    payload,
                    ..
                },
            ) => {
                if ttl_ms == 0 {
                    return Err(RuntimeError::Protocol("expired datagram received"));
                }
                let flow = self
                    .flows
                    .iter_mut()
                    .find(|flow| flow.flow_id == flow_id)
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
                let mut response_flow = DatagramFlow::new(flow_id, self.context.mux_limits);
                response_flow.enqueue(0, ttl_ms, Bytes::from(response))?;
                let frame = response_flow
                    .pop_frame(0)
                    .ok_or(RuntimeError::Protocol("UDP response expired before send"))?;
                self.encrypted.send_frame_to(&frame, self.peer).await?;
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
    NoTcpPath,
    NoUdpPath,
    NoSchedulableTcpPath,
    NoSchedulableUdpPath,
    PathIdOverflow,
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
            Self::NoTcpPath
            | Self::NoUdpPath
            | Self::NoSchedulableTcpPath
            | Self::NoSchedulableUdpPath
            | Self::PathIdOverflow
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

    fn server_context(outbound: OutboundConfig) -> ServerPathContext {
        let resources = ResourceLimits::default();
        ServerPathContext {
            outbound,
            codec_limits: resources.into(),
            mux_limits: resources.into(),
            security: security(),
            max_udp_sessions: resources.max_streams,
            max_udp_flows_per_session: resources.max_streams,
        }
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
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("target bind");
        let addr = listener.local_addr().expect("target addr");
        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("target accept");
            let mut buf = [0u8; 4];
            stream.read_exact(&mut buf).await.expect("target read");
            assert_eq!(&buf, b"ping");
            stream.write_all(b"pong").await.expect("target write");
            stream.shutdown().await.expect("target shutdown");
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
            context.ordered_udp_path_indices(512).first().copied(),
            Some(1)
        );

        context.release_udp_path_load(0);
        assert_eq!(
            context.ordered_udp_path_indices(512).first().copied(),
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
        server.await.expect("server join").expect("server");
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
                    codec_limits: CodecLimits::default(),
                    mux_limits: ResourceLimits::default().into(),
                    security: SecurityConfig::encrypted(
                        SharedSecret::new(b"fedcba9876543210".to_vec()).expect("secret"),
                    ),
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
