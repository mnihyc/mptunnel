use crate::config::{
    AppConfig, ClientConfig, CommandConfig, ResourceLimits, SecurityConfig, TrafficPolicy,
};
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
use crate::transport::encrypted::{
    EncryptedFramedReader, EncryptedFramedStream, EncryptedFramedTransportError,
    EncryptedFramedWriter, PeerRole,
};
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
use tokio::sync::{mpsc, oneshot};

const MAX_HTTP_CONNECT_HEADER_BYTES: usize = 64 * 1024;
const PATH_OPEN_SCORE_BYTES: usize = 4 * 1024;
const UDP_PATH_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(2);
const PATH_FAILURE_COOLDOWN: Duration = Duration::from_secs(5);
const TCP_STREAM_LOAD_BYTES: u64 = 256 * 1024;
const UDP_SESSION_LOAD_BYTES: u64 = 64 * 1024;
const MIN_RATE_SAMPLE_BYTES: u64 = PATH_OPEN_SCORE_BYTES as u64;
const MIN_RATE_SAMPLE_DURATION: Duration = Duration::from_millis(1);

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
    let context = ClientPathContext::new_with_policy(
        client.paths,
        client.traffic_policy,
        security,
        resources,
    )?;
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
        max_tcp_streams: resources.max_streams,
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
    tcp_sessions: Arc<Vec<ClientTcpPathSessionHandle>>,
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
    measured_rate_bps: Option<f64>,
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
            measured_rate_bps: None,
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
    measured_rate_bps: Option<f64>,
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
            measured_rate_bps: self.measured_rate_bps,
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
    commands: mpsc::Sender<TcpPathSessionCommand>,
    frames: mpsc::Receiver<Result<Frame, RuntimeError>>,
}

impl TcpPathStream {
    async fn send_frame(&self, frame: Frame) -> Result<(), RuntimeError> {
        self.commands
            .send(TcpPathSessionCommand::SendFrame(frame))
            .await
            .map_err(|_| RuntimeError::TcpPathSessionClosed)
    }

    async fn recv_frame(&mut self) -> Result<Frame, RuntimeError> {
        match self.frames.recv().await {
            Some(Ok(frame)) => Ok(frame),
            Some(Err(err)) => Err(err),
            None => Err(RuntimeError::TcpPathSessionClosed),
        }
    }

    async fn close(&self) {
        let _ = self
            .commands
            .send(TcpPathSessionCommand::CloseStream(self.stream_id))
            .await;
    }
}

struct ClientTcpPathSessionHandle {
    path: PathSpec,
    path_index: usize,
    security: SecurityConfig,
    codec_limits: CodecLimits,
    mux_limits: MuxLimits,
    command_queue: usize,
    stream_frame_queue: usize,
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
            path: self.path.clone(),
            path_index: self.path_index,
            security: self.security.clone(),
            codec_limits: self.codec_limits,
            mux_limits: self.mux_limits,
            command_queue: self.command_queue,
            stream_frame_queue: self.stream_frame_queue,
            commands: self.commands.clone(),
        }
    }
}

impl ClientTcpPathSessionHandle {
    fn new(
        path: PathSpec,
        path_index: usize,
        security: SecurityConfig,
        codec_limits: CodecLimits,
        mux_limits: MuxLimits,
        command_queue: usize,
        stream_frame_queue: usize,
    ) -> Self {
        Self {
            path,
            path_index,
            security,
            codec_limits,
            mux_limits,
            command_queue,
            stream_frame_queue,
            commands: Arc::new(Mutex::new(None)),
        }
    }

    async fn open_stream(
        &self,
        target: TargetAddr,
        ingress: IngressKind,
        class: TrafficClass,
    ) -> Result<TcpPathStream, RuntimeError> {
        let commands = self.ensure_session();
        let (response_tx, response_rx) = oneshot::channel();
        commands
            .send(TcpPathSessionCommand::OpenStream {
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

        let (commands, receiver) = mpsc::channel(self.command_queue);
        tokio::spawn(run_client_tcp_path_session(
            self.path.clone(),
            self.path_index,
            self.security.clone(),
            self.codec_limits,
            self.mux_limits,
            receiver,
            self.stream_frame_queue,
        ));
        *current = Some(commands.clone());
        commands
    }
}

enum TcpPathSessionCommand {
    OpenStream {
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

struct ClientTcpPathSessionRuntime {
    path: PathSpec,
    path_index: usize,
    security: SecurityConfig,
    codec_limits: CodecLimits,
    mux_limits: MuxLimits,
    stream_frame_queue: usize,
}

struct ClientTcpPathSessionState {
    connection: Option<ClientTcpPathConnection>,
    streams: HashMap<StreamId, ClientTcpPathStreamState>,
    next_stream_id: u64,
}

struct ClientTcpOpenStreamRequest {
    target: TargetAddr,
    ingress: IngressKind,
    class: TrafficClass,
    session_commands: mpsc::Sender<TcpPathSessionCommand>,
    response: oneshot::Sender<Result<TcpPathStream, RuntimeError>>,
}

async fn run_client_tcp_path_session(
    path: PathSpec,
    path_index: usize,
    security: SecurityConfig,
    codec_limits: CodecLimits,
    mux_limits: MuxLimits,
    mut commands: mpsc::Receiver<TcpPathSessionCommand>,
    stream_frame_queue: usize,
) {
    let runtime = ClientTcpPathSessionRuntime {
        path,
        path_index,
        security,
        codec_limits,
        mux_limits,
        stream_frame_queue,
    };
    let mut state = ClientTcpPathSessionState {
        connection: None,
        streams: HashMap::new(),
        next_stream_id: 0,
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
            command = commands.recv() => {
                match command {
                    Some(command) => {
                        if let Err(err) = handle_connected_client_tcp_command(
                            command,
                            state.connection.as_mut().expect("checked connected TCP path session"),
                            &mut state.streams,
                            &mut state.next_stream_id,
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
            target,
            ingress,
            class,
            session_commands,
            response,
        } => match connect_client_tcp_path(
            &runtime.path,
            runtime.path_index,
            &runtime.security,
            runtime.codec_limits,
            runtime.mux_limits,
        )
        .await
        {
            Ok(mut connected) => {
                let open = ClientTcpOpenStreamRequest {
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
                    &mut state.next_stream_id,
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
    next_stream_id: &mut u64,
    stream_frame_queue: usize,
) -> Result<(), RuntimeError> {
    match command {
        TcpPathSessionCommand::OpenStream {
            target,
            ingress,
            class,
            session_commands,
            response,
        } => {
            let open = ClientTcpOpenStreamRequest {
                target,
                ingress,
                class,
                session_commands,
                response,
            };
            open_client_tcp_stream_on_connection(
                connection,
                open,
                streams,
                next_stream_id,
                stream_frame_queue,
            )
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
    let (session_hello, session_auth, path_join) =
        authenticated_path_join_frames(security, path, path_id, UnderlayProtocol::Tcp)?;

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
    next_stream_id: &mut u64,
    stream_frame_queue: usize,
) -> Result<(), RuntimeError> {
    let stream_id = StreamId(*next_stream_id);
    *next_stream_id = next_stream_id
        .checked_add(1)
        .ok_or(RuntimeError::Protocol("TCP stream ID overflow"))?;
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
) -> Result<(), RuntimeError> {
    connection.next_heartbeat_at = tokio::time::Instant::now() + connection.heartbeat_interval;
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
                    commands: pending.session_commands,
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
    let payload = mux_limits.max_payload_bytes.max(1);
    (mux_limits.max_reorder_bytes / payload)
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
        let tcp_sessions = tcp_paths
            .iter()
            .cloned()
            .enumerate()
            .map(|(path_index, path)| {
                ClientTcpPathSessionHandle::new(
                    path,
                    path_index,
                    security.clone(),
                    codec_limits,
                    mux_limits,
                    tcp_session_command_queue(resources),
                    tcp_stream_frame_queue(mux_limits),
                )
            })
            .collect::<Vec<_>>();
        Ok(Self {
            tcp_paths: Arc::new(tcp_paths),
            udp_paths: Arc::new(udp_paths),
            tcp_sessions: Arc::new(tcp_sessions),
            health: Arc::new(Mutex::new(health)),
            traffic_policy,
            codec_limits,
            mux_limits,
            security,
        })
    }

    fn classify_tcp_target(&self, target: &TargetAddr) -> TrafficClass {
        self.traffic_policy.classify_tcp_target(target)
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
                    measured_rate_bps: None,
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
            let class = context.classify_tcp_target(&request.target);
            let remote = match open_remote_stream(
                &context,
                request.target,
                IngressKind::Socks5,
                class,
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
                relay_tcp_stream(stream, remote.stream, context.mux_limits).await
            }
            .await;
            if let Ok(stats) = &result {
                context.mark_tcp_path_delivery(path_index, *stats);
            }
            if relay_error_is_tcp_path_failure(&result) {
                context.mark_tcp_path_failure(path_index);
            }
            context.release_tcp_path_load(path_index);
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
    let class = context.classify_tcp_target(&request.target);
    let remote =
        match open_remote_stream(&context, request.target, IngressKind::HttpConnect, class).await {
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
        relay_tcp_stream(stream, remote.stream, context.mux_limits).await
    }
    .await;
    if let Ok(stats) = &result {
        context.mark_tcp_path_delivery(path_index, *stats);
    }
    if relay_error_is_tcp_path_failure(&result) {
        context.mark_tcp_path_failure(path_index);
    }
    context.release_tcp_path_load(path_index);
    result.map(|_| ())
}

struct OpenedRemoteStream {
    stream: TcpPathStream,
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
        match context
            .tcp_sessions
            .get(path_index)
            .ok_or(RuntimeError::NoSchedulableTcpPath)?
            .open_stream(target.clone(), ingress, class)
            .await
        {
            Ok(stream) => {
                context.mark_tcp_path_open_success(path_index, started_at.elapsed());
                return Ok(OpenedRemoteStream { stream, path_index });
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
        context.mark_udp_path_delivery(session.path_index, session.delivery_stats());
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
    let mut streams: HashMap<StreamId, mpsc::Sender<Result<Frame, RuntimeError>>> = HashMap::new();
    let mut draining = false;

    loop {
        tokio::select! {
            command = commands_rx.recv() => {
                match command {
                    Some(TcpPathSessionCommand::SendFrame(frame)) => {
                        if !server_write_tcp_path_frame(&mut writer, &frame).await? {
                            return Ok(());
                        }
                    }
                    Some(TcpPathSessionCommand::CloseStream(stream_id)) => {
                        streams.remove(&stream_id);
                        if draining && streams.is_empty() {
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
            frame = path_frames.recv() => {
                match frame.ok_or(RuntimeError::TcpPathSessionClosed)?? {
                    Frame::OpenStream {
                        stream_id,
                        target,
                        ..
                    } if !draining => {
                        if streams.len() >= context.max_tcp_streams {
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
                            continue;
                        }
                        outbound::validate_target(&target)?;
                        context.outbound.ensure_supports(TargetProtocol::Tcp)?;
                        if streams.contains_key(&stream_id) {
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
                            continue;
                        }
                        let (stream_tx, stream_rx) =
                            mpsc::channel(tcp_stream_frame_queue(context.mux_limits));
                        streams.insert(stream_id, stream_tx);
                        let stream = TcpPathStream {
                            stream_id,
                            max_offset: context.mux_limits.max_stream_window_bytes,
                            commands: commands_tx.clone(),
                            frames: stream_rx,
                        };
                        let stream_context = context.clone();
                        tokio::spawn(async move {
                            if let Err(err) =
                                run_server_tcp_stream(stream_context, stream, target).await
                            {
                                eprintln!("warning: server TCP stream failed: {err}");
                            }
                        });
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
                        route_server_tcp_stream_frame(
                            &mut streams,
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
                        route_server_tcp_stream_frame(
                            &mut streams,
                            stream_id,
                            Frame::StreamAck { stream_id, ranges },
                        )
                        .await?;
                    }
                    Frame::StreamMaxData {
                        stream_id,
                        max_offset,
                    } => {
                        route_server_tcp_stream_frame(
                            &mut streams,
                            stream_id,
                            Frame::StreamMaxData {
                                stream_id,
                                max_offset,
                            },
                        )
                        .await?;
                    }
                    Frame::StreamFin { stream_id } => {
                        route_server_tcp_stream_frame(
                            &mut streams,
                            stream_id,
                            Frame::StreamFin { stream_id },
                        )
                        .await?;
                    }
                    Frame::StreamReset { stream_id, reason } => {
                        route_server_tcp_stream_frame(
                            &mut streams,
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
                        if streams.is_empty() {
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

async fn route_server_tcp_stream_frame(
    streams: &mut HashMap<StreamId, mpsc::Sender<Result<Frame, RuntimeError>>>,
    stream_id: StreamId,
    frame: Frame,
) -> Result<(), RuntimeError> {
    let Some(stream) = streams.get_mut(&stream_id) else {
        return Err(RuntimeError::Protocol(
            "frame for unknown server TCP stream",
        ));
    };
    stream
        .send(Ok(frame))
        .await
        .map_err(|_| RuntimeError::TcpPathSessionClosed)
}

async fn run_server_tcp_stream(
    context: ServerPathContext,
    stream: TcpPathStream,
    target: TargetAddr,
) -> Result<(), RuntimeError> {
    let stream_id = stream.stream_id;
    let outbound_stream =
        match outbound::connect_tcp(&context.outbound, &target, Duration::from_secs(10)).await {
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
    let result = relay_tcp_stream(outbound_stream, stream, context.mux_limits).await;
    result.map(|_| ())
}

fn tcp_server_session_command_queue(context: &ServerPathContext) -> usize {
    context.max_tcp_streams.clamp(1, 1024)
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
    let chunk_size = mux_limits.max_payload_bytes.clamp(1, 16 * 1024);
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
                        path_stream.send_frame(recv_stream.ack_frame()).await?;
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
    send_stream.repair_bytes() < mux_limits.max_tcp_path_inflight_bytes
}

fn tcp_relay_read_budget(
    send_stream: &ReliableSendStream,
    mux_limits: MuxLimits,
    buffer_len: usize,
) -> usize {
    mux_limits
        .max_tcp_path_inflight_bytes
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
    stats: PathDeliveryStats,
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
            stats: PathDeliveryStats::default(),
        })
    }

    async fn send_to(
        &mut self,
        target: TargetAddr,
        payload: Bytes,
        ttl_ms: u32,
    ) -> Result<Bytes, RuntimeError> {
        let flow_id = self.ensure_flow(target).await?;
        let request_len = payload.len();
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
            } if response_flow_id == flow_id => {
                self.stats.record_payload_bytes(request_len);
                self.stats.record_payload_bytes(payload.len());
                Ok(payload)
            }
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

    fn delivery_stats(&self) -> PathDeliveryStats {
        self.stats
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
    use crate::config::{SharedSecret, TcpPortClassRule, TrafficPolicy};
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
            max_tcp_streams: resources.max_streams,
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

    async fn drive_socks5_echo_client<S>(client: &mut S, target_addr: SocketAddr)
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
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
    fn measured_udp_delivery_rate_updates_next_datagram_order() {
        let hinted_slow_path = "udp://127.0.0.1:10015?srtt-ms=20&rate-mbps=10"
            .parse::<PathSpec>()
            .expect("hinted slow path");
        let hinted_fast_path = "udp://127.0.0.1:10016?srtt-ms=20&rate-mbps=100"
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
                .ordered_udp_path_indices(1024 * 1024)
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
                .ordered_udp_path_indices(1024 * 1024)
                .first()
                .copied(),
            Some(0)
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
            default_tcp_class: TrafficClass::Interactive,
            tcp_port_rules: vec![TcpPortClassRule {
                port: target_addr.port(),
                class: TrafficClass::Bulk,
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
