use super::*;
use tokio::net::lookup_host;
use tokio::sync::Mutex as AsyncMutex;

#[derive(Clone)]
pub(super) struct ClientUdpPathSessionHandle {
    runtime: ClientUdpPathSessionRuntime,
    connection: Arc<AsyncMutex<Option<ClientUdpPathConnection>>>,
}

impl std::fmt::Debug for ClientUdpPathSessionHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientUdpPathSessionHandle")
            .finish_non_exhaustive()
    }
}

impl ClientUdpPathSessionHandle {
    pub(super) fn new(runtime: ClientUdpPathSessionRuntime) -> Self {
        Self {
            runtime,
            connection: Arc::new(AsyncMutex::new(None)),
        }
    }

    #[cfg(test)]
    pub(super) fn session_id(&self) -> SessionId {
        self.runtime.session_id
    }

    pub(super) async fn open_stream(
        &self,
        stream_id: StreamId,
        target: TargetAddr,
        ingress: IngressKind,
        lane: FlowLane,
        role: StreamOpenRole,
    ) -> Result<TcpPathStream, RuntimeError> {
        let connection = self.ensure_connection().await?;
        match open_client_udp_stream_on_connection(
            connection,
            stream_id,
            target.clone(),
            ingress,
            lane,
            role,
            self.runtime.clone(),
        )
        .await
        {
            Ok(stream) => Ok(stream),
            Err(err) if udp_carrier_open_error_is_path_retryable(&err) => {
                self.drop_connection().await;
                let connection = self.ensure_connection().await?;
                open_client_udp_stream_on_connection(
                    connection,
                    stream_id,
                    target,
                    ingress,
                    lane,
                    role,
                    self.runtime.clone(),
                )
                .await
            }
            Err(err) => Err(err),
        }
    }

    pub(super) async fn open_datagram_stream(
        &self,
    ) -> Result<ClientUdpDatagramStream, RuntimeError> {
        let connection = self.ensure_connection().await?;
        match open_client_udp_datagram_stream(connection, self.runtime.clone()).await {
            Ok(stream) => Ok(stream),
            Err(err) if udp_carrier_open_error_is_path_retryable(&err) => {
                self.drop_connection().await;
                let connection = self.ensure_connection().await?;
                open_client_udp_datagram_stream(connection, self.runtime.clone()).await
            }
            Err(err) => Err(err),
        }
    }

    async fn ensure_connection(&self) -> Result<UdpPathConnection, RuntimeError> {
        let mut current = self.connection.lock().await;
        if let Some(connection) = current.as_ref() {
            return Ok(connection.connection.clone());
        }
        let connection = connect_client_udp_path(&self.runtime).await?;
        let carrier_connection = connection.connection.clone();
        spawn_client_udp_path_metrics(self.runtime.clone(), carrier_connection.clone());
        *current = Some(connection);
        Ok(carrier_connection)
    }

    async fn drop_connection(&self) {
        let mut current = self.connection.lock().await;
        if let Some(connection) = current.take() {
            connection.connection.close();
        }
    }
}

#[derive(Clone)]
pub(super) struct ClientUdpPathSessionRuntime {
    pub(super) path: PathSpec,
    pub(super) path_index: usize,
    pub(super) session_id: SessionId,
    pub(super) security: SecurityConfig,
    pub(super) codec_limits: CodecLimits,
    pub(super) mux_limits: MuxLimits,
    pub(super) stream_frame_queue: usize,
    pub(super) health: Arc<Mutex<ClientPathHealth>>,
}

struct ClientUdpPathConnection {
    _endpoint: UdpPathEndpoint,
    connection: UdpPathConnection,
}

#[derive(Debug)]
pub(super) enum UdpPathEndpoint {
    CustomLab(udp_carrier::Endpoint),
    Quic(quic_carrier::Endpoint),
}

#[derive(Debug, Clone)]
pub(super) enum UdpPathConnection {
    CustomLab(udp_carrier::Connection),
    Quic(quic_carrier::Connection),
}

#[derive(Debug, Default)]
struct UdpPathMetricTracker {
    quic: QuicPathMetricTracker,
}

#[derive(Debug, Default)]
struct QuicPathMetricTracker {
    last_tx_bytes: u64,
    last_rx_ack_frames: u64,
    last_tx_stream_frames: u64,
    last_tx_datagram_frames: u64,
    last_observed_at: Option<Instant>,
    app_tx_bytes_pending_sample: u64,
    app_tx_sample_started_at: Option<Instant>,
    delivery_rate_bps: Option<f64>,
    delivery_sample_count: u64,
    last_delivery_sample_at: Option<Instant>,
    min_rtt: Option<Duration>,
}

#[derive(Debug)]
pub(super) enum UdpPathSendStream {
    CustomLab(udp_carrier::SendStream),
    Quic(quic_carrier::SendStream),
}

#[derive(Debug)]
pub(super) enum UdpPathRecvStream {
    CustomLab(udp_carrier::RecvStream),
    Quic(quic_carrier::RecvStream),
}

impl UdpPathEndpoint {
    async fn bind_server(
        path: &PathSpec,
        context: &ServerPathContext,
    ) -> Result<Self, RuntimeError> {
        let addr = resolve_first_socket_addr(path).await?;
        match path.metadata.udp_engine {
            UdpEngine::CustomLab => Ok(Self::CustomLab(
                udp_carrier::Endpoint::bind_server(
                    addr,
                    context.security.secret.as_bytes(),
                    context.security.cipher,
                    context.mux_limits,
                    context.codec_limits,
                )
                .await?,
            )),
            UdpEngine::Quic => Ok(Self::Quic(
                quic_carrier::Endpoint::bind_server(
                    addr,
                    context.security.secret.as_bytes(),
                    context.mux_limits,
                )
                .await?,
            )),
        }
    }

    async fn bind_client(
        path: &PathSpec,
        local_addr: SocketAddr,
        runtime: &ClientUdpPathSessionRuntime,
    ) -> Result<Self, RuntimeError> {
        match path.metadata.udp_engine {
            UdpEngine::CustomLab => Ok(Self::CustomLab(
                udp_carrier::Endpoint::bind_client(
                    local_addr,
                    runtime.security.secret.as_bytes(),
                    runtime.security.cipher,
                    runtime.mux_limits,
                    runtime.codec_limits,
                )
                .await?,
            )),
            UdpEngine::Quic => Ok(Self::Quic(
                quic_carrier::Endpoint::bind_client(
                    local_addr,
                    runtime.security.secret.as_bytes(),
                    runtime.mux_limits,
                )
                .await?,
            )),
        }
    }

    async fn connect(&self, remote_addr: SocketAddr) -> Result<UdpPathConnection, RuntimeError> {
        match self {
            Self::CustomLab(endpoint) => Ok(UdpPathConnection::CustomLab(
                endpoint.connect(remote_addr).await?,
            )),
            Self::Quic(endpoint) => Ok(UdpPathConnection::Quic(
                endpoint.connect(remote_addr).await?,
            )),
        }
    }

    async fn accept(&self) -> Option<UdpPathConnection> {
        match self {
            Self::CustomLab(endpoint) => endpoint.accept().await.map(UdpPathConnection::CustomLab),
            Self::Quic(endpoint) => endpoint.accept().await.map(UdpPathConnection::Quic),
        }
    }

    #[cfg(test)]
    pub(super) fn local_addr(&self) -> Result<SocketAddr, std::io::Error> {
        match self {
            Self::CustomLab(endpoint) => endpoint.local_addr(),
            Self::Quic(endpoint) => endpoint.local_addr(),
        }
    }
}

impl UdpPathConnection {
    async fn open_bi(&self) -> Result<(UdpPathSendStream, UdpPathRecvStream), RuntimeError> {
        match self {
            Self::CustomLab(connection) => {
                let (send, recv) = connection.open_bi().await?;
                Ok((
                    UdpPathSendStream::CustomLab(send),
                    UdpPathRecvStream::CustomLab(recv),
                ))
            }
            Self::Quic(connection) => {
                let (send, recv) = connection.open_bi().await?;
                Ok((UdpPathSendStream::Quic(send), UdpPathRecvStream::Quic(recv)))
            }
        }
    }

    async fn accept_bi(&self) -> Result<(UdpPathSendStream, UdpPathRecvStream), RuntimeError> {
        match self {
            Self::CustomLab(connection) => {
                let (send, recv) = connection.accept_bi().await?;
                Ok((
                    UdpPathSendStream::CustomLab(send),
                    UdpPathRecvStream::CustomLab(recv),
                ))
            }
            Self::Quic(connection) => {
                let (send, recv) = connection.accept_bi().await?;
                Ok((UdpPathSendStream::Quic(send), UdpPathRecvStream::Quic(recv)))
            }
        }
    }

    fn close(&self) {
        match self {
            Self::CustomLab(connection) => connection.close(),
            Self::Quic(connection) => connection.close(),
        }
    }

    fn is_closed(&self) -> bool {
        match self {
            Self::CustomLab(connection) => connection.is_closed(),
            Self::Quic(connection) => connection.is_closed(),
        }
    }

    async fn tx_metrics(
        &self,
        tracker: &mut UdpPathMetricTracker,
        direction: u8,
    ) -> Option<udp_carrier::UdpCarrierPathMetrics> {
        match self {
            Self::CustomLab(connection) => Some(connection.tx_metrics().await),
            Self::Quic(connection) => {
                let stats = connection.stats();
                let congestion = connection.congestion_metrics();
                Some(tracker.quic.observe(stats, congestion, direction))
            }
        }
    }
}

impl QuicPathMetricTracker {
    fn observe(
        &mut self,
        stats: quinn::ConnectionStats,
        congestion: quic_carrier::CongestionMetrics,
        direction: u8,
    ) -> udp_carrier::UdpCarrierPathMetrics {
        let now = Instant::now();
        let elapsed = self
            .last_observed_at
            .map(|seen| now.saturating_duration_since(seen))
            .unwrap_or_default();
        let tx_delta = stats.udp_tx.bytes.saturating_sub(self.last_tx_bytes);
        let ack_delta = stats.frame_rx.acks.saturating_sub(self.last_rx_ack_frames);
        let app_frame_delta = stats
            .frame_tx
            .stream
            .saturating_sub(self.last_tx_stream_frames)
            .saturating_add(
                stats
                    .frame_tx
                    .datagram
                    .saturating_sub(self.last_tx_datagram_frames),
            );
        self.last_tx_bytes = stats.udp_tx.bytes;
        self.last_rx_ack_frames = stats.frame_rx.acks;
        self.last_tx_stream_frames = stats.frame_tx.stream;
        self.last_tx_datagram_frames = stats.frame_tx.datagram;
        self.last_observed_at = Some(now);

        if stats.path.rtt > Duration::ZERO {
            self.min_rtt = Some(
                self.min_rtt
                    .map_or(stats.path.rtt, |previous| previous.min(stats.path.rtt)),
            );
        }
        let rtt = stats.path.rtt.max(Duration::from_millis(1));
        let min_rtt = self.min_rtt.unwrap_or(rtt);
        let inflight_hi = congestion
            .congestion_window
            .max(stats.path.cwnd)
            .max(stats.path.current_mtu as u64) as usize;
        let fallback_rate = congestion
            .pacing_rate_bps
            .map(|rate| rate.max(1) as f64)
            .unwrap_or_else(|| inflight_hi as f64 * 8.0 / rtt.as_secs_f64().max(0.001));

        if app_frame_delta > 0 && tx_delta > 0 {
            if self.app_tx_bytes_pending_sample == 0 {
                self.app_tx_sample_started_at = Some(now.checked_sub(elapsed).unwrap_or(now));
            }
            self.app_tx_bytes_pending_sample =
                self.app_tx_bytes_pending_sample.saturating_add(tx_delta);
        }
        let mut app_limited_low_sample_observed = false;
        if ack_delta > 0 && self.app_tx_bytes_pending_sample > 0 {
            let sample_elapsed = self
                .app_tx_sample_started_at
                .map(|started| now.saturating_duration_since(started))
                .unwrap_or(elapsed);
            if sample_elapsed > Duration::ZERO {
                let sample_bytes = self.app_tx_bytes_pending_sample;
                let sample_rate = (self.app_tx_bytes_pending_sample as f64 * 8.0
                    / sample_elapsed.as_secs_f64())
                .max(1.0);
                let app_limited_low_sample =
                    sample_bytes < inflight_hi as u64 && sample_rate < fallback_rate;
                app_limited_low_sample_observed = app_limited_low_sample;
                if !app_limited_low_sample {
                    self.delivery_sample_count =
                        self.delivery_sample_count.saturating_add(ack_delta);
                    self.last_delivery_sample_at = Some(now);
                    self.delivery_rate_bps = Some(match self.delivery_rate_bps {
                        Some(previous) if sample_rate < previous => {
                            previous.mul_add(0.875, sample_rate * 0.125)
                        }
                        Some(previous) => previous.mul_add(0.25, sample_rate * 0.75),
                        None => sample_rate,
                    });
                } else if self.delivery_rate_bps.is_none() {
                    self.delivery_rate_bps = Some(fallback_rate);
                }
            }
            self.app_tx_bytes_pending_sample = 0;
            self.app_tx_sample_started_at = None;
        }

        let delivery_rate_bps = self.delivery_rate_bps.unwrap_or(fallback_rate).max(1.0);
        udp_carrier::UdpCarrierPathMetrics {
            direction,
            srtt: rtt,
            rttvar: rtt / 4,
            min_rtt,
            min_rtt_observed: stats.path.rtt > Duration::ZERO,
            delivery_rate_bps,
            pacing_rate_bps: congestion
                .pacing_rate_bps
                .map(|rate| rate.max(1) as f64)
                .unwrap_or(delivery_rate_bps),
            inflight_hi,
            bytes_in_flight: 0,
            pending_bytes: 0,
            target_datagram_bytes: stats.path.current_mtu.max(1200) as usize,
            loss_events: stats.path.congestion_events,
            spurious_loss_events: 0,
            packet_loss_threshold: 3,
            pto_count: 0,
            app_limited: app_limited_low_sample_observed
                || (self.app_tx_bytes_pending_sample == 0 && app_frame_delta == 0),
            delivery_sample_count: self.delivery_sample_count,
            last_delivery_sample_at: self.last_delivery_sample_at,
        }
    }
}

impl UdpPathSendStream {
    fn engine(&self) -> UdpEngine {
        match self {
            Self::CustomLab(_) => UdpEngine::CustomLab,
            Self::Quic(_) => UdpEngine::Quic,
        }
    }
}

pub(super) async fn udp_path_read_frame(
    recv: &mut UdpPathRecvStream,
    codec_limits: CodecLimits,
) -> Result<Frame, RuntimeError> {
    match recv {
        UdpPathRecvStream::CustomLab(recv) => {
            Ok(udp_carrier::read_frame(recv, codec_limits).await?)
        }
        UdpPathRecvStream::Quic(recv) => Ok(quic_carrier::read_frame(recv, codec_limits).await?),
    }
}

pub(super) async fn udp_path_write_frame(
    send: &mut UdpPathSendStream,
    frame: &Frame,
    codec_limits: CodecLimits,
) -> Result<(), RuntimeError> {
    match send {
        UdpPathSendStream::CustomLab(send) => {
            udp_carrier::write_frame(send, frame, codec_limits).await?;
        }
        UdpPathSendStream::Quic(send) => {
            quic_carrier::write_frame(send, frame, codec_limits).await?;
        }
    }
    Ok(())
}

pub(super) fn udp_path_finish_stream(send: &mut UdpPathSendStream) -> Result<(), RuntimeError> {
    match send {
        UdpPathSendStream::CustomLab(send) => Ok(udp_carrier::finish_stream(send)?),
        UdpPathSendStream::Quic(send) => Ok(quic_carrier::finish_stream(send)?),
    }
}

fn udp_path_max_stream_payload_bytes(
    engine: UdpEngine,
    codec_limits: CodecLimits,
    mux_limits: MuxLimits,
) -> usize {
    match engine {
        UdpEngine::CustomLab => udp_carrier::max_stream_payload_bytes(codec_limits, mux_limits),
        UdpEngine::Quic => quic_carrier::max_stream_payload_bytes(codec_limits)
            .min(mux_limits.max_tcp_relay_chunk_bytes)
            .max(1),
    }
}

fn spawn_client_udp_path_metrics(
    runtime: ClientUdpPathSessionRuntime,
    connection: UdpPathConnection,
) {
    tokio::spawn(async move {
        let mut tracker = UdpPathMetricTracker::default();
        loop {
            if connection.is_closed() {
                return;
            }
            let Some(metrics) = connection.tx_metrics(&mut tracker, 1).await else {
                tokio::time::sleep(Duration::from_millis(250)).await;
                continue;
            };
            if let Some(record) = runtime
                .health
                .lock()
                .expect("client UDP carrier health lock")
                .udp
                .get_mut(runtime.path_index)
            {
                record.mark_udp_carrier_metrics(metrics);
            }
            tokio::time::sleep(udp_carrier_metrics_poll_interval(metrics)).await;
        }
    });
}

fn udp_carrier_metrics_poll_interval(metrics: udp_carrier::UdpCarrierPathMetrics) -> Duration {
    (metrics.srtt / 2)
        .max(Duration::from_millis(10))
        .min(Duration::from_millis(250))
}

pub(super) struct ClientUdpDatagramStream {
    pub(super) send: UdpPathSendStream,
    pub(super) frames: mpsc::Receiver<Result<Frame, RuntimeError>>,
    pub(super) runtime: ClientUdpPathSessionRuntime,
    pub(super) path_id: PathId,
}

pub(super) async fn bind_server_udp_endpoint(
    path: &PathSpec,
    context: &ServerPathContext,
) -> Result<UdpPathEndpoint, RuntimeError> {
    UdpPathEndpoint::bind_server(path, context).await
}

pub(super) async fn run_server_udp_listener(
    endpoint: UdpPathEndpoint,
    context: ServerPathContext,
) -> Result<(), RuntimeError> {
    loop {
        let Some(connection) = endpoint.accept().await else {
            return Err(RuntimeError::Protocol("UDP carrier endpoint closed"));
        };
        let context = context.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_server_udp_connection(connection, context).await {
                warn_unexpected_udp_runtime_error("server UDP carrier connection failed", &err);
            }
        });
    }
}

async fn connect_client_udp_path(
    runtime: &ClientUdpPathSessionRuntime,
) -> Result<ClientUdpPathConnection, RuntimeError> {
    let remote_addr = resolve_first_socket_addr(&runtime.path).await?;
    let local_addr = if remote_addr.ip().is_loopback() {
        SocketAddr::new(remote_addr.ip(), 0)
    } else if remote_addr.is_ipv4() {
        SocketAddr::from(([0, 0, 0, 0], 0))
    } else {
        "[::]:0"
            .parse()
            .expect("static IPv6 unspecified socket addr")
    };
    let endpoint = UdpPathEndpoint::bind_client(&runtime.path, local_addr, runtime).await?;
    let connection = endpoint.connect(remote_addr).await?;
    perform_client_udp_path_handshake(&connection, runtime).await?;
    Ok(ClientUdpPathConnection {
        _endpoint: endpoint,
        connection,
    })
}

async fn perform_client_udp_path_handshake(
    connection: &UdpPathConnection,
    runtime: &ClientUdpPathSessionRuntime,
) -> Result<(), RuntimeError> {
    let (mut send, mut recv) = connection.open_bi().await?;
    let path_id = PathId(runtime.path_index as u16);
    let (session_hello, session_auth, path_join) = authenticated_path_join_frames_for_session(
        &runtime.security,
        &runtime.path,
        path_id,
        UnderlayProtocol::Udp,
        runtime.session_id,
    )?;
    udp_path_write_frame(&mut send, &session_hello, runtime.codec_limits).await?;
    udp_path_write_frame(&mut send, &session_auth, runtime.codec_limits).await?;
    udp_path_write_frame(&mut send, &path_join, runtime.codec_limits).await?;
    udp_path_finish_stream(&mut send)?;

    let mut session_ready = false;
    let mut path_active = false;
    while !session_ready || !path_active {
        match udp_path_read_frame(&mut recv, runtime.codec_limits).await? {
            Frame::SessionReady => session_ready = true,
            Frame::PathStatus {
                status: crate::protocol::PathStatus::Active,
                ..
            } => path_active = true,
            Frame::PathStatus { .. } => {
                return Err(RuntimeError::Protocol(
                    "UDP path session did not become active",
                ));
            }
            Frame::SessionClose { reason } => return Err(RuntimeError::RemoteClosed(reason)),
            _ => {
                return Err(RuntimeError::Protocol(
                    "unexpected UDP path handshake frame",
                ));
            }
        }
    }
    Ok(())
}

async fn open_client_udp_stream_on_connection(
    connection: UdpPathConnection,
    stream_id: StreamId,
    target: TargetAddr,
    ingress: IngressKind,
    lane: FlowLane,
    role: StreamOpenRole,
    runtime: ClientUdpPathSessionRuntime,
) -> Result<TcpPathStream, RuntimeError> {
    let (mut send, mut recv) = connection.open_bi().await?;
    let open = Frame::OpenStream {
        stream_id,
        target,
        ingress,
        outbound: OutboundPolicy::Direct,
        demand: stream_demand_hint_for_lane(lane),
        role,
    };
    udp_path_write_frame(&mut send, &open, runtime.codec_limits).await?;
    let max_offset = loop {
        match udp_path_read_frame(&mut recv, runtime.codec_limits).await? {
            Frame::StreamMaxData {
                stream_id: max_stream_id,
                max_offset,
            } if max_stream_id == stream_id => break max_offset,
            Frame::StreamReset {
                stream_id: reset_stream_id,
                reason,
            } if reset_stream_id == stream_id => return Err(RuntimeError::RemoteReset(reason)),
            Frame::PathStatus { .. } | Frame::SessionReady => {}
            Frame::SessionClose { reason } => return Err(RuntimeError::RemoteClosed(reason)),
            _ => {
                return Err(RuntimeError::Protocol(
                    "unexpected UDP carrier stream open frame",
                ));
            }
        }
    };
    let (commands, receivers) = tcp_path_session_command_channels(udp_path_command_queue(
        runtime.mux_limits,
        runtime.codec_limits,
    ));
    let (frames_tx, frames_rx) = mpsc::channel(runtime.stream_frame_queue);
    tokio::spawn(run_client_udp_stream(
        send,
        recv,
        stream_id,
        runtime.codec_limits,
        runtime.stream_frame_queue,
        receivers,
        frames_tx,
    ));
    Ok(TcpPathStream {
        stream_id,
        max_offset,
        lane,
        underlay: UnderlayProtocol::Udp,
        max_frame_payload_bytes: udp_path_max_stream_payload_bytes(
            runtime.path.metadata.udp_engine,
            runtime.codec_limits,
            runtime.mux_limits,
        ),
        output: TcpPathStreamOutput::Fixed(commands),
        frames: frames_rx,
    })
}

async fn open_client_udp_datagram_stream(
    connection: UdpPathConnection,
    runtime: ClientUdpPathSessionRuntime,
) -> Result<ClientUdpDatagramStream, RuntimeError> {
    let (send, recv) = connection.open_bi().await?;
    let frames = spawn_udp_carrier_reader(recv, runtime.codec_limits, runtime.stream_frame_queue);
    Ok(ClientUdpDatagramStream {
        send,
        frames,
        path_id: PathId(runtime.path_index as u16),
        runtime,
    })
}

fn spawn_udp_carrier_reader(
    mut recv: UdpPathRecvStream,
    codec_limits: CodecLimits,
    queue_size: usize,
) -> mpsc::Receiver<Result<Frame, RuntimeError>> {
    let (frames_tx, frames_rx) = mpsc::channel(queue_size);
    tokio::spawn(async move {
        loop {
            let frame = match udp_path_read_frame(&mut recv, codec_limits).await {
                Ok(frame) => Ok(frame),
                Err(err) if udp_path_frame_finished(&err) => {
                    Err(RuntimeError::TcpPathSessionClosed)
                }
                Err(err) => Err(err),
            };
            let done = frame.is_err();
            if frames_tx.send(frame).await.is_err() || done {
                return;
            }
        }
    });
    frames_rx
}

async fn run_client_udp_stream(
    mut send: UdpPathSendStream,
    recv: UdpPathRecvStream,
    stream_id: StreamId,
    codec_limits: CodecLimits,
    reader_queue_size: usize,
    mut commands: TcpPathSessionCommandReceivers,
    frames: mpsc::Sender<Result<Frame, RuntimeError>>,
) {
    let mut carrier_frames = spawn_udp_carrier_reader(recv, codec_limits, reader_queue_size);
    loop {
        let command_may_recv = !tcp_path_receivers_closed(&commands);
        if !command_may_recv {
            let _ = udp_path_finish_stream(&mut send);
            return;
        }
        tokio::select! {
            frame = carrier_frames.recv() => {
                match frame {
                    Some(Ok(Frame::Ping { nonce })) => {
                        if let Err(err) = udp_path_write_frame(&mut send, &Frame::Pong { nonce }, codec_limits).await {
                            let _ = frames.send(Err(err)).await;
                            return;
                        }
                    }
                    Some(Ok(frame @ (Frame::StreamData { stream_id: received_stream_id, .. }
                        | Frame::StreamAck { stream_id: received_stream_id, .. }
                        | Frame::StreamMaxData { stream_id: received_stream_id, .. }
                        | Frame::StreamFin { stream_id: received_stream_id, .. }
                        | Frame::StreamReset { stream_id: received_stream_id, .. })))
                        if received_stream_id == stream_id =>
                    {
                        if frames.send(Ok(frame)).await.is_err() {
                            return;
                        }
                    }
                    Some(Ok(frame @ Frame::PathStatus { .. })) => {
                        if frames.send(Ok(frame)).await.is_err() {
                            return;
                        }
                    }
                    Some(Ok(Frame::SessionClose { reason })) => {
                        let _ = frames.send(Err(RuntimeError::RemoteClosed(reason))).await;
                        return;
                    }
                    Some(Ok(_)) => {
                        let _ = frames
                            .send(Err(RuntimeError::Protocol("unexpected UDP carrier reliable stream frame")))
                            .await;
                        return;
                    }
                    Some(Err(err)) => {
                        let _ = frames.send(Err(err)).await;
                        return;
                    }
                    None => {
                        let _ = frames.send(Err(RuntimeError::TcpPathSessionClosed)).await;
                        return;
                    }
                }
            }
            command = recv_tcp_path_command(&mut commands), if command_may_recv => {
                match command {
                    Some(command) => {
                        let pending_bytes = tcp_path_command_pending_bytes(&command);
                        let result = async {
                            match command {
                                TcpPathSessionCommand::SendFrame(frame) => {
                                    udp_path_write_frame(&mut send, &frame, codec_limits).await?;
                                    Ok(false)
                                }
                                TcpPathSessionCommand::CloseStream(close_stream_id) => {
                                    if close_stream_id == stream_id {
                                        let _ = udp_path_finish_stream(&mut send);
                                        return Ok(true);
                                    }
                                    Ok(false)
                                }
                                TcpPathSessionCommand::OpenStream { .. } => {
                                    Err(RuntimeError::Protocol("client UDP carrier stream received open command"))
                                }
                            }
                        }
                        .await;
                        commands.release_pending_command_bytes(pending_bytes);
                        match result {
                            Ok(false) => {}
                            Ok(true) => return,
                            Err(err) => {
                                let _ = frames.send(Err(err)).await;
                                return;
                            }
                        }
                    }
                    None => {}
                }
            }
        }
    }
}

async fn handle_server_udp_connection(
    connection: UdpPathConnection,
    context: ServerPathContext,
) -> Result<(), RuntimeError> {
    let (session_id, path_id, capabilities) =
        accept_server_udp_path_handshake(&connection, &context).await?;
    spawn_server_udp_carrier_metrics(context.clone(), session_id, path_id, connection.clone());
    loop {
        let (send, recv) = match connection.accept_bi().await {
            Ok(streams) => streams,
            Err(err) => return Err(err),
        };
        let context = context.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_server_udp_bidi_stream(
                send,
                recv,
                context,
                session_id,
                path_id,
                capabilities,
            )
            .await
            {
                warn_unexpected_udp_runtime_error("server UDP carrier stream failed", &err);
            }
        });
    }
}

fn spawn_server_udp_carrier_metrics(
    context: ServerPathContext,
    session_id: SessionId,
    path_id: PathId,
    connection: UdpPathConnection,
) {
    tokio::spawn(async move {
        let mut tracker = UdpPathMetricTracker::default();
        loop {
            if connection.is_closed() {
                return;
            }
            let Some(metrics) = connection.tx_metrics(&mut tracker, 2).await else {
                tokio::time::sleep(Duration::from_millis(250)).await;
                continue;
            };
            if metrics.delivery_sample_count > 0 {
                context.tcp_streams.record_local_path_metrics(
                    session_id,
                    UnderlayProtocol::Udp,
                    path_id,
                    path_metrics_from_udp_carrier(path_id, metrics),
                );
            }
            tokio::time::sleep(udp_carrier_metrics_poll_interval(metrics)).await;
        }
    });
}

fn path_metrics_from_udp_carrier(
    path_id: PathId,
    metrics: udp_carrier::UdpCarrierPathMetrics,
) -> PathMetrics {
    PathMetrics {
        path_id,
        underlay: UnderlayProtocol::Udp,
        direction: match metrics.direction {
            1 => PathMetricDirection::ClientToServer,
            2 => PathMetricDirection::ServerToClient,
            _ => PathMetricDirection::ServerToClient,
        },
        metric_epoch: metric_epoch_now(),
        metric_age_us: metrics
            .last_delivery_sample_at
            .map(|seen| {
                let micros = Instant::now().saturating_duration_since(seen).as_micros();
                u32::try_from(micros).unwrap_or(u32::MAX)
            })
            .unwrap_or(0),
        min_rtt_us: duration_to_micros_u32(metrics.min_rtt),
        srtt_us: duration_to_micros_u32(metrics.srtt),
        rttvar_us: duration_to_micros_u32(metrics.rttvar),
        jitter_us: duration_to_micros_u32(metrics.rttvar),
        delivery_rate_bps: metrics.delivery_rate_bps.max(1.0).round() as u64,
        pacing_rate_bps: metrics.pacing_rate_bps.max(1.0).round() as u64,
        loss_ppm: 0,
        ecn_ppm: 0,
        bytes_in_flight: metrics.bytes_in_flight as u64,
        queue_bytes: metrics
            .pending_bytes
            .saturating_sub(metrics.bytes_in_flight) as u64,
        inflight_limit_bytes: metrics.inflight_hi as u64,
        inflight_hi_bytes: metrics.inflight_hi as u64,
        confidence_ppm: ratio_to_ppm((metrics.delivery_sample_count as f64 / 8.0).clamp(0.0, 1.0)),
        app_limited: metrics.app_limited,
        has_ack_derived_data_sample: metrics.delivery_sample_count > 0,
        data_sample_count: u32::try_from(metrics.delivery_sample_count).unwrap_or(u32::MAX),
    }
}

fn duration_to_micros_u32(duration: Duration) -> u32 {
    u32::try_from(duration.as_micros()).unwrap_or(u32::MAX)
}

async fn accept_server_udp_path_handshake(
    connection: &UdpPathConnection,
    context: &ServerPathContext,
) -> Result<(SessionId, PathId, PathCapabilities), RuntimeError> {
    let (mut send, mut recv) = connection.accept_bi().await?;
    let session_id = match udp_path_read_frame(&mut recv, context.codec_limits).await? {
        Frame::SessionHello { session_id } => session_id,
        _ => return Err(RuntimeError::Protocol("expected UDP carrier SESSION_HELLO")),
    };
    let authenticator = SessionAuthenticator::new(context.security.secret.as_bytes())?;
    let now_unix_secs = current_unix_secs()?;
    let auth_freshness_window_secs = context.security.auth_freshness_window.as_secs();
    match udp_path_read_frame(&mut recv, context.codec_limits).await? {
        Frame::SessionAuth {
            session_id: auth_session_id,
            nonce,
            issued_at_unix_secs,
            auth_tag,
        } if auth_session_id == session_id
            && authenticator.verify_session_auth(SessionAuthCheck {
                session_id,
                nonce,
                issued_at_unix_secs,
                tag: auth_tag,
                now_unix_secs,
                freshness_window_secs: auth_freshness_window_secs,
            }) => {}
        _ => return Err(RuntimeError::Protocol("invalid UDP carrier SESSION_AUTH")),
    }
    let (path_id, capabilities) = match udp_path_read_frame(&mut recv, context.codec_limits).await?
    {
        Frame::PathJoin {
            session_id: join_session_id,
            path_id,
            underlay,
            nonce,
            issued_at_unix_secs,
            capabilities,
            auth_tag,
        } if join_session_id == session_id
            && underlay == UnderlayProtocol::Udp
            && authenticator.verify_path_join(PathJoinAuthCheck {
                session_id,
                path_id,
                underlay,
                nonce,
                issued_at_unix_secs,
                capabilities,
                tag: auth_tag,
                now_unix_secs,
                freshness_window_secs: auth_freshness_window_secs,
            })
            && context.accept_path_join_nonce(session_id, path_id, underlay, nonce) =>
        {
            (path_id, capabilities)
        }
        _ => return Err(RuntimeError::Protocol("invalid UDP carrier PATH_JOIN")),
    };

    udp_path_write_frame(&mut send, &Frame::SessionReady, context.codec_limits).await?;
    udp_path_write_frame(
        &mut send,
        &Frame::PathStatus {
            path_id,
            status: crate::protocol::PathStatus::Active,
            capabilities,
        },
        context.codec_limits,
    )
    .await?;
    udp_path_finish_stream(&mut send)?;
    Ok((session_id, path_id, capabilities))
}

async fn handle_server_udp_bidi_stream(
    mut send: UdpPathSendStream,
    mut recv: UdpPathRecvStream,
    context: ServerPathContext,
    session_id: SessionId,
    path_id: PathId,
    capabilities: PathCapabilities,
) -> Result<(), RuntimeError> {
    match udp_path_read_frame(&mut recv, context.codec_limits).await? {
        Frame::OpenStream {
            stream_id,
            target,
            demand,
            role,
            ..
        } => {
            let lane = flow_lane_from_stream_demand_hint(demand);
            handle_server_udp_reliable_stream(
                send,
                recv,
                context,
                ServerUdpReliableStreamContext {
                    session_id,
                    path_id,
                    capabilities,
                    stream_id,
                    target,
                    lane,
                    role,
                },
            )
            .await
        }
        Frame::OpenDatagramFlow {
            flow_id, target, ..
        } => {
            handle_server_udp_datagram_stream(
                send,
                recv,
                context,
                ServerUdpDatagramStreamContext {
                    flow_id,
                    target,
                    lane: FlowLane::RealtimeDatagram,
                },
            )
            .await
        }
        Frame::Ping { nonce } => {
            udp_path_write_frame(&mut send, &Frame::Pong { nonce }, context.codec_limits).await?;
            udp_path_finish_stream(&mut send)?;
            Ok(())
        }
        _ => Err(RuntimeError::Protocol(
            "unexpected first UDP carrier stream frame",
        )),
    }
}

struct ServerUdpReliableStreamContext {
    session_id: SessionId,
    path_id: PathId,
    capabilities: PathCapabilities,
    stream_id: StreamId,
    target: TargetAddr,
    lane: FlowLane,
    role: StreamOpenRole,
}

async fn handle_server_udp_reliable_stream(
    mut send: UdpPathSendStream,
    recv: UdpPathRecvStream,
    context: ServerPathContext,
    stream_context: ServerUdpReliableStreamContext,
) -> Result<(), RuntimeError> {
    let ServerUdpReliableStreamContext {
        session_id,
        path_id,
        capabilities,
        stream_id,
        target,
        lane,
        role,
    } = stream_context;
    outbound::validate_target(&target)?;
    context.outbound.ensure_supports(TargetProtocol::Tcp)?;
    let duplicate_open_target = target.clone();
    let engine = send.engine();
    let (commands_tx, commands_rx) = tcp_path_session_command_channels(udp_path_command_queue(
        context.mux_limits,
        context.codec_limits,
    ));
    match context.tcp_streams.open_or_attach(
        ServerTcpStreamOpenRequest {
            session_id,
            stream_id,
            target: &target,
            lane,
            attachment: ServerTcpPathAttachment {
                path_id,
                underlay: UnderlayProtocol::Udp,
                commands: commands_tx.clone(),
                max_frame_payload_bytes: udp_path_max_stream_payload_bytes(
                    engine,
                    context.codec_limits,
                    context.mux_limits,
                ),
                role,
            },
        },
        context.mux_limits,
        context.max_tcp_streams,
    )? {
        ServerTcpStreamOpen::New(stream) => {
            let stream_context = context.clone();
            let target = target.clone();
            tokio::spawn(async move {
                if let Err(err) =
                    run_server_tcp_stream(stream_context, session_id, stream, target).await
                {
                    eprintln!("warning: server TCP stream failed: {err}");
                }
            });
        }
        ServerTcpStreamOpen::Existing => {
            context
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
            udp_path_write_frame(
                &mut send,
                &Frame::StreamMaxData {
                    stream_id,
                    max_offset: context.mux_limits.max_stream_window_bytes,
                },
                context.codec_limits,
            )
            .await?;
        }
    }
    run_server_udp_reliable_stream_loop(
        send,
        recv,
        ServerUdpReliableStreamLoop {
            context,
            session_id,
            path_id,
            capabilities,
            stream_id,
            target: duplicate_open_target,
            lane,
            role,
            commands_tx,
            commands_rx,
            engine,
        },
    )
    .await
}

struct ServerUdpReliableStreamLoop {
    context: ServerPathContext,
    session_id: SessionId,
    path_id: PathId,
    capabilities: PathCapabilities,
    stream_id: StreamId,
    target: TargetAddr,
    lane: FlowLane,
    role: StreamOpenRole,
    commands_tx: TcpPathSessionCommandSender,
    commands_rx: TcpPathSessionCommandReceivers,
    engine: UdpEngine,
}

async fn run_server_udp_reliable_stream_loop(
    mut send: UdpPathSendStream,
    recv: UdpPathRecvStream,
    stream_context: ServerUdpReliableStreamLoop,
) -> Result<(), RuntimeError> {
    let ServerUdpReliableStreamLoop {
        context,
        session_id,
        path_id,
        capabilities,
        stream_id,
        target,
        lane: _lane,
        role: _role,
        commands_tx,
        mut commands_rx,
        engine,
    } = stream_context;
    let mut carrier_frames = spawn_udp_carrier_reader(
        recv,
        context.codec_limits,
        tcp_stream_frame_queue(context.mux_limits),
    );

    loop {
        let command_may_recv = !tcp_path_receivers_closed(&commands_rx);
        tokio::select! {
            frame = carrier_frames.recv() => {
                match frame {
                    Some(Ok(frame @ (Frame::StreamData { stream_id: received_stream_id, .. }
                        | Frame::StreamAck { stream_id: received_stream_id, .. }
                        | Frame::StreamMaxData { stream_id: received_stream_id, .. }
                        | Frame::StreamFin { stream_id: received_stream_id, .. }
                        | Frame::StreamReset { stream_id: received_stream_id, .. })))
                        if received_stream_id == stream_id =>
                    {
                        context.tcp_streams.route_frame(session_id, stream_id, frame).await?;
                    }
                    Some(Ok(Frame::StreamDetach { stream_id: detach_stream_id }))
                        if detach_stream_id == stream_id =>
                    {
                        context.tcp_streams.detach_path(
                            session_id,
                            stream_id,
                            UnderlayProtocol::Udp,
                            path_id,
                            &commands_tx,
                        );
                        let _ = udp_path_finish_stream(&mut send);
                        return Ok(());
                    }
                    Some(Ok(Frame::PathMetrics { metrics })) if metrics.path_id == path_id => {
                        context.tcp_streams.record_path_metrics(
                            session_id,
                            UnderlayProtocol::Udp,
                            path_id,
                            metrics,
                        );
                    }
                    Some(Ok(Frame::OpenStream {
                        stream_id: open_stream_id,
                        target: open_target,
                        demand: open_demand,
                        role: open_role,
                        ..
                    })) if open_stream_id == stream_id && open_target == target =>
                    {
                        let updated_lane = flow_lane_from_stream_demand_hint(open_demand);
                        match context.tcp_streams.open_or_attach(
                            ServerTcpStreamOpenRequest {
                                session_id,
                                stream_id,
                                target: &target,
                                lane: updated_lane,
                                attachment: ServerTcpPathAttachment {
                                    path_id,
                                    underlay: UnderlayProtocol::Udp,
                                    commands: commands_tx.clone(),
                                    max_frame_payload_bytes: udp_path_max_stream_payload_bytes(
                                        engine,
                                        context.codec_limits,
                                        context.mux_limits,
                                    ),
                                    role: open_role,
                                },
                            },
                            context.mux_limits,
                            context.max_tcp_streams,
                        )? {
                            ServerTcpStreamOpen::Existing => {
                                context
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
                                udp_path_write_frame(
                                    &mut send,
                                    &Frame::StreamMaxData {
                                        stream_id,
                                        max_offset: context.mux_limits.max_stream_window_bytes,
                                    },
                                    context.codec_limits,
                                )
                                .await?;
                            }
                            ServerTcpStreamOpen::New(_) => {
                                return Err(RuntimeError::Protocol(
                                    "UDP carrier reannouncement opened duplicate stream",
                                ));
                            }
                        }
                        continue;
                    }
                    Some(Ok(Frame::Ping { nonce })) => {
                        udp_path_write_frame(&mut send, &Frame::Pong { nonce }, context.codec_limits).await?;
                    }
                    Some(Ok(Frame::SessionClose { reason })) => return Err(RuntimeError::RemoteClosed(reason)),
                    Some(Ok(frame)) => {
                        log_unexpected_stream_relay_frame(
                            "server UDP carrier reliable",
                            stream_id,
                            &frame,
                        );
                        return Err(RuntimeError::Protocol("unexpected server UDP carrier reliable stream frame"));
                    }
                    Some(Err(RuntimeError::TcpPathSessionClosed)) | None => {
                        context.tcp_streams.detach_path(
                            session_id,
                            stream_id,
                            UnderlayProtocol::Udp,
                            path_id,
                            &commands_tx,
                        );
                        return Ok(());
                    }
                    Some(Err(err)) => return Err(err),
                }
            }
            command = recv_tcp_path_command(&mut commands_rx), if command_may_recv => {
                match command {
                    Some(command) => {
                        let pending_bytes = tcp_path_command_pending_bytes(&command);
                        let result = async {
                            match command {
                                TcpPathSessionCommand::SendFrame(frame) => {
                                    udp_path_write_frame(&mut send, &frame, context.codec_limits).await?;
                                    Ok(false)
                                }
                                TcpPathSessionCommand::CloseStream(close_stream_id) => {
                                    if close_stream_id == stream_id {
                                        context.tcp_streams.detach_path(
                                            session_id,
                                            stream_id,
                                            UnderlayProtocol::Udp,
                                            path_id,
                                            &commands_tx,
                                        );
                                        let _ = udp_path_finish_stream(&mut send);
                                        return Ok(true);
                                    }
                                    Ok(false)
                                }
                                TcpPathSessionCommand::OpenStream { .. } => {
                                    Err(RuntimeError::Protocol("server UDP carrier stream received client open command"))
                                }
                            }
                        }
                        .await;
                        commands_rx.release_pending_command_bytes(pending_bytes);
                        if result? {
                            return Ok(());
                        }
                    }
                    None => {}
                }
            }
        }
    }
}

struct ServerUdpDatagramStreamContext {
    flow_id: DatagramFlowId,
    target: TargetAddr,
    lane: FlowLane,
}

async fn handle_server_udp_datagram_stream(
    send: UdpPathSendStream,
    recv: UdpPathRecvStream,
    context: ServerPathContext,
    stream_context: ServerUdpDatagramStreamContext,
) -> Result<(), RuntimeError> {
    let (commands_tx, mut commands_rx) = tcp_path_session_command_channels(udp_path_command_queue(
        context.mux_limits,
        context.codec_limits,
    ));
    let mut send = send;
    let mut carrier_frames = spawn_udp_carrier_reader(
        recv,
        context.codec_limits,
        udp_path_command_queue(context.mux_limits, context.codec_limits),
    );
    let mut flows = Vec::<ServerUdpDatagramFlow>::new();
    open_server_udp_datagram_flow(
        &context,
        &commands_tx,
        &mut send,
        &mut flows,
        stream_context.flow_id,
        stream_context.target,
        stream_context.lane,
    )
    .await?;
    loop {
        let command_may_recv = !tcp_path_receivers_closed(&commands_rx);
        tokio::select! {
            frame = carrier_frames.recv() => {
                match frame {
                    Some(Ok(Frame::OpenDatagramFlow { flow_id, target, .. })) => {
                        open_server_udp_datagram_flow(
                            &context,
                            &commands_tx,
                            &mut send,
                            &mut flows,
                            flow_id,
                            target,
                            FlowLane::RealtimeDatagram,
                        ).await?;
                    }
                    Some(Ok(Frame::DatagramData { flow_id, datagram_id, ttl_ms, payload })) => {
                        if ttl_ms == 0 {
                            return Err(RuntimeError::Protocol("expired UDP carrier datagram received"));
                        }
                        let flow_index = flows
                            .iter()
                            .position(|flow| flow.flow_id == flow_id)
                            .ok_or(RuntimeError::Protocol("unknown UDP carrier datagram flow"))?;
                        let requests = flows
                            .get(flow_index)
                            .ok_or(RuntimeError::Protocol("unknown UDP carrier datagram flow"))?
                            .requests
                            .clone();
                        match requests.try_send(ServerUdpDatagramRequest { datagram_id, ttl_ms, payload }) {
                            Ok(()) => {
                                udp_path_write_frame(
                                    &mut send,
                                    &Frame::DatagramFeedback {
                                        flow_id,
                                        received: vec![datagram_ack_range(datagram_id)?],
                                    },
                                    context.codec_limits,
                                ).await?;
                            }
                            Err(mpsc::error::TrySendError::Full(_)) => {
                                eprintln!("warning: UDP carrier datagram worker queue full; dropping request");
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => {
                                flows.retain(|flow| flow.flow_id != flow_id);
                                udp_path_write_frame(&mut send, &Frame::DatagramClose { flow_id }, context.codec_limits).await?;
                            }
                        }
                    }
                    Some(Ok(Frame::DatagramFeedback { .. })) => {}
                    Some(Ok(Frame::DatagramClose { flow_id })) => {
                        flows.retain(|flow| flow.flow_id != flow_id);
                        if flows.is_empty() {
                            let _ = udp_path_finish_stream(&mut send);
                            return Ok(());
                        }
                    }
                    Some(Ok(Frame::Ping { nonce })) => {
                        udp_path_write_frame(&mut send, &Frame::Pong { nonce }, context.codec_limits).await?;
                    }
                    Some(Ok(Frame::SessionClose { reason })) => return Err(RuntimeError::RemoteClosed(reason)),
                    Some(Ok(_)) => return Err(RuntimeError::Protocol("unexpected server UDP carrier datagram stream frame")),
                    Some(Err(RuntimeError::TcpPathSessionClosed)) | None => return Ok(()),
                    Some(Err(err)) => return Err(err),
                }
            }
            command = recv_tcp_path_command(&mut commands_rx), if command_may_recv => {
                match command {
                    Some(command) => {
                        let pending_bytes = tcp_path_command_pending_bytes(&command);
                        let result = async {
                            match command {
                                TcpPathSessionCommand::SendFrame(frame) => {
                                    if let Frame::DatagramClose { flow_id } = frame {
                                        flows.retain(|flow| flow.flow_id != flow_id);
                                        udp_path_write_frame(
                                            &mut send,
                                            &Frame::DatagramClose { flow_id },
                                            context.codec_limits,
                                        )
                                        .await?;
                                    } else {
                                        udp_path_write_frame(&mut send, &frame, context.codec_limits).await?;
                                    }
                                    Ok(false)
                                }
                                TcpPathSessionCommand::CloseStream(_) => {
                                    let _ = udp_path_finish_stream(&mut send);
                                    Ok(true)
                                }
                                TcpPathSessionCommand::OpenStream { .. } => {
                                    Err(RuntimeError::Protocol("server UDP carrier datagram stream received open command"))
                                }
                            }
                        }
                        .await;
                        commands_rx.release_pending_command_bytes(pending_bytes);
                        if result? {
                            return Ok(());
                        }
                    }
                    None => {}
                }
            }
        }
    }
}

async fn open_server_udp_datagram_flow(
    context: &ServerPathContext,
    commands_tx: &TcpPathSessionCommandSender,
    send: &mut UdpPathSendStream,
    flows: &mut Vec<ServerUdpDatagramFlow>,
    flow_id: DatagramFlowId,
    target: TargetAddr,
    _lane: FlowLane,
) -> Result<(), RuntimeError> {
    if flows.iter().any(|flow| flow.flow_id == flow_id) {
        return Err(RuntimeError::Protocol(
            "duplicate UDP carrier datagram flow",
        ));
    }
    if flows.len() >= context.max_udp_flows_per_session {
        udp_path_write_frame(
            send,
            &Frame::DatagramClose { flow_id },
            context.codec_limits,
        )
        .await?;
        return Ok(());
    }
    outbound::validate_target(&target)?;
    context.outbound.ensure_supports(TargetProtocol::Udp)?;
    let outbound_socket = match outbound::connect_udp(
        &context.outbound,
        &context.outbound_dns,
        &target,
        Duration::from_secs(10),
    )
    .await
    {
        Ok(socket) => socket,
        Err(err) => {
            udp_path_write_frame(
                send,
                &Frame::DatagramClose { flow_id },
                context.codec_limits,
            )
            .await?;
            return Err(RuntimeError::OutboundConnect(err));
        }
    };
    let requests = spawn_server_udp_datagram_flow_worker(
        flow_id,
        outbound_socket,
        commands_tx.clone(),
        context.mux_limits,
    );
    flows.push(ServerUdpDatagramFlow { flow_id, requests });
    Ok(())
}

fn udp_carrier_frame_finished(err: &udp_carrier::UdpCarrierFrameError) -> bool {
    matches!(err, udp_carrier::UdpCarrierFrameError::Closed)
}

fn udp_path_frame_finished(err: &RuntimeError) -> bool {
    match err {
        RuntimeError::UdpCarrierFrame(err) => udp_carrier_frame_finished(err),
        RuntimeError::QuicCarrier(quic_carrier::QuicCarrierError::ReadExact(_)) => true,
        RuntimeError::QuicCarrier(quic_carrier::QuicCarrierError::Connection(_)) => true,
        _ => false,
    }
}

fn udp_runtime_error_is_expected_shutdown(err: &RuntimeError) -> bool {
    match err {
        RuntimeError::UdpCarrierConnection(udp_carrier::UdpCarrierConnectionError::Closed) => true,
        RuntimeError::UdpCarrierFrame(err) => udp_carrier_frame_finished(err),
        RuntimeError::QuicCarrier(quic_carrier::QuicCarrierError::ReadExact(_)) => true,
        RuntimeError::QuicCarrier(quic_carrier::QuicCarrierError::Connection(_)) => true,
        RuntimeError::RemoteClosed(CloseReason::Normal) => true,
        _ => false,
    }
}

fn warn_unexpected_udp_runtime_error(message: &str, err: &RuntimeError) {
    if !udp_runtime_error_is_expected_shutdown(err) {
        eprintln!("warning: {message}: {err}");
    }
}

fn udp_carrier_open_error_is_path_retryable(err: &RuntimeError) -> bool {
    matches!(
        err,
        RuntimeError::Io(_)
            | RuntimeError::Udp(_)
            | RuntimeError::UdpCarrierTransport(_)
            | RuntimeError::UdpCarrierFrame(_)
            | RuntimeError::UdpCarrierConnection(_)
            | RuntimeError::QuicCarrier(_)
            | RuntimeError::RemoteClosed(_)
            | RuntimeError::Protocol(_)
            | RuntimeError::TcpPathSessionClosed
    )
}

fn udp_path_command_queue(mux_limits: MuxLimits, codec_limits: CodecLimits) -> usize {
    tcp_path_command_queue_for_payload(
        mux_limits,
        udp_carrier::max_stream_payload_bytes(codec_limits, mux_limits),
    )
}

async fn resolve_first_socket_addr(path: &PathSpec) -> Result<SocketAddr, RuntimeError> {
    let mut addrs = lookup_host((path.endpoint.host.as_str(), path.endpoint.port)).await?;
    addrs.next().ok_or(RuntimeError::Protocol(
        "UDP carrier endpoint resolved no socket addresses",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quic_stats_feed_sender_side_udp_path_metrics() {
        let mut tracker = UdpPathMetricTracker::default();
        let congestion = quic_carrier::CongestionMetrics {
            congestion_window: 4 * 1024 * 1024,
            pacing_rate_bps: Some(500_000_000),
        };
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(50);
        stats.path.cwnd = 4 * 1024 * 1024;
        stats.path.current_mtu = 1400;

        let startup = tracker.quic.observe(stats, congestion, 2);
        assert_eq!(startup.direction, 2);
        assert_eq!(startup.delivery_sample_count, 0);
        assert_eq!(startup.delivery_rate_bps.round() as u64, 500_000_000);
        assert_eq!(startup.inflight_hi, 4 * 1024 * 1024);

        tracker.quic.last_observed_at = Some(Instant::now() - Duration::from_millis(100));
        stats.udp_tx.bytes = 8 * 1024 * 1024;
        stats.frame_tx.stream = 128;
        stats.frame_rx.acks = 4;
        let measured = tracker.quic.observe(stats, congestion, 2);
        assert_eq!(measured.direction, 2);
        assert_eq!(measured.delivery_sample_count, 4);
        assert!(measured.delivery_rate_bps > 0.0);
        assert!(measured.last_delivery_sample_at.is_some());
        assert!(!measured.app_limited);
    }

    #[test]
    fn quic_ack_only_stats_do_not_create_delivery_rate_evidence() {
        let mut tracker = UdpPathMetricTracker::default();
        let congestion = quic_carrier::CongestionMetrics {
            congestion_window: 4 * 1024 * 1024,
            pacing_rate_bps: Some(500_000_000),
        };
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(1);
        stats.path.cwnd = 4 * 1024 * 1024;
        stats.path.current_mtu = 1400;
        let _ = tracker.quic.observe(stats, congestion, 1);

        stats.frame_rx.acks = 1;
        let ack_only = tracker.quic.observe(stats, congestion, 1);
        assert_eq!(ack_only.delivery_sample_count, 0);
        assert!(ack_only.last_delivery_sample_at.is_none());
        assert_eq!(ack_only.delivery_rate_bps.round() as u64, 500_000_000);
    }

    #[test]
    fn quic_app_limited_low_ack_sample_does_not_poison_delivery_rate() {
        let mut tracker = UdpPathMetricTracker::default();
        let congestion = quic_carrier::CongestionMetrics {
            congestion_window: 4 * 1024 * 1024,
            pacing_rate_bps: Some(500_000_000),
        };
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(50);
        stats.path.cwnd = 4 * 1024 * 1024;
        stats.path.current_mtu = 1400;
        let _ = tracker.quic.observe(stats, congestion, 2);

        tracker.quic.last_observed_at = Some(Instant::now() - Duration::from_millis(1000));
        stats.udp_tx.bytes = 32 * 1024;
        stats.frame_tx.stream = 1;
        stats.frame_rx.acks = 1;
        let app_limited = tracker.quic.observe(stats, congestion, 2);

        assert_eq!(app_limited.delivery_sample_count, 0);
        assert!(app_limited.last_delivery_sample_at.is_none());
        assert_eq!(app_limited.delivery_rate_bps.round() as u64, 500_000_000);
        assert!(app_limited.app_limited);
    }
}
