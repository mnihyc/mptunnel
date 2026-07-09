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
        options: UdpStreamOpenOptions,
    ) -> Result<ReliablePathStream, RuntimeError> {
        let connection = self.ensure_connection().await?;
        match open_client_udp_stream_on_connection(
            connection,
            stream_id,
            target.clone(),
            ingress,
            lane,
            options,
            self.runtime.clone(),
        )
        .await
        {
            Ok(stream) => Ok(stream),
            Err(err) if quic_path_open_error_is_retryable(&err) => {
                self.drop_connection().await;
                let connection = self.ensure_connection().await?;
                open_client_udp_stream_on_connection(
                    connection,
                    stream_id,
                    target,
                    ingress,
                    lane,
                    options,
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
            Err(err) if quic_path_open_error_is_retryable(&err) => {
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
pub(super) struct UdpPathEndpoint {
    endpoint: quic_carrier::Endpoint,
}

#[derive(Debug, Clone)]
pub(super) struct UdpPathConnection {
    connection: quic_carrier::Connection,
}

#[derive(Debug, Default)]
struct UdpPathMetricTracker {
    quic: QuicPathMetricTracker,
}

#[derive(Debug, Default)]
struct QuicPathMetricTracker {
    last_product_data_written_bytes: u64,
    product_data_pending_ack_bytes: u64,
    last_observed_at: Option<Instant>,
    delivery_rate_bps: Option<f64>,
    ack_derived_data_seen: bool,
    delivery_sample_count: u64,
    delivery_sample_bytes: u64,
    last_delivery_sample_at: Option<Instant>,
    min_rtt: Option<Duration>,
}

#[derive(Debug)]
pub(super) struct UdpPathSendStream {
    stream: quic_carrier::SendStream,
}

#[derive(Debug)]
pub(super) struct UdpPathRecvStream {
    stream: quic_carrier::RecvStream,
}

impl UdpPathEndpoint {
    async fn bind_server(
        path: &PathSpec,
        context: &ServerPathContext,
    ) -> Result<Self, RuntimeError> {
        let addr = resolve_first_socket_addr(path).await?;
        Ok(Self {
            endpoint: quic_carrier::Endpoint::bind_server(
                addr,
                context.security.secret.as_bytes(),
                context.mux_limits,
            )
            .await?,
        })
    }

    async fn bind_client(
        _path: &PathSpec,
        local_addr: SocketAddr,
        runtime: &ClientUdpPathSessionRuntime,
    ) -> Result<Self, RuntimeError> {
        Ok(Self {
            endpoint: quic_carrier::Endpoint::bind_client(
                local_addr,
                runtime.security.secret.as_bytes(),
                runtime.mux_limits,
            )
            .await?,
        })
    }

    async fn connect(&self, remote_addr: SocketAddr) -> Result<UdpPathConnection, RuntimeError> {
        Ok(UdpPathConnection {
            connection: self.endpoint.connect(remote_addr).await?,
        })
    }

    async fn accept(&self) -> Option<UdpPathConnection> {
        self.endpoint
            .accept()
            .await
            .map(|connection| UdpPathConnection { connection })
    }

    #[cfg(test)]
    pub(super) fn local_addr(&self) -> Result<SocketAddr, std::io::Error> {
        self.endpoint.local_addr()
    }
}

impl UdpPathConnection {
    async fn open_bi(&self) -> Result<(UdpPathSendStream, UdpPathRecvStream), RuntimeError> {
        let (send, recv) = self.connection.open_bi().await?;
        Ok((
            UdpPathSendStream { stream: send },
            UdpPathRecvStream { stream: recv },
        ))
    }

    async fn accept_bi(&self) -> Result<(UdpPathSendStream, UdpPathRecvStream), RuntimeError> {
        let (send, recv) = self.connection.accept_bi().await?;
        Ok((
            UdpPathSendStream { stream: send },
            UdpPathRecvStream { stream: recv },
        ))
    }

    fn close(&self) {
        self.connection.close();
    }

    fn is_closed(&self) -> bool {
        self.connection.is_closed()
    }

    async fn tx_metrics(
        &self,
        tracker: &mut UdpPathMetricTracker,
        direction: u8,
    ) -> Option<UdpPathMetrics> {
        let stats = self.connection.stats();
        let congestion = self.connection.congestion_metrics();
        Some(tracker.quic.observe(stats, congestion, direction))
    }
}

impl QuicPathMetricTracker {
    fn observe(
        &mut self,
        stats: quinn::ConnectionStats,
        congestion: quic_carrier::CongestionMetrics,
        direction: u8,
    ) -> UdpPathMetrics {
        let now = Instant::now();
        let elapsed = self
            .last_observed_at
            .map(|seen| now.saturating_duration_since(seen))
            .unwrap_or_default();
        self.last_observed_at = Some(now);
        let product_data_delta = congestion
            .product_data_written_bytes
            .saturating_sub(self.last_product_data_written_bytes);
        self.last_product_data_written_bytes = congestion.product_data_written_bytes;
        self.product_data_pending_ack_bytes = self
            .product_data_pending_ack_bytes
            .saturating_add(product_data_delta);

        if stats.path.rtt > Duration::ZERO {
            self.min_rtt = Some(
                self.min_rtt
                    .map_or(stats.path.rtt, |previous| previous.min(stats.path.rtt)),
            );
        }
        let rtt = stats.path.rtt.max(QUIC_TIMER_GRANULARITY);
        let min_rtt = self.min_rtt.unwrap_or(rtt);
        let congestion_window = congestion.congestion_window.max(stats.path.cwnd);
        let carrier_capacity_known = congestion.pacing_rate_bps.is_some() || congestion_window > 0;
        let bytes_in_flight = congestion.bytes_in_flight.unwrap_or(0);
        let inflight_hi = if carrier_capacity_known {
            congestion_window.max(stats.path.current_mtu as u64) as usize
        } else {
            0
        };
        let startup_rate = default_path_rate_bps(UnderlayProtocol::Udp);
        let raw_pacing_rate = congestion.pacing_rate_bps.map(|rate| rate.max(1) as f64);
        let usable_pacing_rate = raw_pacing_rate.map(|rate| {
            if self.delivery_sample_count == 0 {
                rate.max(startup_rate)
            } else {
                rate
            }
        });
        let fallback_rate = usable_pacing_rate.unwrap_or_else(|| {
            if carrier_capacity_known {
                let cwnd_rate = inflight_hi as f64 * 8.0
                    / rtt.as_secs_f64().max(QUIC_TIMER_GRANULARITY.as_secs_f64());
                if self.delivery_sample_count == 0 {
                    cwnd_rate.max(startup_rate)
                } else {
                    cwnd_rate
                }
            } else {
                startup_rate
            }
        });
        let evidence_inflight_hi = if inflight_hi > 0 {
            inflight_hi as u64
        } else {
            (fallback_rate / 8.0 * rtt.as_secs_f64().max(QUIC_TIMER_GRANULARITY.as_secs_f64()))
                .ceil()
                .max(1.0) as u64
        };

        let newly_acked_bytes = congestion.newly_acked_bytes.unwrap_or(0);
        let product_data_pending_before_ack = self.product_data_pending_ack_bytes;
        let product_newly_acked_bytes = newly_acked_bytes.min(product_data_pending_before_ack);
        let product_data_ack_context = product_newly_acked_bytes > 0;
        let mut app_limited = congestion.app_limited || !product_data_ack_context;
        if product_newly_acked_bytes > 0 {
            self.ack_derived_data_seen = true;
            self.product_data_pending_ack_bytes = self
                .product_data_pending_ack_bytes
                .saturating_sub(product_newly_acked_bytes);
        }
        // `product_data_pending_ack_bytes` is the part of the product stream that
        // mptunnel has handed to Quinn but has not yet observed as carrier-ACKed.
        // Quinn's write future completes when bytes are accepted into QUIC's
        // stream/send buffers, not when they leave the connection. Treat that
        // backlog as carrier queue debt, otherwise the product scheduler can
        // overfill QUIC by tens of MiB while believing the path is empty.
        let carrier_committed_bytes = self
            .product_data_pending_ack_bytes
            .saturating_add(congestion.pending_bytes)
            .max(bytes_in_flight);

        if product_newly_acked_bytes > 0 && !congestion.app_limited {
            let sample_elapsed = elapsed.max(QUIC_TIMER_GRANULARITY);
            let sample_rate =
                (product_newly_acked_bytes as f64 * 8.0 / sample_elapsed.as_secs_f64()).max(1.0);
            let delivery_evidence_floor = if self.delivery_sample_count == 0 {
                evidence_inflight_hi.max(PATH_OPEN_SCORE_BYTES as u64)
            } else {
                evidence_inflight_hi
            };
            let sample_is_app_limited = product_newly_acked_bytes < delivery_evidence_floor
                && self.delivery_sample_count == 0;
            app_limited = sample_is_app_limited;
            if !sample_is_app_limited {
                let current_rate = self.delivery_rate_bps.unwrap_or(fallback_rate).max(1.0);
                let bounded_sample = sample_rate.min(current_rate * BBR_DEFAULT_CWND_GAIN);
                self.delivery_sample_count = self
                    .delivery_sample_count
                    .saturating_add(congestion.delivery_sample_count.max(1));
                self.delivery_sample_bytes = self
                    .delivery_sample_bytes
                    .saturating_add(product_newly_acked_bytes);
                self.last_delivery_sample_at = Some(now);
                self.delivery_rate_bps = Some(match self.delivery_rate_bps {
                    Some(previous) if bounded_sample > previous => {
                        previous.mul_add(0.25, bounded_sample * 0.75)
                    }
                    Some(previous) => previous,
                    None => bounded_sample.max(fallback_rate),
                });
            } else if self.delivery_rate_bps.is_none() {
                self.delivery_rate_bps = Some(fallback_rate);
            }
        }

        let delivery_rate_bps = self.delivery_rate_bps.unwrap_or(fallback_rate).max(1.0);
        let pacing_rate_bps = usable_pacing_rate
            .unwrap_or(delivery_rate_bps)
            .max(delivery_rate_bps);
        UdpPathMetrics {
            direction,
            srtt: rtt,
            rttvar: rtt / 4,
            min_rtt,
            min_rtt_observed: stats.path.rtt > Duration::ZERO,
            delivery_rate_bps,
            pacing_rate_bps,
            inflight_hi,
            bytes_in_flight: usize::try_from(bytes_in_flight).unwrap_or(usize::MAX),
            pending_bytes: usize::try_from(carrier_committed_bytes).unwrap_or(usize::MAX),
            loss_ppm: congestion.loss_ppm,
            ecn_ppm: congestion.ecn_ppm,
            app_limited,
            ack_derived_data_seen: self.ack_derived_data_seen,
            delivery_sample_count: self.delivery_sample_count,
            delivery_sample_bytes: self.delivery_sample_bytes,
            last_delivery_sample_at: self.last_delivery_sample_at,
        }
    }
}

pub(super) async fn udp_path_read_frame(
    recv: &mut UdpPathRecvStream,
    codec_limits: CodecLimits,
) -> Result<Frame, RuntimeError> {
    Ok(quic_carrier::read_frame(&mut recv.stream, codec_limits).await?)
}

pub(super) async fn udp_path_write_frame(
    send: &mut UdpPathSendStream,
    frame: &Frame,
    codec_limits: CodecLimits,
) -> Result<(), RuntimeError> {
    quic_carrier::write_frame(&mut send.stream, frame, codec_limits).await?;
    Ok(())
}

async fn udp_path_write_frames(
    send: &mut UdpPathSendStream,
    frames: &[Frame],
    codec_limits: CodecLimits,
) -> Result<(), RuntimeError> {
    quic_carrier::write_frames(&mut send.stream, frames, codec_limits).await?;
    Ok(())
}

async fn flush_udp_frame_batch(
    send: &mut UdpPathSendStream,
    frames: &mut Vec<Frame>,
    codec_limits: CodecLimits,
) -> Result<(), RuntimeError> {
    if frames.is_empty() {
        return Ok(());
    }
    udp_path_write_frames(send, frames, codec_limits).await?;
    frames.clear();
    Ok(())
}

async fn flush_udp_frame_batch_with_path_proofs(
    send: &mut UdpPathSendStream,
    frames: &mut Vec<Frame>,
    codec_limits: CodecLimits,
    path_proofs: &mut PathProofTracker,
) -> Result<(), RuntimeError> {
    if frames.is_empty() {
        return Ok(());
    }
    udp_path_write_frames(send, frames, codec_limits).await?;
    for frame in frames.iter() {
        path_proofs.record_sent_frame(frame);
    }
    frames.clear();
    Ok(())
}

pub(super) fn udp_path_finish_stream(send: &mut UdpPathSendStream) -> Result<(), RuntimeError> {
    Ok(quic_carrier::finish_stream(&mut send.stream)?)
}

// Product-level UDP reliable frame size. This is intentionally the same kind of
// BDP/service quantum used by TCP. Do not cap this to a QUIC packet train: doing
// so turns the carrier record size into the application pacing unit and
// underfeeds QUIC. QUIC-specific recordization is performed inside
// transport::quic_carrier while preserving this product quantum.
fn udp_path_max_stream_payload_bytes(codec_limits: CodecLimits, mux_limits: MuxLimits) -> usize {
    quic_carrier::max_stream_payload_bytes(codec_limits)
        .min(mux_limits.max_reliable_relay_chunk_bytes)
        .max(1)
}

fn udp_reliable_stream_frame_queue(codec_limits: CodecLimits, mux_limits: MuxLimits) -> usize {
    reliable_stream_frame_queue_for_payload(
        mux_limits,
        udp_path_max_stream_payload_bytes(codec_limits, mux_limits),
    )
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
                tokio::time::sleep(default_transport_pto()).await;
                continue;
            };
            if let Some(record) = runtime
                .health
                .lock()
                .expect("client QUIC UDP path health lock")
                .udp
                .get_mut(runtime.path_index)
            {
                record.mark_quic_path_metrics(metrics);
            }
            tokio::time::sleep(quic_path_metrics_poll_interval(metrics)).await;
        }
    });
}

fn quic_path_metrics_poll_interval(metrics: UdpPathMetrics) -> Duration {
    if metrics.app_limited {
        transport_pto_from_ms(
            metrics.srtt.as_secs_f64() * 1000.0,
            metrics.rttvar.as_secs_f64() * 1000.0,
        )
    } else {
        (metrics.srtt / 2).max(QUIC_TIMER_GRANULARITY)
    }
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
            return Err(RuntimeError::Protocol("QUIC UDP path endpoint closed"));
        };
        let context = context.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_server_udp_connection(connection, context).await {
                warn_unexpected_udp_runtime_error("server QUIC UDP path connection failed", &err);
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
    options: UdpStreamOpenOptions,
    runtime: ClientUdpPathSessionRuntime,
) -> Result<ReliablePathStream, RuntimeError> {
    let UdpStreamOpenOptions {
        wait_for_accept,
        role,
    } = options;
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
    let accepted_max_offset = if wait_for_accept {
        Some(read_client_udp_stream_open_accept(&mut recv, stream_id, runtime.codec_limits).await?)
    } else {
        None
    };
    let max_offset = udp_stream_open_initial_max_offset(options, accepted_max_offset);
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
        runtime.codec_limits,
        runtime.mux_limits,
        stream_frame_queue,
        runtime.health.clone(),
        receivers,
        frames_tx,
    ));
    Ok(ReliablePathStream {
        stream_id,
        max_offset,
        lane,
        underlay: UnderlayProtocol::Udp,
        max_frame_payload_bytes: udp_path_max_stream_payload_bytes(
            runtime.codec_limits,
            runtime.mux_limits,
        ),
        output: ReliablePathStreamOutput::fixed_with_snapshot(
            path_startup_snapshot(&runtime.path, runtime.path_index),
            commands,
            runtime.mux_limits,
        ),
        frames: frames_rx,
    })
}

async fn read_client_udp_stream_open_accept(
    recv: &mut UdpPathRecvStream,
    stream_id: StreamId,
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
            Frame::PathStatus { .. } | Frame::SessionReady => {}
            Frame::SessionClose { reason } => return Err(RuntimeError::RemoteClosed(reason)),
            _ => {
                return Err(RuntimeError::Protocol(
                    "unexpected QUIC UDP path stream open frame",
                ));
            }
        }
    }
}

fn udp_stream_open_initial_max_offset(
    options: UdpStreamOpenOptions,
    accepted_max_offset: Option<u64>,
) -> u64 {
    if options.wait_for_accept {
        accepted_max_offset.unwrap_or(0)
    } else {
        0
    }
}

async fn open_client_udp_datagram_stream(
    connection: UdpPathConnection,
    runtime: ClientUdpPathSessionRuntime,
) -> Result<ClientUdpDatagramStream, RuntimeError> {
    let (send, recv) = connection.open_bi().await?;
    let frames = spawn_quic_path_reader(recv, runtime.codec_limits, runtime.stream_frame_queue);
    Ok(ClientUdpDatagramStream {
        send,
        frames,
        path_id: PathId(runtime.path_index as u16),
        runtime,
    })
}

fn spawn_quic_path_reader(
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
                    Err(RuntimeError::ReliablePathSessionClosed)
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
    path_index: usize,
    codec_limits: CodecLimits,
    mux_limits: MuxLimits,
    reader_queue_size: usize,
    health: Arc<Mutex<ClientPathHealth>>,
    mut commands: ReliablePathCommandReceivers,
    frames: mpsc::Sender<Result<Frame, RuntimeError>>,
) {
    let mut carrier_frames = spawn_quic_path_reader(recv, codec_limits, reader_queue_size);
    let mut pending_frames = Vec::<Frame>::new();
    let mut path_proofs = PathProofTracker::default();
    let path_id = PathId(path_index as u16);
    loop {
        let command_may_recv = !reliable_path_receivers_closed(&commands);
        if !command_may_recv {
            let _ = udp_path_finish_stream(&mut send);
            return;
        }
        if let Some(command) = try_recv_reliable_path_priority_command(&mut commands) {
            let result = drain_client_udp_stream_commands(
                command,
                &mut commands,
                &mut send,
                stream_id,
                codec_limits,
                mux_limits,
                &mut pending_frames,
                &mut path_proofs,
            )
            .await;
            match result {
                Ok(false) => {}
                Ok(true) => return,
                Err(err) => {
                    let _ = frames.send(Err(err)).await;
                    return;
                }
            }
            continue;
        }
        tokio::select! {
            biased;
            frame = carrier_frames.recv() => {
                match frame {
                    Some(Ok(Frame::Ping { nonce })) => {
                        if let Err(err) = udp_path_write_frame(&mut send, &Frame::Pong { nonce }, codec_limits).await {
                            let _ = frames.send(Err(err)).await;
                            return;
                        }
                    }
                    Some(Ok(Frame::PathProofData {
                        path_id: proof_path_id,
                        proof_id,
                        payload,
                    })) if proof_path_id == path_id => {
                        if let Err(err) = udp_path_write_frame(
                            &mut send,
                            &path_proof_ack_frame(path_id, proof_id, payload.len()),
                            codec_limits,
                        ).await {
                            let _ = frames.send(Err(err)).await;
                            return;
                        }
                    }
                    Some(Ok(Frame::PathProofAck {
                        path_id: proof_path_id,
                        proof_id,
                        payload_bytes,
                    })) if proof_path_id == path_id => {
                        if let Some(observation) =
                            path_proofs.acknowledge(path_id, proof_id, payload_bytes)
                            && let Some(record) = health
                                .lock()
                                .expect("client path health lock")
                                .udp
                                .get_mut(path_index)
                        {
                            record.mark_path_proof_success(observation.elapsed);
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
                            .send(Err(RuntimeError::Protocol("unexpected QUIC UDP path reliable stream frame")))
                            .await;
                        return;
                    }
                    Some(Err(err)) => {
                        let _ = frames.send(Err(err)).await;
                        return;
                    }
                    None => {
                        let _ = frames.send(Err(RuntimeError::ReliablePathSessionClosed)).await;
                        return;
                    }
                }
                if let Some(command) = try_recv_reliable_path_command(&mut commands) {
                    let result = drain_client_udp_stream_commands(
                        command,
                        &mut commands,
                        &mut send,
                        stream_id,
                            codec_limits,
                            mux_limits,
                            &mut pending_frames,
                            &mut path_proofs,
                        )
                    .await;
                    match result {
                        Ok(false) => {}
                        Ok(true) => return,
                        Err(err) => {
                            let _ = frames.send(Err(err)).await;
                            return;
                        }
                    }
                }
            }
            command = recv_reliable_path_command(&mut commands), if command_may_recv => {
                match command {
                    Some(command) => {
                        let result = drain_client_udp_stream_commands(
                            command,
                            &mut commands,
                            &mut send,
                            stream_id,
                            codec_limits,
                            mux_limits,
                            &mut pending_frames,
                            &mut path_proofs,
                        ).await;
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

async fn drain_client_udp_stream_commands(
    first_command: ReliablePathCommand,
    commands: &mut ReliablePathCommandReceivers,
    send: &mut UdpPathSendStream,
    stream_id: StreamId,
    codec_limits: CodecLimits,
    mux_limits: MuxLimits,
    pending_frames: &mut Vec<Frame>,
    path_proofs: &mut PathProofTracker,
) -> Result<bool, RuntimeError> {
    #[cfg(feature = "lab-diagnostics")]
    let drain_started = Instant::now();
    let byte_budget = reliable_path_command_writer_run_budget_bytes(mux_limits);
    let item_budget = reliable_path_command_writer_run_budget_items(mux_limits);
    let mut next_command = Some(first_command);
    pending_frames.clear();
    let mut sent_bytes = 0usize;
    let mut sent_items = 0usize;

    loop {
        let Some(command) = next_command
            .take()
            .or_else(|| try_recv_reliable_path_command(commands))
        else {
            if try_coalesce_reliable_path_writer_run(
                commands,
                &mut next_command,
                sent_items,
                sent_bytes,
                byte_budget,
                item_budget,
            )
            .await
            {
                continue;
            }
            flush_udp_frame_batch_with_path_proofs(send, pending_frames, codec_limits, path_proofs)
                .await?;
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "path_writer_drain",
                format_args!(
                    "role=client underlay=Udp stream_id={} sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={}",
                    stream_id.0,
                    sent_items,
                    sent_bytes,
                    byte_budget,
                    item_budget,
                    commands.pending_bytes(),
                    drain_started.elapsed().as_micros(),
                    false,
                    false,
                ),
            );
            return Ok(false);
        };
        let pending_bytes = reliable_path_command_pending_bytes(&command);
        let should_close = match command {
            ReliablePathCommand::SendFrame(frame) => {
                pending_frames.push(frame);
                commands.release_pending_command_bytes(pending_bytes);
                sent_bytes = sent_bytes.saturating_add(pending_bytes.max(1));
                sent_items = sent_items.saturating_add(1);
                if sent_bytes >= byte_budget || sent_items >= item_budget {
                    flush_udp_frame_batch_with_path_proofs(
                        send,
                        pending_frames,
                        codec_limits,
                        path_proofs,
                    )
                    .await?;
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "path_writer_drain",
                        format_args!(
                            "role=client underlay=Udp stream_id={} sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={}",
                            stream_id.0,
                            sent_items,
                            sent_bytes,
                            byte_budget,
                            item_budget,
                            commands.pending_bytes(),
                            drain_started.elapsed().as_micros(),
                            true,
                            sent_items >= item_budget,
                        ),
                    );
                    return Ok(false);
                }
                continue;
            }
            ReliablePathCommand::CloseStream(close_stream_id) => {
                flush_udp_frame_batch_with_path_proofs(
                    send,
                    pending_frames,
                    codec_limits,
                    path_proofs,
                )
                .await?;
                if close_stream_id == stream_id {
                    let _ = udp_path_finish_stream(send);
                    true
                } else {
                    false
                }
            }
            ReliablePathCommand::OpenStream { .. } => {
                return Err(RuntimeError::Protocol(
                    "client QUIC UDP path stream received open command",
                ));
            }
        };
        commands.release_pending_command_bytes(pending_bytes);
        if should_close {
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "path_writer_drain",
                format_args!(
                    "role=client underlay=Udp stream_id={} sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={}",
                    stream_id.0,
                    sent_items,
                    sent_bytes,
                    byte_budget,
                    item_budget,
                    commands.pending_bytes(),
                    drain_started.elapsed().as_micros(),
                    false,
                    false,
                ),
            );
            return Ok(true);
        }
        sent_items = sent_items.saturating_add(1);
        if sent_bytes >= byte_budget || sent_items >= item_budget {
            flush_udp_frame_batch_with_path_proofs(send, pending_frames, codec_limits, path_proofs)
                .await?;
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "path_writer_drain",
                format_args!(
                    "role=client underlay=Udp stream_id={} sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={}",
                    stream_id.0,
                    sent_items,
                    sent_bytes,
                    byte_budget,
                    item_budget,
                    commands.pending_bytes(),
                    drain_started.elapsed().as_micros(),
                    true,
                    sent_items >= item_budget,
                ),
            );
            return Ok(false);
        }
    }
}

async fn handle_server_udp_connection(
    connection: UdpPathConnection,
    context: ServerPathContext,
) -> Result<(), RuntimeError> {
    let (session_id, path_id, capabilities) =
        accept_server_udp_path_handshake(&connection, &context).await?;
    spawn_server_quic_path_metrics(context.clone(), session_id, path_id, connection.clone());
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
                warn_unexpected_udp_runtime_error("server QUIC UDP path stream failed", &err);
            }
        });
    }
}

fn spawn_server_quic_path_metrics(
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
                tokio::time::sleep(default_transport_pto()).await;
                continue;
            };
            if quic_path_metrics_should_publish_local_sender(metrics) {
                context.reliable_streams.record_local_path_metrics(
                    session_id,
                    UnderlayProtocol::Udp,
                    path_id,
                    path_metrics_from_quic_path(path_id, metrics),
                );
            }
            tokio::time::sleep(quic_path_metrics_poll_interval(metrics)).await;
        }
    });
}

fn quic_path_metrics_should_publish_local_sender(metrics: UdpPathMetrics) -> bool {
    metrics.delivery_sample_count > 0 || metrics.ack_derived_data_seen
}

fn path_metrics_from_quic_path(path_id: PathId, metrics: UdpPathMetrics) -> PathMetrics {
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
        loss_ppm: metrics.loss_ppm.unwrap_or(0),
        ecn_ppm: metrics.ecn_ppm.unwrap_or(0),
        loss_observed: metrics.loss_ppm.is_some(),
        ecn_observed: metrics.ecn_ppm.is_some(),
        bytes_in_flight: metrics.bytes_in_flight as u64,
        queue_bytes: metrics
            .pending_bytes
            .saturating_sub(metrics.bytes_in_flight) as u64,
        inflight_limit_bytes: metrics.inflight_hi as u64,
        inflight_hi_bytes: metrics.inflight_hi as u64,
        confidence_ppm: ratio_to_ppm(
            (metrics.delivery_sample_count as f64 / QUIC_INITIAL_WINDOW_PACKETS as f64)
                .clamp(0.0, 1.0),
        ),
        app_limited: metrics.app_limited,
        has_ack_derived_data_sample: metrics.ack_derived_data_seen,
        data_sample_count: u32::try_from(metrics.delivery_sample_count).unwrap_or(u32::MAX),
        data_sample_bytes: metrics.delivery_sample_bytes,
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
        _ => {
            return Err(RuntimeError::Protocol(
                "expected QUIC UDP path SESSION_HELLO",
            ));
        }
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
        _ => return Err(RuntimeError::Protocol("invalid QUIC UDP path SESSION_AUTH")),
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
        _ => return Err(RuntimeError::Protocol("invalid QUIC UDP path PATH_JOIN")),
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
            "unexpected first QUIC UDP path stream frame",
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
    let (commands_tx, commands_rx) = reliable_path_command_channels(udp_path_command_queue(
        context.mux_limits,
        context.codec_limits,
    ));
    match context.reliable_streams.open_or_attach(
        ServerReliableStreamOpenRequest {
            session_id,
            stream_id,
            target: &target,
            lane,
            attachment: ServerReliablePathAttachment {
                path_id,
                underlay: UnderlayProtocol::Udp,
                commands: commands_tx.clone(),
                max_frame_payload_bytes: udp_path_max_stream_payload_bytes(
                    context.codec_limits,
                    context.mux_limits,
                ),
                role,
                initial_metrics: context.local_path_startup_metrics(UnderlayProtocol::Udp, path_id),
            },
        },
        context.mux_limits,
        context.max_reliable_streams,
    )? {
        ServerReliableStreamOpen::New(stream) => {
            let stream_context = context.clone();
            let target = target.clone();
            tokio::spawn(async move {
                if let Err(err) =
                    run_server_tcp_stream(stream_context, session_id, stream, target).await
                {
                    eprintln!("warning: server reliable stream failed: {err}");
                }
            });
        }
        ServerReliableStreamOpen::Existing => {
            context
                .reliable_streams
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
                    max_offset: reliable_stream_initial_advertised_window_bytes(
                        UnderlayProtocol::Udp,
                        lane,
                        context.mux_limits,
                    ),
                },
                context.codec_limits,
            )
            .await?;
        }
        ServerReliableStreamOpen::DuplicateLiveIgnored => {
            let _ = udp_path_finish_stream(&mut send);
            return Ok(());
        }
        ServerReliableStreamOpen::Rejected => {
            udp_path_write_frame(
                &mut send,
                &Frame::StreamReset {
                    stream_id,
                    reason: ResetReason::Refused,
                },
                context.codec_limits,
            )
            .await?;
            return Ok(());
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
    commands_tx: ReliablePathCommandSender,
    commands_rx: ReliablePathCommandReceivers,
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
        lane,
        role: _role,
        commands_tx,
        mut commands_rx,
    } = stream_context;
    let mut carrier_frames = spawn_quic_path_reader(
        recv,
        context.codec_limits,
        udp_reliable_stream_frame_queue(context.codec_limits, context.mux_limits),
    );
    let mut pending_frames = Vec::<Frame>::new();
    let mut path_proofs = PathProofTracker::default();

    loop {
        let command_may_recv = !reliable_path_receivers_closed(&commands_rx);
        if let Some(command) = try_recv_reliable_path_priority_command(&mut commands_rx) {
            let result = drain_server_udp_reliable_commands(
                command,
                &mut commands_rx,
                &mut send,
                &context,
                session_id,
                stream_id,
                path_id,
                &commands_tx,
                &mut pending_frames,
                &mut path_proofs,
            )
            .await;
            if result? {
                return Ok(());
            }
            continue;
        }
        tokio::select! {
            biased;
            frame = carrier_frames.recv() => {
                match frame {
                    Some(Ok(frame @ (Frame::StreamData { stream_id: received_stream_id, .. }
                        | Frame::StreamAck { stream_id: received_stream_id, .. }
                        | Frame::StreamMaxData { stream_id: received_stream_id, .. }
                        | Frame::StreamFin { stream_id: received_stream_id, .. }
                        | Frame::StreamReset { stream_id: received_stream_id, .. })))
                        if received_stream_id == stream_id =>
                    {
                        context.reliable_streams.route_frame(session_id, stream_id, frame).await?;
                    }
                    Some(Ok(Frame::StreamDetach { stream_id: detach_stream_id }))
                        if detach_stream_id == stream_id =>
                    {
                        context.reliable_streams.detach_path(
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
                        context.reliable_streams.record_path_metrics(
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
                        match context.reliable_streams.open_or_attach(
                            ServerReliableStreamOpenRequest {
                                session_id,
                                stream_id,
                                target: &target,
                                lane: updated_lane,
                                attachment: ServerReliablePathAttachment {
                                    path_id,
                                    underlay: UnderlayProtocol::Udp,
                                    commands: commands_tx.clone(),
                                    max_frame_payload_bytes: udp_path_max_stream_payload_bytes(
                                        context.codec_limits,
                                        context.mux_limits,
                                    ),
                                    role: open_role,
                                    initial_metrics: context
                                        .local_path_startup_metrics(UnderlayProtocol::Udp, path_id),
                                },
                            },
                            context.mux_limits,
                            context.max_reliable_streams,
                        )? {
                            ServerReliableStreamOpen::Existing => {
                                context
                                    .reliable_streams
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
                                        max_offset: reliable_stream_initial_advertised_window_bytes(
                                            UnderlayProtocol::Udp,
                                            lane,
                                            context.mux_limits,
                                        ),
                                    },
                                    context.codec_limits,
                                )
                                .await?;
                            }
                            ServerReliableStreamOpen::New(_) => {
                                return Err(RuntimeError::Protocol(
                                    "QUIC UDP path reannouncement opened duplicate stream",
                                ));
                            }
                            ServerReliableStreamOpen::DuplicateLiveIgnored => {
                                let _ = udp_path_finish_stream(&mut send);
                                return Ok(());
                            }
                            ServerReliableStreamOpen::Rejected => {
                                udp_path_write_frame(
                                    &mut send,
                                    &Frame::StreamReset {
                                        stream_id,
                                        reason: ResetReason::Refused,
                                    },
                                    context.codec_limits,
                                )
                                .await?;
                            }
                        }
                        continue;
                    }
                    Some(Ok(Frame::Ping { nonce })) => {
                        udp_path_write_frame(&mut send, &Frame::Pong { nonce }, context.codec_limits).await?;
                    }
                    Some(Ok(Frame::PathProofData {
                        path_id: proof_path_id,
                        proof_id,
                        payload,
                    })) if proof_path_id == path_id => {
                        udp_path_write_frame(
                            &mut send,
                            &path_proof_ack_frame(path_id, proof_id, payload.len()),
                            context.codec_limits,
                        )
                        .await?;
                    }
                    Some(Ok(Frame::PathProofAck {
                        path_id: proof_path_id,
                        proof_id,
                        payload_bytes,
                    })) if proof_path_id == path_id => {
                        if let Some(observation) =
                            path_proofs.acknowledge(path_id, proof_id, payload_bytes)
                            && let Some(metrics) = path_proof_metrics(
                                path_id,
                                UnderlayProtocol::Udp,
                                PathMetricDirection::ServerToClient,
                                observation,
                            )
                        {
                            context.reliable_streams.record_local_path_metrics(
                                session_id,
                                UnderlayProtocol::Udp,
                                path_id,
                                metrics,
                            );
                        }
                    }
                    Some(Ok(Frame::SessionClose { reason })) => return Err(RuntimeError::RemoteClosed(reason)),
                    Some(Ok(frame)) => {
                        log_unexpected_stream_relay_frame(
                            "server QUIC UDP path reliable",
                            stream_id,
                            &frame,
                        );
                        return Err(RuntimeError::Protocol("unexpected server QUIC UDP path reliable stream frame"));
                    }
                    Some(Err(RuntimeError::ReliablePathSessionClosed)) | None => {
                        context.reliable_streams.detach_path(
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
                if let Some(command) = try_recv_reliable_path_command(&mut commands_rx) {
                    let result = drain_server_udp_reliable_commands(
                        command,
                        &mut commands_rx,
                        &mut send,
                        &context,
                        session_id,
                        stream_id,
                            path_id,
                            &commands_tx,
                            &mut pending_frames,
                            &mut path_proofs,
                        )
                    .await?;
                    if result {
                        return Ok(());
                    }
                }
            }
            command = recv_reliable_path_command(&mut commands_rx), if command_may_recv => {
                match command {
                    Some(command) => {
                        let result = drain_server_udp_reliable_commands(
                            command,
                            &mut commands_rx,
                            &mut send,
                            &context,
                            session_id,
                            stream_id,
                            path_id,
                            &commands_tx,
                            &mut pending_frames,
                            &mut path_proofs,
                        ).await;
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

async fn drain_server_udp_reliable_commands(
    first_command: ReliablePathCommand,
    commands: &mut ReliablePathCommandReceivers,
    send: &mut UdpPathSendStream,
    context: &ServerPathContext,
    session_id: SessionId,
    stream_id: StreamId,
    path_id: PathId,
    commands_tx: &ReliablePathCommandSender,
    pending_frames: &mut Vec<Frame>,
    path_proofs: &mut PathProofTracker,
) -> Result<bool, RuntimeError> {
    #[cfg(feature = "lab-diagnostics")]
    let drain_started = Instant::now();
    let byte_budget = reliable_path_command_writer_run_budget_bytes(context.mux_limits);
    let item_budget = reliable_path_command_writer_run_budget_items(context.mux_limits);
    let mut next_command = Some(first_command);
    pending_frames.clear();
    let mut sent_bytes = 0usize;
    let mut sent_items = 0usize;

    loop {
        let Some(command) = next_command
            .take()
            .or_else(|| try_recv_reliable_path_command(commands))
        else {
            if try_coalesce_reliable_path_writer_run(
                commands,
                &mut next_command,
                sent_items,
                sent_bytes,
                byte_budget,
                item_budget,
            )
            .await
            {
                continue;
            }
            flush_udp_frame_batch_with_path_proofs(
                send,
                pending_frames,
                context.codec_limits,
                path_proofs,
            )
            .await?;
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "path_writer_drain",
                format_args!(
                    "role=server underlay=Udp path_id={} stream_id={} sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={}",
                    path_id.0,
                    stream_id.0,
                    sent_items,
                    sent_bytes,
                    byte_budget,
                    item_budget,
                    commands.pending_bytes(),
                    drain_started.elapsed().as_micros(),
                    false,
                    false,
                ),
            );
            return Ok(false);
        };
        let pending_bytes = reliable_path_command_pending_bytes(&command);
        let should_close = match command {
            ReliablePathCommand::SendFrame(frame) => {
                pending_frames.push(frame);
                commands.release_pending_command_bytes(pending_bytes);
                sent_bytes = sent_bytes.saturating_add(pending_bytes.max(1));
                sent_items = sent_items.saturating_add(1);
                if sent_bytes >= byte_budget || sent_items >= item_budget {
                    flush_udp_frame_batch_with_path_proofs(
                        send,
                        pending_frames,
                        context.codec_limits,
                        path_proofs,
                    )
                    .await?;
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "path_writer_drain",
                        format_args!(
                            "role=server underlay=Udp path_id={} stream_id={} sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={}",
                            path_id.0,
                            stream_id.0,
                            sent_items,
                            sent_bytes,
                            byte_budget,
                            item_budget,
                            commands.pending_bytes(),
                            drain_started.elapsed().as_micros(),
                            true,
                            sent_items >= item_budget,
                        ),
                    );
                    return Ok(false);
                }
                continue;
            }
            ReliablePathCommand::CloseStream(close_stream_id) => {
                flush_udp_frame_batch_with_path_proofs(
                    send,
                    pending_frames,
                    context.codec_limits,
                    path_proofs,
                )
                .await?;
                if close_stream_id == stream_id {
                    context.reliable_streams.detach_path(
                        session_id,
                        stream_id,
                        UnderlayProtocol::Udp,
                        path_id,
                        commands_tx,
                    );
                    let _ = udp_path_finish_stream(send);
                    true
                } else {
                    false
                }
            }
            ReliablePathCommand::OpenStream { .. } => {
                return Err(RuntimeError::Protocol(
                    "server QUIC UDP path stream received client open command",
                ));
            }
        };
        commands.release_pending_command_bytes(pending_bytes);
        if should_close {
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "path_writer_drain",
                format_args!(
                    "role=server underlay=Udp path_id={} stream_id={} sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={}",
                    path_id.0,
                    stream_id.0,
                    sent_items,
                    sent_bytes,
                    byte_budget,
                    item_budget,
                    commands.pending_bytes(),
                    drain_started.elapsed().as_micros(),
                    false,
                    false,
                ),
            );
            return Ok(true);
        }
        sent_items = sent_items.saturating_add(1);
        if sent_bytes >= byte_budget || sent_items >= item_budget {
            flush_udp_frame_batch_with_path_proofs(
                send,
                pending_frames,
                context.codec_limits,
                path_proofs,
            )
            .await?;
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "path_writer_drain",
                format_args!(
                    "role=server underlay=Udp path_id={} stream_id={} sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={}",
                    path_id.0,
                    stream_id.0,
                    sent_items,
                    sent_bytes,
                    byte_budget,
                    item_budget,
                    commands.pending_bytes(),
                    drain_started.elapsed().as_micros(),
                    true,
                    sent_items >= item_budget,
                ),
            );
            return Ok(false);
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
    let (commands_tx, mut commands_rx) = reliable_path_command_channels(udp_path_command_queue(
        context.mux_limits,
        context.codec_limits,
    ));
    let mut send = send;
    let mut carrier_frames = spawn_quic_path_reader(
        recv,
        context.codec_limits,
        udp_path_command_queue(context.mux_limits, context.codec_limits),
    );
    let mut flows = Vec::<ServerUdpDatagramFlow>::new();
    let mut pending_frames = Vec::<Frame>::new();
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
        let command_may_recv = !reliable_path_receivers_closed(&commands_rx);
        tokio::select! {
            biased;
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
                            return Err(RuntimeError::Protocol("expired QUIC UDP path datagram received"));
                        }
                        let flow_index = flows
                            .iter()
                            .position(|flow| flow.flow_id == flow_id)
                            .ok_or(RuntimeError::Protocol("unknown QUIC UDP path datagram flow"))?;
                        let requests = flows
                            .get(flow_index)
                            .ok_or(RuntimeError::Protocol("unknown QUIC UDP path datagram flow"))?
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
                                eprintln!("warning: QUIC UDP path datagram worker queue full; dropping request");
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
                    Some(Ok(_)) => return Err(RuntimeError::Protocol("unexpected server QUIC UDP path datagram stream frame")),
                    Some(Err(RuntimeError::ReliablePathSessionClosed)) | None => return Ok(()),
                    Some(Err(err)) => return Err(err),
                }
                if let Some(command) = try_recv_reliable_path_command(&mut commands_rx) {
                    let result = drain_server_udp_datagram_commands(
                        command,
                        &mut commands_rx,
                        &mut send,
                        &context,
                        &mut flows,
                        &mut pending_frames,
                    )
                    .await?;
                    if result {
                        return Ok(());
                    }
                }
            }
            command = recv_reliable_path_command(&mut commands_rx), if command_may_recv => {
                match command {
                    Some(command) => {
                        let result = drain_server_udp_datagram_commands(
                            command,
                            &mut commands_rx,
                            &mut send,
                            &context,
                            &mut flows,
                            &mut pending_frames,
                        ).await;
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

async fn drain_server_udp_datagram_commands(
    first_command: ReliablePathCommand,
    commands: &mut ReliablePathCommandReceivers,
    send: &mut UdpPathSendStream,
    context: &ServerPathContext,
    flows: &mut Vec<ServerUdpDatagramFlow>,
    pending_frames: &mut Vec<Frame>,
) -> Result<bool, RuntimeError> {
    #[cfg(feature = "lab-diagnostics")]
    let drain_started = Instant::now();
    let byte_budget = reliable_path_command_writer_run_budget_bytes(context.mux_limits);
    let item_budget = reliable_path_command_writer_run_budget_items(context.mux_limits);
    let mut next_command = Some(first_command);
    pending_frames.clear();
    let mut sent_bytes = 0usize;
    let mut sent_items = 0usize;

    loop {
        let Some(command) = next_command
            .take()
            .or_else(|| try_recv_reliable_path_command(commands))
        else {
            if try_coalesce_reliable_path_writer_run(
                commands,
                &mut next_command,
                sent_items,
                sent_bytes,
                byte_budget,
                item_budget,
            )
            .await
            {
                continue;
            }
            flush_udp_frame_batch(send, pending_frames, context.codec_limits).await?;
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "path_writer_drain",
                format_args!(
                    "role=server underlay=Udp datagram=true sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={}",
                    sent_items,
                    sent_bytes,
                    byte_budget,
                    item_budget,
                    commands.pending_bytes(),
                    drain_started.elapsed().as_micros(),
                    false,
                    false,
                ),
            );
            return Ok(false);
        };
        let pending_bytes = reliable_path_command_pending_bytes(&command);
        let should_close = match command {
            ReliablePathCommand::SendFrame(frame) => {
                if let Frame::DatagramClose { flow_id } = frame {
                    flows.retain(|flow| flow.flow_id != flow_id);
                }
                pending_frames.push(frame);
                commands.release_pending_command_bytes(pending_bytes);
                sent_bytes = sent_bytes.saturating_add(pending_bytes.max(1));
                sent_items = sent_items.saturating_add(1);
                if sent_bytes >= byte_budget || sent_items >= item_budget {
                    flush_udp_frame_batch(send, pending_frames, context.codec_limits).await?;
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "path_writer_drain",
                        format_args!(
                            "role=server underlay=Udp datagram=true sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={}",
                            sent_items,
                            sent_bytes,
                            byte_budget,
                            item_budget,
                            commands.pending_bytes(),
                            drain_started.elapsed().as_micros(),
                            true,
                            sent_items >= item_budget,
                        ),
                    );
                    return Ok(false);
                }
                continue;
            }
            ReliablePathCommand::CloseStream(_) => {
                flush_udp_frame_batch(send, pending_frames, context.codec_limits).await?;
                let _ = udp_path_finish_stream(send);
                true
            }
            ReliablePathCommand::OpenStream { .. } => {
                return Err(RuntimeError::Protocol(
                    "server QUIC UDP path datagram stream received open command",
                ));
            }
        };
        commands.release_pending_command_bytes(pending_bytes);
        if should_close {
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "path_writer_drain",
                format_args!(
                    "role=server underlay=Udp datagram=true sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={}",
                    sent_items,
                    sent_bytes,
                    byte_budget,
                    item_budget,
                    commands.pending_bytes(),
                    drain_started.elapsed().as_micros(),
                    false,
                    false,
                ),
            );
            return Ok(true);
        }
        sent_items = sent_items.saturating_add(1);
        if sent_bytes >= byte_budget || sent_items >= item_budget {
            flush_udp_frame_batch(send, pending_frames, context.codec_limits).await?;
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "path_writer_drain",
                format_args!(
                    "role=server underlay=Udp datagram=true sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={}",
                    sent_items,
                    sent_bytes,
                    byte_budget,
                    item_budget,
                    commands.pending_bytes(),
                    drain_started.elapsed().as_micros(),
                    true,
                    sent_items >= item_budget,
                ),
            );
            return Ok(false);
        }
    }
}

async fn open_server_udp_datagram_flow(
    context: &ServerPathContext,
    commands_tx: &ReliablePathCommandSender,
    send: &mut UdpPathSendStream,
    flows: &mut Vec<ServerUdpDatagramFlow>,
    flow_id: DatagramFlowId,
    target: TargetAddr,
    _lane: FlowLane,
) -> Result<(), RuntimeError> {
    if flows.iter().any(|flow| flow.flow_id == flow_id) {
        return Err(RuntimeError::Protocol(
            "duplicate QUIC UDP path datagram flow",
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
        context.outbound_connect_timeout,
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

fn udp_path_frame_finished(err: &RuntimeError) -> bool {
    match err {
        RuntimeError::QuicCarrier(quic_carrier::QuicCarrierError::Read(_)) => true,
        RuntimeError::QuicCarrier(quic_carrier::QuicCarrierError::ReadExact(_)) => true,
        RuntimeError::QuicCarrier(quic_carrier::QuicCarrierError::UnexpectedEnd) => true,
        RuntimeError::QuicCarrier(quic_carrier::QuicCarrierError::Connection(_)) => true,
        _ => false,
    }
}

fn udp_runtime_error_is_expected_shutdown(err: &RuntimeError) -> bool {
    match err {
        RuntimeError::QuicCarrier(quic_carrier::QuicCarrierError::Read(_)) => true,
        RuntimeError::QuicCarrier(quic_carrier::QuicCarrierError::ReadExact(_)) => true,
        RuntimeError::QuicCarrier(quic_carrier::QuicCarrierError::UnexpectedEnd) => true,
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

fn quic_path_open_error_is_retryable(err: &RuntimeError) -> bool {
    matches!(
        err,
        RuntimeError::Io(_)
            | RuntimeError::Udp(_)
            | RuntimeError::QuicCarrier(_)
            | RuntimeError::RemoteClosed(_)
            | RuntimeError::Protocol(_)
            | RuntimeError::ReliablePathSessionClosed
    )
}

fn udp_path_command_queue(mux_limits: MuxLimits, _codec_limits: CodecLimits) -> usize {
    // This queue is a sender-service work queue, not a QUIC record-buffer queue.
    // QUIC reliable streams may split OwnerData into smaller records to reduce
    // stream head-of-line burst size, but that packetization detail must not
    // multiply the number of commands admitted above the carrier. Otherwise a
    // 12--32 KiB QUIC record cap would inflate the queue from the logical
    // product-flight budget to thousands of commands and recreate the hidden
    // backlog that caused zero-goodput bursts.  Keep queue capacity tied to the
    // logical sender quantum; the QUIC writer/flow-control path performs the
    // lower-level pacing.
    reliable_path_command_queue(mux_limits)
}

async fn resolve_first_socket_addr(path: &PathSpec) -> Result<SocketAddr, RuntimeError> {
    let mut addrs = lookup_host((path.endpoint.host.as_str(), path.endpoint.port)).await?;
    addrs.next().ok_or(RuntimeError::Protocol(
        "QUIC UDP path endpoint resolved no socket addresses",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nonblocking_udp_open_uses_zero_initial_window_without_accept() {
        let options = UdpStreamOpenOptions {
            wait_for_accept: false,
            role: StreamOpenRole::Validation,
        };

        assert_eq!(udp_stream_open_initial_max_offset(options, None), 0);
    }

    #[test]
    fn blocking_udp_open_uses_accepted_initial_window() {
        assert_eq!(
            udp_stream_open_initial_max_offset(UdpStreamOpenOptions::ACTIVE_WAIT, Some(8192)),
            8192
        );
    }

    fn quic_congestion(
        congestion_window: u64,
        pacing_rate_bps: Option<u64>,
    ) -> quic_carrier::CongestionMetrics {
        quic_carrier::CongestionMetrics {
            congestion_window,
            bytes_in_flight: Some(0),
            pending_bytes: 0,
            pacing_rate_bps,
            loss_ppm: None,
            ecn_ppm: None,
            newly_acked_bytes: None,
            product_data_written_bytes: 0,
            delivery_sample_count: 0,
            app_limited: true,
        }
    }

    fn with_product_data_written(
        mut metrics: quic_carrier::CongestionMetrics,
        bytes: u64,
    ) -> quic_carrier::CongestionMetrics {
        metrics.product_data_written_bytes = bytes;
        metrics
    }

    fn with_acked_bytes(
        mut metrics: quic_carrier::CongestionMetrics,
        bytes: u64,
        sample_count: u64,
    ) -> quic_carrier::CongestionMetrics {
        metrics.newly_acked_bytes = Some(bytes);
        metrics.delivery_sample_count = sample_count;
        metrics.app_limited = false;
        metrics
    }

    #[test]
    fn quic_product_payload_uses_sender_quantum_not_packet_train_cap() {
        let mux_limits = MuxLimits::default();
        let codec_limits = CodecLimits::default();
        let payload_cap = udp_path_max_stream_payload_bytes(codec_limits, mux_limits);

        assert!(
            payload_cap >= BBR_MAX_SEND_QUANTUM_BYTES,
            "QUIC product dispatch must stay BDP/service-quantum sized; only carrier serialization may split records"
        );
    }

    #[test]
    fn quic_reliable_stream_reader_queue_stays_logical_product_queue() {
        let mux_limits = MuxLimits::default();
        let codec_limits = CodecLimits::default();
        let queue = udp_reliable_stream_frame_queue(codec_limits, mux_limits);

        assert_eq!(
            queue,
            reliable_stream_frame_queue(mux_limits),
            "carrier recordization must not multiply the product reader queue or hide backlog"
        );
    }

    #[test]
    fn quic_stats_feed_sender_side_udp_path_metrics() {
        let mut tracker = UdpPathMetricTracker::default();
        let congestion = quic_congestion(4 * 1024 * 1024, Some(500_000_000));
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
        stats.frame_rx.acks = 4;
        let measured = tracker.quic.observe(
            stats,
            with_acked_bytes(
                with_product_data_written(congestion, 8 * 1024 * 1024),
                8 * 1024 * 1024,
                4,
            ),
            2,
        );
        assert_eq!(measured.direction, 2);
        assert_eq!(measured.delivery_sample_count, 4);
        assert!(measured.delivery_rate_bps > 0.0);
        assert!(measured.last_delivery_sample_at.is_some());
        assert!(!measured.app_limited);
    }

    #[test]
    fn quic_ack_only_stats_do_not_create_delivery_rate_evidence() {
        let mut tracker = UdpPathMetricTracker::default();
        let congestion = quic_congestion(4 * 1024 * 1024, Some(500_000_000));
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
    fn quic_tx_bytes_without_newly_acked_bytes_do_not_create_delivery_rate_evidence() {
        let mut tracker = UdpPathMetricTracker::default();
        let congestion = quic_congestion(4 * 1024 * 1024, Some(500_000_000));
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(50);
        stats.path.cwnd = 4 * 1024 * 1024;
        stats.path.current_mtu = 1400;
        let _ = tracker.quic.observe(stats, congestion, 2);

        tracker.quic.last_observed_at = Some(Instant::now() - Duration::from_millis(100));
        let tx_only = tracker.quic.observe(
            stats,
            with_product_data_written(congestion, 8 * 1024 * 1024),
            2,
        );

        assert_eq!(tx_only.delivery_sample_count, 0);
        assert!(tx_only.last_delivery_sample_at.is_none());
        assert_eq!(tx_only.delivery_rate_bps.round() as u64, 500_000_000);
    }

    #[test]
    fn quic_product_data_accepted_by_quinn_counts_as_queue_until_ack() {
        let mut tracker = UdpPathMetricTracker::default();
        let congestion = quic_congestion(4 * 1024 * 1024, Some(500_000_000));
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(50);
        stats.path.cwnd = 4 * 1024 * 1024;
        stats.path.current_mtu = 1400;
        let _ = tracker.quic.observe(stats, congestion, 2);

        let queued = tracker.quic.observe(
            stats,
            with_product_data_written(congestion, 8 * 1024 * 1024),
            2,
        );
        assert_eq!(queued.bytes_in_flight, 0);
        assert_eq!(queued.pending_bytes, 8 * 1024 * 1024);
        let product_metrics = path_metrics_from_quic_path(PathId(7), queued);
        assert_eq!(product_metrics.queue_bytes, 8 * 1024 * 1024);

        let partially_acked = tracker.quic.observe(
            stats,
            with_acked_bytes(
                with_product_data_written(congestion, 8 * 1024 * 1024),
                2 * 1024 * 1024,
                1,
            ),
            2,
        );
        assert_eq!(partially_acked.pending_bytes, 6 * 1024 * 1024);
    }

    #[test]
    fn quic_loss_unknown_is_not_reported_as_observed_zero() {
        let metrics = UdpPathMetrics {
            direction: 2,
            srtt: Duration::from_millis(20),
            rttvar: Duration::from_millis(2),
            min_rtt: Duration::from_millis(18),
            min_rtt_observed: true,
            delivery_rate_bps: 500_000_000.0,
            pacing_rate_bps: 500_000_000.0,
            inflight_hi: 4 * 1024 * 1024,
            bytes_in_flight: 128 * 1024,
            pending_bytes: 256 * 1024,
            loss_ppm: None,
            ecn_ppm: None,
            app_limited: true,
            ack_derived_data_seen: false,
            delivery_sample_count: 0,
            delivery_sample_bytes: 0,
            last_delivery_sample_at: None,
        };

        let path_metrics = path_metrics_from_quic_path(PathId(7), metrics);

        assert_eq!(path_metrics.loss_ppm, 0);
        assert!(!path_metrics.loss_observed);
        assert_eq!(path_metrics.ecn_ppm, 0);
        assert!(!path_metrics.ecn_observed);
        assert_eq!(path_metrics.bytes_in_flight, 128 * 1024);
        assert_eq!(path_metrics.queue_bytes, 128 * 1024);
    }

    #[test]
    fn quic_unknown_capacity_ack_sample_does_not_create_bulk_evidence() {
        let mut tracker = UdpPathMetricTracker::default();
        let congestion = quic_congestion(0, None);
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(50);

        let _ = tracker.quic.observe(stats, congestion, 2);

        tracker.quic.last_observed_at = Some(Instant::now() - Duration::from_millis(500));
        stats.frame_rx.acks = 1;
        let unknown_capacity = tracker.quic.observe(
            stats,
            with_acked_bytes(with_product_data_written(congestion, 4096), 4096, 1),
            2,
        );

        assert_eq!(unknown_capacity.delivery_sample_count, 0);
        assert!(unknown_capacity.last_delivery_sample_at.is_none());
        assert_eq!(
            unknown_capacity.delivery_rate_bps.round() as u64,
            default_path_rate_bps(UnderlayProtocol::Udp).round() as u64
        );
        assert!(unknown_capacity.app_limited);
    }

    #[test]
    fn quic_tiny_startup_pacing_does_not_poison_product_scheduler_rate() {
        let mut tracker = UdpPathMetricTracker::default();
        let congestion = quic_congestion(0, Some(4));
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(50);

        let startup = tracker.quic.observe(stats, congestion, 2);
        let udp_startup_rate = default_path_rate_bps(UnderlayProtocol::Udp).round() as u64;

        assert_eq!(startup.delivery_sample_count, 0);
        assert!(startup.last_delivery_sample_at.is_none());
        assert_eq!(startup.delivery_rate_bps.round() as u64, udp_startup_rate);
        assert_eq!(startup.pacing_rate_bps.round() as u64, udp_startup_rate);

        tracker.quic.last_observed_at = Some(Instant::now() - Duration::from_millis(500));
        stats.frame_rx.acks = 1;
        let app_limited =
            tracker
                .quic
                .observe(stats, with_product_data_written(congestion, 4096), 2);

        assert_eq!(app_limited.delivery_sample_count, 0);
        assert!(app_limited.last_delivery_sample_at.is_none());
        assert_eq!(
            app_limited.delivery_rate_bps.round() as u64,
            udp_startup_rate
        );
        assert_eq!(app_limited.pacing_rate_bps.round() as u64, udp_startup_rate);
        assert!(app_limited.app_limited);
    }

    #[test]
    fn quic_udp_command_queue_tracks_sender_quantum_not_record_size() {
        let mux_limits = MuxLimits::default();
        let codec_limits = CodecLimits::default();
        let product_queue = reliable_path_command_queue(mux_limits);
        let quic_udp_queue = udp_path_command_queue(mux_limits, codec_limits);
        let sender_quantum =
            reliable_relay_scheduler_quantum_cap(None, FlowLane::Throughput, mux_limits);
        let record_sized_queue = reliable_path_command_queue_for_payload(
            mux_limits,
            sender_quantum.min(UDP_DEFAULT_MTU_PAYLOAD_BYTES).max(1),
        );

        assert_eq!(
            quic_udp_queue, product_queue,
            "command queue capacity must stay tied to the logical sender quantum"
        );
        assert_ne!(
            quic_udp_queue, record_sized_queue,
            "carrier packet/record sizing must not inflate the command queue"
        );
    }

    #[test]
    fn quic_app_limited_low_ack_sample_does_not_poison_delivery_rate() {
        let mut tracker = UdpPathMetricTracker::default();
        let congestion = quic_congestion(4 * 1024 * 1024, Some(500_000_000));
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(50);
        stats.path.cwnd = 4 * 1024 * 1024;
        stats.path.current_mtu = 1400;
        let _ = tracker.quic.observe(stats, congestion, 2);

        tracker.quic.last_observed_at = Some(Instant::now() - Duration::from_millis(1000));
        stats.frame_rx.acks = 1;
        let app_limited = tracker.quic.observe(
            stats,
            with_acked_bytes(
                with_product_data_written(congestion, 32 * 1024),
                32 * 1024,
                1,
            ),
            2,
        );

        assert_eq!(app_limited.delivery_sample_count, 0);
        assert!(app_limited.last_delivery_sample_at.is_none());
        assert_eq!(app_limited.delivery_rate_bps.round() as u64, 500_000_000);
        assert!(app_limited.app_limited);
    }

    #[test]
    fn quic_initial_full_quantum_sample_does_not_seed_tiny_bulk_rate() {
        let mut tracker = UdpPathMetricTracker::default();
        let congestion = quic_congestion(PATH_OPEN_SCORE_BYTES as u64, Some(500_000_000));
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(50);
        stats.path.cwnd = PATH_OPEN_SCORE_BYTES as u64;
        stats.path.current_mtu = 1400;
        let startup = tracker.quic.observe(stats, congestion, 2);

        tracker.quic.last_observed_at = Some(Instant::now() - Duration::from_millis(1000));
        stats.frame_rx.acks = 1;
        let measured = tracker.quic.observe(
            stats,
            with_acked_bytes(
                with_product_data_written(congestion, PATH_OPEN_SCORE_BYTES as u64),
                PATH_OPEN_SCORE_BYTES as u64,
                1,
            ),
            2,
        );

        assert_eq!(measured.delivery_sample_count, 1);
        assert_eq!(
            measured.delivery_rate_bps.round() as u64,
            startup.delivery_rate_bps.round() as u64,
            "a single underfed validation quantum must not replace the startup/pacing fallback with a tiny rate"
        );
    }

    #[test]
    fn quic_app_limited_duplicate_ack_counts_as_ack_data_seen_not_bulk_rate() {
        let mut tracker = UdpPathMetricTracker::default();
        let congestion = quic_congestion(4 * 1024 * 1024, Some(500_000_000));
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(50);
        stats.path.cwnd = 4 * 1024 * 1024;
        stats.path.current_mtu = 1400;
        let _ = tracker.quic.observe(stats, congestion, 2);

        tracker.quic.last_observed_at = Some(Instant::now() - Duration::from_millis(1000));
        stats.frame_rx.acks = 1;
        let app_limited = tracker.quic.observe(
            stats,
            with_acked_bytes(
                with_product_data_written(congestion, 32 * 1024),
                32 * 1024,
                1,
            ),
            2,
        );
        let product_metrics = path_metrics_from_quic_path(PathId(7), app_limited);

        assert!(app_limited.ack_derived_data_seen);
        assert_eq!(app_limited.delivery_sample_count, 0);
        assert!(app_limited.app_limited);
        assert!(product_metrics.has_ack_derived_data_sample);
        assert_eq!(product_metrics.data_sample_count, 0);
    }

    #[test]
    fn quic_server_metrics_publish_ack_data_seen_even_when_app_limited() {
        let metrics = UdpPathMetrics {
            direction: 2,
            srtt: Duration::from_millis(50),
            rttvar: Duration::from_millis(5),
            min_rtt: Duration::from_millis(45),
            min_rtt_observed: true,
            delivery_rate_bps: 500_000_000.0,
            pacing_rate_bps: 500_000_000.0,
            inflight_hi: 4 * 1024 * 1024,
            bytes_in_flight: 0,
            pending_bytes: 0,
            loss_ppm: None,
            ecn_ppm: None,
            app_limited: true,
            ack_derived_data_seen: true,
            delivery_sample_count: 0,
            delivery_sample_bytes: 0,
            last_delivery_sample_at: None,
        };

        assert!(quic_path_metrics_should_publish_local_sender(metrics));
        let product_metrics = path_metrics_from_quic_path(PathId(7), metrics);
        assert!(product_metrics.has_ack_derived_data_sample);
        assert_eq!(product_metrics.data_sample_count, 0);
        assert!(product_metrics.app_limited);
    }

    #[test]
    fn quic_ack_after_prior_data_send_counts_as_ack_data_seen() {
        let mut tracker = UdpPathMetricTracker::default();
        let congestion = quic_congestion(4 * 1024 * 1024, Some(500_000_000));
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(50);
        stats.path.cwnd = 4 * 1024 * 1024;
        stats.path.current_mtu = 1400;
        let _ = tracker.quic.observe(stats, congestion, 2);

        let sent_without_ack =
            tracker
                .quic
                .observe(stats, with_product_data_written(congestion, 32 * 1024), 2);
        assert!(!sent_without_ack.ack_derived_data_seen);

        tracker.quic.last_observed_at = Some(Instant::now() - Duration::from_millis(1000));
        let ack_after_send = tracker.quic.observe(
            stats,
            with_acked_bytes(
                with_product_data_written(congestion, 32 * 1024),
                32 * 1024,
                1,
            ),
            2,
        );

        assert!(
            ack_after_send.ack_derived_data_seen,
            "QUIC ACK-derived data evidence must survive normal TX/ACK timing; it cannot require TX and ACK in the same metrics poll"
        );
        assert_eq!(ack_after_send.delivery_sample_count, 0);
        assert!(ack_after_send.app_limited);
    }

    #[test]
    fn quic_compressed_ack_sample_cannot_jump_beyond_startup_gain() {
        let mut tracker = UdpPathMetricTracker::default();
        let congestion = quic_congestion(4 * 1024 * 1024, Some(500_000_000));
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(50);
        stats.path.cwnd = 4 * 1024 * 1024;
        stats.path.current_mtu = 1400;
        let startup = tracker.quic.observe(stats, congestion, 2);

        tracker.quic.last_observed_at = Some(Instant::now() - Duration::from_millis(1));
        stats.frame_rx.acks = 64;
        let measured = tracker.quic.observe(
            stats,
            with_acked_bytes(
                with_product_data_written(congestion, 64 * 1024 * 1024),
                64 * 1024 * 1024,
                64,
            ),
            2,
        );

        assert_eq!(measured.delivery_sample_count, 64);
        assert!(measured.delivery_rate_bps <= startup.delivery_rate_bps * BBR_DEFAULT_CWND_GAIN);
    }

    #[test]
    fn quic_lower_full_sample_does_not_directly_reduce_bulk_rate_model() {
        let mut tracker = UdpPathMetricTracker::default();
        let congestion = quic_congestion(512 * 1024, Some(100_000_000));
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(50);
        stats.path.cwnd = 512 * 1024;
        stats.path.current_mtu = 1400;
        let _ = tracker.quic.observe(stats, congestion, 2);

        tracker.quic.last_observed_at = Some(Instant::now() - Duration::from_millis(50));
        stats.udp_tx.bytes = 8 * 1024 * 1024;
        stats.frame_tx.stream = 512;
        stats.frame_rx.acks = 16;
        let raised = tracker.quic.observe(
            stats,
            with_acked_bytes(
                with_product_data_written(congestion, 8 * 1024 * 1024),
                8 * 1024 * 1024,
                16,
            ),
            2,
        );

        tracker.quic.last_observed_at = Some(Instant::now() - Duration::from_millis(500));
        stats.udp_tx.bytes += 512 * 1024;
        stats.frame_tx.stream += 512;
        stats.frame_rx.acks += 16;
        let after_low = tracker.quic.observe(
            stats,
            with_acked_bytes(
                with_product_data_written(congestion, 8 * 1024 * 1024 + 512 * 1024),
                512 * 1024,
                16,
            ),
            2,
        );

        assert_eq!(after_low.delivery_sample_count, 32);
        assert_eq!(after_low.delivery_rate_bps, raised.delivery_rate_bps);
    }
}
