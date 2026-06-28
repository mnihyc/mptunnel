use super::datagram::datagram_ack_range;
use super::*;

pub async fn handle_server_udp_datagram_path_session(
    socket: UdpSocket,
    context: ServerPathContext,
) -> Result<(), RuntimeError> {
    let socket = Arc::new(socket);
    let probe = EncryptedUdpSocket::from_shared_with_cipher_suite(
        socket.clone(),
        context.security.secret.as_bytes(),
        PeerRole::Server,
        context.codec_limits,
        context.security.cipher,
    );
    let mut buffer = vec![0u8; probe.max_datagram_bytes()?];
    let mut session = None;
    loop {
        if session.is_none() {
            #[cfg(feature = "lab-diagnostics")]
            let recv_started = Instant::now();
            let (len, peer) = socket.recv_from(&mut buffer).await?;
            #[cfg(feature = "lab-diagnostics")]
            lab_perf_record(
                "runtime.udp_server.recv_from_wait",
                recv_started.elapsed(),
                len,
            );
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
            received = async {
                #[cfg(feature = "lab-diagnostics")]
                let recv_started = Instant::now();
                let result = socket.recv_from(&mut buffer).await;
                #[cfg(feature = "lab-diagnostics")]
                if let Ok((len, _)) = &result {
                    lab_perf_record("runtime.udp_server.recv_from_wait", recv_started.elapsed(), *len);
                }
                result
            } => {
                let (len, peer) = received?;
                let frame = match session_ref.open_frame(&buffer[..len]) {
                    Ok(frame) => frame,
                    Err(err) if udp_runtime_error_is_ignorable(&err) => continue,
                    Err(err) => return Err(err),
                };
                if session_ref.peer != peer {
                    session_ref.peer = peer;
                }
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

pub(super) struct ServerUdpDatagramFlow {
    pub(super) flow_id: DatagramFlowId,
    pub(super) requests: mpsc::Sender<ServerUdpDatagramRequest>,
}

pub(super) struct ServerUdpDatagramRequest {
    pub(super) datagram_id: DatagramId,
    pub(super) ttl_ms: u32,
    pub(super) payload: Bytes,
}

fn server_udp_datagram_request_queue_len(mux_limits: MuxLimits) -> usize {
    let unit = mux_limits.max_payload_bytes.max(1);
    mux_limits
        .max_datagram_queue_bytes
        .saturating_div(unit)
        .clamp(1, 1024)
}

pub(super) fn spawn_server_udp_datagram_flow_worker(
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

fn frame_kind_name(frame: &Frame) -> &'static str {
    match frame {
        Frame::SessionHello { .. } => "SESSION_HELLO",
        Frame::SessionAuth { .. } => "SESSION_AUTH",
        Frame::SessionReady => "SESSION_READY",
        Frame::SessionClose { .. } => "SESSION_CLOSE",
        Frame::PathJoin { .. } => "PATH_JOIN",
        Frame::PathJoinOk { .. } => "PATH_JOIN_OK",
        Frame::PathChallenge { .. } => "PATH_CHALLENGE",
        Frame::PathResponse { .. } => "PATH_RESPONSE",
        Frame::PathStatus { .. } => "PATH_STATUS",
        Frame::PathDrain { .. } => "PATH_DRAIN",
        Frame::PathClose { .. } => "PATH_CLOSE",
        Frame::PathMtuProbe { .. } => "PATH_MTU_PROBE",
        Frame::PathMtuAck { .. } => "PATH_MTU_ACK",
        Frame::OpenStream { .. } => "OPEN_STREAM",
        Frame::StreamClass { .. } => "STREAM_CLASS",
        Frame::StreamData { .. } => "STREAM_DATA",
        Frame::StreamAck { .. } => "STREAM_ACK",
        Frame::StreamMaxData { .. } => "STREAM_MAX_DATA",
        Frame::StreamFin { .. } => "STREAM_FIN",
        Frame::StreamDetach { .. } => "STREAM_DETACH",
        Frame::StreamReset { .. } => "STREAM_RESET",
        Frame::OpenDatagramFlow { .. } => "OPEN_DGRAM_FLOW",
        Frame::DatagramData { .. } => "DGRAM_DATA",
        Frame::DatagramClose { .. } => "DGRAM_CLOSE",
        Frame::DatagramFeedback { .. } => "DGRAM_FEEDBACK",
        Frame::PathMetrics { .. } => "PATH_METRICS",
        Frame::RxRateHint { .. } => "RX_RATE_HINT",
        Frame::MaxConnectionData { .. } => "MAX_CONNECTION_DATA",
        Frame::Ping { .. } => "PING",
        Frame::Pong { .. } => "PONG",
    }
}

fn frame_subject(frame: &Frame) -> String {
    match frame {
        Frame::SessionHello { session_id } => format!("session_id={}", session_id.0),
        Frame::SessionAuth { session_id, .. } => format!("session_id={}", session_id.0),
        Frame::SessionReady => "none".to_string(),
        Frame::SessionClose { reason } => format!("reason={reason:?}"),
        Frame::PathJoin {
            session_id,
            path_id,
            underlay,
            ..
        } => format!(
            "session_id={} path_id={} underlay={underlay:?}",
            session_id.0, path_id.0
        ),
        Frame::PathJoinOk { path_id, .. }
        | Frame::PathChallenge { path_id, .. }
        | Frame::PathResponse { path_id, .. }
        | Frame::PathDrain { path_id }
        | Frame::PathMtuProbe { path_id, .. }
        | Frame::PathMtuAck { path_id, .. }
        | Frame::RxRateHint { path_id, .. } => format!("path_id={}", path_id.0),
        Frame::PathStatus {
            path_id, status, ..
        } => format!("path_id={} status={status:?}", path_id.0),
        Frame::PathClose { path_id, reason } => {
            format!("path_id={} reason={reason:?}", path_id.0)
        }
        Frame::OpenStream {
            stream_id, class, ..
        } => format!("stream_id={} class={class:?}", stream_id.0),
        Frame::StreamClass { stream_id, class } => {
            format!("stream_id={} class={class:?}", stream_id.0)
        }
        Frame::StreamData {
            stream_id,
            offset,
            payload,
            ..
        } => format!(
            "stream_id={} offset={} payload_len={}",
            stream_id.0,
            offset,
            payload.len()
        ),
        Frame::StreamAck { stream_id, ranges } => {
            format!("stream_id={} ranges={}", stream_id.0, ranges.len())
        }
        Frame::StreamMaxData {
            stream_id,
            max_offset,
        } => format!("stream_id={} max_offset={max_offset}", stream_id.0),
        Frame::StreamFin { stream_id, .. } | Frame::StreamDetach { stream_id } => {
            format!("stream_id={}", stream_id.0)
        }
        Frame::StreamReset { stream_id, reason } => {
            format!("stream_id={} reason={reason:?}", stream_id.0)
        }
        Frame::OpenDatagramFlow { flow_id, class, .. } => {
            format!("flow_id={} class={class:?}", flow_id.0)
        }
        Frame::DatagramData {
            flow_id,
            datagram_id,
            ttl_ms,
            payload,
        } => format!(
            "flow_id={} datagram_id={} ttl_ms={} payload_len={}",
            flow_id.0,
            datagram_id.0,
            ttl_ms,
            payload.len()
        ),
        Frame::DatagramClose { flow_id } => format!("flow_id={}", flow_id.0),
        Frame::DatagramFeedback { flow_id, received } => {
            format!("flow_id={} ranges={}", flow_id.0, received.len())
        }
        Frame::PathMetrics { metrics } => format!("path_id={}", metrics.path_id.0),
        Frame::MaxConnectionData { max_bytes } => format!("max_bytes={max_bytes}"),
        Frame::Ping { nonce } | Frame::Pong { nonce } => format!("nonce={nonce}"),
    }
}

pub(super) fn log_unexpected_stream_relay_frame(
    kind: &'static str,
    expected: StreamId,
    frame: &Frame,
) {
    eprintln!(
        "warning: unexpected {kind} stream relay frame: expected_stream_id={} frame_kind={} {}",
        expected.0,
        frame_kind_name(frame),
        frame_subject(frame)
    );
}

pub(super) struct ServerUdpPathSession {
    pub(super) peer: SocketAddr,
    pub(super) encrypted: EncryptedUdpSocket,
    context: ServerPathContext,
    authenticator: SessionAuthenticator,
    pub(super) state: ServerUdpPathState,
    pub(super) draining: bool,
    pub(super) flows: Vec<ServerUdpDatagramFlow>,
    commands_tx: TcpPathSessionCommandSender,
    pub(super) commands_rx: TcpPathSessionCommandReceivers,
    pub(super) attached_streams: HashSet<StreamId>,
    pub(super) session_id: Option<SessionId>,
    pub(super) path_id: Option<PathId>,
    path_capabilities: Option<crate::protocol::PathCapabilities>,
}

pub(super) enum ServerUdpPathState {
    AwaitSessionHello,
    AwaitSessionAuth,
    AwaitPathJoin,
    Established,
}

fn server_udp_path_state_name(state: &ServerUdpPathState) -> &'static str {
    match state {
        ServerUdpPathState::AwaitSessionHello => "AwaitSessionHello",
        ServerUdpPathState::AwaitSessionAuth => "AwaitSessionAuth",
        ServerUdpPathState::AwaitPathJoin => "AwaitPathJoin",
        ServerUdpPathState::Established => "Established",
    }
}

pub(super) enum ServerUdpSessionOutcome {
    Active,
    Closed,
}

pub(super) enum ServerTcpPathEvent {
    Frame(Frame),
    Command(TcpPathSessionCommand),
}

pub(super) async fn recv_server_tcp_path_event(
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
    pub(super) fn new(
        socket: Arc<UdpSocket>,
        peer: SocketAddr,
        context: ServerPathContext,
    ) -> Result<Self, RuntimeError> {
        let authenticator = SessionAuthenticator::new(context.security.secret.as_bytes())?;
        let encrypted = EncryptedUdpSocket::from_shared_with_cipher_suite(
            socket,
            context.security.secret.as_bytes(),
            PeerRole::Server,
            context.codec_limits,
            context.security.cipher,
        );
        let (commands_tx, commands_rx) =
            tcp_path_session_command_channels(udp_stream_path_command_queue(context.mux_limits));
        Ok(Self {
            peer,
            encrypted,
            context,
            authenticator,
            state: ServerUdpPathState::AwaitSessionHello,
            draining: false,
            flows: Vec::new(),
            commands_tx,
            commands_rx,
            attached_streams: HashSet::new(),
            session_id: None,
            path_id: None,
            path_capabilities: None,
        })
    }

    pub(super) fn open_frame(&mut self, datagram: &[u8]) -> Result<Frame, RuntimeError> {
        Ok(self.encrypted.open_frame_datagram(datagram)?)
    }

    async fn establish_udp_path(
        &mut self,
        session_id: SessionId,
        path_id: PathId,
        capabilities: PathCapabilities,
    ) -> Result<ServerUdpSessionOutcome, RuntimeError> {
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
        self.session_id = Some(session_id);
        self.path_id = Some(path_id);
        self.path_capabilities = Some(capabilities);
        self.draining = false;
        self.state = ServerUdpPathState::Established;
        Ok(ServerUdpSessionOutcome::Active)
    }

    fn verify_session_auth(
        &self,
        session_id: SessionId,
        nonce: AuthNonce,
        issued_at_unix_secs: u64,
        auth_tag: crate::protocol::AuthTag,
    ) -> bool {
        let Ok(now_unix_secs) = current_unix_secs() else {
            return false;
        };
        self.authenticator.verify_session_auth(SessionAuthCheck {
            session_id,
            nonce,
            issued_at_unix_secs,
            tag: auth_tag,
            now_unix_secs,
            freshness_window_secs: self.context.security.auth_freshness_window.as_secs(),
        })
    }

    fn verify_path_join(&self, check: PathJoinAuthCheck) -> bool {
        let Ok(now_unix_secs) = current_unix_secs() else {
            return false;
        };
        self.authenticator.verify_path_join(PathJoinAuthCheck {
            now_unix_secs,
            freshness_window_secs: self.context.security.auth_freshness_window.as_secs(),
            ..check
        })
    }

    pub(super) async fn handle_frame(
        &mut self,
        frame: Frame,
    ) -> Result<ServerUdpSessionOutcome, RuntimeError> {
        match (&self.state, frame) {
            (ServerUdpPathState::AwaitSessionHello, Frame::SessionHello { .. }) => {
                self.state = ServerUdpPathState::AwaitSessionAuth;
                Ok(ServerUdpSessionOutcome::Active)
            }
            (ServerUdpPathState::Established, Frame::SessionHello { session_id })
                if Some(session_id) == self.session_id =>
            {
                self.send_established_udp_path_ready().await?;
                Ok(ServerUdpSessionOutcome::Active)
            }
            (_, Frame::SessionHello { .. }) => {
                self.flows.clear();
                self.attached_streams.clear();
                self.session_id = None;
                self.path_id = None;
                self.path_capabilities = None;
                self.draining = false;
                self.state = ServerUdpPathState::AwaitSessionAuth;
                Ok(ServerUdpSessionOutcome::Active)
            }
            (
                ServerUdpPathState::AwaitSessionHello
                | ServerUdpPathState::AwaitSessionAuth
                | ServerUdpPathState::AwaitPathJoin,
                Frame::SessionAuth {
                    session_id,
                    nonce,
                    issued_at_unix_secs,
                    auth_tag,
                },
            ) if self.verify_session_auth(session_id, nonce, issued_at_unix_secs, auth_tag) => {
                self.state = ServerUdpPathState::AwaitPathJoin;
                Ok(ServerUdpSessionOutcome::Active)
            }
            (
                ServerUdpPathState::Established,
                Frame::SessionAuth {
                    session_id: auth_session_id,
                    nonce,
                    issued_at_unix_secs,
                    auth_tag,
                },
            ) if Some(auth_session_id) == self.session_id
                && self.verify_session_auth(
                    auth_session_id,
                    nonce,
                    issued_at_unix_secs,
                    auth_tag,
                ) =>
            {
                self.send_established_udp_path_ready().await?;
                Ok(ServerUdpSessionOutcome::Active)
            }
            (
                ServerUdpPathState::AwaitSessionHello
                | ServerUdpPathState::AwaitSessionAuth
                | ServerUdpPathState::AwaitPathJoin,
                Frame::PathJoin {
                    session_id,
                    path_id,
                    underlay,
                    nonce,
                    issued_at_unix_secs,
                    capabilities,
                    auth_tag,
                },
            ) if underlay == UnderlayProtocol::Udp
                && self.verify_path_join(PathJoinAuthCheck {
                    session_id,
                    path_id,
                    underlay,
                    nonce,
                    issued_at_unix_secs,
                    capabilities,
                    tag: auth_tag,
                    now_unix_secs: 0,
                    freshness_window_secs: 0,
                })
                && self
                    .context
                    .accept_path_join_nonce(session_id, path_id, underlay, nonce) =>
            {
                self.establish_udp_path(session_id, path_id, capabilities)
                    .await
            }
            (
                ServerUdpPathState::Established,
                Frame::PathJoin {
                    session_id: join_session_id,
                    path_id,
                    underlay,
                    nonce,
                    issued_at_unix_secs,
                    capabilities,
                    auth_tag,
                },
            ) if Some(join_session_id) == self.session_id
                && Some(path_id) == self.path_id
                && underlay == UnderlayProtocol::Udp
                && self.verify_path_join(PathJoinAuthCheck {
                    session_id: join_session_id,
                    path_id,
                    underlay,
                    nonce,
                    issued_at_unix_secs,
                    capabilities,
                    tag: auth_tag,
                    now_unix_secs: 0,
                    freshness_window_secs: 0,
                }) =>
            {
                self.path_capabilities = Some(capabilities);
                self.send_established_udp_path_ready().await?;
                Ok(ServerUdpSessionOutcome::Active)
            }
            (ServerUdpPathState::Established, Frame::PathChallenge { path_id, nonce })
                if Some(path_id) == self.path_id =>
            {
                self.encrypted
                    .send_frame_to(&Frame::PathResponse { path_id, nonce }, self.peer)
                    .await?;
                Ok(ServerUdpSessionOutcome::Active)
            }
            (
                ServerUdpPathState::Established,
                Frame::PathResponse { path_id, .. }
                | Frame::PathMetrics {
                    metrics: crate::protocol::PathMetrics { path_id, .. },
                }
                | Frame::RxRateHint { path_id, .. }
                | Frame::PathStatus { path_id, .. },
            ) if Some(path_id) == self.path_id => Ok(ServerUdpSessionOutcome::Active),
            (
                ServerUdpPathState::Established,
                Frame::PathDrain {
                    path_id: drain_path_id,
                },
            ) if Some(drain_path_id) == self.path_id => {
                self.draining = true;
                let (_, path_id, capabilities) = self.established_stream_context()?;
                self.encrypted
                    .send_frame_to(
                        &Frame::PathStatus {
                            path_id,
                            status: crate::protocol::PathStatus::Draining,
                            capabilities,
                        },
                        self.peer,
                    )
                    .await?;
                if self.flows.is_empty() && self.attached_streams.is_empty() {
                    Ok(ServerUdpSessionOutcome::Closed)
                } else {
                    Ok(ServerUdpSessionOutcome::Active)
                }
            }
            (
                ServerUdpPathState::Established,
                Frame::PathClose {
                    path_id: close_path_id,
                    ..
                },
            ) if Some(close_path_id) == self.path_id => Ok(ServerUdpSessionOutcome::Closed),
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
                    role,
                    ..
                },
            ) if !self.draining => {
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
                            role,
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
            (ServerUdpPathState::Established, Frame::OpenStream { stream_id, .. }) => {
                self.encrypted
                    .send_frame_to(
                        &Frame::StreamReset {
                            stream_id,
                            reason: ResetReason::Refused,
                        },
                        self.peer,
                    )
                    .await?;
                Ok(ServerUdpSessionOutcome::Active)
            }
            (ServerUdpPathState::Established, Frame::StreamClass { stream_id, class }) => {
                let (session_id, _, _) = self.established_stream_context()?;
                self.context
                    .tcp_streams
                    .update_class(session_id, stream_id, class)?;
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
            (
                ServerUdpPathState::Established,
                Frame::StreamFin {
                    stream_id,
                    final_offset,
                },
            ) => {
                let (session_id, _, _) = self.established_stream_context()?;
                self.context
                    .tcp_streams
                    .route_frame(
                        session_id,
                        stream_id,
                        Frame::StreamFin {
                            stream_id,
                            final_offset,
                        },
                    )
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
            ) if !self.draining => {
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
            (ServerUdpPathState::Established, Frame::OpenDatagramFlow { flow_id, .. }) => {
                self.encrypted
                    .send_frame_to(&Frame::DatagramClose { flow_id }, self.peer)
                    .await?;
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
            (
                ServerUdpPathState::AwaitSessionHello
                | ServerUdpPathState::AwaitSessionAuth
                | ServerUdpPathState::AwaitPathJoin,
                ref early_frame,
            ) if udp_pre_establishment_frame_can_be_dropped(early_frame) => {
                eprintln!(
                    "warning: dropping early UDP datagram path frame before handshake completes: state={} frame_kind={} {}",
                    server_udp_path_state_name(&self.state),
                    frame_kind_name(early_frame),
                    frame_subject(early_frame)
                );
                Ok(ServerUdpSessionOutcome::Active)
            }
            (_, Frame::SessionClose { .. }) => Ok(ServerUdpSessionOutcome::Closed),
            (state, unexpected) => {
                eprintln!(
                    "warning: unexpected UDP datagram path frame: state={} session_id={:?} path_id={:?} draining={} attached_streams={} flows={} frame_kind={} {}",
                    server_udp_path_state_name(state),
                    self.session_id.map(|session_id| session_id.0),
                    self.path_id.map(|path_id| path_id.0),
                    self.draining,
                    self.attached_streams.len(),
                    self.flows.len(),
                    frame_kind_name(&unexpected),
                    frame_subject(&unexpected)
                );
                Err(RuntimeError::Protocol("unexpected UDP datagram path frame"))
            }
        }
    }

    pub(super) async fn handle_command(
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

fn udp_pre_establishment_frame_can_be_dropped(frame: &Frame) -> bool {
    matches!(
        frame,
        Frame::PathChallenge { .. }
            | Frame::PathResponse { .. }
            | Frame::PathStatus { .. }
            | Frame::PathDrain { .. }
            | Frame::PathClose { .. }
            | Frame::PathMtuProbe { .. }
            | Frame::PathMtuAck { .. }
            | Frame::OpenStream { .. }
            | Frame::StreamClass { .. }
            | Frame::StreamData { .. }
            | Frame::StreamAck { .. }
            | Frame::StreamMaxData { .. }
            | Frame::StreamFin { .. }
            | Frame::StreamDetach { .. }
            | Frame::StreamReset { .. }
            | Frame::OpenDatagramFlow { .. }
            | Frame::DatagramData { .. }
            | Frame::DatagramFeedback { .. }
            | Frame::DatagramClose { .. }
            | Frame::PathMetrics { .. }
            | Frame::RxRateHint { .. }
            | Frame::MaxConnectionData { .. }
            | Frame::Ping { .. }
            | Frame::Pong { .. }
    )
}
