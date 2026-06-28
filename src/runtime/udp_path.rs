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

    pub(super) async fn open_stream(
        &self,
        stream_id: StreamId,
        target: TargetAddr,
        ingress: IngressKind,
        class: TrafficClass,
        role: StreamOpenRole,
    ) -> Result<TcpPathStream, RuntimeError> {
        let connection = self.ensure_connection().await?;
        match open_client_udp_stream_on_connection(
            connection,
            stream_id,
            target.clone(),
            ingress,
            class,
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
                    class,
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

    async fn ensure_connection(&self) -> Result<quinn::Connection, RuntimeError> {
        let mut current = self.connection.lock().await;
        if let Some(connection) = current.as_ref() {
            return Ok(connection.connection.clone());
        }
        let connection = connect_client_udp_path(&self.runtime).await?;
        let quinn_connection = connection.connection.clone();
        *current = Some(connection);
        Ok(quinn_connection)
    }

    async fn drop_connection(&self) {
        let mut current = self.connection.lock().await;
        if let Some(connection) = current.take() {
            connection
                .connection
                .close(0_u32.into(), b"mptunnel path reconnect");
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
}

#[derive(Clone)]
struct ClientUdpPathConnection {
    _endpoint: quinn::Endpoint,
    connection: quinn::Connection,
}

pub(super) struct ClientUdpDatagramStream {
    pub(super) send: quinn::SendStream,
    pub(super) recv: quinn::RecvStream,
    pub(super) runtime: ClientUdpPathSessionRuntime,
    pub(super) path_id: PathId,
}

pub(super) async fn bind_server_udp_endpoint(
    path: &PathSpec,
    context: &ServerPathContext,
) -> Result<quinn::Endpoint, RuntimeError> {
    let addr = resolve_first_socket_addr(path).await?;
    Ok(quinn::Endpoint::new(
        quinn::EndpointConfig::default(),
        Some(quic_transport::server_config(
            context.security.secret.as_bytes(),
            context.mux_limits,
        )?),
        std::net::UdpSocket::bind(addr)?,
        Arc::new(quinn::TokioRuntime),
    )?)
}

fn bind_client_udp_endpoint(local_addr: SocketAddr) -> Result<quinn::Endpoint, RuntimeError> {
    Ok(quinn::Endpoint::new(
        quinn::EndpointConfig::default(),
        None,
        std::net::UdpSocket::bind(local_addr)?,
        Arc::new(quinn::TokioRuntime),
    )?)
}

pub(super) async fn run_server_udp_listener(
    endpoint: quinn::Endpoint,
    context: ServerPathContext,
) -> Result<(), RuntimeError> {
    loop {
        let Some(incoming) = endpoint.accept().await else {
            return Err(RuntimeError::Protocol("UDP carrier endpoint closed"));
        };
        let context = context.clone();
        tokio::spawn(async move {
            match incoming.await {
                Ok(connection) => {
                    if let Err(err) = handle_server_udp_connection(connection, context).await {
                        if !udp_runtime_error_is_expected_shutdown(&err) {
                            eprintln!("warning: server UDP carrier connection failed: {err}");
                        }
                    }
                }
                Err(err) => {
                    eprintln!("warning: server UDP carrier accept failed: {err}");
                }
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
    let mut endpoint = bind_client_udp_endpoint(local_addr)?;
    endpoint.set_default_client_config(quic_transport::client_config(
        runtime.security.secret.as_bytes(),
        runtime.mux_limits,
    )?);
    let connecting = endpoint.connect(remote_addr, quic_transport::CONNECT_SERVER_NAME)?;
    let connection = connecting.await?;
    perform_client_udp_path_handshake(&connection, runtime).await?;
    Ok(ClientUdpPathConnection {
        _endpoint: endpoint,
        connection,
    })
}

async fn perform_client_udp_path_handshake(
    connection: &quinn::Connection,
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
    quic_transport::write_frame(&mut send, &session_hello, runtime.codec_limits).await?;
    quic_transport::write_frame(&mut send, &session_auth, runtime.codec_limits).await?;
    quic_transport::write_frame(&mut send, &path_join, runtime.codec_limits).await?;
    quic_transport::finish_stream(&mut send)?;

    let mut session_ready = false;
    let mut path_active = false;
    while !session_ready || !path_active {
        match quic_transport::read_frame(&mut recv, runtime.codec_limits).await? {
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
    connection: quinn::Connection,
    stream_id: StreamId,
    target: TargetAddr,
    ingress: IngressKind,
    class: TrafficClass,
    role: StreamOpenRole,
    runtime: ClientUdpPathSessionRuntime,
) -> Result<TcpPathStream, RuntimeError> {
    let (mut send, mut recv) = connection.open_bi().await?;
    let open = Frame::OpenStream {
        stream_id,
        target,
        ingress,
        outbound: OutboundPolicy::Direct,
        class,
        role,
    };
    quic_transport::write_frame(&mut send, &open, runtime.codec_limits).await?;
    let max_offset = loop {
        match quic_transport::read_frame(&mut recv, runtime.codec_limits).await? {
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
    let (commands, receivers) =
        tcp_path_session_command_channels(udp_path_command_queue(runtime.mux_limits));
    let (frames_tx, frames_rx) = mpsc::channel(runtime.stream_frame_queue);
    tokio::spawn(run_client_udp_stream(
        send,
        recv,
        stream_id,
        runtime.codec_limits,
        receivers,
        frames_tx,
    ));
    Ok(TcpPathStream {
        stream_id,
        max_offset,
        class,
        underlay: UnderlayProtocol::Udp,
        max_frame_payload_bytes: tcp_relay_buffer_len(runtime.mux_limits),
        output: TcpPathStreamOutput::Fixed(commands),
        frames: frames_rx,
    })
}

async fn open_client_udp_datagram_stream(
    connection: quinn::Connection,
    runtime: ClientUdpPathSessionRuntime,
) -> Result<ClientUdpDatagramStream, RuntimeError> {
    let (send, recv) = connection.open_bi().await?;
    Ok(ClientUdpDatagramStream {
        send,
        recv,
        path_id: PathId(runtime.path_index as u16),
        runtime,
    })
}

async fn run_client_udp_stream(
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
    stream_id: StreamId,
    codec_limits: CodecLimits,
    mut commands: TcpPathSessionCommandReceivers,
    frames: mpsc::Sender<Result<Frame, RuntimeError>>,
) {
    loop {
        let command_may_recv = !tcp_path_receivers_closed(&commands);
        if !command_may_recv {
            let _ = quic_transport::finish_stream(&mut send);
            return;
        }
        tokio::select! {
            frame = quic_transport::read_frame(&mut recv, codec_limits) => {
                match frame {
                    Ok(Frame::Ping { nonce }) => {
                        if let Err(err) = quic_transport::write_frame(&mut send, &Frame::Pong { nonce }, codec_limits).await {
                            let _ = frames.send(Err(RuntimeError::QuicFrame(err))).await;
                            return;
                        }
                    }
                    Ok(frame @ (Frame::StreamData { stream_id: received_stream_id, .. }
                        | Frame::StreamAck { stream_id: received_stream_id, .. }
                        | Frame::StreamMaxData { stream_id: received_stream_id, .. }
                        | Frame::StreamFin { stream_id: received_stream_id, .. }
                        | Frame::StreamReset { stream_id: received_stream_id, .. }))
                        if received_stream_id == stream_id =>
                    {
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
                            .send(Err(RuntimeError::Protocol("unexpected UDP carrier reliable stream frame")))
                            .await;
                        return;
                    }
                    Err(err) if udp_carrier_frame_finished(&err) => {
                        let _ = frames.send(Err(RuntimeError::TcpPathSessionClosed)).await;
                        return;
                    }
                    Err(err) => {
                        let _ = frames.send(Err(RuntimeError::QuicFrame(err))).await;
                        return;
                    }
                }
            }
            command = recv_tcp_path_command(&mut commands), if command_may_recv => {
                match command {
                    Some(TcpPathSessionCommand::SendFrame(frame)) => {
                        if let Err(err) = quic_transport::write_frame(&mut send, &frame, codec_limits).await {
                            let _ = frames.send(Err(RuntimeError::QuicFrame(err))).await;
                            return;
                        }
                    }
                    Some(TcpPathSessionCommand::CloseStream(close_stream_id)) => {
                        if close_stream_id == stream_id {
                            let _ = quic_transport::finish_stream(&mut send);
                            return;
                        }
                    }
                    Some(TcpPathSessionCommand::OpenStream { .. }) => {
                        let _ = frames
                            .send(Err(RuntimeError::Protocol("client UDP carrier stream received open command")))
                            .await;
                        return;
                    }
                    None => {}
                }
            }
        }
    }
}

async fn handle_server_udp_connection(
    connection: quinn::Connection,
    context: ServerPathContext,
) -> Result<(), RuntimeError> {
    let (session_id, path_id, capabilities) =
        accept_server_udp_path_handshake(&connection, &context).await?;
    loop {
        let (send, recv) = match connection.accept_bi().await {
            Ok(streams) => streams,
            Err(err) if udp_carrier_connection_error_is_expected_shutdown(&err) => return Ok(()),
            Err(err) => return Err(RuntimeError::QuicConnection(err)),
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
                if !udp_runtime_error_is_expected_shutdown(&err) {
                    eprintln!("warning: server UDP carrier stream failed: {err}");
                }
            }
        });
    }
}

async fn accept_server_udp_path_handshake(
    connection: &quinn::Connection,
    context: &ServerPathContext,
) -> Result<(SessionId, PathId, PathCapabilities), RuntimeError> {
    let (mut send, mut recv) = connection.accept_bi().await?;
    let session_id = match quic_transport::read_frame(&mut recv, context.codec_limits).await? {
        Frame::SessionHello { session_id } => session_id,
        _ => return Err(RuntimeError::Protocol("expected UDP carrier SESSION_HELLO")),
    };
    let authenticator = SessionAuthenticator::new(context.security.secret.as_bytes())?;
    let now_unix_secs = current_unix_secs()?;
    let auth_freshness_window_secs = context.security.auth_freshness_window.as_secs();
    match quic_transport::read_frame(&mut recv, context.codec_limits).await? {
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
    let (path_id, capabilities) =
        match quic_transport::read_frame(&mut recv, context.codec_limits).await? {
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

    quic_transport::write_frame(&mut send, &Frame::SessionReady, context.codec_limits).await?;
    quic_transport::write_frame(
        &mut send,
        &Frame::PathStatus {
            path_id,
            status: crate::protocol::PathStatus::Active,
            capabilities,
        },
        context.codec_limits,
    )
    .await?;
    quic_transport::finish_stream(&mut send)?;
    Ok((session_id, path_id, capabilities))
}

async fn handle_server_udp_bidi_stream(
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
    context: ServerPathContext,
    session_id: SessionId,
    path_id: PathId,
    capabilities: PathCapabilities,
) -> Result<(), RuntimeError> {
    match quic_transport::read_frame(&mut recv, context.codec_limits).await? {
        Frame::OpenStream {
            stream_id,
            target,
            class,
            role,
            ..
        } => {
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
                    class,
                    role,
                },
            )
            .await
        }
        Frame::OpenDatagramFlow {
            flow_id,
            target,
            class,
            ..
        } => {
            handle_server_udp_datagram_stream(
                send,
                recv,
                context,
                ServerUdpDatagramStreamContext {
                    flow_id,
                    target,
                    class,
                },
            )
            .await
        }
        Frame::Ping { nonce } => {
            quic_transport::write_frame(&mut send, &Frame::Pong { nonce }, context.codec_limits)
                .await?;
            quic_transport::finish_stream(&mut send)?;
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
    class: TrafficClass,
    role: StreamOpenRole,
}

async fn handle_server_udp_reliable_stream(
    mut send: quinn::SendStream,
    recv: quinn::RecvStream,
    context: ServerPathContext,
    stream_context: ServerUdpReliableStreamContext,
) -> Result<(), RuntimeError> {
    let ServerUdpReliableStreamContext {
        session_id,
        path_id,
        capabilities,
        stream_id,
        target,
        class,
        role,
    } = stream_context;
    outbound::validate_target(&target)?;
    context.outbound.ensure_supports(TargetProtocol::Tcp)?;
    let (commands_tx, commands_rx) =
        tcp_path_session_command_channels(udp_path_command_queue(context.mux_limits));
    match context.tcp_streams.open_or_attach(
        ServerTcpStreamOpenRequest {
            session_id,
            stream_id,
            target: &target,
            class,
            attachment: ServerTcpPathAttachment {
                path_id,
                underlay: UnderlayProtocol::Udp,
                commands: commands_tx.clone(),
                max_frame_payload_bytes: tcp_relay_buffer_len(context.mux_limits),
                role,
            },
        },
        context.mux_limits,
        context.max_tcp_streams,
    )? {
        ServerTcpStreamOpen::New(stream) => {
            let stream_context = context.clone();
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
            quic_transport::write_frame(
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
        context,
        session_id,
        path_id,
        stream_id,
        commands_tx,
        commands_rx,
    )
    .await
}

async fn run_server_udp_reliable_stream_loop(
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
    context: ServerPathContext,
    session_id: SessionId,
    path_id: PathId,
    stream_id: StreamId,
    commands_tx: TcpPathSessionCommandSender,
    mut commands_rx: TcpPathSessionCommandReceivers,
) -> Result<(), RuntimeError> {
    loop {
        let command_may_recv = !tcp_path_receivers_closed(&commands_rx);
        tokio::select! {
            frame = quic_transport::read_frame(&mut recv, context.codec_limits) => {
                match frame {
                    Ok(Frame::StreamClass { stream_id: received_stream_id, class })
                        if received_stream_id == stream_id =>
                    {
                        context.tcp_streams.update_class(session_id, stream_id, class)?;
                    }
                    Ok(frame @ (Frame::StreamData { stream_id: received_stream_id, .. }
                        | Frame::StreamAck { stream_id: received_stream_id, .. }
                        | Frame::StreamMaxData { stream_id: received_stream_id, .. }
                        | Frame::StreamFin { stream_id: received_stream_id, .. }
                        | Frame::StreamReset { stream_id: received_stream_id, .. }))
                        if received_stream_id == stream_id =>
                    {
                        context.tcp_streams.route_frame(session_id, stream_id, frame).await?;
                    }
                    Ok(Frame::StreamDetach { stream_id: detach_stream_id })
                        if detach_stream_id == stream_id =>
                    {
                        context.tcp_streams.detach_path(
                            session_id,
                            stream_id,
                            UnderlayProtocol::Udp,
                            path_id,
                            &commands_tx,
                        );
                        let _ = quic_transport::finish_stream(&mut send);
                        return Ok(());
                    }
                    Ok(Frame::Ping { nonce }) => {
                        quic_transport::write_frame(&mut send, &Frame::Pong { nonce }, context.codec_limits).await?;
                    }
                    Ok(Frame::SessionClose { reason }) => return Err(RuntimeError::RemoteClosed(reason)),
                    Ok(_) => return Err(RuntimeError::Protocol("unexpected server UDP carrier reliable stream frame")),
                    Err(err) if udp_carrier_frame_finished(&err) => {
                        context.tcp_streams.detach_path(
                            session_id,
                            stream_id,
                            UnderlayProtocol::Udp,
                            path_id,
                            &commands_tx,
                        );
                        return Ok(());
                    }
                    Err(err) => return Err(RuntimeError::QuicFrame(err)),
                }
            }
            command = recv_tcp_path_command(&mut commands_rx), if command_may_recv => {
                match command {
                    Some(TcpPathSessionCommand::SendFrame(frame)) => {
                        quic_transport::write_frame(&mut send, &frame, context.codec_limits).await?;
                    }
                    Some(TcpPathSessionCommand::CloseStream(close_stream_id)) => {
                        if close_stream_id == stream_id {
                            context.tcp_streams.detach_path(
                                session_id,
                                stream_id,
                                UnderlayProtocol::Udp,
                                path_id,
                                &commands_tx,
                            );
                            let _ = quic_transport::finish_stream(&mut send);
                            return Ok(());
                        }
                    }
                    Some(TcpPathSessionCommand::OpenStream { .. }) => {
                        return Err(RuntimeError::Protocol("server UDP carrier stream received client open command"));
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
    class: TrafficClass,
}

async fn handle_server_udp_datagram_stream(
    send: quinn::SendStream,
    recv: quinn::RecvStream,
    context: ServerPathContext,
    stream_context: ServerUdpDatagramStreamContext,
) -> Result<(), RuntimeError> {
    let (commands_tx, mut commands_rx) =
        tcp_path_session_command_channels(udp_path_command_queue(context.mux_limits));
    let mut send = send;
    let mut recv = recv;
    let mut flows = Vec::<ServerUdpDatagramFlow>::new();
    open_server_udp_datagram_flow(
        &context,
        &commands_tx,
        &mut send,
        &mut flows,
        stream_context.flow_id,
        stream_context.target,
        stream_context.class,
    )
    .await?;
    loop {
        let command_may_recv = !tcp_path_receivers_closed(&commands_rx);
        tokio::select! {
            frame = quic_transport::read_frame(&mut recv, context.codec_limits) => {
                match frame {
                    Ok(Frame::OpenDatagramFlow { flow_id, target, class, .. }) => {
                        open_server_udp_datagram_flow(
                            &context,
                            &commands_tx,
                            &mut send,
                            &mut flows,
                            flow_id,
                            target,
                            class,
                        ).await?;
                    }
                    Ok(Frame::DatagramData { flow_id, datagram_id, ttl_ms, payload }) => {
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
                                quic_transport::write_frame(
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
                                quic_transport::write_frame(&mut send, &Frame::DatagramClose { flow_id }, context.codec_limits).await?;
                            }
                        }
                    }
                    Ok(Frame::DatagramFeedback { .. }) => {}
                    Ok(Frame::DatagramClose { flow_id }) => {
                        flows.retain(|flow| flow.flow_id != flow_id);
                        if flows.is_empty() {
                            let _ = quic_transport::finish_stream(&mut send);
                            return Ok(());
                        }
                    }
                    Ok(Frame::Ping { nonce }) => {
                        quic_transport::write_frame(&mut send, &Frame::Pong { nonce }, context.codec_limits).await?;
                    }
                    Ok(Frame::SessionClose { reason }) => return Err(RuntimeError::RemoteClosed(reason)),
                    Ok(_) => return Err(RuntimeError::Protocol("unexpected server UDP carrier datagram stream frame")),
                    Err(err) if udp_carrier_frame_finished(&err) => return Ok(()),
                    Err(err) => return Err(RuntimeError::QuicFrame(err)),
                }
            }
            command = recv_tcp_path_command(&mut commands_rx), if command_may_recv => {
                match command {
                    Some(TcpPathSessionCommand::SendFrame(frame)) => {
                        if let Frame::DatagramClose { flow_id } = frame {
                            flows.retain(|flow| flow.flow_id != flow_id);
                            quic_transport::write_frame(&mut send, &Frame::DatagramClose { flow_id }, context.codec_limits).await?;
                        } else {
                            quic_transport::write_frame(&mut send, &frame, context.codec_limits).await?;
                        }
                    }
                    Some(TcpPathSessionCommand::CloseStream(_)) => {
                        let _ = quic_transport::finish_stream(&mut send);
                        return Ok(());
                    }
                    Some(TcpPathSessionCommand::OpenStream { .. }) => {
                        return Err(RuntimeError::Protocol("server UDP carrier datagram stream received open command"));
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
    send: &mut quinn::SendStream,
    flows: &mut Vec<ServerUdpDatagramFlow>,
    flow_id: DatagramFlowId,
    target: TargetAddr,
    _class: TrafficClass,
) -> Result<(), RuntimeError> {
    if flows.iter().any(|flow| flow.flow_id == flow_id) {
        return Err(RuntimeError::Protocol(
            "duplicate UDP carrier datagram flow",
        ));
    }
    if flows.len() >= context.max_udp_flows_per_session {
        quic_transport::write_frame(
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
            quic_transport::write_frame(
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

fn udp_carrier_frame_finished(err: &quic_transport::QuicFrameError) -> bool {
    matches!(
        err,
        quic_transport::QuicFrameError::Read(quinn::ReadExactError::FinishedEarly(_))
            | quic_transport::QuicFrameError::Read(quinn::ReadExactError::ReadError(
                quinn::ReadError::ClosedStream
            ))
    ) || matches!(
        err,
        quic_transport::QuicFrameError::Read(quinn::ReadExactError::ReadError(
            quinn::ReadError::ConnectionLost(connection)
        )) if udp_carrier_connection_error_is_expected_shutdown(connection)
    )
}

fn udp_runtime_error_is_expected_shutdown(err: &RuntimeError) -> bool {
    match err {
        RuntimeError::QuicConnection(err) => udp_carrier_connection_error_is_expected_shutdown(err),
        RuntimeError::QuicFrame(err) => udp_carrier_frame_finished(err),
        RuntimeError::RemoteClosed(CloseReason::Normal) => true,
        _ => false,
    }
}

fn udp_carrier_connection_error_is_expected_shutdown(err: &quinn::ConnectionError) -> bool {
    matches!(
        err,
        quinn::ConnectionError::ApplicationClosed(_) | quinn::ConnectionError::LocallyClosed
    )
}

fn udp_carrier_open_error_is_path_retryable(err: &RuntimeError) -> bool {
    matches!(
        err,
        RuntimeError::Io(_)
            | RuntimeError::Udp(_)
            | RuntimeError::QuicTransport(_)
            | RuntimeError::QuicFrame(_)
            | RuntimeError::QuicConnect(_)
            | RuntimeError::QuicConnection(_)
            | RuntimeError::RemoteClosed(_)
            | RuntimeError::Protocol(_)
            | RuntimeError::TcpPathSessionClosed
    )
}

fn udp_path_command_queue(mux_limits: MuxLimits) -> usize {
    tcp_path_command_queue(mux_limits)
}

async fn resolve_first_socket_addr(path: &PathSpec) -> Result<SocketAddr, RuntimeError> {
    let mut addrs = lookup_host((path.endpoint.host.as_str(), path.endpoint.port)).await?;
    addrs.next().ok_or(RuntimeError::Protocol(
        "UDP carrier endpoint resolved no socket addresses",
    ))
}
