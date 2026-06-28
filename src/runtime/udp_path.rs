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

    async fn ensure_connection(&self) -> Result<udp_carrier::Connection, RuntimeError> {
        let mut current = self.connection.lock().await;
        if let Some(connection) = current.as_ref() {
            return Ok(connection.connection.clone());
        }
        let connection = connect_client_udp_path(&self.runtime).await?;
        let carrier_connection = connection.connection.clone();
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
}

struct ClientUdpPathConnection {
    _endpoint: udp_carrier::Endpoint,
    connection: udp_carrier::Connection,
}

pub(super) struct ClientUdpDatagramStream {
    pub(super) send: udp_carrier::SendStream,
    pub(super) frames: mpsc::Receiver<Result<Frame, RuntimeError>>,
    pub(super) runtime: ClientUdpPathSessionRuntime,
    pub(super) path_id: PathId,
}

pub(super) async fn bind_server_udp_endpoint(
    path: &PathSpec,
    context: &ServerPathContext,
) -> Result<udp_carrier::Endpoint, RuntimeError> {
    let addr = resolve_first_socket_addr(path).await?;
    Ok(udp_carrier::Endpoint::bind_server(
        addr,
        context.security.secret.as_bytes(),
        context.security.cipher,
        context.mux_limits,
        context.codec_limits,
    )
    .await?)
}

pub(super) async fn run_server_udp_listener(
    endpoint: udp_carrier::Endpoint,
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
    let endpoint = udp_carrier::Endpoint::bind_client(
        local_addr,
        runtime.security.secret.as_bytes(),
        runtime.security.cipher,
        runtime.mux_limits,
        runtime.codec_limits,
    )
    .await?;
    let connection = endpoint.connect(remote_addr).await?;
    perform_client_udp_path_handshake(&connection, runtime).await?;
    Ok(ClientUdpPathConnection {
        _endpoint: endpoint,
        connection,
    })
}

async fn perform_client_udp_path_handshake(
    connection: &udp_carrier::Connection,
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
    udp_carrier::write_frame(&mut send, &session_hello, runtime.codec_limits).await?;
    udp_carrier::write_frame(&mut send, &session_auth, runtime.codec_limits).await?;
    udp_carrier::write_frame(&mut send, &path_join, runtime.codec_limits).await?;
    udp_carrier::finish_stream(&mut send)?;

    let mut session_ready = false;
    let mut path_active = false;
    while !session_ready || !path_active {
        match udp_carrier::read_frame(&mut recv, runtime.codec_limits).await? {
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
    connection: udp_carrier::Connection,
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
    udp_carrier::write_frame(&mut send, &open, runtime.codec_limits).await?;
    let max_offset = loop {
        match udp_carrier::read_frame(&mut recv, runtime.codec_limits).await? {
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
        runtime.stream_frame_queue,
        receivers,
        frames_tx,
    ));
    Ok(TcpPathStream {
        stream_id,
        max_offset,
        class,
        underlay: UnderlayProtocol::Udp,
        max_frame_payload_bytes: udp_carrier::max_stream_payload_bytes(runtime.codec_limits),
        output: TcpPathStreamOutput::Fixed(commands),
        frames: frames_rx,
    })
}

async fn open_client_udp_datagram_stream(
    connection: udp_carrier::Connection,
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
    mut recv: udp_carrier::RecvStream,
    codec_limits: CodecLimits,
    queue_size: usize,
) -> mpsc::Receiver<Result<Frame, RuntimeError>> {
    let (frames_tx, frames_rx) = mpsc::channel(queue_size);
    tokio::spawn(async move {
        loop {
            let frame = match udp_carrier::read_frame(&mut recv, codec_limits).await {
                Ok(frame) => Ok(frame),
                Err(err) if udp_carrier_frame_finished(&err) => {
                    Err(RuntimeError::TcpPathSessionClosed)
                }
                Err(err) => Err(RuntimeError::UdpCarrierFrame(err)),
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
    mut send: udp_carrier::SendStream,
    recv: udp_carrier::RecvStream,
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
            let _ = udp_carrier::finish_stream(&mut send);
            return;
        }
        tokio::select! {
            frame = carrier_frames.recv() => {
                match frame {
                    Some(Ok(Frame::Ping { nonce })) => {
                        if let Err(err) = udp_carrier::write_frame(&mut send, &Frame::Pong { nonce }, codec_limits).await {
                            let _ = frames.send(Err(RuntimeError::UdpCarrierFrame(err))).await;
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
                    Some(TcpPathSessionCommand::SendFrame(frame)) => {
                        if let Err(err) = udp_carrier::write_frame(&mut send, &frame, codec_limits).await {
                            let _ = frames.send(Err(RuntimeError::UdpCarrierFrame(err))).await;
                            return;
                        }
                    }
                    Some(TcpPathSessionCommand::CloseStream(close_stream_id)) => {
                        if close_stream_id == stream_id {
                            let _ = udp_carrier::finish_stream(&mut send);
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
    connection: udp_carrier::Connection,
    context: ServerPathContext,
) -> Result<(), RuntimeError> {
    let (session_id, path_id, capabilities) =
        accept_server_udp_path_handshake(&connection, &context).await?;
    loop {
        let (send, recv) = match connection.accept_bi().await {
            Ok(streams) => streams,
            Err(err) => return Err(RuntimeError::UdpCarrierConnection(err)),
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

async fn accept_server_udp_path_handshake(
    connection: &udp_carrier::Connection,
    context: &ServerPathContext,
) -> Result<(SessionId, PathId, PathCapabilities), RuntimeError> {
    let (mut send, mut recv) = connection.accept_bi().await?;
    let session_id = match udp_carrier::read_frame(&mut recv, context.codec_limits).await? {
        Frame::SessionHello { session_id } => session_id,
        _ => return Err(RuntimeError::Protocol("expected UDP carrier SESSION_HELLO")),
    };
    let authenticator = SessionAuthenticator::new(context.security.secret.as_bytes())?;
    let now_unix_secs = current_unix_secs()?;
    let auth_freshness_window_secs = context.security.auth_freshness_window.as_secs();
    match udp_carrier::read_frame(&mut recv, context.codec_limits).await? {
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
        match udp_carrier::read_frame(&mut recv, context.codec_limits).await? {
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

    udp_carrier::write_frame(&mut send, &Frame::SessionReady, context.codec_limits).await?;
    udp_carrier::write_frame(
        &mut send,
        &Frame::PathStatus {
            path_id,
            status: crate::protocol::PathStatus::Active,
            capabilities,
        },
        context.codec_limits,
    )
    .await?;
    udp_carrier::finish_stream(&mut send)?;
    Ok((session_id, path_id, capabilities))
}

async fn handle_server_udp_bidi_stream(
    mut send: udp_carrier::SendStream,
    mut recv: udp_carrier::RecvStream,
    context: ServerPathContext,
    session_id: SessionId,
    path_id: PathId,
    capabilities: PathCapabilities,
) -> Result<(), RuntimeError> {
    match udp_carrier::read_frame(&mut recv, context.codec_limits).await? {
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
            udp_carrier::write_frame(&mut send, &Frame::Pong { nonce }, context.codec_limits)
                .await?;
            udp_carrier::finish_stream(&mut send)?;
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
    mut send: udp_carrier::SendStream,
    recv: udp_carrier::RecvStream,
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
                max_frame_payload_bytes: udp_carrier::max_stream_payload_bytes(
                    context.codec_limits,
                ),
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
            udp_carrier::write_frame(
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
            stream_id,
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
    stream_id: StreamId,
    commands_tx: TcpPathSessionCommandSender,
    commands_rx: TcpPathSessionCommandReceivers,
}

async fn run_server_udp_reliable_stream_loop(
    mut send: udp_carrier::SendStream,
    recv: udp_carrier::RecvStream,
    stream_context: ServerUdpReliableStreamLoop,
) -> Result<(), RuntimeError> {
    let ServerUdpReliableStreamLoop {
        context,
        session_id,
        path_id,
        stream_id,
        commands_tx,
        mut commands_rx,
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
                    Some(Ok(Frame::StreamClass { stream_id: received_stream_id, class }))
                        if received_stream_id == stream_id =>
                    {
                        context.tcp_streams.update_class(session_id, stream_id, class)?;
                    }
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
                        let _ = udp_carrier::finish_stream(&mut send);
                        return Ok(());
                    }
                    Some(Ok(Frame::Ping { nonce })) => {
                        udp_carrier::write_frame(&mut send, &Frame::Pong { nonce }, context.codec_limits).await?;
                    }
                    Some(Ok(Frame::SessionClose { reason })) => return Err(RuntimeError::RemoteClosed(reason)),
                    Some(Ok(_)) => return Err(RuntimeError::Protocol("unexpected server UDP carrier reliable stream frame")),
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
                    Some(TcpPathSessionCommand::SendFrame(frame)) => {
                        udp_carrier::write_frame(&mut send, &frame, context.codec_limits).await?;
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
                            let _ = udp_carrier::finish_stream(&mut send);
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
    send: udp_carrier::SendStream,
    recv: udp_carrier::RecvStream,
    context: ServerPathContext,
    stream_context: ServerUdpDatagramStreamContext,
) -> Result<(), RuntimeError> {
    let (commands_tx, mut commands_rx) =
        tcp_path_session_command_channels(udp_path_command_queue(context.mux_limits));
    let mut send = send;
    let mut carrier_frames = spawn_udp_carrier_reader(
        recv,
        context.codec_limits,
        udp_path_command_queue(context.mux_limits),
    );
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
            frame = carrier_frames.recv() => {
                match frame {
                    Some(Ok(Frame::OpenDatagramFlow { flow_id, target, class, .. })) => {
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
                                udp_carrier::write_frame(
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
                                udp_carrier::write_frame(&mut send, &Frame::DatagramClose { flow_id }, context.codec_limits).await?;
                            }
                        }
                    }
                    Some(Ok(Frame::DatagramFeedback { .. })) => {}
                    Some(Ok(Frame::DatagramClose { flow_id })) => {
                        flows.retain(|flow| flow.flow_id != flow_id);
                        if flows.is_empty() {
                            let _ = udp_carrier::finish_stream(&mut send);
                            return Ok(());
                        }
                    }
                    Some(Ok(Frame::Ping { nonce })) => {
                        udp_carrier::write_frame(&mut send, &Frame::Pong { nonce }, context.codec_limits).await?;
                    }
                    Some(Ok(Frame::SessionClose { reason })) => return Err(RuntimeError::RemoteClosed(reason)),
                    Some(Ok(_)) => return Err(RuntimeError::Protocol("unexpected server UDP carrier datagram stream frame")),
                    Some(Err(RuntimeError::TcpPathSessionClosed)) | None => return Ok(()),
                    Some(Err(err)) => return Err(err),
                }
            }
            command = recv_tcp_path_command(&mut commands_rx), if command_may_recv => {
                match command {
                    Some(TcpPathSessionCommand::SendFrame(frame)) => {
                        if let Frame::DatagramClose { flow_id } = frame {
                            flows.retain(|flow| flow.flow_id != flow_id);
                            udp_carrier::write_frame(&mut send, &Frame::DatagramClose { flow_id }, context.codec_limits).await?;
                        } else {
                            udp_carrier::write_frame(&mut send, &frame, context.codec_limits).await?;
                        }
                    }
                    Some(TcpPathSessionCommand::CloseStream(_)) => {
                        let _ = udp_carrier::finish_stream(&mut send);
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
    send: &mut udp_carrier::SendStream,
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
        udp_carrier::write_frame(
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
            udp_carrier::write_frame(
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

fn udp_runtime_error_is_expected_shutdown(err: &RuntimeError) -> bool {
    match err {
        RuntimeError::UdpCarrierConnection(udp_carrier::UdpCarrierConnectionError::Closed) => true,
        RuntimeError::UdpCarrierFrame(err) => udp_carrier_frame_finished(err),
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
