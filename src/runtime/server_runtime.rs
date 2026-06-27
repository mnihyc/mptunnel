use super::*;

pub(super) async fn run_server(
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
        path_join_replay: Arc::new(Mutex::new(RecentIdCache::new(
            path_join_replay_cache_capacity(resources.max_streams),
        ))),
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

pub(super) enum BoundServerPath {
    Tcp(TcpListener),
    Udp(UdpSocket),
}

pub(super) async fn run_server_tcp_listener(
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

pub(super) async fn run_server_udp_listener(
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
    let mut sessions: HashMap<SocketAddr, mpsc::Sender<ServerUdpInboundDatagram>> = HashMap::new();
    let mut session_peers: HashMap<SessionId, SocketAddr> = HashMap::new();
    let (session_events_tx, mut session_events_rx) =
        mpsc::channel::<ServerUdpSessionEvent>(udp_session_done_queue(&context));
    loop {
        tokio::select! {
            received = socket.recv_from(&mut buffer) => {
                let (len, peer) = received?;
                let datagram = Bytes::copy_from_slice(&buffer[..len]);
                let control_session_id = if sessions.contains_key(&peer) {
                    None
                } else {
                    authenticated_udp_control_session_id(&socket, &context, &datagram)
                };
                if !sessions.contains_key(&peer)
                    && let Some(session_id) = control_session_id
                    && let Some(current_peer) = session_peers.get(&session_id).copied()
                    && let Some(tx) = sessions.get(&current_peer).cloned()
                {
                    sessions.insert(peer, tx);
                }
                if !sessions.contains_key(&peer) {
                    if control_session_id.is_none() {
                        eprintln!(
                            "warning: dropping unknown UDP datagram from {peer} before authenticated control"
                        );
                        continue;
                    }
                    if sessions.len() >= context.max_udp_sessions {
                        eprintln!(
                            "warning: UDP server session limit reached; dropping datagram from {peer}"
                        );
                        continue;
                    }
                    let (tx, rx) = mpsc::channel(udp_session_datagram_queue(&context));
                    let session_socket = socket.clone();
                    let session_context = context.clone();
                    let session_events = session_events_tx.clone();
                    tokio::spawn(async move {
                        match run_server_udp_peer_session(
                            session_socket,
                            peer,
                            session_context,
                            rx,
                            session_events.clone(),
                        )
                        .await
                        {
                            Ok((final_peer, session_id)) => {
                                let _ = session_events
                                    .send(ServerUdpSessionEvent::Closed {
                                        peer: final_peer,
                                        session_id,
                                    })
                                    .await;
                            }
                            Err(err) => {
                                eprintln!("warning: UDP server path session for {peer} failed: {err}");
                                let _ = session_events
                                    .send(ServerUdpSessionEvent::Closed {
                                        peer,
                                        session_id: None,
                                    })
                                    .await;
                            }
                        }
                    });
                    sessions.insert(peer, tx);
                }
                let send_result = sessions
                    .get(&peer)
                    .ok_or(RuntimeError::Protocol("missing UDP peer session"))?
                    .try_send(ServerUdpInboundDatagram { peer, payload: datagram });
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
            event = session_events_rx.recv() => {
                match event {
                    Some(ServerUdpSessionEvent::Established { session_id, peer }) => {
                        if let Some(previous_peer) = session_peers.insert(session_id, peer)
                            && previous_peer != peer
                            && let Some(tx) = sessions.remove(&previous_peer)
                        {
                            sessions.insert(peer, tx);
                        }
                    }
                    Some(ServerUdpSessionEvent::Closed { peer, session_id }) => {
                        sessions.remove(&peer);
                        if let Some(session_id) = session_id {
                            session_peers.remove(&session_id);
                        }
                    }
                    None => {}
                }
            }
        }
    }
}

pub(super) struct ServerUdpInboundDatagram {
    peer: SocketAddr,
    payload: Bytes,
}

pub(super) enum ServerUdpSessionEvent {
    Established {
        session_id: SessionId,
        peer: SocketAddr,
    },
    Closed {
        peer: SocketAddr,
        session_id: Option<SessionId>,
    },
}

fn authenticated_udp_control_session_id(
    socket: &Arc<UdpSocket>,
    context: &ServerPathContext,
    datagram: &[u8],
) -> Option<SessionId> {
    let mut encrypted = EncryptedUdpSocket::from_shared_with_cipher_suite(
        socket.clone(),
        context.security.secret.as_bytes(),
        PeerRole::Server,
        context.codec_limits,
        context.security.cipher,
    );
    udp_control_frame_session_id(&encrypted.open_frame_datagram(datagram).ok()?)
}

fn udp_control_frame_session_id(frame: &Frame) -> Option<SessionId> {
    match frame {
        Frame::SessionHello { session_id }
        | Frame::SessionAuth { session_id, .. }
        | Frame::PathJoin { session_id, .. } => Some(*session_id),
        _ => None,
    }
}

pub(super) async fn run_server_udp_peer_session(
    socket: Arc<UdpSocket>,
    peer: SocketAddr,
    context: ServerPathContext,
    mut datagrams: mpsc::Receiver<ServerUdpInboundDatagram>,
    session_events: mpsc::Sender<ServerUdpSessionEvent>,
) -> Result<(SocketAddr, Option<SessionId>), RuntimeError> {
    let mut session = ServerUdpPathSession::new(socket, peer, context)?;
    let mut reported_session = None;
    loop {
        let command_may_recv = !tcp_path_receivers_closed(&session.commands_rx);
        tokio::select! {
            datagram = datagrams.recv() => {
                let Some(datagram) = datagram else {
                    return Ok((session.peer, session.session_id));
                };
                let frame = match session.open_frame(&datagram.payload) {
                    Ok(frame) => frame,
                    Err(err) if udp_runtime_error_is_ignorable(&err) => continue,
                    Err(err) => return Err(err),
                };
                if session.peer != datagram.peer {
                    session.peer = datagram.peer;
                }
                match session.handle_frame(frame).await? {
                    ServerUdpSessionOutcome::Active => {}
                    ServerUdpSessionOutcome::Closed => return Ok((session.peer, session.session_id)),
                }
                if let Some(session_id) = session.session_id {
                    let current = Some((session_id, session.peer));
                    if reported_session != current {
                        session_events
                            .send(ServerUdpSessionEvent::Established {
                                session_id,
                                peer: session.peer,
                            })
                            .await
                            .map_err(|_| RuntimeError::Protocol("UDP session event receiver closed"))?;
                        reported_session = current;
                    }
                }
            }
            command = recv_tcp_path_command(&mut session.commands_rx), if command_may_recv => {
                if let Some(command) = command {
                    match session.handle_command(command).await? {
                        ServerUdpSessionOutcome::Active => {}
                        ServerUdpSessionOutcome::Closed => return Ok((session.peer, session.session_id)),
                    }
                }
            }
        }
    }
}

pub(super) fn udp_session_datagram_queue(context: &ServerPathContext) -> usize {
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

pub(super) fn udp_session_done_queue(context: &ServerPathContext) -> usize {
    context.max_udp_sessions.max(1)
}

pub(super) fn udp_runtime_error_is_ignorable(err: &RuntimeError) -> bool {
    matches!(
        err,
        RuntimeError::EncryptedUdp(EncryptedUdpTransportError::Replay)
    )
}

pub(super) fn encrypted_udp_error_is_ignorable(err: &EncryptedUdpTransportError) -> bool {
    matches!(err, EncryptedUdpTransportError::Replay)
}
