use super::*;

pub(super) async fn run_socks5_client_ingress(
    listen: Vec<SocketAddr>,
    context: ClientPathContext,
    proxy_auth: ProxyAuthConfig,
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
        let proxy_auth = proxy_auth.clone();
        listeners
            .spawn(async move { run_socks5_client_listener(listener, context, proxy_auth).await });
    }
    wait_for_ingress_listener_failure(listeners, "SOCKS5").await
}

pub(super) async fn run_socks5_client_listener(
    listener: TcpListener,
    context: ClientPathContext,
    proxy_auth: ProxyAuthConfig,
) -> Result<(), RuntimeError> {
    loop {
        let (stream, _) = listener.accept().await?;
        stream.set_nodelay(true)?;
        let context = context.clone();
        let proxy_auth = proxy_auth.clone();
        tokio::spawn(async move {
            if let Err(err) =
                handle_socks5_client_stream_with_auth(stream, context, proxy_auth).await
            {
                eprintln!("warning: SOCKS5 client handler failed: {err}");
            }
        });
    }
}

pub(super) async fn run_http_connect_client_ingress(
    listen: Vec<SocketAddr>,
    context: ClientPathContext,
    proxy_auth: ProxyAuthConfig,
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
        let proxy_auth = proxy_auth.clone();
        listeners.spawn(async move {
            run_http_connect_client_listener(listener, context, proxy_auth).await
        });
    }
    wait_for_ingress_listener_failure(listeners, "HTTP CONNECT").await
}

pub(super) async fn run_http_connect_client_listener(
    listener: TcpListener,
    context: ClientPathContext,
    proxy_auth: ProxyAuthConfig,
) -> Result<(), RuntimeError> {
    loop {
        let (stream, _) = listener.accept().await?;
        stream.set_nodelay(true)?;
        let context = context.clone();
        let proxy_auth = proxy_auth.clone();
        tokio::spawn(async move {
            if let Err(err) =
                handle_http_connect_client_stream_with_auth(stream, context, proxy_auth).await
            {
                eprintln!("warning: HTTP CONNECT client handler failed: {err}");
            }
        });
    }
}

pub(super) async fn wait_for_ingress_listener_failure(
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

#[cfg(test)]
pub(super) async fn handle_socks5_client_stream<S>(
    stream: S,
    context: ClientPathContext,
) -> Result<(), RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let proxy_auth = context.proxy_auth.clone();
    handle_socks5_client_stream_with_auth(stream, context, proxy_auth).await
}

pub(super) async fn handle_socks5_client_stream_with_auth<S>(
    mut stream: S,
    context: ClientPathContext,
    proxy_auth: ProxyAuthConfig,
) -> Result<(), RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    authenticate_socks5_client(&mut stream, &proxy_auth).await?;
    let request = read_socks5_command(&mut stream).await?;
    match request.command {
        socks5::Socks5Command::Connect => {
            let target = request.target;
            let remote = match open_remote_stream(
                &context,
                target.clone(),
                IngressKind::Socks5,
                FlowLane::Latency,
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
                    ReliableRelayOpenSpec {
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

#[cfg(test)]
pub(super) async fn handle_http_connect_client_stream<S>(
    stream: S,
    context: ClientPathContext,
) -> Result<(), RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let proxy_auth = context.proxy_auth.clone();
    handle_http_connect_client_stream_with_auth(stream, context, proxy_auth).await
}

pub(super) async fn handle_http_connect_client_stream_with_auth<S>(
    mut stream: S,
    context: ClientPathContext,
    proxy_auth: ProxyAuthConfig,
) -> Result<(), RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let request = read_http_connect(&mut stream).await?;
    if proxy_auth.is_required()
        && !proxy_auth.verify_basic_header(request.proxy_authorization.as_deref())
    {
        stream
            .write_all(http_connect::error_response(
                HttpStatus::ProxyAuthenticationRequired,
            ))
            .await?;
        return Err(RuntimeError::Protocol("HTTP proxy authentication failed"));
    }
    let target = request.target;
    let remote = match open_remote_stream(
        &context,
        target.clone(),
        IngressKind::HttpConnect,
        FlowLane::Latency,
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
            ReliableRelayOpenSpec {
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

pub(super) const DEFAULT_SOCKS5_UDP_TTL_MS: u32 = 30_000;

pub(super) async fn handle_socks5_udp_associate<S>(
    stream: &mut S,
    context: ClientPathContext,
    request: socks5::UdpAssociateRequest,
) -> Result<(), RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if context.udp_paths.is_empty() && context.tcp_paths.is_empty() {
        return Err(RuntimeError::NoDatagramPath);
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
                        route_hint: None,
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

pub(super) fn local_udp_buffer_len(mux_limits: MuxLimits) -> usize {
    const SOCKS5_UDP_HEADER_BUDGET_BYTES: usize = 512;
    const SOCKS5_UDP_PACKET_BUFFER_BYTES: usize = u16::MAX as usize;
    mux_limits
        .max_payload_bytes
        .saturating_add(SOCKS5_UDP_HEADER_BUDGET_BYTES)
        .clamp(
            SOCKS5_UDP_HEADER_BUDGET_BYTES,
            SOCKS5_UDP_PACKET_BUFFER_BYTES,
        )
}

pub(super) fn socks5_udp_peer_allowed(client_endpoint: &TargetAddr, peer: SocketAddr) -> bool {
    match client_endpoint {
        TargetAddr::Ip(addr) => {
            let ip_matches = addr.ip().is_unspecified() || addr.ip() == peer.ip();
            let port_matches = addr.port() == 0 || addr.port() == peer.port();
            ip_matches && port_matches
        }
        TargetAddr::Domain { port, .. } => *port == 0 || *port == peer.port(),
    }
}

pub(super) async fn open_udp_datagram_session_on_path(
    context: &ClientPathContext,
    path_index: usize,
    session_id: SessionId,
    handshake_timeout: Duration,
) -> Result<UdpDatagramClientSession, RuntimeError> {
    let path_session = context
        .udp_sessions
        .get(path_index)
        .cloned()
        .ok_or(RuntimeError::NoSchedulableUdpPath)?;
    let _ = session_id;
    let started_at = Instant::now();
    let session = UdpDatagramClientSession::open_from_udp_session(
        path_session,
        path_index,
        context.mux_limits,
        handshake_timeout,
    )
    .await?;
    context.mark_udp_path_open_success(path_index, started_at.elapsed());
    Ok(session)
}

pub(super) async fn probe_tcp_client_path(
    context: &ClientPathContext,
    path_index: usize,
    timeout: Duration,
) -> Result<Duration, RuntimeError> {
    let path = context
        .tcp_paths
        .get(path_index)
        .ok_or(RuntimeError::NoSchedulableTcpPath)?;
    let security = context.tcp_path_security(path_index)?;
    let probe_rtt = tokio::time::timeout(timeout, async {
        let tcp_stream = tcp::connect_path(
            path,
            TcpConnectOptions {
                timeout,
                ..TcpConnectOptions::default()
            },
        )
        .await?;
        let mut framed = EncryptedFramedStream::with_cipher_suite(
            tcp_stream,
            security.secret.as_bytes(),
            PeerRole::Client,
            context.codec_limits,
            security.cipher,
        )?;
        let path_id = PathId(path_index as u16);
        let (session_hello, session_auth, path_join) =
            authenticated_path_join_frames(security, path, path_id, UnderlayProtocol::Tcp)?;
        let nonce = random_u64()?;

        // Connection setup is liveness cost, not RTT. Time only the single
        // authenticated request/response exchange used by the path model.
        let ping_started_at = Instant::now();
        framed
            .write_frames(&[
                session_hello,
                session_auth,
                path_join,
                Frame::Ping { nonce },
            ])
            .await?;
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
        let probe_rtt = ping_started_at.elapsed();

        framed
            .write_frame(&Frame::SessionClose {
                reason: CloseReason::Normal,
            })
            .await?;
        framed.flush().await?;
        Ok(probe_rtt)
    })
    .await
    .map_err(|_| RuntimeError::Protocol("TCP path probe timed out"))??;
    Ok(probe_rtt)
}

pub(super) async fn probe_udp_client_path(
    context: &ClientPathContext,
    path_index: usize,
    timeout: Duration,
) -> Result<Duration, RuntimeError> {
    let path_session = context
        .udp_sessions
        .get(path_index)
        .cloned()
        .ok_or(RuntimeError::NoSchedulableUdpPath)?;
    let probe_rtt = tokio::time::timeout(timeout, async {
        let mut session = UdpDatagramClientSession::open_from_udp_session(
            path_session,
            path_index,
            context.mux_limits,
            timeout,
        )
        .await?;
        let ping_started_at = Instant::now();
        session.ping(timeout).await?;
        let probe_rtt = ping_started_at.elapsed();
        let _ = session.close_session().await;
        Ok::<Duration, RuntimeError>(probe_rtt)
    })
    .await
    .map_err(|_| RuntimeError::Protocol("UDP path probe timed out"))??;
    Ok(probe_rtt)
}

pub(super) async fn read_socks5_auth<S>(stream: &mut S) -> Result<socks5::AuthRequest, RuntimeError>
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

pub(super) async fn authenticate_socks5_client<S>(
    stream: &mut S,
    proxy_auth: &ProxyAuthConfig,
) -> Result<(), RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let auth = read_socks5_auth(stream).await?;
    if proxy_auth.is_required() {
        if !auth.supports_username_password() {
            stream
                .write_all(&socks5::no_acceptable_methods_response())
                .await?;
            return Err(RuntimeError::Protocol(
                "SOCKS5 client did not offer username/password auth",
            ));
        }
        stream
            .write_all(&socks5::username_password_method_response())
            .await?;
        let credentials = read_socks5_username_password_auth(stream).await?;
        let accepted = proxy_auth.verify(&credentials.username, &credentials.password);
        stream
            .write_all(&socks5::username_password_auth_response(accepted))
            .await?;
        if !accepted {
            return Err(RuntimeError::Protocol("SOCKS5 proxy authentication failed"));
        }
        return Ok(());
    }

    if !auth.supports_no_auth() {
        stream
            .write_all(&socks5::no_acceptable_methods_response())
            .await?;
        return Err(RuntimeError::Socks5(Socks5Error::UnsupportedCommand(0)));
    }
    stream.write_all(&socks5::no_auth_response()).await?;
    Ok(())
}

pub(super) async fn read_socks5_username_password_auth<S>(
    stream: &mut S,
) -> Result<socks5::UsernamePasswordAuthRequest, RuntimeError>
where
    S: AsyncRead + Unpin,
{
    let mut prefix = [0u8; 2];
    stream.read_exact(&mut prefix).await?;
    let username_len = prefix[1] as usize;
    let mut request = Vec::with_capacity(2 + username_len + 1);
    request.extend_from_slice(&prefix);
    request.resize(2 + username_len + 1, 0);
    stream.read_exact(&mut request[2..]).await?;
    let password_len = *request
        .last()
        .ok_or(RuntimeError::Protocol("missing SOCKS5 password length"))?
        as usize;
    let current_len = request.len();
    request.resize(
        current_len
            .checked_add(password_len)
            .ok_or(RuntimeError::Protocol("SOCKS5 auth message too long"))?,
        0,
    );
    stream.read_exact(&mut request[current_len..]).await?;
    let (auth, consumed) = socks5::parse_username_password_auth_request(&request)?;
    if consumed != request.len() {
        return Err(RuntimeError::Protocol("trailing SOCKS5 auth bytes"));
    }
    Ok(auth)
}

pub(super) async fn read_socks5_command<S>(
    stream: &mut S,
) -> Result<socks5::CommandRequest, RuntimeError>
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

pub(super) async fn read_http_connect<S>(
    stream: &mut S,
) -> Result<http_connect::ConnectRequest, RuntimeError>
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
