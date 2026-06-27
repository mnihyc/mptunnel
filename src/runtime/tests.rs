use super::*;
use crate::config::SharedSecret;
use crate::ingress::ProxyAuthConfig;
use crate::transport::Endpoint;
use crate::transport::tcp::bind_listener;
use tokio::io::duplex;

fn security() -> SecurityConfig {
    SecurityConfig::encrypted(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    )
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

fn udp_stream_path_indices(
    context: &ClientPathContext,
    class: TrafficClass,
    payload_bytes: usize,
) -> Vec<usize> {
    let observations =
        health_observations(&mut context.health.lock().expect("client path health lock").udp);
    ordered_reliable_path_indices(&context.udp_paths, &observations, class, payload_bytes)
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
        path_join_replay: Arc::new(Mutex::new(RecentIdCache::new(
            path_join_replay_cache_capacity(resources.max_streams),
        ))),
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

async fn spawn_udp_echo_target_count(count: usize) -> (SocketAddr, tokio::task::JoinHandle<()>) {
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
        let response = socks5::udp_datagram(&datagram.target, b"pong").expect("udp relay response");
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
                issued_at_unix_secs,
                auth_tag,
            } if auth_session_id == session_id
                && authenticator.verify_session_auth(SessionAuthCheck {
                    session_id,
                    nonce,
                    issued_at_unix_secs,
                    tag: auth_tag,
                    now_unix_secs: current_unix_secs()?,
                    freshness_window_secs: security.auth_freshness_window.as_secs(),
                }) => {}
            _ => return Err(RuntimeError::Protocol("invalid SESSION_AUTH")),
        }
        let (path_id, capabilities) = match framed.read_frame().await? {
            Frame::PathJoin {
                session_id: join_session_id,
                path_id,
                underlay,
                nonce,
                issued_at_unix_secs,
                capabilities,
                auth_tag,
            } if join_session_id == session_id
                && underlay == UnderlayProtocol::Tcp
                && authenticator.verify_path_join(PathJoinAuthCheck {
                    session_id,
                    path_id,
                    underlay,
                    nonce,
                    issued_at_unix_secs,
                    capabilities,
                    tag: auth_tag,
                    now_unix_secs: current_unix_secs()?,
                    freshness_window_secs: security.auth_freshness_window.as_secs(),
                }) =>
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
                    ..
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
            let frame = session_ref.open_frame(&buffer[..len])?;
            if session_ref.peer != peer {
                session_ref.peer = peer;
            }
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
            let frame = session_ref.open_frame(&buffer[..len])?;
            if session_ref.peer != peer {
                session_ref.peer = peer;
            }
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
            let frame = session_ref.open_frame(&buffer[..len])?;
            if session_ref.peer != peer {
                session_ref.peer = peer;
            }
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

#[tokio::test]
async fn server_udp_path_accepts_session_auth_before_hello() {
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
    let session_id = SessionId(78);
    let path_id = PathId(3);
    let (hello, auth, join) = authenticated_path_join_frames_for_session(
        &security(),
        &path,
        path_id,
        UnderlayProtocol::Udp,
        session_id,
    )
    .expect("auth frames");

    session
        .handle_frame(auth)
        .await
        .expect("auth first should advance handshake");
    assert!(matches!(session.state, ServerUdpPathState::AwaitPathJoin));

    session
        .handle_frame(hello)
        .await
        .expect("late hello should stay recoverable");
    session
        .handle_frame(join)
        .await
        .expect("join should establish after reordered auth");
    assert!(matches!(session.state, ServerUdpPathState::Established));
    assert_eq!(session.session_id, Some(session_id));
    assert_eq!(session.path_id, Some(path_id));
}

#[tokio::test]
async fn server_udp_path_accepts_path_join_before_session_frames() {
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
    let session_id = SessionId(79);
    let path_id = PathId(4);
    let (hello, auth, join) = authenticated_path_join_frames_for_session(
        &security(),
        &path,
        path_id,
        UnderlayProtocol::Udp,
        session_id,
    )
    .expect("auth frames");

    session
        .handle_frame(join)
        .await
        .expect("authenticated join should establish without ordered prelude");
    assert!(matches!(session.state, ServerUdpPathState::Established));
    assert_eq!(session.session_id, Some(session_id));
    assert_eq!(session.path_id, Some(path_id));

    session
        .handle_frame(hello)
        .await
        .expect("late hello should be idempotent");
    session
        .handle_frame(auth)
        .await
        .expect("late auth should be idempotent");
    assert!(matches!(session.state, ServerUdpPathState::Established));
    assert_eq!(session.session_id, Some(session_id));
    assert_eq!(session.path_id, Some(path_id));
}

#[tokio::test]
async fn server_udp_path_drops_early_stream_frame_before_reordered_handshake() {
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
    let early = Frame::OpenStream {
        stream_id: StreamId(9),
        target: TargetAddr::Ip("127.0.0.1:80".parse().expect("target")),
        ingress: IngressKind::Socks5,
        outbound: OutboundPolicy::Direct,
        class: TrafficClass::Interactive,
    };
    assert!(matches!(
        session
            .handle_frame(early)
            .await
            .expect("early frame should be dropped"),
        ServerUdpSessionOutcome::Active
    ));
    assert!(matches!(
        session.state,
        ServerUdpPathState::AwaitSessionHello
    ));

    let path = "udp://127.0.0.1:7443".parse::<PathSpec>().expect("path");
    let session_id = SessionId(80);
    let path_id = PathId(5);
    let (hello, auth, join) = authenticated_path_join_frames_for_session(
        &security(),
        &path,
        path_id,
        UnderlayProtocol::Udp,
        session_id,
    )
    .expect("auth frames");

    session.handle_frame(auth).await.expect("auth");
    session.handle_frame(hello).await.expect("late hello");
    session.handle_frame(join).await.expect("join");
    assert!(matches!(session.state, ServerUdpPathState::Established));
    assert_eq!(session.session_id, Some(session_id));
    assert_eq!(session.path_id, Some(path_id));
}

#[tokio::test]
async fn server_udp_listener_requires_control_before_unknown_peer_session() {
    let server_socket = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("server udp bind");
    let server_addr = server_socket.local_addr().expect("server addr");
    let server = tokio::spawn(run_server_udp_listener(
        server_socket,
        server_context(OutboundConfig::Direct),
    ));

    let client_socket = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("client udp bind");
    client_socket
        .connect(server_addr)
        .await
        .expect("client udp connect");
    let security = security();
    let resources = ResourceLimits::default();
    let mut encrypted = EncryptedUdpSocket::new_with_cipher_suite(
        client_socket,
        security.secret.as_bytes(),
        PeerRole::Client,
        resources.into(),
        security.cipher,
    );

    encrypted
        .send_frame(&Frame::OpenStream {
            stream_id: StreamId(11),
            target: TargetAddr::Ip("127.0.0.1:80".parse().expect("target")),
            ingress: IngressKind::Socks5,
            outbound: OutboundPolicy::Direct,
            class: TrafficClass::Interactive,
        })
        .await
        .expect("early open");

    let path = format!("udp://{server_addr}")
        .parse::<PathSpec>()
        .expect("path");
    let session_id = SessionId(81);
    let path_id = PathId(6);
    let (hello, auth, join) = authenticated_path_join_frames_for_session(
        &security,
        &path,
        path_id,
        UnderlayProtocol::Udp,
        session_id,
    )
    .expect("auth frames");
    encrypted.send_frame(&hello).await.expect("hello");
    encrypted.send_frame(&auth).await.expect("auth");
    encrypted.send_frame(&join).await.expect("join");

    let mut buffer = vec![0u8; encrypted.max_datagram_bytes().expect("datagram size")];
    let mut session_ready = false;
    let mut path_active = false;
    let deadline = Instant::now() + Duration::from_secs(2);
    while !session_ready || !path_active {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out waiting for UDP path establishment"
        );
        match tokio::time::timeout(remaining, encrypted.recv_frame(&mut buffer))
            .await
            .expect("establish timeout")
            .expect("establish frame")
        {
            Frame::SessionReady => session_ready = true,
            Frame::PathStatus {
                path_id: active_path,
                status: crate::protocol::PathStatus::Active,
                ..
            } if active_path == path_id => path_active = true,
            frame => panic!("unexpected establishment frame: {frame:?}"),
        }
    }

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn server_udp_path_handles_idempotent_control_and_drain() {
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
    let path = "udp://127.0.0.1:7444".parse::<PathSpec>().expect("path");
    let session_id = SessionId(78);
    let path_id = PathId(3);
    let (hello, auth, join) = authenticated_path_join_frames_for_session(
        &security(),
        &path,
        path_id,
        UnderlayProtocol::Udp,
        session_id,
    )
    .expect("auth frames");

    session.handle_frame(hello).await.expect("hello");
    session.handle_frame(auth).await.expect("auth");
    session.handle_frame(join).await.expect("join");
    assert!(matches!(session.state, ServerUdpPathState::Established));

    assert!(matches!(
        session
            .handle_frame(Frame::PathChallenge { path_id, nonce: 99 })
            .await
            .expect("path challenge"),
        ServerUdpSessionOutcome::Active
    ));
    assert!(matches!(
        session
            .handle_frame(Frame::PathStatus {
                path_id,
                status: crate::protocol::PathStatus::Active,
                capabilities: crate::protocol::PathCapabilities::default(),
            })
            .await
            .expect("path status"),
        ServerUdpSessionOutcome::Active
    ));
    assert!(matches!(
        session
            .handle_frame(Frame::PathMetrics {
                metrics: crate::protocol::PathMetrics {
                    path_id,
                    min_rtt_us: 1_000,
                    srtt_us: 2_000,
                    rttvar_us: 500,
                    jitter_us: 250,
                    delivery_rate_bps: 10_000_000,
                    loss_ppm: 1_000,
                    ecn_ppm: 0,
                    bytes_in_flight: 1024,
                    queue_bytes: 2048,
                },
            })
            .await
            .expect("path metrics"),
        ServerUdpSessionOutcome::Active
    ));
    assert!(matches!(
        session
            .handle_frame(Frame::RxRateHint {
                path_id,
                hint: RateHint::BitsPerSecond(10_000_000),
            })
            .await
            .expect("rate hint"),
        ServerUdpSessionOutcome::Active
    ));
    let active_stream = StreamId(123);
    session.attached_streams.insert(active_stream);
    assert!(matches!(
        session
            .handle_frame(Frame::PathDrain { path_id })
            .await
            .expect("path drain"),
        ServerUdpSessionOutcome::Active
    ));
    assert!(session.draining);
    assert!(matches!(
        session
            .handle_frame(Frame::OpenStream {
                stream_id: StreamId(124),
                target: TargetAddr::Domain {
                    host: "example.com".to_string(),
                    port: 443,
                },
                ingress: IngressKind::Socks5,
                outbound: OutboundPolicy::Direct,
                class: TrafficClass::Interactive,
            })
            .await
            .expect("open after drain"),
        ServerUdpSessionOutcome::Active
    ));
    assert!(!session.attached_streams.contains(&StreamId(124)));
    assert!(matches!(
        session
            .handle_command(TcpPathSessionCommand::CloseStream(active_stream))
            .await
            .expect("close active stream"),
        ServerUdpSessionOutcome::Closed
    ));
    assert!(session.draining);
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
    let high_bdp_path = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 180.0, 300_000_000.0);
    let high_bdp_threshold = tcp_auto_bulk_threshold_bytes(Some(high_bdp_path), mux_limits);
    let high_bdp =
        ((high_bdp_path.delivery_rate_bps / 8.0) * (high_bdp_path.srtt_ms / 1000.0)).ceil() as u64;
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
    let bulk_chunk = adaptive_tcp_relay_chunk_bytes(Some(stable), TrafficClass::Bulk, mux_limits);
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
    let cross_continent = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 900.0, 300_000_000.0);

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
    let full = tcp_relay_repair_replay_interval(mux_limits.max_tcp_path_inflight_bytes, mux_limits);
    let udp_full =
        udp_stream_repair_replay_interval(mux_limits.max_tcp_path_inflight_bytes, mux_limits);

    assert!(light >= TCP_STREAM_STALL_MIN_TIMEOUT);
    assert!(light < full);
    assert_eq!(full, TCP_STREAM_STALL_MAX_TIMEOUT);
    assert_eq!(udp_full, TCP_STREAM_STALL_MIN_TIMEOUT);
    assert!(full < Duration::from_secs(5));
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

#[test]
fn server_tcp_registry_updates_stream_class_without_reopen() {
    let registry = ServerTcpStreamRegistry::new(ResourceLimits::default().max_streams);
    let target = TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80)));
    let (commands, _rx) = tcp_path_session_command_channels(4);
    let opened = registry
        .open_or_attach(
            ServerTcpStreamOpenRequest {
                session_id: SessionId(1),
                stream_id: StreamId(7),
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
            ResourceLimits::default().max_streams,
        )
        .expect("open stream");
    let ServerTcpStreamOpen::New(stream) = opened else {
        panic!("expected new stream");
    };
    let TcpPathStreamOutput::Switchable(binding) = stream.output else {
        panic!("expected switchable binding");
    };

    registry
        .update_class(SessionId(1), StreamId(7), TrafficClass::Bulk)
        .expect("class update");

    assert_eq!(binding.class(), TrafficClass::Bulk);
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

mod datagram;
mod integration;
mod security;
mod tcp_path;
mod udp_stream;
