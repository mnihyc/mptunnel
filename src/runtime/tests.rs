use super::*;
use crate::config::SharedSecret;
use crate::transport::Endpoint;
use crate::transport::tcp::bind_listener;
use tokio::io::duplex;

fn security() -> SecurityConfig {
    SecurityConfig::encrypted(SharedSecret::new(b"0123456789abcdef".to_vec()).expect("secret"))
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

fn server_context(outbound: OutboundConfig) -> ServerPathContext {
    let resources = ResourceLimits::default();
    ServerPathContext {
        outbound,
        outbound_dns: DnsConfig::default(),
        codec_limits: resources.into(),
        mux_limits: resources.into(),
        security: security(),
        tcp_streams: Arc::new(ServerTcpStreamRegistry::default()),
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
            if session_ref.peer != peer {
                return Err(RuntimeError::Protocol(
                    "UDP datagram arrived from unexpected peer",
                ));
            }
            let frame = session_ref.open_frame(&buffer[..len])?;
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
            if session_ref.peer != peer {
                return Err(RuntimeError::Protocol(
                    "UDP datagram arrived from unexpected peer",
                ));
            }
            let frame = session_ref.open_frame(&buffer[..len])?;
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
            if session_ref.peer != peer {
                return Err(RuntimeError::Protocol(
                    "UDP datagram arrived from unexpected peer",
                ));
            }
            let frame = session_ref.open_frame(&buffer[..len])?;
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
fn udp_stream_congestion_self_clocks_and_cuts_back_on_repair_timeout() {
    let mux_limits = MuxLimits {
        max_tcp_path_inflight_bytes: 64 * 1024,
        max_tcp_relay_chunk_bytes: 64 * 1024,
        max_payload_bytes: 64 * 1024,
        ..MuxLimits::default()
    };
    let mss = udp_stream_frame_payload_bytes(mux_limits);
    let mut congestion = UdpStreamCongestion::new(mux_limits);
    let initial = congestion.inflight_limit();

    assert_eq!(initial, mss.saturating_mul(10).min(64 * 1024));
    assert_eq!(congestion.repair_budget(0), 0);
    assert_eq!(congestion.repair_budget(mss / 2), mss);

    congestion.on_ack(mss * 4);
    assert!(congestion.inflight_limit() > initial);

    for _ in 0..32 {
        congestion.on_ack(64 * 1024);
    }
    assert_eq!(congestion.inflight_limit(), 64 * 1024);

    congestion.on_repair_timeout();
    assert!(congestion.inflight_limit() < 64 * 1024);
    assert!(congestion.inflight_limit() >= udp_stream_min_cwnd_bytes(mss).min(64 * 1024));
}

#[test]
fn udp_stream_congestion_ceiling_uses_path_inflight_budget() {
    let mux_limits = MuxLimits {
        max_tcp_path_inflight_bytes: 4 * 1024 * 1024,
        max_tcp_relay_chunk_bytes: 256 * 1024,
        ..MuxLimits::default()
    };
    let mss = udp_stream_frame_payload_bytes(mux_limits);
    let mut congestion = UdpStreamCongestion::new(mux_limits);

    assert!(congestion.inflight_limit() < mux_limits.max_tcp_relay_chunk_bytes);
    for _ in 0..32 {
        congestion.on_ack(mux_limits.max_tcp_path_inflight_bytes);
    }
    assert_eq!(
        congestion.inflight_limit(),
        mux_limits.max_tcp_path_inflight_bytes
    );
    assert_eq!(
        congestion.repair_budget(usize::MAX),
        mux_limits.max_tcp_path_inflight_bytes / 4
    );
    assert!(congestion.repair_budget(usize::MAX) >= mux_limits.max_tcp_relay_chunk_bytes);
    assert!(congestion.repair_budget(mss / 2) >= mss);
}

#[test]
fn udp_stream_ack_gap_repair_budget_is_bounded_to_path_burst() {
    let mux_limits = MuxLimits {
        max_tcp_path_inflight_bytes: 4 * 1024 * 1024,
        max_tcp_relay_chunk_bytes: 256 * 1024,
        ..MuxLimits::default()
    };
    let mss = udp_stream_frame_payload_bytes(mux_limits);

    assert_eq!(udp_stream_ack_gap_repair_budget(0, mux_limits), 0);
    assert_eq!(udp_stream_ack_gap_repair_budget(mss / 2, mux_limits), mss);
    assert_eq!(
        udp_stream_ack_gap_repair_budget(usize::MAX, mux_limits),
        mux_limits.max_tcp_path_inflight_bytes / 4
    );
}

#[test]
fn udp_stream_congestion_paces_after_rtt_evidence() {
    let mux_limits = MuxLimits::default();
    let mss = udp_stream_frame_payload_bytes(mux_limits);
    let mut congestion = UdpStreamCongestion::new(mux_limits);

    assert_eq!(congestion.pacing_interval(mss), None);

    congestion.on_send(mss);
    let sample = congestion
        .pending_samples
        .front_mut()
        .expect("pending sample");
    sample.sent_at = sample
        .sent_at
        .checked_sub(Duration::from_millis(80))
        .expect("past sample");
    congestion.on_ack(mss);

    let interval = congestion
        .pacing_interval(mss)
        .expect("paced after ack RTT");
    assert!(interval > Duration::ZERO);
    assert!(interval < Duration::from_millis(80));
}

#[test]
fn udp_stream_repair_replay_uses_measured_ack_rtt() {
    let mux_limits = MuxLimits::default();
    let mss = udp_stream_frame_payload_bytes(mux_limits);
    let mut congestion = UdpStreamCongestion::new(mux_limits);
    let fallback =
        udp_stream_repair_replay_interval(mux_limits.max_tcp_path_inflight_bytes, mux_limits);

    assert_eq!(
        congestion.repair_replay_interval(mux_limits.max_tcp_path_inflight_bytes, mux_limits),
        fallback
    );

    congestion.on_send(mss);
    let sample = congestion
        .pending_samples
        .front_mut()
        .expect("pending sample");
    sample.sent_at = sample
        .sent_at
        .checked_sub(Duration::from_millis(360))
        .expect("past sample");
    congestion.on_ack(mss);

    let high_rtt_interval =
        congestion.repair_replay_interval(mux_limits.max_tcp_path_inflight_bytes, mux_limits);
    assert!(high_rtt_interval > fallback);
    assert!(high_rtt_interval <= TCP_STREAM_STALL_MAX_TIMEOUT);
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

#[tokio::test]
async fn tcp_path_control_command_bypasses_saturated_data_queue() {
    let (tx, mut rx) = tcp_path_session_command_channels(1);
    tx.send_frame(
        Frame::StreamData {
            stream_id: StreamId(3),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(b"queued-data"),
        },
        TrafficClass::Bulk,
    )
    .await
    .expect("fill data queue");

    tokio::time::timeout(
        Duration::from_millis(50),
        tx.send_control(TcpPathSessionCommand::CloseStream(StreamId(3))),
    )
    .await
    .expect("control send should not wait for data queue")
    .expect("control send");

    match recv_tcp_path_command(&mut rx).await.expect("first command") {
        TcpPathSessionCommand::CloseStream(stream_id) => assert_eq!(stream_id, StreamId(3)),
        _ => panic!("expected prioritized close stream control"),
    }
}

#[tokio::test]
async fn tcp_path_interactive_frame_bypasses_saturated_bulk_queue() {
    let (tx, mut rx) = tcp_path_session_command_channels(1);
    tx.send_frame(
        Frame::StreamData {
            stream_id: StreamId(10),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(b"bulk"),
        },
        TrafficClass::Bulk,
    )
    .await
    .expect("fill bulk data queue");

    tokio::time::timeout(
        Duration::from_millis(50),
        tx.send_frame(
            Frame::StreamData {
                stream_id: StreamId(11),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"i"),
            },
            TrafficClass::Interactive,
        ),
    )
    .await
    .expect("interactive send should not wait for bulk queue")
    .expect("interactive send");

    match recv_tcp_path_command(&mut rx).await.expect("first command") {
        TcpPathSessionCommand::SendFrame(Frame::StreamData {
            stream_id, payload, ..
        }) => {
            assert_eq!(stream_id, StreamId(11));
            assert_eq!(&payload[..], b"i");
        }
        _ => panic!("expected prioritized interactive stream data"),
    }
}

#[tokio::test]
async fn server_tcp_path_input_frame_bypasses_queued_bulk_output() {
    let (tx, mut commands_rx) = tcp_path_session_command_channels(1);
    tx.send_frame(
        Frame::StreamData {
            stream_id: StreamId(10),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(b"bulk"),
        },
        TrafficClass::Bulk,
    )
    .await
    .expect("fill bulk output command queue");
    let (frame_tx, mut path_frames) = mpsc::channel(1);
    frame_tx
        .send(Ok(Frame::Ping { nonce: 7 }))
        .await
        .expect("queue inbound ping");

    match recv_server_tcp_path_event(&mut path_frames, &mut commands_rx)
        .await
        .expect("server path event")
        .expect("event")
    {
        ServerTcpPathEvent::Frame(Frame::Ping { nonce }) => assert_eq!(nonce, 7),
        _ => panic!("expected inbound frame before queued bulk output"),
    }
}

#[tokio::test]
async fn client_tcp_path_ignores_late_frames_for_recently_closed_stream() {
    let stream_id = StreamId(7);
    let (frames_tx, frames_rx) = mpsc::channel(1);
    let mut streams = HashMap::new();
    streams.insert(
        stream_id,
        ClientTcpPathStreamState {
            frames: frames_tx,
            pending_open: None,
        },
    );
    let mut closed_streams = RecentIdCache::new(8);
    drop(frames_rx);

    route_client_tcp_stream_frame(
        &mut streams,
        &mut closed_streams,
        stream_id,
        Frame::StreamFin { stream_id },
    )
    .await
    .expect("receiver close should mark stream drained");
    assert!(!streams.contains_key(&stream_id));
    assert!(closed_streams.contains(&stream_id));

    route_client_tcp_stream_frame(
        &mut streams,
        &mut closed_streams,
        stream_id,
        Frame::StreamAck {
            stream_id,
            ranges: Vec::new(),
        },
    )
    .await
    .expect("late frame for closed stream should be ignored");

    let unknown = StreamId(99);
    let err = route_client_tcp_stream_frame(
        &mut streams,
        &mut closed_streams,
        unknown,
        Frame::StreamFin { stream_id: unknown },
    )
    .await
    .expect_err("unknown stream should remain a protocol error");
    assert!(matches!(err, RuntimeError::Protocol(_)));
}

#[tokio::test]
async fn server_tcp_registry_ignores_late_frames_for_recently_closed_stream() {
    let registry = ServerTcpStreamRegistry::new(8);
    let session_id = SessionId(11);
    let stream_id = StreamId(5);
    let (commands, _receivers) = tcp_path_session_command_channels(4);
    let target = TargetAddr::Domain {
        host: "example.com".to_string(),
        port: 443,
    };

    let opened = registry
        .open_or_attach(
            ServerTcpStreamOpenRequest {
                session_id,
                stream_id,
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
            8,
        )
        .expect("server stream open");
    assert!(matches!(opened, ServerTcpStreamOpen::New(_)));

    registry.close(session_id, stream_id);
    registry
        .route_frame(session_id, stream_id, Frame::StreamFin { stream_id })
        .await
        .expect("late server stream frame should be ignored");

    let unknown = StreamId(99);
    let err = registry
        .route_frame(session_id, unknown, Frame::StreamFin { stream_id: unknown })
        .await
        .expect_err("unknown server stream should remain a protocol error");
    assert!(matches!(err, RuntimeError::Protocol(_)));
}

#[tokio::test]
async fn server_tcp_relay_replays_response_repair_cache_on_path_reattach() {
    let mux_limits = MuxLimits::default();
    let stream_id = StreamId(42);
    let (mut target_peer, target_side) = duplex(4096);
    let (commands_tx, mut commands_rx) = tcp_path_session_command_channels(8);
    let (frames_tx, frames_rx) = mpsc::channel(8);
    let relay = tokio::spawn(relay_tcp_stream(
        target_side,
        TcpPathStream {
            stream_id,
            max_offset: mux_limits.max_stream_window_bytes,
            class: TrafficClass::Interactive,
            underlay: UnderlayProtocol::Tcp,
            max_frame_payload_bytes: tcp_relay_buffer_len(mux_limits),
            output: TcpPathStreamOutput::Fixed(commands_tx),
            frames: frames_rx,
        },
        mux_limits,
    ));

    target_peer
        .write_all(b"response")
        .await
        .expect("target write");
    let first = tokio::time::timeout(
        Duration::from_secs(1),
        recv_tcp_path_command(&mut commands_rx),
    )
    .await
    .expect("first relay frame timeout")
    .expect("first relay frame");
    match first {
        TcpPathSessionCommand::SendFrame(Frame::StreamData {
            stream_id: received_stream_id,
            offset,
            payload,
            ..
        }) => {
            assert_eq!(received_stream_id, stream_id);
            assert_eq!(offset, 0);
            assert_eq!(&payload[..], b"response");
        }
        _ => panic!("expected first response stream data"),
    }

    frames_tx
        .send(Ok(Frame::PathStatus {
            path_id: PathId(1),
            status: crate::protocol::PathStatus::Active,
            capabilities: Default::default(),
        }))
        .await
        .expect("reattach signal");
    let replay = tokio::time::timeout(
        Duration::from_secs(1),
        recv_tcp_path_command(&mut commands_rx),
    )
    .await
    .expect("replay frame timeout")
    .expect("replay frame");
    match replay {
        TcpPathSessionCommand::SendFrame(Frame::StreamData {
            stream_id: received_stream_id,
            offset,
            payload,
            ..
        }) => {
            assert_eq!(received_stream_id, stream_id);
            assert_eq!(offset, 0);
            assert_eq!(&payload[..], b"response");
        }
        _ => panic!("expected replayed response stream data"),
    }

    relay.abort();
    let _ = relay.await;
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
    let suspect_order = context.ordered_tcp_path_indices(TrafficClass::Interactive, 512);
    assert_eq!(suspect_order, vec![0, 1]);
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

    context.mark_tcp_path_open_success(0, Duration::from_millis(120), TrafficClass::Interactive);
    context.mark_tcp_path_open_success(1, Duration::from_millis(5), TrafficClass::Interactive);

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
fn auto_bulk_discovery_uses_bulk_horizon_for_unmeasured_high_bandwidth_path() {
    let low_latency_path = "tcp://127.0.0.1:10015?srtt-ms=20&rate-mbps=30&low-latency=true"
        .parse::<PathSpec>()
        .expect("low-latency path");
    let high_bandwidth_path = "tcp://127.0.0.1:10016?srtt-ms=180&rate-mbps=300"
        .parse::<PathSpec>()
        .expect("high-bandwidth path");
    let context = ClientPathContext::new(
        vec![low_latency_path, high_bandwidth_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    assert_eq!(
        tcp_auto_bulk_discovery_indices(
            &context,
            Some(0),
            MuxLimits::default().max_tcp_path_inflight_bytes,
        )
        .first()
        .copied(),
        Some(1)
    );
}

#[test]
fn auto_bulk_discovery_skips_unmeasured_expensive_path() {
    let low_latency_path = "tcp://127.0.0.1:10017?srtt-ms=20&rate-mbps=30&low-latency=true"
        .parse::<PathSpec>()
        .expect("low-latency path");
    let expensive_path = "tcp://127.0.0.1:10018?srtt-ms=80&rate-mbps=500&expensive=true"
        .parse::<PathSpec>()
        .expect("expensive path");
    let context = ClientPathContext::new(
        vec![low_latency_path, expensive_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    assert!(
        tcp_auto_bulk_discovery_indices(
            &context,
            Some(0),
            MuxLimits::default().max_tcp_path_inflight_bytes,
        )
        .is_empty()
    );
}

#[test]
fn bulk_repair_does_not_attach_worse_path_when_current_path_is_best() {
    let low_latency_path = "tcp://127.0.0.1:10128?srtt-ms=20&rate-mbps=30"
        .parse::<PathSpec>()
        .expect("low-latency path");
    let poor_path = "tcp://127.0.0.1:10129?srtt-ms=420&jitter-ms=120&rate-mbps=8"
        .parse::<PathSpec>()
        .expect("poor path");
    let context = ClientPathContext::new(
        vec![low_latency_path, poor_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    assert!(
        context
            .ordered_tcp_repair_path_indices(Some(0), TrafficClass::Bulk, 4 * 1024 * 1024)
            .is_empty()
    );
    assert_eq!(
        context.ordered_tcp_repair_path_indices(Some(1), TrafficClass::Bulk, 4 * 1024 * 1024),
        vec![0]
    );
    assert_eq!(
        context
            .ordered_tcp_repair_path_indices(Some(0), TrafficClass::Interactive, 512)
            .first()
            .copied(),
        Some(0)
    );
}

#[test]
fn endpoint_only_tcp_bulk_discovery_waits_for_delivery_evidence_before_probe_noise() {
    let low_latency_path = "tcp://127.0.0.1:10132"
        .parse::<PathSpec>()
        .expect("low latency path");
    let high_bandwidth_path = "tcp://127.0.0.1:10133"
        .parse::<PathSpec>()
        .expect("high bandwidth path");
    let poor_path = "tcp://127.0.0.1:10134"
        .parse::<PathSpec>()
        .expect("poor path");
    let context = ClientPathContext::new(
        vec![low_latency_path, high_bandwidth_path, poor_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.mark_tcp_path_open_success(0, Duration::from_millis(20), TrafficClass::Interactive);
    context.mark_tcp_path_probe_success(2, Duration::from_millis(1));

    assert!(
        tcp_auto_bulk_discovery_indices(
            &context,
            Some(0),
            MuxLimits::default().max_tcp_path_inflight_bytes
        )
        .is_empty()
    );

    let now = Instant::now();
    context.mark_tcp_path_delivery(
        1,
        PathDeliveryStats {
            payload_bytes: 4 * 1024 * 1024,
            first_payload_at: Some(now),
            last_payload_at: Some(now + Duration::from_millis(40)),
        },
    );

    assert_eq!(
        tcp_auto_bulk_discovery_indices(
            &context,
            Some(0),
            MuxLimits::default().max_tcp_path_inflight_bytes,
        ),
        vec![1]
    );
}

#[test]
fn endpoint_only_tcp_bulk_discovery_requires_delivery_under_concurrent_latency_demand() {
    let low_latency_path = "tcp://127.0.0.1:10146"
        .parse::<PathSpec>()
        .expect("low latency path");
    let high_bandwidth_path = "tcp://127.0.0.1:10147"
        .parse::<PathSpec>()
        .expect("high bandwidth path");
    let context = ClientPathContext::new(
        vec![low_latency_path, high_bandwidth_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.mark_tcp_path_open_success(0, Duration::from_millis(20), TrafficClass::Interactive);
    assert!(
        tcp_auto_bulk_discovery_indices(
            &context,
            Some(0),
            MuxLimits::default().max_tcp_path_inflight_bytes,
        )
        .is_empty()
    );

    context.mark_tcp_path_open_success(0, Duration::from_millis(20), TrafficClass::Interactive);
    let now = Instant::now();
    context.mark_tcp_path_delivery(
        1,
        PathDeliveryStats {
            payload_bytes: 4 * 1024 * 1024,
            first_payload_at: Some(now),
            last_payload_at: Some(now + Duration::from_millis(40)),
        },
    );
    assert_eq!(
        tcp_auto_bulk_discovery_indices(
            &context,
            Some(0),
            MuxLimits::default().max_tcp_path_inflight_bytes,
        ),
        vec![1]
    );
}

#[test]
fn endpoint_only_udp_stream_startup_preserves_configured_order_on_probe_noise() {
    let first_path = "udp://127.0.0.1:10135"
        .parse::<PathSpec>()
        .expect("first path");
    let second_path = "udp://127.0.0.1:10136"
        .parse::<PathSpec>()
        .expect("second path");
    let context = ClientPathContext::new(
        vec![first_path, second_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.mark_udp_path_failure(0);
    context.mark_udp_path_probe_success(1, Duration::from_millis(1));

    assert_eq!(
        context.ordered_udp_stream_path_indices(TrafficClass::Interactive, PATH_OPEN_SCORE_BYTES),
        vec![0, 1]
    );

    context.mark_udp_path_failure(0);
    assert_eq!(
        context.ordered_udp_stream_path_indices(TrafficClass::Interactive, PATH_OPEN_SCORE_BYTES),
        vec![1]
    );
}

#[test]
fn endpoint_only_udp_stream_auto_bulk_discovery_waits_for_delivery_evidence() {
    let low_latency_path = "udp://127.0.0.1:10137"
        .parse::<PathSpec>()
        .expect("low latency path");
    let high_bandwidth_path = "udp://127.0.0.1:10138"
        .parse::<PathSpec>()
        .expect("high bandwidth path");
    let poor_path = "udp://127.0.0.1:10139"
        .parse::<PathSpec>()
        .expect("poor path");
    let context = ClientPathContext::new(
        vec![low_latency_path, high_bandwidth_path, poor_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.mark_udp_path_probe_success(1, Duration::from_millis(1));
    assert!(
        context
            .ordered_udp_stream_auto_bulk_discovery_indices(
                Some(0),
                MuxLimits::default().max_tcp_path_inflight_bytes,
            )
            .is_empty()
    );

    let now = Instant::now();
    context.mark_udp_path_delivery(
        1,
        PathDeliveryStats {
            payload_bytes: 4 * 1024 * 1024,
            first_payload_at: Some(now),
            last_payload_at: Some(now + Duration::from_millis(40)),
        },
    );

    assert_eq!(
        context.ordered_udp_stream_auto_bulk_discovery_indices(
            Some(0),
            MuxLimits::default().max_tcp_path_inflight_bytes,
        ),
        vec![1]
    );
}

#[test]
fn mixed_udp_repair_waits_for_delivery_evidence_on_active_tcp_stream() {
    let tcp_path = "tcp://127.0.0.1:10157"
        .parse::<PathSpec>()
        .expect("tcp path");
    let udp_low_latency_path = "udp://127.0.0.1:10158"
        .parse::<PathSpec>()
        .expect("udp low latency path");
    let udp_probe_only_path = "udp://127.0.0.1:10159"
        .parse::<PathSpec>()
        .expect("udp probe path");
    let context = ClientPathContext::new(
        vec![tcp_path, udp_low_latency_path, udp_probe_only_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.mark_udp_path_probe_success(1, Duration::from_millis(1));
    assert!(
        context
            .ordered_udp_stream_repair_path_indices(
                None,
                TrafficClass::Bulk,
                MuxLimits::default().max_tcp_path_inflight_bytes,
                true,
            )
            .is_empty()
    );
    assert_eq!(
        context
            .ordered_udp_stream_repair_path_indices(
                None,
                TrafficClass::Bulk,
                MuxLimits::default().max_tcp_path_inflight_bytes,
                false,
            )
            .first()
            .copied(),
        Some(1)
    );

    let now = Instant::now();
    context.mark_udp_path_delivery(
        1,
        PathDeliveryStats {
            payload_bytes: 4 * 1024 * 1024,
            first_payload_at: Some(now),
            last_payload_at: Some(now + Duration::from_millis(40)),
        },
    );

    assert_eq!(
        context.ordered_udp_stream_repair_path_indices(
            None,
            TrafficClass::Bulk,
            MuxLimits::default().max_tcp_path_inflight_bytes,
            true,
        ),
        vec![1]
    );
}

#[test]
fn udp_repair_waits_for_delivery_evidence_on_active_endpoint_only_stream() {
    let udp_low_latency_path = "udp://127.0.0.1:10160"
        .parse::<PathSpec>()
        .expect("udp low latency path");
    let udp_probe_path = "udp://127.0.0.1:10161"
        .parse::<PathSpec>()
        .expect("udp probe path");
    let context = ClientPathContext::new(
        vec![udp_low_latency_path, udp_probe_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.mark_udp_path_probe_success(1, Duration::from_millis(1));
    assert!(
        context
            .ordered_udp_stream_repair_path_indices(
                Some(0),
                TrafficClass::Bulk,
                MuxLimits::default().max_tcp_path_inflight_bytes,
                true,
            )
            .is_empty()
    );
    assert_eq!(
        context
            .ordered_udp_stream_repair_path_indices(
                Some(0),
                TrafficClass::Bulk,
                MuxLimits::default().max_tcp_path_inflight_bytes,
                false,
            )
            .first()
            .copied(),
        Some(1)
    );

    let now = Instant::now();
    context.mark_udp_path_delivery(
        1,
        PathDeliveryStats {
            payload_bytes: 4 * 1024 * 1024,
            first_payload_at: Some(now),
            last_payload_at: Some(now + Duration::from_millis(40)),
        },
    );

    assert_eq!(
        context.ordered_udp_stream_repair_path_indices(
            Some(0),
            TrafficClass::Bulk,
            MuxLimits::default().max_tcp_path_inflight_bytes,
            true,
        ),
        vec![1]
    );
}

#[test]
fn mixed_auto_bulk_discovery_can_cross_to_better_udp_carrier() {
    let tcp_path = "tcp://127.0.0.1:10140?srtt-ms=20&rate-mbps=30"
        .parse::<PathSpec>()
        .expect("tcp path");
    let udp_path = "udp://127.0.0.1:10141?srtt-ms=40&rate-mbps=300"
        .parse::<PathSpec>()
        .expect("udp path");
    let context = ClientPathContext::new(
        vec![tcp_path, udp_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    assert_eq!(
        context.ordered_reliable_auto_bulk_discovery_path_keys(
            Some(0),
            None,
            MuxLimits::default().max_tcp_path_inflight_bytes,
        ),
        vec![RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        }]
    );
}

#[test]
fn mixed_auto_bulk_discovery_rejects_worse_udp_carrier() {
    let tcp_path = "tcp://127.0.0.1:10140?srtt-ms=20&rate-mbps=300"
        .parse::<PathSpec>()
        .expect("tcp path");
    let udp_path = "udp://127.0.0.1:10141?srtt-ms=180&rate-mbps=30"
        .parse::<PathSpec>()
        .expect("udp path");
    let context = ClientPathContext::new(
        vec![tcp_path, udp_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    assert_eq!(
        context.ordered_reliable_auto_bulk_discovery_path_keys(
            Some(0),
            None,
            MuxLimits::default().max_tcp_path_inflight_bytes,
        ),
        Vec::<RelayPathKey>::new()
    );
}

#[test]
fn mixed_auto_bulk_discovery_can_choose_best_carrier_without_active_cohort() {
    let tcp_path = "tcp://127.0.0.1:10144?srtt-ms=20&rate-mbps=30"
        .parse::<PathSpec>()
        .expect("tcp path");
    let udp_path = "udp://127.0.0.1:10145?srtt-ms=40&rate-mbps=300"
        .parse::<PathSpec>()
        .expect("udp path");
    let context = ClientPathContext::new(
        vec![tcp_path, udp_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    assert_eq!(
        context
            .ordered_reliable_auto_bulk_discovery_path_keys(
                None,
                None,
                MuxLimits::default().max_tcp_path_inflight_bytes,
            )
            .first()
            .copied(),
        Some(RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        })
    );
}

#[test]
fn relay_candidate_filter_preserves_current_carrier_cohort() {
    let tcp = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 1,
    };
    let udp = RelayPathKey {
        underlay: UnderlayProtocol::Udp,
        index: 2,
    };

    assert_eq!(
        relay_path_candidates_for_active_carrier(vec![udp, tcp], Some(UnderlayProtocol::Tcp)),
        vec![tcp]
    );
    assert_eq!(
        relay_path_candidates_for_active_carrier(vec![tcp, udp], Some(UnderlayProtocol::Udp)),
        vec![udp]
    );
    assert_eq!(
        relay_path_candidates_for_active_carrier(vec![tcp, udp], None),
        vec![tcp, udp]
    );
}

#[tokio::test]
async fn mixed_relay_current_carrier_tracks_latest_data_path() {
    fn opened_relay_stream_for_test(
        underlay: UnderlayProtocol,
        path_index: usize,
    ) -> (
        OpenedRemoteStream,
        TcpPathSessionCommandReceivers,
        mpsc::Sender<Result<Frame, RuntimeError>>,
    ) {
        let (commands, command_rx) = tcp_path_session_command_channels(4);
        let (frames_tx, frames_rx) = mpsc::channel(4);
        (
            OpenedRemoteStream {
                path_index,
                stream: TcpPathStream {
                    stream_id: StreamId(44),
                    max_offset: MuxLimits::default().max_stream_window_bytes,
                    class: TrafficClass::Bulk,
                    underlay,
                    max_frame_payload_bytes: tcp_relay_buffer_len(MuxLimits::default()),
                    output: TcpPathStreamOutput::Fixed(commands),
                    frames: frames_rx,
                },
            },
            command_rx,
            frames_tx,
        )
    }

    let (tcp_stream, _tcp_commands, _tcp_frames) =
        opened_relay_stream_for_test(UnderlayProtocol::Tcp, 0);
    let mut remotes = TcpRelayRemoteSet::new(tcp_stream, 4);
    assert_eq!(
        remotes.active_carrier_underlay(),
        Some(UnderlayProtocol::Tcp)
    );

    let (udp_stream, _udp_commands, _udp_frames) =
        opened_relay_stream_for_test(UnderlayProtocol::Udp, 1);
    remotes.attach(udp_stream);
    assert_eq!(
        remotes.active_carrier_underlay(),
        Some(UnderlayProtocol::Udp)
    );

    assert_eq!(
        relay_path_candidates_for_active_carrier(
            vec![
                RelayPathKey {
                    underlay: UnderlayProtocol::Tcp,
                    index: 0,
                },
                RelayPathKey {
                    underlay: UnderlayProtocol::Udp,
                    index: 2,
                },
            ],
            remotes.active_carrier_underlay(),
        ),
        vec![RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 2,
        }]
    );
}

#[tokio::test]
async fn mixed_relay_path_status_active_replays_repair_cache_on_instance() {
    let mux_limits = MuxLimits::default();
    let stream_id = StreamId(45);
    let (commands, mut command_rx) = tcp_path_session_command_channels(4);
    let (_frames_tx, frames_rx) = mpsc::channel(4);
    let opened = OpenedRemoteStream {
        path_index: 1,
        stream: TcpPathStream {
            stream_id,
            max_offset: mux_limits.max_stream_window_bytes,
            class: TrafficClass::Bulk,
            underlay: UnderlayProtocol::Udp,
            max_frame_payload_bytes: tcp_relay_buffer_len(mux_limits),
            output: TcpPathStreamOutput::Fixed(commands),
            frames: frames_rx,
        },
    };
    let mut remotes = TcpRelayRemoteSet::new(opened, 4);
    let instance = remotes.active_path_instance().expect("active path");
    let mut send_stream = ReliableSendStream::new(stream_id, mux_limits);
    send_stream.update_max_offset(mux_limits.max_stream_window_bytes);
    send_stream
        .send_data(Bytes::from_static(b"repair"), StreamFlags::NONE)
        .expect("repair data");

    assert!(
        remotes
            .replay_repair_cache_to_instance(instance, &send_stream, false)
            .await
            .expect("replay")
    );

    match recv_tcp_path_command(&mut command_rx)
        .await
        .expect("replay command")
    {
        TcpPathSessionCommand::SendFrame(Frame::StreamData {
            stream_id: received_stream_id,
            offset,
            payload,
            ..
        }) => {
            assert_eq!(received_stream_id, stream_id);
            assert_eq!(offset, 0);
            assert_eq!(&payload[..], b"repair");
        }
        _ => panic!("expected replayed repair data"),
    }
}

#[test]
fn mixed_auto_bulk_discovery_does_not_attach_unmeasured_endpoint_only_udp() {
    let tcp_path = "tcp://127.0.0.1:10142"
        .parse::<PathSpec>()
        .expect("tcp path");
    let udp_path = "udp://127.0.0.1:10143"
        .parse::<PathSpec>()
        .expect("udp path");
    let context = ClientPathContext::new(
        vec![tcp_path, udp_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.mark_udp_path_probe_success(0, Duration::from_millis(1));

    assert!(
        context
            .ordered_reliable_auto_bulk_discovery_path_keys(
                Some(0),
                None,
                MuxLimits::default().max_tcp_path_inflight_bytes,
            )
            .is_empty()
    );
}

#[test]
fn measured_udp_delivery_rate_updates_next_datagram_order() {
    let hinted_slow_path = "udp://127.0.0.1:10019?srtt-ms=20&rate-mbps=10"
        .parse::<PathSpec>()
        .expect("hinted slow path");
    let hinted_fast_path = "udp://127.0.0.1:10020?srtt-ms=20&rate-mbps=100"
        .parse::<PathSpec>()
        .expect("hinted fast path");
    let context = ClientPathContext::new(
        vec![hinted_slow_path, hinted_fast_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    assert_eq!(
        udp_candidate_indices(&context, 1024 * 1024, DEFAULT_SOCKS5_UDP_TTL_MS)
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
        udp_candidate_indices(&context, 1024 * 1024, DEFAULT_SOCKS5_UDP_TTL_MS)
            .first()
            .copied(),
        Some(0)
    );
}

#[test]
fn udp_datagram_feedback_updates_scheduler_health() {
    let stale_path = "udp://127.0.0.1:10021?srtt-ms=250&rate-mbps=1"
        .parse::<PathSpec>()
        .expect("stale path");
    let observed_path = "udp://127.0.0.1:10022?srtt-ms=250&rate-mbps=1"
        .parse::<PathSpec>()
        .expect("observed path");
    let context = ClientPathContext::new(
        vec![stale_path, observed_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.mark_udp_path_feedback(
        1,
        UdpDatagramPathObservation {
            rtt: Duration::from_millis(8),
            jitter: Duration::from_millis(1),
            loss_rate: 0.02,
            rate_sample: PathRateSample::new(1024 * 1024, Duration::from_millis(20)),
        },
    );

    assert_eq!(
        udp_candidate_indices(&context, 4096, DEFAULT_SOCKS5_UDP_TTL_MS)
            .first()
            .copied(),
        Some(1)
    );
    let health = context.health.lock().expect("health lock");
    assert_eq!(health.udp[1].state, SchedulerPathState::Active);
    assert!(health.udp[1].measured_srtt_ms.is_some());
    assert!(health.udp[1].measured_jitter_ms.is_some());
    assert!(health.udp[1].measured_rate_bps.is_some());
    assert_eq!(health.udp[1].measured_loss_rate, Some(0.02));
}

#[test]
fn realtime_udp_datagram_feedback_beats_probe_only_paths() {
    let feedback_path = "udp://127.0.0.1:10144"
        .parse::<PathSpec>()
        .expect("feedback path");
    let probe_only_path = "udp://127.0.0.1:10145"
        .parse::<PathSpec>()
        .expect("probe-only path");
    let context = ClientPathContext::new(
        vec![feedback_path, probe_only_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.mark_udp_path_feedback(
        0,
        UdpDatagramPathObservation {
            rtt: Duration::from_millis(40),
            jitter: Duration::from_millis(4),
            loss_rate: 0.0,
            rate_sample: PathRateSample::new(1024 * 1024, Duration::from_millis(10)),
        },
    );
    context.mark_udp_path_probe_success(1, Duration::from_millis(1));
    context.mark_udp_path_feedback(
        1,
        UdpDatagramPathObservation {
            rtt: Duration::from_millis(20),
            jitter: Duration::from_millis(2),
            loss_rate: 0.0,
            rate_sample: PathRateSample::new(1024 * 1024, Duration::from_millis(10)),
        },
    );

    let association = UdpDatagramClientAssociation::new(context.clone()).expect("assoc");
    let candidates = context.ordered_udp_path_candidates_for_ttl(512, DEFAULT_SOCKS5_UDP_TTL_MS);
    assert_eq!(
        association.select_path_candidate(
            &candidates,
            &HashSet::new(),
            512,
            DEFAULT_SOCKS5_UDP_TTL_MS,
        ),
        Some(0)
    );
    assert_eq!(
        association.select_path_candidate(
            &candidates,
            &HashSet::from([0]),
            512,
            DEFAULT_SOCKS5_UDP_TTL_MS,
        ),
        Some(1)
    );
}

#[test]
fn udp_freshness_filter_rejects_paths_that_cannot_fit_ttl() {
    let high_latency_path = "udp://127.0.0.1:10023?srtt-ms=1000&rate-mbps=1"
        .parse::<PathSpec>()
        .expect("high latency path");
    let context = ClientPathContext::new(
        vec![high_latency_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    assert!(udp_candidate_indices(&context, 1024, 10).is_empty());
}

#[test]
fn realtime_udp_prefers_measured_model_before_unmeasured_startup_paths() {
    let first_path = "udp://127.0.0.1:10024"
        .parse::<PathSpec>()
        .expect("first path");
    let second_path = "udp://127.0.0.1:10025"
        .parse::<PathSpec>()
        .expect("second path");
    let context = ClientPathContext::new(
        vec![first_path, second_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    assert_eq!(
        udp_candidate_indices(&context, 512, DEFAULT_SOCKS5_UDP_TTL_MS),
        vec![0]
    );

    context.mark_udp_path_probe_success(0, Duration::from_millis(20));

    assert_eq!(
        udp_candidate_indices(&context, 512, DEFAULT_SOCKS5_UDP_TTL_MS),
        vec![0]
    );
}

#[test]
fn udp_association_suppression_prefers_survivor_without_dead_ending() {
    let blackhole_path = "udp://127.0.0.1:10026?srtt-ms=5&rate-mbps=100"
        .parse::<PathSpec>()
        .expect("blackhole path");
    let survivor_path = "udp://127.0.0.1:10027?srtt-ms=20&rate-mbps=100"
        .parse::<PathSpec>()
        .expect("survivor path");
    let context = ClientPathContext::new(
        vec![blackhole_path, survivor_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("ctx");
    let mut association = UdpDatagramClientAssociation::new(context).expect("assoc");
    let candidates = [
        UdpPathCandidate {
            path_index: 0,
            eta_ms: 5.0,
        },
        UdpPathCandidate {
            path_index: 1,
            eta_ms: 20.0,
        },
    ];

    assert_eq!(
        association.select_path_candidate(&candidates, &HashSet::new(), 512, 1000),
        Some(0)
    );

    association.suppress_path_after_timeout(0, Duration::from_millis(250), 1000);
    assert_eq!(
        association.select_path_candidate(&candidates, &HashSet::new(), 512, 1000),
        Some(1)
    );

    association.suppress_path_after_timeout(1, Duration::from_millis(250), 1000);
    assert_eq!(
        association.select_path_candidate(&candidates, &HashSet::new(), 512, 1000),
        Some(0)
    );
}

#[test]
fn udp_association_sticks_to_successful_path_until_suppressed() {
    let steady_path = "udp://127.0.0.1:10031?srtt-ms=20&rate-mbps=100"
        .parse::<PathSpec>()
        .expect("steady path");
    let lower_eta_path = "udp://127.0.0.1:10032?srtt-ms=5&rate-mbps=100"
        .parse::<PathSpec>()
        .expect("lower eta path");
    let context = ClientPathContext::new(
        vec![steady_path, lower_eta_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("ctx");
    let mut association = UdpDatagramClientAssociation::new(context).expect("assoc");
    let candidates = [
        UdpPathCandidate {
            path_index: 1,
            eta_ms: 5.0,
        },
        UdpPathCandidate {
            path_index: 0,
            eta_ms: 20.0,
        },
    ];

    association.last_successful_path = Some(0);
    assert_eq!(
        association.select_path_candidate(&candidates, &HashSet::new(), 512, 1000),
        Some(0)
    );

    association.suppress_path_after_timeout(0, Duration::from_millis(250), 1000);
    assert_eq!(
        association.select_path_candidate(&candidates, &HashSet::new(), 512, 1000),
        Some(1)
    );
}

#[test]
fn udp_acked_timeout_migration_requires_validated_alternative() {
    let proven_path = "udp://127.0.0.1:10033"
        .parse::<PathSpec>()
        .expect("proven path");
    let endpoint_only_alternative = "udp://127.0.0.1:10034"
        .parse::<PathSpec>()
        .expect("endpoint-only alternative");
    let hinted_alternative = "udp://127.0.0.1:10035?srtt-ms=80&rate-mbps=30"
        .parse::<PathSpec>()
        .expect("hinted alternative");
    let context = ClientPathContext::new(
        vec![proven_path, endpoint_only_alternative, hinted_alternative],
        security(),
        ResourceLimits::default(),
    )
    .expect("ctx");
    context.mark_udp_path_feedback(
        0,
        UdpDatagramPathObservation {
            rtt: Duration::from_millis(40),
            jitter: Duration::from_millis(4),
            loss_rate: 0.0,
            rate_sample: None,
        },
    );
    let association = UdpDatagramClientAssociation::new(context).expect("assoc");
    let attempted = HashSet::from([0]);

    assert!(!association.has_validated_udp_retry_alternative(
        &[
            UdpPathCandidate {
                path_index: 0,
                eta_ms: 40.0,
            },
            UdpPathCandidate {
                path_index: 1,
                eta_ms: 80.0,
            },
        ],
        &attempted,
        0,
    ));
    assert!(association.has_validated_udp_retry_alternative(
        &[
            UdpPathCandidate {
                path_index: 0,
                eta_ms: 40.0,
            },
            UdpPathCandidate {
                path_index: 2,
                eta_ms: 80.0,
            },
        ],
        &attempted,
        0,
    ));
}

#[test]
fn udp_path_open_timeout_uses_adaptive_multipath_startup_budget() {
    let mut model = UdpPathRuntimeModel {
        pacing_rate_bps: UDP_MIN_PACING_RATE_BPS,
        response_timeout: Duration::from_millis(300),
        mtu_payload_bytes: UDP_DEFAULT_MTU_PAYLOAD_BYTES,
        mtu_is_measured: false,
        mtu_probe_ceiling_payload_bytes: UDP_MAX_MTU_PAYLOAD_BYTES,
    };

    assert_eq!(
        udp_datagram_path_open_timeout(false, false, model, DEFAULT_SOCKS5_UDP_TTL_MS),
        UDP_PATH_HANDSHAKE_TIMEOUT
    );
    assert_eq!(
        udp_datagram_path_open_timeout(true, true, model, DEFAULT_SOCKS5_UDP_TTL_MS),
        Duration::from_millis(300)
    );
    assert_eq!(
        udp_datagram_path_open_timeout(false, true, model, DEFAULT_SOCKS5_UDP_TTL_MS),
        UDP_PATH_HANDSHAKE_TIMEOUT
    );

    model.response_timeout = Duration::from_millis(1);
    assert_eq!(
        udp_datagram_path_open_timeout(true, true, model, DEFAULT_SOCKS5_UDP_TTL_MS),
        UDP_MIN_RESPONSE_TIMEOUT
    );
    assert_eq!(
        udp_datagram_path_open_timeout(false, true, model, DEFAULT_SOCKS5_UDP_TTL_MS),
        UDP_MIN_RESPONSE_TIMEOUT
    );

    model.response_timeout = Duration::from_millis(65);
    assert_eq!(
        udp_datagram_path_open_timeout(false, true, model, DEFAULT_SOCKS5_UDP_TTL_MS),
        Duration::from_millis(520)
    );

    model.response_timeout = UDP_PATH_HANDSHAKE_TIMEOUT + Duration::from_secs(1);
    assert_eq!(
        udp_datagram_path_open_timeout(true, true, model, DEFAULT_SOCKS5_UDP_TTL_MS),
        UDP_PATH_HANDSHAKE_TIMEOUT
    );
    assert_eq!(
        udp_datagram_path_open_timeout(false, false, model, 250),
        Duration::from_millis(250)
    );
}

#[test]
fn udp_runtime_model_backs_off_response_timeout_after_loss() {
    let stable = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 80.0, 30_000_000.0);
    let mut lossy = stable;
    lossy.loss_rate = 0.5;

    let stable_model = UdpPathRuntimeModel::from_snapshot(
        stable,
        DEFAULT_SOCKS5_UDP_TTL_MS,
        UDP_DEFAULT_MTU_PAYLOAD_BYTES,
        true,
        UDP_MAX_MTU_PAYLOAD_BYTES,
    );
    let lossy_model = UdpPathRuntimeModel::from_snapshot(
        lossy,
        DEFAULT_SOCKS5_UDP_TTL_MS,
        UDP_DEFAULT_MTU_PAYLOAD_BYTES,
        true,
        UDP_MAX_MTU_PAYLOAD_BYTES,
    );

    assert!(lossy_model.response_timeout > stable_model.response_timeout);
    assert!(lossy_model.response_timeout <= UDP_MAX_RESPONSE_TIMEOUT);
    assert!(lossy_model.pacing_rate_bps < stable_model.pacing_rate_bps);
}

#[test]
fn udp_association_retry_budget_tracks_live_loss_model() {
    let path = "udp://127.0.0.1:10036?srtt-ms=80&rate-mbps=30"
        .parse::<PathSpec>()
        .expect("path");
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("ctx");
    let association = UdpDatagramClientAssociation::new(context.clone()).expect("assoc");
    let stable_budget = association.adaptive_retry_budget(512, DEFAULT_SOCKS5_UDP_TTL_MS);
    assert!(stable_budget >= UDP_MIN_RETRY_BUDGET);
    assert!(stable_budget <= UDP_MAX_RETRY_BUDGET);

    context.mark_udp_path_feedback(
        0,
        UdpDatagramPathObservation {
            rtt: Duration::from_millis(120),
            jitter: Duration::from_millis(0),
            loss_rate: 1.0,
            rate_sample: None,
        },
    );

    let lossy_budget = association.adaptive_retry_budget(512, DEFAULT_SOCKS5_UDP_TTL_MS);
    assert!(lossy_budget > stable_budget);
    assert!(lossy_budget <= UDP_MAX_RETRY_BUDGET);
}

#[test]
fn udp_edge_lane_limit_scales_with_realtime_response_model() {
    let low_latency = "udp://127.0.0.1:10184?srtt-ms=20&jitter-ms=0&rate-mbps=30"
        .parse::<PathSpec>()
        .expect("low-latency path");
    let high_rtt = "udp://127.0.0.1:10185?srtt-ms=180&jitter-ms=20&rate-mbps=300"
        .parse::<PathSpec>()
        .expect("high-rtt path");
    let low_context =
        ClientPathContext::new(vec![low_latency], security(), ResourceLimits::default())
            .expect("low context");
    let high_context = ClientPathContext::new(
        vec![high_rtt.clone()],
        security(),
        ResourceLimits::default(),
    )
    .expect("high context");

    assert_eq!(udp_edge_lane_limit(&low_context), 2);
    assert!(udp_edge_lane_limit(&high_context) > udp_edge_lane_limit(&low_context));

    let capped_resources = ResourceLimits {
        max_datagram_queue_bytes: ResourceLimits::default().max_payload_bytes * 3,
        ..ResourceLimits::default()
    };
    let capped_context = ClientPathContext::new(vec![high_rtt], security(), capped_resources)
        .expect("capped context");
    assert_eq!(udp_edge_lane_limit(&capped_context), 3);
}

#[test]
fn udp_edge_lane_startup_ramps_after_success_feedback() {
    let paths = vec![
        "udp://127.0.0.1:10180".parse().expect("first path"),
        "udp://127.0.0.1:10181".parse().expect("second path"),
        "udp://127.0.0.1:10182".parse().expect("third path"),
    ];
    let context =
        ClientPathContext::new(paths, security(), ResourceLimits::default()).expect("context");

    assert!(udp_edge_lane_limit(&context) > udp_edge_startup_lane_limit(&context));
    assert_eq!(udp_edge_startup_lane_limit(&context), 2);
    assert!(udp_edge_lane_spawn_allowed(0, 0, &context));
    assert!(udp_edge_lane_spawn_allowed(1, 0, &context));
    assert!(!udp_edge_lane_spawn_allowed(2, 0, &context));
    assert!(udp_edge_lane_spawn_allowed(2, 1, &context));
}

#[test]
fn udp_edge_lane_startup_respects_queue_capacity() {
    let path = "udp://127.0.0.1:10183".parse().expect("path");
    let resources = ResourceLimits {
        max_datagram_queue_bytes: ResourceLimits::default().max_payload_bytes,
        ..ResourceLimits::default()
    };
    let context = ClientPathContext::new(vec![path], security(), resources).expect("context");

    assert_eq!(udp_edge_queue_slots(&context), 1);
    assert_eq!(udp_edge_startup_lane_limit(&context), 1);
    assert!(udp_edge_lane_spawn_allowed(0, 0, &context));
    assert!(!udp_edge_lane_spawn_allowed(1, 0, &context));
    assert!(udp_edge_lane_spawn_allowed(1, 1, &context));
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

    context.mark_tcp_path_open_success(0, Duration::from_millis(1), TrafficClass::Interactive);
    assert_eq!(
        context
            .ordered_tcp_path_indices(TrafficClass::Interactive, 512)
            .first()
            .copied(),
        Some(1)
    );

    context.release_tcp_path_load(0, TrafficClass::Interactive);
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
fn active_interactive_tcp_flow_pushes_bulk_to_other_path() {
    let low_latency_path = "tcp://127.0.0.1:10123?srtt-ms=20&rate-mbps=100"
        .parse::<PathSpec>()
        .expect("low latency path");
    let bulk_candidate_path = "tcp://127.0.0.1:10124?srtt-ms=180&rate-mbps=100"
        .parse::<PathSpec>()
        .expect("bulk candidate path");
    let context = ClientPathContext::new(
        vec![low_latency_path, bulk_candidate_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.mark_tcp_path_open_success(0, Duration::from_millis(20), TrafficClass::Interactive);
    context.mark_tcp_path_delivery(
        1,
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
        Some(1)
    );
    assert_eq!(
        context
            .ordered_tcp_path_indices(TrafficClass::Interactive, PATH_OPEN_SCORE_BYTES)
            .first()
            .copied(),
        Some(0)
    );
}

#[test]
fn endpoint_only_tcp_startup_preserves_configured_order_on_equal_scores() {
    let first_path = "tcp://127.0.0.1:10121"
        .parse::<PathSpec>()
        .expect("first path");
    let second_path = "tcp://127.0.0.1:10122"
        .parse::<PathSpec>()
        .expect("second path");
    let context = ClientPathContext::new(
        vec![first_path, second_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    assert_eq!(
        context
            .ordered_tcp_path_indices(TrafficClass::Interactive, PATH_OPEN_SCORE_BYTES)
            .first()
            .copied(),
        Some(0)
    );
}

#[test]
fn endpoint_only_tcp_startup_validates_order_before_noisy_probe_scores() {
    let first_path = "tcp://127.0.0.1:10125"
        .parse::<PathSpec>()
        .expect("first path");
    let second_path = "tcp://127.0.0.1:10126"
        .parse::<PathSpec>()
        .expect("second path");
    let context = ClientPathContext::new(
        vec![first_path, second_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.mark_tcp_path_failure(0);
    context.mark_tcp_path_probe_success(1, Duration::from_millis(1));

    assert_eq!(
        context.ordered_tcp_path_indices(TrafficClass::Interactive, PATH_OPEN_SCORE_BYTES),
        vec![0, 1]
    );

    context.mark_tcp_path_failure(0);
    assert_eq!(
        context.ordered_tcp_path_indices(TrafficClass::Interactive, PATH_OPEN_SCORE_BYTES),
        vec![1]
    );
}

#[test]
fn endpoint_only_tcp_interactive_opens_stay_latency_first_under_active_flow() {
    let low_latency_path = "tcp://127.0.0.1:10129"
        .parse::<PathSpec>()
        .expect("low latency path");
    let high_latency_path = "tcp://127.0.0.1:10130"
        .parse::<PathSpec>()
        .expect("high latency path");
    let poor_path = "tcp://127.0.0.1:10131"
        .parse::<PathSpec>()
        .expect("poor path");
    let context = ClientPathContext::new(
        vec![low_latency_path, high_latency_path, poor_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.mark_tcp_path_open_success(0, Duration::from_millis(20), TrafficClass::Interactive);
    context.mark_tcp_path_probe_success(2, Duration::from_millis(1));

    assert_eq!(
        context.ordered_tcp_path_indices(TrafficClass::Interactive, PATH_OPEN_SCORE_BYTES),
        vec![0, 1, 2]
    );
}

#[test]
fn hinted_tcp_startup_uses_configured_metrics_before_order() {
    let high_latency_path = "tcp://127.0.0.1:10127?srtt-ms=200&rate-mbps=100"
        .parse::<PathSpec>()
        .expect("high latency path");
    let low_latency_path = "tcp://127.0.0.1:10128?srtt-ms=10&rate-mbps=100"
        .parse::<PathSpec>()
        .expect("low latency path");
    let context = ClientPathContext::new(
        vec![high_latency_path, low_latency_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    assert_eq!(
        context
            .ordered_tcp_path_indices(TrafficClass::Interactive, PATH_OPEN_SCORE_BYTES)
            .first()
            .copied(),
        Some(1)
    );
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

    context.mark_udp_path_probe_success(1, Duration::from_millis(1));
    context.mark_udp_path_open_success(0, Duration::from_millis(1));
    assert_eq!(
        udp_candidate_indices(&context, 512, DEFAULT_SOCKS5_UDP_TTL_MS)
            .first()
            .copied(),
        Some(1)
    );

    context.release_udp_path_load(0);
    assert_eq!(
        udp_candidate_indices(&context, 512, DEFAULT_SOCKS5_UDP_TTL_MS)
            .first()
            .copied(),
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
async fn repeated_path_probe_failure_suppresses_unreachable_tcp_path() {
    let path = reserve_tcp_path().await;
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("ctx");

    probe_client_paths(&context, Duration::from_millis(50)).await;

    {
        let health = context.health.lock().expect("health lock");
        assert_eq!(health.tcp[0].state, SchedulerPathState::Suspect);
        assert_eq!(health.tcp[0].consecutive_failures, 1);
        assert!(health.tcp[0].failed_until.is_none());
    }
    assert_eq!(
        context
            .ordered_tcp_path_indices(TrafficClass::Interactive, 512)
            .first()
            .copied(),
        Some(0)
    );

    probe_client_paths(&context, Duration::from_millis(50)).await;

    {
        let health = context.health.lock().expect("health lock");
        assert_eq!(health.tcp[0].state, SchedulerPathState::Failed);
        assert_eq!(health.tcp[0].consecutive_failures, 2);
        assert!(health.tcp[0].failed_until.is_some());
    }
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

    handler.await.expect("join").expect("handler");
    server_path
        .await
        .expect("server join")
        .expect("server path");
    target.await.expect("target join");
}

#[tokio::test]
async fn socks5_ingress_relays_tcp_payload_over_encrypted_udp_stream_path() {
    let (target_addr, target) = spawn_echo_target().await;
    let path = reserve_udp_path().await;
    let socket = udp::bind_socket(&path).await.expect("bind udp path");
    let server_path = tokio::spawn(handle_server_udp_datagram_path_session(
        socket,
        server_context(OutboundConfig::Direct),
    ));
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
async fn tcp_path_sessions_handle_multiple_dedicated_interactive_streams() {
    let (target_addr, target) = spawn_echo_target_count(2).await;
    let (path, server_path) = spawn_server_path_count(OutboundConfig::Direct, 2).await;
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("ctx");
    let (mut first_client, first_server) = duplex(4096);
    let (mut second_client, second_server) = duplex(4096);
    let first_handler = tokio::spawn(handle_socks5_client_stream(first_server, context.clone()));
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
async fn auto_bulk_tcp_stream_attaches_measured_path_for_large_response() {
    let payload = vec![0x5au8; 2 * 1024 * 1024];
    let expected_payload = payload.clone();
    let target_listener = TcpListener::bind("127.0.0.1:0").await.expect("target bind");
    let target_addr = target_listener.local_addr().expect("target addr");
    let target = tokio::spawn(async move {
        let (mut stream, _) = target_listener.accept().await.expect("target accept");
        let mut request = [0u8; 4];
        stream
            .read_exact(&mut request)
            .await
            .expect("target request");
        assert_eq!(&request, b"ping");
        stream.write_all(&payload).await.expect("target response");
        stream.shutdown().await.expect("target shutdown");
    });

    let low_latency_path =
        reserve_tcp_path_with_query("srtt-ms=10&rate-mbps=20&low-latency=true").await;
    let high_bandwidth_path = reserve_tcp_path_with_query("srtt-ms=120&rate-mbps=300").await;
    let low_latency_listener = bind_listener(&low_latency_path)
        .await
        .expect("low-latency bind");
    let high_bandwidth_listener = bind_listener(&high_bandwidth_path)
        .await
        .expect("high-bandwidth bind");
    let server_context = server_context(OutboundConfig::Direct);
    let (accepted_tx, mut accepted_rx) = mpsc::channel(2);
    let low_latency_context = server_context.clone();
    let low_latency_accepted_tx = accepted_tx.clone();
    let low_latency_server = tokio::spawn(async move {
        let (stream, _) = low_latency_listener
            .accept()
            .await
            .expect("low-latency accept");
        low_latency_accepted_tx
            .send(0usize)
            .await
            .expect("accepted low latency");
        handle_server_path(stream, low_latency_context).await
    });
    let high_bandwidth_context = server_context.clone();
    let high_bandwidth_server = tokio::spawn(async move {
        let (stream, _) = high_bandwidth_listener
            .accept()
            .await
            .expect("high-bandwidth accept");
        accepted_tx
            .send(1usize)
            .await
            .expect("accepted high bandwidth");
        handle_server_path(stream, high_bandwidth_context).await
    });

    let context = ClientPathContext::new(
        vec![low_latency_path, high_bandwidth_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("ctx");
    context.mark_tcp_path_delivery(
        1,
        PathDeliveryStats {
            payload_bytes: 4 * 1024 * 1024,
            first_payload_at: Some(Instant::now()),
            last_payload_at: Some(Instant::now() + Duration::from_millis(100)),
        },
    );
    let health_context = context.clone();
    let ingress_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("ingress bind");
    let ingress_addr = ingress_listener.local_addr().expect("ingress addr");
    let handler = tokio::spawn(async move {
        let (server, _) = ingress_listener.accept().await.expect("ingress accept");
        handle_socks5_client_stream(server, context).await
    });
    let mut client = TcpStream::connect(ingress_addr)
        .await
        .expect("ingress client");

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
    tokio::time::timeout(Duration::from_secs(2), client.read_exact(&mut response))
        .await
        .expect("reply timeout")
        .expect("reply");
    assert_eq!(response[1], Socks5Reply::Succeeded as u8);

    client.write_all(b"ping").await.expect("payload write");
    client.shutdown().await.expect("client shutdown");
    let mut received = vec![0u8; expected_payload.len()];
    tokio::time::timeout(Duration::from_secs(5), client.read_exact(&mut received))
        .await
        .expect("response timeout")
        .expect("payload read");
    assert_eq!(received, expected_payload);

    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), accepted_rx.recv())
            .await
            .expect("first accept timeout"),
        Some(0)
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), accepted_rx.recv())
            .await
            .expect("second accept timeout"),
        Some(1)
    );

    handler.await.expect("handler join").expect("handler");
    {
        let health = health_context.health.lock().expect("health lock");
        assert_eq!(health.tcp[0].active_flows, 0);
        assert_eq!(health.tcp[1].active_flows, 0);
    }
    drop(health_context);
    low_latency_server
        .await
        .expect("low-latency server join")
        .expect("low-latency server");
    high_bandwidth_server
        .await
        .expect("high-bandwidth server join")
        .expect("high-bandwidth server");
    target.await.expect("target join");
}

#[tokio::test]
async fn tcp_stream_migrates_to_survivor_path_after_active_path_failure() {
    let target_listener = TcpListener::bind("127.0.0.1:0").await.expect("target bind");
    let target_addr = target_listener.local_addr().expect("target addr");
    let (first_payload_tx, first_payload_rx) = oneshot::channel();
    let target = tokio::spawn(async move {
        let (mut stream, _) = target_listener.accept().await.expect("target accept");
        let mut first = [0u8; 4];
        stream
            .read_exact(&mut first)
            .await
            .expect("target first read");
        assert_eq!(&first, b"ping");
        let _ = first_payload_tx.send(());
        let mut second = [0u8; 4];
        stream
            .read_exact(&mut second)
            .await
            .expect("target second read");
        assert_eq!(&second, b"pong");
        stream.write_all(b"done").await.expect("target write");
        stream.shutdown().await.expect("target shutdown");
    });

    let first_path = reserve_tcp_path().await;
    let second_path = reserve_tcp_path().await;
    let first_listener = bind_listener(&first_path).await.expect("first bind");
    let second_listener = bind_listener(&second_path).await.expect("second bind");
    let server_context = server_context(OutboundConfig::Direct);
    let first_server_context = server_context.clone();
    let first_server = tokio::spawn(async move {
        let (stream, _) = first_listener.accept().await.expect("first accept");
        handle_server_path(stream, first_server_context).await
    });
    let second_server_context = server_context.clone();
    let second_server = tokio::spawn(async move {
        let (stream, _) = second_listener.accept().await.expect("second accept");
        handle_server_path(stream, second_server_context).await
    });

    let resources = ResourceLimits {
        tcp_path_heartbeat_interval: Duration::from_secs(60),
        tcp_path_heartbeat_timeout: Duration::from_secs(60),
        ..ResourceLimits::default()
    };
    let context =
        ClientPathContext::new(vec![first_path, second_path], security(), resources).expect("ctx");
    let health_context = context.clone();
    let ingress_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("ingress bind");
    let ingress_addr = ingress_listener.local_addr().expect("ingress addr");
    let handler = tokio::spawn(async move {
        let (server, _) = ingress_listener.accept().await.expect("ingress accept");
        handle_socks5_client_stream(server, context.clone()).await
    });
    let mut client = TcpStream::connect(ingress_addr)
        .await
        .expect("ingress client");

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

    client.write_all(b"ping").await.expect("first payload");
    first_payload_rx.await.expect("first payload observed");
    first_server.abort();
    let _ = first_server.await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    client.write_all(b"pong").await.expect("second payload");

    let mut payload = [0u8; 4];
    client.read_exact(&mut payload).await.expect("payload read");
    assert_eq!(&payload, b"done");
    client.shutdown().await.expect("client shutdown");
    handler.await.expect("handler join").expect("handler");
    {
        let health = health_context.health.lock().expect("health lock");
        assert!(matches!(
            health.tcp[0].state,
            SchedulerPathState::Suspect | SchedulerPathState::Failed
        ));
        assert_eq!(health.tcp[0].active_flows, 0);
        assert_eq!(health.tcp[1].active_flows, 0);
    }
    drop(health_context);
    second_server
        .await
        .expect("second server join")
        .expect("second server");
    target.await.expect("target join");
}

#[tokio::test]
async fn tcp_relay_active_stream_heartbeat_timeout_does_not_abort_stream() {
    let (path, server_path) = spawn_tcp_relay_heartbeat_blackhole(Duration::from_millis(500)).await;
    let resources = ResourceLimits {
        tcp_path_heartbeat_interval: Duration::from_millis(10),
        tcp_path_heartbeat_timeout: Duration::from_millis(30),
        ..ResourceLimits::default()
    };
    let context = ClientPathContext::new(vec![path], security(), resources).expect("ctx");
    let (mut client, server) = duplex(4096);
    let mut handler = tokio::spawn(handle_socks5_client_stream(server, context));

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

    tokio::select! {
        result = &mut handler => {
            panic!("active stream should not be aborted by heartbeat timeout: {result:?}");
        }
        _ = tokio::time::sleep(Duration::from_millis(150)) => {}
    }

    handler.abort();
    let _ = handler.await;
    server_path
        .await
        .expect("server join")
        .expect("heartbeat test server");
}

#[test]
fn tcp_path_activity_extends_pending_heartbeat_deadline() {
    let before = tokio::time::Instant::now();
    let mut next_heartbeat_at = before;
    let old_deadline = before + Duration::from_millis(1);
    let mut pending = Some((42, old_deadline));

    refresh_client_tcp_path_liveness_state(
        &mut next_heartbeat_at,
        Duration::from_secs(10),
        &mut pending,
        Duration::from_secs(30),
    );

    assert!(next_heartbeat_at >= before + Duration::from_secs(10));
    let Some((nonce, deadline)) = pending else {
        panic!("heartbeat should remain pending");
    };
    assert_eq!(nonce, 42);
    assert!(deadline >= before + Duration::from_secs(30));
    assert!(deadline > old_deadline);
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
async fn socks5_ingress_starts_tcp_auto_latency_first() {
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
    let context = ClientPathContext::new(
        vec![no_bulk_low_latency_path, bulk_allowed_path],
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

    assert_eq!(accepted_rx.recv().await, Some(0));
    handler.await.expect("join").expect("handler");
    low_latency_server
        .await
        .expect("low latency server join")
        .expect("low latency server");
    bulk_allowed_server.abort();
    let _ = bulk_allowed_server.await;
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
async fn http_connect_ingress_relays_tcp_payload_over_encrypted_udp_stream_path() {
    let (target_addr, target) = spawn_echo_target().await;
    let path = reserve_udp_path().await;
    let socket = udp::bind_socket(&path).await.expect("bind udp path");
    let server_path = tokio::spawn(handle_server_udp_datagram_path_session(
        socket,
        server_context(OutboundConfig::Direct),
    ));
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
        DnsConfig::default(),
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
        DnsConfig::default(),
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
    let health_context = context.clone();
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
    let request = socks5::udp_datagram(&TargetAddr::Ip(target_addr), b"ping").expect("udp request");
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
        let (datagram, consumed) = socks5::parse_udp_datagram(&response[..len]).expect("datagram");
        assert_eq!(consumed, len);
        assert_eq!(datagram.target, TargetAddr::Ip(target_addr));
        assert_eq!(datagram.payload, Bytes::from_static(b"pong"));
    }
    control_client.shutdown().await.expect("control shutdown");

    handler.await.expect("handler join").expect("handler");
    {
        let health = health_context.health.lock().expect("health lock");
        assert_eq!(health.udp[0].state, SchedulerPathState::Active);
        assert!(health.udp[0].measured_srtt_ms.is_some());
        assert!(health.udp[0].measured_jitter_ms.is_some());
        assert_eq!(health.udp[0].measured_loss_rate, Some(0.0));
    }
    server.await.expect("server join").expect("server");
    target.await.expect("target join");
}

#[tokio::test]
async fn socks5_udp_associate_does_not_block_fast_datagram_behind_slow_response() {
    let (target_addr, target) = spawn_udp_reordered_echo_target().await;
    let path = reserve_udp_path().await;
    let server = tokio::spawn(run_server(
        vec![path.clone()],
        OutboundConfig::Direct,
        DnsConfig::default(),
        security(),
        ResourceLimits::default(),
    ));
    tokio::time::sleep(Duration::from_millis(10)).await;
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
    let slow = socks5::udp_datagram(&TargetAddr::Ip(target_addr), b"slow").expect("slow request");
    let fast = socks5::udp_datagram(&TargetAddr::Ip(target_addr), b"fast").expect("fast request");
    udp_client
        .send_to(&slow, relay_addr)
        .await
        .expect("send slow request");
    tokio::time::sleep(Duration::from_millis(50)).await;
    udp_client
        .send_to(&fast, relay_addr)
        .await
        .expect("send fast request");

    let mut response = [0u8; 128];
    let (len, _) = tokio::time::timeout(
        Duration::from_millis(400),
        udp_client.recv_from(&mut response),
    )
    .await
    .expect("fast response should not wait for slow response")
    .expect("fast recv");
    let (datagram, consumed) = socks5::parse_udp_datagram(&response[..len]).expect("datagram");
    assert_eq!(consumed, len);
    assert_eq!(datagram.target, TargetAddr::Ip(target_addr));
    assert_eq!(datagram.payload, Bytes::from_static(b"fast-pong"));

    control_client.shutdown().await.expect("control shutdown");
    handler.await.expect("handler join").expect("handler");
    server.abort();
    let _ = server.await;
    target.await.expect("target join");
}

#[tokio::test]
async fn socks5_udp_associate_prefers_ready_low_latency_path() {
    let (target_addr, target) = spawn_udp_echo_target_count(2).await;
    let first_path = reserve_udp_path_with_query("srtt-ms=10&rate-mbps=10").await;
    let second_path = reserve_udp_path_with_query("srtt-ms=10&rate-mbps=10").await;
    let first_socket = udp::bind_socket(&first_path)
        .await
        .expect("bind first udp path");
    let first_server = tokio::spawn(handle_server_udp_datagram_path_session(
        first_socket,
        server_context(OutboundConfig::Direct),
    ));
    let context = ClientPathContext::new(
        vec![first_path, second_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("ctx");
    let health_context = context.clone();
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
    let request = socks5::udp_datagram(&TargetAddr::Ip(target_addr), b"ping").expect("udp request");
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
        let (datagram, consumed) = socks5::parse_udp_datagram(&response[..len]).expect("datagram");
        assert_eq!(consumed, len);
        assert_eq!(datagram.target, TargetAddr::Ip(target_addr));
        assert_eq!(datagram.payload, Bytes::from_static(b"pong"));
    }
    {
        let health = health_context.health.lock().expect("health lock");
        assert_eq!(health.udp[0].active_flows, 1);
        assert_eq!(health.udp[1].active_flows, 0);
        assert_eq!(health.udp[1].state, SchedulerPathState::Active);
        assert_eq!(health.udp[1].consecutive_failures, 0);
    }
    control_client.shutdown().await.expect("control shutdown");

    handler.await.expect("handler join").expect("handler");
    {
        let health = health_context.health.lock().expect("health lock");
        assert_eq!(health.udp[0].active_flows, 0);
        assert_eq!(health.udp[1].active_flows, 0);
    }
    first_server
        .await
        .expect("first server join")
        .expect("first server");
    target.await.expect("target join");
}

#[tokio::test]
async fn udp_association_scores_pacer_delay_against_path_eta() {
    let (target_addr, target) = spawn_udp_echo_target().await;
    let low_latency_path = reserve_udp_path_with_query("srtt-ms=10&rate-mbps=100").await;
    let slower_path = reserve_udp_path_with_query("srtt-ms=120&rate-mbps=100").await;
    let low_latency_socket = udp::bind_socket(&low_latency_path)
        .await
        .expect("bind low latency udp path");
    let low_latency_server = tokio::spawn(handle_server_udp_datagram_path_session(
        low_latency_socket,
        server_context(OutboundConfig::Direct),
    ));
    let context = ClientPathContext::new(
        vec![low_latency_path, slower_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("ctx");
    let observed_context = context.clone();
    let mut association = UdpDatagramClientAssociation::new(context).expect("assoc");

    let response = association
        .send_to(
            TargetAddr::Ip(target_addr),
            Bytes::from_static(b"ping"),
            1000,
        )
        .await
        .expect("initial response");

    assert_eq!(response, Bytes::from_static(b"pong"));
    association
        .paths
        .iter_mut()
        .find(|path| path.session.path_index == 0)
        .expect("low latency path session")
        .pacer
        .next_send_at = Instant::now() + Duration::from_millis(30);

    assert_eq!(
        association.select_path_candidate(
            &[
                UdpPathCandidate {
                    path_index: 0,
                    eta_ms: 10.0,
                },
                UdpPathCandidate {
                    path_index: 1,
                    eta_ms: 120.0,
                },
            ],
            &HashSet::new(),
            512,
            1000,
        ),
        Some(0)
    );
    observed_context.mark_udp_path_probe_success(1, Duration::from_millis(20));
    assert_eq!(
        association.select_path_candidate(
            &[
                UdpPathCandidate {
                    path_index: 0,
                    eta_ms: 10.0,
                },
                UdpPathCandidate {
                    path_index: 1,
                    eta_ms: 25.0,
                },
            ],
            &HashSet::new(),
            512,
            1000,
        ),
        Some(0)
    );

    association.suppress_path_after_timeout(0, Duration::from_millis(250), 1000);
    assert_eq!(
        association.select_path_candidate(
            &[
                UdpPathCandidate {
                    path_index: 0,
                    eta_ms: 10.0,
                },
                UdpPathCandidate {
                    path_index: 1,
                    eta_ms: 25.0,
                },
            ],
            &HashSet::new(),
            512,
            1000,
        ),
        Some(1)
    );

    association.close().await.expect("close association");
    low_latency_server
        .await
        .expect("low latency server join")
        .expect("low latency server");
    target.await.expect("target join");
}

#[tokio::test]
async fn udp_association_retries_datagram_on_survivor_path_after_timeout() {
    let (target_addr, target) = spawn_udp_echo_target().await;
    let blackhole_path = reserve_udp_path_with_query("srtt-ms=5&rate-mbps=100").await;
    let survivor_path = reserve_udp_path_with_query("srtt-ms=20&rate-mbps=100").await;
    let blackhole = spawn_udp_datagram_blackhole_path(blackhole_path.clone()).await;
    let survivor_socket = udp::bind_socket(&survivor_path)
        .await
        .expect("bind survivor udp path");
    let survivor = tokio::spawn(handle_server_udp_datagram_path_session(
        survivor_socket,
        server_context(OutboundConfig::Direct),
    ));
    let context = ClientPathContext::new(
        vec![blackhole_path, survivor_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("ctx");
    let mut association = UdpDatagramClientAssociation::new(context.clone()).expect("assoc");

    let response = association
        .send_to(
            TargetAddr::Ip(target_addr),
            Bytes::from_static(b"ping"),
            1000,
        )
        .await
        .expect("retry response");

    assert_eq!(response, Bytes::from_static(b"pong"));
    {
        let health = context.health.lock().expect("health lock");
        assert_eq!(health.udp[0].state, SchedulerPathState::Suspect);
        assert_eq!(health.udp[0].active_flows, 0);
        assert_eq!(health.udp[1].state, SchedulerPathState::Active);
        assert_eq!(health.udp[1].active_flows, 1);
    }
    association.close().await.expect("close association");
    blackhole
        .await
        .expect("blackhole join")
        .expect("blackhole path");
    survivor
        .await
        .expect("survivor join")
        .expect("survivor path");
    target.await.expect("target join");
}

#[tokio::test]
async fn udp_association_probes_mtu_before_large_datagram() {
    let payload = Bytes::from(vec![0x5a; UDP_DEFAULT_MTU_PAYLOAD_BYTES + 256]);
    let (target_addr, target) =
        spawn_udp_payload_target(payload.clone(), Bytes::from_static(b"pong")).await;
    let path = reserve_udp_path().await;
    let socket = udp::bind_socket(&path).await.expect("bind udp path");
    let server = tokio::spawn(handle_server_udp_datagram_path_session(
        socket,
        server_context(OutboundConfig::Direct),
    ));
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("ctx");
    let mut association = UdpDatagramClientAssociation::new(context.clone()).expect("assoc");

    let response = association
        .send_to(TargetAddr::Ip(target_addr), payload.clone(), 1000)
        .await
        .expect("large datagram");

    assert_eq!(response, Bytes::from_static(b"pong"));
    {
        let health = context.health.lock().expect("health lock");
        assert_eq!(
            health.udp[0].measured_mtu_payload_bytes,
            Some(payload.len())
        );
    }
    association.close().await.expect("close association");
    server.await.expect("server join").expect("server");
    target.await.expect("target join");
}

#[test]
fn udp_measured_mtu_skips_oversized_path_candidate() {
    let low_mtu_path = "udp://127.0.0.1:12001?srtt-ms=5&rate-mbps=100"
        .parse::<PathSpec>()
        .expect("low mtu path");
    let probeable_path = "udp://127.0.0.1:12002?srtt-ms=20&rate-mbps=100"
        .parse::<PathSpec>()
        .expect("probeable path");
    let context = ClientPathContext::new(
        vec![low_mtu_path, probeable_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("ctx");
    context.mark_udp_path_mtu(0, UDP_DEFAULT_MTU_PAYLOAD_BYTES);
    let association = UdpDatagramClientAssociation::new(context).expect("assoc");

    assert_eq!(
        association.select_path_candidate(
            &[
                UdpPathCandidate {
                    path_index: 0,
                    eta_ms: 5.0,
                },
                UdpPathCandidate {
                    path_index: 1,
                    eta_ms: 20.0,
                },
            ],
            &HashSet::new(),
            UDP_DEFAULT_MTU_PAYLOAD_BYTES + 256,
            1000,
        ),
        Some(1)
    );
}

#[tokio::test]
async fn udp_association_retries_after_acked_response_loss_without_failing_path() {
    let (target_addr, target) = spawn_udp_echo_target().await;
    let drop_path = reserve_udp_path_with_query("srtt-ms=5&rate-mbps=100").await;
    let survivor_path = reserve_udp_path_with_query("srtt-ms=20&rate-mbps=100").await;
    let drop_server = spawn_udp_datagram_ack_then_drop_path(drop_path.clone()).await;
    let survivor_socket = udp::bind_socket(&survivor_path)
        .await
        .expect("bind survivor udp path");
    let survivor = tokio::spawn(handle_server_udp_datagram_path_session(
        survivor_socket,
        server_context(OutboundConfig::Direct),
    ));
    let context = ClientPathContext::new(
        vec![drop_path, survivor_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("ctx");
    let mut association = UdpDatagramClientAssociation::new(context.clone()).expect("assoc");

    let response = association
        .send_to(
            TargetAddr::Ip(target_addr),
            Bytes::from_static(b"ping"),
            1000,
        )
        .await
        .expect("retry response");

    assert_eq!(response, Bytes::from_static(b"pong"));
    {
        let health = context.health.lock().expect("health lock");
        assert_eq!(health.udp[0].state, SchedulerPathState::Active);
        assert!(
            health.udp[0]
                .measured_loss_rate
                .is_some_and(|loss| loss > 0.0)
        );
        assert_eq!(health.udp[1].state, SchedulerPathState::Active);
    }
    association.close().await.expect("close association");
    drop_server
        .await
        .expect("drop server join")
        .expect("drop server");
    survivor
        .await
        .expect("survivor join")
        .expect("survivor path");
    target.await.expect("target join");
}

#[tokio::test]
async fn udp_association_retries_acked_timeout_on_same_open_path() {
    let (target_addr, target) = spawn_udp_drop_first_echo_target().await;
    let path = reserve_udp_path_with_query("srtt-ms=5&rate-mbps=100").await;
    let socket = udp::bind_socket(&path).await.expect("bind udp path");
    let server = tokio::spawn(handle_server_udp_datagram_path_session(
        socket,
        server_context(OutboundConfig::Direct),
    ));
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("ctx");
    let mut association = UdpDatagramClientAssociation::new(context.clone()).expect("assoc");

    let response = association
        .send_to(
            TargetAddr::Ip(target_addr),
            Bytes::from_static(b"ping"),
            1000,
        )
        .await
        .expect("same path retry response");

    assert_eq!(response, Bytes::from_static(b"pong"));
    {
        let health = context.health.lock().expect("health lock");
        assert_eq!(health.udp[0].state, SchedulerPathState::Active);
        assert_eq!(health.udp[0].active_flows, 1);
        assert!(
            health.udp[0]
                .measured_loss_rate
                .is_some_and(|loss| loss > 0.0)
        );
    }
    association.close().await.expect("close association");
    server.await.expect("server join").expect("server");
    target.await.expect("target join");
}

#[tokio::test]
async fn udp_association_ignores_stale_response_datagram_id() {
    let target_socket = UdpSocket::bind("127.0.0.1:0").await.expect("target bind");
    let target_addr = target_socket.local_addr().expect("target addr");
    let path = reserve_udp_path_with_query("srtt-ms=5&rate-mbps=100").await;
    let server = spawn_udp_datagram_stale_then_matching_response_path(path.clone()).await;
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("ctx");
    let mut association = UdpDatagramClientAssociation::new(context).expect("assoc");

    let response = association
        .send_to(
            TargetAddr::Ip(target_addr),
            Bytes::from_static(b"ping"),
            1000,
        )
        .await
        .expect("matched response");

    assert_eq!(response, Bytes::from_static(b"pong"));
    association.close().await.expect("close association");
    server.await.expect("server join").expect("server");
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
                outbound_dns: DnsConfig::default(),
                codec_limits: CodecLimits::default(),
                mux_limits: ResourceLimits::default().into(),
                security: SecurityConfig::encrypted(
                    SharedSecret::new(b"fedcba9876543210".to_vec()).expect("secret"),
                ),
                tcp_streams: Arc::new(ServerTcpStreamRegistry::default()),
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
