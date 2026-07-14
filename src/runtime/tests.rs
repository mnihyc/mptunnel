use super::*;
use crate::config::{DEFAULT_OUTBOUND_CONNECT_TIMEOUT, SharedSecret};
use crate::ingress::ProxyAuthConfig;
use crate::runtime::stream::response::{ResponseStreamAttachOutcome, ResponseStreamBinding};
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

fn tcp_bulk_striping_indices(context: &ClientPathContext, payload_bytes: usize) -> Vec<usize> {
    context
        .ordered_reliable_bulk_striping_path_keys(payload_bytes)
        .into_iter()
        .filter_map(|key| (key.underlay == UnderlayProtocol::Tcp).then_some(key.index))
        .collect()
}

fn reliable_bulk_striping_path_keys(
    context: &ClientPathContext,
    payload_bytes: usize,
) -> Vec<RelayPathKey> {
    context.ordered_reliable_bulk_striping_path_keys(payload_bytes)
}

async fn recv_emitted_tcp_path_command(
    receivers: &mut ReliablePathCommandReceivers,
) -> Option<ReliablePathCommand> {
    let command = recv_reliable_path_command(receivers).await;
    if let Some(command) = &command {
        receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(command));
    }
    command
}

fn udp_stream_path_indices(
    context: &ClientPathContext,
    lane: FlowLane,
    payload_bytes: usize,
) -> Vec<usize> {
    let observations = health_observations(
        &mut context
            .health()
            .lock()
            .expect("client path health lock")
            .udp,
    );
    ordered_reliable_path_indices(&context.udp_paths, &observations, lane, payload_bytes)
}

fn server_context(outbound: OutboundConfig) -> ServerPathContext {
    let resources = ResourceLimits::default();
    ServerPathContext {
        tag: None,
        route_target: None,
        server_paths: Arc::new(Vec::new()),
        outbound,
        outbound_dns: DnsConfig::default(),
        outbound_connect_timeout: DEFAULT_OUTBOUND_CONNECT_TIMEOUT,
        performance: MppPerformanceConfig::default(),
        codec_limits: resources.into(),
        mux_limits: resources.into(),
        security: security(),
        reliable_streams: Arc::new(ServerReliableStreamRegistry::default()),
        path_join_replay: Arc::new(Mutex::new(RecentIdCache::new(
            path_join_replay_cache_capacity(resources.max_streams),
        ))),
        max_reliable_streams: resources.max_streams,
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
    let port = reserve_process_unique_tcp_port().await;
    format!("tcp://127.0.0.1:{port}").parse().expect("path")
}

async fn reserve_tcp_path_with_query(query: &str) -> PathSpec {
    let port = reserve_process_unique_tcp_port().await;
    format!("tcp://127.0.0.1:{port}?{query}")
        .parse()
        .expect("path")
}

async fn reserve_process_unique_tcp_port() -> u16 {
    loop {
        let probe = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("reserve tcp port");
        let port = probe.local_addr().expect("reserved tcp addr").port();
        let inserted = reserved_test_ports()
            .lock()
            .expect("reserved test ports lock")
            .insert(port);
        drop(probe);
        if inserted {
            return port;
        }
    }
}

async fn reserve_process_unique_udp_port() -> u16 {
    loop {
        let probe = UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("reserve udp port");
        let port = probe.local_addr().expect("reserved udp addr").port();
        let inserted = reserved_test_ports()
            .lock()
            .expect("reserved test ports lock")
            .insert(port);
        drop(probe);
        if inserted {
            return port;
        }
    }
}

fn reserved_test_ports() -> &'static std::sync::Mutex<std::collections::HashSet<u16>> {
    static PORTS: std::sync::OnceLock<std::sync::Mutex<std::collections::HashSet<u16>>> =
        std::sync::OnceLock::new();
    PORTS.get_or_init(|| std::sync::Mutex::new(std::collections::HashSet::new()))
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

async fn spawn_reliable_relay_heartbeat_blackhole(
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
        )
        .expect("initialize encrypted stream");
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

        let stream_id = loop {
            match framed.read_frame().await? {
                Frame::OpenStream { stream_id, .. } => break stream_id,
                Frame::PathMetrics { .. } => {}
                _ => return Err(RuntimeError::Protocol("expected OPEN_STREAM")),
            }
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
                Frame::PathMetrics { .. } => {}
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
    let port = reserve_process_unique_udp_port().await;
    format!("udp://127.0.0.1:{port}").parse().expect("path")
}

async fn spawn_udp_server_path(
    outbound: OutboundConfig,
) -> (PathSpec, tokio::task::JoinHandle<Result<(), RuntimeError>>) {
    let path = reserve_udp_path().await;
    let context = server_context(outbound);
    let endpoint = bind_server_udp_endpoint(&path, &context)
        .await
        .expect("bind udp path");
    let server = tokio::spawn(run_server_udp_listener(endpoint, context));
    (path, server)
}

#[tokio::test(flavor = "multi_thread")]
async fn server_udp_listener_accepts_probe_after_noise() {
    let bind_path = reserve_udp_path().await;
    let context = server_context(OutboundConfig::Direct);
    let endpoint = bind_server_udp_endpoint(&bind_path, &context)
        .await
        .expect("bind udp");
    let server_addr = endpoint.local_addr().expect("udp server addr");
    let server = tokio::spawn(run_server_udp_listener(endpoint, context));

    let noise = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("noise udp bind");
    noise
        .send_to(b"not a udp carrier packet", server_addr)
        .await
        .expect("send noise");

    let path = format!("udp://{server_addr}")
        .parse::<PathSpec>()
        .expect("client path");
    let resources = ResourceLimits::default();
    let mut session = UdpDatagramClientSession::open(
        &path,
        0,
        security(),
        resources.into(),
        resources.into(),
        Duration::from_secs(2),
    )
    .await
    .expect("open udp datagram session");
    session
        .ping(Duration::from_secs(2))
        .await
        .expect("udp ping");
    let _ = session.close_session().await;

    server.abort();
    let _ = server.await;
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
fn reliable_relay_sender_queue_budget_is_resource_gated() {
    let mut mux_limits = MuxLimits {
        max_payload_bytes: 64 * 1024,
        max_ack_ranges: 256,
        max_streams: 1024,
        max_quic_concurrent_bidi_streams: 1024,
        max_stream_window_bytes: 1024 * 1024,
        max_repair_bytes: 1024 * 1024,
        max_reorder_bytes: 1024 * 1024,
        max_datagram_queue_bytes: 1024 * 1024,
        max_path_flight_bytes: 32 * 1024,
        max_reliable_relay_chunk_bytes: 32 * 1024,
        tcp_path_heartbeat_interval: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_INTERVAL,
        tcp_path_heartbeat_timeout: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_TIMEOUT,
    };
    let send_stream = ReliableSendStream::new(StreamId(9), mux_limits);
    let mut sender_queue = ReliableRelaySenderQueue::default();
    let queue_limit =
        reliable_relay_sender_queue_limit(mux_limits, mux_limits.max_path_flight_bytes);

    assert!(reliable_relay_can_read_into_sender_queue(
        &send_stream,
        &sender_queue,
        mux_limits,
        queue_limit
    ));
    assert_eq!(
        reliable_relay_sender_queue_read_budget(
            &send_stream,
            &sender_queue,
            mux_limits,
            queue_limit,
            64 * 1024
        ),
        32 * 1024
    );

    sender_queue.push_data(Bytes::from(vec![0u8; 8 * 1024]));
    assert_eq!(
        reliable_relay_sender_queue_read_budget(
            &send_stream,
            &sender_queue,
            mux_limits,
            queue_limit,
            64 * 1024
        ),
        24 * 1024
    );
    assert!(!reliable_relay_can_read_product_source(
        true,
        true,
        &send_stream,
        &sender_queue,
        mux_limits,
        queue_limit
    ));
    assert!(reliable_relay_can_read_product_source(
        true,
        false,
        &send_stream,
        &sender_queue,
        mux_limits,
        queue_limit
    ));

    sender_queue.push_data(Bytes::from(vec![0u8; 24 * 1024]));
    assert!(!reliable_relay_can_read_into_sender_queue(
        &send_stream,
        &sender_queue,
        mux_limits,
        queue_limit
    ));
    assert_eq!(
        reliable_relay_sender_queue_read_budget(
            &send_stream,
            &sender_queue,
            mux_limits,
            queue_limit,
            64 * 1024
        ),
        0
    );

    sender_queue.pop_front();
    assert!(reliable_relay_can_read_into_sender_queue(
        &send_stream,
        &sender_queue,
        mux_limits,
        queue_limit
    ));
    assert_eq!(
        reliable_relay_sender_queue_read_budget(
            &send_stream,
            &sender_queue,
            mux_limits,
            queue_limit,
            64 * 1024
        ),
        8 * 1024
    );

    mux_limits.max_path_flight_bytes = 64 * 1024;
    let larger_queue_limit =
        reliable_relay_sender_queue_limit(mux_limits, mux_limits.max_path_flight_bytes);
    assert_eq!(
        reliable_relay_sender_queue_read_budget(
            &send_stream,
            &sender_queue,
            mux_limits,
            larger_queue_limit,
            16 * 1024
        ),
        16 * 1024
    );
}

#[test]
fn reliable_relay_sender_queue_prioritizes_only_critical_repair_lane() {
    let mut queue = ReliableRelaySenderQueue::default();
    queue.push_data(Bytes::from_static(b"ordinary"));
    queue.push_repair(Frame::StreamData {
        stream_id: StreamId(9),
        offset: 0,
        flags: StreamFlags::NONE,
        payload: Bytes::from_static(b"repair"),
    });

    let (lane, work) = queue.pop_front().expect("owner data");
    assert_eq!(lane, ReliableWorkClass::Data);
    assert_eq!(work.payload_bytes, b"ordinary".len());
    assert_eq!(queue.data_bytes(), 0);

    queue.push_data(Bytes::from_static(b"ordinary"));
    queue.push_critical_repair_with_cause(
        Frame::StreamData {
            stream_id: StreamId(9),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(b"repair"),
        },
        RelaySendCause::AckGapRepair,
    );
    let (lane, work) = queue.pop_front().expect("critical repair work");
    assert_eq!(lane, ReliableWorkClass::Repair);
    assert!(matches!(
        work.kind,
        ReliableRelayQueuedWorkKind::Repair {
            frame: Frame::StreamData { .. },
            cause: RelaySendCause::AckGapRepair,
        }
    ));
    assert_eq!(queue.data_bytes(), b"ordinary".len());
}

#[tokio::test]
async fn server_response_sender_dispatch_creates_stream_data_from_queued_bytes() {
    let stream_id = StreamId(42);
    let (commands, mut receivers) = reliable_path_command_channels(4);
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: 1024,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
        output: ReliablePathStreamOutput::fixed(
            UnderlayProtocol::Tcp,
            PathId(0),
            commands,
            MuxLimits::default(),
        ),
        frames: frame_rx,
    };
    let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
    send_stream.update_max_offset(1024);
    let mut sender = ServerResponseSenderService::new(SessionId(7), stream_id);
    sender.enqueue_data_for_lane(Bytes::from_static(b"response"), FlowLane::Throughput);

    assert!(sender.queued_send_ready());
    let dispatch = sender
        .dispatch_next(
            &path_stream,
            &mut send_stream,
            FlowLane::Throughput,
            MuxLimits::default(),
        )
        .expect("dispatch response bytes");

    assert_eq!(dispatch.lane, ReliableWorkClass::Data);
    assert_eq!(dispatch.payload_bytes, b"response".len());
    assert_eq!(
        dispatch.selected_path,
        Some(CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        })
    );
    assert_eq!(send_stream.next_offset(), b"response".len() as u64);
    assert!(matches!(
        recv_emitted_tcp_path_command(&mut receivers).await,
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            stream_id: id,
            offset: 0,
            payload,
            ..
        })) if id == stream_id && payload == Bytes::from_static(b"response")
    ));
}

#[tokio::test]
async fn fixed_response_output_learns_product_rate_from_stream_ack_batches() {
    let stream_id = StreamId(52);
    let mux_limits = MuxLimits::default();
    let (commands, mut receivers) = reliable_path_command_channels(64);
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: mux_limits.max_stream_window_bytes,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: reliable_relay_buffer_len(mux_limits),
        output: ReliablePathStreamOutput::fixed(
            UnderlayProtocol::Tcp,
            PathId(3),
            commands,
            mux_limits,
        ),
        frames: frame_rx,
    };
    let startup_snapshot = path_stream.send_path_snapshot(FlowLane::Throughput, 1);
    let startup_quantum =
        adaptive_reliable_relay_chunk_bytes(startup_snapshot, FlowLane::Throughput, mux_limits);
    let mut send_stream = ReliableSendStream::new(stream_id, mux_limits);
    send_stream.update_max_offset(mux_limits.max_stream_window_bytes);
    let mut sender = ServerResponseSenderService::new(SessionId(52), stream_id);
    sender.enqueue_data_for_lane(
        Bytes::from(vec![0x5a; PATH_OPEN_SCORE_BYTES * 4]),
        FlowLane::Throughput,
    );

    let mut ack_end = 0_u64;
    while ack_end < PATH_OPEN_SCORE_BYTES as u64 {
        let dispatch = sender
            .dispatch_next(
                &path_stream,
                &mut send_stream,
                FlowLane::Throughput,
                mux_limits,
            )
            .expect("dispatch fixed response quantum");
        ack_end = ack_end.saturating_add(dispatch.payload_bytes as u64);
        let _ = recv_emitted_tcp_path_command(&mut receivers).await;
    }
    path_stream.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: ack_end,
    }]);

    let learned = path_stream
        .send_path_snapshot(FlowLane::Throughput, startup_quantum)
        .expect("fixed output exposes learned path model");
    assert!(learned.product_progress_rate_bps.is_some());
    assert!(
        !learned.has_durable_product_progress,
        "one small ACK batch exposes a rate without graduating product authority"
    );
    assert!(
        adaptive_reliable_relay_chunk_bytes(Some(learned), FlowLane::Throughput, mux_limits)
            >= startup_quantum
    );
}

#[test]
fn client_snapshot_graduates_product_progress_only_at_bulk_sample_floor() {
    let path: PathSpec = "udp://127.0.0.1:1".parse().expect("UDP path");
    let sample_floor = (BBR_MAX_SEND_QUANTUM_BYTES as u64).max(PATH_OPEN_SCORE_BYTES as u64);
    let mut observation = ClientPathObservation {
        measured_rate_bps: Some(100_000_000.0),
        product_delivery_rate_bps: Some(100_000_000.0),
        delivery_samples: 1,
        product_delivery_sample_bytes: sample_floor - 1,
        carrier_inflight_limit_bytes: sample_floor,
        ..ClientPathObservation::default()
    };

    let point_rate = path_snapshot(&path, 0, observation);
    assert!(point_rate.product_progress_rate_bps.is_some());
    assert!(!point_rate.has_durable_product_progress);
    assert!(!bulk_candidate_has_bulk_rate_evidence(&path, observation));

    let handed_off_observation = ClientPathObservation {
        carrier_delivery_rate_bps: Some(117_000_000.0),
        carrier_delivery_samples: 1,
        carrier_delivery_sample_bytes: sample_floor - 1,
        carrier_ack_derived_data_seen: true,
        quic_capacity_product_handoff_complete: true,
        quic_capacity_rate_prior_fresh: true,
        ..ClientPathObservation::default()
    };
    let handed_off = path_snapshot(&path, 0, handed_off_observation);
    assert!(handed_off.has_durable_product_progress);
    assert!(bulk_candidate_has_bulk_rate_evidence(
        &path,
        handed_off_observation
    ));
    assert_eq!(path_model_confidence(handed_off_observation), 1.0);

    let stale_rate_prior = ClientPathObservation {
        quic_capacity_rate_prior_fresh: false,
        ..handed_off_observation
    };
    assert!(path_snapshot(&path, 0, stale_rate_prior).has_durable_product_progress);
    assert!(path_model_confidence(stale_rate_prior) < 1.0);

    observation.product_delivery_sample_bytes = sample_floor;
    let durable = path_snapshot(&path, 0, observation);
    assert!(durable.has_durable_product_progress);
    assert!(bulk_candidate_has_bulk_rate_evidence(&path, observation));
}

#[test]
fn data_plane_failure_invalidates_durable_product_and_native_window_authority() {
    let path: PathSpec = "udp://127.0.0.1:2".parse().expect("UDP path");
    let sample_floor = 7 * 1024 * 1024_u64;
    let mut health = ClientPathHealthRecord::default();
    health.carrier_inflight_limit_bytes = sample_floor;
    health.carrier_delivery_rate_bps = Some(500_000_000.0);
    health.carrier_delivery_sample_bytes = sample_floor;
    health.carrier_delivery_samples = 32;
    health.mark_product_delivery(
        PathRateSample::new(sample_floor, Duration::from_millis(360)).expect("rate sample"),
    );
    let before_failure = health.observe(Instant::now());
    assert!(path_snapshot(&path, 0, before_failure).has_durable_product_progress);

    health.mark_data_plane_failure(Instant::now(), false);
    let after_failure = health.observe(Instant::now());
    assert_eq!(after_failure.state, SchedulerPathState::Suspect);
    assert!(after_failure.product_delivery_rate_bps.is_none());
    assert_eq!(after_failure.product_delivery_sample_bytes, 0);
    assert!(after_failure.carrier_delivery_rate_bps.is_none());
    assert_eq!(after_failure.carrier_inflight_limit_bytes, 0);
    assert!(!after_failure.quic_capacity_product_handoff_complete);
    assert!(!path_snapshot(&path, 0, after_failure).has_durable_product_progress);
}

#[test]
fn fixed_output_graduates_fragmented_product_acks_at_exact_sample_floor() {
    let mux_limits = MuxLimits::default();
    let (commands, _receivers) = reliable_path_command_channels(8);
    let output =
        ReliablePathStreamOutput::fixed(UnderlayProtocol::Udp, PathId(4), commands, mux_limits);
    let sample_bytes =
        usize::try_from(reliable_subflow_startup_sample_limit_bytes(mux_limits)).unwrap();
    let frame = Frame::StreamData {
        stream_id: StreamId(53),
        offset: 0,
        flags: StreamFlags::NONE,
        payload: Bytes::from(vec![0x5b; sample_bytes]),
    };
    let ReliablePathStreamOutput::Fixed(fixed) = &output else {
        panic!("expected fixed output");
    };
    fixed.record_owner_flight(&frame);
    let ack_fragment_bytes = MIN_RATE_SAMPLE_BYTES / 2;
    let mut start = 0_u64;
    while start < sample_bytes as u64 {
        let end = start
            .saturating_add(ack_fragment_bytes)
            .min(sample_bytes as u64);
        output.release_normalized_acked_ranges(&[OffsetRange { start, end }]);
        start = end;
    }

    let snapshot = output
        .send_path_snapshot(FlowLane::Throughput, 1)
        .expect("fixed output exposes learned path model");
    assert!(snapshot.product_progress_rate_bps.is_none());
    assert!(snapshot.has_durable_product_progress);
}

#[test]
fn fixed_response_output_inherits_path_startup_evidence() {
    let mux_limits = MuxLimits::default();
    let (commands, _receivers) = reliable_path_command_channels(64);
    let mut startup = PathSnapshot::new(PathId(9), UnderlayProtocol::Tcp, 20.0, 500_000_000.0);
    startup.pacing_rate_bps = 500_000_000.0;
    startup.inflight_limit_bytes =
        bbr_inflight_target_bytes(startup, FlowLane::Throughput, mux_limits).ceil() as u64;
    startup.confidence = 1.0;
    let output = ReliablePathStreamOutput::fixed_with_snapshot(startup, commands, mux_limits);

    let inherited = output
        .send_path_snapshot(FlowLane::Throughput, 1)
        .expect("fixed output exposes startup path model");
    let default = PathSnapshot::new(
        PathId(9),
        UnderlayProtocol::Tcp,
        default_path_srtt_ms(UnderlayProtocol::Tcp),
        default_path_rate_bps(UnderlayProtocol::Tcp),
    );

    assert_eq!(inherited.id, startup.id);
    assert_eq!(inherited.underlay, startup.underlay);
    assert_eq!(inherited.delivery_rate_bps, startup.delivery_rate_bps);
    assert_eq!(inherited.srtt_ms, startup.srtt_ms);
    assert_eq!(
        adaptive_reliable_relay_chunk_bytes(Some(inherited), FlowLane::Throughput, mux_limits),
        adaptive_reliable_relay_chunk_bytes(Some(default), FlowLane::Throughput, mux_limits),
        "TCP throughput startup uses the BBR feed quantum even before path-rate evidence is measured"
    );
}

#[test]
fn fixed_response_output_keeps_product_flight_out_of_carrier_flight() {
    let mux_limits = MuxLimits::default();
    let (commands, _receivers) = reliable_path_command_channels(64);
    let mut startup = PathSnapshot::new(PathId(9), UnderlayProtocol::Tcp, 20.0, 500_000_000.0);
    startup.pacing_rate_bps = 500_000_000.0;
    startup.inflight_limit_bytes =
        bbr_inflight_target_bytes(startup, FlowLane::Throughput, mux_limits).ceil() as u64;
    startup.confidence = 1.0;
    let output = ReliablePathStreamOutput::fixed_with_snapshot(startup, commands, mux_limits);
    let frame = Frame::StreamData {
        stream_id: StreamId(9),
        offset: 0,
        flags: StreamFlags::NONE,
        payload: Bytes::from(vec![0x33; PATH_OPEN_SCORE_BYTES]),
    };
    let ReliablePathStreamOutput::Fixed(fixed) = &output else {
        panic!("expected fixed output");
    };
    fixed.record_owner_flight(&frame);

    let snapshot = output
        .send_path_snapshot(FlowLane::Throughput, PATH_OPEN_SCORE_BYTES)
        .expect("fixed output exposes path model");

    assert_eq!(snapshot.bytes_in_flight, 0);
    assert_eq!(
        snapshot.product_bytes_in_flight,
        PATH_OPEN_SCORE_BYTES as u64
    );
    assert!(
        adaptive_reliable_relay_chunk_bytes(Some(snapshot), FlowLane::Throughput, mux_limits)
            > bbr_min_send_quantum_bytes(mux_limits),
        "product STREAM_ACK debt must not collapse TCP carrier send quantum to 2*MSS"
    );
}

#[tokio::test]
async fn server_response_sender_slices_large_reads_to_service_quantum() {
    let stream_id = StreamId(42);
    let mux_limits = MuxLimits::default();
    let (commands, mut receivers) = reliable_path_command_channels(16);
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: mux_limits.max_stream_window_bytes,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Udp,
        max_frame_payload_bytes: reliable_relay_buffer_len(mux_limits),
        output: ReliablePathStreamOutput::fixed(
            UnderlayProtocol::Udp,
            PathId(0),
            commands,
            mux_limits,
        ),
        frames: frame_rx,
    };
    let quantum = adaptive_reliable_relay_chunk_bytes(
        path_stream.send_path_snapshot(FlowLane::Throughput, 1),
        FlowLane::Throughput,
        mux_limits,
    );
    let mut send_stream = ReliableSendStream::new(stream_id, mux_limits);
    send_stream.update_max_offset(mux_limits.max_stream_window_bytes);
    let mut sender = ServerResponseSenderService::new(SessionId(17), stream_id);
    let queued_bytes = mux_limits.max_reliable_relay_chunk_bytes;
    sender.enqueue_data_for_lane(Bytes::from(vec![0x5a; queued_bytes]), FlowLane::Throughput);

    let dispatch = sender
        .dispatch_next(
            &path_stream,
            &mut send_stream,
            FlowLane::Throughput,
            mux_limits,
        )
        .expect("dispatch first service quantum");

    assert_eq!(dispatch.lane, ReliableWorkClass::Data);
    assert_eq!(dispatch.payload_bytes, quantum);
    assert_eq!(sender.data_bytes(), queued_bytes - quantum);
    assert_eq!(send_stream.next_offset(), quantum as u64);
    assert!(matches!(
        recv_emitted_tcp_path_command(&mut receivers).await,
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            stream_id: id,
            offset: 0,
            payload,
            ..
        })) if id == stream_id && payload.len() == quantum
    ));
}

#[tokio::test]
async fn server_response_sender_keeps_enqueue_lane_for_remaining_data_after_promotion() {
    let stream_id = StreamId(142);
    let mux_limits = MuxLimits::default();
    let (commands, mut receivers) = reliable_path_command_channels(16);
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: mux_limits.max_stream_window_bytes,
        lane: FlowLane::Latency,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: reliable_relay_buffer_len(mux_limits),
        output: ReliablePathStreamOutput::fixed(
            UnderlayProtocol::Tcp,
            PathId(0),
            commands,
            mux_limits,
        ),
        frames: frame_rx,
    };
    let mut send_stream = ReliableSendStream::new(stream_id, mux_limits);
    send_stream.update_max_offset(mux_limits.max_stream_window_bytes);
    let mut sender = ServerResponseSenderService::new(SessionId(142), stream_id);
    let latency_quantum = adaptive_reliable_relay_chunk_bytes(
        path_stream.send_path_snapshot(FlowLane::Latency, 1),
        FlowLane::Latency,
        mux_limits,
    );
    sender.enqueue_data_for_lane(
        Bytes::from(vec![0x5a; latency_quantum * 3]),
        FlowLane::Latency,
    );

    let first = sender
        .dispatch_next(
            &path_stream,
            &mut send_stream,
            FlowLane::Latency,
            mux_limits,
        )
        .expect("dispatch first latency slice");
    assert_eq!(first.payload_bytes, latency_quantum);
    assert!(matches!(
        recv_emitted_tcp_path_command(&mut receivers).await,
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            offset: 0,
            payload,
            ..
        })) if payload.len() == latency_quantum
    ));

    let second = sender
        .dispatch_next(
            &path_stream,
            &mut send_stream,
            FlowLane::Throughput,
            mux_limits,
        )
        .expect("remaining bytes keep enqueue-time latency lane");
    assert_eq!(second.payload_bytes, latency_quantum);
    assert!(
        matches!(
            recv_emitted_tcp_path_command(&mut receivers).await,
            Some(ReliablePathCommand::SendFrame(Frame::StreamData {
                offset,
                payload,
                ..
            })) if offset == latency_quantum as u64 && payload.len() == latency_quantum
        ),
        "already-queued latency data must keep its enqueue-time dispatch quantum without splitting OwnerData across carrier priority queues"
    );
}

#[tokio::test]
async fn server_response_sender_dispatches_control_before_repair_and_data() {
    let stream_id = StreamId(47);
    let (commands, mut receivers) = reliable_path_command_channels(4);
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: 1024,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
        output: ReliablePathStreamOutput::fixed(
            UnderlayProtocol::Tcp,
            PathId(0),
            commands,
            MuxLimits::default(),
        ),
        frames: frame_rx,
    };
    let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
    send_stream.update_max_offset(1024);
    let mut sender = ServerResponseSenderService::new(SessionId(47), stream_id);
    sender.enqueue_data_for_lane(Bytes::from_static(b"ordinary"), FlowLane::Throughput);
    assert!(
        sender
            .enqueue_repair_frame_with_priority(
                Frame::StreamData {
                    stream_id,
                    offset: 64,
                    flags: StreamFlags::NONE,
                    payload: Bytes::from_static(b"repair"),
                },
                MuxLimits::default(),
                true,
            )
            .is_some()
    );
    sender.enqueue_control_frame(Frame::StreamAck {
        stream_id,
        complete: false,
        ranges: Vec::new(),
    });

    let control_dispatch = sender
        .dispatch_next(
            &path_stream,
            &mut send_stream,
            FlowLane::Throughput,
            MuxLimits::default(),
        )
        .expect("dispatch control");
    assert_eq!(control_dispatch.lane, ReliableWorkClass::Control);
    assert_eq!(send_stream.next_offset(), 0);
    assert!(matches!(
        recv_emitted_tcp_path_command(&mut receivers).await,
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck {
            stream_id: id,
            complete: false,
            ..
        })) if id == stream_id
    ));

    let repair_dispatch = sender
        .dispatch_next(
            &path_stream,
            &mut send_stream,
            FlowLane::Throughput,
            MuxLimits::default(),
        )
        .expect("dispatch repair");
    assert_eq!(repair_dispatch.lane, ReliableWorkClass::Repair);
}

#[tokio::test]
async fn server_response_sender_dispatches_final_fin_after_queued_data() {
    let stream_id = StreamId(49);
    let (commands, mut receivers) = reliable_path_command_channels(4);
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: 1024,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
        output: ReliablePathStreamOutput::fixed(
            UnderlayProtocol::Tcp,
            PathId(0),
            commands,
            MuxLimits::default(),
        ),
        frames: frame_rx,
    };
    let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
    send_stream.update_max_offset(1024);
    let mut sender = ServerResponseSenderService::new(SessionId(49), stream_id);
    sender.enqueue_data_for_lane(Bytes::from_static(b"ordinary"), FlowLane::Throughput);
    sender.enqueue_final_control_frame(Frame::StreamFin {
        stream_id,
        final_offset: b"ordinary".len() as u64,
    });

    let data_dispatch = sender
        .dispatch_next(
            &path_stream,
            &mut send_stream,
            FlowLane::Throughput,
            MuxLimits::default(),
        )
        .expect("dispatch ordinary data first");
    assert_eq!(data_dispatch.lane, ReliableWorkClass::Data);

    let fin_dispatch = sender
        .dispatch_next(
            &path_stream,
            &mut send_stream,
            FlowLane::Throughput,
            MuxLimits::default(),
        )
        .expect("dispatch final FIN after data");
    assert_eq!(fin_dispatch.lane, ReliableWorkClass::Control);
    assert!(matches!(
        recv_emitted_tcp_path_command(&mut receivers).await,
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            stream_id: id,
            offset: 0,
            payload,
            ..
        })) if id == stream_id && payload == Bytes::from_static(b"ordinary")
    ));
    assert!(matches!(
        recv_emitted_tcp_path_command(&mut receivers).await,
        Some(ReliablePathCommand::SendFrame(Frame::StreamFin {
            stream_id: id,
            final_offset,
        })) if id == stream_id && final_offset == b"ordinary".len() as u64
    ));
}

#[tokio::test]
async fn server_response_control_queue_full_is_sender_backpressure() {
    let stream_id = StreamId(48);
    let (commands, mut receivers) = reliable_path_command_channels(1);
    commands
        .try_enqueue_admitted_frame(
            Frame::StreamAck {
                stream_id,
                complete: false,
                ranges: Vec::new(),
            },
            FlowLane::Control,
        )
        .expect("prefill priority queue");
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: 1024,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
        output: ReliablePathStreamOutput::fixed(
            UnderlayProtocol::Tcp,
            PathId(0),
            commands,
            MuxLimits::default(),
        ),
        frames: frame_rx,
    };
    let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
    send_stream.update_max_offset(1024);
    let mut sender = ServerResponseSenderService::new(SessionId(48), stream_id);
    sender.enqueue_control_frame(Frame::StreamAck {
        stream_id,
        complete: false,
        ranges: Vec::new(),
    });

    let err = sender
        .dispatch_next(
            &path_stream,
            &mut send_stream,
            FlowLane::Throughput,
            MuxLimits::default(),
        )
        .expect_err("full control queue should be sender-service backpressure");
    assert!(matches!(err, RuntimeError::SenderServiceBlocked));
    assert_eq!(sender.bytes(), 1);
    assert!(matches!(
        recv_emitted_tcp_path_command(&mut receivers).await,
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
    ));
}

#[tokio::test]
async fn server_response_sender_keeps_data_queued_when_carrier_rejects() {
    let stream_id = StreamId(44);
    let (commands, receivers) = reliable_path_command_channels(1);
    drop(receivers);
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: 1024,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
        output: ReliablePathStreamOutput::fixed(
            UnderlayProtocol::Tcp,
            PathId(0),
            commands,
            MuxLimits::default(),
        ),
        frames: frame_rx,
    };
    let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
    send_stream.update_max_offset(1024);
    let mut sender = ServerResponseSenderService::new(SessionId(9), stream_id);
    sender.enqueue_data_for_lane(Bytes::from_static(b"response"), FlowLane::Throughput);

    assert_eq!(sender.bytes(), b"response".len());
    assert!(
        sender
            .dispatch_next(
                &path_stream,
                &mut send_stream,
                FlowLane::Throughput,
                MuxLimits::default()
            )
            .is_err()
    );
    assert_eq!(sender.bytes(), b"response".len());
    assert_eq!(sender.data_bytes(), b"response".len());
    assert_eq!(send_stream.next_offset(), 0);
    assert_eq!(send_stream.repair_bytes(), 0);
}

#[tokio::test]
async fn server_response_sender_blocks_when_switchable_outputs_detach() {
    let stream_id = StreamId(45);
    let session_id = SessionId(10);
    let (commands, _receivers) = reliable_path_command_channels(4);
    let binding = ResponseStreamBinding::new(
        session_id,
        UnderlayProtocol::Udp,
        PathId(0),
        commands.clone(),
        FlowLane::Throughput,
    );
    binding.detach(
        CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        },
        &commands,
    );
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: 1024,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Udp,
        max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
        output: ReliablePathStreamOutput::Switchable(binding),
        frames: frame_rx,
    };
    let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
    send_stream.update_max_offset(1024);
    let mut sender = ServerResponseSenderService::new(session_id, stream_id);
    sender.enqueue_data_for_lane(Bytes::from_static(b"response"), FlowLane::Throughput);

    let err = sender
        .dispatch_next(
            &path_stream,
            &mut send_stream,
            FlowLane::Throughput,
            MuxLimits::default(),
        )
        .expect_err("detached switchable output should block, not close product stream");

    assert!(matches!(err, RuntimeError::SenderServiceBlocked));
    assert_eq!(sender.bytes(), b"response".len());
    assert_eq!(sender.data_bytes(), b"response".len());
    assert_eq!(send_stream.next_offset(), 0);
    assert_eq!(send_stream.repair_bytes(), 0);
}

#[tokio::test]
async fn server_response_sender_queue_full_is_backpressure_not_path_failure() {
    let stream_id = StreamId(46);
    let (commands, mut receivers) = reliable_path_command_channels(1);
    commands
        .try_enqueue_admitted_frame(
            Frame::StreamData {
                stream_id,
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"already queued"),
            },
            FlowLane::Throughput,
        )
        .expect("prefill carrier data queue");
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: 1024,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
        output: ReliablePathStreamOutput::fixed(
            UnderlayProtocol::Tcp,
            PathId(0),
            commands,
            MuxLimits::default(),
        ),
        frames: frame_rx,
    };
    let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
    send_stream.update_max_offset(1024);
    let mut sender = ServerResponseSenderService::new(SessionId(11), stream_id);
    sender.enqueue_data_for_lane(Bytes::from_static(b"later"), FlowLane::Throughput);

    let err = sender
        .dispatch_next(
            &path_stream,
            &mut send_stream,
            FlowLane::Throughput,
            MuxLimits::default(),
        )
        .expect_err("full carrier queue should be sender-service backpressure");

    assert!(matches!(err, RuntimeError::SenderServiceBlocked));
    assert_eq!(sender.bytes(), b"later".len());
    assert_eq!(sender.data_bytes(), b"later".len());
    assert_eq!(send_stream.next_offset(), 0);
    assert!(matches!(
        recv_emitted_tcp_path_command(&mut receivers).await,
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            payload,
            ..
        })) if payload == Bytes::from_static(b"already queued")
    ));
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_emitted_tcp_path_command(&mut receivers)
        )
        .await
        .is_err(),
        "blocked dispatch must not enqueue another STREAM_DATA frame"
    );
}

#[tokio::test]
async fn response_binding_duplicate_live_path_rejects_fresh_output() {
    let stream_id = StreamId(47);
    let session_id = SessionId(12);
    let (first_commands, mut first_receivers) = reliable_path_command_channels(4);
    let binding = ResponseStreamBinding::new(
        session_id,
        UnderlayProtocol::Udp,
        PathId(0),
        first_commands,
        FlowLane::Throughput,
    );
    let (second_commands, mut second_receivers) = reliable_path_command_channels(4);
    assert_eq!(
        binding.attach(
            UnderlayProtocol::Udp,
            PathId(0),
            second_commands,
            FlowLane::Throughput,
            StreamOpenRole::Active,
            reliable_relay_buffer_len(MuxLimits::default()),
        ),
        ResponseStreamAttachOutcome::RejectedDuplicateLiveOutput
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: 1024,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Udp,
        max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
        output: ReliablePathStreamOutput::Switchable(binding),
        frames: frame_rx,
    };
    let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
    send_stream.update_max_offset(1024);
    let mut sender = ServerResponseSenderService::new(session_id, stream_id);
    sender.enqueue_data_for_lane(Bytes::from_static(b"same-path-live"), FlowLane::Throughput);

    sender
        .dispatch_next(
            &path_stream,
            &mut send_stream,
            FlowLane::Throughput,
            MuxLimits::default(),
        )
        .expect("rejecting a duplicate live output must keep the existing output usable");

    assert!(matches!(
        recv_emitted_tcp_path_command(&mut first_receivers).await,
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            payload,
            ..
        })) if payload == Bytes::from_static(b"same-path-live")
    ));
    let duplicate_output = tokio::time::timeout(
        Duration::from_millis(20),
        recv_emitted_tcp_path_command(&mut second_receivers),
    )
    .await;
    assert!(
        !matches!(duplicate_output, Ok(Some(_))),
        "duplicate live attach must not redirect response data to a fresh carrier output"
    );
}

#[tokio::test]
async fn response_binding_duplicate_closed_path_replaces_output() {
    let stream_id = StreamId(48);
    let session_id = SessionId(13);
    let (first_commands, first_receivers) = reliable_path_command_channels(4);
    let binding = ResponseStreamBinding::new(
        session_id,
        UnderlayProtocol::Udp,
        PathId(0),
        first_commands,
        FlowLane::Throughput,
    );
    drop(first_receivers);

    let (second_commands, mut second_receivers) = reliable_path_command_channels(4);
    assert_eq!(
        binding.attach(
            UnderlayProtocol::Udp,
            PathId(0),
            second_commands,
            FlowLane::Throughput,
            StreamOpenRole::Active,
            reliable_relay_buffer_len(MuxLimits::default()),
        ),
        ResponseStreamAttachOutcome::ReplacedClosedOutput
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: 1024,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Udp,
        max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
        output: ReliablePathStreamOutput::Switchable(binding),
        frames: frame_rx,
    };
    let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
    send_stream.update_max_offset(1024);
    let mut sender = ServerResponseSenderService::new(session_id, stream_id);
    sender.enqueue_data_for_lane(
        Bytes::from_static(b"same-path-closed"),
        FlowLane::Throughput,
    );

    sender
        .dispatch_next(
            &path_stream,
            &mut send_stream,
            FlowLane::Throughput,
            MuxLimits::default(),
        )
        .expect("closed same-path output should be replaced by the new carrier output");

    assert!(matches!(
        recv_emitted_tcp_path_command(&mut second_receivers).await,
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            payload,
            ..
        })) if payload == Bytes::from_static(b"same-path-closed")
    ));
}

fn server_test_bulk_path_metrics(path_id: PathId, delivery_rate_bps: u64) -> PathMetrics {
    PathMetrics {
        path_id,
        underlay: UnderlayProtocol::Tcp,
        direction: PathMetricDirection::ServerToClient,
        metric_epoch: metric_epoch_now(),
        metric_age_us: 0,
        min_rtt_us: 20_000,
        srtt_us: 20_000,
        rttvar_us: 1_000,
        jitter_us: 1_000,
        delivery_rate_bps,
        pacing_rate_bps: delivery_rate_bps,
        loss_ppm: 0,
        ecn_ppm: 0,
        loss_observed: false,
        ecn_observed: false,
        bytes_in_flight: 0,
        queue_bytes: 0,
        inflight_limit_bytes: BBR_MAX_SEND_QUANTUM_BYTES as u64,
        inflight_hi_bytes: BBR_MAX_SEND_QUANTUM_BYTES as u64,
        confidence_ppm: 1_000_000,
        app_limited: false,
        has_ack_derived_data_sample: true,
        data_sample_count: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
        data_sample_bytes: RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES / 2,
    }
}

#[test]
fn server_registry_replaced_output_does_not_reuse_cached_bulk_metrics() {
    let registry = Arc::new(ServerReliableStreamRegistry::new(
        ResourceLimits::default().max_streams,
    ));
    let target = TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80)));
    let session_id = SessionId(14);
    let stream_id = StreamId(49);
    let path_id = PathId(0);
    let old_path_registration =
        registry.register_carrier_path(session_id, UnderlayProtocol::Tcp, path_id);
    let (old_commands, old_receivers) = reliable_path_command_channels(8);
    let stream = match registry
        .open_or_attach(
            ServerReliableStreamOpenRequest {
                session_id,
                stream_id,
                target: &target,
                lane: FlowLane::Throughput,
                attachment: ServerReliablePathAttachment {
                    path_registration: old_path_registration.clone(),
                    commands: old_commands,
                    max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
                    role: StreamOpenRole::Active,
                    initial_metrics: None,
                },
            },
            MuxLimits::default(),
            ResourceLimits::default().max_streams,
        )
        .expect("open response stream")
    {
        ServerReliableStreamOpen::New(stream) => stream,
        _ => panic!("expected a new response stream"),
    };
    let ReliablePathStreamOutput::Switchable(binding) = &stream.output else {
        panic!("expected switchable output");
    };
    let binding = binding.clone();
    registry.record_local_path_metrics(
        &old_path_registration,
        server_test_bulk_path_metrics(path_id, 200_000_000),
    );
    assert!(
        binding
            .sender_path_targets(FlowLane::Throughput, BBR_MAX_SEND_QUANTUM_BYTES)
            .first()
            .is_some_and(|entry| entry.has_bulk_rate_evidence)
    );
    drop(old_receivers);

    let new_path_registration =
        registry.register_carrier_path(session_id, UnderlayProtocol::Tcp, path_id);
    let (new_commands, _new_receivers) = reliable_path_command_channels(8);
    assert!(matches!(
        registry
            .open_or_attach(
                ServerReliableStreamOpenRequest {
                    session_id,
                    stream_id,
                    target: &target,
                    lane: FlowLane::Throughput,
                    attachment: ServerReliablePathAttachment {
                        path_registration: new_path_registration.clone(),
                        commands: new_commands,
                        max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
                        role: StreamOpenRole::Active,
                        initial_metrics: None,
                    },
                },
                MuxLimits::default(),
                ResourceLimits::default().max_streams,
            )
            .expect("replace closed response output"),
        ServerReliableStreamOpen::Existing
    ));
    registry.record_local_path_metrics(
        &old_path_registration,
        server_test_bulk_path_metrics(path_id, 300_000_000),
    );

    let targets = binding.sender_path_targets(FlowLane::Throughput, BBR_MAX_SEND_QUANTUM_BYTES);
    assert_eq!(targets.len(), 1);
    assert!(
        !targets[0].has_bulk_rate_evidence,
        "cached metrics from the closed carrier must not prove its replacement"
    );
}

#[test]
fn carrier_metrics_retire_after_last_publication_lease() {
    let registry = Arc::new(ServerReliableStreamRegistry::new(
        ResourceLimits::default().max_streams,
    ));
    let session_id = SessionId(15);
    let path_id = PathId(0);
    let registration = registry.register_carrier_path(session_id, UnderlayProtocol::Udp, path_id);
    let sampler_registration = registration.clone();

    registry.record_local_path_metrics(
        &registration,
        PathMetrics {
            underlay: UnderlayProtocol::Udp,
            ..server_test_bulk_path_metrics(path_id, 200_000_000)
        },
    );
    assert_eq!(registry.management_snapshot().path_metrics.len(), 1);

    drop(registration);
    assert_eq!(registry.management_snapshot().path_metrics.len(), 1);

    registry.record_local_path_metrics(
        &sampler_registration,
        PathMetrics {
            underlay: UnderlayProtocol::Udp,
            ..server_test_bulk_path_metrics(path_id, 300_000_000)
        },
    );
    drop(sampler_registration);
    assert!(
        registry.management_snapshot().path_metrics.is_empty(),
        "cached evidence must retire when the last task lease ends"
    );
}

#[tokio::test]
async fn server_response_sender_blocked_admission_does_not_fallback_to_eta_target() {
    let stream_id = StreamId(45);
    let (tcp_commands, _tcp_receivers) = reliable_path_command_channels(4);
    let tcp_commands_for_detach = tcp_commands.clone();
    let binding = ResponseStreamBinding::new(
        SessionId(10),
        UnderlayProtocol::Tcp,
        PathId(0),
        tcp_commands,
        FlowLane::Throughput,
    );
    let (udp_commands, mut udp_receivers) = reliable_path_command_channels(4);
    binding.attach(
        UnderlayProtocol::Udp,
        PathId(1),
        udp_commands,
        FlowLane::Throughput,
        StreamOpenRole::Active,
        reliable_relay_buffer_len(MuxLimits::default()),
    );
    let lower_owner_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    binding.record_owner_flight(
        lower_owner_key,
        &Frame::StreamData {
            stream_id,
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(b"x"),
        },
    );
    binding.record_owner_flight(
        lower_owner_key,
        &Frame::StreamData {
            stream_id,
            offset: 1,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(b"y"),
        },
    );
    binding.release_normalized_acked_ranges(&[OffsetRange { start: 1, end: 2 }]);
    binding.detach(lower_owner_key, &tcp_commands_for_detach);

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: 1024,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Udp,
        max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
        output: ReliablePathStreamOutput::Switchable(binding),
        frames: frame_rx,
    };
    let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
    send_stream.update_max_offset(1024);
    send_stream
        .send_data(Bytes::from_static(b"xy"), StreamFlags::NONE)
        .expect("advance response sender past the lower ACK-hole byte");
    let mut sender = ServerResponseSenderService::new(SessionId(10), stream_id);
    sender.enqueue_data_for_lane(Bytes::from_static(b"later"), FlowLane::Throughput);

    let err = sender
        .dispatch_next(
            &path_stream,
            &mut send_stream,
            FlowLane::Throughput,
            MuxLimits::default(),
        )
        .expect_err("cross-underlay ordering debt must block ordinary response data");

    assert!(matches!(err, RuntimeError::SenderServiceBlocked));
    assert_eq!(sender.bytes(), b"later".len());
    assert_eq!(sender.data_bytes(), b"later".len());
    assert_eq!(send_stream.next_offset(), 2);
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_emitted_tcp_path_command(&mut udp_receivers)
        )
        .await
        .is_err(),
        "blocked response admission must not send via raw ETA fallback target"
    );
}

#[tokio::test]
async fn server_response_sender_dispatches_repair_before_data() {
    let stream_id = StreamId(43);
    let (commands, mut receivers) = reliable_path_command_channels(4);
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: 1024,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
        output: ReliablePathStreamOutput::fixed(
            UnderlayProtocol::Tcp,
            PathId(0),
            commands,
            MuxLimits::default(),
        ),
        frames: frame_rx,
    };
    let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
    send_stream.update_max_offset(1024);
    let mut sender = ServerResponseSenderService::new(SessionId(8), stream_id);
    sender.enqueue_data_for_lane(Bytes::from_static(b"ordinary"), FlowLane::Throughput);
    assert!(
        sender
            .enqueue_repair_frame_with_priority(
                Frame::StreamData {
                    stream_id,
                    offset: 64,
                    flags: StreamFlags::NONE,
                    payload: Bytes::from_static(b"repair"),
                },
                MuxLimits::default(),
                true,
            )
            .is_some()
    );

    let repair_dispatch = sender
        .dispatch_next(
            &path_stream,
            &mut send_stream,
            FlowLane::Throughput,
            MuxLimits::default(),
        )
        .expect("dispatch repair");
    assert_eq!(repair_dispatch.lane, ReliableWorkClass::Repair);
    assert_eq!(send_stream.next_offset(), 0);
    assert!(matches!(
        recv_emitted_tcp_path_command(&mut receivers).await,
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            offset: 64,
            payload,
            ..
        })) if payload == Bytes::from_static(b"repair")
    ));

    let data_dispatch = sender
        .dispatch_next(
            &path_stream,
            &mut send_stream,
            FlowLane::Throughput,
            MuxLimits::default(),
        )
        .expect("dispatch ordinary data");
    assert_eq!(data_dispatch.lane, ReliableWorkClass::Data);
    assert_eq!(send_stream.next_offset(), b"ordinary".len() as u64);
    assert!(matches!(
        recv_emitted_tcp_path_command(&mut receivers).await,
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            offset: 0,
            payload,
            ..
        })) if payload == Bytes::from_static(b"ordinary")
    ));
}

#[test]
fn reliable_stream_frame_queue_tracks_relay_chunk_byte_budget() {
    let mux_limits = MuxLimits {
        max_payload_bytes: 1024 * 1024,
        max_ack_ranges: 256,
        max_streams: 1024,
        max_quic_concurrent_bidi_streams: 1024,
        max_stream_window_bytes: 16 * 1024 * 1024,
        max_repair_bytes: 16 * 1024 * 1024,
        max_reorder_bytes: 16 * 1024 * 1024,
        max_datagram_queue_bytes: 4 * 1024 * 1024,
        max_path_flight_bytes: 4 * 1024 * 1024,
        max_reliable_relay_chunk_bytes: 256 * 1024,
        tcp_path_heartbeat_interval: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_INTERVAL,
        tcp_path_heartbeat_timeout: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_TIMEOUT,
    };

    assert_eq!(
        reliable_stream_frame_queue(mux_limits),
        (mux_limits.max_reorder_bytes / mux_limits.max_reliable_relay_chunk_bytes)
            + reliable_path_priority_headroom_frames()
    );
}

#[test]
fn reliable_stream_frame_queue_tracks_actual_attachment_payload() {
    let mux_limits = MuxLimits {
        max_payload_bytes: 1024 * 1024,
        max_ack_ranges: 256,
        max_streams: 1024,
        max_quic_concurrent_bidi_streams: 1024,
        max_stream_window_bytes: 16 * 1024 * 1024,
        max_repair_bytes: 16 * 1024 * 1024,
        max_reorder_bytes: 16 * 1024 * 1024,
        max_datagram_queue_bytes: 4 * 1024 * 1024,
        max_path_flight_bytes: 4 * 1024 * 1024,
        max_reliable_relay_chunk_bytes: 256 * 1024,
        tcp_path_heartbeat_interval: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_INTERVAL,
        tcp_path_heartbeat_timeout: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_TIMEOUT,
    };

    let stream_payload_queue = reliable_stream_frame_queue(mux_limits);
    let packet_payload_queue = reliable_stream_frame_queue_for_payload(mux_limits, 1200);

    assert_eq!(stream_payload_queue, 68);
    assert_eq!(
        packet_payload_queue,
        mux_limits.max_reorder_bytes / 1200 + reliable_path_priority_headroom_frames()
    );
    assert!(packet_payload_queue > stream_payload_queue);
}

#[test]
fn reliable_path_command_queue_tracks_inflight_budget_not_stream_limit() {
    let mux_limits = MuxLimits {
        max_payload_bytes: 1024 * 1024,
        max_ack_ranges: 256,
        max_streams: 1024,
        max_quic_concurrent_bidi_streams: 1024,
        max_stream_window_bytes: 16 * 1024 * 1024,
        max_repair_bytes: 16 * 1024 * 1024,
        max_reorder_bytes: 16 * 1024 * 1024,
        max_datagram_queue_bytes: 4 * 1024 * 1024,
        max_path_flight_bytes: 4 * 1024 * 1024,
        max_reliable_relay_chunk_bytes: 256 * 1024,
        tcp_path_heartbeat_interval: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_INTERVAL,
        tcp_path_heartbeat_timeout: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_TIMEOUT,
    };
    let frame_payload =
        reliable_relay_scheduler_quantum_cap(None, FlowLane::Throughput, mux_limits)
            .min(mux_limits.max_reliable_relay_chunk_bytes)
            .min(mux_limits.max_payload_bytes)
            .max(1);
    let expected_queue = (mux_limits.max_path_flight_bytes.div_ceil(frame_payload)
        + reliable_path_priority_headroom_frames())
    .min(reliable_path_writer_frame_queue_for_payload(
        mux_limits,
        frame_payload,
    ));
    assert_eq!(reliable_path_command_queue(mux_limits), expected_queue);

    let resources = ResourceLimits {
        max_streams: 65_536,
        max_quic_concurrent_bidi_streams: 65_536,
        max_path_flight_bytes: mux_limits.max_path_flight_bytes,
        max_reliable_relay_chunk_bytes: mux_limits.max_reliable_relay_chunk_bytes,
        ..ResourceLimits::default()
    };
    assert_eq!(tcp_session_command_queue(resources), expected_queue);
}

#[test]
fn reliable_path_command_queue_tracks_actual_payload_quantum() {
    let mux_limits = MuxLimits {
        max_payload_bytes: 1024 * 1024,
        max_ack_ranges: 256,
        max_streams: 1024,
        max_quic_concurrent_bidi_streams: 1024,
        max_stream_window_bytes: 16 * 1024 * 1024,
        max_repair_bytes: 16 * 1024 * 1024,
        max_reorder_bytes: 16 * 1024 * 1024,
        max_datagram_queue_bytes: 4 * 1024 * 1024,
        max_path_flight_bytes: 4 * 1024 * 1024,
        max_reliable_relay_chunk_bytes: 256 * 1024,
        tcp_path_heartbeat_interval: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_INTERVAL,
        tcp_path_heartbeat_timeout: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_TIMEOUT,
    };

    let stream_payload_queue = reliable_path_command_queue(mux_limits);
    let packet_payload_queue = reliable_path_command_queue_for_payload(mux_limits, 1200);

    let tcp_frame_payload =
        reliable_relay_scheduler_quantum_cap(None, FlowLane::Throughput, mux_limits)
            .min(mux_limits.max_reliable_relay_chunk_bytes)
            .min(mux_limits.max_payload_bytes)
            .max(1);
    assert_eq!(
        stream_payload_queue,
        (mux_limits.max_path_flight_bytes.div_ceil(tcp_frame_payload)
            + reliable_path_priority_headroom_frames())
        .min(reliable_path_writer_frame_queue_for_payload(
            mux_limits,
            tcp_frame_payload,
        ))
    );
    assert_eq!(
        packet_payload_queue,
        (mux_limits.max_path_flight_bytes.div_ceil(1200)
            + reliable_path_priority_headroom_frames())
        .min(reliable_path_writer_frame_queue_for_payload(
            mux_limits, 1200,
        ))
    );
    assert!(packet_payload_queue > stream_payload_queue);
}

#[test]
fn reliable_flow_demand_promotes_lane_after_runtime_bdp_threshold() {
    let mux_limits = MuxLimits::default();
    let path = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 20.0, 30_000_000.0);
    let threshold = reliable_flow_bulk_threshold_bytes(Some(path), mux_limits);
    let high_bdp_path = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 180.0, 300_000_000.0);
    let high_bdp_threshold = reliable_flow_bulk_threshold_bytes(Some(high_bdp_path), mux_limits);
    let high_bdp =
        ((high_bdp_path.delivery_rate_bps / 8.0) * (high_bdp_path.srtt_ms / 1000.0)).ceil() as u64;
    let mut state = ReliableRelayFlowDemandTracker::new();

    assert!(
        threshold
            >= reliable_relay_scheduler_quantum_cap(Some(path), FlowLane::Throughput, mux_limits)
                as u64
    );
    assert_eq!(high_bdp_threshold, high_bdp);

    let small_flow_bytes =
        (relay_lane_startup_chunk_bytes(FlowLane::Throughput, mux_limits) as u64 / 2).max(1);
    let before = state.refresh(
        ReliableRelayFlowSignals::new(small_flow_bytes, 0, 0),
        Some(path),
        mux_limits,
    );
    assert_eq!(before.demand.lane, FlowLane::Latency);
    assert!(!before.promoted_to_throughput);
    assert!(before.demand.latency_weight_ppm > 0);
    assert!(before.demand.throughput_weight_ppm < FlowDemand::PPM_MAX);

    let after = state.refresh(
        ReliableRelayFlowSignals::new(threshold, 0, 0),
        Some(path),
        mux_limits,
    );
    assert_eq!(after.demand.lane, FlowLane::Throughput);
    assert!(after.promoted_to_throughput);
    assert_eq!(after.demand.latency_weight_ppm, 0);
    assert_eq!(after.demand.throughput_weight_ppm, FlowDemand::PPM_MAX);

    let steady = state.refresh(
        ReliableRelayFlowSignals::new(threshold.saturating_mul(2), 0, 0),
        Some(path),
        mux_limits,
    );
    assert_eq!(steady.demand.lane, FlowLane::Throughput);
    assert!(!steady.promoted_to_throughput);
    assert_eq!(steady.demand.throughput_weight_ppm, FlowDemand::PPM_MAX);
}

#[test]
fn adaptive_tcp_budgets_expand_for_bulk_and_shrink_under_instability() {
    let mux_limits = MuxLimits {
        max_reliable_relay_chunk_bytes: 1024 * 1024,
        ..MuxLimits::default()
    };
    let stable = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 120.0, 300_000_000.0);
    let mut unstable = stable;
    unstable.loss_rate = 0.25;
    unstable.jitter_ms = 120.0;
    unstable.queue_bytes = 8 * 1024 * 1024;

    let interactive_chunk =
        adaptive_reliable_relay_chunk_bytes(Some(stable), FlowLane::Latency, mux_limits);
    let bulk_chunk =
        adaptive_reliable_relay_chunk_bytes(Some(stable), FlowLane::Throughput, mux_limits);
    let unstable_bulk_chunk =
        adaptive_reliable_relay_chunk_bytes(Some(unstable), FlowLane::Throughput, mux_limits);
    assert!(bulk_chunk > interactive_chunk);
    assert_eq!(
        unstable_bulk_chunk, bulk_chunk,
        "TCP bulk congestion is governed by kernel backpressure and the inflight gate; the application record quantum stays at the BBR feed unit"
    );

    let interactive_inflight =
        adaptive_reliable_relay_inflight_bytes(Some(stable), FlowLane::Latency, mux_limits);
    let bulk_inflight =
        adaptive_reliable_relay_inflight_bytes(Some(stable), FlowLane::Throughput, mux_limits);
    let mut stable_with_flight = stable;
    stable_with_flight.bytes_in_flight =
        ((stable.delivery_rate_bps / 8.0) * (stable.srtt_ms / 1000.0)).ceil() as u64;
    let bulk_inflight_with_flight = adaptive_reliable_relay_inflight_bytes(
        Some(stable_with_flight),
        FlowLane::Throughput,
        mux_limits,
    );
    let unstable_bulk_inflight =
        adaptive_reliable_relay_inflight_bytes(Some(unstable), FlowLane::Throughput, mux_limits);
    assert!(bulk_inflight >= interactive_inflight);
    assert_eq!(
        bulk_inflight_with_flight, bulk_inflight,
        "in-flight bytes are the controlled BDP-scale flight, not queue pressure"
    );
    assert!(
        interactive_inflight <= reliable_relay_buffer_len(mux_limits),
        "interactive streams should not inherit the bulk path ceiling"
    );
    assert!(
        bulk_inflight >= interactive_inflight.saturating_mul(8),
        "bulk transfer should be able to ramp far beyond interactive budget on high-BDP paths"
    );
    assert!(unstable_bulk_inflight < bulk_inflight);
}

#[test]
fn reliable_bulk_quantum_keeps_tcp_and_quic_streams_fed_without_rate_prior() {
    let mux_limits = MuxLimits::default();
    let unknown_tcp = PathSnapshot::new(
        PathId(0),
        UnderlayProtocol::Tcp,
        default_path_srtt_ms(UnderlayProtocol::Tcp),
        default_path_rate_bps(UnderlayProtocol::Tcp),
    );
    let unknown_udp = PathSnapshot::new(
        PathId(1),
        UnderlayProtocol::Udp,
        default_path_srtt_ms(UnderlayProtocol::Udp),
        default_path_rate_bps(UnderlayProtocol::Udp),
    );

    assert_eq!(
        adaptive_reliable_relay_chunk_bytes(Some(unknown_tcp), FlowLane::Throughput, mux_limits),
        BBR_MAX_SEND_QUANTUM_BYTES.min(reliable_relay_buffer_len(mux_limits))
    );
    assert_eq!(
        adaptive_reliable_relay_chunk_bytes(Some(unknown_udp), FlowLane::Throughput, mux_limits),
        BBR_MAX_SEND_QUANTUM_BYTES.min(reliable_relay_buffer_len(mux_limits)),
        "QUIC packet pacing is below the product sender; reliable UDP bulk must not self-limit to a 2*MSS product record"
    );
    assert_eq!(
        adaptive_reliable_relay_chunk_bytes(Some(unknown_tcp), FlowLane::Latency, mux_limits),
        PATH_OPEN_SCORE_BYTES
    );
    assert_eq!(
        adaptive_reliable_relay_chunk_bytes(Some(unknown_udp), FlowLane::Latency, mux_limits),
        PATH_OPEN_SCORE_BYTES
    );
}

#[test]
fn unknown_path_startup_inflight_uses_default_bdp_not_configured_ceiling() {
    let mux_limits = MuxLimits::default();
    let startup = adaptive_reliable_relay_inflight_bytes(None, FlowLane::Throughput, mux_limits);
    let default_bbr_target = (reliable_startup_bdp_bytes() * BBR_DEFAULT_CWND_GAIN)
        .max(reliable_startup_send_quantum_bytes() as f64)
        .max(bbr_min_pipe_cwnd_bytes(mux_limits) as f64)
        .ceil() as usize;

    assert_eq!(
        startup,
        default_bbr_target.max(reliable_relay_buffer_len(mux_limits))
    );
    assert!(
        startup < mux_limits.max_path_flight_bytes,
        "configured inflight is a ceiling, not an unknown-path startup target"
    );
}

#[test]
fn carrier_inflight_evidence_does_not_cap_product_source_read_horizon() {
    let mux_limits = MuxLimits::default();
    let mut path = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 80.0, 4_000_000_000.0);
    path.inflight_limit_bytes = 1024 * 1024;

    let inflight =
        adaptive_reliable_relay_inflight_bytes(Some(path), FlowLane::Throughput, mux_limits);

    assert!(inflight > path.inflight_limit_bytes as usize);
    assert!(
        inflight <= mux_limits.max_path_flight_bytes,
        "carrier cwnd is a carrier emission gate, not a product source-read cap"
    );
}

#[test]
fn product_progress_does_not_downshift_source_read_below_carrier_evidence() {
    let mux_limits = MuxLimits::default();
    let mut path = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 80.0, 4_000_000_000.0);
    path.pacing_rate_bps = 4_000_000_000.0;
    path.inflight_limit_bytes = mux_limits.max_path_flight_bytes as u64;
    path.product_progress_rate_bps = Some(160_000_000.0);

    let inflight =
        adaptive_reliable_relay_inflight_bytes(Some(path), FlowLane::Throughput, mux_limits);

    assert_eq!(inflight, mux_limits.max_path_flight_bytes);
    assert_eq!(
        path.delivery_rate_bps, 4_000_000_000.0,
        "carrier rate remains carrier evidence; product progress is a separate field"
    );
}

#[test]
fn udp_source_read_startup_can_fill_reliable_carrier_without_double_cwnd() {
    let mux_limits = MuxLimits::default();
    let mut path = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 50.0, 4_000_000_000.0);
    path.pacing_rate_bps = 4_000_000_000.0;
    path.inflight_limit_bytes = mux_limits.max_path_flight_bytes as u64;

    let inflight =
        adaptive_reliable_relay_inflight_bytes(Some(path), FlowLane::Throughput, mux_limits);

    let expected = ((path.delivery_rate_bps / 8.0) * (path.srtt_ms / 1000.0) * 2.0) as usize;
    assert_eq!(inflight, expected);
    assert!(inflight < mux_limits.max_path_flight_bytes);
}

#[test]
fn sender_dispatch_budget_batches_bounded_bulk_quanta() {
    let mux_limits = MuxLimits::default();
    let adaptive_chunk = 64 * 1024;
    let inflight_limit = 8 * reliable_relay_buffer_len(mux_limits);
    let queue_limit = inflight_limit;

    let (latency_bytes, latency_items) = reliable_relay_sender_dispatch_budget(
        mux_limits,
        FlowLane::Latency,
        adaptive_chunk,
        inflight_limit,
        queue_limit,
    );
    assert_eq!(latency_bytes, adaptive_chunk);
    assert_eq!(latency_items, 1);

    let (bulk_bytes, bulk_items) = reliable_relay_sender_dispatch_budget(
        mux_limits,
        FlowLane::Throughput,
        adaptive_chunk,
        inflight_limit,
        queue_limit,
    );
    assert_eq!(bulk_bytes, reliable_relay_buffer_len(mux_limits));
    assert_eq!(
        bulk_items,
        reliable_relay_buffer_len(mux_limits) / adaptive_chunk
    );
    assert!(bulk_bytes < inflight_limit);
}

#[test]
fn path_writer_coalesces_partial_bulk_run_without_delaying_full_or_empty_runs() {
    let mux_limits = MuxLimits::default();
    let byte_budget = reliable_path_command_writer_run_budget_bytes(mux_limits);
    let item_budget = reliable_path_command_writer_run_budget_items(mux_limits);

    assert!(
        reliable_path_writer_should_coalesce_partial_bulk_run(
            1,
            64 * 1024,
            byte_budget,
            item_budget
        ),
        "a single queued bulk frame should yield once so the producer can enqueue the rest of its service burst"
    );
    assert!(
        !reliable_path_writer_should_coalesce_partial_bulk_run(0, 0, byte_budget, item_budget),
        "an empty writer run must not spin"
    );
    assert!(
        !reliable_path_writer_should_coalesce_partial_bulk_run(
            item_budget,
            64 * 1024,
            byte_budget,
            item_budget
        ),
        "a full item-budget writer run should flush immediately"
    );
    assert!(
        !reliable_path_writer_should_coalesce_partial_bulk_run(
            1,
            byte_budget,
            byte_budget,
            item_budget
        ),
        "a full byte-budget writer run should flush immediately"
    );
}

#[test]
fn noninterlocked_tcp_writer_run_is_one_feedback_preemption_quantum() {
    let mux_limits = MuxLimits::default();
    let byte_budget = reliable_noninterlocked_tcp_writer_run_budget_bytes(mux_limits);
    let item_budget = reliable_path_command_writer_run_budget_items(mux_limits);

    assert_eq!(byte_budget, BBR_MAX_SEND_QUANTUM_BYTES);
    assert!(!reliable_path_writer_should_coalesce_partial_bulk_run(
        1,
        byte_budget,
        byte_budget,
        item_budget,
    ));
}

#[test]
fn path_writer_budget_counts_encoded_payload_and_variable_control_frames() {
    let payload = Bytes::from(vec![0x5a; BBR_MAX_SEND_QUANTUM_BYTES]);
    let frames = [
        Frame::DatagramData {
            flow_id: DatagramFlowId(1),
            datagram_id: DatagramId(1),
            ttl_ms: 1_000,
            payload: payload.clone(),
        },
        Frame::PathMtuProbe {
            path_id: PathId(1),
            probe_id: 1,
            payload: payload.clone(),
        },
        Frame::PathProofData {
            path_id: PathId(1),
            proof_id: 1,
            payload: payload.clone(),
        },
        Frame::PathCapacityData {
            path_id: PathId(1),
            calibration_id: 1,
            payload,
        },
    ];

    for frame in frames {
        assert!(
            reliable_path_command_writer_run_bytes(&ReliablePathCommand::SendFrame(frame))
                >= BBR_MAX_SEND_QUANTUM_BYTES + crate::protocol::codec::FRAME_HEADER_LEN,
        );
    }
    let ack = Frame::StreamAck {
        stream_id: StreamId(1),
        complete: false,
        ranges: (0..MuxLimits::default().max_ack_ranges)
            .map(|index| OffsetRange {
                start: (index as u64) * 2,
                end: (index as u64) * 2 + 1,
            })
            .collect(),
    };
    assert!(
        reliable_path_command_writer_run_bytes(&ReliablePathCommand::SendFrame(ack))
            > MuxLimits::default().max_ack_ranges * 16
    );
}

#[test]
fn capacity_frames_require_explicit_typed_carrier_commands() {
    let frames = [
        (
            Frame::PathCapacityData {
                path_id: PathId(3),
                calibration_id: 9,
                payload: Bytes::from_static(b"carrier-capacity"),
            },
            FlowLane::Throughput,
            b"carrier-capacity".len(),
        ),
        (
            Frame::PathCapacityFinish {
                path_id: PathId(3),
                calibration_id: 9,
                payload_bytes: 16,
            },
            FlowLane::Throughput,
            0,
        ),
        (
            Frame::PathCapacityReceipt {
                path_id: PathId(3),
                calibration_id: 9,
                received_payload_bytes: 16,
            },
            FlowLane::Control,
            0,
        ),
    ];
    let (commands, _receivers) = reliable_path_command_channels(1);
    for (frame, expected_lane, expected_pacing_bytes) in frames {
        assert!(reliable_path_frame_requires_capacity_command(&frame));
        assert_eq!(
            reliable_path_effective_frame_lane(&frame, FlowLane::Throughput),
            expected_lane
        );
        assert_eq!(frame_pacing_bytes(&frame), expected_pacing_bytes);
        assert_eq!(reliable_stream_frame_extent(&frame), None);
        assert!(matches!(
            commands.try_enqueue_admitted_frame(frame, FlowLane::Throughput),
            Err(RuntimeError::Protocol(_))
        ));
    }
}

#[test]
fn reliable_relay_chunking_uses_product_payload_envelope() {
    let mux_limits = MuxLimits {
        max_reliable_relay_chunk_bytes: 64 * 1024,
        max_ack_ranges: 16,
        ..MuxLimits::default()
    };
    let max_frame_payload = CodecLimits::default()
        .max_payload_bytes
        .max(1)
        .min(mux_limits.max_reliable_relay_chunk_bytes)
        .max(1);

    let latency_chunk = adaptive_reliable_relay_chunk_bytes_with_frame_limit(
        None,
        FlowLane::Latency,
        mux_limits,
        max_frame_payload,
    );
    let bulk_chunk = adaptive_reliable_relay_chunk_bytes_with_frame_limit(
        None,
        FlowLane::Throughput,
        mux_limits,
        max_frame_payload,
    );
    let fast = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 50.0, 2_000_000_000.0);
    let fast_bulk_chunk = adaptive_reliable_relay_chunk_bytes_with_frame_limit(
        Some(fast),
        FlowLane::Throughput,
        mux_limits,
        max_frame_payload,
    );

    assert_eq!(
        latency_chunk,
        adaptive_reliable_relay_chunk_bytes(None, FlowLane::Latency, mux_limits)
            .min(max_frame_payload)
            .max(1)
    );
    assert_eq!(
        bulk_chunk,
        adaptive_reliable_relay_chunk_bytes(None, FlowLane::Throughput, mux_limits)
            .min(max_frame_payload)
            .max(1)
    );
    assert_eq!(
        fast_bulk_chunk,
        adaptive_reliable_relay_chunk_bytes(Some(fast), FlowLane::Throughput, mux_limits)
            .min(max_frame_payload)
            .max(1)
    );
    assert!(latency_chunk <= max_frame_payload);
    assert!(bulk_chunk <= max_frame_payload);
    assert!(
        fast_bulk_chunk
            <= reliable_relay_scheduler_quantum_cap(Some(fast), FlowLane::Throughput, mux_limits)
    );
}

#[test]
fn reliable_relay_stall_timeout_is_transport_pto_derived() {
    let low_latency = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 20.0, 30_000_000.0);
    let mut cross_continent =
        PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 900.0, 300_000_000.0);
    cross_continent.jitter_ms = 400.0;

    assert_eq!(
        reliable_relay_stall_timeout(Some(low_latency), FlowLane::Latency),
        transport_pto_from_snapshot(Some(low_latency))
    );
    assert_eq!(
        reliable_relay_stall_timeout(Some(cross_continent), FlowLane::Throughput),
        transport_pto_from_snapshot(Some(cross_continent))
    );
    assert!(
        reliable_relay_stall_timeout(Some(low_latency), FlowLane::Latency) < Duration::from_secs(5)
    );
}

#[test]
fn reliable_stream_recv_progress_resend_tracks_received_state() {
    let mux_limits = MuxLimits::default();
    let mut recv_stream = ReliableRecvStream::new(StreamId(21), mux_limits);
    let low_latency = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 20.0, 30_000_000.0);
    let cross_continent = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 900.0, 300_000_000.0);

    assert!(!reliable_relay_recv_progress_resend_active(
        &recv_stream,
        true,
        Some(UnderlayProtocol::Udp),
    ));

    recv_stream
        .receive_data(1024, Bytes::from_static(b"late"), StreamFlags::NONE)
        .expect("out-of-order data");
    assert!(reliable_relay_recv_progress_resend_active(
        &recv_stream,
        true,
        Some(UnderlayProtocol::Udp),
    ));
    assert!(reliable_relay_recv_progress_resend_active(
        &recv_stream,
        true,
        Some(UnderlayProtocol::Tcp),
    ));
    assert!(!reliable_relay_recv_progress_resend_active(
        &recv_stream,
        false,
        Some(UnderlayProtocol::Udp),
    ));

    let mut contiguous = ReliableRecvStream::new(StreamId(22), mux_limits);
    contiguous
        .receive_data(0, Bytes::from_static(b"head"), StreamFlags::NONE)
        .expect("contiguous data");
    assert!(reliable_relay_recv_progress_resend_active(
        &contiguous,
        true,
        Some(UnderlayProtocol::Udp),
    ));
    assert!(!reliable_relay_recv_progress_resend_active(
        &contiguous,
        true,
        Some(UnderlayProtocol::Tcp),
    ));

    let low_interval = reliable_stream_recv_progress_interval(Some(low_latency), FlowLane::Latency);
    let high_interval =
        reliable_stream_recv_progress_interval(Some(cross_continent), FlowLane::Throughput);
    assert_eq!(
        low_interval,
        (transport_pto_from_snapshot(Some(low_latency)) / 2).max(QUIC_TIMER_GRANULARITY)
    );
    assert!(high_interval >= low_interval);
    assert_eq!(
        high_interval,
        (transport_pto_from_snapshot(Some(cross_continent)) / 2).max(QUIC_TIMER_GRANULARITY)
    );
}

#[test]
fn sender_service_retry_delay_is_ack_paced_not_one_millisecond_spin() {
    let low_latency = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 20.0, 30_000_000.0);
    let cross_continent = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 900.0, 300_000_000.0);

    assert!(
        reliable_stream_recv_progress_interval(Some(cross_continent), FlowLane::Throughput)
            > Duration::from_millis(100)
    );
    let low_retry = sender_service_retry_delay(Some(low_latency), FlowLane::Throughput);
    let high_retry = sender_service_retry_delay(Some(cross_continent), FlowLane::Throughput);
    assert!(
        low_retry > QUIC_TIMER_GRANULARITY,
        "blocked sender retry must not spin at timer granularity"
    );
    assert!(
        high_retry >= low_retry,
        "higher RTT paths should not retry more aggressively than low-latency paths"
    );
    assert!(
        high_retry <= QUIC_MAX_ACK_DELAY,
        "retry remains capped so missed capacity notifications do not stall the sender"
    );
}

#[test]
fn bulk_product_window_is_configured_memory_authority_not_path_proof() {
    let mux_limits = MuxLimits::default();
    let tcp_initial = reliable_stream_initial_advertised_window_bytes(
        UnderlayProtocol::Tcp,
        FlowLane::Throughput,
        mux_limits,
    );
    let udp_initial = reliable_stream_initial_advertised_window_bytes(
        UnderlayProtocol::Udp,
        FlowLane::Throughput,
        mux_limits,
    );

    assert_eq!(tcp_initial, mux_limits.max_stream_window_bytes);
    assert_eq!(udp_initial, mux_limits.max_stream_window_bytes);

    let snapshot = PathSnapshot::new(PathId(7), UnderlayProtocol::Udp, 40.0, 200_000_000.0);
    let measured_window =
        reliable_stream_advertised_window_bytes(Some(snapshot), FlowLane::Throughput, mux_limits);

    assert_eq!(measured_window, mux_limits.max_stream_window_bytes);
    assert!(
        reliable_stream_initial_advertised_window_bytes(
            UnderlayProtocol::Udp,
            FlowLane::Latency,
            mux_limits,
        ) < udp_initial,
        "latency QUIC retains its bounded startup product window"
    );
}

#[test]
fn reliable_recv_progress_batches_max_data_updates() {
    let mux_limits = MuxLimits {
        max_payload_bytes: 1024,
        max_reliable_relay_chunk_bytes: 1024,
        max_path_flight_bytes: 4096,
        max_stream_window_bytes: 4096,
        max_repair_bytes: 4096,
        max_reorder_bytes: 4096,
        ..MuxLimits::default()
    };
    let mut recv_stream = ReliableRecvStream::new(StreamId(22), mux_limits);
    let mut progress = ReliableRecvProgress::default();
    let window = reliable_stream_advertised_window_bytes(None, FlowLane::Throughput, mux_limits);
    let step = reliable_stream_max_data_update_bytes(window, mux_limits);

    assert_eq!(step, 1024);
    assert!(progress.should_send_max_data(
        &recv_stream,
        None,
        FlowLane::Throughput,
        mux_limits,
        false
    ));
    assert!(!progress.should_send_max_data(
        &recv_stream,
        None,
        FlowLane::Throughput,
        mux_limits,
        false
    ));

    recv_stream
        .receive_data(0, Bytes::from(vec![0x11; 512]), StreamFlags::NONE)
        .expect("half-step data");
    assert!(!progress.should_send_max_data(
        &recv_stream,
        None,
        FlowLane::Throughput,
        mux_limits,
        false
    ));

    recv_stream
        .receive_data(512, Bytes::from(vec![0x22; 512]), StreamFlags::NONE)
        .expect("full-step data");
    assert!(progress.should_send_max_data(
        &recv_stream,
        None,
        FlowLane::Throughput,
        mux_limits,
        false
    ));
    assert!(progress.should_send_max_data(
        &recv_stream,
        None,
        FlowLane::Throughput,
        mux_limits,
        true
    ));
}

#[test]
fn reliable_recv_progress_batches_bulk_acks_by_repair_release_cadence() {
    let mux_limits = MuxLimits {
        max_ack_ranges: 16,
        max_stream_window_bytes: 64 * 1024,
        max_repair_bytes: 64 * 1024,
        max_reorder_bytes: 64 * 1024,
        max_path_flight_bytes: 64 * 1024,
        max_reliable_relay_chunk_bytes: 64 * 1024,
        ..MuxLimits::default()
    };
    let mut recv_stream = ReliableRecvStream::new(StreamId(23), mux_limits);
    let mut progress = ReliableRecvProgress::default();
    let ack_step = reliable_stream_ack_update_bytes(None, FlowLane::Throughput, mux_limits);

    assert_eq!(ack_step, mux_limits.max_repair_bytes as u64 / 4);
    recv_stream
        .receive_data(0, Bytes::from(vec![0x11; 1024]), StreamFlags::NONE)
        .expect("first data");
    assert!(progress.should_send_ack(&recv_stream, None, FlowLane::Throughput, mux_limits, false,));

    recv_stream
        .receive_data(1024, Bytes::from(vec![0x22; 1024]), StreamFlags::NONE)
        .expect("below ack step");
    assert!(
        !progress.should_send_ack(&recv_stream, None, FlowLane::Throughput, mux_limits, false,)
    );

    recv_stream
        .receive_data(
            2048,
            Bytes::from(vec![0x33; ack_step as usize]),
            StreamFlags::NONE,
        )
        .expect("past ack step");
    assert!(progress.should_send_ack(&recv_stream, None, FlowLane::Throughput, mux_limits, false,));
}

#[test]
fn reliable_recv_progress_acks_reorder_gap_without_waiting_for_bulk_step() {
    let mux_limits = MuxLimits {
        max_ack_ranges: 16,
        max_stream_window_bytes: 64 * 1024,
        max_repair_bytes: 64 * 1024,
        max_reorder_bytes: 64 * 1024,
        max_path_flight_bytes: 64 * 1024,
        max_reliable_relay_chunk_bytes: 64 * 1024,
        ..MuxLimits::default()
    };
    let mut recv_stream = ReliableRecvStream::new(StreamId(24), mux_limits);
    let mut progress = ReliableRecvProgress::default();

    recv_stream
        .receive_data(0, Bytes::from(vec![0x11; 1024]), StreamFlags::NONE)
        .expect("first data");
    assert!(progress.should_send_ack(&recv_stream, None, FlowLane::Throughput, mux_limits, false,));

    recv_stream
        .receive_data(8192, Bytes::from(vec![0x22; 1024]), StreamFlags::NONE)
        .expect("out-of-order data");
    assert!(progress.should_send_ack(&recv_stream, None, FlowLane::Throughput, mux_limits, false,));
}

#[test]
fn reliable_recv_progress_sends_exact_tcp_sparse_deltas_without_delaying_feedback() {
    let mux_limits = MuxLimits {
        max_ack_ranges: 16,
        max_stream_window_bytes: 64 * 1024,
        max_repair_bytes: 64 * 1024,
        max_reorder_bytes: 64 * 1024,
        max_path_flight_bytes: 64 * 1024,
        max_reliable_relay_chunk_bytes: 64 * 1024,
        ..MuxLimits::default()
    };
    let tcp = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 180.0, 500_000_000.0);
    let udp = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 180.0, 500_000_000.0);

    for (path, request_sparse, compact) in
        [(tcp, true, true), (tcp, false, false), (udp, true, false)]
    {
        let mut recv_stream = ReliableRecvStream::new(StreamId(24), mux_limits);
        let mut progress = ReliableRecvProgress::default();
        let mut sparse_progress = RequestTcpSparseAckProgress::default();
        recv_stream
            .receive_data(0, Bytes::from(vec![0x11; 1024]), StreamFlags::NONE)
            .expect("contiguous prefix");
        assert!(progress.should_send_ack(
            &recv_stream,
            Some(path),
            FlowLane::Throughput,
            mux_limits,
            false,
        ));
        assert_eq!(sparse_progress.ack_frames(&recv_stream, false).len(), 1);

        let mut frames = Vec::new();
        for offset in [8192, 32768, 16384, 12288] {
            recv_stream
                .receive_data(offset, Bytes::from(vec![0x22; 1024]), StreamFlags::NONE)
                .expect("sparse range");
            assert!(
                progress.should_send_ack(
                    &recv_stream,
                    Some(path),
                    FlowLane::Throughput,
                    mux_limits,
                    false,
                ),
                "range-shape feedback cadence must not be weakened"
            );
            frames = sparse_progress.ack_frames(
                &recv_stream,
                request_sparse && path.underlay == UnderlayProtocol::Tcp,
            );
        }
        assert_eq!(frames.len(), 1);
        let Frame::StreamAck {
            complete, ranges, ..
        } = &frames[0]
        else {
            panic!("receive progress must emit STREAM_ACK");
        };
        assert_eq!(*complete, !compact);
        assert_eq!(ranges.len(), if compact { 1 } else { 5 });
        assert_eq!(
            ranges.first().map(|range| range.start),
            Some(if compact { 12288 } else { 0 })
        );
        assert_eq!(
            ranges.last().map(|range| range.start),
            Some(if compact { 12288 } else { 32768 })
        );
        if compact {
            assert_eq!(ranges[0], OffsetRange::new(12288, 13312).unwrap());
        }
    }
}

#[test]
fn reliable_recv_progress_acks_repair_horizon_advancement() {
    let mux_limits = MuxLimits {
        max_ack_ranges: 16,
        max_stream_window_bytes: 64 * 1024,
        max_repair_bytes: 256 * 1024,
        max_reorder_bytes: 256 * 1024,
        max_path_flight_bytes: 256 * 1024,
        max_reliable_relay_chunk_bytes: 64 * 1024,
        ..MuxLimits::default()
    };
    let mut recv_stream = ReliableRecvStream::new(StreamId(25), mux_limits);
    let mut progress = ReliableRecvProgress::default();

    recv_stream
        .receive_data(0, Bytes::from(vec![0x11; 1024]), StreamFlags::NONE)
        .expect("head");
    recv_stream
        .receive_data(8192, Bytes::from(vec![0x22; 1024]), StreamFlags::NONE)
        .expect("first tail");
    assert!(progress.should_send_ack(&recv_stream, None, FlowLane::Throughput, mux_limits, false,));

    recv_stream
        .receive_data(9216, Bytes::from(vec![0x33; 1024]), StreamFlags::NONE)
        .expect("small tail extension");
    assert!(
        !progress.should_send_ack(&recv_stream, None, FlowLane::Throughput, mux_limits, false,),
        "small same-range horizon movement should be batched"
    );

    let ack_step = reliable_stream_ack_update_bytes(None, FlowLane::Throughput, mux_limits);
    assert!(
        ack_step > 1024,
        "test expects a bulk ACK step larger than one small chunk"
    );
    recv_stream
        .receive_data(
            10240,
            Bytes::from(vec![0x44; ack_step as usize]),
            StreamFlags::NONE,
        )
        .expect("meaningful tail extension");
    assert!(
        progress.should_send_ack(&recv_stream, None, FlowLane::Throughput, mux_limits, false,),
        "meaningful repair horizon advancement must be ACKed even when range count is unchanged"
    );
}

#[test]
fn reliable_recv_progress_default_bulk_ack_step_tracks_service_quantum() {
    let mux_limits = MuxLimits::default();
    let ack_step = reliable_stream_ack_update_bytes(None, FlowLane::Throughput, mux_limits);

    assert_eq!(ack_step, BBR_MAX_SEND_QUANTUM_BYTES as u64);
    let window = reliable_stream_advertised_window_bytes(None, FlowLane::Throughput, mux_limits);
    assert!(ack_step < reliable_stream_max_data_update_bytes(window, mux_limits));
    assert_eq!(
        reliable_stream_ack_update_bytes(None, FlowLane::Latency, mux_limits),
        1
    );
}

#[test]
fn reliable_relay_stall_watch_ignores_idle_streams_and_tracks_repairable_work() {
    let mux_limits = MuxLimits::default();
    let mut send_stream = ReliableSendStream::new(StreamId(11), mux_limits);
    let mut recv_stream = ReliableRecvStream::new(StreamId(11), mux_limits);

    assert!(!reliable_relay_stall_watch_active(
        &send_stream,
        &recv_stream,
        false,
        FlowLane::Latency,
        false,
        mux_limits
    ));
    assert!(!reliable_relay_stall_watch_active(
        &send_stream,
        &recv_stream,
        true,
        FlowLane::Latency,
        false,
        mux_limits
    ));

    send_stream
        .send_data(Bytes::from_static(b"request"), StreamFlags::NONE)
        .expect("request data");
    assert!(reliable_relay_stall_watch_active(
        &send_stream,
        &recv_stream,
        true,
        FlowLane::Latency,
        false,
        mux_limits
    ));
    send_stream.apply_ack(&[crate::protocol::OffsetRange { start: 0, end: 7 }]);
    assert!(!reliable_relay_stall_watch_active(
        &send_stream,
        &recv_stream,
        true,
        FlowLane::Latency,
        false,
        mux_limits
    ));
    assert!(reliable_relay_stall_watch_active(
        &send_stream,
        &recv_stream,
        true,
        FlowLane::Latency,
        true,
        mux_limits
    ));

    recv_stream
        .receive_data(0, Bytes::from_static(b"response"), StreamFlags::NONE)
        .expect("response data");
    assert!(!reliable_relay_stall_watch_active(
        &send_stream,
        &recv_stream,
        true,
        FlowLane::Latency,
        false,
        mux_limits
    ));
    assert!(reliable_relay_stall_watch_active(
        &send_stream,
        &recv_stream,
        true,
        FlowLane::Throughput,
        false,
        mux_limits
    ));
    assert!(!reliable_relay_stall_watch_active(
        &send_stream,
        &recv_stream,
        false,
        FlowLane::Throughput,
        true,
        mux_limits
    ));

    let response_watch_bytes = reliable_relay_response_stall_watch_bytes(mux_limits);
    assert_eq!(
        response_watch_bytes,
        reliable_relay_buffer_len(mux_limits) as u64
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
    assert!(reliable_relay_stall_watch_active(
        &send_stream,
        &recv_stream,
        true,
        FlowLane::Latency,
        false,
        mux_limits
    ));
}

#[test]
fn stream_ack_gap_repair_waits_for_persistent_gap_on_reliable_carriers() {
    let mux_limits = MuxLimits::default();
    let mut send_stream = ReliableSendStream::new(StreamId(31), mux_limits);
    send_stream
        .send_data(Bytes::from_static(b"aaaa"), StreamFlags::NONE)
        .expect("first chunk");
    send_stream
        .send_data(Bytes::from_static(b"bbbb"), StreamFlags::NONE)
        .expect("missing chunk");
    send_stream
        .send_data(Bytes::from_static(b"cccc"), StreamFlags::NONE)
        .expect("later chunk");
    let ranges = [
        OffsetRange { start: 0, end: 4 },
        OffsetRange { start: 8, end: 12 },
    ];

    assert!(
        stream_ack_gap_repair_frames(&send_stream, &ranges, usize::MAX, true, false, false,)
            .is_empty(),
        "a single reliable carrier must not replay product bytes over itself"
    );
    assert!(
        stream_ack_gap_repair_frames(&send_stream, &ranges, usize::MAX, true, false, true,)
            .is_empty(),
        "a single reliable carrier owns ordinary packet-loss recovery"
    );
    assert!(
        stream_ack_gap_repair_frames(&send_stream, &ranges, usize::MAX, true, true, false,)
            .is_empty(),
        "fresh multipath ACK gaps wait for persistent product-hole evidence"
    );
    let persistent_gap_repairs =
        stream_ack_gap_repair_frames(&send_stream, &ranges, usize::MAX, true, true, true);
    assert_eq!(
        persistent_gap_repairs.len(),
        1,
        "multipath repair may reinject authoritative product gaps over another path"
    );
    assert!(matches!(
        &persistent_gap_repairs[0],
        Frame::StreamData {
            offset: 4,
            payload,
            ..
        } if payload.as_ref() == b"bbbb"
    ));
    assert!(
        stream_ack_gap_repair_frames(&send_stream, &ranges, usize::MAX, false, false, false,)
            .is_empty(),
        "non-authoritative ACK snapshots must not infer missing holes"
    );
}

#[test]
fn ack_gap_repair_prefers_authoritative_gap_before_frontier_tail() {
    let mux_limits = MuxLimits::default();
    let mut send_stream = ReliableSendStream::new(StreamId(32), mux_limits);
    send_stream
        .send_data(Bytes::from_static(b"aaaa"), StreamFlags::NONE)
        .expect("first chunk");
    send_stream
        .send_data(Bytes::from_static(b"bbbb"), StreamFlags::NONE)
        .expect("second chunk");
    send_stream
        .send_data(Bytes::from_static(b"cccc"), StreamFlags::NONE)
        .expect("third chunk");

    let ranges = [OffsetRange { start: 4, end: 12 }];
    let _ = send_stream.apply_ack(&ranges);
    let repairs = stream_ack_gap_repair_frames(&send_stream, &ranges, usize::MAX, true, true, true);

    assert_eq!(repairs.len(), 1);
    assert!(matches!(
        &repairs[0],
        Frame::StreamData {
            offset: 0,
            payload,
            ..
        } if payload.as_ref() == b"aaaa"
    ));
}

#[test]
fn ack_gap_repair_ignores_contiguous_unacked_owner_tail() {
    let mux_limits = MuxLimits::default();
    let mut send_stream = ReliableSendStream::new(StreamId(33), mux_limits);
    send_stream
        .send_data(Bytes::from_static(b"aaaa"), StreamFlags::NONE)
        .expect("first chunk");
    send_stream
        .send_data(Bytes::from_static(b"bbbb"), StreamFlags::NONE)
        .expect("second chunk");
    send_stream
        .send_data(Bytes::from_static(b"cccc"), StreamFlags::NONE)
        .expect("third chunk");

    let ranges = [OffsetRange { start: 0, end: 4 }];
    let _ = send_stream.apply_ack(&ranges);
    let repairs = stream_ack_gap_repair_frames(&send_stream, &ranges, 6, true, true, true);

    assert!(
        repairs.is_empty(),
        "contiguous unacked owner tail is retained carrier flight, not ACK-gap repair"
    );
}

#[test]
fn tail_stall_repair_retransmits_same_frontier_only_after_stall_evidence() {
    let stream_id = StreamId(34);
    let (commands, _receivers) = reliable_path_command_channels(4);
    let binding = ResponseStreamBinding::new(
        SessionId(34),
        UnderlayProtocol::Tcp,
        PathId(0),
        commands,
        FlowLane::Throughput,
    );
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: 1024,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx,
    };
    let frame = Frame::StreamData {
        stream_id,
        offset: 128,
        flags: StreamFlags::NONE,
        payload: Bytes::from_static(b"frontier"),
    };
    binding.record_owner_flight(
        CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        },
        &frame,
    );
    let later_frame = Frame::StreamData {
        stream_id,
        offset: 136,
        flags: StreamFlags::NONE,
        payload: Bytes::from_static(b"later"),
    };

    let (without_stall, blocked_offset) = prefix_repair_frames_with_available_output(
        &path_stream,
        vec![frame.clone(), later_frame.clone()],
        false,
    );
    assert!(without_stall.is_empty());
    assert_eq!(blocked_offset, Some(128));

    let (with_stall, blocked_offset) =
        prefix_repair_frames_with_available_output(&path_stream, vec![frame, later_frame], true);
    assert_eq!(blocked_offset, None);
    assert_eq!(with_stall.len(), 1);
    assert!(matches!(
        &with_stall[0],
        Frame::StreamData {
            offset: 128,
            payload,
            ..
        } if payload.as_ref() == b"frontier"
    ));
}

#[test]
fn tcp_response_stall_anchor_uses_delivery_progress_not_control_progress() {
    let mux_limits = MuxLimits::default();
    let mut recv_stream = ReliableRecvStream::new(StreamId(12), mux_limits);
    let last_delivery = Instant::now();
    let control_progress = last_delivery + Duration::from_secs(30);

    assert_eq!(
        reliable_relay_stall_progress_anchor(
            control_progress,
            last_delivery,
            last_delivery,
            &recv_stream,
            true,
            FlowLane::Latency,
            false,
            mux_limits,
        ),
        control_progress
    );

    let response_watch_bytes = reliable_relay_response_stall_watch_bytes(mux_limits);
    recv_stream
        .receive_data(
            0,
            Bytes::from(vec![0u8; response_watch_bytes as usize]),
            StreamFlags::NONE,
        )
        .expect("sustained response data");

    assert_eq!(
        reliable_relay_stall_progress_anchor(
            control_progress,
            last_delivery,
            last_delivery,
            &recv_stream,
            true,
            FlowLane::Latency,
            false,
            mux_limits,
        ),
        last_delivery
    );

    let repair_progress = control_progress + Duration::from_secs(1);
    assert_eq!(
        reliable_relay_stall_progress_anchor(
            control_progress,
            last_delivery,
            repair_progress,
            &recv_stream,
            true,
            FlowLane::Latency,
            false,
            mux_limits,
        ),
        repair_progress
    );
}

#[test]
fn tcp_receive_hole_repair_tracks_buffered_ordering_gap() {
    let mux_limits = MuxLimits::default();
    let mut recv_stream = ReliableRecvStream::new(StreamId(14), mux_limits);

    assert!(!reliable_relay_receive_hole_repair_active(
        &recv_stream,
        true
    ));
    recv_stream
        .receive_data(0, Bytes::from_static(b"head"), StreamFlags::NONE)
        .expect("initial response data");
    assert!(!reliable_relay_receive_hole_repair_active(
        &recv_stream,
        true
    ));

    let out_of_order = recv_stream
        .receive_data(8, Bytes::from_static(b"tail"), StreamFlags::NONE)
        .expect("out-of-order response data");
    assert!(out_of_order.delivered.is_empty());
    assert!(reliable_relay_receive_hole_repair_active(
        &recv_stream,
        true
    ));
    assert!(!reliable_relay_receive_hole_repair_active(
        &recv_stream,
        false
    ));

    let hole_fill = recv_stream
        .receive_data(4, Bytes::from_static(b"gap!"), StreamFlags::NONE)
        .expect("hole fill response data");
    assert_eq!(hole_fill.delivered.len(), 2);
    assert!(!reliable_relay_receive_hole_repair_active(
        &recv_stream,
        true
    ));
}

#[test]
fn tcp_receive_hole_repair_deadline_is_progress_signal_not_path_victim_policy() {
    let now = Instant::now();
    let mut path = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 50.0, 100_000_000.0);
    path.jitter_ms = 5.0;
    path.inflight_limit_bytes = 1_000_000;

    let deadline = reliable_relay_receive_hole_repair_deadline(
        now,
        now - Duration::from_secs(1),
        Some(path),
        FlowLane::Throughput,
    );

    assert!(
        deadline > tokio::time::Instant::from_std(now),
        "receive-hole handling schedules ACK/progress repair; path failure is owned by carrier/stall evidence"
    );
}

#[test]
fn reliable_relay_attach_scoring_keeps_interactive_repairs_small() {
    let mux_limits = MuxLimits::default();
    let send_stream = ReliableSendStream::new(StreamId(12), mux_limits);

    assert_eq!(
        reliable_relay_attach_payload_bytes(&send_stream, FlowLane::Latency, mux_limits),
        PATH_OPEN_SCORE_BYTES
    );
    assert_eq!(
        reliable_relay_attach_payload_bytes(&send_stream, FlowLane::Throughput, mux_limits),
        reliable_relay_buffer_len(mux_limits)
    );
}

#[test]
fn reliable_relay_bulk_admission_payload_uses_preemptible_quantum_not_inflight_ceiling() {
    let mux_limits = MuxLimits::default();
    let send_stream = ReliableSendStream::new(StreamId(12), mux_limits);
    let expected_quantum =
        adaptive_reliable_relay_chunk_bytes(None, FlowLane::Throughput, mux_limits);

    assert_eq!(
        reliable_relay_bulk_striping_payload_bytes(&send_stream, mux_limits),
        expected_quantum
    );
    let validation_quantum = reliable_relay_bulk_validation_payload_bytes(&send_stream, mux_limits);
    assert!(validation_quantum >= PATH_OPEN_SCORE_BYTES);
    assert!(validation_quantum <= relay_lane_startup_chunk_bytes(FlowLane::Latency, mux_limits));
    assert!(validation_quantum <= expected_quantum);
    assert!(expected_quantum < mux_limits.max_path_flight_bytes);
}

#[test]
fn tcp_path_lane_classes_separate_latency_from_throughput_opens() {
    assert!(tcp_path_lane_uses_latency_session(FlowLane::Latency));
    assert!(tcp_path_lane_uses_latency_session(FlowLane::Control));
    assert!(tcp_path_lane_uses_latency_session(
        FlowLane::RealtimeDatagram
    ));
    assert!(!tcp_path_lane_uses_latency_session(FlowLane::Throughput));
    assert!(!tcp_path_lane_uses_latency_session(FlowLane::Background));
}

#[test]
fn acked_udp_datagram_timeout_suppresses_path_for_next_realtime_packet() {
    let resources = ResourceLimits::default();
    let context = ClientPathContext::new(
        vec![
            "udp://127.0.0.1:10000?srtt-ms=20&rate-mbps=200"
                .parse()
                .expect("path"),
            "udp://127.0.0.1:10001?srtt-ms=30&rate-mbps=200"
                .parse()
                .expect("path"),
        ],
        security(),
        resources,
    )
    .expect("context");
    let association = UdpDatagramClientAssociation::new(context.clone());
    let payload_bytes = 512;
    let ttl_ms = 1_000;
    let candidates = context.ordered_udp_path_candidates_for_ttl(payload_bytes, ttl_ms);
    assert_eq!(
        association.select_path_candidate(&candidates, &HashSet::new(), payload_bytes, ttl_ms),
        Some(0),
        "the lower-ETA realtime datagram path starts as the best path"
    );

    context.mark_udp_path_feedback(
        0,
        UdpDatagramPathObservation {
            rtt: Duration::from_millis(20),
            jitter: Duration::ZERO,
            loss_rate: 1.0,
            rate_sample: None,
        },
    );

    assert_eq!(
        association.select_path_candidate(&candidates, &HashSet::new(), payload_bytes, ttl_ms),
        Some(1),
        "an ACKed datagram response timeout is data-plane evidence; the next realtime packet should use the alternate path"
    );
}

#[test]
fn switchable_stream_demand_updates_from_local_sender_metrics() {
    let registry = Arc::new(ServerReliableStreamRegistry::new(
        ResourceLimits::default().max_streams,
    ));
    let target = TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80)));
    let path_registration =
        registry.register_carrier_path(SessionId(1), UnderlayProtocol::Tcp, PathId(0));
    let (commands, _rx) = reliable_path_command_channels(4);
    let mut stream = match registry
        .open_or_attach(
            ServerReliableStreamOpenRequest {
                session_id: SessionId(1),
                stream_id: StreamId(7),
                target: &target,
                lane: FlowLane::Latency,
                attachment: ServerReliablePathAttachment {
                    path_registration: path_registration.clone(),
                    commands,
                    max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
                    role: StreamOpenRole::Active,
                    initial_metrics: None,
                },
            },
            MuxLimits::default(),
            ResourceLimits::default().max_streams,
        )
        .expect("open stream")
    {
        ServerReliableStreamOpen::New(stream) => stream,
        ServerReliableStreamOpen::Existing => panic!("expected new stream"),
        ServerReliableStreamOpen::DuplicateLiveIgnored => {
            panic!("new active stream must not be treated as duplicate")
        }
        ServerReliableStreamOpen::Rejected => panic!("active stream open should not be rejected"),
    };
    assert_eq!(stream.current_lane(), FlowLane::Latency);
    let ReliablePathStreamOutput::Switchable(binding) = &stream.output else {
        panic!("expected switchable binding");
    };
    let binding = binding.clone();

    stream.set_lane(FlowLane::Throughput);

    assert_eq!(stream.current_lane(), FlowLane::Throughput);
    assert_eq!(binding.lane(), FlowLane::Throughput);
}

#[test]
fn server_registry_ignores_active_duplicate_same_path_input_without_output_replacement() {
    let registry = Arc::new(ServerReliableStreamRegistry::new(
        ResourceLimits::default().max_streams,
    ));
    let target = TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80)));
    let session_id = SessionId(1);
    let stream_id = StreamId(17);
    let first_path_registration =
        registry.register_carrier_path(session_id, UnderlayProtocol::Udp, PathId(0));
    let (first_commands, _first_rx) = reliable_path_command_channels(4);
    let opened = registry
        .open_or_attach(
            ServerReliableStreamOpenRequest {
                session_id,
                stream_id,
                target: &target,
                lane: FlowLane::Throughput,
                attachment: ServerReliablePathAttachment {
                    path_registration: first_path_registration.clone(),
                    commands: first_commands,
                    max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
                    role: StreamOpenRole::Active,
                    initial_metrics: None,
                },
            },
            MuxLimits::default(),
            ResourceLimits::default().max_streams,
        )
        .expect("open stream");
    assert!(matches!(opened, ServerReliableStreamOpen::New(_)));

    let (duplicate_commands, _duplicate_rx) = reliable_path_command_channels(4);
    let duplicate_path_registration =
        registry.register_carrier_path(session_id, UnderlayProtocol::Udp, PathId(0));
    let duplicate = registry
        .open_or_attach(
            ServerReliableStreamOpenRequest {
                session_id,
                stream_id,
                target: &target,
                lane: FlowLane::Throughput,
                attachment: ServerReliablePathAttachment {
                    path_registration: duplicate_path_registration.clone(),
                    commands: duplicate_commands,
                    max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
                    role: StreamOpenRole::Active,
                    initial_metrics: None,
                },
            },
            MuxLimits::default(),
            ResourceLimits::default().max_streams,
        )
        .expect("duplicate live attach should be handled");

    assert!(matches!(
        duplicate,
        ServerReliableStreamOpen::DuplicateLiveIgnored
    ));
    assert_eq!(registry.management_snapshot().active_streams, 1);
}

#[test]
fn server_response_output_inherits_open_path_startup_metrics() {
    let registry = Arc::new(ServerReliableStreamRegistry::new(
        ResourceLimits::default().max_streams,
    ));
    let target = TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80)));
    let path_registration =
        registry.register_carrier_path(SessionId(1), UnderlayProtocol::Tcp, PathId(0));
    let (commands, _rx) = reliable_path_command_channels(4);
    let path = "tcp://127.0.0.1:10000?srtt-ms=20&rate-mbps=500"
        .parse::<PathSpec>()
        .expect("path spec");
    let initial_metrics = path_startup_metrics(&path, 0, PathMetricDirection::ServerToClient);
    assert!(
        !initial_metrics.app_limited,
        "configured startup rate hints are advisory priors, not app-limited samples"
    );
    let stream = match registry
        .open_or_attach(
            ServerReliableStreamOpenRequest {
                session_id: SessionId(1),
                stream_id: StreamId(8),
                target: &target,
                lane: FlowLane::Throughput,
                attachment: ServerReliablePathAttachment {
                    path_registration: path_registration.clone(),
                    commands,
                    max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
                    role: StreamOpenRole::Active,
                    initial_metrics: Some(initial_metrics),
                },
            },
            MuxLimits::default(),
            ResourceLimits::default().max_streams,
        )
        .expect("open stream")
    {
        ServerReliableStreamOpen::New(stream) => stream,
        ServerReliableStreamOpen::Existing => panic!("expected new stream"),
        ServerReliableStreamOpen::DuplicateLiveIgnored => {
            panic!("new active stream must not be treated as duplicate")
        }
        ServerReliableStreamOpen::Rejected => panic!("active stream open should not be rejected"),
    };
    let snapshot = stream
        .send_path_snapshot(FlowLane::Throughput, 1)
        .expect("switchable output exposes seeded path model");

    assert_eq!(
        snapshot.delivery_rate_bps,
        default_path_rate_bps(UnderlayProtocol::Tcp)
    );
    assert_eq!(snapshot.srtt_ms, 20.0);
    assert!(
        adaptive_reliable_relay_chunk_bytes(
            Some(snapshot),
            FlowLane::Throughput,
            MuxLimits::default(),
        ) > bbr_min_send_quantum_bytes(MuxLimits::default()),
        "server response bytes keep the bulk feed quantum while startup metrics remain validation-only rate hints"
    );

    let ReliablePathStreamOutput::Switchable(binding) = &stream.output else {
        panic!("expected switchable output");
    };
    binding.record_owner_flight(
        CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        },
        &Frame::StreamData {
            stream_id: stream.stream_id,
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![0x22; PATH_OPEN_SCORE_BYTES]),
        },
    );
    let with_product_flight = stream
        .send_path_snapshot(FlowLane::Throughput, PATH_OPEN_SCORE_BYTES)
        .expect("switchable output exposes path model");
    assert_eq!(with_product_flight.bytes_in_flight, 0);
    assert_eq!(
        with_product_flight.product_bytes_in_flight,
        PATH_OPEN_SCORE_BYTES as u64
    );
    assert!(
        adaptive_reliable_relay_chunk_bytes(
            Some(with_product_flight),
            FlowLane::Throughput,
            MuxLimits::default(),
        ) > bbr_min_send_quantum_bytes(MuxLimits::default()),
        "product flight is admission/repair state, not carrier queue pressure"
    );
}

#[test]
fn server_reliable_registry_rejects_attach_only_unknown_stream() {
    let registry = Arc::new(ServerReliableStreamRegistry::new(
        ResourceLimits::default().max_streams,
    ));
    let target = TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80)));
    let path_registration =
        registry.register_carrier_path(SessionId(1), UnderlayProtocol::Tcp, PathId(1));
    let (commands, _rx) = reliable_path_command_channels(4);
    let opened = registry
        .open_or_attach(
            ServerReliableStreamOpenRequest {
                session_id: SessionId(1),
                stream_id: StreamId(99),
                target: &target,
                lane: FlowLane::Throughput,
                attachment: ServerReliablePathAttachment {
                    path_registration: path_registration.clone(),
                    commands,
                    max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
                    role: StreamOpenRole::Validation,
                    initial_metrics: None,
                },
            },
            MuxLimits::default(),
            ResourceLimits::default().max_streams,
        )
        .expect("attach-only open should be handled");
    assert!(matches!(opened, ServerReliableStreamOpen::Rejected));
    assert_eq!(registry.management_snapshot().active_streams, 0);
}

#[test]
fn server_reliable_registry_rejects_active_reopen_for_closed_stream() {
    let registry = Arc::new(ServerReliableStreamRegistry::new(
        ResourceLimits::default().max_streams,
    ));
    let target = TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80)));
    let (commands, _rx) = reliable_path_command_channels(4);
    let session_id = SessionId(1);
    let stream_id = StreamId(100);
    let first_path_registration =
        registry.register_carrier_path(session_id, UnderlayProtocol::Tcp, PathId(0));
    let opened = registry
        .open_or_attach(
            ServerReliableStreamOpenRequest {
                session_id,
                stream_id,
                target: &target,
                lane: FlowLane::Throughput,
                attachment: ServerReliablePathAttachment {
                    path_registration: first_path_registration.clone(),
                    commands,
                    max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
                    role: StreamOpenRole::Active,
                    initial_metrics: None,
                },
            },
            MuxLimits::default(),
            ResourceLimits::default().max_streams,
        )
        .expect("active open should be handled");
    assert!(matches!(opened, ServerReliableStreamOpen::New(_)));
    registry.close(session_id, stream_id);

    let (commands, _rx) = reliable_path_command_channels(4);
    let replacement_path_registration =
        registry.register_carrier_path(session_id, UnderlayProtocol::Tcp, PathId(1));
    let reopened = registry
        .open_or_attach(
            ServerReliableStreamOpenRequest {
                session_id,
                stream_id,
                target: &target,
                lane: FlowLane::Throughput,
                attachment: ServerReliablePathAttachment {
                    path_registration: replacement_path_registration.clone(),
                    commands,
                    max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
                    role: StreamOpenRole::Active,
                    initial_metrics: None,
                },
            },
            MuxLimits::default(),
            ResourceLimits::default().max_streams,
        )
        .expect("closed-stream reopen should be handled");
    assert!(matches!(reopened, ServerReliableStreamOpen::Rejected));
    assert_eq!(registry.management_snapshot().active_streams, 0);
}

#[tokio::test]
async fn server_tcp_binding_keeps_tcp_and_udp_paths_with_same_id_separate() {
    let (tcp_tx, mut tcp_rx) = reliable_path_command_channels(4);
    let binding = ResponseStreamBinding::new(
        SessionId(1),
        UnderlayProtocol::Tcp,
        PathId(0),
        tcp_tx,
        FlowLane::Latency,
    );
    let (udp_tx, mut udp_rx) = reliable_path_command_channels(4);
    binding.attach(
        UnderlayProtocol::Udp,
        PathId(0),
        udp_tx,
        FlowLane::Throughput,
        StreamOpenRole::Active,
        reliable_relay_buffer_len(MuxLimits::default()),
    );

    binding.close_stream(StreamId(7)).await;

    match recv_reliable_path_command(&mut tcp_rx)
        .await
        .expect("tcp close command")
    {
        ReliablePathCommand::CloseStream(stream_id) => assert_eq!(stream_id, StreamId(7)),
        _ => panic!("expected TCP close stream command"),
    }
    match recv_reliable_path_command(&mut udp_rx)
        .await
        .expect("udp close command")
    {
        ReliablePathCommand::CloseStream(stream_id) => assert_eq!(stream_id, StreamId(7)),
        _ => panic!("expected UDP close stream command"),
    }
}

mod datagram;
mod integration;
mod security;
mod tcp_path;
