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
    receivers: &mut TcpPathSessionCommandReceivers,
) -> Option<TcpPathSessionCommand> {
    let command = recv_tcp_path_command(receivers).await;
    if let Some(command) = &command {
        receivers.release_pending_command_bytes(tcp_path_command_pending_bytes(command));
    }
    command
}

#[test]
fn stream_open_demand_hint_preserves_aggressive_bulk_intent() {
    let throughput = stream_demand_hint_for_lane(FlowLane::Throughput);
    assert_eq!(
        flow_lane_from_stream_demand_hint(throughput),
        FlowLane::Throughput
    );

    let tie_break_to_throughput = StreamDemandHint {
        latency_weight_ppm: 500_000,
        throughput_weight_ppm: 500_000,
        ..StreamDemandHint::latency()
    };
    assert_eq!(
        flow_lane_from_stream_demand_hint(tie_break_to_throughput),
        FlowLane::Throughput
    );

    let latency = stream_demand_hint_for_lane(FlowLane::Latency);
    assert_eq!(
        flow_lane_from_stream_demand_hint(latency),
        FlowLane::Latency
    );
}

fn udp_stream_path_indices(
    context: &ClientPathContext,
    lane: FlowLane,
    payload_bytes: usize,
) -> Vec<usize> {
    let observations =
        health_observations(&mut context.health.lock().expect("client path health lock").udp);
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
    let probe = UdpSocket::bind("127.0.0.1:0").await.expect("reserve port");
    let port = probe.local_addr().expect("reserved addr").port();
    drop(probe);
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
        max_stream_window_bytes: 1024 * 1024,
        max_repair_bytes: 1024 * 1024,
        max_reorder_bytes: 1024 * 1024,
        max_datagram_queue_bytes: 1024 * 1024,
        max_tcp_path_inflight_bytes: 32 * 1024,
        max_reliable_relay_chunk_bytes: 32 * 1024,
        tcp_path_heartbeat_interval: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_INTERVAL,
        tcp_path_heartbeat_timeout: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_TIMEOUT,
    };
    let mut send_stream = ReliableSendStream::new(StreamId(9), mux_limits);
    let mut sender_queue = ReliableRelaySenderQueue::default();
    let queue_limit =
        reliable_relay_sender_queue_limit(mux_limits, mux_limits.max_tcp_path_inflight_bytes);

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

    let first = send_stream
        .send_data(Bytes::from(vec![0u8; 8 * 1024]), StreamFlags::NONE)
        .expect("first send");
    sender_queue.push(first);
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

    let second = send_stream
        .send_data(Bytes::from(vec![0u8; 24 * 1024]), StreamFlags::NONE)
        .expect("second send");
    sender_queue.push(second);
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
    send_stream.apply_ack(&[crate::protocol::OffsetRange {
        start: 0,
        end: 8 * 1024,
    }]);
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

    mux_limits.max_tcp_path_inflight_bytes = 64 * 1024;
    let larger_queue_limit =
        reliable_relay_sender_queue_limit(mux_limits, mux_limits.max_tcp_path_inflight_bytes);
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
fn reliable_stream_frame_queue_tracks_relay_chunk_byte_budget() {
    let mux_limits = MuxLimits {
        max_payload_bytes: 1024 * 1024,
        max_ack_ranges: 256,
        max_streams: 1024,
        max_stream_window_bytes: 16 * 1024 * 1024,
        max_repair_bytes: 16 * 1024 * 1024,
        max_reorder_bytes: 16 * 1024 * 1024,
        max_datagram_queue_bytes: 4 * 1024 * 1024,
        max_tcp_path_inflight_bytes: 4 * 1024 * 1024,
        max_reliable_relay_chunk_bytes: 256 * 1024,
        tcp_path_heartbeat_interval: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_INTERVAL,
        tcp_path_heartbeat_timeout: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_TIMEOUT,
    };

    assert_eq!(
        reliable_stream_frame_queue(mux_limits),
        (mux_limits.max_reorder_bytes / mux_limits.max_reliable_relay_chunk_bytes) + 4
    );
}

#[test]
fn reliable_stream_frame_queue_tracks_actual_attachment_payload() {
    let mux_limits = MuxLimits {
        max_payload_bytes: 1024 * 1024,
        max_ack_ranges: 256,
        max_streams: 1024,
        max_stream_window_bytes: 16 * 1024 * 1024,
        max_repair_bytes: 16 * 1024 * 1024,
        max_reorder_bytes: 16 * 1024 * 1024,
        max_datagram_queue_bytes: 4 * 1024 * 1024,
        max_tcp_path_inflight_bytes: 4 * 1024 * 1024,
        max_reliable_relay_chunk_bytes: 256 * 1024,
        tcp_path_heartbeat_interval: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_INTERVAL,
        tcp_path_heartbeat_timeout: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_TIMEOUT,
    };

    let tcp_sized_queue = reliable_stream_frame_queue(mux_limits);
    let udp_sized_queue = reliable_stream_frame_queue_for_payload(mux_limits, 1200);

    assert_eq!(tcp_sized_queue, 68);
    assert_eq!(udp_sized_queue, 1024);
    assert!(udp_sized_queue > tcp_sized_queue);
}

#[test]
fn tcp_path_command_queue_tracks_inflight_budget_not_stream_limit() {
    let mux_limits = MuxLimits {
        max_payload_bytes: 1024 * 1024,
        max_ack_ranges: 256,
        max_streams: 1024,
        max_stream_window_bytes: 16 * 1024 * 1024,
        max_repair_bytes: 16 * 1024 * 1024,
        max_reorder_bytes: 16 * 1024 * 1024,
        max_datagram_queue_bytes: 4 * 1024 * 1024,
        max_tcp_path_inflight_bytes: 4 * 1024 * 1024,
        max_reliable_relay_chunk_bytes: 256 * 1024,
        tcp_path_heartbeat_interval: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_INTERVAL,
        tcp_path_heartbeat_timeout: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_TIMEOUT,
    };
    assert_eq!(tcp_path_command_queue(mux_limits), 20);

    let resources = ResourceLimits {
        max_streams: 65_536,
        max_tcp_path_inflight_bytes: mux_limits.max_tcp_path_inflight_bytes,
        max_reliable_relay_chunk_bytes: mux_limits.max_reliable_relay_chunk_bytes,
        ..ResourceLimits::default()
    };
    assert_eq!(tcp_session_command_queue(resources), 20);
}

#[test]
fn tcp_path_command_queue_tracks_actual_payload_quantum() {
    let mux_limits = MuxLimits {
        max_payload_bytes: 1024 * 1024,
        max_ack_ranges: 256,
        max_streams: 1024,
        max_stream_window_bytes: 16 * 1024 * 1024,
        max_repair_bytes: 16 * 1024 * 1024,
        max_reorder_bytes: 16 * 1024 * 1024,
        max_datagram_queue_bytes: 4 * 1024 * 1024,
        max_tcp_path_inflight_bytes: 4 * 1024 * 1024,
        max_reliable_relay_chunk_bytes: 256 * 1024,
        tcp_path_heartbeat_interval: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_INTERVAL,
        tcp_path_heartbeat_timeout: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_TIMEOUT,
    };

    let tcp_sized_queue = tcp_path_command_queue(mux_limits);
    let udp_sized_queue = tcp_path_command_queue_for_payload(mux_limits, 1200);

    assert_eq!(tcp_sized_queue, 20);
    assert_eq!(
        udp_sized_queue,
        mux_limits.max_tcp_path_inflight_bytes.div_ceil(1200) + 4
    );
    assert!(udp_sized_queue > tcp_sized_queue);
}

#[test]
fn auto_tcp_flow_demand_promotes_lane_after_runtime_bdp_threshold() {
    let mux_limits = MuxLimits::default();
    let path = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 20.0, 30_000_000.0);
    let threshold = tcp_auto_bulk_threshold_bytes(Some(path), mux_limits);
    let high_bdp_path = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 180.0, 300_000_000.0);
    let high_bdp_threshold = tcp_auto_bulk_threshold_bytes(Some(high_bdp_path), mux_limits);
    let high_bdp =
        ((high_bdp_path.delivery_rate_bps / 8.0) * (high_bdp_path.srtt_ms / 1000.0)).ceil() as u64;
    let mut state = ReliableRelayFlowDemandTracker::new();

    assert!(threshold >= (reliable_relay_buffer_len(mux_limits) as u64).saturating_mul(2));
    assert!(high_bdp_threshold < high_bdp / 4);
    assert!(high_bdp_threshold >= high_bdp / 8);

    let small_flow_bytes =
        (tcp_lane_startup_chunk_bytes(FlowLane::Throughput, mux_limits) as u64 / 2).max(1);
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
    assert!(unstable_bulk_chunk < bulk_chunk);

    let interactive_inflight =
        adaptive_reliable_relay_inflight_bytes(Some(stable), FlowLane::Latency, mux_limits);
    let bulk_inflight =
        adaptive_reliable_relay_inflight_bytes(Some(stable), FlowLane::Throughput, mux_limits);
    let unstable_bulk_inflight =
        adaptive_reliable_relay_inflight_bytes(Some(unstable), FlowLane::Throughput, mux_limits);
    assert!(bulk_inflight >= interactive_inflight);
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
fn udp_relay_chunking_keeps_latency_single_packet_and_amortizes_bulk() {
    let mux_limits = MuxLimits {
        max_reliable_relay_chunk_bytes: 64 * 1024,
        max_ack_ranges: 16,
        ..MuxLimits::default()
    };
    let max_frame_payload =
        udp_carrier::max_stream_payload_bytes(CodecLimits::default(), mux_limits);
    let safe_payload = udp_carrier::safe_stream_payload_bytes(mux_limits);

    let latency_chunk = adaptive_relay_chunk_bytes_for_underlay(
        None,
        FlowLane::Latency,
        mux_limits,
        UnderlayProtocol::Udp,
        max_frame_payload,
    );
    let bulk_chunk = adaptive_relay_chunk_bytes_for_underlay(
        None,
        FlowLane::Throughput,
        mux_limits,
        UnderlayProtocol::Udp,
        max_frame_payload,
    );

    assert!(latency_chunk <= safe_payload);
    assert!(bulk_chunk > safe_payload);
    assert!(bulk_chunk <= max_frame_payload);
}

#[test]
fn reliable_relay_stall_timeout_is_adaptive_and_bounded_for_fluent_failover() {
    let low_latency = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 20.0, 30_000_000.0);
    let mut cross_continent =
        PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 900.0, 300_000_000.0);
    cross_continent.jitter_ms = 400.0;

    assert_eq!(
        reliable_relay_stall_timeout(Some(low_latency), FlowLane::Latency),
        TCP_STREAM_STALL_MIN_TIMEOUT
    );
    assert!(
        reliable_relay_stall_timeout(Some(cross_continent), FlowLane::Throughput)
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

    assert!(!reliable_relay_recv_progress_resend_active(
        &recv_stream,
        true
    ));

    recv_stream
        .receive_data(1024, Bytes::from_static(b"late"), StreamFlags::NONE)
        .expect("out-of-order data");
    assert!(reliable_relay_recv_progress_resend_active(
        &recv_stream,
        true
    ));
    assert!(!reliable_relay_recv_progress_resend_active(
        &recv_stream,
        false
    ));

    let low_interval = reliable_stream_recv_progress_interval(Some(low_latency), FlowLane::Latency);
    let high_interval =
        reliable_stream_recv_progress_interval(Some(cross_continent), FlowLane::Throughput);
    assert!(low_interval >= UDP_MIN_RESPONSE_TIMEOUT);
    assert!(low_interval <= TCP_STREAM_STALL_MIN_TIMEOUT);
    assert!(high_interval >= low_interval);
    assert!(high_interval <= TCP_STREAM_STALL_MIN_TIMEOUT);
}

#[test]
fn reliable_recv_progress_batches_max_data_updates() {
    let mux_limits = MuxLimits {
        max_payload_bytes: 1024,
        max_reliable_relay_chunk_bytes: 1024,
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
fn reliable_recv_progress_batches_bulk_acks_by_window_and_ack_capacity() {
    let mux_limits = MuxLimits {
        max_ack_ranges: 16,
        max_stream_window_bytes: 64 * 1024,
        max_repair_bytes: 64 * 1024,
        max_reorder_bytes: 64 * 1024,
        max_tcp_path_inflight_bytes: 64 * 1024,
        max_reliable_relay_chunk_bytes: 64 * 1024,
        ..MuxLimits::default()
    };
    let mut recv_stream = ReliableRecvStream::new(StreamId(23), mux_limits);
    let mut progress = ReliableRecvProgress::default();
    let ack_step = reliable_stream_ack_update_bytes(None, FlowLane::Throughput, mux_limits);

    assert_eq!(ack_step, PATH_OPEN_SCORE_BYTES as u64);
    recv_stream
        .receive_data(0, Bytes::from(vec![0x11; 1024]), StreamFlags::NONE)
        .expect("first data");
    assert!(progress.should_send_ack(&recv_stream, None, FlowLane::Throughput, mux_limits, false));

    recv_stream
        .receive_data(1024, Bytes::from(vec![0x22; 1024]), StreamFlags::NONE)
        .expect("below ack step");
    assert!(!progress.should_send_ack(&recv_stream, None, FlowLane::Throughput, mux_limits, false));

    recv_stream
        .receive_data(
            2048,
            Bytes::from(vec![0x33; ack_step as usize]),
            StreamFlags::NONE,
        )
        .expect("past ack step");
    assert!(progress.should_send_ack(&recv_stream, None, FlowLane::Throughput, mux_limits, false));
}

#[test]
fn reliable_recv_progress_acks_reorder_gap_without_waiting_for_bulk_step() {
    let mux_limits = MuxLimits {
        max_ack_ranges: 16,
        max_stream_window_bytes: 64 * 1024,
        max_repair_bytes: 64 * 1024,
        max_reorder_bytes: 64 * 1024,
        max_tcp_path_inflight_bytes: 64 * 1024,
        max_reliable_relay_chunk_bytes: 64 * 1024,
        ..MuxLimits::default()
    };
    let mut recv_stream = ReliableRecvStream::new(StreamId(24), mux_limits);
    let mut progress = ReliableRecvProgress::default();

    recv_stream
        .receive_data(0, Bytes::from(vec![0x11; 1024]), StreamFlags::NONE)
        .expect("first data");
    assert!(progress.should_send_ack(&recv_stream, None, FlowLane::Throughput, mux_limits, false));

    recv_stream
        .receive_data(8192, Bytes::from(vec![0x22; 1024]), StreamFlags::NONE)
        .expect("out-of-order data");
    assert!(progress.should_send_ack(&recv_stream, None, FlowLane::Throughput, mux_limits, false));
}

#[test]
fn reliable_recv_progress_acks_repair_horizon_advancement() {
    let mux_limits = MuxLimits {
        max_ack_ranges: 16,
        max_stream_window_bytes: 64 * 1024,
        max_repair_bytes: 64 * 1024,
        max_reorder_bytes: 64 * 1024,
        max_tcp_path_inflight_bytes: 64 * 1024,
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
    assert!(progress.should_send_ack(&recv_stream, None, FlowLane::Throughput, mux_limits, false));

    recv_stream
        .receive_data(9216, Bytes::from(vec![0x33; 1024]), StreamFlags::NONE)
        .expect("small tail extension");
    assert!(
        !progress.should_send_ack(&recv_stream, None, FlowLane::Throughput, mux_limits, false),
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
        progress.should_send_ack(&recv_stream, None, FlowLane::Throughput, mux_limits, false),
        "meaningful repair horizon advancement must be ACKed even when range count is unchanged"
    );
}

#[test]
fn tcp_sole_survivor_reannounce_budget_scales_from_stall_model() {
    let low_latency_budget =
        reliable_relay_sole_survivor_reannounce_attempts(TCP_STREAM_STALL_MIN_TIMEOUT);
    let max_timeout_budget =
        reliable_relay_sole_survivor_reannounce_attempts(TCP_STREAM_STALL_MAX_TIMEOUT);
    assert!(
        low_latency_budget > max_timeout_budget,
        "low-latency paths should get more quick repair probes"
    );
    assert!(max_timeout_budget >= 2);
    assert!(low_latency_budget <= 16);
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
fn stream_ack_gap_repair_is_suppressed_on_udp_reliable_carrier() {
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

    let tcp_repairs = stream_ack_gap_repair_frames(
        &send_stream,
        &ranges,
        usize::MAX,
        true,
        Some(UnderlayProtocol::Tcp),
        false,
        false,
    );
    assert_eq!(tcp_repairs.len(), 1);
    assert!(matches!(
        &tcp_repairs[0],
        Frame::StreamData {
            offset: 4,
            payload,
            ..
        } if payload.as_ref() == b"bbbb"
    ));

    assert!(
        stream_ack_gap_repair_frames(
            &send_stream,
            &ranges,
            usize::MAX,
            true,
            Some(UnderlayProtocol::Udp),
            false,
            true,
        )
        .is_empty(),
        "a single UDP reliable carrier owns ordinary packet-loss recovery"
    );
    assert!(
        stream_ack_gap_repair_frames(
            &send_stream,
            &ranges,
            usize::MAX,
            true,
            Some(UnderlayProtocol::Udp),
            true,
            false,
        )
        .is_empty(),
        "fresh UDP multipath ACK gaps wait for persistent product-hole evidence"
    );
    let udp_multipath_repairs = stream_ack_gap_repair_frames(
        &send_stream,
        &ranges,
        usize::MAX,
        true,
        Some(UnderlayProtocol::Udp),
        true,
        true,
    );
    assert_eq!(
        udp_multipath_repairs.len(),
        1,
        "multipath UDP may reinject authoritative product gaps over another path"
    );
    assert!(matches!(
        &udp_multipath_repairs[0],
        Frame::StreamData {
            offset: 4,
            payload,
            ..
        } if payload.as_ref() == b"bbbb"
    ));
    assert!(
        stream_ack_gap_repair_frames(
            &send_stream,
            &ranges,
            usize::MAX,
            false,
            Some(UnderlayProtocol::Tcp),
            false,
            false,
        )
        .is_empty(),
        "non-authoritative ACK snapshots must not infer missing holes"
    );
}

#[test]
fn tail_stall_repair_prefers_authoritative_ack_gap_before_frontier_tail() {
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
    let (repairs, kind) = stream_tail_stall_repair_frames(&send_stream, &ranges, usize::MAX, true);

    assert_eq!(kind, "ack_gap");
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
fn tail_stall_repair_uses_frontier_tail_when_ack_has_no_authoritative_gap() {
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
    let (repairs, kind) = stream_tail_stall_repair_frames(&send_stream, &ranges, 6, true);

    assert_eq!(kind, "ack_frontier");
    assert_eq!(repairs.len(), 2);
    assert!(matches!(
        &repairs[0],
        Frame::StreamData {
            offset: 4,
            payload,
            ..
        } if payload.as_ref() == b"bbbb"
    ));
    assert!(matches!(
        &repairs[1],
        Frame::StreamData {
            offset: 8,
            payload,
            ..
        } if payload.as_ref() == b"cc"
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
        reliable_relay_receive_hole_victim(
            &context,
            &[key(0), key(1), key(2)],
            FlowLane::Throughput,
            64 * 1024,
            &path_last_delivery_at
        ),
        Some(key(2))
    );

    reliable_relay_refresh_path_tracking(
        &mut path_last_delivery_at,
        &[key(0), key(2), key(3)],
        now,
    );
    assert!(!path_last_delivery_at.contains_key(&key(1)));
    assert_eq!(path_last_delivery_at.get(&key(3)), Some(&now));
    assert_eq!(
        reliable_relay_receive_hole_victim(
            &context,
            &[key(0), key(1)],
            FlowLane::Throughput,
            64 * 1024,
            &path_last_delivery_at
        ),
        Some(key(1))
    );
    assert_eq!(
        reliable_relay_receive_hole_victim(
            &context,
            &[key(3)],
            FlowLane::Throughput,
            64 * 1024,
            &path_last_delivery_at
        ),
        None
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
    assert!(validation_quantum <= tcp_lane_startup_chunk_bytes(FlowLane::Latency, mux_limits));
    assert!(validation_quantum < expected_quantum);
    assert!(expected_quantum < mux_limits.max_tcp_path_inflight_bytes);
}

#[test]
fn tcp_path_sessions_are_dedicated_for_latency_sensitive_lanes() {
    assert!(tcp_path_lane_uses_dedicated_session(FlowLane::Latency));
    assert!(tcp_path_lane_uses_dedicated_session(FlowLane::Control));
    assert!(tcp_path_lane_uses_dedicated_session(
        FlowLane::RealtimeDatagram
    ));
    assert!(!tcp_path_lane_uses_dedicated_session(FlowLane::Throughput));
    assert!(!tcp_path_lane_uses_dedicated_session(FlowLane::Background));
}

#[test]
fn switchable_stream_demand_updates_from_local_sender_metrics() {
    let registry = ServerReliableStreamRegistry::new(ResourceLimits::default().max_streams);
    let target = TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80)));
    let (commands, _rx) = tcp_path_session_command_channels(4);
    let mut stream = match registry
        .open_or_attach(
            ServerReliableStreamOpenRequest {
                session_id: SessionId(1),
                stream_id: StreamId(7),
                target: &target,
                lane: FlowLane::Latency,
                attachment: ServerReliablePathAttachment {
                    path_id: PathId(0),
                    underlay: UnderlayProtocol::Tcp,
                    commands,
                    max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
                    role: StreamOpenRole::Active,
                },
            },
            MuxLimits::default(),
            ResourceLimits::default().max_streams,
        )
        .expect("open stream")
    {
        ServerReliableStreamOpen::New(stream) => stream,
        ServerReliableStreamOpen::Existing => panic!("expected new stream"),
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

#[tokio::test]
async fn server_tcp_binding_keeps_tcp_and_udp_paths_with_same_id_separate() {
    let (tcp_tx, mut tcp_rx) = tcp_path_session_command_channels(4);
    let binding = ResponseStreamBinding::new(
        SessionId(1),
        UnderlayProtocol::Tcp,
        PathId(0),
        tcp_tx,
        FlowLane::Latency,
    );
    let (udp_tx, mut udp_rx) = tcp_path_session_command_channels(4);
    binding.attach(
        UnderlayProtocol::Udp,
        PathId(0),
        udp_tx,
        FlowLane::Throughput,
        StreamOpenRole::Active,
        reliable_relay_buffer_len(MuxLimits::default()),
    );

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
mod tcp_path_binding;
