use super::*;
use crate::config::{
    ClientPathConfig, DEFAULT_OUTBOUND_CONNECT_TIMEOUT, ResourceLimits, SessionConfig,
};
use crate::outbound::{DnsConfig, OutboundConfig};
use crate::protocol::PathUsage;
use crate::runtime::path::ServerLocalPath;
use crate::transport::SystemCarrierNetworkProvider;

const FULL_STACK_RESPONSE_TIMEOUT: Duration = Duration::from_secs(15);

fn client_context_with_session_retention(
    paths: Vec<PathSpec>,
    resources: ResourceLimits,
    retention_timeout: Duration,
) -> ClientPathContext {
    let paths = paths
        .into_iter()
        .map(|spec| ClientPathConfig {
            spec,
            security: security(),
        })
        .collect();
    ClientPathContext::new_with_runtime_options(
        paths,
        resources,
        None,
        Vec::new(),
        crate::runtime::path::ClientPathRuntimeOptions {
            session_retention_timeout: retention_timeout,
            path_group_ordinal: 0,
            carrier_network: Arc::new(SystemCarrierNetworkProvider),
            allow_peer_diagnostics: false,
        },
    )
    .expect("client context")
}

async fn open_socks5_tcp_tunnel<S>(client: &mut S, target_addr: SocketAddr)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
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
    client.write_all(&connect).await.expect("connect request");
    let mut response = [0u8; 10];
    client
        .read_exact(&mut response)
        .await
        .expect("connect reply");
    assert_eq!(response[1], Socks5Reply::Succeeded as u8);
}

async fn wait_for_tcp_path_detached(context: &ClientPathContext, path_index: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let detached = {
                let health = context.health().lock().expect("health lock");
                let path = health.tcp.get(path_index).expect("TCP path health");
                path.active_flows == 0 && path.state != SchedulerPathState::Active
            };
            if detached {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("TCP carrier did not detach");
}

#[tokio::test(flavor = "multi_thread")]
async fn mixed_server_listeners_apply_local_policy_independent_of_wire_path_id() {
    let tcp_path = reserve_tcp_path_with_query("srtt-ms=20&rate-mbps=100").await;
    let udp_port = reserve_process_unique_udp_port().await;
    let udp_path = format!("udp://127.0.0.1:{udp_port}?srtt-ms=90&rate-mbps=400&backup=true")
        .parse::<PathSpec>()
        .expect("UDP backup path");
    let server = tokio::spawn(run_server(
        vec![tcp_path.clone(), udp_path.clone()],
        OutboundConfig::Direct,
        DnsConfig::default(),
        DEFAULT_OUTBOUND_CONNECT_TIMEOUT,
        security(),
        MppPerformanceConfig::default(),
        ResourceLimits::default(),
        SessionConfig::default(),
        ManagementConfig::default(),
    ));
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Reversed client order is intentional. Each underlay emits PathId(0),
    // while the server UDP listener is configuration ordinal 1.
    let context = ClientPathContext::new(
        vec![udp_path, tcp_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("mixed client context");
    probe_client_paths(&context, Duration::from_secs(2)).await;

    assert_eq!(
        context.peer_path_usage(UnderlayProtocol::Udp, 0),
        Some(PathUsage::Backup),
        "peer PathId(0) must not select the server's TCP listener policy",
    );

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn path_probe_refreshes_tcp_health_without_stream_load() {
    let (path, server) = spawn_server_path(OutboundConfig::Direct).await;
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("ctx");

    probe_client_paths(&context, Duration::from_secs(1)).await;

    server.await.expect("server join").expect("server probe");
    let health = context.health().lock().expect("health lock");
    assert_eq!(health.tcp[0].state, SchedulerPathState::Active);
    assert!(health.tcp[0].measured_srtt_ms.is_some());
    assert_eq!(health.tcp[0].active_flows, 0);
    assert_eq!(health.tcp[0].relay_bytes_in_flight, 0);
}

#[tokio::test]
async fn path_probe_refreshes_udp_health_without_association_load() {
    let (path, server) = spawn_udp_server_path(OutboundConfig::Direct).await;
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("ctx");

    probe_client_paths(&context, Duration::from_secs(1)).await;

    server.abort();
    let _ = server.await;
    let health = context.health().lock().expect("health lock");
    assert_eq!(health.udp[0].state, SchedulerPathState::Active);
    assert!(health.udp[0].measured_srtt_ms.is_some());
    assert_eq!(health.udp[0].active_flows, 0);
    assert_eq!(health.udp[0].relay_bytes_in_flight, 0);
}

#[tokio::test]
async fn path_probe_skips_tcp_path_with_active_stream() {
    let path = reserve_tcp_path().await;
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("ctx");
    context.mark_tcp_path_open_success(0, Duration::from_millis(5), TrafficClass::Throughput);

    probe_client_paths(&context, Duration::from_millis(20)).await;

    let health = context.health().lock().expect("health lock");
    assert_eq!(health.tcp[0].state, SchedulerPathState::Active);
    assert_eq!(health.tcp[0].consecutive_failures, 0);
    assert_eq!(health.tcp[0].active_flows, 1);
    assert_eq!(health.tcp[0].relay_bytes_in_flight, 0);
}

#[tokio::test]
async fn path_probe_skips_udp_path_with_active_session() {
    let path = reserve_udp_path().await;
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("ctx");
    context.mark_udp_path_open_success(0, Duration::from_millis(5));

    probe_client_paths(&context, Duration::from_millis(20)).await;

    let health = context.health().lock().expect("health lock");
    assert_eq!(health.udp[0].state, SchedulerPathState::Active);
    assert_eq!(health.udp[0].consecutive_failures, 0);
    assert_eq!(health.udp[0].active_flows, 1);
    assert_eq!(health.udp[0].relay_bytes_in_flight, 0);
}

#[tokio::test]
async fn repeated_path_probe_failure_keeps_only_tcp_path_probeable() {
    let path = reserve_tcp_path().await;
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("ctx");

    probe_client_paths(&context, Duration::from_millis(50)).await;

    {
        let health = context.health().lock().expect("health lock");
        assert_eq!(health.tcp[0].state, SchedulerPathState::Suspect);
        assert_eq!(health.tcp[0].consecutive_failures, 1);
        assert!(health.tcp[0].failed_until.is_none());
    }
    assert_eq!(
        context
            .ordered_tcp_path_indices(TrafficClass::Latency, 512)
            .first()
            .copied(),
        Some(0)
    );

    probe_client_paths(&context, Duration::from_millis(50)).await;

    {
        let health = context.health().lock().expect("health lock");
        assert_eq!(health.tcp[0].state, SchedulerPathState::Suspect);
        assert_eq!(health.tcp[0].consecutive_failures, 2);
        assert!(health.tcp[0].failed_until.is_none());
    }
    assert_eq!(
        context
            .ordered_tcp_path_indices(TrafficClass::Latency, 512)
            .first()
            .copied(),
        Some(0)
    );
}

#[tokio::test]
async fn socks5_ingress_relays_tcp_payload_over_encrypted_internal_stream() {
    let (target_addr, target) = spawn_echo_target().await;
    let (path, server_path) = spawn_server_path(OutboundConfig::Direct).await;
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("ctx");
    let telemetry_context = context.clone();
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
    let telemetry = telemetry_context.telemetry_snapshot();
    assert_eq!(telemetry.reliable.io.to_peer_bytes, 4);
    assert_eq!(telemetry.reliable.io.from_peer_bytes, 4);
    assert_eq!(telemetry.reliable.flows.opened, 1);
    assert_eq!(telemetry.reliable.flows.active, 0);
    assert_eq!(telemetry.reliable.flows.completed, 1);
    assert_eq!(telemetry.reliable.flows.failed, 0);
    assert_eq!(
        telemetry.active_flow_capacity,
        crate::runtime::telemetry::active_flow_detail_capacity(
            ResourceLimits::default().max_streams,
        )
    );
    drop(telemetry_context);
    server_path
        .await
        .expect("server join")
        .expect("server path");
    target.await.expect("target join");
}

#[tokio::test]
async fn socks5_ingress_accepts_configured_username_password_auth() {
    let (target_addr, target) = spawn_echo_target().await;
    let (path, server_path) = spawn_server_path(OutboundConfig::Direct).await;
    let context = ClientPathContext::new_with_proxy_auth(
        vec![path],
        security(),
        ResourceLimits::default(),
        ProxyAuthConfig::required("operator".to_string(), "secret".to_string()),
    )
    .expect("ctx");
    let (mut client, server) = duplex(4096);
    let handler = tokio::spawn(handle_socks5_client_stream(server, context));

    client
        .write_all(&[0x05, 0x02, 0x00, 0x02])
        .await
        .expect("auth methods");
    let mut method_response = [0u8; 2];
    client
        .read_exact(&mut method_response)
        .await
        .expect("method response");
    assert_eq!(method_response, [0x05, 0x02]);

    client
        .write_all(&[
            0x01, 0x08, b'o', b'p', b'e', b'r', b'a', b't', b'o', b'r', 0x06, b's', b'e', b'c',
            b'r', b'e', b't',
        ])
        .await
        .expect("credentials");
    let mut auth_response = [0u8; 2];
    client
        .read_exact(&mut auth_response)
        .await
        .expect("auth response");
    assert_eq!(auth_response, [0x01, 0x00]);

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
async fn socks5_ingress_rejects_wrong_username_password_auth() {
    let context = ClientPathContext::new_with_proxy_auth(
        Vec::new(),
        security(),
        ResourceLimits::default(),
        ProxyAuthConfig::required("operator".to_string(), "secret".to_string()),
    )
    .expect("ctx");
    let (mut client, server) = duplex(1024);
    let handler = tokio::spawn(handle_socks5_client_stream(server, context));

    client
        .write_all(&[0x05, 0x01, 0x02])
        .await
        .expect("auth methods");
    let mut method_response = [0u8; 2];
    client
        .read_exact(&mut method_response)
        .await
        .expect("method response");
    assert_eq!(method_response, [0x05, 0x02]);

    client
        .write_all(&[
            0x01, 0x08, b'o', b'p', b'e', b'r', b'a', b't', b'o', b'r', 0x05, b'w', b'r', b'o',
            b'n', b'g',
        ])
        .await
        .expect("credentials");
    let mut auth_response = [0u8; 2];
    client
        .read_exact(&mut auth_response)
        .await
        .expect("auth response");
    assert_eq!(auth_response, [0x01, 0x01]);

    assert!(handler.await.expect("join").is_err());
}

#[tokio::test]
async fn socks5_ingress_relays_tcp_payload_over_udp_stream_path() {
    let (target_addr, target) = spawn_echo_target().await;
    let (path, server_path) = spawn_udp_server_path(OutboundConfig::Direct).await;
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
    server_path.abort();
    let _ = server_path.await;
    target.await.expect("target join");
}

#[tokio::test]
async fn tcp_path_session_multiplexes_multiple_single_path_interactive_streams() {
    let (target_addr, target) = spawn_echo_target_count(2).await;
    let (path, server_path) = spawn_server_path(OutboundConfig::Direct).await;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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

    let low_latency_path = reserve_tcp_path_with_query("srtt-ms=10&rate-mbps=20").await;
    let high_bandwidth_path = reserve_tcp_path_with_query("srtt-ms=120&rate-mbps=300").await;
    let low_latency_listener = bind_listener(&low_latency_path)
        .await
        .expect("low-latency bind");
    let high_bandwidth_listener = bind_listener(&high_bandwidth_path)
        .await
        .expect("high-bandwidth bind");
    let low_latency_local_path = ServerLocalPath::new(0, low_latency_path.clone());
    let high_bandwidth_local_path = ServerLocalPath::new(1, high_bandwidth_path.clone());
    let ServerIdentityRuntime {
        paths: server_context,
        reliable_relay,
    } = server_runtime(OutboundConfig::Direct);
    let server_relay = tokio::spawn(reliable_relay.run());
    let (accepted_tx, mut accepted_rx) = mpsc::channel(8);
    let (stop_servers_tx, stop_servers_rx) = tokio::sync::watch::channel(false);
    let low_latency_context = server_context.clone();
    let low_latency_accepted_tx = accepted_tx.clone();
    let mut low_latency_stop_rx = stop_servers_rx.clone();
    let low_latency_server = tokio::spawn(async move {
        let mut sessions = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                changed = low_latency_stop_rx.changed() => {
                    if changed.is_ok() && *low_latency_stop_rx.borrow() {
                        break;
                    }
                    if changed.is_err() {
                        break;
                    }
                }
                accepted = low_latency_listener.accept() => {
                    let (stream, _) = accepted.expect("low-latency accept");
                    let _ = low_latency_accepted_tx.try_send(0usize);
                    let session_context = low_latency_context.clone();
                    let local_path = low_latency_local_path.clone();
                    sessions.spawn(async move {
                        handle_server_path(stream, local_path, session_context).await
                    });
                }
            }
        }
        while let Some(session) = sessions.join_next().await {
            session.map_err(RuntimeError::TaskJoin)??;
        }
        Ok::<(), RuntimeError>(())
    });
    let high_bandwidth_context = server_context.clone();
    let mut high_bandwidth_stop_rx = stop_servers_rx.clone();
    let high_bandwidth_server = tokio::spawn(async move {
        let mut sessions = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                changed = high_bandwidth_stop_rx.changed() => {
                    if changed.is_ok() && *high_bandwidth_stop_rx.borrow() {
                        break;
                    }
                    if changed.is_err() {
                        break;
                    }
                }
                accepted = high_bandwidth_listener.accept() => {
                    let (stream, _) = accepted.expect("high-bandwidth accept");
                    let _ = accepted_tx.try_send(1usize);
                    let session_context = high_bandwidth_context.clone();
                    let local_path = high_bandwidth_local_path.clone();
                    sessions.spawn(async move {
                        handle_server_path(stream, local_path, session_context).await
                    });
                }
            }
        }
        while let Some(session) = sessions.join_next().await {
            session.map_err(RuntimeError::TaskJoin)??;
        }
        Ok::<(), RuntimeError>(())
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
    tokio::time::timeout(
        FULL_STACK_RESPONSE_TIMEOUT,
        client.read_exact(&mut received),
    )
    .await
    .expect("response timeout")
    .expect("payload read");
    assert_eq!(received, expected_payload);

    let mut accepted = Vec::new();
    while !(accepted.contains(&0) && accepted.contains(&1)) {
        let path = tokio::time::timeout(Duration::from_secs(2), accepted_rx.recv())
            .await
            .expect("accept timeout")
            .expect("accepted path");
        accepted.push(path);
    }

    handler.await.expect("handler join").expect("handler");
    {
        let health = health_context.health().lock().expect("health lock");
        assert_eq!(health.tcp[0].active_flows, 0);
        assert_eq!(health.tcp[1].active_flows, 0);
    }
    drop(health_context);
    let _ = stop_servers_tx.send(true);
    low_latency_server
        .await
        .expect("low-latency server join")
        .expect("low-latency server");
    high_bandwidth_server
        .await
        .expect("high-bandwidth server join")
        .expect("high-bandwidth server");
    server_relay.abort();
    let _ = server_relay.await;
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
    let first_local_path = ServerLocalPath::new(0, first_path.clone());
    let second_local_path = ServerLocalPath::new(1, second_path.clone());
    let ServerIdentityRuntime {
        paths: server_context,
        reliable_relay,
    } = server_runtime(OutboundConfig::Direct);
    let server_relay = tokio::spawn(reliable_relay.run());
    let first_server_context = server_context.clone();
    let first_server = tokio::spawn(async move {
        let (stream, _) = first_listener.accept().await.expect("first accept");
        handle_server_path(stream, first_local_path, first_server_context).await
    });
    let second_server_context = server_context.clone();
    let second_server = tokio::spawn(async move {
        let (stream, _) = second_listener.accept().await.expect("second accept");
        handle_server_path(stream, second_local_path, second_server_context).await
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
        let health = health_context.health().lock().expect("health lock");
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
    server_relay.abort();
    let _ = server_relay.await;
    target.await.expect("target join");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_socks5_stream_survives_five_second_total_outage_and_reattaches_over_quic() {
    let target_listener = TcpListener::bind("127.0.0.1:0").await.expect("target bind");
    let target_addr = target_listener.local_addr().expect("target addr");
    let target = tokio::spawn(async move {
        let (mut stream, _) = target_listener.accept().await.expect("target accept");
        let mut first = [0u8; 4];
        stream
            .read_exact(&mut first)
            .await
            .expect("target first read");
        assert_eq!(&first, b"ping");
        stream.write_all(b"pong").await.expect("target first write");

        let mut second = [0u8; 4];
        stream
            .read_exact(&mut second)
            .await
            .expect("target post-outage read");
        assert_eq!(&second, b"next");
        stream
            .write_all(b"done")
            .await
            .expect("target post-outage write");
        stream.shutdown().await.expect("target shutdown");
    });

    let tcp_path = reserve_tcp_path_with_query("srtt-ms=10&rate-mbps=100").await;
    let udp_port = reserve_process_unique_udp_port().await;
    let udp_path = format!("udp://127.0.0.1:{udp_port}?srtt-ms=120&rate-mbps=100")
        .parse::<PathSpec>()
        .expect("QUIC recovery path");
    let tcp_listener = bind_listener(&tcp_path).await.expect("TCP path bind");
    let tcp_local_path = ServerLocalPath::new(0, tcp_path.clone());
    let ServerIdentityRuntime {
        paths: server_context,
        reliable_relay,
    } = server_runtime(OutboundConfig::Direct);
    let server_relay = tokio::spawn(reliable_relay.run());
    let tcp_server_context = server_context.clone();
    let tcp_server = tokio::spawn(async move {
        let (stream, _) = tcp_listener.accept().await.expect("TCP path accept");
        handle_server_path(stream, tcp_local_path, tcp_server_context).await
    });

    let resources = ResourceLimits {
        tcp_path_heartbeat_interval: Duration::from_secs(60),
        tcp_path_heartbeat_timeout: Duration::from_secs(60),
        ..ResourceLimits::default()
    };
    let context = ClientPathContext::new(vec![tcp_path, udp_path.clone()], security(), resources)
        .expect("client context with default session retention");
    let health_context = context.clone();
    let ingress_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("ingress bind");
    let ingress_addr = ingress_listener.local_addr().expect("ingress addr");
    let mut handler = tokio::spawn(async move {
        let (server, _) = ingress_listener.accept().await.expect("ingress accept");
        handle_socks5_client_stream(server, context).await
    });
    let mut client = TcpStream::connect(ingress_addr)
        .await
        .expect("ingress client");

    open_socks5_tcp_tunnel(&mut client, target_addr).await;
    client.write_all(b"ping").await.expect("first payload");
    let mut first_response = [0u8; 4];
    client
        .read_exact(&mut first_response)
        .await
        .expect("first response");
    assert_eq!(&first_response, b"pong");

    tcp_server.abort();
    let _ = tcp_server.await;
    client
        .write_all(b"next")
        .await
        .expect("payload buffered during outage");
    wait_for_tcp_path_detached(&health_context, 0).await;

    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(
        !handler.is_finished(),
        "logical SOCKS stream closed during the retention window"
    );

    let udp_endpoint = bind_server_udp_endpoint(&udp_path, &server_context)
        .await
        .expect("QUIC recovery bind");
    let udp_local_path = ServerLocalPath::new(1, udp_path);
    let udp_server = tokio::spawn(run_server_udp_listener(
        udp_endpoint,
        udp_local_path,
        server_context,
    ));

    let mut second_response = [0u8; 4];
    tokio::time::timeout(
        Duration::from_secs(10),
        client.read_exact(&mut second_response),
    )
    .await
    .expect("QUIC reattachment timeout")
    .expect("post-outage response");
    assert_eq!(&second_response, b"done");
    client.shutdown().await.expect("client shutdown");
    tokio::time::timeout(Duration::from_secs(5), &mut handler)
        .await
        .expect("handler completion timeout")
        .expect("handler join")
        .expect("handler result");

    udp_server.abort();
    let _ = udp_server.await;
    server_relay.abort();
    let _ = server_relay.await;
    target.await.expect("target join");
}

#[tokio::test]
async fn disconnected_logical_stream_expires_at_configured_retention_timeout() {
    let target_listener = TcpListener::bind("127.0.0.1:0").await.expect("target bind");
    let target_addr = target_listener.local_addr().expect("target addr");
    let target = tokio::spawn(async move {
        let (mut stream, _) = target_listener.accept().await.expect("target accept");
        let mut request = [0u8; 4];
        stream.read_exact(&mut request).await.expect("target read");
        assert_eq!(&request, b"ping");
        stream.write_all(b"pong").await.expect("target write");
        std::future::pending::<()>().await;
    });

    let path = reserve_tcp_path().await;
    let listener = bind_listener(&path).await.expect("path bind");
    let local_path = ServerLocalPath::new(0, path.clone());
    let ServerIdentityRuntime {
        paths: server_context,
        reliable_relay,
    } = server_runtime(OutboundConfig::Direct);
    let server_relay = tokio::spawn(reliable_relay.run());
    let server_path = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("path accept");
        handle_server_path(stream, local_path, server_context).await
    });

    let resources = ResourceLimits {
        tcp_path_heartbeat_interval: Duration::from_secs(60),
        tcp_path_heartbeat_timeout: Duration::from_secs(60),
        ..ResourceLimits::default()
    };
    let context =
        client_context_with_session_retention(vec![path], resources, Duration::from_millis(150));
    let health_context = context.clone();
    let (mut client, server) = duplex(4096);
    let mut handler = tokio::spawn(handle_socks5_client_stream(server, context));

    open_socks5_tcp_tunnel(&mut client, target_addr).await;
    client.write_all(b"ping").await.expect("payload write");
    let mut response = [0u8; 4];
    client
        .read_exact(&mut response)
        .await
        .expect("payload read");
    assert_eq!(&response, b"pong");

    server_path.abort();
    let _ = server_path.await;
    wait_for_tcp_path_detached(&health_context, 0).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !handler.is_finished(),
        "logical stream expired before its configured retention timeout"
    );

    let result = tokio::time::timeout(Duration::from_secs(1), &mut handler)
        .await
        .expect("retention expiry timeout")
        .expect("handler join");
    assert!(matches!(result, Err(RuntimeError::SessionRetentionTimeout)));

    target.abort();
    let _ = target.await;
    server_relay.abort();
    let _ = server_relay.await;
}

#[tokio::test]
async fn reliable_relay_heartbeat_timeout_enters_session_retention_without_a_survivor() {
    let (path, server_path) =
        spawn_reliable_relay_heartbeat_blackhole(Duration::from_millis(500)).await;
    let resources = ResourceLimits {
        tcp_path_heartbeat_interval: Duration::from_millis(10),
        tcp_path_heartbeat_timeout: Duration::from_millis(30),
        ..ResourceLimits::default()
    };
    let context =
        client_context_with_session_retention(vec![path], resources, Duration::from_millis(300));
    let health_context = context.clone();
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

    assert!(
        tokio::time::timeout(Duration::from_millis(150), &mut handler)
            .await
            .is_err(),
        "carrier heartbeat loss must not close the logical stream before retention expiry"
    );
    {
        let health = health_context.health().lock().expect("health lock");
        assert!(matches!(
            health.tcp[0].state,
            SchedulerPathState::Suspect | SchedulerPathState::Failed
        ));
    }

    let result = tokio::time::timeout(Duration::from_secs(1), &mut handler)
        .await
        .expect("session retention expiry")
        .expect("handler join");
    assert!(matches!(result, Err(RuntimeError::SessionRetentionTimeout)));

    server_path
        .await
        .expect("server join")
        .expect("heartbeat test server");
}

#[test]
fn tcp_path_activity_does_not_extend_pending_heartbeat_deadline() {
    let before = tokio::time::Instant::now();
    let mut next_heartbeat_at = before;
    let old_deadline = before + Duration::from_millis(1);
    let pending = Some((42, old_deadline));

    refresh_client_tcp_path_liveness_state(
        &mut next_heartbeat_at,
        Duration::from_secs(10),
        pending.is_some(),
    );

    assert_eq!(next_heartbeat_at, before);
    let Some((nonce, deadline)) = pending else {
        panic!("heartbeat should remain pending");
    };
    assert_eq!(nonce, 42);
    assert_eq!(deadline, old_deadline);
}

#[tokio::test]
async fn socks5_ingress_schedules_tcp_stream_to_best_configured_path() {
    let (target_addr, target) = spawn_echo_target().await;
    let high_latency_path = reserve_tcp_path_with_query("srtt-ms=200&rate-mbps=1000").await;
    let low_latency_path = reserve_tcp_path_with_query("srtt-ms=10&rate-mbps=50").await;
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
async fn socks5_ingress_starts_reliable_auto_latency_first() {
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
async fn http_connect_ingress_accepts_configured_basic_proxy_auth() {
    let (target_addr, target) = spawn_echo_target().await;
    let (path, server_path) = spawn_server_path(OutboundConfig::Direct).await;
    let context = ClientPathContext::new_with_proxy_auth(
        vec![path],
        security(),
        ResourceLimits::default(),
        ProxyAuthConfig::required("operator".to_string(), "secret".to_string()),
    )
    .expect("ctx");
    let (mut client, server) = duplex(4096);
    let handler = tokio::spawn(handle_http_connect_client_stream(server, context));

    client
        .write_all(
            format!(
                "CONNECT {target_addr} HTTP/1.1\r\nHost: {target_addr}\r\nProxy-Authorization: Basic b3BlcmF0b3I6c2VjcmV0\r\n\r\n"
            )
            .as_bytes(),
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
async fn http_connect_ingress_rejects_missing_basic_proxy_auth() {
    let context = ClientPathContext::new_with_proxy_auth(
        Vec::new(),
        security(),
        ResourceLimits::default(),
        ProxyAuthConfig::required("operator".to_string(), "secret".to_string()),
    )
    .expect("ctx");
    let (mut client, server) = duplex(1024);
    let handler = tokio::spawn(handle_http_connect_client_stream(server, context));

    client
        .write_all(b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n")
        .await
        .expect("request");
    let auth_required = http_connect::error_response(HttpStatus::ProxyAuthenticationRequired);
    let mut response = vec![0u8; auth_required.len()];
    client.read_exact(&mut response).await.expect("response");
    assert_eq!(response, auth_required);

    assert!(handler.await.expect("join").is_err());
}

#[tokio::test]
async fn http_connect_ingress_relays_tcp_payload_over_udp_stream_path() {
    let (target_addr, target) = spawn_echo_target().await;
    let (path, server_path) = spawn_udp_server_path(OutboundConfig::Direct).await;
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
    server_path.abort();
    let _ = server_path.await;
    target.await.expect("target join");
}

async fn open_socks5_udp_associate<S>(control_client: &mut S) -> SocketAddr
where
    S: AsyncRead + AsyncWrite + Unpin,
{
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
    SocketAddr::from((
        [
            associate_response[4],
            associate_response[5],
            associate_response[6],
            associate_response[7],
        ],
        u16::from_be_bytes([associate_response[8], associate_response[9]]),
    ))
}

async fn send_socks5_udp_ping(relay_addr: SocketAddr, target_addr: SocketAddr) {
    let udp_client = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("udp client bind");
    let request = socks5::udp_datagram(&TargetAddr::Ip(target_addr), b"ping").expect("udp request");
    udp_client
        .send_to(&request, relay_addr)
        .await
        .expect("send udp request");
    let mut response = [0u8; 128];
    let (len, _) =
        tokio::time::timeout(Duration::from_secs(2), udp_client.recv_from(&mut response))
            .await
            .expect("recv udp response timeout")
            .expect("recv udp response");
    let (datagram, consumed) = socks5::parse_udp_datagram(&response[..len]).expect("datagram");
    assert_eq!(consumed, len);
    assert_eq!(datagram.target, TargetAddr::Ip(target_addr));
    assert_eq!(datagram.payload, Bytes::from_static(b"pong"));
}

#[derive(Debug, Clone, Copy)]
enum ScriptedTcpDatagramAction {
    FeedbackThenClose,
    CloseBeforeFeedback,
    FeedbackThenHold,
}

async fn spawn_scripted_tcp_datagram_path(
    ready_delay: Duration,
    action: ScriptedTcpDatagramAction,
) -> (
    PathSpec,
    oneshot::Receiver<u32>,
    tokio::task::JoinHandle<Result<(), RuntimeError>>,
) {
    let path = reserve_tcp_path().await;
    let listener = bind_listener(&path).await.expect("bind scripted path");
    let (ttl_tx, ttl_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let security = security();
        let mut framed = EncryptedFramedStream::with_cipher_suite(
            stream,
            security.secret.as_bytes(),
            PeerRole::Server,
            CodecLimits::default(),
            security.cipher,
        )
        .expect("initialize encrypted stream");
        if !matches!(framed.read_frame().await?, Frame::SessionHello { .. }) {
            return Err(RuntimeError::Protocol("expected SESSION_HELLO"));
        }
        if !matches!(framed.read_frame().await?, Frame::SessionAuth { .. }) {
            return Err(RuntimeError::Protocol("expected SESSION_AUTH"));
        }
        let path_id = match framed.read_frame().await? {
            Frame::PathJoin { path_id, .. } => path_id,
            _ => return Err(RuntimeError::Protocol("expected PATH_JOIN")),
        };
        match framed.read_frame().await? {
            Frame::PathStatus {
                path_id: status_path_id,
                sequence: 0,
                ..
            } if status_path_id == path_id => {}
            _ => {
                return Err(RuntimeError::Protocol(
                    "invalid initial TCP path usage advertisement",
                ));
            }
        }
        tokio::time::sleep(ready_delay).await;
        framed
            .write_frames(&[
                Frame::SessionReady,
                Frame::PathStatus {
                    path_id,
                    sequence: 0,
                    usage: crate::protocol::PathUsage::Available,
                },
            ])
            .await?;
        framed.flush().await?;

        loop {
            match framed.read_frame().await? {
                Frame::DatagramData {
                    flow_id,
                    datagram_id,
                    ttl_ms,
                    ..
                } => {
                    let _ = ttl_tx.send(ttl_ms);
                    match action {
                        ScriptedTcpDatagramAction::CloseBeforeFeedback => return Ok(()),
                        ScriptedTcpDatagramAction::FeedbackThenClose
                        | ScriptedTcpDatagramAction::FeedbackThenHold => {
                            framed
                                .write_frame(&Frame::DatagramFeedback {
                                    flow_id,
                                    received: vec![datagram_ack_range(datagram_id)?],
                                })
                                .await?;
                            framed.flush().await?;
                            if matches!(action, ScriptedTcpDatagramAction::FeedbackThenHold) {
                                tokio::time::sleep(Duration::from_secs(2)).await;
                            }
                            return Ok(());
                        }
                    }
                }
                Frame::Ping { nonce } => {
                    framed.write_frame(&Frame::Pong { nonce }).await?;
                    framed.flush().await?;
                }
                Frame::OpenDatagramFlow { .. } | Frame::PathMetrics { .. } => {}
                _ => return Err(RuntimeError::Protocol("unexpected scripted TCP frame")),
            }
        }
    });
    (path, ttl_rx, task)
}

#[tokio::test]
async fn udp_datagram_path_relays_direct_udp_target() {
    let (target_addr, target) = spawn_udp_echo_target().await;
    let (path, server) = spawn_udp_server_path(OutboundConfig::Direct).await;

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
async fn udp_datagram_path_relays_upstream_socks5_udp_target() {
    let (proxy, proxy_task) = spawn_socks5_udp_proxy_once().await;
    let (path, server) = spawn_udp_server_path(OutboundConfig::Socks5 { proxy }).await;

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
    server.abort();
    let _ = server.await;
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
        DEFAULT_OUTBOUND_CONNECT_TIMEOUT,
        security(),
        MppPerformanceConfig::default(),
        ResourceLimits::default(),
        SessionConfig::default(),
        ManagementConfig::default(),
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
        DEFAULT_OUTBOUND_CONNECT_TIMEOUT,
        security(),
        MppPerformanceConfig::default(),
        ResourceLimits::default(),
        SessionConfig::default(),
        ManagementConfig::default(),
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
async fn socks5_udp_associate_relays_datagram_over_udp_path() {
    let (target_addr, target) = spawn_udp_echo_target_count(2).await;
    let (path, server) = spawn_udp_server_path(OutboundConfig::Direct).await;
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("ctx");
    let health_context = context.clone();
    let (mut control_client, control_server) = duplex(4096);
    let handler = tokio::spawn(handle_socks5_client_stream(control_server, context));

    let relay_addr = open_socks5_udp_associate(&mut control_client).await;
    for _ in 0..2 {
        send_socks5_udp_ping(relay_addr, target_addr).await;
    }
    control_client.shutdown().await.expect("control shutdown");

    handler.await.expect("handler join").expect("handler");
    {
        let health = health_context.health().lock().expect("health lock");
        assert_eq!(health.udp[0].state, SchedulerPathState::Active);
        assert!(health.udp[0].measured_srtt_ms.is_some());
        assert!(health.udp[0].measured_jitter_ms.is_some());
        assert_eq!(health.udp[0].measured_loss_rate, Some(0.0));
    }
    server.abort();
    let _ = server.await;
    target.await.expect("target join");
}

#[tokio::test]
async fn socks5_udp_associate_relays_datagram_over_encrypted_tcp_path() {
    let (target_addr, target) = spawn_udp_echo_target_count(2).await;
    let (path, server_path) = spawn_server_path(OutboundConfig::Direct).await;
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("ctx");
    let health_context = context.clone();
    let (mut control_client, control_server) = duplex(4096);
    let handler = tokio::spawn(handle_socks5_client_stream(control_server, context));

    let relay_addr = open_socks5_udp_associate(&mut control_client).await;
    for _ in 0..2 {
        send_socks5_udp_ping(relay_addr, target_addr).await;
    }
    control_client.shutdown().await.expect("control shutdown");

    handler.await.expect("handler join").expect("handler");
    {
        let health = health_context.health().lock().expect("health lock");
        assert_eq!(health.tcp[0].state, SchedulerPathState::Active);
        assert!(health.udp.is_empty());
    }
    server_path
        .await
        .expect("server join")
        .expect("server path");
    target.await.expect("target join");
}

#[tokio::test]
async fn tcp_datagram_feedback_then_carrier_close_does_not_replay() {
    let target = UdpSocket::bind("127.0.0.1:0").await.expect("target bind");
    let target_addr = target.local_addr().expect("target address");
    let (scripted_path, ttl_rx, scripted) = spawn_scripted_tcp_datagram_path(
        Duration::ZERO,
        ScriptedTcpDatagramAction::FeedbackThenClose,
    )
    .await;
    let (fallback_path, fallback) = spawn_server_path(OutboundConfig::Direct).await;
    let context = ClientPathContext::new(
        vec![scripted_path, fallback_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    let mut association = DatagramClientAssociation::new(context)
        .await
        .expect("association");

    let result = association
        .send_to_fresh_datagram_with_route_hint(
            TargetAddr::Ip(target_addr),
            Bytes::from_static(b"ping"),
            1_500,
            Some(RelayPathKey {
                underlay: UnderlayProtocol::Tcp,
                index: 0,
            }),
        )
        .await;
    assert!(result.is_err());
    assert!(ttl_rx.await.expect("scripted emission") > 0);
    scripted
        .await
        .expect("scripted join")
        .expect("scripted path");
    let mut packet = [0u8; 16];
    assert!(
        tokio::time::timeout(Duration::from_millis(150), target.recv_from(&mut packet))
            .await
            .is_err(),
        "an acknowledged request must not be emitted on the fallback path"
    );
    fallback.abort();
    let _ = fallback.await;
}

#[tokio::test]
async fn tcp_datagram_runtime_fallback_emits_at_most_twice() {
    let target = UdpSocket::bind("127.0.0.1:0").await.expect("target bind");
    let target_addr = target.local_addr().expect("target address");
    let (first_path, first_ttl, first) = spawn_scripted_tcp_datagram_path(
        Duration::ZERO,
        ScriptedTcpDatagramAction::CloseBeforeFeedback,
    )
    .await;
    let (second_path, second_ttl, second) = spawn_scripted_tcp_datagram_path(
        Duration::ZERO,
        ScriptedTcpDatagramAction::CloseBeforeFeedback,
    )
    .await;
    let (third_path, third) = spawn_server_path(OutboundConfig::Direct).await;
    let context = ClientPathContext::new(
        vec![first_path, second_path, third_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    let mut association = DatagramClientAssociation::new(context)
        .await
        .expect("association");

    let result = association
        .send_to_fresh_datagram_with_route_hint(
            TargetAddr::Ip(target_addr),
            Bytes::from_static(b"ping"),
            2_500,
            Some(RelayPathKey {
                underlay: UnderlayProtocol::Tcp,
                index: 0,
            }),
        )
        .await;
    assert!(result.is_err());
    assert!(first_ttl.await.expect("first emission") > 0);
    assert!(second_ttl.await.expect("second emission") > 0);
    first.await.expect("first join").expect("first path");
    second.await.expect("second join").expect("second path");
    let mut packet = [0u8; 16];
    assert!(
        tokio::time::timeout(Duration::from_millis(150), target.recv_from(&mut packet))
            .await
            .is_err(),
        "a third product emission must not reach the final carrier"
    );
    third.abort();
    let _ = third.await;
}

#[tokio::test]
async fn configured_udp_payload_limit_falls_through_to_tcp_without_emission() {
    let target = UdpSocket::bind("127.0.0.1:0").await.expect("target bind");
    let target_addr = target.local_addr().expect("target address");
    let target_task = tokio::spawn(async move {
        let mut packet = vec![0u8; 2_048];
        let (len, peer) = target.recv_from(&mut packet).await.expect("target recv");
        assert_eq!(len, 1_400);
        target.send_to(b"pong", peer).await.expect("target reply");
    });
    let mut udp_path = reserve_udp_path().await;
    udp_path.metadata.initial_srtt_ms = Some(10);
    udp_path.metadata.initial_rate = RateHint::BitsPerSecond(1_000_000_000);
    udp_path.metadata.max_datagram_payload_bytes = Some(1_200);
    let (mut tcp_path, tcp_server) = spawn_server_path(OutboundConfig::Direct).await;
    tcp_path.metadata.initial_srtt_ms = Some(100);
    let context = ClientPathContext::new(
        vec![udp_path, tcp_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    let telemetry_context = context.clone();
    let mut association = DatagramClientAssociation::new(context)
        .await
        .expect("association");

    let response = association
        .send_to_fresh_datagram_with_route_hint(
            TargetAddr::Ip(target_addr),
            Bytes::from(vec![7u8; 1_400]),
            2_000,
            Some(RelayPathKey {
                underlay: UnderlayProtocol::Udp,
                index: 0,
            }),
        )
        .await
        .expect("TCP fallback response");
    assert_eq!(response, Bytes::from_static(b"pong"));
    let telemetry = telemetry_context.telemetry_snapshot();
    assert_eq!(telemetry.datagram.io.to_peer_bytes, 1_400);
    assert_eq!(telemetry.datagram.io.to_peer_packets, 1);
    assert_eq!(telemetry.datagram.io.from_peer_bytes, 4);
    assert_eq!(telemetry.datagram.io.from_peer_packets, 1);
    assert_eq!(telemetry.datagram.flows.opened, 1);
    assert_eq!(telemetry.datagram.flows.active, 1);
    association.close().await.expect("association close");
    let telemetry = telemetry_context.telemetry_snapshot();
    assert_eq!(telemetry.datagram.flows.active, 0);
    assert_eq!(telemetry.datagram.flows.completed, 1);
    assert_eq!(telemetry.datagram.flows.failed, 0);
    drop(telemetry_context);
    tcp_server
        .await
        .expect("TCP server join")
        .expect("TCP server");
    target_task.await.expect("target task");
}

#[tokio::test]
async fn tcp_datagram_setup_consumes_the_original_absolute_ttl() {
    let target = UdpSocket::bind("127.0.0.1:0").await.expect("target bind");
    let target_addr = target.local_addr().expect("target address");
    let (path, ttl_rx, scripted) = spawn_scripted_tcp_datagram_path(
        Duration::from_millis(300),
        ScriptedTcpDatagramAction::FeedbackThenHold,
    )
    .await;
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("context");
    let mut association = DatagramClientAssociation::new(context)
        .await
        .expect("association");
    let started_at = Instant::now();

    let result = association
        .send_to_fresh_datagram_with_route_hint(
            TargetAddr::Ip(target_addr),
            Bytes::from_static(b"ping"),
            1_000,
            None,
        )
        .await;
    assert!(result.is_err());
    let emitted_ttl = ttl_rx.await.expect("emitted TTL");
    assert!(
        emitted_ttl < 850,
        "carrier setup must be deducted from DGRAM_DATA TTL, got {emitted_ttl} ms"
    );
    assert!(
        started_at.elapsed() < Duration::from_millis(1_200),
        "carrier setup and response wait must share one product expiry"
    );
    scripted.abort();
    let _ = scripted.await;
}

#[tokio::test]
async fn tcp_datagram_carrier_setup_uses_remaining_ttl_for_same_family_fallback() {
    let (target_addr, target) = spawn_udp_echo_target().await;
    let blackhole_path = reserve_tcp_path().await;
    let blackhole_listener = bind_listener(&blackhole_path)
        .await
        .expect("blackhole bind");
    let blackhole = tokio::spawn(async move {
        let (mut stream, _) = blackhole_listener.accept().await.expect("blackhole accept");
        let mut buffer = [0u8; 4096];
        loop {
            match stream.read(&mut buffer).await.expect("blackhole read") {
                0 => break,
                _ => continue,
            }
        }
    });
    let (working_path, server_path) = spawn_server_path(OutboundConfig::Direct).await;
    let context = ClientPathContext::new(
        vec![blackhole_path, working_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    let mut association = DatagramClientAssociation::new(context)
        .await
        .expect("association");
    let ttl_ms = 2_500;
    let started_at = Instant::now();
    let response = association
        .send_to_fresh_datagram_with_route_hint(
            TargetAddr::Ip(target_addr),
            Bytes::from_static(b"ping"),
            ttl_ms,
            Some(RelayPathKey {
                underlay: UnderlayProtocol::Tcp,
                index: 0,
            }),
        )
        .await
        .expect("fallback response");
    assert_eq!(response, Bytes::from_static(b"pong"));
    assert!(
        started_at.elapsed() < Duration::from_millis(u64::from(ttl_ms)),
        "a stalled first TCP carrier must leave TTL for the next TCP path"
    );

    association.close().await.expect("association close");
    blackhole.await.expect("blackhole join");
    server_path
        .await
        .expect("server join")
        .expect("server path");
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
        DEFAULT_OUTBOUND_CONNECT_TIMEOUT,
        security(),
        MppPerformanceConfig::default(),
        ResourceLimits::default(),
        SessionConfig::default(),
        ManagementConfig::default(),
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
async fn server_verifies_auth_sequence_and_rejects_wrong_secret() {
    let path = reserve_tcp_path().await;
    let listener = bind_listener(&path).await.expect("bind");
    let server_path = path.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let local_path = ServerLocalPath::new(0, server_path.clone());
        let ServerIdentityRuntime {
            paths,
            reliable_relay: _reliable_relay,
        } = new_identity_runtime(
            vec![server_path],
            OutboundConfig::Direct,
            DnsConfig::default(),
            DEFAULT_OUTBOUND_CONNECT_TIMEOUT,
            SecurityConfig::encrypted(
                SharedSecret::new(b"fedcba9876543210fedcba9876543210".to_vec()).expect("secret"),
            ),
            MppPerformanceConfig::default(),
            ResourceLimits::default(),
        );
        handle_server_path(stream, local_path, paths).await
    });

    let stream = tcp::connect_path(&path, TcpConnectOptions::default())
        .await
        .expect("connect");
    let mut client = EncryptedFramedStream::new(
        stream,
        b"0123456789abcdef",
        PeerRole::Client,
        CodecLimits::default(),
    )
    .expect("initialize encrypted stream");
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
