use super::*;
use crate::config::DEFAULT_OUTBOUND_CONNECT_TIMEOUT;

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
    let health = context.health.lock().expect("health lock");
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
    context.mark_tcp_path_open_success(0, Duration::from_millis(5), FlowLane::Throughput);

    probe_client_paths(&context, Duration::from_millis(20)).await;

    let health = context.health.lock().expect("health lock");
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

    let health = context.health.lock().expect("health lock");
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
        let health = context.health.lock().expect("health lock");
        assert_eq!(health.tcp[0].state, SchedulerPathState::Suspect);
        assert_eq!(health.tcp[0].consecutive_failures, 1);
        assert!(health.tcp[0].failed_until.is_none());
    }
    assert_eq!(
        context
            .ordered_tcp_path_indices(FlowLane::Latency, 512)
            .first()
            .copied(),
        Some(0)
    );

    probe_client_paths(&context, Duration::from_millis(50)).await;

    {
        let health = context.health.lock().expect("health lock");
        assert_eq!(health.tcp[0].state, SchedulerPathState::Suspect);
        assert_eq!(health.tcp[0].consecutive_failures, 2);
        assert!(health.tcp[0].failed_until.is_none());
    }
    assert_eq!(
        context
            .ordered_tcp_path_indices(FlowLane::Latency, 512)
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
async fn tcp_path_sessions_handle_multiple_single_path_interactive_streams() {
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
async fn reliable_relay_active_stream_heartbeat_timeout_does_not_abort_stream() {
    let (path, server_path) =
        spawn_reliable_relay_heartbeat_blackhole(Duration::from_millis(500)).await;
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
        let health = health_context.health.lock().expect("health lock");
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
        let health = health_context.health.lock().expect("health lock");
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
        handle_server_path(
            stream,
            ServerPathContext {
                tag: None,
                route_target: None,
                server_paths: Arc::new(vec![server_path]),
                outbound: OutboundConfig::Direct,
                outbound_dns: DnsConfig::default(),
                outbound_connect_timeout: DEFAULT_OUTBOUND_CONNECT_TIMEOUT,
                performance: MppPerformanceConfig::default(),
                codec_limits: CodecLimits::default(),
                mux_limits: ResourceLimits::default().into(),
                security: SecurityConfig::encrypted(
                    SharedSecret::new(b"fedcba9876543210fedcba9876543210".to_vec())
                        .expect("secret"),
                ),
                reliable_streams: Arc::new(ServerReliableStreamRegistry::default()),
                path_join_replay: Arc::new(Mutex::new(RecentIdCache::new(
                    path_join_replay_cache_capacity(ResourceLimits::default().max_streams),
                ))),
                max_reliable_streams: ResourceLimits::default().max_streams,
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
