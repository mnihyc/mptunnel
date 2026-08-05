use super::*;
use crate::config::{
    ClientPathConfig, DEFAULT_OUTBOUND_CONNECT_TIMEOUT, ResourceLimits, ServerDestinationAclConfig,
    SessionConfig,
};
use crate::outbound::OutboundConfig;
use crate::protocol::{DatagramFlowId, DatagramId, PathUsage, SessionId};
use crate::runtime::path::ServerLocalPath;
use crate::runtime::path::tcp::group::ClientTcpEndpointControlState;
use crate::transport::SystemCarrierNetworkProvider;

const FULL_STACK_RESPONSE_TIMEOUT: Duration = Duration::from_secs(15);

fn spawn_tcp_pool_reconciliation(context: &ClientPathContext) -> tokio::task::JoinHandle<()> {
    let context = context.clone();
    tokio::spawn(async move {
        let now = tokio::time::Instant::now();
        let mut retry = vec![
            crate::runtime::path::tcp::group::ClientTcpMemberRetry::new(now);
            context.tcp_sessions.len()
        ];
        context
            .tcp_carrier_groups
            .reconcile(
                &context,
                crate::config::DEFAULT_PATH_PROBE_TIMEOUT,
                crate::config::DEFAULT_PATH_PROBE_INTERVAL,
                &mut retry,
            )
            .await;
    })
}

fn local_proxy_auth() -> crate::ingress::ProxyAuthConfig {
    let user = crate::ingress::LocalProxyUser::new(
        "operator".to_string(),
        crate::product::PrincipalId::parse("daily-user").expect("principal"),
        "operator".to_string(),
        "secret".to_string(),
    )
    .expect("local proxy user");
    crate::ingress::ProxyAuthConfig::required([user]).expect("proxy auth")
}

fn local_port_forward_router() -> crate::runtime::product_policy::ClientIngressRouter {
    local_port_forward_runtime().0
}

fn local_port_forward_runtime() -> (
    crate::runtime::product_policy::ClientIngressRouter,
    crate::runtime::telemetry::RuntimeTelemetry,
) {
    let outbound = crate::product::OutboundId::parse("local-direct").expect("outbound ID");
    let policy = crate::config::ProductPolicyConfig {
        generation: 1,
        routes: vec![crate::product::RouteRuleSpec::new(
            crate::product::RuleId::parse("default").expect("route rule ID"),
            crate::product::RouteMatchSpec::default(),
            crate::product::RouteAction::new(
                crate::product::EgressAction::Outbound(outbound.clone()),
                None,
                crate::product::TrafficIntent::Interactive,
            ),
        )],
        destination_acl: vec![crate::product::AclRuleSpec::new(
            crate::product::RuleId::parse("allow-local-test-target").expect("ACL rule ID"),
            crate::product::RouteMatchSpec::default(),
            crate::product::AclEffect::AllowRestricted,
        )],
    };
    let telemetry = crate::runtime::telemetry::RuntimeTelemetry::generation_owner(8);
    let registry = crate::runtime::outbound_registry::RuntimeOutboundRegistryShell::compile(
        [
            crate::runtime::outbound_registry::RuntimeOutboundLeaf::Local {
                id: outbound,
                config: OutboundConfig::Direct,
                connect_timeout: Duration::from_secs(2),
                native_sockets: Arc::new(crate::transport::SystemNativeSocketConfigurator),
            },
        ],
        &[],
    )
    .expect("local outbound registry")
    .with_product_telemetry(telemetry.clone())
    .with_dns(crate::runtime::outbound_registry::test_dns_generation());
    (
        crate::runtime::product_policy::ClientIngressRouter::new(&policy, registry)
            .expect("local ingress router"),
        telemetry,
    )
}

fn port_forward_inbound() -> crate::product::InboundId {
    crate::product::InboundId::parse("local-forward").expect("inbound ID")
}

fn test_server_destination_acl() -> ServerDestinationAclConfig {
    ServerDestinationAclConfig {
        generation: 1,
        rules: vec![crate::product::AclRuleSpec::new(
            crate::product::RuleId::parse("test-allow-restricted").expect("test rule ID"),
            crate::product::RouteMatchSpec::default(),
            crate::product::AclEffect::AllowRestricted,
        )],
    }
}

#[tokio::test]
async fn fixed_tcp_forward_streams_through_the_product_outbound_pipeline() {
    let target_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind TCP target");
    let target_address = target_listener.local_addr().expect("TCP target address");
    let (target_received_tx, target_received_rx) = oneshot::channel();
    let (target_release_tx, target_release_rx) = oneshot::channel();
    let target = tokio::spawn(async move {
        let (mut stream, _) = target_listener
            .accept()
            .await
            .expect("accept target stream");
        let mut request = [0u8; 4];
        stream
            .read_exact(&mut request)
            .await
            .expect("read target request");
        assert_eq!(&request, b"ping");
        target_received_tx
            .send(())
            .expect("signal target request received");
        target_release_rx.await.expect("release target response");
        stream.write_all(b"pong").await.expect("write target reply");
        stream.shutdown().await.expect("target shutdown");
    });

    let forward_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind TCP forward");
    let forward_address = forward_listener.local_addr().expect("TCP forward address");
    let (router, telemetry) = local_port_forward_runtime();
    let forward = tokio::spawn(run_tcp_forward_client_listener(
        forward_listener,
        Arc::new(TargetAddr::Domain {
            host: "localhost".to_string(),
            port: target_address.port(),
        }),
        router,
        port_forward_inbound(),
        Arc::new(tokio::sync::Semaphore::new(1)),
    ));

    let mut client = TcpStream::connect(forward_address)
        .await
        .expect("connect TCP forward");
    client
        .write_all(b"ping")
        .await
        .expect("write client request");
    target_received_rx.await.expect("target request signal");
    let active = telemetry.snapshot();
    assert_eq!(active.reliable.flows.opened, 1);
    assert_eq!(active.reliable.flows.active, 1);
    assert_eq!(active.reliable.io.to_peer_bytes, 4);
    assert_eq!(active.active_flows.len(), 1);
    assert_eq!(active.active_flows[0].display_id, 1);
    assert_eq!(
        active.active_flows[0]
            .origin
            .as_ref()
            .expect("flow origin")
            .inbound
            .as_str(),
        "local-forward"
    );
    assert_eq!(
        active.active_flows[0]
            .selection
            .as_ref()
            .expect("flow selection")
            .outbound
            .as_str(),
        "local-direct"
    );
    assert_eq!(
        active.active_flows[0]
            .target
            .as_ref()
            .expect("original target")
            .authority(),
        format!("localhost:{}", target_address.port())
    );

    let mut overloaded = TcpStream::connect(forward_address)
        .await
        .expect("connect overloaded TCP client");
    let mut overload_probe = [0u8; 1];
    let overload_result =
        tokio::time::timeout(Duration::from_secs(1), overloaded.read(&mut overload_probe))
            .await
            .expect("overloaded connection must be shed without waiting");
    assert!(
        matches!(overload_result, Ok(0) | Err(_)),
        "overloaded connection unexpectedly remained open"
    );

    target_release_tx.send(()).expect("release target response");
    let mut reply = [0u8; 4];
    tokio::time::timeout(Duration::from_secs(2), client.read_exact(&mut reply))
        .await
        .expect("TCP forward response timeout")
        .expect("read TCP forward response");
    assert_eq!(&reply, b"pong");

    target.await.expect("target task");
    drop(client);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if telemetry.snapshot().reliable.flows.active == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("native TCP telemetry retirement");
    let completed = telemetry.snapshot();
    assert_eq!(completed.reliable.io.to_peer_bytes, 4);
    assert_eq!(completed.reliable.io.from_peer_bytes, 4);
    assert_eq!(completed.reliable.flows.completed, 1);
    assert_eq!(completed.reliable.flows.failed, 0);
    forward.abort();
    let _ = forward.await;
}

#[tokio::test]
async fn fixed_udp_forward_preserves_two_source_response_mappings() {
    let target_socket = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind UDP target");
    let target_address = target_socket.local_addr().expect("UDP target address");
    let target = tokio::spawn(async move {
        let mut packet = [0u8; 64];
        let first = target_socket
            .recv_from(&mut packet)
            .await
            .expect("first target receive");
        let first_payload = packet[..first.0].to_vec();
        let second = target_socket
            .recv_from(&mut packet)
            .await
            .expect("second target receive");
        let second_payload = packet[..second.0].to_vec();

        let mut second_reply = b"reply:".to_vec();
        second_reply.extend_from_slice(&second_payload);
        target_socket
            .send_to(&second_reply, second.1)
            .await
            .expect("second target reply");
        let mut first_reply = b"reply:".to_vec();
        first_reply.extend_from_slice(&first_payload);
        target_socket
            .send_to(&first_reply, first.1)
            .await
            .expect("first target reply");
    });

    let forward_socket = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind UDP forward");
    let forward_address = forward_socket.local_addr().expect("UDP forward address");
    let (router, telemetry) = local_port_forward_runtime();
    let forward = tokio::spawn(run_udp_forward_client_socket(
        forward_socket,
        Arc::new(TargetAddr::Ip(target_address)),
        MuxLimits::default(),
        router,
        port_forward_inbound(),
        Arc::new(tokio::sync::Semaphore::new(2)),
        Duration::from_secs(5),
        30_000,
    ));
    let first_client = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind first UDP client");
    let second_client = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind second UDP client");
    first_client
        .send_to(b"first", forward_address)
        .await
        .expect("first forward request");
    second_client
        .send_to(b"second", forward_address)
        .await
        .expect("second forward request");

    let mut first_reply = [0u8; 64];
    let first_length =
        tokio::time::timeout(Duration::from_secs(2), first_client.recv(&mut first_reply))
            .await
            .expect("first response timeout")
            .expect("first response");
    let mut second_reply = [0u8; 64];
    let second_length = tokio::time::timeout(
        Duration::from_secs(2),
        second_client.recv(&mut second_reply),
    )
    .await
    .expect("second response timeout")
    .expect("second response");
    assert_eq!(&first_reply[..first_length], b"reply:first");
    assert_eq!(&second_reply[..second_length], b"reply:second");

    let snapshot = telemetry.snapshot();
    assert_eq!(snapshot.datagram.flows.opened, 2);
    assert_eq!(snapshot.datagram.flows.active, 2);
    assert_eq!(snapshot.datagram.io.to_peer_packets, 2);
    assert_eq!(snapshot.datagram.io.to_peer_bytes, 11);
    assert_eq!(snapshot.datagram.io.from_peer_packets, 2);
    assert_eq!(snapshot.datagram.io.from_peer_bytes, 23);
    assert!(
        snapshot.active_flows.iter().all(|flow| {
            flow.origin
                .as_ref()
                .is_some_and(|origin| origin.inbound.as_str() == "local-forward")
                && flow
                    .selection
                    .as_ref()
                    .is_some_and(|selection| selection.outbound.as_str() == "local-direct")
        }),
        "every native UDP association retains immutable Product attribution"
    );

    target.await.expect("target task");
    forward.abort();
    let _ = forward.await;
}

#[tokio::test]
async fn fixed_udp_forward_bounds_associations_and_reclaims_idle_sources() {
    let target_socket = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind UDP target");
    let target_address = target_socket.local_addr().expect("UDP target address");
    let forward_socket = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind UDP forward");
    let forward_address = forward_socket.local_addr().expect("UDP forward address");
    let forward = tokio::spawn(run_udp_forward_client_socket(
        forward_socket,
        Arc::new(TargetAddr::Ip(target_address)),
        MuxLimits::default(),
        local_port_forward_router(),
        port_forward_inbound(),
        Arc::new(tokio::sync::Semaphore::new(1)),
        Duration::from_millis(120),
        30_000,
    ));
    let first_client = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind first UDP client");
    let second_client = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind second UDP client");
    let mut packet = [0u8; 64];

    first_client
        .send_to(b"first", forward_address)
        .await
        .expect("first forward request");
    let (first_length, first_outbound) =
        tokio::time::timeout(Duration::from_secs(2), target_socket.recv_from(&mut packet))
            .await
            .expect("first target timeout")
            .expect("first target request");
    assert_eq!(&packet[..first_length], b"first");
    target_socket
        .send_to(b"first-reply", first_outbound)
        .await
        .expect("first target reply");
    let first_reply_length =
        tokio::time::timeout(Duration::from_secs(2), first_client.recv(&mut packet))
            .await
            .expect("first reply timeout")
            .expect("first reply");
    assert_eq!(&packet[..first_reply_length], b"first-reply");

    second_client
        .send_to(b"blocked", forward_address)
        .await
        .expect("blocked forward request");
    assert!(
        tokio::time::timeout(
            Duration::from_millis(40),
            target_socket.recv_from(&mut packet)
        )
        .await
        .is_err(),
        "a second source must not exceed the configured association cap"
    );

    tokio::time::sleep(Duration::from_millis(120)).await;
    second_client
        .send_to(b"after-idle", forward_address)
        .await
        .expect("post-idle forward request");
    let (second_length, second_outbound) =
        tokio::time::timeout(Duration::from_secs(2), target_socket.recv_from(&mut packet))
            .await
            .expect("post-idle target timeout")
            .expect("post-idle target request");
    assert_eq!(&packet[..second_length], b"after-idle");
    target_socket
        .send_to(b"second-reply", second_outbound)
        .await
        .expect("second target reply");
    let second_reply_length =
        tokio::time::timeout(Duration::from_secs(2), second_client.recv(&mut packet))
            .await
            .expect("second reply timeout")
            .expect("second reply");
    assert_eq!(&packet[..second_reply_length], b"second-reply");

    forward.abort();
    let _ = forward.await;
}

fn client_context_with_session_retention(
    paths: Vec<PathSpec>,
    resources: ResourceLimits,
    retention_timeout: Duration,
) -> ClientPathContext {
    let paths = paths
        .into_iter()
        .enumerate()
        .map(|(index, spec)| ClientPathConfig {
            name: format!("path-{}", index + 1),
            tls: crate::transport::encrypted::test_client_tls_config(),
            spec,
            security: security(),
        })
        .collect();
    ClientPathContext::new_with_runtime_options(
        paths,
        resources,
        None,
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
    let result = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let detached = {
                let health = context.health().lock().expect("health lock");
                let path = health.tcp.get(path_index).expect("TCP path health");
                context.tcp_sessions[path_index]
                    .connection_instance_id()
                    .is_none()
                    && path.state != SchedulerPathState::Active
            };
            if detached {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    if result.is_err() {
        let (state, active_flows, relay_bytes_in_flight, relay_queue_bytes) = {
            let health = context.health().lock().expect("health lock");
            let path = health.tcp.get(path_index).expect("TCP path health");
            (
                path.state,
                path.active_flows,
                path.relay_bytes_in_flight,
                path.relay_queue_bytes,
            )
        };
        panic!(
            "TCP carrier did not detach: state={:?} active_flows={} relay_bytes_in_flight={} relay_queue_bytes={}",
            state, active_flows, relay_bytes_in_flight, relay_queue_bytes
        );
    }
}

async fn wait_for_tcp_ready_count(context: &ClientPathContext, expected: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let ready = context
                .tcp_sessions
                .iter()
                .filter(|session| session.is_connection_ready())
                .count();
            if ready == expected {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("bounded TCP carrier readiness did not converge");
}

async fn bind_contiguous_tcp_listener_pair() -> (u16, TcpListener, TcpListener) {
    loop {
        let first = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind first ranged TCP listener");
        let first_port = first.local_addr().expect("first listener address").port();
        let Some(second_port) = first_port.checked_add(1) else {
            continue;
        };
        if let Ok(second) = TcpListener::bind(("127.0.0.1", second_port)).await {
            return (first_port, first, second);
        }
    }
}

struct RangedTcpCarrierServer {
    first_port: u16,
    paths: crate::runtime::path::ServerPathContext,
    active: Arc<std::sync::atomic::AtomicUsize>,
    maximum_active: Arc<std::sync::atomic::AtomicUsize>,
    accepted: Arc<[std::sync::atomic::AtomicUsize; 2]>,
    carriers: tokio::task::JoinHandle<()>,
    relay: tokio::task::JoinHandle<Result<(), RuntimeError>>,
}

impl RangedTcpCarrierServer {
    async fn spawn() -> Self {
        let (first_port, first_listener, second_listener) =
            bind_contiguous_tcp_listener_pair().await;
        let server_path = format!("tcp://127.0.0.1:{first_port}")
            .parse::<PathSpec>()
            .expect("server TCP path");
        let local_path = ServerLocalPath::new(0, server_path);
        let ServerIdentityRuntime {
            paths,
            reliable_relay,
        } = server_runtime(OutboundConfig::Direct);
        let relay = tokio::spawn(reliable_relay.run());
        let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let maximum_active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let accepted = Arc::new([
            std::sync::atomic::AtomicUsize::new(0),
            std::sync::atomic::AtomicUsize::new(0),
        ]);
        let carriers = {
            let active = active.clone();
            let maximum_active = maximum_active.clone();
            let accepted = accepted.clone();
            let server_context = paths.clone();
            tokio::spawn(async move {
                let mut sessions = tokio::task::JoinSet::new();
                loop {
                    let (stream, listener_index) = tokio::select! {
                        accepted = first_listener.accept() => {
                            (accepted.expect("accept first ranged carrier").0, 0)
                        }
                        accepted = second_listener.accept() => {
                            (accepted.expect("accept second ranged carrier").0, 1)
                        }
                    };
                    accepted[listener_index].fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                    let active = active.clone();
                    let maximum_active = maximum_active.clone();
                    let local_path = local_path.clone();
                    let server_context = server_context.clone();
                    sessions.spawn(async move {
                        let current = active.fetch_add(1, std::sync::atomic::Ordering::AcqRel) + 1;
                        maximum_active.fetch_max(current, std::sync::atomic::Ordering::AcqRel);
                        let result = handle_server_path(stream, local_path, server_context).await;
                        active.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
                        result
                    });
                    while sessions.try_join_next().is_some() {}
                }
            })
        };
        Self {
            first_port,
            paths,
            active,
            maximum_active,
            accepted,
            carriers,
            relay,
        }
    }

    async fn shutdown(self) {
        self.carriers.abort();
        let _ = self.carriers.await;
        self.relay.abort();
        let _ = self.relay.await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn mixed_server_listeners_apply_local_policy_independent_of_wire_path_id() {
    let tcp_path = reserve_tcp_path_with_query("srtt-ms=20&rate-mbps=100&tcp-carriers=1-1").await;
    let udp_port = reserve_process_unique_udp_port().await;
    let udp_path = format!("udp://127.0.0.1:{udp_port}?srtt-ms=90&rate-mbps=400&backup=true")
        .parse::<PathSpec>()
        .expect("UDP backup path");
    let server = tokio::spawn(run_server(
        vec![tcp_path.clone(), udp_path.clone()],
        OutboundConfig::Direct,
        DEFAULT_OUTBOUND_CONNECT_TIMEOUT,
        test_server_destination_acl(),
        server_security(),
        crate::transport::encrypted::test_server_tls_config(),
        MppPerformanceConfig::default(),
        ResourceLimits::default(),
        SessionConfig::default(),
        ManagementConfig::default(),
        None,
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
async fn initial_path_probe_retains_tcp_carrier_without_stream_load() {
    let (path, server) = spawn_server_path(OutboundConfig::Direct).await;
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("ctx");

    probe_client_paths(&context, Duration::from_secs(1)).await;

    {
        let health = context.health().lock().expect("health lock");
        assert_eq!(health.tcp[0].state, SchedulerPathState::Active);
        assert!(health.tcp[0].measured_srtt_ms.is_some());
        assert_eq!(health.tcp[0].active_flows, 0);
        assert_eq!(health.tcp[0].relay_bytes_in_flight, 0);
    }
    assert_eq!(
        context.tcp_sessions[0]
            .prepare_connection(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .expect("prepared TCP carrier"),
        None,
        "the initial probe must retain the authenticated product carrier",
    );

    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn session_owner_reconciles_bounded_target_and_retires_disabled_group() {
    enum CarrierServerControl {
        Abort(usize),
    }

    let path = reserve_tcp_path_with_query("tcp-carriers=2-3").await;
    let listener = bind_listener(&path).await.expect("bind carrier listener");
    let local_path = ServerLocalPath::new(0, path.clone());
    let ServerIdentityRuntime {
        paths: server_context,
        reliable_relay,
    } = server_runtime(OutboundConfig::Direct);
    let snapshot_context = server_context.clone();
    let server_relay = tokio::spawn(reliable_relay.run());
    let (server_control, mut server_commands) = mpsc::unbounded_channel::<CarrierServerControl>();
    let carrier_server = tokio::spawn(async move {
        let mut sessions = tokio::task::JoinSet::new();
        let mut abort_handles = Vec::new();
        loop {
            tokio::select! {
                accepted_stream = listener.accept() => {
                    let (stream, _) = accepted_stream.expect("accept carrier");
                    let local_path = local_path.clone();
                    let server_context = server_context.clone();
                    abort_handles.push(sessions.spawn(async move {
                        handle_server_path(stream, local_path, server_context).await
                    }));
                }
                command = server_commands.recv() => {
                    match command {
                        Some(CarrierServerControl::Abort(index)) => {
                            if let Some(handle) = abort_handles.get(index) {
                                handle.abort();
                            }
                        }
                        None => break,
                    }
                }
                Some(_) = sessions.join_next(), if !sessions.is_empty() => {}
            }
        }
    });

    let context = ClientPathContext::new_with_carrier_network(
        vec![ClientPathConfig {
            name: "primary".to_string(),
            tls: crate::transport::encrypted::test_client_tls_config(),
            spec: path,
            security: security(),
        }],
        ResourceLimits::default(),
        None,
        0,
        Arc::new(SystemCarrierNetworkProvider),
    )
    .expect("client carrier context");
    let session_id = context.session_id;
    let path_service = tokio::spawn(run_client_path_service(
        context.clone(),
        Duration::from_secs(2),
        Duration::from_secs(1),
    ));

    wait_for_tcp_ready_count(&context, 3).await;
    let initial_client_instances = context
        .tcp_sessions
        .iter()
        .map(|session| {
            session
                .connection_instance_id()
                .expect("bounded-pool carrier instance")
        })
        .collect::<Vec<_>>();
    let initial = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let snapshot = snapshot_context.reliable_streams.management_snapshot();
            if snapshot.paths.len() == 3 {
                break snapshot;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("initial server carrier inventory");
    assert_eq!(
        initial.paths.len(),
        3,
        "the session owner must reconcile the configured healthy maximum"
    );
    assert!(
        initial
            .paths
            .iter()
            .all(|path| path.session_id == session_id),
        "bounded-pool carriers must belong to one MPP session"
    );
    assert_eq!(
        initial
            .paths
            .iter()
            .map(|path| path.path_id)
            .collect::<HashSet<_>>()
            .len(),
        initial.paths.len(),
        "simultaneous TCP carriers require distinct wire labels"
    );
    let initial_instances = initial
        .paths
        .iter()
        .map(|path| path.path_instance_id)
        .collect::<HashSet<_>>();

    // The existing retry interval is also the churn gate. Let the initial
    // carriers become stable before proving event-driven replacement.
    tokio::time::sleep(Duration::from_millis(2100)).await;
    server_control
        .send(CarrierServerControl::Abort(0))
        .expect("abort exact server carrier");
    tokio::time::timeout(Duration::from_millis(1500), async {
        loop {
            let current = context
                .tcp_sessions
                .iter()
                .map(ClientTcpPathSessionHandle::connection_instance_id)
                .collect::<Vec<_>>();
            if current.iter().all(Option::is_some)
                && current
                    .iter()
                    .zip(&initial_client_instances)
                    .filter(|(current, initial)| **current != Some(**initial))
                    .count()
                    == 1
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("exact loss must wake replacement before periodic retry");
    let replacement = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let snapshot = snapshot_context.reliable_streams.management_snapshot();
            if snapshot.paths.len() == 3
                && snapshot
                    .paths
                    .iter()
                    .any(|path| !initial_instances.contains(&path.path_instance_id))
            {
                break snapshot;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("replacement server carrier inventory");
    assert!(
        replacement
            .paths
            .iter()
            .all(|path| path.session_id == session_id),
        "replacement must preserve logical session identity"
    );
    let initial_pairs = initial
        .paths
        .iter()
        .map(|path| (path.path_id, path.path_instance_id))
        .collect::<HashSet<_>>();
    let replacement_pairs = replacement
        .paths
        .iter()
        .map(|path| (path.path_id, path.path_instance_id))
        .collect::<HashSet<_>>();
    assert_eq!(
        initial_pairs.intersection(&replacement_pairs).count(),
        2,
        "the two unaffected bounded-pool carriers must remain exact"
    );
    assert_eq!(
        replacement
            .paths
            .iter()
            .map(|path| path.path_id)
            .collect::<HashSet<_>>()
            .len(),
        replacement.paths.len(),
        "simultaneously live replacement carriers require distinct wire PathIds"
    );

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
            .expect("target post-drain read");
        assert_eq!(&second, b"next");
        stream
            .write_all(b"done")
            .await
            .expect("target post-drain write");
        stream.shutdown().await.expect("target shutdown");
    });
    let (mut product_client, product_server) = duplex(4096);
    let product = tokio::spawn(handle_socks5_client_stream(product_server, context.clone()));
    open_socks5_tcp_tunnel(&mut product_client, target_addr).await;
    product_client
        .write_all(b"ping")
        .await
        .expect("pre-drain payload");
    let mut first_response = [0u8; 4];
    product_client
        .read_exact(&mut first_response)
        .await
        .expect("pre-drain response");
    assert_eq!(&first_response, b"pong");

    let stable_client_instances = context
        .tcp_sessions
        .iter()
        .map(ClientTcpPathSessionHandle::connection_instance_id)
        .collect::<Vec<_>>();
    let stable_server_instances = replacement
        .paths
        .iter()
        .map(|path| path.path_instance_id)
        .collect::<HashSet<_>>();
    context.set_tcp_endpoint_control(0, ClientTcpEndpointControlState::Disabled);
    // A later generation must not cancel drains already requested by the
    // disabled generation.
    context.set_tcp_endpoint_control(0, ClientTcpEndpointControlState::Enabled);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let client_instances = context
                .tcp_sessions
                .iter()
                .map(ClientTcpPathSessionHandle::connection_instance_id)
                .collect::<Vec<_>>();
            let server_paths = snapshot_context
                .reliable_streams
                .management_snapshot()
                .paths;
            if client_instances.iter().all(Option::is_some)
                && client_instances
                    .iter()
                    .all(|instance| !stable_client_instances.contains(instance))
                && server_paths.len() == 3
                && server_paths
                    .iter()
                    .all(|path| !stable_server_instances.contains(&path.path_instance_id))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("re-enable must finish old drains before restoring the fresh bounded target");
    let restored_client_instances = context
        .tcp_sessions
        .iter()
        .map(|session| {
            session
                .connection_instance_id()
                .expect("re-enabled bounded-pool carrier")
        })
        .collect::<Vec<_>>();
    assert!(
        restored_client_instances
            .iter()
            .all(|instance| !stable_client_instances.contains(&Some(*instance))),
        "re-enable must establish fresh physical instances after terminal drain"
    );
    product_client
        .write_all(b"next")
        .await
        .expect("post-drain payload");
    let mut second_response = [0u8; 4];
    tokio::time::timeout(
        Duration::from_secs(5),
        product_client.read_exact(&mut second_response),
    )
    .await
    .expect("post-drain response timeout")
    .expect("post-drain response");
    assert_eq!(&second_response, b"done");
    product_client
        .shutdown()
        .await
        .expect("product client shutdown");
    product.await.expect("product join").expect("product relay");
    target.await.expect("target join");

    path_service.abort();
    let _ = path_service.await;
    carrier_server.abort();
    let _ = carrier_server.await;
    server_relay.abort();
    let _ = server_relay.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fenced_tcp_data_plane_instance_is_replaced_by_pool_reconciliation() {
    let carrier_server = RangedTcpCarrierServer::spawn().await;
    let client_path = format!(
        "tcp://127.0.0.1:{}?tcp-carriers=1-1",
        carrier_server.first_port
    )
    .parse::<PathSpec>()
    .expect("client TCP path");
    let context = ClientPathContext::new_with_carrier_network(
        vec![ClientPathConfig {
            name: "minimum".to_string(),
            tls: crate::transport::encrypted::test_client_tls_config(),
            spec: client_path,
            security: security(),
        }],
        ResourceLimits::default(),
        None,
        0,
        Arc::new(SystemCarrierNetworkProvider),
    )
    .expect("client carrier context");
    let retry_interval = Duration::from_secs(2);
    let path_service = tokio::spawn(run_client_path_service(
        context.clone(),
        retry_interval,
        Duration::from_secs(2),
    ));
    wait_for_tcp_ready_count(&context, 1).await;
    let initial_instance = context.tcp_sessions[0]
        .connection_instance_id()
        .expect("initial bounded-pool instance");

    // A carrier that survives the existing connection-attempt churn gate is
    // eligible for event-driven replacement after an exact data-plane fence.
    tokio::time::sleep(retry_interval + Duration::from_millis(100)).await;

    context.mark_relay_path_data_plane_failure(RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        },
        path_instance_id: initial_instance,
        attachment_id: 1,
    });
    assert_eq!(
        context.tcp_sessions[0].connection_instance_id(),
        None,
        "fenced physical instance must lose readiness immediately"
    );

    tokio::time::timeout(Duration::from_millis(1500), async {
        loop {
            let replacement = context.tcp_sessions[0].connection_instance_id();
            let active = {
                let health = context.health().lock().expect("client path health");
                health.tcp[0].state == SchedulerPathState::Active
                    && health.tcp[0].path_instance_id() == replacement
            };
            if replacement.is_some_and(|replacement| replacement != initial_instance) && active {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("pool reconciliation did not replace its stable fenced physical instance");

    path_service.abort();
    let _ = path_service.await;
    carrier_server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ranged_tcp_bounded_pool_rotates_every_due_member_after_product_quiescence() {
    let carrier_server = RangedTcpCarrierServer::spawn().await;
    let first_port = carrier_server.first_port;
    let second_port = first_port + 1;
    let client_path = format!(
        "tcp://127.0.0.1:{first_port}-{second_port}?tcp-carriers=1-3&port-hop-interval-ms=5000"
    )
    .parse::<PathSpec>()
    .expect("ranged client TCP path");
    let snapshot_context = carrier_server.paths.clone();
    let accepted = carrier_server.accepted.clone();

    let context = ClientPathContext::new_with_carrier_network(
        vec![ClientPathConfig {
            name: "ranged".to_string(),
            tls: crate::transport::encrypted::test_client_tls_config(),
            spec: client_path,
            security: security(),
        }],
        ResourceLimits::default(),
        None,
        0,
        Arc::new(SystemCarrierNetworkProvider),
    )
    .expect("ranged client carrier context");
    let path_service = tokio::spawn(run_client_path_service(
        context.clone(),
        Duration::from_secs(60),
        Duration::from_secs(2),
    ));
    wait_for_tcp_ready_count(&context, 3).await;
    let initial_server = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let snapshot = snapshot_context.reliable_streams.management_snapshot();
            if snapshot.paths.len() == 3 {
                break snapshot;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("initial ranged server carrier inventory");
    let initial_server_instances = initial_server
        .paths
        .iter()
        .map(|path| path.path_instance_id)
        .collect::<HashSet<_>>();
    let initial_session_id = initial_server.paths[0].session_id;
    let initial_members = context
        .tcp_sessions
        .iter()
        .map(|session| {
            (
                session
                    .connection_instance_id()
                    .expect("initial bounded-pool instance"),
                session
                    .connection_remote_port()
                    .expect("initial bounded-pool destination port"),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(context.tcp_carrier_groups.occupied(0), Some(3));

    let target_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind replacement target");
    let target_addr = target_listener
        .local_addr()
        .expect("replacement target address");
    let target = tokio::spawn(async move {
        let (mut stream, _) = target_listener.accept().await.expect("accept target");
        let mut payload = [0_u8; 4];
        loop {
            match stream.read_exact(&mut payload).await {
                Ok(_) => stream
                    .write_all(&payload)
                    .await
                    .expect("echo target payload"),
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(error) => panic!("read target payload: {error}"),
            }
        }
    });
    let (mut product_client, product_server) = duplex(4096);
    let product = tokio::spawn(handle_socks5_client_stream(product_server, context.clone()));
    open_socks5_tcp_tunnel(&mut product_client, target_addr).await;

    {
        let health = context.health().lock().expect("bounded-pool health");
        assert_eq!(health.tcp[0].active_flows, 1);
        assert!(health.tcp[1..].iter().all(|path| path.active_flows == 0));
    }

    // The hop interval makes replacement eligible; it never permits an active
    // Product attachment to be moved between physical TCP instances.
    for sequence in 0_u32..6 {
        let payload = sequence.to_be_bytes();
        product_client
            .write_all(&payload)
            .await
            .expect("write while planned replacement is deferred");
        let mut echoed = [0_u8; 4];
        tokio::time::timeout(
            Duration::from_secs(3),
            product_client.read_exact(&mut echoed),
        )
        .await
        .expect("deferred replacement interrupted Product delivery")
        .expect("read while planned replacement is deferred");
        assert_eq!(echoed, payload);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    for (index, (initial_instance, initial_port)) in initial_members.iter().copied().enumerate() {
        assert_eq!(
            context.tcp_sessions[index].connection_instance_id(),
            Some(initial_instance),
            "planned rotation must preserve every carrier while Product ownership exists"
        );
        assert_eq!(
            context.tcp_sessions[index].connection_remote_port(),
            Some(initial_port),
            "planned rotation must preserve every destination while Product ownership exists"
        );
    }

    product_client
        .shutdown()
        .await
        .expect("shutdown Product client");
    product
        .await
        .expect("Product relay join")
        .expect("Product relay");
    target.await.expect("target join");

    // Releasing the exact Product owner publishes a lifecycle event. An
    // already-overdue member rotates without waiting for the unrelated
    // 60-second probe interval.
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let any_changed = context.tcp_sessions.iter().zip(&initial_members).any(
                |(session, (instance, _))| session.connection_instance_id() != Some(*instance),
            );
            if any_changed {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Product-quiescent owner event did not rotate an overdue member");

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let all_ready = context
                .tcp_sessions
                .iter()
                .all(ClientTcpPathSessionHandle::is_connection_ready);
            let all_rotated = context.tcp_sessions.iter().zip(&initial_members).all(
                |(session, (instance, port))| {
                    session.connection_instance_id() != Some(*instance)
                        && session.connection_remote_port() != Some(*port)
                },
            );
            if all_ready && all_rotated && context.tcp_carrier_groups.occupied(0) == Some(3) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("bounded pool did not fairly rotate every overdue member");
    for (index, (initial_instance, _)) in initial_members.iter().enumerate() {
        assert_ne!(
            context.tcp_sessions[index].connection_instance_id(),
            Some(*initial_instance),
            "each overdue member must receive a fresh physical instance"
        );
    }
    assert!(
        accepted
            .iter()
            .all(|count| count.load(std::sync::atomic::Ordering::Acquire) > 0),
        "planned rotation must visit both configured destination ports"
    );

    let final_server = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let snapshot = snapshot_context.reliable_streams.management_snapshot();
            if snapshot.paths.len() == 3
                && snapshot
                    .paths
                    .iter()
                    .all(|path| !initial_server_instances.contains(&path.path_instance_id))
            {
                break snapshot;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("rotated ranged server carrier inventory");
    assert!(
        final_server
            .paths
            .iter()
            .all(|path| path.session_id == initial_session_id),
        "physical port rotation must preserve the MPP SessionId"
    );
    assert_eq!(
        final_server
            .paths
            .iter()
            .map(|path| path.path_id)
            .collect::<HashSet<_>>()
            .len(),
        3,
        "the restored bounded pool must retain distinct live wire PathIds"
    );

    path_service.abort();
    let _ = path_service.await;
    carrier_server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ranged_tcp_maximum_one_reconnects_only_after_product_quiescence() {
    let carrier_server = RangedTcpCarrierServer::spawn().await;
    let first_port = carrier_server.first_port;
    let second_port = first_port + 1;
    let client_path = format!(
        "tcp://127.0.0.1:{first_port}-{second_port}?tcp-carriers=1-1&port-hop-interval-ms=5000"
    )
    .parse::<PathSpec>()
    .expect("maximum-one ranged client TCP path");
    let context = ClientPathContext::new_with_carrier_network(
        vec![ClientPathConfig {
            name: "maximum-one-ranged".to_string(),
            tls: crate::transport::encrypted::test_client_tls_config(),
            spec: client_path,
            security: security(),
        }],
        ResourceLimits::default(),
        None,
        0,
        Arc::new(SystemCarrierNetworkProvider),
    )
    .expect("maximum-one client carrier context");
    let path_service = tokio::spawn(run_client_path_service(
        context.clone(),
        Duration::from_secs(60),
        Duration::from_secs(2),
    ));
    wait_for_tcp_ready_count(&context, 1).await;
    let initial_instance = context.tcp_sessions[0]
        .connection_instance_id()
        .expect("initial maximum-one carrier");
    let initial_port = context.tcp_sessions[0]
        .connection_remote_port()
        .expect("initial maximum-one port");

    let target_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind maximum-one target");
    let target_addr = target_listener
        .local_addr()
        .expect("maximum-one target address");
    let target = tokio::spawn(async move {
        let (mut stream, _) = target_listener
            .accept()
            .await
            .expect("accept maximum-one target");
        let mut payload = [0_u8; 4];
        loop {
            match stream.read_exact(&mut payload).await {
                Ok(_) => stream
                    .write_all(&payload)
                    .await
                    .expect("echo maximum-one target payload"),
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(error) => panic!("read maximum-one target payload: {error}"),
            }
        }
    });
    let (mut product_client, product_server) = duplex(4096);
    let product = tokio::spawn(handle_socks5_client_stream(product_server, context.clone()));
    open_socks5_tcp_tunnel(&mut product_client, target_addr).await;
    product_client
        .write_all(b"live")
        .await
        .expect("maximum-one live payload");
    let mut echoed = [0_u8; 4];
    product_client
        .read_exact(&mut echoed)
        .await
        .expect("maximum-one live response");
    assert_eq!(&echoed, b"live");

    tokio::time::sleep(Duration::from_secs(6)).await;
    assert_eq!(
        context.tcp_sessions[0].connection_instance_id(),
        Some(initial_instance),
        "maximum-one hopping must not drain an active Product carrier"
    );
    assert_eq!(
        context.tcp_sessions[0].connection_remote_port(),
        Some(initial_port),
        "maximum-one hopping must retain the active destination port"
    );
    assert_eq!(
        context.tcp_carrier_groups.occupied(0),
        Some(1),
        "maximum-one replacement must remain inside its resource envelope"
    );

    product_client
        .shutdown()
        .await
        .expect("shutdown maximum-one Product client");
    product
        .await
        .expect("maximum-one Product relay join")
        .expect("maximum-one Product relay");
    target.await.expect("maximum-one target join");

    // The ordinary probe/retry interval is 60 seconds. Convergence here proves
    // the exact Product release and predecessor terminal events drive the
    // break-before-make transaction without inheriting that deadline.
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if context.tcp_sessions[0]
                .connection_instance_id()
                .is_some_and(|instance| {
                    instance != initial_instance
                        && context.tcp_sessions[0].connection_remote_port() != Some(initial_port)
                })
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("maximum-one Product-quiescent replacement did not converge");
    assert_eq!(
        context.tcp_carrier_groups.occupied(0),
        Some(1),
        "terminal predecessor release must precede maximum-one successor reservation"
    );
    assert_eq!(
        carrier_server
            .maximum_active
            .load(std::sync::atomic::Ordering::Acquire),
        1,
        "maximum-one replacement must never overlap physical TCP carriers"
    );
    assert_eq!(
        carrier_server
            .active
            .load(std::sync::atomic::Ordering::Acquire),
        1,
        "maximum-one reconciliation must restore one authenticated carrier"
    );
    assert!(
        carrier_server
            .accepted
            .iter()
            .all(|count| count.load(std::sync::atomic::Ordering::Acquire) > 0),
        "maximum-one replacement must select the other configured port"
    );

    path_service.abort();
    let _ = path_service.await;
    carrier_server.shutdown().await;
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
async fn socks5_ingress_relays_tcp_payload_over_encrypted_internal_stream() {
    let (target_addr, target) = spawn_echo_target().await;
    let (path, server_path) = spawn_server_path(OutboundConfig::Direct).await;
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("ctx");
    probe_client_paths(&context, Duration::from_secs(2)).await;
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
        local_proxy_auth(),
    )
    .expect("ctx");
    probe_client_paths(&context, Duration::from_secs(2)).await;
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
        local_proxy_auth(),
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
    tokio::time::timeout(Duration::from_secs(10), async {
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
    })
    .await
    .expect("UDP stream relay integration deadline");
}

#[tokio::test]
async fn tcp_path_session_multiplexes_multiple_single_path_interactive_streams() {
    let (target_addr, target) = spawn_echo_target_count(2).await;
    let (path, server_path) = spawn_server_path(OutboundConfig::Direct).await;
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("ctx");
    probe_client_paths(&context, Duration::from_secs(2)).await;
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
    // Keep the logical response alive until the automatically selected path
    // has crossed TLS, admission, PATH_JOIN, and both readiness frames.
    // Otherwise a fast loopback EOF can cancel the speculative connection
    // while its handshake is still in flight.
    let (release_target_tail_tx, release_target_tail_rx) = oneshot::channel();
    let target = tokio::spawn(async move {
        let (mut stream, _) = target_listener.accept().await.expect("target accept");
        let mut request = [0u8; 4];
        stream
            .read_exact(&mut request)
            .await
            .expect("target request");
        assert_eq!(&request, b"ping");
        let tail = payload.len() - 1;
        stream
            .write_all(&payload[..tail])
            .await
            .expect("target response prefix");
        release_target_tail_rx
            .await
            .expect("release target response tail");
        stream
            .write_all(&payload[tail..])
            .await
            .expect("target response tail");
        stream.shutdown().await.expect("target shutdown");
    });

    let low_latency_path =
        reserve_tcp_path_with_query("srtt-ms=10&rate-mbps=20&tcp-carriers=1-1").await;
    let high_bandwidth_path =
        reserve_tcp_path_with_query("srtt-ms=120&rate-mbps=300&tcp-carriers=1-1").await;
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
    probe_client_paths(&context, Duration::from_secs(2)).await;
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
    let response_len = expected_payload.len();
    let response = tokio::spawn(async move {
        let mut received = vec![0u8; response_len];
        client
            .read_exact(&mut received)
            .await
            .expect("payload read");
        received
    });
    tokio::time::timeout(FULL_STACK_RESPONSE_TIMEOUT, async {
        loop {
            let attached =
                health_context.health().lock().expect("health lock").tcp[1].active_flows > 0;
            if attached {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("measured high-bandwidth path never became ready");
    release_target_tail_tx
        .send(())
        .expect("release target response tail");
    let received = tokio::time::timeout(FULL_STACK_RESPONSE_TIMEOUT, response)
        .await
        .expect("response timeout")
        .expect("response task");
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
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let released = {
                let health = health_context.health().lock().expect("health lock");
                health.tcp[0].active_flows == 0 && health.tcp[1].active_flows == 0
            };
            if released {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("reliable path load leases were not released");
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
    probe_client_paths(&context, Duration::from_secs(2)).await;
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

    let tcp_path = reserve_tcp_path_with_query("srtt-ms=10&rate-mbps=100&tcp-carriers=1-1").await;
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
    let _pool = spawn_tcp_pool_reconciliation(&context);
    wait_for_tcp_ready_count(&context, 1).await;
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
    let active_before_outage =
        health_context.health().lock().expect("health lock").tcp[0].active_flows;
    assert_eq!(
        active_before_outage, 1,
        "one live Product attachment must own one path-load lease"
    );

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
    probe_client_paths(&context, Duration::from_secs(2)).await;
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
    probe_client_paths(&context, Duration::from_secs(2)).await;
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
async fn socks5_ingress_uses_ranked_tcp_carriers_for_product_stream() {
    let (target_addr, target) = spawn_echo_target().await;
    let high_latency_path =
        reserve_tcp_path_with_query("srtt-ms=200&rate-mbps=1000&tcp-carriers=1-1").await;
    let low_latency_path =
        reserve_tcp_path_with_query("srtt-ms=10&rate-mbps=50&tcp-carriers=1-1").await;
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
    probe_client_paths(&context, Duration::from_secs(2)).await;
    {
        let mut health = context.health().lock().expect("health lock");
        health.tcp[0].carrier_srtt_ms = Some(200.0);
        health.tcp[0].carrier_rttvar_ms = Some(50.0);
        health.tcp[1].carrier_srtt_ms = Some(10.0);
        health.tcp[1].carrier_rttvar_ms = Some(2.5);
    }
    let ready_paths = HashSet::from([
        accepted_rx.recv().await.expect("first ready TCP carrier"),
        accepted_rx.recv().await.expect("second ready TCP carrier"),
    ]);
    assert_eq!(ready_paths, HashSet::from([0, 1]));
    assert_eq!(
        context
            .ordered_reliable_path_keys(TrafficClass::Latency, 1)
            .first()
            .copied(),
        Some(RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 1,
        }),
        "the lower-latency carrier must lead initial Product placement"
    );
    let health_context = context.clone();
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
    let low_latency_active_flows = {
        let health = health_context.health().lock().expect("health lock");
        health.tcp[1].active_flows
    };
    assert_eq!(
        low_latency_active_flows, 1,
        "the Product stream must attach to the leading carrier"
    );
    client.write_all(b"ping").await.expect("payload write");
    client.shutdown().await.expect("client shutdown");
    let mut payload = [0u8; 4];
    client.read_exact(&mut payload).await.expect("payload read");
    assert_eq!(&payload, b"pong");

    handler.await.expect("join").expect("handler");
    drop(health_context);
    low_latency_server
        .await
        .expect("low latency server join")
        .expect("low latency server");
    high_latency_server
        .await
        .expect("high latency server join")
        .expect("high latency server");
    target.await.expect("target join");
}

#[tokio::test]
async fn socks5_ingress_starts_reliable_auto_latency_first() {
    let (target_addr, target) = spawn_echo_target().await;
    let no_bulk_low_latency_path = reserve_tcp_path_with_query(
        "srtt-ms=10&rate-mbps=1000&bulk-allowed=false&tcp-carriers=1-1",
    )
    .await;
    let bulk_allowed_path =
        reserve_tcp_path_with_query("srtt-ms=120&rate-mbps=100&tcp-carriers=1-1").await;
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
    probe_client_paths(&context, Duration::from_secs(2)).await;
    {
        let mut health = context.health().lock().expect("health lock");
        health.tcp[0].carrier_srtt_ms = Some(10.0);
        health.tcp[0].carrier_rttvar_ms = Some(2.5);
        health.tcp[1].carrier_srtt_ms = Some(120.0);
        health.tcp[1].carrier_rttvar_ms = Some(30.0);
    }
    let ready_paths = HashSet::from([
        accepted_rx.recv().await.expect("first ready TCP carrier"),
        accepted_rx.recv().await.expect("second ready TCP carrier"),
    ]);
    assert_eq!(ready_paths, HashSet::from([0, 1]));
    let health_context = context.clone();
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
    let active_flows = {
        let health = health_context.health().lock().expect("health lock");
        (health.tcp[0].active_flows, health.tcp[1].active_flows)
    };
    assert_eq!(
        active_flows,
        (1, 0),
        "ReliableAuto must start its interactive Product stream on the latency carrier"
    );
    client.write_all(b"ping").await.expect("payload write");
    client.shutdown().await.expect("client shutdown");
    let mut payload = [0u8; 4];
    client.read_exact(&mut payload).await.expect("payload read");
    assert_eq!(&payload, b"pong");

    handler.await.expect("join").expect("handler");
    drop(health_context);
    low_latency_server
        .await
        .expect("low latency server join")
        .expect("low latency server");
    bulk_allowed_server
        .await
        .expect("bulk allowed server join")
        .expect("bulk allowed server");
    target.await.expect("target join");
}

#[tokio::test]
async fn socks5_ingress_uses_a_ready_carrier_while_another_endpoint_is_unavailable() {
    let (target_addr, target) = spawn_echo_target().await;
    let failed_path = reserve_tcp_path().await;
    let (working_path, server_path) = spawn_server_path(OutboundConfig::Direct).await;
    let context = ClientPathContext::new(
        vec![failed_path, working_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("ctx");
    probe_client_paths(&context, Duration::from_secs(2)).await;
    let health_context = context.clone();
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
    {
        let health = health_context.health().lock().expect("health lock");
        assert!(!health.tcp[0].has_physical_carrier());
        assert_eq!(health.tcp[1].active_flows, 1);
    }
    client.write_all(b"ping").await.expect("payload write");
    client.shutdown().await.expect("client shutdown");
    let mut payload = [0u8; 4];
    client.read_exact(&mut payload).await.expect("payload read");
    assert_eq!(&payload, b"pong");

    handler.await.expect("join").expect("handler");
    drop(health_context);
    server_path
        .await
        .expect("server join")
        .expect("server path");
    target.await.expect("target join");
}

#[tokio::test]
async fn socks5_ingress_reports_network_unreachable_while_mpp_outbound_is_offline() {
    let context = ClientPathContext::new(
        vec![reserve_tcp_path().await],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    drop(context.authenticated_carriers.register());
    let (mut client, server) = duplex(1_024);
    let handler = tokio::spawn(handle_socks5_client_stream(server, context));

    client
        .write_all(&[0x05, 0x01, 0x00])
        .await
        .expect("auth request");
    let mut auth_response = [0_u8; 2];
    client
        .read_exact(&mut auth_response)
        .await
        .expect("auth response");
    assert_eq!(auth_response, [0x05, 0x00]);
    client
        .write_all(&[0x05, 0x01, 0x00, 0x01, 192, 0, 2, 1, 0x01, 0xbb])
        .await
        .expect("CONNECT request");
    let mut response = [0_u8; 10];
    client
        .read_exact(&mut response)
        .await
        .expect("CONNECT reply");
    assert_eq!(response[1], Socks5Reply::NetworkUnreachable as u8);
    assert!(matches!(
        handler.await.expect("handler join"),
        Err(RuntimeError::OutboundUnavailable(_))
    ));
}

#[tokio::test]
async fn http_connect_ingress_reports_service_unavailable_while_mpp_outbound_is_offline() {
    let context = ClientPathContext::new(
        vec![reserve_tcp_path().await],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    drop(context.authenticated_carriers.register());
    let (mut client, server) = duplex(1_024);
    let handler = tokio::spawn(handle_http_connect_client_stream(server, context));

    client
        .write_all(b"CONNECT 192.0.2.1:443 HTTP/1.1\r\nHost: 192.0.2.1:443\r\n\r\n")
        .await
        .expect("CONNECT request");
    let expected = http_connect::error_response(HttpStatus::ServiceUnavailable);
    let mut response = vec![0_u8; expected.len()];
    client
        .read_exact(&mut response)
        .await
        .expect("CONNECT reply");
    assert_eq!(response, expected);
    assert!(matches!(
        handler.await.expect("handler join"),
        Err(RuntimeError::OutboundUnavailable(_))
    ));
}

#[tokio::test]
async fn http_connect_ingress_relays_tcp_payload_over_encrypted_internal_stream() {
    let (target_addr, target) = spawn_echo_target().await;
    let (path, server_path) = spawn_server_path(OutboundConfig::Direct).await;
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("ctx");
    probe_client_paths(&context, Duration::from_secs(2)).await;
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
        local_proxy_auth(),
    )
    .expect("ctx");
    probe_client_paths(&context, Duration::from_secs(2)).await;
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
        local_proxy_auth(),
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
    tokio::time::timeout(Duration::from_secs(10), async {
        let (target_addr, target) = spawn_echo_target().await;
        let (path, server_path) = spawn_udp_server_path(OutboundConfig::Direct).await;
        let context = client_context_with_session_retention(
            vec![path],
            ResourceLimits::default(),
            Duration::from_secs(2),
        );
        let (mut client, server) = duplex(1);
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
        // Hold product delivery so the QUIC actor observes peer FIN and clean EOF
        // before the relay can publish final receive feedback.
        target.await.expect("target join");
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut payload = [0u8; 4];
        client.read_exact(&mut payload).await.expect("payload read");
        assert_eq!(&payload, b"pong");

        tokio::time::timeout(FULL_STACK_RESPONSE_TIMEOUT, handler)
            .await
            .expect("handler timeout")
            .expect("join")
            .expect("handler");
        server_path.abort();
        let _ = server_path.await;
    })
    .await
    .expect("HTTP CONNECT UDP stream relay integration deadline");
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

async fn send_socks5_udp_ping(
    udp_client: &UdpSocket,
    relay_addr: SocketAddr,
    target_addr: SocketAddr,
) {
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
    FeedbackThenRespond,
    CloseBeforeFeedback,
    FeedbackThenHold,
    HoldWithoutFeedback,
}

#[derive(Debug, Clone, Copy)]
struct ScriptedTcpDatagramEmission {
    session_id: SessionId,
    flow_id: DatagramFlowId,
    datagram_id: DatagramId,
    ttl_ms: u32,
}

async fn spawn_scripted_tcp_datagram_path(
    ready_delay: Duration,
    action: ScriptedTcpDatagramAction,
) -> (
    PathSpec,
    oneshot::Receiver<ScriptedTcpDatagramEmission>,
    tokio::task::JoinHandle<Result<(), RuntimeError>>,
) {
    let path = reserve_tcp_path().await;
    let listener = bind_listener(&path).await.expect("bind scripted path");
    let (ttl_tx, ttl_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let mut framed = EncryptedFramedStream::accept(
            stream,
            &crate::transport::encrypted::test_server_tls_config(),
            CodecLimits::default(),
        )
        .await
        .expect("initialize encrypted stream");
        let security = ServerSecurityConfig::for_test(
            SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec())
                .expect("scripted server secret"),
        );
        let transport_binding = framed.tcp_admission_binding()?;
        let encoded = framed.read_tcp_admission().await?;
        let authenticated = crate::runtime::path::tcp::admission::authenticate_prelude(
            &security,
            crate::runtime::path::authentication::ProductCredentialAdmission::from_security(
                &security,
            ),
            &encoded,
            &transport_binding,
        )?
        .ok_or(RuntimeError::Protocol("invalid TCP admission prelude"))?;
        let joined = authenticated
            .authenticate_path_join(UnderlayProtocol::Tcp, framed.read_frame().await?)?
            .ok_or(RuntimeError::Protocol("invalid PATH_JOIN"))?;
        let session_id = joined.session_id;
        let path_id = joined.path_id;
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
                    let _ = ttl_tx.send(ScriptedTcpDatagramEmission {
                        session_id,
                        flow_id,
                        datagram_id,
                        ttl_ms,
                    });
                    match action {
                        ScriptedTcpDatagramAction::CloseBeforeFeedback => return Ok(()),
                        ScriptedTcpDatagramAction::HoldWithoutFeedback => {
                            tokio::time::sleep(Duration::from_secs(2)).await;
                            return Ok(());
                        }
                        ScriptedTcpDatagramAction::FeedbackThenRespond => {
                            let response_datagram_id = DatagramId(41);
                            framed
                                .write_frames(&[
                                    Frame::DatagramFeedback {
                                        flow_id,
                                        received: vec![datagram_feedback_range(datagram_id)?],
                                    },
                                    Frame::DatagramData {
                                        flow_id,
                                        // Each endpoint owns an independent datagram ID space.
                                        datagram_id: response_datagram_id,
                                        ttl_ms,
                                        payload: Bytes::from_static(b"pong"),
                                    },
                                ])
                                .await?;
                            framed.flush().await?;
                            loop {
                                match framed.read_frame().await? {
                                    Frame::DatagramFeedback {
                                        flow_id: feedback_flow_id,
                                        received,
                                    } if feedback_flow_id == flow_id
                                        && received.iter().any(|range| {
                                            response_datagram_id.0 >= range.start
                                                && response_datagram_id.0 < range.end
                                        }) =>
                                    {
                                        return Ok(());
                                    }
                                    Frame::Ping { nonce } => {
                                        framed.write_frame(&Frame::Pong { nonce }).await?;
                                        framed.flush().await?;
                                    }
                                    Frame::PathMetrics { .. } => {}
                                    _ => {
                                        return Err(RuntimeError::Protocol(
                                            "unexpected scripted TCP response feedback frame",
                                        ));
                                    }
                                }
                            }
                        }
                        ScriptedTcpDatagramAction::FeedbackThenClose
                        | ScriptedTcpDatagramAction::FeedbackThenHold => {
                            framed
                                .write_frame(&Frame::DatagramFeedback {
                                    flow_id,
                                    received: vec![datagram_feedback_range(datagram_id)?],
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

async fn receive_association_datagram(
    association: &mut DatagramClientAssociation,
) -> (TargetAddr, Bytes) {
    tokio::time::timeout(FULL_STACK_RESPONSE_TIMEOUT, async {
        loop {
            let event = association
                .next_carrier_frame()
                .await
                .expect("next datagram carrier frame");
            match association
                .handle_carrier_frame(event)
                .await
                .expect("handle datagram carrier frame")
            {
                DatagramClientReceive::Deliver {
                    target,
                    payload,
                    receipt,
                } => {
                    association
                        .acknowledge_received(receipt)
                        .await
                        .expect("acknowledge received datagram");
                    return (target, payload);
                }
                DatagramClientReceive::Duplicate(receipt) => association
                    .acknowledge_received(receipt)
                    .await
                    .expect("acknowledge duplicate datagram"),
                DatagramClientReceive::Control => {}
            }
        }
    })
    .await
    .expect("receive independent datagram timeout")
}

async fn process_next_association_control(association: &mut DatagramClientAssociation) {
    let event = tokio::time::timeout(
        FULL_STACK_RESPONSE_TIMEOUT,
        association.next_carrier_frame(),
    )
    .await
    .expect("datagram control frame timeout")
    .expect("next datagram control frame");
    assert!(matches!(
        association
            .handle_carrier_frame(event)
            .await
            .expect("handle datagram control frame"),
        DatagramClientReceive::Control
    ));
}

#[tokio::test]
async fn tcp_datagram_send_and_receive_use_independent_direction_ids() {
    let target = UdpSocket::bind("127.0.0.1:0").await.expect("target bind");
    let target_addr = target.local_addr().expect("target address");
    let (path, emission, scripted) = spawn_scripted_tcp_datagram_path(
        Duration::ZERO,
        ScriptedTcpDatagramAction::FeedbackThenRespond,
    )
    .await;
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("context");
    let _pool = spawn_tcp_pool_reconciliation(&context);
    let mut association = DatagramClientAssociation::new(context)
        .await
        .expect("association");

    association
        .send_to_fresh_datagram_with_route_hint(
            TargetAddr::Ip(target_addr),
            Bytes::from_static(b"ping"),
            1_000,
            None,
        )
        .await
        .expect("local datagram send");
    assert!(emission.await.expect("scripted emission").ttl_ms > 0);
    let (response_target, response) = receive_association_datagram(&mut association).await;
    assert_eq!(response_target, TargetAddr::Ip(target_addr));
    assert_eq!(response, Bytes::from_static(b"pong"));

    scripted
        .await
        .expect("scripted join")
        .expect("scripted path");
}

#[tokio::test]
async fn udp_datagram_path_relays_direct_udp_target() {
    let (target_addr, target) = spawn_udp_echo_target().await;
    let (path, server) = spawn_udp_server_path(OutboundConfig::Direct).await;

    let response = client_udp_datagram_round_trip(
        &path,
        security(),
        crate::transport::encrypted::test_client_tls_config(),
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
    let (path, server) = spawn_udp_server_path(OutboundConfig::Socks5(
        crate::outbound::ProxyConfig::new(proxy, None),
    ))
    .await;

    let response = client_udp_datagram_round_trip(
        &path,
        security(),
        crate::transport::encrypted::test_client_tls_config(),
        ResourceLimits::default(),
        TargetAddr::Domain {
            host: "localhost".to_string(),
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
        DEFAULT_OUTBOUND_CONNECT_TIMEOUT,
        test_server_destination_acl(),
        server_security(),
        crate::transport::encrypted::test_server_tls_config(),
        MppPerformanceConfig::default(),
        ResourceLimits::default(),
        SessionConfig::default(),
        ManagementConfig::default(),
        None,
    ));
    tokio::time::sleep(Duration::from_millis(10)).await;

    let response = client_udp_datagram_round_trip(
        &path,
        security(),
        crate::transport::encrypted::test_client_tls_config(),
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
        DEFAULT_OUTBOUND_CONNECT_TIMEOUT,
        test_server_destination_acl(),
        server_security(),
        crate::transport::encrypted::test_server_tls_config(),
        MppPerformanceConfig::default(),
        ResourceLimits::default(),
        SessionConfig::default(),
        ManagementConfig::default(),
        None,
    ));
    tokio::time::sleep(Duration::from_millis(10)).await;

    let first = client_udp_datagram_round_trip(
        &path,
        security(),
        crate::transport::encrypted::test_client_tls_config(),
        ResourceLimits::default(),
        TargetAddr::Ip(first_target_addr),
        Bytes::from_static(b"ping"),
        1000,
    );
    let second = client_udp_datagram_round_trip(
        &path,
        security(),
        crate::transport::encrypted::test_client_tls_config(),
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
    let udp_client = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("UDP association client bind");
    for _ in 0..2 {
        send_socks5_udp_ping(&udp_client, relay_addr, target_addr).await;
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
    let _pool = spawn_tcp_pool_reconciliation(&context);
    let health_context = context.clone();
    let (mut control_client, control_server) = duplex(4096);
    let handler = tokio::spawn(handle_socks5_client_stream(control_server, context));

    let relay_addr = open_socks5_udp_associate(&mut control_client).await;
    let udp_client = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("UDP association client bind");
    for _ in 0..2 {
        send_socks5_udp_ping(&udp_client, relay_addr, target_addr).await;
    }
    control_client.shutdown().await.expect("control shutdown");

    handler.await.expect("handler join").expect("handler");
    {
        let health = health_context.health().lock().expect("health lock");
        assert_eq!(health.tcp[0].state, SchedulerPathState::Active);
        assert!(health.udp.is_empty());
    }
    // The path actor intentionally retains its shared carrier until the final
    // context owner exits; release the inspection clone before joining it.
    drop(health_context);
    server_path
        .await
        .expect("server join")
        .expect("server path");
    target.await.expect("target join");
}

#[tokio::test]
async fn tcp_datagram_feedback_then_carrier_close_cancels_retry() {
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
    let _pool = spawn_tcp_pool_reconciliation(&context);
    let mut association = DatagramClientAssociation::new(context)
        .await
        .expect("association");

    association
        .send_to_fresh_datagram_with_route_hint(
            TargetAddr::Ip(target_addr),
            Bytes::from_static(b"ping"),
            1_500,
            Some(RelayPathKey {
                underlay: UnderlayProtocol::Tcp,
                index: 0,
            }),
        )
        .await
        .expect("local datagram send");
    assert!(ttl_rx.await.expect("scripted emission").ttl_ms > 0);
    process_next_association_control(&mut association).await;
    assert!(
        association.next_retry_deadline().is_none(),
        "admission feedback must release the retained datagram"
    );
    association
        .retry_due_datagram()
        .await
        .expect("no-op retry after feedback");
    scripted
        .await
        .expect("scripted join")
        .expect("scripted path");
    let mut packet = [0u8; 16];
    assert!(
        tokio::time::timeout(Duration::from_millis(150), target.recv_from(&mut packet))
            .await
            .is_err(),
        "a request with admission feedback must not be emitted on the fallback path"
    );
    fallback.abort();
    let _ = fallback.await;
}

#[tokio::test]
async fn tcp_datagram_no_feedback_reinjects_same_identity_on_alternative() {
    let target = UdpSocket::bind("127.0.0.1:0").await.expect("target bind");
    let target_addr = target.local_addr().expect("target address");
    let (mut first_path, first_emission, first) = spawn_scripted_tcp_datagram_path(
        Duration::ZERO,
        ScriptedTcpDatagramAction::HoldWithoutFeedback,
    )
    .await;
    first_path.metadata.initial_srtt_ms = Some(200);
    first_path.metadata.initial_jitter_ms = Some(25);
    first_path.metadata.initial_rate = RateHint::BitsPerSecond(100_000_000);
    let (mut fallback_path, fallback_emission, fallback) = spawn_scripted_tcp_datagram_path(
        Duration::ZERO,
        ScriptedTcpDatagramAction::FeedbackThenHold,
    )
    .await;
    fallback_path.metadata.initial_srtt_ms = Some(400);
    fallback_path.metadata.initial_jitter_ms = Some(50);
    fallback_path.metadata.initial_rate = RateHint::BitsPerSecond(100_000_000);
    let context = ClientPathContext::new(
        vec![first_path, fallback_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    let ttl_ms = 2_500;
    assert_eq!(
        context.ordered_tcp_path_indices(TrafficClass::RealtimeDatagram, 4),
        vec![0, 1]
    );
    let expected_session_id = context.session_id;
    let health_context = context.clone();
    let _pool = spawn_tcp_pool_reconciliation(&context);
    let mut association = DatagramClientAssociation::new(context)
        .await
        .expect("association");

    association
        .send_to_fresh_datagram_with_route_hint(
            TargetAddr::Ip(target_addr),
            Bytes::from_static(b"ping"),
            ttl_ms,
            None,
        )
        .await
        .expect("local datagram send");
    let emission = first_emission.await.expect("first emission");
    assert_eq!(emission.session_id, expected_session_id);
    let retry_at = association
        .next_retry_deadline()
        .expect("missing feedback retry deadline");
    tokio::time::sleep_until(retry_at).await;
    association
        .retry_due_datagram()
        .await
        .expect("retry due datagram");
    let fallback_emission = fallback_emission.await.expect("fallback emission");
    assert_eq!(fallback_emission.session_id, expected_session_id);
    assert_eq!(fallback_emission.flow_id, emission.flow_id);
    assert_eq!(fallback_emission.datagram_id, emission.datagram_id);
    process_next_association_control(&mut association).await;
    assert!(
        association.next_retry_deadline().is_none(),
        "fallback feedback must release the retained datagram"
    );
    {
        let health = health_context.health().lock().expect("health lock");
        assert_eq!(health.tcp[0].consecutive_failures, 0);
        assert!(health.tcp[0].failed_until.is_none());
    }

    drop(association);
    first.abort();
    let _ = first.await;
    fallback.abort();
    let _ = fallback.await;
}

#[tokio::test]
async fn tcp_datagram_send_does_not_wait_for_feedback_or_target_response() {
    let target = UdpSocket::bind("127.0.0.1:0").await.expect("target bind");
    let target_addr = target.local_addr().expect("target address");
    let (mut first_path, first_emission, first) = spawn_scripted_tcp_datagram_path(
        Duration::ZERO,
        ScriptedTcpDatagramAction::FeedbackThenHold,
    )
    .await;
    first_path.metadata.initial_srtt_ms = Some(50);
    first_path.metadata.initial_jitter_ms = Some(10);
    first_path.metadata.initial_rate = RateHint::BitsPerSecond(100_000_000);
    let (mut fallback_path, fallback) = spawn_server_path(OutboundConfig::Direct).await;
    fallback_path.metadata.initial_srtt_ms = Some(400);
    fallback_path.metadata.initial_jitter_ms = Some(50);
    fallback_path.metadata.initial_rate = RateHint::BitsPerSecond(100_000_000);
    let context = ClientPathContext::new(
        vec![first_path, fallback_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    let ttl_ms = 900;
    let _pool = spawn_tcp_pool_reconciliation(&context);
    let mut association = DatagramClientAssociation::new(context)
        .await
        .expect("association");
    let started_at = Instant::now();

    association
        .send_to_fresh_datagram_with_route_hint(
            TargetAddr::Ip(target_addr),
            Bytes::from_static(b"ping"),
            ttl_ms,
            None,
        )
        .await
        .expect("local datagram send");
    let _ = first_emission.await.expect("first emission");
    assert!(
        started_at.elapsed() < Duration::from_millis(450),
        "local send completion must not wait for peer feedback or the product TTL"
    );
    process_next_association_control(&mut association).await;
    assert!(
        association.next_retry_deadline().is_none(),
        "admission feedback must release the retained datagram"
    );
    association
        .retry_due_datagram()
        .await
        .expect("no-op retry after feedback");
    let mut packet = [0u8; 16];
    assert!(
        tokio::time::timeout(Duration::from_millis(100), target.recv_from(&mut packet))
            .await
            .is_err(),
        "a request with admission feedback must not be replayed"
    );

    first.abort();
    let _ = first.await;
    fallback.abort();
    let _ = fallback.await;
}

#[tokio::test]
async fn tcp_datagram_carrier_failure_reinjects_on_fallback() {
    let (target_addr, target) = spawn_udp_echo_target().await;
    let (first_path, first_ttl, first) = spawn_scripted_tcp_datagram_path(
        Duration::ZERO,
        ScriptedTcpDatagramAction::CloseBeforeFeedback,
    )
    .await;
    let (fallback_path, fallback) = spawn_server_path(OutboundConfig::Direct).await;
    let context = ClientPathContext::new(
        vec![first_path, fallback_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    let _pool = spawn_tcp_pool_reconciliation(&context);
    let mut association = DatagramClientAssociation::new(context)
        .await
        .expect("association");

    association
        .send_to_fresh_datagram_with_route_hint(
            TargetAddr::Ip(target_addr),
            Bytes::from_static(b"ping"),
            2_500,
            Some(RelayPathKey {
                underlay: UnderlayProtocol::Tcp,
                index: 0,
            }),
        )
        .await
        .expect("local datagram send");
    assert!(first_ttl.await.expect("first emission").ttl_ms > 0);
    first.await.expect("first join").expect("first path");
    let carrier_event = tokio::time::timeout(
        FULL_STACK_RESPONSE_TIMEOUT,
        association.next_carrier_frame(),
    )
    .await
    .expect("carrier failure frame timeout")
    .expect("carrier failure frame");
    assert!(
        association
            .handle_carrier_frame(carrier_event)
            .await
            .is_err(),
        "the closed carrier must report a receive failure"
    );
    let retry_at = association
        .next_retry_deadline()
        .expect("carrier failure retry deadline");
    assert!(retry_at <= tokio::time::Instant::now());
    association
        .retry_due_datagram()
        .await
        .expect("carrier failure reinjection");
    let (response_target, response) = receive_association_datagram(&mut association).await;
    assert_eq!(response_target, TargetAddr::Ip(target_addr));
    assert_eq!(response, Bytes::from_static(b"pong"));

    association.close().await.expect("association close");
    drop(association);
    fallback
        .await
        .expect("fallback join")
        .expect("fallback path");
    target.await.expect("target join");
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
    let _pool = spawn_tcp_pool_reconciliation(&context);
    let mut association = DatagramClientAssociation::new(context)
        .await
        .expect("association");

    association
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
        .expect("TCP fallback local send");
    let (response_target, response) = receive_association_datagram(&mut association).await;
    assert_eq!(response_target, TargetAddr::Ip(target_addr));
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
    drop(association);
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
    let _pool = spawn_tcp_pool_reconciliation(&context);
    let mut association = DatagramClientAssociation::new(context)
        .await
        .expect("association");
    let started_at = Instant::now();

    association
        .send_to_fresh_datagram_with_route_hint(
            TargetAddr::Ip(target_addr),
            Bytes::from_static(b"ping"),
            1_000,
            None,
        )
        .await
        .expect("local datagram send after carrier setup");
    let emitted_ttl = ttl_rx.await.expect("emitted TTL").ttl_ms;
    assert!(
        emitted_ttl < 850,
        "carrier setup must be deducted from DGRAM_DATA TTL, got {emitted_ttl} ms"
    );
    assert!(
        started_at.elapsed() < Duration::from_millis(850),
        "local send completion must not wait out the remaining product TTL"
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
    let _pool = spawn_tcp_pool_reconciliation(&context);
    let mut association = DatagramClientAssociation::new(context)
        .await
        .expect("association");
    let ttl_ms = 2_500;
    let started_at = Instant::now();
    association
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
        .expect("fallback local send");
    let (response_target, response) = receive_association_datagram(&mut association).await;
    assert_eq!(response_target, TargetAddr::Ip(target_addr));
    assert_eq!(response, Bytes::from_static(b"pong"));
    assert!(
        started_at.elapsed() < Duration::from_millis(u64::from(ttl_ms)),
        "a stalled first TCP carrier must leave TTL for the next TCP path"
    );

    association.close().await.expect("association close");
    drop(association);
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
        DEFAULT_OUTBOUND_CONNECT_TIMEOUT,
        test_server_destination_acl(),
        server_security(),
        crate::transport::encrypted::test_server_tls_config(),
        MppPerformanceConfig::default(),
        ResourceLimits::default(),
        SessionConfig::default(),
        ManagementConfig::default(),
        None,
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

async fn assert_tcp_server_rejects_wrong_mpp_credential(transport_secret: Option<[u8; 32]>) {
    let path = reserve_tcp_path().await;
    let server_tls = match transport_secret {
        Some(secret) => {
            crate::transport::encrypted::test_server_tls_config_with_transport_secret(secret)
        }
        None => crate::transport::encrypted::test_server_tls_config(),
    };
    let server = tokio::spawn(run_server(
        vec![path.clone()],
        OutboundConfig::Direct,
        DEFAULT_OUTBOUND_CONNECT_TIMEOUT,
        test_server_destination_acl(),
        ServerSecurityConfig::for_test(
            SharedSecret::new(b"fedcba9876543210fedcba9876543210".to_vec())
                .expect("server MPP credential"),
        ),
        server_tls,
        MppPerformanceConfig::default(),
        ResourceLimits::default(),
        SessionConfig::default(),
        ManagementConfig::default(),
        None,
    ));
    tokio::time::sleep(Duration::from_millis(10)).await;

    let stream = tcp::connect_path(&path, TcpConnectOptions::default())
        .await
        .expect("connect");
    let client_security = ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    );
    let client_tls = match transport_secret {
        Some(secret) => {
            crate::transport::encrypted::test_client_tls_config_with_transport_secret(secret)
        }
        None => crate::transport::encrypted::test_client_tls_config(),
    };
    let mut client = EncryptedFramedStream::connect(stream, &client_tls, CodecLimits::default())
        .await
        .expect("initialize protected stream");
    let transport_binding = client
        .tcp_admission_binding()
        .expect("client transport binding");
    let (prelude, path_join) =
        crate::runtime::path::tcp::admission::ClientTcpPathAuthentication::for_new_session(
            &client_security,
            PathId(0),
            &transport_binding,
        )
        .expect("TCP authentication")
        .into_parts();
    client
        .write_tcp_admission(&prelude, &[path_join])
        .await
        .expect("write");
    client.flush().await.expect("flush");

    let rejection = tokio::time::timeout(Duration::from_secs(2), client.read_frame())
        .await
        .expect("wrong MPP credential rejection timeout");
    assert!(
        matches!(
            &rejection,
            Err(crate::transport::encrypted::EncryptedFramedTransportError::Io(error))
                if error.kind() == std::io::ErrorKind::UnexpectedEof
        ),
        "wrong MPP credential must close before any application frame: {rejection:?}"
    );
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn tcp_server_keeps_transport_and_mpp_credentials_separate() {
    assert_tcp_server_rejects_wrong_mpp_credential(None).await;
    assert_tcp_server_rejects_wrong_mpp_credential(Some([0x5a; 32])).await;
}
