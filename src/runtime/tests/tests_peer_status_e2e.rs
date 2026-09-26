use super::*;
use crate::protocol::{PeerPathState, PeerStatusCode, UnderlayProtocol};
use crate::runtime::peer_status::PeerStatusBroker;
use crate::runtime::relay::open::open_remote_stream;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const PEER_STATUS_E2E_TIMEOUT: Duration = Duration::from_secs(5);

async fn hold_tcp_target() -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind peer-status target");
    let address = listener.local_addr().expect("peer-status target address");
    let task = tokio::spawn(async move {
        let (_stream, _) = listener.accept().await.expect("accept peer-status target");
        std::future::pending::<()>().await;
    });
    (address, task)
}

async fn spawn_echo_tcp_target() -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind heartbeat sibling target");
    let address = listener
        .local_addr()
        .expect("heartbeat sibling target address");
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener
            .accept()
            .await
            .expect("accept heartbeat sibling stream");
        let mut buffer = [0; 256];
        loop {
            let bytes = stream
                .read(&mut buffer)
                .await
                .expect("read heartbeat sibling payload");
            if bytes == 0 {
                return;
            }
            stream
                .write_all(&buffer[..bytes])
                .await
                .expect("echo heartbeat sibling payload");
        }
    });
    (address, task)
}

async fn assert_echo_round_trip<S>(stream: &mut S, payload: &[u8])
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    stream
        .write_all(payload)
        .await
        .expect("send Product bytes over healthy sibling");
    let mut echoed = vec![0; payload.len()];
    stream
        .read_exact(&mut echoed)
        .await
        .expect("read Product bytes over healthy sibling");
    assert_eq!(echoed, payload);
}

async fn open_socks5_tcp_tunnel<S>(client: &mut S, target_addr: SocketAddr)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    client
        .write_all(&[0x05, 0x01, 0x00])
        .await
        .expect("SOCKS5 authentication request");
    let mut auth_response = [0; 2];
    client
        .read_exact(&mut auth_response)
        .await
        .expect("SOCKS5 authentication response");
    assert_eq!(auth_response, [0x05, 0x00]);

    let mut connect = vec![0x05, 0x01, 0x00, 0x01];
    match target_addr {
        SocketAddr::V4(address) => {
            connect.extend_from_slice(&address.ip().octets());
            connect.extend_from_slice(&address.port().to_be_bytes());
        }
        SocketAddr::V6(_) => panic!("heartbeat test target must be IPv4"),
    }
    client
        .write_all(&connect)
        .await
        .expect("SOCKS5 CONNECT request");
    let mut response = [0; 10];
    client
        .read_exact(&mut response)
        .await
        .expect("SOCKS5 CONNECT response");
    assert_eq!(response[1], 0, "SOCKS5 CONNECT must succeed");
}

async fn request_and_assert_server_path(
    context: &ClientPathContext,
    expected_underlay: UnderlayProtocol,
) {
    let result = tokio::time::timeout(
        PEER_STATUS_E2E_TIMEOUT,
        context.peer_status.request(context.session_id),
    )
    .await
    .expect("peer-status actor round trip timed out")
    .expect("peer-status actor round trip failed");

    assert_eq!(result.session_id, context.session_id);
    assert_ne!(result.request_id, 0);
    assert_eq!(result.code, PeerStatusCode::Ok);
    assert_eq!(result.paths.len(), 1);
    let path = &result.paths[0];
    assert_eq!(path.metrics.path_id, PathId(0));
    assert_eq!(path.metrics.underlay, expected_underlay);
    assert_eq!(path.metrics.direction, PathMetricDirection::ServerToClient);
    let expected_active_port =
        context
            .peer_status
            .live_path_active_port(context.session_id, expected_underlay, PathId(0));
    assert_eq!(
        result.local_active_port(expected_underlay, PathId(0)),
        expected_active_port,
        "peer diagnostics retain the exact authenticated client carrier port"
    );
    assert_eq!(
        context
            .peer_status
            .latest(context.session_id)
            .expect("cache authenticated peer response"),
        result
    );
}

async fn abort_task<T>(task: tokio::task::JoinHandle<T>) {
    task.abort();
    let _ = task.await;
}

struct ServerEofGate {
    path: PathSpec,
    cut_client: Option<oneshot::Sender<()>>,
    client_cut: Option<oneshot::Receiver<()>>,
    release_server: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<()>,
}

impl ServerEofGate {
    async fn spawn(server: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind EOF-gated TCP proxy");
        let proxy = listener.local_addr().expect("EOF-gated proxy address");
        let path = format!("tcp://{proxy}?max-tcp-carriers=1")
            .parse()
            .expect("EOF-gated TCP path");
        let (cut_client, cut_client_rx) = oneshot::channel();
        let (client_cut_tx, client_cut) = oneshot::channel();
        let (release_server, release_server_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let (mut client, _) = listener.accept().await.expect("accept gated client");
            let mut server = TcpStream::connect(server)
                .await
                .expect("connect gated server");
            {
                let transfer = tokio::io::copy_bidirectional(&mut client, &mut server);
                tokio::pin!(transfer);
                tokio::select! {
                    result = &mut transfer => {
                        panic!("gated carrier ended before client cut: {result:?}");
                    }
                    signal = cut_client_rx => {
                        signal.expect("request gated client cut");
                    }
                }
            }

            // Closing only the downstream socket makes the client actor and
            // its local peer-status registration terminal. The upstream
            // socket deliberately stays live until the test releases EOF.
            drop(client);
            client_cut_tx.send(()).expect("publish gated client cut");
            release_server_rx.await.expect("release gated server EOF");
            drop(server);
        });
        Self {
            path,
            cut_client: Some(cut_client),
            client_cut: Some(client_cut),
            release_server: Some(release_server),
            task,
        }
    }

    async fn cut_client(&mut self) {
        self.cut_client
            .take()
            .expect("client cut requested once")
            .send(())
            .expect("request client cut");
        self.client_cut
            .take()
            .expect("client cut observed once")
            .await
            .expect("observe client cut");
    }

    fn release_server_eof(&mut self) {
        self.release_server
            .take()
            .expect("server EOF released once")
            .send(())
            .expect("release server EOF");
    }

    async fn join(self) {
        self.task.await.expect("EOF-gated proxy task");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn authenticated_peer_status_round_trips_through_tcp_carrier_actors() {
    let path = reserve_tcp_path().await;
    let listener = bind_listener(&path).await.expect("bind TCP carrier");
    let local_path = ServerLocalPath::new(0, path.clone());
    let ServerIdentityRuntime {
        mut paths,
        reliable_relay,
    } = server_runtime(OutboundConfig::Direct);
    paths.peer_status = PeerStatusBroker::new(true);
    let server_context = paths.clone();
    let relay = tokio::spawn(
        reliable_relay
            .expect("L4 test server has a reliable relay")
            .run(),
    );
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept TCP carrier");
        handle_server_path(stream, local_path, paths).await
    });
    let (target_addr, target) = hold_tcp_target().await;
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("context");
    probe_client_paths(&context, Duration::from_secs(2)).await;

    let remote = tokio::time::timeout(
        PEER_STATUS_E2E_TIMEOUT,
        open_remote_stream(&context, TargetAddr::Ip(target_addr), TrafficClass::Latency),
    )
    .await
    .expect("TCP carrier open timed out")
    .expect("open TCP carrier stream");
    assert_eq!(
        server_context.peer_status.carrier_count(context.session_id),
        1
    );
    assert_eq!(context.authenticated_carriers.snapshot().live_count, 1);

    request_and_assert_server_path(&context, UnderlayProtocol::Tcp).await;

    drop(remote);
    drop(context);
    abort_task(server).await;
    abort_task(relay).await;
    abort_task(target).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peer_status_retains_path_identity_until_server_eof_cleanup() {
    let server_path = reserve_tcp_path().await;
    let listener = bind_listener(&server_path)
        .await
        .expect("bind peer-status lifecycle server");
    let server_address = listener
        .local_addr()
        .expect("peer-status lifecycle server address");
    let local_path = ServerLocalPath::new(0, server_path.clone());
    let ServerIdentityRuntime {
        mut paths,
        reliable_relay,
    } = server_runtime(OutboundConfig::Direct);
    paths.peer_status = PeerStatusBroker::new(true);
    let server_context = paths.clone();
    let relay = tokio::spawn(
        reliable_relay
            .expect("L4 test server has a reliable relay")
            .run(),
    );
    let server = tokio::spawn(async move {
        let mut carriers = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let (stream, _) = accepted.expect("accept lifecycle carrier");
                    let local_path = local_path.clone();
                    let paths = paths.clone();
                    carriers.spawn(async move {
                        handle_server_path(stream, local_path, paths).await
                    });
                }
                Some(result) = carriers.join_next(), if !carriers.is_empty() => {
                    result.expect("lifecycle carrier task").expect("lifecycle carrier");
                }
            }
        }
    });

    let mut eof_gate = ServerEofGate::spawn(server_address).await;
    let context = ClientPathContext::new(
        vec![eof_gate.path.clone(), server_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("peer-status lifecycle client context");
    probe_client_paths(&context, Duration::from_secs(2)).await;

    let initial = tokio::time::timeout(PEER_STATUS_E2E_TIMEOUT, async {
        loop {
            let snapshot = server_context.reliable_streams.management_snapshot();
            if snapshot.paths.len() == 2
                && context.peer_status.carrier_count(context.session_id) == 2
            {
                break snapshot;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("two authenticated lifecycle carriers");
    let gated_path_id = initial
        .paths
        .iter()
        .find_map(|path| {
            (context.peer_status.local_path_index(
                context.session_id,
                UnderlayProtocol::Tcp,
                path.path_id,
            ) == Some(0))
            .then_some(path.path_id)
        })
        .expect("identify EOF-gated wire PathId");

    eof_gate.cut_client().await;
    tokio::time::timeout(PEER_STATUS_E2E_TIMEOUT, async {
        loop {
            if context.peer_status.carrier_count(context.session_id) == 1
                && context
                    .peer_status
                    .local_path_index(context.session_id, UnderlayProtocol::Tcp, gated_path_id)
                    .is_none()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("client carrier registration cleanup while server EOF is gated");

    let transient = tokio::time::timeout(
        PEER_STATUS_E2E_TIMEOUT,
        context.peer_status.request(context.session_id),
    )
    .await
    .expect("gated peer-status request timed out")
    .expect("gated peer-status request failed");
    assert_eq!(transient.code, PeerStatusCode::Ok);
    let stale = transient
        .paths
        .iter()
        .find(|path| path.metrics.path_id == gated_path_id)
        .expect("server must retain the EOF-gated path temporarily");
    assert_eq!(stale.state, PeerPathState::Active);
    assert_eq!(
        transient.local_path_index(UnderlayProtocol::Tcp, gated_path_id),
        Some(0),
        "the locally retired carrier must retain its authenticated identity while the peer still reports it"
    );

    eof_gate.release_server_eof();
    eof_gate.join().await;
    tokio::time::timeout(PEER_STATUS_E2E_TIMEOUT, async {
        loop {
            let snapshot = server_context.reliable_streams.management_snapshot();
            if snapshot
                .paths
                .iter()
                .all(|path| path.path_id != gated_path_id)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("server registry cleanup after released EOF");

    // Incoming peer diagnostics are intentionally rate-limited per session.
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let fresh = tokio::time::timeout(
        PEER_STATUS_E2E_TIMEOUT,
        context.peer_status.request(context.session_id),
    )
    .await
    .expect("fresh peer-status request timed out")
    .expect("fresh peer-status request failed");
    assert_eq!(fresh.code, PeerStatusCode::Ok);
    assert!(fresh.request_id > transient.request_id);
    assert!(fresh.received_at > transient.received_at);
    assert!(
        fresh
            .paths
            .iter()
            .all(|path| path.metrics.path_id != gated_path_id),
        "a fresh response after server EOF cleanup must omit the retired path"
    );

    drop(context);
    abort_task(server).await;
    abort_task(relay).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_heartbeat_retires_eof_gated_carrier_while_sibling_serves() {
    let server_path = reserve_tcp_path().await;
    let listener = bind_listener(&server_path)
        .await
        .expect("bind heartbeat lifecycle server");
    let server_address = listener
        .local_addr()
        .expect("heartbeat lifecycle server address");
    let local_path = ServerLocalPath::new(0, server_path.clone());
    let server_resources = ResourceLimits {
        tcp_path_heartbeat_interval: Duration::from_millis(200),
        tcp_path_heartbeat_timeout: Duration::from_millis(600),
        ..ResourceLimits::default()
    };
    let ServerIdentityRuntime {
        mut paths,
        reliable_relay,
    } = new_identity_runtime(
        Vec::new(),
        OutboundConfig::Direct,
        DEFAULT_OUTBOUND_CONNECT_TIMEOUT,
        server_security(),
        crate::performance::MppPerformanceConfig::default(),
        server_resources,
    );
    paths.peer_status = PeerStatusBroker::new(true);
    let server_context = paths.clone();
    let relay = tokio::spawn(
        reliable_relay
            .expect("L4 heartbeat server has a reliable relay")
            .run(),
    );
    let server = tokio::spawn(async move {
        let mut carriers = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let (stream, _) = accepted.expect("accept heartbeat carrier");
                    let local_path = local_path.clone();
                    let paths = paths.clone();
                    carriers.spawn(async move {
                        handle_server_path(stream, local_path, paths).await
                    });
                }
                Some(result) = carriers.join_next(), if !carriers.is_empty() => {
                    result.expect("heartbeat carrier task").expect("heartbeat carrier");
                }
            }
        }
    });

    let mut eof_gate = ServerEofGate::spawn(server_address).await;
    let client = ClientPathContext::new(
        vec![eof_gate.path.clone(), server_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("heartbeat lifecycle client context");
    probe_client_paths(&client, Duration::from_secs(2)).await;

    let initial = tokio::time::timeout(PEER_STATUS_E2E_TIMEOUT, async {
        loop {
            let snapshot = server_context.reliable_streams.management_snapshot();
            if snapshot.paths.len() == 2 && client.peer_status.carrier_count(client.session_id) == 2
            {
                break snapshot;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("two authenticated heartbeat carriers");
    let gated_path_id = initial
        .paths
        .iter()
        .find_map(|path| {
            (client.peer_status.local_path_index(
                client.session_id,
                UnderlayProtocol::Tcp,
                path.path_id,
            ) == Some(0))
            .then_some(path.path_id)
        })
        .expect("identify EOF-gated wire PathId");
    let sibling_path_id = initial
        .paths
        .iter()
        .find_map(|path| (path.path_id != gated_path_id).then_some(path.path_id))
        .expect("identify healthy sibling PathId");
    let (target_addr, target) = spawn_echo_tcp_target().await;
    let (mut product_client, product_server) = duplex(4096);
    let product = tokio::spawn(handle_socks5_client_stream(product_server, client.clone()));
    tokio::time::timeout(
        PEER_STATUS_E2E_TIMEOUT,
        open_socks5_tcp_tunnel(&mut product_client, target_addr),
    )
    .await
    .expect("SOCKS5 sibling CONNECT timed out");
    assert_echo_round_trip(&mut product_client, b"before").await;
    eof_gate.cut_client().await;
    tokio::time::timeout(PEER_STATUS_E2E_TIMEOUT, async {
        loop {
            if client.peer_status.carrier_count(client.session_id) == 1
                && client
                    .peer_status
                    .local_path_index(client.session_id, UnderlayProtocol::Tcp, gated_path_id)
                    .is_none()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("client removes its side of the cut carrier");

    let mut echoes_while_old_path_registered = 0;
    tokio::time::timeout(PEER_STATUS_E2E_TIMEOUT, async {
        loop {
            let snapshot = server_context.reliable_streams.management_snapshot();
            if snapshot.paths.len() == 1 && snapshot.paths[0].path_id == sibling_path_id {
                break;
            }
            assert!(
                snapshot
                    .paths
                    .iter()
                    .any(|path| path.path_id == gated_path_id)
            );
            assert_echo_round_trip(&mut product_client, b"during").await;
            let snapshot = server_context.reliable_streams.management_snapshot();
            if snapshot
                .paths
                .iter()
                .any(|path| path.path_id == gated_path_id)
            {
                echoes_while_old_path_registered += 1;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("server heartbeat retires exact cut carrier while sibling stream runs");
    assert!(
        echoes_while_old_path_registered > 0,
        "persistent Product traffic must continue while the old path remains registered"
    );
    assert_echo_round_trip(&mut product_client, b"after").await;

    // The client-facing side is cut and the gate still owns the server-facing
    // socket here. A fresh peer-status round trip therefore proves the sibling
    // is still usable while the server no longer reports the exact old path.
    let peer_status = tokio::time::timeout(
        PEER_STATUS_E2E_TIMEOUT,
        client.peer_status.request(client.session_id),
    )
    .await
    .expect("post-retirement sibling peer-status request timed out")
    .expect("post-retirement sibling peer-status request failed");
    assert_eq!(peer_status.code, PeerStatusCode::Ok);
    assert_eq!(peer_status.paths.len(), 1);
    assert_eq!(peer_status.paths[0].metrics.path_id, sibling_path_id);

    assert_eq!(
        server_context.reliable_streams.management_snapshot().paths[0].path_id,
        sibling_path_id
    );

    product_client
        .shutdown()
        .await
        .expect("SOCKS product shutdown");
    drop(product_client);
    tokio::time::timeout(PEER_STATUS_E2E_TIMEOUT, product)
        .await
        .expect("SOCKS product relay shutdown timed out")
        .expect("SOCKS product relay task")
        .expect("SOCKS product relay");
    target.await.expect("sibling target task");

    eof_gate.release_server_eof();
    eof_gate.join().await;
    drop(client);
    abort_task(server).await;
    abort_task(relay).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn authenticated_peer_status_round_trips_through_quic_carrier_actors() {
    let path = reserve_udp_path().await;
    let ServerIdentityRuntime {
        mut paths,
        reliable_relay,
    } = server_runtime(OutboundConfig::Direct);
    paths.peer_status = PeerStatusBroker::new(true);
    let server_context = paths.clone();
    let endpoint = bind_server_udp_endpoint(&path, &paths)
        .await
        .expect("bind QUIC carrier");
    let local_path = ServerLocalPath::new(0, path.clone());
    let relay = tokio::spawn(
        reliable_relay
            .expect("L4 test server has a reliable relay")
            .run(),
    );
    let server = tokio::spawn(run_server_udp_listener(endpoint, local_path, paths));
    let (target_addr, target) = hold_tcp_target().await;
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("context");

    let remote = tokio::time::timeout(
        PEER_STATUS_E2E_TIMEOUT,
        open_remote_stream(&context, TargetAddr::Ip(target_addr), TrafficClass::Latency),
    )
    .await
    .expect("QUIC carrier open timed out")
    .expect("open QUIC carrier stream");
    assert_eq!(
        server_context.peer_status.carrier_count(context.session_id),
        1
    );
    assert_eq!(context.authenticated_carriers.snapshot().live_count, 1);

    request_and_assert_server_path(&context, UnderlayProtocol::Udp).await;

    drop(remote);
    drop(context);
    abort_task(server).await;
    abort_task(relay).await;
    abort_task(target).await;
}
