use super::*;
use crate::config::{DEFAULT_OUTBOUND_CONNECT_TIMEOUT, ResourceLimits, SharedSecret};
use crate::outbound::OutboundConfig;
use crate::protocol::frame::{reliable_path_frame_pacing_bytes, reliable_stream_frame_extent};
use crate::protocol::{CloseReason, PathUsage, StreamDemandHint};
use crate::runtime::node::server::{ServerIdentityRuntime, new_identity_runtime};
use crate::runtime::path::commands::{
    reliable_path_command_queue_for_payload, reliable_path_priority_headroom_frames,
    reliable_path_writer_frame_queue_for_payload,
    reliable_path_writer_should_coalesce_partial_bulk_run,
};
use crate::runtime::path::quic::server::{
    accept_server_udp_path_handshake_for_test, bind_server_udp_endpoint, run_server_udp_listener,
};
use crate::runtime::path::{
    ServerCarrierPathIdentity, ServerCarrierPathRegistration, ServerLocalPath,
    ServerLocalPathProperties, ServerStreamOpenRequest, ServerStreamPathAttachment,
};
use crate::runtime::relay::lifecycle::{
    reliable_relay_receive_hole_reinjection_active,
    reliable_relay_receive_hole_reinjection_deadline, reliable_relay_response_stall_watch_bytes,
    reliable_relay_stall_progress_anchor, reliable_relay_stall_watch_active,
};
use crate::runtime::stream::response::{ResponseStreamAttachOutcome, ResponseStreamBinding};
use crate::transport::Endpoint;
use crate::transport::tcp::bind_listener;
use tokio::io::duplex;

/// Asynchronous actor/RAII convergence budget, not a protocol latency contract.
const ACTOR_SETTLEMENT_TIMEOUT: Duration = Duration::from_secs(5);

fn security() -> ClientSecurityConfig {
    ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    )
}

fn server_security() -> ServerSecurityConfig {
    ServerSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    )
}

#[derive(Default)]
struct CountingCarrierNetworkProvider {
    socket_count: std::sync::atomic::AtomicUsize,
    socket_identities: std::sync::Mutex<Vec<crate::transport::CarrierPathIdentity>>,
}

impl CountingCarrierNetworkProvider {
    fn socket_count(&self) -> usize {
        self.socket_count.load(std::sync::atomic::Ordering::Acquire)
    }

    fn socket_identities(&self) -> Vec<crate::transport::CarrierPathIdentity> {
        self.socket_identities
            .lock()
            .expect("socket identities lock")
            .clone()
    }
}

impl crate::transport::CarrierNetworkProvider for CountingCarrierNetworkProvider {
    fn resolve<'a>(
        &'a self,
        request: crate::transport::CarrierResolutionRequest<'a>,
    ) -> crate::transport::CarrierResolutionFuture<'a> {
        <crate::transport::SystemCarrierNetworkProvider as crate::transport::CarrierNetworkProvider>::resolve(
            &crate::transport::SystemCarrierNetworkProvider,
            request,
        )
    }

    fn create_socket(
        &self,
        request: crate::transport::CarrierSocketRequest<'_>,
    ) -> std::io::Result<crate::transport::CarrierSocket> {
        self.socket_count
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        self.socket_identities
            .lock()
            .expect("socket identities lock")
            .push(request.identity);
        crate::transport::CarrierSocket::system(request)
    }
}

struct GatedCarrierResolver {
    blocked_path_ordinal: usize,
    started: std::sync::atomic::AtomicBool,
    started_notify: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

impl GatedCarrierResolver {
    fn new(blocked_path_ordinal: usize) -> Self {
        Self {
            blocked_path_ordinal,
            started: std::sync::atomic::AtomicBool::new(false),
            started_notify: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        }
    }

    async fn wait_started(&self) {
        while !self.started.load(std::sync::atomic::Ordering::Acquire) {
            self.started_notify.notified().await;
        }
    }

    fn release(&self) {
        self.release.notify_one();
    }
}

impl crate::transport::CarrierNetworkProvider for GatedCarrierResolver {
    fn resolve<'a>(
        &'a self,
        request: crate::transport::CarrierResolutionRequest<'a>,
    ) -> crate::transport::CarrierResolutionFuture<'a> {
        if request.identity.path_ordinal != self.blocked_path_ordinal {
            return <crate::transport::SystemCarrierNetworkProvider as crate::transport::CarrierNetworkProvider>::resolve(
                &crate::transport::SystemCarrierNetworkProvider,
                request,
            );
        }
        Box::pin(async move {
            request.validate()?;
            self.started
                .store(true, std::sync::atomic::Ordering::Release);
            self.started_notify.notify_waiters();
            self.release.notified().await;
            <crate::transport::SystemCarrierNetworkProvider as crate::transport::CarrierNetworkProvider>::resolve(
                &crate::transport::SystemCarrierNetworkProvider,
                request,
            )
            .await
        })
    }

    fn create_socket(
        &self,
        request: crate::transport::CarrierSocketRequest<'_>,
    ) -> std::io::Result<crate::transport::CarrierSocket> {
        crate::transport::CarrierSocket::system(request)
    }
}

async fn ping_client_udp_stream(
    stream: &mut crate::runtime::path::quic::client::ClientUdpDatagramStream,
) {
    let nonce = random_u64().expect("probe nonce");
    crate::runtime::path::quic::io::udp_path_write_frame(
        &mut stream.send,
        &Frame::Ping { nonce },
        stream.runtime.codec_limits,
    )
    .await
    .expect("write UDP stream ping");
    loop {
        match stream.frames.recv().await.expect("UDP stream response") {
            Ok(Frame::Pong {
                nonce: received_nonce,
            }) if received_nonce == nonce => break,
            Ok(Frame::SessionReady | Frame::PathStatus { .. }) => {}
            Ok(frame) => panic!("unexpected UDP stream frame: {frame:?}"),
            Err(err) => panic!("UDP stream failed before pong: {err}"),
        }
    }
    crate::runtime::path::quic::io::udp_path_finish_stream(&mut stream.send)
        .await
        .expect("finish UDP ping stream");
}

fn server_carrier_identity(
    registration: &ServerCarrierPathRegistration,
) -> ServerCarrierPathIdentity {
    ServerCarrierPathIdentity {
        session_id: registration.session_id(),
        underlay: registration.underlay(),
        path_id: registration.path_id(),
        path_instance_id: registration.path_instance_id(),
    }
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

async fn recv_emitted_tcp_path_command(
    receivers: &mut ReliablePathCommandReceivers,
) -> Option<ReliablePathCommand> {
    let command = recv_reliable_path_command(receivers).await;
    if let Some(command) = &command {
        receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(command));
    }
    command
}

fn server_runtime(outbound: OutboundConfig) -> ServerIdentityRuntime {
    new_identity_runtime(
        Vec::new(),
        outbound,
        DEFAULT_OUTBOUND_CONNECT_TIMEOUT,
        server_security(),
        MppPerformanceConfig::default(),
        ResourceLimits::default(),
    )
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

#[test]
fn socks5_udp_relay_preserves_control_address_family() {
    let v4 = std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);
    let v6 = std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST);

    assert_eq!(socks5_udp_relay_bind_addr(v4), SocketAddr::new(v4, 0));
    assert_eq!(socks5_udp_relay_bind_addr(v6), SocketAddr::new(v6, 0));
}

#[test]
fn socks5_udp_association_is_owned_by_the_tcp_peer_and_one_udp_port() {
    let control_ip = "192.0.2.10".parse().expect("control IP");
    let wildcard = TargetAddr::Ip("0.0.0.0:0".parse().expect("wildcard endpoint"));
    let mut binding = Socks5UdpPeerBinding::new(control_ip, &wildcard).expect("wildcard binding");

    assert!(!binding.accept("192.0.2.11:40000".parse().expect("foreign peer")));
    assert!(binding.accept("192.0.2.10:40001".parse().expect("first peer")));
    assert!(binding.accept("192.0.2.10:40001".parse().expect("same peer")));
    assert!(!binding.accept("192.0.2.10:40002".parse().expect("changed port")));

    let explicit = TargetAddr::Ip("192.0.2.10:41000".parse().expect("explicit endpoint"));
    let mut explicit_binding =
        Socks5UdpPeerBinding::new(control_ip, &explicit).expect("explicit binding");
    assert!(!explicit_binding.accept("192.0.2.10:40999".parse().expect("wrong port")));
    assert!(explicit_binding.accept("192.0.2.10:41000".parse().expect("declared peer")));

    let foreign = TargetAddr::Ip("192.0.2.11:41000".parse().expect("foreign endpoint"));
    assert!(Socks5UdpPeerBinding::new(control_ip, &foreign).is_err());
    let domain = TargetAddr::Domain {
        host: "client.example".to_string(),
        port: 41000,
    };
    assert!(Socks5UdpPeerBinding::new(control_ip, &domain).is_err());
}

#[test]
fn tun_tcp_accept_tasks_share_the_configured_core_stream_ceiling() {
    let limits = MuxLimits {
        max_streams: 37,
        ..MuxLimits::default()
    };

    assert_eq!(tun_tcp_flow_limit(limits), 37);
}

async fn reserve_tcp_path() -> PathSpec {
    let port = reserve_process_unique_tcp_port().await;
    format!("tcp://127.0.0.1:{port}?max-tcp-carriers=1")
        .parse()
        .expect("path")
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
        let TargetAddr::Domain { host, port } = datagram.target else {
            panic!("server must delegate the canonical domain to its SOCKS5 outbound");
        };
        assert_eq!(host, "localhost");
        assert_eq!(port, 53);
        assert_eq!(datagram.payload, Bytes::from_static(b"ping"));
        let response = socks5::udp_datagram(&TargetAddr::Domain { host, port }, b"pong")
            .expect("udp relay response");
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
    let local_path = ServerLocalPath::new(0, path.clone());
    let handle = tokio::spawn(async move {
        let ServerIdentityRuntime {
            paths,
            reliable_relay,
        } = server_runtime(outbound);
        let relay = tokio::spawn(
            reliable_relay
                .expect("L4 test server has a reliable relay")
                .run(),
        );
        let result = async {
            let mut sessions = tokio::task::JoinSet::new();
            for _ in 0..count {
                let (stream, _) = listener.accept().await.expect("accept");
                let session_context = paths.clone();
                let local_path = local_path.clone();
                sessions.spawn(async move {
                    handle_server_path(stream, local_path, session_context).await
                });
            }
            while let Some(session) = sessions.join_next().await {
                session.map_err(RuntimeError::TaskJoin)??;
            }
            Ok(())
        }
        .await;
        relay.abort();
        let _ = relay.await;
        result
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
        let security = server_security();
        let mut framed = EncryptedFramedStream::accept(
            stream,
            &crate::transport::encrypted::test_server_tls_config(),
            CodecLimits::default(),
        )
        .await
        .expect("initialize encrypted stream");
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
        let resources = ResourceLimits::default();
        framed.write_frame(&Frame::SessionReady).await?;
        framed
            .write_frame(&Frame::PathStatus {
                path_id,
                sequence: 0,
                usage: crate::protocol::PathUsage::Available,
            })
            .await?;
        framed.flush().await?;

        let stream_id = loop {
            match framed.read_frame().await? {
                Frame::OpenStream { stream_id, .. } => break stream_id,
                Frame::Ping { nonce } => {
                    framed.write_frame(&Frame::Pong { nonce }).await?;
                    framed.flush().await?;
                }
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
                }
                | Frame::StreamMaxData {
                    stream_id: ack_stream_id,
                    ..
                } if ack_stream_id == stream_id => {}
                Frame::PathProofData {
                    path_id: proof_path_id,
                    proof_id,
                    payload,
                } if proof_path_id == path_id => {
                    framed
                        .write_frame(&Frame::PathProofAck {
                            path_id,
                            proof_id,
                            payload_bytes: u32::try_from(payload.len()).unwrap_or(u32::MAX),
                        })
                        .await?;
                    framed.flush().await?;
                }
                Frame::PathMetrics { .. } => {}
                Frame::SessionClose { .. } => return Ok(()),
                _ => return Err(RuntimeError::Protocol("unexpected heartbeat test frame")),
            }
        }
    });
    (path, handle)
}

#[derive(Clone)]
struct TcpSessionCloseControl {
    send_close: Arc<tokio::sync::Notify>,
    close_written: Arc<tokio::sync::Notify>,
    release_server: Arc<tokio::sync::Notify>,
}

async fn spawn_tcp_session_close_controlled(
    accept_stream: bool,
) -> (
    PathSpec,
    TcpSessionCloseControl,
    tokio::task::JoinHandle<Result<(), RuntimeError>>,
) {
    let path = reserve_tcp_path().await;
    let listener = bind_listener(&path).await.expect("bind");
    let control = TcpSessionCloseControl {
        send_close: Arc::new(tokio::sync::Notify::new()),
        close_written: Arc::new(tokio::sync::Notify::new()),
        release_server: Arc::new(tokio::sync::Notify::new()),
    };
    let server_control = control.clone();
    let handle = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let security = server_security();
        let mut framed = EncryptedFramedStream::accept(
            stream,
            &crate::transport::encrypted::test_server_tls_config(),
            CodecLimits::default(),
        )
        .await?;
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
        framed.write_frame(&Frame::SessionReady).await?;
        framed
            .write_frame(&Frame::PathStatus {
                path_id,
                sequence: 0,
                usage: PathUsage::Available,
            })
            .await?;
        framed.flush().await?;

        if accept_stream {
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
                    max_offset: ResourceLimits::default().max_stream_window_bytes,
                })
                .await?;
            framed.flush().await?;
        }

        server_control.send_close.notified().await;
        framed
            .write_frame(&Frame::SessionClose {
                reason: CloseReason::PolicyRejected,
            })
            .await?;
        framed.flush().await?;
        server_control.close_written.notify_one();
        server_control.release_server.notified().await;
        Ok(())
    });
    (path, control, handle)
}

async fn prepare_tcp_test_carrier(context: &ClientPathContext, path_index: usize) {
    context.tcp_sessions[path_index]
        .prepare_connection(tokio::time::Instant::now() + Duration::from_secs(5))
        .await
        .expect("prepare controlled TCP carrier");
}

async fn wait_for_tcp_test_carrier_retirement(context: &ClientPathContext, path_index: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while context.tcp_sessions[path_index].is_connection_ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("TCP carrier retirement");
}

async fn publish_controlled_tcp_session_close(
    context: &ClientPathContext,
    control: &TcpSessionCloseControl,
) {
    control.send_close.notify_one();
    tokio::time::timeout(Duration::from_secs(2), control.close_written.notified())
        .await
        .expect("server writes authenticated SESSION_CLOSE");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), context.session_retirement().wait())
            .await
            .expect("client publishes sticky SESSION_CLOSE"),
        CloseReason::PolicyRejected
    );
}

fn set_tcp_test_path_latency(path: &mut PathSpec, initial_srtt_ms: u32) {
    path.metadata.initial_srtt_ms = Some(initial_srtt_ms);
    path.metadata.initial_rate = RateHint::BitsPerSecond(100_000_000);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pre_model_red_outward_tcp_open_prefers_sticky_session_terminal() {
    let (path, server_control, server) = spawn_tcp_session_close_controlled(true).await;
    let client =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("context");
    prepare_tcp_test_carrier(&client, 0).await;
    let mut settlement = client.arm_reliable_tcp_settlement_test(0);
    let open_context = client.clone();
    let open = tokio::spawn(async move {
        crate::runtime::relay::open::open_remote_stream(
            &open_context,
            TargetAddr::Ip("127.0.0.1:80".parse().expect("target")),
            TrafficClass::Latency,
        )
        .await
    });

    tokio::time::timeout(Duration::from_secs(2), settlement.wait_reached())
        .await
        .expect("accepted reliable Product reaches outward settlement");
    publish_controlled_tcp_session_close(&client, &server_control).await;
    settlement.release();
    let result = tokio::time::timeout(Duration::from_secs(2), open)
        .await
        .expect("outward reliable open settles")
        .expect("outward reliable task");
    assert!(matches!(
        result,
        Err(RuntimeError::RemoteClosed(CloseReason::PolicyRejected))
    ));
    assert_eq!(client.reliable_selection_passes_for_test(), 1);
    wait_for_tcp_test_carrier_retirement(&client, 0).await;
    assert_eq!(client.authenticated_carriers.snapshot().live_count, 0);
    assert_eq!(
        client.health().lock().expect("path health").tcp[0].active_flows,
        0
    );

    server_control.release_server.notify_one();
    server
        .await
        .expect("controlled server task")
        .expect("controlled server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pre_model_red_outward_tcp_open_does_not_retry_after_sticky_terminal() {
    let (mut first_path, server_control, server) = spawn_tcp_session_close_controlled(true).await;
    let mut unused_path = reserve_tcp_path().await;
    set_tcp_test_path_latency(&mut first_path, 20);
    set_tcp_test_path_latency(&mut unused_path, 400);
    let client = ClientPathContext::new(
        vec![first_path, unused_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    prepare_tcp_test_carrier(&client, 0).await;
    let mut settlement = client.arm_reliable_tcp_settlement_test(0);
    let open_context = client.clone();
    let open = tokio::spawn(async move {
        crate::runtime::relay::open::open_remote_stream(
            &open_context,
            TargetAddr::Ip("127.0.0.1:80".parse().expect("target")),
            TrafficClass::Latency,
        )
        .await
    });

    tokio::time::timeout(Duration::from_secs(2), settlement.wait_reached())
        .await
        .expect("first accepted open reaches outward settlement");
    publish_controlled_tcp_session_close(&client, &server_control).await;
    settlement.release();
    let result = tokio::time::timeout(Duration::from_secs(2), open)
        .await
        .expect("outward reliable open settles")
        .expect("outward reliable task");
    assert!(matches!(
        result,
        Err(RuntimeError::RemoteClosed(CloseReason::PolicyRejected))
    ));
    assert_eq!(
        client.reliable_selection_passes_for_test(),
        1,
        "sticky session retirement must prevent a second selection pass"
    );
    assert!(!client.tcp_sessions[1].is_connection_ready());
    assert!(
        client
            .health()
            .lock()
            .expect("path health")
            .tcp
            .iter()
            .all(|path| path.active_flows == 0)
    );

    server_control.release_server.notify_one();
    server
        .await
        .expect("controlled server task")
        .expect("controlled server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn control_outward_tcp_open_without_terminal_preserves_success() {
    let (path, server_control, server) = spawn_tcp_session_close_controlled(true).await;
    let client =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("context");
    prepare_tcp_test_carrier(&client, 0).await;
    let mut settlement = client.arm_reliable_tcp_settlement_test(0);
    let open_context = client.clone();
    let open = tokio::spawn(async move {
        crate::runtime::relay::open::open_remote_stream(
            &open_context,
            TargetAddr::Ip("127.0.0.1:80".parse().expect("target")),
            TrafficClass::Latency,
        )
        .await
    });

    tokio::time::timeout(Duration::from_secs(2), settlement.wait_reached())
        .await
        .expect("accepted open reaches outward settlement");
    client
        .ensure_session_active()
        .expect("control session remains active");
    settlement.release();
    let opened = tokio::time::timeout(Duration::from_secs(2), open)
        .await
        .expect("outward reliable open settles")
        .expect("outward reliable task")
        .expect("no-terminal open succeeds");
    assert_eq!(opened.path_index(), 0);
    assert_eq!(client.reliable_selection_passes_for_test(), 1);
    assert_eq!(
        client.health().lock().expect("path health").tcp[0].active_flows,
        1
    );
    opened.close().await;
    assert_eq!(
        client.health().lock().expect("path health").tcp[0].active_flows,
        0
    );

    publish_controlled_tcp_session_close(&client, &server_control).await;
    server_control.release_server.notify_one();
    server
        .await
        .expect("controlled server task")
        .expect("controlled server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pre_model_red_outward_tcp_datagram_prefers_sticky_session_terminal() {
    let (path, server_control, server) = spawn_tcp_session_close_controlled(false).await;
    let client =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("context");
    prepare_tcp_test_carrier(&client, 0).await;
    let mut settlement = client.arm_tcp_datagram_settlement_test(0);
    let mut association = DatagramClientAssociation::new(client.clone())
        .await
        .expect("datagram association");
    let send = tokio::spawn(async move {
        let result = association
            .send_to_fresh_datagram_with_route_hint(
                TargetAddr::Ip("127.0.0.1:9".parse().expect("target")),
                Bytes::from_static(b"ping"),
                2_000,
                None,
            )
            .await;
        (association, result)
    });

    tokio::time::timeout(Duration::from_secs(2), settlement.wait_reached())
        .await
        .expect("accepted datagram attachment reaches outward settlement");
    publish_controlled_tcp_session_close(&client, &server_control).await;
    settlement.release();
    let (association, result) = tokio::time::timeout(Duration::from_secs(2), send)
        .await
        .expect("outward datagram send settles")
        .expect("outward datagram task");
    assert!(matches!(
        result,
        Err(RuntimeError::RemoteClosed(CloseReason::PolicyRejected))
    ));
    assert_eq!(client.datagram_candidate_attempts_for_test(), 1);
    assert!(association.next_retry_deadline().is_none());
    assert!(!association.can_receive());
    drop(association);
    wait_for_tcp_test_carrier_retirement(&client, 0).await;
    assert_eq!(
        client.health().lock().expect("path health").tcp[0].active_flows,
        0
    );
    assert_eq!(client.authenticated_carriers.snapshot().live_count, 0);

    server_control.release_server.notify_one();
    server
        .await
        .expect("controlled server task")
        .expect("controlled server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pre_model_red_outward_tcp_datagram_does_not_retry_after_sticky_terminal() {
    let (mut first_path, server_control, server) = spawn_tcp_session_close_controlled(false).await;
    let mut unused_path = reserve_tcp_path().await;
    set_tcp_test_path_latency(&mut first_path, 20);
    set_tcp_test_path_latency(&mut unused_path, 400);
    let client = ClientPathContext::new(
        vec![first_path, unused_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    prepare_tcp_test_carrier(&client, 0).await;
    let mut settlement = client.arm_tcp_datagram_settlement_test(0);
    let mut association = DatagramClientAssociation::new(client.clone())
        .await
        .expect("datagram association");
    let send = tokio::spawn(async move {
        let result = association
            .send_to_fresh_datagram_with_route_hint(
                TargetAddr::Ip("127.0.0.1:9".parse().expect("target")),
                Bytes::from_static(b"ping"),
                3_000,
                None,
            )
            .await;
        (association, result)
    });

    tokio::time::timeout(Duration::from_secs(2), settlement.wait_reached())
        .await
        .expect("first accepted attachment reaches outward settlement");
    publish_controlled_tcp_session_close(&client, &server_control).await;
    settlement.release();
    let (association, result) = tokio::time::timeout(Duration::from_secs(2), send)
        .await
        .expect("outward datagram send settles")
        .expect("outward datagram task");
    assert!(matches!(
        result,
        Err(RuntimeError::RemoteClosed(CloseReason::PolicyRejected))
    ));
    assert_eq!(
        client.datagram_candidate_attempts_for_test(),
        1,
        "sticky session retirement must prevent a second datagram candidate"
    );
    assert!(association.next_retry_deadline().is_none());
    drop(association);
    assert!(
        client
            .health()
            .lock()
            .expect("path health")
            .tcp
            .iter()
            .all(|path| path.active_flows == 0)
    );

    server_control.release_server.notify_one();
    server
        .await
        .expect("controlled server task")
        .expect("controlled server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn control_outward_tcp_datagram_without_terminal_preserves_success() {
    let (target_addr, target) = spawn_udp_echo_target().await;
    let (path, server) = spawn_server_path(OutboundConfig::Direct).await;
    let client =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("context");
    prepare_tcp_test_carrier(&client, 0).await;
    let mut settlement = client.arm_tcp_datagram_settlement_test(0);
    let mut association = DatagramClientAssociation::new(client.clone())
        .await
        .expect("datagram association");
    let send = tokio::spawn(async move {
        let result = association
            .send_to_fresh_datagram_with_route_hint(
                TargetAddr::Ip(target_addr),
                Bytes::from_static(b"ping"),
                2_000,
                None,
            )
            .await;
        (association, result)
    });

    tokio::time::timeout(Duration::from_secs(2), settlement.wait_reached())
        .await
        .expect("accepted attachment reaches outward settlement");
    client
        .ensure_session_active()
        .expect("control session remains active");
    settlement.release();
    let (mut association, result) = tokio::time::timeout(Duration::from_secs(5), send)
        .await
        .expect("outward datagram send settles")
        .expect("outward datagram task");
    result.expect("no-terminal datagram send succeeds");
    assert_eq!(client.datagram_candidate_attempts_for_test(), 1);
    target
        .await
        .expect("UDP target receives real Product datagram");
    client
        .ensure_session_active()
        .expect("successful send keeps SessionId active");
    let _ = association.close().await;
    drop(association);

    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn control_outward_tcp_datagram_retries_after_native_carrier_loss() {
    let (target_addr, target) = spawn_udp_echo_target().await;
    let (mut first_path, _first_control, first_server) =
        spawn_tcp_session_close_controlled(false).await;
    let (mut second_path, second_server) = spawn_server_path(OutboundConfig::Direct).await;
    set_tcp_test_path_latency(&mut first_path, 20);
    set_tcp_test_path_latency(&mut second_path, 400);
    second_path.metadata.policy.backup = true;
    let client = ClientPathContext::new(
        vec![first_path, second_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    prepare_tcp_test_carrier(&client, 0).await;
    prepare_tcp_test_carrier(&client, 1).await;
    let mut settlement = client.arm_tcp_datagram_settlement_test(0);
    let mut association = DatagramClientAssociation::new(client.clone())
        .await
        .expect("datagram association");
    let send = tokio::spawn(async move {
        let result = association
            .send_to_fresh_datagram_with_route_hint(
                TargetAddr::Ip(target_addr),
                Bytes::from_static(b"ping"),
                4_000,
                None,
            )
            .await;
        (association, result)
    });

    tokio::time::timeout(Duration::from_secs(2), settlement.wait_reached())
        .await
        .expect("first accepted attachment reaches outward settlement");
    first_server.abort();
    let _ = first_server.await;
    wait_for_tcp_test_carrier_retirement(&client, 0).await;
    client
        .ensure_session_active()
        .expect("native carrier loss is not SESSION_CLOSE");
    settlement.release();
    let (mut association, result) = tokio::time::timeout(Duration::from_secs(5), send)
        .await
        .expect("two-attempt outward datagram send")
        .expect("outward datagram task");
    result.expect("second TCP carrier sends Product datagram");
    assert_eq!(client.datagram_candidate_attempts_for_test(), 2);
    target.await.expect("fallback UDP target receives datagram");
    client
        .ensure_session_active()
        .expect("successful fallback keeps SessionId active");
    let _ = association.close().await;
    drop(association);

    second_server.abort();
    let _ = second_server.await;
}

async fn spawn_notified_server_path(
    path: PathSpec,
    marker: u8,
    outbound: OutboundConfig,
    accepted: mpsc::Sender<u8>,
) -> tokio::task::JoinHandle<Result<(), RuntimeError>> {
    let listener = bind_listener(&path).await.expect("bind");
    let local_path = ServerLocalPath::new(0, path.clone());
    tokio::spawn(async move {
        let ServerIdentityRuntime {
            paths,
            reliable_relay,
        } = server_runtime(outbound);
        let relay = tokio::spawn(
            reliable_relay
                .expect("L4 test server has a reliable relay")
                .run(),
        );
        let (stream, _) = listener.accept().await.expect("accept");
        let _ = accepted.send(marker).await;
        let result = handle_server_path(stream, local_path, paths).await;
        relay.abort();
        let _ = relay.await;
        result
    })
}

async fn spawn_notified_server_path_with_context(
    path: PathSpec,
    config_ordinal: usize,
    marker: u8,
    paths: crate::runtime::path::ServerPathContext,
    accepted: mpsc::Sender<u8>,
) -> tokio::task::JoinHandle<Result<(), RuntimeError>> {
    let listener = bind_listener(&path).await.expect("bind");
    let local_path = ServerLocalPath::new(config_ordinal, path);
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let _ = accepted.send(marker).await;
        handle_server_path(stream, local_path, paths).await
    })
}

async fn reserve_udp_path() -> PathSpec {
    let port = reserve_process_unique_udp_port().await;
    format!("quic://127.0.0.1:{port}").parse().expect("path")
}

async fn spawn_udp_server_path(
    outbound: OutboundConfig,
) -> (PathSpec, tokio::task::JoinHandle<Result<(), RuntimeError>>) {
    let path = reserve_udp_path().await;
    let ServerIdentityRuntime {
        paths,
        reliable_relay,
    } = server_runtime(outbound);
    let endpoint = bind_server_udp_endpoint(&path, &paths)
        .await
        .expect("bind udp path");
    let local_path = ServerLocalPath::new(0, path.clone());
    let server = tokio::spawn(async move {
        tokio::select! {
            result = run_server_udp_listener(endpoint, local_path, paths) => result,
            result = reliable_relay.expect("L4 test server has a reliable relay").run() => result,
        }
    });
    (path, server)
}

async fn spawn_udp_server_path_with_resources(
    outbound: OutboundConfig,
    resources: ResourceLimits,
) -> (PathSpec, tokio::task::JoinHandle<Result<(), RuntimeError>>) {
    let path = reserve_udp_path().await;
    let ServerIdentityRuntime {
        paths,
        reliable_relay,
    } = new_identity_runtime(
        Vec::new(),
        outbound,
        DEFAULT_OUTBOUND_CONNECT_TIMEOUT,
        server_security(),
        MppPerformanceConfig::default(),
        resources,
    );
    let endpoint = bind_server_udp_endpoint(&path, &paths)
        .await
        .expect("bind udp path");
    let local_path = ServerLocalPath::new(0, path.clone());
    let server = tokio::spawn(async move {
        tokio::select! {
            result = run_server_udp_listener(endpoint, local_path, paths) => result,
            result = reliable_relay.expect("L4 test server has a reliable relay").run() => result,
        }
    });
    (path, server)
}

#[tokio::test(flavor = "multi_thread")]
async fn datagram_product_open_deadline_does_not_fail_the_live_udp_carrier() {
    let resources = ResourceLimits {
        max_quic_concurrent_bidi_streams: 1,
        ..ResourceLimits::default()
    };
    let (path, server) =
        spawn_udp_server_path_with_resources(OutboundConfig::Direct, resources).await;
    let context =
        ClientPathContext::new(vec![path], security(), resources).expect("client path context");
    probe_client_paths(&context, Duration::from_secs(2)).await;
    let carrier_instance = context
        .health()
        .lock()
        .expect("established carrier health")
        .udp[0]
        .path_instance_id()
        .expect("control stream established the carrier");

    let target = UdpSocket::bind("127.0.0.1:0").await.expect("UDP target");
    let mut association = DatagramClientAssociation::new(context.clone())
        .await
        .expect("datagram association");
    let started_at = tokio::time::Instant::now();
    let error = association
        .send_to_fresh_datagram_with_policy(
            TargetAddr::Ip(target.local_addr().expect("UDP target address")),
            Bytes::from_static(b"stream-credit deadline"),
            100,
            None,
            TrafficClass::RealtimeDatagram,
        )
        .await
        .expect_err("the sole QUIC bidi credit remains owned by the control stream");
    assert!(matches!(error, RuntimeError::PathOpenTimedOut));
    assert!(
        started_at.elapsed() < Duration::from_secs(1),
        "the operation-local Product deadline bounds outer association settlement",
    );

    {
        let health = context.health().lock().expect("live carrier health");
        let current = &health.udp[0];
        assert_eq!(current.path_instance_id(), Some(carrier_instance));
        assert_eq!(
            current.state,
            crate::scheduler::PathState::Active,
            "Product stream-credit exhaustion is not evidence that the carrier failed",
        );
        assert_eq!(current.consecutive_failures, 0);
        assert_eq!(
            current.active_flows, 0,
            "a cancelled Product open releases its key-scoped load lease",
        );
    }

    association.close().await.expect("close association");
    server.abort();
    let _ = server.await;
}

async fn quic_admission_with_existing_registration(
    registered_underlay: UnderlayProtocol,
    principal: crate::product::PrincipalPermit,
) -> (Result<Option<Duration>, RuntimeError>, Option<PathUsage>) {
    let path = reserve_udp_path().await;
    let client = ClientPathContext::new(vec![path.clone()], security(), ResourceLimits::default())
        .expect("client context");
    let ServerIdentityRuntime {
        paths,
        reliable_relay: _,
    } = server_runtime(OutboundConfig::Direct);
    let existing = paths
        .reliable_streams
        .register_carrier_path(
            client.session_id,
            registered_underlay,
            PathId(0),
            ServerLocalPathProperties::default(),
            principal,
        )
        .expect("pre-register carrier");
    let server_context = paths.clone();
    let endpoint = bind_server_udp_endpoint(&path, &paths)
        .await
        .expect("bind UDP endpoint");
    let local_path = ServerLocalPath::new(0, path);
    let server = tokio::spawn(run_server_udp_listener(endpoint, local_path, paths));

    let result = client.udp_sessions[0]
        .prepare_connection(tokio::time::Instant::now() + Duration::from_secs(2))
        .await;
    let peer_usage = client.peer_path_usage(UnderlayProtocol::Udp, 0);
    let snapshot = server_context.reliable_streams.management_snapshot();
    assert_eq!(snapshot.paths.len(), 1);
    assert_eq!(snapshot.paths[0].session_id, client.session_id);
    assert_eq!(snapshot.sessions.len(), 1);
    assert_eq!(snapshot.sessions[0].reference_count, 1);

    drop(client);
    drop(existing);
    server.abort();
    let _ = server.await;
    (result, peer_usage)
}

#[tokio::test(flavor = "multi_thread")]
async fn quic_duplicate_carrier_rejection_precedes_readiness() {
    let (result, peer_usage) = quic_admission_with_existing_registration(
        UnderlayProtocol::Udp,
        crate::product::PrincipalPermit::for_test("test-peer"),
    )
    .await;
    assert!(result.is_err(), "duplicate carrier received readiness");
    assert_eq!(peer_usage, None);
}

#[tokio::test(flavor = "multi_thread")]
async fn quic_session_principal_rejection_precedes_readiness() {
    let (result, peer_usage) = quic_admission_with_existing_registration(
        UnderlayProtocol::Tcp,
        crate::product::PrincipalPermit::for_test("different-peer"),
    )
    .await;
    assert!(
        result.is_err(),
        "different-principal carrier received readiness"
    );
    assert_eq!(peer_usage, None);
}

#[tokio::test(flavor = "multi_thread")]
async fn pre_model_red_quic_session_close_interrupts_a_sibling_in_flight_connect() {
    let path = reserve_udp_path().await;
    let provider = Arc::new(GatedCarrierResolver::new(1));
    let client = ClientPathContext::new_with_carrier_network(
        vec![
            crate::config::ClientPathConfig {
                name: "rejecting-path".to_string(),
                tls: crate::transport::encrypted::test_client_tls_config(),
                spec: path.clone(),
                security: security(),
            },
            crate::config::ClientPathConfig {
                name: "blocked-sibling".to_string(),
                tls: crate::transport::encrypted::test_client_tls_config(),
                spec: path.clone(),
                security: security(),
            },
        ],
        ResourceLimits::default(),
        None,
        0,
        provider.clone(),
    )
    .expect("client context");
    let ServerIdentityRuntime {
        paths,
        reliable_relay: _,
    } = server_runtime(OutboundConfig::Direct);
    let endpoint = bind_server_udp_endpoint(&path, &paths)
        .await
        .expect("bind terminal UDP endpoint");
    let emit_close = Arc::new(tokio::sync::Notify::new());
    let release_server = Arc::new(tokio::sync::Notify::new());
    let server_emit_close = emit_close.clone();
    let server_release = release_server.clone();
    let server = tokio::spawn(async move {
        let connection = endpoint
            .accept_for_test()
            .await
            .ok_or(RuntimeError::Protocol("test QUIC endpoint closed"))?;
        let (_registration, mut control_send, _control_recv) =
            accept_server_udp_path_handshake_for_test(
                &connection,
                &ServerLocalPath::new(0, path),
                &paths,
            )
            .await?;
        server_emit_close.notified().await;
        crate::runtime::path::quic::io::udp_path_write_frame(
            &mut control_send,
            &Frame::SessionClose {
                reason: CloseReason::PolicyRejected,
            },
            paths.codec_limits,
        )
        .await?;
        server_release.notified().await;
        Ok::<(), RuntimeError>(())
    });

    client.udp_sessions[0]
        .prepare_connection(tokio::time::Instant::now() + Duration::from_secs(2))
        .await
        .expect("establish authenticated carrier that can emit SESSION_CLOSE");

    let blocked_session = client.udp_sessions[1].clone();
    let mut blocked = tokio::spawn(async move {
        blocked_session
            .prepare_connection(tokio::time::Instant::now() + Duration::from_secs(5))
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), provider.wait_started())
        .await
        .expect("sibling resolver entered its in-flight connect");

    emit_close.notify_one();
    let sibling_result = tokio::time::timeout(Duration::from_secs(1), &mut blocked)
        .await
        .expect("SESSION_CLOSE must cancel the sibling resolver")
        .expect("sibling connect task");
    assert!(matches!(
        sibling_result,
        Err(RuntimeError::RemoteClosed(CloseReason::PolicyRejected))
    ));
    tokio::time::timeout(ACTOR_SETTLEMENT_TIMEOUT, async {
        while client.authenticated_carriers.snapshot().live_count != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("retired QUIC carrier releases its authenticated registration");
    assert_eq!(client.authenticated_carriers.snapshot().live_count, 0);

    provider.release();
    release_server.notify_one();
    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread")]
async fn active_quic_in_flight_connect_remains_owned_until_its_resolver_releases() {
    let (path, server) = spawn_udp_server_path(OutboundConfig::Direct).await;
    let provider = Arc::new(GatedCarrierResolver::new(0));
    let client = ClientPathContext::new_with_carrier_network(
        vec![crate::config::ClientPathConfig {
            name: "gated-active-path".to_string(),
            tls: crate::transport::encrypted::test_client_tls_config(),
            spec: path,
            security: security(),
        }],
        ResourceLimits::default(),
        None,
        0,
        provider.clone(),
    )
    .expect("client context");
    let session = client.udp_sessions[0].clone();
    let mut preparing = tokio::spawn(async move {
        session
            .prepare_connection(tokio::time::Instant::now() + Duration::from_secs(5))
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), provider.wait_started())
        .await
        .expect("resolver gate entered");
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut preparing)
            .await
            .is_err(),
        "an active session must not cancel an in-flight connection"
    );
    client
        .ensure_session_active()
        .expect("session stays active");
    assert_eq!(client.authenticated_carriers.snapshot().live_count, 0);

    provider.release();
    preparing
        .await
        .expect("prepare task")
        .expect("released active QUIC connection");
    assert_eq!(client.authenticated_carriers.snapshot().live_count, 1);

    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pre_model_red_tcp_session_close_interrupts_a_backpressured_ordered_writer() {
    let (path, server_control, server) = spawn_tcp_session_close_controlled(true).await;
    let resources = ResourceLimits::default();
    let client = ClientPathContext::new(vec![path], security(), resources).expect("client context");
    prepare_tcp_test_carrier(&client, 0).await;
    let stream_id = StreamId(997);
    let opened = client.tcp_sessions[0]
        .open_stream_with_deadlines(
            stream_id,
            TargetAddr::Ip("127.0.0.1:80".parse().expect("target")),
            TrafficClass::Throughput,
            StreamDemandHint::Throughput,
            crate::runtime::path::commands::ClientTcpOpenDeadlines::fixed(
                tokio::time::Instant::now() + Duration::from_secs(5),
            ),
            resources.max_stream_window_bytes,
        )
        .await
        .expect("open TCP product stream");
    let max_frame_payload_bytes = opened.carrier.max_frame_payload_bytes;
    let commands = opened.carrier.commands.clone();
    let mut inbound_frames = opened.carrier.frames;
    let flood_commands = commands.clone();
    let flood = tokio::spawn(async move {
        let payload = Bytes::from(vec![0x5a; max_frame_payload_bytes]);
        while flood_commands
            .send_stream_ordered_frame(
                Frame::StreamData {
                    stream_id,
                    offset: 0,
                    payload: payload.clone(),
                },
                TrafficClass::Throughput,
            )
            .await
            .is_ok()
        {}
    });

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if !commands
                .queue_snapshot()
                .can_enqueue_lane(TrafficClass::Throughput)
            {
                tokio::time::sleep(Duration::from_millis(150)).await;
                if !commands
                    .queue_snapshot()
                    .can_enqueue_lane(TrafficClass::Throughput)
                {
                    break;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("TCP writer reaches sustained carrier backpressure");

    server_control.send_close.notify_one();
    tokio::time::timeout(
        Duration::from_secs(2),
        server_control.close_written.notified(),
    )
    .await
    .expect("server writes authenticated SESSION_CLOSE");
    let reason = tokio::time::timeout(Duration::from_secs(1), client.session_retirement().wait())
        .await
        .expect("decoded SESSION_CLOSE must bypass the ordered writer barrier");
    assert_eq!(reason, CloseReason::PolicyRejected);
    let actor_settlement_deadline = tokio::time::Instant::now() + ACTOR_SETTLEMENT_TIMEOUT;
    assert!(matches!(
        tokio::time::timeout_at(actor_settlement_deadline, inbound_frames.recv())
            .await
            .expect("product stream receives terminal fanout"),
        Some(Err(RuntimeError::RemoteClosed(CloseReason::PolicyRejected)))
    ));
    tokio::time::timeout_at(actor_settlement_deadline, async {
        while client.tcp_sessions[0].is_connection_ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("terminal TCP actor withdraws readiness");
    assert_eq!(client.authenticated_carriers.snapshot().live_count, 0);
    assert!(matches!(
        client.tcp_sessions[0]
            .prepare_connection(tokio::time::Instant::now() + Duration::from_secs(1))
            .await,
        Err(RuntimeError::RemoteClosed(CloseReason::PolicyRejected))
    ));

    tokio::time::timeout_at(actor_settlement_deadline, flood)
        .await
        .expect("terminal actor wakes the blocked ordered producer")
        .expect("ordered producer task");
    assert_eq!(commands.pending_bytes(), 0);
    assert_eq!(commands.writer_pending_bytes(), 0);
    server_control.release_server.notify_one();
    server
        .await
        .expect("server task")
        .expect("server SESSION_CLOSE schedule");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pre_model_red_tcp_queued_open_observes_sticky_session_terminal() {
    let (path, server_control, server) = spawn_tcp_session_close_controlled(true).await;
    let resources = ResourceLimits::default();
    let client = ClientPathContext::new(vec![path], security(), resources).expect("client context");
    prepare_tcp_test_carrier(&client, 0).await;
    let active_stream_id = StreamId(1_001);
    let opened = client.tcp_sessions[0]
        .open_stream_with_deadlines(
            active_stream_id,
            TargetAddr::Ip("127.0.0.1:80".parse().expect("target")),
            TrafficClass::Throughput,
            StreamDemandHint::Throughput,
            crate::runtime::path::commands::ClientTcpOpenDeadlines::fixed(
                tokio::time::Instant::now() + Duration::from_secs(5),
            ),
            resources.max_stream_window_bytes,
        )
        .await
        .expect("open first TCP product stream");
    let max_frame_payload_bytes = opened.carrier.max_frame_payload_bytes;
    let commands = opened.carrier.commands.clone();
    let _inbound_frames = opened.carrier.frames;
    let flood_commands = commands.clone();
    let flood = tokio::spawn(async move {
        let payload = Bytes::from(vec![0x7c; max_frame_payload_bytes]);
        while flood_commands
            .send_stream_ordered_frame(
                Frame::StreamData {
                    stream_id: active_stream_id,
                    offset: 0,
                    payload: payload.clone(),
                },
                TrafficClass::Throughput,
            )
            .await
            .is_ok()
        {}
    });

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if !commands
                .queue_snapshot()
                .can_enqueue_lane(TrafficClass::Throughput)
            {
                tokio::time::sleep(Duration::from_millis(150)).await;
                if !commands
                    .queue_snapshot()
                    .can_enqueue_lane(TrafficClass::Throughput)
                {
                    break;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("TCP writer reaches sustained carrier backpressure");

    let queued_stream_id = StreamId(1_002);
    let queued_session = client.tcp_sessions[0].clone();
    let mut queued_open = tokio::spawn(async move {
        queued_session
            .open_stream_with_deadlines(
                queued_stream_id,
                TargetAddr::Ip("127.0.0.1:81".parse().expect("target")),
                TrafficClass::Latency,
                StreamDemandHint::Latency,
                crate::runtime::path::commands::ClientTcpOpenDeadlines::fixed(
                    tokio::time::Instant::now() + Duration::from_secs(5),
                ),
                resources.max_stream_window_bytes,
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !commands.open_stream_command_accepted_for_test(queued_stream_id)
            || commands.control_queue_len_for_test() == 0
        {
            assert!(
                !queued_open.is_finished(),
                "second open completed before its command was accepted"
            );
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("second OPEN_STREAM crosses the accepted-command boundary");

    server_control.send_close.notify_one();
    tokio::time::timeout(
        Duration::from_secs(2),
        server_control.close_written.notified(),
    )
    .await
    .expect("server writes authenticated SESSION_CLOSE");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), client.session_retirement().wait())
            .await
            .expect("reader publishes complete-session retirement"),
        CloseReason::PolicyRejected
    );
    let queued_result = tokio::time::timeout(Duration::from_secs(1), &mut queued_open)
        .await
        .expect("queued open receives terminal settlement")
        .expect("queued open task");
    assert!(matches!(
        queued_result,
        Err(RuntimeError::RemoteClosed(CloseReason::PolicyRejected))
    ));
    let actor_settlement_deadline = tokio::time::Instant::now() + ACTOR_SETTLEMENT_TIMEOUT;
    tokio::time::timeout_at(actor_settlement_deadline, async {
        while client.tcp_sessions[0].is_connection_ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("terminal actor withdraws readiness after settling accepted work");
    assert_eq!(client.authenticated_carriers.snapshot().live_count, 0);
    tokio::time::timeout_at(actor_settlement_deadline, async {
        while !commands.is_closed() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("terminal actor releases the accepted command queue");

    flood.abort();
    let _ = flood.await;
    server_control.release_server.notify_one();
    server
        .await
        .expect("server task")
        .expect("server SESSION_CLOSE schedule");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pre_model_red_tcp_sibling_session_close_interrupts_a_backpressured_actor() {
    let (blocked_path, blocked_control, blocked_server) =
        spawn_tcp_session_close_controlled(true).await;
    let (closing_path, closing_control, closing_server) =
        spawn_tcp_session_close_controlled(false).await;
    let resources = ResourceLimits::default();
    let client = ClientPathContext::new(vec![blocked_path, closing_path], security(), resources)
        .expect("two-carrier client context");
    prepare_tcp_test_carrier(&client, 0).await;
    let stream_id = StreamId(998);
    let opened = client.tcp_sessions[0]
        .open_stream_with_deadlines(
            stream_id,
            TargetAddr::Ip("127.0.0.1:80".parse().expect("target")),
            TrafficClass::Throughput,
            StreamDemandHint::Throughput,
            crate::runtime::path::commands::ClientTcpOpenDeadlines::fixed(
                tokio::time::Instant::now() + Duration::from_secs(5),
            ),
            resources.max_stream_window_bytes,
        )
        .await
        .expect("open stream on carrier A");
    client.tcp_sessions[1]
        .prepare_connection(tokio::time::Instant::now() + Duration::from_secs(5))
        .await
        .expect("prepare sibling carrier B");
    assert_eq!(client.authenticated_carriers.snapshot().live_count, 2);

    let max_frame_payload_bytes = opened.carrier.max_frame_payload_bytes;
    let commands = opened.carrier.commands.clone();
    let mut inbound_frames = opened.carrier.frames;
    let flood_commands = commands.clone();
    let flood = tokio::spawn(async move {
        let payload = Bytes::from(vec![0x6b; max_frame_payload_bytes]);
        while flood_commands
            .send_stream_ordered_frame(
                Frame::StreamData {
                    stream_id,
                    offset: 0,
                    payload: payload.clone(),
                },
                TrafficClass::Throughput,
            )
            .await
            .is_ok()
        {}
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if !commands
                .queue_snapshot()
                .can_enqueue_lane(TrafficClass::Throughput)
            {
                tokio::time::sleep(Duration::from_millis(150)).await;
                if !commands
                    .queue_snapshot()
                    .can_enqueue_lane(TrafficClass::Throughput)
                {
                    break;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("carrier A reaches sustained writer backpressure");

    closing_control.send_close.notify_one();
    tokio::time::timeout(
        Duration::from_secs(2),
        closing_control.close_written.notified(),
    )
    .await
    .expect("carrier B writes authenticated SESSION_CLOSE");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), client.session_retirement().wait())
            .await
            .expect("carrier B publishes complete-session retirement"),
        CloseReason::PolicyRejected
    );
    let actor_settlement_deadline = tokio::time::Instant::now() + ACTOR_SETTLEMENT_TIMEOUT;
    assert!(matches!(
        tokio::time::timeout_at(actor_settlement_deadline, inbound_frames.recv())
            .await
            .expect("sibling terminal cancels carrier A Product owner"),
        Some(Err(RuntimeError::RemoteClosed(CloseReason::PolicyRejected)))
    ));
    tokio::time::timeout_at(actor_settlement_deadline, async {
        while client.tcp_sessions[0].is_connection_ready()
            || client.tcp_sessions[1].is_connection_ready()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("complete-session retirement withdraws both TCP carriers");
    assert_eq!(client.authenticated_carriers.snapshot().live_count, 0);

    flood.abort();
    let _ = flood.await;
    closing_control.release_server.notify_one();
    closing_server
        .await
        .expect("closing server task")
        .expect("closing carrier schedule");
    blocked_server.abort();
    let _ = blocked_server.await;
    drop(blocked_control);
}

#[tokio::test(flavor = "multi_thread")]
async fn tcp_native_carrier_loss_preserves_the_session_and_allows_reconnect() {
    let (path, first_server) = spawn_server_path(OutboundConfig::Direct).await;
    let client = ClientPathContext::new(vec![path.clone()], security(), ResourceLimits::default())
        .expect("client context");
    client.tcp_sessions[0]
        .prepare_connection(tokio::time::Instant::now() + Duration::from_secs(5))
        .await
        .expect("prepare first TCP carrier");
    assert_eq!(client.authenticated_carriers.snapshot().live_count, 1);

    first_server.abort();
    let _ = first_server.await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while client.tcp_sessions[0].is_connection_ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("native loss withdraws only the failed carrier");
    client
        .ensure_session_active()
        .expect("native carrier loss is not SESSION_CLOSE");
    assert_eq!(client.authenticated_carriers.snapshot().live_count, 0);

    let (accepted_tx, mut accepted_rx) = mpsc::channel(1);
    let replacement_server =
        spawn_notified_server_path(path, 7, OutboundConfig::Direct, accepted_tx).await;
    client.tcp_sessions[0]
        .prepare_connection(tokio::time::Instant::now() + Duration::from_secs(5))
        .await
        .expect("reconnect after carrier-local native loss");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), accepted_rx.recv())
            .await
            .expect("replacement accept timeout"),
        Some(7)
    );
    client
        .ensure_session_active()
        .expect("replacement preserves the same active SessionId");
    assert_eq!(client.authenticated_carriers.snapshot().live_count, 1);

    replacement_server.abort();
    let _ = replacement_server.await;
}

#[derive(Default)]
struct UnexpectedPacketDeviceProvider {
    open_count: std::sync::atomic::AtomicUsize,
}

impl crate::platform::PacketDeviceProvider for UnexpectedPacketDeviceProvider {
    fn open(
        &self,
        _config: &crate::platform::PacketDeviceConfig<'_>,
    ) -> std::io::Result<crate::platform::PacketDevice> {
        self.open_count
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Err(std::io::Error::other(
            "terminal TUN startup must not open a packet device",
        ))
    }
}

#[tokio::test]
async fn terminal_tun_startup_refuses_before_packet_device_or_readiness_publication() {
    let path = reserve_tcp_path().await;
    let client = ClientPathContext::new(vec![path], security(), ResourceLimits::default())
        .expect("client context");
    client.retire_session(CloseReason::PolicyRejected);
    let devices = Arc::new(UnexpectedPacketDeviceProvider::default());
    let generation = crate::runtime::RuntimeGenerationControl::new();
    let barrier = crate::runtime::readiness::RuntimeReadinessBarrier::new(generation.clone());
    let readiness = barrier.require("terminal-tun-test");
    barrier.seal();

    let result = crate::runtime::tun_l3::run_client_tun_l3(
        "terminal-tun".to_string(),
        crate::ingress::TunL3IngressConfig {
            outbound: crate::product::OutboundId::parse("terminal-tun").expect("outbound ID"),
            interface_name: None,
        },
        client,
        devices.clone(),
        readiness,
    )
    .await;
    assert!(matches!(
        result,
        Err(RuntimeError::RemoteClosed(CloseReason::PolicyRejected))
    ));
    assert_eq!(
        devices
            .open_count
            .load(std::sync::atomic::Ordering::Acquire),
        0
    );
    assert!(!generation.is_ready());
}

#[tokio::test(flavor = "multi_thread")]
async fn quic_readiness_follows_committed_carrier_registration() {
    let path = reserve_udp_path().await;
    let client = ClientPathContext::new(vec![path.clone()], security(), ResourceLimits::default())
        .expect("client context");
    let ServerIdentityRuntime {
        paths,
        reliable_relay: _,
    } = server_runtime(OutboundConfig::Direct);
    let server_context = paths.clone();
    let endpoint = bind_server_udp_endpoint(&path, &paths)
        .await
        .expect("bind UDP endpoint");
    let local_path = ServerLocalPath::new(0, path);
    let server = tokio::spawn(run_server_udp_listener(endpoint, local_path, paths));

    assert!(
        client.udp_sessions[0]
            .prepare_connection(tokio::time::Instant::now() + Duration::from_secs(2))
            .await
            .expect("prepare accepted carrier")
            .is_some()
    );
    let snapshot = server_context.reliable_streams.management_snapshot();
    assert_eq!(snapshot.paths.len(), 1);
    assert_eq!(snapshot.paths[0].session_id, client.session_id);
    assert_eq!(snapshot.sessions.len(), 1);
    assert_eq!(snapshot.sessions[0].session_id, client.session_id);
    assert_eq!(snapshot.sessions[0].reference_count, 1);
    assert_eq!(
        client.peer_path_usage(UnderlayProtocol::Udp, 0),
        Some(PathUsage::Available)
    );

    drop(client);
    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread")]
async fn server_udp_listener_accepts_probe_after_noise() {
    let bind_path = reserve_udp_path().await;
    let ServerIdentityRuntime {
        paths,
        reliable_relay,
    } = server_runtime(OutboundConfig::Direct);
    let endpoint = bind_server_udp_endpoint(&bind_path, &paths)
        .await
        .expect("bind udp");
    let local_path = ServerLocalPath::new(0, bind_path.clone());
    let server_addr = endpoint.local_addr().expect("udp server addr");
    let server = tokio::spawn(async move {
        tokio::select! {
            result = run_server_udp_listener(endpoint, local_path, paths) => result,
            result = reliable_relay.expect("L4 test server has a reliable relay").run() => result,
        }
    });

    let noise = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("noise udp bind");
    noise
        .send_to(b"not a udp carrier packet", server_addr)
        .await
        .expect("send noise");

    let path = format!("quic://{server_addr}")
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
        .ping_until(tokio::time::Instant::now() + Duration::from_secs(2))
        .await
        .expect("udp ping");
    session.close().await.expect("finish UDP probe stream");

    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread")]
async fn udp_probe_prepares_and_reuses_the_durable_carrier() {
    let (path, server) = spawn_udp_server_path(OutboundConfig::Direct).await;
    let resources = ResourceLimits::default();
    let provider = Arc::new(CountingCarrierNetworkProvider::default());
    let context = ClientPathContext::new_with_carrier_network(
        vec![crate::config::ClientPathConfig {
            name: "path-1".to_string(),
            tls: crate::transport::encrypted::test_client_tls_config(),
            spec: path,
            security: security(),
        }],
        resources,
        None,
        5,
        provider.clone(),
    )
    .expect("client path context");

    probe_udp_client_path(&context, 0, Duration::from_secs(2))
        .await
        .expect("prepare cold UDP path")
        .expect("cold UDP path publishes native RTT");
    assert_eq!(provider.socket_count(), 1);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut product_stream = context.udp_sessions[0]
        .open_datagram_stream(deadline)
        .await
        .expect("open durable UDP carrier stream");
    let product_instance = product_stream.path_instance_id;
    ping_client_udp_stream(&mut product_stream).await;
    assert_eq!(provider.socket_count(), 1);

    let observation = probe_udp_client_path(&context, 0, Duration::from_secs(2))
        .await
        .expect("observe durable UDP carrier");
    assert!(
        observation.is_none(),
        "a ready carrier does not manufacture fresh liveness evidence"
    );
    assert_eq!(provider.socket_count(), 1);
    assert_eq!(
        provider.socket_identities(),
        vec![crate::transport::CarrierPathIdentity {
            group_ordinal: 5,
            path_ordinal: 0,
        }],
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut reused_stream = context.udp_sessions[0]
        .open_datagram_stream(deadline)
        .await
        .expect("reuse durable UDP carrier");
    assert_eq!(reused_stream.path_instance_id, product_instance);
    assert_eq!(provider.socket_count(), 1);
    ping_client_udp_stream(&mut reused_stream).await;

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
        max_reinjection_cache_chunks: 65_536,
        max_reorder_buffer_chunks: 65_536,
        max_retained_receive_ranges: 65_536,
        max_datagram_queue_bytes: 1024 * 1024,
        max_path_flight_bytes: 32 * 1024,
        max_reliable_relay_chunk_bytes: 32 * 1024,
        tcp_path_heartbeat_interval: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_INTERVAL,
        tcp_path_heartbeat_timeout: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_TIMEOUT,
        quic_path_keep_alive_interval: crate::config::DEFAULT_QUIC_PATH_KEEP_ALIVE_INTERVAL,
        quic_path_idle_timeout: crate::config::DEFAULT_QUIC_PATH_IDLE_TIMEOUT,
    };
    let send_stream = ReliableSendStream::new(StreamId(9), mux_limits);
    let mut sender_queue = ReliableRelaySenderQueue::default();
    let queue_limit =
        reliable_relay_sender_queue_limit(mux_limits, mux_limits.max_path_flight_bytes);

    assert!(reliable_relay_can_read_into_sender_queue(
        &send_stream,
        &sender_queue,
        queue_limit
    ));
    assert_eq!(
        reliable_relay_sender_queue_read_budget(
            &send_stream,
            &sender_queue,
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
        queue_limit
    ));
    assert!(reliable_relay_can_read_product_source(
        true,
        false,
        &send_stream,
        &sender_queue,
        queue_limit
    ));

    sender_queue.push_data(Bytes::from(vec![0u8; 24 * 1024]));
    assert!(!reliable_relay_can_read_into_sender_queue(
        &send_stream,
        &sender_queue,
        queue_limit
    ));
    assert_eq!(
        reliable_relay_sender_queue_read_budget(
            &send_stream,
            &sender_queue,
            queue_limit,
            64 * 1024
        ),
        0
    );

    sender_queue.pop_front();
    assert!(reliable_relay_can_read_into_sender_queue(
        &send_stream,
        &sender_queue,
        queue_limit
    ));
    assert_eq!(
        reliable_relay_sender_queue_read_budget(
            &send_stream,
            &sender_queue,
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
            larger_queue_limit,
            16 * 1024
        ),
        16 * 1024
    );
}

#[test]
fn reliable_relay_sender_queue_prioritizes_only_critical_reinjection_lane() {
    let mut queue = ReliableRelaySenderQueue::default();
    queue.push_data(Bytes::from_static(b"ordinary"));
    queue.push_reinjection(Frame::StreamData {
        stream_id: StreamId(9),
        offset: 0,
        payload: Bytes::from_static(b"reinjection"),
    });

    let (lane, work) = queue.pop_front().expect("owner data");
    assert_eq!(lane, ReliableWorkClass::Data);
    assert_eq!(work.payload_bytes, b"ordinary".len());
    assert_eq!(queue.data_bytes(), 0);

    queue.push_data(Bytes::from_static(b"ordinary"));
    queue.push_critical_reinjection_with_cause(
        Frame::StreamData {
            stream_id: StreamId(9),
            offset: 0,
            payload: Bytes::from_static(b"reinjection"),
        },
        RelaySendCause::AckGapReinjection,
    );
    let (lane, work) = queue.pop_front().expect("critical reinjection work");
    assert_eq!(lane, ReliableWorkClass::Reinjection);
    assert!(matches!(
        work.kind,
        ReliableRelayQueuedWorkKind::Reinjection {
            frame: Frame::StreamData { .. },
            cause: RelaySendCause::AckGapReinjection,
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
        lane: TrafficClass::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
        output: ReliablePathStreamOutput::fixed(
            UnderlayProtocol::Tcp,
            PathId(0),
            commands,
            MuxLimits::default(),
        ),
        frames: frame_rx.into(),
    };
    let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
    send_stream.update_max_offset(1024);
    let mut sender = ServerResponseSenderService::new(SessionId(7), stream_id);
    sender.enqueue_data_for_lane(Bytes::from_static(b"response"), TrafficClass::Throughput);

    assert!(sender.queued_send_ready());
    let dispatch = sender
        .dispatch_next(
            &path_stream,
            &mut send_stream,
            TrafficClass::Throughput,
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
        lane: TrafficClass::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: reliable_relay_buffer_len(mux_limits),
        output: ReliablePathStreamOutput::fixed(
            UnderlayProtocol::Tcp,
            PathId(3),
            commands,
            mux_limits,
        ),
        frames: frame_rx.into(),
    };
    let startup_snapshot = path_stream.send_path_snapshot(TrafficClass::Throughput, 1);
    let startup_quantum =
        adaptive_reliable_relay_chunk_bytes(startup_snapshot, TrafficClass::Throughput, mux_limits);
    let mut send_stream = ReliableSendStream::new(stream_id, mux_limits);
    send_stream.update_max_offset(mux_limits.max_stream_window_bytes);
    let mut sender = ServerResponseSenderService::new(SessionId(52), stream_id);
    sender.enqueue_data_for_lane(
        Bytes::from(vec![0x5a; PATH_OPEN_SCORE_BYTES * 4]),
        TrafficClass::Throughput,
    );

    let mut ack_end = 0_u64;
    while ack_end < PATH_OPEN_SCORE_BYTES as u64 {
        let dispatch = sender
            .dispatch_next(
                &path_stream,
                &mut send_stream,
                TrafficClass::Throughput,
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
        .send_path_snapshot(TrafficClass::Throughput, startup_quantum)
        .expect("fixed output exposes learned path model");
    assert!(learned.product_progress_rate_bps.is_some());
    assert!(
        !learned.has_durable_product_progress,
        "one small ACK batch exposes a rate without graduating product authority"
    );
    assert!(
        adaptive_reliable_relay_chunk_bytes(Some(learned), TrafficClass::Throughput, mux_limits)
            >= startup_quantum
    );
}

#[test]
fn client_snapshot_separates_product_progress_from_native_carrier_evidence() {
    let path: PathSpec = "quic://127.0.0.1:1".parse().expect("UDP path");
    let sample_floor =
        (MAX_RELIABLE_SERVICE_QUANTUM_BYTES as u64).max(PATH_OPEN_SCORE_BYTES as u64);
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

    let mut native_observation = ClientPathObservation {
        carrier_delivery_rate_bps: Some(117_000_000.0),
        carrier_delivery_samples: 1,
        carrier_delivery_sample_bytes: sample_floor - 1,
        carrier_ack_derived_data_seen: true,
        carrier_app_limited: false,
        carrier_bulk_proof_expires_at: Some(Instant::now() + Duration::from_secs(1)),
        ..ClientPathObservation::default()
    };
    assert!(!bulk_candidate_has_bulk_rate_evidence(
        &path,
        native_observation
    ));
    native_observation.carrier_delivery_sample_bytes = sample_floor;
    let native = path_snapshot(&path, 0, native_observation);
    assert!(!native.has_durable_product_progress);
    assert!(bulk_candidate_has_bulk_rate_evidence(
        &path,
        native_observation
    ));
    assert!(path_model_confidence(native_observation) < 1.0);

    observation.product_delivery_sample_bytes = sample_floor;
    let durable = path_snapshot(&path, 0, observation);
    assert!(durable.has_durable_product_progress);
    assert!(bulk_candidate_has_bulk_rate_evidence(&path, observation));
}

#[test]
fn data_plane_failure_invalidates_durable_product_and_native_window_authority() {
    let path: PathSpec = "quic://127.0.0.1:2".parse().expect("UDP path");
    let sample_floor = 7 * 1024 * 1024_u64;
    let mut health = ClientPathHealthRecord::default();
    health.carrier_inflight_limit_bytes = sample_floor;
    health.carrier_delivery_rate_bps = Some(500_000_000.0);
    health.carrier_delivery_sample_bytes = sample_floor;
    health.carrier_delivery_samples = 32;
    health.mark_product_delivery(
        PathRateSample::new(sample_floor, Duration::from_millis(360)).expect("rate sample"),
    );
    let before_failure = health.observation_at(Instant::now());
    assert!(path_snapshot(&path, 0, before_failure).has_durable_product_progress);

    let path_instance_id = crate::model::path::next_carrier_path_instance_id();
    health.install_peer_usage(path_instance_id, 0, crate::protocol::PathUsage::Available);
    assert!(health.mark_data_plane_failure(path_instance_id, Instant::now(), false));
    let after_failure = health.observation_at(Instant::now());
    assert_eq!(after_failure.state, SchedulerPathState::Suspect);
    assert!(after_failure.product_delivery_rate_bps.is_none());
    assert_eq!(after_failure.product_delivery_sample_bytes, 0);
    assert!(after_failure.carrier_delivery_rate_bps.is_none());
    assert_eq!(after_failure.carrier_inflight_limit_bytes, 0);
    assert_eq!(after_failure.carrier_delivery_samples, 0);
    assert_eq!(after_failure.carrier_delivery_sample_bytes, 0);
    assert!(!after_failure.carrier_ack_derived_data_seen);
    assert!(!path_snapshot(&path, 0, after_failure).has_durable_product_progress);
}

#[test]
fn fixed_output_graduates_fragmented_product_acks_at_exact_sample_floor() {
    let mux_limits = MuxLimits::default();
    let (commands, _receivers) = reliable_path_command_channels(8);
    let output =
        ReliablePathStreamOutput::fixed(UnderlayProtocol::Udp, PathId(4), commands, mux_limits);
    let sample_bytes =
        usize::try_from(reliable_path_startup_sample_limit_bytes(mux_limits)).unwrap();
    let frame = Frame::StreamData {
        stream_id: StreamId(53),
        offset: 0,
        payload: Bytes::from(vec![0x5b; sample_bytes]),
    };
    let ReliablePathStreamOutput::Fixed(fixed) = &output else {
        panic!("expected fixed output");
    };
    fixed.record_original_flight(&frame);
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
        .send_path_snapshot(TrafficClass::Throughput, 1)
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
    startup.data_level_limit_bytes =
        data_level_service_window_bytes(startup, TrafficClass::Throughput, mux_limits).ceil()
            as u64;
    startup.confidence = 1.0;
    let output = ReliablePathStreamOutput::fixed_with_snapshot(startup, commands, mux_limits);

    let inherited = output
        .send_path_snapshot(TrafficClass::Throughput, 1)
        .expect("fixed output exposes startup path model");
    let default = PathSnapshot::new(
        PathId(9),
        UnderlayProtocol::Tcp,
        default_path_srtt_ms(),
        default_path_rate_bps(),
    );

    assert_eq!(inherited.id, startup.id);
    assert_eq!(inherited.underlay, startup.underlay);
    assert_eq!(inherited.delivery_rate_bps, startup.delivery_rate_bps);
    assert_eq!(inherited.srtt_ms, startup.srtt_ms);
    assert_eq!(
        adaptive_reliable_relay_chunk_bytes(Some(inherited), TrafficClass::Throughput, mux_limits),
        adaptive_reliable_relay_chunk_bytes(Some(default), TrafficClass::Throughput, mux_limits),
        "TCP throughput startup uses the bounded MPP feed quantum before path-rate evidence is measured"
    );
}

#[test]
fn fixed_response_output_keeps_product_flight_out_of_carrier_flight() {
    let mux_limits = MuxLimits::default();
    let (commands, _receivers) = reliable_path_command_channels(64);
    let mut startup = PathSnapshot::new(PathId(9), UnderlayProtocol::Tcp, 20.0, 500_000_000.0);
    startup.pacing_rate_bps = 500_000_000.0;
    startup.data_level_limit_bytes =
        data_level_service_window_bytes(startup, TrafficClass::Throughput, mux_limits).ceil()
            as u64;
    startup.confidence = 1.0;
    let output = ReliablePathStreamOutput::fixed_with_snapshot(startup, commands, mux_limits);
    let frame = Frame::StreamData {
        stream_id: StreamId(9),
        offset: 0,
        payload: Bytes::from(vec![0x33; PATH_OPEN_SCORE_BYTES]),
    };
    let ReliablePathStreamOutput::Fixed(fixed) = &output else {
        panic!("expected fixed output");
    };
    fixed.record_original_flight(&frame);

    let snapshot = output
        .send_path_snapshot(TrafficClass::Throughput, PATH_OPEN_SCORE_BYTES)
        .expect("fixed output exposes path model");

    assert_eq!(snapshot.bytes_in_flight, 0);
    assert_eq!(
        snapshot.data_level_bytes_in_flight,
        PATH_OPEN_SCORE_BYTES as u64
    );
    assert!(
        adaptive_reliable_relay_chunk_bytes(Some(snapshot), TrafficClass::Throughput, mux_limits)
            > MIN_RELIABLE_SERVICE_QUANTUM_PACKETS * TRANSPORT_MSS_BYTES,
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
        lane: TrafficClass::Throughput,
        underlay: UnderlayProtocol::Udp,
        max_frame_payload_bytes: reliable_relay_buffer_len(mux_limits),
        output: ReliablePathStreamOutput::fixed(
            UnderlayProtocol::Udp,
            PathId(0),
            commands,
            mux_limits,
        ),
        frames: frame_rx.into(),
    };
    let quantum = adaptive_reliable_relay_chunk_bytes(
        path_stream.send_path_snapshot(TrafficClass::Throughput, 1),
        TrafficClass::Throughput,
        mux_limits,
    );
    let mut send_stream = ReliableSendStream::new(stream_id, mux_limits);
    send_stream.update_max_offset(mux_limits.max_stream_window_bytes);
    let mut sender = ServerResponseSenderService::new(SessionId(17), stream_id);
    let queued_bytes = mux_limits.max_reliable_relay_chunk_bytes;
    sender.enqueue_data_for_lane(
        Bytes::from(vec![0x5a; queued_bytes]),
        TrafficClass::Throughput,
    );

    let dispatch = sender
        .dispatch_next(
            &path_stream,
            &mut send_stream,
            TrafficClass::Throughput,
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
async fn server_response_sender_promotes_remaining_data_at_dispatch() {
    let stream_id = StreamId(142);
    let mux_limits = MuxLimits::default();
    let (commands, mut receivers) = reliable_path_command_channels(16);
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: mux_limits.max_stream_window_bytes,
        lane: TrafficClass::Latency,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: reliable_relay_buffer_len(mux_limits),
        output: ReliablePathStreamOutput::fixed(
            UnderlayProtocol::Tcp,
            PathId(0),
            commands,
            mux_limits,
        ),
        frames: frame_rx.into(),
    };
    let mut send_stream = ReliableSendStream::new(stream_id, mux_limits);
    send_stream.update_max_offset(mux_limits.max_stream_window_bytes);
    let mut sender = ServerResponseSenderService::new(SessionId(142), stream_id);
    let latency_quantum = adaptive_reliable_relay_chunk_bytes(
        path_stream.send_path_snapshot(TrafficClass::Latency, 1),
        TrafficClass::Latency,
        mux_limits,
    );
    let promoted_remainder = latency_quantum * 2;
    sender.enqueue_data_for_lane(
        Bytes::from(vec![0x5a; latency_quantum * 3]),
        TrafficClass::Latency,
    );

    let first = sender
        .dispatch_next(
            &path_stream,
            &mut send_stream,
            TrafficClass::Latency,
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
            TrafficClass::Throughput,
            mux_limits,
        )
        .expect("remaining bytes adopt the promoted dispatch lane");
    assert_eq!(second.payload_bytes, promoted_remainder);
    assert!(
        matches!(
            recv_emitted_tcp_path_command(&mut receivers).await,
            Some(ReliablePathCommand::SendFrame(Frame::StreamData {
                offset,
                payload,
                ..
            })) if offset == latency_quantum as u64 && payload.len() == promoted_remainder
        ),
        "staged response bytes must adopt live bulk pacing without splitting OriginalData across carrier priority queues"
    );
}

#[tokio::test]
async fn server_response_sender_dispatches_final_fin_after_queued_data() {
    let stream_id = StreamId(49);
    let (commands, mut receivers) = reliable_path_command_channels(4);
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: 1024,
        lane: TrafficClass::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
        output: ReliablePathStreamOutput::fixed(
            UnderlayProtocol::Tcp,
            PathId(0),
            commands,
            MuxLimits::default(),
        ),
        frames: frame_rx.into(),
    };
    let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
    send_stream.update_max_offset(1024);
    let mut sender = ServerResponseSenderService::new(SessionId(49), stream_id);
    sender.enqueue_data_for_lane(Bytes::from_static(b"ordinary"), TrafficClass::Throughput);
    sender.enqueue_final_control_frame(Frame::StreamFin {
        stream_id,
        final_offset: b"ordinary".len() as u64,
    });

    let data_dispatch = sender
        .dispatch_next(
            &path_stream,
            &mut send_stream,
            TrafficClass::Throughput,
            MuxLimits::default(),
        )
        .expect("dispatch ordinary data first");
    assert_eq!(data_dispatch.lane, ReliableWorkClass::Data);

    let fin_dispatch = sender
        .dispatch_next(
            &path_stream,
            &mut send_stream,
            TrafficClass::Throughput,
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
async fn server_response_sender_keeps_data_queued_when_carrier_rejects() {
    let stream_id = StreamId(44);
    let (commands, receivers) = reliable_path_command_channels(1);
    drop(receivers);
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: 1024,
        lane: TrafficClass::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
        output: ReliablePathStreamOutput::fixed(
            UnderlayProtocol::Tcp,
            PathId(0),
            commands,
            MuxLimits::default(),
        ),
        frames: frame_rx.into(),
    };
    let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
    send_stream.update_max_offset(1024);
    let mut sender = ServerResponseSenderService::new(SessionId(9), stream_id);
    sender.enqueue_data_for_lane(Bytes::from_static(b"response"), TrafficClass::Throughput);

    assert_eq!(sender.bytes(), b"response".len());
    assert!(
        sender
            .dispatch_next(
                &path_stream,
                &mut send_stream,
                TrafficClass::Throughput,
                MuxLimits::default()
            )
            .is_err()
    );
    assert_eq!(sender.bytes(), b"response".len());
    assert_eq!(sender.data_bytes(), b"response".len());
    assert_eq!(send_stream.next_offset(), 0);
    assert_eq!(send_stream.reinjection_bytes(), 0);
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
        TrafficClass::Throughput,
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
        lane: TrafficClass::Throughput,
        underlay: UnderlayProtocol::Udp,
        max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
        output: ReliablePathStreamOutput::Switchable(binding),
        frames: frame_rx.into(),
    };
    let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
    send_stream.update_max_offset(1024);
    let mut sender = ServerResponseSenderService::new(session_id, stream_id);
    sender.enqueue_data_for_lane(Bytes::from_static(b"response"), TrafficClass::Throughput);

    let err = sender
        .dispatch_next(
            &path_stream,
            &mut send_stream,
            TrafficClass::Throughput,
            MuxLimits::default(),
        )
        .expect_err("detached switchable output should block, not close product stream");

    assert!(matches!(err, RuntimeError::SenderServiceBlocked));
    assert_eq!(sender.bytes(), b"response".len());
    assert_eq!(sender.data_bytes(), b"response".len());
    assert_eq!(send_stream.next_offset(), 0);
    assert_eq!(send_stream.reinjection_bytes(), 0);
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
                payload: Bytes::from_static(b"already queued"),
            },
            TrafficClass::Throughput,
        )
        .expect("prefill carrier data queue");
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: 1024,
        lane: TrafficClass::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
        output: ReliablePathStreamOutput::fixed(
            UnderlayProtocol::Tcp,
            PathId(0),
            commands,
            MuxLimits::default(),
        ),
        frames: frame_rx.into(),
    };
    let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
    send_stream.update_max_offset(1024);
    let mut sender = ServerResponseSenderService::new(SessionId(11), stream_id);
    sender.enqueue_data_for_lane(Bytes::from_static(b"later"), TrafficClass::Throughput);

    let err = sender
        .dispatch_next(
            &path_stream,
            &mut send_stream,
            TrafficClass::Throughput,
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
        TrafficClass::Throughput,
    );
    let (second_commands, mut second_receivers) = reliable_path_command_channels(4);
    assert_eq!(
        binding.attach(
            UnderlayProtocol::Udp,
            PathId(0),
            second_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::RejectedDuplicateLiveOutput
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: 1024,
        lane: TrafficClass::Throughput,
        underlay: UnderlayProtocol::Udp,
        max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
        output: ReliablePathStreamOutput::Switchable(binding),
        frames: frame_rx.into(),
    };
    let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
    send_stream.update_max_offset(1024);
    let mut sender = ServerResponseSenderService::new(session_id, stream_id);
    sender.enqueue_data_for_lane(
        Bytes::from_static(b"same-path-live"),
        TrafficClass::Throughput,
    );

    sender
        .dispatch_next(
            &path_stream,
            &mut send_stream,
            TrafficClass::Throughput,
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
        TrafficClass::Throughput,
    );
    drop(first_receivers);

    let (second_commands, mut second_receivers) = reliable_path_command_channels(4);
    assert_eq!(
        binding.attach(
            UnderlayProtocol::Udp,
            PathId(0),
            second_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::ReplacedClosedOutput
    );
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: 1024,
        lane: TrafficClass::Throughput,
        underlay: UnderlayProtocol::Udp,
        max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
        output: ReliablePathStreamOutput::Switchable(binding),
        frames: frame_rx.into(),
    };
    let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
    send_stream.update_max_offset(1024);
    let mut sender = ServerResponseSenderService::new(session_id, stream_id);
    sender.enqueue_data_for_lane(
        Bytes::from_static(b"same-path-closed"),
        TrafficClass::Throughput,
    );

    sender
        .dispatch_next(
            &path_stream,
            &mut send_stream,
            TrafficClass::Throughput,
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
        rate_valid_for_us: 60_000_000,
        rate_observed: true,
        srtt_us: 20_000,
        rttvar_us: 1_000,
        jitter_us: 1_000,
        delivery_rate_bps,
        pacing_rate_bps: delivery_rate_bps,
        pacing_rate_observed: true,
        loss_ppm: 0,
        ecn_ppm: 0,
        loss_observed: false,
        ecn_observed: false,
        bytes_in_flight_observed: false,
        queue_observed: false,
        bytes_in_flight: 0,
        queue_bytes: 0,
        inflight_limit_bytes: MAX_RELIABLE_SERVICE_QUANTUM_BYTES as u64,
        inflight_hi_bytes: MAX_RELIABLE_SERVICE_QUANTUM_BYTES as u64,
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
    let accepted = match registry
        .open_or_attach(ServerStreamOpenRequest {
            session_id,
            stream_id,
            target: target.clone(),
            initial_demand: StreamDemandHint::Throughput,
            attachment: ServerStreamPathAttachment {
                path_registration: old_path_registration.clone(),
                commands: old_commands,
                max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
            },
            mux_limits: MuxLimits::default(),
        })
        .expect("open response stream")
    {
        ServerReliableStreamOpen::New(accepted, _) => accepted,
        _ => panic!("expected a new response stream"),
    };
    let stream = accepted.stream();
    let ReliablePathStreamOutput::Switchable(binding) = &stream.output else {
        panic!("expected switchable output");
    };
    let binding = binding.clone();
    registry.record_local_path_metrics(
        server_carrier_identity(&old_path_registration),
        server_test_bulk_path_metrics(path_id, 200_000_000),
        false,
    );
    assert!(
        binding
            .sender_path_targets(TrafficClass::Throughput, MAX_RELIABLE_SERVICE_QUANTUM_BYTES)
            .first()
            .is_some_and(|entry| entry.observation.has_bulk_rate_evidence)
    );
    drop(old_receivers);
    let old_path_identity = server_carrier_identity(&old_path_registration);
    drop(old_path_registration);

    let new_path_registration =
        registry.register_carrier_path(session_id, UnderlayProtocol::Tcp, path_id);
    let (new_commands, _new_receivers) = reliable_path_command_channels(8);
    assert!(matches!(
        registry
            .open_or_attach(ServerStreamOpenRequest {
                session_id,
                stream_id,
                target: target.clone(),
                initial_demand: StreamDemandHint::Throughput,
                attachment: ServerStreamPathAttachment {
                    path_registration: new_path_registration.clone(),
                    commands: new_commands,
                    max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
                },
                mux_limits: MuxLimits::default(),
            },)
            .expect("replace closed response output"),
        ServerReliableStreamOpen::Existing(_)
    ));
    registry.record_local_path_metrics(
        old_path_identity,
        server_test_bulk_path_metrics(path_id, 300_000_000),
        false,
    );

    let targets =
        binding.sender_path_targets(TrafficClass::Throughput, MAX_RELIABLE_SERVICE_QUANTUM_BYTES);
    assert_eq!(targets.len(), 1);
    assert!(
        !targets[0].observation.has_bulk_rate_evidence,
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
        server_carrier_identity(&registration),
        PathMetrics {
            underlay: UnderlayProtocol::Udp,
            ..server_test_bulk_path_metrics(path_id, 200_000_000)
        },
        false,
    );
    assert_eq!(registry.management_snapshot().paths.len(), 1);

    drop(registration);
    assert_eq!(registry.management_snapshot().paths.len(), 1);

    registry.record_local_path_metrics(
        server_carrier_identity(&sampler_registration),
        PathMetrics {
            underlay: UnderlayProtocol::Udp,
            ..server_test_bulk_path_metrics(path_id, 300_000_000)
        },
        false,
    );
    drop(sampler_registration);
    assert!(
        registry.management_snapshot().paths.is_empty(),
        "cached evidence must retire when the last task lease ends"
    );
}

#[tokio::test]
async fn server_response_sender_uses_bounded_cross_path_reordering_after_path_loss() {
    let stream_id = StreamId(45);
    let (tcp_commands, _tcp_receivers) = reliable_path_command_channels(4);
    let tcp_commands_for_detach = tcp_commands.clone();
    let binding = ResponseStreamBinding::new(
        SessionId(10),
        UnderlayProtocol::Tcp,
        PathId(0),
        tcp_commands,
        TrafficClass::Throughput,
    );
    let (udp_commands, mut udp_receivers) = reliable_path_command_channels(4);
    binding.attach(
        UnderlayProtocol::Udp,
        PathId(1),
        udp_commands,
        TrafficClass::Throughput,
    );
    let lower_owner_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    binding.record_original_flight(
        lower_owner_key,
        &Frame::StreamData {
            stream_id,
            offset: 0,
            payload: Bytes::from_static(b"x"),
        },
    );
    binding.record_original_flight(
        lower_owner_key,
        &Frame::StreamData {
            stream_id,
            offset: 1,
            payload: Bytes::from_static(b"y"),
        },
    );
    binding.release_normalized_acked_ranges(&[OffsetRange { start: 1, end: 2 }]);
    binding.detach(lower_owner_key, &tcp_commands_for_detach);

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: 1024,
        lane: TrafficClass::Throughput,
        underlay: UnderlayProtocol::Udp,
        max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
        output: ReliablePathStreamOutput::Switchable(binding),
        frames: frame_rx.into(),
    };
    let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
    send_stream.update_max_offset(1024);
    send_stream
        .send_data(Bytes::from_static(b"xy"))
        .expect("advance response sender past the lower ACK-hole byte");
    let mut sender = ServerResponseSenderService::new(SessionId(10), stream_id);
    sender.enqueue_data_for_lane(Bytes::from_static(b"later"), TrafficClass::Throughput);

    let dispatched = sender
        .dispatch_next(
            &path_stream,
            &mut send_stream,
            TrafficClass::Throughput,
            MuxLimits::default(),
        )
        .expect("the live path may carry data within the receiver reorder budget");

    assert_eq!(
        dispatched.selected_path.map(|key| key.underlay),
        Some(UnderlayProtocol::Udp)
    );
    assert_eq!(sender.bytes(), 0);
    assert_eq!(sender.data_bytes(), 0);
    assert_eq!(send_stream.next_offset(), 2 + b"later".len() as u64);
    assert!(matches!(
        recv_emitted_tcp_path_command(&mut udp_receivers).await,
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            offset: 2,
            payload,
            ..
        })) if payload == Bytes::from_static(b"later")
    ));
}

#[tokio::test]
async fn server_response_sender_dispatches_reinjection_before_data() {
    let stream_id = StreamId(43);
    let (commands, mut receivers) = reliable_path_command_channels(4);
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: 1024,
        lane: TrafficClass::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
        output: ReliablePathStreamOutput::fixed(
            UnderlayProtocol::Tcp,
            PathId(0),
            commands,
            MuxLimits::default(),
        ),
        frames: frame_rx.into(),
    };
    let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
    send_stream.update_max_offset(1024);
    let mut sender = ServerResponseSenderService::new(SessionId(8), stream_id);
    sender.enqueue_data_for_lane(Bytes::from_static(b"ordinary"), TrafficClass::Throughput);
    assert!(
        sender
            .enqueue_reinjection_frame_with_priority(
                Frame::StreamData {
                    stream_id,
                    offset: 64,
                    payload: Bytes::from_static(b"reinjection"),
                },
                MuxLimits::default(),
                true,
            )
            .is_some()
    );

    let reinjection_dispatch = sender
        .dispatch_next(
            &path_stream,
            &mut send_stream,
            TrafficClass::Throughput,
            MuxLimits::default(),
        )
        .expect("dispatch reinjection");
    assert_eq!(reinjection_dispatch.lane, ReliableWorkClass::Reinjection);
    assert_eq!(send_stream.next_offset(), 0);
    assert!(matches!(
        recv_emitted_tcp_path_command(&mut receivers).await,
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            offset: 64,
            payload,
            ..
        })) if payload == Bytes::from_static(b"reinjection")
    ));

    let data_dispatch = sender
        .dispatch_next(
            &path_stream,
            &mut send_stream,
            TrafficClass::Throughput,
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
        max_reinjection_cache_chunks: 65_536,
        max_reorder_buffer_chunks: 65_536,
        max_retained_receive_ranges: 65_536,
        max_datagram_queue_bytes: 4 * 1024 * 1024,
        max_path_flight_bytes: 4 * 1024 * 1024,
        max_reliable_relay_chunk_bytes: 256 * 1024,
        tcp_path_heartbeat_interval: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_INTERVAL,
        tcp_path_heartbeat_timeout: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_TIMEOUT,
        quic_path_keep_alive_interval: crate::config::DEFAULT_QUIC_PATH_KEEP_ALIVE_INTERVAL,
        quic_path_idle_timeout: crate::config::DEFAULT_QUIC_PATH_IDLE_TIMEOUT,
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
        max_reinjection_cache_chunks: 65_536,
        max_reorder_buffer_chunks: 65_536,
        max_retained_receive_ranges: 65_536,
        max_datagram_queue_bytes: 4 * 1024 * 1024,
        max_path_flight_bytes: 4 * 1024 * 1024,
        max_reliable_relay_chunk_bytes: 256 * 1024,
        tcp_path_heartbeat_interval: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_INTERVAL,
        tcp_path_heartbeat_timeout: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_TIMEOUT,
        quic_path_keep_alive_interval: crate::config::DEFAULT_QUIC_PATH_KEEP_ALIVE_INTERVAL,
        quic_path_idle_timeout: crate::config::DEFAULT_QUIC_PATH_IDLE_TIMEOUT,
    };

    let stream_payload_queue = reliable_stream_frame_queue(mux_limits);
    let packet_payload_queue = reliable_stream_frame_queue_for_payload(mux_limits, 1200);

    assert_eq!(
        packet_payload_queue,
        mux_limits.max_reorder_bytes / 1200 + reliable_path_priority_headroom_frames()
    );
    assert!(packet_payload_queue > stream_payload_queue);
}

#[test]
fn reliable_path_and_tcp_command_queues_follow_carrier_backpressure_models() {
    let mux_limits = MuxLimits {
        max_payload_bytes: 1024 * 1024,
        max_ack_ranges: 256,
        max_streams: 1024,
        max_quic_concurrent_bidi_streams: 1024,
        max_stream_window_bytes: 16 * 1024 * 1024,
        max_repair_bytes: 16 * 1024 * 1024,
        max_reorder_bytes: 16 * 1024 * 1024,
        max_reinjection_cache_chunks: 65_536,
        max_reorder_buffer_chunks: 65_536,
        max_retained_receive_ranges: 65_536,
        max_datagram_queue_bytes: 4 * 1024 * 1024,
        max_path_flight_bytes: 4 * 1024 * 1024,
        max_reliable_relay_chunk_bytes: 256 * 1024,
        tcp_path_heartbeat_interval: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_INTERVAL,
        tcp_path_heartbeat_timeout: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_TIMEOUT,
        quic_path_keep_alive_interval: crate::config::DEFAULT_QUIC_PATH_KEEP_ALIVE_INTERVAL,
        quic_path_idle_timeout: crate::config::DEFAULT_QUIC_PATH_IDLE_TIMEOUT,
    };
    let frame_payload =
        reliable_relay_scheduler_quantum_cap(None, TrafficClass::Throughput, mux_limits)
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
        max_reinjection_cache_chunks: 65_536,
        max_reorder_buffer_chunks: 65_536,
        max_retained_receive_ranges: 65_536,
        max_datagram_queue_bytes: 4 * 1024 * 1024,
        max_path_flight_bytes: 4 * 1024 * 1024,
        max_reliable_relay_chunk_bytes: 256 * 1024,
        tcp_path_heartbeat_interval: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_INTERVAL,
        tcp_path_heartbeat_timeout: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_TIMEOUT,
        quic_path_keep_alive_interval: crate::config::DEFAULT_QUIC_PATH_KEEP_ALIVE_INTERVAL,
        quic_path_idle_timeout: crate::config::DEFAULT_QUIC_PATH_IDLE_TIMEOUT,
    };

    let stream_payload_queue = reliable_path_command_queue(mux_limits);
    let packet_payload_queue = reliable_path_command_queue_for_payload(mux_limits, 1200);

    let tcp_frame_payload =
        reliable_relay_scheduler_quantum_cap(None, TrafficClass::Throughput, mux_limits)
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
            >= reliable_relay_scheduler_quantum_cap(
                Some(path),
                TrafficClass::Throughput,
                mux_limits
            ) as u64
    );
    assert_eq!(high_bdp_threshold, high_bdp);

    let small_flow_bytes =
        (relay_lane_startup_chunk_bytes(TrafficClass::Throughput, mux_limits) as u64 / 2).max(1);
    let before = state.refresh(
        ReliableRelayFlowSignals::new(small_flow_bytes),
        Some(path),
        mux_limits,
    );
    assert_eq!(before.lane, TrafficClass::Latency);
    assert!(!before.promoted_to_throughput);

    let after = state.refresh(
        ReliableRelayFlowSignals::new(threshold),
        Some(path),
        mux_limits,
    );
    assert_eq!(after.lane, TrafficClass::Throughput);
    assert!(after.promoted_to_throughput);

    let steady = state.refresh(
        ReliableRelayFlowSignals::new(threshold.saturating_mul(2)),
        Some(path),
        mux_limits,
    );
    assert_eq!(steady.lane, TrafficClass::Throughput);
    assert!(!steady.promoted_to_throughput);
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
fn carrier_writer_run_batches_multiple_product_service_quanta() {
    let mux_limits = MuxLimits::default();
    let byte_budget = reliable_path_command_writer_run_budget_bytes(mux_limits);

    assert!(byte_budget > MAX_RELIABLE_SERVICE_QUANTUM_BYTES);
    assert!(byte_budget <= mux_limits.max_payload_bytes);
}

#[test]
fn path_writer_budget_counts_encoded_payload_and_variable_control_frames() {
    let payload = Bytes::from(vec![0x5a; MAX_RELIABLE_SERVICE_QUANTUM_BYTES]);
    let frames = [
        Frame::DatagramData {
            flow_id: DatagramFlowId(1),
            datagram_id: DatagramId(1),
            ttl_ms: 1_000,
            payload: payload.clone(),
        },
        Frame::PathProofData {
            path_id: PathId(1),
            proof_id: 1,
            payload: payload.clone(),
        },
        Frame::PathCapacityData {
            path_id: PathId(1),
            measurement_id: 1,
            payload,
        },
    ];

    for frame in frames {
        assert!(
            reliable_path_command_writer_run_bytes(&ReliablePathCommand::SendFrame(frame))
                >= MAX_RELIABLE_SERVICE_QUANTUM_BYTES + crate::protocol::codec::FRAME_HEADER_LEN,
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
                measurement_id: 9,
                payload: Bytes::from_static(b"carrier-capacity"),
            },
            TrafficClass::Throughput,
            b"carrier-capacity".len(),
        ),
        (
            Frame::PathCapacityFinish {
                path_id: PathId(3),
                measurement_id: 9,
                payload_bytes: 16,
            },
            TrafficClass::Throughput,
            0,
        ),
        (
            Frame::PathCapacityReceipt {
                path_id: PathId(3),
                measurement_id: 9,
                received_payload_bytes: 16,
            },
            TrafficClass::Control,
            0,
        ),
    ];
    let (commands, _receivers) = reliable_path_command_channels(1);
    for (frame, expected_lane, expected_pacing_bytes) in frames {
        assert!(reliable_path_frame_requires_capacity_command(&frame));
        assert_eq!(
            reliable_path_effective_frame_lane(&frame, TrafficClass::Throughput),
            expected_lane
        );
        assert_eq!(
            reliable_path_frame_pacing_bytes(&frame),
            expected_pacing_bytes
        );
        assert_eq!(reliable_stream_frame_extent(&frame), None);
        assert!(matches!(
            commands.try_enqueue_admitted_frame(frame, TrafficClass::Throughput),
            Err(RuntimeError::Protocol(_))
        ));
    }
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
        .receive_data(1024, Bytes::from_static(b"late"))
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
        .receive_data(0, Bytes::from_static(b"head"))
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

    let low_interval = reliable_stream_recv_progress_interval(Some(low_latency));
    let high_interval = reliable_stream_recv_progress_interval(Some(cross_continent));
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
        reliable_stream_recv_progress_interval(Some(cross_continent)) > Duration::from_millis(100)
    );
    let low_retry = sender_service_retry_delay(Some(low_latency));
    let high_retry = sender_service_retry_delay(Some(cross_continent));
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
    let window =
        reliable_stream_advertised_window_bytes(None, TrafficClass::Throughput, mux_limits);
    let step = reliable_stream_max_data_update_bytes(window, mux_limits);

    assert_eq!(step, 1024);
    assert!(progress.should_send_max_data(
        &recv_stream,
        None,
        TrafficClass::Throughput,
        mux_limits,
        false
    ));
    assert!(!progress.should_send_max_data(
        &recv_stream,
        None,
        TrafficClass::Throughput,
        mux_limits,
        false
    ));

    recv_stream
        .receive_data(0, Bytes::from(vec![0x11; 512]))
        .expect("half-step data");
    assert!(!progress.should_send_max_data(
        &recv_stream,
        None,
        TrafficClass::Throughput,
        mux_limits,
        false
    ));

    recv_stream
        .receive_data(512, Bytes::from(vec![0x22; 512]))
        .expect("full-step data");
    assert!(progress.should_send_max_data(
        &recv_stream,
        None,
        TrafficClass::Throughput,
        mux_limits,
        false
    ));
    assert!(progress.should_send_max_data(
        &recv_stream,
        None,
        TrafficClass::Throughput,
        mux_limits,
        true
    ));
}

#[test]
fn reliable_recv_progress_batches_bulk_acks_by_reinjection_release_cadence() {
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
    let ack_step = reliable_stream_ack_update_bytes(None, TrafficClass::Throughput, mux_limits);

    assert_eq!(ack_step, mux_limits.max_repair_bytes as u64 / 4);
    recv_stream
        .receive_data(0, Bytes::from(vec![0x11; 1024]))
        .expect("first data");
    assert!(progress.should_send_ack(
        &recv_stream,
        None,
        TrafficClass::Throughput,
        mux_limits,
        false,
    ));
    let first_generation = progress.ack_generation();
    assert!(progress.should_send_ack(
        &recv_stream,
        None,
        TrafficClass::Throughput,
        mux_limits,
        true,
    ));
    assert_eq!(
        progress.ack_generation(),
        first_generation,
        "forced publication of unchanged cumulative state reuses its fence"
    );

    recv_stream
        .receive_data(1024, Bytes::from(vec![0x22; 1024]))
        .expect("below ack step");
    assert!(!progress.should_send_ack(
        &recv_stream,
        None,
        TrafficClass::Throughput,
        mux_limits,
        false,
    ));

    recv_stream
        .receive_data(2048, Bytes::from(vec![0x33; ack_step as usize]))
        .expect("past ack step");
    assert!(progress.should_send_ack(
        &recv_stream,
        None,
        TrafficClass::Throughput,
        mux_limits,
        false,
    ));
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
        .receive_data(0, Bytes::from(vec![0x11; 1024]))
        .expect("first data");
    assert!(progress.should_send_ack(
        &recv_stream,
        None,
        TrafficClass::Throughput,
        mux_limits,
        false,
    ));

    recv_stream
        .receive_data(8192, Bytes::from(vec![0x22; 1024]))
        .expect("out-of-order data");
    assert!(progress.should_send_ack(
        &recv_stream,
        None,
        TrafficClass::Throughput,
        mux_limits,
        false,
    ));
}

#[test]
fn reliable_recv_progress_acks_reinjection_horizon_advancement() {
    let mux_limits = MuxLimits {
        max_ack_ranges: 16,
        max_stream_window_bytes: 256 * 1024,
        max_repair_bytes: 256 * 1024,
        max_reorder_bytes: 256 * 1024,
        max_path_flight_bytes: 256 * 1024,
        max_reliable_relay_chunk_bytes: 64 * 1024,
        ..MuxLimits::default()
    };
    let mut recv_stream = ReliableRecvStream::new(StreamId(25), mux_limits);
    let mut progress = ReliableRecvProgress::default();

    recv_stream
        .receive_data(0, Bytes::from(vec![0x11; 1024]))
        .expect("head");
    recv_stream
        .receive_data(8192, Bytes::from(vec![0x22; 1024]))
        .expect("first tail");
    assert!(progress.should_send_ack(
        &recv_stream,
        None,
        TrafficClass::Throughput,
        mux_limits,
        false,
    ));

    recv_stream
        .receive_data(9216, Bytes::from(vec![0x33; 1024]))
        .expect("small tail extension");
    assert!(
        !progress.should_send_ack(
            &recv_stream,
            None,
            TrafficClass::Throughput,
            mux_limits,
            false,
        ),
        "small same-range horizon movement should be batched"
    );

    let ack_step = reliable_stream_ack_update_bytes(None, TrafficClass::Throughput, mux_limits);
    assert!(
        ack_step > 1024,
        "test expects a bulk ACK step larger than one small chunk"
    );
    recv_stream
        .receive_data(10240, Bytes::from(vec![0x44; ack_step as usize]))
        .expect("meaningful tail extension");
    assert!(
        progress.should_send_ack(
            &recv_stream,
            None,
            TrafficClass::Throughput,
            mux_limits,
            false,
        ),
        "meaningful reinjection horizon advancement must be ACKed even when range count is unchanged"
    );
}

#[test]
fn reliable_relay_stall_watch_ignores_idle_streams_and_tracks_reinjectionable_work() {
    let mux_limits = MuxLimits::default();
    let mut send_stream = ReliableSendStream::new(StreamId(11), mux_limits);
    let mut recv_stream = ReliableRecvStream::new(StreamId(11), mux_limits);

    assert!(!reliable_relay_stall_watch_active(
        &send_stream,
        &recv_stream,
        false,
        TrafficClass::Latency,
        false,
        mux_limits
    ));
    assert!(!reliable_relay_stall_watch_active(
        &send_stream,
        &recv_stream,
        true,
        TrafficClass::Latency,
        false,
        mux_limits
    ));

    send_stream
        .send_data(Bytes::from_static(b"request"))
        .expect("request data");
    assert!(reliable_relay_stall_watch_active(
        &send_stream,
        &recv_stream,
        true,
        TrafficClass::Latency,
        false,
        mux_limits
    ));
    send_stream
        .apply_ack(&[crate::protocol::OffsetRange { start: 0, end: 7 }])
        .expect("ACK assigned stream bytes");
    assert!(!reliable_relay_stall_watch_active(
        &send_stream,
        &recv_stream,
        true,
        TrafficClass::Latency,
        false,
        mux_limits
    ));
    assert!(reliable_relay_stall_watch_active(
        &send_stream,
        &recv_stream,
        true,
        TrafficClass::Latency,
        true,
        mux_limits
    ));

    recv_stream
        .receive_data(0, Bytes::from_static(b"response"))
        .expect("response data");
    assert!(!reliable_relay_stall_watch_active(
        &send_stream,
        &recv_stream,
        true,
        TrafficClass::Latency,
        false,
        mux_limits
    ));
    assert!(reliable_relay_stall_watch_active(
        &send_stream,
        &recv_stream,
        true,
        TrafficClass::Throughput,
        false,
        mux_limits
    ));
    assert!(!reliable_relay_stall_watch_active(
        &send_stream,
        &recv_stream,
        false,
        TrafficClass::Throughput,
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
        .receive_data(current_offset, Bytes::from(vec![0u8; first_fill]))
        .expect("first sustained response data");
    let remaining = response_watch_bytes.saturating_sub(recv_stream.next_offset());
    if remaining > 0 {
        recv_stream
            .receive_data(
                recv_stream.next_offset(),
                Bytes::from(vec![0u8; remaining as usize]),
            )
            .expect("second sustained response data");
    }
    assert!(reliable_relay_stall_watch_active(
        &send_stream,
        &recv_stream,
        true,
        TrafficClass::Latency,
        false,
        mux_limits
    ));
}

#[test]
fn stream_ack_gap_reinjection_waits_for_persistent_gap_on_reliable_carriers() {
    let mux_limits = MuxLimits::default();
    let mut send_stream = ReliableSendStream::new(StreamId(31), mux_limits);
    send_stream
        .send_data(Bytes::from_static(b"aaaa"))
        .expect("first chunk");
    send_stream
        .send_data(Bytes::from_static(b"bbbb"))
        .expect("missing chunk");
    send_stream
        .send_data(Bytes::from_static(b"cccc"))
        .expect("later chunk");
    let ranges = [
        OffsetRange { start: 0, end: 4 },
        OffsetRange { start: 8, end: 12 },
    ];

    assert!(
        stream_ack_gap_reinjection_frames(&send_stream, &ranges, usize::MAX, true, false, false,)
            .is_empty(),
        "a single reliable carrier must not replay product bytes over itself"
    );
    assert!(
        stream_ack_gap_reinjection_frames(&send_stream, &ranges, usize::MAX, true, false, true,)
            .is_empty(),
        "a single reliable carrier owns ordinary packet-loss recovery"
    );
    assert!(
        stream_ack_gap_reinjection_frames(&send_stream, &ranges, usize::MAX, true, true, false,)
            .is_empty(),
        "fresh multipath ACK gaps wait for persistent product-hole evidence"
    );
    let persistent_gap_reinjections =
        stream_ack_gap_reinjection_frames(&send_stream, &ranges, usize::MAX, true, true, true);
    assert_eq!(
        persistent_gap_reinjections.len(),
        1,
        "multipath reinjection may reinject authoritative product gaps over another path"
    );
    assert!(matches!(
        &persistent_gap_reinjections[0],
        Frame::StreamData {
            offset: 4,
            payload,
            ..
        } if payload.as_ref() == b"bbbb"
    ));
    assert!(
        stream_ack_gap_reinjection_frames(&send_stream, &ranges, usize::MAX, false, false, false,)
            .is_empty(),
        "non-authoritative ACK snapshots must not infer missing holes"
    );
}

#[test]
fn ack_gap_reinjection_prefers_authoritative_gap_before_frontier_tail() {
    let mux_limits = MuxLimits::default();
    let mut send_stream = ReliableSendStream::new(StreamId(32), mux_limits);
    send_stream
        .send_data(Bytes::from_static(b"aaaa"))
        .expect("first chunk");
    send_stream
        .send_data(Bytes::from_static(b"bbbb"))
        .expect("second chunk");
    send_stream
        .send_data(Bytes::from_static(b"cccc"))
        .expect("third chunk");

    let ranges = [OffsetRange { start: 4, end: 12 }];
    let _ = send_stream.apply_ack(&ranges);
    let reinjections =
        stream_ack_gap_reinjection_frames(&send_stream, &ranges, usize::MAX, true, true, true);

    assert_eq!(reinjections.len(), 1);
    assert!(matches!(
        &reinjections[0],
        Frame::StreamData {
            offset: 0,
            payload,
            ..
        } if payload.as_ref() == b"aaaa"
    ));
}

#[test]
fn ack_gap_reinjection_ignores_contiguous_unacked_original_tail() {
    let mux_limits = MuxLimits::default();
    let mut send_stream = ReliableSendStream::new(StreamId(33), mux_limits);
    send_stream
        .send_data(Bytes::from_static(b"aaaa"))
        .expect("first chunk");
    send_stream
        .send_data(Bytes::from_static(b"bbbb"))
        .expect("second chunk");
    send_stream
        .send_data(Bytes::from_static(b"cccc"))
        .expect("third chunk");

    let ranges = [OffsetRange { start: 0, end: 4 }];
    let _ = send_stream.apply_ack(&ranges);
    let reinjections =
        stream_ack_gap_reinjection_frames(&send_stream, &ranges, 6, true, true, true);

    assert!(
        reinjections.is_empty(),
        "contiguous unacked owner tail is retained carrier flight, not ACK-gap reinjection"
    );
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
            TrafficClass::Latency,
            false,
            mux_limits,
        ),
        control_progress
    );

    let response_watch_bytes = reliable_relay_response_stall_watch_bytes(mux_limits);
    recv_stream
        .receive_data(0, Bytes::from(vec![0u8; response_watch_bytes as usize]))
        .expect("sustained response data");

    assert_eq!(
        reliable_relay_stall_progress_anchor(
            control_progress,
            last_delivery,
            last_delivery,
            &recv_stream,
            true,
            TrafficClass::Latency,
            false,
            mux_limits,
        ),
        last_delivery
    );

    let reinjection_progress = control_progress + Duration::from_secs(1);
    assert_eq!(
        reliable_relay_stall_progress_anchor(
            control_progress,
            last_delivery,
            reinjection_progress,
            &recv_stream,
            true,
            TrafficClass::Latency,
            false,
            mux_limits,
        ),
        reinjection_progress
    );
}

#[test]
fn tcp_receive_hole_reinjection_tracks_buffered_ordering_gap() {
    let mux_limits = MuxLimits::default();
    let mut recv_stream = ReliableRecvStream::new(StreamId(14), mux_limits);

    assert!(!reliable_relay_receive_hole_reinjection_active(
        &recv_stream,
        true
    ));
    recv_stream
        .receive_data(0, Bytes::from_static(b"head"))
        .expect("initial response data");
    assert!(!reliable_relay_receive_hole_reinjection_active(
        &recv_stream,
        true
    ));

    let out_of_order = recv_stream
        .receive_data(8, Bytes::from_static(b"tail"))
        .expect("out-of-order response data");
    assert!(out_of_order.delivered.is_empty());
    assert!(reliable_relay_receive_hole_reinjection_active(
        &recv_stream,
        true
    ));
    assert!(!reliable_relay_receive_hole_reinjection_active(
        &recv_stream,
        false
    ));

    let hole_fill = recv_stream
        .receive_data(4, Bytes::from_static(b"gap!"))
        .expect("hole fill response data");
    assert_eq!(hole_fill.delivered.len(), 2);
    assert!(!reliable_relay_receive_hole_reinjection_active(
        &recv_stream,
        true
    ));
}

#[test]
fn tcp_receive_hole_reinjection_deadline_is_progress_signal_not_path_victim_policy() {
    let now = Instant::now();
    let mut path = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 50.0, 100_000_000.0);
    path.jitter_ms = 5.0;
    path.carrier_inflight_limit_bytes = 1_000_000;

    let deadline = reliable_relay_receive_hole_reinjection_deadline(
        now,
        now - Duration::from_secs(1),
        Some(path),
    );

    assert!(
        deadline > tokio::time::Instant::from_std(now),
        "receive-hole handling schedules ACK/progress reinjection; path failure is owned by carrier/stall evidence"
    );
}

#[test]
fn observed_udp_datagram_loss_prefers_alternative_for_next_realtime_packet() {
    let resources = ResourceLimits::default();
    let context = ClientPathContext::new(
        vec![
            "quic://127.0.0.1:10000?initial-srtt-s=0.02&initial-rate-mbps=200"
                .parse()
                .expect("path"),
            "quic://127.0.0.1:10001?initial-srtt-s=0.03&initial-rate-mbps=200"
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
            loss_rate: Some(1.0),
            rate_sample: None,
            rate_sample_expires_at: None,
        },
    );

    assert_eq!(
        association.select_path_candidate(&candidates, &HashSet::new(), payload_bytes, ttl_ms),
        Some(1),
        "explicit carrier delivery loss should move the next realtime packet to the alternate path"
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
    let mut accepted = match registry
        .open_or_attach(ServerStreamOpenRequest {
            session_id: SessionId(1),
            stream_id: StreamId(7),
            target: target.clone(),
            initial_demand: StreamDemandHint::Latency,
            attachment: ServerStreamPathAttachment {
                path_registration: path_registration.clone(),
                commands,
                max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
            },
            mux_limits: MuxLimits::default(),
        })
        .expect("open stream")
    {
        ServerReliableStreamOpen::New(accepted, _) => accepted,
        ServerReliableStreamOpen::Existing(_) => panic!("expected new stream"),
        ServerReliableStreamOpen::DuplicateLiveIgnored => {
            panic!("new active stream must not be treated as duplicate")
        }
        ServerReliableStreamOpen::Rejected => panic!("active stream open should not be rejected"),
    };
    let mut stream = accepted.take_stream();
    assert_eq!(stream.current_lane(), TrafficClass::Latency);
    let ReliablePathStreamOutput::Switchable(binding) = &stream.output else {
        panic!("expected switchable binding");
    };
    let binding = binding.clone();

    stream.set_lane(TrafficClass::Throughput);

    assert_eq!(stream.current_lane(), TrafficClass::Throughput);
    assert_eq!(binding.lane(), TrafficClass::Throughput);
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
        .open_or_attach(ServerStreamOpenRequest {
            session_id,
            stream_id,
            target: target.clone(),
            initial_demand: StreamDemandHint::Throughput,
            attachment: ServerStreamPathAttachment {
                path_registration: first_path_registration.clone(),
                commands: first_commands,
                max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
            },
            mux_limits: MuxLimits::default(),
        })
        .expect("open stream");
    let _accepted = match opened {
        ServerReliableStreamOpen::New(accepted, _) => accepted,
        _ => panic!("expected new stream"),
    };

    let (duplicate_commands, _duplicate_rx) = reliable_path_command_channels(4);
    let duplicate = registry
        .open_or_attach(ServerStreamOpenRequest {
            session_id,
            stream_id,
            target: target.clone(),
            initial_demand: StreamDemandHint::Throughput,
            attachment: ServerStreamPathAttachment {
                path_registration: first_path_registration.clone(),
                commands: duplicate_commands,
                max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
            },
            mux_limits: MuxLimits::default(),
        })
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
    let path = "tcp://127.0.0.1:10000?initial-srtt-s=0.02&initial-rate-mbps=500"
        .parse::<PathSpec>()
        .expect("path spec");
    let initial_metrics =
        path_startup_metrics(&path, PathId(0), PathMetricDirection::ServerToClient);
    let path_registration = registry.register_carrier_path_with_local_properties(
        SessionId(1),
        UnderlayProtocol::Tcp,
        PathId(0),
        ServerLocalPathProperties {
            config_ordinal: 0,
            policy: path.metadata.policy,
            initial_metrics: Some(initial_metrics),
        },
    );
    let (commands, _rx) = reliable_path_command_channels(4);
    assert!(
        !initial_metrics.app_limited,
        "configured startup rate hints are advisory priors, not app-limited samples"
    );
    let accepted = match registry
        .open_or_attach(ServerStreamOpenRequest {
            session_id: SessionId(1),
            stream_id: StreamId(8),
            target: target.clone(),
            initial_demand: StreamDemandHint::Throughput,
            attachment: ServerStreamPathAttachment {
                path_registration: path_registration.clone(),
                commands,
                max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
            },
            mux_limits: MuxLimits::default(),
        })
        .expect("open stream")
    {
        ServerReliableStreamOpen::New(accepted, _) => accepted,
        ServerReliableStreamOpen::Existing(_) => panic!("expected new stream"),
        ServerReliableStreamOpen::DuplicateLiveIgnored => {
            panic!("new active stream must not be treated as duplicate")
        }
        ServerReliableStreamOpen::Rejected => panic!("active stream open should not be rejected"),
    };
    let stream = accepted.stream();
    let snapshot = stream
        .send_path_snapshot(TrafficClass::Throughput, 1)
        .expect("switchable output exposes seeded path model");

    assert_eq!(snapshot.delivery_rate_bps, default_path_rate_bps());
    assert_eq!(snapshot.srtt_ms, 20.0);
    assert!(
        adaptive_reliable_relay_chunk_bytes(
            Some(snapshot),
            TrafficClass::Throughput,
            MuxLimits::default(),
        ) > MIN_RELIABLE_SERVICE_QUANTUM_PACKETS * TRANSPORT_MSS_BYTES,
        "server response bytes keep the bulk feed quantum while startup metrics remain measurement-only rate hints"
    );

    let ReliablePathStreamOutput::Switchable(binding) = &stream.output else {
        panic!("expected switchable output");
    };
    binding.record_original_flight(
        CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        },
        &Frame::StreamData {
            stream_id: stream.stream_id,
            offset: 0,
            payload: Bytes::from(vec![0x22; PATH_OPEN_SCORE_BYTES]),
        },
    );
    let with_product_flight = stream
        .send_path_snapshot(TrafficClass::Throughput, PATH_OPEN_SCORE_BYTES)
        .expect("switchable output exposes path model");
    assert_eq!(with_product_flight.bytes_in_flight, 0);
    assert_eq!(
        with_product_flight.data_level_bytes_in_flight,
        PATH_OPEN_SCORE_BYTES as u64
    );
    assert!(
        adaptive_reliable_relay_chunk_bytes(
            Some(with_product_flight),
            TrafficClass::Throughput,
            MuxLimits::default(),
        ) > MIN_RELIABLE_SERVICE_QUANTUM_PACKETS * TRANSPORT_MSS_BYTES,
        "product flight is admission/reinjection state, not carrier queue pressure"
    );
}

#[test]
fn server_reliable_registry_opens_an_unknown_stream_on_any_live_path() {
    let registry = Arc::new(ServerReliableStreamRegistry::new(
        ResourceLimits::default().max_streams,
    ));
    let target = TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80)));
    let path_registration =
        registry.register_carrier_path(SessionId(1), UnderlayProtocol::Tcp, PathId(1));
    let (commands, _rx) = reliable_path_command_channels(4);
    let opened = registry
        .open_or_attach(ServerStreamOpenRequest {
            session_id: SessionId(1),
            stream_id: StreamId(99),
            target: target.clone(),
            initial_demand: StreamDemandHint::Throughput,
            attachment: ServerStreamPathAttachment {
                path_registration: path_registration.clone(),
                commands,
                max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
            },
            mux_limits: MuxLimits::default(),
        })
        .expect("neutral stream open should be handled");
    let _accepted = match opened {
        ServerReliableStreamOpen::New(accepted, _) => accepted,
        _ => panic!("unknown neutral stream must open on its first live path"),
    };
    assert_eq!(registry.management_snapshot().active_streams, 1);
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
        .open_or_attach(ServerStreamOpenRequest {
            session_id,
            stream_id,
            target: target.clone(),
            initial_demand: StreamDemandHint::Throughput,
            attachment: ServerStreamPathAttachment {
                path_registration: first_path_registration.clone(),
                commands,
                max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
            },
            mux_limits: MuxLimits::default(),
        })
        .expect("active open should be handled");
    let _accepted = match opened {
        ServerReliableStreamOpen::New(accepted, _) => accepted,
        _ => panic!("expected active stream open"),
    };
    registry.close(session_id, stream_id);

    let (commands, _rx) = reliable_path_command_channels(4);
    let replacement_path_registration =
        registry.register_carrier_path(session_id, UnderlayProtocol::Tcp, PathId(1));
    let reopened = registry
        .open_or_attach(ServerStreamOpenRequest {
            session_id,
            stream_id,
            target: target.clone(),
            initial_demand: StreamDemandHint::Throughput,
            attachment: ServerStreamPathAttachment {
                path_registration: replacement_path_registration.clone(),
                commands,
                max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
            },
            mux_limits: MuxLimits::default(),
        })
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
        TrafficClass::Latency,
    );
    let (udp_tx, mut udp_rx) = reliable_path_command_channels(4);
    binding.attach(
        UnderlayProtocol::Udp,
        PathId(0),
        udp_tx,
        TrafficClass::Throughput,
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

#[path = "runtime/tests/tests_datagram.rs"]
mod datagram;
#[path = "runtime/tests/tests_integration.rs"]
mod integration;
#[path = "runtime/tests/tests_peer_status_e2e.rs"]
mod peer_status_e2e;
#[path = "runtime/tests/tests_security.rs"]
mod security;
