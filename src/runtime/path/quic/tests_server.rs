use super::*;
use crate::config::{
    ClientSecurityConfig, MppPerformanceConfig, ResourceLimits, ServerSecurityConfig, SharedSecret,
};
use crate::product::{PrincipalId, TunL3AddressPlan, TunL3AllocationSpec, TunL3ServerSpec};
use crate::protocol::{
    CloseReason, DatagramFlowId, DatagramId, Frame, IpTunnelId, PathId, StreamDemandHint, StreamId,
    TargetAddr, UnderlayProtocol,
};
use crate::runtime::node::server::{ServerIdentityRuntime, new_identity_runtime};
use crate::runtime::path::commands::reliable_path_command_channels;
use crate::runtime::path::quic::client::{
    ClientUdpCarrierReconciliation, ClientUdpPathSessionRuntime,
};
use crate::runtime::path::quic::io::{
    UdpPathConnection, UdpPathEndpoint, UdpPathRecvStream, UdpPathSendStream,
    udp_path_finish_stream, udp_path_read_frame, udp_path_write_frame,
};
use crate::runtime::path::{
    ClientPathHealth, ClientPathState, ServerCarrierPeer, ServerDatagramOpenRequest,
    ServerDatagramRequest, ServerDatagramSendOutcome, ServerLocalPathProperties, ServerMppIngress,
    ServerStreamOpenOutcome, ServerStreamOpenRequest, ServerStreamPathAttachment,
};
use crate::runtime::peer_status::{PeerStatusBroker, PeerStatusSnapshotSource};
use crate::runtime::tun_l3::ServerIpTunnelService;
use crate::transport::encrypted::test_client_tls_config;
use crate::transport::{
    CarrierEndpoint, CarrierPathIdentity, CarrierSocket, CarrierSocketRequest, PathBinding,
    PathMetadata, SystemCarrierNetworkProvider,
};
use bytes::Bytes;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncReadExt;

struct ServerQuicChildDropProbe<F: FnOnce()> {
    callback: Option<F>,
}

impl<F: FnOnce()> ServerQuicChildDropProbe<F> {
    fn new(callback: F) -> Self {
        Self {
            callback: Some(callback),
        }
    }
}

impl<F: FnOnce()> Drop for ServerQuicChildDropProbe<F> {
    fn drop(&mut self) {
        if let Some(callback) = self.callback.take() {
            callback();
        }
    }
}

struct ServerControlFixture {
    context: ServerPathContext,
    client_send: UdpPathSendStream,
    client_recv: UdpPathRecvStream,
    server_send: UdpPathSendStream,
    server_recv: UdpPathRecvStream,
    client_connection: UdpPathConnection,
    _server_connection: UdpPathConnection,
    _client_endpoint: UdpPathEndpoint,
    _server_endpoint: UdpPathEndpoint,
    reliable_relay: Option<crate::runtime::relay::ServerReliableRelayService>,
}

impl ServerControlFixture {
    async fn open() -> Self {
        let shared_secret = SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec())
            .expect("test shared secret");
        let security = ServerSecurityConfig::for_test(shared_secret.clone());
        let client_security = ClientSecurityConfig::for_test(shared_secret);
        let ServerIdentityRuntime {
            paths: mut context,
            reliable_relay,
        } = new_identity_runtime(
            Vec::new(),
            crate::outbound::OutboundConfig::Direct,
            crate::config::DEFAULT_OUTBOUND_CONNECT_TIMEOUT,
            security,
            MppPerformanceConfig::default(),
            ResourceLimits::default(),
        );
        context.peer_status = PeerStatusBroker::new(false);

        let reserved =
            std::net::UdpSocket::bind("127.0.0.1:0").expect("reserve server QUIC address");
        let reserved_addr = reserved.local_addr().expect("reserved QUIC address");
        drop(reserved);
        let server_path = PathSpec {
            underlay: UnderlayProtocol::Udp,
            endpoint: CarrierEndpoint::single("127.0.0.1", reserved_addr.port())
                .expect("server carrier endpoint"),
            binding: PathBinding::default(),
            metadata: PathMetadata::default(),
        };
        let server_endpoint = UdpPathEndpoint::bind_server(&server_path, &context)
            .await
            .expect("bind server QUIC endpoint");
        let server_addr = server_endpoint.local_addr().expect("server QUIC address");
        let client_path = PathSpec {
            underlay: UnderlayProtocol::Udp,
            endpoint: CarrierEndpoint::single(server_addr.ip().to_string(), server_addr.port())
                .expect("client carrier endpoint"),
            binding: PathBinding::default(),
            metadata: PathMetadata::default(),
        };
        let session_id = SessionId(980);
        let client_runtime = ClientUdpPathSessionRuntime {
            paths: Arc::new(vec![client_path.clone()]),
            config_index: 0,
            path_index: 0,
            carrier_identity: CarrierPathIdentity {
                group_ordinal: 0,
                path_ordinal: 0,
            },
            session_id,
            candidate_selector: crate::transport::quic::QuicCandidateSelector::derive(
                client_security.credential.id().as_str(),
                client_security.credential.secret().as_bytes(),
            ),
            security: Arc::new(vec![client_security]),
            tls: Arc::new(vec![test_client_tls_config()]),
            codec_limits: context.codec_limits,
            mux_limits: context.mux_limits,
            stream_frame_queue: 8,
            state: ClientPathState::new(ClientPathHealth::new(Vec::new(), Vec::new())),
            carrier_network: Arc::new(SystemCarrierNetworkProvider),
            peer_status: PeerStatusBroker::new(false),
            peer_status_snapshot: PeerStatusSnapshotSource::new(|| Some(Vec::new())),
            authenticated_carriers: crate::runtime::path::AuthenticatedCarrierInventory::default(),
            ip_tunnels: crate::runtime::tun_l3::ClientIpTunnelHub::default(),
            reconciliation: ClientUdpCarrierReconciliation::new(),
        };
        let client_carrier = CarrierSocket::system(CarrierSocketRequest {
            path: &client_path,
            identity: client_runtime.carrier_identity,
            remote_addr: server_addr,
        })
        .expect("create client QUIC carrier");
        let client_endpoint = UdpPathEndpoint::bind_client(client_carrier, &client_runtime)
            .await
            .expect("bind client QUIC endpoint");
        let (client_connection, server_connection) =
            tokio::time::timeout(Duration::from_secs(5), async {
                tokio::join!(
                    client_endpoint.connect(server_addr),
                    server_endpoint.accept()
                )
            })
            .await
            .expect("QUIC connection timeout");
        let client_connection = client_connection.expect("connect QUIC carrier");
        let server_connection = server_connection.expect("accept QUIC carrier");
        let (client_send, client_recv) = client_connection
            .open_bi()
            .await
            .expect("open client control stream");
        let (server_send, server_recv) =
            tokio::time::timeout(Duration::from_secs(5), server_connection.accept_bi())
                .await
                .expect("server control stream timeout")
                .expect("accept server control stream");

        Self {
            context,
            client_send,
            client_recv,
            server_send,
            server_recv,
            client_connection,
            _server_connection: server_connection,
            _client_endpoint: client_endpoint,
            _server_endpoint: server_endpoint,
            reliable_relay,
        }
    }

    fn peer_status(&self) -> PeerStatusCarrier {
        self.context
            .peer_status
            .register_with_incoming(SessionId(980), true)
    }

    fn register_mixed_carriers(
        &self,
    ) -> (ServerCarrierPathRegistration, ServerCarrierPathRegistration) {
        let permit = crate::product::PrincipalPermit::for_test("test-peer");
        let control = self
            .context
            .reliable_streams
            .register_carrier_path_with_observed_peer(
                SessionId(980),
                UnderlayProtocol::Udp,
                PathId(0),
                ServerLocalPathProperties::default(),
                permit.clone(),
                ServerCarrierPeer::fixed(
                    "203.0.113.8:52000"
                        .parse()
                        .expect("authenticated QUIC test carrier peer"),
                ),
                Some(Arc::from("test-quic")),
            )
            .expect("register QUIC control carrier");
        let sibling = self
            .context
            .reliable_streams
            .register_carrier_path_with_observed_peer(
                SessionId(980),
                UnderlayProtocol::Tcp,
                PathId(1),
                ServerLocalPathProperties::default(),
                permit,
                ServerCarrierPeer::fixed(
                    "203.0.113.9:52001"
                        .parse()
                        .expect("authenticated TCP test carrier peer"),
                ),
                Some(Arc::from("test-tcp")),
            )
            .expect("register mixed-underlay sibling carrier");
        (control, sibling)
    }

    async fn open_datagram_flow(
        &self,
        target: SocketAddr,
    ) -> crate::runtime::path::AcceptedServerDatagramFlow {
        let (commands, _commands_rx) = reliable_path_command_channels(8);
        self.context
            .datagrams
            .as_ref()
            .expect("L4 server datagram service")
            .open(ServerDatagramOpenRequest {
                session_id: SessionId(980),
                principal_permit: crate::product::PrincipalPermit::for_test("test-peer"),
                flow_id: DatagramFlowId(981),
                target: TargetAddr::Ip(target),
                commands,
                ingress: ServerMppIngress::for_test(
                    SessionId(980),
                    "203.0.113.8:52000"
                        .parse()
                        .expect("authenticated test carrier peer"),
                    UnderlayProtocol::Udp,
                    Some("test-quic"),
                    PathId(0),
                    crate::model::path::CarrierPathInstanceId::from_raw(980),
                ),
            })
            .await
            .expect("open retained server datagram flow")
    }
}

fn server_tun_plan(security: &ServerSecurityConfig) -> TunL3AddressPlan {
    TunL3AddressPlan::compile(
        TunL3ServerSpec {
            interface_name: Some("test-tun".to_string()),
            ipv4_pool: Some("10.89.0.0/24".parse().expect("test IPv4 pool")),
            ipv4: Some(Ipv4Addr::new(10, 89, 0, 1)),
            ipv6_pool: None,
            ipv6: None,
            mtu: 1_500,
            allocations: vec![TunL3AllocationSpec {
                principal_id: PrincipalId::parse("test-peer").expect("test principal"),
                ipv4: Some(Ipv4Addr::new(10, 89, 0, 2)),
                ipv6: None,
                allowed_ips: Vec::new(),
            }],
        },
        &security.credential_authority,
    )
    .expect("compile server TUN plan")
}

async fn open_server_ip_tunnel_actor(
    fixture: &ServerControlFixture,
    path_registration: ServerCarrierPathRegistration,
    tunnel_id: IpTunnelId,
) -> (
    UdpPathSendStream,
    UdpPathRecvStream,
    tokio::task::JoinHandle<Result<(), RuntimeError>>,
) {
    let (mut client_send, mut client_recv) = fixture
        .client_connection
        .open_bi()
        .await
        .expect("open client IP tunnel stream");
    let (server_send, mut server_recv) = tokio::time::timeout(
        Duration::from_secs(5),
        fixture._server_connection.accept_bi(),
    )
    .await
    .expect("server IP tunnel stream timeout")
    .expect("accept server IP tunnel stream");
    let context = fixture.context.clone();
    udp_path_write_frame(
        &mut client_send,
        &Frame::OpenIpTunnel { tunnel_id },
        context.codec_limits,
    )
    .await
    .expect("write IP tunnel opener");
    assert_eq!(
        udp_path_read_frame(&mut server_recv, context.codec_limits)
            .await
            .expect("read IP tunnel opener"),
        Frame::OpenIpTunnel { tunnel_id }
    );
    let native_scope = crate::model::carrier_rate_authority::CarrierRateAuthorityScope::new(
        path_registration.path_instance_id(),
        crate::protocol::PathMetricDirection::ServerToClient,
    );
    let startup_rate_bps = path_registration
        .initial_metrics()
        .map_or(1_000_000, |metrics| metrics.delivery_rate_bps.max(1));
    let native_rate_authority = fixture
        ._server_connection
        .bind_native_rate_authority(
            native_scope,
            crate::transport::RateHint::BitsPerSecond(startup_rate_bps),
        )
        .await
        .expect("bind server test QUIC native authority");
    let _ = stage_current_server_native_scheduling_shape(
        &context,
        &path_registration,
        &native_rate_authority,
        native_scope,
    )
    .await
    .expect("stage server test QUIC native shape");
    let actor = tokio::spawn(handle_server_udp_ip_tunnel(
        server_send,
        server_recv,
        context.clone(),
        path_registration,
        tunnel_id,
        native_rate_authority,
    ));
    assert!(matches!(
        udp_path_read_frame(&mut client_recv, context.codec_limits)
            .await
            .expect("read IP tunnel ready"),
        Frame::IpTunnelReady {
            tunnel_id: ready_id,
            ..
        } if ready_id == tunnel_id
    ));
    (client_send, client_recv, actor)
}

#[tokio::test]
async fn server_quic_terminal_shutdown_joins_children_before_exact_registry_retirement() {
    let fixture = ServerControlFixture::open().await;
    let (path_registration, sibling_registration) = fixture.register_mixed_carriers();
    let session_id = path_registration.session_id();
    let underlay = path_registration.underlay();
    let path_id = path_registration.path_id();
    let path_instance_id = path_registration.path_instance_id();
    let sibling_instance_id = sibling_registration.path_instance_id();
    let connection = fixture._server_connection.clone();
    let observed_connection = connection.clone();
    let observed_context = fixture.context.clone();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (stopped_tx, stopped_rx) = tokio::sync::oneshot::channel();
    let mut streams = tokio::task::JoinSet::new();
    streams.spawn(async move {
        let _drop_probe = ServerQuicChildDropProbe::new(move || {
            let state = observed_context
                .reliable_streams
                .management_snapshot()
                .paths
                .into_iter()
                .find(|path| {
                    path.session_id == session_id
                        && path.underlay == underlay
                        && path.path_id == path_id
                        && path.path_instance_id == path_instance_id
                })
                .map(|path| path.state);
            let _ = stopped_tx.send((observed_connection.is_locally_closed(), state));
        });
        let _ = started_tx.send(());
        std::future::pending::<()>().await;
    });
    started_rx.await.expect("server QUIC child started");

    retire_server_udp_connection(&connection, &mut streams, &path_registration).await;

    let (native_closed, state_at_child_drop) =
        tokio::time::timeout(Duration::from_secs(1), stopped_rx)
            .await
            .expect("server QUIC child shutdown timeout")
            .expect("server QUIC child shutdown observation");
    assert!(
        native_closed,
        "native QUIC must close before child shutdown"
    );
    assert_eq!(
        state_at_child_drop,
        Some(PeerPathState::Draining),
        "the exact carrier must remain registered as Draining until every child has joined",
    );
    let paths = fixture.context.reliable_streams.management_snapshot().paths;
    assert!(
        paths
            .iter()
            .all(|path| path.path_instance_id != path_instance_id),
        "terminal shutdown must retire the exact QUIC carrier",
    );
    assert!(
        paths
            .iter()
            .any(|path| path.path_instance_id == sibling_instance_id),
        "exact QUIC retirement must preserve the sibling carrier",
    );
}

#[tokio::test]
async fn server_quic_session_close_is_terminal_to_the_carrying_connection() {
    let mut fixture = ServerControlFixture::open().await;
    let peer_status = fixture.peer_status();
    let actor = tokio::spawn(run_server_udp_control_stream(
        fixture.server_send,
        fixture.server_recv,
        peer_status,
        fixture.context.clone(),
        SessionId(980),
    ));

    udp_path_write_frame(
        &mut fixture.client_send,
        &Frame::SessionClose {
            reason: CloseReason::Normal,
        },
        fixture.context.codec_limits,
    )
    .await
    .expect("write explicit session close");

    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(5), actor)
            .await
            .expect("server control actor timeout")
            .expect("server control actor join"),
        Err(RuntimeError::RemoteClosed(CloseReason::Normal))
    ));
}

#[tokio::test]
async fn server_quic_clean_legacy_control_eof_does_not_retire_the_session() {
    let mut fixture = ServerControlFixture::open().await;
    let (_control_registration, _sibling_registration) = fixture.register_mixed_carriers();
    let target = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind UDP target");
    let target_addr = target.local_addr().expect("UDP target address");
    let flow = fixture.open_datagram_flow(target_addr).await;
    let peer_status = fixture.peer_status();
    let actor = tokio::spawn(run_server_udp_control_stream(
        fixture.server_send,
        fixture.server_recv,
        peer_status,
        fixture.context.clone(),
        SessionId(980),
    ));

    udp_path_finish_stream(&mut fixture.client_send)
        .await
        .expect("finish legacy control stream");
    tokio::time::timeout(Duration::from_secs(5), actor)
        .await
        .expect("server control actor timeout")
        .expect("server control actor join")
        .expect("clean legacy control EOF remains non-terminal");
    assert!(!fixture.client_connection.is_closed());
    assert_eq!(
        fixture
            .context
            .reliable_streams
            .management_snapshot()
            .paths
            .len(),
        2,
        "clean control EOF must preserve both carrier registrations",
    );
    assert_eq!(
        flow.send(ServerDatagramRequest {
            datagram_id: DatagramId(0),
            ttl_ms: 1_000,
            payload: Bytes::from_static(b"after-clean-control-eof"),
        })
        .await
        .expect("admit post-EOF datagram"),
        ServerDatagramSendOutcome::Accepted,
    );
    let mut target_payload = [0_u8; 64];
    let (received, _) = tokio::time::timeout(
        Duration::from_secs(1),
        target.recv_from(&mut target_payload),
    )
    .await
    .expect("post-EOF target datagram timeout")
    .expect("receive post-EOF target datagram");
    assert_eq!(&target_payload[..received], b"after-clean-control-eof");
    let _ = &mut fixture.client_recv;
}

#[tokio::test]
async fn server_quic_session_close_retires_sibling_carrier_and_retained_datagram_flow() {
    let mut fixture = ServerControlFixture::open().await;
    let (control_registration, _sibling_registration) = fixture.register_mixed_carriers();
    let unrelated_registration = fixture
        .context
        .reliable_streams
        .register_carrier_path(
            SessionId(982),
            UnderlayProtocol::Tcp,
            PathId(2),
            ServerLocalPathProperties::default(),
            crate::product::PrincipalPermit::for_test("test-peer"),
        )
        .expect("register unrelated session carrier");
    let target = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind UDP target");
    let target_addr = target.local_addr().expect("UDP target address");
    let flow = fixture.open_datagram_flow(target_addr).await;
    let reliable_target = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind reliable target");
    let reliable_target_addr = reliable_target
        .local_addr()
        .expect("reliable target address");
    let relay_task = tokio::spawn(
        fixture
            .reliable_relay
            .take()
            .expect("L4 reliable relay service")
            .run(),
    );
    let (reliable_commands, _reliable_commands_rx) = reliable_path_command_channels(8);
    assert!(matches!(
        fixture
            .context
            .reliable_streams
            .open_or_attach(ServerStreamOpenRequest {
                session_id: SessionId(980),
                stream_id: StreamId(982),
                target: TargetAddr::Ip(reliable_target_addr),
                initial_demand: StreamDemandHint::Latency,
                return_plan: Default::default(),
                attachment: ServerStreamPathAttachment {
                    path_registration: control_registration.clone(),
                    commands: reliable_commands,
                    max_frame_payload_bytes: fixture.context.mux_limits.max_payload_bytes,
                },
                mux_limits: fixture.context.mux_limits,
            })
            .await
            .expect("open reliable target flow"),
        ServerStreamOpenOutcome::New(_)
    ));
    let (mut reliable_target_stream, _) =
        tokio::time::timeout(Duration::from_secs(5), reliable_target.accept())
            .await
            .expect("reliable target accept timeout")
            .expect("accept reliable target flow");

    assert_eq!(
        flow.send(ServerDatagramRequest {
            datagram_id: DatagramId(0),
            ttl_ms: 1_000,
            payload: Bytes::from_static(b"before-session-close"),
        })
        .await
        .expect("admit pre-close datagram"),
        ServerDatagramSendOutcome::Accepted,
    );
    let mut target_payload = [0_u8; 64];
    let (received, _) = tokio::time::timeout(
        Duration::from_secs(1),
        target.recv_from(&mut target_payload),
    )
    .await
    .expect("pre-close target datagram timeout")
    .expect("receive pre-close target datagram");
    assert_eq!(&target_payload[..received], b"before-session-close");

    let peer_status = fixture.peer_status();
    let actor = tokio::spawn(run_server_udp_control_stream(
        fixture.server_send,
        fixture.server_recv,
        peer_status,
        fixture.context.clone(),
        SessionId(980),
    ));
    udp_path_write_frame(
        &mut fixture.client_send,
        &Frame::SessionClose {
            reason: CloseReason::Normal,
        },
        fixture.context.codec_limits,
    )
    .await
    .expect("write explicit session close");
    let _control_result = tokio::time::timeout(Duration::from_secs(5), actor)
        .await
        .expect("server control actor timeout")
        .expect("server control actor join");

    let remaining_snapshot = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let snapshot = fixture.context.reliable_streams.management_snapshot();
            if !snapshot
                .paths
                .iter()
                .any(|path| path.session_id == SessionId(980))
                && snapshot.active_streams == 0
            {
                break snapshot;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("complete session registry retirement timeout");
    let remaining_paths = remaining_snapshot.paths;
    let post_close = flow
        .send(ServerDatagramRequest {
            datagram_id: DatagramId(1),
            ttl_ms: 1_000,
            payload: Bytes::from_static(b"after-session-close"),
        })
        .await
        .expect("session retirement reports a closed flow outcome");
    let leaked_to_target = tokio::time::timeout(
        Duration::from_millis(100),
        target.recv_from(&mut target_payload),
    )
    .await
    .is_ok();
    let mut reliable_payload = [0_u8; 1];
    let reliable_closed = match tokio::time::timeout(
        Duration::from_secs(1),
        reliable_target_stream.read(&mut reliable_payload),
    )
    .await
    {
        Ok(Ok(0)) | Ok(Err(_)) => true,
        Ok(Ok(_)) | Err(_) => false,
    };
    let same_id_rejoin = fixture.context.reliable_streams.register_carrier_path(
        SessionId(980),
        UnderlayProtocol::Tcp,
        PathId(3),
        ServerLocalPathProperties::default(),
        crate::product::PrincipalPermit::for_test("test-peer"),
    );
    let retired_paths = remaining_paths
        .iter()
        .filter(|path| path.session_id == SessionId(980))
        .count();
    let unrelated_live = remaining_paths.iter().any(|path| {
        path.session_id == SessionId(982)
            && path.path_instance_id == unrelated_registration.path_instance_id()
    });

    assert!(
        retired_paths == 0
            && unrelated_live
            && post_close == ServerDatagramSendOutcome::Closed
            && !leaked_to_target
            && reliable_closed
            && matches!(
                same_id_rejoin,
                Err(RuntimeError::RemoteClosed(CloseReason::Normal))
            ),
        "SESSION_CLOSE left {retired_paths} matching carriers, unrelated_live={unrelated_live}, returned {post_close:?}, target_leak={leaked_to_target}, reliable_closed={reliable_closed}, same_id_rejoin={same_id_rejoin:?}",
    );
    relay_task.abort();
}

#[tokio::test]
async fn server_quic_ip_tunnel_session_close_retires_the_complete_session() {
    let mut fixture = ServerControlFixture::open().await;
    let (ip_tunnels, _device) = ServerIpTunnelService::build(
        server_tun_plan(&fixture.context.security),
        fixture.context.reliable_streams.clone(),
        4,
        16 * 1_500,
        fixture.context.session_retention_timeout,
    );
    fixture.context.ip_tunnels = Some(ip_tunnels);
    let (tunnel_path, _sibling) = fixture.register_mixed_carriers();
    let unrelated = fixture
        .context
        .reliable_streams
        .register_carrier_path(
            SessionId(983),
            UnderlayProtocol::Tcp,
            PathId(3),
            ServerLocalPathProperties::default(),
            crate::product::PrincipalPermit::for_test("test-peer"),
        )
        .expect("register unrelated carrier");
    let (mut client_send, _client_recv, actor) =
        open_server_ip_tunnel_actor(&fixture, tunnel_path, IpTunnelId(7)).await;

    udp_path_write_frame(
        &mut client_send,
        &Frame::SessionClose {
            reason: CloseReason::PolicyRejected,
        },
        fixture.context.codec_limits,
    )
    .await
    .expect("write IP tunnel SESSION_CLOSE");

    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(5), actor)
            .await
            .expect("server IP tunnel actor timeout")
            .expect("server IP tunnel actor join"),
        Err(RuntimeError::RemoteClosed(CloseReason::PolicyRejected))
    ));
    let paths = fixture.context.reliable_streams.management_snapshot().paths;
    assert!(paths.iter().all(|path| path.session_id != SessionId(980)));
    assert!(paths.iter().any(|path| {
        path.session_id == SessionId(983) && path.path_instance_id == unrelated.path_instance_id()
    }));
}

#[tokio::test]
async fn server_quic_ip_tunnel_local_close_and_clean_eof_preserve_the_session() {
    let mut fixture = ServerControlFixture::open().await;
    let (ip_tunnels, _device) = ServerIpTunnelService::build(
        server_tun_plan(&fixture.context.security),
        fixture.context.reliable_streams.clone(),
        4,
        16 * 1_500,
        fixture.context.session_retention_timeout,
    );
    fixture.context.ip_tunnels = Some(ip_tunnels);
    let (tunnel_path, _sibling) = fixture.register_mixed_carriers();

    let (mut local_close_send, _local_close_recv, local_close_actor) =
        open_server_ip_tunnel_actor(&fixture, tunnel_path.clone(), IpTunnelId(8)).await;
    udp_path_write_frame(
        &mut local_close_send,
        &Frame::IpTunnelClose {
            tunnel_id: IpTunnelId(8),
            reason: CloseReason::Normal,
        },
        fixture.context.codec_limits,
    )
    .await
    .expect("write attachment-local IP_TUNNEL_CLOSE");
    tokio::time::timeout(Duration::from_secs(5), local_close_actor)
        .await
        .expect("local close actor timeout")
        .expect("local close actor join")
        .expect("IP_TUNNEL_CLOSE remains attachment-local");
    assert_eq!(
        fixture
            .context
            .reliable_streams
            .management_snapshot()
            .paths
            .len(),
        2
    );
    assert!(!tunnel_path.session_retirement().is_retired());

    let (mut eof_send, _eof_recv, eof_actor) =
        open_server_ip_tunnel_actor(&fixture, tunnel_path.clone(), IpTunnelId(9)).await;
    udp_path_finish_stream(&mut eof_send)
        .await
        .expect("finish IP tunnel request stream");
    tokio::time::timeout(Duration::from_secs(5), eof_actor)
        .await
        .expect("clean EOF actor timeout")
        .expect("clean EOF actor join")
        .expect("clean IP tunnel EOF remains attachment-local");
    assert_eq!(
        fixture
            .context
            .reliable_streams
            .management_snapshot()
            .paths
            .len(),
        2
    );
    assert!(!tunnel_path.session_retirement().is_retired());
}
