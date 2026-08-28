use super::*;
use crate::config::{
    AppConfig, CommandConfig, DEFAULT_OUTBOUND_CONNECT_TIMEOUT, DnsPolicyConfig,
    LocalIngressConfig, ManagementConfig, NodeConfig, ServerSecurityConfig, ServiceConfig,
    SessionConfig, SharedSecret,
};
use crate::ingress::IngressConfig;
use crate::ingress::tun::{ManagedVpnConfig, ManagedVpnPlatformConfig, TunHostConfig, TunL4Config};
use crate::outbound::OutboundConfig;
use crate::performance::{MppPerformanceConfig, ResourceLimits};
use crate::product::{
    DnsPlanId, DnsPlanSpec, DnsPolicySpec, DnsUpstreamEndpoint, DnsUpstreamId, DnsUpstreamSpec,
    ProductAdmissionConfig,
};
use crate::runtime::path::ServerLocalPath;
use crate::transport::{
    CarrierNetworkProvider, CarrierPathIdentity, CarrierResolutionFuture, CarrierResolutionRequest,
    CarrierSocket, CarrierSocketRequest, PathSpec,
};
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::oneshot;

struct DropNotice(Option<oneshot::Sender<()>>);

impl Drop for DropNotice {
    fn drop(&mut self) {
        if let Some(notice) = self.0.take() {
            let _ = notice.send(());
        }
    }
}

struct AuthoritativeCarrierNetworkProvider {
    address: SocketAddr,
}

impl CarrierNetworkProvider for AuthoritativeCarrierNetworkProvider {
    fn resolve<'a>(
        &'a self,
        _request: CarrierResolutionRequest<'a>,
    ) -> CarrierResolutionFuture<'a> {
        Box::pin(async move { Ok(vec![self.address]) })
    }

    fn create_socket(&self, request: CarrierSocketRequest<'_>) -> io::Result<CarrierSocket> {
        CarrierSocket::system(request)
    }
}

fn tun_host_runtime_config(host: TunHostConfig) -> AppConfig {
    AppConfig {
        logging: crate::config::LoggingConfig {
            level: crate::config::LogLevel::Off,
            ..crate::config::LoggingConfig::default()
        },
        check_config: false,
        service: ServiceConfig::default(),
        session: SessionConfig::default(),
        flow: crate::config::ProductFlowConfig::default(),
        resources: ResourceLimits::default(),
        admission: ProductAdmissionConfig::default(),
        management: ManagementConfig::default(),
        command: CommandConfig::Node(NodeConfig {
            forwarding_mode: crate::config::ForwardingMode::L4,
            outbounds: Vec::new(),
            gateway_balancers: Vec::new(),
            local_ingresses: vec![LocalIngressConfig {
                name: "host-tun".to_string(),
                config: IngressConfig::TunL4(TunL4Config {
                    host,
                    ..TunL4Config::default()
                }),
            }],
            tun_l3_ingresses: Vec::new(),
            product_policy: None,
            dns_policy: DnsPolicyConfig::default(),
            servers: Vec::new(),
        }),
    }
}

fn literal_dns_policy() -> DnsPolicyConfig {
    let upstream = DnsUpstreamId::parse("literal").expect("upstream ID");
    let plan = DnsPlanId::parse("default").expect("plan ID");
    DnsPolicyConfig {
        generation: 2,
        spec: DnsPolicySpec {
            upstreams: vec![DnsUpstreamSpec::direct(
                upstream.clone(),
                DnsUpstreamEndpoint::Udp {
                    bootstrap: "192.0.2.53:53".parse().expect("bootstrap"),
                },
            )],
            outbound_capabilities: Vec::new(),
            plans: vec![DnsPlanSpec::new(plan.clone(), vec![upstream])],
            rules: Vec::new(),
            override_records: Vec::new(),
            synthetic_captures: Vec::new(),
            default_plan: plan,
        },
    }
}

fn proxy_only_system_dns_config(listen: SocketAddr) -> AppConfig {
    crate::config::load_config_toml_str(&format!(
        r#"
[logging]
level = "off"

[[inbounds]]
name = "local-mixed"
protocol = "mixed"
listen = ["{listen}"]

[[outbounds]]
name = "direct"
protocol = "direct"

[routing]
[[routing.rules]]
name = "default"
outbound = "direct"
"#
    ))
    .expect("proxy-only system-DNS config")
}

#[tokio::test]
async fn public_vpn_runtime_entry_contract_fails_closed_before_host_io() {
    let system_dns = tun_host_runtime_config(TunHostConfig::External);
    assert!(require_external_tun_host(&system_dns).is_ok());
    assert!(matches!(
        require_protectable_vpn_dns(&system_dns),
        Err(RuntimeError::ProductPolicy(message)) if message == VPN_HOST_SYSTEM_DNS_ERROR
    ));
    let runtime_error = run_with_vpn_host_providers(
        system_dns.clone(),
        Arc::new(SystemPacketDeviceProvider),
        Arc::new(AuthoritativeCarrierNetworkProvider {
            address: "192.0.2.254:443".parse().expect("host address"),
        }),
        Arc::new(PanicHostSocketProtector),
    )
    .await
    .expect_err("system DNS must fail before host adapters start");
    assert!(matches!(
        runtime_error,
        RuntimeError::ProductPolicy(message) if message == VPN_HOST_SYSTEM_DNS_ERROR
    ));
    let controlled_error = run_with_vpn_host_providers_and_control(
        system_dns.clone(),
        Arc::new(SystemPacketDeviceProvider),
        Arc::new(AuthoritativeCarrierNetworkProvider {
            address: "192.0.2.254:443".parse().expect("host address"),
        }),
        Arc::new(PanicHostSocketProtector),
        RuntimeHostControl::for_config(&system_dns),
    )
    .await
    .expect_err("controlled protected runtime must retain strict system-DNS rejection");
    assert!(matches!(
        controlled_error,
        RuntimeError::ProductPolicy(message) if message == VPN_HOST_SYSTEM_DNS_ERROR
    ));

    let mut literal_dns = tun_host_runtime_config(TunHostConfig::External);
    let CommandConfig::Node(node) = &mut literal_dns.command;
    node.dns_policy = literal_dns_policy();
    assert!(require_protectable_vpn_dns(&literal_dns).is_ok());

    let managed = TunHostConfig::Managed(ManagedVpnConfig {
        route_mode: crate::platform::RouteMode::Full,
        excludes: Vec::new(),
        local_lan: false,
        dns_capture_servers: vec!["10.88.0.53".parse().expect("DNS capture")],
        platform: ManagedVpnPlatformConfig::default(),
    });
    assert!(matches!(
        require_external_tun_host(&tun_host_runtime_config(managed)),
        Err(RuntimeError::Protocol(PUBLIC_RUNTIME_MANAGED_VPN_ERROR))
    ));
}

#[tokio::test]
async fn unprotected_controlled_host_runtime_accepts_system_dns_and_preserves_guards() {
    let port_reservation = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve port");
    let listen = port_reservation.local_addr().expect("reserved address");
    drop(port_reservation);
    let config = proxy_only_system_dns_config(listen);

    assert!(matches!(
        require_protectable_vpn_dns(&config),
        Err(RuntimeError::ProductPolicy(message)) if message == VPN_HOST_SYSTEM_DNS_ERROR
    ));

    let control = RuntimeHostControl::for_config(&config);
    let runtime_control = control.clone();
    let runtime = tokio::spawn(run_with_all_host_providers_and_control(
        config.clone(),
        Arc::new(SystemPacketDeviceProvider),
        Arc::new(SystemCarrierNetworkProvider),
        Arc::new(SystemNativeSocketConfigurator),
        runtime_control,
    ));
    tokio::time::timeout(Duration::from_secs(2), control.wait_until_ready())
        .await
        .expect("proxy listener readiness deadline")
        .expect("proxy listener readiness");
    control.request_shutdown();
    tokio::time::timeout(Duration::from_secs(2), runtime)
        .await
        .expect("proxy runtime shutdown deadline")
        .expect("proxy runtime task")
        .expect("proxy runtime shutdown");

    let mut mismatched = config.clone();
    mismatched.resources.max_streams = 1;
    let mismatch_error = run_with_all_host_providers_and_control(
        mismatched,
        Arc::new(SystemPacketDeviceProvider),
        Arc::new(SystemCarrierNetworkProvider),
        Arc::new(SystemNativeSocketConfigurator),
        RuntimeHostControl::for_config(&config),
    )
    .await
    .expect_err("mismatched host control must fail closed");
    assert!(matches!(
        mismatch_error,
        RuntimeError::Protocol(
            "runtime host control was created for a different resource envelope"
        )
    ));

    let managed = TunHostConfig::Managed(ManagedVpnConfig {
        route_mode: crate::platform::RouteMode::Full,
        excludes: Vec::new(),
        local_lan: false,
        dns_capture_servers: vec!["10.88.0.53".parse().expect("DNS capture")],
        platform: ManagedVpnPlatformConfig::default(),
    });
    let managed = tun_host_runtime_config(managed);
    let managed_error = run_with_all_host_providers_and_control(
        managed.clone(),
        Arc::new(SystemPacketDeviceProvider),
        Arc::new(SystemCarrierNetworkProvider),
        Arc::new(SystemNativeSocketConfigurator),
        RuntimeHostControl::for_config(&managed),
    )
    .await
    .expect_err("managed TUN must remain application-lifecycle owned");
    assert!(matches!(
        managed_error,
        RuntimeError::Protocol(PUBLIC_RUNTIME_MANAGED_VPN_ERROR)
    ));
}

struct PanicHostSocketProtector;

impl HostSocketProtector for PanicHostSocketProtector {
    fn protect(
        &self,
        _socket: crate::transport::HostSocketHandle<'_>,
        _request: crate::transport::HostSocketProtectionRequest,
    ) -> io::Result<()> {
        panic!("system DNS validation must finish before socket protection")
    }
}

#[tokio::test]
async fn product_dns_carrier_resolution_is_generation_scoped_and_literal_safe() {
    let socket_provider: Arc<dyn CarrierNetworkProvider> =
        Arc::new(AuthoritativeCarrierNetworkProvider {
            address: "192.0.2.254:443".parse().expect("authoritative address"),
        });
    let network =
        GenerationCarrierNetwork::new(socket_provider, CarrierResolutionAuthority::ProductDns);
    let domain_path = "tcp://carrier.product.test:440-450"
        .parse::<PathSpec>()
        .expect("domain carrier");
    let literal_path = "quic://192.0.2.10:8440-8450"
        .parse::<PathSpec>()
        .expect("literal carrier");
    let identity = CarrierPathIdentity {
        group_ordinal: 0,
        path_ordinal: 0,
    };

    let before_install = network
        .provider
        .resolve(CarrierResolutionRequest {
            path: &domain_path,
            identity,
            remote_port: 447,
        })
        .await
        .expect_err("domain resolution must fail before Product DNS installation");
    assert_eq!(before_install.kind(), io::ErrorKind::NotConnected);
    assert_eq!(
        network
            .provider
            .resolve(CarrierResolutionRequest {
                path: &literal_path,
                identity,
                remote_port: 8447,
            })
            .await
            .expect("literal carrier remains DNS-independent"),
        vec!["192.0.2.10:8447".parse().expect("literal address")]
    );

    network
        .install_product_dns(crate::dns::DnsGeneration::from_test_answers(HashMap::from(
            [(
                "carrier.product.test".to_string(),
                vec!["198.51.100.42".parse().expect("Product DNS answer")],
            )],
        )))
        .expect("install Product DNS generation");
    assert_eq!(
        network
            .provider
            .resolve(CarrierResolutionRequest {
                path: &domain_path,
                identity,
                remote_port: 449,
            })
            .await
            .expect("resolve carrier through Product DNS"),
        vec![
            "198.51.100.42:449"
                .parse()
                .expect("resolved carrier address")
        ]
    );
}

#[tokio::test]
async fn host_carrier_resolution_authority_is_preserved_without_wrapping() {
    let host: Arc<dyn CarrierNetworkProvider> = Arc::new(AuthoritativeCarrierNetworkProvider {
        address: "203.0.113.7:443".parse().expect("host address"),
    });
    let network = GenerationCarrierNetwork::new(host.clone(), CarrierResolutionAuthority::Host);
    assert!(network.product_dns.is_none());
    assert!(Arc::ptr_eq(&network.provider, &host));

    let path = "tcp://host-owned.invalid:443"
        .parse::<PathSpec>()
        .expect("host-owned path");
    assert_eq!(
        network
            .provider
            .resolve(CarrierResolutionRequest {
                path: &path,
                identity: CarrierPathIdentity {
                    group_ordinal: 2,
                    path_ordinal: 3,
                },
                remote_port: 443,
            })
            .await
            .expect("host resolver remains authoritative"),
        vec!["203.0.113.7:443".parse().expect("host address")]
    );
}

#[tokio::test]
async fn deferred_stop_keeps_services_live_until_retirement_is_authorized() {
    let generation = RuntimeGenerationControl::new();
    generation.defer_retirement();
    let (started_tx, started_rx) = oneshot::channel();
    let (dropped_tx, mut dropped_rx) = oneshot::channel();
    let mut services = JoinSet::new();
    services.spawn(async move {
        let _drop_notice = DropNotice(Some(dropped_tx));
        let _ = started_tx.send(());
        std::future::pending::<Result<(), RuntimeError>>().await
    });
    started_rx.await.expect("service started");

    let supervisor_generation = generation.clone();
    let supervisor = tokio::spawn(async move {
        supervise_runtime_services(
            services,
            &supervisor_generation,
            "service exited",
            "no services",
        )
        .await
    });
    generation.request_shutdown();
    tokio::task::yield_now().await;
    assert!(!supervisor.is_finished());
    assert_eq!(
        dropped_rx.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    );

    generation.authorize_retirement();
    let outcome = tokio::time::timeout(Duration::from_secs(1), supervisor)
        .await
        .expect("supervisor retirement")
        .expect("supervisor task")
        .expect("requested stop");
    assert_eq!(outcome, RuntimeGenerationStopReason::ShutdownRequested);
    tokio::time::timeout(Duration::from_secs(1), &mut dropped_rx)
        .await
        .expect("service drop")
        .expect("drop notice");
}

#[tokio::test]
async fn tcp_pending_authentication_is_reserved_before_task_spawn() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener");
    let address = listener.local_addr().expect("listener address");
    let path = format!("tcp://{address}")
        .parse::<PathSpec>()
        .expect("TCP path");
    let runtime = server::new_identity_runtime(
        vec![path.clone()],
        OutboundConfig::Direct,
        DEFAULT_OUTBOUND_CONNECT_TIMEOUT,
        ServerSecurityConfig::for_test(
            SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("test secret"),
        )
        .with_max_pending_authentications(1),
        MppPerformanceConfig::default(),
        ResourceLimits::default(),
    );
    let held = runtime
        .paths
        .try_begin_authentication()
        .expect("reserve the only pending-authentication slot");
    let client = tokio::net::TcpStream::connect(address);
    let accepted = listener.accept();
    let (mut client, (server_stream, _)) =
        tokio::try_join!(client, accepted).expect("connected TCP pair");
    let mut tasks = JoinSet::new();

    assert!(
        !server::try_spawn_server_tcp_connection(
            &mut tasks,
            server_stream,
            ServerLocalPath::new(0, path),
            runtime.paths.clone(),
        ),
        "an exhausted pending-authentication budget must reject before spawning"
    );
    assert!(
        tasks.is_empty(),
        "rejected unauthenticated sockets must not consume task metadata"
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), client.read_u8())
            .await
            .expect("rejected socket closes promptly")
            .expect_err("closed socket has no byte")
            .kind(),
        std::io::ErrorKind::UnexpectedEof
    );
    drop(held);
    assert_eq!(runtime.paths.pending_authentications.available_permits(), 1);
}

#[tokio::test]
async fn rejected_noise_socket_releases_authentication_work_into_bounded_retention() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener");
    let address = listener.local_addr().expect("listener address");
    let path = format!("tcp://{address}")
        .parse::<PathSpec>()
        .expect("TCP path");
    let secret = [0x5a; 32];
    let security = ServerSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("test secret"),
    )
    .with_authentication_timeout(Duration::from_millis(500))
    .with_max_pending_authentications(1);
    let mut runtime = server::new_identity_runtime(
        vec![path.clone()],
        OutboundConfig::Direct,
        DEFAULT_OUTBOUND_CONNECT_TIMEOUT,
        security,
        MppPerformanceConfig::default(),
        ResourceLimits::default(),
    );
    runtime.paths.tls =
        crate::transport::encrypted::test_server_tls_config_with_transport_secret(secret);
    let context = runtime.paths.clone();
    let local_path = ServerLocalPath::new(0, path);
    let mut tasks = JoinSet::new();

    let first_client = tokio::net::TcpStream::connect(address);
    let first_accepted = listener.accept();
    let (mut first_client, (first_server, _)) =
        tokio::try_join!(first_client, first_accepted).expect("first connected TCP pair");
    assert!(server::try_spawn_server_tcp_connection(
        &mut tasks,
        first_server,
        local_path.clone(),
        context.clone(),
    ));
    first_client
        .write_all(&[0_u8; 34])
        .await
        .expect("write rejected Noise opener");
    first_client
        .shutdown()
        .await
        .expect("half-close rejected Noise opener");

    tokio::time::timeout(Duration::from_millis(100), async {
        while context.pending_authentications.available_permits() != 1
            || context.silent_rejections.available_permits() != 0
        {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("rejected opener transfers out of authentication work");
    assert!(
        tokio::time::timeout(Duration::from_millis(40), first_client.read_u8())
            .await
            .is_err(),
        "retained rejection must remain silent before the shared deadline"
    );

    let valid_client = tokio::net::TcpStream::connect(address);
    let valid_accepted = listener.accept();
    let (valid_client, (valid_server, _)) =
        tokio::try_join!(valid_client, valid_accepted).expect("valid connected TCP pair");
    assert!(server::try_spawn_server_tcp_connection(
        &mut tasks,
        valid_server,
        local_path.clone(),
        context.clone(),
    ));
    let valid = tokio::time::timeout(
        Duration::from_millis(150),
        crate::transport::encrypted::EncryptedFramedStream::connect(
            valid_client,
            &crate::transport::encrypted::test_client_tls_config_with_transport_secret(secret),
            crate::protocol::codec::CodecLimits::default(),
        ),
    )
    .await
    .expect("valid Noise handshake is not blocked by retained rejection")
    .expect("valid Noise handshake");
    drop(valid);

    tokio::time::timeout(Duration::from_millis(100), async {
        while context.pending_authentications.available_permits() != 1 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("valid peer close releases authentication work");

    let overflow_client = tokio::net::TcpStream::connect(address);
    let overflow_accepted = listener.accept();
    let (mut overflow_client, (overflow_server, _)) =
        tokio::try_join!(overflow_client, overflow_accepted).expect("overflow connected TCP pair");
    assert!(server::try_spawn_server_tcp_connection(
        &mut tasks,
        overflow_server,
        local_path,
        context.clone(),
    ));
    overflow_client
        .write_all(&[0_u8; 34])
        .await
        .expect("write overflow Noise opener");
    overflow_client
        .shutdown()
        .await
        .expect("half-close overflow Noise opener");
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(100), overflow_client.read_u8())
            .await
            .expect("retention overload sheds promptly")
            .expect_err("shed rejection has no response byte")
            .kind(),
        std::io::ErrorKind::UnexpectedEof,
        "retention overload must close without response bytes"
    );

    tokio::time::timeout(Duration::from_secs(1), async {
        while context.pending_authentications.available_permits() != 1
            || context.silent_rejections.available_permits() != 1
        {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("both admission budgets recover after the absolute deadline");
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(100), first_client.read_u8())
            .await
            .expect("retained rejection closes at the absolute deadline")
            .expect_err("retained rejection closes without a response byte")
            .kind(),
        std::io::ErrorKind::UnexpectedEof
    );
}
