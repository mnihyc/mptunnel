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
use tokio::io::AsyncReadExt;
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
        resources: ResourceLimits::default(),
        admission: ProductAdmissionConfig::default(),
        management: ManagementConfig::default(),
        command: CommandConfig::Node(NodeConfig {
            outbounds: Vec::new(),
            gateway_balancers: Vec::new(),
            local_ingresses: vec![LocalIngressConfig {
                name: "host-tun".to_string(),
                config: IngressConfig::TunL4(TunL4Config {
                    host,
                    ..TunL4Config::default()
                }),
            }],
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
            hosts: Vec::new(),
            fake_dns: None,
            default_plan: plan,
        },
    }
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
        system_dns,
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
    let literal_path = "udp://192.0.2.10:8440-8450"
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
