use super::*;
use crate::config::{
    ClientPathConfig, ClientSecurityConfig, CommandConfig, DnsPolicyConfig, LocalIngressConfig,
    ManagementConfig, MppOutboundConfig, OutboundLeafConfig, ProductPolicyConfig, ServiceConfig,
    SessionConfig, SharedSecret,
};
use crate::ingress::IngressConfig;
use crate::ingress::tun::{ManagedVpnConfig, ManagedVpnPlatformConfig, TunHostConfig, TunL4Config};
use crate::outbound::{HttpsProxyConfig, OutboundConfig, ProxyConfig};
use crate::performance::{MppPerformanceConfig, ResourceLimits};
use crate::platform::{
    AddressFamily, LinuxHostMutationBackend, LinuxHostOperation, LinuxInterfaceName,
    LinuxNativeRoute, LinuxVpnEnvironment, LinuxVpnPlan, RouteMode,
};
use crate::product::{
    DnsEgressSpec, DnsOutboundCapabilitySpec, DnsPlanId, DnsPlanSpec, DnsPolicySpec,
    DnsSecurityPolicy, DnsUpstreamEndpoint, DnsUpstreamId, DnsUpstreamSpec, DomainName,
    EgressAction, InitialDemand, Network, NetworkSet, OutboundId, RouteAction, RouteMatchSpec,
    RouteRuleSpec, RuleId,
};
use ipnet::IpNet;
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Mutex;

#[derive(Default)]
struct FakeBackend {
    applied: Vec<LinuxHostOperation>,
    rolled_back: Vec<LinuxHostOperation>,
    device: Option<u8>,
}

impl LinuxHostMutationBackend for FakeBackend {
    type RollbackToken = LinuxHostOperation;
    type PreparedDevice = u8;
    type Error = Infallible;

    fn apply(&mut self, operation: &LinuxHostOperation) -> Result<LinuxHostOperation, Infallible> {
        self.applied.push(operation.clone());
        if matches!(operation, LinuxHostOperation::CreateTun { .. }) {
            self.device = Some(7);
        }
        Ok(operation.clone())
    }

    fn rollback(
        &mut self,
        operation: &LinuxHostOperation,
        _token: &LinuxHostOperation,
    ) -> Result<(), Infallible> {
        self.rolled_back.push(operation.clone());
        Ok(())
    }

    fn take_prepared_device(&mut self) -> Result<u8, Infallible> {
        Ok(self.device.take().expect("prepared fake device"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InjectedHostError(&'static str);

impl fmt::Display for InjectedHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for InjectedHostError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InjectedHostEvent {
    Apply(usize),
    Rollback(usize),
    TakeDevice,
}

#[derive(Debug, Default)]
struct InjectedHostControl {
    fail_apply_at: Option<usize>,
    fail_take: bool,
    rollback_failures: HashMap<usize, usize>,
    events: Vec<InjectedHostEvent>,
}

#[derive(Debug, Clone, Default)]
struct InjectedHostHandle(Arc<Mutex<InjectedHostControl>>);

impl InjectedHostHandle {
    fn fail_apply_at(&self, operation: usize) {
        self.0.lock().expect("host fault control").fail_apply_at = Some(operation);
    }

    fn fail_take(&self) {
        self.0.lock().expect("host fault control").fail_take = true;
    }

    fn fail_rollback(&self, token: usize, times: usize) {
        self.0
            .lock()
            .expect("host fault control")
            .rollback_failures
            .insert(token, times);
    }

    fn events(&self) -> Vec<InjectedHostEvent> {
        self.0.lock().expect("host fault control").events.clone()
    }
}

struct InjectedHostBackend {
    control: InjectedHostHandle,
    next_token: usize,
    device: Option<u8>,
}

impl InjectedHostBackend {
    fn new(control: InjectedHostHandle) -> Self {
        Self {
            control,
            next_token: 0,
            device: None,
        }
    }
}

impl LinuxHostMutationBackend for InjectedHostBackend {
    type RollbackToken = usize;
    type PreparedDevice = u8;
    type Error = InjectedHostError;

    fn apply(
        &mut self,
        operation: &LinuxHostOperation,
    ) -> Result<Self::RollbackToken, Self::Error> {
        let token = self.next_token;
        self.next_token = self.next_token.saturating_add(1);
        let mut control = self.control.0.lock().expect("host fault control");
        control.events.push(InjectedHostEvent::Apply(token));
        if control.fail_apply_at == Some(token) {
            return Err(InjectedHostError("apply"));
        }
        if matches!(operation, LinuxHostOperation::CreateTun { .. }) {
            self.device = Some(7);
        }
        Ok(token)
    }

    fn rollback(
        &mut self,
        _operation: &LinuxHostOperation,
        token: &Self::RollbackToken,
    ) -> Result<(), Self::Error> {
        let mut control = self.control.0.lock().expect("host fault control");
        control.events.push(InjectedHostEvent::Rollback(*token));
        if let Some(remaining) = control.rollback_failures.get_mut(token)
            && *remaining > 0
        {
            *remaining -= 1;
            return Err(InjectedHostError("rollback"));
        }
        Ok(())
    }

    fn take_prepared_device(&mut self) -> Result<Self::PreparedDevice, Self::Error> {
        let mut control = self.control.0.lock().expect("host fault control");
        control.events.push(InjectedHostEvent::TakeDevice);
        if control.fail_take {
            return Err(InjectedHostError("take device"));
        }
        self.device
            .take()
            .ok_or(InjectedHostError("missing device"))
    }
}

fn test_plan() -> LinuxVpnPlan {
    let interface = LinuxInterfaceName::parse("mptun0").expect("interface");
    let config = LinuxVpnConfig::new(
        interface.clone(),
        vec!["10.88.0.1/24".parse::<IpNet>().expect("TUN address")],
        1500,
        RouteMode::Full,
    )
    .expect("VPN config");
    let native = LinuxNativeRoute::new(
        AddressFamily::Ipv4,
        LinuxInterfaceName::parse("eth0").expect("native interface"),
        Some("192.0.2.1".parse().expect("gateway")),
        Some("192.0.2.2".parse().expect("source")),
        100,
    )
    .expect("native route");
    let environment = LinuxVpnEnvironment::new(vec![native], Vec::new()).expect("environment");
    LinuxVpnPlan::build(
        &config,
        &environment,
        ["198.51.100.10".parse().expect("carrier")],
        [],
    )
    .expect("plan")
}

fn outbound_id(value: &str) -> OutboundId {
    OutboundId::parse(value).expect("outbound ID")
}

fn encrypted_dns_policy() -> DnsPolicyConfig {
    encrypted_dns_policy_with_egress(DnsEgressSpec::Direct)
}

fn encrypted_dns_policy_with_egress(egress: DnsEgressSpec) -> DnsPolicyConfig {
    let upstream_id = DnsUpstreamId::parse("bootstrap-dot").expect("upstream ID");
    let plan_id = DnsPlanId::parse("default").expect("plan ID");
    let mut plan = DnsPlanSpec::new(plan_id.clone(), vec![upstream_id.clone()]);
    plan.security = DnsSecurityPolicy::RequireEncrypted;
    let outbound_capabilities = match &egress {
        DnsEgressSpec::Direct => Vec::new(),
        DnsEgressSpec::Outbound(outbound) => vec![DnsOutboundCapabilitySpec::new(
            outbound.clone(),
            NetworkSet::TCP,
            true,
        )],
    };
    DnsPolicyConfig {
        generation: 17,
        spec: DnsPolicySpec {
            upstreams: vec![DnsUpstreamSpec {
                id: upstream_id,
                endpoint: DnsUpstreamEndpoint::Tls {
                    bootstrap: "1.1.1.1:853".parse().expect("bootstrap"),
                    server_name: DomainName::parse("one.one.one.one").expect("server name"),
                },
                egress,
            }],
            outbound_capabilities,
            plans: vec![plan],
            rules: Vec::new(),
            override_records: Vec::new(),
            synthetic_captures: Vec::new(),
            default_plan: plan_id,
        },
    }
}

fn plaintext_dns_policy() -> DnsPolicyConfig {
    let upstream_id = DnsUpstreamId::parse("plaintext").expect("upstream ID");
    let plan_id = DnsPlanId::parse("default").expect("plan ID");
    DnsPolicyConfig {
        generation: 18,
        spec: DnsPolicySpec {
            upstreams: vec![DnsUpstreamSpec::direct(
                upstream_id.clone(),
                DnsUpstreamEndpoint::Udp {
                    bootstrap: "1.1.1.1:53".parse().expect("bootstrap"),
                },
            )],
            outbound_capabilities: Vec::new(),
            plans: vec![DnsPlanSpec::new(plan_id.clone(), vec![upstream_id])],
            rules: Vec::new(),
            override_records: Vec::new(),
            synthetic_captures: Vec::new(),
            default_plan: plan_id,
        },
    }
}

fn managed_tun(name: &str) -> LocalIngressConfig {
    LocalIngressConfig {
        name: name.to_owned(),
        config: IngressConfig::TunL4(TunL4Config {
            host: TunHostConfig::Managed(ManagedVpnConfig {
                route_mode: RouteMode::Full,
                excludes: Vec::new(),
                local_lan: false,
                dns_capture_servers: vec!["10.88.0.53".parse().expect("DNS capture server")],
                platform: ManagedVpnPlatformConfig {
                    linux: Some(crate::platform::LinuxPolicyConfig::default()),
                },
            }),
            ..TunL4Config::default()
        }),
    }
}

fn external_tun(name: &str) -> LocalIngressConfig {
    LocalIngressConfig {
        name: name.to_owned(),
        config: IngressConfig::TunL4(TunL4Config::default()),
    }
}

fn direct_leaf(id: &str) -> OutboundLeafConfig {
    local_leaf(id, OutboundConfig::Direct)
}

fn local_leaf(id: &str, config: OutboundConfig) -> OutboundLeafConfig {
    OutboundLeafConfig::Local {
        id: outbound_id(id),
        config,
        connect_timeout: Duration::from_secs(5),
    }
}

fn proxy_leaf(id: &str, endpoint: Endpoint) -> OutboundLeafConfig {
    local_leaf(id, OutboundConfig::Socks5(ProxyConfig::new(endpoint, None)))
}

fn test_security() -> ClientSecurityConfig {
    ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("shared secret"),
    )
}

fn mpp_leaf(id: &str, paths: impl IntoIterator<Item = PathSpec>) -> OutboundLeafConfig {
    let security = test_security();
    let paths = paths
        .into_iter()
        .enumerate()
        .map(|(index, spec)| ClientPathConfig {
            name: format!("path-{}", index + 1),
            tls: crate::transport::encrypted::test_client_tls_config(),
            spec,
            security: security.clone(),
        })
        .collect();
    OutboundLeafConfig::Mpp {
        id: outbound_id(id),
        config: Box::new(MppOutboundConfig {
            security,
            paths,
            path_probe_interval: Duration::from_secs(10),
            path_probe_timeout: Duration::from_secs(2),
            allow_peer_diagnostics: false,
            performance: MppPerformanceConfig::default(),
        }),
    }
}

fn product_policy_for_outbounds(outbounds: &[OutboundLeafConfig]) -> ProductPolicyConfig {
    let mut routes = outbounds
        .iter()
        .enumerate()
        .map(|(index, outbound)| {
            let capabilities = outbound.networks();
            let matcher = RouteMatchSpec {
                domain_exact: vec![
                    DomainName::parse(&format!("active-{index}.example"))
                        .expect("active route domain"),
                ],
                networks: [Network::Tcp, Network::Udp]
                    .into_iter()
                    .filter(|network| capabilities.contains(*network))
                    .collect(),
                ..RouteMatchSpec::default()
            };
            RouteRuleSpec::new(
                RuleId::parse(&format!("active-{index}")).expect("active route ID"),
                matcher,
                RouteAction::allow(
                    EgressAction::Outbound(outbound.id().clone()),
                    None,
                    InitialDemand::Automatic,
                ),
            )
        })
        .collect::<Vec<_>>();
    routes.push(RouteRuleSpec::new(
        RuleId::parse("default-reject").expect("default route ID"),
        RouteMatchSpec::default(),
        RouteAction::reject(),
    ));
    ProductPolicyConfig {
        generation: 1,
        routes,
    }
}

fn replace_outbounds(node: &mut NodeConfig, outbounds: Vec<OutboundLeafConfig>) {
    node.product_policy = Some(product_policy_for_outbounds(&outbounds));
    node.outbounds = outbounds;
}

fn node_with_vpn(outbounds: Vec<OutboundLeafConfig>) -> NodeConfig {
    let product_policy = product_policy_for_outbounds(&outbounds);
    NodeConfig {
        forwarding_mode: crate::config::ForwardingMode::L4,
        outbounds,
        gateway_balancers: Vec::new(),
        local_ingresses: vec![managed_tun("vpn")],
        tun_l3_ingresses: Vec::new(),
        product_policy: Some(product_policy),
        dns_policy: encrypted_dns_policy(),
        servers: Vec::new(),
    }
}

fn app_with_node(node: NodeConfig) -> AppConfig {
    AppConfig {
        logging: crate::config::LoggingConfig::default(),
        check_config: false,
        service: ServiceConfig::default(),
        session: SessionConfig::default(),
        flow: crate::config::ProductFlowConfig::default(),
        resources: ResourceLimits::default(),
        admission: crate::product::ProductAdmissionConfig::default(),
        management: ManagementConfig::default(),
        command: CommandConfig::Node(node),
    }
}

#[test]
fn node_without_managed_tun_compiles_to_none_without_dns_side_effects() {
    let node = NodeConfig {
        forwarding_mode: crate::config::ForwardingMode::L4,
        outbounds: vec![direct_leaf("direct")],
        gateway_balancers: Vec::new(),
        local_ingresses: vec![external_tun("external")],
        tun_l3_ingresses: Vec::new(),
        product_policy: None,
        dns_policy: DnsPolicyConfig::system_default(),
        servers: Vec::new(),
    };

    assert!(
        compile_node_linux_vpn_prepare_request(&node)
            .expect("external TUN is not a managed generation")
            .is_none()
    );
}

#[test]
fn app_compiler_supports_direct_only_managed_vpn() {
    let app = app_with_node(node_with_vpn(vec![direct_leaf("direct")]));

    let request = compile_linux_vpn_prepare_request(&app)
        .expect("compile")
        .expect("managed request");

    assert_eq!(request.managed_tun_count, 1);
    assert!(request.carrier_paths.is_empty());
    assert!(request.native_proxy_endpoints.is_empty());
    assert!(request.prepublication_domains.is_empty());
    assert_eq!(request.resolution_timeout, LINUX_VPN_RESOLUTION_TIMEOUT);
    assert!(!request.resolution_timeout.is_zero());
    assert_eq!(request.config.route_mode(), &RouteMode::Full);
    assert_eq!(request.dns_policy.generation(), 17);
    assert_eq!(
        request.dns_policy.bootstrap_endpoints().collect::<Vec<_>>(),
        vec!["1.1.1.1:853".parse().expect("bootstrap")]
    );
}

#[test]
fn compiler_matches_combined_runtime_ordinals_and_collects_all_native_proxies() {
    let proxy_a = Endpoint::new("proxy-a.example", 1080).expect("proxy A");
    let proxy_b = Endpoint::new("proxy-b.example", 8443).expect("proxy B");
    let proxy_c = Endpoint::new("proxy-c.example", 8080).expect("proxy C");
    let https = HttpsProxyConfig::new(
        ProxyConfig::new(proxy_b.clone(), None),
        Some("proxy-b.example".to_owned()),
        Vec::new(),
    )
    .expect("HTTPS proxy");
    let node = node_with_vpn(vec![
        direct_leaf("direct"),
        mpp_leaf(
            "first-mpp",
            [
                "tcp://carrier-a.example:443".parse().expect("TCP path"),
                "quic://carrier-b.example:443".parse().expect("QUIC path"),
            ],
        ),
        proxy_leaf("socks", proxy_a.clone()),
        local_leaf("https", OutboundConfig::HttpsConnect(Box::new(https))),
        mpp_leaf(
            "second-mpp",
            ["quic://carrier-c.example:8443"
                .parse()
                .expect("second QUIC path")],
        ),
        local_leaf(
            "connect",
            OutboundConfig::HttpConnect(ProxyConfig::new(proxy_c.clone(), None)),
        ),
        proxy_leaf("duplicate-socks", proxy_a.clone()),
    ]);

    let request = compile_node_linux_vpn_prepare_request(&node)
        .expect("compile")
        .expect("managed request");

    assert_eq!(
        request
            .carrier_paths
            .iter()
            .map(|path| (
                path.identity.group_ordinal,
                path.identity.path_ordinal,
                path.path.endpoint.authority(),
            ))
            .collect::<Vec<_>>(),
        vec![
            (0, 0, "carrier-a.example:443".to_owned()),
            (0, 1, "carrier-b.example:443".to_owned()),
            (1, 0, "carrier-c.example:8443".to_owned()),
        ]
    );
    assert_eq!(
        request.native_proxy_endpoints,
        vec![proxy_a, proxy_b, proxy_c],
        "every local proxy control endpoint is retained once in leaf order"
    );
    assert_eq!(
        request
            .prepublication_domains
            .iter()
            .map(DomainName::as_str)
            .collect::<Vec<_>>(),
        vec![
            "carrier-a.example",
            "carrier-b.example",
            "carrier-c.example",
            "proxy-a.example",
            "proxy-b.example",
            "proxy-c.example",
        ],
        "pre-publication DNS inventory is canonical, sorted, and deduplicated"
    );
}

#[test]
fn compiler_rejects_multiple_or_invalid_managed_tun_inventory_precisely() {
    let mut node = node_with_vpn(vec![direct_leaf("direct")]);
    node.local_ingresses.push(managed_tun("second"));
    assert!(matches!(
        compile_node_linux_vpn_prepare_request(&node),
        Err(LinuxVpnGenerationSpecError::MultipleManagedTunInbounds { actual: 2 })
    ));

    node.local_ingresses.pop();
    let IngressConfig::TunL4(tun) = &mut node.local_ingresses[0].config else {
        panic!("managed TUN");
    };
    tun.dns_resolvers = vec!["1.1.1.1:53".parse().expect("external DNS")];
    let error = compile_node_linux_vpn_prepare_request(&node).expect_err("invalid TUN");
    assert!(matches!(
        error,
        LinuxVpnGenerationSpecError::ManagedTun {
            ingress_index: 0,
            ref ingress_name,
            source: ManagedVpnCompileError::ExternalDnsResolvers,
        } if ingress_name == "vpn"
    ));
    assert!(error.to_string().contains("vpn"));
}

#[test]
fn compiler_rejects_impossible_mpp_inventory_before_resolution() {
    let mut node = node_with_vpn(vec![mpp_leaf("empty", [])]);
    assert!(matches!(
        compile_node_linux_vpn_prepare_request(&node),
        Err(
            LinuxVpnGenerationSpecError::MppOutboundWithoutCarrierPaths {
                ref outbound
            }
        ) if outbound == "empty"
    ));

    replace_outbounds(
        &mut node,
        vec![mpp_leaf(
            "invalid",
            ["quic://carrier.example:443".parse().expect("path")],
        )],
    );
    let OutboundLeafConfig::Mpp { config, .. } = &mut node.outbounds[0] else {
        panic!("MPP");
    };
    config.paths[0].spec.endpoint.host.clear();
    assert!(matches!(
        compile_node_linux_vpn_prepare_request(&node),
        Err(LinuxVpnGenerationSpecError::InvalidCarrierEndpoint {
            ref outbound,
            path_ordinal: 0,
            ..
        }) if outbound == "invalid"
    ));
}

#[test]
fn compiler_enforces_generation_inventory_bounds() {
    let path = "quic://carrier.example:443"
        .parse::<PathSpec>()
        .expect("path");
    let node = node_with_vpn(vec![mpp_leaf(
        "too-many",
        std::iter::repeat_n(path, MAX_PREPARED_CARRIER_PATHS + 1),
    )]);
    assert!(matches!(
        compile_node_linux_vpn_prepare_request(&node),
        Err(LinuxVpnGenerationSpecError::TooManyCarrierPaths {
            actual,
            maximum: MAX_PREPARED_CARRIER_PATHS,
        }) if actual == MAX_PREPARED_CARRIER_PATHS + 1
    ));

    let proxies = (0..=MAX_NATIVE_ENDPOINTS)
        .map(|index| {
            proxy_leaf(
                &format!("proxy-{index}"),
                Endpoint::new(
                    format!("proxy-{index}.example"),
                    u16::try_from(10_000 + index).expect("bounded test port"),
                )
                .expect("proxy endpoint"),
            )
        })
        .collect();
    let node = node_with_vpn(proxies);
    assert!(matches!(
        compile_node_linux_vpn_prepare_request(&node),
        Err(LinuxVpnGenerationSpecError::TooManyNativeEndpoints {
            actual,
            maximum: MAX_NATIVE_ENDPOINTS,
        }) if actual == MAX_NATIVE_ENDPOINTS + 1
    ));
}

#[test]
fn compiler_rejects_system_plaintext_invalid_and_precarrier_outbound_dns() {
    let mut node = node_with_vpn(vec![direct_leaf("direct")]);
    node.dns_policy = DnsPolicyConfig::system_default();
    assert!(matches!(
        compile_node_linux_vpn_prepare_request(&node),
        Err(LinuxVpnGenerationSpecError::SystemDnsUnsupported)
    ));

    node.dns_policy = plaintext_dns_policy();
    assert!(matches!(
        compile_node_linux_vpn_prepare_request(&node),
        Err(LinuxVpnGenerationSpecError::EncryptedDnsRequired)
    ));

    node.dns_policy = encrypted_dns_policy();
    node.dns_policy.spec.default_plan = DnsPlanId::parse("missing").expect("missing plan ID");
    assert!(matches!(
        compile_node_linux_vpn_prepare_request(&node),
        Err(LinuxVpnGenerationSpecError::DnsPolicy(_))
    ));

    let dns_outbound = outbound_id("dns-proxy");
    node.dns_policy =
        encrypted_dns_policy_with_egress(DnsEgressSpec::Outbound(dns_outbound.clone()));
    replace_outbounds(
        &mut node,
        vec![proxy_leaf(
            dns_outbound.as_str(),
            Endpoint::new("192.0.2.40", 1080).expect("literal DNS proxy"),
        )],
    );
    let request = compile_node_linux_vpn_prepare_request(&node)
        .expect("literal-only routed DNS is bootstrap-safe")
        .expect("managed request");
    assert!(request.prepublication_domains.is_empty());
    assert!(
        request.dns_policy.bootstrap_endpoints().next().is_none(),
        "a routed DNS endpoint must not be leaked into the host bypass"
    );

    replace_outbounds(
        &mut node,
        vec![proxy_leaf(
            dns_outbound.as_str(),
            Endpoint::new("dns-proxy.example", 1080).expect("named DNS proxy"),
        )],
    );
    assert!(matches!(
        compile_node_linux_vpn_prepare_request(&node),
        Err(
            LinuxVpnGenerationSpecError::PreCarrierDnsEgressUnsupported {
                ref upstream,
                ref outbound,
            }
        ) if upstream == "bootstrap-dot" && outbound == dns_outbound.as_str()
    ));
}

#[test]
fn bootstrap_dns_fails_closed_for_named_outbound_egress() {
    let policy = Arc::new(
        encrypted_dns_policy_with_egress(DnsEgressSpec::Outbound(outbound_id("named")))
            .compile()
            .expect("compiled Product DNS"),
    );
    let domains = [DomainName::parse("carrier.example").expect("carrier domain")];

    assert!(matches!(
        compile_bootstrap_dns(policy, &domains),
        Err(LinuxVpnPrepareError::BootstrapDns(
            DnsRuntimeError::PrepublicationDnsRequiresDirect { .. }
        ))
    ));
}

#[tokio::test]
async fn prepublication_resolution_uses_injected_dns_and_literal_fast_path() {
    let dns = DnsGeneration::from_test_answers(HashMap::from([
        (
            "carrier.example".to_owned(),
            vec!["198.51.100.10".parse().expect("carrier IP")],
        ),
        (
            "proxy.example".to_owned(),
            vec!["203.0.113.20".parse().expect("proxy IP")],
        ),
    ]));
    let carrier_path = LinuxVpnCarrierPath {
        identity: CarrierPathIdentity {
            group_ordinal: 2,
            path_ordinal: 3,
        },
        path: "quic://carrier.example:443".parse().expect("carrier path"),
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);

    let carriers = resolve_carrier_paths(vec![carrier_path], Some(&dns), deadline)
        .await
        .expect("carrier resolution");
    assert_eq!(
        carriers.paths()[0].addresses(),
        &["198.51.100.10".parse::<IpAddr>().expect("carrier address")]
    );
    let native = resolve_native_endpoints(
        vec![
            Endpoint::new("proxy.example", 8443).expect("proxy"),
            Endpoint::new("192.0.2.30", 9443).expect("literal"),
            Endpoint::new("127.0.0.1", 1080).expect("loopback"),
        ],
        Some(&dns),
        deadline,
    )
    .await
    .expect("native resolution");
    assert_eq!(
        native,
        vec![
            "192.0.2.30".parse::<IpAddr>().expect("literal IP"),
            "203.0.113.20".parse::<IpAddr>().expect("proxy IP"),
        ]
    );

    let literal_carrier = LinuxVpnCarrierPath {
        identity: CarrierPathIdentity {
            group_ordinal: 4,
            path_ordinal: 0,
        },
        path: "tcp://198.51.100.30:443"
            .parse()
            .expect("literal carrier path"),
    };
    let literal_carriers = resolve_carrier_paths(vec![literal_carrier], None, deadline)
        .await
        .expect("literal carrier does not require DNS");
    assert_eq!(
        literal_carriers.paths()[0].addresses(),
        &["198.51.100.30".parse::<IpAddr>().expect("literal address")]
    );
    assert_eq!(
        resolve_native_endpoints(
            vec![Endpoint::new("192.0.2.31", 1080).expect("literal proxy")],
            None,
            deadline,
        )
        .await
        .expect("literal proxy does not require DNS"),
        vec!["192.0.2.31".parse::<IpAddr>().expect("literal IP")]
    );
}

#[test]
fn linux_vpn_production_source_never_calls_the_system_hostname_resolver() {
    let production_source = include_str!("linux_vpn.rs")
        .split("#[cfg(test)]")
        .next()
        .expect("production source");
    assert!(!production_source.contains("lookup_host"));
}

#[test]
fn host_prepare_failure_retries_residual_rollback_before_returning() {
    let plan = test_plan();
    let prepare_count = plan.prepare_operations().len();
    assert!(prepare_count >= 2);
    let control = InjectedHostHandle::default();
    control.fail_apply_at(prepare_count - 1);
    control.fail_rollback(0, 1);

    let error = match VpnHostLifecycle::prepare(InjectedHostBackend::new(control.clone()), plan) {
        Err(error) => error,
        Ok(_) => panic!("injected prepare failure"),
    };
    assert!(matches!(
        error,
        HostPrepareError::Prepare { cleanup: None, .. }
    ));
    assert_eq!(
        control
            .events()
            .iter()
            .filter(|event| **event == InjectedHostEvent::Rollback(0))
            .count(),
        2,
        "prepare rollback residue was not retried before controller drop"
    );
}

#[test]
fn host_device_handoff_failure_cleans_the_inert_prepare() {
    let plan = test_plan();
    let prepare_count = plan.prepare_operations().len();
    let control = InjectedHostHandle::default();
    control.fail_take();

    let error = match VpnHostLifecycle::prepare(InjectedHostBackend::new(control.clone()), plan) {
        Err(error) => error,
        Ok(_) => panic!("injected device handoff failure"),
    };
    assert!(matches!(
        error,
        HostPrepareError::Device { cleanup: None, .. }
    ));
    assert_eq!(
        control
            .events()
            .iter()
            .filter(|event| matches!(event, InjectedHostEvent::Rollback(_)))
            .count(),
        prepare_count
    );
}

#[test]
fn host_unpublish_failure_retains_only_failed_publication_for_retry() {
    let plan = test_plan();
    let prepare_count = plan.prepare_operations().len();
    let publish_count = plan.publish_operations().len();
    let last_publish_token = prepare_count + publish_count - 1;
    let control = InjectedHostHandle::default();
    let (mut lifecycle, _device) =
        VpnHostLifecycle::prepare(InjectedHostBackend::new(control.clone()), plan)
            .expect("prepare");
    lifecycle.publish().expect("publish");
    control.fail_rollback(last_publish_token, 1);

    assert!(lifecycle.unpublish().is_err());
    assert_eq!(lifecycle.pending_publish_steps(), 1);
    assert_eq!(lifecycle.state(), LinuxControllerState::CleanupPending);
    lifecycle.unpublish().expect("retry unpublish");
    assert_eq!(lifecycle.pending_publish_steps(), 0);
    assert_eq!(lifecycle.state(), LinuxControllerState::Prepared);
    lifecycle.cleanup().expect("inert prepare cleanup");
}

#[test]
fn cleanup_failure_after_unpublish_cannot_restore_host_publication() {
    let plan = test_plan();
    let prepare_count = plan.prepare_operations().len();
    let control = InjectedHostHandle::default();
    let (mut lifecycle, _device) =
        VpnHostLifecycle::prepare(InjectedHostBackend::new(control.clone()), plan)
            .expect("prepare");
    lifecycle.publish().expect("publish");
    lifecycle.unpublish().expect("unpublish");
    control.fail_rollback(prepare_count - 1, 1);

    assert!(lifecycle.cleanup().is_err());
    assert_eq!(lifecycle.pending_publish_steps(), 0);
    assert_eq!(lifecycle.state(), LinuxControllerState::CleanupPending);
    lifecycle.cleanup().expect("retry inert cleanup");
    assert_eq!(lifecycle.state(), LinuxControllerState::Idle);
}

#[test]
fn host_lifecycle_preserves_prepare_publish_unpublish_cleanup_order() {
    let (mut lifecycle, device) =
        VpnHostLifecycle::prepare(FakeBackend::default(), test_plan()).expect("prepare");
    assert_eq!(device, 7);
    assert_eq!(lifecycle.state(), LinuxControllerState::Prepared);
    assert!(
        lifecycle
            .controller
            .backend()
            .applied
            .iter()
            .all(|operation| {
                !matches!(
                    operation,
                    LinuxHostOperation::ActivateNativeEgressRule { .. }
                        | LinuxHostOperation::ActivateCaptureRule { .. }
                        | LinuxHostOperation::ConfigureDns { .. }
                )
            })
    );

    lifecycle.publish().expect("publish");
    assert_eq!(lifecycle.state(), LinuxControllerState::Active);
    lifecycle.unpublish().expect("unpublish");
    assert_eq!(lifecycle.state(), LinuxControllerState::Prepared);
    lifecycle.cleanup().expect("cleanup");
    assert_eq!(lifecycle.state(), LinuxControllerState::Idle);

    let backend = lifecycle.controller.backend();
    let first_publish = backend
        .applied
        .iter()
        .position(|operation| {
            matches!(
                operation,
                LinuxHostOperation::ActivateNativeEgressRule { .. }
            )
        })
        .expect("native publish");
    let first_capture = backend
        .applied
        .iter()
        .position(|operation| matches!(operation, LinuxHostOperation::ActivateCaptureRule { .. }))
        .expect("capture publish");
    assert!(first_publish < first_capture);
    assert_eq!(
        backend.rolled_back.first(),
        backend.applied.iter().rfind(|operation| {
            matches!(operation, LinuxHostOperation::ActivateCaptureRule { .. })
        })
    );
    assert_eq!(
        backend.rolled_back.last(),
        backend.applied.first(),
        "TUN creation is the final cleanup operation"
    );
}
