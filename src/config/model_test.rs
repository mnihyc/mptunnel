use super::*;

fn managed_tun(route_mode: crate::platform::RouteMode) -> crate::ingress::tun::TunL4Config {
    use crate::ingress::tun::{ManagedVpnConfig, ManagedVpnPlatformConfig, TunHostConfig};

    crate::ingress::tun::TunL4Config {
        host: TunHostConfig::Managed(ManagedVpnConfig {
            route_mode,
            excludes: Vec::new(),
            local_lan: false,
            dns_capture_servers: vec!["10.88.0.53".parse().expect("DNS capture server")],
            platform: ManagedVpnPlatformConfig::default(),
        }),
        ..crate::ingress::tun::TunL4Config::default()
    }
}

#[test]
fn external_tun_is_the_non_mutating_default() {
    let tun = crate::ingress::tun::TunL4Config::default();

    assert!(tun.managed_vpn().is_none());
    assert!(tun.managed_dns_capture_servers().is_empty());
    assert_eq!(tun.compile_managed_vpn().expect("compile"), None);
}

#[test]
fn managed_full_tun_compiles_portable_platform_config() {
    let mut tun = managed_tun(crate::platform::RouteMode::Full);
    tun.interface_name = None;
    let managed = tun.managed_vpn().expect("managed");
    assert_eq!(
        tun.managed_dns_capture_servers(),
        managed.dns_capture_servers
    );

    let platform = tun
        .compile_managed_vpn()
        .expect("compile")
        .expect("managed config");
    assert_eq!(
        platform.addresses(),
        &["10.88.0.1/24".parse().expect("address")]
    );
    assert_eq!(platform.route_mode(), &crate::platform::RouteMode::Full);
    assert_eq!(
        platform.dns().expect("DNS").servers(),
        &["10.88.0.53".parse::<std::net::IpAddr>().expect("DNS")]
    );
}

#[test]
fn managed_vpn_compile_excludes_platform_identity_and_linux_tuning() {
    let mut tun = managed_tun(crate::platform::RouteMode::Full);
    tun.interface_name = Some("host/adapter/owned/name".to_string());
    let baseline = tun
        .compile_managed_vpn()
        .expect("portable compile")
        .expect("managed config");
    let crate::ingress::tun::TunHostConfig::Managed(managed) = &mut tun.host else {
        panic!("managed host");
    };
    managed.platform.linux = Some(
        crate::platform::LinuxPolicyConfig::new(
            51_900,
            10_100,
            10_101,
            crate::platform::LinuxSocketMark::new(0x1234).expect("socket mark"),
        )
        .expect("Linux tuning"),
    );

    assert_eq!(
        tun.compile_managed_vpn()
            .expect("portable compile with tuning")
            .expect("managed config"),
        baseline,
        "portable desired state must not absorb platform identity or Linux RPDB tuning"
    );
}

#[test]
fn managed_full_tun_requires_local_dns_capture() {
    let mut tun = managed_tun(crate::platform::RouteMode::Full);
    let crate::ingress::tun::TunHostConfig::Managed(managed) = &mut tun.host else {
        panic!("managed host");
    };
    managed.dns_capture_servers.clear();

    assert!(matches!(
        validate_tun_l4(&tun),
        Err(ConfigError::ManagedVpn(message))
            if message.contains("full VPN requires at least one DNS capture server")
    ));
}

#[test]
fn managed_tun_rejects_external_dns_and_gateway_fields() {
    let mut tun = managed_tun(crate::platform::RouteMode::Full);
    tun.dns_resolvers = vec!["1.1.1.1:53".parse().expect("resolver")];
    assert!(matches!(
        validate_tun_l4(&tun),
        Err(ConfigError::ManagedVpn(message))
            if message.contains("cannot set external TUN dns_resolvers")
    ));

    tun.dns_resolvers.clear();
    tun.ipv4_gateway = Some("10.88.0.254".parse().expect("gateway"));
    assert!(matches!(
        validate_tun_l4(&tun),
        Err(ConfigError::ManagedVpn(message))
            if message.contains("external/manual TUN IPv4 gateway")
    ));
}

#[test]
fn managed_tun_uses_platform_family_validation() {
    let mut tun = managed_tun(crate::platform::RouteMode::Split(vec![
        "2001:db8::/32".parse().expect("include"),
    ]));
    let crate::ingress::tun::TunHostConfig::Managed(managed) = &mut tun.host else {
        panic!("managed host");
    };
    managed.dns_capture_servers.clear();

    assert!(matches!(
        validate_tun_l4(&tun),
        Err(ConfigError::ManagedVpn(message))
            if message.contains("no configured TUN address of the same family")
    ));
}

#[test]
fn node_rejects_multiple_managed_tun_owners() {
    let ingress = |name: &str| LocalIngressConfig {
        name: name.to_string(),
        config: IngressConfig::TunL4(managed_tun(crate::platform::RouteMode::Full)),
    };

    assert_eq!(
        validate_local_ingresses(&[ingress("tun-a"), ingress("tun-b")]),
        Err(ConfigError::MultipleManagedTunInbounds { actual: 2 })
    );
}

#[test]
fn extra_traffic_hint_default_is_five_percent() {
    assert_eq!(
        MppPerformanceConfig::default().extra_traffic_hint_percent,
        5
    );
}

#[test]
fn udp_path_configuration_is_strict_and_requires_sni_identity() {
    let default_path = "udp://127.0.0.1:443-445"
        .parse::<PathSpec>()
        .expect("default udp path parses");

    assert_eq!(
        default_path.underlay,
        crate::protocol::UnderlayProtocol::Udp
    );
    assert!(
        "udp://127.0.0.1:443?unsupported=true"
            .parse::<PathSpec>()
            .is_err()
    );
    assert!(
        "udp://127.0.0.1:443?profile=experimental"
            .parse::<PathSpec>()
            .is_err()
    );

    let security = ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec())
            .expect("test shared secret"),
    );
    let outbound = MppOutboundConfig {
        security: security.clone(),
        paths: vec![ClientPathConfig {
            name: "path-1".to_string(),
            spec: default_path,
            security,
            tls: crate::transport::encrypted::test_client_tls_config_for_server_name("127.0.0.1"),
        }],
        path_probe_interval: DEFAULT_PATH_PROBE_INTERVAL,
        path_probe_timeout: DEFAULT_PATH_PROBE_TIMEOUT,
        performance: MppPerformanceConfig::default(),
    };
    assert_eq!(
        validate_mpp_outbound(&outbound, ResourceLimits::default()),
        Err(ConfigError::QuicTlsServerNameRequiresDns)
    );

    let mut outbound = outbound;
    outbound.paths[0].spec = "tcp://127.0.0.1:443"
        .parse()
        .expect("default TCP carrier range");
    let mut resources = ResourceLimits::default();
    resources.max_paths = 2;
    assert_eq!(
        validate_mpp_outbound(&outbound, resources),
        Err(ConfigError::TooManyPaths {
            actual: 3,
            limit: 2
        })
    );
    outbound.paths[0].spec = "tcp://127.0.0.1:443?tcp-carriers=1-2"
        .parse()
        .expect("bounded TCP carrier range");
    assert_eq!(validate_mpp_outbound(&outbound, resources), Ok(()));
}

#[test]
fn server_paths_reject_client_only_endpoint_options() {
    let security = ServerSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec())
            .expect("test shared secret"),
    );
    let server = MppInboundConfig {
        name: "mpp-inbound".to_string(),
        egress: EgressRef::Outbound(OutboundId::parse("direct").expect("outbound")),
        dns_plan: None,
        paths: vec![NamedPathConfig {
            name: "path-1".to_string(),
            spec: "tcp://127.0.0.1:443?source-ip=127.0.0.1"
                .parse()
                .expect("server path"),
        }],
        security,
        tls: crate::transport::encrypted::test_server_tls_config(),
        destination_acl: ServerDestinationAclConfig::default(),
        performance: MppPerformanceConfig::default(),
    };

    assert_eq!(
        validate_mpp_inbound(&server, ResourceLimits::default()),
        Err(ConfigError::ServerPathSourceBinding)
    );

    let mut server = server;
    for ranged in ["tcp://127.0.0.1:443-445", "udp://127.0.0.1:443-445"] {
        server.paths[0].spec = ranged.parse().expect("ranged server path");
        assert_eq!(
            validate_mpp_inbound(&server, ResourceLimits::default()),
            Err(ConfigError::ServerPathPortRange)
        );
    }
    server.paths[0].spec = "tcp://127.0.0.1:443?tcp-carriers=1-3"
        .parse()
        .expect("client carrier policy");
    assert_eq!(
        validate_mpp_inbound(&server, ResourceLimits::default()),
        Err(ConfigError::ServerTcpCarrierRange)
    );
}

#[test]
fn management_dashboard_requires_an_http_listener() {
    let config = ManagementConfig {
        dashboard: true,
        ..ManagementConfig::default()
    };

    assert_eq!(
        config.validate(),
        Err(ConfigError::ManagementDashboardWithoutListener)
    );
}

#[test]
fn loopback_management_listener_requires_a_token() {
    let config = ManagementConfig {
        listen: vec!["127.0.0.1:7600".parse().expect("listen")],
        ..ManagementConfig::default()
    };

    assert_eq!(
        config.validate(),
        Err(ConfigError::ManagementListenerRequiresToken)
    );
}

#[test]
fn management_listener_rejects_an_empty_token() {
    let config = ManagementConfig {
        listen: vec!["127.0.0.1:7600".parse().expect("listen")],
        token: Some(String::new()),
        ..ManagementConfig::default()
    };

    assert_eq!(config.validate(), Err(ConfigError::ManagementTokenEmpty));
}

#[test]
fn management_listener_rejects_weak_or_header_unsafe_tokens() {
    for token in ["short", "sixteen bytes bad ", "sixteen\nbytesbad"] {
        let config = ManagementConfig {
            listen: vec!["127.0.0.1:9090".parse().expect("address")],
            token: Some(token.to_string()),
            ..ManagementConfig::default()
        };
        assert_eq!(config.validate(), Err(ConfigError::ManagementTokenInvalid));
    }
}

#[test]
fn non_loopback_management_listener_is_rejected_even_with_a_token() {
    let config = ManagementConfig {
        listen: vec!["0.0.0.0:7600".parse().expect("listen")],
        token: Some("operator-token-123".to_string()),
        ..ManagementConfig::default()
    };

    assert_eq!(
        config.validate(),
        Err(ConfigError::ManagementListenerMustBeLoopback)
    );
}

#[test]
fn loopback_management_listener_with_a_token_is_valid() {
    let config = ManagementConfig {
        listen: vec!["[::1]:7600".parse().expect("listen")],
        token: Some("operator-token-123".to_string()),
        ..ManagementConfig::default()
    };

    assert_eq!(config.validate(), Ok(()));
}

#[test]
fn peer_diagnostics_does_not_require_a_local_http_listener() {
    let config = ManagementConfig {
        allow_peer_diagnostics: true,
        ..ManagementConfig::default()
    };

    assert_eq!(config.validate(), Ok(()));
    assert!(!config.http_enabled());
    assert!(config.peer_diagnostics_enabled());
}

#[test]
fn quic_idle_timeout_must_exceed_native_keep_alive() {
    let limits = ResourceLimits {
        quic_path_keep_alive_interval: Duration::from_secs(10),
        quic_path_idle_timeout: Duration::from_secs(10),
        ..ResourceLimits::default()
    };

    assert_eq!(
        limits.validate(),
        Err(crate::performance::ResourceLimitError::QuicPathIdleTimeoutTooSmall)
    );
}

#[test]
fn app_config_maps_engine_resource_errors_to_product_config_errors() {
    assert_eq!(
        ConfigError::from(crate::performance::ResourceLimitError::QuicPathIdleTimeoutTooSmall),
        ConfigError::QuicPathIdleTimeoutTooSmall
    );
}

#[test]
fn default_dns_policy_is_explicit_system_resolution() {
    let compiled = DnsPolicyConfig::default().compile().expect("default DNS");
    assert!(compiled.uses_system_resolution());
    assert!(!compiled.is_encrypted_only());
    assert!(compiled.bootstrap_endpoints().next().is_none());
}

#[test]
fn dns_runtime_bounds_are_validated_by_the_product_owner() {
    let mut config = DnsPolicyConfig::default();
    config.spec.plans[0].limits.max_inflight = 0;
    assert!(matches!(
        config.compile(),
        Err(crate::product::DnsCompileError::InvalidPlanLimits(_))
    ));

    let mut config = DnsPolicyConfig::default();
    config.spec.plans[0].limits.positive_ttl_cap = Duration::ZERO;
    assert!(matches!(
        config.compile(),
        Err(crate::product::DnsCompileError::InvalidPlanLimits(_))
    ));
}
