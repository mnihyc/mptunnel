use super::*;

#[test]
fn mixed_ingress_classification_is_strict_and_non_consuming_by_design() {
    assert_eq!(
        classify_mixed_ingress_byte(0x05).expect("SOCKS5 byte"),
        MixedIngressProtocol::Socks5
    );
    assert_eq!(
        classify_mixed_ingress_byte(b'C').expect("HTTP CONNECT byte"),
        MixedIngressProtocol::HttpConnect
    );
    for unsupported in [0x04, b' ', b'G', b'p', 0x16, 0xff] {
        assert!(classify_mixed_ingress_byte(unsupported).is_err());
    }
}

#[test]
fn socks_udp_lane_identity_includes_selected_context_and_target() {
    let peer = SocketAddr::from(([127, 0, 0, 1], 40_000));
    let first = SocksUdpLaneKey {
        peer,
        target_slot: 0,
    };
    assert_ne!(
        first,
        SocksUdpLaneKey {
            peer,
            target_slot: 1,
        }
    );
}

#[test]
fn socks_udp_classifies_once_per_cached_target() {
    use crate::config::{
        ClientSecurityConfig, GatewayBalancerConfig, ProductPolicyConfig, ResourceLimits,
        SharedSecret,
    };
    use crate::product::{
        BalancerId, EgressAction, GatewayBalancerSpec, GatewayMemberSpec, GatewayStrategy,
        InitialDemand, NetworkSet, OutboundId, RouteAction, RouteMatchSpec, RouteRuleSpec, RuleId,
    };
    use crate::runtime::outbound_registry::{RuntimeOutboundLeaf, RuntimeOutboundRegistry};

    let security = || {
        ClientSecurityConfig::for_test(
            SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
        )
    };
    let context = |port: u16| {
        ClientPathContext::new(
            vec![format!("quic://127.0.0.1:{port}").parse().expect("path")],
            security(),
            ResourceLimits::default(),
        )
        .expect("context")
    };
    let first = context(7443);
    let second = context(8443);
    let first_id = OutboundId::parse("edge-a").expect("outbound");
    let second_id = OutboundId::parse("edge-b").expect("outbound");
    let gateway_id = BalancerId::parse("edge-gateway").expect("gateway");
    let product = ProductPolicyConfig {
        generation: 11,
        routes: vec![RouteRuleSpec::new(
            RuleId::parse("default").expect("rule"),
            RouteMatchSpec::default(),
            RouteAction::new(
                EgressAction::Balancer(gateway_id.clone()),
                None,
                InitialDemand::Automatic,
            ),
        )],
        destination_acl: Vec::new(),
    };
    let gateways = [GatewayBalancerConfig {
        id: gateway_id,
        generation: product.generation,
        spec: GatewayBalancerSpec::new(
            GatewayStrategy::RoundRobin,
            vec![
                GatewayMemberSpec::new(first_id, 1, NetworkSet::TCP_UDP),
                GatewayMemberSpec::new(second_id, 1, NetworkSet::TCP_UDP),
            ],
        ),
    }];
    let registry = RuntimeOutboundRegistry::compile(
        [
            RuntimeOutboundLeaf::Mpp {
                id: OutboundId::parse("edge-a").expect("outbound"),
                context: first,
                performance: MppPerformanceConfig::default(),
            },
            RuntimeOutboundLeaf::Mpp {
                id: OutboundId::parse("edge-b").expect("outbound"),
                context: second,
                performance: MppPerformanceConfig::default(),
            },
        ],
        &gateways,
        crate::runtime::outbound_registry::test_dns_generation(),
    )
    .expect("registry");
    let router = ClientIngressRouter::new(&product, registry).expect("router");
    let policy = SocksUdpAssociationPolicy {
        router,
        inbound: InboundId::parse("local-socks").expect("inbound"),
        source: "198.51.100.4:41000".parse().expect("source"),
        principal: PrincipalId::parse("anonymous").expect("principal"),
    };
    let first_target = TargetAddr::Domain {
        host: "first.example".to_string(),
        port: 443,
    };
    let second_target = TargetAddr::Domain {
        host: "second.example".to_string(),
        port: 443,
    };
    let mut routes = Vec::new();
    let first_slot = resolve_socks_udp_target_route(&mut routes, 8, &first_target, &policy)
        .expect("first route")
        .expect("route capacity");
    let repeated_slot = resolve_socks_udp_target_route(&mut routes, 8, &first_target, &policy)
        .expect("cached route")
        .expect("route capacity");
    let second_slot = resolve_socks_udp_target_route(&mut routes, 8, &second_target, &policy)
        .expect("second route")
        .expect("route capacity");

    assert_eq!(first_slot, repeated_slot);
    assert_eq!(routes.len(), 2);
    assert!(routes[first_slot].binding.is_some());
    assert!(routes[second_slot].binding.is_some());
}
