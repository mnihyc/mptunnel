use super::*;
use crate::config::{ClientSecurityConfig, GatewayBalancerConfig, ResourceLimits, SharedSecret};
use crate::ingress::ProxyAuthConfig;
use crate::outbound::{OutboundConfig, ProxyConfig};
use crate::performance::MppPerformanceConfig;
use crate::product::{
    BalancerId, DomainName, GatewayBalancerSpec, GatewayMemberSpec, GatewayStrategy, InitialDemand,
    NetworkSet, OutboundId, RouteAction, RouteMatchSpec, RouteRuleSpec, RouteStage, RuleId,
};
use crate::runtime::ingress_runtime::{
    handle_http_connect_client_stream_with_auth, handle_socks5_client_stream_with_auth,
    local_admission_permit_for_test,
};
use crate::runtime::outbound_registry::{RuntimeOutboundLeaf, RuntimeOutboundRegistry};
use crate::runtime::path::ClientPathContext;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

fn security() -> ClientSecurityConfig {
    ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    )
}

fn context(port: u16) -> ClientPathContext {
    ClientPathContext::new(
        vec![format!("udp://127.0.0.1:{port}").parse().expect("path")],
        security(),
        ResourceLimits::default(),
    )
    .expect("context")
}

fn registry(
    contexts: impl IntoIterator<Item = (&'static str, ClientPathContext)>,
    balancers: &[GatewayBalancerConfig],
) -> RuntimeOutboundRegistry {
    registry_with_dns(
        contexts,
        balancers,
        crate::runtime::outbound_registry::test_dns_generation(),
    )
}

fn registry_with_dns(
    contexts: impl IntoIterator<Item = (&'static str, ClientPathContext)>,
    balancers: &[GatewayBalancerConfig],
    dns: crate::dns::DnsGeneration,
) -> RuntimeOutboundRegistry {
    RuntimeOutboundRegistry::compile(
        contexts
            .into_iter()
            .map(|(id, context)| RuntimeOutboundLeaf::Mpp {
                id: OutboundId::parse(id).expect("outbound ID"),
                context,
                performance: MppPerformanceConfig::default(),
            }),
        balancers,
        dns,
    )
    .expect("runtime registry")
}

fn rule(id: &str, matcher: RouteMatchSpec, egress: EgressAction) -> RouteRuleSpec {
    RouteRuleSpec::new(
        RuleId::parse(id).expect("rule ID"),
        matcher,
        RouteAction::new(egress, None, InitialDemand::Automatic),
    )
}

fn policy(rules: Vec<RouteRuleSpec>) -> ProductPolicyConfig {
    ProductPolicyConfig {
        generation: 9,
        routes: rules,
        destination_acl: Vec::new(),
    }
}

fn source() -> SocketAddr {
    "198.51.100.8:41000".parse().expect("source")
}

fn inbound() -> InboundId {
    InboundId::parse("local-socks").expect("inbound")
}

fn anonymous() -> PrincipalId {
    PrincipalId::parse("anonymous").expect("principal")
}

#[tokio::test]
async fn round_robin_selects_independent_leaves_and_established_binding_stays_fixed() {
    let first_context = context(7443);
    let second_context = context(8443);
    let first_session = first_context.session_id;
    let second_session = second_context.session_id;
    let balancer_id = BalancerId::parse("all-edges").expect("balancer");
    let config = policy(vec![rule(
        "default",
        RouteMatchSpec::default(),
        EgressAction::Balancer(balancer_id.clone()),
    )]);
    let balancers = [GatewayBalancerConfig {
        id: balancer_id,
        generation: config.generation,
        spec: GatewayBalancerSpec::new(
            GatewayStrategy::RoundRobin,
            vec![
                GatewayMemberSpec::new(
                    OutboundId::parse("edge-a").expect("outbound"),
                    1,
                    NetworkSet::TCP_UDP,
                ),
                GatewayMemberSpec::new(
                    OutboundId::parse("edge-b").expect("outbound"),
                    1,
                    NetworkSet::TCP_UDP,
                ),
            ],
        ),
    }];
    let router = ClientIngressRouter::new(
        &config,
        registry(
            [("edge-a", first_context), ("edge-b", second_context)],
            &balancers,
        ),
    )
    .expect("router");

    let first_target = TargetAddr::Ip("8.8.8.8:443".parse().expect("first target"));
    let ClientRoute::Open(first) = router
        .route_udp(&first_target, source(), anonymous(), inbound())
        .expect("first route")
    else {
        panic!("expected first route");
    };
    let OpenedUdpOutbound::Mpp {
        context: first,
        gateway_lease: first_lease,
        ..
    } = first.open_udp(&first_target).await.expect("first open")
    else {
        panic!("expected MPP UDP leaf");
    };
    assert_eq!(first.session_id, first_session);
    assert!(first_lease.is_some());

    let second_target = TargetAddr::Ip("1.1.1.1:443".parse().expect("second target"));
    let ClientRoute::Open(second) = router
        .route_udp(&second_target, source(), anonymous(), inbound())
        .expect("second route")
    else {
        panic!("expected second route");
    };
    let OpenedUdpOutbound::Mpp {
        context: second, ..
    } = second.open_udp(&second_target).await.expect("second open")
    else {
        panic!("expected MPP UDP leaf");
    };
    assert_eq!(second.session_id, second_session);
    assert_eq!(
        first.session_id, first_session,
        "the first established binding is not migrated by later selection"
    );
}

#[tokio::test]
async fn udp_rules_select_their_own_context_and_datagram_traffic_class() {
    let udp_context = context(7443);
    let tcp_context = context(8443);
    let udp_session = udp_context.session_id;
    let config = policy(vec![
        rule(
            "udp",
            RouteMatchSpec {
                networks: vec![Network::Udp],
                ..RouteMatchSpec::default()
            },
            EgressAction::Outbound(OutboundId::parse("udp-edge").expect("outbound")),
        ),
        rule(
            "default",
            RouteMatchSpec::default(),
            EgressAction::Outbound(OutboundId::parse("tcp-edge").expect("outbound")),
        ),
    ]);
    let router = ClientIngressRouter::new(
        &config,
        registry([("udp-edge", udp_context), ("tcp-edge", tcp_context)], &[]),
    )
    .expect("router");
    let target = TargetAddr::Ip("8.8.4.4:443".parse().expect("target"));

    let ClientRoute::Open(selected) = router
        .route_udp(&target, source(), anonymous(), inbound())
        .expect("UDP route")
    else {
        panic!("expected UDP route");
    };
    let OpenedUdpOutbound::Mpp {
        context: selected,
        traffic_class,
        ..
    } = selected.open_udp(&target).await.expect("UDP open")
    else {
        panic!("expected MPP UDP leaf");
    };
    assert_eq!(selected.session_id, udp_session);
    assert_eq!(traffic_class, TrafficClass::RealtimeDatagram);
}

#[tokio::test]
async fn stable_domain_route_delegates_canonical_target_without_dns() {
    let edge = context(7443);
    let config = policy(vec![rule(
        "default",
        RouteMatchSpec::default(),
        EgressAction::Outbound(OutboundId::parse("edge").expect("outbound")),
    )]);
    let router = ClientIngressRouter::new(
        &config,
        registry_with_dns(
            [("edge", edge)],
            &[],
            crate::dns::DnsGeneration::from_test_answers(HashMap::new()),
        ),
    )
    .expect("router");
    let target = TargetAddr::Domain {
        host: "ExAmPlE.COM".to_string(),
        port: 443,
    };

    let ClientRoute::Open(plan) = router
        .route_udp(&target, source(), anonymous(), inbound())
        .expect("domain route")
    else {
        panic!("expected an open plan");
    };
    let OpenedUdpOutbound::Mpp {
        target: delegated, ..
    } = plan.open_udp(&target).await.expect("domain delegation")
    else {
        panic!("expected MPP UDP outbound");
    };

    assert_eq!(
        delegated,
        TargetAddr::Domain {
            host: "example.com".to_string(),
            port: 443,
        }
    );
}

#[tokio::test]
async fn post_dns_routing_authorizes_the_complete_answer_and_opens_only_a_literal_action() {
    let routed_context = context(8443);
    let routed_session = routed_context.session_id;
    let routed_id = OutboundId::parse("routed-edge").expect("outbound");
    let config = policy(vec![
        rule(
            "skip-first-address",
            RouteMatchSpec {
                destination_cidrs: vec!["8.8.8.0/24".parse().expect("CIDR")],
                stages: vec![RouteStage::PostResolution],
                ..RouteMatchSpec::default()
            },
            EgressAction::Reject,
        ),
        RouteRuleSpec::new(
            RuleId::parse("route-second-address").expect("rule"),
            RouteMatchSpec {
                destination_cidrs: vec!["1.1.1.0/24".parse().expect("CIDR")],
                stages: vec![RouteStage::PostResolution],
                ..RouteMatchSpec::default()
            },
            RouteAction::new(
                EgressAction::Outbound(routed_id.clone()),
                None,
                InitialDemand::Throughput,
            ),
        ),
        rule("default", RouteMatchSpec::default(), EgressAction::Reject),
    ]);
    let dns = crate::dns::DnsGeneration::from_test_answers(HashMap::from([
        (
            "post-route.example".to_string(),
            vec![
                "8.8.8.8".parse().expect("first answer"),
                "1.1.1.1".parse().expect("second answer"),
            ],
        ),
        (
            "mixed-safety.example".to_string(),
            vec![
                "1.1.1.1".parse().expect("public answer"),
                "127.0.0.1".parse().expect("restricted answer"),
            ],
        ),
        (
            "denied.example".to_string(),
            vec!["8.8.8.8".parse().expect("denied answer")],
        ),
    ]));
    let router = ClientIngressRouter::new(
        &config,
        registry_with_dns([("routed-edge", routed_context)], &[], dns),
    )
    .expect("router");

    let target = TargetAddr::Domain {
        host: "post-route.example".to_string(),
        port: 443,
    };
    let ClientRoute::Open(plan) = router
        .route_udp(&target, source(), anonymous(), inbound())
        .expect("pre-resolution route")
    else {
        panic!("expected an open plan");
    };
    let OpenedUdpOutbound::Mpp {
        context: selected,
        target: routed_target,
        traffic_class,
        ..
    } = plan.open_udp(&target).await.expect("post-resolution open")
    else {
        panic!("expected MPP UDP outbound");
    };
    assert_eq!(selected.session_id, routed_session);
    assert_eq!(
        routed_target,
        TargetAddr::Ip("1.1.1.1:443".parse().expect("literal target"))
    );
    assert_eq!(traffic_class, TrafficClass::Throughput);

    let mixed_safety = TargetAddr::Domain {
        host: "mixed-safety.example".to_string(),
        port: 443,
    };
    let ClientRoute::Open(plan) = router
        .route_udp(&mixed_safety, source(), anonymous(), inbound())
        .expect("pre-resolution route")
    else {
        panic!("expected an open plan");
    };
    assert!(matches!(
        plan.open_udp(&mixed_safety).await,
        Err(RuntimeError::DestinationDenied(_))
    ));

    let denied = TargetAddr::Domain {
        host: "denied.example".to_string(),
        port: 443,
    };
    let ClientRoute::Open(plan) = router
        .route_udp(&denied, source(), anonymous(), inbound())
        .expect("provisional pre-resolution route")
    else {
        panic!("expected an open plan");
    };
    assert!(matches!(
        plan.open_udp(&denied).await,
        Err(RuntimeError::RouteRejected)
    ));
}

#[tokio::test]
async fn post_resolution_route_groups_keep_independent_member_deadlines() {
    let blackhole = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("blackhole proxy bind");
    let blackhole_addr = blackhole.local_addr().expect("blackhole proxy address");
    let working = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("working proxy bind");
    let working_addr = working.local_addr().expect("working proxy address");
    let first_target = TargetAddr::Ip("8.8.8.8:443".parse().expect("first target"));
    let second_target = TargetAddr::Ip("1.1.1.1:443".parse().expect("second target"));
    let first_expected = first_target.clone();
    let second_expected = second_target.clone();
    let blackhole_task = tokio::spawn(async move {
        let (mut stream, _) = blackhole.accept().await.expect("blackhole accept");
        let mut greeting = [0_u8; 3];
        stream
            .read_exact(&mut greeting)
            .await
            .expect("blackhole greeting");
        assert_eq!(greeting, crate::outbound::socks5::no_auth_greeting());
        stream
            .write_all(&[0x05, 0x00])
            .await
            .expect("blackhole method");
        let expected =
            crate::outbound::socks5::connect_request(&first_expected).expect("first request");
        let mut request = vec![0_u8; expected.len()];
        stream
            .read_exact(&mut request)
            .await
            .expect("blackhole request");
        assert_eq!(request, expected);
        let mut remainder = Vec::new();
        stream
            .read_to_end(&mut remainder)
            .await
            .expect("first route-group timeout closes");
    });
    let working_task = tokio::spawn(async move {
        let (mut stream, _) = working.accept().await.expect("working accept");
        let mut greeting = [0_u8; 3];
        stream
            .read_exact(&mut greeting)
            .await
            .expect("working greeting");
        assert_eq!(greeting, crate::outbound::socks5::no_auth_greeting());
        stream
            .write_all(&[0x05, 0x00])
            .await
            .expect("working method");
        let expected =
            crate::outbound::socks5::connect_request(&second_expected).expect("second request");
        let mut request = vec![0_u8; expected.len()];
        stream
            .read_exact(&mut request)
            .await
            .expect("working request");
        assert_eq!(request, expected);
        stream
            .write_all(&[0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1, 0, 0])
            .await
            .expect("working success");
    });

    let first = OutboundId::parse("first-route-proxy").expect("first outbound");
    let second = OutboundId::parse("second-route-proxy").expect("second outbound");
    let routes = vec![
        rule(
            "first-address",
            RouteMatchSpec {
                destination_cidrs: vec!["8.8.8.0/24".parse().expect("first CIDR")],
                stages: vec![RouteStage::PostResolution],
                ..RouteMatchSpec::default()
            },
            EgressAction::Outbound(first.clone()),
        ),
        rule(
            "second-address",
            RouteMatchSpec {
                destination_cidrs: vec!["1.1.1.0/24".parse().expect("second CIDR")],
                stages: vec![RouteStage::PostResolution],
                ..RouteMatchSpec::default()
            },
            EgressAction::Outbound(second.clone()),
        ),
        rule("default", RouteMatchSpec::default(), EgressAction::Reject),
    ];
    let dns = crate::dns::DnsGeneration::from_test_answers(HashMap::from([(
        "route-group.example".to_string(),
        vec![
            "8.8.8.8".parse().expect("first IP"),
            "1.1.1.1".parse().expect("second IP"),
        ],
    )]));
    let leaf = |id: OutboundId, endpoint: SocketAddr| RuntimeOutboundLeaf::Local {
        id,
        config: OutboundConfig::Socks5(ProxyConfig::new(
            endpoint
                .to_string()
                .parse()
                .expect("literal proxy endpoint"),
            None,
        )),
        connect_timeout: Duration::from_millis(500),
        native_sockets: Arc::new(crate::transport::SystemNativeSocketConfigurator),
    };
    let registry = RuntimeOutboundRegistry::compile(
        [leaf(first, blackhole_addr), leaf(second, working_addr)],
        &[],
        dns,
    )
    .expect("post-resolution registry");
    let router =
        ClientIngressRouter::new(&policy(routes), registry).expect("post-resolution router");
    let target = TargetAddr::Domain {
        host: "route-group.example".to_string(),
        port: 443,
    };
    let ClientRoute::Open(plan) = router
        .route_tcp(&target, source(), anonymous(), inbound())
        .expect("route plan")
    else {
        panic!("expected an open route plan");
    };
    let OpenedTcpOutbound::Local { .. } = plan
        .open_tcp(&target)
        .await
        .expect("second post-resolution route group")
    else {
        panic!("expected the working local proxy");
    };
    blackhole_task.await.expect("blackhole task");
    working_task.await.expect("working task");
}

#[test]
fn deny_actions_finish_before_target_lookup_or_acl_open_authority() {
    let config = policy(vec![
        rule(
            "drop",
            RouteMatchSpec {
                domain_exact: vec![DomainName::parse("drop.example").expect("domain")],
                ..RouteMatchSpec::default()
            },
            EgressAction::Drop,
        ),
        rule("reject", RouteMatchSpec::default(), EgressAction::Reject),
    ]);
    let router = ClientIngressRouter::new(&config, registry([], &[]))
        .expect("router without outbound bindings");
    for (host, expected) in [
        ("drop.example", ClientPolicyDisposition::Drop),
        ("other.example", ClientPolicyDisposition::Reject),
    ] {
        let target = TargetAddr::Domain {
            host: host.to_string(),
            port: 443,
        };
        assert!(matches!(
            router
                .route_tcp(&target, source(), anonymous(), inbound())
                .expect("deny route"),
            ClientRoute::Deny(actual) if actual == expected
        ));
        assert!(matches!(
            router
                .route_udp(&target, source(), anonymous(), inbound())
                .expect("UDP deny route"),
            ClientRoute::Deny(actual) if actual == expected
        ));
    }
}

#[test]
fn safe_acl_denies_restricted_literal_before_mpp_open() {
    let context = context(7443);
    let config = policy(vec![rule(
        "default",
        RouteMatchSpec::default(),
        EgressAction::Outbound(OutboundId::parse("edge").expect("outbound")),
    )]);
    let router =
        ClientIngressRouter::new(&config, registry([("edge", context)], &[])).expect("router");
    let target = TargetAddr::Ip("127.0.0.1:443".parse().expect("target"));
    assert!(matches!(
        router.route_tcp(&target, source(), anonymous(), inbound()),
        Err(RuntimeError::DestinationDenied(_))
    ));
}

#[tokio::test]
async fn socks5_post_resolution_reject_returns_connection_not_allowed() {
    let edge = context(8443);
    let edge_id = OutboundId::parse("allowlisted-edge").expect("outbound");
    let config = policy(vec![
        rule(
            "allowlisted-address",
            RouteMatchSpec {
                destination_cidrs: vec!["1.1.1.0/24".parse().expect("CIDR")],
                stages: vec![RouteStage::PostResolution],
                ..RouteMatchSpec::default()
            },
            EgressAction::Outbound(edge_id),
        ),
        rule("default", RouteMatchSpec::default(), EgressAction::Reject),
    ]);
    let router = ClientIngressRouter::new(
        &config,
        registry_with_dns(
            [("allowlisted-edge", edge)],
            &[],
            crate::dns::DnsGeneration::from_test_answers(HashMap::from([(
                "example.com".to_string(),
                vec!["8.8.8.8".parse().expect("non-allowlisted answer")],
            )])),
        ),
    )
    .expect("router");
    let udp_context = context(7443);
    let (mut client, server) = tokio::io::duplex(1024);
    let task = tokio::spawn(handle_socks5_client_stream_with_auth(
        server,
        udp_context.mux_limits,
        router,
        inbound(),
        source(),
        ProxyAuthConfig::disabled(),
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        local_admission_permit_for_test(IpAddr::V4(Ipv4Addr::LOCALHOST)),
    ));
    client
        .write_all(&[
            0x05, 0x01, 0x00, // no-auth negotiation
            0x05, 0x01, 0x00, 0x03, 0x0b, b'e', b'x', b'a', b'm', b'p', b'l', b'e', b'.', b'c',
            b'o', b'm', 0x01, 0xbb,
        ])
        .await
        .expect("request");
    let mut response = Vec::new();
    client.read_to_end(&mut response).await.expect("response");
    task.await.expect("handler task").expect("policy reject");
    assert_eq!(&response[..2], &[0x05, 0x00]);
    assert_eq!(response[3], 0x02);

    let config = policy(vec![rule(
        "default",
        RouteMatchSpec::default(),
        EgressAction::Drop,
    )]);
    let router = ClientIngressRouter::new(&config, registry([], &[])).expect("router");
    let udp_context = context(7443);
    let (mut client, server) = tokio::io::duplex(1024);
    let task = tokio::spawn(handle_socks5_client_stream_with_auth(
        server,
        udp_context.mux_limits,
        router,
        inbound(),
        source(),
        ProxyAuthConfig::disabled(),
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        local_admission_permit_for_test(IpAddr::V4(Ipv4Addr::LOCALHOST)),
    ));
    client
        .write_all(&[
            0x05, 0x01, 0x00, // no-auth negotiation
            0x05, 0x01, 0x00, 0x03, 0x0b, b'e', b'x', b'a', b'm', b'p', b'l', b'e', b'.', b'c',
            b'o', b'm', 0x01, 0xbb,
        ])
        .await
        .expect("drop request");
    let mut negotiation = [0u8; 2];
    client
        .read_exact(&mut negotiation)
        .await
        .expect("method negotiation");
    assert_eq!(negotiation, [0x05, 0x00]);
    tokio::task::yield_now().await;
    assert!(
        !task.is_finished(),
        "drop must retain the accepted connection without a CONNECT reply"
    );
    drop(client);
    task.await.expect("drop handler task").expect("policy drop");
}

#[tokio::test]
async fn http_connect_reject_returns_forbidden_without_mpp_open() {
    let config = policy(vec![rule(
        "default",
        RouteMatchSpec::default(),
        EgressAction::Reject,
    )]);
    let router = ClientIngressRouter::new(&config, registry([], &[])).expect("router");
    let (mut client, server) = tokio::io::duplex(1024);
    let task = tokio::spawn(handle_http_connect_client_stream_with_auth(
        server,
        router,
        InboundId::parse("local-http").expect("inbound"),
        source(),
        ProxyAuthConfig::disabled(),
        local_admission_permit_for_test(IpAddr::V4(Ipv4Addr::LOCALHOST)),
    ));
    client
        .write_all(b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com\r\n\r\n")
        .await
        .expect("request");
    let mut response = Vec::new();
    client.read_to_end(&mut response).await.expect("response");
    task.await.expect("handler task").expect("policy reject");
    assert!(response.starts_with(b"HTTP/1.1 403 Forbidden\r\n"));

    let config = policy(vec![rule(
        "default",
        RouteMatchSpec::default(),
        EgressAction::Drop,
    )]);
    let router = ClientIngressRouter::new(&config, registry([], &[])).expect("router");
    let (mut client, server) = tokio::io::duplex(1024);
    let task = tokio::spawn(handle_http_connect_client_stream_with_auth(
        server,
        router,
        InboundId::parse("local-http").expect("inbound"),
        source(),
        ProxyAuthConfig::disabled(),
        local_admission_permit_for_test(IpAddr::V4(Ipv4Addr::LOCALHOST)),
    ));
    client
        .write_all(b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com\r\n\r\n")
        .await
        .expect("drop request");
    tokio::task::yield_now().await;
    assert!(
        !task.is_finished(),
        "drop must retain the accepted connection without an HTTP response"
    );
    drop(client);
    task.await.expect("drop handler task").expect("policy drop");
}
