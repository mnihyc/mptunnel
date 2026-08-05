use super::*;
use crate::config::{ClientSecurityConfig, ProductPolicyConfig, ResourceLimits, SharedSecret};
use crate::ingress::tun::{ManagedVpnConfig, ManagedVpnPlatformConfig, TunHostConfig};
use crate::performance::MppPerformanceConfig;
use crate::platform::{LinuxPolicyConfig, RouteMode};
use crate::product::{
    CompiledDnsPolicy, DnsPlanId, DnsPlanSpec, DnsPolicySpec, DnsUpstreamEndpoint, DnsUpstreamId,
    DnsUpstreamSpec, EgressAction, FakeDnsSpec, InitialDemand, Network, OutboundId, PortRange,
    RouteAction, RouteMatchSpec, RouteRuleSpec, RuleId,
};
use crate::runtime::outbound_registry::{RuntimeOutboundLeaf, RuntimeOutboundRegistry};
use hickory_proto::op::{Message, MessageType, Query, ResponseCode};
use hickory_proto::rr::{Name, RData, RecordType};
use std::net::IpAddr;

fn context() -> ClientPathContext {
    ClientPathContext::new(
        vec!["udp://127.0.0.1:7443".parse().expect("path")],
        ClientSecurityConfig::for_test(
            SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
        ),
        ResourceLimits::default(),
    )
    .expect("context")
}

fn test_router() -> ClientIngressRouter {
    test_router_with_dns(crate::runtime::outbound_registry::test_dns_generation())
}

fn test_router_with_dns(dns: crate::dns::DnsGeneration) -> ClientIngressRouter {
    let id = OutboundId::parse("dns-edge").expect("outbound");
    let policy = ProductPolicyConfig {
        generation: 1,
        routes: vec![RouteRuleSpec::new(
            RuleId::parse("default").expect("rule"),
            RouteMatchSpec::default(),
            RouteAction::new(
                EgressAction::Outbound(id.clone()),
                None,
                InitialDemand::Automatic,
            ),
        )],
        destination_acl: Vec::new(),
    };
    let registry = RuntimeOutboundRegistry::compile(
        [RuntimeOutboundLeaf::Mpp {
            id,
            context: context(),
            performance: MppPerformanceConfig::default(),
        }],
        &[],
        dns,
    )
    .expect("registry");
    ClientIngressRouter::new(&policy, registry).expect("router")
}

fn managed_dns_tun() -> TunL4Config {
    TunL4Config {
        host: TunHostConfig::Managed(ManagedVpnConfig {
            route_mode: RouteMode::Full,
            excludes: Vec::new(),
            local_lan: true,
            dns_capture_servers: vec!["10.88.0.53".parse().expect("DNS capture")],
            platform: ManagedVpnPlatformConfig {
                linux: Some(LinuxPolicyConfig::default()),
            },
        }),
        ..TunL4Config::default()
    }
}

fn dns_wire_query(id: u16, name: &str, record_type: RecordType) -> Vec<u8> {
    let mut message = Message::query();
    message.metadata.id = id;
    message.add_query(Query::query(
        Name::from_ascii(name).expect("DNS name"),
        record_type,
    ));
    message.to_vec().expect("DNS query")
}

#[test]
fn managed_dns_capture_matches_only_configured_port_53_addresses() {
    let tun = managed_dns_tun();
    assert!(tun_dns_capture_target(
        "10.88.0.53:53".parse().expect("captured"),
        &tun
    ));
    assert!(!tun_dns_capture_target(
        "10.88.0.54:53".parse().expect("other resolver"),
        &tun
    ));
    assert!(!tun_dns_capture_target(
        "10.88.0.53:5353".parse().expect("other port"),
        &tun
    ));
    assert!(!tun_dns_capture_target(
        "10.88.0.53:53".parse().expect("external"),
        &TunL4Config::default()
    ));
}

#[tokio::test]
async fn managed_dns_tcp_capture_serves_bounded_framed_queries_locally() {
    let (mut client, server) = tokio::io::duplex(4_096);
    let service = tokio::spawn(serve_tun_dns_tcp(
        server,
        test_router(),
        Duration::from_secs(7),
    ));
    let request = dns_wire_query(0x5050, "localhost.", RecordType::A);
    client
        .write_all(&(request.len() as u16).to_be_bytes())
        .await
        .expect("query length");
    client.write_all(&request).await.expect("query");

    let response_length = client.read_u16().await.expect("response length");
    let mut response = vec![0u8; usize::from(response_length)];
    client
        .read_exact(&mut response)
        .await
        .expect("DNS response");
    let response = Message::from_vec(&response).expect("decoded DNS response");
    assert_eq!(response.metadata.id, 0x5050);
    assert_eq!(response.metadata.message_type, MessageType::Response);
    assert_eq!(response.metadata.response_code, ResponseCode::NoError);
    assert_eq!(response.answers.len(), 1);
    assert_eq!(response.answers[0].ttl, 7);
    assert!(matches!(
        &response.answers[0].data,
        RData::A(address) if address.0 == std::net::Ipv4Addr::LOCALHOST
    ));

    drop(client);
    service
        .await
        .expect("DNS TCP task")
        .expect("DNS TCP service");
}

#[tokio::test]
async fn tun_flow_recovers_fake_dns_domain_before_product_routing() {
    let upstream = DnsUpstreamId::parse("system").expect("upstream");
    let plan = DnsPlanId::parse("default").expect("plan");
    let policy = Arc::new(
        CompiledDnsPolicy::compile(
            9,
            DnsPolicySpec {
                upstreams: vec![DnsUpstreamSpec::direct(
                    upstream.clone(),
                    DnsUpstreamEndpoint::System,
                )],
                outbound_capabilities: Vec::new(),
                plans: vec![DnsPlanSpec::new(plan.clone(), vec![upstream])],
                rules: Vec::new(),
                hosts: Vec::new(),
                fake_dns: Some(FakeDnsSpec {
                    ipv4_pool: Some("198.18.0.0/24".parse().expect("pool")),
                    ipv6_pool: None,
                    max_entries: 32,
                    answer_ttl: Duration::from_secs(30),
                    recovery_ttl: Duration::from_secs(60),
                }),
                default_plan: plan,
            },
        )
        .expect("FakeDNS policy"),
    );
    let dns = crate::dns::DnsGeneration::compile(policy).expect("FakeDNS generation");
    let capture = dns
        .answer_wire_query(
            &dns_wire_query(0x5151, "video.example.", RecordType::A),
            Duration::from_secs(30),
            1_232,
        )
        .await
        .expect("FakeDNS answer");
    let capture = Message::from_vec(&capture).expect("decoded FakeDNS answer");
    let fake = match &capture.answers[0].data {
        RData::A(address) => IpAddr::V4(address.0),
        other => panic!("unexpected FakeDNS record {other:?}"),
    };
    let router = test_router_with_dns(dns);
    assert_eq!(
        router
            .recover_tun_target(SocketAddr::new(fake, 443))
            .expect("recovered target"),
        TargetAddr::Domain {
            host: "video.example".to_string(),
            port: 443,
        }
    );
    assert_eq!(
        router
            .recover_tun_target("192.0.2.20:443".parse().expect("ordinary target"))
            .expect("ordinary target"),
        TargetAddr::Ip("192.0.2.20:443".parse().expect("ordinary target"))
    );
}

#[test]
fn tun_udp_flow_routes_once_with_local_identity_and_effective_target() {
    let selected = context();
    let inbound = InboundId::parse("tun-main").expect("inbound");
    let principal = PrincipalId::parse("anonymous").expect("principal");
    let policy = ProductPolicyConfig {
        generation: 1,
        routes: vec![
            RouteRuleSpec::new(
                RuleId::parse("tun-dns").expect("rule"),
                RouteMatchSpec {
                    source_cidrs: vec!["10.0.0.0/8".parse().expect("CIDR")],
                    destination_ports: vec![PortRange::single(5353)],
                    networks: vec![Network::Udp],
                    inbounds: vec![inbound.clone()],
                    principals: vec![principal.clone()],
                    ..RouteMatchSpec::default()
                },
                RouteAction::new(
                    EgressAction::Outbound(OutboundId::parse("dns-edge").expect("outbound")),
                    None,
                    InitialDemand::Automatic,
                ),
            ),
            RouteRuleSpec::new(
                RuleId::parse("default").expect("rule"),
                RouteMatchSpec::default(),
                RouteAction::new(EgressAction::Reject, None, InitialDemand::Automatic),
            ),
        ],
        destination_acl: Vec::new(),
    };
    let registry = RuntimeOutboundRegistry::compile(
        [RuntimeOutboundLeaf::Mpp {
            id: OutboundId::parse("dns-edge").expect("outbound"),
            context: selected,
            performance: MppPerformanceConfig::default(),
        }],
        &[],
        crate::runtime::outbound_registry::test_dns_generation(),
    )
    .expect("registry");
    let router = ClientIngressRouter::new(&policy, registry).expect("router");
    let tun = TunL4Config {
        dns_resolvers: vec!["1.1.1.1:5353".parse().expect("resolver")],
        ..TunL4Config::default()
    };

    let binding = route_tun_udp_flow(
        &router,
        TunUdpFlowKey {
            local: "10.0.0.2:43000".parse().expect("local"),
            remote: "8.8.8.8:53".parse().expect("remote"),
        },
        &tun,
        principal.clone(),
        inbound.clone(),
    )
    .expect("route")
    .expect("selected binding");
    assert_eq!(
        binding.target,
        TargetAddr::Ip("1.1.1.1:5353".parse().expect("target"))
    );

    let denied = route_tun_udp_flow(
        &router,
        TunUdpFlowKey {
            local: "192.0.2.2:43000".parse().expect("local"),
            remote: "8.8.8.8:53".parse().expect("remote"),
        },
        &tun,
        principal,
        inbound,
    )
    .expect("deny route");
    assert!(denied.is_none());
}
