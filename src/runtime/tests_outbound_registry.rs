use super::*;
use crate::config::{ClientSecurityConfig, ResourceLimits, SharedSecret};
use crate::outbound::{HttpsProxyConfig, ProxyConfig};
use crate::product::{
    CompiledDnsPolicy, DnsIpStrategy, DnsOutboundCapabilitySpec, DnsPlanId, DnsPlanSpec,
    DnsPolicySpec, DnsUpstreamEndpoint, DnsUpstreamId, DnsUpstreamSpec, GatewayBalancerSpec,
    GatewayMemberSpec, GatewayStrategy, ProductAdmissionConfig, ProductAdmissionRejection,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};

fn local_leaf_with_timeout(
    id: &str,
    config: OutboundConfig,
    connect_timeout: Duration,
) -> RuntimeOutboundLeaf {
    RuntimeOutboundLeaf::Local {
        id: OutboundId::parse(id).expect("outbound ID"),
        config,
        connect_timeout,
        native_sockets: Arc::new(crate::transport::SystemNativeSocketConfigurator),
    }
}

fn local_leaf(id: &str, config: OutboundConfig) -> RuntimeOutboundLeaf {
    local_leaf_with_timeout(id, config, Duration::from_millis(250))
}

fn mpp_context(port: u16) -> ClientPathContext {
    ClientPathContext::new(
        vec![
            format!("quic://127.0.0.1:{port}")
                .parse()
                .expect("MPP path"),
        ],
        ClientSecurityConfig::for_test(
            SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("test secret"),
        ),
        ResourceLimits::default(),
    )
    .expect("MPP context")
}

fn mpp_leaf(id: &str, context: ClientPathContext) -> RuntimeOutboundLeaf {
    RuntimeOutboundLeaf::Mpp {
        id: OutboundId::parse(id).expect("outbound ID"),
        context,
        performance: MppPerformanceConfig::default(),
    }
}

fn selection(registry: &RuntimeOutboundRegistry, id: &str) -> EgressSelection {
    registry
        .selection_for_egress(&EgressRef::Outbound(
            OutboundId::parse(id).expect("outbound ID"),
        ))
        .expect("outbound selection")
}

fn registry_with_product_admission(
    leaves: impl IntoIterator<Item = RuntimeOutboundLeaf>,
    admission: ProductAdmission,
) -> RuntimeOutboundRegistry {
    RuntimeOutboundRegistryShell::compile(leaves, &[])
        .expect("outbound shell")
        .with_product_admission(admission)
        .with_dns(test_dns_generation())
}

fn one_flow_admission() -> ProductAdmission {
    ProductAdmission::new(ProductAdmissionConfig {
        max_live_flows: 1,
        max_concurrent_work: 1,
        max_live_flows_per_principal: 1,
        max_live_flows_per_outbound: 1,
        max_connects_per_outbound: 1,
        max_live_flows_per_target: 1,
        max_connects_per_target: 1,
        max_dns_work: 1,
    })
    .expect("one-flow Product admission")
}

#[tokio::test]
async fn new_flow_admission_distinguishes_initial_establishment_from_outage() {
    let target = TargetAddr::Ip("192.0.2.1:443".parse().expect("target"));
    let policy = crate::outbound::ServerDestinationPolicy::allow_restricted_for_test()
        .test_principal_policy();

    let initial = mpp_context(7443);
    let initial_registry = RuntimeOutboundRegistry::compile(
        [mpp_leaf("initial", initial)],
        &[],
        test_dns_generation(),
    )
    .expect("initial registry");
    let initial_selection = selection(&initial_registry, "initial");
    assert!(matches!(
        initial_registry
            .open_udp(&initial_selection, &target, None, &policy)
            .await,
        Ok(OpenedUdpOutbound::Mpp { .. })
    ));

    let offline = mpp_context(8443);
    let authenticated = offline.authenticated_carriers.register();
    assert_eq!(
        offline.authenticated_carriers.snapshot().availability(),
        AuthenticatedCarrierAvailability::Available
    );
    drop(authenticated);
    assert_eq!(
        offline.authenticated_carriers.snapshot().availability(),
        AuthenticatedCarrierAvailability::Offline
    );
    let offline_registry = RuntimeOutboundRegistry::compile(
        [mpp_leaf("offline", offline)],
        &[],
        test_dns_generation(),
    )
    .expect("offline registry");
    let offline_selection = selection(&offline_registry, "offline");
    assert!(matches!(
        offline_registry
            .open_udp(&offline_selection, &target, None, &policy)
            .await,
        Err(RuntimeError::OutboundUnavailable(id)) if id.as_str() == "offline"
    ));
}

#[tokio::test]
async fn balancer_skips_offline_mpp_without_masking_native_availability() {
    let offline = mpp_context(7443);
    drop(offline.authenticated_carriers.register());
    let offline_id = OutboundId::parse("offline-edge").expect("outbound ID");
    let direct_id = OutboundId::parse("direct-edge").expect("outbound ID");
    let balancer_id = BalancerId::parse("daily-egress").expect("balancer ID");
    let balancers = [GatewayBalancerConfig {
        id: balancer_id.clone(),
        generation: 1,
        spec: GatewayBalancerSpec::new(
            GatewayStrategy::OrderedFailover,
            vec![
                GatewayMemberSpec::new(offline_id.clone(), 1, NetworkSet::TCP_UDP),
                GatewayMemberSpec::new(direct_id.clone(), 1, NetworkSet::TCP_UDP),
            ],
        ),
    }];
    let registry = RuntimeOutboundRegistry::compile(
        [
            mpp_leaf(offline_id.as_str(), offline),
            local_leaf(direct_id.as_str(), OutboundConfig::Direct),
        ],
        &balancers,
        test_dns_generation(),
    )
    .expect("mixed registry");
    let target = UdpSocket::bind("127.0.0.1:0").await.expect("UDP target");
    let target = TargetAddr::Ip(target.local_addr().expect("target address"));
    let policy = crate::outbound::ServerDestinationPolicy::allow_restricted_for_test()
        .test_principal_policy();
    assert!(matches!(
        registry
            .open_udp(
                &EgressSelection::Balancer(balancer_id),
                &target,
                None,
                &policy,
            )
            .await,
        Ok(OpenedUdpOutbound::Local { .. })
    ));
}

#[tokio::test]
async fn balancer_pre_excludes_member_without_matching_bind_family() {
    let v6_only_id = OutboundId::parse("v6-only").expect("outbound ID");
    let fallback_id = OutboundId::parse("fallback-v4").expect("outbound ID");
    let balancer_id = BalancerId::parse("family-aware").expect("balancer ID");
    let balancers = [GatewayBalancerConfig {
        id: balancer_id.clone(),
        generation: 1,
        spec: GatewayBalancerSpec::new(
            GatewayStrategy::OrderedFailover,
            vec![
                GatewayMemberSpec::new(v6_only_id.clone(), 1, NetworkSet::TCP_UDP),
                GatewayMemberSpec::new(fallback_id.clone(), 1, NetworkSet::TCP_UDP),
            ],
        ),
    }];
    let registry = RuntimeOutboundRegistry::compile(
        [
            local_leaf(
                v6_only_id.as_str(),
                OutboundConfig::BindSourceIps {
                    ipv4: None,
                    ipv6: Some("2001:db8::2".parse().expect("IPv6 source")),
                },
            ),
            mpp_leaf(fallback_id.as_str(), mpp_context(9443)),
        ],
        &balancers,
        test_dns_generation(),
    )
    .expect("family-aware registry");
    let target = TargetAddr::Ip("8.8.8.8:443".parse().expect("target address"));
    let policy = crate::outbound::ServerDestinationPolicy::allow_restricted_for_test()
        .test_principal_policy();
    assert!(matches!(
        registry
            .open_udp(
                &EgressSelection::Balancer(balancer_id),
                &target,
                None,
                &policy,
            )
            .await,
        Ok(OpenedUdpOutbound::Mpp { .. })
    ));
    let snapshots = registry
        .gateway_control()
        .snapshots()
        .expect("balancer snapshot");
    assert_eq!(snapshots[0].runtime.members[0].counters.selections, 0);
    assert_eq!(snapshots[0].runtime.members[0].counters.open_attempts, 0);
    assert_eq!(snapshots[0].runtime.members[1].counters.selections, 1);
    assert_eq!(snapshots[0].runtime.members[1].counters.open_attempts, 1);
}

#[test]
fn complementary_family_balancer_resolves_before_member_selection() {
    let v4_id = OutboundId::parse("v4-only").expect("outbound ID");
    let v6_id = OutboundId::parse("v6-only").expect("outbound ID");
    let balancer_id = BalancerId::parse("family-pair").expect("balancer ID");
    let balancers = [GatewayBalancerConfig {
        id: balancer_id.clone(),
        generation: 1,
        spec: GatewayBalancerSpec::new(
            GatewayStrategy::OrderedFailover,
            vec![
                GatewayMemberSpec::new(v4_id.clone(), 1, NetworkSet::TCP_UDP),
                GatewayMemberSpec::new(v6_id.clone(), 1, NetworkSet::TCP_UDP),
            ],
        ),
    }];
    let registry = RuntimeOutboundRegistry::compile(
        [
            local_leaf(
                v4_id.as_str(),
                OutboundConfig::BindSourceIps {
                    ipv4: Some("198.51.100.2".parse().expect("IPv4 source")),
                    ipv6: None,
                },
            ),
            local_leaf(
                v6_id.as_str(),
                OutboundConfig::BindSourceIps {
                    ipv4: None,
                    ipv6: Some("2001:db8::2".parse().expect("IPv6 source")),
                },
            ),
        ],
        &balancers,
        test_dns_generation(),
    )
    .expect("family-pair registry");

    assert!(
        registry
            .action_requires_family_resolution(&EgressAction::Balancer(balancer_id))
            .expect("family-resolution requirement")
    );
}

#[test]
fn active_probe_skips_family_ineligible_member_without_health_feedback() {
    let v6_id = OutboundId::parse("v6-only-probe").expect("outbound ID");
    let balancer_id = BalancerId::parse("probe-family").expect("balancer ID");
    let mut spec = GatewayBalancerSpec::new(
        GatewayStrategy::OrderedFailover,
        vec![GatewayMemberSpec::new(
            v6_id.clone(),
            1,
            NetworkSet::TCP_UDP,
        )],
    );
    spec.probe = Some(crate::product::GatewayProbePolicy {
        target: ProtocolTarget::parse_authority("192.0.2.1:443").expect("probe target"),
        interval: Duration::from_millis(100),
        timeout: Duration::from_millis(20),
    });
    let registry = RuntimeOutboundRegistry::compile(
        [local_leaf(
            v6_id.as_str(),
            OutboundConfig::BindSourceIps {
                ipv4: None,
                ipv6: Some("2001:db8::2".parse().expect("IPv6 source")),
            },
        )],
        &[GatewayBalancerConfig {
            id: balancer_id,
            generation: 1,
            spec,
        }],
        test_dns_generation(),
    )
    .expect("probe-family registry");
    let runtime = registry
        .shell
        .balancers
        .values()
        .next()
        .expect("balancer runtime");
    let target = runtime.probe_policy().expect("probe policy").target.clone();

    assert!(
        !registry
            .gateway_member_supports_probe_target(&v6_id, &target)
            .expect("probe eligibility")
    );
    let snapshot = runtime.snapshot().expect("balancer snapshot");
    let member = &snapshot.members[0];
    assert!(!member.probe_in_flight);
    assert_eq!(member.consecutive_failures, 0);
    assert_eq!(member.counters.probes, 0);
    assert_eq!(member.counters.probe_failures, 0);
    assert!(member.last_error.is_none());
}

#[tokio::test]
async fn temporarily_unassigned_source_is_a_flow_error_and_runtime_remains_usable() {
    let registry = RuntimeOutboundRegistry::compile(
        [
            local_leaf(
                "temporarily-down",
                OutboundConfig::BindSourceIps {
                    ipv4: Some("198.51.100.254".parse().expect("unassigned source")),
                    ipv6: None,
                },
            ),
            mpp_leaf("available-mpp", mpp_context(10443)),
        ],
        &[],
        test_dns_generation(),
    )
    .expect("runtime registry");
    let target = TargetAddr::Ip("8.8.8.8:443".parse().expect("target address"));
    let policy = crate::outbound::ServerDestinationPolicy::allow_restricted_for_test()
        .test_principal_policy();

    let unavailable = selection(&registry, "temporarily-down");
    assert!(matches!(
        registry
            .open_udp(&unavailable, &target, None, &policy)
            .await,
        Err(RuntimeError::OutboundConnect(_))
    ));

    let available = selection(&registry, "available-mpp");
    assert!(matches!(
        registry.open_udp(&available, &target, None, &policy).await,
        Ok(OpenedUdpOutbound::Mpp { .. })
    ));
}

#[tokio::test]
async fn all_offline_balancer_members_return_typed_unavailability() {
    let first = mpp_context(7443);
    let second = mpp_context(8443);
    drop(first.authenticated_carriers.register());
    drop(second.authenticated_carriers.register());
    let first_id = OutboundId::parse("edge-a").expect("outbound ID");
    let second_id = OutboundId::parse("edge-b").expect("outbound ID");
    let balancer_id = BalancerId::parse("daily-egress").expect("balancer ID");
    let balancers = [GatewayBalancerConfig {
        id: balancer_id.clone(),
        generation: 1,
        spec: GatewayBalancerSpec::new(
            GatewayStrategy::OrderedFailover,
            vec![
                GatewayMemberSpec::new(first_id.clone(), 1, NetworkSet::TCP_UDP),
                GatewayMemberSpec::new(second_id.clone(), 1, NetworkSet::TCP_UDP),
            ],
        ),
    }];
    let registry = RuntimeOutboundRegistry::compile(
        [
            mpp_leaf(first_id.as_str(), first),
            mpp_leaf(second_id.as_str(), second),
        ],
        &balancers,
        test_dns_generation(),
    )
    .expect("offline registry");
    let target = TargetAddr::Ip("192.0.2.1:443".parse().expect("target"));
    let policy = crate::outbound::ServerDestinationPolicy::allow_restricted_for_test()
        .test_principal_policy();
    assert!(matches!(
        registry
            .open_udp(
                &EgressSelection::Balancer(balancer_id),
                &target,
                None,
                &policy,
            )
            .await,
        Err(RuntimeError::OutboundUnavailable(_))
    ));
}

#[tokio::test]
async fn runtime_product_admission_precedes_target_io_and_recovers_after_close() {
    let first_target = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("first target");
    let first_address = first_target.local_addr().expect("first target address");
    let second_target = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("second target");
    let second_address = second_target.local_addr().expect("second target address");
    let admission = one_flow_admission();
    let registry = registry_with_product_admission(
        [local_leaf("direct", OutboundConfig::Direct)],
        admission.clone(),
    );
    let selection = selection(&registry, "direct");
    let policy = crate::outbound::ServerDestinationPolicy::allow_restricted_for_test()
        .test_principal_policy();

    let first = registry
        .open_tcp(
            &selection,
            &TargetAddr::Ip(first_address),
            None,
            TrafficClass::Latency,
            &policy,
        )
        .await
        .expect("first admitted TCP flow");
    assert_eq!(admission.snapshot().live_flows, 1);
    assert!(matches!(
        registry
            .open_tcp(
                &selection,
                &TargetAddr::Ip(second_address),
                None,
                TrafficClass::Latency,
                &policy,
            )
            .await,
        Err(RuntimeError::ProductAdmission(error))
            if error.rejection() == ProductAdmissionRejection::GlobalLiveFlows
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), second_target.accept())
            .await
            .is_err(),
        "rejected flow reached target I/O"
    );

    drop(first);
    assert_eq!(admission.snapshot().live_flows, 0);
    let recovered = registry
        .open_tcp(
            &selection,
            &TargetAddr::Ip(second_address),
            None,
            TrafficClass::Latency,
            &policy,
        )
        .await
        .expect("admission recovered after close");
    second_target.accept().await.expect("recovered target I/O");
    drop(recovered);
    let snapshot = admission.snapshot();
    assert_eq!(snapshot.live_flows, 0);
    assert_eq!(snapshot.concurrent_work, 0);
    assert!(snapshot.principals.is_empty());
    assert!(snapshot.outbounds.is_empty());
    assert!(snapshot.targets.is_empty());
}

#[tokio::test]
async fn cancelled_outbound_open_releases_every_product_counter() {
    let proxy = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("SOCKS proxy listener");
    let proxy_address = proxy.local_addr().expect("SOCKS proxy address");
    let admission = one_flow_admission();
    let registry = registry_with_product_admission(
        [local_leaf(
            "proxy",
            OutboundConfig::Socks5(ProxyConfig::new(
                proxy_address.to_string().parse().expect("proxy endpoint"),
                None,
            )),
        )],
        admission.clone(),
    );
    let selection = selection(&registry, "proxy");
    let policy = crate::outbound::ServerDestinationPolicy::allow_restricted_for_test()
        .test_principal_policy();
    let opener = tokio::spawn(async move {
        registry
            .open_tcp(
                &selection,
                &TargetAddr::Ip("192.0.2.1:443".parse().expect("target")),
                None,
                TrafficClass::Latency,
                &policy,
            )
            .await
    });
    let (_stalled_proxy_stream, _) = proxy.accept().await.expect("proxy open reached");
    let snapshot = admission.snapshot();
    assert_eq!(snapshot.live_flows, 1);
    assert_eq!(snapshot.concurrent_work, 1);
    assert_eq!(snapshot.outbounds[0].connecting, 1);
    assert_eq!(snapshot.targets[0].connecting, 1);

    opener.abort();
    match opener.await {
        Err(error) if error.is_cancelled() => {}
        Err(error) => panic!("open task failed instead of cancelling: {error}"),
        Ok(_) => panic!("open task completed instead of cancelling"),
    }
    let snapshot = admission.snapshot();
    assert_eq!(snapshot.live_flows, 0);
    assert_eq!(snapshot.concurrent_work, 0);
    assert!(snapshot.principals.is_empty());
    assert!(snapshot.outbounds.is_empty());
    assert!(snapshot.targets.is_empty());
}

#[tokio::test]
async fn local_only_registry_opens_concrete_tcp_and_udp_without_mpp_context() {
    let tcp_target = TcpListener::bind("127.0.0.1:0").await.expect("TCP bind");
    let tcp_addr = tcp_target.local_addr().expect("TCP address");
    let tcp_server = tokio::spawn(async move {
        let (mut stream, _) = tcp_target.accept().await.expect("TCP accept");
        let mut payload = [0_u8; 4];
        stream.read_exact(&mut payload).await.expect("TCP read");
        assert_eq!(&payload, b"ping");
        stream.write_all(b"pong").await.expect("TCP write");
    });
    let udp_target = UdpSocket::bind("127.0.0.1:0").await.expect("UDP bind");
    let udp_addr = udp_target.local_addr().expect("UDP address");
    let udp_server = tokio::spawn(async move {
        let mut payload = [0_u8; 4];
        let (length, peer) = udp_target.recv_from(&mut payload).await.expect("UDP read");
        assert_eq!(&payload[..length], b"ping");
        udp_target.send_to(b"pong", peer).await.expect("UDP write");
    });
    let telemetry = RuntimeTelemetry::generation_owner(8);
    let registry =
        RuntimeOutboundRegistryShell::compile([local_leaf("direct", OutboundConfig::Direct)], &[])
            .expect("registry")
            .with_product_telemetry(telemetry.clone())
            .with_dns(test_dns_generation());
    let selection = selection(&registry, "direct");
    let policy = crate::outbound::ServerDestinationPolicy::allow_restricted_for_test()
        .test_principal_policy();

    let OpenedTcpOutbound::Local {
        stream: OutboundTcpStream::Plain(mut tcp),
        ..
    } = registry
        .open_tcp(
            &selection,
            &TargetAddr::Ip(tcp_addr),
            None,
            TrafficClass::Latency,
            &policy,
        )
        .await
        .expect("local TCP")
    else {
        panic!("expected a concrete native TCP stream");
    };
    tcp.write_all(b"ping").await.expect("TCP request");
    let mut response = [0_u8; 4];
    tcp.read_exact(&mut response).await.expect("TCP response");
    assert_eq!(&response, b"pong");

    let OpenedUdpOutbound::Local {
        socket: OutboundUdpSocket::Direct(udp),
        ..
    } = registry
        .open_udp(&selection, &TargetAddr::Ip(udp_addr), None, &policy)
        .await
        .expect("local UDP")
    else {
        panic!("expected a concrete native UDP socket");
    };
    udp.send(b"ping").await.expect("UDP request");
    let length = udp.recv(&mut response).await.expect("UDP response");
    assert_eq!(&response[..length], b"pong");
    assert_eq!(
        telemetry.snapshot().io,
        crate::runtime::telemetry::ProductIoSnapshot::default(),
        "server MPP-to-native connectors must not add a second native observer"
    );
    tcp_server.await.expect("TCP task");
    udp_server.await.expect("UDP task");
}

#[tokio::test]
async fn gateway_blackhole_failover_keeps_domain_resolution_lazy_and_irreversible() {
    let blackhole_proxy = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("blackhole proxy bind");
    let blackhole_proxy_addr = blackhole_proxy.local_addr().expect("proxy address");
    let target = TcpListener::bind("127.0.0.1:0").await.expect("target bind");
    let target_addr = target.local_addr().expect("target address");
    let target_name = "fallback.example";
    let domain_target = TargetAddr::Domain {
        host: target_name.to_string(),
        port: target_addr.port(),
    };
    let expected_proxy_target = domain_target.clone();
    let blackhole_proxy_task = tokio::spawn(async move {
        let (mut stream, _) = blackhole_proxy.accept().await.expect("proxy accept");
        let mut greeting = [0_u8; 3];
        stream
            .read_exact(&mut greeting)
            .await
            .expect("proxy greeting");
        assert_eq!(greeting, crate::outbound::socks5::no_auth_greeting());
        stream.write_all(&[0x05, 0x00]).await.expect("proxy method");
        let expected = crate::outbound::socks5::connect_request(&expected_proxy_target)
            .expect("expected SOCKS5 request");
        let mut request = vec![0_u8; expected.len()];
        stream
            .read_exact(&mut request)
            .await
            .expect("proxy request");
        assert_eq!(request, expected, "proxy must receive the canonical domain");
        let mut remainder = Vec::new();
        stream
            .read_to_end(&mut remainder)
            .await
            .expect("timed-out proxy attempt closes");
    });
    let first = OutboundId::parse("failed-proxy").expect("outbound ID");
    let second = OutboundId::parse("working-direct").expect("outbound ID");
    let balancer_id = BalancerId::parse("native-failover").expect("balancer ID");
    let balancers = [GatewayBalancerConfig {
        id: balancer_id.clone(),
        generation: 1,
        spec: GatewayBalancerSpec::new(
            GatewayStrategy::OrderedFailover,
            vec![
                GatewayMemberSpec::new(first, 1, NetworkSet::TCP_UDP),
                GatewayMemberSpec::new(second, 1, NetworkSet::TCP_UDP),
            ],
        ),
    }];
    let dns = DnsGeneration::from_test_answers(HashMap::from([(
        target_name.to_string(),
        vec![target_addr.ip()],
    )]));
    let registry = RuntimeOutboundRegistry::compile(
        [
            local_leaf_with_timeout(
                "failed-proxy",
                OutboundConfig::Socks5(ProxyConfig::new(
                    blackhole_proxy_addr
                        .to_string()
                        .parse()
                        .expect("proxy endpoint"),
                    None,
                )),
                Duration::from_secs(1),
            ),
            local_leaf_with_timeout(
                "working-direct",
                OutboundConfig::Direct,
                Duration::from_secs(1),
            ),
        ],
        &balancers,
        dns.clone(),
    )
    .expect("registry");
    let policy = crate::outbound::ServerDestinationPolicy::allow_restricted_for_test()
        .test_principal_policy();
    let started = tokio::time::Instant::now();
    let opened = registry
        .open_tcp(
            &EgressSelection::Balancer(balancer_id),
            &domain_target,
            None,
            TrafficClass::Latency,
            &policy,
        )
        .await
        .expect("bounded pre-commit failover");
    assert!(
        started.elapsed() >= Duration::from_millis(900),
        "the blackholed member must retain its configured one-second connect stage"
    );
    let OpenedTcpOutbound::Local {
        _gateway_lease: Some(_),
        _product_flow,
        ..
    } = opened
    else {
        panic!("balancer failover must return the working native member");
    };
    assert_eq!(
        _product_flow.scope().selection.outbound.as_str(),
        "working-direct"
    );
    assert_eq!(
        _product_flow
            .scope()
            .selection
            .balancer
            .as_ref()
            .map(BalancerId::as_str),
        Some("native-failover")
    );
    assert_eq!(
        _product_flow
            .scope()
            .selection
            .member
            .as_ref()
            .map(OutboundId::as_str),
        Some("working-direct")
    );
    blackhole_proxy_task.await.expect("blackhole proxy task");
    target.accept().await.expect("direct target accepted");
    let dns = dns.runtime_snapshot();
    assert!(
        dns.plans[0].queries > 0,
        "failover to an IP-only leaf must request Product DNS evidence"
    );
    assert_eq!(
        dns.plans[0].fresh_cache_hits, 0,
        "the promoted destination must not be resolved a second time"
    );

    let remote_proxy = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("remote-resolution proxy bind");
    let remote_proxy_addr = remote_proxy.local_addr().expect("proxy address");
    let unresolved_target = TargetAddr::Domain {
        host: "remote-resolution.example".to_string(),
        port: 443,
    };
    let expected_domain = unresolved_target.clone();
    let remote_proxy_task = tokio::spawn(async move {
        let (mut stream, _) = remote_proxy.accept().await.expect("proxy accept");
        let mut greeting = [0_u8; 3];
        stream
            .read_exact(&mut greeting)
            .await
            .expect("proxy greeting");
        assert_eq!(greeting, crate::outbound::socks5::no_auth_greeting());
        stream.write_all(&[0x05, 0x00]).await.expect("proxy method");
        let expected = crate::outbound::socks5::connect_request(&expected_domain)
            .expect("expected domain request");
        let mut request = vec![0_u8; expected.len()];
        stream
            .read_exact(&mut request)
            .await
            .expect("proxy request");
        assert_eq!(
            request, expected,
            "DNS failure on an IP-only member must not replace the canonical domain"
        );
        stream
            .write_all(&[0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1, 0, 0])
            .await
            .expect("proxy success");
    });
    let local = OutboundId::parse("dns-required-direct").expect("outbound ID");
    let second_local = OutboundId::parse("second-dns-required-direct").expect("outbound ID");
    let remote = OutboundId::parse("remote-domain-proxy").expect("outbound ID");
    let dns_failover_id = BalancerId::parse("dns-failover").expect("balancer ID");
    let dns_failover_balancers = [GatewayBalancerConfig {
        id: dns_failover_id.clone(),
        generation: 1,
        spec: GatewayBalancerSpec::new(
            GatewayStrategy::OrderedFailover,
            vec![
                GatewayMemberSpec::new(local, 1, NetworkSet::TCP_UDP),
                GatewayMemberSpec::new(second_local, 1, NetworkSet::TCP_UDP),
                GatewayMemberSpec::new(remote, 1, NetworkSet::TCP_UDP),
            ],
        ),
    }];
    let failing_dns = DnsGeneration::from_test_answers(HashMap::new());
    let dns_failover_registry = RuntimeOutboundRegistry::compile(
        [
            local_leaf_with_timeout(
                "dns-required-direct",
                OutboundConfig::Direct,
                Duration::from_secs(1),
            ),
            local_leaf_with_timeout(
                "second-dns-required-direct",
                OutboundConfig::Direct,
                Duration::from_secs(1),
            ),
            local_leaf_with_timeout(
                "remote-domain-proxy",
                OutboundConfig::Socks5(ProxyConfig::new(
                    remote_proxy_addr
                        .to_string()
                        .parse()
                        .expect("proxy endpoint"),
                    None,
                )),
                Duration::from_secs(1),
            ),
        ],
        &dns_failover_balancers,
        failing_dns.clone(),
    )
    .expect("DNS failover registry");
    let opened = dns_failover_registry
        .open_tcp(
            &EgressSelection::Balancer(dns_failover_id.clone()),
            &unresolved_target,
            None,
            TrafficClass::Latency,
            &policy,
        )
        .await
        .expect("remote-resolution member survives local DNS failure");
    let OpenedTcpOutbound::Local { _product_flow, .. } = opened else {
        panic!("expected the remote-resolution proxy member");
    };
    assert_eq!(
        _product_flow.scope().selection.outbound.as_str(),
        "remote-domain-proxy"
    );
    let snapshots = dns_failover_registry
        .gateway_control()
        .snapshots()
        .expect("balancer snapshot");
    let members = &snapshots[0].runtime.members;
    assert_eq!(members[0].counters.open_attempts, 1);
    assert_eq!(
        members[0].counters.open_failures, 0,
        "shared target DNS failure is not gateway failure evidence"
    );
    assert_eq!(members[1].counters.open_attempts, 1);
    assert_eq!(
        members[1].counters.open_failures, 0,
        "skipping a member after flow-level DNS failure is not gateway failure evidence"
    );
    assert_eq!(members[2].counters.open_successes, 1);
    assert_eq!(
        failing_dns.runtime_snapshot().plans[0].queries,
        2,
        "one dual-family flow lookup must not be repeated for every IP-only member"
    );
    remote_proxy_task.await.expect("remote proxy task");

    let closed_target = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("closed target reservation");
    let closed_target_addr = closed_target.local_addr().expect("closed target address");
    drop(closed_target);
    let accepting_proxy = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("accepting proxy bind");
    let accepting_proxy_addr = accepting_proxy.local_addr().expect("proxy address");
    let promoted_name = "promoted.example";
    let promoted_target = TargetAddr::Domain {
        host: promoted_name.to_string(),
        port: closed_target_addr.port(),
    };
    let expected_literal = TargetAddr::Ip(closed_target_addr);
    let accepting_proxy_task = tokio::spawn(async move {
        let (mut stream, _) = accepting_proxy.accept().await.expect("proxy accept");
        let mut greeting = [0_u8; 3];
        stream
            .read_exact(&mut greeting)
            .await
            .expect("proxy greeting");
        assert_eq!(greeting, crate::outbound::socks5::no_auth_greeting());
        stream.write_all(&[0x05, 0x00]).await.expect("proxy method");
        let expected = crate::outbound::socks5::connect_request(&expected_literal)
            .expect("expected SOCKS5 request");
        let mut request = vec![0_u8; expected.len()];
        stream
            .read_exact(&mut request)
            .await
            .expect("proxy request");
        assert_eq!(
            request, expected,
            "a proxy attempted after an IP-only member must receive the authorized literal"
        );
        stream
            .write_all(&[0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1, 0, 0])
            .await
            .expect("proxy success");
    });
    let failed_direct = OutboundId::parse("failed-direct").expect("outbound ID");
    let working_proxy = OutboundId::parse("working-proxy").expect("outbound ID");
    let promoted_balancer = BalancerId::parse("promoted-failover").expect("balancer ID");
    let promoted_balancers = [GatewayBalancerConfig {
        id: promoted_balancer.clone(),
        generation: 1,
        spec: GatewayBalancerSpec::new(
            GatewayStrategy::OrderedFailover,
            vec![
                GatewayMemberSpec::new(failed_direct, 1, NetworkSet::TCP_UDP),
                GatewayMemberSpec::new(working_proxy, 1, NetworkSet::TCP_UDP),
            ],
        ),
    }];
    let promoted_dns = DnsGeneration::from_test_answers(HashMap::from([(
        promoted_name.to_string(),
        vec![closed_target_addr.ip()],
    )]));
    let promoted_registry = RuntimeOutboundRegistry::compile(
        [
            local_leaf("failed-direct", OutboundConfig::Direct),
            local_leaf(
                "working-proxy",
                OutboundConfig::Socks5(ProxyConfig::new(
                    accepting_proxy_addr
                        .to_string()
                        .parse()
                        .expect("proxy endpoint"),
                    None,
                )),
            ),
        ],
        &promoted_balancers,
        promoted_dns.clone(),
    )
    .expect("promoted registry");
    let opened = promoted_registry
        .open_tcp(
            &EgressSelection::Balancer(promoted_balancer),
            &promoted_target,
            None,
            TrafficClass::Latency,
            &policy,
        )
        .await
        .expect("IP-only failure followed by proxy fallback");
    let OpenedTcpOutbound::Local {
        _gateway_lease: Some(_),
        _product_flow,
        ..
    } = opened
    else {
        panic!("balancer failover must return the working proxy member");
    };
    assert_eq!(
        _product_flow.scope().selection.outbound.as_str(),
        "working-proxy"
    );
    accepting_proxy_task.await.expect("accepting proxy task");
    let promoted_dns = promoted_dns.runtime_snapshot();
    assert!(promoted_dns.plans[0].queries > 0);
    assert_eq!(
        promoted_dns.plans[0].fresh_cache_hits, 0,
        "an already promoted destination must never be resolved again or revert to its domain"
    );
}

#[test]
fn server_native_selector_rejects_mpp_chaining_at_runtime_assembly() {
    let context = ClientPathContext::new(
        vec!["quic://127.0.0.1:7443".parse().expect("path")],
        ClientSecurityConfig::for_test(
            SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
        ),
        ResourceLimits::default(),
    )
    .expect("MPP context");
    let id = OutboundId::parse("another-mpp").expect("outbound ID");
    let registry = RuntimeOutboundRegistry::compile(
        [RuntimeOutboundLeaf::Mpp {
            id: id.clone(),
            context,
            performance: MppPerformanceConfig::default(),
        }],
        &[],
        test_dns_generation(),
    )
    .expect("registry");
    assert!(matches!(
        registry.ensure_native_egress(&EgressSelection::Outbound(id)),
        Err(RuntimeError::ProductPolicy(message))
            if message.contains("cannot select an MPP outbound")
    ));
}

fn named_dns_policy_for(
    outbound: OutboundId,
    endpoint: DnsUpstreamEndpoint,
    networks: NetworkSet,
) -> Arc<CompiledDnsPolicy> {
    let upstream = DnsUpstreamId::parse("named-upstream").expect("upstream ID");
    let plan = DnsPlanId::parse("default").expect("plan ID");
    let mut plan_spec = DnsPlanSpec::new(plan.clone(), vec![upstream.clone()]);
    plan_spec.ip_strategy = DnsIpStrategy::Ipv4Only;
    Arc::new(
        CompiledDnsPolicy::compile(
            1,
            DnsPolicySpec {
                upstreams: vec![DnsUpstreamSpec {
                    id: upstream.clone(),
                    endpoint,
                    egress: DnsEgressSpec::Outbound(outbound.clone()),
                }],
                outbound_capabilities: vec![DnsOutboundCapabilitySpec::new(
                    outbound, networks, true,
                )],
                plans: vec![plan_spec],
                rules: Vec::new(),
                hosts: Vec::new(),
                fake_dns: None,
                default_plan: plan,
            },
        )
        .expect("named DNS policy"),
    )
}

fn named_udp_dns_policy(outbound: OutboundId) -> Arc<CompiledDnsPolicy> {
    named_dns_policy_for(
        outbound,
        DnsUpstreamEndpoint::Udp {
            bootstrap: "1.1.1.1:53".parse().expect("bootstrap"),
        },
        NetworkSet::TCP_UDP,
    )
}

fn named_tcp_dns_policy(outbound: OutboundId, bootstrap: SocketAddr) -> Arc<CompiledDnsPolicy> {
    named_dns_policy_for(
        outbound,
        DnsUpstreamEndpoint::Tcp { bootstrap },
        NetworkSet::TCP,
    )
}

#[test]
fn named_dns_egress_accepts_only_dns_independent_native_leaves() {
    let bind_id = OutboundId::parse("bound-direct").expect("outbound ID");
    let shell = RuntimeOutboundRegistryShell::compile(
        [local_leaf(
            bind_id.as_str(),
            OutboundConfig::BindSourceIp(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        )],
        &[],
    )
    .expect("shell");
    let factory =
        shell.dns_backend_factory(Arc::new(crate::transport::SystemNativeSocketConfigurator));
    DnsGeneration::compile_with_factory(named_udp_dns_policy(bind_id), &factory)
        .expect("named bind-source DNS connector");

    let proxy_id = OutboundId::parse("proxy").expect("outbound ID");
    let shell = RuntimeOutboundRegistryShell::compile(
        [local_leaf(
            proxy_id.as_str(),
            OutboundConfig::Socks5(ProxyConfig::new(
                "127.0.0.1:1080".parse().expect("proxy endpoint"),
                None,
            )),
        )],
        &[],
    )
    .expect("shell");
    let factory =
        shell.dns_backend_factory(Arc::new(crate::transport::SystemNativeSocketConfigurator));
    assert!(matches!(
        DnsGeneration::compile_with_factory(named_udp_dns_policy(proxy_id.clone()), &factory),
        Err(DnsRuntimeError::UnsupportedEgressTransport { outbound, .. })
            if outbound == proxy_id
    ));
}

#[test]
fn routed_tcp_dot_and_doh_compile_for_literal_proxy_control_endpoints() {
    let configs = [
        (
            "socks",
            OutboundConfig::Socks5(ProxyConfig::new(
                "127.0.0.1:1080".parse().expect("SOCKS endpoint"),
                None,
            )),
            DnsUpstreamEndpoint::Tcp {
                bootstrap: "192.0.2.53:53".parse().expect("TCP bootstrap"),
            },
        ),
        (
            "http",
            OutboundConfig::HttpConnect(ProxyConfig::new(
                "127.0.0.1:8080".parse().expect("HTTP endpoint"),
                None,
            )),
            DnsUpstreamEndpoint::Tls {
                bootstrap: "192.0.2.53:853".parse().expect("DoT bootstrap"),
                server_name: crate::product::DomainName::parse("resolver.example")
                    .expect("DoT identity"),
            },
        ),
        (
            "https",
            OutboundConfig::HttpsConnect(Box::new(
                HttpsProxyConfig::new(
                    ProxyConfig::new("127.0.0.1:8443".parse().expect("HTTPS endpoint"), None),
                    Some("proxy.example".to_string()),
                    Vec::new(),
                )
                .expect("HTTPS proxy"),
            )),
            DnsUpstreamEndpoint::Https {
                bootstrap: "192.0.2.53:443".parse().expect("DoH bootstrap"),
                server_name: crate::product::DomainName::parse("resolver.example")
                    .expect("DoH identity"),
                path: "/dns-query".to_string(),
            },
        ),
    ];
    for (tag, config, endpoint) in configs {
        let id = OutboundId::parse(tag).expect("outbound ID");
        let shell = RuntimeOutboundRegistryShell::compile([local_leaf(id.as_str(), config)], &[])
            .expect("shell");
        let factory =
            shell.dns_backend_factory(Arc::new(crate::transport::SystemNativeSocketConfigurator));
        DnsGeneration::compile_with_factory(
            named_dns_policy_for(id, endpoint, NetworkSet::TCP),
            &factory,
        )
        .unwrap_or_else(|error| panic!("{tag} routed DNS did not compile: {error}"));
    }
}

#[test]
fn routed_dns_rejects_proxy_and_mpp_control_hostnames_at_runtime_assembly() {
    let proxy_id = OutboundId::parse("named-proxy").expect("outbound ID");
    let shell = RuntimeOutboundRegistryShell::compile(
        [local_leaf(
            proxy_id.as_str(),
            OutboundConfig::Socks5(ProxyConfig::new(
                "proxy.example:1080".parse().expect("proxy endpoint"),
                None,
            )),
        )],
        &[],
    )
    .expect("shell");
    let factory =
        shell.dns_backend_factory(Arc::new(crate::transport::SystemNativeSocketConfigurator));
    assert!(matches!(
        DnsGeneration::compile_with_factory(
            named_tcp_dns_policy(
                proxy_id.clone(),
                "192.0.2.53:53".parse().expect("bootstrap")
            ),
            &factory,
        ),
        Err(DnsRuntimeError::RecursiveEgressConnector { outbound, .. })
            if outbound == proxy_id
    ));

    let mpp_id = OutboundId::parse("named-mpp").expect("outbound ID");
    let context = ClientPathContext::new(
        vec![
            "quic://carrier.example:7443"
                .parse()
                .expect("MPP path endpoint"),
        ],
        ClientSecurityConfig::for_test(
            SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
        ),
        ResourceLimits::default(),
    )
    .expect("MPP context");
    let shell = RuntimeOutboundRegistryShell::compile(
        [RuntimeOutboundLeaf::Mpp {
            id: mpp_id.clone(),
            context,
            performance: MppPerformanceConfig::default(),
        }],
        &[],
    )
    .expect("shell");
    let factory =
        shell.dns_backend_factory(Arc::new(crate::transport::SystemNativeSocketConfigurator));
    assert!(matches!(
        DnsGeneration::compile_with_factory(
            named_tcp_dns_policy(
                mpp_id.clone(),
                "192.0.2.53:53".parse().expect("bootstrap")
            ),
            &factory,
        ),
        Err(DnsRuntimeError::RecursiveEgressConnector { outbound, .. })
            if outbound == mpp_id
    ));

    let literal_id = OutboundId::parse("literal-mpp").expect("outbound ID");
    let context = ClientPathContext::new(
        vec![
            "quic://127.0.0.1:7443"
                .parse()
                .expect("literal MPP path endpoint"),
        ],
        ClientSecurityConfig::for_test(
            SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
        ),
        ResourceLimits::default(),
    )
    .expect("MPP context");
    let shell = RuntimeOutboundRegistryShell::compile(
        [RuntimeOutboundLeaf::Mpp {
            id: literal_id.clone(),
            context,
            performance: MppPerformanceConfig::default(),
        }],
        &[],
    )
    .expect("shell");
    let factory =
        shell.dns_backend_factory(Arc::new(crate::transport::SystemNativeSocketConfigurator));
    DnsGeneration::compile_with_factory(
        named_tcp_dns_policy(literal_id, "192.0.2.53:53".parse().expect("bootstrap")),
        &factory,
    )
    .expect("literal MPP DNS connector");
}

#[tokio::test]
async fn routed_dns_query_traverses_the_selected_socks_connector() {
    let proxy = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("SOCKS listener");
    let proxy_address = proxy.local_addr().expect("SOCKS address");
    let bootstrap: SocketAddr = "192.0.2.53:53".parse().expect("DNS bootstrap");
    let answer: std::net::Ipv4Addr = "203.0.113.9".parse().expect("DNS answer");
    let proxy_task = tokio::spawn(async move {
        let (mut stream, _) = proxy.accept().await.expect("SOCKS accept");
        let mut greeting = [0_u8; 3];
        stream
            .read_exact(&mut greeting)
            .await
            .expect("SOCKS greeting");
        assert_eq!(greeting, [0x05, 0x01, 0x00]);
        stream.write_all(&[0x05, 0x00]).await.expect("SOCKS method");

        let mut connect = [0_u8; 10];
        stream
            .read_exact(&mut connect)
            .await
            .expect("SOCKS CONNECT");
        assert_eq!(&connect[..4], &[0x05, 0x01, 0x00, 0x01]);
        assert_eq!(
            &connect[4..8],
            &bootstrap
                .ip()
                .to_string()
                .parse::<std::net::Ipv4Addr>()
                .expect("IPv4")
                .octets()
        );
        assert_eq!(
            u16::from_be_bytes([connect[8], connect[9]]),
            bootstrap.port()
        );
        stream
            .write_all(&[0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1, 0, 0])
            .await
            .expect("SOCKS success");

        let mut length = [0_u8; 2];
        stream
            .read_exact(&mut length)
            .await
            .expect("DNS frame length");
        let mut request_wire = vec![0_u8; usize::from(u16::from_be_bytes(length))];
        stream
            .read_exact(&mut request_wire)
            .await
            .expect("DNS request");
        let request = hickory_proto::op::Message::from_vec(&request_wire).expect("DNS message");
        let query = request.queries[0].clone();
        let mut response = hickory_proto::op::Message::response(
            request.metadata.id,
            hickory_proto::op::OpCode::Query,
        );
        response.add_query(query.clone());
        response.add_answer(hickory_proto::rr::Record::from_rdata(
            query.name().clone(),
            60,
            hickory_proto::rr::RData::A(hickory_proto::rr::rdata::A(answer)),
        ));
        let response_wire = response.to_vec().expect("DNS response");
        stream
            .write_all(
                &u16::try_from(response_wire.len())
                    .expect("DNS response length")
                    .to_be_bytes(),
            )
            .await
            .expect("DNS response length");
        stream
            .write_all(&response_wire)
            .await
            .expect("DNS response");
    });

    let id = OutboundId::parse("socks-dns").expect("outbound ID");
    let shell = RuntimeOutboundRegistryShell::compile(
        [local_leaf(
            id.as_str(),
            OutboundConfig::Socks5(ProxyConfig::new(
                proxy_address.to_string().parse().expect("proxy endpoint"),
                None,
            )),
        )],
        &[],
    )
    .expect("shell");
    let factory =
        shell.dns_backend_factory(Arc::new(crate::transport::SystemNativeSocketConfigurator));
    let dns = DnsGeneration::compile_with_factory(named_tcp_dns_policy(id, bootstrap), &factory)
        .expect("routed DNS generation");
    let resolution = dns
        .resolve(&crate::product::DomainName::parse("through-proxy.example").expect("domain"))
        .await
        .expect("routed DNS answer");
    assert_eq!(resolution.addresses().as_ref(), &[IpAddr::V4(answer)]);
    proxy_task.await.expect("SOCKS task");
}
