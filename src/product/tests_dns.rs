use super::*;

fn upstream_id(value: &str) -> DnsUpstreamId {
    DnsUpstreamId::parse(value).expect("upstream ID")
}

fn plan_id(value: &str) -> DnsPlanId {
    DnsPlanId::parse(value).expect("plan ID")
}

fn rule_id(value: &str) -> DnsRuleId {
    DnsRuleId::parse(value).expect("rule ID")
}

fn domain(value: &str) -> DomainName {
    DomainName::parse(value).expect("domain")
}

fn udp(id: &str, address: &str) -> DnsUpstreamSpec {
    DnsUpstreamSpec::direct(
        upstream_id(id),
        DnsUpstreamEndpoint::Udp {
            bootstrap: address.parse().expect("bootstrap"),
        },
    )
}

fn dot(id: &str, address: &str, name: &str) -> DnsUpstreamSpec {
    DnsUpstreamSpec::direct(
        upstream_id(id),
        DnsUpstreamEndpoint::Tls {
            bootstrap: address.parse().expect("bootstrap"),
            server_name: domain(name),
        },
    )
}

fn base_spec() -> DnsPolicySpec {
    DnsPolicySpec {
        upstreams: vec![
            udp("local", "192.0.2.53:53"),
            dot("private", "198.51.100.53:853", "resolver.example"),
        ],
        outbound_capabilities: Vec::new(),
        plans: vec![
            DnsPlanSpec::new(plan_id("public"), vec![upstream_id("private")]),
            DnsPlanSpec::new(plan_id("lan"), vec![upstream_id("local")]),
        ],
        rules: vec![
            DnsRuleSpec {
                id: rule_id("exact-router"),
                matcher: DnsRuleMatch::Exact(domain("router.lan")),
                plan: plan_id("lan"),
                explanation: Some("local router name".to_string()),
            },
            DnsRuleSpec {
                id: rule_id("suffix-corp"),
                matcher: DnsRuleMatch::Suffix(domain("corp.example")),
                plan: plan_id("lan"),
                explanation: Some("private split-DNS zone".to_string()),
            },
            DnsRuleSpec {
                id: rule_id("suffix-deeper"),
                matcher: DnsRuleMatch::Suffix(domain("dev.corp.example")),
                plan: plan_id("public"),
                explanation: None,
            },
        ],
        hosts: Vec::new(),
        fake_dns: None,
        default_plan: plan_id("public"),
    }
}

#[test]
fn exact_longest_suffix_and_default_are_deterministic() {
    let policy = CompiledDnsPolicy::compile(17, base_spec()).expect("policy");
    let cases = [
        (
            "router.lan",
            "lan",
            DnsRuleMatchKind::Exact,
            Some("exact-router"),
            Some("local router name"),
        ),
        (
            "api.dev.corp.example",
            "public",
            DnsRuleMatchKind::Suffix,
            Some("suffix-deeper"),
            None,
        ),
        (
            "www.corp.example",
            "lan",
            DnsRuleMatchKind::Suffix,
            Some("suffix-corp"),
            Some("private split-DNS zone"),
        ),
        (
            "example.net",
            "public",
            DnsRuleMatchKind::Default,
            None,
            None,
        ),
    ];
    for (name, plan, kind, rule, explanation) in cases {
        let selection = policy.select(&domain(name));
        assert_eq!(selection.generation(), 17);
        assert_eq!(selection.plan().id().as_str(), plan);
        assert_eq!(selection.match_kind(), kind);
        assert_eq!(selection.rule_id().map(DnsRuleId::as_str), rule);
        assert_eq!(selection.explanation(), explanation);
    }
}

#[test]
fn encrypted_plan_rejects_every_plaintext_member() {
    let mut spec = base_spec();
    spec.plans[1].security = DnsSecurityPolicy::RequireEncrypted;
    assert_eq!(
        CompiledDnsPolicy::compile(1, spec).expect_err("plaintext must fail"),
        DnsCompileError::PlaintextUpstreamInEncryptedPlan {
            plan: plan_id("lan"),
            upstream: upstream_id("local"),
        }
    );
}

#[test]
fn doh_identity_bootstrap_and_path_are_strict() {
    let invalid = [
        DnsUpstreamEndpoint::Https {
            bootstrap: "0.0.0.0:443".parse().expect("address"),
            server_name: domain("resolver.example"),
            path: "/dns-query".to_string(),
        },
        DnsUpstreamEndpoint::Https {
            bootstrap: "192.0.2.53:443".parse().expect("address"),
            server_name: domain("resolver.example"),
            path: "dns-query".to_string(),
        },
        DnsUpstreamEndpoint::Https {
            bootstrap: "192.0.2.53:443".parse().expect("address"),
            server_name: domain("resolver.example"),
            path: "/dns-query?wire=get".to_string(),
        },
    ];
    for endpoint in invalid {
        let mut spec = base_spec();
        spec.upstreams[0] = DnsUpstreamSpec::direct(upstream_id("local"), endpoint);
        assert!(
            CompiledDnsPolicy::compile(1, spec).is_err(),
            "invalid DoH endpoint compiled"
        );
    }
    assert!(DomainName::parse("192.0.2.53").is_err());
}

#[test]
fn doh_custom_port_is_valid_and_system_is_explicit_plaintext_only() {
    let mut spec = base_spec();
    spec.upstreams[0] = DnsUpstreamSpec::direct(
        upstream_id("local"),
        DnsUpstreamEndpoint::Https {
            bootstrap: "192.0.2.53:8443".parse().expect("address"),
            server_name: domain("resolver.example"),
            path: "/dns-query".to_string(),
        },
    );
    assert!(CompiledDnsPolicy::compile(1, spec).is_ok());

    let mut system = base_spec();
    system.upstreams[0] =
        DnsUpstreamSpec::direct(upstream_id("local"), DnsUpstreamEndpoint::System);
    system.plans[1].security = DnsSecurityPolicy::RequireEncrypted;
    assert!(matches!(
        CompiledDnsPolicy::compile(1, system),
        Err(DnsCompileError::PlaintextUpstreamInEncryptedPlan { .. })
    ));

    let proxy = OutboundId::parse("proxy").expect("outbound");
    let mut routed_system = base_spec();
    routed_system.upstreams[0] = DnsUpstreamSpec {
        id: upstream_id("local"),
        endpoint: DnsUpstreamEndpoint::System,
        egress: DnsEgressSpec::Outbound(proxy.clone()),
    };
    routed_system.outbound_capabilities = vec![DnsOutboundCapabilitySpec::new(
        proxy,
        NetworkSet::TCP_UDP,
        true,
    )];
    assert!(matches!(
        CompiledDnsPolicy::compile(1, routed_system),
        Err(DnsCompileError::SystemUpstreamWithOutbound { .. })
    ));
}

#[test]
fn vpn_preflight_facts_are_explicit_and_literal() {
    let policy = CompiledDnsPolicy::compile(1, base_spec()).expect("policy");
    assert_eq!(
        policy.bootstrap_endpoints().collect::<Vec<_>>(),
        vec![
            "192.0.2.53:53".parse().expect("bootstrap"),
            "198.51.100.53:853".parse().expect("bootstrap"),
        ]
    );
    assert!(!policy.uses_system_resolution());
    assert!(!policy.is_encrypted_only());

    let mut encrypted = base_spec();
    encrypted.plans.remove(1);
    encrypted
        .rules
        .retain(|rule| rule.plan.as_str() == "public");
    encrypted.default_plan = plan_id("public");
    encrypted.plans[0].security = DnsSecurityPolicy::RequireEncrypted;
    let encrypted = CompiledDnsPolicy::compile(2, encrypted).expect("encrypted policy");
    assert!(encrypted.is_encrypted_only());

    let mut system = base_spec();
    system.upstreams[0] =
        DnsUpstreamSpec::direct(upstream_id("local"), DnsUpstreamEndpoint::System);
    let system = CompiledDnsPolicy::compile(3, system).expect("system policy");
    assert!(system.uses_system_resolution());
}

#[test]
fn outbound_must_exist_support_transport_and_be_dns_independent() {
    let proxy = OutboundId::parse("proxy").expect("outbound");
    let mut spec = base_spec();
    spec.upstreams[0].egress = DnsEgressSpec::Outbound(proxy.clone());
    assert!(matches!(
        CompiledDnsPolicy::compile(1, spec.clone()),
        Err(DnsCompileError::UnknownOutbound { .. })
    ));

    spec.outbound_capabilities
        .push(DnsOutboundCapabilitySpec::new(
            proxy.clone(),
            NetworkSet::UDP,
            false,
        ));
    assert!(matches!(
        CompiledDnsPolicy::compile(1, spec.clone()),
        Err(DnsCompileError::RecursiveOutbound { .. })
    ));

    spec.outbound_capabilities[0].dns_independent = true;
    spec.upstreams[0].endpoint = DnsUpstreamEndpoint::Tcp {
        bootstrap: "192.0.2.53:53".parse().expect("address"),
    };
    assert!(matches!(
        CompiledDnsPolicy::compile(1, spec),
        Err(DnsCompileError::UnsupportedOutboundNetwork {
            network: Network::Tcp,
            ..
        })
    ));
}

#[test]
fn doq_is_an_encrypted_udp_transport_with_literal_identity() {
    let mut spec = base_spec();
    spec.upstreams[1].endpoint = DnsUpstreamEndpoint::Quic {
        bootstrap: "198.51.100.53:8853".parse().expect("bootstrap"),
        server_name: domain("resolver.example"),
    };
    spec.plans[0].security = DnsSecurityPolicy::RequireEncrypted;
    let compiled = CompiledDnsPolicy::compile(1, spec).expect("DoQ policy");
    let upstream = compiled
        .upstream(&upstream_id("private"))
        .expect("DoQ upstream");
    assert_eq!(upstream.endpoint().transport(), DnsTransport::Quic);
    assert_eq!(upstream.endpoint().transport().networks(), NetworkSet::UDP);
    assert!(upstream.endpoint().transport().is_encrypted());
    assert_eq!(
        upstream.endpoint().server_name(),
        Some(&domain("resolver.example"))
    );
}

#[test]
fn fake_dns_pools_lifetimes_and_bootstraps_fail_closed() {
    let mut valid = base_spec();
    valid.fake_dns = Some(FakeDnsSpec {
        ipv4_pool: Some("198.18.0.0/16".parse().expect("IPv4 pool")),
        ipv6_pool: Some("fd00:4d50::/112".parse().expect("IPv6 pool")),
        max_entries: 4_096,
        answer_ttl: Duration::from_secs(30),
        recovery_ttl: Duration::from_secs(120),
    });
    assert!(CompiledDnsPolicy::compile(1, valid.clone()).is_ok());

    let mut public = valid.clone();
    public.fake_dns.as_mut().expect("FakeDNS").ipv4_pool =
        Some("203.0.113.0/24".parse().expect("public pool"));
    assert!(matches!(
        CompiledDnsPolicy::compile(1, public),
        Err(DnsCompileError::InvalidFakeDnsIpv4Pool(_))
    ));

    let mut short_recovery = valid.clone();
    short_recovery
        .fake_dns
        .as_mut()
        .expect("FakeDNS")
        .recovery_ttl = Duration::from_secs(1);
    assert!(matches!(
        CompiledDnsPolicy::compile(1, short_recovery),
        Err(DnsCompileError::InvalidFakeDnsLifetime { .. })
    ));

    let mut overlap = valid;
    overlap.upstreams[0].endpoint = DnsUpstreamEndpoint::Udp {
        bootstrap: "198.18.0.53:53".parse().expect("overlapping bootstrap"),
    };
    assert!(matches!(
        CompiledDnsPolicy::compile(1, overlap),
        Err(DnsCompileError::FakeDnsContainsBootstrap { .. })
    ));
}

#[test]
fn duplicate_ids_matches_and_references_fail_closed() {
    let mut duplicate_upstream = base_spec();
    duplicate_upstream
        .upstreams
        .push(udp("local", "203.0.113.53:53"));
    assert!(matches!(
        CompiledDnsPolicy::compile(1, duplicate_upstream),
        Err(DnsCompileError::DuplicateUpstreamId(_))
    ));

    let mut duplicate_match = base_spec();
    duplicate_match.rules.push(DnsRuleSpec {
        id: rule_id("another"),
        matcher: DnsRuleMatch::Suffix(domain("corp.example")),
        plan: plan_id("public"),
        explanation: None,
    });
    assert!(matches!(
        CompiledDnsPolicy::compile(1, duplicate_match),
        Err(DnsCompileError::DuplicateRuleMatch { .. })
    ));

    let mut missing = base_spec();
    missing.plans[0].upstreams[0] = upstream_id("missing");
    assert!(matches!(
        CompiledDnsPolicy::compile(1, missing),
        Err(DnsCompileError::UnknownUpstream { .. })
    ));
}
