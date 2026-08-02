use super::*;
use crate::product::{DomainName, FlowContext, ProtocolTarget, SourceEndpoint};
use std::net::{IpAddr, Ipv4Addr};

fn id<T: FromStr>(value: &str) -> T
where
    T::Err: fmt::Debug,
{
    value.parse().expect("valid ID")
}

fn flow(domain: &str, network: Network) -> FlowContext {
    FlowContext::new(
        network,
        ProtocolTarget::from_host_port(domain, 443).expect("target"),
        SourceEndpoint::new(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 4)), 40000),
        id("alice"),
        id("socks-main"),
    )
}

fn default_rule() -> RouteRuleSpec {
    RouteRuleSpec::new(
        id("default"),
        RouteMatchSpec::default(),
        RouteAction::direct(TrafficIntent::Interactive),
    )
}

#[test]
fn exact_suffix_keyword_and_regex_are_deterministic() {
    let cases = [
        (
            "exact",
            RouteMatchSpec {
                domain_exact: vec![DomainName::parse("api.example").expect("domain")],
                ..RouteMatchSpec::default()
            },
            "api.example",
        ),
        (
            "suffix",
            RouteMatchSpec {
                domain_suffix: vec![DomainName::parse("example.org").expect("domain")],
                ..RouteMatchSpec::default()
            },
            "deep.service.example.org",
        ),
        (
            "keyword",
            RouteMatchSpec {
                domain_keyword: vec!["bücher".to_owned()],
                ..RouteMatchSpec::default()
            },
            "shop.xn--bcher-kva.example",
        ),
        (
            "regex",
            RouteMatchSpec {
                domain_regex: vec![r"^cdn-[0-9]+\.example\.net$".to_owned()],
                ..RouteMatchSpec::default()
            },
            "cdn-42.example.net",
        ),
    ];
    for (rule_name, matcher, domain) in cases {
        let table = CompiledRouteTable::compile(
            7,
            vec![
                RouteRuleSpec::new(
                    id(rule_name),
                    matcher,
                    RouteAction::new(
                        EgressAction::Outbound(id("edge")),
                        Some(id("secure")),
                        TrafficIntent::Throughput,
                    ),
                ),
                default_rule(),
            ],
        )
        .expect("table");
        for _ in 0..100 {
            let current = flow(domain, Network::Tcp);
            let decision = table.classify(RouteInput::pre_resolution(&current));
            assert_eq!(decision.rule_id().as_str(), rule_name);
            assert_eq!(decision.generation(), 7);
        }
    }
}

#[test]
fn suffix_matching_observes_label_boundaries_and_apex() {
    let table = CompiledRouteTable::compile(
        1,
        vec![
            RouteRuleSpec::new(
                id("suffix"),
                RouteMatchSpec {
                    domain_suffix: vec![DomainName::parse("example.com").expect("domain")],
                    ..RouteMatchSpec::default()
                },
                RouteAction::new(EgressAction::Reject, None, TrafficIntent::Interactive),
            ),
            default_rule(),
        ],
    )
    .expect("table");
    for domain in ["example.com", "a.example.com"] {
        let current = flow(domain, Network::Tcp);
        assert_eq!(
            table
                .classify(RouteInput::pre_resolution(&current))
                .rule_id()
                .as_str(),
            "suffix"
        );
    }
    let unrelated = flow("notexample.com", Network::Tcp);
    assert_eq!(
        table
            .classify(RouteInput::pre_resolution(&unrelated))
            .rule_id()
            .as_str(),
        "default"
    );
}

#[test]
fn match_categories_are_anded_and_values_are_ored() {
    let table = CompiledRouteTable::compile(
        2,
        vec![
            RouteRuleSpec::new(
                id("specific"),
                RouteMatchSpec {
                    domain_suffix: vec![DomainName::parse("example").expect("domain")],
                    destination_ports: vec![PortRange::single(443), PortRange::single(8443)],
                    networks: vec![Network::Tcp],
                    inbounds: vec![id("socks-main")],
                    principals: vec![id("alice")],
                    ..RouteMatchSpec::default()
                },
                RouteAction::new(
                    EgressAction::Balancer(id("fastest")),
                    None,
                    TrafficIntent::Realtime,
                ),
            ),
            default_rule(),
        ],
    )
    .expect("table");
    let tcp = flow("video.example", Network::Tcp);
    assert_eq!(
        table
            .classify(RouteInput::pre_resolution(&tcp))
            .rule_id()
            .as_str(),
        "specific"
    );
    let udp = flow("video.example", Network::Udp);
    assert_eq!(
        table
            .classify(RouteInput::pre_resolution(&udp))
            .rule_id()
            .as_str(),
        "default"
    );
}

#[test]
fn destination_cidr_uses_literal_pre_dns_and_answer_post_dns() {
    let private = "10.0.0.0/8".parse::<IpNet>().expect("CIDR");
    let table = CompiledRouteTable::compile(
        3,
        vec![
            RouteRuleSpec::new(
                id("private"),
                RouteMatchSpec {
                    destination_cidrs: vec![private],
                    ..RouteMatchSpec::default()
                },
                RouteAction::new(EgressAction::Reject, None, TrafficIntent::Interactive),
            ),
            default_rule(),
        ],
    )
    .expect("table");
    let name = flow("service.example", Network::Tcp);
    assert_eq!(
        table
            .classify(RouteInput::pre_resolution(&name))
            .rule_id()
            .as_str(),
        "default"
    );
    assert_eq!(
        table
            .classify(RouteInput::post_resolution(
                &name,
                "10.2.3.4".parse().expect("address")
            ))
            .rule_id()
            .as_str(),
        "private"
    );

    let literal = FlowContext::new(
        Network::Tcp,
        ProtocolTarget::from_ip("10.1.2.3".parse().expect("address"), 443).expect("target"),
        name.source(),
        name.principal().clone(),
        name.inbound().clone(),
    );
    assert_eq!(
        table
            .classify(RouteInput::pre_resolution(&literal))
            .rule_id()
            .as_str(),
        "private"
    );
    assert_eq!(
        table
            .classify(RouteInput::post_resolution(
                &literal,
                "203.0.113.8".parse().expect("untrusted replacement")
            ))
            .rule_id()
            .as_str(),
        "private",
        "a literal destination remains authoritative after resolution"
    );
}

#[test]
fn first_match_order_determines_domain_resolution_demand() {
    let domain_rule = || {
        RouteRuleSpec::new(
            id("domain"),
            RouteMatchSpec {
                domain_exact: vec![DomainName::parse("service.example").expect("domain")],
                ..RouteMatchSpec::default()
            },
            RouteAction::new(
                EgressAction::Outbound(id("domain-edge")),
                None,
                TrafficIntent::Interactive,
            ),
        )
    };
    let address_rule = || {
        RouteRuleSpec::new(
            id("address"),
            RouteMatchSpec {
                destination_cidrs: vec!["203.0.113.0/24".parse().expect("CIDR")],
                ..RouteMatchSpec::default()
            },
            RouteAction::new(
                EgressAction::Outbound(id("address-edge")),
                None,
                TrafficIntent::Interactive,
            ),
        )
    };
    let current = flow("service.example", Network::Tcp);

    let address_first =
        CompiledRouteTable::compile(5, vec![address_rule(), domain_rule(), default_rule()])
            .expect("address-first table");
    assert!(address_first.requires_post_resolution(&current));

    let domain_first =
        CompiledRouteTable::compile(6, vec![domain_rule(), address_rule(), default_rule()])
            .expect("domain-first table");
    assert!(!domain_first.requires_post_resolution(&current));

    let fixed_mismatch = CompiledRouteTable::compile(
        7,
        vec![
            RouteRuleSpec::new(
                id("udp-address"),
                RouteMatchSpec {
                    destination_cidrs: vec!["203.0.113.0/24".parse().expect("CIDR")],
                    networks: vec![Network::Udp],
                    ..RouteMatchSpec::default()
                },
                RouteAction::new(
                    EgressAction::Outbound(id("udp-edge")),
                    None,
                    TrafficIntent::Interactive,
                ),
            ),
            domain_rule(),
            default_rule(),
        ],
    )
    .expect("fixed-category mismatch table");
    assert!(
        !fixed_mismatch.requires_post_resolution(&current),
        "an IP rule that cannot match this flow must not trigger DNS"
    );

    let post_stage = CompiledRouteTable::compile(
        8,
        vec![
            RouteRuleSpec::new(
                id("post"),
                RouteMatchSpec {
                    networks: vec![Network::Tcp],
                    stages: vec![RouteStage::PostResolution],
                    ..RouteMatchSpec::default()
                },
                RouteAction::new(EgressAction::Reject, None, TrafficIntent::Interactive),
            ),
            default_rule(),
        ],
    )
    .expect("post-stage table");
    assert!(post_stage.requires_post_resolution(&current));
}

#[test]
fn source_cidr_ports_and_stage_match() {
    let table = CompiledRouteTable::compile(
        4,
        vec![
            RouteRuleSpec::new(
                id("post"),
                RouteMatchSpec {
                    source_cidrs: vec!["198.51.100.0/24".parse().expect("CIDR")],
                    source_ports: vec![PortRange::new(39000, 41000).expect("range")],
                    stages: vec![RouteStage::PostResolution],
                    ..RouteMatchSpec::default()
                },
                RouteAction::new(EgressAction::Drop, None, TrafficIntent::Background),
            ),
            default_rule(),
        ],
    )
    .expect("table");
    let current = flow("sync.example", Network::Tcp);
    assert_eq!(
        table
            .classify(RouteInput::pre_resolution(&current))
            .rule_id()
            .as_str(),
        "default"
    );
    assert_eq!(
        table
            .classify(RouteInput::post_resolution(
                &current,
                "203.0.113.10".parse().expect("address")
            ))
            .rule_id()
            .as_str(),
        "post"
    );
}

#[test]
fn first_match_precedence_and_action_fields_are_stable() {
    let table = CompiledRouteTable::compile(
        9,
        vec![
            RouteRuleSpec::new(
                id("first"),
                RouteMatchSpec {
                    domain_suffix: vec![DomainName::parse("example").expect("domain")],
                    ..RouteMatchSpec::default()
                },
                RouteAction::new(
                    EgressAction::Outbound(id("primary")),
                    Some(id("privacy")),
                    TrafficIntent::Realtime,
                ),
            ),
            RouteRuleSpec::new(
                id("second"),
                RouteMatchSpec {
                    domain_exact: vec![DomainName::parse("call.example").expect("domain")],
                    ..RouteMatchSpec::default()
                },
                RouteAction::new(EgressAction::Reject, None, TrafficIntent::Interactive),
            ),
            default_rule(),
        ],
    )
    .expect("table");
    let current = flow("call.example", Network::Udp);
    let decision = table.classify(RouteInput::pre_resolution(&current));
    assert_eq!(decision.rule_id().as_str(), "first");
    assert_eq!(
        decision.action().egress(),
        &EgressAction::Outbound(id("primary"))
    );
    assert_eq!(
        decision.action().dns_plan().expect("DNS plan").as_str(),
        "privacy"
    );
    assert_eq!(decision.action().traffic_intent(), TrafficIntent::Realtime);
    assert_eq!(decision.explanation(), "matched route rule 'first'");
}

#[test]
fn compile_rejects_ambiguous_precedence_and_unbounded_inputs() {
    let early_default = CompiledRouteTable::compile(
        1,
        vec![
            default_rule(),
            RouteRuleSpec::new(
                id("later"),
                RouteMatchSpec {
                    networks: vec![Network::Tcp],
                    ..RouteMatchSpec::default()
                },
                RouteAction::direct(TrafficIntent::Interactive),
            ),
        ],
    );
    assert!(matches!(
        early_default,
        Err(RouteCompileError::ShadowingDefaultRule(_))
    ));

    let duplicate = CompiledRouteTable::compile(1, vec![default_rule(), default_rule()]);
    assert!(matches!(
        duplicate,
        Err(RouteCompileError::ShadowingDefaultRule(_))
            | Err(RouteCompileError::DuplicateRuleId(_))
    ));

    let post_dns_plan = CompiledRouteTable::compile(
        1,
        vec![
            RouteRuleSpec::new(
                id("post-only"),
                RouteMatchSpec {
                    destination_cidrs: vec!["1.1.1.0/24".parse().expect("CIDR")],
                    stages: vec![RouteStage::PostResolution],
                    ..RouteMatchSpec::default()
                },
                RouteAction::new(
                    EgressAction::Outbound(id("edge")),
                    Some(id("too-late")),
                    TrafficIntent::Interactive,
                ),
            ),
            default_rule(),
        ],
    );
    assert!(matches!(
        post_dns_plan,
        Err(RouteCompileError::PostResolutionDnsPlan(rule)) if rule.as_str() == "post-only"
    ));

    let bad_regex = CompiledRouteTable::compile(
        1,
        vec![
            RouteRuleSpec::new(
                id("bad"),
                RouteMatchSpec {
                    domain_regex: vec!["(".to_owned()],
                    ..RouteMatchSpec::default()
                },
                RouteAction::direct(TrafficIntent::Interactive),
            ),
            default_rule(),
        ],
    );
    assert!(matches!(
        bad_regex,
        Err(RouteCompileError::InvalidDomainRegex { .. })
    ));
}

#[test]
fn explanation_rejects_control_character_injection() {
    let mut rule = RouteRuleSpec::new(
        id("unsafe"),
        RouteMatchSpec {
            networks: vec![Network::Tcp],
            ..RouteMatchSpec::default()
        },
        RouteAction::direct(TrafficIntent::Interactive),
    );
    rule.explanation = Some("matched\r\nforged-event".to_owned());
    assert!(matches!(
        CompiledRouteTable::compile(1, vec![rule, default_rule()]),
        Err(RouteCompileError::InvalidExplanation(_))
    ));
}

#[test]
fn replay_vectors_produce_identical_rule_ids() {
    let table = CompiledRouteTable::compile(
        77,
        vec![
            RouteRuleSpec::new(
                id("udp"),
                RouteMatchSpec {
                    networks: vec![Network::Udp],
                    ..RouteMatchSpec::default()
                },
                RouteAction::new(
                    EgressAction::Outbound(id("datagram-edge")),
                    None,
                    TrafficIntent::Realtime,
                ),
            ),
            RouteRuleSpec::new(
                id("example"),
                RouteMatchSpec {
                    domain_suffix: vec![DomainName::parse("example").expect("domain")],
                    ..RouteMatchSpec::default()
                },
                RouteAction::new(
                    EgressAction::Balancer(id("web")),
                    Some(id("split")),
                    TrafficIntent::Interactive,
                ),
            ),
            default_rule(),
        ],
    )
    .expect("table");
    let vectors = [
        ("DNS.Example.", Network::Udp, "udp"),
        ("WWW.Example", Network::Tcp, "example"),
        ("other.test", Network::Tcp, "default"),
    ];
    for round in 0..256 {
        for (domain, network, expected) in vectors {
            let current = flow(domain, network);
            let decision = if round % 2 == 0 {
                table.classify(RouteInput::pre_resolution(&current))
            } else {
                table.classify(RouteInput::post_resolution(
                    &current,
                    "203.0.113.8".parse().expect("address"),
                ))
            };
            assert_eq!(decision.rule_id().as_str(), expected);
        }
    }
}
