use super::*;
use crate::product::{DomainName, InboundId, Network, PortRange, PrincipalId, SourceEndpoint};
use ipnet::IpNet;
use std::str::FromStr;

fn id<T: FromStr>(value: &str) -> T
where
    T::Err: fmt::Debug,
{
    value.parse().expect("valid ID")
}

fn domain_flow(domain: &str) -> FlowContext {
    FlowContext::new(
        Network::Tcp,
        ProtocolTarget::from_host_port(domain, 443).expect("target"),
        SourceEndpoint::new("198.51.100.7".parse().expect("source"), 41000),
        id::<PrincipalId>("alice"),
        id::<InboundId>("socks"),
    )
}

fn ip_flow(address: &str) -> FlowContext {
    FlowContext::new(
        Network::Tcp,
        ProtocolTarget::from_ip(address.parse().expect("address"), 443).expect("target"),
        SourceEndpoint::new("198.51.100.7".parse().expect("source"), 41000),
        id("alice"),
        id("socks"),
    )
}

#[test]
fn safe_default_allows_public_ipv4_and_ipv6() {
    let acl = DestinationAcl::safe_default(1);
    for address in ["8.8.8.8", "2606:4700:4700::1111"] {
        let decision = acl
            .evaluate_pre_resolution(ip_flow(address))
            .expect("public pre-check");
        let authorized = acl.authorize_literal(decision).expect("public post-check");
        assert_eq!(
            authorized.addresses(),
            &[address.parse::<IpAddr>().expect("address")]
        );
    }
}

#[test]
fn safe_default_denies_all_restricted_classes() {
    let cases = [
        ("0.0.0.0", RestrictedIpClass::Unspecified),
        ("::", RestrictedIpClass::Unspecified),
        ("127.0.0.1", RestrictedIpClass::Loopback),
        ("::1", RestrictedIpClass::Loopback),
        ("10.1.2.3", RestrictedIpClass::Private),
        ("fc00::1", RestrictedIpClass::Private),
        ("169.254.12.3", RestrictedIpClass::LinkLocal),
        ("fe80::1", RestrictedIpClass::LinkLocal),
        ("224.0.0.1", RestrictedIpClass::Multicast),
        ("ff02::1", RestrictedIpClass::Multicast),
        ("169.254.169.254", RestrictedIpClass::Metadata),
        ("100.100.100.200", RestrictedIpClass::Metadata),
        ("fd00:ec2::254", RestrictedIpClass::Metadata),
    ];
    let acl = DestinationAcl::safe_default(1);
    for (address, expected_class) in cases {
        let error = acl
            .evaluate_pre_resolution(ip_flow(address))
            .expect_err("restricted address");
        assert!(matches!(
            error,
            AclError::RestrictedAddress { class, .. } if class == expected_class
        ));
    }
}

#[test]
fn ipv4_mapped_ipv6_cannot_bypass_ipv4_policy() {
    let acl = DestinationAcl::safe_default(1);
    let error = acl
        .evaluate_pre_resolution(ip_flow("::ffff:127.0.0.1"))
        .expect_err("mapped loopback");
    assert!(matches!(
        error,
        AclError::RestrictedAddress {
            class: RestrictedIpClass::Loopback,
            ..
        }
    ));
}

#[test]
fn restricted_override_must_be_explicit() {
    let matcher = RouteMatchSpec {
        domain_suffix: vec![DomainName::parse("home.arpa").expect("domain")],
        destination_cidrs: vec!["192.168.0.0/16".parse::<IpNet>().expect("CIDR")],
        destination_ports: vec![PortRange::single(443)],
        ..RouteMatchSpec::default()
    };
    let ordinary_allow = DestinationAcl::compile(
        1,
        vec![AclRuleSpec::new(
            id("home"),
            matcher.clone(),
            AclEffect::Allow,
        )],
    )
    .expect("ACL");
    let flow = domain_flow("router.home.arpa");
    let decision = ordinary_allow
        .evaluate_pre_resolution(flow)
        .expect("domain pre-check");
    assert!(matches!(
        ordinary_allow.authorize_resolution(decision, &["192.168.1.1".parse().expect("address")]),
        Err(AclError::RestrictedAddress { .. })
    ));

    let override_acl = DestinationAcl::compile(
        2,
        vec![AclRuleSpec::new(
            id("home"),
            matcher,
            AclEffect::AllowRestricted,
        )],
    )
    .expect("ACL");
    let decision = override_acl
        .evaluate_pre_resolution(domain_flow("router.home.arpa"))
        .expect("pre-check");
    let authorized = override_acl
        .authorize_resolution(decision, &["192.168.1.1".parse().expect("address")])
        .expect("explicit override");
    assert_eq!(
        authorized.addresses(),
        &["192.168.1.1".parse::<IpAddr>().expect("address")]
    );
}

#[test]
fn override_does_not_leak_beyond_match_constraints() {
    let acl = DestinationAcl::compile(
        3,
        vec![AclRuleSpec::new(
            id("one-service"),
            RouteMatchSpec {
                domain_exact: vec![DomainName::parse("router.home").expect("domain")],
                destination_cidrs: vec!["192.168.1.1/32".parse().expect("CIDR")],
                ..RouteMatchSpec::default()
            },
            AclEffect::AllowRestricted,
        )],
    )
    .expect("ACL");
    let decision = acl
        .evaluate_pre_resolution(domain_flow("router.home"))
        .expect("pre-check");
    assert!(matches!(
        acl.authorize_resolution(decision, &["192.168.1.2".parse().expect("address")]),
        Err(AclError::RestrictedAddress { .. })
    ));
}

#[test]
fn ordered_explicit_deny_applies_pre_and_post_resolution() {
    let acl = DestinationAcl::compile(
        4,
        vec![
            AclRuleSpec::new(
                id("blocked-domain"),
                RouteMatchSpec {
                    domain_suffix: vec![DomainName::parse("blocked.example").expect("domain")],
                    ..RouteMatchSpec::default()
                },
                AclEffect::Deny,
            ),
            AclRuleSpec::new(
                id("blocked-range"),
                RouteMatchSpec {
                    destination_cidrs: vec!["203.0.113.0/24".parse().expect("CIDR")],
                    ..RouteMatchSpec::default()
                },
                AclEffect::Deny,
            ),
        ],
    )
    .expect("ACL");
    assert!(matches!(
        acl.evaluate_pre_resolution(domain_flow("api.blocked.example")),
        Err(AclError::DeniedByRule { rule_id }) if rule_id.as_str() == "blocked-domain"
    ));

    let decision = acl
        .evaluate_pre_resolution(domain_flow("allowed.example"))
        .expect("pre-check");
    assert!(matches!(
        acl.authorize_resolution(
            decision,
            &["203.0.113.12".parse().expect("address")]
        ),
        Err(AclError::DeniedByRule { rule_id }) if rule_id.as_str() == "blocked-range"
    ));
}

#[test]
fn explicit_first_match_policy_controls_domain_delegation() {
    let current = domain_flow("service.example");
    let safe = DestinationAcl::safe_default(5);
    assert!(!safe.requires_post_resolution(&current));
    let decision = safe
        .evaluate_pre_resolution(current.clone())
        .expect("pre-resolution decision");
    let delegated = safe.authorize_domain(decision).expect("domain proof");
    assert_eq!(delegated.flow(), &current);

    let address_first = DestinationAcl::compile(
        6,
        vec![
            AclRuleSpec::new(
                id("blocked-address"),
                RouteMatchSpec {
                    destination_cidrs: vec!["203.0.113.0/24".parse().expect("CIDR")],
                    ..RouteMatchSpec::default()
                },
                AclEffect::Deny,
            ),
            AclRuleSpec::new(
                id("domain"),
                RouteMatchSpec {
                    domain_exact: vec![DomainName::parse("service.example").expect("domain")],
                    ..RouteMatchSpec::default()
                },
                AclEffect::Allow,
            ),
        ],
    )
    .expect("address-first ACL");
    assert!(address_first.requires_post_resolution(&current));
    let decision = address_first
        .evaluate_pre_resolution(current.clone())
        .expect("pre-resolution decision");
    assert!(matches!(
        address_first.authorize_domain(decision),
        Err(AclError::PostResolutionRequired)
    ));

    let domain_first = DestinationAcl::compile(
        7,
        vec![
            AclRuleSpec::new(
                id("domain"),
                RouteMatchSpec {
                    domain_exact: vec![DomainName::parse("service.example").expect("domain")],
                    ..RouteMatchSpec::default()
                },
                AclEffect::Allow,
            ),
            AclRuleSpec::new(
                id("blocked-address"),
                RouteMatchSpec {
                    destination_cidrs: vec!["203.0.113.0/24".parse().expect("CIDR")],
                    ..RouteMatchSpec::default()
                },
                AclEffect::Deny,
            ),
        ],
    )
    .expect("domain-first ACL");
    assert!(!domain_first.requires_post_resolution(&current));

    let address_allowlist = DestinationAcl::compile(
        8,
        vec![
            AclRuleSpec::new(
                id("allowed-address"),
                RouteMatchSpec {
                    destination_cidrs: vec!["203.0.113.0/24".parse().expect("CIDR")],
                    ..RouteMatchSpec::default()
                },
                AclEffect::Allow,
            ),
            AclRuleSpec::new(
                id("default-deny"),
                RouteMatchSpec::default(),
                AclEffect::Deny,
            ),
        ],
    )
    .expect("address allowlist ACL");
    let decision = address_allowlist
        .evaluate_pre_resolution(current.clone())
        .expect("default denial is provisional while an earlier address rule can match");
    assert!(decision.requires_post_resolution());
    assert!(matches!(
        address_allowlist.authorize_domain(decision.clone()),
        Err(AclError::PostResolutionRequired)
    ));
    assert_eq!(
        address_allowlist
            .authorize_resolution(
                decision,
                &["203.0.113.9".parse().expect("allowlisted address")],
            )
            .expect("post-resolution allowlist authorization")
            .addresses(),
        &["203.0.113.9"
            .parse::<IpAddr>()
            .expect("allowlisted address")]
    );
}

#[test]
fn all_dns_answers_must_pass_policy() {
    let acl = DestinationAcl::safe_default(5);
    let decision = acl
        .evaluate_pre_resolution(domain_flow("mixed.example"))
        .expect("pre-check");
    assert!(matches!(
        acl.authorize_resolution(
            decision,
            &[
                "203.0.113.5".parse().expect("public"),
                "127.0.0.1".parse().expect("loopback")
            ]
        ),
        Err(AclError::RestrictedAddress {
            class: RestrictedIpClass::Loopback,
            ..
        })
    ));
}

#[test]
fn authorized_resolution_preserves_preference_order_deduplicates_and_binds() {
    let acl = DestinationAcl::safe_default(6);
    let original = domain_flow("service.example");
    let decision = acl
        .evaluate_pre_resolution(original.clone())
        .expect("pre-check");
    let resolution = acl
        .authorize_resolution(
            decision,
            &[
                "203.0.113.9".parse().expect("address"),
                "2001:db8::9".parse().expect("address"),
                "203.0.113.9".parse().expect("address"),
            ],
        )
        .expect("resolution");
    assert_eq!(resolution.addresses().len(), 2);
    assert_eq!(
        resolution.addresses(),
        &[
            "203.0.113.9".parse::<IpAddr>().expect("address"),
            "2001:db8::9".parse::<IpAddr>().expect("address")
        ]
    );
    let authorized = resolution
        .authorize_connect(original.target(), "203.0.113.9".parse().expect("address"))
        .expect("authorized connect");
    assert_eq!(authorized.acl_generation(), 6);
    assert_eq!(authorized.flow(), &original);
}

#[test]
fn post_dns_rebinding_and_target_substitution_are_rejected() {
    let acl = DestinationAcl::safe_default(7);
    let original = domain_flow("service.example");
    let decision = acl
        .evaluate_pre_resolution(original.clone())
        .expect("pre-check");
    let resolution = acl
        .authorize_resolution(decision, &["203.0.113.9".parse().expect("address")])
        .expect("resolution");
    assert!(matches!(
        resolution.authorize_connect(original.target(), "203.0.113.10".parse().expect("address")),
        Err(AclError::DnsRebinding { .. })
    ));
    let replacement =
        ProtocolTarget::from_host_port("other.example", 443).expect("replacement target");
    assert!(matches!(
        resolution.authorize_connect(&replacement, "203.0.113.9".parse().expect("address")),
        Err(AclError::TargetChanged)
    ));
}

#[test]
fn pre_resolution_decision_cannot_cross_acl_generation() {
    let old = DestinationAcl::safe_default(8);
    let new = DestinationAcl::safe_default(9);
    let decision = old
        .evaluate_pre_resolution(domain_flow("service.example"))
        .expect("pre-check");
    assert!(matches!(
        new.authorize_resolution(decision, &["203.0.113.9".parse().expect("address")]),
        Err(AclError::GenerationMismatch {
            evaluated: 8,
            current: 9
        })
    ));
}

#[test]
fn acl_replay_vectors_are_stable() {
    let acl = DestinationAcl::compile(
        10,
        vec![
            AclRuleSpec::new(
                id("deny-admin"),
                RouteMatchSpec {
                    domain_exact: vec![DomainName::parse("admin.example").expect("domain")],
                    ..RouteMatchSpec::default()
                },
                AclEffect::Deny,
            ),
            AclRuleSpec::new(
                id("lab"),
                RouteMatchSpec {
                    domain_suffix: vec![DomainName::parse("lab.example").expect("domain")],
                    destination_cidrs: vec!["10.42.0.0/16".parse().expect("CIDR")],
                    ..RouteMatchSpec::default()
                },
                AclEffect::AllowRestricted,
            ),
        ],
    )
    .expect("ACL");

    for _ in 0..256 {
        assert!(matches!(
            acl.evaluate(RouteInput::pre_resolution(&domain_flow("admin.example"))),
            AclVerdict::DeniedByRule { rule_id } if rule_id.as_str() == "deny-admin"
        ));
        let lab = domain_flow("host.lab.example");
        assert!(matches!(
            acl.evaluate(RouteInput::post_resolution(
                &lab,
                "10.42.2.3".parse().expect("address")
            )),
            AclVerdict::Allowed {
                rule_id: Some(rule_id),
                restricted_override: true
            } if rule_id.as_str() == "lab"
        ));
    }
}
