use super::*;
use crate::product::{
    DomainName, EgressAction, FlowContext, InboundId, InitialDemand, Network, OutboundId,
    PrincipalId, ProductPolicyGeneration, ProtocolTarget, RouteAction, RouteMatchSpec,
    RouteRuleSpec, RuleId, SourceEndpoint,
};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

fn id<T: std::str::FromStr>(value: &str) -> T
where
    T::Err: std::fmt::Debug,
{
    value.parse().expect("valid policy ID")
}

fn domain_flow(domain: &str) -> Arc<FlowContext> {
    Arc::new(FlowContext::new(
        Network::Tcp,
        ProtocolTarget::from_host_port(domain, 443).expect("target"),
        SourceEndpoint::new(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 7)), 40_000),
        id::<PrincipalId>("alice"),
        id::<InboundId>("local-mixed"),
    ))
}

fn table(
    first: Option<(RouteMatchSpec, RouteAction)>,
    fallback: RouteAction,
) -> ProductPolicyGeneration {
    let mut rules = Vec::new();
    if let Some((matcher, action)) = first {
        rules.push(RouteRuleSpec::new(
            id::<RuleId>("selected"),
            matcher,
            action,
        ));
    }
    rules.push(RouteRuleSpec::new(
        id::<RuleId>("default"),
        RouteMatchSpec::default(),
        fallback,
    ));
    ProductPolicyGeneration::compile(7, rules).expect("policy")
}

fn direct() -> RouteAction {
    RouteAction::direct(InitialDemand::Automatic)
}

fn private_match() -> RouteMatchSpec {
    RouteMatchSpec {
        destination_cidrs: vec!["10.0.0.0/8".parse().expect("CIDR")],
        ..RouteMatchSpec::default()
    }
}

#[test]
fn plain_allow_rejects_restricted_answers_but_allow_restricted_authorizes_them() {
    let flow = domain_flow("internal.example");
    let private = IpAddr::V4(Ipv4Addr::new(10, 42, 0, 9));

    let ordinary = table(Some((private_match(), direct())), direct());
    let pre = ordinary
        .evaluate_pre_resolution_shared(Arc::clone(&flow))
        .expect("pre-resolution decision");
    assert!(matches!(
        ordinary.authorize_resolution(pre, &[private], |_, _| true),
        Err(RouteAuthorizationError::RestrictedAddress {
            class: RestrictedIpClass::Private,
            ..
        })
    ));

    let restricted = table(
        Some((
            private_match(),
            RouteAction::allow_restricted(
                EgressAction::Outbound(id::<OutboundId>("corp")),
                None,
                InitialDemand::Automatic,
            ),
        )),
        RouteAction::reject(),
    );
    let pre = restricted
        .evaluate_pre_resolution_shared(flow)
        .expect("address-dependent decision");
    let authorized = restricted
        .authorize_resolution(pre, &[private], |_, _| true)
        .expect("restricted route authorizes private address");
    assert_eq!(authorized.targets().len(), 1);
    assert_eq!(authorized.targets()[0].address(), private);
    assert_eq!(
        authorized.targets()[0].permit().rule_id().as_str(),
        "selected"
    );
}

#[test]
fn mixed_public_answers_filter_terminal_rules_but_restricted_plain_allow_fails_all() {
    let flow = domain_flow("split.example");
    let rejected = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 10));
    let allowed = IpAddr::V4(Ipv4Addr::new(198, 51, 100, 20));
    let private = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 20));
    let policy = table(
        Some((
            RouteMatchSpec {
                destination_cidrs: vec!["203.0.113.0/24".parse().expect("CIDR")],
                ..RouteMatchSpec::default()
            },
            RouteAction::reject(),
        )),
        direct(),
    );

    let pre = policy
        .evaluate_pre_resolution_shared(Arc::clone(&flow))
        .expect("post-resolution routing");
    let result = policy
        .authorize_resolution(pre, &[rejected, allowed], |_, _| true)
        .expect("one public address survives");
    assert_eq!(result.targets().len(), 1);
    assert_eq!(result.targets()[0].address(), allowed);

    let pre = policy
        .evaluate_pre_resolution_shared(flow)
        .expect("post-resolution routing");
    assert!(matches!(
        policy.authorize_resolution(pre, &[rejected, private], |_, _| true),
        Err(RouteAuthorizationError::RestrictedAddress { address, .. }) if address == private
    ));
}

#[test]
fn terminal_drop_dominates_reject_when_no_address_is_allowed() {
    let flow = domain_flow("blocked.example");
    let drop_address = IpAddr::V4(Ipv4Addr::new(198, 51, 100, 8));
    let reject_address = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 8));
    let policy = ProductPolicyGeneration::compile(
        7,
        vec![
            RouteRuleSpec::new(
                id("drop"),
                RouteMatchSpec {
                    destination_cidrs: vec!["198.51.100.0/24".parse().expect("CIDR")],
                    ..RouteMatchSpec::default()
                },
                RouteAction::drop(),
            ),
            RouteRuleSpec::new(
                id("default"),
                RouteMatchSpec::default(),
                RouteAction::reject(),
            ),
        ],
    )
    .expect("policy");
    let pre = policy
        .evaluate_pre_resolution_shared(flow)
        .expect("address-dependent decision");
    assert!(matches!(
        policy.authorize_resolution(pre, &[reject_address, drop_address], |_, _| true),
        Err(RouteAuthorizationError::Dropped { rule_id }) if rule_id.as_str() == "drop"
    ));
}

#[test]
fn domain_delegation_keeps_rule_action_and_rejects_rebinding() {
    let policy = table(None, direct());
    let flow = domain_flow("public.example");
    let pre = policy
        .evaluate_pre_resolution_shared(flow)
        .expect("pre-resolution decision");
    let delegated = policy.authorize_domain(pre).expect("domain permit");
    assert_eq!(delegated.permit().rule_id().as_str(), "default");

    let address = IpAddr::V4(Ipv4Addr::new(198, 51, 100, 42));
    let resolution = policy
        .authorize_domain_resolution(&delegated, &[address])
        .expect("authorized resolution");
    assert!(matches!(
        resolution.authorize_connect(
            delegated.flow().target(),
            IpAddr::V4(Ipv4Addr::new(198, 51, 100, 43))
        ),
        Err(RouteAuthorizationError::DnsRebinding { .. })
    ));
    let replacement =
        ProtocolTarget::from_host_port("other.example", 443).expect("replacement target");
    assert!(matches!(
        resolution.authorize_connect(&replacement, address),
        Err(RouteAuthorizationError::TargetChanged)
    ));
    assert_eq!(
        resolution
            .authorize_connect(delegated.flow().target(), address)
            .expect("original answer")
            .address(),
        address
    );
}

#[test]
fn canonical_resolution_preserves_order_deduplicates_and_binds_the_flow() {
    let policy = table(None, direct());
    let flow = domain_flow("service.example");
    let pre = policy
        .evaluate_pre_resolution_shared(Arc::clone(&flow))
        .expect("pre-resolution decision");
    let ipv4 = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 9));
    let ipv6 = "2001:db8::9".parse::<IpAddr>().expect("IPv6 address");
    let resolution = policy
        .authorize_resolution(pre, &[ipv4, ipv6, ipv4], |_, _| true)
        .expect("authorized resolution");

    assert_eq!(
        resolution
            .targets()
            .iter()
            .map(AuthorizedTarget::address)
            .collect::<Vec<_>>(),
        vec![ipv4, ipv6]
    );
    for target in resolution.targets() {
        assert_eq!(target.resolution(), &[ipv4, ipv6]);
        assert_eq!(target.policy_generation(), 7);
        assert_eq!(target.flow(), flow.as_ref());
    }
    assert_eq!(
        resolution
            .authorize_connect(flow.target(), ipv4)
            .expect("original answer")
            .address(),
        ipv4
    );
}

#[test]
fn permit_generation_and_flow_are_immutable() {
    let old = table(None, direct());
    let flow = domain_flow("public.example");
    let pre = old
        .evaluate_pre_resolution_shared(flow)
        .expect("old decision");
    let new = ProductPolicyGeneration::compile(
        8,
        vec![RouteRuleSpec::new(
            id("default"),
            RouteMatchSpec::default(),
            direct(),
        )],
    )
    .expect("new policy");
    assert!(matches!(
        new.authorize_resolution(
            pre,
            &[IpAddr::V4(Ipv4Addr::new(198, 51, 100, 9))],
            |_, _| true
        ),
        Err(RouteAuthorizationError::GenerationMismatch {
            evaluated: 7,
            current: 8
        })
    ));
}

#[test]
fn restricted_classifier_covers_internal_and_metadata_ranges() {
    let cases = [
        ("0.0.0.0", RestrictedIpClass::Unspecified),
        ("::", RestrictedIpClass::Unspecified),
        ("10.0.0.1", RestrictedIpClass::Private),
        ("fc00::1", RestrictedIpClass::Private),
        ("127.0.0.1", RestrictedIpClass::Loopback),
        ("::1", RestrictedIpClass::Loopback),
        ("169.254.12.3", RestrictedIpClass::LinkLocal),
        ("fe80::1", RestrictedIpClass::LinkLocal),
        ("224.0.0.1", RestrictedIpClass::Multicast),
        ("ff02::1", RestrictedIpClass::Multicast),
        ("169.254.169.254", RestrictedIpClass::Metadata),
        ("169.254.170.2", RestrictedIpClass::Metadata),
        ("100.100.100.200", RestrictedIpClass::Metadata),
    ];
    for (address, class) in cases {
        assert_eq!(
            RestrictedIpClass::classify(address.parse().expect("address")),
            Some(class)
        );
    }
    assert_eq!(
        RestrictedIpClass::classify("198.51.100.1".parse().expect("address")),
        None
    );
}

#[test]
fn ipv4_mapped_ipv6_cannot_bypass_restricted_address_policy() {
    let policy = table(None, direct());
    let pre = policy
        .evaluate_pre_resolution_shared(domain_flow("mapped.example"))
        .expect("pre-resolution decision");
    let mapped = "::ffff:127.0.0.1"
        .parse::<IpAddr>()
        .expect("IPv4-mapped IPv6 address");

    assert!(matches!(
        policy.authorize_resolution(pre, &[mapped], |_, _| true),
        Err(RouteAuthorizationError::RestrictedAddress {
            address: IpAddr::V4(address),
            class: RestrictedIpClass::Loopback,
            ..
        }) if address == Ipv4Addr::LOCALHOST
    ));
}

#[test]
fn route_match_domain_identity_remains_exact() {
    let matcher = RouteMatchSpec {
        domain_exact: vec![DomainName::parse("internal.example").expect("domain")],
        ..RouteMatchSpec::default()
    };
    let policy = table(Some((matcher, RouteAction::reject())), direct());
    assert!(matches!(
        policy.evaluate_pre_resolution_shared(domain_flow("internal.example")),
        Err(RouteAuthorizationError::Rejected { rule_id }) if rule_id.as_str() == "selected"
    ));
}
