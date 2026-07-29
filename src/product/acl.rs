use crate::product::flow::{FlowContext, ProtocolTarget};
use crate::product::routing::{
    CompiledMatcher, RouteCompileError, RouteInput, RouteMatchSpec, RuleId,
};
use std::collections::HashSet;
use std::error::Error;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::Arc;

const MAX_ACL_RULES: usize = 4_096;
const MAX_RESOLUTION_ADDRESSES: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclEffect {
    /// Explicitly deny a matching destination.
    Deny,
    /// Allow a matching public destination. This cannot override the built-in
    /// restricted-address safety policy.
    Allow,
    /// Explicitly opt a matching destination into restricted address space.
    AllowRestricted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclRuleSpec {
    pub id: RuleId,
    pub matcher: RouteMatchSpec,
    pub effect: AclEffect,
}

impl AclRuleSpec {
    pub const fn new(id: RuleId, matcher: RouteMatchSpec, effect: AclEffect) -> Self {
        Self {
            id,
            matcher,
            effect,
        }
    }
}

#[derive(Debug)]
struct CompiledAclRule {
    id: RuleId,
    matcher: CompiledMatcher,
    effect: AclEffect,
}

/// Immutable shared client/server destination authorization generation.
#[derive(Debug)]
pub struct DestinationAcl {
    generation: u64,
    rules: Vec<CompiledAclRule>,
}

impl DestinationAcl {
    pub const fn safe_default(generation: u64) -> Self {
        Self {
            generation,
            rules: Vec::new(),
        }
    }

    pub fn compile(generation: u64, rules: Vec<AclRuleSpec>) -> Result<Self, AclError> {
        if rules.len() > MAX_ACL_RULES {
            return Err(AclError::TooManyRules {
                count: rules.len(),
                maximum: MAX_ACL_RULES,
            });
        }
        let final_index = rules.len().saturating_sub(1);
        let mut ids = HashSet::with_capacity(rules.len());
        let mut compiled = Vec::with_capacity(rules.len());
        for (index, rule) in rules.into_iter().enumerate() {
            if !ids.insert(rule.id.clone()) {
                return Err(AclError::DuplicateRuleId(rule.id));
            }
            if rule.matcher.is_catch_all() && index != final_index {
                return Err(AclError::ShadowingCatchAll(rule.id));
            }
            let matcher =
                CompiledMatcher::compile(rule.matcher, &rule.id).map_err(AclError::Matcher)?;
            compiled.push(CompiledAclRule {
                id: rule.id,
                matcher,
                effect: rule.effect,
            });
        }
        Ok(Self {
            generation,
            rules: compiled,
        })
    }

    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Return whether an explicit ACL rule can change the pre-resolution
    /// verdict once a domain has address evidence.
    ///
    /// This is deliberately about configured first-match rules. The built-in
    /// restricted-address guard is always applied when MPTUNNEL owns a native
    /// connection or otherwise resolves a target. A configured proxy that
    /// receives an unresolved domain is the resolution trust boundary, so it
    /// does not trigger a query through the selected DNS plan by itself.
    pub fn requires_post_resolution(&self, flow: &FlowContext) -> bool {
        if let Some(address) = flow.target().ip() {
            let pre_input = RouteInput::pre_resolution(flow);
            let selected = self
                .rules
                .iter()
                .position(|rule| rule.matcher.matches(pre_input));
            let post_input = RouteInput::post_resolution(flow, address);
            let post_selected = self
                .rules
                .iter()
                .position(|rule| rule.matcher.matches(post_input));
            return post_selected != selected;
        }
        self.pre_resolution_verdict_and_demand(flow).1
    }

    /// Evaluate without allocating. This is suitable for route explanation and
    /// dry-run tooling.
    pub fn evaluate<'a>(&'a self, input: RouteInput<'_>) -> AclVerdict<'a> {
        let matched = self.rules.iter().find(|rule| rule.matcher.matches(input));
        if let Some(rule) = matched
            && rule.effect == AclEffect::Deny
        {
            return AclVerdict::DeniedByRule { rule_id: &rule.id };
        }

        if let Some(address) = input.destination_ip()
            && let Some(class) = restricted_ip_class(address)
        {
            if let Some(rule) = matched
                && rule.effect == AclEffect::AllowRestricted
            {
                return AclVerdict::Allowed {
                    rule_id: Some(&rule.id),
                    restricted_override: true,
                };
            }
            return AclVerdict::DeniedRestricted { address, class };
        }

        AclVerdict::Allowed {
            rule_id: matched.map(|rule| &rule.id),
            restricted_override: false,
        }
    }

    /// Bind one immutable normalized flow to this ACL generation before DNS.
    pub fn evaluate_pre_resolution(
        &self,
        flow: FlowContext,
    ) -> Result<PreResolutionDecision, AclError> {
        self.evaluate_pre_resolution_shared(Arc::new(flow))
    }

    pub(crate) fn evaluate_pre_resolution_shared(
        &self,
        flow: Arc<FlowContext>,
    ) -> Result<PreResolutionDecision, AclError> {
        let (verdict, requires_post_resolution) =
            self.pre_resolution_verdict_and_demand(flow.as_ref());
        if !requires_post_resolution {
            verdict_to_result(verdict)?;
        }
        Ok(PreResolutionDecision {
            acl_generation: self.generation,
            flow,
            requires_post_resolution,
        })
    }

    /// Authorize every DNS answer. One disallowed answer fails the whole
    /// resolution rather than being silently filtered into a policy-dependent
    /// fallback.
    pub fn authorize_resolution(
        &self,
        decision: PreResolutionDecision,
        addresses: &[IpAddr],
    ) -> Result<AuthorizedResolution, AclError> {
        if decision.acl_generation != self.generation {
            return Err(AclError::GenerationMismatch {
                evaluated: decision.acl_generation,
                current: self.generation,
            });
        }
        if addresses.is_empty() {
            return Err(AclError::EmptyResolution);
        }
        if addresses.len() > MAX_RESOLUTION_ADDRESSES {
            return Err(AclError::TooManyResolutionAddresses {
                count: addresses.len(),
                maximum: MAX_RESOLUTION_ADDRESSES,
            });
        }

        let literal = decision.flow.target().ip();
        let mut canonical = Vec::with_capacity(addresses.len());
        for address in addresses.iter().copied().map(canonical_ip) {
            if literal.is_some_and(|literal| literal != address) {
                return Err(AclError::TargetChanged);
            }
            verdict_to_result(self.evaluate(RouteInput::post_resolution(&decision.flow, address)))?;
            if !canonical.contains(&address) {
                canonical.push(address);
            }
        }

        Ok(AuthorizedResolution {
            acl_generation: self.generation,
            flow: decision.flow,
            addresses: canonical,
        })
    }

    pub fn authorize_literal(
        &self,
        decision: PreResolutionDecision,
    ) -> Result<AuthorizedResolution, AclError> {
        let address = decision
            .flow
            .target()
            .ip()
            .ok_or(AclError::ExpectedLiteralIp)?;
        self.authorize_resolution(decision, &[address])
    }

    /// Authorize delegation of the canonical domain to a configured
    /// domain-capable outbound. Explicit post-resolution ACL dependencies must
    /// be resolved through the selected DNS plan and therefore cannot produce
    /// this proof.
    pub fn authorize_domain(
        &self,
        decision: PreResolutionDecision,
    ) -> Result<AuthorizedDomainTarget, AclError> {
        if decision.acl_generation != self.generation {
            return Err(AclError::GenerationMismatch {
                evaluated: decision.acl_generation,
                current: self.generation,
            });
        }
        if decision.flow.target().domain().is_none() {
            return Err(AclError::ExpectedDomain);
        }
        if decision.requires_post_resolution {
            return Err(AclError::PostResolutionRequired);
        }
        Ok(AuthorizedDomainTarget { decision })
    }

    /// Promote a delegated-domain proof to exact address proofs when a later
    /// IP-only leaf requires address evidence from the selected DNS plan.
    pub fn authorize_domain_resolution(
        &self,
        domain: &AuthorizedDomainTarget,
        addresses: &[IpAddr],
    ) -> Result<AuthorizedResolution, AclError> {
        self.authorize_resolution(domain.decision.clone(), addresses)
    }

    fn pre_resolution_verdict_and_demand<'a>(
        &'a self,
        flow: &FlowContext,
    ) -> (AclVerdict<'a>, bool) {
        if let Some(address) = flow.target().ip() {
            let pre_input = RouteInput::pre_resolution(flow);
            let post_input = RouteInput::post_resolution(flow, address);
            let pre_selected = self
                .rules
                .iter()
                .position(|rule| rule.matcher.matches(pre_input));
            let post_selected = self
                .rules
                .iter()
                .position(|rule| rule.matcher.matches(post_input));
            return (self.evaluate(pre_input), pre_selected != post_selected);
        }

        let input = RouteInput::pre_resolution(flow);
        let mut earlier_post_candidate = false;
        for rule in &self.rules {
            if rule.matcher.matches(input) {
                let verdict = if rule.effect == AclEffect::Deny {
                    AclVerdict::DeniedByRule { rule_id: &rule.id }
                } else {
                    AclVerdict::Allowed {
                        rule_id: Some(&rule.id),
                        restricted_override: false,
                    }
                };
                return (
                    verdict,
                    earlier_post_candidate || !rule.matcher.could_match_post_resolution(flow),
                );
            }
            earlier_post_candidate |= rule.matcher.could_match_post_resolution(flow);
        }
        (
            AclVerdict::Allowed {
                rule_id: None,
                restricted_override: false,
            },
            earlier_post_candidate,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclVerdict<'a> {
    Allowed {
        rule_id: Option<&'a RuleId>,
        restricted_override: bool,
    },
    DeniedByRule {
        rule_id: &'a RuleId,
    },
    DeniedRestricted {
        address: IpAddr,
        class: RestrictedIpClass,
    },
}

/// Unforgeable pre-resolution policy decision for one immutable flow.
///
/// Stable denials are returned as errors. A decision may instead require
/// post-resolution evidence before it can authorize either a domain or exact
/// addresses.
#[derive(Debug, Clone)]
pub struct PreResolutionDecision {
    acl_generation: u64,
    flow: Arc<FlowContext>,
    requires_post_resolution: bool,
}

/// Unforgeable proof that one normalized domain may cross a configured
/// domain-capable outbound without target resolution at this node.
#[derive(Debug, Clone)]
pub struct AuthorizedDomainTarget {
    decision: PreResolutionDecision,
}

impl AuthorizedDomainTarget {
    pub const fn acl_generation(&self) -> u64 {
        self.decision.acl_generation()
    }

    pub fn flow(&self) -> &FlowContext {
        self.decision.flow()
    }
}

impl PreResolutionDecision {
    pub const fn acl_generation(&self) -> u64 {
        self.acl_generation
    }

    pub fn flow(&self) -> &FlowContext {
        self.flow.as_ref()
    }

    pub const fn requires_post_resolution(&self) -> bool {
        self.requires_post_resolution
    }
}

/// Exact normalized addresses authorized for one immutable DNS result.
#[derive(Debug)]
pub struct AuthorizedResolution {
    acl_generation: u64,
    flow: Arc<FlowContext>,
    addresses: Vec<IpAddr>,
}

impl AuthorizedResolution {
    pub const fn acl_generation(&self) -> u64 {
        self.acl_generation
    }

    pub fn flow(&self) -> &FlowContext {
        self.flow.as_ref()
    }

    pub fn addresses(&self) -> &[IpAddr] {
        &self.addresses
    }

    /// Bind a connector to the original target and an exact authorized DNS
    /// answer. A later re-resolution or target substitution is rejected.
    pub fn authorize_connect(
        &self,
        target: &ProtocolTarget,
        address: IpAddr,
    ) -> Result<AuthorizedTarget, AclError> {
        if target != self.flow.target() {
            return Err(AclError::TargetChanged);
        }
        let address = canonical_ip(address);
        if !self.addresses.contains(&address) {
            return Err(AclError::DnsRebinding { address });
        }
        Ok(AuthorizedTarget {
            acl_generation: self.acl_generation,
            flow: self.flow.clone(),
            address,
        })
    }
}

/// Immutable unforgeable Product proof passed to an outbound connector.
#[derive(Debug, Clone)]
pub struct AuthorizedTarget {
    acl_generation: u64,
    flow: Arc<FlowContext>,
    address: IpAddr,
}

impl AuthorizedTarget {
    pub const fn acl_generation(&self) -> u64 {
        self.acl_generation
    }

    pub fn flow(&self) -> &FlowContext {
        self.flow.as_ref()
    }

    pub const fn address(&self) -> IpAddr {
        self.address
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestrictedIpClass {
    Metadata,
    Unspecified,
    Loopback,
    Private,
    LinkLocal,
    Multicast,
}

impl fmt::Display for RestrictedIpClass {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Metadata => "metadata",
            Self::Unspecified => "unspecified",
            Self::Loopback => "loopback",
            Self::Private => "private",
            Self::LinkLocal => "link-local",
            Self::Multicast => "multicast",
        })
    }
}

#[derive(Debug)]
pub enum AclError {
    DuplicateRuleId(RuleId),
    ShadowingCatchAll(RuleId),
    TooManyRules {
        count: usize,
        maximum: usize,
    },
    Matcher(RouteCompileError),
    DeniedByRule {
        rule_id: RuleId,
    },
    RestrictedAddress {
        address: IpAddr,
        class: RestrictedIpClass,
    },
    EmptyResolution,
    TooManyResolutionAddresses {
        count: usize,
        maximum: usize,
    },
    GenerationMismatch {
        evaluated: u64,
        current: u64,
    },
    TargetChanged,
    DnsRebinding {
        address: IpAddr,
    },
    ExpectedLiteralIp,
    ExpectedDomain,
    PostResolutionRequired,
}

impl fmt::Display for AclError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateRuleId(id) => {
                write!(formatter, "duplicate destination ACL rule ID {id}")
            }
            Self::ShadowingCatchAll(id) => {
                write!(
                    formatter,
                    "catch-all destination ACL rule {id} must be last"
                )
            }
            Self::TooManyRules { count, maximum } => {
                write!(
                    formatter,
                    "destination ACL has {count} rules; maximum is {maximum}"
                )
            }
            Self::Matcher(error) => write!(formatter, "invalid destination ACL matcher: {error}"),
            Self::DeniedByRule { rule_id } => {
                write!(formatter, "destination denied by ACL rule {rule_id}")
            }
            Self::RestrictedAddress { address, class } => {
                write!(formatter, "{address} is denied {class} address space")
            }
            Self::EmptyResolution => formatter.write_str("DNS resolution returned no addresses"),
            Self::TooManyResolutionAddresses { count, maximum } => write!(
                formatter,
                "DNS resolution returned {count} addresses; maximum is {maximum}"
            ),
            Self::GenerationMismatch { evaluated, current } => write!(
                formatter,
                "destination decision generation {evaluated} does not match ACL generation {current}"
            ),
            Self::TargetChanged => {
                formatter.write_str("destination changed after the pre-resolution decision")
            }
            Self::DnsRebinding { address } => {
                write!(
                    formatter,
                    "connect address {address} was not in the authorized DNS result"
                )
            }
            Self::ExpectedLiteralIp => {
                formatter.write_str("literal authorization requires an IP target")
            }
            Self::ExpectedDomain => {
                formatter.write_str("domain delegation authorization requires a domain target")
            }
            Self::PostResolutionRequired => {
                formatter.write_str("destination ACL requires post-resolution authorization")
            }
        }
    }
}

impl Error for AclError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Matcher(error) => Some(error),
            _ => None,
        }
    }
}

fn verdict_to_result(verdict: AclVerdict<'_>) -> Result<(), AclError> {
    match verdict {
        AclVerdict::Allowed { .. } => Ok(()),
        AclVerdict::DeniedByRule { rule_id } => Err(AclError::DeniedByRule {
            rule_id: rule_id.clone(),
        }),
        AclVerdict::DeniedRestricted { address, class } => {
            Err(AclError::RestrictedAddress { address, class })
        }
    }
}

fn restricted_ip_class(address: IpAddr) -> Option<RestrictedIpClass> {
    let address = canonical_ip(address);
    if is_metadata(address) {
        return Some(RestrictedIpClass::Metadata);
    }
    match address {
        IpAddr::V4(address) if address.is_unspecified() => Some(RestrictedIpClass::Unspecified),
        IpAddr::V6(address) if address.is_unspecified() => Some(RestrictedIpClass::Unspecified),
        IpAddr::V4(address) if address.is_loopback() => Some(RestrictedIpClass::Loopback),
        IpAddr::V6(address) if address.is_loopback() => Some(RestrictedIpClass::Loopback),
        IpAddr::V4(address) if address.is_private() => Some(RestrictedIpClass::Private),
        IpAddr::V6(address) if address.is_unique_local() => Some(RestrictedIpClass::Private),
        IpAddr::V4(address) if address.is_link_local() => Some(RestrictedIpClass::LinkLocal),
        IpAddr::V6(address) if address.is_unicast_link_local() => {
            Some(RestrictedIpClass::LinkLocal)
        }
        IpAddr::V4(address) if address.is_multicast() => Some(RestrictedIpClass::Multicast),
        IpAddr::V6(address) if address.is_multicast() => Some(RestrictedIpClass::Multicast),
        _ => None,
    }
}

fn is_metadata(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => {
            address == Ipv4Addr::new(169, 254, 169, 254)
                || address == Ipv4Addr::new(169, 254, 170, 2)
                || address == Ipv4Addr::new(100, 100, 100, 200)
        }
        IpAddr::V6(address) => address == Ipv6Addr::new(0xfd00, 0x0ec2, 0, 0, 0, 0, 0, 0x0254),
    }
}

const fn canonical_ip(address: IpAddr) -> IpAddr {
    match address {
        IpAddr::V6(address) if address.is_unspecified() || address.is_loopback() => {
            IpAddr::V6(address)
        }
        IpAddr::V6(address) => match address.to_ipv4() {
            Some(address) => IpAddr::V4(address),
            None => IpAddr::V6(address),
        },
        IpAddr::V4(address) => IpAddr::V4(address),
    }
}

#[cfg(test)]
mod tests {
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
            ordinary_allow
                .authorize_resolution(decision, &["192.168.1.1".parse().expect("address")]),
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
            resolution
                .authorize_connect(original.target(), "203.0.113.10".parse().expect("address")),
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
}
