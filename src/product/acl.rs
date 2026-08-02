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
    /// The operator-facing destination-ACL rule `name`, compiled to the same
    /// typed rule identity used in explanations and audit output.
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
#[path = "tests_acl.rs"]
mod tests;
