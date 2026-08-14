use crate::product::flow::{FlowContext, ProtocolTarget};
use crate::product::routing::{RouteAction, RouteDecision, RuleId};
use std::error::Error;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::Arc;

pub(crate) const MAX_RESOLUTION_ADDRESSES: usize = 64;

/// The immutable identity of the routing rule that authorized a flow.
///
/// A permit deliberately owns the complete terminal action and normalized
/// flow. Consequently an address authorized by one rule, egress, principal,
/// inbound, or policy generation cannot be replayed through another.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutePermit {
    generation: u64,
    rule_id: RuleId,
    action: RouteAction,
    pub(crate) flow: Arc<FlowContext>,
}

impl RoutePermit {
    pub(crate) fn from_decision(decision: RouteDecision<'_>, flow: Arc<FlowContext>) -> Self {
        Self {
            generation: decision.generation(),
            rule_id: decision.rule_id().clone(),
            action: decision.action().clone(),
            flow,
        }
    }

    pub const fn generation(&self) -> u64 {
        self.generation
    }

    pub const fn rule_id(&self) -> &RuleId {
        &self.rule_id
    }

    pub const fn action(&self) -> &RouteAction {
        &self.action
    }

    pub fn flow(&self) -> &FlowContext {
        self.flow.as_ref()
    }
}

/// Pre-resolution routing result for one immutable normalized flow.
#[derive(Debug, Clone)]
pub struct PreResolutionDecision {
    permit: RoutePermit,
    requires_post_resolution: bool,
}

impl PreResolutionDecision {
    pub(crate) const fn new(permit: RoutePermit, requires_post_resolution: bool) -> Self {
        Self {
            permit,
            requires_post_resolution,
        }
    }

    pub(crate) fn require_post_resolution(&mut self) {
        self.requires_post_resolution = true;
    }

    pub const fn policy_generation(&self) -> u64 {
        self.permit.generation()
    }

    pub const fn permit(&self) -> &RoutePermit {
        &self.permit
    }

    pub fn flow(&self) -> &FlowContext {
        self.permit.flow()
    }

    pub const fn requires_post_resolution(&self) -> bool {
        self.requires_post_resolution
    }
}

/// Proof that one normalized domain may cross a domain-capable outbound.
#[derive(Debug, Clone)]
pub struct AuthorizedDomainTarget {
    permit: RoutePermit,
}

impl AuthorizedDomainTarget {
    pub(crate) const fn new(permit: RoutePermit) -> Self {
        Self { permit }
    }

    pub const fn policy_generation(&self) -> u64 {
        self.permit.generation()
    }

    pub const fn permit(&self) -> &RoutePermit {
        &self.permit
    }

    pub fn flow(&self) -> &FlowContext {
        self.permit.flow()
    }
}

/// Exact normalized addresses authorized from one immutable DNS answer set.
#[derive(Debug)]
pub struct AuthorizedResolution {
    targets: Vec<AuthorizedTarget>,
}

impl AuthorizedResolution {
    pub(crate) fn new(targets: Vec<AuthorizedTarget>) -> Self {
        Self { targets }
    }

    pub fn targets(&self) -> &[AuthorizedTarget] {
        &self.targets
    }

    pub fn into_targets(self) -> Vec<AuthorizedTarget> {
        self.targets
    }

    /// Bind a connector to the original target and one address from the exact
    /// answer set recorded by the permit.
    pub fn authorize_connect(
        &self,
        target: &ProtocolTarget,
        address: IpAddr,
    ) -> Result<AuthorizedTarget, RouteAuthorizationError> {
        let address = canonical_ip(address);
        let Some(first) = self.targets.first() else {
            return Err(RouteAuthorizationError::EmptyResolution);
        };
        if target != first.flow().target() {
            return Err(RouteAuthorizationError::TargetChanged);
        }
        self.targets
            .iter()
            .find(|candidate| candidate.address == address)
            .cloned()
            .ok_or(RouteAuthorizationError::DnsRebinding { address })
    }
}

/// Immutable, unforgeable Product proof passed to an outbound connector.
#[derive(Debug, Clone)]
pub struct AuthorizedTarget {
    permit: RoutePermit,
    resolution: Arc<[IpAddr]>,
    address: IpAddr,
}

impl AuthorizedTarget {
    pub(crate) fn new(permit: RoutePermit, resolution: Arc<[IpAddr]>, address: IpAddr) -> Self {
        Self {
            permit,
            resolution,
            address,
        }
    }

    pub const fn policy_generation(&self) -> u64 {
        self.permit.generation()
    }

    pub const fn permit(&self) -> &RoutePermit {
        &self.permit
    }

    pub fn flow(&self) -> &FlowContext {
        self.permit.flow()
    }

    pub fn resolution(&self) -> &[IpAddr] {
        self.resolution.as_ref()
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

impl RestrictedIpClass {
    /// Classify the canonical destination safety class used by Product
    /// authorization. Control-plane explanations call this same function so
    /// they cannot drift from forwarding enforcement.
    pub(crate) fn classify(address: IpAddr) -> Option<Self> {
        restricted_ip_class(address)
    }
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
pub enum RouteAuthorizationError {
    RestrictedAddress {
        address: IpAddr,
        class: RestrictedIpClass,
        rule_id: RuleId,
    },
    Rejected {
        rule_id: RuleId,
    },
    Dropped {
        rule_id: RuleId,
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
    NoUsableAddress,
}

impl fmt::Display for RouteAuthorizationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RestrictedAddress {
                address,
                class,
                rule_id,
            } => write!(
                formatter,
                "route {rule_id} does not authorize restricted {class} address {address}"
            ),
            Self::Rejected { rule_id } => write!(formatter, "route {rule_id} rejected destination"),
            Self::Dropped { rule_id } => write!(formatter, "route {rule_id} dropped destination"),
            Self::EmptyResolution => formatter.write_str("DNS resolution returned no addresses"),
            Self::TooManyResolutionAddresses { count, maximum } => write!(
                formatter,
                "DNS resolution returned {count} addresses; maximum is {maximum}"
            ),
            Self::GenerationMismatch { evaluated, current } => write!(
                formatter,
                "route decision generation {evaluated} does not match policy generation {current}"
            ),
            Self::TargetChanged => {
                formatter.write_str("destination changed after the pre-resolution decision")
            }
            Self::DnsRebinding { address } => write!(
                formatter,
                "connect address {address} was not in the authorized DNS result"
            ),
            Self::ExpectedLiteralIp => {
                formatter.write_str("literal authorization requires an IP target")
            }
            Self::ExpectedDomain => {
                formatter.write_str("domain delegation authorization requires a domain target")
            }
            Self::PostResolutionRequired => {
                formatter.write_str("routing requires post-resolution authorization")
            }
            Self::NoUsableAddress => {
                formatter.write_str("routing produced no eligible destination address")
            }
        }
    }
}

impl Error for RouteAuthorizationError {}

pub(crate) fn canonical_resolution(
    flow: &FlowContext,
    addresses: &[IpAddr],
) -> Result<Arc<[IpAddr]>, RouteAuthorizationError> {
    if addresses.is_empty() {
        return Err(RouteAuthorizationError::EmptyResolution);
    }
    if addresses.len() > MAX_RESOLUTION_ADDRESSES {
        return Err(RouteAuthorizationError::TooManyResolutionAddresses {
            count: addresses.len(),
            maximum: MAX_RESOLUTION_ADDRESSES,
        });
    }
    let literal = flow.target().ip();
    let mut canonical = Vec::with_capacity(addresses.len());
    for address in addresses.iter().copied().map(canonical_ip) {
        if literal.is_some_and(|literal| literal != address) {
            return Err(RouteAuthorizationError::TargetChanged);
        }
        if !canonical.contains(&address) {
            canonical.push(address);
        }
    }
    Ok(Arc::from(canonical))
}

pub(crate) fn restricted_ip_class(address: IpAddr) -> Option<RestrictedIpClass> {
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

pub(crate) const fn canonical_ip(address: IpAddr) -> IpAddr {
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
