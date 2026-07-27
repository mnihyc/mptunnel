use crate::product::flow::{DomainName, FlowError, Network, NetworkSet, canonical_policy_id};
use crate::product::routing::{DnsPlanId, OutboundId};
use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::str::FromStr;
use std::time::Duration;

pub const MAX_DNS_UPSTREAMS: usize = 128;
pub const MAX_DNS_PLANS: usize = 128;
pub const MAX_DNS_RULES: usize = 4_096;
pub const MAX_DNS_HOSTS: usize = 4_096;
pub const MAX_DNS_HOST_ADDRESSES: usize = 64;
pub const MAX_DNS_UPSTREAMS_PER_PLAN: usize = 16;
pub const MAX_DNS_EXPECTED_CIDRS_PER_PLAN: usize = 256;
pub const MAX_DNS_CACHE_ENTRIES_PER_PLAN: usize = 65_536;
pub const MAX_DNS_INFLIGHT_PER_PLAN: usize = 1_024;
pub const MAX_DNS_ANSWERS: usize = 64;
pub const MAX_DNS_TOTAL_CACHE_ENTRIES: usize = 262_144;
pub const MAX_DNS_TOTAL_INFLIGHT: usize = 4_096;
pub const MAX_FAKE_DNS_ENTRIES: usize = 262_144;

const MAX_DOH_PATH_BYTES: usize = 256;
const MAX_DNS_EXPLANATION_BYTES: usize = 256;
const MAX_DNS_LOOKUP_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_DNS_TTL_CAP: Duration = Duration::from_secs(86_400);
const MAX_FAKE_DNS_ANSWER_TTL: Duration = Duration::from_secs(3_600);
const MAX_FAKE_DNS_RECOVERY_TTL: Duration = Duration::from_secs(86_400);

macro_rules! dns_id {
    ($name:ident) => {
        #[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(String);

        impl $name {
            pub fn parse(input: &str) -> Result<Self, FlowError> {
                canonical_policy_id(input).map(Self)
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_tuple(stringify!($name))
                    .field(&self.0)
                    .finish()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl FromStr for $name {
            type Err = FlowError;

            fn from_str(input: &str) -> Result<Self, Self::Err> {
                Self::parse(input)
            }
        }
    };
}

dns_id!(DnsUpstreamId);
dns_id!(DnsRuleId);

/// The wire transport used to reach a tagged DNS upstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DnsTransport {
    System,
    Udp,
    Tcp,
    UdpTcp,
    Tls,
    Https,
    Quic,
}

impl DnsTransport {
    pub const fn networks(self) -> NetworkSet {
        match self {
            Self::System => NetworkSet::NONE,
            Self::Udp => NetworkSet::UDP,
            Self::Tcp | Self::Tls | Self::Https => NetworkSet::TCP,
            Self::Quic => NetworkSet::UDP,
            Self::UdpTcp => NetworkSet::TCP_UDP,
        }
    }

    pub const fn is_encrypted(self) -> bool {
        matches!(self, Self::Tls | Self::Https | Self::Quic)
    }
}

/// A DNS endpoint. Every explicit network endpoint has a literal bootstrap IP;
/// `System` is a separate opt-in for non-VPN/simple-client use.
///
/// The tagged variants make it impossible to construct DoT or DoH without an
/// authenticated TLS identity. DoH intentionally uses one identity for both
/// TLS SNI and the HTTP authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DnsUpstreamEndpoint {
    System,
    Udp {
        bootstrap: SocketAddr,
    },
    Tcp {
        bootstrap: SocketAddr,
    },
    UdpTcp {
        bootstrap: SocketAddr,
    },
    Tls {
        bootstrap: SocketAddr,
        server_name: DomainName,
    },
    Https {
        bootstrap: SocketAddr,
        server_name: DomainName,
        path: String,
    },
    Quic {
        bootstrap: SocketAddr,
        server_name: DomainName,
    },
}

impl DnsUpstreamEndpoint {
    pub const fn transport(&self) -> DnsTransport {
        match self {
            Self::System => DnsTransport::System,
            Self::Udp { .. } => DnsTransport::Udp,
            Self::Tcp { .. } => DnsTransport::Tcp,
            Self::UdpTcp { .. } => DnsTransport::UdpTcp,
            Self::Tls { .. } => DnsTransport::Tls,
            Self::Https { .. } => DnsTransport::Https,
            Self::Quic { .. } => DnsTransport::Quic,
        }
    }

    pub const fn bootstrap(&self) -> Option<SocketAddr> {
        match self {
            Self::System => None,
            Self::Udp { bootstrap }
            | Self::Tcp { bootstrap }
            | Self::UdpTcp { bootstrap }
            | Self::Tls { bootstrap, .. }
            | Self::Https { bootstrap, .. }
            | Self::Quic { bootstrap, .. } => Some(*bootstrap),
        }
    }

    pub const fn server_name(&self) -> Option<&DomainName> {
        match self {
            Self::Tls { server_name, .. }
            | Self::Https { server_name, .. }
            | Self::Quic { server_name, .. } => Some(server_name),
            Self::System | Self::Udp { .. } | Self::Tcp { .. } | Self::UdpTcp { .. } => None,
        }
    }

    pub fn https_path(&self) -> Option<&str> {
        match self {
            Self::Https { path, .. } => Some(path),
            Self::System
            | Self::Udp { .. }
            | Self::Tcp { .. }
            | Self::UdpTcp { .. }
            | Self::Tls { .. }
            | Self::Quic { .. } => None,
        }
    }
}

/// DNS traffic either leaves directly or through one DNS-independent outbound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DnsEgressSpec {
    Direct,
    Outbound(OutboundId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsUpstreamSpec {
    pub id: DnsUpstreamId,
    pub endpoint: DnsUpstreamEndpoint,
    pub egress: DnsEgressSpec,
}

impl DnsUpstreamSpec {
    pub const fn direct(id: DnsUpstreamId, endpoint: DnsUpstreamEndpoint) -> Self {
        Self {
            id,
            endpoint,
            egress: DnsEgressSpec::Direct,
        }
    }
}

/// Product facts needed to prove that a DNS upstream cannot recurse through
/// the resolver it is helping to implement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsOutboundCapabilitySpec {
    pub outbound: OutboundId,
    pub networks: NetworkSet,
    pub dns_independent: bool,
}

impl DnsOutboundCapabilitySpec {
    pub const fn new(outbound: OutboundId, networks: NetworkSet, dns_independent: bool) -> Self {
        Self {
            outbound,
            networks,
            dns_independent,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DnsIpStrategy {
    Ipv4Only,
    Ipv6Only,
    Ipv4ThenIpv6,
    Ipv6ThenIpv4,
    Ipv4AndIpv6,
    Ipv6AndIpv4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DnsSecurityPolicy {
    AllowPlaintext,
    RequireEncrypted,
}

/// How one plan uses its ordered upstream inventory.
///
/// `Ordered` preserves strict primary-then-fallback behavior. `Race` starts
/// the primary immediately and hedges the next fallback after each delay; a
/// definitive transport failure advances immediately. One absolute plan
/// deadline still bounds the whole query.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum DnsUpstreamStrategy {
    #[default]
    Ordered,
    Race {
        fallback_delay: Duration,
    },
}

/// An immutable exact-name override owned by one DNS policy generation.
///
/// A/AAAA queries return the matching address family. Every other record type,
/// including an absent address family, returns authoritative local NODATA and
/// never leaks the private name to an upstream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsHostSpec {
    pub domain: DomainName,
    pub addresses: Vec<IpAddr>,
}

/// Bounded synthetic-address policy for local DNS capture.
///
/// FakeDNS is deliberately absent from ordinary dial-time resolution. A TUN
/// capture may publish a synthetic address and recover the original domain
/// once when the application opens a flow; the selected outbound then resolves
/// that domain normally. Pools are restricted to non-public ranges so enabling
/// FakeDNS cannot silently shadow an Internet destination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FakeDnsSpec {
    pub ipv4_pool: Option<Ipv4Net>,
    pub ipv6_pool: Option<Ipv6Net>,
    pub max_entries: usize,
    pub answer_ttl: Duration,
    pub recovery_ttl: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DnsPlanLimits {
    pub lookup_timeout: Duration,
    pub cache_capacity: usize,
    pub max_inflight: usize,
    pub max_answers: usize,
    pub positive_ttl_cap: Duration,
    pub negative_ttl_cap: Duration,
    /// How long a positive answer may be served only after a retriable
    /// upstream failure. Zero disables stale answers.
    pub stale_if_error: Duration,
    /// Maximum refresh-ahead window. The runtime refreshes at most ten percent
    /// of the answer TTL early; zero disables proactive refresh.
    pub prefetch_max: Duration,
}

impl Default for DnsPlanLimits {
    fn default() -> Self {
        Self {
            lookup_timeout: Duration::from_secs(5),
            cache_capacity: 4_096,
            max_inflight: 64,
            max_answers: MAX_DNS_ANSWERS,
            positive_ttl_cap: Duration::from_secs(300),
            negative_ttl_cap: Duration::from_secs(30),
            stale_if_error: Duration::from_secs(30),
            prefetch_max: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsPlanSpec {
    pub id: DnsPlanId,
    /// Ordered failover list. A negative DNS result is authoritative and does
    /// not fall through; transport and server failures do.
    pub upstreams: Vec<DnsUpstreamId>,
    pub ip_strategy: DnsIpStrategy,
    pub security: DnsSecurityPolicy,
    pub upstream_strategy: DnsUpstreamStrategy,
    /// If non-empty, address responses are accepted only when every returned
    /// A/AAAA answer belongs to one of these networks. A rejected candidate
    /// advances to the next upstream.
    pub expected_cidrs: Vec<IpNet>,
    pub limits: DnsPlanLimits,
}

impl DnsPlanSpec {
    pub fn new(id: DnsPlanId, upstreams: Vec<DnsUpstreamId>) -> Self {
        Self {
            id,
            upstreams,
            ip_strategy: DnsIpStrategy::Ipv4AndIpv6,
            security: DnsSecurityPolicy::AllowPlaintext,
            upstream_strategy: DnsUpstreamStrategy::Ordered,
            expected_cidrs: Vec::new(),
            limits: DnsPlanLimits::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DnsRuleMatchKind {
    Exact,
    Suffix,
    Default,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DnsRuleMatch {
    Exact(DomainName),
    Suffix(DomainName),
}

impl DnsRuleMatch {
    pub const fn kind(&self) -> DnsRuleMatchKind {
        match self {
            Self::Exact(_) => DnsRuleMatchKind::Exact,
            Self::Suffix(_) => DnsRuleMatchKind::Suffix,
        }
    }

    pub const fn domain(&self) -> &DomainName {
        match self {
            Self::Exact(domain) | Self::Suffix(domain) => domain,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsRuleSpec {
    pub id: DnsRuleId,
    pub matcher: DnsRuleMatch,
    pub plan: DnsPlanId,
    pub explanation: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsPolicySpec {
    pub upstreams: Vec<DnsUpstreamSpec>,
    pub outbound_capabilities: Vec<DnsOutboundCapabilitySpec>,
    pub plans: Vec<DnsPlanSpec>,
    pub rules: Vec<DnsRuleSpec>,
    pub hosts: Vec<DnsHostSpec>,
    pub fake_dns: Option<FakeDnsSpec>,
    pub default_plan: DnsPlanId,
}

#[derive(Debug, Clone)]
pub struct CompiledDnsUpstream {
    id: DnsUpstreamId,
    endpoint: DnsUpstreamEndpoint,
    egress: DnsEgressSpec,
}

impl CompiledDnsUpstream {
    pub const fn id(&self) -> &DnsUpstreamId {
        &self.id
    }

    pub const fn endpoint(&self) -> &DnsUpstreamEndpoint {
        &self.endpoint
    }

    pub const fn egress(&self) -> &DnsEgressSpec {
        &self.egress
    }
}

#[derive(Debug, Clone)]
pub struct CompiledDnsPlan {
    id: DnsPlanId,
    upstreams: Vec<DnsUpstreamId>,
    ip_strategy: DnsIpStrategy,
    security: DnsSecurityPolicy,
    upstream_strategy: DnsUpstreamStrategy,
    expected_cidrs: Vec<IpNet>,
    limits: DnsPlanLimits,
}

impl CompiledDnsPlan {
    pub const fn id(&self) -> &DnsPlanId {
        &self.id
    }

    pub fn upstreams(&self) -> &[DnsUpstreamId] {
        &self.upstreams
    }

    pub const fn ip_strategy(&self) -> DnsIpStrategy {
        self.ip_strategy
    }

    pub const fn security(&self) -> DnsSecurityPolicy {
        self.security
    }

    pub const fn upstream_strategy(&self) -> DnsUpstreamStrategy {
        self.upstream_strategy
    }

    pub fn expected_cidrs(&self) -> &[IpNet] {
        &self.expected_cidrs
    }

    pub const fn limits(&self) -> DnsPlanLimits {
        self.limits
    }
}

#[derive(Debug, Clone)]
struct CompiledDnsRule {
    id: DnsRuleId,
    matcher: DnsRuleMatch,
    plan: DnsPlanId,
    explanation: Option<Box<str>>,
}

/// Immutable split-DNS policy used by one runtime generation.
#[derive(Debug)]
pub struct CompiledDnsPolicy {
    generation: u64,
    upstreams: BTreeMap<DnsUpstreamId, CompiledDnsUpstream>,
    plans: BTreeMap<DnsPlanId, CompiledDnsPlan>,
    rules: Vec<CompiledDnsRule>,
    exact_rules: BTreeMap<DomainName, usize>,
    suffix_rules: Vec<usize>,
    hosts: BTreeMap<DomainName, std::sync::Arc<[IpAddr]>>,
    fake_dns: Option<FakeDnsSpec>,
    default_plan: DnsPlanId,
}

impl CompiledDnsPolicy {
    pub fn compile(generation: u64, spec: DnsPolicySpec) -> Result<Self, DnsCompileError> {
        validate_collection_bounds(&spec)?;
        validate_fake_dns(spec.fake_dns.as_ref(), &spec.upstreams)?;
        let capabilities = compile_capabilities(spec.outbound_capabilities)?;
        let upstreams = compile_upstreams(spec.upstreams, &capabilities)?;
        let plans = compile_plans(spec.plans, &upstreams)?;
        validate_generation_limits(&plans)?;
        if !plans.contains_key(&spec.default_plan) {
            return Err(DnsCompileError::UnknownDefaultPlan(spec.default_plan));
        }
        let (rules, exact_rules, suffix_rules) = compile_rules(spec.rules, &plans)?;
        let hosts = compile_hosts(spec.hosts)?;
        Ok(Self {
            generation,
            upstreams,
            plans,
            rules,
            exact_rules,
            suffix_rules,
            hosts,
            fake_dns: spec.fake_dns,
            default_plan: spec.default_plan,
        })
    }

    pub const fn generation(&self) -> u64 {
        self.generation
    }

    pub fn upstreams(&self) -> impl ExactSizeIterator<Item = &CompiledDnsUpstream> {
        self.upstreams.values()
    }

    pub fn upstream(&self, id: &DnsUpstreamId) -> Option<&CompiledDnsUpstream> {
        self.upstreams.get(id)
    }

    /// Literal DNS endpoints that must bypass a managed full-tunnel route.
    /// Upstreams carried through a named outbound are deliberately excluded:
    /// only that outbound's own native endpoint belongs in the host bypass
    /// inventory, while the DNS payload remains inside the selected outbound.
    /// System DNS has no explicit endpoint and is intentionally absent.
    pub fn bootstrap_endpoints(&self) -> impl Iterator<Item = SocketAddr> + '_ {
        self.upstreams
            .values()
            .filter(|upstream| matches!(upstream.egress, DnsEgressSpec::Direct))
            .filter_map(|upstream| upstream.endpoint.bootstrap())
    }

    /// True only when every configured plan is encrypted end to end. Platform
    /// full-VPN validation can use this without reinterpreting DNS internals.
    pub fn is_encrypted_only(&self) -> bool {
        self.plans
            .values()
            .all(|plan| plan.security == DnsSecurityPolicy::RequireEncrypted)
    }

    /// System resolution is deliberately explicit so managed VPN activation
    /// can reject it before changing host routes or resolver state.
    pub fn uses_system_resolution(&self) -> bool {
        self.upstreams
            .values()
            .any(|upstream| matches!(upstream.endpoint, DnsUpstreamEndpoint::System))
    }

    pub fn plans(&self) -> impl ExactSizeIterator<Item = &CompiledDnsPlan> {
        self.plans.values()
    }

    pub fn plan(&self, id: &DnsPlanId) -> Option<&CompiledDnsPlan> {
        self.plans.get(id)
    }

    pub const fn default_plan(&self) -> &DnsPlanId {
        &self.default_plan
    }

    pub fn hosts(&self) -> impl ExactSizeIterator<Item = (&DomainName, &std::sync::Arc<[IpAddr]>)> {
        self.hosts.iter()
    }

    pub fn host(&self, domain: &DomainName) -> Option<&std::sync::Arc<[IpAddr]>> {
        self.hosts.get(domain)
    }

    pub const fn fake_dns(&self) -> Option<&FakeDnsSpec> {
        self.fake_dns.as_ref()
    }

    /// Select exact first, then the longest matching suffix, then default.
    pub fn select<'a>(&'a self, domain: &DomainName) -> DnsSelection<'a> {
        if let Some(index) = self.exact_rules.get(domain) {
            return self.selection_for_rule(*index);
        }
        if let Some(index) = self.suffix_rules.iter().copied().find(|index| {
            suffix_matches(
                domain.as_str(),
                self.rules[*index].matcher.domain().as_str(),
            )
        }) {
            return self.selection_for_rule(index);
        }
        DnsSelection {
            generation: self.generation,
            plan: self
                .plans
                .get(&self.default_plan)
                .expect("compiled DNS default plan"),
            rule_id: None,
            match_kind: DnsRuleMatchKind::Default,
            matched_domain: None,
            explanation: None,
        }
    }

    fn selection_for_rule(&self, index: usize) -> DnsSelection<'_> {
        let rule = &self.rules[index];
        DnsSelection {
            generation: self.generation,
            plan: self.plans.get(&rule.plan).expect("compiled DNS rule plan"),
            rule_id: Some(&rule.id),
            match_kind: rule.matcher.kind(),
            matched_domain: Some(rule.matcher.domain()),
            explanation: rule.explanation.as_deref(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DnsSelection<'a> {
    generation: u64,
    plan: &'a CompiledDnsPlan,
    rule_id: Option<&'a DnsRuleId>,
    match_kind: DnsRuleMatchKind,
    matched_domain: Option<&'a DomainName>,
    explanation: Option<&'a str>,
}

impl<'a> DnsSelection<'a> {
    pub const fn generation(self) -> u64 {
        self.generation
    }

    pub const fn plan(self) -> &'a CompiledDnsPlan {
        self.plan
    }

    pub const fn rule_id(self) -> Option<&'a DnsRuleId> {
        self.rule_id
    }

    pub const fn match_kind(self) -> DnsRuleMatchKind {
        self.match_kind
    }

    pub const fn matched_domain(self) -> Option<&'a DomainName> {
        self.matched_domain
    }

    pub const fn explanation(self) -> Option<&'a str> {
        self.explanation
    }
}

fn validate_collection_bounds(spec: &DnsPolicySpec) -> Result<(), DnsCompileError> {
    if spec.upstreams.is_empty() {
        return Err(DnsCompileError::NoUpstreams);
    }
    if spec.upstreams.len() > MAX_DNS_UPSTREAMS {
        return Err(DnsCompileError::TooManyUpstreams {
            count: spec.upstreams.len(),
            maximum: MAX_DNS_UPSTREAMS,
        });
    }
    if spec.plans.is_empty() {
        return Err(DnsCompileError::NoPlans);
    }
    if spec.plans.len() > MAX_DNS_PLANS {
        return Err(DnsCompileError::TooManyPlans {
            count: spec.plans.len(),
            maximum: MAX_DNS_PLANS,
        });
    }
    if spec.rules.len() > MAX_DNS_RULES {
        return Err(DnsCompileError::TooManyRules {
            count: spec.rules.len(),
            maximum: MAX_DNS_RULES,
        });
    }
    if spec.hosts.len() > MAX_DNS_HOSTS {
        return Err(DnsCompileError::TooManyHosts {
            count: spec.hosts.len(),
            maximum: MAX_DNS_HOSTS,
        });
    }
    Ok(())
}

fn validate_fake_dns(
    fake_dns: Option<&FakeDnsSpec>,
    upstreams: &[DnsUpstreamSpec],
) -> Result<(), DnsCompileError> {
    let Some(fake_dns) = fake_dns else {
        return Ok(());
    };
    if fake_dns.ipv4_pool.is_none() && fake_dns.ipv6_pool.is_none() {
        return Err(DnsCompileError::FakeDnsPoolRequired);
    }
    if !(1..=MAX_FAKE_DNS_ENTRIES).contains(&fake_dns.max_entries) {
        return Err(DnsCompileError::InvalidFakeDnsCapacity {
            capacity: fake_dns.max_entries,
            maximum: MAX_FAKE_DNS_ENTRIES,
        });
    }
    if fake_dns.answer_ttl.is_zero()
        || fake_dns.answer_ttl > MAX_FAKE_DNS_ANSWER_TTL
        || fake_dns.recovery_ttl < fake_dns.answer_ttl
        || fake_dns.recovery_ttl > MAX_FAKE_DNS_RECOVERY_TTL
    {
        return Err(DnsCompileError::InvalidFakeDnsLifetime {
            answer_ttl: fake_dns.answer_ttl,
            recovery_ttl: fake_dns.recovery_ttl,
        });
    }

    if let Some(pool) = fake_dns.ipv4_pool {
        let reserved =
            Ipv4Net::new(Ipv4Addr::new(198, 18, 0, 0), 15).expect("static FakeDNS IPv4 range");
        if pool.addr() != pool.network() || !reserved.contains(&pool.network()) {
            return Err(DnsCompileError::InvalidFakeDnsIpv4Pool(pool));
        }
        let addresses = 1_u128 << u32::from(32 - pool.prefix_len());
        let usable = addresses.saturating_sub(2);
        if usable < fake_dns.max_entries as u128 {
            return Err(DnsCompileError::FakeDnsPoolTooSmall {
                pool: IpNet::V4(pool),
                capacity: usable,
                required: fake_dns.max_entries,
            });
        }
    }
    if let Some(pool) = fake_dns.ipv6_pool {
        let reserved =
            Ipv6Net::new(Ipv6Addr::from(0xfc00_u128 << 112), 7).expect("static FakeDNS IPv6 range");
        if pool.addr() != pool.network() || !reserved.contains(&pool.network()) {
            return Err(DnsCompileError::InvalidFakeDnsIpv6Pool(pool));
        }
        let host_bits = u32::from(128 - pool.prefix_len());
        let usable = 1_u128
            .checked_shl(host_bits)
            .unwrap_or(u128::MAX)
            .saturating_sub(1);
        if usable < fake_dns.max_entries as u128 {
            return Err(DnsCompileError::FakeDnsPoolTooSmall {
                pool: IpNet::V6(pool),
                capacity: usable,
                required: fake_dns.max_entries,
            });
        }
    }

    for upstream in upstreams {
        let Some(bootstrap) = upstream.endpoint.bootstrap() else {
            continue;
        };
        let overlaps = match bootstrap.ip() {
            IpAddr::V4(address) => fake_dns
                .ipv4_pool
                .is_some_and(|pool| pool.contains(&address)),
            IpAddr::V6(address) => fake_dns
                .ipv6_pool
                .is_some_and(|pool| pool.contains(&address)),
        };
        if overlaps {
            return Err(DnsCompileError::FakeDnsContainsBootstrap {
                pool: match bootstrap.ip() {
                    IpAddr::V4(_) => IpNet::V4(fake_dns.ipv4_pool.expect("matching IPv4 pool")),
                    IpAddr::V6(_) => IpNet::V6(fake_dns.ipv6_pool.expect("matching IPv6 pool")),
                },
                upstream: upstream.id.clone(),
                bootstrap,
            });
        }
    }
    Ok(())
}

fn compile_capabilities(
    capabilities: Vec<DnsOutboundCapabilitySpec>,
) -> Result<BTreeMap<OutboundId, DnsOutboundCapabilitySpec>, DnsCompileError> {
    let mut compiled = BTreeMap::new();
    for capability in capabilities {
        let duplicate = capability.outbound.clone();
        if compiled
            .insert(capability.outbound.clone(), capability)
            .is_some()
        {
            return Err(DnsCompileError::DuplicateOutboundCapability(duplicate));
        }
    }
    Ok(compiled)
}

fn compile_upstreams(
    upstreams: Vec<DnsUpstreamSpec>,
    capabilities: &BTreeMap<OutboundId, DnsOutboundCapabilitySpec>,
) -> Result<BTreeMap<DnsUpstreamId, CompiledDnsUpstream>, DnsCompileError> {
    let mut compiled = BTreeMap::new();
    for upstream in upstreams {
        validate_upstream(&upstream, capabilities)?;
        let duplicate = upstream.id.clone();
        let value = CompiledDnsUpstream {
            id: upstream.id.clone(),
            endpoint: upstream.endpoint,
            egress: upstream.egress,
        };
        if compiled.insert(upstream.id, value).is_some() {
            return Err(DnsCompileError::DuplicateUpstreamId(duplicate));
        }
    }
    Ok(compiled)
}

fn validate_upstream(
    upstream: &DnsUpstreamSpec,
    capabilities: &BTreeMap<OutboundId, DnsOutboundCapabilitySpec>,
) -> Result<(), DnsCompileError> {
    if let Some(bootstrap) = upstream.endpoint.bootstrap()
        && (bootstrap.port() == 0 || !is_usable_bootstrap(bootstrap.ip()))
    {
        return Err(DnsCompileError::InvalidBootstrap {
            upstream: upstream.id.clone(),
            bootstrap,
        });
    }
    if let DnsUpstreamEndpoint::Https { path, .. } = &upstream.endpoint
        && !is_valid_doh_path(path)
    {
        return Err(DnsCompileError::InvalidDohPath {
            upstream: upstream.id.clone(),
        });
    }
    if let DnsEgressSpec::Outbound(outbound) = &upstream.egress {
        if matches!(upstream.endpoint, DnsUpstreamEndpoint::System) {
            return Err(DnsCompileError::SystemUpstreamWithOutbound {
                upstream: upstream.id.clone(),
                outbound: outbound.clone(),
            });
        }
        let capability =
            capabilities
                .get(outbound)
                .ok_or_else(|| DnsCompileError::UnknownOutbound {
                    upstream: upstream.id.clone(),
                    outbound: outbound.clone(),
                })?;
        if !capability.dns_independent {
            return Err(DnsCompileError::RecursiveOutbound {
                upstream: upstream.id.clone(),
                outbound: outbound.clone(),
            });
        }
        let required = upstream.endpoint.transport().networks();
        for network in [Network::Tcp, Network::Udp] {
            if required.contains(network) && !capability.networks.contains(network) {
                return Err(DnsCompileError::UnsupportedOutboundNetwork {
                    upstream: upstream.id.clone(),
                    outbound: outbound.clone(),
                    network,
                });
            }
        }
    }
    Ok(())
}

fn compile_plans(
    plans: Vec<DnsPlanSpec>,
    upstreams: &BTreeMap<DnsUpstreamId, CompiledDnsUpstream>,
) -> Result<BTreeMap<DnsPlanId, CompiledDnsPlan>, DnsCompileError> {
    let mut compiled = BTreeMap::new();
    for plan in plans {
        validate_plan(&plan, upstreams)?;
        let duplicate = plan.id.clone();
        let value = CompiledDnsPlan {
            id: plan.id.clone(),
            upstreams: plan.upstreams,
            ip_strategy: plan.ip_strategy,
            security: plan.security,
            upstream_strategy: plan.upstream_strategy,
            expected_cidrs: plan.expected_cidrs,
            limits: plan.limits,
        };
        if compiled.insert(plan.id, value).is_some() {
            return Err(DnsCompileError::DuplicatePlanId(duplicate));
        }
    }
    Ok(compiled)
}

fn validate_plan(
    plan: &DnsPlanSpec,
    upstreams: &BTreeMap<DnsUpstreamId, CompiledDnsUpstream>,
) -> Result<(), DnsCompileError> {
    if plan.upstreams.is_empty() {
        return Err(DnsCompileError::EmptyPlan(plan.id.clone()));
    }
    if plan.upstreams.len() > MAX_DNS_UPSTREAMS_PER_PLAN {
        return Err(DnsCompileError::TooManyPlanUpstreams {
            plan: plan.id.clone(),
            count: plan.upstreams.len(),
            maximum: MAX_DNS_UPSTREAMS_PER_PLAN,
        });
    }
    validate_limits(&plan.id, plan.limits)?;
    if matches!(plan.upstream_strategy, DnsUpstreamStrategy::Race { .. })
        && plan.upstreams.len() < 2
    {
        return Err(DnsCompileError::RacingPlanRequiresFallback(plan.id.clone()));
    }
    if let DnsUpstreamStrategy::Race { fallback_delay } = plan.upstream_strategy
        && fallback_delay > plan.limits.lookup_timeout
    {
        return Err(DnsCompileError::InvalidRaceDelay {
            plan: plan.id.clone(),
            fallback_delay,
            lookup_timeout: plan.limits.lookup_timeout,
        });
    }
    if plan.expected_cidrs.len() > MAX_DNS_EXPECTED_CIDRS_PER_PLAN {
        return Err(DnsCompileError::TooManyExpectedCidrs {
            plan: plan.id.clone(),
            count: plan.expected_cidrs.len(),
            maximum: MAX_DNS_EXPECTED_CIDRS_PER_PLAN,
        });
    }
    let mut expected_cidrs = BTreeSet::new();
    for cidr in &plan.expected_cidrs {
        if !expected_cidrs.insert(*cidr) {
            return Err(DnsCompileError::DuplicateExpectedCidr {
                plan: plan.id.clone(),
                cidr: *cidr,
            });
        }
    }
    let mut seen = BTreeSet::new();
    for upstream_id in &plan.upstreams {
        if !seen.insert(upstream_id.clone()) {
            return Err(DnsCompileError::DuplicatePlanUpstream {
                plan: plan.id.clone(),
                upstream: upstream_id.clone(),
            });
        }
        let upstream =
            upstreams
                .get(upstream_id)
                .ok_or_else(|| DnsCompileError::UnknownUpstream {
                    plan: plan.id.clone(),
                    upstream: upstream_id.clone(),
                })?;
        if plan.security == DnsSecurityPolicy::RequireEncrypted
            && !upstream.endpoint.transport().is_encrypted()
        {
            return Err(DnsCompileError::PlaintextUpstreamInEncryptedPlan {
                plan: plan.id.clone(),
                upstream: upstream_id.clone(),
            });
        }
    }
    Ok(())
}

fn validate_limits(plan: &DnsPlanId, limits: DnsPlanLimits) -> Result<(), DnsCompileError> {
    let valid = !limits.lookup_timeout.is_zero()
        && limits.lookup_timeout <= MAX_DNS_LOOKUP_TIMEOUT
        && limits.cache_capacity <= MAX_DNS_CACHE_ENTRIES_PER_PLAN
        && (1..=MAX_DNS_INFLIGHT_PER_PLAN).contains(&limits.max_inflight)
        && (1..=MAX_DNS_ANSWERS).contains(&limits.max_answers)
        && !limits.positive_ttl_cap.is_zero()
        && limits.positive_ttl_cap <= MAX_DNS_TTL_CAP
        && !limits.negative_ttl_cap.is_zero()
        && limits.negative_ttl_cap <= MAX_DNS_TTL_CAP
        && limits.stale_if_error <= MAX_DNS_TTL_CAP
        && limits.prefetch_max <= MAX_DNS_TTL_CAP;
    if !valid {
        return Err(DnsCompileError::InvalidPlanLimits(plan.clone()));
    }
    Ok(())
}

fn compile_hosts(
    hosts: Vec<DnsHostSpec>,
) -> Result<BTreeMap<DomainName, std::sync::Arc<[IpAddr]>>, DnsCompileError> {
    let mut compiled = BTreeMap::new();
    for host in hosts {
        let DnsHostSpec { domain, addresses } = host;
        if addresses.is_empty() {
            return Err(DnsCompileError::EmptyHostAddresses(domain));
        }
        if addresses.len() > MAX_DNS_HOST_ADDRESSES {
            return Err(DnsCompileError::TooManyHostAddresses {
                domain,
                count: addresses.len(),
                maximum: MAX_DNS_HOST_ADDRESSES,
            });
        }
        let mut seen = BTreeSet::new();
        for address in &addresses {
            if !seen.insert(*address) {
                return Err(DnsCompileError::DuplicateHostAddress {
                    domain,
                    address: *address,
                });
            }
        }
        if compiled
            .insert(domain.clone(), std::sync::Arc::from(addresses))
            .is_some()
        {
            return Err(DnsCompileError::DuplicateHostDomain(domain));
        }
    }
    Ok(compiled)
}

fn validate_generation_limits(
    plans: &BTreeMap<DnsPlanId, CompiledDnsPlan>,
) -> Result<(), DnsCompileError> {
    let total_cache_entries = plans
        .values()
        .map(|plan| plan.limits.cache_capacity)
        .try_fold(0_usize, usize::checked_add)
        .ok_or(DnsCompileError::GenerationLimitsOverflow)?;
    let total_inflight = plans
        .values()
        .map(|plan| plan.limits.max_inflight)
        .try_fold(0_usize, usize::checked_add)
        .ok_or(DnsCompileError::GenerationLimitsOverflow)?;
    if total_cache_entries > MAX_DNS_TOTAL_CACHE_ENTRIES || total_inflight > MAX_DNS_TOTAL_INFLIGHT
    {
        return Err(DnsCompileError::GenerationLimitsExceeded {
            cache_entries: total_cache_entries,
            maximum_cache_entries: MAX_DNS_TOTAL_CACHE_ENTRIES,
            inflight: total_inflight,
            maximum_inflight: MAX_DNS_TOTAL_INFLIGHT,
        });
    }
    Ok(())
}

type CompiledRules = (
    Vec<CompiledDnsRule>,
    BTreeMap<DomainName, usize>,
    Vec<usize>,
);

fn compile_rules(
    rules: Vec<DnsRuleSpec>,
    plans: &BTreeMap<DnsPlanId, CompiledDnsPlan>,
) -> Result<CompiledRules, DnsCompileError> {
    let mut ids = BTreeSet::new();
    let mut matches = BTreeSet::new();
    let mut compiled = Vec::with_capacity(rules.len());
    let mut exact = BTreeMap::new();
    let mut suffix = Vec::new();
    for rule in rules {
        if !ids.insert(rule.id.clone()) {
            return Err(DnsCompileError::DuplicateRuleId(rule.id));
        }
        let match_key = (rule.matcher.kind(), rule.matcher.domain().clone());
        if !matches.insert(match_key.clone()) {
            return Err(DnsCompileError::DuplicateRuleMatch {
                kind: match_key.0,
                domain: match_key.1,
            });
        }
        if !plans.contains_key(&rule.plan) {
            return Err(DnsCompileError::UnknownRulePlan {
                rule: rule.id,
                plan: rule.plan,
            });
        }
        let explanation = validate_explanation(&rule.id, rule.explanation)?;
        let index = compiled.len();
        match rule.matcher {
            DnsRuleMatch::Exact(ref domain) => {
                exact.insert(domain.clone(), index);
            }
            DnsRuleMatch::Suffix(_) => suffix.push(index),
        }
        compiled.push(CompiledDnsRule {
            id: rule.id,
            matcher: rule.matcher,
            plan: rule.plan,
            explanation,
        });
    }
    suffix.sort_by(|left, right| {
        let left_rule = &compiled[*left];
        let right_rule = &compiled[*right];
        let left_domain = left_rule.matcher.domain().as_str();
        let right_domain = right_rule.matcher.domain().as_str();
        right_domain
            .split('.')
            .count()
            .cmp(&left_domain.split('.').count())
            .then_with(|| right_domain.len().cmp(&left_domain.len()))
            .then_with(|| left_domain.cmp(right_domain))
            .then_with(|| left_rule.id.cmp(&right_rule.id))
    });
    Ok((compiled, exact, suffix))
}

fn validate_explanation(
    rule: &DnsRuleId,
    explanation: Option<String>,
) -> Result<Option<Box<str>>, DnsCompileError> {
    let Some(explanation) = explanation else {
        return Ok(None);
    };
    if explanation.is_empty()
        || explanation.trim() != explanation
        || explanation.len() > MAX_DNS_EXPLANATION_BYTES
        || explanation.chars().any(char::is_control)
    {
        return Err(DnsCompileError::InvalidExplanation(rule.clone()));
    }
    Ok(Some(explanation.into_boxed_str()))
}

fn suffix_matches(domain: &str, suffix: &str) -> bool {
    domain == suffix
        || domain
            .strip_suffix(suffix)
            .is_some_and(|prefix| prefix.ends_with('.'))
}

fn is_usable_bootstrap(address: IpAddr) -> bool {
    !address.is_unspecified()
        && !address.is_multicast()
        && address != IpAddr::V4(Ipv4Addr::BROADCAST)
}

fn is_valid_doh_path(path: &str) -> bool {
    !path.is_empty()
        && path.len() <= MAX_DOH_PATH_BYTES
        && path.starts_with('/')
        && !path.starts_with("//")
        && path
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && !matches!(byte, b'?' | b'#' | b'\\'))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DnsCompileError {
    NoUpstreams,
    TooManyUpstreams {
        count: usize,
        maximum: usize,
    },
    NoPlans,
    TooManyPlans {
        count: usize,
        maximum: usize,
    },
    TooManyRules {
        count: usize,
        maximum: usize,
    },
    TooManyHosts {
        count: usize,
        maximum: usize,
    },
    DuplicateUpstreamId(DnsUpstreamId),
    DuplicateOutboundCapability(OutboundId),
    DuplicatePlanId(DnsPlanId),
    DuplicateRuleId(DnsRuleId),
    DuplicateRuleMatch {
        kind: DnsRuleMatchKind,
        domain: DomainName,
    },
    InvalidBootstrap {
        upstream: DnsUpstreamId,
        bootstrap: SocketAddr,
    },
    InvalidDohPath {
        upstream: DnsUpstreamId,
    },
    SystemUpstreamWithOutbound {
        upstream: DnsUpstreamId,
        outbound: OutboundId,
    },
    UnknownOutbound {
        upstream: DnsUpstreamId,
        outbound: OutboundId,
    },
    RecursiveOutbound {
        upstream: DnsUpstreamId,
        outbound: OutboundId,
    },
    UnsupportedOutboundNetwork {
        upstream: DnsUpstreamId,
        outbound: OutboundId,
        network: Network,
    },
    EmptyPlan(DnsPlanId),
    TooManyPlanUpstreams {
        plan: DnsPlanId,
        count: usize,
        maximum: usize,
    },
    DuplicatePlanUpstream {
        plan: DnsPlanId,
        upstream: DnsUpstreamId,
    },
    UnknownUpstream {
        plan: DnsPlanId,
        upstream: DnsUpstreamId,
    },
    PlaintextUpstreamInEncryptedPlan {
        plan: DnsPlanId,
        upstream: DnsUpstreamId,
    },
    RacingPlanRequiresFallback(DnsPlanId),
    InvalidRaceDelay {
        plan: DnsPlanId,
        fallback_delay: Duration,
        lookup_timeout: Duration,
    },
    TooManyExpectedCidrs {
        plan: DnsPlanId,
        count: usize,
        maximum: usize,
    },
    DuplicateExpectedCidr {
        plan: DnsPlanId,
        cidr: IpNet,
    },
    InvalidPlanLimits(DnsPlanId),
    GenerationLimitsOverflow,
    GenerationLimitsExceeded {
        cache_entries: usize,
        maximum_cache_entries: usize,
        inflight: usize,
        maximum_inflight: usize,
    },
    UnknownDefaultPlan(DnsPlanId),
    UnknownRulePlan {
        rule: DnsRuleId,
        plan: DnsPlanId,
    },
    InvalidExplanation(DnsRuleId),
    DuplicateHostDomain(DomainName),
    EmptyHostAddresses(DomainName),
    TooManyHostAddresses {
        domain: DomainName,
        count: usize,
        maximum: usize,
    },
    DuplicateHostAddress {
        domain: DomainName,
        address: IpAddr,
    },
    FakeDnsPoolRequired,
    InvalidFakeDnsCapacity {
        capacity: usize,
        maximum: usize,
    },
    InvalidFakeDnsLifetime {
        answer_ttl: Duration,
        recovery_ttl: Duration,
    },
    InvalidFakeDnsIpv4Pool(Ipv4Net),
    InvalidFakeDnsIpv6Pool(Ipv6Net),
    FakeDnsPoolTooSmall {
        pool: IpNet,
        capacity: u128,
        required: usize,
    },
    FakeDnsContainsBootstrap {
        pool: IpNet,
        upstream: DnsUpstreamId,
        bootstrap: SocketAddr,
    },
}

impl fmt::Display for DnsCompileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoUpstreams => {
                formatter.write_str("DNS policy must define at least one upstream")
            }
            Self::TooManyUpstreams { count, maximum } => {
                write!(
                    formatter,
                    "DNS policy has {count} upstreams; maximum is {maximum}"
                )
            }
            Self::NoPlans => formatter.write_str("DNS policy must define at least one plan"),
            Self::TooManyPlans { count, maximum } => {
                write!(
                    formatter,
                    "DNS policy has {count} plans; maximum is {maximum}"
                )
            }
            Self::TooManyRules { count, maximum } => {
                write!(
                    formatter,
                    "DNS policy has {count} rules; maximum is {maximum}"
                )
            }
            Self::TooManyHosts { count, maximum } => {
                write!(
                    formatter,
                    "DNS policy has {count} host overrides; maximum is {maximum}"
                )
            }
            Self::DuplicateUpstreamId(id) => write!(formatter, "duplicate DNS upstream ID {id}"),
            Self::DuplicateOutboundCapability(id) => {
                write!(formatter, "duplicate DNS outbound capability for {id}")
            }
            Self::DuplicatePlanId(id) => write!(formatter, "duplicate DNS plan ID {id}"),
            Self::DuplicateRuleId(id) => write!(formatter, "duplicate DNS rule ID {id}"),
            Self::DuplicateRuleMatch { kind, domain } => {
                write!(formatter, "duplicate DNS {kind:?} rule match for {domain}")
            }
            Self::InvalidBootstrap {
                upstream,
                bootstrap,
            } => write!(
                formatter,
                "DNS upstream {upstream} has unusable literal bootstrap address {bootstrap}"
            ),
            Self::InvalidDohPath { upstream } => {
                write!(
                    formatter,
                    "DoH upstream {upstream} has an invalid absolute path"
                )
            }
            Self::SystemUpstreamWithOutbound { upstream, outbound } => write!(
                formatter,
                "system DNS upstream {upstream} cannot use named outbound {outbound}"
            ),
            Self::UnknownOutbound { upstream, outbound } => write!(
                formatter,
                "DNS upstream {upstream} references unknown outbound {outbound}"
            ),
            Self::RecursiveOutbound { upstream, outbound } => write!(
                formatter,
                "DNS upstream {upstream} uses DNS-dependent outbound {outbound}"
            ),
            Self::UnsupportedOutboundNetwork {
                upstream,
                outbound,
                network,
            } => write!(
                formatter,
                "DNS upstream {upstream} requires {network}, which outbound {outbound} does not support"
            ),
            Self::EmptyPlan(plan) => write!(formatter, "DNS plan {plan} has no upstreams"),
            Self::TooManyPlanUpstreams {
                plan,
                count,
                maximum,
            } => write!(
                formatter,
                "DNS plan {plan} has {count} upstreams; maximum is {maximum}"
            ),
            Self::DuplicatePlanUpstream { plan, upstream } => {
                write!(formatter, "DNS plan {plan} repeats upstream {upstream}")
            }
            Self::UnknownUpstream { plan, upstream } => {
                write!(
                    formatter,
                    "DNS plan {plan} references unknown upstream {upstream}"
                )
            }
            Self::PlaintextUpstreamInEncryptedPlan { plan, upstream } => write!(
                formatter,
                "encrypted DNS plan {plan} contains plaintext upstream {upstream}"
            ),
            Self::RacingPlanRequiresFallback(plan) => {
                write!(
                    formatter,
                    "racing DNS plan {plan} requires at least two upstreams"
                )
            }
            Self::InvalidRaceDelay {
                plan,
                fallback_delay,
                lookup_timeout,
            } => write!(
                formatter,
                "DNS plan {plan} fallback race delay {fallback_delay:?} exceeds its lookup timeout {lookup_timeout:?}"
            ),
            Self::TooManyExpectedCidrs {
                plan,
                count,
                maximum,
            } => write!(
                formatter,
                "DNS plan {plan} has {count} expected CIDRs; maximum is {maximum}"
            ),
            Self::DuplicateExpectedCidr { plan, cidr } => {
                write!(formatter, "DNS plan {plan} repeats expected CIDR {cidr}")
            }
            Self::InvalidPlanLimits(plan) => {
                write!(
                    formatter,
                    "DNS plan {plan} has invalid or unbounded runtime limits"
                )
            }
            Self::GenerationLimitsOverflow => {
                formatter.write_str("DNS generation aggregate limits overflow")
            }
            Self::GenerationLimitsExceeded {
                cache_entries,
                maximum_cache_entries,
                inflight,
                maximum_inflight,
            } => write!(
                formatter,
                "DNS generation requests {cache_entries} cache entries and {inflight} in-flight queries; maxima are {maximum_cache_entries} and {maximum_inflight}"
            ),
            Self::UnknownDefaultPlan(plan) => {
                write!(formatter, "DNS default plan {plan} does not exist")
            }
            Self::UnknownRulePlan { rule, plan } => {
                write!(formatter, "DNS rule {rule} references unknown plan {plan}")
            }
            Self::InvalidExplanation(rule) => {
                write!(formatter, "DNS rule {rule} has an invalid explanation")
            }
            Self::DuplicateHostDomain(domain) => {
                write!(formatter, "duplicate DNS host override for {domain}")
            }
            Self::EmptyHostAddresses(domain) => {
                write!(formatter, "DNS host override {domain} has no addresses")
            }
            Self::TooManyHostAddresses {
                domain,
                count,
                maximum,
            } => write!(
                formatter,
                "DNS host override {domain} has {count} addresses; maximum is {maximum}"
            ),
            Self::DuplicateHostAddress { domain, address } => {
                write!(
                    formatter,
                    "DNS host override {domain} repeats address {address}"
                )
            }
            Self::FakeDnsPoolRequired => {
                formatter.write_str("FakeDNS requires at least one IPv4 or IPv6 pool")
            }
            Self::InvalidFakeDnsCapacity { capacity, maximum } => write!(
                formatter,
                "FakeDNS capacity {capacity} must be between 1 and {maximum}"
            ),
            Self::InvalidFakeDnsLifetime {
                answer_ttl,
                recovery_ttl,
            } => write!(
                formatter,
                "FakeDNS answer TTL {answer_ttl:?} and recovery TTL {recovery_ttl:?} are invalid"
            ),
            Self::InvalidFakeDnsIpv4Pool(pool) => write!(
                formatter,
                "FakeDNS IPv4 pool {pool} must be a canonical subnet of 198.18.0.0/15"
            ),
            Self::InvalidFakeDnsIpv6Pool(pool) => write!(
                formatter,
                "FakeDNS IPv6 pool {pool} must be a canonical subnet of fc00::/7"
            ),
            Self::FakeDnsPoolTooSmall {
                pool,
                capacity,
                required,
            } => write!(
                formatter,
                "FakeDNS pool {pool} has {capacity} usable addresses but capacity requires {required}"
            ),
            Self::FakeDnsContainsBootstrap {
                pool,
                upstream,
                bootstrap,
            } => write!(
                formatter,
                "FakeDNS pool {pool} contains bootstrap {bootstrap} for upstream {upstream}"
            ),
        }
    }
}

impl Error for DnsCompileError {}

#[cfg(test)]
mod tests {
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
}
