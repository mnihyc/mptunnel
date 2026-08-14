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
pub const MAX_DNS_OVERRIDE_RECORDS: usize = 4_096;
pub const MAX_DNS_SYNTHETIC_CAPTURES: usize = 128;
pub const MAX_DNS_OVERRIDE_ADDRESSES: usize = 64;
pub const MAX_DNS_UPSTREAMS_PER_PLAN: usize = 16;
pub const MAX_DNS_EXPECTED_CIDRS_PER_PLAN: usize = 256;
pub const MAX_DNS_CACHE_ENTRIES_PER_PLAN: usize = 65_536;
pub const MAX_DNS_INFLIGHT_PER_PLAN: usize = 1_024;
pub const MAX_DNS_ANSWERS: usize = 64;
pub const MAX_DNS_TOTAL_CACHE_ENTRIES: usize = 262_144;
pub const MAX_DNS_TOTAL_INFLIGHT: usize = 4_096;
pub const MAX_DNS_SYNTHETIC_ENTRIES: usize = 262_144;

const MAX_DOH_PATH_BYTES: usize = 256;
const MAX_DNS_EXPLANATION_BYTES: usize = 256;
const MAX_DNS_LOOKUP_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_DNS_TTL_CAP: Duration = Duration::from_secs(86_400);
const MAX_DNS_SYNTHETIC_ANSWER_TTL: Duration = Duration::from_secs(3_600);
const MAX_DNS_SYNTHETIC_RECOVERY_TTL: Duration = Duration::from_secs(86_400);

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
dns_id!(DnsOverrideRecordId);
dns_id!(DnsSyntheticCaptureId);

/// The wire protocol used to reach a named DNS server.
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

/// A DNS endpoint. Every explicit network endpoint has a literal server IP;
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
    /// The operator-facing `[[dns.servers]].name`, compiled to a typed
    /// reference and never serialized as an MPP protocol identifier.
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

/// Product facts needed to prove that a DNS server cannot recurse through
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
pub struct DnsOverrideRecordSpec {
    pub id: DnsOverrideRecordId,
    pub domain: DomainName,
    pub addresses: Vec<IpAddr>,
}

/// Bounded synthetic-address policy for local DNS capture.
///
/// Synthetic capture is deliberately absent from ordinary dial-time resolution. A TUN
/// capture may publish a synthetic address and recover the original domain
/// once when the application opens a flow; the selected outbound then resolves
/// that domain normally. Pools are restricted to non-public ranges so enabling
/// the override cannot silently shadow an Internet destination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsSyntheticCaptureSpec {
    pub id: DnsSyntheticCaptureId,
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
    /// The operator-facing `[[dns.policies]].name`, compiled to the type used by
    /// routing rules and DNS policy references.
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
    /// Named exact-name records explicitly attached to this policy.
    pub override_records: Vec<DnsOverrideRecordId>,
    /// One optional synthetic capture explicitly attached to this policy.
    pub synthetic_capture: Option<DnsSyntheticCaptureId>,
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
            override_records: Vec::new(),
            synthetic_capture: None,
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

impl DnsRuleMatchKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::Suffix => "suffix",
            Self::Default => "default",
        }
    }
}

impl fmt::Display for DnsRuleMatchKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
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
    /// The operator-facing `[[dns.rules]].name`; this is a stable diagnostic
    /// label, not a rule-order selector or wire field.
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
    pub override_records: Vec<DnsOverrideRecordSpec>,
    pub synthetic_captures: Vec<DnsSyntheticCaptureSpec>,
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
    override_records: Vec<DnsOverrideRecordId>,
    synthetic_capture: Option<DnsSyntheticCaptureId>,
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

    pub fn override_records(&self) -> &[DnsOverrideRecordId] {
        &self.override_records
    }

    pub const fn synthetic_capture(&self) -> Option<&DnsSyntheticCaptureId> {
        self.synthetic_capture.as_ref()
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
    override_records: BTreeMap<DnsOverrideRecordId, CompiledDnsOverrideRecord>,
    synthetic_captures: BTreeMap<DnsSyntheticCaptureId, DnsSyntheticCaptureSpec>,
    dns_active_plans: BTreeSet<DnsPlanId>,
    default_plan: DnsPlanId,
}

/// Validated selector-reachable DNS policy roots for one generation.
///
/// Construction always includes the default policy and every split-DNS rule
/// target; callers add route-selected policy IDs. Catalog entries outside this
/// set remain parsed and validated but must not be instantiated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsActivation {
    generation: u64,
    plans: BTreeSet<DnsPlanId>,
}

impl DnsActivation {
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    pub fn plans(&self) -> impl ExactSizeIterator<Item = &DnsPlanId> {
        self.plans.iter()
    }

    pub fn contains(&self, plan: &DnsPlanId) -> bool {
        self.plans.contains(plan)
    }
}

#[derive(Debug, Clone)]
pub struct CompiledDnsOverrideRecord {
    id: DnsOverrideRecordId,
    domain: DomainName,
    addresses: std::sync::Arc<[IpAddr]>,
}

impl CompiledDnsOverrideRecord {
    pub const fn id(&self) -> &DnsOverrideRecordId {
        &self.id
    }

    pub const fn domain(&self) -> &DomainName {
        &self.domain
    }

    pub const fn addresses(&self) -> &std::sync::Arc<[IpAddr]> {
        &self.addresses
    }
}

impl CompiledDnsPolicy {
    pub fn compile(generation: u64, spec: DnsPolicySpec) -> Result<Self, DnsCompileError> {
        validate_collection_bounds(&spec)?;
        let override_records = compile_override_records(spec.override_records)?;
        let synthetic_captures = compile_synthetic_captures(spec.synthetic_captures)?;
        let capabilities = compile_capabilities(spec.outbound_capabilities)?;
        let upstreams = compile_upstreams(spec.upstreams, &capabilities)?;
        let plans = compile_plans(
            spec.plans,
            &upstreams,
            &override_records,
            &synthetic_captures,
        )?;
        if !plans.contains_key(&spec.default_plan) {
            return Err(DnsCompileError::UnknownDefaultPlan(spec.default_plan));
        }
        let (rules, exact_rules, suffix_rules) = compile_rules(spec.rules, &plans)?;
        let mut dns_active_plans = BTreeSet::from([spec.default_plan.clone()]);
        dns_active_plans.extend(rules.iter().map(|rule| rule.plan.clone()));
        let compiled = Self {
            generation,
            upstreams,
            plans,
            rules,
            exact_rules,
            suffix_rules,
            override_records,
            synthetic_captures,
            dns_active_plans,
            default_plan: spec.default_plan,
        };
        compiled.activate(std::iter::empty::<&DnsPlanId>())?;
        Ok(compiled)
    }

    pub const fn generation(&self) -> u64 {
        self.generation
    }

    pub fn upstreams(&self) -> impl ExactSizeIterator<Item = &CompiledDnsUpstream> {
        self.upstreams.values()
    }

    /// DNS-policy roots selected by the default and split-DNS rules. Routing
    /// may add explicit policy roots before runtime activation.
    pub fn dns_active_plans(&self) -> impl ExactSizeIterator<Item = &DnsPlanId> {
        self.dns_active_plans.iter()
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
        self.bootstrap_endpoints_for_plans(self.dns_active_plans.iter())
    }

    pub fn bootstrap_endpoints_for_plans<'a>(
        &'a self,
        plans: impl IntoIterator<Item = &'a DnsPlanId> + 'a,
    ) -> impl Iterator<Item = SocketAddr> + 'a {
        self.active_upstreams(plans)
            .filter(|upstream| matches!(upstream.egress, DnsEgressSpec::Direct))
            .filter_map(|upstream| upstream.endpoint.bootstrap())
    }

    /// True only when every configured plan is encrypted end to end. Platform
    /// full-VPN validation can use this without reinterpreting DNS internals.
    pub fn is_encrypted_only(&self) -> bool {
        self.dns_active_plans
            .iter()
            .filter_map(|id| self.plans.get(id))
            .all(|plan| plan.security == DnsSecurityPolicy::RequireEncrypted)
    }

    /// System resolution is deliberately explicit so managed VPN activation
    /// can reject it before changing host routes or resolver state.
    pub fn uses_system_resolution(&self) -> bool {
        self.active_upstreams(self.dns_active_plans.iter())
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

    pub fn override_records(&self) -> impl ExactSizeIterator<Item = &CompiledDnsOverrideRecord> {
        self.override_records.values()
    }

    pub fn override_record(&self, id: &DnsOverrideRecordId) -> Option<&CompiledDnsOverrideRecord> {
        self.override_records.get(id)
    }

    pub fn override_record_for_plan(
        &self,
        plan: &DnsPlanId,
        domain: &DomainName,
    ) -> Option<&CompiledDnsOverrideRecord> {
        self.plans
            .get(plan)?
            .override_records
            .iter()
            .find_map(|id| {
                self.override_records
                    .get(id)
                    .filter(|record| record.domain == *domain)
            })
    }

    pub fn synthetic_captures(&self) -> impl ExactSizeIterator<Item = &DnsSyntheticCaptureSpec> {
        self.synthetic_captures.values()
    }

    pub fn synthetic_capture(
        &self,
        id: &DnsSyntheticCaptureId,
    ) -> Option<&DnsSyntheticCaptureSpec> {
        self.synthetic_captures.get(id)
    }

    pub fn synthetic_capture_for_plan(&self, plan: &DnsPlanId) -> Option<&DnsSyntheticCaptureSpec> {
        let id = self.plans.get(plan)?.synthetic_capture.as_ref()?;
        self.synthetic_captures.get(id)
    }

    /// Build the only activation set accepted by the normal runtime and
    /// platform preflight paths. Intrinsic DNS roots cannot be omitted.
    pub fn activate<'a>(
        &self,
        extra_plans: impl IntoIterator<Item = &'a DnsPlanId>,
    ) -> Result<DnsActivation, DnsCompileError> {
        let mut plans = self.dns_active_plans.clone();
        plans.extend(extra_plans.into_iter().cloned());
        self.validate_active_plans(plans.iter())?;
        Ok(DnsActivation {
            generation: self.generation,
            plans,
        })
    }

    pub fn bootstrap_endpoints_for_activation<'a>(
        &'a self,
        activation: &'a DnsActivation,
    ) -> impl Iterator<Item = SocketAddr> + 'a {
        debug_assert_eq!(activation.generation, self.generation);
        self.bootstrap_endpoints_for_plans(activation.plans())
    }

    pub fn upstreams_for_activation<'a>(
        &'a self,
        activation: &'a DnsActivation,
    ) -> impl Iterator<Item = &'a CompiledDnsUpstream> + 'a {
        debug_assert_eq!(activation.generation, self.generation);
        self.active_upstreams(activation.plans())
    }

    pub fn override_records_for_activation<'a>(
        &'a self,
        activation: &'a DnsActivation,
    ) -> impl Iterator<Item = &'a CompiledDnsOverrideRecord> + 'a {
        debug_assert_eq!(activation.generation, self.generation);
        let ids = activation
            .plans()
            .flat_map(|plan| self.plans[plan].override_records.iter().cloned())
            .collect::<BTreeSet<_>>();
        ids.into_iter()
            .filter_map(|id| self.override_records.get(&id))
    }

    pub fn synthetic_captures_for_activation<'a>(
        &'a self,
        activation: &'a DnsActivation,
    ) -> impl Iterator<Item = &'a DnsSyntheticCaptureSpec> + 'a {
        debug_assert_eq!(activation.generation, self.generation);
        let ids = activation
            .plans()
            .filter_map(|plan| self.plans[plan].synthetic_capture.clone())
            .collect::<BTreeSet<_>>();
        ids.into_iter()
            .filter_map(|id| self.synthetic_captures.get(&id))
    }

    pub fn uses_system_resolution_for_activation(&self, activation: &DnsActivation) -> bool {
        debug_assert_eq!(activation.generation, self.generation);
        self.active_upstreams(activation.plans())
            .any(|upstream| matches!(upstream.endpoint, DnsUpstreamEndpoint::System))
    }

    pub fn is_encrypted_only_for_activation(&self, activation: &DnsActivation) -> bool {
        debug_assert_eq!(activation.generation, self.generation);
        activation
            .plans()
            .filter_map(|id| self.plans.get(id))
            .all(|plan| plan.security == DnsSecurityPolicy::RequireEncrypted)
    }

    /// Validate the complete set of policies that one runtime generation will
    /// activate. Catalog definitions outside this closure remain inert.
    pub fn validate_active_plans<'a>(
        &self,
        plans: impl IntoIterator<Item = &'a DnsPlanId>,
    ) -> Result<(), DnsCompileError> {
        let plans = plans.into_iter().cloned().collect::<BTreeSet<_>>();
        for plan in &plans {
            if !self.plans.contains_key(plan) {
                return Err(DnsCompileError::UnknownActivePlan(plan.clone()));
            }
        }
        let total_cache_entries = plans
            .iter()
            .map(|id| self.plans[id].limits.cache_capacity)
            .try_fold(0_usize, usize::checked_add)
            .ok_or(DnsCompileError::GenerationLimitsOverflow)?;
        let total_inflight = plans
            .iter()
            .map(|id| self.plans[id].limits.max_inflight)
            .try_fold(0_usize, usize::checked_add)
            .ok_or(DnsCompileError::GenerationLimitsOverflow)?;
        if total_cache_entries > MAX_DNS_TOTAL_CACHE_ENTRIES
            || total_inflight > MAX_DNS_TOTAL_INFLIGHT
        {
            return Err(DnsCompileError::GenerationLimitsExceeded {
                cache_entries: total_cache_entries,
                maximum_cache_entries: MAX_DNS_TOTAL_CACHE_ENTRIES,
                inflight: total_inflight,
                maximum_inflight: MAX_DNS_TOTAL_INFLIGHT,
            });
        }
        let captures = plans
            .iter()
            .filter_map(|plan| self.plans[plan].synthetic_capture.as_ref())
            .collect::<BTreeSet<_>>();
        for (index, left_id) in captures.iter().enumerate() {
            let left = &self.synthetic_captures[*left_id];
            for right_id in captures.iter().skip(index + 1) {
                let right = &self.synthetic_captures[*right_id];
                if capture_specs_overlap(left, right) {
                    return Err(DnsCompileError::OverlappingSyntheticCapturePools {
                        left: (*left_id).clone(),
                        right: (*right_id).clone(),
                    });
                }
            }
        }
        let upstream_ids = plans
            .iter()
            .flat_map(|plan| self.plans[plan].upstreams.iter().cloned())
            .collect::<BTreeSet<_>>();
        let record_ids = plans
            .iter()
            .flat_map(|plan| self.plans[plan].override_records.iter().cloned())
            .collect::<BTreeSet<_>>();
        for capture_id in captures {
            let capture = &self.synthetic_captures[capture_id];
            for upstream_id in &upstream_ids {
                if let Some(bootstrap) = self.upstreams[upstream_id].endpoint.bootstrap()
                    && synthetic_capture_contains(capture, bootstrap.ip())
                {
                    return Err(DnsCompileError::SyntheticCaptureContainsBootstrap {
                        capture: capture_id.clone(),
                        upstream: upstream_id.clone(),
                        bootstrap,
                    });
                }
            }
            for record_id in &record_ids {
                let record = &self.override_records[record_id];
                if let Some(address) = record
                    .addresses
                    .iter()
                    .copied()
                    .find(|address| synthetic_capture_contains(capture, *address))
                {
                    return Err(DnsCompileError::SyntheticCaptureContainsOverrideAddress {
                        capture: capture_id.clone(),
                        record: record_id.clone(),
                        address,
                    });
                }
            }
        }
        Ok(())
    }

    fn active_upstreams<'a>(
        &'a self,
        plans: impl IntoIterator<Item = &'a DnsPlanId>,
    ) -> impl Iterator<Item = &'a CompiledDnsUpstream> + 'a {
        let ids = plans
            .into_iter()
            .filter_map(|id| self.plans.get(id))
            .flat_map(|plan| plan.upstreams.iter().cloned())
            .collect::<BTreeSet<_>>();
        ids.into_iter().filter_map(|id| self.upstreams.get(&id))
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
                .expect("compiled default DNS policy"),
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
            plan: self
                .plans
                .get(&rule.plan)
                .expect("compiled DNS rule policy"),
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
    if spec.override_records.len() > MAX_DNS_OVERRIDE_RECORDS {
        return Err(DnsCompileError::TooManyOverrideRecords {
            count: spec.override_records.len(),
            maximum: MAX_DNS_OVERRIDE_RECORDS,
        });
    }
    if spec.synthetic_captures.len() > MAX_DNS_SYNTHETIC_CAPTURES {
        return Err(DnsCompileError::TooManySyntheticCaptures {
            count: spec.synthetic_captures.len(),
            maximum: MAX_DNS_SYNTHETIC_CAPTURES,
        });
    }
    Ok(())
}

fn validate_synthetic_capture(capture: &DnsSyntheticCaptureSpec) -> Result<(), DnsCompileError> {
    if capture.ipv4_pool.is_none() && capture.ipv6_pool.is_none() {
        return Err(DnsCompileError::SyntheticCapturePoolRequired);
    }
    if !(1..=MAX_DNS_SYNTHETIC_ENTRIES).contains(&capture.max_entries) {
        return Err(DnsCompileError::InvalidSyntheticCaptureCapacity {
            capacity: capture.max_entries,
            maximum: MAX_DNS_SYNTHETIC_ENTRIES,
        });
    }
    if capture.answer_ttl.is_zero()
        || capture.answer_ttl > MAX_DNS_SYNTHETIC_ANSWER_TTL
        || capture.recovery_ttl < capture.answer_ttl
        || capture.recovery_ttl > MAX_DNS_SYNTHETIC_RECOVERY_TTL
    {
        return Err(DnsCompileError::InvalidSyntheticCaptureLifetime {
            answer_ttl: capture.answer_ttl,
            recovery_ttl: capture.recovery_ttl,
        });
    }

    if let Some(pool) = capture.ipv4_pool {
        let reserved = Ipv4Net::new(Ipv4Addr::new(198, 18, 0, 0), 15)
            .expect("static DNS synthetic-capture IPv4 range");
        if pool.addr() != pool.network() || !reserved.contains(&pool.network()) {
            return Err(DnsCompileError::InvalidSyntheticCaptureIpv4Pool(pool));
        }
        let addresses = 1_u128 << u32::from(32 - pool.prefix_len());
        let usable = addresses.saturating_sub(2);
        if usable < capture.max_entries as u128 {
            return Err(DnsCompileError::SyntheticCapturePoolTooSmall {
                pool: IpNet::V4(pool),
                capacity: usable,
                required: capture.max_entries,
            });
        }
    }
    if let Some(pool) = capture.ipv6_pool {
        let reserved = Ipv6Net::new(Ipv6Addr::from(0xfc00_u128 << 112), 7)
            .expect("static DNS synthetic-capture IPv6 range");
        if pool.addr() != pool.network() || !reserved.contains(&pool.network()) {
            return Err(DnsCompileError::InvalidSyntheticCaptureIpv6Pool(pool));
        }
        let host_bits = u32::from(128 - pool.prefix_len());
        let usable = 1_u128
            .checked_shl(host_bits)
            .unwrap_or(u128::MAX)
            .saturating_sub(1);
        if usable < capture.max_entries as u128 {
            return Err(DnsCompileError::SyntheticCapturePoolTooSmall {
                pool: IpNet::V6(pool),
                capacity: usable,
                required: capture.max_entries,
            });
        }
    }

    Ok(())
}

fn compile_synthetic_captures(
    captures: Vec<DnsSyntheticCaptureSpec>,
) -> Result<BTreeMap<DnsSyntheticCaptureId, DnsSyntheticCaptureSpec>, DnsCompileError> {
    let mut compiled = BTreeMap::new();
    for capture in captures {
        validate_synthetic_capture(&capture)?;
        let duplicate = capture.id.clone();
        if compiled.insert(capture.id.clone(), capture).is_some() {
            return Err(DnsCompileError::DuplicateSyntheticCaptureId(duplicate));
        }
    }
    Ok(compiled)
}

fn capture_specs_overlap(left: &DnsSyntheticCaptureSpec, right: &DnsSyntheticCaptureSpec) -> bool {
    left.ipv4_pool
        .zip(right.ipv4_pool)
        .is_some_and(|(left, right)| {
            left.contains(&right.network()) || right.contains(&left.network())
        })
        || left
            .ipv6_pool
            .zip(right.ipv6_pool)
            .is_some_and(|(left, right)| {
                left.contains(&right.network()) || right.contains(&left.network())
            })
}

fn synthetic_capture_contains(capture: &DnsSyntheticCaptureSpec, address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => capture
            .ipv4_pool
            .is_some_and(|pool| pool.contains(&address)),
        IpAddr::V6(address) => capture
            .ipv6_pool
            .is_some_and(|pool| pool.contains(&address)),
    }
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
    override_records: &BTreeMap<DnsOverrideRecordId, CompiledDnsOverrideRecord>,
    synthetic_captures: &BTreeMap<DnsSyntheticCaptureId, DnsSyntheticCaptureSpec>,
) -> Result<BTreeMap<DnsPlanId, CompiledDnsPlan>, DnsCompileError> {
    let mut compiled = BTreeMap::new();
    for plan in plans {
        validate_plan(&plan, upstreams, override_records, synthetic_captures)?;
        let duplicate = plan.id.clone();
        let value = CompiledDnsPlan {
            id: plan.id.clone(),
            upstreams: plan.upstreams,
            ip_strategy: plan.ip_strategy,
            security: plan.security,
            upstream_strategy: plan.upstream_strategy,
            expected_cidrs: plan.expected_cidrs,
            override_records: plan.override_records,
            synthetic_capture: plan.synthetic_capture,
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
    override_records: &BTreeMap<DnsOverrideRecordId, CompiledDnsOverrideRecord>,
    synthetic_captures: &BTreeMap<DnsSyntheticCaptureId, DnsSyntheticCaptureSpec>,
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
    let mut record_ids = BTreeSet::new();
    let mut domains = BTreeMap::new();
    for record_id in &plan.override_records {
        if !record_ids.insert(record_id.clone()) {
            return Err(DnsCompileError::DuplicatePlanOverrideRecord {
                plan: plan.id.clone(),
                record: record_id.clone(),
            });
        }
        let record = override_records.get(record_id).ok_or_else(|| {
            DnsCompileError::UnknownPlanOverrideRecord {
                plan: plan.id.clone(),
                record: record_id.clone(),
            }
        })?;
        if let Some(previous) = domains.insert(record.domain.clone(), record_id.clone()) {
            return Err(DnsCompileError::DuplicatePlanOverrideDomain {
                plan: plan.id.clone(),
                domain: record.domain.clone(),
                first: previous,
                second: record_id.clone(),
            });
        }
        let eligible_addresses = record
            .addresses
            .iter()
            .filter(|address| dns_strategy_allows_address(plan.ip_strategy, **address))
            .count();
        if eligible_addresses > plan.limits.max_answers {
            return Err(DnsCompileError::PlanOverrideTooManyAddresses {
                plan: plan.id.clone(),
                record: record_id.clone(),
                count: eligible_addresses,
                maximum: plan.limits.max_answers,
            });
        }
        if !plan.expected_cidrs.is_empty()
            && let Some(address) = record.addresses.iter().copied().find(|address| {
                dns_strategy_allows_address(plan.ip_strategy, *address)
                    && !plan
                        .expected_cidrs
                        .iter()
                        .any(|cidr| cidr.contains(address))
            })
        {
            return Err(DnsCompileError::PlanOverrideOutsideExpectedCidrs {
                plan: plan.id.clone(),
                record: record_id.clone(),
                address,
            });
        }
    }
    if let Some(capture) = &plan.synthetic_capture
        && !synthetic_captures.contains_key(capture)
    {
        return Err(DnsCompileError::UnknownPlanSyntheticCapture {
            plan: plan.id.clone(),
            capture: capture.clone(),
        });
    }
    Ok(())
}

fn dns_strategy_allows_address(strategy: DnsIpStrategy, address: IpAddr) -> bool {
    match address {
        IpAddr::V4(_) => !matches!(strategy, DnsIpStrategy::Ipv6Only),
        IpAddr::V6(_) => !matches!(strategy, DnsIpStrategy::Ipv4Only),
    }
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

fn compile_override_records(
    records: Vec<DnsOverrideRecordSpec>,
) -> Result<BTreeMap<DnsOverrideRecordId, CompiledDnsOverrideRecord>, DnsCompileError> {
    let mut compiled = BTreeMap::new();
    for record in records {
        let DnsOverrideRecordSpec {
            id,
            domain,
            addresses,
        } = record;
        if addresses.is_empty() {
            return Err(DnsCompileError::EmptyOverrideRecordAddresses(domain));
        }
        if addresses.len() > MAX_DNS_OVERRIDE_ADDRESSES {
            return Err(DnsCompileError::TooManyOverrideRecordAddresses {
                domain,
                count: addresses.len(),
                maximum: MAX_DNS_OVERRIDE_ADDRESSES,
            });
        }
        let mut seen = BTreeSet::new();
        for address in &addresses {
            if !seen.insert(*address) {
                return Err(DnsCompileError::DuplicateOverrideRecordAddress {
                    domain,
                    address: *address,
                });
            }
        }
        let duplicate = id.clone();
        if compiled
            .insert(
                id.clone(),
                CompiledDnsOverrideRecord {
                    id,
                    domain,
                    addresses: std::sync::Arc::from(addresses),
                },
            )
            .is_some()
        {
            return Err(DnsCompileError::DuplicateOverrideRecordId(duplicate));
        }
    }
    Ok(compiled)
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
    TooManyOverrideRecords {
        count: usize,
        maximum: usize,
    },
    TooManySyntheticCaptures {
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
    DuplicateOverrideRecordId(DnsOverrideRecordId),
    EmptyOverrideRecordAddresses(DomainName),
    TooManyOverrideRecordAddresses {
        domain: DomainName,
        count: usize,
        maximum: usize,
    },
    DuplicateOverrideRecordAddress {
        domain: DomainName,
        address: IpAddr,
    },
    DuplicateSyntheticCaptureId(DnsSyntheticCaptureId),
    UnknownPlanOverrideRecord {
        plan: DnsPlanId,
        record: DnsOverrideRecordId,
    },
    DuplicatePlanOverrideRecord {
        plan: DnsPlanId,
        record: DnsOverrideRecordId,
    },
    DuplicatePlanOverrideDomain {
        plan: DnsPlanId,
        domain: DomainName,
        first: DnsOverrideRecordId,
        second: DnsOverrideRecordId,
    },
    PlanOverrideTooManyAddresses {
        plan: DnsPlanId,
        record: DnsOverrideRecordId,
        count: usize,
        maximum: usize,
    },
    PlanOverrideOutsideExpectedCidrs {
        plan: DnsPlanId,
        record: DnsOverrideRecordId,
        address: IpAddr,
    },
    UnknownPlanSyntheticCapture {
        plan: DnsPlanId,
        capture: DnsSyntheticCaptureId,
    },
    UnknownActivePlan(DnsPlanId),
    OverlappingSyntheticCapturePools {
        left: DnsSyntheticCaptureId,
        right: DnsSyntheticCaptureId,
    },
    SyntheticCaptureContainsBootstrap {
        capture: DnsSyntheticCaptureId,
        upstream: DnsUpstreamId,
        bootstrap: SocketAddr,
    },
    SyntheticCaptureContainsOverrideAddress {
        capture: DnsSyntheticCaptureId,
        record: DnsOverrideRecordId,
        address: IpAddr,
    },
    SyntheticCapturePoolRequired,
    InvalidSyntheticCaptureCapacity {
        capacity: usize,
        maximum: usize,
    },
    InvalidSyntheticCaptureLifetime {
        answer_ttl: Duration,
        recovery_ttl: Duration,
    },
    InvalidSyntheticCaptureIpv4Pool(Ipv4Net),
    InvalidSyntheticCaptureIpv6Pool(Ipv6Net),
    SyntheticCapturePoolTooSmall {
        pool: IpNet,
        capacity: u128,
        required: usize,
    },
}

impl fmt::Display for DnsCompileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoUpstreams => {
                formatter.write_str("DNS configuration must define at least one server")
            }
            Self::TooManyUpstreams { count, maximum } => {
                write!(
                    formatter,
                    "DNS configuration has {count} servers; maximum is {maximum}"
                )
            }
            Self::NoPlans => {
                formatter.write_str("DNS configuration must define at least one policy")
            }
            Self::TooManyPlans { count, maximum } => {
                write!(
                    formatter,
                    "DNS configuration has {count} policies; maximum is {maximum}"
                )
            }
            Self::TooManyRules { count, maximum } => {
                write!(
                    formatter,
                    "DNS configuration has {count} rules; maximum is {maximum}"
                )
            }
            Self::TooManyOverrideRecords { count, maximum } => {
                write!(
                    formatter,
                    "DNS configuration has {count} records; maximum is {maximum}"
                )
            }
            Self::TooManySyntheticCaptures { count, maximum } => write!(
                formatter,
                "DNS configuration has {count} synthetic captures; maximum is {maximum}"
            ),
            Self::DuplicateUpstreamId(id) => write!(formatter, "duplicate DNS server name {id}"),
            Self::DuplicateOutboundCapability(id) => {
                write!(formatter, "duplicate DNS outbound capability for {id}")
            }
            Self::DuplicatePlanId(id) => write!(formatter, "duplicate DNS policy name {id}"),
            Self::DuplicateRuleId(id) => write!(formatter, "duplicate DNS rule ID {id}"),
            Self::DuplicateRuleMatch { kind, domain } => {
                write!(formatter, "duplicate DNS {kind:?} rule match for {domain}")
            }
            Self::InvalidBootstrap {
                upstream,
                bootstrap,
            } => write!(
                formatter,
                "DNS server {upstream} has unusable address {bootstrap}"
            ),
            Self::InvalidDohPath { upstream } => {
                write!(
                    formatter,
                    "DoH server {upstream} has an invalid absolute path"
                )
            }
            Self::SystemUpstreamWithOutbound { upstream, outbound } => write!(
                formatter,
                "system DNS server {upstream} cannot use outbound {outbound}"
            ),
            Self::UnknownOutbound { upstream, outbound } => write!(
                formatter,
                "DNS server {upstream} references unknown outbound {outbound}"
            ),
            Self::RecursiveOutbound { upstream, outbound } => write!(
                formatter,
                "DNS server {upstream} uses DNS-dependent outbound {outbound}"
            ),
            Self::UnsupportedOutboundNetwork {
                upstream,
                outbound,
                network,
            } => write!(
                formatter,
                "DNS server {upstream} requires {network}, which outbound {outbound} does not support"
            ),
            Self::EmptyPlan(plan) => write!(formatter, "DNS policy {plan} has no servers"),
            Self::TooManyPlanUpstreams {
                plan,
                count,
                maximum,
            } => write!(
                formatter,
                "DNS policy {plan} has {count} servers; maximum is {maximum}"
            ),
            Self::DuplicatePlanUpstream { plan, upstream } => {
                write!(formatter, "DNS policy {plan} repeats server {upstream}")
            }
            Self::UnknownUpstream { plan, upstream } => {
                write!(
                    formatter,
                    "DNS policy {plan} references unknown server {upstream}"
                )
            }
            Self::PlaintextUpstreamInEncryptedPlan { plan, upstream } => write!(
                formatter,
                "encrypted DNS policy {plan} contains plaintext server {upstream}"
            ),
            Self::RacingPlanRequiresFallback(plan) => {
                write!(
                    formatter,
                    "racing DNS policy {plan} requires at least two servers"
                )
            }
            Self::InvalidRaceDelay {
                plan,
                fallback_delay,
                lookup_timeout,
            } => write!(
                formatter,
                "DNS policy {plan} fallback delay {fallback_delay:?} exceeds its query timeout {lookup_timeout:?}"
            ),
            Self::TooManyExpectedCidrs {
                plan,
                count,
                maximum,
            } => write!(
                formatter,
                "DNS policy {plan} has {count} answer CIDRs; maximum is {maximum}"
            ),
            Self::DuplicateExpectedCidr { plan, cidr } => {
                write!(formatter, "DNS policy {plan} repeats answer CIDR {cidr}")
            }
            Self::InvalidPlanLimits(plan) => {
                write!(
                    formatter,
                    "DNS policy {plan} has invalid or unbounded runtime limits"
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
                write!(formatter, "default DNS policy {plan} does not exist")
            }
            Self::UnknownRulePlan { rule, plan } => {
                write!(
                    formatter,
                    "DNS rule {rule} references unknown policy {plan}"
                )
            }
            Self::InvalidExplanation(rule) => {
                write!(formatter, "DNS rule {rule} has an invalid explanation")
            }
            Self::DuplicateOverrideRecordId(id) => {
                write!(formatter, "duplicate DNS override-record name {id}")
            }
            Self::EmptyOverrideRecordAddresses(domain) => {
                write!(formatter, "DNS override record {domain} has no addresses")
            }
            Self::TooManyOverrideRecordAddresses {
                domain,
                count,
                maximum,
            } => write!(
                formatter,
                "DNS override record {domain} has {count} addresses; maximum is {maximum}"
            ),
            Self::DuplicateOverrideRecordAddress { domain, address } => {
                write!(
                    formatter,
                    "DNS override record {domain} repeats address {address}"
                )
            }
            Self::DuplicateSyntheticCaptureId(id) => {
                write!(formatter, "duplicate DNS synthetic-capture name {id}")
            }
            Self::UnknownPlanOverrideRecord { plan, record } => write!(
                formatter,
                "DNS policy {plan} references unknown override record {record}"
            ),
            Self::DuplicatePlanOverrideRecord { plan, record } => write!(
                formatter,
                "DNS policy {plan} repeats override record {record}"
            ),
            Self::DuplicatePlanOverrideDomain {
                plan,
                domain,
                first,
                second,
            } => write!(
                formatter,
                "DNS policy {plan} attaches override records {first} and {second} for the same domain {domain}"
            ),
            Self::PlanOverrideTooManyAddresses {
                plan,
                record,
                count,
                maximum,
            } => write!(
                formatter,
                "DNS policy {plan} override record {record} has {count} addresses; policy maximum is {maximum}"
            ),
            Self::PlanOverrideOutsideExpectedCidrs {
                plan,
                record,
                address,
            } => write!(
                formatter,
                "DNS policy {plan} override record {record} address {address} is outside its answer CIDRs"
            ),
            Self::UnknownPlanSyntheticCapture { plan, capture } => write!(
                formatter,
                "DNS policy {plan} references unknown synthetic capture {capture}"
            ),
            Self::UnknownActivePlan(plan) => {
                write!(formatter, "active DNS policy {plan} does not exist")
            }
            Self::OverlappingSyntheticCapturePools { left, right } => write!(
                formatter,
                "active DNS synthetic captures {left} and {right} have overlapping pools"
            ),
            Self::SyntheticCaptureContainsBootstrap {
                capture,
                upstream,
                bootstrap,
            } => write!(
                formatter,
                "DNS synthetic capture {capture} contains address {bootstrap} for server {upstream}"
            ),
            Self::SyntheticCaptureContainsOverrideAddress {
                capture,
                record,
                address,
            } => write!(
                formatter,
                "DNS synthetic capture {capture} contains address {address} from override record {record}"
            ),
            Self::SyntheticCapturePoolRequired => {
                formatter.write_str("DNS synthetic capture requires at least one IPv4 or IPv6 pool")
            }
            Self::InvalidSyntheticCaptureCapacity { capacity, maximum } => write!(
                formatter,
                "DNS synthetic-capture capacity {capacity} must be between 1 and {maximum}"
            ),
            Self::InvalidSyntheticCaptureLifetime {
                answer_ttl,
                recovery_ttl,
            } => write!(
                formatter,
                "DNS synthetic-capture answer TTL {answer_ttl:?} and recovery TTL {recovery_ttl:?} are invalid"
            ),
            Self::InvalidSyntheticCaptureIpv4Pool(pool) => write!(
                formatter,
                "DNS synthetic-capture IPv4 pool {pool} must be a canonical subnet of 198.18.0.0/15"
            ),
            Self::InvalidSyntheticCaptureIpv6Pool(pool) => write!(
                formatter,
                "DNS synthetic-capture IPv6 pool {pool} must be a canonical subnet of fc00::/7"
            ),
            Self::SyntheticCapturePoolTooSmall {
                pool,
                capacity,
                required,
            } => write!(
                formatter,
                "DNS synthetic-capture pool {pool} has {capacity} usable addresses but capacity requires {required}"
            ),
        }
    }
}

impl Error for DnsCompileError {}

#[cfg(test)]
#[path = "tests_dns.rs"]
mod tests;
