//! Pure Product policy for MPTunnel.
//!
//! This module owns normalized flow identity, deterministic new-flow routing,
//! and destination authorization. It deliberately contains no sockets, DNS
//! client, configuration-document, process lifecycle, or MPP path state.

mod acl;
mod admission;
mod dns;
mod flow;
mod gateway;
mod identity;
mod routing;
mod rule_set;

pub use acl::{
    AclEffect, AclError, AclRuleSpec, AclVerdict, AuthorizedDomainTarget, AuthorizedResolution,
    AuthorizedTarget, DestinationAcl, PreResolutionDecision, RestrictedIpClass,
};
pub use admission::{
    DEFAULT_MAX_PRODUCT_CONCURRENT_WORK, DEFAULT_MAX_PRODUCT_CONNECTS_PER_OUTBOUND,
    DEFAULT_MAX_PRODUCT_CONNECTS_PER_TARGET, DEFAULT_MAX_PRODUCT_DNS_WORK,
    DEFAULT_MAX_PRODUCT_LIVE_FLOWS, DEFAULT_MAX_PRODUCT_LIVE_FLOWS_PER_OUTBOUND,
    DEFAULT_MAX_PRODUCT_LIVE_FLOWS_PER_PRINCIPAL, DEFAULT_MAX_PRODUCT_LIVE_FLOWS_PER_TARGET,
    MAX_PRODUCT_ADMISSION_LIMIT, PendingProductFlow, ProductAdmission, ProductAdmissionConfig,
    ProductAdmissionConfigError, ProductAdmissionError, ProductAdmissionRejection,
    ProductAdmissionRejectionSnapshot, ProductAdmissionSnapshot, ProductConnectWork,
    ProductDnsWork, ProductFlowLease, ProductOutboundAdmissionSnapshot, ProductOutboundFlow,
    ProductPrincipalAdmissionSnapshot, ProductTargetAdmissionSnapshot,
};
pub use dns::{
    CompiledDnsPlan, CompiledDnsPolicy, CompiledDnsUpstream, DnsCompileError, DnsEgressSpec,
    DnsHostSpec, DnsIpStrategy, DnsOutboundCapabilitySpec, DnsPlanLimits, DnsPlanSpec,
    DnsPolicySpec, DnsRuleId, DnsRuleMatch, DnsRuleMatchKind, DnsRuleSpec, DnsSecurityPolicy,
    DnsSelection, DnsTransport, DnsUpstreamEndpoint, DnsUpstreamId, DnsUpstreamSpec,
    DnsUpstreamStrategy, FakeDnsSpec, MAX_DNS_ANSWERS, MAX_DNS_CACHE_ENTRIES_PER_PLAN,
    MAX_DNS_EXPECTED_CIDRS_PER_PLAN, MAX_DNS_HOST_ADDRESSES, MAX_DNS_HOSTS,
    MAX_DNS_INFLIGHT_PER_PLAN, MAX_DNS_PLANS, MAX_DNS_RULES, MAX_DNS_TOTAL_CACHE_ENTRIES,
    MAX_DNS_TOTAL_INFLIGHT, MAX_DNS_UPSTREAMS, MAX_DNS_UPSTREAMS_PER_PLAN, MAX_FAKE_DNS_ENTRIES,
};
pub use flow::{
    CredentialId, DomainName, FlowContext, FlowError, InboundId, Network, NetworkSet, PrincipalId,
    ProtocolTarget, SourceEndpoint, TargetHost, TargetPort,
};
pub use gateway::{
    GatewayBalancer, GatewayBalancerSpec, GatewayCompileError, GatewayEntropy,
    GatewayFreshnessStatus, GatewayHealthPolicy, GatewayHealthStatus, GatewayInstant, GatewayLoad,
    GatewayMemberCounters, GatewayMemberHandle, GatewayMemberMode, GatewayMemberSpec,
    GatewayMemberStatus, GatewayObservationSource, GatewayOutcome, GatewayProbePolicy,
    GatewaySelection, GatewaySelectionError, GatewaySelectionReason, GatewayStateError,
    GatewayStickinessKey, GatewayStickinessPolicy, GatewayStrategy, MAX_GATEWAY_MEMBERS,
    MAX_GATEWAY_STICKY_DESTINATIONS,
};
pub use identity::{
    CredentialAdmissionError, CredentialAuthority, CredentialCandidate, CredentialCatalog,
    CredentialCatalogError, CredentialRecord, MAX_CREDENTIALS, PrincipalPermit,
    SecurityPolicyError, SharedSecret,
};
pub use routing::{
    BalancerId, CompiledRouteTable, DnsPlanId, EgressAction, OutboundId, PortRange, RouteAction,
    RouteCompileError, RouteDecision, RouteExplanation, RouteInput, RouteMatchSpec, RouteMismatch,
    RouteRuleSpec, RouteRuleTrace, RouteStage, RuleId, TrafficIntent,
};
pub use rule_set::{
    CompiledRuleSetRegistry, MAX_ENTRIES_ACROSS_RULE_SET_REGISTRY, MAX_ENTRIES_PER_RULE_SET,
    MAX_RULE_SET_ENVELOPE_BYTES, MAX_RULE_SET_PAYLOAD_BYTES, MAX_RULE_SET_PUBLISHERS,
    MAX_RULE_SETS, RULE_SET_SCHEMA_VERSION, RULE_SET_SIGNATURE_CONTEXT, RuleSetError, RuleSetId,
    RuleSetPublisher, RuleSetPublisherCatalog, RuleSetPublisherId, VerifiedRuleSet,
};

/// One immutable Product-policy generation shared by all new-flow inbounds.
///
/// Runtime path state is intentionally absent. A caller first classifies and
/// authorizes a normalized flow here, then maps the selected Product ID to its
/// independently owned transport context.
#[derive(Debug)]
pub struct ProductPolicyGeneration {
    routes: CompiledRouteTable,
    destination_acl: DestinationAcl,
}

impl ProductPolicyGeneration {
    pub fn compile(
        generation: u64,
        routes: Vec<RouteRuleSpec>,
        destination_acl: Vec<AclRuleSpec>,
    ) -> Result<Self, ProductPolicyCompileError> {
        Ok(Self {
            routes: CompiledRouteTable::compile(generation, routes)
                .map_err(ProductPolicyCompileError::Routing)?,
            destination_acl: DestinationAcl::compile(generation, destination_acl)
                .map_err(ProductPolicyCompileError::DestinationAcl)?,
        })
    }

    pub fn safe_default_acl(
        generation: u64,
        routes: Vec<RouteRuleSpec>,
    ) -> Result<Self, ProductPolicyCompileError> {
        Ok(Self {
            routes: CompiledRouteTable::compile(generation, routes)
                .map_err(ProductPolicyCompileError::Routing)?,
            destination_acl: DestinationAcl::safe_default(generation),
        })
    }

    pub const fn generation(&self) -> u64 {
        self.routes.generation()
    }

    pub const fn routes(&self) -> &CompiledRouteTable {
        &self.routes
    }

    pub const fn destination_acl(&self) -> &DestinationAcl {
        &self.destination_acl
    }
}

#[derive(Debug)]
pub enum ProductPolicyCompileError {
    Routing(RouteCompileError),
    DestinationAcl(AclError),
}

impl std::fmt::Display for ProductPolicyCompileError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Routing(error) => write!(formatter, "invalid routing policy: {error}"),
            Self::DestinationAcl(error) => {
                write!(formatter, "invalid destination ACL policy: {error}")
            }
        }
    }
}

impl std::error::Error for ProductPolicyCompileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Routing(error) => Some(error),
            Self::DestinationAcl(error) => Some(error),
        }
    }
}
