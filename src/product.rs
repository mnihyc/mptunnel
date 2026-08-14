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
mod tun_l3;

pub use acl::{
    AuthorizedDomainTarget, AuthorizedResolution, AuthorizedTarget, PreResolutionDecision,
    RestrictedIpClass, RouteAuthorizationError, RoutePermit,
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
    CompiledDnsOverrideRecord, CompiledDnsPlan, CompiledDnsPolicy, CompiledDnsUpstream,
    DnsActivation, DnsCompileError, DnsEgressSpec, DnsIpStrategy, DnsOutboundCapabilitySpec,
    DnsOverrideRecordId, DnsOverrideRecordSpec, DnsPlanLimits, DnsPlanSpec, DnsPolicySpec,
    DnsRuleId, DnsRuleMatch, DnsRuleMatchKind, DnsRuleSpec, DnsSecurityPolicy, DnsSelection,
    DnsSyntheticCaptureId, DnsSyntheticCaptureSpec, DnsTransport, DnsUpstreamEndpoint,
    DnsUpstreamId, DnsUpstreamSpec, DnsUpstreamStrategy, MAX_DNS_ANSWERS,
    MAX_DNS_CACHE_ENTRIES_PER_PLAN, MAX_DNS_EXPECTED_CIDRS_PER_PLAN, MAX_DNS_INFLIGHT_PER_PLAN,
    MAX_DNS_OVERRIDE_ADDRESSES, MAX_DNS_OVERRIDE_RECORDS, MAX_DNS_PLANS, MAX_DNS_RULES,
    MAX_DNS_SYNTHETIC_CAPTURES, MAX_DNS_SYNTHETIC_ENTRIES, MAX_DNS_TOTAL_CACHE_ENTRIES,
    MAX_DNS_TOTAL_INFLIGHT, MAX_DNS_UPSTREAMS, MAX_DNS_UPSTREAMS_PER_PLAN,
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
    BalancerId, CompiledRouteTable, DnsPlanId, EgressAction, InitialDemand, OutboundId, PortRange,
    RouteAction, RouteCompileError, RouteDecision, RouteDisposition, RouteExplanation, RouteInput,
    RouteMatchSpec, RouteMismatch, RouteRuleSpec, RouteRuleTrace, RouteStage, RuleId,
};
pub use rule_set::{
    CompiledRuleSetRegistry, MAX_ENTRIES_ACROSS_RULE_SET_REGISTRY, MAX_ENTRIES_PER_RULE_SET,
    MAX_RULE_SET_ENVELOPE_BYTES, MAX_RULE_SET_PAYLOAD_BYTES, MAX_RULE_SET_PUBLISHERS,
    MAX_RULE_SETS, RULE_SET_SCHEMA_VERSION, RULE_SET_SIGNATURE_CONTEXT, RuleSetError, RuleSetId,
    RuleSetPublisher, RuleSetPublisherCatalog, RuleSetPublisherId, VerifiedRuleSet,
};
pub use tun_l3::{
    TunL3AddressPlan, TunL3AllocationSpec, TunL3PeerAllocation, TunL3PlanError, TunL3ServerSpec,
};

/// One immutable Product-policy generation shared by all new-flow inbounds.
///
/// Runtime path state is intentionally absent. A caller first classifies and
/// authorizes a normalized flow here, then maps the selected Product ID to its
/// independently owned transport context.
#[derive(Debug)]
pub struct ProductPolicyGeneration {
    routes: CompiledRouteTable,
}

impl ProductPolicyGeneration {
    pub fn compile(
        generation: u64,
        routes: Vec<RouteRuleSpec>,
    ) -> Result<Self, ProductPolicyCompileError> {
        Ok(Self {
            routes: CompiledRouteTable::compile(generation, routes)
                .map_err(ProductPolicyCompileError::Routing)?,
        })
    }

    pub const fn generation(&self) -> u64 {
        self.routes.generation()
    }

    pub const fn routes(&self) -> &CompiledRouteTable {
        &self.routes
    }

    /// Bind the first-match pre-resolution rule to one immutable normalized
    /// flow. A stable reject/drop terminates immediately; an address-dependent
    /// table continues so every answer can be classified independently.
    pub(crate) fn evaluate_pre_resolution_shared(
        &self,
        flow: std::sync::Arc<FlowContext>,
    ) -> Result<PreResolutionDecision, RouteAuthorizationError> {
        self.evaluate_pre_resolution_shared_with_eligibility(flow, |_| true)
    }

    pub(crate) fn evaluate_pre_resolution_shared_with_eligibility(
        &self,
        flow: std::sync::Arc<FlowContext>,
        eligible: impl FnMut(&RouteAction) -> bool,
    ) -> Result<PreResolutionDecision, RouteAuthorizationError> {
        let (decision, requires_post_resolution) = self
            .routes
            .classify_pre_resolution_with_action_eligibility(flow.as_ref(), eligible)
            .ok_or(RouteAuthorizationError::NoUsableAddress)?;
        if !requires_post_resolution {
            terminal_result(decision)?;
        }
        Ok(PreResolutionDecision::new(
            RoutePermit::from_decision(decision, flow),
            requires_post_resolution,
        ))
    }

    /// Authorize a complete DNS result through this same route generation.
    /// Public terminal answers are filtered when an allowed group remains.
    /// Restricted space selected by plain `allow` fails the complete result.
    pub(crate) fn authorize_resolution(
        &self,
        decision: PreResolutionDecision,
        addresses: &[std::net::IpAddr],
        mut eligible: impl FnMut(&RouteAction, std::net::IpAddr) -> bool,
    ) -> Result<AuthorizedResolution, RouteAuthorizationError> {
        use std::sync::Arc;

        if decision.policy_generation() != self.generation() {
            return Err(RouteAuthorizationError::GenerationMismatch {
                evaluated: decision.policy_generation(),
                current: self.generation(),
            });
        }
        let resolution = acl::canonical_resolution(decision.flow(), addresses)?;
        let mut targets = Vec::with_capacity(resolution.len());
        let mut rejected = None;
        let mut dropped = None;
        for address in resolution.iter().copied() {
            let Some(route) = self.routes.classify_with_action_eligibility(
                RouteInput::post_resolution(decision.flow(), address),
                |action| action.egress().is_none() || eligible(action, address),
            ) else {
                continue;
            };
            match route.action().disposition() {
                RouteDisposition::Allow => {
                    if let Some(class) = acl::restricted_ip_class(address) {
                        return Err(RouteAuthorizationError::RestrictedAddress {
                            address,
                            class,
                            rule_id: route.rule_id().clone(),
                        });
                    }
                    targets.push(AuthorizedTarget::new(
                        RoutePermit::from_decision(route, decision.permit().flow.clone()),
                        Arc::clone(&resolution),
                        address,
                    ));
                }
                RouteDisposition::AllowRestricted => targets.push(AuthorizedTarget::new(
                    RoutePermit::from_decision(route, decision.permit().flow.clone()),
                    Arc::clone(&resolution),
                    address,
                )),
                RouteDisposition::Reject => rejected = Some(route.rule_id().clone()),
                RouteDisposition::Drop => dropped = Some(route.rule_id().clone()),
            }
        }
        if !targets.is_empty() {
            return Ok(AuthorizedResolution::new(targets));
        }
        if let Some(rule_id) = dropped {
            return Err(RouteAuthorizationError::Dropped { rule_id });
        }
        if let Some(rule_id) = rejected {
            return Err(RouteAuthorizationError::Rejected { rule_id });
        }
        Err(RouteAuthorizationError::NoUsableAddress)
    }

    pub(crate) fn authorize_domain(
        &self,
        decision: PreResolutionDecision,
    ) -> Result<AuthorizedDomainTarget, RouteAuthorizationError> {
        if decision.policy_generation() != self.generation() {
            return Err(RouteAuthorizationError::GenerationMismatch {
                evaluated: decision.policy_generation(),
                current: self.generation(),
            });
        }
        if decision.flow().target().domain().is_none() {
            return Err(RouteAuthorizationError::ExpectedDomain);
        }
        if decision.requires_post_resolution() {
            return Err(RouteAuthorizationError::PostResolutionRequired);
        }
        terminal_permit_result(decision.permit())?;
        Ok(AuthorizedDomainTarget::new(decision.permit().clone()))
    }

    pub(crate) fn authorize_domain_resolution(
        &self,
        domain: &AuthorizedDomainTarget,
        addresses: &[std::net::IpAddr],
    ) -> Result<AuthorizedResolution, RouteAuthorizationError> {
        use std::sync::Arc;

        if domain.policy_generation() != self.generation() {
            return Err(RouteAuthorizationError::GenerationMismatch {
                evaluated: domain.policy_generation(),
                current: self.generation(),
            });
        }
        let resolution = acl::canonical_resolution(domain.flow(), addresses)?;
        let disposition = domain.permit().action().disposition();
        let mut targets = Vec::with_capacity(resolution.len());
        for address in resolution.iter().copied() {
            if disposition == RouteDisposition::Allow
                && let Some(class) = acl::restricted_ip_class(address)
            {
                return Err(RouteAuthorizationError::RestrictedAddress {
                    address,
                    class,
                    rule_id: domain.permit().rule_id().clone(),
                });
            }
            targets.push(AuthorizedTarget::new(
                domain.permit().clone(),
                Arc::clone(&resolution),
                address,
            ));
        }
        Ok(AuthorizedResolution::new(targets))
    }
}

fn terminal_result(decision: RouteDecision<'_>) -> Result<(), RouteAuthorizationError> {
    match decision.action().disposition() {
        RouteDisposition::Allow | RouteDisposition::AllowRestricted => Ok(()),
        RouteDisposition::Reject => Err(RouteAuthorizationError::Rejected {
            rule_id: decision.rule_id().clone(),
        }),
        RouteDisposition::Drop => Err(RouteAuthorizationError::Dropped {
            rule_id: decision.rule_id().clone(),
        }),
    }
}

fn terminal_permit_result(permit: &RoutePermit) -> Result<(), RouteAuthorizationError> {
    match permit.action().disposition() {
        RouteDisposition::Allow | RouteDisposition::AllowRestricted => Ok(()),
        RouteDisposition::Reject => Err(RouteAuthorizationError::Rejected {
            rule_id: permit.rule_id().clone(),
        }),
        RouteDisposition::Drop => Err(RouteAuthorizationError::Dropped {
            rule_id: permit.rule_id().clone(),
        }),
    }
}

#[derive(Debug)]
pub enum ProductPolicyCompileError {
    Routing(RouteCompileError),
}

impl std::fmt::Display for ProductPolicyCompileError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Routing(error) => write!(formatter, "invalid routing policy: {error}"),
        }
    }
}

impl std::error::Error for ProductPolicyCompileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Routing(error) => Some(error),
        }
    }
}
