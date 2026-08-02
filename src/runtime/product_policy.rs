use crate::config::ProductPolicyConfig;
use crate::dns::{DnsWireError, FakeDnsRecovery};
use crate::outbound::{
    DestinationAuthorization, DestinationAuthorizationError, DestinationAuthorizer,
};
use crate::product::{
    DestinationAcl, DnsPlanId, EgressAction, FlowContext, InboundId, Network, PrincipalId,
    ProductPolicyGeneration, ProtocolTarget, RouteInput, SourceEndpoint, TrafficIntent,
};
use crate::protocol::TargetAddr;
use crate::runtime::error::RuntimeError;
use crate::runtime::outbound_registry::{
    EgressSelection, OpenedTcpOutbound, OpenedUdpOutbound, ProductDestination, ProductOpenRequest,
    RuntimeOutboundRegistry,
};
use crate::scheduler::TrafficClass;
use bytes::Bytes;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum ClientPolicyDisposition {
    Reject,
    Drop,
}

// Routing creates this once per flow. Boxing the open plan would impose a heap
// allocation on every accepted TCP/UDP flow solely to shrink the deny branch.
#[allow(clippy::large_enum_variant)]
pub(in crate::runtime) enum ClientRoute {
    Open(ClientOutboundPlan),
    Deny(ClientPolicyDisposition),
}

#[derive(Clone)]
pub(in crate::runtime) struct ClientIngressRouter {
    policy: Arc<ProductPolicyGeneration>,
    registry: RuntimeOutboundRegistry,
}

#[derive(Clone)]
pub(in crate::runtime) struct ClientOutboundPlan {
    registry: RuntimeOutboundRegistry,
    selection: Option<EgressSelection>,
    dns_plan: Option<DnsPlanId>,
    traffic_class: TrafficClass,
    route_requires_post_resolution: bool,
    acl_requires_post_resolution: bool,
    authorizer: ProductFlowDestinationAuthorizer,
    authorization: DestinationAuthorization,
}

struct DestinationRouteGroup {
    selection: EgressSelection,
    destination: ProductDestination,
    traffic_class: TrafficClass,
}

impl ClientOutboundPlan {
    pub(in crate::runtime) async fn open_tcp(
        &self,
        target: &TargetAddr,
    ) -> Result<OpenedTcpOutbound, RuntimeError> {
        let normalized = protocol_target(target)?;
        if self.authorizer.flow.network() != Network::Tcp
            || self.authorizer.flow.target() != &normalized
        {
            return Err(RuntimeError::DestinationDenied(
                "TCP target changed after routing".to_string(),
            ));
        }
        let pending = self
            .registry
            .try_admit_product_flow(&self.authorizer.flow)?;
        let deadline = self
            .registry
            .destination_resolution_deadline(self.dns_plan.as_ref(), self.authorization.target())?;
        let groups = self
            .destination_route_groups(Network::Tcp, deadline)
            .await?;
        let mut last_error = None;
        for group in groups {
            match self
                .registry
                .open_product_tcp(ProductOpenRequest {
                    pending: &pending,
                    selection: &group.selection,
                    destination: group.destination,
                    authorizer: &self.authorizer,
                    dns_plan: self.dns_plan.as_ref(),
                    traffic_class: group.traffic_class,
                })
                .await
            {
                Ok((opened, outbound)) => {
                    return Ok(opened.with_product_flow(pending.commit(outbound)));
                }
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            RuntimeError::DestinationDenied(
                "post-resolution routing denied every authorized TCP address".to_string(),
            )
        }))
    }

    pub(in crate::runtime) async fn open_udp(
        &self,
        target: &TargetAddr,
    ) -> Result<OpenedUdpOutbound, RuntimeError> {
        let normalized = protocol_target(target)?;
        if self.authorizer.flow.network() != Network::Udp
            || self.authorizer.flow.target() != &normalized
        {
            return Err(RuntimeError::DestinationDenied(
                "UDP target changed after routing".to_string(),
            ));
        }
        let pending = self
            .registry
            .try_admit_product_flow(&self.authorizer.flow)?;
        let deadline = self
            .registry
            .destination_resolution_deadline(self.dns_plan.as_ref(), self.authorization.target())?;
        let groups = self
            .destination_route_groups(Network::Udp, deadline)
            .await?;
        let mut last_error = None;
        for group in groups {
            match self
                .registry
                .open_product_udp(ProductOpenRequest {
                    pending: &pending,
                    selection: &group.selection,
                    destination: group.destination,
                    authorizer: &self.authorizer,
                    dns_plan: self.dns_plan.as_ref(),
                    traffic_class: group.traffic_class,
                })
                .await
            {
                Ok((opened, outbound)) => {
                    return Ok(opened.with_product_flow(pending.commit(outbound)));
                }
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            RuntimeError::DestinationDenied(
                "post-resolution routing denied every authorized UDP address".to_string(),
            )
        }))
    }

    async fn destination_route_groups(
        &self,
        network: Network,
        deadline: tokio::time::Instant,
    ) -> Result<Vec<DestinationRouteGroup>, RuntimeError> {
        if self.authorization.target().ip().is_none()
            && !self.route_requires_post_resolution
            && !self.acl_requires_post_resolution
        {
            let domain = self
                .authorizer
                .authorize_domain(self.authorization.clone())
                .map_err(|error| RuntimeError::DestinationDenied(error.to_string()))?;
            return Ok(vec![DestinationRouteGroup {
                selection: self
                    .selection
                    .clone()
                    .expect("stable non-terminal pre-resolution route has an egress"),
                destination: ProductDestination::domain(domain),
                traffic_class: self.traffic_class,
            }]);
        }

        let authorized = crate::outbound::resolve_authorization_before(
            self.registry.dns(),
            self.dns_plan.as_ref(),
            &self.authorizer,
            self.authorization.clone(),
            deadline,
        )
        .await
        .map_err(|error| match error {
            crate::outbound::OutboundConnectError::DestinationAuthorization(error) => {
                RuntimeError::DestinationDenied(error.to_string())
            }
            error => RuntimeError::OutboundConnect(error),
        })?;
        if !self.route_requires_post_resolution {
            return Ok(vec![DestinationRouteGroup {
                selection: self
                    .selection
                    .clone()
                    .expect("non-post-resolution route has an egress"),
                destination: ProductDestination::resolved(authorized)?,
                traffic_class: self.traffic_class,
            }]);
        }

        struct PendingRouteGroup {
            selection: EgressSelection,
            authorized: Vec<crate::product::AuthorizedTarget>,
            traffic_class: TrafficClass,
        }
        let mut groups: Vec<PendingRouteGroup> = Vec::new();
        let mut terminal_disposition = None;
        for authorized_target in authorized {
            let decision = self
                .authorizer
                .policy
                .routes()
                .classify(RouteInput::post_resolution(
                    &self.authorizer.flow,
                    authorized_target.address(),
                ));
            let traffic_class = traffic_class(decision.action().traffic_intent(), network);
            let selection = match decision.action().egress() {
                EgressAction::Outbound(_) | EgressAction::Balancer(_) => self
                    .registry
                    .selection_for_action(decision.action().egress())?,
                EgressAction::Reject => {
                    terminal_disposition.get_or_insert(ClientPolicyDisposition::Reject);
                    continue;
                }
                EgressAction::Drop => {
                    terminal_disposition = Some(ClientPolicyDisposition::Drop);
                    continue;
                }
                EgressAction::Direct => {
                    return Err(RuntimeError::ProductPolicy(format!(
                        "post-resolution route {} must select a configured direct outbound",
                        decision.rule_id().as_str()
                    )));
                }
            };
            if let Some(group) = groups
                .iter_mut()
                .find(|group| group.selection == selection && group.traffic_class == traffic_class)
            {
                group.authorized.push(authorized_target);
            } else {
                groups.push(PendingRouteGroup {
                    selection,
                    authorized: vec![authorized_target],
                    traffic_class,
                });
            }
        }
        if groups.is_empty() {
            return Err(match terminal_disposition {
                Some(ClientPolicyDisposition::Reject) => RuntimeError::RouteRejected,
                Some(ClientPolicyDisposition::Drop) => RuntimeError::RouteDropped,
                None => RuntimeError::DestinationDenied(
                    "post-resolution routing produced no usable address".to_string(),
                ),
            });
        }
        groups
            .into_iter()
            .map(|group| {
                Ok(DestinationRouteGroup {
                    selection: group.selection,
                    destination: ProductDestination::resolved(group.authorized)?,
                    traffic_class: group.traffic_class,
                })
            })
            .collect()
    }
}

#[derive(Clone)]
struct ProductFlowDestinationAuthorizer {
    policy: Arc<ProductPolicyGeneration>,
    flow: Arc<FlowContext>,
}

impl DestinationAuthorizer for ProductFlowDestinationAuthorizer {
    fn destination_acl(&self) -> &DestinationAcl {
        self.policy.destination_acl()
    }

    fn flow(
        &self,
        network: Network,
        target: &ProtocolTarget,
    ) -> Result<Arc<FlowContext>, DestinationAuthorizationError> {
        if network != self.flow.network() || target != self.flow.target() {
            return Err(DestinationAuthorizationError::TargetChanged);
        }
        Ok(self.flow.clone())
    }
}

impl ClientIngressRouter {
    #[cfg(test)]
    pub(in crate::runtime) fn single_for_test(
        context: crate::runtime::path::ClientPathContext,
        performance: crate::performance::MppPerformanceConfig,
    ) -> Result<Self, RuntimeError> {
        use crate::product::{
            AclEffect, AclRuleSpec, OutboundId, RouteAction, RouteMatchSpec, RouteRuleSpec, RuleId,
        };

        let id = OutboundId::parse("test-default")
            .map_err(|error| RuntimeError::ProductPolicy(error.to_string()))?;
        let routes = vec![RouteRuleSpec::new(
            RuleId::parse("default")
                .map_err(|error| RuntimeError::ProductPolicy(error.to_string()))?,
            RouteMatchSpec::default(),
            RouteAction::new(
                EgressAction::Outbound(id.clone()),
                None,
                TrafficIntent::Interactive,
            ),
        )];
        let acl = vec![AclRuleSpec::new(
            RuleId::parse("test-allow-restricted")
                .map_err(|error| RuntimeError::ProductPolicy(error.to_string()))?,
            RouteMatchSpec::default(),
            AclEffect::AllowRestricted,
        )];
        let policy = Arc::new(
            ProductPolicyGeneration::compile(1, routes, acl)
                .map_err(|error| RuntimeError::ProductPolicy(error.to_string()))?,
        );
        let registry = RuntimeOutboundRegistry::compile(
            [
                crate::runtime::outbound_registry::RuntimeOutboundLeaf::Mpp {
                    id,
                    context,
                    performance,
                },
            ],
            &[],
            crate::runtime::outbound_registry::test_dns_generation(),
        )?;
        Ok(Self { policy, registry })
    }

    pub(in crate::runtime) fn new(
        policy: &ProductPolicyConfig,
        registry: RuntimeOutboundRegistry,
    ) -> Result<Self, RuntimeError> {
        let policy = Arc::new(
            policy
                .compile()
                .map_err(|error| RuntimeError::ProductPolicy(error.to_string()))?,
        );
        Ok(Self { policy, registry })
    }

    pub(in crate::runtime) async fn answer_dns_wire_query(
        &self,
        request: &[u8],
        answer_ttl: Duration,
        response_limit: usize,
    ) -> Result<Bytes, DnsWireError> {
        self.registry
            .dns()
            .answer_wire_query(request, answer_ttl, response_limit)
            .await
    }

    /// Recover a captured FakeDNS address exactly once while establishing a TUN
    /// flow. Established payload paths retain the resulting domain target and
    /// never call back into DNS per packet.
    pub(in crate::runtime) fn recover_tun_target(
        &self,
        remote: SocketAddr,
    ) -> Result<TargetAddr, RuntimeError> {
        match self.registry.dns().recover_fake_dns(remote.ip()) {
            FakeDnsRecovery::NotFake => Ok(TargetAddr::Ip(remote)),
            FakeDnsRecovery::Recovered(domain) => Ok(TargetAddr::Domain {
                host: domain.to_string(),
                port: remote.port(),
            }),
            FakeDnsRecovery::Expired => Err(RuntimeError::DestinationDenied(
                "expired FakeDNS address rejected before routing".to_string(),
            )),
            FakeDnsRecovery::Unknown => Err(RuntimeError::DestinationDenied(
                "unknown FakeDNS address rejected before routing".to_string(),
            )),
        }
    }

    pub(in crate::runtime) fn route_tcp(
        &self,
        target: &TargetAddr,
        source: SocketAddr,
        principal: PrincipalId,
        inbound: InboundId,
    ) -> Result<ClientRoute, RuntimeError> {
        self.route(Network::Tcp, target, source, principal, inbound)
    }

    pub(in crate::runtime) fn route_udp(
        &self,
        target: &TargetAddr,
        source: SocketAddr,
        principal: PrincipalId,
        inbound: InboundId,
    ) -> Result<ClientRoute, RuntimeError> {
        self.route(Network::Udp, target, source, principal, inbound)
    }

    fn route(
        &self,
        network: Network,
        target: &TargetAddr,
        source: SocketAddr,
        principal: PrincipalId,
        inbound: InboundId,
    ) -> Result<ClientRoute, RuntimeError> {
        let normalized_target = protocol_target(target)?;
        let flow = Arc::new(FlowContext::new(
            network,
            normalized_target,
            SourceEndpoint::from_socket_addr(source),
            principal,
            inbound,
        ));
        let (decision, route_requires_post_resolution) =
            self.policy.routes().classify_pre_resolution(flow.as_ref());
        let selection = match decision.action().egress() {
            EgressAction::Reject if !route_requires_post_resolution => {
                return Ok(ClientRoute::Deny(ClientPolicyDisposition::Reject));
            }
            EgressAction::Drop if !route_requires_post_resolution => {
                return Ok(ClientRoute::Deny(ClientPolicyDisposition::Drop));
            }
            EgressAction::Reject | EgressAction::Drop => None,
            EgressAction::Direct => {
                return Err(RuntimeError::ProductPolicy(
                    "direct action has no local MPP runtime binding".to_string(),
                ));
            }
            EgressAction::Outbound(_) | EgressAction::Balancer(_) => Some(
                self.registry
                    .selection_for_action(decision.action().egress())?,
            ),
        };
        let dns_plan = decision.action().dns_plan().cloned();
        let selected_traffic_class = traffic_class(decision.action().traffic_intent(), network);
        let authorizer = ProductFlowDestinationAuthorizer {
            policy: self.policy.clone(),
            flow,
        };
        let authorization = authorizer
            .begin_target(network, authorizer.flow.target())
            .map_err(|error| RuntimeError::DestinationDenied(error.to_string()))?;
        let acl_requires_post_resolution = authorization.requires_post_resolution();
        Ok(ClientRoute::Open(ClientOutboundPlan {
            registry: self.registry.clone(),
            selection,
            dns_plan,
            traffic_class: selected_traffic_class,
            route_requires_post_resolution,
            acl_requires_post_resolution,
            authorizer,
            authorization,
        }))
    }
}

fn protocol_target(target: &TargetAddr) -> Result<ProtocolTarget, RuntimeError> {
    match target {
        TargetAddr::Domain { host, port } => ProtocolTarget::from_host_port(host, *port),
        TargetAddr::Ip(address) => ProtocolTarget::from_ip(address.ip(), address.port()),
    }
    .map_err(|error| RuntimeError::ProductPolicy(error.to_string()))
}

const fn traffic_class(intent: TrafficIntent, network: Network) -> TrafficClass {
    match (intent, network) {
        (TrafficIntent::Interactive | TrafficIntent::Realtime, Network::Tcp) => {
            TrafficClass::Latency
        }
        (TrafficIntent::Interactive | TrafficIntent::Realtime, Network::Udp) => {
            TrafficClass::RealtimeDatagram
        }
        (TrafficIntent::Throughput, _) => TrafficClass::Throughput,
        (TrafficIntent::Background, _) => TrafficClass::Background,
    }
}

#[cfg(test)]
#[path = "tests_product_policy.rs"]
mod tests;
