use crate::config::ProductPolicyConfig;
use crate::dns::{DnsWireError, SyntheticCaptureRecovery};
use crate::outbound::{
    DestinationAuthorization, DestinationAuthorizationError, DestinationAuthorizer,
};
use crate::product::{
    DnsPlanId, DnsSyntheticCaptureId, EgressAction, FlowContext, InboundId, InitialDemand, Network,
    PrincipalId, ProductPolicyGeneration, ProtocolTarget, RouteAuthorizationError,
    RouteDisposition, RoutePermit, RuleId, SourceEndpoint,
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

#[allow(clippy::large_enum_variant)]
pub(in crate::runtime) enum ClientRoute {
    Open(ClientOutboundPlan),
    Deny(ClientPolicyDisposition),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RouteOrigin {
    Local,
    Mpp,
}

#[derive(Clone)]
pub(in crate::runtime) struct ClientIngressRouter {
    policy: Arc<ProductPolicyGeneration>,
    registry: RuntimeOutboundRegistry,
}

#[derive(Clone)]
pub(in crate::runtime) struct ClientOutboundPlan {
    registry: RuntimeOutboundRegistry,
    origin: RouteOrigin,
    authorizer: ProductFlowDestinationAuthorizer,
    authorization: DestinationAuthorization,
    recovered_dns: Option<SyntheticCaptureRoute>,
    debug_connection_id: Option<crate::observability::DebugConnectionId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SyntheticCaptureRoute {
    generation: u64,
    plan: DnsPlanId,
    capture: DnsSyntheticCaptureId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime) struct RecoveredTunTarget {
    target: TargetAddr,
    synthetic_capture: Option<SyntheticCaptureRoute>,
}

impl RecoveredTunTarget {
    pub(in crate::runtime) const fn target(&self) -> &TargetAddr {
        &self.target
    }
}

struct DestinationRouteGroup {
    debug_connection_id: Option<crate::observability::DebugConnectionId>,
    rule_id: RuleId,
    disposition: RouteDisposition,
    selection: EgressSelection,
    destination: ProductDestination,
    dns_plan: Option<DnsPlanId>,
    traffic_class: TrafficClass,
}

struct PendingRouteGroup {
    permit: RoutePermit,
    selection: EgressSelection,
    authorized: Vec<crate::product::AuthorizedTarget>,
    dns_plan: Option<DnsPlanId>,
    traffic_class: TrafficClass,
}

impl ClientOutboundPlan {
    pub(in crate::runtime) async fn open_tcp(
        &self,
        target: &TargetAddr,
    ) -> Result<OpenedTcpOutbound, RuntimeError> {
        let normalized = protocol_target(target)?;
        self.ensure_target(Network::Tcp, &normalized)?;
        let pending = self
            .registry
            .try_admit_product_flow(&self.authorizer.flow)?;
        let groups = self.destination_route_groups(Network::Tcp).await?;
        let mut last_error = None;
        for group in groups {
            self.emit_routing_selected(&group);
            let request = ProductOpenRequest {
                pending: &pending,
                selection: &group.selection,
                destination: group.destination,
                authorizer: &self.authorizer,
                dns_plan: group.dns_plan.as_ref(),
                traffic_class: group.traffic_class,
                debug_connection_id: group.debug_connection_id,
            };
            let opened = match self.origin {
                RouteOrigin::Local => self.registry.open_product_tcp(request).await,
                RouteOrigin::Mpp => self.registry.open_product_tcp_from_mpp(request).await,
            };
            match opened {
                Ok((opened, outbound)) => {
                    self.emit_inbound_established();
                    return Ok(opened.with_product_flow(pending.commit(outbound)));
                }
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            RuntimeError::DestinationDenied(
                "routing produced no usable authorized TCP address".to_string(),
            )
        }))
    }

    pub(in crate::runtime) async fn open_udp(
        &self,
        target: &TargetAddr,
    ) -> Result<OpenedUdpOutbound, RuntimeError> {
        let normalized = protocol_target(target)?;
        self.ensure_target(Network::Udp, &normalized)?;
        let pending = self
            .registry
            .try_admit_product_flow(&self.authorizer.flow)?;
        let groups = self.destination_route_groups(Network::Udp).await?;
        let mut last_error = None;
        for group in groups {
            self.emit_routing_selected(&group);
            let request = ProductOpenRequest {
                pending: &pending,
                selection: &group.selection,
                destination: group.destination,
                authorizer: &self.authorizer,
                dns_plan: group.dns_plan.as_ref(),
                traffic_class: group.traffic_class,
                debug_connection_id: group.debug_connection_id,
            };
            let opened = match self.origin {
                RouteOrigin::Local => self.registry.open_product_udp(request).await,
                RouteOrigin::Mpp => self.registry.open_product_udp_from_mpp(request).await,
            };
            match opened {
                Ok((opened, outbound)) => {
                    self.emit_inbound_established();
                    return Ok(opened.with_product_flow(pending.commit(outbound)));
                }
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            RuntimeError::DestinationDenied(
                "routing produced no usable authorized UDP address".to_string(),
            )
        }))
    }

    fn ensure_target(&self, network: Network, target: &ProtocolTarget) -> Result<(), RuntimeError> {
        if self.authorizer.flow.network() != network || self.authorizer.flow.target() != target {
            return Err(RuntimeError::DestinationDenied(
                "target changed after routing".to_string(),
            ));
        }
        Ok(())
    }

    async fn destination_route_groups(
        &self,
        network: Network,
    ) -> Result<Vec<DestinationRouteGroup>, RuntimeError> {
        if self.authorization.target().ip().is_none()
            && !self.authorization.requires_post_resolution()
        {
            let permit = self.authorization.decision.permit();
            let (selection, dns_plan, traffic_class) =
                self.group_metadata(permit, network, None)?;
            let domain = self
                .authorizer
                .authorize_domain(self.authorization.clone())
                .map_err(map_destination_authorization_error)?;
            return Ok(vec![DestinationRouteGroup {
                debug_connection_id: self.debug_connection_id,
                rule_id: permit.rule_id().clone(),
                disposition: permit.action().disposition(),
                selection,
                destination: ProductDestination::domain(domain),
                dns_plan,
                traffic_class,
            }]);
        }

        let resolution_dns_plan = self.effective_dns_plan(self.authorization.decision.permit())?;
        let deadline = self.registry.destination_resolution_deadline(
            resolution_dns_plan.as_ref(),
            self.authorization.target(),
        )?;
        let resolved = crate::outbound::resolve_target_addresses_with_plan_before(
            self.registry.dns(),
            resolution_dns_plan.as_ref(),
            self.authorization.target(),
            deadline,
        )
        .await
        .map_err(RuntimeError::OutboundConnect)?;
        let resolution_dns_plan = resolved.plan;
        let origin = self.origin;
        let authorized = match self.authorizer.policy.authorize_resolution(
            self.authorization.decision.clone(),
            &resolved.addresses,
            |action, address| {
                if let (Some(selected), Some(resolved)) =
                    (action.dns_plan(), resolution_dns_plan.as_ref())
                    && selected != resolved
                {
                    return false;
                }
                let Some(egress) = action.egress() else {
                    return true;
                };
                (origin != RouteOrigin::Mpp || self.registry.action_is_native_egress(egress))
                    && self.registry.action_supports_ip_family(egress, address)
            },
        ) {
            Ok(authorized) => authorized.into_targets(),
            Err(error) => {
                self.emit_routing_terminal_error(&error);
                return Err(map_route_authorization_error(error));
            }
        };

        let mut groups: Vec<PendingRouteGroup> = Vec::new();
        for target in authorized {
            let permit = target.permit().clone();
            if let Some(group) = groups.iter_mut().find(|group| group.permit == permit) {
                group.authorized.push(target);
                continue;
            }
            let (selection, dns_plan, traffic_class) =
                self.group_metadata(&permit, network, resolution_dns_plan.as_ref())?;
            groups.push(PendingRouteGroup {
                permit,
                selection,
                authorized: vec![target],
                dns_plan,
                traffic_class,
            });
        }
        groups
            .into_iter()
            .map(|group| {
                Ok(DestinationRouteGroup {
                    debug_connection_id: self.debug_connection_id,
                    rule_id: group.permit.rule_id().clone(),
                    disposition: group.permit.action().disposition(),
                    selection: group.selection,
                    destination: ProductDestination::resolved(group.authorized)?,
                    dns_plan: group.dns_plan,
                    traffic_class: group.traffic_class,
                })
            })
            .collect()
    }

    fn group_metadata(
        &self,
        permit: &RoutePermit,
        network: Network,
        resolved_dns_plan: Option<&DnsPlanId>,
    ) -> Result<(EgressSelection, Option<DnsPlanId>, TrafficClass), RuntimeError> {
        let egress = permit.action().egress().ok_or_else(|| {
            RuntimeError::ProductPolicy(format!(
                "terminal route {} cannot create an outbound group",
                permit.rule_id()
            ))
        })?;
        if matches!(egress, EgressAction::Direct) {
            return Err(RuntimeError::ProductPolicy(format!(
                "route {} must select a configured outbound",
                permit.rule_id()
            )));
        }
        let selection = self.registry.selection_for_action(egress)?;
        if self.origin == RouteOrigin::Mpp {
            self.registry.ensure_native_egress(&selection)?;
        }
        let selected_dns_plan = self.effective_dns_plan(permit)?;
        let dns_plan = match (resolved_dns_plan, selected_dns_plan) {
            (Some(resolved), Some(selected)) if resolved != &selected => {
                return Err(RuntimeError::DestinationDenied(format!(
                    "route {} selects DNS policy {} but the answer came from policy {}",
                    permit.rule_id(),
                    selected,
                    resolved,
                )));
            }
            (Some(resolved), _) => Some(resolved.clone()),
            (None, selected) => selected,
        };
        Ok((
            selection,
            dns_plan,
            traffic_class(permit.action().initial_demand(), network),
        ))
    }

    fn effective_dns_plan(&self, permit: &RoutePermit) -> Result<Option<DnsPlanId>, RuntimeError> {
        let selected = permit.action().dns_plan();
        let Some(recovered) = &self.recovered_dns else {
            return Ok(selected.cloned());
        };
        if recovered.generation != self.registry.dns().generation() {
            return Err(RuntimeError::DestinationDenied(
                "synthetic-capture DNS generation changed before routing".to_string(),
            ));
        }
        if let Some(selected) = selected
            && selected != &recovered.plan
        {
            return Err(RuntimeError::DestinationDenied(format!(
                "route {} selects DNS policy {} but synthetic capture {} belongs to policy {}",
                permit.rule_id(),
                selected,
                recovered.capture,
                recovered.plan,
            )));
        }
        Ok(Some(recovered.plan.clone()))
    }

    fn emit_inbound_established(&self) {
        emit_inbound_debug_for_flow(
            self.debug_connection_id,
            crate::observability::InboundDebugEvent::Established,
            self.authorizer.flow.as_ref(),
        );
    }

    fn emit_routing_selected(&self, group: &DestinationRouteGroup) {
        if group.debug_connection_id.is_none() {
            return;
        }
        let egress = match &group.selection {
            EgressSelection::Outbound(outbound) => format!("outbound:{}", outbound.as_str()),
            EgressSelection::Balancer(balancer) => format!("balancer:{}", balancer.as_str()),
        };
        let decision = match group.disposition {
            RouteDisposition::Allow => "allow",
            RouteDisposition::AllowRestricted => "allow-restricted",
            RouteDisposition::Reject => "reject",
            RouteDisposition::Drop => "drop",
        };
        debug_assert!(matches!(
            group.disposition,
            RouteDisposition::Allow | RouteDisposition::AllowRestricted
        ));
        let flow = self.authorizer.flow.as_ref();
        let destination = flow.target().authority();
        crate::observability::emit_routing_debug(
            group.debug_connection_id,
            crate::observability::RoutingDebugEvent::Selected,
            debug_network_label(flow.network()),
            Some(group.rule_id.as_str()),
            decision,
            Some(&egress),
            &destination,
        );
    }

    fn emit_routing_terminal_error(&self, error: &RouteAuthorizationError) {
        emit_product_routing_terminal_debug(
            self.debug_connection_id,
            self.authorizer.flow.as_ref(),
            error,
        );
    }
}

pub(super) fn emit_product_routing_terminal_debug(
    debug_connection_id: Option<crate::observability::DebugConnectionId>,
    flow: &FlowContext,
    error: &RouteAuthorizationError,
) {
    let Some((event, rule, decision)) = routing_terminal_debug_fields(error) else {
        return;
    };
    emit_routing_terminal_debug(debug_connection_id, event, flow, rule, decision);
}

fn routing_terminal_debug_fields(
    error: &RouteAuthorizationError,
) -> Option<(crate::observability::RoutingDebugEvent, &str, &'static str)> {
    match error {
        RouteAuthorizationError::RestrictedAddress { rule_id, .. } => Some((
            crate::observability::RoutingDebugEvent::Rejected,
            rule_id.as_str(),
            "restricted-address",
        )),
        RouteAuthorizationError::Rejected { rule_id } => Some((
            crate::observability::RoutingDebugEvent::Rejected,
            rule_id.as_str(),
            "reject",
        )),
        RouteAuthorizationError::Dropped { rule_id } => Some((
            crate::observability::RoutingDebugEvent::Dropped,
            rule_id.as_str(),
            "drop",
        )),
        _ => None,
    }
}

#[derive(Clone)]
struct ProductFlowDestinationAuthorizer {
    policy: Arc<ProductPolicyGeneration>,
    flow: Arc<FlowContext>,
}

impl DestinationAuthorizer for ProductFlowDestinationAuthorizer {
    fn product_policy(&self) -> &ProductPolicyGeneration {
        self.policy.as_ref()
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
        use crate::product::{OutboundId, RouteAction, RouteMatchSpec, RouteRuleSpec, RuleId};

        let id = OutboundId::parse("test-default")
            .map_err(|error| RuntimeError::ProductPolicy(error.to_string()))?;
        let routes = vec![RouteRuleSpec::new(
            RuleId::parse("default")
                .map_err(|error| RuntimeError::ProductPolicy(error.to_string()))?,
            RouteMatchSpec::default(),
            RouteAction::allow_restricted(
                EgressAction::Outbound(id.clone()),
                None,
                InitialDemand::Automatic,
            ),
        )];
        let policy = Arc::new(
            ProductPolicyGeneration::compile(1, routes)
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

    pub(in crate::runtime) fn recover_tun_target(
        &self,
        remote: SocketAddr,
    ) -> Result<RecoveredTunTarget, RuntimeError> {
        match self.registry.dns().recover_synthetic_capture(remote.ip()) {
            SyntheticCaptureRecovery::NotSynthetic => Ok(RecoveredTunTarget {
                target: TargetAddr::Ip(remote),
                synthetic_capture: None,
            }),
            SyntheticCaptureRecovery::Recovered {
                generation,
                plan,
                capture,
                domain,
            } => Ok(RecoveredTunTarget {
                target: TargetAddr::Domain {
                    host: domain.to_string(),
                    port: remote.port(),
                },
                synthetic_capture: Some(SyntheticCaptureRoute {
                    generation,
                    plan,
                    capture,
                }),
            }),
            SyntheticCaptureRecovery::Expired => Err(RuntimeError::DestinationDenied(
                "expired synthetic-capture address rejected before routing".to_string(),
            )),
            SyntheticCaptureRecovery::Unknown => Err(RuntimeError::DestinationDenied(
                "unknown synthetic-capture address rejected before routing".to_string(),
            )),
        }
    }

    pub(in crate::runtime) fn route_tun_tcp(
        &self,
        recovered: &RecoveredTunTarget,
        source: SocketAddr,
        principal: PrincipalId,
        inbound: InboundId,
    ) -> Result<ClientRoute, RuntimeError> {
        self.route(
            Network::Tcp,
            recovered.target(),
            Some(SourceEndpoint::from_socket_addr(source)),
            principal,
            inbound,
            RouteOrigin::Local,
            recovered.synthetic_capture.clone(),
        )
    }

    pub(in crate::runtime) fn route_tun_udp(
        &self,
        recovered: &RecoveredTunTarget,
        source: SocketAddr,
        principal: PrincipalId,
        inbound: InboundId,
    ) -> Result<ClientRoute, RuntimeError> {
        self.route(
            Network::Udp,
            recovered.target(),
            Some(SourceEndpoint::from_socket_addr(source)),
            principal,
            inbound,
            RouteOrigin::Local,
            recovered.synthetic_capture.clone(),
        )
    }

    pub(in crate::runtime) fn route_tcp(
        &self,
        target: &TargetAddr,
        source: SocketAddr,
        principal: PrincipalId,
        inbound: InboundId,
    ) -> Result<ClientRoute, RuntimeError> {
        self.route(
            Network::Tcp,
            target,
            Some(SourceEndpoint::from_socket_addr(source)),
            principal,
            inbound,
            RouteOrigin::Local,
            None,
        )
    }

    pub(in crate::runtime) fn route_udp(
        &self,
        target: &TargetAddr,
        source: SocketAddr,
        principal: PrincipalId,
        inbound: InboundId,
    ) -> Result<ClientRoute, RuntimeError> {
        self.route(
            Network::Udp,
            target,
            Some(SourceEndpoint::from_socket_addr(source)),
            principal,
            inbound,
            RouteOrigin::Local,
            None,
        )
    }

    pub(in crate::runtime) fn route_mpp_tcp(
        &self,
        target: &TargetAddr,
        principal: PrincipalId,
        inbound: InboundId,
    ) -> Result<ClientRoute, RuntimeError> {
        self.route(
            Network::Tcp,
            target,
            None,
            principal,
            inbound,
            RouteOrigin::Mpp,
            None,
        )
    }

    /// Performs the server opening-path policy check silently. An allowed
    /// logical stream is traced later by its relay actor; a terminal decision
    /// is repeated once with a trace identity because no relay will follow.
    pub(in crate::runtime) fn preflight_mpp_tcp(
        &self,
        target: &TargetAddr,
        principal: PrincipalId,
        inbound: InboundId,
    ) -> Result<ClientRoute, RuntimeError> {
        self.preflight_mpp_tcp_with_terminal_debug_connection(
            target,
            principal,
            inbound,
            crate::observability::next_debug_connection_id,
        )
    }

    fn preflight_mpp_tcp_with_terminal_debug_connection(
        &self,
        target: &TargetAddr,
        principal: PrincipalId,
        inbound: InboundId,
        terminal_debug_connection: impl FnOnce() -> Option<crate::observability::DebugConnectionId>,
    ) -> Result<ClientRoute, RuntimeError> {
        let traced_principal = principal.clone();
        let traced_inbound = inbound.clone();
        let preflight = self.preflight_mpp_tcp_once(target, principal, inbound, None);
        match preflight {
            Ok(allowed @ ClientRoute::Open(_)) => Ok(allowed),
            Ok(ClientRoute::Deny(_)) | Err(_) => self.preflight_mpp_tcp_once(
                target,
                traced_principal,
                traced_inbound,
                terminal_debug_connection(),
            ),
        }
    }

    fn preflight_mpp_tcp_once(
        &self,
        target: &TargetAddr,
        principal: PrincipalId,
        inbound: InboundId,
        debug_connection_id: Option<crate::observability::DebugConnectionId>,
    ) -> Result<ClientRoute, RuntimeError> {
        let route = self.route_with_debug_connection(
            Network::Tcp,
            target,
            None,
            principal,
            inbound,
            RouteOrigin::Mpp,
            None,
            debug_connection_id,
        )?;
        let ClientRoute::Open(plan) = route else {
            return Ok(route);
        };
        let Some(address) = plan.authorizer.flow.target().ip() else {
            return Ok(ClientRoute::Open(plan));
        };
        let authorization = plan.authorizer.policy.authorize_resolution(
            plan.authorization.decision.clone(),
            &[address],
            |action, address| {
                let Some(egress) = action.egress() else {
                    return true;
                };
                plan.registry.action_is_native_egress(egress)
                    && plan.registry.action_supports_ip_family(egress, address)
            },
        );
        match authorization {
            Ok(_) => Ok(ClientRoute::Open(plan)),
            Err(error) => {
                plan.emit_routing_terminal_error(&error);
                match error {
                    RouteAuthorizationError::Rejected { .. }
                    | RouteAuthorizationError::RestrictedAddress { .. } => {
                        Ok(ClientRoute::Deny(ClientPolicyDisposition::Reject))
                    }
                    RouteAuthorizationError::Dropped { .. } => {
                        Ok(ClientRoute::Deny(ClientPolicyDisposition::Drop))
                    }
                    error => Err(map_route_authorization_error(error)),
                }
            }
        }
    }

    pub(in crate::runtime) fn route_mpp_udp(
        &self,
        target: &TargetAddr,
        principal: PrincipalId,
        inbound: InboundId,
    ) -> Result<ClientRoute, RuntimeError> {
        self.route(
            Network::Udp,
            target,
            None,
            principal,
            inbound,
            RouteOrigin::Mpp,
            None,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "one shared routing entry point receives the immutable flow identity and optional synthetic-capture provenance from thin local and MPP wrappers"
    )]
    fn route(
        &self,
        network: Network,
        target: &TargetAddr,
        source: Option<SourceEndpoint>,
        principal: PrincipalId,
        inbound: InboundId,
        origin: RouteOrigin,
        recovered_dns: Option<SyntheticCaptureRoute>,
    ) -> Result<ClientRoute, RuntimeError> {
        let debug_connection_id = crate::observability::next_debug_connection_id();
        self.route_with_debug_connection(
            network,
            target,
            source,
            principal,
            inbound,
            origin,
            recovered_dns,
            debug_connection_id,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the shared policy path also receives its optional connection-trace identity so server preflight can remain deliberately unlogged"
    )]
    fn route_with_debug_connection(
        &self,
        network: Network,
        target: &TargetAddr,
        source: Option<SourceEndpoint>,
        principal: PrincipalId,
        inbound: InboundId,
        origin: RouteOrigin,
        recovered_dns: Option<SyntheticCaptureRoute>,
        debug_connection_id: Option<crate::observability::DebugConnectionId>,
    ) -> Result<ClientRoute, RuntimeError> {
        let normalized_target = protocol_target(target)?;
        let flow = Arc::new(match source {
            Some(source) => {
                FlowContext::new(network, normalized_target, source, principal, inbound)
            }
            None => FlowContext::without_source(network, normalized_target, principal, inbound),
        });
        emit_inbound_debug_for_flow(
            debug_connection_id,
            crate::observability::InboundDebugEvent::Accepted,
            flow.as_ref(),
        );
        let decision =
            self.policy
                .evaluate_pre_resolution_shared_with_eligibility(flow.clone(), |action| {
                    action.egress().is_none()
                        || origin == RouteOrigin::Local
                        || action
                            .egress()
                            .is_some_and(|egress| self.registry.action_is_native_egress(egress))
                });
        let mut decision = match decision {
            Ok(decision) => decision,
            Err(error) => {
                emit_product_routing_terminal_debug(debug_connection_id, flow.as_ref(), &error);
                return match error {
                    RouteAuthorizationError::Rejected { .. } => {
                        Ok(ClientRoute::Deny(ClientPolicyDisposition::Reject))
                    }
                    RouteAuthorizationError::Dropped { .. } => {
                        Ok(ClientRoute::Deny(ClientPolicyDisposition::Drop))
                    }
                    error => Err(map_route_authorization_error(error)),
                };
            }
        };
        if let Some(recovered) = &recovered_dns {
            if recovered.generation != self.registry.dns().generation() {
                return Err(RuntimeError::DestinationDenied(
                    "synthetic-capture DNS generation does not match the active resolver"
                        .to_string(),
                ));
            }
            if decision
                .permit()
                .action()
                .dns_plan()
                .is_some_and(|selected| selected != &recovered.plan)
            {
                return Err(RuntimeError::DestinationDenied(format!(
                    "route {} selects a DNS policy different from recovered synthetic capture {}",
                    decision.permit().rule_id(),
                    recovered.capture,
                )));
            }
        }
        // An explicit route DNS selector is an instruction, not metadata. It
        // must resolve even for a domain-capable proxy/Mpp egress; otherwise
        // the selected policy and local restricted-address check would be
        // silently bypassed by next-hop resolution.
        if decision.permit().action().dns_plan().is_some() || recovered_dns.is_some() {
            decision.require_post_resolution();
        }
        if let Some(egress) = decision.permit().action().egress().cloned() {
            if matches!(egress, EgressAction::Direct) && !decision.requires_post_resolution() {
                return Err(RuntimeError::ProductPolicy(format!(
                    "route {} must select a configured outbound",
                    decision.permit().rule_id()
                )));
            }
            if self.registry.action_requires_family_resolution(&egress)? {
                decision.require_post_resolution();
            }
            if !decision.requires_post_resolution() {
                let selection = self.registry.selection_for_action(&egress)?;
                if origin == RouteOrigin::Mpp {
                    self.registry.ensure_native_egress(&selection)?;
                }
            }
        }
        let authorizer = ProductFlowDestinationAuthorizer {
            policy: self.policy.clone(),
            flow,
        };
        Ok(ClientRoute::Open(ClientOutboundPlan {
            registry: self.registry.clone(),
            origin,
            authorizer,
            authorization: DestinationAuthorization { decision },
            recovered_dns,
            debug_connection_id,
        }))
    }
}

const fn debug_network_label(network: Network) -> &'static str {
    match network {
        Network::Tcp => "tcp",
        Network::Udp => "udp",
    }
}

fn emit_inbound_debug_for_flow(
    debug_connection_id: Option<crate::observability::DebugConnectionId>,
    event: crate::observability::InboundDebugEvent,
    flow: &FlowContext,
) {
    if debug_connection_id.is_none() {
        return;
    }
    let (source, destination) = match event {
        crate::observability::InboundDebugEvent::Accepted => (
            flow.source()
                .map(|source| SocketAddr::new(source.address(), source.port()).to_string()),
            Some(flow.target().authority()),
        ),
        crate::observability::InboundDebugEvent::Established => (None, None),
    };
    crate::observability::emit_inbound_debug(
        debug_connection_id,
        event,
        debug_network_label(flow.network()),
        flow.inbound().as_str(),
        source.as_deref(),
        destination.as_deref(),
    );
}

fn emit_routing_terminal_debug(
    debug_connection_id: Option<crate::observability::DebugConnectionId>,
    event: crate::observability::RoutingDebugEvent,
    flow: &FlowContext,
    rule: &str,
    decision: &str,
) {
    if debug_connection_id.is_none() {
        return;
    }
    let destination = flow.target().authority();
    crate::observability::emit_routing_debug(
        debug_connection_id,
        event,
        debug_network_label(flow.network()),
        Some(rule),
        decision,
        None,
        &destination,
    );
}

fn map_destination_authorization_error(error: DestinationAuthorizationError) -> RuntimeError {
    match error {
        DestinationAuthorizationError::Policy(error) => map_route_authorization_error(error),
        error => RuntimeError::DestinationDenied(error.to_string()),
    }
}

fn map_route_authorization_error(error: RouteAuthorizationError) -> RuntimeError {
    match error {
        RouteAuthorizationError::Rejected { .. } => RuntimeError::RouteRejected,
        RouteAuthorizationError::Dropped { .. } => RuntimeError::RouteDropped,
        error => RuntimeError::DestinationDenied(error.to_string()),
    }
}

fn protocol_target(target: &TargetAddr) -> Result<ProtocolTarget, RuntimeError> {
    match target {
        TargetAddr::Domain { host, port } => ProtocolTarget::from_host_port(host, *port),
        TargetAddr::Ip(address) => ProtocolTarget::from_ip(address.ip(), address.port()),
    }
    .map_err(|error| RuntimeError::ProductPolicy(error.to_string()))
}

const fn traffic_class(initial_demand: InitialDemand, network: Network) -> TrafficClass {
    match (initial_demand, network) {
        (InitialDemand::Automatic, Network::Tcp) => TrafficClass::Latency,
        (InitialDemand::Automatic, Network::Udp) => TrafficClass::RealtimeDatagram,
        (InitialDemand::Throughput, _) => TrafficClass::Throughput,
    }
}

#[cfg(test)]
#[path = "tests_product_policy.rs"]
mod tests;
