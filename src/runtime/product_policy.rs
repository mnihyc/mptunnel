use crate::config::ProductPolicyConfig;
use crate::dns::{DnsWireError, FakeDnsRecovery};
use crate::outbound::{DestinationAuthorizationError, DestinationAuthorizer};
use crate::product::{
    AuthorizedTarget, DestinationAcl, DnsPlanId, EgressAction, FlowContext, InboundId, Network,
    PrincipalId, ProductPolicyGeneration, ProtocolTarget, RouteInput, SourceEndpoint,
    TrafficIntent,
};
use crate::protocol::TargetAddr;
use crate::runtime::error::RuntimeError;
use crate::runtime::outbound_registry::{
    EgressSelection, OpenedTcpOutbound, OpenedUdpOutbound, RuntimeOutboundRegistry,
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
    dns_plan: Option<DnsPlanId>,
    authorizer: ProductFlowDestinationAuthorizer,
}

struct PostResolutionRouteGroup {
    selection: EgressSelection,
    authorized: Vec<AuthorizedTarget>,
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
                "Product TCP open target changed after routing".to_string(),
            ));
        }
        let pending = self
            .registry
            .try_admit_product_flow(&self.authorizer.flow)?;
        let deadline = self.registry.flow_open_deadline();
        let groups = self
            .post_resolution_route_groups(Network::Tcp, target, deadline)
            .await?;
        let mut last_error = None;
        for group in groups {
            match self
                .registry
                .open_authorized_tcp(
                    &pending,
                    &group.selection,
                    &group.authorized,
                    self.dns_plan.as_ref(),
                    group.traffic_class,
                    deadline,
                )
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
                "Product UDP open target changed after routing".to_string(),
            ));
        }
        let pending = self
            .registry
            .try_admit_product_flow(&self.authorizer.flow)?;
        let deadline = self.registry.flow_open_deadline();
        let groups = self
            .post_resolution_route_groups(Network::Udp, target, deadline)
            .await?;
        let mut last_error = None;
        for group in groups {
            match self
                .registry
                .open_authorized_udp(
                    &pending,
                    &group.selection,
                    &group.authorized,
                    self.dns_plan.as_ref(),
                    group.traffic_class,
                    deadline,
                )
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

    async fn post_resolution_route_groups(
        &self,
        network: Network,
        target: &TargetAddr,
        deadline: tokio::time::Instant,
    ) -> Result<Vec<PostResolutionRouteGroup>, RuntimeError> {
        let authorized = crate::outbound::resolve_authorized_target_before(
            self.registry.dns(),
            self.dns_plan.as_ref(),
            &self.authorizer,
            network,
            target,
            deadline,
        )
        .await
        .map_err(|error| match error {
            crate::outbound::OutboundConnectError::DestinationAuthorization(error) => {
                RuntimeError::DestinationDenied(error.to_string())
            }
            error => RuntimeError::OutboundConnect(error),
        })?;
        let mut groups: Vec<PostResolutionRouteGroup> = Vec::new();
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
                EgressAction::Reject | EgressAction::Drop => continue,
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
                groups.push(PostResolutionRouteGroup {
                    selection,
                    authorized: vec![authorized_target],
                    traffic_class,
                });
            }
        }
        if groups.is_empty() {
            return Err(RuntimeError::DestinationDenied(
                "post-resolution routing denied every authorized address".to_string(),
            ));
        }
        Ok(groups)
    }
}

#[derive(Clone)]
struct ProductFlowDestinationAuthorizer {
    policy: Arc<ProductPolicyGeneration>,
    flow: FlowContext,
}

impl DestinationAuthorizer for ProductFlowDestinationAuthorizer {
    fn destination_acl(&self) -> &DestinationAcl {
        self.policy.destination_acl()
    }

    fn flow(
        &self,
        network: Network,
        target: &TargetAddr,
    ) -> Result<FlowContext, DestinationAuthorizationError> {
        let normalized =
            protocol_target(target).map_err(|_| DestinationAuthorizationError::TargetChanged)?;
        if network != self.flow.network() || &normalized != self.flow.target() {
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
        let target = protocol_target(target)?;
        let flow = FlowContext::new(
            network,
            target.clone(),
            SourceEndpoint::from_socket_addr(source),
            principal,
            inbound,
        );
        let decision = self
            .policy
            .routes()
            .classify(RouteInput::pre_resolution(&flow));
        match decision.action().egress() {
            EgressAction::Reject => {
                return Ok(ClientRoute::Deny(ClientPolicyDisposition::Reject));
            }
            EgressAction::Drop => {
                return Ok(ClientRoute::Deny(ClientPolicyDisposition::Drop));
            }
            EgressAction::Direct => {
                return Err(RuntimeError::ProductPolicy(
                    "direct action has no local MPP runtime binding".to_string(),
                ));
            }
            EgressAction::Outbound(_) | EgressAction::Balancer(_) => {}
        }
        self.policy
            .destination_acl()
            .authorize_pre_resolution(flow.clone())
            .map_err(|error| RuntimeError::DestinationDenied(error.to_string()))?;
        match decision.action().egress() {
            EgressAction::Outbound(_) | EgressAction::Balancer(_) => {
                Ok(ClientRoute::Open(ClientOutboundPlan {
                    registry: self.registry.clone(),
                    dns_plan: decision.action().dns_plan().cloned(),
                    authorizer: ProductFlowDestinationAuthorizer {
                        policy: self.policy.clone(),
                        flow,
                    },
                }))
            }
            EgressAction::Reject | EgressAction::Drop | EgressAction::Direct => {
                unreachable!("terminal Product action handled above")
            }
        }
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
mod tests {
    use super::*;
    use crate::config::{
        ClientSecurityConfig, GatewayBalancerConfig, ResourceLimits, SharedSecret,
    };
    use crate::ingress::ProxyAuthConfig;
    use crate::performance::MppPerformanceConfig;
    use crate::product::{
        BalancerId, DomainName, GatewayBalancerSpec, GatewayMemberSpec, GatewayStrategy,
        NetworkSet, OutboundId, RouteAction, RouteMatchSpec, RouteRuleSpec, RouteStage, RuleId,
    };
    use crate::runtime::ingress_runtime::{
        handle_http_connect_client_stream_with_auth, handle_socks5_client_stream_with_auth,
        local_admission_permit_for_test,
    };
    use crate::runtime::outbound_registry::{RuntimeOutboundLeaf, RuntimeOutboundRegistry};
    use crate::runtime::path::ClientPathContext;
    use std::collections::HashMap;
    use std::net::{IpAddr, Ipv4Addr};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn security() -> ClientSecurityConfig {
        ClientSecurityConfig::for_test(
            SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
        )
    }

    fn context(port: u16) -> ClientPathContext {
        ClientPathContext::new(
            vec![format!("udp://127.0.0.1:{port}").parse().expect("path")],
            security(),
            ResourceLimits::default(),
        )
        .expect("context")
    }

    fn registry(
        contexts: impl IntoIterator<Item = (&'static str, ClientPathContext)>,
        balancers: &[GatewayBalancerConfig],
    ) -> RuntimeOutboundRegistry {
        registry_with_dns(
            contexts,
            balancers,
            crate::runtime::outbound_registry::test_dns_generation(),
        )
    }

    fn registry_with_dns(
        contexts: impl IntoIterator<Item = (&'static str, ClientPathContext)>,
        balancers: &[GatewayBalancerConfig],
        dns: crate::dns::DnsGeneration,
    ) -> RuntimeOutboundRegistry {
        RuntimeOutboundRegistry::compile(
            contexts
                .into_iter()
                .map(|(id, context)| RuntimeOutboundLeaf::Mpp {
                    id: OutboundId::parse(id).expect("outbound ID"),
                    context,
                    performance: MppPerformanceConfig::default(),
                }),
            balancers,
            dns,
        )
        .expect("runtime registry")
    }

    fn rule(id: &str, matcher: RouteMatchSpec, egress: EgressAction) -> RouteRuleSpec {
        RouteRuleSpec::new(
            RuleId::parse(id).expect("rule ID"),
            matcher,
            RouteAction::new(egress, None, TrafficIntent::Interactive),
        )
    }

    fn policy(rules: Vec<RouteRuleSpec>) -> ProductPolicyConfig {
        ProductPolicyConfig {
            generation: 9,
            routes: rules,
            destination_acl: Vec::new(),
        }
    }

    fn source() -> SocketAddr {
        "198.51.100.8:41000".parse().expect("source")
    }

    fn inbound() -> InboundId {
        InboundId::parse("local-socks").expect("inbound")
    }

    fn anonymous() -> PrincipalId {
        PrincipalId::parse("anonymous").expect("principal")
    }

    #[tokio::test]
    async fn round_robin_selects_independent_leaves_and_established_binding_stays_fixed() {
        let first_context = context(7443);
        let second_context = context(8443);
        let first_session = first_context.session_id;
        let second_session = second_context.session_id;
        let balancer_id = BalancerId::parse("all-edges").expect("balancer");
        let config = policy(vec![rule(
            "default",
            RouteMatchSpec::default(),
            EgressAction::Balancer(balancer_id.clone()),
        )]);
        let balancers = [GatewayBalancerConfig {
            id: balancer_id,
            generation: config.generation,
            spec: GatewayBalancerSpec::new(
                GatewayStrategy::RoundRobin,
                vec![
                    GatewayMemberSpec::new(
                        OutboundId::parse("edge-a").expect("outbound"),
                        1,
                        NetworkSet::TCP_UDP,
                    ),
                    GatewayMemberSpec::new(
                        OutboundId::parse("edge-b").expect("outbound"),
                        1,
                        NetworkSet::TCP_UDP,
                    ),
                ],
            ),
        }];
        let router = ClientIngressRouter::new(
            &config,
            registry(
                [("edge-a", first_context), ("edge-b", second_context)],
                &balancers,
            ),
        )
        .expect("router");

        let first_target = TargetAddr::Ip("8.8.8.8:443".parse().expect("first target"));
        let ClientRoute::Open(first) = router
            .route_udp(&first_target, source(), anonymous(), inbound())
            .expect("first route")
        else {
            panic!("expected first route");
        };
        let OpenedUdpOutbound::Mpp {
            context: first,
            gateway_lease: first_lease,
            ..
        } = first.open_udp(&first_target).await.expect("first open")
        else {
            panic!("expected MPP UDP leaf");
        };
        assert_eq!(first.session_id, first_session);
        assert!(first_lease.is_some());

        let second_target = TargetAddr::Ip("1.1.1.1:443".parse().expect("second target"));
        let ClientRoute::Open(second) = router
            .route_udp(&second_target, source(), anonymous(), inbound())
            .expect("second route")
        else {
            panic!("expected second route");
        };
        let OpenedUdpOutbound::Mpp {
            context: second, ..
        } = second.open_udp(&second_target).await.expect("second open")
        else {
            panic!("expected MPP UDP leaf");
        };
        assert_eq!(second.session_id, second_session);
        assert_eq!(
            first.session_id, first_session,
            "the first established binding is not migrated by later selection"
        );
    }

    #[tokio::test]
    async fn udp_rules_select_their_own_context_and_datagram_traffic_class() {
        let udp_context = context(7443);
        let tcp_context = context(8443);
        let udp_session = udp_context.session_id;
        let config = policy(vec![
            rule(
                "udp",
                RouteMatchSpec {
                    networks: vec![Network::Udp],
                    ..RouteMatchSpec::default()
                },
                EgressAction::Outbound(OutboundId::parse("udp-edge").expect("outbound")),
            ),
            rule(
                "default",
                RouteMatchSpec::default(),
                EgressAction::Outbound(OutboundId::parse("tcp-edge").expect("outbound")),
            ),
        ]);
        let router = ClientIngressRouter::new(
            &config,
            registry([("udp-edge", udp_context), ("tcp-edge", tcp_context)], &[]),
        )
        .expect("router");
        let target = TargetAddr::Ip("8.8.4.4:443".parse().expect("target"));

        let ClientRoute::Open(selected) = router
            .route_udp(&target, source(), anonymous(), inbound())
            .expect("UDP route")
        else {
            panic!("expected UDP route");
        };
        let OpenedUdpOutbound::Mpp {
            context: selected,
            traffic_class,
            ..
        } = selected.open_udp(&target).await.expect("UDP open")
        else {
            panic!("expected MPP UDP leaf");
        };
        assert_eq!(selected.session_id, udp_session);
        assert_eq!(traffic_class, TrafficClass::RealtimeDatagram);
    }

    #[tokio::test]
    async fn post_dns_routing_authorizes_the_complete_answer_and_opens_only_a_literal_action() {
        let default_context = context(7443);
        let routed_context = context(8443);
        let routed_session = routed_context.session_id;
        let default_id = OutboundId::parse("default-edge").expect("outbound");
        let routed_id = OutboundId::parse("routed-edge").expect("outbound");
        let config = policy(vec![
            rule(
                "skip-first-address",
                RouteMatchSpec {
                    destination_cidrs: vec!["8.8.8.0/24".parse().expect("CIDR")],
                    stages: vec![RouteStage::PostResolution],
                    ..RouteMatchSpec::default()
                },
                EgressAction::Reject,
            ),
            RouteRuleSpec::new(
                RuleId::parse("route-second-address").expect("rule"),
                RouteMatchSpec {
                    destination_cidrs: vec!["1.1.1.0/24".parse().expect("CIDR")],
                    stages: vec![RouteStage::PostResolution],
                    ..RouteMatchSpec::default()
                },
                RouteAction::new(
                    EgressAction::Outbound(routed_id.clone()),
                    None,
                    TrafficIntent::Throughput,
                ),
            ),
            rule(
                "default",
                RouteMatchSpec::default(),
                EgressAction::Outbound(default_id),
            ),
        ]);
        let dns = crate::dns::DnsGeneration::from_test_answers(HashMap::from([
            (
                "post-route.example".to_string(),
                vec![
                    "8.8.8.8".parse().expect("first answer"),
                    "1.1.1.1".parse().expect("second answer"),
                ],
            ),
            (
                "mixed-safety.example".to_string(),
                vec![
                    "1.1.1.1".parse().expect("public answer"),
                    "127.0.0.1".parse().expect("restricted answer"),
                ],
            ),
        ]));
        let router = ClientIngressRouter::new(
            &config,
            registry_with_dns(
                [
                    ("default-edge", default_context),
                    ("routed-edge", routed_context),
                ],
                &[],
                dns,
            ),
        )
        .expect("router");

        let target = TargetAddr::Domain {
            host: "post-route.example".to_string(),
            port: 443,
        };
        let ClientRoute::Open(plan) = router
            .route_udp(&target, source(), anonymous(), inbound())
            .expect("pre-resolution route")
        else {
            panic!("expected an open plan");
        };
        let OpenedUdpOutbound::Mpp {
            context: selected,
            target: routed_target,
            traffic_class,
            ..
        } = plan.open_udp(&target).await.expect("post-resolution open")
        else {
            panic!("expected MPP UDP outbound");
        };
        assert_eq!(selected.session_id, routed_session);
        assert_eq!(
            routed_target,
            TargetAddr::Ip("1.1.1.1:443".parse().expect("literal target"))
        );
        assert_eq!(traffic_class, TrafficClass::Throughput);

        let mixed_safety = TargetAddr::Domain {
            host: "mixed-safety.example".to_string(),
            port: 443,
        };
        let ClientRoute::Open(plan) = router
            .route_udp(&mixed_safety, source(), anonymous(), inbound())
            .expect("pre-resolution route")
        else {
            panic!("expected an open plan");
        };
        assert!(matches!(
            plan.open_udp(&mixed_safety).await,
            Err(RuntimeError::DestinationDenied(_))
        ));
    }

    #[test]
    fn deny_actions_finish_before_target_lookup_or_acl_open_authority() {
        let config = policy(vec![
            rule(
                "drop",
                RouteMatchSpec {
                    domain_exact: vec![DomainName::parse("drop.example").expect("domain")],
                    ..RouteMatchSpec::default()
                },
                EgressAction::Drop,
            ),
            rule("reject", RouteMatchSpec::default(), EgressAction::Reject),
        ]);
        let router = ClientIngressRouter::new(&config, registry([], &[]))
            .expect("router without outbound bindings");
        for (host, expected) in [
            ("drop.example", ClientPolicyDisposition::Drop),
            ("other.example", ClientPolicyDisposition::Reject),
        ] {
            let target = TargetAddr::Domain {
                host: host.to_string(),
                port: 443,
            };
            assert!(matches!(
                router
                    .route_tcp(&target, source(), anonymous(), inbound())
                    .expect("deny route"),
                ClientRoute::Deny(actual) if actual == expected
            ));
            assert!(matches!(
                router
                    .route_udp(&target, source(), anonymous(), inbound())
                    .expect("UDP deny route"),
                ClientRoute::Deny(actual) if actual == expected
            ));
        }
    }

    #[test]
    fn safe_acl_denies_restricted_literal_before_mpp_open() {
        let context = context(7443);
        let config = policy(vec![rule(
            "default",
            RouteMatchSpec::default(),
            EgressAction::Outbound(OutboundId::parse("edge").expect("outbound")),
        )]);
        let router =
            ClientIngressRouter::new(&config, registry([("edge", context)], &[])).expect("router");
        let target = TargetAddr::Ip("127.0.0.1:443".parse().expect("target"));
        assert!(matches!(
            router.route_tcp(&target, source(), anonymous(), inbound()),
            Err(RuntimeError::DestinationDenied(_))
        ));
    }

    #[tokio::test]
    async fn socks5_reject_returns_connection_not_allowed_without_mpp_open() {
        let config = policy(vec![rule(
            "default",
            RouteMatchSpec::default(),
            EgressAction::Reject,
        )]);
        let router = ClientIngressRouter::new(&config, registry([], &[])).expect("router");
        let udp_context = context(7443);
        let (mut client, server) = tokio::io::duplex(1024);
        let task = tokio::spawn(handle_socks5_client_stream_with_auth(
            server,
            udp_context.mux_limits,
            router,
            inbound(),
            source(),
            ProxyAuthConfig::disabled(),
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            local_admission_permit_for_test(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        ));
        client
            .write_all(&[
                0x05, 0x01, 0x00, // no-auth negotiation
                0x05, 0x01, 0x00, 0x03, 0x0b, b'e', b'x', b'a', b'm', b'p', b'l', b'e', b'.', b'c',
                b'o', b'm', 0x01, 0xbb,
            ])
            .await
            .expect("request");
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.expect("response");
        task.await.expect("handler task").expect("policy reject");
        assert_eq!(&response[..2], &[0x05, 0x00]);
        assert_eq!(response[3], 0x02);

        let config = policy(vec![rule(
            "default",
            RouteMatchSpec::default(),
            EgressAction::Drop,
        )]);
        let router = ClientIngressRouter::new(&config, registry([], &[])).expect("router");
        let udp_context = context(7443);
        let (mut client, server) = tokio::io::duplex(1024);
        let task = tokio::spawn(handle_socks5_client_stream_with_auth(
            server,
            udp_context.mux_limits,
            router,
            inbound(),
            source(),
            ProxyAuthConfig::disabled(),
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            local_admission_permit_for_test(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        ));
        client
            .write_all(&[
                0x05, 0x01, 0x00, // no-auth negotiation
                0x05, 0x01, 0x00, 0x03, 0x0b, b'e', b'x', b'a', b'm', b'p', b'l', b'e', b'.', b'c',
                b'o', b'm', 0x01, 0xbb,
            ])
            .await
            .expect("drop request");
        let mut negotiation = [0u8; 2];
        client
            .read_exact(&mut negotiation)
            .await
            .expect("method negotiation");
        assert_eq!(negotiation, [0x05, 0x00]);
        tokio::task::yield_now().await;
        assert!(
            !task.is_finished(),
            "drop must retain the accepted connection without a CONNECT reply"
        );
        drop(client);
        task.await.expect("drop handler task").expect("policy drop");
    }

    #[tokio::test]
    async fn http_connect_reject_returns_forbidden_without_mpp_open() {
        let config = policy(vec![rule(
            "default",
            RouteMatchSpec::default(),
            EgressAction::Reject,
        )]);
        let router = ClientIngressRouter::new(&config, registry([], &[])).expect("router");
        let (mut client, server) = tokio::io::duplex(1024);
        let task = tokio::spawn(handle_http_connect_client_stream_with_auth(
            server,
            router,
            InboundId::parse("local-http").expect("inbound"),
            source(),
            ProxyAuthConfig::disabled(),
            local_admission_permit_for_test(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        ));
        client
            .write_all(b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com\r\n\r\n")
            .await
            .expect("request");
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.expect("response");
        task.await.expect("handler task").expect("policy reject");
        assert!(response.starts_with(b"HTTP/1.1 403 Forbidden\r\n"));

        let config = policy(vec![rule(
            "default",
            RouteMatchSpec::default(),
            EgressAction::Drop,
        )]);
        let router = ClientIngressRouter::new(&config, registry([], &[])).expect("router");
        let (mut client, server) = tokio::io::duplex(1024);
        let task = tokio::spawn(handle_http_connect_client_stream_with_auth(
            server,
            router,
            InboundId::parse("local-http").expect("inbound"),
            source(),
            ProxyAuthConfig::disabled(),
            local_admission_permit_for_test(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        ));
        client
            .write_all(b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com\r\n\r\n")
            .await
            .expect("drop request");
        tokio::task::yield_now().await;
        assert!(
            !task.is_finished(),
            "drop must retain the accepted connection without an HTTP response"
        );
        drop(client);
        task.await.expect("drop handler task").expect("policy drop");
    }
}
