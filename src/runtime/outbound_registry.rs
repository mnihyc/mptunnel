//! Unified Product outbound registry.
//!
//! Selection, pre-commit balancer failover, and connector opening happen once
//! per Product flow. The returned concrete branch is then pinned for the flow
//! lifetime and no routing or balancer decision enters payload forwarding.

use crate::config::{DEFAULT_OUTBOUND_CONNECT_TIMEOUT, EgressRef, GatewayBalancerConfig};
use crate::dns::{
    DirectDnsBackendFactory, DnsBackendError, DnsBackendFactory, DnsGeneration,
    DnsNativeSocketPolicy, DnsQueryBackend, DnsRuntimeError, DnsTcpConnectFuture, DnsTcpConnector,
    DnsTcpStream, DohDnsBackend, RoutedTcpDnsBackend,
};
use crate::outbound::{
    self, DestinationAuthorizer, OutboundConfig, OutboundTcpStream, OutboundUdpSocket,
};
use crate::performance::MppPerformanceConfig;
use crate::product::{
    AuthorizedDomainTarget, AuthorizedTarget, BalancerId, CompiledDnsPlan, CompiledDnsUpstream,
    DnsEgressSpec, DnsPlanId, EgressAction, FlowContext, GatewayMemberMode, Network, NetworkSet,
    OutboundId, PendingProductFlow, ProductAdmission, ProductFlowLease as ProductAdmissionLease,
    ProductOutboundFlow, ProtocolTarget,
};
use crate::protocol::TargetAddr;
use crate::runtime::error::RuntimeError;
use crate::runtime::gateway::{ClientGatewayRuntime, GatewayFlowLease, GatewayRuntimeSnapshot};
use crate::runtime::path::ClientPathContext;
use crate::runtime::relay::open::{ReliableRelayOpenSpec, open_remote_stream};
use crate::runtime::stream::OpenedRemoteStream;
use crate::runtime::telemetry::{
    ObservedProductIo, ProductFlowCounter, ProductFlowLease as RuntimeProductFlowLease,
    ProductFlowOriginKind, ProductFlowScope, RuntimeTelemetry,
};
use crate::scheduler::TrafficClass;
use crate::transport::NativeSocketConfigurator;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};

const MAX_CONCURRENT_GATEWAY_PROBES: usize = 4;

fn gateway_probe_permits() -> Arc<tokio::sync::Semaphore> {
    static PERMITS: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();
    PERMITS
        .get_or_init(|| Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_GATEWAY_PROBES)))
        .clone()
}

#[derive(Clone)]
pub(in crate::runtime) struct RuntimeOutboundRegistry {
    shell: RuntimeOutboundRegistryShell,
    dns: DnsGeneration,
}

#[derive(Clone)]
pub(in crate::runtime) struct RuntimeOutboundRegistryShell {
    leaves: Arc<HashMap<OutboundId, Arc<RuntimeOutboundLeaf>>>,
    balancers: Arc<HashMap<BalancerId, ClientGatewayRuntime>>,
    product_admission: ProductAdmission,
    product_telemetry: RuntimeTelemetry,
}

#[derive(Clone)]
pub(in crate::runtime) struct GatewayRuntimeControl {
    balancers: Arc<HashMap<BalancerId, ClientGatewayRuntime>>,
}

#[derive(Debug, Clone)]
pub(in crate::runtime) struct NamedGatewayRuntimeSnapshot {
    pub(in crate::runtime) id: BalancerId,
    pub(in crate::runtime) runtime: GatewayRuntimeSnapshot,
}

/// DNS factory over the already-validated native subset of the outbound
/// registry. It exists only while an immutable DNS generation is compiled.
pub(in crate::runtime) struct RuntimeOutboundDnsBackendFactory {
    shell: RuntimeOutboundRegistryShell,
    direct_policy: DnsNativeSocketPolicy,
}

#[derive(Clone)]
struct LocalOutboundDnsTcpConnector {
    config: OutboundConfig,
    connect_timeout: Duration,
    native_sockets: Arc<dyn NativeSocketConfigurator>,
}

impl DnsTcpConnector for LocalOutboundDnsTcpConnector {
    fn connect(&self, bootstrap: SocketAddr, timeout: Duration) -> DnsTcpConnectFuture {
        let config = self.config.clone();
        let timeout = timeout.min(self.connect_timeout);
        let native_sockets = self.native_sockets.clone();
        Box::pin(async move {
            outbound::connect_tcp_literal_target_with_configurator(
                &config,
                bootstrap,
                timeout,
                native_sockets.as_ref(),
            )
            .await
            .map(|stream| Box::new(stream) as DnsTcpStream)
            .map_err(|error| match error {
                outbound::OutboundConnectError::ConnectTimeout
                | outbound::OutboundConnectError::ProxyTimeout => DnsBackendError::Timeout,
                error => DnsBackendError::Failed(error.to_string()),
            })
        })
    }
}

#[derive(Clone)]
struct MppOutboundDnsTcpConnector {
    context: ClientPathContext,
    performance: MppPerformanceConfig,
}

impl DnsTcpConnector for MppOutboundDnsTcpConnector {
    fn connect(&self, bootstrap: SocketAddr, timeout: Duration) -> DnsTcpConnectFuture {
        const DNS_MPP_RELAY_BUFFER_BYTES: usize = 64 * 1024;

        let context = self.context.clone();
        let performance = self.performance;
        Box::pin(async move {
            let target = TargetAddr::Ip(bootstrap);
            let remote = tokio::time::timeout(
                timeout,
                open_remote_stream(&context, target.clone(), TrafficClass::Latency),
            )
            .await
            .map_err(|_| DnsBackendError::Timeout)?
            .map_err(|error| DnsBackendError::Failed(error.to_string()))?;
            let (dns_side, relay_side) = tokio::io::duplex(DNS_MPP_RELAY_BUFFER_BYTES);
            tokio::spawn({
                let context = context.clone();
                async move {
                    let _ = crate::runtime::relay::control::relay_migrating_tcp_stream(
                        relay_side,
                        &context,
                        performance,
                        ReliableRelayOpenSpec { target },
                        remote,
                    )
                    .await;
                }
            });
            Ok(Box::new(dns_side) as DnsTcpStream)
        })
    }
}

// Leaves are allocated once behind `Arc` during generation compilation.
// Boxing only the MPP variant would add an extra steady selection dereference
// without reducing any per-flow allocation.
#[allow(clippy::large_enum_variant)]
pub(in crate::runtime) enum RuntimeOutboundLeaf {
    Mpp {
        id: OutboundId,
        context: ClientPathContext,
        performance: MppPerformanceConfig,
    },
    Local {
        id: OutboundId,
        config: OutboundConfig,
        connect_timeout: Duration,
        native_sockets: Arc<dyn NativeSocketConfigurator>,
    },
}

impl RuntimeOutboundLeaf {
    pub(in crate::runtime) const fn id(&self) -> &OutboundId {
        match self {
            Self::Mpp { id, .. } | Self::Local { id, .. } => id,
        }
    }

    pub(in crate::runtime) fn networks(&self) -> NetworkSet {
        match self {
            Self::Mpp { .. } => NetworkSet::TCP_UDP,
            Self::Local { config, .. } if config.supports_udp_targets() => NetworkSet::TCP_UDP,
            Self::Local { .. } => NetworkSet::TCP,
        }
    }

    const fn is_local(&self) -> bool {
        matches!(self, Self::Local { .. })
    }

    fn requires_ip_target(&self) -> bool {
        match self {
            Self::Mpp { .. } => false,
            Self::Local { config, .. } => config.requires_ip_target(),
        }
    }

    const fn open_timeout(&self) -> Duration {
        match self {
            Self::Mpp { .. } => DEFAULT_OUTBOUND_CONNECT_TIMEOUT,
            Self::Local {
                connect_timeout, ..
            } => *connect_timeout,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime) enum EgressSelection {
    Outbound(OutboundId),
    Balancer(BalancerId),
}

// This is the one-time handoff into a concrete relay branch. Boxing the MPP
// side would add one heap allocation to every selected MPP flow.
#[allow(clippy::large_enum_variant)]
pub(in crate::runtime) enum OpenedTcpOutbound {
    Mpp {
        context: ClientPathContext,
        performance: MppPerformanceConfig,
        remote: OpenedRemoteStream,
        spec: ReliableRelayOpenSpec,
        _gateway_lease: Option<GatewayFlowLease>,
        _product_flow: OpenedProductFlow,
    },
    Local {
        stream: OutboundTcpStream,
        _gateway_lease: Option<GatewayFlowLease>,
        _product_flow: OpenedProductFlow,
    },
}

// This is consumed before the payload loop; keep it allocation-free per flow.
#[allow(clippy::large_enum_variant)]
pub(in crate::runtime) enum OpenedUdpOutbound {
    Mpp {
        context: ClientPathContext,
        target: TargetAddr,
        traffic_class: TrafficClass,
        gateway_lease: Option<GatewayFlowLease>,
        product_flow: OpenedProductFlow,
    },
    Local {
        socket: OutboundUdpSocket,
        _gateway_lease: Option<GatewayFlowLease>,
        _product_flow: OpenedProductFlow,
    },
}

struct ProductLeafOpen<'a> {
    destination: &'a mut ProductDestination,
    dns_plan: Option<&'a DnsPlanId>,
    traffic_class: TrafficClass,
    gateway_lease: Option<GatewayFlowLease>,
    scope: ProductFlowScope,
    observe_native: bool,
}

pub(in crate::runtime) struct ProductOpenRequest<'a> {
    pub(in crate::runtime) pending: &'a PendingProductFlow,
    pub(in crate::runtime) selection: &'a EgressSelection,
    pub(in crate::runtime) destination: ProductDestination,
    pub(in crate::runtime) authorizer: &'a dyn DestinationAuthorizer,
    pub(in crate::runtime) dns_plan: Option<&'a DnsPlanId>,
    pub(in crate::runtime) traffic_class: TrafficClass,
}

pub(in crate::runtime) enum ProductDestination {
    Domain(AuthorizedDomainTarget),
    Resolved(ResolvedProductDestination),
}

pub(in crate::runtime) struct ResolvedProductDestination {
    targets: Vec<AuthorizedTarget>,
}

impl ResolvedProductDestination {
    fn new(targets: Vec<AuthorizedTarget>) -> Result<Self, RuntimeError> {
        authorized_flow(&targets)?;
        Ok(Self { targets })
    }

    fn flow(&self) -> &FlowContext {
        self.targets
            .first()
            .expect("validated resolved Product destination has an address")
            .flow()
    }

    fn targets(&self) -> &[AuthorizedTarget] {
        &self.targets
    }
}

impl ProductDestination {
    pub(in crate::runtime) const fn domain(domain: AuthorizedDomainTarget) -> Self {
        Self::Domain(domain)
    }

    pub(in crate::runtime) fn resolved(
        targets: Vec<AuthorizedTarget>,
    ) -> Result<Self, RuntimeError> {
        ResolvedProductDestination::new(targets).map(Self::Resolved)
    }

    fn flow(&self) -> Result<&FlowContext, RuntimeError> {
        match self {
            Self::Domain(domain) => Ok(domain.flow()),
            Self::Resolved(resolved) => Ok(resolved.flow()),
        }
    }
}

/// Product ownership retained for the lifetime of one concrete opened branch.
///
/// Admission and runtime observation have independent lifecycles but share the
/// same immutable route identity. Server MPP flows retain only admission here
/// because their existing MPP boundary is the sole traffic observer.
pub(in crate::runtime) struct OpenedProductFlow {
    scope: ProductFlowScope,
    admission: Option<ProductAdmissionLease>,
    runtime: Option<RuntimeProductFlowLease>,
}

impl OpenedProductFlow {
    fn new(
        scope: ProductFlowScope,
        telemetry: Option<&RuntimeTelemetry>,
        network: Network,
    ) -> Self {
        let runtime = telemetry.map(|telemetry| match network {
            Network::Tcp => telemetry.open_native_reliable_flow(scope.clone()),
            Network::Udp => telemetry.open_native_datagram_flow(scope.clone()),
        });
        Self {
            scope,
            admission: None,
            runtime,
        }
    }

    pub(in crate::runtime) fn scope(&self) -> &ProductFlowScope {
        &self.scope
    }

    pub(in crate::runtime) fn runtime_counter(&self) -> Option<ProductFlowCounter> {
        self.runtime.as_ref().map(RuntimeProductFlowLease::counter)
    }

    pub(in crate::runtime) fn complete_runtime(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.complete();
        }
    }

    fn attach_admission(&mut self, admission: ProductAdmissionLease) {
        assert!(
            self.admission.replace(admission).is_none(),
            "Product flow admission attached more than once"
        );
    }
}

impl OpenedTcpOutbound {
    pub(in crate::runtime) fn with_product_flow(
        mut self,
        product_flow: ProductAdmissionLease,
    ) -> Self {
        let owner = match &mut self {
            Self::Mpp { _product_flow, .. } | Self::Local { _product_flow, .. } => _product_flow,
        };
        owner.attach_admission(product_flow);
        self
    }
}

impl OpenedUdpOutbound {
    pub(in crate::runtime) fn with_product_flow(
        mut self,
        product_flow: ProductAdmissionLease,
    ) -> Self {
        let owner = match &mut self {
            Self::Mpp { product_flow, .. } => product_flow,
            Self::Local { _product_flow, .. } => _product_flow,
        };
        owner.attach_admission(product_flow);
        self
    }
}

pub(in crate::runtime) async fn relay_opened_tcp<S>(
    local: S,
    opened: OpenedTcpOutbound,
) -> Result<(), RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match opened {
        OpenedTcpOutbound::Mpp {
            context,
            performance,
            remote,
            spec,
            mut _gateway_lease,
            _product_flow,
        } => {
            let result = crate::runtime::relay::control::relay_migrating_tcp_stream(
                local,
                &context,
                performance,
                spec,
                remote,
            )
            .await
            .map(|_| ());
            finish_gateway_flow(&mut _gateway_lease, &result);
            result?;
        }
        OpenedTcpOutbound::Local {
            stream,
            mut _gateway_lease,
            mut _product_flow,
        } => {
            let counter = _product_flow
                .runtime_counter()
                .ok_or(RuntimeError::Protocol(
                    "local Product TCP flow is missing its runtime observer",
                ))?;
            let mut local = ObservedProductIo::new(local, counter);
            let result = match stream {
                OutboundTcpStream::Plain(mut remote) => {
                    tokio::io::copy_bidirectional(&mut local, &mut remote)
                        .await
                        .map(|_| ())
                        .map_err(RuntimeError::from)
                }
                OutboundTcpStream::Tls(mut remote) => {
                    tokio::io::copy_bidirectional(&mut local, remote.as_mut())
                        .await
                        .map(|_| ())
                        .map_err(RuntimeError::from)
                }
            };
            finish_gateway_flow(&mut _gateway_lease, &result);
            if result.is_ok() {
                _product_flow.complete_runtime();
            }
            result?;
        }
    }
    Ok(())
}

pub(in crate::runtime) fn finish_gateway_flow(
    lease: &mut Option<GatewayFlowLease>,
    outcome: &Result<(), RuntimeError>,
) {
    let Some(lease) = lease.as_mut() else {
        return;
    };
    let error = outcome.as_ref().err().map(ToString::to_string);
    if let Err(feedback) = lease.completed(error) {
        crate::observability::process_event!(
            Warn,
            "balancer",
            "flow_outcome_feedback_failed",
            "balancer flow-outcome feedback failed: {feedback}"
        );
    }
}

impl RuntimeOutboundRegistry {
    #[cfg(test)]
    pub(in crate::runtime) fn compile(
        leaves: impl IntoIterator<Item = RuntimeOutboundLeaf>,
        balancer_configs: &[GatewayBalancerConfig],
        dns: DnsGeneration,
    ) -> Result<Self, RuntimeError> {
        Ok(RuntimeOutboundRegistryShell::compile(leaves, balancer_configs)?.with_dns(dns))
    }

    pub(in crate::runtime) fn selection_for_action(
        &self,
        action: &EgressAction,
    ) -> Result<EgressSelection, RuntimeError> {
        self.shell.selection_for_action(action)
    }

    pub(in crate::runtime) const fn dns(&self) -> &DnsGeneration {
        &self.dns
    }

    pub(in crate::runtime) fn product_admission(&self) -> &ProductAdmission {
        self.shell.product_admission()
    }

    pub(in crate::runtime) fn gateway_control(&self) -> GatewayRuntimeControl {
        GatewayRuntimeControl {
            balancers: self.shell.balancers.clone(),
        }
    }

    pub(in crate::runtime) fn try_admit_product_flow(
        &self,
        flow: &FlowContext,
    ) -> Result<PendingProductFlow, RuntimeError> {
        self.shell
            .product_admission()
            .try_admit_flow(flow.principal().clone(), flow.target().clone())
            .map_err(RuntimeError::ProductAdmission)
    }

    pub(in crate::runtime) fn spawn_gateway_probe_services(
        &self,
        services: &mut tokio::task::JoinSet<Result<(), RuntimeError>>,
    ) {
        let permits = gateway_probe_permits();
        for (id, runtime) in self.shell.balancers.iter() {
            let Some(policy) = runtime.probe_policy().cloned() else {
                continue;
            };
            services.spawn(run_gateway_probe_service(
                self.clone(),
                id.clone(),
                runtime.clone(),
                policy,
                permits.clone(),
            ));
        }
    }

    pub(in crate::runtime) fn destination_resolution_deadline(
        &self,
        dns_plan: Option<&DnsPlanId>,
        target: &ProtocolTarget,
    ) -> Result<tokio::time::Instant, RuntimeError> {
        let Some(domain) = target.domain() else {
            return deadline_after(Duration::ZERO);
        };
        let timeout = self.dns.lookup_timeout(dns_plan, domain).map_err(|error| {
            RuntimeError::OutboundConnect(outbound::OutboundConnectError::Dns(error))
        })?;
        deadline_after(timeout)
    }

    pub(in crate::runtime) fn selection_for_egress(
        &self,
        egress: &EgressRef,
    ) -> Result<EgressSelection, RuntimeError> {
        self.shell.selection_for_egress(egress)
    }

    /// Proves the server-side no-chaining invariant at runtime assembly.
    ///
    /// Configuration validation performs the same check, but server services
    /// retain this defense so a hand-built runtime cannot forward one MPP
    /// session into another MPP session.
    pub(in crate::runtime) fn ensure_native_egress(
        &self,
        selection: &EgressSelection,
    ) -> Result<(), RuntimeError> {
        self.shell.ensure_native_egress(selection)
    }

    pub(in crate::runtime) async fn open_tcp(
        &self,
        selection: &EgressSelection,
        target: &TargetAddr,
        dns_plan: Option<&DnsPlanId>,
        traffic_class: TrafficClass,
        authorizer: &dyn DestinationAuthorizer,
    ) -> Result<OpenedTcpOutbound, RuntimeError> {
        let authorization = authorizer
            .begin(Network::Tcp, target)
            .map_err(|error| RuntimeError::DestinationDenied(error.to_string()))?;
        let pending = self.try_admit_product_flow(authorization.flow())?;
        let deadline = self.destination_resolution_deadline(dns_plan, authorization.target())?;
        let destination =
            authorize_product_destination(&self.dns, dns_plan, authorizer, authorization, deadline)
                .await?;
        let (opened, outbound) = self
            .open_product_tcp_for_origin(
                ProductOpenRequest {
                    pending: &pending,
                    selection,
                    destination,
                    authorizer,
                    dns_plan,
                    traffic_class,
                },
                ProductFlowOriginKind::MppInbound,
                false,
            )
            .await?;
        Ok(opened.with_product_flow(pending.commit(outbound)))
    }

    pub(in crate::runtime) async fn open_udp(
        &self,
        selection: &EgressSelection,
        target: &TargetAddr,
        dns_plan: Option<&DnsPlanId>,
        authorizer: &dyn DestinationAuthorizer,
    ) -> Result<OpenedUdpOutbound, RuntimeError> {
        let authorization = authorizer
            .begin(Network::Udp, target)
            .map_err(|error| RuntimeError::DestinationDenied(error.to_string()))?;
        let pending = self.try_admit_product_flow(authorization.flow())?;
        let deadline = self.destination_resolution_deadline(dns_plan, authorization.target())?;
        let destination =
            authorize_product_destination(&self.dns, dns_plan, authorizer, authorization, deadline)
                .await?;
        let (opened, outbound) = self
            .open_product_udp_for_origin(
                ProductOpenRequest {
                    pending: &pending,
                    selection,
                    destination,
                    authorizer,
                    dns_plan,
                    traffic_class: TrafficClass::RealtimeDatagram,
                },
                ProductFlowOriginKind::MppInbound,
                false,
            )
            .await?;
        Ok(opened.with_product_flow(pending.commit(outbound)))
    }

    pub(in crate::runtime) async fn open_product_tcp(
        &self,
        request: ProductOpenRequest<'_>,
    ) -> Result<(OpenedTcpOutbound, ProductOutboundFlow), RuntimeError> {
        self.open_product_tcp_for_origin(request, ProductFlowOriginKind::LocalInbound, true)
            .await
    }

    async fn open_product_tcp_for_origin(
        &self,
        request: ProductOpenRequest<'_>,
        origin_kind: ProductFlowOriginKind,
        observe_native: bool,
    ) -> Result<(OpenedTcpOutbound, ProductOutboundFlow), RuntimeError> {
        let ProductOpenRequest {
            pending,
            selection,
            mut destination,
            authorizer,
            dns_plan,
            traffic_class,
        } = request;
        ensure_destination_network(&destination, Network::Tcp)?;
        ensure_product_open_identity(&destination, pending)?;
        let protocol_target = pending.target();
        let principal = pending.principal();
        match selection {
            EgressSelection::Outbound(id) => {
                let leaf = self.shell.require_leaf(id, Network::Tcp)?;
                if leaf.requires_ip_target() {
                    self.resolve_destination(&mut destination, dns_plan, authorizer)
                        .await?;
                }
                let scope = product_flow_scope(&destination, origin_kind, leaf.id(), None)?;
                let connect = pending
                    .try_begin_connect(leaf.id().clone())
                    .map_err(RuntimeError::ProductAdmission)?;
                let opened = self
                    .open_product_tcp_leaf(
                        leaf,
                        ProductLeafOpen {
                            destination: &mut destination,
                            dns_plan,
                            traffic_class,
                            gateway_lease: None,
                            scope,
                            observe_native,
                        },
                    )
                    .await?;
                Ok((opened, connect.connected()))
            }
            EgressSelection::Balancer(id) => {
                let runtime = self.shell.require_balancer(id)?;
                let attempt_limit = runtime.member_count();
                let mut excluded = Vec::with_capacity(attempt_limit);
                let mut last_error = None;
                let mut resolution_unavailable = false;
                for _ in 0..attempt_limit {
                    let binding = match runtime.select_for_principal(
                        Network::Tcp,
                        protocol_target,
                        Some(principal),
                        &excluded,
                    ) {
                        Ok(binding) => binding,
                        Err(error) => return Err(last_error.unwrap_or(error)),
                    };
                    let handle = binding.handle;
                    let leaf = self
                        .shell
                        .require_leaf(runtime.member_id(handle)?, Network::Tcp)?;
                    if leaf.requires_ip_target() {
                        if resolution_unavailable {
                            excluded.push(handle);
                            continue;
                        }
                        if let Err(error) = self
                            .resolve_destination(&mut destination, dns_plan, authorizer)
                            .await
                        {
                            if matches!(error, RuntimeError::DestinationDenied(_)) {
                                return Err(error);
                            }
                            resolution_unavailable = true;
                            excluded.push(handle);
                            last_error = Some(error);
                            continue;
                        }
                    }
                    let scope = product_flow_scope(&destination, origin_kind, leaf.id(), Some(id))?;
                    let connect = match pending.try_begin_connect(leaf.id().clone()) {
                        Ok(connect) => connect,
                        Err(error) => {
                            excluded.push(handle);
                            last_error = Some(RuntimeError::ProductAdmission(error));
                            continue;
                        }
                    };
                    match self
                        .open_product_tcp_leaf(
                            leaf,
                            ProductLeafOpen {
                                destination: &mut destination,
                                dns_plan,
                                traffic_class,
                                gateway_lease: Some(binding.lease),
                                scope,
                                observe_native,
                            },
                        )
                        .await
                    {
                        Ok(opened) => return Ok((opened, connect.connected())),
                        Err(error @ RuntimeError::DestinationDenied(_)) => return Err(error),
                        Err(error) => {
                            excluded.push(handle);
                            last_error = Some(error);
                        }
                    }
                }
                Err(last_error.unwrap_or_else(|| {
                    RuntimeError::GatewayUnavailable(
                        "balancer has no TCP member attempts".to_string(),
                    )
                }))
            }
        }
    }

    pub(in crate::runtime) async fn open_product_udp(
        &self,
        request: ProductOpenRequest<'_>,
    ) -> Result<(OpenedUdpOutbound, ProductOutboundFlow), RuntimeError> {
        self.open_product_udp_for_origin(request, ProductFlowOriginKind::LocalInbound, true)
            .await
    }

    async fn open_product_udp_for_origin(
        &self,
        request: ProductOpenRequest<'_>,
        origin_kind: ProductFlowOriginKind,
        observe_native: bool,
    ) -> Result<(OpenedUdpOutbound, ProductOutboundFlow), RuntimeError> {
        let ProductOpenRequest {
            pending,
            selection,
            mut destination,
            authorizer,
            dns_plan,
            traffic_class,
        } = request;
        ensure_destination_network(&destination, Network::Udp)?;
        ensure_product_open_identity(&destination, pending)?;
        let protocol_target = pending.target();
        let principal = pending.principal();
        match selection {
            EgressSelection::Outbound(id) => {
                let leaf = self.shell.require_leaf(id, Network::Udp)?;
                if leaf.requires_ip_target() {
                    self.resolve_destination(&mut destination, dns_plan, authorizer)
                        .await?;
                }
                let scope = product_flow_scope(&destination, origin_kind, leaf.id(), None)?;
                let connect = pending
                    .try_begin_connect(leaf.id().clone())
                    .map_err(RuntimeError::ProductAdmission)?;
                let opened = self
                    .open_product_udp_leaf(
                        leaf,
                        ProductLeafOpen {
                            destination: &mut destination,
                            dns_plan,
                            traffic_class,
                            gateway_lease: None,
                            scope,
                            observe_native,
                        },
                    )
                    .await?;
                Ok((opened, connect.connected()))
            }
            EgressSelection::Balancer(id) => {
                let runtime = self.shell.require_balancer(id)?;
                let attempt_limit = runtime.member_count();
                let mut excluded = Vec::with_capacity(attempt_limit);
                let mut last_error = None;
                let mut resolution_unavailable = false;
                for _ in 0..attempt_limit {
                    let binding = match runtime.select_for_principal(
                        Network::Udp,
                        protocol_target,
                        Some(principal),
                        &excluded,
                    ) {
                        Ok(binding) => binding,
                        Err(error) => return Err(last_error.unwrap_or(error)),
                    };
                    let handle = binding.handle;
                    let leaf = self
                        .shell
                        .require_leaf(runtime.member_id(handle)?, Network::Udp)?;
                    if leaf.requires_ip_target() {
                        if resolution_unavailable {
                            excluded.push(handle);
                            continue;
                        }
                        if let Err(error) = self
                            .resolve_destination(&mut destination, dns_plan, authorizer)
                            .await
                        {
                            if matches!(error, RuntimeError::DestinationDenied(_)) {
                                return Err(error);
                            }
                            resolution_unavailable = true;
                            excluded.push(handle);
                            last_error = Some(error);
                            continue;
                        }
                    }
                    let scope = product_flow_scope(&destination, origin_kind, leaf.id(), Some(id))?;
                    let connect = match pending.try_begin_connect(leaf.id().clone()) {
                        Ok(connect) => connect,
                        Err(error) => {
                            excluded.push(handle);
                            last_error = Some(RuntimeError::ProductAdmission(error));
                            continue;
                        }
                    };
                    match self
                        .open_product_udp_leaf(
                            leaf,
                            ProductLeafOpen {
                                destination: &mut destination,
                                dns_plan,
                                traffic_class,
                                gateway_lease: Some(binding.lease),
                                scope,
                                observe_native,
                            },
                        )
                        .await
                    {
                        Ok(opened) => return Ok((opened, connect.connected())),
                        Err(error @ RuntimeError::DestinationDenied(_)) => return Err(error),
                        Err(error) => {
                            excluded.push(handle);
                            last_error = Some(error);
                        }
                    }
                }
                Err(last_error.unwrap_or_else(|| {
                    RuntimeError::GatewayUnavailable(
                        "balancer has no UDP member attempts".to_string(),
                    )
                }))
            }
        }
    }

    async fn open_product_tcp_leaf(
        &self,
        leaf: Arc<RuntimeOutboundLeaf>,
        request: ProductLeafOpen<'_>,
    ) -> Result<OpenedTcpOutbound, RuntimeError> {
        let ProductLeafOpen {
            destination,
            dns_plan,
            traffic_class,
            mut gateway_lease,
            scope,
            observe_native,
        } = request;
        let deadline = deadline_after(leaf.open_timeout())?;
        let opened: Result<OpenedTcpOutbound, RuntimeError> = async {
            match leaf.as_ref() {
                RuntimeOutboundLeaf::Mpp {
                    context,
                    performance,
                    ..
                } => {
                    let context = context.with_product_flow_scope(scope.clone());
                    let (remote, target) =
                        open_mpp_tcp_destination(&context, destination, traffic_class, deadline)
                            .await?;
                    Ok(OpenedTcpOutbound::Mpp {
                        context: context.clone(),
                        performance: *performance,
                        remote,
                        spec: ReliableRelayOpenSpec { target },
                        _gateway_lease: None,
                        _product_flow: OpenedProductFlow::new(scope.clone(), None, Network::Tcp),
                    })
                }
                RuntimeOutboundLeaf::Local {
                    config,
                    native_sockets,
                    ..
                } => {
                    let connector_target = connector_target(destination)?;
                    let stream = outbound::connect_tcp_target_with_configurator(
                        config,
                        &self.dns,
                        dns_plan,
                        connector_target,
                        deadline,
                        native_sockets.as_ref(),
                    )
                    .await?;
                    Ok(OpenedTcpOutbound::Local {
                        stream,
                        _gateway_lease: None,
                        _product_flow: OpenedProductFlow::new(
                            scope.clone(),
                            observe_native.then_some(&self.shell.product_telemetry),
                            Network::Tcp,
                        ),
                    })
                }
            }
        }
        .await;
        let opened = match opened {
            Ok(opened) => opened,
            Err(error) => {
                if let Some(lease) = gateway_lease.as_mut()
                    && let Err(feedback) = lease.failed(error.to_string())
                {
                    crate::observability::process_event!(
                        Warn,
                        "balancer",
                        "open_failure_feedback_failed",
                        "balancer open-failure feedback failed: {feedback}"
                    );
                }
                return Err(error);
            }
        };
        if let Some(lease) = gateway_lease.as_mut() {
            lease.opened()?;
        }
        Ok(match opened {
            OpenedTcpOutbound::Mpp {
                context,
                performance,
                remote,
                spec,
                _product_flow,
                ..
            } => OpenedTcpOutbound::Mpp {
                context,
                performance,
                remote,
                spec,
                _gateway_lease: gateway_lease,
                _product_flow,
            },
            OpenedTcpOutbound::Local {
                stream,
                _product_flow,
                ..
            } => OpenedTcpOutbound::Local {
                stream,
                _gateway_lease: gateway_lease,
                _product_flow,
            },
        })
    }

    async fn open_product_udp_leaf(
        &self,
        leaf: Arc<RuntimeOutboundLeaf>,
        request: ProductLeafOpen<'_>,
    ) -> Result<OpenedUdpOutbound, RuntimeError> {
        let ProductLeafOpen {
            destination,
            dns_plan,
            traffic_class,
            mut gateway_lease,
            scope,
            observe_native,
        } = request;
        match leaf.as_ref() {
            RuntimeOutboundLeaf::Mpp { context, .. } => {
                let target = mpp_udp_target(destination)?;
                Ok(OpenedUdpOutbound::Mpp {
                    context: context.with_product_flow_scope(scope.clone()),
                    target,
                    traffic_class,
                    gateway_lease,
                    product_flow: OpenedProductFlow::new(scope, None, Network::Udp),
                })
            }
            RuntimeOutboundLeaf::Local {
                config,
                native_sockets,
                ..
            } => {
                let connector_target = connector_target(destination)?;
                let deadline = deadline_after(leaf.open_timeout())?;
                let socket = match outbound::connect_udp_target_with_configurator(
                    config,
                    &self.dns,
                    dns_plan,
                    connector_target,
                    deadline,
                    native_sockets.as_ref(),
                )
                .await
                {
                    Ok(socket) => socket,
                    Err(error) => {
                        if let Some(lease) = gateway_lease.as_mut() {
                            lease.failed(error.to_string())?;
                        }
                        return Err(error.into());
                    }
                };
                if let Some(lease) = gateway_lease.as_mut() {
                    lease.opened()?;
                }
                Ok(OpenedUdpOutbound::Local {
                    socket,
                    _gateway_lease: gateway_lease,
                    _product_flow: OpenedProductFlow::new(
                        scope,
                        observe_native.then_some(&self.shell.product_telemetry),
                        Network::Udp,
                    ),
                })
            }
        }
    }

    async fn resolve_destination<'a>(
        &self,
        destination: &'a mut ProductDestination,
        dns_plan: Option<&DnsPlanId>,
        authorizer: &dyn DestinationAuthorizer,
    ) -> Result<&'a [AuthorizedTarget], RuntimeError> {
        if let ProductDestination::Domain(domain) = destination {
            let deadline =
                self.destination_resolution_deadline(dns_plan, domain.flow().target())?;
            let authorized = outbound::resolve_authorized_domain_before(
                &self.dns, dns_plan, authorizer, domain, deadline,
            )
            .await
            .map_err(map_destination_resolution_error)?;
            *destination = ProductDestination::resolved(authorized)?;
        }
        match destination {
            ProductDestination::Resolved(resolved) => Ok(resolved.targets()),
            ProductDestination::Domain(_) => {
                unreachable!("domain destination was promoted to resolved addresses")
            }
        }
    }
}

async fn run_gateway_probe_service(
    registry: RuntimeOutboundRegistry,
    id: BalancerId,
    runtime: ClientGatewayRuntime,
    policy: crate::product::GatewayProbePolicy,
    permits: Arc<tokio::sync::Semaphore>,
) -> Result<(), RuntimeError> {
    let mut ticker = tokio::time::interval(policy.interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        for member in runtime.members() {
            let permit = permits.clone().acquire_owned().await.map_err(|_| {
                RuntimeError::ProductPolicy(
                    "balancer active-probe concurrency owner closed".to_string(),
                )
            })?;
            let Some(mut lease) = runtime.begin_active_probe(member)? else {
                continue;
            };
            let result = registry
                .probe_gateway_member(member, &policy.target, policy.timeout)
                .await;
            let feedback = match &result {
                Ok(()) => lease.succeeded(),
                Err(error) => lease.failed(error.to_string()),
            };
            feedback?;
            if let Err(error) = result {
                crate::observability::process_event!(
                    Warn,
                    "balancer",
                    "active_probe_failed",
                    "balancer active probe failed: balancer={} outbound={} error={error}",
                    id.as_str(),
                    member.as_str(),
                );
            }
            drop(permit);
        }
    }
}

impl RuntimeOutboundRegistry {
    async fn probe_gateway_member(
        &self,
        member: &OutboundId,
        target: &ProtocolTarget,
        timeout: Duration,
    ) -> Result<(), RuntimeError> {
        let address = SocketAddr::new(
            target.ip().ok_or_else(|| {
                RuntimeError::ProductPolicy(
                    "validated balancer probe target is not a literal IP".to_string(),
                )
            })?,
            target.port().get(),
        );
        let leaf = self.shell.require_leaf(member, Network::Tcp)?;
        let probe = async {
            match leaf.as_ref() {
                RuntimeOutboundLeaf::Mpp { context, .. } => {
                    let opened =
                        open_remote_stream(context, TargetAddr::Ip(address), TrafficClass::Latency)
                            .await?;
                    opened.close().await;
                    Ok(())
                }
                RuntimeOutboundLeaf::Local {
                    config,
                    native_sockets,
                    ..
                } => {
                    let stream = outbound::connect_tcp_literal_target_with_configurator(
                        config,
                        address,
                        timeout,
                        native_sockets.as_ref(),
                    )
                    .await?;
                    drop(stream);
                    Ok(())
                }
            }
        };
        tokio::time::timeout(timeout, probe).await.map_err(|_| {
            RuntimeError::OutboundConnect(outbound::OutboundConnectError::ConnectTimeout)
        })?
    }
}

impl GatewayRuntimeControl {
    pub(in crate::runtime) fn is_empty(&self) -> bool {
        self.balancers.is_empty()
    }

    pub(in crate::runtime) fn snapshots(
        &self,
    ) -> Result<Vec<NamedGatewayRuntimeSnapshot>, RuntimeError> {
        let mut balancers = self.balancers.iter().collect::<Vec<_>>();
        balancers.sort_unstable_by_key(|(id, _)| *id);
        balancers
            .into_iter()
            .map(|(id, runtime)| {
                Ok(NamedGatewayRuntimeSnapshot {
                    id: id.clone(),
                    runtime: runtime.snapshot()?,
                })
            })
            .collect()
    }

    pub(in crate::runtime) fn set_member_mode(
        &self,
        balancer: &BalancerId,
        member: &OutboundId,
        mode: GatewayMemberMode,
    ) -> Result<(), RuntimeError> {
        self.require_balancer(balancer)?
            .set_member_mode(member, mode)
    }

    pub(in crate::runtime) fn set_manual_member(
        &self,
        balancer: &BalancerId,
        member: Option<&OutboundId>,
    ) -> Result<(), RuntimeError> {
        self.require_balancer(balancer)?.set_manual_member(member)
    }

    fn require_balancer(&self, id: &BalancerId) -> Result<&ClientGatewayRuntime, RuntimeError> {
        self.balancers.get(id).ok_or_else(|| {
            RuntimeError::GatewayUnavailable(format!("balancer {} is not configured", id.as_str()))
        })
    }
}

impl RuntimeOutboundRegistryShell {
    pub(in crate::runtime) fn compile(
        leaves: impl IntoIterator<Item = RuntimeOutboundLeaf>,
        balancer_configs: &[GatewayBalancerConfig],
    ) -> Result<Self, RuntimeError> {
        let mut leaf_map = HashMap::new();
        for leaf in leaves {
            let id = leaf.id().clone();
            if leaf_map.insert(id, Arc::new(leaf)).is_some() {
                return Err(RuntimeError::ProductPolicy(
                    "duplicate runtime outbound leaf".to_string(),
                ));
            }
        }

        let mut balancers = HashMap::new();
        for config in balancer_configs {
            for member in &config.spec.members {
                let Some(leaf) = leaf_map.get(&member.id) else {
                    return Err(RuntimeError::ProductPolicy(format!(
                        "balancer member {} has no runtime outbound",
                        member.id.as_str()
                    )));
                };
                if member.networks != leaf.networks() {
                    return Err(RuntimeError::ProductPolicy(format!(
                        "balancer member {} capability differs from runtime outbound",
                        member.id.as_str()
                    )));
                }
            }
            if balancers
                .insert(config.id.clone(), ClientGatewayRuntime::compile(config)?)
                .is_some()
            {
                return Err(RuntimeError::ProductPolicy(
                    "duplicate runtime balancer".to_string(),
                ));
            }
        }
        Ok(Self {
            leaves: Arc::new(leaf_map),
            balancers: Arc::new(balancers),
            product_admission: ProductAdmission::default(),
            product_telemetry: RuntimeTelemetry::new(
                crate::runtime::telemetry::MAX_ACTIVE_FLOW_DETAIL_RECORDS,
            ),
        })
    }

    pub(in crate::runtime) fn with_product_admission(
        mut self,
        product_admission: ProductAdmission,
    ) -> Self {
        self.product_admission = product_admission;
        self
    }

    pub(in crate::runtime) fn with_product_telemetry(
        mut self,
        product_telemetry: RuntimeTelemetry,
    ) -> Self {
        self.product_telemetry = product_telemetry;
        self
    }

    pub(in crate::runtime) fn product_admission(&self) -> &ProductAdmission {
        &self.product_admission
    }

    pub(in crate::runtime) fn with_dns(self, dns: DnsGeneration) -> RuntimeOutboundRegistry {
        RuntimeOutboundRegistry { shell: self, dns }
    }

    pub(in crate::runtime) fn dns_backend_factory(
        &self,
        native_sockets: Arc<dyn NativeSocketConfigurator>,
    ) -> RuntimeOutboundDnsBackendFactory {
        RuntimeOutboundDnsBackendFactory {
            shell: self.clone(),
            direct_policy: DnsNativeSocketPolicy::direct(native_sockets),
        }
    }

    pub(in crate::runtime) fn selection_for_action(
        &self,
        action: &EgressAction,
    ) -> Result<EgressSelection, RuntimeError> {
        match action {
            EgressAction::Outbound(id) if self.leaves.contains_key(id) => {
                Ok(EgressSelection::Outbound(id.clone()))
            }
            EgressAction::Balancer(id) if self.balancers.contains_key(id) => {
                Ok(EgressSelection::Balancer(id.clone()))
            }
            EgressAction::Outbound(id) => Err(RuntimeError::ProductPolicy(format!(
                "route selected unavailable outbound {}",
                id.as_str()
            ))),
            EgressAction::Balancer(id) => Err(RuntimeError::ProductPolicy(format!(
                "route selected unavailable balancer {}",
                id.as_str()
            ))),
            EgressAction::Direct => Err(RuntimeError::ProductPolicy(
                "direct route must select a configured direct outbound".to_string(),
            )),
            EgressAction::Reject | EgressAction::Drop => Err(RuntimeError::ProductPolicy(
                "terminal route cannot open an outbound".to_string(),
            )),
        }
    }

    pub(in crate::runtime) fn selection_for_egress(
        &self,
        egress: &EgressRef,
    ) -> Result<EgressSelection, RuntimeError> {
        let selection = match egress {
            EgressRef::Outbound(outbound) => EgressSelection::Outbound(outbound.clone()),
            EgressRef::Balancer(balancer) => EgressSelection::Balancer(balancer.clone()),
        };
        let present = match &selection {
            EgressSelection::Outbound(id) => self.leaves.contains_key(id),
            EgressSelection::Balancer(id) => self.balancers.contains_key(id),
        };
        present.then_some(selection).ok_or_else(|| {
            RuntimeError::ProductPolicy(format!("egress {} has no runtime binding", egress.name()))
        })
    }

    /// Proves the server-side no-chaining invariant at runtime assembly.
    ///
    /// Configuration validation performs the same check, but server services
    /// retain this defense so a hand-built runtime cannot forward one MPP
    /// session into another MPP session.
    pub(in crate::runtime) fn ensure_native_egress(
        &self,
        selection: &EgressSelection,
    ) -> Result<(), RuntimeError> {
        let ensure_leaf = |id: &OutboundId| {
            let leaf = self.leaves.get(id).ok_or_else(|| {
                RuntimeError::ProductPolicy(format!("outbound {} is unavailable", id.as_str()))
            })?;
            if leaf.is_local() {
                Ok(())
            } else {
                Err(RuntimeError::ProductPolicy(
                    "MPP inbound egress cannot select an MPP outbound".to_string(),
                ))
            }
        };
        match selection {
            EgressSelection::Outbound(id) => ensure_leaf(id),
            EgressSelection::Balancer(id) => {
                let runtime = self.require_balancer(id)?;
                for member in runtime.members() {
                    ensure_leaf(member)?;
                }
                Ok(())
            }
        }
    }

    fn require_leaf(
        &self,
        id: &OutboundId,
        network: Network,
    ) -> Result<Arc<RuntimeOutboundLeaf>, RuntimeError> {
        let leaf = self.leaves.get(id).cloned().ok_or_else(|| {
            RuntimeError::ProductPolicy(format!("outbound {} is unavailable", id.as_str()))
        })?;
        if !leaf.networks().contains(network) {
            return Err(RuntimeError::GatewayUnavailable(format!(
                "outbound {} does not support {network}",
                id.as_str()
            )));
        }
        Ok(leaf)
    }

    fn require_balancer(&self, id: &BalancerId) -> Result<&ClientGatewayRuntime, RuntimeError> {
        self.balancers.get(id).ok_or_else(|| {
            RuntimeError::ProductPolicy(format!("balancer {} is unavailable", id.as_str()))
        })
    }
}

impl DnsBackendFactory for RuntimeOutboundDnsBackendFactory {
    fn build_backend(
        &self,
        plan: &CompiledDnsPlan,
        upstream: &CompiledDnsUpstream,
    ) -> Result<Arc<dyn DnsQueryBackend>, DnsRuntimeError> {
        let DnsEgressSpec::Outbound(id) = upstream.egress() else {
            return DirectDnsBackendFactory::build_backend_with_policy(
                plan,
                upstream,
                self.direct_policy.clone(),
            );
        };
        let leaf =
            self.shell
                .leaves
                .get(id)
                .ok_or_else(|| DnsRuntimeError::MissingEgressConnector {
                    upstream: upstream.id().clone(),
                    outbound: id.clone(),
                })?;
        let connector: Arc<dyn DnsTcpConnector> = match leaf.as_ref() {
            RuntimeOutboundLeaf::Local {
                config: OutboundConfig::Direct,
                native_sockets,
                ..
            } => {
                return DirectDnsBackendFactory::build_backend_with_policy(
                    plan,
                    upstream,
                    DnsNativeSocketPolicy::direct(native_sockets.clone()),
                );
            }
            RuntimeOutboundLeaf::Local {
                config: OutboundConfig::BindSourceIp(source_ip),
                native_sockets,
                ..
            } => {
                return DirectDnsBackendFactory::build_backend_with_policy(
                    plan,
                    upstream,
                    DnsNativeSocketPolicy::bind_source(native_sockets.clone(), *source_ip),
                );
            }
            RuntimeOutboundLeaf::Local {
                config,
                connect_timeout,
                native_sockets,
                ..
            } => {
                let independent = config
                    .native_proxy_endpoint()
                    .is_some_and(|endpoint| endpoint.host.parse::<IpAddr>().is_ok());
                if !independent {
                    return Err(DnsRuntimeError::RecursiveEgressConnector {
                        upstream: upstream.id().clone(),
                        outbound: id.clone(),
                    });
                }
                Arc::new(LocalOutboundDnsTcpConnector {
                    config: config.clone(),
                    connect_timeout: *connect_timeout,
                    native_sockets: native_sockets.clone(),
                })
            }
            RuntimeOutboundLeaf::Mpp {
                context,
                performance,
                ..
            } => {
                let independent = context
                    .tcp_paths
                    .iter()
                    .chain(context.udp_paths.iter())
                    .all(|path| path.endpoint.host.parse::<IpAddr>().is_ok());
                if !independent {
                    return Err(DnsRuntimeError::RecursiveEgressConnector {
                        upstream: upstream.id().clone(),
                        outbound: id.clone(),
                    });
                }
                Arc::new(MppOutboundDnsTcpConnector {
                    context: context.clone(),
                    performance: *performance,
                })
            }
        };
        match upstream.endpoint() {
            crate::product::DnsUpstreamEndpoint::Tcp { .. }
            | crate::product::DnsUpstreamEndpoint::Tls { .. } => {
                RoutedTcpDnsBackend::compile_with_connector(plan, upstream, connector)
                    .map(|backend| Arc::new(backend) as Arc<dyn DnsQueryBackend>)
            }
            crate::product::DnsUpstreamEndpoint::Https { .. } => {
                DohDnsBackend::compile_with_connector(plan, upstream, connector)
                    .map(|backend| Arc::new(backend) as Arc<dyn DnsQueryBackend>)
            }
            _ => Err(DnsRuntimeError::UnsupportedEgressTransport {
                upstream: upstream.id().clone(),
                outbound: id.clone(),
            }),
        }
    }
}

async fn authorize_product_destination(
    dns: &DnsGeneration,
    dns_plan: Option<&DnsPlanId>,
    authorizer: &dyn DestinationAuthorizer,
    authorization: crate::outbound::DestinationAuthorization,
    deadline: tokio::time::Instant,
) -> Result<ProductDestination, RuntimeError> {
    if authorization.target().ip().is_some() || authorization.requires_post_resolution() {
        let authorized = outbound::resolve_authorization_before(
            dns,
            dns_plan,
            authorizer,
            authorization,
            deadline,
        )
        .await
        .map_err(map_destination_resolution_error)?;
        ProductDestination::resolved(authorized)
    } else {
        authorizer
            .authorize_domain(authorization)
            .map(ProductDestination::Domain)
            .map_err(|error| RuntimeError::DestinationDenied(error.to_string()))
    }
}

fn authorized_flow(authorized: &[AuthorizedTarget]) -> Result<&FlowContext, RuntimeError> {
    let first = authorized.first().ok_or_else(|| {
        RuntimeError::OutboundConnect(outbound::OutboundConnectError::NoAuthorizedAddresses)
    })?;
    let generation = first.acl_generation();
    let flow = first.flow();
    if authorized
        .iter()
        .any(|target| target.acl_generation() != generation || target.flow() != flow)
    {
        return Err(RuntimeError::DestinationDenied(
            "authorized connector targets do not belong to one Product flow".to_string(),
        ));
    }
    Ok(flow)
}

fn ensure_destination_network(
    destination: &ProductDestination,
    network: Network,
) -> Result<(), RuntimeError> {
    if destination.flow()?.network() != network {
        return Err(RuntimeError::DestinationDenied(
            "authorized connector target has the wrong network".to_string(),
        ));
    }
    Ok(())
}

fn ensure_product_open_identity(
    destination: &ProductDestination,
    pending: &PendingProductFlow,
) -> Result<(), RuntimeError> {
    let flow = destination.flow()?;
    if flow.principal() != pending.principal() || flow.target() != pending.target() {
        return Err(RuntimeError::DestinationDenied(
            "Product destination does not belong to the admitted flow".to_string(),
        ));
    }
    Ok(())
}

fn product_flow_scope(
    destination: &ProductDestination,
    origin_kind: ProductFlowOriginKind,
    outbound: &OutboundId,
    balancer: Option<&BalancerId>,
) -> Result<ProductFlowScope, RuntimeError> {
    Ok(ProductFlowScope::from_flow(
        origin_kind,
        destination.flow()?,
        outbound.clone(),
        balancer.cloned(),
    ))
}

fn connector_target(
    destination: &ProductDestination,
) -> Result<outbound::ConnectorTarget<'_>, RuntimeError> {
    match destination {
        ProductDestination::Domain(domain) => Ok(outbound::ConnectorTarget::Domain(domain)),
        ProductDestination::Resolved(resolved) => {
            Ok(outbound::ConnectorTarget::Resolved(resolved.targets()))
        }
    }
}

async fn open_mpp_tcp_destination(
    context: &ClientPathContext,
    destination: &ProductDestination,
    traffic_class: TrafficClass,
    deadline: tokio::time::Instant,
) -> Result<(OpenedRemoteStream, TargetAddr), RuntimeError> {
    ensure_destination_network(destination, Network::Tcp)?;
    match destination {
        ProductDestination::Domain(domain) => {
            let target = outbound::protocol_target_addr(domain.flow().target());
            let remote =
                open_mpp_tcp_target(context, target.clone(), traffic_class, deadline).await?;
            Ok((remote, target))
        }
        ProductDestination::Resolved(resolved) => {
            let mut last_error = None;
            for authorized in resolved.targets() {
                let target = authorized_literal_target(authorized, Network::Tcp)?;
                match open_mpp_tcp_target(context, target.clone(), traffic_class, deadline).await {
                    Ok(remote) => return Ok((remote, target)),
                    Err(error) => last_error = Some(error),
                }
            }
            Err(last_error.unwrap_or_else(|| {
                RuntimeError::OutboundConnect(outbound::OutboundConnectError::NoAuthorizedAddresses)
            }))
        }
    }
}

async fn open_mpp_tcp_target(
    context: &ClientPathContext,
    target: TargetAddr,
    traffic_class: TrafficClass,
    deadline: tokio::time::Instant,
) -> Result<OpenedRemoteStream, RuntimeError> {
    match tokio::time::timeout_at(deadline, open_remote_stream(context, target, traffic_class))
        .await
    {
        Ok(result) => result,
        Err(_) => Err(RuntimeError::OutboundConnect(
            outbound::OutboundConnectError::ConnectTimeout,
        )),
    }
}

fn mpp_udp_target(destination: &ProductDestination) -> Result<TargetAddr, RuntimeError> {
    ensure_destination_network(destination, Network::Udp)?;
    match destination {
        ProductDestination::Domain(domain) => {
            Ok(outbound::protocol_target_addr(domain.flow().target()))
        }
        ProductDestination::Resolved(resolved) => {
            let first = resolved
                .targets()
                .first()
                .expect("validated resolved Product destination has an address");
            authorized_literal_target(first, Network::Udp)
        }
    }
}

fn authorized_literal_target(
    authorized: &AuthorizedTarget,
    network: Network,
) -> Result<TargetAddr, RuntimeError> {
    if authorized.flow().network() != network {
        return Err(RuntimeError::DestinationDenied(
            "authorized connector target has the wrong network".to_string(),
        ));
    }
    Ok(TargetAddr::Ip(SocketAddr::new(
        authorized.address(),
        authorized.flow().target().port().get(),
    )))
}

fn map_destination_resolution_error(error: outbound::OutboundConnectError) -> RuntimeError {
    match error {
        outbound::OutboundConnectError::DestinationAuthorization(error) => {
            RuntimeError::DestinationDenied(error.to_string())
        }
        error => RuntimeError::OutboundConnect(error),
    }
}

fn deadline_after(timeout: Duration) -> Result<tokio::time::Instant, RuntimeError> {
    tokio::time::Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| RuntimeError::ProductPolicy("Product stage deadline overflow".to_string()))
}

#[cfg(test)]
pub(in crate::runtime) fn test_dns_generation() -> DnsGeneration {
    DnsGeneration::from_test_answers(HashMap::from([(
        "localhost".to_string(),
        vec![
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST),
        ],
    )]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ClientSecurityConfig, ResourceLimits, SharedSecret};
    use crate::outbound::{HttpsProxyConfig, ProxyConfig};
    use crate::product::{
        CompiledDnsPolicy, DnsIpStrategy, DnsOutboundCapabilitySpec, DnsPlanId, DnsPlanSpec,
        DnsPolicySpec, DnsUpstreamEndpoint, DnsUpstreamId, DnsUpstreamSpec, GatewayBalancerSpec,
        GatewayMemberSpec, GatewayStrategy, ProductAdmissionConfig, ProductAdmissionRejection,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, UdpSocket};

    fn local_leaf_with_timeout(
        id: &str,
        config: OutboundConfig,
        connect_timeout: Duration,
    ) -> RuntimeOutboundLeaf {
        RuntimeOutboundLeaf::Local {
            id: OutboundId::parse(id).expect("outbound ID"),
            config,
            connect_timeout,
            native_sockets: Arc::new(crate::transport::SystemNativeSocketConfigurator),
        }
    }

    fn local_leaf(id: &str, config: OutboundConfig) -> RuntimeOutboundLeaf {
        local_leaf_with_timeout(id, config, Duration::from_millis(250))
    }

    fn selection(registry: &RuntimeOutboundRegistry, id: &str) -> EgressSelection {
        registry
            .selection_for_egress(&EgressRef::Outbound(
                OutboundId::parse(id).expect("outbound ID"),
            ))
            .expect("outbound selection")
    }

    fn registry_with_product_admission(
        leaves: impl IntoIterator<Item = RuntimeOutboundLeaf>,
        admission: ProductAdmission,
    ) -> RuntimeOutboundRegistry {
        RuntimeOutboundRegistryShell::compile(leaves, &[])
            .expect("outbound shell")
            .with_product_admission(admission)
            .with_dns(test_dns_generation())
    }

    fn one_flow_admission() -> ProductAdmission {
        ProductAdmission::new(ProductAdmissionConfig {
            max_live_flows: 1,
            max_concurrent_work: 1,
            max_live_flows_per_principal: 1,
            max_live_flows_per_outbound: 1,
            max_connects_per_outbound: 1,
            max_live_flows_per_target: 1,
            max_connects_per_target: 1,
            max_dns_work: 1,
        })
        .expect("one-flow Product admission")
    }

    #[tokio::test]
    async fn runtime_product_admission_precedes_target_io_and_recovers_after_close() {
        let first_target = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("first target");
        let first_address = first_target.local_addr().expect("first target address");
        let second_target = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("second target");
        let second_address = second_target.local_addr().expect("second target address");
        let admission = one_flow_admission();
        let registry = registry_with_product_admission(
            [local_leaf("direct", OutboundConfig::Direct)],
            admission.clone(),
        );
        let selection = selection(&registry, "direct");
        let policy = crate::outbound::ServerDestinationPolicy::allow_restricted_for_test()
            .test_principal_policy();

        let first = registry
            .open_tcp(
                &selection,
                &TargetAddr::Ip(first_address),
                None,
                TrafficClass::Latency,
                &policy,
            )
            .await
            .expect("first admitted TCP flow");
        assert_eq!(admission.snapshot().live_flows, 1);
        assert!(matches!(
            registry
                .open_tcp(
                    &selection,
                    &TargetAddr::Ip(second_address),
                    None,
                    TrafficClass::Latency,
                    &policy,
                )
                .await,
            Err(RuntimeError::ProductAdmission(error))
                if error.rejection() == ProductAdmissionRejection::GlobalLiveFlows
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(50), second_target.accept())
                .await
                .is_err(),
            "rejected flow reached target I/O"
        );

        drop(first);
        assert_eq!(admission.snapshot().live_flows, 0);
        let recovered = registry
            .open_tcp(
                &selection,
                &TargetAddr::Ip(second_address),
                None,
                TrafficClass::Latency,
                &policy,
            )
            .await
            .expect("admission recovered after close");
        second_target.accept().await.expect("recovered target I/O");
        drop(recovered);
        let snapshot = admission.snapshot();
        assert_eq!(snapshot.live_flows, 0);
        assert_eq!(snapshot.concurrent_work, 0);
        assert!(snapshot.principals.is_empty());
        assert!(snapshot.outbounds.is_empty());
        assert!(snapshot.targets.is_empty());
    }

    #[tokio::test]
    async fn cancelled_outbound_open_releases_every_product_counter() {
        let proxy = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("SOCKS proxy listener");
        let proxy_address = proxy.local_addr().expect("SOCKS proxy address");
        let admission = one_flow_admission();
        let registry = registry_with_product_admission(
            [local_leaf(
                "proxy",
                OutboundConfig::Socks5(ProxyConfig::new(
                    proxy_address.to_string().parse().expect("proxy endpoint"),
                    None,
                )),
            )],
            admission.clone(),
        );
        let selection = selection(&registry, "proxy");
        let policy = crate::outbound::ServerDestinationPolicy::allow_restricted_for_test()
            .test_principal_policy();
        let opener = tokio::spawn(async move {
            registry
                .open_tcp(
                    &selection,
                    &TargetAddr::Ip("192.0.2.1:443".parse().expect("target")),
                    None,
                    TrafficClass::Latency,
                    &policy,
                )
                .await
        });
        let (_stalled_proxy_stream, _) = proxy.accept().await.expect("proxy open reached");
        let snapshot = admission.snapshot();
        assert_eq!(snapshot.live_flows, 1);
        assert_eq!(snapshot.concurrent_work, 1);
        assert_eq!(snapshot.outbounds[0].connecting, 1);
        assert_eq!(snapshot.targets[0].connecting, 1);

        opener.abort();
        match opener.await {
            Err(error) if error.is_cancelled() => {}
            Err(error) => panic!("open task failed instead of cancelling: {error}"),
            Ok(_) => panic!("open task completed instead of cancelling"),
        }
        let snapshot = admission.snapshot();
        assert_eq!(snapshot.live_flows, 0);
        assert_eq!(snapshot.concurrent_work, 0);
        assert!(snapshot.principals.is_empty());
        assert!(snapshot.outbounds.is_empty());
        assert!(snapshot.targets.is_empty());
    }

    #[tokio::test]
    async fn local_only_registry_opens_concrete_tcp_and_udp_without_mpp_context() {
        let tcp_target = TcpListener::bind("127.0.0.1:0").await.expect("TCP bind");
        let tcp_addr = tcp_target.local_addr().expect("TCP address");
        let tcp_server = tokio::spawn(async move {
            let (mut stream, _) = tcp_target.accept().await.expect("TCP accept");
            let mut payload = [0_u8; 4];
            stream.read_exact(&mut payload).await.expect("TCP read");
            assert_eq!(&payload, b"ping");
            stream.write_all(b"pong").await.expect("TCP write");
        });
        let udp_target = UdpSocket::bind("127.0.0.1:0").await.expect("UDP bind");
        let udp_addr = udp_target.local_addr().expect("UDP address");
        let udp_server = tokio::spawn(async move {
            let mut payload = [0_u8; 4];
            let (length, peer) = udp_target.recv_from(&mut payload).await.expect("UDP read");
            assert_eq!(&payload[..length], b"ping");
            udp_target.send_to(b"pong", peer).await.expect("UDP write");
        });
        let telemetry = RuntimeTelemetry::generation_owner(8);
        let registry = RuntimeOutboundRegistryShell::compile(
            [local_leaf("direct", OutboundConfig::Direct)],
            &[],
        )
        .expect("registry")
        .with_product_telemetry(telemetry.clone())
        .with_dns(test_dns_generation());
        let selection = selection(&registry, "direct");
        let policy = crate::outbound::ServerDestinationPolicy::allow_restricted_for_test()
            .test_principal_policy();

        let OpenedTcpOutbound::Local {
            stream: OutboundTcpStream::Plain(mut tcp),
            ..
        } = registry
            .open_tcp(
                &selection,
                &TargetAddr::Ip(tcp_addr),
                None,
                TrafficClass::Latency,
                &policy,
            )
            .await
            .expect("local TCP")
        else {
            panic!("expected a concrete native TCP stream");
        };
        tcp.write_all(b"ping").await.expect("TCP request");
        let mut response = [0_u8; 4];
        tcp.read_exact(&mut response).await.expect("TCP response");
        assert_eq!(&response, b"pong");

        let OpenedUdpOutbound::Local {
            socket: OutboundUdpSocket::Direct(udp),
            ..
        } = registry
            .open_udp(&selection, &TargetAddr::Ip(udp_addr), None, &policy)
            .await
            .expect("local UDP")
        else {
            panic!("expected a concrete native UDP socket");
        };
        udp.send(b"ping").await.expect("UDP request");
        let length = udp.recv(&mut response).await.expect("UDP response");
        assert_eq!(&response[..length], b"pong");
        assert_eq!(
            telemetry.snapshot().io,
            crate::runtime::telemetry::ProductIoSnapshot::default(),
            "server MPP-to-native connectors must not add a second native observer"
        );
        tcp_server.await.expect("TCP task");
        udp_server.await.expect("UDP task");
    }

    #[tokio::test]
    async fn gateway_blackhole_failover_keeps_domain_resolution_lazy_and_irreversible() {
        let blackhole_proxy = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("blackhole proxy bind");
        let blackhole_proxy_addr = blackhole_proxy.local_addr().expect("proxy address");
        let target = TcpListener::bind("127.0.0.1:0").await.expect("target bind");
        let target_addr = target.local_addr().expect("target address");
        let target_name = "fallback.example";
        let domain_target = TargetAddr::Domain {
            host: target_name.to_string(),
            port: target_addr.port(),
        };
        let expected_proxy_target = domain_target.clone();
        let blackhole_proxy_task = tokio::spawn(async move {
            let (mut stream, _) = blackhole_proxy.accept().await.expect("proxy accept");
            let mut greeting = [0_u8; 3];
            stream
                .read_exact(&mut greeting)
                .await
                .expect("proxy greeting");
            assert_eq!(greeting, crate::outbound::socks5::no_auth_greeting());
            stream.write_all(&[0x05, 0x00]).await.expect("proxy method");
            let expected = crate::outbound::socks5::connect_request(&expected_proxy_target)
                .expect("expected SOCKS5 request");
            let mut request = vec![0_u8; expected.len()];
            stream
                .read_exact(&mut request)
                .await
                .expect("proxy request");
            assert_eq!(request, expected, "proxy must receive the canonical domain");
            let mut remainder = Vec::new();
            stream
                .read_to_end(&mut remainder)
                .await
                .expect("timed-out proxy attempt closes");
        });
        let first = OutboundId::parse("failed-proxy").expect("outbound ID");
        let second = OutboundId::parse("working-direct").expect("outbound ID");
        let balancer_id = BalancerId::parse("native-failover").expect("balancer ID");
        let balancers = [GatewayBalancerConfig {
            id: balancer_id.clone(),
            generation: 1,
            spec: GatewayBalancerSpec::new(
                GatewayStrategy::OrderedFailover,
                vec![
                    GatewayMemberSpec::new(first, 1, NetworkSet::TCP_UDP),
                    GatewayMemberSpec::new(second, 1, NetworkSet::TCP_UDP),
                ],
            ),
        }];
        let dns = DnsGeneration::from_test_answers(HashMap::from([(
            target_name.to_string(),
            vec![target_addr.ip()],
        )]));
        let registry = RuntimeOutboundRegistry::compile(
            [
                local_leaf_with_timeout(
                    "failed-proxy",
                    OutboundConfig::Socks5(ProxyConfig::new(
                        blackhole_proxy_addr
                            .to_string()
                            .parse()
                            .expect("proxy endpoint"),
                        None,
                    )),
                    Duration::from_secs(1),
                ),
                local_leaf_with_timeout(
                    "working-direct",
                    OutboundConfig::Direct,
                    Duration::from_secs(1),
                ),
            ],
            &balancers,
            dns.clone(),
        )
        .expect("registry");
        let policy = crate::outbound::ServerDestinationPolicy::allow_restricted_for_test()
            .test_principal_policy();
        let started = tokio::time::Instant::now();
        let opened = registry
            .open_tcp(
                &EgressSelection::Balancer(balancer_id),
                &domain_target,
                None,
                TrafficClass::Latency,
                &policy,
            )
            .await
            .expect("bounded pre-commit failover");
        assert!(
            started.elapsed() >= Duration::from_millis(900),
            "the blackholed member must retain its configured one-second connect stage"
        );
        let OpenedTcpOutbound::Local {
            _gateway_lease: Some(_),
            _product_flow,
            ..
        } = opened
        else {
            panic!("balancer failover must return the working native member");
        };
        assert_eq!(
            _product_flow.scope().selection.outbound.as_str(),
            "working-direct"
        );
        assert_eq!(
            _product_flow
                .scope()
                .selection
                .balancer
                .as_ref()
                .map(BalancerId::as_str),
            Some("native-failover")
        );
        assert_eq!(
            _product_flow
                .scope()
                .selection
                .member
                .as_ref()
                .map(OutboundId::as_str),
            Some("working-direct")
        );
        blackhole_proxy_task.await.expect("blackhole proxy task");
        target.accept().await.expect("direct target accepted");
        let dns = dns.runtime_snapshot();
        assert!(
            dns.plans[0].queries > 0,
            "failover to an IP-only leaf must request Product DNS evidence"
        );
        assert_eq!(
            dns.plans[0].fresh_cache_hits, 0,
            "the promoted destination must not be resolved a second time"
        );

        let remote_proxy = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("remote-resolution proxy bind");
        let remote_proxy_addr = remote_proxy.local_addr().expect("proxy address");
        let unresolved_target = TargetAddr::Domain {
            host: "remote-resolution.example".to_string(),
            port: 443,
        };
        let expected_domain = unresolved_target.clone();
        let remote_proxy_task = tokio::spawn(async move {
            let (mut stream, _) = remote_proxy.accept().await.expect("proxy accept");
            let mut greeting = [0_u8; 3];
            stream
                .read_exact(&mut greeting)
                .await
                .expect("proxy greeting");
            assert_eq!(greeting, crate::outbound::socks5::no_auth_greeting());
            stream.write_all(&[0x05, 0x00]).await.expect("proxy method");
            let expected = crate::outbound::socks5::connect_request(&expected_domain)
                .expect("expected domain request");
            let mut request = vec![0_u8; expected.len()];
            stream
                .read_exact(&mut request)
                .await
                .expect("proxy request");
            assert_eq!(
                request, expected,
                "DNS failure on an IP-only member must not replace the canonical domain"
            );
            stream
                .write_all(&[0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1, 0, 0])
                .await
                .expect("proxy success");
        });
        let local = OutboundId::parse("dns-required-direct").expect("outbound ID");
        let second_local = OutboundId::parse("second-dns-required-direct").expect("outbound ID");
        let remote = OutboundId::parse("remote-domain-proxy").expect("outbound ID");
        let dns_failover_id = BalancerId::parse("dns-failover").expect("balancer ID");
        let dns_failover_balancers = [GatewayBalancerConfig {
            id: dns_failover_id.clone(),
            generation: 1,
            spec: GatewayBalancerSpec::new(
                GatewayStrategy::OrderedFailover,
                vec![
                    GatewayMemberSpec::new(local, 1, NetworkSet::TCP_UDP),
                    GatewayMemberSpec::new(second_local, 1, NetworkSet::TCP_UDP),
                    GatewayMemberSpec::new(remote, 1, NetworkSet::TCP_UDP),
                ],
            ),
        }];
        let failing_dns = DnsGeneration::from_test_answers(HashMap::new());
        let dns_failover_registry = RuntimeOutboundRegistry::compile(
            [
                local_leaf_with_timeout(
                    "dns-required-direct",
                    OutboundConfig::Direct,
                    Duration::from_secs(1),
                ),
                local_leaf_with_timeout(
                    "second-dns-required-direct",
                    OutboundConfig::Direct,
                    Duration::from_secs(1),
                ),
                local_leaf_with_timeout(
                    "remote-domain-proxy",
                    OutboundConfig::Socks5(ProxyConfig::new(
                        remote_proxy_addr
                            .to_string()
                            .parse()
                            .expect("proxy endpoint"),
                        None,
                    )),
                    Duration::from_secs(1),
                ),
            ],
            &dns_failover_balancers,
            failing_dns.clone(),
        )
        .expect("DNS failover registry");
        let opened = dns_failover_registry
            .open_tcp(
                &EgressSelection::Balancer(dns_failover_id.clone()),
                &unresolved_target,
                None,
                TrafficClass::Latency,
                &policy,
            )
            .await
            .expect("remote-resolution member survives local DNS failure");
        let OpenedTcpOutbound::Local { _product_flow, .. } = opened else {
            panic!("expected the remote-resolution proxy member");
        };
        assert_eq!(
            _product_flow.scope().selection.outbound.as_str(),
            "remote-domain-proxy"
        );
        let snapshots = dns_failover_registry
            .gateway_control()
            .snapshots()
            .expect("balancer snapshot");
        let members = &snapshots[0].runtime.members;
        assert_eq!(members[0].counters.open_attempts, 1);
        assert_eq!(
            members[0].counters.open_failures, 0,
            "shared target DNS failure is not gateway failure evidence"
        );
        assert_eq!(members[1].counters.open_attempts, 1);
        assert_eq!(
            members[1].counters.open_failures, 0,
            "skipping a member after flow-level DNS failure is not gateway failure evidence"
        );
        assert_eq!(members[2].counters.open_successes, 1);
        assert_eq!(
            failing_dns.runtime_snapshot().plans[0].queries,
            2,
            "one dual-family flow lookup must not be repeated for every IP-only member"
        );
        remote_proxy_task.await.expect("remote proxy task");

        let closed_target = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("closed target reservation");
        let closed_target_addr = closed_target.local_addr().expect("closed target address");
        drop(closed_target);
        let accepting_proxy = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("accepting proxy bind");
        let accepting_proxy_addr = accepting_proxy.local_addr().expect("proxy address");
        let promoted_name = "promoted.example";
        let promoted_target = TargetAddr::Domain {
            host: promoted_name.to_string(),
            port: closed_target_addr.port(),
        };
        let expected_literal = TargetAddr::Ip(closed_target_addr);
        let accepting_proxy_task = tokio::spawn(async move {
            let (mut stream, _) = accepting_proxy.accept().await.expect("proxy accept");
            let mut greeting = [0_u8; 3];
            stream
                .read_exact(&mut greeting)
                .await
                .expect("proxy greeting");
            assert_eq!(greeting, crate::outbound::socks5::no_auth_greeting());
            stream.write_all(&[0x05, 0x00]).await.expect("proxy method");
            let expected = crate::outbound::socks5::connect_request(&expected_literal)
                .expect("expected SOCKS5 request");
            let mut request = vec![0_u8; expected.len()];
            stream
                .read_exact(&mut request)
                .await
                .expect("proxy request");
            assert_eq!(
                request, expected,
                "a proxy attempted after an IP-only member must receive the authorized literal"
            );
            stream
                .write_all(&[0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1, 0, 0])
                .await
                .expect("proxy success");
        });
        let failed_direct = OutboundId::parse("failed-direct").expect("outbound ID");
        let working_proxy = OutboundId::parse("working-proxy").expect("outbound ID");
        let promoted_balancer = BalancerId::parse("promoted-failover").expect("balancer ID");
        let promoted_balancers = [GatewayBalancerConfig {
            id: promoted_balancer.clone(),
            generation: 1,
            spec: GatewayBalancerSpec::new(
                GatewayStrategy::OrderedFailover,
                vec![
                    GatewayMemberSpec::new(failed_direct, 1, NetworkSet::TCP_UDP),
                    GatewayMemberSpec::new(working_proxy, 1, NetworkSet::TCP_UDP),
                ],
            ),
        }];
        let promoted_dns = DnsGeneration::from_test_answers(HashMap::from([(
            promoted_name.to_string(),
            vec![closed_target_addr.ip()],
        )]));
        let promoted_registry = RuntimeOutboundRegistry::compile(
            [
                local_leaf("failed-direct", OutboundConfig::Direct),
                local_leaf(
                    "working-proxy",
                    OutboundConfig::Socks5(ProxyConfig::new(
                        accepting_proxy_addr
                            .to_string()
                            .parse()
                            .expect("proxy endpoint"),
                        None,
                    )),
                ),
            ],
            &promoted_balancers,
            promoted_dns.clone(),
        )
        .expect("promoted registry");
        let opened = promoted_registry
            .open_tcp(
                &EgressSelection::Balancer(promoted_balancer),
                &promoted_target,
                None,
                TrafficClass::Latency,
                &policy,
            )
            .await
            .expect("IP-only failure followed by proxy fallback");
        let OpenedTcpOutbound::Local {
            _gateway_lease: Some(_),
            _product_flow,
            ..
        } = opened
        else {
            panic!("balancer failover must return the working proxy member");
        };
        assert_eq!(
            _product_flow.scope().selection.outbound.as_str(),
            "working-proxy"
        );
        accepting_proxy_task.await.expect("accepting proxy task");
        let promoted_dns = promoted_dns.runtime_snapshot();
        assert!(promoted_dns.plans[0].queries > 0);
        assert_eq!(
            promoted_dns.plans[0].fresh_cache_hits, 0,
            "an already promoted destination must never be resolved again or revert to its domain"
        );
    }

    #[test]
    fn server_native_selector_rejects_mpp_chaining_at_runtime_assembly() {
        let context = ClientPathContext::new(
            vec!["udp://127.0.0.1:7443".parse().expect("path")],
            ClientSecurityConfig::for_test(
                SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
            ),
            ResourceLimits::default(),
        )
        .expect("MPP context");
        let id = OutboundId::parse("another-mpp").expect("outbound ID");
        let registry = RuntimeOutboundRegistry::compile(
            [RuntimeOutboundLeaf::Mpp {
                id: id.clone(),
                context,
                performance: MppPerformanceConfig::default(),
            }],
            &[],
            test_dns_generation(),
        )
        .expect("registry");
        assert!(matches!(
            registry.ensure_native_egress(&EgressSelection::Outbound(id)),
            Err(RuntimeError::ProductPolicy(message))
                if message.contains("cannot select an MPP outbound")
        ));
    }

    fn named_dns_policy_for(
        outbound: OutboundId,
        endpoint: DnsUpstreamEndpoint,
        networks: NetworkSet,
    ) -> Arc<CompiledDnsPolicy> {
        let upstream = DnsUpstreamId::parse("named-upstream").expect("upstream ID");
        let plan = DnsPlanId::parse("default").expect("plan ID");
        let mut plan_spec = DnsPlanSpec::new(plan.clone(), vec![upstream.clone()]);
        plan_spec.ip_strategy = DnsIpStrategy::Ipv4Only;
        Arc::new(
            CompiledDnsPolicy::compile(
                1,
                DnsPolicySpec {
                    upstreams: vec![DnsUpstreamSpec {
                        id: upstream.clone(),
                        endpoint,
                        egress: DnsEgressSpec::Outbound(outbound.clone()),
                    }],
                    outbound_capabilities: vec![DnsOutboundCapabilitySpec::new(
                        outbound, networks, true,
                    )],
                    plans: vec![plan_spec],
                    rules: Vec::new(),
                    hosts: Vec::new(),
                    fake_dns: None,
                    default_plan: plan,
                },
            )
            .expect("named DNS policy"),
        )
    }

    fn named_udp_dns_policy(outbound: OutboundId) -> Arc<CompiledDnsPolicy> {
        named_dns_policy_for(
            outbound,
            DnsUpstreamEndpoint::Udp {
                bootstrap: "1.1.1.1:53".parse().expect("bootstrap"),
            },
            NetworkSet::TCP_UDP,
        )
    }

    fn named_tcp_dns_policy(outbound: OutboundId, bootstrap: SocketAddr) -> Arc<CompiledDnsPolicy> {
        named_dns_policy_for(
            outbound,
            DnsUpstreamEndpoint::Tcp { bootstrap },
            NetworkSet::TCP,
        )
    }

    #[test]
    fn named_dns_egress_accepts_only_dns_independent_native_leaves() {
        let bind_id = OutboundId::parse("bound-direct").expect("outbound ID");
        let shell = RuntimeOutboundRegistryShell::compile(
            [local_leaf(
                bind_id.as_str(),
                OutboundConfig::BindSourceIp(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
            )],
            &[],
        )
        .expect("shell");
        let factory =
            shell.dns_backend_factory(Arc::new(crate::transport::SystemNativeSocketConfigurator));
        DnsGeneration::compile_with_factory(named_udp_dns_policy(bind_id), &factory)
            .expect("named bind-source DNS connector");

        let proxy_id = OutboundId::parse("proxy").expect("outbound ID");
        let shell = RuntimeOutboundRegistryShell::compile(
            [local_leaf(
                proxy_id.as_str(),
                OutboundConfig::Socks5(ProxyConfig::new(
                    "127.0.0.1:1080".parse().expect("proxy endpoint"),
                    None,
                )),
            )],
            &[],
        )
        .expect("shell");
        let factory =
            shell.dns_backend_factory(Arc::new(crate::transport::SystemNativeSocketConfigurator));
        assert!(matches!(
            DnsGeneration::compile_with_factory(named_udp_dns_policy(proxy_id.clone()), &factory),
            Err(DnsRuntimeError::UnsupportedEgressTransport { outbound, .. })
                if outbound == proxy_id
        ));
    }

    #[test]
    fn routed_tcp_dot_and_doh_compile_for_literal_proxy_control_endpoints() {
        let configs = [
            (
                "socks",
                OutboundConfig::Socks5(ProxyConfig::new(
                    "127.0.0.1:1080".parse().expect("SOCKS endpoint"),
                    None,
                )),
                DnsUpstreamEndpoint::Tcp {
                    bootstrap: "192.0.2.53:53".parse().expect("TCP bootstrap"),
                },
            ),
            (
                "http",
                OutboundConfig::HttpConnect(ProxyConfig::new(
                    "127.0.0.1:8080".parse().expect("HTTP endpoint"),
                    None,
                )),
                DnsUpstreamEndpoint::Tls {
                    bootstrap: "192.0.2.53:853".parse().expect("DoT bootstrap"),
                    server_name: crate::product::DomainName::parse("resolver.example")
                        .expect("DoT identity"),
                },
            ),
            (
                "https",
                OutboundConfig::HttpsConnect(Box::new(
                    HttpsProxyConfig::new(
                        ProxyConfig::new("127.0.0.1:8443".parse().expect("HTTPS endpoint"), None),
                        Some("proxy.example".to_string()),
                        Vec::new(),
                    )
                    .expect("HTTPS proxy"),
                )),
                DnsUpstreamEndpoint::Https {
                    bootstrap: "192.0.2.53:443".parse().expect("DoH bootstrap"),
                    server_name: crate::product::DomainName::parse("resolver.example")
                        .expect("DoH identity"),
                    path: "/dns-query".to_string(),
                },
            ),
        ];
        for (tag, config, endpoint) in configs {
            let id = OutboundId::parse(tag).expect("outbound ID");
            let shell =
                RuntimeOutboundRegistryShell::compile([local_leaf(id.as_str(), config)], &[])
                    .expect("shell");
            let factory = shell
                .dns_backend_factory(Arc::new(crate::transport::SystemNativeSocketConfigurator));
            DnsGeneration::compile_with_factory(
                named_dns_policy_for(id, endpoint, NetworkSet::TCP),
                &factory,
            )
            .unwrap_or_else(|error| panic!("{tag} routed DNS did not compile: {error}"));
        }
    }

    #[test]
    fn routed_dns_rejects_proxy_and_mpp_control_hostnames_at_runtime_assembly() {
        let proxy_id = OutboundId::parse("named-proxy").expect("outbound ID");
        let shell = RuntimeOutboundRegistryShell::compile(
            [local_leaf(
                proxy_id.as_str(),
                OutboundConfig::Socks5(ProxyConfig::new(
                    "proxy.example:1080".parse().expect("proxy endpoint"),
                    None,
                )),
            )],
            &[],
        )
        .expect("shell");
        let factory =
            shell.dns_backend_factory(Arc::new(crate::transport::SystemNativeSocketConfigurator));
        assert!(matches!(
            DnsGeneration::compile_with_factory(
                named_tcp_dns_policy(
                    proxy_id.clone(),
                    "192.0.2.53:53".parse().expect("bootstrap")
                ),
                &factory,
            ),
            Err(DnsRuntimeError::RecursiveEgressConnector { outbound, .. })
                if outbound == proxy_id
        ));

        let mpp_id = OutboundId::parse("named-mpp").expect("outbound ID");
        let context = ClientPathContext::new(
            vec![
                "udp://carrier.example:7443"
                    .parse()
                    .expect("MPP path endpoint"),
            ],
            ClientSecurityConfig::for_test(
                SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
            ),
            ResourceLimits::default(),
        )
        .expect("MPP context");
        let shell = RuntimeOutboundRegistryShell::compile(
            [RuntimeOutboundLeaf::Mpp {
                id: mpp_id.clone(),
                context,
                performance: MppPerformanceConfig::default(),
            }],
            &[],
        )
        .expect("shell");
        let factory =
            shell.dns_backend_factory(Arc::new(crate::transport::SystemNativeSocketConfigurator));
        assert!(matches!(
            DnsGeneration::compile_with_factory(
                named_tcp_dns_policy(
                    mpp_id.clone(),
                    "192.0.2.53:53".parse().expect("bootstrap")
                ),
                &factory,
            ),
            Err(DnsRuntimeError::RecursiveEgressConnector { outbound, .. })
                if outbound == mpp_id
        ));

        let literal_id = OutboundId::parse("literal-mpp").expect("outbound ID");
        let context = ClientPathContext::new(
            vec![
                "udp://127.0.0.1:7443"
                    .parse()
                    .expect("literal MPP path endpoint"),
            ],
            ClientSecurityConfig::for_test(
                SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
            ),
            ResourceLimits::default(),
        )
        .expect("MPP context");
        let shell = RuntimeOutboundRegistryShell::compile(
            [RuntimeOutboundLeaf::Mpp {
                id: literal_id.clone(),
                context,
                performance: MppPerformanceConfig::default(),
            }],
            &[],
        )
        .expect("shell");
        let factory =
            shell.dns_backend_factory(Arc::new(crate::transport::SystemNativeSocketConfigurator));
        DnsGeneration::compile_with_factory(
            named_tcp_dns_policy(literal_id, "192.0.2.53:53".parse().expect("bootstrap")),
            &factory,
        )
        .expect("literal MPP DNS connector");
    }

    #[tokio::test]
    async fn routed_dns_query_traverses_the_selected_socks_connector() {
        let proxy = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("SOCKS listener");
        let proxy_address = proxy.local_addr().expect("SOCKS address");
        let bootstrap: SocketAddr = "192.0.2.53:53".parse().expect("DNS bootstrap");
        let answer: std::net::Ipv4Addr = "203.0.113.9".parse().expect("DNS answer");
        let proxy_task = tokio::spawn(async move {
            let (mut stream, _) = proxy.accept().await.expect("SOCKS accept");
            let mut greeting = [0_u8; 3];
            stream
                .read_exact(&mut greeting)
                .await
                .expect("SOCKS greeting");
            assert_eq!(greeting, [0x05, 0x01, 0x00]);
            stream.write_all(&[0x05, 0x00]).await.expect("SOCKS method");

            let mut connect = [0_u8; 10];
            stream
                .read_exact(&mut connect)
                .await
                .expect("SOCKS CONNECT");
            assert_eq!(&connect[..4], &[0x05, 0x01, 0x00, 0x01]);
            assert_eq!(
                &connect[4..8],
                &bootstrap
                    .ip()
                    .to_string()
                    .parse::<std::net::Ipv4Addr>()
                    .expect("IPv4")
                    .octets()
            );
            assert_eq!(
                u16::from_be_bytes([connect[8], connect[9]]),
                bootstrap.port()
            );
            stream
                .write_all(&[0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1, 0, 0])
                .await
                .expect("SOCKS success");

            let mut length = [0_u8; 2];
            stream
                .read_exact(&mut length)
                .await
                .expect("DNS frame length");
            let mut request_wire = vec![0_u8; usize::from(u16::from_be_bytes(length))];
            stream
                .read_exact(&mut request_wire)
                .await
                .expect("DNS request");
            let request = hickory_proto::op::Message::from_vec(&request_wire).expect("DNS message");
            let query = request.queries[0].clone();
            let mut response = hickory_proto::op::Message::response(
                request.metadata.id,
                hickory_proto::op::OpCode::Query,
            );
            response.add_query(query.clone());
            response.add_answer(hickory_proto::rr::Record::from_rdata(
                query.name().clone(),
                60,
                hickory_proto::rr::RData::A(hickory_proto::rr::rdata::A(answer)),
            ));
            let response_wire = response.to_vec().expect("DNS response");
            stream
                .write_all(
                    &u16::try_from(response_wire.len())
                        .expect("DNS response length")
                        .to_be_bytes(),
                )
                .await
                .expect("DNS response length");
            stream
                .write_all(&response_wire)
                .await
                .expect("DNS response");
        });

        let id = OutboundId::parse("socks-dns").expect("outbound ID");
        let shell = RuntimeOutboundRegistryShell::compile(
            [local_leaf(
                id.as_str(),
                OutboundConfig::Socks5(ProxyConfig::new(
                    proxy_address.to_string().parse().expect("proxy endpoint"),
                    None,
                )),
            )],
            &[],
        )
        .expect("shell");
        let factory =
            shell.dns_backend_factory(Arc::new(crate::transport::SystemNativeSocketConfigurator));
        let dns =
            DnsGeneration::compile_with_factory(named_tcp_dns_policy(id, bootstrap), &factory)
                .expect("routed DNS generation");
        let resolution = dns
            .resolve(&crate::product::DomainName::parse("through-proxy.example").expect("domain"))
            .await
            .expect("routed DNS answer");
        assert_eq!(resolution.addresses().as_ref(), &[IpAddr::V4(answer)]);
        proxy_task.await.expect("SOCKS task");
    }
}
