//! Unified Product outbound registry.
//!
//! Selection, pre-commit balancer failover, and connector opening happen once
//! per Product flow. The returned concrete branch is then pinned for the flow
//! lifetime and no routing or balancer decision enters payload forwarding.

use crate::config::{DEFAULT_OUTBOUND_CONNECT_TIMEOUT, GatewayBalancerConfig};
use crate::dns::{
    DirectDnsBackendFactory, DnsBackendError, DnsBackendFactory, DnsGeneration,
    DnsNativeSocketPolicy, DnsQueryBackend, DnsRuntimeError, DnsTcpConnectFuture, DnsTcpConnector,
    DnsTcpStream, DohDnsBackend, RoutedTcpDnsBackend,
};
use crate::outbound::{
    self, DestinationAuthorizationError, DestinationAuthorizer, OutboundConfig, OutboundTcpStream,
    OutboundUdpSocket,
};
use crate::performance::MppPerformanceConfig;
use crate::product::{
    AuthorizedDomainTarget, AuthorizedTarget, BalancerId, CompiledDnsPlan, CompiledDnsUpstream,
    DnsEgressSpec, DnsPlanId, EgressAction, FlowContext, GatewayMemberHandle, GatewayMemberMode,
    Network, NetworkSet, OutboundId, PendingProductFlow, ProductAdmission,
    ProductFlowLease as ProductAdmissionLease, ProductOutboundFlow, ProtocolTarget,
};
use crate::protocol::TargetAddr;
use crate::runtime::error::RuntimeError;
use crate::runtime::gateway::{ClientGatewayRuntime, GatewayFlowLease, GatewayRuntimeSnapshot};
use crate::runtime::path::AuthenticatedCarrierAvailability;
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
                        ReliableRelayOpenSpec::new(target, TrafficClass::Latency),
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

    fn supports_ip_family(&self, address: IpAddr) -> bool {
        match self {
            Self::Mpp { .. } => true,
            Self::Local { config, .. } => config.supports_ip_family(address),
        }
    }

    fn supports_destination_family(&self, destination: &ProductDestination) -> bool {
        match (self, destination) {
            (Self::Mpp { .. }, _) | (_, ProductDestination::Domain(_)) => true,
            (Self::Local { config, .. }, ProductDestination::Resolved(resolved)) => resolved
                .targets()
                .iter()
                .any(|target| config.supports_ip_family(target.address())),
        }
    }

    fn destination_family_error(&self) -> RuntimeError {
        RuntimeError::GatewayUnavailable(format!(
            "outbound {} has no source binding for the destination IP family",
            self.id().as_str()
        ))
    }

    const fn open_timeout(&self) -> Duration {
        match self {
            Self::Mpp { .. } => DEFAULT_OUTBOUND_CONNECT_TIMEOUT,
            Self::Local {
                connect_timeout, ..
            } => *connect_timeout,
        }
    }

    fn ensure_new_product_flow_available(&self) -> Result<(), RuntimeError> {
        match self {
            Self::Mpp { id, context, .. }
                if context.authenticated_carriers.snapshot().availability()
                    == AuthenticatedCarrierAvailability::Offline =>
            {
                Err(RuntimeError::OutboundUnavailable(id.clone()))
            }
            Self::Mpp { .. } | Self::Local { .. } => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime) enum EgressSelection {
    Outbound(OutboundId),
    Balancer(BalancerId),
}

fn product_network_label(network: Network) -> &'static str {
    match network {
        Network::Tcp => "tcp",
        Network::Udp => "udp",
    }
}

fn emit_balancer_selection(
    connection_id: Option<crate::observability::DebugConnectionId>,
    network: Network,
    balancer: &BalancerId,
    outbound: &OutboundId,
    attempt: usize,
) {
    crate::observability::emit_balancer_debug(
        connection_id,
        product_network_label(network),
        balancer.as_str(),
        outbound.as_str(),
        attempt,
    );
}

struct OutboundDebugAttempt<'a> {
    connection_id: Option<crate::observability::DebugConnectionId>,
    network: Network,
    outbound: &'a OutboundId,
    destination: &'a str,
    attempt: usize,
}

impl<'a> OutboundDebugAttempt<'a> {
    fn begin(
        connection_id: Option<crate::observability::DebugConnectionId>,
        network: Network,
        outbound: &'a OutboundId,
        destination: &'a str,
        attempt: usize,
    ) -> Self {
        let state = Self {
            connection_id,
            network,
            outbound,
            destination,
            attempt,
        };
        state.emit(crate::observability::OutboundDebugEvent::Connecting, None);
        state
    }

    fn result<T>(&self, result: Result<T, RuntimeError>) -> Result<T, RuntimeError> {
        result.inspect_err(|error| self.failed(error))
    }

    fn connected(&self) {
        self.emit(crate::observability::OutboundDebugEvent::Connected, None);
    }

    fn failed(&self, error: &RuntimeError) {
        if self.connection_id.is_none()
            || !crate::observability::enabled(crate::config::LogLevel::Debug)
        {
            return;
        }
        let error = error.to_string();
        self.emit(
            crate::observability::OutboundDebugEvent::Failed,
            Some(&error),
        );
    }

    fn emit(&self, event: crate::observability::OutboundDebugEvent, error: Option<&str>) {
        crate::observability::emit_outbound_debug(
            self.connection_id,
            event,
            product_network_label(self.network),
            self.outbound.as_str(),
            self.destination,
            self.attempt,
            error,
        );
    }
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
    pub(in crate::runtime) debug_connection_id: Option<crate::observability::DebugConnectionId>,
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

    pub(in crate::runtime) fn action_requires_family_resolution(
        &self,
        action: &EgressAction,
    ) -> Result<bool, RuntimeError> {
        let ipv4 = IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED);
        let ipv6 = IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED);
        match action {
            EgressAction::Outbound(id) => {
                let leaf = self.shell.leaves.get(id).ok_or_else(|| {
                    RuntimeError::ProductPolicy(format!(
                        "route selected unavailable outbound {}",
                        id.as_str()
                    ))
                })?;
                Ok(!leaf.supports_ip_family(ipv4) || !leaf.supports_ip_family(ipv6))
            }
            EgressAction::Balancer(id) => {
                let runtime = self.shell.require_balancer(id)?;
                for member in runtime.members() {
                    let leaf = self.shell.leaves.get(member).ok_or_else(|| {
                        RuntimeError::ProductPolicy(format!(
                            "balancer member {} has no runtime outbound",
                            member.as_str()
                        ))
                    })?;
                    if !leaf.supports_ip_family(ipv4) || !leaf.supports_ip_family(ipv6) {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            EgressAction::Direct => Ok(false),
        }
    }

    pub(in crate::runtime) fn action_supports_ip_family(
        &self,
        action: &EgressAction,
        address: IpAddr,
    ) -> bool {
        match action {
            EgressAction::Outbound(id) => self
                .shell
                .leaves
                .get(id)
                .is_some_and(|leaf| leaf.supports_ip_family(address)),
            EgressAction::Balancer(id) => self.shell.balancers.get(id).is_some_and(|runtime| {
                runtime.members().iter().any(|member| {
                    self.shell
                        .leaves
                        .get(member)
                        .is_some_and(|leaf| leaf.supports_ip_family(address))
                })
            }),
            EgressAction::Direct => true,
        }
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

    pub(in crate::runtime) fn action_is_native_egress(&self, action: &EgressAction) -> bool {
        self.selection_for_action(action)
            .and_then(|selection| self.ensure_native_egress(&selection))
            .is_ok()
    }

    #[cfg(test)]
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
                    debug_connection_id: None,
                },
                ProductFlowOriginKind::MppInbound,
                false,
            )
            .await?;
        Ok(opened.with_product_flow(pending.commit(outbound)))
    }

    #[cfg(test)]
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
                    debug_connection_id: None,
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

    pub(in crate::runtime) async fn open_product_tcp_from_mpp(
        &self,
        request: ProductOpenRequest<'_>,
    ) -> Result<(OpenedTcpOutbound, ProductOutboundFlow), RuntimeError> {
        self.ensure_native_egress(request.selection)?;
        self.open_product_tcp_for_origin(request, ProductFlowOriginKind::MppInbound, false)
            .await
    }

    fn exclude_ineligible_balancer_members(
        &self,
        runtime: &ClientGatewayRuntime,
        destination: &ProductDestination,
        excluded: &mut Vec<GatewayMemberHandle>,
    ) -> Result<(), RuntimeError> {
        if matches!(destination, ProductDestination::Domain(_)) {
            return Ok(());
        }
        for member in runtime.members() {
            let leaf = self.shell.leaves.get(member).ok_or_else(|| {
                RuntimeError::ProductPolicy(format!(
                    "balancer member {} has no runtime outbound",
                    member.as_str()
                ))
            })?;
            if !leaf.supports_destination_family(destination) {
                let handle = runtime.member_handle(member)?;
                if !excluded.contains(&handle) {
                    excluded.push(handle);
                }
            }
        }
        Ok(())
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
            debug_connection_id,
        } = request;
        ensure_destination_network(&destination, Network::Tcp)?;
        ensure_product_open_identity(&destination, pending)?;
        let protocol_target = pending.target();
        let principal = pending.principal();
        let debug_destination = debug_connection_id
            .map(|_| protocol_target.authority())
            .unwrap_or_default();
        match selection {
            EgressSelection::Outbound(id) => {
                let leaf = self.shell.require_leaf(id, Network::Tcp)?;
                if leaf.requires_ip_target() {
                    self.resolve_destination(
                        &mut destination,
                        dns_plan,
                        authorizer,
                        debug_connection_id,
                    )
                    .await?;
                }
                let attempt = OutboundDebugAttempt::begin(
                    debug_connection_id,
                    Network::Tcp,
                    id,
                    &debug_destination,
                    1,
                );
                attempt.result(leaf.ensure_new_product_flow_available())?;
                if !leaf.supports_destination_family(&destination) {
                    return attempt.result(Err(leaf.destination_family_error()));
                }
                let scope = attempt.result(product_flow_scope(
                    &destination,
                    origin_kind,
                    leaf.id(),
                    None,
                ))?;
                let connect = attempt.result(
                    pending
                        .try_begin_connect(leaf.id().clone())
                        .map_err(RuntimeError::ProductAdmission),
                )?;
                let opened = attempt.result(
                    self.open_product_tcp_leaf(
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
                    .await,
                )?;
                attempt.connected();
                Ok((opened, connect.connected()))
            }
            EgressSelection::Balancer(id) => {
                let runtime = self.shell.require_balancer(id)?;
                let attempt_limit = runtime.member_count();
                let mut excluded = Vec::with_capacity(attempt_limit);
                let mut last_error = None;
                let mut resolution_unavailable = false;
                self.exclude_ineligible_balancer_members(runtime, &destination, &mut excluded)?;
                for attempt_number in 1..=attempt_limit {
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
                    let member = runtime.member_id(handle)?;
                    emit_balancer_selection(
                        debug_connection_id,
                        Network::Tcp,
                        id,
                        member,
                        attempt_number,
                    );
                    let leaf = self.shell.require_leaf(member, Network::Tcp)?;
                    if leaf.requires_ip_target() {
                        if resolution_unavailable {
                            excluded.push(handle);
                            continue;
                        }
                        if let Err(error) = self
                            .resolve_destination(
                                &mut destination,
                                dns_plan,
                                authorizer,
                                debug_connection_id,
                            )
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
                    let attempt = OutboundDebugAttempt::begin(
                        debug_connection_id,
                        Network::Tcp,
                        member,
                        &debug_destination,
                        attempt_number,
                    );
                    if let Err(error) = attempt.result(leaf.ensure_new_product_flow_available()) {
                        excluded.push(handle);
                        if last_error.is_none() {
                            last_error = Some(error);
                        }
                        continue;
                    }
                    if !leaf.supports_destination_family(&destination) {
                        let error = leaf.destination_family_error();
                        attempt.failed(&error);
                        excluded.push(handle);
                        last_error.get_or_insert(error);
                        self.exclude_ineligible_balancer_members(
                            runtime,
                            &destination,
                            &mut excluded,
                        )?;
                        continue;
                    }
                    let scope = attempt.result(product_flow_scope(
                        &destination,
                        origin_kind,
                        leaf.id(),
                        Some(id),
                    ))?;
                    let connect = match attempt.result(
                        pending
                            .try_begin_connect(leaf.id().clone())
                            .map_err(RuntimeError::ProductAdmission),
                    ) {
                        Ok(connect) => connect,
                        Err(error) => {
                            excluded.push(handle);
                            last_error = Some(error);
                            continue;
                        }
                    };
                    match attempt.result(
                        self.open_product_tcp_leaf(
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
                        .await,
                    ) {
                        Ok(opened) => {
                            attempt.connected();
                            return Ok((opened, connect.connected()));
                        }
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

    pub(in crate::runtime) async fn open_product_udp_from_mpp(
        &self,
        request: ProductOpenRequest<'_>,
    ) -> Result<(OpenedUdpOutbound, ProductOutboundFlow), RuntimeError> {
        self.ensure_native_egress(request.selection)?;
        self.open_product_udp_for_origin(request, ProductFlowOriginKind::MppInbound, false)
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
            debug_connection_id,
        } = request;
        ensure_destination_network(&destination, Network::Udp)?;
        ensure_product_open_identity(&destination, pending)?;
        let protocol_target = pending.target();
        let principal = pending.principal();
        let debug_destination = debug_connection_id
            .map(|_| protocol_target.authority())
            .unwrap_or_default();
        match selection {
            EgressSelection::Outbound(id) => {
                let leaf = self.shell.require_leaf(id, Network::Udp)?;
                if leaf.requires_ip_target() {
                    self.resolve_destination(
                        &mut destination,
                        dns_plan,
                        authorizer,
                        debug_connection_id,
                    )
                    .await?;
                }
                let attempt = OutboundDebugAttempt::begin(
                    debug_connection_id,
                    Network::Udp,
                    id,
                    &debug_destination,
                    1,
                );
                attempt.result(leaf.ensure_new_product_flow_available())?;
                if !leaf.supports_destination_family(&destination) {
                    return attempt.result(Err(leaf.destination_family_error()));
                }
                let scope = attempt.result(product_flow_scope(
                    &destination,
                    origin_kind,
                    leaf.id(),
                    None,
                ))?;
                let connect = attempt.result(
                    pending
                        .try_begin_connect(leaf.id().clone())
                        .map_err(RuntimeError::ProductAdmission),
                )?;
                let opened = attempt.result(
                    self.open_product_udp_leaf(
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
                    .await,
                )?;
                attempt.connected();
                Ok((opened, connect.connected()))
            }
            EgressSelection::Balancer(id) => {
                let runtime = self.shell.require_balancer(id)?;
                let attempt_limit = runtime.member_count();
                let mut excluded = Vec::with_capacity(attempt_limit);
                let mut last_error = None;
                let mut resolution_unavailable = false;
                self.exclude_ineligible_balancer_members(runtime, &destination, &mut excluded)?;
                for attempt_number in 1..=attempt_limit {
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
                    let member = runtime.member_id(handle)?;
                    emit_balancer_selection(
                        debug_connection_id,
                        Network::Udp,
                        id,
                        member,
                        attempt_number,
                    );
                    let leaf = self.shell.require_leaf(member, Network::Udp)?;
                    if leaf.requires_ip_target() {
                        if resolution_unavailable {
                            excluded.push(handle);
                            continue;
                        }
                        if let Err(error) = self
                            .resolve_destination(
                                &mut destination,
                                dns_plan,
                                authorizer,
                                debug_connection_id,
                            )
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
                    let attempt = OutboundDebugAttempt::begin(
                        debug_connection_id,
                        Network::Udp,
                        member,
                        &debug_destination,
                        attempt_number,
                    );
                    if let Err(error) = attempt.result(leaf.ensure_new_product_flow_available()) {
                        excluded.push(handle);
                        if last_error.is_none() {
                            last_error = Some(error);
                        }
                        continue;
                    }
                    if !leaf.supports_destination_family(&destination) {
                        let error = leaf.destination_family_error();
                        attempt.failed(&error);
                        excluded.push(handle);
                        last_error.get_or_insert(error);
                        self.exclude_ineligible_balancer_members(
                            runtime,
                            &destination,
                            &mut excluded,
                        )?;
                        continue;
                    }
                    let scope = attempt.result(product_flow_scope(
                        &destination,
                        origin_kind,
                        leaf.id(),
                        Some(id),
                    ))?;
                    let connect = match attempt.result(
                        pending
                            .try_begin_connect(leaf.id().clone())
                            .map_err(RuntimeError::ProductAdmission),
                    ) {
                        Ok(connect) => connect,
                        Err(error) => {
                            excluded.push(handle);
                            last_error = Some(error);
                            continue;
                        }
                    };
                    match attempt.result(
                        self.open_product_udp_leaf(
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
                        .await,
                    ) {
                        Ok(opened) => {
                            attempt.connected();
                            return Ok((opened, connect.connected()));
                        }
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
                        spec: ReliableRelayOpenSpec::new(target, traffic_class),
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
        debug_connection_id: Option<crate::observability::DebugConnectionId>,
    ) -> Result<&'a [AuthorizedTarget], RuntimeError> {
        if let ProductDestination::Domain(domain) = destination {
            let deadline =
                self.destination_resolution_deadline(dns_plan, domain.flow().target())?;
            let authorized = match outbound::resolve_authorized_domain_before(
                &self.dns, dns_plan, authorizer, domain, deadline,
            )
            .await
            {
                Ok(authorized) => authorized,
                Err(error) => {
                    if let outbound::OutboundConnectError::DestinationAuthorization(
                        DestinationAuthorizationError::Policy(error),
                    ) = &error
                    {
                        crate::runtime::product_policy::emit_product_routing_terminal_debug(
                            debug_connection_id,
                            domain.flow(),
                            error,
                        );
                    }
                    return Err(map_destination_resolution_error(error));
                }
            };
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
            if !registry.gateway_member_supports_probe_target(member, &policy.target)? {
                continue;
            }
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
    fn gateway_member_supports_probe_target(
        &self,
        member: &OutboundId,
        target: &ProtocolTarget,
    ) -> Result<bool, RuntimeError> {
        let address = target.ip().ok_or_else(|| {
            RuntimeError::ProductPolicy(
                "validated balancer probe target is not a literal IP".to_string(),
            )
        })?;
        Ok(self
            .shell
            .require_leaf(member, Network::Tcp)?
            .supports_ip_family(address))
    }

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
        }
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
                config: OutboundConfig::BindSourceIps { ipv4, ipv6 },
                native_sockets,
                ..
            } => {
                return DirectDnsBackendFactory::build_backend_with_policy(
                    plan,
                    upstream,
                    DnsNativeSocketPolicy::bind_sources(native_sockets.clone(), *ipv4, *ipv6),
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

#[cfg(test)]
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
    let generation = first.policy_generation();
    let permit = first.permit();
    let flow = first.flow();
    if authorized.iter().any(|target| {
        target.policy_generation() != generation
            || target.flow() != flow
            || target.permit() != permit
    }) {
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
        .ok_or_else(|| RuntimeError::ProductPolicy("outbound stage deadline overflow".to_string()))
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
#[path = "tests_outbound_registry.rs"]
mod tests;
