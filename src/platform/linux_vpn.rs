//! Linux managed-VPN activation outside the packet/data path.
//!
//! This module owns the generation boundary only. Product routing, DNS, and
//! outbound selection remain independent and hand their immutable endpoint
//! inventory to this lifecycle before host policy is published.

use super::managed_vpn::{
    MANAGED_VPN_RESOLUTION_TIMEOUT, MAX_NATIVE_ENDPOINTS, MAX_PREPARED_CARRIER_PATHS,
    ManagedVpnGenerationSpec, ManagedVpnGenerationSpecError, compile_managed_vpn_generation_spec,
};
use super::packet_device::{ManagedPacketDeviceProvider, PacketDeviceProvider};
use crate::config::{AppConfig, CommandConfig, NodeConfig};
use crate::dns::{DnsGeneration, DnsRuntimeError};
use crate::ingress::tun::ManagedVpnCompileError;
use crate::platform::{
    LinuxBackendError, LinuxCleanupError, LinuxControllerState, LinuxHostMutationBackend,
    LinuxInterfaceName, LinuxInterfaceNameError, LinuxPrepareError, LinuxPreparedDeviceError,
    LinuxPublishError, LinuxVpnConfig, LinuxVpnPlan, LinuxVpnPlanError, SystemCommandRunner,
    SystemLinuxHostNetworkBackend, SystemTunDeviceFactory, TransactionalLinuxVpnController,
    snapshot_linux_environment,
};
use crate::product::{CompiledDnsPolicy, DnsActivation, DnsCompileError, DomainName};
use crate::transport::{
    CarrierNetworkProvider, CarrierPathIdentity, Endpoint, LinuxMarkedCarrierNetworkProvider,
    LinuxMarkedNativeSocketConfigurator, NativeSocketConfigurator, PathSpec,
    PreparedCarrierNetworkProvider, PreparedCarrierPath,
};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use std::fmt;
use std::io;
use std::net::IpAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

/// One bounded pre-publication deadline shared by carrier and native-proxy
/// resolution. Runtime DNS-plan timeouts govern product lookups, not the native
/// bootstrap needed to make a managed VPN generation reachable.
pub const LINUX_VPN_RESOLUTION_TIMEOUT: Duration = MANAGED_VPN_RESOLUTION_TIMEOUT;

/// One MPP carrier whose name must be resolved before VPN publication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinuxVpnCarrierPath {
    pub identity: CarrierPathIdentity,
    pub path: PathSpec,
}

/// Immutable process-owned endpoints needed by one runtime generation.
#[derive(Debug, Clone)]
pub struct LinuxVpnPrepareRequest {
    pub config: LinuxVpnConfig,
    /// Counted from the validated node ingress inventory. Exactly one device
    /// may participate in one host transaction.
    pub managed_tun_count: usize,
    pub carrier_paths: Vec<LinuxVpnCarrierPath>,
    /// SOCKS/HTTP proxy control endpoints. Flow-scoped direct targets are
    /// protected by SO_MARK and are intentionally not pre-enumerated.
    pub native_proxy_endpoints: Vec<Endpoint>,
    /// Canonical endpoint names that genuinely require DNS before protected
    /// routes are published. An empty set means preparation is literal-only.
    pub prepublication_domains: Vec<DomainName>,
    pub dns_policy: Arc<CompiledDnsPolicy>,
    pub dns_activation: DnsActivation,
    pub resolution_timeout: Duration,
}

/// Compiles the managed-VPN portion of one validated process generation.
///
/// This is intentionally synchronous and host-independent: it performs no
/// resolution, socket creation, TUN creation, route lookup, or host mutation.
pub fn compile_linux_vpn_prepare_request(
    config: &AppConfig,
) -> Result<Option<LinuxVpnPrepareRequest>, LinuxVpnGenerationSpecError> {
    let CommandConfig::Node(node) = &config.command;
    compile_node_linux_vpn_prepare_request(node)
}

/// Compiles one node's complete native-bypass inventory.
///
/// Carrier group ordinals exactly mirror `runtime::node::combined`: only MPP
/// outbound leaves advance the group, and path ordinals are positions in the
/// original unfiltered path list. MPP server bind paths are local listeners,
/// not native egress endpoints, and therefore are not bypass inventory.
pub fn compile_node_linux_vpn_prepare_request(
    node: &NodeConfig,
) -> Result<Option<LinuxVpnPrepareRequest>, LinuxVpnGenerationSpecError> {
    let Some(spec) =
        compile_managed_vpn_generation_spec(node).map_err(LinuxVpnGenerationSpecError::from)?
    else {
        return Ok(None);
    };
    let ManagedVpnGenerationSpec {
        managed,
        managed_tun_count,
        interface_name,
        ingress_index,
        ingress_name,
        platform,
        carrier_paths,
        native_proxy_endpoints,
        prepublication_domains,
        dns_policy,
        dns_activation,
        resolution_timeout,
    } = spec;
    let interface = LinuxInterfaceName::parse(interface_name).map_err(|source| {
        LinuxVpnGenerationSpecError::LinuxInterface {
            ingress_index,
            ingress_name,
            source,
        }
    })?;
    let config = LinuxVpnConfig::from_managed(interface, managed)
        .with_linux_policy(platform.linux.unwrap_or_default());
    Ok(Some(LinuxVpnPrepareRequest {
        config,
        managed_tun_count,
        carrier_paths: carrier_paths
            .into_iter()
            .map(|path| LinuxVpnCarrierPath {
                identity: path.identity,
                path: path.path,
            })
            .collect(),
        native_proxy_endpoints,
        prepublication_domains,
        dns_policy,
        dns_activation,
        resolution_timeout,
    }))
}

#[derive(Debug)]
pub enum LinuxVpnGenerationSpecError {
    MultipleManagedTunInbounds {
        actual: usize,
    },
    ManagedTun {
        ingress_index: usize,
        ingress_name: String,
        source: ManagedVpnCompileError,
    },
    LinuxInterface {
        ingress_index: usize,
        ingress_name: String,
        source: LinuxInterfaceNameError,
    },
    MppOutboundWithoutCarrierPaths {
        outbound: String,
    },
    InvalidCarrierEndpoint {
        outbound: String,
        path_ordinal: usize,
        endpoint: String,
    },
    InvalidNativeEndpoint {
        endpoint: String,
    },
    TooManyCarrierPaths {
        actual: usize,
        maximum: usize,
    },
    TooManyNativeEndpoints {
        actual: usize,
        maximum: usize,
    },
    DnsPolicy(DnsCompileError),
    DnsPolicyInvariant {
        message: String,
    },
    PreCarrierDnsEgressUnsupported {
        upstream: String,
        outbound: String,
    },
    SystemDnsUnsupported,
    EncryptedDnsRequired,
    FullTunnelDnsCaptureRequired,
}

impl From<ManagedVpnGenerationSpecError> for LinuxVpnGenerationSpecError {
    fn from(error: ManagedVpnGenerationSpecError) -> Self {
        match error {
            ManagedVpnGenerationSpecError::MultipleManagedTunInbounds { actual } => {
                Self::MultipleManagedTunInbounds { actual }
            }
            ManagedVpnGenerationSpecError::ManagedTun {
                ingress_index,
                ingress_name,
                source,
            } => Self::ManagedTun {
                ingress_index,
                ingress_name,
                source,
            },
            ManagedVpnGenerationSpecError::MppOutboundWithoutCarrierPaths { outbound } => {
                Self::MppOutboundWithoutCarrierPaths { outbound }
            }
            ManagedVpnGenerationSpecError::InvalidCarrierEndpoint {
                outbound,
                path_ordinal,
                endpoint,
            } => Self::InvalidCarrierEndpoint {
                outbound,
                path_ordinal,
                endpoint,
            },
            ManagedVpnGenerationSpecError::InvalidNativeEndpoint { endpoint } => {
                Self::InvalidNativeEndpoint { endpoint }
            }
            ManagedVpnGenerationSpecError::TooManyCarrierPaths { actual, maximum } => {
                Self::TooManyCarrierPaths { actual, maximum }
            }
            ManagedVpnGenerationSpecError::TooManyNativeEndpoints { actual, maximum } => {
                Self::TooManyNativeEndpoints { actual, maximum }
            }
            ManagedVpnGenerationSpecError::DnsPolicy(source) => Self::DnsPolicy(source),
            ManagedVpnGenerationSpecError::DnsPolicyInvariant { message } => {
                Self::DnsPolicyInvariant { message }
            }
            ManagedVpnGenerationSpecError::PreCarrierDnsEgressUnsupported {
                upstream,
                outbound,
            } => Self::PreCarrierDnsEgressUnsupported { upstream, outbound },
            ManagedVpnGenerationSpecError::SystemDnsUnsupported => Self::SystemDnsUnsupported,
            ManagedVpnGenerationSpecError::EncryptedDnsRequired => Self::EncryptedDnsRequired,
            ManagedVpnGenerationSpecError::FullTunnelDnsCaptureRequired => {
                Self::FullTunnelDnsCaptureRequired
            }
        }
    }
}

impl fmt::Display for LinuxVpnGenerationSpecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MultipleManagedTunInbounds { actual } => write!(
                formatter,
                "managed VPN generation requires exactly one managed TUN ingress; found {actual}"
            ),
            Self::ManagedTun {
                ingress_index,
                ingress_name,
                source,
            } => write!(
                formatter,
                "managed TUN inbound {ingress_name} at index {ingress_index} is invalid: {source}"
            ),
            Self::LinuxInterface {
                ingress_index,
                ingress_name,
                source,
            } => write!(
                formatter,
                "managed TUN inbound {ingress_name} at index {ingress_index} has an invalid Linux interface: {source}"
            ),
            Self::MppOutboundWithoutCarrierPaths { outbound } => write!(
                formatter,
                "managed VPN cannot prepare MPP outbound {outbound}: it has no carrier paths"
            ),
            Self::InvalidCarrierEndpoint {
                outbound,
                path_ordinal,
                endpoint,
            } => write!(
                formatter,
                "managed VPN MPP outbound {outbound} path {path_ordinal} has invalid endpoint {endpoint}"
            ),
            Self::InvalidNativeEndpoint { endpoint } => write!(
                formatter,
                "managed VPN native proxy has invalid endpoint {endpoint}"
            ),
            Self::TooManyCarrierPaths { actual, maximum } => write!(
                formatter,
                "managed VPN generation has {actual} carrier paths; maximum is {maximum}"
            ),
            Self::TooManyNativeEndpoints { actual, maximum } => write!(
                formatter,
                "managed VPN generation has {actual} unique native proxy endpoints; maximum is {maximum}"
            ),
            Self::DnsPolicy(error) => {
                write!(formatter, "managed VPN DNS policy is invalid: {error}")
            }
            Self::DnsPolicyInvariant { message } => {
                write!(formatter, "managed VPN DNS policy invariant failed: {message}")
            }
            Self::PreCarrierDnsEgressUnsupported { upstream, outbound } => write!(
                formatter,
                "managed VPN DNS server {upstream} selects outbound {outbound}, which is unavailable before carrier bootstrap"
            ),
            Self::SystemDnsUnsupported => formatter.write_str(
                "managed VPN cannot use system DNS because it creates a recursive tunnel dependency",
            ),
            Self::EncryptedDnsRequired => {
                formatter.write_str("managed VPN requires encrypted-only DNS policies")
            }
            Self::FullTunnelDnsCaptureRequired => {
                formatter.write_str("full managed VPN requires host DNS capture")
            }
        }
    }
}

impl std::error::Error for LinuxVpnGenerationSpecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ManagedTun { source, .. } => Some(source),
            Self::LinuxInterface { source, .. } => Some(source),
            Self::DnsPolicy(source) => Some(source),
            Self::MultipleManagedTunInbounds { .. }
            | Self::MppOutboundWithoutCarrierPaths { .. }
            | Self::InvalidCarrierEndpoint { .. }
            | Self::InvalidNativeEndpoint { .. }
            | Self::TooManyCarrierPaths { .. }
            | Self::TooManyNativeEndpoints { .. }
            | Self::DnsPolicyInvariant { .. }
            | Self::PreCarrierDnsEgressUnsupported { .. }
            | Self::SystemDnsUnsupported
            | Self::EncryptedDnsRequired
            | Self::FullTunnelDnsCaptureRequired => None,
        }
    }
}

/// A prepared host transaction and the exact providers for its runtime.
///
/// The caller starts one generation with these providers, calls
/// [`Self::publish_when_worker_ready`], retries [`Self::unpublish`] while the
/// worker remains live, stops the worker, and then calls
/// [`Self::cleanup_after_worker_stopped`]. No host policy is published by
/// `prepare`.
pub struct PreparedLinuxVpn {
    host: VpnHostLifecycle<SystemLinuxHostNetworkBackend>,
    packet_devices: Arc<ManagedPacketDeviceProvider>,
    worker_ready: Option<oneshot::Receiver<()>>,
    carrier_network: Arc<dyn CarrierNetworkProvider>,
    native_sockets: Arc<dyn NativeSocketConfigurator>,
}

impl PreparedLinuxVpn {
    pub fn packet_device_provider(&self) -> Arc<dyn PacketDeviceProvider> {
        self.packet_devices.clone()
    }

    pub fn carrier_network_provider(&self) -> Arc<dyn CarrierNetworkProvider> {
        self.carrier_network.clone()
    }

    pub fn native_socket_configurator(&self) -> Arc<dyn NativeSocketConfigurator> {
        self.native_sockets.clone()
    }

    pub fn state(&self) -> LinuxControllerState {
        self.host.state()
    }

    /// Waits for the managed TUN stack to construct all packet queues and
    /// listeners, then atomically publishes policy rules followed by DNS.
    pub async fn publish_when_worker_ready(
        &mut self,
        ready_timeout: Duration,
    ) -> Result<(), LinuxVpnPublishError> {
        let receiver = self
            .worker_ready
            .as_mut()
            .ok_or(LinuxVpnPublishError::ReadinessAlreadyConsumed)?;
        match tokio::time::timeout(ready_timeout, receiver).await {
            Err(_) => return Err(LinuxVpnPublishError::WorkerReadyTimeout(ready_timeout)),
            Ok(Err(_)) => {
                self.worker_ready.take();
                return Err(LinuxVpnPublishError::WorkerExitedBeforeReady);
            }
            Ok(Ok(())) => {
                self.worker_ready.take();
            }
        }
        if !self.packet_devices.device_live() {
            return Err(LinuxVpnPublishError::WorkerExitedBeforeReady);
        }
        self.host
            .publish()
            .map_err(|error| LinuxVpnPublishError::Publish(Box::new(error)))?;
        if !self.packet_devices.device_live() {
            let _ = self.host.unpublish();
            return Err(LinuxVpnPublishError::WorkerExitedDuringPublish);
        }
        Ok(())
    }

    /// Retries removal of every host traffic-publication step.
    ///
    /// The caller must keep the packet runtime alive until this succeeds.
    pub async fn unpublish(
        &mut self,
        attempts: NonZeroUsize,
        retry_delay: Duration,
    ) -> Result<(), LinuxVpnShutdownError> {
        let mut last_unpublish = None;
        for attempt in 0..attempts.get() {
            if self.host.pending_publish_steps() == 0 {
                return Ok(());
            }
            match self.host.unpublish() {
                Ok(()) => return Ok(()),
                Err(error) => last_unpublish = Some(error),
            }
            if attempt + 1 < attempts.get() {
                tokio::time::sleep(retry_delay).await;
            }
        }
        if self.host.pending_publish_steps() != 0 {
            return Err(LinuxVpnShutdownError::Unpublish(Box::new(
                last_unpublish.expect("pending publication has a rollback error"),
            )));
        }
        Ok(())
    }

    /// Cleans inert host preparation after the packet runtime has stopped.
    ///
    /// Publication must already be absent. Cleanup failure may leave only
    /// non-capturing preparation artifacts; it can never re-publish traffic.
    pub async fn cleanup_after_worker_stopped(
        &mut self,
        attempts: NonZeroUsize,
        retry_delay: Duration,
    ) -> Result<(), LinuxVpnShutdownError> {
        let pending_publish_steps = self.host.pending_publish_steps();
        if pending_publish_steps != 0 {
            return Err(LinuxVpnShutdownError::PublicationStillActive {
                pending_steps: pending_publish_steps,
            });
        }
        self.packet_devices.discard_unopened_device();
        if self.packet_devices.device_live() {
            return Err(LinuxVpnShutdownError::PacketWorkerStillRunning);
        }

        let mut last_cleanup = None;
        for attempt in 0..attempts.get() {
            if self.host.state() == LinuxControllerState::Idle {
                return Ok(());
            }
            match self.host.cleanup() {
                Ok(()) => return Ok(()),
                Err(error) => last_cleanup = Some(error),
            }
            if attempt + 1 < attempts.get() {
                tokio::time::sleep(retry_delay).await;
            }
        }
        Err(LinuxVpnShutdownError::Cleanup(Box::new(
            last_cleanup.expect("non-idle host transaction has a cleanup error"),
        )))
    }
}

/// Snapshots native routes, resolves all generation-critical endpoints, builds
/// and prepares an inert plan, then hands the TUN to a one-shot provider.
pub async fn prepare_linux_vpn(
    request: LinuxVpnPrepareRequest,
) -> Result<PreparedLinuxVpn, LinuxVpnPrepareError> {
    validate_dns_preflight(&request)?;
    if request.managed_tun_count != 1 {
        return Err(LinuxVpnPrepareError::ManagedTunCount(
            request.managed_tun_count,
        ));
    }
    if request.resolution_timeout.is_zero() {
        return Err(LinuxVpnPrepareError::ResolutionTimeoutZero);
    }
    if request.carrier_paths.len() > MAX_PREPARED_CARRIER_PATHS {
        return Err(LinuxVpnPrepareError::TooManyCarrierPaths {
            actual: request.carrier_paths.len(),
            maximum: MAX_PREPARED_CARRIER_PATHS,
        });
    }
    if request.native_proxy_endpoints.len() > MAX_NATIVE_ENDPOINTS {
        return Err(LinuxVpnPrepareError::TooManyNativeEndpoints {
            actual: request.native_proxy_endpoints.len(),
            maximum: MAX_NATIVE_ENDPOINTS,
        });
    }
    let bootstrap_dns = if request.prepublication_domains.is_empty() {
        None
    } else {
        Some(compile_bootstrap_dns(
            request.dns_policy.clone(),
            &request.prepublication_domains,
        )?)
    };

    // Snapshot comes first: every plan decision is based on one immutable view
    // taken before this transaction creates an interface or route.
    let mut runner = SystemCommandRunner;
    let environment = snapshot_linux_environment(&mut runner)
        .map_err(|error| LinuxVpnPrepareError::Snapshot(Box::new(error)))?;
    let deadline = tokio::time::Instant::now() + request.resolution_timeout;
    let prepared_carriers =
        resolve_carrier_paths(request.carrier_paths, bootstrap_dns.as_ref(), deadline).await?;
    let native_proxy_addresses = resolve_native_endpoints(
        request.native_proxy_endpoints,
        bootstrap_dns.as_ref(),
        deadline,
    )
    .await?;
    let bootstrap_dns = request
        .dns_policy
        .bootstrap_endpoints_for_activation(&request.dns_activation)
        .map(|endpoint| endpoint.ip())
        .collect::<Vec<_>>();

    let mut native_endpoints = prepared_carriers.endpoint_addresses();
    native_endpoints.extend(native_proxy_addresses);
    native_endpoints.sort_unstable();
    native_endpoints.dedup();
    let plan = LinuxVpnPlan::build(
        &request.config,
        &environment,
        native_endpoints,
        bootstrap_dns,
    )
    .map_err(LinuxVpnPrepareError::Plan)?;

    let backend = SystemLinuxHostNetworkBackend::new(runner, SystemTunDeviceFactory);
    let (host, device) = VpnHostLifecycle::prepare(backend, plan).map_err(|error| match error {
        HostPrepareError::Prepare { source, cleanup } => {
            LinuxVpnPrepareError::Prepare { source, cleanup }
        }
        HostPrepareError::Device { source, cleanup } => {
            LinuxVpnPrepareError::DeviceHandoff { source, cleanup }
        }
    })?;
    let (packet_devices, worker_ready) = ManagedPacketDeviceProvider::new(device);
    let mark = request.config.linux_policy().socket_mark();
    let prepared_carriers: Arc<dyn CarrierNetworkProvider> = Arc::new(prepared_carriers);
    let carrier_network: Arc<dyn CarrierNetworkProvider> = Arc::new(
        LinuxMarkedCarrierNetworkProvider::new(prepared_carriers, mark),
    );
    let native_sockets: Arc<dyn NativeSocketConfigurator> =
        Arc::new(LinuxMarkedNativeSocketConfigurator::new(mark));
    Ok(PreparedLinuxVpn {
        host,
        packet_devices,
        worker_ready: Some(worker_ready),
        carrier_network,
        native_sockets,
    })
}

fn validate_dns_preflight(request: &LinuxVpnPrepareRequest) -> Result<(), LinuxVpnPrepareError> {
    if request
        .dns_policy
        .uses_system_resolution_for_activation(&request.dns_activation)
    {
        return Err(LinuxVpnPrepareError::SystemDnsUnsupported);
    }
    if !request
        .dns_policy
        .is_encrypted_only_for_activation(&request.dns_activation)
    {
        return Err(LinuxVpnPrepareError::EncryptedDnsRequired);
    }
    if request.config.dns().is_none()
        && matches!(
            request.config.route_mode(),
            crate::platform::RouteMode::Full
        )
    {
        return Err(LinuxVpnPrepareError::FullTunnelDnsCaptureRequired);
    }
    Ok(())
}

fn compile_bootstrap_dns(
    policy: Arc<CompiledDnsPolicy>,
    domains: &[DomainName],
) -> Result<DnsGeneration, LinuxVpnPrepareError> {
    DnsGeneration::compile_prepublication(policy, domains)
        .map_err(LinuxVpnPrepareError::BootstrapDns)
}

async fn resolve_carrier_paths(
    requests: Vec<LinuxVpnCarrierPath>,
    dns: Option<&DnsGeneration>,
    deadline: tokio::time::Instant,
) -> Result<PreparedCarrierNetworkProvider, LinuxVpnPrepareError> {
    let mut resolutions = FuturesUnordered::new();
    for request in requests {
        let dns = dns.cloned();
        resolutions.push(async move {
            let authority = request.path.endpoint.authority();
            let bootstrap_port = request.path.endpoint.ports().first();
            let addresses = match request.path.endpoint.host.parse::<IpAddr>() {
                Ok(address) => vec![std::net::SocketAddr::new(address, bootstrap_port)],
                Err(_) => {
                    let dns = dns.ok_or_else(|| LinuxVpnPrepareError::DnsResolution {
                        endpoint: authority.clone(),
                        source: DnsRuntimeError::PolicyInvariant(
                            "pre-publication endpoint requires an unavailable direct DNS policy"
                                .to_string(),
                        ),
                    })?;
                    tokio::time::timeout_at(
                        deadline,
                        dns.resolve_socket_addrs(
                            request.path.endpoint.host.as_str(),
                            bootstrap_port,
                        ),
                    )
                    .await
                    .map_err(|_| LinuxVpnPrepareError::ResolutionTimedOut(authority.clone()))?
                    .map_err(|source| LinuxVpnPrepareError::DnsResolution {
                        endpoint: authority.clone(),
                        source,
                    })?
                }
            };
            PreparedCarrierPath::new(request.identity, request.path, addresses).map_err(|source| {
                LinuxVpnPrepareError::Resolution {
                    endpoint: authority,
                    source,
                }
            })
        });
    }
    let mut prepared = Vec::new();
    while let Some(result) = resolutions.next().await {
        prepared.push(result?);
    }
    PreparedCarrierNetworkProvider::new(prepared).map_err(LinuxVpnPrepareError::Inventory)
}

async fn resolve_native_endpoints(
    endpoints: Vec<Endpoint>,
    dns: Option<&DnsGeneration>,
    deadline: tokio::time::Instant,
) -> Result<Vec<IpAddr>, LinuxVpnPrepareError> {
    let mut unique = Vec::new();
    for endpoint in endpoints {
        if !unique.contains(&endpoint) {
            unique.push(endpoint);
        }
    }
    let mut resolutions = FuturesUnordered::new();
    for endpoint in unique {
        let dns = dns.cloned();
        resolutions.push(async move {
            let authority = endpoint.authority();
            match endpoint.host.parse::<IpAddr>() {
                Ok(address) => Ok(vec![address]),
                Err(_) => {
                    let dns = dns.ok_or_else(|| LinuxVpnPrepareError::DnsResolution {
                        endpoint: authority.clone(),
                        source: DnsRuntimeError::PolicyInvariant(
                            "pre-publication endpoint requires an unavailable direct DNS policy"
                                .to_string(),
                        ),
                    })?;
                    tokio::time::timeout_at(
                        deadline,
                        dns.resolve_socket_addrs(endpoint.host.as_str(), endpoint.port),
                    )
                    .await
                    .map_err(|_| LinuxVpnPrepareError::ResolutionTimedOut(authority.clone()))?
                    .map_err(|source| LinuxVpnPrepareError::DnsResolution {
                        endpoint: authority,
                        source,
                    })
                    .map(|addresses| {
                        addresses
                            .into_iter()
                            .map(|address| address.ip())
                            .collect::<Vec<_>>()
                    })
                }
            }
        });
    }
    let mut addresses = Vec::new();
    while let Some(result) = resolutions.next().await {
        addresses.extend(result?);
    }
    // Loopback proxy controls never cross host routing and need no bypass.
    addresses.retain(|address| !address.is_loopback());
    addresses.sort_unstable();
    addresses.dedup();
    Ok(addresses)
}

struct VpnHostLifecycle<Backend>
where
    Backend: LinuxHostMutationBackend,
{
    controller: TransactionalLinuxVpnController<Backend>,
}

impl<Backend> VpnHostLifecycle<Backend>
where
    Backend: LinuxHostMutationBackend,
{
    fn prepare(
        backend: Backend,
        plan: LinuxVpnPlan,
    ) -> Result<(Self, Backend::PreparedDevice), HostPrepareError<Backend::Error>> {
        let mut controller = TransactionalLinuxVpnController::new(backend);
        if let Err(source) = controller.prepare(plan) {
            let cleanup = (controller.state() != LinuxControllerState::Idle)
                .then(|| controller.cleanup().err())
                .flatten();
            return Err(HostPrepareError::Prepare {
                source: Box::new(source),
                cleanup: cleanup.map(Box::new),
            });
        }
        let device = match controller.take_prepared_device() {
            Ok(device) => device,
            Err(source) => {
                let cleanup = controller.cleanup().err();
                return Err(HostPrepareError::Device {
                    source: Box::new(source),
                    cleanup: cleanup.map(Box::new),
                });
            }
        };
        Ok((Self { controller }, device))
    }

    fn state(&self) -> LinuxControllerState {
        self.controller.state()
    }

    fn pending_publish_steps(&self) -> usize {
        self.controller.pending_publish_steps()
    }

    fn publish(&mut self) -> Result<(), LinuxPublishError<Backend::Error>> {
        self.controller.publish()
    }

    fn unpublish(&mut self) -> Result<(), LinuxCleanupError<Backend::Error>> {
        self.controller.unpublish().map(|_| ())
    }

    fn cleanup(&mut self) -> Result<(), LinuxCleanupError<Backend::Error>> {
        self.controller.cleanup().map(|_| ())
    }
}

#[derive(Debug)]
enum HostPrepareError<Error> {
    Prepare {
        source: Box<LinuxPrepareError<Error>>,
        cleanup: Option<Box<LinuxCleanupError<Error>>>,
    },
    Device {
        source: Box<LinuxPreparedDeviceError<Error>>,
        cleanup: Option<Box<LinuxCleanupError<Error>>>,
    },
}

#[derive(Debug)]
pub enum LinuxVpnPrepareError {
    SystemDnsUnsupported,
    EncryptedDnsRequired,
    FullTunnelDnsCaptureRequired,
    ManagedTunCount(usize),
    ResolutionTimeoutZero,
    TooManyCarrierPaths {
        actual: usize,
        maximum: usize,
    },
    TooManyNativeEndpoints {
        actual: usize,
        maximum: usize,
    },
    Snapshot(Box<LinuxBackendError>),
    BootstrapDns(DnsRuntimeError),
    ResolutionTimedOut(String),
    DnsResolution {
        endpoint: String,
        source: DnsRuntimeError,
    },
    Resolution {
        endpoint: String,
        source: io::Error,
    },
    Inventory(io::Error),
    Plan(LinuxVpnPlanError),
    Prepare {
        source: Box<LinuxPrepareError<LinuxBackendError>>,
        cleanup: Option<Box<LinuxCleanupError<LinuxBackendError>>>,
    },
    DeviceHandoff {
        source: Box<LinuxPreparedDeviceError<LinuxBackendError>>,
        cleanup: Option<Box<LinuxCleanupError<LinuxBackendError>>>,
    },
}

impl fmt::Display for LinuxVpnPrepareError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SystemDnsUnsupported => formatter.write_str(
                "managed VPN cannot use system DNS because it creates a recursive tunnel dependency",
            ),
            Self::EncryptedDnsRequired => {
                formatter.write_str("managed VPN requires encrypted-only DNS policies")
            }
            Self::FullTunnelDnsCaptureRequired => {
                formatter.write_str("full managed VPN requires host DNS capture")
            }
            Self::ManagedTunCount(count) => {
                write!(
                    formatter,
                    "managed VPN requires exactly one TUN ingress; found {count}"
                )
            }
            Self::ResolutionTimeoutZero => {
                formatter.write_str("managed VPN endpoint resolution timeout must be nonzero")
            }
            Self::TooManyCarrierPaths { actual, maximum } => {
                write!(formatter, "managed VPN has {actual} carrier paths; maximum is {maximum}")
            }
            Self::TooManyNativeEndpoints { actual, maximum } => write!(
                formatter,
                "managed VPN has {actual} native proxy endpoints; maximum is {maximum}"
            ),
            Self::Snapshot(error) => write!(formatter, "failed to snapshot native routes: {error}"),
            Self::BootstrapDns(error) => {
                write!(formatter, "failed to compile pre-VPN encrypted DNS: {error}")
            }
            Self::ResolutionTimedOut(endpoint) => {
                write!(formatter, "pre-VPN resolution timed out for {endpoint}")
            }
            Self::DnsResolution { endpoint, source } => {
                write!(
                    formatter,
                    "pre-VPN encrypted DNS failed for {endpoint}: {source}"
                )
            }
            Self::Resolution { endpoint, source } => {
                write!(formatter, "pre-VPN resolution failed for {endpoint}: {source}")
            }
            Self::Inventory(error) => write!(formatter, "invalid carrier inventory: {error}"),
            Self::Plan(error) => write!(formatter, "failed to plan managed VPN: {error}"),
            Self::Prepare { source, cleanup } => {
                write!(formatter, "failed to prepare managed VPN: {source}")?;
                write_prepare_cleanup_suffix(formatter, cleanup.as_deref())
            }
            Self::DeviceHandoff { source, cleanup } => {
                write!(formatter, "failed to hand off managed VPN device: {source}")?;
                write_prepare_cleanup_suffix(formatter, cleanup.as_deref())
            }
        }
    }
}

fn write_prepare_cleanup_suffix(
    formatter: &mut fmt::Formatter<'_>,
    cleanup: Option<&LinuxCleanupError<LinuxBackendError>>,
) -> fmt::Result {
    if let Some(cleanup) = cleanup {
        write!(
            formatter,
            "; residual prepare cleanup also failed: {cleanup}"
        )
    } else {
        Ok(())
    }
}

impl std::error::Error for LinuxVpnPrepareError {}

#[derive(Debug)]
pub enum LinuxVpnPublishError {
    ReadinessAlreadyConsumed,
    WorkerReadyTimeout(Duration),
    WorkerExitedBeforeReady,
    WorkerExitedDuringPublish,
    Publish(Box<LinuxPublishError<LinuxBackendError>>),
}

impl fmt::Display for LinuxVpnPublishError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReadinessAlreadyConsumed => {
                formatter.write_str("managed VPN worker readiness was already consumed")
            }
            Self::WorkerReadyTimeout(timeout) => {
                write!(
                    formatter,
                    "managed VPN worker was not ready within {timeout:?}"
                )
            }
            Self::WorkerExitedBeforeReady => {
                formatter.write_str("managed VPN packet worker exited before readiness")
            }
            Self::WorkerExitedDuringPublish => {
                formatter.write_str("managed VPN packet worker exited during publication")
            }
            Self::Publish(error) => write!(formatter, "failed to publish managed VPN: {error}"),
        }
    }
}

impl std::error::Error for LinuxVpnPublishError {}

#[derive(Debug)]
pub enum LinuxVpnShutdownError {
    Unpublish(Box<LinuxCleanupError<LinuxBackendError>>),
    PublicationStillActive { pending_steps: usize },
    PacketWorkerStillRunning,
    Cleanup(Box<LinuxCleanupError<LinuxBackendError>>),
}

impl fmt::Display for LinuxVpnShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unpublish(error) => {
                write!(formatter, "failed to unpublish managed VPN: {error}")
            }
            Self::PublicationStillActive { pending_steps } => write!(
                formatter,
                "managed VPN cleanup refused while {pending_steps} host publication steps remain"
            ),
            Self::PacketWorkerStillRunning => formatter
                .write_str("managed VPN packet worker still owns the TUN after stop completed"),
            Self::Cleanup(error) => write!(formatter, "failed to clean managed VPN: {error}"),
        }
    }
}

impl std::error::Error for LinuxVpnShutdownError {}

#[cfg(test)]
#[path = "tests_linux_vpn.rs"]
mod tests;
