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
use crate::product::{CompiledDnsPolicy, DnsCompileError, DomainName};
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
        ingress_tag,
        platform,
        carrier_paths,
        native_proxy_endpoints,
        prepublication_domains,
        dns_policy,
        resolution_timeout,
    } = spec;
    let interface = LinuxInterfaceName::parse(interface_name).map_err(|source| {
        LinuxVpnGenerationSpecError::LinuxInterface {
            ingress_index,
            ingress_tag,
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
        ingress_tag: Option<String>,
        source: ManagedVpnCompileError,
    },
    LinuxInterface {
        ingress_index: usize,
        ingress_tag: Option<String>,
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
                ingress_tag,
                source,
            } => Self::ManagedTun {
                ingress_index,
                ingress_tag,
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
                ingress_tag,
                source,
            } => write!(
                formatter,
                "managed TUN ingress {} at index {ingress_index} is invalid: {source}",
                ingress_tag.as_deref().unwrap_or("<untagged>")
            ),
            Self::LinuxInterface {
                ingress_index,
                ingress_tag,
                source,
            } => write!(
                formatter,
                "managed TUN ingress {} at index {ingress_index} has an invalid Linux interface: {source}",
                ingress_tag.as_deref().unwrap_or("<untagged>")
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
                "managed VPN DNS upstream {upstream} selects outbound {outbound}, which is unavailable before carrier bootstrap"
            ),
            Self::SystemDnsUnsupported => formatter.write_str(
                "managed VPN cannot use system DNS because it creates a recursive tunnel dependency",
            ),
            Self::EncryptedDnsRequired => {
                formatter.write_str("managed VPN requires encrypted-only DNS plans")
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
        .bootstrap_endpoints()
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
    if request.dns_policy.uses_system_resolution() {
        return Err(LinuxVpnPrepareError::SystemDnsUnsupported);
    }
    if !request.dns_policy.is_encrypted_only() {
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
            let addresses = match request.path.endpoint.host.parse::<IpAddr>() {
                Ok(address) => vec![std::net::SocketAddr::new(
                    address,
                    request.path.endpoint.port,
                )],
                Err(_) => {
                    let dns = dns.ok_or_else(|| LinuxVpnPrepareError::DnsResolution {
                        endpoint: authority.clone(),
                        source: DnsRuntimeError::PolicyInvariant(
                            "pre-publication endpoint requires an unavailable direct DNS plan"
                                .to_string(),
                        ),
                    })?;
                    tokio::time::timeout_at(
                        deadline,
                        dns.resolve_socket_addrs(
                            request.path.endpoint.host.as_str(),
                            request.path.endpoint.port,
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
                            "pre-publication endpoint requires an unavailable direct DNS plan"
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
                formatter.write_str("managed VPN requires encrypted-only DNS plans")
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
mod tests {
    use super::*;
    use crate::config::{
        ClientPathConfig, ClientSecurityConfig, CommandConfig, DnsPolicyConfig, LocalIngressConfig,
        ManagementConfig, MppOutboundConfig, OutboundLeafConfig, ProductPolicyConfig,
        ServiceConfig, SessionConfig, SharedSecret,
    };
    use crate::ingress::IngressConfig;
    use crate::ingress::tun::{
        ManagedVpnConfig, ManagedVpnPlatformConfig, TunHostConfig, TunL4Config,
    };
    use crate::outbound::{HttpsProxyConfig, OutboundConfig, ProxyConfig};
    use crate::performance::{MppPerformanceConfig, ResourceLimits};
    use crate::platform::{
        AddressFamily, LinuxHostMutationBackend, LinuxHostOperation, LinuxInterfaceName,
        LinuxNativeRoute, LinuxVpnEnvironment, LinuxVpnPlan, RouteMode,
    };
    use crate::product::{
        DnsEgressSpec, DnsOutboundCapabilitySpec, DnsPlanId, DnsPlanSpec, DnsPolicySpec,
        DnsSecurityPolicy, DnsUpstreamEndpoint, DnsUpstreamId, DnsUpstreamSpec, DomainName,
        NetworkSet, OutboundId,
    };
    use ipnet::IpNet;
    use std::collections::HashMap;
    use std::convert::Infallible;
    use std::sync::Mutex;

    #[derive(Default)]
    struct FakeBackend {
        applied: Vec<LinuxHostOperation>,
        rolled_back: Vec<LinuxHostOperation>,
        device: Option<u8>,
    }

    impl LinuxHostMutationBackend for FakeBackend {
        type RollbackToken = LinuxHostOperation;
        type PreparedDevice = u8;
        type Error = Infallible;

        fn apply(
            &mut self,
            operation: &LinuxHostOperation,
        ) -> Result<LinuxHostOperation, Infallible> {
            self.applied.push(operation.clone());
            if matches!(operation, LinuxHostOperation::CreateTun { .. }) {
                self.device = Some(7);
            }
            Ok(operation.clone())
        }

        fn rollback(
            &mut self,
            operation: &LinuxHostOperation,
            _token: &LinuxHostOperation,
        ) -> Result<(), Infallible> {
            self.rolled_back.push(operation.clone());
            Ok(())
        }

        fn take_prepared_device(&mut self) -> Result<u8, Infallible> {
            Ok(self.device.take().expect("prepared fake device"))
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct InjectedHostError(&'static str);

    impl fmt::Display for InjectedHostError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str(self.0)
        }
    }

    impl std::error::Error for InjectedHostError {}

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum InjectedHostEvent {
        Apply(usize),
        Rollback(usize),
        TakeDevice,
    }

    #[derive(Debug, Default)]
    struct InjectedHostControl {
        fail_apply_at: Option<usize>,
        fail_take: bool,
        rollback_failures: HashMap<usize, usize>,
        events: Vec<InjectedHostEvent>,
    }

    #[derive(Debug, Clone, Default)]
    struct InjectedHostHandle(Arc<Mutex<InjectedHostControl>>);

    impl InjectedHostHandle {
        fn fail_apply_at(&self, operation: usize) {
            self.0.lock().expect("host fault control").fail_apply_at = Some(operation);
        }

        fn fail_take(&self) {
            self.0.lock().expect("host fault control").fail_take = true;
        }

        fn fail_rollback(&self, token: usize, times: usize) {
            self.0
                .lock()
                .expect("host fault control")
                .rollback_failures
                .insert(token, times);
        }

        fn events(&self) -> Vec<InjectedHostEvent> {
            self.0.lock().expect("host fault control").events.clone()
        }
    }

    struct InjectedHostBackend {
        control: InjectedHostHandle,
        next_token: usize,
        device: Option<u8>,
    }

    impl InjectedHostBackend {
        fn new(control: InjectedHostHandle) -> Self {
            Self {
                control,
                next_token: 0,
                device: None,
            }
        }
    }

    impl LinuxHostMutationBackend for InjectedHostBackend {
        type RollbackToken = usize;
        type PreparedDevice = u8;
        type Error = InjectedHostError;

        fn apply(
            &mut self,
            operation: &LinuxHostOperation,
        ) -> Result<Self::RollbackToken, Self::Error> {
            let token = self.next_token;
            self.next_token = self.next_token.saturating_add(1);
            let mut control = self.control.0.lock().expect("host fault control");
            control.events.push(InjectedHostEvent::Apply(token));
            if control.fail_apply_at == Some(token) {
                return Err(InjectedHostError("apply"));
            }
            if matches!(operation, LinuxHostOperation::CreateTun { .. }) {
                self.device = Some(7);
            }
            Ok(token)
        }

        fn rollback(
            &mut self,
            _operation: &LinuxHostOperation,
            token: &Self::RollbackToken,
        ) -> Result<(), Self::Error> {
            let mut control = self.control.0.lock().expect("host fault control");
            control.events.push(InjectedHostEvent::Rollback(*token));
            if let Some(remaining) = control.rollback_failures.get_mut(token)
                && *remaining > 0
            {
                *remaining -= 1;
                return Err(InjectedHostError("rollback"));
            }
            Ok(())
        }

        fn take_prepared_device(&mut self) -> Result<Self::PreparedDevice, Self::Error> {
            let mut control = self.control.0.lock().expect("host fault control");
            control.events.push(InjectedHostEvent::TakeDevice);
            if control.fail_take {
                return Err(InjectedHostError("take device"));
            }
            self.device
                .take()
                .ok_or(InjectedHostError("missing device"))
        }
    }

    fn test_plan() -> LinuxVpnPlan {
        let interface = LinuxInterfaceName::parse("mptun0").expect("interface");
        let config = LinuxVpnConfig::new(
            interface.clone(),
            vec!["10.88.0.1/24".parse::<IpNet>().expect("TUN address")],
            1500,
            RouteMode::Full,
        )
        .expect("VPN config");
        let native = LinuxNativeRoute::new(
            AddressFamily::Ipv4,
            LinuxInterfaceName::parse("eth0").expect("native interface"),
            Some("192.0.2.1".parse().expect("gateway")),
            Some("192.0.2.2".parse().expect("source")),
            100,
        )
        .expect("native route");
        let environment = LinuxVpnEnvironment::new(vec![native], Vec::new()).expect("environment");
        LinuxVpnPlan::build(
            &config,
            &environment,
            ["198.51.100.10".parse().expect("carrier")],
            [],
        )
        .expect("plan")
    }

    fn outbound_id(value: &str) -> OutboundId {
        OutboundId::parse(value).expect("outbound ID")
    }

    fn encrypted_dns_policy() -> DnsPolicyConfig {
        encrypted_dns_policy_with_egress(DnsEgressSpec::Direct)
    }

    fn encrypted_dns_policy_with_egress(egress: DnsEgressSpec) -> DnsPolicyConfig {
        let upstream_id = DnsUpstreamId::parse("bootstrap-dot").expect("upstream ID");
        let plan_id = DnsPlanId::parse("default").expect("plan ID");
        let mut plan = DnsPlanSpec::new(plan_id.clone(), vec![upstream_id.clone()]);
        plan.security = DnsSecurityPolicy::RequireEncrypted;
        let outbound_capabilities = match &egress {
            DnsEgressSpec::Direct => Vec::new(),
            DnsEgressSpec::Outbound(outbound) => vec![DnsOutboundCapabilitySpec::new(
                outbound.clone(),
                NetworkSet::TCP,
                true,
            )],
        };
        DnsPolicyConfig {
            generation: 17,
            spec: DnsPolicySpec {
                upstreams: vec![DnsUpstreamSpec {
                    id: upstream_id,
                    endpoint: DnsUpstreamEndpoint::Tls {
                        bootstrap: "1.1.1.1:853".parse().expect("bootstrap"),
                        server_name: DomainName::parse("one.one.one.one").expect("server name"),
                    },
                    egress,
                }],
                outbound_capabilities,
                plans: vec![plan],
                rules: Vec::new(),
                hosts: Vec::new(),
                fake_dns: None,
                default_plan: plan_id,
            },
        }
    }

    fn plaintext_dns_policy() -> DnsPolicyConfig {
        let upstream_id = DnsUpstreamId::parse("plaintext").expect("upstream ID");
        let plan_id = DnsPlanId::parse("default").expect("plan ID");
        DnsPolicyConfig {
            generation: 18,
            spec: DnsPolicySpec {
                upstreams: vec![DnsUpstreamSpec::direct(
                    upstream_id.clone(),
                    DnsUpstreamEndpoint::Udp {
                        bootstrap: "1.1.1.1:53".parse().expect("bootstrap"),
                    },
                )],
                outbound_capabilities: Vec::new(),
                plans: vec![DnsPlanSpec::new(plan_id.clone(), vec![upstream_id])],
                rules: Vec::new(),
                hosts: Vec::new(),
                fake_dns: None,
                default_plan: plan_id,
            },
        }
    }

    fn managed_tun(tag: &str) -> LocalIngressConfig {
        LocalIngressConfig {
            tag: Some(tag.to_owned()),
            config: IngressConfig::TunL4(TunL4Config {
                host: TunHostConfig::Managed(ManagedVpnConfig {
                    route_mode: RouteMode::Full,
                    excludes: Vec::new(),
                    local_lan: false,
                    dns_capture_servers: vec!["10.88.0.53".parse().expect("DNS capture server")],
                    platform: ManagedVpnPlatformConfig {
                        linux: Some(crate::platform::LinuxPolicyConfig::default()),
                    },
                }),
                ..TunL4Config::default()
            }),
        }
    }

    fn external_tun(tag: &str) -> LocalIngressConfig {
        LocalIngressConfig {
            tag: Some(tag.to_owned()),
            config: IngressConfig::TunL4(TunL4Config::default()),
        }
    }

    fn direct_leaf(id: &str) -> OutboundLeafConfig {
        local_leaf(id, OutboundConfig::Direct)
    }

    fn local_leaf(id: &str, config: OutboundConfig) -> OutboundLeafConfig {
        OutboundLeafConfig::Local {
            id: outbound_id(id),
            config,
            connect_timeout: Duration::from_secs(5),
        }
    }

    fn proxy_leaf(id: &str, endpoint: Endpoint) -> OutboundLeafConfig {
        local_leaf(id, OutboundConfig::Socks5(ProxyConfig::new(endpoint, None)))
    }

    fn test_security() -> ClientSecurityConfig {
        ClientSecurityConfig::for_test(
            SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("shared secret"),
        )
    }

    fn mpp_leaf(id: &str, paths: impl IntoIterator<Item = PathSpec>) -> OutboundLeafConfig {
        let security = test_security();
        let paths = paths
            .into_iter()
            .map(|spec| ClientPathConfig {
                tls: crate::transport::encrypted::test_client_tls_config(),
                spec,
                security: security.clone(),
            })
            .collect();
        OutboundLeafConfig::Mpp {
            id: outbound_id(id),
            config: Box::new(MppOutboundConfig {
                security,
                paths,
                path_probe_interval: Duration::from_secs(10),
                path_probe_timeout: Duration::from_secs(2),
                performance: MppPerformanceConfig::default(),
            }),
        }
    }

    fn node_with_vpn(outbounds: Vec<OutboundLeafConfig>) -> NodeConfig {
        NodeConfig {
            outbounds,
            gateway_balancers: Vec::new(),
            local_ingresses: vec![managed_tun("vpn")],
            product_policy: Some(ProductPolicyConfig {
                generation: 1,
                routes: Vec::new(),
                destination_acl: Vec::new(),
            }),
            dns_policy: encrypted_dns_policy(),
            servers: Vec::new(),
        }
    }

    fn app_with_node(node: NodeConfig) -> AppConfig {
        AppConfig {
            log_level: "info".to_owned(),
            check_config: false,
            service: ServiceConfig::default(),
            session: SessionConfig::default(),
            resources: ResourceLimits::default(),
            admission: crate::product::ProductAdmissionConfig::default(),
            management: ManagementConfig::default(),
            command: CommandConfig::Node(node),
        }
    }

    #[test]
    fn node_without_managed_tun_compiles_to_none_without_dns_side_effects() {
        let node = NodeConfig {
            outbounds: vec![direct_leaf("direct")],
            gateway_balancers: Vec::new(),
            local_ingresses: vec![external_tun("external")],
            product_policy: None,
            dns_policy: DnsPolicyConfig::system_default(),
            servers: Vec::new(),
        };

        assert!(
            compile_node_linux_vpn_prepare_request(&node)
                .expect("external TUN is not a managed generation")
                .is_none()
        );
    }

    #[test]
    fn app_compiler_supports_direct_only_managed_vpn() {
        let app = app_with_node(node_with_vpn(vec![direct_leaf("direct")]));

        let request = compile_linux_vpn_prepare_request(&app)
            .expect("compile")
            .expect("managed request");

        assert_eq!(request.managed_tun_count, 1);
        assert!(request.carrier_paths.is_empty());
        assert!(request.native_proxy_endpoints.is_empty());
        assert!(request.prepublication_domains.is_empty());
        assert_eq!(request.resolution_timeout, LINUX_VPN_RESOLUTION_TIMEOUT);
        assert!(!request.resolution_timeout.is_zero());
        assert_eq!(request.config.route_mode(), &RouteMode::Full);
        assert_eq!(request.dns_policy.generation(), 17);
        assert_eq!(
            request.dns_policy.bootstrap_endpoints().collect::<Vec<_>>(),
            vec!["1.1.1.1:853".parse().expect("bootstrap")]
        );
    }

    #[test]
    fn compiler_matches_combined_runtime_ordinals_and_collects_all_native_proxies() {
        let proxy_a = Endpoint::new("proxy-a.example", 1080).expect("proxy A");
        let proxy_b = Endpoint::new("proxy-b.example", 8443).expect("proxy B");
        let proxy_c = Endpoint::new("proxy-c.example", 8080).expect("proxy C");
        let https = HttpsProxyConfig::new(
            ProxyConfig::new(proxy_b.clone(), None),
            Some("proxy-b.example".to_owned()),
            Vec::new(),
        )
        .expect("HTTPS proxy");
        let node = node_with_vpn(vec![
            direct_leaf("direct"),
            mpp_leaf(
                "first-mpp",
                [
                    "tcp://carrier-a.example:443".parse().expect("TCP path"),
                    "udp://carrier-b.example:443".parse().expect("QUIC path"),
                ],
            ),
            proxy_leaf("socks", proxy_a.clone()),
            local_leaf("https", OutboundConfig::HttpsConnect(Box::new(https))),
            mpp_leaf(
                "second-mpp",
                ["udp://carrier-c.example:8443"
                    .parse()
                    .expect("second QUIC path")],
            ),
            local_leaf(
                "connect",
                OutboundConfig::HttpConnect(ProxyConfig::new(proxy_c.clone(), None)),
            ),
            proxy_leaf("duplicate-socks", proxy_a.clone()),
        ]);

        let request = compile_node_linux_vpn_prepare_request(&node)
            .expect("compile")
            .expect("managed request");

        assert_eq!(
            request
                .carrier_paths
                .iter()
                .map(|path| (
                    path.identity.group_ordinal,
                    path.identity.path_ordinal,
                    path.path.endpoint.authority(),
                ))
                .collect::<Vec<_>>(),
            vec![
                (0, 0, "carrier-a.example:443".to_owned()),
                (0, 1, "carrier-b.example:443".to_owned()),
                (1, 0, "carrier-c.example:8443".to_owned()),
            ]
        );
        assert_eq!(
            request.native_proxy_endpoints,
            vec![proxy_a, proxy_b, proxy_c],
            "every local proxy control endpoint is retained once in leaf order"
        );
        assert_eq!(
            request
                .prepublication_domains
                .iter()
                .map(DomainName::as_str)
                .collect::<Vec<_>>(),
            vec![
                "carrier-a.example",
                "carrier-b.example",
                "carrier-c.example",
                "proxy-a.example",
                "proxy-b.example",
                "proxy-c.example",
            ],
            "pre-publication DNS inventory is canonical, sorted, and deduplicated"
        );
    }

    #[test]
    fn compiler_rejects_multiple_or_invalid_managed_tun_inventory_precisely() {
        let mut node = node_with_vpn(vec![direct_leaf("direct")]);
        node.local_ingresses.push(managed_tun("second"));
        assert!(matches!(
            compile_node_linux_vpn_prepare_request(&node),
            Err(LinuxVpnGenerationSpecError::MultipleManagedTunInbounds { actual: 2 })
        ));

        node.local_ingresses.pop();
        let IngressConfig::TunL4(tun) = &mut node.local_ingresses[0].config else {
            panic!("managed TUN");
        };
        tun.dns_resolvers = vec!["1.1.1.1:53".parse().expect("external DNS")];
        let error = compile_node_linux_vpn_prepare_request(&node).expect_err("invalid TUN");
        assert!(matches!(
            error,
            LinuxVpnGenerationSpecError::ManagedTun {
                ingress_index: 0,
                ingress_tag: Some(ref tag),
                source: ManagedVpnCompileError::ExternalDnsResolvers,
            } if tag == "vpn"
        ));
        assert!(error.to_string().contains("vpn"));
    }

    #[test]
    fn compiler_rejects_impossible_mpp_inventory_before_resolution() {
        let mut node = node_with_vpn(vec![mpp_leaf("empty", [])]);
        assert!(matches!(
            compile_node_linux_vpn_prepare_request(&node),
            Err(
                LinuxVpnGenerationSpecError::MppOutboundWithoutCarrierPaths {
                    ref outbound
                }
            ) if outbound == "empty"
        ));

        node.outbounds = vec![mpp_leaf(
            "invalid",
            ["udp://carrier.example:443".parse().expect("path")],
        )];
        let OutboundLeafConfig::Mpp { config, .. } = &mut node.outbounds[0] else {
            panic!("MPP");
        };
        config.paths[0].spec.endpoint.port = 0;
        assert!(matches!(
            compile_node_linux_vpn_prepare_request(&node),
            Err(LinuxVpnGenerationSpecError::InvalidCarrierEndpoint {
                ref outbound,
                path_ordinal: 0,
                ..
            }) if outbound == "invalid"
        ));
    }

    #[test]
    fn compiler_enforces_generation_inventory_bounds() {
        let path = "udp://carrier.example:443"
            .parse::<PathSpec>()
            .expect("path");
        let node = node_with_vpn(vec![mpp_leaf(
            "too-many",
            std::iter::repeat_n(path, MAX_PREPARED_CARRIER_PATHS + 1),
        )]);
        assert!(matches!(
            compile_node_linux_vpn_prepare_request(&node),
            Err(LinuxVpnGenerationSpecError::TooManyCarrierPaths {
                actual,
                maximum: MAX_PREPARED_CARRIER_PATHS,
            }) if actual == MAX_PREPARED_CARRIER_PATHS + 1
        ));

        let proxies = (0..=MAX_NATIVE_ENDPOINTS)
            .map(|index| {
                proxy_leaf(
                    &format!("proxy-{index}"),
                    Endpoint::new(
                        format!("proxy-{index}.example"),
                        u16::try_from(10_000 + index).expect("bounded test port"),
                    )
                    .expect("proxy endpoint"),
                )
            })
            .collect();
        let node = node_with_vpn(proxies);
        assert!(matches!(
            compile_node_linux_vpn_prepare_request(&node),
            Err(LinuxVpnGenerationSpecError::TooManyNativeEndpoints {
                actual,
                maximum: MAX_NATIVE_ENDPOINTS,
            }) if actual == MAX_NATIVE_ENDPOINTS + 1
        ));
    }

    #[test]
    fn compiler_rejects_system_plaintext_invalid_and_precarrier_outbound_dns() {
        let mut node = node_with_vpn(vec![direct_leaf("direct")]);
        node.dns_policy = DnsPolicyConfig::system_default();
        assert!(matches!(
            compile_node_linux_vpn_prepare_request(&node),
            Err(LinuxVpnGenerationSpecError::SystemDnsUnsupported)
        ));

        node.dns_policy = plaintext_dns_policy();
        assert!(matches!(
            compile_node_linux_vpn_prepare_request(&node),
            Err(LinuxVpnGenerationSpecError::EncryptedDnsRequired)
        ));

        node.dns_policy = encrypted_dns_policy();
        node.dns_policy.spec.default_plan = DnsPlanId::parse("missing").expect("missing plan ID");
        assert!(matches!(
            compile_node_linux_vpn_prepare_request(&node),
            Err(LinuxVpnGenerationSpecError::DnsPolicy(_))
        ));

        let dns_outbound = outbound_id("dns-proxy");
        node.dns_policy =
            encrypted_dns_policy_with_egress(DnsEgressSpec::Outbound(dns_outbound.clone()));
        node.outbounds = vec![proxy_leaf(
            dns_outbound.as_str(),
            Endpoint::new("192.0.2.40", 1080).expect("literal DNS proxy"),
        )];
        let request = compile_node_linux_vpn_prepare_request(&node)
            .expect("literal-only routed DNS is bootstrap-safe")
            .expect("managed request");
        assert!(request.prepublication_domains.is_empty());
        assert!(
            request.dns_policy.bootstrap_endpoints().next().is_none(),
            "a routed DNS endpoint must not be leaked into the host bypass"
        );

        node.outbounds = vec![proxy_leaf(
            dns_outbound.as_str(),
            Endpoint::new("dns-proxy.example", 1080).expect("named DNS proxy"),
        )];
        assert!(matches!(
            compile_node_linux_vpn_prepare_request(&node),
            Err(
                LinuxVpnGenerationSpecError::PreCarrierDnsEgressUnsupported {
                    ref upstream,
                    ref outbound,
                }
            ) if upstream == "bootstrap-dot" && outbound == dns_outbound.as_str()
        ));
    }

    #[test]
    fn bootstrap_dns_fails_closed_for_named_outbound_egress() {
        let policy = Arc::new(
            encrypted_dns_policy_with_egress(DnsEgressSpec::Outbound(outbound_id("named")))
                .compile()
                .expect("compiled Product DNS"),
        );
        let domains = [DomainName::parse("carrier.example").expect("carrier domain")];

        assert!(matches!(
            compile_bootstrap_dns(policy, &domains),
            Err(LinuxVpnPrepareError::BootstrapDns(
                DnsRuntimeError::PrepublicationDnsRequiresDirect { .. }
            ))
        ));
    }

    #[tokio::test]
    async fn prepublication_resolution_uses_injected_dns_and_literal_fast_path() {
        let dns = DnsGeneration::from_test_answers(HashMap::from([
            (
                "carrier.example".to_owned(),
                vec!["198.51.100.10".parse().expect("carrier IP")],
            ),
            (
                "proxy.example".to_owned(),
                vec!["203.0.113.20".parse().expect("proxy IP")],
            ),
        ]));
        let carrier_path = LinuxVpnCarrierPath {
            identity: CarrierPathIdentity {
                group_ordinal: 2,
                path_ordinal: 3,
            },
            path: "udp://carrier.example:443".parse().expect("carrier path"),
        };
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);

        let carriers = resolve_carrier_paths(vec![carrier_path], Some(&dns), deadline)
            .await
            .expect("carrier resolution");
        assert_eq!(
            carriers.paths()[0].addresses(),
            &["198.51.100.10:443".parse().expect("carrier socket")]
        );
        let native = resolve_native_endpoints(
            vec![
                Endpoint::new("proxy.example", 8443).expect("proxy"),
                Endpoint::new("192.0.2.30", 9443).expect("literal"),
                Endpoint::new("127.0.0.1", 1080).expect("loopback"),
            ],
            Some(&dns),
            deadline,
        )
        .await
        .expect("native resolution");
        assert_eq!(
            native,
            vec![
                "192.0.2.30".parse::<IpAddr>().expect("literal IP"),
                "203.0.113.20".parse::<IpAddr>().expect("proxy IP"),
            ]
        );

        let literal_carrier = LinuxVpnCarrierPath {
            identity: CarrierPathIdentity {
                group_ordinal: 4,
                path_ordinal: 0,
            },
            path: "tcp://198.51.100.30:443"
                .parse()
                .expect("literal carrier path"),
        };
        let literal_carriers = resolve_carrier_paths(vec![literal_carrier], None, deadline)
            .await
            .expect("literal carrier does not require DNS");
        assert_eq!(
            literal_carriers.paths()[0].addresses(),
            &["198.51.100.30:443".parse().expect("literal socket")]
        );
        assert_eq!(
            resolve_native_endpoints(
                vec![Endpoint::new("192.0.2.31", 1080).expect("literal proxy")],
                None,
                deadline,
            )
            .await
            .expect("literal proxy does not require DNS"),
            vec!["192.0.2.31".parse::<IpAddr>().expect("literal IP")]
        );
    }

    #[test]
    fn linux_vpn_production_source_never_calls_the_system_hostname_resolver() {
        let production_source = include_str!("linux_vpn.rs")
            .split("#[cfg(test)]")
            .next()
            .expect("production source");
        assert!(!production_source.contains("lookup_host"));
    }

    #[test]
    fn host_prepare_failure_retries_residual_rollback_before_returning() {
        let plan = test_plan();
        let prepare_count = plan.prepare_operations().len();
        assert!(prepare_count >= 2);
        let control = InjectedHostHandle::default();
        control.fail_apply_at(prepare_count - 1);
        control.fail_rollback(0, 1);

        let error = match VpnHostLifecycle::prepare(InjectedHostBackend::new(control.clone()), plan)
        {
            Err(error) => error,
            Ok(_) => panic!("injected prepare failure"),
        };
        assert!(matches!(
            error,
            HostPrepareError::Prepare { cleanup: None, .. }
        ));
        assert_eq!(
            control
                .events()
                .iter()
                .filter(|event| **event == InjectedHostEvent::Rollback(0))
                .count(),
            2,
            "prepare rollback residue was not retried before controller drop"
        );
    }

    #[test]
    fn host_device_handoff_failure_cleans_the_inert_prepare() {
        let plan = test_plan();
        let prepare_count = plan.prepare_operations().len();
        let control = InjectedHostHandle::default();
        control.fail_take();

        let error = match VpnHostLifecycle::prepare(InjectedHostBackend::new(control.clone()), plan)
        {
            Err(error) => error,
            Ok(_) => panic!("injected device handoff failure"),
        };
        assert!(matches!(
            error,
            HostPrepareError::Device { cleanup: None, .. }
        ));
        assert_eq!(
            control
                .events()
                .iter()
                .filter(|event| matches!(event, InjectedHostEvent::Rollback(_)))
                .count(),
            prepare_count
        );
    }

    #[test]
    fn host_unpublish_failure_retains_only_failed_publication_for_retry() {
        let plan = test_plan();
        let prepare_count = plan.prepare_operations().len();
        let publish_count = plan.publish_operations().len();
        let last_publish_token = prepare_count + publish_count - 1;
        let control = InjectedHostHandle::default();
        let (mut lifecycle, _device) =
            VpnHostLifecycle::prepare(InjectedHostBackend::new(control.clone()), plan)
                .expect("prepare");
        lifecycle.publish().expect("publish");
        control.fail_rollback(last_publish_token, 1);

        assert!(lifecycle.unpublish().is_err());
        assert_eq!(lifecycle.pending_publish_steps(), 1);
        assert_eq!(lifecycle.state(), LinuxControllerState::CleanupPending);
        lifecycle.unpublish().expect("retry unpublish");
        assert_eq!(lifecycle.pending_publish_steps(), 0);
        assert_eq!(lifecycle.state(), LinuxControllerState::Prepared);
        lifecycle.cleanup().expect("inert prepare cleanup");
    }

    #[test]
    fn cleanup_failure_after_unpublish_cannot_restore_host_publication() {
        let plan = test_plan();
        let prepare_count = plan.prepare_operations().len();
        let control = InjectedHostHandle::default();
        let (mut lifecycle, _device) =
            VpnHostLifecycle::prepare(InjectedHostBackend::new(control.clone()), plan)
                .expect("prepare");
        lifecycle.publish().expect("publish");
        lifecycle.unpublish().expect("unpublish");
        control.fail_rollback(prepare_count - 1, 1);

        assert!(lifecycle.cleanup().is_err());
        assert_eq!(lifecycle.pending_publish_steps(), 0);
        assert_eq!(lifecycle.state(), LinuxControllerState::CleanupPending);
        lifecycle.cleanup().expect("retry inert cleanup");
        assert_eq!(lifecycle.state(), LinuxControllerState::Idle);
    }

    #[test]
    fn host_lifecycle_preserves_prepare_publish_unpublish_cleanup_order() {
        let (mut lifecycle, device) =
            VpnHostLifecycle::prepare(FakeBackend::default(), test_plan()).expect("prepare");
        assert_eq!(device, 7);
        assert_eq!(lifecycle.state(), LinuxControllerState::Prepared);
        assert!(
            lifecycle
                .controller
                .backend()
                .applied
                .iter()
                .all(|operation| {
                    !matches!(
                        operation,
                        LinuxHostOperation::ActivateNativeEgressRule { .. }
                            | LinuxHostOperation::ActivateCaptureRule { .. }
                            | LinuxHostOperation::ConfigureDns { .. }
                    )
                })
        );

        lifecycle.publish().expect("publish");
        assert_eq!(lifecycle.state(), LinuxControllerState::Active);
        lifecycle.unpublish().expect("unpublish");
        assert_eq!(lifecycle.state(), LinuxControllerState::Prepared);
        lifecycle.cleanup().expect("cleanup");
        assert_eq!(lifecycle.state(), LinuxControllerState::Idle);

        let backend = lifecycle.controller.backend();
        let first_publish = backend
            .applied
            .iter()
            .position(|operation| {
                matches!(
                    operation,
                    LinuxHostOperation::ActivateNativeEgressRule { .. }
                )
            })
            .expect("native publish");
        let first_capture = backend
            .applied
            .iter()
            .position(|operation| {
                matches!(operation, LinuxHostOperation::ActivateCaptureRule { .. })
            })
            .expect("capture publish");
        assert!(first_publish < first_capture);
        assert_eq!(
            backend.rolled_back.first(),
            backend.applied.iter().rfind(|operation| {
                matches!(operation, LinuxHostOperation::ActivateCaptureRule { .. })
            })
        );
        assert_eq!(
            backend.rolled_back.last(),
            backend.applied.first(),
            "TUN creation is the final cleanup operation"
        );
    }
}
