//! Executable Windows managed-VPN generation bridge.
//!
//! This adapter joins the Wintun packet factory, immutable native-route
//! snapshot, exact two-phase route/DNS transaction, and per-socket native
//! interface binding. It is called only at runtime-generation boundaries.

use super::managed_vpn::{
    MAX_NATIVE_ENDPOINTS, MAX_PREPARED_CARRIER_PATHS, ManagedVpnCarrierPath,
    ManagedVpnGenerationSpec, ManagedVpnGenerationSpecError, compile_managed_vpn_generation_spec,
};
use super::packet_device::{ManagedPacketDeviceProvider, PacketDeviceProvider};
use crate::config::{AppConfig, CommandConfig, NodeConfig};
use crate::dns::{DnsGeneration, DnsRuntimeError};
use crate::platform::{
    ManagedVpnConfig, ProcessCleanupError, ProcessControllerState, ProcessPrepareError,
    ProcessPublishError, ProcessVpnPlan, ProcessVpnPlanError, SystemProcessHostNetworkBackend,
    SystemProcessMutationError, TransactionalProcessVpnController, WindowsNativeSocketBinder,
    WindowsWintunConfig, WindowsWintunConfigError, WindowsWintunCreateError,
    WindowsWintunDeviceFactory, snapshot_process_vpn_environment,
};
use crate::product::{CompiledDnsPolicy, DomainName};
use crate::transport::{
    CarrierNetworkProvider, Endpoint, NativeSocketConfigurator, PreparedCarrierNetworkProvider,
    PreparedCarrierPath, ProtectedCarrierNetworkProvider, ProtectedNativeSocketConfigurator,
};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use std::fmt;
use std::io;
use std::net::IpAddr;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

const WINTUN_TUNNEL_TYPE: &str = "MPTUNNEL";
const WINTUN_DLL_NAME: &str = "wintun.dll";

pub(crate) struct WindowsVpnPrepareRequest {
    managed: ManagedVpnConfig,
    managed_tun_count: usize,
    interface_name: String,
    carrier_paths: Vec<ManagedVpnCarrierPath>,
    native_proxy_endpoints: Vec<Endpoint>,
    prepublication_domains: Vec<DomainName>,
    dns_policy: Arc<CompiledDnsPolicy>,
    resolution_timeout: Duration,
}

pub(crate) fn compile_windows_vpn_prepare_request(
    config: &AppConfig,
) -> Result<Option<WindowsVpnPrepareRequest>, WindowsVpnGenerationSpecError> {
    let CommandConfig::Node(node) = &config.command;
    compile_node_windows_vpn_prepare_request(node)
}

fn compile_node_windows_vpn_prepare_request(
    node: &NodeConfig,
) -> Result<Option<WindowsVpnPrepareRequest>, WindowsVpnGenerationSpecError> {
    let Some(spec) = compile_managed_vpn_generation_spec(node)
        .map_err(WindowsVpnGenerationSpecError::Portable)?
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
        resolution_timeout,
    } = spec;
    // Portable configs may carry Linux tuning for use by the same file on a
    // different host. Windows deliberately consumes and ignores that tuning.
    drop(platform);
    validate_wintun_name(&interface_name).map_err(|source| {
        WindowsVpnGenerationSpecError::Wintun {
            ingress_index,
            ingress_name,
            source,
        }
    })?;
    Ok(Some(WindowsVpnPrepareRequest {
        managed,
        managed_tun_count,
        interface_name,
        carrier_paths,
        native_proxy_endpoints,
        prepublication_domains,
        dns_policy,
        resolution_timeout,
    }))
}

fn validate_wintun_name(name: &str) -> Result<(), WindowsWintunConfigError> {
    WindowsWintunConfig::new(name, WINTUN_TUNNEL_TYPE, 1, WINTUN_DLL_NAME).map(drop)
}

#[derive(Debug)]
pub(crate) enum WindowsVpnGenerationSpecError {
    Portable(ManagedVpnGenerationSpecError),
    Wintun {
        ingress_index: usize,
        ingress_name: String,
        source: WindowsWintunConfigError,
    },
}

impl fmt::Display for WindowsVpnGenerationSpecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Portable(error) => fmt::Display::fmt(error, formatter),
            Self::Wintun {
                ingress_index,
                ingress_name,
                source,
            } => write!(
                formatter,
                "managed TUN inbound {ingress_name} at index {ingress_index} has an invalid Wintun identity: {source}"
            ),
        }
    }
}

impl std::error::Error for WindowsVpnGenerationSpecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Portable(error) => Some(error),
            Self::Wintun { source, .. } => Some(source),
        }
    }
}

pub(crate) struct PreparedWindowsVpn {
    host: WindowsVpnHostLifecycle<SystemProcessHostNetworkBackend>,
    packet_devices: Arc<ManagedPacketDeviceProvider>,
    worker_ready: Option<oneshot::Receiver<()>>,
    carrier_network: Arc<dyn CarrierNetworkProvider>,
    native_sockets: Arc<dyn NativeSocketConfigurator>,
}

impl PreparedWindowsVpn {
    pub(crate) fn packet_device_provider(&self) -> Arc<dyn PacketDeviceProvider> {
        self.packet_devices.clone()
    }

    pub(crate) fn carrier_network_provider(&self) -> Arc<dyn CarrierNetworkProvider> {
        self.carrier_network.clone()
    }

    pub(crate) fn native_socket_configurator(&self) -> Arc<dyn NativeSocketConfigurator> {
        self.native_sockets.clone()
    }

    pub(crate) async fn publish_when_worker_ready(
        &mut self,
        ready_timeout: Duration,
    ) -> Result<(), WindowsVpnPublishError> {
        let receiver = self
            .worker_ready
            .as_mut()
            .ok_or(WindowsVpnPublishError::ReadinessAlreadyConsumed)?;
        match tokio::time::timeout(ready_timeout, receiver).await {
            Err(_) => return Err(WindowsVpnPublishError::WorkerReadyTimeout(ready_timeout)),
            Ok(Err(_)) => {
                self.worker_ready.take();
                return Err(WindowsVpnPublishError::WorkerExitedBeforeReady);
            }
            Ok(Ok(())) => {
                self.worker_ready.take();
            }
        }
        if !self.packet_devices.device_live() {
            return Err(WindowsVpnPublishError::WorkerExitedBeforeReady);
        }
        self.host
            .publish()
            .map_err(|error| WindowsVpnPublishError::Publish(Box::new(error)))?;
        if !self.packet_devices.device_live() {
            let _ = self.host.unpublish();
            return Err(WindowsVpnPublishError::WorkerExitedDuringPublish);
        }
        Ok(())
    }

    pub(crate) async fn unpublish(
        &mut self,
        attempts: NonZeroUsize,
        retry_delay: Duration,
    ) -> Result<(), WindowsVpnShutdownError> {
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
            return Err(WindowsVpnShutdownError::Unpublish(Box::new(
                last_unpublish.expect("pending publication has a rollback error"),
            )));
        }
        Ok(())
    }

    pub(crate) async fn cleanup_after_worker_stopped(
        &mut self,
        attempts: NonZeroUsize,
        retry_delay: Duration,
    ) -> Result<(), WindowsVpnShutdownError> {
        let pending_publish_steps = self.host.pending_publish_steps();
        if pending_publish_steps != 0 {
            return Err(WindowsVpnShutdownError::PublicationStillActive {
                pending_steps: pending_publish_steps,
            });
        }
        self.packet_devices.discard_unopened_device();
        if self.packet_devices.device_live() {
            return Err(WindowsVpnShutdownError::PacketWorkerStillRunning);
        }

        let mut last_cleanup = None;
        for attempt in 0..attempts.get() {
            if self.host.state() == ProcessControllerState::Idle {
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
        Err(WindowsVpnShutdownError::Cleanup(Box::new(
            last_cleanup.expect("non-idle host transaction has a cleanup error"),
        )))
    }
}

pub(crate) async fn prepare_windows_vpn(
    request: WindowsVpnPrepareRequest,
) -> Result<PreparedWindowsVpn, WindowsVpnPrepareError> {
    validate_prepare_request(&request)?;
    let bootstrap_dns = if request.prepublication_domains.is_empty() {
        None
    } else {
        Some(
            DnsGeneration::compile_prepublication(
                request.dns_policy.clone(),
                &request.prepublication_domains,
            )
            .map_err(WindowsVpnPrepareError::BootstrapDns)?,
        )
    };

    // Snapshot before Wintun exists so native route selection cannot observe
    // or accidentally reuse the new capture interface.
    let environment = Arc::new(
        snapshot_process_vpn_environment()
            .map_err(|error| WindowsVpnPrepareError::Snapshot(Box::new(error)))?,
    );
    let deadline = tokio::time::Instant::now() + request.resolution_timeout;
    let prepared_carriers =
        resolve_carrier_paths(request.carrier_paths, bootstrap_dns.as_ref(), deadline).await?;
    let native_proxy_addresses = resolve_native_endpoints(
        request.native_proxy_endpoints,
        bootstrap_dns.as_ref(),
        deadline,
    )
    .await?;
    let bootstrap_dns_addresses = request
        .dns_policy
        .bootstrap_endpoints()
        .map(|endpoint| endpoint.ip())
        .collect::<Vec<_>>();

    let mut native_endpoints = prepared_carriers.endpoint_addresses();
    native_endpoints.extend(native_proxy_addresses);
    native_endpoints.sort_unstable();
    native_endpoints.dedup();

    let wintun = WindowsWintunConfig::new(
        request.interface_name,
        WINTUN_TUNNEL_TYPE,
        random_generation_guid()?,
        packaged_wintun_path()?,
    )
    .map_err(WindowsVpnPrepareError::WintunConfig)?;
    let prepared_device = WindowsWintunDeviceFactory::create(&wintun, &request.managed)
        .map_err(WindowsVpnPrepareError::Wintun)?;
    let plan = ProcessVpnPlan::build(
        &request.managed,
        &environment,
        prepared_device.interface_index(),
        native_endpoints,
        bootstrap_dns_addresses,
    )
    .map_err(WindowsVpnPrepareError::Plan)?;
    let backend = SystemProcessHostNetworkBackend::new(prepared_device.device())
        .map_err(|error| WindowsVpnPrepareError::Backend(Box::new(error)))?;
    let host = WindowsVpnHostLifecycle::prepare(backend, plan).map_err(|error| {
        WindowsVpnPrepareError::Prepare {
            source: Box::new(error.source),
            cleanup: error.cleanup.map(Box::new),
        }
    })?;

    let (packet_devices, worker_ready) =
        ManagedPacketDeviceProvider::new(prepared_device.into_device());
    let protector = Arc::new(WindowsNativeSocketBinder::new(environment));
    let prepared_carriers: Arc<dyn CarrierNetworkProvider> = Arc::new(prepared_carriers);
    let carrier_network: Arc<dyn CarrierNetworkProvider> = Arc::new(
        ProtectedCarrierNetworkProvider::new(prepared_carriers, protector.clone()),
    );
    let native_sockets: Arc<dyn NativeSocketConfigurator> =
        Arc::new(ProtectedNativeSocketConfigurator::new(protector));
    Ok(PreparedWindowsVpn {
        host,
        packet_devices,
        worker_ready: Some(worker_ready),
        carrier_network,
        native_sockets,
    })
}

fn validate_prepare_request(
    request: &WindowsVpnPrepareRequest,
) -> Result<(), WindowsVpnPrepareError> {
    if request.managed_tun_count != 1 {
        return Err(WindowsVpnPrepareError::ManagedTunCount(
            request.managed_tun_count,
        ));
    }
    if request.resolution_timeout.is_zero() {
        return Err(WindowsVpnPrepareError::ResolutionTimeoutZero);
    }
    if request.carrier_paths.len() > MAX_PREPARED_CARRIER_PATHS {
        return Err(WindowsVpnPrepareError::TooManyCarrierPaths {
            actual: request.carrier_paths.len(),
            maximum: MAX_PREPARED_CARRIER_PATHS,
        });
    }
    if request.native_proxy_endpoints.len() > MAX_NATIVE_ENDPOINTS {
        return Err(WindowsVpnPrepareError::TooManyNativeEndpoints {
            actual: request.native_proxy_endpoints.len(),
            maximum: MAX_NATIVE_ENDPOINTS,
        });
    }
    if request.dns_policy.uses_system_resolution() {
        return Err(WindowsVpnPrepareError::SystemDnsUnsupported);
    }
    if !request.dns_policy.is_encrypted_only() {
        return Err(WindowsVpnPrepareError::EncryptedDnsRequired);
    }
    if request.managed.dns().is_none()
        && matches!(
            request.managed.route_mode(),
            crate::platform::RouteMode::Full
        )
    {
        return Err(WindowsVpnPrepareError::FullTunnelDnsCaptureRequired);
    }
    Ok(())
}

fn packaged_wintun_path() -> Result<PathBuf, WindowsVpnPrepareError> {
    let executable = std::env::current_exe().map_err(WindowsVpnPrepareError::CurrentExecutable)?;
    let directory = executable
        .parent()
        .ok_or_else(|| WindowsVpnPrepareError::ExecutableWithoutParent(executable.clone()))?;
    Ok(directory.join(WINTUN_DLL_NAME))
}

fn random_generation_guid() -> Result<u128, WindowsVpnPrepareError> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes).map_err(WindowsVpnPrepareError::Random)?;
    // RFC 4122 variant/version bits keep diagnostics conventional. Wintun
    // consumes the value as a stable 128-bit adapter identity.
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Ok(u128::from_be_bytes(bytes))
}

async fn resolve_carrier_paths(
    requests: Vec<ManagedVpnCarrierPath>,
    dns: Option<&DnsGeneration>,
    deadline: tokio::time::Instant,
) -> Result<PreparedCarrierNetworkProvider, WindowsVpnPrepareError> {
    let mut resolutions = FuturesUnordered::new();
    for request in requests {
        let dns = dns.cloned();
        resolutions.push(async move {
            let authority = request.path.endpoint.authority();
            let bootstrap_port = request.path.endpoint.ports().first();
            let addresses = match request.path.endpoint.host.parse::<IpAddr>() {
                Ok(address) => vec![std::net::SocketAddr::new(address, bootstrap_port)],
                Err(_) => {
                    let dns = dns.ok_or_else(|| WindowsVpnPrepareError::DnsResolution {
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
                            bootstrap_port,
                        ),
                    )
                    .await
                    .map_err(|_| WindowsVpnPrepareError::ResolutionTimedOut(authority.clone()))?
                    .map_err(|source| {
                        WindowsVpnPrepareError::DnsResolution {
                            endpoint: authority.clone(),
                            source,
                        }
                    })?
                }
            };
            PreparedCarrierPath::new(request.identity, request.path, addresses).map_err(|source| {
                WindowsVpnPrepareError::Resolution {
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
    PreparedCarrierNetworkProvider::new(prepared).map_err(WindowsVpnPrepareError::Inventory)
}

async fn resolve_native_endpoints(
    endpoints: Vec<Endpoint>,
    dns: Option<&DnsGeneration>,
    deadline: tokio::time::Instant,
) -> Result<Vec<IpAddr>, WindowsVpnPrepareError> {
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
                    let dns = dns.ok_or_else(|| WindowsVpnPrepareError::DnsResolution {
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
                    .map_err(|_| WindowsVpnPrepareError::ResolutionTimedOut(authority.clone()))?
                    .map_err(|source| WindowsVpnPrepareError::DnsResolution {
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
    addresses.retain(|address| !address.is_loopback());
    addresses.sort_unstable();
    addresses.dedup();
    Ok(addresses)
}

struct WindowsVpnHostLifecycle<Backend>
where
    Backend: crate::platform::ProcessHostMutationBackend,
{
    controller: TransactionalProcessVpnController<Backend>,
}

impl<Backend> WindowsVpnHostLifecycle<Backend>
where
    Backend: crate::platform::ProcessHostMutationBackend,
{
    fn prepare(
        backend: Backend,
        plan: ProcessVpnPlan,
    ) -> Result<Self, WindowsHostPrepareError<Backend::Error>> {
        let mut controller = TransactionalProcessVpnController::new(backend);
        if let Err(source) = controller.prepare(plan) {
            let cleanup = (controller.state() != ProcessControllerState::Idle)
                .then(|| controller.cleanup().err())
                .flatten();
            return Err(WindowsHostPrepareError { source, cleanup });
        }
        Ok(Self { controller })
    }

    fn state(&self) -> ProcessControllerState {
        self.controller.state()
    }

    fn pending_publish_steps(&self) -> usize {
        self.controller.pending_publish_steps()
    }

    fn publish(&mut self) -> Result<(), ProcessPublishError<Backend::Error>> {
        self.controller.publish()
    }

    fn unpublish(&mut self) -> Result<(), ProcessCleanupError<Backend::Error>> {
        self.controller.unpublish().map(|_| ())
    }

    fn cleanup(&mut self) -> Result<(), ProcessCleanupError<Backend::Error>> {
        self.controller.cleanup().map(|_| ())
    }
}

struct WindowsHostPrepareError<Error> {
    source: ProcessPrepareError<Error>,
    cleanup: Option<ProcessCleanupError<Error>>,
}

#[derive(Debug)]
pub(crate) enum WindowsVpnPrepareError {
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
    Snapshot(Box<SystemProcessMutationError>),
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
    CurrentExecutable(io::Error),
    ExecutableWithoutParent(PathBuf),
    Random(getrandom::Error),
    WintunConfig(WindowsWintunConfigError),
    Wintun(WindowsWintunCreateError),
    Plan(ProcessVpnPlanError),
    Backend(Box<SystemProcessMutationError>),
    Prepare {
        source: Box<ProcessPrepareError<SystemProcessMutationError>>,
        cleanup: Option<Box<ProcessCleanupError<SystemProcessMutationError>>>,
    },
}

impl fmt::Display for WindowsVpnPrepareError {
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
                write!(
                    formatter,
                    "managed VPN has {actual} carrier paths; maximum is {maximum}"
                )
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
            Self::DnsResolution { endpoint, source } => write!(
                formatter,
                "pre-VPN encrypted DNS failed for {endpoint}: {source}"
            ),
            Self::Resolution { endpoint, source } => {
                write!(
                    formatter,
                    "pre-VPN resolution failed for {endpoint}: {source}"
                )
            }
            Self::Inventory(error) => write!(formatter, "invalid carrier inventory: {error}"),
            Self::CurrentExecutable(error) => {
                write!(formatter, "failed to locate the Windows executable: {error}")
            }
            Self::ExecutableWithoutParent(path) => write!(
                formatter,
                "Windows executable path has no package directory: {}",
                path.display()
            ),
            Self::Random(error) => {
                write!(formatter, "failed to create a Wintun generation identity: {error}")
            }
            Self::WintunConfig(error) => write!(formatter, "invalid Wintun config: {error}"),
            Self::Wintun(error) => write!(formatter, "failed to create Wintun: {error}"),
            Self::Plan(error) => write!(formatter, "failed to plan managed VPN: {error}"),
            Self::Backend(error) => {
                write!(formatter, "failed to open Windows host-network backend: {error}")
            }
            Self::Prepare { source, cleanup } => {
                write!(formatter, "failed to prepare managed VPN: {source}")?;
                if let Some(cleanup) = cleanup {
                    write!(
                        formatter,
                        "; residual prepare cleanup also failed: {cleanup}"
                    )?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for WindowsVpnPrepareError {}

#[derive(Debug)]
pub(crate) enum WindowsVpnPublishError {
    ReadinessAlreadyConsumed,
    WorkerReadyTimeout(Duration),
    WorkerExitedBeforeReady,
    WorkerExitedDuringPublish,
    Publish(Box<ProcessPublishError<SystemProcessMutationError>>),
}

impl fmt::Display for WindowsVpnPublishError {
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

impl std::error::Error for WindowsVpnPublishError {}

#[derive(Debug)]
pub(crate) enum WindowsVpnShutdownError {
    Unpublish(Box<ProcessCleanupError<SystemProcessMutationError>>),
    PublicationStillActive { pending_steps: usize },
    PacketWorkerStillRunning,
    Cleanup(Box<ProcessCleanupError<SystemProcessMutationError>>),
}

impl fmt::Display for WindowsVpnShutdownError {
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
                .write_str("managed VPN packet worker still owns Wintun after stop completed"),
            Self::Cleanup(error) => write!(formatter, "failed to clean managed VPN: {error}"),
        }
    }
}

impl std::error::Error for WindowsVpnShutdownError {}

#[cfg(test)]
#[path = "tests_windows_vpn.rs"]
mod tests;
