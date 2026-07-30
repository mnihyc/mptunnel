//! Process composition for client, server, and combined-node roles.
//!
//! Node code starts long-lived services. Carrier state and scheduling policy
//! stay in their owning `path`, `stream`, and `sender` modules.

mod client;
mod combined;
pub(super) mod server;

#[cfg(test)]
pub(in crate::runtime) use client::{probe_paths, run_path_probe_service};

use crate::config::{AppConfig, CommandConfig};
use crate::dns::DnsGeneration;
use crate::platform::{PacketDeviceProvider, SystemPacketDeviceProvider};
use crate::runtime::config_control::RuntimeConfigControl;
use crate::runtime::error::RuntimeError;
use crate::runtime::readiness::{RuntimeGenerationControl, RuntimeGenerationStopReason};
use crate::transport::{
    CarrierNetworkProvider, CarrierResolutionFuture, CarrierResolutionRequest,
    CarrierSocketRequest, HostSocketProtector, NativeSocketConfigurator,
    ProtectedCarrierNetworkProvider, ProtectedNativeSocketConfigurator,
    SystemCarrierNetworkProvider, SystemNativeSocketConfigurator,
};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, OnceLock};
use tokio::task::JoinSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CarrierResolutionAuthority {
    /// Ordinary process mode resolves carrier names through Product DNS.
    ProductDns,
    /// Embedded and managed hosts retain authority over resolution and sockets.
    Host,
}

struct ProductDnsCarrierNetworkProvider {
    socket_provider: Arc<dyn CarrierNetworkProvider>,
    dns: OnceLock<DnsGeneration>,
}

impl ProductDnsCarrierNetworkProvider {
    fn new(socket_provider: Arc<dyn CarrierNetworkProvider>) -> Self {
        Self {
            socket_provider,
            dns: OnceLock::new(),
        }
    }

    fn install(&self, dns: DnsGeneration) -> Result<(), RuntimeError> {
        self.dns.set(dns).map_err(|_| {
            RuntimeError::Protocol(
                "Product DNS was installed more than once for one runtime generation",
            )
        })
    }
}

impl CarrierNetworkProvider for ProductDnsCarrierNetworkProvider {
    fn resolve<'a>(&'a self, request: CarrierResolutionRequest<'a>) -> CarrierResolutionFuture<'a> {
        Box::pin(async move {
            let endpoint = &request.path.endpoint;
            request.validate()?;
            if let Ok(address) = endpoint.host.parse::<IpAddr>() {
                return Ok(vec![SocketAddr::new(address, request.remote_port)]);
            }
            let dns = self.dns.get().cloned().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotConnected,
                    "Product DNS is not installed for this runtime generation",
                )
            })?;
            let addresses = dns
                .resolve_socket_addrs(&endpoint.host, request.remote_port)
                .await
                .map_err(|error| {
                    std::io::Error::new(
                        std::io::ErrorKind::AddrNotAvailable,
                        format!(
                            "Product DNS could not resolve carrier {}: {error}",
                            endpoint.authority()
                        ),
                    )
                })?;
            if addresses.is_empty() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AddrNotAvailable,
                    format!(
                        "Product DNS returned no addresses for carrier {}",
                        endpoint.authority()
                    ),
                ));
            }
            Ok(addresses)
        })
    }

    fn create_socket(
        &self,
        request: CarrierSocketRequest<'_>,
    ) -> std::io::Result<crate::transport::CarrierSocket> {
        self.socket_provider.create_socket(request)
    }
}

struct GenerationCarrierNetwork {
    provider: Arc<dyn CarrierNetworkProvider>,
    product_dns: Option<Arc<ProductDnsCarrierNetworkProvider>>,
}

impl GenerationCarrierNetwork {
    fn new(
        provider: Arc<dyn CarrierNetworkProvider>,
        authority: CarrierResolutionAuthority,
    ) -> Self {
        match authority {
            CarrierResolutionAuthority::ProductDns => {
                let product_dns = Arc::new(ProductDnsCarrierNetworkProvider::new(provider));
                Self {
                    provider: product_dns.clone(),
                    product_dns: Some(product_dns),
                }
            }
            CarrierResolutionAuthority::Host => Self {
                provider,
                product_dns: None,
            },
        }
    }

    fn install_product_dns(&self, dns: DnsGeneration) -> Result<(), RuntimeError> {
        match &self.product_dns {
            Some(provider) => provider.install(dns),
            None => Ok(()),
        }
    }
}

/// Runs the low-level portable runtime without process-managed host mutation.
///
/// TUN ingresses must use external host ownership. The MPTUNNEL application
/// lifecycle, not this entry point, owns built-in Linux/Windows managed VPN.
pub async fn run(config: AppConfig) -> Result<(), RuntimeError> {
    run_public_generation(
        config,
        Arc::new(SystemPacketDeviceProvider),
        Arc::new(SystemCarrierNetworkProvider),
        CarrierResolutionAuthority::ProductDns,
        Arc::new(SystemNativeSocketConfigurator),
    )
    .await
}

pub(crate) async fn run_with_generation_control(
    config: AppConfig,
    generation: RuntimeGenerationControl,
) -> RuntimeGenerationOutcome {
    run_with_generation_and_host_providers(
        config,
        Arc::new(SystemPacketDeviceProvider),
        Arc::new(SystemCarrierNetworkProvider),
        CarrierResolutionAuthority::ProductDns,
        Arc::new(SystemNativeSocketConfigurator),
        None,
        generation,
    )
    .await
}

pub(crate) async fn run_with_config_control(
    config: AppConfig,
    config_control: RuntimeConfigControl,
) -> RuntimeGenerationOutcome {
    let generation = config_control.generation();
    run_with_generation_and_host_providers(
        config,
        Arc::new(SystemPacketDeviceProvider),
        Arc::new(SystemCarrierNetworkProvider),
        CarrierResolutionAuthority::ProductDns,
        Arc::new(SystemNativeSocketConfigurator),
        Some(config_control),
        generation,
    )
    .await
}

pub(crate) async fn run_with_all_host_providers_and_config_control(
    config: AppConfig,
    packet_devices: Arc<dyn PacketDeviceProvider>,
    carrier_network: Arc<dyn CarrierNetworkProvider>,
    native_sockets: Arc<dyn NativeSocketConfigurator>,
    config_control: RuntimeConfigControl,
) -> RuntimeGenerationOutcome {
    let generation = config_control.generation();
    run_with_generation_and_host_providers(
        config,
        packet_devices,
        carrier_network,
        CarrierResolutionAuthority::Host,
        native_sockets,
        Some(config_control),
        generation,
    )
    .await
}

pub(crate) async fn run_with_all_host_providers_and_generation_control(
    config: AppConfig,
    packet_devices: Arc<dyn PacketDeviceProvider>,
    carrier_network: Arc<dyn CarrierNetworkProvider>,
    native_sockets: Arc<dyn NativeSocketConfigurator>,
    generation: RuntimeGenerationControl,
) -> RuntimeGenerationOutcome {
    run_with_generation_and_host_providers(
        config,
        packet_devices,
        carrier_network,
        CarrierResolutionAuthority::Host,
        native_sockets,
        None,
        generation,
    )
    .await
}

/// Runs a process with host-controlled packet-device construction.
///
/// Hosts that only customize packet-device construction use this entry point.
/// Catch-all embedded VPNs must use [`run_with_vpn_host_providers`] so every
/// carrier, target, proxy, and DNS socket is protected. TUN configuration must
/// declare external host ownership.
pub async fn run_with_packet_device_provider(
    config: AppConfig,
    packet_devices: Arc<dyn PacketDeviceProvider>,
) -> Result<(), RuntimeError> {
    run_public_generation(
        config,
        packet_devices,
        Arc::new(SystemCarrierNetworkProvider),
        CarrierResolutionAuthority::ProductDns,
        Arc::new(SystemNativeSocketConfigurator),
    )
    .await
}

/// Runs a process with packet-device and carrier-network host adapters.
///
/// This does not protect native target/proxy/DNS sockets and is therefore not
/// sufficient for a catch-all embedded VPN. TUN configuration must declare
/// external host ownership.
pub async fn run_with_host_providers(
    config: AppConfig,
    packet_devices: Arc<dyn PacketDeviceProvider>,
    carrier_network: Arc<dyn CarrierNetworkProvider>,
) -> Result<(), RuntimeError> {
    run_public_generation(
        config,
        packet_devices,
        carrier_network,
        CarrierResolutionAuthority::Host,
        Arc::new(SystemNativeSocketConfigurator),
    )
    .await
}

/// Runs an embedded VPN with one fail-closed socket-protection callback.
///
/// `carrier_network` still owns native-network resolution and socket
/// construction; it must return sockets without invoking `socket_protector`
/// itself. MPTunnel wraps it so the same callback is invoked exactly once for
/// every MPP carrier and every MPTunnel-created native target/proxy/DNS TCP/UDP
/// socket, after optional source binding and before connect/first send.
/// Operating-system DNS is rejected before host adapters are started because
/// its resolver sockets cannot be passed through this callback.
/// Android callbacks normally invoke `VpnService.protect` synchronously using
/// the borrowed descriptor. Apple packet-tunnel hosts can apply their
/// equivalent native-network exclusion without taking socket ownership.
/// The host must publish its own addresses/routes/DNS and declare external TUN
/// ownership; process-managed TUN belongs to the application lifecycle.
pub async fn run_with_vpn_host_providers(
    config: AppConfig,
    packet_devices: Arc<dyn PacketDeviceProvider>,
    carrier_network: Arc<dyn CarrierNetworkProvider>,
    socket_protector: Arc<dyn HostSocketProtector>,
) -> Result<(), RuntimeError> {
    require_external_tun_host(&config)?;
    require_protectable_vpn_dns(&config)?;
    let carrier_network: Arc<dyn CarrierNetworkProvider> = Arc::new(
        ProtectedCarrierNetworkProvider::new(carrier_network, socket_protector.clone()),
    );
    let native_sockets: Arc<dyn NativeSocketConfigurator> =
        Arc::new(ProtectedNativeSocketConfigurator::new(socket_protector));
    run_validated_public_generation(
        config,
        packet_devices,
        carrier_network,
        CarrierResolutionAuthority::Host,
        native_sockets,
    )
    .await
}

/// Runs with independent host adapters for packet devices, MPP carriers, and
/// native target/proxy/DNS sockets. Embedded VPNs should prefer
/// [`run_with_vpn_host_providers`], which derives both socket adapters from one
/// callback and cannot accidentally leave one egress class unprotected. TUN
/// configuration must declare external host ownership.
pub async fn run_with_all_host_providers(
    config: AppConfig,
    packet_devices: Arc<dyn PacketDeviceProvider>,
    carrier_network: Arc<dyn CarrierNetworkProvider>,
    native_sockets: Arc<dyn NativeSocketConfigurator>,
) -> Result<(), RuntimeError> {
    run_public_generation(
        config,
        packet_devices,
        carrier_network,
        CarrierResolutionAuthority::Host,
        native_sockets,
    )
    .await
}

const PUBLIC_RUNTIME_MANAGED_VPN_ERROR: &str = "low-level runtime entry points accept only external TUN host ownership; process-managed VPN must use the application lifecycle, while Android/macOS VPN hosts must own OS route/DNS publication";
const VPN_HOST_SYSTEM_DNS_ERROR: &str = "catch-all embedded VPN cannot use operating-system DNS because its resolver sockets cannot be passed to HostSocketProtector; configure literal-bootstrap or outbound-backed DNS";

async fn run_public_generation(
    config: AppConfig,
    packet_devices: Arc<dyn PacketDeviceProvider>,
    carrier_network: Arc<dyn CarrierNetworkProvider>,
    carrier_resolution_authority: CarrierResolutionAuthority,
    native_sockets: Arc<dyn NativeSocketConfigurator>,
) -> Result<(), RuntimeError> {
    require_external_tun_host(&config)?;
    run_validated_public_generation(
        config,
        packet_devices,
        carrier_network,
        carrier_resolution_authority,
        native_sockets,
    )
    .await
}

async fn run_validated_public_generation(
    config: AppConfig,
    packet_devices: Arc<dyn PacketDeviceProvider>,
    carrier_network: Arc<dyn CarrierNetworkProvider>,
    carrier_resolution_authority: CarrierResolutionAuthority,
    native_sockets: Arc<dyn NativeSocketConfigurator>,
) -> Result<(), RuntimeError> {
    public_runtime_result(
        run_with_generation_and_host_providers(
            config,
            packet_devices,
            carrier_network,
            carrier_resolution_authority,
            native_sockets,
            None,
            RuntimeGenerationControl::new(),
        )
        .await,
    )
}

fn require_external_tun_host(config: &AppConfig) -> Result<(), RuntimeError> {
    let CommandConfig::Node(node) = &config.command;
    if node.local_ingresses.iter().any(|ingress| {
        matches!(
            &ingress.config,
            crate::ingress::IngressConfig::TunL4(tun) if tun.managed_vpn().is_some()
        )
    }) {
        return Err(RuntimeError::Protocol(PUBLIC_RUNTIME_MANAGED_VPN_ERROR));
    }
    Ok(())
}

fn require_protectable_vpn_dns(config: &AppConfig) -> Result<(), RuntimeError> {
    let CommandConfig::Node(node) = &config.command;
    let dns_policy = node.dns_policy.compile().map_err(|error| {
        RuntimeError::ProductPolicy(format!(
            "invalid DNS policy for catch-all embedded VPN: {error}"
        ))
    })?;
    if dns_policy.uses_system_resolution() {
        return Err(RuntimeError::ProductPolicy(
            VPN_HOST_SYSTEM_DNS_ERROR.to_string(),
        ));
    }
    Ok(())
}

async fn run_with_generation_and_host_providers(
    config: AppConfig,
    packet_devices: Arc<dyn PacketDeviceProvider>,
    carrier_network: Arc<dyn CarrierNetworkProvider>,
    carrier_resolution_authority: CarrierResolutionAuthority,
    native_sockets: Arc<dyn NativeSocketConfigurator>,
    config_control: Option<RuntimeConfigControl>,
    generation: RuntimeGenerationControl,
) -> RuntimeGenerationOutcome {
    let CommandConfig::Node(node) = config.command;
    let carrier_network =
        GenerationCarrierNetwork::new(carrier_network, carrier_resolution_authority);
    let result = combined::run(
        node,
        combined::NodeRuntimeEnvironment {
            resources: config.resources,
            admission: config.admission,
            session: config.session,
            management: config.management,
            packet_devices,
            carrier_network,
            native_sockets,
            config_control,
            generation: generation.clone(),
        },
    )
    .await;
    match result {
        Ok(RuntimeGenerationStopReason::ReloadRequested) => {
            RuntimeGenerationOutcome::ReloadRequested
        }
        Ok(RuntimeGenerationStopReason::ShutdownRequested) => {
            RuntimeGenerationOutcome::ShutdownRequested
        }
        Err(error) => {
            generation.mark_failed(error.to_string());
            RuntimeGenerationOutcome::Failed(error)
        }
    }
}

fn public_runtime_result(outcome: RuntimeGenerationOutcome) -> Result<(), RuntimeError> {
    match outcome {
        RuntimeGenerationOutcome::ShutdownRequested => Ok(()),
        RuntimeGenerationOutcome::ReloadRequested => Err(RuntimeError::Protocol(
            "standalone runtime received a configuration reload request",
        )),
        RuntimeGenerationOutcome::Failed(error) => Err(error),
    }
}

#[derive(Debug)]
pub(crate) enum RuntimeGenerationOutcome {
    ReloadRequested,
    ShutdownRequested,
    Failed(RuntimeError),
}

/// Returns the first service result or requested terminal action after joining
/// every task owned by this runtime generation.
pub(super) async fn supervise_runtime_services(
    mut services: JoinSet<Result<(), RuntimeError>>,
    generation: &RuntimeGenerationControl,
    exited_message: &'static str,
    empty_message: &'static str,
) -> Result<RuntimeGenerationStopReason, RuntimeError> {
    let result = tokio::select! {
        biased;
        service = services.join_next() => {
            map_runtime_service_result(service, exited_message, empty_message)
        }
        stop = generation.wait_for_stop() => Ok(stop),
    };
    if result.is_ok() {
        generation.wait_for_retirement_authorization().await;
    }
    retire_runtime_services(&mut services).await;
    result
}

pub(super) async fn retire_runtime_services(services: &mut JoinSet<Result<(), RuntimeError>>) {
    services.abort_all();
    while services.join_next().await.is_some() {}
}

pub(super) fn map_runtime_service_result(
    result: Option<Result<Result<(), RuntimeError>, tokio::task::JoinError>>,
    exited_message: &'static str,
    empty_message: &'static str,
) -> Result<RuntimeGenerationStopReason, RuntimeError> {
    match result {
        Some(Ok(Ok(()))) => Err(RuntimeError::Protocol(exited_message)),
        Some(Ok(Err(error))) => Err(error),
        Some(Err(error)) => Err(RuntimeError::TaskJoin(error)),
        None => Err(RuntimeError::Protocol(empty_message)),
    }
}

#[cfg(test)]
#[path = "node_test.rs"]
mod tests;
