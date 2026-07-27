//! Platform-neutral managed-VPN contracts and host-network ownership.
//!
//! Portable desired state and lifecycle capabilities sit above explicitly
//! target-specific adapters. Host mutations occur only during VPN activation
//! and cleanup; packet and byte paths never call this API.

mod config;
#[cfg(any(target_os = "windows", target_os = "macos"))]
mod desktop_routes;
mod lifecycle;
#[cfg(target_os = "linux")]
mod linux;
mod linux_plan;
mod linux_transaction;
#[cfg(target_os = "linux")]
mod linux_vpn;
#[cfg(target_os = "macos")]
mod macos;
mod managed_vpn;
mod packet_device;
mod process_plan;
mod process_transaction;
mod route;
mod vpn_generation;
#[cfg(target_os = "windows")]
mod windows;
#[cfg(target_os = "windows")]
mod windows_vpn;

pub use config::{
    DEFAULT_LINUX_CAPTURE_RULE_PRIORITY, DEFAULT_LINUX_NATIVE_RULE_PRIORITY,
    DEFAULT_LINUX_ROUTE_TABLE, DEFAULT_LINUX_SOCKET_MARK, DnsCaptureConfig, LinuxInterfaceName,
    LinuxInterfaceNameError, LinuxPolicyConfig, LinuxPolicyConfigError, LinuxSocketMark,
    LinuxSocketMarkError, LinuxVpnConfig, ManagedVpnConfig, ManagedVpnConfigError, RouteMode,
};
#[cfg(any(target_os = "windows", target_os = "macos"))]
pub use desktop_routes::{
    SystemProcessHostNetworkBackend, SystemProcessMutationError, SystemProcessRollbackToken,
    snapshot_process_vpn_environment,
};
pub use lifecycle::{
    AndroidVpnServiceIntegration, MacosVpnLifecycleIntegration, PreparedVpn, VpnActivationModel,
    VpnBypassAddressKind, VpnCapability, VpnCapabilityAvailability, VpnCapabilityError,
    VpnLifecycleAdapter, VpnLifecycleContractError, VpnLifecycleError, VpnLifecycleRequest,
    VpnLifecycleRequestError, VpnOwnership, VpnPlatform, VpnPlatformCapabilities, VpnSocketHandle,
    VpnTrafficPublication, WindowsVpnLifecycleIntegration,
};
#[cfg(target_os = "linux")]
pub use linux::{
    CommandOutput, CommandRunner, IpRouteSnapshot, LinuxBackendError, LinuxHostNetworkBackend,
    LinuxRollbackToken, LinuxSocketMarkApplyError, SystemCommandRunner,
    SystemLinuxHostNetworkBackend, SystemTunDeviceFactory, TunDeviceFactory,
    apply_linux_socket_mark, parse_ip_route_snapshot, snapshot_linux_environment,
};
pub use linux_plan::{
    LinuxCaptureRoute, LinuxHostOperation, LinuxNativeNetwork, LinuxNativeRoute,
    LinuxNativeRouteError, LinuxVpnEnvironment, LinuxVpnPlan, LinuxVpnPlanError,
};
pub use linux_transaction::{
    LinuxCleanupError, LinuxCleanupOutcome, LinuxControllerState, LinuxHostMutationBackend,
    LinuxOperationPhase, LinuxPrepareError, LinuxPreparedDeviceError, LinuxPublishError,
    LinuxRollbackFailure, TransactionalLinuxVpnController,
};
#[cfg(target_os = "linux")]
pub use linux_vpn::{
    LINUX_VPN_RESOLUTION_TIMEOUT, LinuxVpnCarrierPath, LinuxVpnGenerationSpecError,
    LinuxVpnPrepareError, LinuxVpnPrepareRequest, LinuxVpnPublishError, LinuxVpnShutdownError,
    PreparedLinuxVpn, compile_linux_vpn_prepare_request, compile_node_linux_vpn_prepare_request,
    prepare_linux_vpn,
};
#[cfg(target_os = "macos")]
pub use macos::{MacosUtunCreateError, MacosUtunDeviceFactory, PreparedMacosUtun};
pub use packet_device::{
    PacketDevice, PacketDeviceConfig, PacketDeviceProvider, SystemPacketDeviceProvider,
};
pub use process_plan::{
    DEFAULT_PROCESS_CAPTURE_METRIC, ProcessHostOperation, ProcessNativeNetwork, ProcessNativeRoute,
    ProcessNativeRouteError, ProcessVpnEnvironment, ProcessVpnPlan, ProcessVpnPlanError,
};
pub use process_transaction::{
    ProcessCleanupError, ProcessCleanupOutcome, ProcessControllerState, ProcessHostMutationBackend,
    ProcessOperationPhase, ProcessPrepareError, ProcessPublishError, ProcessRollbackFailure,
    TransactionalProcessVpnController,
};
pub use route::{AddressFamily, BypassReason, BypassReasons};
pub(crate) use vpn_generation::{
    PreparedVpnGeneration, VpnGenerationLifecycle, prepare as prepare_vpn_generation,
    validate as validate_vpn_generation,
};
pub use vpn_generation::{VpnGenerationError, VpnGenerationStage};
#[cfg(target_os = "windows")]
pub use windows::{
    PreparedWindowsWintun, WindowsNativeSocketBinder, WindowsWintunConfig,
    WindowsWintunConfigError, WindowsWintunCreateError, WindowsWintunDeviceFactory,
};

use std::fmt::Write as _;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VpnCapabilityReport {
    pub capability: &'static str,
    pub availability: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlatformReport {
    pub os: &'static str,
    pub arch: &'static str,
    pub tun_backend: &'static str,
    pub tun_privilege: &'static str,
    pub tun_device_probe: String,
    pub service_host: &'static str,
    pub managed_vpn_platform: &'static str,
    pub managed_vpn_ownership: &'static str,
    pub managed_vpn_activation: &'static str,
    pub managed_vpn_capabilities: Vec<VpnCapabilityReport>,
}

impl PlatformReport {
    pub fn current() -> Self {
        let capabilities = VpnPlatformCapabilities::current();
        Self {
            os: std::env::consts::OS,
            arch: std::env::consts::ARCH,
            tun_backend: tun_backend(),
            tun_privilege: tun_privilege_hint(),
            tun_device_probe: tun_device_probe(),
            service_host: service_host_hint(),
            managed_vpn_platform: capabilities.map_or("unsupported", |capabilities| {
                capabilities.platform().as_str()
            }),
            managed_vpn_ownership: capabilities.map_or("unsupported", |capabilities| {
                vpn_ownership_label(capabilities.ownership())
            }),
            managed_vpn_activation: capabilities.map_or("unsupported", |capabilities| {
                vpn_activation_label(capabilities.activation())
            }),
            managed_vpn_capabilities: capabilities.map_or_else(Vec::new, |capabilities| {
                VpnCapability::ALL
                    .into_iter()
                    .map(|capability| VpnCapabilityReport {
                        capability: capability.as_str(),
                        availability: vpn_availability_label(capabilities.availability(capability)),
                    })
                    .collect()
            }),
        }
    }

    pub fn render_text(&self) -> String {
        let mut output = String::new();
        let _ = writeln!(output, "platform:");
        let _ = writeln!(output, "  os: {}", self.os);
        let _ = writeln!(output, "  arch: {}", self.arch);
        let _ = writeln!(output, "  tun_backend: {}", self.tun_backend);
        let _ = writeln!(output, "  tun_privilege: {}", self.tun_privilege);
        let _ = writeln!(output, "  tun_device_probe: {}", self.tun_device_probe);
        let _ = writeln!(output, "  service_host: {}", self.service_host);
        let _ = writeln!(output, "managed_vpn:");
        let _ = writeln!(output, "  platform: {}", self.managed_vpn_platform);
        let _ = writeln!(output, "  ownership: {}", self.managed_vpn_ownership);
        let _ = writeln!(output, "  activation: {}", self.managed_vpn_activation);
        let _ = writeln!(output, "  capabilities:");
        for capability in &self.managed_vpn_capabilities {
            let _ = writeln!(
                output,
                "    - {}: {}",
                capability.capability, capability.availability
            );
        }
        output
    }
}

const fn vpn_ownership_label(ownership: VpnOwnership) -> &'static str {
    match ownership {
        VpnOwnership::ProcessManaged => "process-managed",
        VpnOwnership::HostOwned => "host-owned",
    }
}

const fn vpn_activation_label(activation: VpnActivationModel) -> &'static str {
    match activation {
        VpnActivationModel::TransactionalTwoPhase => "transactional-two-phase",
        VpnActivationModel::HostEstablished => "host-established",
    }
}

const fn vpn_availability_label(availability: VpnCapabilityAvailability) -> &'static str {
    match availability {
        VpnCapabilityAvailability::BuiltIn => "built-in",
        VpnCapabilityAvailability::HostRequired => "host-required",
        VpnCapabilityAvailability::AdapterRequired => "adapter-required",
        VpnCapabilityAvailability::Unsupported => "unsupported",
    }
}

pub fn tun_privilege_hint() -> &'static str {
    #[cfg(target_os = "linux")]
    {
        "requires CAP_NET_ADMIN or equivalent privilege to create/configure TUN"
    }
    #[cfg(target_os = "macos")]
    {
        "consumer VPN requires an entitled Network Extension host; privileged utun helpers alone are incomplete"
    }
    #[cfg(target_os = "windows")]
    {
        "requires Administrator rights and the Wintun driver for TUN mode"
    }
    #[cfg(target_os = "android")]
    {
        "requires VpnService consent and a host-provided owned TUN descriptor"
    }
    #[cfg(not(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "windows",
        target_os = "android"
    )))]
    {
        "TUN privilege requirements are platform-specific"
    }
}

fn tun_backend() -> &'static str {
    #[cfg(target_os = "linux")]
    {
        "Linux /dev/net/tun via tun-rs"
    }
    #[cfg(target_os = "macos")]
    {
        "Network Extension host required for product VPN; privileged utun primitives available"
    }
    #[cfg(target_os = "windows")]
    {
        "Windows Wintun via tun-rs"
    }
    #[cfg(target_os = "android")]
    {
        "Android VpnService descriptor via host PacketDeviceProvider"
    }
    #[cfg(not(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "windows",
        target_os = "android"
    )))]
    {
        "platform TUN via tun-rs"
    }
}

fn service_host_hint() -> &'static str {
    #[cfg(target_os = "linux")]
    {
        "external supervisor (systemd is common; not detected)"
    }
    #[cfg(target_os = "macos")]
    {
        "launchd can supervise proxy mode; product VPN requires a Network Extension host"
    }
    #[cfg(target_os = "windows")]
    {
        "external supervisor (SCM wrapper or service adapter required)"
    }
    #[cfg(target_os = "android")]
    {
        "embedding Android VpnService lifecycle"
    }
    #[cfg(not(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "windows",
        target_os = "android"
    )))]
    {
        "external supervisor"
    }
}

#[cfg(target_os = "linux")]
fn tun_device_probe() -> String {
    let path = std::path::Path::new("/dev/net/tun");
    if !path.exists() {
        return "probed: /dev/net/tun missing".to_string();
    }
    match std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
    {
        Ok(_) => "probed: /dev/net/tun present and openable".to_string(),
        Err(err) => format!("probed: /dev/net/tun present but not openable: {err}"),
    }
}

#[cfg(target_os = "macos")]
fn tun_device_probe() -> String {
    "not probed: no Network Extension host is embedded in the CLI".to_string()
}

#[cfg(target_os = "windows")]
fn tun_device_probe() -> String {
    "not probed: Wintun is checked when the packet provider opens it".to_string()
}

#[cfg(target_os = "android")]
fn tun_device_probe() -> String {
    "not probed: the embedding VpnService supplies the future descriptor".to_string()
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "windows",
    target_os = "android"
)))]
fn tun_device_probe() -> String {
    "not probed: packet-device capability is provider-specific".to_string()
}

#[cfg(test)]
#[path = "platform_test.rs"]
mod tests;
