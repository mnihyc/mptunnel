//! Platform-neutral managed-VPN lifecycle and capability contract.
//!
//! This module describes ownership and activation sequencing only. It is
//! called while constructing or retiring a runtime generation, plus once when
//! a native socket is created. Packet, stream, relay, and scheduler loops must
//! never call a lifecycle adapter.

use crate::platform::ManagedVpnConfig;
use std::fmt;
use std::net::IpAddr;

const MAX_CARRIER_ENDPOINTS: usize = 128;
const MAX_BOOTSTRAP_DNS_ADDRESSES: usize = 32;
const CAPABILITY_COUNT: usize = 7;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum VpnPlatform {
    Linux,
    Android,
    Windows,
    Macos,
}

impl VpnPlatform {
    pub const fn current() -> Option<Self> {
        #[cfg(target_os = "linux")]
        {
            Some(Self::Linux)
        }
        #[cfg(target_os = "android")]
        {
            Some(Self::Android)
        }
        #[cfg(target_os = "windows")]
        {
            Some(Self::Windows)
        }
        #[cfg(target_os = "macos")]
        {
            Some(Self::Macos)
        }
        #[cfg(not(any(
            target_os = "linux",
            target_os = "android",
            target_os = "windows",
            target_os = "macos"
        )))]
        {
            None
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Linux => "linux",
            Self::Android => "android",
            Self::Windows => "windows",
            Self::Macos => "macos",
        }
    }
}

impl fmt::Display for VpnPlatform {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VpnOwnership {
    /// MPTUNNEL creates the device and owns route/DNS mutations.
    ProcessManaged,
    /// An embedding OS service owns the device and all host mutations.
    HostOwned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VpnActivationModel {
    /// Device and inert routes are prepared first; publication occurs only
    /// after the packet worker reports ready.
    ///
    /// Linux and Windows implement this inside the process. A future macOS
    /// Network Extension host can preserve the same ordering by starting its
    /// packet-flow adapter before publishing `NEPacketTunnelNetworkSettings`.
    TransactionalTwoPhase,
    /// The host API creates and publishes the VPN as one operation.
    ///
    /// Android `VpnService.Builder.establish()` has this shape. The returned
    /// packet device is already receiving captured traffic.
    HostEstablished,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum VpnCapability {
    PacketDevice = 0,
    AddressConfiguration = 1,
    RouteConfiguration = 2,
    DnsConfiguration = 3,
    NativeSocketBypass = 4,
    TwoPhasePublication = 5,
    TransactionalCleanup = 6,
}

impl VpnCapability {
    pub const ALL: [Self; CAPABILITY_COUNT] = [
        Self::PacketDevice,
        Self::AddressConfiguration,
        Self::RouteConfiguration,
        Self::DnsConfiguration,
        Self::NativeSocketBypass,
        Self::TwoPhasePublication,
        Self::TransactionalCleanup,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PacketDevice => "packet-device creation",
            Self::AddressConfiguration => "interface address configuration",
            Self::RouteConfiguration => "route publication",
            Self::DnsConfiguration => "DNS publication",
            Self::NativeSocketBypass => "native-socket VPN bypass",
            Self::TwoPhasePublication => "two-phase traffic publication",
            Self::TransactionalCleanup => "transactional cleanup",
        }
    }

    const fn index(self) -> usize {
        self as usize
    }
}

impl fmt::Display for VpnCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VpnCapabilityAvailability {
    /// Implemented by this crate on the named platform.
    BuiltIn,
    /// Must be supplied by the embedding host application.
    HostRequired,
    /// The contract exists, but an OS adapter has not been integrated.
    AdapterRequired,
    /// The platform API cannot provide this lifecycle property.
    Unsupported,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VpnPlatformCapabilities {
    platform: VpnPlatform,
    ownership: VpnOwnership,
    activation: VpnActivationModel,
    support: [VpnCapabilityAvailability; CAPABILITY_COUNT],
}

impl VpnPlatformCapabilities {
    pub const fn for_platform(platform: VpnPlatform) -> Self {
        match platform {
            VpnPlatform::Linux => Self {
                platform,
                ownership: VpnOwnership::ProcessManaged,
                activation: VpnActivationModel::TransactionalTwoPhase,
                support: [VpnCapabilityAvailability::BuiltIn; CAPABILITY_COUNT],
            },
            VpnPlatform::Android => Self {
                platform,
                ownership: VpnOwnership::HostOwned,
                activation: VpnActivationModel::HostEstablished,
                support: [
                    VpnCapabilityAvailability::HostRequired,
                    VpnCapabilityAvailability::HostRequired,
                    VpnCapabilityAvailability::HostRequired,
                    VpnCapabilityAvailability::HostRequired,
                    VpnCapabilityAvailability::HostRequired,
                    VpnCapabilityAvailability::Unsupported,
                    VpnCapabilityAvailability::HostRequired,
                ],
            },
            VpnPlatform::Windows => Self {
                platform,
                ownership: VpnOwnership::ProcessManaged,
                activation: VpnActivationModel::TransactionalTwoPhase,
                // Each generation-scoped operation below is invoked by the
                // executable Windows adapter. This does not imply a kill
                // switch, crash restoration, NRPT policy, or service install.
                support: [
                    VpnCapabilityAvailability::BuiltIn, // Wintun creation
                    VpnCapabilityAvailability::BuiltIn, // Wintun addresses
                    VpnCapabilityAvailability::BuiltIn, // IP Helper routes
                    VpnCapabilityAvailability::BuiltIn, // per-interface DNS
                    VpnCapabilityAvailability::BuiltIn, // unicast-interface socket binding
                    VpnCapabilityAvailability::BuiltIn, // readiness-gated route/DNS publish
                    VpnCapabilityAvailability::BuiltIn, // ordered route/DNS reversal
                ],
            },
            VpnPlatform::Macos => Self {
                platform,
                // A supported consumer VPN is owned by an entitled
                // NEPacketTunnelProvider. The privileged utun/route helpers
                // shipped in this crate are lower-level primitives and never
                // turn that product lifecycle into process-owned support.
                ownership: VpnOwnership::HostOwned,
                activation: VpnActivationModel::TransactionalTwoPhase,
                support: [VpnCapabilityAvailability::AdapterRequired; CAPABILITY_COUNT],
            },
        }
    }

    pub const fn current() -> Option<Self> {
        match VpnPlatform::current() {
            Some(platform) => Some(Self::for_platform(platform)),
            None => None,
        }
    }

    pub const fn platform(self) -> VpnPlatform {
        self.platform
    }

    pub const fn ownership(self) -> VpnOwnership {
        self.ownership
    }

    pub const fn activation(self) -> VpnActivationModel {
        self.activation
    }

    pub const fn availability(self, capability: VpnCapability) -> VpnCapabilityAvailability {
        self.support[capability.index()]
    }

    /// Requires an implementation shipped by this crate.
    ///
    /// Host applications and future OS adapters deliberately receive a typed
    /// error instead of silently falling back to incomplete host mutation.
    pub fn require_built_in(self, capability: VpnCapability) -> Result<(), VpnCapabilityError> {
        match self.availability(capability) {
            VpnCapabilityAvailability::BuiltIn => Ok(()),
            VpnCapabilityAvailability::HostRequired => {
                Err(VpnCapabilityError::HostIntegrationRequired {
                    platform: self.platform,
                    capability,
                })
            }
            VpnCapabilityAvailability::AdapterRequired => {
                Err(VpnCapabilityError::AdapterRequired {
                    platform: self.platform,
                    capability,
                })
            }
            VpnCapabilityAvailability::Unsupported => Err(VpnCapabilityError::Unsupported {
                platform: self.platform,
                capability,
            }),
        }
    }

    pub fn require_built_in_on_current_target(
        self,
        capability: VpnCapability,
    ) -> Result<(), VpnCapabilityError> {
        let current = VpnPlatform::current();
        if current != Some(self.platform) {
            return Err(VpnCapabilityError::TargetMismatch {
                requested: self.platform,
                current,
            });
        }
        self.require_built_in(capability)
    }

    pub fn validate_prepared_publication(
        self,
        actual: VpnTrafficPublication,
    ) -> Result<(), VpnLifecycleContractError> {
        let expected = match self.activation {
            VpnActivationModel::TransactionalTwoPhase => VpnTrafficPublication::Inert,
            VpnActivationModel::HostEstablished => VpnTrafficPublication::Published,
        };
        if actual == expected {
            Ok(())
        } else {
            Err(VpnLifecycleContractError::UnexpectedPreparedPublication {
                platform: self.platform,
                expected,
                actual,
            })
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VpnCapabilityError {
    HostIntegrationRequired {
        platform: VpnPlatform,
        capability: VpnCapability,
    },
    AdapterRequired {
        platform: VpnPlatform,
        capability: VpnCapability,
    },
    Unsupported {
        platform: VpnPlatform,
        capability: VpnCapability,
    },
    TargetMismatch {
        requested: VpnPlatform,
        current: Option<VpnPlatform>,
    },
}

impl fmt::Display for VpnCapabilityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::HostIntegrationRequired {
                platform: VpnPlatform::Android,
                capability,
            } => write!(
                formatter,
                "Android {capability} is host-owned and requires a VpnService integration"
            ),
            Self::HostIntegrationRequired {
                platform,
                capability,
            } => write!(
                formatter,
                "{platform} {capability} must be supplied by the embedding host"
            ),
            Self::AdapterRequired {
                platform: VpnPlatform::Windows,
                capability,
            } => write!(
                formatter,
                "Windows {capability} is built into Windows targets; deployment must still provide the signed architecture-matched wintun.dll"
            ),
            Self::AdapterRequired {
                platform: VpnPlatform::Macos,
                capability,
            } => write!(
                formatter,
                "macOS {capability} is not wired into an entitled Network Extension host; privileged utun/route primitives do not provide product DNS lifecycle"
            ),
            Self::AdapterRequired {
                platform,
                capability,
            } => write!(
                formatter,
                "{platform} {capability} requires a platform lifecycle adapter"
            ),
            Self::Unsupported {
                platform,
                capability,
            } => write!(formatter, "{platform} does not support {capability}"),
            Self::TargetMismatch { requested, current } => match current {
                Some(current) => write!(
                    formatter,
                    "{requested} VPN support cannot run in a {current} build"
                ),
                None => write!(
                    formatter,
                    "{requested} VPN support cannot run on this unsupported build target"
                ),
            },
        }
    }
}

impl std::error::Error for VpnCapabilityError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VpnTrafficPublication {
    /// No host traffic is routed to the prepared packet device.
    Inert,
    /// The host may already deliver captured traffic to the packet device.
    Published,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VpnLifecycleContractError {
    UnexpectedPreparedPublication {
        platform: VpnPlatform,
        expected: VpnTrafficPublication,
        actual: VpnTrafficPublication,
    },
}

impl fmt::Display for VpnLifecycleContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedPreparedPublication {
                platform,
                expected,
                actual,
            } => write!(
                formatter,
                "{platform} lifecycle returned {actual:?} traffic at prepare; expected {expected:?}"
            ),
        }
    }
}

impl std::error::Error for VpnLifecycleContractError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VpnLifecycleRequest {
    config: ManagedVpnConfig,
    carrier_endpoints: Vec<IpAddr>,
    bootstrap_dns: Vec<IpAddr>,
}

impl VpnLifecycleRequest {
    pub fn new(
        config: ManagedVpnConfig,
        carrier_endpoints: impl IntoIterator<Item = IpAddr>,
        bootstrap_dns: impl IntoIterator<Item = IpAddr>,
    ) -> Result<Self, VpnLifecycleRequestError> {
        Ok(Self {
            config,
            carrier_endpoints: validate_bypass_addresses(
                carrier_endpoints,
                VpnBypassAddressKind::CarrierEndpoint,
                MAX_CARRIER_ENDPOINTS,
            )?,
            bootstrap_dns: validate_bypass_addresses(
                bootstrap_dns,
                VpnBypassAddressKind::BootstrapDns,
                MAX_BOOTSTRAP_DNS_ADDRESSES,
            )?,
        })
    }

    pub fn config(&self) -> &ManagedVpnConfig {
        &self.config
    }

    pub fn carrier_endpoints(&self) -> &[IpAddr] {
        &self.carrier_endpoints
    }

    pub fn bootstrap_dns(&self) -> &[IpAddr] {
        &self.bootstrap_dns
    }
}

fn validate_bypass_addresses(
    addresses: impl IntoIterator<Item = IpAddr>,
    kind: VpnBypassAddressKind,
    maximum: usize,
) -> Result<Vec<IpAddr>, VpnLifecycleRequestError> {
    let mut addresses = addresses.into_iter().collect::<Vec<_>>();
    addresses.sort_unstable();
    addresses.dedup();
    if addresses.len() > maximum {
        return Err(VpnLifecycleRequestError::TooManyBypassAddresses {
            kind,
            actual: addresses.len(),
            maximum,
        });
    }
    if let Some(address) = addresses
        .iter()
        .copied()
        .find(|address| address.is_unspecified() || address.is_loopback() || address.is_multicast())
    {
        return Err(VpnLifecycleRequestError::InvalidBypassAddress { kind, address });
    }
    Ok(addresses)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VpnBypassAddressKind {
    CarrierEndpoint,
    BootstrapDns,
}

impl fmt::Display for VpnBypassAddressKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CarrierEndpoint => formatter.write_str("carrier endpoint"),
            Self::BootstrapDns => formatter.write_str("bootstrap DNS"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VpnLifecycleRequestError {
    TooManyBypassAddresses {
        kind: VpnBypassAddressKind,
        actual: usize,
        maximum: usize,
    },
    InvalidBypassAddress {
        kind: VpnBypassAddressKind,
        address: IpAddr,
    },
}

impl fmt::Display for VpnLifecycleRequestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyBypassAddresses {
                kind,
                actual,
                maximum,
            } => write!(
                formatter,
                "VPN lifecycle has {actual} {kind} addresses; maximum is {maximum}"
            ),
            Self::InvalidBypassAddress { kind, address } => {
                write!(formatter, "invalid VPN {kind} bypass address {address}")
            }
        }
    }
}

impl std::error::Error for VpnLifecycleRequestError {}

/// Platform-neutral socket identity supplied before connect or first send.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VpnSocketHandle {
    UnixFd(i32),
    WindowsSocket(usize),
}

#[derive(Debug)]
pub struct PreparedVpn<Device> {
    device: Device,
    publication: VpnTrafficPublication,
}

impl<Device> PreparedVpn<Device> {
    pub fn inert(device: Device) -> Self {
        Self {
            device,
            publication: VpnTrafficPublication::Inert,
        }
    }

    /// Constructs a host-owned device that is already published.
    ///
    /// This is the truthful Android `VpnService.establish()` result. It must
    /// not be used by a two-phase process-managed adapter.
    pub fn host_published(device: Device) -> Self {
        Self {
            device,
            publication: VpnTrafficPublication::Published,
        }
    }

    pub fn publication(&self) -> VpnTrafficPublication {
        self.publication
    }

    pub fn into_device(self) -> Device {
        self.device
    }
}

#[derive(Debug)]
pub enum VpnLifecycleError<AdapterError> {
    Capability(VpnCapabilityError),
    Contract(VpnLifecycleContractError),
    Adapter(AdapterError),
}

impl<AdapterError> From<VpnCapabilityError> for VpnLifecycleError<AdapterError> {
    fn from(error: VpnCapabilityError) -> Self {
        Self::Capability(error)
    }
}

impl<AdapterError> From<VpnLifecycleContractError> for VpnLifecycleError<AdapterError> {
    fn from(error: VpnLifecycleContractError) -> Self {
        Self::Contract(error)
    }
}

impl<AdapterError: fmt::Display> fmt::Display for VpnLifecycleError<AdapterError> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Capability(error) => fmt::Display::fmt(error, formatter),
            Self::Contract(error) => fmt::Display::fmt(error, formatter),
            Self::Adapter(error) => write!(formatter, "VPN lifecycle adapter failed: {error}"),
        }
    }
}

impl<AdapterError> std::error::Error for VpnLifecycleError<AdapterError>
where
    AdapterError: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Capability(error) => Some(error),
            Self::Contract(error) => Some(error),
            Self::Adapter(error) => Some(error),
        }
    }
}

/// Host-neutral lifecycle boundary for a single runtime generation.
///
/// Transactional adapters return an inert prepared device, publish after the
/// worker is ready, unpublish before worker shutdown, then clean up. Android
/// hosts return an already-published device from `prepare`, do not call
/// `publish`, and own revocation/cleanup through `VpnService`.
pub trait VpnLifecycleAdapter {
    type PacketDevice;
    type Error;

    fn capabilities(&self) -> VpnPlatformCapabilities;

    fn prepare(
        &mut self,
        request: &VpnLifecycleRequest,
    ) -> Result<PreparedVpn<Self::PacketDevice>, VpnLifecycleError<Self::Error>>;

    fn publish(&mut self) -> Result<(), VpnLifecycleError<Self::Error>>;

    fn unpublish(&mut self) -> Result<(), VpnLifecycleError<Self::Error>>;

    fn cleanup(&mut self) -> Result<(), VpnLifecycleError<Self::Error>>;

    /// Excludes one MPTUNNEL-owned native socket from VPN recapture.
    ///
    /// Android integrations must call `VpnService.protect(fd)`. Linux uses
    /// `SO_MARK`; Windows binds the socket to its pre-VPN interface and also
    /// installs exact bootstrap routes. macOS hosts own the corresponding
    /// Network Extension operation. This method is never a payload-path
    /// callback.
    fn protect_native_socket(
        &mut self,
        socket: VpnSocketHandle,
    ) -> Result<(), VpnLifecycleError<Self::Error>>;
}

/// Marker for the embedding Android `VpnService` implementation.
///
/// The host must configure addresses/routes/DNS, call `establish`, transfer
/// the resulting descriptor, protect every native socket, and revoke the
/// session. This crate never claims those Java-owned operations are built in.
pub trait AndroidVpnServiceIntegration: VpnLifecycleAdapter {}

/// Runtime-generation seam for the built-in Wintun + IP Helper + DNS primitives.
pub trait WindowsVpnLifecycleIntegration: VpnLifecycleAdapter {}

/// Network Extension host seam. The host starts packet-flow I/O before
/// publishing `NEPacketTunnelNetworkSettings`, then reverses settings before
/// worker shutdown. The built-in privileged utun/route primitives deliberately
/// do not claim this consumer-product lifecycle.
pub trait MacosVpnLifecycleIntegration: VpnLifecycleAdapter {}

#[cfg(test)]
#[path = "tests_lifecycle.rs"]
mod tests;
