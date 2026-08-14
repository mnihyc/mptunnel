use crate::platform::{
    DnsCaptureConfig, LinuxPolicyConfig, ManagedVpnConfig as PlatformManagedVpnConfig,
    ManagedVpnConfigError as PlatformManagedVpnConfigError, RouteMode,
};
use ipnet::IpNet;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

pub const DEFAULT_TUN_IPV4: Ipv4Addr = Ipv4Addr::new(10, 88, 0, 1);
pub const DEFAULT_TUN_IPV4_PREFIX: u8 = 24;
pub const DEFAULT_TUN_MTU: u16 = 1500;
pub const DEFAULT_TUN_DNS_TTL_MS: u32 = 5_000;
pub const DEFAULT_MANAGED_TUN_NAME: &str = "mptun0";

/// Host ownership for one TUN ingress.
///
/// External mode only opens or consumes the packet device. Managed mode is a
/// platform-neutral request for the process to own the host interface, routes,
/// and optional DNS publication as one transaction on a supported host.
/// Android `VpnService` lifecycle remains host-provided and therefore uses
/// external mode rather than claiming process-managed ownership.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum TunHostConfig {
    #[default]
    External,
    Managed(ManagedVpnConfig),
}

/// Portable managed-VPN policy kept separate from packet processing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedVpnConfig {
    pub route_mode: RouteMode,
    pub excludes: Vec<IpNet>,
    pub local_lan: bool,
    pub dns_capture_servers: Vec<IpAddr>,
    pub platform: ManagedVpnPlatformConfig,
}

/// Optional platform tuning. Portable VPN behavior must not depend on it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ManagedVpnPlatformConfig {
    pub linux: Option<LinuxPolicyConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TunL4Config {
    pub interface_name: Option<String>,
    pub ipv4: Option<Ipv4Addr>,
    pub ipv4_prefix: u8,
    pub ipv4_gateway: Option<Ipv4Addr>,
    pub ipv6: Option<Ipv6Addr>,
    pub ipv6_prefix: u8,
    pub mtu: u16,
    pub enable_icmp: bool,
    pub dns_resolvers: Vec<SocketAddr>,
    pub dns_ttl_ms: u32,
    pub host: TunHostConfig,
}

impl Default for TunL4Config {
    fn default() -> Self {
        Self {
            interface_name: None,
            ipv4: Some(DEFAULT_TUN_IPV4),
            ipv4_prefix: DEFAULT_TUN_IPV4_PREFIX,
            ipv4_gateway: None,
            ipv6: None,
            ipv6_prefix: 64,
            mtu: DEFAULT_TUN_MTU,
            enable_icmp: true,
            dns_resolvers: Vec::new(),
            dns_ttl_ms: DEFAULT_TUN_DNS_TTL_MS,
            host: TunHostConfig::External,
        }
    }
}

impl TunL4Config {
    pub fn managed_vpn(&self) -> Option<&ManagedVpnConfig> {
        match &self.host {
            TunHostConfig::External => None,
            TunHostConfig::Managed(config) => Some(config),
        }
    }

    /// System-facing addresses handled by the managed local DNS listener.
    ///
    /// External/manual TUN forwarding intentionally returns an empty slice and
    /// continues to use the configured DNS redirects.
    pub fn managed_dns_capture_servers(&self) -> &[IpAddr] {
        self.managed_vpn()
            .map_or(&[], |config| config.dns_capture_servers.as_slice())
    }

    /// Compile the host-independent TUN fields and managed policy into one
    /// portable desired-state value. A platform lifecycle adapter adds its
    /// interface identity and mutation policy after this boundary.
    pub fn compile_managed_vpn(
        &self,
    ) -> Result<Option<PlatformManagedVpnConfig>, ManagedVpnCompileError> {
        let Some(managed) = self.managed_vpn() else {
            return Ok(None);
        };
        if !self.dns_resolvers.is_empty() {
            return Err(ManagedVpnCompileError::ExternalDnsResolvers);
        }
        if self.ipv4_gateway.is_some() {
            return Err(ManagedVpnCompileError::ExternalIpv4Gateway);
        }
        if self.ipv4_prefix > 32 {
            return Err(ManagedVpnCompileError::InvalidIpv4Prefix(self.ipv4_prefix));
        }
        if self.ipv6_prefix > 128 {
            return Err(ManagedVpnCompileError::InvalidIpv6Prefix(self.ipv6_prefix));
        }
        if matches!(managed.route_mode, RouteMode::Full) && managed.dns_capture_servers.is_empty() {
            return Err(ManagedVpnCompileError::FullDnsCaptureRequired);
        }
        let mut addresses =
            Vec::with_capacity(usize::from(self.ipv4.is_some()) + usize::from(self.ipv6.is_some()));
        if let Some(address) = self.ipv4 {
            addresses.push(
                IpNet::new(IpAddr::V4(address), self.ipv4_prefix)
                    .expect("validated IPv4 prefix length"),
            );
        }
        if let Some(address) = self.ipv6 {
            addresses.push(
                IpNet::new(IpAddr::V6(address), self.ipv6_prefix)
                    .expect("validated IPv6 prefix length"),
            );
        }

        let mut config =
            PlatformManagedVpnConfig::new(addresses, self.mtu, managed.route_mode.clone())
                .map_err(ManagedVpnCompileError::Platform)?
                .with_excludes(managed.excludes.clone())
                .map_err(ManagedVpnCompileError::Platform)?
                .with_local_lan(managed.local_lan);
        if !managed.dns_capture_servers.is_empty() {
            let dns = DnsCaptureConfig::new(managed.dns_capture_servers.clone())
                .map_err(ManagedVpnCompileError::Platform)?;
            config = config
                .with_dns(dns)
                .map_err(ManagedVpnCompileError::Platform)?;
        }
        Ok(Some(config))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManagedVpnCompileError {
    InvalidIpv4Prefix(u8),
    InvalidIpv6Prefix(u8),
    FullDnsCaptureRequired,
    ExternalDnsResolvers,
    ExternalIpv4Gateway,
    Platform(PlatformManagedVpnConfigError),
}

impl fmt::Display for ManagedVpnCompileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidIpv4Prefix(prefix) => {
                write!(formatter, "managed VPN IPv4 prefix {prefix} exceeds 32")
            }
            Self::InvalidIpv6Prefix(prefix) => {
                write!(formatter, "managed VPN IPv6 prefix {prefix} exceeds 128")
            }
            Self::FullDnsCaptureRequired => {
                formatter.write_str("managed full VPN requires at least one DNS listener")
            }
            Self::ExternalDnsResolvers => formatter
                .write_str("managed VPN cannot set external TUN dns_redirects; use DNS listeners"),
            Self::ExternalIpv4Gateway => {
                formatter.write_str("managed VPN cannot set the external/manual TUN IPv4 gateway")
            }
            Self::Platform(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for ManagedVpnCompileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Platform(error) => Some(error),
            Self::InvalidIpv4Prefix(_)
            | Self::InvalidIpv6Prefix(_)
            | Self::FullDnsCaptureRequired
            | Self::ExternalDnsResolvers
            | Self::ExternalIpv4Gateway => None,
        }
    }
}
