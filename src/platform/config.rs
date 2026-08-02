use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use std::cmp::Ordering;
use std::fmt;
use std::net::IpAddr;

pub const DEFAULT_LINUX_ROUTE_TABLE: u32 = 51_820;
pub const DEFAULT_LINUX_NATIVE_RULE_PRIORITY: u32 = 9_999;
pub const DEFAULT_LINUX_CAPTURE_RULE_PRIORITY: u32 = 10_000;
pub const DEFAULT_LINUX_SOCKET_MARK: LinuxSocketMark = LinuxSocketMark(0x4d50_5455);

const LINUX_INTERFACE_NAME_MAX_BYTES: usize = 15;
const LINUX_MAIN_ROUTE_PRIORITY: u32 = 32_766;

/// Nonzero mark applied only to MPTUNNEL-owned native-egress sockets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LinuxSocketMark(u32);

impl LinuxSocketMark {
    pub fn new(value: u32) -> Result<Self, LinuxSocketMarkError> {
        if value == 0 {
            return Err(LinuxSocketMarkError::Zero);
        }
        Ok(Self(value))
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinuxSocketMarkError {
    Zero,
}

impl fmt::Display for LinuxSocketMarkError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Zero => formatter.write_str("Linux native-egress socket mark must be nonzero"),
        }
    }
}

impl std::error::Error for LinuxSocketMarkError {}

/// Collision-free Linux RPDB ownership for one VPN instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LinuxPolicyConfig {
    route_table: u32,
    native_rule_priority: u32,
    capture_rule_priority: u32,
    socket_mark: LinuxSocketMark,
}

impl LinuxPolicyConfig {
    pub fn new(
        route_table: u32,
        native_rule_priority: u32,
        capture_rule_priority: u32,
        socket_mark: LinuxSocketMark,
    ) -> Result<Self, LinuxPolicyConfigError> {
        validate_linux_policy(route_table, native_rule_priority, capture_rule_priority)?;
        Ok(Self {
            route_table,
            native_rule_priority,
            capture_rule_priority,
            socket_mark,
        })
    }

    pub const fn route_table(self) -> u32 {
        self.route_table
    }

    pub const fn native_rule_priority(self) -> u32 {
        self.native_rule_priority
    }

    pub const fn capture_rule_priority(self) -> u32 {
        self.capture_rule_priority
    }

    pub const fn socket_mark(self) -> LinuxSocketMark {
        self.socket_mark
    }
}

impl Default for LinuxPolicyConfig {
    fn default() -> Self {
        Self {
            route_table: DEFAULT_LINUX_ROUTE_TABLE,
            native_rule_priority: DEFAULT_LINUX_NATIVE_RULE_PRIORITY,
            capture_rule_priority: DEFAULT_LINUX_CAPTURE_RULE_PRIORITY,
            socket_mark: DEFAULT_LINUX_SOCKET_MARK,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinuxPolicyConfigError {
    ReservedRouteTable(u32),
    InvalidNativeRulePriority(u32),
    InvalidCaptureRulePriority(u32),
    NativeRuleMustPrecedeCapture { native: u32, capture: u32 },
}

impl fmt::Display for LinuxPolicyConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReservedRouteTable(table) => {
                write!(formatter, "Linux route table {table} is reserved")
            }
            Self::InvalidNativeRulePriority(priority) => write!(
                formatter,
                "Linux native-egress rule priority {priority} must be between 1 and 32765"
            ),
            Self::InvalidCaptureRulePriority(priority) => write!(
                formatter,
                "Linux capture rule priority {priority} must be between 1 and 32765"
            ),
            Self::NativeRuleMustPrecedeCapture { native, capture } => write!(
                formatter,
                "Linux native-egress rule priority {native} must be numerically lower than capture priority {capture}"
            ),
        }
    }
}

impl std::error::Error for LinuxPolicyConfigError {}

/// A validated Linux interface name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LinuxInterfaceName(String);

impl LinuxInterfaceName {
    pub fn parse(value: impl Into<String>) -> Result<Self, LinuxInterfaceNameError> {
        let value = value.into();
        if value.is_empty() {
            return Err(LinuxInterfaceNameError::Empty);
        }
        if value.len() > LINUX_INTERFACE_NAME_MAX_BYTES {
            return Err(LinuxInterfaceNameError::TooLong {
                actual: value.len(),
                maximum: LINUX_INTERFACE_NAME_MAX_BYTES,
            });
        }
        if !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        {
            return Err(LinuxInterfaceNameError::InvalidCharacter);
        }
        if !value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        {
            return Err(LinuxInterfaceNameError::InvalidFirstCharacter);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for LinuxInterfaceName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LinuxInterfaceNameError {
    Empty,
    TooLong { actual: usize, maximum: usize },
    InvalidCharacter,
    InvalidFirstCharacter,
}

impl fmt::Display for LinuxInterfaceNameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("Linux VPN interface name must not be empty"),
            Self::TooLong { actual, maximum } => write!(
                formatter,
                "Linux VPN interface name is {actual} bytes; maximum is {maximum}"
            ),
            Self::InvalidCharacter => formatter.write_str(
                "Linux VPN interface name may contain only ASCII letters, digits, '_', '-', and '.'",
            ),
            Self::InvalidFirstCharacter => {
                formatter.write_str("Linux VPN interface name must start with a letter or digit")
            }
        }
    }
}

impl std::error::Error for LinuxInterfaceNameError {}

/// Network prefixes captured by the VPN.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteMode {
    /// Capture both address families configured on the TUN.
    Full,
    /// Capture only the listed prefixes. Explicit excludes still win.
    Split(Vec<IpNet>),
}

/// Host DNS servers whose port-53 traffic must enter the TUN.
///
/// Product DNS selects the real upstream. These addresses are only the
/// system-facing destinations published on the VPN link.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsCaptureConfig {
    servers: Vec<IpAddr>,
}

impl DnsCaptureConfig {
    pub fn new(mut servers: Vec<IpAddr>) -> Result<Self, ManagedVpnConfigError> {
        servers.sort_unstable();
        servers.dedup();
        if servers.is_empty() {
            return Err(ManagedVpnConfigError::DnsServerRequired);
        }
        if let Some(server) = servers.iter().copied().find(|server| !usable_ip(*server)) {
            return Err(ManagedVpnConfigError::InvalidDnsServer(server));
        }
        Ok(Self { servers })
    }

    pub fn servers(&self) -> &[IpAddr] {
        &self.servers
    }
}

/// Platform-neutral desired state for one managed VPN.
///
/// This value contains only packet-device, route, and DNS intent. Platform
/// ownership, interface identity, socket-bypass mechanics, and mutation policy
/// belong to a lifecycle adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedVpnConfig {
    addresses: Vec<IpNet>,
    mtu: u16,
    route_mode: RouteMode,
    excludes: Vec<IpNet>,
    local_lan: bool,
    dns: Option<DnsCaptureConfig>,
}

impl ManagedVpnConfig {
    pub fn new(
        mut addresses: Vec<IpNet>,
        mtu: u16,
        mut route_mode: RouteMode,
    ) -> Result<Self, ManagedVpnConfigError> {
        sort_ip_nets(&mut addresses);
        addresses.dedup();
        validate_addresses(&addresses, mtu)?;
        if let RouteMode::Split(includes) = &mut route_mode {
            canonicalize_ip_nets(includes);
            if includes.is_empty() {
                return Err(ManagedVpnConfigError::SplitIncludeRequired);
            }
            validate_route_families(includes, &addresses)?;
        }
        Ok(Self {
            addresses,
            mtu,
            route_mode,
            excludes: Vec::new(),
            local_lan: false,
            dns: None,
        })
    }

    pub fn with_excludes(
        mut self,
        mut excludes: Vec<IpNet>,
    ) -> Result<Self, ManagedVpnConfigError> {
        canonicalize_ip_nets(&mut excludes);
        validate_route_families(&excludes, &self.addresses)?;
        if let Some(dns) = &self.dns {
            validate_dns_not_excluded(dns, &excludes)?;
        }
        self.excludes = excludes;
        Ok(self)
    }

    pub fn with_local_lan(mut self, enabled: bool) -> Self {
        self.local_lan = enabled;
        self
    }

    pub fn with_dns(mut self, dns: DnsCaptureConfig) -> Result<Self, ManagedVpnConfigError> {
        for server in dns.servers() {
            if !family_is_configured(*server, &self.addresses) {
                return Err(ManagedVpnConfigError::UnsupportedDnsFamily(*server));
            }
        }
        validate_dns_not_excluded(&dns, &self.excludes)?;
        self.dns = Some(dns);
        Ok(self)
    }

    pub fn addresses(&self) -> &[IpNet] {
        &self.addresses
    }

    pub fn mtu(&self) -> u16 {
        self.mtu
    }

    pub fn route_mode(&self) -> &RouteMode {
        &self.route_mode
    }

    pub fn excludes(&self) -> &[IpNet] {
        &self.excludes
    }

    pub fn local_lan(&self) -> bool {
        self.local_lan
    }

    pub fn dns(&self) -> Option<&DnsCaptureConfig> {
        self.dns.as_ref()
    }
}

/// Linux-specific ownership around one platform-neutral VPN configuration.
///
/// The Linux planner requires an exact kernel interface name and collision-free
/// RPDB policy. Neither leaks into [`ManagedVpnConfig`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinuxVpnConfig {
    interface: LinuxInterfaceName,
    managed: ManagedVpnConfig,
    linux_policy: LinuxPolicyConfig,
}

impl LinuxVpnConfig {
    pub fn new(
        interface: LinuxInterfaceName,
        addresses: Vec<IpNet>,
        mtu: u16,
        route_mode: RouteMode,
    ) -> Result<Self, ManagedVpnConfigError> {
        ManagedVpnConfig::new(addresses, mtu, route_mode)
            .map(|managed| Self::from_managed(interface, managed))
    }

    pub fn from_managed(interface: LinuxInterfaceName, managed: ManagedVpnConfig) -> Self {
        Self {
            interface,
            managed,
            linux_policy: LinuxPolicyConfig::default(),
        }
    }

    pub fn with_excludes(mut self, excludes: Vec<IpNet>) -> Result<Self, ManagedVpnConfigError> {
        self.managed = self.managed.with_excludes(excludes)?;
        Ok(self)
    }

    pub fn with_local_lan(mut self, enabled: bool) -> Self {
        self.managed = self.managed.with_local_lan(enabled);
        self
    }

    pub fn with_dns(mut self, dns: DnsCaptureConfig) -> Result<Self, ManagedVpnConfigError> {
        self.managed = self.managed.with_dns(dns)?;
        Ok(self)
    }

    pub fn with_linux_policy(mut self, policy: LinuxPolicyConfig) -> Self {
        self.linux_policy = policy;
        self
    }

    pub fn interface(&self) -> &LinuxInterfaceName {
        &self.interface
    }

    pub fn managed(&self) -> &ManagedVpnConfig {
        &self.managed
    }

    pub fn into_managed(self) -> ManagedVpnConfig {
        self.managed
    }

    pub fn addresses(&self) -> &[IpNet] {
        self.managed.addresses()
    }

    pub fn mtu(&self) -> u16 {
        self.managed.mtu()
    }

    pub fn route_mode(&self) -> &RouteMode {
        self.managed.route_mode()
    }

    pub fn excludes(&self) -> &[IpNet] {
        self.managed.excludes()
    }

    pub fn local_lan(&self) -> bool {
        self.managed.local_lan()
    }

    pub fn dns(&self) -> Option<&DnsCaptureConfig> {
        self.managed.dns()
    }

    pub fn linux_policy(&self) -> LinuxPolicyConfig {
        self.linux_policy
    }
}

fn validate_addresses(addresses: &[IpNet], mtu: u16) -> Result<(), ManagedVpnConfigError> {
    if addresses.is_empty() {
        return Err(ManagedVpnConfigError::AddressRequired);
    }
    let mut has_v4 = false;
    let mut has_v6 = false;
    for address in addresses {
        if !usable_ip(address.addr()) {
            return Err(ManagedVpnConfigError::InvalidInterfaceAddress(
                address.addr(),
            ));
        }
        match address {
            IpNet::V4(_) if std::mem::replace(&mut has_v4, true) => {
                return Err(ManagedVpnConfigError::DuplicateAddressFamily);
            }
            IpNet::V6(_) if std::mem::replace(&mut has_v6, true) => {
                return Err(ManagedVpnConfigError::DuplicateAddressFamily);
            }
            _ => {}
        }
    }
    if mtu < 576 {
        return Err(ManagedVpnConfigError::MtuTooSmall(mtu));
    }
    if has_v6 && mtu < 1280 {
        return Err(ManagedVpnConfigError::Ipv6MtuTooSmall(mtu));
    }
    Ok(())
}

fn validate_route_families(
    routes: &[IpNet],
    addresses: &[IpNet],
) -> Result<(), ManagedVpnConfigError> {
    for route in routes {
        let supported = addresses
            .iter()
            .any(|address| address.addr().is_ipv4() == route.addr().is_ipv4());
        if !supported {
            return Err(ManagedVpnConfigError::UnsupportedRouteFamily(*route));
        }
    }
    Ok(())
}

fn validate_dns_not_excluded(
    dns: &DnsCaptureConfig,
    excludes: &[IpNet],
) -> Result<(), ManagedVpnConfigError> {
    if let Some(server) = dns
        .servers()
        .iter()
        .copied()
        .find(|server| excludes.iter().any(|exclude| exclude.contains(server)))
    {
        return Err(ManagedVpnConfigError::DnsServerExcluded(server));
    }
    Ok(())
}

fn validate_linux_policy(
    route_table: u32,
    native_rule_priority: u32,
    capture_rule_priority: u32,
) -> Result<(), LinuxPolicyConfigError> {
    if matches!(route_table, 0 | 253 | 254 | 255) {
        return Err(LinuxPolicyConfigError::ReservedRouteTable(route_table));
    }
    if native_rule_priority == 0 || native_rule_priority >= LINUX_MAIN_ROUTE_PRIORITY {
        return Err(LinuxPolicyConfigError::InvalidNativeRulePriority(
            native_rule_priority,
        ));
    }
    if capture_rule_priority == 0 || capture_rule_priority >= LINUX_MAIN_ROUTE_PRIORITY {
        return Err(LinuxPolicyConfigError::InvalidCaptureRulePriority(
            capture_rule_priority,
        ));
    }
    if native_rule_priority >= capture_rule_priority {
        return Err(LinuxPolicyConfigError::NativeRuleMustPrecedeCapture {
            native: native_rule_priority,
            capture: capture_rule_priority,
        });
    }
    Ok(())
}

fn family_is_configured(address: IpAddr, configured: &[IpNet]) -> bool {
    configured
        .iter()
        .any(|candidate| candidate.addr().is_ipv4() == address.is_ipv4())
}

fn usable_ip(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => {
            !address.is_unspecified()
                && !address.is_loopback()
                && !address.is_multicast()
                && !address.is_broadcast()
        }
        IpAddr::V6(address) => {
            !address.is_unspecified() && !address.is_loopback() && !address.is_multicast()
        }
    }
}

pub(crate) fn canonical_net(network: IpNet) -> IpNet {
    match network {
        IpNet::V4(network) => IpNet::V4(
            Ipv4Net::new(network.network(), network.prefix_len()).expect("valid IPv4 net"),
        ),
        IpNet::V6(network) => IpNet::V6(
            Ipv6Net::new(network.network(), network.prefix_len()).expect("valid IPv6 net"),
        ),
    }
}

pub(crate) fn canonicalize_ip_nets(networks: &mut Vec<IpNet>) {
    for network in networks.iter_mut() {
        *network = canonical_net(*network);
    }
    sort_ip_nets(networks);
    networks.dedup();
}

pub(crate) fn sort_ip_nets(networks: &mut [IpNet]) {
    networks.sort_unstable_by(compare_ip_nets);
}

pub(crate) fn compare_ip_nets(left: &IpNet, right: &IpNet) -> Ordering {
    match (left, right) {
        (IpNet::V4(left), IpNet::V4(right)) => left
            .addr()
            .octets()
            .cmp(&right.addr().octets())
            .then_with(|| left.prefix_len().cmp(&right.prefix_len())),
        (IpNet::V6(left), IpNet::V6(right)) => left
            .addr()
            .octets()
            .cmp(&right.addr().octets())
            .then_with(|| left.prefix_len().cmp(&right.prefix_len())),
        (IpNet::V4(_), IpNet::V6(_)) => Ordering::Less,
        (IpNet::V6(_), IpNet::V4(_)) => Ordering::Greater,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManagedVpnConfigError {
    AddressRequired,
    DuplicateAddressFamily,
    InvalidInterfaceAddress(IpAddr),
    MtuTooSmall(u16),
    Ipv6MtuTooSmall(u16),
    SplitIncludeRequired,
    UnsupportedRouteFamily(IpNet),
    DnsServerRequired,
    InvalidDnsServer(IpAddr),
    UnsupportedDnsFamily(IpAddr),
    DnsServerExcluded(IpAddr),
}

impl fmt::Display for ManagedVpnConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AddressRequired => formatter.write_str("VPN requires at least one TUN address"),
            Self::DuplicateAddressFamily => {
                formatter.write_str("VPN accepts at most one TUN address per IP family")
            }
            Self::InvalidInterfaceAddress(address) => {
                write!(formatter, "invalid TUN interface address {address}")
            }
            Self::MtuTooSmall(mtu) => write!(formatter, "VPN MTU {mtu} is below 576"),
            Self::Ipv6MtuTooSmall(mtu) => {
                write!(formatter, "IPv6 VPN MTU {mtu} is below 1280")
            }
            Self::SplitIncludeRequired => {
                formatter.write_str("split VPN mode requires at least one include prefix")
            }
            Self::UnsupportedRouteFamily(route) => write!(
                formatter,
                "VPN route {route} has no configured TUN address of the same family"
            ),
            Self::DnsServerRequired => {
                formatter.write_str("DNS capture requires at least one server address")
            }
            Self::InvalidDnsServer(server) => {
                write!(
                    formatter,
                    "invalid system-facing DNS server address {server}"
                )
            }
            Self::UnsupportedDnsFamily(server) => write!(
                formatter,
                "DNS server {server} has no configured TUN address of the same family"
            ),
            Self::DnsServerExcluded(server) => {
                write!(
                    formatter,
                    "DNS server {server} is covered by an explicit VPN exclude"
                )
            }
        }
    }
}

impl std::error::Error for ManagedVpnConfigError {}

#[cfg(test)]
#[path = "tests_config.rs"]
mod tests;
