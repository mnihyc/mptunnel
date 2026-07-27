use crate::platform::config::{
    DnsCaptureConfig, LinuxInterfaceName, LinuxSocketMark, LinuxVpnConfig, RouteMode,
    canonical_net, compare_ip_nets, sort_ip_nets,
};
use crate::platform::route::{AddressFamily, BypassReason, BypassReasons, host_prefix};
use ipnet::IpNet;
use std::cmp::Ordering;
use std::fmt;
use std::net::IpAddr;

const MAX_CARRIER_ENDPOINTS: usize = 128;
const MAX_BOOTSTRAP_DNS_ADDRESSES: usize = 32;
const MAX_LOCAL_NETWORKS: usize = 256;
const MAX_BYPASS_ROUTES: usize = 1_024;
const MAX_CAPTURE_ROUTES: usize = 1_024;

/// Native route captured before VPN policy becomes active.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LinuxNativeRoute {
    family: AddressFamily,
    interface: LinuxInterfaceName,
    gateway: Option<IpAddr>,
    preferred_source: Option<IpAddr>,
    metric: u32,
    onlink: bool,
}

impl LinuxNativeRoute {
    pub fn new(
        family: AddressFamily,
        interface: LinuxInterfaceName,
        gateway: Option<IpAddr>,
        preferred_source: Option<IpAddr>,
        metric: u32,
    ) -> Result<Self, LinuxNativeRouteError> {
        if let Some(gateway) = gateway {
            validate_native_address("gateway", family, gateway)?;
        }
        if let Some(source) = preferred_source {
            validate_native_address("preferred source", family, source)?;
        }
        Ok(Self {
            family,
            interface,
            gateway,
            preferred_source,
            metric,
            onlink: false,
        })
    }

    /// Preserves Linux's explicit on-link gateway assertion.
    pub fn with_onlink(mut self, onlink: bool) -> Result<Self, LinuxNativeRouteError> {
        if onlink && self.gateway.is_none() {
            return Err(LinuxNativeRouteError::OnlinkWithoutGateway);
        }
        self.onlink = onlink;
        Ok(self)
    }

    pub fn family(&self) -> AddressFamily {
        self.family
    }

    pub fn interface(&self) -> &LinuxInterfaceName {
        &self.interface
    }

    pub fn gateway(&self) -> Option<IpAddr> {
        self.gateway
    }

    pub fn preferred_source(&self) -> Option<IpAddr> {
        self.preferred_source
    }

    pub fn metric(&self) -> u32 {
        self.metric
    }

    pub fn onlink(&self) -> bool {
        self.onlink
    }
}

/// A directly reachable native network used for LAN and route-specific bypass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinuxNativeNetwork {
    prefix: IpNet,
    route: LinuxNativeRoute,
}

impl LinuxNativeNetwork {
    pub fn new(prefix: IpNet, route: LinuxNativeRoute) -> Result<Self, LinuxNativeRouteError> {
        let prefix = canonical_net(prefix);
        if prefix.prefix_len() == 0 {
            return Err(LinuxNativeRouteError::LocalNetworkIsDefault(prefix));
        }
        if AddressFamily::of_net(prefix) != route.family() {
            return Err(LinuxNativeRouteError::NetworkFamilyMismatch(prefix));
        }
        Ok(Self { prefix, route })
    }

    pub fn prefix(&self) -> IpNet {
        self.prefix
    }

    pub fn route(&self) -> &LinuxNativeRoute {
        &self.route
    }
}

/// Immutable native-network snapshot used by the pure planner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinuxVpnEnvironment {
    ipv4_default: Option<LinuxNativeRoute>,
    ipv6_default: Option<LinuxNativeRoute>,
    local_networks: Vec<LinuxNativeNetwork>,
}

impl LinuxVpnEnvironment {
    pub fn new(
        defaults: Vec<LinuxNativeRoute>,
        mut local_networks: Vec<LinuxNativeNetwork>,
    ) -> Result<Self, LinuxNativeRouteError> {
        if local_networks.len() > MAX_LOCAL_NETWORKS {
            return Err(LinuxNativeRouteError::TooManyLocalNetworks {
                actual: local_networks.len(),
                maximum: MAX_LOCAL_NETWORKS,
            });
        }
        let mut ipv4_default = None;
        let mut ipv6_default = None;
        for route in defaults {
            let slot = match route.family() {
                AddressFamily::Ipv4 => &mut ipv4_default,
                AddressFamily::Ipv6 => &mut ipv6_default,
            };
            if slot.replace(route).is_some() {
                return Err(LinuxNativeRouteError::DuplicateDefaultRoute);
            }
        }
        local_networks.sort_unstable_by(|left, right| {
            compare_ip_nets(&left.prefix, &right.prefix).then_with(|| left.route.cmp(&right.route))
        });
        for pair in local_networks.windows(2) {
            if pair[0].prefix == pair[1].prefix && pair[0].route != pair[1].route {
                return Err(LinuxNativeRouteError::ConflictingLocalNetwork(
                    pair[0].prefix,
                ));
            }
        }
        local_networks.dedup();
        Ok(Self {
            ipv4_default,
            ipv6_default,
            local_networks,
        })
    }

    pub fn default_route(&self, family: AddressFamily) -> Option<&LinuxNativeRoute> {
        match family {
            AddressFamily::Ipv4 => self.ipv4_default.as_ref(),
            AddressFamily::Ipv6 => self.ipv6_default.as_ref(),
        }
    }

    pub fn local_networks(&self) -> &[LinuxNativeNetwork] {
        &self.local_networks
    }

    fn native_route_for(&self, destination: IpNet) -> Option<&LinuxNativeRoute> {
        self.local_networks
            .iter()
            .filter(|network| {
                network.prefix.prefix_len() <= destination.prefix_len()
                    && network.prefix.contains(&destination.network())
            })
            .max_by_key(|network| network.prefix.prefix_len())
            .map(|network| &network.route)
            .or_else(|| self.default_route(AddressFamily::of_net(destination)))
    }
}

fn validate_native_address(
    field: &'static str,
    family: AddressFamily,
    address: IpAddr,
) -> Result<(), LinuxNativeRouteError> {
    if AddressFamily::of(address) != family {
        return Err(LinuxNativeRouteError::AddressFamilyMismatch { field, address });
    }
    let invalid = match address {
        IpAddr::V4(address) => address.is_unspecified() || address.is_multicast(),
        IpAddr::V6(address) => address.is_unspecified() || address.is_multicast(),
    };
    if invalid {
        return Err(LinuxNativeRouteError::InvalidAddress { field, address });
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LinuxNativeRouteError {
    AddressFamilyMismatch {
        field: &'static str,
        address: IpAddr,
    },
    InvalidAddress {
        field: &'static str,
        address: IpAddr,
    },
    NetworkFamilyMismatch(IpNet),
    LocalNetworkIsDefault(IpNet),
    DuplicateDefaultRoute,
    ConflictingLocalNetwork(IpNet),
    TooManyLocalNetworks {
        actual: usize,
        maximum: usize,
    },
    OnlinkWithoutGateway,
}

impl fmt::Display for LinuxNativeRouteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AddressFamilyMismatch { field, address } => {
                write!(
                    formatter,
                    "native route {field} {address} has the wrong family"
                )
            }
            Self::InvalidAddress { field, address } => {
                write!(formatter, "native route {field} {address} is invalid")
            }
            Self::NetworkFamilyMismatch(network) => write!(
                formatter,
                "native network {network} and its route use different address families"
            ),
            Self::LocalNetworkIsDefault(network) => {
                write!(
                    formatter,
                    "native local network {network} must not be a default route"
                )
            }
            Self::DuplicateDefaultRoute => {
                formatter.write_str("native environment has duplicate default routes")
            }
            Self::ConflictingLocalNetwork(network) => write!(
                formatter,
                "native environment has conflicting routes for local network {network}"
            ),
            Self::TooManyLocalNetworks { actual, maximum } => write!(
                formatter,
                "native environment has {actual} local networks; maximum is {maximum}"
            ),
            Self::OnlinkWithoutGateway => {
                formatter.write_str("native route has onlink flag without a gateway")
            }
        }
    }
}

impl std::error::Error for LinuxNativeRouteError {}

/// One route directed into the TUN's private Linux route table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinuxCaptureRoute {
    pub table: u32,
    pub destination: IpNet,
    pub interface: LinuxInterfaceName,
}

/// Ordered Linux host mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LinuxHostOperation {
    CheckResolvedSupport,
    CreateTun {
        interface: LinuxInterfaceName,
        mtu: u16,
    },
    AddAddress {
        interface: LinuxInterfaceName,
        address: IpNet,
    },
    SetLinkUp {
        interface: LinuxInterfaceName,
    },
    AddBypassRoute {
        table: u32,
        destination: IpNet,
        native: LinuxNativeRoute,
        reasons: BypassReasons,
    },
    AddCaptureRoute(LinuxCaptureRoute),
    ActivateNativeEgressRule {
        family: AddressFamily,
        mark: LinuxSocketMark,
        priority: u32,
    },
    ActivateCaptureRule {
        family: AddressFamily,
        table: u32,
        priority: u32,
    },
    ConfigureDns {
        interface: LinuxInterfaceName,
        servers: Vec<IpAddr>,
        route_all: bool,
    },
}

/// Fully validated deterministic activation plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinuxVpnPlan {
    prepare_operations: Vec<LinuxHostOperation>,
    publish_operations: Vec<LinuxHostOperation>,
}

impl LinuxVpnPlan {
    pub fn build(
        config: &LinuxVpnConfig,
        environment: &LinuxVpnEnvironment,
        carrier_endpoints: impl IntoIterator<Item = IpAddr>,
        bootstrap_dns: impl IntoIterator<Item = IpAddr>,
    ) -> Result<Self, LinuxVpnPlanError> {
        let carrier_endpoints = validated_addresses(carrier_endpoints, MAX_CARRIER_ENDPOINTS)?;
        let bootstrap_dns = validated_addresses(bootstrap_dns, MAX_BOOTSTRAP_DNS_ADDRESSES)?;

        let mut bypasses = Vec::<PlannedBypass>::new();
        for address in carrier_endpoints {
            add_address_bypass(
                &mut bypasses,
                environment,
                address,
                BypassReason::CarrierEndpoint,
            )?;
        }
        for address in bootstrap_dns {
            add_address_bypass(
                &mut bypasses,
                environment,
                address,
                BypassReason::BootstrapDns,
            )?;
        }
        for exclude in config.excludes() {
            add_prefix_bypass(
                &mut bypasses,
                environment,
                *exclude,
                BypassReason::ExplicitExclude,
            )?;
        }
        if config.local_lan() {
            for network in environment.local_networks() {
                add_bypass(
                    &mut bypasses,
                    network.prefix(),
                    network.route().clone(),
                    BypassReason::LocalLan,
                )?;
            }
        }
        if bypasses.len() > MAX_BYPASS_ROUTES {
            return Err(LinuxVpnPlanError::TooManyBypassRoutes {
                actual: bypasses.len(),
                maximum: MAX_BYPASS_ROUTES,
            });
        }

        let mut captures = capture_destinations(config);
        if let Some(dns) = config.dns() {
            reject_bypassed_dns(dns, &bypasses)?;
            captures.extend(dns.servers().iter().copied().map(host_prefix));
        }
        canonical_minimal_routes(&mut captures);
        captures.retain(|capture| {
            !bypasses.iter().any(|bypass| {
                bypass.destination.prefix_len() <= capture.prefix_len()
                    && bypass.destination.contains(&capture.network())
            })
        });
        canonical_minimal_routes(&mut captures);
        if captures.is_empty() {
            return Err(LinuxVpnPlanError::NoEffectiveCaptureRoute);
        }
        if captures.len() > MAX_CAPTURE_ROUTES {
            return Err(LinuxVpnPlanError::TooManyCaptureRoutes {
                actual: captures.len(),
                maximum: MAX_CAPTURE_ROUTES,
            });
        }

        let captures_v4 = captures
            .iter()
            .any(|network| matches!(network, IpNet::V4(_)));
        let captures_v6 = captures
            .iter()
            .any(|network| matches!(network, IpNet::V6(_)));
        bypasses.retain(|bypass| match bypass.destination {
            IpNet::V4(_) => captures_v4,
            IpNet::V6(_) => captures_v6,
        });
        bypasses.sort_unstable_by(compare_bypasses);
        let linux_policy = config.linux_policy();

        let mut prepare_operations = Vec::with_capacity(
            2 + config.addresses().len()
                + bypasses.len()
                + captures.len()
                + usize::from(config.dns().is_some()),
        );
        if config.dns().is_some() {
            prepare_operations.push(LinuxHostOperation::CheckResolvedSupport);
        }
        prepare_operations.push(LinuxHostOperation::CreateTun {
            interface: config.interface().clone(),
            mtu: config.mtu(),
        });
        prepare_operations.extend(config.addresses().iter().copied().map(|address| {
            LinuxHostOperation::AddAddress {
                interface: config.interface().clone(),
                address,
            }
        }));
        prepare_operations.push(LinuxHostOperation::SetLinkUp {
            interface: config.interface().clone(),
        });
        prepare_operations.extend(bypasses.into_iter().map(|bypass| {
            LinuxHostOperation::AddBypassRoute {
                table: linux_policy.route_table(),
                destination: bypass.destination,
                native: bypass.native,
                reasons: bypass.reasons,
            }
        }));
        prepare_operations.extend(captures.into_iter().map(|destination| {
            LinuxHostOperation::AddCaptureRoute(LinuxCaptureRoute {
                table: linux_policy.route_table(),
                destination,
                interface: config.interface().clone(),
            })
        }));

        let captured_family_count = usize::from(captures_v4) + usize::from(captures_v6);
        let mut publish_operations =
            Vec::with_capacity(2 * captured_family_count + usize::from(config.dns().is_some()));
        for family in [AddressFamily::Ipv4, AddressFamily::Ipv6] {
            if matches!(family, AddressFamily::Ipv4) && !captures_v4
                || matches!(family, AddressFamily::Ipv6) && !captures_v6
            {
                continue;
            }
            publish_operations.push(LinuxHostOperation::ActivateNativeEgressRule {
                family,
                mark: linux_policy.socket_mark(),
                priority: linux_policy.native_rule_priority(),
            });
        }
        for family in [AddressFamily::Ipv4, AddressFamily::Ipv6] {
            if matches!(family, AddressFamily::Ipv4) && !captures_v4
                || matches!(family, AddressFamily::Ipv6) && !captures_v6
            {
                continue;
            }
            publish_operations.push(LinuxHostOperation::ActivateCaptureRule {
                family,
                table: linux_policy.route_table(),
                priority: linux_policy.capture_rule_priority(),
            });
        }
        if let Some(dns) = config.dns() {
            publish_operations.push(LinuxHostOperation::ConfigureDns {
                interface: config.interface().clone(),
                servers: dns.servers().to_vec(),
                route_all: true,
            });
        }
        debug_assert!(bypasses_precede_capture(&prepare_operations));
        debug_assert!(prepare_operations.iter().all(|operation| !matches!(
            operation,
            LinuxHostOperation::ActivateNativeEgressRule { .. }
                | LinuxHostOperation::ActivateCaptureRule { .. }
                | LinuxHostOperation::ConfigureDns { .. }
        )));
        debug_assert!(publish_operations.iter().all(|operation| matches!(
            operation,
            LinuxHostOperation::ActivateNativeEgressRule { .. }
                | LinuxHostOperation::ActivateCaptureRule { .. }
                | LinuxHostOperation::ConfigureDns { .. }
        )));
        Ok(Self {
            prepare_operations,
            publish_operations,
        })
    }

    /// Inert host work that may run before the packet worker exists.
    ///
    /// Capture routes live only in the private VPN table during this phase.
    /// No policy rule or DNS setting can direct host traffic to the TUN.
    pub fn prepare_operations(&self) -> &[LinuxHostOperation] {
        &self.prepare_operations
    }

    /// Publication work allowed only after the packet worker reports ready.
    ///
    /// Policy rules activate the prepared table before link DNS is published.
    pub fn publish_operations(&self) -> &[LinuxHostOperation] {
        &self.publish_operations
    }

    pub(crate) fn into_phases(self) -> (Vec<LinuxHostOperation>, Vec<LinuxHostOperation>) {
        (self.prepare_operations, self.publish_operations)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PlannedBypass {
    destination: IpNet,
    native: LinuxNativeRoute,
    reasons: BypassReasons,
}

fn validated_addresses(
    addresses: impl IntoIterator<Item = IpAddr>,
    maximum: usize,
) -> Result<Vec<IpAddr>, LinuxVpnPlanError> {
    let mut addresses = addresses.into_iter().collect::<Vec<_>>();
    addresses.sort_unstable();
    addresses.dedup();
    if addresses.len() > maximum {
        return Err(LinuxVpnPlanError::TooManyResolvedAddresses {
            actual: addresses.len(),
            maximum,
        });
    }
    if let Some(address) = addresses
        .iter()
        .copied()
        .find(|address| address.is_unspecified() || address.is_loopback() || address.is_multicast())
    {
        return Err(LinuxVpnPlanError::InvalidResolvedAddress(address));
    }
    Ok(addresses)
}

fn add_address_bypass(
    bypasses: &mut Vec<PlannedBypass>,
    environment: &LinuxVpnEnvironment,
    address: IpAddr,
    reason: BypassReason,
) -> Result<(), LinuxVpnPlanError> {
    add_prefix_bypass(bypasses, environment, host_prefix(address), reason)
}

fn add_prefix_bypass(
    bypasses: &mut Vec<PlannedBypass>,
    environment: &LinuxVpnEnvironment,
    destination: IpNet,
    reason: BypassReason,
) -> Result<(), LinuxVpnPlanError> {
    let destination = canonical_net(destination);
    let native = environment.native_route_for(destination).cloned().ok_or(
        LinuxVpnPlanError::MissingNativeRoute {
            destination,
            reason,
        },
    )?;
    add_bypass(bypasses, destination, native, reason)
}

fn add_bypass(
    bypasses: &mut Vec<PlannedBypass>,
    destination: IpNet,
    native: LinuxNativeRoute,
    reason: BypassReason,
) -> Result<(), LinuxVpnPlanError> {
    if let Some(existing) = bypasses
        .iter_mut()
        .find(|existing| existing.destination == destination)
    {
        if existing.native != native {
            return Err(LinuxVpnPlanError::ConflictingNativeRoutes(destination));
        }
        existing.reasons.insert(reason);
        return Ok(());
    }
    bypasses.push(PlannedBypass {
        destination,
        native,
        reasons: BypassReasons::one(reason),
    });
    Ok(())
}

fn reject_bypassed_dns(
    dns: &DnsCaptureConfig,
    bypasses: &[PlannedBypass],
) -> Result<(), LinuxVpnPlanError> {
    if let Some(server) = dns.servers().iter().copied().find(|server| {
        bypasses
            .iter()
            .any(|bypass| bypass.destination.contains(server))
    }) {
        return Err(LinuxVpnPlanError::DnsServerBypassed(server));
    }
    Ok(())
}

fn capture_destinations(config: &LinuxVpnConfig) -> Vec<IpNet> {
    match config.route_mode() {
        RouteMode::Full => {
            let mut routes = Vec::with_capacity(config.addresses().len());
            for address in config.addresses() {
                routes.push(match address {
                    IpNet::V4(_) => "0.0.0.0/0".parse().expect("IPv4 default"),
                    IpNet::V6(_) => "::/0".parse().expect("IPv6 default"),
                });
            }
            routes
        }
        RouteMode::Split(includes) => includes.clone(),
    }
}

fn canonical_minimal_routes(routes: &mut Vec<IpNet>) {
    for route in routes.iter_mut() {
        *route = canonical_net(*route);
    }
    sort_ip_nets(routes);
    routes.dedup();
    let snapshot = routes.clone();
    routes.retain(|candidate| {
        !snapshot.iter().any(|other| {
            other != candidate
                && other.addr().is_ipv4() == candidate.addr().is_ipv4()
                && other.prefix_len() < candidate.prefix_len()
                && other.contains(&candidate.network())
        })
    });
}

fn compare_bypasses(left: &PlannedBypass, right: &PlannedBypass) -> Ordering {
    left.reasons
        .order()
        .cmp(&right.reasons.order())
        .then_with(|| compare_ip_nets(&left.destination, &right.destination))
        .then_with(|| left.native.cmp(&right.native))
}

fn bypasses_precede_capture(operations: &[LinuxHostOperation]) -> bool {
    let last_bypass = operations
        .iter()
        .rposition(|operation| matches!(operation, LinuxHostOperation::AddBypassRoute { .. }));
    let first_capture = operations
        .iter()
        .position(|operation| matches!(operation, LinuxHostOperation::AddCaptureRoute(_)));
    match (last_bypass, first_capture) {
        (Some(last_bypass), Some(first_capture)) => last_bypass < first_capture,
        _ => true,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LinuxVpnPlanError {
    TooManyResolvedAddresses {
        actual: usize,
        maximum: usize,
    },
    InvalidResolvedAddress(IpAddr),
    MissingNativeRoute {
        destination: IpNet,
        reason: BypassReason,
    },
    ConflictingNativeRoutes(IpNet),
    DnsServerBypassed(IpAddr),
    NoEffectiveCaptureRoute,
    TooManyBypassRoutes {
        actual: usize,
        maximum: usize,
    },
    TooManyCaptureRoutes {
        actual: usize,
        maximum: usize,
    },
}

impl fmt::Display for LinuxVpnPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyResolvedAddresses { actual, maximum } => write!(
                formatter,
                "VPN plan has {actual} resolved bypass addresses; maximum is {maximum}"
            ),
            Self::InvalidResolvedAddress(address) => {
                write!(formatter, "invalid resolved VPN bypass address {address}")
            }
            Self::MissingNativeRoute {
                destination,
                reason,
            } => write!(
                formatter,
                "no native route exists for {destination} required by {reason:?}"
            ),
            Self::ConflictingNativeRoutes(destination) => write!(
                formatter,
                "VPN bypass {destination} resolves to conflicting native routes"
            ),
            Self::DnsServerBypassed(server) => write!(
                formatter,
                "captured DNS server {server} is also selected for native bypass"
            ),
            Self::NoEffectiveCaptureRoute => {
                formatter.write_str("VPN excludes remove every effective capture route")
            }
            Self::TooManyBypassRoutes { actual, maximum } => write!(
                formatter,
                "VPN plan has {actual} bypass routes; maximum is {maximum}"
            ),
            Self::TooManyCaptureRoutes { actual, maximum } => write!(
                formatter,
                "VPN plan has {actual} capture routes; maximum is {maximum}"
            ),
        }
    }
}

impl std::error::Error for LinuxVpnPlanError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::platform::config::DnsCaptureConfig;

    fn interface(name: &str) -> LinuxInterfaceName {
        LinuxInterfaceName::parse(name).expect("interface")
    }

    fn route(family: AddressFamily, interface_name: &str, gateway: &str) -> LinuxNativeRoute {
        LinuxNativeRoute::new(
            family,
            interface(interface_name),
            Some(gateway.parse().expect("gateway")),
            None,
            100,
        )
        .expect("native route")
    }

    fn dual_environment() -> LinuxVpnEnvironment {
        LinuxVpnEnvironment::new(
            vec![
                route(AddressFamily::Ipv4, "eth0", "192.0.2.1"),
                route(AddressFamily::Ipv6, "eth0", "2001:db8::1"),
            ],
            vec![
                LinuxNativeNetwork::new(
                    "192.168.0.0/16".parse().expect("LAN"),
                    LinuxNativeRoute::new(
                        AddressFamily::Ipv4,
                        interface("eth0"),
                        None,
                        Some("192.168.1.2".parse().expect("source")),
                        50,
                    )
                    .expect("LAN route"),
                )
                .expect("LAN"),
                LinuxNativeNetwork::new(
                    "fd12:3456::/48".parse().expect("LAN"),
                    LinuxNativeRoute::new(
                        AddressFamily::Ipv6,
                        interface("eth0"),
                        None,
                        Some("fd12:3456::2".parse().expect("source")),
                        50,
                    )
                    .expect("LAN route"),
                )
                .expect("LAN"),
            ],
        )
        .expect("environment")
    }

    fn dual_full_config() -> LinuxVpnConfig {
        LinuxVpnConfig::new(
            interface("mptun0"),
            vec![
                "10.88.0.1/24".parse().expect("address"),
                "fd00:88::1/64".parse().expect("address"),
            ],
            1500,
            RouteMode::Full,
        )
        .expect("config")
    }

    #[test]
    fn full_dual_stack_plan_has_exact_safe_activation_order() {
        let config = dual_full_config()
            .with_excludes(vec!["198.51.100.0/24".parse().expect("exclude")])
            .expect("exclude")
            .with_local_lan(true)
            .with_dns(
                DnsCaptureConfig::new(vec![
                    "1.1.1.1".parse().expect("DNS"),
                    "2606:4700:4700::1111".parse().expect("DNS"),
                ])
                .expect("DNS"),
            )
            .expect("DNS");
        let plan = LinuxVpnPlan::build(
            &config,
            &dual_environment(),
            [
                "203.0.113.9".parse().expect("carrier"),
                "2001:db8:ffff::9".parse().expect("carrier"),
            ],
            ["9.9.9.9".parse().expect("bootstrap")],
        )
        .expect("plan");

        let prepare = plan.prepare_operations();
        let publish = plan.publish_operations();
        assert!(matches!(
            prepare[0],
            LinuxHostOperation::CheckResolvedSupport
        ));
        assert!(matches!(prepare[1], LinuxHostOperation::CreateTun { .. }));
        assert!(matches!(prepare[2], LinuxHostOperation::AddAddress { .. }));
        assert!(matches!(prepare[3], LinuxHostOperation::AddAddress { .. }));
        assert!(matches!(prepare[4], LinuxHostOperation::SetLinkUp { .. }));

        let last_bypass = prepare
            .iter()
            .rposition(|operation| matches!(operation, LinuxHostOperation::AddBypassRoute { .. }))
            .expect("bypass");
        let first_capture = prepare
            .iter()
            .position(|operation| matches!(operation, LinuxHostOperation::AddCaptureRoute(_)))
            .expect("capture");
        assert!(last_bypass < first_capture);
        assert!(prepare.iter().all(|operation| !matches!(
            operation,
            LinuxHostOperation::ActivateNativeEgressRule { .. }
                | LinuxHostOperation::ActivateCaptureRule { .. }
                | LinuxHostOperation::ConfigureDns { .. }
        )));
        assert!(publish.iter().take(2).all(|operation| matches!(
            operation,
            LinuxHostOperation::ActivateNativeEgressRule {
                mark: crate::platform::config::DEFAULT_LINUX_SOCKET_MARK,
                priority: crate::platform::config::DEFAULT_LINUX_NATIVE_RULE_PRIORITY,
                ..
            }
        )));
        assert!(publish.iter().skip(2).take(2).all(|operation| matches!(
            operation,
            LinuxHostOperation::ActivateCaptureRule {
                priority: crate::platform::config::DEFAULT_LINUX_CAPTURE_RULE_PRIORITY,
                ..
            }
        )));
        assert!(matches!(
            publish.last(),
            Some(LinuxHostOperation::ConfigureDns {
                route_all: true,
                ..
            })
        ));

        let bypass_reasons = prepare
            .iter()
            .filter_map(|operation| match operation {
                LinuxHostOperation::AddBypassRoute { reasons, .. } => Some(*reasons),
                _ => None,
            })
            .collect::<Vec<_>>();
        let ranks = bypass_reasons
            .iter()
            .map(|reasons| reasons.order())
            .collect::<Vec<_>>();
        assert!(ranks.windows(2).all(|pair| pair[0] <= pair[1]));
        for reason in [
            BypassReason::CarrierEndpoint,
            BypassReason::BootstrapDns,
            BypassReason::ExplicitExclude,
            BypassReason::LocalLan,
        ] {
            assert!(
                bypass_reasons
                    .iter()
                    .any(|reasons| reasons.contains(reason))
            );
        }
    }

    #[test]
    fn custom_linux_mark_policy_is_ordered_before_capture_without_removing_bypasses() {
        let mark = crate::platform::config::LinuxSocketMark::new(0x1234).expect("mark");
        let policy = crate::platform::config::LinuxPolicyConfig::new(40_000, 7_000, 8_000, mark)
            .expect("policy");
        let config = dual_full_config().with_linux_policy(policy);
        let plan = LinuxVpnPlan::build(
            &config,
            &dual_environment(),
            ["203.0.113.9".parse().expect("carrier")],
            ["9.9.9.9".parse().expect("bootstrap")],
        )
        .expect("plan");

        assert!(matches!(
            plan.publish_operations(),
            [
                LinuxHostOperation::ActivateNativeEgressRule {
                    family: AddressFamily::Ipv4,
                    mark: actual_v4,
                    priority: 7_000,
                },
                LinuxHostOperation::ActivateNativeEgressRule {
                    family: AddressFamily::Ipv6,
                    mark: actual_v6,
                    priority: 7_000,
                },
                LinuxHostOperation::ActivateCaptureRule {
                    family: AddressFamily::Ipv4,
                    table: 40_000,
                    priority: 8_000,
                },
                LinuxHostOperation::ActivateCaptureRule {
                    family: AddressFamily::Ipv6,
                    table: 40_000,
                    priority: 8_000,
                },
            ] if *actual_v4 == mark && *actual_v6 == mark
        ));
        let bypass_reasons = plan
            .prepare_operations()
            .iter()
            .filter_map(|operation| match operation {
                LinuxHostOperation::AddBypassRoute { reasons, .. } => Some(*reasons),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(
            bypass_reasons
                .iter()
                .any(|reasons| reasons.contains(BypassReason::CarrierEndpoint))
        );
        assert!(
            bypass_reasons
                .iter()
                .any(|reasons| reasons.contains(BypassReason::BootstrapDns))
        );
    }

    #[test]
    fn explicit_exclude_wins_over_more_specific_split_include() {
        let config = LinuxVpnConfig::new(
            interface("mptun0"),
            vec!["10.88.0.1/24".parse().expect("address")],
            1500,
            RouteMode::Split(vec![
                "10.10.0.0/16".parse().expect("include"),
                "172.16.0.0/12".parse().expect("include"),
            ]),
        )
        .expect("config")
        .with_excludes(vec!["10.0.0.0/8".parse().expect("exclude")])
        .expect("exclude");
        let plan = LinuxVpnPlan::build(
            &config,
            &dual_environment(),
            ["203.0.113.9".parse().expect("carrier")],
            [],
        )
        .expect("plan");
        let captures = plan
            .prepare_operations()
            .iter()
            .filter_map(|operation| match operation {
                LinuxHostOperation::AddCaptureRoute(route) => Some(route.destination),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            captures,
            vec!["172.16.0.0/12".parse().expect("remaining capture")]
        );
    }

    #[test]
    fn duplicate_bypass_addresses_merge_reasons_without_reordering() {
        let config = dual_full_config();
        let address: IpAddr = "203.0.113.9".parse().expect("address");
        let plan = LinuxVpnPlan::build(&config, &dual_environment(), [address, address], [address])
            .expect("plan");
        let matching = plan
            .prepare_operations()
            .iter()
            .filter_map(|operation| match operation {
                LinuxHostOperation::AddBypassRoute {
                    destination,
                    reasons,
                    ..
                } if destination.contains(&address) => Some(*reasons),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(matching.len(), 1);
        assert!(matching[0].contains(BypassReason::CarrierEndpoint));
        assert!(matching[0].contains(BypassReason::BootstrapDns));
    }

    #[test]
    fn dns_gets_a_capture_route_in_split_mode_and_cannot_be_bypassed() {
        let config = LinuxVpnConfig::new(
            interface("mptun0"),
            vec!["10.88.0.1/24".parse().expect("address")],
            1500,
            RouteMode::Split(vec!["10.0.0.0/8".parse().expect("include")]),
        )
        .expect("config")
        .with_dns(DnsCaptureConfig::new(vec!["1.1.1.1".parse().expect("DNS")]).expect("DNS"))
        .expect("DNS");
        let plan = LinuxVpnPlan::build(
            &config,
            &dual_environment(),
            ["203.0.113.9".parse().expect("carrier")],
            [],
        )
        .expect("plan");
        assert!(plan.prepare_operations().iter().any(|operation| matches!(
            operation,
            LinuxHostOperation::AddCaptureRoute(LinuxCaptureRoute { destination, .. })
                if *destination == "1.1.1.1/32".parse::<IpNet>().expect("DNS route")
        )));

        let conflict = LinuxVpnPlan::build(
            &config,
            &dual_environment(),
            ["203.0.113.9".parse().expect("carrier")],
            ["1.1.1.1".parse().expect("bootstrap")],
        );
        assert_eq!(
            conflict,
            Err(LinuxVpnPlanError::DnsServerBypassed(
                "1.1.1.1".parse().expect("DNS")
            ))
        );
    }

    #[test]
    fn plan_is_deterministic_across_input_order_and_duplicates() {
        let config = dual_full_config().with_local_lan(true);
        let environment = dual_environment();
        let first = LinuxVpnPlan::build(
            &config,
            &environment,
            [
                "203.0.113.10".parse().expect("carrier"),
                "203.0.113.9".parse().expect("carrier"),
            ],
            [
                "9.9.9.9".parse().expect("bootstrap"),
                "8.8.8.8".parse().expect("bootstrap"),
            ],
        )
        .expect("first plan");
        let second = LinuxVpnPlan::build(
            &config,
            &environment,
            [
                "203.0.113.9".parse().expect("carrier"),
                "203.0.113.10".parse().expect("carrier"),
                "203.0.113.9".parse().expect("carrier"),
            ],
            [
                "8.8.8.8".parse().expect("bootstrap"),
                "9.9.9.9".parse().expect("bootstrap"),
            ],
        )
        .expect("second plan");
        assert_eq!(first, second);
    }

    #[test]
    fn direct_only_full_vpn_uses_native_mark_without_static_bypasses() {
        let config = dual_full_config();
        let plan = LinuxVpnPlan::build(&config, &dual_environment(), [], [])
            .expect("direct-only full VPN");
        assert!(
            !plan
                .prepare_operations()
                .iter()
                .any(|operation| matches!(operation, LinuxHostOperation::AddBypassRoute { .. }))
        );
        for family in [AddressFamily::Ipv4, AddressFamily::Ipv6] {
            let native = plan
                .publish_operations()
                .iter()
                .position(|operation| {
                    matches!(
                        operation,
                        LinuxHostOperation::ActivateNativeEgressRule {
                            family: candidate,
                            ..
                        } if *candidate == family
                    )
                })
                .expect("native-main mark rule");
            let capture = plan
                .publish_operations()
                .iter()
                .position(|operation| {
                    matches!(
                        operation,
                        LinuxHostOperation::ActivateCaptureRule {
                            family: candidate,
                            ..
                        } if *candidate == family
                    )
                })
                .expect("capture rule");
            assert!(native < capture);
        }
    }

    #[test]
    fn planner_requires_native_routes_for_known_endpoints() {
        let config = dual_full_config();
        let v4_only_environment = LinuxVpnEnvironment::new(
            vec![route(AddressFamily::Ipv4, "eth0", "192.0.2.1")],
            vec![],
        )
        .expect("environment");
        assert!(matches!(
            LinuxVpnPlan::build(
                &config,
                &v4_only_environment,
                ["2001:db8:ffff::9".parse().expect("carrier")],
                [],
            ),
            Err(LinuxVpnPlanError::MissingNativeRoute { .. })
        ));
    }

    #[test]
    fn environment_rejects_conflicting_or_ambiguous_native_state() {
        assert_eq!(
            LinuxVpnEnvironment::new(
                vec![
                    route(AddressFamily::Ipv4, "eth0", "192.0.2.1"),
                    route(AddressFamily::Ipv4, "eth1", "198.51.100.1"),
                ],
                vec![],
            ),
            Err(LinuxNativeRouteError::DuplicateDefaultRoute)
        );

        let first = LinuxNativeNetwork::new(
            "192.168.0.0/16".parse().expect("LAN"),
            LinuxNativeRoute::new(AddressFamily::Ipv4, interface("eth0"), None, None, 10)
                .expect("route"),
        )
        .expect("network");
        let second = LinuxNativeNetwork::new(
            "192.168.0.0/16".parse().expect("LAN"),
            LinuxNativeRoute::new(AddressFamily::Ipv4, interface("eth1"), None, None, 10)
                .expect("route"),
        )
        .expect("network");
        assert_eq!(
            LinuxVpnEnvironment::new(vec![], vec![first, second]),
            Err(LinuxNativeRouteError::ConflictingLocalNetwork(
                "192.168.0.0/16".parse().expect("LAN")
            ))
        );
    }

    #[test]
    fn every_bypass_precedes_every_capture_for_large_plans() {
        let config = dual_full_config()
            .with_excludes(
                (0..64)
                    .map(|index| format!("100.{index}.0.0/16").parse().expect("exclude"))
                    .collect(),
            )
            .expect("excludes");
        let carriers = (1..=64)
            .map(|last| format!("203.0.113.{last}").parse().expect("carrier"))
            .collect::<Vec<_>>();
        let plan =
            LinuxVpnPlan::build(&config, &dual_environment(), carriers, []).expect("large plan");
        assert!(bypasses_precede_capture(plan.prepare_operations()));
    }
}
