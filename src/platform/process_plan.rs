//! Pure route plan for process-managed desktop VPN adapters.
//!
//! Windows and privileged-process macOS adapters share one routing model: the
//! native table is snapshotted before the tunnel exists, exact bypass routes
//! are installed first, and capture routes plus DNS are published only after
//! the packet worker is ready. Packet-device creation and address assignment
//! remain adapter operations because their native ownership differs.

use crate::platform::config::{
    DnsCaptureConfig, ManagedVpnConfig, RouteMode, canonical_net, compare_ip_nets, sort_ip_nets,
};
use crate::platform::route::{AddressFamily, BypassReason, BypassReasons, host_prefix};
use ipnet::IpNet;
use std::cmp::Ordering;
use std::fmt;
use std::net::IpAddr;
use std::num::NonZeroU32;

const MAX_NATIVE_NETWORKS: usize = 2_048;
const MAX_BYPASS_ROUTES: usize = 1_024;
const MAX_CAPTURE_ROUTES: usize = 1_024;

/// Default route metric used for a Windows tunnel route.
///
/// macOS route sockets do not expose an equivalent per-route metric and the
/// macOS backend ignores this value.
pub const DEFAULT_PROCESS_CAPTURE_METRIC: u32 = 5;

/// One native egress path captured before VPN publication.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProcessNativeRoute {
    family: AddressFamily,
    interface_index: NonZeroU32,
    gateway: Option<IpAddr>,
    metric: u32,
}

impl ProcessNativeRoute {
    pub fn new(
        family: AddressFamily,
        interface_index: u32,
        gateway: Option<IpAddr>,
        metric: u32,
    ) -> Result<Self, ProcessNativeRouteError> {
        let interface_index =
            NonZeroU32::new(interface_index).ok_or(ProcessNativeRouteError::ZeroInterfaceIndex)?;
        if let Some(gateway) = gateway {
            if AddressFamily::of(gateway) != family {
                return Err(ProcessNativeRouteError::GatewayFamilyMismatch(gateway));
            }
            let invalid = match gateway {
                IpAddr::V4(address) => address.is_unspecified() || address.is_multicast(),
                IpAddr::V6(address) => address.is_unspecified() || address.is_multicast(),
            };
            if invalid {
                return Err(ProcessNativeRouteError::InvalidGateway(gateway));
            }
        }
        Ok(Self {
            family,
            interface_index,
            gateway,
            metric,
        })
    }

    pub const fn family(&self) -> AddressFamily {
        self.family
    }

    pub const fn interface_index(&self) -> NonZeroU32 {
        self.interface_index
    }

    pub const fn gateway(&self) -> Option<IpAddr> {
        self.gateway
    }

    pub const fn metric(&self) -> u32 {
        self.metric
    }
}

/// One non-default native route from the pre-VPN snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessNativeNetwork {
    prefix: IpNet,
    route: ProcessNativeRoute,
    directly_connected: bool,
}

impl ProcessNativeNetwork {
    pub fn new(
        prefix: IpNet,
        route: ProcessNativeRoute,
        directly_connected: bool,
    ) -> Result<Self, ProcessNativeRouteError> {
        let prefix = canonical_net(prefix);
        if prefix.prefix_len() == 0 {
            return Err(ProcessNativeRouteError::NetworkIsDefault(prefix));
        }
        if AddressFamily::of_net(prefix) != route.family() {
            return Err(ProcessNativeRouteError::NetworkFamilyMismatch(prefix));
        }
        if directly_connected && route.gateway().is_some() {
            return Err(ProcessNativeRouteError::ConnectedNetworkHasGateway(prefix));
        }
        Ok(Self {
            prefix,
            route,
            directly_connected,
        })
    }

    pub const fn prefix(&self) -> IpNet {
        self.prefix
    }

    pub const fn route(&self) -> &ProcessNativeRoute {
        &self.route
    }

    pub const fn directly_connected(&self) -> bool {
        self.directly_connected
    }
}

/// Immutable native routing snapshot used by the deterministic planner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessVpnEnvironment {
    ipv4_default: Option<ProcessNativeRoute>,
    ipv6_default: Option<ProcessNativeRoute>,
    native_networks: Vec<ProcessNativeNetwork>,
}

impl ProcessVpnEnvironment {
    pub fn new(
        defaults: impl IntoIterator<Item = ProcessNativeRoute>,
        mut native_networks: Vec<ProcessNativeNetwork>,
    ) -> Result<Self, ProcessNativeRouteError> {
        if native_networks.len() > MAX_NATIVE_NETWORKS {
            return Err(ProcessNativeRouteError::TooManyNativeNetworks {
                actual: native_networks.len(),
                maximum: MAX_NATIVE_NETWORKS,
            });
        }

        let mut ipv4_default = None;
        let mut ipv6_default = None;
        for route in defaults {
            let slot = match route.family() {
                AddressFamily::Ipv4 => &mut ipv4_default,
                AddressFamily::Ipv6 => &mut ipv6_default,
            };
            match slot {
                Some(existing) if existing != &route => {
                    return Err(ProcessNativeRouteError::ConflictingDefaultRoute(
                        route.family(),
                    ));
                }
                Some(_) => {}
                None => *slot = Some(route),
            }
        }

        native_networks.sort_unstable_by(|left, right| {
            compare_ip_nets(&left.prefix, &right.prefix)
                .then_with(|| left.route.cmp(&right.route))
                .then_with(|| left.directly_connected.cmp(&right.directly_connected))
        });
        for pair in native_networks.windows(2) {
            if pair[0].prefix == pair[1].prefix && pair[0] != pair[1] {
                return Err(ProcessNativeRouteError::ConflictingNativeNetwork(
                    pair[0].prefix,
                ));
            }
        }
        native_networks.dedup();

        Ok(Self {
            ipv4_default,
            ipv6_default,
            native_networks,
        })
    }

    pub fn default_route(&self, family: AddressFamily) -> Option<&ProcessNativeRoute> {
        match family {
            AddressFamily::Ipv4 => self.ipv4_default.as_ref(),
            AddressFamily::Ipv6 => self.ipv6_default.as_ref(),
        }
    }

    pub fn native_networks(&self) -> &[ProcessNativeNetwork] {
        &self.native_networks
    }

    /// Resolves an address through the immutable pre-VPN snapshot using the
    /// same longest-prefix rule as static bypass planning.
    pub fn native_route_for_address(&self, address: IpAddr) -> Option<&ProcessNativeRoute> {
        self.native_route_for(host_prefix(address))
    }

    fn native_route_for(&self, destination: IpNet) -> Option<&ProcessNativeRoute> {
        self.native_networks
            .iter()
            .filter(|network| {
                network.prefix.prefix_len() <= destination.prefix_len()
                    && network.prefix.contains(&destination.network())
            })
            .max_by_key(|network| network.prefix.prefix_len())
            .map(ProcessNativeNetwork::route)
            .or_else(|| self.default_route(AddressFamily::of_net(destination)))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessNativeRouteError {
    ZeroInterfaceIndex,
    GatewayFamilyMismatch(IpAddr),
    InvalidGateway(IpAddr),
    NetworkFamilyMismatch(IpNet),
    NetworkIsDefault(IpNet),
    ConnectedNetworkHasGateway(IpNet),
    ConflictingDefaultRoute(AddressFamily),
    ConflictingNativeNetwork(IpNet),
    TooManyNativeNetworks { actual: usize, maximum: usize },
}

impl fmt::Display for ProcessNativeRouteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroInterfaceIndex => {
                formatter.write_str("native route interface index must be nonzero")
            }
            Self::GatewayFamilyMismatch(gateway) => {
                write!(
                    formatter,
                    "native route gateway {gateway} has the wrong family"
                )
            }
            Self::InvalidGateway(gateway) => {
                write!(formatter, "native route gateway {gateway} is invalid")
            }
            Self::NetworkFamilyMismatch(network) => write!(
                formatter,
                "native network {network} and its egress route use different address families"
            ),
            Self::NetworkIsDefault(network) => {
                write!(
                    formatter,
                    "native network {network} must not be a default route"
                )
            }
            Self::ConnectedNetworkHasGateway(network) => write!(
                formatter,
                "directly connected native network {network} must not have a gateway"
            ),
            Self::ConflictingDefaultRoute(family) => {
                write!(
                    formatter,
                    "native snapshot has conflicting {family:?} defaults"
                )
            }
            Self::ConflictingNativeNetwork(network) => {
                write!(
                    formatter,
                    "native snapshot has conflicting routes for {network}"
                )
            }
            Self::TooManyNativeNetworks { actual, maximum } => write!(
                formatter,
                "native snapshot has {actual} non-default routes; maximum is {maximum}"
            ),
        }
    }
}

impl std::error::Error for ProcessNativeRouteError {}

/// Ordered mutation owned by a Windows or privileged-process macOS adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessHostOperation {
    AddBypassRoute {
        destination: IpNet,
        native: ProcessNativeRoute,
        reasons: BypassReasons,
    },
    AddCaptureRoute {
        destination: IpNet,
        tunnel_interface_index: NonZeroU32,
        metric: u32,
    },
    ConfigureDns {
        servers: Vec<IpAddr>,
    },
}

/// Fully validated two-phase desktop route/DNS plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessVpnPlan {
    prepare_operations: Vec<ProcessHostOperation>,
    publish_operations: Vec<ProcessHostOperation>,
}

impl ProcessVpnPlan {
    pub fn build(
        config: &ManagedVpnConfig,
        environment: &ProcessVpnEnvironment,
        tunnel_interface_index: u32,
        carrier_endpoints: impl IntoIterator<Item = IpAddr>,
        bootstrap_dns: impl IntoIterator<Item = IpAddr>,
    ) -> Result<Self, ProcessVpnPlanError> {
        Self::build_with_metric(
            config,
            environment,
            tunnel_interface_index,
            DEFAULT_PROCESS_CAPTURE_METRIC,
            carrier_endpoints,
            bootstrap_dns,
        )
    }

    pub fn build_with_metric(
        config: &ManagedVpnConfig,
        environment: &ProcessVpnEnvironment,
        tunnel_interface_index: u32,
        capture_metric: u32,
        carrier_endpoints: impl IntoIterator<Item = IpAddr>,
        bootstrap_dns: impl IntoIterator<Item = IpAddr>,
    ) -> Result<Self, ProcessVpnPlanError> {
        let tunnel_interface_index = NonZeroU32::new(tunnel_interface_index)
            .ok_or(ProcessVpnPlanError::ZeroTunnelInterfaceIndex)?;

        let mut bypasses = Vec::<PlannedBypass>::new();
        for address in canonical_addresses(carrier_endpoints) {
            add_prefix_bypass(
                &mut bypasses,
                environment,
                host_prefix(address),
                BypassReason::CarrierEndpoint,
                tunnel_interface_index,
            )?;
        }
        for address in canonical_addresses(bootstrap_dns) {
            add_prefix_bypass(
                &mut bypasses,
                environment,
                host_prefix(address),
                BypassReason::BootstrapDns,
                tunnel_interface_index,
            )?;
        }
        for destination in config.excludes() {
            add_prefix_bypass(
                &mut bypasses,
                environment,
                *destination,
                BypassReason::ExplicitExclude,
                tunnel_interface_index,
            )?;
        }
        if config.local_lan() {
            for network in environment
                .native_networks()
                .iter()
                .filter(|network| network.directly_connected())
            {
                add_bypass(
                    &mut bypasses,
                    network.prefix(),
                    network.route().clone(),
                    BypassReason::LocalLan,
                    tunnel_interface_index,
                )?;
            }
        }
        if bypasses.len() > MAX_BYPASS_ROUTES {
            return Err(ProcessVpnPlanError::TooManyBypassRoutes {
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
            return Err(ProcessVpnPlanError::NoEffectiveCaptureRoute);
        }
        if captures.len() > MAX_CAPTURE_ROUTES {
            return Err(ProcessVpnPlanError::TooManyCaptureRoutes {
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

        let prepare_operations = bypasses
            .into_iter()
            .map(|bypass| ProcessHostOperation::AddBypassRoute {
                destination: bypass.destination,
                native: bypass.native,
                reasons: bypass.reasons,
            })
            .collect::<Vec<_>>();
        let mut publish_operations = captures
            .into_iter()
            .map(|destination| ProcessHostOperation::AddCaptureRoute {
                destination,
                tunnel_interface_index,
                metric: capture_metric,
            })
            .collect::<Vec<_>>();
        if let Some(dns) = config.dns() {
            publish_operations.push(ProcessHostOperation::ConfigureDns {
                servers: dns.servers().to_vec(),
            });
        }

        debug_assert!(
            prepare_operations
                .iter()
                .all(|operation| matches!(operation, ProcessHostOperation::AddBypassRoute { .. }))
        );
        debug_assert!(publish_operations.iter().all(|operation| matches!(
            operation,
            ProcessHostOperation::AddCaptureRoute { .. }
                | ProcessHostOperation::ConfigureDns { .. }
        )));
        Ok(Self {
            prepare_operations,
            publish_operations,
        })
    }

    /// Native bypasses installed before any catch-all route exists.
    pub fn prepare_operations(&self) -> &[ProcessHostOperation] {
        &self.prepare_operations
    }

    /// Capture routes followed by DNS, allowed only after worker readiness.
    pub fn publish_operations(&self) -> &[ProcessHostOperation] {
        &self.publish_operations
    }

    pub(crate) fn into_phases(self) -> (Vec<ProcessHostOperation>, Vec<ProcessHostOperation>) {
        (self.prepare_operations, self.publish_operations)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PlannedBypass {
    destination: IpNet,
    native: ProcessNativeRoute,
    reasons: BypassReasons,
}

fn canonical_addresses(addresses: impl IntoIterator<Item = IpAddr>) -> Vec<IpAddr> {
    let mut addresses = addresses.into_iter().collect::<Vec<_>>();
    addresses.sort_unstable();
    addresses.dedup();
    addresses
}

fn add_prefix_bypass(
    bypasses: &mut Vec<PlannedBypass>,
    environment: &ProcessVpnEnvironment,
    destination: IpNet,
    reason: BypassReason,
    tunnel_interface_index: NonZeroU32,
) -> Result<(), ProcessVpnPlanError> {
    let destination = canonical_net(destination);
    let native = environment.native_route_for(destination).cloned().ok_or(
        ProcessVpnPlanError::MissingNativeRoute {
            destination,
            reason,
        },
    )?;
    add_bypass(
        bypasses,
        destination,
        native,
        reason,
        tunnel_interface_index,
    )
}

fn add_bypass(
    bypasses: &mut Vec<PlannedBypass>,
    destination: IpNet,
    native: ProcessNativeRoute,
    reason: BypassReason,
    tunnel_interface_index: NonZeroU32,
) -> Result<(), ProcessVpnPlanError> {
    if native.interface_index() == tunnel_interface_index {
        return Err(ProcessVpnPlanError::NativeRouteUsesTunnel {
            destination,
            interface_index: tunnel_interface_index,
        });
    }
    if let Some(existing) = bypasses
        .iter_mut()
        .find(|existing| existing.destination == destination)
    {
        if existing.native != native {
            return Err(ProcessVpnPlanError::ConflictingNativeRoutes(destination));
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
) -> Result<(), ProcessVpnPlanError> {
    if let Some(server) = dns.servers().iter().copied().find(|server| {
        bypasses
            .iter()
            .any(|bypass| bypass.destination.contains(server))
    }) {
        return Err(ProcessVpnPlanError::DnsServerBypassed(server));
    }
    Ok(())
}

fn capture_destinations(config: &ManagedVpnConfig) -> Vec<IpNet> {
    match config.route_mode() {
        RouteMode::Full => config
            .addresses()
            .iter()
            .map(|address| match address {
                IpNet::V4(_) => "0.0.0.0/0".parse().expect("valid IPv4 default"),
                IpNet::V6(_) => "::/0".parse().expect("valid IPv6 default"),
            })
            .collect(),
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessVpnPlanError {
    ZeroTunnelInterfaceIndex,
    MissingNativeRoute {
        destination: IpNet,
        reason: BypassReason,
    },
    NativeRouteUsesTunnel {
        destination: IpNet,
        interface_index: NonZeroU32,
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

impl fmt::Display for ProcessVpnPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroTunnelInterfaceIndex => {
                formatter.write_str("tunnel interface index must be nonzero")
            }
            Self::MissingNativeRoute {
                destination,
                reason,
            } => write!(
                formatter,
                "no pre-VPN native route for {destination} required by {reason:?}"
            ),
            Self::NativeRouteUsesTunnel {
                destination,
                interface_index,
            } => write!(
                formatter,
                "native bypass {destination} resolves back to tunnel interface {interface_index}"
            ),
            Self::ConflictingNativeRoutes(destination) => {
                write!(
                    formatter,
                    "bypass {destination} has conflicting native routes"
                )
            }
            Self::DnsServerBypassed(server) => {
                write!(formatter, "captured DNS server {server} is also bypassed")
            }
            Self::NoEffectiveCaptureRoute => {
                formatter.write_str("VPN plan has no effective capture route")
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

impl std::error::Error for ProcessVpnPlanError {}

#[cfg(test)]
#[path = "tests_process_plan.rs"]
mod tests;
