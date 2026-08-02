//! Native route/DNS backend shared by Windows and privileged-process macOS.

use crate::platform::{
    AddressFamily, ProcessHostMutationBackend, ProcessHostOperation, ProcessNativeNetwork,
    ProcessNativeRoute, ProcessNativeRouteError, ProcessVpnEnvironment,
};
use ipnet::IpNet;
use route_manager::{Route, RouteManager};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::net::IpAddr;
#[cfg(target_os = "windows")]
use windows_sys::Win32::NetworkManagement::IpHelper::{
    ConvertInterfaceLuidToGuid, DNS_INTERFACE_SETTINGS, DNS_INTERFACE_SETTINGS_VERSION1,
    DNS_SETTING_IPV6, DNS_SETTING_NAMESERVER, GetIpInterfaceEntry, InitializeIpInterfaceEntry,
    MIB_IPINTERFACE_ROW, SetInterfaceDnsSettings,
};
#[cfg(target_os = "windows")]
use windows_sys::Win32::Networking::WinSock::{AF_INET, AF_INET6};
#[cfg(target_os = "windows")]
use windows_sys::core::GUID;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemProcessRollbackToken {
    RouteOwned,
    RoutePreexisting,
    #[cfg(target_os = "windows")]
    Dns {
        ipv4: bool,
        ipv6: bool,
    },
}

/// Direct route-socket/IP-Helper backend.
///
/// Route additions are postcondition-checked and distinguish owned from
/// identical pre-existing entries. Windows DNS is scoped to the new Wintun
/// interface. macOS DNS deliberately fails closed because consumer VPN DNS
/// belongs to `NEPacketTunnelNetworkSettings`, not an undocumented global
/// resolver mutation.
pub struct SystemProcessHostNetworkBackend {
    routes: RouteManager,
    #[cfg(target_os = "windows")]
    interface_guid: GUID,
}

impl SystemProcessHostNetworkBackend {
    #[cfg(target_os = "windows")]
    pub fn new(device: &tun_rs::AsyncDevice) -> Result<Self, SystemProcessMutationError> {
        let luid = device
            .if_luid()
            .map_err(|source| SystemProcessMutationError::Io {
                action: "read Wintun interface LUID",
                source,
            })?;
        let mut interface_guid: GUID = unsafe { std::mem::zeroed() };
        let status = unsafe { ConvertInterfaceLuidToGuid(&luid, &mut interface_guid) };
        if status != 0 {
            return Err(SystemProcessMutationError::Io {
                action: "resolve Wintun interface GUID",
                source: io::Error::from_raw_os_error(status as i32),
            });
        }
        Ok(Self {
            routes: RouteManager::new().map_err(|source| SystemProcessMutationError::Io {
                action: "open IP Helper route manager",
                source,
            })?,
            interface_guid,
        })
    }

    #[cfg(target_os = "macos")]
    pub fn new() -> Result<Self, SystemProcessMutationError> {
        Ok(Self {
            routes: RouteManager::new().map_err(|source| SystemProcessMutationError::Io {
                action: "open routing socket",
                source,
            })?,
        })
    }

    fn route_exists(&mut self, desired: &Route) -> io::Result<bool> {
        self.routes
            .list()
            .map(|routes| routes.iter().any(|route| routes_equal(route, desired)))
    }

    fn add_route(
        &mut self,
        operation: &ProcessHostOperation,
    ) -> Result<SystemProcessRollbackToken, SystemProcessMutationError> {
        let route = operation_route(operation).expect("route operation");
        if self
            .route_exists(&route)
            .map_err(|source| SystemProcessMutationError::Io {
                action: "list native routes before add",
                source,
            })?
        {
            return Ok(SystemProcessRollbackToken::RoutePreexisting);
        }
        self.routes
            .add(&route)
            .map_err(|source| SystemProcessMutationError::Io {
                action: "add native route",
                source,
            })?;
        match self.route_exists(&route) {
            Ok(true) => Ok(SystemProcessRollbackToken::RouteOwned),
            Ok(false) => {
                let rollback = self.routes.delete(&route).err();
                Err(SystemProcessMutationError::Postcondition {
                    action: "add native route",
                    rollback,
                })
            }
            Err(source) => {
                let rollback = self.routes.delete(&route).err();
                Err(SystemProcessMutationError::AtomicMutation {
                    action: "verify added native route",
                    source,
                    rollback,
                })
            }
        }
    }

    fn remove_owned_route(
        &mut self,
        operation: &ProcessHostOperation,
    ) -> Result<(), SystemProcessMutationError> {
        let route = operation_route(operation).expect("route operation");
        if !self
            .route_exists(&route)
            .map_err(|source| SystemProcessMutationError::Io {
                action: "list native routes before delete",
                source,
            })?
        {
            return Ok(());
        }
        self.routes
            .delete(&route)
            .map_err(|source| SystemProcessMutationError::Io {
                action: "delete owned native route",
                source,
            })?;
        if self
            .route_exists(&route)
            .map_err(|source| SystemProcessMutationError::Io {
                action: "verify deleted native route",
                source,
            })?
        {
            Err(SystemProcessMutationError::Postcondition {
                action: "delete owned native route",
                rollback: None,
            })
        } else {
            Ok(())
        }
    }

    #[cfg(target_os = "windows")]
    fn configure_dns(
        &self,
        servers: &[IpAddr],
    ) -> Result<SystemProcessRollbackToken, SystemProcessMutationError> {
        let ipv4 = servers
            .iter()
            .copied()
            .filter(IpAddr::is_ipv4)
            .collect::<Vec<_>>();
        let ipv6 = servers
            .iter()
            .copied()
            .filter(IpAddr::is_ipv6)
            .collect::<Vec<_>>();
        let mut applied_v4 = false;
        if !ipv4.is_empty() {
            apply_windows_dns(&self.interface_guid, &ipv4, true).map_err(|source| {
                SystemProcessMutationError::Io {
                    action: "configure Wintun IPv4 DNS",
                    source,
                }
            })?;
            applied_v4 = true;
        }
        if !ipv6.is_empty()
            && let Err(source) = apply_windows_dns(&self.interface_guid, &ipv6, false)
        {
            let rollback = applied_v4
                .then(|| apply_windows_dns(&self.interface_guid, &[], true).err())
                .flatten();
            return Err(SystemProcessMutationError::AtomicMutation {
                action: "configure Wintun IPv6 DNS",
                source,
                rollback,
            });
        }
        Ok(SystemProcessRollbackToken::Dns {
            ipv4: applied_v4,
            ipv6: !ipv6.is_empty(),
        })
    }

    #[cfg(target_os = "windows")]
    fn clear_dns(&self, ipv4: bool, ipv6: bool) -> Result<(), SystemProcessMutationError> {
        let mut first_error = None;
        if ipv6 {
            first_error = apply_windows_dns(&self.interface_guid, &[], false).err();
        }
        if ipv4 {
            let error = apply_windows_dns(&self.interface_guid, &[], true).err();
            if first_error.is_none() {
                first_error = error;
            }
        }
        match first_error {
            Some(source) => Err(SystemProcessMutationError::Io {
                action: "clear owned Wintun DNS",
                source,
            }),
            None => Ok(()),
        }
    }
}

#[cfg(target_os = "windows")]
fn apply_windows_dns(guid: &GUID, servers: &[IpAddr], ipv4: bool) -> io::Result<()> {
    debug_assert!(servers.iter().all(|server| server.is_ipv4() == ipv4));
    let nameservers = servers
        .iter()
        .map(IpAddr::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let mut nameservers = nameservers.encode_utf16().chain([0]).collect::<Vec<_>>();
    let flags = if ipv4 {
        DNS_SETTING_NAMESERVER
    } else {
        DNS_SETTING_NAMESERVER | DNS_SETTING_IPV6
    };
    let settings = DNS_INTERFACE_SETTINGS {
        Version: DNS_INTERFACE_SETTINGS_VERSION1,
        Flags: flags as u64,
        NameServer: nameservers.as_mut_ptr(),
        ..Default::default()
    };
    let status = unsafe { SetInterfaceDnsSettings(*guid, &settings) };
    if status == 0 {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(status as i32))
    }
}

impl ProcessHostMutationBackend for SystemProcessHostNetworkBackend {
    type RollbackToken = SystemProcessRollbackToken;
    type Error = SystemProcessMutationError;

    fn apply(
        &mut self,
        operation: &ProcessHostOperation,
    ) -> Result<Self::RollbackToken, Self::Error> {
        match operation {
            ProcessHostOperation::AddBypassRoute { .. }
            | ProcessHostOperation::AddCaptureRoute { .. } => self.add_route(operation),
            ProcessHostOperation::ConfigureDns { servers } => {
                #[cfg(target_os = "windows")]
                {
                    self.configure_dns(servers)
                }
                #[cfg(target_os = "macos")]
                {
                    let _ = servers;
                    Err(SystemProcessMutationError::MacosNetworkExtensionRequired)
                }
            }
        }
    }

    fn rollback(
        &mut self,
        operation: &ProcessHostOperation,
        token: &Self::RollbackToken,
    ) -> Result<(), Self::Error> {
        match token {
            SystemProcessRollbackToken::RouteOwned => self.remove_owned_route(operation),
            SystemProcessRollbackToken::RoutePreexisting => Ok(()),
            #[cfg(target_os = "windows")]
            SystemProcessRollbackToken::Dns { ipv4, ipv6 } => self.clear_dns(*ipv4, *ipv6),
        }
    }
}

fn operation_route(operation: &ProcessHostOperation) -> Option<Route> {
    match operation {
        ProcessHostOperation::AddBypassRoute {
            destination,
            native,
            ..
        } => {
            let route = Route::new(destination.network(), destination.prefix_len())
                .with_if_index(native.interface_index().get());
            let route = match native.gateway() {
                Some(gateway) => route.with_gateway(gateway),
                None => route,
            };
            #[cfg(target_os = "windows")]
            let route = route.with_metric(native.metric());
            Some(route)
        }
        ProcessHostOperation::AddCaptureRoute {
            destination,
            tunnel_interface_index,
            metric,
        } => {
            let route = Route::new(destination.network(), destination.prefix_len())
                .with_if_index(tunnel_interface_index.get());
            #[cfg(target_os = "windows")]
            let route = route.with_metric(*metric);
            #[cfg(target_os = "macos")]
            let _ = metric;
            Some(route)
        }
        ProcessHostOperation::ConfigureDns { .. } => None,
    }
}

fn routes_equal(left: &Route, right: &Route) -> bool {
    left.network() == right.network()
        && left.prefix() == right.prefix()
        && normalized_gateway(left.gateway()) == normalized_gateway(right.gateway())
        && left.if_index() == right.if_index()
        && route_metrics_equal(left, right)
}

fn normalized_gateway(gateway: Option<IpAddr>) -> Option<IpAddr> {
    gateway.filter(|gateway| !gateway.is_unspecified())
}

#[cfg(target_os = "windows")]
fn route_metrics_equal(left: &Route, right: &Route) -> bool {
    left.metric() == right.metric()
}

#[cfg(target_os = "macos")]
fn route_metrics_equal(_left: &Route, _right: &Route) -> bool {
    true
}

/// Captures a strict, immutable native route snapshot before tunnel creation.
///
/// Equal-prefix routes with equal preference but different egress paths are
/// rejected rather than guessed. A host integration can resolve such ECMP
/// policy explicitly and construct [`ProcessVpnEnvironment`] itself.
pub fn snapshot_process_vpn_environment()
-> Result<ProcessVpnEnvironment, SystemProcessMutationError> {
    let mut manager = RouteManager::new().map_err(|source| SystemProcessMutationError::Io {
        action: "open native route manager",
        source,
    })?;
    let routes = manager
        .list()
        .map_err(|source| SystemProcessMutationError::Io {
            action: "snapshot native routes",
            source,
        })?;

    let mut defaults = BTreeMap::<AddressFamily, NativeRouteCandidate>::new();
    let mut networks = BTreeMap::<IpNet, (NativeRouteCandidate, bool)>::new();
    #[cfg(target_os = "windows")]
    let mut interface_metrics = BTreeMap::<(AddressFamily, u32), u32>::new();
    for route in routes {
        let Some(interface_index) = route.if_index().filter(|index| *index != 0) else {
            continue;
        };
        let Ok(prefix) = IpNet::new(route.destination(), route.prefix()) else {
            continue;
        };
        let prefix = match prefix {
            IpNet::V4(network) => IpNet::V4(network.trunc()),
            IpNet::V6(network) => IpNet::V6(network.trunc()),
        };
        let family = AddressFamily::of(prefix.addr());
        let gateway = normalized_gateway(route.gateway());
        let raw_metric = route_metric(&route);
        #[cfg(target_os = "windows")]
        let preference_metric = {
            let interface_metric =
                windows_interface_metric(&mut interface_metrics, family, interface_index).map_err(
                    |source| SystemProcessMutationError::Io {
                        action: "read native Windows interface metric",
                        source,
                    },
                )?;
            windows_effective_route_metric(raw_metric, interface_metric)
        };
        #[cfg(target_os = "macos")]
        let preference_metric = u64::from(raw_metric);
        let route = ProcessNativeRoute::new(family, interface_index, gateway, raw_metric)
            .map_err(SystemProcessMutationError::InvalidNativeSnapshot)?;
        let native = NativeRouteCandidate {
            route,
            preference_metric,
        };

        if prefix.prefix_len() == 0 {
            insert_preferred_default(&mut defaults, native)?;
        } else {
            insert_preferred_network(&mut networks, prefix, native, gateway.is_none())?;
        }
    }

    let networks = networks
        .into_iter()
        .map(|(prefix, (candidate, directly_connected))| {
            ProcessNativeNetwork::new(prefix, candidate.route, directly_connected)
                .map_err(SystemProcessMutationError::InvalidNativeSnapshot)
        })
        .collect::<Result<Vec<_>, _>>()?;
    ProcessVpnEnvironment::new(
        defaults.into_values().map(|candidate| candidate.route),
        networks,
    )
    .map_err(SystemProcessMutationError::InvalidNativeSnapshot)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NativeRouteCandidate {
    route: ProcessNativeRoute,
    /// Complete host preference used only while selecting the immutable
    /// pre-VPN route. `route.metric()` remains the raw mutation value.
    preference_metric: u64,
}

fn insert_preferred_default(
    defaults: &mut BTreeMap<AddressFamily, NativeRouteCandidate>,
    candidate: NativeRouteCandidate,
) -> Result<(), SystemProcessMutationError> {
    match defaults.get(&candidate.route.family()) {
        None => {
            defaults.insert(candidate.route.family(), candidate);
            Ok(())
        }
        Some(existing) => match compare_native_preference(&candidate, existing) {
            Ordering::Less => {
                defaults.insert(candidate.route.family(), candidate);
                Ok(())
            }
            Ordering::Greater => Ok(()),
            Ordering::Equal if existing.route == candidate.route => Ok(()),
            Ordering::Equal => Err(SystemProcessMutationError::AmbiguousNativeRoute {
                destination: match candidate.route.family() {
                    AddressFamily::Ipv4 => "0.0.0.0/0".parse().expect("IPv4 default"),
                    AddressFamily::Ipv6 => "::/0".parse().expect("IPv6 default"),
                },
            }),
        },
    }
}

fn insert_preferred_network(
    networks: &mut BTreeMap<IpNet, (NativeRouteCandidate, bool)>,
    prefix: IpNet,
    candidate: NativeRouteCandidate,
    directly_connected: bool,
) -> Result<(), SystemProcessMutationError> {
    match networks.get(&prefix) {
        None => {
            networks.insert(prefix, (candidate, directly_connected));
            Ok(())
        }
        Some((existing, existing_connected)) => {
            match compare_native_preference(&candidate, existing) {
                Ordering::Less => {
                    networks.insert(prefix, (candidate, directly_connected));
                    Ok(())
                }
                Ordering::Greater => Ok(()),
                Ordering::Equal
                    if existing.route == candidate.route
                        && *existing_connected == directly_connected =>
                {
                    Ok(())
                }
                Ordering::Equal => Err(SystemProcessMutationError::AmbiguousNativeRoute {
                    destination: prefix,
                }),
            }
        }
    }
}

fn compare_native_preference(
    left: &NativeRouteCandidate,
    right: &NativeRouteCandidate,
) -> std::cmp::Ordering {
    left.preference_metric.cmp(&right.preference_metric)
}

#[cfg(target_os = "windows")]
fn route_metric(route: &Route) -> u32 {
    route.metric().unwrap_or(u32::MAX)
}

#[cfg(target_os = "windows")]
const fn windows_effective_route_metric(route_metric: u32, interface_metric: u32) -> u64 {
    route_metric as u64 + interface_metric as u64
}

#[cfg(target_os = "windows")]
fn windows_interface_metric(
    cache: &mut BTreeMap<(AddressFamily, u32), u32>,
    family: AddressFamily,
    interface_index: u32,
) -> io::Result<u32> {
    if let Some(metric) = cache.get(&(family, interface_index)) {
        return Ok(*metric);
    }

    let mut row = MIB_IPINTERFACE_ROW::default();
    unsafe { InitializeIpInterfaceEntry(&mut row) };
    row.Family = match family {
        AddressFamily::Ipv4 => AF_INET,
        AddressFamily::Ipv6 => AF_INET6,
    };
    row.InterfaceIndex = interface_index;
    let status = unsafe { GetIpInterfaceEntry(&mut row) };
    if status != 0 {
        return Err(io::Error::from_raw_os_error(status as i32));
    }
    cache.insert((family, interface_index), row.Metric);
    Ok(row.Metric)
}

#[cfg(target_os = "macos")]
fn route_metric(_route: &Route) -> u32 {
    0
}

#[derive(Debug)]
pub enum SystemProcessMutationError {
    Io {
        action: &'static str,
        source: io::Error,
    },
    AtomicMutation {
        action: &'static str,
        source: io::Error,
        rollback: Option<io::Error>,
    },
    Postcondition {
        action: &'static str,
        rollback: Option<io::Error>,
    },
    InvalidNativeSnapshot(ProcessNativeRouteError),
    AmbiguousNativeRoute {
        destination: IpNet,
    },
    #[cfg(target_os = "macos")]
    MacosNetworkExtensionRequired,
}

impl fmt::Display for SystemProcessMutationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { action, source } => write!(formatter, "{action}: {source}"),
            Self::AtomicMutation {
                action,
                source,
                rollback,
            } => {
                write!(formatter, "{action}: {source}")?;
                if let Some(rollback) = rollback {
                    write!(formatter, "; atomic rollback also failed: {rollback}")?;
                }
                Ok(())
            }
            Self::Postcondition { action, rollback } => {
                write!(
                    formatter,
                    "{action} did not reach its required postcondition"
                )?;
                if let Some(rollback) = rollback {
                    write!(formatter, "; cleanup also failed: {rollback}")?;
                }
                Ok(())
            }
            Self::InvalidNativeSnapshot(error) => {
                write!(formatter, "invalid native route snapshot: {error}")
            }
            Self::AmbiguousNativeRoute { destination } => write!(
                formatter,
                "native route snapshot has equal-preference paths for {destination}; host policy must select one"
            ),
            #[cfg(target_os = "macos")]
            Self::MacosNetworkExtensionRequired => formatter
                .write_str("macOS DNS publication requires a Network Extension packet-tunnel host"),
        }
    }
}

impl std::error::Error for SystemProcessMutationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } | Self::AtomicMutation { source, .. } => Some(source),
            Self::InvalidNativeSnapshot(error) => Some(error),
            Self::Postcondition { .. } | Self::AmbiguousNativeRoute { .. } => None,
            #[cfg(target_os = "macos")]
            Self::MacosNetworkExtensionRequired => None,
        }
    }
}

#[cfg(all(test, target_os = "windows"))]
#[path = "tests_desktop_routes.rs"]
mod tests;
