//! Static layer-3 address ownership compiled from one MPP inbound.
//!
//! The server configuration owns address pools and explicit peer allocations.
//! Runtime packet dispatch consumes this immutable plan and never derives peer
//! identity from an outer locator or from a claimed inner address.

use super::{CredentialAuthority, PrincipalId};
use ipnet::IpNet;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TunL3AllocationSpec {
    pub principal_id: PrincipalId,
    pub ipv4: Option<Ipv4Addr>,
    pub ipv6: Option<Ipv6Addr>,
    /// Additional inner prefixes routed to this peer by externally managed
    /// host policy. MPTUNNEL only enforces ownership and dispatch.
    pub allowed_ips: Vec<IpNet>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TunL3ServerSpec {
    pub interface_name: Option<String>,
    pub ipv4_pool: Option<ipnet::Ipv4Net>,
    pub ipv4: Option<Ipv4Addr>,
    pub ipv6_pool: Option<ipnet::Ipv6Net>,
    pub ipv6: Option<Ipv6Addr>,
    pub mtu: u16,
    pub allocations: Vec<TunL3AllocationSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TunL3PeerAllocation {
    principal_id: PrincipalId,
    ipv4: Option<Ipv4Addr>,
    ipv6: Option<Ipv6Addr>,
    allowed_ips: Arc<Vec<IpNet>>,
}

impl TunL3PeerAllocation {
    pub fn principal_id(&self) -> &PrincipalId {
        &self.principal_id
    }

    pub const fn ipv4(&self) -> Option<Ipv4Addr> {
        self.ipv4
    }

    pub const fn ipv6(&self) -> Option<Ipv6Addr> {
        self.ipv6
    }

    pub fn assigned_addresses(&self) -> impl Iterator<Item = IpAddr> + '_ {
        self.ipv4
            .map(IpAddr::V4)
            .into_iter()
            .chain(self.ipv6.map(IpAddr::V6))
    }

    pub fn allowed_ips(&self) -> &[IpNet] {
        self.allowed_ips.as_slice()
    }

    pub fn owns(&self, address: IpAddr) -> bool {
        self.ipv4
            .is_some_and(|assigned| address == IpAddr::V4(assigned))
            || self
                .ipv6
                .is_some_and(|assigned| address == IpAddr::V6(assigned))
            || self
                .allowed_ips
                .iter()
                .any(|network| network.contains(&address))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TunL3AddressPlan {
    interface_name: Option<String>,
    ipv4_pool: Option<ipnet::Ipv4Net>,
    ipv4: Option<Ipv4Addr>,
    ipv6_pool: Option<ipnet::Ipv6Net>,
    ipv6: Option<Ipv6Addr>,
    mtu: u16,
    peers: Arc<HashMap<PrincipalId, TunL3PeerAllocation>>,
    host_owners: Arc<HashMap<IpAddr, PrincipalId>>,
    routed_prefixes: Arc<Vec<OwnedPrefix>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OwnedPrefix {
    network: IpNet,
    principal_id: PrincipalId,
}

impl TunL3AddressPlan {
    pub fn compile(
        spec: TunL3ServerSpec,
        authority: &CredentialAuthority,
    ) -> Result<Self, TunL3PlanError> {
        validate_server_family(spec.ipv4_pool.map(IpNet::V4), spec.ipv4.map(IpAddr::V4), 4)?;
        validate_server_family(spec.ipv6_pool.map(IpNet::V6), spec.ipv6.map(IpAddr::V6), 6)?;
        if spec.ipv4_pool.is_none() && spec.ipv6_pool.is_none() {
            return Err(TunL3PlanError::PoolRequired);
        }
        if spec.allocations.is_empty() {
            return Err(TunL3PlanError::AllocationRequired);
        }
        if spec.mtu < 576 {
            return Err(TunL3PlanError::MtuTooSmall { mtu: spec.mtu });
        }
        if spec.ipv6_pool.is_some() && spec.mtu < 1280 {
            return Err(TunL3PlanError::Ipv6MtuTooSmall { mtu: spec.mtu });
        }

        let accepted_principals = authority
            .credentials()
            .into_iter()
            .map(|credential| credential.principal().clone())
            .collect::<HashSet<_>>();
        let server_addresses = spec
            .ipv4
            .map(IpAddr::V4)
            .into_iter()
            .chain(spec.ipv6.map(IpAddr::V6))
            .collect::<HashSet<_>>();
        let mut peers = HashMap::with_capacity(spec.allocations.len());
        let mut host_owners = HashMap::new();
        let mut routed_prefixes = Vec::new();

        for allocation in spec.allocations {
            if !accepted_principals.contains(&allocation.principal_id) {
                return Err(TunL3PlanError::UnknownPrincipal(allocation.principal_id));
            }
            if allocation.ipv4.is_none() && allocation.ipv6.is_none() {
                return Err(TunL3PlanError::EmptyAllocation(allocation.principal_id));
            }
            if peers.contains_key(&allocation.principal_id) {
                return Err(TunL3PlanError::DuplicatePrincipal(allocation.principal_id));
            }
            validate_allocated_address(
                allocation.ipv4.map(IpAddr::V4),
                spec.ipv4_pool.map(IpNet::V4),
                &server_addresses,
                &allocation.principal_id,
            )?;
            validate_allocated_address(
                allocation.ipv6.map(IpAddr::V6),
                spec.ipv6_pool.map(IpNet::V6),
                &server_addresses,
                &allocation.principal_id,
            )?;

            if let Some(address) = allocation.ipv4.map(IpAddr::V4)
                && let Some(previous) = host_owners.insert(address, allocation.principal_id.clone())
            {
                return Err(TunL3PlanError::AddressOwnedTwice {
                    address,
                    left_principal: previous,
                    right_principal: allocation.principal_id,
                });
            }
            if let Some(address) = allocation.ipv6.map(IpAddr::V6)
                && let Some(previous) = host_owners.insert(address, allocation.principal_id.clone())
            {
                return Err(TunL3PlanError::AddressOwnedTwice {
                    address,
                    left_principal: previous,
                    right_principal: allocation.principal_id,
                });
            }
            for network in &allocation.allowed_ips {
                if server_addresses
                    .iter()
                    .any(|address| network.contains(address))
                {
                    return Err(TunL3PlanError::OwnershipContainsServerAddress {
                        principal_id: allocation.principal_id,
                        network: *network,
                    });
                }
                routed_prefixes.push(OwnedPrefix {
                    network: *network,
                    principal_id: allocation.principal_id.clone(),
                });
            }

            let peer = TunL3PeerAllocation {
                principal_id: allocation.principal_id.clone(),
                ipv4: allocation.ipv4,
                ipv6: allocation.ipv6,
                allowed_ips: Arc::new(allocation.allowed_ips),
            };
            peers.insert(allocation.principal_id, peer);
        }

        validate_disjoint_ownership(&host_owners, &routed_prefixes)?;
        routed_prefixes.sort_by(|left, right| {
            route_start(left.network)
                .cmp(&route_start(right.network))
                .then_with(|| right.network.prefix_len().cmp(&left.network.prefix_len()))
        });
        Ok(Self {
            interface_name: spec.interface_name,
            ipv4_pool: spec.ipv4_pool,
            ipv4: spec.ipv4,
            ipv6_pool: spec.ipv6_pool,
            ipv6: spec.ipv6,
            mtu: spec.mtu,
            peers: Arc::new(peers),
            host_owners: Arc::new(host_owners),
            routed_prefixes: Arc::new(routed_prefixes),
        })
    }

    pub fn interface_name(&self) -> Option<&str> {
        self.interface_name.as_deref()
    }

    pub const fn ipv4_pool(&self) -> Option<ipnet::Ipv4Net> {
        self.ipv4_pool
    }

    pub const fn ipv4(&self) -> Option<Ipv4Addr> {
        self.ipv4
    }

    pub const fn ipv6_pool(&self) -> Option<ipnet::Ipv6Net> {
        self.ipv6_pool
    }

    pub const fn ipv6(&self) -> Option<Ipv6Addr> {
        self.ipv6
    }

    pub const fn mtu(&self) -> u16 {
        self.mtu
    }

    pub fn peer(&self, principal_id: &PrincipalId) -> Option<&TunL3PeerAllocation> {
        self.peers.get(principal_id)
    }

    pub fn peers(&self) -> impl Iterator<Item = &TunL3PeerAllocation> {
        self.peers.values()
    }

    /// Longest-prefix ownership lookup. Configuration rejects cross-peer
    /// overlap, so the first matching route is authoritative.
    pub fn owner(&self, address: IpAddr) -> Option<&PrincipalId> {
        self.host_owners.get(&address).or_else(|| {
            self.routed_prefixes
                .iter()
                .filter(|route| route.network.contains(&address))
                .max_by_key(|route| route.network.prefix_len())
                .map(|route| &route.principal_id)
        })
    }
}

fn validate_server_family(
    pool: Option<IpNet>,
    server: Option<IpAddr>,
    family: u8,
) -> Result<(), TunL3PlanError> {
    match (pool, server) {
        (None, None) => Ok(()),
        (Some(pool), Some(server)) if !pool.contains(&server) => {
            Err(TunL3PlanError::ServerAddressOutsidePool {
                address: server,
                pool,
            })
        }
        (Some(pool), Some(server)) if !is_usable_host_address(server, pool) => {
            Err(TunL3PlanError::UnusableHostAddress { address: server })
        }
        (Some(_), Some(_)) => Ok(()),
        (Some(_), None) => Err(TunL3PlanError::ServerAddressRequired(family)),
        (None, Some(_)) => Err(TunL3PlanError::PoolRequiredForServerAddress(family)),
    }
}

fn validate_allocated_address(
    address: Option<IpAddr>,
    pool: Option<IpNet>,
    server_addresses: &HashSet<IpAddr>,
    principal_id: &PrincipalId,
) -> Result<(), TunL3PlanError> {
    let Some(address) = address else {
        return Ok(());
    };
    let Some(pool) = pool else {
        return Err(TunL3PlanError::AllocationWithoutPool {
            principal_id: principal_id.clone(),
            address,
        });
    };
    if !pool.contains(&address) {
        return Err(TunL3PlanError::AllocationOutsidePool {
            principal_id: principal_id.clone(),
            address,
            pool,
        });
    }
    if !is_usable_host_address(address, pool) {
        return Err(TunL3PlanError::UnusableHostAddress { address });
    }
    if server_addresses.contains(&address) {
        return Err(TunL3PlanError::AllocationUsesServerAddress {
            principal_id: principal_id.clone(),
            address,
        });
    }
    Ok(())
}

fn is_usable_host_address(address: IpAddr, pool: IpNet) -> bool {
    match (address, pool) {
        (IpAddr::V4(address), IpNet::V4(pool)) => {
            !address.is_unspecified()
                && !address.is_multicast()
                && address != Ipv4Addr::BROADCAST
                && (pool.prefix_len() >= 31
                    || (address != pool.network() && address != pool.broadcast()))
        }
        (IpAddr::V6(address), IpNet::V6(_)) => !address.is_unspecified() && !address.is_multicast(),
        _ => false,
    }
}

fn validate_disjoint_ownership(
    host_owners: &HashMap<IpAddr, PrincipalId>,
    routes: &[OwnedPrefix],
) -> Result<(), TunL3PlanError> {
    for (index, route) in routes.iter().enumerate() {
        for (address, principal) in host_owners {
            if principal != &route.principal_id && route.network.contains(address) {
                return Err(TunL3PlanError::OverlappingOwnership {
                    left: IpNet::new(*address, if address.is_ipv4() { 32 } else { 128 })
                        .expect("host prefix"),
                    left_principal: principal.clone(),
                    right: route.network,
                    right_principal: route.principal_id.clone(),
                });
            }
        }
        for other in &routes[..index] {
            if route.principal_id != other.principal_id
                && networks_overlap(route.network, other.network)
            {
                return Err(TunL3PlanError::OverlappingOwnership {
                    left: other.network,
                    left_principal: other.principal_id.clone(),
                    right: route.network,
                    right_principal: route.principal_id.clone(),
                });
            }
        }
    }
    Ok(())
}

fn networks_overlap(left: IpNet, right: IpNet) -> bool {
    match (left, right) {
        (IpNet::V4(left), IpNet::V4(right)) => {
            left.contains(&right.network()) || right.contains(&left.network())
        }
        (IpNet::V6(left), IpNet::V6(right)) => {
            left.contains(&right.network()) || right.contains(&left.network())
        }
        _ => false,
    }
}

fn route_start(network: IpNet) -> (u8, u128) {
    match network.network() {
        IpAddr::V4(address) => (4, u128::from(u32::from(address))),
        IpAddr::V6(address) => (6, u128::from(address)),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TunL3PlanError {
    PoolRequired,
    AllocationRequired,
    MtuTooSmall {
        mtu: u16,
    },
    Ipv6MtuTooSmall {
        mtu: u16,
    },
    ServerAddressRequired(u8),
    PoolRequiredForServerAddress(u8),
    ServerAddressOutsidePool {
        address: IpAddr,
        pool: IpNet,
    },
    UnusableHostAddress {
        address: IpAddr,
    },
    UnknownPrincipal(PrincipalId),
    DuplicatePrincipal(PrincipalId),
    EmptyAllocation(PrincipalId),
    AllocationWithoutPool {
        principal_id: PrincipalId,
        address: IpAddr,
    },
    AllocationOutsidePool {
        principal_id: PrincipalId,
        address: IpAddr,
        pool: IpNet,
    },
    AllocationUsesServerAddress {
        principal_id: PrincipalId,
        address: IpAddr,
    },
    AddressOwnedTwice {
        address: IpAddr,
        left_principal: PrincipalId,
        right_principal: PrincipalId,
    },
    OwnershipContainsServerAddress {
        principal_id: PrincipalId,
        network: IpNet,
    },
    OverlappingOwnership {
        left: IpNet,
        left_principal: PrincipalId,
        right: IpNet,
        right_principal: PrincipalId,
    },
}

impl fmt::Display for TunL3PlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PoolRequired => formatter.write_str("TUN-L3 requires an IPv4 or IPv6 pool"),
            Self::AllocationRequired => {
                formatter.write_str("TUN-L3 requires at least one peer allocation")
            }
            Self::MtuTooSmall { mtu } => {
                write!(formatter, "TUN-L3 MTU {mtu} is below the IPv4 minimum 576")
            }
            Self::Ipv6MtuTooSmall { mtu } => {
                write!(formatter, "TUN-L3 MTU {mtu} is below the IPv6 minimum 1280")
            }
            Self::ServerAddressRequired(family) => {
                write!(
                    formatter,
                    "TUN-L3 IPv{family} pool requires a server address"
                )
            }
            Self::PoolRequiredForServerAddress(family) => write!(
                formatter,
                "TUN-L3 IPv{family} server address requires a matching pool"
            ),
            Self::ServerAddressOutsidePool { address, pool } => {
                write!(
                    formatter,
                    "TUN-L3 server address {address} is outside pool {pool}"
                )
            }
            Self::UnusableHostAddress { address } => write!(
                formatter,
                "TUN-L3 address {address} is not a usable unicast host address"
            ),
            Self::UnknownPrincipal(principal) => write!(
                formatter,
                "TUN-L3 principal {principal} is not accepted by this MPP inbound"
            ),
            Self::DuplicatePrincipal(principal) => {
                write!(
                    formatter,
                    "duplicate TUN-L3 allocation for principal {principal}"
                )
            }
            Self::EmptyAllocation(principal) => {
                write!(
                    formatter,
                    "TUN-L3 allocation for {principal} requires an IPv4 or IPv6 address"
                )
            }
            Self::AllocationWithoutPool {
                principal_id,
                address,
            } => write!(
                formatter,
                "TUN-L3 allocation {address} for {principal_id} has no matching pool"
            ),
            Self::AllocationOutsidePool {
                principal_id,
                address,
                pool,
            } => write!(
                formatter,
                "TUN-L3 allocation {address} for {principal_id} is outside pool {pool}"
            ),
            Self::AllocationUsesServerAddress {
                principal_id,
                address,
            } => write!(
                formatter,
                "TUN-L3 allocation for {principal_id} uses server address {address}"
            ),
            Self::AddressOwnedTwice {
                address,
                left_principal,
                right_principal,
            } => write!(
                formatter,
                "TUN-L3 address {address} is owned by both {left_principal} and {right_principal}"
            ),
            Self::OwnershipContainsServerAddress {
                principal_id,
                network,
            } => write!(
                formatter,
                "TUN-L3 prefix {network} for {principal_id} contains a server address"
            ),
            Self::OverlappingOwnership {
                left,
                left_principal,
                right,
                right_principal,
            } => write!(
                formatter,
                "TUN-L3 ownership overlaps: {left} ({left_principal}) and {right} ({right_principal})"
            ),
        }
    }
}

impl std::error::Error for TunL3PlanError {}

#[cfg(test)]
#[path = "tests_tun_l3.rs"]
mod tests;
