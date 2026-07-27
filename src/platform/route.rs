//! Platform-neutral route identity shared by host-network planners.

use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use std::net::IpAddr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AddressFamily {
    Ipv4,
    Ipv6,
}

impl AddressFamily {
    pub const fn of(address: IpAddr) -> Self {
        match address {
            IpAddr::V4(_) => Self::Ipv4,
            IpAddr::V6(_) => Self::Ipv6,
        }
    }

    pub(crate) fn of_net(network: IpNet) -> Self {
        Self::of(network.addr())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum BypassReason {
    CarrierEndpoint = 0b0001,
    BootstrapDns = 0b0010,
    ExplicitExclude = 0b0100,
    LocalLan = 0b1000,
}

/// One or more reasons that a prefix must stay on the native network.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BypassReasons(u8);

impl BypassReasons {
    pub const fn one(reason: BypassReason) -> Self {
        Self(reason as u8)
    }

    pub const fn contains(self, reason: BypassReason) -> bool {
        self.0 & reason as u8 != 0
    }

    pub(crate) fn insert(&mut self, reason: BypassReason) {
        self.0 |= reason as u8;
    }

    pub(crate) fn order(self) -> u8 {
        [
            BypassReason::CarrierEndpoint,
            BypassReason::BootstrapDns,
            BypassReason::ExplicitExclude,
            BypassReason::LocalLan,
        ]
        .into_iter()
        .position(|reason| self.contains(reason))
        .unwrap_or(usize::MAX) as u8
    }
}

pub(crate) fn host_prefix(address: IpAddr) -> IpNet {
    match address {
        IpAddr::V4(address) => {
            IpNet::V4(Ipv4Net::new(address, 32).expect("valid IPv4 host prefix"))
        }
        IpAddr::V6(address) => {
            IpNet::V6(Ipv6Net::new(address, 128).expect("valid IPv6 host prefix"))
        }
    }
}
