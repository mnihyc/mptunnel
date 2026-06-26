use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

pub const DEFAULT_TUN_IPV4: Ipv4Addr = Ipv4Addr::new(10, 88, 0, 1);
pub const DEFAULT_TUN_IPV4_PREFIX: u8 = 24;
pub const DEFAULT_TUN_MTU: u16 = 1500;
pub const DEFAULT_TUN_DNS_TTL_MS: u32 = 5_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TunL4Config {
    pub name: Option<String>,
    pub ipv4: Option<Ipv4Addr>,
    pub ipv4_prefix: u8,
    pub ipv4_gateway: Option<Ipv4Addr>,
    pub ipv6: Option<Ipv6Addr>,
    pub ipv6_prefix: u8,
    pub mtu: u16,
    pub enable_icmp: bool,
    pub dns_resolvers: Vec<SocketAddr>,
    pub dns_ttl_ms: u32,
}

impl Default for TunL4Config {
    fn default() -> Self {
        Self {
            name: None,
            ipv4: Some(DEFAULT_TUN_IPV4),
            ipv4_prefix: DEFAULT_TUN_IPV4_PREFIX,
            ipv4_gateway: None,
            ipv6: None,
            ipv6_prefix: 64,
            mtu: DEFAULT_TUN_MTU,
            enable_icmp: true,
            dns_resolvers: Vec::new(),
            dns_ttl_ms: DEFAULT_TUN_DNS_TTL_MS,
        }
    }
}
