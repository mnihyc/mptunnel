//! First-class layer-3 packet service above authenticated MPP carriers.
//!
//! Address ownership, packet admission, and flow affinity live here. Carrier
//! adapters translate only typed attachment commands; Product routing, DNS,
//! firewall policy, NAT, and the TUN-L4 userspace stack are not dependencies.

mod client;
mod flow;
mod server;
mod service;

pub(in crate::runtime) use client::{ClientIpTunnelEvent, ClientIpTunnelHub, run_client_tun_l3};
pub(in crate::runtime) use server::run_server_tun_l3;
pub(in crate::runtime) use service::{
    AcceptedServerIpTunnel, IpTunnelCarrierCommand, ServerIpTunnelDevice,
    ServerIpTunnelOpenRequest, ServerIpTunnelPort, ServerIpTunnelService,
};

#[cfg(test)]
#[path = "tests_service.rs"]
mod tests;
