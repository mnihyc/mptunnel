//! Layer-3 packet ingress configuration.
//!
//! This surface selects exactly one MPP path group and names a packet device.
//! Address allocation comes from the authenticated server. Route, DNS,
//! firewall, and NAT ownership deliberately do not appear here.

use crate::product::OutboundId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TunL3IngressConfig {
    pub outbound: OutboundId,
    pub interface_name: Option<String>,
}
