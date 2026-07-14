//! Product relay orchestration.
//!
//! Relay modules bridge local product I/O and sender/stream owners. They may
//! coordinate tasks, but carrier policy and exact-flight state live elsewhere.

#[cfg(test)]
use super::*;

pub(super) mod control;
mod diagnostics;
pub(super) mod flow;
pub(super) mod io;
pub(super) mod open;
mod server;

#[cfg(test)]
pub(super) use control::*;
pub(in crate::runtime) use diagnostics::log_unexpected_stream_relay_frame;
#[cfg(test)]
pub(super) use flow::*;
#[cfg(test)]
pub(super) use io::*;
#[cfg(any(test, feature = "lab-diagnostics"))]
pub(in crate::runtime) use open::ReliableRelayRemotePath;
pub(in crate::runtime) use open::{
    RelayPathPlacement, ReliableRelayRemoteSet, UdpStreamOpenOptions,
};
pub(in crate::runtime) use server::ServerReliableRelayService;
#[cfg(test)]
pub(in crate::runtime) use server::relay_reliable_stream;
