//! Product relay orchestration.
//!
//! Relay modules bridge local product I/O and sender/stream owners. They may
//! coordinate tasks, but carrier policy and exact-flight state live elsewhere.

pub(super) mod control;
mod diagnostics;
pub(super) mod flow;
pub(super) mod io;
pub(super) mod lifecycle;
pub(super) mod open;
pub(super) mod remote;
mod server;

#[cfg(test)]
pub(super) use control::*;
#[cfg(test)]
pub(super) use flow::*;
#[cfg(test)]
pub(super) use io::*;
#[cfg(any(test, feature = "lab-diagnostics"))]
pub(in crate::runtime) use remote::ReliableRelayRemotePath;
pub(in crate::runtime) use remote::ReliableRelayRemoteSet;
pub(in crate::runtime) use server::ServerReliableRelayService;
#[cfg(test)]
pub(in crate::runtime) use server::relay_reliable_stream;
