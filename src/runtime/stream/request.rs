//! Request-direction stream ownership.
//!
//! The facade exposes attachment, path-state, flow-control, and exact-flight
//! contracts while each child module owns one independent state transition.

mod attachment;
mod flight;
mod state;

pub(in crate::runtime) use attachment::{
    OpenedRemoteStream, ReliableRelayAttachOutcome, ReliableRelayRemoteFrame,
    ReliableRelayRemotePath, ReliableRelayRemoteSet,
};
// Keep inferred result/state types nameable without exposing child modules.
#[allow(unused_imports)]
pub(in crate::runtime) use flight::{RequestFlightLedger, RequestPathRelease};
#[allow(unused_imports)]
pub(in crate::runtime) use state::{
    RequestAckClockOperation, RequestPathState, RequestPathStates, RequestStreamState,
};
