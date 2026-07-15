//! Reliable product-stream ownership.
//!
//! Stream bindings own product offsets, exact carrier flights, attachment
//! generations, and atomic commit. Sender modules rank snapshots and submit
//! intents; carrier paths never own product byte ranges.

mod handle;
mod registry;
pub(in crate::runtime) mod request;
pub(in crate::runtime) mod response;

#[cfg(test)]
pub(in crate::runtime) use handle::FixedReliablePathOutput;
pub(in crate::runtime) use handle::{
    ReliablePathStream, ReliablePathStreamHandle, ReliablePathStreamOutput,
    reliable_work_lane_to_carrier_lane, wait_for_carrier_capacity_notifies,
};
pub(in crate::runtime) use registry::{
    AcceptedServerReliableStream, AcceptedServerReliableStreamRetirement,
    ServerCarrierPathRegistration, ServerReliablePathAttachment,
    ServerReliableRegistryManagementSnapshot, ServerReliableStreamOpen,
    ServerReliableStreamOpenRequest, ServerReliableStreamRegistry,
};
