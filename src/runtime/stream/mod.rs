//! Reliable product-stream ownership.
//!
//! Stream bindings own product offsets, exact carrier flights, attachment
//! generations, and atomic commit. Sender modules rank snapshots and submit
//! intents; carrier paths never own product byte ranges.

mod demand;
mod handle;
mod registry;
pub(in crate::runtime) mod response;
mod server;

pub(in crate::runtime) use demand::{
    flow_lane_from_stream_demand_hint, stream_demand_hint_for_lane,
};
pub(in crate::runtime) use handle::{
    FixedReliablePathOutput, ReliablePathStream, ReliablePathStreamHandle,
    ReliablePathStreamOutput, reliable_work_lane_to_carrier_lane,
    wait_for_carrier_capacity_notifies,
};
pub(in crate::runtime) use registry::{
    ServerCarrierPathRegistration, ServerReliablePathAttachment,
    ServerReliableRegistryManagementSnapshot, ServerReliableStreamOpen,
    ServerReliableStreamOpenRequest, ServerReliableStreamRegistry,
};
pub(in crate::runtime) use server::{ServerStreamContext, run_server_reliable_stream};
