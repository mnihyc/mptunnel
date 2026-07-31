//! Reliable product-stream ownership.
//!
//! Stream bindings own product offsets, exact carrier flights, attachment
//! generations, and atomic commit. Sender modules rank snapshots and submit
//! intents; carrier paths never own product byte ranges.

mod feedback;
mod handle;
mod registry;
pub(in crate::runtime) mod request;
pub(in crate::runtime) mod response;
mod send_buffer;

pub(in crate::runtime) use feedback::{
    ReliableRecvProgress, reliable_relay_recv_progress_resend_active,
    reliable_stream_recv_progress_interval,
};
#[cfg(test)]
pub(in crate::runtime) use handle::FixedReliablePathOutput;
pub(in crate::runtime) use handle::{
    ReliablePathStream, ReliablePathStreamHandle, ReliablePathStreamOutput,
    arm_carrier_capacity_notifies, reliable_work_lane_to_carrier_lane,
    wait_for_carrier_capacity_notifies,
};
#[cfg(test)]
pub(in crate::runtime) use registry::ServerReliableStreamOpen;
pub(in crate::runtime) use registry::{
    AcceptedServerReliableStream, AcceptedServerReliableStreamRetirement,
    ServerReliableStreamRegistry,
};
pub(in crate::runtime) use request::{
    OpenedRemoteStream, ReliableRelayAttachOutcome, ReliableRelayRemoteFrame,
    ReliableRelayRemotePath, ReliableRelayRemoteSet,
};
pub(in crate::runtime) use send_buffer::SessionSendBuffer;
