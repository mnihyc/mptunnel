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
    ReliablePathStream, ReliablePathStreamHandle, ReliablePathStreamOutput, RequalificationAttempt,
    TargetCarrierCapacityWait, arm_carrier_capacity_notifies, reliable_work_lane_to_carrier_lane,
    wait_for_carrier_capacity_notifies,
};
#[cfg(test)]
pub(in crate::runtime) use registry::ServerReliableStreamOpen;
pub(in crate::runtime) use registry::{
    AcceptedServerReliableStream, AcceptedServerReliableStreamRetirement,
    ServerReliableStreamRegistry,
};
#[cfg(test)]
pub(in crate::runtime) use request::arm_client_relay_attachment_commits_for_test;
pub(in crate::runtime) use request::{
    OpenedRemoteStream, ReliableRelayAttachOutcome, ReliableRelayOpenedStartup,
    ReliableRelayRemoteFrame, ReliableRelayRemotePath, ReliableRelayRemoteSet,
    ReliableRelayReturnCandidate, ReliableRelayReturnPlan,
};
pub(in crate::runtime) use send_buffer::SessionSendBuffer;
