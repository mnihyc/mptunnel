//! Response-direction sender ownership.
//!
//! The service owns queued connection data. Scheduling ranks immutable live
//! path observations, and dispatch revalidates one exact identity before the
//! binding records connection flight and publishes a carrier command.

mod dispatch;
mod multipath;
mod scheduling;
mod service;
#[cfg(test)]
#[path = "response/test_support_test.rs"]
pub(super) mod test_support;

use crate::model::path::CarrierPathKey;
use crate::protocol::Frame;
use crate::runtime::sender::RelaySendCause;
use crate::runtime::stream::response::ResponseStreamBinding;

pub(super) type ResponseOutputIdentity = (CarrierPathKey, u64);

/// Native recovery owns the exact original carrier instance. Data-ACK and tail
/// repair may reuse the best eligible alternate after the live-copy interval;
/// confirmed path-failure recovery avoids every output already carrying it.
pub(super) fn response_reinjection_avoid_outputs(
    binding: &ResponseStreamBinding,
    frame: &Frame,
    cause: RelaySendCause,
) -> Vec<ResponseOutputIdentity> {
    if cause.is_ack_gap_reinjection() || cause == RelaySendCause::TailReinjection {
        binding.original_flight_outputs_overlapping_frame(frame)
    } else if cause.is_reinjection() {
        binding.flight_outputs_overlapping_frame(frame)
    } else {
        Vec::new()
    }
}

pub(in crate::runtime) use dispatch::emit_response_control_frame;
pub(in crate::runtime) use service::ServerResponseSenderService;
