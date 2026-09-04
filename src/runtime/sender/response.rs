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
#[path = "response/tests_test_support.rs"]
pub(super) mod test_support;

use crate::model::path::CarrierPathKey;
use crate::protocol::Frame;
use crate::runtime::sender::RelaySendCause;
use crate::runtime::stream::response::ResponseStreamBinding;

pub(super) type ResponseOutputIdentity = (CarrierPathKey, u64);

/// One stable configured slot may own at most one current local publication
/// of the same Product range.
///
/// Native suppression deadlines and physical replacement do not transfer that
/// authority while the predecessor remains in Product scheduling membership.
/// Product DataACK releases the range; serialized membership removal transfers
/// publication authority to an attached successor.
pub(super) fn response_reinjection_avoid_outputs(
    binding: &ResponseStreamBinding,
    frame: &Frame,
    cause: RelaySendCause,
) -> Vec<ResponseOutputIdentity> {
    if cause.is_reinjection() {
        binding.reinjection_avoid_outputs_for_frame(frame)
    } else {
        Vec::new()
    }
}

pub(in crate::runtime) use service::ServerResponseSenderService;
