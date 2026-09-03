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

/// One exact output incarnation may carry at most one unresolved copy of the
/// same Product range.
///
/// Native suppression deadlines only make a different exact incarnation
/// eligible for recovery. They do not authorize another copy behind the same
/// obstruction; the accepted copy retains that target's K until DataACK or
/// target terminal/detach.
pub(super) fn response_reinjection_avoid_outputs(
    binding: &ResponseStreamBinding,
    frame: &Frame,
    cause: RelaySendCause,
) -> Vec<ResponseOutputIdentity> {
    if cause.is_reinjection() {
        binding.flight_outputs_overlapping_frame(frame)
    } else {
        Vec::new()
    }
}

pub(in crate::runtime) use service::ServerResponseSenderService;
