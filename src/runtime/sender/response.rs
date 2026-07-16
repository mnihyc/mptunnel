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

pub(in crate::runtime) use dispatch::emit_response_control_frame;
pub(in crate::runtime) use service::ServerResponseSenderService;
