//! Response-direction sender ownership.
//!
//! The planner turns immutable stream/path evidence into generation-stamped
//! intents. The service owns queued response work; dispatch alone resolves
//! carrier handles, revalidates the intent, and enqueues commands.

#[cfg(test)]
use super::*;

mod admission;
#[cfg(feature = "lab-diagnostics")]
mod diagnostics;
mod dispatch;
mod planner;
mod quic_capacity;
mod service;
mod tcp_capacity;
#[cfg(test)]
pub(super) mod test_support;

pub(in crate::runtime) use dispatch::emit_response_control_frame;
pub(in crate::runtime) use service::ServerResponseSenderService;
