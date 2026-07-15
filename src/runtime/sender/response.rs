//! Response-direction sender ownership.
//!
//! The planner ranks captured path evidence into one selection intent. The
//! multipath transaction maintains shared state, stamps one coherent epoch,
//! and returns an executable plan. The service owns queued response work;
//! dispatch alone revalidates the plan and enqueues commands.

#[cfg(test)]
use super::*;

mod admission;
#[cfg(feature = "lab-diagnostics")]
mod diagnostics;
mod dispatch;
mod multipath;
mod planner;
mod quic_capacity;
mod service;
mod tcp_capacity;
#[cfg(test)]
pub(super) mod test_support;

pub(in crate::runtime) use dispatch::emit_response_control_frame;
pub(in crate::runtime) use service::ServerResponseSenderService;
