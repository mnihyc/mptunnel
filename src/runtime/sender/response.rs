//! Response-direction sender ownership.
//!
//! The planner turns immutable stream/path evidence into generation-stamped
//! intents. The service owns queued response work; dispatch alone resolves
//! carrier handles, revalidates the intent, and enqueues commands.

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

#[cfg(test)]
use admission::*;
#[cfg(feature = "lab-diagnostics")]
use diagnostics::lab_response_service_handoff_evaluation;
pub(in crate::runtime) use dispatch::emit_response_control_frame;
use dispatch::{
    emit_planned_response_data_frame, emit_response_frame_from_sender_service,
    response_frame_has_carrier_credit, response_repair_carrier_lane,
};
use planner::*;
pub(in crate::runtime) use service::ServerResponseSenderService;
