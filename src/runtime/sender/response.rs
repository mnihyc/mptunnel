//! Response-direction sender ownership.
//!
//! The planner turns immutable stream/path evidence into generation-stamped
//! intents. The service owns queued response work; dispatch alone resolves
//! carrier handles, revalidates the intent, and enqueues commands.

use super::*;

#[cfg(feature = "lab-diagnostics")]
mod diagnostics;
mod dispatch;
mod planner;
mod service;

#[cfg(feature = "lab-diagnostics")]
use diagnostics::lab_response_service_handoff_evaluation;
use dispatch::{
    emit_planned_response_data_frame, emit_response_frame_from_sender_service,
    response_frame_has_carrier_credit, response_repair_carrier_lane,
};
pub(in crate::runtime) use dispatch::{
    emit_relay_path_frame, emit_relay_path_frame_with_mode, relay_cursor_distance,
    send_sender_service_control_frame,
};
use planner::*;
pub(in crate::runtime) use service::ServerResponseSenderService;
