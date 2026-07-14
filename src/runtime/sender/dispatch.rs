//! Non-blocking handoff from product senders to one carrier command queue.
//!
//! Directional senders decide ownership before this gate. Queue saturation is
//! returned to that owner so no carrier wait can stall ACK or control polling.

use super::work::CarrierEmitMode;
use crate::protocol::Frame;
use crate::runtime::RuntimeError;
use crate::runtime::path::commands::ReliablePathCommandSender;
use crate::scheduler::FlowLane;

pub(super) fn try_emit_carrier_frame(
    commands: &ReliablePathCommandSender,
    frame: Frame,
    lane: FlowLane,
    emit_mode: CarrierEmitMode,
) -> Result<(), RuntimeError> {
    match emit_mode {
        CarrierEmitMode::Classified => commands.try_enqueue_admitted_frame(frame, lane),
        CarrierEmitMode::StreamOrdered => commands.try_enqueue_stream_ordered_frame(frame, lane),
    }
}
