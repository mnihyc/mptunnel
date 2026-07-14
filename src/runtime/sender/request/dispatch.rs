//! Fixed-carrier request emission.
//!
//! Client request streams bind to one carrier attachment. A switchable output
//! is a server response contract and is rejected before command enqueue.

use super::super::dispatch::try_emit_carrier_frame;
use super::super::work::CarrierEmitMode;
use crate::protocol::Frame;
use crate::runtime::RuntimeError;
use crate::runtime::stream::{
    ReliablePathStream, ReliablePathStreamHandle, ReliablePathStreamOutput,
};
use crate::scheduler::FlowLane;

pub(in crate::runtime) fn emit_request_control_frame(
    stream: &ReliablePathStream,
    frame: Frame,
) -> Result<(), RuntimeError> {
    emit_fixed_request_output(
        &stream.output,
        frame,
        FlowLane::Control,
        CarrierEmitMode::Classified,
    )
}

pub(in crate::runtime) fn emit_request_frame(
    stream: &ReliablePathStreamHandle,
    frame: Frame,
    lane: FlowLane,
) -> Result<(), RuntimeError> {
    emit_request_frame_with_mode(stream, frame, lane, CarrierEmitMode::Classified)
}

pub(in crate::runtime) fn emit_request_frame_with_mode(
    stream: &ReliablePathStreamHandle,
    frame: Frame,
    lane: FlowLane,
    emit_mode: CarrierEmitMode,
) -> Result<(), RuntimeError> {
    emit_fixed_request_output(&stream.output, frame, lane, emit_mode)
}

fn emit_fixed_request_output(
    output: &ReliablePathStreamOutput,
    frame: Frame,
    lane: FlowLane,
    emit_mode: CarrierEmitMode,
) -> Result<(), RuntimeError> {
    match output {
        ReliablePathStreamOutput::Fixed(fixed) => {
            try_emit_carrier_frame(fixed.commands(), frame, lane, emit_mode)
        }
        ReliablePathStreamOutput::Switchable(_) => {
            Err(RuntimeError::Protocol("request relay path is not fixed"))
        }
    }
}

#[cfg(test)]
#[path = "dispatch_test.rs"]
mod tests;
