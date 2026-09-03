//! Response data scheduling transaction.
//!
//! Planning reads immutable carrier observations and returns one exact path
//! identity. The response binding revalidates that identity and the connection
//! flight generation before publishing the carrier command.

use super::scheduling::select_response_data_path_with_payload;
use crate::model::admission::{BulkCandidatePosition, ReliableDataAckFrontierState};
use crate::model::path::CarrierPathKey;
use crate::model::work::ReliableWorkClass;
use crate::runtime::RuntimeError;
use crate::runtime::stream::response::ResponseDispatchTarget;
use crate::runtime::stream::{
    ReliablePathStream, ReliablePathStreamOutput, reliable_work_lane_to_carrier_lane,
};
use crate::scheduler::TrafficClass;

pub(super) enum ResponseDataDispatchTarget {
    Fixed {
        key: CarrierPathKey,
    },
    Switchable {
        target: ResponseDispatchTarget,
        expected_model_generation: u64,
        position: BulkCandidatePosition,
    },
}

#[cfg(test)]
pub(super) fn plan_response_data_dispatch(
    stream: &ReliablePathStream,
    relay_lane: TrafficClass,
    next_offset: u64,
    payload_bytes: usize,
) -> Result<ResponseDataDispatchTarget, RuntimeError> {
    plan_response_data_dispatch_with_data_ack_outstanding_impl(
        stream,
        relay_lane,
        next_offset,
        payload_bytes,
        0,
    )
}

#[cfg(test)]
pub(super) fn plan_response_data_dispatch_with_data_ack_outstanding_impl(
    stream: &ReliablePathStream,
    relay_lane: TrafficClass,
    next_offset: u64,
    payload_bytes: usize,
    data_ack_outstanding_bytes: usize,
) -> Result<ResponseDataDispatchTarget, RuntimeError> {
    plan_response_data_payload_with_data_ack_outstanding_impl(
        stream,
        relay_lane,
        next_offset,
        payload_bytes,
        data_ack_outstanding_bytes,
        ReliableDataAckFrontierState::Live,
    )
    .map(|(_, target)| target)
}

pub(super) fn plan_response_data_payload_with_data_ack_outstanding_impl(
    stream: &ReliablePathStream,
    relay_lane: TrafficClass,
    next_offset: u64,
    payload_bytes: usize,
    data_ack_outstanding_bytes: usize,
    frontier_state: ReliableDataAckFrontierState,
) -> Result<(usize, ResponseDataDispatchTarget), RuntimeError> {
    match &stream.output {
        ReliablePathStreamOutput::Fixed(fixed) => {
            let lane = reliable_work_lane_to_carrier_lane(ReliableWorkClass::Data, relay_lane);
            if !fixed.commands().can_enqueue_lane_now(lane)
                || !fixed.can_assign_original_data(relay_lane)
            {
                return Err(RuntimeError::SenderServiceBlocked);
            }
            Ok((
                payload_bytes,
                ResponseDataDispatchTarget::Fixed { key: fixed.key() },
            ))
        }
        ReliablePathStreamOutput::Switchable(binding) => {
            // Read the generation before the observation. Any concurrent model
            // update then makes commit reject this plan instead of accepting a
            // snapshot assembled across two generations.
            let expected_model_generation = binding.response_model_generation();
            let lower_flights = binding.lower_flights_before_offset(next_offset);
            let targets = binding.sender_path_targets(relay_lane, payload_bytes);
            let selection = select_response_data_path_with_payload(
                &targets,
                relay_lane,
                payload_bytes,
                binding.mux_limits(),
                &lower_flights,
                data_ack_outstanding_bytes,
                frontier_state,
            )
            .ok_or(RuntimeError::SenderServiceBlocked)?;
            Ok((
                selection.payload_bytes,
                ResponseDataDispatchTarget::Switchable {
                    target: selection.target.into(),
                    expected_model_generation,
                    position: selection.position,
                },
            ))
        }
    }
}

/// Readiness observes the same queue and model as apply, but never reserves a
/// carrier command or advances a connection generation.
#[cfg(test)]
pub(super) fn preview_response_data_payload_with_data_ack_outstanding(
    path_stream: &ReliablePathStream,
    relay_lane: TrafficClass,
    next_offset: u64,
    payload_bytes: usize,
    data_ack_outstanding_bytes: usize,
) -> bool {
    plan_response_data_payload_with_data_ack_outstanding_impl(
        path_stream,
        relay_lane,
        next_offset,
        payload_bytes,
        data_ack_outstanding_bytes,
        ReliableDataAckFrontierState::Live,
    )
    .is_ok()
}

#[cfg(test)]
#[path = "tests_multipath.rs"]
mod tests;
