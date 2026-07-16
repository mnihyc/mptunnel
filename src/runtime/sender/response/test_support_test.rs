//! Shared response scheduler and dispatch test fixtures.

use crate::model::path::CarrierPathKey;
use crate::model::response::ResponsePathObservation;
use crate::protocol::{PathId, UnderlayProtocol};
use crate::runtime::path::commands::reliable_path_command_channels;
use crate::runtime::stream::response::{
    ResponseSenderPathTarget, next_server_carrier_path_instance_id,
};
use crate::scheduler::PathSnapshot;

pub(in crate::runtime::sender) fn response_target(
    path_id: u16,
    underlay: UnderlayProtocol,
    srtt_ms: f64,
    bytes_in_flight: u64,
    inflight_limit_bytes: u64,
    request_feedback: bool,
) -> ResponseSenderPathTarget {
    let (commands, _receivers) = reliable_path_command_channels(8);
    let path_id = PathId(path_id);
    let mut snapshot = PathSnapshot::new(path_id, underlay, srtt_ms.max(1.0), 500_000_000.0);
    snapshot.bytes_in_flight = bytes_in_flight;
    snapshot.data_level_bytes_in_flight = bytes_in_flight;
    snapshot.carrier_inflight_limit_bytes = inflight_limit_bytes;
    snapshot.confidence = 1.0;
    snapshot.has_durable_product_progress = true;
    ResponseSenderPathTarget {
        observation: ResponsePathObservation {
            key: CarrierPathKey { underlay, path_id },
            path_instance_id: next_server_carrier_path_instance_id(),
            incarnation: u64::from(path_id.0) + 1,
            snapshot,
            original_data_in_flight_bytes: bytes_in_flight,
            is_request_feedback: request_feedback,
            has_bulk_rate_evidence: true,
        },
        command_queue: commands.queue_snapshot(),
    }
}
