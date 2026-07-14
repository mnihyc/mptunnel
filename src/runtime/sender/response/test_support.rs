//! Shared response-sender fixtures used while policy tests move to their owners.

use crate::model::path::CarrierPathKey;
use crate::protocol::{PathId, SessionId, StreamOpenRole, UnderlayProtocol};
use crate::runtime::path::commands::reliable_path_command_channels;
use crate::runtime::stream::response::{
    ResponseSenderPathTarget, next_server_carrier_path_instance_id,
};
use crate::scheduler::PathSnapshot;

pub(in crate::runtime::sender) fn response_target(
    path_id: u16,
    underlay: UnderlayProtocol,
    eta_ms: f64,
    bytes_in_flight: u64,
    inflight_limit_bytes: u64,
    is_active: bool,
) -> ResponseSenderPathTarget {
    let (commands, _receivers) = reliable_path_command_channels(8);
    let mut snapshot = PathSnapshot::new(PathId(path_id), underlay, eta_ms.max(1.0), 500_000_000.0);
    snapshot.bytes_in_flight = bytes_in_flight;
    snapshot.product_bytes_in_flight = bytes_in_flight;
    snapshot.inflight_limit_bytes = inflight_limit_bytes;
    snapshot.confidence = 1.0;
    ResponseSenderPathTarget {
        #[cfg(feature = "lab-diagnostics")]
        session_id: SessionId(0),
        #[cfg(feature = "lab-diagnostics")]
        binding_instance_id: 0,
        key: CarrierPathKey {
            underlay,
            path_id: PathId(path_id),
        },
        path_instance_id: next_server_carrier_path_instance_id(),
        incarnation: u64::from(path_id) + 1,
        commands,
        attachment_role: if is_active {
            StreamOpenRole::Active
        } else {
            StreamOpenRole::Validation
        },
        snapshot,
        owner_data_in_flight_bytes: bytes_in_flight,
        command_pending_bytes: 0,
        eta_ms,
        is_active,
        is_request_active: is_active,
        has_sender_evidence: true,
        has_service_feed_evidence: true,
        has_bulk_rate_evidence: true,
        endpoint_only_service_prior_eligible: false,
        quic_capacity_proof: None,
        quic_capacity_calibration_attempts: 0,
        ack_clock_calibration_eligible: false,
        ack_clock_calibration_proven: false,
        ack_clock_calibration_spent_bytes: 0,
        ack_clock_calibration_credit_limit_bytes: 0,
        ack_clock_calibration_max_limit_bytes: 0,
        ack_clock_calibration_active: false,
    }
}
