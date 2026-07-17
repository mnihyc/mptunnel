//! Response-direction connection-data observations and range-flight arithmetic.
//!
//! Runtime code owns path handles, queues, and the exact range ledger. This
//! module carries immutable path state into scheduling and computes only the
//! connection-level ordering debt imposed by lower unacknowledged ranges.

use crate::model::path::{CarrierPathInstanceId, CarrierPathKey};
use crate::scheduler::PathSnapshot;

/// One immutable response-path observation consumed by connection scheduling.
///
/// Physical identity is retained so dispatch can revalidate the selected path
/// before publishing a frame. TCP and QUIC congestion control, recovery, and
/// pacing remain below this carrier-neutral observation.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ResponsePathObservation {
    pub(crate) key: CarrierPathKey,
    pub(crate) path_instance_id: CarrierPathInstanceId,
    pub(crate) incarnation: u64,
    pub(crate) snapshot: PathSnapshot,
    /// Bytes already accepted by the native TCP/QUIC sender queue. MPP command
    /// backlog remains in `snapshot.queue_bytes` for completion scoring, but it
    /// must not make a priority reinjection look native-busy.
    pub(crate) native_queue_bytes: u64,
    /// Exact native flight and unsent-queue counters were both observed.
    pub(crate) native_drain_observed: bool,
    /// Bytes removed from the command queues but not yet handed to the ordered
    /// carrier. Priority repair cannot overtake this private writer backlog.
    pub(crate) writer_pending_bytes: u64,
    /// Exact connection-data bytes assigned to this path and awaiting Data ACK.
    pub(crate) original_data_in_flight_bytes: u64,
    /// Exact request ingress preferred for path-neutral stream feedback.
    pub(crate) is_request_feedback: bool,
    pub(crate) has_bulk_rate_evidence: bool,
}

/// Lower connection-data bytes not yet retired from contiguous Data ACK order.
///
/// This includes live OriginalData and ranges acknowledged above a lower hole.
/// The attachment incarnation prevents a replacement carrier with the same
/// wire PathId from inheriting the old range's ordering credit.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CarrierPathFlightDebt {
    pub(crate) key: CarrierPathKey,
    pub(crate) output_incarnation: u64,
    pub(crate) bytes: u64,
}

/// Lower-range bytes that would arrive outside `candidate`'s path.
///
/// This bounds receive-side reordering; it is not a congestion window and does
/// not replace either carrier's native bytes-in-flight accounting.
pub(crate) fn response_ordering_debt_bytes(
    lower_flights: &[CarrierPathFlightDebt],
    candidate_key: CarrierPathKey,
    candidate_incarnation: u64,
) -> u64 {
    lower_flights
        .iter()
        .filter_map(|flight| {
            (flight.key != candidate_key || flight.output_incarnation != candidate_incarnation)
                .then_some(flight.bytes)
        })
        .sum()
}

/// Path carrying the lowest outstanding connection-data range.
///
/// The ledger is ordered by data sequence number, so this is the path whose
/// native delivery currently gates contiguous product progress.
pub(crate) fn response_oldest_lower_flight_owner(
    lower_flights: &[CarrierPathFlightDebt],
) -> Option<(CarrierPathKey, u64)> {
    lower_flights
        .first()
        .map(|flight| (flight.key, flight.output_incarnation))
}

#[cfg(test)]
#[path = "response_test.rs"]
mod tests;
