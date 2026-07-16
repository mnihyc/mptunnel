//! Response path scheduling above independent carrier controllers.
//!
//! TCP and QUIC decide congestion and recovery below this layer. This module
//! selects among paths that can currently accept data, using connection-level
//! flight, completion time, peer backup preference, and reorder limits.

use crate::model::admission::{
    BulkAdmissionCheck, BulkCandidatePosition, bulk_additional_candidate_position,
    bulk_candidate_admission_suppression_with_completion_backlog,
};
use crate::model::capacity::reliable_unproven_path_startup_flight_limit_bytes;
use crate::model::path::{CarrierPathKey, carrier_path_key_order};
use crate::model::response::{
    CarrierPathFlightDebt, response_oldest_lower_flight_owner, response_ordering_debt_bytes,
};
use crate::mux::MuxLimits;
use crate::protocol::Frame;
use crate::runtime::sender::{CarrierEmitMode, RelaySendCause};
use crate::runtime::stream::response::ResponseSenderPathTarget;
use crate::scheduler::{self, TrafficClass};

/// Selects the path for the next unique connection-data range.
///
/// Backup is a peer preference, not an attachment role: a backup path is used
/// only when no regular path is currently schedulable. Exact target identity is
/// revalidated by the response binding before the carrier command is published.
pub(super) fn select_response_data_path(
    targets: &[ResponseSenderPathTarget],
    lane: TrafficClass,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    lower_flights: &[CarrierPathFlightDebt],
    connection_ordering_debt_bytes: usize,
) -> Option<ResponseSenderPathTarget> {
    let connection_window = u64::try_from(mux_limits.max_reorder_bytes)
        .unwrap_or(u64::MAX)
        .min(mux_limits.max_stream_window_bytes);
    let connection_flight = u64::try_from(connection_ordering_debt_bytes).unwrap_or(u64::MAX);
    if connection_flight.saturating_add(payload_bytes as u64) > connection_window {
        return None;
    }
    let lower_owner = response_oldest_lower_flight_owner(lower_flights);
    let select = |allow_backup: bool| {
        let candidates = targets
            .iter()
            .filter(|target| target.can_enqueue_stream_ordered_frame())
            .filter(|target| {
                allow_backup || !scheduler::path_is_backup(target.observation.snapshot)
            })
            .filter_map(|target| {
                let snapshot = response_completion_snapshot(target);
                let score = scheduler::score_path(snapshot, lane, payload_bytes)?;
                let external_flight = response_external_ordering_debt_bytes(
                    target,
                    lower_flights,
                    connection_ordering_debt_bytes,
                );
                Some((target, snapshot, score.eta_ms, external_flight))
            })
            .collect::<Vec<_>>();
        let lead = lower_owner
            .and_then(|(owner_key, owner_incarnation)| {
                candidates.iter().find(|candidate| {
                    candidate.0.observation.key == owner_key
                        && candidate.0.observation.incarnation == owner_incarnation
                })
            })
            .or_else(|| {
                candidates.iter().min_by(|left, right| {
                    left.2.total_cmp(&right.2).then_with(|| {
                        carrier_path_key_order(left.0.observation.key, right.0.observation.key)
                    })
                })
            })?;
        let lead_key = lead.0.observation.key;
        let lead_snapshot = lead.1;
        let lead_eta_ms = lead.2;
        // ECF may credit only work the lead path will actually complete. The
        // rest of the Data-ACK tail is cross-path ordering debt, not additional
        // lead-path backlog.
        let lead_completion_backlog_bytes = lead.0.observation.original_data_in_flight_bytes;
        let reference_key = lower_owner.map_or(lead_key, |(key, _)| key);

        candidates
            .into_iter()
            .filter(|(target, snapshot, eta_ms, external_flight)| {
                let key = target.observation.key;
                let owns_lower_frontier =
                    lower_owner == Some((key, target.observation.incarnation));
                let position = if owns_lower_frontier || (lower_owner.is_none() && key == lead_key)
                {
                    BulkCandidatePosition::FirstPath
                } else {
                    bulk_additional_candidate_position(reference_key.underlay, key.underlay)
                };
                let (best_snapshot, best_eta_ms) = if owns_lower_frontier {
                    (*snapshot, *eta_ms)
                } else {
                    (lead_snapshot, lead_eta_ms)
                };
                // The contiguous frontier cannot create a cross-path Data
                // Sequence hole, so the negotiated connection window and the
                // native carrier controller are its limits. Only speculative
                // placement on an additional path needs a bounded startup
                // flight until exact Data ACKs prove path-local progress.
                let has_data_level_credit = position == BulkCandidatePosition::FirstPath
                    || snapshot.has_durable_product_progress
                    || target
                        .observation
                        .original_data_in_flight_bytes
                        .saturating_add(payload_bytes as u64)
                        <= reliable_unproven_path_startup_flight_limit_bytes(mux_limits);
                if !has_data_level_credit {
                    return false;
                }
                bulk_candidate_admission_suppression_with_completion_backlog(
                    BulkAdmissionCheck {
                        best_snapshot,
                        best_eta_ms,
                        candidate_snapshot: *snapshot,
                        candidate_eta_ms: *eta_ms,
                        payload_bytes,
                        mux_limits,
                        position,
                        stream_ordering_debt_bytes: *external_flight,
                    },
                    lead_completion_backlog_bytes,
                )
                .is_none()
            })
            .min_by(|left, right| {
                left.2
                    .total_cmp(&right.2)
                    .then_with(|| left.3.cmp(&right.3))
                    .then_with(|| {
                        carrier_path_key_order(left.0.observation.key, right.0.observation.key)
                    })
            })
            .map(|(target, _, _, _)| target.clone())
    };

    select(false).or_else(|| select(true))
}

fn response_completion_snapshot(
    target: &ResponseSenderPathTarget,
) -> crate::scheduler::PathSnapshot {
    let mut snapshot = target.observation.snapshot;
    // MPP bytes waiting above the scheduler are common to every path and
    // therefore are not path-assigned completion work. Exact Data-ACK flight
    // includes commands already drained into a TCP socket, which TCP_INFO
    // cannot report in bytes portably. Repair copies remain carrier work; only
    // unique data contributes to the data-level completion view.
    snapshot.data_level_queue_bytes = 0;
    snapshot.data_level_bytes_in_flight = target.observation.original_data_in_flight_bytes;
    snapshot
}

/// Selects a carrier for connection control or reinjection. New data uses
/// `select_response_data_path` because it additionally owns Data-ACK ordering.
pub(super) fn select_response_frame_path(
    targets: &[ResponseSenderPathTarget],
    lane: TrafficClass,
    frame: &Frame,
    emit_mode: CarrierEmitMode,
    avoid_keys: &[CarrierPathKey],
    reinjection_cause: Option<RelaySendCause>,
) -> Option<ResponseSenderPathTarget> {
    let payload_bytes = crate::protocol::frame::reliable_stream_frame_accounted_bytes(frame);
    let ack_gap_reinjection = reinjection_cause.is_some_and(RelaySendCause::is_ack_gap_reinjection);
    let path_failure_reinjection = matches!(
        reinjection_cause,
        Some(RelaySendCause::PathFailureReinjection)
    );
    let exact_reinjection_target =
        reinjection_cause.and_then(RelaySendCause::persistent_server_target);
    let can_enqueue = |target: &&ResponseSenderPathTarget| match emit_mode {
        CarrierEmitMode::Classified => target.can_enqueue_frame(frame, lane),
        CarrierEmitMode::StreamOrdered => target.can_enqueue_stream_ordered_frame(),
    };
    let select = |allow_backup: bool, avoid_existing: bool, require_delivery_evidence: bool| {
        let candidates = targets
            .iter()
            .filter(&can_enqueue)
            .filter(|target| {
                allow_backup || !scheduler::path_is_backup(target.observation.snapshot)
            })
            .filter(|target| {
                exact_reinjection_target.is_none_or(|exact| {
                    target.observation.key == exact.key
                        && target.observation.incarnation == exact.incarnation
                })
            })
            .filter(|target| {
                !require_delivery_evidence || target.observation.has_bulk_rate_evidence
            })
            .filter(|target| !avoid_existing || !avoid_keys.contains(&target.observation.key))
            .filter_map(|target| {
                let score = scheduler::score_path(
                    response_completion_snapshot(target),
                    lane,
                    payload_bytes,
                )?;
                Some((target, score.eta_ms))
            });

        if reinjection_cause.is_none()
            && matches!(frame, Frame::StreamAck { .. } | Frame::StreamMaxData { .. })
            && let Some((target, _)) = candidates
                .clone()
                .find(|(target, _)| target.observation.is_request_feedback)
        {
            return Some(target.clone());
        }

        candidates
            .min_by(|left, right| {
                left.1.total_cmp(&right.1).then_with(|| {
                    carrier_path_key_order(left.0.observation.key, right.0.observation.key)
                })
            })
            .map(|(target, _)| target.clone())
    };

    let select_for_cause = |allow_backup: bool, avoid_existing: bool| {
        if ack_gap_reinjection {
            // A Data ACK gap may move connection-level ownership only to an
            // alternate with measured delivery-rate evidence.
            return select(allow_backup, avoid_existing, true);
        }
        if path_failure_reinjection {
            // Prefer a measured survivor for a failed output, but liveness is
            // sufficient for correctness when no measured survivor remains.
            return select(allow_backup, avoid_existing, true)
                .or_else(|| select(allow_backup, avoid_existing, false));
        }
        select(allow_backup, avoid_existing, false)
    };

    let prefer_distinct = reinjection_cause.is_some() && !avoid_keys.is_empty();
    let require_distinct = (matches!(reinjection_cause, Some(RelaySendCause::TailReinjection))
        || ack_gap_reinjection)
        && !avoid_keys.is_empty();
    if require_distinct {
        // Reinjection of a live output's range is useful only on a different
        // path; same-path recovery remains the native TCP or QUIC controller's
        // responsibility.
        return select_for_cause(false, true).or_else(|| select_for_cause(true, true));
    }
    select_for_cause(false, prefer_distinct)
        .or_else(|| {
            prefer_distinct
                .then(|| select_for_cause(false, false))
                .flatten()
        })
        .or_else(|| select_for_cause(true, prefer_distinct))
        .or_else(|| {
            prefer_distinct
                .then(|| select_for_cause(true, false))
                .flatten()
        })
}

fn response_external_ordering_debt_bytes(
    target: &ResponseSenderPathTarget,
    lower_flights: &[CarrierPathFlightDebt],
    connection_ordering_debt_bytes: usize,
) -> u64 {
    let exact_other_path_debt = response_ordering_debt_bytes(
        lower_flights,
        target.observation.key,
        target.observation.incarnation,
    );
    let connection_other_path_debt = u64::try_from(connection_ordering_debt_bytes)
        .unwrap_or(u64::MAX)
        .saturating_sub(target.observation.original_data_in_flight_bytes);
    exact_other_path_debt.max(connection_other_path_debt)
}

#[cfg(test)]
#[path = "scheduling_test.rs"]
mod tests;
