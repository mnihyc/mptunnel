//! Response path scheduling above independent carrier controllers.
//!
//! TCP and QUIC decide congestion and recovery below this layer. This module
//! selects among paths that can currently accept data, using connection-level
//! flight, completion time, peer backup preference, and reorder limits.

#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::admission::{
    BulkAdmissionCheck, BulkCandidatePosition,
    bulk_candidate_admission_suppression_with_completion_backlog,
};
use crate::model::capacity::data_level_service_window_bytes;
use crate::model::capacity::reliable_unproven_path_startup_flight_limit_bytes;
use crate::model::path::{CarrierPathInstanceId, CarrierPathKey, carrier_path_key_order};
use crate::model::response::{
    CarrierPathFlightDebt, response_oldest_lower_flight_owner, response_ordering_debt_bytes,
};
use crate::model::tcp_carrier::TcpCarrierStableGenerations;
use crate::mux::MuxLimits;
use crate::protocol::{Frame, PathUsage, StreamId};
use crate::runtime::sender::response::ResponseOutputIdentity;
use crate::runtime::sender::{CarrierEmitMode, RelaySendCause};
use crate::runtime::stream::response::{ResponseSenderPathObservation, ResponseSenderPathTarget};
use crate::scheduler::{self, TrafficClass};
use smallvec::SmallVec;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct ResponseOrdinaryOutputInstance {
    pub(in crate::runtime) key: CarrierPathKey,
    pub(in crate::runtime) path_instance_id: CarrierPathInstanceId,
    pub(in crate::runtime) output_incarnation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct ResponseOrdinaryCarrierService {
    pub(in crate::runtime) instance: ResponseOrdinaryOutputInstance,
    pub(in crate::runtime) service_pipe_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime) struct ResponseOrdinarySaturationObservation {
    pub(in crate::runtime) stream_id: StreamId,
    pub(in crate::runtime) stable: TcpCarrierStableGenerations,
    pub(in crate::runtime) ordinary_services: SmallVec<[ResponseOrdinaryCarrierService; 4]>,
}

/// Recognizes only the RFC ordinary-carrier saturation transition. Dynamic
/// transport evidence ranks the exact observation but never becomes stable
/// admission authority.
pub(super) fn response_ordinary_saturation_observation(
    observation: &ResponseSenderPathObservation,
    stream_id: StreamId,
    lane: TrafficClass,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    connection_ordering_debt_bytes: usize,
) -> Option<ResponseOrdinarySaturationObservation> {
    if lane != TrafficClass::Throughput || payload_bytes == 0 {
        return None;
    }
    let connection_window = u64::try_from(mux_limits.max_reorder_bytes)
        .unwrap_or(u64::MAX)
        .min(mux_limits.max_stream_window_bytes);
    if u64::try_from(connection_ordering_debt_bytes).unwrap_or(u64::MAX) >= connection_window {
        return None;
    }

    let eligible = observation
        .targets
        .iter()
        .filter(|target| !target.observation.stale_for_original_data)
        .filter(|target| {
            scheduler::score_path(
                response_completion_snapshot(target),
                TrafficClass::Throughput,
                payload_bytes,
            )
            .is_some()
        })
        .collect::<SmallVec<[&ResponseSenderPathTarget; 4]>>();
    let authority_class = if eligible
        .iter()
        .any(|target| !scheduler::path_is_backup(target.observation.snapshot))
    {
        PathUsage::Available
    } else if eligible
        .iter()
        .any(|target| scheduler::path_is_backup(target.observation.snapshot))
    {
        PathUsage::Backup
    } else {
        return None;
    };
    let eligible = eligible
        .into_iter()
        .filter(|target| {
            scheduler::path_is_backup(target.observation.snapshot)
                == (authority_class == PathUsage::Backup)
        })
        .collect::<SmallVec<[&ResponseSenderPathTarget; 4]>>();
    if eligible.is_empty()
        || eligible.iter().any(|target| {
            target.can_enqueue_stream_data(TrafficClass::Throughput)
                || target.observation.original_data_in_flight_bytes == 0
                || target.observation.snapshot.active_latency_sensitive_flows != 0
        })
    {
        return None;
    }

    let ordinary_services = eligible
        .into_iter()
        .filter_map(|target| {
            let service_pipe = data_level_service_window_bytes(
                response_completion_snapshot(target),
                TrafficClass::Throughput,
                mux_limits,
            )
            .ceil();
            (service_pipe.is_finite() && service_pipe > 0.0).then_some(
                ResponseOrdinaryCarrierService {
                    instance: ResponseOrdinaryOutputInstance {
                        key: target.observation.key,
                        path_instance_id: target.observation.path_instance_id,
                        output_incarnation: target.observation.incarnation,
                    },
                    service_pipe_bytes: service_pipe as u64,
                },
            )
        })
        .collect::<SmallVec<[ResponseOrdinaryCarrierService; 4]>>();
    if ordinary_services.is_empty() {
        return None;
    }
    Some(ResponseOrdinarySaturationObservation {
        stream_id,
        stable: observation.tcp_carrier_stable_generations(authority_class)?,
        ordinary_services,
    })
}

/// Selects the path for the next unique connection-data range.
///
/// Backup is a peer preference, not an attachment role: a backup path is used
/// only when no regular path is currently schedulable. Exact target identity is
/// revalidated by the response binding before the carrier command is published.
pub(super) struct ResponseDataPathSelection {
    pub(super) target: ResponseSenderPathTarget,
    pub(super) payload_bytes: usize,
}

#[cfg(test)]
pub(super) fn select_response_data_path(
    targets: &[ResponseSenderPathTarget],
    lane: TrafficClass,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    lower_flights: &[CarrierPathFlightDebt],
    connection_ordering_debt_bytes: usize,
) -> Option<ResponseSenderPathTarget> {
    select_response_data_path_with_payload(
        targets,
        lane,
        payload_bytes,
        mux_limits,
        lower_flights,
        connection_ordering_debt_bytes,
    )
    .map(|selection| selection.target)
}

pub(super) fn select_response_data_path_with_payload(
    targets: &[ResponseSenderPathTarget],
    lane: TrafficClass,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    lower_flights: &[CarrierPathFlightDebt],
    connection_ordering_debt_bytes: usize,
) -> Option<ResponseDataPathSelection> {
    let nonstale_live_paths = targets
        .iter()
        .filter(|target| !target.observation.stale_for_original_data)
        .count();
    let has_nonstale_live_path = nonstale_live_paths > 0;
    let single_live_path = if has_nonstale_live_path {
        nonstale_live_paths == 1
    } else {
        targets.len() == 1
    };
    let connection_window = u64::try_from(mux_limits.max_reorder_bytes)
        .unwrap_or(u64::MAX)
        .min(mux_limits.max_stream_window_bytes);
    let connection_flight = u64::try_from(connection_ordering_debt_bytes).unwrap_or(u64::MAX);
    let connection_credit = connection_window.saturating_sub(connection_flight);
    if connection_credit == 0 {
        return None;
    }
    let lower_owner = response_oldest_lower_flight_owner(lower_flights);
    let select = |allow_backup: bool, allow_stale: bool| {
        let candidates = targets
            .iter()
            .filter(|target| allow_stale || !target.observation.stale_for_original_data)
            .filter(|target| target.can_enqueue_stream_data(lane))
            // Registration follows the carrier's TCP/QUIC establishment and
            // authenticated PATH_JOIN/SESSION_READY exchange. That is the
            // MPTUN equivalent of an established MPTCP subflow; a second
            // product-stream challenge must not delay data placement.
            .filter(|target| {
                allow_backup || !scheduler::path_is_backup(target.observation.snapshot)
            })
            .filter_map(|target| {
                let snapshot = response_completion_snapshot(target);
                let score = scheduler::score_path(snapshot, lane, payload_bytes)?;
                let target_payload_bytes =
                    payload_bytes.min(usize::try_from(connection_credit).unwrap_or(usize::MAX));
                if target_payload_bytes == 0 {
                    return None;
                }
                let external_flight = response_external_ordering_debt_bytes(
                    target,
                    lower_flights,
                    connection_ordering_debt_bytes,
                );
                Some((
                    target,
                    snapshot,
                    score.eta_ms,
                    external_flight,
                    target_payload_bytes,
                ))
            })
            .collect::<Vec<_>>();
        // Queue readiness decides where the next frame may be published, but it
        // does not change which live path owns the lower Data Sequence range.
        // Keep that owner as the ECF reference while its carrier queue drains.
        let lower_owner_reference = lower_owner
            .and_then(|(owner_key, owner_incarnation)| {
                targets.iter().find(|target| {
                    target.observation.key == owner_key
                        && target.observation.incarnation == owner_incarnation
                        && (allow_stale || !target.observation.stale_for_original_data)
                })
            })
            .and_then(|target| {
                let snapshot = response_completion_snapshot(target);
                let score = scheduler::score_path(snapshot, lane, payload_bytes)?;
                Some((target, snapshot, score.eta_ms))
            });
        #[cfg(feature = "lab-diagnostics")]
        let lower_owner_live = lower_owner_reference.is_some();
        let lead = lower_owner_reference.or_else(|| {
            candidates
                .iter()
                .min_by(|left, right| {
                    left.2.total_cmp(&right.2).then_with(|| {
                        carrier_path_key_order(left.0.observation.key, right.0.observation.key)
                    })
                })
                .map(|candidate| (candidate.0, candidate.1, candidate.2))
        })?;
        let lead_key = lead.0.observation.key;
        let lead_snapshot = lead.1;
        let lead_eta_ms = lead.2;
        // ECF may credit only work the lead path will actually complete. The
        // rest of the Data-ACK tail is cross-path ordering debt, not additional
        // lead-path backlog.
        let lead_completion_backlog_bytes = lead.0.observation.original_data_in_flight_bytes;
        let admitted = candidates
            .into_iter()
            .filter_map(|(target, snapshot, eta_ms, external_flight, target_payload_bytes)| {
                let key = target.observation.key;
                let owns_lower_frontier =
                    lower_owner == Some((key, target.observation.incarnation));
                let position = if owns_lower_frontier || (lower_owner.is_none() && key == lead_key)
                {
                    BulkCandidatePosition::FirstPath
                } else {
                    BulkCandidatePosition::AdditionalPath
                };
                let (best_snapshot, best_eta_ms) = if owns_lower_frontier {
                    (snapshot, eta_ms)
                } else {
                    (lead_snapshot, lead_eta_ms)
                };
                // Every unproven path may own only one bounded startup flight
                // until exact Data ACKs prove progress. Owning the contiguous
                // frontier changes reorder debt, not Data Sequence credit.
                let has_data_level_credit = snapshot.has_durable_product_progress
                    || target
                        .observation
                        .original_data_in_flight_bytes
                        .saturating_add(target_payload_bytes as u64)
                        <= reliable_unproven_path_startup_flight_limit_bytes(mux_limits);
                // ECF and product service windows govern bulk placement. A
                // sole enqueueable output may bypass that second controller
                // only while it owns the contiguous frontier. If an absent
                // output still owns lower bytes, this candidate remains an
                // additional path and must not extend that receive hole
                // without bound. Once latency work is ready, deferring it
                // cannot move it ahead of bytes already handed to an ordered
                // carrier; publish it into the carrier's priority queue at the
                // earliest opportunity.
                let suppression = if lane.is_latency_sensitive()
                    || (single_live_path
                        && position == BulkCandidatePosition::FirstPath
                        && snapshot.active_latency_sensitive_flows == 0)
                {
                    None
                } else {
                    has_data_level_credit
                    .then(|| {
                        bulk_candidate_admission_suppression_with_completion_backlog(
                            BulkAdmissionCheck {
                                best_snapshot,
                                best_eta_ms,
                                candidate_snapshot: snapshot,
                                candidate_eta_ms: eta_ms,
                                payload_bytes: target_payload_bytes,
                                mux_limits,
                                position,
                                stream_ordering_debt_bytes: external_flight,
                            },
                            lead_completion_backlog_bytes,
                            target.observation.has_bulk_rate_evidence,
                        )
                    })
                    .flatten()
                    .or((!has_data_level_credit).then_some("startup_flight_limit"))
                };
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "response_data_candidate_evaluated",
                    format_args!(
                        "path_underlay={:?} path_id={} output_incarnation={} position={:?} lower_owner_underlay={:?} lower_owner_path_id={:?} lower_owner_incarnation={:?} lower_owner_live={} lead_underlay={:?} lead_path_id={} lead_incarnation={} eta_ms={:.3} lead_eta_ms={:.3} payload_bytes={} selected_payload_bytes={} original_flight={} external_flight={} native_flight={} native_queue={} carrier_limit={} data_level_limit={} active_flows={} active_latency_flows={} delivery_mbps={:.3} carrier_delivery_mbps={:.3} pacing_mbps={:.3} confidence_ppm={} app_limited={} durable_progress={} suppression={}",
                        key.underlay,
                        key.path_id.0,
                        target.observation.incarnation,
                        position,
                        lower_owner.map(|(owner, _)| owner.underlay),
                        lower_owner.map(|(owner, _)| owner.path_id.0),
                        lower_owner.map(|(_, incarnation)| incarnation),
                        lower_owner_live,
                        lead_key.underlay,
                        lead_key.path_id.0,
                        lead.0.observation.incarnation,
                        eta_ms,
                        lead_eta_ms,
                        payload_bytes,
                        target_payload_bytes,
                        target.observation.original_data_in_flight_bytes,
                        external_flight,
                        snapshot.bytes_in_flight,
                        snapshot.queue_bytes,
                        snapshot.carrier_inflight_limit_bytes,
                        snapshot.data_level_limit_bytes,
                        snapshot.active_flows,
                        snapshot.active_latency_sensitive_flows,
                        snapshot.delivery_rate_bps / 1_000_000.0,
                        snapshot.carrier_delivery_rate_bps.unwrap_or(0.0) / 1_000_000.0,
                        snapshot.pacing_rate_bps / 1_000_000.0,
                        (snapshot.confidence.clamp(0.0, 1.0) * 1_000_000.0).round() as u32,
                        snapshot.app_limited,
                        snapshot.has_durable_product_progress,
                        suppression.unwrap_or("none"),
                    ),
                );
                suppression.is_none().then_some((
                    target,
                    snapshot,
                    eta_ms,
                    external_flight,
                    target_payload_bytes,
                ))
            })
            .collect::<Vec<_>>();

        if lane.is_bulk()
            && let Some((target, _, _, _, payload_bytes)) = admitted
                .iter()
                .filter(|(target, snapshot, _, _, _)| {
                    !target.observation.has_bulk_rate_evidence
                        && snapshot.product_progress_rate_bps.is_none()
                        && target.observation.original_data_in_flight_bytes == 0
                })
                .min_by(|left, right| {
                    left.2.total_cmp(&right.2).then_with(|| {
                        carrier_path_key_order(left.0.observation.key, right.0.observation.key)
                    })
                })
        {
            // An established subflow needs one bounded real-data sample before
            // a fallback rate can rank it. Native TCP/QUIC congestion control
            // owns this flight; later placement returns to completion time.
            return Some(ResponseDataPathSelection {
                target: (*target).clone(),
                payload_bytes: *payload_bytes,
            });
        }

        admitted
            .into_iter()
            .min_by(|left, right| {
                left.2
                    .total_cmp(&right.2)
                    .then_with(|| left.3.cmp(&right.3))
                    .then_with(|| {
                        carrier_path_key_order(left.0.observation.key, right.0.observation.key)
                    })
            })
            .map(
                |(target, _, _, _, payload_bytes)| ResponseDataPathSelection {
                    target: target.clone(),
                    payload_bytes,
                },
            )
    };

    select(false, false)
        .or_else(|| select(true, false))
        .or_else(|| {
            (!has_nonstale_live_path)
                .then(|| select(false, true))
                .flatten()
        })
        .or_else(|| {
            (!has_nonstale_live_path)
                .then(|| select(true, true))
                .flatten()
        })
}

pub(super) fn response_completion_snapshot(
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
    avoid_outputs: &[ResponseOutputIdentity],
    reinjection_cause: Option<RelaySendCause>,
) -> Option<ResponseSenderPathTarget> {
    let payload_bytes = crate::protocol::frame::reliable_stream_frame_accounted_bytes(frame);
    let ack_gap_reinjection = reinjection_cause.is_some_and(RelaySendCause::is_ack_gap_reinjection);
    let path_failure_reinjection = matches!(
        reinjection_cause,
        Some(
            RelaySendCause::PathFailureReinjection
                | RelaySendCause::StaleResponsePathReinjection(_)
        )
    );
    let stale_response_path_reinjection = matches!(
        reinjection_cause,
        Some(RelaySendCause::StaleResponsePathReinjection(_))
    );
    let requires_measured_reinjection_target =
        reinjection_cause.is_some_and(RelaySendCause::is_persistent_ack_gap_reinjection);
    let exact_reinjection_target =
        reinjection_cause.and_then(RelaySendCause::persistent_server_target);
    let can_enqueue = |target: &&ResponseSenderPathTarget, require_idle: bool| {
        if reinjection_cause.is_some() {
            return target.can_enqueue_reinjection_frame(frame)
                && (!require_idle || ordered_carrier_reinjection_ready(target));
        }
        match emit_mode {
            CarrierEmitMode::Classified => target.can_enqueue_frame(frame, lane),
            CarrierEmitMode::StreamOrdered => target.can_enqueue_stream_ordered_frame(),
        }
    };
    let select = |allow_backup: bool,
                  avoid_existing: bool,
                  require_delivery_progress: bool,
                  require_idle: bool| {
        let candidates = targets
            .iter()
            .filter(|target| {
                reinjection_cause.is_none() || !target.observation.stale_for_original_data
            })
            .filter(|target| can_enqueue(target, require_idle))
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
                !require_delivery_progress
                    || target.observation.snapshot.has_durable_product_progress
                    || target.observation.has_bulk_rate_evidence
            })
            .filter(|target| {
                !avoid_existing
                    || !avoid_outputs
                        .contains(&(target.observation.key, target.observation.incarnation))
            })
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
        if requires_measured_reinjection_target {
            // The recovery decision already proved that this measured path can
            // finish before the original carrier. Its bounded repair queue,
            // rather than complete carrier idleness, is the admission limit.
            return select(allow_backup, avoid_existing, true, false);
        }
        if path_failure_reinjection {
            // Prefer a drained measured survivor. Confirmed failure may use a
            // busy carrier as a final liveness fallback.
            return select(allow_backup, avoid_existing, true, true)
                .or_else(|| select(allow_backup, avoid_existing, false, true))
                .or_else(|| select(allow_backup, avoid_existing, true, false))
                .or_else(|| select(allow_backup, avoid_existing, false, false));
        }
        select(
            allow_backup,
            avoid_existing,
            false,
            reinjection_cause.is_some(),
        )
    };

    let prefer_distinct = reinjection_cause.is_some() && !avoid_outputs.is_empty();
    let require_distinct = (matches!(reinjection_cause, Some(RelaySendCause::TailReinjection))
        || ack_gap_reinjection
        || stale_response_path_reinjection)
        && !avoid_outputs.is_empty();
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

/// A repeated Data Sequence range helps only where it can get ahead of the
/// blocked copy. Confirmed path failure has a separate busy-carrier fallback.
fn ordered_carrier_reinjection_ready(target: &ResponseSenderPathTarget) -> bool {
    let observation = &target.observation;
    if observation.writer_pending_bytes != 0 {
        return false;
    }
    match observation.key.underlay {
        crate::protocol::UnderlayProtocol::Tcp if observation.native_drain_observed => {
            observation.snapshot.bytes_in_flight == 0 && observation.native_queue_bytes == 0
        }
        crate::protocol::UnderlayProtocol::Tcp | crate::protocol::UnderlayProtocol::Udp => {
            observation.original_data_in_flight_bytes == 0 && observation.native_queue_bytes == 0
        }
    }
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
