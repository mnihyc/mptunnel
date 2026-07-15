//! Response OwnerData admission and byte-credit policy.
//!
//! Admission decides whether one immutable carrier snapshot may own new
//! product offsets and returns the exact Subflow selection that may be stamped
//! by the lifecycle owner. The same owner bounds emitted bytes: an admitted
//! path without a product-flight envelope is not a complete protocol state.
//! Ranking and mutable stream commits remain outside this module.

use crate::model::admission::{
    BulkAdmissionCheck, BulkAdmissionRole, bulk_active_service_product_envelope_bytes,
    bulk_additional_admission_role, bulk_candidate_admission_suppression_with_completion_backlog,
    bulk_exploration_completion_projection, bulk_latency_pressure_service_feed_window_bytes,
    bulk_service_feed_reservoir_payload_bytes, bulk_service_horizon_payload_bytes,
};
use crate::model::capacity::{
    adaptive_reliable_relay_inflight_bytes, reliable_bulk_carrier_feed_quantum_bytes,
    reliable_relay_scheduler_quantum_cap, reliable_subflow_startup_sample_limit_bytes,
};
use crate::model::multipath::{FlowSubflowSet, PathAdmission, SubflowAdmissionInput};
use crate::model::path::CarrierPathKey;
use crate::model::response::{
    ResponseBulkLead, ResponseCandidateTailDebt, ResponseOrderedTail, ResponseSameFamilyReservoir,
};
use crate::mux::MuxLimits;
use crate::protocol::{StreamOpenRole, UnderlayProtocol};
use crate::runtime::stream::response::ResponseSenderPathTarget;
use crate::scheduler::{FlowLane, PathSnapshot};

#[derive(Clone, Copy)]
pub(super) struct ResponseSubflowAdmissionSelection {
    pub(super) service: CarrierPathKey,
    pub(super) startup_owner_credit_bytes: usize,
    pub(super) input: SubflowAdmissionInput,
}

pub(super) struct ResponseOwnerAdmission {
    admission: PathAdmission,
    subflow_admission_selection: Option<ResponseSubflowAdmissionSelection>,
    bulk_role: BulkAdmissionRole,
    model_suppression: Option<&'static str>,
}

impl ResponseOwnerAdmission {
    pub(super) fn into_parts(
        self,
    ) -> (
        PathAdmission,
        Option<ResponseSubflowAdmissionSelection>,
        BulkAdmissionRole,
        Option<&'static str>,
    ) {
        (
            self.admission,
            self.subflow_admission_selection,
            self.bulk_role,
            self.model_suppression,
        )
    }
}

pub(super) fn response_bulk_admission_role(
    service_key: CarrierPathKey,
    candidate: CarrierPathKey,
    lower_owner: Option<CarrierPathKey>,
    ordering_debt: u64,
) -> BulkAdmissionRole {
    if candidate == service_key && ordering_debt == 0 {
        BulkAdmissionRole::ActiveDataPath
    } else if let Some(owner) = lower_owner {
        // Continuing the existing lower-flight carrier does not introduce a
        // new carrier-family transition, even when Service uses another
        // underlay family. Runtime ownership still remains Subflow below.
        bulk_additional_admission_role(owner.underlay, candidate.underlay)
    } else {
        bulk_additional_admission_role(service_key.underlay, candidate.underlay)
    }
}

pub(super) fn response_service_anchor_key(
    candidates: &[&ResponseSenderPathTarget],
    lower_owner: Option<CarrierPathKey>,
    ordered_data_owner: Option<CarrierPathKey>,
    fallback: CarrierPathKey,
) -> CarrierPathKey {
    ordered_data_owner
        .or_else(|| {
            candidates
                .iter()
                .copied()
                .find(|candidate| candidate.observation.is_service)
                .map(|candidate| candidate.observation.key)
        })
        .or(lower_owner)
        .unwrap_or(fallback)
}

fn response_unique_quic_data_would_expand_ordering_debt(
    lower_owner: Option<CarrierPathKey>,
    target: &ResponseSenderPathTarget,
    ordering_debt: u64,
) -> bool {
    matches!(
        lower_owner,
        Some(owner)
            if owner != target.observation.key
                && owner.underlay == UnderlayProtocol::Udp
                && target.observation.key.underlay == UnderlayProtocol::Udp
                && ordering_debt > 0
                && !target.observation.has_bulk_rate_evidence
    )
}

pub(super) fn response_target_is_measured_same_underlay_subflow_candidate(
    service_key: CarrierPathKey,
    target: &ResponseSenderPathTarget,
) -> bool {
    target.observation.attachment_role != StreamOpenRole::Repair
        && target.observation.key != service_key
        && target.observation.key.underlay == service_key.underlay
        && !target.observation.is_service
        && target.observation.has_bulk_rate_evidence
}

fn response_target_measured_admission_snapshot(target: &ResponseSenderPathTarget) -> PathSnapshot {
    let mut snapshot = target.observation.snapshot;
    if target.observation.has_bulk_rate_evidence {
        // An app-limited poll does not erase the retained path-scoped rate
        // model. Proven Subflows must continue to pass ECF completion math.
        snapshot.app_limited = false;
    }
    snapshot
}

pub(super) fn response_target_is_startup_same_underlay_subflow_candidate(
    service_key: CarrierPathKey,
    service: &ResponseSenderPathTarget,
    target: &ResponseSenderPathTarget,
    ordered_tail_debt: u64,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> bool {
    let product_envelope = (mux_limits.max_path_flight_bytes as u64)
        .min(mux_limits.max_repair_bytes as u64)
        .min(mux_limits.max_reorder_bytes as u64)
        .min(mux_limits.max_stream_window_bytes)
        .max(payload_bytes as u64);
    let candidate_committed = response_target_assigned_product_bytes(target);
    // The ordered tail spans all unacknowledged product offsets, including
    // this candidate's assigned flight. The path snapshot is a fallback view.
    let projected_ordering_debt = ordered_tail_debt
        .max(candidate_committed)
        .saturating_add(payload_bytes as u64);
    let service_bulk_flows = service
        .observation
        .snapshot
        .active_flows
        .saturating_sub(service.observation.snapshot.active_latency_sensitive_flows);
    let target_bulk_flows = target
        .observation
        .snapshot
        .active_flows
        .saturating_sub(target.observation.snapshot.active_latency_sensitive_flows);

    service.observation.key == service_key
        && service.observation.is_service
        && service.observation.has_bulk_rate_evidence
        // One sustained response is real demand. The candidate must still be
        // less occupied than Service; flow count never substitutes for the
        // bounded epoch, sender evidence, or product-debt guards below.
        && service_bulk_flows > target_bulk_flows
        && service.observation.snapshot.active_latency_sensitive_flows == 0
        && service.observation.snapshot.session_active_latency_sensitive_flows == 0
        && target.observation.snapshot.active_latency_sensitive_flows == 0
        && target.observation.snapshot.session_active_latency_sensitive_flows == 0
        && target.observation.attachment_role == StreamOpenRole::Validation
        && target.observation.key != service_key
        && target.observation.key.underlay == service_key.underlay
        && !target.observation.is_service
        && target.observation.has_sender_evidence
        && !target.observation.has_bulk_rate_evidence
        && projected_ordering_debt <= product_envelope
}

pub(super) fn response_startup_sample_has_completion_opportunity(
    candidates: &[&ResponseSenderPathTarget],
    service: &ResponseSenderPathTarget,
    target: &ResponseSenderPathTarget,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> bool {
    let measured_same_family_subflow_exists = candidates.iter().copied().any(|candidate| {
        candidate.observation.key != target.observation.key
            && response_target_is_measured_same_underlay_subflow_candidate(
                service.observation.key,
                candidate,
            )
    });
    if !measured_same_family_subflow_exists {
        // The first bounded candidate is the bootstrap that makes an optional
        // path measurable. Latency pressure and resource/debt guards still
        // apply; requiring a preexisting completion model would be circular.
        return true;
    }
    // Once one optional path is measured, another candidate must justify its
    // own ordering risk; serially probing every cold path starves capacity that
    // the binding has already discovered.
    bulk_exploration_completion_projection(
        service.observation.snapshot,
        service.observation.eta_ms,
        target.observation.snapshot,
        target.observation.eta_ms,
        reliable_subflow_startup_sample_limit_bytes(mux_limits),
        payload_bytes,
        mux_limits,
    )
    .completes_within_service_reservoir()
}

pub(super) fn response_owner_bulk_model_suppression(
    target: &ResponseSenderPathTarget,
    lead: ResponseBulkLead,
    lower_owner: Option<CarrierPathKey>,
    effective_ordering_debt: u64,
    completion_backlog_bytes: u64,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    role: BulkAdmissionRole,
) -> Option<&'static str> {
    if response_unique_quic_data_would_expand_ordering_debt(
        lower_owner,
        target,
        effective_ordering_debt,
    ) {
        return Some("quic_ordering_debt");
    }
    bulk_candidate_admission_suppression_with_completion_backlog(
        BulkAdmissionCheck {
            best_snapshot: lead.snapshot,
            best_eta_ms: lead.eta_ms,
            candidate_snapshot: response_target_measured_admission_snapshot(target),
            candidate_eta_ms: target.observation.eta_ms,
            payload_bytes,
            mux_limits,
            role,
            stream_ordering_debt_bytes: effective_ordering_debt,
        },
        completion_backlog_bytes,
    )
}

pub(super) fn response_fallback_bulk_model_suppression(
    target: &ResponseSenderPathTarget,
    lead: ResponseBulkLead,
    ordering_debt: u64,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    role: BulkAdmissionRole,
) -> Option<&'static str> {
    // This is response-owned lower flight, so it is real Service completion
    // backlog. Request receive holes carry no such authority.
    bulk_candidate_admission_suppression_with_completion_backlog(
        BulkAdmissionCheck {
            best_snapshot: lead.snapshot,
            best_eta_ms: lead.eta_ms,
            candidate_snapshot: target.observation.snapshot,
            candidate_eta_ms: target.observation.eta_ms,
            payload_bytes,
            mux_limits,
            role,
            stream_ordering_debt_bytes: ordering_debt,
        },
        ordering_debt,
    )
}

#[cfg(test)]
pub(super) fn response_target_unique_owner_admission(
    target: &ResponseSenderPathTarget,
    candidates: &[&ResponseSenderPathTarget],
    lead: ResponseBulkLead,
    lower_owner: Option<CarrierPathKey>,
    ordering_debt: u64,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> PathAdmission {
    response_target_unique_owner_admission_with_epoch(
        target,
        candidates,
        lead,
        lower_owner,
        None,
        ordering_debt,
        ResponseOrderedTail::new(None, 0).for_candidate(target.observation.key),
        payload_bytes,
        mux_limits,
        None,
        true,
        false,
    )
    .admission
}

// Decides whether one candidate may own the next unique product byte range.
//
// The important split is:
// * Service: the current active owner, kept fed while healthy.
// * Subflow: an additional path admitted after path-scoped bulk-rate evidence,
//   or the one same-family Validation path consuming a bounded startup sample.
//
// Path proof, ACK-data visibility, and carrier attachment are evidence inputs,
// not implicit owner states. Startup ownership is explicit, bulk-only, and
// ledger-bounded.
#[allow(clippy::too_many_arguments)]
pub(super) fn response_target_unique_owner_admission_with_epoch(
    target: &ResponseSenderPathTarget,
    candidates: &[&ResponseSenderPathTarget],
    lead: ResponseBulkLead,
    lower_owner: Option<CarrierPathKey>,
    ordered_data_owner: Option<CarrierPathKey>,
    ordering_debt: u64,
    ordered_tail_debt: ResponseCandidateTailDebt,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    subflow_set: Option<&FlowSubflowSet>,
    startup_sampling_allowed: bool,
    allow_liveness_service_failover: bool,
) -> ResponseOwnerAdmission {
    let service_key =
        response_service_anchor_key(candidates, lower_owner, ordered_data_owner, lead.key);
    let candidate_tail_debt_bytes = ordered_tail_debt.external_bytes();
    let effective_ordering_debt = ordering_debt.max(candidate_tail_debt_bytes);
    let completion_backlog_bytes = ordering_debt.max(ordered_tail_debt.global_bytes());
    let role = response_bulk_admission_role(
        service_key,
        target.observation.key,
        lower_owner,
        effective_ordering_debt,
    );
    let result =
        |admission, subflow_admission_selection, model_suppression| ResponseOwnerAdmission {
            admission,
            subflow_admission_selection,
            bulk_role: role,
            model_suppression,
        };
    let direct_result = |admission: PathAdmission| {
        if !admission.owns_unique_data() {
            return result(admission, None, None);
        }
        let suppression = response_owner_bulk_model_suppression(
            target,
            lead,
            lower_owner,
            effective_ordering_debt,
            completion_backlog_bytes,
            payload_bytes,
            mux_limits,
            role,
        );
        suppression.map_or_else(
            || result(admission, None, None),
            |reason| result(PathAdmission::Standby, None, Some(reason)),
        )
    };
    if target.observation.attachment_role == StreamOpenRole::Repair {
        return result(PathAdmission::Standby, None, None);
    }
    let liveness_service_failover =
        allow_liveness_service_failover && target.observation.key == service_key;
    let continues_lower_frontier = lower_owner == Some(target.observation.key);
    let current_startup_owner_continues_lower_frontier = startup_sampling_allowed
        && continues_lower_frontier
        && target.observation.key != service_key
        && !target.observation.has_bulk_rate_evidence
        && subflow_set.is_some_and(|epoch| {
            epoch.service_key() == service_key
                && epoch.startup_owner_key() == Some(target.observation.key)
        })
        && candidates
            .iter()
            .copied()
            .find(|candidate| candidate.observation.key == service_key)
            .is_some_and(|service| {
                response_target_is_startup_same_underlay_subflow_candidate(
                    service_key,
                    service,
                    target,
                    candidate_tail_debt_bytes,
                    payload_bytes,
                    mux_limits,
                )
            });
    if continues_lower_frontier
        && (target.observation.key == service_key || target.observation.is_service)
    {
        if ordering_debt > 0 {
            return result(PathAdmission::Standby, None, None);
        }
        return if target.observation.is_service || target.observation.has_bulk_rate_evidence {
            direct_result(PathAdmission::Service)
        } else {
            result(PathAdmission::ProbeOnly, None, None)
        };
    }
    if continues_lower_frontier
        && target.observation.key != service_key
        && (!target.observation.has_bulk_rate_evidence || target.observation.is_service)
        && !current_startup_owner_continues_lower_frontier
    {
        // Only the exact bounded startup owner or an already measured Subflow
        // may continue its own authoritative lower frontier.
        return result(PathAdmission::Standby, None, None);
    }
    if lower_owner.is_some() && !continues_lower_frontier {
        return result(PathAdmission::Standby, None, None);
    }
    if target.observation.key == service_key {
        if ordered_tail_debt.global_bytes() > 0
            && Some(target.observation.key) != ordered_data_owner
            && !target.observation.has_bulk_rate_evidence
            && !liveness_service_failover
        {
            return result(PathAdmission::Standby, None, None);
        }
        return if target.observation.is_service
            || target.observation.has_bulk_rate_evidence
            || liveness_service_failover
        {
            direct_result(PathAdmission::Service)
        } else {
            result(PathAdmission::ProbeOnly, None, None)
        };
    }
    if target.observation.is_service {
        return result(PathAdmission::Standby, None, None);
    }
    let existing_startup_owner = subflow_set.is_some_and(|epoch| {
        epoch.service_key() == service_key
            && epoch.startup_owner_key() == Some(target.observation.key)
    });
    let startup_owner_allowed = startup_sampling_allowed
        && candidates
            .iter()
            .copied()
            .find(|candidate| candidate.observation.key == service_key)
            .is_some_and(|service| {
                response_target_is_startup_same_underlay_subflow_candidate(
                    service_key,
                    service,
                    target,
                    candidate_tail_debt_bytes,
                    payload_bytes,
                    mux_limits,
                ) && (existing_startup_owner
                    || response_startup_sample_has_completion_opportunity(
                        candidates,
                        service,
                        target,
                        payload_bytes,
                        mux_limits,
                    ))
            });
    if candidate_tail_debt_bytes > 0
        && !continues_lower_frontier
        && !response_target_is_measured_same_underlay_subflow_candidate(service_key, target)
        && !startup_owner_allowed
    {
        return result(PathAdmission::Standby, None, None);
    }

    let model_suppression = response_owner_bulk_model_suppression(
        target,
        lead,
        lower_owner,
        effective_ordering_debt,
        completion_backlog_bytes,
        payload_bytes,
        mux_limits,
        role,
    );
    let measured_model_allows_owner = model_suppression.is_none();
    // A candidate cannot produce a meaningful completion model until it has
    // received enough work to leave the app-limited startup state. The bounded
    // startup epoch therefore uses explicit role/pressure/resource guards and
    // does not compare the path against its own underfed rate prior.
    let model_allows_owner = startup_owner_allowed || measured_model_allows_owner;
    let completion_improves =
        measured_model_allows_owner && target.observation.has_bulk_rate_evidence;
    let startup_owner_credit_bytes =
        usize::try_from(reliable_subflow_startup_sample_limit_bytes(mux_limits))
            .unwrap_or(usize::MAX)
            .max(payload_bytes);
    let input = SubflowAdmissionInput {
        key: target.observation.key,
        bulk_rate_proven: target.observation.has_bulk_rate_evidence,
        startup_owner_allowed,
        frontier_clear: model_allows_owner,
        completion_improves,
        observed_goodput_non_degrading: model_allows_owner,
        owner_bytes: payload_bytes,
    };
    let mut epoch = subflow_set
        .filter(|epoch| epoch.matches_envelope(service_key, startup_owner_credit_bytes))
        .cloned()
        .unwrap_or_else(|| FlowSubflowSet::new(service_key, startup_owner_credit_bytes));
    let admission = epoch.admit_subflow_owner(input);
    let selection =
        (admission == PathAdmission::Subflow).then_some(ResponseSubflowAdmissionSelection {
            service: service_key,
            startup_owner_credit_bytes,
            input,
        });
    result(
        admission,
        selection,
        if startup_owner_allowed {
            None
        } else {
            model_suppression
        },
    )
}

pub(super) fn response_target_can_own_unique_bulk_data(
    target: &ResponseSenderPathTarget,
    candidates: &[&ResponseSenderPathTarget],
    lead: ResponseBulkLead,
    lower_owner: Option<CarrierPathKey>,
    ordering_debt: u64,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> bool {
    response_target_can_own_unique_bulk_data_with_epoch(
        target,
        candidates,
        lead,
        lower_owner,
        ordering_debt,
        payload_bytes,
        mux_limits,
        None,
    )
}

fn response_target_can_own_unique_bulk_data_with_epoch(
    target: &ResponseSenderPathTarget,
    candidates: &[&ResponseSenderPathTarget],
    lead: ResponseBulkLead,
    lower_owner: Option<CarrierPathKey>,
    ordering_debt: u64,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    subflow_set: Option<&FlowSubflowSet>,
) -> bool {
    let admission = response_target_unique_owner_admission_with_epoch(
        target,
        candidates,
        lead,
        lower_owner,
        None,
        ordering_debt,
        ResponseOrderedTail::new(None, 0).for_candidate(target.observation.key),
        payload_bytes,
        mux_limits,
        subflow_set,
        true,
        false,
    )
    .admission;
    admission.owns_unique_data()
}

pub(super) fn response_target_assigned_product_bytes(target: &ResponseSenderPathTarget) -> u64 {
    // Product flight includes frames still pending in the carrier command
    // pipe. Treat the ledger and queue snapshots as overlapping views so the
    // same OwnerData cannot consume calibration credit twice.
    target.observation.snapshot.product_bytes_in_flight.max(
        target
            .observation
            .snapshot
            .queue_bytes
            .max(target.observation.command_pending_bytes),
    )
}

pub(super) fn response_same_family_reservoir_for_service(
    service: &ResponseSenderPathTarget,
    ordered_tail: ResponseOrderedTail,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> Option<ResponseSameFamilyReservoir> {
    if !service.observation.is_service
        || !service.observation.has_bulk_rate_evidence
        || service.observation.snapshot.active_latency_sensitive_flows > 0
        || service
            .observation
            .snapshot
            .session_active_latency_sensitive_flows
            > 0
    {
        return None;
    }
    let service_horizon = bulk_service_horizon_payload_bytes(payload_bytes, mux_limits);
    let service_assigned = service.observation.owner_data_in_flight_bytes;
    // Same-family proven paths may drain a bulk backlog concurrently, but a
    // full resource envelope can become tens of MiB of receiver-prefix debt.
    // The BBR-shaped feed reservoir preserves aggregation headroom while
    // keeping cross-path ownership close enough for latency-sensitive takeover.
    let ordered_reservoir = bulk_service_feed_reservoir_payload_bytes(payload_bytes, mux_limits);

    ResponseSameFamilyReservoir::new(
        service.observation.key,
        ordered_tail,
        service_assigned,
        service_horizon,
        ordered_reservoir,
        payload_bytes,
    )
}

pub(super) fn response_target_is_same_family_reservoir_candidate(
    reservoir: ResponseSameFamilyReservoir,
    target: &ResponseSenderPathTarget,
) -> bool {
    target.observation.key != reservoir.service()
        && target.observation.key.underlay == reservoir.service().underlay
        && !target.observation.is_service
        && target.observation.has_bulk_rate_evidence
        && target.observation.snapshot.active_latency_sensitive_flows == 0
        && target
            .observation
            .snapshot
            .session_active_latency_sensitive_flows
            == 0
}

pub(super) fn response_same_family_reservoir_candidate_debt(
    reservoir: ResponseSameFamilyReservoir,
    target: &ResponseSenderPathTarget,
) -> ResponseCandidateTailDebt {
    // The global tail contains unique OwnerData. Subtract only this candidate's
    // unique share; generic carrier admission separately keeps every OwnerData
    // and RepairData copy charged as product flight.
    reservoir.for_candidate(
        target.observation.key,
        target.observation.owner_data_in_flight_bytes,
    )
}

pub(super) fn response_target_has_emission_credit(
    target: &ResponseSenderPathTarget,
    lane: FlowLane,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> bool {
    if !lane.is_bulk() {
        return true;
    }
    let credit = response_target_emission_credit_bytes(target, lane, payload_bytes, mux_limits);
    target
        .observation
        .command_pending_bytes
        .saturating_add(payload_bytes as u64)
        <= credit as u64
}

pub(super) fn response_service_has_assigned_owner_credit(
    target: &ResponseSenderPathTarget,
    lane: FlowLane,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> bool {
    if !lane.is_bulk() {
        return true;
    }
    let credit = response_service_emission_credit_bytes(target, payload_bytes, mux_limits);
    // Product flight owns the offset range from carrier enqueue until
    // STREAM_ACK, including frames still pending in the carrier pipe. Retain
    // an independent queue-pressure fallback for incomplete/synthetic
    // snapshots, but use a union-style maximum so those views cannot charge
    // the same assigned OwnerData twice against hard Service credit.
    let assigned = target.observation.snapshot.product_bytes_in_flight.max(
        target
            .observation
            .snapshot
            .queue_bytes
            .max(target.observation.command_pending_bytes),
    );
    assigned.saturating_add(payload_bytes as u64) <= credit as u64
}

pub(super) fn response_service_emission_credit_bytes(
    target: &ResponseSenderPathTarget,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> usize {
    if !target.observation.has_service_feed_evidence {
        return response_service_startup_emission_credit_bytes(
            target.observation.key.underlay,
            payload_bytes,
            mux_limits,
        );
    }
    if target.observation.snapshot.active_latency_sensitive_flows > 0 {
        return usize::try_from(bulk_latency_pressure_service_feed_window_bytes(
            payload_bytes,
            mux_limits,
        ))
        .unwrap_or(usize::MAX)
        .max(payload_bytes)
        .max(1);
    }
    usize::try_from(bulk_active_service_product_envelope_bytes(
        payload_bytes,
        mux_limits,
    ))
    .unwrap_or(usize::MAX)
    .max(payload_bytes)
    .max(1)
}

pub(super) fn response_target_emission_credit_bytes(
    target: &ResponseSenderPathTarget,
    lane: FlowLane,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> usize {
    if lane.is_bulk() {
        if target.observation.is_service {
            return response_service_emission_credit_bytes(target, payload_bytes, mux_limits);
        }
        if target.observation.key.underlay == UnderlayProtocol::Udp {
            return response_quic_carrier_feed_credit_bytes(target, payload_bytes, mux_limits);
        }
    }
    adaptive_reliable_relay_inflight_bytes(Some(target.observation.snapshot), lane, mux_limits)
        .max(reliable_relay_scheduler_quantum_cap(
            Some(target.observation.snapshot),
            lane,
            mux_limits,
        ))
        .max(payload_bytes)
        .max(1)
}

pub(super) fn response_service_startup_emission_credit_bytes(
    underlay: UnderlayProtocol,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> usize {
    if underlay == UnderlayProtocol::Udp {
        bulk_service_feed_reservoir_payload_bytes(payload_bytes, mux_limits)
    } else {
        bulk_service_horizon_payload_bytes(payload_bytes, mux_limits)
    }
}

pub(super) fn response_quic_carrier_feed_credit_bytes(
    target: &ResponseSenderPathTarget,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> usize {
    let product_envelope = mux_limits
        .max_path_flight_bytes
        .min(mux_limits.max_repair_bytes)
        .min(mux_limits.max_reorder_bytes)
        .min(usize::try_from(mux_limits.max_stream_window_bytes).unwrap_or(usize::MAX))
        .max(payload_bytes)
        .max(1);
    let carrier_window = usize::try_from(target.observation.snapshot.inflight_limit_bytes)
        .unwrap_or(usize::MAX)
        .max(reliable_bulk_carrier_feed_quantum_bytes(mux_limits));
    let live_carrier_debt = usize::try_from(
        target
            .observation
            .snapshot
            .bytes_in_flight
            .saturating_add(target.observation.snapshot.queue_bytes),
    )
    .unwrap_or(usize::MAX);
    product_envelope
        .min(carrier_window.saturating_add(live_carrier_debt))
        .max(reliable_bulk_carrier_feed_quantum_bytes(mux_limits))
        .max(payload_bytes)
}

#[cfg(test)]
#[path = "admission_test.rs"]
mod tests;
