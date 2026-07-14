//! Pure admission and completion projections for ordered bulk product data.

#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::mux::MuxLimits;
use crate::protocol::UnderlayProtocol;
use crate::runtime::relay_open::RelayPathKey;
use crate::runtime::{BBR_DEFAULT_CWND_GAIN, BBR_MAX_SEND_QUANTUM_BYTES};
use crate::scheduler::{FlowLane, PathSnapshot};

// Decisions in this module never mutate a path or enqueue carrier work.

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct BulkPathCandidate {
    pub(in crate::runtime) key: RelayPathKey,
    pub(in crate::runtime) eta_ms: f64,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(in crate::runtime) has_liveness_evidence: bool,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(in crate::runtime) has_path_proof_evidence: bool,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(in crate::runtime) has_ack_data_evidence: bool,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(in crate::runtime) has_bulk_rate_evidence: bool,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(in crate::runtime) has_sender_delivery_evidence: bool,
    pub(in crate::runtime) snapshot: PathSnapshot,
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum BulkAdmissionRole {
    ActiveDataPath,
    ActiveSingleCarrier,
    AdditionalSameUnderlay,
    AdditionalCrossUnderlay,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct BulkAdmissionCheck {
    pub(in crate::runtime) best_snapshot: PathSnapshot,
    pub(in crate::runtime) best_eta_ms: f64,
    pub(in crate::runtime) candidate_snapshot: PathSnapshot,
    pub(in crate::runtime) candidate_eta_ms: f64,
    pub(in crate::runtime) payload_bytes: usize,
    pub(in crate::runtime) mux_limits: MuxLimits,
    pub(in crate::runtime) role: BulkAdmissionRole,
    // Lower unique bytes on other owners are completion/HOL debt. Same-family
    // TCP charges their aggregate only to the stream reorder envelope; the
    // candidate-local pipe is bounded independently by the inflight gate.
    pub(in crate::runtime) stream_ordering_debt_bytes: u64,
}

pub(in crate::runtime) fn bulk_additional_admission_role(
    reference_underlay: UnderlayProtocol,
    candidate_underlay: UnderlayProtocol,
) -> BulkAdmissionRole {
    if reference_underlay == candidate_underlay {
        BulkAdmissionRole::AdditionalSameUnderlay
    } else {
        BulkAdmissionRole::AdditionalCrossUnderlay
    }
}

pub(in crate::runtime) fn bulk_striping_admitted_subflows(
    candidates: Vec<BulkPathCandidate>,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> Vec<BulkPathCandidate> {
    let Some(best) = candidates.first().copied() else {
        return candidates;
    };
    let mut selected = Vec::new();
    for candidate in candidates {
        let role = if selected.is_empty() {
            BulkAdmissionRole::ActiveDataPath
        } else {
            bulk_additional_admission_role(best.key.underlay, candidate.key.underlay)
        };
        let suppression =
            bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
                best_snapshot: best.snapshot,
                best_eta_ms: best.eta_ms,
                candidate_snapshot: candidate.snapshot,
                candidate_eta_ms: candidate.eta_ms,
                payload_bytes,
                mux_limits,
                role,
                stream_ordering_debt_bytes: 0,
            });
        if suppression.is_none() {
            selected.push(candidate);
        } else {
            #[cfg(feature = "lab-diagnostics")]
            let reason = suppression.unwrap_or("suppressed");
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "bulk_striping_candidate_suppressed",
                format_args!(
                    "path_underlay={:?} path_index={} role={:?} eta_ms={:.3} best_eta_ms={:.3} horizon_ms={:.3} best_sender_evidence={} candidate_sender_evidence={} best_confidence={:.3} candidate_confidence={:.3} best_app_limited={} candidate_app_limited={} product_bytes_in_flight={} carrier_bytes_in_flight={} carrier_inflight_limit={} product_inflight_limit={} scheduler_debt={} queue_bytes={} reorder_budget={} reason={}",
                    candidate.key.underlay,
                    candidate.key.index,
                    role,
                    candidate.eta_ms,
                    best.eta_ms,
                    bulk_completion_horizon_ms(
                        best.snapshot,
                        best.eta_ms,
                        candidate.snapshot,
                        payload_bytes,
                        mux_limits,
                    ),
                    best.has_sender_delivery_evidence,
                    candidate.has_sender_delivery_evidence,
                    best.snapshot.confidence,
                    candidate.snapshot.confidence,
                    best.snapshot.app_limited,
                    candidate.snapshot.app_limited,
                    bulk_product_reorder_debt_bytes(candidate.snapshot),
                    candidate.snapshot.bytes_in_flight,
                    bulk_carrier_inflight_limit_bytes(
                        candidate.snapshot,
                        payload_bytes,
                        mux_limits,
                        role,
                    ),
                    bulk_product_inflight_limit_bytes(
                        candidate.snapshot,
                        payload_bytes,
                        mux_limits,
                        role,
                    ),
                    bulk_scheduler_inflight_debt_bytes(candidate.snapshot, role),
                    candidate.snapshot.queue_bytes,
                    bulk_admission_reorder_budget_bytes(
                        candidate.snapshot,
                        payload_bytes,
                        mux_limits,
                        role,
                    ),
                    reason,
                ),
            );
        }
    }
    selected
}

pub(in crate::runtime) fn bulk_service_horizon_payload_bytes(
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> usize {
    let service_payload = payload_bytes
        .max(BBR_MAX_SEND_QUANTUM_BYTES.min(mux_limits.max_reliable_relay_chunk_bytes))
        .max(1);
    let envelope = bulk_service_product_envelope_payload_bytes(service_payload, mux_limits);
    let horizon = ((service_payload as f64) * (envelope as f64))
        .sqrt()
        .round() as usize;
    horizon.clamp(service_payload, envelope)
}

pub(in crate::runtime) fn bulk_service_product_envelope_payload_bytes(
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> usize {
    let service_payload = payload_bytes
        .max(BBR_MAX_SEND_QUANTUM_BYTES.min(mux_limits.max_reliable_relay_chunk_bytes))
        .max(1);
    let stream_window = usize::try_from(mux_limits.max_stream_window_bytes).unwrap_or(usize::MAX);
    mux_limits
        .max_path_flight_bytes
        .min(mux_limits.max_reorder_bytes)
        .min(stream_window)
        .max(service_payload)
        .max(1)
}

pub(in crate::runtime) fn bulk_service_feed_reservoir_payload_bytes(
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> usize {
    let horizon = bulk_service_horizon_payload_bytes(payload_bytes, mux_limits);
    let envelope = bulk_service_product_envelope_payload_bytes(payload_bytes, mux_limits);
    ((horizon as f64) * BBR_DEFAULT_CWND_GAIN)
        .ceil()
        .clamp(horizon as f64, envelope as f64) as usize
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct BulkExplorationCompletionProjection {
    pub(in crate::runtime) candidate_completion_ms: f64,
    pub(in crate::runtime) service_reservoir_horizon_ms: f64,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(in crate::runtime) exploration_bytes: u64,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(in crate::runtime) service_followup_bytes: u64,
}

impl BulkExplorationCompletionProjection {
    pub(in crate::runtime) fn completes_within_service_reservoir(self) -> bool {
        self.candidate_completion_ms <= self.service_reservoir_horizon_ms
    }
}

/// Bounds unique-byte exploration by the ordered product reservoir it may block.
///
/// The candidate ETA already includes the next payload. After that payload, the
/// candidate must finish its authorized seed before Service can consume the
/// remaining feed reservoir behind those lower offsets. Carrier controllers
/// still own pacing; this model only decides whether exploration can own bytes.
pub(in crate::runtime) fn bulk_exploration_completion_projection(
    service_snapshot: PathSnapshot,
    service_eta_ms: f64,
    candidate_snapshot: PathSnapshot,
    candidate_eta_ms: f64,
    exploration_bytes: u64,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> BulkExplorationCompletionProjection {
    let service_reservoir_bytes =
        bulk_service_feed_reservoir_payload_bytes(payload_bytes, mux_limits) as u64;
    let service_followup_bytes = service_reservoir_bytes.saturating_sub(exploration_bytes);
    let candidate_followup_bytes = exploration_bytes.saturating_sub(payload_bytes as u64);
    BulkExplorationCompletionProjection {
        candidate_completion_ms: candidate_eta_ms.max(0.0)
            + bulk_bytes_tx_ms(candidate_snapshot, candidate_followup_bytes),
        service_reservoir_horizon_ms: service_eta_ms.max(0.0)
            + bulk_bytes_tx_ms(service_snapshot, service_followup_bytes),
        exploration_bytes,
        service_followup_bytes,
    }
}

pub(in crate::runtime) fn bulk_tcp_calibration_completion_projection(
    service_snapshot: PathSnapshot,
    service_eta_ms: f64,
    candidate_snapshot: PathSnapshot,
    candidate_eta_ms: f64,
    exploration_bytes: u64,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> BulkExplorationCompletionProjection {
    // Calibration follows an exact proven startup prefix. Those missing lower
    // bytes do not occupy receiver reorder memory; later Service bytes do. Keep
    // one full bounded Service reservoir behind the calibration prefix.
    let service_followup_bytes =
        bulk_service_feed_reservoir_payload_bytes(payload_bytes, mux_limits) as u64;
    let candidate_followup_bytes = exploration_bytes.saturating_sub(payload_bytes as u64);
    BulkExplorationCompletionProjection {
        candidate_completion_ms: candidate_eta_ms.max(0.0)
            + bulk_bytes_tx_ms(candidate_snapshot, candidate_followup_bytes),
        service_reservoir_horizon_ms: service_eta_ms.max(0.0)
            + bulk_bytes_tx_ms(service_snapshot, service_followup_bytes),
        exploration_bytes,
        service_followup_bytes,
    }
}

// Source staging is product admission, not socket-loop orchestration. Keeping
// this limit beside the Service horizon prevents event loops from inventing a
// second ownership model for bytes that do not have offsets yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct ReliableSourceServiceStagingContext {
    /// Switchable responses own an exact global tail; fixed request-side
    /// outputs keep their narrower established staging policy.
    pub(in crate::runtime) allows_product_envelope: bool,
    pub(in crate::runtime) has_latency_pressure: bool,
    pub(in crate::runtime) has_feed_evidence: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct ReliableSourceStagingContext {
    /// Mixed-family raw bytes remain unassigned until dispatch chooses a path.
    pub(in crate::runtime) independent: bool,
    pub(in crate::runtime) service: Option<ReliableSourceServiceStagingContext>,
}

pub(in crate::runtime) fn reliable_relay_source_staging_owner_tail_headroom(
    context: ReliableSourceStagingContext,
    lane: FlowLane,
    ordered_owner_debt_bytes: usize,
    queued_data_bytes: usize,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> usize {
    if !lane.is_bulk() {
        return usize::MAX;
    }
    let service_has_latency_pressure = context
        .service
        .is_some_and(|service| service.has_latency_pressure);
    let service_has_feed_evidence = context
        .service
        .is_some_and(|service| service.has_feed_evidence);
    let service_allows_product_envelope = context
        .service
        .is_some_and(|service| service.allows_product_envelope);
    let feed_limit = if service_has_latency_pressure {
        bulk_service_horizon_payload_bytes(payload_bytes, mux_limits)
    } else if !service_has_feed_evidence {
        if service_allows_product_envelope && !context.independent {
            // A switchable Service needs enough bounded work to escape an
            // app-limited carrier sample. This is still only the feed
            // reservoir; native evidence is required for the full envelope.
            bulk_service_feed_reservoir_payload_bytes(payload_bytes, mux_limits)
        } else {
            bulk_service_horizon_payload_bytes(payload_bytes, mux_limits)
        }
    } else if service_allows_product_envelope && !context.independent {
        // Same-family responses charge every assigned owner range plus raw
        // queue byte to one exact tail. Per-path admission still decides where
        // those bytes go; this boundary only avoids starving native carriers.
        bulk_service_product_envelope_payload_bytes(payload_bytes, mux_limits)
    } else {
        bulk_service_feed_reservoir_payload_bytes(payload_bytes, mux_limits)
    };
    let staged_debt = if context.independent {
        // Mixed-family raw bytes have no offset/path owner yet, but still use
        // sender-service memory and therefore consume the global feed limit.
        queued_data_bytes
    } else {
        ordered_owner_debt_bytes.saturating_add(queued_data_bytes)
    };
    feed_limit.saturating_sub(staged_debt)
}

#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::runtime) fn bulk_candidate_admission_suppression(
    best_snapshot: PathSnapshot,
    best_eta_ms: f64,
    candidate_snapshot: PathSnapshot,
    candidate_eta_ms: f64,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    role: BulkAdmissionRole,
) -> Option<&'static str> {
    bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
        best_snapshot,
        best_eta_ms,
        candidate_snapshot,
        candidate_eta_ms,
        payload_bytes,
        mux_limits,
        role,
        stream_ordering_debt_bytes: 0,
    })
}

pub(in crate::runtime) fn bulk_candidate_admission_suppression_with_ordering_debt(
    check: BulkAdmissionCheck,
) -> Option<&'static str> {
    // Ordering debt bounds receive-hole resources; it does not prove that
    // Service has an independent lower backlog. Response policy supplies that
    // product backlog explicitly when it owns one.
    bulk_candidate_admission_suppression_with_completion_backlog(check, 0)
}

pub(in crate::runtime) fn bulk_candidate_admission_suppression_with_completion_backlog(
    check: BulkAdmissionCheck,
    completion_backlog_bytes: u64,
) -> Option<&'static str> {
    if let Some(reason) = bulk_cross_underlay_completion_suppression(check) {
        return Some(reason);
    }
    if let Some(reason) = bulk_same_underlay_completion_suppression(check, completion_backlog_bytes)
    {
        return Some(reason);
    }
    if !bulk_candidate_within_inflight_limit(check) {
        return Some("inflight_limit");
    }
    if !bulk_candidate_within_reorder_budget(
        check.candidate_snapshot,
        check.payload_bytes,
        check.mux_limits,
        check.role,
        check.stream_ordering_debt_bytes,
    ) {
        return Some("reorder_budget");
    }
    if (bulk_completion_horizon_applies(check)
        || (check.stream_ordering_debt_bytes > 0 && check.candidate_eta_ms > check.best_eta_ms))
        && check.candidate_eta_ms
            > bulk_completion_horizon_ms_with_ordering_debt(
                check.best_snapshot,
                check.best_eta_ms,
                check.candidate_snapshot,
                check.payload_bytes,
                check.mux_limits,
                check.stream_ordering_debt_bytes,
            )
    {
        return Some("completion_horizon");
    }
    None
}

fn bulk_same_underlay_completion_suppression(
    check: BulkAdmissionCheck,
    completion_backlog_bytes: u64,
) -> Option<&'static str> {
    if check.role != BulkAdmissionRole::AdditionalSameUnderlay {
        return None;
    }
    if !bulk_same_underlay_requires_completion_gain(check.candidate_snapshot) {
        return None;
    }
    // For a full bulk backlog, compare the candidate with Service draining the
    // lower ordered tail, not only with Service's next quantum. This is the ECF
    // completion boundary: a candidate that finishes before those earlier bytes
    // adds capacity without extending the receive hole. A clear/small frontier
    // retains the strict next-quantum rule.
    // Service ETA already includes its command queue and, for QUIC, native
    // carrier flight. Subtract those overlapping views from the product tail
    // before adding backlog transmission time.
    let service_modeled_debt_bytes = check
        .best_snapshot
        .queue_bytes
        .saturating_add(check.best_snapshot.product_queue_bytes)
        .saturating_add(check.best_snapshot.bytes_in_flight);
    let service_followup_bytes = completion_backlog_bytes
        .saturating_sub(service_modeled_debt_bytes)
        .max(check.payload_bytes as u64);
    let service_completion_eta_ms =
        check.best_eta_ms + bulk_bytes_tx_ms(check.best_snapshot, service_followup_bytes);
    if check.candidate_eta_ms > service_completion_eta_ms {
        return Some("same_underlay_no_completion_gain");
    }
    None
}

fn bulk_same_underlay_requires_completion_gain(candidate: PathSnapshot) -> bool {
    // This gate is for measured Subflow owner admission only.  It MUST NOT be
    // applied to Probe candidates, because those paths do not yet have a
    // meaningful completion model.  Otherwise the scheduler becomes circular:
    // the path needs evidence to prove a rate, while the rate proof is required
    // to own bytes.  Low-confidence or app-limited same-underlay paths remain
    // Probe/Standby/RepairOnly; they are simply not rejected by measured
    // completion-gain math.
    !candidate.app_limited && candidate.confidence >= 1.0
}

fn bulk_completion_horizon_applies(check: BulkAdmissionCheck) -> bool {
    if check.role == BulkAdmissionRole::ActiveDataPath {
        return false;
    }
    if check.stream_ordering_debt_bytes > 0 {
        return true;
    }
    if check.role == BulkAdmissionRole::AdditionalSameUnderlay {
        return false;
    }
    true
}

fn bulk_cross_underlay_completion_suppression(check: BulkAdmissionCheck) -> Option<&'static str> {
    if check.role != BulkAdmissionRole::AdditionalCrossUnderlay {
        return None;
    }
    if check.stream_ordering_debt_bytes > 0 {
        return Some("cross_underlay_ordering_debt");
    }
    let lead_next_quantum_eta_ms =
        check.best_eta_ms + bulk_payload_tx_ms(check.best_snapshot, check.payload_bytes);
    if check.candidate_eta_ms > lead_next_quantum_eta_ms {
        return Some("cross_underlay_no_completion_gain");
    }
    None
}

#[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
pub(in crate::runtime) fn bulk_completion_horizon_ms(
    best_snapshot: PathSnapshot,
    best_eta_ms: f64,
    candidate_snapshot: PathSnapshot,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> f64 {
    bulk_completion_horizon_ms_with_ordering_debt(
        best_snapshot,
        best_eta_ms,
        candidate_snapshot,
        payload_bytes,
        mux_limits,
        0,
    )
}

pub(in crate::runtime) fn bulk_completion_horizon_ms_with_ordering_debt(
    best_snapshot: PathSnapshot,
    best_eta_ms: f64,
    candidate_snapshot: PathSnapshot,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    stream_ordering_debt_bytes: u64,
) -> f64 {
    let best_payload_tx_ms = bulk_payload_tx_ms(best_snapshot, payload_bytes);
    let absorption_ms = bulk_reorder_absorption_ms(
        best_snapshot,
        candidate_snapshot,
        payload_bytes,
        mux_limits,
        stream_ordering_debt_bytes,
    );
    best_eta_ms.max(0.0) + best_payload_tx_ms + absorption_ms
}

fn bulk_candidate_within_inflight_limit(check: BulkAdmissionCheck) -> bool {
    let candidate = check.candidate_snapshot;
    let payload_bytes = check.payload_bytes;
    let mux_limits = check.mux_limits;
    let role = check.role;
    if bulk_active_lead_has_contiguous_frontier(candidate, role, check.stream_ordering_debt_bytes) {
        let inflight_limit =
            bulk_active_service_product_envelope_bytes(candidate, payload_bytes, mux_limits);
        let committed = bulk_assigned_service_debt_bytes(candidate);
        if committed.saturating_add(payload_bytes as u64) > inflight_limit
            && committed >= inflight_limit
        {
            return false;
        }
        if bulk_active_role_has_latency_pressure(candidate, role) {
            let backlog_limit =
                bulk_latency_pressure_service_feed_window_bytes(payload_bytes, mux_limits);
            return committed.saturating_add(payload_bytes as u64) <= backlog_limit
                || committed < backlog_limit;
        }
        return true;
    }
    let use_carrier_gate = candidate.underlay == UnderlayProtocol::Udp
        && !bulk_uses_product_only_active_gate(candidate, role);
    let (inflight_limit, committed) = if use_carrier_gate {
        (
            bulk_carrier_inflight_limit_bytes(candidate, payload_bytes, mux_limits, role),
            bulk_scheduler_inflight_debt_bytes(candidate, role),
        )
    } else {
        (
            bulk_product_inflight_limit_bytes(candidate, payload_bytes, mux_limits, role),
            bulk_scheduler_inflight_debt_bytes(candidate, role),
        )
    };
    if !bulk_quantum_granular_limit_allows(committed, payload_bytes, inflight_limit, role) {
        return false;
    }
    true
}

fn bulk_quantum_granular_limit_allows(
    committed: u64,
    payload_bytes: usize,
    limit: u64,
    role: BulkAdmissionRole,
) -> bool {
    if limit == 0 {
        return true;
    }
    let payload_bytes = payload_bytes as u64;
    if committed.saturating_add(payload_bytes) <= limit {
        return true;
    }
    matches!(role, BulkAdmissionRole::AdditionalSameUnderlay) && committed < limit
}

fn bulk_active_lead_has_contiguous_frontier(
    candidate: PathSnapshot,
    role: BulkAdmissionRole,
    stream_ordering_debt_bytes: u64,
) -> bool {
    let _ = candidate;
    matches!(
        role,
        BulkAdmissionRole::ActiveDataPath | BulkAdmissionRole::ActiveSingleCarrier
    ) && stream_ordering_debt_bytes == 0
}

pub(in crate::runtime) fn bulk_active_service_product_envelope_bytes(
    candidate: PathSnapshot,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> u64 {
    let stream_window = usize::try_from(mux_limits.max_stream_window_bytes).unwrap_or(usize::MAX);
    let _ = candidate;
    mux_limits
        .max_path_flight_bytes
        .min(stream_window)
        .min(mux_limits.max_reorder_bytes)
        .max(payload_bytes) as u64
}

fn bulk_product_inflight_limit_bytes(
    candidate: PathSnapshot,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    role: BulkAdmissionRole,
) -> u64 {
    if candidate.underlay == UnderlayProtocol::Udp
        && matches!(
            role,
            BulkAdmissionRole::ActiveDataPath | BulkAdmissionRole::ActiveSingleCarrier
        )
    {
        return bulk_active_service_product_envelope_bytes(candidate, payload_bytes, mux_limits);
    }
    let configured_ceiling = mux_limits.max_path_flight_bytes as u64;
    let payload_floor = payload_bytes as u64;
    let bdp = bulk_path_bdp_bytes(candidate);
    let bdp_limit = bulk_bbr_inflight_bytes(bdp).max(payload_floor);
    let modeled_limit = if candidate.inflight_limit_bytes > 0 {
        candidate
            .inflight_limit_bytes
            .min(bdp_limit)
            .max(payload_floor)
    } else {
        bdp_limit
    };
    let modeled_limit = modeled_limit.min(configured_ceiling.max(payload_floor));
    if bulk_active_role_has_latency_pressure(candidate, role) {
        let service_horizon = bulk_service_horizon_payload_bytes(payload_bytes, mux_limits) as u64;
        return modeled_limit.min(service_horizon.max(payload_floor));
    }
    modeled_limit
}

fn bulk_carrier_inflight_limit_bytes(
    candidate: PathSnapshot,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    role: BulkAdmissionRole,
) -> u64 {
    if candidate.underlay != UnderlayProtocol::Udp {
        return bulk_product_inflight_limit_bytes(candidate, payload_bytes, mux_limits, role);
    }
    let configured_ceiling = mux_limits.max_path_flight_bytes as u64;
    let payload_floor = payload_bytes as u64;
    let carrier_limit = if candidate.inflight_limit_bytes > 0 {
        candidate.inflight_limit_bytes
    } else {
        bulk_bbr_inflight_bytes(bulk_path_bdp_bytes(candidate))
    };
    carrier_limit
        .max(payload_floor)
        .min(configured_ceiling.max(payload_floor))
}

fn bulk_candidate_within_reorder_budget(
    candidate: PathSnapshot,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    role: BulkAdmissionRole,
    stream_ordering_debt_bytes: u64,
) -> bool {
    if role == BulkAdmissionRole::AdditionalSameUnderlay {
        // Per-path product authority and receiver reorder memory are distinct
        // resources. Foreign lower offsets consume only the stream envelope;
        // this candidate's own unique bytes still need local QUIC/TCP credit.
        let candidate_product_debt = bulk_product_reorder_debt_bytes(candidate);
        if !bulk_quantum_granular_limit_allows(
            candidate_product_debt,
            payload_bytes,
            bulk_same_underlay_product_authority_bytes(candidate, payload_bytes, mux_limits),
            role,
        ) {
            return false;
        }
        return bulk_quantum_granular_limit_allows(
            candidate_product_debt.saturating_add(stream_ordering_debt_bytes),
            payload_bytes,
            bulk_stream_reorder_envelope_bytes(payload_bytes, mux_limits),
            role,
        );
    }
    let admission_budget = bulk_admission_reorder_budget_bytes_for_ordering_debt(
        candidate,
        payload_bytes,
        mux_limits,
        role,
        stream_ordering_debt_bytes,
    );
    bulk_quantum_granular_limit_allows(
        bulk_total_reorder_debt_bytes(candidate, role, stream_ordering_debt_bytes),
        payload_bytes,
        admission_budget,
        role,
    )
}

fn bulk_reorder_absorption_ms(
    best: PathSnapshot,
    candidate: PathSnapshot,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    stream_ordering_debt_bytes: u64,
) -> f64 {
    let budget = bulk_effective_reorder_budget_bytes(candidate, payload_bytes, mux_limits);
    let committed = bulk_total_reorder_debt_bytes(
        candidate,
        BulkAdmissionRole::AdditionalCrossUnderlay,
        stream_ordering_debt_bytes,
    )
    .saturating_add(payload_bytes as u64);
    let remaining = budget.saturating_sub(committed);
    remaining as f64 * 8.0 / bulk_effective_rate_bps(best) * 1000.0
}

fn bulk_scheduler_inflight_debt_bytes(candidate: PathSnapshot, role: BulkAdmissionRole) -> u64 {
    if matches!(
        role,
        BulkAdmissionRole::ActiveDataPath | BulkAdmissionRole::ActiveSingleCarrier
    ) {
        return bulk_assigned_service_debt_bytes(candidate);
    }
    if candidate.underlay == UnderlayProtocol::Udp
        && matches!(role, BulkAdmissionRole::AdditionalCrossUnderlay)
    {
        return candidate
            .queue_bytes
            .saturating_add(candidate.bytes_in_flight);
    }
    bulk_product_reorder_debt_bytes(candidate)
}

fn bulk_assigned_service_debt_bytes(candidate: PathSnapshot) -> u64 {
    // Product flight owns accepted OwnerData from carrier enqueue through
    // STREAM_ACK, so the carrier queue is an overlapping pressure view rather
    // than additional product debt.
    candidate.product_bytes_in_flight.max(candidate.queue_bytes)
}

fn bulk_uses_product_only_active_gate(candidate: PathSnapshot, role: BulkAdmissionRole) -> bool {
    candidate.underlay == UnderlayProtocol::Udp
        && matches!(
            role,
            BulkAdmissionRole::ActiveDataPath | BulkAdmissionRole::ActiveSingleCarrier
        )
    // Lane fairness is enforced above carrier admission. It must not move the
    // active owner back onto a tiny carrier/startup hard gate while the product
    // ordered frontier is clear. QUIC/TCP carriers own packet pacing below
    // this layer.
}

pub(in crate::runtime) fn bulk_latency_pressure_service_feed_window_bytes(
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> u64 {
    let service_horizon = bulk_service_horizon_payload_bytes(payload_bytes, mux_limits) as f64;
    let stream_window = usize::try_from(mux_limits.max_stream_window_bytes).unwrap_or(usize::MAX);
    let product_envelope = mux_limits
        .max_path_flight_bytes
        .min(stream_window)
        .min(mux_limits.max_reorder_bytes)
        .max(payload_bytes) as u64;
    ((service_horizon * BBR_DEFAULT_CWND_GAIN).ceil() as u64)
        .min(product_envelope)
        .max(payload_bytes as u64)
}

fn bulk_active_role_has_latency_pressure(candidate: PathSnapshot, role: BulkAdmissionRole) -> bool {
    matches!(
        role,
        BulkAdmissionRole::ActiveDataPath | BulkAdmissionRole::ActiveSingleCarrier
    ) && bulk_latency_pressure_flows(candidate) > 0
}

fn bulk_latency_pressure_flows(candidate: PathSnapshot) -> u32 {
    candidate.active_latency_sensitive_flows
}

fn bulk_product_reorder_debt_bytes(candidate: PathSnapshot) -> u64 {
    if candidate.product_bytes_in_flight > 0 {
        candidate.product_bytes_in_flight
    } else {
        candidate.bytes_in_flight
    }
}

fn bulk_total_reorder_debt_bytes(
    candidate: PathSnapshot,
    role: BulkAdmissionRole,
    stream_ordering_debt_bytes: u64,
) -> u64 {
    let path_debt = match role {
        BulkAdmissionRole::ActiveDataPath | BulkAdmissionRole::ActiveSingleCarrier => 0,
        BulkAdmissionRole::AdditionalSameUnderlay | BulkAdmissionRole::AdditionalCrossUnderlay => {
            bulk_product_reorder_debt_bytes(candidate)
        }
    };
    path_debt.saturating_add(stream_ordering_debt_bytes)
}

fn bulk_payload_tx_ms(snapshot: PathSnapshot, payload_bytes: usize) -> f64 {
    bulk_bytes_tx_ms(snapshot, payload_bytes as u64)
}

fn bulk_bytes_tx_ms(snapshot: PathSnapshot, bytes: u64) -> f64 {
    bytes as f64 * 8.0 / bulk_effective_rate_bps(snapshot) * 1000.0
}

fn bulk_effective_rate_bps(snapshot: PathSnapshot) -> f64 {
    snapshot
        .pacing_rate_bps
        .max(snapshot.delivery_rate_bps)
        .max(1.0)
}

#[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
fn bulk_admission_reorder_budget_bytes(
    candidate: PathSnapshot,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    role: BulkAdmissionRole,
) -> u64 {
    bulk_admission_reorder_budget_bytes_for_ordering_debt(
        candidate,
        payload_bytes,
        mux_limits,
        role,
        0,
    )
}

fn bulk_admission_reorder_budget_bytes_for_ordering_debt(
    candidate: PathSnapshot,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    role: BulkAdmissionRole,
    stream_ordering_debt_bytes: u64,
) -> u64 {
    match role {
        BulkAdmissionRole::ActiveDataPath | BulkAdmissionRole::ActiveSingleCarrier
            if stream_ordering_debt_bytes == 0 =>
        {
            if bulk_active_role_has_latency_pressure(candidate, role) {
                bulk_service_horizon_payload_bytes(payload_bytes, mux_limits) as u64
            } else {
                bulk_active_service_product_envelope_bytes(candidate, payload_bytes, mux_limits)
            }
        }
        BulkAdmissionRole::ActiveDataPath | BulkAdmissionRole::ActiveSingleCarrier => {
            let reorder_budget = bulk_reorder_budget_bytes(candidate, payload_bytes, mux_limits);
            if stream_ordering_debt_bytes > 0
                && bulk_active_role_has_latency_pressure(candidate, role)
            {
                let service_horizon =
                    bulk_service_horizon_payload_bytes(payload_bytes, mux_limits) as u64;
                return reorder_budget.min(service_horizon.max(payload_bytes as u64));
            }
            reorder_budget
        }
        BulkAdmissionRole::AdditionalSameUnderlay => {
            bulk_stream_reorder_envelope_bytes(payload_bytes, mux_limits)
        }
        BulkAdmissionRole::AdditionalCrossUnderlay => {
            bulk_effective_reorder_budget_bytes(candidate, payload_bytes, mux_limits)
        }
    }
}

fn bulk_stream_reorder_envelope_bytes(payload_bytes: usize, mux_limits: MuxLimits) -> u64 {
    (mux_limits.max_reorder_bytes as u64)
        .min(mux_limits.max_stream_window_bytes)
        .max(payload_bytes as u64)
}

fn bulk_same_underlay_product_authority_bytes(
    candidate: PathSnapshot,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> u64 {
    if candidate.underlay != UnderlayProtocol::Udp {
        // A TCP Subflow's BDP is its local pipe allowance, not the budget for
        // all lower ranges concurrently owned by the product stream. Reusing
        // 2*BDP here makes a healthy Service prefix permanently lock out an
        // empty candidate. The inflight gate already bounds this candidate;
        // this gate owns the aggregate stream/receiver reorder resource.
        return bulk_stream_reorder_envelope_bytes(payload_bytes, mux_limits);
    }
    let delivery_rate_inflight_target = bulk_bbr_inflight_bytes(bulk_rate_bdp_bytes(
        candidate.delivery_rate_bps,
        candidate.srtt_ms,
    ));
    let product_progress_budget = match (
        candidate.has_durable_product_progress,
        candidate.product_progress_rate_bps,
    ) {
        (true, Some(product_progress_rate_bps)) => bulk_bbr_inflight_bytes(bulk_rate_bdp_bytes(
            product_progress_rate_bps,
            candidate.srtt_ms,
        )),
        (true, None) => delivery_rate_inflight_target,
        _ if candidate.confidence >= 1.0 => {
            // A high-confidence QUIC carrier may grow cwnd before exact product
            // ACKs reach their sample floor. Its receipt-rate inflight target
            // remains the ordered authority.
            return delivery_rate_inflight_target
                .max(payload_bytes as u64)
                .min(mux_limits.max_reorder_bytes as u64);
        }
        // Low-confidence paths remain inside the separately bounded startup
        // epoch. Use native credit when present, otherwise the delivery-rate
        // target; carrier pacing is never product authority.
        _ => candidate
            .inflight_limit_bytes
            .max(delivery_rate_inflight_target),
    };
    candidate
        .inflight_limit_bytes
        .max(product_progress_budget)
        .max(payload_bytes as u64)
        .min(mux_limits.max_reorder_bytes as u64)
}

fn bulk_effective_reorder_budget_bytes(
    candidate: PathSnapshot,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> u64 {
    let budget = bulk_reorder_budget_bytes(candidate, payload_bytes, mux_limits);
    (budget as f64 * candidate.confidence.clamp(0.0, 1.0)).ceil() as u64
}

fn bulk_reorder_budget_bytes(
    candidate: PathSnapshot,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> u64 {
    let adaptive_budget =
        bulk_bbr_inflight_bytes(bulk_path_bdp_bytes(candidate)).max(payload_bytes as u64);
    adaptive_budget.min(mux_limits.max_reorder_bytes as u64)
}

fn bulk_path_bdp_bytes(candidate: PathSnapshot) -> u64 {
    let rate = bulk_effective_rate_bps(candidate);
    bulk_rate_bdp_bytes(rate, candidate.srtt_ms)
}

pub(in crate::runtime) fn bulk_candidate_pipe_bytes(candidate: PathSnapshot) -> u64 {
    bulk_bbr_inflight_bytes(bulk_path_bdp_bytes(candidate))
}

fn bulk_rate_bdp_bytes(rate_bps: f64, srtt_ms: f64) -> u64 {
    let rate_bps = rate_bps.max(1.0);
    (rate_bps / 8.0 * srtt_ms.max(1.0) / 1000.0).ceil() as u64
}

fn bulk_bbr_inflight_bytes(bdp_bytes: u64) -> u64 {
    ((bdp_bytes as f64) * BBR_DEFAULT_CWND_GAIN).ceil() as u64
}

#[cfg(test)]
mod tests;
