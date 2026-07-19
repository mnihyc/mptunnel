//! Pure admission and completion projections for ordered bulk product data.

#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::capacity::{
    MAX_RELIABLE_SERVICE_QUANTUM_BYTES, RELIABLE_PIPE_WINDOW_BDPS,
    reliable_unproven_path_startup_flight_limit_bytes,
};
use crate::model::path::RelayPathKey;
use crate::mux::MuxLimits;
use crate::scheduler::{PathSnapshot, TrafficClass, path_is_backup};
use smallvec::SmallVec;

// Decisions in this module never mutate a path or enqueue carrier work.

#[derive(Debug, Clone, Copy)]
pub(crate) struct BulkPathCandidate {
    pub(crate) key: RelayPathKey,
    pub(crate) eta_ms: f64,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(crate) has_liveness_evidence: bool,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(crate) has_path_proof_evidence: bool,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(crate) has_ack_data_evidence: bool,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(crate) has_bulk_rate_evidence: bool,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(crate) has_sender_delivery_evidence: bool,
    pub(crate) snapshot: PathSnapshot,
}

pub(crate) fn bulk_candidate_has_active_bulk_work(candidate: &BulkPathCandidate) -> bool {
    candidate.snapshot.active_flows > candidate.snapshot.active_latency_sensitive_flows
}

fn bulk_candidate_is_validated_path(candidate: &BulkPathCandidate) -> bool {
    candidate.has_liveness_evidence && candidate.has_path_proof_evidence
}

pub(crate) fn bulk_striping_admitted_candidates(
    candidates: impl IntoIterator<Item = BulkPathCandidate>,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    compare_keys: impl Fn(RelayPathKey, RelayPathKey) -> std::cmp::Ordering,
) -> SmallVec<[BulkPathCandidate; 4]> {
    let mut candidates = candidates
        .into_iter()
        .collect::<SmallVec<[BulkPathCandidate; 4]>>();
    if candidates
        .iter()
        .any(|candidate| !path_is_backup(candidate.snapshot))
    {
        candidates.retain(|candidate| !path_is_backup(candidate.snapshot));
    }
    let has_bulk_rate_evidence = candidates
        .iter()
        .any(|candidate| candidate.has_bulk_rate_evidence);
    let has_active_bulk_work = candidates.iter().any(bulk_candidate_has_active_bulk_work);
    if has_bulk_rate_evidence {
        // Path validation and native TCP/QUIC send credit are enough to carry
        // one bounded startup flight. Delivery rate ranks later work; it is
        // not a second availability handshake.
        candidates.retain(|candidate| {
            candidate.has_bulk_rate_evidence || bulk_candidate_is_validated_path(candidate)
        });
    } else if has_active_bulk_work {
        candidates.retain(|candidate| {
            bulk_candidate_has_active_bulk_work(candidate)
                || bulk_candidate_is_validated_path(candidate)
        });
    }
    candidates.sort_by(|left, right| {
        path_is_backup(left.snapshot)
            .cmp(&path_is_backup(right.snapshot))
            .then_with(|| left.eta_ms.total_cmp(&right.eta_ms))
            .then_with(|| compare_keys(left.key, right.key))
    });
    if !has_bulk_rate_evidence
        && !has_active_bulk_work
        && !candidates.iter().any(bulk_candidate_is_validated_path)
    {
        candidates.truncate(1);
    }
    bulk_striping_admitted_paths(candidates, payload_bytes, mux_limits)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BulkCandidatePosition {
    FirstPath,
    AdditionalPath,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct BulkAdmissionCheck {
    pub(crate) best_snapshot: PathSnapshot,
    pub(crate) best_eta_ms: f64,
    pub(crate) candidate_snapshot: PathSnapshot,
    pub(crate) candidate_eta_ms: f64,
    pub(crate) payload_bytes: usize,
    pub(crate) mux_limits: MuxLimits,
    pub(crate) position: BulkCandidatePosition,
    // Lower unique bytes on other owners are completion/HOL debt. The
    // candidate-local pipe is bounded independently by the inflight gate.
    pub(crate) stream_ordering_debt_bytes: u64,
}

pub(crate) fn bulk_striping_admitted_paths(
    candidates: impl IntoIterator<Item = BulkPathCandidate>,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> SmallVec<[BulkPathCandidate; 4]> {
    let mut candidates = candidates.into_iter();
    let Some(best) = candidates.next() else {
        return SmallVec::new();
    };
    let mut selected = SmallVec::new();
    for candidate in std::iter::once(best).chain(candidates) {
        let position = if selected.is_empty() {
            BulkCandidatePosition::FirstPath
        } else {
            BulkCandidatePosition::AdditionalPath
        };
        let check = BulkAdmissionCheck {
            best_snapshot: best.snapshot,
            best_eta_ms: best.eta_ms,
            candidate_snapshot: candidate.snapshot,
            candidate_eta_ms: candidate.eta_ms,
            payload_bytes,
            mux_limits,
            position,
            stream_ordering_debt_bytes: 0,
        };
        // Global discovery has no stream backlog. Defer ECF to the directional
        // sender, which owns the exact lower-offset tail for every carrier.
        let suppression =
            bulk_candidate_resource_suppression(check, candidate.has_bulk_rate_evidence);
        if suppression.is_none() {
            selected.push(candidate);
        } else {
            #[cfg(feature = "lab-diagnostics")]
            let reason = suppression.unwrap_or("suppressed");
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "bulk_striping_candidate_suppressed",
                format_args!(
                    "path_underlay={:?} path_index={} position={:?} eta_ms={:.3} best_eta_ms={:.3} horizon_ms={:.3} best_sender_evidence={} candidate_sender_evidence={} best_confidence={:.3} candidate_confidence={:.3} best_app_limited={} candidate_app_limited={} data_level_bytes_in_flight={} carrier_bytes_in_flight={} carrier_inflight_limit={} product_inflight_limit={} scheduler_debt={} queue_bytes={} reorder_budget={} reason={}",
                    candidate.key.underlay,
                    candidate.key.index,
                    position,
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
                    candidate.snapshot.carrier_inflight_limit_bytes,
                    bulk_product_inflight_limit_bytes(
                        candidate.snapshot,
                        payload_bytes,
                        mux_limits,
                        position,
                        bulk_path_has_completion_evidence(
                            candidate.snapshot,
                            candidate.has_bulk_rate_evidence,
                        ),
                    ),
                    bulk_scheduler_inflight_debt_bytes(candidate.snapshot, position),
                    candidate.snapshot.queue_bytes,
                    bulk_admission_reorder_budget_bytes(
                        candidate.snapshot,
                        payload_bytes,
                        mux_limits,
                        position,
                    ),
                    reason,
                ),
            );
        }
    }
    selected
}

pub(crate) fn bulk_scheduling_horizon_bytes(payload_bytes: usize, mux_limits: MuxLimits) -> usize {
    let base_payload = payload_bytes
        .max(MAX_RELIABLE_SERVICE_QUANTUM_BYTES.min(mux_limits.max_reliable_relay_chunk_bytes))
        .max(1);
    let envelope = bulk_reorder_window_bytes(base_payload, mux_limits);
    let horizon = ((base_payload as f64) * (envelope as f64)).sqrt().round() as usize;
    horizon.clamp(base_payload, envelope)
}

pub(crate) fn bulk_reorder_window_bytes(payload_bytes: usize, mux_limits: MuxLimits) -> usize {
    let base_payload = payload_bytes
        .max(MAX_RELIABLE_SERVICE_QUANTUM_BYTES.min(mux_limits.max_reliable_relay_chunk_bytes))
        .max(1);
    let stream_window = usize::try_from(mux_limits.max_stream_window_bytes).unwrap_or(usize::MAX);
    mux_limits
        .max_path_flight_bytes
        .min(mux_limits.max_reorder_bytes)
        .min(stream_window)
        .max(base_payload)
        .max(1)
}

pub(crate) fn bulk_scheduling_window_bytes(payload_bytes: usize, mux_limits: MuxLimits) -> usize {
    let horizon = bulk_scheduling_horizon_bytes(payload_bytes, mux_limits);
    let envelope = bulk_reorder_window_bytes(payload_bytes, mux_limits);
    ((horizon as f64) * RELIABLE_PIPE_WINDOW_BDPS)
        .ceil()
        .clamp(horizon as f64, envelope as f64) as usize
}

/// Bounds bytes read from the source before they receive a data sequence.
///
/// Unassigned bytes are connection work regardless of the eventual TCP/QUIC
/// path. Exact Data-ACK outstanding bytes and queued source bytes therefore
/// consume one shared reorder/receive-window envelope.
pub(crate) fn reliable_relay_source_staging_headroom(
    lane: TrafficClass,
    data_ack_outstanding_bytes: usize,
    queued_data_bytes: usize,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> usize {
    if !lane.is_bulk() {
        return usize::MAX;
    }
    let envelope = bulk_reorder_window_bytes(payload_bytes, mux_limits);
    envelope.saturating_sub(data_ack_outstanding_bytes.saturating_add(queued_data_bytes))
}

#[cfg(test)]
pub(crate) fn bulk_candidate_admission_suppression(
    best_snapshot: PathSnapshot,
    best_eta_ms: f64,
    candidate_snapshot: PathSnapshot,
    candidate_eta_ms: f64,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    position: BulkCandidatePosition,
) -> Option<&'static str> {
    bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
        best_snapshot,
        best_eta_ms,
        candidate_snapshot,
        candidate_eta_ms,
        payload_bytes,
        mux_limits,
        position,
        stream_ordering_debt_bytes: 0,
    })
}

pub(crate) fn bulk_candidate_admission_suppression_with_ordering_debt(
    check: BulkAdmissionCheck,
) -> Option<&'static str> {
    bulk_candidate_admission_suppression_with_completion_backlog(check, 0, true)
}

pub(crate) fn bulk_candidate_admission_suppression_with_completion_backlog(
    check: BulkAdmissionCheck,
    completion_backlog_bytes: u64,
    has_bulk_rate_evidence: bool,
) -> Option<&'static str> {
    let has_qualified_bulk_rate =
        bulk_path_has_completion_evidence(check.candidate_snapshot, has_bulk_rate_evidence);
    if let Some(reason) = bulk_candidate_resource_suppression(check, has_qualified_bulk_rate) {
        return Some(reason);
    }
    // Ordering debt bounds receive-hole resources; it does not prove that the
    // leading path has an independent lower backlog. Response policy supplies
    // that product backlog explicitly when it owns one.
    if let Some(reason) = bulk_additional_path_completion_suppression(
        check,
        completion_backlog_bytes,
        has_qualified_bulk_rate,
    ) {
        return Some(reason);
    }
    None
}

fn bulk_candidate_resource_suppression(
    check: BulkAdmissionCheck,
    has_qualified_bulk_rate: bool,
) -> Option<&'static str> {
    if !bulk_candidate_within_inflight_limit(check, has_qualified_bulk_rate) {
        return Some("inflight_limit");
    }
    if !bulk_candidate_within_reorder_budget(
        check.candidate_snapshot,
        check.payload_bytes,
        check.mux_limits,
        check.position,
        check.stream_ordering_debt_bytes,
        has_qualified_bulk_rate,
    ) {
        return Some("reorder_budget");
    }
    // Once another path owns lower sequence bytes, ECF protects that frontier:
    // path exploration is not authority to extend an ordered receive hole.
    if bulk_completion_horizon_applies(check)
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

fn bulk_additional_path_completion_suppression(
    check: BulkAdmissionCheck,
    completion_backlog_bytes: u64,
    has_qualified_bulk_rate: bool,
) -> Option<&'static str> {
    if check.position != BulkCandidatePosition::AdditionalPath {
        return None;
    }
    if !has_qualified_bulk_rate {
        return None;
    }
    // For a full bulk backlog, compare the candidate with the leading path
    // draining the lower ordered tail, not only with its next quantum. This is the ECF
    // completion boundary: a candidate that finishes before those earlier bytes
    // adds capacity without extending the receive hole. A clear/small frontier
    // retains the strict next-quantum rule.
    // The leading ETA already includes its command queue and, for QUIC, native
    // carrier flight. Subtract those overlapping views from the product tail
    // before adding backlog transmission time.
    let lead_modeled_debt_bytes = check
        .best_snapshot
        .queue_bytes
        .saturating_add(check.best_snapshot.data_level_queue_bytes)
        .saturating_add(check.best_snapshot.bytes_in_flight);
    let reference_followup_bytes = completion_backlog_bytes
        .saturating_sub(lead_modeled_debt_bytes)
        .max(check.payload_bytes as u64);
    let lead_completion_eta_ms =
        check.best_eta_ms + bulk_bytes_tx_ms(check.best_snapshot, reference_followup_bytes);
    if check.candidate_eta_ms > lead_completion_eta_ms {
        return Some("ecf_no_completion_gain");
    }
    None
}

fn bulk_path_has_completion_evidence(
    candidate: PathSnapshot,
    has_bulk_rate_evidence: bool,
) -> bool {
    // An idle native carrier's rate is a lower bound while send-window credit
    // remains available; let that credit reacquire bulk service instead of
    // feeding scheduler-induced idleness back into ECF. Portable paths and
    // carriers under latency pressure keep the conservative boundary.
    let acquiring_native_capacity = bulk_path_acquiring_native_capacity(candidate);
    has_bulk_rate_evidence && candidate.confidence >= 1.0 && !acquiring_native_capacity
}

fn bulk_path_acquiring_native_capacity(candidate: PathSnapshot) -> bool {
    candidate.app_limited
        && candidate.carrier_inflight_limit_bytes > 0
        && !bulk_path_has_latency_pressure(candidate)
}

fn bulk_completion_horizon_applies(check: BulkAdmissionCheck) -> bool {
    // Additional carriers already use ECF against the sender-owned completion
    // backlog. Only the leading owner can need a separate lower-offset horizon.
    check.position == BulkCandidatePosition::FirstPath && check.stream_ordering_debt_bytes > 0
}

#[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
pub(crate) fn bulk_completion_horizon_ms(
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

pub(crate) fn bulk_completion_horizon_ms_with_ordering_debt(
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

fn bulk_candidate_within_inflight_limit(
    check: BulkAdmissionCheck,
    has_qualified_bulk_rate: bool,
) -> bool {
    let candidate = check.candidate_snapshot;
    let payload_bytes = check.payload_bytes;
    let mux_limits = check.mux_limits;
    let position = check.position;
    if bulk_path_has_latency_pressure(candidate) {
        let inflight_limit = bulk_product_inflight_limit_bytes(
            candidate,
            payload_bytes,
            mux_limits,
            position,
            has_qualified_bulk_rate,
        );
        let committed = bulk_scheduler_inflight_debt_bytes(candidate, position);
        return committed.saturating_add(payload_bytes as u64) <= inflight_limit
            || committed < inflight_limit;
    }

    // Native TCP or QUIC credit governs packet emission. This independent
    // product-service window bounds how much ordered Data Sequence work MPP
    // may assign before product ACKs prove that the carrier drained it.
    let inflight_limit = bulk_product_inflight_limit_bytes(
        candidate,
        payload_bytes,
        mux_limits,
        position,
        has_qualified_bulk_rate,
    );
    let committed = bulk_scheduler_inflight_debt_bytes(candidate, position);
    bulk_quantum_granular_limit_allows(committed, payload_bytes, inflight_limit, position)
}

fn bulk_quantum_granular_limit_allows(
    committed: u64,
    payload_bytes: usize,
    limit: u64,
    position: BulkCandidatePosition,
) -> bool {
    if limit == 0 {
        return true;
    }
    let payload_bytes = payload_bytes as u64;
    if committed.saturating_add(payload_bytes) <= limit {
        return true;
    }
    position == BulkCandidatePosition::AdditionalPath && committed < limit
}

fn bulk_product_inflight_limit_bytes(
    candidate: PathSnapshot,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    _position: BulkCandidatePosition,
    has_qualified_bulk_rate: bool,
) -> u64 {
    let configured_ceiling = mux_limits.max_path_flight_bytes as u64;
    let payload_floor = payload_bytes as u64;
    let bdp = if bulk_path_has_latency_pressure(candidate) {
        bulk_latency_pressure_bdp_bytes(candidate)
    } else {
        bulk_path_bdp_bytes(candidate)
    };
    let bdp_limit = bulk_pipe_window_bytes(bdp).max(payload_floor);
    let modeled_limit = if candidate.data_level_limit_bytes > 0 {
        // The runtime capacity model has already bounded this product-layer
        // service window by measured delivery and native carrier flight. Do
        // not collapse its feed headroom back to the cold delivery-rate BDP.
        candidate.data_level_limit_bytes.max(payload_floor)
    } else {
        bdp_limit
    };
    let modeled_limit = if !has_qualified_bulk_rate {
        // Positive Data ACK attribution proves liveness, not capacity. Let the
        // native congestion window acquire a complete rate sample; a cold path
        // without native state retains only the bounded startup window.
        modeled_limit.max(bulk_unproven_path_flight_limit_bytes(candidate, mux_limits))
    } else {
        modeled_limit
    }
    .min(configured_ceiling.max(payload_floor));
    if bulk_path_has_latency_pressure(candidate) {
        let latency_limit = if has_qualified_bulk_rate {
            bulk_latency_pressure_bdp_bytes(candidate)
        } else {
            reliable_unproven_path_startup_flight_limit_bytes(mux_limits)
        }
        .max(payload_floor)
        .min(configured_ceiling.max(payload_floor));
        return modeled_limit.min(latency_limit);
    }
    modeled_limit
}

fn bulk_candidate_within_reorder_budget(
    candidate: PathSnapshot,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    position: BulkCandidatePosition,
    stream_ordering_debt_bytes: u64,
    has_qualified_bulk_rate: bool,
) -> bool {
    if position == BulkCandidatePosition::AdditionalPath {
        // Native carrier congestion windows bound each path below MPP. The
        // product service window may be larger than an app-limited rate BDP;
        // clamping it again here would prevent TCP/QUIC from growing that pipe.
        // The connection-wide envelope still bounds the complete receive hole.
        let candidate_product_debt = bulk_product_reorder_debt_bytes(candidate);
        let measured_budget = bulk_reorder_budget_bytes(candidate, payload_bytes, mux_limits);
        let acquisition_budget = if has_qualified_bulk_rate {
            candidate.data_level_limit_bytes
        } else {
            // QUIC/TCP congestion credit bounds Startup below MPP. Keeping the
            // product window in step lets that native controller finish its
            // capacity search without granting completion-time authority.
            candidate
                .data_level_limit_bytes
                .max(bulk_unproven_path_flight_limit_bytes(candidate, mux_limits))
        };
        let path_budget = measured_budget
            .max(acquisition_budget)
            .min(mux_limits.max_path_flight_bytes as u64)
            .max(payload_bytes as u64);
        let within_path_budget = bulk_quantum_granular_limit_allows(
            candidate_product_debt,
            payload_bytes,
            path_budget,
            position,
        );
        let within_stream_envelope = bulk_quantum_granular_limit_allows(
            candidate_product_debt.saturating_add(stream_ordering_debt_bytes),
            payload_bytes,
            bulk_stream_reorder_envelope_bytes(payload_bytes, mux_limits),
            position,
        );
        return within_path_budget && within_stream_envelope;
    }
    let admission_budget = bulk_admission_reorder_budget_bytes_for_ordering_debt(
        candidate,
        payload_bytes,
        mux_limits,
        position,
        stream_ordering_debt_bytes,
    );
    bulk_quantum_granular_limit_allows(
        bulk_total_reorder_debt_bytes(candidate, position, stream_ordering_debt_bytes),
        payload_bytes,
        admission_budget,
        position,
    )
}

fn bulk_unproven_path_flight_limit_bytes(candidate: PathSnapshot, mux_limits: MuxLimits) -> u64 {
    candidate
        .carrier_inflight_limit_bytes
        .max(reliable_unproven_path_startup_flight_limit_bytes(
            mux_limits,
        ))
        .min(mux_limits.max_path_flight_bytes as u64)
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
        BulkCandidatePosition::AdditionalPath,
        stream_ordering_debt_bytes,
    )
    .saturating_add(payload_bytes as u64);
    let remaining = budget.saturating_sub(committed);
    remaining as f64 * 8.0 / bulk_effective_rate_bps(best) * 1000.0
}

fn bulk_scheduler_inflight_debt_bytes(
    candidate: PathSnapshot,
    position: BulkCandidatePosition,
) -> u64 {
    if position == BulkCandidatePosition::FirstPath {
        return bulk_assigned_product_debt_bytes(candidate);
    }
    bulk_product_reorder_debt_bytes(candidate)
}

fn bulk_assigned_product_debt_bytes(candidate: PathSnapshot) -> u64 {
    // Product flight owns accepted OriginalData from carrier enqueue through
    // STREAM_ACK, so the carrier queue is an overlapping pressure view rather
    // than additional product debt.
    candidate
        .data_level_bytes_in_flight
        .max(candidate.queue_bytes)
}

fn bulk_first_path_has_latency_pressure(
    candidate: PathSnapshot,
    position: BulkCandidatePosition,
) -> bool {
    position == BulkCandidatePosition::FirstPath && bulk_path_has_latency_pressure(candidate)
}

fn bulk_path_has_latency_pressure(candidate: PathSnapshot) -> bool {
    bulk_latency_pressure_flows(candidate) > 0
}

fn bulk_latency_pressure_bdp_bytes(candidate: PathSnapshot) -> u64 {
    let rate = candidate
        .carrier_delivery_rate_bps
        .filter(|rate| rate.is_finite() && *rate > 0.0)
        .unwrap_or_else(|| bulk_effective_rate_bps(candidate));
    bulk_rate_bdp_bytes(rate, candidate.srtt_ms)
}

fn bulk_latency_pressure_flows(candidate: PathSnapshot) -> u32 {
    candidate.active_latency_sensitive_flows
}

fn bulk_product_reorder_debt_bytes(candidate: PathSnapshot) -> u64 {
    if candidate.data_level_bytes_in_flight > 0 {
        candidate.data_level_bytes_in_flight
    } else {
        candidate.bytes_in_flight
    }
}

fn bulk_total_reorder_debt_bytes(
    candidate: PathSnapshot,
    position: BulkCandidatePosition,
    stream_ordering_debt_bytes: u64,
) -> u64 {
    let path_debt = match position {
        BulkCandidatePosition::FirstPath => 0,
        BulkCandidatePosition::AdditionalPath => bulk_product_reorder_debt_bytes(candidate),
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
        .delivery_rate_bps
        .max(snapshot.product_progress_rate_bps.unwrap_or(0.0))
        .max(1.0)
}

#[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
fn bulk_admission_reorder_budget_bytes(
    candidate: PathSnapshot,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    position: BulkCandidatePosition,
) -> u64 {
    bulk_admission_reorder_budget_bytes_for_ordering_debt(
        candidate,
        payload_bytes,
        mux_limits,
        position,
        0,
    )
}

fn bulk_admission_reorder_budget_bytes_for_ordering_debt(
    candidate: PathSnapshot,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    position: BulkCandidatePosition,
    stream_ordering_debt_bytes: u64,
) -> u64 {
    match position {
        BulkCandidatePosition::FirstPath if stream_ordering_debt_bytes == 0 => {
            if bulk_first_path_has_latency_pressure(candidate, position) {
                bulk_scheduling_horizon_bytes(payload_bytes, mux_limits) as u64
            } else {
                bulk_reorder_window_bytes(payload_bytes, mux_limits) as u64
            }
        }
        BulkCandidatePosition::FirstPath => {
            let reorder_budget = bulk_reorder_budget_bytes(candidate, payload_bytes, mux_limits);
            if stream_ordering_debt_bytes > 0
                && bulk_first_path_has_latency_pressure(candidate, position)
            {
                let service_horizon =
                    bulk_scheduling_horizon_bytes(payload_bytes, mux_limits) as u64;
                return reorder_budget.min(service_horizon.max(payload_bytes as u64));
            }
            reorder_budget
        }
        BulkCandidatePosition::AdditionalPath => {
            bulk_stream_reorder_envelope_bytes(payload_bytes, mux_limits)
        }
    }
}

fn bulk_stream_reorder_envelope_bytes(payload_bytes: usize, mux_limits: MuxLimits) -> u64 {
    (mux_limits.max_reorder_bytes as u64)
        .min(mux_limits.max_stream_window_bytes)
        .max(payload_bytes as u64)
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
        bulk_pipe_window_bytes(bulk_path_bdp_bytes(candidate)).max(payload_bytes as u64);
    adaptive_budget.min(mux_limits.max_reorder_bytes as u64)
}

fn bulk_path_bdp_bytes(candidate: PathSnapshot) -> u64 {
    let rate = bulk_effective_rate_bps(candidate);
    bulk_rate_bdp_bytes(rate, candidate.srtt_ms)
}

pub(crate) fn bulk_candidate_pipe_bytes(candidate: PathSnapshot) -> u64 {
    bulk_pipe_window_bytes(bulk_path_bdp_bytes(candidate))
}

fn bulk_rate_bdp_bytes(rate_bps: f64, srtt_ms: f64) -> u64 {
    let rate_bps = rate_bps.max(1.0);
    (rate_bps / 8.0 * srtt_ms.max(1.0) / 1000.0).ceil() as u64
}

fn bulk_pipe_window_bytes(bdp_bytes: u64) -> u64 {
    ((bdp_bytes as f64) * RELIABLE_PIPE_WINDOW_BDPS).ceil() as u64
}

#[cfg(test)]
#[path = "admission_test.rs"]
mod tests;
