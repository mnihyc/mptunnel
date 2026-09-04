//! Pure admission and completion projections for ordered bulk product data.

#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::capacity::{
    MAX_RELIABLE_SERVICE_QUANTUM_BYTES, RELIABLE_PIPE_WINDOW_BDPS, reliable_bulk_product_windows,
    reliable_bulk_unproven_exploration_limit_bytes,
};
use crate::model::path::RelayPathKey;
use crate::mux::MuxLimits;
use crate::scheduler::{PathSnapshot, path_is_backup};
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
    /// First-ranked candidate when no exact lower Data Sequence owner exists.
    FirstPath,
    /// Exact owner of the lowest outstanding original Data Sequence range.
    ContiguousFrontier,
    AdditionalPath,
}

/// Independent provenance carried into one bulk admission decision.
///
/// Product qualification controls only `P` versus `E`. A fresh achieved-rate
/// sample controls only completion comparison. Neither authority is inferred
/// from the snapshot's generic confidence scalar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BulkAdmissionEvidence {
    pub(crate) product_assignment_qualified: bool,
    pub(crate) fresh_completion_rate: bool,
}

/// Exact bulk OriginalData authority for one current output observation.
///
/// `P` is the configured Product envelope as published by the exact output.
/// `E` is the bounded acquisition envelope for an unqualified additional
/// output. `A` is the effective assignment envelope after position and
/// evidence are applied. Traffic-class arbitration and native writer state may
/// protect latency, but sampled carrier rate never rewrites Product authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BulkOriginalDataAssignmentAuthority {
    pub(crate) product_limit_bytes: u64,
    pub(crate) exploration_limit_bytes: u64,
    pub(crate) assignment_limit_bytes: u64,
    /// Exact pending OriginalData quantum covered by this authority snapshot.
    pub(crate) assignment_payload_bytes: u64,
}

impl BulkOriginalDataAssignmentAuthority {
    pub(crate) fn has_headroom(self, assigned_product_bytes: u64) -> bool {
        self.assignment_limit_bytes > 0
            && assigned_product_bytes
                .checked_add(self.assignment_payload_bytes)
                .is_some_and(|committed| committed <= self.assignment_limit_bytes)
    }
}

pub(crate) fn bulk_original_data_assignment_authority(
    candidate: PathSnapshot,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    position: BulkCandidatePosition,
    product_assignment_qualified: bool,
) -> BulkOriginalDataAssignmentAuthority {
    let configured_product_limit =
        reliable_bulk_product_windows(mux_limits).per_output_product_limit_bytes;
    // Zero is an exact negative publication at assignment/apply time. A global
    // planning projection is installed separately by discovery above.
    let product_limit_bytes = candidate
        .data_level_limit_bytes
        .min(configured_product_limit);
    let exploration_limit_bytes =
        reliable_bulk_unproven_exploration_limit_bytes(candidate, mux_limits)
            .min(product_limit_bytes);
    let base_limit =
        if position == BulkCandidatePosition::AdditionalPath && !product_assignment_qualified {
            exploration_limit_bytes
        } else {
            product_limit_bytes
        };
    let assignment_limit_bytes = base_limit;
    BulkOriginalDataAssignmentAuthority {
        product_limit_bytes,
        exploration_limit_bytes,
        assignment_limit_bytes,
        assignment_payload_bytes: payload_bytes as u64,
    }
}

/// Receiver knowledge that determines whether the exact lowest-range owner
/// still owns a live contiguous frontier.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReliableDataAckFrontierState {
    /// No complete sparse Data ACK proves that the lowest outstanding range is
    /// missing. The owner remains work-conserving while both Product and
    /// native admission authorities have headroom.
    #[default]
    Live,
    /// A complete retained Data ACK omits the lowest outstanding range. Exact
    /// ownership remains authoritative for ordering and recovery, but fresh
    /// originals must use the ordinary Product service window.
    AuthoritativeGap,
}

impl ReliableDataAckFrontierState {
    pub(crate) fn from_authoritative_gap(authoritative_gap: bool) -> Self {
        if authoritative_gap {
            Self::AuthoritativeGap
        } else {
            Self::Live
        }
    }

    pub(crate) fn owner_has_live_contiguous_frontier(self) -> bool {
        self == Self::Live
    }
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

/// Exact/configured inputs allowed to decide ordinary Product admission.
///
/// Completion estimates are absent by construction. `candidate_snapshot` is
/// retained because the current published `P/E` authority is projected there;
/// this predicate reads only its Product resource fields.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BulkProductResourceCheck {
    pub(crate) candidate_snapshot: PathSnapshot,
    pub(crate) payload_bytes: usize,
    pub(crate) mux_limits: MuxLimits,
    pub(crate) position: BulkCandidatePosition,
    pub(crate) stream_ordering_debt_bytes: u64,
    pub(crate) product_assignment_qualified: bool,
}

impl BulkAdmissionCheck {
    fn product_resource_check(
        self,
        product_assignment_qualified: bool,
    ) -> BulkProductResourceCheck {
        BulkProductResourceCheck {
            candidate_snapshot: self.candidate_snapshot,
            payload_bytes: self.payload_bytes,
            mux_limits: self.mux_limits,
            position: self.position,
            stream_ordering_debt_bytes: self.stream_ordering_debt_bytes,
            product_assignment_qualified,
        }
    }
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
        let check = BulkProductResourceCheck {
            candidate_snapshot: candidate.snapshot,
            payload_bytes,
            mux_limits,
            position,
            stream_ordering_debt_bytes: 0,
            product_assignment_qualified: false,
        };
        // Global discovery has no exact stream backlog. The directional
        // sender rechecks this resource predicate with its exact lower-offset
        // ownership ledger before committing Product work.
        // Global discovery has no exact flow-local Product evidence. It may
        // rank a measured carrier, but cannot promote an additional output
        // from acquisition `E` to Product assignment `P`.
        let suppression = bulk_product_candidate_resource_suppression(check);
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
                        false,
                    ),
                    bulk_scheduler_inflight_debt_bytes(candidate.snapshot, position),
                    candidate.snapshot.queue_bytes,
                    reliable_bulk_product_windows(mux_limits).stream_resource_limit_bytes,
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

#[cfg(test)]
pub(crate) fn bulk_candidate_admission_suppression_with_ordering_debt(
    check: BulkAdmissionCheck,
) -> Option<&'static str> {
    bulk_product_candidate_resource_suppression(check.product_resource_check(true))
}

/// Optional ACK-clock measurement-start policy.
///
/// Unlike ordinary Product admission, an optional measurement may decline to
/// annotate the already-selected data quantum when its inferred completion
/// would extend the current lower tail. The caller must retain the ordinary
/// Product action when this returns a reason.
pub(crate) fn bulk_measurement_start_suppression_with_completion_backlog(
    check: BulkAdmissionCheck,
    completion_backlog_bytes: u64,
    evidence: BulkAdmissionEvidence,
) -> Option<&'static str> {
    if let Some(reason) = bulk_product_candidate_resource_suppression(
        check.product_resource_check(evidence.product_assignment_qualified),
    ) {
        return Some(reason);
    }
    // Ordering debt bounds receive-hole resources; it does not prove that the
    // leading path has an independent lower backlog. Response policy supplies
    // that product backlog explicitly when it owns one.
    if let Some(reason) = bulk_additional_path_completion_suppression(
        check,
        completion_backlog_bytes,
        evidence.fresh_completion_rate,
    ) {
        return Some(reason);
    }
    None
}

fn bulk_candidate_within_stream_product_resource(
    candidate: PathSnapshot,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    stream_ordering_debt_bytes: u64,
) -> bool {
    let committed = bulk_product_reorder_debt_bytes(candidate)
        .checked_add(stream_ordering_debt_bytes)
        .and_then(|debt| debt.checked_add(payload_bytes as u64));
    committed.is_some_and(|committed| {
        committed <= reliable_bulk_product_windows(mux_limits).stream_resource_limit_bytes
    })
}

/// Structural/configured resource admission for one ordinary Product action.
///
/// Numeric completion inference is deliberately absent. It may rank this
/// action, but only exact `W/P/E` authority may reject its Product extent.
pub(crate) fn bulk_product_candidate_resource_suppression(
    check: BulkProductResourceCheck,
) -> Option<&'static str> {
    if !bulk_candidate_within_original_data_assignment_windows(check) {
        return Some("inflight_limit");
    }
    if !bulk_candidate_within_stream_product_resource(
        check.candidate_snapshot,
        check.payload_bytes,
        check.mux_limits,
        check.stream_ordering_debt_bytes,
    ) {
        return Some("reorder_budget");
    }
    None
}

fn bulk_additional_path_completion_suppression(
    check: BulkAdmissionCheck,
    completion_backlog_bytes: u64,
    fresh_completion_rate: bool,
) -> Option<&'static str> {
    if check.position != BulkCandidatePosition::AdditionalPath {
        return None;
    }
    if !fresh_completion_rate {
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

fn bulk_candidate_within_original_data_assignment_windows(check: BulkProductResourceCheck) -> bool {
    let authority = bulk_original_data_assignment_authority(
        check.candidate_snapshot,
        check.payload_bytes,
        check.mux_limits,
        check.position,
        check.product_assignment_qualified,
    );
    authority.has_headroom(bulk_scheduler_inflight_debt_bytes(
        check.candidate_snapshot,
        check.position,
    ))
}

/// Whether one exact output retains Product authority for another OriginalData
/// assignment. The assignment may overshoot by the already-sized command
/// quantum; the next decision observes the incremented debt and stops.
///
/// Native packet flight is intentionally absent. This is Product authority
/// only.
pub(crate) fn original_data_assignment_has_product_headroom(candidate: PathSnapshot) -> bool {
    candidate.data_level_limit_bytes > 0
        && candidate.data_level_bytes_in_flight < candidate.data_level_limit_bytes
}

/// Whether the exact contiguous-frontier carrier can accept another original
/// data quantum without installing a second congestion controller.
///
/// Every underlay independently requires unique Product debt below `P`.
pub(crate) fn bulk_contiguous_frontier_can_accept_enqueue(
    candidate: PathSnapshot,
    _payload_bytes: usize,
    _mux_limits: MuxLimits,
) -> bool {
    original_data_assignment_has_product_headroom(candidate)
}

#[cfg_attr(not(any(test, feature = "lab-diagnostics")), allow(dead_code))]
fn bulk_product_inflight_limit_bytes(
    candidate: PathSnapshot,
    payload_bytes: usize,
    mux_limits: MuxLimits,
    position: BulkCandidatePosition,
    product_assignment_qualified: bool,
) -> u64 {
    bulk_original_data_assignment_authority(
        candidate,
        payload_bytes,
        mux_limits,
        position,
        product_assignment_qualified,
    )
    .assignment_limit_bytes
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
    _position: BulkCandidatePosition,
) -> u64 {
    bulk_assigned_product_debt_bytes(candidate)
}

fn bulk_assigned_product_debt_bytes(candidate: PathSnapshot) -> u64 {
    // This is exact per-flow unique data awaiting a Product Data ACK. The
    // carrier queue is shared native state (and can contain other flows); it
    // remains geometry/diagnostics while actual writer reservation owns native
    // admission.
    candidate.data_level_bytes_in_flight
}

fn bulk_product_reorder_debt_bytes(candidate: PathSnapshot) -> u64 {
    // Reorder debt is unique assigned Product data awaiting a Data ACK.
    // Native packet flight is a separate, renewable transport view and must
    // never stand in for Product debt when the exact Product value is zero.
    candidate.data_level_bytes_in_flight
}

fn bulk_total_reorder_debt_bytes(
    candidate: PathSnapshot,
    position: BulkCandidatePosition,
    stream_ordering_debt_bytes: u64,
) -> u64 {
    let path_debt = match position {
        BulkCandidatePosition::FirstPath | BulkCandidatePosition::ContiguousFrontier => 0,
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

#[cfg(test)]
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
#[path = "tests_admission.rs"]
mod tests;
