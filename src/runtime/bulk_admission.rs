use super::prelude::*;
use super::relay_open::RelayPathKey;
use super::{BBR_DEFAULT_CWND_GAIN, RELIABLE_UDP_INITIAL_PRODUCT_WINDOW_BYTES};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;

#[derive(Debug, Clone, Copy)]
pub(super) struct BulkPathCandidate {
    pub(super) key: RelayPathKey,
    pub(super) eta_ms: f64,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(super) has_liveness_evidence: bool,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(super) has_path_proof_evidence: bool,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(super) has_ack_data_evidence: bool,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(super) has_bulk_rate_evidence: bool,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(super) has_sender_delivery_evidence: bool,
    pub(super) snapshot: PathSnapshot,
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BulkAdmissionRole {
    ActiveDataPath,
    ActiveSingleCarrier,
    AdditionalSameUnderlay,
    AdditionalCrossUnderlay,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct BulkAdmissionCheck {
    pub(super) best_snapshot: PathSnapshot,
    pub(super) best_eta_ms: f64,
    pub(super) candidate_snapshot: PathSnapshot,
    pub(super) candidate_eta_ms: f64,
    pub(super) payload_bytes: usize,
    pub(super) mux_limits: MuxLimits,
    pub(super) role: BulkAdmissionRole,
    pub(super) stream_ordering_debt_bytes: u64,
}

pub(super) fn bulk_additional_admission_role(
    reference_underlay: UnderlayProtocol,
    candidate_underlay: UnderlayProtocol,
) -> BulkAdmissionRole {
    if reference_underlay == candidate_underlay {
        BulkAdmissionRole::AdditionalSameUnderlay
    } else {
        BulkAdmissionRole::AdditionalCrossUnderlay
    }
}

pub(super) fn bulk_striping_admitted_subflows(
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

pub(super) fn bulk_service_horizon_payload_bytes(
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> usize {
    let stream_window = usize::try_from(mux_limits.max_stream_window_bytes).unwrap_or(usize::MAX);
    let envelope = mux_limits
        .max_path_flight_bytes
        .min(mux_limits.max_reorder_bytes)
        .min(stream_window)
        .max(payload_bytes)
        .max(1);
    let horizon = ((payload_bytes as f64) * (envelope as f64)).sqrt().round() as usize;
    horizon.clamp(payload_bytes.max(1), envelope)
}

#[cfg_attr(not(test), allow(dead_code))]
pub(super) fn bulk_candidate_admission_suppression(
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

pub(super) fn bulk_candidate_admission_suppression_with_ordering_debt(
    check: BulkAdmissionCheck,
) -> Option<&'static str> {
    if let Some(reason) = bulk_cross_underlay_completion_suppression(check) {
        return Some(reason);
    }
    if let Some(reason) = bulk_same_underlay_ordering_debt_suppression(check) {
        return Some(reason);
    }
    if let Some(reason) = bulk_same_underlay_completion_suppression(check) {
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

fn bulk_same_underlay_ordering_debt_suppression(check: BulkAdmissionCheck) -> Option<&'static str> {
    if check.role == BulkAdmissionRole::AdditionalSameUnderlay
        && check.stream_ordering_debt_bytes > 0
    {
        return Some("same_underlay_ordering_debt");
    }
    None
}

fn bulk_same_underlay_completion_suppression(check: BulkAdmissionCheck) -> Option<&'static str> {
    if check.role != BulkAdmissionRole::AdditionalSameUnderlay {
        return None;
    }
    if !bulk_same_underlay_requires_completion_gain(check.candidate_snapshot) {
        return None;
    }
    let lead_next_quantum_eta_ms =
        check.best_eta_ms + bulk_payload_tx_ms(check.best_snapshot, check.payload_bytes);
    if check.candidate_eta_ms > lead_next_quantum_eta_ms {
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
pub(super) fn bulk_completion_horizon_ms(
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

pub(super) fn bulk_completion_horizon_ms_with_ordering_debt(
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
            bulk_active_lead_product_envelope_bytes(candidate, payload_bytes, mux_limits);
        let committed = candidate
            .product_bytes_in_flight
            .saturating_add(candidate.product_queue_bytes)
            .saturating_add(candidate.queue_bytes);
        return committed.saturating_add(payload_bytes as u64) <= inflight_limit
            || committed < inflight_limit;
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

fn bulk_active_lead_product_envelope_bytes(
    candidate: PathSnapshot,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> u64 {
    let stream_window = usize::try_from(mux_limits.max_stream_window_bytes).unwrap_or(usize::MAX);
    let configured = mux_limits
        .max_path_flight_bytes
        .min(stream_window)
        .min(mux_limits.max_reorder_bytes)
        .max(payload_bytes) as u64;
    let startup_limit = if candidate.underlay == UnderlayProtocol::Udp {
        RELIABLE_UDP_INITIAL_PRODUCT_WINDOW_BYTES
            .min(configured)
            .max(payload_bytes as u64)
    } else {
        configured
    };
    let Some(progress_rate_bps) = candidate
        .product_progress_rate_bps
        .filter(|rate| rate.is_finite() && *rate > 0.0)
    else {
        return startup_limit;
    };

    let progress_bdp =
        bulk_rate_bdp_bytes(progress_rate_bps, bulk_product_budget_rtt_ms(candidate));
    let progress_limit = bulk_bbr_inflight_bytes(progress_bdp)
        .max((payload_bytes as u64).saturating_mul(8))
        .max(startup_limit);
    configured.min(progress_limit)
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
        return bulk_active_lead_product_envelope_bytes(candidate, payload_bytes, mux_limits);
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
    if bulk_uses_product_only_active_gate(candidate, role) {
        return candidate
            .product_bytes_in_flight
            .saturating_add(candidate.queue_bytes);
    }
    if role == BulkAdmissionRole::ActiveDataPath {
        return bulk_product_reorder_debt_bytes(candidate).saturating_add(candidate.queue_bytes);
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

fn bulk_active_role_has_latency_pressure(candidate: PathSnapshot, role: BulkAdmissionRole) -> bool {
    matches!(
        role,
        BulkAdmissionRole::ActiveDataPath | BulkAdmissionRole::ActiveSingleCarrier
    ) && bulk_latency_pressure_flows(candidate) > 0
}

fn bulk_latency_pressure_flows(candidate: PathSnapshot) -> u32 {
    candidate
        .active_latency_sensitive_flows
        .max(candidate.session_active_latency_sensitive_flows)
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
        BulkAdmissionRole::ActiveDataPath | BulkAdmissionRole::ActiveSingleCarrier => {
            candidate.queue_bytes
        }
        BulkAdmissionRole::AdditionalSameUnderlay | BulkAdmissionRole::AdditionalCrossUnderlay => {
            bulk_product_reorder_debt_bytes(candidate)
        }
    };
    path_debt.saturating_add(stream_ordering_debt_bytes)
}

fn bulk_payload_tx_ms(snapshot: PathSnapshot, payload_bytes: usize) -> f64 {
    payload_bytes as f64 * 8.0 / bulk_effective_rate_bps(snapshot) * 1000.0
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
                bulk_product_inflight_limit_bytes(
                    candidate,
                    payload_bytes,
                    mux_limits,
                    BulkAdmissionRole::ActiveDataPath,
                )
            } else {
                bulk_active_lead_product_envelope_bytes(candidate, payload_bytes, mux_limits)
            }
        }
        BulkAdmissionRole::ActiveDataPath | BulkAdmissionRole::ActiveSingleCarrier => {
            let reorder_budget = bulk_reorder_budget_bytes(candidate, payload_bytes, mux_limits);
            if stream_ordering_debt_bytes > 0 {
                let service_horizon =
                    bulk_service_horizon_payload_bytes(payload_bytes, mux_limits) as u64;
                return reorder_budget.min(service_horizon.max(payload_bytes as u64));
            }
            reorder_budget
        }
        BulkAdmissionRole::AdditionalSameUnderlay => {
            bulk_same_underlay_reorder_budget_bytes(candidate, payload_bytes, mux_limits)
        }
        BulkAdmissionRole::AdditionalCrossUnderlay => {
            bulk_effective_reorder_budget_bytes(candidate, payload_bytes, mux_limits)
        }
    }
}

fn bulk_same_underlay_reorder_budget_bytes(
    candidate: PathSnapshot,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> u64 {
    let product_budget = bulk_reorder_budget_bytes(candidate, payload_bytes, mux_limits);
    if candidate.underlay != UnderlayProtocol::Udp || candidate.inflight_limit_bytes == 0 {
        return product_budget;
    }
    candidate
        .inflight_limit_bytes
        .max(product_budget)
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

fn bulk_rate_bdp_bytes(rate_bps: f64, srtt_ms: f64) -> u64 {
    let rate_bps = rate_bps.max(1.0);
    (rate_bps / 8.0 * srtt_ms.max(1.0) / 1000.0).ceil() as u64
}

fn bulk_product_budget_rtt_ms(candidate: PathSnapshot) -> f64 {
    candidate.min_rtt_ms.min(candidate.srtt_ms).max(1.0)
}

fn bulk_bbr_inflight_bytes(bdp_bytes: u64) -> u64 {
    ((bdp_bytes as f64) * BBR_DEFAULT_CWND_GAIN).ceil() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{PathId, UnderlayProtocol};

    fn mbps(value: f64) -> f64 {
        value * 1_000_000.0
    }

    fn candidate(index: usize, eta_ms: f64, srtt_ms: f64, rate_mbps: f64) -> BulkPathCandidate {
        BulkPathCandidate {
            key: RelayPathKey {
                underlay: UnderlayProtocol::Udp,
                index,
            },
            eta_ms,
            has_liveness_evidence: true,
            has_path_proof_evidence: false,
            has_ack_data_evidence: true,
            has_bulk_rate_evidence: true,
            has_sender_delivery_evidence: true,
            snapshot: PathSnapshot::new(
                PathId(index as u16),
                UnderlayProtocol::Udp,
                srtt_ms,
                mbps(rate_mbps),
            ),
        }
    }

    #[test]
    fn bulk_admission_allows_same_underlay_candidate_that_beats_lead_next_quantum() {
        let admitted = bulk_striping_admitted_subflows(
            vec![
                candidate(0, 1000.0, 250.0, 100.0),
                candidate(1, 1004.0, 260.0, 100.0),
            ],
            64 * 1024,
            MuxLimits::default(),
        );

        assert_eq!(admitted.len(), 2);
        assert_eq!(admitted[1].key.index, 1);
    }

    #[test]
    fn same_underlay_candidate_with_sub_quantum_debt_still_gets_one_service_quantum() {
        let payload_bytes = 512 * 1024;
        let best = candidate(0, 1000.0, 80.0, 200.0);
        let mut alternate = candidate(1, 1001.0, 80.0, 200.0);
        alternate.snapshot.inflight_limit_bytes = payload_bytes as u64;
        alternate.snapshot.bytes_in_flight = 34;
        alternate.snapshot.product_bytes_in_flight = 34;

        let admitted = bulk_striping_admitted_subflows(
            vec![best, alternate],
            payload_bytes,
            MuxLimits::default(),
        );

        assert_eq!(
            admitted
                .iter()
                .map(|candidate| candidate.key.index)
                .collect::<Vec<_>>(),
            vec![0, 1],
            "same-underlay admission is quantum-granular: tiny existing product debt should not suppress one otherwise-admissible service quantum"
        );
    }

    #[test]
    fn cross_underlay_bulk_admission_rejects_candidate_outside_completion_horizon() {
        let mut cross_underlay = candidate(1, 5000.0, 260.0, 250.0);
        cross_underlay.key.underlay = UnderlayProtocol::Tcp;
        cross_underlay.snapshot.underlay = UnderlayProtocol::Tcp;
        let admitted = bulk_striping_admitted_subflows(
            vec![candidate(0, 1000.0, 250.0, 300.0), cross_underlay],
            64 * 1024,
            MuxLimits::default(),
        );

        assert_eq!(admitted.len(), 1);
        assert_eq!(admitted[0].key.index, 0);
    }

    #[test]
    fn active_tcp_product_inflight_limit_is_model_based() {
        let mux_limits = MuxLimits {
            max_path_flight_bytes: 32 * 1024 * 1024,
            ..MuxLimits::default()
        };
        let candidate = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 50.0, mbps(50.0));
        let limit = bulk_product_inflight_limit_bytes(
            candidate,
            64 * 1024,
            mux_limits,
            BulkAdmissionRole::ActiveDataPath,
        );

        assert!(limit < mux_limits.max_path_flight_bytes as u64);
        assert_eq!(limit, 625_000);
    }

    #[test]
    fn active_tcp_with_session_latency_pressure_uses_preemptible_service_horizon() {
        let mux_limits = MuxLimits {
            max_path_flight_bytes: 32 * 1024 * 1024,
            ..MuxLimits::default()
        };
        let mut candidate = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 672.0, mbps(100.0));
        candidate.session_active_latency_sensitive_flows = 1;
        let payload = 64 * 1024;
        let limit = bulk_product_inflight_limit_bytes(
            candidate,
            payload,
            mux_limits,
            BulkAdmissionRole::ActiveDataPath,
        );

        assert_eq!(
            limit,
            bulk_service_horizon_payload_bytes(payload, mux_limits) as u64
        );
        assert!(limit < bulk_bbr_inflight_bytes(bulk_path_bdp_bytes(candidate)));
        assert!(limit >= payload as u64);
    }

    #[test]
    fn bulk_service_horizon_is_geometric_mean_not_full_envelope() {
        let payload = 64 * 1024;
        let mux_limits = MuxLimits::default();

        assert_eq!(
            bulk_service_horizon_payload_bytes(payload, mux_limits),
            2 * 1024 * 1024,
            "the service horizon is a preemptible scoring window, not the full product envelope"
        );
    }

    #[test]
    fn active_tcp_with_session_latency_pressure_rejects_hidden_command_backlog() {
        let best = candidate(0, 100.0, 50.0, 500.0);
        let mut active = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 672.0, mbps(100.0));
        active.queue_bytes = 8 * 1024 * 1024;
        active.product_bytes_in_flight = 8 * 1024 * 1024;
        active.session_active_latency_sensitive_flows = 1;

        assert_eq!(
            bulk_candidate_admission_suppression(
                best.snapshot,
                best.eta_ms,
                active,
                100.0,
                64 * 1024,
                MuxLimits::default(),
                BulkAdmissionRole::ActiveDataPath,
            ),
            Some("reorder_budget")
        );
    }

    #[test]
    fn active_tcp_service_with_clear_frontier_uses_product_envelope_for_reorder_budget() {
        let mux_limits = MuxLimits {
            max_path_flight_bytes: 64 * 1024 * 1024,
            max_reorder_bytes: 64 * 1024 * 1024,
            ..MuxLimits::default()
        };
        let payload = 64 * 1024;
        let mut active = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 333.0, mbps(0.351));
        active.pacing_rate_bps = mbps(0.351);
        active.product_progress_rate_bps = Some(mbps(0.351));
        active.queue_bytes = payload as u64;
        active.product_bytes_in_flight = 341_200;
        active.app_limited = true;

        assert_eq!(
            bulk_candidate_admission_suppression(
                active,
                50_018.062,
                active,
                50_018.062,
                payload,
                mux_limits,
                BulkAdmissionRole::ActiveDataPath,
            ),
            None,
            "a clear-frontier Service owner must not lose lead eligibility because the reorder gate reuses a tiny app-limited BDP budget"
        );
    }

    #[test]
    fn active_udp_owner_with_clear_frontier_uses_product_envelope_not_carrier_cwnd() {
        let mux_limits = MuxLimits {
            max_path_flight_bytes: 32 * 1024 * 1024,
            ..MuxLimits::default()
        };
        let mut candidate = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 80.0, mbps(500.0));
        candidate.inflight_limit_bytes = 128 * 1024;
        candidate.product_bytes_in_flight = 96 * 1024;
        candidate.queue_bytes = 16 * 1024;

        assert_eq!(
            bulk_candidate_admission_suppression(
                candidate,
                10.0,
                candidate,
                10.0,
                32 * 1024,
                mux_limits,
                BulkAdmissionRole::ActiveDataPath,
            ),
            None,
            "active QUIC owner must not use current carrier cwnd as a product admission ceiling"
        );
    }

    #[test]
    fn active_udp_clear_frontier_uses_product_service_envelope() {
        let mux_limits = MuxLimits {
            max_path_flight_bytes: 64 * 1024 * 1024,
            max_reorder_bytes: 64 * 1024 * 1024,
            ..MuxLimits::default()
        };
        let mut candidate = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 60.0, mbps(200.0));
        candidate.inflight_limit_bytes = 512 * 1024;
        candidate.product_bytes_in_flight = 512 * 1024;
        candidate.queue_bytes = 0;

        assert_eq!(
            bulk_candidate_admission_suppression(
                candidate,
                10.0,
                candidate,
                10.0,
                64 * 1024,
                mux_limits,
                BulkAdmissionRole::ActiveDataPath,
            ),
            None,
            "active service owner should be fed through the product service envelope when the ordered frontier is clear"
        );
    }

    #[test]
    fn active_udp_with_product_progress_debt_is_bdp_capped() {
        let mux_limits = MuxLimits {
            max_path_flight_bytes: 64 * 1024 * 1024,
            max_reorder_bytes: 64 * 1024 * 1024,
            ..MuxLimits::default()
        };
        let mut candidate =
            PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 1000.0, mbps(500.0));
        candidate.pacing_rate_bps = mbps(2_000.0);
        candidate.product_progress_rate_bps = Some(mbps(10.0));
        candidate.product_bytes_in_flight = 8 * 1024 * 1024;

        assert_eq!(
            bulk_candidate_admission_suppression(
                candidate,
                10.0,
                candidate,
                10.0,
                64 * 1024,
                mux_limits,
                BulkAdmissionRole::ActiveDataPath,
            ),
            Some("inflight_limit"),
            "active QUIC Service ownership must be bounded by product-progress BDP once real product progress exists; carrier pacing must not inflate product offset flight to the full resource envelope"
        );
    }

    #[test]
    fn active_udp_with_latency_pressure_still_uses_service_product_cap() {
        let mux_limits = MuxLimits {
            max_path_flight_bytes: 64 * 1024 * 1024,
            max_reorder_bytes: 64 * 1024 * 1024,
            ..MuxLimits::default()
        };
        let mut candidate =
            PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 1000.0, mbps(500.0));
        candidate.pacing_rate_bps = mbps(2_000.0);
        candidate.product_progress_rate_bps = Some(mbps(10.0));
        candidate.product_bytes_in_flight = 8 * 1024 * 1024;
        candidate.active_latency_sensitive_flows = 1;

        assert_eq!(
            bulk_candidate_admission_suppression(
                candidate,
                10.0,
                candidate,
                10.0,
                64 * 1024,
                mux_limits,
                BulkAdmissionRole::ActiveDataPath,
            ),
            Some("inflight_limit"),
            "latency pressure may shorten Service bursts, but must not switch active QUIC product ownership back to a carrier-pacing-derived backlog limit"
        );
    }

    #[test]
    fn active_udp_product_progress_cap_uses_queue_resistant_min_rtt() {
        let mux_limits = MuxLimits {
            max_path_flight_bytes: 64 * 1024 * 1024,
            max_reorder_bytes: 64 * 1024 * 1024,
            ..MuxLimits::default()
        };
        let mut candidate =
            PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 1400.0, mbps(500.0));
        candidate.min_rtt_ms = 80.0;
        candidate.product_progress_rate_bps = Some(mbps(85.0));
        candidate.product_bytes_in_flight = 4 * 1024 * 1024;

        assert_eq!(
            bulk_candidate_admission_suppression(
                candidate,
                10.0,
                candidate,
                10.0,
                64 * 1024,
                mux_limits,
                BulkAdmissionRole::ActiveDataPath,
            ),
            Some("inflight_limit"),
            "active QUIC product-progress caps must use queue-resistant RTT; using queue-inflated SRTT grows the product backlog and feeds HOL debt"
        );
    }

    #[test]
    fn active_udp_without_product_progress_uses_startup_envelope_not_full_resource_ceiling() {
        let mux_limits = MuxLimits {
            max_path_flight_bytes: 64 * 1024 * 1024,
            max_reorder_bytes: 64 * 1024 * 1024,
            ..MuxLimits::default()
        };
        let mut candidate = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 80.0, mbps(500.0));
        candidate.pacing_rate_bps = mbps(2_000.0);
        candidate.product_bytes_in_flight = 8 * 1024 * 1024;

        assert_eq!(
            bulk_candidate_admission_suppression(
                candidate,
                10.0,
                candidate,
                10.0,
                64 * 1024,
                mux_limits,
                BulkAdmissionRole::ActiveDataPath,
            ),
            Some("inflight_limit"),
            "active QUIC Service startup must not borrow the full configured product flight resource before product progress exists"
        );
    }

    #[test]
    fn cross_underlay_product_inflight_limit_is_bdp_modeled() {
        let mux_limits = MuxLimits {
            max_path_flight_bytes: 32 * 1024 * 1024,
            ..MuxLimits::default()
        };
        let candidate = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 50.0, mbps(50.0));
        let limit = bulk_product_inflight_limit_bytes(
            candidate,
            64 * 1024,
            mux_limits,
            BulkAdmissionRole::AdditionalCrossUnderlay,
        );

        assert!(limit < mux_limits.max_path_flight_bytes as u64);
        assert_eq!(limit, 625_000);
    }

    #[test]
    fn same_underlay_candidate_must_not_join_only_because_reorder_budget_can_absorb_gap() {
        let admitted = bulk_striping_admitted_subflows(
            vec![
                candidate(0, 2958.0, 80.0, 180.0),
                candidate(1, 3202.0, 180.0, 220.0),
            ],
            64 * 1024,
            MuxLimits::default(),
        );

        assert_eq!(admitted.len(), 1);
        assert_eq!(admitted[0].key.index, 0);
    }

    #[test]
    fn bulk_admission_rejects_candidate_that_would_exceed_product_inflight_limit() {
        let best = candidate(0, 100.0, 50.0, 500.0);
        let mut saturated = candidate(1, 110.0, 50.0, 500.0);
        saturated.snapshot.inflight_limit_bytes = 64 * 1024;
        saturated.snapshot.bytes_in_flight = MuxLimits::default().max_path_flight_bytes as u64;

        let admitted =
            bulk_striping_admitted_subflows(vec![best, saturated], 16 * 1024, MuxLimits::default());

        assert_eq!(admitted.len(), 1);
        assert_eq!(admitted[0].key.index, 0);
    }

    #[test]
    fn additional_path_reorder_budget_is_not_floored_to_product_inflight_limit() {
        let best = candidate(0, 100.0, 50.0, 500.0);
        let mut extra = candidate(1, 100.0, 50.0, 50.0);
        extra.snapshot.confidence = 1.0;
        extra.snapshot.inflight_limit_bytes = MuxLimits::default().max_path_flight_bytes as u64;
        extra.snapshot.bytes_in_flight = 1024 * 1024;

        assert_eq!(
            bulk_candidate_admission_suppression(
                best.snapshot,
                best.eta_ms,
                extra.snapshot,
                extra.eta_ms,
                64 * 1024,
                MuxLimits::default(),
                BulkAdmissionRole::AdditionalCrossUnderlay,
            ),
            Some("reorder_budget")
        );
    }

    #[test]
    fn tcp_active_path_without_ordering_debt_uses_lead_product_envelope() {
        let best = candidate(0, 100.0, 50.0, 500.0);
        let mut active = candidate(0, 100.0, 50.0, 0.35);
        active.snapshot.underlay = UnderlayProtocol::Tcp;
        active.snapshot.confidence = 0.1;
        active.snapshot.app_limited = true;
        active.snapshot.bytes_in_flight = 8 * 1024 * 1024;
        active.snapshot.product_bytes_in_flight = 8 * 1024 * 1024;

        assert_eq!(
            bulk_candidate_admission_suppression(
                best.snapshot,
                best.eta_ms,
                active.snapshot,
                active.eta_ms,
                64 * 1024,
                MuxLimits::default(),
                BulkAdmissionRole::ActiveDataPath,
            ),
            None
        );
    }

    #[test]
    fn tcp_active_service_under_bulk_demand_is_not_starved_by_latency_pressure_flag() {
        let mut active = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 333.0, mbps(0.351));
        active.pacing_rate_bps = mbps(0.351);
        active.product_bytes_in_flight = 380_304;
        active.product_queue_bytes = 395_112;
        active.session_active_latency_sensitive_flows = 1;
        active.confidence = 0.1;
        active.app_limited = true;

        assert_eq!(
            bulk_candidate_admission_suppression(
                active,
                57_482.654,
                active,
                57_482.654,
                64 * 1024,
                MuxLimits::default(),
                BulkAdmissionRole::ActiveDataPath,
            ),
            None,
            "latency-first startup state must not shrink an active bulk Service owner to a tiny startup-rate BDP while the ordered frontier is clear"
        );
    }

    #[test]
    fn tcp_active_path_with_ordering_debt_obeys_model_based_product_flight_budget() {
        let best = candidate(0, 100.0, 50.0, 500.0);
        let mut active = candidate(0, 100.0, 50.0, 50.0);
        active.snapshot.underlay = UnderlayProtocol::Tcp;
        active.snapshot.confidence = 1.0;
        active.snapshot.inflight_limit_bytes = MuxLimits::default().max_path_flight_bytes as u64;
        active.snapshot.bytes_in_flight = 1024 * 1024;

        assert_eq!(
            bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
                best_snapshot: best.snapshot,
                best_eta_ms: best.eta_ms,
                candidate_snapshot: active.snapshot,
                candidate_eta_ms: active.eta_ms,
                payload_bytes: 64 * 1024,
                mux_limits: MuxLimits::default(),
                role: BulkAdmissionRole::ActiveDataPath,
                stream_ordering_debt_bytes: 1024 * 1024,
            }),
            Some("inflight_limit")
        );
    }

    #[test]
    fn lagging_active_path_must_not_expand_cross_path_stream_hole() {
        let best = candidate(1, 100.0, 170.0, 180.0);
        let mut lagging_active = candidate(0, 1900.0, 1800.0, 1.0);
        lagging_active.snapshot.underlay = UnderlayProtocol::Tcp;
        lagging_active.snapshot.confidence = 1.0;
        lagging_active.snapshot.inflight_limit_bytes =
            MuxLimits::default().max_path_flight_bytes as u64;

        assert_eq!(
            bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
                best_snapshot: best.snapshot,
                best_eta_ms: best.eta_ms,
                candidate_snapshot: lagging_active.snapshot,
                candidate_eta_ms: lagging_active.eta_ms,
                payload_bytes: 16 * 1024,
                mux_limits: MuxLimits::default(),
                role: BulkAdmissionRole::ActiveDataPath,
                stream_ordering_debt_bytes: 15 * 1024 * 1024,
            },),
            Some("reorder_budget")
        );
    }

    #[test]
    fn best_active_path_can_continue_across_small_existing_hole() {
        let active = candidate(0, 100.0, 170.0, 180.0);
        let payload = 16 * 1024;
        let stream_ordering_debt_bytes = 64 * 1024;

        assert_eq!(
            bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
                best_snapshot: active.snapshot,
                best_eta_ms: active.eta_ms,
                candidate_snapshot: active.snapshot,
                candidate_eta_ms: active.eta_ms,
                payload_bytes: payload,
                mux_limits: MuxLimits::default(),
                role: BulkAdmissionRole::ActiveDataPath,
                stream_ordering_debt_bytes,
            },),
            None
        );
    }

    #[test]
    fn active_lower_frontier_owner_uses_service_horizon_when_debt_exists() {
        let active = candidate(0, 100.0, 170.0, 500.0);
        let payload = 64 * 1024;
        let mux_limits = MuxLimits::default();
        let service_horizon = bulk_service_horizon_payload_bytes(payload, mux_limits) as u64;

        assert_eq!(
            bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
                best_snapshot: active.snapshot,
                best_eta_ms: active.eta_ms,
                candidate_snapshot: active.snapshot,
                candidate_eta_ms: active.eta_ms,
                payload_bytes: payload,
                mux_limits,
                role: BulkAdmissionRole::ActiveDataPath,
                stream_ordering_debt_bytes: service_horizon.saturating_add(payload as u64),
            },),
            Some("reorder_budget")
        );
    }

    #[test]
    fn lead_path_with_large_cross_path_hole_uses_reorder_budget() {
        let lead = candidate(0, 100.0, 170.0, 180.0);

        assert_eq!(
            bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
                best_snapshot: lead.snapshot,
                best_eta_ms: lead.eta_ms,
                candidate_snapshot: lead.snapshot,
                candidate_eta_ms: lead.eta_ms,
                payload_bytes: 16 * 1024,
                mux_limits: MuxLimits::default(),
                role: BulkAdmissionRole::ActiveDataPath,
                stream_ordering_debt_bytes: 32 * 1024 * 1024,
            },),
            Some("reorder_budget")
        );
    }

    #[test]
    fn product_inflight_limit_is_modeled_limit_capped_by_configured_ceiling() {
        let best = candidate(0, 100.0, 50.0, 500.0);
        let mut constrained = candidate(1, 100.1, 50.0, 10.0);
        constrained.snapshot.confidence = 1.0;
        constrained.snapshot.inflight_limit_bytes = 64 * 1024;
        constrained.snapshot.bytes_in_flight = 128 * 1024;

        assert_eq!(
            bulk_candidate_admission_suppression(
                best.snapshot,
                best.eta_ms,
                constrained.snapshot,
                constrained.eta_ms,
                16 * 1024,
                MuxLimits::default(),
                BulkAdmissionRole::AdditionalCrossUnderlay,
            ),
            Some("inflight_limit")
        );
    }

    #[test]
    fn udp_multipath_active_path_ignores_carrier_flight_as_product_stop() {
        let best = candidate(0, 100.0, 50.0, 500.0);
        let mut active = candidate(0, 100.0, 50.0, 500.0);
        active.snapshot.confidence = 1.0;
        active.snapshot.inflight_limit_bytes = 512 * 1024;
        active.snapshot.bytes_in_flight = 512 * 1024;
        active.snapshot.product_bytes_in_flight = 0;

        assert_eq!(
            bulk_candidate_admission_suppression(
                best.snapshot,
                best.eta_ms,
                active.snapshot,
                active.eta_ms,
                64 * 1024,
                MuxLimits::default(),
                BulkAdmissionRole::ActiveDataPath,
            ),
            None
        );
    }

    #[test]
    fn udp_active_ordered_owner_uses_product_envelope_not_carrier_cwnd_gate() {
        let best = candidate(0, 100.0, 50.0, 500.0);
        let mut active = candidate(0, 100.0, 50.0, 500.0);
        active.snapshot.confidence = 1.0;
        active.snapshot.inflight_limit_bytes = 512 * 1024;
        active.snapshot.bytes_in_flight = 512 * 1024;

        assert_eq!(
            bulk_candidate_admission_suppression(
                best.snapshot,
                best.eta_ms,
                active.snapshot,
                active.eta_ms,
                64 * 1024,
                MuxLimits::default(),
                BulkAdmissionRole::ActiveDataPath,
            ),
            None,
            "QUIC carrier cwnd/in-flight is carrier-owned pacing evidence; the product active owner must be bounded by product flight and carrier writer backpressure, not a duplicate hard cwnd gate"
        );
    }

    #[test]
    fn udp_single_carrier_lead_uses_product_budget_not_duplicate_carrier_cwnd() {
        let best = candidate(0, 100.0, 50.0, 500.0);
        let mut active = candidate(0, 100.0, 50.0, 500.0);
        active.snapshot.confidence = 1.0;
        active.snapshot.active_flows = 2;
        active.snapshot.active_latency_sensitive_flows = 1;
        active.snapshot.inflight_limit_bytes = 512 * 1024;
        active.snapshot.bytes_in_flight = 512 * 1024;

        assert_eq!(
            bulk_candidate_admission_suppression(
                best.snapshot,
                best.eta_ms,
                active.snapshot,
                active.eta_ms,
                64 * 1024,
                MuxLimits::default(),
                BulkAdmissionRole::ActiveSingleCarrier,
            ),
            None
        );
    }

    #[test]
    fn udp_single_flow_lead_uses_product_gate_not_carrier_queue_gate() {
        let best = candidate(0, 100.0, 50.0, 500.0);
        let mut active = candidate(0, 100.0, 50.0, 500.0);
        active.snapshot.confidence = 1.0;
        active.snapshot.active_flows = 1;
        active.snapshot.active_latency_sensitive_flows = 0;
        active.snapshot.inflight_limit_bytes = 512 * 1024;
        active.snapshot.bytes_in_flight = 512 * 1024;

        assert_eq!(
            bulk_candidate_admission_suppression(
                best.snapshot,
                best.eta_ms,
                active.snapshot,
                active.eta_ms,
                64 * 1024,
                MuxLimits::default(),
                BulkAdmissionRole::ActiveSingleCarrier,
            ),
            None
        );
    }

    #[test]
    fn udp_cross_underlay_extra_path_uses_carrier_queue_gate() {
        let best = candidate(0, 100.0, 50.0, 500.0);
        let mut extra = candidate(1, 100.0, 50.0, 500.0);
        extra.snapshot.confidence = 1.0;
        extra.snapshot.inflight_limit_bytes = 512 * 1024;
        extra.snapshot.bytes_in_flight = 512 * 1024;

        assert_eq!(
            bulk_candidate_admission_suppression(
                best.snapshot,
                best.eta_ms,
                extra.snapshot,
                extra.eta_ms,
                64 * 1024,
                MuxLimits::default(),
                BulkAdmissionRole::AdditionalCrossUnderlay,
            ),
            Some("inflight_limit")
        );
    }

    #[test]
    fn udp_active_path_without_carrier_limit_uses_modeled_credit() {
        let best = candidate(0, 100.0, 50.0, 500.0);
        let mut startup = candidate(0, 100.1, 50.0, 10.0);
        startup.snapshot.confidence = 0.1;
        startup.snapshot.active_flows = 2;
        startup.snapshot.active_latency_sensitive_flows = 1;
        startup.snapshot.bytes_in_flight = 1024 * 1024;

        assert_eq!(
            bulk_candidate_admission_suppression(
                best.snapshot,
                best.eta_ms,
                startup.snapshot,
                startup.eta_ms,
                64 * 1024,
                MuxLimits::default(),
                BulkAdmissionRole::ActiveSingleCarrier,
            ),
            None
        );
        assert_eq!(
            bulk_candidate_admission_suppression(
                best.snapshot,
                best.eta_ms,
                startup.snapshot,
                startup.eta_ms,
                64 * 1024,
                MuxLimits::default(),
                BulkAdmissionRole::AdditionalCrossUnderlay,
            ),
            Some("inflight_limit")
        );
    }

    #[test]
    fn same_underlay_extra_path_uses_ack_clocked_budget_not_tiny_probe_budget() {
        let best = candidate(0, 100.0, 50.0, 500.0);
        let mut extra = candidate(1, 50.0, 50.0, 50.0);
        extra.snapshot.confidence = 0.1;
        extra.snapshot.bytes_in_flight = 512 * 1024;

        assert_eq!(
            bulk_candidate_admission_suppression(
                best.snapshot,
                best.eta_ms,
                extra.snapshot,
                extra.eta_ms,
                64 * 1024,
                MuxLimits::default(),
                BulkAdmissionRole::AdditionalSameUnderlay,
            ),
            None
        );
        assert_eq!(
            bulk_candidate_admission_suppression(
                best.snapshot,
                best.eta_ms,
                extra.snapshot,
                extra.eta_ms,
                64 * 1024,
                MuxLimits::default(),
                BulkAdmissionRole::AdditionalCrossUnderlay,
            ),
            Some("reorder_budget")
        );
    }

    #[test]
    fn same_underlay_startup_probe_is_not_rejected_by_app_limited_completion_horizon() {
        let mut lead = candidate(0, 1_530_000.0, 50.0, 0.35);
        let mut extra = candidate(1, 3_060_000.0, 50.0, 0.35);
        lead.snapshot.confidence = 0.1;
        lead.snapshot.app_limited = true;
        extra.snapshot.confidence = 0.1;
        extra.snapshot.app_limited = true;

        assert_eq!(
            bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
                best_snapshot: lead.snapshot,
                best_eta_ms: lead.eta_ms,
                candidate_snapshot: extra.snapshot,
                candidate_eta_ms: extra.eta_ms,
                payload_bytes: 64 * 1024,
                mux_limits: MuxLimits::default(),
                role: BulkAdmissionRole::AdditionalSameUnderlay,
                stream_ordering_debt_bytes: 0,
            }),
            None
        );
    }

    #[test]
    fn same_underlay_low_confidence_sender_samples_remain_startup_admissible() {
        let mut lead = candidate(0, 98_000.0, 80.0, 1.0);
        let mut extra = candidate(1, 1_500_000.0, 80.0, 1.0);
        lead.snapshot.confidence = 0.1;
        extra.snapshot.confidence = 0.1;
        extra.snapshot.inflight_limit_bytes = 256 * 1024;
        extra.snapshot.product_bytes_in_flight = 128 * 1024;

        assert_eq!(
            bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
                best_snapshot: lead.snapshot,
                best_eta_ms: lead.eta_ms,
                candidate_snapshot: extra.snapshot,
                candidate_eta_ms: extra.eta_ms,
                payload_bytes: 64 * 1024,
                mux_limits: MuxLimits::default(),
                role: BulkAdmissionRole::AdditionalSameUnderlay,
                stream_ordering_debt_bytes: 0,
            }),
            None
        );
    }

    #[test]
    fn same_underlay_clear_frontier_still_requires_completion_gain_after_bulk_proof() {
        let mut lead = candidate(0, 98_000.0, 80.0, 500.0);
        let mut extra = candidate(1, 1_500_000.0, 80.0, 500.0);
        lead.snapshot.confidence = 1.0;
        extra.snapshot.confidence = 1.0;
        extra.snapshot.inflight_limit_bytes = 16 * 1024 * 1024;

        assert_eq!(
            bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
                best_snapshot: lead.snapshot,
                best_eta_ms: lead.eta_ms,
                candidate_snapshot: extra.snapshot,
                candidate_eta_ms: extra.eta_ms,
                payload_bytes: 64 * 1024,
                mux_limits: MuxLimits::default(),
                role: BulkAdmissionRole::AdditionalSameUnderlay,
                stream_ordering_debt_bytes: 0,
            }),
            Some("same_underlay_no_completion_gain")
        );
        assert_eq!(
            bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
                best_snapshot: lead.snapshot,
                best_eta_ms: lead.eta_ms,
                candidate_snapshot: extra.snapshot,
                candidate_eta_ms: extra.eta_ms,
                payload_bytes: 64 * 1024,
                mux_limits: MuxLimits::default(),
                role: BulkAdmissionRole::AdditionalSameUnderlay,
                stream_ordering_debt_bytes: 512 * 1024,
            }),
            Some("same_underlay_ordering_debt")
        );
    }

    #[test]
    fn bulk_rate_proven_same_underlay_path_must_beat_lead_next_quantum() {
        let mut lead = candidate(0, 100_000_000.0, 50.0, 100.0);
        let mut extra = candidate(1, 500_000_000.0, 700.0, 500.0);
        lead.snapshot.confidence = 1.0;
        extra.snapshot.confidence = 1.0;
        extra.snapshot.app_limited = false;

        assert_eq!(
            bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
                best_snapshot: lead.snapshot,
                best_eta_ms: lead.eta_ms,
                candidate_snapshot: extra.snapshot,
                candidate_eta_ms: extra.eta_ms,
                payload_bytes: 64 * 1024,
                mux_limits: MuxLimits::default(),
                role: BulkAdmissionRole::AdditionalSameUnderlay,
                stream_ordering_debt_bytes: 0,
            }),
            Some("same_underlay_no_completion_gain")
        );
    }

    #[test]
    fn cross_underlay_path_can_join_only_when_it_beats_lead_next_quantum() {
        let best = candidate(0, 500.0, 50.0, 500.0);
        let mut extra = candidate(1, 504.0, 250.0, 500.0);
        extra.snapshot.confidence = 1.0;
        extra.snapshot.bytes_in_flight = 8 * 1024 * 1024;

        assert_eq!(
            bulk_candidate_admission_suppression(
                best.snapshot,
                best.eta_ms,
                extra.snapshot,
                extra.eta_ms,
                512 * 1024,
                MuxLimits::default(),
                BulkAdmissionRole::AdditionalCrossUnderlay,
            ),
            None
        );
    }

    #[test]
    fn cross_underlay_path_is_rejected_when_it_cannot_beat_lead_next_quantum() {
        let best = candidate(0, 500.0, 50.0, 500.0);
        let mut extra = candidate(1, 620.0, 250.0, 500.0);
        extra.snapshot.confidence = 1.0;

        assert_eq!(
            bulk_candidate_admission_suppression(
                best.snapshot,
                best.eta_ms,
                extra.snapshot,
                extra.eta_ms,
                512 * 1024,
                MuxLimits::default(),
                BulkAdmissionRole::AdditionalCrossUnderlay,
            ),
            Some("cross_underlay_no_completion_gain")
        );
    }

    #[test]
    fn stream_ordering_debt_suppresses_cross_underlay_candidate() {
        let best = candidate(0, 80.0, 50.0, 500.0);
        let mut extra = candidate(1, 80.5, 50.0, 500.0);
        extra.snapshot.confidence = 1.0;

        assert_eq!(
            bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
                best_snapshot: best.snapshot,
                best_eta_ms: best.eta_ms,
                candidate_snapshot: extra.snapshot,
                candidate_eta_ms: extra.eta_ms,
                payload_bytes: 64 * 1024,
                mux_limits: MuxLimits::default(),
                role: BulkAdmissionRole::AdditionalCrossUnderlay,
                stream_ordering_debt_bytes: 4 * 1024 * 1024,
            },),
            Some("cross_underlay_ordering_debt")
        );
    }

    #[test]
    fn active_path_with_ordering_debt_must_still_beat_lead_completion_horizon() {
        let best = candidate(0, 10.0, 10.0, 1000.0);
        let active_with_debt = candidate(1, 100.0, 10.0, 1000.0);
        let payload = 32 * 1024;

        assert_eq!(
            bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
                best_snapshot: best.snapshot,
                best_eta_ms: best.eta_ms,
                candidate_snapshot: active_with_debt.snapshot,
                candidate_eta_ms: active_with_debt.eta_ms,
                payload_bytes: payload,
                mux_limits: MuxLimits::default(),
                role: BulkAdmissionRole::ActiveDataPath,
                stream_ordering_debt_bytes: 16 * 1024,
            }),
            Some("completion_horizon")
        );
    }

    #[test]
    fn bulk_admission_rejects_saturated_best_candidate() {
        let mut saturated_best = candidate(0, 100.0, 50.0, 500.0);
        saturated_best.snapshot.inflight_limit_bytes = 64 * 1024;
        saturated_best.snapshot.product_bytes_in_flight =
            MuxLimits::default().max_path_flight_bytes as u64;
        let backup = candidate(1, 130.0, 50.0, 500.0);

        let admitted = bulk_striping_admitted_subflows(
            vec![saturated_best, backup],
            16 * 1024,
            MuxLimits::default(),
        );

        assert_eq!(admitted.len(), 1);
        assert_eq!(admitted[0].key.index, 1);
    }

    #[test]
    fn low_confidence_cross_underlay_candidate_outside_eta_subflow_set_is_suppressed() {
        let mut best = candidate(0, 1000.0, 180.0, 500.0);
        best.snapshot.confidence = 1.0;
        let mut uncertain = candidate(1, 1350.0, 180.0, 500.0);
        uncertain.key.underlay = UnderlayProtocol::Tcp;
        uncertain.snapshot.underlay = UnderlayProtocol::Tcp;
        uncertain.snapshot.confidence = 0.1;

        let admitted =
            bulk_striping_admitted_subflows(vec![best, uncertain], 64 * 1024, MuxLimits::default());

        assert_eq!(admitted.len(), 1);
        assert_eq!(admitted[0].key.index, 0);
    }
}
