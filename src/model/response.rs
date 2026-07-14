//! Carrier-neutral response ownership, load, and placement arithmetic.
//!
//! Runtime bindings measure and commit response state. This model keeps the
//! typed evidence and pure calculations shared by those transactions and the
//! sender planner; it owns no locks, channels, timers, or carrier handles.

use crate::model::admission::{
    BulkAdmissionCheck, BulkAdmissionRole, bulk_candidate_admission_suppression_with_ordering_debt,
};
use crate::model::multipath::cross_family_reliable_owner_health;
use crate::model::path::{CarrierPathInstanceId, CarrierPathKey, carrier_path_key_order};
use crate::mux::MuxLimits;
use crate::protocol::{StreamOpenRole, UnderlayProtocol};
use crate::scheduler::{PathRateScope, PathSnapshot};

/// One coherent response-path observation consumed by carrier-neutral policy.
///
/// Runtime command senders and TCP/QUIC controller state deliberately stay out
/// of this value. Physical identity remains present so a decision can return an
/// exact, generation-fenced intent instead of a live carrier handle.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ResponsePathObservation {
    pub(crate) key: CarrierPathKey,
    pub(crate) path_instance_id: CarrierPathInstanceId,
    pub(crate) incarnation: u64,
    pub(crate) attachment_role: StreamOpenRole,
    pub(crate) snapshot: PathSnapshot,
    pub(crate) owner_data_in_flight_bytes: u64,
    pub(crate) command_pending_bytes: u64,
    pub(crate) eta_ms: f64,
    /// True only for the persistent response Service owner.
    pub(crate) is_service: bool,
    /// Request Active and response Service are independent directional roles.
    pub(crate) is_request_active: bool,
    pub(crate) has_sender_evidence: bool,
    pub(crate) has_service_feed_evidence: bool,
    pub(crate) has_bulk_rate_evidence: bool,
}

impl AsRef<ResponsePathObservation> for ResponsePathObservation {
    fn as_ref(&self) -> &ResponsePathObservation {
        self
    }
}

impl ResponsePathObservation {
    pub(crate) fn same_physical_output(self, other: Self) -> bool {
        self.key == other.key
            && self.path_instance_id == other.path_instance_id
            && self.incarnation == other.incarnation
    }

    pub(crate) fn active_bulk_flows(self) -> u32 {
        self.snapshot
            .active_flows
            .saturating_sub(self.snapshot.active_latency_sensitive_flows)
    }

    pub(crate) fn is_plausible_unique_owner_candidate(self) -> bool {
        self.attachment_role != StreamOpenRole::Repair
            && (self.is_service || self.has_bulk_rate_evidence)
    }

    pub(crate) fn has_ack_gap_repair_evidence(self) -> bool {
        self.is_service || self.has_bulk_rate_evidence
    }
}

/// Completion baseline used by response bulk-admission policy.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ResponseBulkLead {
    pub(crate) key: CarrierPathKey,
    pub(crate) snapshot: PathSnapshot,
    pub(crate) eta_ms: f64,
}

pub(crate) fn response_service_fair_share_bps(
    target: &ResponsePathObservation,
    adds_flow: bool,
) -> f64 {
    response_rate_fair_share_bps(target.snapshot, target.snapshot.rate_scope, adds_flow)
}

#[cfg(any(test, feature = "lab-diagnostics"))]
pub(crate) fn response_service_handoff_preserves_fair_share(
    service: &ResponsePathObservation,
    target: &ResponsePathObservation,
) -> bool {
    // Sticky placement compares one moved flow; only aggregate carrier rates
    // are divided because TCP product ACK clocks already measure a flow share.
    response_service_fair_share_bps(service, false) <= response_service_fair_share_bps(target, true)
}

pub(crate) fn response_service_handoff_mode_for_observations(
    service: &ResponsePathObservation,
    target: &ResponsePathObservation,
    family_loads: ResponseServiceFamilyLoads,
) -> Option<ResponseServiceHandoffMode> {
    response_snapshot_handoff_mode(
        service.key.underlay,
        service.snapshot,
        target.key.underlay,
        target.snapshot,
        family_loads,
    )
}

pub(crate) fn response_cross_underlay_owner_allowed<T>(
    target: &ResponsePathObservation,
    candidates: &[T],
    ordered_data_owner: Option<CarrierPathKey>,
    lower_flights: &[CarrierPathFlightDebt],
) -> bool
where
    T: AsRef<ResponsePathObservation>,
{
    // A candidate that owns the lower frontier does not expand cross-path debt.
    let fallback_service = candidates
        .iter()
        .map(AsRef::as_ref)
        .find(|candidate| candidate.is_service);
    let current_owner = ordered_data_owner.or_else(|| fallback_service.map(|service| service.key));
    let current_owner_bulk_rate_proven = current_owner
        .and_then(|owner| {
            candidates
                .iter()
                .map(AsRef::as_ref)
                .find(|candidate| candidate.key == owner)
        })
        .is_none_or(|owner| owner.has_bulk_rate_evidence);
    let continues_lower_frontier =
        response_oldest_lower_flight_owner(lower_flights) == Some(target.key);
    if continues_lower_frontier && (target.is_service || target.has_bulk_rate_evidence) {
        return true;
    }
    cross_family_reliable_owner_health(
        current_owner,
        current_owner_bulk_rate_proven,
        target.key,
        target.has_bulk_rate_evidence,
        continues_lower_frontier,
    )
    .reliable_owner_allowed()
}

pub(crate) fn response_ordered_owner_missing_under_debt<T>(
    targets: &[T],
    lower_flights: &[CarrierPathFlightDebt],
    ordered_data_owner: Option<CarrierPathKey>,
    ordered_owner_debt_bytes: usize,
) -> bool
where
    T: AsRef<ResponsePathObservation>,
{
    if ordered_owner_debt_bytes == 0 || response_oldest_lower_flight_owner(lower_flights).is_some()
    {
        return false;
    }
    match ordered_data_owner {
        Some(owner) => {
            let live_owner = targets
                .iter()
                .map(AsRef::as_ref)
                .any(|target| target.key == owner);
            let same_family_sender_evidence = targets
                .iter()
                .map(AsRef::as_ref)
                .any(|target| target.key.underlay == owner.underlay && target.has_sender_evidence);
            !live_owner && !same_family_sender_evidence
        }
        None => true,
    }
}

pub(crate) fn response_active_lead_suppression(
    target: &ResponsePathObservation,
    mux_limits: MuxLimits,
    payload_bytes: usize,
    stream_ordering_debt_bytes: u64,
) -> Option<&'static str> {
    bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
        best_snapshot: target.snapshot,
        best_eta_ms: target.eta_ms,
        candidate_snapshot: target.snapshot,
        candidate_eta_ms: target.eta_ms,
        payload_bytes,
        mux_limits,
        role: BulkAdmissionRole::ActiveDataPath,
        stream_ordering_debt_bytes,
    })
}

pub(crate) fn choose_response_admissible_lead<T>(
    candidate_targets: &[T],
    service_baseline: Option<&ResponsePathObservation>,
    mux_limits: MuxLimits,
    payload_bytes: usize,
    lower_flights: &[CarrierPathFlightDebt],
    allow_liveness_service_failover: bool,
) -> Option<ResponseBulkLead>
where
    T: AsRef<ResponsePathObservation>,
{
    let lower_owner = response_oldest_lower_flight_owner(lower_flights);
    if let Some(service) = service_baseline {
        return Some(ResponseBulkLead {
            key: service.key,
            snapshot: service.snapshot,
            eta_ms: service.eta_ms,
        });
    }

    if let Some(owner) = lower_owner {
        let owner = candidate_targets
            .iter()
            .map(AsRef::as_ref)
            .find(|candidate| candidate.key == owner)?;
        if owner.is_service || owner.has_bulk_rate_evidence {
            let debt = response_ordering_debt_bytes(lower_flights, owner.key);
            return response_active_lead_suppression(owner, mux_limits, payload_bytes, debt)
                .is_none()
                .then_some(ResponseBulkLead {
                    key: owner.key,
                    snapshot: owner.snapshot,
                    eta_ms: owner.eta_ms,
                });
        }
    }

    let choose = |require_plausible: bool, require_proven: bool, require_unsuppressed: bool| {
        candidate_targets
            .iter()
            .map(AsRef::as_ref)
            .filter(|target| {
                (!require_plausible || target.is_plausible_unique_owner_candidate())
                    && (!require_proven || target.has_bulk_rate_evidence)
                    && (!require_unsuppressed
                        || response_active_lead_suppression(target, mux_limits, payload_bytes, 0)
                            .is_none())
            })
            .min_by(|left, right| {
                left.eta_ms
                    .total_cmp(&right.eta_ms)
                    .then_with(|| carrier_path_key_order(left.key, right.key))
            })
            .map(|target| ResponseBulkLead {
                key: target.key,
                snapshot: target.snapshot,
                eta_ms: target.eta_ms,
            })
    };

    choose(true, false, true)
        .or_else(|| {
            (lower_owner.is_none() && allow_liveness_service_failover)
                .then(|| choose(false, false, true))
                .flatten()
        })
        .or_else(|| {
            (lower_owner.is_none())
                .then(|| choose(false, true, false))
                .flatten()
        })
}

pub(crate) fn select_response_lowest_eta_observation<T>(
    targets: &[T],
    avoid_keys: &[CarrierPathKey],
    prefer_avoiding: bool,
) -> Option<ResponsePathObservation>
where
    T: AsRef<ResponsePathObservation>,
{
    targets
        .iter()
        .map(AsRef::as_ref)
        .filter(|target| !prefer_avoiding || !avoid_keys.contains(&target.key))
        .min_by(|left, right| {
            left.eta_ms
                .total_cmp(&right.eta_ms)
                .then_with(|| carrier_path_key_order(left.key, right.key))
        })
        .copied()
}

pub(crate) fn select_response_same_family_sender_evidenced_observation<T>(
    targets: &[T],
    avoid_keys: &[CarrierPathKey],
) -> Option<ResponsePathObservation>
where
    T: AsRef<ResponsePathObservation>,
{
    if avoid_keys.is_empty() {
        return None;
    }
    targets
        .iter()
        .map(AsRef::as_ref)
        .filter(|target| {
            !avoid_keys.contains(&target.key)
                && target.has_sender_evidence
                && avoid_keys
                    .iter()
                    .any(|avoid| avoid.underlay == target.key.underlay)
        })
        .min_by(|left, right| {
            left.eta_ms
                .total_cmp(&right.eta_ms)
                .then_with(|| carrier_path_key_order(left.key, right.key))
        })
        .copied()
}

pub(crate) fn select_response_service_or_proven_observation<T>(
    targets: &[T],
    lower_flights: &[CarrierPathFlightDebt],
    avoid_keys: &[CarrierPathKey],
) -> Option<ResponsePathObservation>
where
    T: AsRef<ResponsePathObservation>,
{
    if let Some(lower_owner) = response_oldest_lower_flight_owner(lower_flights)
        && let Some(target) = targets
            .iter()
            .map(AsRef::as_ref)
            .find(|target| target.key == lower_owner && !avoid_keys.contains(&target.key))
    {
        return Some(*target);
    }
    if let Some(service) = targets
        .iter()
        .map(AsRef::as_ref)
        .find(|target| target.is_service && !avoid_keys.contains(&target.key))
    {
        return Some(*service);
    }
    let choose = |require_proven: bool, prefer_avoiding: bool| {
        targets
            .iter()
            .map(AsRef::as_ref)
            .filter(|target| !require_proven || target.has_bulk_rate_evidence)
            .filter(|target| !prefer_avoiding || !avoid_keys.contains(&target.key))
            .min_by(|left, right| {
                left.eta_ms
                    .total_cmp(&right.eta_ms)
                    .then_with(|| carrier_path_key_order(left.key, right.key))
            })
            .copied()
    };
    choose(true, true)
        .or_else(|| choose(true, false))
        .or_else(|| choose(false, true))
        .or_else(|| choose(false, false))
}

/// Product offset debt attributed to the carrier that owns the oldest range.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CarrierPathFlightDebt {
    pub(crate) key: CarrierPathKey,
    pub(crate) bytes: u64,
}

pub(crate) fn response_ordering_debt_bytes(
    lower_flights: &[CarrierPathFlightDebt],
    candidate: CarrierPathKey,
) -> u64 {
    lower_flights
        .iter()
        .filter_map(|flight| (flight.key != candidate).then_some(flight.bytes))
        .sum()
}

pub(crate) fn response_oldest_lower_flight_owner(
    lower_flights: &[CarrierPathFlightDebt],
) -> Option<CarrierPathKey> {
    lower_flights.first().map(|flight| flight.key)
}

/// Number of response Service owners carried by each underlay family.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ResponseServiceFamilyLoads {
    tcp: u32,
    udp: u32,
}

impl ResponseServiceFamilyLoads {
    #[cfg(test)]
    pub(crate) fn new(tcp: u32, udp: u32) -> Self {
        Self { tcp, udp }
    }

    pub(crate) fn for_underlay(self, underlay: UnderlayProtocol) -> u32 {
        match underlay {
            UnderlayProtocol::Tcp => self.tcp,
            UnderlayProtocol::Udp => self.udp,
        }
    }

    pub(crate) fn needs_diversification(self) -> bool {
        self.tcp.abs_diff(self.udp) >= 2
    }

    /// Load mutation stays explicit so runtime accounting cannot bypass
    /// saturation or depend on this model's representation.
    pub(crate) fn saturating_add_one_for_underlay(&mut self, underlay: UnderlayProtocol) {
        match underlay {
            UnderlayProtocol::Tcp => self.tcp = self.tcp.saturating_add(1),
            UnderlayProtocol::Udp => self.udp = self.udp.saturating_add(1),
        }
    }

    pub(crate) fn saturating_remove_one_for_underlay(&mut self, underlay: UnderlayProtocol) {
        match underlay {
            UnderlayProtocol::Tcp => self.tcp = self.tcp.saturating_sub(1),
            UnderlayProtocol::Udp => self.udp = self.udp.saturating_sub(1),
        }
    }
}

/// Why a response Service owner may move across carrier families.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResponseServiceHandoffMode {
    Diversification,
    PerformanceOverride,
}

pub(crate) fn response_rate_fair_share_bps(
    snapshot: PathSnapshot,
    scope: PathRateScope,
    adds_flow: bool,
) -> f64 {
    let bulk_flows = snapshot
        .active_flows
        .saturating_sub(snapshot.active_latency_sensitive_flows)
        .saturating_add(u32::from(adds_flow))
        .max(1);
    match scope {
        PathRateScope::PerFlowGoodput => snapshot.delivery_rate_bps.max(1.0),
        PathRateScope::PathCapacity => snapshot.delivery_rate_bps.max(1.0) / f64::from(bulk_flows),
    }
}

pub(crate) fn response_service_handoff_mode(
    service_underlay: UnderlayProtocol,
    service_share_bps: f64,
    target_underlay: UnderlayProtocol,
    target_share_bps: f64,
    family_loads: ResponseServiceFamilyLoads,
) -> Option<ResponseServiceHandoffMode> {
    let diversifies = family_loads.for_underlay(service_underlay)
        >= family_loads.for_underlay(target_underlay).saturating_add(2)
        && target_share_bps >= service_share_bps;
    if diversifies {
        return Some(ResponseServiceHandoffMode::Diversification);
    }
    // A 2x gain survives one additional equal-share flow without erasing the
    // improvement, so family balance becomes a preference rather than a veto.
    (target_share_bps >= service_share_bps * 2.0)
        .then_some(ResponseServiceHandoffMode::PerformanceOverride)
}

/// Adapts immutable carrier snapshots to the shared Service-placement model.
/// Concrete TCP and QUIC discovery remain separate; both may ask whether an
/// already measured target makes further optional discovery unnecessary.
pub(crate) fn response_snapshot_handoff_mode(
    service_underlay: UnderlayProtocol,
    service: PathSnapshot,
    target_underlay: UnderlayProtocol,
    target: PathSnapshot,
    family_loads: ResponseServiceFamilyLoads,
) -> Option<ResponseServiceHandoffMode> {
    response_service_handoff_mode(
        service_underlay,
        response_rate_fair_share_bps(service, service.rate_scope, false),
        target_underlay,
        response_rate_fair_share_bps(target, target.rate_scope, true),
        family_loads,
    )
}

// Product offsets are shared across every response carrier, but carrier flight
// is not. Tail arithmetic stays separate from path ranking so a connection-level
// tail cannot silently become a per-path congestion window.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ResponseOrderedTail {
    service_anchor: Option<CarrierPathKey>,
    bytes: u64,
}

impl ResponseOrderedTail {
    pub(crate) fn new(service_anchor: Option<CarrierPathKey>, bytes: usize) -> Self {
        Self {
            service_anchor,
            bytes: u64::try_from(bytes).unwrap_or(u64::MAX),
        }
    }

    pub(crate) fn for_candidate(self, candidate: CarrierPathKey) -> ResponseCandidateTailDebt {
        let external_bytes = if self.bytes > 0 && Some(candidate) != self.service_anchor {
            self.bytes
        } else {
            0
        };
        ResponseCandidateTailDebt {
            global_bytes: self.bytes,
            external_bytes,
        }
    }

    fn projected_union_bytes(self, assigned_bytes: u64, payload_bytes: usize) -> u64 {
        self.bytes
            .max(assigned_bytes)
            .saturating_add(payload_bytes as u64)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ResponseCandidateTailDebt {
    global_bytes: u64,
    external_bytes: u64,
}

impl ResponseCandidateTailDebt {
    pub(crate) fn global_bytes(self) -> u64 {
        self.global_bytes
    }

    // Bulk admission adds the candidate's own product flight. This value must
    // therefore contain only exposure external to that candidate.
    pub(crate) fn external_bytes(self) -> u64 {
        self.external_bytes
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ResponseSameFamilyReservoir {
    service: CarrierPathKey,
    tail: ResponseOrderedTail,
    service_assigned_bytes: u64,
}

impl ResponseSameFamilyReservoir {
    pub(crate) fn new(
        service: CarrierPathKey,
        tail: ResponseOrderedTail,
        service_assigned_bytes: u64,
        protected_service_bytes: usize,
        ordered_reservoir_bytes: usize,
        payload_bytes: usize,
    ) -> Option<Self> {
        let protected_service_bytes = protected_service_bytes as u64;
        if service_assigned_bytes < protected_service_bytes
            || tail.projected_union_bytes(service_assigned_bytes, payload_bytes)
                > ordered_reservoir_bytes as u64
        {
            return None;
        }
        Some(Self {
            service,
            tail,
            service_assigned_bytes,
        })
    }

    pub(crate) fn service(self) -> CarrierPathKey {
        self.service
    }

    pub(crate) fn for_candidate(
        self,
        candidate: CarrierPathKey,
        candidate_owner_bytes: u64,
    ) -> ResponseCandidateTailDebt {
        debug_assert_ne!(candidate, self.service);
        debug_assert_eq!(candidate.underlay, self.service.underlay);
        // The Service horizon is charged once to the global reservoir. The
        // remaining tail and candidate OwnerData are overlapping unique-product
        // views. Repair copies stay outside this subtraction and remain charged
        // by carrier admission.
        let external_bytes = self
            .tail
            .bytes
            .saturating_sub(self.service_assigned_bytes)
            .saturating_sub(candidate_owner_bytes);
        ResponseCandidateTailDebt {
            global_bytes: self.tail.bytes,
            external_bytes,
        }
    }
}

#[cfg(test)]
#[path = "response_test.rs"]
mod tests;
