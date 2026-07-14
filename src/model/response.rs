//! Carrier-neutral response ownership, load, and placement arithmetic.
//!
//! Runtime bindings measure and commit response state. This model keeps the
//! typed evidence and pure calculations shared by those transactions and the
//! sender planner; it owns no locks, channels, timers, or carrier handles.

use crate::model::path::CarrierPathKey;
use crate::protocol::UnderlayProtocol;
use crate::scheduler::{PathRateScope, PathSnapshot};

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
