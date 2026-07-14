//! Response-stream ordering debt and same-family reservoir arithmetic.

use crate::model::path::CarrierPathKey;

// Product offsets are shared across every response carrier, but carrier flight
// is not. This module keeps that ownership arithmetic separate from path
// ranking so a connection-level tail cannot silently become a per-path cwnd.

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
#[path = "response_ownership_test.rs"]
mod tests;
