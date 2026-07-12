use super::CarrierPathKey;

// Product offsets are shared across every response carrier, but carrier flight
// is not. This module keeps that ownership arithmetic separate from path
// ranking so a connection-level tail cannot silently become a per-path cwnd.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ResponseOrderedTail {
    service_anchor: Option<CarrierPathKey>,
    bytes: u64,
}

impl ResponseOrderedTail {
    pub(super) fn new(service_anchor: Option<CarrierPathKey>, bytes: usize) -> Self {
        Self {
            service_anchor,
            bytes: u64::try_from(bytes).unwrap_or(u64::MAX),
        }
    }

    pub(super) fn for_candidate(self, candidate: CarrierPathKey) -> ResponseCandidateTailDebt {
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
pub(super) struct ResponseCandidateTailDebt {
    global_bytes: u64,
    external_bytes: u64,
}

impl ResponseCandidateTailDebt {
    pub(super) fn global_bytes(self) -> u64 {
        self.global_bytes
    }

    // Bulk admission adds the candidate's own product flight. This value must
    // therefore contain only exposure external to that candidate.
    pub(super) fn external_bytes(self) -> u64 {
        self.external_bytes
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ResponseSameFamilyReservoir {
    service: CarrierPathKey,
    tail: ResponseOrderedTail,
    protected_service_bytes: u64,
}

impl ResponseSameFamilyReservoir {
    pub(super) fn new(
        service: CarrierPathKey,
        tail: ResponseOrderedTail,
        service_assigned_bytes: u64,
        protected_service_bytes: usize,
        feed_reservoir_bytes: usize,
        payload_bytes: usize,
    ) -> Option<Self> {
        let protected_service_bytes = protected_service_bytes as u64;
        if service_assigned_bytes < protected_service_bytes
            || tail.projected_union_bytes(service_assigned_bytes, payload_bytes)
                > feed_reservoir_bytes as u64
        {
            return None;
        }
        Some(Self {
            service,
            tail,
            protected_service_bytes,
        })
    }

    pub(super) fn service(self) -> CarrierPathKey {
        self.service
    }

    pub(super) fn for_candidate(
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
            .saturating_sub(self.protected_service_bytes)
            .saturating_sub(candidate_owner_bytes);
        ResponseCandidateTailDebt {
            global_bytes: self.tail.bytes,
            external_bytes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{PathId, UnderlayProtocol};

    fn key(path_id: u16) -> CarrierPathKey {
        CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(path_id),
        }
    }

    #[test]
    fn ordered_tail_is_global_but_external_only_for_an_alternate() {
        let service = key(0);
        let tail = ResponseOrderedTail::new(Some(service), 2 * 1024 * 1024);

        assert_eq!(tail.for_candidate(service).global_bytes(), 2 * 1024 * 1024);
        assert_eq!(tail.for_candidate(service).external_bytes(), 0);
        assert_eq!(tail.for_candidate(key(1)).external_bytes(), 2 * 1024 * 1024);
    }

    #[test]
    fn same_family_reservoir_partitions_candidate_flight_by_union() {
        let service = key(0);
        let candidate = key(1);
        let horizon = 2 * 1024 * 1024;
        let tail = ResponseOrderedTail::new(Some(service), horizon + 512 * 1024);
        let reservoir = ResponseSameFamilyReservoir::new(
            service,
            tail,
            horizon as u64,
            horizon,
            4 * 1024 * 1024,
            64 * 1024,
        )
        .expect("global reservoir has credit");

        let candidate_flight = 128 * 1024;
        let external = reservoir
            .for_candidate(candidate, candidate_flight)
            .external_bytes();
        assert_eq!(candidate_flight as u64 + external, 512 * 1024);
    }

    #[test]
    fn same_family_reservoir_keeps_global_feed_cap_authoritative() {
        let service = key(0);
        let tail = ResponseOrderedTail::new(Some(service), 4 * 1024 * 1024);

        assert!(
            ResponseSameFamilyReservoir::new(
                service,
                tail,
                2 * 1024 * 1024,
                2 * 1024 * 1024,
                4 * 1024 * 1024,
                64 * 1024,
            )
            .is_none()
        );
    }
}
