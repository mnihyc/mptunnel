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
    assert_eq!(candidate_flight + external, 512 * 1024);
}

#[test]
fn same_family_reservoir_keeps_global_ordered_cap_authoritative() {
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
