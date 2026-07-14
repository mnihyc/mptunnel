use super::*;
use crate::protocol::{PathId, StreamOpenRole, UnderlayProtocol};
use crate::scheduler::{PathRateScope, PathSnapshot};

fn key(path_id: u16) -> CarrierPathKey {
    CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(path_id),
    }
}

fn observation(
    path_id: u16,
    underlay: UnderlayProtocol,
    delivery_rate_bps: f64,
    rate_scope: PathRateScope,
    active_flows: u32,
    is_service: bool,
) -> ResponsePathObservation {
    let mut snapshot = PathSnapshot::new(PathId(path_id), underlay, 20.0, delivery_rate_bps);
    snapshot.rate_scope = rate_scope;
    snapshot.active_flows = active_flows;
    ResponsePathObservation {
        key: CarrierPathKey {
            underlay,
            path_id: PathId(path_id),
        },
        path_instance_id: CarrierPathInstanceId::from_raw(u64::from(path_id) + 1),
        incarnation: u64::from(path_id) + 1,
        attachment_role: if is_service {
            StreamOpenRole::Active
        } else {
            StreamOpenRole::Validation
        },
        snapshot,
        owner_data_in_flight_bytes: 0,
        command_pending_bytes: 0,
        eta_ms: 20.0,
        is_service,
        is_request_active: false,
        has_sender_evidence: true,
        has_service_feed_evidence: true,
        has_bulk_rate_evidence: true,
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

#[test]
fn balanced_service_handoff_requires_two_x_projected_gain() {
    let mut service = observation(
        0,
        UnderlayProtocol::Tcp,
        60_000_000.0,
        PathRateScope::PerFlowGoodput,
        0,
        true,
    );
    let target = observation(
        1,
        UnderlayProtocol::Udp,
        100_000_000.0,
        PathRateScope::PathCapacity,
        0,
        false,
    );
    let balanced = ResponseServiceFamilyLoads::new(1, 1);

    assert_eq!(
        response_service_handoff_mode_for_observations(&service, &target, balanced),
        None,
        "a modest gain must not churn sticky Service ownership",
    );
    service.snapshot.delivery_rate_bps = 50_000_000.0;
    assert_eq!(
        response_service_handoff_mode_for_observations(&service, &target, balanced),
        Some(ResponseServiceHandoffMode::PerformanceOverride),
    );
}

#[test]
fn service_handoff_fair_share_respects_rate_scope() {
    let mut service = observation(
        0,
        UnderlayProtocol::Tcp,
        100_000_000.0,
        PathRateScope::PathCapacity,
        2,
        true,
    );
    let target = observation(
        1,
        UnderlayProtocol::Udp,
        80_000_000.0,
        PathRateScope::PathCapacity,
        0,
        false,
    );

    assert!(response_service_handoff_preserves_fair_share(
        &service, &target,
    ));
    service.snapshot.rate_scope = PathRateScope::PerFlowGoodput;
    assert!(
        !response_service_handoff_preserves_fair_share(&service, &target),
        "a per-flow TCP observation must not be divided a second time",
    );
}
