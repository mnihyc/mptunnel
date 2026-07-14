use super::super::session::ServerPathLaneTracker;
use super::super::test_support::test_quic_capacity_proof;
use super::super::topology::next_server_carrier_path_instance_id;
use crate::model::capacity::QuicCapacityProofCandidate;
use crate::model::path::CarrierPathKey;
use crate::model::response::ResponseServiceFamilyLoads;
use crate::mux::MuxLimits;
use crate::protocol::{PathId, SessionId, UnderlayProtocol};
use crate::scheduler::FlowLane;
use std::time::{Duration, Instant};

#[test]
fn response_service_handoff_drain_is_session_serialized() {
    let tracker = ServerPathLaneTracker::default();
    let session_id = SessionId(513);
    let service = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let target = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let service_instance = next_server_carrier_path_instance_id();
    let target_instance = next_server_carrier_path_instance_id();
    tracker.set_response_flow_active(session_id, true);
    tracker.set_response_flow_active(session_id, true);
    tracker.attach_response_service(session_id, service, FlowLane::Throughput);
    tracker.attach_response_service(session_id, service, FlowLane::Throughput);

    let generation = tracker.generation(session_id);
    assert!(tracker.try_reserve_response_service_handoff_drain(
        session_id,
        generation,
        1,
        service,
        service_instance,
        10,
        target,
        target_instance,
        20,
        None,
        Instant::now() + Duration::from_secs(1),
    ));
    let reserved = tracker.response_scheduling_snapshot(session_id);
    assert!(!tracker.try_reserve_response_service_handoff_drain(
        session_id,
        reserved.generation,
        2,
        service,
        service_instance,
        11,
        target,
        target_instance,
        21,
        None,
        Instant::now() + Duration::from_secs(1),
    ));
    assert!(!tracker.clear_response_service_handoff_drain_for_binding(session_id, 2));
    assert!(tracker.clear_response_service_handoff_drain_for_binding(session_id, 1));
}

#[test]
fn expired_response_service_handoff_drain_rejects_move_without_changing_loads() {
    let tracker = ServerPathLaneTracker::default();
    let session_id = SessionId(514);
    let service = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let target = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let service_instance = next_server_carrier_path_instance_id();
    let target_instance = next_server_carrier_path_instance_id();
    tracker.set_response_flow_active(session_id, true);
    tracker.set_response_flow_active(session_id, true);
    tracker.attach_response_service(session_id, service, FlowLane::Throughput);
    tracker.attach_response_service(session_id, service, FlowLane::Throughput);

    assert!(tracker.try_reserve_response_service_handoff_drain(
        session_id,
        tracker.generation(session_id),
        1,
        service,
        service_instance,
        10,
        target,
        target_instance,
        20,
        None,
        Instant::now() + Duration::from_secs(1),
    ));
    let reserved = tracker.response_scheduling_snapshot(session_id);
    let move_generation = reserved.generation;
    assert!(
        tracker.set_response_service_handoff_drain_expiry_for_test(
            session_id,
            reserved
                .response_service_handoff_drain
                .expect("reserved handoff drain"),
            Instant::now() - Duration::from_millis(1),
        )
    );

    assert!(!tracker.try_move_response_service_handoff(
        session_id,
        move_generation,
        1,
        service,
        service_instance,
        10,
        target,
        target_instance,
        20,
        None,
        FlowLane::Throughput,
    ));
    let scheduling = tracker.response_scheduling_snapshot(session_id);
    assert_eq!(
        scheduling.service_family_loads,
        ResponseServiceFamilyLoads::new(2, 0)
    );
    assert!(scheduling.response_service_handoff_drain.is_none());
    assert_eq!(
        tracker
            .response_service_snapshot(session_id, service)
            .active_flows,
        2
    );
    assert_eq!(
        tracker
            .response_service_snapshot(session_id, target)
            .active_flows,
        0
    );
}

#[test]
fn direct_response_service_handoff_rejects_proof_that_expired_before_atomic_move() {
    let tracker = ServerPathLaneTracker::default();
    let session_id = SessionId(518);
    let service = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let target = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let service_instance = next_server_carrier_path_instance_id();
    let target_instance = next_server_carrier_path_instance_id();
    tracker.set_response_flow_active(session_id, true);
    tracker.set_response_flow_active(session_id, true);
    tracker.attach_response_service(session_id, service, FlowLane::Throughput);
    tracker.attach_response_service(session_id, service, FlowLane::Throughput);
    let mut proof = test_quic_capacity_proof(MuxLimits::default(), 518, Duration::from_secs(1));
    proof.accepted_at = Instant::now() - Duration::from_secs(2);
    proof.expires_at = proof.accepted_at + proof.proof_validity;

    assert!(!tracker.try_move_response_service_handoff(
        session_id,
        tracker.generation(session_id),
        1,
        service,
        service_instance,
        10,
        target,
        target_instance,
        20,
        Some(proof),
        FlowLane::Throughput,
    ));
    let scheduling = tracker.response_scheduling_snapshot(session_id);
    assert_eq!(
        scheduling.service_family_loads,
        ResponseServiceFamilyLoads::new(2, 0)
    );
    assert_eq!(
        tracker
            .response_service_snapshot(session_id, service)
            .active_flows,
        2
    );
    assert_eq!(
        tracker
            .response_service_snapshot(session_id, target)
            .active_flows,
        0
    );
}

#[test]
fn response_service_handoff_drain_requires_every_reserved_identity() {
    let tracker = ServerPathLaneTracker::default();
    let session_id = SessionId(515);
    let service = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let target = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let service_instance = next_server_carrier_path_instance_id();
    let target_instance = next_server_carrier_path_instance_id();
    tracker.set_response_flow_active(session_id, true);
    tracker.set_response_flow_active(session_id, true);
    tracker.attach_response_service(session_id, service, FlowLane::Throughput);
    tracker.attach_response_service(session_id, service, FlowLane::Throughput);
    let proof = test_quic_capacity_proof(MuxLimits::default(), 515, Duration::from_secs(1));

    assert!(tracker.try_reserve_response_service_handoff_drain(
        session_id,
        tracker.generation(session_id),
        1,
        service,
        service_instance,
        10,
        target,
        target_instance,
        20,
        Some(proof),
        Instant::now() + Duration::from_secs(1),
    ));
    let generation = tracker.generation(session_id);
    let wrong_service_instance = next_server_carrier_path_instance_id();
    let wrong_target_instance = next_server_carrier_path_instance_id();

    for (binding, from_instance, from_incarnation, to_instance, to_incarnation) in [
        (2, service_instance, 10, target_instance, 20),
        (1, wrong_service_instance, 10, target_instance, 20),
        (1, service_instance, 11, target_instance, 20),
        (1, service_instance, 10, wrong_target_instance, 20),
        (1, service_instance, 10, target_instance, 21),
    ] {
        assert!(!tracker.try_move_response_service_handoff(
            session_id,
            generation,
            binding,
            service,
            from_instance,
            from_incarnation,
            target,
            to_instance,
            to_incarnation,
            Some(proof),
            FlowLane::Throughput,
        ));
    }
    assert!(!tracker.try_move_response_service_handoff(
        session_id,
        generation,
        1,
        service,
        service_instance,
        10,
        target,
        target_instance,
        20,
        Some(QuicCapacityProofCandidate {
            token: proof.token.wrapping_add(1),
            ..proof
        }),
        FlowLane::Throughput,
    ));

    let scheduling = tracker.response_scheduling_snapshot(session_id);
    assert_eq!(
        scheduling.service_family_loads,
        ResponseServiceFamilyLoads::new(2, 0)
    );
    assert_eq!(
        scheduling
            .response_service_handoff_drain
            .expect("identity mismatch must preserve drain")
            .binding_instance_id,
        1
    );
}

#[test]
fn matching_response_service_handoff_drain_moves_one_flow_and_is_consumed() {
    let tracker = ServerPathLaneTracker::default();
    let session_id = SessionId(516);
    let service = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let target = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let service_instance = next_server_carrier_path_instance_id();
    let target_instance = next_server_carrier_path_instance_id();
    tracker.set_response_flow_active(session_id, true);
    tracker.set_response_flow_active(session_id, true);
    tracker.attach_response_service(session_id, service, FlowLane::Throughput);
    tracker.attach_response_service(session_id, service, FlowLane::Throughput);

    assert!(tracker.try_reserve_response_service_handoff_drain(
        session_id,
        tracker.generation(session_id),
        1,
        service,
        service_instance,
        10,
        target,
        target_instance,
        20,
        None,
        Instant::now() + Duration::from_secs(1),
    ));
    assert!(tracker.try_move_response_service_handoff(
        session_id,
        tracker.generation(session_id),
        1,
        service,
        service_instance,
        10,
        target,
        target_instance,
        20,
        None,
        FlowLane::Throughput,
    ));

    let scheduling = tracker.response_scheduling_snapshot(session_id);
    assert_eq!(
        scheduling.service_family_loads,
        ResponseServiceFamilyLoads::new(1, 1)
    );
    assert!(scheduling.response_service_handoff_drain.is_none());
    assert_eq!(
        tracker
            .response_service_snapshot(session_id, service)
            .active_flows,
        1
    );
    assert_eq!(
        tracker
            .response_service_snapshot(session_id, target)
            .active_flows,
        1
    );
}

#[test]
fn clearing_response_service_handoff_drain_requires_exact_target_path() {
    let tracker = ServerPathLaneTracker::default();
    let session_id = SessionId(517);
    let service = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let target = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let service_instance = next_server_carrier_path_instance_id();
    let target_instance = next_server_carrier_path_instance_id();
    tracker.set_response_flow_active(session_id, true);
    tracker.set_response_flow_active(session_id, true);
    tracker.attach_response_service(session_id, service, FlowLane::Throughput);
    tracker.attach_response_service(session_id, service, FlowLane::Throughput);

    assert!(tracker.try_reserve_response_service_handoff_drain(
        session_id,
        tracker.generation(session_id),
        1,
        service,
        service_instance,
        10,
        target,
        target_instance,
        20,
        None,
        Instant::now() + Duration::from_secs(1),
    ));
    assert!(!tracker.clear_response_service_handoff_drain_for_path(
        session_id,
        2,
        target,
        target_instance,
    ));
    assert!(!tracker.clear_response_service_handoff_drain_for_path(
        session_id,
        1,
        target,
        next_server_carrier_path_instance_id(),
    ));
    assert!(
        tracker
            .response_scheduling_snapshot(session_id)
            .response_service_handoff_drain
            .is_some()
    );
    assert!(tracker.clear_response_service_handoff_drain_for_path(
        session_id,
        1,
        target,
        target_instance,
    ));
    let scheduling = tracker.response_scheduling_snapshot(session_id);
    assert!(scheduling.response_service_handoff_drain.is_none());
    assert_eq!(
        scheduling.service_family_loads,
        ResponseServiceFamilyLoads::new(2, 0)
    );
}
