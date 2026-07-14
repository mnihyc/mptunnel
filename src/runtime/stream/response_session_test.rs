use super::*;
use crate::protocol::PathId;
use std::sync::Arc;
use std::time::Duration;

fn consume_capacity_attempt(
    tracker: &ServerPathLaneTracker,
    session_id: SessionId,
    path: CarrierPathKey,
    path_instance_id: ServerCarrierPathInstanceId,
    token: u64,
) {
    let binding_instance_id = token;
    assert!(tracker.try_reserve_test_quic_capacity_calibration(
        session_id,
        tracker.generation(session_id),
        binding_instance_id,
        path,
        path_instance_id,
        1,
        16,
        token,
    ));
    assert!(tracker.commit_test_quic_capacity_calibration(
        session_id,
        binding_instance_id,
        path,
        path_instance_id,
        Duration::from_secs(1),
        token,
    ));
    assert!(tracker.complete_test_quic_capacity_calibration(
        session_id,
        binding_instance_id,
        path,
        path_instance_id,
    ));
}

#[test]
fn tcp_capacity_probe_serializes_the_session_and_quic_discovery() {
    let tracker = Arc::new(ServerPathLaneTracker::default());
    let session_id = SessionId(603);
    let path = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(4),
    };
    let path_instance_id = super::super::next_server_carrier_path_instance_id();
    tracker.set_response_flow_active(session_id, true);
    tracker.set_response_flow_active(session_id, true);

    let lease = tracker
        .try_reserve_tcp_capacity_probe(session_id, tracker.generation(session_id))
        .expect("reserve first TCP probe");
    assert!(
        tracker
            .response_scheduling_snapshot(session_id)
            .tcp_capacity_probe_reserved
    );
    assert!(
        tracker
            .try_reserve_tcp_capacity_probe(session_id, tracker.generation(session_id))
            .is_none(),
        "a second TCP carrier must not overlap the session probe"
    );
    assert!(
        !tracker.try_reserve_test_quic_capacity_calibration(
            session_id,
            tracker.generation(session_id),
            1,
            path,
            path_instance_id,
            1,
            16,
            1,
        ),
        "TCP and QUIC discovery share product-session serialization"
    );

    tracker.set_response_flow_active(session_id, false);
    tracker.set_response_flow_active(session_id, false);
    assert!(
        tracker
            .response_scheduling_snapshot(session_id)
            .tcp_capacity_probe_reserved,
        "session reclaim must retain the live typed lease"
    );
    assert!(
        tracker
            .try_reserve_tcp_capacity_probe(session_id, tracker.generation(session_id))
            .is_none(),
        "a detached session cannot replace a still-live reservation"
    );

    drop(lease);
    assert!(
        !tracker
            .response_scheduling_snapshot(session_id)
            .tcp_capacity_probe_reserved
    );
    assert!(
        tracker
            .try_reserve_tcp_capacity_probe(session_id, tracker.generation(session_id))
            .is_some(),
        "dropping the typed command lease reopens discovery"
    );
}

fn assert_same_scheduling_snapshot(
    actual: ServerResponsePathSchedulingSnapshot,
    expected: ServerResponsePathSchedulingSnapshot,
) {
    assert_eq!(
        actual.path_load.active_flows,
        expected.path_load.active_flows
    );
    assert_eq!(
        actual.path_load.active_latency_sensitive_flows,
        expected.path_load.active_latency_sensitive_flows
    );
    assert_eq!(
        actual.session_load.active_flows,
        expected.session_load.active_flows
    );
    assert_eq!(
        actual.session_load.active_latency_sensitive_flows,
        expected.session_load.active_latency_sensitive_flows
    );
    assert_eq!(
        actual.quic_capacity_calibration_attempts,
        expected.quic_capacity_calibration_attempts
    );
}

#[test]
fn batch_response_path_snapshots_match_single_output_reads() {
    let tracker = ServerPathLaneTracker::default();
    let session_id = SessionId(601);
    let first_path = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let second_path = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(2),
    };
    let first_instance = super::super::next_server_carrier_path_instance_id();
    let replacement_instance = super::super::next_server_carrier_path_instance_id();
    let second_instance = super::super::next_server_carrier_path_instance_id();

    tracker.set_response_flow_active(session_id, true);
    tracker.set_response_flow_active(session_id, true);
    tracker.attach_response_service(session_id, first_path, FlowLane::Throughput);
    tracker.attach_response_service(session_id, first_path, FlowLane::Throughput);
    tracker.attach_response_service(session_id, second_path, FlowLane::Latency);
    tracker.attach_realtime_flow(session_id);
    consume_capacity_attempt(&tracker, session_id, first_path, first_instance, 1);
    consume_capacity_attempt(&tracker, session_id, second_path, second_instance, 2);
    consume_capacity_attempt(&tracker, session_id, second_path, second_instance, 3);

    let output_keys = [
        (first_path, first_instance),
        (second_path, second_instance),
        (first_path, replacement_instance),
    ];
    let batch = tracker.response_path_scheduling_snapshots(session_id, output_keys);
    assert_eq!(batch.len(), output_keys.len());
    for (actual, (path, path_instance_id)) in batch.iter().copied().zip(output_keys) {
        assert_same_scheduling_snapshot(
            actual,
            tracker.response_path_scheduling_snapshot(session_id, path, path_instance_id),
        );
    }

    assert_eq!(batch[0].path_load.active_flows, 2);
    assert_eq!(batch[0].path_load.active_latency_sensitive_flows, 0);
    assert_eq!(batch[1].path_load.active_flows, 1);
    assert_eq!(batch[1].path_load.active_latency_sensitive_flows, 1);
    for snapshot in &batch {
        assert_eq!(snapshot.session_load.active_flows, 4);
        assert_eq!(snapshot.session_load.active_latency_sensitive_flows, 2);
    }
    assert_eq!(batch[0].quic_capacity_calibration_attempts, 1);
    assert_eq!(batch[1].quic_capacity_calibration_attempts, 2);
    assert_eq!(
        batch[2].quic_capacity_calibration_attempts, 0,
        "attempts belong to one exact carrier instance"
    );
}
