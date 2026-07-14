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

#[test]
fn quic_capacity_proof_is_spec_exact_and_stays_reserved_until_publish_commit() {
    let tracker = ServerPathLaneTracker::default();
    let session_id = SessionId(602);
    let path = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(3),
    };
    let path_instance_id = super::super::next_server_carrier_path_instance_id();
    let binding_instance_id = 91;
    let token = 17;
    let command_ticket = QuicCapacityProbeCommandTicket::new();
    tracker.set_response_flow_active(session_id, true);
    tracker.set_response_flow_active(session_id, true);
    assert!(tracker.try_reserve_quic_capacity_calibration(
        session_id,
        tracker.generation(session_id),
        binding_instance_id,
        path,
        path_instance_id,
        838,
        500,
        62,
        400,
        438,
        Duration::from_secs(3),
        4_096,
        token,
        command_ticket.clone(),
    ));
    assert!(tracker.commit_quic_capacity_calibration(
        session_id,
        binding_instance_id,
        path,
        path_instance_id,
        Instant::now() + Duration::from_secs(1),
        token,
    ));

    let accepted_at = Instant::now();
    let candidate = QuicCapacityProofCandidate {
        token,
        train_bytes: 838,
        sample_floor_bytes: 500,
        accounting_slack_bytes: 62,
        warmup_bytes: 400,
        required_proof_bytes: 438,
        written_bytes: 838,
        written_data_frame_count: 8,
        receipt_confirmed: true,
        received_bytes: 838,
        proof_elapsed: Duration::from_millis(10),
        rate_bps: 670_400,
        accepted_at,
        expires_at: accepted_at + Duration::from_secs(3),
        proof_validity: Duration::from_secs(3),
    };
    assert!(
        tracker
            .try_accept_quic_capacity_proof(
                session_id,
                path,
                path_instance_id,
                QuicCapacityProofCandidate {
                    required_proof_bytes: 437,
                    ..candidate
                },
            )
            .is_none(),
        "carrier evidence cannot weaken the frozen proof floor"
    );
    let ticket = tracker
        .try_accept_quic_capacity_proof(session_id, path, path_instance_id, candidate)
        .expect("exact capacity proof");
    assert!(
        tracker
            .response_scheduling_snapshot(session_id)
            .quic_capacity_calibration_reserved,
        "publication must finish before another probe can start"
    );
    assert_eq!(
        tracker.commit_quic_capacity_proof(ticket),
        Some(binding_instance_id)
    );
    assert_eq!(
        command_ticket.resolution(),
        QuicCapacityProbeCommandResolution::Current
    );
    assert!(
        tracker
            .response_scheduling_snapshot(session_id)
            .quic_capacity_calibration_reserved,
        "committed proof remains a barrier until publication finishes"
    );
    assert_eq!(
        tracker.finish_quic_capacity_proof_publication(ticket),
        Some(binding_instance_id)
    );
    assert_eq!(
        command_ticket.resolution(),
        QuicCapacityProbeCommandResolution::Published,
        "registry publication wakes carrier cleanup without cancelling it"
    );
    assert!(
        !tracker
            .response_scheduling_snapshot(session_id)
            .quic_capacity_calibration_reserved,
    );
}

#[test]
fn quic_capacity_evidence_after_active_lease_expires_instead_of_completing() {
    let tracker = ServerPathLaneTracker::default();
    let session_id = SessionId(603);
    let path = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(4),
    };
    let path_instance_id = super::super::next_server_carrier_path_instance_id();
    tracker.set_response_flow_active(session_id, true);
    tracker.set_response_flow_active(session_id, true);
    assert!(tracker.try_reserve_test_quic_capacity_calibration(
        session_id,
        tracker.generation(session_id),
        92,
        path,
        path_instance_id,
        100,
        1_000,
        18,
    ));
    tracker
        .state
        .lock()
        .expect("server path lane tracker lock")
        .quic_capacity_calibrations
        .get_mut(&session_id)
        .expect("capacity reservation")
        .phase = ServerQuicCapacityCalibrationPhase::Active {
        expires_at: Instant::now() - Duration::from_millis(1),
    };
    let accepted_at = Instant::now();
    let candidate = QuicCapacityProofCandidate {
        token: 18,
        train_bytes: 100,
        sample_floor_bytes: 100,
        accounting_slack_bytes: 12,
        warmup_bytes: 0,
        required_proof_bytes: 88,
        written_bytes: 100,
        written_data_frame_count: 1,
        receipt_confirmed: true,
        received_bytes: 100,
        proof_elapsed: Duration::from_millis(1),
        rate_bps: 800_000,
        accepted_at,
        expires_at: accepted_at + Duration::from_secs(1),
        proof_validity: Duration::from_secs(1),
    };
    assert!(
        tracker
            .try_accept_quic_capacity_proof(session_id, path, path_instance_id, candidate,)
            .is_none()
    );
    assert!(
        !tracker
            .response_scheduling_snapshot(session_id)
            .quic_capacity_calibration_reserved
    );
}

#[test]
fn quic_capacity_geometry_freezes_policy_slack_and_exact_train() {
    assert!(valid_quic_capacity_geometry(838, 500, 62, 400, 438));
    assert!(
        !valid_quic_capacity_geometry(838, 500, 0, 400, 500),
        "zero slack may not weaken or rewrite the planner policy"
    );
    assert!(
        !valid_quic_capacity_geometry(838, 500, 62, 400, 1),
        "one byte cannot satisfy a representative sample floor"
    );
    assert!(
        !valid_quic_capacity_geometry(900, 500, 62, 400, 438),
        "the declared train must equal the frozen planner geometry"
    );
}

#[test]
fn quic_capacity_commit_rechecks_deadline_after_lane_lock_wait() {
    let tracker = Arc::new(ServerPathLaneTracker::default());
    let session_id = SessionId(604);
    let path = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(5),
    };
    let path_instance_id = super::super::next_server_carrier_path_instance_id();
    tracker.set_response_flow_active(session_id, true);
    tracker.set_response_flow_active(session_id, true);
    assert!(tracker.try_reserve_test_quic_capacity_calibration(
        session_id,
        tracker.generation(session_id),
        93,
        path,
        path_instance_id,
        100,
        1_000,
        19,
    ));

    let state = tracker.state.lock().expect("hold lane tracker lock");
    let blocked = tracker.clone();
    let expires_at = Instant::now() + Duration::from_millis(30);
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let commit = std::thread::spawn(move || {
        started_tx.send(()).expect("signal blocked commit");
        blocked.commit_quic_capacity_calibration(
            session_id,
            93,
            path,
            path_instance_id,
            expires_at,
            19,
        )
    });
    started_rx.recv().expect("blocked commit started");
    std::thread::sleep(Duration::from_millis(50));
    drop(state);
    assert!(!commit.join().expect("deadline test thread"));
}

#[test]
fn invalidated_capacity_command_releases_serialization_without_refund() {
    let tracker = ServerPathLaneTracker::default();
    let session_id = SessionId(605);
    let path = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(6),
    };
    let path_instance_id = super::super::next_server_carrier_path_instance_id();
    let ticket = QuicCapacityProbeCommandTicket::new();
    tracker.set_response_flow_active(session_id, true);
    tracker.set_response_flow_active(session_id, true);
    assert!(tracker.try_reserve_quic_capacity_calibration(
        session_id,
        tracker.generation(session_id),
        94,
        path,
        path_instance_id,
        100,
        100,
        12,
        0,
        88,
        Duration::from_secs(1),
        1_000,
        20,
        ticket.clone(),
    ));
    assert!(tracker.commit_test_quic_capacity_calibration(
        session_id,
        94,
        path,
        path_instance_id,
        Duration::from_secs(1),
        20,
    ));
    ticket.cancel();
    let scheduling = tracker.response_scheduling_snapshot(session_id);
    assert!(!scheduling.quic_capacity_calibration_reserved);
    assert_eq!(scheduling.quic_capacity_calibration_spent_bytes, 100);
    assert_eq!(
        tracker
            .response_path_scheduling_snapshot(session_id, path, path_instance_id)
            .quic_capacity_calibration_attempts,
        1
    );
}

#[test]
fn session_reference_preserves_capacity_budget_across_zero_flow_gap() {
    let tracker = ServerPathLaneTracker::default();
    let session_id = SessionId(606);
    let path = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(7),
    };
    let path_instance_id = super::super::next_server_carrier_path_instance_id();
    tracker.attach_session(session_id);
    tracker.set_response_flow_active(session_id, true);
    tracker.set_response_flow_active(session_id, true);
    assert!(tracker.try_reserve_test_quic_capacity_calibration(
        session_id,
        tracker.generation(session_id),
        95,
        path,
        path_instance_id,
        100,
        1_000,
        21,
    ));
    assert!(tracker.commit_test_quic_capacity_calibration(
        session_id,
        95,
        path,
        path_instance_id,
        Duration::from_secs(1),
        21,
    ));
    assert!(tracker.cancel_quic_capacity_calibration(
        session_id,
        95,
        path,
        path_instance_id,
        "test_cancelled",
    ));
    tracker.set_response_flow_active(session_id, false);
    tracker.set_response_flow_active(session_id, false);
    assert_eq!(
        tracker
            .response_scheduling_snapshot(session_id)
            .quic_capacity_calibration_spent_bytes,
        100,
        "a live carrier session reference keeps cumulative spend"
    );
    tracker.detach_session(session_id);
    assert_eq!(
        tracker
            .response_scheduling_snapshot(session_id)
            .quic_capacity_calibration_spent_bytes,
        0
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
