use super::super::response_session::ServerPathLaneTracker;
use super::valid_quic_capacity_geometry;
use crate::model::path::CarrierPathKey;
use crate::protocol::{PathId, SessionId, UnderlayProtocol};
use crate::runtime::path::commands::{
    QuicCapacityProbeCommandResolution, QuicCapacityProbeCommandTicket,
};
use crate::runtime::path::quic::metrics::QuicCapacityProofCandidate;
use std::sync::Arc;
use std::time::{Duration, Instant};

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
    assert!(tracker.commit_test_quic_capacity_calibration(
        session_id,
        92,
        path,
        path_instance_id,
        Duration::from_secs(1),
        18,
    ));
    assert!(tracker.set_quic_capacity_active_expiry_for_test(
        session_id,
        92,
        path,
        path_instance_id,
        18,
        Instant::now() - Duration::from_millis(1),
    ));
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

    let state = tracker.hold_state_lock_for_test();
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
