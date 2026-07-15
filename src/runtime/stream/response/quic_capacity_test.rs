use super::super::ResponseStreamBinding;
use super::super::attachment::{ResponseStreamAttachOutcome, next_server_carrier_path_instance_id};
use super::super::session::ServerPathLaneTracker;
use super::ServerQuicCapacityHistorySnapshot;
use crate::model::capacity::QuicCapacityProofCandidate;
use crate::model::path::CarrierPathKey;
use crate::mux::MuxLimits;
use crate::protocol::{PathId, SessionId, StreamOpenRole, UnderlayProtocol};
use crate::runtime::path::commands::{
    CapacityProbeCommandResolution, CapacityProbeCommandTicket, reliable_path_command_channels,
};
use crate::scheduler::FlowLane;
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
    let command_ticket = CapacityProbeCommandTicket::new();
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
        CapacityProbeCommandResolution::Current
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
        CapacityProbeCommandResolution::Published,
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
    let ticket = CapacityProbeCommandTicket::new();
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
    let invalidated = tracker.response_scheduling_snapshot(session_id);
    assert!(invalidated.quic_capacity_calibration_reserved);
    assert!(invalidated.operation_maintenance_due);
    assert!(tracker.maintain_response_session_operations(session_id));
    let scheduling = tracker.response_scheduling_snapshot(session_id);
    assert!(!scheduling.quic_capacity_calibration_reserved);
    assert!(!scheduling.operation_maintenance_due);
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

#[test]
fn quic_capacity_reservation_expires_and_completion_releases_probe_slot() {
    let tracker = ServerPathLaneTracker::default();
    let session_id = SessionId(510);
    let path = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(9),
    };
    let binding_instance_id = 77;
    let path_instance_id = next_server_carrier_path_instance_id();
    let train_bytes = 100;
    let session_byte_limit = 1_000;
    tracker.attach_session(session_id);
    tracker.set_response_flow_active(session_id, true);
    tracker.set_response_flow_active(session_id, true);

    let first_generation = tracker.generation(session_id);
    assert!(tracker.try_reserve_test_quic_capacity_calibration(
        session_id,
        first_generation,
        binding_instance_id,
        path,
        path_instance_id,
        train_bytes,
        session_byte_limit,
        1,
    ));
    assert!(tracker.commit_test_quic_capacity_calibration(
        session_id,
        binding_instance_id,
        path,
        path_instance_id,
        Duration::from_secs(1),
        1,
    ));
    tracker.clear_quic_capacity_calibration(
        session_id,
        binding_instance_id + 1,
        path,
        path_instance_id,
    );
    assert!(
        tracker
            .response_scheduling_snapshot(session_id)
            .quic_capacity_calibration_reserved,
        "an unrelated binding on the shared carrier path cannot clear the lease"
    );
    assert!(tracker.set_quic_capacity_active_expiry_for_test(
        session_id,
        binding_instance_id,
        path,
        path_instance_id,
        1,
        Instant::now() - Duration::from_millis(1),
    ));
    let observed_expiry = tracker.response_scheduling_snapshot(session_id);
    assert!(observed_expiry.quic_capacity_calibration_reserved);
    assert!(observed_expiry.operation_maintenance_due);
    assert!(tracker.maintain_response_session_operations(session_id));
    let expired = tracker.response_scheduling_snapshot(session_id);
    assert!(!expired.quic_capacity_calibration_reserved);
    assert!(!expired.operation_maintenance_due);

    assert!(tracker.try_reserve_test_quic_capacity_calibration(
        session_id,
        expired.generation,
        binding_instance_id,
        path,
        path_instance_id,
        train_bytes,
        session_byte_limit,
        2,
    ));
    assert!(tracker.commit_test_quic_capacity_calibration(
        session_id,
        binding_instance_id,
        path,
        path_instance_id,
        Duration::from_secs(1),
        2,
    ));
    assert!(tracker.complete_test_quic_capacity_calibration(
        session_id,
        binding_instance_id,
        path,
        path_instance_id,
    ));
    let completed = tracker.response_scheduling_snapshot(session_id);
    assert!(
        !completed.quic_capacity_calibration_reserved,
        "measured evidence releases serialization for a different candidate"
    );

    tracker.clear_quic_capacity_calibration(
        session_id,
        binding_instance_id,
        path,
        path_instance_id,
    );
    let cleared = tracker.response_scheduling_snapshot(session_id);
    assert!(
        !tracker.try_reserve_test_quic_capacity_calibration(
            session_id,
            cleared.generation,
            binding_instance_id,
            path,
            path_instance_id,
            train_bytes,
            session_byte_limit,
            3,
        ),
        "completion releases the slot but not the exact path's two-attempt budget"
    );
    let alternate_path_instance_id = next_server_carrier_path_instance_id();
    assert!(tracker.try_reserve_test_quic_capacity_calibration(
        session_id,
        cleared.generation,
        binding_instance_id,
        path,
        alternate_path_instance_id,
        train_bytes,
        session_byte_limit,
        4,
    ));
    let alternate = tracker.response_scheduling_snapshot(session_id);
    assert_eq!(
        alternate.quic_capacity_calibration_spent_bytes,
        3 * train_bytes
    );
}

#[test]
fn quic_capacity_attempts_are_path_instance_scoped_but_session_bytes_are_shared() {
    let tracker = ServerPathLaneTracker::default();
    let session_id = SessionId(518);
    let path = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(9),
    };
    let path_instance_id = next_server_carrier_path_instance_id();
    let replacement_path_instance_id = next_server_carrier_path_instance_id();
    let session_byte_limit = 250;
    tracker.set_response_flow_active(session_id, true);
    tracker.set_response_flow_active(session_id, true);

    for (binding_instance_id, token) in [(71, 1), (72, 2)] {
        assert!(tracker.try_reserve_test_quic_capacity_calibration(
            session_id,
            tracker.generation(session_id),
            binding_instance_id,
            path,
            path_instance_id,
            100,
            session_byte_limit,
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

    let shared_path = tracker.response_path_scheduling_snapshot(session_id, path, path_instance_id);
    assert_eq!(shared_path.quic_capacity_calibration_attempts, 2);
    let exhausted_generation = tracker.generation(session_id);
    assert!(!tracker.try_reserve_test_quic_capacity_calibration(
        session_id,
        exhausted_generation,
        73,
        path,
        path_instance_id,
        1,
        session_byte_limit,
        3,
    ));

    let replacement =
        tracker.response_path_scheduling_snapshot(session_id, path, replacement_path_instance_id);
    assert_eq!(replacement.quic_capacity_calibration_attempts, 0);
    let before_budget_rejection = tracker.response_scheduling_snapshot(session_id);
    assert_eq!(
        before_budget_rejection.quic_capacity_calibration_spent_bytes,
        200
    );
    assert!(!before_budget_rejection.quic_capacity_calibration_reserved);
    assert!(!tracker.try_reserve_test_quic_capacity_calibration(
        session_id,
        before_budget_rejection.generation,
        73,
        path,
        replacement_path_instance_id,
        51,
        session_byte_limit,
        4,
    ));

    let after_budget_rejection = tracker.response_scheduling_snapshot(session_id);
    assert_eq!(
        after_budget_rejection.generation,
        before_budget_rejection.generation
    );
    assert_eq!(
        after_budget_rejection.quic_capacity_calibration_spent_bytes,
        before_budget_rejection.quic_capacity_calibration_spent_bytes
    );
    assert!(!after_budget_rejection.quic_capacity_calibration_reserved);
    assert_eq!(
        tracker
            .response_path_scheduling_snapshot(session_id, path, replacement_path_instance_id,)
            .quic_capacity_calibration_attempts,
        0,
        "a byte-budget rejection must not consume the replacement path's first attempt"
    );
    assert_eq!(
        tracker
            .response_path_scheduling_snapshot(session_id, path, path_instance_id)
            .quic_capacity_calibration_attempts,
        2
    );
}

#[test]
fn quic_capacity_retirement_bounds_flapping_attempt_keys_without_refunding_spend() {
    let tracker = ServerPathLaneTracker::default();
    let session_id = SessionId(520);
    let path = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(9),
    };
    tracker.set_response_flow_active(session_id, true);
    tracker.set_response_flow_active(session_id, true);

    for token in 1..=32 {
        let path_instance_id = next_server_carrier_path_instance_id();
        assert!(tracker.try_reserve_test_quic_capacity_calibration(
            session_id,
            tracker.generation(session_id),
            71,
            path,
            path_instance_id,
            10,
            1_000,
            token,
        ));
        assert!(tracker.commit_test_quic_capacity_calibration(
            session_id,
            71,
            path,
            path_instance_id,
            Duration::from_secs(1),
            token,
        ));
        if token < 32 {
            assert!(tracker.complete_test_quic_capacity_calibration(
                session_id,
                71,
                path,
                path_instance_id,
            ));
        }
        assert_eq!(
            tracker
                .response_path_scheduling_snapshot(session_id, path, path_instance_id)
                .quic_capacity_calibration_attempts,
            1
        );
        tracker.retire_quic_capacity_calibration_path_instance(session_id, path, path_instance_id);
        assert!(
            !tracker
                .response_scheduling_snapshot(session_id)
                .quic_capacity_calibration_reserved
        );
        assert_eq!(
            tracker
                .response_path_scheduling_snapshot(session_id, path, path_instance_id)
                .quic_capacity_calibration_attempts,
            0
        );
    }

    assert_eq!(
        tracker.quic_capacity_history_snapshot_for_test(session_id),
        Some(ServerQuicCapacityHistorySnapshot {
            attempt_entry_count: 0,
            spent_bytes: Some(320),
        }),
        "carrier-instance retirement cannot refill the session envelope"
    );
}

#[test]
fn quic_capacity_replacement_only_resets_a_distinct_retired_instance() {
    let mux_limits = MuxLimits::default();
    let session_id = SessionId(521);
    let tracker = Arc::new(ServerPathLaneTracker::default());
    let (service_commands, _service_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(0),
        service_commands,
        FlowLane::Throughput,
        mux_limits,
        tracker.clone(),
    );
    let (second_commands, _second_receivers) = reliable_path_command_channels(8);
    let _second_flow = ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(2),
        second_commands,
        FlowLane::Throughput,
        mux_limits,
        tracker.clone(),
    );
    let candidate = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let old_instance = next_server_carrier_path_instance_id();
    let (old_commands, old_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach_with_path_instance(
            candidate.underlay,
            candidate.path_id,
            old_instance,
            old_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            mux_limits.max_payload_bytes,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    assert!(tracker.try_reserve_test_quic_capacity_calibration(
        session_id,
        tracker.generation(session_id),
        binding.binding_instance_id,
        candidate,
        old_instance,
        10,
        100,
        1,
    ));
    assert!(tracker.commit_test_quic_capacity_calibration(
        session_id,
        binding.binding_instance_id,
        candidate,
        old_instance,
        Duration::from_secs(1),
        1,
    ));
    drop(old_receivers);

    let (same_instance_commands, same_instance_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach_with_path_instance(
            candidate.underlay,
            candidate.path_id,
            old_instance,
            same_instance_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            mux_limits.max_payload_bytes,
        ),
        ResponseStreamAttachOutcome::ReplacedClosedOutput
    );
    assert_eq!(
        tracker
            .response_path_scheduling_snapshot(session_id, candidate, old_instance)
            .quic_capacity_calibration_attempts,
        1,
        "reopening commands for the same carrier instance cannot reset its allowance"
    );
    assert!(
        !tracker
            .response_scheduling_snapshot(session_id)
            .quic_capacity_calibration_reserved,
        "replacing a dead command queue must release its active serialization lease"
    );
    drop(same_instance_receivers);

    let replacement_instance = next_server_carrier_path_instance_id();
    let (replacement_commands, _replacement_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach_with_path_instance(
            candidate.underlay,
            candidate.path_id,
            replacement_instance,
            replacement_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            mux_limits.max_payload_bytes,
        ),
        ResponseStreamAttachOutcome::ReplacedClosedOutput
    );
    assert_eq!(
        tracker
            .response_path_scheduling_snapshot(session_id, candidate, old_instance)
            .quic_capacity_calibration_attempts,
        1,
        "binding replacement cannot retire a carrier shared by other streams"
    );
    tracker.retire_quic_capacity_calibration_path_instance(session_id, candidate, old_instance);
    assert_eq!(
        tracker
            .response_path_scheduling_snapshot(session_id, candidate, old_instance)
            .quic_capacity_calibration_attempts,
        0,
        "exact carrier retirement releases its instance-scoped attempt key"
    );
    let scheduling = tracker.response_scheduling_snapshot(session_id);
    assert_eq!(scheduling.quic_capacity_calibration_spent_bytes, 10);
    assert_eq!(
        tracker
            .response_path_scheduling_snapshot(session_id, candidate, replacement_instance)
            .quic_capacity_calibration_attempts,
        0
    );
}

#[test]
fn quic_capacity_rollback_is_provisional_token_exact_and_reclaim_clears_ledgers() {
    let tracker = ServerPathLaneTracker::default();
    let session_id = SessionId(512);
    let path = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(7),
    };
    let path_instance_id = next_server_carrier_path_instance_id();
    let binding_instance_id = 41;
    tracker.attach_session(session_id);
    tracker.set_response_flow_active(session_id, true);
    tracker.set_response_flow_active(session_id, true);

    let generation = tracker.generation(session_id);
    assert!(tracker.try_reserve_test_quic_capacity_calibration(
        session_id,
        generation,
        binding_instance_id,
        path,
        path_instance_id,
        100,
        1_000,
        10,
    ));
    tracker.rollback_quic_capacity_calibration(
        session_id,
        binding_instance_id,
        path,
        path_instance_id,
        9,
    );
    let stale_rollback = tracker.response_scheduling_snapshot(session_id);
    assert!(stale_rollback.quic_capacity_calibration_reserved);
    assert_eq!(stale_rollback.quic_capacity_calibration_spent_bytes, 100);

    tracker.rollback_quic_capacity_calibration(
        session_id,
        binding_instance_id,
        path,
        path_instance_id,
        10,
    );
    let rolled_back = tracker.response_scheduling_snapshot(session_id);
    assert!(!rolled_back.quic_capacity_calibration_reserved);
    assert_eq!(rolled_back.quic_capacity_calibration_spent_bytes, 0);
    assert_eq!(
        tracker
            .response_path_scheduling_snapshot(session_id, path, path_instance_id)
            .quic_capacity_calibration_attempts,
        0
    );

    assert!(tracker.try_reserve_test_quic_capacity_calibration(
        session_id,
        rolled_back.generation,
        binding_instance_id,
        path,
        path_instance_id,
        100,
        1_000,
        11,
    ));
    assert!(tracker.commit_test_quic_capacity_calibration(
        session_id,
        binding_instance_id,
        path,
        path_instance_id,
        Duration::from_secs(1),
        11,
    ));
    tracker.rollback_quic_capacity_calibration(
        session_id,
        binding_instance_id,
        path,
        path_instance_id,
        11,
    );
    let admitted = tracker.response_scheduling_snapshot(session_id);
    assert!(admitted.quic_capacity_calibration_reserved);
    assert_eq!(admitted.quic_capacity_calibration_spent_bytes, 100);
    assert!(tracker.complete_test_quic_capacity_calibration(
        session_id,
        binding_instance_id,
        path,
        path_instance_id,
    ));
    assert_eq!(
        tracker
            .response_scheduling_snapshot(session_id)
            .quic_capacity_calibration_spent_bytes,
        100,
        "admitted carrier bytes remain charged after proof"
    );

    tracker.set_response_flow_active(session_id, false);
    tracker.set_response_flow_active(session_id, false);
    tracker.detach_session(session_id);
    assert_eq!(
        tracker.quic_capacity_history_snapshot_for_test(session_id),
        None,
        "session reclamation removes all QUIC capacity history"
    );
}
