use super::super::ResponseStreamBinding;
use super::super::response_ack_clock::{
    RESPONSE_ACK_CLOCK_GOODPUT_MIN_ELAPSED, ResponseAckClockCalibrationState,
};
use super::super::response_load::ServerRealtimeFlowRegistration;
use super::super::response_session::ServerPathLaneTracker;
use super::super::response_topology::ResponseStreamAttachOutcome;
use super::super::test_support::{binding_for_underlay, stream_data_frame, stream_data_frame_at};
use crate::model::ack_clock::{
    reliable_ack_clock_calibration_ceiling_bytes, reliable_ack_clock_calibration_limit_bytes,
};
use crate::model::capacity::MIN_RATE_SAMPLE_BYTES;
use crate::model::multipath::{PathAdmissionDecision, SubflowAdmissionInput};
use crate::model::path::CarrierPathKey;
use crate::mux::MuxLimits;
use crate::protocol::{OffsetRange, PathId, SessionId, StreamOpenRole, UnderlayProtocol};
use crate::runtime::path::commands::reliable_path_command_channels;
use crate::runtime::relay::io::{
    reliable_bulk_carrier_feed_quantum_bytes, reliable_relay_buffer_len,
};
use crate::scheduler::FlowLane;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[test]
fn response_subflow_set_allows_repeated_measured_subflow_admission() {
    let (binding, service) = binding_for_underlay(UnderlayProtocol::Udp);
    let optional = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
    let input = SubflowAdmissionInput {
        key: optional,
        bulk_rate_proven: true,
        startup_owner_allowed: false,
        frontier_clear: true,
        completion_improves: true,
        observed_goodput_non_degrading: true,
        read_gap: Duration::ZERO,
        owner_bytes: payload_bytes,
        optional_overhead_bytes: 0,
    };

    let first =
        binding.preview_subflow_owner_admission(service, payload_bytes, 0, Duration::ZERO, input);
    assert_eq!(first.decision, PathAdmissionDecision::AdmitSubflow);

    let committed =
        binding.commit_subflow_owner_admission(service, payload_bytes, 0, Duration::ZERO, input);
    assert_eq!(committed.decision, PathAdmissionDecision::AdmitSubflow);

    let second =
        binding.preview_subflow_owner_admission(service, payload_bytes, 0, Duration::ZERO, input);
    assert_eq!(
        second.decision,
        PathAdmissionDecision::AdmitSubflow,
        "measured subflows are paced by inflight/completion/reorder gates, not by a startup quantum"
    );

    binding.reset_subflow_set();
    let after_reset =
        binding.preview_subflow_owner_admission(service, payload_bytes, 0, Duration::ZERO, input);
    assert_eq!(after_reset.decision, PathAdmissionDecision::AdmitSubflow);
}

#[test]
fn response_semantic_reset_retires_partial_ack_clock_credit_without_refill() {
    let mux_limits = MuxLimits::default();
    let (binding, _service) = binding_for_underlay(UnderlayProtocol::Tcp);
    let candidate = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (candidate_commands, _candidate_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            candidate.underlay,
            candidate.path_id,
            candidate_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            reliable_relay_buffer_len(mux_limits),
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let initial_limit = reliable_ack_clock_calibration_limit_bytes(mux_limits);
    let spent_bytes = initial_limit / 2;
    let identity = {
        let mut outputs = binding.outputs.lock().expect("test response outputs lock");
        let entry = outputs
            .entries
            .iter()
            .find(|entry| entry.key == candidate)
            .expect("candidate output");
        let identity = (entry.key, entry.incarnation);
        let mut calibration = ResponseAckClockCalibrationState::new(
            initial_limit,
            reliable_ack_clock_calibration_ceiling_bytes(mux_limits),
        );
        calibration.spent_bytes = spent_bytes;
        outputs.ack_clock_calibrations.insert(identity, calibration);
        outputs.active_ack_clock_calibration = Some(identity);
        identity
    };

    binding.reset_subflow_set();

    let outputs = binding.outputs.lock().expect("test response outputs lock");
    let calibration = outputs
        .ack_clock_calibrations
        .get(&identity)
        .expect("retired calibration tombstone");
    assert_eq!(calibration.spent_bytes, spent_bytes);
    assert_eq!(calibration.credit_limit_bytes, spent_bytes);
    assert_eq!(calibration.max_limit_bytes, spent_bytes);
    assert_eq!(outputs.active_ack_clock_calibration, None);
    drop(outputs);

    let target = binding
        .sender_path_targets(FlowLane::Throughput, 1)
        .into_iter()
        .find(|target| target.key == candidate)
        .expect("retired candidate target");
    assert_eq!(
        target.ack_clock_calibration_spent_bytes, target.ack_clock_calibration_max_limit_bytes,
        "selection sees an exhausted tombstone instead of refilled credit"
    );
}

#[test]
fn response_semantic_reset_keeps_retired_active_identity_until_owner_flight_drains() {
    let mux_limits = MuxLimits::default();
    let (binding, _service) = binding_for_underlay(UnderlayProtocol::Tcp);
    let candidate = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (candidate_commands, _candidate_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            candidate.underlay,
            candidate.path_id,
            candidate_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            reliable_relay_buffer_len(mux_limits),
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let frame = stream_data_frame_at(0, 4096);
    let identity = {
        let mut outputs = binding.outputs.lock().expect("test response outputs lock");
        let entry = outputs
            .entries
            .iter_mut()
            .find(|entry| entry.key == candidate)
            .expect("candidate output");
        entry.product_progress_rate_bps = Some(1.0);
        entry.delivery_rate_bps = Some(1.0);
        let identity = (entry.key, entry.incarnation);
        let mut calibration = ResponseAckClockCalibrationState::new(
            reliable_ack_clock_calibration_limit_bytes(mux_limits),
            reliable_ack_clock_calibration_ceiling_bytes(mux_limits),
        );
        calibration.spent_bytes = 4096;
        outputs.ack_clock_calibrations.insert(identity, calibration);
        outputs.active_ack_clock_calibration = Some(identity);
        identity
    };
    binding.record_owner_flight(candidate, &frame);

    binding.reset_subflow_set();

    {
        let outputs = binding.outputs.lock().expect("test response outputs lock");
        assert_eq!(outputs.active_ack_clock_calibration, Some(identity));
        let calibration = outputs
            .ack_clock_calibrations
            .get(&identity)
            .expect("retired calibration state");
        assert_eq!(calibration.spent_bytes, calibration.max_limit_bytes);
    }
    let target = binding
        .sender_path_targets(FlowLane::Throughput, 1)
        .into_iter()
        .find(|target| target.key == candidate)
        .expect("retired candidate target");
    assert!(target.ack_clock_calibration_active);

    std::thread::sleep(Duration::from_millis(1));
    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: 4096,
    }]);
    assert_eq!(
        binding
            .outputs
            .lock()
            .expect("test response outputs lock")
            .active_ack_clock_calibration,
        None
    );
    {
        let outputs = binding.outputs.lock().expect("test response outputs lock");
        let entry = outputs
            .entries
            .iter()
            .find(|entry| entry.key == candidate)
            .expect("candidate output after calibration drain");
        assert_eq!(entry.delivery_rate_bps, Some(1.0));
    }

    let ordinary = stream_data_frame_at(4096, MIN_RATE_SAMPLE_BYTES as usize);
    let later = stream_data_frame_at(4096 + MIN_RATE_SAMPLE_BYTES, MIN_RATE_SAMPLE_BYTES as usize);
    binding.record_owner_flight(candidate, &ordinary);
    binding.record_owner_flight(candidate, &later);
    let first_ack = Instant::now() + RESPONSE_ACK_CLOCK_GOODPUT_MIN_ELAPSED;
    binding.release_normalized_acked_ranges_at(
        &[OffsetRange {
            start: 4096,
            end: 4096 + MIN_RATE_SAMPLE_BYTES,
        }],
        first_ack,
    );
    binding.release_normalized_acked_ranges_at(
        &[OffsetRange {
            start: 4096 + MIN_RATE_SAMPLE_BYTES,
            end: 4096 + 2 * MIN_RATE_SAMPLE_BYTES,
        }],
        first_ack + RESPONSE_ACK_CLOCK_GOODPUT_MIN_ELAPSED,
    );
    let outputs = binding.outputs.lock().expect("test response outputs lock");
    let entry = outputs
        .entries
        .iter()
        .find(|entry| entry.key == candidate)
        .expect("candidate output after ordinary ACK");
    assert!(entry.delivery_rate_bps.is_some_and(|rate| rate > 1.0));
}

#[test]
fn response_subflow_set_rejects_unproven_owner_without_bulk_rate() {
    let (binding, service) = binding_for_underlay(UnderlayProtocol::Udp);
    let optional = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
    let input = SubflowAdmissionInput {
        key: optional,
        bulk_rate_proven: false,
        startup_owner_allowed: false,
        frontier_clear: true,
        completion_improves: true,
        observed_goodput_non_degrading: true,
        read_gap: Duration::ZERO,
        owner_bytes: payload_bytes,
        optional_overhead_bytes: 0,
    };

    let committed =
        binding.commit_subflow_owner_admission(service, payload_bytes, 0, Duration::ZERO, input);
    assert_eq!(
        committed.decision,
        PathAdmissionDecision::ProbeOnly,
        "sender/proof/ACK-data evidence is not enough to enter the owner Subflow set"
    );
    assert!(binding.subflow_set_snapshot().is_none());

    let second =
        binding.preview_subflow_owner_admission(service, payload_bytes, 0, Duration::ZERO, input);
    assert_eq!(
        second.decision,
        PathAdmissionDecision::ProbeOnly,
        "unproven Subflows remain Probe until they have bulk-rate evidence"
    );

    binding.reset_subflow_set();
    let after_reset =
        binding.preview_subflow_owner_admission(service, payload_bytes, 0, Duration::ZERO, input);
    assert_eq!(after_reset.decision, PathAdmissionDecision::ProbeOnly);
}

#[test]
fn response_subflow_unproven_probe_state_survives_ack_progress() {
    let (binding, service) = binding_for_underlay(UnderlayProtocol::Udp);
    let optional = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
    let input = SubflowAdmissionInput {
        key: optional,
        bulk_rate_proven: false,
        startup_owner_allowed: false,
        frontier_clear: true,
        completion_improves: true,
        observed_goodput_non_degrading: true,
        read_gap: Duration::ZERO,
        owner_bytes: payload_bytes,
        optional_overhead_bytes: 0,
    };

    assert_eq!(
        binding
            .commit_subflow_owner_admission(service, payload_bytes, 0, Duration::ZERO, input)
            .decision,
        PathAdmissionDecision::ProbeOnly
    );
    assert_eq!(
        binding
            .preview_subflow_owner_admission(service, payload_bytes, 0, Duration::ZERO, input)
            .decision,
        PathAdmissionDecision::ProbeOnly
    );

    let service_frame = stream_data_frame(payload_bytes);
    binding.record_owner_flight(service, &service_frame);
    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: payload_bytes as u64,
    }]);

    assert_eq!(
        binding
            .preview_subflow_owner_admission(service, payload_bytes, 0, Duration::ZERO, input)
            .decision,
        PathAdmissionDecision::ProbeOnly,
        "ordinary ACK progress must not convert an unproven path into a Subflow owner"
    );
}

#[test]
fn response_subflow_epoch_survives_passive_growth_but_resets_on_detach() {
    let (binding, service) = binding_for_underlay(UnderlayProtocol::Udp);
    let optional = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (commands, _receivers) = reliable_path_command_channels(8);
    binding.attach(
        optional.underlay,
        optional.path_id,
        commands.clone(),
        FlowLane::Throughput,
        StreamOpenRole::Validation,
        reliable_relay_buffer_len(MuxLimits::default()),
    );

    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
    let input = SubflowAdmissionInput {
        key: optional,
        bulk_rate_proven: true,
        startup_owner_allowed: false,
        frontier_clear: true,
        completion_improves: true,
        observed_goodput_non_degrading: true,
        read_gap: Duration::ZERO,
        owner_bytes: payload_bytes,
        optional_overhead_bytes: 0,
    };

    assert_eq!(
        binding
            .commit_subflow_owner_admission(service, payload_bytes, 0, Duration::ZERO, input)
            .decision,
        PathAdmissionDecision::AdmitSubflow
    );
    assert!(binding.subflow_set_snapshot().is_some());

    let (stale_generation, _) = binding.subflow_state_snapshot();
    let stale_lane_generation = binding.lane_generation();
    let added = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(2),
    };
    let (added_commands, _added_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            added.underlay,
            added.path_id,
            added_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            reliable_relay_buffer_len(MuxLimits::default()),
        ),
        ResponseStreamAttachOutcome::Attached
    );
    assert!(
        binding.subflow_set_snapshot().is_some(),
        "passive output growth must preserve the current Subflow epoch"
    );
    assert_eq!(
        binding
            .commit_subflow_owner_admission_for_planner_generation(
                stale_generation,
                stale_lane_generation,
                service,
                payload_bytes,
                0,
                Duration::ZERO,
                input,
            )
            .decision,
        PathAdmissionDecision::Standby,
        "a plan made before passive membership changed must not commit afterward"
    );
    assert_eq!(
        binding
            .commit_subflow_owner_admission(service, payload_bytes, 0, Duration::ZERO, input,)
            .decision,
        PathAdmissionDecision::AdmitSubflow
    );
    assert!(binding.subflow_set_snapshot().is_some());

    binding.detach(optional, &commands);

    assert!(
        binding.subflow_set_snapshot().is_none(),
        "carrier output detach resets the Subflow set"
    );
}

#[test]
fn passive_cross_family_attach_does_not_refill_or_transfer_startup_epoch() {
    let (binding, service) = binding_for_underlay(UnderlayProtocol::Tcp);
    let candidate = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (candidate_commands, _candidate_receivers) = reliable_path_command_channels(8);
    binding.attach(
        candidate.underlay,
        candidate.path_id,
        candidate_commands,
        FlowLane::Throughput,
        StreamOpenRole::Validation,
        reliable_relay_buffer_len(MuxLimits::default()),
    );

    let quantum = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
    let startup_credit = quantum * 4;
    let input = SubflowAdmissionInput {
        key: candidate,
        bulk_rate_proven: false,
        startup_owner_allowed: true,
        frontier_clear: true,
        completion_improves: false,
        observed_goodput_non_degrading: true,
        read_gap: Duration::ZERO,
        owner_bytes: quantum,
        optional_overhead_bytes: 0,
    };
    for _ in 0..4 {
        assert_eq!(
            binding
                .commit_subflow_owner_admission(service, startup_credit, 0, Duration::ZERO, input,)
                .decision,
            PathAdmissionDecision::AdmitSubflow
        );
    }
    assert_eq!(
        binding
            .preview_subflow_owner_admission(
                service,
                startup_credit,
                0,
                Duration::ZERO,
                SubflowAdmissionInput {
                    owner_bytes: 1,
                    ..input
                },
            )
            .decision,
        PathAdmissionDecision::ProbeOnly,
        "the initial candidate has spent the cumulative startup cap"
    );

    let (stale_generation, _) = binding.subflow_state_snapshot();
    let stale_lane_generation = binding.lane_generation();
    let added = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    let (added_commands, _added_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            added.underlay,
            added.path_id,
            added_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            reliable_relay_buffer_len(MuxLimits::default()),
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let (current_generation, epoch) = binding.subflow_state_snapshot();
    assert_ne!(current_generation, stale_generation);
    let epoch = epoch.expect("passive attachment preserves startup epoch");
    assert_eq!(epoch.members().len(), 1);
    assert_eq!(epoch.members()[0].key, candidate);
    assert_eq!(epoch.members()[0].owner_sent_bytes, startup_credit as u64);
    assert_eq!(
        binding
            .commit_subflow_owner_admission_for_planner_generation(
                stale_generation,
                stale_lane_generation,
                service,
                startup_credit,
                0,
                Duration::ZERO,
                input,
            )
            .decision,
        PathAdmissionDecision::Standby,
        "a plan made before passive growth must not commit afterward"
    );
    assert_eq!(
        binding
            .commit_subflow_owner_admission(
                service,
                startup_credit,
                0,
                Duration::ZERO,
                SubflowAdmissionInput {
                    owner_bytes: 1,
                    ..input
                },
            )
            .decision,
        PathAdmissionDecision::ProbeOnly,
        "passive growth must not refill the selected candidate's startup credit"
    );
    assert_eq!(
        binding
            .commit_subflow_owner_admission(
                service,
                startup_credit,
                0,
                Duration::ZERO,
                SubflowAdmissionInput {
                    key: added,
                    owner_bytes: 1,
                    ..input
                },
            )
            .decision,
        PathAdmissionDecision::ProbeOnly,
        "passive growth must not transfer startup ownership to the new output"
    );
}

#[test]
fn passive_attach_after_reservation_preserves_unemitted_credit_rollback() {
    for passive_role in [StreamOpenRole::Validation, StreamOpenRole::Repair] {
        let (binding, service) = binding_for_underlay(UnderlayProtocol::Tcp);
        let candidate = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let (candidate_commands, _candidate_receivers) = reliable_path_command_channels(8);
        binding.attach(
            candidate.underlay,
            candidate.path_id,
            candidate_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            reliable_relay_buffer_len(MuxLimits::default()),
        );
        let quantum = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
        let optional_bytes = 1024;
        let input = SubflowAdmissionInput {
            key: candidate,
            bulk_rate_proven: false,
            startup_owner_allowed: true,
            frontier_clear: true,
            completion_improves: false,
            observed_goodput_non_degrading: true,
            read_gap: Duration::ZERO,
            owner_bytes: quantum,
            optional_overhead_bytes: optional_bytes,
        };
        let (planner_generation, _) = binding.subflow_state_snapshot();
        let reservation = binding.reserve_subflow_owner_admission_for_planner_generation(
            planner_generation,
            binding.lane_generation(),
            service,
            quantum,
            optional_bytes,
            Duration::ZERO,
            input,
        );
        assert_eq!(
            reservation.admission.decision,
            PathAdmissionDecision::AdmitSubflow
        );
        let epoch_generation = reservation
            .epoch_generation
            .expect("admitted Subflow reservation has an epoch token");

        let passive = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        };
        let (passive_commands, _passive_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                passive.underlay,
                passive.path_id,
                passive_commands,
                FlowLane::Throughput,
                passive_role,
                reliable_relay_buffer_len(MuxLimits::default()),
            ),
            ResponseStreamAttachOutcome::Attached
        );
        binding.rollback_subflow_owner_admission_for_epoch(epoch_generation, input);

        assert_eq!(
            binding
                .commit_subflow_owner_admission(
                    service,
                    quantum,
                    optional_bytes,
                    Duration::ZERO,
                    input,
                )
                .decision,
            PathAdmissionDecision::AdmitSubflow,
            "{passive_role:?} planner invalidation must not block refund of unemitted bytes"
        );
    }
}

#[test]
fn full_reset_rejects_stale_epoch_rollback() {
    let (binding, service) = binding_for_underlay(UnderlayProtocol::Tcp);
    let candidate = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let quantum = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
    let input = SubflowAdmissionInput {
        key: candidate,
        bulk_rate_proven: false,
        startup_owner_allowed: true,
        frontier_clear: true,
        completion_improves: false,
        observed_goodput_non_degrading: true,
        read_gap: Duration::ZERO,
        owner_bytes: quantum,
        optional_overhead_bytes: 0,
    };
    let (planner_generation, _) = binding.subflow_state_snapshot();
    let reservation = binding.reserve_subflow_owner_admission_for_planner_generation(
        planner_generation,
        binding.lane_generation(),
        service,
        quantum,
        0,
        Duration::ZERO,
        input,
    );
    let stale_epoch_generation = reservation
        .epoch_generation
        .expect("initial reservation has an epoch token");

    binding.reset_subflow_set();
    assert_eq!(
        binding
            .commit_subflow_owner_admission(service, quantum, 0, Duration::ZERO, input,)
            .decision,
        PathAdmissionDecision::AdmitSubflow
    );
    binding.rollback_subflow_owner_admission_for_epoch(stale_epoch_generation, input);

    assert_eq!(
        binding
            .preview_subflow_owner_admission(
                service,
                quantum,
                0,
                Duration::ZERO,
                SubflowAdmissionInput {
                    owner_bytes: 1,
                    ..input
                },
            )
            .decision,
        PathAdmissionDecision::ProbeOnly,
        "a stale refund must not debit a replacement epoch"
    );
}

#[test]
fn every_envelope_change_replaces_epoch_and_invalidates_competing_plans() {
    let base_service = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let changed_service = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(4),
    };
    let candidate = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let quantum = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
    let base_credit = quantum * 2;
    let base_overhead = 1024;
    let base_gap = Duration::from_millis(10);
    let variants = [
        (changed_service, base_credit, base_overhead, base_gap),
        (base_service, quantum, base_overhead, base_gap),
        (base_service, base_credit, base_overhead * 2, base_gap),
        (
            base_service,
            base_credit,
            base_overhead,
            Duration::from_millis(20),
        ),
    ];

    for (service, credit, overhead, max_gap) in variants {
        let (binding, _) = binding_for_underlay(UnderlayProtocol::Tcp);
        let input = SubflowAdmissionInput {
            key: candidate,
            bulk_rate_proven: false,
            startup_owner_allowed: true,
            frontier_clear: true,
            completion_improves: false,
            observed_goodput_non_degrading: true,
            read_gap: Duration::ZERO,
            owner_bytes: quantum,
            optional_overhead_bytes: 0,
        };
        let (initial_planner_generation, _) = binding.subflow_state_snapshot();
        let initial = binding.reserve_subflow_owner_admission_for_planner_generation(
            initial_planner_generation,
            binding.lane_generation(),
            base_service,
            base_credit,
            base_overhead,
            base_gap,
            input,
        );
        let stale_epoch_generation = initial
            .epoch_generation
            .expect("base envelope reservation has an epoch token");
        let (stale_planner_generation, _) = binding.subflow_state_snapshot();

        let replacement = binding.reserve_subflow_owner_admission_for_planner_generation(
            stale_planner_generation,
            binding.lane_generation(),
            service,
            credit,
            overhead,
            max_gap,
            input,
        );
        assert_eq!(
            replacement.admission.decision,
            PathAdmissionDecision::AdmitSubflow
        );
        assert_ne!(
            replacement.epoch_generation,
            Some(stale_epoch_generation),
            "each envelope field owns a new epoch identity"
        );
        let (current_planner_generation, _) = binding.subflow_state_snapshot();
        assert_ne!(current_planner_generation, stale_planner_generation);
        assert_eq!(
            binding
                .commit_subflow_owner_admission_for_planner_generation(
                    stale_planner_generation,
                    binding.lane_generation(),
                    service,
                    credit,
                    overhead,
                    max_gap,
                    input,
                )
                .decision,
            PathAdmissionDecision::Standby,
            "a competing plan for the replaced envelope must be stale"
        );

        binding.rollback_subflow_owner_admission_for_epoch(stale_epoch_generation, input);
        let epoch = binding
            .subflow_set_snapshot()
            .expect("replacement epoch remains present");
        assert_eq!(epoch.members().len(), 1);
        assert_eq!(epoch.members()[0].owner_sent_bytes, quantum as u64);
    }
}

#[test]
fn stale_subflow_commit_is_rejected_after_reset_or_realtime_pressure() {
    let session_id = SessionId(91);
    let service = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    let candidate = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (commands, _receivers) = reliable_path_command_channels(8);
    let lane_tracker = Arc::new(ServerPathLaneTracker::default());
    let binding = ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        service.underlay,
        service.path_id,
        commands,
        FlowLane::Throughput,
        MuxLimits::default(),
        lane_tracker.clone(),
    );
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
    let input = SubflowAdmissionInput {
        key: candidate,
        bulk_rate_proven: false,
        startup_owner_allowed: true,
        frontier_clear: true,
        completion_improves: false,
        observed_goodput_non_degrading: true,
        read_gap: Duration::ZERO,
        owner_bytes: payload_bytes,
        optional_overhead_bytes: 0,
    };

    let (stale_generation, _) = binding.subflow_state_snapshot();
    let stale_lane_generation = binding.lane_generation();
    binding.reset_subflow_set();
    assert_eq!(
        binding
            .commit_subflow_owner_admission_for_planner_generation(
                stale_generation,
                stale_lane_generation,
                service,
                payload_bytes * 4,
                0,
                Duration::ZERO,
                input,
            )
            .decision,
        PathAdmissionDecision::Standby,
        "a reset must invalidate an already-planned startup commit"
    );

    let (current_generation, _) = binding.subflow_state_snapshot();
    let pre_pressure_lane_generation = binding.lane_generation();
    let realtime = ServerRealtimeFlowRegistration::new(lane_tracker.clone(), session_id);
    assert_eq!(
        lane_tracker
            .session_snapshot(session_id)
            .active_latency_sensitive_flows,
        1
    );
    assert_eq!(
        binding
            .commit_subflow_owner_admission_for_planner_generation(
                current_generation,
                pre_pressure_lane_generation,
                service,
                payload_bytes * 4,
                0,
                Duration::ZERO,
                input,
            )
            .decision,
        PathAdmissionDecision::Standby,
        "new realtime pressure must invalidate an already-planned startup commit"
    );
    drop(realtime);
    assert_eq!(
        lane_tracker
            .session_snapshot(session_id)
            .active_latency_sensitive_flows,
        0
    );
}

#[test]
fn startup_commit_rechecks_response_flow_generation() {
    let session_id = SessionId(92);
    let service = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    let candidate = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let lane_tracker = Arc::new(ServerPathLaneTracker::default());
    let (commands, _receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        service.underlay,
        service.path_id,
        commands,
        FlowLane::Throughput,
        MuxLimits::default(),
        lane_tracker.clone(),
    );
    let (second_commands, _second_receivers) = reliable_path_command_channels(8);
    let second_flow = ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        UnderlayProtocol::Udp,
        PathId(2),
        second_commands,
        FlowLane::Throughput,
        MuxLimits::default(),
        lane_tracker.clone(),
    );
    let (planner_generation, _) = binding.subflow_state_snapshot();
    let (multi_flow_generation, active_response_flows) =
        binding.lane_generation_and_active_response_flows();
    assert_eq!(active_response_flows, 2);

    drop(second_flow);
    assert_eq!(lane_tracker.session_snapshot(session_id).active_flows, 1);
    assert_eq!(binding.lane_generation_and_active_response_flows().1, 1);
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
    let admission = binding.commit_subflow_owner_admission_for_planner_generation(
        planner_generation,
        multi_flow_generation,
        service,
        payload_bytes * 4,
        0,
        Duration::ZERO,
        SubflowAdmissionInput {
            key: candidate,
            bulk_rate_proven: false,
            startup_owner_allowed: true,
            frontier_clear: true,
            completion_improves: false,
            observed_goodput_non_degrading: true,
            read_gap: Duration::ZERO,
            owner_bytes: payload_bytes,
            optional_overhead_bytes: 0,
        },
    );
    assert_eq!(
        admission.decision,
        PathAdmissionDecision::Standby,
        "response-flow churn must invalidate a planned startup sample before commit"
    );
}

#[test]
fn unrelated_session_churn_does_not_invalidate_subflow_commit() {
    let session_id = SessionId(93);
    let other_session_id = SessionId(94);
    let service = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let candidate = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (commands, _receivers) = reliable_path_command_channels(8);
    let lane_tracker = Arc::new(ServerPathLaneTracker::default());
    let binding = ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        service.underlay,
        service.path_id,
        commands,
        FlowLane::Throughput,
        MuxLimits::default(),
        lane_tracker.clone(),
    );
    let (planner_generation, _) = binding.subflow_state_snapshot();
    let lane_generation = binding.lane_generation();

    let realtime = ServerRealtimeFlowRegistration::new(lane_tracker.clone(), other_session_id);
    let other_path = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(9),
    };
    lane_tracker.attach(other_session_id, other_path, FlowLane::Latency);
    lane_tracker.detach(other_session_id, other_path, FlowLane::Latency);

    assert_eq!(binding.lane_generation(), lane_generation);
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
    assert_eq!(
        binding
            .commit_subflow_owner_admission_for_planner_generation(
                planner_generation,
                lane_generation,
                service,
                payload_bytes * 4,
                0,
                Duration::ZERO,
                SubflowAdmissionInput {
                    key: candidate,
                    bulk_rate_proven: false,
                    startup_owner_allowed: true,
                    frontier_clear: true,
                    completion_improves: false,
                    observed_goodput_non_degrading: true,
                    read_gap: Duration::ZERO,
                    owner_bytes: payload_bytes,
                    optional_overhead_bytes: 0,
                },
            )
            .decision,
        PathAdmissionDecision::AdmitSubflow,
        "lane and realtime churn in another session must not reject this session's commit"
    );
    drop(realtime);
    assert_eq!(binding.lane_generation(), lane_generation);
}
