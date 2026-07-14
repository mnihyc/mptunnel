use super::super::ResponseStreamBinding;
use super::super::ack_clock::ResponseAckClockCalibrationState;
use super::super::attachment::ResponseStreamAttachOutcome;
use super::super::test_support::{
    binding_for_underlay, output_entry_for_key, stream_data_frame, stream_data_frame_at,
};
use crate::model::capacity::{
    BBR_MAX_SEND_QUANTUM_BYTES, MIN_RATE_SAMPLE_BYTES, PATH_OPEN_SCORE_BYTES,
    reliable_relay_buffer_len,
};
use crate::model::path::CarrierPathKey;
use crate::mux::MuxLimits;
use crate::protocol::frame::reliable_stream_frame_accounted_bytes;
use crate::protocol::{OffsetRange, PathId, SessionId, StreamOpenRole, UnderlayProtocol};
use crate::runtime::RuntimeError;
use crate::runtime::path::commands::{
    reliable_path_command_channels, try_recv_reliable_path_command,
};
use crate::scheduler::FlowLane;

#[test]
fn response_repair_enqueue_rejects_detached_output_incarnation() {
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(6),
    };
    let (commands, mut receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(42),
        key.underlay,
        key.path_id,
        commands.clone(),
        FlowLane::Throughput,
    );
    let stale_target = binding
        .sender_path_targets(FlowLane::Throughput, 64)
        .into_iter()
        .next()
        .expect("initial response output");
    binding.detach(key, &commands);
    let (replacement_commands, mut replacement_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            key.underlay,
            key.path_id,
            replacement_commands,
            FlowLane::Throughput,
            StreamOpenRole::Active,
            reliable_relay_buffer_len(MuxLimits::default()),
        ),
        ResponseStreamAttachOutcome::Attached
    );

    assert!(matches!(
        binding.try_enqueue_repair_frame_for_target(
            &stale_target,
            &stream_data_frame(64),
            FlowLane::Throughput,
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    assert!(try_recv_reliable_path_command(&mut receivers).is_none());
    assert!(try_recv_reliable_path_command(&mut replacement_receivers).is_none());
}

#[test]
fn udp_stream_ack_releases_product_flight_without_seeding_carrier_rate() {
    let (binding, key) = binding_for_underlay(UnderlayProtocol::Udp);
    let frame = stream_data_frame(MIN_RATE_SAMPLE_BYTES as usize);

    binding.record_owner_flight(key, &frame);
    std::thread::sleep(Duration::from_millis(1));
    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: reliable_stream_frame_accounted_bytes(&frame) as u64,
    }]);

    let entry = output_entry_for_key(&binding, key);
    assert_eq!(entry.bytes_in_flight, 0);
    assert_eq!(entry.delivery_samples, 1);
    assert_eq!(entry.owner_data_acked_bytes, MIN_RATE_SAMPLE_BYTES);
    assert!(entry.product_progress_rate_bps.is_some());
    assert!(entry.delivery_rate_bps.is_none());
    assert!(entry.srtt_ms.is_none());
}

#[test]
fn duplicate_response_validation_copy_does_not_become_ordering_owner() {
    let (binding, owner) = binding_for_underlay(UnderlayProtocol::Tcp);
    let duplicate = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (duplicate_commands, _duplicate_receivers) = reliable_path_command_channels(8);
    binding.attach(
        duplicate.underlay,
        duplicate.path_id,
        duplicate_commands,
        FlowLane::Throughput,
        StreamOpenRole::Repair,
        reliable_relay_buffer_len(MuxLimits::default()),
    );
    let frame = stream_data_frame_at(0, 4096);

    binding.record_owner_flight(owner, &frame);
    binding.record_repair_flight(duplicate, &frame);
    let owner_identity = {
        let mut outputs = binding.outputs.lock().expect("test response outputs lock");
        let entry = outputs
            .entries
            .iter()
            .find(|entry| entry.key == owner)
            .expect("owner output exists");
        let identity = (entry.key, entry.incarnation);
        let mut calibration = ResponseAckClockCalibrationState::new(
            PATH_OPEN_SCORE_BYTES as u64,
            PATH_OPEN_SCORE_BYTES as u64,
        );
        calibration.spent_bytes = PATH_OPEN_SCORE_BYTES as u64;
        outputs.ack_clock_calibrations.insert(identity, calibration);
        identity
    };

    let lower = binding.lower_flights_before_offset(4096);
    assert!(
        lower.is_empty(),
        "plain unacked owner flight is recovery state, not authoritative ordering debt"
    );

    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: 4096,
    }]);
    let entries = binding.outputs.lock().expect("test response outputs lock");
    let owner_entry = entries
        .entries
        .iter()
        .find(|entry| entry.key == owner)
        .expect("owner output exists");
    let duplicate_entry = entries
        .entries
        .iter()
        .find(|entry| entry.key == duplicate)
        .expect("duplicate output exists");
    assert_eq!(owner_entry.bytes_in_flight, 0);
    assert_eq!(duplicate_entry.bytes_in_flight, 0);
    assert_eq!(
        owner_entry.delivery_samples, 0,
        "ACK of a duplicated byte range is not path-scoped proof for the owner path"
    );
    assert_eq!(
        duplicate_entry.delivery_samples, 0,
        "repair duplicate STREAM_ACK must not become response bulk evidence"
    );
    assert_eq!(owner_entry.owner_data_acked_bytes, 0);
    assert_eq!(duplicate_entry.owner_data_acked_bytes, 0);
    assert!(owner_entry.tcp_product_rate_evidence.is_none());
    assert!(
        entries
            .ack_clock_calibrations
            .get(&owner_identity)
            .expect("owner calibration state")
            .rate_evidence
            .is_none(),
        "ambiguous OwnerData/RepairData ACKs cannot advance the TCP ACK clock"
    );
}

#[test]
fn partial_same_start_response_ack_releases_each_copy_and_retains_owner_suffix() {
    let (binding, owner) = binding_for_underlay(UnderlayProtocol::Tcp);
    let repair = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (repair_commands, _repair_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            repair.underlay,
            repair.path_id,
            repair_commands,
            FlowLane::Latency,
            StreamOpenRole::Repair,
            reliable_relay_buffer_len(MuxLimits::default()),
        ),
        ResponseStreamAttachOutcome::Attached,
    );
    binding.record_owner_flight(owner, &stream_data_frame_at(0, 4096));
    binding.record_repair_flight(repair, &stream_data_frame_at(0, 1024));

    {
        let outputs = binding.outputs.lock().expect("test response outputs lock");
        let owner_entry = outputs
            .entries
            .iter()
            .find(|entry| entry.key == owner)
            .expect("owner output exists");
        let repair_entry = outputs
            .entries
            .iter()
            .find(|entry| entry.key == repair)
            .expect("repair output exists");
        assert_eq!(owner_entry.owner_data_in_flight_bytes, 4096);
        assert_eq!(owner_entry.bytes_in_flight, 4096);
        assert_eq!(repair_entry.owner_data_in_flight_bytes, 0);
        assert_eq!(repair_entry.bytes_in_flight, 1024);
    }

    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: 1024,
    }]);
    {
        let outputs = binding.outputs.lock().expect("test response outputs lock");
        let owner_entry = outputs
            .entries
            .iter()
            .find(|entry| entry.key == owner)
            .expect("owner output exists");
        let repair_entry = outputs
            .entries
            .iter()
            .find(|entry| entry.key == repair)
            .expect("repair output exists");
        assert_eq!(owner_entry.bytes_in_flight, 3072);
        assert_eq!(repair_entry.bytes_in_flight, 0);
        assert_eq!(owner_entry.owner_data_in_flight_bytes, 3072);
        assert_eq!(repair_entry.owner_data_in_flight_bytes, 0);
        assert_eq!(
            owner_entry.delivery_samples, 0,
            "the duplicated prefix ACK is not path-scoped owner evidence"
        );
        assert_eq!(repair_entry.delivery_samples, 0);
        assert_eq!(owner_entry.owner_data_acked_bytes, 0);
        assert_eq!(repair_entry.owner_data_acked_bytes, 0);
    }
    let owner_suffix = stream_data_frame_at(1024, 3072);
    assert_eq!(
        binding.owner_flight_keys_overlapping_frame(&owner_suffix),
        vec![owner],
        "the longer owner flight must survive after its shorter same-start repair copy is released"
    );
    assert_eq!(
        binding.flight_keys_overlapping_frame(&owner_suffix),
        vec![owner]
    );

    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 1024,
        end: 4096,
    }]);
    let outputs = binding.outputs.lock().expect("test response outputs lock");
    let owner_entry = outputs
        .entries
        .iter()
        .find(|entry| entry.key == owner)
        .expect("owner output exists");
    let repair_entry = outputs
        .entries
        .iter()
        .find(|entry| entry.key == repair)
        .expect("repair output exists");
    assert_eq!(owner_entry.bytes_in_flight, 0);
    assert_eq!(repair_entry.bytes_in_flight, 0);
    assert_eq!(owner_entry.owner_data_in_flight_bytes, 0);
    assert_eq!(repair_entry.owner_data_in_flight_bytes, 0);
    assert_eq!(
        owner_entry.delivery_samples, 1,
        "the later owner-only suffix ACK may become path-scoped evidence"
    );
    assert_eq!(repair_entry.delivery_samples, 0);
    assert_eq!(owner_entry.owner_data_acked_bytes, 3072);
    assert_eq!(repair_entry.owner_data_acked_bytes, 0);
}

#[test]
fn lower_flight_debt_ignores_plain_unacked_owner_data_until_ack_hole() {
    let (binding, owner) = binding_for_underlay(UnderlayProtocol::Tcp);
    binding.record_owner_flight(owner, &stream_data_frame_at(0, 1024));
    binding.record_owner_flight(owner, &stream_data_frame_at(1024, 2048));

    assert!(
        binding.lower_flights_before_offset(3072).is_empty(),
        "ordinary unacked owner flight is recovery state, not authoritative ordering debt"
    );

    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 1024,
        end: 3072,
    }]);

    let lower = binding.lower_flights_before_offset(3072);
    assert_eq!(lower.len(), 1);
    assert_eq!(lower[0].key, owner);
    assert_eq!(
        lower[0].bytes, 2048,
        "ACK-hole evidence remains ordering debt until the frontier becomes contiguous"
    );
}

#[test]
fn repair_stream_ack_progress_does_not_promote_repair_output() {
    let (binding, owner) = binding_for_underlay(UnderlayProtocol::Udp);
    let repair = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (repair_commands, _repair_receivers) = reliable_path_command_channels(8);
    binding.attach(
        repair.underlay,
        repair.path_id,
        repair_commands,
        FlowLane::Throughput,
        StreamOpenRole::Repair,
        reliable_relay_buffer_len(MuxLimits::default()),
    );
    let frame = stream_data_frame_at(0, 4096);

    let before_order = binding
        .outputs
        .lock()
        .expect("test response outputs lock")
        .entries
        .iter()
        .map(|entry| entry.key)
        .collect::<Vec<_>>();

    binding.record_repair_flight(repair, &frame);
    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: 4096,
    }]);

    let outputs = binding.outputs.lock().expect("test response outputs lock");
    let after_order = outputs
        .entries
        .iter()
        .map(|entry| entry.key)
        .collect::<Vec<_>>();
    let owner_entry = outputs
        .entries
        .iter()
        .find(|entry| entry.key == owner)
        .expect("owner output exists");
    let repair_entry = outputs
        .entries
        .iter()
        .find(|entry| entry.key == repair)
        .expect("repair output exists");

    assert_eq!(after_order, before_order);
    assert_eq!(owner_entry.delivery_samples, 0);
    assert_eq!(repair_entry.delivery_samples, 0);
    assert_eq!(repair_entry.bytes_in_flight, 0);
    assert_eq!(binding.ordered_data_owner(), Some(owner));
}

#[test]
fn repair_flight_kind_never_owns_ordering_or_delivery_evidence() {
    let (binding, owner) = binding_for_underlay(UnderlayProtocol::Udp);
    let repair = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (repair_commands, _repair_receivers) = reliable_path_command_channels(8);
    binding.attach(
        repair.underlay,
        repair.path_id,
        repair_commands,
        FlowLane::Throughput,
        StreamOpenRole::Repair,
        reliable_relay_buffer_len(MuxLimits::default()),
    );

    let owner_frame = stream_data_frame_at(0, 1024);
    let repair_frame = stream_data_frame_at(1024, 1024);
    binding.record_owner_flight(owner, &owner_frame);
    binding.record_repair_flight(repair, &repair_frame);

    let lower = binding.lower_flights_before_offset(2048);
    assert!(
        lower.is_empty(),
        "plain owner flight and repair-only flight must not become admission ordering debt"
    );

    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 1024,
        end: 2048,
    }]);

    let outputs = binding.outputs.lock().expect("test response outputs lock");
    let repair_entry = outputs
        .entries
        .iter()
        .find(|entry| entry.key == repair)
        .expect("repair output exists");
    assert_eq!(repair_entry.bytes_in_flight, 0);
    assert_eq!(
        repair_entry.delivery_samples, 0,
        "RepairData ACKs release product flight but never become path delivery evidence"
    );
}

#[test]
fn old_flight_ack_does_not_debit_or_prove_replaced_output() {
    let session_id = SessionId(92);
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let (old_commands, old_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        session_id,
        key.underlay,
        key.path_id,
        old_commands,
        FlowLane::Throughput,
    );
    let frame = stream_data_frame(BBR_MAX_SEND_QUANTUM_BYTES);
    binding.record_owner_flight(key, &frame);
    drop(old_receivers);

    let (new_commands, _new_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            key.underlay,
            key.path_id,
            new_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            reliable_relay_buffer_len(MuxLimits::default()),
        ),
        ResponseStreamAttachOutcome::ReplacedClosedOutput
    );
    assert_eq!(
        binding.ordered_data_owner(),
        None,
        "a fresh Validation incarnation must not inherit the closed Service owner"
    );
    let replacement = binding
        .sender_path_targets(FlowLane::Throughput, BBR_MAX_SEND_QUANTUM_BYTES)
        .into_iter()
        .find(|target| target.key == key)
        .expect("replacement target remains attached");
    assert!(!replacement.is_active);
    let replacement_frame = stream_data_frame_at(
        BBR_MAX_SEND_QUANTUM_BYTES as u64,
        BBR_MAX_SEND_QUANTUM_BYTES,
    );
    binding.record_owner_flight_for_target(&replacement, &replacement_frame);
    assert_eq!(
        output_entry_for_key(&binding, key).bytes_in_flight,
        BBR_MAX_SEND_QUANTUM_BYTES as u64
    );
    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: BBR_MAX_SEND_QUANTUM_BYTES as u64,
    }]);

    let entry = output_entry_for_key(&binding, key);
    assert_eq!(
        entry.bytes_in_flight, BBR_MAX_SEND_QUANTUM_BYTES as u64,
        "an old output ACK must not debit replacement flight accounting"
    );
    assert_eq!(entry.owner_data_acked_bytes, 0);
    assert_eq!(entry.delivery_samples, 0);
    assert!(entry.product_progress_rate_bps.is_none());
}

#[test]
fn late_old_output_record_cannot_account_or_prove_replacement() {
    let session_id = SessionId(95);
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let (old_commands, old_receivers) = reliable_path_command_channels(8);
    let old_commands_for_detach = old_commands.clone();
    let binding = ResponseStreamBinding::new(
        session_id,
        key.underlay,
        key.path_id,
        old_commands,
        FlowLane::Throughput,
    );
    let stale_target = binding
        .sender_path_targets(FlowLane::Throughput, BBR_MAX_SEND_QUANTUM_BYTES)
        .into_iter()
        .next()
        .expect("initial target exists");
    drop(old_receivers);
    binding.detach(key, &old_commands_for_detach);
    assert_eq!(binding.ordered_data_owner(), None);

    let (new_commands, _new_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            key.underlay,
            key.path_id,
            new_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            reliable_relay_buffer_len(MuxLimits::default()),
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let frame = stream_data_frame(BBR_MAX_SEND_QUANTUM_BYTES);
    binding.record_owner_flight_for_target(&stale_target, &frame);
    assert!(
        !binding.commit_ordered_data_owner_for_target(&stale_target),
        "a stale plan must not restore ownership after detach"
    );
    assert_eq!(binding.ordered_data_owner(), None);
    assert!(
        binding
            .sender_path_targets(FlowLane::Throughput, BBR_MAX_SEND_QUANTUM_BYTES)
            .iter()
            .all(|target| !target.is_active),
        "a same-key Validation replacement must not inherit stale Service ownership"
    );
    assert_eq!(output_entry_for_key(&binding, key).bytes_in_flight, 0);
    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: BBR_MAX_SEND_QUANTUM_BYTES as u64,
    }]);

    let entry = output_entry_for_key(&binding, key);
    assert_eq!(entry.bytes_in_flight, 0);
    assert_eq!(entry.owner_data_acked_bytes, 0);
    assert_eq!(entry.delivery_samples, 0);
    assert!(entry.product_progress_rate_bps.is_none());
}

#[test]
fn old_acked_hole_cannot_prove_replacement_when_frontier_advances() {
    let session_id = SessionId(96);
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let (old_commands, old_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        session_id,
        key.underlay,
        key.path_id,
        old_commands,
        FlowLane::Throughput,
    );
    binding.record_owner_flight(key, &stream_data_frame_at(0, 1024));
    binding.record_owner_flight(key, &stream_data_frame_at(1024, 1024));
    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 1024,
        end: 2048,
    }]);
    drop(old_receivers);

    let (new_commands, _new_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            key.underlay,
            key.path_id,
            new_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            reliable_relay_buffer_len(MuxLimits::default()),
        ),
        ResponseStreamAttachOutcome::ReplacedClosedOutput
    );
    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: 1024,
    }]);

    let entry = output_entry_for_key(&binding, key);
    assert_eq!(entry.owner_data_acked_bytes, 0);
    assert_eq!(entry.delivery_samples, 0);
    assert!(entry.product_progress_rate_bps.is_none());
}

#[test]
fn late_record_from_pre_role_change_plan_is_not_path_proving() {
    let (binding, _) = binding_for_underlay(UnderlayProtocol::Tcp);
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (commands, _receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            key.underlay,
            key.path_id,
            commands.clone(),
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            reliable_relay_buffer_len(MuxLimits::default()),
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let stale_target = binding
        .sender_path_targets(FlowLane::Throughput, BBR_MAX_SEND_QUANTUM_BYTES)
        .into_iter()
        .find(|target| target.key == key)
        .expect("validation target is attached");
    assert_eq!(
        binding.attach(
            key.underlay,
            key.path_id,
            commands,
            FlowLane::Throughput,
            StreamOpenRole::Repair,
            reliable_relay_buffer_len(MuxLimits::default()),
        ),
        ResponseStreamAttachOutcome::RoleChanged
    );

    let frame = stream_data_frame(BBR_MAX_SEND_QUANTUM_BYTES);
    binding.record_owner_flight_for_target(&stale_target, &frame);
    assert_eq!(
        output_entry_for_key(&binding, key).bytes_in_flight,
        BBR_MAX_SEND_QUANTUM_BYTES as u64,
        "a late record on the same live channel must follow the new incarnation as non-proving debt"
    );
    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: BBR_MAX_SEND_QUANTUM_BYTES as u64,
    }]);

    let outputs = binding.outputs.lock().expect("test response outputs lock");
    let entry = outputs
        .entries
        .iter()
        .find(|entry| entry.key == key)
        .expect("role-changed output remains attached");
    assert_eq!(entry.bytes_in_flight, 0);
    assert_eq!(entry.owner_data_acked_bytes, 0);
    assert_eq!(entry.delivery_samples, 0);
    assert!(entry.product_progress_rate_bps.is_none());
}

#[test]
fn pre_role_change_acked_hole_cannot_restore_delivery_evidence() {
    let (binding, _) = binding_for_underlay(UnderlayProtocol::Tcp);
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (commands, _receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            key.underlay,
            key.path_id,
            commands.clone(),
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            reliable_relay_buffer_len(MuxLimits::default()),
        ),
        ResponseStreamAttachOutcome::Attached
    );
    binding.record_owner_flight(key, &stream_data_frame_at(0, 1024));
    binding.record_owner_flight(key, &stream_data_frame_at(1024, 1024));
    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 1024,
        end: 2048,
    }]);

    assert_eq!(
        binding.attach(
            key.underlay,
            key.path_id,
            commands,
            FlowLane::Throughput,
            StreamOpenRole::Repair,
            reliable_relay_buffer_len(MuxLimits::default()),
        ),
        ResponseStreamAttachOutcome::RoleChanged
    );
    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: 1024,
    }]);

    let outputs = binding.outputs.lock().expect("test response outputs lock");
    let entry = outputs
        .entries
        .iter()
        .find(|entry| entry.key == key)
        .expect("role-changed output remains attached");
    assert_eq!(entry.owner_data_acked_bytes, 0);
    assert_eq!(entry.delivery_samples, 0);
    assert!(entry.product_progress_rate_bps.is_none());
}

#[test]
fn response_acked_hole_debt_counts_unique_ordering_owner_only() {
    let (binding, owner) = binding_for_underlay(UnderlayProtocol::Udp);
    let duplicate = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (duplicate_commands, _duplicate_receivers) = reliable_path_command_channels(8);
    binding.attach(
        duplicate.underlay,
        duplicate.path_id,
        duplicate_commands,
        FlowLane::Throughput,
        StreamOpenRole::Repair,
        reliable_relay_buffer_len(MuxLimits::default()),
    );
    let lower_missing = stream_data_frame_at(0, 1024);
    let later = stream_data_frame_at(1024, 4096);
    binding.record_owner_flight(owner, &lower_missing);
    binding.record_owner_flight(owner, &later);
    binding.record_repair_flight(duplicate, &later);

    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 1024,
        end: 5120,
    }]);

    let lower = binding.lower_flights_before_offset(5120);
    assert_eq!(lower.len(), 1);
    assert_eq!(lower[0].key, owner);
    assert_eq!(
        lower[0].bytes, 4096,
        "acked hole debt must not double-count repair duplicate copies"
    );
    let ordering = binding
        .ack_ordering
        .lock()
        .expect("server response ACK ordering lock");
    assert_eq!(ordering.acked_hole_bytes(), 4096);
}
