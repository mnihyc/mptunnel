use super::super::evidence::ServerPathMetricsSource;
use super::super::test_support::{
    binding_for_underlay, mark_test_quic_output_carrier_bulk_proven, output_entry_for_key,
    stream_data_frame,
};
use super::{RESPONSE_OWNER_MIXED_SEEN, RESPONSE_OWNER_TCP_SEEN, ResponseStreamAttachOutcome};
use crate::model::capacity::{
    BBR_MAX_SEND_QUANTUM_BYTES, MIN_RATE_SAMPLE_BYTES, PATH_OPEN_SCORE_BYTES,
    RELIABLE_INITIAL_WINDOW_PACKETS, reliable_subflow_startup_sample_limit_bytes,
};
use crate::model::multipath::{FlowSubflowSet, PathAdmission, SubflowAdmissionInput};
use crate::model::path::CarrierPathKey;
use crate::mux::MuxLimits;
use crate::protocol::{
    Frame, OffsetRange, PathId, PathMetricDirection, PathMetrics, StreamOpenRole, UnderlayProtocol,
};
use crate::runtime::path::commands::{
    ReliablePathCommand, reliable_path_command_channels, try_recv_reliable_path_priority_command,
};
use crate::runtime::path::model::metric_epoch_now;
use crate::runtime::relay::io::reliable_stream_recv_progress_interval;
use crate::scheduler::FlowLane;
use std::sync::atomic::Ordering;

#[test]
fn response_validation_attach_adds_output_without_promoting_lead() {
    let (binding, active) = binding_for_underlay(UnderlayProtocol::Udp);
    let validation = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (validation_commands, mut validation_receivers) = reliable_path_command_channels(8);

    assert_eq!(binding.ordered_data_owner(), Some(active));
    assert_eq!(
        binding.attach(
            validation.underlay,
            validation.path_id,
            validation_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let outputs = binding.outputs.lock().expect("test response outputs lock");
    assert_eq!(outputs.entries.len(), 2);
    assert!(outputs.entries.iter().any(|entry| entry.key == validation));
    drop(outputs);
    assert_eq!(
        binding.ordered_data_owner(),
        Some(active),
        "validation attachment opens a carrier output but is not scheduler ownership"
    );
    match try_recv_reliable_path_priority_command(&mut validation_receivers) {
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData {
            path_id, payload, ..
        })) => {
            assert_eq!(path_id, validation.path_id);
            assert!(!payload.is_empty());
        }
        _ => panic!("validation attach must enqueue carrier path proof"),
    }
}

#[test]
fn response_repair_output_requires_explicit_active_reannounce() {
    let (binding, active) = binding_for_underlay(UnderlayProtocol::Tcp);
    let repair = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (repair_commands, _repair_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.request_active_underlay(),
        Some(UnderlayProtocol::Tcp)
    );

    assert_eq!(
        binding.attach(
            repair.underlay,
            repair.path_id,
            repair_commands.clone(),
            FlowLane::Latency,
            StreamOpenRole::Repair,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    assert_eq!(
        binding.owner_underlay_history.load(Ordering::Acquire),
        RESPONSE_OWNER_TCP_SEEN,
        "Repair attachment must not disable the single-family fast path"
    );

    assert_eq!(
        binding.attach(
            repair.underlay,
            repair.path_id,
            repair_commands.clone(),
            FlowLane::Latency,
            StreamOpenRole::Validation,
        ),
        ResponseStreamAttachOutcome::Attached,
        "same-channel Validation cannot weaken an existing Repair role"
    );
    assert_eq!(
        binding.owner_underlay_history.load(Ordering::Acquire),
        RESPONSE_OWNER_TCP_SEEN,
        "an ineffective Validation request must not poison family history"
    );

    assert_eq!(
        binding.attach(
            repair.underlay,
            repair.path_id,
            repair_commands,
            FlowLane::Latency,
            StreamOpenRole::Active,
        ),
        ResponseStreamAttachOutcome::RoleChanged,
        "explicit Active reannounce may promote future work without changing old repair-flight semantics"
    );

    let mut outputs = binding.outputs.lock().expect("test response outputs lock");
    let repair_entry = outputs
        .entries
        .iter_mut()
        .find(|entry| entry.key == repair)
        .expect("repair output remains attached");
    assert_eq!(repair_entry.role, StreamOpenRole::Active);
    repair_entry.srtt_ms = Some(40.0);
    outputs
        .entries
        .iter_mut()
        .find(|entry| entry.key == active)
        .expect("response Service output remains attached")
        .srtt_ms = Some(500.0);
    drop(outputs);
    assert_eq!(binding.ordered_data_owner(), Some(active));
    assert_eq!(
        binding.request_active_owner(),
        Some(repair),
        "request Active reannounce must not depend on the response data owner"
    );
    assert_eq!(
        binding.request_active_underlay(),
        Some(UnderlayProtocol::Udp),
        "server receive-progress policy follows the current request Active family"
    );
    let request_active_snapshot = binding
        .request_active_path_snapshot(FlowLane::Throughput)
        .expect("request Active output remains attached");
    let response_service_snapshot = binding
        .send_path_snapshot(FlowLane::Throughput, PATH_OPEN_SCORE_BYTES)
        .expect("response Service output remains attached");
    assert_eq!(request_active_snapshot.id, repair.path_id);
    assert_eq!(request_active_snapshot.underlay, UnderlayProtocol::Udp);
    assert_eq!(response_service_snapshot.id, active.path_id);
    assert_eq!(response_service_snapshot.underlay, UnderlayProtocol::Tcp);
    assert!(
        reliable_stream_recv_progress_interval(Some(request_active_snapshot))
            < reliable_stream_recv_progress_interval(Some(response_service_snapshot)),
        "receive-progress cadence must follow the request Active PTO rather than the response Service PTO"
    );
    assert_eq!(
        binding.owner_underlay_history.load(Ordering::Acquire),
        RESPONSE_OWNER_MIXED_SEEN
    );
}

#[test]
fn response_sender_targets_active_path_follows_ordered_data_owner_not_output_tail() {
    let (binding, active) = binding_for_underlay(UnderlayProtocol::Udp);
    let validation = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (validation_commands, _validation_receivers) = reliable_path_command_channels(8);

    assert_eq!(
        binding.attach(
            validation.underlay,
            validation.path_id,
            validation_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let targets = binding.sender_path_targets(FlowLane::Throughput, 4096);
    assert!(
        targets
            .iter()
            .find(|target| target.observation.key == active)
            .is_some_and(
                |target| target.observation.is_service && target.observation.is_request_active
            ),
        "the initial active output remains the scheduler-active target"
    );
    assert!(
        targets
            .iter()
            .find(|target| target.observation.key == validation)
            .is_some_and(|target| !target.observation.is_service),
        "validation output must not be active before lead migration"
    );

    binding.set_ordered_data_owner(validation);

    let targets = binding.sender_path_targets(FlowLane::Throughput, 4096);
    assert!(
        targets
            .iter()
            .find(|target| target.observation.key == validation)
            .is_some_and(
                |target| target.observation.is_service && !target.observation.is_request_active
            ),
        "scheduler-active target must follow ordered_data_owner after migration"
    );
    assert!(
        targets
            .iter()
            .find(|target| target.observation.key == active)
            .is_some_and(
                |target| !target.observation.is_service && target.observation.is_request_active
            ),
        "response owner migration must not overwrite the request Active identity"
    );
}

#[test]
fn response_duplicate_active_attach_with_different_channel_is_rejected() {
    let (binding, active) = binding_for_underlay(UnderlayProtocol::Udp);
    let validation = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (validation_commands, _validation_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            validation.underlay,
            validation.path_id,
            validation_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let before = {
        let outputs = binding.outputs.lock().expect("test response outputs lock");
        outputs
            .entries
            .iter()
            .map(|entry| entry.key)
            .collect::<Vec<_>>()
    };
    let (duplicate_commands, _duplicate_receivers) = reliable_path_command_channels(8);

    assert_eq!(
        binding.attach(
            validation.underlay,
            validation.path_id,
            duplicate_commands,
            FlowLane::Throughput,
            StreamOpenRole::Active,
        ),
        ResponseStreamAttachOutcome::RejectedDuplicateLiveOutput
    );

    let after = {
        let outputs = binding.outputs.lock().expect("test response outputs lock");
        outputs
            .entries
            .iter()
            .map(|entry| entry.key)
            .collect::<Vec<_>>()
    };
    assert_eq!(after, before);
    assert_eq!(binding.ordered_data_owner(), Some(active));
}

#[test]
fn response_validation_same_channel_active_attach_does_not_promote_service_owner() {
    let (binding, active) = binding_for_underlay(UnderlayProtocol::Tcp);
    let validation = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (validation_commands, _validation_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            validation.underlay,
            validation.path_id,
            validation_commands.clone(),
            FlowLane::Throughput,
            StreamOpenRole::Validation,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    assert_eq!(binding.ordered_data_owner(), Some(active));

    assert_eq!(
        binding.attach(
            validation.underlay,
            validation.path_id,
            validation_commands,
            FlowLane::Throughput,
            StreamOpenRole::Active,
        ),
        ResponseStreamAttachOutcome::RoleChanged
    );

    let outputs = binding.outputs.lock().expect("test response outputs lock");
    assert_eq!(
        outputs
            .entries
            .iter()
            .filter(|entry| entry.key == validation)
            .count(),
        1,
        "same-channel Active reannouncement updates the existing output instead of opening a duplicate"
    );
    drop(outputs);
    assert_eq!(
        binding.ordered_data_owner(),
        Some(active),
        "Active reannouncement is attachment state, not Service ownership"
    );
}

#[test]
fn response_detaching_service_owner_does_not_promote_probe_only_survivor_to_service() {
    let (binding, active) = binding_for_underlay(UnderlayProtocol::Tcp);
    let survivor = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (survivor_commands, _survivor_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            survivor.underlay,
            survivor.path_id,
            survivor_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    binding.update_path_metrics(
        survivor,
        PathMetrics {
            path_id: survivor.path_id,
            underlay: survivor.underlay,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: metric_epoch_now(),
            metric_age_us: 0,
            srtt_us: 20_000,
            rttvar_us: 1_000,
            jitter_us: 1_000,
            delivery_rate_bps: 200_000_000,
            pacing_rate_bps: 200_000_000,
            loss_ppm: 0,
            ecn_ppm: 0,
            loss_observed: false,
            ecn_observed: false,
            bytes_in_flight: 0,
            queue_bytes: 0,
            inflight_limit_bytes: 0,
            inflight_hi_bytes: 0,
            confidence_ppm: 1_000_000,
            app_limited: true,
            has_ack_derived_data_sample: false,
            data_sample_count: 0,
            data_sample_bytes: 0,
        },
        ServerPathMetricsSource::LocalSender,
    );

    let active_commands = {
        let outputs = binding.outputs.lock().expect("test response outputs lock");
        outputs
            .entries
            .iter()
            .find(|entry| entry.key == active)
            .expect("active output exists")
            .commands
            .clone()
    };
    binding.detach(active, &active_commands);

    assert_eq!(
        binding.ordered_data_owner(),
        None,
        "proof/liveness evidence is not enough to promote a failover Service owner"
    );
    let targets = binding.sender_path_targets(FlowLane::Throughput, 4096);
    assert!(
        targets
            .iter()
            .find(|target| target.observation.key == survivor)
            .is_some_and(
                |target| !target.observation.is_service && target.observation.has_sender_evidence
            ),
        "probe-only survivor stays attached for validation but is not scheduler-active"
    );
}

#[test]
fn response_detaching_service_owner_does_not_promote_ack_data_survivor() {
    let (binding, active) = binding_for_underlay(UnderlayProtocol::Tcp);
    let survivor = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (survivor_commands, _survivor_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            survivor.underlay,
            survivor.path_id,
            survivor_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    binding.update_path_metrics(
        survivor,
        PathMetrics {
            path_id: survivor.path_id,
            underlay: survivor.underlay,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: metric_epoch_now(),
            metric_age_us: 0,
            srtt_us: 20_000,
            rttvar_us: 1_000,
            jitter_us: 1_000,
            delivery_rate_bps: 614_000,
            pacing_rate_bps: 1_000_000,
            loss_ppm: 0,
            ecn_ppm: 0,
            loss_observed: false,
            ecn_observed: false,
            bytes_in_flight: PATH_OPEN_SCORE_BYTES as u64,
            queue_bytes: 0,
            inflight_limit_bytes: PATH_OPEN_SCORE_BYTES as u64,
            inflight_hi_bytes: PATH_OPEN_SCORE_BYTES as u64,
            confidence_ppm: 1_000_000,
            app_limited: true,
            has_ack_derived_data_sample: true,
            data_sample_count: 1,
            data_sample_bytes: 1,
        },
        ServerPathMetricsSource::LocalSender,
    );

    let active_commands = {
        let outputs = binding.outputs.lock().expect("test response outputs lock");
        outputs
            .entries
            .iter()
            .find(|entry| entry.key == active)
            .expect("active output exists")
            .commands
            .clone()
    };
    binding.detach(active, &active_commands);

    assert_eq!(
        binding.ordered_data_owner(),
        None,
        "carrier output detachment is not a Service ownership transfer; later OwnerData must wait for frontier-clear admission or repair"
    );
    let targets = binding.sender_path_targets(FlowLane::Throughput, 4096);
    assert!(
        targets
            .iter()
            .find(|target| target.observation.key == survivor)
            .is_some_and(
                |target| !target.observation.is_service && target.observation.has_sender_evidence
            ),
        "ACK-data survivor remains attached evidence, not the scheduler-active Service"
    );
}

#[test]
fn response_service_detach_does_not_pick_measured_survivor_by_output_tail() {
    let (binding, active) = binding_for_underlay(UnderlayProtocol::Tcp);
    let measured = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let probe_only_tail = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    let (measured_commands, _measured_receivers) = reliable_path_command_channels(8);
    let (probe_commands, _probe_receivers) = reliable_path_command_channels(8);

    assert_eq!(
        binding.attach(
            measured.underlay,
            measured.path_id,
            measured_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    binding.update_path_metrics(
        measured,
        PathMetrics {
            path_id: measured.path_id,
            underlay: measured.underlay,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: metric_epoch_now(),
            metric_age_us: 0,
            srtt_us: 20_000,
            rttvar_us: 1_000,
            jitter_us: 1_000,
            delivery_rate_bps: 200_000_000,
            pacing_rate_bps: 200_000_000,
            loss_ppm: 0,
            ecn_ppm: 0,
            loss_observed: false,
            ecn_observed: false,
            bytes_in_flight: 0,
            queue_bytes: 0,
            inflight_limit_bytes: 0,
            inflight_hi_bytes: 0,
            confidence_ppm: 1_000_000,
            app_limited: false,
            has_ack_derived_data_sample: true,
            data_sample_count: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
            data_sample_bytes: MIN_RATE_SAMPLE_BYTES,
        },
        ServerPathMetricsSource::LocalSender,
    );
    assert_eq!(
        binding.attach(
            probe_only_tail.underlay,
            probe_only_tail.path_id,
            probe_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let active_commands = {
        let outputs = binding.outputs.lock().expect("test response outputs lock");
        outputs
            .entries
            .iter()
            .find(|entry| entry.key == active)
            .expect("active output exists")
            .commands
            .clone()
    };
    binding.detach(active, &active_commands);

    assert_eq!(
        binding.ordered_data_owner(),
        None,
        "output membership changes are not Service admission; measured survivors compete only when ordered debt is clear"
    );
}

#[test]
fn live_role_change_clears_evidence_and_invalidates_old_flights() {
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
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let frame = stream_data_frame(BBR_MAX_SEND_QUANTUM_BYTES);
    binding.record_owner_flight(key, &frame);
    let before_role_change = output_entry_for_key(&binding, key);
    assert_eq!(
        before_role_change.bytes_in_flight,
        BBR_MAX_SEND_QUANTUM_BYTES as u64
    );
    let previous_incarnation = before_role_change.incarnation;
    assert_eq!(
        binding.attach(
            key.underlay,
            key.path_id,
            commands,
            FlowLane::Throughput,
            StreamOpenRole::Repair,
        ),
        ResponseStreamAttachOutcome::RoleChanged
    );
    let after_role_change = output_entry_for_key(&binding, key);
    assert_ne!(after_role_change.incarnation, previous_incarnation);
    assert_eq!(
        after_role_change.bytes_in_flight, BBR_MAX_SEND_QUANTUM_BYTES as u64,
        "live role change must preserve actual outstanding product debt"
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
    assert_eq!(entry.role, StreamOpenRole::Repair);
    assert_eq!(entry.bytes_in_flight, 0);
    assert_eq!(entry.owner_data_acked_bytes, 0);
    assert_eq!(entry.delivery_samples, 0);
    assert!(entry.product_progress_rate_bps.is_none());
}

#[test]
fn validation_to_active_preserves_response_identity_evidence_and_subflow_epoch() {
    let limits = MuxLimits::default();
    let sample_bytes = reliable_subflow_startup_sample_limit_bytes(limits) as usize;
    let (binding, service) = binding_for_underlay(UnderlayProtocol::Udp);
    let candidate = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (commands, _receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            candidate.underlay,
            candidate.path_id,
            commands.clone(),
            FlowLane::Throughput,
            StreamOpenRole::Validation,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let startup_input = SubflowAdmissionInput {
        key: candidate,
        bulk_rate_proven: false,
        startup_owner_allowed: true,
        frontier_clear: true,
        completion_improves: false,
        observed_goodput_non_degrading: true,
        owner_bytes: sample_bytes,
    };
    assert_eq!(
        binding.commit_subflow_owner_admission(service, sample_bytes, startup_input,),
        PathAdmission::Subflow
    );
    let incarnation = {
        let mut outputs = binding.outputs.lock().expect("test response outputs lock");
        let entry = outputs
            .entries
            .iter_mut()
            .find(|entry| entry.key == candidate)
            .expect("Validation output");
        mark_test_quic_output_carrier_bulk_proven(entry, limits);
        entry.incarnation
    };
    let (planner_generation, epoch) = binding.subflow_state_snapshot();
    assert_eq!(
        epoch.as_ref().and_then(FlowSubflowSet::startup_owner_key),
        Some(candidate)
    );

    assert_eq!(
        binding.attach(
            candidate.underlay,
            candidate.path_id,
            commands,
            FlowLane::Throughput,
            StreamOpenRole::Active,
        ),
        ResponseStreamAttachOutcome::RoleChanged
    );

    let target = binding
        .sender_path_targets(FlowLane::Throughput, BBR_MAX_SEND_QUANTUM_BYTES)
        .into_iter()
        .find(|target| target.observation.key == candidate)
        .expect("promoted response output");
    assert_eq!(target.observation.incarnation, incarnation);
    assert!(target.observation.has_bulk_rate_evidence);
    let (after_generation, after_epoch) = binding.subflow_state_snapshot();
    assert_eq!(after_generation, planner_generation);
    assert_eq!(
        after_epoch.and_then(|epoch| epoch.startup_owner_key()),
        Some(candidate),
        "request-role promotion cannot erase paid-for response membership"
    );
}
