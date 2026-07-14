use super::super::handle::{ReliablePathStreamHandle, ReliablePathStreamOutput};
use super::super::response_placement::ResponseServiceHandoffMode;
use super::response_ack_clock::RESPONSE_ACK_CLOCK_GOODPUT_MIN_ELAPSED;
use super::response_evidence::server_output_fresh_quic_capacity_proof;
use super::response_handoff::ResponseServiceHandoffDrainRequest;
use super::response_handoff_commit::ResponseServiceHandoffRequest;
use super::response_quic_capacity::ServerQuicCapacityCalibrationPhase;
use super::response_quic_probe::ResponseQuicCapacityCalibrationRequest;
use super::*;
use crate::model::admission::{
    ReliableSourceServiceStagingContext, ReliableSourceStagingContext,
    bulk_service_feed_reservoir_payload_bytes, reliable_relay_source_staging_owner_tail_headroom,
};
use crate::model::capacity::{
    BBR_MAX_SEND_QUANTUM_BYTES, MIN_RATE_SAMPLE_BYTES, PATH_OPEN_SCORE_BYTES,
    RELIABLE_INITIAL_WINDOW_PACKETS, RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES,
};
use crate::protocol::{PathMetricDirection, StreamFlags};
use crate::runtime::path::commands::{
    reliable_path_command_channels, reliable_path_command_pending_bytes,
    try_recv_reliable_path_command, try_recv_reliable_path_priority_command,
};
use crate::runtime::path::model::{default_path_rate_bps, metric_epoch_now};
use crate::runtime::path::proof::PathProofTracker;
use crate::runtime::relay::io::reliable_stream_recv_progress_interval;
use crate::runtime::relay::io::{
    adaptive_reliable_relay_chunk_bytes, bbr_min_send_quantum_bytes, reliable_relay_buffer_len,
};
use crate::runtime::relay_striping::reliable_stream_frame_payload_bytes;
use crate::scheduler::PathRateScope;
use bytes::Bytes;
use std::sync::{TryLockError, mpsc as std_mpsc};
use std::time::Duration;

fn binding_for_underlay(
    underlay: UnderlayProtocol,
) -> (Arc<ResponseStreamBinding>, CarrierPathKey) {
    let (commands, _receivers) = reliable_path_command_channels(8);
    let key = CarrierPathKey {
        underlay,
        path_id: PathId(0),
    };
    let binding = ResponseStreamBinding::new(
        SessionId(42),
        underlay,
        key.path_id,
        commands,
        FlowLane::Throughput,
    );
    (binding, key)
}

fn stream_data_frame(payload_len: usize) -> Frame {
    stream_data_frame_at(0, payload_len)
}

fn stream_data_frame_at(offset: u64, payload_len: usize) -> Frame {
    Frame::StreamData {
        stream_id: StreamId(7),
        offset,
        flags: StreamFlags::NONE,
        payload: Bytes::from(vec![0x5a; payload_len]),
    }
}

fn test_ack_clock_rate_sample(bytes: u64, rate_bps: f64) -> PathRateSample {
    PathRateSample::new(
        bytes,
        Duration::from_secs_f64(bytes as f64 * 8.0 / rate_bps),
    )
    .expect("valid ACK-clock rate sample")
}

fn assert_test_rate_close(actual: Option<f64>, expected: f64) {
    let actual = actual.expect("calibrated rate");
    assert!((actual - expected).abs() / expected.max(1.0) < 1e-6);
}

fn first_output_entry(binding: &ResponseStreamBinding) -> ResponseStreamOutputEntry {
    binding
        .outputs
        .lock()
        .expect("test response outputs lock")
        .entries
        .first()
        .expect("test response binding has output")
        .clone()
}

fn mark_test_response_output_bulk_proven(
    entry: &mut ResponseStreamOutputEntry,
    mux_limits: MuxLimits,
) {
    entry.product_progress_rate_bps = Some(100_000_000.0);
    entry.delivery_rate_bps = Some(100_000_000.0);
    entry.delivery_samples = 1;
    entry.owner_data_acked_bytes = reliable_subflow_startup_sample_limit_bytes(mux_limits);
}

fn mark_test_quic_output_carrier_bulk_proven(
    entry: &mut ResponseStreamOutputEntry,
    mux_limits: MuxLimits,
) {
    let sample_bytes = reliable_subflow_startup_sample_limit_bytes(mux_limits);
    entry.local_path_metrics = Some(ServerPathMetricsEntry {
        source: ServerPathMetricsSource::LocalSender,
        recorded_at: Instant::now(),
        capacity_proof: None,
        tcp_capacity_proof: None,
        metrics: PathMetrics {
            path_id: entry.key.path_id,
            underlay: UnderlayProtocol::Udp,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: 1,
            metric_age_us: 0,
            min_rtt_us: 10_000,
            srtt_us: 12_000,
            rttvar_us: 1_000,
            jitter_us: 1_000,
            delivery_rate_bps: 500_000_000,
            pacing_rate_bps: 500_000_000,
            loss_ppm: 0,
            ecn_ppm: 0,
            loss_observed: false,
            ecn_observed: false,
            bytes_in_flight: 0,
            queue_bytes: 0,
            inflight_limit_bytes: sample_bytes,
            inflight_hi_bytes: sample_bytes,
            confidence_ppm: 1_000_000,
            app_limited: false,
            has_ack_derived_data_sample: true,
            data_sample_count: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
            data_sample_bytes: sample_bytes,
        },
    });
}

fn test_quic_capacity_proof(
    mux_limits: MuxLimits,
    token: u64,
    proof_validity: Duration,
) -> QuicCapacityProofCandidate {
    let proof_bytes = reliable_subflow_startup_sample_limit_bytes(mux_limits);
    let accounting_slack_bytes = (PATH_OPEN_SCORE_BYTES as u64).min(proof_bytes / 8);
    let proof_elapsed = Duration::from_millis(2);
    let accepted_at = Instant::now();
    QuicCapacityProofCandidate {
        token,
        train_bytes: proof_bytes,
        sample_floor_bytes: proof_bytes,
        accounting_slack_bytes,
        warmup_bytes: 0,
        required_proof_bytes: proof_bytes - accounting_slack_bytes,
        written_bytes: proof_bytes,
        written_data_frame_count: 1,
        receipt_confirmed: true,
        received_bytes: proof_bytes,
        proof_elapsed,
        rate_bps: quic_capacity_receipt_rate_bps(proof_bytes, proof_elapsed)
            .expect("test receipt rate"),
        accepted_at,
        expires_at: accepted_at + proof_validity,
        proof_validity,
    }
}

fn mark_test_quic_output_receipt_bulk_proven(
    entry: &mut ResponseStreamOutputEntry,
    mux_limits: MuxLimits,
    token: u64,
    proof_validity: Duration,
) -> QuicCapacityProofCandidate {
    mark_test_quic_output_carrier_bulk_proven(entry, mux_limits);
    let proof = test_quic_capacity_proof(mux_limits, token, proof_validity);
    let path_metrics = entry
        .local_path_metrics
        .as_mut()
        .expect("test QUIC metrics");
    // Keep receipt proof as the only bulk authority so expiry is observable.
    path_metrics.metrics.app_limited = true;
    path_metrics.metrics.has_ack_derived_data_sample = false;
    path_metrics.metrics.confidence_ppm = 0;
    path_metrics.metrics.data_sample_count = 0;
    path_metrics.metrics.data_sample_bytes = 0;
    path_metrics.capacity_proof = Some(proof);
    proof
}

#[test]
fn quic_capacity_calibration_uses_carrier_bytes_without_product_flight() {
    let mux_limits = MuxLimits::default();
    let session_id = SessionId(510);
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
        tracker,
    );
    let candidate = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (candidate_commands, mut candidate_receivers) = reliable_path_command_channels(8);
    binding.attach(
        candidate.underlay,
        candidate.path_id,
        candidate_commands,
        FlowLane::Throughput,
        StreamOpenRole::Validation,
        mux_limits.max_payload_bytes,
    );
    assert!(matches!(
        try_recv_reliable_path_command(&mut candidate_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
    ));

    let (planner_generation, _) = binding.subflow_state_snapshot();
    let scheduling = binding.response_scheduling_snapshot();
    let model_generation = binding.response_model_generation();
    let target = binding
        .sender_path_targets(FlowLane::Throughput, 64 * 1024)
        .into_iter()
        .find(|target| target.key == candidate)
        .expect("UDP Validation target");
    let train_bytes = mux_limits
        .max_payload_bytes
        .saturating_add(PATH_OPEN_SCORE_BYTES);
    let sample_floor_bytes = train_bytes as u64;
    let accounting_slack_bytes = (PATH_OPEN_SCORE_BYTES as u64).min(sample_floor_bytes / 8);
    let required_proof_bytes = sample_floor_bytes - accounting_slack_bytes;
    assert!(binding.try_start_quic_capacity_calibration(
        &target,
        ResponseQuicCapacityCalibrationRequest {
            expected_planner_generation: planner_generation,
            expected_lane_generation: scheduling.generation,
            expected_model_generation: model_generation,
            target: candidate,
            target_path_instance_id: target.path_instance_id,
            target_incarnation: target.incarnation,
            target_pending_bytes: target.command_pending_bytes,
            train_bytes,
            sample_floor_bytes,
            accounting_slack_bytes,
            fresh_strict_window_bytes: required_proof_bytes,
            carrier_window_bytes: 0,
            lease: Duration::from_secs(1),
            proof_validity: Duration::from_secs(3),
        },
    ));
    let probe = match try_recv_reliable_path_command(&mut candidate_receivers)
        .expect("capacity probe command")
    {
        ReliablePathCommand::SendQuicCapacityProbe(probe) => probe,
        _ => panic!("expected typed QUIC capacity probe"),
    };
    assert_ne!(probe.calibration_id, 0);
    assert_eq!(probe.path_id, candidate.path_id);
    assert_eq!(probe.train_payload_bytes, train_bytes as u64);
    assert_eq!(probe.sample_floor_bytes, sample_floor_bytes);
    assert_eq!(probe.warmup_carrier_bytes, 0);
    assert_eq!(probe.required_timed_carrier_bytes, required_proof_bytes);
    assert!(probe.expires_at > Instant::now());
    assert!(
        binding
            .flights
            .lock()
            .expect("test response flight lock")
            .is_empty(),
        "carrier capacity bytes must not enter product ownership"
    );
    assert_eq!(
        binding.ordered_data_owner(),
        Some(CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        })
    );
    assert!(
        binding
            .response_scheduling_snapshot()
            .quic_capacity_calibration_reserved
    );
    let generic_bulk_metrics = {
        let mut entry = binding
            .outputs
            .lock()
            .expect("test response outputs lock")
            .entries
            .iter()
            .find(|entry| entry.key == candidate)
            .expect("UDP capacity candidate")
            .clone();
        mark_test_quic_output_carrier_bulk_proven(&mut entry, mux_limits);
        entry
            .local_path_metrics
            .expect("generic local bulk metrics")
            .metrics
    };
    binding.update_path_metrics(
        candidate,
        generic_bulk_metrics,
        ServerPathMetricsSource::LocalSender,
    );
    assert!(
        binding
            .response_scheduling_snapshot()
            .quic_capacity_calibration_reserved,
        "generic path metrics cannot complete a token-owned capacity train"
    );
}

#[test]
fn generic_metrics_preserve_but_do_not_extend_fixed_capacity_proof_deadline() {
    let mux_limits = MuxLimits::default();
    let (commands, _receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits(
        SessionId(521),
        UnderlayProtocol::Udp,
        PathId(6),
        commands,
        FlowLane::Throughput,
        mux_limits,
    );
    let mut output = first_output_entry(&binding);
    mark_test_quic_output_carrier_bulk_proven(&mut output, mux_limits);
    let metrics = output
        .local_path_metrics
        .expect("test QUIC metrics")
        .metrics;
    let accepted_at = Instant::now();
    let expires_at = accepted_at + Duration::from_millis(20);
    let proof_bytes = reliable_subflow_startup_sample_limit_bytes(mux_limits);
    let accounting_slack = (PATH_OPEN_SCORE_BYTES as u64).min(proof_bytes / 8);
    let required_proof_bytes = proof_bytes - accounting_slack;
    let proof_elapsed = Duration::from_millis(10);
    let proof = QuicCapacityProofCandidate {
        token: 77,
        train_bytes: proof_bytes,
        sample_floor_bytes: proof_bytes,
        accounting_slack_bytes: accounting_slack,
        warmup_bytes: 0,
        required_proof_bytes,
        written_bytes: proof_bytes,
        written_data_frame_count: RELIABLE_INITIAL_WINDOW_PACKETS as u64,
        receipt_confirmed: true,
        received_bytes: proof_bytes,
        proof_elapsed,
        rate_bps: quic_capacity_receipt_rate_bps(proof_bytes, proof_elapsed)
            .expect("valid receipt rate"),
        accepted_at,
        expires_at,
        proof_validity: Duration::from_millis(20),
    };
    assert!(binding.install_quic_capacity_proof_for_instance(
        output.key,
        output.path_instance_id,
        metrics,
        proof,
    ));
    binding.update_path_metrics(
        output.key,
        PathMetrics {
            delivery_rate_bps: metrics.delivery_rate_bps / 2,
            ..metrics
        },
        ServerPathMetricsSource::LocalSender,
    );
    assert_eq!(
        first_output_entry(&binding)
            .local_path_metrics
            .and_then(|entry| entry.capacity_proof)
            .map(|proof| proof.expires_at),
        Some(expires_at)
    );

    std::thread::sleep(Duration::from_millis(25));
    binding.update_path_metrics(
        output.key,
        PathMetrics {
            delivery_rate_bps: metrics.delivery_rate_bps / 3,
            ..metrics
        },
        ServerPathMetricsSource::LocalSender,
    );
    assert!(
        first_output_entry(&binding)
            .local_path_metrics
            .is_some_and(|entry| entry.capacity_proof.is_none()),
        "an expired fixed proof cannot be resurrected by a generic refresh"
    );
}

#[test]
fn quic_capacity_lease_deadline_is_created_after_admission_and_failure_propagates() {
    let mux_limits = MuxLimits::default();
    let session_id = SessionId(519);
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
        tracker,
    );
    let candidate = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (candidate_commands, mut candidate_receivers) = reliable_path_command_channels(8);
    let candidate_queue = candidate_commands.clone();
    binding.attach(
        candidate.underlay,
        candidate.path_id,
        candidate_commands,
        FlowLane::Throughput,
        StreamOpenRole::Validation,
        mux_limits.max_payload_bytes,
    );
    assert!(matches!(
        try_recv_reliable_path_command(&mut candidate_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
    ));

    let (planner_generation, _) = binding.subflow_state_snapshot();
    let scheduling = binding.response_scheduling_snapshot();
    let target = binding
        .sender_path_targets(FlowLane::Throughput, 64 * 1024)
        .into_iter()
        .find(|target| target.key == candidate)
        .expect("UDP Validation target");
    let pending_before_train = candidate_queue.pending_bytes();
    let train_bytes = mux_limits.max_payload_bytes / 2;
    let sample_floor_bytes = train_bytes as u64;
    let accounting_slack_bytes = (PATH_OPEN_SCORE_BYTES as u64).min(sample_floor_bytes / 8);
    let required_proof_bytes = sample_floor_bytes - accounting_slack_bytes;
    let mut deadline_observed_admitted_train = false;
    assert!(!binding.try_start_quic_capacity_calibration_with_lease(
        &target,
        ResponseQuicCapacityCalibrationRequest {
            expected_planner_generation: planner_generation,
            expected_lane_generation: scheduling.generation,
            expected_model_generation: binding.response_model_generation(),
            target: candidate,
            target_path_instance_id: target.path_instance_id,
            target_incarnation: target.incarnation,
            target_pending_bytes: target.command_pending_bytes,
            train_bytes,
            sample_floor_bytes,
            accounting_slack_bytes,
            fresh_strict_window_bytes: required_proof_bytes,
            carrier_window_bytes: 0,
            lease: Duration::from_secs(1),
            proof_validity: Duration::from_secs(3),
        },
        |_| {
            deadline_observed_admitted_train =
                candidate_queue.pending_bytes() > pending_before_train;
            Duration::ZERO
        },
    ));
    assert!(deadline_observed_admitted_train);
    let after_failed_commit = binding.response_scheduling_snapshot();
    assert!(!after_failed_commit.quic_capacity_calibration_reserved);
    assert_eq!(
        after_failed_commit.quic_capacity_calibration_spent_bytes, train_bytes as u64,
        "an admitted train remains charged even when its lease cannot commit"
    );
    assert_eq!(
        binding
            .lane_tracker
            .response_path_scheduling_snapshot(session_id, candidate, target.path_instance_id,)
            .quic_capacity_calibration_attempts,
        1
    );
    assert!(matches!(
        try_recv_reliable_path_command(&mut candidate_receivers),
        Some(ReliablePathCommand::SendQuicCapacityProbe(_))
    ));
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
    tracker
        .state
        .lock()
        .expect("test lane tracker lock")
        .quic_capacity_calibrations
        .get_mut(&session_id)
        .expect("first reservation")
        .phase = ServerQuicCapacityCalibrationPhase::Active {
        expires_at: Instant::now() - Duration::from_millis(1),
    };
    let expired = tracker.response_scheduling_snapshot(session_id);
    assert!(!expired.quic_capacity_calibration_reserved);

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

    let state = tracker.state.lock().expect("test lane tracker lock");
    assert!(
        state
            .quic_capacity_calibration_attempts
            .keys()
            .all(|key| key.session_id != session_id)
    );
    assert_eq!(
        state.quic_capacity_calibration_bytes.get(&session_id),
        Some(&320),
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
    let state = tracker.state.lock().expect("test lane tracker lock");
    assert!(
        !state
            .quic_capacity_calibration_bytes
            .contains_key(&session_id)
    );
    assert!(
        state
            .quic_capacity_calibration_attempts
            .keys()
            .all(|key| key.session_id != session_id)
    );
}

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
    let move_generation = tracker.generation(session_id);
    tracker
        .state
        .lock()
        .expect("test lane tracker lock")
        .response_service_handoff_drains
        .get_mut(&session_id)
        .expect("reserved handoff drain")
        .expires_at = Instant::now() - Duration::from_millis(1);

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

#[test]
fn exact_clear_frontier_handoff_pins_quic_proof_through_marker_expiry() {
    let mux_limits = MuxLimits::default();
    let session_id = SessionId(511);
    let tracker = Arc::new(ServerPathLaneTracker::default());
    let service = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let candidate = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (service_commands, _service_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        service.underlay,
        service.path_id,
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
        tracker,
    );
    let (candidate_commands, mut candidate_receivers) = reliable_path_command_channels(8);
    binding.attach(
        candidate.underlay,
        candidate.path_id,
        candidate_commands,
        FlowLane::Throughput,
        StreamOpenRole::Validation,
        mux_limits.max_payload_bytes,
    );
    let _ = try_recv_reliable_path_command(&mut candidate_receivers);
    let proof = {
        let mut outputs = binding.outputs.lock().expect("test response outputs lock");
        let mut proof = None;
        for entry in &mut outputs.entries {
            if entry.key == service {
                mark_test_response_output_bulk_proven(entry, mux_limits);
            } else if entry.key == candidate {
                proof = Some(mark_test_quic_output_receipt_bulk_proven(
                    entry,
                    mux_limits,
                    511,
                    Duration::from_millis(250),
                ));
            }
        }
        proof.expect("installed QUIC receipt proof")
    };
    let frontier = 4096;
    binding
        .ack_ordering
        .lock()
        .expect("test response ACK ordering lock")
        .contiguous_frontier = frontier;
    let (planner_generation, _) = binding.subflow_state_snapshot();
    let scheduling = binding.response_scheduling_snapshot();
    assert_eq!(
        scheduling.service_family_loads,
        ResponseServiceFamilyLoads::new(2, 0)
    );
    let model_generation = binding.response_model_generation();
    let targets = binding.sender_path_targets(FlowLane::Throughput, 64 * 1024);
    let service_target = targets
        .iter()
        .find(|target| target.key == service)
        .expect("TCP Service target")
        .clone();
    let candidate_target = targets
        .iter()
        .find(|target| target.key == candidate)
        .expect("measured QUIC target")
        .clone();
    let frame = stream_data_frame_at(frontier, 64 * 1024);
    let request = ResponseServiceHandoffRequest {
        expected_planner_generation: planner_generation,
        expected_lane_generation: scheduling.generation,
        expected_model_generation: model_generation,
        handoff_frontier: frontier,
        service,
        service_path_instance_id: service_target.path_instance_id,
        service_incarnation: service_target.incarnation,
        target: candidate,
        target_path_instance_id: candidate_target.path_instance_id,
        target_incarnation: candidate_target.incarnation,
        mode: ResponseServiceHandoffMode::Diversification,
        target_command_pending_limit_bytes: u64::MAX,
        capacity_proof: Some(proof),
    };
    assert!(matches!(
        binding.try_enqueue_response_service_handoff(
            &candidate_target,
            &frame,
            FlowLane::Throughput,
            ResponseServiceHandoffRequest {
                expected_model_generation: model_generation.wrapping_sub(1),
                ..request
            },
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    assert_eq!(binding.ordered_data_owner(), Some(service));
    assert_eq!(
        binding.response_scheduling_snapshot().service_family_loads,
        ResponseServiceFamilyLoads::new(2, 0),
        "a stale handoff must not reserve or move session Service load"
    );
    assert!(binding.try_start_response_service_handoff_drain(
        &service_target,
        &candidate_target,
        FlowLane::Throughput,
        ResponseServiceHandoffDrainRequest {
            expected_planner_generation: planner_generation,
            expected_lane_generation: request.expected_lane_generation,
            expected_model_generation: model_generation,
            service,
            service_path_instance_id: service_target.path_instance_id,
            service_incarnation: service_target.incarnation,
            target: candidate,
            target_path_instance_id: candidate_target.path_instance_id,
            target_incarnation: candidate_target.incarnation,
            mode: ResponseServiceHandoffMode::Diversification,
            capacity_proof: Some(proof),
            outstanding_owner_bytes: 64 * 1024,
            lease: Duration::from_secs(1),
        },
    ));
    let drained_scheduling = binding.response_scheduling_snapshot();
    assert!(drained_scheduling.response_service_handoff_drain.is_some());
    std::thread::sleep(
        proof
            .expires_at
            .saturating_duration_since(Instant::now())
            .saturating_add(Duration::from_millis(10)),
    );
    assert!(
        binding
            .outputs
            .lock()
            .expect("test response outputs lock")
            .entries
            .iter()
            .find(|entry| entry.key == candidate)
            .is_some_and(|entry| {
                server_output_quic_capacity_proof_marker(entry) == Some(proof)
                    && server_output_fresh_quic_capacity_proof(entry).is_none()
            }),
        "the raw marker remains observable after ordinary authority expires"
    );
    let candidate_target = binding
        .sender_path_targets(FlowLane::Throughput, 64 * 1024)
        .into_iter()
        .find(|target| target.key == candidate)
        .expect("reserved QUIC target after marker expiry");
    assert!(!candidate_target.has_bulk_rate_evidence);
    binding
        .try_enqueue_response_service_handoff(
            &candidate_target,
            &frame,
            FlowLane::Throughput,
            ResponseServiceHandoffRequest {
                expected_lane_generation: drained_scheduling.generation,
                ..request
            },
        )
        .expect("exact drained frontier should commit one sticky handoff");

    assert_eq!(binding.ordered_data_owner(), Some(candidate));
    assert!(!binding.response_service_handoff_open());
    assert!(
        binding
            .response_scheduling_snapshot()
            .response_service_handoff_drain
            .is_none(),
        "the matching drain intent must be consumed with the Service move"
    );
    assert_eq!(
        binding.response_scheduling_snapshot().service_family_loads,
        ResponseServiceFamilyLoads::new(1, 1)
    );
    assert_eq!(
        binding
            .lane_tracker
            .response_service_snapshot(session_id, service)
            .active_flows,
        0,
        "the old Active attachment must not retain response Service pressure"
    );
    assert_eq!(
        binding
            .lane_tracker
            .response_service_snapshot(session_id, candidate)
            .active_flows,
        1
    );
    let moved_targets = binding.sender_path_targets(FlowLane::Throughput, 64 * 1024);
    assert_eq!(
        moved_targets
            .iter()
            .find(|target| target.key == service)
            .expect("old TCP attachment")
            .snapshot
            .active_flows,
        0
    );
    assert_eq!(
        moved_targets
            .iter()
            .find(|target| target.key == candidate)
            .expect("new QUIC Service")
            .snapshot
            .active_flows,
        1
    );
    assert!(matches!(
        try_recv_reliable_path_command(&mut candidate_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            offset,
            ..
        })) if offset == frontier
    ));
}

#[test]
fn fixed_stream_ordered_path_proof_follows_earlier_stream_data() {
    let mux_limits = MuxLimits::default();
    let path_id = PathId(3);
    let (commands, mut receivers) = reliable_path_command_channels(4);
    commands
        .try_enqueue_admitted_frame(stream_data_frame(32), FlowLane::Throughput)
        .expect("queue earlier stream data");
    let stream = ReliablePathStreamHandle {
        stream_id: StreamId(7),
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: mux_limits.max_payload_bytes,
        output: ReliablePathStreamOutput::fixed(
            UnderlayProtocol::Tcp,
            path_id,
            commands,
            mux_limits,
        ),
    };

    let proof_id = stream
        .enqueue_stream_ordered_path_proof(FlowLane::Throughput)
        .expect("queue stream-ordered path proof")
        .expect("fixed output has a carrier path");

    assert!(
        try_recv_reliable_path_priority_command(&mut receivers).is_none(),
        "stream-ordered proof must not enter the priority queue"
    );
    assert!(matches!(
        try_recv_reliable_path_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
    let proof_frame = match try_recv_reliable_path_command(&mut receivers) {
        Some(ReliablePathCommand::SendFrame(frame)) => {
            let Frame::PathProofData {
                path_id: queued_path_id,
                proof_id: queued_proof_id,
                payload,
            } = &frame
            else {
                panic!("stream-ordered proof must follow earlier product data");
            };
            assert_eq!(*queued_path_id, path_id);
            assert_eq!(*queued_proof_id, proof_id);
            assert!(!payload.is_empty());
            frame
        }
        _ => panic!("stream-ordered proof must follow earlier product data"),
    };
    let payload_len = match &proof_frame {
        Frame::PathProofData { payload, .. } => payload.len(),
        _ => unreachable!("matched path proof frame above"),
    };
    let mut tracker = PathProofTracker::default();
    tracker.record_sent_frame(&proof_frame);
    let observation = tracker
        .acknowledge(
            path_id,
            proof_id,
            u32::try_from(payload_len).expect("test proof payload length fits u32"),
        )
        .expect("consumed ordered proof is tracked for acknowledgement");
    assert_eq!(observation.proof_id, proof_id);
    assert_eq!(observation.bytes, payload_len as u64);
}

#[test]
fn fixed_priority_path_proof_preserves_attachment_liveness_ordering() {
    let mux_limits = MuxLimits::default();
    let path_id = PathId(4);
    let (commands, mut receivers) = reliable_path_command_channels(4);
    commands
        .try_enqueue_admitted_frame(stream_data_frame(32), FlowLane::Throughput)
        .expect("queue earlier stream data");
    let stream = ReliablePathStreamHandle {
        stream_id: StreamId(7),
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: mux_limits.max_payload_bytes,
        output: ReliablePathStreamOutput::fixed(
            UnderlayProtocol::Tcp,
            path_id,
            commands,
            mux_limits,
        ),
    };

    let proof_id = stream
        .enqueue_path_proof()
        .expect("queue priority path proof")
        .expect("fixed output has a carrier path");

    match try_recv_reliable_path_priority_command(&mut receivers) {
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData {
            path_id: queued_path_id,
            proof_id: queued_proof_id,
            ..
        })) => {
            assert_eq!(queued_path_id, path_id);
            assert_eq!(queued_proof_id, proof_id);
        }
        _ => panic!("attachment-liveness proof must retain priority ordering"),
    }
    assert!(matches!(
        try_recv_reliable_path_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
}

#[test]
fn switchable_stream_ordered_path_proof_keeps_no_fixed_carrier_semantics() {
    let (binding, key) = binding_for_underlay(UnderlayProtocol::Tcp);
    let stream = ReliablePathStreamHandle {
        stream_id: StreamId(7),
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: key.underlay,
        max_frame_payload_bytes: MuxLimits::default().max_payload_bytes,
        output: ReliablePathStreamOutput::Switchable(binding),
    };

    assert_eq!(
        stream
            .enqueue_stream_ordered_path_proof(FlowLane::Throughput)
            .expect("switchable output is a successful no-op"),
        None
    );
}

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
            reliable_relay_buffer_len(MuxLimits::default()),
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
fn independent_source_staging_requires_live_mixed_owner_underlays() {
    let (active_commands, active_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(42),
        UnderlayProtocol::Tcp,
        PathId(0),
        active_commands,
        FlowLane::Throughput,
    );
    let mut receivers = vec![active_receivers];
    assert!(!binding.has_live_mixed_owner_underlays());
    assert!(
        !binding
            .relay_read_snapshot(FlowLane::Throughput, PATH_OPEN_SCORE_BYTES)
            .independent_source_staging
    );
    assert_eq!(
        binding.owner_underlay_history.load(Ordering::Acquire),
        RESPONSE_OWNER_TCP_SEEN
    );

    for (path_id, underlay, role, expected, expected_history) in [
        (
            1,
            UnderlayProtocol::Tcp,
            StreamOpenRole::Validation,
            false,
            RESPONSE_OWNER_TCP_SEEN,
        ),
        (
            2,
            UnderlayProtocol::Udp,
            StreamOpenRole::Repair,
            false,
            RESPONSE_OWNER_TCP_SEEN,
        ),
        (
            3,
            UnderlayProtocol::Udp,
            StreamOpenRole::Validation,
            true,
            RESPONSE_OWNER_MIXED_SEEN,
        ),
    ] {
        let (commands, output_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                underlay,
                PathId(path_id),
                commands,
                FlowLane::Throughput,
                role,
                reliable_relay_buffer_len(MuxLimits::default()),
            ),
            ResponseStreamAttachOutcome::Attached,
        );
        assert_eq!(
            binding.has_live_mixed_owner_underlays(),
            expected,
            "only a live owner-capable cross-underlay output enables independent raw staging",
        );
        assert_eq!(
            binding
                .relay_read_snapshot(FlowLane::Throughput, PATH_OPEN_SCORE_BYTES)
                .independent_source_staging,
            expected,
            "the composite relay snapshot must use the same live-family policy",
        );
        assert_eq!(
            binding.owner_underlay_history.load(Ordering::Acquire),
            expected_history,
            "Repair-only attachments must retain the single-family fast path",
        );
        receivers.push(output_receivers);
    }
}

#[test]
fn response_relay_read_snapshot_keeps_source_evidence_on_the_ordered_service() {
    let session_id = SessionId(42);
    let lane_tracker = Arc::new(ServerPathLaneTracker::default());
    let service = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let (service_commands, _service_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        service.underlay,
        service.path_id,
        service_commands,
        FlowLane::Throughput,
        MuxLimits::default(),
        lane_tracker.clone(),
    );
    let alternate = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (alternate_commands, _alternate_receivers) = reliable_path_command_channels(8);
    let alternate_commands_for_detach = alternate_commands.clone();
    assert_eq!(
        binding.attach(
            alternate.underlay,
            alternate.path_id,
            alternate_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            reliable_relay_buffer_len(MuxLimits::default()),
        ),
        ResponseStreamAttachOutcome::Attached,
    );
    let (latency_commands, _latency_receivers) = reliable_path_command_channels(8);
    let _alternate_latency_flow = ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        alternate.underlay,
        alternate.path_id,
        latency_commands,
        FlowLane::Latency,
        MuxLimits::default(),
        lane_tracker,
    );
    {
        let mut outputs = binding.outputs.lock().expect("test response outputs lock");
        let service_entry = outputs
            .entries
            .iter_mut()
            .find(|entry| entry.key == service)
            .expect("ordered Service output");
        service_entry.delivery_rate_bps = Some(1_000_000.0);
        service_entry.srtt_ms = Some(500.0);
        service_entry.delivery_samples = 1;
    }
    binding.update_path_metrics(
        alternate,
        PathMetrics {
            path_id: alternate.path_id,
            underlay: alternate.underlay,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: metric_epoch_now(),
            metric_age_us: 0,
            min_rtt_us: 5_000,
            srtt_us: 5_000,
            rttvar_us: 500,
            jitter_us: 500,
            delivery_rate_bps: 1_000_000_000,
            pacing_rate_bps: 1_000_000_000,
            loss_ppm: 0,
            ecn_ppm: 0,
            loss_observed: false,
            ecn_observed: false,
            bytes_in_flight: 0,
            queue_bytes: 0,
            inflight_limit_bytes: PATH_OPEN_SCORE_BYTES as u64,
            inflight_hi_bytes: PATH_OPEN_SCORE_BYTES as u64,
            confidence_ppm: 1_000_000,
            app_limited: false,
            has_ack_derived_data_sample: true,
            data_sample_count: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
            data_sample_bytes: RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES,
        },
        ServerPathMetricsSource::LocalSender,
    );

    let before = binding.relay_read_snapshot(FlowLane::Throughput, PATH_OPEN_SCORE_BYTES);
    assert!(before.send_path.is_some_and(|path| {
        path.id == alternate.path_id && path.underlay == alternate.underlay
    }));
    assert_eq!(
        before
            .send_path
            .expect("faster alternate send path")
            .active_latency_sensitive_flows,
        1
    );
    let source = before
        .source_service
        .expect("live ordered Service snapshot");
    assert_eq!(source.key, service);
    assert!(!source.has_bulk_rate_evidence);
    assert!(before.independent_source_staging);

    {
        let mut outputs = binding.outputs.lock().expect("test response outputs lock");
        let service_entry = outputs
            .entries
            .iter_mut()
            .find(|entry| entry.key == service)
            .expect("ordered Service output");
        service_entry.product_progress_rate_bps = Some(1_000_000.0);
        service_entry.owner_data_acked_bytes =
            reliable_subflow_startup_sample_limit_bytes(binding.mux_limits());
    }
    let after = binding.relay_read_snapshot(FlowLane::Throughput, PATH_OPEN_SCORE_BYTES);
    let source = after.source_service.expect("live ordered Service snapshot");
    assert!(source.has_bulk_rate_evidence);
    assert_eq!(source.active_latency_sensitive_flows, 0);
    assert_eq!(
        reliable_relay_source_staging_owner_tail_headroom(
            ReliableSourceStagingContext {
                independent: after.independent_source_staging,
                service: Some(ReliableSourceServiceStagingContext {
                    allows_product_envelope: true,
                    has_latency_pressure: source.active_latency_sensitive_flows > 0,
                    has_feed_evidence: source.has_service_feed_evidence,
                }),
            },
            FlowLane::Throughput,
            0,
            0,
            reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default()),
            MuxLimits::default(),
        ),
        bulk_service_feed_reservoir_payload_bytes(
            reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default()),
            MuxLimits::default(),
        ),
        "alternate-path latency pressure must not narrow exact-Service source staging"
    );
    let service_target = binding
        .sender_path_targets(FlowLane::Throughput, PATH_OPEN_SCORE_BYTES)
        .into_iter()
        .find(|target| target.key == service)
        .expect("ordered Service sender target");
    assert_eq!(
        source.has_bulk_rate_evidence, service_target.has_bulk_rate_evidence,
        "source staging and sender admission must consume the same Service proof"
    );

    binding.detach(alternate, &alternate_commands_for_detach);
    assert!(
        !binding
            .relay_read_snapshot(FlowLane::Throughput, PATH_OPEN_SCORE_BYTES)
            .independent_source_staging,
        "mixed-family source staging must end when the alternate family detaches"
    );
}

#[test]
fn udp_product_progress_matures_only_current_service_feed() {
    let service = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    let (service_commands, _service_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(42),
        service.underlay,
        service.path_id,
        service_commands,
        FlowLane::Throughput,
    );
    {
        let mut outputs = binding.outputs.lock().expect("test response outputs lock");
        let service_entry = outputs
            .entries
            .iter_mut()
            .find(|entry| entry.key == service)
            .expect("ordered Service output");
        service_entry.product_progress_rate_bps = Some(100_000_000.0);
        service_entry.delivery_rate_bps = Some(100_000_000.0);
        service_entry.srtt_ms = Some(20.0);
        service_entry.delivery_samples = u32::MAX;
        service_entry.owner_data_acked_bytes = u64::MAX;
    }

    let read = binding.relay_read_snapshot(FlowLane::Throughput, PATH_OPEN_SCORE_BYTES);
    let source = read.source_service.expect("live ordered Service snapshot");
    assert_eq!(source.key, service);
    let send_path = read.send_path.expect("single live Service send snapshot");
    assert_eq!(send_path.confidence, 1.0);
    assert!(send_path.product_progress_rate_bps.is_some());
    assert!(
        source.has_service_feed_evidence,
        "substantial uniquely owned product ACKs may release current-Service staging"
    );
    assert!(
        !source.has_bulk_rate_evidence,
        "product ACK timing must not mint optional QUIC placement authority"
    );
}

#[test]
fn udp_app_limited_carrier_progress_feeds_only_the_current_service() {
    let mux_limits = MuxLimits::default();
    let service = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    let (service_commands, _service_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(42),
        service.underlay,
        service.path_id,
        service_commands,
        FlowLane::Throughput,
    );
    let mut entry = first_output_entry(&binding);
    mark_test_quic_output_carrier_bulk_proven(&mut entry, mux_limits);
    let metrics = PathMetrics {
        app_limited: true,
        ..entry
            .local_path_metrics
            .expect("test QUIC sender metrics")
            .metrics
    };
    binding.update_path_metrics(service, metrics, ServerPathMetricsSource::LocalSender);

    let read = binding.relay_read_snapshot(FlowLane::Throughput, PATH_OPEN_SCORE_BYTES);
    let source = read.source_service.expect("current response Service");
    assert!(source.has_service_feed_evidence);
    assert!(
        !source.has_bulk_rate_evidence,
        "an app-limited sample must not authorize optional placement"
    );

    let target = binding
        .sender_path_targets(FlowLane::Throughput, PATH_OPEN_SCORE_BYTES)
        .into_iter()
        .find(|target| target.key == service)
        .expect("current Service sender target");
    assert!(target.is_active);
    assert!(target.has_service_feed_evidence);
    assert!(!target.has_bulk_rate_evidence);

    let alternate = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (alternate_commands, _alternate_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            alternate.underlay,
            alternate.path_id,
            alternate_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            reliable_relay_buffer_len(mux_limits),
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let mut alternate_entry = binding
        .outputs
        .lock()
        .expect("test response outputs lock")
        .entries
        .iter()
        .find(|entry| entry.key == alternate)
        .expect("Validation output")
        .clone();
    mark_test_quic_output_carrier_bulk_proven(&mut alternate_entry, mux_limits);
    let alternate_metrics = PathMetrics {
        app_limited: true,
        ..alternate_entry
            .local_path_metrics
            .expect("alternate QUIC sender metrics")
            .metrics
    };
    binding.update_path_metrics(
        alternate,
        alternate_metrics,
        ServerPathMetricsSource::LocalSender,
    );
    let alternate_target = binding
        .sender_path_targets(FlowLane::Throughput, PATH_OPEN_SCORE_BYTES)
        .into_iter()
        .find(|target| target.key == alternate)
        .expect("Validation sender target");
    assert!(!alternate_target.is_active);
    assert!(!alternate_target.has_service_feed_evidence);
    assert!(!alternate_target.has_bulk_rate_evidence);
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
            reliable_relay_buffer_len(MuxLimits::default()),
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
            reliable_relay_buffer_len(MuxLimits::default()),
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
            reliable_relay_buffer_len(MuxLimits::default()),
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
        reliable_stream_recv_progress_interval(Some(request_active_snapshot), FlowLane::Throughput,)
            < reliable_stream_recv_progress_interval(
                Some(response_service_snapshot),
                FlowLane::Throughput,
            ),
        "receive-progress cadence must follow the request Active PTO rather than the response Service PTO"
    );
    assert_eq!(
        binding.owner_underlay_history.load(Ordering::Acquire),
        RESPONSE_OWNER_MIXED_SEEN
    );
}

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
            reliable_relay_buffer_len(MuxLimits::default()),
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let targets = binding.sender_path_targets(FlowLane::Throughput, 4096);
    assert!(
        targets
            .iter()
            .find(|target| target.key == active)
            .is_some_and(|target| target.is_active && target.is_request_active),
        "the initial active output remains the scheduler-active target"
    );
    assert!(
        targets
            .iter()
            .find(|target| target.key == validation)
            .is_some_and(|target| !target.is_active),
        "validation output must not be active before lead migration"
    );

    binding.set_ordered_data_owner(validation);

    let targets = binding.sender_path_targets(FlowLane::Throughput, 4096);
    assert!(
        targets
            .iter()
            .find(|target| target.key == validation)
            .is_some_and(|target| target.is_active && !target.is_request_active),
        "scheduler-active target must follow ordered_data_owner after migration"
    );
    assert!(
        targets
            .iter()
            .find(|target| target.key == active)
            .is_some_and(|target| !target.is_active && target.is_request_active),
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
            reliable_relay_buffer_len(MuxLimits::default()),
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
            reliable_relay_buffer_len(MuxLimits::default()),
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
            reliable_relay_buffer_len(MuxLimits::default()),
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
            reliable_relay_buffer_len(MuxLimits::default()),
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
            reliable_relay_buffer_len(MuxLimits::default()),
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
            min_rtt_us: 20_000,
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
            .find(|target| target.key == survivor)
            .is_some_and(|target| !target.is_active && target.has_sender_evidence),
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
            reliable_relay_buffer_len(MuxLimits::default()),
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
            min_rtt_us: 20_000,
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
            .find(|target| target.key == survivor)
            .is_some_and(|target| !target.is_active && target.has_sender_evidence),
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
            reliable_relay_buffer_len(MuxLimits::default()),
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
            min_rtt_us: 20_000,
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
            reliable_relay_buffer_len(MuxLimits::default()),
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
fn udp_stream_ack_releases_product_flight_without_seeding_carrier_rate() {
    let (binding, key) = binding_for_underlay(UnderlayProtocol::Udp);
    let frame = stream_data_frame(MIN_RATE_SAMPLE_BYTES as usize);

    binding.record_owner_flight(key, &frame);
    std::thread::sleep(Duration::from_millis(1));
    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: reliable_stream_frame_payload_bytes(&frame) as u64,
    }]);

    let entry = first_output_entry(&binding);
    assert_eq!(entry.bytes_in_flight, 0);
    assert_eq!(entry.delivery_samples, 1);
    assert_eq!(entry.owner_data_acked_bytes, MIN_RATE_SAMPLE_BYTES);
    assert!(entry.product_progress_rate_bps.is_some());
    assert!(entry.delivery_rate_bps.is_none());
    assert!(entry.srtt_ms.is_none());
}

#[test]
fn tcp_first_stream_ack_is_progress_but_not_a_capacity_clock() {
    let (binding, key) = binding_for_underlay(UnderlayProtocol::Tcp);
    let frame = stream_data_frame(MIN_RATE_SAMPLE_BYTES as usize);

    binding.record_owner_flight(key, &frame);
    std::thread::sleep(Duration::from_millis(1));
    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: reliable_stream_frame_payload_bytes(&frame) as u64,
    }]);

    let entry = first_output_entry(&binding);
    assert_eq!(entry.bytes_in_flight, 0);
    assert_eq!(entry.delivery_samples, 1);
    assert_eq!(entry.owner_data_acked_bytes, MIN_RATE_SAMPLE_BYTES);
    assert!(entry.product_progress_rate_bps.is_none());
    assert!(entry.delivery_rate_bps.is_none());
    assert!(entry.tcp_ack_clock_rate_bps.is_none());
    assert!(entry.srtt_ms.is_none());
}

#[test]
fn tcp_ordinary_ack_clock_excludes_assignment_queue_residence() {
    let (binding, key) = binding_for_underlay(UnderlayProtocol::Tcp);
    let window_bytes = PATH_OPEN_SCORE_BYTES;
    binding.record_owner_flight(key, &stream_data_frame_at(0, window_bytes));
    binding.record_owner_flight(
        key,
        &stream_data_frame_at(window_bytes as u64, window_bytes),
    );
    let clock = Instant::now();
    let first_ack_at = clock + Duration::from_secs(1);
    binding.release_normalized_acked_ranges_at(
        &[OffsetRange {
            start: 0,
            end: window_bytes as u64,
        }],
        first_ack_at,
    );
    let provisional = first_output_entry(&binding);
    assert!(provisional.tcp_ack_clock_rate_bps.is_none());
    assert!(provisional.product_progress_rate_bps.is_none());
    assert!(provisional.delivery_rate_bps.is_none());
    assert!(provisional.srtt_ms.is_none());

    let second_ack_at = first_ack_at + Duration::from_millis(100);
    binding.release_normalized_acked_ranges_at(
        &[OffsetRange {
            start: window_bytes as u64,
            end: (2 * window_bytes) as u64,
        }],
        second_ack_at,
    );
    let expected_rate = window_bytes as f64 * 8.0 / 0.1;
    let measured = first_output_entry(&binding);
    assert_test_rate_close(measured.tcp_ack_clock_rate_bps, expected_rate);
    assert_test_rate_close(measured.product_progress_rate_bps, expected_rate);
    assert_test_rate_close(measured.delivery_rate_bps, expected_rate);

    let late_offset = (2 * window_bytes) as u64;
    binding.record_owner_flight(key, &stream_data_frame_at(late_offset, window_bytes));
    {
        let mut flights = binding
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        flights
            .get_mut(&late_offset)
            .expect("late flight")
            .iter_mut()
            .for_each(|flight| flight.sent_at = second_ack_at + Duration::from_millis(1));
    }
    binding.release_normalized_acked_ranges_at(
        &[OffsetRange {
            start: late_offset,
            end: late_offset + window_bytes as u64,
        }],
        second_ack_at + Duration::from_millis(100),
    );
    let after_late_assignment = first_output_entry(&binding);
    assert_test_rate_close(after_late_assignment.tcp_ack_clock_rate_bps, expected_rate);
    assert_test_rate_close(after_late_assignment.delivery_rate_bps, expected_rate);

    let late_ack_at = second_ack_at + Duration::from_millis(100);
    let recovery_offset = late_offset + window_bytes as u64;
    binding.record_owner_flight(key, &stream_data_frame_at(recovery_offset, window_bytes));
    {
        let mut flights = binding
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        flights
            .get_mut(&recovery_offset)
            .expect("recovery flight")
            .iter_mut()
            .for_each(|flight| flight.sent_at = late_ack_at - Duration::from_millis(1));
    }
    binding.release_normalized_acked_ranges_at(
        &[OffsetRange {
            start: recovery_offset,
            end: recovery_offset + window_bytes as u64,
        }],
        late_ack_at + Duration::from_millis(50),
    );
    let recovered_rate = (3 * window_bytes) as f64 * 8.0 / 0.25;
    let recovered = first_output_entry(&binding);
    assert_test_rate_close(recovered.tcp_ack_clock_rate_bps, recovered_rate);
    assert_test_rate_close(recovered.delivery_rate_bps, recovered_rate);
}

#[test]
fn tcp_ack_clock_can_reduce_rate_while_carrier_snapshot_is_app_limited() {
    let (binding, key) = binding_for_underlay(UnderlayProtocol::Tcp);
    let window_bytes = PATH_OPEN_SCORE_BYTES;
    binding.update_path_metrics(
        key,
        PathMetrics {
            path_id: key.path_id,
            underlay: key.underlay,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: metric_epoch_now(),
            metric_age_us: 0,
            min_rtt_us: 20_000,
            srtt_us: 20_000,
            rttvar_us: 1_000,
            jitter_us: 1_000,
            delivery_rate_bps: 100_000_000,
            pacing_rate_bps: 100_000_000,
            loss_ppm: 0,
            ecn_ppm: 0,
            loss_observed: false,
            ecn_observed: false,
            bytes_in_flight: 0,
            queue_bytes: 0,
            inflight_limit_bytes: window_bytes as u64,
            inflight_hi_bytes: window_bytes as u64,
            confidence_ppm: 1_000_000,
            app_limited: true,
            has_ack_derived_data_sample: false,
            data_sample_count: 0,
            data_sample_bytes: 0,
        },
        ServerPathMetricsSource::LocalSender,
    );
    {
        let mut outputs = binding.outputs.lock().expect("test response outputs lock");
        let entry = outputs.entries.first_mut().expect("TCP output");
        entry.tcp_ack_clock_rate_bps = Some(100_000_000.0);
        entry.product_progress_rate_bps = Some(100_000_000.0);
        entry.delivery_rate_bps = Some(100_000_000.0);
    }
    binding.record_owner_flight(key, &stream_data_frame_at(0, window_bytes));
    binding.record_owner_flight(
        key,
        &stream_data_frame_at(window_bytes as u64, window_bytes),
    );
    let first_ack_at = Instant::now() + Duration::from_millis(100);
    binding.release_normalized_acked_ranges_at(
        &[OffsetRange {
            start: 0,
            end: window_bytes as u64,
        }],
        first_ack_at,
    );
    binding.release_normalized_acked_ranges_at(
        &[OffsetRange {
            start: window_bytes as u64,
            end: (2 * window_bytes) as u64,
        }],
        first_ack_at + Duration::from_millis(100),
    );

    let entry = first_output_entry(&binding);
    assert!(
        entry
            .tcp_ack_clock_rate_bps
            .is_some_and(|rate| rate < 100_000_000.0),
        "per-flow TCP ACK evidence must not inherit QUIC's app-limited max filter"
    );
}

#[test]
fn tcp_ack_clock_is_independent_from_global_contiguous_frontier() {
    let (binding, key) = binding_for_underlay(UnderlayProtocol::Tcp);
    let window_bytes = PATH_OPEN_SCORE_BYTES;
    binding.record_owner_flight(key, &stream_data_frame_at(0, window_bytes));
    binding.record_owner_flight(
        key,
        &stream_data_frame_at(window_bytes as u64, window_bytes),
    );
    let first_ack_at = Instant::now() + Duration::from_secs(1);
    binding.release_normalized_acked_ranges_at(
        &[OffsetRange {
            start: window_bytes as u64,
            end: (2 * window_bytes) as u64,
        }],
        first_ack_at,
    );
    let hole = first_output_entry(&binding);
    assert!(hole.tcp_ack_clock_rate_bps.is_none());

    binding.release_normalized_acked_ranges_at(
        &[OffsetRange {
            start: 0,
            end: window_bytes as u64,
        }],
        first_ack_at + Duration::from_millis(100),
    );
    let expected_rate = window_bytes as f64 * 8.0 / 0.1;
    let measured = first_output_entry(&binding);
    assert_test_rate_close(measured.tcp_ack_clock_rate_bps, expected_rate);
}

#[test]
fn tcp_response_single_stage_ack_clock_sample_preserves_startup_rate() {
    let (binding, key) = binding_for_underlay(UnderlayProtocol::Tcp);
    let window_bytes = PATH_OPEN_SCORE_BYTES;
    let identity = {
        let mut outputs = binding.outputs.lock().expect("test response outputs lock");
        let entry = outputs.entries.first_mut().expect("TCP output");
        entry.product_progress_rate_bps = Some(1.0);
        entry.delivery_rate_bps = Some(1.0);
        let identity = (entry.key, entry.incarnation);
        let mut calibration = ResponseAckClockCalibrationState::new(
            (2 * window_bytes) as u64,
            (2 * window_bytes) as u64,
        );
        calibration.spent_bytes = (2 * window_bytes) as u64;
        outputs.ack_clock_calibrations.insert(identity, calibration);
        outputs.active_ack_clock_calibration = Some(identity);
        identity
    };
    binding.record_owner_flight(key, &stream_data_frame_at(0, window_bytes));
    binding.record_owner_flight(
        key,
        &stream_data_frame_at(window_bytes as u64, window_bytes),
    );
    std::thread::sleep(Duration::from_millis(2));
    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: window_bytes as u64,
    }]);
    {
        let outputs = binding.outputs.lock().expect("test response outputs lock");
        assert!(
            !outputs
                .ack_clock_calibrations
                .get(&identity)
                .expect("calibration state")
                .proven,
            "the first send-to-ACK window remains provisional"
        );
        assert_eq!(outputs.active_ack_clock_calibration, Some(identity));
    }

    std::thread::sleep(Duration::from_millis(2));
    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: window_bytes as u64,
        end: (2 * window_bytes) as u64,
    }]);
    let outputs = binding.outputs.lock().expect("test response outputs lock");
    let entry = outputs.entries.first().expect("TCP output");
    assert!(
        outputs
            .ack_clock_calibrations
            .get(&identity)
            .expect("calibration state")
            .proven,
        "the later window was already in flight at the previous ACK"
    );
    assert_eq!(entry.delivery_rate_bps, Some(1.0));
    assert_eq!(entry.product_progress_rate_bps, Some(1.0));
    assert_eq!(
        outputs
            .ack_clock_calibrations
            .get(&identity)
            .expect("calibration state")
            .calibrated_rate_bps,
        None,
        "one compressed stage sample cannot replace the startup rate"
    );
    assert_eq!(outputs.active_ack_clock_calibration, None);
    assert!(entry.tcp_product_rate_evidence.is_none());
    assert!(entry.tcp_ack_clock_rate_bps.is_none());
    assert!(entry.tcp_capacity_prior.is_none());
    drop(outputs);

    let next_offset = (2 * window_bytes) as u64;
    binding.record_owner_flight(
        key,
        &stream_data_frame_at(next_offset, MIN_RATE_SAMPLE_BYTES as usize),
    );
    binding.record_owner_flight(
        key,
        &stream_data_frame_at(
            next_offset + MIN_RATE_SAMPLE_BYTES,
            MIN_RATE_SAMPLE_BYTES as usize,
        ),
    );
    let ordinary_ack_at = Instant::now() + RESPONSE_ACK_CLOCK_GOODPUT_MIN_ELAPSED;
    binding.release_normalized_acked_ranges_at(
        &[OffsetRange {
            start: next_offset,
            end: next_offset + MIN_RATE_SAMPLE_BYTES,
        }],
        ordinary_ack_at,
    );
    binding.release_normalized_acked_ranges_at(
        &[OffsetRange {
            start: next_offset + MIN_RATE_SAMPLE_BYTES,
            end: next_offset + 2 * MIN_RATE_SAMPLE_BYTES,
        }],
        ordinary_ack_at + RESPONSE_ACK_CLOCK_GOODPUT_MIN_ELAPSED,
    );
    let entry = first_output_entry(&binding);
    assert!(
        entry.delivery_rate_bps.is_some_and(|rate| rate > 1.0),
        "a terminal calibration without a robust rate must not freeze later ordinary TCP evidence"
    );
}

#[test]
fn tcp_response_robust_calibration_yields_to_mature_ordinary_rate_without_fake_rtt() {
    let (binding, key) = binding_for_underlay(UnderlayProtocol::Tcp);
    let identity = {
        let mut outputs = binding.outputs.lock().expect("test response outputs lock");
        let entry = outputs.entries.first_mut().expect("TCP output");
        entry.product_progress_rate_bps = Some(7_000_000_000.0);
        entry.delivery_rate_bps = Some(7_000_000_000.0);
        entry.srtt_ms = Some(10.0);
        entry.delivery_samples = u32::MAX;
        let identity = (entry.key, entry.incarnation);
        let initial = PATH_OPEN_SCORE_BYTES as u64;
        let mut calibration = ResponseAckClockCalibrationState::new(initial, 4 * initial);
        for sample_bps in [90_000_000.0, 7_000_000_000.0, 110_000_000.0] {
            calibration.spent_bytes = calibration.credit_limit_bytes;
            let stage_authorized_at = calibration.stage_authorized_at;
            let sample =
                test_ack_clock_rate_sample(calibration.stage_rate_coverage_floor_bytes, sample_bps);
            let _ = calibration.record_ack_clock_sample(
                sample,
                stage_authorized_at + Duration::from_millis(1),
                stage_authorized_at + Duration::from_millis(10),
            );
        }
        assert_test_rate_close(calibration.calibrated_rate_bps, 110_000_000.0);
        outputs.ack_clock_calibrations.insert(identity, calibration);
        outputs.active_ack_clock_calibration = Some(identity);
        identity
    };
    let sample_bytes = 4096;
    binding.record_owner_flight(key, &stream_data_frame_at(0, sample_bytes));
    std::thread::sleep(Duration::from_millis(1));
    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: sample_bytes as u64,
    }]);

    let outputs = binding.outputs.lock().expect("test response outputs lock");
    let entry = outputs
        .entries
        .iter()
        .find(|entry| (entry.key, entry.incarnation) == identity)
        .expect("TCP output");
    assert_test_rate_close(entry.product_progress_rate_bps, 110_000_000.0);
    assert_test_rate_close(entry.delivery_rate_bps, 110_000_000.0);
    assert!(entry.tcp_ack_clock_rate_bps.is_none());
    assert_eq!(entry.srtt_ms, Some(10.0));
    assert!(entry.tcp_product_rate_evidence.is_none());
    let prior = entry.tcp_capacity_prior.expect("robust capacity prior");
    assert_test_rate_close(Some(prior.rate_bps), 110_000_000.0);
    assert_eq!(prior.ordinary_windows, 0);
    assert_eq!(outputs.active_ack_clock_calibration, None);
    drop(outputs);

    let prior_entry = first_output_entry(&binding);
    let prior_snapshot = server_bulk_output_snapshot(
        &prior_entry,
        binding.session_id,
        FlowLane::Throughput,
        binding.lane_tracker.as_ref(),
        binding.mux_limits,
        Instant::now(),
    );
    assert_test_rate_close(Some(prior_snapshot.delivery_rate_bps), 110_000_000.0);
    assert_eq!(
        prior_snapshot.rate_scope,
        crate::scheduler::PathRateScope::PathCapacity
    );

    let fragment_bytes = (PATH_OPEN_SCORE_BYTES / (RELIABLE_INITIAL_WINDOW_PACKETS + 1)).max(1);
    let fragmented_ack_start = Instant::now() + Duration::from_millis(1);
    for fragment_index in 0..RELIABLE_INITIAL_WINDOW_PACKETS as u64 {
        let offset = sample_bytes as u64 + fragment_index * fragment_bytes as u64;
        binding.record_owner_flight(key, &stream_data_frame_at(offset, fragment_bytes));
        binding.release_normalized_acked_ranges_at(
            &[OffsetRange {
                start: offset,
                end: offset + fragment_bytes as u64,
            }],
            fragmented_ack_start + Duration::from_millis(fragment_index),
        );
    }
    let entry = first_output_entry(&binding);
    assert_eq!(
        entry
            .tcp_capacity_prior
            .expect("fragmented ACKs retain capacity prior")
            .ordinary_windows,
        0,
        "callbacks smaller than one exact ACK window are not evidence samples"
    );

    let ordinary_sample_bytes = PATH_OPEN_SCORE_BYTES;
    let ordinary_ack_start = Instant::now() + RESPONSE_ACK_CLOCK_GOODPUT_MIN_ELAPSED;
    for sample_index in 0..RELIABLE_INITIAL_WINDOW_PACKETS as u64 {
        let offset = sample_bytes as u64
            + RELIABLE_INITIAL_WINDOW_PACKETS as u64 * fragment_bytes as u64
            + sample_index * ordinary_sample_bytes as u64;
        binding.record_owner_flight(key, &stream_data_frame_at(offset, ordinary_sample_bytes));
        binding.release_normalized_acked_ranges_at(
            &[OffsetRange {
                start: offset,
                end: offset + ordinary_sample_bytes as u64,
            }],
            ordinary_ack_start
                + RESPONSE_ACK_CLOCK_GOODPUT_MIN_ELAPSED.mul_f64(sample_index as f64),
        );
        if sample_index + 1 < RELIABLE_INITIAL_WINDOW_PACKETS as u64 {
            let entry = first_output_entry(&binding);
            assert_test_rate_close(entry.delivery_rate_bps, 110_000_000.0);
        }
    }
    let outputs = binding.outputs.lock().expect("test response outputs lock");
    let entry = outputs
        .entries
        .iter()
        .find(|entry| (entry.key, entry.incarnation) == identity)
        .expect("TCP output");
    assert!(entry.tcp_capacity_prior.is_none());
    assert!(
        entry
            .delivery_rate_bps
            .is_some_and(|rate_bps| rate_bps < 110_000_000.0),
        "mature ordinary exact-ACK evidence must replace the bounded capacity prior"
    );
    assert_eq!(entry.product_progress_rate_bps, entry.delivery_rate_bps);
    assert_eq!(entry.tcp_ack_clock_rate_bps, entry.delivery_rate_bps);
    assert_eq!(
        entry.srtt_ms,
        Some(10.0),
        "scheduler assignment time is not a TCP dispatch or RTT timestamp"
    );
    let ordinary_snapshot = server_bulk_output_snapshot(
        entry,
        binding.session_id,
        FlowLane::Throughput,
        binding.lane_tracker.as_ref(),
        binding.mux_limits,
        Instant::now(),
    );
    assert_eq!(
        ordinary_snapshot.rate_scope,
        crate::scheduler::PathRateScope::PerFlowGoodput
    );
}

#[test]
fn tcp_response_active_calibration_remainder_honors_state_boundaries() {
    let (binding, _key) = binding_for_underlay(UnderlayProtocol::Tcp);
    let stage_limit = (2 * 1024 * 1024) as u64;
    let residual = 4032_u64;
    let identity = {
        let mut outputs = binding.outputs.lock().expect("test response outputs lock");
        let entry = outputs.entries.first().expect("TCP output");
        let identity = (entry.key, entry.incarnation);
        let mut calibration = ResponseAckClockCalibrationState::new(stage_limit, 4 * stage_limit);
        calibration.spent_bytes = stage_limit - residual;
        outputs.ack_clock_calibrations.insert(identity, calibration);
        outputs.active_ack_clock_calibration = Some(identity);
        identity
    };

    assert_eq!(
        binding.active_tcp_ack_clock_calibration_remaining_bytes(),
        Some(residual as usize),
    );

    {
        let mut outputs = binding.outputs.lock().expect("test response outputs lock");
        outputs
            .ack_clock_calibrations
            .get_mut(&identity)
            .expect("calibration state")
            .spent_bytes = stage_limit;
    }
    assert_eq!(
        binding.active_tcp_ack_clock_calibration_remaining_bytes(),
        None,
        "an exhausted stage returns to Service while it awaits ACK evidence",
    );

    {
        let mut outputs = binding.outputs.lock().expect("test response outputs lock");
        let calibration = outputs
            .ack_clock_calibrations
            .get_mut(&identity)
            .expect("calibration state");
        calibration.spent_bytes = stage_limit - 1;
    }
    assert_eq!(
        binding.active_tcp_ack_clock_calibration_remaining_bytes(),
        Some(1),
        "a one-byte residual must not be expanded to a minimum quantum",
    );

    {
        let mut outputs = binding.outputs.lock().expect("test response outputs lock");
        outputs
            .ack_clock_calibrations
            .get_mut(&identity)
            .expect("calibration state")
            .proven = true;
    }
    assert_eq!(
        binding.active_tcp_ack_clock_calibration_remaining_bytes(),
        None
    );
    {
        let mut outputs = binding.outputs.lock().expect("test response outputs lock");
        let calibration = outputs
            .ack_clock_calibrations
            .get_mut(&identity)
            .expect("calibration state");
        calibration.proven = false;
        calibration.retired = true;
    }
    assert_eq!(
        binding.active_tcp_ack_clock_calibration_remaining_bytes(),
        None
    );
    {
        let mut outputs = binding.outputs.lock().expect("test response outputs lock");
        outputs.active_ack_clock_calibration = Some((
            CarrierPathKey {
                underlay: UnderlayProtocol::Tcp,
                path_id: PathId(99),
            },
            99,
        ));
    }
    assert_eq!(
        binding.active_tcp_ack_clock_calibration_remaining_bytes(),
        None
    );

    let (udp_binding, _udp_key) = binding_for_underlay(UnderlayProtocol::Udp);
    {
        let mut outputs = udp_binding
            .outputs
            .lock()
            .expect("test response outputs lock");
        let entry = outputs.entries.first().expect("UDP output");
        let identity = (entry.key, entry.incarnation);
        outputs.ack_clock_calibrations.insert(
            identity,
            ResponseAckClockCalibrationState::new(stage_limit, 4 * stage_limit),
        );
        outputs.active_ack_clock_calibration = Some(identity);
    }
    assert_eq!(
        udp_binding.active_tcp_ack_clock_calibration_remaining_bytes(),
        None,
        "QUIC/UDP product frames stay under the carrier-local controller",
    );
}

#[test]
fn tcp_response_mixed_window_consumes_fresh_capacity_without_publishing_rate() {
    let (binding, key) = binding_for_underlay(UnderlayProtocol::Tcp);
    let window_bytes = PATH_OPEN_SCORE_BYTES;
    let identity = {
        let mut outputs = binding.outputs.lock().expect("test response outputs lock");
        let entry = outputs.entries.first().expect("TCP output");
        let identity = (entry.key, entry.incarnation);
        let mut calibration = ResponseAckClockCalibrationState::new(
            (2 * window_bytes) as u64,
            (2 * window_bytes) as u64,
        );
        calibration.spent_bytes = (2 * window_bytes) as u64;
        outputs.ack_clock_calibrations.insert(identity, calibration);
        outputs.active_ack_clock_calibration = Some(identity);
        identity
    };
    binding.record_owner_flight(key, &stream_data_frame_at(0, window_bytes));
    binding.record_owner_flight(key, &stream_data_frame_at(window_bytes as u64, 1));
    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: window_bytes as u64,
    }]);

    binding.record_owner_flight(
        key,
        &stream_data_frame_at(window_bytes as u64 + 1, window_bytes.saturating_sub(1)),
    );
    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: window_bytes as u64,
        end: (2 * window_bytes) as u64,
    }]);

    let outputs = binding.outputs.lock().expect("test response outputs lock");
    let calibration = outputs
        .ack_clock_calibrations
        .get(&identity)
        .expect("calibration state");
    assert_eq!(calibration.calibrated_rate_bps, None);
    assert!(
        calibration.proven,
        "the hard-capped stage cannot recover a representative strict window"
    );
    assert_eq!(
        outputs.active_ack_clock_calibration, None,
        "a terminal stage without causal evidence retires after exact flights drain"
    );
}

#[test]
fn later_owner_ack_window_proves_tcp_but_not_udp_without_carrier_evidence() {
    let sample_bytes = reliable_subflow_startup_sample_limit_bytes(MuxLimits::default());
    let frame_bytes = BBR_MAX_SEND_QUANTUM_BYTES as u64;
    assert_eq!(sample_bytes % frame_bytes, 0);

    for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
        let (binding, key) = binding_for_underlay(underlay);
        for offset in (0..2 * sample_bytes).step_by(BBR_MAX_SEND_QUANTUM_BYTES) {
            binding.record_owner_flight(
                key,
                &stream_data_frame_at(offset, BBR_MAX_SEND_QUANTUM_BYTES),
            );
        }
        let first_ack = Instant::now() + RESPONSE_ACK_CLOCK_GOODPUT_MIN_ELAPSED;
        binding.release_normalized_acked_ranges_at(
            &[OffsetRange {
                start: 0,
                end: sample_bytes,
            }],
            first_ack,
        );
        binding.release_normalized_acked_ranges_at(
            &[OffsetRange {
                start: sample_bytes,
                end: 2 * sample_bytes,
            }],
            first_ack + RESPONSE_ACK_CLOCK_GOODPUT_MIN_ELAPSED,
        );

        let entry = first_output_entry(&binding);
        assert_eq!(
            entry.owner_data_acked_bytes,
            2 * sample_bytes,
            "{underlay:?}"
        );
        assert!(entry.product_progress_rate_bps.is_some(), "{underlay:?}");
        assert_eq!(
            server_output_has_bulk_rate_evidence(&entry),
            underlay == UnderlayProtocol::Tcp,
            "TCP may use product owner ACKs; QUIC requires local carrier bulk evidence"
        );
    }
}

#[test]
fn tcp_response_startup_ack_graduates_epoch_and_admits_next_candidate() {
    let limits = MuxLimits::default();
    let sample_bytes = reliable_subflow_startup_sample_limit_bytes(limits) as usize;
    let (binding, service) = binding_for_underlay(UnderlayProtocol::Tcp);
    let first = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let second = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(2),
    };
    let (first_commands, _first_receivers) = reliable_path_command_channels(8);
    let (second_commands, _second_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            first.underlay,
            first.path_id,
            first_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            reliable_relay_buffer_len(limits),
        ),
        ResponseStreamAttachOutcome::Attached
    );
    assert_eq!(
        binding.attach(
            second.underlay,
            second.path_id,
            second_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            reliable_relay_buffer_len(limits),
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let startup_input = |key| SubflowAdmissionInput {
        key,
        bulk_rate_proven: false,
        startup_owner_allowed: true,
        frontier_clear: true,
        completion_improves: false,
        observed_goodput_non_degrading: true,
        read_gap: Duration::ZERO,
        owner_bytes: sample_bytes,
        optional_overhead_bytes: 0,
    };

    assert_eq!(
        binding
            .commit_subflow_owner_admission(
                service,
                sample_bytes,
                0,
                Duration::ZERO,
                startup_input(first),
            )
            .decision,
        PathAdmissionDecision::AdmitSubflow
    );
    assert_eq!(
        binding
            .commit_subflow_owner_admission(
                service,
                sample_bytes,
                0,
                Duration::ZERO,
                startup_input(second),
            )
            .decision,
        PathAdmissionDecision::ProbeOnly,
        "only one unproven response candidate may own startup bytes"
    );
    let generation_before_ack = binding.subflow_state_snapshot().0;
    binding.record_owner_flight(first, &stream_data_frame(sample_bytes));
    std::thread::sleep(Duration::from_millis(1));
    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: sample_bytes as u64,
    }]);

    let (generation_after_ack, epoch) = binding.subflow_state_snapshot();
    assert_ne!(generation_after_ack, generation_before_ack);
    assert_eq!(
        epoch.as_ref().and_then(FlowSubflowSet::startup_owner_key),
        None,
        "exact TCP OwnerData ACK evidence should graduate the sampled response path"
    );
    {
        let outputs = binding.outputs.lock().expect("test response outputs lock");
        let entry = outputs
            .entries
            .iter()
            .find(|entry| entry.key == first)
            .expect("graduated TCP output remains attached");
        assert!(
            outputs
                .ack_clock_calibrations
                .contains_key(&(entry.key, entry.incarnation)),
            "TCP graduation creates an exact-incarnation ACK-clock phase"
        );
    }
    assert_eq!(
        binding
            .commit_subflow_owner_admission(
                service,
                sample_bytes,
                0,
                Duration::ZERO,
                startup_input(second),
            )
            .decision,
        PathAdmissionDecision::AdmitSubflow
    );
    assert_eq!(
        binding
            .subflow_state_snapshot()
            .1
            .and_then(|epoch| epoch.startup_owner_key()),
        Some(second)
    );
}

#[test]
fn endpoint_only_tcp_startup_uses_service_capacity_prior_without_exclusive_calibration() {
    let limits = MuxLimits::default();
    let sample_bytes = reliable_subflow_startup_sample_limit_bytes(limits) as usize;
    let service = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let (service_commands, _service_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(42),
        service.underlay,
        service.path_id,
        service_commands,
        FlowLane::Throughput,
    );
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
            reliable_relay_buffer_len(limits),
        ),
        ResponseStreamAttachOutcome::Attached
    );
    binding.mark_output_bulk_proven_for_test(service);
    binding.update_path_metrics(
        candidate,
        PathMetrics {
            path_id: candidate.path_id,
            underlay: candidate.underlay,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: metric_epoch_now(),
            metric_age_us: 0,
            min_rtt_us: 333_000,
            srtt_us: 333_000,
            rttvar_us: 0,
            jitter_us: 0,
            delivery_rate_bps: 1,
            pacing_rate_bps: 1,
            loss_ppm: 0,
            ecn_ppm: 0,
            loss_observed: false,
            ecn_observed: false,
            bytes_in_flight: 0,
            queue_bytes: 0,
            inflight_limit_bytes: 0,
            inflight_hi_bytes: 0,
            confidence_ppm: 0,
            app_limited: true,
            has_ack_derived_data_sample: false,
            data_sample_count: 0,
            data_sample_bytes: 0,
        },
        ServerPathMetricsSource::PeerHint,
    );
    {
        let outputs = binding.outputs.lock().expect("test response outputs lock");
        let service_entry = outputs
            .entries
            .iter()
            .find(|entry| entry.key == service)
            .expect("Service output");
        let candidate_entry = outputs
            .entries
            .iter()
            .find(|entry| entry.key == candidate)
            .expect("candidate output");
        assert!(server_output_has_bulk_rate_evidence_with_limits(
            service_entry,
            limits
        ));
        assert!(server_output_accepts_service_capacity_prior(
            candidate_entry
        ));
    }
    assert_eq!(
        binding
            .commit_subflow_owner_admission(
                service,
                sample_bytes,
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
                    owner_bytes: sample_bytes,
                    optional_overhead_bytes: 0,
                },
            )
            .decision,
        PathAdmissionDecision::AdmitSubflow
    );
    binding.record_owner_flight(candidate, &stream_data_frame(sample_bytes));
    std::thread::sleep(Duration::from_millis(1));
    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: sample_bytes as u64,
    }]);
    assert_eq!(
        binding
            .subflow_state_snapshot()
            .1
            .and_then(|epoch| epoch.startup_owner_key()),
        None,
        "exact startup drain must graduate before installing a capacity prior"
    );

    {
        let outputs = binding.outputs.lock().expect("test response outputs lock");
        let entry = outputs
            .entries
            .iter()
            .find(|entry| entry.key == candidate)
            .expect("graduated candidate output");
        assert!(
            server_output_accepts_service_capacity_prior(entry),
            "post-startup eligibility: delivery_samples={} local_ack={} peer_app_limited={} peer_ack={} calibrations={}",
            entry.delivery_samples,
            entry
                .local_path_metrics
                .is_some_and(|metrics| metrics.metrics.has_ack_derived_data_sample),
            entry
                .peer_path_metrics
                .is_some_and(|metrics| metrics.metrics.app_limited),
            entry
                .peer_path_metrics
                .is_some_and(|metrics| metrics.metrics.has_ack_derived_data_sample),
            outputs.ack_clock_calibrations.len(),
        );
        let prior = entry
            .tcp_capacity_prior
            .expect("Service opportunity capacity prior");
        assert_test_rate_close(Some(prior.rate_bps), 100_000_000.0);
        assert_eq!(prior.ordinary_windows, 0);
        assert!(outputs.ack_clock_calibrations.is_empty());
    }
    let target = binding
        .sender_path_targets(FlowLane::Throughput, BBR_MAX_SEND_QUANTUM_BYTES)
        .into_iter()
        .find(|target| target.key == candidate)
        .expect("graduated endpoint-only candidate");
    assert!(target.has_bulk_rate_evidence);
    assert!(!target.ack_clock_calibration_eligible);
    assert_eq!(target.snapshot.rate_scope, PathRateScope::PathCapacity);
    assert_test_rate_close(Some(target.snapshot.delivery_rate_bps), 100_000_000.0);
}

#[test]
fn tcp_response_graduation_skips_calibration_below_ack_sample_resource_floor() {
    let limits = MuxLimits {
        max_path_flight_bytes: PATH_OPEN_SCORE_BYTES - 1,
        ..MuxLimits::default()
    };
    let sample_bytes = reliable_subflow_startup_sample_limit_bytes(limits) as usize;
    let service = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let candidate = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (service_commands, _service_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits(
        SessionId(43),
        service.underlay,
        service.path_id,
        service_commands,
        FlowLane::Throughput,
        limits,
    );
    let (candidate_commands, _candidate_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            candidate.underlay,
            candidate.path_id,
            candidate_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            reliable_relay_buffer_len(limits),
        ),
        ResponseStreamAttachOutcome::Attached
    );
    assert_eq!(
        binding
            .commit_subflow_owner_admission(
                service,
                sample_bytes,
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
                    owner_bytes: sample_bytes,
                    optional_overhead_bytes: 0,
                },
            )
            .decision,
        PathAdmissionDecision::AdmitSubflow
    );
    binding.record_owner_flight(candidate, &stream_data_frame(sample_bytes));
    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: sample_bytes as u64,
    }]);
    assert_eq!(
        binding
            .subflow_state_snapshot()
            .1
            .and_then(|epoch| epoch.startup_owner_key()),
        None,
        "a fully ACKed TCP startup sample may graduate without inventing a rate"
    );
    let candidate_target = binding
        .sender_path_targets(FlowLane::Throughput, 1)
        .into_iter()
        .find(|target| target.key == candidate)
        .expect("graduated candidate target");
    assert!(!candidate_target.ack_clock_calibration_eligible);
}

#[test]
fn udp_response_startup_requires_local_carrier_bulk_evidence_to_graduate() {
    let limits = MuxLimits::default();
    let sample_bytes = reliable_subflow_startup_sample_limit_bytes(limits) as usize;
    let (binding, service) = binding_for_underlay(UnderlayProtocol::Udp);
    let first = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let second = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(2),
    };
    let (first_commands, _first_receivers) = reliable_path_command_channels(8);
    let (second_commands, _second_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            first.underlay,
            first.path_id,
            first_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            reliable_relay_buffer_len(limits),
        ),
        ResponseStreamAttachOutcome::Attached
    );
    assert_eq!(
        binding.attach(
            second.underlay,
            second.path_id,
            second_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            reliable_relay_buffer_len(limits),
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let startup_input = |key| SubflowAdmissionInput {
        key,
        bulk_rate_proven: false,
        startup_owner_allowed: true,
        frontier_clear: true,
        completion_improves: false,
        observed_goodput_non_degrading: true,
        read_gap: Duration::ZERO,
        owner_bytes: sample_bytes,
        optional_overhead_bytes: 0,
    };

    assert_eq!(
        binding
            .commit_subflow_owner_admission(
                service,
                sample_bytes,
                0,
                Duration::ZERO,
                startup_input(first),
            )
            .decision,
        PathAdmissionDecision::AdmitSubflow
    );
    let generation_before_ack = binding.subflow_state_snapshot().0;
    binding.record_owner_flight(first, &stream_data_frame(sample_bytes));
    std::thread::sleep(Duration::from_millis(1));
    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: sample_bytes as u64,
    }]);

    let (generation_after_ack, epoch) = binding.subflow_state_snapshot();
    assert_eq!(generation_after_ack, generation_before_ack);
    assert_eq!(
        epoch.as_ref().and_then(FlowSubflowSet::startup_owner_key),
        Some(first),
        "UDP product ACKs alone must not graduate a QUIC response Subflow"
    );
    assert_eq!(
        binding
            .commit_subflow_owner_admission(
                service,
                sample_bytes,
                0,
                Duration::ZERO,
                startup_input(second),
            )
            .decision,
        PathAdmissionDecision::ProbeOnly
    );

    binding.update_path_metrics(
        first,
        PathMetrics {
            path_id: first.path_id,
            underlay: first.underlay,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: metric_epoch_now(),
            metric_age_us: 0,
            min_rtt_us: 80_000,
            srtt_us: 80_000,
            rttvar_us: 5_000,
            jitter_us: 5_000,
            delivery_rate_bps: 200_000_000,
            pacing_rate_bps: 200_000_000,
            loss_ppm: 0,
            ecn_ppm: 0,
            loss_observed: false,
            ecn_observed: false,
            bytes_in_flight: 0,
            queue_bytes: 0,
            inflight_limit_bytes: sample_bytes as u64,
            inflight_hi_bytes: sample_bytes as u64,
            confidence_ppm: 1_000_000,
            app_limited: false,
            has_ack_derived_data_sample: true,
            data_sample_count: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
            data_sample_bytes: sample_bytes as u64,
        },
        ServerPathMetricsSource::LocalSender,
    );

    let (generation_after_carrier_proof, epoch) = binding.subflow_state_snapshot();
    assert_ne!(generation_after_carrier_proof, generation_after_ack);
    assert_eq!(
        epoch.as_ref().and_then(FlowSubflowSet::startup_owner_key),
        None
    );
    assert!(
        binding
            .outputs
            .lock()
            .expect("test response outputs lock")
            .ack_clock_calibrations
            .is_empty(),
        "UDP/QUIC graduation remains carrier-owned and never enters TCP calibration"
    );
    assert_eq!(
        binding
            .commit_subflow_owner_admission(
                service,
                sample_bytes,
                0,
                Duration::ZERO,
                startup_input(second),
            )
            .decision,
        PathAdmissionDecision::AdmitSubflow
    );
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
fn tcp_calibration_commit_fences_generations_and_rolls_back_blocked_enqueue() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = PATH_OPEN_SCORE_BYTES;
    let session_id = SessionId(190);
    let tracker = Arc::new(ServerPathLaneTracker::default());
    let service = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let candidate = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (service_commands, mut service_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        service.underlay,
        service.path_id,
        service_commands,
        FlowLane::Throughput,
        mux_limits,
        tracker.clone(),
    );
    let (second_commands, _second_receivers) = reliable_path_command_channels(8);
    let mut second_flow = Some(ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(9),
        second_commands,
        FlowLane::Throughput,
        mux_limits,
        tracker.clone(),
    ));
    assert_eq!(binding.lane_generation_and_active_response_flows().1, 2);

    let (candidate_commands, mut candidate_receivers) = reliable_path_command_channels(1);
    assert_eq!(
        binding.attach(
            candidate.underlay,
            candidate.path_id,
            candidate_commands.clone(),
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            payload_bytes,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    assert!(matches!(
        try_recv_reliable_path_command(&mut candidate_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
    ));

    let (service_incarnation, candidate_incarnation) = {
        let mut outputs = binding.outputs.lock().expect("test response outputs lock");
        for entry in &mut outputs.entries {
            if entry.key == service || entry.key == candidate {
                mark_test_response_output_bulk_proven(entry, mux_limits);
            }
        }
        let service_incarnation = outputs
            .entries
            .iter()
            .find(|entry| entry.key == service)
            .expect("service output")
            .incarnation;
        let candidate_incarnation = outputs
            .entries
            .iter()
            .find(|entry| entry.key == candidate)
            .expect("candidate output")
            .incarnation;
        outputs.ack_clock_calibrations.insert(
            (candidate, candidate_incarnation),
            ResponseAckClockCalibrationState::new(
                reliable_ack_clock_calibration_limit_bytes(mux_limits),
                reliable_ack_clock_calibration_ceiling_bytes(mux_limits),
            ),
        );
        (service_incarnation, candidate_incarnation)
    };
    assert_eq!(
        binding
            .commit_subflow_owner_admission(
                service,
                payload_bytes,
                0,
                Duration::ZERO,
                SubflowAdmissionInput {
                    key: candidate,
                    bulk_rate_proven: true,
                    startup_owner_allowed: false,
                    frontier_clear: true,
                    completion_improves: true,
                    observed_goodput_non_degrading: true,
                    read_gap: Duration::ZERO,
                    owner_bytes: payload_bytes,
                    optional_overhead_bytes: 0,
                },
            )
            .decision,
        PathAdmissionDecision::AdmitSubflow
    );
    let target = binding
        .sender_path_targets(FlowLane::Throughput, payload_bytes)
        .into_iter()
        .find(|target| target.key == candidate)
        .expect("candidate target");
    let service_target = binding
        .sender_path_targets(FlowLane::Throughput, payload_bytes)
        .into_iter()
        .find(|target| target.key == service)
        .expect("service target");
    let request_for = |binding: &ResponseStreamBinding| {
        let (expected_planner_generation, _) = binding.subflow_state_snapshot();
        ResponseAckClockCalibrationRequest {
            expected_planner_generation,
            expected_lane_generation: binding.lane_generation(),
            expected_model_generation: binding.response_model_generation(),
            service,
            service_incarnation,
            service_pending_bytes: 0,
            target_pending_bytes: target.commands.pending_bytes(),
            limit_bytes: reliable_ack_clock_calibration_limit_bytes(mux_limits),
            requires_active_response_start: true,
        }
    };
    let frame = stream_data_frame(payload_bytes);

    let stale_model = request_for(&binding);
    binding.set_output_product_model_for_test(candidate, 500_000_000.0, 10.0);
    assert!(matches!(
        binding.try_enqueue_owner_frame_for_target(
            &target,
            &frame,
            FlowLane::Throughput,
            None,
            Some(stale_model),
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));

    let stale = request_for(&binding);
    binding.invalidate_subflow_plan();
    assert!(matches!(
        binding.try_enqueue_owner_frame_for_target(
            &target,
            &frame,
            FlowLane::Throughput,
            None,
            Some(stale),
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));

    let stale_lane = request_for(&binding);
    drop(second_flow.take());
    assert_eq!(binding.lane_generation_and_active_response_flows().1, 1);
    assert!(matches!(
        binding.try_enqueue_owner_frame_for_target(
            &target,
            &frame,
            FlowLane::Throughput,
            None,
            Some(stale_lane),
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    let (replacement_commands, _replacement_receivers) = reliable_path_command_channels(8);
    second_flow = Some(ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(9),
        replacement_commands,
        FlowLane::Throughput,
        mux_limits,
        tracker,
    ));
    assert_eq!(binding.lane_generation_and_active_response_flows().1, 2);

    let stale_stage = request_for(&binding);
    {
        let mut outputs = binding.outputs.lock().expect("test response outputs lock");
        let calibration = outputs
            .ack_clock_calibrations
            .get_mut(&(candidate, candidate_incarnation))
            .expect("candidate calibration state");
        calibration.spent_bytes = calibration.credit_limit_bytes;
        let stage_authorized_at = calibration.stage_authorized_at;
        let sample =
            test_ack_clock_rate_sample(calibration.stage_rate_coverage_floor_bytes, 10_000_000.0);
        assert!(calibration.record_ack_clock_sample(
            sample,
            stage_authorized_at,
            stage_authorized_at + Duration::from_millis(1),
        ));
    }
    assert!(matches!(
        binding.try_enqueue_owner_frame_for_target(
            &target,
            &frame,
            FlowLane::Throughput,
            None,
            Some(stale_stage),
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    {
        let mut outputs = binding.outputs.lock().expect("test response outputs lock");
        outputs.ack_clock_calibrations.insert(
            (candidate, candidate_incarnation),
            ResponseAckClockCalibrationState::new(
                reliable_ack_clock_calibration_limit_bytes(mux_limits),
                reliable_ack_clock_calibration_ceiling_bytes(mux_limits),
            ),
        );
    }

    let stale_target_pending = request_for(&binding);
    candidate_commands
        .try_enqueue_stream_ordered_frame(
            stream_data_frame_at(payload_bytes as u64, payload_bytes),
            FlowLane::Throughput,
        )
        .expect("change candidate pending bytes");
    let candidate_pending_command = try_recv_reliable_path_command(&mut candidate_receivers)
        .expect("drain candidate queue without releasing pending bytes");
    let candidate_pending_bytes = reliable_path_command_pending_bytes(&candidate_pending_command);
    assert!(matches!(
        binding.try_enqueue_owner_frame_for_target(
            &target,
            &frame,
            FlowLane::Throughput,
            None,
            Some(stale_target_pending),
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    candidate_receivers.release_pending_command_bytes(candidate_pending_bytes);

    let stale_service_pending = request_for(&binding);
    service_target
        .commands
        .try_enqueue_stream_ordered_frame(
            stream_data_frame_at(payload_bytes as u64, payload_bytes),
            FlowLane::Throughput,
        )
        .expect("change service pending bytes");
    let service_pending_command = try_recv_reliable_path_command(&mut service_receivers)
        .expect("drain service queue without releasing pending bytes");
    let service_pending_bytes = reliable_path_command_pending_bytes(&service_pending_command);
    assert!(matches!(
        binding.try_enqueue_owner_frame_for_target(
            &target,
            &frame,
            FlowLane::Throughput,
            None,
            Some(stale_service_pending),
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    service_receivers.release_pending_command_bytes(service_pending_bytes);

    candidate_commands
        .try_enqueue_stream_ordered_frame(
            stream_data_frame_at(payload_bytes as u64, payload_bytes),
            FlowLane::Throughput,
        )
        .expect("fill candidate queue");
    let fresh = request_for(&binding);
    assert!(matches!(
        binding.try_enqueue_owner_frame_for_target(
            &target,
            &frame,
            FlowLane::Throughput,
            None,
            Some(fresh),
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    {
        let outputs = binding.outputs.lock().expect("test response outputs lock");
        assert_eq!(
            outputs
                .ack_clock_calibrations
                .get(&(candidate, candidate_incarnation))
                .expect("candidate calibration state")
                .spent_bytes,
            0,
            "blocked enqueue restores cumulative calibration credit"
        );
        assert_eq!(outputs.active_ack_clock_calibration, None);
    }
    assert!(try_recv_reliable_path_command(&mut candidate_receivers).is_some());

    binding
        .try_enqueue_owner_frame_for_target(
            &target,
            &frame,
            FlowLane::Throughput,
            None,
            Some(request_for(&binding)),
        )
        .expect("fresh exact calibration reservation enqueues");
    {
        let outputs = binding.outputs.lock().expect("test response outputs lock");
        assert_eq!(
            outputs
                .ack_clock_calibrations
                .get(&(candidate, candidate_incarnation))
                .expect("candidate calibration state")
                .spent_bytes,
            payload_bytes as u64
        );
        assert_eq!(
            outputs.active_ack_clock_calibration,
            Some((candidate, candidate_incarnation))
        );
    }

    binding.detach(candidate, &candidate_commands);
    assert!(matches!(
        binding.try_enqueue_owner_frame_for_target(
            &target,
            &frame,
            FlowLane::Throughput,
            None,
            Some(request_for(&binding)),
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    assert!(
        binding
            .outputs
            .lock()
            .expect("test response outputs lock")
            .ack_clock_calibrations
            .get(&(candidate, candidate_incarnation))
            .is_none(),
        "detach removes exact-incarnation calibration state"
    );
    drop(second_flow);
}

#[test]
fn subflow_reservation_and_enqueue_linearize_before_topology_reset() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let candidate = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let unrelated = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(2),
    };
    let (service_commands, _service_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits(
        SessionId(91),
        service.underlay,
        service.path_id,
        service_commands,
        FlowLane::Throughput,
        mux_limits,
    );
    let (candidate_commands, mut candidate_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            candidate.underlay,
            candidate.path_id,
            candidate_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            payload_bytes,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    assert!(matches!(
        try_recv_reliable_path_command(&mut candidate_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
    ));
    let (unrelated_commands, mut unrelated_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            unrelated.underlay,
            unrelated.path_id,
            unrelated_commands.clone(),
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            payload_bytes,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    assert!(matches!(
        try_recv_reliable_path_command(&mut unrelated_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
    ));

    let target = binding
        .sender_path_targets(FlowLane::Throughput, payload_bytes)
        .into_iter()
        .find(|target| target.key == candidate)
        .expect("candidate output is attached");
    let (planner_generation, _) = binding.subflow_state_snapshot();
    let request = ResponseSubflowAdmissionRequest {
        expected_planner_generation: planner_generation,
        expected_lane_generation: binding.lane_generation(),
        service,
        startup_owner_credit_bytes: payload_bytes,
        optional_overhead_budget_bytes: 0,
        max_read_gap_budget: Duration::ZERO,
        input: SubflowAdmissionInput {
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
    };
    let frame = stream_data_frame(payload_bytes);
    let frame_for_sender = frame.clone();
    let binding_for_sender = binding.clone();
    let (reserved_tx, reserved_rx) = std_mpsc::sync_channel(0);
    let (resume_tx, resume_rx) = std_mpsc::sync_channel(0);
    let sender = std::thread::spawn(move || {
        binding_for_sender.try_enqueue_owner_frame_for_target_inner(
            &ResponseDispatchTarget::from(&target),
            &frame_for_sender,
            FlowLane::Throughput,
            Some(request),
            None,
            || {
                reserved_tx
                    .send(())
                    .expect("reservation observer remains live");
                resume_rx.recv().expect("reservation test resumes enqueue");
            },
        )
    });
    reserved_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("Subflow reservation reaches the pre-enqueue barrier");

    let outputs_locked_across_reservation = matches!(
        binding.outputs.try_lock(),
        Err(std::sync::TryLockError::WouldBlock)
    );
    let binding_for_detach = binding.clone();
    let (detach_started_tx, detach_started_rx) = std_mpsc::sync_channel(0);
    let (detach_done_tx, detach_done_rx) = std_mpsc::channel();
    let detacher = std::thread::spawn(move || {
        detach_started_tx
            .send(())
            .expect("detach observer remains live");
        binding_for_detach.detach(unrelated, &unrelated_commands);
        detach_done_tx
            .send(())
            .expect("detach completion observer remains live");
    });
    detach_started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("detach attempt starts while enqueue is paused");
    let generation_while_paused = binding.subflow_state_snapshot().0;
    let detach_completed_while_paused = detach_done_rx
        .recv_timeout(Duration::from_millis(50))
        .is_ok();

    resume_tx
        .send(())
        .expect("paused reservation remains ready to enqueue");
    let reservation_epoch = sender
        .join()
        .expect("sender thread does not panic")
        .expect("generation-fenced reservation enqueues");
    detacher.join().expect("detach thread does not panic");

    assert!(
        outputs_locked_across_reservation,
        "outputs must remain locked from Subflow reservation through owner enqueue"
    );
    assert_eq!(generation_while_paused, planner_generation);
    assert!(
        !detach_completed_while_paused,
        "topology reset must not linearize between reservation and enqueue"
    );
    assert!(reservation_epoch.is_some());
    assert_ne!(binding.subflow_state_snapshot().0, planner_generation);
    assert!(matches!(
        try_recv_reliable_path_command(&mut candidate_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
    assert_eq!(
        binding.owner_flight_keys_overlapping_frame(&frame),
        vec![candidate],
        "owner flight must be recorded before the topology reset"
    );
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

#[test]
fn lane_tracker_reclaims_session_state_when_last_binding_drops() {
    let session_id = SessionId(95);
    let lane_tracker = Arc::new(ServerPathLaneTracker::default());
    let (commands, _receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(0),
        commands,
        FlowLane::Throughput,
        MuxLimits::default(),
        lane_tracker.clone(),
    );

    {
        let state = lane_tracker
            .state
            .lock()
            .expect("server path lane tracker lock");
        assert_eq!(state.session_references.get(&session_id), Some(&1));
        assert_eq!(state.active_response_flows.get(&session_id), Some(&1));
        assert!(state.session_generations.contains_key(&session_id));
        assert!(state.loads.keys().any(|key| key.session_id == session_id));
    }

    drop(binding);

    let state = lane_tracker
        .state
        .lock()
        .expect("server path lane tracker lock");
    assert!(!state.session_references.contains_key(&session_id));
    assert!(!state.session_generations.contains_key(&session_id));
    assert!(!state.realtime_flows.contains_key(&session_id));
    assert!(!state.active_response_flows.contains_key(&session_id));
    assert!(!state.loads.keys().any(|key| key.session_id == session_id));
}

#[test]
fn active_response_flow_count_is_per_binding_not_per_attachment() {
    let session_id = SessionId(99);
    let service = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    let alternate = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let lane_tracker = Arc::new(ServerPathLaneTracker::default());
    let (service_commands, _service_receivers) = reliable_path_command_channels(8);
    let service_commands_for_detach = service_commands.clone();
    let binding = ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        service.underlay,
        service.path_id,
        service_commands,
        FlowLane::Throughput,
        MuxLimits::default(),
        lane_tracker.clone(),
    );
    let (alternate_commands, _alternate_receivers) = reliable_path_command_channels(8);
    let alternate_commands_for_detach = alternate_commands.clone();
    assert_eq!(
        binding.attach(
            alternate.underlay,
            alternate.path_id,
            alternate_commands,
            FlowLane::Throughput,
            StreamOpenRole::Active,
            reliable_relay_buffer_len(MuxLimits::default()),
        ),
        ResponseStreamAttachOutcome::Attached
    );
    assert_eq!(lane_tracker.session_snapshot(session_id).active_flows, 2);
    assert_eq!(
        binding.lane_generation_and_active_response_flows().1,
        1,
        "one response stream must contribute one flow despite two Active attachments"
    );

    binding.detach(service, &service_commands_for_detach);
    assert_eq!(lane_tracker.session_snapshot(session_id).active_flows, 1);
    assert_eq!(binding.lane_generation_and_active_response_flows().1, 1);
    binding.detach(alternate, &alternate_commands_for_detach);
    assert_eq!(lane_tracker.session_snapshot(session_id).active_flows, 0);
    assert_eq!(
        binding.lane_generation_and_active_response_flows().1,
        0,
        "a response stream with no Active attachment must not satisfy the gate"
    );
}

#[test]
fn passive_attachments_do_not_consume_or_release_shared_flow_load() {
    let session_id = SessionId(97);
    let service_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    let shared_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let lane_tracker = Arc::new(ServerPathLaneTracker::default());
    let (service_commands, _service_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        service_key.underlay,
        service_key.path_id,
        service_commands,
        FlowLane::Throughput,
        MuxLimits::default(),
        lane_tracker.clone(),
    );
    let (shared_commands, _shared_receivers) = reliable_path_command_channels(8);
    let shared_binding = ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        shared_key.underlay,
        shared_key.path_id,
        shared_commands,
        FlowLane::Throughput,
        MuxLimits::default(),
        lane_tracker.clone(),
    );

    let (repair_commands, _repair_receivers) = reliable_path_command_channels(8);
    let repair_commands_for_detach = repair_commands.clone();
    assert_eq!(
        binding.attach(
            shared_key.underlay,
            shared_key.path_id,
            repair_commands,
            FlowLane::Throughput,
            StreamOpenRole::Repair,
            reliable_relay_buffer_len(MuxLimits::default()),
        ),
        ResponseStreamAttachOutcome::Attached
    );
    assert_eq!(
        lane_tracker.snapshot(session_id, shared_key).active_flows,
        1
    );
    binding.detach(shared_key, &repair_commands_for_detach);
    assert_eq!(
        lane_tracker.snapshot(session_id, shared_key).active_flows,
        1,
        "detaching passive Repair must not debit another stream's share"
    );

    let (validation_commands, _validation_receivers) = reliable_path_command_channels(8);
    let validation_commands_for_promotion = validation_commands.clone();
    let validation_commands_for_repeat = validation_commands.clone();
    let validation_commands_for_detach = validation_commands.clone();
    assert_eq!(
        binding.attach(
            shared_key.underlay,
            shared_key.path_id,
            validation_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            reliable_relay_buffer_len(MuxLimits::default()),
        ),
        ResponseStreamAttachOutcome::Attached
    );
    assert_eq!(
        lane_tracker.snapshot(session_id, shared_key).active_flows,
        1
    );
    assert_eq!(lane_tracker.session_snapshot(session_id).active_flows, 2);

    assert_eq!(
        binding.attach(
            shared_key.underlay,
            shared_key.path_id,
            validation_commands_for_promotion,
            FlowLane::Latency,
            StreamOpenRole::Active,
            reliable_relay_buffer_len(MuxLimits::default()),
        ),
        ResponseStreamAttachOutcome::RoleChanged
    );
    let service_load = lane_tracker.snapshot(session_id, service_key);
    assert_eq!(service_load.active_flows, 1);
    assert_eq!(service_load.active_latency_sensitive_flows, 1);
    let shared_load = lane_tracker.snapshot(session_id, shared_key);
    assert_eq!(shared_load.active_flows, 2);
    assert_eq!(
        shared_load.active_latency_sensitive_flows, 1,
        "promotion must add this stream in its new lane without moving the other stream"
    );

    assert_eq!(
        binding.attach(
            shared_key.underlay,
            shared_key.path_id,
            validation_commands_for_repeat,
            FlowLane::Latency,
            StreamOpenRole::Active,
            reliable_relay_buffer_len(MuxLimits::default()),
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let repeated_shared_load = lane_tracker.snapshot(session_id, shared_key);
    assert_eq!(repeated_shared_load.active_flows, shared_load.active_flows);
    assert_eq!(
        repeated_shared_load.active_latency_sensitive_flows,
        shared_load.active_latency_sensitive_flows
    );

    binding.detach(shared_key, &validation_commands_for_detach);
    let remaining_shared_load = lane_tracker.snapshot(session_id, shared_key);
    assert_eq!(remaining_shared_load.active_flows, 1);
    assert_eq!(remaining_shared_load.active_latency_sensitive_flows, 0);
    drop(binding);
    assert_eq!(
        lane_tracker.snapshot(session_id, service_key).active_flows,
        0
    );
    assert_eq!(
        lane_tracker.snapshot(session_id, shared_key).active_flows,
        1
    );
    drop(shared_binding);
    assert_eq!(
        lane_tracker.snapshot(session_id, shared_key).active_flows,
        0
    );
}

#[test]
fn closed_output_replacement_reconciles_role_flow_load() {
    let session_id = SessionId(98);
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let lane_tracker = Arc::new(ServerPathLaneTracker::default());
    let (active_commands, active_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        key.underlay,
        key.path_id,
        active_commands,
        FlowLane::Throughput,
        MuxLimits::default(),
        lane_tracker.clone(),
    );
    assert_eq!(lane_tracker.snapshot(session_id, key).active_flows, 1);
    assert_eq!(binding.lane_generation_and_active_response_flows().1, 1);
    drop(active_receivers);

    let (validation_commands, validation_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            key.underlay,
            key.path_id,
            validation_commands,
            FlowLane::Latency,
            StreamOpenRole::Validation,
            reliable_relay_buffer_len(MuxLimits::default()),
        ),
        ResponseStreamAttachOutcome::ReplacedClosedOutput
    );
    assert_eq!(lane_tracker.snapshot(session_id, key).active_flows, 0);
    assert_eq!(binding.lane_generation_and_active_response_flows().1, 0);
    drop(validation_receivers);

    let (replacement_commands, _replacement_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            key.underlay,
            key.path_id,
            replacement_commands,
            FlowLane::Throughput,
            StreamOpenRole::Active,
            reliable_relay_buffer_len(MuxLimits::default()),
        ),
        ResponseStreamAttachOutcome::ReplacedClosedOutput
    );
    let replacement_load = lane_tracker.snapshot(session_id, key);
    assert_eq!(replacement_load.active_flows, 1);
    assert_eq!(replacement_load.active_latency_sensitive_flows, 0);
    assert_eq!(binding.lane_generation_and_active_response_flows().1, 1);
    drop(binding);
    assert_eq!(lane_tracker.snapshot(session_id, key).active_flows, 0);
}

#[tokio::test]
async fn close_command_detaches_shared_lane_load_exactly_once() {
    let session_id = SessionId(96);
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let lane_tracker = Arc::new(ServerPathLaneTracker::default());
    let (first_commands, _first_receivers) = reliable_path_command_channels(8);
    let first_commands_for_detach = first_commands.clone();
    let first = ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        key.underlay,
        key.path_id,
        first_commands,
        FlowLane::Throughput,
        MuxLimits::default(),
        lane_tracker.clone(),
    );
    let (second_commands, _second_receivers) = reliable_path_command_channels(8);
    let second = ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        key.underlay,
        key.path_id,
        second_commands,
        FlowLane::Throughput,
        MuxLimits::default(),
        lane_tracker.clone(),
    );
    assert_eq!(lane_tracker.snapshot(session_id, key).active_flows, 2);
    let stale_target = first
        .sender_path_targets(FlowLane::Throughput, 64 * 1024)
        .into_iter()
        .find(|target| target.key == key)
        .expect("first response Service target");

    first.close_stream(StreamId(10)).await;
    assert_eq!(
        lane_tracker.snapshot(session_id, key).active_flows,
        2,
        "enqueuing close does not complete carrier detachment"
    );
    assert_eq!(
        first.response_scheduling_snapshot().service_family_loads,
        ResponseServiceFamilyLoads::new(1, 0),
        "close retires product Service ownership independently of attachment cleanup"
    );
    assert!(!first.commit_ordered_data_owner_for_target(&stale_target));
    first.set_lane(FlowLane::Latency);
    assert_eq!(
        first.response_scheduling_snapshot().service_family_loads,
        ResponseServiceFamilyLoads::new(1, 0),
        "a stale owner commit or lane change cannot resurrect closed Service load"
    );

    first.detach(key, &first_commands_for_detach);
    first.detach(key, &first_commands_for_detach);
    assert_eq!(
        lane_tracker.snapshot(session_id, key).active_flows,
        1,
        "command handling and repeated cleanup must leave the other stream counted"
    );

    drop(first);
    assert_eq!(lane_tracker.snapshot(session_id, key).active_flows, 1);
    drop(second);
    assert_eq!(lane_tracker.snapshot(session_id, key).active_flows, 0);
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
        first_output_entry(&binding).bytes_in_flight,
        BBR_MAX_SEND_QUANTUM_BYTES as u64
    );
    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: BBR_MAX_SEND_QUANTUM_BYTES as u64,
    }]);

    let entry = first_output_entry(&binding);
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
    assert_eq!(first_output_entry(&binding).bytes_in_flight, 0);
    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: BBR_MAX_SEND_QUANTUM_BYTES as u64,
    }]);

    let entry = first_output_entry(&binding);
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

    let entry = first_output_entry(&binding);
    assert_eq!(entry.owner_data_acked_bytes, 0);
    assert_eq!(entry.delivery_samples, 0);
    assert!(entry.product_progress_rate_bps.is_none());
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
            reliable_relay_buffer_len(MuxLimits::default()),
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let frame = stream_data_frame(BBR_MAX_SEND_QUANTUM_BYTES);
    binding.record_owner_flight(key, &frame);
    let before_role_change = first_output_entry(&binding);
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
            reliable_relay_buffer_len(MuxLimits::default()),
        ),
        ResponseStreamAttachOutcome::RoleChanged
    );
    let after_role_change = first_output_entry(&binding);
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
            reliable_relay_buffer_len(limits),
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
        read_gap: Duration::ZERO,
        owner_bytes: sample_bytes,
        optional_overhead_bytes: 0,
    };
    assert_eq!(
        binding
            .commit_subflow_owner_admission(
                service,
                sample_bytes,
                0,
                Duration::ZERO,
                startup_input,
            )
            .decision,
        PathAdmissionDecision::AdmitSubflow
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
            reliable_relay_buffer_len(limits),
        ),
        ResponseStreamAttachOutcome::RoleChanged
    );

    let target = binding
        .sender_path_targets(FlowLane::Throughput, BBR_MAX_SEND_QUANTUM_BYTES)
        .into_iter()
        .find(|target| target.key == candidate)
        .expect("promoted response output");
    assert_eq!(target.incarnation, incarnation);
    assert!(target.has_bulk_rate_evidence);
    let (after_generation, after_epoch) = binding.subflow_state_snapshot();
    assert_eq!(after_generation, planner_generation);
    assert_eq!(
        after_epoch.and_then(|epoch| epoch.startup_owner_key()),
        Some(candidate),
        "request-role promotion cannot erase paid-for response membership"
    );
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
        first_output_entry(&binding).bytes_in_flight,
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

#[test]
fn peer_app_limited_metrics_do_not_seed_response_bulk_rate_or_envelope() {
    for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
        let (binding, key) = binding_for_underlay(underlay);
        let metrics = PathMetrics {
            path_id: key.path_id,
            underlay: key.underlay,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: metric_epoch_now(),
            metric_age_us: 0,
            min_rtt_us: 20_000,
            srtt_us: 20_000,
            rttvar_us: 1_000,
            jitter_us: 1_000,
            delivery_rate_bps: 614_000,
            pacing_rate_bps: 614_000,
            loss_ppm: 0,
            ecn_ppm: 0,
            loss_observed: false,
            ecn_observed: false,
            bytes_in_flight: PATH_OPEN_SCORE_BYTES as u64,
            queue_bytes: PATH_OPEN_SCORE_BYTES as u64,
            inflight_limit_bytes: PATH_OPEN_SCORE_BYTES as u64,
            inflight_hi_bytes: PATH_OPEN_SCORE_BYTES as u64,
            confidence_ppm: 900_000,
            app_limited: true,
            has_ack_derived_data_sample: true,
            data_sample_count: 142,
            data_sample_bytes: 0,
        };
        binding.update_path_metrics(key, metrics, ServerPathMetricsSource::PeerHint);

        let snapshot = binding
            .send_path_snapshot(FlowLane::Throughput, MIN_RATE_SAMPLE_BYTES as usize)
            .expect("peer metrics remain validation hints");
        assert_eq!(snapshot.delivery_rate_bps, default_path_rate_bps(underlay));
        assert_eq!(snapshot.pacing_rate_bps, snapshot.delivery_rate_bps);
        assert_eq!(snapshot.inflight_limit_bytes, 0);
        assert_eq!(snapshot.bytes_in_flight, 0);
        assert_eq!(snapshot.confidence, 0.0);
        assert!(snapshot.app_limited);
    }
}

#[test]
fn response_peer_hint_yields_to_durable_local_quic_estimate() {
    let (binding, key) = binding_for_underlay(UnderlayProtocol::Udp);
    let mut peer_hint = PathMetrics {
        path_id: key.path_id,
        underlay: key.underlay,
        direction: PathMetricDirection::ClientToServer,
        metric_epoch: metric_epoch_now(),
        metric_age_us: 0,
        min_rtt_us: 200_000,
        srtt_us: 200_000,
        rttvar_us: 10_000,
        jitter_us: 10_000,
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
        confidence_ppm: 100_000,
        app_limited: false,
        has_ack_derived_data_sample: false,
        data_sample_count: 0,
        data_sample_bytes: 0,
    };
    binding.update_path_metrics(key, peer_hint, ServerPathMetricsSource::PeerHint);

    let local_proof = PathMetrics {
        direction: PathMetricDirection::ServerToClient,
        min_rtt_us: 20_000,
        srtt_us: 20_000,
        rttvar_us: 1_000,
        jitter_us: 1_000,
        delivery_rate_bps: 500_000,
        pacing_rate_bps: 500_000,
        confidence_ppm: 1_000_000,
        app_limited: true,
        ..peer_hint
    };
    binding.update_path_metrics(key, local_proof, ServerPathMetricsSource::LocalSender);

    let snapshot = binding
        .send_path_snapshot(FlowLane::Throughput, MIN_RATE_SAMPLE_BYTES as usize)
        .expect("path remains attached");

    assert_eq!(snapshot.delivery_rate_bps, 200_000_000.0);
    assert_eq!(snapshot.srtt_ms, 20.0);
    assert!(snapshot.app_limited);

    peer_hint.delivery_rate_bps = 300_000_000;
    binding.update_path_metrics(key, peer_hint, ServerPathMetricsSource::PeerHint);
    let updated = binding
        .send_path_snapshot(FlowLane::Throughput, MIN_RATE_SAMPLE_BYTES as usize)
        .expect("path remains attached");
    assert_eq!(updated.delivery_rate_bps, 300_000_000.0);
    assert_eq!(
        updated.srtt_ms, 20.0,
        "local liveness RTT must not be erased by peer hint refresh"
    );

    let durable_local = PathMetrics {
        metric_epoch: metric_epoch_now(),
        delivery_rate_bps: 500_000,
        pacing_rate_bps: 500_000,
        app_limited: true,
        has_ack_derived_data_sample: true,
        data_sample_count: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
        data_sample_bytes: reliable_subflow_startup_sample_limit_bytes(MuxLimits::default()),
        ..local_proof
    };
    binding.update_path_metrics(key, durable_local, ServerPathMetricsSource::LocalSender);
    let local_estimate = binding
        .send_path_snapshot(FlowLane::Throughput, MIN_RATE_SAMPLE_BYTES as usize)
        .expect("path remains attached");
    assert_eq!(local_estimate.delivery_rate_bps, 500_000.0);
    assert!(local_estimate.app_limited);
    let entry = first_output_entry(&binding);
    assert!(!server_output_has_bulk_rate_evidence(&entry));
}

#[test]
fn tcp_local_sender_metrics_remain_send_quantum_prior_after_low_product_sample() {
    let (binding, key) = binding_for_underlay(UnderlayProtocol::Tcp);
    let mux_limits = binding.mux_limits();
    let metrics = PathMetrics {
        path_id: key.path_id,
        underlay: key.underlay,
        direction: PathMetricDirection::ServerToClient,
        metric_epoch: metric_epoch_now(),
        metric_age_us: 0,
        min_rtt_us: 20_000,
        srtt_us: 20_000,
        rttvar_us: 1_000,
        jitter_us: 1_000,
        delivery_rate_bps: 500_000_000,
        pacing_rate_bps: 500_000_000,
        loss_ppm: 0,
        ecn_ppm: 0,
        loss_observed: true,
        ecn_observed: false,
        bytes_in_flight: 0,
        queue_bytes: 0,
        inflight_limit_bytes: 0,
        inflight_hi_bytes: 0,
        confidence_ppm: 0,
        app_limited: false,
        has_ack_derived_data_sample: true,
        data_sample_count: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
        data_sample_bytes: MIN_RATE_SAMPLE_BYTES,
    };
    binding.update_path_metrics(key, metrics, ServerPathMetricsSource::LocalSender);

    let before_ack = binding
        .send_path_snapshot(FlowLane::Throughput, MIN_RATE_SAMPLE_BYTES as usize)
        .expect("path metrics seed response path snapshot");
    assert_eq!(before_ack.delivery_rate_bps, 500_000_000.0);

    let frame = stream_data_frame(MIN_RATE_SAMPLE_BYTES as usize);
    let later = stream_data_frame_at(MIN_RATE_SAMPLE_BYTES, MIN_RATE_SAMPLE_BYTES as usize);
    binding.record_owner_flight(key, &frame);
    binding.record_owner_flight(key, &later);
    let first_ack = Instant::now() + RESPONSE_ACK_CLOCK_GOODPUT_MIN_ELAPSED;
    binding.release_normalized_acked_ranges_at(
        &[OffsetRange {
            start: 0,
            end: reliable_stream_frame_payload_bytes(&frame) as u64,
        }],
        first_ack,
    );
    binding.release_normalized_acked_ranges_at(
        &[OffsetRange {
            start: MIN_RATE_SAMPLE_BYTES,
            end: 2 * MIN_RATE_SAMPLE_BYTES,
        }],
        first_ack + RESPONSE_ACK_CLOCK_GOODPUT_MIN_ELAPSED,
    );

    let entry = first_output_entry(&binding);
    assert_eq!(entry.delivery_samples, 2);
    assert!(
        entry.delivery_rate_bps.unwrap_or(f64::INFINITY) < 500_000_000.0,
        "the test must create a low product progress sample"
    );
    let after_ack = binding
        .send_path_snapshot(FlowLane::Throughput, MIN_RATE_SAMPLE_BYTES as usize)
        .expect("peer rate prior remains available after product ACK sample");
    assert_eq!(after_ack.delivery_rate_bps, 500_000_000.0);
    assert!(
        adaptive_reliable_relay_chunk_bytes(Some(after_ack), FlowLane::Throughput, mux_limits)
            > bbr_min_send_quantum_bytes(mux_limits),
        "a low product ACK sample must not collapse TCP send quantum below the path-rate prior"
    );
}

#[test]
fn tcp_fixed_output_startup_prior_yields_after_persistent_local_delivery_samples() {
    let mux_limits = MuxLimits::default();
    let (commands, _receivers) = reliable_path_command_channels(64);
    let startup_rate = 500_000_000.0;
    let startup = PathSnapshot::new(PathId(8), UnderlayProtocol::Tcp, 20.0, startup_rate);
    let output = ReliablePathStreamOutput::fixed_with_snapshot(startup, commands, mux_limits);
    let ReliablePathStreamOutput::Fixed(fixed) = &output else {
        panic!("expected fixed output");
    };
    assert_eq!(
        fixed.send_path_snapshot().rate_scope,
        PathRateScope::PathCapacity
    );
    let mut offset = 0_u64;

    for _ in 0..RELIABLE_INITIAL_WINDOW_PACKETS {
        let frame = stream_data_frame_at(offset, MIN_RATE_SAMPLE_BYTES as usize);
        let end = offset + reliable_stream_frame_payload_bytes(&frame) as u64;
        fixed.record_owner_flight(&frame);
        std::thread::sleep(Duration::from_millis(20));
        fixed.release_normalized_acked_ranges(&[OffsetRange { start: offset, end }]);
        offset = end;
    }

    let learned_rate = fixed
        .model
        .lock()
        .expect("fixed output model lock")
        .delivery_rate_bps
        .expect("persistent samples produce a delivery model");
    assert!(learned_rate < startup_rate * 0.5);

    let snapshot = output
        .send_path_snapshot(FlowLane::Throughput, MIN_RATE_SAMPLE_BYTES as usize)
        .expect("response binding exposes learned path model");
    assert!(
        snapshot.delivery_rate_bps < startup_rate * 0.5,
        "startup/default rate is only a hint; persistent local delivery samples must correct it downward"
    );
    assert_eq!(snapshot.rate_scope, PathRateScope::PerFlowGoodput);
}

#[test]
fn fixed_output_request_active_snapshot_preserves_send_path_timing() {
    let mux_limits = MuxLimits::default();
    let (commands, _receivers) = reliable_path_command_channels(8);
    let startup = PathSnapshot::new(PathId(9), UnderlayProtocol::Tcp, 123.0, 8_000_000.0);
    let output = ReliablePathStreamOutput::fixed_with_snapshot(startup, commands, mux_limits);
    let send_snapshot = output
        .send_path_snapshot(FlowLane::Latency, PATH_OPEN_SCORE_BYTES)
        .expect("fixed output has a send path snapshot");
    let request_active_snapshot = output
        .request_active_path_snapshot(FlowLane::Latency)
        .expect("fixed output has a request Active path snapshot");

    assert_eq!(request_active_snapshot.id, send_snapshot.id);
    assert_eq!(request_active_snapshot.underlay, send_snapshot.underlay);
    assert_eq!(request_active_snapshot.srtt_ms, send_snapshot.srtt_ms);
    assert_eq!(
        reliable_stream_recv_progress_interval(Some(request_active_snapshot), FlowLane::Latency,),
        reliable_stream_recv_progress_interval(Some(send_snapshot), FlowLane::Latency),
        "fixed-path replay cadence must remain unchanged"
    );
}
