use super::*;
use crate::model::admission::{
    bulk_service_feed_reservoir_payload_bytes, bulk_service_product_envelope_payload_bytes,
};
use crate::model::capacity::QuicCapacityProofCandidate;
use crate::model::response::{
    CarrierPathFlightDebt, ResponseOrderedTail, ResponseSameFamilyReservoir,
    ResponseServiceFamilyLoads, ResponseServiceHandoffMode,
};
use crate::protocol::frame::reliable_stream_frame_accounted_bytes;
use crate::runtime::sender::response::test_support::response_target;
use crate::runtime::stream::response::{
    ResponseAckClockCalibrationRetirementRequest, ResponseDispatchTarget, ResponseSenderPathTarget,
    ResponseServiceHandoffDrainReservation, ResponseStreamAttachOutcome, ResponseStreamBinding,
    ServerPathLaneTracker, ServerPathMetricsSource, next_server_carrier_path_instance_id,
};
use crate::scheduler::{PathRateScope, PathSnapshot};

#[test]
fn response_dispatch_plan_drops_ranked_snapshot_state() {
    let ranked = std::mem::size_of::<ResponseSenderPathTarget>();
    let dispatch = std::mem::size_of::<ResponseDispatchTarget>();
    let plan = std::mem::size_of::<ResponseDataDispatchTarget>();

    assert!(dispatch < ranked, "dispatch={dispatch} ranked={ranked}");
    assert!(
        plan <= 512,
        "the per-frame plan must not regain full scheduler snapshots: {plan} bytes"
    );
}

struct ResponseServiceHandoffDrainFixture {
    binding: Arc<ResponseStreamBinding>,
    other_binding: Arc<ResponseStreamBinding>,
    stream: ReliablePathStream,
    other_stream: ReliablePathStream,
    service: CarrierPathKey,
    target: CarrierPathKey,
    other_service: CarrierPathKey,
    _service_receivers: ReliablePathCommandReceivers,
    target_receivers: ReliablePathCommandReceivers,
    _other_service_receivers: ReliablePathCommandReceivers,
}

fn response_service_handoff_drain_fixture() -> ResponseServiceHandoffDrainFixture {
    response_service_handoff_drain_fixture_with_other_service(UnderlayProtocol::Tcp)
}

fn response_service_handoff_drain_fixture_with_other_service(
    other_underlay: UnderlayProtocol,
) -> ResponseServiceHandoffDrainFixture {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let session_id = SessionId(192);
    let tracker = Arc::new(ServerPathLaneTracker::default());
    let service = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let target = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let other_service = CarrierPathKey {
        underlay: other_underlay,
        path_id: if other_underlay == UnderlayProtocol::Udp {
            target.path_id
        } else {
            PathId(2)
        },
    };
    let (service_commands, service_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        service.underlay,
        service.path_id,
        service_commands,
        FlowLane::Throughput,
        mux_limits,
        tracker.clone(),
    );
    let (other_service_commands, other_service_receivers) = reliable_path_command_channels(8);
    let other_binding = ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        other_service.underlay,
        other_service.path_id,
        other_service_commands,
        FlowLane::Throughput,
        mux_limits,
        tracker,
    );
    let (target_commands, mut target_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            target.underlay,
            target.path_id,
            target_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            payload_bytes,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    assert!(matches!(
        try_recv_reliable_path_command(&mut target_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
    ));
    binding.mark_output_bulk_proven_for_test(service);
    other_binding.mark_output_bulk_proven_for_test(other_service);
    let sample_bytes = reliable_subflow_startup_sample_limit_bytes(mux_limits);
    binding.update_path_metrics(
        target,
        PathMetrics {
            path_id: target.path_id,
            underlay: target.underlay,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: metric_epoch_now(),
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
        ServerPathMetricsSource::LocalSender,
    );
    let expected_family_loads = if other_underlay == UnderlayProtocol::Udp {
        ResponseServiceFamilyLoads::new(1, 1)
    } else {
        ResponseServiceFamilyLoads::new(2, 0)
    };
    assert_eq!(
        binding.response_scheduling_snapshot().service_family_loads,
        expected_family_loads
    );

    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let stream = ReliablePathStream {
        stream_id: StreamId(192),
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: service.underlay,
        max_frame_payload_bytes: payload_bytes,
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frames_rx,
    };
    let (_other_frames_tx, other_frames_rx) = mpsc::channel(1);
    let other_stream = ReliablePathStream {
        stream_id: StreamId(193),
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: other_service.underlay,
        max_frame_payload_bytes: payload_bytes,
        output: ReliablePathStreamOutput::Switchable(other_binding.clone()),
        frames: other_frames_rx,
    };
    ResponseServiceHandoffDrainFixture {
        binding,
        other_binding,
        stream,
        other_stream,
        service,
        target,
        other_service,
        _service_receivers: service_receivers,
        target_receivers,
        _other_service_receivers: other_service_receivers,
    }
}

struct ResponseCalibrationDispatchFixture {
    binding: Arc<ResponseStreamBinding>,
    stream: ReliablePathStream,
    service: CarrierPathKey,
    candidate: CarrierPathKey,
    candidate_commands: ReliablePathCommandSender,
    service_receivers: ReliablePathCommandReceivers,
    candidate_receivers: ReliablePathCommandReceivers,
    second_binding: Option<Arc<ResponseStreamBinding>>,
}

fn response_calibration_dispatch_fixture(
    candidate_capacity: usize,
) -> ResponseCalibrationDispatchFixture {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let session_id = SessionId(191);
    let tracker = Arc::new(ServerPathLaneTracker::default());
    let service = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let candidate = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (service_commands, service_receivers) = reliable_path_command_channels(8);
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
    let second_binding = ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(9),
        second_commands,
        FlowLane::Throughput,
        mux_limits,
        tracker,
    );
    let (candidate_commands, mut candidate_receivers) =
        reliable_path_command_channels(candidate_capacity);
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
    binding.mark_output_bulk_proven_for_test(service);
    binding.mark_output_bulk_proven_for_test(candidate);
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
    let stage_limit = reliable_ack_clock_calibration_limit_bytes(mux_limits);
    binding.install_tcp_ack_clock_calibration_for_test(
        candidate,
        stage_limit - 4032,
        stage_limit,
        reliable_ack_clock_calibration_ceiling_bytes(mux_limits),
        true,
    );
    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let stream = ReliablePathStream {
        stream_id: StreamId(191),
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: payload_bytes,
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frames_rx,
    };
    ResponseCalibrationDispatchFixture {
        binding,
        stream,
        service,
        candidate,
        candidate_commands,
        service_receivers,
        candidate_receivers,
        second_binding: Some(second_binding),
    }
}

fn install_slow_fresh_response_calibration(fixture: &ResponseCalibrationDispatchFixture) {
    fixture
        .binding
        .set_output_product_model_for_test(fixture.service, 47_429_000.0, 333.0);
    fixture
        .binding
        .set_output_product_model_for_test(fixture.candidate, 1_342_000.0, 891.787);
    fixture.binding.install_tcp_ack_clock_calibration_for_test(
        fixture.candidate,
        0,
        299_176,
        reliable_ack_clock_calibration_ceiling_bytes(MuxLimits::default()),
        false,
    );
}

fn response_calibration_retirement_request(
    fixture: &ResponseCalibrationDispatchFixture,
) -> ResponseAckClockCalibrationRetirementRequest {
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
    let targets = fixture
        .binding
        .sender_path_targets(FlowLane::Throughput, payload_bytes);
    let service = targets
        .iter()
        .find(|target| target.key == fixture.service)
        .expect("Service target");
    let candidate = targets
        .iter()
        .find(|target| target.key == fixture.candidate)
        .expect("calibration target");
    let (expected_planner_generation, _) = fixture.binding.subflow_state_snapshot();
    let expected_lane_generation = fixture
        .binding
        .lane_generation_and_active_response_flows()
        .0;
    ResponseAckClockCalibrationRetirementRequest {
        expected_planner_generation,
        expected_lane_generation,
        expected_model_generation: fixture.binding.response_model_generation(),
        service: service.key,
        service_incarnation: service.incarnation,
        service_pending_bytes: service.command_pending_bytes,
        target: candidate.key,
        target_incarnation: candidate.incarnation,
        target_pending_bytes: candidate.command_pending_bytes,
        limit_bytes: candidate.ack_clock_calibration_credit_limit_bytes,
    }
}

#[test]
fn repair_target_requires_active_or_bulk_rate_evidence() {
    let mut proof_only = response_target(1, UnderlayProtocol::Udp, 25.0, 0, 1_000_000, false);
    proof_only.has_sender_evidence = true;
    proof_only.has_bulk_rate_evidence = false;
    let mut unevidenced = response_target(2, UnderlayProtocol::Tcp, 5.0, 0, 1_000_000, false);
    unevidenced.has_sender_evidence = false;
    unevidenced.has_bulk_rate_evidence = false;

    assert!(
        choose_response_repair_target(
            &[proof_only, unevidenced],
            &[],
            RelaySendCause::AckGapRepair,
        )
        .is_none(),
        "RepairData is correctness traffic, not path discovery; unproven outputs must not receive repair merely because no proven target is available"
    );
}

#[test]
fn persistent_response_repair_stays_bound_to_modeled_output() {
    let modeled = response_target(1, UnderlayProtocol::Udp, 25.0, 0, 1_000_000, false);
    let alternate = response_target(2, UnderlayProtocol::Tcp, 5.0, 0, 1_000_000, false);
    let cause = RelaySendCause::persistent_server_ack_gap_repair(
        ServerRepairOutputIdentity {
            key: modeled.key,
            incarnation: modeled.incarnation,
        },
        modeled.snapshot,
        FlowLane::Throughput,
    );

    let selected = choose_response_repair_target(&[modeled.clone(), alternate.clone()], &[], cause)
        .expect("modeled output remains eligible");
    assert_eq!(selected.key, modeled.key);
    assert!(
        choose_response_repair_target(&[alternate], &[], cause).is_none(),
        "a queued BDP repair must pause instead of switching to a differently modeled output"
    );
    let mut replacement = modeled;
    replacement.incarnation = replacement.incarnation.saturating_add(1);
    assert!(
        choose_response_repair_target(&[replacement], &[], cause).is_none(),
        "a same-key replacement must not inherit a batch sized from the old output incarnation"
    );
}

#[test]
fn response_owner_data_waits_for_missing_lower_owner_debt() {
    let frame = Frame::StreamData {
        stream_id: StreamId(82),
        offset: 0,
        flags: StreamFlags::NONE,
        payload: Bytes::from_static(b"owner"),
    };
    let survivor = response_target(1, UnderlayProtocol::Udp, 10.0, 0, 1_000_000, false);
    let lower_flights = [
        CarrierPathFlightDebt {
            key: survivor.key,
            bytes: 64,
        },
        CarrierPathFlightDebt {
            key: CarrierPathKey {
                underlay: UnderlayProtocol::Tcp,
                path_id: PathId(9),
            },
            bytes: 64,
        },
    ];
    assert!(
        select_response_sender_data_target_with_ordered_debt_and_epoch(
            std::slice::from_ref(&survivor),
            FlowLane::Latency,
            reliable_stream_frame_accounted_bytes(&frame),
            MuxLimits::default(),
            &lower_flights,
            None,
            128,
            None,
        )
        .is_none(),
        "a sole survivor must not receive later OwnerData while a missing lower owner still has debt"
    );
    assert!(
        select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[survivor],
            FlowLane::Latency,
            reliable_stream_frame_accounted_bytes(&frame),
            MuxLimits::default(),
            &[],
            None,
            0,
            None,
        )
        .is_some()
    );
}

#[test]
fn repair_target_does_not_fallback_to_avoided_owner_path() {
    let owner = response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 1_000_000, true);
    let mut proof_only = response_target(2, UnderlayProtocol::Udp, 25.0, 0, 1_000_000, false);
    proof_only.has_sender_evidence = true;
    proof_only.has_bulk_rate_evidence = false;

    assert!(
        choose_response_repair_target(
            &[owner.clone(), proof_only],
            &[owner.key],
            RelaySendCause::AckGapRepair,
        )
        .is_none(),
        "RepairData must not retransmit an already-owned range on the same Service path when no distinct proven repair output exists"
    );
}

#[test]
fn path_failure_repair_may_retry_stale_copy_when_all_outputs_are_avoided() {
    let owner = response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 1_000_000, true);
    let backup = response_target(2, UnderlayProtocol::Udp, 25.0, 0, 1_000_000, false);

    let selected = choose_response_repair_target(
        &[owner.clone(), backup.clone()],
        &[owner.key, backup.key],
        RelaySendCause::PathFailureRepair,
    )
    .expect("path-failure recovery may retry on a stale live output");

    assert_eq!(
        selected.key, owner.key,
        "PathFailureRepair should fall back by metrics when every live output already has a stale copy; this must not be available to ordinary AckGapRepair"
    );
    assert!(
        choose_response_repair_target(
            &[owner.clone(), backup.clone()],
            &[selected.key],
            RelaySendCause::AckGapRepair,
        )
        .is_some(),
        "ordinary ACK-gap repair still uses a distinct available output when one exists"
    );
    assert!(
        choose_response_repair_target(
            &[owner.clone(), backup.clone()],
            &[owner.key, backup.key],
            RelaySendCause::AckGapRepair,
        )
        .is_none(),
        "ordinary ACK-gap repair must not retry an already-owned or already-repaired range when every output is avoided"
    );
}

#[test]
fn response_lead_must_be_admissible_not_lowest_raw_eta() {
    let mux_limits = MuxLimits::default();
    let mut saturated_low_eta =
        response_target(0, UnderlayProtocol::Udp, 1.0, 512 * 1024, 512 * 1024, true);
    saturated_low_eta.snapshot.product_bytes_in_flight = mux_limits.max_path_flight_bytes as u64;
    let admissible_higher_eta =
        response_target(1, UnderlayProtocol::Udp, 2.0, 0, 512 * 1024, false);
    let selected = choose_response_sender_target(
        &[saturated_low_eta, admissible_higher_eta.clone()],
        FlowLane::Throughput,
        &Frame::StreamData {
            stream_id: StreamId(7),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![0; 64 * 1024]),
        },
        CarrierEmitMode::Classified,
        mux_limits,
        &[],
        &[],
        None,
    )
    .expect("admissible higher ETA path should lead");

    assert_eq!(selected.key, admissible_higher_eta.key);
}

#[test]
fn response_stream_ordered_final_control_stays_on_active_lead() {
    let active_data_owner = response_target(0, UnderlayProtocol::Tcp, 50.0, 0, 512 * 1024, true);
    let validation_lower_eta = response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 512 * 1024, false);

    let selected = choose_response_sender_target(
        &[active_data_owner.clone(), validation_lower_eta],
        FlowLane::Throughput,
        &Frame::StreamFin {
            stream_id: StreamId(7),
            final_offset: 2 * 1024 * 1024,
        },
        CarrierEmitMode::StreamOrdered,
        MuxLimits::default(),
        &[],
        &[],
        None,
    )
    .expect("stream-ordered final control should remain dispatchable");

    assert_eq!(
        selected.key, active_data_owner.key,
        "FIN/final-offset must not move to a validation path and overtake older data"
    );
}

#[test]
fn response_stream_ack_prefers_request_active_over_response_owner() {
    let mut request_active = response_target(1, UnderlayProtocol::Tcp, 50.0, 0, 512 * 1024, false);
    request_active.is_request_active = true;
    let mut response_owner = response_target(0, UnderlayProtocol::Udp, 5.0, 0, 512 * 1024, true);
    response_owner.is_request_active = false;
    let selected = choose_response_sender_target(
        &[response_owner, request_active.clone()],
        FlowLane::Control,
        &Frame::StreamAck {
            stream_id: StreamId(7),
            complete: true,
            ranges: vec![OffsetRange { start: 0, end: 64 }],
        },
        CarrierEmitMode::Classified,
        MuxLimits::default(),
        &[],
        &[],
        None,
    )
    .expect("request Active ACK carrier should remain dispatchable");

    assert_eq!(selected.key, request_active.key);
}

#[test]
fn response_stream_ordered_final_control_waits_for_backpressured_active_lead() {
    let (active_commands, _active_receivers) = reliable_path_command_channels(1);
    active_commands
        .try_enqueue_admitted_frame(
            Frame::StreamData {
                stream_id: StreamId(7),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"queued"),
            },
            FlowLane::Throughput,
        )
        .expect("fill active data queue");
    let mut active_data_owner =
        response_target(0, UnderlayProtocol::Tcp, 50.0, 0, 512 * 1024, true);
    active_data_owner.commands = active_commands;
    let validation_lower_eta = response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 512 * 1024, false);

    let selected = choose_response_sender_target(
        &[active_data_owner, validation_lower_eta],
        FlowLane::Throughput,
        &Frame::StreamFin {
            stream_id: StreamId(7),
            final_offset: 2 * 1024 * 1024,
        },
        CarrierEmitMode::StreamOrdered,
        MuxLimits::default(),
        &[],
        &[],
        None,
    );

    assert!(
        selected.is_none(),
        "stream-ordered FIN must wait behind older active-owner data instead of escaping to validation output"
    );
}

#[test]
fn single_active_response_target_still_obeys_bulk_admission() {
    let mux_limits = MuxLimits::default();
    let mut saturated =
        response_target(0, UnderlayProtocol::Udp, 1.0, 512 * 1024, 512 * 1024, true);
    saturated.snapshot.product_bytes_in_flight = mux_limits.max_path_flight_bytes as u64;
    let candidates = [&saturated];
    let outcome = response_target_unique_owner_admission_with_epoch(
        &saturated,
        &candidates,
        ResponseBulkLead {
            key: saturated.key,
            snapshot: saturated.snapshot,
            eta_ms: saturated.eta_ms,
        },
        None,
        Some(saturated.key),
        0,
        ResponseOrderedTail::new(Some(saturated.key), 0).for_candidate(saturated.key),
        64 * 1024,
        mux_limits,
        None,
        true,
        false,
    );
    assert_eq!(outcome.admission.decision, PathAdmissionDecision::Standby);
    assert_eq!(outcome.model_suppression, Some("inflight_limit"));

    let selected = choose_response_sender_data_target(
        &[saturated],
        FlowLane::Throughput,
        64 * 1024,
        mux_limits,
        &[],
        None,
    );

    assert!(
        selected.is_none(),
        "a temporarily single attached output must not bypass product/carrier flight admission"
    );
}

#[test]
fn response_data_admission_uses_writer_pending_bytes_not_only_slots() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 512 * 1024,
        max_repair_bytes: 512 * 1024,
        max_reorder_bytes: 512 * 1024,
        max_stream_window_bytes: 512 * 1024,
        ..MuxLimits::default()
    };
    let payload_bytes = 8 * 1024;
    let (commands, _receivers) = reliable_path_command_channels(2048);
    let mut snapshot = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 1.0, 8_000_000.0);
    snapshot.confidence = 1.0;
    let saturated = ResponseSenderPathTarget {
        #[cfg(feature = "lab-diagnostics")]
        session_id: SessionId(0),
        #[cfg(feature = "lab-diagnostics")]
        binding_instance_id: 0,
        key: CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        },
        path_instance_id: next_server_carrier_path_instance_id(),
        incarnation: 1,
        commands,
        attachment_role: StreamOpenRole::Active,
        snapshot,
        owner_data_in_flight_bytes: 0,
        command_pending_bytes: 0,
        eta_ms: 1.0,
        is_active: true,
        is_request_active: true,
        has_sender_evidence: true,
        has_service_feed_evidence: true,
        has_bulk_rate_evidence: true,
        endpoint_only_service_prior_eligible: false,
        quic_capacity_proof: None,
        quic_capacity_calibration_attempts: 0,
        ack_clock_calibration_eligible: false,
        ack_clock_calibration_proven: false,
        ack_clock_calibration_spent_bytes: 0,
        ack_clock_calibration_credit_limit_bytes: 0,
        ack_clock_calibration_max_limit_bytes: 0,
        ack_clock_calibration_active: false,
    };
    let credit = response_target_emission_credit_bytes(
        &saturated,
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
    );
    while saturated.commands.pending_bytes() + payload_bytes as u64 <= credit as u64 {
        saturated
            .commands
            .try_enqueue_admitted_frame(
                Frame::StreamData {
                    stream_id: StreamId(7),
                    offset: saturated.commands.pending_bytes(),
                    flags: StreamFlags::NONE,
                    payload: Bytes::from(vec![0; payload_bytes]),
                },
                FlowLane::Throughput,
            )
            .expect("prefill data pipe");
    }

    let admissible = response_target(1, UnderlayProtocol::Udp, 2.0, 0, 512 * 1024, false);
    let selected = choose_response_sender_data_target(
        &[saturated.clone(), admissible.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        None,
    )
    .expect("higher-ETA target with writer credit should be selected");

    assert_eq!(selected.key, admissible.key);
    assert!(
        saturated
            .commands
            .pending_bytes()
            .saturating_add(payload_bytes as u64)
            > credit as u64,
        "test must fill the low-ETA writer pipe until the next data frame would exceed byte credit"
    );
}

#[test]
fn quic_proof_success_path_gets_bounded_bulk_only_startup_sampling() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut active = response_target(
        0,
        UnderlayProtocol::Udp,
        1.0,
        0,
        4 * payload_bytes as u64,
        true,
    );
    active.snapshot.active_flows = 2;
    let mut proof_success = response_target(1, UnderlayProtocol::Udp, 500.0, 0, 0, false);
    proof_success.snapshot.delivery_rate_bps = default_path_rate_bps(UnderlayProtocol::Udp);
    proof_success.snapshot.pacing_rate_bps = proof_success.snapshot.delivery_rate_bps;
    proof_success.snapshot.app_limited = true;
    proof_success.snapshot.confidence = 1.0;
    proof_success.has_bulk_rate_evidence = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[active.clone(), proof_success.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(active.key),
        0,
        None,
    )
    .expect("QUIC Validation sampling should be dispatchable");

    assert_eq!(selected.target.key, proof_success.key);
    assert_eq!(selected.admission.role, PathRuntimeRole::Subflow);
    assert!(
        selected
            .subflow_set_commit
            .is_some_and(|commit| commit.input.startup_owner_allowed)
    );
}

#[test]
fn proof_path_owner_sampling_is_explicit_subflow_not_service_migration() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut active = response_target(
        0,
        UnderlayProtocol::Udp,
        1.0,
        0,
        4 * payload_bytes as u64,
        true,
    );
    active.snapshot.active_flows = 2;
    let mut proof_success = response_target(1, UnderlayProtocol::Udp, 500.0, 0, 0, false);
    proof_success.has_sender_evidence = true;
    proof_success.has_bulk_rate_evidence = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[active.clone(), proof_success],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(active.key),
        0,
        None,
    )
    .expect("bounded startup sampling should be dispatchable");

    assert_ne!(selected.target.key, active.key);
    assert_eq!(selected.admission.role, PathRuntimeRole::Subflow);
    assert!(
        selected
            .subflow_set_commit
            .is_some_and(|commit| commit.input.startup_owner_allowed)
    );
}

#[test]
fn measured_udp_bulk_path_remains_overflow_behind_feedable_udp_service() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let active_udp = response_target(
        0,
        UnderlayProtocol::Udp,
        150.0,
        0,
        4 * payload_bytes as u64,
        true,
    );
    let measured_udp = response_target(
        1,
        UnderlayProtocol::Udp,
        10.0,
        0,
        4 * payload_bytes as u64,
        false,
    );

    let selected = choose_response_sender_data_target(
        &[active_udp.clone(), measured_udp],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        }),
    )
    .expect("the feedable UDP Service should remain eligible for ordinary bulk");

    assert_eq!(
        selected.key, active_udp.key,
        "a measured same-family Subflow is additive overflow and must not displace feedable Service"
    );
}

#[test]
fn measured_udp_bulk_path_does_not_steal_tcp_owner_under_lower_debt() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let active_tcp = response_target(
        0,
        UnderlayProtocol::Tcp,
        150.0,
        0,
        4 * payload_bytes as u64,
        true,
    );
    let measured_udp = response_target(
        1,
        UnderlayProtocol::Udp,
        10.0,
        0,
        4 * payload_bytes as u64,
        false,
    );

    let selected = choose_response_sender_data_target(
        &[active_tcp.clone(), measured_udp],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[CarrierPathFlightDebt {
            key: active_tcp.key,
            bytes: payload_bytes as u64,
        }],
        Some(active_tcp.key),
    )
    .expect("current TCP primary remains eligible while it owns unresolved lower bytes");

    assert_eq!(
        selected.key, active_tcp.key,
        "mixed TCP/QUIC paths may probe or repair, but must not steal same-stream OwnerData under lower-owner debt"
    );
}

#[test]
fn measured_udp_alternate_does_not_replace_active_service_at_clear_frontier() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut active_unproven_udp = response_target(
        0,
        UnderlayProtocol::Udp,
        5.0,
        0,
        4 * payload_bytes as u64,
        true,
    );
    active_unproven_udp.has_bulk_rate_evidence = false;
    let measured_udp = response_target(
        1,
        UnderlayProtocol::Udp,
        10.0,
        0,
        4 * payload_bytes as u64,
        false,
    );

    let selected = choose_response_sender_data_target(
        &[active_unproven_udp, measured_udp.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        }),
    )
    .expect("bulk-rate-proven UDP owner should be eligible at a clear frontier");

    assert_eq!(
        selected.key,
        CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        },
        "a measured alternate must not steal Service ownership merely by existing"
    );
}

#[test]
fn clear_frontier_without_live_service_elects_liveness_service_failover() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut restart = response_target(
        1,
        UnderlayProtocol::Tcp,
        5.0,
        0,
        4 * payload_bytes as u64,
        false,
    );
    restart.has_sender_evidence = false;
    restart.has_bulk_rate_evidence = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[restart.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        None,
        0,
        None,
    );

    let selected = selected.expect(
        "when the previous Service is gone and the ordered frontier is clear, the stream must elect a new Service failover path",
    );
    assert_eq!(
        selected.target.key, restart.key,
        "liveness from an attached output is enough for bounded Service failover only when no live Service owner remains"
    );
    assert_eq!(
        selected.admission.role,
        PathRuntimeRole::Service,
        "failover owner bytes are Service OwnerData, not optional Subflow exploration"
    );
}

#[test]
fn repair_attachment_cannot_suppress_liveness_service_failover() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut repair = response_target(
        0,
        UnderlayProtocol::Tcp,
        1.0,
        0,
        4 * payload_bytes as u64,
        false,
    );
    repair.attachment_role = StreamOpenRole::Repair;
    let mut validation = response_target(
        1,
        UnderlayProtocol::Tcp,
        50.0,
        0,
        4 * payload_bytes as u64,
        false,
    );
    validation.has_sender_evidence = false;
    validation.has_bulk_rate_evidence = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[repair, validation.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        None,
        0,
        None,
    )
    .expect("Repair output must not hide an eligible liveness Service survivor");

    assert_eq!(selected.target.key, validation.key);
    assert_eq!(selected.admission.role, PathRuntimeRole::Service);
}

#[test]
fn unproven_liveness_service_failover_respects_startup_assigned_credit() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut failover = response_target(
        1,
        UnderlayProtocol::Tcp,
        5.0,
        0,
        4 * payload_bytes as u64,
        false,
    );
    let startup_credit = response_service_startup_emission_credit_bytes(
        failover.key.underlay,
        payload_bytes,
        mux_limits,
    );
    failover.has_service_feed_evidence = false;
    failover.has_bulk_rate_evidence = false;
    failover.snapshot.product_bytes_in_flight = startup_credit.saturating_sub(payload_bytes) as u64;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[failover.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        None,
        0,
        None,
    )
    .expect("a prospective Service with startup credit remaining stays feedable");
    assert_eq!(selected.admission.role, PathRuntimeRole::Service);

    failover.snapshot.product_bytes_in_flight = startup_credit as u64;
    assert!(
        select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[failover],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            None,
            0,
            None,
        )
        .is_none(),
        "newly elected unproven Service must not exceed the cumulative startup horizon before becoming active"
    );
}

#[test]
fn prospective_service_uses_service_credit_instead_of_optional_pipe_credit() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let (commands, _receivers) = reliable_path_command_channels(128);
    let mut failover = response_target(
        1,
        UnderlayProtocol::Tcp,
        5.0,
        0,
        payload_bytes as u64,
        false,
    );
    failover.commands = commands;
    failover.has_bulk_rate_evidence = false;
    failover.snapshot.delivery_rate_bps = 1.0;
    failover.snapshot.pacing_rate_bps = 1.0;
    let optional_credit = response_target_emission_credit_bytes(
        &failover,
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
    );
    let service_credit =
        response_service_emission_credit_bytes(&failover, payload_bytes, mux_limits);
    assert!(
        optional_credit < service_credit,
        "fixture requires optional-path credit below prospective Service credit"
    );
    while failover
        .commands
        .pending_bytes()
        .saturating_add(payload_bytes as u64)
        <= optional_credit as u64
    {
        failover
            .commands
            .try_enqueue_admitted_frame(
                Frame::StreamData {
                    stream_id: StreamId(74),
                    offset: failover.commands.pending_bytes(),
                    flags: StreamFlags::NONE,
                    payload: Bytes::from(vec![0; payload_bytes]),
                },
                FlowLane::Throughput,
            )
            .expect("prefill prospective Service without exhausting queue slots");
    }
    assert!(
        failover.commands.can_enqueue_lane_now(FlowLane::Throughput),
        "fixture must retain a real writer queue slot"
    );
    assert!(
        !response_target_has_emission_credit(
            &failover,
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
        ),
        "fixture must exceed the optional-path pipe credit"
    );
    assert!(
        response_service_has_assigned_owner_credit(
            &failover,
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
        ),
        "the same assigned queue remains inside prospective Service credit"
    );

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[failover],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        None,
        0,
        None,
    )
    .expect("pre-role optional-path credit must not suppress Service failover");
    assert_eq!(selected.admission.role, PathRuntimeRole::Service);
}

#[test]
fn mature_liveness_service_failover_uses_product_envelope() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut failover = response_target(
        1,
        UnderlayProtocol::Tcp,
        5.0,
        0,
        4 * payload_bytes as u64,
        false,
    );
    let mature_credit =
        response_service_emission_credit_bytes(&failover, payload_bytes, mux_limits);
    let full_envelope = usize::try_from(bulk_active_service_product_envelope_bytes(
        failover.snapshot,
        payload_bytes,
        mux_limits,
    ))
    .unwrap();
    assert!(
        mature_credit
            > response_service_startup_emission_credit_bytes(
                failover.key.underlay,
                payload_bytes,
                mux_limits,
            ),
        "fixture requires a mature product envelope larger than startup credit"
    );
    assert_eq!(mature_credit, full_envelope);
    failover.snapshot.product_bytes_in_flight = mature_credit.saturating_sub(payload_bytes) as u64;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[failover.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        None,
        0,
        None,
    )
    .expect("bulk-rate-proven prospective Service may use the product envelope");
    assert_eq!(selected.admission.role, PathRuntimeRole::Service);

    failover.snapshot.product_bytes_in_flight = mature_credit as u64;
    assert!(
        select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[failover],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            None,
            0,
            None,
        )
        .is_none(),
        "mature Service failover must stop at the product envelope"
    );
}

#[test]
fn mixed_family_clear_frontier_service_failover_is_metric_first() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut tcp = response_target(
        1,
        UnderlayProtocol::Tcp,
        50.0,
        0,
        4 * payload_bytes as u64,
        false,
    );
    tcp.has_sender_evidence = true;
    tcp.has_bulk_rate_evidence = false;
    let mut udp = response_target(
        0,
        UnderlayProtocol::Udp,
        5.0,
        0,
        4 * payload_bytes as u64,
        false,
    );
    udp.has_sender_evidence = true;
    udp.has_bulk_rate_evidence = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[tcp, udp.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        None,
        0,
        None,
    );

    let selected = selected
        .expect("Service failover must be carrier-neutral when no live ordered owner remains");
    assert_eq!(
        selected.target.key, udp.key,
        "clear-frontier Service failover is selected by path metrics, not by TCP/UDP family"
    );
    assert_eq!(
        selected.admission.role,
        PathRuntimeRole::Service,
        "the elected failover path becomes the new Service owner"
    );
}

#[test]
fn clear_frontier_stale_owner_without_lane_capacity_elects_liveness_service_failover() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut stale_owner =
        response_target(1, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, true);
    let (owner_commands, _owner_rx) = reliable_path_command_channels(1);
    owner_commands
        .try_enqueue_admitted_frame(
            Frame::StreamData {
                stream_id: StreamId(99),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"queued"),
            },
            FlowLane::Throughput,
        )
        .expect("seed full owner data queue");
    stale_owner.commands = owner_commands;
    let mut failover = response_target(
        0,
        UnderlayProtocol::Udp,
        5.0,
        0,
        4 * payload_bytes as u64,
        false,
    );
    failover.has_sender_evidence = true;
    failover.has_bulk_rate_evidence = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[stale_owner.clone(), failover.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(stale_owner.key),
        0,
        None,
    );

    let selected = selected.expect(
        "when the ordered frontier is clear and the old Service cannot enqueue, a validated survivor must become Service failover",
    );
    assert_eq!(
        selected.target.key, failover.key,
        "clear-frontier failover is metric-first and must not be trapped by the stale owner's carrier family"
    );
    assert_eq!(selected.admission.role, PathRuntimeRole::Service);
}

#[test]
fn liveness_service_failover_waits_behind_live_owner_tail_guard() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut failover = response_target(
        1,
        UnderlayProtocol::Udp,
        5.0,
        0,
        4 * payload_bytes as u64,
        false,
    );
    failover.has_sender_evidence = true;
    failover.has_bulk_rate_evidence = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[failover],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        None,
        payload_bytes,
        None,
    );

    assert!(
        selected.is_none(),
        "liveness Service failover can only own future bytes after the live lower owner frontier is clear"
    );
}

#[test]
fn repair_prefers_bulk_proven_path_over_proof_only_low_eta_path() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let original_owner = response_target(
        0,
        UnderlayProtocol::Tcp,
        20.0,
        0,
        4 * payload_bytes as u64,
        true,
    );
    let proven_alternate = response_target(
        1,
        UnderlayProtocol::Tcp,
        50.0,
        0,
        4 * payload_bytes as u64,
        false,
    );
    let mut proof_only_udp = response_target(
        2,
        UnderlayProtocol::Udp,
        5.0,
        0,
        4 * payload_bytes as u64,
        false,
    );
    proof_only_udp.has_bulk_rate_evidence = false;

    let selected = choose_response_sender_target(
        &[
            original_owner.clone(),
            proven_alternate.clone(),
            proof_only_udp,
        ],
        FlowLane::Latency,
        &Frame::StreamData {
            stream_id: StreamId(7),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![0; payload_bytes]),
        },
        CarrierEmitMode::Classified,
        mux_limits,
        &[],
        &[original_owner.key],
        Some(RelaySendCause::AckGapRepair),
    )
    .expect("repair should remain dispatchable on the proven alternate");

    assert_eq!(
        selected.key, proven_alternate.key,
        "repair must not treat proof-only validation as bulk-capable just because it has lower ETA"
    );
}

#[test]
fn repair_does_not_use_proof_only_path_when_no_proven_repair_path_exists() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let original_owner = response_target(
        0,
        UnderlayProtocol::Tcp,
        20.0,
        0,
        4 * payload_bytes as u64,
        true,
    );
    let mut proof_only_udp = response_target(
        1,
        UnderlayProtocol::Udp,
        5.0,
        0,
        4 * payload_bytes as u64,
        false,
    );
    proof_only_udp.has_bulk_rate_evidence = false;

    let selected = choose_response_sender_target(
        &[original_owner.clone(), proof_only_udp.clone()],
        FlowLane::Latency,
        &Frame::StreamData {
            stream_id: StreamId(7),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![0; payload_bytes]),
        },
        CarrierEmitMode::Classified,
        mux_limits,
        &[],
        &[original_owner.key],
        Some(RelaySendCause::AckGapRepair),
    );

    assert!(
        selected.is_none(),
        "RepairData must wait for an active or bulk-rate-proven alternate instead of turning proof-only validation into a repair path"
    );
}

#[test]
fn path_failure_repair_can_use_live_liveness_survivor_without_path_proving_it() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let original_owner = response_target(
        0,
        UnderlayProtocol::Tcp,
        20.0,
        0,
        4 * payload_bytes as u64,
        true,
    );
    let mut liveness_survivor = response_target(
        1,
        UnderlayProtocol::Udp,
        5.0,
        0,
        4 * payload_bytes as u64,
        false,
    );
    liveness_survivor.has_sender_evidence = true;
    liveness_survivor.has_bulk_rate_evidence = false;

    let selected = choose_response_sender_target(
        &[original_owner.clone(), liveness_survivor.clone()],
        FlowLane::Latency,
        &Frame::StreamData {
            stream_id: StreamId(7),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![0; payload_bytes]),
        },
        CarrierEmitMode::Classified,
        mux_limits,
        &[],
        &[original_owner.key],
        Some(RelaySendCause::PathFailureRepair),
    )
    .expect("path-failure repair must be able to recover on a live non-owner output");

    assert_eq!(
        selected.key, liveness_survivor.key,
        "PathFailureRepair is bounded failover retransmission; it must not require bulk-rate proof because it never path-proves or changes Service ownership"
    );
}

#[test]
fn path_failure_repair_prefers_same_family_survivor_before_cross_family_low_eta() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let original_owner = response_target(
        0,
        UnderlayProtocol::Tcp,
        20.0,
        0,
        4 * payload_bytes as u64,
        true,
    );
    let mut same_family_survivor = response_target(
        1,
        UnderlayProtocol::Tcp,
        50.0,
        0,
        4 * payload_bytes as u64,
        false,
    );
    same_family_survivor.has_sender_evidence = true;
    same_family_survivor.has_bulk_rate_evidence = false;
    let mut cross_family_low_eta = response_target(
        2,
        UnderlayProtocol::Udp,
        5.0,
        0,
        4 * payload_bytes as u64,
        false,
    );
    cross_family_low_eta.has_sender_evidence = true;
    cross_family_low_eta.has_bulk_rate_evidence = false;

    let selected = choose_response_sender_target(
        &[
            original_owner.clone(),
            same_family_survivor.clone(),
            cross_family_low_eta,
        ],
        FlowLane::Throughput,
        &Frame::StreamData {
            stream_id: StreamId(7),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![0; payload_bytes]),
        },
        CarrierEmitMode::Classified,
        mux_limits,
        &[],
        &[original_owner.key],
        Some(RelaySendCause::PathFailureRepair),
    )
    .expect("path-failure repair should remain dispatchable on a live survivor");

    assert_eq!(
        selected.key, same_family_survivor.key,
        "failed-owner RepairData should follow the same-family failover survivor before trying cross-family low-ETA repair"
    );
}

#[test]
fn path_failure_repair_bypasses_stale_owner_emission_credit_but_not_queue_capacity() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 64 * 1024,
        max_repair_bytes: 64 * 1024,
        max_reorder_bytes: 64 * 1024,
        max_stream_window_bytes: 64 * 1024,
        ..MuxLimits::default()
    };
    let payload_bytes = 8 * 1024;
    let (commands, _receivers) = reliable_path_command_channels(64);
    let mut survivor = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024, false);
    survivor.commands = commands.clone();
    survivor.has_sender_evidence = true;
    survivor.has_bulk_rate_evidence = false;

    let credit = response_target_emission_credit_bytes(
        &survivor,
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
    );
    while commands
        .pending_bytes()
        .saturating_add(payload_bytes as u64)
        <= credit as u64
    {
        commands
            .try_enqueue_admitted_frame(
                Frame::StreamData {
                    stream_id: StreamId(72),
                    offset: commands.pending_bytes(),
                    flags: StreamFlags::NONE,
                    payload: Bytes::from(vec![0; payload_bytes]),
                },
                FlowLane::Throughput,
            )
            .expect("prefill survivor data queue without exhausting slots");
    }

    let repair_frame = Frame::StreamData {
        stream_id: StreamId(72),
        offset: 1024,
        flags: StreamFlags::NONE,
        payload: Bytes::from(vec![7_u8; payload_bytes]),
    };
    assert!(
        survivor
            .commands
            .can_enqueue_frame_now(&repair_frame, FlowLane::Throughput),
        "test setup must leave a real queue slot for failover RepairData"
    );
    assert!(
        !response_target_has_emission_credit(
            &survivor,
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
        ),
        "test setup must exceed ordinary owner emission credit"
    );

    let selected = choose_response_sender_target(
        &[survivor.clone()],
        FlowLane::Throughput,
        &repair_frame,
        CarrierEmitMode::Classified,
        mux_limits,
        &[],
        &[],
        Some(RelaySendCause::PathFailureRepair),
    )
    .expect("path-failure RepairData must be admitted while a live queue slot exists");

    assert_eq!(
        selected.key, survivor.key,
        "failed-owner repair is bounded correctness traffic and must not be blocked by stale owner emission credit"
    );
}

#[test]
fn path_failure_repair_stream_data_uses_data_queue_when_priority_is_full() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let stream_id = StreamId(71);
    let repair_frame = Frame::StreamData {
        stream_id,
        offset: 1024,
        flags: StreamFlags::NONE,
        payload: Bytes::from(vec![7_u8; payload_bytes]),
    };
    let active_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let (active_commands, _active_rx) = reliable_path_command_channels(1);
    active_commands
        .try_enqueue_admitted_frame(
            Frame::StreamAck {
                stream_id,
                complete: false,
                ranges: Vec::new(),
            },
            FlowLane::Control,
        )
        .expect("fill active priority queue");
    let binding = ResponseStreamBinding::new_with_limits(
        SessionId(71),
        active_key.underlay,
        active_key.path_id,
        active_commands,
        FlowLane::Throughput,
        mux_limits,
    );
    binding.record_owner_flight(active_key, &repair_frame);

    let (survivor_commands, _survivor_rx) = reliable_path_command_channels(1);
    assert_eq!(
        binding.attach(
            UnderlayProtocol::Udp,
            PathId(1),
            survivor_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            payload_bytes,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: payload_bytes,
        output: ReliablePathStreamOutput::Switchable(binding),
        frames: frames_rx,
    };

    assert!(
        response_frame_has_carrier_credit(
            &path_stream,
            &repair_frame,
            FlowLane::Latency,
            CarrierEmitMode::Classified,
            Some(RelaySendCause::PathFailureRepair),
        ),
        "RepairData is product-critical stream data: carrier priority queues may be full, but an open stream-data queue must still admit failover repair"
    );
}

#[test]
fn quic_ack_data_seen_path_does_not_own_unique_data_without_bulk_rate_proof() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let (active_commands, _active_rx) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits(
        SessionId(81),
        UnderlayProtocol::Udp,
        PathId(0),
        active_commands,
        FlowLane::Throughput,
        mux_limits,
    );
    let validation_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (validation_commands, _validation_rx) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            validation_key.underlay,
            validation_key.path_id,
            validation_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            payload_bytes,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    binding.update_path_metrics(
        validation_key,
        PathMetrics {
            path_id: validation_key.path_id,
            underlay: validation_key.underlay,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: metric_epoch_now(),
            metric_age_us: 0,
            min_rtt_us: 5_000,
            srtt_us: 5_000,
            rttvar_us: 500,
            jitter_us: 500,
            delivery_rate_bps: 1_000_000,
            pacing_rate_bps: 1_000_000,
            loss_ppm: 0,
            ecn_ppm: 0,
            loss_observed: false,
            ecn_observed: false,
            bytes_in_flight: 0,
            queue_bytes: 0,
            inflight_limit_bytes: payload_bytes as u64,
            inflight_hi_bytes: payload_bytes as u64,
            confidence_ppm: 1,
            app_limited: true,
            has_ack_derived_data_sample: true,
            data_sample_count: 0,
            data_sample_bytes: 0,
        },
        ServerPathMetricsSource::LocalSender,
    );
    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let stream = ReliablePathStream {
        stream_id: StreamId(7),
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Udp,
        max_frame_payload_bytes: payload_bytes,
        output: ReliablePathStreamOutput::Switchable(binding),
        frames: frames_rx,
    };

    let plan = plan_response_data_dispatch(&stream, FlowLane::Throughput, 0, payload_bytes)
        .expect("active owner should remain dispatchable");

    assert_eq!(
        plan.primary_key(),
        Some(CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        }),
        "ACK-data evidence cannot create Subflow OwnerData before the candidate has bulk-rate evidence"
    );
    assert_eq!(
        plan.primary_role(),
        PathRuntimeRole::Service,
        "ACK-data-only paths must not become Service owners"
    );
}

#[test]
fn ack_data_only_udp_path_cannot_own_unique_data_when_lower_owner_exists() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let active_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    let mut active = response_target(
        0,
        UnderlayProtocol::Udp,
        50.0,
        payload_bytes as u64,
        4 * payload_bytes as u64,
        true,
    );
    active.has_bulk_rate_evidence = false;
    let mut ack_data_only_path = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 0, false);
    ack_data_only_path.has_bulk_rate_evidence = false;

    let selected = choose_response_sender_data_target(
        &[active.clone(), ack_data_only_path.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[CarrierPathFlightDebt {
            key: active_key,
            bytes: PATH_OPEN_SCORE_BYTES as u64,
        }],
        Some(active_key),
    )
    .expect("active owner should remain admissible while lower bytes are unresolved");

    assert_eq!(
        selected.key, active.key,
        "ACK-data-only QUIC paths must not own later ordered bytes while another path owns unresolved lower bytes"
    );
}

#[test]
fn ack_data_quic_path_does_not_preempt_service_owner_under_lower_debt() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let active_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    let mut active = response_target(
        0,
        UnderlayProtocol::Udp,
        5.0,
        payload_bytes as u64,
        16 * payload_bytes as u64,
        true,
    );
    active.has_bulk_rate_evidence = true;
    let mut ack_data_only_path = response_target(1, UnderlayProtocol::Udp, 500.0, 0, 0, false);
    ack_data_only_path.has_bulk_rate_evidence = false;
    ack_data_only_path.snapshot.delivery_rate_bps = default_path_rate_bps(UnderlayProtocol::Udp);
    ack_data_only_path.snapshot.pacing_rate_bps = ack_data_only_path.snapshot.delivery_rate_bps;
    ack_data_only_path.snapshot.app_limited = true;

    let selected = choose_response_sender_data_target(
        &[active.clone(), ack_data_only_path.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[CarrierPathFlightDebt {
            key: active_key,
            bytes: PATH_OPEN_SCORE_BYTES as u64,
        }],
        Some(active_key),
    )
    .expect("active owner should remain selected while it owns the lower frontier");

    assert_eq!(
        selected.key, active.key,
        "ACK-data-only paths must not preempt the service owner while lower-owner debt exists"
    );
}

#[test]
fn quic_ack_data_seen_validation_path_bootstraps_as_bounded_subflow() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut active = response_target(
        0,
        UnderlayProtocol::Udp,
        50.0,
        0,
        4 * payload_bytes as u64,
        true,
    );
    active.has_bulk_rate_evidence = true;
    active.snapshot.active_flows = 2;
    let mut ack_data_only = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 0, false);
    ack_data_only.has_bulk_rate_evidence = false;
    ack_data_only.has_sender_evidence = true;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[active.clone(), ack_data_only.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        }),
        0,
        None,
    )
    .expect("bulk-rate-proven Service should remain dispatchable");

    assert_eq!(
        selected.target.key, ack_data_only.key,
        "sender-evidenced same-family Validation may consume bounded startup sampling credit"
    );
    assert_eq!(
        selected.admission.role,
        PathRuntimeRole::Subflow,
        "startup sampling must not migrate the Service owner"
    );
    assert!(
        selected
            .subflow_set_commit
            .is_some_and(|commit| commit.input.startup_owner_allowed)
    );
}

#[test]
fn measured_same_family_subflow_is_not_throttled_by_startup_credit() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let active_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    let mut active = response_target(
        0,
        UnderlayProtocol::Udp,
        50.0,
        0,
        16 * payload_bytes as u64,
        true,
    );
    active.has_bulk_rate_evidence = true;
    let service_envelope =
        bulk_active_service_product_envelope_bytes(active.snapshot, payload_bytes, mux_limits);
    active.snapshot.product_bytes_in_flight = service_envelope;
    active.snapshot.queue_bytes = payload_bytes as u64;
    let mut bulk_rate_subflow = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 0, false);
    bulk_rate_subflow.has_sender_evidence = true;
    bulk_rate_subflow.has_bulk_rate_evidence = true;

    let first = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[active.clone(), bulk_rate_subflow.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(active_key),
        0,
        None,
    )
    .expect("first measured Subflow frame should be admitted");
    let commit = first
        .subflow_set_commit
        .expect("measured Subflow admission should carry commit state");
    assert_eq!(first.admission.role, PathRuntimeRole::Subflow);
    assert_eq!(
        commit.startup_owner_credit_bytes,
        usize::try_from(reliable_subflow_startup_sample_limit_bytes(mux_limits)).unwrap(),
        "the Subflow ledger keeps one stable startup sampling envelope across all decisions"
    );

    let mut subflow_set = FlowSubflowSet::new(
        0,
        commit.service,
        commit.startup_owner_credit_bytes,
        commit.optional_overhead_budget_bytes,
        commit.max_read_gap_budget,
    );
    assert_eq!(
        subflow_set.admit_subflow_owner(commit.input).decision,
        PathAdmissionDecision::AdmitSubflow
    );

    let second = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[active.clone(), bulk_rate_subflow.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(active_key),
        0,
        Some(&subflow_set),
    )
    .expect("measured Subflow should remain eligible if per-decision no-worse gates pass");
    assert_eq!(second.target.key, bulk_rate_subflow.key);
    assert_eq!(second.admission.role, PathRuntimeRole::Subflow);
}

#[test]
fn mixed_dispatch_plan_does_not_carry_udp_product_duplicate_when_primary_is_tcp() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let (active_commands, _active_rx) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits(
        SessionId(79),
        UnderlayProtocol::Tcp,
        PathId(0),
        active_commands,
        FlowLane::Throughput,
        mux_limits,
    );
    binding.update_path_metrics(
        CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        },
        PathMetrics {
            path_id: PathId(0),
            underlay: UnderlayProtocol::Tcp,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: metric_epoch_now(),
            metric_age_us: 0,
            min_rtt_us: 50_000,
            srtt_us: 50_000,
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
            inflight_limit_bytes: payload_bytes as u64,
            inflight_hi_bytes: payload_bytes as u64,
            confidence_ppm: 1_000_000,
            app_limited: false,
            has_ack_derived_data_sample: true,
            data_sample_count: 4,
            data_sample_bytes: payload_bytes as u64,
        },
        ServerPathMetricsSource::LocalSender,
    );
    let (validation_commands, _validation_rx) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            UnderlayProtocol::Udp,
            PathId(1),
            validation_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            payload_bytes,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    binding.update_path_metrics(
        CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        },
        PathMetrics {
            path_id: PathId(1),
            underlay: UnderlayProtocol::Udp,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: metric_epoch_now(),
            metric_age_us: 0,
            min_rtt_us: 20_000,
            srtt_us: 20_000,
            rttvar_us: 1_000,
            jitter_us: 1_000,
            delivery_rate_bps: 1_000_000,
            pacing_rate_bps: 1_000_000,
            loss_ppm: 0,
            ecn_ppm: 0,
            loss_observed: false,
            ecn_observed: false,
            bytes_in_flight: 0,
            queue_bytes: 0,
            inflight_limit_bytes: payload_bytes as u64,
            inflight_hi_bytes: payload_bytes as u64,
            confidence_ppm: 1_000_000,
            app_limited: true,
            has_ack_derived_data_sample: true,
            data_sample_count: 0,
            data_sample_bytes: 0,
        },
        ServerPathMetricsSource::LocalSender,
    );
    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let stream = ReliablePathStream {
        stream_id: StreamId(7),
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: payload_bytes,
        output: ReliablePathStreamOutput::Switchable(binding),
        frames: frames_rx,
    };

    let plan = plan_response_data_dispatch(&stream, FlowLane::Throughput, 0, payload_bytes)
        .expect("TCP primary remains dispatchable");

    assert_eq!(
        plan.primary_key(),
        Some(CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        })
    );
}

#[tokio::test]
async fn stale_service_plan_cannot_enqueue_owner_data_after_repair_role_change() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let active = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let validation = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (active_commands, _active_rx) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits(
        SessionId(77),
        active.underlay,
        active.path_id,
        active_commands.clone(),
        FlowLane::Throughput,
        mux_limits,
    );
    let (validation_commands, mut validation_rx) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            validation.underlay,
            validation.path_id,
            validation_commands.clone(),
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            payload_bytes,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    while try_recv_reliable_path_command(&mut validation_rx).is_some() {}
    binding.detach(active, &active_commands);
    assert_eq!(binding.ordered_data_owner(), None);

    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let stream = ReliablePathStream {
        stream_id: StreamId(77),
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: payload_bytes,
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frames_rx,
    };
    let plan = plan_response_data_dispatch(&stream, FlowLane::Throughput, 0, payload_bytes)
        .expect("liveness survivor may become the frontier-clear Service");
    assert_eq!(plan.primary_key(), Some(validation));
    assert_eq!(plan.primary_role(), PathRuntimeRole::Service);
    assert_eq!(
        binding.attach(
            validation.underlay,
            validation.path_id,
            validation_commands,
            FlowLane::Throughput,
            StreamOpenRole::Repair,
            payload_bytes,
        ),
        ResponseStreamAttachOutcome::RoleChanged
    );
    let frame = Frame::StreamData {
        stream_id: StreamId(77),
        offset: 0,
        flags: StreamFlags::NONE,
        payload: Bytes::from(vec![0x77; payload_bytes]),
    };

    assert!(matches!(
        emit_planned_response_data_frame(&stream, plan, frame, FlowLane::Throughput).await,
        Err(RuntimeError::SenderServiceBlocked)
    ));
    assert!(
        try_recv_reliable_path_command(&mut validation_rx).is_none(),
        "a stale Service plan must not enqueue STREAM_DATA on a Repair attachment"
    );
    let target = binding
        .sender_path_targets(FlowLane::Throughput, payload_bytes)
        .into_iter()
        .find(|target| target.key == validation)
        .expect("Repair output remains attached");
    assert_eq!(target.attachment_role, StreamOpenRole::Repair);
    assert_eq!(target.snapshot.product_bytes_in_flight, 0);
    assert_eq!(target.commands.pending_bytes(), 0);
    assert_eq!(binding.ordered_data_owner(), None);
}

#[tokio::test]
async fn passive_attach_preserves_one_bounded_exact_service_plan() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let (service_commands, mut service_rx) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits(
        SessionId(109),
        service.underlay,
        service.path_id,
        service_commands,
        FlowLane::Throughput,
        mux_limits,
    );
    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let stream = ReliablePathStream {
        stream_id: StreamId(109),
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: service.underlay,
        max_frame_payload_bytes: payload_bytes,
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frames_rx,
    };
    let plan = plan_response_data_dispatch(&stream, FlowLane::Throughput, 0, payload_bytes)
        .expect("live Service has a bounded owner plan");
    assert_eq!(plan.primary_key(), Some(service));
    assert_eq!(plan.primary_role(), PathRuntimeRole::Service);
    let planner_generation = binding.subflow_state_snapshot().0;

    let repair = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    let (repair_commands, mut repair_rx) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            repair.underlay,
            repair.path_id,
            repair_commands,
            FlowLane::Throughput,
            StreamOpenRole::Repair,
            payload_bytes,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    assert_ne!(binding.subflow_state_snapshot().0, planner_generation);

    let frame = Frame::StreamData {
        stream_id: StreamId(109),
        offset: 0,
        flags: StreamFlags::NONE,
        payload: Bytes::from(vec![0x6d; payload_bytes]),
    };
    let outcome =
        emit_planned_response_data_frame(&stream, plan, frame.clone(), FlowLane::Throughput)
            .await
            .expect("passive growth does not revoke the exact live Service quantum");
    assert_eq!(outcome.selected_path, Some(service));
    assert!(matches!(
        try_recv_reliable_path_command(&mut service_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
    assert!(try_recv_reliable_path_command(&mut repair_rx).is_none());
    assert_eq!(
        binding.owner_flight_keys_overlapping_frame(&frame),
        vec![service]
    );
}

#[tokio::test]
async fn quic_probe_path_does_not_receive_product_duplicate_data() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let (active_commands, mut active_rx) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits(
        SessionId(78),
        UnderlayProtocol::Udp,
        PathId(0),
        active_commands,
        FlowLane::Throughput,
        mux_limits,
    );
    let (validation_commands, mut validation_rx) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            UnderlayProtocol::Udp,
            PathId(1),
            validation_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            payload_bytes,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    while try_recv_reliable_path_command(&mut validation_rx).is_some() {}
    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let stream = ReliablePathStream {
        stream_id: StreamId(7),
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Udp,
        max_frame_payload_bytes: payload_bytes,
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frames_rx,
    };
    let plan = plan_response_data_dispatch(&stream, FlowLane::Throughput, 0, payload_bytes)
        .expect("active path should remain dispatchable");
    let frame = Frame::StreamData {
        stream_id: StreamId(7),
        offset: 0,
        flags: StreamFlags::NONE,
        payload: Bytes::from(vec![7_u8; payload_bytes]),
    };

    let outcome =
        emit_planned_response_data_frame(&stream, plan, frame.clone(), FlowLane::Throughput)
            .await
            .expect("primary data should emit");

    assert_eq!(
        outcome.selected_path,
        Some(CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        })
    );
    assert!(matches!(
        recv_reliable_path_command(&mut active_rx).await,
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
    assert!(
        try_recv_reliable_path_command(&mut validation_rx).is_none(),
        "Probe paths must not receive product STREAM_DATA"
    );
    let lower = binding.lower_flights_before_offset(payload_bytes as u64);
    assert!(
        lower.is_empty(),
        "plain unacked OwnerData stays in the flight ledger but is not ACK-hole ordering debt"
    );
}

#[tokio::test]
async fn response_owner_data_keeps_fifo_order_across_lane_changes() {
    let (commands, mut receiver) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(108),
        UnderlayProtocol::Tcp,
        PathId(0),
        commands,
        FlowLane::Throughput,
    );
    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let stream = ReliablePathStream {
        stream_id: StreamId(108),
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: 4,
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frames_rx,
    };
    let target = binding
        .sender_path_targets(FlowLane::Throughput, 4)
        .into_iter()
        .next()
        .expect("binding has service target");

    let bulk_first = ResponseDataDispatchPlan {
        primary: ResponseDataDispatchTarget::Switchable {
            binding: binding.clone(),
            target: target.clone().into(),
            role: PathRuntimeRole::Service,
            service_handoff_commit: None,
            subflow_set_commit: None,
            ack_clock_calibration_commit: None,
        },
    };
    let latency_second = ResponseDataDispatchPlan {
        primary: ResponseDataDispatchTarget::Switchable {
            binding,
            target: target.into(),
            role: PathRuntimeRole::Service,
            service_handoff_commit: None,
            subflow_set_commit: None,
            ack_clock_calibration_commit: None,
        },
    };

    emit_planned_response_data_frame(
        &stream,
        bulk_first,
        Frame::StreamData {
            stream_id: StreamId(108),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(b"aaaa"),
        },
        FlowLane::Throughput,
    )
    .await
    .expect("bulk owner data should enqueue");
    emit_planned_response_data_frame(
        &stream,
        latency_second,
        Frame::StreamData {
            stream_id: StreamId(108),
            offset: 4,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(b"bbbb"),
        },
        FlowLane::Latency,
    )
    .await
    .expect("latency owner data should enqueue");

    assert!(matches!(
        recv_reliable_path_command(&mut receiver).await,
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            offset: 0,
            ..
        }))
    ));
    assert!(matches!(
        recv_reliable_path_command(&mut receiver).await,
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            offset: 4,
            ..
        }))
    ));
}

#[test]
fn sender_evidence_same_family_candidate_cannot_own_under_lower_owner_debt() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let active_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let active = response_target(
        active_key.path_id.0,
        active_key.underlay,
        100.0,
        0,
        4 * payload_bytes as u64,
        true,
    );
    let mut proof_only = response_target(
        1,
        UnderlayProtocol::Tcp,
        5.0,
        0,
        4 * payload_bytes as u64,
        false,
    );
    proof_only.has_bulk_rate_evidence = false;
    proof_only.has_sender_evidence = true;

    let selected = choose_response_sender_data_target(
        &[active.clone(), proof_only.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[CarrierPathFlightDebt {
            key: active_key,
            bytes: payload_bytes as u64,
        }],
        Some(active_key),
    )
    .expect("service path should remain dispatchable");

    assert_eq!(
        selected.key, active.key,
        "same-family sender evidence is not enough to assign later unique bytes while the Service owns unresolved lower bytes"
    );
}

#[test]
fn bulk_rate_same_family_candidate_cannot_own_later_data_under_lower_owner_debt() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut owner = response_target(
        0,
        UnderlayProtocol::Udp,
        80.0,
        2 * 1024 * 1024,
        16 * 1024 * 1024,
        true,
    );
    owner.snapshot.product_progress_rate_bps = Some(500_000_000.0);
    let alternate = response_target(1, UnderlayProtocol::Udp, 7.0, 0, 16 * 1024 * 1024, false);
    let lower_flights = vec![CarrierPathFlightDebt {
        key: owner.key,
        bytes: 2 * 1024 * 1024,
    }];

    let selected = choose_response_sender_data_target(
        &[owner.clone(), alternate],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &lower_flights,
        Some(owner.key),
    )
    .expect("lower owner should remain dispatchable");

    assert_eq!(
        selected.key, owner.key,
        "bulk-rate evidence proves the alternate path is eligible at a clear frontier, not that it may extend an existing ordered receive hole"
    );
}

#[tokio::test]
async fn one_flow_response_bounds_app_limited_sampling_before_service_resumes() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let lane_tracker = Arc::new(ServerPathLaneTracker::default());
    let (active_commands, mut active_rx) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits_and_tracker(
        SessionId(88),
        UnderlayProtocol::Udp,
        PathId(0),
        active_commands,
        FlowLane::Throughput,
        mux_limits,
        lane_tracker,
    );
    let service = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    binding.update_path_metrics(
        service,
        PathMetrics {
            path_id: service.path_id,
            underlay: service.underlay,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: metric_epoch_now(),
            metric_age_us: 0,
            min_rtt_us: 50_000,
            srtt_us: 50_000,
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
            inflight_limit_bytes: (payload_bytes * 8) as u64,
            inflight_hi_bytes: (payload_bytes * 8) as u64,
            confidence_ppm: 1_000_000,
            app_limited: false,
            has_ack_derived_data_sample: true,
            data_sample_count: 4,
            data_sample_bytes: (payload_bytes * 8) as u64,
        },
        ServerPathMetricsSource::LocalSender,
    );
    let optional = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (optional_commands, mut optional_rx) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            optional.underlay,
            optional.path_id,
            optional_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            payload_bytes,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    binding.update_path_metrics(
        optional,
        PathMetrics {
            path_id: optional.path_id,
            underlay: optional.underlay,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: metric_epoch_now(),
            metric_age_us: 0,
            min_rtt_us: 5_000,
            srtt_us: 5_000,
            rttvar_us: 500,
            jitter_us: 500,
            delivery_rate_bps: 500_000_000,
            pacing_rate_bps: 500_000_000,
            loss_ppm: 0,
            ecn_ppm: 0,
            loss_observed: false,
            ecn_observed: false,
            bytes_in_flight: 0,
            queue_bytes: 0,
            inflight_limit_bytes: (payload_bytes * 8) as u64,
            inflight_hi_bytes: (payload_bytes * 8) as u64,
            confidence_ppm: 900_000,
            app_limited: true,
            has_ack_derived_data_sample: true,
            data_sample_count: 0,
            data_sample_bytes: 0,
        },
        ServerPathMetricsSource::LocalSender,
    );
    binding.update_path_metrics(
        optional,
        PathMetrics {
            path_id: optional.path_id,
            underlay: optional.underlay,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: metric_epoch_now(),
            metric_age_us: 0,
            min_rtt_us: 5_000,
            srtt_us: 5_000,
            rttvar_us: 500,
            jitter_us: 500,
            delivery_rate_bps: 500_000_000,
            pacing_rate_bps: 500_000_000,
            loss_ppm: 0,
            ecn_ppm: 0,
            loss_observed: false,
            ecn_observed: false,
            bytes_in_flight: 0,
            queue_bytes: 0,
            inflight_limit_bytes: (payload_bytes * 8) as u64,
            inflight_hi_bytes: (payload_bytes * 8) as u64,
            confidence_ppm: 0,
            app_limited: false,
            has_ack_derived_data_sample: false,
            data_sample_count: 0,
            data_sample_bytes: 0,
        },
        ServerPathMetricsSource::PeerHint,
    );

    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let stream = ReliablePathStream {
        stream_id: StreamId(88),
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Udp,
        max_frame_payload_bytes: payload_bytes,
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frames_rx,
    };
    let startup_limit =
        usize::try_from(reliable_subflow_startup_sample_limit_bytes(mux_limits)).unwrap();
    assert_eq!(startup_limit % payload_bytes, 0);
    for quantum in 0..(startup_limit / payload_bytes) {
        let offset = (quantum * payload_bytes) as u64;
        let plan =
            plan_response_data_dispatch(&stream, FlowLane::Throughput, offset, payload_bytes)
                .expect("bounded Validation sampling should be dispatchable");
        assert_eq!(plan.primary_key(), Some(optional));
        assert_eq!(plan.primary_role(), PathRuntimeRole::Subflow);

        let frame = Frame::StreamData {
            stream_id: StreamId(88),
            offset,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![9_u8; payload_bytes]),
        };
        let outcome = emit_planned_response_data_frame(&stream, plan, frame, FlowLane::Throughput)
            .await
            .expect("bounded startup Subflow OwnerData should emit");
        assert_eq!(outcome.selected_path, Some(optional));
        assert!(try_recv_reliable_path_command(&mut optional_rx).is_some());
        assert_eq!(
            binding.ordered_data_owner(),
            Some(service),
            "startup sampling must not migrate Service ownership"
        );
    }

    let service_offset = startup_limit as u64;
    let plan =
        plan_response_data_dispatch(&stream, FlowLane::Throughput, service_offset, payload_bytes)
            .expect("Service should resume after the startup sample cap");
    assert_eq!(plan.primary_key(), Some(service));
    assert_eq!(plan.primary_role(), PathRuntimeRole::Service);
    let frame = Frame::StreamData {
        stream_id: StreamId(88),
        offset: service_offset,
        flags: StreamFlags::NONE,
        payload: Bytes::from(vec![7_u8; payload_bytes]),
    };
    let outcome = emit_planned_response_data_frame(&stream, plan, frame, FlowLane::Throughput)
        .await
        .expect("Service OwnerData should emit after bounded sampling");
    assert_eq!(outcome.selected_path, Some(service));
    assert!(try_recv_reliable_path_command(&mut active_rx).is_some());
}

#[tokio::test]
async fn blocked_path_queue_rolls_back_unemitted_startup_credit() {
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
    let (service_commands, _service_rx) = reliable_path_command_channels(1);
    let binding = ResponseStreamBinding::new_with_limits(
        SessionId(89),
        service.underlay,
        service.path_id,
        service_commands,
        FlowLane::Throughput,
        mux_limits,
    );
    let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(1);
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
        try_recv_reliable_path_command(&mut candidate_rx),
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
    ));
    let target = binding
        .sender_path_targets(FlowLane::Throughput, payload_bytes)
        .into_iter()
        .find(|target| target.key == candidate)
        .expect("candidate output is attached");
    let (planner_generation, _) = binding.subflow_state_snapshot();
    let commit = ResponseSubflowAdmissionCommit {
        planner_generation,
        lane_generation: binding.lane_generation(),
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
    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let stream = ReliablePathStream {
        stream_id: StreamId(89),
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: service.underlay,
        max_frame_payload_bytes: payload_bytes,
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frames_rx,
    };
    let frame = Frame::StreamData {
        stream_id: StreamId(89),
        offset: 0,
        flags: StreamFlags::NONE,
        payload: Bytes::from(vec![5_u8; payload_bytes]),
    };
    candidate_commands
        .try_enqueue_stream_ordered_frame(frame.clone(), FlowLane::Throughput)
        .expect("fill the candidate data queue after planning");
    let blocked = emit_planned_response_data_frame(
        &stream,
        ResponseDataDispatchPlan {
            primary: ResponseDataDispatchTarget::Switchable {
                binding: binding.clone(),
                target: target.clone().into(),
                role: PathRuntimeRole::Subflow,
                service_handoff_commit: None,
                subflow_set_commit: Some(commit),
                ack_clock_calibration_commit: None,
            },
        },
        frame.clone(),
        FlowLane::Throughput,
    )
    .await;
    assert!(matches!(blocked, Err(RuntimeError::SenderServiceBlocked)));
    assert!(try_recv_reliable_path_command(&mut candidate_rx).is_some());

    let emitted = emit_planned_response_data_frame(
        &stream,
        ResponseDataDispatchPlan {
            primary: ResponseDataDispatchTarget::Switchable {
                binding,
                target: target.into(),
                role: PathRuntimeRole::Subflow,
                service_handoff_commit: None,
                subflow_set_commit: Some(commit),
                ack_clock_calibration_commit: None,
            },
        },
        frame,
        FlowLane::Throughput,
    )
    .await
    .expect("the rolled-back startup quantum remains admissible");
    assert_eq!(emitted.selected_path, Some(candidate));
}

#[tokio::test]
async fn stale_passive_topology_plan_blocks_subflow_reservation_and_enqueue() {
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
    let (service_commands, _service_rx) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits(
        SessionId(90),
        service.underlay,
        service.path_id,
        service_commands,
        FlowLane::Throughput,
        mux_limits,
    );
    let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(8);
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
        try_recv_reliable_path_command(&mut candidate_rx),
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
    ));
    let target = binding
        .sender_path_targets(FlowLane::Throughput, payload_bytes)
        .into_iter()
        .find(|target| target.key == candidate)
        .expect("candidate output is attached");
    let (stale_planner_generation, _) = binding.subflow_state_snapshot();
    let lane_generation = binding.lane_generation();
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
    let stale_commit = ResponseSubflowAdmissionCommit {
        planner_generation: stale_planner_generation,
        lane_generation,
        service,
        startup_owner_credit_bytes: payload_bytes,
        optional_overhead_budget_bytes: 0,
        max_read_gap_budget: Duration::ZERO,
        input,
    };
    let (unrelated_commands, _unrelated_rx) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            unrelated.underlay,
            unrelated.path_id,
            unrelated_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            payload_bytes,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let (fresh_planner_generation, _) = binding.subflow_state_snapshot();
    assert_ne!(fresh_planner_generation, stale_planner_generation);

    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let stream = ReliablePathStream {
        stream_id: StreamId(90),
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: service.underlay,
        max_frame_payload_bytes: payload_bytes,
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frames_rx,
    };
    let frame = Frame::StreamData {
        stream_id: StreamId(90),
        offset: 0,
        flags: StreamFlags::NONE,
        payload: Bytes::from(vec![0x55; payload_bytes]),
    };
    let stale = emit_planned_response_data_frame(
        &stream,
        ResponseDataDispatchPlan {
            primary: ResponseDataDispatchTarget::Switchable {
                binding: binding.clone(),
                target: target.clone().into(),
                role: PathRuntimeRole::Subflow,
                service_handoff_commit: None,
                subflow_set_commit: Some(stale_commit),
                ack_clock_calibration_commit: None,
            },
        },
        frame.clone(),
        FlowLane::Throughput,
    )
    .await;
    assert!(matches!(stale, Err(RuntimeError::SenderServiceBlocked)));
    assert!(
        try_recv_reliable_path_command(&mut candidate_rx).is_none(),
        "planner invalidation must fence both reservation and owner enqueue"
    );

    let fresh = emit_planned_response_data_frame(
        &stream,
        ResponseDataDispatchPlan {
            primary: ResponseDataDispatchTarget::Switchable {
                binding,
                target: target.into(),
                role: PathRuntimeRole::Subflow,
                service_handoff_commit: None,
                subflow_set_commit: Some(ResponseSubflowAdmissionCommit {
                    planner_generation: fresh_planner_generation,
                    ..stale_commit
                }),
                ack_clock_calibration_commit: None,
            },
        },
        frame,
        FlowLane::Throughput,
    )
    .await
    .expect("fresh generation may reserve and enqueue the startup quantum");
    assert_eq!(fresh.selected_path, Some(candidate));
    assert!(matches!(
        try_recv_reliable_path_command(&mut candidate_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
}

#[tokio::test]
async fn normal_repair_cache_retention_does_not_create_authoritative_owner_debt() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let active_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    let alternate_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (active_commands, mut active_rx) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits(
        SessionId(83),
        UnderlayProtocol::Udp,
        active_key.path_id,
        active_commands,
        FlowLane::Throughput,
        mux_limits,
    );
    binding.set_ordered_data_owner(active_key);
    binding.update_path_metrics(
        active_key,
        PathMetrics {
            path_id: active_key.path_id,
            underlay: active_key.underlay,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: metric_epoch_now(),
            metric_age_us: 0,
            min_rtt_us: 50_000,
            srtt_us: 50_000,
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
            inflight_limit_bytes: (16 * payload_bytes) as u64,
            inflight_hi_bytes: (16 * payload_bytes) as u64,
            confidence_ppm: 1_000_000,
            app_limited: false,
            has_ack_derived_data_sample: true,
            data_sample_count: 4,
            data_sample_bytes: (16 * payload_bytes) as u64,
        },
        ServerPathMetricsSource::LocalSender,
    );
    let (alternate_commands, mut alternate_rx) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            UnderlayProtocol::Udp,
            alternate_key.path_id,
            alternate_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            payload_bytes,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    binding.update_path_metrics(
        alternate_key,
        PathMetrics {
            path_id: alternate_key.path_id,
            underlay: alternate_key.underlay,
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
            inflight_limit_bytes: (16 * payload_bytes) as u64,
            inflight_hi_bytes: (16 * payload_bytes) as u64,
            confidence_ppm: 1_000_000,
            app_limited: false,
            has_ack_derived_data_sample: true,
            data_sample_count: 4,
            data_sample_bytes: (16 * payload_bytes) as u64,
        },
        ServerPathMetricsSource::LocalSender,
    );
    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let stream = ReliablePathStream {
        stream_id: StreamId(7),
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Udp,
        max_frame_payload_bytes: payload_bytes,
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frames_rx,
    };
    let owner_tail_guard_bytes = payload_bytes.saturating_mul(2);
    let mut send_stream = ReliableSendStream::new(StreamId(7), mux_limits);
    let mut retained_unacked_bytes = owner_tail_guard_bytes.saturating_add(payload_bytes);
    while retained_unacked_bytes > 0 {
        let chunk = retained_unacked_bytes.min(payload_bytes);
        let _unacked = send_stream
            .send_data(Bytes::from(vec![1_u8; chunk]), StreamFlags::NONE)
            .expect("seed normal retained unacked OwnerData above the synthetic tail guard");
        retained_unacked_bytes -= chunk;
    }
    assert!(send_stream.repair_bytes() > owner_tail_guard_bytes);
    assert!(
        binding
            .lower_flights_before_offset(send_stream.next_offset())
            .is_empty(),
        "this regression isolates repair-cache retention from authoritative path-flight debt"
    );
    while let Some(_setup_command) = try_recv_reliable_path_command(&mut alternate_rx) {}

    let mut sender = ServerResponseSenderService::new(SessionId(83), StreamId(7));
    sender.enqueue_data_for_lane(Bytes::from(vec![2_u8; payload_bytes]), FlowLane::Throughput);
    let dispatch = sender
        .dispatch_next(&stream, &mut send_stream, FlowLane::Throughput, mux_limits)
        .expect("normal repair-cache retention must not block Service OwnerData");

    assert_eq!(dispatch.selected_path, Some(active_key));
    assert_eq!(
        binding.ordered_data_owner(),
        Some(active_key),
        "normal repair-cache retention must not rewrite the Service owner hint"
    );
    assert!(matches!(
        recv_reliable_path_command(&mut active_rx).await,
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
    assert!(
        try_recv_reliable_path_command(&mut alternate_rx).is_none(),
        "retained repair-cache bytes are not authoritative debt and must not displace feedable Service"
    );
}

#[tokio::test]
async fn response_owner_tail_guard_admits_measured_subflow_when_service_is_backpressured() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let active_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    let alternate_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (active_commands, mut active_rx) = reliable_path_command_channels(1);
    let active_commands_for_backpressure = active_commands.clone();
    let binding = ResponseStreamBinding::new_with_limits(
        SessionId(82),
        UnderlayProtocol::Udp,
        active_key.path_id,
        active_commands,
        FlowLane::Throughput,
        mux_limits,
    );
    binding.set_ordered_data_owner(active_key);
    binding.update_path_metrics(
        active_key,
        PathMetrics {
            path_id: active_key.path_id,
            underlay: active_key.underlay,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: metric_epoch_now(),
            metric_age_us: 0,
            min_rtt_us: 50_000,
            srtt_us: 50_000,
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
            inflight_limit_bytes: (16 * payload_bytes) as u64,
            inflight_hi_bytes: (16 * payload_bytes) as u64,
            confidence_ppm: 1_000_000,
            app_limited: false,
            has_ack_derived_data_sample: true,
            data_sample_count: 4,
            data_sample_bytes: (16 * payload_bytes) as u64,
        },
        ServerPathMetricsSource::LocalSender,
    );
    let (alternate_commands, mut alternate_rx) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            UnderlayProtocol::Udp,
            alternate_key.path_id,
            alternate_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            payload_bytes,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    binding.update_path_metrics(
        alternate_key,
        PathMetrics {
            path_id: alternate_key.path_id,
            underlay: alternate_key.underlay,
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
            inflight_limit_bytes: (16 * payload_bytes) as u64,
            inflight_hi_bytes: (16 * payload_bytes) as u64,
            confidence_ppm: 1_000_000,
            app_limited: false,
            has_ack_derived_data_sample: true,
            data_sample_count: 4,
            data_sample_bytes: (16 * payload_bytes) as u64,
        },
        ServerPathMetricsSource::LocalSender,
    );
    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let stream = ReliablePathStream {
        stream_id: StreamId(7),
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Udp,
        max_frame_payload_bytes: payload_bytes,
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frames_rx,
    };
    let owner_tail_guard_bytes = payload_bytes.saturating_mul(2);
    let mut send_stream = ReliableSendStream::new(StreamId(7), mux_limits);
    let mut remaining_owner_debt = owner_tail_guard_bytes.saturating_add(payload_bytes);
    while remaining_owner_debt > 0 {
        let chunk = remaining_owner_debt.min(payload_bytes);
        let _unacked = send_stream
            .send_data(Bytes::from(vec![1_u8; chunk]), StreamFlags::NONE)
            .expect("seed unacked ordered-owner tail guard");
        remaining_owner_debt -= chunk;
    }
    assert!(send_stream.repair_bytes() > owner_tail_guard_bytes);
    while let Some(_setup_command) = try_recv_reliable_path_command(&mut alternate_rx) {}
    active_commands_for_backpressure
        .try_enqueue_admitted_frame(
            Frame::StreamData {
                stream_id: StreamId(7),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"queued"),
            },
            FlowLane::Throughput,
        )
        .expect("seed full Service data queue");

    let mut sender = ServerResponseSenderService::new(SessionId(82), StreamId(7));
    sender.enqueue_data_for_lane(Bytes::from(vec![2_u8; payload_bytes]), FlowLane::Throughput);
    let ordered_owner_debt_bytes = send_stream.repair_bytes();
    let dispatch = sender.dispatch_next_with_ordered_owner_debt(
        &stream,
        &mut send_stream,
        FlowLane::Throughput,
        mux_limits,
        ordered_owner_debt_bytes,
    );

    let dispatch =
        dispatch.expect("measured same-underlay Subflow should pass no-worse tail admission");
    assert_eq!(dispatch.selected_path, Some(alternate_key));
    assert_eq!(binding.ordered_data_owner(), Some(active_key));
    assert!(matches!(
        try_recv_reliable_path_command(&mut alternate_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
    assert!(matches!(
        try_recv_reliable_path_command(&mut active_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { payload, .. }))
            if payload == Bytes::from_static(b"queued")
    ));
    assert!(try_recv_reliable_path_command(&mut active_rx).is_none());
}

#[tokio::test]
async fn quic_ack_data_path_does_not_own_range_under_lower_owner_debt() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let active_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    let (active_commands, _active_rx) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits(
        SessionId(88),
        active_key.underlay,
        active_key.path_id,
        active_commands,
        FlowLane::Throughput,
        mux_limits,
    );
    binding.set_ordered_data_owner(active_key);
    let active_frame = Frame::StreamData {
        stream_id: StreamId(7),
        offset: 0,
        flags: StreamFlags::NONE,
        payload: Bytes::from(vec![3_u8; payload_bytes]),
    };
    binding.record_owner_flight(active_key, &active_frame);

    let (ack_data_path_commands, mut ack_data_path_rx) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            UnderlayProtocol::Udp,
            PathId(1),
            ack_data_path_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            payload_bytes,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let ack_data_path_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    binding.update_path_metrics(
        ack_data_path_key,
        PathMetrics {
            path_id: ack_data_path_key.path_id,
            underlay: ack_data_path_key.underlay,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: metric_epoch_now(),
            metric_age_us: 0,
            min_rtt_us: 20_000,
            srtt_us: 20_000,
            rttvar_us: 1_000,
            jitter_us: 1_000,
            delivery_rate_bps: default_path_rate_bps(UnderlayProtocol::Udp).round() as u64,
            pacing_rate_bps: default_path_rate_bps(UnderlayProtocol::Udp).round() as u64,
            loss_ppm: 0,
            ecn_ppm: 0,
            loss_observed: false,
            ecn_observed: false,
            bytes_in_flight: 0,
            queue_bytes: 0,
            inflight_limit_bytes: payload_bytes as u64,
            inflight_hi_bytes: payload_bytes as u64,
            confidence_ppm: 0,
            app_limited: true,
            has_ack_derived_data_sample: true,
            data_sample_count: 0,
            data_sample_bytes: 0,
        },
        ServerPathMetricsSource::LocalSender,
    );
    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let stream = ReliablePathStream {
        stream_id: StreamId(7),
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Udp,
        max_frame_payload_bytes: payload_bytes,
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frames_rx,
    };
    let plan = plan_response_data_dispatch(
        &stream,
        FlowLane::Throughput,
        payload_bytes as u64,
        payload_bytes,
    )
    .expect("active owner should remain dispatchable");
    assert_eq!(plan.primary_key(), Some(active_key));
    assert_eq!(
        plan.primary_role(),
        PathRuntimeRole::Service,
        "validation paths must not receive unique owner data while lower bytes are unresolved"
    );

    let service_frame = Frame::StreamData {
        stream_id: StreamId(7),
        offset: payload_bytes as u64,
        flags: StreamFlags::NONE,
        payload: Bytes::from(vec![4_u8; payload_bytes]),
    };
    let outcome =
        emit_planned_response_data_frame(&stream, plan, service_frame, FlowLane::Throughput)
            .await
            .expect("service owner data should emit");

    assert_eq!(outcome.selected_path, Some(active_key));
    assert_eq!(
        binding.ordered_data_owner(),
        Some(active_key),
        "service owner remains the ordinary lead"
    );
    while let Some(_command) = try_recv_reliable_path_command(&mut ack_data_path_rx) {}
    let lower = binding.lower_flights_before_offset((payload_bytes * 2) as u64);
    assert!(!lower.iter().any(|flight| flight.key == ack_data_path_key));
}

#[test]
fn single_response_carrier_uses_sliding_window_not_multipath_ordering_debt() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let assigned_bytes = bulk_service_feed_reservoir_payload_bytes(payload_bytes, mux_limits)
        .saturating_sub(payload_bytes);
    let mut target = response_target(
        0,
        UnderlayProtocol::Tcp,
        5.0,
        assigned_bytes as u64,
        16 * 1024 * 1024,
        true,
    );
    target.snapshot.product_progress_rate_bps = Some(10_000_000_000.0);
    let lower_flights = vec![CarrierPathFlightDebt {
        key: target.key,
        bytes: assigned_bytes as u64,
    }];

    let selected = choose_response_sender_data_target(
        std::slice::from_ref(&target),
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &lower_flights,
        Some(target.key),
    )
    .expect("single carrier lower flight is normal sliding-window debt");

    assert_eq!(selected.key, target.key);
}

#[test]
fn proven_udp_candidate_cannot_overtake_large_lower_owner() {
    let mut owner = response_target(
        0,
        UnderlayProtocol::Udp,
        80.0,
        2 * 1024 * 1024,
        16 * 1024 * 1024,
        true,
    );
    owner.snapshot.product_progress_rate_bps = Some(500_000_000.0);
    let alternate = response_target(1, UnderlayProtocol::Udp, 7.0, 0, 16 * 1024 * 1024, false);
    let lower_flights = vec![CarrierPathFlightDebt {
        key: owner.key,
        bytes: 2 * 1024 * 1024,
    }];

    let selected = choose_response_sender_data_target(
        &[owner.clone(), alternate.clone()],
        FlowLane::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &lower_flights,
        Some(owner.key),
    )
    .expect("lower owner should remain eligible while it owns unresolved lower bytes");

    assert_eq!(selected.key, owner.key);
}

#[test]
fn proven_udp_candidate_waits_even_when_lower_owner_debt_is_within_reorder_budget() {
    let mut owner = response_target(
        0,
        UnderlayProtocol::Udp,
        80.0,
        2 * 1024 * 1024,
        16 * 1024 * 1024,
        true,
    );
    owner.snapshot.product_progress_rate_bps = Some(500_000_000.0);
    let lower_eta_alternate =
        response_target(1, UnderlayProtocol::Udp, 7.0, 0, 16 * 1024 * 1024, false);
    let lower_flights = vec![CarrierPathFlightDebt {
        key: owner.key,
        bytes: 64 * 1024,
    }];

    let selected = choose_response_sender_data_target(
        &[owner.clone(), lower_eta_alternate],
        FlowLane::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &lower_flights,
        Some(owner.key),
    )
    .expect("lower owner should remain eligible while the frontier is not clear");

    assert_eq!(selected.key.path_id, PathId(0));
}

#[test]
fn proof_only_udp_candidate_is_blocked_from_unique_data_with_lower_udp_owner() {
    let mut owner = response_target(
        0,
        UnderlayProtocol::Udp,
        80.0,
        2 * 1024 * 1024,
        16 * 1024 * 1024,
        true,
    );
    owner.snapshot.product_progress_rate_bps = Some(500_000_000.0);
    let mut proof_only = response_target(1, UnderlayProtocol::Udp, 7.0, 0, 16 * 1024 * 1024, false);
    proof_only.has_bulk_rate_evidence = false;
    let lower_flights = vec![CarrierPathFlightDebt {
        key: owner.key,
        bytes: 64 * 1024,
    }];

    let selected = choose_response_sender_data_target(
        &[owner.clone(), proof_only],
        FlowLane::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &lower_flights,
        Some(owner.key),
    )
    .expect("proof-only path should not own unique later bytes");

    assert_eq!(selected.key, owner.key);
}

#[test]
fn proof_only_tcp_candidate_does_not_displace_bulk_rate_proven_udp() {
    let bulk_proven_udp =
        response_target(0, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, true);
    let mut proof_only_tcp =
        response_target(0, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);
    proof_only_tcp.has_sender_evidence = true;
    proof_only_tcp.has_bulk_rate_evidence = false;

    let selected = choose_response_sender_data_target(
        &[bulk_proven_udp.clone(), proof_only_tcp],
        FlowLane::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &[],
        Some(bulk_proven_udp.key),
    )
    .expect("bulk-rate-proven path should remain unique ordered owner");

    assert_eq!(selected.key, bulk_proven_udp.key);
}

#[test]
fn response_clear_frontier_keeps_feedable_service_ahead_of_lower_eta_subflow() {
    let lead = response_target(0, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, true);
    let lower_eta_alternate =
        response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);

    let selected = choose_response_sender_data_target(
        &[lead.clone(), lower_eta_alternate.clone()],
        FlowLane::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &[],
        Some(lead.key),
    )
    .expect("feedable Service should remain selected");

    assert_eq!(selected.key, lead.key);
}

#[test]
fn feedable_service_precedes_lower_eta_same_family_subflow() {
    let service = response_target(0, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, true);
    let lower_eta_subflow =
        response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), lower_eta_subflow.clone()],
        FlowLane::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &[],
        Some(service.key),
        0,
        None,
    )
    .expect("feedable Service should remain selected ahead of admitted overflow");

    assert_eq!(selected.target.key, service.key);
    assert_eq!(
        selected.admission.role,
        PathRuntimeRole::Service,
        "a lower-ETA Subflow remains eligible overflow and does not displace feedable Service"
    );
}

#[test]
fn same_family_lower_frontier_owner_remains_subflow() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);

    for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
        let service = response_target(1, underlay, 50.0, 0, 16 * 1024 * 1024, true);
        let lower_owner = response_target(0, underlay, 5.0, 0, 16 * 1024 * 1024, false);
        let lower_flights = [CarrierPathFlightDebt {
            key: lower_owner.key,
            bytes: payload_bytes as u64,
        }];

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), lower_owner.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &lower_flights,
            Some(service.key),
            payload_bytes.saturating_mul(2),
            None,
        )
        .expect("measured lower-frontier owner should remain dispatchable as a Subflow");

        assert_eq!(selected.target.key, lower_owner.key, "{underlay:?}");
        assert_eq!(selected.admission.role, PathRuntimeRole::Subflow);
        assert_eq!(
            selected.subflow_set_commit.map(|commit| commit.service),
            Some(service.key),
            "{underlay:?} lower-frontier continuation must retain the Service anchor"
        );
    }
}

#[test]
fn cross_family_lower_frontier_owner_remains_subflow() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service = response_target(1, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, true);
    let lower_owner = response_target(0, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    let lower_flights = [CarrierPathFlightDebt {
        key: lower_owner.key,
        bytes: payload_bytes as u64,
    }];

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), lower_owner.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &lower_flights,
        Some(service.key),
        payload_bytes.saturating_mul(2),
        None,
    )
    .expect("measured cross-family lower-frontier owner should remain dispatchable");

    assert_eq!(selected.target.key, lower_owner.key);
    assert_eq!(selected.admission.role, PathRuntimeRole::Subflow);
    assert_eq!(
        selected.subflow_set_commit.map(|commit| commit.service),
        Some(service.key),
        "cross-family continuation must not commit an implicit Service migration"
    );
}

#[test]
fn authoritative_lower_frontier_suspends_unmeasured_startup_sampling() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);

    for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
        let service = response_target(1, underlay, 50.0, 0, 16 * 1024 * 1024, true);
        let mut proof_only = response_target(0, underlay, 5.0, 0, 16 * 1024 * 1024, false);
        proof_only.has_bulk_rate_evidence = false;
        let lower_flights = [CarrierPathFlightDebt {
            key: proof_only.key,
            bytes: payload_bytes as u64,
        }];

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), proof_only],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &lower_flights,
            Some(service.key),
            payload_bytes.saturating_mul(2),
            None,
        );

        assert!(
            selected.is_none(),
            "{underlay:?} sender evidence alone must not extend an ACK hole"
        );
    }
}

#[test]
fn slow_measured_lower_frontier_cannot_borrow_service_admission() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);

    for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
        let service = response_target(1, underlay, 5.0, 0, 16 * 1024 * 1024, true);
        let mut slow_lower_owner = response_target(0, underlay, 500.0, 0, 16 * 1024 * 1024, false);
        slow_lower_owner.snapshot.delivery_rate_bps = 20_000_000.0;
        slow_lower_owner.snapshot.pacing_rate_bps = 20_000_000.0;
        slow_lower_owner.snapshot.product_progress_rate_bps = Some(20_000_000.0);
        let lower_flights = [CarrierPathFlightDebt {
            key: slow_lower_owner.key,
            bytes: payload_bytes as u64,
        }];

        let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), slow_lower_owner],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &lower_flights,
            Some(service.key),
            payload_bytes.saturating_mul(2),
            None,
        );

        assert!(
            selected.is_none(),
            "{underlay:?} lower ownership is not permission to borrow Service admission"
        );
    }
}

#[test]
fn backpressured_service_remains_lower_frontier_completion_baseline() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(1, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, true);
    let (service_commands, _service_receivers) = reliable_path_command_channels(1);
    service_commands
        .try_enqueue_stream_ordered_frame(
            Frame::StreamData {
                stream_id: StreamId(901),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"queued"),
            },
            FlowLane::Throughput,
        )
        .expect("test setup should fill the Service data queue");
    service.commands = service_commands;
    let lower_owner = response_target(0, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    let lower_flights = [CarrierPathFlightDebt {
        key: lower_owner.key,
        bytes: payload_bytes as u64,
    }];

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), lower_owner.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &lower_flights,
        Some(service.key),
        payload_bytes.saturating_mul(2),
        None,
    )
    .expect("measured lower-frontier Subflow should be evaluated against queued Service");

    assert_eq!(selected.target.key, lower_owner.key);
    assert_eq!(selected.admission.role, PathRuntimeRole::Subflow);
    assert_eq!(
        selected.subflow_set_commit.map(|commit| commit.service),
        Some(service.key)
    );
}

#[test]
fn detached_service_with_lower_frontier_waits_for_repair_or_ack_clear() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let lower_owner = response_target(0, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    let lower_flights = [CarrierPathFlightDebt {
        key: lower_owner.key,
        bytes: payload_bytes as u64,
    }];

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        std::slice::from_ref(&lower_owner),
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &lower_flights,
        None,
        payload_bytes.saturating_mul(2),
        None,
    );

    assert!(
        selected.is_none(),
        "a lower-hole owner cannot infer Service authority after the anchor detaches"
    );
}

#[test]
fn clear_frontier_unavailable_ordered_owner_reanchors_service_to_bulk_proven_path() {
    let (service_commands, _service_receivers) = reliable_path_command_channels(1);
    let mut service_snapshot =
        PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 50.0, 500_000_000.0);
    service_snapshot.inflight_limit_bytes = 16 * 1024 * 1024;
    service_snapshot.confidence = 1.0;
    let service = ResponseSenderPathTarget {
        #[cfg(feature = "lab-diagnostics")]
        session_id: SessionId(0),
        #[cfg(feature = "lab-diagnostics")]
        binding_instance_id: 0,
        key: CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        },
        path_instance_id: next_server_carrier_path_instance_id(),
        incarnation: 1,
        commands: service_commands,
        attachment_role: StreamOpenRole::Active,
        snapshot: service_snapshot,
        owner_data_in_flight_bytes: 0,
        command_pending_bytes: 0,
        eta_ms: 50.0,
        is_active: true,
        is_request_active: true,
        has_sender_evidence: true,
        has_service_feed_evidence: true,
        has_bulk_rate_evidence: true,
        endpoint_only_service_prior_eligible: false,
        quic_capacity_proof: None,
        quic_capacity_calibration_attempts: 0,
        ack_clock_calibration_eligible: false,
        ack_clock_calibration_proven: false,
        ack_clock_calibration_spent_bytes: 0,
        ack_clock_calibration_credit_limit_bytes: 0,
        ack_clock_calibration_max_limit_bytes: 0,
        ack_clock_calibration_active: false,
    };
    service
        .commands
        .try_enqueue_stream_ordered_frame(
            Frame::StreamData {
                stream_id: StreamId(900),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"x"),
            },
            FlowLane::Throughput,
        )
        .expect("test setup should fill the service data queue");
    let lower_eta_subflow =
        response_target(2, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), lower_eta_subflow.clone()],
        FlowLane::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &[],
        Some(service.key),
        0,
        None,
    )
    .expect("bulk-rate-proven alternate should become Service when the prior clear-frontier owner is not dispatchable");

    assert_eq!(selected.target.key, lower_eta_subflow.key);
    assert_eq!(
        selected.admission.role,
        PathRuntimeRole::Service,
        "a clear-frontier owner hint is not a permanent Service anchor when that output cannot enqueue owner bytes"
    );
}

#[test]
fn lower_eta_same_family_subflow_does_not_borrow_active_service_envelope() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service = response_target(0, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, true);
    let mut saturated_subflow =
        response_target(1, UnderlayProtocol::Udp, 5.0, 0, 512 * 1024, false);
    saturated_subflow.snapshot.product_bytes_in_flight =
        RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES;
    saturated_subflow.snapshot.bytes_in_flight = RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), saturated_subflow],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        0,
        None,
    )
    .expect("Service should remain eligible when the lower-ETA Subflow is out of credit");

    assert_eq!(
        selected.target.key, service.key,
        "non-active Subflow admission must use additional-path gates instead of the active Service envelope"
    );
    assert_eq!(selected.admission.role, PathRuntimeRole::Service);
}

#[test]
fn response_ordinary_bulk_keeps_lead_only_inside_measured_hysteresis() {
    let mut lead = response_target(0, UnderlayProtocol::Udp, 5.1, 0, 16 * 1024 * 1024, true);
    let mut lower_eta_alternate =
        response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    lead.snapshot.jitter_ms = 0.2;
    lower_eta_alternate.snapshot.jitter_ms = 0.1;

    let selected = choose_response_sender_data_target(
        &[lead.clone(), lower_eta_alternate],
        FlowLane::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &[],
        Some(lead.key),
    )
    .expect("near-tie lead should remain selected inside observed jitter");

    assert_eq!(selected.key, lead.key);
}

#[test]
fn active_service_remains_admissible_lead_when_subflow_is_not_admissible() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Tcp, 25.0, 0, 16 * 1024 * 1024, true);
    service.has_bulk_rate_evidence = false;
    let mut subflow = response_target(
        1,
        UnderlayProtocol::Tcp,
        5.0,
        mux_limits.max_path_flight_bytes as u64,
        16 * 1024 * 1024,
        false,
    );
    subflow.has_bulk_rate_evidence = true;
    let candidates = [&service, &subflow];

    let lead = choose_response_admissible_lead(
        &candidates,
        Some(&service),
        mux_limits,
        payload_bytes,
        &[],
        false,
    )
    .expect("active Service must remain a lead candidate when optional Subflow is blocked");

    assert_eq!(
        lead.key, service.key,
        "optional bulk-rate evidence must not hide the current Service owner"
    );
}

#[test]
fn active_service_remains_lead_when_measured_subflow_has_lower_eta() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service = response_target(0, UnderlayProtocol::Tcp, 25.0, 0, 16 * 1024 * 1024, true);
    let measured_subflow =
        response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);
    let candidates = [&service, &measured_subflow];

    let lead = choose_response_admissible_lead(
        &candidates,
        Some(&service),
        mux_limits,
        payload_bytes,
        &[],
        false,
    )
    .expect("active Service should remain the lead anchor");

    assert_eq!(
        lead.key, service.key,
        "a lower-ETA same-family Subflow must not redefine Service ownership"
    );
}

#[test]
fn feedable_service_owner_is_selected_before_lower_eta_same_family_subflow() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Udp, 25.0, 0, 16 * 1024 * 1024, true);
    service.snapshot.product_progress_rate_bps = Some(80_000_000.0);
    service.has_sender_evidence = true;
    service.has_bulk_rate_evidence = true;

    let mut measured_subflow =
        response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    measured_subflow.snapshot.product_progress_rate_bps = Some(120_000_000.0);
    measured_subflow.snapshot.app_limited = false;
    measured_subflow.has_sender_evidence = true;
    measured_subflow.has_bulk_rate_evidence = true;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), measured_subflow],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        0,
        None,
    )
    .expect("feedable Service owner should remain dispatchable");

    assert_eq!(
        selected.target.key, service.key,
        "same-family Subflow OwnerData is additive; it must not replace a feedable Service quantum just because its instantaneous ETA is lower"
    );
    assert_eq!(selected.admission.role, PathRuntimeRole::Service);
}

#[test]
fn measured_tcp_subflow_uses_bounded_reservoir_beyond_service_horizon() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service_horizon = bulk_service_horizon_payload_bytes(payload_bytes, mux_limits);
    let mut service = response_target(
        0,
        UnderlayProtocol::Tcp,
        25.0,
        service_horizon.saturating_sub(payload_bytes) as u64,
        mux_limits.max_path_flight_bytes as u64,
        true,
    );
    service.snapshot.product_progress_rate_bps = Some(80_000_000.0);
    let mut measured_subflow = response_target(
        1,
        UnderlayProtocol::Tcp,
        5.0,
        0,
        mux_limits.max_path_flight_bytes as u64,
        false,
    );
    measured_subflow.snapshot.product_progress_rate_bps = Some(120_000_000.0);
    measured_subflow.snapshot.srtt_ms = 80.0;
    measured_subflow.snapshot.min_rtt_ms = 80.0;
    measured_subflow.snapshot.delivery_rate_bps = 200_000_000.0;
    measured_subflow.snapshot.pacing_rate_bps = 200_000_000.0;
    measured_subflow.snapshot.app_limited = false;

    let below_horizon = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), measured_subflow.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        service_horizon.saturating_sub(payload_bytes),
        None,
    )
    .expect("Service should fill its protected horizon first");
    assert_eq!(below_horizon.target.key, service.key);
    assert_eq!(below_horizon.admission.role, PathRuntimeRole::Service);

    service.snapshot.product_bytes_in_flight = service_horizon as u64;
    service.owner_data_in_flight_bytes = service_horizon as u64;
    let reservoir_subflow = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), measured_subflow.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        service_horizon,
        None,
    )
    .expect("measured TCP Subflow should use the remaining source reservoir");
    assert_eq!(reservoir_subflow.target.key, measured_subflow.key);
    assert_eq!(reservoir_subflow.admission.role, PathRuntimeRole::Subflow);
    assert_eq!(
        reservoir_subflow
            .subflow_set_commit
            .map(|commit| commit.service),
        Some(service.key),
        "overflow must remain bound to the exact current Service epoch"
    );

    let product_reservoir = bulk_service_product_envelope_payload_bytes(payload_bytes, mux_limits);
    service.snapshot.product_bytes_in_flight = (product_reservoir / 2) as u64;
    service.owner_data_in_flight_bytes = (product_reservoir / 2) as u64;
    let mut backlog_subflow = measured_subflow.clone();
    backlog_subflow.eta_ms = 400.0;
    backlog_subflow.snapshot.srtt_ms = 360.0;
    backlog_subflow.snapshot.min_rtt_ms = 360.0;
    backlog_subflow.snapshot.delivery_rate_bps = 200_000_000.0;
    backlog_subflow.snapshot.pacing_rate_bps = 200_000_000.0;
    let backlog_selection = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), backlog_subflow.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        product_reservoir / 2,
        None,
    )
    .expect("Service remains feedable when cross-path prefix debt is capped");
    assert_eq!(backlog_selection.target.key, service.key);
    assert_eq!(backlog_selection.admission.role, PathRuntimeRole::Service);

    service.snapshot.product_bytes_in_flight = product_reservoir as u64;
    service.owner_data_in_flight_bytes = product_reservoir as u64;
    let exhausted_reservoir = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), measured_subflow],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        product_reservoir,
        None,
    );
    assert!(
        exhausted_reservoir.is_none(),
        "the full product envelope blocks new ownership until ACK progress"
    );
}

#[test]
fn measured_quic_subflow_uses_bounded_reservoir_before_new_startup() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service_horizon = bulk_service_horizon_payload_bytes(payload_bytes, mux_limits);
    let mut service = response_target(
        0,
        UnderlayProtocol::Udp,
        25.0,
        service_horizon as u64,
        mux_limits.max_path_flight_bytes as u64,
        true,
    );
    service.snapshot.product_progress_rate_bps = Some(80_000_000.0);
    service.snapshot.active_flows = 1;
    service.owner_data_in_flight_bytes = service_horizon as u64;
    let mut measured_subflow = response_target(
        1,
        UnderlayProtocol::Udp,
        5.0,
        0,
        mux_limits.max_path_flight_bytes as u64,
        false,
    );
    measured_subflow.snapshot.product_progress_rate_bps = Some(120_000_000.0);
    measured_subflow.snapshot.srtt_ms = 80.0;
    measured_subflow.snapshot.min_rtt_ms = 80.0;
    measured_subflow.snapshot.delivery_rate_bps = 200_000_000.0;
    measured_subflow.snapshot.pacing_rate_bps = 200_000_000.0;
    measured_subflow.snapshot.app_limited = false;
    let mut unmeasured = response_target(
        2,
        UnderlayProtocol::Udp,
        1.0,
        0,
        mux_limits.max_path_flight_bytes as u64,
        false,
    );
    unmeasured.has_bulk_rate_evidence = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), measured_subflow.clone(), unmeasured],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        service_horizon,
        None,
    )
    .expect("a measured QUIC Subflow should use the bounded same-family partition");

    assert_eq!(selected.target.key, measured_subflow.key);
    assert_eq!(selected.admission.role, PathRuntimeRole::Subflow);
    assert_eq!(
        selected.subflow_set_commit.map(|commit| commit.service),
        Some(service.key),
        "measured QUIC overflow remains bound to the current Service"
    );

    let product_reservoir = bulk_service_product_envelope_payload_bytes(payload_bytes, mux_limits);
    let exhausted = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), measured_subflow],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        product_reservoir,
        None,
    )
    .expect("Service remains the fallback at the ordering-reservoir boundary");
    assert_eq!(exhausted.target.key, service.key);
    assert_eq!(exhausted.admission.role, PathRuntimeRole::Service);
}

#[test]
fn measured_quic_subflow_does_not_cross_into_equal_path_load() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service_horizon = bulk_service_horizon_payload_bytes(payload_bytes, mux_limits);
    let mut service = response_target(
        0,
        UnderlayProtocol::Udp,
        25.0,
        service_horizon as u64,
        mux_limits.max_path_flight_bytes as u64,
        true,
    );
    service.owner_data_in_flight_bytes = service_horizon as u64;
    service.snapshot.active_flows = 1;
    let mut measured_subflow = response_target(
        1,
        UnderlayProtocol::Udp,
        5.0,
        0,
        mux_limits.max_path_flight_bytes as u64,
        false,
    );
    measured_subflow.snapshot.active_flows = 1;
    measured_subflow.snapshot.app_limited = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), measured_subflow],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        service_horizon,
        None,
    )
    .expect("the balanced QUIC Service should remain dispatchable");

    assert_eq!(selected.target.key, service.key);
    assert_eq!(selected.admission.role, PathRuntimeRole::Service);
}

#[test]
fn tcp_reservoir_does_not_charge_service_horizon_to_low_bdp_subflow() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service_horizon = bulk_service_horizon_payload_bytes(payload_bytes, mux_limits);
    let mut service = response_target(
        0,
        UnderlayProtocol::Tcp,
        25.0,
        service_horizon as u64,
        mux_limits.max_path_flight_bytes as u64,
        true,
    );
    service.snapshot.product_progress_rate_bps = Some(80_000_000.0);

    let mut low_bdp_subflow = response_target(
        1,
        UnderlayProtocol::Tcp,
        5.0,
        0,
        mux_limits.max_path_flight_bytes as u64,
        false,
    );
    low_bdp_subflow.snapshot.product_progress_rate_bps = Some(54_016_000.0);
    low_bdp_subflow.snapshot.delivery_rate_bps = 54_016_000.0;
    low_bdp_subflow.snapshot.pacing_rate_bps = 54_016_000.0;
    low_bdp_subflow.snapshot.srtt_ms = 137.968;
    low_bdp_subflow.snapshot.min_rtt_ms = 137.968;
    low_bdp_subflow.snapshot.app_limited = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), low_bdp_subflow.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        service_horizon,
        None,
    )
    .expect("Service or its measured TCP Subflow must remain feedable");

    assert_eq!(
        selected.target.key, low_bdp_subflow.key,
        "the connection-level Service horizon consumes global reservoir credit once; it is not candidate-local BDP flight"
    );
    assert_eq!(selected.admission.role, PathRuntimeRole::Subflow);
}

#[test]
fn tcp_reservoir_requires_unique_service_owner_horizon() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service_horizon = bulk_service_horizon_payload_bytes(payload_bytes, mux_limits);
    let mut service = response_target(
        0,
        UnderlayProtocol::Tcp,
        25.0,
        service_horizon as u64,
        mux_limits.max_path_flight_bytes as u64,
        true,
    );
    service.owner_data_in_flight_bytes = payload_bytes as u64;
    service.snapshot.queue_bytes = service_horizon as u64;
    service.snapshot.product_progress_rate_bps = Some(80_000_000.0);

    let mut measured_subflow = response_target(
        1,
        UnderlayProtocol::Tcp,
        5.0,
        0,
        mux_limits.max_path_flight_bytes as u64,
        false,
    );
    measured_subflow.snapshot.product_progress_rate_bps = Some(200_000_000.0);
    measured_subflow.snapshot.delivery_rate_bps = 200_000_000.0;
    measured_subflow.snapshot.pacing_rate_bps = 200_000_000.0;
    measured_subflow.snapshot.app_limited = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), measured_subflow],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        service_horizon,
        None,
    )
    .expect("Service remains the fallback until its unique quota is assigned");

    assert_eq!(selected.target.key, service.key);
    assert_eq!(selected.admission.role, PathRuntimeRole::Service);
}

#[test]
fn tcp_reservoir_split_derives_reduced_resource_geometry() {
    let mut mux_limits = MuxLimits::default();
    let resource_limit = 4 * 1024 * 1024;
    mux_limits.max_path_flight_bytes = resource_limit;
    mux_limits.max_repair_bytes = resource_limit;
    mux_limits.max_reorder_bytes = resource_limit;
    mux_limits.max_stream_window_bytes = resource_limit as u64;
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service_horizon = bulk_service_horizon_payload_bytes(payload_bytes, mux_limits);
    let feed_reservoir = bulk_service_feed_reservoir_payload_bytes(payload_bytes, mux_limits);
    assert!(
        service_horizon < bulk_service_horizon_payload_bytes(payload_bytes, MuxLimits::default())
    );
    assert!(feed_reservoir <= resource_limit);

    let service = response_target(
        0,
        UnderlayProtocol::Tcp,
        25.0,
        service_horizon as u64,
        resource_limit as u64,
        true,
    );
    let mut measured_subflow = response_target(
        1,
        UnderlayProtocol::Tcp,
        5.0,
        0,
        resource_limit as u64,
        false,
    );
    measured_subflow.snapshot.srtt_ms = 80.0;
    measured_subflow.snapshot.min_rtt_ms = 80.0;
    measured_subflow.snapshot.delivery_rate_bps = 200_000_000.0;
    measured_subflow.snapshot.pacing_rate_bps = 200_000_000.0;
    measured_subflow.snapshot.app_limited = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), measured_subflow.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        service_horizon,
        None,
    )
    .expect("reduced valid resources should retain the derived TCP split");
    assert_eq!(selected.target.key, measured_subflow.key);
    assert_eq!(selected.admission.role, PathRuntimeRole::Subflow);
}

#[test]
fn tcp_reservoir_split_yields_to_latency_and_calibration_fences() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service_horizon = bulk_service_horizon_payload_bytes(payload_bytes, mux_limits);
    let mut service = response_target(
        0,
        UnderlayProtocol::Tcp,
        25.0,
        service_horizon as u64,
        mux_limits.max_path_flight_bytes as u64,
        true,
    );
    let mut measured_subflow = response_target(
        1,
        UnderlayProtocol::Tcp,
        5.0,
        0,
        mux_limits.max_path_flight_bytes as u64,
        false,
    );
    measured_subflow.snapshot.srtt_ms = 80.0;
    measured_subflow.snapshot.min_rtt_ms = 80.0;
    measured_subflow.snapshot.delivery_rate_bps = 200_000_000.0;
    measured_subflow.snapshot.pacing_rate_bps = 200_000_000.0;
    measured_subflow.snapshot.app_limited = false;

    service.snapshot.active_latency_sensitive_flows = 1;
    let path_pressure = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), measured_subflow.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        service_horizon,
        None,
    )
    .expect("Service stays live under path-local latency pressure");
    assert_eq!(path_pressure.target.key, service.key);

    service.snapshot.active_latency_sensitive_flows = 0;
    service.snapshot.session_active_latency_sensitive_flows = 1;
    let session_pressure = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), measured_subflow.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        service_horizon,
        None,
    )
    .expect("Service stays live under session latency pressure");
    assert_eq!(session_pressure.target.key, service.key);

    service.snapshot.session_active_latency_sensitive_flows = 0;
    measured_subflow.ack_clock_calibration_eligible = true;
    measured_subflow.ack_clock_calibration_active = true;
    measured_subflow.ack_clock_calibration_proven = true;
    measured_subflow.ack_clock_calibration_spent_bytes =
        reliable_ack_clock_calibration_limit_bytes(mux_limits);
    measured_subflow.ack_clock_calibration_credit_limit_bytes =
        measured_subflow.ack_clock_calibration_spent_bytes;
    measured_subflow.ack_clock_calibration_max_limit_bytes =
        measured_subflow.ack_clock_calibration_spent_bytes;
    let calibration_fence = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), measured_subflow],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        service_horizon,
        None,
    )
    .expect("Service remains available while exact calibration flights drain");
    assert_eq!(calibration_fence.target.key, service.key);
    assert_eq!(calibration_fence.admission.role, PathRuntimeRole::Service);
}

#[test]
fn tcp_reservoir_waits_for_binding_calibration_tail() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service_horizon = bulk_service_horizon_payload_bytes(payload_bytes, mux_limits);
    let mut service = response_target(
        0,
        UnderlayProtocol::Tcp,
        25.0,
        service_horizon as u64,
        mux_limits.max_path_flight_bytes as u64,
        true,
    );
    service.snapshot.product_progress_rate_bps = Some(80_000_000.0);

    let mut proven = response_target(
        1,
        UnderlayProtocol::Tcp,
        5.0,
        0,
        mux_limits.max_path_flight_bytes as u64,
        false,
    );
    proven.snapshot.product_progress_rate_bps = Some(200_000_000.0);
    proven.snapshot.delivery_rate_bps = 200_000_000.0;
    proven.snapshot.pacing_rate_bps = 200_000_000.0;
    proven.snapshot.app_limited = false;

    let mut calibrating = response_target(
        2,
        UnderlayProtocol::Tcp,
        10.0,
        0,
        mux_limits.max_path_flight_bytes as u64,
        false,
    );
    let stage = reliable_ack_clock_calibration_limit_bytes(mux_limits);
    calibrating.ack_clock_calibration_eligible = true;
    calibrating.ack_clock_calibration_active = true;
    calibrating.ack_clock_calibration_spent_bytes = stage;
    calibrating.ack_clock_calibration_credit_limit_bytes = stage;
    calibrating.ack_clock_calibration_max_limit_bytes = 2 * stage;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), proven, calibrating],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        service_horizon,
        None,
    )
    .expect("Service remains available while calibration waits for ACK evidence");

    assert_eq!(selected.target.key, service.key);
    assert_eq!(selected.admission.role, PathRuntimeRole::Service);
}

#[test]
fn udp_service_remains_first_after_its_service_horizon() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service_horizon = bulk_service_horizon_payload_bytes(payload_bytes, mux_limits);
    let service = response_target(
        0,
        UnderlayProtocol::Udp,
        25.0,
        service_horizon as u64,
        mux_limits.max_path_flight_bytes as u64,
        true,
    );
    let measured_subflow = response_target(
        1,
        UnderlayProtocol::Udp,
        5.0,
        0,
        mux_limits.max_path_flight_bytes as u64,
        false,
    );

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), measured_subflow],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        service_horizon,
        None,
    )
    .expect("UDP Service remains the packet-controller owner policy");
    assert_eq!(selected.target.key, service.key);
    assert_eq!(selected.admission.role, PathRuntimeRole::Service);
}

#[test]
fn unproven_service_bootstraps_before_app_limited_proven_subflow() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Udp, 25.0, 0, 16 * 1024 * 1024, true);
    service.snapshot.product_queue_bytes = (2 * payload_bytes) as u64;
    service.snapshot.app_limited = true;
    service.has_bulk_rate_evidence = false;

    let mut proven_subflow =
        response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    proven_subflow.snapshot.app_limited = true;
    proven_subflow.has_bulk_rate_evidence = true;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), proven_subflow],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        0,
        None,
    )
    .expect("the unproven live Service remains feedable");

    assert_eq!(selected.target.key, service.key);
    assert_eq!(selected.admission.role, PathRuntimeRole::Service);
}

#[test]
fn feedable_service_precedes_less_committed_app_limited_subflow() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Udp, 25.0, 0, 16 * 1024 * 1024, true);
    service.snapshot.product_queue_bytes = (2 * payload_bytes) as u64;

    let mut underloaded =
        response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    underloaded.snapshot.app_limited = true;
    underloaded.has_bulk_rate_evidence = true;

    let mut overloaded = response_target(2, UnderlayProtocol::Udp, 1.0, 0, 16 * 1024 * 1024, false);
    overloaded.snapshot.product_queue_bytes = (4 * payload_bytes) as u64;
    overloaded.snapshot.app_limited = true;
    overloaded.has_bulk_rate_evidence = true;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), underloaded, overloaded],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        }),
        0,
        None,
    )
    .expect("feedable Service remains selected despite more committed work");

    assert_eq!(selected.target.key, service.key);
    assert_eq!(selected.admission.role, PathRuntimeRole::Service);
}

#[test]
fn tcp_ack_clock_calibration_rejects_seed_beyond_service_reservoir() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service = response_target(0, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, true);
    let mut candidate = response_target(
        1,
        UnderlayProtocol::Tcp,
        1_500.0,
        0,
        16 * 1024 * 1024,
        false,
    );
    candidate.snapshot.delivery_rate_bps = 2_000_000.0;
    candidate.snapshot.product_progress_rate_bps = Some(2_000_000.0);
    candidate.snapshot.app_limited = true;
    candidate.ack_clock_calibration_eligible = true;
    let initial_limit = reliable_ack_clock_calibration_limit_bytes(mux_limits);
    candidate.ack_clock_calibration_credit_limit_bytes = initial_limit;
    candidate.ack_clock_calibration_max_limit_bytes = initial_limit.saturating_mul(4);
    let candidates = [&service, &candidate];
    let lead = ResponseBulkLead {
        key: service.key,
        snapshot: service.snapshot,
        eta_ms: service.eta_ms,
    };
    assert_eq!(
        response_target_unique_owner_admission(
            &candidate,
            &candidates,
            lead,
            None,
            0,
            payload_bytes,
            mux_limits,
        )
        .decision,
        PathAdmissionDecision::Standby,
        "the provisional first-RTT rate remains too slow for ordinary ECF admission"
    );

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), candidate.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        0,
        None,
    )
    .expect("Service remains available when exploration would create an ordering stall");
    assert_eq!(selected.target.key, service.key);
    assert!(selected.ack_clock_calibration_commit.is_none());
}

#[test]
fn tcp_ack_clock_calibration_explores_within_service_reservoir() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(
        0,
        UnderlayProtocol::Tcp,
        1_098.657,
        0,
        16 * 1024 * 1024,
        true,
    );
    service.snapshot.delivery_rate_bps = 18_561_000.0;
    service.snapshot.pacing_rate_bps = 18_561_000.0;
    service.snapshot.srtt_ms = 333.0;
    service.snapshot.min_rtt_ms = 333.0;

    let mut candidate = response_target(
        1,
        UnderlayProtocol::Tcp,
        1_406.704,
        0,
        16 * 1024 * 1024,
        false,
    );
    candidate.snapshot.delivery_rate_bps = 1_007_000.0;
    candidate.snapshot.pacing_rate_bps = 1_007_000.0;
    candidate.snapshot.product_progress_rate_bps = Some(1_007_000.0);
    candidate.snapshot.srtt_ms = 730.287;
    candidate.snapshot.min_rtt_ms = 730.287;
    candidate.snapshot.app_limited = true;
    candidate.ack_clock_calibration_eligible = true;
    let initial_limit = 183_802;
    candidate.ack_clock_calibration_credit_limit_bytes = initial_limit;
    candidate.ack_clock_calibration_max_limit_bytes = 64 * 1024 * 1024;
    let candidates = [&service, &candidate];
    let lead = ResponseBulkLead {
        key: service.key,
        snapshot: service.snapshot,
        eta_ms: service.eta_ms,
    };
    assert_eq!(
        response_target_unique_owner_admission(
            &candidate,
            &candidates,
            lead,
            None,
            0,
            payload_bytes,
            mux_limits,
        )
        .decision,
        PathAdmissionDecision::Standby,
        "the provisional model still cannot claim ordinary ownership"
    );

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), candidate.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        0,
        None,
    )
    .expect("bounded exploration should fit behind the Service reservoir");
    assert_eq!(selected.target.key, candidate.key);
    assert_eq!(selected.admission.role, PathRuntimeRole::Subflow);
    assert!(selected.ack_clock_calibration_commit.is_some());

    candidate.ack_clock_calibration_active = true;
    candidate.ack_clock_calibration_spent_bytes = initial_limit;
    candidate.ack_clock_calibration_credit_limit_bytes = initial_limit.saturating_mul(2);
    let grown = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), candidate.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        0,
        None,
    )
    .expect("a causally authorized stage continues calibration");
    assert_eq!(grown.target.key, candidate.key);
    assert_eq!(
        grown
            .ack_clock_calibration_commit
            .expect("staged calibration commit")
            .limit_bytes,
        initial_limit.saturating_mul(2)
    );

    candidate.ack_clock_calibration_spent_bytes = initial_limit.saturating_mul(2);
    let awaiting_evidence = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), candidate],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        0,
        None,
    )
    .expect("a stage awaiting new ACK evidence returns to Service");
    assert_eq!(awaiting_evidence.target.key, service.key);
    assert!(awaiting_evidence.ack_clock_calibration_commit.is_none());
}

#[test]
fn safe_tcp_calibration_waits_for_repair_carrier_headroom() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service = response_target(0, UnderlayProtocol::Tcp, 5_000.0, 0, 16 * 1024 * 1024, true);
    let mut candidate =
        response_target(1, UnderlayProtocol::Tcp, 100.0, 0, 16 * 1024 * 1024, false);
    candidate.ack_clock_calibration_eligible = true;
    candidate.ack_clock_calibration_credit_limit_bytes = 256 * 1024;
    candidate.ack_clock_calibration_max_limit_bytes = 64 * 1024 * 1024;
    candidate.snapshot.product_bytes_in_flight = 256 * 1024;
    candidate.owner_data_in_flight_bytes = 0;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), candidate],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        0,
        None,
    )
    .expect("Service remains available while RepairData occupies candidate headroom");

    assert_eq!(selected.target.key, service.key);
    assert!(selected.ack_clock_calibration_commit.is_none());
}

#[test]
fn tcp_ack_clock_calibration_retirement_releases_binding_fences() {
    let fixture = response_calibration_dispatch_fixture(8);
    install_slow_fresh_response_calibration(&fixture);
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
    let generation_before = fixture.binding.subflow_state_snapshot().0;

    let plan = plan_response_data_dispatch(&fixture.stream, FlowLane::Throughput, 0, payload_bytes)
        .expect("Service remains available after retiring unsafe exploration");

    assert_eq!(plan.primary_key(), Some(fixture.service));
    assert_ne!(
        fixture.binding.subflow_state_snapshot().0,
        generation_before
    );
    let candidate = fixture
        .binding
        .sender_path_targets(FlowLane::Throughput, payload_bytes)
        .into_iter()
        .find(|target| target.key == fixture.candidate)
        .expect("retired candidate remains attached");
    assert_eq!(candidate.ack_clock_calibration_spent_bytes, 0);
    assert_eq!(candidate.ack_clock_calibration_credit_limit_bytes, 0);
    assert_eq!(candidate.ack_clock_calibration_max_limit_bytes, 0);
    assert!(!candidate.ack_clock_calibration_active);
    assert!(!response_ack_clock_calibration_blocks_generic_owner(
        &candidate
    ));
}

#[test]
fn tcp_ack_clock_calibration_retirement_ignores_repair_only_carrier_debt() {
    let fixture = response_calibration_dispatch_fixture(1);
    install_slow_fresh_response_calibration(&fixture);
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
    let repair = Frame::StreamData {
        stream_id: fixture.stream.stream_id,
        offset: 0,
        flags: StreamFlags::NONE,
        payload: Bytes::from_static(b"repair-only"),
    };
    fixture
        .candidate_commands
        .try_enqueue_stream_ordered_frame(repair.clone(), FlowLane::Throughput)
        .expect("fill the candidate lane with RepairData");
    fixture
        .binding
        .record_repair_flight(fixture.candidate, &repair);

    let plan = plan_response_data_dispatch(
        &fixture.stream,
        FlowLane::Throughput,
        reliable_stream_frame_accounted_bytes(&repair) as u64,
        payload_bytes,
    )
    .expect("RepairData must not preserve a unique-owner calibration fence");

    assert_eq!(plan.primary_key(), Some(fixture.service));
    let candidate = fixture
        .binding
        .sender_path_targets(FlowLane::Throughput, payload_bytes)
        .into_iter()
        .find(|target| target.key == fixture.candidate)
        .expect("candidate remains attached");
    assert_eq!(candidate.owner_data_in_flight_bytes, 0);
    assert!(candidate.snapshot.product_bytes_in_flight > 0);
    assert_eq!(candidate.ack_clock_calibration_credit_limit_bytes, 0);
    assert_eq!(candidate.ack_clock_calibration_max_limit_bytes, 0);
    assert!(!response_ack_clock_calibration_blocks_generic_owner(
        &candidate
    ));
}

#[test]
fn tcp_ack_clock_calibration_retirement_refuses_exact_owner_flight() {
    let fixture = response_calibration_dispatch_fixture(8);
    install_slow_fresh_response_calibration(&fixture);
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
    let candidate = fixture
        .binding
        .sender_path_targets(FlowLane::Throughput, payload_bytes)
        .into_iter()
        .find(|target| target.key == fixture.candidate)
        .expect("fresh calibration candidate");
    fixture.binding.record_owner_flight_for_target(
        &candidate,
        &Frame::StreamData {
            stream_id: fixture.stream.stream_id,
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(b"stale-owner"),
        },
    );

    let plan = plan_response_data_dispatch(&fixture.stream, FlowLane::Throughput, 0, payload_bytes)
        .expect("stale calibration state must fall back without erasing exact flight");

    assert_eq!(plan.primary_key(), Some(fixture.service));
    let candidate = fixture
        .binding
        .sender_path_targets(FlowLane::Throughput, payload_bytes)
        .into_iter()
        .find(|target| target.key == fixture.candidate)
        .expect("candidate remains attached");
    assert!(candidate.ack_clock_calibration_credit_limit_bytes > 0);
    assert!(candidate.ack_clock_calibration_max_limit_bytes > 0);
    assert!(response_ack_clock_calibration_blocks_generic_owner(
        &candidate
    ));
}

#[test]
fn tcp_ack_clock_calibration_retirement_rejects_stale_path_model() {
    let fixture = response_calibration_dispatch_fixture(8);
    install_slow_fresh_response_calibration(&fixture);
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
    let request = response_calibration_retirement_request(&fixture);
    fixture
        .binding
        .set_output_product_model_for_test(fixture.candidate, 500_000_000.0, 10.0);

    assert!(
        !fixture
            .binding
            .try_retire_tcp_ack_clock_calibration(request)
    );
    let candidate = fixture
        .binding
        .sender_path_targets(FlowLane::Throughput, payload_bytes)
        .into_iter()
        .find(|target| target.key == fixture.candidate)
        .expect("candidate remains attached");
    assert!(candidate.ack_clock_calibration_credit_limit_bytes > 0);
}

#[test]
fn tcp_ack_clock_calibration_retirement_rejects_stale_pending_snapshots() {
    let mut fixture = response_calibration_dispatch_fixture(8);
    install_slow_fresh_response_calibration(&fixture);
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());

    let stale_candidate = response_calibration_retirement_request(&fixture);
    fixture
        .candidate_commands
        .try_enqueue_stream_ordered_frame(
            Frame::StreamData {
                stream_id: fixture.stream.stream_id,
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"candidate-pending"),
            },
            FlowLane::Throughput,
        )
        .expect("change candidate pending bytes");
    let candidate_command = try_recv_reliable_path_command(&mut fixture.candidate_receivers)
        .expect("drain candidate queue without releasing pending bytes");
    let candidate_pending_bytes = reliable_path_command_pending_bytes(&candidate_command);
    assert!(
        !fixture
            .binding
            .try_retire_tcp_ack_clock_calibration(stale_candidate)
    );
    fixture
        .candidate_receivers
        .release_pending_command_bytes(candidate_pending_bytes);

    let stale_service = response_calibration_retirement_request(&fixture);
    let service = fixture
        .binding
        .sender_path_targets(FlowLane::Throughput, payload_bytes)
        .into_iter()
        .find(|target| target.key == fixture.service)
        .expect("Service target");
    service
        .commands
        .try_enqueue_stream_ordered_frame(
            Frame::StreamData {
                stream_id: fixture.stream.stream_id,
                offset: payload_bytes as u64,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"service-pending"),
            },
            FlowLane::Throughput,
        )
        .expect("change Service pending bytes");
    let service_command = try_recv_reliable_path_command(&mut fixture.service_receivers)
        .expect("drain Service queue without releasing pending bytes");
    let service_pending_bytes = reliable_path_command_pending_bytes(&service_command);
    assert!(
        !fixture
            .binding
            .try_retire_tcp_ack_clock_calibration(stale_service)
    );
    fixture
        .service_receivers
        .release_pending_command_bytes(service_pending_bytes);

    assert!(
        fixture.binding.try_retire_tcp_ack_clock_calibration(
            response_calibration_retirement_request(&fixture)
        )
    );
}

#[test]
fn tcp_response_calibration_does_not_double_count_pending_owner_flight() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service = response_target(0, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, true);
    let mut candidate =
        response_target(1, UnderlayProtocol::Tcp, 500.0, 0, 16 * 1024 * 1024, false);
    let (commands, _receivers) = reliable_path_command_channels(8);
    candidate.commands = commands;
    let initial_limit = reliable_ack_clock_calibration_limit_bytes(mux_limits);
    let committed = initial_limit - payload_bytes as u64;
    candidate.snapshot.product_bytes_in_flight = committed;
    candidate.ack_clock_calibration_eligible = true;
    candidate.ack_clock_calibration_active = true;
    candidate.ack_clock_calibration_spent_bytes = committed;
    candidate.ack_clock_calibration_credit_limit_bytes = initial_limit;
    candidate.ack_clock_calibration_max_limit_bytes =
        reliable_ack_clock_calibration_ceiling_bytes(mux_limits);
    candidate
        .commands
        .try_enqueue_stream_ordered_frame(
            Frame::StreamData {
                stream_id: StreamId(991),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from(vec![0x5a; committed as usize]),
            },
            FlowLane::Throughput,
        )
        .expect("mirror the product flight in the carrier queue");
    assert_eq!(candidate.commands.pending_bytes(), committed);
    assert_eq!(
        response_target_assigned_product_bytes(&candidate),
        committed
    );
    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), candidate.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        0,
        None,
    )
    .expect("overlapping flight and queue views count as one debt");

    assert_eq!(selected.target.key, candidate.key);
    assert_eq!(selected.admission.role, PathRuntimeRole::Subflow);
    assert_eq!(
        selected
            .ack_clock_calibration_commit
            .expect("calibration commit")
            .limit_bytes,
        initial_limit
    );
}

#[test]
fn tcp_response_calibration_does_not_double_count_global_ordered_tail() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service = response_target(0, UnderlayProtocol::Tcp, 5.0, 0, 64 * 1024 * 1024, true);
    let mut candidate =
        response_target(1, UnderlayProtocol::Tcp, 500.0, 0, 64 * 1024 * 1024, false);
    let ceiling = reliable_ack_clock_calibration_ceiling_bytes(mux_limits);
    let committed = ceiling - payload_bytes as u64;
    candidate.snapshot.product_bytes_in_flight = committed;
    candidate.ack_clock_calibration_eligible = true;
    candidate.ack_clock_calibration_active = true;
    candidate.ack_clock_calibration_spent_bytes = committed;
    candidate.ack_clock_calibration_credit_limit_bytes = ceiling;
    candidate.ack_clock_calibration_max_limit_bytes = ceiling;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), candidate.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        committed as usize,
        None,
    )
    .expect("the global tail and candidate flight are the same product debt");

    assert_eq!(selected.target.key, candidate.key);
    assert_eq!(
        selected
            .ack_clock_calibration_commit
            .expect("calibration commit")
            .limit_bytes,
        ceiling
    );
}

#[tokio::test]
async fn tcp_response_calibration_dispatch_restores_credit_after_exact_remainder() {
    let mux_limits = MuxLimits::default();
    let normal_payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut fixture = response_calibration_dispatch_fixture(8);
    let mut sender = ServerResponseSenderService::new(SessionId(191), fixture.stream.stream_id);
    sender.enqueue_data_for_lane(
        Bytes::from(vec![0x5a; normal_payload_bytes]),
        FlowLane::Throughput,
    );
    let mut send_stream = ReliableSendStream::new_with_initial_max_offset(
        fixture.stream.stream_id,
        mux_limits,
        u64::MAX,
    );

    let dispatch = sender
        .dispatch_next(
            &fixture.stream,
            &mut send_stream,
            FlowLane::Throughput,
            mux_limits,
        )
        .expect("the exact residual remains spendable");

    assert_eq!(dispatch.selected_path, Some(fixture.candidate));
    assert_eq!(dispatch.payload_bytes, 4032);
    assert_eq!(fixture.binding.ordered_data_owner(), Some(fixture.service));
    assert!(try_recv_reliable_path_command(&mut fixture.service_receivers).is_none());
    assert!(matches!(
        try_recv_reliable_path_command(&mut fixture.candidate_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { payload, .. }))
            if payload.len() == 4032
    ));
    let target = fixture
        .binding
        .sender_path_targets(FlowLane::Throughput, normal_payload_bytes)
        .into_iter()
        .find(|target| target.key == fixture.candidate)
        .expect("calibration target");
    assert_eq!(
        target.ack_clock_calibration_spent_bytes,
        target.ack_clock_calibration_credit_limit_bytes
    );
    assert_eq!(sender.data_bytes(), normal_payload_bytes - 4032);

    fixture
        .binding
        .release_normalized_acked_ranges(&[OffsetRange {
            start: 0,
            end: 4032,
        }]);
    let drained = fixture
        .binding
        .sender_path_targets(FlowLane::Throughput, normal_payload_bytes)
        .into_iter()
        .find(|target| target.key == fixture.candidate)
        .expect("drained calibration target");
    assert!(drained.ack_clock_calibration_active);
    assert!(
        drained.ack_clock_calibration_credit_limit_bytes
            > drained.ack_clock_calibration_spent_bytes,
        "exact drain restores bounded credit when no representative strict window was reachable"
    );
    assert!(
        drained.ack_clock_calibration_credit_limit_bytes
            <= drained.ack_clock_calibration_max_limit_bytes
    );
}

#[tokio::test]
async fn active_tcp_calibration_continues_after_another_response_flow_closes() {
    let mux_limits = MuxLimits::default();
    let normal_payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut fixture = response_calibration_dispatch_fixture(8);
    drop(fixture.second_binding.take());
    assert_eq!(
        fixture
            .binding
            .lane_generation_and_active_response_flows()
            .1,
        1
    );
    let mut sender = ServerResponseSenderService::new(SessionId(191), fixture.stream.stream_id);
    sender.enqueue_data_for_lane(
        Bytes::from(vec![0x5a; normal_payload_bytes]),
        FlowLane::Throughput,
    );
    let mut send_stream = ReliableSendStream::new_with_initial_max_offset(
        fixture.stream.stream_id,
        mux_limits,
        u64::MAX,
    );

    let dispatch = sender
        .dispatch_next(
            &fixture.stream,
            &mut send_stream,
            FlowLane::Throughput,
            mux_limits,
        )
        .expect("an exact active calibration may finish after the start gate closes");

    assert_eq!(dispatch.selected_path, Some(fixture.candidate));
    assert_eq!(dispatch.payload_bytes, 4032);
    assert!(matches!(
        try_recv_reliable_path_command(&mut fixture.candidate_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { payload, .. }))
            if payload.len() == 4032
    ));
}

#[tokio::test]
async fn tcp_response_calibration_dispatch_treats_pending_flight_as_one_debt() {
    let mux_limits = MuxLimits::default();
    let normal_payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let stage_limit = reliable_ack_clock_calibration_limit_bytes(mux_limits);
    let committed = stage_limit - 4032;
    let mut fixture = response_calibration_dispatch_fixture(8);
    let overlap = Frame::StreamData {
        stream_id: fixture.stream.stream_id,
        offset: normal_payload_bytes as u64,
        flags: StreamFlags::NONE,
        payload: Bytes::from(vec![0x5a; committed as usize]),
    };
    fixture
        .binding
        .record_owner_flight(fixture.candidate, &overlap);
    fixture
        .candidate_commands
        .try_enqueue_stream_ordered_frame(overlap, FlowLane::Throughput)
        .expect("mirror the assigned product flight in the carrier queue");

    let mut sender = ServerResponseSenderService::new(SessionId(191), fixture.stream.stream_id);
    sender.enqueue_data_for_lane(
        Bytes::from(vec![0x5a; normal_payload_bytes]),
        FlowLane::Throughput,
    );
    let mut send_stream = ReliableSendStream::new_with_initial_max_offset(
        fixture.stream.stream_id,
        mux_limits,
        u64::MAX,
    );
    let dispatch = sender
        .dispatch_next(
            &fixture.stream,
            &mut send_stream,
            FlowLane::Throughput,
            mux_limits,
        )
        .expect("overlapping ledger and queue views leave the residual spendable");

    assert_eq!(dispatch.selected_path, Some(fixture.candidate));
    assert_eq!(dispatch.payload_bytes, 4032);
    assert!(matches!(
        try_recv_reliable_path_command(&mut fixture.candidate_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { payload, .. }))
            if payload.len() == committed as usize
    ));
    assert!(matches!(
        try_recv_reliable_path_command(&mut fixture.candidate_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { payload, .. }))
            if payload.len() == 4032
    ));
    let target = fixture
        .binding
        .sender_path_targets(FlowLane::Throughput, normal_payload_bytes)
        .into_iter()
        .find(|target| target.key == fixture.candidate)
        .expect("calibration target");
    assert_eq!(
        target.ack_clock_calibration_spent_bytes,
        target.ack_clock_calibration_credit_limit_bytes
    );
}

#[tokio::test]
async fn blocked_tcp_calibration_remainder_keeps_normal_service_chunk() {
    let mux_limits = MuxLimits::default();
    let normal_payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut fixture = response_calibration_dispatch_fixture(1);
    fixture
        .candidate_commands
        .try_enqueue_stream_ordered_frame(
            Frame::StreamData {
                stream_id: fixture.stream.stream_id,
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"blocked"),
            },
            FlowLane::Throughput,
        )
        .expect("fill exact calibration candidate queue");
    let mut sender = ServerResponseSenderService::new(SessionId(191), fixture.stream.stream_id);
    sender.enqueue_data_for_lane(
        Bytes::from(vec![0x5a; normal_payload_bytes]),
        FlowLane::Throughput,
    );
    let mut send_stream = ReliableSendStream::new_with_initial_max_offset(
        fixture.stream.stream_id,
        mux_limits,
        u64::MAX,
    );

    let dispatch = sender
        .dispatch_next(
            &fixture.stream,
            &mut send_stream,
            FlowLane::Throughput,
            mux_limits,
        )
        .expect("blocked calibration falls back to normal Service emission");

    assert_eq!(dispatch.selected_path, Some(fixture.service));
    assert_eq!(dispatch.payload_bytes, normal_payload_bytes);
    assert!(matches!(
        try_recv_reliable_path_command(&mut fixture.service_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { payload, .. }))
            if payload.len() == normal_payload_bytes
    ));
    assert_eq!(
        fixture
            .binding
            .active_tcp_ack_clock_calibration_remaining_bytes(),
        Some(4032),
        "Service fallback must not spend or repeatedly fragment the candidate's residual credit"
    );
}

#[test]
fn blocked_active_ack_clock_candidate_does_not_select_another_calibration_owner() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service = response_target(0, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, true);
    let initial_limit = reliable_ack_clock_calibration_limit_bytes(mux_limits);

    let mut active_candidate =
        response_target(1, UnderlayProtocol::Tcp, 500.0, 0, 16 * 1024 * 1024, false);
    let (blocked_commands, _blocked_receivers) = reliable_path_command_channels(1);
    blocked_commands
        .try_enqueue_stream_ordered_frame(
            Frame::StreamData {
                stream_id: StreamId(901),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"x"),
            },
            FlowLane::Throughput,
        )
        .expect("fill active calibration candidate queue");
    active_candidate.commands = blocked_commands;
    active_candidate.ack_clock_calibration_eligible = true;
    active_candidate.ack_clock_calibration_active = true;
    active_candidate.ack_clock_calibration_credit_limit_bytes = initial_limit;
    active_candidate.ack_clock_calibration_max_limit_bytes = initial_limit.saturating_mul(2);

    let mut other_candidate = response_target(
        2,
        UnderlayProtocol::Tcp,
        1_500.0,
        0,
        16 * 1024 * 1024,
        false,
    );
    other_candidate.snapshot.delivery_rate_bps = 2_000_000.0;
    other_candidate.snapshot.product_progress_rate_bps = Some(2_000_000.0);
    other_candidate.snapshot.app_limited = true;
    other_candidate.ack_clock_calibration_eligible = true;
    other_candidate.ack_clock_calibration_credit_limit_bytes = initial_limit;
    other_candidate.ack_clock_calibration_max_limit_bytes = initial_limit.saturating_mul(2);

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), active_candidate, other_candidate],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        0,
        None,
    )
    .expect("Service remains feedable while the active calibration path is blocked");
    assert_eq!(selected.target.key, service.key);
    assert!(selected.ack_clock_calibration_commit.is_none());
}

#[test]
fn exhausted_active_calibration_cannot_bypass_saturated_service_via_generic_subflow() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, true);
    let (blocked_service_commands, _blocked_service_receivers) = reliable_path_command_channels(1);
    blocked_service_commands
        .try_enqueue_stream_ordered_frame(
            Frame::StreamData {
                stream_id: StreamId(902),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"x"),
            },
            FlowLane::Throughput,
        )
        .expect("fill Service queue");
    service.commands = blocked_service_commands;

    let initial_limit = reliable_ack_clock_calibration_limit_bytes(mux_limits);
    let mut candidate = response_target(1, UnderlayProtocol::Tcp, 1.0, 0, 16 * 1024 * 1024, false);
    candidate.ack_clock_calibration_eligible = true;
    candidate.ack_clock_calibration_active = true;
    candidate.ack_clock_calibration_spent_bytes = initial_limit;
    candidate.ack_clock_calibration_credit_limit_bytes = initial_limit;
    candidate.ack_clock_calibration_max_limit_bytes = initial_limit.saturating_mul(2);

    assert!(
        select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), candidate],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service.key),
            0,
            None,
        )
        .is_none(),
        "generic Subflow selection must not bypass staged credit while Service is blocked"
    );
}

#[test]
fn proven_active_calibration_cannot_reenter_generic_ownership_before_drain() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, true);
    let (blocked_service_commands, _blocked_service_receivers) = reliable_path_command_channels(1);
    blocked_service_commands
        .try_enqueue_stream_ordered_frame(
            Frame::StreamData {
                stream_id: StreamId(903),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"x"),
            },
            FlowLane::Throughput,
        )
        .expect("fill Service queue");
    service.commands = blocked_service_commands;

    let initial_limit = reliable_ack_clock_calibration_limit_bytes(mux_limits);
    let mut candidate = response_target(1, UnderlayProtocol::Tcp, 1.0, 0, 16 * 1024 * 1024, false);
    candidate.ack_clock_calibration_eligible = true;
    candidate.ack_clock_calibration_active = true;
    candidate.ack_clock_calibration_proven = true;
    candidate.ack_clock_calibration_spent_bytes = initial_limit;
    candidate.ack_clock_calibration_credit_limit_bytes = initial_limit;
    candidate.ack_clock_calibration_max_limit_bytes = initial_limit;

    assert!(response_ack_clock_calibration_blocks_generic_owner(
        &candidate
    ));
    assert!(
        select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), candidate.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service.key),
            0,
            None,
        )
        .is_none(),
        "the exact active fence must drain before proven capacity becomes ordinary ownership"
    );

    candidate.ack_clock_calibration_active = false;
    assert!(!response_ack_clock_calibration_blocks_generic_owner(
        &candidate
    ));
}

#[test]
fn closed_active_calibration_drain_fence_blocks_next_startup_owner() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, true);
    let (service_commands, _service_receivers) = reliable_path_command_channels(8);
    service.commands = service_commands;

    let initial_limit = reliable_ack_clock_calibration_limit_bytes(mux_limits);
    let mut draining = response_target(1, UnderlayProtocol::Tcp, 500.0, 0, 16 * 1024 * 1024, false);
    let (closed_commands, closed_receivers) = reliable_path_command_channels(8);
    drop(closed_receivers);
    draining.commands = closed_commands;
    draining.ack_clock_calibration_eligible = true;
    draining.ack_clock_calibration_active = true;
    draining.ack_clock_calibration_spent_bytes = initial_limit;
    draining.ack_clock_calibration_credit_limit_bytes = initial_limit;
    draining.ack_clock_calibration_max_limit_bytes = initial_limit.saturating_mul(2);
    assert!(draining.commands.is_closed());

    let mut next_startup =
        response_target(2, UnderlayProtocol::Tcp, 1.0, 0, 16 * 1024 * 1024, false);
    let (startup_commands, _startup_receivers) = reliable_path_command_channels(8);
    next_startup.commands = startup_commands;
    next_startup.has_bulk_rate_evidence = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), draining, next_startup],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        0,
        None,
    )
    .expect("Service remains available during exact-flight drain");
    assert_eq!(selected.target.key, service.key);
    assert!(selected.subflow_set_commit.is_none());
}

#[test]
fn measured_same_family_alternate_is_subflow_when_service_is_not_feedable() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Tcp, 25.0, 0, 16 * 1024 * 1024, true);
    let service_envelope =
        bulk_active_service_product_envelope_bytes(service.snapshot, payload_bytes, mux_limits);
    service.snapshot.product_bytes_in_flight = service_envelope;
    service.snapshot.queue_bytes = payload_bytes as u64;
    let measured_subflow =
        response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), measured_subflow.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        0,
        None,
    )
    .expect("measured same-family path should remain an admissible Subflow when Service is not feedable");

    assert_eq!(selected.target.key, measured_subflow.key);
    assert_eq!(
        selected.admission.role,
        PathRuntimeRole::Subflow,
        "additional same-family owner bytes must be labeled Subflow, not Service"
    );
    assert!(
        selected.subflow_set_commit.is_some(),
        "Subflow OwnerData must be committed through the Subflow admission ledger"
    );
}

#[test]
fn saturated_service_may_admit_one_startup_same_underlay_subflow_owner() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(
        0,
        UnderlayProtocol::Tcp,
        25.0,
        mux_limits.max_path_flight_bytes as u64,
        16 * 1024 * 1024,
        true,
    );
    service.snapshot.product_progress_rate_bps = Some(180_000_000.0);
    service.has_sender_evidence = true;
    service.has_bulk_rate_evidence = true;
    service.snapshot.active_flows = 2;
    let mut startup_subflow =
        response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);
    startup_subflow.has_sender_evidence = true;
    startup_subflow.has_bulk_rate_evidence = false;
    startup_subflow.snapshot.product_queue_bytes = mux_limits.max_path_flight_bytes as u64;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), startup_subflow.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        0,
        None,
    );

    let selected =
        selected.expect("startup same-underlay Subflow should receive one owner quantum");
    assert_eq!(selected.target.key, startup_subflow.key);
    assert_eq!(selected.admission.role, PathRuntimeRole::Subflow);
    assert!(
        selected
            .subflow_set_commit
            .is_some_and(|commit| commit.input.startup_owner_allowed),
        "sender evidence permits only explicit bounded startup Subflow admission"
    );
}

#[test]
fn bulk_only_live_tcp_service_tail_admits_bounded_same_underlay_startup_sampling() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Tcp, 25.0, 0, 16 * 1024 * 1024, true);
    service.snapshot.product_progress_rate_bps = Some(180_000_000.0);
    service.has_sender_evidence = true;
    service.has_bulk_rate_evidence = true;
    service.snapshot.active_flows = 2;
    let mut startup_subflow =
        response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);
    startup_subflow.has_sender_evidence = true;
    startup_subflow.has_bulk_rate_evidence = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), startup_subflow.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        payload_bytes,
        None,
    )
    .expect("bounded TCP startup sampling should remain dispatchable behind a live Service suffix");

    assert_eq!(selected.target.key, startup_subflow.key);
    assert_eq!(selected.admission.role, PathRuntimeRole::Subflow);
    assert!(
        selected
            .subflow_set_commit
            .is_some_and(|commit| commit.input.startup_owner_allowed),
        "TCP startup sampling must be explicit and ledger-bounded"
    );
}

#[test]
fn quic_service_uses_bounded_startup_when_no_measured_subflow_exists() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Udp, 25.0, 0, 16 * 1024 * 1024, true);
    service.snapshot.product_progress_rate_bps = Some(180_000_000.0);
    service.has_sender_evidence = true;
    service.has_bulk_rate_evidence = true;
    service.snapshot.active_flows = 2;
    let mut startup_subflow =
        response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    startup_subflow.has_sender_evidence = true;
    startup_subflow.has_bulk_rate_evidence = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), startup_subflow.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        payload_bytes,
        None,
    )
    .expect("one unmeasured QUIC path should receive bounded startup work");

    assert_eq!(selected.target.key, startup_subflow.key);
    assert_eq!(selected.admission.role, PathRuntimeRole::Subflow);
    assert!(
        selected
            .subflow_set_commit
            .is_some_and(|commit| commit.input.startup_owner_allowed)
    );
}

#[test]
fn sole_quic_service_does_not_sample_an_equally_loaded_path() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Udp, 25.0, 0, 16 * 1024 * 1024, true);
    service.snapshot.product_progress_rate_bps = Some(180_000_000.0);
    service.snapshot.active_flows = 1;
    let mut validation = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    validation.has_bulk_rate_evidence = false;
    validation.snapshot.active_flows = 1;

    let selected = select_response_sender_data_target_with_ordered_debt_inner(
        &[service.clone(), validation],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        payload_bytes,
        None,
        true,
    )
    .expect("the equally loaded Service should remain dispatchable");

    assert_eq!(selected.target.key, service.key);
    assert_eq!(selected.admission.role, PathRuntimeRole::Service);
    assert!(selected.subflow_set_commit.is_none());
}

#[test]
fn latency_pressure_keeps_unmeasured_validation_path_out_of_owner_sampling() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Udp, 25.0, 0, 16 * 1024 * 1024, true);
    service.snapshot.product_progress_rate_bps = Some(180_000_000.0);
    service.snapshot.session_active_latency_sensitive_flows = 1;
    let mut validation = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    validation.has_bulk_rate_evidence = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), validation.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        payload_bytes,
        None,
    )
    .expect("the Service path should remain dispatchable under latency pressure");

    assert_eq!(selected.target.key, service.key);
    assert_eq!(selected.admission.role, PathRuntimeRole::Service);
    assert!(selected.subflow_set_commit.is_none());
}

#[test]
fn repair_attachment_never_receives_startup_owner_sampling() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Tcp, 25.0, 0, 16 * 1024 * 1024, true);
    service.snapshot.product_progress_rate_bps = Some(180_000_000.0);
    let mut repair = response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);
    repair.attachment_role = StreamOpenRole::Repair;
    repair.has_bulk_rate_evidence = true;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), repair],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        payload_bytes,
        None,
    )
    .expect("the Service path should remain dispatchable with a proven Repair attachment");

    assert_eq!(selected.target.key, service.key);
    assert_eq!(selected.admission.role, PathRuntimeRole::Service);
}

#[test]
fn exact_startup_owner_continues_lower_frontier_within_multi_flow_cap() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let startup_credit =
        usize::try_from(reliable_subflow_startup_sample_limit_bytes(mux_limits)).unwrap();
    assert_eq!(startup_credit % payload_bytes, 0);

    let mut service = response_target(0, UnderlayProtocol::Udp, 25.0, 0, 16 * 1024 * 1024, true);
    service.snapshot.active_flows = 2;
    service.has_bulk_rate_evidence = true;
    let mut startup_owner =
        response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    startup_owner.has_bulk_rate_evidence = false;

    let first = select_response_sender_data_target_with_ordered_debt_inner(
        &[service.clone(), startup_owner.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        0,
        None,
        true,
    )
    .expect("the first bounded startup quantum should be admitted");
    let input = first
        .subflow_set_commit
        .expect("startup admission must carry the exact epoch commit")
        .input;
    let mut partial_epoch = FlowSubflowSet::new(0, service.key, startup_credit, 0, Duration::ZERO);
    assert_eq!(
        partial_epoch.admit_subflow_owner(input).decision,
        PathAdmissionDecision::AdmitSubflow
    );
    startup_owner.snapshot.product_bytes_in_flight = payload_bytes as u64;
    startup_owner.owner_data_in_flight_bytes = payload_bytes as u64;
    let startup_lower_flight = [CarrierPathFlightDebt {
        key: startup_owner.key,
        bytes: payload_bytes as u64,
    }];

    assert!(
        select_response_sender_data_target_with_ordered_debt_inner(
            &[service.clone(), startup_owner.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &startup_lower_flight,
            Some(service.key),
            payload_bytes,
            Some(&partial_epoch),
            false,
        )
        .is_none(),
        "an exact startup owner cannot bypass a disabled startup policy"
    );

    let continued = select_response_sender_data_target_with_ordered_debt_inner(
        &[service.clone(), startup_owner.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &startup_lower_flight,
        Some(service.key),
        payload_bytes,
        Some(&partial_epoch),
        true,
    )
    .expect("the exact startup owner should continue its own lower frontier");
    assert_eq!(continued.target.key, startup_owner.key);
    assert_eq!(continued.admission.role, PathRuntimeRole::Subflow);

    let mut other_unmeasured =
        response_target(2, UnderlayProtocol::Udp, 4.0, 0, 16 * 1024 * 1024, false);
    other_unmeasured.has_bulk_rate_evidence = false;
    let other_lower_flight = [CarrierPathFlightDebt {
        key: other_unmeasured.key,
        bytes: payload_bytes as u64,
    }];
    assert!(
        select_response_sender_data_target_with_ordered_debt_inner(
            &[service.clone(), startup_owner.clone(), other_unmeasured],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &other_lower_flight,
            Some(service.key),
            payload_bytes,
            Some(&partial_epoch),
            true,
        )
        .is_none(),
        "a different unmeasured lower owner cannot borrow the startup epoch"
    );

    let mut exhausted_epoch = partial_epoch;
    for _ in 1..(startup_credit / payload_bytes) {
        assert_eq!(
            exhausted_epoch.admit_subflow_owner(input).decision,
            PathAdmissionDecision::AdmitSubflow
        );
    }
    startup_owner.snapshot.product_bytes_in_flight = startup_credit as u64;
    startup_owner.owner_data_in_flight_bytes = startup_credit as u64;
    assert!(
        select_response_sender_data_target_with_ordered_debt_inner(
            &[service.clone(), startup_owner.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &startup_lower_flight,
            Some(service.key),
            startup_credit,
            Some(&exhausted_epoch),
            true,
        )
        .is_none(),
        "an exhausted unproven startup owner must wait for its lower ACK hole"
    );

    let after_ack = select_response_sender_data_target_with_ordered_debt_inner(
        &[service.clone(), startup_owner],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        startup_credit,
        Some(&exhausted_epoch),
        true,
    )
    .expect("Service should resume after the exhausted startup hole clears");
    assert_eq!(after_ack.target.key, service.key);
    assert_eq!(after_ack.admission.role, PathRuntimeRole::Service);
}

#[test]
fn active_response_flow_may_start_one_bounded_same_family_sample() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Udp, 25.0, 0, 16 * 1024 * 1024, true);
    service.snapshot.product_progress_rate_bps = Some(180_000_000.0);
    service.snapshot.active_flows = 1;
    let service_key = service.key;
    let mut validation = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    validation.has_bulk_rate_evidence = false;

    let no_active_work = select_response_sender_data_target_with_ordered_debt_inner(
        &[service.clone(), validation.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service_key),
        0,
        None,
        false,
    )
    .expect("the Service remains dispatchable while discovery is dormant");
    assert_eq!(no_active_work.target.key, service.key);
    assert_eq!(no_active_work.admission.role, PathRuntimeRole::Service);
    assert!(no_active_work.subflow_set_commit.is_none());

    let active_response = select_response_sender_data_target_with_ordered_debt_inner(
        &[service, validation.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service_key),
        0,
        None,
        true,
    )
    .expect("one active response may spend the bounded startup sample");
    assert_eq!(active_response.target.key, validation.key);
    assert!(
        active_response
            .subflow_set_commit
            .is_some_and(|commit| commit.input.startup_owner_allowed)
    );
}

#[test]
fn startup_sample_cap_returns_dispatch_to_service() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let startup_credit =
        usize::try_from(reliable_subflow_startup_sample_limit_bytes(mux_limits)).unwrap();
    assert_eq!(startup_credit % payload_bytes, 0);

    let mut service = response_target(0, UnderlayProtocol::Udp, 25.0, 0, 16 * 1024 * 1024, true);
    service.snapshot.product_progress_rate_bps = Some(180_000_000.0);
    service.snapshot.active_flows = 2;
    let mut validation = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    validation.has_bulk_rate_evidence = false;
    let candidates = [&service, &validation];
    let lead = ResponseBulkLead {
        key: service.key,
        snapshot: service.snapshot,
        eta_ms: service.eta_ms,
    };
    let outcome = response_target_unique_owner_admission_with_epoch(
        &validation,
        &candidates,
        lead,
        None,
        Some(service.key),
        0,
        ResponseOrderedTail::new(Some(service.key), payload_bytes).for_candidate(validation.key),
        payload_bytes,
        mux_limits,
        None,
        true,
        false,
    );
    let input = outcome
        .subflow_set_commit
        .expect("first sample quantum should be admitted")
        .input;
    let mut epoch = FlowSubflowSet::new(0, service.key, startup_credit, 0, Duration::ZERO);
    for _ in 0..(startup_credit / payload_bytes) {
        assert_eq!(
            epoch.admit_subflow_owner(input).decision,
            PathAdmissionDecision::AdmitSubflow
        );
    }

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), validation],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        payload_bytes,
        Some(&epoch),
    )
    .expect("Service should resume once startup sampling credit is exhausted");

    assert_eq!(selected.target.key, service.key);
    assert_eq!(selected.admission.role, PathRuntimeRole::Service);
    assert!(selected.subflow_set_commit.is_none());
}

#[test]
fn feedable_service_precedes_measured_subflow_under_bounded_tail_debt() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, true);
    service.has_sender_evidence = true;
    service.has_bulk_rate_evidence = true;
    let mut measured_subflow =
        response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);
    measured_subflow.has_sender_evidence = true;
    measured_subflow.has_bulk_rate_evidence = true;
    measured_subflow.snapshot.app_limited = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), measured_subflow.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        payload_bytes.saturating_mul(2),
        None,
    )
    .expect("feedable Service should remain selected under bounded tail debt");

    assert_eq!(selected.target.key, service.key);
    assert_eq!(
        selected.admission.role,
        PathRuntimeRole::Service,
        "measured Subflow remains overflow while Service has capacity"
    );
}

#[test]
fn response_owner_tail_guard_keeps_service_owner_feedable_under_pressure() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let owner = response_target(0, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, true);
    let alternate = response_target(1, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, false);
    let owner_key = owner.key;
    let owner_tail_guard_bytes = payload_bytes.saturating_mul(2);

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[owner, alternate],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(owner_key),
        owner_tail_guard_bytes,
        None,
    )
    .expect("live Service owner must remain feedable under contiguous owner-tail guard");

    assert_eq!(
        selected.target.key, owner_key,
        "contiguous owner-tail guard blocks alternates but must not starve the current Service owner"
    );
    assert_eq!(selected.admission.role, PathRuntimeRole::Service);
}

#[test]
fn response_owner_tail_guard_uses_measured_same_underlay_when_service_queue_is_full() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut owner = response_target(0, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, true);
    let alternate = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    let (owner_commands, _owner_rx) = reliable_path_command_channels(1);
    owner_commands
        .try_enqueue_admitted_frame(
            Frame::StreamData {
                stream_id: StreamId(99),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"queued"),
            },
            FlowLane::Throughput,
        )
        .expect("seed full owner data queue");
    owner.commands = owner_commands;

    let owner_tail_guard_bytes = payload_bytes.saturating_mul(2);

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[owner.clone(), alternate.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(owner.key),
        owner_tail_guard_bytes,
        None,
    );
    let selected =
        selected.expect("measured same-underlay Subflow should remain eligible under tail debt");
    assert_eq!(selected.target.key, alternate.key);
    assert_eq!(
        selected.admission.role,
        PathRuntimeRole::Subflow,
        "queue backpressure on Service does not promote a new Service; it admits a measured same-underlay Subflow"
    );
}

#[test]
fn ordered_owner_debt_admits_measured_same_underlay_subflow_when_service_is_backpressured() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, true);
    let (service_commands, _service_rx) = reliable_path_command_channels(1);
    service_commands
        .try_enqueue_admitted_frame(
            Frame::StreamData {
                stream_id: StreamId(199),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"queued"),
            },
            FlowLane::Throughput,
        )
        .expect("seed full stale Service data queue");
    service.commands = service_commands;
    let survivor = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    let owner_tail_guard_bytes = payload_bytes.saturating_mul(2);

    let selected = select_response_sender_data_target_with_ordered_debt_inner(
        &[service.clone(), survivor.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        owner_tail_guard_bytes,
        None,
        true,
    );

    let selected =
        selected.expect("measured same-underlay Subflow should pass tail-debt admission");
    assert_eq!(selected.target.key, survivor.key);
    assert_eq!(
        selected.admission.role,
        PathRuntimeRole::Subflow,
        "queue backpressure on a live Service owner is not Service failure; measured same-underlay work remains Subflow OwnerData"
    );
}

#[test]
fn ordered_owner_debt_keeps_live_service_owner_when_it_has_capacity() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Tcp, 333.0, 0, 16 * 1024 * 1024, true);
    service.has_sender_evidence = true;
    service.has_bulk_rate_evidence = true;
    service.snapshot.product_progress_rate_bps = Some(1_121_000.0);
    let survivor = response_target(1, UnderlayProtocol::Tcp, 712.0, 0, 16 * 1024 * 1024, false);
    let owner_tail_guard_bytes = payload_bytes.saturating_mul(58);

    let selected = select_response_sender_data_target_with_ordered_debt_inner(
        &[service.clone(), survivor],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        owner_tail_guard_bytes,
        None,
        true,
    )
    .expect("ordered-owner debt must not suppress a live Service owner with emission credit");

    assert_eq!(
        selected.target.key, service.key,
        "ordered-owner debt must not eject a live owner and create no_admissible_lead"
    );
    assert_eq!(selected.admission.role, PathRuntimeRole::Service);
}

#[test]
fn unresolved_ordered_owner_debt_does_not_grant_owner_bytes_to_unmeasured_survivor() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut stale_service =
        response_target(0, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, true);
    let (service_commands, _service_rx) = reliable_path_command_channels(1);
    service_commands
        .try_enqueue_admitted_frame(
            Frame::StreamData {
                stream_id: StreamId(200),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"queued"),
            },
            FlowLane::Throughput,
        )
        .expect("seed full stale Service data queue");
    stale_service.commands = service_commands;
    let mut proof_only = response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);
    proof_only.has_sender_evidence = true;
    proof_only.has_bulk_rate_evidence = false;
    let owner_tail_guard_bytes = payload_bytes.saturating_mul(2);

    let selected = select_response_sender_data_target_with_ordered_debt_inner(
        &[stale_service.clone(), proof_only],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(stale_service.key),
        owner_tail_guard_bytes,
        None,
        true,
    );

    assert!(
        selected.is_none(),
        "ordered-owner debt is not a proof shortcut; an unmeasured survivor remains Probe/Standby until path-scoped bulk evidence exists"
    );
}

#[test]
fn unresolved_ordered_owner_debt_blocks_active_liveness_survivor() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let stale_owner = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(2),
    };
    let mut active_validation =
        response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, true);
    active_validation.has_sender_evidence = true;
    active_validation.has_bulk_rate_evidence = false;
    let owner_tail_guard_bytes = payload_bytes.saturating_mul(2);

    let selected = select_response_sender_data_target_with_ordered_debt_inner(
        &[active_validation],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(stale_owner),
        owner_tail_guard_bytes,
        None,
        true,
    );

    assert!(
        selected.is_none(),
        "unresolved prior Service bytes block active validation/liveness from becoming Service OwnerData"
    );
}

#[test]
fn clear_frontier_stale_owner_hint_does_not_block_liveness_service_failover() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut stale_owner =
        response_target(2, UnderlayProtocol::Tcp, 500.0, 0, 16 * 1024 * 1024, false);
    stale_owner.has_sender_evidence = true;
    stale_owner.has_bulk_rate_evidence = false;
    let mut survivor = response_target(3, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, false);
    survivor.has_sender_evidence = true;
    survivor.has_bulk_rate_evidence = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[stale_owner.clone(), survivor.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(stale_owner.key),
        0,
        None,
    )
    .expect("with no active Service and a clear frontier, sender-evidence survivors may elect exactly one liveness Service");

    assert_eq!(
        selected.target.key, survivor.key,
        "a stale ordered-owner hint without unresolved bytes must not pin Service ownership to a worse proof-only path"
    );
    assert_eq!(
        selected.admission.role,
        PathRuntimeRole::Service,
        "liveness failover elects one Service owner; it must not admit optional Subflow ownership"
    );
}

#[test]
fn clear_frontier_ownerless_stream_elects_measured_service() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let survivor = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);

    let selected = select_response_sender_data_target_with_ordered_debt_inner(
        std::slice::from_ref(&survivor),
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        None,
        0,
        None,
        true,
    )
    .expect("frontier-clear ownerless stream may elect a measured survivor as Service");

    assert_eq!(selected.target.key, survivor.key);
    assert_eq!(
        selected.admission.role,
        PathRuntimeRole::Service,
        "ownerless failover elects a new Service, not an optional Subflow behind missing-owner debt"
    );
}

#[test]
fn response_owner_tail_guard_admits_measured_same_underlay_when_service_over_budget() {
    let mux_limits = MuxLimits {
        max_path_flight_bytes: 64 * 1024 * 1024,
        max_reorder_bytes: 64 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let payload_bytes = 64 * 1024usize;
    let mut owner = response_target(0, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, true);
    let service_envelope =
        bulk_active_service_product_envelope_bytes(owner.snapshot, payload_bytes, mux_limits);
    owner.snapshot.product_bytes_in_flight = service_envelope;
    owner.snapshot.queue_bytes = payload_bytes as u64;
    let alternate = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    let owner_tail_guard_bytes = payload_bytes.saturating_mul(2);

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[owner.clone(), alternate],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(owner.key),
        owner_tail_guard_bytes,
        None,
    )
    .expect("measured same-underlay Subflow should remain eligible under bounded tail debt");
    assert_eq!(
        selected.admission.role,
        PathRuntimeRole::Subflow,
        "owner-tail debt is accounted as ordering risk, not an absolute same-underlay Subflow ban"
    );
}

#[test]
fn response_owner_tail_guard_blocks_cross_underlay_when_owner_queue_is_full() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut owner = response_target(0, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, true);
    let alternate = response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);
    let (owner_commands, _owner_rx) = reliable_path_command_channels(1);
    owner_commands
        .try_enqueue_admitted_frame(
            Frame::StreamData {
                stream_id: StreamId(99),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"queued"),
            },
            FlowLane::Throughput,
        )
        .expect("seed full owner data queue");
    owner.commands = owner_commands;

    let owner_tail_guard_bytes = payload_bytes.saturating_mul(2);

    assert!(
        select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[owner.clone(), alternate],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(owner.key),
            owner_tail_guard_bytes,
            None,
        )
        .is_none(),
        "owner-debt fallback must not migrate ordered bytes across TCP/QUIC families"
    );
}

#[test]
fn cross_underlay_alternate_waits_when_service_owner_is_backpressured() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let assigned_bytes = bulk_service_feed_reservoir_payload_bytes(payload_bytes, mux_limits)
        .saturating_sub(payload_bytes);
    let owner = response_target(
        1,
        UnderlayProtocol::Tcp,
        50.0,
        assigned_bytes as u64,
        0,
        true,
    );
    let alternate = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);

    let owner_tail_guard_bytes = payload_bytes.saturating_mul(2);
    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[owner.clone(), alternate],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(owner.key),
        owner_tail_guard_bytes,
        None,
    );

    let selected = selected.expect("feedable Service owner should remain selected under tail debt");
    assert_eq!(
        selected.target.key, owner.key,
        "a cross-underlay alternate must not own later bytes while the current Service owner has unresolved contiguous tail"
    );
}

#[test]
fn response_owner_tail_guard_blocks_proof_only_same_family_subflow() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut owner = response_target(0, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, true);
    let mut alternate = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    alternate.has_sender_evidence = true;
    alternate.has_bulk_rate_evidence = false;
    let (owner_commands, _owner_rx) = reliable_path_command_channels(1);
    owner_commands
        .try_enqueue_admitted_frame(
            Frame::StreamData {
                stream_id: StreamId(99),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"queued"),
            },
            FlowLane::Throughput,
        )
        .expect("seed full owner data queue");
    owner.commands = owner_commands;

    let owner_tail_guard_bytes = payload_bytes.saturating_mul(2);

    assert!(
        select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[owner.clone(), alternate],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(owner.key),
            owner_tail_guard_bytes,
            None,
        )
        .is_none(),
        "proof-only paths must stay Probe/Standby while older owner debt is unresolved"
    );
}

#[test]
fn response_small_owner_debt_keeps_feedable_service_ahead_of_measured_subflow() {
    let owner = response_target(0, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, true);
    let lower_eta_alternate =
        response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[owner.clone(), lower_eta_alternate.clone()],
        FlowLane::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &[],
        Some(owner.key),
        64 * 1024,
        None,
    )
    .expect("feedable Service should pass bounded tail-debt admission");

    assert_eq!(
        selected.target.key, owner.key,
        "small Service-tail debt must not displace a feedable Service with optional same-underlay work"
    );
    assert_eq!(
        selected.admission.role,
        PathRuntimeRole::Service,
        "the lower-ETA same-underlay path remains Subflow overflow"
    );
}

#[test]
fn small_ordered_owner_debt_blocks_cross_underlay_service_migration() {
    let owner = response_target(1, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, true);
    let active_cross_underlay =
        response_target(0, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, true);

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[owner.clone(), active_cross_underlay],
        FlowLane::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &[],
        Some(owner.key),
        64 * 1024,
        None,
    );

    assert!(
        selected.is_none()
            || selected
                .as_ref()
                .is_some_and(|selected| selected.target.key == owner.key),
        "any unresolved ordered-owner tail must block TCP/QUIC Service migration until the frontier clears or the candidate already owns the lower range"
    );
}

#[test]
fn ordered_owner_debt_blocks_fallback_service_when_owner_target_is_absent() {
    let missing_owner = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let active_cross_underlay =
        response_target(0, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, true);

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[active_cross_underlay],
        FlowLane::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &[],
        Some(missing_owner),
        64 * 1024,
        None,
    );

    assert!(
        selected.is_none(),
        "an absent ordered owner with unresolved lower bytes must trigger repair/failover handling, not make another underlay the Service owner for later bytes"
    );
}

#[test]
fn missing_same_underlay_owner_debt_admits_measured_service_failover() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let missing_owner = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    let measured_survivor =
        response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        std::slice::from_ref(&measured_survivor),
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(missing_owner),
        payload_bytes.saturating_mul(2),
        None,
    )
    .expect("a bulk-rate-proven same-underlay survivor should elect Service failover when the previous Service output is gone and no lower-flight owner remains");

    assert_eq!(selected.target.key, measured_survivor.key);
    assert_eq!(
        selected.admission.role,
        PathRuntimeRole::Service,
        "same-underlay failover resumes Service OwnerData; it is not optional Subflow exploration and does not credit RepairData as proof"
    );
}

#[test]
fn missing_same_underlay_service_failover_respects_path_latency_window() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let missing_owner = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let mut measured_survivor = response_target(
        1,
        UnderlayProtocol::Tcp,
        5.0,
        0,
        mux_limits.max_path_flight_bytes as u64,
        false,
    );
    measured_survivor.snapshot.delivery_rate_bps = 10_000_000_000.0;
    measured_survivor.snapshot.pacing_rate_bps = 10_000_000_000.0;
    measured_survivor.snapshot.active_latency_sensitive_flows = 1;
    let latency_credit = usize::try_from(bulk_latency_pressure_service_feed_window_bytes(
        payload_bytes,
        mux_limits,
    ))
    .unwrap();
    measured_survivor.snapshot.product_bytes_in_flight =
        latency_credit.saturating_sub(payload_bytes) as u64;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        std::slice::from_ref(&measured_survivor),
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(missing_owner),
        payload_bytes.saturating_mul(2),
        None,
    )
    .expect("mature same-underlay Service failover may consume remaining latency-window credit");
    assert_eq!(selected.admission.role, PathRuntimeRole::Service);

    measured_survivor.snapshot.product_bytes_in_flight = latency_credit as u64;
    assert!(
        select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[measured_survivor],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(missing_owner),
            payload_bytes.saturating_mul(2),
            None,
        )
        .is_none(),
        "runtime Service failover must stop at the same path-local latency window even when its bulk role is AdditionalSameUnderlay"
    );
}

#[test]
fn missing_same_underlay_owner_debt_admits_sender_evidence_service_failover() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let missing_owner = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let mut liveness_survivor =
        response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);
    liveness_survivor.has_sender_evidence = true;
    liveness_survivor.has_bulk_rate_evidence = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        std::slice::from_ref(&liveness_survivor),
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(missing_owner),
        payload_bytes.saturating_mul(2),
        None,
    )
    .expect("a same-underlay sender-evidenced survivor should receive bounded Service failover when the previous Service output is gone and no lower-flight owner remains");

    assert_eq!(selected.target.key, liveness_survivor.key);
    assert_eq!(
        selected.admission.role,
        PathRuntimeRole::Service,
        "same-underlay failover is Service continuation, not Subflow aggregation"
    );
    assert!(
        selected.subflow_set_commit.is_none(),
        "failover Service election must not spend Subflow owner credit"
    );
}

#[test]
fn ordered_owner_debt_without_owner_hint_blocks_active_fallback_service() {
    let active_cross_underlay =
        response_target(0, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, true);

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[active_cross_underlay],
        FlowLane::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &[],
        None,
        64 * 1024,
        None,
    );

    assert!(
        selected.is_none(),
        "ordered-owner debt without an owner hint must not fall back to the active path as Service"
    );
}

#[test]
fn proof_only_active_service_can_continue_under_its_own_tail_guard() {
    let mut active_fallback =
        response_target(0, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, true);
    active_fallback.has_sender_evidence = true;
    active_fallback.has_bulk_rate_evidence = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[active_fallback.clone()],
        FlowLane::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &[],
        Some(active_fallback.key),
        315_680,
        None,
    );

    let selected =
        selected.expect("the live active Service owner may continue under its own tail guard");
    assert_eq!(selected.target.key, active_fallback.key);
    assert_eq!(
        selected.admission.role,
        PathRuntimeRole::Service,
        "tail guard must not turn active Service OwnerData into Subflow exploration"
    );
}

#[test]
fn bulk_only_tcp_sender_evidence_admits_startup_subflow_not_service() {
    let mut owner = response_target(0, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, true);
    owner.snapshot.active_flows = 2;
    let mut lower_eta_alternate =
        response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);
    lower_eta_alternate.has_sender_evidence = true;
    lower_eta_alternate.has_bulk_rate_evidence = false;

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[owner.clone(), lower_eta_alternate.clone()],
        FlowLane::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &[],
        Some(owner.key),
        0,
        None,
    )
    .expect("current Service owner should remain eligible");

    assert_eq!(
        selected.target.key, lower_eta_alternate.key,
        "sender evidence may start one bounded same-underlay Subflow sampling epoch"
    );
    assert_eq!(
        selected.admission.role,
        PathRuntimeRole::Subflow,
        "startup owner bytes are Subflow OwnerData and must not migrate Service ownership"
    );
    assert!(
        selected
            .subflow_set_commit
            .is_some_and(|commit| commit.input.startup_owner_allowed),
        "startup Subflow admission must be explicit and bounded"
    );
}

fn client_data_frame_for_test(stream_id: StreamId, offset: u64, payload_bytes: usize) -> Frame {
    Frame::StreamData {
        stream_id,
        offset,
        flags: StreamFlags::NONE,
        payload: Bytes::from(vec![0x5a; payload_bytes]),
    }
}

#[test]
fn measured_cross_family_path_handoff_allows_diversification_or_two_x_gain() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Tcp, 80.0, 0, 16 * 1024 * 1024, true);
    service.snapshot.active_flows = 2;
    let udp = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);

    let selected = select_response_service_handoff_target(
        &[service.clone(), udp.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        0,
        ResponseServiceFamilyLoads::new(2, 0),
        4096,
        None,
    )
    .expect("measured underloaded family should receive one whole flow");
    assert_eq!(selected.target.key, udp.key);
    assert_eq!(selected.admission.role, PathRuntimeRole::Service);
    assert_eq!(
        selected
            .service_handoff_commit
            .map(|commit| commit.handoff_frontier),
        Some(4096)
    );

    assert!(
        select_response_service_handoff_target(
            &[service.clone(), udp.clone()],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service.key),
            1,
            ResponseServiceFamilyLoads::new(2, 0),
            4096,
            None,
        )
        .is_none(),
        "any unresolved product tail blocks carrier-family handoff"
    );
    let balanced_gain = select_response_service_handoff_target(
        &[service, udp],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        }),
        0,
        ResponseServiceFamilyLoads::new(1, 1),
        4096,
        None,
    )
    .expect("a balanced family may still move one flow for a two-fold projected gain");
    assert_eq!(balanced_gain.admission.role, PathRuntimeRole::Service);
}

#[test]
fn balanced_service_handoff_requires_two_x_projected_gain() {
    let mut service = response_target(0, UnderlayProtocol::Tcp, 80.0, 0, 16 * 1024 * 1024, true);
    service.snapshot.rate_scope = PathRateScope::PerFlowGoodput;
    service.snapshot.delivery_rate_bps = 60_000_000.0;
    let mut udp = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    udp.snapshot.delivery_rate_bps = 100_000_000.0;

    assert_eq!(
        response_service_handoff_mode_for_targets(
            &service,
            &udp,
            ResponseServiceFamilyLoads::new(1, 1),
        ),
        None,
        "a modest gain must not churn sticky Service ownership"
    );
    service.snapshot.delivery_rate_bps = 50_000_000.0;
    assert_eq!(
        response_service_handoff_mode_for_targets(
            &service,
            &udp,
            ResponseServiceFamilyLoads::new(1, 1),
        ),
        Some(ResponseServiceHandoffMode::PerformanceOverride),
        "a two-fold projected gain survives one additional equal-share flow"
    );
}

#[test]
fn busy_shared_target_carrier_is_pressure_not_binding_debt() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Tcp, 80.0, 0, 16 * 1024 * 1024, true);
    service.snapshot.rate_scope = PathRateScope::PerFlowGoodput;
    service.snapshot.delivery_rate_bps = 1_000_000.0;
    let mut udp = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    let (udp_commands, _udp_receivers) = reliable_path_command_channels(8);
    udp.commands = udp_commands;
    udp.snapshot.delivery_rate_bps = 100_000_000.0;
    udp.snapshot.active_flows = 1;
    udp.commands
        .try_enqueue_stream_ordered_frame(
            client_data_frame_for_test(StreamId(999), 0, 1),
            FlowLane::Throughput,
        )
        .expect("shared target carrier accepts unrelated queued work");
    udp.command_pending_bytes = udp.commands.pending_bytes();
    udp.snapshot.queue_bytes = udp.command_pending_bytes;
    udp.snapshot.bytes_in_flight = 1;
    assert!(udp.command_pending_bytes > 0);
    assert_eq!(udp.owner_data_in_flight_bytes, 0);
    assert_eq!(udp.snapshot.product_bytes_in_flight, 0);

    let selected = select_response_service_handoff_target(
        &[service.clone(), udp.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        0,
        ResponseServiceFamilyLoads::new(1, 1),
        4096,
        None,
    )
    .expect("another binding's carrier pressure must not masquerade as this binding's debt");
    assert_eq!(selected.target.key, udp.key);
}

#[test]
fn response_service_handoff_drain_blocks_only_its_own_binding() {
    let fixture = response_service_handoff_drain_fixture();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());

    assert!(matches!(
        plan_response_data_dispatch_with_ordered_debt_impl(
            &fixture.stream,
            FlowLane::Throughput,
            payload_bytes as u64,
            payload_bytes,
            payload_bytes,
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    let mut sender = ServerResponseSenderService::new(SessionId(191), fixture.stream.stream_id);
    sender.enqueue_data_for_lane(Bytes::from(vec![0x5a; payload_bytes]), FlowLane::Throughput);
    assert!(
        sender.drain_allows_bounded_source_staging(&fixture.stream, true),
        "a drain blocks offset assignment, not bounded raw target read-ahead"
    );
    let reservation = fixture
        .binding
        .response_scheduling_snapshot()
        .response_service_handoff_drain
        .expect("the eligible flow should own the session drain intent");
    assert_eq!(
        reservation.binding_instance_id,
        fixture.binding.binding_instance_id()
    );
    assert_eq!(reservation.service, fixture.service);
    assert_eq!(reservation.target, fixture.target);
    assert!(matches!(
        plan_response_data_dispatch_with_ordered_debt_impl(
            &fixture.stream,
            FlowLane::Throughput,
            payload_bytes as u64,
            payload_bytes,
            payload_bytes,
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));

    let other_plan = plan_response_data_dispatch(
        &fixture.other_stream,
        FlowLane::Throughput,
        0,
        payload_bytes,
    )
    .expect("another binding must keep planning ordinary OwnerData");
    assert_eq!(other_plan.primary_key(), Some(fixture.other_service));
    assert_eq!(
        fixture.other_binding.ordered_data_owner(),
        Some(fixture.other_service),
        "a session drain is serialization for handoff, not a session-wide data pause"
    );
}

#[test]
fn response_readiness_preview_does_not_start_handoff_drain() {
    let fixture = response_service_handoff_drain_fixture();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
    let before = fixture.binding.response_scheduling_snapshot();
    assert!(before.response_service_handoff_drain.is_none());

    for _ in 0..2 {
        assert!(preview_response_data_payload_with_ordered_debt(
            &fixture.stream,
            FlowLane::Throughput,
            payload_bytes as u64,
            payload_bytes,
            payload_bytes,
        ));
    }

    let after_preview = fixture.binding.response_scheduling_snapshot();
    assert_eq!(after_preview.generation, before.generation);
    assert!(after_preview.response_service_handoff_drain.is_none());
    assert!(matches!(
        plan_response_data_dispatch_with_ordered_debt_impl(
            &fixture.stream,
            FlowLane::Throughput,
            payload_bytes as u64,
            payload_bytes,
            payload_bytes,
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    assert!(
        fixture
            .binding
            .response_scheduling_snapshot()
            .response_service_handoff_drain
            .is_some()
    );
}

#[tokio::test]
async fn response_service_handoff_drain_holds_raw_offset_until_frontier_commit() {
    let mut fixture = response_service_handoff_drain_fixture();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
    let frontier = payload_bytes as u64;
    let service_target = fixture
        .binding
        .sender_path_targets(FlowLane::Throughput, payload_bytes)
        .into_iter()
        .find(|target| target.key == fixture.service)
        .expect("TCP Service target");
    let old_owner_frame = Frame::StreamData {
        stream_id: fixture.stream.stream_id,
        offset: 0,
        flags: StreamFlags::NONE,
        payload: Bytes::from(vec![0x61; payload_bytes]),
    };
    fixture
        .binding
        .record_owner_flight_for_target(&service_target, &old_owner_frame);

    for _ in 0..2 {
        assert!(matches!(
            plan_response_data_dispatch_with_ordered_debt_impl(
                &fixture.stream,
                FlowLane::Throughput,
                frontier,
                payload_bytes,
                payload_bytes,
            ),
            Err(RuntimeError::SenderServiceBlocked)
        ));
    }
    assert_eq!(fixture.binding.ordered_data_owner(), Some(fixture.service));
    assert!(
        try_recv_reliable_path_command(&mut fixture.target_receivers).is_none(),
        "the paused raw payload must not consume its offset or enter the target queue"
    );

    fixture.binding.release_normalized_acked_ranges(&[
        OffsetRange::new(0, frontier).expect("old owner ACK range")
    ]);
    let plan = plan_response_data_dispatch_with_ordered_debt_impl(
        &fixture.stream,
        FlowLane::Throughput,
        frontier,
        payload_bytes,
        0,
    )
    .expect("the identical raw offset should become the clear-frontier handoff frame");
    assert_eq!(plan.primary_key(), Some(fixture.target));
    assert!(matches!(
        &plan.primary,
        ResponseDataDispatchTarget::Switchable {
            service_handoff_commit: Some(ResponseServiceHandoffCommit {
                handoff_frontier,
                ..
            }),
            ..
        } if *handoff_frontier == frontier
    ));

    let handoff_frame = Frame::StreamData {
        stream_id: fixture.stream.stream_id,
        offset: frontier,
        flags: StreamFlags::NONE,
        payload: Bytes::from(vec![0x62; payload_bytes]),
    };
    let outcome = emit_planned_response_data_frame(
        &fixture.stream,
        plan,
        handoff_frame,
        FlowLane::Throughput,
    )
    .await
    .expect("the first post-drain raw payload should atomically move Service");
    assert_eq!(outcome.selected_path, Some(fixture.target));
    assert_eq!(fixture.binding.ordered_data_owner(), Some(fixture.target));
    assert!(matches!(
        try_recv_reliable_path_command(&mut fixture.target_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { offset, .. }))
            if offset == frontier
    ));
}

#[tokio::test]
async fn balanced_performance_override_commits_full_handoff_transaction() {
    let mut fixture =
        response_service_handoff_drain_fixture_with_other_service(UnderlayProtocol::Udp);
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
    let frontier = payload_bytes as u64;
    fixture
        .binding
        .set_output_product_model_for_test(fixture.service, 1_000_000.0, 20.0);
    let service_target = fixture
        .binding
        .sender_path_targets(FlowLane::Throughput, payload_bytes)
        .into_iter()
        .find(|target| target.key == fixture.service)
        .expect("slow TCP Service target");
    fixture.binding.record_owner_flight_for_target(
        &service_target,
        &client_data_frame_for_test(fixture.stream.stream_id, 0, payload_bytes),
    );

    assert!(matches!(
        plan_response_data_dispatch_with_ordered_debt_impl(
            &fixture.stream,
            FlowLane::Throughput,
            frontier,
            payload_bytes,
            payload_bytes,
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    fixture.binding.release_normalized_acked_ranges(&[
        OffsetRange::new(0, frontier).expect("old owner ACK range")
    ]);
    let plan = plan_response_data_dispatch_with_ordered_debt_impl(
        &fixture.stream,
        FlowLane::Throughput,
        frontier,
        payload_bytes,
        0,
    )
    .expect("balanced slow TCP should move to the measured fast QUIC carrier");
    assert!(matches!(
        &plan.primary,
        ResponseDataDispatchTarget::Switchable {
            service_handoff_commit: Some(ResponseServiceHandoffCommit {
                mode: ResponseServiceHandoffMode::PerformanceOverride,
                ..
            }),
            ..
        }
    ));

    let outcome = emit_planned_response_data_frame(
        &fixture.stream,
        plan,
        client_data_frame_for_test(fixture.stream.stream_id, frontier, payload_bytes),
        FlowLane::Throughput,
    )
    .await
    .expect("the balanced performance override should commit atomically");
    assert_eq!(outcome.selected_path, Some(fixture.target));
    assert_eq!(fixture.binding.ordered_data_owner(), Some(fixture.target));
    assert!(matches!(
        try_recv_reliable_path_command(&mut fixture.target_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { offset, .. }))
            if offset == frontier
    ));
}

#[tokio::test]
async fn handoff_commit_rejects_shared_queue_growth_beyond_ranked_credit() {
    let fixture = response_service_handoff_drain_fixture();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(MuxLimits::default());
    let frontier = payload_bytes as u64;
    let service_target = fixture
        .binding
        .sender_path_targets(FlowLane::Throughput, payload_bytes)
        .into_iter()
        .find(|target| target.key == fixture.service)
        .expect("TCP Service target");
    fixture.binding.record_owner_flight_for_target(
        &service_target,
        &client_data_frame_for_test(fixture.stream.stream_id, 0, payload_bytes),
    );
    assert!(matches!(
        plan_response_data_dispatch_with_ordered_debt_impl(
            &fixture.stream,
            FlowLane::Throughput,
            frontier,
            payload_bytes,
            payload_bytes,
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    fixture.binding.release_normalized_acked_ranges(&[
        OffsetRange::new(0, frontier).expect("old owner ACK range")
    ]);
    let plan = plan_response_data_dispatch_with_ordered_debt_impl(
        &fixture.stream,
        FlowLane::Throughput,
        frontier,
        payload_bytes,
        0,
    )
    .expect("clear frontier should produce a bounded handoff commit");
    let (target_commands, pending_limit) = match &plan.primary {
        ResponseDataDispatchTarget::Switchable {
            target,
            service_handoff_commit: Some(commit),
            ..
        } => (
            target.commands.clone(),
            commit.target_command_pending_limit_bytes,
        ),
        _ => panic!("expected switchable handoff plan"),
    };
    let excess_bytes =
        usize::try_from(pending_limit.saturating_add(1)).expect("test credit fits process memory");
    target_commands
        .try_enqueue_stream_ordered_frame(
            client_data_frame_for_test(StreamId(999), 0, excess_bytes),
            FlowLane::Throughput,
        )
        .expect("unrelated shared work races with the planned commit");

    let result = emit_planned_response_data_frame(
        &fixture.stream,
        plan,
        client_data_frame_for_test(fixture.stream.stream_id, frontier, payload_bytes),
        FlowLane::Throughput,
    )
    .await;
    assert!(matches!(result, Err(RuntimeError::SenderServiceBlocked)));
    assert_eq!(fixture.binding.ordered_data_owner(), Some(fixture.service));
    assert!(
        fixture
            .binding
            .response_scheduling_snapshot()
            .response_service_handoff_drain
            .is_none(),
        "a credit-regressed transaction must release its session reservation"
    );
}

#[test]
fn service_handoff_rejects_lower_projected_fair_share() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Tcp, 80.0, 0, 16 * 1024 * 1024, true);
    service.snapshot.active_flows = 1;
    service.snapshot.delivery_rate_bps = 500_000_000.0;
    let mut udp = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    udp.snapshot.delivery_rate_bps = 100_000_000.0;

    assert!(
        select_response_service_handoff_target(
            &[service.clone(), udp],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service.key),
            0,
            ResponseServiceFamilyLoads::new(2, 0),
            4096,
            None,
        )
        .is_none(),
        "low RTT cannot justify a sticky move to a much slower carrier"
    );
}

#[test]
fn generic_evidence_drain_clears_unpinned_expired_receipt_marker() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Tcp, 80.0, 0, 16 * 1024 * 1024, true);
    service.snapshot.delivery_rate_bps = 1_000_000.0;
    let mut target = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    let now = Instant::now();
    let accepted_at = now
        .checked_sub(Duration::from_secs(2))
        .expect("test clock supports short subtraction");
    target.quic_capacity_proof = Some(QuicCapacityProofCandidate {
        token: 8,
        train_bytes: 1024,
        sample_floor_bytes: 1024,
        accounting_slack_bytes: 128,
        warmup_bytes: 128,
        required_proof_bytes: 896,
        written_bytes: 1024,
        written_data_frame_count: 1,
        receipt_confirmed: true,
        received_bytes: 1024,
        proof_elapsed: Duration::from_millis(1),
        rate_bps: 8_192_000,
        accepted_at,
        expires_at: accepted_at + Duration::from_secs(1),
        proof_validity: Duration::from_secs(1),
    });
    target.has_bulk_rate_evidence = true;
    let reservation = ResponseServiceHandoffDrainReservation {
        binding_instance_id: 8,
        service: service.key,
        service_path_instance_id: service.path_instance_id,
        service_incarnation: service.incarnation,
        target: target.key,
        target_path_instance_id: target.path_instance_id,
        target_incarnation: target.incarnation,
        capacity_proof: None,
        expires_at: now + Duration::from_secs(1),
    };

    let effective = response_service_handoff_target_view(
        &target,
        service.key,
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        Some(reservation),
        now,
    )
    .expect("the exact generic-evidence drain target");
    assert!(effective.has_bulk_rate_evidence);
    assert_eq!(effective.quic_capacity_proof, None);
    assert!(response_service_handoff_drain_matches_candidate(
        reservation.binding_instance_id,
        reservation,
        &ResponseServiceHandoffCandidate {
            service,
            target: effective,
            mode: ResponseServiceHandoffMode::Diversification,
        },
    ));
}

#[cfg(feature = "lab-diagnostics")]
#[test]
fn service_handoff_diagnostic_distinguishes_frontier_and_expired_receipt() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Tcp, 80.0, 0, 16 * 1024 * 1024, true);
    service.snapshot.delivery_rate_bps = 1_000_000.0;
    let mut udp = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    let now = Instant::now();
    let accepted_at = now
        .checked_sub(Duration::from_secs(2))
        .expect("test clock supports short subtraction");
    udp.quic_capacity_proof = Some(QuicCapacityProofCandidate {
        token: 7,
        train_bytes: 1024,
        sample_floor_bytes: 1024,
        accounting_slack_bytes: 128,
        warmup_bytes: 128,
        required_proof_bytes: 896,
        written_bytes: 1024,
        written_data_frame_count: 1,
        receipt_confirmed: true,
        received_bytes: 1024,
        proof_elapsed: Duration::from_millis(1),
        rate_bps: 8_192_000,
        accepted_at,
        expires_at: accepted_at + Duration::from_secs(1),
        proof_validity: Duration::from_secs(1),
    });
    udp.has_bulk_rate_evidence = false;
    let targets = [service.clone(), udp.clone()];
    let expired = response_service_handoff_diagnostics::evaluate_response_service_handoff(
        &targets,
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        0,
        ResponseServiceFamilyLoads::new(2, 0),
        None,
        true,
        false,
        false,
        false,
        now,
    );
    assert_eq!(expired.first_failed_gate, "target_proof_expired");

    let proof = udp.quic_capacity_proof.expect("raw expired marker");
    let reservation = ResponseServiceHandoffDrainReservation {
        binding_instance_id: 7,
        service: service.key,
        service_path_instance_id: service.path_instance_id,
        service_incarnation: service.incarnation,
        target: udp.key,
        target_path_instance_id: udp.path_instance_id,
        target_incarnation: udp.incarnation,
        capacity_proof: Some(proof),
        expires_at: now + Duration::from_secs(1),
    };
    let effective =
        response_service_handoff_diagnostics::response_service_handoff_diagnostic_target_view(
            &service,
            &udp,
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            Some(reservation),
            now,
        )
        .expect("diagnostic must retain the exact bounded proof view");
    assert!(!udp.has_bulk_rate_evidence, "raw marker is expired");
    assert!(
        effective.has_bulk_rate_evidence,
        "pinned view remains authoritative"
    );
    assert_eq!(effective.quic_capacity_proof, Some(proof));
    assert_eq!(effective.snapshot.delivery_rate_bps, proof.rate_bps as f64);
    assert_eq!(
        effective.snapshot.rate_scope,
        PathRateScope::PathCapacity,
        "the pinned QUIC receipt rate and its capacity scope are one snapshot authority"
    );
    assert!(response_service_handoff_preserves_fair_share(
        &service, &effective
    ));

    udp.has_bulk_rate_evidence = true;
    udp.quic_capacity_proof = udp
        .quic_capacity_proof
        .map(|proof| QuicCapacityProofCandidate {
            accepted_at: now,
            expires_at: now + Duration::from_secs(1),
            ..proof
        });
    let targets = [service.clone(), udp];
    let blocked_frontier = response_service_handoff_diagnostics::evaluate_response_service_handoff(
        &targets,
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        payload_bytes,
        ResponseServiceFamilyLoads::new(2, 0),
        None,
        true,
        false,
        false,
        false,
        now,
    );
    assert_eq!(blocked_frontier.first_failed_gate, "frontier_not_clear");
    assert!(response_service_handoff_preserves_fair_share(
        blocked_frontier.service.expect("diagnostic Service"),
        blocked_frontier.target.expect("diagnostic target"),
    ));
}

#[test]
fn service_handoff_fair_share_respects_rate_scope() {
    let mut tcp = response_target(0, UnderlayProtocol::Tcp, 20.0, 0, 16 * 1024 * 1024, true);
    tcp.snapshot.delivery_rate_bps = 100_000_000.0;
    tcp.snapshot.active_flows = 2;
    let mut udp = response_target(1, UnderlayProtocol::Udp, 80.0, 0, 16 * 1024 * 1024, false);
    udp.snapshot.delivery_rate_bps = 80_000_000.0;
    udp.snapshot.active_flows = 0;

    tcp.snapshot.rate_scope = PathRateScope::PathCapacity;
    assert!(response_service_handoff_preserves_fair_share(&tcp, &udp));
    tcp.snapshot.rate_scope = PathRateScope::PerFlowGoodput;
    assert!(
        !response_service_handoff_preserves_fair_share(&tcp, &udp),
        "a 100 Mbps per-flow TCP observation must not be divided a second time"
    );
}

#[cfg(feature = "lab-diagnostics")]
#[test]
fn family_or_gain_diagnostic_ignores_shared_carrier_churn() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service = response_target(0, UnderlayProtocol::Tcp, 20.0, 0, 16 * 1024 * 1024, true);
    let udp = response_target(1, UnderlayProtocol::Udp, 180.0, 0, 16 * 1024 * 1024, false);
    let mut targets = [service, udp];
    let service_family_loads = ResponseServiceFamilyLoads::new(1, 1);
    let now = Instant::now();
    let signature = |targets: &[ResponseSenderPathTarget]| {
        let evaluation = response_service_handoff_diagnostics::evaluate_response_service_handoff(
            targets,
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(targets[0].key),
            0,
            service_family_loads,
            None,
            true,
            false,
            false,
            false,
            now,
        );
        assert_eq!(evaluation.first_failed_gate, "family_or_gain");
        response_service_handoff_diagnostics::response_service_handoff_evaluation_signature(
            evaluation,
            service_family_loads,
        )
    };

    let before = signature(&targets);
    targets[1].snapshot.bytes_in_flight = payload_bytes as u64;
    targets[1].snapshot.queue_bytes = payload_bytes as u64;
    targets[1].eta_ms = 1_000_000.0;
    let after = signature(&targets);

    assert_eq!(
        before, after,
        "shared carrier pressure cannot change a family/gain policy decision"
    );
}

#[test]
fn cross_underlay_candidate_does_not_displace_owner_without_bulk_rate() {
    let owner = response_target(0, UnderlayProtocol::Tcp, 80.0, 0, 16 * 1024 * 1024, true);
    let mut candidate = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    candidate.has_bulk_rate_evidence = false;

    let selected = choose_response_sender_data_target(
        &[owner.clone(), candidate],
        FlowLane::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &[],
        Some(owner.key),
    )
    .expect(
        "current service owner should remain eligible while cross-family candidate is unproven",
    );

    assert_eq!(selected.key, owner.key);
}

#[test]
fn cross_underlay_bulk_rate_candidate_does_not_become_service_at_clear_frontier() {
    let owner = response_target(0, UnderlayProtocol::Tcp, 80.0, 0, 16 * 1024 * 1024, true);
    let candidate = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);

    let selected = choose_response_sender_data_target(
        &[owner.clone(), candidate.clone()],
        FlowLane::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &[],
        Some(owner.key),
    )
    .expect("current Service owner should remain eligible at a clear frontier");

    assert_eq!(
        selected.key, owner.key,
        "mixed-family Service migration must be explicit; lower-ETA cross-underlay candidates do not become Service through per-quantum selection"
    );
}

#[test]
fn cross_underlay_candidate_does_not_become_service_when_owner_hint_is_missing() {
    let owner = response_target(0, UnderlayProtocol::Tcp, 80.0, 0, 16 * 1024 * 1024, true);
    let candidate = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);

    let selected = choose_response_sender_data_target(
        &[owner.clone(), candidate.clone()],
        FlowLane::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &[],
        None,
    )
    .expect(
        "active Service output should anchor family ownership even if the owner hint was cleared",
    );

    assert_eq!(
        selected.key, owner.key,
        "a missing ordered-owner hint is not permission for implicit cross-family Service migration while an active Service output is live"
    );
}

#[test]
fn cross_underlay_bulk_rate_candidate_that_owns_lower_flight_remains_eligible() {
    let service = response_target(0, UnderlayProtocol::Tcp, 80.0, 0, 16 * 1024 * 1024, true);
    let candidate = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    let lower_flights = vec![CarrierPathFlightDebt {
        key: candidate.key,
        bytes: 64 * 1024,
    }];

    let selected = choose_response_sender_data_target(
        &[service.clone(), candidate.clone()],
        FlowLane::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &lower_flights,
        Some(service.key),
    )
    .expect("candidate owning the lower flight should remain eligible");

    assert_eq!(
        selected.key, candidate.key,
        "a bulk-rate-proven path that already owns the lower range must not be blocked by a stale cross-family frontier check"
    );
}

#[test]
fn active_cross_underlay_path_that_owns_lower_flight_remains_service_candidate() {
    let mut old_service =
        response_target(0, UnderlayProtocol::Tcp, 80.0, 0, 16 * 1024 * 1024, false);
    old_service.has_bulk_rate_evidence = true;
    let mut lower_active =
        response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, true);
    lower_active.has_sender_evidence = true;
    lower_active.has_bulk_rate_evidence = false;
    let lower_flights = vec![CarrierPathFlightDebt {
        key: lower_active.key,
        bytes: 64 * 1024,
    }];

    let selected = choose_response_sender_data_target(
        &[old_service.clone(), lower_active.clone()],
        FlowLane::Throughput,
        64 * 1024,
        MuxLimits::default(),
        &lower_flights,
        Some(old_service.key),
    )
    .expect("active lower-owner path must remain eligible to advance its own frontier");

    assert_eq!(
        selected.key, lower_active.key,
        "mixed-family health gates must not remove the active path that already owns unresolved lower bytes"
    );
}

#[test]
fn owner_tail_guard_keeps_cross_underlay_candidate_that_owns_lower_flight() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service = response_target(0, UnderlayProtocol::Tcp, 80.0, 0, 16 * 1024 * 1024, true);
    let candidate = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    let lower_flights = vec![CarrierPathFlightDebt {
        key: candidate.key,
        bytes: payload_bytes as u64,
    }];
    let owner_tail_guard_bytes = payload_bytes.saturating_mul(2);

    let selected = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), candidate.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &lower_flights,
        Some(service.key),
        owner_tail_guard_bytes,
        None,
    )
    .expect("candidate owning the lower flight should survive tail-guard filtering");

    assert_eq!(
        selected.target.key, candidate.key,
        "tail guard must filter by candidate ordering safety, not by carrier family alone"
    );
}
