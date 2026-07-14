use super::quic_capacity::RequestQuicCapacityCalibration;
use super::tcp_capacity::request_tcp_carrier_authority_expired_naturally;
use super::test_support::*;
use super::*;
use crate::model::ack_clock::reliable_request_ack_clock_calibration_target_bytes;
use crate::model::capacity::QuicCapacityProofCandidate;
use crate::model::request::capacity::{
    request_capacity_stable_candidate_share_bytes, request_tcp_capacity_calibration_geometry,
};
use crate::protocol::frame::reliable_stream_frame_extent;
use crate::runtime::path::commands::{
    ReliablePathCommand, reliable_path_command_channels, try_recv_reliable_path_command,
    try_recv_reliable_path_priority_command,
};
use crate::runtime::stream::response::ResponseStreamBinding;

#[test]
fn budgeted_critical_repair_preempts_owner_data_and_debits_budget() {
    let mux_limits = MuxLimits::default();
    let stream_id = StreamId(79);
    let mut sender = ServerResponseSenderService::new_with_performance(
        SessionId(79),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 1,
        },
    );
    let startup_floor = sender_extra_traffic_startup_floor_bytes(mux_limits);

    sender.enqueue_data_for_lane(Bytes::from_static(b"owner"), FlowLane::Throughput);
    assert!(
        sender
            .enqueue_repair_frame_with_priority(
                Frame::StreamData {
                    stream_id,
                    offset: 0,
                    flags: StreamFlags::NONE,
                    payload: Bytes::from(vec![0x7a; startup_floor]),
                },
                mux_limits,
                true,
            )
            .is_some(),
        "startup repair floor should be spendable"
    );

    assert_eq!(sender.queue.front_lane(), Some(ReliableWorkClass::Repair));
    assert_eq!(
        sender.repair_extra_budget_remaining(mux_limits),
        0,
        "critical priority is not budget bypass"
    );
}

fn request_handle(output: ReliablePathStreamOutput) -> ReliablePathStreamHandle {
    ReliablePathStreamHandle {
        stream_id: StreamId(7),
        max_offset: 64 * 1024,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: 16 * 1024,
        output,
    }
}

#[test]
fn request_dispatch_preserves_classified_and_stream_ordered_queues() {
    let (commands, mut receivers) = reliable_path_command_channels(1);
    let fixed = FixedReliablePathOutput::new(
        UnderlayProtocol::Tcp,
        PathId(0),
        commands,
        MuxLimits::default(),
    );
    let handle = request_handle(ReliablePathStreamOutput::Fixed(fixed));

    emit_request_frame_with_mode(
        &handle,
        Frame::Ping { nonce: 1 },
        FlowLane::Control,
        CarrierEmitMode::Classified,
    )
    .expect("classified control uses the priority queue");
    assert!(matches!(
        emit_request_frame_with_mode(
            &handle,
            Frame::Ping { nonce: 2 },
            FlowLane::Control,
            CarrierEmitMode::Classified,
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    emit_request_frame_with_mode(
        &handle,
        Frame::Ping { nonce: 3 },
        FlowLane::Control,
        CarrierEmitMode::StreamOrdered,
    )
    .expect("stream-ordered control uses the data queue");

    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::Ping { nonce: 1 }))
    ));
    assert!(matches!(
        try_recv_reliable_path_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::Ping { nonce: 3 }))
    ));
}

#[test]
fn request_dispatch_rejects_switchable_response_output() {
    let (commands, _receivers) = reliable_path_command_channels(1);
    let binding = ResponseStreamBinding::new(
        SessionId(9),
        UnderlayProtocol::Tcp,
        PathId(0),
        commands,
        FlowLane::Throughput,
    );
    let handle = request_handle(ReliablePathStreamOutput::Switchable(binding));

    assert!(matches!(
        emit_request_frame_with_mode(
            &handle,
            Frame::Ping { nonce: 1 },
            FlowLane::Control,
            CarrierEmitMode::Classified,
        ),
        Err(RuntimeError::Protocol("request relay path is not fixed"))
    ));
}

#[test]
fn stream_ack_releases_flights_without_publishing_a_tiny_rate_sample() {
    let path = "tcp://127.0.0.1:10251".parse::<PathSpec>().expect("path");
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("context");
    let key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    let seeded =
        PathRateSample::new(4 * 1024 * 1024, Duration::from_millis(20)).expect("seed rate sample");
    context.mark_relay_path_rate_sample(key.underlay, key.index, seeded);

    let frame = Frame::StreamData {
        stream_id: StreamId(7),
        offset: 0,
        flags: StreamFlags::NONE,
        payload: Bytes::from(vec![0u8; PATH_OPEN_SCORE_BYTES]),
    };
    context.record_relay_path_send(key.underlay, key.index, PATH_OPEN_SCORE_BYTES);
    let mut sender = RequestSenderService::new(StreamId(7));
    sender.request.flights.record_owner_frame(key, &frame);

    let before = context.tcp_path_snapshot(0).expect("before snapshot");
    assert_eq!(before.bytes_in_flight, PATH_OPEN_SCORE_BYTES as u64);
    let owner_progress = sender.release_normalized_acked_ranges_with_owner_progress(
        &context,
        &[OffsetRange::new(0, PATH_OPEN_SCORE_BYTES as u64).expect("ack range")],
    );
    let after = context.tcp_path_snapshot(0).expect("after snapshot");

    assert_eq!(after.bytes_in_flight, 0);
    assert_eq!(
        after.delivery_rate_bps, before.delivery_rate_bps,
        "an unambiguous tiny ACK proves ownership but must not replace the retained rate"
    );
    assert_eq!(
        owner_progress.as_slice(),
        &[RequestOwnerAckProgress {
            instance: RelayPathInstance { key, id: 0 },
            bytes: PATH_OPEN_SCORE_BYTES,
        }],
        "request-window growth must use exact flight ownership, not the ACK carrier"
    );
}

#[test]
fn udp_stream_ack_reports_exact_product_progress_without_carrier_capacity_evidence() {
    let path = "udp://127.0.0.1:10255".parse::<PathSpec>().expect("path");
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("context");
    let instance = RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        },
        id: 17,
    };
    let frame = client_data_frame_for_test(StreamId(11), 0, PATH_OPEN_SCORE_BYTES);
    context.record_relay_path_send(
        instance.key.underlay,
        instance.key.index,
        PATH_OPEN_SCORE_BYTES,
    );
    let mut sender = RequestSenderService::new(StreamId(11));
    sender
        .request
        .flights
        .record_owner_frame_instance(instance, &frame);

    let owner_progress = sender.release_normalized_acked_ranges_with_owner_progress(
        &context,
        &[OffsetRange::new(0, PATH_OPEN_SCORE_BYTES as u64).expect("ACK range")],
    );

    assert_eq!(
        context
            .udp_path_snapshot(0)
            .expect("UDP path snapshot")
            .bytes_in_flight,
        0
    );
    assert_eq!(
        owner_progress.as_slice(),
        &[RequestOwnerAckProgress {
            instance,
            bytes: PATH_OPEN_SCORE_BYTES,
        }],
        "exact QUIC OwnerData ACKs are product-consumption evidence"
    );
    assert!(
        !context.relay_path_has_bulk_model_evidence(instance.key.underlay, instance.key.index,),
        "product STREAM_ACK timing must not become QUIC carrier-capacity evidence"
    );
}

#[test]
fn ambiguous_udp_stream_ack_does_not_report_product_window_progress() {
    let path = "udp://127.0.0.1:10256".parse::<PathSpec>().expect("path");
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("context");
    let instance = RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        },
        id: 18,
    };
    let frame = client_data_frame_for_test(StreamId(12), 0, PATH_OPEN_SCORE_BYTES);
    context.record_relay_path_send(
        instance.key.underlay,
        instance.key.index,
        PATH_OPEN_SCORE_BYTES,
    );
    context.record_relay_path_send(
        instance.key.underlay,
        instance.key.index,
        PATH_OPEN_SCORE_BYTES,
    );
    let mut sender = RequestSenderService::new(StreamId(12));
    sender
        .request
        .flights
        .record_owner_frame_instance(instance, &frame);
    sender
        .request
        .flights
        .record_repair_frame_instance(instance, &frame);

    let owner_progress = sender.release_normalized_acked_ranges_with_owner_progress(
        &context,
        &[OffsetRange::new(0, PATH_OPEN_SCORE_BYTES as u64).expect("ACK range")],
    );

    assert!(
        owner_progress.is_empty(),
        "an OwnerData/RepairData duplicate ACK is not exact product-owner progress"
    );
    assert!(
        !context.relay_path_has_bulk_model_evidence(instance.key.underlay, instance.key.index,)
    );
}

#[test]
fn sub_coverage_stream_ack_does_not_publish_a_path_rate_sample() {
    let path = "tcp://127.0.0.1:10253".parse::<PathSpec>().expect("path");
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("context");
    let instance = RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        },
        id: 11,
    };
    let mut sender = RequestSenderService::new(StreamId(9));
    let frames = (0..4)
        .map(|index| {
            client_data_frame_for_test(
                StreamId(9),
                index * BBR_MAX_SEND_QUANTUM_BYTES as u64,
                BBR_MAX_SEND_QUANTUM_BYTES,
            )
        })
        .collect::<Vec<_>>();
    for frame in &frames {
        context.record_relay_path_send(
            instance.key.underlay,
            instance.key.index,
            BBR_MAX_SEND_QUANTUM_BYTES,
        );
        sender
            .request
            .flights
            .record_owner_frame_instance(instance, frame);
    }

    sender.release_normalized_acked_ranges(
        &context,
        &[OffsetRange::new(0, (4 * BBR_MAX_SEND_QUANTUM_BYTES) as u64)
            .expect("cumulative ACK range")],
    );
    let delivery_samples =
        context.health().lock().expect("path health lock").tcp[0].delivery_samples;

    assert_eq!(
        context
            .tcp_path_snapshot(0)
            .expect("path snapshot")
            .bytes_in_flight,
        0
    );
    assert_eq!(delivery_samples, 0);
    assert!(
        !context.relay_path_has_bulk_model_evidence(instance.key.underlay, instance.key.index,),
        "callback-sized ACK batches must not become a shared scheduling rate"
    );
}

#[test]
fn fragmented_service_acks_establish_provenance_without_publishing_rate() {
    let path = "tcp://127.0.0.1:10254".parse::<PathSpec>().expect("path");
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("context");
    let instance = RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        },
        id: 12,
    };
    let mut sender = RequestSenderService::new(StreamId(10));
    let chunk = 8 * 1024;
    let first = client_data_frame_for_test(StreamId(10), 0, chunk);
    let second = client_data_frame_for_test(StreamId(10), chunk as u64, chunk);
    for frame in [&first, &second] {
        context.record_relay_path_send(instance.key.underlay, instance.key.index, chunk);
        sender
            .request
            .flights
            .record_owner_frame_instance(instance, frame);
    }

    sender.release_normalized_acked_ranges(
        &context,
        &[OffsetRange::new(0, chunk as u64).expect("first ACK range")],
    );
    assert!(
        !sender
            .request
            .subflows
            .get(instance)
            .is_some_and(|state| state.rate_proven())
    );
    assert_eq!(
        context.health().lock().expect("path health lock").tcp[0].delivery_samples,
        0
    );

    sender.release_normalized_acked_ranges(
        &context,
        &[OffsetRange::new(chunk as u64, (2 * chunk) as u64).expect("second ACK range")],
    );
    let health = context.health().lock().expect("path health lock");
    assert!(
        sender
            .request
            .subflows
            .get(instance)
            .is_some_and(|state| state.rate_proven())
    );
    assert_eq!(health.tcp[0].delivery_samples, 0);
    assert_eq!(health.tcp[0].product_delivery_sample_bytes, 0);
}

#[test]
fn tcp_request_service_first_window_publishes_bulk_authority_without_rate_override() {
    let path = "tcp://127.0.0.1:10257?rate-mbps=500"
        .parse::<PathSpec>()
        .expect("path");
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("context");
    let instance = RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        },
        id: 14,
    };
    let mut sender = RequestSenderService::new(StreamId(12));
    sender.request.ordered_service = Some(instance);
    let coverage = usize::try_from(reliable_ack_clock_calibration_rate_coverage_floor_bytes(
        context.mux_limits,
    ))
    .expect("coverage");
    let frame = client_data_frame_for_test(StreamId(12), 0, coverage);
    context.record_relay_path_send(instance.key.underlay, instance.key.index, coverage);
    sender
        .request
        .flights
        .record_owner_frame_instance(instance, &frame);

    sender.release_normalized_acked_ranges(
        &context,
        &[OffsetRange::new(0, coverage as u64).expect("Service ACK")],
    );

    assert!(context.relay_path_has_bulk_model_evidence(instance.key.underlay, instance.key.index));
    assert!(
        sender
            .request
            .subflows
            .get(instance)
            .and_then(|state| state.per_flow_rate())
            .is_none()
    );
    assert!(
        !sender
            .request
            .subflows
            .get(instance)
            .is_some_and(|state| state.ack_clock_proven())
    );
}

#[test]
fn tcp_request_first_window_only_establishes_the_ack_clock() {
    let path = "tcp://127.0.0.1:10255".parse::<PathSpec>().expect("path");
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("context");
    let instance = RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        },
        id: 13,
    };
    let mut sender = RequestSenderService::new(StreamId(11));
    let coverage_floor = usize::try_from(reliable_ack_clock_calibration_rate_coverage_floor_bytes(
        context.mux_limits,
    ))
    .expect("coverage floor");
    let first = client_data_frame_for_test(StreamId(11), 0, coverage_floor);
    let second = client_data_frame_for_test(StreamId(11), coverage_floor as u64, coverage_floor);
    context.record_relay_path_send(instance.key.underlay, instance.key.index, coverage_floor);
    sender
        .request
        .flights
        .record_owner_frame_instance(instance, &first);

    sender.release_normalized_acked_ranges(
        &context,
        &[OffsetRange::new(0, coverage_floor as u64).expect("first window")],
    );
    assert!(
        sender
            .request
            .subflows
            .get(instance)
            .is_some_and(|state| state.rate_proven())
    );
    assert!(
        !sender
            .request
            .subflows
            .get(instance)
            .is_some_and(|state| state.ack_clock_proven())
    );
    assert!(
        sender
            .request
            .subflows
            .get(instance)
            .and_then(|state| state.per_flow_rate())
            .is_none()
    );
    assert_eq!(
        context.health().lock().expect("path health lock").tcp[0].delivery_samples,
        0,
        "the RTT-bearing first window establishes the clock but is not a rate sample"
    );

    context.record_relay_path_send(instance.key.underlay, instance.key.index, coverage_floor);
    sender
        .request
        .flights
        .record_owner_frame_instance(instance, &second);
    sender.release_normalized_acked_ranges(
        &context,
        &[
            OffsetRange::new(coverage_floor as u64, (2 * coverage_floor) as u64)
                .expect("second window"),
        ],
    );
    let health = context.health().lock().expect("path health lock");
    assert!(
        sender
            .request
            .subflows
            .get(instance)
            .is_some_and(|state| state.ack_clock_proven())
    );
    assert!(
        sender
            .request
            .subflows
            .get(instance)
            .and_then(|state| state.per_flow_rate())
            .is_some()
    );
    assert_eq!(health.tcp[0].delivery_samples, 1);
    assert_eq!(
        health.tcp[0].product_delivery_sample_bytes,
        coverage_floor as u64
    );
}

#[test]
fn duplicate_stream_ack_release_does_not_seed_sender_service_path_rate() {
    let path = "tcp://127.0.0.1:10252".parse::<PathSpec>().expect("path");
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("context");
    let owner = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    let repair = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    let frame = Frame::StreamData {
        stream_id: StreamId(8),
        offset: 0,
        flags: StreamFlags::NONE,
        payload: Bytes::from(vec![0u8; PATH_OPEN_SCORE_BYTES]),
    };
    context.record_relay_path_send(owner.underlay, owner.index, PATH_OPEN_SCORE_BYTES);
    context.record_relay_path_send(repair.underlay, repair.index, PATH_OPEN_SCORE_BYTES);
    let mut sender = RequestSenderService::new(StreamId(8));
    sender.request.flights.record_owner_frame(owner, &frame);
    sender.request.flights.record_repair_frame(repair, &frame);

    let owner_progress = sender.release_normalized_acked_ranges_with_owner_progress(
        &context,
        &[OffsetRange::new(0, PATH_OPEN_SCORE_BYTES as u64).expect("ack range")],
    );
    let after = context.tcp_path_snapshot(0).expect("after snapshot");

    assert_eq!(after.bytes_in_flight, 0);
    assert!(
        !context.relay_path_has_bulk_model_evidence(owner.underlay, owner.index),
        "ACK of a duplicated request byte range releases inflight state but must not seed path evidence"
    );
    assert!(
        owner_progress.is_empty(),
        "ambiguous OwnerData/RepairData release must not grow request read-ahead"
    );
}

fn reserve_request_quic_capacity_calibration_for_test(
    sender: &mut RequestSenderService,
    context: &ClientPathContext,
    target: RelayPathInstance,
    valid_after: Instant,
    train_deadline: Instant,
    accepted_at: Instant,
    proof_validity: Duration,
) -> (
    QuicCapacityProofCandidate,
    crate::transport::quic::MeasurementMetrics,
) {
    let token = sender.stream_id.0.saturating_add(1_000);
    let train_bytes = (PATH_OPEN_SCORE_BYTES * 2) as u64;
    let required_proof_bytes = PATH_OPEN_SCORE_BYTES as u64;
    let ticket = QuicCapacityProbeCommandTicket::new();
    let mut lease = context
        .try_reserve_request_quic_capacity_probe(
            sender.stream_id,
            target.key.index,
            target,
            token,
            train_bytes,
            reliable_capacity_calibration_session_limit_bytes(context.mux_limits),
            sender.quic_capacity.campaign.clone(),
            valid_after,
            train_deadline,
            proof_validity,
            ticket.clone(),
        )
        .expect("reserve request QUIC capacity probe");
    lease.commit();
    let expires_at = accepted_at + proof_validity;
    let candidate = QuicCapacityProofCandidate {
        token,
        train_bytes,
        sample_floor_bytes: train_bytes,
        accounting_slack_bytes: train_bytes - required_proof_bytes,
        warmup_bytes: train_bytes - required_proof_bytes,
        required_proof_bytes,
        written_bytes: train_bytes,
        written_data_frame_count: 2,
        receipt_confirmed: true,
        received_bytes: train_bytes,
        proof_elapsed: Duration::from_millis(10),
        rate_bps: train_bytes * 800,
        accepted_at,
        expires_at,
        proof_validity,
    };
    let probe = crate::transport::quic::MeasurementMetrics {
        token,
        train_payload_bytes: train_bytes,
        sample_floor_bytes: train_bytes,
        warmup_carrier_bytes: train_bytes - required_proof_bytes,
        required_timed_carrier_bytes: required_proof_bytes,
        expires_at: train_deadline,
        phase: crate::transport::quic::MeasurementPhase::Complete,
        started_clean: true,
        write_committed: true,
        written_payload_bytes: train_bytes,
        written_data_frame_count: 2,
        total_acked_carrier_bytes: train_bytes,
        total_ack_sample_count: 2,
        warmup_acked_carrier_bytes: train_bytes - required_proof_bytes,
        warmup_ack_sample_count: 1,
        measurement_acked_carrier_bytes: required_proof_bytes,
        measurement_ack_sample_count: 1,
        timed_measurement_acked_carrier_bytes: required_proof_bytes,
        timed_measurement_ack_sample_count: 1,
        app_limited_acked_carrier_bytes: 0,
        app_limited_ack_sample_count: 0,
        timed_measurement_ack_elapsed: Some(Duration::from_millis(10)),
        native_threshold_at: Some(accepted_at),
        confirmed_at: Some(accepted_at),
        retention: proof_validity,
        receipt_received_payload_bytes: train_bytes,
        receipt_elapsed: Some(Duration::from_millis(10)),
        receipt_rtt: Some(Duration::from_millis(5)),
        receipt_at: Some(accepted_at),
        last_authoritative_in_flight: Some(0),
        last_authoritative_in_flight_at: Some(accepted_at),
        last_authoritative_sent_watermark: Some(train_bytes),
        receipt_frozen_sent_watermark: Some(train_bytes),
        current_sent_watermark: train_bytes,
    };
    sender.quic_capacity.active = Some(RequestQuicCapacityCalibration {
        target,
        token,
        publication_expires_at: train_deadline + proof_validity,
        graduated: false,
        ticket,
        _lease: lease,
    });
    (candidate, probe)
}

fn publish_request_quic_capacity_calibration_for_test(
    sender: &RequestSenderService,
    context: &ClientPathContext,
    target: RelayPathInstance,
    candidate: QuicCapacityProofCandidate,
    probe: crate::transport::quic::MeasurementMetrics,
) {
    context.health().lock().expect("path health lock").udp[target.key.index]
        .accept_request_quic_capacity_proof(candidate, probe, Instant::now())
        .expect("accept request QUIC capacity proof");
    assert_eq!(
        sender
            .quic_capacity
            .active
            .as_ref()
            .expect("request QUIC calibration")
            .ticket
            .resolution(),
        QuicCapacityProbeCommandResolution::Published
    );
}

fn install_request_quic_capacity_calibration_for_test(
    sender: &mut RequestSenderService,
    context: &ClientPathContext,
    target: RelayPathInstance,
    valid_after: Instant,
    train_deadline: Instant,
    proof_validity: Duration,
) -> QuicCapacityProofCandidate {
    let (candidate, probe) = reserve_request_quic_capacity_calibration_for_test(
        sender,
        context,
        target,
        valid_after,
        train_deadline,
        Instant::now(),
        proof_validity,
    );
    publish_request_quic_capacity_calibration_for_test(sender, context, target, candidate, probe);
    candidate
}

fn active_request_bulk_flow_registrations(
    context: &ClientPathContext,
) -> [ReliableTcpRequestBulkFlowRegistration; 2] {
    let first = context.reliable_tcp_request_bulk_flow_registration();
    let second = context.reliable_tcp_request_bulk_flow_registration();
    first.update(true, Some(UnderlayProtocol::Tcp));
    second.update(true, Some(UnderlayProtocol::Tcp));
    [first, second]
}

fn client_data_frame_for_test(stream_id: StreamId, offset: u64, payload_bytes: usize) -> Frame {
    Frame::StreamData {
        stream_id,
        offset,
        flags: StreamFlags::NONE,
        payload: Bytes::from(vec![0x5a; payload_bytes]),
    }
}

fn ack_client_frame_for_test(
    sender: &mut RequestSenderService,
    context: &ClientPathContext,
    frame: &Frame,
) {
    let (start, end, _) = reliable_stream_frame_extent(frame).expect("request data extent");
    sender.release_normalized_acked_ranges(
        context,
        &[OffsetRange::new(start, end).expect("request ACK range")],
    );
}

fn seed_client_quic_native_bulk_evidence_for_test(context: &ClientPathContext, index: usize) {
    context.health().lock().expect("path health lock").udp[index].mark_quic_path_metrics(
        UdpPathMetrics {
            direction: 1,
            srtt: Duration::from_millis(20),
            rttvar: Duration::from_millis(2),
            min_rtt: Duration::from_millis(18),
            min_rtt_observed: true,
            delivery_rate_bps: 500_000_000.0,
            pacing_rate_bps: 500_000_000.0,
            inflight_hi: 4 * 1024 * 1024,
            bytes_in_flight: 0,
            pending_bytes: 0,
            loss_ppm: Some(0),
            ecn_ppm: Some(0),
            app_limited: false,
            ack_derived_data_seen: true,
            delivery_sample_count: 1,
            delivery_sample_bytes: 4 * 1024 * 1024,
            last_delivery_sample_at: Some(Instant::now()),
            bulk_proof_expires_at: None,
            latest_delivery_sample_bytes: 4 * 1024 * 1024,
            latest_delivery_sample_count: 1,
            latest_carrier_ack_elapsed: Some(Duration::from_millis(20)),
            latest_rate_sample_elapsed: Some(Duration::from_millis(20)),
            capacity_proof_candidate: None,
            capacity_probe: None,
            #[cfg(feature = "lab-diagnostics")]
            ack_poll: QuicAckPollDiagnostics::default(),
        },
    );
}

#[tokio::test]
async fn client_ack_gap_model_separates_owner_transport_from_repair_output() {
    let stream_id = StreamId(90);
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10260?srtt-ms=500&rate-mbps=400",
        "udp://127.0.0.1:10261?srtt-ms=40&rate-mbps=200",
        "udp://127.0.0.1:10262?srtt-ms=5&rate-mbps=500",
    ]);
    let (tcp_commands, _tcp_receivers) = reliable_path_command_channels(8);
    let (udp_commands, _udp_receivers) = reliable_path_command_channels(1);
    let (proof_only_commands, mut proof_only_receivers) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream_with_underlay(
            stream_id,
            UnderlayProtocol::Udp,
            0,
            udp_commands.clone(),
        ),
        8,
    );
    remotes.attach_for_validation(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Tcp,
        0,
        tcp_commands,
    ));
    remotes.attach_for_validation(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        1,
        proof_only_commands,
    ));
    consume_client_validation_proof_for_test(&mut proof_only_receivers);

    let limits = MuxLimits::default();
    let mut send_stream = ReliableSendStream::new(stream_id, limits);
    let blocked = send_stream
        .send_data(Bytes::from(vec![0x41; 4096]), StreamFlags::NONE)
        .expect("blocked owner data");
    send_stream
        .send_data(Bytes::from(vec![0x42; 4096]), StreamFlags::NONE)
        .expect("later delivered data");
    let mut sender = RequestSenderService::new(stream_id);
    sender.record_owner_frame_for_test(
        remotes
            .paths
            .iter()
            .find(|path| path.key().underlay == UnderlayProtocol::Tcp)
            .map(ReliableRelayRemotePath::instance)
            .expect("slow TCP validation owner"),
        &blocked,
    );
    let ranges = [OffsetRange {
        start: 4096,
        end: 8192,
    }];

    let (unproven_owner, owner_timing_path, unproven_repair_path) = sender
        .ack_gap_repair_path_model(
            &context,
            &remotes,
            &send_stream,
            &ranges,
            64 * 1024,
            FlowLane::Throughput,
        );
    assert_eq!(unproven_owner, Some(UnderlayProtocol::Tcp));
    assert_eq!(
        owner_timing_path.map(|snapshot| snapshot.srtt_ms),
        Some(500.0),
        "persistent-gap proof time follows the slow exact owner rather than the 40 ms Active repair output"
    );
    assert!(
        unproven_repair_path.is_none(),
        "a proof-only Validation output may carry a bounded repair quantum but must not authorize a BDP-sized burst from configured hints"
    );
    seed_client_bulk_evidence_for_test(
        &context,
        RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        },
    );
    let (owner_underlay, owner_timing_path, repair_path) = sender.ack_gap_repair_path_model(
        &context,
        &remotes,
        &send_stream,
        &ranges,
        64 * 1024,
        FlowLane::Throughput,
    );

    assert_eq!(owner_underlay, Some(UnderlayProtocol::Tcp));
    assert_eq!(
        owner_timing_path.map(|snapshot| snapshot.underlay),
        Some(UnderlayProtocol::Tcp)
    );
    assert_eq!(
        repair_path.map(|(_, snapshot)| snapshot.underlay),
        Some(UnderlayProtocol::Udp),
        "the exact ACK-gap selector must avoid the TCP owner and model the distinct QUIC repair output"
    );
    let (repair_target, repair_path) = repair_path.expect("distinct repair output");
    assert!(
        reliable_persistent_ack_gap_repair_limit_bytes(
            Some(repair_path),
            owner_underlay,
            FlowLane::Throughput,
            limits.max_repair_bytes,
            limits,
        ) > adaptive_reliable_relay_repair_bytes(Some(repair_path), FlowLane::Throughput, limits,),
        "TCP owner persistence controls amplification even when QUIC carries the repair"
    );

    seed_client_bulk_evidence_for_test(
        &context,
        RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 1,
        },
    );

    udp_commands
        .try_enqueue_admitted_frame(
            Frame::StreamData {
                stream_id: StreamId(91),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"busy"),
            },
            FlowLane::Throughput,
        )
        .expect("fill the modeled repair output after sizing");
    let bound_cause = RelaySendCause::persistent_client_ack_gap_repair(
        repair_target,
        repair_path,
        FlowLane::Throughput,
    );
    assert!(matches!(
        sender
            .send_repair_frame(&context, &mut remotes, blocked.clone(), bound_cause,)
            .await,
        Err(RuntimeError::SenderServiceBlocked)
    ));
    assert!(
        try_recv_reliable_path_command(&mut proof_only_receivers).is_none(),
        "an amplified batch stays bound to the modeled output instead of switching to another proven output"
    );

    let replacement = remotes
        .paths
        .iter_mut()
        .find(|path| path.instance() == repair_target.instance)
        .expect("modeled repair attachment remains present");
    replacement.instance_id = replacement.instance_id.saturating_add(1);
    assert!(matches!(
        sender
            .send_repair_frame(&context, &mut remotes, blocked.clone(), bound_cause)
            .await,
        Err(RuntimeError::ReliablePathSessionClosed)
    ));
    let mut queue = ReliableRelaySenderQueue::default();
    queue.push_critical_repair_with_cause(blocked, bound_cause);
    let dispatch = sender
        .dispatch_client_queued_work(
            &context,
            &ReliableRelayOpenSpec {
                target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80))),
                ingress: IngressKind::Socks5,
            },
            FlowLane::Throughput,
            FlowLane::Throughput,
            &mut remotes,
            &mut send_stream,
            &mut queue,
            true,
            &HashSet::new(),
            4096,
        )
        .await
        .expect("stale bound repair is cancelled without aborting the stream");
    assert!(matches!(
        dispatch,
        ClientQueuedDispatch::PersistentRepairCancelled
    ));
    assert!(queue.is_empty());
}

#[tokio::test]
async fn client_recv_progress_backpressure_is_retryable_not_stream_fatal() {
    let stream_id = StreamId(92);
    let context = client_test_context();
    let (commands, mut receivers) = reliable_path_command_channels(1);
    commands
        .try_enqueue_admitted_frame(
            Frame::StreamAck {
                stream_id,
                complete: false,
                ranges: Vec::new(),
            },
            FlowLane::Control,
        )
        .expect("prefill priority queue");
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, commands), 4);
    let mut recv_stream = ReliableRecvStream::new(stream_id, MuxLimits::default());
    recv_stream
        .receive_data(0, Bytes::from_static(b"reply"), StreamFlags::NONE)
        .expect("receive response bytes");
    let mut progress = ReliableRecvProgress::default();
    let mut sender = RequestSenderService::new(stream_id);

    let sent = sender
        .send_recv_progress(
            &mut remotes,
            &context,
            &recv_stream,
            &mut progress,
            RelayRecvProgressSend::new(None, FlowLane::Throughput, false),
        )
        .await
        .expect("recv progress backpressure should not close the product stream");

    assert!(!sent, "blocked advisory progress must report no frame sent");
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
    ));

    let retried = sender
        .send_recv_progress(
            &mut remotes,
            &context,
            &recv_stream,
            &mut progress,
            RelayRecvProgressSend::new(None, FlowLane::Throughput, false),
        )
        .await
        .expect("recv progress should retry once queue capacity returns");

    assert!(
        retried,
        "progress watermark must roll back after a blocked enqueue"
    );
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
    ));
}

#[tokio::test]
async fn client_recv_progress_uses_available_control_queue_instead_of_full_low_eta_path() {
    let stream_id = StreamId(93);
    let first_path = "tcp://127.0.0.1:10251"
        .parse::<PathSpec>()
        .expect("first path");
    let second_path = "tcp://127.0.0.1:10252"
        .parse::<PathSpec>()
        .expect("second path");
    let context = ClientPathContext::new(
        vec![first_path, second_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    let (first_commands, mut first_rx) = reliable_path_command_channels(1);
    first_commands
        .try_enqueue_admitted_frame(
            Frame::StreamAck {
                stream_id,
                complete: false,
                ranges: Vec::new(),
            },
            FlowLane::Control,
        )
        .expect("prefill first priority queue");
    let (second_commands, mut second_rx) = reliable_path_command_channels(1);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, first_commands), 4);
    remotes.attach(opened_test_relay_stream(stream_id, 1, second_commands));
    let mut recv_stream = ReliableRecvStream::new(stream_id, MuxLimits::default());
    recv_stream
        .receive_data(0, Bytes::from_static(b"reply"), StreamFlags::NONE)
        .expect("receive response bytes");
    let mut progress = ReliableRecvProgress::default();
    let mut sender = RequestSenderService::new(stream_id);

    let sent = sender
        .send_recv_progress(
            &mut remotes,
            &context,
            &recv_stream,
            &mut progress,
            RelayRecvProgressSend::new(None, FlowLane::Throughput, false),
        )
        .await
        .expect("available alternate control queue should accept recv progress");

    assert!(sent);
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut first_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
    ));
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut second_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
    ));
}

#[tokio::test]
async fn client_recv_progress_prefers_active_service_path_over_validation_probe() {
    let stream_id = StreamId(96);
    let tcp_path = "tcp://127.0.0.1:10270?srtt-ms=500&rate-mbps=50"
        .parse::<PathSpec>()
        .expect("tcp path");
    let udp_path = "udp://127.0.0.1:10271?srtt-ms=5&rate-mbps=500"
        .parse::<PathSpec>()
        .expect("udp path");
    let context = ClientPathContext::new(
        vec![tcp_path, udp_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    let (tcp_commands, mut tcp_rx) = reliable_path_command_channels(8);
    let (udp_commands, _udp_rx) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream_with_underlay(stream_id, UnderlayProtocol::Tcp, 0, tcp_commands),
        8,
    );
    remotes.attach_for_validation(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        0,
        udp_commands,
    ));
    let mut recv_stream = ReliableRecvStream::new(stream_id, MuxLimits::default());
    recv_stream
        .receive_data(0, Bytes::from_static(b"reply"), StreamFlags::NONE)
        .expect("receive response bytes");
    let mut progress = ReliableRecvProgress::default();
    let mut sender = RequestSenderService::new(stream_id);

    let sent = sender
        .send_recv_progress(
            &mut remotes,
            &context,
            &recv_stream,
            &mut progress,
            RelayRecvProgressSend::new(None, FlowLane::Throughput, false),
        )
        .await
        .expect("recv progress should use the active service return path");

    assert!(sent);
    assert!(
        matches!(
            try_recv_reliable_path_priority_command(&mut tcp_rx),
            Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
        ),
        "STREAM_ACK for received OwnerData should prefer the Active Service path; a lower-ETA validation probe must not own the product ACK clock while the Service path is usable"
    );
}

#[tokio::test]
async fn client_stall_recv_progress_prefers_accepted_repair_path() {
    let stream_id = StreamId(97);
    let tcp_path = "tcp://127.0.0.1:10272?srtt-ms=5&rate-mbps=500"
        .parse::<PathSpec>()
        .expect("tcp path");
    let udp_path = "udp://127.0.0.1:10273?srtt-ms=500&rate-mbps=50"
        .parse::<PathSpec>()
        .expect("udp path");
    let context = ClientPathContext::new(
        vec![tcp_path, udp_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    let (tcp_commands, mut tcp_rx) = reliable_path_command_channels(8);
    let (udp_commands, mut udp_rx) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream_with_underlay(stream_id, UnderlayProtocol::Tcp, 0, tcp_commands),
        8,
    );
    remotes.attach_for_repair(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        0,
        udp_commands,
    ));
    let mut recv_stream = ReliableRecvStream::new(stream_id, MuxLimits::default());
    recv_stream
        .receive_data(0, Bytes::from_static(b"reply"), StreamFlags::NONE)
        .expect("receive response bytes");
    let mut progress = ReliableRecvProgress::default();
    let mut sender = RequestSenderService::new(stream_id);

    let ordinary_sent = sender
        .send_recv_progress(
            &mut remotes,
            &context,
            &recv_stream,
            &mut progress,
            RelayRecvProgressSend::new(None, FlowLane::Latency, true),
        )
        .await
        .expect("ordinary receive progress should use Active");

    assert!(ordinary_sent);
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut tcp_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
    ));
    while try_recv_reliable_path_priority_command(&mut tcp_rx).is_some() {}
    assert!(try_recv_reliable_path_priority_command(&mut udp_rx).is_none());

    let mut progress = ReliableRecvProgress::default();
    let sent = sender
        .send_recv_progress(
            &mut remotes,
            &context,
            &recv_stream,
            &mut progress,
            RelayRecvProgressSend::new(None, FlowLane::Latency, true).recover_stalled_service(),
        )
        .await
        .expect("stall receive progress should use an accepted repair carrier");

    assert!(sent);
    assert!(
        try_recv_reliable_path_priority_command(&mut tcp_rx).is_none(),
        "the stalled Active path must not keep the recovery ACK when Repair is usable"
    );
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut udp_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
    ));
    assert_eq!(
        remotes.active_path_key(),
        Some(RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        }),
        "routing recovery control over Repair must not promote it to Active"
    );
}

#[tokio::test]
async fn client_stall_recv_progress_falls_back_to_active_when_repair_is_full() {
    let stream_id = StreamId(98);
    let context = client_test_context();
    let (active_commands, mut active_rx) = reliable_path_command_channels(1);
    let (repair_commands, mut repair_rx) = reliable_path_command_channels(1);
    repair_commands
        .try_enqueue_admitted_frame(
            Frame::StreamAck {
                stream_id,
                complete: false,
                ranges: Vec::new(),
            },
            FlowLane::Control,
        )
        .expect("prefill repair control queue");
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream_with_underlay(
            stream_id,
            UnderlayProtocol::Tcp,
            0,
            active_commands,
        ),
        4,
    );
    remotes.attach_for_repair(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        0,
        repair_commands,
    ));
    let mut recv_stream = ReliableRecvStream::new(stream_id, MuxLimits::default());
    recv_stream
        .receive_data(0, Bytes::from_static(b"reply"), StreamFlags::NONE)
        .expect("receive response bytes");
    let mut progress = ReliableRecvProgress::default();
    let mut sender = RequestSenderService::new(stream_id);

    let sent = sender
        .send_recv_progress(
            &mut remotes,
            &context,
            &recv_stream,
            &mut progress,
            RelayRecvProgressSend::new(None, FlowLane::Latency, true).recover_stalled_service(),
        )
        .await
        .expect("a full repair queue should fall back to Active");

    assert!(sent);
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut active_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
    ));
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut repair_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
    ));
    assert!(try_recv_reliable_path_priority_command(&mut repair_rx).is_none());
}

#[tokio::test]
async fn client_stall_recv_progress_never_uses_validation_path() {
    let stream_id = StreamId(99);
    let context = client_test_context();
    let (active_commands, mut active_rx) = reliable_path_command_channels(1);
    active_commands
        .try_enqueue_admitted_frame(
            Frame::StreamAck {
                stream_id,
                complete: false,
                ranges: Vec::new(),
            },
            FlowLane::Control,
        )
        .expect("prefill active control queue");
    let (validation_commands, mut validation_rx) = reliable_path_command_channels(2);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream_with_underlay(
            stream_id,
            UnderlayProtocol::Tcp,
            0,
            active_commands,
        ),
        4,
    );
    remotes.attach_for_validation(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        0,
        validation_commands,
    ));
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut validation_rx),
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
    ));
    let mut recv_stream = ReliableRecvStream::new(stream_id, MuxLimits::default());
    recv_stream
        .receive_data(0, Bytes::from_static(b"reply"), StreamFlags::NONE)
        .expect("receive response bytes");
    let mut progress = ReliableRecvProgress::default();
    let mut sender = RequestSenderService::new(stream_id);

    let sent = sender
        .send_recv_progress(
            &mut remotes,
            &context,
            &recv_stream,
            &mut progress,
            RelayRecvProgressSend::new(None, FlowLane::Latency, true).recover_stalled_service(),
        )
        .await
        .expect("blocked recovery feedback remains retryable");

    assert!(!sent);
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut active_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
    ));
    assert!(
        try_recv_reliable_path_priority_command(&mut validation_rx).is_none(),
        "Validation must remain product-ineligible during ACK recovery"
    );
}

#[tokio::test]
async fn client_subflow_data_preserves_service_owner_after_frontier_clear_selection() {
    let stream_id = StreamId(94);
    let slow_path = "tcp://127.0.0.1:10261?srtt-ms=500&rate-mbps=50"
        .parse::<PathSpec>()
        .expect("slow path");
    let fast_path = "tcp://127.0.0.1:10262?srtt-ms=5&rate-mbps=500"
        .parse::<PathSpec>()
        .expect("fast path");
    let context = ClientPathContext::new(
        vec![slow_path, fast_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    let (slow_commands, _slow_rx) = reliable_path_command_channels(8);
    let (fast_commands, mut fast_rx) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, slow_commands), 8);
    remotes.attach(opened_test_relay_stream(stream_id, 1, fast_commands));
    let slow_key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    let fast_key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 1,
    };
    let slow_instance = remotes
        .path_instance_for_key(slow_key)
        .expect("stable Service instance");
    let mut sender = RequestSenderService::new(stream_id);
    sender.request.ordered_service = Some(slow_instance);
    assert_ne!(remotes.active_path_instance(), Some(slow_instance));
    assert_eq!(
        sender.request_ordered_service_instance(),
        Some(slow_instance),
        "the product epoch follows ordered ownership, not the newest Active placement"
    );

    let frame = Frame::StreamData {
        stream_id,
        offset: 0,
        flags: StreamFlags::NONE,
        payload: Bytes::from(vec![0xab; 64 * 1024]),
    };
    let outcome = sender
        .send_stream_data(&context, &mut remotes, frame)
        .await
        .expect("frontier-clear owner data should migrate to the faster admitted active path");

    assert_eq!(outcome.path_key, fast_key);
    assert_eq!(
        sender.request.ordered_service_key(),
        Some(slow_key),
        "a selected Subflow owns its exact ranges without silently replacing the stable Service anchor"
    );
    assert_eq!(
        sender.request_ordered_service_instance(),
        Some(slow_instance),
        "Subflow data must not reset the stable Service product window"
    );
    assert!(matches!(
        try_recv_reliable_path_command(&mut fast_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
}

#[tokio::test]
async fn client_fresh_validation_proof_enables_startup_data_without_replacing_service() {
    let stream_id = StreamId(100);
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10280?srtt-ms=20&rate-mbps=500",
        "tcp://127.0.0.1:10281?srtt-ms=10&rate-mbps=500",
    ]);
    let _request_bulk_flows = active_request_bulk_flow_registrations(&context);
    let service_key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    let candidate_key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 1,
    };
    seed_client_bulk_evidence_for_test(&context, service_key);

    let (service_commands, mut service_rx) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream(stream_id, service_key.index, service_commands),
        8,
    );
    let mut sender = RequestSenderService::new(stream_id);
    let service_frame = client_data_frame_for_test(stream_id, 0, PATH_OPEN_SCORE_BYTES);
    let service_outcome = sender
        .send_stream_data(&context, &mut remotes, service_frame.clone())
        .await
        .expect("establish request Service owner");
    assert_eq!(service_outcome.path_key, service_key);
    assert!(matches!(
        try_recv_reliable_path_command(&mut service_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
    ack_client_frame_for_test(&mut sender, &context, &service_frame);

    let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(8);
    remotes.attach_for_validation(opened_test_relay_stream(
        stream_id,
        candidate_key.index,
        candidate_commands,
    ));
    let candidate_instance = remotes
        .path_instance_for_key(candidate_key)
        .expect("validation instance");
    consume_client_validation_proof_for_test(&mut candidate_rx);
    mark_client_validation_proof_fresh_for_test(
        &context,
        &remotes,
        candidate_instance,
        Duration::from_millis(10),
    );

    let startup_frame =
        client_data_frame_for_test(stream_id, PATH_OPEN_SCORE_BYTES as u64, 8 * 1024);
    let startup_outcome = sender
        .send_stream_data(&context, &mut remotes, startup_frame)
        .await
        .expect("freshly proven Validation should receive bounded request data");

    assert_eq!(startup_outcome.path_key, candidate_key);
    assert!(matches!(
        try_recv_reliable_path_command(&mut candidate_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
    assert!(try_recv_reliable_path_command(&mut service_rx).is_none());
    assert_eq!(sender.request.ordered_service_key(), Some(service_key));
    assert_eq!(remotes.active_path_key(), Some(service_key));
    assert_eq!(
        remotes
            .paths
            .iter()
            .find(|path| path.instance() == candidate_instance)
            .map(|path| path.placement),
        Some(RelayPathPlacement::Validation)
    );
    assert_eq!(
        sender
            .request
            .startup
            .epoch
            .as_ref()
            .and_then(FlowSubflowSet::startup_owner_key),
        Some(candidate_instance)
    );
    assert!(
        !context.relay_path_has_bulk_model_evidence(candidate_key.underlay, candidate_key.index,),
        "PATH_PROOF enables only the bounded startup epoch"
    );
}

#[tokio::test]
async fn client_request_startup_does_not_borrow_reverse_promoted_relay_lane() {
    let stream_id = StreamId(123);
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10320?srtt-ms=20&rate-mbps=500",
        "tcp://127.0.0.1:10321?srtt-ms=10&rate-mbps=500",
    ]);
    let _other_request_bulk_flows = active_request_bulk_flow_registrations(&context);
    let service_key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    let candidate_key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 1,
    };
    seed_client_bulk_evidence_for_test(&context, service_key);

    let (service_commands, mut service_rx) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream(stream_id, service_key.index, service_commands),
        8,
    );
    let mut send_stream = ReliableSendStream::new(stream_id, context.mux_limits);
    let service_frame = send_stream
        .send_data(
            Bytes::from(vec![0x41; PATH_OPEN_SCORE_BYTES]),
            StreamFlags::NONE,
        )
        .expect("initial Service request frame");
    let mut sender = RequestSenderService::new(stream_id);
    sender
        .send_stream_data(&context, &mut remotes, service_frame.clone())
        .await
        .expect("establish request Service owner");
    assert!(matches!(
        try_recv_reliable_path_command(&mut service_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
    ack_client_frame_for_test(&mut sender, &context, &service_frame);
    let service_range =
        OffsetRange::new(0, PATH_OPEN_SCORE_BYTES as u64).expect("initial Service request range");
    let _ = send_stream.apply_ack(&[service_range]);

    let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(8);
    remotes.attach_for_validation(opened_test_relay_stream(
        stream_id,
        candidate_key.index,
        candidate_commands,
    ));
    let candidate = remotes
        .path_instance_for_key(candidate_key)
        .expect("Validation candidate");
    consume_client_validation_proof_for_test(&mut candidate_rx);
    mark_client_validation_proof_fresh_for_test(
        &context,
        &remotes,
        candidate,
        Duration::from_millis(10),
    );

    let spec = ReliableRelayOpenSpec {
        target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80))),
        ingress: IngressKind::Socks5,
    };
    let mut sender_queue = ReliableRelaySenderQueue::default();
    sender_queue.push_data(Bytes::from(vec![0x42; 8 * 1024]));
    sender
        .dispatch_client_queued_work(
            &context,
            &spec,
            FlowLane::Throughput,
            FlowLane::Latency,
            &mut remotes,
            &mut send_stream,
            &mut sender_queue,
            true,
            &HashSet::new(),
            8 * 1024,
        )
        .await
        .expect("latency request stays on Service");
    assert!(matches!(
        try_recv_reliable_path_command(&mut service_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
    assert!(try_recv_reliable_path_command(&mut candidate_rx).is_none());
    assert_eq!(
        sender
            .request
            .startup
            .epoch
            .as_ref()
            .and_then(FlowSubflowSet::startup_owner_key),
        None,
        "reverse-direction bulk classification must not authorize request exploration"
    );

    sender_queue.push_data(Bytes::from(vec![0x43; 8 * 1024]));
    sender
        .dispatch_client_queued_work(
            &context,
            &spec,
            FlowLane::Throughput,
            FlowLane::Throughput,
            &mut remotes,
            &mut send_stream,
            &mut sender_queue,
            true,
            &HashSet::new(),
            8 * 1024,
        )
        .await
        .expect("request-direction bulk classification enables bounded startup");
    assert!(matches!(
        try_recv_reliable_path_command(&mut candidate_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
    assert_eq!(
        sender
            .request
            .startup
            .epoch
            .as_ref()
            .and_then(FlowSubflowSet::startup_owner_key),
        Some(candidate)
    );
}

#[tokio::test]
async fn client_path_failure_unpublishes_contention_before_cleanup_waits() {
    let stream_id = StreamId(124);
    let context = Arc::new(client_test_context());
    let registration = context.reliable_tcp_request_bulk_flow_registration();
    registration.update(true, Some(UnderlayProtocol::Tcp));
    assert_eq!(context.active_tcp_service_request_bulk_flows(), 1);

    let (commands, mut receivers) = reliable_path_command_channels(1);
    commands
        .send_control(ReliablePathCommand::CloseStream(StreamId(999)))
        .await
        .expect("prefill cleanup control queue");
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, commands), 1);
    let service = remotes.active_path_instance().expect("active Service");
    let mut sender = RequestSenderService::new(stream_id);
    sender.request.ordered_service = Some(service);
    sender.bind_request_bulk_flow_registration(registration.clone());

    let task_context = context.clone();
    let failure = tokio::spawn(async move {
        let removed = sender
            .fail_client_path_instance(&task_context, &mut remotes, service)
            .await;
        (removed, sender, remotes)
    });
    tokio::task::yield_now().await;

    assert_eq!(
        context.active_tcp_service_request_bulk_flows(),
        0,
        "a removed Service must stop authorizing concurrent exploration before cleanup can await"
    );
    assert!(
        !failure.is_finished(),
        "the full control queue must keep detach cleanup pending for the race assertion"
    );
    assert!(matches!(
        recv_reliable_path_command(&mut receivers).await,
        Some(ReliablePathCommand::CloseStream(StreamId(999)))
    ));
    assert!(matches!(
        recv_reliable_path_command(&mut receivers).await,
        Some(ReliablePathCommand::SendFrame(Frame::StreamDetach { stream_id: id }))
            if id == stream_id
    ));
    assert!(matches!(
        recv_reliable_path_command(&mut receivers).await,
        Some(ReliablePathCommand::CloseStream(id)) if id == stream_id
    ));
    let (removed, _, remotes) = failure.await.expect("path failure task");
    assert!(removed);
    assert!(remotes.is_empty());
}

#[tokio::test]
async fn client_path_failure_releases_optional_load_before_cleanup_waits() {
    let stream_id = StreamId(125);
    let context = Arc::new(client_test_context_with_paths(&[
        "tcp://127.0.0.1:10331?srtt-ms=20&rate-mbps=500",
        "tcp://127.0.0.1:10332?srtt-ms=20&rate-mbps=500",
    ]));
    let (service_commands, _service_rx) = reliable_path_command_channels(1);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, service_commands), 2);
    let service = remotes.active_path_instance().expect("active Service");
    let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(1);
    candidate_commands
        .send_control(ReliablePathCommand::CloseStream(StreamId(999)))
        .await
        .expect("prefill cleanup control queue");
    remotes.attach_for_validation(opened_test_relay_stream(stream_id, 1, candidate_commands));
    let candidate = remotes
        .path_instance_for_key(RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 1,
        })
        .expect("Validation candidate");
    let lease = context
        .try_reserve_relay_path_load_if_unchanged(candidate.key, FlowLane::Throughput, 0, 0)
        .expect("reserve optional path load");
    assert!(
        remotes
            .commit_path_instance_load_claim(candidate, lease)
            .is_ok(),
        "commit optional path load"
    );
    let registration = context.reliable_tcp_request_bulk_flow_registration();
    registration.update(true, Some(UnderlayProtocol::Tcp));
    let mut sender = RequestSenderService::new(stream_id);
    sender.request.ordered_service = Some(service);
    sender.bind_request_bulk_flow_registration(registration);

    let task_context = context.clone();
    let failure = tokio::spawn(async move {
        let removed = sender
            .fail_client_path_instance(&task_context, &mut remotes, candidate)
            .await;
        (removed, sender, remotes)
    });
    tokio::task::yield_now().await;

    assert_eq!(
        context.health().lock().expect("path health lock").tcp[1].active_flows,
        0,
        "a removed optional path must release load before detach can block"
    );
    assert_eq!(
        context.active_tcp_service_request_bulk_flows(),
        1,
        "optional cleanup must not unpublish the still-live TCP Service"
    );
    assert!(!failure.is_finished());
    assert!(matches!(
        recv_reliable_path_command(&mut candidate_rx).await,
        Some(ReliablePathCommand::CloseStream(StreamId(999)))
    ));
    loop {
        match recv_reliable_path_command(&mut candidate_rx).await {
            Some(ReliablePathCommand::SendFrame(Frame::StreamDetach { stream_id: id }))
                if id == stream_id =>
            {
                break;
            }
            Some(_) => continue,
            None => panic!("candidate command channel closed before detach"),
        }
    }
    assert!(matches!(
        recv_reliable_path_command(&mut candidate_rx).await,
        Some(ReliablePathCommand::CloseStream(id)) if id == stream_id
    ));
    let (removed, _, _) = failure.await.expect("path failure task");
    assert!(removed);
}

#[tokio::test]
async fn client_startup_credit_is_cumulative_and_stream_acks_do_not_refill_it() {
    let stream_id = StreamId(101);
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10282?srtt-ms=20&rate-mbps=500",
        "tcp://127.0.0.1:10283?srtt-ms=10&rate-mbps=500",
    ]);
    let _request_bulk_flows = active_request_bulk_flow_registrations(&context);
    let service_key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    let candidate_key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 1,
    };
    seed_client_bulk_evidence_for_test(&context, service_key);

    let (service_commands, mut service_rx) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream(stream_id, service_key.index, service_commands),
        8,
    );
    let mut sender = RequestSenderService::new(stream_id);
    let mut offset = 0_u64;
    let service_frame = client_data_frame_for_test(stream_id, offset, PATH_OPEN_SCORE_BYTES);
    sender
        .send_stream_data(&context, &mut remotes, service_frame.clone())
        .await
        .expect("establish request Service owner");
    assert!(matches!(
        try_recv_reliable_path_command(&mut service_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
    ack_client_frame_for_test(&mut sender, &context, &service_frame);
    offset = offset.saturating_add(PATH_OPEN_SCORE_BYTES as u64);

    let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(8);
    remotes.attach_for_validation(opened_test_relay_stream(
        stream_id,
        candidate_key.index,
        candidate_commands,
    ));
    let candidate_instance = remotes
        .path_instance_for_key(candidate_key)
        .expect("validation instance");
    consume_client_validation_proof_for_test(&mut candidate_rx);
    mark_client_validation_proof_fresh_for_test(
        &context,
        &remotes,
        candidate_instance,
        Duration::from_millis(10),
    );

    let startup_limit = usize::try_from(reliable_subflow_startup_sample_limit_bytes(
        context.mux_limits,
    ))
    .expect("startup limit");
    let ack_chunk = 8 * 1024;
    assert!(ack_chunk < PATH_OPEN_SCORE_BYTES);
    let mut startup_sent = 0_usize;
    while startup_sent < startup_limit {
        let payload_bytes = ack_chunk.min(startup_limit - startup_sent);
        let frame = client_data_frame_for_test(stream_id, offset, payload_bytes);
        let outcome = sender
            .send_stream_data(&context, &mut remotes, frame.clone())
            .await
            .expect("startup request sample within cumulative credit");
        assert_eq!(outcome.path_key, candidate_key);
        assert!(matches!(
            try_recv_reliable_path_command(&mut candidate_rx),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
        ));
        ack_client_frame_for_test(&mut sender, &context, &frame);
        if startup_sent.saturating_add(payload_bytes) < startup_limit {
            assert!(
                !context.relay_path_has_bulk_model_evidence(
                    candidate_key.underlay,
                    candidate_key.index,
                ),
                "fragmented ACKs must not create bulk evidence before cumulative startup evidence reaches its floor"
            );
        }
        startup_sent = startup_sent.saturating_add(payload_bytes);
        offset = offset.saturating_add(payload_bytes as u64);
    }

    let epoch = sender
        .request
        .startup
        .epoch
        .as_ref()
        .expect("request startup epoch");
    let candidate_member = epoch
        .members()
        .iter()
        .find(|member| member.key == candidate_instance)
        .expect("startup candidate member");
    assert_eq!(candidate_member.owner_sent_bytes, startup_limit as u64);
    let (receipt_proof_id, _) = sender
        .request
        .startup
        .receipt_proofs
        .get(&candidate_instance)
        .copied()
        .expect("exhausted startup credit queues one ordered receipt proof");
    assert!(matches!(
        try_recv_reliable_path_command(&mut candidate_rx),
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData {
            proof_id,
            ..
        })) if proof_id == receipt_proof_id
    ));

    let (delivery_samples, delivery_bytes) = {
        let health = context.health().lock().expect("path health lock");
        let candidate = &health.tcp[candidate_key.index];
        (
            candidate.delivery_samples,
            candidate.product_delivery_sample_bytes,
        )
    };
    sender.release_normalized_acked_ranges(&context, &[]);
    let health = context.health().lock().expect("path health lock");
    assert_eq!(
        health.tcp[candidate_key.index].delivery_samples,
        delivery_samples
    );
    assert_eq!(
        health.tcp[candidate_key.index].product_delivery_sample_bytes, delivery_bytes,
        "an unrelated ACK event must not republish a completed cumulative startup sample"
    );
    drop(health);

    let after_cap = client_data_frame_for_test(stream_id, offset, ack_chunk);
    let outcome = sender
        .send_stream_data(&context, &mut remotes, after_cap)
        .await
        .expect("graduated scheduling resumes after cumulative startup cap");
    match outcome.path_key {
        key if key == service_key => assert!(matches!(
            try_recv_reliable_path_command(&mut service_rx),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
        )),
        key if key == candidate_key => assert!(matches!(
            try_recv_reliable_path_command(&mut candidate_rx),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
        )),
        key => panic!("unexpected post-graduation path: {key:?}"),
    }
    let epoch = sender
        .request
        .startup
        .epoch
        .as_ref()
        .expect("graduated request epoch");
    assert_eq!(epoch.startup_owner_key(), None);
    assert_eq!(
        epoch
            .members()
            .iter()
            .find(|member| member.key == candidate_instance)
            .expect("retained graduated member")
            .owner_sent_bytes,
        startup_limit as u64,
        "ACK release and ordinary measured sends must not refill or extend startup credit"
    );
    assert_eq!(sender.request.ordered_service_key(), Some(service_key));
}

#[tokio::test]
async fn near_cap_startup_sample_seals_when_next_frame_cannot_fit() {
    let stream_id = StreamId(115);
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10305?srtt-ms=20&rate-mbps=500",
        "tcp://127.0.0.1:10306?srtt-ms=10&rate-mbps=500",
    ]);
    let _request_bulk_flows = active_request_bulk_flow_registrations(&context);
    let service_key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    let candidate_key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 1,
    };
    seed_client_bulk_evidence_for_test(&context, service_key);

    let (service_commands, mut service_rx) = reliable_path_command_channels(16);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream(stream_id, service_key.index, service_commands),
        16,
    );
    let mut sender = RequestSenderService::new(stream_id);
    let mut offset = 0_u64;
    let service_frame = client_data_frame_for_test(stream_id, offset, PATH_OPEN_SCORE_BYTES);
    sender
        .send_stream_data(&context, &mut remotes, service_frame.clone())
        .await
        .expect("establish request Service owner");
    assert!(matches!(
        try_recv_reliable_path_command(&mut service_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
    ack_client_frame_for_test(&mut sender, &context, &service_frame);
    offset = offset.saturating_add(PATH_OPEN_SCORE_BYTES as u64);

    let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(16);
    remotes.attach_for_validation(opened_test_relay_stream(
        stream_id,
        candidate_key.index,
        candidate_commands,
    ));
    let candidate = remotes
        .path_instance_for_key(candidate_key)
        .expect("Validation candidate");
    consume_client_validation_proof_for_test(&mut candidate_rx);
    mark_client_validation_proof_fresh_for_test(
        &context,
        &remotes,
        candidate,
        Duration::from_millis(10),
    );

    let startup_limit = usize::try_from(reliable_subflow_startup_sample_limit_bytes(
        context.mux_limits,
    ))
    .expect("startup limit");
    let payload_bytes = 60 * 1024;
    let admitted_frames = startup_limit / payload_bytes;
    let admitted_bytes = admitted_frames * payload_bytes;
    assert!(admitted_frames > 0);
    assert!(admitted_bytes >= PATH_OPEN_SCORE_BYTES);
    assert!(startup_limit - admitted_bytes < payload_bytes);

    for _ in 0..admitted_frames {
        let frame = client_data_frame_for_test(stream_id, offset, payload_bytes);
        let outcome = sender
            .send_stream_data(&context, &mut remotes, frame.clone())
            .await
            .expect("near-cap startup sample frame");
        assert_eq!(outcome.path_key, candidate_key);
        assert!(matches!(
            try_recv_reliable_path_command(&mut candidate_rx),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
        ));
        ack_client_frame_for_test(&mut sender, &context, &frame);
        offset = offset.saturating_add(payload_bytes as u64);
    }
    assert!(sender.request.startup.receipt_proofs.is_empty());
    assert_eq!(
        sender
            .request
            .startup
            .epoch
            .as_ref()
            .and_then(|epoch| epoch.startup_owner_sealed_sample_bytes(candidate)),
        None
    );

    let next_frame = client_data_frame_for_test(stream_id, offset, payload_bytes);
    let outcome = sender
        .send_stream_data(&context, &mut remotes, next_frame)
        .await
        .expect("oversized remainder returns to Service after sealing the sample");
    assert_eq!(outcome.path_key, service_key);
    assert!(matches!(
        try_recv_reliable_path_command(&mut service_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
    assert_eq!(
        sender
            .request
            .startup
            .epoch
            .as_ref()
            .and_then(|epoch| epoch.startup_owner_sealed_sample_bytes(candidate)),
        Some(admitted_bytes as u64)
    );
    let (receipt_proof_id, _) = sender.request.startup.receipt_proofs[&candidate];
    assert!(matches!(
        try_recv_reliable_path_command(&mut candidate_rx),
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData {
            proof_id,
            ..
        })) if proof_id == receipt_proof_id
    ));

    context.mark_relay_path_proof_observation(
        candidate_key.underlay,
        candidate_key.index,
        PathProofObservation {
            proof_id: receipt_proof_id,
            bytes: PATH_OPEN_SCORE_BYTES as u64,
            elapsed: Duration::from_millis(10),
            sent_at: Instant::now(),
        },
    );
    sender.reconcile_request_subflow_set(&context, &remotes);

    assert!(
        sender
            .request
            .subflows
            .get(candidate)
            .is_some_and(|state| state.graduated())
    );
    assert!(
        sender
            .request
            .subflows
            .get(candidate)
            .is_some_and(|state| state.ack_clock_first_window()),
        "the ordered startup receipt is the causal boundary for calibration"
    );
    assert!(
        sender
            .request
            .subflows
            .get(candidate)
            .and_then(|state| state.rate_evidence())
            .is_some_and(RequestPathRateEvidence::has_ack_boundary)
    );
    let health = context.health().lock().expect("path health lock");
    assert_eq!(
        health.tcp[candidate_key.index].product_delivery_sample_bytes, admitted_bytes as u64,
        "receipt goodput must use only the bytes actually admitted before sealing"
    );
}

#[tokio::test]
async fn udp_product_window_growth_accepts_only_live_owner_capable_instances() {
    let stream_id = StreamId(117);
    let (service_commands, _service_rx) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream_with_underlay(
            stream_id,
            UnderlayProtocol::Udp,
            0,
            service_commands,
        ),
        8,
    );
    let (candidate_commands, _candidate_rx) = reliable_path_command_channels(8);
    remotes.attach_for_validation(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        1,
        candidate_commands,
    ));
    let service = remotes.active_path_instance().expect("active UDP Service");
    let candidate = remotes
        .path_instance_for_key(RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 1,
        })
        .expect("UDP Validation candidate");
    let stale_service = RelayPathInstance {
        key: service.key,
        id: service.id.wrapping_add(1000),
    };
    let mut sender = RequestSenderService::new(stream_id);
    sender.request.ordered_service = Some(service);

    assert!(
        sender.request_owner_ack_can_grow_window(&remotes, Some(service), service),
        "exact active UDP OwnerData progress should advance its product window"
    );
    assert!(
        !sender.request_owner_ack_can_grow_window(&remotes, Some(service), stale_service),
        "a detached same-key instance must not advance the current product epoch"
    );
    assert!(
        !sender.request_owner_ack_can_grow_window(&remotes, Some(service), candidate),
        "proof-only Validation is not yet an ordinary product owner"
    );

    sender.request.subflows.get_mut(candidate).mark_graduated();
    assert!(
        sender.request_owner_ack_can_grow_window(&remotes, Some(service), candidate),
        "durably graduated UDP Subflow progress may grow the same-family product window without borrowing TCP ACK-clock policy"
    );

    let (replacement_commands, _replacement_rx) = reliable_path_command_channels(8);
    remotes.attach(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        2,
        replacement_commands,
    ));
    let replacement = remotes
        .active_path_instance()
        .expect("replacement UDP Service");
    assert_ne!(replacement, service);
    assert!(
        sender.request_owner_ack_can_grow_window(&remotes, Some(service), service),
        "Active-list churn must not replace the ordered Service epoch"
    );
    sender.request.ordered_service = Some(replacement);
    assert!(
        !sender.request_owner_ack_can_grow_window(&remotes, Some(replacement), service),
        "an explicit exact Service handoff invalidates the older owner"
    );
    assert!(
        sender.request_owner_ack_can_grow_window(&remotes, Some(replacement), replacement),
        "the committed replacement owns its new product epoch"
    );
}

#[tokio::test]
async fn tcp_product_window_turnover_sums_only_live_exact_owner_models() {
    let stream_id = StreamId(118);
    let _context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10309?srtt-ms=20&rate-mbps=80",
        "tcp://127.0.0.1:10310?srtt-ms=180&rate-mbps=200",
    ]);
    let (service_commands, _service_rx) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, service_commands), 8);
    let (candidate_commands, _candidate_rx) = reliable_path_command_channels(8);
    remotes.attach_for_validation(opened_test_relay_stream(stream_id, 1, candidate_commands));
    let service = remotes.active_path_instance().expect("TCP Service");
    let candidate = remotes
        .path_instance_for_key(RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 1,
        })
        .expect("TCP candidate");
    let mut sender = RequestSenderService::new(stream_id);
    sender.request.ordered_service = Some(service);
    sender.request.subflows.get_mut(candidate).mark_graduated();
    let now = Instant::now();
    sender
        .request
        .subflows
        .get_mut(service)
        .set_tcp_ack_turnover(RequestTcpAckTurnoverModel {
            turnover_bytes: 512_000.0,
            sampled_at: now,
            sample_pto: Duration::from_secs(1),
        });
    sender
        .request
        .subflows
        .get_mut(candidate)
        .set_tcp_ack_turnover(RequestTcpAckTurnoverModel {
            turnover_bytes: 2_500_000.0,
            sampled_at: now,
            sample_pto: Duration::from_millis(100),
        });

    let service_turnover =
        sender.request_tcp_owner_ack_turnover_bytes(&remotes, Some(service), now);
    assert!(service_turnover > 0);
    sender
        .request
        .subflows
        .get_mut(candidate)
        .mark_ack_clock_proven();
    assert_eq!(
        sender.request_tcp_owner_ack_turnover_bytes(&remotes, Some(service), now),
        service_turnover,
        "a retained calibration pipe is measurement, not shared-window authority"
    );
    sender
        .request
        .subflows
        .get_mut(candidate)
        .mark_window_turnover_proven();
    let aggregate_turnover =
        sender.request_tcp_owner_ack_turnover_bytes(&remotes, Some(service), now);
    assert!(
        aggregate_turnover > service_turnover,
        "only exact ACK-clock graduation may add the candidate's own PTO turnover"
    );
    assert_eq!(
        sender.request_tcp_owner_ack_turnover_bytes(
            &remotes,
            Some(service),
            now + Duration::from_millis(300),
        ),
        service_turnover,
        "a candidate pipe is stale at the exact three-PTO boundary"
    );
    assert!(!sender.revoke_request_tcp_capacity_calibration(candidate, false));
    assert!(
        sender
            .request
            .subflows
            .get(candidate)
            .and_then(|state| state.tcp_ack_turnover())
            .is_none(),
        "full exact-instance revocation must discard retained pipe authority"
    );
    assert!(
        !sender
            .request
            .subflows
            .get(candidate)
            .is_some_and(|state| state.window_turnover_proven())
    );
    assert!(
        !sender
            .request
            .subflows
            .get(candidate)
            .is_some_and(|state| state.ack_clock_proven()),
        "full revocation must permit an exact instance to calibrate again"
    );

    sender.request.ordered_service = Some(RelayPathInstance {
        key: service.key,
        id: service.id.wrapping_add(1),
    });
    assert_eq!(
        sender.request_tcp_owner_ack_turnover_bytes(&remotes, Some(service), now),
        0,
        "a stale exact Service epoch cannot borrow retained flow models"
    );
}

#[tokio::test]
async fn graduated_candidate_calibration_produces_ack_clock_capacity_sample() {
    let stream_id = StreamId(116);
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10307?srtt-ms=20&rate-mbps=500",
        "tcp://127.0.0.1:10308?srtt-ms=10&rate-mbps=500",
    ]);
    let _request_bulk_flows = active_request_bulk_flow_registrations(&context);
    let service_key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    let candidate_key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 1,
    };
    context.mark_relay_path_rate_sample(
        service_key.underlay,
        service_key.index,
        PathRateSample::new(4 * 1024 * 1024, Duration::from_millis(64)).expect("Service rate"),
    );
    context.mark_relay_path_rate_sample(
        candidate_key.underlay,
        candidate_key.index,
        PathRateSample::new(256 * 1024, Duration::from_secs(1)).expect("receipt rate"),
    );

    let (service_commands, mut service_rx) = reliable_path_command_channels(1024);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream(stream_id, service_key.index, service_commands),
        1024,
    );
    let service = remotes
        .path_instance_for_key(service_key)
        .expect("Service instance");
    let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(1024);
    remotes.attach_for_validation(opened_test_relay_stream(
        stream_id,
        candidate_key.index,
        candidate_commands,
    ));
    let candidate = remotes
        .path_instance_for_key(candidate_key)
        .expect("Validation candidate");
    consume_client_validation_proof_for_test(&mut candidate_rx);
    mark_client_validation_proof_fresh_for_test(
        &context,
        &remotes,
        candidate,
        Duration::from_millis(10),
    );

    let mut sender = RequestSenderService::new(stream_id);
    sender.request.ordered_service = Some(service);
    sender.request.subflows.get_mut(service).mark_rate_proven();
    let candidate_state = sender.request.subflows.get_mut(candidate);
    candidate_state.mark_rate_proven();
    candidate_state.mark_graduated();
    assert!(!sender.request_owner_ack_can_grow_window(&remotes, Some(service), candidate));

    let calibration_target = usize::try_from(reliable_request_ack_clock_calibration_target_bytes(
        context.mux_limits,
    ))
    .expect("calibration target");
    let calibration_frames = (0..calibration_target.div_ceil(BBR_MAX_SEND_QUANTUM_BYTES))
        .map(|index| {
            client_data_frame_for_test(
                stream_id,
                (index * BBR_MAX_SEND_QUANTUM_BYTES) as u64,
                BBR_MAX_SEND_QUANTUM_BYTES,
            )
        })
        .collect::<Vec<_>>();
    sender
        .request
        .subflows
        .get_mut(candidate)
        .mark_ack_clock_first_window();
    sender
        .request
        .subflows
        .get_mut(candidate)
        .rate_evidence_mut(Instant::now())
        .seed_ack_boundary(Instant::now());

    let cancelled_selection = sender
        .choose_relay_path_position(
            &context,
            &remotes,
            &calibration_frames[0],
            FlowLane::Throughput,
            RelaySendCause::StreamData,
            &[],
        )
        .expect("calibration selection before carrier enqueue");
    assert!(matches!(
        cancelled_selection.request_calibration_commit,
        Some(RequestAckClockCalibrationCommit::OwnerData {
            candidate: selected,
            ..
        }) if selected == candidate
    ));
    assert_eq!(sender.request.ack_clock_operation, None);
    assert!(sender.request.subflows.iter().all(|(_, state)| {
        state.ack_clock_calibration_bytes().is_none()
            && state.ack_clock_calibration_target().is_none()
    }));
    drop(cancelled_selection);

    let mut sent_calibration_frames = Vec::new();
    for frame in &calibration_frames {
        let outcome = sender
            .send_stream_data(&context, &mut remotes, frame.clone())
            .await
            .expect("bounded ACK-clock calibration frame");
        assert_eq!(outcome.path_key, candidate_key);
        assert!(matches!(
            try_recv_reliable_path_command(&mut candidate_rx),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
        ));
        sent_calibration_frames.push(frame.clone());
        let candidate_state = sender
            .request
            .subflows
            .get(candidate)
            .expect("candidate calibration state");
        if candidate_state
            .ack_clock_calibration_bytes()
            .expect("candidate calibration spend")
            >= candidate_state
                .ack_clock_calibration_target()
                .expect("candidate calibration target")
        {
            break;
        }
    }
    let candidate_state = sender
        .request
        .subflows
        .get(candidate)
        .expect("candidate calibration state");
    assert!(
        candidate_state
            .ack_clock_calibration_bytes()
            .expect("candidate calibration spend")
            >= candidate_state
                .ack_clock_calibration_target()
                .expect("candidate calibration target")
    );
    assert_eq!(
        sender.request.ack_clock_operation,
        Some(RequestAckClockOperation::Owner {
            candidate,
            target_bytes: candidate_state
                .ack_clock_calibration_target()
                .expect("candidate calibration target"),
        })
    );
    assert!(
        !sender
            .request
            .subflows
            .get(candidate)
            .is_some_and(|state| state.ack_clock_proven())
    );
    let final_ack_start = sent_calibration_frames.len().saturating_sub(2);
    for frame in &sent_calibration_frames[..final_ack_start] {
        ack_client_frame_for_test(&mut sender, &context, frame);
    }
    assert!(sender.revoke_request_tcp_capacity_calibration(candidate, true));
    let candidate_state = sender
        .request
        .subflows
        .get(candidate)
        .expect("preserved candidate calibration state");
    assert_eq!(
        sender.request.ack_clock_operation,
        Some(RequestAckClockOperation::Owner {
            candidate,
            target_bytes: candidate_state
                .ack_clock_calibration_target()
                .expect("candidate calibration target"),
        }),
        "natural carrier expiry must preserve the sealed AwaitingAck owner"
    );
    assert!(
        !sender
            .request
            .subflows
            .get(candidate)
            .is_some_and(|state| state.ack_clock_proven())
    );
    for frame in &sent_calibration_frames[final_ack_start..] {
        ack_client_frame_for_test(&mut sender, &context, frame);
    }
    assert!(
        sender
            .request
            .subflows
            .get(candidate)
            .is_some_and(|state| state.ack_clock_proven())
    );
    assert!(
        sender
            .request
            .subflows
            .get(candidate)
            .and_then(|state| state.tcp_ack_turnover())
            .is_some(),
        "the bounded calibration ACK retains its same-epoch pipe measurement"
    );
    assert!(
        !sender
            .request
            .subflows
            .get(candidate)
            .is_some_and(|state| state.window_turnover_proven()),
        "the bounded calibration ACK cannot finance shared source-window growth"
    );
    assert_eq!(
        sender.request_tcp_owner_ack_turnover_bytes(&remotes, Some(service), Instant::now(),),
        0,
        "pending candidate evidence must remain invisible without an authorized Service pipe"
    );
    let candidate_state = sender
        .request
        .subflows
        .get(candidate)
        .expect("candidate calibration state");
    let pending_turnover = candidate_state
        .tcp_ack_turnover()
        .expect("candidate ACK turnover")
        .turnover_bytes;
    let ordinary_offset = candidate_state
        .ack_clock_calibration_bytes()
        .expect("candidate calibration spend");
    let ordinary = client_data_frame_for_test(stream_id, ordinary_offset, calibration_target);
    context.record_relay_path_send(
        candidate.key.underlay,
        candidate.key.index,
        calibration_target,
    );
    sender
        .request
        .flights
        .record_owner_frame_instance(candidate, &ordinary);
    tokio::time::sleep(Duration::from_millis(1)).await;
    ack_client_frame_for_test(&mut sender, &context, &ordinary);
    assert!(
        sender
            .request
            .subflows
            .get(candidate)
            .is_some_and(|state| state.window_turnover_proven()),
        "one subsequent causal ordinary window grants exact-instance turnover authority"
    );
    assert_ne!(
        sender
            .request
            .subflows
            .get(candidate)
            .and_then(|state| state.tcp_ack_turnover())
            .expect("updated candidate ACK turnover")
            .turnover_bytes,
        pending_turnover,
        "the ordinary sample must update, not merely unlock, the pending calibration pipe"
    );
    assert!(
        sender.request_tcp_owner_ack_turnover_bytes(&remotes, Some(service), Instant::now(),) > 0
    );
    assert_eq!(sender.request.ack_clock_operation, None);
    assert!(sender.request_owner_ack_can_grow_window(&remotes, Some(service), service));
    assert!(
        sender.request_owner_ack_can_grow_window(&remotes, Some(service), candidate),
        "a live graduated instance gains window-growth rights only after ACK-clock proof"
    );
    let learned_rate = context
        .tcp_path_snapshot(candidate_key.index)
        .expect("candidate snapshot")
        .delivery_rate_bps;
    assert!(
        learned_rate > 100_000_000.0,
        "the first usable ACK-clock sample must replace the receipt-latency prior: {learned_rate}"
    );

    let third = client_data_frame_for_test(
        stream_id,
        ordinary_offset.saturating_add(calibration_target as u64),
        BBR_MAX_SEND_QUANTUM_BYTES,
    );
    let outcome = sender
        .send_stream_data(&context, &mut remotes, third)
        .await
        .expect("ordinary scheduling after calibration");
    match outcome.path_key {
        key if key == candidate_key => assert!(matches!(
            try_recv_reliable_path_command(&mut candidate_rx),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
        )),
        key if key == service_key => assert!(matches!(
            try_recv_reliable_path_command(&mut service_rx),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
        )),
        key => panic!("unexpected post-calibration path: {key:?}"),
    }
    let candidate_state = sender
        .request
        .subflows
        .get(candidate)
        .expect("candidate calibration state");
    assert!(
        candidate_state
            .ack_clock_calibration_bytes()
            .expect("candidate calibration spend")
            >= candidate_state
                .ack_clock_calibration_target()
                .expect("candidate calibration target"),
        "ACK release and ordinary scheduling must not refill calibration credit"
    );
}

#[tokio::test]
async fn client_startup_graduation_advances_to_second_validation_instance() {
    let stream_id = StreamId(102);
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10284?srtt-ms=20&rate-mbps=500",
        "tcp://127.0.0.1:10285?srtt-ms=5&rate-mbps=500",
        "tcp://127.0.0.1:10286?srtt-ms=40&rate-mbps=500",
    ]);
    let _request_bulk_flows = active_request_bulk_flow_registrations(&context);
    let service_key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    let first_key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 1,
    };
    let second_key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 2,
    };
    seed_client_bulk_evidence_for_test(&context, service_key);

    let (service_commands, mut service_rx) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream(stream_id, service_key.index, service_commands),
        8,
    );
    let mut sender = RequestSenderService::new(stream_id);
    let service_frame = client_data_frame_for_test(stream_id, 0, PATH_OPEN_SCORE_BYTES);
    sender
        .send_stream_data(&context, &mut remotes, service_frame.clone())
        .await
        .expect("establish request Service owner");
    assert!(matches!(
        try_recv_reliable_path_command(&mut service_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
    ack_client_frame_for_test(&mut sender, &context, &service_frame);

    let (first_commands, mut first_rx) = reliable_path_command_channels(8);
    remotes.attach_for_validation(opened_test_relay_stream(
        stream_id,
        first_key.index,
        first_commands,
    ));
    let first_instance = remotes
        .path_instance_for_key(first_key)
        .expect("first validation instance");
    consume_client_validation_proof_for_test(&mut first_rx);

    let (second_commands, mut second_rx) = reliable_path_command_channels(8);
    remotes.attach_for_validation(opened_test_relay_stream(
        stream_id,
        second_key.index,
        second_commands,
    ));
    let second_instance = remotes
        .path_instance_for_key(second_key)
        .expect("second validation instance");
    consume_client_validation_proof_for_test(&mut second_rx);
    mark_client_validation_proof_fresh_for_test(
        &context,
        &remotes,
        first_instance,
        Duration::from_millis(5),
    );
    mark_client_validation_proof_fresh_for_test(
        &context,
        &remotes,
        second_instance,
        Duration::from_millis(40),
    );

    let startup_limit = usize::try_from(reliable_subflow_startup_sample_limit_bytes(
        context.mux_limits,
    ))
    .expect("startup limit");
    let mut first_sent = 0_usize;
    while first_sent < startup_limit {
        let payload_bytes = BBR_MAX_SEND_QUANTUM_BYTES.min(startup_limit - first_sent);
        let first_frame = client_data_frame_for_test(
            stream_id,
            PATH_OPEN_SCORE_BYTES as u64 + first_sent as u64,
            payload_bytes,
        );
        let first_outcome = sender
            .send_stream_data(&context, &mut remotes, first_frame.clone())
            .await
            .expect("first validation startup sample");
        assert_eq!(first_outcome.path_key, first_key);
        assert!(matches!(
            try_recv_reliable_path_command(&mut first_rx),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
        ));
        ack_client_frame_for_test(&mut sender, &context, &first_frame);
        first_sent = first_sent.saturating_add(payload_bytes);
    }
    assert!(context.relay_path_has_bulk_model_evidence(first_key.underlay, first_key.index,));

    let second_offset = PATH_OPEN_SCORE_BYTES as u64 + startup_limit as u64;
    let second_frame = client_data_frame_for_test(stream_id, second_offset, 8 * 1024);
    let second_outcome = sender
        .send_stream_data(&context, &mut remotes, second_frame)
        .await
        .expect("second validation startup sample after first graduates");
    assert_eq!(second_outcome.path_key, second_key);
    assert!(matches!(
        try_recv_reliable_path_command(&mut second_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));

    let epoch = sender
        .request
        .startup
        .epoch
        .as_ref()
        .expect("request startup epoch");
    assert_eq!(epoch.startup_owner_key(), Some(second_instance));
    assert!(
        epoch
            .members()
            .iter()
            .any(|member| member.key == first_instance)
    );
    assert!(
        epoch
            .members()
            .iter()
            .any(|member| member.key == second_instance)
    );
    assert_eq!(sender.request.ordered_service_key(), Some(service_key));
    assert_eq!(remotes.active_path_key(), Some(service_key));
}

#[tokio::test]
async fn delayed_old_instance_ack_cannot_graduate_replacement_candidate() {
    let stream_id = StreamId(103);
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10287?srtt-ms=20&rate-mbps=500",
        "tcp://127.0.0.1:10288?srtt-ms=10&rate-mbps=500",
    ]);
    let service_key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    let candidate_key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 1,
    };
    seed_client_bulk_evidence_for_test(&context, candidate_key);
    let (service_commands, _service_rx) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream(stream_id, service_key.index, service_commands),
        8,
    );
    let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(8);
    remotes.attach_for_validation(opened_test_relay_stream(
        stream_id,
        candidate_key.index,
        candidate_commands,
    ));
    consume_client_validation_proof_for_test(&mut candidate_rx);
    let service = remotes
        .path_instance_for_key(service_key)
        .expect("Service instance");
    let replacement = remotes
        .path_instance_for_key(candidate_key)
        .expect("replacement candidate instance");
    let stale = RelayPathInstance {
        key: candidate_key,
        id: replacement.id.wrapping_add(1000),
    };
    let startup_limit = usize::try_from(reliable_subflow_startup_sample_limit_bytes(
        context.mux_limits,
    ))
    .expect("startup limit");
    let mut epoch = FlowSubflowSet::new(0, service, startup_limit, 0, Duration::ZERO);
    assert_eq!(
        epoch
            .admit_subflow_owner(SubflowAdmissionInput {
                key: replacement,
                bulk_rate_proven: false,
                startup_owner_allowed: true,
                frontier_clear: true,
                completion_improves: false,
                observed_goodput_non_degrading: true,
                read_gap: Duration::ZERO,
                owner_bytes: BBR_MAX_SEND_QUANTUM_BYTES,
                optional_overhead_bytes: 0,
            })
            .decision,
        PathAdmissionDecision::AdmitSubflow
    );
    let mut sender = RequestSenderService::new(stream_id);
    sender.request.ordered_service = Some(service);
    sender.request.startup.epoch = Some(epoch);
    sender
        .request
        .startup
        .attempted_subflows
        .insert(replacement);
    let frame = client_data_frame_for_test(stream_id, 0, BBR_MAX_SEND_QUANTUM_BYTES);
    sender
        .request
        .flights
        .record_owner_frame_instance(stale, &frame);
    let owner_progress = sender.release_normalized_acked_ranges_with_owner_progress(
        &context,
        &[OffsetRange::new(0, BBR_MAX_SEND_QUANTUM_BYTES as u64).expect("ACK range")],
    );
    assert_eq!(owner_progress.len(), 1);
    assert_eq!(owner_progress[0].instance, stale);
    assert!(
        sender
            .request
            .subflows
            .get(stale)
            .is_some_and(|state| state.rate_proven())
    );
    assert!(
        !sender.request_owner_ack_can_grow_window(&remotes, Some(service), stale),
        "same-key progress from a detached instance must not grow the replacement epoch"
    );
    sender.reconcile_request_subflow_set(&context, &remotes);

    assert!(
        context.relay_path_has_bulk_model_evidence(candidate_key.underlay, candidate_key.index,)
    );
    assert_eq!(
        sender
            .request
            .startup
            .epoch
            .as_ref()
            .and_then(FlowSubflowSet::startup_owner_key),
        Some(replacement),
        "logical-path evidence from an old attachment must not graduate the replacement"
    );
    assert!(
        !sender
            .request
            .subflows
            .get(replacement)
            .is_some_and(|state| state.graduated())
    );
    assert!(
        !sender
            .request
            .startup
            .acked_bytes
            .contains_key(&replacement)
    );
}

#[tokio::test]
async fn delayed_old_service_ack_cannot_authorize_replacement_service() {
    let stream_id = StreamId(109);
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10293?srtt-ms=20&rate-mbps=500",
        "tcp://127.0.0.1:10294?srtt-ms=10&rate-mbps=500",
    ]);
    let _request_bulk_flows = active_request_bulk_flow_registrations(&context);
    let service_key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    let candidate_key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 1,
    };
    seed_client_bulk_evidence_for_test(&context, service_key);
    let (old_commands, mut old_rx) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream(stream_id, service_key.index, old_commands),
        8,
    );
    let old_service = remotes
        .path_instance_for_key(service_key)
        .expect("old Service instance");
    let mut sender = RequestSenderService::new(stream_id);
    let stale_frame = client_data_frame_for_test(stream_id, 0, BBR_MAX_SEND_QUANTUM_BYTES);
    sender
        .send_stream_data(&context, &mut remotes, stale_frame.clone())
        .await
        .expect("send on old Service");
    assert!(matches!(
        try_recv_reliable_path_command(&mut old_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
    let _removed = remotes
        .remove_path_instance(old_service)
        .expect("remove old Service attachment");

    let (replacement_commands, mut replacement_rx) = reliable_path_command_channels(8);
    remotes.attach(opened_test_relay_stream(
        stream_id,
        service_key.index,
        replacement_commands,
    ));
    let replacement_service = remotes
        .path_instance_for_key(service_key)
        .expect("replacement Service instance");
    assert_ne!(replacement_service, old_service);
    let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(8);
    remotes.attach_for_validation(opened_test_relay_stream(
        stream_id,
        candidate_key.index,
        candidate_commands,
    ));
    let candidate = remotes
        .path_instance_for_key(candidate_key)
        .expect("candidate instance");
    consume_client_validation_proof_for_test(&mut candidate_rx);
    mark_client_validation_proof_fresh_for_test(
        &context,
        &remotes,
        candidate,
        Duration::from_millis(10),
    );

    ack_client_frame_for_test(&mut sender, &context, &stale_frame);
    assert!(
        sender
            .request
            .subflows
            .get(old_service)
            .is_some_and(|state| state.rate_proven())
    );
    assert!(
        !sender
            .request
            .subflows
            .get(replacement_service)
            .is_some_and(|state| state.rate_proven())
    );

    let replacement_frame = client_data_frame_for_test(
        stream_id,
        BBR_MAX_SEND_QUANTUM_BYTES as u64,
        BBR_MAX_SEND_QUANTUM_BYTES,
    );
    sender
        .send_stream_data(&context, &mut remotes, replacement_frame.clone())
        .await
        .expect("replacement must first establish itself as Service");
    assert!(matches!(
        try_recv_reliable_path_command(&mut replacement_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
    assert!(try_recv_reliable_path_command(&mut candidate_rx).is_none());

    ack_client_frame_for_test(&mut sender, &context, &replacement_frame);
    assert!(
        sender
            .request
            .subflows
            .get(replacement_service)
            .is_some_and(|state| state.rate_proven())
    );
    let startup_frame =
        client_data_frame_for_test(stream_id, (2 * BBR_MAX_SEND_QUANTUM_BYTES) as u64, 8 * 1024);
    let outcome = sender
        .send_stream_data(&context, &mut remotes, startup_frame)
        .await
        .expect("current Service evidence may authorize bounded startup");
    assert_eq!(outcome.path_key, candidate_key);
    assert!(matches!(
        try_recv_reliable_path_command(&mut candidate_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
}

#[tokio::test]
async fn udp_product_stream_ack_does_not_create_quic_graduation_evidence() {
    let stream_id = StreamId(104);
    let context = client_test_context_with_paths(&[
        "udp://127.0.0.1:10289?srtt-ms=20&rate-mbps=500",
        "udp://127.0.0.1:10290?srtt-ms=10&rate-mbps=500",
    ]);
    let service_key = RelayPathKey {
        underlay: UnderlayProtocol::Udp,
        index: 0,
    };
    let candidate_key = RelayPathKey {
        underlay: UnderlayProtocol::Udp,
        index: 1,
    };
    let (service_commands, _service_rx) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream_with_underlay(
            stream_id,
            UnderlayProtocol::Udp,
            service_key.index,
            service_commands,
        ),
        8,
    );
    let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(8);
    remotes.attach_for_validation(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        candidate_key.index,
        candidate_commands,
    ));
    consume_client_validation_proof_for_test(&mut candidate_rx);
    let service = remotes
        .path_instance_for_key(service_key)
        .expect("Service instance");
    let candidate = remotes
        .path_instance_for_key(candidate_key)
        .expect("candidate instance");
    let startup_limit = usize::try_from(reliable_subflow_startup_sample_limit_bytes(
        context.mux_limits,
    ))
    .expect("startup limit");
    let mut epoch = FlowSubflowSet::new(0, service, startup_limit, 0, Duration::ZERO);
    assert_eq!(
        epoch
            .admit_subflow_owner(SubflowAdmissionInput {
                key: candidate,
                bulk_rate_proven: false,
                startup_owner_allowed: true,
                frontier_clear: true,
                completion_improves: false,
                observed_goodput_non_degrading: true,
                read_gap: Duration::ZERO,
                owner_bytes: startup_limit,
                optional_overhead_bytes: 0,
            })
            .decision,
        PathAdmissionDecision::AdmitSubflow
    );
    let mut sender = RequestSenderService::new(stream_id);
    sender.request.ordered_service = Some(service);
    sender.request.startup.epoch = Some(epoch);
    sender.request.startup.attempted_subflows.insert(candidate);
    let frame = client_data_frame_for_test(stream_id, 0, startup_limit);
    sender
        .request
        .flights
        .record_owner_frame_instance(candidate, &frame);
    sender.release_normalized_acked_ranges(
        &context,
        &[OffsetRange::new(0, startup_limit as u64).expect("ACK range")],
    );
    sender.reconcile_request_subflow_set(&context, &remotes);

    assert!(
        !context.relay_path_has_bulk_model_evidence(candidate_key.underlay, candidate_key.index,)
    );
    assert!(!sender.request.startup.acked_bytes.contains_key(&candidate));
    assert!(
        !sender
            .request
            .subflows
            .get(candidate)
            .is_some_and(|state| state.graduated())
    );
    assert_eq!(
        sender
            .request
            .startup
            .epoch
            .as_ref()
            .and_then(FlowSubflowSet::startup_owner_key),
        None,
        "a defensive UDP product-startup epoch is discarded instead of becoming QUIC carrier evidence"
    );
}

#[tokio::test]
async fn request_quic_proof_at_train_deadline_keeps_exact_handoff_owner() {
    let stream_id = StreamId(201);
    let context = client_test_context_with_paths(&[
        "udp://127.0.0.1:10321?srtt-ms=20&rate-mbps=500",
        "udp://127.0.0.1:10322?srtt-ms=10&rate-mbps=500",
    ]);
    let (service_commands, _service_rx) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream_with_underlay(
            stream_id,
            UnderlayProtocol::Udp,
            0,
            service_commands,
        ),
        8,
    );
    let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(8);
    remotes.attach_for_validation(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        1,
        candidate_commands,
    ));
    consume_client_validation_proof_for_test(&mut candidate_rx);
    let candidate_path = remotes
        .paths
        .iter()
        .find(|path| path.placement == RelayPathPlacement::Validation)
        .expect("Validation candidate");
    let candidate_instance = candidate_path.instance();
    let attached_at = candidate_path.attached_at;
    let service_path = remotes
        .paths
        .iter()
        .find(|path| path.placement == RelayPathPlacement::Active)
        .expect("Active Service");
    let service_instance = service_path.instance();
    let service_attached_at = service_path.attached_at;
    let train_deadline = Instant::now() + Duration::from_millis(40);
    let mut sender = RequestSenderService::new(stream_id);
    let (proof, probe) = reserve_request_quic_capacity_calibration_for_test(
        &mut sender,
        &context,
        candidate_instance,
        attached_at,
        train_deadline,
        train_deadline - Duration::from_nanos(1),
        Duration::from_secs(2),
    );

    tokio::time::sleep(Duration::from_millis(60)).await;
    sender.reconcile_request_subflow_set(&context, &remotes);
    assert!(!context.request_quic_capacity_probe_proven_at(1, proof.token, Instant::now()));
    assert!(
        sender
            .quic_capacity
            .active
            .as_ref()
            .is_some_and(|calibration| {
                !calibration.graduated && calibration.ticket.is_current()
            })
    );
    publish_request_quic_capacity_calibration_for_test(
        &sender,
        &context,
        candidate_instance,
        proof,
        probe,
    );
    assert!(context.request_quic_capacity_probe_proven_at(1, proof.token, Instant::now()));
    sender.reconcile_request_subflow_set(&context, &remotes);
    assert!(
        sender
            .request
            .subflows
            .get(candidate_instance)
            .is_some_and(|state| state.graduated())
    );
    assert!(
        sender
            .quic_capacity
            .active
            .as_ref()
            .is_some_and(|calibration| calibration.graduated)
    );
    assert_eq!(
        context.request_quic_capacity_product_handoff_state_at(1, proof.token, Instant::now()),
        RequestQuicCapacityProductHandoffState::Pending
    );

    let ack_range = OffsetRange::new(0, proof.required_proof_bytes).expect("ACK range");
    let foreign_stream_id = StreamId(202);
    let foreign_frame =
        client_data_frame_for_test(foreign_stream_id, 0, proof.required_proof_bytes as usize);
    let mut foreign_sender = RequestSenderService::new(foreign_stream_id);
    foreign_sender
        .request
        .flights
        .record_owner_frame_instance(candidate_instance, &foreign_frame);
    foreign_sender.release_normalized_acked_ranges(&context, &[ack_range]);
    assert_eq!(
        context.request_quic_capacity_product_handoff_state_at(1, proof.token, Instant::now()),
        RequestQuicCapacityProductHandoffState::Pending,
        "a colliding stream-local path instance cannot satisfy the owner handoff"
    );
    assert!(
        context
            .try_reserve_request_quic_capacity_probe(
                StreamId(204),
                service_instance.key.index,
                service_instance,
                9_000,
                PATH_OPEN_SCORE_BYTES as u64,
                reliable_capacity_calibration_session_limit_bytes(context.mux_limits),
                Arc::new(RequestCapacityProbeCampaignBudget::default()),
                service_attached_at,
                Instant::now() + Duration::from_secs(1),
                Duration::from_secs(1),
                QuicCapacityProbeCommandTicket::new(),
            )
            .is_none(),
        "a pending product handoff still serializes the next carrier train"
    );

    let owner_frame = client_data_frame_for_test(stream_id, 0, proof.required_proof_bytes as usize);
    sender
        .request
        .flights
        .record_owner_frame_instance(candidate_instance, &owner_frame);
    sender.release_normalized_acked_ranges(&context, &[ack_range]);
    let next_ticket = QuicCapacityProbeCommandTicket::new();
    let next_lease = context
        .try_reserve_request_quic_capacity_probe(
            StreamId(204),
            service_instance.key.index,
            service_instance,
            9_001,
            PATH_OPEN_SCORE_BYTES as u64,
            reliable_capacity_calibration_session_limit_bytes(context.mux_limits),
            Arc::new(RequestCapacityProbeCampaignBudget::default()),
            service_attached_at,
            Instant::now() + Duration::from_secs(1),
            Duration::from_secs(1),
            next_ticket,
        )
        .expect("completion releases session ownership without another owner send");
    assert_eq!(
        context.request_quic_capacity_product_handoff_state_at(1, proof.token, Instant::now()),
        RequestQuicCapacityProductHandoffState::Complete
    );
    sender.reconcile_request_subflow_set(&context, &remotes);
    assert!(sender.quic_capacity.active.is_none());
    assert_eq!(
        context.request_quic_capacity_product_handoff_state_at(1, proof.token, Instant::now()),
        RequestQuicCapacityProductHandoffState::Complete
    );
    assert!(
        sender
            .request
            .subflows
            .get(candidate_instance)
            .is_some_and(|state| state.graduated())
    );
    assert!(
        context
            .try_reserve_request_quic_capacity_probe(
                StreamId(205),
                service_instance.key.index,
                service_instance,
                9_002,
                PATH_OPEN_SCORE_BYTES as u64,
                reliable_capacity_calibration_session_limit_bytes(context.mux_limits),
                Arc::new(RequestCapacityProbeCampaignBudget::default()),
                service_attached_at,
                Instant::now() + Duration::from_secs(1),
                Duration::from_secs(1),
                QuicCapacityProbeCommandTicket::new(),
            )
            .is_none(),
        "dropping the old owner lease cannot clear a newer token"
    );
    drop(next_lease);
    assert!(
        context
            .try_reserve_request_quic_capacity_probe(
                StreamId(205),
                service_instance.key.index,
                service_instance,
                9_002,
                PATH_OPEN_SCORE_BYTES as u64,
                reliable_capacity_calibration_session_limit_bytes(context.mux_limits),
                Arc::new(RequestCapacityProbeCampaignBudget::default()),
                service_attached_at,
                Instant::now() + Duration::from_secs(1),
                Duration::from_secs(1),
                QuicCapacityProbeCommandTicket::new(),
            )
            .is_some()
    );
}

#[test]
fn dropping_request_quic_owner_revokes_pending_handoff() {
    let stream_id = StreamId(206);
    let context =
        client_test_context_with_paths(&["udp://127.0.0.1:10325?srtt-ms=20&rate-mbps=500"]);
    let target = RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        },
        id: 1,
    };
    let mut sender = RequestSenderService::new(stream_id);
    let proof = install_request_quic_capacity_calibration_for_test(
        &mut sender,
        &context,
        target,
        Instant::now() - Duration::from_millis(1),
        Instant::now() + Duration::from_secs(1),
        Duration::from_secs(1),
    );
    assert_eq!(
        context.request_quic_capacity_product_handoff_state_at(0, proof.token, Instant::now()),
        RequestQuicCapacityProductHandoffState::Pending
    );

    sender.quic_capacity.active = None;
    assert_eq!(
        context.request_quic_capacity_product_handoff_state_at(0, proof.token, Instant::now()),
        RequestQuicCapacityProductHandoffState::Absent
    );
    assert!(
        context
            .try_reserve_request_quic_capacity_probe(
                StreamId(207),
                0,
                target,
                9_003,
                PATH_OPEN_SCORE_BYTES as u64,
                reliable_capacity_calibration_session_limit_bytes(context.mux_limits),
                Arc::new(RequestCapacityProbeCampaignBudget::default()),
                Instant::now() - Duration::from_millis(1),
                Instant::now() + Duration::from_secs(1),
                Duration::from_secs(1),
                QuicCapacityProbeCommandTicket::new(),
            )
            .is_some()
    );
}

#[test]
fn request_quic_product_ack_without_transaction_skips_health_lock() {
    let context =
        client_test_context_with_paths(&["udp://127.0.0.1:10329?srtt-ms=20&rate-mbps=500"]);
    poison_client_path_health_for_test(&context);
    let now = Instant::now();

    context.record_relay_path_product_ack(
        StreamId(209),
        RelayPathInstance {
            key: RelayPathKey {
                underlay: UnderlayProtocol::Udp,
                index: 0,
            },
            id: 1,
        },
        PATH_OPEN_SCORE_BYTES,
        now,
        now + Duration::from_millis(1),
    );
}

#[tokio::test]
async fn incomplete_request_quic_handoff_revokes_ephemeral_graduation() {
    let stream_id = StreamId(203);
    let context = client_test_context_with_paths(&[
        "udp://127.0.0.1:10323?srtt-ms=20&rate-mbps=500",
        "udp://127.0.0.1:10324?srtt-ms=10&rate-mbps=500",
    ]);
    let (service_commands, _service_rx) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream_with_underlay(
            stream_id,
            UnderlayProtocol::Udp,
            0,
            service_commands,
        ),
        8,
    );
    let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(8);
    remotes.attach_for_validation(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        1,
        candidate_commands,
    ));
    consume_client_validation_proof_for_test(&mut candidate_rx);
    let candidate_path = remotes
        .paths
        .iter()
        .find(|path| path.placement == RelayPathPlacement::Validation)
        .expect("Validation candidate");
    let candidate_instance = candidate_path.instance();
    let attached_at = candidate_path.attached_at;
    let service_path = remotes
        .paths
        .iter()
        .find(|path| path.placement == RelayPathPlacement::Active)
        .expect("Active Service");
    let service_instance = service_path.instance();
    let service_attached_at = service_path.attached_at;
    let train_deadline = Instant::now() + Duration::from_millis(40);
    let mut sender = RequestSenderService::new(stream_id);
    let proof = install_request_quic_capacity_calibration_for_test(
        &mut sender,
        &context,
        candidate_instance,
        attached_at,
        train_deadline,
        Duration::from_secs(2),
    );

    tokio::time::sleep(Duration::from_millis(60)).await;
    sender.reconcile_request_subflow_set(&context, &remotes);
    assert!(
        sender
            .request
            .subflows
            .get(candidate_instance)
            .is_some_and(|state| state.graduated())
    );
    context.health().lock().expect("path health lock").udp[1].maintain(proof.expires_at);
    let next_lease = context
        .try_reserve_request_quic_capacity_probe(
            StreamId(208),
            service_instance.key.index,
            service_instance,
            9_004,
            PATH_OPEN_SCORE_BYTES as u64,
            reliable_capacity_calibration_session_limit_bytes(context.mux_limits),
            Arc::new(RequestCapacityProbeCampaignBudget::default()),
            service_attached_at,
            Instant::now() + Duration::from_secs(1),
            Duration::from_secs(1),
            QuicCapacityProbeCommandTicket::new(),
        )
        .expect("an expired idle handoff is reclaimed by the next reservation");
    sender.reconcile_request_subflow_set(&context, &remotes);

    assert!(sender.quic_capacity.active.is_none());
    assert!(
        !sender
            .request
            .subflows
            .get(candidate_instance)
            .is_some_and(|state| state.graduated())
    );
    assert_eq!(
        context.request_quic_capacity_product_handoff_state_at(1, proof.token, Instant::now()),
        RequestQuicCapacityProductHandoffState::Absent
    );
    assert!(
        context
            .try_reserve_request_quic_capacity_probe(
                StreamId(209),
                service_instance.key.index,
                service_instance,
                9_005,
                PATH_OPEN_SCORE_BYTES as u64,
                reliable_capacity_calibration_session_limit_bytes(context.mux_limits),
                Arc::new(RequestCapacityProbeCampaignBudget::default()),
                service_attached_at,
                Instant::now() + Duration::from_secs(1),
                Duration::from_secs(1),
                QuicCapacityProbeCommandTicket::new(),
            )
            .is_none()
    );
    drop(next_lease);
    assert!(
        context
            .try_reserve_request_quic_capacity_probe(
                StreamId(209),
                service_instance.key.index,
                service_instance,
                9_005,
                PATH_OPEN_SCORE_BYTES as u64,
                reliable_capacity_calibration_session_limit_bytes(context.mux_limits),
                Arc::new(RequestCapacityProbeCampaignBudget::default()),
                service_attached_at,
                Instant::now() + Duration::from_secs(1),
                Duration::from_secs(1),
                QuicCapacityProbeCommandTicket::new(),
            )
            .is_some()
    );
}

#[tokio::test]
async fn ordered_receipt_proof_cannot_resurrect_udp_product_startup() {
    let stream_id = StreamId(110);
    let context = client_test_context_with_paths(&[
        "udp://127.0.0.1:10295?srtt-ms=20&rate-mbps=500",
        "udp://127.0.0.1:10296?srtt-ms=10&rate-mbps=500",
    ]);
    let service_key = RelayPathKey {
        underlay: UnderlayProtocol::Udp,
        index: 0,
    };
    let candidate_key = RelayPathKey {
        underlay: UnderlayProtocol::Udp,
        index: 1,
    };
    let (service_commands, _service_rx) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream_with_underlay(
            stream_id,
            UnderlayProtocol::Udp,
            service_key.index,
            service_commands,
        ),
        8,
    );
    let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(8);
    remotes.attach_for_validation(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        candidate_key.index,
        candidate_commands,
    ));
    consume_client_validation_proof_for_test(&mut candidate_rx);
    let service = remotes
        .path_instance_for_key(service_key)
        .expect("Service instance");
    let candidate = remotes
        .path_instance_for_key(candidate_key)
        .expect("candidate instance");
    let startup_limit = usize::try_from(reliable_subflow_startup_sample_limit_bytes(
        context.mux_limits,
    ))
    .expect("startup limit");
    let mut epoch = FlowSubflowSet::new(0, service, startup_limit, 0, Duration::ZERO);
    assert_eq!(
        epoch
            .admit_subflow_owner(SubflowAdmissionInput {
                key: candidate,
                bulk_rate_proven: false,
                startup_owner_allowed: true,
                frontier_clear: true,
                completion_improves: false,
                observed_goodput_non_degrading: true,
                read_gap: Duration::ZERO,
                owner_bytes: startup_limit,
                optional_overhead_bytes: 0,
            })
            .decision,
        PathAdmissionDecision::AdmitSubflow
    );
    let mut sender = RequestSenderService::new(stream_id);
    sender.request.ordered_service = Some(service);
    sender.request.startup.epoch = Some(epoch);
    sender.request.startup.attempted_subflows.insert(candidate);
    let receipt_proof_id = 991;
    sender
        .request
        .startup
        .receipt_proofs
        .insert(candidate, (receipt_proof_id, 0));
    sender
        .request
        .startup
        .first_sent_at
        .insert(candidate, Instant::now());

    let frame = client_data_frame_for_test(stream_id, 0, startup_limit);
    sender
        .request
        .flights
        .record_owner_frame_instance(candidate, &frame);
    sender
        .request
        .flights
        .record_repair_frame_instance(service, &frame);
    sender.release_normalized_acked_ranges(
        &context,
        &[OffsetRange::new(0, startup_limit as u64).expect("ACK range")],
    );
    assert!(!sender.request.startup.acked_bytes.contains_key(&candidate));

    tokio::time::sleep(Duration::from_millis(10)).await;
    context.mark_relay_path_proof_observation(
        candidate_key.underlay,
        candidate_key.index,
        PathProofObservation {
            proof_id: receipt_proof_id,
            bytes: PATH_OPEN_SCORE_BYTES as u64,
            elapsed: Duration::from_millis(10),
            sent_at: Instant::now(),
        },
    );
    sender.reconcile_request_subflow_set(&context, &remotes);

    assert!(
        !sender
            .request
            .subflows
            .get(candidate)
            .is_some_and(|state| state.graduated())
    );
    assert_eq!(
        sender
            .request
            .startup
            .epoch
            .as_ref()
            .and_then(FlowSubflowSet::startup_owner_key),
        None
    );
    assert!(
        !context.relay_path_has_bulk_model_evidence(candidate_key.underlay, candidate_key.index,),
        "an ordered product receipt is not native QUIC packet-ACK capacity evidence"
    );
}

#[tokio::test]
async fn udp_service_evidence_does_not_bootstrap_validation_with_product_bytes() {
    let stream_id = StreamId(114);
    let context = client_test_context_with_paths(&[
        "udp://127.0.0.1:10303?srtt-ms=20&rate-mbps=500",
        "udp://127.0.0.1:10304?srtt-ms=10&rate-mbps=500",
    ]);
    let service_key = RelayPathKey {
        underlay: UnderlayProtocol::Udp,
        index: 0,
    };
    let candidate_key = RelayPathKey {
        underlay: UnderlayProtocol::Udp,
        index: 1,
    };
    context.health().lock().expect("path health lock").udp[service_key.index]
        .mark_quic_path_metrics(UdpPathMetrics {
            direction: 1,
            srtt: Duration::from_millis(20),
            rttvar: Duration::from_millis(2),
            min_rtt: Duration::from_millis(18),
            min_rtt_observed: true,
            delivery_rate_bps: 500_000_000.0,
            pacing_rate_bps: 500_000_000.0,
            inflight_hi: 4 * 1024 * 1024,
            bytes_in_flight: 0,
            pending_bytes: 0,
            loss_ppm: Some(0),
            ecn_ppm: Some(0),
            app_limited: false,
            ack_derived_data_seen: true,
            delivery_sample_count: 1,
            delivery_sample_bytes: 4 * 1024 * 1024,
            last_delivery_sample_at: Some(Instant::now()),
            bulk_proof_expires_at: None,
            latest_delivery_sample_bytes: 4 * 1024 * 1024,
            latest_delivery_sample_count: 1,
            latest_carrier_ack_elapsed: Some(Duration::from_millis(20)),
            latest_rate_sample_elapsed: Some(Duration::from_millis(20)),
            capacity_proof_candidate: None,
            capacity_probe: None,
            #[cfg(feature = "lab-diagnostics")]
            ack_poll: QuicAckPollDiagnostics::default(),
        });
    let (service_commands, mut service_rx) = reliable_path_command_channels(16);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream_with_underlay(
            stream_id,
            UnderlayProtocol::Udp,
            service_key.index,
            service_commands,
        ),
        16,
    );
    let service = remotes
        .path_instance_for_key(service_key)
        .expect("Service instance");
    let mut sender = RequestSenderService::new(stream_id);
    let service_frame = client_data_frame_for_test(stream_id, 0, BBR_MAX_SEND_QUANTUM_BYTES);
    sender
        .send_stream_data(&context, &mut remotes, service_frame.clone())
        .await
        .expect("send UDP Service evidence");
    assert!(matches!(
        try_recv_reliable_path_command(&mut service_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
    ack_client_frame_for_test(&mut sender, &context, &service_frame);
    assert!(
        sender
            .request
            .subflows
            .get(service)
            .is_some_and(|state| state.rate_proven())
    );
    assert!(context.relay_path_has_bulk_model_evidence(service_key.underlay, service_key.index,));

    let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(16);
    remotes.attach_for_validation(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        candidate_key.index,
        candidate_commands,
    ));
    let candidate = remotes
        .path_instance_for_key(candidate_key)
        .expect("Validation candidate");
    consume_client_validation_proof_for_test(&mut candidate_rx);
    mark_client_validation_proof_fresh_for_test(
        &context,
        &remotes,
        candidate,
        Duration::from_millis(10),
    );
    let frame = client_data_frame_for_test(
        stream_id,
        BBR_MAX_SEND_QUANTUM_BYTES as u64,
        BBR_MAX_SEND_QUANTUM_BYTES,
    );
    let outcome = sender
        .send_stream_data(&context, &mut remotes, frame)
        .await
        .expect("UDP Service remains the only product owner");
    assert_eq!(outcome.path_key, service_key);
    assert!(matches!(
        try_recv_reliable_path_command(&mut service_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
    assert!(try_recv_reliable_path_command(&mut candidate_rx).is_none());
    assert!(
        !context.relay_path_has_bulk_model_evidence(candidate_key.underlay, candidate_key.index,)
    );
    assert!(
        !sender
            .request
            .startup
            .attempted_subflows
            .contains(&candidate)
    );
    assert!(
        !sender
            .request
            .subflows
            .get(candidate)
            .is_some_and(|state| state.graduated())
    );
    assert_eq!(
        sender
            .request
            .startup
            .epoch
            .as_ref()
            .and_then(FlowSubflowSet::startup_owner_key),
        None,
        "reachability plus Service capacity cannot turn an app-limited QUIC product burst into candidate carrier evidence"
    );
}

#[tokio::test]
async fn udp_validation_uses_fresh_native_evidence_after_service_is_established() {
    let stream_id = StreamId(118);
    let context = client_test_context_with_paths(&[
        "udp://127.0.0.1:10305?srtt-ms=20&rate-mbps=500",
        "udp://127.0.0.1:10306?srtt-ms=10&rate-mbps=500",
    ]);
    let service_key = RelayPathKey {
        underlay: UnderlayProtocol::Udp,
        index: 0,
    };
    let candidate_key = RelayPathKey {
        underlay: UnderlayProtocol::Udp,
        index: 1,
    };
    let (service_commands, mut service_rx) = reliable_path_command_channels(16);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream_with_underlay(
            stream_id,
            UnderlayProtocol::Udp,
            service_key.index,
            service_commands,
        ),
        16,
    );
    let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(16);
    remotes.attach_for_validation(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        candidate_key.index,
        candidate_commands,
    ));
    let candidate = remotes
        .path_instance_for_key(candidate_key)
        .expect("Validation candidate");
    consume_client_validation_proof_for_test(&mut candidate_rx);
    mark_client_validation_proof_fresh_for_test(
        &context,
        &remotes,
        candidate,
        Duration::from_millis(10),
    );
    seed_client_quic_native_bulk_evidence_for_test(&context, candidate_key.index);

    let mut sender = RequestSenderService::new(stream_id);
    let service_frame = client_data_frame_for_test(stream_id, 0, BBR_MAX_SEND_QUANTUM_BYTES);
    let first = sender
        .send_stream_data(&context, &mut remotes, service_frame.clone())
        .await
        .expect("offset zero establishes the stable Service owner");
    assert_eq!(first.path_key, service_key);
    assert!(matches!(
        try_recv_reliable_path_command(&mut service_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
    assert!(
        !sender
            .request
            .subflows
            .get(candidate)
            .is_some_and(|state| state.graduated()),
        "path-wide carrier evidence must not steal offset zero before a Service instance exists"
    );
    ack_client_frame_for_test(&mut sender, &context, &service_frame);

    let candidate_frame = client_data_frame_for_test(
        stream_id,
        BBR_MAX_SEND_QUANTUM_BYTES as u64,
        BBR_MAX_SEND_QUANTUM_BYTES,
    );
    let second = sender
        .send_stream_data(&context, &mut remotes, candidate_frame)
        .await
        .expect("fresh native QUIC evidence should admit the live Validation instance");
    assert_eq!(second.path_key, candidate_key);
    assert!(
        sender
            .request
            .subflows
            .get(candidate)
            .is_some_and(|state| state.graduated())
    );
    assert!(matches!(
        try_recv_reliable_path_command(&mut candidate_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
    assert!(sender.request.startup.epoch.is_none());
    assert!(sender.request.startup.receipt_proofs.is_empty());
}

#[tokio::test]
async fn startup_candidate_can_progress_when_service_command_queue_is_full() {
    let stream_id = StreamId(105);
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10291?srtt-ms=20&rate-mbps=500",
        "tcp://127.0.0.1:10292?srtt-ms=10&rate-mbps=500",
    ]);
    let _request_bulk_flows = active_request_bulk_flow_registrations(&context);
    let service_key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    let candidate_key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 1,
    };
    seed_client_bulk_evidence_for_test(&context, service_key);
    let (service_commands, _service_rx) = reliable_path_command_channels(1);
    service_commands
        .try_enqueue_admitted_frame(
            client_data_frame_for_test(stream_id, 0, BBR_MAX_SEND_QUANTUM_BYTES),
            FlowLane::Throughput,
        )
        .expect("fill Service data queue");
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream(stream_id, service_key.index, service_commands),
        8,
    );
    let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(8);
    remotes.attach_for_validation(opened_test_relay_stream(
        stream_id,
        candidate_key.index,
        candidate_commands,
    ));
    consume_client_validation_proof_for_test(&mut candidate_rx);
    let candidate = remotes
        .path_instance_for_key(candidate_key)
        .expect("candidate instance");
    mark_client_validation_proof_fresh_for_test(
        &context,
        &remotes,
        candidate,
        Duration::from_millis(10),
    );
    let mut sender = RequestSenderService::new(stream_id);
    let service = remotes
        .path_instance_for_key(service_key)
        .expect("Service instance");
    sender.request.ordered_service = Some(service);
    sender.request.subflows.get_mut(service).mark_rate_proven();

    let outcome = sender
        .send_stream_data(
            &context,
            &mut remotes,
            client_data_frame_for_test(stream_id, 0, 8 * 1024),
        )
        .await
        .expect("fresh candidate should provide bounded overflow credit");

    assert_eq!(outcome.path_key, candidate_key);
    assert!(matches!(
        try_recv_reliable_path_command(&mut candidate_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
    assert_eq!(sender.request.ordered_service_key(), Some(service_key));
    assert!(
        remotes
            .paths
            .iter()
            .find(|path| path.instance() == candidate)
            .expect("candidate path")
            .has_load_reservation(),
        "first optional OwnerData commits this logical flow's path load"
    );
    assert_eq!(
        context.health().lock().expect("path health lock").tcp[1].active_flows,
        1,
        "concurrent flows must see that this Subflow already consumes carrier capacity"
    );

    drop(remotes);
    assert_eq!(
        context.health().lock().expect("path health lock").tcp[1].active_flows,
        0,
        "dropping the remote set must release a committed startup load lease"
    );
}

#[test]
fn stale_shared_load_snapshot_has_only_one_claim_winner() {
    let context =
        client_test_context_with_paths(&["tcp://127.0.0.1:10307?srtt-ms=180&rate-mbps=500"]);
    let key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };

    let first = context
        .try_reserve_relay_path_load_if_unchanged(key, FlowLane::Throughput, 0, 0)
        .expect("first exact snapshot claim");
    assert!(
        context
            .try_reserve_relay_path_load_if_unchanged(key, FlowLane::Throughput, 0, 0,)
            .is_none(),
        "a stale contender must rescore instead of sharing the same idle candidate"
    );
    assert_eq!(
        context.health().lock().expect("path health lock").tcp[0].active_flows,
        1
    );

    drop(first);
    assert_eq!(
        context.health().lock().expect("path health lock").tcp[0].active_flows,
        0
    );
}

#[tokio::test]
async fn failed_path_proof_enqueue_retries_without_sticking_validation() {
    let stream_id = StreamId(106);
    let context = client_test_context();
    let (commands, mut receivers) = reliable_path_command_channels(1);
    commands
        .try_enqueue_admitted_frame(
            Frame::StreamAck {
                stream_id,
                complete: false,
                ranges: Vec::new(),
            },
            FlowLane::Control,
        )
        .expect("fill priority queue");
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, commands), 4);
    remotes.paths[0].placement = RelayPathPlacement::Validation;
    remotes.paths[0].path_proof_id = None;
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
    ));

    remotes.retry_pending_path_proofs(&context);

    assert!(remotes.paths[0].path_proof_id.is_some());
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
    ));
}

#[tokio::test]
async fn queued_path_proof_keeps_one_identity_until_ack_or_path_failure() {
    let stream_id = StreamId(108);
    let context = client_test_context();
    let (commands, mut receivers) = reliable_path_command_channels(2);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, commands), 4);
    remotes.paths[0].placement = RelayPathPlacement::Validation;
    remotes.paths[0].path_proof_id = Some(41);

    remotes.retry_pending_path_proofs(&context);

    assert_eq!(remotes.paths[0].path_proof_id, Some(41));
    assert!(try_recv_reliable_path_priority_command(&mut receivers).is_none());

    context.health().lock().expect("path health lock").tcp[0].invalidate_path_proofs();
    remotes.retry_pending_path_proofs(&context);
    assert_ne!(remotes.paths[0].path_proof_id, Some(41));
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
    ));
}

#[tokio::test]
async fn invalidated_startup_receipt_proof_requeues_in_new_generation() {
    let stream_id = StreamId(113);
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10301?srtt-ms=20&rate-mbps=500",
        "tcp://127.0.0.1:10302?srtt-ms=10&rate-mbps=500",
    ]);
    let (service_commands, _service_rx) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, service_commands), 8);
    let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(8);
    remotes.attach_for_validation(opened_test_relay_stream(stream_id, 1, candidate_commands));
    consume_client_validation_proof_for_test(&mut candidate_rx);
    let service = remotes
        .paths
        .iter()
        .find(|path| path.placement == RelayPathPlacement::Active)
        .expect("Active Service")
        .instance();
    let candidate_path = remotes
        .paths
        .iter()
        .find(|path| path.placement == RelayPathPlacement::Validation)
        .expect("Validation candidate");
    let candidate = candidate_path.instance();
    let attached_at = candidate_path.attached_at;
    let mut epoch = FlowSubflowSet::new(0, service, 64 * 1024, 0, Duration::ZERO);
    assert_eq!(
        epoch
            .admit_subflow_owner(SubflowAdmissionInput {
                key: candidate,
                bulk_rate_proven: false,
                startup_owner_allowed: true,
                frontier_clear: true,
                completion_improves: false,
                observed_goodput_non_degrading: true,
                read_gap: Duration::ZERO,
                owner_bytes: 64 * 1024,
                optional_overhead_bytes: 0,
            })
            .decision,
        PathAdmissionDecision::AdmitSubflow
    );
    let mut sender = RequestSenderService::new(stream_id);
    sender.request.startup.epoch = Some(epoch);
    sender.try_enqueue_request_startup_receipt_proof(&context, &remotes, candidate);
    let (old_proof_id, old_generation) = sender.request.startup.receipt_proofs[&candidate];
    assert_eq!(old_generation, 0);
    assert!(matches!(
        try_recv_reliable_path_command(&mut candidate_rx),
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData {
            proof_id,
            ..
        })) if proof_id == old_proof_id
    ));
    let stale_sent_at = Instant::now();

    context.health().lock().expect("path health lock").tcp[1].invalidate_path_proofs();
    context.mark_relay_path_proof_observation(
        candidate.key.underlay,
        candidate.key.index,
        PathProofObservation {
            proof_id: old_proof_id,
            bytes: PATH_OPEN_SCORE_BYTES as u64,
            elapsed: Duration::from_millis(10),
            sent_at: stale_sent_at,
        },
    );
    assert!(!context.relay_path_has_fresh_proof(
        candidate.key.underlay,
        candidate.key.index,
        old_proof_id,
        attached_at,
    ));

    sender.try_enqueue_request_startup_receipt_proof(&context, &remotes, candidate);
    let (new_proof_id, new_generation) = sender.request.startup.receipt_proofs[&candidate];
    assert_eq!(new_generation, 1);
    assert_ne!(new_proof_id, old_proof_id);
    assert!(matches!(
        try_recv_reliable_path_command(&mut candidate_rx),
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData {
            proof_id,
            ..
        })) if proof_id == new_proof_id
    ));
}

#[test]
fn service_epoch_reset_retains_attempted_and_graduated_instance_tombstones() {
    let key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 1,
    };
    let attempted = RelayPathInstance { key, id: 7 };
    let graduated = RelayPathInstance { key, id: 8 };
    let service = RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        },
        id: 1,
    };
    let mut sender = RequestSenderService::new(StreamId(107));
    sender.request.startup.attempted_subflows.insert(attempted);
    sender.request.startup.attempted_subflows.insert(graduated);
    sender.request.subflows.get_mut(graduated).mark_graduated();
    sender.request.startup.epoch = Some(FlowSubflowSet::new(
        0,
        service,
        256 * 1024,
        0,
        Duration::ZERO,
    ));
    sender.request.ack_clock_operation = Some(RequestAckClockOperation::Pending {
        service,
        candidate: graduated,
    });

    sender.reset_request_subflow_epoch();

    assert!(sender.request.startup.epoch.is_none());
    assert!(
        sender
            .request
            .startup
            .attempted_subflows
            .contains(&attempted)
    );
    assert!(
        sender
            .request
            .startup
            .attempted_subflows
            .contains(&graduated)
    );
    assert!(
        sender
            .request
            .subflows
            .get(graduated)
            .is_some_and(|state| state.graduated())
    );
    assert!(sender.request.ack_clock_operation.is_none());
}

#[test]
fn request_calibration_commit_installs_pending_owner_and_spend_atomically() {
    let service = RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        },
        id: 3,
    };
    let candidate = RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 1,
        },
        id: 7,
    };
    let mut sender = RequestSenderService::new(StreamId(109));
    sender.request.ordered_service = Some(service);
    sender.commit_request_ack_clock_calibration(Some(
        RequestAckClockCalibrationCommit::ServiceFence {
            service,
            candidate,
            entry_offset: 64 * 1024,
            foreign_optional_ranges: 1,
            foreign_optional_bytes: 64 * 1024,
        },
    ));
    assert_eq!(
        sender.request.ack_clock_operation,
        Some(RequestAckClockOperation::Pending { service, candidate })
    );
    assert!(sender.request.subflows.iter().all(|(_, state)| {
        state.ack_clock_calibration_bytes().is_none()
            && state.ack_clock_calibration_target().is_none()
    }));

    sender.commit_request_ack_clock_calibration(Some(
        RequestAckClockCalibrationCommit::OwnerData {
            candidate,
            target_bytes: 2 * 1024 * 1024,
            payload_bytes: 64 * 1024,
            entry_offset: 64 * 1024,
            foreign_optional_ranges: 0,
            foreign_optional_bytes: 0,
        },
    ));
    assert!(matches!(
        sender.request.ack_clock_operation,
        Some(RequestAckClockOperation::Owner { .. })
    ));
    let candidate_state = sender
        .request
        .subflows
        .get(candidate)
        .expect("candidate calibration state");
    assert_eq!(
        candidate_state
            .ack_clock_calibration_bytes()
            .expect("candidate calibration spend"),
        64 * 1024
    );
    assert_eq!(
        candidate_state
            .ack_clock_calibration_target()
            .expect("candidate calibration target"),
        2 * 1024 * 1024
    );
    assert_eq!(
        sender.request.ack_clock_operation,
        Some(RequestAckClockOperation::Owner {
            candidate,
            target_bytes: 2 * 1024 * 1024,
        })
    );

    sender.commit_request_ack_clock_calibration(Some(
        RequestAckClockCalibrationCommit::OwnerData {
            candidate,
            target_bytes: 2 * 1024 * 1024,
            payload_bytes: 64 * 1024,
            entry_offset: 128 * 1024,
            foreign_optional_ranges: 1,
            foreign_optional_bytes: 64 * 1024,
        },
    ));
    assert_eq!(
        sender
            .request
            .subflows
            .get(candidate)
            .and_then(|state| state.ack_clock_calibration_bytes())
            .expect("candidate calibration spend"),
        128 * 1024
    );
}

#[test]
fn tcp_carrier_expiry_preserves_only_sealed_product_transaction() {
    let now = Instant::now();
    assert!(request_tcp_carrier_authority_expired_naturally(
        true,
        Some(now),
        now,
    ));
    assert!(!request_tcp_carrier_authority_expired_naturally(
        false,
        Some(now),
        now,
    ));
    assert!(!request_tcp_carrier_authority_expired_naturally(
        true,
        Some(now + Duration::from_secs(1)),
        now,
    ));
    let service = RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        },
        id: 3,
    };
    let candidate = RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 1,
        },
        id: 7,
    };
    let target_bytes = 2 * 1024 * 1024;
    let seed_owner = |spent_bytes| {
        let mut sender = RequestSenderService::new(StreamId(110));
        sender.request.ack_clock_operation = Some(RequestAckClockOperation::Owner {
            candidate,
            target_bytes,
        });
        let candidate_state = sender.request.subflows.get_mut(candidate);
        candidate_state.set_ack_clock_calibration_bytes(spent_bytes);
        candidate_state.set_ack_clock_calibration_target(target_bytes);
        candidate_state.mark_tcp_capacity_proven();
        candidate_state.mark_graduated();
        candidate_state.mark_rate_proven();
        let _ = candidate_state.rate_evidence_mut(Instant::now());
        sender
    };

    let mut sealed = seed_owner(target_bytes);
    assert!(sealed.revoke_request_tcp_capacity_calibration(candidate, true));
    assert!(
        !sealed
            .request
            .subflows
            .get(candidate)
            .is_some_and(|state| state.tcp_capacity_proven())
    );
    assert!(
        sealed
            .request
            .subflows
            .get(candidate)
            .is_some_and(|state| state.graduated())
    );
    assert!(
        sealed
            .request
            .subflows
            .get(candidate)
            .is_some_and(|state| state.rate_proven())
    );
    assert!(
        sealed
            .request
            .subflows
            .get(candidate)
            .and_then(|state| state.rate_evidence())
            .is_some()
    );
    assert_eq!(
        sealed.request.ack_clock_operation,
        Some(RequestAckClockOperation::Owner {
            candidate,
            target_bytes,
        })
    );
    assert_eq!(
        sealed
            .request
            .subflows
            .get(candidate)
            .and_then(|state| state.ack_clock_calibration_bytes())
            .expect("sealed calibration spend"),
        target_bytes
    );

    let mut partial = seed_owner(target_bytes - 64 * 1024);
    assert!(!partial.revoke_request_tcp_capacity_calibration(candidate, true));
    assert!(partial.request.ack_clock_operation.is_none());
    assert!(
        !partial
            .request
            .subflows
            .get(candidate)
            .is_some_and(|state| state.graduated())
    );
    assert!(
        partial
            .request
            .subflows
            .get(candidate)
            .and_then(|state| state.rate_evidence())
            .is_none()
    );
    assert!(
        partial
            .request
            .subflows
            .get(candidate)
            .and_then(|state| state.ack_clock_calibration_bytes())
            .is_none()
    );

    let mut pending = RequestSenderService::new(StreamId(111));
    pending.request.ack_clock_operation =
        Some(RequestAckClockOperation::Pending { service, candidate });
    let candidate_state = pending.request.subflows.get_mut(candidate);
    candidate_state.mark_tcp_capacity_proven();
    candidate_state.mark_graduated();
    assert!(!pending.revoke_request_tcp_capacity_calibration(candidate, true));
    assert!(pending.request.ack_clock_operation.is_none());
    assert!(
        !pending
            .request
            .subflows
            .get(candidate)
            .is_some_and(|state| state.graduated())
    );

    let mut detached = seed_owner(target_bytes);
    assert!(!detached.revoke_request_tcp_capacity_calibration(candidate, false));
    assert!(detached.request.ack_clock_operation.is_none());
    assert!(
        !detached
            .request
            .subflows
            .get(candidate)
            .is_some_and(|state| state.graduated())
    );
}

#[tokio::test]
async fn startup_epoch_clears_when_candidate_is_no_longer_validation() {
    let stream_id = StreamId(111);
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10297?srtt-ms=20&rate-mbps=500",
        "tcp://127.0.0.1:10298?srtt-ms=10&rate-mbps=500",
    ]);
    let (service_commands, _service_rx) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, service_commands), 8);
    let (candidate_commands, _candidate_rx) = reliable_path_command_channels(8);
    remotes.attach_for_validation(opened_test_relay_stream(stream_id, 1, candidate_commands));
    let service = remotes
        .paths
        .iter()
        .find(|path| path.placement == RelayPathPlacement::Active)
        .expect("Active Service")
        .instance();
    let candidate = remotes
        .paths
        .iter()
        .find(|path| path.placement == RelayPathPlacement::Validation)
        .expect("Validation candidate")
        .instance();
    let mut epoch = FlowSubflowSet::new(0, service, 256 * 1024, 0, Duration::ZERO);
    assert_eq!(
        epoch
            .admit_subflow_owner(SubflowAdmissionInput {
                key: candidate,
                bulk_rate_proven: false,
                startup_owner_allowed: true,
                frontier_clear: true,
                completion_improves: false,
                observed_goodput_non_degrading: true,
                read_gap: Duration::ZERO,
                owner_bytes: 64 * 1024,
                optional_overhead_bytes: 0,
            })
            .decision,
        PathAdmissionDecision::AdmitSubflow
    );
    let mut sender = RequestSenderService::new(stream_id);
    sender.request.ordered_service = Some(service);
    sender.request.startup.epoch = Some(epoch);
    sender.request.startup.attempted_subflows.insert(candidate);
    sender.request.ack_clock_operation = Some(RequestAckClockOperation::Owner {
        candidate,
        target_bytes: 2 * 1024 * 1024,
    });
    let candidate_state = sender.request.subflows.get_mut(candidate);
    candidate_state.set_ack_clock_calibration_bytes(64 * 1024);
    candidate_state.set_ack_clock_calibration_target(2 * 1024 * 1024);
    remotes
        .paths
        .iter_mut()
        .find(|path| path.instance() == candidate)
        .expect("candidate path")
        .placement = RelayPathPlacement::Active;

    sender.reconcile_request_subflow_set(&context, &remotes);

    assert!(sender.request.startup.epoch.is_none());
    assert_eq!(
        sender.request.ack_clock_operation, None,
        "an optional calibration epoch cannot survive promotion away from Validation"
    );
    assert!(
        sender
            .request
            .subflows
            .get(candidate)
            .and_then(|state| state.ack_clock_calibration_bytes())
            .is_none()
    );
    assert!(
        sender
            .request
            .subflows
            .get(candidate)
            .and_then(|state| state.ack_clock_calibration_target())
            .is_none()
    );
    assert!(
        !sender
            .request
            .subflows
            .get(candidate)
            .is_some_and(|state| state.graduated()),
        "real placement loss must fully abort any preserved AwaitingAck state"
    );
    sender.request.ack_clock_operation =
        Some(RequestAckClockOperation::Pending { service, candidate });
    sender.reconcile_request_subflow_set(&context, &remotes);
    assert_eq!(
        sender.request.ack_clock_operation, None,
        "pending exact-instance entry cannot survive promotion away from Validation"
    );
    assert!(
        sender
            .request
            .startup
            .attempted_subflows
            .contains(&candidate),
        "a live role change invalidates the epoch without minting fresh credit"
    );
}

#[tokio::test]
async fn orphaned_validation_owner_tail_repairs_on_active_service() {
    let stream_id = StreamId(112);
    let limits = MuxLimits::default();
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10299?srtt-ms=20&rate-mbps=500",
        "tcp://127.0.0.1:10300?srtt-ms=10&rate-mbps=500",
    ]);
    let (service_commands, mut service_rx) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, service_commands), 8);
    let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(8);
    remotes.attach_for_validation(opened_test_relay_stream(stream_id, 1, candidate_commands));
    consume_client_validation_proof_for_test(&mut candidate_rx);
    let service = remotes
        .paths
        .iter()
        .find(|path| path.placement == RelayPathPlacement::Active)
        .expect("Active Service")
        .instance();
    let candidate = remotes
        .paths
        .iter()
        .find(|path| path.placement == RelayPathPlacement::Validation)
        .expect("Validation candidate")
        .instance();
    let mut send_stream = ReliableSendStream::new(stream_id, limits);
    let _prefix = send_stream
        .send_data(Bytes::from(vec![0x31; 64]), StreamFlags::NONE)
        .expect("prefix");
    let candidate_tail = send_stream
        .send_data(Bytes::from(vec![0x32; 64]), StreamFlags::NONE)
        .expect("candidate tail");
    let ack_ranges = [OffsetRange::new(0, 64).expect("prefix ACK")];
    let _ = send_stream.apply_ack(&ack_ranges);

    let mut sender = RequestSenderService::new(stream_id);
    sender.request.ordered_service = Some(service);
    sender
        .request
        .flights
        .record_owner_frame_instance(candidate, &candidate_tail);
    sender.age_product_flights_for_test(Duration::from_secs(10));
    sender.reset_request_subflow_epoch();
    let mut sender_queue = ReliableRelaySenderQueue::default();
    assert!(sender.enqueue_live_owner_tail_repair(
        &mut sender_queue,
        &context,
        &remotes,
        &send_stream,
        &ack_ranges,
        true,
        64,
        FlowLane::Throughput,
    ));
    assert_eq!(
        sender.discard_unusable_live_owner_tail_repairs(&mut sender_queue, &remotes),
        0,
        "ledger-owned Validation debt remains a live repair source after epoch reset"
    );
    let spec = ReliableRelayOpenSpec {
        target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80))),
        ingress: IngressKind::Socks5,
    };
    let dispatch = sender
        .dispatch_client_queued_work(
            &context,
            &spec,
            FlowLane::Throughput,
            FlowLane::Throughput,
            &mut remotes,
            &mut send_stream,
            &mut sender_queue,
            true,
            &HashSet::new(),
            64,
        )
        .await
        .expect("dispatch repair on Service");
    assert!(matches!(dispatch, ClientQueuedDispatch::Repair { .. }));
    assert!(matches!(
        try_recv_reliable_path_command(&mut service_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            offset: 64,
            ..
        }))
    ));
    assert!(try_recv_reliable_path_command(&mut candidate_rx).is_none());
}

#[test]
fn client_repair_extra_budget_is_cumulative_not_per_event() {
    let mux_limits = MuxLimits::default();
    let stream_id = StreamId(93);
    let mut sender = RequestSenderService::new_with_performance(
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 1,
        },
    );
    let mut sender_queue = ReliableRelaySenderQueue::default();
    let startup_floor = sender_extra_traffic_startup_floor_bytes(mux_limits);
    let repair_payload = Bytes::from(vec![0x33; startup_floor]);

    assert!(sender.enqueue_repair_frame_with_priority(
        &mut sender_queue,
        Frame::StreamData {
            stream_id,
            offset: 0,
            flags: StreamFlags::NONE,
            payload: repair_payload.clone(),
        },
        RelaySendCause::AckGapRepair,
        mux_limits,
        false,
    ));
    assert!(!sender.enqueue_repair_frame_with_priority(
        &mut sender_queue,
        Frame::StreamData {
            stream_id,
            offset: startup_floor as u64,
            flags: StreamFlags::NONE,
            payload: repair_payload.clone(),
        },
        RelaySendCause::AckGapRepair,
        mux_limits,
        false,
    ));

    sender.record_owner_progress_for_test(startup_floor.saturating_mul(100));
    assert!(sender.enqueue_repair_frame_with_priority(
        &mut sender_queue,
        Frame::StreamData {
            stream_id,
            offset: (startup_floor * 2) as u64,
            flags: StreamFlags::NONE,
            payload: repair_payload,
        },
        RelaySendCause::PathFailureRepair,
        mux_limits,
        false,
    ));
}

#[test]
fn client_critical_repair_closes_tail_after_optional_budget_exhaustion() {
    let mux_limits = MuxLimits::default();
    let stream_id = StreamId(95);
    let mut sender = RequestSenderService::new_with_performance(
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 1,
        },
    );
    let mut sender_queue = ReliableRelaySenderQueue::default();
    let startup_floor = sender_extra_traffic_startup_floor_bytes(mux_limits);
    let frame = Frame::StreamData {
        stream_id,
        offset: 0,
        flags: StreamFlags::NONE,
        payload: Bytes::from(vec![0x33; startup_floor]),
    };
    assert!(sender.enqueue_repair_frame_with_priority(
        &mut sender_queue,
        frame,
        RelaySendCause::AckGapRepair,
        mux_limits,
        false,
    ));

    let closure_frame = Frame::StreamData {
        stream_id,
        offset: startup_floor as u64,
        flags: StreamFlags::NONE,
        payload: Bytes::from_static(b"tail"),
    };
    assert!(!sender.enqueue_repair_frame_with_priority(
        &mut sender_queue,
        closure_frame.clone(),
        RelaySendCause::AckGapRepair,
        mux_limits,
        false,
    ));

    sender.enqueue_critical_repair_frame(
        &mut sender_queue,
        closure_frame,
        RelaySendCause::AckGapRepair,
    );
    assert_eq!(sender.extra_traffic_budget_remaining(mux_limits), 0);
}

#[test]
fn client_critical_tail_repair_is_idempotent_while_range_is_queued() {
    let mux_limits = MuxLimits::default();
    let stream_id = StreamId(97);
    let mut sender = RequestSenderService::new_with_performance(
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 1,
        },
    );
    let mut sender_queue = ReliableRelaySenderQueue::default();
    let first = Frame::StreamData {
        stream_id,
        offset: 128,
        flags: StreamFlags::NONE,
        payload: Bytes::from_static(&[0x55; 64]),
    };
    let duplicate = first.clone();

    assert!(sender.enqueue_critical_tail_repair_frame(&mut sender_queue, first));
    let bytes_after_first = sender_queue.bytes();
    let budget_after_first = sender.extra_traffic_budget_remaining(mux_limits);

    assert!(
        !sender.enqueue_critical_tail_repair_frame(&mut sender_queue, duplicate),
        "client final-tail RepairData must not stack duplicate pending ranges"
    );
    assert_eq!(sender_queue.bytes(), bytes_after_first);
    assert_eq!(
        sender.extra_traffic_budget_remaining(mux_limits),
        budget_after_first
    );
}
