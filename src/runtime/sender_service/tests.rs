use super::*;
use crate::config::SharedSecret;
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::{
    lab_assert_server_sender_service_balanced, lab_diag_test_guard,
    lab_sender_service_counts_for_test,
};

fn poison_client_path_health_for_test(context: &ClientPathContext) {
    let health = Arc::clone(&context.health);
    assert!(
        std::thread::spawn(move || {
            let _guard = health.lock().expect("path health lock");
            panic!("poison path health for a no-lock fast-path assertion");
        })
        .join()
        .is_err()
    );
    assert!(context.health.is_poisoned());
}

#[test]
fn request_tcp_rate_uses_representative_coverage_for_service_and_candidate() {
    let service = RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        },
        id: 1,
    };
    let candidate = RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 1,
        },
        id: 2,
    };
    let mux_limits = MuxLimits::default();
    assert_eq!(
        request_path_rate_coverage_floor_bytes(service, Some(service), None, mux_limits,),
        reliable_ack_clock_calibration_rate_coverage_floor_bytes(mux_limits)
    );
    assert_eq!(
        request_path_rate_coverage_floor_bytes(
            candidate,
            Some(service),
            Some(reliable_request_ack_clock_calibration_target_bytes(
                mux_limits
            )),
            mux_limits,
        ),
        reliable_request_ack_clock_calibration_target_bytes(mux_limits)
    );
}

#[test]
fn request_tcp_turnover_authority_requires_calibration_plus_ordinary_coverage() {
    let started = Instant::now();
    let target = reliable_request_ack_clock_calibration_target_bytes(MuxLimits::default());
    let first_ack = started + Duration::from_millis(100);
    let mut split = RequestPathRateEvidence::new(started);
    let mut coalesced = RequestPathRateEvidence::new(started);

    for evidence in [&mut split, &mut coalesced] {
        assert!(matches!(
            evidence.observe(target, started, started, first_ack, target, true),
            RequestPathRateEvidenceUpdate::Proven {
                sample: Some(_),
                ..
            }
        ));
        assert!(!request_tcp_candidate_turnover_authorized(
            evidence.exact_attributed_bytes(),
            target,
            target,
        ));
    }

    let second_sent = first_ack + Duration::from_millis(1);
    assert!(matches!(
        split.observe(
            target / 2,
            second_sent,
            second_sent,
            first_ack + Duration::from_millis(50),
            target,
            true,
        ),
        RequestPathRateEvidenceUpdate::Pending
    ));
    assert!(!request_tcp_candidate_turnover_authorized(
        split.exact_attributed_bytes(),
        target,
        target,
    ));
    assert!(matches!(
        split.observe(
            target - target / 2,
            second_sent,
            second_sent,
            first_ack + Duration::from_millis(100),
            target,
            true,
        ),
        RequestPathRateEvidenceUpdate::Proven {
            sample: Some(_),
            ..
        }
    ));
    assert!(matches!(
        coalesced.observe(
            target,
            second_sent,
            second_sent,
            first_ack + Duration::from_millis(100),
            target,
            true,
        ),
        RequestPathRateEvidenceUpdate::Proven {
            sample: Some(_),
            ..
        }
    ));
    assert_eq!(
        split.exact_attributed_bytes(),
        coalesced.exact_attributed_bytes()
    );
    assert!(request_tcp_candidate_turnover_authorized(
        split.exact_attributed_bytes(),
        target,
        target,
    ));
    assert!(request_tcp_candidate_turnover_authorized(
        coalesced.exact_attributed_bytes(),
        target,
        target,
    ));
}

#[test]
fn request_tcp_turnover_smooths_same_epoch_pipe_and_expires_at_three_ptos() {
    let started = Instant::now();
    let first_sample =
        PathRateSample::new(1_000_000, Duration::from_millis(100)).expect("first rate sample");
    let first_pto = Duration::from_millis(50);
    let first = RequestTcpAckTurnoverModel::observe(None, first_sample, first_pto, started)
        .expect("first turnover");
    let first_pipe = first_sample.rate_bps() / 8.0 * first_pto.as_secs_f64();
    assert_eq!(first.turnover_bytes, first_pipe);
    assert!(first.is_fresh_at(started + Duration::from_millis(149)));
    assert!(!first.is_fresh_at(started + Duration::from_millis(150)));

    let second_sample =
        PathRateSample::new(2_000_000, Duration::from_millis(100)).expect("second rate sample");
    let second_pto = Duration::from_millis(200);
    let second = RequestTcpAckTurnoverModel::observe(
        Some(first),
        second_sample,
        second_pto,
        started + Duration::from_millis(100),
    )
    .expect("smoothed turnover");
    let second_pipe = second_sample.rate_bps() / 8.0 * second_pto.as_secs_f64();
    assert_eq!(
        second.turnover_bytes,
        first_pipe.mul_add(0.75, second_pipe * 0.25),
        "smooth pipe estimates captured with each sample's PTO, not a retained rate times current PTO"
    );
}

#[test]
fn request_rate_evidence_uses_ack_clock_after_initial_provenance() {
    let started = Instant::now();
    let bytes = PATH_OPEN_SCORE_BYTES as u64;
    let mut evidence = RequestPathRateEvidence::new(started);

    let initial = match evidence.observe(
        bytes,
        started,
        started,
        started + Duration::from_millis(100),
        bytes,
        true,
    ) {
        RequestPathRateEvidenceUpdate::Proven {
            sample: Some(sample),
            ..
        } => sample.rate_bps(),
        _ => panic!("first complete window must publish conservative provenance"),
    };
    let ack_clocked = match evidence.observe(
        bytes,
        started + Duration::from_millis(100),
        started + Duration::from_millis(100),
        started + Duration::from_millis(101),
        bytes,
        true,
    ) {
        RequestPathRateEvidenceUpdate::Proven {
            sample: Some(sample),
            ..
        } => sample.rate_bps(),
        _ => panic!("pipelined bytes must use ACK-to-ACK delivery time"),
    };

    assert!(
        ack_clocked > initial * 50.0,
        "a post-boundary stage must use ACK-to-ACK time without charging the first-stage RTT again"
    );
}

#[test]
fn request_service_rate_keeps_continuous_ack_clock_for_pipelined_bytes() {
    let started = Instant::now();
    let bytes = PATH_OPEN_SCORE_BYTES as u64;
    let first_ack = started + Duration::from_millis(100);
    let mut evidence = RequestPathRateEvidence::new(started);
    assert!(matches!(
        evidence.observe(bytes, started, started, first_ack, bytes, false),
        RequestPathRateEvidenceUpdate::Proven {
            sample: Some(_),
            first_window: true,
        }
    ));

    let sample = match evidence.observe(
        bytes,
        started + Duration::from_millis(90),
        started + Duration::from_millis(95),
        started + Duration::from_millis(120),
        bytes,
        false,
    ) {
        RequestPathRateEvidenceUpdate::Proven {
            sample: Some(sample),
            first_window: false,
        } => sample,
        _ => panic!("ordered Service bytes must retain continuous ACK-clock evidence"),
    };
    assert_eq!(sample.elapsed(), Duration::from_millis(20));
}

#[test]
fn request_rate_evidence_charges_post_boundary_idle_gap() {
    let started = Instant::now();
    let bytes = PATH_OPEN_SCORE_BYTES as u64;
    let mut evidence = RequestPathRateEvidence::new(started);
    assert!(matches!(
        evidence.observe(
            bytes,
            started,
            started,
            started + Duration::from_millis(100),
            bytes,
            true,
        ),
        RequestPathRateEvidenceUpdate::Proven {
            sample: Some(_),
            ..
        }
    ));

    let conservative = match evidence.observe(
        bytes,
        started + Duration::from_millis(200),
        started + Duration::from_millis(200),
        started + Duration::from_millis(300),
        bytes,
        true,
    ) {
        RequestPathRateEvidenceUpdate::Proven {
            sample: Some(sample),
            ..
        } => sample,
        _ => panic!("post-boundary bytes must retain the full idle gap in their rate"),
    };
    assert_eq!(conservative.elapsed(), Duration::from_millis(200));
    assert!(matches!(
        evidence.observe(
            bytes,
            started + Duration::from_millis(290),
            started + Duration::from_millis(290),
            started + Duration::from_millis(301),
            bytes,
            true,
        ),
        RequestPathRateEvidenceUpdate::Proven { sample: None, .. }
    ));
}

#[test]
fn request_rate_evidence_rejects_window_with_any_pre_ack_bytes() {
    let started = Instant::now();
    let bytes = PATH_OPEN_SCORE_BYTES as u64;
    let previous_ack = started + Duration::from_millis(100);
    let mut evidence = RequestPathRateEvidence::new(started);
    assert!(matches!(
        evidence.observe(bytes, started, started, previous_ack, bytes, true),
        RequestPathRateEvidenceUpdate::Proven {
            sample: Some(_),
            ..
        }
    ));

    let old_byte_sent_at = started + Duration::from_millis(90);
    let new_bytes_sent_at = started + Duration::from_millis(101);
    assert!(matches!(
        evidence.observe(
            1,
            old_byte_sent_at,
            old_byte_sent_at,
            started + Duration::from_millis(110),
            bytes,
            true,
        ),
        RequestPathRateEvidenceUpdate::Pending
    ));
    assert!(matches!(
        evidence.observe(
            bytes - 1,
            new_bytes_sent_at,
            new_bytes_sent_at,
            started + Duration::from_millis(200),
            bytes,
            true,
        ),
        RequestPathRateEvidenceUpdate::Proven { sample: None, .. }
    ));
}

#[test]
fn request_rate_evidence_waits_for_representative_coverage() {
    let started = Instant::now();
    let coverage_floor =
        reliable_ack_clock_calibration_rate_coverage_floor_bytes(MuxLimits::default());
    let mut evidence = RequestPathRateEvidence::new(started);

    assert!(matches!(
        evidence.observe(
            coverage_floor / 2,
            started,
            started,
            started + Duration::from_millis(10),
            coverage_floor,
            true,
        ),
        RequestPathRateEvidenceUpdate::Pending
    ));
    assert!(matches!(
        evidence.observe(
            coverage_floor - coverage_floor / 2,
            started,
            started,
            started + Duration::from_millis(20),
            coverage_floor,
            true,
        ),
        RequestPathRateEvidenceUpdate::Proven {
            sample: Some(_),
            first_window: true,
        }
    ));
}

#[test]
fn request_rate_evidence_post_boundary_clock_cannot_outrun_send_rate() {
    let started = Instant::now();
    let bytes = reliable_ack_clock_calibration_rate_coverage_floor_bytes(MuxLimits::default());
    let mut evidence = RequestPathRateEvidence::new(started);
    assert!(matches!(
        evidence.observe(
            bytes,
            started,
            started,
            started + Duration::from_millis(100),
            bytes,
            true,
        ),
        RequestPathRateEvidenceUpdate::Proven {
            first_window: true,
            ..
        }
    ));

    let sample = match evidence.observe(
        bytes,
        started + Duration::from_millis(100),
        started + Duration::from_millis(140),
        started + Duration::from_millis(141),
        bytes,
        true,
    ) {
        RequestPathRateEvidenceUpdate::Proven {
            sample: Some(sample),
            first_window: false,
        } => sample,
        _ => panic!("a causal second window must produce a sample"),
    };
    assert_eq!(sample.elapsed(), Duration::from_millis(41));
    assert_eq!(sample.rate_bps(), bytes as f64 * 8.0 / 0.041);
}

#[test]
fn sender_queue_dispatches_owner_data_before_ordinary_repair() {
    let stream_id = StreamId(77);
    let mut queue = ReliableRelaySenderQueue::default();

    queue.push_data(Bytes::from_static(b"owner"));
    queue.push_repair(Frame::StreamData {
        stream_id,
        offset: 0,
        flags: StreamFlags::NONE,
        payload: Bytes::from_static(b"repair"),
    });

    let (lane, work) = queue
        .pop_front()
        .expect("ordinary owner data should be queued");
    assert_eq!(
        lane,
        ReliableRelayQueuedWorkLane::Data,
        "ordinary RepairData must not preempt OwnerData; repair only preempts when explicitly critical"
    );
    assert_eq!(work.payload_bytes, 5);
}

#[test]
fn sender_queue_dispatches_critical_repair_before_owner_data() {
    let stream_id = StreamId(78);
    let mut queue = ReliableRelaySenderQueue::default();

    queue.push_data(Bytes::from_static(b"owner"));
    queue.push_critical_repair_with_cause(
        Frame::StreamData {
            stream_id,
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(b"repair"),
        },
        RelaySendCause::AckGapRepair,
    );

    let (lane, work) = queue.pop_front().expect("critical repair should be queued");
    assert_eq!(
        lane,
        ReliableRelayQueuedWorkLane::Repair,
        "critical RepairData closes an active product hole and must preempt later OwnerData"
    );
    assert_eq!(work.payload_bytes, 6);
}

#[test]
fn sender_queue_trims_and_releases_acked_live_tail_repair() {
    let stream_id = StreamId(80);
    let mut queue = ReliableRelaySenderQueue::default();
    queue.push_critical_repair_with_cause(
        Frame::StreamData {
            stream_id,
            offset: 128,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(&[0x5a; 64]),
        },
        RelaySendCause::LiveOwnerTailRepair,
    );

    assert_eq!(
        queue.release_normalized_acked_repairs(&[OffsetRange { start: 0, end: 160 }]),
        32,
    );
    assert_eq!(queue.bytes(), 32);
    assert!(matches!(
        queue.front().map(|(_, work)| &work.kind),
        Some(ReliableRelayQueuedWorkKind::Repair {
            frame: Frame::StreamData { offset: 160, payload, .. },
            cause: RelaySendCause::LiveOwnerTailRepair,
        }) if payload.len() == 32
    ));

    assert_eq!(
        queue.release_normalized_acked_repairs(&[OffsetRange { start: 0, end: 192 }]),
        32,
    );
    assert!(queue.is_empty());
    assert_eq!(queue.bytes(), 0);
}

#[test]
fn sender_queue_discards_only_unusable_live_owner_tail_repair() {
    let stream_id = StreamId(81);
    let mut queue = ReliableRelaySenderQueue::default();
    for cause in [
        RelaySendCause::LiveOwnerTailRepair,
        RelaySendCause::PathFailureRepair,
    ] {
        queue.push_critical_repair_with_cause(
            Frame::StreamData {
                stream_id,
                offset: if cause == RelaySendCause::LiveOwnerTailRepair {
                    0
                } else {
                    64
                },
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(&[0x5b; 64]),
            },
            cause,
        );
    }

    assert_eq!(
        queue.discard_unusable_live_owner_tail_repairs(|_| false),
        64,
    );
    assert_eq!(queue.bytes(), 64);
    assert!(matches!(
        queue.front().map(|(_, work)| &work.kind),
        Some(ReliableRelayQueuedWorkKind::Repair {
            cause: RelaySendCause::PathFailureRepair,
            ..
        })
    ));
}

#[test]
fn sender_queue_discards_stale_bound_repair_without_touching_ordinary_repair() {
    let stream_id = StreamId(82);
    let mut queue = ReliableRelaySenderQueue::default();
    let cause = RelaySendCause::PersistentClientAckGapRepair(PersistentClientAckGapBatch {
        target: ClientRepairOutputIdentity {
            instance: RelayPathInstance {
                key: RelayPathKey {
                    underlay: UnderlayProtocol::Tcp,
                    index: 2,
                },
                id: 7,
            },
        },
        expires_at: Instant::now() + Duration::from_secs(1),
    });
    for (offset, cause) in [(0, cause), (64, RelaySendCause::AckGapRepair)] {
        queue.push_critical_repair_with_cause(
            Frame::StreamData {
                stream_id,
                offset,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(&[0x5c; 64]),
            },
            cause,
        );
    }

    assert_eq!(
        queue.discard_stale_persistent_ack_gap_repairs(|_| false),
        64
    );
    assert_eq!(queue.bytes(), 64);
    assert!(matches!(
        queue.front().map(|(_, work)| &work.kind),
        Some(ReliableRelayQueuedWorkKind::Repair {
            cause: RelaySendCause::AckGapRepair,
            ..
        })
    ));
}

#[test]
fn sender_queue_discards_expired_bound_repair_on_live_output() {
    let mut queue = ReliableRelaySenderQueue::default();
    queue.push_critical_repair_with_cause(
        Frame::StreamData {
            stream_id: StreamId(83),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(&[0x5d; 64]),
        },
        RelaySendCause::PersistentServerAckGapRepair(PersistentServerAckGapBatch {
            target: ServerRepairOutputIdentity {
                key: CarrierPathKey {
                    underlay: UnderlayProtocol::Udp,
                    path_id: PathId(3),
                },
                incarnation: 9,
            },
            expires_at: Instant::now() - Duration::from_millis(1),
        }),
    );

    assert_eq!(queue.discard_stale_persistent_ack_gap_repairs(|_| true), 64);
    assert!(queue.is_empty());
    assert_eq!(queue.bytes(), 0);
}

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
    let startup_floor = response_extra_traffic_startup_floor_bytes(mux_limits);

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

    assert_eq!(
        sender.queue.front_lane(),
        Some(ReliableRelayQueuedWorkLane::Repair)
    );
    assert_eq!(
        sender.repair_extra_budget_remaining(mux_limits),
        0,
        "critical priority is not budget bypass"
    );
}

fn security() -> SecurityConfig {
    SecurityConfig::encrypted(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    )
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
    let mut sender = RelaySenderService::new(StreamId(7));
    sender.flights.record_owner_frame(key, &frame);

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
    let mut sender = RelaySenderService::new(StreamId(11));
    sender.flights.record_owner_frame_instance(instance, &frame);

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
    let mut sender = RelaySenderService::new(StreamId(12));
    sender.flights.record_owner_frame_instance(instance, &frame);
    sender
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
    let mut sender = RelaySenderService::new(StreamId(9));
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
        sender.flights.record_owner_frame_instance(instance, frame);
    }

    sender.release_normalized_acked_ranges(
        &context,
        &[OffsetRange::new(0, (4 * BBR_MAX_SEND_QUANTUM_BYTES) as u64)
            .expect("cumulative ACK range")],
    );
    let delivery_samples = context.health.lock().expect("path health lock").tcp[0].delivery_samples;

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
    let mut sender = RelaySenderService::new(StreamId(10));
    let chunk = 8 * 1024;
    let first = client_data_frame_for_test(StreamId(10), 0, chunk);
    let second = client_data_frame_for_test(StreamId(10), chunk as u64, chunk);
    for frame in [&first, &second] {
        context.record_relay_path_send(instance.key.underlay, instance.key.index, chunk);
        sender.flights.record_owner_frame_instance(instance, frame);
    }

    sender.release_normalized_acked_ranges(
        &context,
        &[OffsetRange::new(0, chunk as u64).expect("first ACK range")],
    );
    assert!(!sender.request_rate_proven_subflows.contains(&instance));
    assert_eq!(
        context.health.lock().expect("path health lock").tcp[0].delivery_samples,
        0
    );

    sender.release_normalized_acked_ranges(
        &context,
        &[OffsetRange::new(chunk as u64, (2 * chunk) as u64).expect("second ACK range")],
    );
    let health = context.health.lock().expect("path health lock");
    assert!(sender.request_rate_proven_subflows.contains(&instance));
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
    let mut sender = RelaySenderService::new(StreamId(12));
    sender.ordered_data_owner = Some(instance.key);
    sender.ordered_data_owner_instance = Some(instance);
    let coverage = usize::try_from(reliable_ack_clock_calibration_rate_coverage_floor_bytes(
        context.mux_limits,
    ))
    .expect("coverage");
    let frame = client_data_frame_for_test(StreamId(12), 0, coverage);
    context.record_relay_path_send(instance.key.underlay, instance.key.index, coverage);
    sender.flights.record_owner_frame_instance(instance, &frame);

    sender.release_normalized_acked_ranges(
        &context,
        &[OffsetRange::new(0, coverage as u64).expect("Service ACK")],
    );

    assert!(context.relay_path_has_bulk_model_evidence(instance.key.underlay, instance.key.index));
    assert!(!sender.request_per_flow_rate_bps.contains_key(&instance));
    assert!(!sender.request_ack_clock_proven_subflows.contains(&instance));
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
    let mut sender = RelaySenderService::new(StreamId(11));
    let coverage_floor = usize::try_from(reliable_ack_clock_calibration_rate_coverage_floor_bytes(
        context.mux_limits,
    ))
    .expect("coverage floor");
    let first = client_data_frame_for_test(StreamId(11), 0, coverage_floor);
    let second = client_data_frame_for_test(StreamId(11), coverage_floor as u64, coverage_floor);
    context.record_relay_path_send(instance.key.underlay, instance.key.index, coverage_floor);
    sender.flights.record_owner_frame_instance(instance, &first);

    sender.release_normalized_acked_ranges(
        &context,
        &[OffsetRange::new(0, coverage_floor as u64).expect("first window")],
    );
    assert!(sender.request_rate_proven_subflows.contains(&instance));
    assert!(!sender.request_ack_clock_proven_subflows.contains(&instance));
    assert!(!sender.request_per_flow_rate_bps.contains_key(&instance));
    assert_eq!(
        context.health.lock().expect("path health lock").tcp[0].delivery_samples,
        0,
        "the RTT-bearing first window establishes the clock but is not a rate sample"
    );

    context.record_relay_path_send(instance.key.underlay, instance.key.index, coverage_floor);
    sender
        .flights
        .record_owner_frame_instance(instance, &second);
    sender.release_normalized_acked_ranges(
        &context,
        &[
            OffsetRange::new(coverage_floor as u64, (2 * coverage_floor) as u64)
                .expect("second window"),
        ],
    );
    let health = context.health.lock().expect("path health lock");
    assert!(sender.request_ack_clock_proven_subflows.contains(&instance));
    assert!(sender.request_per_flow_rate_bps.contains_key(&instance));
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
    let mut sender = RelaySenderService::new(StreamId(8));
    sender.flights.record_owner_frame(owner, &frame);
    sender.flights.record_repair_frame(repair, &frame);

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

fn response_target(
    path_id: u16,
    underlay: UnderlayProtocol,
    eta_ms: f64,
    bytes_in_flight: u64,
    inflight_limit_bytes: u64,
    is_active: bool,
) -> ResponseSenderPathTarget {
    let (commands, _receivers) = reliable_path_command_channels(8);
    let mut snapshot = PathSnapshot::new(PathId(path_id), underlay, eta_ms.max(1.0), 500_000_000.0);
    snapshot.bytes_in_flight = bytes_in_flight;
    snapshot.product_bytes_in_flight = bytes_in_flight;
    snapshot.inflight_limit_bytes = inflight_limit_bytes;
    snapshot.confidence = 1.0;
    ResponseSenderPathTarget {
        #[cfg(feature = "lab-diagnostics")]
        session_id: SessionId(0),
        #[cfg(feature = "lab-diagnostics")]
        binding_instance_id: 0,
        key: CarrierPathKey {
            underlay,
            path_id: PathId(path_id),
        },
        path_instance_id: next_server_carrier_path_instance_id(),
        incarnation: u64::from(path_id) + 1,
        commands,
        attachment_role: if is_active {
            StreamOpenRole::Active
        } else {
            StreamOpenRole::Validation
        },
        snapshot,
        owner_data_in_flight_bytes: bytes_in_flight,
        command_pending_bytes: 0,
        eta_ms,
        is_active,
        is_request_active: is_active,
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
    }
}

#[cfg(target_os = "linux")]
#[test]
fn tcp_capacity_probe_does_not_wait_for_product_subflow_graduation() {
    let mux_limits = MuxLimits::default();
    let mut service = response_target(0, UnderlayProtocol::Tcp, 20.0, 0, 64 * 1024, true);
    let mut cold = response_target(1, UnderlayProtocol::Tcp, 80.0, 0, 64 * 1024, false);
    service.has_bulk_rate_evidence = false;
    cold.has_bulk_rate_evidence = false;
    let (cold_commands, _cold_receivers) = reliable_path_command_channels(4);
    cold.commands = cold_commands;

    assert!(
        select_response_tcp_capacity_probe_target(
            &[service.clone(), cold.clone()],
            FlowLane::Throughput,
            Some(service.key),
            ResponseServiceFamilyLoads::default(),
            mux_limits,
        )
        .is_none()
    );

    service.has_bulk_rate_evidence = true;
    let (selected, train_bytes) = select_response_tcp_capacity_probe_target(
        &[service.clone(), cold.clone()],
        FlowLane::Throughput,
        Some(service.key),
        ResponseServiceFamilyLoads::default(),
        mux_limits,
    )
    .expect("proven Service opens offset-free discovery");
    assert_eq!(selected.key, cold.key);
    assert_eq!(train_bytes, 2 * 1024 * 1024);

    let udp = response_target(2, UnderlayProtocol::Udp, 10.0, 0, 64 * 1024, false);
    assert!(
        select_response_tcp_capacity_probe_target(
            &[service.clone(), cold, udp],
            FlowLane::Throughput,
            Some(service.key),
            ResponseServiceFamilyLoads::new(2, 0),
            mux_limits,
        )
        .is_none(),
        "a measured cross-family handoff must outrank optional TCP discovery"
    );
}

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
fn persistent_response_repair_is_cancelled_when_output_incarnation_detaches() {
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(7),
    };
    let (commands, _receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(84),
        key.underlay,
        key.path_id,
        commands.clone(),
        FlowLane::Throughput,
    );
    let target = binding
        .sender_path_targets(FlowLane::Throughput, 64)
        .into_iter()
        .next()
        .expect("initial response output");
    let (_frames_tx, frames) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id: StreamId(84),
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: key.underlay,
        max_frame_payload_bytes: MuxLimits::default().max_payload_bytes,
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames,
    };
    let mut sender = ServerResponseSenderService::new(SessionId(84), StreamId(84));
    sender.enqueue_critical_repair_frame_with_cause(
        Frame::StreamData {
            stream_id: StreamId(84),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(&[0x5e; 64]),
        },
        RelaySendCause::persistent_server_ack_gap_repair(
            ServerRepairOutputIdentity {
                key,
                incarnation: target.incarnation,
            },
            target.snapshot,
            FlowLane::Throughput,
        ),
    );

    binding.detach(key, &commands);
    assert_eq!(
        sender.discard_stale_persistent_ack_gap_repairs(&path_stream),
        64
    );
    assert!(sender.is_empty());
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
            reliable_stream_frame_payload_bytes(&frame),
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
            reliable_stream_frame_payload_bytes(&frame),
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

fn client_test_context() -> ClientPathContext {
    let path = "tcp://127.0.0.1:10251".parse::<PathSpec>().expect("path");
    ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("context")
}

fn client_test_context_with_paths(paths: &[&str]) -> ClientPathContext {
    ClientPathContext::new(
        paths
            .iter()
            .map(|path| path.parse::<PathSpec>().expect("path"))
            .collect(),
        security(),
        ResourceLimits::default(),
    )
    .expect("context")
}

fn reserve_request_quic_capacity_calibration_for_test(
    sender: &mut RelaySenderService,
    context: &ClientPathContext,
    target: RelayPathInstance,
    valid_after: Instant,
    train_deadline: Instant,
    accepted_at: Instant,
    proof_validity: Duration,
) -> (
    QuicCapacityProofCandidate,
    crate::transport::quic_carrier::CapacityProbeMetrics,
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
            reliable_quic_capacity_calibration_session_limit_bytes(context.mux_limits),
            sender.request_quic_capacity_campaign.clone(),
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
    let probe = crate::transport::quic_carrier::CapacityProbeMetrics {
        token,
        train_payload_bytes: train_bytes,
        sample_floor_bytes: train_bytes,
        warmup_carrier_bytes: train_bytes - required_proof_bytes,
        required_timed_carrier_bytes: required_proof_bytes,
        expires_at: train_deadline,
        phase: crate::transport::quic_carrier::CapacityProbePhase::Proven,
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
        native_proved_at: Some(accepted_at),
        proved_at: Some(accepted_at),
        proof_validity,
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
    sender.request_quic_capacity_calibration = Some(RequestQuicCapacityCalibration {
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
    sender: &RelaySenderService,
    context: &ClientPathContext,
    target: RelayPathInstance,
    candidate: QuicCapacityProofCandidate,
    probe: crate::transport::quic_carrier::CapacityProbeMetrics,
) {
    context.health.lock().expect("path health lock").udp[target.key.index]
        .accept_request_quic_capacity_proof(candidate, probe, Instant::now())
        .expect("accept request QUIC capacity proof");
    assert_eq!(
        sender
            .request_quic_capacity_calibration
            .as_ref()
            .expect("request QUIC calibration")
            .ticket
            .resolution(),
        QuicCapacityProbeCommandResolution::Published
    );
}

fn install_request_quic_capacity_calibration_for_test(
    sender: &mut RelaySenderService,
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

fn opened_test_relay_stream(
    stream_id: StreamId,
    path_index: usize,
    commands: ReliablePathCommandSender,
) -> OpenedRemoteStream {
    opened_test_relay_stream_with_underlay(stream_id, UnderlayProtocol::Tcp, path_index, commands)
}

fn opened_test_relay_stream_with_underlay(
    stream_id: StreamId,
    underlay: UnderlayProtocol,
    path_index: usize,
    commands: ReliablePathCommandSender,
) -> OpenedRemoteStream {
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    OpenedRemoteStream {
        path_index,
        stream: ReliablePathStream {
            stream_id,
            max_offset: MuxLimits::default().max_stream_window_bytes,
            lane: FlowLane::Throughput,
            underlay,
            max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
            output: ReliablePathStreamOutput::fixed(
                underlay,
                PathId(path_index as u16),
                commands,
                MuxLimits::default(),
            ),
            frames: frame_rx,
        },
    }
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
    sender: &mut RelaySenderService,
    context: &ClientPathContext,
    frame: &Frame,
) {
    let (start, end, _) = reliable_stream_frame_extent(frame).expect("request data extent");
    sender.release_normalized_acked_ranges(
        context,
        &[OffsetRange::new(start, end).expect("request ACK range")],
    );
}

fn seed_client_bulk_evidence_for_test(context: &ClientPathContext, key: RelayPathKey) {
    context.mark_relay_path_rate_sample(
        key.underlay,
        key.index,
        PathRateSample::new(4 * 1024 * 1024, Duration::from_millis(20)).expect("bulk rate sample"),
    );
}

fn seed_client_quic_native_bulk_evidence_for_test(context: &ClientPathContext, index: usize) {
    context.health.lock().expect("path health lock").udp[index].mark_quic_path_metrics(
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

fn consume_client_validation_proof_for_test(receivers: &mut ReliablePathCommandReceivers) {
    assert!(matches!(
        try_recv_reliable_path_priority_command(receivers),
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
    ));
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
    let mut sender = RelaySenderService::new(stream_id);
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

fn mark_client_validation_proof_fresh_for_test(
    context: &ClientPathContext,
    remotes: &ReliableRelayRemoteSet,
    instance: RelayPathInstance,
    elapsed: Duration,
) {
    let (attached_at, proof_id) = remotes
        .paths
        .iter()
        .find(|path| path.instance() == instance)
        .map(|path| {
            (
                path.attached_at,
                path.path_proof_id.expect("queued attachment proof"),
            )
        })
        .expect("attached validation instance");
    context.mark_relay_path_proof_observation(
        instance.key.underlay,
        instance.key.index,
        PathProofObservation {
            proof_id,
            bytes: PATH_OPEN_SCORE_BYTES as u64,
            elapsed,
            sent_at: Instant::now(),
        },
    );
    assert!(context.relay_path_has_fresh_proof(
        instance.key.underlay,
        instance.key.index,
        proof_id,
        attached_at,
    ));
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
    let mut sender = RelaySenderService::new(stream_id);

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
    let mut sender = RelaySenderService::new(stream_id);

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
    let mut sender = RelaySenderService::new(stream_id);

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
    let mut sender = RelaySenderService::new(stream_id);

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
    let mut sender = RelaySenderService::new(stream_id);

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
    let mut sender = RelaySenderService::new(stream_id);

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
    let mut sender = RelaySenderService::new(stream_id);
    sender.ordered_data_owner = Some(slow_key);
    sender.ordered_data_owner_instance = Some(slow_instance);
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
        sender.ordered_data_owner,
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
    let mut sender = RelaySenderService::new(stream_id);
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
    assert_eq!(sender.ordered_data_owner, Some(service_key));
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
            .request_subflow_set
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
    let mut sender = RelaySenderService::new(stream_id);
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
            .request_subflow_set
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
            .request_subflow_set
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
    let mut sender = RelaySenderService::new(stream_id);
    sender.ordered_data_owner = Some(service.key);
    sender.ordered_data_owner_instance = Some(service);
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
    let mut sender = RelaySenderService::new(stream_id);
    sender.ordered_data_owner = Some(service.key);
    sender.ordered_data_owner_instance = Some(service);
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
        context.health.lock().expect("path health lock").tcp[1].active_flows,
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
    let mut sender = RelaySenderService::new(stream_id);
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
        .request_subflow_set
        .as_ref()
        .expect("request startup epoch");
    let candidate_member = epoch
        .members()
        .iter()
        .find(|member| member.key == candidate_instance)
        .expect("startup candidate member");
    assert_eq!(candidate_member.owner_sent_bytes, startup_limit as u64);
    let (receipt_proof_id, _) = sender
        .request_startup_receipt_proofs
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
        let health = context.health.lock().expect("path health lock");
        let candidate = &health.tcp[candidate_key.index];
        (
            candidate.delivery_samples,
            candidate.product_delivery_sample_bytes,
        )
    };
    sender.release_normalized_acked_ranges(&context, &[]);
    let health = context.health.lock().expect("path health lock");
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
        .request_subflow_set
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
    assert_eq!(sender.ordered_data_owner, Some(service_key));
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
    let mut sender = RelaySenderService::new(stream_id);
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
    assert!(sender.request_startup_receipt_proofs.is_empty());
    assert_eq!(
        sender
            .request_subflow_set
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
            .request_subflow_set
            .as_ref()
            .and_then(|epoch| epoch.startup_owner_sealed_sample_bytes(candidate)),
        Some(admitted_bytes as u64)
    );
    let (receipt_proof_id, _) = sender.request_startup_receipt_proofs[&candidate];
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

    assert!(sender.request_graduated_subflows.contains(&candidate));
    assert!(
        sender
            .request_ack_clock_first_window_subflows
            .contains(&candidate),
        "the ordered startup receipt is the causal boundary for calibration"
    );
    assert!(
        sender.request_rate_evidence[&candidate]
            .previous_window_acked_at
            .is_some()
    );
    let health = context.health.lock().expect("path health lock");
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
    let mut sender = RelaySenderService::new(stream_id);
    sender.ordered_data_owner = Some(service.key);
    sender.ordered_data_owner_instance = Some(service);

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

    sender.request_graduated_subflows.insert(candidate);
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
    sender.ordered_data_owner = Some(replacement.key);
    sender.ordered_data_owner_instance = Some(replacement);
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
    let mut sender = RelaySenderService::new(stream_id);
    sender.ordered_data_owner = Some(service.key);
    sender.ordered_data_owner_instance = Some(service);
    sender.request_graduated_subflows.insert(candidate);
    let now = Instant::now();
    sender.request_tcp_ack_turnover.insert(
        service,
        RequestTcpAckTurnoverModel {
            turnover_bytes: 512_000.0,
            sampled_at: now,
            sample_pto: Duration::from_secs(1),
        },
    );
    sender.request_tcp_ack_turnover.insert(
        candidate,
        RequestTcpAckTurnoverModel {
            turnover_bytes: 2_500_000.0,
            sampled_at: now,
            sample_pto: Duration::from_millis(100),
        },
    );

    let service_turnover =
        sender.request_tcp_owner_ack_turnover_bytes(&remotes, Some(service), now);
    assert!(service_turnover > 0);
    sender.request_ack_clock_proven_subflows.insert(candidate);
    assert_eq!(
        sender.request_tcp_owner_ack_turnover_bytes(&remotes, Some(service), now),
        service_turnover,
        "a retained calibration pipe is measurement, not shared-window authority"
    );
    sender
        .request_window_turnover_proven_subflows
        .insert(candidate);
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
        !sender.request_tcp_ack_turnover.contains_key(&candidate),
        "full exact-instance revocation must discard retained pipe authority"
    );
    assert!(
        !sender
            .request_window_turnover_proven_subflows
            .contains(&candidate)
    );
    assert!(
        !sender
            .request_ack_clock_proven_subflows
            .contains(&candidate),
        "full revocation must permit an exact instance to calibrate again"
    );

    sender.ordered_data_owner_instance = Some(RelayPathInstance {
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

    let mut sender = RelaySenderService::new(stream_id);
    sender.ordered_data_owner = Some(service_key);
    sender.ordered_data_owner_instance = Some(service);
    sender.request_rate_proven_subflows.insert(service);
    sender.request_rate_proven_subflows.insert(candidate);
    sender.request_graduated_subflows.insert(candidate);
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
        .request_ack_clock_first_window_subflows
        .insert(candidate);
    sender
        .request_rate_evidence
        .entry(candidate)
        .or_insert_with(|| RequestPathRateEvidence::new(Instant::now()))
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
    assert_eq!(sender.request_ack_clock_calibration_owner, None);
    assert!(sender.request_ack_clock_calibration_pending.is_none());
    assert!(sender.request_ack_clock_calibration_bytes.is_empty());
    assert!(sender.request_ack_clock_calibration_targets.is_empty());
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
        if sender.request_ack_clock_calibration_bytes[&candidate]
            >= sender.request_ack_clock_calibration_targets[&candidate]
        {
            break;
        }
    }
    assert!(
        sender.request_ack_clock_calibration_bytes[&candidate]
            >= sender.request_ack_clock_calibration_targets[&candidate]
    );
    assert_eq!(
        sender.request_ack_clock_calibration_owner,
        Some(RequestAckClockCalibrationOwner {
            candidate,
            target_bytes: sender.request_ack_clock_calibration_targets[&candidate],
        })
    );
    assert!(
        !sender
            .request_ack_clock_proven_subflows
            .contains(&candidate)
    );
    let final_ack_start = sent_calibration_frames.len().saturating_sub(2);
    for frame in &sent_calibration_frames[..final_ack_start] {
        ack_client_frame_for_test(&mut sender, &context, frame);
    }
    assert!(sender.revoke_request_tcp_capacity_calibration(candidate, true));
    assert_eq!(
        sender.request_ack_clock_calibration_owner,
        Some(RequestAckClockCalibrationOwner {
            candidate,
            target_bytes: sender.request_ack_clock_calibration_targets[&candidate],
        }),
        "natural carrier expiry must preserve the sealed AwaitingAck owner"
    );
    assert!(
        !sender
            .request_ack_clock_proven_subflows
            .contains(&candidate)
    );
    for frame in &sent_calibration_frames[final_ack_start..] {
        ack_client_frame_for_test(&mut sender, &context, frame);
    }
    assert!(
        sender
            .request_ack_clock_proven_subflows
            .contains(&candidate)
    );
    assert!(
        sender.request_tcp_ack_turnover.contains_key(&candidate),
        "the bounded calibration ACK retains its same-epoch pipe measurement"
    );
    assert!(
        !sender
            .request_window_turnover_proven_subflows
            .contains(&candidate),
        "the bounded calibration ACK cannot finance shared source-window growth"
    );
    assert_eq!(
        sender.request_tcp_owner_ack_turnover_bytes(&remotes, Some(service), Instant::now(),),
        0,
        "pending candidate evidence must remain invisible without an authorized Service pipe"
    );
    let pending_turnover = sender.request_tcp_ack_turnover[&candidate].turnover_bytes;
    let ordinary_offset = sender.request_ack_clock_calibration_bytes[&candidate];
    let ordinary = client_data_frame_for_test(stream_id, ordinary_offset, calibration_target);
    context.record_relay_path_send(
        candidate.key.underlay,
        candidate.key.index,
        calibration_target,
    );
    sender
        .flights
        .record_owner_frame_instance(candidate, &ordinary);
    tokio::time::sleep(Duration::from_millis(1)).await;
    ack_client_frame_for_test(&mut sender, &context, &ordinary);
    assert!(
        sender
            .request_window_turnover_proven_subflows
            .contains(&candidate),
        "one subsequent causal ordinary window grants exact-instance turnover authority"
    );
    assert_ne!(
        sender.request_tcp_ack_turnover[&candidate].turnover_bytes, pending_turnover,
        "the ordinary sample must update, not merely unlock, the pending calibration pipe"
    );
    assert!(
        sender.request_tcp_owner_ack_turnover_bytes(&remotes, Some(service), Instant::now(),) > 0
    );
    assert_eq!(sender.request_ack_clock_calibration_owner, None);
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
    assert!(
        sender.request_ack_clock_calibration_bytes[&candidate]
            >= sender.request_ack_clock_calibration_targets[&candidate],
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
    let mut sender = RelaySenderService::new(stream_id);
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
        .request_subflow_set
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
    assert_eq!(sender.ordered_data_owner, Some(service_key));
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
    let mut sender = RelaySenderService::new(stream_id);
    sender.ordered_data_owner = Some(service_key);
    sender.ordered_data_owner_instance = Some(service);
    sender.request_subflow_set = Some(epoch);
    sender.request_attempted_subflows.insert(replacement);
    let frame = client_data_frame_for_test(stream_id, 0, BBR_MAX_SEND_QUANTUM_BYTES);
    sender.flights.record_owner_frame_instance(stale, &frame);
    let owner_progress = sender.release_normalized_acked_ranges_with_owner_progress(
        &context,
        &[OffsetRange::new(0, BBR_MAX_SEND_QUANTUM_BYTES as u64).expect("ACK range")],
    );
    assert_eq!(owner_progress.len(), 1);
    assert_eq!(owner_progress[0].instance, stale);
    assert!(sender.request_rate_proven_subflows.contains(&stale));
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
            .request_subflow_set
            .as_ref()
            .and_then(FlowSubflowSet::startup_owner_key),
        Some(replacement),
        "logical-path evidence from an old attachment must not graduate the replacement"
    );
    assert!(!sender.request_graduated_subflows.contains(&replacement));
    assert!(
        !sender
            .request_startup_acked_bytes
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
    let mut sender = RelaySenderService::new(stream_id);
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
    assert!(sender.request_rate_proven_subflows.contains(&old_service));
    assert!(
        !sender
            .request_rate_proven_subflows
            .contains(&replacement_service)
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
            .request_rate_proven_subflows
            .contains(&replacement_service)
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
    let mut sender = RelaySenderService::new(stream_id);
    sender.ordered_data_owner = Some(service_key);
    sender.ordered_data_owner_instance = Some(service);
    sender.request_subflow_set = Some(epoch);
    sender.request_attempted_subflows.insert(candidate);
    let frame = client_data_frame_for_test(stream_id, 0, startup_limit);
    sender
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
    assert!(!sender.request_startup_acked_bytes.contains_key(&candidate));
    assert!(!sender.request_graduated_subflows.contains(&candidate));
    assert_eq!(
        sender
            .request_subflow_set
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
    let mut sender = RelaySenderService::new(stream_id);
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
    assert!(!context.request_quic_capacity_probe_proven(1, proof.token));
    assert!(
        sender
            .request_quic_capacity_calibration
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
    assert!(context.request_quic_capacity_probe_proven(1, proof.token));
    sender.reconcile_request_subflow_set(&context, &remotes);
    assert!(
        sender
            .request_graduated_subflows
            .contains(&candidate_instance)
    );
    assert!(
        sender
            .request_quic_capacity_calibration
            .as_ref()
            .is_some_and(|calibration| calibration.graduated)
    );
    assert_eq!(
        context.request_quic_capacity_product_handoff_state(1, proof.token),
        RequestQuicCapacityProductHandoffState::Pending
    );

    let ack_range = OffsetRange::new(0, proof.required_proof_bytes).expect("ACK range");
    let foreign_stream_id = StreamId(202);
    let foreign_frame =
        client_data_frame_for_test(foreign_stream_id, 0, proof.required_proof_bytes as usize);
    let mut foreign_sender = RelaySenderService::new(foreign_stream_id);
    foreign_sender
        .flights
        .record_owner_frame_instance(candidate_instance, &foreign_frame);
    foreign_sender.release_normalized_acked_ranges(&context, &[ack_range]);
    assert_eq!(
        context.request_quic_capacity_product_handoff_state(1, proof.token),
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
                reliable_quic_capacity_calibration_session_limit_bytes(context.mux_limits),
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
            reliable_quic_capacity_calibration_session_limit_bytes(context.mux_limits),
            Arc::new(RequestCapacityProbeCampaignBudget::default()),
            service_attached_at,
            Instant::now() + Duration::from_secs(1),
            Duration::from_secs(1),
            next_ticket,
        )
        .expect("completion releases session ownership without another owner send");
    assert_eq!(
        context.request_quic_capacity_product_handoff_state(1, proof.token),
        RequestQuicCapacityProductHandoffState::Complete
    );
    sender.reconcile_request_subflow_set(&context, &remotes);
    assert!(sender.request_quic_capacity_calibration.is_none());
    assert_eq!(
        context.request_quic_capacity_product_handoff_state(1, proof.token),
        RequestQuicCapacityProductHandoffState::Complete
    );
    assert!(
        sender
            .request_graduated_subflows
            .contains(&candidate_instance)
    );
    assert!(
        context
            .try_reserve_request_quic_capacity_probe(
                StreamId(205),
                service_instance.key.index,
                service_instance,
                9_002,
                PATH_OPEN_SCORE_BYTES as u64,
                reliable_quic_capacity_calibration_session_limit_bytes(context.mux_limits),
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
                reliable_quic_capacity_calibration_session_limit_bytes(context.mux_limits),
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
    let mut sender = RelaySenderService::new(stream_id);
    let proof = install_request_quic_capacity_calibration_for_test(
        &mut sender,
        &context,
        target,
        Instant::now() - Duration::from_millis(1),
        Instant::now() + Duration::from_secs(1),
        Duration::from_secs(1),
    );
    assert_eq!(
        context.request_quic_capacity_product_handoff_state(0, proof.token),
        RequestQuicCapacityProductHandoffState::Pending
    );

    sender.request_quic_capacity_calibration = None;
    assert_eq!(
        context.request_quic_capacity_product_handoff_state(0, proof.token),
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
                reliable_quic_capacity_calibration_session_limit_bytes(context.mux_limits),
                Arc::new(RequestCapacityProbeCampaignBudget::default()),
                Instant::now() - Duration::from_millis(1),
                Instant::now() + Duration::from_secs(1),
                Duration::from_secs(1),
                QuicCapacityProbeCommandTicket::new(),
            )
            .is_some()
    );
}

#[tokio::test]
async fn request_quic_single_path_skips_health_lock() {
    let stream_id = StreamId(208);
    let context =
        client_test_context_with_paths(&["udp://127.0.0.1:10328?srtt-ms=20&rate-mbps=500"]);
    let (service_commands, _service_rx) = reliable_path_command_channels(8);
    let remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream_with_underlay(
            stream_id,
            UnderlayProtocol::Udp,
            0,
            service_commands,
        ),
        8,
    );
    let service = remotes.active_path_instance().expect("Active Service");
    let mut sender = RelaySenderService::new(stream_id);
    sender.ordered_data_owner = Some(service.key);
    sender.ordered_data_owner_instance = Some(service);
    poison_client_path_health_for_test(&context);

    sender.try_start_request_quic_capacity_calibration(&context, &remotes, FlowLane::Throughput);

    assert!(sender.request_quic_capacity_calibration.is_none());
}

#[tokio::test]
#[cfg(target_os = "linux")]
async fn request_tcp_capacity_batches_only_policy_eligible_sockets() {
    let stream_id = StreamId(211);
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10330?srtt-ms=20&rate-mbps=500",
        "tcp://127.0.0.1:10331?srtt-ms=10&rate-mbps=500&expensive=true",
        "tcp://127.0.0.1:10332?srtt-ms=80&rate-mbps=500",
        "tcp://127.0.0.1:10333?srtt-ms=160&rate-mbps=500",
    ]);
    let (service_commands, _service_rx) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, service_commands), 8);
    let (forbidden_commands, mut forbidden_rx) = reliable_path_command_channels(8);
    let (first_commands, mut first_rx) = reliable_path_command_channels(8);
    let (second_commands, mut second_rx) = reliable_path_command_channels(8);
    remotes.attach_for_validation(opened_test_relay_stream(stream_id, 1, forbidden_commands));
    remotes.attach_for_validation(opened_test_relay_stream(stream_id, 2, first_commands));
    remotes.attach_for_validation(opened_test_relay_stream(stream_id, 3, second_commands));
    consume_client_validation_proof_for_test(&mut forbidden_rx);
    consume_client_validation_proof_for_test(&mut first_rx);
    consume_client_validation_proof_for_test(&mut second_rx);

    let service = remotes.active_path_instance().expect("Active Service");
    let instance = |index| {
        remotes
            .paths
            .iter()
            .find(|path| path.key().index == index)
            .expect("attached path")
            .instance()
    };
    let forbidden = instance(1);
    let first = instance(2);
    let second = instance(3);
    for candidate in [forbidden, first, second] {
        mark_client_validation_proof_fresh_for_test(
            &context,
            &remotes,
            candidate,
            Duration::from_millis(10),
        );
    }
    seed_client_bulk_evidence_for_test(&context, service.key);
    let registration = context.reliable_tcp_request_bulk_flow_registration();
    registration.update(true, Some(UnderlayProtocol::Tcp));
    let service_model = RequestPerFlowRateModel {
        rate_bps: 100_000_000.0,
        delivery_samples: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
    };
    let mut sender = RelaySenderService::new(stream_id);
    sender.ordered_data_owner = Some(service.key);
    sender.ordered_data_owner_instance = Some(service);
    sender
        .request_per_flow_rate_bps
        .insert(service, service_model);

    assert!(!context.relay_path_allows_automatic_bulk_use(forbidden.key));
    assert_eq!(
        context.automatic_bulk_path_count(UnderlayProtocol::Tcp, Some(service.key.index)),
        2,
        "the forbidden path must not dilute the train envelope"
    );
    let train_envelope = request_capacity_stable_candidate_share_bytes(context.mux_limits, 2);
    let expected_first = request_tcp_capacity_calibration_geometry(
        context
            .reliable_path_snapshot(first.key)
            .expect("first candidate snapshot"),
        service_model,
        context.mux_limits,
        train_envelope,
    )
    .expect("first train geometry");
    let expected_second = request_tcp_capacity_calibration_geometry(
        context
            .reliable_path_snapshot(second.key)
            .expect("second candidate snapshot"),
        service_model,
        context.mux_limits,
        train_envelope,
    )
    .expect("second train geometry");
    sender.try_start_request_tcp_capacity_calibration(&context, &remotes, FlowLane::Throughput);

    assert!(try_recv_reliable_path_command(&mut forbidden_rx).is_none());
    let first_probe = match try_recv_reliable_path_command(&mut first_rx) {
        Some(ReliablePathCommand::SendTcpCapacityProbe(probe)) => probe,
        _ => panic!("expected first TCP capacity probe"),
    };
    let second_probe = match try_recv_reliable_path_command(&mut second_rx) {
        Some(ReliablePathCommand::SendTcpCapacityProbe(probe)) => probe,
        _ => panic!("expected second TCP capacity probe"),
    };
    assert_eq!(first_probe.train_payload_bytes, expected_first.train_bytes);
    assert_eq!(
        second_probe.train_payload_bytes,
        expected_second.train_bytes
    );
    assert_ne!(first_probe.calibration_id, second_probe.calibration_id);
    assert!(first_probe.valid_request_tcp_train());
    assert!(second_probe.valid_request_tcp_train());
    assert!(matches!(
        first_probe.owner,
        TcpCapacityProbeOwner::Request { path_instance, .. } if path_instance == first
    ));
    assert!(matches!(
        second_probe.owner,
        TcpCapacityProbeOwner::Request { path_instance, .. } if path_instance == second
    ));
    assert_eq!(
        sender.request_tcp_capacity_attempted_paths,
        HashSet::from([first.key.index, second.key.index])
    );
    assert_eq!(sender.request_tcp_capacity_calibrations.len(), 2);
    assert!(
        sender
            .request_tcp_capacity_calibrations
            .contains_key(&first)
    );
    assert!(
        sender
            .request_tcp_capacity_calibrations
            .contains_key(&second)
    );
    assert_eq!(
        context.automatic_bulk_path_count(UnderlayProtocol::Tcp, Some(service.key.index)),
        2,
        "attempted paths must not collapse the configured budget denominator"
    );
}

#[tokio::test]
#[cfg(target_os = "linux")]
async fn request_tcp_capacity_flow_campaign_rejects_third_parallel_train() {
    let stream_id = StreamId(214);
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10343?srtt-ms=20&rate-mbps=500",
        "tcp://127.0.0.1:10344?srtt-ms=80&rate-mbps=500",
        "tcp://127.0.0.1:10345?srtt-ms=220&rate-mbps=500",
        "tcp://127.0.0.1:10346?srtt-ms=340&rate-mbps=500",
        "tcp://127.0.0.1:10347?srtt-ms=420&rate-mbps=500",
    ]);
    let (service_commands, _service_rx) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, service_commands), 8);
    let mut candidate_receivers = Vec::new();
    for index in 1..=4 {
        let (commands, mut receiver) = reliable_path_command_channels(8);
        remotes.attach_for_validation(opened_test_relay_stream(stream_id, index, commands));
        consume_client_validation_proof_for_test(&mut receiver);
        candidate_receivers.push(receiver);
    }

    let service = remotes.active_path_instance().expect("Active Service");
    let candidates = (1..=4)
        .map(|index| {
            remotes
                .paths
                .iter()
                .find(|path| path.key().index == index)
                .expect("attached candidate")
                .instance()
        })
        .collect::<Vec<_>>();
    for (candidate, srtt_ms) in candidates.iter().copied().zip([80_u64, 220, 340, 420]) {
        mark_client_validation_proof_fresh_for_test(
            &context,
            &remotes,
            candidate,
            Duration::from_millis(srtt_ms),
        );
    }
    seed_client_bulk_evidence_for_test(&context, service.key);
    let registration = context.reliable_tcp_request_bulk_flow_registration();
    registration.update(true, Some(UnderlayProtocol::Tcp));
    let service_model = RequestPerFlowRateModel {
        rate_bps: 100_000_000.0,
        delivery_samples: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
    };
    let mut sender = RelaySenderService::new(stream_id);
    sender.ordered_data_owner = Some(service.key);
    sender.ordered_data_owner_instance = Some(service);
    sender
        .request_per_flow_rate_bps
        .insert(service, service_model);

    let stable_share = request_capacity_stable_candidate_share_bytes(context.mux_limits, 4);
    let geometries = candidates
        .iter()
        .map(|candidate| {
            request_tcp_capacity_calibration_geometry(
                context
                    .reliable_path_snapshot(candidate.key)
                    .expect("candidate snapshot"),
                service_model,
                context.mux_limits,
                stable_share,
            )
            .expect("each train independently fits one candidate share")
        })
        .collect::<Vec<_>>();
    assert!(geometries[0].train_bytes + geometries[1].train_bytes <= stable_share);
    assert!(
        geometries[0].train_bytes + geometries[1].train_bytes + geometries[2].train_bytes
            > stable_share
    );

    sender.try_start_request_tcp_capacity_calibration(&context, &remotes, FlowLane::Throughput);

    for (index, receiver) in candidate_receivers.iter_mut().enumerate() {
        let command = try_recv_reliable_path_command(receiver);
        if index < 2 {
            let probe = match command {
                Some(ReliablePathCommand::SendTcpCapacityProbe(probe)) => probe,
                _ => panic!("expected campaign-admitted TCP capacity probe"),
            };
            assert_eq!(probe.train_payload_bytes, geometries[index].train_bytes);
        } else {
            assert!(
                command.is_none(),
                "the flow campaign must reject every train beyond its residual share"
            );
        }
    }
    assert_eq!(
        sender.request_tcp_capacity_attempted_paths,
        HashSet::from([candidates[0].key.index, candidates[1].key.index])
    );
    assert_eq!(sender.request_tcp_capacity_calibrations.len(), 2);
    assert_eq!(
        sender
            .request_tcp_capacity_campaign
            .remaining_bytes(stable_share),
        stable_share - geometries[0].train_bytes - geometries[1].train_bytes
    );
    assert_eq!(
        context.request_tcp_capacity_probe_remaining_bytes(),
        reliable_quic_capacity_calibration_session_limit_bytes(context.mux_limits)
            - geometries[0].train_bytes
            - geometries[1].train_bytes,
        "rejected flow work must preserve the session envelope for later streams"
    );
    let campaign_remaining = sender
        .request_tcp_capacity_campaign
        .remaining_bytes(stable_share);
    let session_remaining = context.request_tcp_capacity_probe_remaining_bytes();

    sender.try_start_request_tcp_capacity_calibration(&context, &remotes, FlowLane::Throughput);

    assert!(
        candidate_receivers
            .iter_mut()
            .all(|receiver| try_recv_reliable_path_command(receiver).is_none()),
        "repeated planning must not reopen rejected campaign work"
    );
    assert_eq!(
        sender
            .request_tcp_capacity_campaign
            .remaining_bytes(stable_share),
        campaign_remaining
    );
    assert_eq!(
        context.request_tcp_capacity_probe_remaining_bytes(),
        session_remaining
    );
    assert_eq!(
        sender.request_tcp_capacity_attempted_paths,
        HashSet::from([candidates[0].key.index, candidates[1].key.index]),
        "campaign rejection is not a path retirement decision"
    );
}

#[tokio::test]
#[cfg(target_os = "linux")]
async fn request_tcp_stable_share_rejects_oversized_train_without_retiring_candidate() {
    let stream_id = StreamId(212);
    let context = client_test_context_with_paths(&[
        "tcp://127.0.0.1:10334?srtt-ms=20&rate-mbps=500",
        "tcp://127.0.0.1:10335?srtt-ms=800&rate-mbps=500",
        "tcp://127.0.0.1:10336?srtt-ms=80&rate-mbps=500",
        "tcp://127.0.0.1:10337?srtt-ms=160&rate-mbps=500",
        "tcp://127.0.0.1:10338?srtt-ms=320&rate-mbps=500",
    ]);
    let (service_commands, _service_rx) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, service_commands), 8);
    let (candidate_commands, mut candidate_rx) = reliable_path_command_channels(8);
    remotes.attach_for_validation(opened_test_relay_stream(stream_id, 1, candidate_commands));
    consume_client_validation_proof_for_test(&mut candidate_rx);

    let service = remotes.active_path_instance().expect("Active Service");
    let candidate = remotes
        .paths
        .iter()
        .find(|path| path.key().index == 1)
        .expect("candidate path")
        .instance();
    mark_client_validation_proof_fresh_for_test(
        &context,
        &remotes,
        candidate,
        Duration::from_millis(800),
    );
    seed_client_bulk_evidence_for_test(&context, service.key);
    let registration = context.reliable_tcp_request_bulk_flow_registration();
    registration.update(true, Some(UnderlayProtocol::Tcp));
    let service_model = RequestPerFlowRateModel {
        rate_bps: 100_000_000.0,
        delivery_samples: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
    };
    let mut sender = RelaySenderService::new(stream_id);
    sender.ordered_data_owner = Some(service.key);
    sender.ordered_data_owner_instance = Some(service);
    sender
        .request_per_flow_rate_bps
        .insert(service, service_model);

    let full_session_geometry = request_tcp_capacity_calibration_geometry(
        context
            .reliable_path_snapshot(candidate.key)
            .expect("candidate snapshot"),
        service_model,
        context.mux_limits,
        reliable_quic_capacity_calibration_session_limit_bytes(context.mux_limits),
    )
    .expect("candidate train fits the session envelope");
    let eligible_candidates =
        context.automatic_bulk_path_count(UnderlayProtocol::Tcp, Some(service.key.index));
    assert_eq!(eligible_candidates, 4);
    let stable_share =
        request_capacity_stable_candidate_share_bytes(context.mux_limits, eligible_candidates);
    assert!(full_session_geometry.train_bytes > stable_share);
    assert!(
        request_tcp_capacity_calibration_geometry(
            context
                .reliable_path_snapshot(candidate.key)
                .expect("candidate snapshot"),
            service_model,
            context.mux_limits,
            stable_share,
        )
        .is_none(),
        "a late path must not inherit unused shares from earlier candidates"
    );
    let session_remaining_before = context.request_tcp_capacity_probe_remaining_bytes();

    sender.try_start_request_tcp_capacity_calibration(&context, &remotes, FlowLane::Throughput);

    assert!(try_recv_reliable_path_command(&mut candidate_rx).is_none());
    assert!(sender.request_tcp_capacity_attempted_paths.is_empty());
    assert!(sender.request_tcp_capacity_calibrations.is_empty());
    assert_eq!(
        context.request_tcp_capacity_probe_remaining_bytes(),
        session_remaining_before,
        "an oversized fixed-share train must not consume the session budget"
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
async fn request_quic_capacity_skips_an_earlier_exhausted_path_share() {
    let stream_id = StreamId(213);
    let context = client_test_context_with_paths(&[
        "udp://127.0.0.1:10339?srtt-ms=20&rate-mbps=500",
        "udp://127.0.0.1:10340?srtt-ms=40&rate-mbps=500",
        "udp://127.0.0.1:10341?srtt-ms=80&rate-mbps=500",
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
    let (first_commands, mut first_rx) = reliable_path_command_channels(8);
    let (second_commands, mut second_rx) = reliable_path_command_channels(8);
    remotes.attach_for_validation(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        1,
        first_commands,
    ));
    remotes.attach_for_validation(opened_test_relay_stream_with_underlay(
        stream_id,
        UnderlayProtocol::Udp,
        2,
        second_commands,
    ));
    consume_client_validation_proof_for_test(&mut first_rx);
    consume_client_validation_proof_for_test(&mut second_rx);

    let service = remotes.active_path_instance().expect("Active Service");
    let candidate = |index| {
        remotes
            .paths
            .iter()
            .find(|path| path.key().index == index)
            .expect("Validation candidate")
    };
    let first = candidate(1).instance();
    let second = candidate(2).instance();
    for instance in [first, second] {
        mark_client_validation_proof_fresh_for_test(
            &context,
            &remotes,
            instance,
            Duration::from_millis(10),
        );
    }
    seed_client_bulk_evidence_for_test(&context, service.key);
    context.health.lock().expect("path health lock").udp[service.key.index].relay_bytes_in_flight =
        reliable_subflow_startup_sample_limit_bytes(context.mux_limits);

    let eligible_candidates =
        context.automatic_bulk_path_count(UnderlayProtocol::Udp, Some(service.key.index));
    assert_eq!(eligible_candidates, 2);
    let stable_share =
        request_capacity_stable_candidate_share_bytes(context.mux_limits, eligible_candidates);
    let now = Instant::now();
    let mut spent = context
        .try_reserve_request_quic_capacity_probe(
            StreamId(212),
            first.key.index,
            first,
            9_100,
            stable_share,
            stable_share,
            Arc::new(RequestCapacityProbeCampaignBudget::default()),
            candidate(1).attached_at,
            now + Duration::from_secs(1),
            Duration::from_secs(1),
            QuicCapacityProbeCommandTicket::new(),
        )
        .expect("reserve the earlier path's complete fixed share");
    spent.commit();
    drop(spent);
    assert_eq!(
        context.request_quic_capacity_probe_path_remaining_bytes(first.key.index, stable_share,),
        0
    );

    let mut sender = RelaySenderService::new(stream_id);
    sender.ordered_data_owner = Some(service.key);
    sender.ordered_data_owner_instance = Some(service);
    sender.try_start_request_quic_capacity_calibration(&context, &remotes, FlowLane::Throughput);

    assert!(try_recv_reliable_path_command(&mut first_rx).is_none());
    let probe = match try_recv_reliable_path_command(&mut second_rx) {
        Some(ReliablePathCommand::SendQuicCapacityProbe(probe)) => probe,
        _ => panic!("the first viable later candidate must receive the probe"),
    };
    assert!(matches!(
        probe.owner,
        QuicCapacityProbeOwner::Request { path_instance, .. } if path_instance == second
    ));
}

#[tokio::test]
async fn request_quic_train_waits_for_candidate_latency_pressure_to_clear() {
    let stream_id = StreamId(210);
    let context = client_test_context_with_paths(&[
        "udp://127.0.0.1:10326?srtt-ms=20&rate-mbps=500",
        "udp://127.0.0.1:10327?srtt-ms=10&rate-mbps=500",
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
    let service = remotes.active_path_instance().expect("Active Service");
    let candidate = remotes
        .paths
        .iter()
        .find(|path| path.placement == RelayPathPlacement::Validation)
        .expect("Validation candidate")
        .instance();
    mark_client_validation_proof_fresh_for_test(
        &context,
        &remotes,
        candidate,
        Duration::from_millis(10),
    );
    seed_client_bulk_evidence_for_test(&context, service.key);
    context.health.lock().expect("path health lock").udp[service.key.index].relay_bytes_in_flight =
        reliable_subflow_startup_sample_limit_bytes(context.mux_limits);
    context.health.lock().expect("path health lock").udp[candidate.key.index]
        .reserve_load(FlowLane::Latency);

    let mut sender = RelaySenderService::new(stream_id);
    sender.ordered_data_owner = Some(service.key);
    sender.ordered_data_owner_instance = Some(service);
    sender.try_start_request_quic_capacity_calibration(&context, &remotes, FlowLane::Throughput);
    assert!(sender.request_quic_capacity_calibration.is_none());
    assert!(sender.request_quic_capacity_attempted_paths.is_empty());

    context.health.lock().expect("path health lock").udp[candidate.key.index]
        .release_load(FlowLane::Latency);
    sender.try_start_request_quic_capacity_calibration(&context, &remotes, FlowLane::Throughput);
    assert!(sender.request_quic_capacity_calibration.is_some());
    assert!(matches!(
        try_recv_reliable_path_command(&mut candidate_rx),
        Some(ReliablePathCommand::SendQuicCapacityProbe(_))
    ));
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
    let mut sender = RelaySenderService::new(stream_id);
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
            .request_graduated_subflows
            .contains(&candidate_instance)
    );
    let _ = context.health.lock().expect("path health lock").udp[1].observe(proof.expires_at);
    let next_lease = context
        .try_reserve_request_quic_capacity_probe(
            StreamId(208),
            service_instance.key.index,
            service_instance,
            9_004,
            PATH_OPEN_SCORE_BYTES as u64,
            reliable_quic_capacity_calibration_session_limit_bytes(context.mux_limits),
            Arc::new(RequestCapacityProbeCampaignBudget::default()),
            service_attached_at,
            Instant::now() + Duration::from_secs(1),
            Duration::from_secs(1),
            QuicCapacityProbeCommandTicket::new(),
        )
        .expect("an expired idle handoff is reclaimed by the next reservation");
    sender.reconcile_request_subflow_set(&context, &remotes);

    assert!(sender.request_quic_capacity_calibration.is_none());
    assert!(
        !sender
            .request_graduated_subflows
            .contains(&candidate_instance)
    );
    assert_eq!(
        context.request_quic_capacity_product_handoff_state(1, proof.token),
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
                reliable_quic_capacity_calibration_session_limit_bytes(context.mux_limits),
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
                reliable_quic_capacity_calibration_session_limit_bytes(context.mux_limits),
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
    let mut sender = RelaySenderService::new(stream_id);
    sender.ordered_data_owner = Some(service_key);
    sender.ordered_data_owner_instance = Some(service);
    sender.request_subflow_set = Some(epoch);
    sender.request_attempted_subflows.insert(candidate);
    let receipt_proof_id = 991;
    sender
        .request_startup_receipt_proofs
        .insert(candidate, (receipt_proof_id, 0));
    sender
        .request_startup_first_sent_at
        .insert(candidate, Instant::now());

    let frame = client_data_frame_for_test(stream_id, 0, startup_limit);
    sender
        .flights
        .record_owner_frame_instance(candidate, &frame);
    sender.flights.record_repair_frame_instance(service, &frame);
    sender.release_normalized_acked_ranges(
        &context,
        &[OffsetRange::new(0, startup_limit as u64).expect("ACK range")],
    );
    assert!(!sender.request_startup_acked_bytes.contains_key(&candidate));

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

    assert!(!sender.request_graduated_subflows.contains(&candidate));
    assert_eq!(
        sender
            .request_subflow_set
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
    context.health.lock().expect("path health lock").udp[service_key.index].mark_quic_path_metrics(
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
    let mut sender = RelaySenderService::new(stream_id);
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
    assert!(sender.request_rate_proven_subflows.contains(&service));
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
    assert!(!sender.request_attempted_subflows.contains(&candidate));
    assert!(!sender.request_graduated_subflows.contains(&candidate));
    assert_eq!(
        sender
            .request_subflow_set
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

    let mut sender = RelaySenderService::new(stream_id);
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
        !sender.request_graduated_subflows.contains(&candidate),
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
    assert!(sender.request_graduated_subflows.contains(&candidate));
    assert!(matches!(
        try_recv_reliable_path_command(&mut candidate_rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
    assert!(sender.request_subflow_set.is_none());
    assert!(sender.request_startup_receipt_proofs.is_empty());
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
    let mut sender = RelaySenderService::new(stream_id);
    let service = remotes
        .path_instance_for_key(service_key)
        .expect("Service instance");
    sender.ordered_data_owner = Some(service_key);
    sender.ordered_data_owner_instance = Some(service);
    sender.request_rate_proven_subflows.insert(service);

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
    assert_eq!(sender.ordered_data_owner, Some(service_key));
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
        context.health.lock().expect("path health lock").tcp[1].active_flows,
        1,
        "concurrent flows must see that this Subflow already consumes carrier capacity"
    );

    drop(remotes);
    assert_eq!(
        context.health.lock().expect("path health lock").tcp[1].active_flows,
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
        context.health.lock().expect("path health lock").tcp[0].active_flows,
        1
    );

    drop(first);
    assert_eq!(
        context.health.lock().expect("path health lock").tcp[0].active_flows,
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

    context.health.lock().expect("path health lock").tcp[0].invalidate_path_proofs();
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
    let mut sender = RelaySenderService::new(stream_id);
    sender.request_subflow_set = Some(epoch);
    sender.try_enqueue_request_startup_receipt_proof(&context, &remotes, candidate);
    let (old_proof_id, old_generation) = sender.request_startup_receipt_proofs[&candidate];
    assert_eq!(old_generation, 0);
    assert!(matches!(
        try_recv_reliable_path_command(&mut candidate_rx),
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData {
            proof_id,
            ..
        })) if proof_id == old_proof_id
    ));
    let stale_sent_at = Instant::now();

    context.health.lock().expect("path health lock").tcp[1].invalidate_path_proofs();
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
    let (new_proof_id, new_generation) = sender.request_startup_receipt_proofs[&candidate];
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
    let mut sender = RelaySenderService::new(StreamId(107));
    sender.request_attempted_subflows.insert(attempted);
    sender.request_attempted_subflows.insert(graduated);
    sender.request_graduated_subflows.insert(graduated);
    sender.request_subflow_set = Some(FlowSubflowSet::new(
        0,
        service,
        256 * 1024,
        0,
        Duration::ZERO,
    ));
    sender.request_ack_clock_calibration_pending = Some(RequestAckClockCalibrationPending {
        service,
        candidate: graduated,
    });

    sender.reset_request_subflow_epoch();

    assert!(sender.request_subflow_set.is_none());
    assert!(sender.request_attempted_subflows.contains(&attempted));
    assert!(sender.request_attempted_subflows.contains(&graduated));
    assert!(sender.request_graduated_subflows.contains(&graduated));
    assert!(sender.request_ack_clock_calibration_pending.is_none());
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
    let mut sender = RelaySenderService::new(StreamId(109));
    sender.ordered_data_owner_instance = Some(service);
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
        sender.request_ack_clock_calibration_pending,
        Some(RequestAckClockCalibrationPending { service, candidate })
    );
    assert_eq!(sender.request_ack_clock_calibration_owner, None);
    assert!(sender.request_ack_clock_calibration_bytes.is_empty());
    assert!(sender.request_ack_clock_calibration_targets.is_empty());

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
    assert_eq!(sender.request_ack_clock_calibration_pending, None);
    assert_eq!(
        sender.request_ack_clock_calibration_bytes[&candidate],
        64 * 1024
    );
    assert_eq!(
        sender.request_ack_clock_calibration_targets[&candidate],
        2 * 1024 * 1024
    );
    assert_eq!(
        sender.request_ack_clock_calibration_owner,
        Some(RequestAckClockCalibrationOwner {
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
        sender.request_ack_clock_calibration_bytes[&candidate],
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
        let mut sender = RelaySenderService::new(StreamId(110));
        sender.request_ack_clock_calibration_owner = Some(RequestAckClockCalibrationOwner {
            candidate,
            target_bytes,
        });
        sender
            .request_ack_clock_calibration_bytes
            .insert(candidate, spent_bytes);
        sender
            .request_ack_clock_calibration_targets
            .insert(candidate, target_bytes);
        sender
            .request_tcp_capacity_proven_subflows
            .insert(candidate);
        sender.request_graduated_subflows.insert(candidate);
        sender.request_rate_proven_subflows.insert(candidate);
        sender
            .request_rate_evidence
            .insert(candidate, RequestPathRateEvidence::new(Instant::now()));
        sender
    };

    let mut sealed = seed_owner(target_bytes);
    assert!(sealed.revoke_request_tcp_capacity_calibration(candidate, true));
    assert!(
        !sealed
            .request_tcp_capacity_proven_subflows
            .contains(&candidate)
    );
    assert!(sealed.request_graduated_subflows.contains(&candidate));
    assert!(sealed.request_rate_proven_subflows.contains(&candidate));
    assert!(sealed.request_rate_evidence.contains_key(&candidate));
    assert_eq!(
        sealed.request_ack_clock_calibration_owner,
        Some(RequestAckClockCalibrationOwner {
            candidate,
            target_bytes,
        })
    );
    assert_eq!(
        sealed.request_ack_clock_calibration_bytes[&candidate],
        target_bytes
    );

    let mut partial = seed_owner(target_bytes - 64 * 1024);
    assert!(!partial.revoke_request_tcp_capacity_calibration(candidate, true));
    assert!(partial.request_ack_clock_calibration_owner.is_none());
    assert!(!partial.request_graduated_subflows.contains(&candidate));
    assert!(!partial.request_rate_evidence.contains_key(&candidate));
    assert!(
        !partial
            .request_ack_clock_calibration_bytes
            .contains_key(&candidate)
    );

    let mut pending = RelaySenderService::new(StreamId(111));
    pending.request_ack_clock_calibration_pending =
        Some(RequestAckClockCalibrationPending { service, candidate });
    pending
        .request_tcp_capacity_proven_subflows
        .insert(candidate);
    pending.request_graduated_subflows.insert(candidate);
    assert!(!pending.revoke_request_tcp_capacity_calibration(candidate, true));
    assert!(pending.request_ack_clock_calibration_pending.is_none());
    assert!(!pending.request_graduated_subflows.contains(&candidate));

    let mut detached = seed_owner(target_bytes);
    assert!(!detached.revoke_request_tcp_capacity_calibration(candidate, false));
    assert!(detached.request_ack_clock_calibration_owner.is_none());
    assert!(!detached.request_graduated_subflows.contains(&candidate));
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
    let mut sender = RelaySenderService::new(stream_id);
    sender.ordered_data_owner = Some(service.key);
    sender.ordered_data_owner_instance = Some(service);
    sender.request_subflow_set = Some(epoch);
    sender.request_attempted_subflows.insert(candidate);
    sender.request_ack_clock_calibration_owner = Some(RequestAckClockCalibrationOwner {
        candidate,
        target_bytes: 2 * 1024 * 1024,
    });
    sender
        .request_ack_clock_calibration_bytes
        .insert(candidate, 64 * 1024);
    sender
        .request_ack_clock_calibration_targets
        .insert(candidate, 2 * 1024 * 1024);
    remotes
        .paths
        .iter_mut()
        .find(|path| path.instance() == candidate)
        .expect("candidate path")
        .placement = RelayPathPlacement::Active;

    sender.reconcile_request_subflow_set(&context, &remotes);

    assert!(sender.request_subflow_set.is_none());
    assert_eq!(
        sender.request_ack_clock_calibration_owner, None,
        "an optional calibration epoch cannot survive promotion away from Validation"
    );
    assert!(sender.request_ack_clock_calibration_bytes.is_empty());
    assert!(sender.request_ack_clock_calibration_targets.is_empty());
    assert!(
        !sender.request_graduated_subflows.contains(&candidate),
        "real placement loss must fully abort any preserved AwaitingAck state"
    );
    sender.request_ack_clock_calibration_pending =
        Some(RequestAckClockCalibrationPending { service, candidate });
    sender.reconcile_request_subflow_set(&context, &remotes);
    assert_eq!(
        sender.request_ack_clock_calibration_pending, None,
        "pending exact-instance entry cannot survive promotion away from Validation"
    );
    assert!(
        sender.request_attempted_subflows.contains(&candidate),
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

    let mut sender = RelaySenderService::new(stream_id);
    sender.ordered_data_owner = Some(service.key);
    sender.ordered_data_owner_instance = Some(service);
    sender
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
fn response_repair_extra_budget_is_cumulative_not_per_event() {
    let mux_limits = MuxLimits::default();
    let stream_id = StreamId(91);
    let mut sender = ServerResponseSenderService::new_with_performance(
        SessionId(91),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 1,
        },
    );
    let startup_floor = response_extra_traffic_startup_floor_bytes(mux_limits);
    let repair_payload = Bytes::from(vec![0x55; startup_floor]);

    assert_eq!(
        sender.repair_extra_budget_remaining(mux_limits),
        startup_floor
    );
    assert!(
        sender
            .enqueue_repair_frame_with_priority(
                Frame::StreamData {
                    stream_id,
                    offset: 0,
                    flags: StreamFlags::NONE,
                    payload: repair_payload.clone(),
                },
                mux_limits,
                false,
            )
            .is_some(),
        "startup repair floor should be spendable once"
    );
    assert_eq!(sender.repair_extra_budget_remaining(mux_limits), 0);
    assert!(
        sender
            .enqueue_repair_frame_with_priority(
                Frame::StreamData {
                    stream_id,
                    offset: startup_floor as u64,
                    flags: StreamFlags::NONE,
                    payload: repair_payload.clone(),
                },
                mux_limits,
                false,
            )
            .is_none(),
        "repair budget must be cumulative, not refreshed for every tail/ACK event"
    );

    let earned_data_bytes = startup_floor.saturating_mul(100);
    sender.record_owner_progress_for_test(earned_data_bytes);

    assert!(
        sender.repair_extra_budget_remaining(mux_limits) >= startup_floor,
        "ACK-released owner progress earns more bounded extra repair budget"
    );
    assert!(
        sender
            .enqueue_repair_frame_with_priority(
                Frame::StreamData {
                    stream_id,
                    offset: (startup_floor * 2) as u64,
                    flags: StreamFlags::NONE,
                    payload: repair_payload,
                },
                mux_limits,
                false,
            )
            .is_some()
    );
}

#[test]
fn response_source_read_budget_is_separate_from_repair_cache_retention() {
    let stream_id = StreamId(93);
    let mux_limits = MuxLimits {
        max_repair_bytes: 4096,
        max_payload_bytes: 4096,
        max_stream_window_bytes: 64 * 1024,
        max_path_flight_bytes: 4096,
        ..MuxLimits::default()
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, mux_limits, u64::MAX);
    send_stream
        .send_data(
            Bytes::from(vec![0x5a; mux_limits.max_repair_bytes]),
            StreamFlags::NONE,
        )
        .expect("seed retained unacked OwnerData");
    assert_eq!(send_stream.repair_bytes(), mux_limits.max_repair_bytes);

    let sender_queue = ReliableRelaySenderQueue::default();
    assert!(
        reliable_relay_can_read_into_sender_queue(
            &send_stream,
            &sender_queue,
            mux_limits,
            mux_limits.max_repair_bytes,
        ),
        "repair cache retention is unacked OwnerData memory, not already-queued source bytes"
    );
    assert_eq!(
        reliable_relay_sender_queue_read_budget(
            &send_stream,
            &sender_queue,
            mux_limits,
            mux_limits.max_repair_bytes,
            mux_limits.max_repair_bytes,
        ),
        mux_limits.max_repair_bytes,
        "bounded product-source reads may continue while dispatch waits for repair-cache ACK release"
    );
}

#[test]
fn mixed_response_dispatch_payload_is_bounded_by_remaining_repair_capacity() {
    let stream_id = StreamId(98);
    let mux_limits = MuxLimits {
        max_payload_bytes: 4096,
        max_repair_bytes: 4096,
        max_path_flight_bytes: 4096,
        max_reliable_relay_chunk_bytes: 4096,
        ..MuxLimits::default()
    };
    let (commands, _active_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits(
        SessionId(98),
        UnderlayProtocol::Tcp,
        PathId(0),
        commands,
        FlowLane::Throughput,
        mux_limits,
    );
    let (validation_commands, _validation_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            UnderlayProtocol::Udp,
            PathId(1),
            validation_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            4096,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: 4096,
        output: ReliablePathStreamOutput::Switchable(binding),
        frames: frame_rx,
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, mux_limits, u64::MAX);
    send_stream
        .send_data(Bytes::from(vec![0x5a; 3072]), StreamFlags::NONE)
        .expect("seed retained OwnerData");

    assert_eq!(
        response_dispatch_payload_bytes(
            &path_stream,
            &send_stream,
            FlowLane::Throughput,
            mux_limits,
            4096,
        ),
        Some(1024),
    );
    send_stream
        .send_data(Bytes::from(vec![0x5a; 1024]), StreamFlags::NONE)
        .expect("fill repair cache");
    assert_eq!(
        response_dispatch_payload_bytes(
            &path_stream,
            &send_stream,
            FlowLane::Throughput,
            mux_limits,
            4096,
        ),
        None,
    );
}

#[test]
fn coupled_response_dispatch_keeps_the_authoritative_send_stream_check() {
    let stream_id = StreamId(97);
    let mux_limits = MuxLimits {
        max_payload_bytes: 4096,
        max_repair_bytes: 4096,
        max_path_flight_bytes: 4096,
        max_reliable_relay_chunk_bytes: 4096,
        ..MuxLimits::default()
    };
    let (commands, _receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits(
        SessionId(97),
        UnderlayProtocol::Tcp,
        PathId(0),
        commands,
        FlowLane::Throughput,
        mux_limits,
    );
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: 4096,
        output: ReliablePathStreamOutput::Switchable(binding),
        frames: frame_rx,
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, mux_limits, u64::MAX);
    send_stream
        .send_data(Bytes::from(vec![0x5a; 4096]), StreamFlags::NONE)
        .expect("fill repair cache");

    assert_eq!(
        response_dispatch_payload_bytes(
            &path_stream,
            &send_stream,
            FlowLane::Throughput,
            mux_limits,
            4096,
        ),
        Some(4096),
        "coupled paths retain the existing send-stream repair-capacity boundary"
    );
}

#[tokio::test]
async fn formerly_mixed_response_retains_repair_preflight_after_family_detach() {
    let stream_id = StreamId(96);
    let mux_limits = MuxLimits {
        max_payload_bytes: 4096,
        max_repair_bytes: 4096,
        max_path_flight_bytes: 4096,
        max_reliable_relay_chunk_bytes: 4096,
        ..MuxLimits::default()
    };
    let (commands, _active_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits(
        SessionId(96),
        UnderlayProtocol::Tcp,
        PathId(0),
        commands,
        FlowLane::Throughput,
        mux_limits,
    );
    let udp_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (udp_commands, _udp_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            udp_key.underlay,
            udp_key.path_id,
            udp_commands.clone(),
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            4096,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    assert!(binding.has_live_mixed_owner_underlays());
    binding.detach(udp_key, &udp_commands);
    assert!(!binding.has_live_mixed_owner_underlays());
    assert!(binding.may_have_mixed_owner_underlays());

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: 4096,
        output: ReliablePathStreamOutput::Switchable(binding),
        frames: frame_rx,
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, mux_limits, u64::MAX);
    send_stream
        .send_data(Bytes::from(vec![0x5a; 3072]), StreamFlags::NONE)
        .expect("seed retained OwnerData");
    let mut sender = ServerResponseSenderService::new(SessionId(96), stream_id);
    sender.enqueue_data_for_lane(Bytes::from(vec![0x33; 4096]), FlowLane::Throughput);

    let first = sender
        .dispatch_next(
            &path_stream,
            &mut send_stream,
            FlowLane::Throughput,
            mux_limits,
        )
        .await
        .expect("formerly mixed raw bytes dispatch within remaining repair capacity");
    assert_eq!(first.payload_bytes, 1024);
    assert_eq!(send_stream.repair_bytes(), 4096);
    assert_eq!(sender.data_bytes(), 3072);
    assert!(matches!(
        sender
            .dispatch_next(
                &path_stream,
                &mut send_stream,
                FlowLane::Throughput,
                mux_limits,
            )
            .await,
        Err(RuntimeError::SenderServiceBlocked)
    ));
    assert_eq!(sender.data_bytes(), 3072);
}

#[tokio::test]
async fn mixed_response_dispatch_waits_retryably_when_repair_cache_is_full() {
    let stream_id = StreamId(99);
    let mux_limits = MuxLimits {
        max_payload_bytes: 4096,
        max_repair_bytes: 4096,
        max_path_flight_bytes: 4096,
        max_reliable_relay_chunk_bytes: 4096,
        ..MuxLimits::default()
    };
    let (commands, _active_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits(
        SessionId(99),
        UnderlayProtocol::Tcp,
        PathId(0),
        commands,
        FlowLane::Throughput,
        mux_limits,
    );
    let (validation_commands, _validation_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            UnderlayProtocol::Udp,
            PathId(1),
            validation_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            4096,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: 4096,
        output: ReliablePathStreamOutput::Switchable(binding),
        frames: frame_rx,
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, mux_limits, u64::MAX);
    send_stream
        .send_data(Bytes::from(vec![0x5a; 4096]), StreamFlags::NONE)
        .expect("fill repair cache");
    let blocked_offset = send_stream.next_offset();
    let mut sender = ServerResponseSenderService::new(SessionId(99), stream_id);
    sender.enqueue_data_for_lane(Bytes::from_static(b"next"), FlowLane::Throughput);

    assert!(matches!(
        sender
            .dispatch_next(
                &path_stream,
                &mut send_stream,
                FlowLane::Throughput,
                mux_limits,
            )
            .await,
        Err(RuntimeError::SenderServiceBlocked)
    ));
    assert_eq!(send_stream.next_offset(), blocked_offset);
    assert_eq!(sender.data_bytes(), 4, "blocked raw bytes remain queued");

    send_stream.apply_ack(&[OffsetRange {
        start: 0,
        end: blocked_offset,
    }]);
    sender
        .dispatch_next(
            &path_stream,
            &mut send_stream,
            FlowLane::Throughput,
            mux_limits,
        )
        .await
        .expect("ACK release restores dispatch capacity");
    assert_eq!(sender.data_bytes(), 0);
}

#[test]
fn response_repair_extra_budget_accumulates_until_useful_attempt() {
    let mux_limits = MuxLimits::default();
    let stream_id = StreamId(92);
    let mut sender = ServerResponseSenderService::new_with_performance(
        SessionId(92),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 1,
        },
    );
    let startup_floor = response_extra_traffic_startup_floor_bytes(mux_limits);
    let min_attempt = response_repair_minimum_useful_attempt_bytes(mux_limits);

    assert!(sender.repair_extra_event_budget_remaining(mux_limits) >= min_attempt);
    assert!(
        sender
            .enqueue_repair_frame_with_priority(
                Frame::StreamData {
                    stream_id,
                    offset: 0,
                    flags: StreamFlags::NONE,
                    payload: Bytes::from(vec![0x44; startup_floor]),
                },
                mux_limits,
                false,
            )
            .is_some()
    );

    sender.record_owner_progress_for_test(startup_floor);
    assert!(
        sender.repair_extra_budget_remaining(mux_limits) > 0,
        "ACK-released owner progress earns fractional repair budget"
    );
    assert_eq!(
        sender.repair_extra_event_budget_remaining(mux_limits),
        0,
        "tiny earned repair crumbs should accumulate instead of emitting high-overhead repair frames"
    );

    sender.record_owner_progress_for_test(min_attempt.saturating_mul(100));
    assert!(
        sender.repair_extra_event_budget_remaining(mux_limits) >= min_attempt,
        "once enough owner bytes make ACK progress, repair can spend a useful attempt"
    );
}

#[tokio::test]
async fn response_owner_dispatch_does_not_earn_repair_budget_before_ack_progress() {
    let mux_limits = MuxLimits::default();
    let stream_id = StreamId(96);
    let startup_floor = response_extra_traffic_startup_floor_bytes(mux_limits);
    let (commands, _receivers) = reliable_path_command_channels(8);
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: reliable_relay_buffer_len(mux_limits),
        output: ReliablePathStreamOutput::fixed(
            UnderlayProtocol::Tcp,
            PathId(0),
            commands,
            mux_limits,
        ),
        frames: frame_rx,
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, mux_limits, u64::MAX);
    let mut sender = ServerResponseSenderService::new_with_performance(
        SessionId(96),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 1,
        },
    );

    sender
        .extra_traffic
        .record_optional(ExtraTrafficKind::Repair, startup_floor);
    assert_eq!(sender.repair_extra_budget_remaining(mux_limits), 0);

    sender.enqueue_data_for_lane(
        Bytes::from(vec![0x96; startup_floor.saturating_mul(100)]),
        FlowLane::Throughput,
    );
    sender
        .dispatch_next(
            &path_stream,
            &mut send_stream,
            FlowLane::Throughput,
            mux_limits,
        )
        .await
        .expect("owner dispatch should not be blocked by exhausted repair budget");

    assert_eq!(
        sender.repair_extra_budget_remaining(mux_limits),
        0,
        "emitted OwnerData must not earn optional repair budget until ordered ACK progress releases it"
    );
}

#[cfg(feature = "lab-diagnostics")]
#[tokio::test]
async fn fixed_output_owner_data_records_sender_service_decision_for_conformance() {
    let _guard = lab_diag_test_guard();
    let mux_limits = MuxLimits::default();
    let session_id = SessionId(97);
    let stream_id = StreamId(97);
    let (commands, _receivers) = reliable_path_command_channels(8);
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: reliable_relay_buffer_len(mux_limits),
        output: ReliablePathStreamOutput::fixed(
            UnderlayProtocol::Tcp,
            PathId(0),
            commands,
            mux_limits,
        ),
        frames: frame_rx,
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, mux_limits, u64::MAX);
    let mut sender = ServerResponseSenderService::new(session_id, stream_id);

    sender.enqueue_data_for_lane(Bytes::from_static(b"owner"), FlowLane::Throughput);
    sender
        .dispatch_next(
            &path_stream,
            &mut send_stream,
            FlowLane::Throughput,
            mux_limits,
        )
        .await
        .expect("fixed output OwnerData dispatch should succeed");

    assert_eq!(
        lab_sender_service_counts_for_test(session_id.0, stream_id.0),
        (1, 1),
        "fixed output OwnerData must be accounted as a sender-service decision"
    );
    lab_assert_server_sender_service_balanced(session_id.0, stream_id.0);
}

#[test]
fn response_critical_repair_closes_tail_after_optional_budget_exhaustion() {
    let mux_limits = MuxLimits::default();
    let stream_id = StreamId(94);
    let mut sender = ServerResponseSenderService::new_with_performance(
        SessionId(94),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 1,
        },
    );
    let startup_floor = response_extra_traffic_startup_floor_bytes(mux_limits);
    let frame = Frame::StreamData {
        stream_id,
        offset: 0,
        flags: StreamFlags::NONE,
        payload: Bytes::from(vec![0x44; startup_floor]),
    };
    assert!(
        sender
            .enqueue_repair_frame_with_priority(frame, mux_limits, false)
            .is_some()
    );

    let closure_frame = Frame::StreamData {
        stream_id,
        offset: startup_floor as u64,
        flags: StreamFlags::NONE,
        payload: Bytes::from_static(b"tail"),
    };
    assert!(
        sender
            .enqueue_repair_frame_with_priority(closure_frame.clone(), mux_limits, false)
            .is_none(),
        "ordinary optional repair budget should be exhausted"
    );

    sender.enqueue_critical_repair_frame(closure_frame);
    assert_eq!(sender.repair_extra_budget_remaining(mux_limits), 0);
}

#[test]
fn response_critical_tail_repair_is_idempotent_while_range_is_queued() {
    let mux_limits = MuxLimits::default();
    let stream_id = StreamId(96);
    let mut sender = ServerResponseSenderService::new_with_performance(
        SessionId(96),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 1,
        },
    );
    let first = Frame::StreamData {
        stream_id,
        offset: 128,
        flags: StreamFlags::NONE,
        payload: Bytes::from_static(&[0x44; 64]),
    };
    let duplicate = first.clone();

    assert!(sender.enqueue_critical_tail_repair_frame(first).is_some());
    let bytes_after_first = sender.bytes();
    let budget_after_first = sender.repair_extra_budget_remaining(mux_limits);

    assert!(
        sender
            .enqueue_critical_tail_repair_frame(duplicate)
            .is_none(),
        "final-tail RepairData is a one pending repair per offset range, not a repeatable owner-data substitute"
    );
    assert_eq!(sender.bytes(), bytes_after_first);
    assert_eq!(
        sender.repair_extra_budget_remaining(mux_limits),
        budget_after_first
    );
}

#[test]
fn client_repair_extra_budget_is_cumulative_not_per_event() {
    let mux_limits = MuxLimits::default();
    let stream_id = StreamId(93);
    let mut sender = RelaySenderService::new_with_performance(
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 1,
        },
    );
    let mut sender_queue = ReliableRelaySenderQueue::default();
    let startup_floor = response_extra_traffic_startup_floor_bytes(mux_limits);
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
    let mut sender = RelaySenderService::new_with_performance(
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 1,
        },
    );
    let mut sender_queue = ReliableRelaySenderQueue::default();
    let startup_floor = response_extra_traffic_startup_floor_bytes(mux_limits);
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
    let mut sender = RelaySenderService::new_with_performance(
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
        ResponseCarrierEmitMode::Classified,
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
        ResponseCarrierEmitMode::StreamOrdered,
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
        ResponseCarrierEmitMode::Classified,
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
        ResponseCarrierEmitMode::StreamOrdered,
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
fn active_quic_response_owner_emission_credit_uses_product_envelope_not_carrier_cwnd() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut active = response_target(0, UnderlayProtocol::Udp, 5.0, 0, payload_bytes as u64, true);
    active.snapshot.inflight_limit_bytes = payload_bytes as u64;

    let credit = response_target_emission_credit_bytes(
        &active,
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
    );

    assert_eq!(
        credit,
        bulk_active_service_product_envelope_bytes(active.snapshot, payload_bytes, mux_limits)
            as usize,
        "active response owner must use the product envelope, not current carrier cwnd"
    );
    assert!(
        credit > payload_bytes,
        "the regression requires credit above one carrier quantum"
    );
}

#[test]
fn active_tcp_response_owner_without_bulk_evidence_uses_startup_credit_not_full_envelope() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut active = response_target(0, UnderlayProtocol::Tcp, 80.0, 0, 16 * 1024 * 1024, true);
    active.has_sender_evidence = false;
    active.has_service_feed_evidence = false;
    active.has_bulk_rate_evidence = false;

    let credit = response_target_emission_credit_bytes(
        &active,
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
    );

    assert_eq!(
        credit,
        bulk_service_horizon_payload_bytes(payload_bytes, mux_limits),
        "unproven active Service startup must be bounded until path-scoped bulk-rate evidence exists"
    );
    assert!(
        credit >= payload_bytes,
        "startup Service credit must still admit at least one bulk quantum"
    );

    active.snapshot.product_bytes_in_flight = credit as u64;
    assert!(
        !response_service_has_assigned_owner_credit(
            &active,
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
        ),
        "startup credit bounds cumulative assigned flight, not only the draining writer queue"
    );
}

#[test]
fn active_quic_response_owner_bootstraps_with_bounded_feed_reservoir() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut active = response_target(0, UnderlayProtocol::Udp, 360.0, 0, 16 * 1024 * 1024, true);
    active.has_sender_evidence = true;
    active.has_service_feed_evidence = false;
    active.has_bulk_rate_evidence = false;

    let credit = response_target_emission_credit_bytes(
        &active,
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
    );

    assert_eq!(
        credit,
        bulk_service_feed_reservoir_payload_bytes(payload_bytes, mux_limits),
        "QUIC needs a durable ACK-derived sample before the full product envelope"
    );
    assert!(
        credit
            < bulk_active_service_product_envelope_bytes(active.snapshot, payload_bytes, mux_limits,)
                as usize
    );

    active.has_service_feed_evidence = true;
    let mature_feed_credit = response_target_emission_credit_bytes(
        &active,
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
    );
    assert_eq!(
        mature_feed_credit,
        bulk_active_service_product_envelope_bytes(active.snapshot, payload_bytes, mux_limits)
            as usize,
        "durable current-Service QUIC ACK progress unlocks the product envelope"
    );
    assert!(
        !active.has_bulk_rate_evidence,
        "current-Service feed evidence must not grant optional Subflow or handoff authority"
    );
}

#[test]
fn response_quic_feed_credit_uses_live_carrier_debt_not_outdated_bdp() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = 64 * 1024usize;
    let mut loaded_quic = response_target(0, UnderlayProtocol::Udp, 250.0, 0, 64 * 1024, true);
    loaded_quic.snapshot.delivery_rate_bps = 351_000.0;
    loaded_quic.snapshot.pacing_rate_bps = 351_000.0;
    loaded_quic.snapshot.product_progress_rate_bps = Some(10_000_000.0);
    loaded_quic.snapshot.bytes_in_flight = 8 * 1024 * 1024;
    loaded_quic.snapshot.queue_bytes = 1024 * 1024;

    let quic_credit = response_target_emission_credit_bytes(
        &loaded_quic,
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
    );
    let outdated_bdp_credit = adaptive_reliable_relay_inflight_bytes(
        Some(loaded_quic.snapshot),
        FlowLane::Throughput,
        mux_limits,
    );

    assert_eq!(
        quic_credit,
        bulk_active_service_product_envelope_bytes(loaded_quic.snapshot, payload_bytes, mux_limits,)
            as usize,
        "active QUIC Service feed credit must follow the product envelope, not live carrier debt"
    );
    assert!(
        quic_credit > outdated_bdp_credit,
        "app-limited BDP must not be the only active QUIC Service writer-feed ceiling"
    );

    let mut loaded_tcp = response_target(1, UnderlayProtocol::Tcp, 250.0, 0, 64 * 1024, true);
    loaded_tcp.snapshot.delivery_rate_bps = 351_000.0;
    loaded_tcp.snapshot.pacing_rate_bps = 351_000.0;
    loaded_tcp.snapshot.bytes_in_flight = 8 * 1024 * 1024;
    loaded_tcp.snapshot.queue_bytes = 1024 * 1024;
    loaded_tcp.snapshot.product_progress_rate_bps = Some(351_000.0);
    let tcp_credit = response_target_emission_credit_bytes(
        &loaded_tcp,
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
    );

    assert_eq!(
        tcp_credit,
        bulk_active_service_product_envelope_bytes(loaded_tcp.snapshot, payload_bytes, mux_limits,)
            as usize,
        "active TCP owners use the same carrier-neutral product envelope as active QUIC owners"
    );

    let mut subflow_quic = response_target(2, UnderlayProtocol::Udp, 250.0, 0, 64 * 1024, false);
    subflow_quic.snapshot.delivery_rate_bps = 351_000.0;
    subflow_quic.snapshot.pacing_rate_bps = 351_000.0;
    subflow_quic.snapshot.bytes_in_flight = 8 * 1024 * 1024;
    subflow_quic.snapshot.queue_bytes = 1024 * 1024;
    let subflow_credit = response_target_emission_credit_bytes(
        &subflow_quic,
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
    );

    assert!(
        subflow_credit >= 8 * 1024 * 1024,
        "Subflow QUIC paths remain carrier-debt gated rather than borrowing the active owner envelope"
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
fn active_tcp_response_owner_uses_product_envelope() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut target = response_target(
        0,
        UnderlayProtocol::Tcp,
        50.0,
        0,
        payload_bytes as u64,
        true,
    );
    target.snapshot.product_progress_rate_bps = Some(10_000_000_000.0);

    assert_eq!(
        response_target_emission_credit_bytes(
            &target,
            FlowLane::Throughput,
            payload_bytes,
            mux_limits
        ),
        bulk_active_service_product_envelope_bytes(target.snapshot, payload_bytes, mux_limits)
            as usize,
        "active TCP and QUIC owners should use the same product envelope; transport pacing belongs below the sender service"
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
fn proof_only_fallback_lead_cannot_become_response_service_owner() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut proof_only = response_target(
        1,
        UnderlayProtocol::Udp,
        5.0,
        0,
        4 * payload_bytes as u64,
        false,
    );
    proof_only.has_sender_evidence = true;
    proof_only.has_bulk_rate_evidence = false;
    let lead = ResponseBulkLead {
        key: proof_only.key,
        snapshot: proof_only.snapshot,
        eta_ms: proof_only.eta_ms,
    };

    let admission = response_target_unique_owner_admission(
        &proof_only,
        &[&proof_only],
        lead,
        None,
        0,
        payload_bytes,
        mux_limits,
    );

    assert_eq!(
        admission.decision,
        PathAdmissionDecision::ProbeOnly,
        "sender/proof evidence is not Service ownership; only an active anchor or bulk-rate-proven failover may own the Service role"
    );
    assert_eq!(admission.role, PathRuntimeRole::Probe);
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
        ResponseCarrierEmitMode::Classified,
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
        ResponseCarrierEmitMode::Classified,
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
        ResponseCarrierEmitMode::Classified,
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
        ResponseCarrierEmitMode::Classified,
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
        ResponseCarrierEmitMode::Classified,
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
            ResponseCarrierEmitMode::Classified,
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

#[test]
fn proof_only_validation_candidate_gets_explicit_startup_admission() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut active = response_target(
        0,
        UnderlayProtocol::Udp,
        100.0,
        0,
        4 * payload_bytes as u64,
        true,
    );
    active.snapshot.active_flows = 2;
    let mut proof_only = response_target(
        1,
        UnderlayProtocol::Udp,
        50.0,
        0,
        4 * payload_bytes as u64,
        false,
    );
    proof_only.has_bulk_rate_evidence = false;
    proof_only.has_sender_evidence = true;
    let candidates = vec![&active, &proof_only];
    let lead = ResponseBulkLead {
        key: active.key,
        snapshot: active.snapshot,
        eta_ms: active.eta_ms,
    };

    let admission = response_target_unique_owner_admission(
        &proof_only,
        &candidates,
        lead,
        None,
        0,
        payload_bytes,
        mux_limits,
    );

    assert_eq!(admission.decision, PathAdmissionDecision::AdmitSubflow);
    assert_eq!(admission.role, PathRuntimeRole::Subflow);
}

#[test]
fn frontier_clear_bulk_rate_candidate_is_subflow_not_service() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let active = response_target(0, UnderlayProtocol::Udp, 80.0, 0, 16 * 1024 * 1024, true);
    let alternate = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    let candidates = vec![&active, &alternate];
    let lead = ResponseBulkLead {
        key: alternate.key,
        snapshot: alternate.snapshot,
        eta_ms: alternate.eta_ms,
    };

    let admission = response_target_unique_owner_admission(
        &alternate,
        &candidates,
        lead,
        None,
        0,
        payload_bytes,
        mux_limits,
    );

    assert_eq!(admission.decision, PathAdmissionDecision::AdmitSubflow);
    assert_eq!(admission.role, PathRuntimeRole::Subflow);
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
        .await
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
    let dispatch = sender
        .dispatch_next_with_ordered_owner_debt(
            &stream,
            &mut send_stream,
            FlowLane::Throughput,
            mux_limits,
            ordered_owner_debt_bytes,
        )
        .await;

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
fn tcp_reservoir_subtracts_only_unique_owner_not_queue_or_repair() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service_horizon = bulk_service_horizon_payload_bytes(payload_bytes, mux_limits);
    let overflow = 1024 * 1024;
    let candidate_owner_bytes = 128 * 1024;
    let candidate_product_copies = 2 * 1024 * 1024;
    let service = response_target(
        0,
        UnderlayProtocol::Tcp,
        25.0,
        service_horizon as u64,
        mux_limits.max_path_flight_bytes as u64,
        true,
    );
    let mut candidate = response_target(
        1,
        UnderlayProtocol::Tcp,
        5.0,
        candidate_product_copies,
        mux_limits.max_path_flight_bytes as u64,
        false,
    );
    candidate.owner_data_in_flight_bytes = candidate_owner_bytes;
    candidate.snapshot.queue_bytes = (3 * 1024 * 1024) as u64;
    let tail = ResponseOrderedTail::new(Some(service.key), service_horizon + overflow);
    let reservoir = ResponseSameFamilyReservoir::new(
        service.key,
        tail,
        service_horizon as u64,
        service_horizon,
        bulk_service_feed_reservoir_payload_bytes(payload_bytes, mux_limits),
        payload_bytes,
    )
    .expect("global reservoir has credit");

    let debt = response_same_family_reservoir_candidate_debt(reservoir, &candidate);
    assert_eq!(
        debt.external_bytes(),
        (overflow - candidate_owner_bytes as usize) as u64
    );
    assert_eq!(
        debt.external_bytes() + candidate.snapshot.product_bytes_in_flight,
        (overflow + candidate_product_copies as usize - candidate_owner_bytes as usize) as u64,
        "shared queue pressure and duplicate RepairData cannot erase unique tail exposure"
    );
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
fn app_limited_bulk_proven_slow_subflow_still_requires_completion_gain() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, true);
    service.snapshot.product_progress_rate_bps = Some(120_000_000.0);
    let mut slow_subflow =
        response_target(1, UnderlayProtocol::Udp, 500.0, 0, 16 * 1024 * 1024, false);
    slow_subflow.snapshot.product_progress_rate_bps = Some(20_000_000.0);
    slow_subflow.snapshot.app_limited = true;
    slow_subflow.has_bulk_rate_evidence = true;
    let candidates = [&service, &slow_subflow];
    let lead = ResponseBulkLead {
        key: service.key,
        snapshot: service.snapshot,
        eta_ms: service.eta_ms,
    };

    let admission = response_target_unique_owner_admission(
        &slow_subflow,
        &candidates,
        lead,
        None,
        0,
        payload_bytes,
        mux_limits,
    );

    assert_eq!(admission.decision, PathAdmissionDecision::Standby);
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
fn endpoint_only_response_calibration_uses_service_only_as_opportunity_prior() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Tcp, 600.0, 0, 16 * 1024 * 1024, true);
    service.snapshot.delivery_rate_bps = 128_000_000.0;
    service.snapshot.pacing_rate_bps = 128_000_000.0;
    service.snapshot.srtt_ms = 333.0;
    service.snapshot.min_rtt_ms = 333.0;

    let mut candidate =
        response_target(1, UnderlayProtocol::Tcp, 570.0, 0, 16 * 1024 * 1024, false);
    candidate.snapshot.delivery_rate_bps = 2_500_000.0;
    candidate.snapshot.pacing_rate_bps = 2_500_000.0;
    candidate.snapshot.srtt_ms = 722.0;
    candidate.snapshot.min_rtt_ms = 722.0;
    candidate.snapshot.app_limited = true;
    candidate.endpoint_only_service_prior_eligible = true;
    candidate.ack_clock_calibration_eligible = true;
    candidate.ack_clock_calibration_credit_limit_bytes = 452_124;
    candidate.ack_clock_calibration_max_limit_bytes = 64 * 1024 * 1024;

    let (effective, effective_eta_ms, borrowed) = response_tcp_calibration_opportunity_candidate(
        &service,
        &candidate,
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
    );
    assert!(borrowed);
    assert_eq!(
        effective.delivery_rate_bps,
        service.snapshot.delivery_rate_bps
    );
    assert_eq!(effective.rate_scope, ResponseRateScope::PathCapacity);
    assert!(effective_eta_ms < candidate.eta_ms);
    assert_eq!(candidate.snapshot.delivery_rate_bps, 2_500_000.0);

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
    .expect("the endpoint-only candidate should receive bounded calibration work");
    assert_eq!(selected.target.key, candidate.key);
    assert!(selected.ack_clock_calibration_commit.is_some());

    let feed_reservoir = bulk_service_feed_reservoir_payload_bytes(payload_bytes, mux_limits);
    let mut calibrating = candidate.clone();
    calibrating.ack_clock_calibration_active = true;
    calibrating.ack_clock_calibration_spent_bytes =
        calibrating.ack_clock_calibration_credit_limit_bytes;
    let calibration_reservoir = feed_reservoir
        + usize::try_from(calibrating.ack_clock_calibration_credit_limit_bytes).unwrap();
    let service_within_projection = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), calibrating.clone()],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        calibration_reservoir - payload_bytes,
        None,
    )
    .expect("Service may fill the exact remainder projected behind calibration");
    assert_eq!(service_within_projection.target.key, service.key);
    assert!(
        select_response_sender_data_target_with_ordered_debt_and_epoch(
            &[service.clone(), calibrating],
            FlowLane::Throughput,
            payload_bytes,
            mux_limits,
            &[],
            Some(service.key),
            calibration_reservoir,
            None,
        )
        .is_none(),
        "Service must wait when calibration flight and its projected follow-up fill the reservoir"
    );

    candidate.endpoint_only_service_prior_eligible = false;
    let configured = select_response_sender_data_target_with_ordered_debt_and_epoch(
        &[service.clone(), candidate],
        FlowLane::Throughput,
        payload_bytes,
        mux_limits,
        &[],
        Some(service.key),
        0,
        None,
    )
    .expect("configured candidate rejection must leave Service available");
    assert_eq!(configured.target.key, service.key);
    assert!(configured.ack_clock_calibration_commit.is_none());
}

#[test]
fn fresh_tcp_calibration_is_dormant_without_active_response_demand() {
    let mut candidate =
        response_target(1, UnderlayProtocol::Tcp, 500.0, 0, 16 * 1024 * 1024, false);
    let (commands, _receivers) = reliable_path_command_channels(8);
    candidate.commands = commands;
    candidate.ack_clock_calibration_eligible = true;
    candidate.ack_clock_calibration_credit_limit_bytes = 256 * 1024;
    candidate.ack_clock_calibration_max_limit_bytes = 64 * 1024 * 1024;

    assert!(response_ack_clock_calibration_pending(&candidate, true));
    assert!(!response_ack_clock_calibration_pending(&candidate, false));
    assert!(response_ack_clock_calibration_blocks_generic_owner(
        &candidate
    ));
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
        reliable_stream_frame_payload_bytes(&repair) as u64,
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

#[test]
fn tcp_response_startup_does_not_double_count_global_ordered_tail() {
    let mut mux_limits = MuxLimits::default();
    mux_limits.max_path_flight_bytes = 2 * 1024 * 1024;
    mux_limits.max_repair_bytes = 2 * 1024 * 1024;
    mux_limits.max_reorder_bytes = 2 * 1024 * 1024;
    mux_limits.max_stream_window_bytes = 2 * 1024 * 1024;
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Tcp, 5.0, 0, 2 * 1024 * 1024, true);
    service.snapshot.active_flows = 1;
    let mut candidate = response_target(1, UnderlayProtocol::Tcp, 500.0, 0, 2 * 1024 * 1024, false);
    let committed = 2 * 1024 * 1024 - payload_bytes as u64;
    candidate.snapshot.product_bytes_in_flight = committed;
    candidate.has_bulk_rate_evidence = false;

    assert!(response_target_is_startup_same_underlay_subflow_candidate(
        service.key,
        &service,
        &candidate,
        committed,
        payload_bytes,
        mux_limits,
    ));
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
        .await
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
        .await
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
        .await
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
        .await
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
fn app_limited_bulk_proven_fast_subflow_can_still_improve_completion() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut service = response_target(0, UnderlayProtocol::Udp, 50.0, 0, 16 * 1024 * 1024, true);
    service.snapshot.product_progress_rate_bps = Some(20_000_000.0);
    let mut fast_subflow =
        response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    fast_subflow.snapshot.product_progress_rate_bps = Some(120_000_000.0);
    fast_subflow.snapshot.app_limited = true;
    fast_subflow.has_bulk_rate_evidence = true;
    let candidates = [&service, &fast_subflow];
    let lead = ResponseBulkLead {
        key: service.key,
        snapshot: service.snapshot,
        eta_ms: service.eta_ms,
    };

    let admission = response_target_unique_owner_admission(
        &fast_subflow,
        &candidates,
        lead,
        None,
        0,
        payload_bytes,
        mux_limits,
    );

    assert_eq!(admission.decision, PathAdmissionDecision::AdmitSubflow);
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
fn active_attachment_without_bulk_evidence_remains_service_anchor_when_measured_subflow_exists() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let mut active_attachment =
        response_target(0, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, true);
    active_attachment.has_bulk_rate_evidence = false;
    let measured_lead = response_target(1, UnderlayProtocol::Tcp, 5.0, 0, 16 * 1024 * 1024, false);
    let candidates = vec![&active_attachment, &measured_lead];
    let lead = ResponseBulkLead {
        key: measured_lead.key,
        snapshot: measured_lead.snapshot,
        eta_ms: measured_lead.eta_ms,
    };

    let admission = response_target_unique_owner_admission(
        &active_attachment,
        &candidates,
        lead,
        None,
        0,
        payload_bytes,
        mux_limits,
    );

    assert_eq!(
        admission.decision,
        PathAdmissionDecision::Service,
        "the active attachment remains the Service anchor; measured alternates are Subflows"
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
fn measured_subflow_requires_later_startup_candidate_to_beat_service_reservoir() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let service = response_target(
        0,
        UnderlayProtocol::Tcp,
        250.0,
        0,
        mux_limits.max_path_flight_bytes as u64,
        true,
    );
    let mut measured = response_target(
        1,
        UnderlayProtocol::Tcp,
        400.0,
        0,
        mux_limits.max_path_flight_bytes as u64,
        false,
    );
    measured.snapshot.app_limited = false;
    let mut cold = response_target(
        2,
        UnderlayProtocol::Tcp,
        10_000.0,
        0,
        mux_limits.max_path_flight_bytes as u64,
        false,
    );
    cold.has_bulk_rate_evidence = false;

    assert!(
        response_startup_sample_has_completion_opportunity(
            &[&service, &cold],
            &service,
            &cold,
            payload_bytes,
            mux_limits,
        ),
        "the first bounded candidate remains the non-circular discovery bootstrap"
    );
    assert!(
        !response_startup_sample_has_completion_opportunity(
            &[&service, &measured, &cold],
            &service,
            &cold,
            payload_bytes,
            mux_limits,
        ),
        "after useful capacity exists, a cold candidate cannot create a much slower ordered prefix"
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

#[test]
fn quic_capacity_calibration_requires_reachable_underloaded_family() {
    let service = response_target(0, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, true);
    let mut udp = response_target(1, UnderlayProtocol::Udp, 5.0, 0, 16 * 1024 * 1024, false);
    udp.has_bulk_rate_evidence = false;

    assert_eq!(
        select_response_quic_capacity_calibration_target(
            &[service.clone(), udp.clone()],
            FlowLane::Throughput,
            Some(service.key),
            ResponseServiceFamilyLoads::new(2, 0),
            MuxLimits::default(),
            reliable_quic_capacity_calibration_session_limit_bytes(MuxLimits::default()),
        )
        .map(|target| target.key),
        Some(udp.key),
        "a native QUIC sample may break the proof cycle without product offsets"
    );
    assert!(
        select_response_quic_capacity_calibration_target(
            &[service.clone(), udp.clone()],
            FlowLane::Throughput,
            Some(service.key),
            ResponseServiceFamilyLoads::new(1, 1),
            MuxLimits::default(),
            reliable_quic_capacity_calibration_session_limit_bytes(MuxLimits::default()),
        )
        .is_none(),
        "balanced Service families need no optional carrier calibration"
    );
    udp.has_sender_evidence = false;
    assert!(
        select_response_quic_capacity_calibration_target(
            &[service, udp],
            FlowLane::Throughput,
            Some(CarrierPathKey {
                underlay: UnderlayProtocol::Tcp,
                path_id: PathId(0),
            }),
            ResponseServiceFamilyLoads::new(2, 0),
            MuxLimits::default(),
            reliable_quic_capacity_calibration_session_limit_bytes(MuxLimits::default()),
        )
        .is_none(),
        "capacity traffic must not replace path reachability proof"
    );
}

#[test]
fn request_quic_capacity_geometry_models_the_competing_service_rate_pipe() {
    let mux_limits = MuxLimits::default();
    let mut candidate = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 180.0, 1_000_000.0);
    candidate.inflight_limit_bytes = 262_144;

    let geometry = request_quic_capacity_calibration_geometry(
        candidate,
        100_000_000.0,
        mux_limits,
        reliable_quic_capacity_calibration_session_limit_bytes(mux_limits),
    )
    .expect("the competing pipe should fit the default session envelope");

    assert_eq!(geometry.warmup_carrier_bytes, 4_500_000);
    assert_eq!(geometry.desired_warmup_carrier_bytes, 4_500_000);
    assert_eq!(geometry.service_rate_bps, 100_000_000);
    assert_eq!(geometry.candidate_carrier_flight_bytes, 0);
    assert_eq!(
        geometry.train_bytes,
        geometry
            .warmup_carrier_bytes
            .saturating_add(geometry.required_timed_carrier_bytes)
            .saturating_add(geometry.timing_slack_bytes),
        "the strict window retains a full callback-batching guard after cold-start warmup"
    );
    assert_eq!(
        geometry.accounting_slack_bytes,
        PATH_OPEN_SCORE_BYTES as u64
    );
}

#[test]
#[cfg(target_os = "linux")]
fn request_tcp_capacity_geometry_requires_mature_service_and_full_pipe() {
    let mux_limits = MuxLimits::default();
    let mut candidate = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 180.0, 1_000_000.0);
    candidate.inflight_limit_bytes = 32 * 1024 * 1024;
    let mature_service = RequestPerFlowRateModel {
        rate_bps: 100_000_000.0,
        delivery_samples: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
    };
    let envelope = reliable_quic_capacity_calibration_session_limit_bytes(mux_limits);

    let geometry =
        request_tcp_capacity_calibration_geometry(candidate, mature_service, mux_limits, envelope)
            .expect("the exact competing pipe fits the default carrier budget");
    assert_eq!(geometry.warmup_carrier_bytes, 4_500_000);
    assert_eq!(geometry.required_timed_carrier_bytes, 247_544);
    assert_eq!(
        geometry.timing_slack_bytes,
        BBR_MAX_SEND_QUANTUM_BYTES as u64
    );
    let measurement_bytes = geometry
        .timing_slack_bytes
        .checked_add(geometry.required_timed_carrier_bytes)
        .expect("measurement sizing fits the carrier envelope");
    assert_eq!(measurement_bytes, 313_080);
    assert!(measurement_bytes >= geometry.sample_floor_bytes);
    assert_eq!(
        geometry.warmup_carrier_bytes + measurement_bytes,
        geometry.train_bytes,
        "the full receipt sample uses the existing bounded train"
    );
    assert_eq!(geometry.train_bytes, 4_813_080);
    assert_eq!(geometry.service_rate_bps, 100_000_000);
    assert!(
        request_tcp_capacity_calibration_geometry(
            candidate,
            RequestPerFlowRateModel {
                delivery_samples: RELIABLE_INITIAL_WINDOW_PACKETS as u32 - 1,
                ..mature_service
            },
            mux_limits,
            envelope,
        )
        .is_none(),
        "a startup/path-capacity prior must never size request TCP calibration"
    );
    assert!(
        request_tcp_capacity_calibration_geometry(
            candidate,
            mature_service,
            mux_limits,
            geometry.train_bytes - 1,
        )
        .is_none(),
        "TCP must skip rather than truncate below its complete warmup and ACK span"
    );
}

#[test]
fn request_capacity_candidate_share_is_fixed_by_eligible_topology() {
    let mux_limits = MuxLimits::default();
    let session_limit = reliable_quic_capacity_calibration_session_limit_bytes(mux_limits);

    assert_eq!(
        request_capacity_stable_candidate_share_bytes(mux_limits, 4),
        session_limit / 4
    );
    assert_eq!(
        request_capacity_stable_candidate_share_bytes(mux_limits, 2),
        session_limit / 2
    );
    assert_eq!(
        request_capacity_stable_candidate_share_bytes(mux_limits, 0),
        session_limit,
        "zero is a defensive no-candidate input, not a zero-byte divisor"
    );
}

#[test]
#[cfg(target_os = "linux")]
fn request_tcp_capacity_receipt_admission_ignores_only_stale_control_flight() {
    let mut candidate = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 180.0, 1_000_000.0);
    candidate.bytes_in_flight = 1_448;
    assert!(
        request_tcp_capacity_candidate_can_start_receipt(candidate),
        "a full typed receipt safely includes stale control delay"
    );

    candidate.product_bytes_in_flight = 1;
    assert!(!request_tcp_capacity_candidate_can_start_receipt(candidate));
    candidate.product_bytes_in_flight = 0;
    candidate.queue_bytes = 1;
    assert!(!request_tcp_capacity_candidate_can_start_receipt(candidate));
    candidate.queue_bytes = 0;
    candidate.active_latency_sensitive_flows = 1;
    assert!(!request_tcp_capacity_candidate_can_start_receipt(candidate));
}

#[test]
#[cfg(target_os = "linux")]
fn request_tcp_capacity_lease_is_derived_from_growth_service_and_recovery() {
    let candidate = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 180.0, 1_000_000.0);
    let train_bytes = 4_813_080;
    let service_rate_bps = 100_000_000;
    let pto = transport_pto_from_snapshot(Some(candidate));
    let growth = pto.saturating_mul(request_quic_capacity_slow_start_rounds(train_bytes));
    let service_transfer =
        Duration::from_secs_f64(train_bytes as f64 * 8.0 / service_rate_bps as f64);
    let expected = pto
        .saturating_add(growth.max(service_transfer))
        .saturating_add(pto)
        .max(Duration::from_secs(1));

    assert_eq!(
        request_tcp_capacity_calibration_lease(candidate, train_bytes, service_rate_bps),
        expected,
        "every cold growth round owns one recovery-capable candidate PTO"
    );
}

#[test]
fn request_quic_capacity_geometry_excludes_candidate_product_flight() {
    let mux_limits = MuxLimits::default();
    let envelope = reliable_quic_capacity_calibration_session_limit_bytes(mux_limits);
    let mut candidate = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 180.0, 1_000_000.0);
    candidate.inflight_limit_bytes = 262_144;
    candidate.product_bytes_in_flight = 3_000_000;
    candidate.bytes_in_flight = 3_500_000;

    let first =
        request_quic_capacity_calibration_geometry(candidate, 1_000_000.0, mux_limits, envelope)
            .expect("the native carrier flight should fit");
    assert_eq!(first.candidate_carrier_flight_bytes, 500_000);
    assert_eq!(first.warmup_carrier_bytes, 500_000);

    candidate.product_bytes_in_flight = 7_000_000;
    candidate.bytes_in_flight = 7_500_000;
    let more_product =
        request_quic_capacity_calibration_geometry(candidate, 1_000_000.0, mux_limits, envelope)
            .expect("product debt must not alter carrier geometry");
    assert_eq!(more_product, first);

    candidate.bytes_in_flight = 7_750_000;
    let more_carrier =
        request_quic_capacity_calibration_geometry(candidate, 1_000_000.0, mux_limits, envelope)
            .expect("the larger native carrier flight should fit");
    assert_eq!(more_carrier.candidate_carrier_flight_bytes, 750_000);
    assert_eq!(more_carrier.warmup_carrier_bytes, 750_000);
}

#[test]
fn request_quic_capacity_geometry_requires_valid_rate_and_budget() {
    let mux_limits = MuxLimits::default();
    let candidate = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 180.0, 1_000_000.0);

    assert!(
        request_quic_capacity_calibration_geometry(
            candidate,
            f64::NAN,
            mux_limits,
            reliable_quic_capacity_calibration_session_limit_bytes(mux_limits),
        )
        .is_none(),
        "an invalid carrier rate must not size capacity traffic"
    );

    let bounded = request_quic_capacity_calibration_geometry(
        candidate,
        2_000_000_000.0,
        mux_limits,
        reliable_quic_capacity_calibration_session_limit_bytes(mux_limits),
    )
    .expect("a bounded train can still test capacity below a larger competing pipe");
    assert_eq!(
        bounded.train_bytes,
        reliable_quic_capacity_calibration_session_limit_bytes(mux_limits)
    );
    assert!(bounded.desired_warmup_carrier_bytes > bounded.warmup_carrier_bytes);

    let mut carrier_loaded = candidate;
    carrier_loaded.bytes_in_flight =
        reliable_quic_capacity_calibration_session_limit_bytes(mux_limits);
    assert!(
        request_quic_capacity_calibration_geometry(
            carrier_loaded,
            100_000_000.0,
            mux_limits,
            reliable_quic_capacity_calibration_session_limit_bytes(mux_limits),
        )
        .is_none(),
        "the session envelope must not clamp below native carrier flight"
    );
}

#[test]
fn request_quic_capacity_lease_covers_cold_congestion_window_growth() {
    let mut candidate = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 180.0, 1_000_000.0);
    candidate.jitter_ms = 90.0;
    let train_bytes = 8_553_080;
    let rounds = request_quic_capacity_slow_start_rounds(train_bytes);
    let pto = transport_pto_from_snapshot(Some(candidate));
    let modeled_round_trip = Duration::from_millis(180).max(pto.div_f64(BBR_DEFAULT_CWND_GAIN));

    assert_eq!(rounds, 10);
    assert!(
        request_quic_capacity_calibration_lease(candidate, train_bytes)
            >= modeled_round_trip
                .saturating_mul(rounds)
                .saturating_add(pto),
        "a competing-pipe train must not inherit the smaller startup-sample deadline"
    );
}

#[test]
fn quic_capacity_calibration_prefers_fresh_path_before_retry() {
    let mux_limits = MuxLimits::default();
    let service = response_target(0, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, true);
    let mut retry = response_target(1, UnderlayProtocol::Udp, 1.0, 0, 16 * 1024 * 1024, false);
    retry.has_bulk_rate_evidence = false;
    retry.quic_capacity_calibration_attempts = 1;
    let mut fresh = response_target(2, UnderlayProtocol::Udp, 100.0, 0, 16 * 1024 * 1024, false);
    fresh.has_bulk_rate_evidence = false;

    let selected = select_response_quic_capacity_calibration_target(
        &[service.clone(), retry, fresh.clone()],
        FlowLane::Throughput,
        Some(service.key),
        ResponseServiceFamilyLoads::new(2, 0),
        mux_limits,
        reliable_quic_capacity_calibration_session_limit_bytes(mux_limits),
    )
    .expect("at least one reachable UDP path should remain probeable");

    assert_eq!(
        selected.key, fresh.key,
        "an unattempted path must be sampled before a lower-ETA retry"
    );
}

#[test]
fn quic_capacity_calibration_filters_path_at_attempt_limit() {
    let mux_limits = MuxLimits::default();
    let service = response_target(0, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, true);
    let mut exhausted = response_target(1, UnderlayProtocol::Udp, 1.0, 0, 16 * 1024 * 1024, false);
    exhausted.has_bulk_rate_evidence = false;
    exhausted.quic_capacity_calibration_attempts =
        MAX_RESPONSE_QUIC_CAPACITY_CALIBRATION_ATTEMPTS_PER_PATH;

    assert!(
        select_response_quic_capacity_calibration_target(
            &[service.clone(), exhausted],
            FlowLane::Throughput,
            Some(service.key),
            ResponseServiceFamilyLoads::new(2, 0),
            mux_limits,
            reliable_quic_capacity_calibration_session_limit_bytes(mux_limits),
        )
        .is_none(),
        "a path must not exceed its exact-path calibration attempt limit"
    );
}

#[test]
fn quic_capacity_calibration_uses_smaller_retry_when_fresh_train_does_not_fit() {
    let mux_limits = MuxLimits::default();
    let session_limit = reliable_quic_capacity_calibration_session_limit_bytes(mux_limits);
    let service = response_target(0, UnderlayProtocol::Tcp, 50.0, 0, 16 * 1024 * 1024, true);
    let mut retry = response_target(1, UnderlayProtocol::Udp, 50.0, 0, 1, false);
    retry.has_bulk_rate_evidence = false;
    retry.quic_capacity_calibration_attempts = 1;
    let mut fresh = response_target(2, UnderlayProtocol::Udp, 1.0, 0, session_limit, false);
    fresh.has_bulk_rate_evidence = false;

    let retry_train = response_quic_capacity_calibration_train_bytes(&retry, mux_limits) as u64;
    let fresh_train = response_quic_capacity_calibration_train_bytes(&fresh, mux_limits) as u64;
    assert!(
        !response_quic_capacity_calibration_geometry(&fresh, mux_limits).fits_session_envelope,
        "a clamped train cannot silently change its frozen warmup/proof geometry"
    );
    assert!(
        retry_train < fresh_train,
        "the test needs a retry train that fits below the fresh path's live window"
    );

    let selected = select_response_quic_capacity_calibration_target(
        &[service.clone(), retry.clone(), fresh],
        FlowLane::Throughput,
        Some(service.key),
        ResponseServiceFamilyLoads::new(2, 0),
        mux_limits,
        retry_train,
    )
    .expect("the smaller retry should fit the remaining session envelope");

    assert_eq!(
        selected.key, retry.key,
        "a too-large fresh train must not hide a retry that still fits the remaining budget"
    );
}

#[test]
fn quic_capacity_retry_fills_live_window_plus_fresh_proof_window() {
    let mux_limits = MuxLimits::default();
    let mut udp = response_target(3, UnderlayProtocol::Udp, 390.0, 0, 798_666, false);
    udp.snapshot.pacing_rate_bps = 5_530_000.0;
    udp.snapshot.delivery_rate_bps = 153_000.0;

    assert_eq!(
        response_quic_capacity_calibration_train_bytes(&udp, mux_limits),
        1_111_746,
        "the grown window needs one strict-proof window plus one pacing guard"
    );
    assert!(
        response_quic_capacity_calibration_lease(&udp, 1_111_746)
            >= transport_pto_from_snapshot(Some(udp.snapshot)),
        "the admitted train lease must cover at least one recovery horizon"
    );

    udp.snapshot.inflight_limit_bytes = u64::MAX;
    assert!(
        !response_quic_capacity_calibration_geometry(&udp, mux_limits).fits_session_envelope,
        "a live window larger than the resource envelope is ineligible, not repeatedly reservable"
    );
    assert_eq!(
        response_quic_capacity_calibration_train_bytes(&udp, mux_limits) as u64,
        reliable_quic_capacity_calibration_session_limit_bytes(mux_limits),
        "a single train cannot exceed the session carrier envelope"
    );
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
    service.snapshot.rate_scope = ResponseRateScope::PerFlowGoodput;
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
    service.snapshot.rate_scope = ResponseRateScope::PerFlowGoodput;
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
        ResponseRateScope::PathCapacity,
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

    tcp.snapshot.rate_scope = ResponseRateScope::PathCapacity;
    assert!(response_service_handoff_preserves_fair_share(&tcp, &udp));
    tcp.snapshot.rate_scope = ResponseRateScope::PerFlowGoodput;
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
fn response_fallback_preserves_lower_flight_completion_backlog() {
    let payload_bytes = 64 * 1024;
    let mux_limits = MuxLimits::default();
    let mut service = response_target(0, UnderlayProtocol::Tcp, 400.0, 0, 16 * 1024 * 1024, true);
    service.snapshot.srtt_ms = 360.0;
    service.snapshot.delivery_rate_bps = 400_000_000.0;
    service.snapshot.pacing_rate_bps = 400_000_000.0;
    let mut candidate =
        response_target(1, UnderlayProtocol::Tcp, 410.0, 0, 16 * 1024 * 1024, false);
    candidate.snapshot.srtt_ms = 360.0;
    candidate.snapshot.delivery_rate_bps = 200_000_000.0;
    candidate.snapshot.pacing_rate_bps = 200_000_000.0;
    candidate.snapshot.app_limited = false;
    let lead = ResponseBulkLead {
        key: service.key,
        snapshot: service.snapshot,
        eta_ms: service.eta_ms,
    };
    let lower_flight_bytes = 8 * 1024 * 1024;
    let check = BulkAdmissionCheck {
        best_snapshot: lead.snapshot,
        best_eta_ms: lead.eta_ms,
        candidate_snapshot: candidate.snapshot,
        candidate_eta_ms: candidate.eta_ms,
        payload_bytes,
        mux_limits,
        role: BulkAdmissionRole::AdditionalSameUnderlay,
        stream_ordering_debt_bytes: lower_flight_bytes,
    };

    assert_eq!(
        bulk_candidate_admission_suppression_with_ordering_debt(check),
        Some("same_underlay_no_completion_gain"),
        "request receive-hole policy must not infer Service backlog"
    );
    assert_eq!(
        response_fallback_bulk_model_suppression(
            &candidate,
            lead,
            lower_flight_bytes,
            payload_bytes,
            mux_limits,
            BulkAdmissionRole::AdditionalSameUnderlay,
        ),
        None,
        "response lower flight is real completion backlog and must retain the 8c response policy"
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
