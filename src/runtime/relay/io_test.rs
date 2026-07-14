use super::*;
use crate::protocol::frame::{reliable_stream_frame_extent, stream_ack_contiguous_frontier};

#[test]
fn stream_fin_waits_for_final_offset_before_close() {
    let mut recv_stream = ReliableRecvStream::new(StreamId(1), MuxLimits::default());
    let mut pending_final_offset = None;

    assert!(
        !receive_stream_fin(&recv_stream, &mut pending_final_offset, 5)
            .expect("record pending fin")
    );
    assert_eq!(pending_final_offset, Some(5));
    assert!(!pending_stream_fin_ready(
        &recv_stream,
        pending_final_offset
    ));

    recv_stream
        .receive_data(0, Bytes::from_static(b"hello"), StreamFlags::NONE)
        .expect("tail data");

    assert!(pending_stream_fin_ready(&recv_stream, pending_final_offset));
}

#[test]
fn terminal_fin_replay_requires_sent_fin_and_completed_owner_bytes() {
    assert!(!stream_terminal_fin_replay_required(
        false, false, true, 0, 64, 64,
    ));
    assert!(!stream_terminal_fin_replay_required(
        true, true, true, 0, 64, 64,
    ));
    assert!(!stream_terminal_fin_replay_required(
        true, false, false, 0, 64, 64,
    ));
    assert!(!stream_terminal_fin_replay_required(
        true, false, true, 1, 64, 64,
    ));
    assert!(!stream_terminal_fin_replay_required(
        true, false, true, 0, 63, 64,
    ));
    assert!(stream_terminal_fin_replay_required(
        true, false, true, 0, 64, 64,
    ));
}

#[test]
fn duplicate_stream_data_below_final_frontier_is_already_delivered() {
    let mut recv_stream = ReliableRecvStream::new(StreamId(1), MuxLimits::default());
    recv_stream
        .receive_data(0, Bytes::from_static(b"hello"), StreamFlags::NONE)
        .expect("receive data");

    assert!(stream_data_range_already_delivered(&recv_stream, 0, 5));
    assert!(!stream_data_range_already_delivered(&recv_stream, 0, 6));
    assert!(!stream_data_range_already_delivered(&recv_stream, 5, 1));
}

#[test]
fn reliable_relay_sender_queue_budget_respects_stream_flow_control_credit() {
    let limits = MuxLimits {
        max_stream_window_bytes: 4,
        max_repair_bytes: 16,
        max_path_flight_bytes: 16,
        max_reliable_relay_chunk_bytes: 16,
        ..MuxLimits::default()
    };
    let mut send_stream = ReliableSendStream::new(StreamId(7), limits);
    let sender_queue = ReliableRelaySenderQueue::default();
    send_stream
        .send_data(Bytes::from_static(b"data"), StreamFlags::NONE)
        .expect("initial window payload");

    assert!(!reliable_relay_can_read_into_sender_queue(
        &send_stream,
        &sender_queue,
        limits,
        16
    ));
    assert_eq!(
        reliable_relay_sender_queue_read_budget(&send_stream, &sender_queue, limits, 16, 16),
        0
    );

    send_stream.update_max_offset(6);
    assert!(reliable_relay_can_read_into_sender_queue(
        &send_stream,
        &sender_queue,
        limits,
        16
    ));
    assert_eq!(
        reliable_relay_sender_queue_read_budget(&send_stream, &sender_queue, limits, 16, 16),
        2
    );
}

#[test]
fn ack_gap_repair_requires_multipath_alternative_and_persistent_gap() {
    assert!(!stream_ack_gap_repair_allowed(true, false, true));
    assert!(!stream_ack_gap_repair_allowed(true, true, false));
    assert!(stream_ack_gap_repair_allowed(true, true, true));
    assert!(!stream_ack_gap_repair_allowed(false, true, true));
}

#[test]
fn ack_gap_repair_requires_authoritative_ack_gap_shape() {
    assert!(!stream_ack_ranges_expose_authoritative_gap(
        false,
        &[
            OffsetRange {
                start: 0,
                end: 1024,
            },
            OffsetRange {
                start: 2048,
                end: 4096,
            },
        ],
    ));
    assert!(!stream_ack_ranges_expose_authoritative_gap(
        true,
        &[OffsetRange {
            start: 0,
            end: 1024,
        }],
    ));
    assert!(stream_ack_ranges_expose_authoritative_gap(
        true,
        &[OffsetRange {
            start: 1024,
            end: 4096,
        }],
    ));
    assert!(stream_ack_ranges_expose_authoritative_gap(
        true,
        &[
            OffsetRange {
                start: 0,
                end: 1024,
            },
            OffsetRange {
                start: 2048,
                end: 4096,
            },
        ],
    ));
}

#[test]
fn fixed_source_staging_keeps_the_existing_owner_tail_reservoir() {
    let mux_limits = MuxLimits::default();
    let payload = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let horizon = bulk_service_horizon_payload_bytes(payload, mux_limits);
    let reservoir = bulk_service_feed_reservoir_payload_bytes(payload, mux_limits);

    assert_eq!(
        reliable_relay_source_staging_owner_tail_headroom(
            ReliableSourceStagingContext {
                independent: false,
                service: Some(ReliableSourceServiceStagingContext {
                    allows_product_envelope: false,
                    has_latency_pressure: false,
                    has_feed_evidence: true,
                }),
            },
            FlowLane::Throughput,
            horizon,
            0,
            payload,
            mux_limits,
        ),
        reservoir.saturating_sub(horizon),
    );
}

#[test]
fn proven_response_source_staging_uses_the_configured_product_envelope() {
    let mut mux_limits = MuxLimits::default();
    mux_limits.max_path_flight_bytes = 12 * 1024 * 1024;
    mux_limits.max_reorder_bytes = 16 * 1024 * 1024;
    mux_limits.max_stream_window_bytes = 20 * 1024 * 1024;
    let payload = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let owner_tail = 3 * 1024 * 1024;
    let raw_queue = payload;
    let envelope = bulk_service_product_envelope_payload_bytes(payload, mux_limits);

    assert_eq!(
        reliable_relay_source_staging_owner_tail_headroom(
            ReliableSourceStagingContext {
                independent: false,
                service: Some(ReliableSourceServiceStagingContext {
                    allows_product_envelope: true,
                    has_latency_pressure: false,
                    has_feed_evidence: true,
                }),
            },
            FlowLane::Throughput,
            owner_tail,
            raw_queue,
            payload,
            mux_limits,
        ),
        envelope
            .saturating_sub(owner_tail)
            .saturating_sub(raw_queue),
        "a proven coupled response may fill its configured product envelope"
    );
}

#[test]
fn coupled_source_staging_keeps_the_latency_pressure_horizon() {
    let mux_limits = MuxLimits::default();
    let payload = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let horizon = bulk_service_horizon_payload_bytes(payload, mux_limits);

    assert_eq!(
        reliable_relay_source_staging_owner_tail_headroom(
            ReliableSourceStagingContext {
                independent: false,
                service: Some(ReliableSourceServiceStagingContext {
                    allows_product_envelope: true,
                    has_latency_pressure: true,
                    has_feed_evidence: true,
                }),
            },
            FlowLane::Throughput,
            horizon,
            0,
            payload,
            mux_limits,
        ),
        0,
    );
}

#[test]
fn mixed_underlay_source_staging_is_independent_from_assigned_owner_tail() {
    let mux_limits = MuxLimits::default();
    let payload = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let horizon = bulk_service_horizon_payload_bytes(payload, mux_limits);

    assert_eq!(
        reliable_relay_source_staging_owner_tail_headroom(
            ReliableSourceStagingContext {
                independent: true,
                service: None,
            },
            FlowLane::Throughput,
            usize::MAX,
            0,
            payload,
            mux_limits,
        ),
        horizon,
        "assigned owner tail must not consume the independent raw staging reservoir"
    );
    assert_eq!(
        reliable_relay_source_staging_owner_tail_headroom(
            ReliableSourceStagingContext {
                independent: true,
                service: None,
            },
            FlowLane::Throughput,
            usize::MAX,
            horizon,
            payload,
            mux_limits,
        ),
        0,
        "unassigned raw staging remains bounded by its own reservoir"
    );
}

#[test]
fn mixed_underlay_source_staging_uses_mature_raw_reservoir() {
    let mux_limits = MuxLimits::default();
    let payload = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let horizon = bulk_service_horizon_payload_bytes(payload, mux_limits);
    let reservoir = bulk_service_feed_reservoir_payload_bytes(payload, mux_limits);

    assert_eq!(
        reliable_relay_source_staging_owner_tail_headroom(
            ReliableSourceStagingContext {
                independent: true,
                service: Some(ReliableSourceServiceStagingContext {
                    allows_product_envelope: true,
                    has_latency_pressure: false,
                    has_feed_evidence: true,
                }),
            },
            FlowLane::Throughput,
            usize::MAX,
            horizon,
            payload,
            mux_limits,
        ),
        reservoir.saturating_sub(horizon),
        "mature raw staging may use the feed reservoir without borrowing owner-tail credit"
    );
}

#[test]
fn mixed_underlay_source_staging_returns_to_horizon_under_latency_pressure() {
    let mux_limits = MuxLimits::default();
    let payload = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let horizon = bulk_service_horizon_payload_bytes(payload, mux_limits);

    assert_eq!(
        reliable_relay_source_staging_owner_tail_headroom(
            ReliableSourceStagingContext {
                independent: true,
                service: Some(ReliableSourceServiceStagingContext {
                    allows_product_envelope: true,
                    has_latency_pressure: true,
                    has_feed_evidence: true,
                }),
            },
            FlowLane::Throughput,
            usize::MAX,
            horizon.saturating_sub(payload),
            payload,
            mux_limits,
        ),
        payload,
        "path-local latency pressure narrows independent raw staging back to the Service horizon"
    );
}

#[test]
fn full_confidence_progress_does_not_unlock_source_staging_without_bulk_evidence() {
    let mux_limits = MuxLimits::default();
    let payload = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let reservoir = bulk_service_feed_reservoir_payload_bytes(payload, mux_limits);

    assert_eq!(
        reliable_relay_source_staging_owner_tail_headroom(
            ReliableSourceStagingContext {
                independent: false,
                service: Some(ReliableSourceServiceStagingContext {
                    allows_product_envelope: true,
                    has_latency_pressure: false,
                    has_feed_evidence: false,
                }),
            },
            FlowLane::Throughput,
            0,
            0,
            payload,
            mux_limits,
        ),
        reservoir,
        "product progress may bootstrap the bounded feed reservoir but cannot unlock the product envelope"
    );
}

#[test]
fn authoritative_ack_snapshot_does_not_regress_on_stale_or_incomplete_ack() {
    let mut frontier = 0;
    let mut ranges = Vec::new();
    let mut complete = false;
    update_repair_authoritative_ack_snapshot(
        &mut frontier,
        &mut ranges,
        &mut complete,
        true,
        &[OffsetRange { start: 0, end: 128 }],
    );
    update_repair_authoritative_ack_snapshot(
        &mut frontier,
        &mut ranges,
        &mut complete,
        true,
        &[OffsetRange { start: 0, end: 64 }],
    );
    update_repair_authoritative_ack_snapshot(
        &mut frontier,
        &mut ranges,
        &mut complete,
        false,
        &[OffsetRange {
            start: 192,
            end: 256,
        }],
    );

    assert_eq!(frontier, 128);
    assert_eq!(ranges, vec![OffsetRange { start: 0, end: 128 }]);
    assert!(complete);
}

#[test]
fn persistent_ack_gap_repair_limit_uses_critical_event_quantum() {
    let limits = MuxLimits::default();
    let base_limit = BBR_MAX_SEND_QUANTUM_BYTES.min(reliable_relay_buffer_len(limits));
    let repair_debt = base_limit.saturating_mul(32);

    let repair_limit = reliable_critical_tail_repair_limit_bytes(base_limit, repair_debt, limits);

    assert_eq!(
        repair_limit, base_limit,
        "persistent ACK-gap repair may bypass optional budget, but one event repairs only one bounded quantum"
    );
}

#[test]
fn persistent_tcp_bulk_ack_gap_repair_uses_one_service_flight() {
    let limits = MuxLimits::default();
    let tcp = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 500.0, 400_000_000.0);
    let repair_debt = limits.max_repair_bytes;
    let base_limit = adaptive_reliable_relay_repair_bytes(Some(tcp), FlowLane::Throughput, limits);
    let service_flight =
        adaptive_reliable_relay_inflight_bytes(Some(tcp), FlowLane::Throughput, limits);
    let repair_limit = reliable_persistent_ack_gap_repair_limit_bytes(
        Some(tcp),
        Some(UnderlayProtocol::Tcp),
        FlowLane::Throughput,
        repair_debt,
        limits,
    );

    assert_eq!(
        repair_limit,
        reliable_critical_tail_repair_limit_bytes(
            service_flight.max(base_limit),
            repair_debt,
            limits,
        )
    );
    assert!(
        repair_limit > base_limit,
        "a proven bulk TCP owner failure must not refill a large ordered hole one 64 KiB quantum per three-PTO interval"
    );
}

#[test]
fn persistent_tcp_bulk_ack_gap_repair_uses_remaining_service_headroom() {
    let limits = MuxLimits::default();
    let mut tcp = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 500.0, 400_000_000.0);
    let service_flight =
        adaptive_reliable_relay_inflight_bytes(Some(tcp), FlowLane::Throughput, limits);
    let remaining = 32 * 1024;
    tcp.product_bytes_in_flight = service_flight.saturating_sub(remaining) as u64;

    assert_eq!(
        reliable_persistent_ack_gap_repair_limit_bytes(
            Some(tcp),
            Some(UnderlayProtocol::Tcp),
            FlowLane::Throughput,
            limits.max_repair_bytes,
            limits,
        ),
        remaining,
        "a persistent repair event may fill only the selected output's remaining modeled service flight"
    );

    tcp.queue_bytes = service_flight as u64;
    assert_eq!(
        reliable_persistent_ack_gap_repair_limit_bytes(
            Some(tcp),
            Some(UnderlayProtocol::Tcp),
            FlowLane::Throughput,
            limits.max_repair_bytes,
            limits,
        ),
        0,
        "overlapping product/carrier queue debt at the service target blocks another amplified batch"
    );
}

#[test]
fn persistent_ack_gap_repair_keeps_udp_and_latency_event_bounded() {
    let limits = MuxLimits::default();
    let repair_debt = limits.max_repair_bytes;
    for (underlay, lane) in [
        (UnderlayProtocol::Udp, FlowLane::Throughput),
        (UnderlayProtocol::Tcp, FlowLane::Latency),
    ] {
        let path = PathSnapshot::new(PathId(1), underlay, 500.0, 400_000_000.0);
        let base_limit = adaptive_reliable_relay_repair_bytes(Some(path), lane, limits);
        assert_eq!(
            reliable_persistent_ack_gap_repair_limit_bytes(
                Some(path),
                Some(underlay),
                lane,
                repair_debt,
                limits,
            ),
            reliable_critical_tail_repair_limit_bytes(base_limit, repair_debt, limits,),
            "UDP/QUIC loss recovery and latency traffic retain one bounded product-repair event"
        );
    }
}

#[test]
fn persistent_ack_gap_repair_limit_ignores_optional_budget_exhaustion() {
    let limits = MuxLimits::default();
    let base_limit = BBR_MAX_SEND_QUANTUM_BYTES.min(reliable_relay_buffer_len(limits));
    let small_tail = base_limit.saturating_sub(1024).max(1);

    let repair_limit = reliable_critical_tail_repair_limit_bytes(base_limit, small_tail, limits);

    assert_eq!(
        repair_limit, small_tail,
        "persistent ACK-gap repair is correctness repair and must not depend on optional duplicate/probe budget"
    );
    assert_eq!(
        reliable_critical_tail_repair_limit_bytes(
            limits.max_repair_bytes.saturating_add(base_limit),
            limits.max_repair_bytes.saturating_add(base_limit),
            limits
        ),
        limits.max_repair_bytes.min(limits.max_path_flight_bytes),
        "persistent ACK-gap repair remains bounded by configured repair/path-flight caps"
    );
}

#[test]
fn final_tail_critical_repair_limit_can_exceed_optional_budget() {
    let limits = MuxLimits::default();
    let base_limit = BBR_MAX_SEND_QUANTUM_BYTES.min(reliable_relay_buffer_len(limits));
    let resource_cap = limits.max_repair_bytes.min(limits.max_path_flight_bytes);
    let small_tail = base_limit.saturating_sub(1024).max(1);
    let repair_debt = base_limit.saturating_mul(8);

    assert_eq!(
        reliable_critical_tail_repair_limit_bytes(base_limit, small_tail, limits),
        small_tail,
        "terminal owner-tail repair may close a retained final tail even after optional repair budget is exhausted"
    );

    assert_eq!(
        reliable_critical_tail_repair_limit_bytes(base_limit, repair_debt, limits),
        base_limit,
        "terminal owner-tail repair keeps a bounded critical path for final stream closure"
    );
    assert_eq!(
        reliable_critical_tail_repair_limit_bytes(
            resource_cap.saturating_add(base_limit),
            resource_cap.saturating_add(base_limit),
            limits
        ),
        resource_cap,
        "critical final-tail repair remains bounded by configured repair resources"
    );
}

#[test]
fn ack_gap_repair_still_repairs_authoritative_ack_gap() {
    let limits = MuxLimits::default();
    let mut send_stream = ReliableSendStream::new(StreamId(9), limits);
    send_stream
        .send_data(Bytes::from_static(&[7; 4096]), StreamFlags::NONE)
        .expect("send stream data");

    let repair_frames = stream_ack_gap_repair_frames(
        &send_stream,
        &[
            OffsetRange {
                start: 0,
                end: 1024,
            },
            OffsetRange {
                start: 2048,
                end: 4096,
            },
        ],
        4096,
        true,
        true,
        true,
    );

    assert_eq!(repair_frames.len(), 1);
    assert_eq!(
        reliable_stream_frame_extent(&repair_frames[0]),
        Some((1024, 2048, 1024))
    );
}

#[test]
fn final_offset_tail_repair_can_recover_unacked_terminal_tail() {
    let limits = MuxLimits::default();
    let mut send_stream = ReliableSendStream::new(StreamId(9), limits);
    send_stream
        .send_data(Bytes::from_static(&[7; 4096]), StreamFlags::NONE)
        .expect("send stream data");

    let repair_frames = stream_final_offset_tail_repair_frames(
        &send_stream,
        &[OffsetRange {
            start: 0,
            end: 1024,
        }],
        4096,
        true,
        true,
    );

    assert_eq!(repair_frames.len(), 1);
    assert_eq!(
        reliable_stream_frame_extent(&repair_frames[0]),
        Some((1024, 4096, 3072))
    );
}

#[test]
fn final_offset_tail_repair_can_use_service_when_no_alternate_survives() {
    let limits = MuxLimits::default();
    let mut send_stream = ReliableSendStream::new(StreamId(9), limits);
    send_stream
        .send_data(Bytes::from_static(&[7; 4096]), StreamFlags::NONE)
        .expect("send stream data");

    let repair_frames = stream_final_offset_tail_repair_frames(
        &send_stream,
        &[OffsetRange {
            start: 0,
            end: 1024,
        }],
        4096,
        true,
        true,
    );

    assert_eq!(repair_frames.len(), 1);
    assert_eq!(
        reliable_stream_frame_extent(&repair_frames[0]),
        Some((1024, 4096, 3072)),
        "terminal final-tail RepairData is connection completion traffic and may use the Service survivor after stall evidence"
    );
}

#[test]
fn final_offset_tail_repair_can_recover_tail_with_no_ack_frontier() {
    let limits = MuxLimits::default();
    let mut send_stream = ReliableSendStream::new(StreamId(9), limits);
    send_stream
        .send_data(Bytes::from_static(&[7; 4096]), StreamFlags::NONE)
        .expect("send stream data");

    let repair_frames = stream_final_offset_tail_repair_frames(&send_stream, &[], 4096, true, true);

    assert_eq!(repair_frames.len(), 1);
    assert_eq!(
        reliable_stream_frame_extent(&repair_frames[0]),
        Some((0, 4096, 4096)),
        "a closed stream with no response ACK frontier must be able to repair the retained owner tail from offset zero"
    );
}

#[test]
fn final_offset_tail_repair_waits_for_persistent_stall_evidence() {
    let limits = MuxLimits::default();
    let mut send_stream = ReliableSendStream::new(StreamId(9), limits);
    send_stream
        .send_data(Bytes::from_static(&[7; 4096]), StreamFlags::NONE)
        .expect("send stream data");

    let repair_frames = stream_final_offset_tail_repair_frames(
        &send_stream,
        &[OffsetRange {
            start: 0,
            end: 1024,
        }],
        4096,
        true,
        false,
    );

    assert!(
        repair_frames.is_empty(),
        "known final offset is not enough to reinject a contiguous owner tail before persistent stall/failure evidence"
    );
}

#[test]
fn ack_gap_repair_progress_keeps_growing_hole_identity() {
    let mut progress = ReliableAckGapRepairProgress::default();
    let first = [
        OffsetRange {
            start: 0,
            end: 110_098,
        },
        OffsetRange {
            start: 112_318,
            end: 114_538,
        },
    ];
    let grown = [
        OffsetRange {
            start: 0,
            end: 110_098,
        },
        OffsetRange {
            start: 113_428,
            end: 116_758,
        },
    ];
    let now = Instant::now();
    let interval = reliable_stream_recv_progress_interval(None, FlowLane::Throughput);
    let repair_delay = reliable_ack_gap_repair_delay(None, FlowLane::Throughput);

    assert!(!progress.repair_ready_at(
        true,
        &first,
        Some(UnderlayProtocol::Udp),
        true,
        repair_delay,
        now,
    ));
    assert!(!progress.repair_ready_at(
        true,
        &grown,
        Some(UnderlayProtocol::Udp),
        true,
        repair_delay,
        now + interval,
    ));
    assert!(
        progress.repair_ready_at(
            true,
            &grown,
            Some(UnderlayProtocol::Udp),
            true,
            repair_delay,
            now + repair_delay,
        ),
        "a growing ACK horizon with the same missing frontier is one persistent hole"
    );
    assert!(
        progress.repair_ready_at(
            true,
            &grown,
            Some(UnderlayProtocol::Udp),
            true,
            repair_delay,
            now + repair_delay + Duration::from_millis(1),
        ),
        "a ready gap is not throttled until at least one repair frame actually queues"
    );
    progress.record_repair_queued_at(now + repair_delay + Duration::from_millis(1));
    assert!(!progress.repair_ready_at(
        true,
        &grown,
        Some(UnderlayProtocol::Udp),
        true,
        repair_delay,
        now + repair_delay + Duration::from_millis(2),
    ));
}

#[test]
fn ack_gap_repair_progress_resets_when_frontier_advances() {
    let mut progress = ReliableAckGapRepairProgress::default();
    let first = [
        OffsetRange {
            start: 0,
            end: 1024,
        },
        OffsetRange {
            start: 4096,
            end: 8192,
        },
    ];
    let advanced = [
        OffsetRange {
            start: 0,
            end: 2048,
        },
        OffsetRange {
            start: 4096,
            end: 8192,
        },
    ];
    let now = Instant::now();
    let repair_delay = reliable_ack_gap_repair_delay(None, FlowLane::Throughput);

    assert!(!progress.repair_ready_at(
        true,
        &first,
        Some(UnderlayProtocol::Udp),
        true,
        repair_delay,
        now,
    ));
    assert!(!progress.repair_ready_at(
        true,
        &advanced,
        Some(UnderlayProtocol::Udp),
        true,
        repair_delay,
        now + repair_delay,
    ));
    assert!(progress.repair_ready_at(
        true,
        &advanced,
        Some(UnderlayProtocol::Udp),
        true,
        repair_delay,
        now + repair_delay + repair_delay,
    ));
}

#[test]
fn ack_gap_repair_waits_for_persistent_gap_on_reliable_carriers() {
    let ranges = [
        OffsetRange {
            start: 0,
            end: 64 * 1024,
        },
        OffsetRange {
            start: 128 * 1024,
            end: 192 * 1024,
        },
    ];
    let now = Instant::now();
    let repair_delay = reliable_relay_stall_timeout(None, FlowLane::Throughput)
        .saturating_mul(QUIC_PERSISTENT_CONGESTION_THRESHOLD);

    for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
        let mut progress = ReliableAckGapRepairProgress::default();
        assert!(!progress.repair_ready_at(true, &ranges, Some(underlay), true, repair_delay, now,));
        assert!(!progress.repair_ready_at(
            true,
            &ranges,
            Some(underlay),
            true,
            repair_delay,
            now + repair_delay - Duration::from_millis(1),
        ));
        assert!(
            progress.repair_ready_at(
                true,
                &ranges,
                Some(underlay),
                true,
                repair_delay,
                now + repair_delay,
            ),
            "{underlay:?} product repair should wait for a persistent ordered-stream gap",
        );
        progress.record_repair_queued_at(now + repair_delay);
        assert!(!progress.repair_ready_at(
            true,
            &ranges,
            Some(underlay),
            true,
            repair_delay,
            now + repair_delay + Duration::from_millis(1),
        ));
        progress.release_repair_attempt();
        assert!(
            progress.repair_ready_at(
                true,
                &ranges,
                Some(underlay),
                true,
                repair_delay,
                now + repair_delay + Duration::from_millis(1),
            ),
            "cancelling a queued batch makes the already-persistent gap immediately replannable",
        );
    }
}
