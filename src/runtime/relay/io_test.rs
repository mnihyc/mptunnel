use super::*;
use crate::model::capacity::{
    MAX_RELIABLE_SERVICE_QUANTUM_BYTES, PATH_OPEN_SCORE_BYTES,
    QUIC_PERSISTENT_CONGESTION_THRESHOLD, adaptive_reliable_relay_inflight_bytes,
    adaptive_reliable_relay_reinjection_bytes, reliable_bulk_carrier_feed_quantum_bytes,
    reliable_relay_buffer_len,
};
use crate::model::path::{CarrierPathInstanceId, RelayPathInstance, RelayPathKey};
use crate::model::timing::{
    reliable_data_ack_gap_persistence_interval, reliable_data_retransmission_interval,
    transport_pto_from_snapshot,
};
use crate::model::work::{
    reliable_critical_tail_reinjection_limit_bytes,
    reliable_failed_original_reinjection_limit_bytes,
    reliable_persistent_ack_gap_reinjection_limit_bytes,
};
use crate::mux::MuxLimits;
use crate::protocol::frame::reliable_stream_frame_extent;
use crate::protocol::{PathId, StreamId};
use crate::runtime::stream::reliable_stream_recv_progress_interval;
use crate::scheduler::TrafficClass;

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
        .receive_data(0, Bytes::from_static(b"hello"))
        .expect("tail data");

    assert!(pending_stream_fin_ready(&recv_stream, pending_final_offset));
}

#[test]
fn terminal_fin_replay_is_independent_of_payload_ack_progress() {
    assert!(!stream_terminal_fin_replay_required(false, false, true));
    assert!(!stream_terminal_fin_replay_required(true, true, true));
    assert!(!stream_terminal_fin_replay_required(true, false, false));
    assert!(stream_terminal_fin_replay_required(true, false, true));
}

#[test]
fn duplicate_stream_data_below_final_frontier_is_already_delivered() {
    let mut recv_stream = ReliableRecvStream::new(StreamId(1), MuxLimits::default());
    recv_stream
        .receive_data(0, Bytes::from_static(b"hello"))
        .expect("receive data");

    assert!(stream_data_range_already_delivered(&recv_stream, 0, 5));
    assert!(!stream_data_range_already_delivered(&recv_stream, 0, 6));
    assert!(!stream_data_range_already_delivered(&recv_stream, 5, 1));
}

#[test]
fn ack_gap_reinjection_requires_multipath_alternative_and_persistent_gap() {
    assert!(!stream_ack_gap_reinjection_allowed(true, false, true));
    assert!(!stream_ack_gap_reinjection_allowed(true, true, false));
    assert!(stream_ack_gap_reinjection_allowed(true, true, true));
    assert!(!stream_ack_gap_reinjection_allowed(false, true, true));
}

#[test]
fn ack_gap_reinjection_requires_authoritative_ack_gap_shape() {
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
fn authoritative_ack_snapshot_does_not_regress_on_stale_or_incomplete_ack() {
    let mut frontier = 0;
    let mut ranges = Vec::new();
    let mut complete = false;
    update_reinjection_authoritative_ack_snapshot(
        &mut frontier,
        &mut ranges,
        &mut complete,
        true,
        &[OffsetRange { start: 0, end: 128 }],
    );
    update_reinjection_authoritative_ack_snapshot(
        &mut frontier,
        &mut ranges,
        &mut complete,
        true,
        &[OffsetRange { start: 0, end: 64 }],
    );
    update_reinjection_authoritative_ack_snapshot(
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
fn authoritative_gap_persistence_ignores_an_older_complete_snapshot() {
    let mut frontier = 0;
    let mut ranges = Vec::new();
    let mut complete = false;
    let current = [
        OffsetRange {
            start: 0,
            end: 4096,
        },
        OffsetRange {
            start: 8192,
            end: 16_384,
        },
    ];
    let stale = [
        OffsetRange {
            start: 0,
            end: 2048,
        },
        OffsetRange {
            start: 8192,
            end: 12_288,
        },
    ];
    let now = Instant::now();
    let persistence = Duration::from_millis(300);
    let mut progress = ReliableAckGapReinjectionProgress::default();

    update_reinjection_authoritative_ack_snapshot(
        &mut frontier,
        &mut ranges,
        &mut complete,
        true,
        &current,
    );
    assert!(!progress.reinjection_ready_at(
        complete,
        &ranges,
        Some(UnderlayProtocol::Tcp),
        true,
        persistence,
        now,
    ));

    update_reinjection_authoritative_ack_snapshot(
        &mut frontier,
        &mut ranges,
        &mut complete,
        true,
        &stale,
    );
    assert_eq!(frontier, 4096);
    assert_eq!(ranges.as_slice(), current.as_slice());
    assert!(progress.reinjection_ready_at(
        complete,
        &ranges,
        Some(UnderlayProtocol::Tcp),
        true,
        persistence,
        now + persistence,
    ));
}

#[test]
fn persistent_ack_gap_reinjection_limit_uses_critical_event_quantum() {
    let limits = MuxLimits::default();
    let base_limit = MAX_RELIABLE_SERVICE_QUANTUM_BYTES.min(reliable_relay_buffer_len(limits));
    let reinjection_debt = base_limit.saturating_mul(32);

    let reinjection_limit =
        reliable_critical_tail_reinjection_limit_bytes(base_limit, reinjection_debt, limits);

    assert_eq!(
        reinjection_limit, base_limit,
        "persistent ACK-gap reinjection may bypass optional budget, but one event reinjections only one bounded quantum"
    );
}

#[test]
fn persistent_tcp_data_ack_gap_reinjection_uses_one_target_flight() {
    let limits = MuxLimits::default();
    let tcp = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 500.0, 400_000_000.0);
    let reinjection_debt = limits.max_repair_bytes;
    let base_limit =
        adaptive_reliable_relay_reinjection_bytes(Some(tcp), TrafficClass::Throughput, limits);
    let target_flight =
        adaptive_reliable_relay_inflight_bytes(Some(tcp), TrafficClass::Throughput, limits);
    let reinjection_limit = reliable_persistent_ack_gap_reinjection_limit_bytes(
        Some(tcp),
        Some(UnderlayProtocol::Tcp),
        TrafficClass::Throughput,
        reinjection_debt,
        limits,
    );

    assert_eq!(
        reinjection_limit,
        reliable_critical_tail_reinjection_limit_bytes(
            target_flight.max(base_limit),
            reinjection_debt,
            limits,
        )
    );
    assert!(
        reinjection_limit > base_limit,
        "a proven bulk TCP original-transmission path failure must not refill a large ordered hole one 64 KiB quantum per three-PTO interval"
    );
}

#[test]
fn persistent_tcp_bulk_ack_gap_reinjection_retains_failure_quantum() {
    let limits = MuxLimits::default();
    let mut tcp = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 500.0, 400_000_000.0);
    let target_flight =
        adaptive_reliable_relay_inflight_bytes(Some(tcp), TrafficClass::Throughput, limits);
    let base_limit =
        adaptive_reliable_relay_reinjection_bytes(Some(tcp), TrafficClass::Throughput, limits);
    let remaining = 32 * 1024;
    tcp.data_level_bytes_in_flight = target_flight.saturating_sub(remaining) as u64;

    assert_eq!(
        reliable_persistent_ack_gap_reinjection_limit_bytes(
            Some(tcp),
            Some(UnderlayProtocol::Tcp),
            TrafficClass::Throughput,
            limits.max_repair_bytes,
            limits,
        ),
        remaining.max(base_limit),
        "failure reinjection fills modeled target path headroom without dropping below one bounded quantum"
    );

    tcp.queue_bytes = target_flight as u64;
    assert_eq!(
        reliable_persistent_ack_gap_reinjection_limit_bytes(
            Some(tcp),
            Some(UnderlayProtocol::Tcp),
            TrafficClass::Throughput,
            limits.max_repair_bytes,
            limits,
        ),
        base_limit,
        "stale alternate-flight debt must not suppress all recovery after a proven TCP original-transmission path failure"
    );
}

#[test]
fn failed_original_reinjection_uses_available_target_flight() {
    let limits = MuxLimits::default();
    let mut tcp = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 500.0, 400_000_000.0);
    let base_limit = reliable_bulk_carrier_feed_quantum_bytes(limits).max(
        adaptive_reliable_relay_reinjection_bytes(Some(tcp), TrafficClass::Throughput, limits),
    );
    let target_flight =
        adaptive_reliable_relay_inflight_bytes(Some(tcp), TrafficClass::Throughput, limits);
    tcp.data_level_bytes_in_flight = target_flight as u64;
    tcp.queue_bytes = target_flight as u64;
    tcp.carrier_inflight_limit_bytes = PATH_OPEN_SCORE_BYTES as u64;
    let congested_recovery_flight =
        adaptive_reliable_relay_inflight_bytes(Some(tcp), TrafficClass::Throughput, limits);

    assert_eq!(
        reliable_failed_original_reinjection_limit_bytes(
            Some(tcp),
            limits.max_repair_bytes,
            limits,
        ),
        reliable_critical_tail_reinjection_limit_bytes(base_limit, limits.max_repair_bytes, limits,),
        "a full target retains one product work quantum while native congestion gates emission",
    );
    assert_eq!(
        congested_recovery_flight, target_flight,
        "carrier queue and congestion credit gate emission below MPP; they do not shrink the retained-data recovery window a second time"
    );
}

#[test]
fn failed_original_reinjection_is_transport_neutral_above_native_congestion() {
    let limits = MuxLimits::default();
    for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
        let path = PathSnapshot::new(PathId(1), underlay, 40.0, 400_000_000.0);
        let target_flight =
            adaptive_reliable_relay_inflight_bytes(Some(path), TrafficClass::Throughput, limits);
        assert_eq!(
            reliable_failed_original_reinjection_limit_bytes(
                Some(path),
                limits.max_repair_bytes,
                limits,
            ),
            reliable_critical_tail_reinjection_limit_bytes(
                target_flight,
                limits.max_repair_bytes,
                limits,
            ),
            "product reinjection is unified while each target retains native admission"
        );
    }
}

#[test]
fn ack_gap_reinjection_waits_for_a_persistent_carrier_recovery_gap() {
    assert_eq!(
        reliable_data_retransmission_interval(Some(UnderlayProtocol::Tcp), None),
        Duration::from_secs(1),
    );
    assert_eq!(
        reliable_data_retransmission_interval(Some(UnderlayProtocol::Udp), None),
        transport_pto_from_snapshot(None),
        "TCP data-level recovery and QUIC PTO must not share one timer formula",
    );
    assert_eq!(
        reliable_data_ack_gap_persistence_interval(Some(UnderlayProtocol::Tcp), None),
        Duration::from_secs(3),
    );
    assert_eq!(
        reliable_data_ack_gap_persistence_interval(Some(UnderlayProtocol::Udp), None),
        transport_pto_from_snapshot(None).saturating_mul(3),
    );
}

#[test]
fn persistent_ack_gap_reinjection_keeps_udp_event_bounded() {
    let limits = MuxLimits::default();
    let reinjection_debt = limits.max_repair_bytes;
    let path = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 500.0, 400_000_000.0);
    let base_limit =
        adaptive_reliable_relay_reinjection_bytes(Some(path), TrafficClass::Throughput, limits);
    assert_eq!(
        reliable_persistent_ack_gap_reinjection_limit_bytes(
            Some(path),
            Some(UnderlayProtocol::Udp),
            TrafficClass::Throughput,
            reinjection_debt,
            limits,
        ),
        reliable_critical_tail_reinjection_limit_bytes(base_limit, reinjection_debt, limits,),
        "QUIC original-transmission path loss retains one bounded product-reinjection event"
    );
}

#[test]
fn persistent_tcp_ack_gap_reinjection_uses_path_model_after_flow_demotes() {
    let limits = MuxLimits::default();
    let reinjection_debt = limits.max_repair_bytes;
    let path = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 500.0, 400_000_000.0);
    let base_limit =
        adaptive_reliable_relay_reinjection_bytes(Some(path), TrafficClass::Latency, limits);
    let target_flight =
        adaptive_reliable_relay_inflight_bytes(Some(path), TrafficClass::Throughput, limits);
    let reinjection_limit = reliable_persistent_ack_gap_reinjection_limit_bytes(
        Some(path),
        Some(UnderlayProtocol::Tcp),
        TrafficClass::Latency,
        reinjection_debt,
        limits,
    );

    assert_eq!(
        reinjection_limit,
        reliable_critical_tail_reinjection_limit_bytes(
            target_flight.max(base_limit),
            reinjection_debt,
            limits,
        )
    );
    assert!(reinjection_limit > base_limit);
}

#[test]
fn persistent_ack_gap_reinjection_limit_ignores_optional_budget_exhaustion() {
    let limits = MuxLimits::default();
    let base_limit = MAX_RELIABLE_SERVICE_QUANTUM_BYTES.min(reliable_relay_buffer_len(limits));
    let small_tail = base_limit.saturating_sub(1024).max(1);

    let reinjection_limit =
        reliable_critical_tail_reinjection_limit_bytes(base_limit, small_tail, limits);

    assert_eq!(
        reinjection_limit, small_tail,
        "persistent ACK-gap reinjection is correctness reinjection and must not depend on optional duplicate/probe budget"
    );
    assert_eq!(
        reliable_critical_tail_reinjection_limit_bytes(
            limits.max_repair_bytes.saturating_add(base_limit),
            limits.max_repair_bytes.saturating_add(base_limit),
            limits
        ),
        limits.max_repair_bytes.min(limits.max_path_flight_bytes),
        "persistent ACK-gap reinjection remains bounded by configured reinjection/path-flight caps"
    );
}

#[test]
fn final_tail_critical_reinjection_limit_can_exceed_optional_budget() {
    let limits = MuxLimits::default();
    let base_limit = MAX_RELIABLE_SERVICE_QUANTUM_BYTES.min(reliable_relay_buffer_len(limits));
    let resource_cap = limits.max_repair_bytes.min(limits.max_path_flight_bytes);
    let small_tail = base_limit.saturating_sub(1024).max(1);
    let reinjection_debt = base_limit.saturating_mul(8);

    assert_eq!(
        reliable_critical_tail_reinjection_limit_bytes(base_limit, small_tail, limits),
        small_tail,
        "terminal original-transmission path-tail reinjection may close a retained final tail even after optional reinjection budget is exhausted"
    );

    assert_eq!(
        reliable_critical_tail_reinjection_limit_bytes(base_limit, reinjection_debt, limits),
        base_limit,
        "terminal original-transmission path-tail reinjection keeps a bounded critical path for final stream closure"
    );
    assert_eq!(
        reliable_critical_tail_reinjection_limit_bytes(
            resource_cap.saturating_add(base_limit),
            resource_cap.saturating_add(base_limit),
            limits
        ),
        resource_cap,
        "critical final-tail reinjection remains bounded by configured reinjection resources"
    );
}

#[test]
fn ack_gap_reinjection_still_reinjections_authoritative_ack_gap() {
    let limits = MuxLimits::default();
    let mut send_stream = ReliableSendStream::new(StreamId(9), limits);
    send_stream
        .send_data(Bytes::from_static(&[7; 4096]))
        .expect("send stream data");

    let reinjection_frames = stream_ack_gap_reinjection_frames(
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

    assert_eq!(reinjection_frames.len(), 1);
    assert_eq!(
        reliable_stream_frame_extent(&reinjection_frames[0]),
        Some((1024, 2048, 1024))
    );
}

#[test]
fn final_offset_tail_reinjection_can_recover_unacked_terminal_tail() {
    let limits = MuxLimits::default();
    let mut send_stream = ReliableSendStream::new(StreamId(9), limits);
    send_stream
        .send_data(Bytes::from_static(&[7; 4096]))
        .expect("send stream data");

    let reinjection_frames = stream_final_offset_tail_reinjection_frames_normalized(
        &send_stream,
        &[OffsetRange {
            start: 0,
            end: 1024,
        }],
        4096,
        true,
        true,
    );

    assert_eq!(reinjection_frames.len(), 1);
    assert_eq!(
        reliable_stream_frame_extent(&reinjection_frames[0]),
        Some((1024, 4096, 3072))
    );
}

#[test]
fn final_offset_tail_reinjection_can_use_only_available_path() {
    let limits = MuxLimits::default();
    let mut send_stream = ReliableSendStream::new(StreamId(9), limits);
    send_stream
        .send_data(Bytes::from_static(&[7; 4096]))
        .expect("send stream data");

    let reinjection_frames = stream_final_offset_tail_reinjection_frames_normalized(
        &send_stream,
        &[OffsetRange {
            start: 0,
            end: 1024,
        }],
        4096,
        true,
        true,
    );

    assert_eq!(reinjection_frames.len(), 1);
    assert_eq!(
        reliable_stream_frame_extent(&reinjection_frames[0]),
        Some((1024, 4096, 3072)),
        "terminal final-tail reinjection may use the only available path after stall evidence"
    );
}

#[test]
fn final_offset_tail_reinjection_can_recover_tail_with_no_ack_frontier() {
    let limits = MuxLimits::default();
    let mut send_stream = ReliableSendStream::new(StreamId(9), limits);
    send_stream
        .send_data(Bytes::from_static(&[7; 4096]))
        .expect("send stream data");

    let reinjection_frames =
        stream_final_offset_tail_reinjection_frames_normalized(&send_stream, &[], 4096, true, true);

    assert_eq!(reinjection_frames.len(), 1);
    assert_eq!(
        reliable_stream_frame_extent(&reinjection_frames[0]),
        Some((0, 4096, 4096)),
        "a closed stream with no response ACK frontier must be able to reinjection the retained original-transmission path tail from offset zero"
    );
}

#[test]
fn final_offset_tail_reinjection_waits_for_persistent_stall_evidence() {
    let limits = MuxLimits::default();
    let mut send_stream = ReliableSendStream::new(StreamId(9), limits);
    send_stream
        .send_data(Bytes::from_static(&[7; 4096]))
        .expect("send stream data");

    let reinjection_frames = stream_final_offset_tail_reinjection_frames_normalized(
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
        reinjection_frames.is_empty(),
        "known final offset is not enough to reinject a contiguous original-transmission path tail before persistent stall/failure evidence"
    );
}

#[test]
fn ack_gap_reinjection_progress_keeps_growing_hole_identity() {
    let mut progress = ReliableAckGapReinjectionProgress::default();
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
    let interval = reliable_stream_recv_progress_interval(None);
    let reinjection_delay =
        reliable_data_ack_gap_persistence_interval(Some(UnderlayProtocol::Udp), None);

    assert!(!progress.reinjection_ready_at(
        true,
        &first,
        Some(UnderlayProtocol::Udp),
        true,
        reinjection_delay,
        now,
    ));
    assert!(!progress.reinjection_ready_at(
        true,
        &grown,
        Some(UnderlayProtocol::Udp),
        true,
        reinjection_delay,
        now + interval,
    ));
    assert!(
        progress.reinjection_ready_at(
            true,
            &grown,
            Some(UnderlayProtocol::Udp),
            true,
            reinjection_delay,
            now + reinjection_delay,
        ),
        "a growing ACK horizon with the same missing frontier is one persistent hole"
    );
    assert!(
        progress.reinjection_ready_at(
            true,
            &grown,
            Some(UnderlayProtocol::Udp),
            true,
            reinjection_delay,
            now + reinjection_delay + Duration::from_millis(1),
        ),
        "a ready gap is not throttled until at least one reinjection frame actually queues"
    );
    progress.record_reinjection_queued_at(now + reinjection_delay + Duration::from_millis(1));
    assert!(!progress.reinjection_ready_at(
        true,
        &grown,
        Some(UnderlayProtocol::Udp),
        true,
        reinjection_delay,
        now + reinjection_delay + Duration::from_millis(2),
    ));
}

#[test]
fn ack_gap_reinjection_progress_resets_when_frontier_advances() {
    let mut progress = ReliableAckGapReinjectionProgress::default();
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
    let reinjection_delay =
        reliable_data_ack_gap_persistence_interval(Some(UnderlayProtocol::Udp), None);

    assert!(!progress.reinjection_ready_at(
        true,
        &first,
        Some(UnderlayProtocol::Udp),
        true,
        reinjection_delay,
        now,
    ));
    assert!(!progress.reinjection_ready_at(
        true,
        &advanced,
        Some(UnderlayProtocol::Udp),
        true,
        reinjection_delay,
        now + reinjection_delay,
    ));
    assert!(progress.reinjection_ready_at(
        true,
        &advanced,
        Some(UnderlayProtocol::Udp),
        true,
        reinjection_delay,
        now + reinjection_delay + reinjection_delay,
    ));
}

#[test]
fn ack_gap_reinjection_waits_for_persistent_gap_on_reliable_carriers() {
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
    let reinjection_delay =
        transport_pto_from_snapshot(None).saturating_mul(QUIC_PERSISTENT_CONGESTION_THRESHOLD);

    for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
        let mut progress = ReliableAckGapReinjectionProgress::default();
        assert!(!progress.reinjection_ready_at(
            true,
            &ranges,
            Some(underlay),
            true,
            reinjection_delay,
            now,
        ));
        assert!(!progress.reinjection_ready_at(
            true,
            &ranges,
            Some(underlay),
            true,
            reinjection_delay,
            now + reinjection_delay - Duration::from_millis(1),
        ));
        assert!(
            progress.reinjection_ready_at(
                true,
                &ranges,
                Some(underlay),
                true,
                reinjection_delay,
                now + reinjection_delay,
            ),
            "{underlay:?} product reinjection should wait for a persistent ordered-stream gap",
        );
        progress.record_reinjection_queued_at(now + reinjection_delay);
        assert!(!progress.reinjection_ready_at(
            true,
            &ranges,
            Some(underlay),
            true,
            reinjection_delay,
            now + reinjection_delay + Duration::from_millis(1),
        ));
        progress.release_reinjection_attempt();
        assert!(
            progress.reinjection_ready_at(
                true,
                &ranges,
                Some(underlay),
                true,
                reinjection_delay,
                now + reinjection_delay + Duration::from_millis(1),
            ),
            "cancelling a queued batch makes the already-persistent gap immediately replannable",
        );
    }
}

#[test]
fn request_path_staleness_requires_persistent_missing_data_ack_progress() {
    let path = RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        },
        path_instance_id: CarrierPathInstanceId::from_raw(7),
        attachment_id: 7,
    };
    let now = Instant::now();
    let persistence = Duration::from_millis(300);
    let mut progress = ReliableRequestPathStaleness::default();

    assert_eq!(
        progress.stale_path_at(true, Some(path), false, true, persistence, now),
        None
    );
    assert_eq!(
        progress.stale_path_at(
            true,
            Some(path),
            false,
            true,
            persistence,
            now + persistence - Duration::from_millis(1),
        ),
        None
    );
    assert_eq!(
        progress.stale_path_at(
            true,
            Some(path),
            false,
            true,
            persistence,
            now + persistence,
        ),
        Some(path)
    );
}

#[test]
fn request_path_staleness_resets_on_exact_data_ack_progress() {
    let path = RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 1,
        },
        path_instance_id: CarrierPathInstanceId::from_raw(11),
        attachment_id: 11,
    };
    let now = Instant::now();
    let persistence = Duration::from_millis(200);
    let mut progress = ReliableRequestPathStaleness::default();

    assert_eq!(
        progress.stale_path_at(true, Some(path), false, true, persistence, now),
        None
    );
    assert_eq!(
        progress.stale_path_at(true, Some(path), true, true, persistence, now + persistence,),
        None
    );
    assert_eq!(
        progress.stale_path_at(
            true,
            Some(path),
            false,
            true,
            persistence,
            now + persistence + persistence - Duration::from_millis(1),
        ),
        None
    );
    assert_eq!(
        progress.stale_path_at(
            true,
            Some(path),
            false,
            true,
            persistence,
            now + persistence + persistence,
        ),
        Some(path)
    );
}

#[test]
fn partial_data_ack_does_not_erase_request_path_staleness_evidence() {
    let path = RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 2,
        },
        path_instance_id: CarrierPathInstanceId::from_raw(13),
        attachment_id: 13,
    };
    let now = Instant::now();
    let persistence = Duration::from_millis(100);
    let mut progress = ReliableRequestPathStaleness::default();

    assert_eq!(
        progress.stale_path_at(true, Some(path), false, true, persistence, now),
        None
    );
    assert_eq!(
        progress.stale_path_at(false, None, false, false, persistence, now + persistence,),
        None
    );
    assert_eq!(
        progress.stale_path_at(
            true,
            Some(path),
            false,
            true,
            persistence,
            now + persistence,
        ),
        Some(path)
    );
}
