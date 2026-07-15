use super::*;
use crate::model::capacity::BBR_MAX_SEND_QUANTUM_BYTES;
use crate::model::path::CarrierPathKey;
use crate::model::timing::transport_pto_from_snapshot;
use crate::protocol::frame::stream_ack_contiguous_frontier;
use crate::protocol::{PathId, PathMetricDirection, PathMetrics, StreamFlags, StreamOpenRole};
use crate::runtime::path::commands::{
    ReliablePathCommand, reliable_path_command_channels, reliable_path_stream_ordered_queue_lane,
    try_recv_reliable_path_command,
};
use crate::runtime::path::model::metric_epoch_now;
use crate::runtime::relay::io::stream_ack_ranges_expose_authoritative_gap;
use crate::runtime::stream::response::{
    ResponseStreamAttachOutcome, ResponseStreamBinding, ServerPathMetricsSource,
};
use bytes::Bytes;

#[test]
fn reliable_recv_progress_sends_exact_tcp_sparse_deltas_without_delaying_feedback() {
    let mux_limits = MuxLimits {
        max_ack_ranges: 16,
        max_stream_window_bytes: 64 * 1024,
        max_repair_bytes: 64 * 1024,
        max_reorder_bytes: 64 * 1024,
        max_path_flight_bytes: 64 * 1024,
        max_reliable_relay_chunk_bytes: 64 * 1024,
        ..MuxLimits::default()
    };
    let tcp = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 180.0, 500_000_000.0);
    let udp = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 180.0, 500_000_000.0);

    for (path, request_sparse, compact) in
        [(tcp, true, true), (tcp, false, false), (udp, true, false)]
    {
        let mut recv_stream = ReliableRecvStream::new(StreamId(24), mux_limits);
        let mut progress = ReliableRecvProgress::default();
        let mut sparse_progress = RequestTcpSparseAckProgress::default();
        recv_stream
            .receive_data(0, Bytes::from(vec![0x11; 1024]), StreamFlags::NONE)
            .expect("contiguous prefix");
        assert!(progress.should_send_ack(
            &recv_stream,
            Some(path),
            FlowLane::Throughput,
            mux_limits,
            false,
        ));
        assert_eq!(sparse_progress.ack_frames(&recv_stream, false).len(), 1);

        let mut frames = Vec::new();
        for offset in [8192, 32768, 16384, 12288] {
            recv_stream
                .receive_data(offset, Bytes::from(vec![0x22; 1024]), StreamFlags::NONE)
                .expect("sparse range");
            assert!(
                progress.should_send_ack(
                    &recv_stream,
                    Some(path),
                    FlowLane::Throughput,
                    mux_limits,
                    false,
                ),
                "range-shape feedback cadence must not be weakened"
            );
            frames = sparse_progress.ack_frames(
                &recv_stream,
                request_sparse && path.underlay == UnderlayProtocol::Tcp,
            );
        }
        assert_eq!(frames.len(), 1);
        let Frame::StreamAck {
            complete, ranges, ..
        } = &frames[0]
        else {
            panic!("receive progress must emit STREAM_ACK");
        };
        assert_eq!(*complete, !compact);
        assert_eq!(ranges.len(), if compact { 1 } else { 5 });
        assert_eq!(
            ranges.first().map(|range| range.start),
            Some(if compact { 12288 } else { 0 })
        );
        assert_eq!(
            ranges.last().map(|range| range.start),
            Some(if compact { 12288 } else { 32768 })
        );
        if compact {
            assert_eq!(ranges[0], OffsetRange::new(12288, 13312).unwrap());
        }
    }
}

#[test]
fn tail_stall_repair_retransmits_same_frontier_only_after_stall_evidence() {
    let stream_id = StreamId(34);
    let (commands, _receivers) = reliable_path_command_channels(4);
    let binding = ResponseStreamBinding::new(
        SessionId(34),
        UnderlayProtocol::Tcp,
        PathId(0),
        commands,
        FlowLane::Throughput,
    );
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: 1024,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx,
    };
    let frame = Frame::StreamData {
        stream_id,
        offset: 128,
        flags: StreamFlags::NONE,
        payload: Bytes::from_static(b"frontier"),
    };
    binding.record_owner_flight(
        CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        },
        &frame,
    );
    let later_frame = Frame::StreamData {
        stream_id,
        offset: 136,
        flags: StreamFlags::NONE,
        payload: Bytes::from_static(b"later"),
    };

    let (without_stall, blocked_offset) = prefix_repair_frames_with_available_output(
        &path_stream,
        vec![frame.clone(), later_frame.clone()],
        false,
    );
    assert!(without_stall.is_empty());
    assert_eq!(blocked_offset, Some(128));

    let (with_stall, blocked_offset) =
        prefix_repair_frames_with_available_output(&path_stream, vec![frame, later_frame], true);
    assert_eq!(blocked_offset, None);
    assert_eq!(with_stall.len(), 1);
    assert!(matches!(
        &with_stall[0],
        Frame::StreamData {
            offset: 128,
            payload,
            ..
        } if payload.as_ref() == b"frontier"
    ));
}

#[test]
fn sender_effective_lane_promotes_from_local_or_peer_bulk_evidence() {
    assert_eq!(
        reliable_sender_effective_relay_lane(FlowLane::Latency, FlowLane::Latency),
        FlowLane::Latency
    );
    assert_eq!(
        reliable_sender_effective_relay_lane(FlowLane::Throughput, FlowLane::Latency),
        FlowLane::Throughput
    );
    assert_eq!(
        reliable_sender_effective_relay_lane(FlowLane::Latency, FlowLane::Throughput),
        FlowLane::Throughput
    );
    assert_eq!(
        reliable_sender_effective_relay_lane(FlowLane::Latency, FlowLane::Background),
        FlowLane::Background
    );
}

#[test]
fn response_sender_wait_state_blocks_immediately_without_carrier_credit() {
    let now = tokio::time::Instant::now();
    let retry_delay = Duration::from_millis(10);

    let state = response_sender_wait_state(true, true, false, None, now, retry_delay);

    assert!(state.blocked);
    assert!(!state.ready);
    assert!(state.subscribe_capacity);
    assert_eq!(state.retry_at, Some(now + retry_delay));
}

#[test]
fn response_sender_wait_state_allows_admission_when_carrier_has_credit() {
    let now = tokio::time::Instant::now();
    let retry_delay = Duration::from_millis(10);

    let state = response_sender_wait_state(true, true, true, None, now, retry_delay);

    assert!(!state.blocked);
    assert!(state.ready);
    assert!(
        !state.subscribe_capacity,
        "product-ordering pressure is handled by sender admission, not carrier pipe exhaustion"
    );
    assert_eq!(state.retry_at, None);
}

#[test]
fn response_sender_wait_state_preserves_pending_retry_with_carrier_credit() {
    let now = tokio::time::Instant::now();
    let retry_delay = Duration::from_millis(10);
    let retry_at = now + retry_delay;

    let state = response_sender_wait_state(true, true, true, Some(retry_at), now, retry_delay);

    assert!(state.blocked);
    assert!(!state.ready);
    assert!(state.subscribe_capacity);
    assert_eq!(state.retry_at, Some(retry_at));
}

#[test]
fn tail_timer_repair_allows_only_authoritative_or_failed_owner_repair() {
    assert!(
        stream_tail_timer_repair_allowed(false, true),
        "after the owner output is gone, the remaining output is the failover path even though it is no longer a second live alternative"
    );
    assert!(!stream_tail_timer_repair_allowed(false, false));
    assert!(
        stream_tail_timer_repair_allowed(true, false),
        "authoritative ACK-frontier tail repair may use a live alternate"
    );
}

#[test]
fn contiguous_ack_frontier_lag_is_tail_guard_not_repair_debt() {
    let ranges = [OffsetRange {
        start: 0,
        end: 1024,
    }];

    assert!(
        !stream_ack_ranges_expose_authoritative_gap(true, &ranges),
        "a contiguous unacknowledged suffix is not an authoritative product repair gap"
    );
    assert_eq!(
        reliable_relay_ordered_owner_debt_bytes(FlowLane::Throughput, true, 1024, 8192,),
        7168,
        "a contiguous unacknowledged suffix is a tail guard for alternate owners"
    );
    assert_eq!(
        reliable_relay_ordered_owner_debt_bytes(FlowLane::Throughput, false, 1024, 8192,),
        7168,
        "an incomplete ACK chunk can still prove the contiguous prefix for owner-tail guarding"
    );
    assert_eq!(
        reliable_relay_ordered_owner_debt_bytes(FlowLane::Throughput, false, 0, 8192,),
        8192,
        "before the first contiguous ACK, already-sent bulk bytes are still owner-tail debt for alternate owners"
    );
    assert_eq!(
        reliable_relay_ordered_owner_debt_bytes(FlowLane::Latency, true, 1024, 8192,),
        0,
        "latency traffic must not be pinned by bulk owner-tail pressure"
    );
}

#[test]
fn tail_repair_uses_single_pto_stall_timeout() {
    let last_progress = Instant::now();
    let last_repair = last_progress - Duration::from_secs(1);
    let deadline = reliable_relay_tail_repair_deadline(last_progress, last_repair, None);
    let expected =
        tokio::time::Instant::from_std(last_progress + transport_pto_from_snapshot(None));

    assert_eq!(deadline, expected);
}

#[test]
fn tail_repair_timer_is_lane_neutral_after_stall_evidence() {
    assert!(
        reliable_relay_tail_repair_timer_active(64, true, false),
        "a complete stalled owner suffix must use bounded alternate-output repair in every reliable lane"
    );
    assert!(
        reliable_relay_tail_repair_timer_active(64, false, true),
        "failed-owner correctness repair must not depend on the product lane"
    );
    assert!(
        !reliable_relay_tail_repair_timer_active(64, false, false),
        "an outstanding suffix without an eligible alternate must remain with its carrier"
    );
    assert!(
        !reliable_relay_tail_repair_timer_active(0, true, true),
        "a fully acknowledged stream must not arm the repair timer"
    );
}

#[tokio::test]
async fn latency_live_owner_tail_repair_dispatches_suffix_on_distinct_repair_without_fin() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(118);
    let owner_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let repair_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (owner_commands, mut owner_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(118),
        owner_key.underlay,
        owner_key.path_id,
        owner_commands,
        FlowLane::Latency,
    );
    let (repair_commands, mut repair_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            repair_key.underlay,
            repair_key.path_id,
            repair_commands,
            FlowLane::Latency,
            StreamOpenRole::Repair,
            reliable_relay_buffer_len(limits),
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: FlowLane::Latency,
        underlay: owner_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx,
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    for value in [0x41, 0x42, 0x43] {
        let frame = send_stream
            .send_data(Bytes::from(vec![value; 64]), StreamFlags::NONE)
            .expect("seed owner response data");
        binding.record_owner_flight(owner_key, &frame);
    }
    let ack_ranges = [OffsetRange { start: 0, end: 128 }];
    let _ = send_stream.apply_ack(&ack_ranges);
    assert_eq!(send_stream.next_offset(), 192);
    assert_eq!(send_stream.repair_bytes(), 64);
    assert!(reliable_relay_tail_repair_timer_active(
        send_stream.repair_bytes(),
        true,
        false,
    ));

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(118),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
    );
    response_sender.record_owner_progress(128);
    let outcome = enqueue_reliable_tail_repair(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        FlowLane::Latency,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        128,
    );
    assert_eq!(outcome.queued, 1);
    assert!(!outcome.pending);

    let ordered_owner_debt_bytes = send_stream.repair_bytes();
    let dispatch = response_sender
        .dispatch_next_with_ordered_owner_debt(
            &path_stream,
            &mut send_stream,
            FlowLane::Latency,
            limits,
            ordered_owner_debt_bytes,
        )
        .expect("latency tail repair must dispatch on the distinct Repair output");
    assert_eq!(dispatch.lane, ReliableWorkClass::Repair);
    assert_eq!(dispatch.selected_path, Some(repair_key));

    let repair_frame = match try_recv_reliable_path_command(&mut repair_receivers) {
        Some(ReliablePathCommand::SendFrame(frame)) => {
            assert!(matches!(
                &frame,
                Frame::StreamData {
                    offset: 128,
                    flags,
                    payload,
                    ..
                } if payload.len() == 64 && !flags.fin
            ));
            frame
        }
        _ => panic!("expected the nonterminal 64-byte suffix on Repair"),
    };
    assert!(try_recv_reliable_path_command(&mut owner_receivers).is_none());
    assert_eq!(binding.ordered_data_owner(), Some(owner_key));
    assert!(path_stream.has_recent_live_repair_flight_overlap(
        &repair_frame,
        reliable_relay_tail_repair_delay(None),
    ));
}

#[test]
fn sparse_authoritative_ack_does_not_skip_lower_gap_for_live_tail_repair() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(119);
    let owner_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let repair_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(119),
        owner_key.underlay,
        owner_key.path_id,
        owner_commands,
        FlowLane::Latency,
    );
    let (repair_commands, _repair_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            repair_key.underlay,
            repair_key.path_id,
            repair_commands,
            FlowLane::Latency,
            StreamOpenRole::Repair,
            reliable_relay_buffer_len(limits),
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: FlowLane::Latency,
        underlay: owner_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx,
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    for value in [0x41, 0x42, 0x43, 0x44] {
        let frame = send_stream
            .send_data(Bytes::from(vec![value; 64]), StreamFlags::NONE)
            .expect("seed owner response data");
        binding.record_owner_flight(owner_key, &frame);
    }
    let ack_ranges = [
        OffsetRange { start: 0, end: 64 },
        OffsetRange {
            start: 128,
            end: 192,
        },
    ];
    let _ = send_stream.apply_ack(&ack_ranges);
    assert_eq!(stream_ack_contiguous_frontier(&ack_ranges), 64);
    assert!(!stream_ack_is_authoritative_contiguous_prefix(
        true,
        &ack_ranges,
        64,
    ));
    assert_eq!(send_stream.repair_bytes(), 128);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(119),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
    );
    let outcome = enqueue_reliable_tail_repair(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        FlowLane::Latency,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        64,
    );

    assert_eq!(
        outcome.queued, 0,
        "the live-tail timer must not skip an authoritative lower ACK gap"
    );
    assert!(!outcome.pending);
    assert_eq!(response_sender.bytes(), 0);
}

#[tokio::test]
async fn sparse_ack_failed_owner_repair_starts_at_lowest_hole() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(120);
    let owner_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let repair_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(120),
        owner_key.underlay,
        owner_key.path_id,
        owner_commands.clone(),
        FlowLane::Latency,
    );
    let (repair_commands, mut repair_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            repair_key.underlay,
            repair_key.path_id,
            repair_commands,
            FlowLane::Latency,
            StreamOpenRole::Repair,
            reliable_relay_buffer_len(limits),
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: FlowLane::Latency,
        underlay: owner_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx,
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    for value in [0x51, 0x52, 0x53, 0x54] {
        let frame = send_stream
            .send_data(Bytes::from(vec![value; 64]), StreamFlags::NONE)
            .expect("seed failed-owner response data");
        binding.record_owner_flight(owner_key, &frame);
    }
    let ack_ranges = [
        OffsetRange { start: 0, end: 64 },
        OffsetRange {
            start: 128,
            end: 192,
        },
    ];
    let _ = send_stream.apply_ack(&ack_ranges);
    binding.release_normalized_acked_ranges(&ack_ranges);
    binding.detach(owner_key, &owner_commands);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(120),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
    );
    let outcome = enqueue_reliable_tail_repair(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        FlowLane::Latency,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        64,
    );
    assert!(outcome.queued > 0);
    let ordered_owner_debt_bytes = send_stream.repair_bytes();
    let dispatch = response_sender
        .dispatch_next_with_ordered_owner_debt(
            &path_stream,
            &mut send_stream,
            FlowLane::Latency,
            limits,
            ordered_owner_debt_bytes,
        )
        .expect("dispatch lowest failed-owner hole");
    assert_eq!(dispatch.selected_path, Some(repair_key));
    assert!(matches!(
        try_recv_reliable_path_command(&mut repair_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            offset: 64,
            payload,
            ..
        })) if payload.len() == 64
    ));
}

#[test]
fn tail_repair_repeats_after_persistent_delay_without_progress() {
    let last_progress = Instant::now();
    let last_repair = last_progress + transport_pto_from_snapshot(None);
    let deadline = reliable_relay_tail_repair_deadline(last_progress, last_repair, None);
    let expected = tokio::time::Instant::from_std(
        last_repair
            + transport_pto_from_snapshot(None)
                .saturating_mul(QUIC_PERSISTENT_CONGESTION_THRESHOLD),
    );

    assert_eq!(deadline, expected);
}

#[test]
fn live_tail_repair_timer_uses_blocking_owner_snapshot_not_fast_alternate() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(110);
    let owner_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let fast_alternate = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(110),
        owner_key.underlay,
        owner_key.path_id,
        owner_commands,
        FlowLane::Throughput,
    );
    let (alternate_commands, _alternate_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            fast_alternate.underlay,
            fast_alternate.path_id,
            alternate_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            reliable_relay_buffer_len(limits),
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let slow_owner_metrics = PathMetrics {
        path_id: owner_key.path_id,
        underlay: owner_key.underlay,
        direction: PathMetricDirection::ServerToClient,
        metric_epoch: metric_epoch_now(),
        metric_age_us: 0,
        min_rtt_us: 480_000,
        srtt_us: 500_000,
        rttvar_us: 60_000,
        jitter_us: 60_000,
        delivery_rate_bps: 80_000_000,
        pacing_rate_bps: 80_000_000,
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
        data_sample_count: 1,
        data_sample_bytes: 65_536,
    };
    let fast_alternate_metrics = PathMetrics {
        path_id: fast_alternate.path_id,
        underlay: fast_alternate.underlay,
        min_rtt_us: 20_000,
        srtt_us: 25_000,
        rttvar_us: 2_000,
        jitter_us: 2_000,
        delivery_rate_bps: 200_000_000,
        pacing_rate_bps: 200_000_000,
        ..slow_owner_metrics
    };
    binding.update_path_metrics(
        owner_key,
        slow_owner_metrics,
        ServerPathMetricsSource::LocalSender,
    );
    binding.update_path_metrics(
        fast_alternate,
        fast_alternate_metrics,
        ServerPathMetricsSource::LocalSender,
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: owner_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx,
    };
    let owner_frame = Frame::StreamData {
        stream_id,
        offset: 1024,
        flags: StreamFlags::NONE,
        payload: Bytes::from(vec![0x55; 65_536]),
    };
    binding.record_owner_flight(owner_key, &owner_frame);

    let snapshot = path_stream
        .tail_repair_snapshot(1024, FlowLane::Throughput, 65_536)
        .expect("blocking owner path is still attached");

    assert_eq!(snapshot.id, owner_key.path_id);
    assert_eq!(snapshot.underlay, owner_key.underlay);
    assert!(
        transport_pto_from_snapshot(Some(snapshot))
            > transport_pto_from_snapshot(
                path_stream.send_path_snapshot(FlowLane::Throughput, 65_536)
            ),
        "tail repair timing must follow the blocking OwnerData path, not the fastest attached alternate"
    );
}

#[test]
fn failed_owner_tail_repair_deadline_is_immediate_for_repairable_detached_owner() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(111);
    let owner_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let failover_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(111),
        owner_key.underlay,
        owner_key.path_id,
        owner_commands.clone(),
        FlowLane::Throughput,
    );
    let (failover_commands, _failover_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            failover_key.underlay,
            failover_key.path_id,
            failover_commands,
            FlowLane::Throughput,
            StreamOpenRole::Active,
            reliable_relay_buffer_len(limits),
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: failover_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx,
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x51; 4096]), StreamFlags::NONE)
        .expect("prepare owner data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit owner data");
    binding.record_owner_flight(owner_key, &frame);
    binding.detach(owner_key, &owner_commands);
    let ack_ranges = [OffsetRange {
        start: 0,
        end: 1024,
    }];
    let _ = send_stream.apply_ack(&ack_ranges);

    let last_progress = Instant::now();
    let last_repair = last_progress - Duration::from_secs(1);
    let generic_deadline = reliable_relay_tail_repair_deadline(last_progress, last_repair, None);
    let failover_deadline = reliable_relay_effective_tail_repair_deadline(
        last_progress,
        last_repair,
        None,
        reliable_failed_owner_tail_repair_ready(
            &path_stream,
            &send_stream,
            &ack_ranges,
            true,
            1024,
            limits,
        ),
    );

    assert_eq!(
        failover_deadline,
        tokio::time::Instant::from_std(last_progress),
        "detached-owner tail repair should not wait a generic PTO before failing over"
    );
    assert!(
        failover_deadline < generic_deadline,
        "failed-owner repair timing must be faster than live-owner tail repair"
    );
}

#[test]
fn failed_owner_tail_repair_retry_uses_single_pto_not_persistent_backoff() {
    let last_progress = Instant::now();
    let last_repair = last_progress + Duration::from_millis(1);
    let slow_stale_owner = PathSnapshot::new(PathId(9), UnderlayProtocol::Tcp, 20.0, 1.0);

    let deadline = reliable_relay_effective_tail_repair_deadline(
        last_progress,
        last_repair,
        Some(slow_stale_owner),
        true,
    );
    let expected = tokio::time::Instant::from_std(last_repair + transport_pto_from_snapshot(None));

    assert_eq!(
        deadline, expected,
        "failed-owner repair may fire immediately once, then retries one bounded repair quantum per PTO; persistent backoff is for live owner congestion recovery, not detached-owner failover"
    );
}

#[test]
fn live_tcp_bulk_tail_repair_uses_one_bounded_owner_flight() {
    let limits = MuxLimits::default();
    let path = PathSnapshot::new(PathId(3), UnderlayProtocol::Tcp, 180.0, 500_000_000.0);
    let base = adaptive_reliable_relay_repair_bytes(Some(path), FlowLane::Throughput, limits);
    let repair_debt = limits.max_repair_bytes;
    let repair_limit = reliable_live_owner_tail_repair_limit_bytes(
        Some(path),
        Some(UnderlayProtocol::Tcp),
        FlowLane::Throughput,
        repair_debt,
        limits,
    );

    assert!(repair_limit > base);
    assert!(
        repair_limit <= bulk_service_feed_reservoir_payload_bytes(base, limits),
        "one-PTO TCP reinjection must remain inside the ordered feed reservoir"
    );
}

#[test]
fn live_quic_tail_repair_remains_one_product_quantum() {
    let limits = MuxLimits::default();
    let path = PathSnapshot::new(PathId(3), UnderlayProtocol::Udp, 180.0, 500_000_000.0);
    let base = adaptive_reliable_relay_repair_bytes(Some(path), FlowLane::Throughput, limits);
    assert_eq!(
        reliable_live_owner_tail_repair_limit_bytes(
            Some(path),
            Some(UnderlayProtocol::Udp),
            FlowLane::Throughput,
            limits.max_repair_bytes,
            limits,
        ),
        base,
        "QUIC keeps packet recovery below the shared product repair boundary"
    );
}

#[test]
fn live_tail_stall_repair_is_not_queued_even_with_optional_budget() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(98);
    let base_limit = BBR_MAX_SEND_QUANTUM_BYTES.min(reliable_relay_buffer_len(limits));
    let repair_debt = base_limit.saturating_mul(8);
    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(98),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 1,
        },
    );
    let initial_budget = response_sender.repair_extra_budget_remaining(limits);
    assert!(initial_budget > 0);

    let (commands, _receivers) = reliable_path_command_channels(8);
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::fixed(UnderlayProtocol::Tcp, PathId(0), commands, limits),
        frames: frame_rx,
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    send_stream
        .send_data(Bytes::from(vec![0x32; repair_debt]), StreamFlags::NONE)
        .expect("owner data");
    let ack_frontier = base_limit as u64;

    let outcome = enqueue_reliable_tail_repair(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &[OffsetRange {
            start: 0,
            end: ack_frontier,
        }],
        true,
        None,
        FlowLane::Throughput,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 1,
        },
        path_stream.max_frame_payload_bytes,
        ack_frontier,
    );

    assert_eq!(
        outcome.queued, 0,
        "live contiguous owner-tail bytes are neither ACK-gap nor final-tail correctness repair"
    );
    assert!(!outcome.pending);
    assert!(
        outcome.record_as_repair_attempt(),
        "an empty tail-repair scan must still advance the retry timer"
    );
}

#[test]
fn failed_owner_tail_repair_uses_remaining_output_after_persistent_stall() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(99);
    let owner_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let failover_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(99),
        owner_key.underlay,
        owner_key.path_id,
        owner_commands.clone(),
        FlowLane::Throughput,
    );
    let (failover_commands, _failover_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            failover_key.underlay,
            failover_key.path_id,
            failover_commands,
            FlowLane::Throughput,
            StreamOpenRole::Active,
            reliable_relay_buffer_len(limits),
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: failover_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx,
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x42; 4096]), StreamFlags::NONE)
        .expect("prepare owner data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit owner data");
    binding.record_owner_flight(owner_key, &frame);
    binding.detach(owner_key, &owner_commands);
    let ack_ranges = [OffsetRange {
        start: 0,
        end: 1024,
    }];
    let _ = send_stream.apply_ack(&ack_ranges);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(99),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
    );
    response_sender.record_owner_progress(1024);

    let outcome = enqueue_reliable_tail_repair(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        FlowLane::Throughput,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        1024,
    );

    assert_eq!(
        outcome.queued, 1,
        "a detached owner path turns a persistent contiguous tail into failover repair on the remaining output"
    );
    assert!(!outcome.pending);
}

#[test]
fn failed_owner_tail_repair_queues_single_service_quantum_not_recovery_burst() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(121);
    let owner_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let failover_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(121),
        owner_key.underlay,
        owner_key.path_id,
        owner_commands.clone(),
        FlowLane::Throughput,
    );
    let (failover_commands, _failover_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            failover_key.underlay,
            failover_key.path_id,
            failover_commands,
            FlowLane::Throughput,
            StreamOpenRole::Active,
            reliable_relay_buffer_len(limits),
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: failover_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx,
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let unresolved_payload_len = reliable_relay_buffer_len(limits)
        .saturating_add(BBR_MAX_SEND_QUANTUM_BYTES)
        .min(limits.max_payload_bytes);
    let frame = send_stream
        .prepare_data(
            Bytes::from(vec![0x52; unresolved_payload_len]),
            StreamFlags::NONE,
        )
        .expect("prepare owner data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit owner data");
    binding.record_owner_flight(owner_key, &frame);
    binding.detach(owner_key, &owner_commands);
    let ack_ranges = [OffsetRange {
        start: 0,
        end: 1024,
    }];
    let _ = send_stream.apply_ack(&ack_ranges);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(121),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
    );
    response_sender.record_owner_progress(1024);

    let outcome = enqueue_reliable_tail_repair(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        FlowLane::Throughput,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        1024,
    );

    assert_eq!(outcome.queued, 1);
    assert!(
        response_sender.bytes() <= BBR_MAX_SEND_QUANTUM_BYTES,
        "failed-owner recovery is correctness repair: one stall/failover event must queue one service repair quantum, not a multi-frame burst that inflates overhead under flapping"
    );
}

#[test]
fn unknown_owner_tail_repair_uses_remaining_output_after_persistent_stall() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(119);
    let owner_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let failover_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(119),
        owner_key.underlay,
        owner_key.path_id,
        owner_commands.clone(),
        FlowLane::Throughput,
    );
    let (failover_commands, _failover_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            failover_key.underlay,
            failover_key.path_id,
            failover_commands,
            FlowLane::Throughput,
            StreamOpenRole::Active,
            reliable_relay_buffer_len(limits),
        ),
        ResponseStreamAttachOutcome::Attached
    );
    binding.detach(owner_key, &owner_commands);

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: failover_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx,
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x43; 4096]), StreamFlags::NONE)
        .expect("prepare owner data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit owner data");
    let ack_ranges = [OffsetRange {
        start: 0,
        end: 1024,
    }];
    let _ = send_stream.apply_ack(&ack_ranges);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(119),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
    );
    response_sender.record_owner_progress(1024);

    let outcome = enqueue_reliable_tail_repair(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        FlowLane::Throughput,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        1024,
    );

    assert_eq!(
        outcome.queued, 1,
        "when retained owner bytes have no live owner and no path-flight record, persistent tail repair must still use a live survivor instead of deadlocking"
    );
    assert!(!outcome.pending);
}

#[tokio::test]
async fn unknown_owner_tail_repair_dispatches_as_path_failure_repair() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(120);
    let owner_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let failover_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(120),
        owner_key.underlay,
        owner_key.path_id,
        owner_commands.clone(),
        FlowLane::Throughput,
    );
    let (failover_commands, mut failover_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            failover_key.underlay,
            failover_key.path_id,
            failover_commands,
            FlowLane::Throughput,
            StreamOpenRole::Active,
            reliable_relay_buffer_len(limits),
        ),
        ResponseStreamAttachOutcome::Attached
    );
    binding.detach(owner_key, &owner_commands);

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: failover_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding),
        frames: frame_rx,
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x43; 4096]), StreamFlags::NONE)
        .expect("prepare owner data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit owner data");
    let ack_ranges = [OffsetRange {
        start: 0,
        end: 1024,
    }];
    let _ = send_stream.apply_ack(&ack_ranges);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(120),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
    );
    response_sender.record_owner_progress(1024);

    let outcome = enqueue_reliable_tail_repair(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        FlowLane::Throughput,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        1024,
    );
    assert_eq!(outcome.queued, 1);

    let ordered_owner_debt_bytes = send_stream.repair_bytes();
    let dispatch = response_sender
        .dispatch_next_with_ordered_owner_debt(
            &path_stream,
            &mut send_stream,
            FlowLane::Throughput,
            limits,
            ordered_owner_debt_bytes,
        )
        .expect("unknown-owner tail repair must be failover-dispatchable");

    assert_eq!(dispatch.lane, ReliableWorkClass::Repair);
    assert_eq!(dispatch.selected_path, Some(failover_key));
    assert!(matches!(
        try_recv_reliable_path_command(&mut failover_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
}

#[test]
fn live_owner_no_ack_frontier_tail_repair_waits_for_authoritative_gap() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(121);
    let owner_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let repair_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(121),
        owner_key.underlay,
        owner_key.path_id,
        owner_commands,
        FlowLane::Throughput,
    );
    let (repair_commands, _repair_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            repair_key.underlay,
            repair_key.path_id,
            repair_commands,
            FlowLane::Throughput,
            StreamOpenRole::Active,
            reliable_relay_buffer_len(limits),
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: repair_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx,
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x48; 4096]), StreamFlags::NONE)
        .expect("prepare owner data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit owner data");
    binding.record_owner_flight(owner_key, &frame);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(121),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
    );
    let outcome = enqueue_reliable_tail_repair(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &[],
        false,
        None,
        FlowLane::Throughput,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        0,
    );

    assert_eq!(
        outcome.queued, 0,
        "no ACK frontier is not an authoritative product gap; live-owner recovery must wait for ACK progress, failed-owner evidence, or terminal-tail repair"
    );
    assert!(!outcome.pending);
}

#[test]
fn live_owner_no_ack_frontier_tail_repair_does_not_probe_prefix() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(122);
    let owner_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let repair_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(122),
        owner_key.underlay,
        owner_key.path_id,
        owner_commands,
        FlowLane::Throughput,
    );
    let (repair_commands, _repair_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            repair_key.underlay,
            repair_key.path_id,
            repair_commands,
            FlowLane::Throughput,
            StreamOpenRole::Active,
            reliable_relay_buffer_len(limits),
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: repair_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx,
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let total = reliable_relay_buffer_len(limits).saturating_mul(4);
    let mut remaining = total;
    while remaining > 0 {
        let chunk = remaining.min(limits.max_payload_bytes);
        let frame = send_stream
            .prepare_data(Bytes::from(vec![0x48; chunk]), StreamFlags::NONE)
            .expect("prepare owner data");
        send_stream
            .commit_prepared_data(&frame)
            .expect("commit owner data");
        binding.record_owner_flight(owner_key, &frame);
        remaining = remaining.saturating_sub(chunk);
    }

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(122),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
    );
    let outcome = enqueue_reliable_tail_repair(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &[],
        false,
        None,
        FlowLane::Throughput,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        0,
    );

    assert_eq!(
        outcome.queued, 0,
        "no-frontier live-owner data may still be in carrier recovery and must not become product RepairData"
    );
    assert!(!outcome.pending);
    assert_eq!(response_sender.bytes(), 0);
}

#[test]
fn unknown_owner_tail_repair_without_ack_frontier_waits() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(120);
    let owner_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let failover_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(120),
        owner_key.underlay,
        owner_key.path_id,
        owner_commands.clone(),
        FlowLane::Throughput,
    );
    let (failover_commands, _failover_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            failover_key.underlay,
            failover_key.path_id,
            failover_commands,
            FlowLane::Throughput,
            StreamOpenRole::Active,
            reliable_relay_buffer_len(limits),
        ),
        ResponseStreamAttachOutcome::Attached
    );
    binding.detach(owner_key, &owner_commands);

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: failover_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding),
        frames: frame_rx,
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x44; 4096]), StreamFlags::NONE)
        .expect("prepare owner data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit owner data");

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(120),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
    );

    let outcome = enqueue_reliable_tail_repair(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &[],
        false,
        None,
        FlowLane::Throughput,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        0,
    );

    assert_eq!(
        outcome.queued, 0,
        "unknown-owner repair needs an ACK frontier; without one it can duplicate the entire startup tail and inflate overhead"
    );
    assert!(!outcome.pending);
}

#[test]
fn failed_owner_tail_repair_does_not_duplicate_queued_repair_range() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(109);
    let owner_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let failover_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(109),
        owner_key.underlay,
        owner_key.path_id,
        owner_commands.clone(),
        FlowLane::Throughput,
    );
    let (failover_commands, _failover_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            failover_key.underlay,
            failover_key.path_id,
            failover_commands,
            FlowLane::Throughput,
            StreamOpenRole::Active,
            reliable_relay_buffer_len(limits),
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: failover_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx,
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x43; 4096]), StreamFlags::NONE)
        .expect("prepare owner data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit owner data");
    binding.record_owner_flight(owner_key, &frame);
    binding.detach(owner_key, &owner_commands);
    let ack_ranges = [OffsetRange {
        start: 0,
        end: 1024,
    }];
    let _ = send_stream.apply_ack(&ack_ranges);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(109),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
    );
    response_sender.record_owner_progress(1024);
    let performance = MppPerformanceConfig {
        extra_traffic_hint_percent: 5,
    };

    let first = enqueue_reliable_tail_repair(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        FlowLane::Throughput,
        limits,
        performance,
        path_stream.max_frame_payload_bytes,
        1024,
    );
    let queued_bytes_after_first = response_sender.bytes();
    let second = enqueue_reliable_tail_repair(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        FlowLane::Throughput,
        limits,
        performance,
        path_stream.max_frame_payload_bytes,
        1024,
    );

    assert_eq!(first.queued, 1);
    assert!(!first.pending);
    assert_eq!(
        second.queued, 0,
        "tail repair must not enqueue the same RepairData range while it is already queued"
    );
    assert!(
        second.pending,
        "already queued RepairData should count as a pending repair attempt so the tail timer backs off"
    );
    assert_eq!(response_sender.bytes(), queued_bytes_after_first);
}

#[test]
fn tail_repair_treats_live_inflight_repair_as_pending() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(127);
    let owner_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let repair_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(127),
        owner_key.underlay,
        owner_key.path_id,
        owner_commands,
        FlowLane::Throughput,
    );
    let (repair_commands, _repair_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            repair_key.underlay,
            repair_key.path_id,
            repair_commands,
            FlowLane::Throughput,
            StreamOpenRole::Active,
            reliable_relay_buffer_len(limits),
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: owner_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx,
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x49; 4096]), StreamFlags::NONE)
        .expect("prepare owner data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit owner data");
    binding.record_owner_flight(owner_key, &frame);
    let ack_ranges = [OffsetRange {
        start: 0,
        end: 1024,
    }];
    let _ = send_stream.apply_ack(&ack_ranges);
    let inflight_repair = send_stream
        .retransmission_frames_after_ack_frontier(&ack_ranges, 1024)
        .into_iter()
        .next()
        .expect("expected frontier repair frame");
    binding.record_repair_flight(repair_key, &inflight_repair);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(127),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
    );
    response_sender.record_owner_progress(1024);

    let outcome = enqueue_reliable_tail_repair(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        FlowLane::Throughput,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        1024,
    );

    assert_eq!(
        outcome.queued, 0,
        "live in-flight RepairData for the same range must not be stacked"
    );
    assert!(
        outcome.pending,
        "live in-flight RepairData should keep the tail repair timer backed off"
    );
    assert_eq!(response_sender.bytes(), 0);
}

#[test]
fn persistent_tail_repair_waits_when_live_repair_copy_is_in_flight() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(105);
    let owner_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let repair_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(105),
        owner_key.underlay,
        owner_key.path_id,
        owner_commands,
        FlowLane::Throughput,
    );
    let (repair_commands, _repair_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            repair_key.underlay,
            repair_key.path_id,
            repair_commands,
            FlowLane::Throughput,
            StreamOpenRole::Active,
            reliable_relay_buffer_len(limits),
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: owner_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx,
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x48; 4096]), StreamFlags::NONE)
        .expect("prepare owner data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit owner data");
    binding.record_owner_flight(owner_key, &frame);
    binding.record_repair_flight(repair_key, &frame);
    let ack_ranges = [OffsetRange {
        start: 0,
        end: 1024,
    }];
    let _ = send_stream.apply_ack(&ack_ranges);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(105),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
    );
    response_sender.record_owner_progress(1024);

    let outcome = enqueue_reliable_tail_repair(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        FlowLane::Throughput,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        1024,
    );

    assert_eq!(
        outcome.queued, 0,
        "persistent tail repair must not stack another copy while a live RepairData flight already covers the frontier range"
    );
    assert!(
        outcome.pending,
        "live in-flight RepairData should back off the tail repair timer"
    );
    assert_eq!(response_sender.bytes(), 0);
}

#[test]
fn stale_live_repair_flight_does_not_block_terminal_tail_retry() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(106);
    let owner_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let repair_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(106),
        owner_key.underlay,
        owner_key.path_id,
        owner_commands,
        FlowLane::Throughput,
    );
    let (repair_commands, _repair_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            repair_key.underlay,
            repair_key.path_id,
            repair_commands,
            FlowLane::Throughput,
            StreamOpenRole::Active,
            reliable_relay_buffer_len(limits),
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: owner_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx,
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x4a; 4096]), StreamFlags::NONE)
        .expect("prepare owner data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit owner data");
    binding.record_owner_flight(owner_key, &frame);
    let ack_ranges = [OffsetRange {
        start: 0,
        end: 1024,
    }];
    let _ = send_stream.apply_ack(&ack_ranges);
    let inflight_repair = send_stream
        .retransmission_frames_after_ack_frontier(&ack_ranges, 1024)
        .into_iter()
        .next()
        .expect("expected frontier repair frame");
    binding.record_repair_flight(repair_key, &inflight_repair);
    binding.age_repair_flights_for_test(
        reliable_relay_tail_repair_delay(None).saturating_add(Duration::from_millis(1)),
    );

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(106),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
    );
    response_sender.record_owner_progress(1024);

    let outcome = enqueue_reliable_tail_repair(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        FlowLane::Throughput,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        1024,
    );

    assert_eq!(
        outcome.queued, 1,
        "stale unacked RepairData must not suppress correctness repair forever"
    );
    assert!(
        !outcome.pending,
        "stale RepairData should be retried instead of keeping the tail timer backed off"
    );
    assert!(response_sender.bytes() > 0);
}

#[tokio::test]
async fn persistent_live_owner_tail_repair_waits_when_distinct_alternate_lacks_queue_credit() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(124);
    let owner_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let repair_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (owner_commands, mut owner_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(124),
        owner_key.underlay,
        owner_key.path_id,
        owner_commands,
        FlowLane::Throughput,
    );
    let (repair_commands, _repair_receivers) = reliable_path_command_channels(1);
    let repair_commands_for_fill = repair_commands.clone();
    assert_eq!(
        binding.attach(
            repair_key.underlay,
            repair_key.path_id,
            repair_commands,
            FlowLane::Throughput,
            StreamOpenRole::Active,
            reliable_relay_buffer_len(limits),
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: owner_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx,
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x55; 4096]), StreamFlags::NONE)
        .expect("prepare owner data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit owner data");
    binding.record_owner_flight(owner_key, &frame);
    let ack_ranges = [OffsetRange {
        start: 0,
        end: 1024,
    }];
    let _ = send_stream.apply_ack(&ack_ranges);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(124),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
    );
    response_sender.record_owner_progress(1024);

    let outcome = enqueue_reliable_tail_repair(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        FlowLane::Throughput,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        1024,
    );
    assert_eq!(outcome.queued, 1);

    repair_commands_for_fill
        .try_enqueue_admitted_frame(
            Frame::StreamData {
                stream_id,
                offset: 4096,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"queued"),
            },
            FlowLane::Throughput,
        )
        .expect("test setup fills alternate data queue");

    let dispatch = response_sender.dispatch_next_with_ordered_owner_debt(
        &path_stream,
        &mut send_stream,
        FlowLane::Throughput,
        limits,
        0,
    );

    assert!(matches!(dispatch, Err(RuntimeError::SenderServiceBlocked)));
    assert!(
        try_recv_reliable_path_command(&mut owner_receivers).is_none(),
        "live-owner tail repair must wait rather than retransmit on its owner"
    );
    let completed = [OffsetRange {
        start: 0,
        end: send_stream.next_offset(),
    }];
    let _ = send_stream.apply_ack(&completed);
    path_stream.release_normalized_acked_ranges(&completed);
    response_sender.release_normalized_acked_repairs(&completed);
    assert!(
        response_sender.is_empty(),
        "owner ACK progress must remove a blocked queued live-tail repair before FIN or later data"
    );
}

#[tokio::test]
async fn final_tail_repair_dispatches_on_service_when_alternate_lacks_queue_credit() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(125);
    let owner_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let repair_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (owner_commands, mut owner_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(125),
        owner_key.underlay,
        owner_key.path_id,
        owner_commands,
        FlowLane::Latency,
    );
    let (repair_commands, _repair_receivers) = reliable_path_command_channels(1);
    let repair_commands_for_fill = repair_commands.clone();
    assert_eq!(
        binding.attach(
            repair_key.underlay,
            repair_key.path_id,
            repair_commands,
            FlowLane::Latency,
            StreamOpenRole::Active,
            reliable_relay_buffer_len(limits),
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: FlowLane::Latency,
        underlay: owner_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx,
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x56; 192]), StreamFlags::NONE)
        .expect("prepare owner data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit owner data");
    binding.record_owner_flight(owner_key, &frame);
    let ack_ranges = [OffsetRange { start: 0, end: 128 }];
    let _ = send_stream.apply_ack(&ack_ranges);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(125),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
    );
    response_sender.record_owner_progress(128);

    let (repair_frames, blocked_frontier_offset, same_output_frontier_retransmit) =
        prefix_final_tail_repair_frames_with_available_output(
            &path_stream,
            stream_final_offset_tail_repair_frames(&send_stream, &ack_ranges, 64, true, true),
        );
    assert_eq!(blocked_frontier_offset, None);
    assert!(!same_output_frontier_retransmit);
    assert_eq!(repair_frames.len(), 1);
    for frame in repair_frames {
        let _ = response_sender.enqueue_critical_tail_repair_frame(frame);
    }

    repair_commands_for_fill
        .try_enqueue_admitted_frame(
            Frame::StreamData {
                stream_id,
                offset: 192,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"queued"),
            },
            reliable_path_stream_ordered_queue_lane(),
        )
        .expect("test setup fills alternate data queue");

    response_sender
        .dispatch_next_with_ordered_owner_debt(
            &path_stream,
            &mut send_stream,
            FlowLane::Latency,
            limits,
            0,
        )
        .expect("final-tail RepairData must use the Service path when the alternate has no queue credit");

    let command = try_recv_reliable_path_command(&mut owner_receivers)
        .expect("expected same-Service final-tail repair frame");
    match command {
        ReliablePathCommand::SendFrame(Frame::StreamData {
            offset, payload, ..
        }) => {
            assert_eq!(offset, 128);
            assert_eq!(payload.len(), 64);
        }
        _ => panic!("expected same-Service final-tail repair STREAM_DATA"),
    }
}

#[tokio::test]
async fn final_tail_repair_dispatches_on_service_when_no_alternate_survives() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(126);
    let owner_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let (owner_commands, mut owner_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(126),
        owner_key.underlay,
        owner_key.path_id,
        owner_commands,
        FlowLane::Latency,
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: FlowLane::Latency,
        underlay: owner_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx,
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x57; 192]), StreamFlags::NONE)
        .expect("prepare owner data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit owner data");
    binding.record_owner_flight(owner_key, &frame);
    let ack_ranges = [OffsetRange { start: 0, end: 128 }];
    let _ = send_stream.apply_ack(&ack_ranges);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(126),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
    );
    response_sender.record_owner_progress(128);

    let (repair_frames, blocked_frontier_offset, same_output_frontier_retransmit) =
        prefix_final_tail_repair_frames_with_available_output(
            &path_stream,
            stream_final_offset_tail_repair_frames(&send_stream, &ack_ranges, 64, true, true),
        );
    assert_eq!(blocked_frontier_offset, None);
    assert!(same_output_frontier_retransmit);
    assert_eq!(repair_frames.len(), 1);
    for frame in repair_frames {
        let _ = response_sender.enqueue_critical_tail_repair_frame(frame);
    }

    response_sender
        .dispatch_next_with_ordered_owner_debt(
            &path_stream,
            &mut send_stream,
            FlowLane::Latency,
            limits,
            0,
        )
        .expect("final-tail RepairData must use the only Service survivor");

    let command = try_recv_reliable_path_command(&mut owner_receivers)
        .expect("expected Service final-tail repair frame");
    match command {
        ReliablePathCommand::SendFrame(Frame::StreamData {
            offset, payload, ..
        }) => {
            assert_eq!(offset, 128);
            assert_eq!(payload.len(), 64);
        }
        _ => panic!("expected Service final-tail repair STREAM_DATA"),
    }
}

#[tokio::test]
async fn failed_owner_repair_without_ack_frontier_starts_at_zero() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(103);
    let owner_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let failover_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(103),
        owner_key.underlay,
        owner_key.path_id,
        owner_commands.clone(),
        FlowLane::Throughput,
    );
    let (failover_commands, mut failover_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            failover_key.underlay,
            failover_key.path_id,
            failover_commands,
            FlowLane::Throughput,
            StreamOpenRole::Active,
            reliable_relay_buffer_len(limits),
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: failover_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx,
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x46; 4096]), StreamFlags::NONE)
        .expect("prepare owner data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit owner data");
    binding.record_owner_flight(owner_key, &frame);
    binding.detach(owner_key, &owner_commands);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(103),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
    );

    let outcome = enqueue_reliable_tail_repair(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &[],
        false,
        None,
        FlowLane::Throughput,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        0,
    );

    assert_eq!(
        outcome.queued, 1,
        "failed-owner repair must retransmit from offset zero when no response ACK frontier exists"
    );
    assert!(!outcome.pending);
    response_sender
        .dispatch_next_with_ordered_owner_debt(
            &path_stream,
            &mut send_stream,
            FlowLane::Throughput,
            limits,
            0,
        )
        .expect("dispatch failed-owner repair");
    let command = try_recv_reliable_path_command(&mut failover_receivers).expect("repair frame");
    match command {
        ReliablePathCommand::SendFrame(Frame::StreamData {
            offset, payload, ..
        }) => {
            assert_eq!(offset, 0);
            assert!(!payload.is_empty());
        }
        _ => panic!("expected failed-owner repair STREAM_DATA"),
    }
}

#[test]
fn live_owner_tail_without_ack_frontier_does_not_repair_on_alternate() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(104);
    let owner_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let alternative_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(104),
        owner_key.underlay,
        owner_key.path_id,
        owner_commands,
        FlowLane::Throughput,
    );
    let (alternative_commands, _alternative_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            alternative_key.underlay,
            alternative_key.path_id,
            alternative_commands,
            FlowLane::Throughput,
            StreamOpenRole::Active,
            reliable_relay_buffer_len(limits),
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: owner_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding),
        frames: frame_rx,
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x47; 4096]), StreamFlags::NONE)
        .expect("prepare owner data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit owner data");

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(104),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
    );

    let outcome = enqueue_reliable_tail_repair(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &[],
        false,
        None,
        FlowLane::Throughput,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        0,
    );

    assert_eq!(
        outcome.queued, 0,
        "without a complete ACK frontier or failed owner, live owner bytes are normal in-flight data and must not be duplicated onto an alternate"
    );
    assert!(!outcome.pending);
}

#[test]
fn failed_owner_tail_repair_is_not_blocked_by_optional_budget() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(101);
    let owner_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let failover_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(101),
        owner_key.underlay,
        owner_key.path_id,
        owner_commands.clone(),
        FlowLane::Throughput,
    );
    let (failover_commands, _failover_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            failover_key.underlay,
            failover_key.path_id,
            failover_commands,
            FlowLane::Throughput,
            StreamOpenRole::Active,
            reliable_relay_buffer_len(limits),
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: failover_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx,
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x44; 4096]), StreamFlags::NONE)
        .expect("prepare owner data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit owner data");
    binding.record_owner_flight(owner_key, &frame);
    binding.detach(owner_key, &owner_commands);
    let ack_ranges = [OffsetRange {
        start: 0,
        end: 1024,
    }];
    let _ = send_stream.apply_ack(&ack_ranges);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(101),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
    );
    let optional_budget = response_sender.repair_extra_budget_remaining(limits);
    assert!(optional_budget > 0);
    assert!(
        response_sender
            .enqueue_repair_frame_with_priority(
                Frame::StreamData {
                    stream_id,
                    offset: 10_000,
                    flags: StreamFlags::NONE,
                    payload: Bytes::from(vec![0x99; optional_budget]),
                },
                limits,
                true,
            )
            .is_some()
    );
    assert_eq!(
        response_sender.repair_extra_event_budget_remaining(limits),
        0
    );

    let outcome = enqueue_reliable_tail_repair(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        FlowLane::Throughput,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        1024,
    );

    assert!(
        outcome.queued > 0,
        "failed-owner tail recovery is correctness repair and must not depend on optional duplicate/probe budget"
    );
    assert!(!outcome.pending);
}

#[test]
fn persistent_ack_gap_tail_timer_does_not_duplicate_ack_gap_controller() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(102);
    let owner_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let repair_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(102),
        owner_key.underlay,
        owner_key.path_id,
        owner_commands,
        FlowLane::Throughput,
    );
    let (repair_commands, _repair_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            repair_key.underlay,
            repair_key.path_id,
            repair_commands,
            FlowLane::Throughput,
            StreamOpenRole::Active,
            reliable_relay_buffer_len(limits),
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: owner_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx,
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x45; 4096]), StreamFlags::NONE)
        .expect("prepare owner data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit owner data");
    binding.record_owner_flight(owner_key, &frame);
    let ack_ranges = [
        OffsetRange {
            start: 0,
            end: 1024,
        },
        OffsetRange {
            start: 2048,
            end: 4096,
        },
    ];
    let _ = send_stream.apply_ack(&ack_ranges);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(102),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
    );
    let optional_budget = response_sender.repair_extra_budget_remaining(limits);
    assert!(optional_budget > 0);
    assert!(
        response_sender
            .enqueue_repair_frame_with_priority(
                Frame::StreamData {
                    stream_id,
                    offset: 10_000,
                    flags: StreamFlags::NONE,
                    payload: Bytes::from(vec![0x99; optional_budget]),
                },
                limits,
                true,
            )
            .is_some()
    );
    assert_eq!(
        response_sender.repair_extra_event_budget_remaining(limits),
        0
    );

    let outcome = enqueue_reliable_tail_repair(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        FlowLane::Throughput,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        4096,
    );

    assert_eq!(
        outcome.queued, 0,
        "persistent ACK gaps are repaired by the ACK-gap controller; the tail timer must not duplicate live-owner gap repair"
    );
    assert!(!outcome.pending);
}

#[test]
fn persistent_live_owner_tail_repair_queues_repairdata_without_service_migration() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(100);
    let owner_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let alternative_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(100),
        owner_key.underlay,
        owner_key.path_id,
        owner_commands,
        FlowLane::Throughput,
    );
    let (alternative_commands, _alternative_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            alternative_key.underlay,
            alternative_key.path_id,
            alternative_commands,
            FlowLane::Throughput,
            StreamOpenRole::Active,
            reliable_relay_buffer_len(limits),
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: owner_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx,
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let repair_debt = reliable_relay_buffer_len(limits).saturating_mul(4);
    let mut remaining = repair_debt;
    while remaining > 0 {
        let chunk = remaining.min(limits.max_payload_bytes);
        let frame = send_stream
            .send_data(Bytes::from(vec![0x43; chunk]), StreamFlags::NONE)
            .expect("seed owner data");
        binding.record_owner_flight(owner_key, &frame);
        remaining = remaining.saturating_sub(chunk);
    }
    let ack_ranges = [OffsetRange {
        start: 0,
        end: 1024,
    }];
    let _ = send_stream.apply_ack(&ack_ranges);
    assert!(
        send_stream.repair_bytes() > reliable_relay_buffer_len(limits),
        "test must cover a retained tail larger than one bounded repair event"
    );

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(100),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
    );
    response_sender.record_owner_progress(1024);

    let outcome = enqueue_reliable_tail_repair(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        FlowLane::Throughput,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        1024,
    );

    assert_eq!(
        outcome.queued, 1,
        "a persistent live-owner tail stall should reinject the lowest blocked range as RepairData on an alternate output without migrating Service ownership"
    );
    assert!(!outcome.pending);
    assert_eq!(
        binding.ordered_data_owner(),
        Some(owner_key),
        "tail repair is RepairData; it must not rewrite the Service owner"
    );
}

#[test]
fn final_tail_repair_ready_allows_closed_no_ack_frontier_after_deadline() {
    let limits = MuxLimits::default();
    let mut send_stream = ReliableSendStream::new(StreamId(9), limits);
    send_stream
        .send_data(Bytes::from_static(&[7; 4096]), StreamFlags::NONE)
        .expect("send stream data");
    let now = tokio::time::Instant::now();

    assert!(reliable_final_tail_repair_ready(
        true,
        &send_stream,
        &[],
        0,
        now,
        now,
    ));
}

#[test]
fn tcp_multipath_progress_timer_stays_enabled_with_repair_alternatives() {
    assert!(reliable_relay_recv_progress_timer_enabled(
        UnderlayProtocol::Udp,
        false,
    ));
    assert!(reliable_relay_recv_progress_timer_enabled(
        UnderlayProtocol::Tcp,
        true,
    ));
    assert!(!reliable_relay_recv_progress_timer_enabled(
        UnderlayProtocol::Tcp,
        false,
    ));
}
