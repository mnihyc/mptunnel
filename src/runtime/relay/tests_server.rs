use super::*;
use crate::model::capacity::MAX_RELIABLE_SERVICE_QUANTUM_BYTES;
use crate::model::path::CarrierPathKey;
use crate::model::timing::transport_pto_from_snapshot;
use crate::mux::stream::validate_stream_ack;
use crate::outbound::OutboundConfig;
use crate::protocol::frame::stream_ack_contiguous_frontier;
use crate::protocol::{PathId, PathMetricDirection, PathMetrics};
use crate::runtime::outbound_registry::{RuntimeOutboundLeaf, RuntimeOutboundRegistry};
use crate::runtime::path::commands::{
    ReliablePathCommand, reliable_path_command_channels, try_recv_reliable_path_command,
};
use crate::runtime::path::model::metric_epoch_now;
use crate::runtime::relay::io::stream_ack_ranges_expose_authoritative_gap;
use crate::runtime::stream::ReliablePathStreamOutput;
use crate::runtime::stream::response::{
    ResponseStreamAttachOutcome, ResponseStreamBinding, ServerPathMetricsSource,
};
use bytes::Bytes;

fn tail_recovery_candidate(start: u64, sent_at: Instant) -> ReliableRelayTailRecoveryCandidate {
    ReliableRelayTailRecoveryCandidate::Untracked {
        start,
        end: start + 64,
        sent_at,
    }
}

#[test]
fn server_completion_waits_for_every_live_ack_publication() {
    let mut publication = ServerAckPublicationState::default();
    publication.record_status(1, true, true);
    assert!(
        !publication.current_generation_is_fully_published(),
        "one accepted copy cannot retire retained state for a blocked attachment"
    );

    publication.record_status(1, true, false);
    assert!(publication.current_generation_is_fully_published());
}

#[tokio::test]
async fn server_relay_expires_only_after_its_absolute_no_output_interval() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(611);
    let (commands, command_receivers) = reliable_path_command_channels(4);
    drop(command_receivers);
    let binding = ResponseStreamBinding::new(
        SessionId(611),
        UnderlayProtocol::Tcp,
        PathId(0),
        commands,
        TrafficClass::Latency,
    );
    let (frames_tx, frames_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: limits.max_stream_window_bytes,
        lane: TrafficClass::Latency,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding),
        frames: frames_rx.into(),
    };
    let id = crate::product::OutboundId::parse("test-direct").expect("outbound");
    let outbound_registry = RuntimeOutboundRegistry::compile(
        [RuntimeOutboundLeaf::Local {
            id: id.clone(),
            config: OutboundConfig::Direct,
            connect_timeout: Duration::from_secs(1),
            native_sockets: Arc::new(crate::transport::SystemNativeSocketConfigurator),
        }],
        &[],
        crate::runtime::outbound_registry::test_dns_generation(),
    )
    .expect("registry");
    let egress_selection = outbound_registry
        .selection_for_egress(&crate::config::EgressRef::Outbound(id))
        .expect("selection");
    let context = ServerReliableRelayContext {
        outbound_registry,
        egress_selection,
        dns_plan: None,
        destination_policy: Arc::new(
            crate::outbound::ServerDestinationPolicy::allow_restricted_for_test(),
        ),
        performance: MppPerformanceConfig::default(),
        mux_limits: limits,
        max_paths_per_session: crate::performance::ResourceLimits::default().max_paths,
        session_retention_timeout: Duration::from_millis(100),
        telemetry: RuntimeTelemetry::new(1),
    };
    let (application, relay_side) = tokio::io::duplex(4096);
    let send_buffer = crate::runtime::stream::SessionSendBuffer::from_limits(limits);
    let mut relay = Box::pin(relay_reliable_stream(
        relay_side,
        path_stream,
        &context,
        SessionId(611),
        send_buffer,
        None,
    ));

    assert!(
        tokio::time::timeout(Duration::from_millis(30), relay.as_mut())
            .await
            .is_err(),
        "server relay expired before its configured retention interval"
    );
    let result = tokio::time::timeout(Duration::from_secs(1), relay.as_mut())
        .await
        .expect("server retention expiry");
    assert!(matches!(result, Err(RuntimeError::SessionRetentionTimeout)));

    drop(application);
    drop(frames_tx);
}

fn record_server_delivery_evidence(binding: &ResponseStreamBinding, key: CarrierPathKey) {
    binding.update_path_metrics(
        key,
        PathMetrics {
            path_id: key.path_id,
            underlay: key.underlay,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: metric_epoch_now(),
            metric_age_us: 0,
            srtt_us: 40_000,
            rttvar_us: 5_000,
            jitter_us: 5_000,
            delivery_rate_bps: 100_000_000,
            pacing_rate_bps: 100_000_000,
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
        },
        ServerPathMetricsSource::LocalSender,
    );
}

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
            .receive_data(0, Bytes::from(vec![0x11; 1024]))
            .expect("contiguous prefix");
        assert!(progress.should_send_ack(
            &recv_stream,
            Some(path),
            TrafficClass::Throughput,
            mux_limits,
            false,
        ));
        assert_eq!(sparse_progress.ack_frames(&recv_stream, false).len(), 1);

        let mut frames = Vec::new();
        for offset in [8192, 32768, 16384, 12288] {
            recv_stream
                .receive_data(offset, Bytes::from(vec![0x22; 1024]))
                .expect("sparse range");
            assert!(
                progress.should_send_ack(
                    &recv_stream,
                    Some(path),
                    TrafficClass::Throughput,
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
fn tail_stall_reinjection_retransmits_same_frontier_only_after_stall_evidence() {
    let stream_id = StreamId(34);
    let (commands, _receivers) = reliable_path_command_channels(4);
    let binding = ResponseStreamBinding::new(
        SessionId(34),
        UnderlayProtocol::Tcp,
        PathId(0),
        commands,
        TrafficClass::Throughput,
    );
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: 1024,
        lane: TrafficClass::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let frame = Frame::StreamData {
        stream_id,
        offset: 128,
        payload: Bytes::from_static(b"frontier"),
    };
    binding.record_original_flight(
        CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        },
        &frame,
    );
    let later_frame = Frame::StreamData {
        stream_id,
        offset: 136,
        payload: Bytes::from_static(b"later"),
    };

    let (without_stall, blocked_offset) = prefix_reinjection_frames_with_available_output(
        &path_stream,
        vec![frame.clone(), later_frame.clone()],
        false,
    );
    assert!(without_stall.is_empty());
    assert_eq!(blocked_offset, Some(128));

    let (with_stall, blocked_offset) = prefix_reinjection_frames_with_available_output(
        &path_stream,
        vec![frame, later_frame],
        true,
    );
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
fn tail_timer_reinjection_allows_only_authoritative_or_failed_original_reinjection() {
    assert!(
        stream_tail_timer_reinjection_allowed(false, true),
        "after the original-transmission path output is gone, the remaining output is the failover path even though it is no longer a second live alternative"
    );
    assert!(!stream_tail_timer_reinjection_allowed(false, false));
    assert!(
        stream_tail_timer_reinjection_allowed(true, false),
        "authoritative ACK-frontier tail reinjection may use a live alternate"
    );
}

#[test]
fn contiguous_ack_frontier_lag_is_tail_guard_not_reinjection_debt() {
    let ranges = [OffsetRange {
        start: 0,
        end: 1024,
    }];

    assert!(
        !stream_ack_ranges_expose_authoritative_gap(true, &ranges),
        "a contiguous unacknowledged suffix is not an authoritative product reinjection gap"
    );
    assert_eq!(
        reliable_relay_data_ack_outstanding_bytes(TrafficClass::Throughput, 1024, 8192,),
        7168,
        "a contiguous unacknowledged suffix is a tail guard for alternate original-transmission paths"
    );
    assert_eq!(
        reliable_relay_data_ack_outstanding_bytes(TrafficClass::Throughput, 0, 8192,),
        8192,
        "before the first contiguous ACK, already-sent bulk bytes are still original-transmission path-tail debt for alternate original-transmission paths"
    );
    assert_eq!(
        reliable_relay_data_ack_outstanding_bytes(TrafficClass::Latency, 1024, 8192,),
        0,
        "latency traffic must not be pinned by bulk original-transmission path-tail pressure"
    );
}

#[test]
fn incomplete_ack_chunks_after_a_snapshot_do_not_extend_its_negative_authority() {
    let limits = MuxLimits::default();
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(StreamId(312), limits, u64::MAX);
    send_stream
        .send_data(Bytes::from(vec![0x11; 1024]))
        .expect("send response data before snapshot");

    let mut authoritative = AuthoritativeStreamAckSnapshot::default();
    let complete_prefix = [OffsetRange {
        start: 0,
        end: 1024,
    }];
    let complete_ack =
        validate_stream_ack(true, complete_prefix.to_vec(), send_stream.next_offset())
            .expect("complete prefix stays within assigned data");
    send_stream
        .apply_validated_ack(&complete_ack)
        .expect("complete ACK fits retained send chunks");
    update_reinjection_authoritative_ack_snapshot(&mut authoritative, &complete_ack);

    for value in [0x22, 0x33] {
        send_stream
            .send_data(Bytes::from(vec![value; 1024]))
            .expect("send response data after snapshot");
    }
    let incomplete_progress = [OffsetRange {
        start: 1024,
        end: 3072,
    }];
    let incomplete_ack = validate_stream_ack(
        false,
        incomplete_progress.to_vec(),
        send_stream.next_offset(),
    )
    .expect("incomplete progress stays within assigned data");
    send_stream
        .apply_validated_ack(&incomplete_ack)
        .expect("incomplete ACK fits retained send chunks");
    update_reinjection_authoritative_ack_snapshot(&mut authoritative, &incomplete_ack);

    assert_eq!(
        authoritative.ranges(),
        &[OffsetRange {
            start: 0,
            end: 1024,
        }]
    );
    assert!(authoritative.complete());
    assert_eq!(authoritative.horizon(), Some(1024));
    assert_eq!(send_stream.data_ack_frontier(), 3072);
    assert_eq!(
        reliable_relay_current_data_ack_outstanding_bytes(
            TrafficClass::Throughput,
            &send_stream,
            send_stream.data_ack_frontier(),
        ),
        0,
        "positive incomplete ACK chunks must not leave stale tail-guard debt",
    );
}

#[test]
fn tail_reinjection_uses_single_pto_stall_timeout() {
    let original_sent_at = Instant::now();
    let deadline = reliable_relay_tail_reinjection_deadline(original_sent_at, None, None);
    let expected =
        tokio::time::Instant::from_std(original_sent_at + transport_pto_from_snapshot(None));

    assert_eq!(deadline, expected);
}

#[test]
fn tail_reinjection_timer_is_lane_neutral_after_stall_evidence() {
    assert!(
        reliable_relay_tail_reinjection_timer_active(64, true, false),
        "a complete stalled original-transmission path suffix must use bounded alternate-output reinjection in every reliable lane"
    );
    assert!(
        reliable_relay_tail_reinjection_timer_active(64, false, true),
        "failed-original-transmission path correctness reinjection must not depend on the product lane"
    );
    assert!(
        !reliable_relay_tail_reinjection_timer_active(64, false, false),
        "an outstanding suffix without an eligible alternate must remain with its carrier"
    );
    assert!(
        !reliable_relay_tail_reinjection_timer_active(0, true, true),
        "a fully acknowledged stream must not arm the reinjection timer"
    );
}

#[tokio::test]
async fn latency_tail_reinjection_dispatches_suffix_on_distinct_reinjection_without_fin() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(118);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let reinjection_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (original_commands, mut original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(118),
        original_key.underlay,
        original_key.path_id,
        original_commands,
        TrafficClass::Latency,
    );
    let (reinjection_commands, mut reinjection_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            reinjection_key.underlay,
            reinjection_key.path_id,
            reinjection_commands,
            TrafficClass::Latency,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Latency,
        underlay: original_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    for value in [0x41, 0x42, 0x43] {
        let frame = send_stream
            .send_data(Bytes::from(vec![value; 64]))
            .expect("seed original-transmission path response data");
        binding.record_original_flight(original_key, &frame);
    }
    let ack_ranges = [OffsetRange { start: 0, end: 128 }];
    let _ = send_stream.apply_ack(&ack_ranges);
    assert_eq!(send_stream.next_offset(), 192);
    assert_eq!(send_stream.reinjection_bytes(), 64);
    assert!(reliable_relay_tail_reinjection_timer_active(
        send_stream.reinjection_bytes(),
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
    response_sender.record_delivered_data(128);
    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        TrafficClass::Latency,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        128,
    );
    assert_eq!(outcome.queued, 1);
    assert!(!outcome.pending);

    let data_ack_outstanding_bytes = send_stream.reinjection_bytes();
    let dispatch = response_sender
        .dispatch_next_with_data_ack_outstanding(
            &path_stream,
            &mut send_stream,
            TrafficClass::Latency,
            limits,
            data_ack_outstanding_bytes,
        )
        .expect("latency tail reinjection must dispatch on a distinct output");
    assert_eq!(dispatch.lane, ReliableWorkClass::Reinjection);
    assert_eq!(dispatch.selected_path, Some(reinjection_key));

    let reinjection_frame = match try_recv_reliable_path_command(&mut reinjection_receivers) {
        Some(ReliablePathCommand::SendFrame(frame)) => {
            assert!(matches!(
                &frame,
                Frame::StreamData {
                    offset: 128,
                    payload,
                    ..
                } if payload.len() == 64
            ));
            frame
        }
        _ => panic!("expected the nonterminal 64-byte reinjected suffix"),
    };
    assert!(try_recv_reliable_path_command(&mut original_receivers).is_none());
    let original_outputs = binding.original_flight_outputs_overlapping_frame(&reinjection_frame);
    assert_eq!(original_outputs.len(), 1);
    assert_eq!(original_outputs[0].0, original_key);
    assert!(
        binding.has_output_incarnation(original_outputs[0].0, original_outputs[0].1),
        "reinjection must preserve the exact original-output attribution",
    );
    assert!(path_stream.has_recent_reinjection_overlap(
        &reinjection_frame,
        reliable_relay_tail_reinjection_delay(None),
    ));
}

#[test]
fn sparse_authoritative_ack_reinjects_the_lowest_live_path_gap() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(119);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let reinjection_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(119),
        original_key.underlay,
        original_key.path_id,
        original_commands,
        TrafficClass::Latency,
    );
    let (reinjection_commands, _reinjection_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            reinjection_key.underlay,
            reinjection_key.path_id,
            reinjection_commands,
            TrafficClass::Latency,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    record_server_delivery_evidence(&binding, reinjection_key);

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Latency,
        underlay: original_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    for value in [0x41, 0x42, 0x43, 0x44] {
        let frame = send_stream
            .send_data(Bytes::from(vec![value; 64]))
            .expect("seed original-transmission path response data");
        binding.record_original_flight(original_key, &frame);
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
    assert_eq!(send_stream.reinjection_bytes(), 128);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(119),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
    );
    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        TrafficClass::Latency,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        64,
    );

    assert_eq!(outcome.queued, 1);
    assert!(!outcome.pending);
    assert_eq!(response_sender.bytes(), 64);
}

#[tokio::test]
async fn sparse_ack_failed_original_reinjection_starts_at_lowest_hole() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(120);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let reinjection_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(120),
        original_key.underlay,
        original_key.path_id,
        original_commands.clone(),
        TrafficClass::Latency,
    );
    let (reinjection_commands, mut reinjection_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            reinjection_key.underlay,
            reinjection_key.path_id,
            reinjection_commands,
            TrafficClass::Latency,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Latency,
        underlay: original_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    for value in [0x51, 0x52, 0x53, 0x54] {
        let frame = send_stream
            .send_data(Bytes::from(vec![value; 64]))
            .expect("seed failed-original-transmission path response data");
        binding.record_original_flight(original_key, &frame);
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
    binding.detach(original_key, &original_commands);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(120),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
    );
    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        TrafficClass::Latency,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        64,
    );
    assert!(outcome.queued > 0);
    let data_ack_outstanding_bytes = send_stream.reinjection_bytes();
    let dispatch = response_sender
        .dispatch_next_with_data_ack_outstanding(
            &path_stream,
            &mut send_stream,
            TrafficClass::Latency,
            limits,
            data_ack_outstanding_bytes,
        )
        .expect("dispatch lowest failed-original-transmission path hole");
    assert_eq!(dispatch.selected_path, Some(reinjection_key));
    assert!(matches!(
        try_recv_reliable_path_command(&mut reinjection_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            offset: 64,
            payload,
            ..
        })) if payload.len() == 64
    ));
}

#[test]
fn tail_reinjection_repeats_after_one_recovery_interval_without_progress() {
    let original_sent_at = Instant::now();
    let last_reinjection = original_sent_at + transport_pto_from_snapshot(None);
    let deadline =
        reliable_relay_tail_reinjection_deadline(original_sent_at, Some(last_reinjection), None);
    let expected =
        tokio::time::Instant::from_std(last_reinjection + transport_pto_from_snapshot(None));

    assert_eq!(deadline, expected);
}

#[test]
fn tail_reinjection_deadline_does_not_move_with_metrics_for_the_same_gap() {
    let original_sent_at = Instant::now();
    let fast_snapshot = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 20.0, 1.0);
    let slow_snapshot = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 800.0, 1.0);
    let mut timer = ReliableRelayTailReinjectionTimer::default();

    let candidate = tail_recovery_candidate(0, original_sent_at);
    let armed = timer.observe(
        Some(candidate),
        original_sent_at,
        Some(fast_snapshot),
        false,
    );
    let refreshed = timer.observe(
        Some(candidate),
        original_sent_at,
        Some(slow_snapshot),
        false,
    );

    assert_eq!(refreshed, armed);
}

#[test]
fn data_ack_recovery_deadline_shortens_but_does_not_postpone_tail_timer() {
    let original_sent_at = Instant::now();
    let slow_snapshot = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 800.0, 1.0);
    let mut timer = ReliableRelayTailReinjectionTimer::default();
    let candidate = tail_recovery_candidate(0, original_sent_at);

    let generic_deadline = timer.observe(
        Some(candidate),
        original_sent_at,
        Some(slow_snapshot),
        false,
    );
    let recovery_deadline = original_sent_at + Duration::from_millis(200);
    assert!(tokio::time::Instant::from_std(recovery_deadline) < generic_deadline);

    timer.arm_recovery_deadline(candidate, recovery_deadline);
    assert_eq!(
        timer.observe(
            Some(candidate),
            original_sent_at,
            Some(slow_snapshot),
            false,
        ),
        tokio::time::Instant::from_std(recovery_deadline)
    );

    timer.arm_recovery_deadline(candidate, original_sent_at + Duration::from_millis(300));
    assert_eq!(
        timer.observe(
            Some(candidate),
            original_sent_at,
            Some(slow_snapshot),
            false,
        ),
        tokio::time::Instant::from_std(recovery_deadline)
    );
}

#[test]
fn tail_reinjection_timer_clears_without_an_authoritative_candidate() {
    let sent_at = Instant::now();
    let mut timer = ReliableRelayTailReinjectionTimer::default();
    let _ = timer.observe(
        Some(tail_recovery_candidate(0, sent_at)),
        sent_at,
        None,
        false,
    );
    assert!(timer.candidate.is_some());
    assert!(timer.deadline.is_some());

    let _ = timer.observe(None, sent_at, None, false);

    assert_eq!(timer.candidate, None);
    assert_eq!(timer.deadline, None);
    assert_eq!(timer.last_attempt_at, None);
}

#[test]
fn tail_reinjection_candidate_uses_the_latest_flight_or_data_ack_progress_time() {
    let first_original_sent_at = Instant::now();
    let first_snapshot = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 20.0, 1.0);
    let next_snapshot = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 800.0, 1.0);
    let mut timer = ReliableRelayTailReinjectionTimer::default();
    let first_deadline = timer.observe(
        Some(tail_recovery_candidate(0, first_original_sent_at)),
        first_original_sent_at,
        Some(first_snapshot),
        false,
    );

    let next_original_sent_at = first_original_sent_at + Duration::from_secs(1);
    let next_candidate = tail_recovery_candidate(64, next_original_sent_at);
    let progress_deadline = timer.observe(
        Some(next_candidate),
        next_original_sent_at,
        Some(next_snapshot),
        false,
    );
    assert!(progress_deadline > first_deadline);

    let attempted_at = next_original_sent_at + Duration::from_secs(1);
    timer.record_attempt_at(attempted_at);
    let attempt_deadline = timer.observe(
        Some(next_candidate),
        next_original_sent_at,
        Some(first_snapshot),
        false,
    );
    assert_eq!(
        attempt_deadline,
        reliable_relay_tail_reinjection_deadline(
            next_original_sent_at,
            Some(attempted_at),
            Some(first_snapshot),
        )
    );
}

#[test]
fn new_original_flight_does_not_inherit_pre_send_data_ack_stall_time() {
    let data_ack_progress_at = Instant::now();
    let original_sent_at = data_ack_progress_at + Duration::from_millis(250);
    let mut timer = ReliableRelayTailReinjectionTimer::default();
    let deadline = timer.observe(
        Some(tail_recovery_candidate(0, original_sent_at)),
        data_ack_progress_at,
        None,
        false,
    );

    assert_eq!(
        deadline,
        tokio::time::Instant::from_std(original_sent_at + transport_pto_from_snapshot(None)),
        "recovery time begins when the blocking range exists, not when the stream last had no data",
    );
}

#[test]
fn data_ack_progress_rearms_live_path_recovery_for_an_old_original_flight() {
    let original_sent_at = Instant::now() - Duration::from_secs(2);
    let data_ack_progress_at = Instant::now();
    let mut timer = ReliableRelayTailReinjectionTimer::default();
    let deadline = timer.observe(
        Some(tail_recovery_candidate(64, original_sent_at)),
        data_ack_progress_at,
        None,
        false,
    );

    assert_eq!(
        deadline,
        tokio::time::Instant::from_std(data_ack_progress_at + transport_pto_from_snapshot(None)),
        "live-path Data ACK progress starts a new connection-level recovery interval"
    );
}

#[test]
fn failed_original_retry_keeps_pacing_across_ack_progress() {
    let original_sent_at = Instant::now() - Duration::from_secs(2);
    let attempted_at = Instant::now();
    let mut timer = ReliableRelayTailReinjectionTimer::default();
    let _ = timer.observe(
        Some(tail_recovery_candidate(0, original_sent_at)),
        original_sent_at,
        None,
        true,
    );
    timer.record_attempt_at(attempted_at);

    let deadline = timer.observe(
        Some(tail_recovery_candidate(64, original_sent_at)),
        attempted_at,
        None,
        true,
    );

    assert_eq!(
        deadline,
        tokio::time::Instant::from_std(attempted_at + transport_pto_from_snapshot(None)),
        "ACK progress on a failed carrier must not trigger an unpaced retry cascade"
    );
}

#[test]
fn empty_tail_reinjection_scan_retries_after_one_recovery_interval() {
    let sent_at = Instant::now();
    let mut timer = ReliableRelayTailReinjectionTimer::default();
    let candidate = tail_recovery_candidate(0, sent_at);
    let _ = timer.observe(Some(candidate), sent_at, None, false);

    let scan_started_at = Instant::now();
    timer.record_scan();
    let retry_deadline = timer.observe(Some(candidate), sent_at, None, false);

    assert!(
        retry_deadline
            >= tokio::time::Instant::from_std(
                scan_started_at + reliable_relay_tail_reinjection_delay(None),
            ),
        "an empty scan remains time-wakeable when carrier capacity changes without a model update",
    );
}

#[test]
fn live_tail_reinjection_timer_uses_blocking_original_snapshot() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(110);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let fast_alternate = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(110),
        original_key.underlay,
        original_key.path_id,
        original_commands,
        TrafficClass::Throughput,
    );
    let (alternate_commands, _alternate_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            fast_alternate.underlay,
            fast_alternate.path_id,
            alternate_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let slow_original_metrics = PathMetrics {
        path_id: original_key.path_id,
        underlay: original_key.underlay,
        direction: PathMetricDirection::ServerToClient,
        metric_epoch: metric_epoch_now(),
        metric_age_us: 0,
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
        srtt_us: 25_000,
        rttvar_us: 2_000,
        jitter_us: 2_000,
        delivery_rate_bps: 200_000_000,
        pacing_rate_bps: 200_000_000,
        ..slow_original_metrics
    };
    binding.update_path_metrics(
        original_key,
        slow_original_metrics,
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
        lane: TrafficClass::Throughput,
        underlay: original_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let original_frame = Frame::StreamData {
        stream_id,
        offset: 1024,
        payload: Bytes::from(vec![0x55; 65_536]),
    };
    binding.record_original_flight(original_key, &original_frame);

    let snapshot = path_stream
        .tail_reinjection_snapshot(1024, TrafficClass::Throughput, 65_536)
        .expect("blocking original-transmission path path is still attached");

    assert_eq!(snapshot.id, original_key.path_id);
    assert_eq!(snapshot.underlay, original_key.underlay);
    assert!(
        transport_pto_from_snapshot(Some(snapshot))
            > transport_pto_from_snapshot(
                path_stream.send_path_snapshot(TrafficClass::Throughput, 65_536)
            ),
        "tail reinjection timing must follow the blocking OriginalData path, not the fastest attached alternate"
    );
}

#[test]
fn failed_original_tail_reinjection_is_immediate_after_original_path_detaches() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(111);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let failover_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(111),
        original_key.underlay,
        original_key.path_id,
        original_commands.clone(),
        TrafficClass::Throughput,
    );
    let (failover_commands, _failover_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            failover_key.underlay,
            failover_key.path_id,
            failover_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: failover_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x51; 4096]))
        .expect("prepare original-transmission path data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit original-transmission path data");
    binding.record_original_flight(original_key, &frame);
    binding.detach(original_key, &original_commands);
    let ack_ranges = [OffsetRange {
        start: 0,
        end: 1024,
    }];
    let _ = send_stream.apply_ack(&ack_ranges);

    let original_sent_at = Instant::now();
    let generic_deadline = reliable_relay_tail_reinjection_deadline(original_sent_at, None, None);
    let failover_deadline = reliable_relay_effective_tail_reinjection_deadline(
        original_sent_at,
        None,
        None,
        reliable_failed_original_tail_reinjection_ready(&path_stream, &send_stream),
    );

    assert_eq!(
        failover_deadline,
        tokio::time::Instant::from_std(original_sent_at),
        "detached-original-transmission path tail reinjection should not wait a generic PTO before failing over"
    );
    assert!(
        failover_deadline < generic_deadline,
        "failed-original-transmission path reinjection timing must be faster than live-original-transmission path tail reinjection"
    );
}

#[test]
fn failed_original_tail_reinjection_retry_uses_single_pto_not_persistent_backoff() {
    let original_sent_at = Instant::now();
    let last_reinjection = original_sent_at + Duration::from_millis(1);
    let slow_stale_original = PathSnapshot::new(PathId(9), UnderlayProtocol::Tcp, 20.0, 1.0);

    let deadline = reliable_relay_effective_tail_reinjection_deadline(
        original_sent_at,
        Some(last_reinjection),
        Some(slow_stale_original),
        true,
    );
    let expected =
        tokio::time::Instant::from_std(last_reinjection + transport_pto_from_snapshot(None));

    assert_eq!(
        deadline, expected,
        "failed-original-transmission path reinjection may fire immediately once, then retries one bounded reinjection quantum per PTO; persistent backoff is for live original-transmission path congestion recovery, not detached-original-transmission path failover"
    );
}

#[test]
fn live_tail_reinjection_is_one_product_quantum_for_every_underlay() {
    let limits = MuxLimits::default();
    for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
        let path = PathSnapshot::new(PathId(3), underlay, 180.0, 500_000_000.0);
        let base =
            adaptive_reliable_relay_reinjection_bytes(Some(path), TrafficClass::Throughput, limits);
        assert_eq!(
            reliable_critical_tail_reinjection_limit_bytes(base, limits.max_repair_bytes, limits,),
            base,
            "live-tail recovery must not synthesize a transport-sized flight above native recovery",
        );
    }
}

#[test]
fn live_tail_stall_reinjection_is_not_queued_even_with_optional_budget() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(98);
    let base_limit = MAX_RELIABLE_SERVICE_QUANTUM_BYTES.min(reliable_relay_buffer_len(limits));
    let reinjection_debt = base_limit.saturating_mul(8);
    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(98),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 1,
        },
    );
    let initial_budget = response_sender.reinjection_extra_budget_remaining(limits);
    assert!(initial_budget > 0);

    let (commands, _receivers) = reliable_path_command_channels(8);
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::fixed(UnderlayProtocol::Tcp, PathId(0), commands, limits),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    send_stream
        .send_data(Bytes::from(vec![0x32; reinjection_debt]))
        .expect("original-transmission path data");
    let ack_frontier = base_limit as u64;

    let outcome = enqueue_reliable_tail_reinjection(
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
        TrafficClass::Throughput,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 1,
        },
        path_stream.max_frame_payload_bytes,
        ack_frontier,
    );

    assert_eq!(
        outcome.queued, 0,
        "live contiguous original-transmission path-tail bytes are neither ACK-gap nor final-tail correctness reinjection"
    );
    assert!(!outcome.pending);
    assert!(
        !outcome.has_reinjection_attempt(),
        "an empty scan must wait for carrier-state change without rewriting the recovery clock"
    );
}

#[test]
fn failed_original_tail_reinjection_uses_remaining_output_after_persistent_stall() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(99);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let failover_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(99),
        original_key.underlay,
        original_key.path_id,
        original_commands.clone(),
        TrafficClass::Throughput,
    );
    let (failover_commands, _failover_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            failover_key.underlay,
            failover_key.path_id,
            failover_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: failover_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x42; 4096]))
        .expect("prepare original-transmission path data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit original-transmission path data");
    binding.record_original_flight(original_key, &frame);
    binding.detach(original_key, &original_commands);
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
    response_sender.record_delivered_data(1024);

    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        TrafficClass::Throughput,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        1024,
    );

    assert_eq!(
        outcome.queued, 1,
        "a detached original-transmission path path turns a persistent contiguous tail into failover reinjection on the remaining output"
    );
    assert!(!outcome.pending);
}

#[test]
fn failed_original_tail_reinjection_queues_one_bounded_target_flight() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(121);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let failover_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(121),
        original_key.underlay,
        original_key.path_id,
        original_commands.clone(),
        TrafficClass::Throughput,
    );
    let (failover_commands, _failover_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            failover_key.underlay,
            failover_key.path_id,
            failover_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: failover_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let unresolved_payload_len = reliable_relay_buffer_len(limits)
        .saturating_add(MAX_RELIABLE_SERVICE_QUANTUM_BYTES)
        .min(limits.max_payload_bytes);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x52; unresolved_payload_len]))
        .expect("prepare original-transmission path data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit original-transmission path data");
    binding.record_original_flight(original_key, &frame);
    binding.detach(original_key, &original_commands);
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
    response_sender.record_delivered_data(1024);

    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        TrafficClass::Throughput,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        1024,
    );

    assert_eq!(outcome.queued, 1);
    assert!(
        response_sender.bytes() > MAX_RELIABLE_SERVICE_QUANTUM_BYTES,
        "path failure recovery must not serialize a modeled target flight into 64 KiB PTO steps"
    );
    assert!(
        response_sender.bytes() <= limits.max_path_flight_bytes.min(limits.max_repair_bytes),
        "failed-path reinjection remains bounded by the configured product flight envelope"
    );
}

#[test]
fn unknown_original_tail_reinjection_uses_remaining_output_after_persistent_stall() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(119);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let failover_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(119),
        original_key.underlay,
        original_key.path_id,
        original_commands.clone(),
        TrafficClass::Throughput,
    );
    let (failover_commands, _failover_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            failover_key.underlay,
            failover_key.path_id,
            failover_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    binding.detach(original_key, &original_commands);

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: failover_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x43; 4096]))
        .expect("prepare original-transmission path data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit original-transmission path data");
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
    response_sender.record_delivered_data(1024);

    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        TrafficClass::Throughput,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        1024,
    );

    assert_eq!(
        outcome.queued, 1,
        "when retained original-transmission path bytes have no live original-transmission path and no path-flight record, persistent tail reinjection must still use a live survivor instead of deadlocking"
    );
    assert!(!outcome.pending);
}

#[tokio::test]
async fn unknown_original_tail_reinjection_dispatches_as_path_failure_reinjection() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(120);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let failover_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(120),
        original_key.underlay,
        original_key.path_id,
        original_commands.clone(),
        TrafficClass::Throughput,
    );
    let (failover_commands, mut failover_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            failover_key.underlay,
            failover_key.path_id,
            failover_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    binding.detach(original_key, &original_commands);

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: failover_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x43; 4096]))
        .expect("prepare original-transmission path data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit original-transmission path data");
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
    response_sender.record_delivered_data(1024);

    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        TrafficClass::Throughput,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        1024,
    );
    assert_eq!(outcome.queued, 1);

    let data_ack_outstanding_bytes = send_stream.reinjection_bytes();
    let dispatch = response_sender
        .dispatch_next_with_data_ack_outstanding(
            &path_stream,
            &mut send_stream,
            TrafficClass::Throughput,
            limits,
            data_ack_outstanding_bytes,
        )
        .expect(
            "unknown-original-transmission path tail reinjection must be failover-dispatchable",
        );

    assert_eq!(dispatch.lane, ReliableWorkClass::Reinjection);
    assert_eq!(dispatch.selected_path, Some(failover_key));
    assert!(matches!(
        try_recv_reliable_path_command(&mut failover_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
}

#[test]
fn live_original_without_data_ack_waits_for_authoritative_gap() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(121);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let reinjection_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(121),
        original_key.underlay,
        original_key.path_id,
        original_commands,
        TrafficClass::Throughput,
    );
    let (reinjection_commands, _reinjection_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            reinjection_key.underlay,
            reinjection_key.path_id,
            reinjection_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: reinjection_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x48; 4096]))
        .expect("prepare original-transmission path data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit original-transmission path data");
    binding.record_original_flight(original_key, &frame);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(121),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
    );
    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &[],
        false,
        None,
        TrafficClass::Throughput,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        0,
    );

    assert_eq!(
        outcome.queued, 0,
        "no ACK frontier is not an authoritative product gap; live-original-transmission path recovery must wait for ACK progress, failed-original-transmission path evidence, or terminal-tail reinjection"
    );
    assert!(!outcome.pending);
}

#[test]
fn live_original_without_data_ack_does_not_probe_prefix() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(122);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let reinjection_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(122),
        original_key.underlay,
        original_key.path_id,
        original_commands,
        TrafficClass::Throughput,
    );
    let (reinjection_commands, _reinjection_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            reinjection_key.underlay,
            reinjection_key.path_id,
            reinjection_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: reinjection_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let total = reliable_relay_buffer_len(limits).saturating_mul(4);
    let mut remaining = total;
    while remaining > 0 {
        let chunk = remaining.min(limits.max_payload_bytes);
        let frame = send_stream
            .prepare_data(Bytes::from(vec![0x48; chunk]))
            .expect("prepare original-transmission path data");
        send_stream
            .commit_prepared_data(&frame)
            .expect("commit original-transmission path data");
        binding.record_original_flight(original_key, &frame);
        remaining = remaining.saturating_sub(chunk);
    }

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(122),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
    );
    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &[],
        false,
        None,
        TrafficClass::Throughput,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        0,
    );

    assert_eq!(
        outcome.queued, 0,
        "no-frontier live-original-transmission path data may still be in carrier recovery and must not become product ReinjectedData"
    );
    assert!(!outcome.pending);
    assert_eq!(response_sender.bytes(), 0);
}

#[test]
fn unknown_original_tail_reinjection_without_ack_frontier_waits() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(120);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let failover_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(120),
        original_key.underlay,
        original_key.path_id,
        original_commands.clone(),
        TrafficClass::Throughput,
    );
    let (failover_commands, _failover_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            failover_key.underlay,
            failover_key.path_id,
            failover_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    binding.detach(original_key, &original_commands);

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: failover_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x44; 4096]))
        .expect("prepare original-transmission path data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit original-transmission path data");

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(120),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
    );

    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &[],
        false,
        None,
        TrafficClass::Throughput,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        0,
    );

    assert_eq!(
        outcome.queued, 0,
        "unknown-original-transmission path reinjection needs an ACK frontier; without one it can duplicate the entire startup tail and inflate overhead"
    );
    assert!(!outcome.pending);
}

#[test]
fn failed_original_tail_reinjection_does_not_duplicate_queued_reinjection_range() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(109);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let failover_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(109),
        original_key.underlay,
        original_key.path_id,
        original_commands.clone(),
        TrafficClass::Throughput,
    );
    let (failover_commands, _failover_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            failover_key.underlay,
            failover_key.path_id,
            failover_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: failover_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x43; 4096]))
        .expect("prepare original-transmission path data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit original-transmission path data");
    binding.record_original_flight(original_key, &frame);
    binding.detach(original_key, &original_commands);
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
    response_sender.record_delivered_data(1024);
    let performance = MppPerformanceConfig {
        extra_traffic_hint_percent: 5,
    };

    let first = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        TrafficClass::Throughput,
        limits,
        performance,
        path_stream.max_frame_payload_bytes,
        1024,
    );
    let queued_bytes_after_first = response_sender.bytes();
    let second = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        TrafficClass::Throughput,
        limits,
        performance,
        path_stream.max_frame_payload_bytes,
        1024,
    );

    assert_eq!(first.queued, 1);
    assert!(!first.pending);
    assert_eq!(
        second.queued, 0,
        "tail reinjection must not enqueue the same ReinjectedData range while it is already queued"
    );
    assert!(
        second.pending,
        "already queued ReinjectedData should count as a pending reinjection attempt so the tail timer backs off"
    );
    assert_eq!(response_sender.bytes(), queued_bytes_after_first);
}

#[test]
fn tail_reinjection_treats_live_inflight_reinjection_as_pending() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(127);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let reinjection_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(127),
        original_key.underlay,
        original_key.path_id,
        original_commands,
        TrafficClass::Throughput,
    );
    let (reinjection_commands, _reinjection_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            reinjection_key.underlay,
            reinjection_key.path_id,
            reinjection_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: original_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x49; 4096]))
        .expect("prepare original-transmission path data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit original-transmission path data");
    binding.record_original_flight(original_key, &frame);
    let ack_ranges = [OffsetRange {
        start: 0,
        end: 1024,
    }];
    let _ = send_stream.apply_ack(&ack_ranges);
    let inflight_reinjection = send_stream
        .retransmission_frames_after_ack_frontier(&ack_ranges, 1024)
        .into_iter()
        .next()
        .expect("expected frontier reinjection frame");
    binding.record_reinjected_flight(reinjection_key, &inflight_reinjection);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(127),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
    );
    response_sender.record_delivered_data(1024);

    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        TrafficClass::Throughput,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        1024,
    );

    assert_eq!(
        outcome.queued, 0,
        "live in-flight ReinjectedData for the same range must not be stacked"
    );
    assert!(
        outcome.pending,
        "live in-flight ReinjectedData should keep the tail reinjection timer backed off"
    );
    assert_eq!(response_sender.bytes(), 0);
}

#[test]
fn persistent_tail_reinjection_waits_when_live_reinjection_copy_is_in_flight() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(105);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let reinjection_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(105),
        original_key.underlay,
        original_key.path_id,
        original_commands,
        TrafficClass::Throughput,
    );
    let (reinjection_commands, _reinjection_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            reinjection_key.underlay,
            reinjection_key.path_id,
            reinjection_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: original_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x48; 4096]))
        .expect("prepare original-transmission path data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit original-transmission path data");
    binding.record_original_flight(original_key, &frame);
    binding.record_reinjected_flight(reinjection_key, &frame);
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
    response_sender.record_delivered_data(1024);

    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        TrafficClass::Throughput,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        1024,
    );

    assert_eq!(
        outcome.queued, 0,
        "persistent tail reinjection must not stack another copy while a live ReinjectedData flight already covers the frontier range"
    );
    assert!(
        outcome.pending,
        "live in-flight ReinjectedData should back off the tail reinjection timer"
    );
    assert_eq!(response_sender.bytes(), 0);
}

#[test]
fn stale_live_reinjection_flight_does_not_block_terminal_tail_retry() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(106);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let reinjection_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(106),
        original_key.underlay,
        original_key.path_id,
        original_commands,
        TrafficClass::Throughput,
    );
    let (reinjection_commands, _reinjection_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            reinjection_key.underlay,
            reinjection_key.path_id,
            reinjection_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: original_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x4a; 4096]))
        .expect("prepare original-transmission path data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit original-transmission path data");
    binding.record_original_flight(original_key, &frame);
    let ack_ranges = [OffsetRange {
        start: 0,
        end: 1024,
    }];
    let _ = send_stream.apply_ack(&ack_ranges);
    let inflight_reinjection = send_stream
        .retransmission_frames_after_ack_frontier(&ack_ranges, 1024)
        .into_iter()
        .next()
        .expect("expected frontier reinjection frame");
    binding.record_reinjected_flight(reinjection_key, &inflight_reinjection);
    binding.age_reinjected_flights_for_test(
        reliable_relay_tail_reinjection_delay(None).saturating_add(Duration::from_millis(1)),
    );

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(106),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
    );
    response_sender.record_delivered_data(1024);

    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        TrafficClass::Throughput,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        1024,
    );

    assert_eq!(
        outcome.queued, 1,
        "stale unacked ReinjectedData must not suppress correctness reinjection forever"
    );
    assert!(
        !outcome.pending,
        "stale ReinjectedData should be retried instead of keeping the tail timer backed off"
    );
    assert!(response_sender.bytes() > 0);
}

#[tokio::test]
async fn live_tail_reinjection_uses_repair_headroom_before_new_data() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(127);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let reinjection_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(127),
        original_key.underlay,
        original_key.path_id,
        original_commands,
        TrafficClass::Throughput,
    );
    let (reinjection_commands, mut reinjection_receivers) = reliable_path_command_channels(1);
    let reinjection_commands_for_fill = reinjection_commands.clone();
    assert_eq!(
        binding.attach(
            reinjection_key.underlay,
            reinjection_key.path_id,
            reinjection_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    binding.mark_output_path_proven_for_test(original_key);
    binding.mark_output_path_proven_for_test(reinjection_key);
    reinjection_commands_for_fill
        .try_enqueue_admitted_frame(
            Frame::StreamData {
                stream_id,
                offset: 4096,
                payload: Bytes::from_static(b"queued"),
            },
            TrafficClass::Throughput,
        )
        .expect("test setup fills alternate data queue");

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: original_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x55; 4096]))
        .expect("prepare original-transmission path data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit original-transmission path data");
    binding.record_original_flight(original_key, &frame);
    let ack_ranges = [OffsetRange {
        start: 0,
        end: 1024,
    }];
    let _ = send_stream.apply_ack(&ack_ranges);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(127),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
    );
    response_sender.record_delivered_data(1024);
    response_sender.enqueue_data_for_lane(
        Bytes::from_static(b"new response data"),
        TrafficClass::Throughput,
    );

    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        TrafficClass::Throughput,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        1024,
    );

    assert_eq!(outcome.queued, 1);
    assert!(!outcome.pending);
    let data_ack_outstanding_bytes = send_stream.reinjection_bytes();
    let dispatch = response_sender
        .dispatch_next_with_data_ack_outstanding(
            &path_stream,
            &mut send_stream,
            TrafficClass::Throughput,
            limits,
            data_ack_outstanding_bytes,
        )
        .expect("repair remains dispatchable despite a full fresh-data queue");
    assert_eq!(dispatch.lane, ReliableWorkClass::Reinjection);
    assert_eq!(dispatch.selected_path, Some(reinjection_key));
    assert!(matches!(
        try_recv_reliable_path_command(&mut reinjection_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            offset: 1024,
            ..
        }))
    ));
    assert!(matches!(
        try_recv_reliable_path_command(&mut reinjection_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            offset: 4096,
            ..
        }))
    ));

    let dispatch = response_sender
        .dispatch_next_with_data_ack_outstanding(
            &path_stream,
            &mut send_stream,
            TrafficClass::Throughput,
            limits,
            data_ack_outstanding_bytes,
        )
        .expect("new data follows the dispatched repair");
    assert_eq!(dispatch.lane, ReliableWorkClass::Data);
}

#[tokio::test]
async fn persistent_tail_reinjection_waits_when_distinct_alternate_lacks_repair_headroom() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(124);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let reinjection_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (original_commands, mut original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(124),
        original_key.underlay,
        original_key.path_id,
        original_commands,
        TrafficClass::Throughput,
    );
    let (reinjection_commands, _reinjection_receivers) = reliable_path_command_channels(1);
    let reinjection_commands_for_fill = reinjection_commands.clone();
    assert_eq!(
        binding.attach(
            reinjection_key.underlay,
            reinjection_key.path_id,
            reinjection_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: original_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x55; 4096]))
        .expect("prepare original-transmission path data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit original-transmission path data");
    binding.record_original_flight(original_key, &frame);
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
    response_sender.record_delivered_data(1024);

    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        TrafficClass::Throughput,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        1024,
    );
    assert_eq!(outcome.queued, 1);

    reinjection_commands_for_fill
        .try_enqueue_reinjection_frame(
            Frame::StreamData {
                stream_id,
                offset: 4096,
                payload: Bytes::from_static(b"queued"),
            },
            TrafficClass::Throughput,
        )
        .expect("test setup fills alternate repair headroom");

    let dispatch = response_sender.dispatch_next_with_data_ack_outstanding(
        &path_stream,
        &mut send_stream,
        TrafficClass::Throughput,
        limits,
        0,
    );

    assert!(matches!(dispatch, Err(RuntimeError::SenderServiceBlocked)));
    assert!(
        try_recv_reliable_path_command(&mut original_receivers).is_none(),
        "live-original-transmission path tail reinjection must wait rather than retransmit on its original-transmission path"
    );
    let completed = [OffsetRange {
        start: 0,
        end: send_stream.next_offset(),
    }];
    let _ = send_stream.apply_ack(&completed);
    path_stream.release_normalized_acked_ranges(&completed);
    response_sender.release_normalized_acked_reinjections(&completed);
    assert!(
        response_sender.is_empty(),
        "original-transmission path ACK progress must remove a blocked queued live-tail reinjection before FIN or later data"
    );
}

#[tokio::test]
async fn final_tail_reinjection_uses_original_path_when_alternate_lacks_credit() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(125);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let reinjection_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(1),
    };
    let (original_commands, mut original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(125),
        original_key.underlay,
        original_key.path_id,
        original_commands,
        TrafficClass::Latency,
    );
    let (reinjection_commands, _reinjection_receivers) = reliable_path_command_channels(1);
    let reinjection_commands_for_fill = reinjection_commands.clone();
    assert_eq!(
        binding.attach(
            reinjection_key.underlay,
            reinjection_key.path_id,
            reinjection_commands,
            TrafficClass::Latency,
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Latency,
        underlay: original_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x56; 192]))
        .expect("prepare original-transmission path data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit original-transmission path data");
    binding.record_original_flight(original_key, &frame);
    let ack_ranges = [OffsetRange { start: 0, end: 128 }];
    let _ = send_stream.apply_ack(&ack_ranges);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(125),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
    );
    response_sender.record_delivered_data(128);

    let (reinjection_frames, blocked_frontier_offset, same_output_frontier_retransmit) =
        prefix_final_tail_reinjection_frames_with_available_output(
            &path_stream,
            stream_final_offset_tail_reinjection_frames_normalized(
                &send_stream,
                &ack_ranges,
                64,
                true,
                true,
            ),
        );
    assert_eq!(blocked_frontier_offset, None);
    assert!(!same_output_frontier_retransmit);
    assert_eq!(reinjection_frames.len(), 1);
    for frame in reinjection_frames {
        let _ = response_sender.enqueue_critical_tail_reinjection_frame(frame);
    }

    reinjection_commands_for_fill
        .try_enqueue_reinjection_frame(
            Frame::StreamData {
                stream_id,
                offset: 192,
                payload: Bytes::from_static(b"queued"),
            },
            TrafficClass::Latency,
        )
        .expect("test setup fills alternate repair headroom");

    response_sender
        .dispatch_next_with_data_ack_outstanding(
            &path_stream,
            &mut send_stream,
            TrafficClass::Latency,
            limits,
            0,
        )
        .expect("final-tail reinjection must use the original path when the alternate has no queue credit");

    let command = try_recv_reliable_path_command(&mut original_receivers)
        .expect("expected final-tail reinjection on the original path");
    match command {
        ReliablePathCommand::SendFrame(Frame::StreamData {
            offset, payload, ..
        }) => {
            assert_eq!(offset, 128);
            assert_eq!(payload.len(), 64);
        }
        _ => panic!("expected final-tail reinjected STREAM_DATA"),
    }
}

#[tokio::test]
async fn final_tail_reinjection_uses_only_available_path() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(126);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let (original_commands, mut original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(126),
        original_key.underlay,
        original_key.path_id,
        original_commands,
        TrafficClass::Latency,
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Latency,
        underlay: original_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x57; 192]))
        .expect("prepare original-transmission path data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit original-transmission path data");
    binding.record_original_flight(original_key, &frame);
    let ack_ranges = [OffsetRange { start: 0, end: 128 }];
    let _ = send_stream.apply_ack(&ack_ranges);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(126),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
    );
    response_sender.record_delivered_data(128);

    let (reinjection_frames, blocked_frontier_offset, same_output_frontier_retransmit) =
        prefix_final_tail_reinjection_frames_with_available_output(
            &path_stream,
            stream_final_offset_tail_reinjection_frames_normalized(
                &send_stream,
                &ack_ranges,
                64,
                true,
                true,
            ),
        );
    assert_eq!(blocked_frontier_offset, None);
    assert!(same_output_frontier_retransmit);
    assert_eq!(reinjection_frames.len(), 1);
    for frame in reinjection_frames {
        let _ = response_sender.enqueue_critical_tail_reinjection_frame(frame);
    }

    response_sender
        .dispatch_next_with_data_ack_outstanding(
            &path_stream,
            &mut send_stream,
            TrafficClass::Latency,
            limits,
            0,
        )
        .expect("final-tail reinjection must use the only available path");

    let command = try_recv_reliable_path_command(&mut original_receivers)
        .expect("expected final-tail reinjection on the available path");
    match command {
        ReliablePathCommand::SendFrame(Frame::StreamData {
            offset, payload, ..
        }) => {
            assert_eq!(offset, 128);
            assert_eq!(payload.len(), 64);
        }
        _ => panic!("expected final-tail reinjected STREAM_DATA"),
    }
}

#[tokio::test]
async fn failed_original_reinjection_without_ack_frontier_starts_at_zero() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(103);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let failover_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(103),
        original_key.underlay,
        original_key.path_id,
        original_commands.clone(),
        TrafficClass::Throughput,
    );
    let (failover_commands, mut failover_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            failover_key.underlay,
            failover_key.path_id,
            failover_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: failover_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x46; 4096]))
        .expect("prepare original-transmission path data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit original-transmission path data");
    binding.record_original_flight(original_key, &frame);
    binding.detach(original_key, &original_commands);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(103),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
    );

    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &[],
        false,
        None,
        TrafficClass::Throughput,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        0,
    );

    assert_eq!(
        outcome.queued, 1,
        "failed-original-transmission path reinjection must retransmit from offset zero when no response ACK frontier exists"
    );
    assert!(!outcome.pending);
    response_sender
        .dispatch_next_with_data_ack_outstanding(
            &path_stream,
            &mut send_stream,
            TrafficClass::Throughput,
            limits,
            0,
        )
        .expect("dispatch failed-original-transmission path reinjection");
    let command =
        try_recv_reliable_path_command(&mut failover_receivers).expect("reinjection frame");
    match command {
        ReliablePathCommand::SendFrame(Frame::StreamData {
            offset, payload, ..
        }) => {
            assert_eq!(offset, 0);
            assert!(!payload.is_empty());
        }
        _ => panic!("expected failed-original-transmission path reinjection STREAM_DATA"),
    }
}

#[test]
fn live_original_tail_without_ack_frontier_does_not_reinjection_on_alternate() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(104);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let alternative_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(104),
        original_key.underlay,
        original_key.path_id,
        original_commands,
        TrafficClass::Throughput,
    );
    let (alternative_commands, _alternative_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            alternative_key.underlay,
            alternative_key.path_id,
            alternative_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: original_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x47; 4096]))
        .expect("prepare original-transmission path data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit original-transmission path data");

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(104),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
    );

    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &[],
        false,
        None,
        TrafficClass::Throughput,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        0,
    );

    assert_eq!(
        outcome.queued, 0,
        "without a complete ACK frontier or failed original-transmission path, live original-transmission path bytes are normal in-flight data and must not be duplicated onto an alternate"
    );
    assert!(!outcome.pending);
}

#[test]
fn failed_original_tail_reinjection_is_not_blocked_by_optional_budget() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(101);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let failover_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(101),
        original_key.underlay,
        original_key.path_id,
        original_commands.clone(),
        TrafficClass::Throughput,
    );
    let (failover_commands, _failover_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            failover_key.underlay,
            failover_key.path_id,
            failover_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: failover_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x44; 4096]))
        .expect("prepare original-transmission path data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit original-transmission path data");
    binding.record_original_flight(original_key, &frame);
    binding.detach(original_key, &original_commands);
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
    let optional_budget = response_sender.reinjection_extra_budget_remaining(limits);
    assert!(optional_budget > 0);
    assert!(
        response_sender
            .enqueue_reinjection_frame_with_priority(
                Frame::StreamData {
                    stream_id,
                    offset: 10_000,
                    payload: Bytes::from(vec![0x99; optional_budget]),
                },
                limits,
                true,
            )
            .is_some()
    );
    assert_eq!(
        response_sender.reinjection_extra_event_budget_remaining(limits),
        0
    );

    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        TrafficClass::Throughput,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        1024,
    );

    assert!(
        outcome.queued > 0,
        "failed-original-transmission path tail recovery is correctness reinjection and must not depend on optional duplicate/probe budget"
    );
    assert!(!outcome.pending);
}

#[test]
fn ack_gap_timer_retransmission_is_not_optional_traffic() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(102);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let reinjection_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(102),
        original_key.underlay,
        original_key.path_id,
        original_commands,
        TrafficClass::Throughput,
    );
    let (reinjection_commands, _reinjection_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            reinjection_key.underlay,
            reinjection_key.path_id,
            reinjection_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    record_server_delivery_evidence(&binding, reinjection_key);

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: original_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x45; 4096]))
        .expect("prepare original-transmission path data");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit original-transmission path data");
    binding.record_original_flight(original_key, &frame);
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
    let optional_budget = response_sender.reinjection_extra_budget_remaining(limits);
    assert!(optional_budget > 0);
    assert!(
        response_sender
            .enqueue_reinjection_frame_with_priority(
                Frame::StreamData {
                    stream_id,
                    offset: 10_000,
                    payload: Bytes::from(vec![0x99; optional_budget]),
                },
                limits,
                true,
            )
            .is_some()
    );
    assert_eq!(
        response_sender.reinjection_extra_event_budget_remaining(limits),
        0
    );

    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        TrafficClass::Throughput,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        4096,
    );

    assert_eq!(outcome.queued, 1);
    assert!(!outcome.pending);
}

#[test]
fn persistent_ack_gap_timer_refills_a_measured_target_service_window() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(103);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let reinjection_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(103),
        original_key.underlay,
        original_key.path_id,
        original_commands,
        TrafficClass::Throughput,
    );
    let (reinjection_commands, mut reinjection_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            reinjection_key.underlay,
            reinjection_key.path_id,
            reinjection_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    record_server_delivery_evidence(&binding, reinjection_key);

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: original_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let quantum = MAX_RELIABLE_SERVICE_QUANTUM_BYTES;
    let frame = send_stream
        .prepare_data(Bytes::from(vec![0x51; quantum * 9]))
        .expect("prepare sparse ACK-gap flight");
    send_stream
        .commit_prepared_data(&frame)
        .expect("commit sparse ACK-gap flight");
    binding.record_original_flight(original_key, &frame);
    let ack_ranges = [
        OffsetRange {
            start: 0,
            end: quantum as u64,
        },
        OffsetRange {
            start: (quantum * 2) as u64,
            end: (quantum * 3) as u64,
        },
        OffsetRange {
            start: (quantum * 4) as u64,
            end: (quantum * 5) as u64,
        },
        OffsetRange {
            start: (quantum * 6) as u64,
            end: (quantum * 7) as u64,
        },
        OffsetRange {
            start: (quantum * 8) as u64,
            end: (quantum * 9) as u64,
        },
    ];
    let validated_ack = begin_reliable_stream_ack(&send_stream, true, ack_ranges.to_vec())
        .expect("validate sparse ACK-gap snapshot");
    let _ = send_stream.apply_validated_ack(&validated_ack);
    let mut authoritative_ack = AuthoritativeStreamAckSnapshot::default();
    update_reinjection_authoritative_ack_snapshot(&mut authoritative_ack, &validated_ack);
    let modeled_path = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 500.0, 400_000_000.0);
    let base_limit = adaptive_reliable_relay_chunk_bytes_with_frame_limit(
        Some(modeled_path),
        TrafficClass::Throughput,
        limits,
        path_stream.max_frame_payload_bytes,
    )
    .max(adaptive_reliable_relay_reinjection_bytes(
        Some(modeled_path),
        TrafficClass::Throughput,
        limits,
    ));
    assert_eq!(base_limit, quantum);

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(103),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
    );
    let mut progress = ReliableAckGapReinjectionProgress::default();
    progress.arm_recovery_deadline(
        true,
        &ack_ranges,
        true,
        Some(Instant::now() - Duration::from_secs(1)),
    );
    let outcome = evaluate_server_data_ack_reinjection(
        &mut response_sender,
        &path_stream,
        &send_stream,
        &mut progress,
        &authoritative_ack,
        quantum as u64,
        Some(modeled_path),
        Some(modeled_path),
        TrafficClass::Throughput,
        limits,
        stream_id,
    );

    assert_eq!(
        outcome.queued, 4,
        "a proven recovery-copy timeout may service every exact omitted range within the measured target window"
    );
    assert!(
        response_sender.bytes() > base_limit,
        "persistent timer service must not collapse back to one liveness quantum"
    );
    assert!(outcome.persistent_ready);
    assert!(progress.repeat_reinjection_deadline().is_some());
    let data_ack_outstanding_bytes = send_stream.reinjection_bytes();
    let dispatch = response_sender
        .dispatch_next_with_data_ack_outstanding(
            &path_stream,
            &mut send_stream,
            TrafficClass::Throughput,
            limits,
            data_ack_outstanding_bytes,
        )
        .expect("frontier repair dispatch");
    assert_eq!(dispatch.selected_path, Some(reinjection_key));
    assert!(matches!(
        try_recv_reliable_path_command(&mut reinjection_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            offset,
            payload,
            ..
        })) if offset == quantum as u64 && payload.len() == quantum
    ));
}

#[test]
fn persistent_tail_reinjection_preserves_original_flight_attribution() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(100);
    let original_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let alternative_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(1),
    };
    let (original_commands, _original_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(100),
        original_key.underlay,
        original_key.path_id,
        original_commands,
        TrafficClass::Throughput,
    );
    let (alternative_commands, _alternative_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            alternative_key.underlay,
            alternative_key.path_id,
            alternative_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let (_frame_tx, frame_rx) = mpsc::channel(1);
    let path_stream = ReliablePathStream {
        stream_id,
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: original_key.underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(limits),
        output: ReliablePathStreamOutput::Switchable(binding.clone()),
        frames: frame_rx.into(),
    };
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
    let reinjection_debt = reliable_relay_buffer_len(limits).saturating_mul(4);
    let mut remaining = reinjection_debt;
    while remaining > 0 {
        let chunk = remaining.min(limits.max_payload_bytes);
        let frame = send_stream
            .send_data(Bytes::from(vec![0x43; chunk]))
            .expect("seed original-transmission path data");
        binding.record_original_flight(original_key, &frame);
        remaining = remaining.saturating_sub(chunk);
    }
    let ack_ranges = [OffsetRange {
        start: 0,
        end: 1024,
    }];
    let _ = send_stream.apply_ack(&ack_ranges);
    assert!(
        send_stream.reinjection_bytes() > reliable_relay_buffer_len(limits),
        "test must cover a retained tail larger than one bounded reinjection event"
    );

    let mut response_sender = ServerResponseSenderService::new_with_performance(
        SessionId(100),
        stream_id,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
    );
    response_sender.record_delivered_data(1024);

    let outcome = enqueue_reliable_tail_reinjection(
        &mut response_sender,
        &path_stream,
        stream_id,
        &send_stream,
        &ack_ranges,
        true,
        None,
        TrafficClass::Throughput,
        limits,
        MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        },
        path_stream.max_frame_payload_bytes,
        1024,
    );

    assert_eq!(
        outcome.queued, 1,
        "a persistent original-transmission stall should reinject the lowest blocked range on an alternate output without changing original-flight attribution"
    );
    assert!(!outcome.pending);
    let first_unacknowledged_byte = Frame::StreamData {
        stream_id,
        offset: 1024,
        payload: Bytes::from_static(&[0]),
    };
    let original_outputs =
        binding.original_flight_outputs_overlapping_frame(&first_unacknowledged_byte);
    assert_eq!(original_outputs.len(), 1);
    assert_eq!(original_outputs[0].0, original_key);
    assert!(
        binding.has_output_incarnation(original_outputs[0].0, original_outputs[0].1),
        "reinjection must not rewrite exact original-output attribution",
    );
}

#[test]
fn final_tail_reinjection_ready_allows_closed_no_ack_frontier_after_deadline() {
    let limits = MuxLimits::default();
    let mut send_stream = ReliableSendStream::new(StreamId(9), limits);
    send_stream
        .send_data(Bytes::from_static(&[7; 4096]))
        .expect("send stream data");
    let now = tokio::time::Instant::now();

    assert!(reliable_final_tail_reinjection_ready(
        true,
        &send_stream,
        &[],
        0,
        now,
        now,
    ));
}

#[test]
fn tcp_multipath_progress_timer_stays_enabled_with_reinjection_alternatives() {
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

#[test]
fn subthreshold_receive_tail_retains_one_existing_ack_deadline() {
    let limits = MuxLimits::default();
    let mut recv_stream = ReliableRecvStream::new(StreamId(25), limits);
    let mut progress = ReliableRecvProgress::default();
    recv_stream
        .receive_data(0, Bytes::from_static(b"first"))
        .expect("first validation input");
    assert!(progress.should_send_ack(&recv_stream, None, TrafficClass::Throughput, limits, true,));
    assert!(!progress.ack_update_pending());

    recv_stream
        .receive_data(5, Bytes::from_static(b"tail"))
        .expect("validation tail");
    assert!(
        !progress.should_send_ack(&recv_stream, None, TrafficClass::Throughput, limits, false,)
    );
    assert!(progress.ack_update_pending());
    assert!(progress.should_send_ack(&recv_stream, None, TrafficClass::Throughput, limits, true,));
    assert!(!progress.ack_update_pending());
}
