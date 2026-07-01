use super::*;

fn test_path_metrics(
    path_id: PathId,
    underlay: UnderlayProtocol,
    srtt_us: u32,
    delivery_rate_bps: u64,
) -> PathMetrics {
    PathMetrics {
        path_id,
        underlay,
        direction: PathMetricDirection::ServerToClient,
        metric_epoch: 1,
        metric_age_us: 0,
        min_rtt_us: srtt_us,
        srtt_us,
        rttvar_us: (srtt_us / 10).max(1),
        jitter_us: (srtt_us / 10).max(1),
        delivery_rate_bps,
        pacing_rate_bps: delivery_rate_bps,
        loss_ppm: 0,
        ecn_ppm: 0,
        bytes_in_flight: 0,
        queue_bytes: 0,
        inflight_limit_bytes: 512 * 1024,
        inflight_hi_bytes: 512 * 1024,
        confidence_ppm: 900_000,
        app_limited: false,
        has_ack_derived_data_sample: true,
        data_sample_count: 8,
    }
}

fn switchable_binding(stream: &TcpPathStream) -> Arc<ServerTcpStreamBinding> {
    let TcpPathStreamOutput::Switchable(binding) = &stream.output else {
        panic!("expected switchable binding");
    };
    binding.clone()
}

#[tokio::test]
async fn server_tcp_binding_active_reattach_carries_ordinary_bulk_data() {
    let (old_tx, mut old_rx) = tcp_path_session_command_channels(4);
    let binding = ServerTcpStreamBinding::new(
        SessionId(1),
        UnderlayProtocol::Tcp,
        PathId(0),
        old_tx,
        FlowLane::Throughput,
    );
    let (new_tx, mut new_rx) = tcp_path_session_command_channels(4);
    binding.attach(
        UnderlayProtocol::Tcp,
        PathId(1),
        new_tx,
        FlowLane::Throughput,
        StreamOpenRole::Active,
        tcp_relay_buffer_len(MuxLimits::default()),
    );
    assert_eq!(binding.lane(), FlowLane::Throughput);

    let large_payload = Bytes::from(vec![7u8; tcp_relay_buffer_len(MuxLimits::default())]);
    let large_len = large_payload.len() as u64;
    binding
        .send_frame(
            StreamId(7),
            FlowLane::Throughput,
            Frame::StreamData {
                stream_id: StreamId(7),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: large_payload,
            },
        )
        .await
        .expect("binding send active bulk");

    assert!(matches!(
        recv_tcp_path_command(&mut new_rx).await,
        Some(TcpPathSessionCommand::SendFrame(Frame::StreamData {
            offset: 0,
            ..
        }))
    ));

    binding
        .send_frame(
            StreamId(7),
            FlowLane::Throughput,
            Frame::StreamData {
                stream_id: StreamId(7),
                offset: large_len,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"bulk"),
            },
        )
        .await
        .expect("binding send active bulk");

    assert!(matches!(
        recv_tcp_path_command(&mut new_rx).await,
        Some(TcpPathSessionCommand::SendFrame(Frame::StreamData {
            offset,
            ..
        })) if offset == large_len
    ));
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_tcp_path_command(&mut old_rx)
        )
        .await
        .is_err(),
        "older TCP peer path must not receive ordinary data after active reattach"
    );
}

#[tokio::test]
async fn server_tcp_binding_active_reattach_promotes_existing_path_for_data() {
    let (path0_initial_tx, _path0_initial_rx) = tcp_path_session_command_channels(1);
    let binding = ServerTcpStreamBinding::new(
        SessionId(1),
        UnderlayProtocol::Tcp,
        PathId(0),
        path0_initial_tx,
        FlowLane::Latency,
    );
    let (path1_tx, mut path1_rx) = tcp_path_session_command_channels(1);
    binding.attach(
        UnderlayProtocol::Tcp,
        PathId(1),
        path1_tx,
        FlowLane::Latency,
        StreamOpenRole::Active,
        tcp_relay_buffer_len(MuxLimits::default()),
    );
    let (path0_repair_tx, mut path0_repair_rx) = tcp_path_session_command_channels(1);
    binding.attach(
        UnderlayProtocol::Tcp,
        PathId(0),
        path0_repair_tx,
        FlowLane::Latency,
        StreamOpenRole::Active,
        tcp_relay_buffer_len(MuxLimits::default()),
    );

    binding
        .send_frame(
            StreamId(7),
            FlowLane::Latency,
            Frame::StreamData {
                stream_id: StreamId(7),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"repair"),
            },
        )
        .await
        .expect("send on promoted repair path");

    match recv_tcp_path_command(&mut path0_repair_rx)
        .await
        .expect("path0 repair command")
    {
        TcpPathSessionCommand::SendFrame(Frame::StreamData { payload, .. }) => {
            assert_eq!(&payload[..], b"repair");
        }
        _ => panic!("expected data on promoted repair path"),
    }
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_tcp_path_command(&mut path1_rx)
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn server_tcp_binding_bulk_repair_reattach_keeps_repair_out_of_ordinary_data() {
    let max_frame_payload_bytes = tcp_relay_buffer_len(MuxLimits::default());
    let validation_credit_bytes = max_frame_payload_bytes.saturating_mul(2);
    let (path0_initial_tx, _path0_initial_rx) = tcp_path_session_command_channels(1);
    let binding = ServerTcpStreamBinding::new(
        SessionId(1),
        UnderlayProtocol::Tcp,
        PathId(0),
        path0_initial_tx,
        FlowLane::Throughput,
    );
    let (path1_tx, mut path1_rx) = tcp_path_session_command_channels(1);
    binding.attach(
        UnderlayProtocol::Tcp,
        PathId(1),
        path1_tx,
        FlowLane::Throughput,
        StreamOpenRole::Active,
        max_frame_payload_bytes,
    );
    let (path0_repair_tx, mut path0_repair_rx) = tcp_path_session_command_channels(1);
    binding.attach(
        UnderlayProtocol::Tcp,
        PathId(0),
        path0_repair_tx,
        FlowLane::Throughput,
        StreamOpenRole::Repair,
        max_frame_payload_bytes,
    );

    let large_payload = Bytes::from(vec![9u8; validation_credit_bytes + 1]);
    let large_len = large_payload.len() as u64;
    binding
        .send_frame(
            StreamId(7),
            FlowLane::Throughput,
            Frame::StreamData {
                stream_id: StreamId(7),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: large_payload,
            },
        )
        .await
        .expect("send active bulk frame");

    assert!(matches!(
        recv_tcp_path_command(&mut path1_rx).await,
        Some(TcpPathSessionCommand::SendFrame(Frame::StreamData {
            offset: 0,
            ..
        }))
    ));
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_tcp_path_command(&mut path0_repair_rx)
        )
        .await
        .is_err()
    );

    binding
        .send_frame(
            StreamId(7),
            FlowLane::Throughput,
            Frame::StreamData {
                stream_id: StreamId(7),
                offset: large_len,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"bulk-repair"),
            },
        )
        .await
        .expect("send active bulk frame");

    assert!(matches!(
        recv_tcp_path_command(&mut path1_rx).await,
        Some(TcpPathSessionCommand::SendFrame(Frame::StreamData {
            offset,
            ..
        })) if offset == large_len
    ));
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_tcp_path_command(&mut path0_repair_rx)
        )
        .await
        .is_err(),
        "TCP repair attachment must not receive ordinary bulk response data"
    );
}

#[tokio::test]
async fn server_udp_binding_bulk_validation_credit_can_lead_bounded_probe_data() {
    let max_frame_payload_bytes = tcp_relay_buffer_len(MuxLimits::default());
    let validation_credit_bytes = max_frame_payload_bytes.saturating_mul(2);
    let (path0_initial_tx, _path0_initial_rx) = tcp_path_session_command_channels(1);
    let binding = ServerTcpStreamBinding::new(
        SessionId(1),
        UnderlayProtocol::Udp,
        PathId(0),
        path0_initial_tx,
        FlowLane::Throughput,
    );
    let (path1_tx, mut path1_rx) = tcp_path_session_command_channels(1);
    binding.attach(
        UnderlayProtocol::Udp,
        PathId(1),
        path1_tx,
        FlowLane::Throughput,
        StreamOpenRole::Active,
        max_frame_payload_bytes,
    );
    let (path0_validation_tx, mut path0_validation_rx) = tcp_path_session_command_channels(1);
    binding.attach(
        UnderlayProtocol::Udp,
        PathId(0),
        path0_validation_tx,
        FlowLane::Throughput,
        StreamOpenRole::Validation,
        max_frame_payload_bytes,
    );
    binding.update_peer_path_metrics_for_test(
        UnderlayProtocol::Udp,
        PathId(1),
        test_path_metrics(PathId(1), UnderlayProtocol::Udp, 220_000, 40_000_000),
    );
    binding.update_peer_path_metrics_for_test(
        UnderlayProtocol::Udp,
        PathId(0),
        test_path_metrics(PathId(0), UnderlayProtocol::Udp, 40_000, 400_000_000),
    );

    binding
        .send_frame(
            StreamId(7),
            FlowLane::Throughput,
            Frame::StreamData {
                stream_id: StreamId(7),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from(vec![9u8; validation_credit_bytes.min(64 * 1024)]),
            },
        )
        .await
        .expect("send UDP validation bulk frame");

    assert!(matches!(
        recv_tcp_path_command(&mut path0_validation_rx).await,
        Some(TcpPathSessionCommand::SendFrame(Frame::StreamData {
            offset: 0,
            ..
        }))
    ));
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_tcp_path_command(&mut path1_rx)
        )
        .await
        .is_err(),
        "bounded UDP validation credit should let the faster validation path lead before the active path creates ordering debt"
    );
}

#[tokio::test]
async fn server_udp_binding_peer_hint_validation_does_not_promote_without_carrier_evidence() {
    let max_frame_payload_bytes = tcp_relay_buffer_len(MuxLimits::default());
    let (active_tx, mut active_rx) = tcp_path_session_command_channels(4);
    let binding = ServerTcpStreamBinding::new(
        SessionId(1),
        UnderlayProtocol::Udp,
        PathId(1),
        active_tx,
        FlowLane::Throughput,
    );
    let (validation_tx, mut validation_rx) = tcp_path_session_command_channels(4);
    binding.attach(
        UnderlayProtocol::Udp,
        PathId(0),
        validation_tx,
        FlowLane::Throughput,
        StreamOpenRole::Validation,
        max_frame_payload_bytes,
    );
    binding.update_peer_path_metrics_for_test(
        UnderlayProtocol::Udp,
        PathId(1),
        test_path_metrics(PathId(1), UnderlayProtocol::Udp, 40_000, 400_000_000),
    );
    binding.update_peer_path_metrics_for_test(
        UnderlayProtocol::Udp,
        PathId(0),
        test_path_metrics(PathId(0), UnderlayProtocol::Udp, 45_000, 350_000_000),
    );

    binding
        .send_frame(
            StreamId(7),
            FlowLane::Throughput,
            Frame::StreamData {
                stream_id: StreamId(7),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"first"),
            },
        )
        .await
        .expect("send duplicate validation frame");
    assert!(matches!(
        recv_tcp_path_command(&mut active_rx).await,
        Some(TcpPathSessionCommand::SendFrame(Frame::StreamData {
            offset: 0,
            ..
        }))
    ));
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_tcp_path_command(&mut validation_rx)
        )
        .await
        .is_err(),
        "a slower UDP validation peer hint should not receive duplicate ordered stream data"
    );
    binding.release_acked_ranges(&[OffsetRange { start: 0, end: 5 }]);

    binding
        .send_frame(
            StreamId(7),
            FlowLane::Throughput,
            Frame::StreamData {
                stream_id: StreamId(7),
                offset: 5,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"second"),
            },
        )
        .await
        .expect("send after duplicate ack");

    assert!(matches!(
        recv_tcp_path_command(&mut active_rx).await,
        Some(TcpPathSessionCommand::SendFrame(Frame::StreamData {
            offset: 5,
            ..
        }))
    ));
}

#[tokio::test]
async fn server_udp_binding_local_carrier_metrics_promote_validation_path() {
    let max_frame_payload_bytes = tcp_relay_buffer_len(MuxLimits::default());
    let (active_tx, mut active_rx) = tcp_path_session_command_channels(4);
    let binding = ServerTcpStreamBinding::new(
        SessionId(1),
        UnderlayProtocol::Udp,
        PathId(1),
        active_tx,
        FlowLane::Throughput,
    );
    let (validation_tx, mut validation_rx) = tcp_path_session_command_channels(4);
    binding.attach(
        UnderlayProtocol::Udp,
        PathId(0),
        validation_tx,
        FlowLane::Throughput,
        StreamOpenRole::Validation,
        max_frame_payload_bytes,
    );
    let active_metrics = test_path_metrics(PathId(1), UnderlayProtocol::Udp, 40_000, 400_000_000);
    let peer_hint_metrics =
        test_path_metrics(PathId(0), UnderlayProtocol::Udp, 45_000, 350_000_000);
    let fast_metrics = test_path_metrics(PathId(0), UnderlayProtocol::Udp, 35_000, 500_000_000);
    binding.update_peer_path_metrics_for_test(UnderlayProtocol::Udp, PathId(1), active_metrics);
    binding.update_peer_path_metrics_for_test(UnderlayProtocol::Udp, PathId(0), peer_hint_metrics);

    binding
        .send_frame(
            StreamId(7),
            FlowLane::Throughput,
            Frame::StreamData {
                stream_id: StreamId(7),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"first"),
            },
        )
        .await
        .expect("send duplicate validation frame");
    assert!(matches!(
        recv_tcp_path_command(&mut active_rx).await,
        Some(TcpPathSessionCommand::SendFrame(Frame::StreamData {
            offset: 0,
            ..
        }))
    ));
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_tcp_path_command(&mut validation_rx)
        )
        .await
        .is_err(),
        "peer-hint validation must wait for local carrier evidence when admission rejects duplicate data"
    );

    binding.update_path_metrics_for_test(UnderlayProtocol::Udp, PathId(0), fast_metrics);
    binding.release_acked_ranges(&[OffsetRange { start: 0, end: 5 }]);
    binding
        .send_frame(
            StreamId(7),
            FlowLane::Throughput,
            Frame::StreamData {
                stream_id: StreamId(7),
                offset: 5,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"second"),
            },
        )
        .await
        .expect("send after local carrier evidence");

    assert!(matches!(
        recv_tcp_path_command(&mut validation_rx).await,
        Some(TcpPathSessionCommand::SendFrame(Frame::StreamData {
            offset: 5,
            ..
        }))
    ));
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_tcp_path_command(&mut active_rx)
        )
        .await
        .is_err(),
        "local UDP carrier evidence should promote the validation path into ordinary selection"
    );
}

#[tokio::test]
async fn server_udp_binding_ignores_product_ack_rate_without_local_carrier_metrics() {
    let (active_tx, mut active_rx) = tcp_path_session_command_channels(4);
    let binding = ServerTcpStreamBinding::new(
        SessionId(1),
        UnderlayProtocol::Udp,
        PathId(0),
        active_tx,
        FlowLane::Throughput,
    );
    let payload_len = 64 * 1024;
    binding
        .send_frame(
            StreamId(7),
            FlowLane::Throughput,
            Frame::StreamData {
                stream_id: StreamId(7),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from(vec![7u8; payload_len]),
            },
        )
        .await
        .expect("send active UDP frame");
    assert!(matches!(
        recv_tcp_path_command(&mut active_rx).await,
        Some(TcpPathSessionCommand::SendFrame(Frame::StreamData {
            offset: 0,
            ..
        }))
    ));

    tokio::time::sleep(Duration::from_millis(40)).await;
    binding.release_acked_ranges(&[OffsetRange {
        start: 0,
        end: payload_len as u64,
    }]);
    let snapshot = binding
        .output_snapshot_for_test(UnderlayProtocol::Udp, PathId(0))
        .expect("UDP output snapshot");
    let default_rate = default_path_rate_bps(UnderlayProtocol::Udp);
    assert!(
        (snapshot.delivery_rate_bps - default_rate).abs() < 1.0,
        "UDP product ACK timing must not become carrier throughput evidence: got {:.3}, default {:.3}",
        snapshot.delivery_rate_bps,
        default_rate
    );

    binding.update_path_metrics_for_test(
        UnderlayProtocol::Udp,
        PathId(0),
        test_path_metrics(PathId(0), UnderlayProtocol::Udp, 40_000, 300_000_000),
    );
    let snapshot = binding
        .output_snapshot_for_test(UnderlayProtocol::Udp, PathId(0))
        .expect("UDP output snapshot after local metrics");
    assert_eq!(snapshot.delivery_rate_bps.round() as u64, 300_000_000);
}

#[tokio::test]
async fn server_udp_binding_uses_quic_pacing_when_local_rate_sample_is_app_limited() {
    let (active_tx, _active_rx) = tcp_path_session_command_channels(4);
    let binding = ServerTcpStreamBinding::new(
        SessionId(1),
        UnderlayProtocol::Udp,
        PathId(0),
        active_tx,
        FlowLane::Throughput,
    );
    let mut app_limited_metrics =
        test_path_metrics(PathId(0), UnderlayProtocol::Udp, 80_000, 7_000);
    app_limited_metrics.pacing_rate_bps = 300_000_000;
    app_limited_metrics.app_limited = true;

    binding.update_path_metrics_for_test(UnderlayProtocol::Udp, PathId(0), app_limited_metrics);
    let snapshot = binding
        .output_snapshot_for_test(UnderlayProtocol::Udp, PathId(0))
        .expect("UDP output snapshot");

    assert_eq!(snapshot.delivery_rate_bps.round() as u64, 300_000_000);
    assert_eq!(snapshot.pacing_rate_bps.round() as u64, 300_000_000);
    assert!(
        snapshot.app_limited,
        "the scheduler should preserve provenance while refusing to learn a false low bulk rate"
    );
}

#[tokio::test]
async fn server_tcp_binding_ignores_product_ack_rate_without_path_metrics() {
    let (active_tx, mut active_rx) = tcp_path_session_command_channels(4);
    let binding = ServerTcpStreamBinding::new(
        SessionId(1),
        UnderlayProtocol::Tcp,
        PathId(0),
        active_tx,
        FlowLane::Throughput,
    );
    let payload_len = 64 * 1024;
    binding
        .send_frame(
            StreamId(7),
            FlowLane::Throughput,
            Frame::StreamData {
                stream_id: StreamId(7),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from(vec![7u8; payload_len]),
            },
        )
        .await
        .expect("send active TCP frame");
    assert!(matches!(
        recv_tcp_path_command(&mut active_rx).await,
        Some(TcpPathSessionCommand::SendFrame(Frame::StreamData {
            offset: 0,
            ..
        }))
    ));

    binding.release_acked_ranges(&[OffsetRange {
        start: 0,
        end: payload_len as u64,
    }]);
    assert!(
        binding.output_has_sender_evidence_for_test(UnderlayProtocol::Tcp, PathId(0)),
        "a stream ACK can prove liveness and release flight state"
    );
    let snapshot = binding
        .output_snapshot_for_test(UnderlayProtocol::Tcp, PathId(0))
        .expect("TCP output snapshot");
    let default_rate = default_path_rate_bps(UnderlayProtocol::Tcp);
    assert!(
        (snapshot.delivery_rate_bps - default_rate).abs() < 1.0,
        "TCP product ACK timing must not become throughput evidence: got {:.3}, default {:.3}",
        snapshot.delivery_rate_bps,
        default_rate
    );

    binding.update_peer_path_metrics_for_test(
        UnderlayProtocol::Tcp,
        PathId(0),
        test_path_metrics(PathId(0), UnderlayProtocol::Tcp, 40_000, 250_000_000),
    );
    let snapshot = binding
        .output_snapshot_for_test(UnderlayProtocol::Tcp, PathId(0))
        .expect("TCP output snapshot after peer path metrics");
    assert_eq!(
        snapshot.delivery_rate_bps.round() as u64,
        default_rate.round() as u64,
        "peer PATH_METRICS must remain a validation hint after sender evidence exists"
    );

    binding.update_path_metrics_for_test(
        UnderlayProtocol::Tcp,
        PathId(0),
        test_path_metrics(PathId(0), UnderlayProtocol::Tcp, 40_000, 250_000_000),
    );
    let snapshot = binding
        .output_snapshot_for_test(UnderlayProtocol::Tcp, PathId(0))
        .expect("TCP output snapshot after local path metrics");
    assert_eq!(snapshot.delivery_rate_bps.round() as u64, 250_000_000);
}

#[tokio::test]
async fn server_mixed_binding_udp_validation_can_be_primary_before_tcp_debt() {
    let max_frame_payload_bytes = tcp_relay_buffer_len(MuxLimits::default());
    let (tcp_tx, mut tcp_rx) = tcp_path_session_command_channels(4);
    let binding = ServerTcpStreamBinding::new(
        SessionId(1),
        UnderlayProtocol::Tcp,
        PathId(1),
        tcp_tx,
        FlowLane::Throughput,
    );
    let (udp_tx, mut udp_rx) = tcp_path_session_command_channels(4);
    binding.attach(
        UnderlayProtocol::Udp,
        PathId(0),
        udp_tx,
        FlowLane::Throughput,
        StreamOpenRole::Validation,
        max_frame_payload_bytes,
    );
    binding.update_peer_path_metrics_for_test(
        UnderlayProtocol::Tcp,
        PathId(1),
        test_path_metrics(PathId(1), UnderlayProtocol::Tcp, 650_000, 100_000_000),
    );
    binding.update_peer_path_metrics_for_test(
        UnderlayProtocol::Udp,
        PathId(0),
        test_path_metrics(PathId(0), UnderlayProtocol::Udp, 170_000, 100_000_000),
    );

    assert_eq!(
        binding.bulk_choice_key_for_test(64 * 1024),
        Some(ServerTcpPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        }),
        "bounded UDP validation credit must compete for the lead before active TCP owns the lower frontier"
    );

    binding
        .send_frame(
            StreamId(7),
            FlowLane::Throughput,
            Frame::StreamData {
                stream_id: StreamId(7),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from(vec![3u8; 64 * 1024]),
            },
        )
        .await
        .expect("send first mixed bulk frame");

    assert!(matches!(
        recv_tcp_path_command(&mut udp_rx).await,
        Some(TcpPathSessionCommand::SendFrame(Frame::StreamData {
            offset: 0,
            ..
        }))
    ));
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_tcp_path_command(&mut tcp_rx)
        )
        .await
        .is_err(),
        "active TCP must not receive the first bulk frame when a bounded UDP validation lead is faster"
    );
}

#[tokio::test]
async fn server_mixed_binding_unproven_frontier_owner_blocks_cross_underlay_lead_jump() {
    let max_frame_payload_bytes = tcp_relay_buffer_len(MuxLimits::default());
    let (tcp_tx, mut tcp_rx) = tcp_path_session_command_channels(4);
    let binding = ServerTcpStreamBinding::new(
        SessionId(1),
        UnderlayProtocol::Tcp,
        PathId(1),
        tcp_tx,
        FlowLane::Throughput,
    );
    let (udp_tx, mut udp_rx) = tcp_path_session_command_channels(4);
    binding.attach(
        UnderlayProtocol::Udp,
        PathId(0),
        udp_tx,
        FlowLane::Throughput,
        StreamOpenRole::Validation,
        max_frame_payload_bytes,
    );
    binding.update_peer_path_metrics_for_test(
        UnderlayProtocol::Tcp,
        PathId(1),
        test_path_metrics(PathId(1), UnderlayProtocol::Tcp, 650_000, 100_000_000),
    );
    binding.update_peer_path_metrics_for_test(
        UnderlayProtocol::Udp,
        PathId(0),
        test_path_metrics(PathId(0), UnderlayProtocol::Udp, 170_000, 100_000_000),
    );

    binding
        .send_frame(
            StreamId(7),
            FlowLane::Throughput,
            Frame::StreamData {
                stream_id: StreamId(7),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from(vec![3u8; 64 * 1024]),
            },
        )
        .await
        .expect("send first mixed bulk frame");
    assert!(matches!(
        recv_tcp_path_command(&mut udp_rx).await,
        Some(TcpPathSessionCommand::SendFrame(Frame::StreamData {
            offset: 0,
            ..
        }))
    ));
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_tcp_path_command(&mut tcp_rx)
        )
        .await
        .is_err()
    );

    binding.update_path_metrics_for_test(
        UnderlayProtocol::Tcp,
        PathId(1),
        test_path_metrics(PathId(1), UnderlayProtocol::Tcp, 30_000, 500_000_000),
    );

    assert!(
        !binding.can_send_stream_data_extent(FlowLane::Throughput, 64 * 1024, 64 * 1024),
        "unproven validation that owns lower bytes must wait instead of growing a cross-underlay hole"
    );
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_tcp_path_command(&mut udp_rx)
        )
        .await
        .is_err()
    );
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_tcp_path_command(&mut tcp_rx)
        )
        .await
        .is_err(),
        "a cross-underlay path must not become the response lead while lower offsets are still outstanding elsewhere"
    );
}

#[tokio::test]
async fn server_mixed_binding_proven_path_waits_for_lower_frontier_owner() {
    let max_frame_payload_bytes = tcp_relay_buffer_len(MuxLimits::default());
    let (tcp_tx, mut tcp_rx) = tcp_path_session_command_channels(4);
    let binding = ServerTcpStreamBinding::new(
        SessionId(1),
        UnderlayProtocol::Tcp,
        PathId(0),
        tcp_tx,
        FlowLane::Throughput,
    );
    let (udp_tx, mut udp_rx) = tcp_path_session_command_channels(4);
    binding.attach(
        UnderlayProtocol::Udp,
        PathId(1),
        udp_tx,
        FlowLane::Throughput,
        StreamOpenRole::Active,
        max_frame_payload_bytes,
    );
    binding.update_path_metrics_for_test(
        UnderlayProtocol::Tcp,
        PathId(0),
        test_path_metrics(PathId(0), UnderlayProtocol::Tcp, 40_000, 400_000_000),
    );
    binding.update_path_metrics_for_test(
        UnderlayProtocol::Udp,
        PathId(1),
        test_path_metrics(PathId(1), UnderlayProtocol::Udp, 220_000, 40_000_000),
    );

    binding
        .send_frame(
            StreamId(7),
            FlowLane::Throughput,
            Frame::StreamData {
                stream_id: StreamId(7),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from(vec![3u8; 64 * 1024]),
            },
        )
        .await
        .expect("send first mixed bulk frame");
    assert!(matches!(
        recv_tcp_path_command(&mut tcp_rx).await,
        Some(TcpPathSessionCommand::SendFrame(Frame::StreamData {
            offset: 0,
            ..
        }))
    ));

    binding.update_path_metrics_for_test(
        UnderlayProtocol::Udp,
        PathId(1),
        test_path_metrics(PathId(1), UnderlayProtocol::Udp, 30_000, 500_000_000),
    );
    binding
        .send_frame(
            StreamId(7),
            FlowLane::Throughput,
            Frame::StreamData {
                stream_id: StreamId(7),
                offset: 64 * 1024,
                flags: StreamFlags::NONE,
                payload: Bytes::from(vec![4u8; 64 * 1024]),
            },
        )
        .await
        .expect("send second mixed bulk frame");

    assert!(matches!(
        recv_tcp_path_command(&mut tcp_rx).await,
        Some(TcpPathSessionCommand::SendFrame(Frame::StreamData {
            offset,
            ..
        })) if offset == 64 * 1024
    ));
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_tcp_path_command(&mut udp_rx)
        )
        .await
        .is_err(),
        "a faster proven path must wait until the lower frontier owner is ACKed"
    );
}

#[tokio::test]
async fn server_mixed_binding_unproven_lower_frontier_waits_when_proven_path_exists() {
    let max_frame_payload_bytes = tcp_relay_buffer_len(MuxLimits::default());
    let probe_payload_bytes = (max_frame_payload_bytes / 4).max(1024);
    let (tcp_tx, mut tcp_rx) = tcp_path_session_command_channels(4);
    let binding = ServerTcpStreamBinding::new(
        SessionId(1),
        UnderlayProtocol::Tcp,
        PathId(1),
        tcp_tx,
        FlowLane::Throughput,
    );
    let (udp_tx, mut udp_rx) = tcp_path_session_command_channels(4);
    binding.attach(
        UnderlayProtocol::Udp,
        PathId(0),
        udp_tx,
        FlowLane::Throughput,
        StreamOpenRole::Validation,
        max_frame_payload_bytes,
    );
    binding.update_peer_path_metrics_for_test(
        UnderlayProtocol::Tcp,
        PathId(1),
        test_path_metrics(PathId(1), UnderlayProtocol::Tcp, 650_000, 100_000_000),
    );
    binding.update_peer_path_metrics_for_test(
        UnderlayProtocol::Udp,
        PathId(0),
        test_path_metrics(PathId(0), UnderlayProtocol::Udp, 170_000, 100_000_000),
    );

    binding
        .send_frame(
            StreamId(7),
            FlowLane::Throughput,
            Frame::StreamData {
                stream_id: StreamId(7),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from(vec![3u8; probe_payload_bytes]),
            },
        )
        .await
        .expect("send first mixed bulk frame");
    assert!(matches!(
        recv_tcp_path_command(&mut udp_rx).await,
        Some(TcpPathSessionCommand::SendFrame(Frame::StreamData {
            offset,
            ..
        })) if offset == 0
    ));

    binding.update_path_metrics_for_test(
        UnderlayProtocol::Tcp,
        PathId(1),
        test_path_metrics(PathId(1), UnderlayProtocol::Tcp, 30_000, 500_000_000),
    );

    assert!(
        !binding.can_send_stream_data_extent(
            FlowLane::Throughput,
            probe_payload_bytes as u64,
            probe_payload_bytes,
        ),
        "unproven validation must not grow a lower-frontier hole once a proven path exists"
    );
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_tcp_path_command(&mut udp_rx)
        )
        .await
        .is_err(),
        "unproven lower-frontier validation must wait for ACK or local sender evidence"
    );
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_tcp_path_command(&mut tcp_rx)
        )
        .await
        .is_err(),
        "the lower-frontier UDP owner may continue only while it still has bounded validation credit"
    );
}

#[tokio::test]
async fn server_mixed_binding_pre_read_gate_blocks_when_lower_owner_cannot_continue() {
    let max_frame_payload_bytes = tcp_relay_buffer_len(MuxLimits::default());
    let validation_credit_bytes = max_frame_payload_bytes.saturating_mul(2);
    let (tcp_tx, mut tcp_rx) = tcp_path_session_command_channels(4);
    let binding = ServerTcpStreamBinding::new(
        SessionId(1),
        UnderlayProtocol::Tcp,
        PathId(1),
        tcp_tx,
        FlowLane::Throughput,
    );
    let (udp_tx, mut udp_rx) = tcp_path_session_command_channels(4);
    binding.attach(
        UnderlayProtocol::Udp,
        PathId(0),
        udp_tx,
        FlowLane::Throughput,
        StreamOpenRole::Validation,
        max_frame_payload_bytes,
    );
    binding.update_peer_path_metrics_for_test(
        UnderlayProtocol::Tcp,
        PathId(1),
        test_path_metrics(PathId(1), UnderlayProtocol::Tcp, 650_000, 100_000_000),
    );
    binding.update_peer_path_metrics_for_test(
        UnderlayProtocol::Udp,
        PathId(0),
        test_path_metrics(PathId(0), UnderlayProtocol::Udp, 170_000, 100_000_000),
    );

    assert!(binding.can_send_stream_data_extent(FlowLane::Throughput, 0, validation_credit_bytes));
    binding
        .send_frame(
            StreamId(7),
            FlowLane::Throughput,
            Frame::StreamData {
                stream_id: StreamId(7),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from(vec![3u8; validation_credit_bytes]),
            },
        )
        .await
        .expect("send full validation-credit frame");
    assert!(matches!(
        recv_tcp_path_command(&mut udp_rx).await,
        Some(TcpPathSessionCommand::SendFrame(Frame::StreamData {
            offset: 0,
            ..
        }))
    ));

    binding.update_path_metrics_for_test(
        UnderlayProtocol::Tcp,
        PathId(1),
        test_path_metrics(PathId(1), UnderlayProtocol::Tcp, 30_000, 500_000_000),
    );
    assert!(
        !binding.can_send_stream_data_extent(
            FlowLane::Throughput,
            validation_credit_bytes as u64,
            64 * 1024,
        ),
        "the server must pause reads instead of creating later offsets on TCP while lower bytes are outstanding on an unproven UDP validation path"
    );
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_tcp_path_command(&mut tcp_rx)
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn server_mixed_binding_read_backpressure_uses_best_safe_lead_snapshot() {
    let max_frame_payload_bytes = tcp_relay_buffer_len(MuxLimits::default());
    let (tcp_tx, _tcp_rx) = tcp_path_session_command_channels(4);
    let binding = ServerTcpStreamBinding::new(
        SessionId(1),
        UnderlayProtocol::Tcp,
        PathId(1),
        tcp_tx,
        FlowLane::Throughput,
    );
    let (udp_tx, _udp_rx) = tcp_path_session_command_channels(4);
    binding.attach(
        UnderlayProtocol::Udp,
        PathId(0),
        udp_tx,
        FlowLane::Throughput,
        StreamOpenRole::Validation,
        max_frame_payload_bytes,
    );
    binding.update_peer_path_metrics_for_test(
        UnderlayProtocol::Tcp,
        PathId(1),
        test_path_metrics(PathId(1), UnderlayProtocol::Tcp, 650_000, 100_000_000),
    );
    binding.update_peer_path_metrics_for_test(
        UnderlayProtocol::Udp,
        PathId(0),
        test_path_metrics(PathId(0), UnderlayProtocol::Udp, 170_000, 100_000_000),
    );

    let snapshot = binding
        .send_path_snapshot(FlowLane::Throughput, 64 * 1024)
        .expect("read backpressure snapshot");

    assert_eq!(snapshot.underlay, UnderlayProtocol::Tcp);
    assert_eq!(snapshot.id, PathId(1));
}

#[tokio::test]
async fn server_mixed_binding_unproven_tcp_validation_cannot_jump_udp_frontier() {
    let max_frame_payload_bytes = tcp_relay_buffer_len(MuxLimits::default());
    let (udp_tx, mut udp_rx) = tcp_path_session_command_channels(4);
    let binding = ServerTcpStreamBinding::new(
        SessionId(1),
        UnderlayProtocol::Udp,
        PathId(1),
        udp_tx,
        FlowLane::Throughput,
    );
    let (tcp_validation_tx, mut tcp_validation_rx) = tcp_path_session_command_channels(4);
    binding.attach(
        UnderlayProtocol::Tcp,
        PathId(0),
        tcp_validation_tx,
        FlowLane::Throughput,
        StreamOpenRole::Validation,
        max_frame_payload_bytes,
    );
    binding.update_path_metrics_for_test(
        UnderlayProtocol::Udp,
        PathId(1),
        test_path_metrics(PathId(1), UnderlayProtocol::Udp, 170_000, 100_000_000),
    );
    binding.update_peer_path_metrics_for_test(
        UnderlayProtocol::Tcp,
        PathId(0),
        test_path_metrics(PathId(0), UnderlayProtocol::Tcp, 650_000, 50_000_000),
    );

    binding
        .send_frame(
            StreamId(7),
            FlowLane::Throughput,
            Frame::StreamData {
                stream_id: StreamId(7),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from(vec![3u8; 64 * 1024]),
            },
        )
        .await
        .expect("send first mixed bulk frame");
    assert!(matches!(
        recv_tcp_path_command(&mut udp_rx).await,
        Some(TcpPathSessionCommand::SendFrame(Frame::StreamData {
            offset: 0,
            ..
        }))
    ));

    binding.update_peer_path_metrics_for_test(
        UnderlayProtocol::Tcp,
        PathId(0),
        test_path_metrics(PathId(0), UnderlayProtocol::Tcp, 30_000, 500_000_000),
    );

    binding
        .send_frame(
            StreamId(7),
            FlowLane::Throughput,
            Frame::StreamData {
                stream_id: StreamId(7),
                offset: 64 * 1024,
                flags: StreamFlags::NONE,
                payload: Bytes::from(vec![4u8; 64 * 1024]),
            },
        )
        .await
        .expect("send second mixed bulk frame");
    assert!(matches!(
        recv_tcp_path_command(&mut udp_rx).await,
        Some(TcpPathSessionCommand::SendFrame(Frame::StreamData {
            offset,
            ..
        })) if offset == 64 * 1024
    ));
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_tcp_path_command(&mut tcp_validation_rx)
        )
        .await
        .is_err(),
        "unproven TCP validation must not carry later response bytes while UDP owns the lower frontier"
    );
}

#[tokio::test]
async fn server_binding_allows_bounded_tcp_validation_without_duplicate_data() {
    let registry = ServerTcpStreamRegistry::default();
    let session_id = SessionId(71);
    let stream_id = StreamId(7);
    let target = TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80)));
    let (active_tx, mut active_rx) = tcp_path_session_command_channels(4);
    let stream = match registry
        .open_or_attach(
            ServerTcpStreamOpenRequest {
                session_id,
                stream_id,
                target: &target,
                lane: FlowLane::Throughput,
                attachment: ServerTcpPathAttachment {
                    path_id: PathId(0),
                    underlay: UnderlayProtocol::Tcp,
                    commands: active_tx,
                    max_frame_payload_bytes: tcp_relay_buffer_len(MuxLimits::default()),
                    role: StreamOpenRole::Active,
                },
            },
            MuxLimits::default(),
            ResourceLimits::default().max_streams,
        )
        .expect("open active stream")
    {
        ServerTcpStreamOpen::New(stream) => stream,
        ServerTcpStreamOpen::Existing => panic!("expected new stream"),
    };
    registry.record_path_metrics(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(0),
        test_path_metrics(PathId(0), UnderlayProtocol::Tcp, 220_000, 40_000_000),
    );
    registry.record_path_metrics(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(1),
        test_path_metrics(PathId(1), UnderlayProtocol::Tcp, 40_000, 400_000_000),
    );
    let (validation_tx, mut validation_rx) = tcp_path_session_command_channels(4);
    assert!(matches!(
        registry
            .open_or_attach(
                ServerTcpStreamOpenRequest {
                    session_id,
                    stream_id,
                    target: &target,
                    lane: FlowLane::Throughput,
                    attachment: ServerTcpPathAttachment {
                        path_id: PathId(1),
                        underlay: UnderlayProtocol::Tcp,
                        commands: validation_tx,
                        max_frame_payload_bytes: tcp_relay_buffer_len(MuxLimits::default()),
                        role: StreamOpenRole::Validation,
                    },
                },
                MuxLimits::default(),
                ResourceLimits::default().max_streams,
            )
            .expect("attach validation path"),
        ServerTcpStreamOpen::Existing
    ));

    stream
        .send_frame(Frame::StreamData {
            stream_id,
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![1u8; 64 * 1024]),
        })
        .await
        .expect("send response bulk");

    assert!(matches!(
        recv_tcp_path_command(&mut validation_rx).await,
        Some(TcpPathSessionCommand::SendFrame(Frame::StreamData {
            offset: 0,
            ..
        }))
    ));
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_tcp_path_command(&mut active_rx)
        )
        .await
        .is_err(),
        "TCP validation credit may carry bounded primary probe data, but peer hints must not create duplicate ordered stream data"
    );
}

#[tokio::test]
async fn server_tcp_binding_counts_command_queue_debt_for_bulk_admission() {
    let (active_tx, _active_rx) = tcp_path_session_command_channels(128);
    let binding = ServerTcpStreamBinding::new(
        SessionId(1),
        UnderlayProtocol::Tcp,
        PathId(0),
        active_tx.clone(),
        FlowLane::Throughput,
    );
    let (validation_tx, mut validation_rx) = tcp_path_session_command_channels(128);
    binding.attach(
        UnderlayProtocol::Tcp,
        PathId(1),
        validation_tx,
        FlowLane::Throughput,
        StreamOpenRole::Validation,
        tcp_relay_buffer_len(MuxLimits::default()),
    );
    binding.update_path_metrics_for_test(
        UnderlayProtocol::Tcp,
        PathId(0),
        test_path_metrics(PathId(0), UnderlayProtocol::Tcp, 40_000, 80_000_000),
    );
    binding.update_path_metrics_for_test(
        UnderlayProtocol::Tcp,
        PathId(1),
        test_path_metrics(PathId(1), UnderlayProtocol::Tcp, 80_000, 500_000_000),
    );

    const SEEDED_ACTIVE_FRAMES: u64 = 96;
    for offset in 0..SEEDED_ACTIVE_FRAMES {
        active_tx
            .send_frame(
                Frame::StreamData {
                    stream_id: StreamId(7),
                    offset: offset * 64 * 1024,
                    flags: StreamFlags::NONE,
                    payload: Bytes::from(vec![0u8; 64 * 1024]),
                },
                FlowLane::Throughput,
            )
            .await
            .expect("seed active path queue debt");
    }
    let active_snapshot = binding
        .output_snapshot_for_test(UnderlayProtocol::Tcp, PathId(0))
        .expect("active snapshot");
    let validation_snapshot = binding
        .output_snapshot_for_test(UnderlayProtocol::Tcp, PathId(1))
        .expect("validation snapshot");
    assert!(
        active_snapshot.queue_bytes >= SEEDED_ACTIVE_FRAMES * 64 * 1024,
        "active TCP command queue debt must feed server-side admission"
    );
    assert!(
        active_snapshot.queue_bytes > validation_snapshot.queue_bytes,
        "validation path should not inherit active path command debt"
    );
    assert!(
        binding.output_has_sender_evidence_for_test(UnderlayProtocol::Tcp, PathId(1)),
        "local ACK-derived metrics should be sender evidence for the validation path"
    );
    assert!(
        active_snapshot.queue_bytes > active_snapshot.inflight_limit_bytes,
        "seeded active queue debt should exceed the active path's modeled inflight gate"
    );
    let active_eta = binding
        .output_eta_ms_for_test(UnderlayProtocol::Tcp, PathId(0), 64 * 1024)
        .expect("active ETA");
    let validation_eta = binding
        .output_eta_ms_for_test(UnderlayProtocol::Tcp, PathId(1), 64 * 1024)
        .expect("validation ETA");
    assert!(
        validation_eta < active_eta,
        "queue plus service-horizon scoring should make the faster proven path lower ETA: active={active_eta:.3} validation={validation_eta:.3}"
    );
    assert_eq!(
        binding.bulk_choice_key_for_test(64 * 1024),
        Some(ServerTcpPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        }),
        "server bulk admission should choose the locally proven path when active TCP queue debt dominates"
    );

    binding
        .send_frame(
            StreamId(7),
            FlowLane::Throughput,
            Frame::StreamData {
                stream_id: StreamId(7),
                offset: SEEDED_ACTIVE_FRAMES * 64 * 1024,
                flags: StreamFlags::NONE,
                payload: Bytes::from(vec![1u8; 64 * 1024]),
            },
        )
        .await
        .expect("send response bulk with hidden TCP queue debt");

    let validation_frame = tokio::time::timeout(
        Duration::from_millis(20),
        recv_tcp_path_command(&mut validation_rx),
    )
    .await
    .ok()
    .flatten();
    assert!(matches!(
        validation_frame,
        Some(TcpPathSessionCommand::SendFrame(Frame::StreamData {
            offset,
            ..
        })) if offset == SEEDED_ACTIVE_FRAMES * 64 * 1024
    ));
}

#[tokio::test]
async fn server_bulk_binding_uses_service_horizon_for_fat_path() {
    let (lowlat_tx, _lowlat_rx) = tcp_path_session_command_channels(128);
    let binding = ServerTcpStreamBinding::new(
        SessionId(1),
        UnderlayProtocol::Tcp,
        PathId(0),
        lowlat_tx,
        FlowLane::Throughput,
    );
    let (fat_tx, mut fat_rx) = tcp_path_session_command_channels(128);
    binding.attach(
        UnderlayProtocol::Tcp,
        PathId(2),
        fat_tx,
        FlowLane::Throughput,
        StreamOpenRole::Validation,
        tcp_relay_buffer_len(MuxLimits::default()),
    );
    binding.update_path_metrics_for_test(
        UnderlayProtocol::Tcp,
        PathId(0),
        test_path_metrics(PathId(0), UnderlayProtocol::Tcp, 20_000, 80_000_000),
    );
    binding.update_path_metrics_for_test(
        UnderlayProtocol::Tcp,
        PathId(2),
        test_path_metrics(PathId(2), UnderlayProtocol::Tcp, 180_000, 500_000_000),
    );

    assert_eq!(
        binding.bulk_choice_key_for_test(64 * 1024),
        Some(ServerTcpPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(2),
        }),
        "response bulk lead selection must prefer the sustained high-bandwidth path"
    );

    binding
        .send_frame(
            StreamId(7),
            FlowLane::Throughput,
            Frame::StreamData {
                stream_id: StreamId(7),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from(vec![1u8; 64 * 1024]),
            },
        )
        .await
        .expect("send response bulk on service-horizon lead");

    assert!(matches!(
        recv_tcp_path_command(&mut fat_rx).await,
        Some(TcpPathSessionCommand::SendFrame(Frame::StreamData {
            offset: 0,
            ..
        }))
    ));
}

#[tokio::test]
async fn server_registry_bulk_snapshot_includes_cross_stream_latency_load() {
    let registry = ServerTcpStreamRegistry::default();
    let session_id = SessionId(81);
    let target = TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80)));
    let (latency_tx, _latency_rx) = tcp_path_session_command_channels(4);
    let latency_stream = match registry
        .open_or_attach(
            ServerTcpStreamOpenRequest {
                session_id,
                stream_id: StreamId(1),
                target: &target,
                lane: FlowLane::Latency,
                attachment: ServerTcpPathAttachment {
                    path_id: PathId(0),
                    underlay: UnderlayProtocol::Tcp,
                    commands: latency_tx,
                    max_frame_payload_bytes: tcp_relay_buffer_len(MuxLimits::default()),
                    role: StreamOpenRole::Active,
                },
            },
            MuxLimits::default(),
            ResourceLimits::default().max_streams,
        )
        .expect("open latency stream")
    {
        ServerTcpStreamOpen::New(stream) => stream,
        ServerTcpStreamOpen::Existing => panic!("expected latency stream to be new"),
    };

    let (bulk_tx, _bulk_rx) = tcp_path_session_command_channels(4);
    let bulk_stream = match registry
        .open_or_attach(
            ServerTcpStreamOpenRequest {
                session_id,
                stream_id: StreamId(2),
                target: &target,
                lane: FlowLane::Throughput,
                attachment: ServerTcpPathAttachment {
                    path_id: PathId(0),
                    underlay: UnderlayProtocol::Tcp,
                    commands: bulk_tx,
                    max_frame_payload_bytes: tcp_relay_buffer_len(MuxLimits::default()),
                    role: StreamOpenRole::Active,
                },
            },
            MuxLimits::default(),
            ResourceLimits::default().max_streams,
        )
        .expect("open bulk stream")
    {
        ServerTcpStreamOpen::New(stream) => stream,
        ServerTcpStreamOpen::Existing => panic!("expected bulk stream to be new"),
    };
    registry.record_local_path_metrics(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(0),
        test_path_metrics(PathId(0), UnderlayProtocol::Tcp, 40_000, 400_000_000),
    );

    let binding = switchable_binding(&bulk_stream);
    let snapshot = binding
        .send_path_snapshot(FlowLane::Throughput, 64 * 1024)
        .expect("bulk snapshot");
    assert_eq!(snapshot.active_latency_sensitive_flows, 1);
    assert!(
        snapshot.queue_bytes > 0,
        "bulk admission must see adaptive queue debt reserved for active latency streams"
    );

    let latency_binding = switchable_binding(&latency_stream);
    latency_binding.set_lane(FlowLane::Throughput);
    let after_promotion = binding
        .send_path_snapshot(FlowLane::Throughput, 64 * 1024)
        .expect("bulk snapshot after latency promotion");
    assert_eq!(after_promotion.active_latency_sensitive_flows, 0);
    assert!(
        after_promotion.queue_bytes < snapshot.queue_bytes,
        "lane promotion must release latency headroom from the shared path model"
    );
}

#[tokio::test]
async fn server_registry_all_startup_latency_flows_do_not_protect_against_each_other() {
    let registry = ServerTcpStreamRegistry::default();
    let session_id = SessionId(83);
    let target = TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80)));
    let (first_tx, _first_rx) = tcp_path_session_command_channels(4);
    let first_stream = match registry
        .open_or_attach(
            ServerTcpStreamOpenRequest {
                session_id,
                stream_id: StreamId(1),
                target: &target,
                lane: FlowLane::Latency,
                attachment: ServerTcpPathAttachment {
                    path_id: PathId(0),
                    underlay: UnderlayProtocol::Tcp,
                    commands: first_tx,
                    max_frame_payload_bytes: tcp_relay_buffer_len(MuxLimits::default()),
                    role: StreamOpenRole::Active,
                },
            },
            MuxLimits::default(),
            ResourceLimits::default().max_streams,
        )
        .expect("open first startup stream")
    {
        ServerTcpStreamOpen::New(stream) => stream,
        ServerTcpStreamOpen::Existing => panic!("expected first stream to be new"),
    };
    let (second_tx, _second_rx) = tcp_path_session_command_channels(4);
    let second_stream = match registry
        .open_or_attach(
            ServerTcpStreamOpenRequest {
                session_id,
                stream_id: StreamId(2),
                target: &target,
                lane: FlowLane::Latency,
                attachment: ServerTcpPathAttachment {
                    path_id: PathId(0),
                    underlay: UnderlayProtocol::Tcp,
                    commands: second_tx,
                    max_frame_payload_bytes: tcp_relay_buffer_len(MuxLimits::default()),
                    role: StreamOpenRole::Active,
                },
            },
            MuxLimits::default(),
            ResourceLimits::default().max_streams,
        )
        .expect("open second startup stream")
    {
        ServerTcpStreamOpen::New(stream) => stream,
        ServerTcpStreamOpen::Existing => panic!("expected second stream to be new"),
    };
    registry.record_local_path_metrics(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(0),
        test_path_metrics(PathId(0), UnderlayProtocol::Tcp, 40_000, 400_000_000),
    );

    let second_binding = switchable_binding(&second_stream);
    let startup_snapshot = second_binding
        .send_path_snapshot(FlowLane::Throughput, 64 * 1024)
        .expect("startup bulk snapshot");
    assert_eq!(startup_snapshot.active_flows, 2);
    assert_eq!(startup_snapshot.active_latency_sensitive_flows, 2);
    assert_eq!(
        startup_snapshot.queue_bytes, 0,
        "all-startup latency streams must not reserve latency headroom against each other"
    );

    let first_binding = switchable_binding(&first_stream);
    first_binding.set_lane(FlowLane::Throughput);
    let protected_snapshot = second_binding
        .send_path_snapshot(FlowLane::Throughput, 64 * 1024)
        .expect("protected bulk snapshot");
    assert_eq!(protected_snapshot.active_latency_sensitive_flows, 1);
    assert!(
        protected_snapshot.queue_bytes > startup_snapshot.queue_bytes,
        "once throughput exists, remaining latency streams must receive protected headroom"
    );
}

#[tokio::test]
async fn server_registry_bulk_admission_yields_to_path_without_latency_load() {
    let registry = ServerTcpStreamRegistry::default();
    let session_id = SessionId(82);
    let target = TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80)));
    let max_frame_payload_bytes = tcp_relay_buffer_len(MuxLimits::default());
    let (latency_tx, _latency_rx) = tcp_path_session_command_channels(4);
    match registry
        .open_or_attach(
            ServerTcpStreamOpenRequest {
                session_id,
                stream_id: StreamId(1),
                target: &target,
                lane: FlowLane::Latency,
                attachment: ServerTcpPathAttachment {
                    path_id: PathId(0),
                    underlay: UnderlayProtocol::Tcp,
                    commands: latency_tx,
                    max_frame_payload_bytes,
                    role: StreamOpenRole::Active,
                },
            },
            MuxLimits::default(),
            ResourceLimits::default().max_streams,
        )
        .expect("open latency stream")
    {
        ServerTcpStreamOpen::New(_) => {}
        ServerTcpStreamOpen::Existing => panic!("expected latency stream to be new"),
    }

    let (bulk_path0_tx, mut bulk_path0_rx) = tcp_path_session_command_channels(4);
    let bulk_stream = match registry
        .open_or_attach(
            ServerTcpStreamOpenRequest {
                session_id,
                stream_id: StreamId(2),
                target: &target,
                lane: FlowLane::Throughput,
                attachment: ServerTcpPathAttachment {
                    path_id: PathId(0),
                    underlay: UnderlayProtocol::Tcp,
                    commands: bulk_path0_tx,
                    max_frame_payload_bytes,
                    role: StreamOpenRole::Active,
                },
            },
            MuxLimits::default(),
            ResourceLimits::default().max_streams,
        )
        .expect("open bulk stream")
    {
        ServerTcpStreamOpen::New(stream) => stream,
        ServerTcpStreamOpen::Existing => panic!("expected bulk stream to be new"),
    };
    let binding = switchable_binding(&bulk_stream);
    let (bulk_path1_tx, mut bulk_path1_rx) = tcp_path_session_command_channels(4);
    binding.attach(
        UnderlayProtocol::Tcp,
        PathId(1),
        bulk_path1_tx,
        FlowLane::Throughput,
        StreamOpenRole::Validation,
        max_frame_payload_bytes,
    );
    registry.record_local_path_metrics(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(0),
        test_path_metrics(PathId(0), UnderlayProtocol::Tcp, 10_000, 100_000_000),
    );
    registry.record_local_path_metrics(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(1),
        test_path_metrics(PathId(1), UnderlayProtocol::Tcp, 16_000, 100_000_000),
    );

    assert_eq!(
        binding.bulk_choice_key_for_test(64 * 1024),
        Some(ServerTcpPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        }),
        "bulk should use the proven path without active latency load when the active path's protected queue makes completion worse"
    );

    binding
        .send_frame(
            StreamId(2),
            FlowLane::Throughput,
            Frame::StreamData {
                stream_id: StreamId(2),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from(vec![1u8; 64 * 1024]),
            },
        )
        .await
        .expect("send bulk frame");
    assert!(matches!(
        recv_tcp_path_command(&mut bulk_path1_rx).await,
        Some(TcpPathSessionCommand::SendFrame(Frame::StreamData {
            offset: 0,
            ..
        }))
    ));
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_tcp_path_command(&mut bulk_path0_rx)
        )
        .await
        .is_err(),
        "path with active latency load must not receive this bulk quantum"
    );
}

#[tokio::test]
async fn server_tcp_binding_interactive_repair_reattach_preserves_active_path() {
    let (path0_initial_tx, _path0_initial_rx) = tcp_path_session_command_channels(1);
    let binding = ServerTcpStreamBinding::new(
        SessionId(1),
        UnderlayProtocol::Tcp,
        PathId(0),
        path0_initial_tx,
        FlowLane::Latency,
    );
    let (path1_tx, mut path1_rx) = tcp_path_session_command_channels(1);
    binding.attach(
        UnderlayProtocol::Tcp,
        PathId(1),
        path1_tx,
        FlowLane::Latency,
        StreamOpenRole::Active,
        tcp_relay_buffer_len(MuxLimits::default()),
    );
    let (path0_repair_tx, mut path0_repair_rx) = tcp_path_session_command_channels(1);
    binding.attach(
        UnderlayProtocol::Tcp,
        PathId(0),
        path0_repair_tx,
        FlowLane::Latency,
        StreamOpenRole::Repair,
        tcp_relay_buffer_len(MuxLimits::default()),
    );

    binding
        .send_frame(
            StreamId(7),
            FlowLane::Latency,
            Frame::StreamData {
                stream_id: StreamId(7),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"auto-ramp"),
            },
        )
        .await
        .expect("send on active interactive path");

    match recv_tcp_path_command(&mut path1_rx)
        .await
        .expect("interactive active command")
    {
        TcpPathSessionCommand::SendFrame(Frame::StreamData { payload, .. }) => {
            assert_eq!(&payload[..], b"auto-ramp");
        }
        _ => panic!("expected data on active interactive path"),
    }
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_tcp_path_command(&mut path0_repair_rx)
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn server_tcp_binding_repair_reattach_preserves_realtime_data_path() {
    let (path0_initial_tx, _path0_initial_rx) = tcp_path_session_command_channels(1);
    let binding = ServerTcpStreamBinding::new(
        SessionId(1),
        UnderlayProtocol::Tcp,
        PathId(0),
        path0_initial_tx,
        FlowLane::Latency,
    );
    let (path1_tx, mut path1_rx) = tcp_path_session_command_channels(1);
    binding.attach(
        UnderlayProtocol::Tcp,
        PathId(1),
        path1_tx,
        FlowLane::RealtimeDatagram,
        StreamOpenRole::Active,
        tcp_relay_buffer_len(MuxLimits::default()),
    );
    let (path0_repair_tx, mut path0_repair_rx) = tcp_path_session_command_channels(1);
    binding.attach(
        UnderlayProtocol::Tcp,
        PathId(0),
        path0_repair_tx,
        FlowLane::RealtimeDatagram,
        StreamOpenRole::Repair,
        tcp_relay_buffer_len(MuxLimits::default()),
    );

    binding
        .send_frame(
            StreamId(7),
            FlowLane::Throughput,
            Frame::StreamData {
                stream_id: StreamId(7),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"active"),
            },
        )
        .await
        .expect("send on active path");

    match recv_tcp_path_command(&mut path1_rx)
        .await
        .expect("active path command")
    {
        TcpPathSessionCommand::SendFrame(Frame::StreamData { payload, .. }) => {
            assert_eq!(&payload[..], b"active");
        }
        _ => panic!("expected data on active path"),
    }
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_tcp_path_command(&mut path0_repair_rx)
        )
        .await
        .is_err()
    );
}
