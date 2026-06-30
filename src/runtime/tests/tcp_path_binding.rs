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
async fn server_udp_binding_bulk_validation_reattach_duplicates_bounded_probe_data() {
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
        recv_tcp_path_command(&mut path1_rx).await,
        Some(TcpPathSessionCommand::SendFrame(Frame::StreamData {
            offset: 0,
            ..
        }))
    ));
    assert!(matches!(
        recv_tcp_path_command(&mut path0_validation_rx).await,
        Some(TcpPathSessionCommand::SendFrame(Frame::StreamData {
            offset: 0,
            ..
        }))
    ));
}

#[tokio::test]
async fn server_udp_binding_validation_duplicate_ack_does_not_promote_without_carrier_evidence() {
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
    assert!(matches!(
        recv_tcp_path_command(&mut validation_rx).await,
        Some(TcpPathSessionCommand::SendFrame(Frame::StreamData {
            offset: 0,
            ..
        }))
    ));
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
    let active_metrics = test_path_metrics(PathId(1), UnderlayProtocol::Udp, 220_000, 40_000_000);
    let fast_metrics = test_path_metrics(PathId(0), UnderlayProtocol::Udp, 40_000, 400_000_000);
    binding.update_peer_path_metrics_for_test(UnderlayProtocol::Udp, PathId(1), active_metrics);
    binding.update_peer_path_metrics_for_test(UnderlayProtocol::Udp, PathId(0), fast_metrics);

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
    assert!(matches!(
        recv_tcp_path_command(&mut validation_rx).await,
        Some(TcpPathSessionCommand::SendFrame(Frame::StreamData {
            offset: 0,
            ..
        }))
    ));

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
async fn server_binding_treats_tcp_validation_metrics_as_hints_without_duplicate_data() {
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
        "TCP validation metrics are peer hints; without path-scoped sender proof they must not receive duplicate ordered stream data"
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
