use super::*;

fn test_tcp_session_runtime(reuse_latency_session: bool) -> ClientTcpPathSessionRuntime {
    ClientTcpPathSessionRuntime {
        path: PathSpec {
            underlay: UnderlayProtocol::Tcp,
            endpoint: Endpoint::new("127.0.0.1", 1).expect("endpoint"),
            metadata: crate::transport::PathMetadata::default(),
        },
        path_index: 0,
        session_id: SessionId(7),
        security: security(),
        codec_limits: CodecLimits::default(),
        mux_limits: MuxLimits::default(),
        command_queue: 4,
        stream_frame_queue: 4,
        closed_stream_cache_capacity: 8,
        reuse_latency_session,
    }
}

#[tokio::test]
async fn tcp_path_latency_lane_reuse_depends_on_topology() {
    let single_path = ClientTcpPathSessionHandle::new(test_tcp_session_runtime(false));
    let first_single = single_path.ensure_session(TrafficClass::Interactive);
    let second_single = single_path.ensure_session(TrafficClass::Interactive);
    assert!(!first_single.same_channel(&second_single));

    let multipath = ClientTcpPathSessionHandle::new(test_tcp_session_runtime(true));
    let first_latency = multipath.ensure_session(TrafficClass::Interactive);
    let second_latency = multipath.ensure_session(TrafficClass::Interactive);
    let realtime_latency = multipath.ensure_session(TrafficClass::RealtimeDatagram);
    assert!(first_latency.same_channel(&second_latency));
    assert!(first_latency.same_channel(&realtime_latency));

    let bulk = multipath.ensure_session(TrafficClass::Bulk);
    assert!(!first_latency.same_channel(&bulk));
}

#[tokio::test]
async fn tcp_path_control_command_bypasses_saturated_data_queue() {
    let (tx, mut rx) = tcp_path_session_command_channels(1);
    tx.send_frame(
        Frame::StreamData {
            stream_id: StreamId(3),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(b"queued-data"),
        },
        TrafficClass::Bulk,
    )
    .await
    .expect("fill data queue");

    tokio::time::timeout(
        Duration::from_millis(50),
        tx.send_control(TcpPathSessionCommand::CloseStream(StreamId(3))),
    )
    .await
    .expect("control send should not wait for data queue")
    .expect("control send");

    match recv_tcp_path_command(&mut rx).await.expect("first command") {
        TcpPathSessionCommand::CloseStream(stream_id) => assert_eq!(stream_id, StreamId(3)),
        _ => panic!("expected prioritized close stream control"),
    }
}

#[tokio::test]
async fn tcp_path_interactive_frame_bypasses_saturated_bulk_queue() {
    let (tx, mut rx) = tcp_path_session_command_channels(1);
    tx.send_frame(
        Frame::StreamData {
            stream_id: StreamId(10),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(b"bulk"),
        },
        TrafficClass::Bulk,
    )
    .await
    .expect("fill bulk data queue");

    tokio::time::timeout(
        Duration::from_millis(50),
        tx.send_frame(
            Frame::StreamData {
                stream_id: StreamId(11),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"i"),
            },
            TrafficClass::Interactive,
        ),
    )
    .await
    .expect("interactive send should not wait for bulk queue")
    .expect("interactive send");

    match recv_tcp_path_command(&mut rx).await.expect("first command") {
        TcpPathSessionCommand::SendFrame(Frame::StreamData {
            stream_id, payload, ..
        }) => {
            assert_eq!(stream_id, StreamId(11));
            assert_eq!(&payload[..], b"i");
        }
        _ => panic!("expected prioritized interactive stream data"),
    }
}

#[tokio::test]
async fn server_tcp_path_input_frame_bypasses_queued_bulk_output() {
    let (tx, mut commands_rx) = tcp_path_session_command_channels(1);
    tx.send_frame(
        Frame::StreamData {
            stream_id: StreamId(10),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(b"bulk"),
        },
        TrafficClass::Bulk,
    )
    .await
    .expect("fill bulk output command queue");
    let (frame_tx, mut path_frames) = mpsc::channel(1);
    frame_tx
        .send(Ok(Frame::Ping { nonce: 7 }))
        .await
        .expect("queue inbound ping");

    match recv_server_tcp_path_event(&mut path_frames, &mut commands_rx)
        .await
        .expect("server path event")
        .expect("event")
    {
        ServerTcpPathEvent::Frame(Frame::Ping { nonce }) => assert_eq!(nonce, 7),
        _ => panic!("expected inbound frame before queued bulk output"),
    }
}

#[tokio::test]
async fn client_tcp_path_ignores_late_frames_for_recently_closed_stream() {
    let stream_id = StreamId(7);
    let (frames_tx, frames_rx) = mpsc::channel(1);
    let mut streams = HashMap::new();
    streams.insert(
        stream_id,
        ClientTcpPathStreamState {
            frames: frames_tx,
            pending_open: None,
        },
    );
    let mut closed_streams = RecentIdCache::new(8);
    drop(frames_rx);

    route_client_tcp_stream_frame(
        &mut streams,
        &mut closed_streams,
        stream_id,
        Frame::StreamFin {
            stream_id,
            final_offset: 0,
        },
    )
    .await
    .expect("receiver close should mark stream drained");
    assert!(!streams.contains_key(&stream_id));
    assert!(closed_streams.contains(&stream_id));

    route_client_tcp_stream_frame(
        &mut streams,
        &mut closed_streams,
        stream_id,
        Frame::StreamAck {
            stream_id,
            ranges: Vec::new(),
        },
    )
    .await
    .expect("late frame for closed stream should be ignored");

    let unknown = StreamId(99);
    let err = route_client_tcp_stream_frame(
        &mut streams,
        &mut closed_streams,
        unknown,
        Frame::StreamFin {
            stream_id: unknown,
            final_offset: 0,
        },
    )
    .await
    .expect_err("unknown stream should remain a protocol error");
    assert!(matches!(err, RuntimeError::Protocol(_)));
}

#[tokio::test]
async fn server_tcp_registry_ignores_late_frames_for_recently_closed_stream() {
    let registry = ServerTcpStreamRegistry::new(8);
    let session_id = SessionId(11);
    let stream_id = StreamId(5);
    let (commands, _receivers) = tcp_path_session_command_channels(4);
    let target = TargetAddr::Domain {
        host: "example.com".to_string(),
        port: 443,
    };

    let opened = registry
        .open_or_attach(
            ServerTcpStreamOpenRequest {
                session_id,
                stream_id,
                target: &target,
                class: TrafficClass::Interactive,
                attachment: ServerTcpPathAttachment {
                    path_id: PathId(0),
                    underlay: UnderlayProtocol::Tcp,
                    commands,
                    max_frame_payload_bytes: tcp_relay_buffer_len(MuxLimits::default()),
                    role: StreamOpenRole::Active,
                },
            },
            MuxLimits::default(),
            8,
        )
        .expect("server stream open");
    assert!(matches!(opened, ServerTcpStreamOpen::New(_)));

    registry.close(session_id, stream_id);
    registry
        .route_frame(
            session_id,
            stream_id,
            Frame::StreamFin {
                stream_id,
                final_offset: 0,
            },
        )
        .await
        .expect("late server stream frame should be ignored");

    let unknown = StreamId(99);
    let err = registry
        .route_frame(
            session_id,
            unknown,
            Frame::StreamFin {
                stream_id: unknown,
                final_offset: 0,
            },
        )
        .await
        .expect_err("unknown server stream should remain a protocol error");
    assert!(matches!(err, RuntimeError::Protocol(_)));
}

#[tokio::test]
async fn server_tcp_binding_reselects_blocked_data_send_after_path_update() {
    let (old_tx, _old_rx) = tcp_path_session_command_channels(1);
    old_tx
        .send_frame(
            Frame::StreamData {
                stream_id: StreamId(99),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"fill"),
            },
            TrafficClass::Interactive,
        )
        .await
        .expect("fill old path priority command queue");
    let binding = ServerTcpStreamBinding::new(
        UnderlayProtocol::Tcp,
        PathId(0),
        old_tx,
        TrafficClass::Interactive,
    );
    let send_binding = binding.clone();
    let send_task = tokio::spawn(async move {
        send_binding
            .send_frame(
                StreamId(7),
                TrafficClass::Bulk,
                Frame::StreamData {
                    stream_id: StreamId(7),
                    offset: 0,
                    flags: StreamFlags::NONE,
                    payload: Bytes::from_static(b"bulk"),
                },
            )
            .await
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(!send_task.is_finished());

    let (new_tx, mut new_rx) = tcp_path_session_command_channels(1);
    binding.attach(
        UnderlayProtocol::Tcp,
        PathId(1),
        new_tx,
        TrafficClass::Bulk,
        StreamOpenRole::Active,
    );
    assert_eq!(binding.class(), TrafficClass::Bulk);
    send_task
        .await
        .expect("binding send join")
        .expect("binding send");
    match recv_tcp_path_command(&mut new_rx)
        .await
        .expect("new path command")
    {
        TcpPathSessionCommand::SendFrame(Frame::StreamData {
            stream_id, payload, ..
        }) => {
            assert_eq!(stream_id, StreamId(7));
            assert_eq!(&payload[..], b"bulk");
        }
        _ => panic!("expected stream data on reselected path"),
    }
}

#[tokio::test]
async fn server_tcp_binding_active_reattach_promotes_existing_path_for_data() {
    let (path0_initial_tx, _path0_initial_rx) = tcp_path_session_command_channels(1);
    let binding = ServerTcpStreamBinding::new(
        UnderlayProtocol::Tcp,
        PathId(0),
        path0_initial_tx,
        TrafficClass::Interactive,
    );
    let (path1_tx, mut path1_rx) = tcp_path_session_command_channels(1);
    binding.attach(
        UnderlayProtocol::Tcp,
        PathId(1),
        path1_tx,
        TrafficClass::Bulk,
        StreamOpenRole::Active,
    );
    let (path0_repair_tx, mut path0_repair_rx) = tcp_path_session_command_channels(1);
    binding.attach(
        UnderlayProtocol::Tcp,
        PathId(0),
        path0_repair_tx,
        TrafficClass::Bulk,
        StreamOpenRole::Active,
    );

    binding
        .send_frame(
            StreamId(7),
            TrafficClass::Bulk,
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
async fn server_tcp_binding_bulk_repair_reattach_promotes_for_throughput() {
    let (path0_initial_tx, _path0_initial_rx) = tcp_path_session_command_channels(1);
    let binding = ServerTcpStreamBinding::new(
        UnderlayProtocol::Tcp,
        PathId(0),
        path0_initial_tx,
        TrafficClass::Bulk,
    );
    let (path1_tx, mut path1_rx) = tcp_path_session_command_channels(1);
    binding.attach(
        UnderlayProtocol::Tcp,
        PathId(1),
        path1_tx,
        TrafficClass::Bulk,
        StreamOpenRole::Active,
    );
    let (path0_repair_tx, mut path0_repair_rx) = tcp_path_session_command_channels(1);
    binding.attach(
        UnderlayProtocol::Tcp,
        PathId(0),
        path0_repair_tx,
        TrafficClass::Bulk,
        StreamOpenRole::Repair,
    );

    binding
        .send_frame(
            StreamId(7),
            TrafficClass::Bulk,
            Frame::StreamData {
                stream_id: StreamId(7),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"bulk-repair"),
            },
        )
        .await
        .expect("send on promoted bulk repair path");

    match recv_tcp_path_command(&mut path0_repair_rx)
        .await
        .expect("bulk repair command")
    {
        TcpPathSessionCommand::SendFrame(Frame::StreamData { payload, .. }) => {
            assert_eq!(&payload[..], b"bulk-repair");
        }
        _ => panic!("expected data on promoted bulk repair path"),
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
async fn server_tcp_binding_interactive_repair_reattach_promotes_for_auto_ramp() {
    let (path0_initial_tx, _path0_initial_rx) = tcp_path_session_command_channels(1);
    let binding = ServerTcpStreamBinding::new(
        UnderlayProtocol::Tcp,
        PathId(0),
        path0_initial_tx,
        TrafficClass::Interactive,
    );
    let (path1_tx, mut path1_rx) = tcp_path_session_command_channels(1);
    binding.attach(
        UnderlayProtocol::Tcp,
        PathId(1),
        path1_tx,
        TrafficClass::Interactive,
        StreamOpenRole::Active,
    );
    let (path0_repair_tx, mut path0_repair_rx) = tcp_path_session_command_channels(1);
    binding.attach(
        UnderlayProtocol::Tcp,
        PathId(0),
        path0_repair_tx,
        TrafficClass::Interactive,
        StreamOpenRole::Repair,
    );

    binding
        .send_frame(
            StreamId(7),
            TrafficClass::Interactive,
            Frame::StreamData {
                stream_id: StreamId(7),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"auto-ramp"),
            },
        )
        .await
        .expect("send on promoted interactive repair path");

    match recv_tcp_path_command(&mut path0_repair_rx)
        .await
        .expect("interactive repair command")
    {
        TcpPathSessionCommand::SendFrame(Frame::StreamData { payload, .. }) => {
            assert_eq!(&payload[..], b"auto-ramp");
        }
        _ => panic!("expected data on promoted interactive repair path"),
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
async fn server_tcp_binding_repair_reattach_preserves_realtime_data_path() {
    let (path0_initial_tx, _path0_initial_rx) = tcp_path_session_command_channels(1);
    let binding = ServerTcpStreamBinding::new(
        UnderlayProtocol::Tcp,
        PathId(0),
        path0_initial_tx,
        TrafficClass::Interactive,
    );
    let (path1_tx, mut path1_rx) = tcp_path_session_command_channels(1);
    binding.attach(
        UnderlayProtocol::Tcp,
        PathId(1),
        path1_tx,
        TrafficClass::RealtimeDatagram,
        StreamOpenRole::Active,
    );
    let (path0_repair_tx, mut path0_repair_rx) = tcp_path_session_command_channels(1);
    binding.attach(
        UnderlayProtocol::Tcp,
        PathId(0),
        path0_repair_tx,
        TrafficClass::RealtimeDatagram,
        StreamOpenRole::Repair,
    );

    binding
        .send_frame(
            StreamId(7),
            TrafficClass::Bulk,
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

#[tokio::test]
async fn server_tcp_relay_replays_response_repair_cache_on_path_reattach() {
    let mux_limits = MuxLimits::default();
    let stream_id = StreamId(42);
    let (mut target_peer, target_side) = duplex(4096);
    let (commands_tx, mut commands_rx) = tcp_path_session_command_channels(8);
    let (frames_tx, frames_rx) = mpsc::channel(8);
    let relay = tokio::spawn(relay_tcp_stream(
        target_side,
        TcpPathStream {
            stream_id,
            max_offset: mux_limits.max_stream_window_bytes,
            class: TrafficClass::Interactive,
            underlay: UnderlayProtocol::Tcp,
            max_frame_payload_bytes: tcp_relay_buffer_len(mux_limits),
            output: TcpPathStreamOutput::Fixed(commands_tx),
            frames: frames_rx,
        },
        mux_limits,
    ));

    target_peer
        .write_all(b"response")
        .await
        .expect("target write");
    let first = tokio::time::timeout(
        Duration::from_secs(1),
        recv_tcp_path_command(&mut commands_rx),
    )
    .await
    .expect("first relay frame timeout")
    .expect("first relay frame");
    match first {
        TcpPathSessionCommand::SendFrame(Frame::StreamData {
            stream_id: received_stream_id,
            offset,
            payload,
            ..
        }) => {
            assert_eq!(received_stream_id, stream_id);
            assert_eq!(offset, 0);
            assert_eq!(&payload[..], b"response");
        }
        _ => panic!("expected first response stream data"),
    }

    frames_tx
        .send(Ok(Frame::PathStatus {
            path_id: PathId(1),
            status: crate::protocol::PathStatus::Active,
            capabilities: Default::default(),
        }))
        .await
        .expect("reattach signal");
    let replay = tokio::time::timeout(
        Duration::from_secs(1),
        recv_tcp_path_command(&mut commands_rx),
    )
    .await
    .expect("replay frame timeout")
    .expect("replay frame");
    match replay {
        TcpPathSessionCommand::SendFrame(Frame::StreamData {
            stream_id: received_stream_id,
            offset,
            payload,
            ..
        }) => {
            assert_eq!(received_stream_id, stream_id);
            assert_eq!(offset, 0);
            assert_eq!(&payload[..], b"response");
        }
        _ => panic!("expected replayed response stream data"),
    }

    relay.abort();
    let _ = relay.await;
}

#[test]
fn client_path_health_suppresses_failed_paths_until_cooldown() {
    let fast_path = "tcp://127.0.0.1:10001?srtt-ms=5&rate-mbps=100"
        .parse::<PathSpec>()
        .expect("fast path");
    let slow_path = "tcp://127.0.0.1:10002?srtt-ms=200&rate-mbps=100"
        .parse::<PathSpec>()
        .expect("slow path");
    let context = ClientPathContext::new(
        vec![fast_path, slow_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    assert_eq!(
        context
            .ordered_tcp_path_indices(TrafficClass::Interactive, 512)
            .first()
            .copied(),
        Some(0)
    );
    context.mark_tcp_path_failure(0);
    let suspect_order = context.ordered_tcp_path_indices(TrafficClass::Interactive, 512);
    assert_eq!(suspect_order, vec![0, 1]);
    context.mark_tcp_path_failure(0);
    let failed_order = context.ordered_tcp_path_indices(TrafficClass::Interactive, 512);
    assert_eq!(failed_order, vec![1]);

    {
        let mut health = context.health.lock().expect("health lock");
        health.tcp[0].failed_until = Some(Instant::now() - Duration::from_millis(1));
    }
    let recovered_order = context.ordered_tcp_path_indices(TrafficClass::Interactive, 512);
    assert!(recovered_order.contains(&0));
}

#[test]
fn single_path_failure_stays_probeable_without_alternative() {
    let tcp_path = "tcp://127.0.0.1:10003?srtt-ms=5&rate-mbps=100"
        .parse::<PathSpec>()
        .expect("tcp path");
    let tcp_context = ClientPathContext::new(vec![tcp_path], security(), ResourceLimits::default())
        .expect("tcp context");
    tcp_context.mark_tcp_path_failure(0);
    tcp_context.mark_tcp_path_failure(0);
    assert_eq!(
        tcp_context.ordered_tcp_path_indices(TrafficClass::Interactive, 512),
        vec![0]
    );

    let udp_path = "udp://127.0.0.1:10004?srtt-ms=5&rate-mbps=100"
        .parse::<PathSpec>()
        .expect("udp path");
    let udp_context = ClientPathContext::new(vec![udp_path], security(), ResourceLimits::default())
        .expect("udp context");
    udp_context.mark_udp_path_failure(0);
    udp_context.mark_udp_path_failure(0);
    assert_eq!(
        udp_stream_path_indices(&udp_context, TrafficClass::Interactive, 512),
        vec![0]
    );
}

#[test]
fn measured_path_latency_updates_next_scheduling_order() {
    let first_path = "tcp://127.0.0.1:10011?srtt-ms=50&rate-mbps=100"
        .parse::<PathSpec>()
        .expect("first path");
    let second_path = "tcp://127.0.0.1:10012?srtt-ms=50&rate-mbps=100"
        .parse::<PathSpec>()
        .expect("second path");
    let context = ClientPathContext::new(
        vec![first_path, second_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.mark_tcp_path_open_success(0, Duration::from_millis(120), TrafficClass::Interactive);
    context.mark_tcp_path_open_success(1, Duration::from_millis(5), TrafficClass::Interactive);

    assert_eq!(
        context
            .ordered_tcp_path_indices(TrafficClass::Interactive, 512)
            .first()
            .copied(),
        Some(1)
    );
}

#[test]
fn measured_tcp_delivery_rate_updates_next_bulk_order() {
    let hinted_slow_path = "tcp://127.0.0.1:10013?srtt-ms=20&rate-mbps=10"
        .parse::<PathSpec>()
        .expect("hinted slow path");
    let hinted_fast_path = "tcp://127.0.0.1:10014?srtt-ms=20&rate-mbps=100"
        .parse::<PathSpec>()
        .expect("hinted fast path");
    let context = ClientPathContext::new(
        vec![hinted_slow_path, hinted_fast_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    assert_eq!(
        context
            .ordered_tcp_path_indices(TrafficClass::Bulk, 4 * 1024 * 1024)
            .first()
            .copied(),
        Some(1)
    );

    context.mark_tcp_path_delivery(
        0,
        PathDeliveryStats {
            payload_bytes: 4 * 1024 * 1024,
            first_payload_at: Some(Instant::now()),
            last_payload_at: Some(Instant::now() + Duration::from_millis(40)),
        },
    );

    assert_eq!(
        context
            .ordered_tcp_path_indices(TrafficClass::Bulk, 4 * 1024 * 1024)
            .first()
            .copied(),
        Some(0)
    );
}

#[test]
fn auto_bulk_discovery_uses_bulk_horizon_for_unmeasured_high_bandwidth_path() {
    let low_latency_path = "tcp://127.0.0.1:10015?srtt-ms=20&rate-mbps=30&low-latency=true"
        .parse::<PathSpec>()
        .expect("low-latency path");
    let high_bandwidth_path = "tcp://127.0.0.1:10016?srtt-ms=180&rate-mbps=300"
        .parse::<PathSpec>()
        .expect("high-bandwidth path");
    let context = ClientPathContext::new(
        vec![low_latency_path, high_bandwidth_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    assert_eq!(
        tcp_auto_bulk_discovery_indices(
            &context,
            Some(0),
            MuxLimits::default().max_tcp_path_inflight_bytes,
        )
        .first()
        .copied(),
        Some(1)
    );
}

#[test]
fn auto_bulk_discovery_skips_unmeasured_expensive_path() {
    let low_latency_path = "tcp://127.0.0.1:10017?srtt-ms=20&rate-mbps=30&low-latency=true"
        .parse::<PathSpec>()
        .expect("low-latency path");
    let expensive_path = "tcp://127.0.0.1:10018?srtt-ms=80&rate-mbps=500&expensive=true"
        .parse::<PathSpec>()
        .expect("expensive path");
    let context = ClientPathContext::new(
        vec![low_latency_path, expensive_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    assert!(
        tcp_auto_bulk_discovery_indices(
            &context,
            Some(0),
            MuxLimits::default().max_tcp_path_inflight_bytes,
        )
        .is_empty()
    );
}

#[test]
fn bulk_repair_does_not_attach_worse_path_when_current_path_is_best() {
    let low_latency_path = "tcp://127.0.0.1:10128?srtt-ms=20&rate-mbps=30"
        .parse::<PathSpec>()
        .expect("low-latency path");
    let poor_path = "tcp://127.0.0.1:10129?srtt-ms=420&jitter-ms=120&rate-mbps=8"
        .parse::<PathSpec>()
        .expect("poor path");
    let context = ClientPathContext::new(
        vec![low_latency_path, poor_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    assert!(
        context
            .ordered_tcp_repair_path_indices(Some(0), TrafficClass::Bulk, 4 * 1024 * 1024)
            .is_empty()
    );
    assert_eq!(
        context.ordered_tcp_repair_path_indices(Some(1), TrafficClass::Bulk, 4 * 1024 * 1024),
        vec![0]
    );
    assert_eq!(
        context
            .ordered_tcp_repair_path_indices(Some(0), TrafficClass::Interactive, 512)
            .first()
            .copied(),
        Some(0)
    );
}

#[test]
fn endpoint_only_tcp_bulk_discovery_waits_for_delivery_evidence_before_probe_noise() {
    let low_latency_path = "tcp://127.0.0.1:10132"
        .parse::<PathSpec>()
        .expect("low latency path");
    let high_bandwidth_path = "tcp://127.0.0.1:10133"
        .parse::<PathSpec>()
        .expect("high bandwidth path");
    let poor_path = "tcp://127.0.0.1:10134"
        .parse::<PathSpec>()
        .expect("poor path");
    let context = ClientPathContext::new(
        vec![low_latency_path, high_bandwidth_path, poor_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.mark_tcp_path_open_success(0, Duration::from_millis(20), TrafficClass::Interactive);
    context.mark_tcp_path_probe_success(2, Duration::from_millis(1));

    assert!(
        tcp_auto_bulk_discovery_indices(
            &context,
            Some(0),
            MuxLimits::default().max_tcp_path_inflight_bytes
        )
        .is_empty()
    );

    let now = Instant::now();
    context.mark_tcp_path_delivery(
        1,
        PathDeliveryStats {
            payload_bytes: 4 * 1024 * 1024,
            first_payload_at: Some(now),
            last_payload_at: Some(now + Duration::from_millis(40)),
        },
    );

    assert_eq!(
        tcp_auto_bulk_discovery_indices(
            &context,
            Some(0),
            MuxLimits::default().max_tcp_path_inflight_bytes,
        ),
        vec![1]
    );
}

#[test]
fn endpoint_only_tcp_bulk_discovery_still_requires_delivery_after_bulk_promotion() {
    let low_latency_path = "tcp://127.0.0.1:10162"
        .parse::<PathSpec>()
        .expect("low latency path");
    let balanced_path = "tcp://127.0.0.1:10163"
        .parse::<PathSpec>()
        .expect("balanced path");
    let fat_path = "tcp://127.0.0.1:10164"
        .parse::<PathSpec>()
        .expect("fat path");
    let context = ClientPathContext::new(
        vec![low_latency_path, balanced_path, fat_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.mark_tcp_path_open_success(0, Duration::from_millis(20), TrafficClass::Interactive);
    context.reclassify_relay_path_load(
        UnderlayProtocol::Tcp,
        0,
        TrafficClass::Interactive,
        TrafficClass::Bulk,
    );

    assert!(
        tcp_auto_bulk_discovery_indices(
            &context,
            Some(0),
            MuxLimits::default().max_tcp_path_inflight_bytes,
        )
        .is_empty()
    );
}

#[test]
fn bulk_promotion_reclassifies_active_path_load_without_leaking_flow_accounting() {
    let path = "tcp://127.0.0.1:10165".parse::<PathSpec>().expect("path");
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("context");

    context.mark_tcp_path_open_success(0, Duration::from_millis(20), TrafficClass::Interactive);
    {
        let health = context.health.lock().expect("client path health lock");
        assert_eq!(health.tcp[0].active_flows, 1);
        assert_eq!(health.tcp[0].active_latency_sensitive_flows, 1);
        assert_eq!(health.tcp[0].load_bytes, TCP_STREAM_LOAD_BYTES);
    }

    context.reclassify_relay_path_load(
        UnderlayProtocol::Tcp,
        0,
        TrafficClass::Interactive,
        TrafficClass::Bulk,
    );
    {
        let health = context.health.lock().expect("client path health lock");
        assert_eq!(health.tcp[0].active_flows, 1);
        assert_eq!(health.tcp[0].active_latency_sensitive_flows, 0);
        assert_eq!(health.tcp[0].load_bytes, TCP_STREAM_LOAD_BYTES);
    }

    context.release_relay_path_load(UnderlayProtocol::Tcp, 0, TrafficClass::Bulk);
    let health = context.health.lock().expect("client path health lock");
    assert_eq!(health.tcp[0].active_flows, 0);
    assert_eq!(health.tcp[0].active_latency_sensitive_flows, 0);
    assert_eq!(health.tcp[0].load_bytes, 0);
}

#[test]
fn endpoint_only_tcp_bulk_discovery_requires_delivery_under_concurrent_latency_demand() {
    let low_latency_path = "tcp://127.0.0.1:10146"
        .parse::<PathSpec>()
        .expect("low latency path");
    let high_bandwidth_path = "tcp://127.0.0.1:10147"
        .parse::<PathSpec>()
        .expect("high bandwidth path");
    let context = ClientPathContext::new(
        vec![low_latency_path, high_bandwidth_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.mark_tcp_path_open_success(0, Duration::from_millis(20), TrafficClass::Interactive);
    assert!(
        tcp_auto_bulk_discovery_indices(
            &context,
            Some(0),
            MuxLimits::default().max_tcp_path_inflight_bytes,
        )
        .is_empty()
    );

    context.mark_tcp_path_open_success(0, Duration::from_millis(20), TrafficClass::Interactive);
    let now = Instant::now();
    context.mark_tcp_path_delivery(
        1,
        PathDeliveryStats {
            payload_bytes: 4 * 1024 * 1024,
            first_payload_at: Some(now),
            last_payload_at: Some(now + Duration::from_millis(40)),
        },
    );
    assert_eq!(
        tcp_auto_bulk_discovery_indices(
            &context,
            Some(0),
            MuxLimits::default().max_tcp_path_inflight_bytes,
        ),
        vec![1]
    );
}

#[test]
fn endpoint_only_udp_stream_startup_preserves_configured_order_on_probe_noise() {
    let first_path = "udp://127.0.0.1:10135"
        .parse::<PathSpec>()
        .expect("first path");
    let second_path = "udp://127.0.0.1:10136"
        .parse::<PathSpec>()
        .expect("second path");
    let context = ClientPathContext::new(
        vec![first_path, second_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.mark_udp_path_failure(0);
    context.mark_udp_path_probe_success(1, Duration::from_millis(1));

    assert_eq!(
        udp_stream_path_indices(&context, TrafficClass::Interactive, PATH_OPEN_SCORE_BYTES),
        vec![0, 1]
    );

    context.mark_udp_path_failure(0);
    assert_eq!(
        udp_stream_path_indices(&context, TrafficClass::Interactive, PATH_OPEN_SCORE_BYTES),
        vec![1]
    );
}

#[test]
fn endpoint_only_udp_stream_auto_bulk_discovery_waits_for_delivery_evidence() {
    let low_latency_path = "udp://127.0.0.1:10137"
        .parse::<PathSpec>()
        .expect("low latency path");
    let high_bandwidth_path = "udp://127.0.0.1:10138"
        .parse::<PathSpec>()
        .expect("high bandwidth path");
    let poor_path = "udp://127.0.0.1:10139"
        .parse::<PathSpec>()
        .expect("poor path");
    let context = ClientPathContext::new(
        vec![low_latency_path, high_bandwidth_path, poor_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.mark_udp_path_probe_success(1, Duration::from_millis(1));
    assert!(
        context
            .ordered_udp_stream_auto_bulk_discovery_indices(
                Some(0),
                MuxLimits::default().max_tcp_path_inflight_bytes,
            )
            .is_empty()
    );

    let now = Instant::now();
    context.mark_udp_path_delivery(
        1,
        PathDeliveryStats {
            payload_bytes: 4 * 1024 * 1024,
            first_payload_at: Some(now),
            last_payload_at: Some(now + Duration::from_millis(40)),
        },
    );

    assert_eq!(
        context.ordered_udp_stream_auto_bulk_discovery_indices(
            Some(0),
            MuxLimits::default().max_tcp_path_inflight_bytes,
        ),
        vec![1]
    );
}

#[test]
fn mixed_udp_repair_waits_for_delivery_evidence_on_active_tcp_stream() {
    let tcp_path = "tcp://127.0.0.1:10157"
        .parse::<PathSpec>()
        .expect("tcp path");
    let udp_low_latency_path = "udp://127.0.0.1:10158"
        .parse::<PathSpec>()
        .expect("udp low latency path");
    let udp_probe_only_path = "udp://127.0.0.1:10159"
        .parse::<PathSpec>()
        .expect("udp probe path");
    let context = ClientPathContext::new(
        vec![tcp_path, udp_low_latency_path, udp_probe_only_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.mark_udp_path_probe_success(1, Duration::from_millis(1));
    assert!(
        context
            .ordered_udp_stream_repair_path_indices(
                None,
                TrafficClass::Bulk,
                MuxLimits::default().max_tcp_path_inflight_bytes,
                true,
            )
            .is_empty()
    );
    assert_eq!(
        context
            .ordered_udp_stream_repair_path_indices(
                None,
                TrafficClass::Bulk,
                MuxLimits::default().max_tcp_path_inflight_bytes,
                false,
            )
            .first()
            .copied(),
        Some(1)
    );

    let now = Instant::now();
    context.mark_udp_path_delivery(
        1,
        PathDeliveryStats {
            payload_bytes: 4 * 1024 * 1024,
            first_payload_at: Some(now),
            last_payload_at: Some(now + Duration::from_millis(40)),
        },
    );

    assert_eq!(
        context.ordered_udp_stream_repair_path_indices(
            None,
            TrafficClass::Bulk,
            MuxLimits::default().max_tcp_path_inflight_bytes,
            true,
        ),
        vec![1]
    );
}

#[test]
fn udp_repair_waits_for_delivery_evidence_on_active_endpoint_only_stream() {
    let udp_low_latency_path = "udp://127.0.0.1:10160"
        .parse::<PathSpec>()
        .expect("udp low latency path");
    let udp_probe_path = "udp://127.0.0.1:10161"
        .parse::<PathSpec>()
        .expect("udp probe path");
    let context = ClientPathContext::new(
        vec![udp_low_latency_path, udp_probe_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.mark_udp_path_probe_success(1, Duration::from_millis(1));
    assert!(
        context
            .ordered_udp_stream_repair_path_indices(
                Some(0),
                TrafficClass::Bulk,
                MuxLimits::default().max_tcp_path_inflight_bytes,
                true,
            )
            .is_empty()
    );
    assert_eq!(
        context
            .ordered_udp_stream_repair_path_indices(
                Some(0),
                TrafficClass::Bulk,
                MuxLimits::default().max_tcp_path_inflight_bytes,
                false,
            )
            .first()
            .copied(),
        Some(1)
    );

    let now = Instant::now();
    context.mark_udp_path_delivery(
        1,
        PathDeliveryStats {
            payload_bytes: 4 * 1024 * 1024,
            first_payload_at: Some(now),
            last_payload_at: Some(now + Duration::from_millis(40)),
        },
    );

    assert_eq!(
        context.ordered_udp_stream_repair_path_indices(
            Some(0),
            TrafficClass::Bulk,
            MuxLimits::default().max_tcp_path_inflight_bytes,
            true,
        ),
        vec![1]
    );
}

#[test]
fn mixed_auto_bulk_discovery_can_cross_to_better_udp_carrier() {
    let tcp_path = "tcp://127.0.0.1:10140?srtt-ms=20&rate-mbps=30"
        .parse::<PathSpec>()
        .expect("tcp path");
    let udp_path = "udp://127.0.0.1:10141?srtt-ms=40&rate-mbps=300"
        .parse::<PathSpec>()
        .expect("udp path");
    let context = ClientPathContext::new(
        vec![tcp_path, udp_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    assert_eq!(
        context.ordered_reliable_auto_bulk_discovery_path_keys(
            Some(0),
            None,
            MuxLimits::default().max_tcp_path_inflight_bytes,
        ),
        vec![RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        }]
    );
}

#[test]
fn mixed_auto_bulk_discovery_rejects_worse_udp_carrier() {
    let tcp_path = "tcp://127.0.0.1:10140?srtt-ms=20&rate-mbps=300"
        .parse::<PathSpec>()
        .expect("tcp path");
    let udp_path = "udp://127.0.0.1:10141?srtt-ms=180&rate-mbps=30"
        .parse::<PathSpec>()
        .expect("udp path");
    let context = ClientPathContext::new(
        vec![tcp_path, udp_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    assert_eq!(
        context.ordered_reliable_auto_bulk_discovery_path_keys(
            Some(0),
            None,
            MuxLimits::default().max_tcp_path_inflight_bytes,
        ),
        Vec::<RelayPathKey>::new()
    );
}

#[test]
fn mixed_auto_bulk_discovery_penalizes_lossy_high_rtt_udp_carrier() {
    let tcp_path = "tcp://127.0.0.1:10142?srtt-ms=250&rate-mbps=25"
        .parse::<PathSpec>()
        .expect("tcp path");
    let udp_path = "udp://127.0.0.1:10143?srtt-ms=250&jitter-ms=60&rate-mbps=200"
        .parse::<PathSpec>()
        .expect("udp path");
    let context = ClientPathContext::new(
        vec![tcp_path, udp_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    context.mark_udp_path_feedback(
        0,
        UdpDatagramPathObservation {
            rtt: Duration::from_millis(250),
            jitter: Duration::from_millis(60),
            loss_rate: 0.01,
            rate_sample: PathRateSample::new(4 * 1024 * 1024, Duration::from_millis(170)),
        },
    );

    assert_eq!(
        context.ordered_reliable_auto_bulk_discovery_path_keys(
            Some(0),
            None,
            MuxLimits::default().max_tcp_path_inflight_bytes,
        ),
        Vec::<RelayPathKey>::new()
    );
}

#[test]
fn mixed_auto_bulk_discovery_can_choose_best_carrier_without_active_cohort() {
    let tcp_path = "tcp://127.0.0.1:10144?srtt-ms=20&rate-mbps=30"
        .parse::<PathSpec>()
        .expect("tcp path");
    let udp_path = "udp://127.0.0.1:10145?srtt-ms=40&rate-mbps=300"
        .parse::<PathSpec>()
        .expect("udp path");
    let context = ClientPathContext::new(
        vec![tcp_path, udp_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    assert_eq!(
        context
            .ordered_reliable_auto_bulk_discovery_path_keys(
                None,
                None,
                MuxLimits::default().max_tcp_path_inflight_bytes,
            )
            .first()
            .copied(),
        Some(RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        })
    );
}

#[test]
fn relay_candidate_filter_preserves_current_carrier_cohort() {
    let tcp = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 1,
    };
    let udp = RelayPathKey {
        underlay: UnderlayProtocol::Udp,
        index: 2,
    };

    assert_eq!(
        relay_path_candidates_for_active_carrier(vec![udp, tcp], Some(UnderlayProtocol::Tcp)),
        vec![tcp]
    );
    assert_eq!(
        relay_path_candidates_for_active_carrier(vec![tcp, udp], Some(UnderlayProtocol::Udp)),
        vec![udp]
    );
    assert_eq!(
        relay_path_candidates_for_active_carrier(vec![tcp, udp], None),
        vec![tcp, udp]
    );
}

fn opened_relay_stream_for_test(
    stream_id: StreamId,
    underlay: UnderlayProtocol,
    path_index: usize,
) -> (
    OpenedRemoteStream,
    TcpPathSessionCommandReceivers,
    mpsc::Sender<Result<Frame, RuntimeError>>,
) {
    let mux_limits = MuxLimits::default();
    let (commands, command_rx) = tcp_path_session_command_channels(4);
    let (frames_tx, frames_rx) = mpsc::channel(4);
    (
        OpenedRemoteStream {
            path_index,
            stream: TcpPathStream {
                stream_id,
                max_offset: mux_limits.max_stream_window_bytes,
                class: TrafficClass::Bulk,
                underlay,
                max_frame_payload_bytes: tcp_relay_buffer_len(mux_limits),
                output: TcpPathStreamOutput::Fixed(commands),
                frames: frames_rx,
            },
        },
        command_rx,
        frames_tx,
    )
}

#[tokio::test]
async fn mixed_relay_current_carrier_tracks_latest_data_path() {
    let (tcp_stream, _tcp_commands, _tcp_frames) =
        opened_relay_stream_for_test(StreamId(44), UnderlayProtocol::Tcp, 0);
    let mut remotes = TcpRelayRemoteSet::new(tcp_stream, 4);
    assert_eq!(
        remotes.active_carrier_underlay(),
        Some(UnderlayProtocol::Tcp)
    );

    let (udp_stream, _udp_commands, _udp_frames) =
        opened_relay_stream_for_test(StreamId(44), UnderlayProtocol::Udp, 1);
    remotes.attach(udp_stream);
    assert_eq!(
        remotes.active_carrier_underlay(),
        Some(UnderlayProtocol::Udp)
    );

    assert_eq!(
        relay_path_candidates_for_active_carrier(
            vec![
                RelayPathKey {
                    underlay: UnderlayProtocol::Tcp,
                    index: 0,
                },
                RelayPathKey {
                    underlay: UnderlayProtocol::Udp,
                    index: 2,
                },
            ],
            remotes.active_carrier_underlay(),
        ),
        vec![RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 2,
        }]
    );
}

#[tokio::test]
async fn repair_race_attach_preserves_active_data_path() {
    let stream_id = StreamId(46);
    let (active_stream, mut active_commands, _active_frames) =
        opened_relay_stream_for_test(stream_id, UnderlayProtocol::Udp, 0);
    let mut remotes = TcpRelayRemoteSet::new(active_stream, 4);
    let (repair_stream, mut repair_commands, _repair_frames) =
        opened_relay_stream_for_test(stream_id, UnderlayProtocol::Udp, 1);

    remotes.attach_for_repair(repair_stream);

    assert_eq!(
        remotes.active_path_index_for(UnderlayProtocol::Udp),
        Some(0)
    );
    let context = ClientPathContext::new(
        vec![
            "udp://127.0.0.1:11000".parse().expect("first path"),
            "udp://127.0.0.1:11001".parse().expect("second path"),
        ],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    remotes
        .send_frame(
            &context,
            Frame::StreamData {
                stream_id,
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"data"),
            },
        )
        .await
        .expect("send data");

    match recv_tcp_path_command(&mut active_commands)
        .await
        .expect("active command")
    {
        TcpPathSessionCommand::SendFrame(Frame::StreamData { payload, .. }) => {
            assert_eq!(&payload[..], b"data");
        }
        _ => panic!("expected data on active path"),
    }
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_tcp_path_command(&mut repair_commands)
        )
        .await
        .is_err(),
        "repair path should not become active for new data"
    );
}

#[tokio::test]
async fn delivered_repair_path_promotes_only_when_scheduler_score_improves() {
    let stream_id = StreamId(47);
    let context = ClientPathContext::new(
        vec![
            "udp://127.0.0.1:11010?srtt-ms=20&rate-mbps=120"
                .parse()
                .expect("fast path"),
            "udp://127.0.0.1:11011?srtt-ms=180&rate-mbps=30"
                .parse()
                .expect("slow path"),
        ],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    let (slow_active, mut slow_commands, _slow_frames) =
        opened_relay_stream_for_test(stream_id, UnderlayProtocol::Udp, 1);
    let mut remotes = TcpRelayRemoteSet::new(slow_active, 4);
    let (fast_repair, mut fast_commands, _fast_frames) =
        opened_relay_stream_for_test(stream_id, UnderlayProtocol::Udp, 0);
    remotes.attach_for_repair(fast_repair);
    let fast_instance = remotes
        .paths
        .iter()
        .find(|path| path.path_index == 0)
        .expect("fast repair path")
        .instance();

    assert!(tcp_relay_delivery_path_should_become_active(
        &context,
        remotes.active_path_key(),
        fast_instance.key,
        TrafficClass::Bulk,
        64 * 1024,
    ));
    assert!(remotes.promote_path_instance_to_active(fast_instance));
    assert_eq!(
        remotes.active_path_index_for(UnderlayProtocol::Udp),
        Some(0)
    );

    remotes
        .send_frame(
            &context,
            Frame::StreamData {
                stream_id,
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"bulk"),
            },
        )
        .await
        .expect("send data");
    match recv_tcp_path_command(&mut fast_commands)
        .await
        .expect("fast command")
    {
        TcpPathSessionCommand::SendFrame(Frame::StreamData { payload, .. }) => {
            assert_eq!(&payload[..], b"bulk");
        }
        _ => panic!("expected data on promoted fast path"),
    }
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_tcp_path_command(&mut slow_commands)
        )
        .await
        .is_err(),
        "slow active path should stop receiving new data after promotion"
    );

    let worse_path = RelayPathKey {
        underlay: UnderlayProtocol::Udp,
        index: 1,
    };
    assert!(!tcp_relay_delivery_path_should_become_active(
        &context,
        remotes.active_path_key(),
        worse_path,
        TrafficClass::Bulk,
        64 * 1024,
    ));
}

#[tokio::test]
async fn mixed_relay_path_status_active_replays_repair_cache_on_instance() {
    let mux_limits = MuxLimits::default();
    let stream_id = StreamId(45);
    let (commands, mut command_rx) = tcp_path_session_command_channels(4);
    let (_frames_tx, frames_rx) = mpsc::channel(4);
    let opened = OpenedRemoteStream {
        path_index: 1,
        stream: TcpPathStream {
            stream_id,
            max_offset: mux_limits.max_stream_window_bytes,
            class: TrafficClass::Bulk,
            underlay: UnderlayProtocol::Udp,
            max_frame_payload_bytes: tcp_relay_buffer_len(mux_limits),
            output: TcpPathStreamOutput::Fixed(commands),
            frames: frames_rx,
        },
    };
    let mut remotes = TcpRelayRemoteSet::new(opened, 4);
    let instance = remotes.active_path_instance().expect("active path");
    let mut send_stream = ReliableSendStream::new(stream_id, mux_limits);
    send_stream.update_max_offset(mux_limits.max_stream_window_bytes);
    send_stream
        .send_data(Bytes::from_static(b"repair"), StreamFlags::NONE)
        .expect("repair data");

    assert!(
        remotes
            .replay_repair_cache_to_instance(instance, &send_stream, false)
            .await
            .expect("replay")
    );

    match recv_tcp_path_command(&mut command_rx)
        .await
        .expect("replay command")
    {
        TcpPathSessionCommand::SendFrame(Frame::StreamData {
            stream_id: received_stream_id,
            offset,
            payload,
            ..
        }) => {
            assert_eq!(received_stream_id, stream_id);
            assert_eq!(offset, 0);
            assert_eq!(&payload[..], b"repair");
        }
        _ => panic!("expected replayed repair data"),
    }
}
