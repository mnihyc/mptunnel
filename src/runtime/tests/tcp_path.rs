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
    let first_single = single_path.ensure_session(FlowLane::Latency);
    let second_single = single_path.ensure_session(FlowLane::Latency);
    assert!(!first_single.same_channel(&second_single));

    let multipath = ClientTcpPathSessionHandle::new(test_tcp_session_runtime(true));
    let first_latency = multipath.ensure_session(FlowLane::Latency);
    let second_latency = multipath.ensure_session(FlowLane::Latency);
    let realtime_latency = multipath.ensure_session(FlowLane::RealtimeDatagram);
    assert!(first_latency.same_channel(&second_latency));
    assert!(first_latency.same_channel(&realtime_latency));

    let bulk = multipath.ensure_session(FlowLane::Throughput);
    assert!(!first_latency.same_channel(&bulk));
}

#[test]
fn mixed_reliable_underlays_share_one_logical_session() {
    let context = ClientPathContext::new(
        vec![
            "tcp://127.0.0.1:11080".parse().expect("tcp path"),
            "udp://127.0.0.1:11081".parse().expect("udp path"),
        ],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    assert_eq!(context.tcp_sessions.len(), 1);
    assert_eq!(context.udp_sessions.len(), 1);
    assert_eq!(
        context.tcp_sessions[0].session_id(),
        context.udp_sessions[0].session_id(),
        "TCP and UDP underlay paths must attach to the same reliable-stream session"
    );
}

#[test]
fn mixed_reliable_initial_open_uses_best_carrier_not_tcp_first() {
    let context = ClientPathContext::new(
        vec![
            "tcp://127.0.0.1:11082?srtt-ms=240&rate-mbps=50"
                .parse()
                .expect("slow tcp path"),
            "udp://127.0.0.1:11083?srtt-ms=30&rate-mbps=500"
                .parse()
                .expect("fast udp path"),
        ],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    let selected = context
        .reserve_reliable_stream_path(FlowLane::Latency, PATH_OPEN_SCORE_BYTES, &[])
        .expect("selected path");

    assert_eq!(selected.underlay, UnderlayProtocol::Udp);
    assert_eq!(selected.index, 0);
}

#[test]
fn mixed_reliable_latency_startup_ignores_udp_probe_only_sample() {
    let context = ClientPathContext::new(
        vec![
            "tcp://127.0.0.1:11086".parse().expect("tcp path"),
            "udp://127.0.0.1:11087".parse().expect("udp path"),
        ],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    context.mark_udp_path_probe_success(0, Duration::from_millis(1));

    let selected = context
        .reserve_reliable_stream_path(FlowLane::Latency, PATH_OPEN_SCORE_BYTES, &[])
        .expect("selected path");

    assert_eq!(selected.underlay, UnderlayProtocol::Tcp);
    assert_eq!(selected.index, 0);
}

#[test]
fn reliable_initial_open_allows_no_bulk_path_for_latency_lane() {
    let context = ClientPathContext::new(
        vec![
            "tcp://127.0.0.1:11084?srtt-ms=10&rate-mbps=1000&no-bulk"
                .parse()
                .expect("low latency no-bulk tcp path"),
            "tcp://127.0.0.1:11085?srtt-ms=120&rate-mbps=100"
                .parse()
                .expect("bulk tcp path"),
        ],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    let low_latency_score = scheduler::score_path(
        context.tcp_path_snapshot(0).expect("low-latency snapshot"),
        FlowLane::Latency,
        PATH_OPEN_SCORE_BYTES,
        SchedulerPolicy::default(),
    )
    .expect("low-latency score")
    .eta_ms;
    let bulk_score = scheduler::score_path(
        context.tcp_path_snapshot(1).expect("bulk snapshot"),
        FlowLane::Latency,
        PATH_OPEN_SCORE_BYTES,
        SchedulerPolicy::default(),
    )
    .expect("bulk score")
    .eta_ms;
    assert!(
        low_latency_score < bulk_score,
        "low-latency no-bulk path should score ahead for latency: low={low_latency_score} bulk={bulk_score}"
    );

    let selected = context
        .reserve_reliable_stream_path(FlowLane::Latency, PATH_OPEN_SCORE_BYTES, &[])
        .expect("selected path");

    assert_eq!(selected.underlay, UnderlayProtocol::Tcp);
    assert_eq!(selected.index, 0);
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
        FlowLane::Throughput,
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
        FlowLane::Throughput,
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
            FlowLane::Latency,
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
async fn tcp_path_stream_fin_bypasses_saturated_bulk_queue() {
    let (tx, mut rx) = tcp_path_session_command_channels(1);
    tx.send_frame(
        Frame::StreamData {
            stream_id: StreamId(30),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(b"bulk"),
        },
        FlowLane::Throughput,
    )
    .await
    .expect("fill bulk data queue");

    tokio::time::timeout(
        Duration::from_millis(50),
        tx.send_frame(
            Frame::StreamFin {
                stream_id: StreamId(30),
                final_offset: 4,
            },
            FlowLane::Throughput,
        ),
    )
    .await
    .expect("FIN should not wait behind bulk data")
    .expect("queue FIN");

    match recv_tcp_path_command(&mut rx).await.expect("first command") {
        TcpPathSessionCommand::SendFrame(Frame::StreamFin {
            stream_id,
            final_offset,
        }) => {
            assert_eq!(stream_id, StreamId(30));
            assert_eq!(final_offset, 4);
        }
        _ => panic!("expected prioritized stream FIN"),
    }
}

#[tokio::test]
async fn tcp_path_command_queue_tracks_pending_frame_bytes() {
    let (tx, mut rx) = tcp_path_session_command_channels(2);
    let frame = Frame::StreamData {
        stream_id: StreamId(13),
        offset: 0,
        flags: StreamFlags::NONE,
        payload: Bytes::from_static(b"queued-bytes"),
    };
    let expected = frame_pacing_bytes(&frame) as u64;

    tx.send_frame(frame, FlowLane::Throughput)
        .await
        .expect("queue frame");
    assert_eq!(tx.pending_bytes(), expected);

    let command = recv_tcp_path_command(&mut rx)
        .await
        .expect("queued command");
    assert!(matches!(
        command,
        TcpPathSessionCommand::SendFrame(Frame::StreamData { .. })
    ));
    assert_eq!(
        tx.pending_bytes(),
        expected,
        "dequeue alone must not hide writer backlog from admission"
    );
    rx.release_pending_command_bytes(tcp_path_command_pending_bytes(&command));
    assert_eq!(tx.pending_bytes(), 0);
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
        FlowLane::Throughput,
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
            complete: true,
            ranges: Vec::new(),
        },
    )
    .await
    .expect("late frame for closed stream should be ignored");

    let unknown = StreamId(99);
    route_client_tcp_stream_frame(
        &mut streams,
        &mut closed_streams,
        unknown,
        Frame::StreamFin {
            stream_id: unknown,
            final_offset: 0,
        },
    )
    .await
    .expect("unknown product stream frame should be dropped at product layer");
    assert!(closed_streams.contains(&unknown));
}

#[tokio::test]
async fn server_tcp_registry_ignores_late_frames_for_recently_closed_stream() {
    let registry = ServerReliableStreamRegistry::new(8);
    let session_id = SessionId(11);
    let stream_id = StreamId(5);
    let (commands, _receivers) = tcp_path_session_command_channels(4);
    let target = TargetAddr::Domain {
        host: "example.com".to_string(),
        port: 443,
    };

    let opened = registry
        .open_or_attach(
            ServerReliableStreamOpenRequest {
                session_id,
                stream_id,
                target: &target,
                lane: FlowLane::Latency,
                attachment: ServerReliablePathAttachment {
                    path_id: PathId(0),
                    underlay: UnderlayProtocol::Tcp,
                    commands,
                    max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
                    role: StreamOpenRole::Active,
                },
            },
            MuxLimits::default(),
            8,
        )
        .expect("server stream open");
    assert!(matches!(opened, ServerReliableStreamOpen::New(_)));

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
    registry
        .route_frame(
            session_id,
            unknown,
            Frame::StreamFin {
                stream_id: unknown,
                final_offset: 0,
            },
        )
        .await
        .expect("unknown server product stream frame should be dropped");
}

#[tokio::test]
async fn server_reliable_relay_does_not_replay_whole_repair_cache_on_path_reattach() {
    let mux_limits = MuxLimits::default();
    let stream_id = StreamId(42);
    let (mut target_peer, target_side) = duplex(4096);
    let (commands_tx, mut commands_rx) = tcp_path_session_command_channels(8);
    let (frames_tx, frames_rx) = mpsc::channel(8);
    let relay = tokio::spawn(relay_reliable_stream(
        target_side,
        ReliablePathStream {
            stream_id,
            max_offset: mux_limits.max_stream_window_bytes,
            lane: FlowLane::Latency,
            underlay: UnderlayProtocol::Tcp,
            max_frame_payload_bytes: reliable_relay_buffer_len(mux_limits),
            output: ReliablePathStreamOutput::Fixed(commands_tx),
            frames: frames_rx,
        },
        mux_limits,
        MppPerformanceConfig::default(),
        SessionId(1),
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
    let no_replay = tokio::time::timeout(
        Duration::from_millis(100),
        recv_tcp_path_command(&mut commands_rx),
    )
    .await;
    assert!(
        no_replay.is_err(),
        "reattach without ACK gap must not emit whole-cache repair data"
    );

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
            .ordered_tcp_path_indices(FlowLane::Latency, 512)
            .first()
            .copied(),
        Some(0)
    );
    context.mark_tcp_path_failure(0);
    let suspect_order = context.ordered_tcp_path_indices(FlowLane::Latency, 512);
    assert_eq!(suspect_order, vec![0, 1]);
    context.mark_tcp_path_failure(0);
    let failed_order = context.ordered_tcp_path_indices(FlowLane::Latency, 512);
    assert_eq!(failed_order, vec![1]);

    {
        let mut health = context.health.lock().expect("health lock");
        health.tcp[0].failed_until = Some(Instant::now() - Duration::from_millis(1));
    }
    let recovered_order = context.ordered_tcp_path_indices(FlowLane::Latency, 512);
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
        tcp_context.ordered_tcp_path_indices(FlowLane::Latency, 512),
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
        udp_stream_path_indices(&udp_context, FlowLane::Latency, 512),
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

    context.mark_tcp_path_probe_success(0, Duration::from_millis(120));
    context.mark_tcp_path_probe_success(1, Duration::from_millis(5));

    assert_eq!(
        context
            .ordered_tcp_path_indices(FlowLane::Latency, 512)
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
            .ordered_tcp_path_indices(FlowLane::Throughput, 4 * 1024 * 1024)
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
            .ordered_tcp_path_indices(FlowLane::Throughput, 4 * 1024 * 1024)
            .first()
            .copied(),
        Some(0)
    );
}

#[test]
fn bulk_striping_orders_paths_by_bulk_eta() {
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
        tcp_bulk_striping_indices(
            &context,
            MuxLimits::default().max_reliable_relay_chunk_bytes
        )
        .first()
        .copied(),
        Some(1)
    );
}

#[test]
fn bulk_striping_uses_service_horizon_for_realistic_fat_path() {
    let low_latency_path = "tcp://127.0.0.1:10019?srtt-ms=20&rate-mbps=80&low-latency=true"
        .parse::<PathSpec>()
        .expect("low-latency path");
    let high_bandwidth_path = "tcp://127.0.0.1:10020?srtt-ms=180&rate-mbps=500"
        .parse::<PathSpec>()
        .expect("high-bandwidth path");
    let context = ClientPathContext::new(
        vec![low_latency_path, high_bandwidth_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    assert_eq!(
        tcp_bulk_striping_indices(&context, 64 * 1024)
            .first()
            .copied(),
        Some(1),
        "sustained bulk must score against a service horizon, not the next tiny frame"
    );
}

#[test]
fn bulk_striping_skips_expensive_path() {
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

    assert_eq!(
        tcp_bulk_striping_indices(
            &context,
            MuxLimits::default().max_reliable_relay_chunk_bytes
        ),
        vec![0]
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
            .ordered_tcp_repair_path_indices(Some(0), FlowLane::Throughput, 4 * 1024 * 1024)
            .is_empty()
    );
    assert_eq!(
        context.ordered_tcp_repair_path_indices(Some(1), FlowLane::Throughput, 4 * 1024 * 1024),
        vec![0]
    );
    assert_eq!(
        context
            .ordered_tcp_repair_path_indices(Some(0), FlowLane::Latency, 512)
            .first()
            .copied(),
        Some(0)
    );
}

#[test]
fn data_plane_failure_cools_path_for_active_reopen() {
    let failed_path = "tcp://127.0.0.1:10130?srtt-ms=20&rate-mbps=100"
        .parse::<PathSpec>()
        .expect("failed path");
    let survivor_path = "tcp://127.0.0.1:10131?srtt-ms=80&rate-mbps=100"
        .parse::<PathSpec>()
        .expect("survivor path");
    let context = ClientPathContext::new(
        vec![failed_path, survivor_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.mark_relay_path_data_plane_failure(UnderlayProtocol::Tcp, 0);

    assert_eq!(
        context.ordered_tcp_path_indices(FlowLane::Throughput, 64 * 1024),
        vec![1]
    );
}

#[test]
fn data_plane_failure_requires_probe_success_before_bulk_readmission() {
    let failed_fast_path = "tcp://127.0.0.1:10130?srtt-ms=20&rate-mbps=500"
        .parse::<PathSpec>()
        .expect("failed fast path");
    let survivor_path = "tcp://127.0.0.1:10131?srtt-ms=80&rate-mbps=100"
        .parse::<PathSpec>()
        .expect("survivor path");
    let context = ClientPathContext::new(
        vec![failed_fast_path, survivor_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.mark_relay_path_data_plane_failure(UnderlayProtocol::Tcp, 0);
    {
        let mut health = context.health.lock().expect("path health");
        health.tcp[0].failed_until = Some(Instant::now() - Duration::from_millis(1));
    }

    assert_eq!(
        context.ordered_reliable_bulk_striping_path_keys(64 * 1024),
        vec![RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 1,
        }]
    );
    assert_eq!(
        context.ordered_tcp_repair_path_indices(None, FlowLane::Throughput, 64 * 1024),
        vec![1]
    );

    context.mark_tcp_path_probe_success(0, Duration::from_millis(20));

    assert_eq!(
        context
            .ordered_reliable_bulk_striping_path_keys(64 * 1024)
            .first()
            .copied(),
        Some(RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        })
    );
}

#[test]
fn tcp_attach_open_timeout_uses_active_data_plane_budget() {
    let low_latency_path = "tcp://127.0.0.1:10130?srtt-ms=20&rate-mbps=100"
        .parse::<PathSpec>()
        .expect("low latency path");
    let high_rtt_path = "tcp://127.0.0.1:10131?srtt-ms=900&jitter-ms=400&rate-mbps=500"
        .parse::<PathSpec>()
        .expect("high rtt path");
    let context = ClientPathContext::new(
        vec![low_latency_path, high_rtt_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    assert_eq!(
        reliable_relay_attach_open_timeout(
            &context,
            RelayPathKey {
                underlay: UnderlayProtocol::Tcp,
                index: 0,
            },
            FlowLane::Latency,
        ),
        TCP_STREAM_STALL_MIN_TIMEOUT
    );
    assert!(
        reliable_relay_attach_open_timeout(
            &context,
            RelayPathKey {
                underlay: UnderlayProtocol::Tcp,
                index: 1,
            },
            FlowLane::Throughput,
        ) <= TCP_STREAM_STALL_MAX_TIMEOUT
    );
}

#[test]
fn path_open_timeout_is_a_migratable_retryable_path_failure() {
    let err = RuntimeError::PathOpenTimedOut;
    assert!(stream_open_error_is_path_retryable(&err));
    assert!(reliable_relay_error_is_migratable(&err));
    assert!(relay_error_is_tcp_path_failure::<()>(&Err(err)));
}

#[test]
fn endpoint_only_tcp_bulk_striping_admits_only_best_unmeasured_path() {
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

    assert_eq!(
        tcp_bulk_striping_indices(
            &context,
            MuxLimits::default().max_reliable_relay_chunk_bytes
        ),
        vec![0]
    );
}

#[test]
fn endpoint_only_tcp_bulk_striping_validates_unmeasured_paths_after_bulk_promotion() {
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

    context.mark_tcp_path_open_success(0, Duration::from_millis(20), FlowLane::Latency);
    context.change_relay_path_lane_load(
        UnderlayProtocol::Tcp,
        0,
        FlowLane::Latency,
        FlowLane::Throughput,
    );

    assert_eq!(
        tcp_bulk_striping_indices(
            &context,
            MuxLimits::default().max_reliable_relay_chunk_bytes
        ),
        vec![0]
    );
    assert_eq!(
        context
            .ordered_reliable_bulk_validation_path_keys(reliable_relay_buffer_len(
                MuxLimits::default()
            ))
            .into_iter()
            .map(|key| key.index)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
}

#[test]
fn mixed_bulk_validation_prioritizes_udp_path_scoped_proof() {
    let tcp_active = "tcp://127.0.0.1:10172"
        .parse::<PathSpec>()
        .expect("active tcp path");
    let tcp_probe = "tcp://127.0.0.1:10173"
        .parse::<PathSpec>()
        .expect("probe tcp path");
    let udp_probe = "udp://127.0.0.1:10174"
        .parse::<PathSpec>()
        .expect("probe udp path");
    let context = ClientPathContext::new(
        vec![tcp_active, tcp_probe, udp_probe],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.mark_tcp_path_open_success(0, Duration::from_millis(20), FlowLane::Latency);
    context.change_relay_path_lane_load(
        UnderlayProtocol::Tcp,
        0,
        FlowLane::Latency,
        FlowLane::Throughput,
    );

    let candidates = context.ordered_reliable_bulk_validation_path_keys(reliable_relay_buffer_len(
        MuxLimits::default(),
    ));

    assert_eq!(
        candidates
            .into_iter()
            .map(|key| (key.underlay, key.index))
            .collect::<Vec<_>>(),
        vec![(UnderlayProtocol::Udp, 0), (UnderlayProtocol::Tcp, 1)]
    );
}

#[test]
fn bulk_promotion_changes_active_path_lane_load_without_leaking_flow_accounting() {
    let path = "tcp://127.0.0.1:10165".parse::<PathSpec>().expect("path");
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("context");

    context.mark_tcp_path_open_success(0, Duration::from_millis(20), FlowLane::Latency);
    {
        let health = context.health.lock().expect("client path health lock");
        assert_eq!(health.tcp[0].active_flows, 1);
        assert_eq!(health.tcp[0].active_latency_sensitive_flows, 1);
        assert_eq!(health.tcp[0].relay_bytes_in_flight, 0);
    }

    context.change_relay_path_lane_load(
        UnderlayProtocol::Tcp,
        0,
        FlowLane::Latency,
        FlowLane::Throughput,
    );
    {
        let health = context.health.lock().expect("client path health lock");
        assert_eq!(health.tcp[0].active_flows, 1);
        assert_eq!(health.tcp[0].active_latency_sensitive_flows, 0);
        assert_eq!(health.tcp[0].relay_bytes_in_flight, 0);
    }

    context.release_relay_path_load(UnderlayProtocol::Tcp, 0, FlowLane::Throughput);
    let health = context.health.lock().expect("client path health lock");
    assert_eq!(health.tcp[0].active_flows, 0);
    assert_eq!(health.tcp[0].active_latency_sensitive_flows, 0);
    assert_eq!(health.tcp[0].relay_bytes_in_flight, 0);
}

#[test]
fn endpoint_only_tcp_bulk_striping_keeps_unknown_paths_out_of_measured_bulk_cohort() {
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

    context.mark_tcp_path_open_success(0, Duration::from_millis(20), FlowLane::Latency);
    assert_eq!(
        tcp_bulk_striping_indices(
            &context,
            MuxLimits::default().max_reliable_relay_chunk_bytes
        ),
        vec![0]
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
        udp_stream_path_indices(&context, FlowLane::Latency, PATH_OPEN_SCORE_BYTES),
        vec![0, 1]
    );

    context.mark_udp_path_failure(0);
    assert_eq!(
        udp_stream_path_indices(&context, FlowLane::Latency, PATH_OPEN_SCORE_BYTES),
        vec![1]
    );
}

#[test]
fn endpoint_only_udp_stream_bulk_striping_admits_only_best_unmeasured_path() {
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

    assert_eq!(
        reliable_bulk_striping_path_keys(
            &context,
            MuxLimits::default().max_reliable_relay_chunk_bytes
        )
        .into_iter()
        .filter_map(|key| (key.underlay == UnderlayProtocol::Udp).then_some(key.index))
        .collect::<Vec<_>>(),
        vec![0]
    );
}

#[test]
fn udp_bulk_repair_requires_delivery_evidence_when_stream_has_active_path() {
    let active_path = "udp://127.0.0.1:10140"
        .parse::<PathSpec>()
        .expect("active path");
    let probe_only_path = "udp://127.0.0.1:10141"
        .parse::<PathSpec>()
        .expect("probe path");
    let context = ClientPathContext::new(
        vec![active_path, probe_only_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.mark_udp_path_probe_success(0, Duration::from_millis(20));
    context.mark_udp_path_probe_success(1, Duration::from_millis(25));

    assert!(
        context
            .ordered_udp_stream_repair_path_indices(Some(0), FlowLane::Throughput, 64 * 1024, true)
            .is_empty()
    );

    context.mark_udp_path_delivery(
        1,
        PathDeliveryStats {
            payload_bytes: 1024 * 1024,
            first_payload_at: Some(Instant::now() - Duration::from_millis(80)),
            last_payload_at: Some(Instant::now()),
        },
    );

    assert_eq!(
        context.ordered_udp_stream_repair_path_indices(
            Some(0),
            FlowLane::Throughput,
            64 * 1024,
            true
        ),
        vec![1]
    );
}

#[test]
fn mixed_endpoint_only_bulk_striping_admits_only_best_unmeasured_path() {
    let tcp_path = "tcp://127.0.0.1:10136"
        .parse::<PathSpec>()
        .expect("tcp path");
    let udp_path = "udp://127.0.0.1:10137"
        .parse::<PathSpec>()
        .expect("udp path");
    let context = ClientPathContext::new(
        vec![tcp_path, udp_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    assert_eq!(
        reliable_bulk_striping_path_keys(
            &context,
            MuxLimits::default().max_reliable_relay_chunk_bytes
        ),
        vec![RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        }]
    );
}

#[test]
fn mixed_endpoint_only_bulk_striping_keeps_udp_eligible_under_tcp_pressure() {
    let tcp_path = "tcp://127.0.0.1:10136"
        .parse::<PathSpec>()
        .expect("tcp path");
    let udp_path = "udp://127.0.0.1:10137"
        .parse::<PathSpec>()
        .expect("udp path");
    let context = ClientPathContext::new(
        vec![tcp_path, udp_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    context.reserve_tcp_path_load(0, FlowLane::Latency);

    assert_eq!(
        reliable_bulk_striping_path_keys(
            &context,
            MuxLimits::default().max_reliable_relay_chunk_bytes
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
fn mixed_endpoint_only_bulk_striping_keeps_measured_udp_without_unmeasured_tcp() {
    let tcp_path = "tcp://127.0.0.1:10136"
        .parse::<PathSpec>()
        .expect("tcp path");
    let udp_path = "udp://127.0.0.1:10137"
        .parse::<PathSpec>()
        .expect("udp path");
    let context = ClientPathContext::new(
        vec![tcp_path, udp_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    context.mark_udp_path_probe_success(0, Duration::from_millis(1));
    context.reserve_udp_stream_path_load(0, FlowLane::RealtimeDatagram);

    assert_eq!(
        reliable_bulk_striping_path_keys(
            &context,
            MuxLimits::default().max_reliable_relay_chunk_bytes
        ),
        vec![RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        }]
    );
}

#[test]
fn mixed_udp_repair_keeps_healthy_udp_eligible_on_active_tcp_stream() {
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
                FlowLane::Throughput,
                MuxLimits::default().max_reliable_relay_chunk_bytes,
                true,
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
        context.ordered_udp_stream_repair_path_indices(
            None,
            FlowLane::Throughput,
            MuxLimits::default().max_reliable_relay_chunk_bytes,
            true,
        ),
        vec![1]
    );
}

#[test]
fn udp_repair_keeps_healthy_endpoint_only_path_eligible() {
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
                FlowLane::Throughput,
                MuxLimits::default().max_reliable_relay_chunk_bytes,
                true,
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
        context.ordered_udp_stream_repair_path_indices(
            Some(0),
            FlowLane::Throughput,
            MuxLimits::default().max_reliable_relay_chunk_bytes,
            true,
        ),
        vec![1]
    );
}

#[test]
fn mixed_bulk_striping_orders_better_udp_before_tcp() {
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
        reliable_bulk_striping_path_keys(
            &context,
            MuxLimits::default().max_reliable_relay_chunk_bytes
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
fn mixed_bulk_striping_suppresses_catastrophically_worse_udp_candidate() {
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
        reliable_bulk_striping_path_keys(
            &context,
            MuxLimits::default().max_reliable_relay_chunk_bytes
        ),
        vec![RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        }]
    );
}

#[test]
fn mixed_bulk_striping_penalizes_lossy_high_rtt_udp_carrier() {
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
            loss_rate: 0.15,
            rate_sample: PathRateSample::new(4 * 1024 * 1024, Duration::from_millis(170)),
        },
    );

    assert_eq!(
        reliable_bulk_striping_path_keys(
            &context,
            MuxLimits::default().max_reliable_relay_chunk_bytes
        )
        .first()
        .copied(),
        Some(RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        })
    );
}

#[test]
fn mixed_bulk_striping_can_choose_best_carrier_without_active_cohort() {
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
            .ordered_reliable_bulk_striping_path_keys(
                MuxLimits::default().max_reliable_relay_chunk_bytes
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
            stream: ReliablePathStream {
                stream_id,
                max_offset: mux_limits.max_stream_window_bytes,
                lane: FlowLane::Throughput,
                underlay,
                max_frame_payload_bytes: reliable_relay_buffer_len(mux_limits),
                output: ReliablePathStreamOutput::Fixed(commands),
                frames: frames_rx,
            },
        },
        command_rx,
        frames_tx,
    )
}

async fn send_relay_stream_frame_for_test(
    sender: &mut RelaySenderService,
    context: &ClientPathContext,
    remotes: &mut ReliableRelayRemoteSet,
    frame: Frame,
) -> Result<RelaySendOutcome, RuntimeError> {
    sender.send_stream_data(context, remotes, frame).await
}

#[tokio::test]
async fn mixed_relay_current_carrier_tracks_latest_data_path() {
    let (tcp_stream, _tcp_commands, _tcp_frames) =
        opened_relay_stream_for_test(StreamId(44), UnderlayProtocol::Tcp, 0);
    let mut remotes = ReliableRelayRemoteSet::new(tcp_stream, 4);
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
    let mut remotes = ReliableRelayRemoteSet::new(active_stream, 4);
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
    let mut sender = RelaySenderService::new(stream_id);
    send_relay_stream_frame_for_test(
        &mut sender,
        &context,
        &mut remotes,
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
async fn bulk_relay_keeps_normal_stream_data_on_active_path() {
    let stream_id = StreamId(146);
    let (active_stream, mut active_commands, _active_frames) =
        opened_relay_stream_for_test(stream_id, UnderlayProtocol::Tcp, 0);
    let mut remotes = ReliableRelayRemoteSet::new(active_stream, 8);
    let (repair_stream, mut repair_commands, _repair_frames) =
        opened_relay_stream_for_test(stream_id, UnderlayProtocol::Tcp, 1);
    remotes.attach_for_repair(repair_stream);
    let context = ClientPathContext::new(
        vec![
            "tcp://127.0.0.1:11146?srtt-ms=40&rate-mbps=100"
                .parse()
                .expect("active path"),
            "tcp://127.0.0.1:11147?srtt-ms=40&rate-mbps=100"
                .parse()
                .expect("repair path"),
        ],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    let mut sender = RelaySenderService::new(stream_id);
    let first_payload = Bytes::from(vec![1u8; 64 * 1024]);
    send_relay_stream_frame_for_test(
        &mut sender,
        &context,
        &mut remotes,
        Frame::StreamData {
            stream_id,
            offset: 0,
            flags: StreamFlags::NONE,
            payload: first_payload,
        },
    )
    .await
    .expect("first send");
    match recv_tcp_path_command(&mut active_commands)
        .await
        .expect("active path first command")
    {
        TcpPathSessionCommand::SendFrame(Frame::StreamData { offset, .. }) => {
            assert_eq!(offset, 0);
        }
        _ => panic!("expected first bulk chunk on active path"),
    }

    let second_payload = Bytes::from(vec![2u8; 64 * 1024]);
    send_relay_stream_frame_for_test(
        &mut sender,
        &context,
        &mut remotes,
        Frame::StreamData {
            stream_id,
            offset: 64 * 1024,
            flags: StreamFlags::NONE,
            payload: second_payload,
        },
    )
    .await
    .expect("second send");
    match recv_tcp_path_command(&mut active_commands)
        .await
        .expect("active path second command")
    {
        TcpPathSessionCommand::SendFrame(Frame::StreamData { offset, .. }) => {
            assert_eq!(offset, 64 * 1024);
        }
        _ => panic!("expected second bulk chunk on active path"),
    }
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_tcp_path_command(&mut repair_commands)
        )
        .await
        .is_err(),
        "repair attachment must not receive ordinary bulk data"
    );
}

#[tokio::test]
async fn bulk_relay_validation_path_can_receive_admitted_probe_data() {
    let stream_id = StreamId(149);
    let (active_stream, mut active_commands, _active_frames) =
        opened_relay_stream_for_test(stream_id, UnderlayProtocol::Tcp, 0);
    let mut remotes = ReliableRelayRemoteSet::new(active_stream, 8);
    let (validation_stream, mut validation_commands, _validation_frames) =
        opened_relay_stream_for_test(stream_id, UnderlayProtocol::Tcp, 1);
    remotes.attach_for_validation(validation_stream);
    let context = ClientPathContext::new(
        vec![
            "tcp://127.0.0.1:11160?srtt-ms=180&rate-mbps=40"
                .parse()
                .expect("slower active path"),
            "tcp://127.0.0.1:11161?srtt-ms=20&rate-mbps=200"
                .parse()
                .expect("validation path"),
        ],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    context.mark_tcp_path_open_success(0, Duration::from_millis(180), FlowLane::Throughput);
    context.mark_tcp_path_open_success(1, Duration::from_millis(20), FlowLane::Throughput);

    let mut sender = RelaySenderService::new(stream_id);
    send_relay_stream_frame_for_test(
        &mut sender,
        &context,
        &mut remotes,
        Frame::StreamData {
            stream_id,
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![8u8; 64 * 1024]),
        },
    )
    .await
    .expect("send admitted validation bulk");

    match tokio::time::timeout(
        Duration::from_millis(100),
        recv_tcp_path_command(&mut validation_commands),
    )
    .await
    .expect("validation path timeout")
    .expect("validation path command")
    {
        TcpPathSessionCommand::SendFrame(Frame::StreamData { offset, .. }) => {
            assert_eq!(offset, 0);
        }
        _ => panic!("expected validation path to receive admitted probe data"),
    }
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_tcp_path_command(&mut active_commands)
        )
        .await
        .is_err(),
        "slower active path should not keep validation probe data"
    );
}

#[tokio::test]
async fn bulk_relay_uses_measured_tcp_peer_when_ecf_prefers_it() {
    let stream_id = StreamId(148);
    let (best_stream, mut best_commands, _best_frames) =
        opened_relay_stream_for_test(stream_id, UnderlayProtocol::Tcp, 0);
    let mut remotes = ReliableRelayRemoteSet::new(best_stream, 8);
    let (slow_active_stream, mut slow_active_commands, _slow_active_frames) =
        opened_relay_stream_for_test(stream_id, UnderlayProtocol::Tcp, 1);
    remotes.attach(slow_active_stream);
    let context = ClientPathContext::new(
        vec![
            "tcp://127.0.0.1:11158?srtt-ms=20&rate-mbps=200"
                .parse()
                .expect("better path"),
            "tcp://127.0.0.1:11159?srtt-ms=180&rate-mbps=40"
                .parse()
                .expect("uncompetitive active path"),
        ],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    context.mark_tcp_path_open_success(0, Duration::from_millis(20), FlowLane::Throughput);
    context.mark_tcp_path_open_success(1, Duration::from_millis(180), FlowLane::Throughput);

    let mut sender = RelaySenderService::new(stream_id);
    send_relay_stream_frame_for_test(
        &mut sender,
        &context,
        &mut remotes,
        Frame::StreamData {
            stream_id,
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![4u8; 64 * 1024]),
        },
    )
    .await
    .expect("send bulk through ECF-gated peer path");

    match tokio::time::timeout(
        Duration::from_millis(100),
        recv_tcp_path_command(&mut best_commands),
    )
    .await
    .expect("best path timeout")
    .expect("best path command")
    {
        TcpPathSessionCommand::SendFrame(Frame::StreamData { offset, .. }) => {
            assert_eq!(offset, 0);
        }
        _ => panic!("expected measured better path to carry admitted TCP bulk data"),
    }
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_tcp_path_command(&mut slow_active_commands)
        )
        .await
        .is_err(),
        "uncompetitive active path should not keep bulk when ECF admits a better path"
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
    let mut remotes = ReliableRelayRemoteSet::new(slow_active, 4);
    let (fast_repair, mut fast_commands, _fast_frames) =
        opened_relay_stream_for_test(stream_id, UnderlayProtocol::Udp, 0);
    remotes.attach_for_repair(fast_repair);
    let fast_instance = remotes
        .paths
        .iter()
        .find(|path| path.path_index == 0)
        .expect("fast repair path")
        .instance();

    assert!(
        !reliable_relay_delivery_path_should_become_active(
            &context,
            remotes.active_path_key(),
            fast_instance.key,
            FlowLane::Throughput,
            64 * 1024,
        ),
        "bulk promotion needs measured delivery, not only path hints"
    );
    context.mark_relay_path_rate_sample(
        UnderlayProtocol::Udp,
        0,
        PathRateSample::new(512 * 1024, Duration::from_millis(50)).expect("rate sample"),
    );
    assert!(reliable_relay_delivery_path_should_become_active(
        &context,
        remotes.active_path_key(),
        fast_instance.key,
        FlowLane::Throughput,
        64 * 1024,
    ));
    assert!(remotes.promote_path_instance_to_active(fast_instance));
    assert_eq!(
        remotes.active_path_index_for(UnderlayProtocol::Udp),
        Some(0)
    );

    let mut sender = RelaySenderService::new(stream_id);
    send_relay_stream_frame_for_test(
        &mut sender,
        &context,
        &mut remotes,
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
    assert!(!reliable_relay_delivery_path_should_become_active(
        &context,
        remotes.active_path_key(),
        worse_path,
        FlowLane::Throughput,
        64 * 1024,
    ));
}

#[tokio::test]
async fn mixed_relay_path_status_active_does_not_replay_whole_repair_cache_on_instance() {
    let mux_limits = MuxLimits::default();
    let stream_id = StreamId(45);
    let (commands, mut command_rx) = tcp_path_session_command_channels(4);
    let (_frames_tx, frames_rx) = mpsc::channel(4);
    let opened = OpenedRemoteStream {
        path_index: 1,
        stream: ReliablePathStream {
            stream_id,
            max_offset: mux_limits.max_stream_window_bytes,
            lane: FlowLane::Throughput,
            underlay: UnderlayProtocol::Udp,
            max_frame_payload_bytes: reliable_relay_buffer_len(mux_limits),
            output: ReliablePathStreamOutput::Fixed(commands),
            frames: frames_rx,
        },
    };
    let mut remotes = ReliableRelayRemoteSet::new(opened, 4);
    let instance = remotes.active_path_instance().expect("active path");
    let mut send_stream = ReliableSendStream::new(stream_id, mux_limits);
    send_stream.update_max_offset(mux_limits.max_stream_window_bytes);
    send_stream
        .send_data(Bytes::from_static(b"repair"), StreamFlags::NONE)
        .expect("repair data");
    let mut sender = RelaySenderService::new(stream_id);

    assert!(
        !sender
            .send_attach_control_to_instance(&mut remotes, instance, &send_stream, false)
            .await
            .expect("repair replay decision"),
        "reattach without an explicit ACK gap must not emit whole-cache repair data"
    );
    assert!(
        tokio::time::timeout(
            Duration::from_millis(100),
            recv_tcp_path_command(&mut command_rx)
        )
        .await
        .is_err(),
        "repair emission must be gap-targeted instead of whole-cache"
    );
}
