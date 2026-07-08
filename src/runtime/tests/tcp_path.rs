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
        connect_timeout: crate::config::DEFAULT_PATH_PROBE_TIMEOUT,
        health: Arc::new(Mutex::new(ClientPathHealth {
            tcp: vec![ClientPathHealthRecord::default()],
            udp: Vec::new(),
        })),
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

#[tokio::test]
async fn tcp_client_sends_path_metrics_before_open_stream() {
    let path = reserve_tcp_path_with_query("srtt-ms=20&rate-mbps=500").await;
    let listener = bind_listener(&path).await.expect("bind");
    let server_path = path.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let security = security();
        let mut framed = EncryptedFramedStream::with_cipher_suite(
            stream,
            security.secret.as_bytes(),
            PeerRole::Server,
            CodecLimits::default(),
            security.cipher,
        );
        let session_id = match framed.read_frame().await? {
            Frame::SessionHello { session_id } => session_id,
            _ => return Err(RuntimeError::Protocol("expected SESSION_HELLO")),
        };
        let authenticator = SessionAuthenticator::new(security.secret.as_bytes())?;
        match framed.read_frame().await? {
            Frame::SessionAuth {
                session_id: auth_session_id,
                nonce,
                issued_at_unix_secs,
                auth_tag,
            } if auth_session_id == session_id
                && authenticator.verify_session_auth(SessionAuthCheck {
                    session_id,
                    nonce,
                    issued_at_unix_secs,
                    tag: auth_tag,
                    now_unix_secs: current_unix_secs()?,
                    freshness_window_secs: security.auth_freshness_window.as_secs(),
                }) => {}
            _ => return Err(RuntimeError::Protocol("invalid SESSION_AUTH")),
        }
        let (path_id, capabilities) = match framed.read_frame().await? {
            Frame::PathJoin {
                session_id: join_session_id,
                path_id,
                underlay,
                nonce,
                issued_at_unix_secs,
                capabilities,
                auth_tag,
            } if join_session_id == session_id
                && underlay == UnderlayProtocol::Tcp
                && authenticator.verify_path_join(PathJoinAuthCheck {
                    session_id,
                    path_id,
                    underlay,
                    nonce,
                    issued_at_unix_secs,
                    capabilities,
                    tag: auth_tag,
                    now_unix_secs: current_unix_secs()?,
                    freshness_window_secs: security.auth_freshness_window.as_secs(),
                }) =>
            {
                (path_id, capabilities)
            }
            _ => return Err(RuntimeError::Protocol("invalid PATH_JOIN")),
        };
        framed.write_frame(&Frame::SessionReady).await?;
        framed
            .write_frame(&Frame::PathStatus {
                path_id,
                status: crate::protocol::PathStatus::Active,
                capabilities,
            })
            .await?;
        framed.flush().await?;

        match framed.read_frame().await? {
            Frame::PathMetrics { metrics } => {
                assert_eq!(metrics.path_id, path_id);
                assert_eq!(metrics.underlay, UnderlayProtocol::Tcp);
                assert_eq!(metrics.direction, PathMetricDirection::ClientToServer);
                assert!(
                    metrics.delivery_rate_bps >= 500_000_000,
                    "startup metrics must carry configured path evidence before OPEN_STREAM"
                );
            }
            other => panic!("expected PATH_METRICS before OPEN_STREAM, got {other:?}"),
        }

        match framed.read_frame().await? {
            Frame::OpenStream { stream_id, .. } => {
                framed
                    .write_frame(&Frame::StreamMaxData {
                        stream_id,
                        max_offset: ResourceLimits::default().max_stream_window_bytes,
                    })
                    .await?;
                framed.flush().await?;
            }
            other => panic!("expected OPEN_STREAM after PATH_METRICS, got {other:?}"),
        }

        let _ = server_path;
        Ok::<(), RuntimeError>(())
    });

    let handle = ClientTcpPathSessionHandle::new(ClientTcpPathSessionRuntime {
        path,
        path_index: 0,
        session_id: SessionId(44),
        security: security(),
        codec_limits: CodecLimits::default(),
        mux_limits: MuxLimits::default(),
        command_queue: 4,
        stream_frame_queue: 4,
        closed_stream_cache_capacity: 8,
        reuse_latency_session: true,
        connect_timeout: crate::config::DEFAULT_PATH_PROBE_TIMEOUT,
        health: Arc::new(Mutex::new(ClientPathHealth {
            tcp: vec![ClientPathHealthRecord::default()],
            udp: Vec::new(),
        })),
    });
    let stream = handle
        .open_stream(
            StreamId(99),
            TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80))),
            IngressKind::HttpConnect,
            FlowLane::Throughput,
            StreamOpenRole::Active,
        )
        .await
        .expect("open stream");
    drop(stream);

    server.await.expect("server task").expect("server path");
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
fn mixed_reliable_latency_startup_ignores_probe_noise_without_product_evidence() {
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
fn mixed_reliable_latency_startup_uses_delivery_backed_metrics_after_idle() {
    let context = ClientPathContext::new(
        vec![
            "tcp://127.0.0.1:11092".parse().expect("tcp path"),
            "udp://127.0.0.1:11093".parse().expect("udp path"),
        ],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    context.mark_udp_path_probe_success(0, Duration::from_millis(1));
    context.mark_udp_path_delivery(
        0,
        PathDeliveryStats {
            payload_bytes: 1024 * 1024,
            first_payload_at: Some(Instant::now() - Duration::from_millis(10)),
            last_payload_at: Some(Instant::now()),
        },
    );

    let selected = context
        .reserve_reliable_stream_path(FlowLane::Latency, PATH_OPEN_SCORE_BYTES, &[])
        .expect("selected path");

    assert_eq!(selected.underlay, UnderlayProtocol::Udp);
    assert_eq!(selected.index, 0);
}

#[test]
fn mixed_reliable_latency_startup_preserves_global_order_without_family_preference() {
    let context = ClientPathContext::new(
        vec![
            "udp://127.0.0.1:11088".parse().expect("udp path"),
            "tcp://127.0.0.1:11089".parse().expect("tcp path"),
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
fn latency_startup_prefers_lowest_latency_path_before_bulk_promotion() {
    let context = ClientPathContext::new(
        vec![
            "udp://127.0.0.1:11120?srtt-ms=20&rate-mbps=80&low-latency=true"
                .parse()
                .expect("low latency path"),
            "udp://127.0.0.1:11121?srtt-ms=80&rate-mbps=200"
                .parse()
                .expect("balanced path"),
            "udp://127.0.0.1:11122?srtt-ms=160&rate-mbps=100"
                .parse()
                .expect("mild loss path"),
            "udp://127.0.0.1:11123?srtt-ms=180&rate-mbps=500"
                .parse()
                .expect("fat path"),
            "udp://127.0.0.1:11124?srtt-ms=420&jitter-ms=120&rate-mbps=50&expensive=true"
                .parse()
                .expect("poor path"),
        ],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    let first = context
        .reserve_reliable_stream_path(FlowLane::Latency, PATH_OPEN_SCORE_BYTES, &[])
        .expect("first latency path");
    assert_eq!(
        first,
        RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        },
        "stream open must start on the lowest-latency admissible path, not the highest-bandwidth path"
    );

    let second = context
        .reserve_reliable_stream_path(FlowLane::Latency, PATH_OPEN_SCORE_BYTES, &[])
        .expect("second latency path");
    assert_eq!(second.underlay, UnderlayProtocol::Udp);
    assert!(
        second.index <= 1,
        "additional latency opens may reuse the best latency path or spread to the next low-latency path, but must not jump to a high-RTT bulk path before demand is proven"
    );
}

#[test]
fn endpoint_only_latency_startup_uses_scored_order_when_bulk_load_exists() {
    let context = ClientPathContext::new(
        vec![
            "tcp://127.0.0.1:11090".parse().expect("busy path"),
            "tcp://127.0.0.1:11091".parse().expect("idle path"),
        ],
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

    let selected = context
        .reserve_reliable_stream_path(FlowLane::Latency, PATH_OPEN_SCORE_BYTES, &[])
        .expect("selected path");

    assert_eq!(selected.underlay, UnderlayProtocol::Tcp);
    assert_eq!(selected.index, 1);
}

#[test]
fn endpoint_only_tcp_latency_order_uses_scored_order_when_bulk_load_exists() {
    let context = ClientPathContext::new(
        vec![
            "tcp://127.0.0.1:11092".parse().expect("busy path"),
            "tcp://127.0.0.1:11093".parse().expect("idle path"),
        ],
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
        context.ordered_tcp_path_indices(FlowLane::Latency, PATH_OPEN_SCORE_BYTES),
        vec![1, 0]
    );
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
    let (tx, mut rx) = reliable_path_command_channels(1);
    tx.try_enqueue_admitted_frame(
        Frame::StreamData {
            stream_id: StreamId(3),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(b"queued-data"),
        },
        FlowLane::Throughput,
    )
    .expect("fill data queue");

    tokio::time::timeout(
        Duration::from_millis(50),
        tx.send_control(ReliablePathCommand::CloseStream(StreamId(3))),
    )
    .await
    .expect("control send should not wait for data queue")
    .expect("control send");

    match recv_reliable_path_command(&mut rx)
        .await
        .expect("first command")
    {
        ReliablePathCommand::CloseStream(stream_id) => assert_eq!(stream_id, StreamId(3)),
        _ => panic!("expected prioritized close stream control"),
    }
}

#[tokio::test]
async fn tcp_path_priority_probe_does_not_consume_bulk_data() {
    let (tx, mut rx) = reliable_path_command_channels(2);
    tx.try_enqueue_admitted_frame(
        Frame::StreamData {
            stream_id: StreamId(10),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(b"bulk"),
        },
        FlowLane::Throughput,
    )
    .expect("queue bulk data");

    assert!(
        try_recv_reliable_path_priority_command(&mut rx).is_none(),
        "priority-only probe must not consume ordinary data"
    );

    tx.try_enqueue_admitted_frame(
        Frame::StreamAck {
            stream_id: StreamId(10),
            complete: false,
            ranges: Vec::new(),
        },
        FlowLane::Throughput,
    )
    .expect("queue product feedback");

    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut rx),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
    ));
    assert!(matches!(
        recv_reliable_path_command(&mut rx).await,
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
}

#[tokio::test]
async fn fixed_reliable_path_close_is_carrier_local_without_hidden_detach() {
    let (commands, mut commands_rx) = reliable_path_command_channels(4);
    let stream = ReliablePathStreamHandle {
        stream_id: StreamId(44),
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: 64 * 1024,
        output: ReliablePathStreamOutput::fixed(
            UnderlayProtocol::Tcp,
            PathId(0),
            commands,
            MuxLimits::default(),
        ),
    };

    stream.close().await;

    match recv_reliable_path_command(&mut commands_rx)
        .await
        .expect("close command")
    {
        ReliablePathCommand::CloseStream(stream_id) => assert_eq!(stream_id, StreamId(44)),
        _ => panic!("expected close stream command"),
    }
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_reliable_path_command(&mut commands_rx)
        )
        .await
        .is_err(),
        "close() must not hide a STREAM_DETACH product frame"
    );
}

#[tokio::test]
async fn fixed_reliable_path_detach_is_explicit_product_control() {
    let (commands, mut commands_rx) = reliable_path_command_channels(4);
    let stream = ReliablePathStreamHandle {
        stream_id: StreamId(45),
        max_offset: u64::MAX,
        lane: FlowLane::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: 64 * 1024,
        output: ReliablePathStreamOutput::fixed(
            UnderlayProtocol::Tcp,
            PathId(0),
            commands,
            MuxLimits::default(),
        ),
    };

    stream.send_detach().await;
    stream.close().await;

    match recv_reliable_path_command(&mut commands_rx)
        .await
        .expect("detach command")
    {
        ReliablePathCommand::SendFrame(Frame::StreamDetach { stream_id }) => {
            assert_eq!(stream_id, StreamId(45));
        }
        _ => panic!("expected detach before close"),
    }
    match recv_reliable_path_command(&mut commands_rx)
        .await
        .expect("close command")
    {
        ReliablePathCommand::CloseStream(stream_id) => assert_eq!(stream_id, StreamId(45)),
        _ => panic!("expected close after detach"),
    }
}

#[tokio::test]
async fn tcp_path_interactive_frame_bypasses_saturated_bulk_queue() {
    let (tx, mut rx) = reliable_path_command_channels(1);
    tx.try_enqueue_admitted_frame(
        Frame::StreamData {
            stream_id: StreamId(10),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(b"bulk"),
        },
        FlowLane::Throughput,
    )
    .expect("fill bulk data queue");

    tx.try_enqueue_admitted_frame(
        Frame::StreamData {
            stream_id: StreamId(11),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(b"i"),
        },
        FlowLane::Latency,
    )
    .expect("interactive send");

    match recv_reliable_path_command(&mut rx)
        .await
        .expect("first command")
    {
        ReliablePathCommand::SendFrame(Frame::StreamData {
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
    let (tx, mut rx) = reliable_path_command_channels(1);
    tx.try_enqueue_admitted_frame(
        Frame::StreamData {
            stream_id: StreamId(30),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(b"bulk"),
        },
        FlowLane::Throughput,
    )
    .expect("fill bulk data queue");

    tx.try_enqueue_admitted_frame(
        Frame::StreamFin {
            stream_id: StreamId(30),
            final_offset: 4,
        },
        FlowLane::Throughput,
    )
    .expect("queue FIN");

    match recv_reliable_path_command(&mut rx)
        .await
        .expect("first command")
    {
        ReliablePathCommand::SendFrame(Frame::StreamFin {
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
async fn tcp_path_stream_ordered_fin_and_close_do_not_overtake_bulk_data() {
    let (tx, mut rx) = reliable_path_command_channels(3);
    tx.try_enqueue_admitted_frame(
        Frame::StreamData {
            stream_id: StreamId(31),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(b"bulk"),
        },
        FlowLane::Throughput,
    )
    .expect("queue bulk data");
    tx.try_enqueue_stream_ordered_frame(
        Frame::StreamFin {
            stream_id: StreamId(31),
            final_offset: 4,
        },
        FlowLane::Throughput,
    )
    .expect("queue ordered FIN");
    tx.send_stream_ordered_close(StreamId(31), FlowLane::Throughput)
        .await
        .expect("queue ordered close");

    assert!(matches!(
        recv_reliable_path_command(&mut rx).await,
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
    assert!(matches!(
        recv_reliable_path_command(&mut rx).await,
        Some(ReliablePathCommand::SendFrame(Frame::StreamFin {
            stream_id: StreamId(31),
            final_offset: 4,
        }))
    ));
    assert!(matches!(
        recv_reliable_path_command(&mut rx).await,
        Some(ReliablePathCommand::CloseStream(StreamId(31)))
    ));
}

#[tokio::test]
async fn tcp_path_command_queue_tracks_pending_frame_bytes() {
    let (tx, mut rx) = reliable_path_command_channels(2);
    let frame = Frame::StreamData {
        stream_id: StreamId(13),
        offset: 0,
        flags: StreamFlags::NONE,
        payload: Bytes::from_static(b"queued-bytes"),
    };
    let expected = frame_pacing_bytes(&frame) as u64;

    tx.try_enqueue_admitted_frame(frame, FlowLane::Throughput)
        .expect("queue frame");
    assert_eq!(tx.pending_bytes(), expected);

    let command = recv_reliable_path_command(&mut rx)
        .await
        .expect("queued command");
    assert!(matches!(
        command,
        ReliablePathCommand::SendFrame(Frame::StreamData { .. })
    ));
    assert_eq!(
        tx.pending_bytes(),
        expected,
        "dequeue alone must not hide writer backlog from admission"
    );
    rx.release_pending_command_bytes(reliable_path_command_pending_bytes(&command));
    assert_eq!(tx.pending_bytes(), 0);
}

#[tokio::test]
async fn server_tcp_path_input_frame_bypasses_queued_bulk_output() {
    let (tx, mut commands_rx) = reliable_path_command_channels(1);
    tx.try_enqueue_admitted_frame(
        Frame::StreamData {
            stream_id: StreamId(10),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(b"bulk"),
        },
        FlowLane::Throughput,
    )
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
            local_close_pending: false,
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
async fn client_tcp_path_local_close_keeps_inflight_receive_route() {
    let stream_id = StreamId(70);
    let (frames_tx, mut frames_rx) = mpsc::channel(4);
    let mut streams = HashMap::new();
    streams.insert(
        stream_id,
        ClientTcpPathStreamState {
            frames: frames_tx,
            pending_open: None,
            local_close_pending: true,
        },
    );
    let mut closed_streams = RecentIdCache::new(8);

    route_client_tcp_stream_frame(
        &mut streams,
        &mut closed_streams,
        stream_id,
        Frame::StreamData {
            stream_id,
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(b"inflight"),
        },
    )
    .await
    .expect("locally closing path should still drain in-flight data");

    match frames_rx
        .recv()
        .await
        .expect("in-flight frame")
        .expect("frame")
    {
        Frame::StreamData {
            stream_id: routed,
            payload,
            ..
        } => {
            assert_eq!(routed, stream_id);
            assert_eq!(&payload[..], b"inflight");
        }
        other => panic!("expected routed stream data, got {other:?}"),
    }
    assert!(
        streams.contains_key(&stream_id),
        "local close is a drain state, not receive-route deletion"
    );

    route_client_tcp_stream_frame(
        &mut streams,
        &mut closed_streams,
        stream_id,
        Frame::StreamFin {
            stream_id,
            final_offset: 8,
        },
    )
    .await
    .expect("FIN routes before cleanup");
    assert!(
        streams.contains_key(&stream_id),
        "plain routing alone does not apply terminal cleanup"
    );
}

#[tokio::test]
async fn server_tcp_registry_ignores_late_frames_for_recently_closed_stream() {
    let registry = ServerReliableStreamRegistry::new(8);
    let session_id = SessionId(11);
    let stream_id = StreamId(5);
    let (commands, _receivers) = reliable_path_command_channels(4);
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
                    initial_metrics: None,
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

    let (commands, _receivers) = reliable_path_command_channels(4);
    let opened = registry
        .open_or_attach(
            ServerReliableStreamOpenRequest {
                session_id,
                stream_id: unknown,
                target: &target,
                lane: FlowLane::Latency,
                attachment: ServerReliablePathAttachment {
                    path_id: PathId(1),
                    underlay: UnderlayProtocol::Tcp,
                    commands,
                    max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
                    role: StreamOpenRole::Active,
                    initial_metrics: None,
                },
            },
            MuxLimits::default(),
            8,
        )
        .expect("unknown-frame drop must not poison active open");
    assert!(
        matches!(opened, ServerReliableStreamOpen::New(_)),
        "unknown stream data is a product reordering/drop event, not terminal close state"
    );
}

#[tokio::test]
async fn server_reliable_relay_does_not_replay_whole_repair_cache_on_path_reattach() {
    let mux_limits = MuxLimits::default();
    let stream_id = StreamId(42);
    let (mut target_peer, target_side) = duplex(4096);
    let (commands_tx, mut commands_rx) = reliable_path_command_channels(8);
    let (frames_tx, frames_rx) = mpsc::channel(8);
    let relay = tokio::spawn(relay_reliable_stream(
        target_side,
        ReliablePathStream {
            stream_id,
            max_offset: mux_limits.max_stream_window_bytes,
            lane: FlowLane::Latency,
            underlay: UnderlayProtocol::Tcp,
            max_frame_payload_bytes: reliable_relay_buffer_len(mux_limits),
            output: ReliablePathStreamOutput::fixed(
                UnderlayProtocol::Tcp,
                PathId(0),
                commands_tx,
                mux_limits,
            ),
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
        recv_reliable_path_command(&mut commands_rx),
    )
    .await
    .expect("first relay frame timeout")
    .expect("first relay frame");
    match first {
        ReliablePathCommand::SendFrame(Frame::StreamData {
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
        recv_reliable_path_command(&mut commands_rx),
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
    context.mark_tcp_path_delivery(
        1,
        PathDeliveryStats {
            payload_bytes: 4 * 1024 * 1024,
            first_payload_at: Some(Instant::now()),
            last_payload_at: Some(Instant::now() + Duration::from_millis(100)),
        },
    );

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
    context.mark_tcp_path_delivery(
        1,
        PathDeliveryStats {
            payload_bytes: 8 * 1024 * 1024,
            first_payload_at: Some(Instant::now()),
            last_payload_at: Some(Instant::now() + Duration::from_millis(100)),
        },
    );

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
fn sole_suspect_path_remains_reopenable_when_no_survivor_exists() {
    let path = "tcp://127.0.0.1:10130?srtt-ms=20&rate-mbps=500"
        .parse::<PathSpec>()
        .expect("single path");
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("context");

    context.mark_relay_path_data_plane_failure(UnderlayProtocol::Tcp, 0);

    assert_eq!(
        context.reserve_reliable_stream_path(FlowLane::Throughput, 64 * 1024, &[]),
        Some(RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        }),
        "a sole suspect path is a recovery candidate; only Failed/Draining paths are hard excluded"
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
        transport_pto_from_snapshot(context.tcp_path_snapshot(0))
    );
    assert_eq!(
        reliable_relay_attach_open_timeout(
            &context,
            RelayPathKey {
                underlay: UnderlayProtocol::Tcp,
                index: 1,
            },
            FlowLane::Throughput,
        ),
        transport_pto_from_snapshot(context.tcp_path_snapshot(1))
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
fn mixed_bulk_validation_is_carrier_diverse_without_family_preference() {
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
        vec![(UnderlayProtocol::Tcp, 1), (UnderlayProtocol::Udp, 0)]
    );
}

#[test]
fn path_proof_observation_does_not_promote_tcp_candidate_without_delivery_evidence() {
    let context = ClientPathContext::new(
        vec![
            "tcp://127.0.0.1:10175"
                .parse()
                .expect("active endpoint-only path"),
            "tcp://127.0.0.1:10176"
                .parse()
                .expect("validation endpoint-only path"),
        ],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    assert!(
        !context.relay_path_has_bulk_model_evidence(UnderlayProtocol::Tcp, 1),
        "endpoint-only validation path starts without sender evidence"
    );

    context.mark_relay_path_proof_observation(
        UnderlayProtocol::Tcp,
        1,
        PathProofObservation {
            bytes: PATH_OPEN_SCORE_BYTES as u64,
            elapsed: Duration::from_millis(20),
        },
    );

    assert!(
        !context.relay_path_has_bulk_model_evidence(UnderlayProtocol::Tcp, 1),
        "path-scoped proof ACK creates liveness evidence, not unique bulk-model permission"
    );
    assert!(
        context
            .ordered_reliable_bulk_validation_path_keys(reliable_relay_buffer_len(
                MuxLimits::default()
            ))
            .contains(&RelayPathKey {
                underlay: UnderlayProtocol::Tcp,
                index: 1,
            }),
        "proof-success path remains eligible for validation until ACK-data evidence arrives"
    );
    {
        let health = context.health.lock().expect("client path health lock");
        let record = &health.tcp[1];
        assert!(record.path_proof_success);
        assert_eq!(
            record.delivery_samples, 0,
            "path proof is eligibility evidence, not bulk byte delivery"
        );
        assert!(
            record.measured_rate_bps.is_none(),
            "path proof must not seed bandwidth estimation with a tiny probe rate"
        );
    }
}

#[test]
fn path_proof_observation_does_not_promote_udp_candidate_without_carrier_sample() {
    let context = ClientPathContext::new(
        vec![
            "udp://127.0.0.1:10177"
                .parse()
                .expect("active endpoint-only path"),
            "udp://127.0.0.1:10178"
                .parse()
                .expect("validation endpoint-only path"),
        ],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.mark_relay_path_proof_observation(
        UnderlayProtocol::Udp,
        1,
        PathProofObservation {
            bytes: PATH_OPEN_SCORE_BYTES as u64,
            elapsed: Duration::from_millis(20),
        },
    );

    assert!(
        !context.relay_path_has_bulk_model_evidence(UnderlayProtocol::Udp, 1),
        "UDP proof ACK is reachability evidence; QUIC UDP needs ACK-derived data samples before unique bulk"
    );
    assert!(
        context
            .ordered_reliable_bulk_validation_path_keys(reliable_relay_buffer_len(
                MuxLimits::default()
            ))
            .contains(&RelayPathKey {
                underlay: UnderlayProtocol::Udp,
                index: 1,
            }),
        "proof-success QUIC UDP path remains eligible for validation until ACK-data evidence arrives"
    );
}

#[test]
fn path_proof_metrics_do_not_publish_probe_rate_as_bulk_capacity() {
    let metrics = path_proof_metrics(
        PathId(3),
        UnderlayProtocol::Udp,
        PathMetricDirection::ServerToClient,
        PathProofObservation {
            bytes: PATH_OPEN_SCORE_BYTES as u64,
            elapsed: Duration::from_millis(500),
        },
    )
    .expect("proof metrics");

    assert!(metrics.app_limited);
    assert!(!metrics.has_ack_derived_data_sample);
    assert_eq!(metrics.data_sample_count, 0);
    assert_eq!(
        metrics.delivery_rate_bps,
        ((PATH_OPEN_SCORE_BYTES as u64).saturating_mul(8)).saturating_mul(2),
        "proof metrics report the observed proof-frame rate but remain app-limited/non-bulk"
    );
    assert!(metrics.confidence_ppm > 0);
    assert_eq!(metrics.srtt_us, 500_000);
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
fn udp_latency_flow_is_protected_from_bulk_like_other_reliable_paths() {
    let first_path = "udp://127.0.0.1:10179?srtt-ms=20&rate-mbps=200"
        .parse::<PathSpec>()
        .expect("first udp path");
    let second_path = "udp://127.0.0.1:10180?srtt-ms=20&rate-mbps=200"
        .parse::<PathSpec>()
        .expect("second udp path");
    let context = ClientPathContext::new(
        vec![first_path, second_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    let now = Instant::now();
    for index in 0..2 {
        context.mark_udp_path_delivery(
            index,
            PathDeliveryStats {
                payload_bytes: 4 * 1024 * 1024,
                first_payload_at: Some(now),
                last_payload_at: Some(now + Duration::from_millis(160)),
            },
        );
    }
    context.reserve_udp_stream_path_load(0, FlowLane::Latency);

    assert_eq!(
        context
            .ordered_reliable_path_keys(FlowLane::Throughput, 4 * 1024 * 1024)
            .first()
            .copied(),
        Some(RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 1,
        })
    );
    assert_eq!(
        reliable_bulk_striping_path_keys(&context, 4 * 1024 * 1024)
            .first()
            .copied(),
        Some(RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 1,
        })
    );
}

#[test]
fn endpoint_only_tcp_bulk_striping_keeps_unknown_paths_out_of_measured_bulk_subflow_set() {
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
fn endpoint_only_udp_latency_reservation_preserves_configured_order_on_probe_noise() {
    let context = ClientPathContext::new(
        vec![
            "udp://127.0.0.1:10135".parse().expect("first path"),
            "udp://127.0.0.1:10136".parse().expect("second path"),
            "udp://127.0.0.1:10137".parse().expect("third path"),
        ],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.record_relay_path_send(UnderlayProtocol::Udp, 0, 34);
    context.record_relay_path_send(UnderlayProtocol::Udp, 1, 34);

    let selected = context
        .reserve_reliable_stream_path(FlowLane::Latency, PATH_OPEN_SCORE_BYTES, &[])
        .expect("selected path");

    assert_eq!(
        selected,
        RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        },
        "endpoint-only latency reservation must not turn path-proof/probe noise into a path preference"
    );
}

#[test]
fn endpoint_only_udp_latency_reservation_spreads_by_order_not_probe_noise() {
    let context = ClientPathContext::new(
        vec![
            "udp://127.0.0.1:10145".parse().expect("first path"),
            "udp://127.0.0.1:10146".parse().expect("second path"),
            "udp://127.0.0.1:10147".parse().expect("third path"),
        ],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    let first = context
        .reserve_reliable_stream_path(FlowLane::Latency, PATH_OPEN_SCORE_BYTES, &[])
        .expect("first latency reservation");
    assert_eq!(
        first,
        RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        }
    );

    context.record_relay_path_send(UnderlayProtocol::Udp, 1, 34);

    let second = context
        .reserve_reliable_stream_path(FlowLane::Latency, PATH_OPEN_SCORE_BYTES, &[])
        .expect("second latency reservation");

    assert_eq!(
        second,
        RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 1,
        },
        "endpoint-only latency spread must use configured fallback order until sender delivery evidence exists"
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
fn endpoint_only_udp_rate_hints_rank_service_but_do_not_prove_subflows() {
    let low_latency_path = "udp://127.0.0.1:10140?srtt-ms=20&rate-mbps=80&low-latency=true"
        .parse::<PathSpec>()
        .expect("low latency path");
    let balanced_path = "udp://127.0.0.1:10141?srtt-ms=80&rate-mbps=200"
        .parse::<PathSpec>()
        .expect("balanced path");
    let fat_path = "udp://127.0.0.1:10142?srtt-ms=180&rate-mbps=500"
        .parse::<PathSpec>()
        .expect("fat path");
    let context = ClientPathContext::new(
        vec![low_latency_path, balanced_path, fat_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    let candidates = reliable_bulk_striping_path_keys(
        &context,
        MuxLimits::default().max_reliable_relay_chunk_bytes,
    );

    assert_eq!(
        candidates.len(),
        1,
        "configured rate hints are priors for Service selection, not bulk delivery evidence for optional Subflow owners"
    );
}

#[test]
fn endpoint_only_udp_bulk_load_spreads_replacement_without_realtime_work() {
    let first_path = "udp://127.0.0.1:10177"
        .parse::<PathSpec>()
        .expect("first path");
    let second_path = "udp://127.0.0.1:10178"
        .parse::<PathSpec>()
        .expect("second path");
    let context = ClientPathContext::new(
        vec![first_path, second_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.mark_udp_path_probe_success(0, Duration::from_millis(1));
    context.reserve_udp_stream_path_load(0, FlowLane::Throughput);

    let reserved = context
        .reserve_reliable_stream_path(FlowLane::Latency, PATH_OPEN_SCORE_BYTES, &[])
        .expect("interactive reservation");
    assert_eq!(reserved.underlay, UnderlayProtocol::Udp);
    assert_eq!(reserved.index, 1);
}

#[test]
fn udp_bulk_repair_uses_liveness_status_when_stream_has_active_path() {
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

    assert_eq!(
        context.ordered_udp_stream_repair_path_indices(Some(0), FlowLane::Throughput, 64 * 1024),
        vec![1]
    );
}

#[tokio::test]
async fn udp_bulk_repair_candidate_uses_liveness_status_not_family_penalty() {
    let active_path = "udp://127.0.0.1:10142?srtt-ms=80&rate-mbps=80"
        .parse::<PathSpec>()
        .expect("active path");
    let repair_path = "udp://127.0.0.1:10143?srtt-ms=20&rate-mbps=200"
        .parse::<PathSpec>()
        .expect("repair path");
    let context = ClientPathContext::new(
        vec![active_path, repair_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    context.mark_udp_path_probe_success(0, Duration::from_millis(80));
    context.mark_udp_path_probe_success(1, Duration::from_millis(20));
    let stream_id = StreamId(149);
    let (udp_stream, _udp_commands, _udp_frames) =
        opened_relay_stream_for_test(stream_id, UnderlayProtocol::Udp, 0);
    let remotes = ReliableRelayRemoteSet::new(udp_stream, 4);

    let candidates =
        reliable_relay_repair_path_candidates(&context, &remotes, FlowLane::Throughput, 64 * 1024);

    assert_eq!(
        candidates.first().copied(),
        Some(RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 1,
        }),
        "repair-only admission should be driven by live path status and metrics, not a UDP-specific delivery-evidence penalty"
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
fn mixed_latency_startup_uses_metrics_not_udp_family_penalty() {
    let tcp_path = "tcp://127.0.0.1:10190?srtt-ms=12&rate-mbps=1000"
        .parse::<PathSpec>()
        .expect("tcp path");
    let udp_path = "udp://127.0.0.1:10191?srtt-ms=8&rate-mbps=1000"
        .parse::<PathSpec>()
        .expect("udp path");
    let context = ClientPathContext::new(
        vec![tcp_path, udp_path],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    context.reserve_udp_stream_path_load(0, FlowLane::RealtimeDatagram);

    assert_eq!(
        context.reserve_reliable_stream_path(FlowLane::Latency, PATH_OPEN_SCORE_BYTES, &[]),
        Some(RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        }),
        "latency startup must follow measured link status instead of penalizing the UDP family"
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
fn mixed_udp_repair_uses_liveness_status_on_active_tcp_stream() {
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

    let candidates = context.ordered_udp_stream_repair_path_indices(
        None,
        FlowLane::Throughput,
        MuxLimits::default().max_reliable_relay_chunk_bytes,
    );
    assert_eq!(candidates.first().copied(), Some(1));
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

    assert_eq!(
        context.ordered_udp_stream_repair_path_indices(
            Some(0),
            FlowLane::Throughput,
            MuxLimits::default().max_reliable_relay_chunk_bytes,
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
fn mixed_bulk_striping_penalizes_lossy_high_rtt_quic_udp_path() {
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
fn mixed_bulk_striping_can_choose_best_carrier_without_active_subflow_set() {
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

#[tokio::test]
async fn relay_active_attach_candidates_are_metric_ordered_across_carriers() {
    let context = ClientPathContext::new(
        vec![
            "udp://127.0.0.1:10179?srtt-ms=100&rate-mbps=20"
                .parse()
                .expect("active slow udp path"),
            "tcp://127.0.0.1:10180?srtt-ms=10&rate-mbps=500"
                .parse()
                .expect("fast tcp path"),
            "udp://127.0.0.1:10181?srtt-ms=20&rate-mbps=200"
                .parse()
                .expect("moderate udp path"),
        ],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    let (active_udp, _commands, _frames) =
        opened_relay_stream_for_test(StreamId(160), UnderlayProtocol::Udp, 0);
    let remotes = ReliableRelayRemoteSet::new(active_udp, 4);

    let candidates =
        reliable_relay_active_path_candidates(&context, &remotes, FlowLane::Throughput, 64 * 1024);

    assert_eq!(
        candidates.first().copied(),
        Some(RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        }),
        "active UDP must not hide a better measured TCP candidate"
    );
}

#[tokio::test]
async fn repair_attach_candidates_cross_carrier_by_eta_not_active_family() {
    let context = ClientPathContext::new(
        vec![
            "tcp://127.0.0.1:11168?srtt-ms=20&rate-mbps=500"
                .parse()
                .expect("fast tcp repair path"),
            "udp://127.0.0.1:11169?srtt-ms=180&rate-mbps=40"
                .parse()
                .expect("slow active udp path"),
        ],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    let stream_id = StreamId(151);
    let (udp_stream, _udp_commands, _udp_frames) =
        opened_relay_stream_for_test(stream_id, UnderlayProtocol::Udp, 0);
    let remotes = ReliableRelayRemoteSet::new(udp_stream, 4);

    let candidates =
        reliable_relay_repair_path_candidates(&context, &remotes, FlowLane::Throughput, 64 * 1024);

    assert_eq!(
        candidates.first().copied(),
        Some(RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        })
    );
}

#[test]
fn throughput_repair_bytes_use_repair_attachment_role() {
    let mut send_stream = ReliableSendStream::new(StreamId(152), MuxLimits::default());
    send_stream
        .send_data(Bytes::from_static(b"repairable"), StreamFlags::NONE)
        .expect("send data");

    assert!(reliable_relay_should_race_repair(
        FlowLane::Throughput,
        &send_stream,
        false,
        ReliableRelayAttachMode::Any,
    ));
}

fn opened_relay_stream_for_test(
    stream_id: StreamId,
    underlay: UnderlayProtocol,
    path_index: usize,
) -> (
    OpenedRemoteStream,
    ReliablePathCommandReceivers,
    mpsc::Sender<Result<Frame, RuntimeError>>,
) {
    let mux_limits = MuxLimits::default();
    let (commands, command_rx) = reliable_path_command_channels(4);
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
                output: ReliablePathStreamOutput::fixed(
                    underlay,
                    PathId(path_index as u16),
                    commands,
                    mux_limits,
                ),
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
async fn relay_sender_queue_full_blocks_without_detaching_path() {
    let path = "tcp://127.0.0.1:10270?srtt-ms=20&rate-mbps=500"
        .parse::<PathSpec>()
        .expect("path");
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("context");
    let stream_id = StreamId(77);
    let mux_limits = MuxLimits::default();
    let (commands, mut command_rx) = reliable_path_command_channels(1);
    commands
        .try_enqueue_admitted_frame(
            Frame::StreamData {
                stream_id,
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"queued"),
            },
            FlowLane::Throughput,
        )
        .expect("prefill relay carrier queue");
    let (_frames_tx, frames_rx) = mpsc::channel(4);
    let opened = OpenedRemoteStream {
        path_index: 0,
        stream: ReliablePathStream {
            stream_id,
            max_offset: mux_limits.max_stream_window_bytes,
            lane: FlowLane::Throughput,
            underlay: UnderlayProtocol::Tcp,
            max_frame_payload_bytes: reliable_relay_buffer_len(mux_limits),
            output: ReliablePathStreamOutput::fixed(
                UnderlayProtocol::Tcp,
                PathId(0),
                commands,
                mux_limits,
            ),
            frames: frames_rx,
        },
    };
    let mut remotes = ReliableRelayRemoteSet::new(opened, 4);
    let mut sender = RelaySenderService::new(stream_id);

    let err = sender
        .send_stream_data(
            &context,
            &mut remotes,
            Frame::StreamData {
                stream_id,
                offset: 1024,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"later"),
            },
        )
        .await
        .expect_err("full relay carrier queue should be sender-service backpressure");

    assert!(matches!(err, RuntimeError::SenderServiceBlocked));
    assert!(
        !remotes.is_empty(),
        "queue backpressure must not detach path"
    );
    assert!(matches!(
        recv_reliable_path_command(&mut command_rx).await,
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            payload,
            ..
        })) if payload == Bytes::from_static(b"queued")
    ));
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_reliable_path_command(&mut command_rx)
        )
        .await
        .is_err(),
        "blocked relay dispatch must not enqueue another STREAM_DATA frame"
    );
}

#[tokio::test]
async fn client_sender_slices_large_upload_reads_to_service_quantum() {
    let path = "udp://127.0.0.1:10273?srtt-ms=20&rate-mbps=500"
        .parse::<PathSpec>()
        .expect("path");
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("context");
    let stream_id = StreamId(80);
    let mux_limits = MuxLimits::default();
    let (opened, mut command_rx, _frames_tx) =
        opened_relay_stream_for_test(stream_id, UnderlayProtocol::Udp, 0);
    let mut remotes = ReliableRelayRemoteSet::new(opened, 4);
    let mut send_stream = ReliableSendStream::new(stream_id, mux_limits);
    send_stream.update_max_offset(mux_limits.max_stream_window_bytes);
    let mut sender = RelaySenderService::new(stream_id);
    let mut sender_queue = ReliableRelaySenderQueue::default();
    let queued_bytes = mux_limits.max_reliable_relay_chunk_bytes;
    let quantum = BBR_MAX_SEND_QUANTUM_BYTES.min(reliable_relay_buffer_len(mux_limits));
    sender_queue.push_data(Bytes::from(vec![0x5a; queued_bytes]));

    let dispatch = sender
        .dispatch_client_queued_work(
            &context,
            &ReliableRelayOpenSpec {
                target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80))),
                ingress: IngressKind::Socks5,
            },
            FlowLane::Throughput,
            &mut remotes,
            &mut send_stream,
            &mut sender_queue,
            true,
            quantum,
        )
        .await
        .expect("dispatch upload service quantum");

    match dispatch {
        ClientQueuedDispatch::Data { payload_bytes } => {
            assert_eq!(payload_bytes, quantum);
        }
        ClientQueuedDispatch::Repair { .. } => panic!("expected upload data dispatch"),
    }
    assert_eq!(sender_queue.data_bytes(), queued_bytes - quantum);
    assert_eq!(send_stream.next_offset(), quantum as u64);
    assert!(matches!(
        recv_reliable_path_command(&mut command_rx).await,
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            stream_id: id,
            offset: 0,
            payload,
            ..
        })) if id == stream_id && payload.len() == quantum
    ));
}

#[tokio::test]
async fn path_failure_repairs_enqueue_repair_lane_without_carrier_send() {
    let path = "tcp://127.0.0.1:10271?srtt-ms=20&rate-mbps=500"
        .parse::<PathSpec>()
        .expect("path");
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("context");
    let stream_id = StreamId(78);
    let mux_limits = MuxLimits::default();
    let (opened, mut command_rx, _frames_tx) =
        opened_relay_stream_for_test(stream_id, UnderlayProtocol::Tcp, 0);
    let mut remotes = ReliableRelayRemoteSet::new(opened, 4);
    let mut send_stream = ReliableSendStream::new(stream_id, mux_limits);
    let mut sender = RelaySenderService::new(stream_id);
    let failed_key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };

    let frame = send_stream
        .send_data(Bytes::from_static(b"repair-me"), StreamFlags::NONE)
        .expect("stream data frame");
    sender
        .send_stream_data(&context, &mut remotes, frame)
        .await
        .expect("initial data send");
    assert!(matches!(
        recv_reliable_path_command(&mut command_rx).await,
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            payload,
            ..
        })) if payload == Bytes::from_static(b"repair-me")
    ));

    let mut sender_queue = ReliableRelaySenderQueue::default();
    assert!(sender.enqueue_failed_path_gap_repairs(
        &mut sender_queue,
        &context,
        &remotes,
        &send_stream,
        failed_key,
        FlowLane::Throughput,
    ));

    let (lane, work) = sender_queue.pop_front().expect("queued repair");
    assert_eq!(lane, ReliableRelayQueuedWorkLane::Repair);
    assert!(matches!(
        work.kind,
        ReliableRelayQueuedWorkKind::Repair {
            frame: Frame::StreamData {
                payload,
                ..
            },
            cause: RelaySendCause::PathFailureRepair,
        } if payload == Bytes::from_static(b"repair-me")
    ));
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_reliable_path_command(&mut command_rx)
        )
        .await
        .is_err(),
        "path-failure repair generation must not send directly to the carrier"
    );
}

#[tokio::test]
async fn datagram_response_queue_full_is_realtime_backpressure() {
    let flow_id = DatagramFlowId(12);
    let (commands, mut command_rx) = reliable_path_command_channels(1);
    commands
        .try_enqueue_admitted_frame(
            Frame::DatagramData {
                flow_id,
                datagram_id: DatagramId(1),
                ttl_ms: 1000,
                payload: Bytes::from_static(b"queued"),
            },
            FlowLane::RealtimeDatagram,
        )
        .expect("prefill realtime queue");

    let err = try_send_server_datagram_realtime_frame(
        &commands,
        Frame::DatagramData {
            flow_id,
            datagram_id: DatagramId(2),
            ttl_ms: 1000,
            payload: Bytes::from_static(b"later"),
        },
    )
    .expect_err("full realtime queue should be sender-service backpressure");

    assert!(matches!(err, RuntimeError::SenderServiceBlocked));
    assert!(matches!(
        recv_reliable_path_command(&mut command_rx).await,
        Some(ReliablePathCommand::SendFrame(Frame::DatagramData {
            datagram_id,
            payload,
            ..
        })) if datagram_id == DatagramId(1) && payload == Bytes::from_static(b"queued")
    ));
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_reliable_path_command(&mut command_rx)
        )
        .await
        .is_err(),
        "blocked datagram response must not enqueue another frame"
    );
}

#[tokio::test]
async fn datagram_close_queue_full_is_realtime_backpressure() {
    let flow_id = DatagramFlowId(13);
    let (commands, mut command_rx) = reliable_path_command_channels(1);
    commands
        .try_enqueue_admitted_frame(
            Frame::DatagramData {
                flow_id,
                datagram_id: DatagramId(1),
                ttl_ms: 1000,
                payload: Bytes::from_static(b"queued"),
            },
            FlowLane::RealtimeDatagram,
        )
        .expect("prefill realtime queue");

    let err = try_send_server_datagram_realtime_frame(&commands, Frame::DatagramClose { flow_id })
        .expect_err("full realtime queue should be sender-service backpressure");

    assert!(matches!(err, RuntimeError::SenderServiceBlocked));
    assert!(matches!(
        recv_reliable_path_command(&mut command_rx).await,
        Some(ReliablePathCommand::SendFrame(Frame::DatagramData {
            datagram_id,
            payload,
            ..
        })) if datagram_id == DatagramId(1) && payload == Bytes::from_static(b"queued")
    ));
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_reliable_path_command(&mut command_rx)
        )
        .await
        .is_err(),
        "blocked datagram close must not wait or enqueue behind a full realtime queue"
    );
}

#[tokio::test]
async fn mixed_relay_current_carrier_tracks_latest_data_path() {
    let (tcp_stream, _tcp_commands, _tcp_frames) =
        opened_relay_stream_for_test(StreamId(44), UnderlayProtocol::Tcp, 0);
    let mut remotes = ReliableRelayRemoteSet::new(tcp_stream, 4);
    assert_eq!(remotes.active_path_underlay(), Some(UnderlayProtocol::Tcp));

    let (udp_stream, _udp_commands, _udp_frames) =
        opened_relay_stream_for_test(StreamId(44), UnderlayProtocol::Udp, 0);
    remotes.attach(udp_stream);
    assert_eq!(remotes.active_path_underlay(), Some(UnderlayProtocol::Udp));

    let context = ClientPathContext::new(
        vec![
            "tcp://127.0.0.1:10182?srtt-ms=200&rate-mbps=20"
                .parse()
                .expect("attached slow tcp path"),
            "udp://127.0.0.1:10183?srtt-ms=200&rate-mbps=20"
                .parse()
                .expect("attached slow udp path"),
            "tcp://127.0.0.1:10184?srtt-ms=10&rate-mbps=500"
                .parse()
                .expect("fast tcp candidate"),
            "udp://127.0.0.1:10185?srtt-ms=100&rate-mbps=50"
                .parse()
                .expect("slower udp candidate"),
        ],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    assert_eq!(
        reliable_relay_active_path_candidates(&context, &remotes, FlowLane::Throughput, 64 * 1024)
            .first()
            .copied(),
        Some(RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 1,
        }),
        "latest active path is state, not a family preference"
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

    match recv_reliable_path_command(&mut active_commands)
        .await
        .expect("active command")
    {
        ReliablePathCommand::SendFrame(Frame::StreamData { payload, .. }) => {
            assert_eq!(&payload[..], b"data");
        }
        _ => panic!("expected data on active path"),
    }
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_reliable_path_command(&mut repair_commands)
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
    match recv_reliable_path_command(&mut active_commands)
        .await
        .expect("active path first command")
    {
        ReliablePathCommand::SendFrame(Frame::StreamData { offset, .. }) => {
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
    match recv_reliable_path_command(&mut active_commands)
        .await
        .expect("active path second command")
    {
        ReliablePathCommand::SendFrame(Frame::StreamData { offset, .. }) => {
            assert_eq!(offset, 64 * 1024);
        }
        _ => panic!("expected second bulk chunk on active path"),
    }
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_reliable_path_command(&mut repair_commands)
        )
        .await
        .is_err(),
        "repair attachment must not receive ordinary bulk data"
    );
}

#[tokio::test]
async fn request_sender_blocked_bulk_admission_does_not_fallback_to_eta_path() {
    let stream_id = StreamId(147);
    let (initial_stream, mut initial_commands, _initial_frames) =
        opened_relay_stream_for_test(stream_id, UnderlayProtocol::Tcp, 0);
    let mut remotes = ReliableRelayRemoteSet::new(initial_stream, 8);
    let (survivor_stream, mut survivor_commands, _survivor_frames) =
        opened_relay_stream_for_test(stream_id, UnderlayProtocol::Udp, 0);
    remotes.attach(survivor_stream);
    let context = ClientPathContext::new(
        vec![
            "tcp://127.0.0.1:11148?srtt-ms=20&rate-mbps=500"
                .parse()
                .expect("initial owner path"),
            "udp://127.0.0.1:11149?srtt-ms=10&rate-mbps=500"
                .parse()
                .expect("survivor path"),
        ],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    context.mark_tcp_path_open_success(0, Duration::from_millis(20), FlowLane::Throughput);
    context.mark_udp_path_open_success(0, Duration::from_millis(10));

    let mut sender = RelaySenderService::new(stream_id);
    send_relay_stream_frame_for_test(
        &mut sender,
        &context,
        &mut remotes,
        Frame::StreamData {
            stream_id,
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![1u8; 64 * 1024]),
        },
    )
    .await
    .expect("first send establishes lower-frontier owner");
    match recv_reliable_path_command(&mut initial_commands)
        .await
        .expect("initial owner command")
    {
        ReliablePathCommand::SendFrame(Frame::StreamData { offset, .. }) => {
            assert_eq!(offset, 0);
        }
        _ => panic!("expected first bulk chunk on initial owner path"),
    }

    let initial_instance = remotes
        .path_instance_for_key(RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        })
        .expect("initial lower-frontier owner should still be attached");
    assert!(
        remotes.fail_path_instance(&context, initial_instance).await,
        "initial lower-frontier owner should be removable"
    );
    match recv_reliable_path_command(&mut initial_commands)
        .await
        .expect("detach command for failed owner")
    {
        ReliablePathCommand::SendFrame(Frame::StreamDetach {
            stream_id: detached,
        }) => {
            assert_eq!(detached, stream_id);
        }
        _ => panic!("failed owner must detach before local close"),
    }
    match recv_reliable_path_command(&mut initial_commands)
        .await
        .expect("close command for failed owner")
    {
        ReliablePathCommand::CloseStream(closed) => assert_eq!(closed, stream_id),
        _ => panic!("failed owner must close after detach"),
    }

    let err = send_relay_stream_frame_for_test(
        &mut sender,
        &context,
        &mut remotes,
        Frame::StreamData {
            stream_id,
            offset: 64 * 1024,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![2u8; 64 * 1024]),
        },
    )
    .await
    .expect_err("later data must wait when no attached path owns the lower frontier");
    assert!(matches!(err, RuntimeError::SenderServiceBlocked));
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_reliable_path_command(&mut survivor_commands)
        )
        .await
        .is_err(),
        "blocked admission must not fall back to the raw lowest-ETA survivor path"
    );
}

#[tokio::test]
async fn bulk_relay_validation_attach_sends_path_proof_not_unique_stream_data() {
    let stream_id = StreamId(149);
    let (active_stream, mut active_commands, _active_frames) =
        opened_relay_stream_for_test(stream_id, UnderlayProtocol::Tcp, 0);
    let mut remotes = ReliableRelayRemoteSet::new(active_stream, 8);
    let (validation_stream, mut validation_commands, _validation_frames) =
        opened_relay_stream_for_test(stream_id, UnderlayProtocol::Tcp, 1);
    remotes.attach_for_validation(validation_stream);
    match tokio::time::timeout(
        Duration::from_millis(100),
        recv_reliable_path_command(&mut validation_commands),
    )
    .await
    .expect("validation proof timeout")
    .expect("validation proof command")
    {
        ReliablePathCommand::SendFrame(Frame::PathProofData {
            path_id, payload, ..
        }) => {
            assert_eq!(path_id, PathId(1));
            assert!(!payload.is_empty());
        }
        _ => panic!("validation attach must send carrier proof"),
    }
    let context = ClientPathContext::new(
        vec![
            "tcp://127.0.0.1:11160"
                .parse()
                .expect("active endpoint-only path"),
            "tcp://127.0.0.1:11161"
                .parse()
                .expect("validation endpoint-only path"),
        ],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    context.mark_tcp_path_open_success(0, Duration::from_millis(20), FlowLane::Throughput);
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
        recv_reliable_path_command(&mut active_commands),
    )
    .await
    .expect("active path timeout")
    .expect("active path command")
    {
        ReliablePathCommand::SendFrame(Frame::StreamData { offset, .. }) => assert_eq!(offset, 0),
        _ => panic!("expected active owner to receive ordinary stream data"),
    }
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_reliable_path_command(&mut validation_commands)
        )
        .await
        .is_err(),
        "validation path must not receive unique ordinary data before path-scoped proof graduates"
    );
}

#[tokio::test]
async fn validation_attach_keeps_active_data_lane_credit_visible() {
    let stream_id = StreamId(146);
    let (active_stream, _active_commands, _active_frames) =
        opened_relay_stream_for_test(stream_id, UnderlayProtocol::Tcp, 0);
    let mut remotes = ReliableRelayRemoteSet::new(active_stream, 8);

    assert!(
        remotes.can_enqueue_work_lane_now(ReliableRelayQueuedWorkLane::Data, FlowLane::Throughput),
        "active path credit must be visible before validation attach"
    );

    let (validation_stream, mut validation_commands, _validation_frames) =
        opened_relay_stream_for_test(stream_id, UnderlayProtocol::Tcp, 1);
    remotes.attach_for_validation(validation_stream);

    match tokio::time::timeout(
        Duration::from_millis(100),
        recv_reliable_path_command(&mut validation_commands),
    )
    .await
    .expect("validation proof timeout")
    .expect("validation proof command")
    {
        ReliablePathCommand::SendFrame(Frame::PathProofData { .. }) => {}
        _ => panic!("validation attach must send carrier proof"),
    }

    assert!(
        remotes.can_enqueue_work_lane_now(ReliableRelayQueuedWorkLane::Data, FlowLane::Throughput),
        "validation attachment must not hide active service-path data credit"
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
        recv_reliable_path_command(&mut best_commands),
    )
    .await
    .expect("best path timeout")
    .expect("best path command")
    {
        ReliablePathCommand::SendFrame(Frame::StreamData { offset, .. }) => {
            assert_eq!(offset, 0);
        }
        _ => panic!("expected measured better path to carry admitted TCP bulk data"),
    }
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_reliable_path_command(&mut slow_active_commands)
        )
        .await
        .is_err(),
        "uncompetitive active path should not keep bulk when ECF admits a better path"
    );
}

#[tokio::test]
async fn measured_alternate_path_promotes_only_when_scheduler_score_improves() {
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
        PathRateSample::new(PATH_OPEN_SCORE_BYTES as u64, Duration::from_millis(2))
            .expect("startup-sized rate sample"),
    );
    assert!(
        !reliable_relay_delivery_path_should_become_active(
            &context,
            remotes.active_path_key(),
            fast_instance.key,
            FlowLane::Throughput,
            64 * 1024,
        ),
        "startup-sized owner-byte evidence may keep a same-family Subflow eligible, but must not migrate the Service owner"
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
    match recv_reliable_path_command(&mut fast_commands)
        .await
        .expect("fast command")
    {
        ReliablePathCommand::SendFrame(Frame::StreamData { payload, .. }) => {
            assert_eq!(&payload[..], b"bulk");
        }
        _ => panic!("expected data on promoted fast path"),
    }
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            recv_reliable_path_command(&mut slow_commands)
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
async fn cross_underlay_bulk_promotion_requires_bulk_sized_delivery_sample() {
    let context = ClientPathContext::new(
        vec![
            "tcp://127.0.0.1:11020?srtt-ms=80&rate-mbps=80"
                .parse()
                .expect("tcp service path"),
            "udp://127.0.0.1:11021?srtt-ms=20&rate-mbps=500"
                .parse()
                .expect("udp candidate"),
        ],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    let current = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    let delivered = RelayPathKey {
        underlay: UnderlayProtocol::Udp,
        index: 0,
    };

    context.mark_relay_path_rate_sample(
        UnderlayProtocol::Udp,
        0,
        PathRateSample::new(PATH_OPEN_SCORE_BYTES as u64, Duration::from_millis(1))
            .expect("startup-sized sample"),
    );

    assert!(
        !reliable_relay_delivery_path_should_become_active(
            &context,
            Some(current),
            delivered,
            FlowLane::Throughput,
            BBR_MAX_SEND_QUANTUM_BYTES,
        ),
        "a startup-sized product sample must not migrate reliable Service ownership across TCP/QUIC families"
    );

    context.mark_relay_path_rate_sample(
        UnderlayProtocol::Udp,
        0,
        PathRateSample::new(BBR_MAX_SEND_QUANTUM_BYTES as u64, Duration::from_millis(40))
            .expect("bulk-sized sample"),
    );

    assert!(
        reliable_relay_delivery_path_should_become_active(
            &context,
            Some(current),
            delivered,
            FlowLane::Throughput,
            BBR_MAX_SEND_QUANTUM_BYTES,
        ),
        "bulk-sized product evidence may explicitly migrate Service ownership when the metric model prefers it"
    );
}

#[tokio::test]
async fn mixed_relay_path_status_active_does_not_replay_whole_repair_cache_on_instance() {
    let mux_limits = MuxLimits::default();
    let stream_id = StreamId(45);
    let (commands, mut command_rx) = reliable_path_command_channels(4);
    let (_frames_tx, frames_rx) = mpsc::channel(4);
    let opened = OpenedRemoteStream {
        path_index: 1,
        stream: ReliablePathStream {
            stream_id,
            max_offset: mux_limits.max_stream_window_bytes,
            lane: FlowLane::Throughput,
            underlay: UnderlayProtocol::Udp,
            max_frame_payload_bytes: reliable_relay_buffer_len(mux_limits),
            output: ReliablePathStreamOutput::fixed(
                UnderlayProtocol::Udp,
                PathId(1),
                commands,
                mux_limits,
            ),
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
            recv_reliable_path_command(&mut command_rx)
        )
        .await
        .is_err(),
        "repair emission must be gap-targeted instead of whole-cache"
    );
}
