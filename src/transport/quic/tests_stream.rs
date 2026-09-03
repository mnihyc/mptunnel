use super::super::Endpoint;
use super::*;
use crate::mux::MuxLimits;
use crate::protocol::{
    CloseReason, DatagramFlowId, DatagramId, IpPacketId, IpTunnelId, StreamId, TargetAddr,
};
use bytes::Bytes;
use std::time::Duration;
use tokio::time::timeout;

#[test]
fn quic_writer_splits_large_stream_data_below_product_scheduler() {
    let limits = CodecLimits::default();
    let payload = Bytes::from(vec![7u8; QUIC_STREAM_RECORD_PAYLOAD_BYTES * 2 + 17]);
    let mut packet = Vec::new();
    encode_quic_length_prefixed_frame(
        &Frame::StreamData {
            stream_id: StreamId(9),
            offset: 123,
            payload,
        },
        limits,
        &mut packet,
    )
    .expect("encode split stream data");

    let mut cursor = 0usize;
    let mut decoded = Vec::new();
    while cursor < packet.len() {
        let len = u32::from_be_bytes([
            packet[cursor],
            packet[cursor + 1],
            packet[cursor + 2],
            packet[cursor + 3],
        ]) as usize;
        cursor += FRAME_LEN_BYTES;
        let frame = decode_frame_bytes(
            Bytes::copy_from_slice(&packet[cursor..cursor + len]),
            limits,
        )
        .expect("decode split carrier record");
        decoded.push(frame);
        cursor += len;
    }

    assert_eq!(decoded.len(), 3);
    let mut expected_offset = 123u64;
    for frame in &decoded {
        let Frame::StreamData {
            stream_id,
            offset,
            payload,
        } = frame
        else {
            panic!("all split records must remain STREAM_DATA");
        };
        assert_eq!(*stream_id, StreamId(9));
        assert_eq!(*offset, expected_offset);
        expected_offset = expected_offset.saturating_add(payload.len() as u64);
        assert!(payload.len() <= QUIC_STREAM_RECORD_PAYLOAD_BYTES);
    }
}

fn encoded_h3_records(frames: &[Frame], limits: CodecLimits) -> Bytes {
    let mut packet = Vec::new();
    for frame in frames {
        encode_length_prefixed_frame(frame, limits, &mut packet).expect("encode H3 record");
    }
    Bytes::from(packet)
}

#[test]
fn ready_h3_stream_data_decode_is_zero_copy_for_one_record() {
    let limits = CodecLimits::default();
    let payload = Bytes::from_static(b"one contiguous record");
    let mut pending = encoded_h3_records(
        &[Frame::StreamData {
            stream_id: StreamId(7),
            offset: 11,
            payload: payload.clone(),
        }],
        limits,
    );
    let encoded_frame_len =
        u32::from_be_bytes([pending[0], pending[1], pending[2], pending[3]]) as usize;
    let payload_start = FRAME_LEN_BYTES + encoded_frame_len - payload.len();
    let encoded_payload_ptr = pending.slice(payload_start..).as_ptr();

    let decoded = decode_ready_h3_frame(&mut pending, limits)
        .expect("decode ready record")
        .expect("complete record");
    let Frame::StreamData {
        stream_id,
        offset,
        payload: decoded_payload,
    } = decoded
    else {
        panic!("record must remain STREAM_DATA");
    };
    assert_eq!(stream_id, StreamId(7));
    assert_eq!(offset, 11);
    assert_eq!(decoded_payload, payload);
    assert_eq!(decoded_payload.as_ptr(), encoded_payload_ptr);
    assert!(pending.is_empty());
}

#[test]
fn ready_h3_stream_data_coalesces_adjacent_records_from_one_chunk() {
    let limits = CodecLimits::default();
    let mut pending = encoded_h3_records(
        &[
            Frame::StreamData {
                stream_id: StreamId(9),
                offset: 100,
                payload: Bytes::from_static(b"abc"),
            },
            Frame::StreamData {
                stream_id: StreamId(9),
                offset: 103,
                payload: Bytes::from_static(b"defg"),
            },
            Frame::StreamData {
                stream_id: StreamId(9),
                offset: 107,
                payload: Bytes::from_static(b"hij"),
            },
        ],
        limits,
    );

    assert_eq!(
        decode_ready_h3_frame(&mut pending, limits).expect("decode ready batch"),
        Some(Frame::StreamData {
            stream_id: StreamId(9),
            offset: 100,
            payload: Bytes::from_static(b"abcdefghij"),
        })
    );
    assert!(pending.is_empty());
}

#[test]
fn ready_h3_stream_data_stops_at_semantic_and_codec_boundaries() {
    let limits = CodecLimits::default();
    let boundaries = [
        Frame::Ping { nonce: 1 },
        Frame::StreamFin {
            stream_id: StreamId(9),
            final_offset: 3,
        },
        Frame::StreamData {
            stream_id: StreamId(9),
            offset: 4,
            payload: Bytes::from_static(b"gap"),
        },
        Frame::StreamData {
            stream_id: StreamId(10),
            offset: 3,
            payload: Bytes::from_static(b"other"),
        },
    ];
    for boundary in boundaries {
        let first = Frame::StreamData {
            stream_id: StreamId(9),
            offset: 0,
            payload: Bytes::from_static(b"abc"),
        };
        let mut pending = encoded_h3_records(&[first.clone(), boundary.clone()], limits);
        assert_eq!(
            decode_ready_h3_frame(&mut pending, limits).expect("decode first record"),
            Some(first)
        );
        assert_eq!(
            decode_ready_h3_frame(&mut pending, limits).expect("decode preserved boundary"),
            Some(boundary)
        );
        assert!(pending.is_empty());
    }

    for bounded_limits in [
        CodecLimits {
            max_payload_bytes: 4,
            ..limits
        },
        CodecLimits {
            max_frame_bytes: 33,
            ..limits
        },
    ] {
        let first = Frame::StreamData {
            stream_id: StreamId(9),
            offset: 0,
            payload: Bytes::from_static(b"abc"),
        };
        let second = Frame::StreamData {
            stream_id: StreamId(9),
            offset: 3,
            payload: Bytes::from_static(b"de"),
        };
        let mut pending = encoded_h3_records(&[first.clone(), second.clone()], bounded_limits);
        assert_eq!(
            decode_ready_h3_frame(&mut pending, bounded_limits)
                .expect("decode frame below aggregate limit"),
            Some(first)
        );
        assert_eq!(
            decode_ready_h3_frame(&mut pending, bounded_limits)
                .expect("decode record preserved by aggregate limit"),
            Some(second)
        );
        assert!(pending.is_empty());
    }
}

#[test]
fn native_flow_registry_bounds_live_state_without_exhausting_on_churn() {
    let target = TargetAddr::Ip("127.0.0.1:53".parse().expect("target"));
    let mut registry = DatagramFlowRegistry::new(2);

    // The global allocator is monotonic, so long-lived sequential churn
    // coalesces into bounded seen-ID ranges while only live flows consume the
    // concurrency limit.
    for value in 0..100_u64 {
        let flow_id = DatagramFlowId(value);
        registry
            .apply_transitions(&[Frame::OpenDatagramFlow {
                flow_id,
                target: target.clone(),
            }])
            .expect("open one live flow");
        assert_eq!(registry.active.len(), 1);
        assert!(registry.seen_ranges.len() <= registry.max_seen_ranges);
        registry
            .apply_transitions(&[Frame::DatagramClose { flow_id }])
            .expect("reliably close flow");
        assert!(registry.active.is_empty());
    }

    // A delayed, previously unseen allocation can fill a sparse gap and
    // coalesce it; an actually closed identity cannot be reopened.
    for value in [102_u64, 104, 103] {
        let flow_id = DatagramFlowId(value);
        registry
            .apply_transitions(&[Frame::OpenDatagramFlow {
                flow_id,
                target: target.clone(),
            }])
            .expect("out-of-order unseen flow remains valid");
        registry
            .apply_transitions(&[Frame::DatagramClose { flow_id }])
            .expect("close sparse flow");
    }

    registry
        .apply_transitions(&[
            Frame::OpenDatagramFlow {
                flow_id: DatagramFlowId(200),
                target: target.clone(),
            },
            Frame::OpenDatagramFlow {
                flow_id: DatagramFlowId(201),
                target: target.clone(),
            },
        ])
        .expect("fill the live-flow bound");
    assert!(matches!(
        registry.apply_transitions(&[Frame::OpenDatagramFlow {
            flow_id: DatagramFlowId(202),
            target: target.clone(),
        }]),
        Err(QuicCarrierError::NativeDatagramFlowsExhausted)
    ));
    registry
        .apply_received_transitions(&[Frame::OpenDatagramFlow {
            flow_id: DatagramFlowId(202),
            target: target.clone(),
        }])
        .expect("receive side provisionally admits an over-cap OPEN");
    assert_eq!(
        registry.state(DatagramFlowId(202)),
        DatagramFlowState::Active,
    );
    assert_eq!(registry.active.len(), 3);
    registry
        .retain_refusal(DatagramFlowId(202), None, 2)
        .expect("runtime turns the excess candidate into a capacity refusal");
    assert_eq!(
        registry.state(DatagramFlowId(202)),
        DatagramFlowState::Refused,
    );
    assert_eq!(registry.active.len(), 2);
    registry
        .apply_transitions(&[Frame::DatagramClose {
            flow_id: DatagramFlowId(200),
        }])
        .expect("release one live flow");
    registry
        .apply_transitions(&[Frame::OpenDatagramFlow {
            flow_id: DatagramFlowId(203),
            target: target.clone(),
        }])
        .expect("a new flow may consume the released live slot");
    assert_eq!(
        registry.state(DatagramFlowId(203)),
        DatagramFlowState::Active,
    );

    registry
        .apply_transitions(&[Frame::OpenDatagramFlow {
            flow_id: DatagramFlowId(50),
            target,
        }])
        .expect("a terminal historical ID is ignored without failing the carrier");
    assert_eq!(
        registry.state(DatagramFlowId(50)),
        DatagramFlowState::Closed,
    );
}

#[test]
fn native_flow_registry_bounds_refusals_and_terminalizes_evictions() {
    let target = TargetAddr::Ip("127.0.0.1:53".parse().expect("target"));
    let refused = DatagramFlowId(1);
    let mut registry = DatagramFlowRegistry::new(2);
    registry
        .apply_transitions(&[Frame::OpenDatagramFlow {
            flow_id: refused,
            target: target.clone(),
        }])
        .expect("open refused flow");
    registry
        .retain_refusal(refused, None, 2)
        .expect("mark refusal");
    assert_eq!(registry.state(refused), DatagramFlowState::Refused);
    assert!(registry.active.is_empty());

    registry
        .apply_transitions(&[Frame::OpenDatagramFlow {
            flow_id: refused,
            target: target.clone(),
        }])
        .expect("accept an in-flight repeated open");
    registry
        .retain_refusal(refused, None, 2)
        .expect("repeat refusal");
    assert_eq!(registry.state(refused), DatagramFlowState::Refused);

    let accepted = DatagramFlowId(2);
    registry
        .apply_transitions(&[Frame::OpenDatagramFlow {
            flow_id: accepted,
            target,
        }])
        .expect("refusal does not consume live capacity");
    assert_eq!(registry.state(accepted), DatagramFlowState::Active);

    let mut retained = std::collections::VecDeque::from([refused]);
    for value in 3..103 {
        let flow_id = DatagramFlowId(value);
        registry
            .apply_transitions(&[Frame::OpenDatagramFlow {
                flow_id,
                target: TargetAddr::Ip("127.0.0.1:53".parse().expect("denied target")),
            }])
            .expect("open denial-flood flow");
        let evicted = if retained.len() == 2 {
            retained.pop_front()
        } else {
            None
        };
        retained.push_back(flow_id);
        registry
            .retain_refusal(flow_id, evicted, 2)
            .expect("retain bounded denial");
        assert!(registry.refused.len() <= 2);
        assert!(registry.refused_lru.len() <= 2);
    }
    assert_eq!(
        registry.state(refused),
        DatagramFlowState::Closed,
        "the runtime-selected LRU eviction becomes terminal in transport",
    );
    registry
        .apply_transitions(&[Frame::OpenDatagramFlow {
            flow_id: refused,
            target: TargetAddr::Ip("127.0.0.1:53".parse().expect("terminal target")),
        }])
        .expect("ignore a terminal duplicate without failing the carrier");
    assert_eq!(registry.state(refused), DatagramFlowState::Closed);
    assert_eq!(registry.active.len(), 1, "accepted capacity is unchanged");

    registry
        .apply_transitions(&[Frame::DatagramClose { flow_id: refused }])
        .expect("close refused flow");
    assert_eq!(registry.state(refused), DatagramFlowState::Closed);
}

#[test]
fn native_receive_registry_allows_later_burst_candidate_after_earlier_denials() {
    let target = TargetAddr::Ip("127.0.0.1:53".parse().expect("target"));
    let first = DatagramFlowId(10);
    let second = DatagramFlowId(11);
    let later = DatagramFlowId(12);
    let mut registry = DatagramFlowRegistry::new(2);
    registry
        .apply_received_transitions(&[
            Frame::OpenDatagramFlow {
                flow_id: first,
                target: target.clone(),
            },
            Frame::OpenDatagramFlow {
                flow_id: second,
                target: target.clone(),
            },
            Frame::OpenDatagramFlow {
                flow_id: later,
                target,
            },
        ])
        .expect("bounded reader queue provisionally admits the burst");
    assert_eq!(registry.active.len(), 3);
    registry
        .retain_refusal(first, None, 2)
        .expect("deny first candidate");
    registry
        .retain_refusal(second, None, 2)
        .expect("deny second candidate");
    assert_eq!(registry.active.len(), 1);
    assert_eq!(registry.state(later), DatagramFlowState::Active);
}

#[test]
fn native_ip_tunnel_registry_requires_one_open_ready_close_lifecycle() {
    let tunnel_id = IpTunnelId(7);
    let mut registry = IpTunnelRegistry::new();
    assert_eq!(registry.state(tunnel_id), IpTunnelState::Unknown);
    registry
        .apply_transitions(&[Frame::OpenIpTunnel { tunnel_id }])
        .expect("open tunnel association");
    assert_eq!(registry.state(tunnel_id), IpTunnelState::Open(tunnel_id));
    assert!(matches!(
        registry.apply_transitions(&[Frame::IpTunnelReady {
            tunnel_id: IpTunnelId(8),
            mtu: 1_400,
            addresses: Vec::new(),
        }]),
        Err(QuicCarrierError::InvalidNativeDatagram(_))
    ));
    registry
        .apply_transitions(&[Frame::IpTunnelReady {
            tunnel_id,
            mtu: 1_400,
            addresses: Vec::new(),
        }])
        .expect("ready matching tunnel association");
    assert_eq!(registry.state(tunnel_id), IpTunnelState::Ready(tunnel_id));
    registry
        .apply_transitions(&[Frame::IpTunnelClose {
            tunnel_id,
            reason: CloseReason::Normal,
        }])
        .expect("close matching tunnel association");
    assert_eq!(registry.state(tunnel_id), IpTunnelState::Closed(tunnel_id));
    assert!(matches!(
        registry.apply_transitions(&[Frame::OpenIpTunnel { tunnel_id }]),
        Err(QuicCarrierError::InvalidNativeDatagram(_))
    ));
}

#[tokio::test]
async fn quic_carrier_round_trips_product_frames() {
    let limits = CodecLimits::default();
    let mux_limits = MuxLimits::default();
    let server = Endpoint::bind_server(
        "127.0.0.1:0".parse().expect("server addr"),
        &crate::transport::encrypted::test_server_tls_config(),
        super::super::test_candidate_verifier(),
        mux_limits,
    )
    .await
    .expect("server endpoint");
    let server_addr = server.local_addr().expect("server local addr");
    let (client_done_tx, client_done_rx) = tokio::sync::oneshot::channel();
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await.expect("accepted connection");
        let (mut send, mut recv) = connection.accept_bi().await.expect("accepted stream");
        match read_frame(&mut recv, limits)
            .await
            .expect("server read ping")
        {
            Frame::Ping { nonce } => {
                write_frame(&mut send, &Frame::Pong { nonce }, limits)
                    .await
                    .expect("server write pong");
                finish_stream(&mut send)
                    .await
                    .expect("server finish stream");
            }
            frame => panic!("unexpected frame: {frame:?}"),
        }
        let _ = timeout(Duration::from_secs(5), client_done_rx).await;
    });

    let client = Endpoint::bind_client(
        "127.0.0.1:0".parse().expect("client addr"),
        &crate::transport::encrypted::test_client_tls_config(),
        super::super::test_candidate_selector(),
        mux_limits,
    )
    .await
    .expect("client endpoint");
    let connection = client.connect(server_addr).await.expect("client connect");
    let (mut send, mut recv) = connection.open_bi().await.expect("client stream");
    send.set_priority(1).expect("set QUIC stream priority");
    assert_eq!(send.priority().expect("read QUIC stream priority"), 1);
    write_frame(&mut send, &Frame::Ping { nonce: 42 }, limits)
        .await
        .expect("client write ping");
    assert_eq!(connection.congestion_metrics().pending_bytes, 0);
    assert!(!connection.is_closed());
    finish_stream(&mut send)
        .await
        .expect("client finish stream");
    let response = timeout(Duration::from_secs(5), read_frame(&mut recv, limits))
        .await
        .expect("response timeout")
        .expect("client read pong");
    assert_eq!(response, Frame::Pong { nonce: 42 });
    let finished = timeout(Duration::from_secs(5), read_frame(&mut recv, limits))
        .await
        .expect("stream finish timeout")
        .expect_err("server finished its QUIC send half");
    assert!(matches!(finished, QuicCarrierError::StreamFinished));
    let _ = client_done_tx.send(());

    server_task.await.expect("server task");
}

#[tokio::test]
async fn http_datagram_send_requires_an_open_request_send_side() {
    let limits = CodecLimits::default();
    let mux_limits = MuxLimits::default();
    let server = Endpoint::bind_server(
        "127.0.0.1:0".parse().expect("server addr"),
        &crate::transport::encrypted::test_server_tls_config(),
        super::super::test_candidate_verifier(),
        mux_limits,
    )
    .await
    .expect("server endpoint");
    let server_addr = server.local_addr().expect("server local addr");
    let (client_done_tx, client_done_rx) = tokio::sync::oneshot::channel();
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await.expect("accepted connection");
        let (_send, mut recv) = connection.accept_bi().await.expect("accepted stream");
        assert!(matches!(
            read_frame(&mut recv, limits)
                .await
                .expect("read datagram flow open"),
            Frame::OpenDatagramFlow {
                flow_id: DatagramFlowId(9),
                ..
            }
        ));
        let _ = timeout(Duration::from_secs(5), client_done_rx).await;
    });

    let client = Endpoint::bind_client(
        "127.0.0.1:0".parse().expect("client addr"),
        &crate::transport::encrypted::test_client_tls_config(),
        super::super::test_candidate_selector(),
        mux_limits,
    )
    .await
    .expect("client endpoint");
    let connection = client.connect(server_addr).await.expect("client connect");
    let (mut send, _recv) = connection.open_bi().await.expect("client stream");
    let target = TargetAddr::Ip("127.0.0.1:53".parse().expect("target"));
    write_frame(
        &mut send,
        &Frame::OpenDatagramFlow {
            flow_id: DatagramFlowId(9),
            target,
        },
        limits,
    )
    .await
    .expect("open native datagram flow");
    finish_stream(&mut send)
        .await
        .expect("finish request send side");

    assert!(matches!(
        write_frame(
            &mut send,
            &Frame::DatagramData {
                flow_id: DatagramFlowId(9),
                datagram_id: DatagramId(1),
                ttl_ms: 1_000,
                payload: Bytes::from_static(b"late"),
            },
            limits,
        )
        .await,
        Err(QuicCarrierError::H3StreamFinished)
    ));

    let _ = client_done_tx.send(());
    server_task.await.expect("server task");
}

#[tokio::test]
async fn request_receive_fin_retires_native_datagram_route() {
    let limits = CodecLimits::default();
    let mux_limits = MuxLimits::default();
    let server = Endpoint::bind_server(
        "127.0.0.1:0".parse().expect("server addr"),
        &crate::transport::encrypted::test_server_tls_config(),
        super::super::test_candidate_verifier(),
        mux_limits,
    )
    .await
    .expect("server endpoint");
    let server_addr = server.local_addr().expect("server local addr");
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await.expect("accepted connection");
        let (_send, mut recv) = connection.accept_bi().await.expect("accepted stream");
        assert_eq!(
            read_frame(&mut recv, limits).await.expect("read request"),
            Frame::Ping { nonce: 1 }
        );
        assert!(matches!(
            read_frame(&mut recv, limits).await,
            Err(QuicCarrierError::StreamFinished)
        ));
        assert_eq!(
            connection.native_datagram_routing_counts().0,
            0,
            "a closed H3 receive side must not retain its datagram route"
        );
    });

    let client = Endpoint::bind_client(
        "127.0.0.1:0".parse().expect("client addr"),
        &crate::transport::encrypted::test_client_tls_config(),
        super::super::test_candidate_selector(),
        mux_limits,
    )
    .await
    .expect("client endpoint");
    let connection = client.connect(server_addr).await.expect("client connect");
    let (mut send, _recv) = connection.open_bi().await.expect("client stream");
    write_frame(&mut send, &Frame::Ping { nonce: 1 }, limits)
        .await
        .expect("write request");
    finish_stream(&mut send).await.expect("finish request");

    server_task.await.expect("server task");
}

#[tokio::test]
async fn closed_request_datagrams_are_dropped_without_handoff_buffering() {
    let limits = CodecLimits::default();
    let mux_limits = MuxLimits::default();
    let server = Endpoint::bind_server(
        "127.0.0.1:0".parse().expect("server addr"),
        &crate::transport::encrypted::test_server_tls_config(),
        super::super::test_candidate_verifier(),
        mux_limits,
    )
    .await
    .expect("server endpoint");
    let server_addr = server.local_addr().expect("server local addr");
    let (route_closed_tx, route_closed_rx) = tokio::sync::oneshot::channel();
    let (late_sent_tx, late_sent_rx) = tokio::sync::oneshot::channel();
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await.expect("accepted connection");
        let (_send, mut recv) = connection.accept_bi().await.expect("accepted stream");
        assert!(matches!(
            read_frame(&mut recv, limits)
                .await
                .expect("read datagram flow open"),
            Frame::OpenDatagramFlow {
                flow_id: DatagramFlowId(9),
                ..
            }
        ));
        drop(recv);
        let before = connection.native_datagram_routing_counts();
        assert_eq!(before.0, 0);
        route_closed_tx.send(()).expect("publish closed route");
        late_sent_rx.await.expect("late datagram sent");

        let after = timeout(Duration::from_secs(1), async {
            loop {
                let after = connection.native_datagram_routing_counts();
                if after.1 > before.1 || after.2 > before.2 {
                    break after;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("late datagram routing outcome");
        assert_eq!(
            after.1, before.1,
            "a closed request must not re-enter the pre-request handoff queue"
        );
        assert!(after.2 > before.2);
    });

    let client = Endpoint::bind_client(
        "127.0.0.1:0".parse().expect("client addr"),
        &crate::transport::encrypted::test_client_tls_config(),
        super::super::test_candidate_selector(),
        mux_limits,
    )
    .await
    .expect("client endpoint");
    let connection = client.connect(server_addr).await.expect("client connect");
    let (mut send, _recv) = connection.open_bi().await.expect("client stream");
    write_frame(
        &mut send,
        &Frame::OpenDatagramFlow {
            flow_id: DatagramFlowId(9),
            target: TargetAddr::Ip("127.0.0.1:53".parse().expect("target")),
        },
        limits,
    )
    .await
    .expect("open native datagram flow");
    route_closed_rx.await.expect("server closed route");
    write_frame(
        &mut send,
        &Frame::DatagramData {
            flow_id: DatagramFlowId(9),
            datagram_id: DatagramId(1),
            ttl_ms: 1_000,
            payload: Bytes::from_static(b"late"),
        },
        limits,
    )
    .await
    .expect("send late native datagram");
    late_sent_tx.send(()).expect("publish late datagram");

    server_task.await.expect("server task");
}

#[tokio::test]
async fn stopped_quic_stream_write_keeps_the_shared_connection_available() {
    let limits = CodecLimits::default();
    let mux_limits = MuxLimits::default();
    let server = Endpoint::bind_server(
        "127.0.0.1:0".parse().expect("server addr"),
        &crate::transport::encrypted::test_server_tls_config(),
        super::super::test_candidate_verifier(),
        mux_limits,
    )
    .await
    .expect("server endpoint");
    let server_addr = server.local_addr().expect("server local addr");
    let (client_done_tx, client_done_rx) = tokio::sync::oneshot::channel();
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await.expect("accepted connection");
        let (_send, mut recv) = connection.accept_bi().await.expect("accepted stream");
        assert_eq!(
            read_frame(&mut recv, limits).await.expect("read opener"),
            Frame::Ping { nonce: 1 }
        );
        // Dropping an unread H3 receive half exercises the normal receiver
        // abandonment path and emits QUIC STOP_SENDING without relying on
        // h3-quinn's non-cancel-safe test-only stop wrapper.
        drop(recv);
        let (mut send, mut recv) = connection
            .accept_bi()
            .await
            .expect("accept replacement stream");
        assert_eq!(
            read_frame(&mut recv, limits)
                .await
                .expect("read replacement"),
            Frame::Ping { nonce: 2 }
        );
        write_frame(&mut send, &Frame::Pong { nonce: 2 }, limits)
            .await
            .expect("write replacement response");
        let _ = timeout(Duration::from_secs(5), client_done_rx).await;
    });

    let client = Endpoint::bind_client(
        "127.0.0.1:0".parse().expect("client addr"),
        &crate::transport::encrypted::test_client_tls_config(),
        super::super::test_candidate_selector(),
        mux_limits,
    )
    .await
    .expect("client endpoint");
    let connection = client.connect(server_addr).await.expect("client connect");
    let (mut send, _recv) = connection.open_bi().await.expect("client stream");
    write_frame(&mut send, &Frame::Ping { nonce: 1 }, limits)
        .await
        .expect("open carrier stream");
    assert_eq!(connection.congestion_metrics().pending_bytes, 0);
    assert!(!connection.is_closed());

    let err = timeout(Duration::from_secs(5), async {
        loop {
            match write_frame(&mut send, &Frame::Ping { nonce: 99 }, limits).await {
                Ok(()) => tokio::task::yield_now().await,
                Err(err) => break err,
            }
        }
    })
    .await
    .expect("HTTP/3 request cancellation timeout");
    assert!(matches!(err, QuicCarrierError::H3Stream(_)));
    let metrics = connection.congestion_metrics();
    assert_eq!(metrics.pending_bytes, 0);
    assert!(!connection.is_closed());

    let (mut replacement_send, mut replacement_recv) =
        connection.open_bi().await.expect("open replacement stream");
    write_frame(&mut replacement_send, &Frame::Ping { nonce: 2 }, limits)
        .await
        .expect("write replacement request");
    assert_eq!(
        read_frame(&mut replacement_recv, limits)
            .await
            .expect("read replacement response"),
        Frame::Pong { nonce: 2 }
    );

    let _ = client_done_tx.send(());
    server_task.await.expect("server task");
}

#[tokio::test]
async fn cancelled_h3_request_write_retires_only_that_request_stream() {
    let limits = CodecLimits::default();
    let mux_limits = MuxLimits {
        max_stream_window_bytes: 4 * 1024,
        max_repair_bytes: 4 * 1024,
        max_reorder_bytes: 4 * 1024,
        max_datagram_queue_bytes: 4 * 1024,
        max_path_flight_bytes: 4 * 1024,
        max_reliable_relay_chunk_bytes: 4 * 1024,
        ..MuxLimits::default()
    };
    let server = Endpoint::bind_server(
        "127.0.0.1:0".parse().expect("server addr"),
        &crate::transport::encrypted::test_server_tls_config(),
        super::super::test_candidate_verifier(),
        mux_limits,
    )
    .await
    .expect("server endpoint");
    let server_addr = server.local_addr().expect("server local addr");
    let (server_ready_tx, server_ready_rx) = tokio::sync::oneshot::channel();
    let (client_done_tx, client_done_rx) = tokio::sync::oneshot::channel();
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await.expect("accepted connection");
        let (_send, mut recv) = connection.accept_bi().await.expect("accepted stream");
        assert_eq!(
            read_frame(&mut recv, limits).await.expect("read opener"),
            Frame::Ping { nonce: 1 }
        );
        let _ = server_ready_tx.send(());
        let _recv = recv;
        let (mut send, mut recv) = connection
            .accept_bi()
            .await
            .expect("accept replacement stream");
        assert_eq!(
            read_frame(&mut recv, limits)
                .await
                .expect("read replacement"),
            Frame::Ping { nonce: 2 }
        );
        write_frame(&mut send, &Frame::Pong { nonce: 2 }, limits)
            .await
            .expect("write replacement response");
        let _ = timeout(Duration::from_secs(5), client_done_rx).await;
    });

    let client = Endpoint::bind_client(
        "127.0.0.1:0".parse().expect("client addr"),
        &crate::transport::encrypted::test_client_tls_config(),
        super::super::test_candidate_selector(),
        mux_limits,
    )
    .await
    .expect("client endpoint");
    let connection = client.connect(server_addr).await.expect("client connect");
    let (mut send, _recv) = connection.open_bi().await.expect("client stream");
    write_frame(&mut send, &Frame::Ping { nonce: 1 }, limits)
        .await
        .expect("open carrier stream");
    timeout(Duration::from_secs(5), server_ready_rx)
        .await
        .expect("server ready timeout")
        .expect("server ready sender");

    let payload_len = 256 * 1024;
    let write_task = tokio::spawn(async move {
        write_frame(
            &mut send,
            &Frame::StreamData {
                stream_id: StreamId(9),
                offset: 0,
                payload: Bytes::from(vec![0x5a; payload_len]),
            },
            limits,
        )
        .await
    });
    timeout(Duration::from_secs(5), async {
        loop {
            if connection.congestion_metrics().pending_bytes > 0 {
                break;
            }
            assert!(!write_task.is_finished(), "constrained write must block");
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("write did not enter backlog");

    write_task.abort();
    assert!(
        write_task
            .await
            .expect_err("aborted writer must be cancelled")
            .is_cancelled()
    );
    let metrics = connection.congestion_metrics();
    assert_eq!(metrics.pending_bytes, 0);
    assert!(!connection.is_closed());

    let (mut replacement_send, mut replacement_recv) =
        connection.open_bi().await.expect("open replacement stream");
    write_frame(&mut replacement_send, &Frame::Ping { nonce: 2 }, limits)
        .await
        .expect("write replacement request");
    assert_eq!(
        read_frame(&mut replacement_recv, limits)
            .await
            .expect("read replacement response"),
        Frame::Pong { nonce: 2 }
    );

    let _ = client_done_tx.send(());
    server_task.await.expect("server task");
}

#[tokio::test]
async fn quic_carrier_batches_multiple_product_frames_per_write() {
    let limits = CodecLimits::default();
    let mux_limits = MuxLimits::default();
    let server = Endpoint::bind_server(
        "127.0.0.1:0".parse().expect("server addr"),
        &crate::transport::encrypted::test_server_tls_config(),
        super::super::test_candidate_verifier(),
        mux_limits,
    )
    .await
    .expect("server endpoint");
    let server_addr = server.local_addr().expect("server local addr");
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await.expect("accepted connection");
        let (_send, mut recv) = connection.accept_bi().await.expect("accepted stream");
        assert_eq!(
            read_frame(&mut recv, limits).await.expect("read first"),
            Frame::Ping { nonce: 1 }
        );
        assert_eq!(
            read_frame(&mut recv, limits).await.expect("read second"),
            Frame::Pong { nonce: 2 }
        );
    });

    let client = Endpoint::bind_client(
        "127.0.0.1:0".parse().expect("client addr"),
        &crate::transport::encrypted::test_client_tls_config(),
        super::super::test_candidate_selector(),
        mux_limits,
    )
    .await
    .expect("client endpoint");
    let connection = client.connect(server_addr).await.expect("client connect");
    let (mut send, _recv) = connection.open_bi().await.expect("client stream");
    write_frames(
        &mut send,
        &[Frame::Ping { nonce: 1 }, Frame::Pong { nonce: 2 }],
        limits,
    )
    .await
    .expect("client write batch");
    finish_stream(&mut send)
        .await
        .expect("client finish stream");
    timeout(Duration::from_secs(5), server_task)
        .await
        .expect("server task timeout")
        .expect("server task");
}

#[tokio::test]
async fn native_http_datagram_fragments_preserve_identity_without_reliable_hol() {
    let limits = CodecLimits::default();
    let mux_limits = MuxLimits::default();
    let flow_id = DatagramFlowId(7);
    let request_id = DatagramId(11);
    let response_id = DatagramId(12);
    let target = TargetAddr::Ip("127.0.0.1:53".parse().expect("target"));
    let request_payload = Bytes::from(vec![0x5a; 60_000]);
    let response_payload = Bytes::from_static(b"native response");
    let (client_done_tx, client_done_rx) = tokio::sync::oneshot::channel();

    let server = Endpoint::bind_server(
        "127.0.0.1:0".parse().expect("server addr"),
        &crate::transport::encrypted::test_server_tls_config(),
        super::super::test_candidate_verifier(),
        mux_limits,
    )
    .await
    .expect("server endpoint");
    let server_addr = server.local_addr().expect("server local addr");
    let expected_request = request_payload.clone();
    let expected_response = response_payload.clone();
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await.expect("accepted connection");
        let (mut send, mut recv) = connection.accept_bi().await.expect("accepted request");
        assert_eq!(
            read_frame(&mut recv, limits)
                .await
                .expect("read reliable open"),
            Frame::OpenDatagramFlow { flow_id, target }
        );

        let mut saw_native = false;
        let mut saw_reliable_ping = false;
        while !saw_native || !saw_reliable_ping {
            match read_frame(&mut recv, limits)
                .await
                .expect("read mixed traffic")
            {
                Frame::DatagramData {
                    flow_id: received_flow,
                    datagram_id,
                    ttl_ms,
                    payload,
                } => {
                    assert_eq!(received_flow, flow_id);
                    assert_eq!(datagram_id, request_id);
                    assert!(ttl_ms > 0 && ttl_ms <= 5_000);
                    assert_eq!(payload, expected_request);
                    saw_native = true;
                }
                Frame::Ping { nonce: 99 } => saw_reliable_ping = true,
                frame => panic!("unexpected mixed HTTP/3 frame: {frame:?}"),
            }
        }

        write_frame(
            &mut send,
            &Frame::DatagramData {
                flow_id,
                datagram_id: response_id,
                ttl_ms: 5_000,
                payload: expected_response,
            },
            limits,
        )
        .await
        .expect("write native response");
        let _ = timeout(Duration::from_secs(5), client_done_rx).await;
    });

    let client = Endpoint::bind_client(
        "127.0.0.1:0".parse().expect("client addr"),
        &crate::transport::encrypted::test_client_tls_config(),
        super::super::test_candidate_selector(),
        mux_limits,
    )
    .await
    .expect("client endpoint");
    let connection = client.connect(server_addr).await.expect("client connect");
    let (mut send, mut recv) = connection.open_bi().await.expect("client request");
    write_frames(
        &mut send,
        &[
            Frame::OpenDatagramFlow {
                flow_id,
                target: TargetAddr::Ip("127.0.0.1:53".parse().expect("target")),
            },
            Frame::DatagramData {
                flow_id,
                datagram_id: request_id,
                ttl_ms: 5_000,
                payload: request_payload,
            },
            Frame::Ping { nonce: 99 },
        ],
        limits,
    )
    .await
    .expect("write reliable open, native payload, and independent control");

    match timeout(Duration::from_secs(5), read_frame(&mut recv, limits))
        .await
        .expect("native response timeout")
        .expect("native response")
    {
        Frame::DatagramData {
            flow_id: received_flow,
            datagram_id,
            ttl_ms,
            payload,
        } => {
            assert_eq!(received_flow, flow_id);
            assert_eq!(datagram_id, response_id);
            assert!(ttl_ms > 0 && ttl_ms <= 5_000);
            assert_eq!(payload, response_payload);
        }
        frame => panic!("unexpected native response frame: {frame:?}"),
    }
    let _ = client_done_tx.send(());
    timeout(Duration::from_secs(5), server_task)
        .await
        .expect("server task timeout")
        .expect("server task");
}

#[tokio::test]
async fn native_ip_packets_require_ready_and_preserve_fragmented_identity() {
    let limits = CodecLimits::default();
    let mux_limits = MuxLimits::default();
    let tunnel_id = IpTunnelId(17);
    let request_id = IpPacketId(31);
    let response_id = IpPacketId(32);
    let request_payload = Bytes::from(vec![0x45; 60_000]);
    let response_payload = Bytes::from(vec![0x60; 32_000]);

    let server = Endpoint::bind_server(
        "127.0.0.1:0".parse().expect("server addr"),
        &crate::transport::encrypted::test_server_tls_config(),
        super::super::test_candidate_verifier(),
        mux_limits,
    )
    .await
    .expect("server endpoint");
    let server_addr = server.local_addr().expect("server local addr");
    let expected_request = request_payload.clone();
    let expected_response = response_payload.clone();
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await.expect("accepted connection");
        let (mut send, mut recv) = connection.accept_bi().await.expect("accepted request");
        assert_eq!(
            read_frame(&mut recv, limits)
                .await
                .expect("read reliable tunnel open"),
            Frame::OpenIpTunnel { tunnel_id }
        );
        write_frame(
            &mut send,
            &Frame::IpTunnelReady {
                tunnel_id,
                mtu: 65_535,
                addresses: vec!["10.0.0.2".parse().expect("tunnel address")],
            },
            limits,
        )
        .await
        .expect("write reliable tunnel ready");
        assert_eq!(
            read_frame(&mut recv, limits)
                .await
                .expect("read native IP packet"),
            Frame::IpPacket {
                tunnel_id,
                packet_id: request_id,
                payload: expected_request,
            }
        );
        write_frame(
            &mut send,
            &Frame::IpPacket {
                tunnel_id,
                packet_id: response_id,
                payload: expected_response,
            },
            limits,
        )
        .await
        .expect("write native IP response");
        assert_eq!(
            read_frame(&mut recv, limits)
                .await
                .expect("read reliable tunnel close"),
            Frame::IpTunnelClose {
                tunnel_id,
                reason: CloseReason::Normal,
            }
        );
        finish_stream(&mut send)
            .await
            .expect("finish tunnel response");
    });

    let client = Endpoint::bind_client(
        "127.0.0.1:0".parse().expect("client addr"),
        &crate::transport::encrypted::test_client_tls_config(),
        super::super::test_candidate_selector(),
        mux_limits,
    )
    .await
    .expect("client endpoint");
    let connection = client.connect(server_addr).await.expect("client connect");
    let (mut send, mut recv) = connection.open_bi().await.expect("client request");
    write_frame(&mut send, &Frame::OpenIpTunnel { tunnel_id }, limits)
        .await
        .expect("write reliable tunnel open");
    assert!(matches!(
        write_frame(
            &mut send,
            &Frame::IpPacket {
                tunnel_id,
                packet_id: request_id,
                payload: request_payload.clone(),
            },
            limits,
        )
        .await,
        Err(QuicCarrierError::InvalidNativeDatagram(_))
    ));
    assert_eq!(
        read_frame(&mut recv, limits)
            .await
            .expect("read reliable tunnel ready"),
        Frame::IpTunnelReady {
            tunnel_id,
            mtu: 65_535,
            addresses: vec!["10.0.0.2".parse().expect("tunnel address")],
        }
    );
    write_frame(
        &mut send,
        &Frame::IpPacket {
            tunnel_id,
            packet_id: request_id,
            payload: request_payload,
        },
        limits,
    )
    .await
    .expect("write native IP request");
    assert_eq!(
        read_frame(&mut recv, limits)
            .await
            .expect("read native IP response"),
        Frame::IpPacket {
            tunnel_id,
            packet_id: response_id,
            payload: response_payload,
        }
    );
    write_frame(
        &mut send,
        &Frame::IpTunnelClose {
            tunnel_id,
            reason: CloseReason::Normal,
        },
        limits,
    )
    .await
    .expect("write reliable tunnel close");
    assert!(matches!(
        write_frame(
            &mut send,
            &Frame::IpPacket {
                tunnel_id,
                packet_id: IpPacketId(33),
                payload: Bytes::from_static(b"late"),
            },
            limits,
        )
        .await,
        Err(QuicCarrierError::InvalidNativeDatagram(_))
    ));
    finish_stream(&mut send)
        .await
        .expect("finish tunnel request");
    timeout(Duration::from_secs(5), server_task)
        .await
        .expect("server task timeout")
        .expect("server task");
}
