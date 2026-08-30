//! Wire-codec contract tests for the current clean-break wire version.

use super::*;

fn round_trip(frame: Frame) {
    let encoded = encode_frame(&frame, CodecLimits::default()).expect("encode");
    let decoded = decode_frame_bytes(Bytes::from(encoded), CodecLimits::default()).expect("decode");
    assert_eq!(decoded, frame);
}

fn peer_status_metrics(
    path_id: u16,
    underlay: UnderlayProtocol,
    direction: PathMetricDirection,
) -> PathMetrics {
    PathMetrics {
        path_id: PathId(path_id),
        underlay,
        direction,
        metric_epoch: 1_735_689_600,
        metric_age_us: 5_000,
        rate_valid_for_us: 100_000,
        rate_observed: true,
        srtt_us: 25_000,
        rttvar_us: 3_000,
        jitter_us: 1_200,
        delivery_rate_bps: 125_000_000,
        pacing_rate_bps: 150_000_000,
        pacing_rate_observed: true,
        loss_ppm: 1_500,
        ecn_ppm: 25,
        loss_observed: true,
        ecn_observed: true,
        bytes_in_flight_observed: true,
        queue_observed: true,
        bytes_in_flight: 64 * 1024,
        queue_bytes: 16 * 1024,
        inflight_limit_bytes: 512 * 1024,
        inflight_hi_bytes: 768 * 1024,
        confidence_ppm: 875_000,
        app_limited: false,
        has_ack_derived_data_sample: true,
        data_sample_count: 9,
        data_sample_bytes: 512 * 1024,
    }
}

fn peer_path_status(state: PeerPathState, usage: PathUsage) -> PeerPathStatus {
    PeerPathStatus {
        state,
        usage,
        metrics: peer_status_metrics(
            7,
            UnderlayProtocol::Tcp,
            PathMetricDirection::ClientToServer,
        ),
    }
}

#[test]
fn stream_frames_round_trip() {
    round_trip(Frame::OpenStream {
        stream_id: StreamId(7),
        target: TargetAddr::Domain {
            host: "example.com".to_string(),
            port: 443,
        },
        demand: StreamDemandHint::Throughput,
    });
    round_trip(Frame::StreamData {
        stream_id: StreamId(7),
        offset: 1024,
        payload: Bytes::from_static(b"hello"),
    });
    round_trip(Frame::StreamAck {
        stream_id: StreamId(7),
        complete: true,
        ranges: vec![
            OffsetRange::new(0, 5).expect("range"),
            OffsetRange::new(10, 12).expect("range"),
        ],
    });
    round_trip(Frame::StreamRequalifyData {
        stream_id: StreamId(7),
        probe_id: 19,
        offset: 1024,
        payload: Bytes::from_static(b"probe-data"),
    });
    round_trip(Frame::StreamRequalifyAck {
        stream_id: StreamId(7),
        probe_id: 19,
        offset: 1024,
        payload_bytes: 10,
    });
    round_trip(Frame::StreamFin {
        stream_id: StreamId(7),
        final_offset: 1234,
    });
    round_trip(Frame::StreamDetach {
        stream_id: StreamId(7),
    });
}

#[test]
fn open_stream_v9_has_no_attachment_role_field() {
    let frame = Frame::OpenStream {
        stream_id: StreamId(0x0102_0304_0506_0708),
        target: TargetAddr::Ip("192.0.2.1:443".parse().expect("addr")),
        demand: StreamDemandHint::Throughput,
    };

    let encoded = encode_frame(&frame, CodecLimits::default()).expect("encode");
    assert_eq!(
        encoded,
        vec![
            b'M', b'P', b'T', b'F', 9, 7, 0, 0, 0, 16, 1, 2, 3, 4, 5, 6, 7, 8, 2, 192, 0, 2, 1, 1,
            187, 2,
        ]
    );
    assert_eq!(
        decode_frame_bytes(Bytes::from(encoded), CodecLimits::default()).expect("decode"),
        frame
    );
}

#[test]
fn path_usage_round_trips_with_sequence_bounds() {
    for (sequence, usage) in [(0, PathUsage::Available), (u64::MAX, PathUsage::Backup)] {
        round_trip(Frame::PathStatus {
            path_id: PathId(u16::MAX),
            sequence,
            usage,
        });
    }
}

#[test]
fn decoder_rejects_unknown_path_usage() {
    let mut encoded = encode_frame(
        &Frame::PathStatus {
            path_id: PathId(3),
            sequence: 17,
            usage: PathUsage::Available,
        },
        CodecLimits::default(),
    )
    .expect("encode");
    assert_eq!(encoded.len(), FRAME_HEADER_LEN + 11);
    *encoded.last_mut().expect("usage byte") = 2;

    assert_eq!(
        decode_frame_bytes(Bytes::from(encoded), CodecLimits::default()),
        Err(CodecError::InvalidEnum)
    );
}

#[test]
fn decoder_rejects_v8_frames_after_v9_wire_cut() {
    let mut encoded =
        encode_frame(&Frame::Ping { nonce: 42 }, CodecLimits::default()).expect("encode");
    encoded[4] = 8;

    assert_eq!(
        decode_frame_bytes(Bytes::from(encoded), CodecLimits::default()),
        Err(CodecError::UnsupportedVersion(8))
    );
}

#[test]
fn path_metrics_v9_presence_bits_distinguish_absence_from_observed_zero() {
    let mut absent = peer_status_metrics(
        7,
        UnderlayProtocol::Tcp,
        PathMetricDirection::ServerToClient,
    );
    absent.bytes_in_flight = 0;
    absent.queue_bytes = 0;
    absent.bytes_in_flight_observed = false;
    absent.queue_observed = false;

    let absent_wire = encode_frame(
        &Frame::PathMetrics { metrics: absent },
        CodecLimits::default(),
    )
    .expect("encode absent native counters");
    assert_eq!(
        absent_wire.len(),
        FRAME_HEADER_LEN + PATH_METRICS_ENCODED_LEN
    );
    assert_eq!(absent_wire[4], 9);
    assert_eq!(
        &absent_wire[FRAME_HEADER_LEN + 64..FRAME_HEADER_LEN + 66],
        &[0, 0]
    );
    assert_eq!(
        decode_frame_bytes(Bytes::from(absent_wire), CodecLimits::default()).expect("decode"),
        Frame::PathMetrics { metrics: absent }
    );

    let observed_zero = PathMetrics {
        bytes_in_flight_observed: true,
        queue_observed: true,
        ..absent
    };
    let observed_wire = encode_frame(
        &Frame::PathMetrics {
            metrics: observed_zero,
        },
        CodecLimits::default(),
    )
    .expect("encode observed zero native counters");
    assert_eq!(
        &observed_wire[FRAME_HEADER_LEN + 64..FRAME_HEADER_LEN + 66],
        &[1, 1]
    );
    for presence_offset in [24, 53, 62, 63, 64, 65] {
        let mut invalid = observed_wire.clone();
        invalid[FRAME_HEADER_LEN + presence_offset] = 2;
        assert_eq!(
            decode_frame_bytes(Bytes::from(invalid), CodecLimits::default()),
            Err(CodecError::InvalidEnum),
            "presence byte at fixed offset {presence_offset} must be canonical boolean"
        );
    }
    assert_eq!(
        decode_frame_bytes(Bytes::from(observed_wire), CodecLimits::default()).expect("decode"),
        Frame::PathMetrics {
            metrics: observed_zero
        }
    );
}

#[test]
fn path_metrics_v9_rate_authority_budget_is_bounded_canonically() {
    let mut metrics = peer_status_metrics(
        7,
        UnderlayProtocol::Udp,
        PathMetricDirection::ServerToClient,
    );
    metrics.rate_valid_for_us = PATH_METRICS_MAX_RATE_VALID_FOR_US;
    let exact_wire = encode_frame(&Frame::PathMetrics { metrics }, CodecLimits::default())
        .expect("encode exact maximum rate authority");
    assert_eq!(
        decode_frame_bytes(Bytes::from(exact_wire.clone()), CodecLimits::default())
            .expect("decode exact maximum rate authority"),
        Frame::PathMetrics { metrics }
    );

    let mut hostile_wire = exact_wire;
    hostile_wire[FRAME_HEADER_LEN + 16..FRAME_HEADER_LEN + 24]
        .copy_from_slice(&(PATH_METRICS_MAX_RATE_VALID_FOR_US + 1).to_be_bytes());
    assert_eq!(
        decode_frame_bytes(Bytes::from(hostile_wire), CodecLimits::default()),
        Err(CodecError::InvalidPathMetrics)
    );

    let local_noncanonical = PathMetrics {
        rate_valid_for_us: u64::MAX,
        ..metrics
    };
    let capped_wire = encode_frame(
        &Frame::PathMetrics {
            metrics: local_noncanonical,
        },
        CodecLimits::default(),
    )
    .expect("local encoder caps a noncanonical authority budget");
    assert_eq!(
        decode_frame_bytes(Bytes::from(capped_wire), CodecLimits::default())
            .expect("decode capped local rate authority"),
        Frame::PathMetrics { metrics }
    );

    for malformed in [
        PathMetrics {
            rate_valid_for_us: 1,
            rate_observed: false,
            pacing_rate_observed: false,
            ..metrics
        },
        PathMetrics {
            rate_valid_for_us: 0,
            rate_observed: false,
            pacing_rate_observed: true,
            ..metrics
        },
    ] {
        let invalid_wire = encode_frame(
            &Frame::PathMetrics { metrics: malformed },
            CodecLimits::default(),
        )
        .expect("encode malformed provenance for decoder validation");
        assert_eq!(
            decode_frame_bytes(Bytes::from(invalid_wire), CodecLimits::default()),
            Err(CodecError::InvalidPathMetrics)
        );
    }
}

#[test]
fn ip_tunnel_frames_round_trip_without_transport_semantics() {
    let tunnel_id = IpTunnelId(17);
    round_trip(Frame::OpenIpTunnel { tunnel_id });
    round_trip(Frame::IpTunnelReady {
        tunnel_id,
        mtu: 1420,
        addresses: vec![
            "10.88.0.2".parse().expect("IPv4 address"),
            "fd88::2".parse().expect("IPv6 address"),
        ],
    });
    round_trip(Frame::IpPacket {
        tunnel_id,
        packet_id: IpPacketId(23),
        payload: Bytes::from_static(&[
            0x45, 0, 0, 20, 0, 0, 0, 0, 64, 59, 0, 0, 10, 88, 0, 2, 10, 88, 0, 1,
        ]),
    });
    round_trip(Frame::IpTunnelClose {
        tunnel_id,
        reason: CloseReason::Normal,
    });
}

#[test]
fn ip_tunnel_ready_rejects_duplicate_families_and_invalid_mtu() {
    let empty = Frame::IpTunnelReady {
        tunnel_id: IpTunnelId(1),
        mtu: 1500,
        addresses: Vec::new(),
    };
    assert_eq!(
        encode_frame(&empty, CodecLimits::default()),
        Err(CodecError::InvalidIpTunnel)
    );

    let duplicate = Frame::IpTunnelReady {
        tunnel_id: IpTunnelId(1),
        mtu: 1500,
        addresses: vec![
            "10.88.0.2".parse().expect("IPv4 address"),
            "10.88.0.3".parse().expect("IPv4 address"),
        ],
    };
    assert_eq!(
        encode_frame(&duplicate, CodecLimits::default()),
        Err(CodecError::InvalidIpTunnel)
    );
    let ipv6_small = Frame::IpTunnelReady {
        tunnel_id: IpTunnelId(1),
        mtu: 1279,
        addresses: vec!["fd88::2".parse().expect("IPv6 address")],
    };
    assert_eq!(
        encode_frame(&ipv6_small, CodecLimits::default()),
        Err(CodecError::InvalidIpTunnel)
    );
}

#[test]
fn owned_decode_slices_payloads_without_copying() {
    let data = Frame::StreamData {
        stream_id: StreamId(7),
        offset: 1024,
        payload: Bytes::from_static(b"zero-copy-payload"),
    };
    let encoded = Bytes::from(encode_frame(&data, CodecLimits::default()).expect("encode"));
    let source_start = encoded.as_ptr() as usize;
    let source_end = source_start + encoded.len();

    let decoded = decode_frames_bytes(encoded, CodecLimits::default()).expect("decode");
    let [Frame::StreamData { payload, .. }] = decoded.as_slice() else {
        panic!("expected one stream data frame");
    };
    let payload_start = payload.as_ptr() as usize;
    let payload_end = payload_start + payload.len();

    assert!(payload_start >= source_start);
    assert!(payload_end <= source_end);
    assert_eq!(payload.as_ref(), b"zero-copy-payload");
}

#[test]
fn owned_single_frame_decode_slices_payload_without_copying() {
    let data = Frame::StreamData {
        stream_id: StreamId(7),
        offset: 1024,
        payload: Bytes::from_static(b"quic-zero-copy-payload"),
    };
    let encoded = Bytes::from(encode_frame(&data, CodecLimits::default()).expect("encode"));
    let source_start = encoded.as_ptr() as usize;
    let source_end = source_start + encoded.len();

    let decoded = decode_frame_bytes(encoded, CodecLimits::default()).expect("decode");
    let Frame::StreamData { payload, .. } = decoded else {
        panic!("expected stream data frame");
    };
    let payload_start = payload.as_ptr() as usize;
    let payload_end = payload_start + payload.len();

    assert!(payload_start >= source_start);
    assert!(payload_end <= source_end);
    assert_eq!(payload.as_ref(), b"quic-zero-copy-payload");
}

#[test]
fn stream_data_encoder_rejects_offset_extent_overflow() {
    assert_eq!(
        encode_frame(
            &Frame::StreamData {
                stream_id: StreamId(7),
                offset: u64::MAX,
                payload: Bytes::from_static(b"x"),
            },
            CodecLimits::default(),
        ),
        Err(CodecError::LengthOverflow)
    );
}

#[test]
fn stream_data_decoder_rejects_offset_extent_overflow() {
    let mut encoded = encode_frame(
        &Frame::StreamData {
            stream_id: StreamId(7),
            offset: 0,
            payload: Bytes::from_static(b"x"),
        },
        CodecLimits::default(),
    )
    .expect("encode canonical stream data");
    let offset_start = FRAME_HEADER_LEN + std::mem::size_of::<u64>();
    encoded[offset_start..offset_start + std::mem::size_of::<u64>()]
        .copy_from_slice(&u64::MAX.to_be_bytes());

    assert_eq!(
        decode_frame_bytes(Bytes::from(encoded), CodecLimits::default()),
        Err(CodecError::LengthOverflow)
    );
}

#[test]
fn datagram_flow_uses_compact_flow_id_after_open() {
    round_trip(Frame::OpenDatagramFlow {
        flow_id: DatagramFlowId(9),
        target: TargetAddr::Ip("192.0.2.10:53".parse().expect("addr")),
    });
    let data = Frame::DatagramData {
        flow_id: DatagramFlowId(9),
        datagram_id: DatagramId(11),
        ttl_ms: 250,
        payload: Bytes::from_static(b"dns"),
    };
    let encoded = encode_frame(&data, CodecLimits::default()).expect("encode");
    assert!(encoded.len() < 40);
    assert_eq!(
        decode_frame_bytes(Bytes::from(encoded), CodecLimits::default()).expect("decode"),
        data
    );
}

#[test]
fn encoder_rejects_zero_port_ip_targets() {
    for target in [
        TargetAddr::Ip("192.0.2.1:0".parse().expect("IPv4 target")),
        TargetAddr::Ip("[2001:db8::1]:0".parse().expect("IPv6 target")),
    ] {
        for frame in [
            Frame::OpenStream {
                stream_id: StreamId(7),
                target: target.clone(),
                demand: StreamDemandHint::Throughput,
            },
            Frame::OpenDatagramFlow {
                flow_id: DatagramFlowId(9),
                target: target.clone(),
            },
        ] {
            assert_eq!(
                encode_frame(&frame, CodecLimits::default()),
                Err(CodecError::InvalidPort)
            );
        }
    }
}

#[test]
fn control_frames_round_trip_auth_and_path_metrics() {
    let nonce = AuthNonce([7; 16]);
    let auth_tag = AuthTag([9; 32]);

    round_trip(Frame::SessionAuth {
        session_id: SessionId(42),
        credential_id: "home-client".to_string(),
        nonce,
        issued_at_unix_secs: 1_735_689_600,
        auth_tag,
    });
    round_trip(Frame::PathJoin {
        session_id: SessionId(42),
        credential_id: "home-client".to_string(),
        path_id: PathId(3),
        underlay: UnderlayProtocol::Udp,
        nonce,
        issued_at_unix_secs: 1_735_689_600,
        auth_tag,
    });
    round_trip(Frame::PathStatus {
        path_id: PathId(3),
        sequence: 17,
        usage: PathUsage::Backup,
    });
    round_trip(Frame::PathDrain { path_id: PathId(3) });
    round_trip(Frame::PathClose {
        path_id: PathId(3),
        reason: CloseReason::ProtocolError,
    });
    round_trip(Frame::PathProofData {
        path_id: PathId(3),
        proof_id: 100,
        payload: Bytes::from_static(b"path-proof"),
    });
    round_trip(Frame::PathProofAck {
        path_id: PathId(3),
        proof_id: 100,
        payload_bytes: 10,
    });
    round_trip(Frame::DatagramFeedback {
        flow_id: DatagramFlowId(10),
        received: vec![
            OffsetRange::new(1, 2).expect("range"),
            OffsetRange::new(8, 12).expect("range"),
        ],
    });
    round_trip(Frame::PathMetrics {
        metrics: PathMetrics {
            path_id: PathId(3),
            underlay: UnderlayProtocol::Udp,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: 1_735_689_600,
            metric_age_us: 5_000,
            rate_valid_for_us: 100_000,
            rate_observed: true,
            srtt_us: 25_000,
            rttvar_us: 3_000,
            jitter_us: 1_200,
            delivery_rate_bps: 125_000_000,
            pacing_rate_bps: 150_000_000,
            pacing_rate_observed: true,
            loss_ppm: 1_500,
            ecn_ppm: 25,
            loss_observed: true,
            ecn_observed: true,
            bytes_in_flight_observed: true,
            queue_observed: true,
            bytes_in_flight: 64 * 1024,
            queue_bytes: 16 * 1024,
            inflight_limit_bytes: 512 * 1024,
            inflight_hi_bytes: 768 * 1024,
            confidence_ppm: 875_000,
            app_limited: false,
            has_ack_derived_data_sample: true,
            data_sample_count: 9,
            data_sample_bytes: 512 * 1024,
        },
    });
}

#[test]
fn credential_ids_are_bounded_canonical_wire_names() {
    let frame = Frame::SessionAuth {
        session_id: SessionId(42),
        credential_id: "home-client".to_string(),
        nonce: AuthNonce([7; 16]),
        issued_at_unix_secs: 1_735_689_600,
        auth_tag: AuthTag([9; 32]),
    };
    let mut encoded = encode_frame(&frame, CodecLimits::default()).expect("encode");
    let credential_start = FRAME_HEADER_LEN + std::mem::size_of::<u64>() + 1;
    encoded[credential_start] = b'H';
    assert_eq!(
        decode_frame_bytes(Bytes::from(encoded), CodecLimits::default()),
        Err(CodecError::InvalidCredentialId)
    );

    for credential_id in [String::new(), "a".repeat(65), "home/client".to_string()] {
        assert_eq!(
            encode_frame(
                &Frame::SessionAuth {
                    session_id: SessionId(42),
                    credential_id,
                    nonce: AuthNonce([7; 16]),
                    issued_at_unix_secs: 1_735_689_600,
                    auth_tag: AuthTag([9; 32]),
                },
                CodecLimits::default(),
            ),
            Err(CodecError::InvalidCredentialId)
        );
    }
}

#[test]
fn path_capacity_protocol_round_trips_without_product_stream_identity() {
    round_trip(Frame::PathCapacityData {
        path_id: PathId(7),
        measurement_id: 42,
        payload: Bytes::from_static(b"native-quic-capacity-sample"),
    });
    round_trip(Frame::PathCapacityFinish {
        path_id: PathId(7),
        measurement_id: 42,
        payload_bytes: 27,
    });
    round_trip(Frame::PathCapacityReceipt {
        path_id: PathId(7),
        measurement_id: 42,
        received_payload_bytes: 27,
    });
}

#[test]
fn path_join_has_stable_canonical_layout() {
    let frame = Frame::PathJoin {
        session_id: SessionId(0x0102_0304_0506_0708),
        credential_id: "a".to_string(),
        path_id: PathId(0x1112),
        underlay: UnderlayProtocol::Udp,
        nonce: AuthNonce([3; 16]),
        issued_at_unix_secs: 0x2122_2324_2526_2728,
        auth_tag: AuthTag([4; 32]),
    };
    let encoded = encode_frame(&frame, CodecLimits::default()).expect("encode");

    assert_eq!(&encoded[..4], b"MPTF");
    assert_eq!(encoded[4], VERSION);
    assert_eq!(&encoded[5..10], &[4, 0, 0, 0, 69]);
    assert_eq!(&encoded[10..18], &0x0102_0304_0506_0708_u64.to_be_bytes());
    assert_eq!(&encoded[18..20], &[1, b'a']);
    assert_eq!(&encoded[20..22], &0x1112_u16.to_be_bytes());
    assert_eq!(encoded[22], 2);
    assert_eq!(&encoded[23..39], &[3; 16]);
    assert_eq!(&encoded[39..47], &0x2122_2324_2526_2728_u64.to_be_bytes());
    assert_eq!(&encoded[47..], &[4; 32]);
    assert_eq!(
        decode_frame_bytes(Bytes::from(encoded.clone()), CodecLimits::default()).expect("decode"),
        frame
    );
}

#[test]
fn peer_status_frames_round_trip_with_bounded_fixed_entries() {
    round_trip(Frame::PeerStatusRequest {
        request_id: u64::MAX,
    });

    let response = Frame::PeerStatusResponse {
        request_id: 42,
        code: PeerStatusCode::Ok,
        paths: vec![
            peer_path_status(PeerPathState::Active, PathUsage::Available),
            peer_path_status(PeerPathState::Suspect, PathUsage::Backup),
            peer_path_status(PeerPathState::Draining, PathUsage::Available),
            peer_path_status(PeerPathState::Failed, PathUsage::Backup),
        ],
    };
    let encoded = encode_frame(&response, CodecLimits::default()).expect("encode");
    assert_eq!(encoded[5], 37);
    assert_eq!(encoded.len(), FRAME_HEADER_LEN + 11 + 4 * 118);
    assert_eq!(
        decode_frame_bytes(Bytes::from(encoded), CodecLimits::default()).expect("decode"),
        response
    );

    for code in [PeerStatusCode::Disabled, PeerStatusCode::Unavailable] {
        round_trip(Frame::PeerStatusResponse {
            request_id: 43,
            code,
            paths: Vec::new(),
        });
    }

    let request = encode_frame(
        &Frame::PeerStatusRequest { request_id: 44 },
        CodecLimits::default(),
    )
    .expect("encode");
    assert_eq!(request[5], 36);
}

#[test]
fn peer_status_encoder_rejects_paths_on_non_ok_response() {
    for code in [PeerStatusCode::Disabled, PeerStatusCode::Unavailable] {
        assert_eq!(
            encode_frame(
                &Frame::PeerStatusResponse {
                    request_id: 42,
                    code,
                    paths: vec![peer_path_status(
                        PeerPathState::Active,
                        PathUsage::Available,
                    )],
                },
                CodecLimits::default(),
            ),
            Err(CodecError::InvalidPeerStatus)
        );
    }
}

#[test]
fn peer_status_decoder_rejects_paths_on_non_ok_response() {
    let encoded = encode_frame(
        &Frame::PeerStatusResponse {
            request_id: 42,
            code: PeerStatusCode::Ok,
            paths: vec![peer_path_status(
                PeerPathState::Active,
                PathUsage::Available,
            )],
        },
        CodecLimits::default(),
    )
    .expect("encode canonical peer status");

    for non_ok_code in [1, 2] {
        let mut malformed = encoded.clone();
        malformed[FRAME_HEADER_LEN + 8] = non_ok_code;
        assert_eq!(
            decode_frame_bytes(Bytes::from(malformed), CodecLimits::default()),
            Err(CodecError::InvalidPeerStatus)
        );
    }
}

#[test]
fn peer_status_decoder_rejects_invalid_enums() {
    let response = Frame::PeerStatusResponse {
        request_id: 42,
        code: PeerStatusCode::Ok,
        paths: vec![peer_path_status(
            PeerPathState::Active,
            PathUsage::Available,
        )],
    };
    let encoded = encode_frame(&response, CodecLimits::default()).expect("encode");

    for offset in [
        FRAME_HEADER_LEN + 8,
        FRAME_HEADER_LEN + 11,
        FRAME_HEADER_LEN + 12,
        FRAME_HEADER_LEN + 15,
        FRAME_HEADER_LEN + 16,
    ] {
        let mut malformed = encoded.clone();
        malformed[offset] = u8::MAX;
        assert_eq!(
            decode_frame_bytes(Bytes::from(malformed), CodecLimits::default()),
            Err(CodecError::InvalidEnum)
        );
    }
}

#[test]
fn peer_status_count_is_bounded_before_allocation() {
    let mut encoded = encode_frame(
        &Frame::PeerStatusResponse {
            request_id: 42,
            code: PeerStatusCode::Ok,
            paths: Vec::new(),
        },
        CodecLimits::default(),
    )
    .expect("encode");
    encoded[FRAME_HEADER_LEN + 9..FRAME_HEADER_LEN + 11].copy_from_slice(&u16::MAX.to_be_bytes());

    assert_eq!(
        decode_frame_bytes(Bytes::from(encoded), CodecLimits::default()),
        Err(CodecError::TooManyPaths {
            actual: usize::from(u16::MAX),
            limit: CodecLimits::default().max_paths,
        })
    );
}

#[test]
fn peer_status_response_limit_follows_the_configured_frame_size() {
    let fixed_bytes = FRAME_HEADER_LEN + 11;
    assert_eq!(
        peer_status_response_path_limit(CodecLimits {
            max_frame_bytes: fixed_bytes,
            ..CodecLimits::default()
        }),
        0
    );
    assert_eq!(
        peer_status_response_path_limit(CodecLimits {
            max_frame_bytes: fixed_bytes + 118,
            ..CodecLimits::default()
        }),
        1
    );
}

#[test]
fn peer_status_encoder_rejects_count_overflow() {
    let paths = vec![
        peer_path_status(PeerPathState::Active, PathUsage::Available);
        usize::from(u16::MAX) + 1
    ];
    assert_eq!(
        encode_frame(
            &Frame::PeerStatusResponse {
                request_id: 42,
                code: PeerStatusCode::Ok,
                paths,
            },
            CodecLimits {
                max_paths: usize::MAX,
                ..CodecLimits::default()
            },
        ),
        Err(CodecError::LengthOverflow)
    );
}

#[test]
fn codec_rejects_oversize_payloads_and_ack_ranges() {
    let limits = CodecLimits {
        max_payload_bytes: 4,
        max_ack_ranges: 1,
        ..CodecLimits::default()
    };
    let oversized = Frame::StreamData {
        stream_id: StreamId(1),
        offset: 0,
        payload: Bytes::from_static(b"hello"),
    };
    assert!(matches!(
        encode_frame(&oversized, limits),
        Err(CodecError::PayloadTooLarge { .. })
    ));

    let too_many_ranges = Frame::StreamAck {
        stream_id: StreamId(1),
        complete: true,
        ranges: vec![
            OffsetRange::new(0, 1).expect("range"),
            OffsetRange::new(2, 3).expect("range"),
        ],
    };
    assert!(matches!(
        encode_frame(&too_many_ranges, limits),
        Err(CodecError::TooManyAckRanges { .. })
    ));

    let too_many_datagram_ranges = Frame::DatagramFeedback {
        flow_id: DatagramFlowId(1),
        received: vec![
            OffsetRange::new(0, 1).expect("range"),
            OffsetRange::new(2, 3).expect("range"),
        ],
    };
    assert!(matches!(
        encode_frame(&too_many_datagram_ranges, limits),
        Err(CodecError::TooManyAckRanges { .. })
    ));
}

#[test]
fn decoder_rejects_trailing_bytes() {
    let mut encoded =
        encode_frame(&Frame::Ping { nonce: 42 }, CodecLimits::default()).expect("encode");
    encoded.push(0);

    assert_eq!(
        decode_frame_bytes(Bytes::from(encoded), CodecLimits::default()),
        Err(CodecError::TrailingBytes)
    );
}
