use super::*;

fn round_trip(frame: Frame) {
    let encoded = encode_frame(&frame, CodecLimits::default()).expect("encode");
    let decoded = decode_frame_bytes(Bytes::from(encoded), CodecLimits::default()).expect("decode");
    assert_eq!(decoded, frame);
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
        role: StreamOpenRole::Active,
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
    round_trip(Frame::StreamFin {
        stream_id: StreamId(7),
        final_offset: 1234,
    });
    round_trip(Frame::StreamDetach {
        stream_id: StreamId(7),
    });
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
fn control_frames_round_trip_auth_and_path_metrics() {
    let nonce = AuthNonce([7; 16]);
    let auth_tag = AuthTag([9; 32]);

    round_trip(Frame::SessionAuth {
        session_id: SessionId(42),
        nonce,
        issued_at_unix_secs: 1_735_689_600,
        auth_tag,
    });
    round_trip(Frame::PathJoin {
        session_id: SessionId(42),
        path_id: PathId(3),
        underlay: UnderlayProtocol::Udp,
        nonce,
        issued_at_unix_secs: 1_735_689_600,
        auth_tag,
    });
    round_trip(Frame::PathJoinOk {
        path_id: PathId(3),
        nonce,
        auth_tag,
    });
    round_trip(Frame::PathStatus {
        path_id: PathId(3),
        status: PathStatus::Suspect,
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
            srtt_us: 25_000,
            rttvar_us: 3_000,
            jitter_us: 1_200,
            delivery_rate_bps: 125_000_000,
            pacing_rate_bps: 150_000_000,
            loss_ppm: 1_500,
            ecn_ppm: 25,
            loss_observed: true,
            ecn_observed: true,
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
fn path_capacity_protocol_round_trips_without_product_stream_identity() {
    round_trip(Frame::PathCapacityData {
        path_id: PathId(7),
        calibration_id: 42,
        payload: Bytes::from_static(b"native-quic-capacity-sample"),
    });
    round_trip(Frame::PathCapacityFinish {
        path_id: PathId(7),
        calibration_id: 42,
        payload_bytes: 27,
    });
    round_trip(Frame::PathCapacityReceipt {
        path_id: PathId(7),
        calibration_id: 42,
        received_payload_bytes: 27,
    });
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
