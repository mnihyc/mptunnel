use super::*;

#[test]
fn fragmented_ack_wire_cost_preserves_complete_snapshot() {
    let frame = Frame::StreamAck {
        stream_id: StreamId(7),
        complete: true,
        ranges: (0..234)
            .map(|i| OffsetRange::new(i * 2048, i * 2048 + 1024).unwrap())
            .collect(),
    };
    let wire = encode_frame(&frame, CodecLimits::default()).unwrap();
    assert_eq!(
        decode_frame_bytes(Bytes::from(wire.clone()), CodecLimits::default()).unwrap(),
        frame
    );
    assert_eq!(wire.len(), 21 + 1 + 2 + 233 * 4);
}

fn assert_exact_and_no_larger(ranges: Vec<OffsetRange>) {
    for complete in [false, true] {
        let frame = Frame::StreamAck {
            stream_id: StreamId(u64::MAX),
            complete,
            ranges: ranges.clone(),
        };
        let wire = encode_frame(&frame, CodecLimits::default()).unwrap();
        assert!(wire.len() <= FRAME_HEADER_LEN + 11 + 16 * ranges.len());
        assert_eq!(
            decode_frame_bytes(Bytes::from(wire), CodecLimits::default()).unwrap(),
            frame
        );
    }
}

#[test]
fn ack_encoding_roundtrip_keeps_full_u64_extent_order_and_overlap() {
    assert_exact_and_no_larger(vec![]);
    assert_exact_and_no_larger(vec![OffsetRange::new(0, u64::MAX).unwrap()]);
    for bit in 0..64 {
        let edge = 1u64 << bit;
        for value in [edge - 1, edge, edge.saturating_add(1)] {
            if value < u64::MAX {
                assert_exact_and_no_larger(vec![OffsetRange::new(value, value + 1).unwrap()]);
            }
        }
    }
    let ordered = vec![
        OffsetRange::new(0, 5).unwrap(),
        OffsetRange::new(5, 9).unwrap(),
    ];
    assert_exact_and_no_larger(ordered.clone());
    assert_exact_and_no_larger(ordered.into_iter().rev().collect());
    assert_exact_and_no_larger(vec![
        OffsetRange::new(3, 10).unwrap(),
        OffsetRange::new(5, 12).unwrap(),
    ]);
    // Both packed integers would need ten bytes; fixed remains exactly16.
    let ranges = vec![OffsetRange::new(1u64 << 63, u64::MAX).unwrap()];
    let frame = Frame::StreamAck {
        stream_id: StreamId(1),
        complete: true,
        ranges: ranges.clone(),
    };
    let wire = encode_frame(&frame, CodecLimits::default()).unwrap();
    assert_eq!(wire[FRAME_HEADER_LEN + 8], 1);
    assert_exact_and_no_larger(ranges);
}

#[test]
fn ack_encoding_wide_fragmentation_keeps_every_snapshot_and_size_bound() {
    let mut state = 0x83a7_169b_c002_54efu64;
    for case in 0..8192 {
        let mut end = 0;
        let mut ranges = Vec::new();
        for _ in 0..case % 257 {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let start = end + (state & 0xff_ffff);
            let length = 1 + ((state >> 24) & 0xff_ffff);
            end = start + length;
            ranges.push(OffsetRange::new(start, end).unwrap());
        }
        if case % 3 == 0 {
            ranges.reverse();
        }
        assert_exact_and_no_larger(ranges);
    }
}

fn packed_wire(flags: u8, count: u16, payload: &[u8]) -> Bytes {
    let mut wire = encode_frame(
        &Frame::StreamAck {
            stream_id: StreamId(1),
            complete: false,
            ranges: vec![],
        },
        CodecLimits::default(),
    )
    .unwrap();
    wire[FRAME_HEADER_LEN + 8] = flags;
    wire[FRAME_HEADER_LEN + 9..FRAME_HEADER_LEN + 11].copy_from_slice(&count.to_be_bytes());
    wire.extend_from_slice(payload);
    let len = (wire.len() - FRAME_HEADER_LEN) as u32;
    wire[6..10].copy_from_slice(&len.to_be_bytes());
    Bytes::from(wire)
}

#[test]
fn ack_packed_decoder_enforces_integer_range_and_count_bounds() {
    for payload in [
        vec![],
        vec![0],
        vec![0, 0],
        vec![0x80, 0, 1],
        vec![0, 0x81, 0],
        vec![0xff; 11],
        vec![0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 2, 1],
        // Gap=u64::MAX followed by length1 overflows the range end.
        vec![0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 1, 1],
    ] {
        assert!(decode_frame_bytes(packed_wire(3, 1, &payload), CodecLimits::default()).is_err());
    }
    assert_eq!(
        decode_frame_bytes(packed_wire(4, 0, &[]), CodecLimits::default()),
        Err(CodecError::InvalidEnum)
    );
    assert!(matches!(
        decode_frame_bytes(packed_wire(3, 257, &[]), CodecLimits::default()),
        Err(CodecError::TooManyAckRanges { .. })
    ));
    assert!(matches!(
        decode_frame_bytes(packed_wire(3, 1, &[0, 1, 0]), CodecLimits::default()),
        Err(CodecError::TrailingBytes)
    ));
    assert_exact_and_no_larger(vec![OffsetRange::new(u64::MAX - 1, u64::MAX).unwrap()]);
}

#[test]
fn ack_packed_frame_limit_is_actual_wire_size_and_range_count_still_applies() {
    let frame = Frame::StreamAck {
        stream_id: StreamId(1),
        complete: true,
        ranges: vec![OffsetRange::new(0, 1).unwrap()],
    };
    let wire = encode_frame(&frame, CodecLimits::default()).unwrap();
    let limits = CodecLimits {
        max_frame_bytes: wire.len(),
        ..CodecLimits::default()
    };
    assert_eq!(encode_frame(&frame, limits).unwrap(), wire);
    assert_eq!(
        decode_frame_bytes(Bytes::from(wire.clone()), limits).unwrap(),
        frame
    );
    let short = CodecLimits {
        max_frame_bytes: wire.len() - 1,
        ..limits
    };
    assert!(matches!(
        encode_frame(&frame, short),
        Err(CodecError::FrameTooLarge { .. })
    ));
    assert!(matches!(
        decode_frame_bytes(Bytes::from(wire), short),
        Err(CodecError::FrameTooLarge { .. })
    ));
    assert!(matches!(
        encode_frame(
            &frame,
            CodecLimits {
                max_ack_ranges: 0,
                ..limits
            }
        ),
        Err(CodecError::TooManyAckRanges { .. })
    ));
}
