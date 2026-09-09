use super::*;

#[test]
fn fragmented_ack_wire_cost_preserves_scoped_snapshot() {
    let frame = Frame::StreamAck {
        stream_id: StreamId(7),
        scope_start: Some(0),
        ranges: (0..234)
            .map(|i| OffsetRange::new(i * 2048, i * 2048 + 1024).unwrap())
            .collect(),
    };
    let wire = encode_frame(&frame, CodecLimits::default()).unwrap();
    assert_eq!(
        decode_frame_bytes(Bytes::from(wire.clone()), CodecLimits::default()).unwrap(),
        frame
    );
    assert_eq!(wire.len(), 21 + 1 + 1 + 2 + 233 * 4);
}

fn assert_exact_and_no_larger(ranges: Vec<OffsetRange>) {
    for scope_start in [None, (!ranges.is_empty()).then_some(0)] {
        let frame = Frame::StreamAck {
            stream_id: StreamId(u64::MAX),
            scope_start,
            ranges: ranges.clone(),
        };
        let wire = encode_frame(&frame, CodecLimits::default()).unwrap();
        assert!(
            wire.len()
                <= FRAME_HEADER_LEN + 11 + usize::from(scope_start.is_some()) + 16 * ranges.len()
        );
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
        scope_start: Some(0),
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
            scope_start: None,
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
        assert!(decode_frame_bytes(packed_wire(2, 1, &payload), CodecLimits::default()).is_err());
    }
    assert_eq!(
        decode_frame_bytes(packed_wire(4, 0, &[]), CodecLimits::default()),
        Err(CodecError::InvalidEnum)
    );
    assert!(matches!(
        decode_frame_bytes(packed_wire(2, 257, &[]), CodecLimits::default()),
        Err(CodecError::TooManyAckRanges { .. })
    ));
    assert!(matches!(
        decode_frame_bytes(packed_wire(2, 1, &[0, 1, 0]), CodecLimits::default()),
        Err(CodecError::TrailingBytes)
    ));
    assert_exact_and_no_larger(vec![OffsetRange::new(u64::MAX - 1, u64::MAX).unwrap()]);
}

#[test]
fn ack_packed_frame_limit_is_actual_wire_size_and_range_count_still_applies() {
    let frame = Frame::StreamAck {
        stream_id: StreamId(1),
        scope_start: Some(0),
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

#[test]
fn scoped_ack_wire_is_independent_and_validates_scope_before_delivery() {
    let limits = CodecLimits::default();
    for scope_start in [0, 127, 128, (1u64 << 63), u64::MAX - 1] {
        let frame = Frame::StreamAck {
            stream_id: StreamId(7),
            scope_start: Some(scope_start),
            // Deliberately unordered positives: the scope ends at the maximum,
            // not the last element. No preceding ACK is needed to decode it.
            ranges: vec![
                OffsetRange {
                    start: u64::MAX - 1,
                    end: u64::MAX,
                },
                OffsetRange { start: 0, end: 1 },
            ],
        };
        let wire = encode_frame(&frame, limits).unwrap();
        assert!(wire.len() <= FRAME_HEADER_LEN + 11 + 10 + 32);
        assert_eq!(
            decode_frame_bytes(Bytes::from(wire), limits).unwrap(),
            frame
        );
    }
    for (scope_start, ranges) in [
        (0, vec![]),
        (5, vec![OffsetRange { start: 0, end: 5 }]),
        (6, vec![OffsetRange { start: 0, end: 5 }]),
    ] {
        assert_eq!(
            encode_frame(
                &Frame::StreamAck {
                    stream_id: StreamId(7),
                    scope_start: Some(scope_start),
                    ranges,
                },
                limits
            ),
            Err(CodecError::InvalidRange)
        );
    }
    // Scope offset is the same canonical independent varuint64 as ranges.
    assert!(decode_frame_bytes(packed_wire(3, 1, &[0x80, 0, 0, 1]), limits).is_err());
    assert!(decode_frame_bytes(packed_wire(3, 1, &[1, 0, 1]), limits).is_err());
    assert_eq!(
        decode_frame_bytes(packed_wire(3, 1, &[0, 0, 1]), limits).unwrap(),
        Frame::StreamAck {
            stream_id: StreamId(1),
            scope_start: Some(0),
            ranges: vec![OffsetRange { start: 0, end: 1 }],
        }
    );
}
