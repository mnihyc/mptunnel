//! Complete output oracle and actual work bound for the public recovery API.
use super::*;

fn old_full_scan(stream: &ReliableSendStream, ranges: &[OffsetRange], limit: usize) -> Vec<Frame> {
    if limit == 0 || ranges.is_empty() {
        return Vec::new();
    }
    let ranges = normalized_offset_ranges(ranges);
    let mut frames = Vec::new();
    let mut emitted = 0;
    let mut index = 0;
    for chunk in stream.reinjection_cache.values() {
        while index < ranges.len() {
            if ranges[index].end <= chunk.offset {
                index += 1;
            } else {
                break;
            }
        }
        let chunk_end = chunk.offset.saturating_add(chunk.payload.len() as u64);
        for range in &ranges[index..] {
            if range.start >= chunk_end {
                break;
            }
            if range.end > chunk.offset
                && !push_retransmission_slice(
                    &mut frames,
                    stream.stream_id,
                    chunk,
                    range.start.max(chunk.offset),
                    range.end.min(chunk_end),
                    limit,
                    &mut emitted,
                )
            {
                return frames;
            }
        }
    }
    frames
}

fn fixture() -> ReliableSendStream {
    let mut stream = ReliableSendStream::new(StreamId(7001), MuxLimits::default());
    for (i, len) in [1, 7, 4, 11, 3, 8, 9, 5].into_iter().enumerate() {
        stream.send_data(Bytes::from(vec![i as u8; len])).unwrap();
    }
    stream
}

#[test]
fn recovery_seek_does_not_scan_preceding_payload_per_exact_query() {
    const N: usize = 2048;
    let mut stream = ReliableSendStream::new(StreamId(7002), MuxLimits::default());
    for _ in 0..N {
        stream.send_data(Bytes::from_static(b"abcd")).unwrap();
    }
    ReliableSendStream::take_recovery_chunk_visits_for_test();
    for i in 0..N {
        let frames = stream.retransmission_frames_for_ranges(
            &[OffsetRange {
                start: (4 * i) as u64,
                end: (4 * (i + 1)) as u64,
            }],
            4,
        );
        assert_eq!(frames.len(), 1);
    }
    let visits = ReliableSendStream::take_recovery_chunk_visits_for_test();
    eprintln!("recovery_payload_work chunks={N} queries={N} visited_chunks={visits}");
    // Counts actual visited cache chunks, not BTreeMap's logarithmic key
    // comparisons. No wall-clock limit or runtime restriction is introduced.
    assert!(visits <= 4 * N, "unrelated payload traversal: {visits}");
}

#[test]
fn recovery_seek_matches_original_slices_order_budget_and_sparse_ack() {
    let mut stream = fixture();
    let mut seed = 0x41d379b2_u64;
    for phase in 0..3 {
        if phase == 1 {
            stream
                .apply_ack(&[
                    OffsetRange { start: 3, end: 6 },
                    OffsetRange { start: 19, end: 22 },
                    OffsetRange { start: 39, end: 42 },
                ])
                .unwrap();
        }
        if phase == 2 {
            stream
                .send_data(Bytes::from_static(b"new retained tail"))
                .unwrap();
        }
        for case in 0..1024 {
            let mut ranges = Vec::new();
            for _ in 0..1 + (case % 5) {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                let start = seed % 80;
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                let end = seed % 80;
                ranges.push(OffsetRange { start, end });
            }
            for limit in [0, 1, 3, 9, 17, 64, usize::MAX] {
                let expected = old_full_scan(&stream, &ranges, limit);
                let actual = stream.retransmission_frames_for_ranges(&ranges, limit);
                assert_eq!(actual, expected, "phase={phase} case={case} limit={limit}");
                for (a, b) in actual.iter().zip(&expected) {
                    let (
                        Frame::StreamData { payload: a, .. },
                        Frame::StreamData { payload: b, .. },
                    ) = (a, b)
                    else {
                        panic!("only exact DATA slices");
                    };
                    assert_eq!(
                        a.as_ptr(),
                        b.as_ptr(),
                        "same retained allocation and slice, not a replacement copy"
                    );
                }
            }
        }
    }
}

#[test]
fn recovery_seek_empty_outside_and_final_tail_preserve_exact_gap_rejection() {
    let mut stream = fixture();
    let end = stream.next_offset();
    stream
        .apply_ack(&[OffsetRange { start: 5, end: 7 }])
        .unwrap();
    for range in [
        OffsetRange { start: 0, end },
        OffsetRange { start: 4, end: 8 },
        OffsetRange { start: 5, end: 7 },
        OffsetRange {
            start: end,
            end: end + 10,
        },
        OffsetRange {
            start: u64::MAX,
            end: u64::MAX,
        },
        OffsetRange {
            start: 0,
            end: u64::MAX,
        },
    ] {
        assert_eq!(
            stream.retransmission_frames_for_ranges(&[range], usize::MAX),
            old_full_scan(&stream, &[range], usize::MAX)
        );
    }
    let frames = stream.retransmission_frames_for_ranges(&[OffsetRange { start: 4, end: 8 }], 4);
    assert_eq!(frames.len(), 2);
    assert!(matches!(&frames[0],Frame::StreamData{offset:4,payload,..} if payload.len()==1));
    assert!(matches!(&frames[1],Frame::StreamData{offset:7,payload,..} if payload.len()==1));
    // The authoritative gap remains absent from the returned exact slices;
    // the runtime's existing continuity validator must continue rejecting it.
}
