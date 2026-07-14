use crate::protocol::frame::{
    normalized_offset_ranges, reliable_stream_frame_extent, stream_ack_contiguous_frontier,
};
use crate::protocol::{Frame, OffsetRange, StreamFlags, StreamId};
use bytes::Bytes;

#[test]
fn offset_range_normalization_sorts_filters_and_merges_adjacency() {
    let ranges = [
        OffsetRange { start: 10, end: 20 },
        OffsetRange { start: 0, end: 5 },
        OffsetRange { start: 5, end: 10 },
        OffsetRange { start: 30, end: 30 },
        OffsetRange { start: 18, end: 25 },
        OffsetRange { start: 40, end: 35 },
        OffsetRange { start: 29, end: 30 },
        OffsetRange { start: 27, end: 29 },
    ];

    assert_eq!(
        normalized_offset_ranges(&ranges),
        vec![
            OffsetRange { start: 0, end: 25 },
            OffsetRange { start: 27, end: 30 },
        ]
    );
}

#[test]
fn reliable_stream_extent_counts_payload_and_saturates_end() {
    let frame = Frame::StreamData {
        stream_id: StreamId(7),
        offset: u64::MAX - 1,
        flags: StreamFlags::NONE,
        payload: Bytes::from_static(b"data"),
    };

    assert_eq!(
        reliable_stream_frame_extent(&frame),
        Some((u64::MAX - 1, u64::MAX, 4))
    );
}

#[test]
fn reliable_stream_extent_rejects_empty_and_non_stream_frames() {
    let empty = Frame::StreamData {
        stream_id: StreamId(7),
        offset: 11,
        flags: StreamFlags::NONE,
        payload: Bytes::new(),
    };

    assert_eq!(reliable_stream_frame_extent(&empty), None);
    assert_eq!(
        reliable_stream_frame_extent(&Frame::Ping { nonce: 7 }),
        None
    );
}

#[test]
fn sparse_ack_largest_end_is_not_contiguous_frontier() {
    let ranges = [
        OffsetRange {
            start: 0,
            end: 1024,
        },
        OffsetRange {
            start: 4096,
            end: 8192,
        },
    ];

    assert_eq!(
        stream_ack_contiguous_frontier(&ranges),
        1024,
        "sparse ACK ranges must keep the scheduling frontier at the first hole, not the largest ACK end"
    );
}
