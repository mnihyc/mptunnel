use crate::protocol::frame::{
    datagram_ack_range, normalized_offset_ranges, reliable_path_frame_pacing_bytes,
    reliable_stream_frame_accounted_bytes, reliable_stream_frame_extent,
    stream_ack_contiguous_frontier,
};
use crate::protocol::{
    DatagramFlowId, DatagramId, Frame, OffsetRange, PathId, ResetReason, StreamId,
};
use bytes::Bytes;

#[test]
fn datagram_ack_range_is_one_identity_and_rejects_overflow() {
    assert_eq!(
        datagram_ack_range(DatagramId(7)),
        Some(OffsetRange { start: 7, end: 8 })
    );
    assert_eq!(datagram_ack_range(DatagramId(u64::MAX)), None);
}

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
        payload: Bytes::new(),
    };

    assert_eq!(reliable_stream_frame_extent(&empty), None);
    assert_eq!(
        reliable_stream_frame_extent(&Frame::Ping { nonce: 7 }),
        None
    );
}

#[test]
fn frame_accounting_and_pacing_cover_each_semantic_row() {
    let stream_id = StreamId(7);
    let path_id = PathId(3);
    let cases = [
        (
            "stream data",
            Frame::StreamData {
                stream_id,
                offset: 0,
                payload: Bytes::from_static(b"data"),
            },
            4,
            4,
        ),
        (
            "empty stream data",
            Frame::StreamData {
                stream_id,
                offset: 0,
                payload: Bytes::new(),
            },
            1,
            1,
        ),
        (
            "capacity data",
            Frame::PathCapacityData {
                path_id,
                calibration_id: 1,
                payload: Bytes::from_static(b"probe"),
            },
            1,
            5,
        ),
        (
            "empty capacity data",
            Frame::PathCapacityData {
                path_id,
                calibration_id: 1,
                payload: Bytes::new(),
            },
            1,
            1,
        ),
        (
            "capacity finish",
            Frame::PathCapacityFinish {
                path_id,
                calibration_id: 1,
                payload_bytes: 5,
            },
            1,
            0,
        ),
        (
            "capacity receipt",
            Frame::PathCapacityReceipt {
                path_id,
                calibration_id: 1,
                received_payload_bytes: 5,
            },
            1,
            0,
        ),
        (
            "stream fin",
            Frame::StreamFin {
                stream_id,
                final_offset: 4,
            },
            1,
            1,
        ),
        (
            "stream ack",
            Frame::StreamAck {
                stream_id,
                complete: true,
                ranges: vec![OffsetRange { start: 0, end: 4 }],
            },
            1,
            1,
        ),
        (
            "stream max data",
            Frame::StreamMaxData {
                stream_id,
                max_offset: 4,
            },
            1,
            1,
        ),
        (
            "stream reset",
            Frame::StreamReset {
                stream_id,
                reason: ResetReason::RemoteClosed,
            },
            1,
            1,
        ),
        ("stream detach", Frame::StreamDetach { stream_id }, 1, 1),
        (
            "datagram data",
            Frame::DatagramData {
                flow_id: DatagramFlowId(9),
                datagram_id: DatagramId(1),
                ttl_ms: 100,
                payload: Bytes::from_static(b"datagram"),
            },
            1,
            0,
        ),
        (
            "empty datagram data",
            Frame::DatagramData {
                flow_id: DatagramFlowId(9),
                datagram_id: DatagramId(2),
                ttl_ms: 100,
                payload: Bytes::new(),
            },
            1,
            0,
        ),
        ("other control", Frame::Ping { nonce: 11 }, 1, 0),
    ];

    for (label, frame, accounted_bytes, pacing_bytes) in cases {
        assert_eq!(
            reliable_stream_frame_accounted_bytes(&frame),
            accounted_bytes,
            "{label}: sender accounting"
        );
        assert_eq!(
            reliable_path_frame_pacing_bytes(&frame),
            pacing_bytes,
            "{label}: path pacing"
        );
    }
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
