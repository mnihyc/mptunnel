use super::*;

fn limits() -> MuxLimits {
    MuxLimits {
        max_payload_bytes: 1024,
        max_ack_ranges: 8,
        max_streams: 1024,
        max_quic_concurrent_bidi_streams: 1024,
        max_stream_window_bytes: 4096,
        max_repair_bytes: 2048,
        max_reorder_bytes: 2048,
        max_datagram_queue_bytes: 2048,
        max_path_flight_bytes: 2048,
        max_reliable_relay_chunk_bytes: 1024,
        tcp_path_heartbeat_interval: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_INTERVAL,
        tcp_path_heartbeat_timeout: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_TIMEOUT,
        quic_path_keep_alive_interval: crate::config::DEFAULT_QUIC_PATH_KEEP_ALIVE_INTERVAL,
        quic_path_idle_timeout: crate::config::DEFAULT_QUIC_PATH_IDLE_TIMEOUT,
    }
}

#[test]
fn send_stream_keeps_data_until_tunnel_ack() {
    let mut stream = ReliableSendStream::new(StreamId(1), limits());
    stream
        .send_data(Bytes::from_static(b"hello"))
        .expect("send");

    assert_eq!(stream.reinjection_bytes(), 5);

    let outcome = stream.apply_ack(&[OffsetRange::new(0, 5).expect("range")]);
    assert_eq!(
        outcome,
        AckOutcome {
            released_bytes: 5,
            released_chunks: 1,
            remaining_reinjection_bytes: 0
        }
    );
    assert_eq!(stream.reinjection_bytes(), 0);
}

#[test]
fn send_stream_trims_reinjection_cache_by_ack_subranges() {
    let mut stream = ReliableSendStream::new(StreamId(2), limits());
    stream
        .send_data(Bytes::from_static(b"abcdefgh"))
        .expect("send");

    let outcome = stream.apply_ack(&[OffsetRange::new(2, 6).expect("range")]);
    assert_eq!(
        outcome,
        AckOutcome {
            released_bytes: 4,
            released_chunks: 0,
            remaining_reinjection_bytes: 4,
        }
    );

    let frames = stream.retransmission_frames_for_ranges(&[OffsetRange { start: 0, end: 8 }], 8);
    assert_eq!(frames.len(), 2);
    assert!(matches!(
        &frames[0],
        Frame::StreamData { offset: 0, payload, .. } if payload.as_ref() == b"ab"
    ));
    assert!(matches!(
        &frames[1],
        Frame::StreamData { offset: 6, payload, .. } if payload.as_ref() == b"gh"
    ));
}

#[test]
fn send_stream_data_ack_frontier_advances_across_incomplete_ack_chunks() {
    let mut stream = ReliableSendStream::new(StreamId(3), limits());
    for payload in [b"aaaa", b"bbbb", b"cccc", b"dddd"] {
        stream
            .send_data(Bytes::copy_from_slice(payload))
            .expect("send");
    }

    // ACK framing completeness controls gap inference, not positive delivery.
    stream.apply_normalized_ack(&[OffsetRange { start: 8, end: 16 }]);
    assert_eq!(stream.data_ack_frontier(), 0);
    stream.apply_normalized_ack(&[OffsetRange { start: 0, end: 4 }]);
    assert_eq!(stream.data_ack_frontier(), 4);
    stream.apply_normalized_ack(&[OffsetRange { start: 4, end: 8 }]);
    assert_eq!(stream.data_ack_frontier(), 16);
}

#[test]
fn send_stream_retransmits_ack_range_holes_before_later_inflight() {
    let mut stream = ReliableSendStream::new(StreamId(7), limits());
    for payload in [b"aaaa", b"bbbb", b"cccc", b"dddd"] {
        stream
            .send_data(Bytes::copy_from_slice(payload))
            .expect("send");
    }

    let frames = stream.retransmission_frames_for_ack_gaps(
        &[
            OffsetRange { start: 0, end: 4 },
            OffsetRange { start: 8, end: 12 },
        ],
        1024,
    );

    assert_eq!(frames.len(), 1);
    assert!(matches!(
        &frames[0],
        Frame::StreamData { offset: 4, payload, .. } if payload.as_ref() == b"bbbb"
    ));
    assert!(
        stream
            .retransmission_frames_for_ack_gaps(&[OffsetRange { start: 0, end: 4 }], 1024)
            .is_empty()
    );
}

#[test]
fn send_stream_slices_ack_gap_reinjections_to_byte_limit() {
    let mut stream = ReliableSendStream::new(StreamId(8), limits());
    stream
        .send_data(Bytes::from_static(b"abcdefgh"))
        .expect("send");

    let frames = stream.retransmission_frames_for_ack_gaps(
        &[
            OffsetRange { start: 0, end: 2 },
            OffsetRange { start: 6, end: 8 },
        ],
        3,
    );

    assert_eq!(frames.len(), 1);
    assert!(matches!(
        &frames[0],
        Frame::StreamData { offset: 2, payload, .. } if payload.as_ref() == b"cde"
    ));
}

#[test]
fn send_stream_slices_path_failure_reinjections_to_byte_limit() {
    let mut stream = ReliableSendStream::new(StreamId(9), limits());
    stream
        .send_data(Bytes::from_static(b"abcdefgh"))
        .expect("send");

    let frames = stream.retransmission_frames_for_ranges(&[OffsetRange { start: 2, end: 7 }], 4);

    assert_eq!(frames.len(), 1);
    assert!(matches!(
        &frames[0],
        Frame::StreamData { offset: 2, payload, .. } if payload.as_ref() == b"cdef"
    ));
}

#[test]
fn send_stream_reinjections_tail_after_ack_frontier() {
    let mut stream = ReliableSendStream::new(StreamId(10), limits());
    for payload in [b"aaaa", b"bbbb", b"cccc"] {
        stream
            .send_data(Bytes::copy_from_slice(payload))
            .expect("send");
    }

    let frames =
        stream.retransmission_frames_after_ack_frontier(&[OffsetRange { start: 0, end: 4 }], 6);

    assert_eq!(frames.len(), 2);
    assert!(matches!(
        &frames[0],
        Frame::StreamData { offset: 4, payload, .. } if payload.as_ref() == b"bbbb"
    ));
    assert!(matches!(
        &frames[1],
        Frame::StreamData { offset: 8, payload, .. } if payload.as_ref() == b"cc"
    ));
}

#[test]
fn send_stream_prepares_data_without_taking_ownership_until_commit() {
    let mut stream = ReliableSendStream::new(StreamId(1), limits());
    let frame = stream
        .prepare_data(Bytes::from_static(b"hello"))
        .expect("prepare");

    assert_eq!(stream.next_offset(), 0);
    assert_eq!(stream.reinjection_bytes(), 0);
    assert!(matches!(
        &frame,
        Frame::StreamData { offset: 0, payload, .. } if payload.as_ref() == b"hello"
    ));

    stream
        .commit_prepared_data(&frame)
        .expect("commit prepared frame");
    assert_eq!(stream.next_offset(), 5);
    assert_eq!(stream.reinjection_bytes(), 5);
    stream
        .rollback_committed_data(&frame)
        .expect("rollback tail frame");
    assert_eq!(stream.next_offset(), 0);
    assert_eq!(stream.reinjection_bytes(), 0);
    stream
        .commit_prepared_data(&frame)
        .expect("commit prepared frame again");
    assert!(matches!(
        stream.commit_prepared_data(&frame),
        Err(StreamError::InvalidPreparedFrame)
    ));
}

#[test]
fn send_stream_with_explicit_zero_credit_waits_for_peer_max_data() {
    let mut stream = ReliableSendStream::new_with_initial_max_offset(StreamId(11), limits(), 0);

    assert!(matches!(
        stream.send_data(Bytes::from_static(b"hello")),
        Err(StreamError::FlowControlBlocked { max: 0, .. })
    ));

    stream.update_max_offset(5);
    stream
        .send_data(Bytes::from_static(b"hello"))
        .expect("peer max data creates send credit");
}

#[test]
fn send_stream_enforces_flow_control_and_reinjection_limit() {
    let mut limit = limits();
    limit.max_stream_window_bytes = 4;
    let mut stream = ReliableSendStream::new(StreamId(1), limit);

    assert!(matches!(
        stream.send_data(Bytes::from_static(b"hello")),
        Err(StreamError::FlowControlBlocked { .. })
    ));

    let mut limit = limits();
    limit.max_repair_bytes = 4;
    let mut stream = ReliableSendStream::new(StreamId(1), limit);
    assert!(matches!(
        stream.send_data(Bytes::from_static(b"hello")),
        Err(StreamError::ReinjectionCacheFull { .. })
    ));
}

#[test]
fn recv_stream_reassembles_out_of_order_data_and_builds_ack_ranges() {
    let mut stream = ReliableRecvStream::new(StreamId(7), limits());
    assert_eq!(stream.max_data_offset(), limits().max_stream_window_bytes);
    let first = stream
        .receive_data(5, Bytes::from_static(b" world"))
        .expect("second chunk");
    assert!(first.delivered.is_empty());
    assert_eq!(
        stream.ack_ranges(),
        vec![OffsetRange::new(5, 11).expect("range")]
    );

    let second = stream
        .receive_data(0, Bytes::from_static(b"hello"))
        .expect("first chunk");

    assert_eq!(
        second.delivered.as_slice(),
        &[Bytes::from_static(b"hello"), Bytes::from_static(b" world")]
    );
    assert_eq!(stream.next_offset(), 11);
    assert_eq!(stream.reorder_bytes(), 0);
    assert_eq!(
        stream.ack_ranges(),
        vec![OffsetRange::new(0, 11).expect("range")]
    );
    assert_eq!(
        stream.ack_frame(),
        Frame::StreamAck {
            stream_id: StreamId(7),
            complete: true,
            ranges: vec![OffsetRange::new(0, 11).expect("range")]
        }
    );
    assert_eq!(
        stream.max_data_offset(),
        11 + limits().max_stream_window_bytes
    );
    assert_eq!(
        stream.max_data_frame(),
        Frame::StreamMaxData {
            stream_id: StreamId(7),
            max_offset: 11 + limits().max_stream_window_bytes
        }
    );
}

#[test]
fn recv_stream_limits_encoded_ack_ranges_without_rejecting_reordering() {
    let mut limit = limits();
    limit.max_ack_ranges = 4;
    let mut stream = ReliableRecvStream::new(StreamId(7), limit);

    stream
        .receive_data(0, Bytes::from_static(b"a"))
        .expect("first chunk");
    stream
        .receive_data(10, Bytes::from_static(b"b"))
        .expect("second range");
    stream
        .receive_data(20, Bytes::from_static(b"c"))
        .expect("third range");
    stream
        .receive_data(30, Bytes::from_static(b"d"))
        .expect("fourth range");
    stream
        .receive_data(40, Bytes::from_static(b"e"))
        .expect("fifth range");
    stream
        .receive_data(50, Bytes::from_static(b"f"))
        .expect("sixth range");

    assert_eq!(stream.ack_ranges().len(), 6);
    assert_eq!(
        stream.ack_frame(),
        Frame::StreamAck {
            stream_id: StreamId(7),
            complete: false,
            ranges: vec![
                OffsetRange::new(0, 1).expect("contiguous range"),
                OffsetRange::new(10, 11).expect("first reinjection-adjacent range"),
                OffsetRange::new(20, 21).expect("third range"),
                OffsetRange::new(30, 31).expect("fourth range"),
            ]
        }
    );
    let delta_ranges = [
        OffsetRange::new(20, 21).expect("middle delta"),
        OffsetRange::new(50, 51).expect("tail delta"),
    ];
    assert_eq!(
        stream.ack_delta_frames(&delta_ranges),
        vec![Frame::StreamAck {
            stream_id: StreamId(7),
            complete: false,
            ranges: delta_ranges.to_vec(),
        }],
        "a delta ACK must preserve exact new coverage without claiming a full snapshot"
    );
}

#[test]
fn recv_stream_splits_large_ack_sets_into_bounded_frames() {
    let mut limit = limits();
    limit.max_ack_ranges = 2;
    let mut stream = ReliableRecvStream::new(StreamId(7), limit);

    for offset in [0, 10, 20, 30, 40] {
        stream
            .receive_data(offset, Bytes::from_static(b"x"))
            .expect("range");
    }

    assert_eq!(
        stream.ack_frames(),
        vec![
            Frame::StreamAck {
                stream_id: StreamId(7),
                complete: false,
                ranges: vec![
                    OffsetRange::new(0, 1).expect("first"),
                    OffsetRange::new(10, 11).expect("second"),
                ],
            },
            Frame::StreamAck {
                stream_id: StreamId(7),
                complete: false,
                ranges: vec![
                    OffsetRange::new(20, 21).expect("third"),
                    OffsetRange::new(30, 31).expect("fourth"),
                ],
            },
            Frame::StreamAck {
                stream_id: StreamId(7),
                complete: false,
                ranges: vec![OffsetRange::new(40, 41).expect("fifth")],
            },
        ]
    );
}

#[test]
fn recv_stream_accepts_duplicate_overlap_and_rejects_reorder_pressure() {
    let mut stream = ReliableRecvStream::new(StreamId(7), limits());
    stream
        .receive_data(0, Bytes::from_static(b"hello"))
        .expect("first");
    assert_eq!(
        stream.receive_data(2, Bytes::from_static(b"xx")),
        Ok(ReceiveOutcome::default())
    );

    let mut stream = ReliableRecvStream::new(StreamId(8), limits());
    stream
        .receive_data(5, Bytes::from_static(b"world"))
        .expect("out of order tail");
    let outcome = stream
        .receive_data(0, Bytes::from_static(b"hello w"))
        .expect("partially overlapping lower range");
    assert_eq!(
        outcome.delivered.as_slice(),
        &[Bytes::from_static(b"hello"), Bytes::from_static(b"world")]
    );
    assert_eq!(stream.next_offset(), 10);
    assert_eq!(stream.reorder_bytes(), 0);
    assert_eq!(
        stream.receive_data(3, Bytes::from_static(b"lo wo")),
        Ok(ReceiveOutcome::default())
    );

    let mut limit = limits();
    limit.max_reorder_bytes = 4;
    let mut stream = ReliableRecvStream::new(StreamId(9), limit);
    assert!(matches!(
        stream.receive_data(10, Bytes::from_static(b"hello")),
        Err(StreamError::ReorderBufferFull { .. })
    ));
}
