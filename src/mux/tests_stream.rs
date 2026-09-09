use super::*;
use crate::model::capacity::RELIABLE_STREAM_ATTACHMENT_ACCEPT_MAX_OFFSET;

fn limits() -> MuxLimits {
    MuxLimits {
        max_payload_bytes: 1024,
        max_ack_ranges: 8,
        max_streams: 1024,
        max_quic_concurrent_bidi_streams: 1024,
        max_quic_loss_journal_bytes: crate::config::DEFAULT_MAX_QUIC_LOSS_JOURNAL_BYTES,
        max_stream_window_bytes: 4096,
        max_repair_bytes: 2048,
        max_reorder_bytes: 2048,
        max_reinjection_cache_chunks: 65_536,
        max_reorder_buffer_chunks: 65_536,
        max_retained_receive_ranges: 65_536,
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

    let outcome = stream
        .apply_ack(&[OffsetRange::new(0, 5).expect("range")])
        .expect("ACK assigned bytes");
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
fn validated_stream_ack_accepts_the_exact_assigned_extent() {
    let mut stream = ReliableSendStream::new(StreamId(101), limits());
    stream
        .send_data(Bytes::from_static(b"abcdefgh"))
        .expect("send assigned bytes");
    let assigned_end = stream.next_offset();

    let ack = validate_stream_ack(
        Some(0),
        vec![OffsetRange {
            start: 0,
            end: assigned_end,
        }],
        assigned_end,
    )
    .expect("exact assigned extent is valid");

    assert_eq!(ack.scope_start(), Some(0));
    assert_eq!(ack.assigned_end(), 8);
    assert_eq!(ack.ranges(), &[OffsetRange { start: 0, end: 8 }]);
    assert_eq!(
        stream
            .apply_validated_ack(&ack)
            .expect("validated ACK fits retained chunk limit")
            .released_bytes,
        8
    );
}

#[test]
fn validated_stream_ack_rejects_beyond_and_crossing_ranges_without_mutation() {
    let mut stream = ReliableSendStream::new(StreamId(102), limits());
    stream
        .send_data(Bytes::from_static(b"abcdefgh"))
        .expect("send assigned bytes");
    let before = stream.clone();
    let assigned_end = stream.next_offset();

    assert_eq!(
        validate_stream_ack(None, vec![OffsetRange { start: 9, end: 10 }], assigned_end,),
        Err(StreamError::AckRangeBeyondAssigned {
            start: 9,
            end: 10,
            assigned_end: 8,
        })
    );
    assert_eq!(stream, before);

    assert_eq!(
        validate_stream_ack(
            Some(0),
            vec![OffsetRange { start: 4, end: 9 }],
            assigned_end,
        ),
        Err(StreamError::AckRangeBeyondAssigned {
            start: 4,
            end: 9,
            assigned_end: 8,
        })
    );
    assert_eq!(stream, before);
}

#[test]
fn validated_stream_ack_handles_empty_snapshot_and_rejects_empty_original_range() {
    let mut stream = ReliableSendStream::new(StreamId(103), limits());
    stream
        .send_data(Bytes::from_static(b"abcdefgh"))
        .expect("send assigned bytes");
    let before = stream.clone();
    let assigned_end = stream.next_offset();

    let empty = validate_stream_ack(None, Vec::new(), assigned_end)
        .expect("an empty positive report is valid and has no negative authority");
    assert_eq!(empty.scope_start(), None);
    assert!(empty.ranges().is_empty());
    assert_eq!(empty.assigned_end(), assigned_end);
    assert_eq!(
        stream
            .apply_validated_ack(&empty)
            .expect("empty ACK cannot add retained chunks")
            .released_bytes,
        0
    );
    assert_eq!(stream, before);

    assert_eq!(
        validate_stream_ack(None, vec![OffsetRange { start: 4, end: 4 }], assigned_end,),
        Err(StreamError::InvalidAckRange { start: 4, end: 4 })
    );
    assert_eq!(stream, before);
}

#[test]
fn validated_stream_ack_rejects_scope_without_a_positive_end_before_mutation() {
    let mut stream = ReliableSendStream::new(StreamId(104), limits());
    stream.send_data(Bytes::from_static(b"abcdefgh")).unwrap();
    let before = stream.clone();
    for (scope_start, ranges) in [
        (0, vec![]),
        (8, vec![OffsetRange { start: 4, end: 8 }]),
        (9, vec![OffsetRange { start: 4, end: 8 }]),
    ] {
        assert!(validate_stream_ack(Some(scope_start), ranges, stream.next_offset()).is_err());
        assert_eq!(stream, before);
    }
    let scoped = validate_stream_ack(
        Some(6),
        vec![
            OffsetRange { start: 0, end: 2 },
            OffsetRange { start: 7, end: 8 },
        ],
        stream.next_offset(),
    )
    .unwrap();
    assert_eq!(scoped.scope_start(), Some(6));
    assert_eq!(
        stream.apply_validated_ack(&scoped).unwrap().released_bytes,
        3
    );
}

#[test]
fn send_stream_trims_reinjection_cache_by_ack_subranges() {
    let mut stream = ReliableSendStream::new(StreamId(2), limits());
    stream
        .send_data(Bytes::from_static(b"abcdefgh"))
        .expect("send");

    let outcome = stream
        .apply_ack(&[OffsetRange::new(2, 6).expect("range")])
        .expect("ACK assigned bytes");
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
    stream
        .apply_normalized_ack(&[OffsetRange { start: 8, end: 16 }])
        .expect("tail ACK fits retained chunks");
    assert_eq!(stream.data_ack_frontier(), 0);
    stream
        .apply_normalized_ack(&[OffsetRange { start: 0, end: 4 }])
        .expect("prefix ACK fits retained chunks");
    assert_eq!(stream.data_ack_frontier(), 4);
    stream
        .apply_normalized_ack(&[OffsetRange { start: 4, end: 8 }])
        .expect("middle ACK fits retained chunks");
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
fn first_retransmission_frame_matches_existing_cache_slices_without_mutation() {
    for acknowledgements in [
        vec![],
        vec![OffsetRange { start: 2, end: 5 }],
        vec![OffsetRange { start: 5, end: 19 }],
        vec![OffsetRange { start: 0, end: 24 }],
    ] {
        let mut stream = ReliableSendStream::new(StreamId(110), limits());
        for start in [0u8, 8, 16] {
            stream
                .send_data(Bytes::from((start..start + 8).collect::<Vec<_>>()))
                .expect("ordinary contiguous cache producer");
        }
        stream
            .apply_ack(&acknowledgements)
            .expect("actual ACK clipping, including cross-chunk holes");
        let before = stream.clone();
        for start in 0..=25 {
            for end in start..=25 {
                for byte_limit in [0, 1, 7, usize::MAX] {
                    let range = OffsetRange { start, end };
                    assert_eq!(
                        stream.first_retransmission_frame_for_range(range, byte_limit),
                        stream
                            .retransmission_frames_for_ranges(&[range], byte_limit)
                            .into_iter()
                            .next(),
                        "range={range:?}, limit={byte_limit}, ACKs={acknowledgements:?}"
                    );
                }
            }
        }
        assert!(
            stream
                .first_retransmission_frame_for_range(
                    OffsetRange {
                        start: u64::MAX - 1,
                        end: u64::MAX
                    },
                    usize::MAX,
                )
                .is_none()
        );
        assert_eq!(
            stream, before,
            "view cannot change credit, offsets or cache"
        );
    }
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
fn attachment_accept_does_not_widen_shared_stream_credit() {
    let mut stream = ReliableSendStream::new_with_initial_max_offset(StreamId(12), limits(), 4);

    stream.update_max_offset(RELIABLE_STREAM_ATTACHMENT_ACCEPT_MAX_OFFSET);
    assert_eq!(stream.send_credit_bytes(), 4);
    stream
        .send_data(Bytes::from_static(b"head"))
        .expect("existing logical credit remains usable");
    assert!(matches!(
        stream.send_data(Bytes::from_static(b"x")),
        Err(StreamError::FlowControlBlocked { max: 4, .. })
    ));
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
            scope_start: None,
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
            scope_start: Some(0),
            ranges: vec![
                OffsetRange::new(0, 1).expect("contiguous range"),
                OffsetRange::new(10, 11).expect("first reinjection-adjacent range"),
                OffsetRange::new(20, 21).expect("third range"),
                OffsetRange::new(30, 31).expect("fourth range"),
            ]
        }
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
                scope_start: Some(0),
                ranges: vec![
                    OffsetRange::new(0, 1).expect("first"),
                    OffsetRange::new(10, 11).expect("second"),
                ],
            },
            Frame::StreamAck {
                stream_id: StreamId(7),
                scope_start: Some(11),
                ranges: vec![
                    OffsetRange::new(20, 21).expect("third"),
                    OffsetRange::new(30, 31).expect("fourth"),
                ],
            },
            Frame::StreamAck {
                stream_id: StreamId(7),
                scope_start: Some(31),
                ranges: vec![OffsetRange::new(40, 41).expect("fifth")],
            },
        ]
    );
}

#[test]
fn scoped_ack_update_preserves_batched_merges_without_repeating_unchanged_islands() {
    let stream_id = StreamId(70);
    let mut limit = limits();
    limit.max_ack_ranges = 1;
    let mut stream = ReliableRecvStream::new(stream_id, limit);
    for offset in [10, 30] {
        stream
            .receive_data(offset, Bytes::from_static(b"0123456789"))
            .expect("admit initial islands");
    }
    assert_eq!(
        stream.take_ack_update(),
        vec![
            Frame::StreamAck {
                stream_id,
                scope_start: Some(0),
                ranges: vec![OffsetRange { start: 10, end: 20 }],
            },
            Frame::StreamAck {
                stream_id,
                scope_start: Some(20),
                ranges: vec![OffsetRange { start: 30, end: 40 }],
            },
        ]
    );

    // One receive-publication boundary covers all three novel receives, not
    // just the latest DATA. The middle receive merges with a clean old node.
    stream
        .receive_data(0, Bytes::from_static(b"first"))
        .unwrap();
    stream
        .receive_data(15, Bytes::from_static(b"0123456789"))
        .unwrap();
    stream
        .receive_data(45, Bytes::from_static(b"later"))
        .unwrap();
    assert_eq!(
        stream.receive_data(30, Bytes::from_static(b"0123456789")),
        Ok(ReceiveOutcome::default()),
        "a duplicate must not dirty the unchanged island",
    );
    let before_rejected = stream.clone();
    assert!(
        stream
            .receive_data(limit.max_stream_window_bytes, Bytes::from_static(b"x"))
            .is_err()
    );
    assert_eq!(
        stream, before_rejected,
        "rejected bytes cannot alter feedback evidence"
    );

    let update = stream.take_ack_update();
    assert_eq!(
        update,
        vec![
            Frame::StreamAck {
                stream_id,
                scope_start: None,
                ranges: vec![OffsetRange { start: 0, end: 5 }],
            },
            Frame::StreamAck {
                stream_id,
                scope_start: None,
                ranges: vec![OffsetRange { start: 10, end: 25 }],
            },
            Frame::StreamAck {
                stream_id,
                scope_start: Some(40),
                ranges: vec![OffsetRange { start: 45, end: 50 }],
            },
        ]
    );
    // Each record is truthful by itself; dropping preceding chunks does not
    // turn the unchanged received island [30,40) into a claimed missing gap.
    let truth = stream.ack_ranges();
    for frame in update.iter().rev().chain(update.iter()) {
        let Frame::StreamAck {
            scope_start,
            ranges,
            ..
        } = frame
        else {
            unreachable!();
        };
        for range in ranges {
            assert!(
                truth
                    .iter()
                    .any(|known| known.start <= range.start && known.end >= range.end)
            );
        }
        if let Some(start) = scope_start {
            for offset in *start..ranges.last().unwrap().end {
                let covered = |ranges: &[OffsetRange]| {
                    ranges
                        .iter()
                        .any(|range| range.start <= offset && offset < range.end)
                };
                assert_eq!(covered(ranges), covered(&truth));
            }
        }
    }
    assert_eq!(
        stream.take_ack_update(),
        vec![Frame::StreamAck {
            stream_id,
            scope_start: None,
            ranges: vec![]
        }],
        "publication consumes only the dirty markers, not cumulative catch-up coverage",
    );
    assert_eq!(stream.ack_ranges(), truth);
}

#[test]
fn scoped_ack_updates_reduce_repeated_support_and_omit_empty_negative_scopes() {
    use crate::protocol::codec::{CodecLimits, encode_frame};

    let mut limit = limits();
    limit.max_ack_ranges = 256;
    let mut stream = ReliableRecvStream::new(StreamId(71), limit);
    let mut update_ranges = 0;
    let mut cumulative_ranges = 0;
    let mut update_bytes = 0;
    let mut cumulative_bytes = 0;
    for index in 0..128 {
        stream
            .receive_data(index * 20, Bytes::from_static(b"x"))
            .unwrap();
        let cumulative = stream.ack_frames();
        let update = stream.take_ack_update();
        assert_eq!(cumulative.len(), 1);
        assert_eq!(update.len(), 1);
        let count_ranges = |frame: &Frame| match frame {
            Frame::StreamAck { ranges, .. } => ranges.len(),
            _ => unreachable!(),
        };
        cumulative_ranges += count_ranges(&cumulative[0]);
        update_ranges += count_ranges(&update[0]);
        cumulative_bytes += encode_frame(&cumulative[0], CodecLimits::default())
            .unwrap()
            .len();
        update_bytes += encode_frame(&update[0], CodecLimits::default())
            .unwrap()
            .len();
    }
    assert_eq!(cumulative_ranges, 128 * 129 / 2);
    assert_eq!(
        update_ranges, 128,
        "one new island contributes one normalized node, not its whole history"
    );
    assert!(
        update_bytes < cumulative_bytes,
        "actual independently encoded records must remove repeated range cost: {update_bytes} versus {cumulative_bytes}"
    );

    let mut contiguous = ReliableRecvStream::new(StreamId(72), limits());
    for offset in [0, 10] {
        contiguous
            .receive_data(offset, Bytes::from_static(b"0123456789"))
            .unwrap();
        let update = contiguous.take_ack_update();
        let [
            Frame::StreamAck {
                scope_start,
                ranges,
                ..
            },
        ] = update.as_slice()
        else {
            panic!("contiguous receive has one normalized positive range");
        };
        assert_eq!(
            *scope_start, None,
            "a fully positive scope proves no omissions"
        );
        assert_eq!(
            ranges,
            &[OffsetRange {
                start: 0,
                end: offset + 10
            }]
        );
        let redundant_scope = Frame::StreamAck {
            stream_id: StreamId(72),
            scope_start: Some(offset),
            ranges: ranges.clone(),
        };
        assert!(
            encode_frame(&update[0], CodecLimits::default())
                .unwrap()
                .len()
                < encode_frame(&redundant_scope, CodecLimits::default())
                    .unwrap()
                    .len()
        );
        assert_eq!(contiguous.ack_frames(), update);
    }
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

#[test]
fn send_chunk_limit_rejection_and_rollback_are_transactional() {
    let mut limit = limits();
    limit.max_reinjection_cache_chunks = 1;
    let mut stream = ReliableSendStream::new(StreamId(201), limit);
    let first = stream
        .send_data(Bytes::from_static(b"a"))
        .expect("first chunk");

    assert_eq!(
        stream.send_data(Bytes::from_static(b"b")),
        Err(StreamError::TooManyReinjectionCacheChunks {
            actual: 2,
            limit: 1,
        })
    );
    assert_eq!(stream.next_offset(), 1);
    assert_eq!(stream.reinjection_bytes(), 1);
    assert_eq!(stream.reinjection_chunks(), 1);

    stream
        .rollback_committed_data(&first)
        .expect("rollback frees both byte and node reservations");
    assert_eq!(stream.next_offset(), 0);
    assert_eq!(stream.reinjection_bytes(), 0);
    assert_eq!(stream.reinjection_chunks(), 0);
    stream
        .send_data(Bytes::from_static(b"c"))
        .expect("freed node can be reused");
}

#[test]
fn ack_split_node_limit_rejection_leaves_send_cache_unchanged() {
    let mut limit = limits();
    limit.max_reinjection_cache_chunks = 1;
    let mut stream = ReliableSendStream::new(StreamId(202), limit);
    stream
        .send_data(Bytes::from_static(b"abcd"))
        .expect("one retained chunk");

    assert_eq!(
        stream.apply_ack(&[OffsetRange::new(1, 3).expect("middle ACK")]),
        Err(StreamError::TooManyReinjectionCacheChunks {
            actual: 2,
            limit: 1,
        })
    );
    assert_eq!(stream.next_offset(), 4);
    assert_eq!(stream.reinjection_bytes(), 4);
    assert_eq!(stream.reinjection_chunks(), 1);

    let released = stream
        .apply_ack(&[OffsetRange::new(0, 4).expect("complete ACK")])
        .expect("complete ACK releases the retained node");
    assert_eq!(released.released_bytes, 4);
    assert_eq!(stream.reinjection_chunks(), 0);
}

#[test]
fn ack_preview_accounts_for_nodes_released_in_the_same_transaction() {
    let mut limit = limits();
    limit.max_reinjection_cache_chunks = 2;
    let mut stream = ReliableSendStream::new(StreamId(203), limit);
    stream
        .send_data(Bytes::from_static(b"abcd"))
        .expect("first chunk");
    stream
        .send_data(Bytes::from_static(b"efgh"))
        .expect("second chunk");

    let outcome = stream
        .apply_ack(&[
            OffsetRange::new(1, 3).expect("split first chunk"),
            OffsetRange::new(4, 8).expect("release second chunk"),
        ])
        .expect("net retained node count remains at the limit");
    assert_eq!(outcome.released_bytes, 6);
    assert_eq!(stream.reinjection_bytes(), 2);
    assert_eq!(stream.reinjection_chunks(), 2);
}

#[test]
fn one_byte_sparse_receive_ranges_stop_at_the_node_limit_atomically() {
    let mut limit = limits();
    limit.max_retained_receive_ranges = 3;
    limit.max_reorder_buffer_chunks = 8;
    let mut stream = ReliableRecvStream::new(StreamId(204), limit);
    for offset in [1, 3, 5] {
        stream
            .receive_data(offset, Bytes::from_static(b"x"))
            .expect("sparse byte within node limit");
    }
    let before_ranges = stream.ack_ranges();
    let before_bytes = stream.reorder_bytes();
    let before_chunks = stream.reorder_chunks();

    assert_eq!(
        stream.receive_data(7, Bytes::from_static(b"x")),
        Err(StreamError::TooManyReceiveRanges {
            actual: 4,
            limit: 3,
        })
    );
    assert_eq!(stream.ack_ranges(), before_ranges);
    assert_eq!(stream.reorder_bytes(), before_bytes);
    assert_eq!(stream.reorder_chunks(), before_chunks);
}

#[test]
fn receive_range_merge_is_accepted_at_the_node_limit() {
    let mut limit = limits();
    limit.max_retained_receive_ranges = 2;
    limit.max_reorder_buffer_chunks = 3;
    let mut stream = ReliableRecvStream::new(StreamId(205), limit);
    stream
        .receive_data(2, Bytes::from_static(b"a"))
        .expect("first sparse range");
    stream
        .receive_data(4, Bytes::from_static(b"c"))
        .expect("second sparse range");

    stream
        .receive_data(3, Bytes::from_static(b"b"))
        .expect("bridge merges two retained ranges at the limit");
    assert_eq!(
        stream.ack_ranges(),
        vec![OffsetRange::new(2, 5).expect("merged range")]
    );
    assert_eq!(stream.reorder_chunks(), 3);
}

#[test]
fn partial_overlap_drains_reorder_nodes_without_false_limit_rejection() {
    let mut limit = limits();
    limit.max_reorder_buffer_chunks = 1;
    let mut stream = ReliableRecvStream::new(StreamId(206), limit);
    stream
        .receive_data(5, Bytes::from_static(b"world"))
        .expect("one retained tail chunk");
    assert_eq!(stream.reorder_chunks(), 1);

    let outcome = stream
        .receive_data(0, Bytes::from_static(b"hello w"))
        .expect("new prefix and duplicate overlap drain the retained tail");
    assert_eq!(
        outcome.delivered.as_slice(),
        &[Bytes::from_static(b"hello"), Bytes::from_static(b"world")]
    );
    assert_eq!(stream.next_offset(), 10);
    assert_eq!(stream.reorder_bytes(), 0);
    assert_eq!(stream.reorder_chunks(), 0);
}

#[test]
fn duplicate_receive_does_not_consume_range_or_reorder_nodes() {
    let mut limit = limits();
    limit.max_retained_receive_ranges = 1;
    limit.max_reorder_buffer_chunks = 1;
    let mut stream = ReliableRecvStream::new(StreamId(207), limit);
    stream
        .receive_data(5, Bytes::from_static(b"tail"))
        .expect("one sparse node");

    assert_eq!(
        stream.receive_data(5, Bytes::from_static(b"tail")),
        Ok(ReceiveOutcome::default())
    );
    assert_eq!(stream.ack_range_summary().count, 1);
    assert_eq!(stream.reorder_chunks(), 1);
    assert_eq!(stream.reorder_bytes(), 4);
}

#[test]
fn reorder_chunk_limit_rejection_leaves_receive_state_unchanged() {
    let mut limit = limits();
    limit.max_reorder_buffer_chunks = 2;
    limit.max_retained_receive_ranges = 8;
    let mut stream = ReliableRecvStream::new(StreamId(208), limit);
    for offset in [10, 20] {
        stream
            .receive_data(offset, Bytes::from_static(b"x"))
            .expect("sparse chunk within limit");
    }
    let before_ranges = stream.ack_ranges();

    assert_eq!(
        stream.receive_data(30, Bytes::from_static(b"x")),
        Err(StreamError::TooManyReorderBufferChunks {
            actual: 3,
            limit: 2,
        })
    );
    assert_eq!(stream.ack_ranges(), before_ranges);
    assert_eq!(stream.reorder_bytes(), 2);
    assert_eq!(stream.reorder_chunks(), 2);
}
