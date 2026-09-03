use super::*;
use crate::model::capacity::{
    MAX_RELIABLE_SERVICE_QUANTUM_BYTES, PATH_OPEN_SCORE_BYTES,
    adaptive_reliable_relay_reinjection_bytes, data_level_service_window_bytes,
    reliable_bulk_carrier_feed_quantum_bytes, reliable_bulk_product_windows,
    reliable_product_feedback_window_bytes, reliable_product_recovery_window_bytes,
    reliable_relay_buffer_len,
};
use crate::model::path::{CarrierPathInstanceId, RelayPathInstance, RelayPathKey};
use crate::model::timing::{reliable_data_retransmission_interval, transport_pto_from_snapshot};
use crate::model::work::{
    ReliableReinjectionTargetWork, reliable_critical_tail_reinjection_limit_bytes,
    reliable_reinjection_service_limit_bytes,
};
use crate::mux::MuxLimits;
use crate::mux::stream::validate_stream_ack;
use crate::protocol::frame::reliable_stream_frame_extent;
use crate::protocol::{PathId, StreamId, UnderlayProtocol};
use crate::runtime::stream::reliable_stream_recv_progress_interval;
use crate::scheduler::TrafficClass;
use std::collections::VecDeque;
use std::io::IoSlice;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::AsyncWrite;

#[derive(Default)]
struct VectoredWriteProbe {
    bytes: Vec<u8>,
    scalar_writes: usize,
    vectored_writes: usize,
    flushes: usize,
}

impl AsyncWrite for VectoredWriteProbe {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.scalar_writes = self.scalar_writes.saturating_add(1);
        self.bytes.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.flushes = self.flushes.saturating_add(1);
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        self.vectored_writes = self.vectored_writes.saturating_add(1);
        let written = bufs.iter().map(|slice| slice.len()).sum();
        for slice in bufs {
            self.bytes.extend_from_slice(slice);
        }
        Poll::Ready(Ok(written))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }
}

fn attributed_stream_data(
    path: RelayPathInstance,
    stream_id: StreamId,
    offset: u64,
    payload: &'static [u8],
) -> (RelayPathInstance, Frame) {
    (
        path,
        Frame::StreamData {
            stream_id,
            offset,
            payload: Bytes::from_static(payload),
        },
    )
}

fn attributed_stream_data_extent(
    item: &(RelayPathInstance, Frame),
) -> Option<(StreamId, u64, usize)> {
    match &item.1 {
        Frame::StreamData {
            stream_id,
            offset,
            payload,
        } => Some((*stream_id, *offset, payload.len())),
        _ => None,
    }
}

#[tokio::test]
async fn ready_stream_data_batch_preserves_attribution_boundaries_and_vectored_delivery() {
    let stream_id = StreamId(700);
    let tcp_path = RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        },
        path_instance_id: CarrierPathInstanceId::from_raw(70),
        attachment_id: 700,
    };
    let udp_path = RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 1,
        },
        path_instance_id: CarrierPathInstanceId::from_raw(71),
        attachment_id: 701,
    };
    let first = attributed_stream_data(tcp_path, stream_id, 0, b"abcd");
    let mut queued = VecDeque::from([
        attributed_stream_data(udp_path, stream_id, 4, b"efgh"),
        attributed_stream_data(tcp_path, stream_id, 8, b"ijkl"),
        (
            udp_path,
            Frame::StreamFin {
                stream_id,
                final_offset: 12,
            },
        ),
        attributed_stream_data(tcp_path, stream_id, 12, b"must-not-cross-fin"),
    ]);
    let ready_items = queued.len();
    let mut batch = ReadyStreamDataBatch::new();
    let deferred = collect_ready_stream_data_batch(
        &mut batch,
        first,
        ReadyStreamDataBatchBounds {
            stream_id,
            receive_frontier: 0,
            receive_limit: 12,
            payload_limit: 12,
            ready_items,
        },
        || queued.pop_front(),
        attributed_stream_data_extent,
    );

    assert_eq!(batch.len(), 3);
    assert_eq!(batch.payload_bytes(), 12);
    assert!(
        batch.items_spilled(),
        "a real multi-frame batch grows reusable item storage once"
    );
    let retained_item_capacity = batch.item_capacity();
    assert!(matches!(
        deferred,
        Some((
            path,
            Frame::StreamFin {
                stream_id: received_stream_id,
                final_offset: 12,
            },
        )) if path == udp_path && received_stream_id == stream_id
    ));
    assert_eq!(
        queued.len(),
        1,
        "the collector must stop polling at the first boundary"
    );

    let mut recv_stream = ReliableRecvStream::new(stream_id, MuxLimits::default());
    let mut writer = VectoredWriteProbe::default();
    let mut observed_paths = Vec::new();
    let applied = apply_ready_stream_data_batch(
        &mut recv_stream,
        &mut batch,
        ReadyStreamDataDirection::ClientDownload,
        false,
        |recv_stream, (path, frame)| {
            observed_paths.push(path);
            let Frame::StreamData {
                stream_id: received_stream_id,
                offset,
                payload,
            } = frame
            else {
                unreachable!("collector admitted only STREAM_DATA");
            };
            assert_eq!(received_stream_id, stream_id);
            recv_stream
                .receive_data(offset, payload)
                .map_err(RuntimeError::Stream)
        },
    );
    assert!(!applied.has_apply_error());
    assert_eq!(recv_stream.next_offset(), 12);
    assert!(
        writer.bytes.is_empty(),
        "the synchronous phase must not take local-I/O ownership"
    );
    let written = write_applied_ready_stream_data_batch(&mut writer, &mut batch, applied)
        .await
        .expect("apply and write ready data");

    assert_eq!(observed_paths, vec![tcp_path, udp_path, tcp_path]);
    assert_eq!(written, 12);
    assert_eq!(writer.bytes, b"abcdefghijkl");
    assert_eq!(writer.scalar_writes, 0);
    assert_eq!(writer.vectored_writes, 1);
    assert_eq!(writer.flushes, 1);
    assert!(batch.items_spilled());
    assert_eq!(batch.item_capacity(), retained_item_capacity);
    assert!(
        !batch.delivered_spilled(),
        "the existing eight-iovec span keeps this delivered batch inline"
    );

    let first = attributed_stream_data(udp_path, stream_id, 12, b"mnop");
    let mut queued = VecDeque::from([attributed_stream_data(tcp_path, stream_id, 16, b"qrst")]);
    let deferred = collect_ready_stream_data_batch(
        &mut batch,
        first,
        ReadyStreamDataBatchBounds {
            stream_id,
            receive_frontier: 12,
            receive_limit: 20,
            payload_limit: 8,
            ready_items: queued.len(),
        },
        || queued.pop_front(),
        attributed_stream_data_extent,
    );
    assert!(deferred.is_none());
    assert_eq!(
        batch.item_capacity(),
        retained_item_capacity,
        "later batches reuse the relay-lifetime allocation"
    );
    apply_and_write_ready_stream_data_batch(
        &mut writer,
        &mut recv_stream,
        &mut batch,
        ReadyStreamDataDirection::ServerUpload,
        false,
        |recv_stream, (_, frame)| {
            let Frame::StreamData {
                offset, payload, ..
            } = frame
            else {
                unreachable!("collector admitted only STREAM_DATA");
            };
            recv_stream
                .receive_data(offset, payload)
                .map_err(RuntimeError::Stream)
        },
    )
    .await
    .expect("reuse batch for server-upload direction");
    assert_eq!(recv_stream.next_offset(), 20);
    assert_eq!(writer.bytes, b"abcdefghijklmnopqrst");
    assert_eq!(writer.scalar_writes, 0);
    assert_eq!(writer.vectored_writes, 2);
    assert_eq!(writer.flushes, 2);

    let first = attributed_stream_data(tcp_path, stream_id, 20, b"uvwx");
    let mut queued = VecDeque::from([attributed_stream_data(udp_path, stream_id, 24, b"yz12")]);
    let deferred = collect_ready_stream_data_batch(
        &mut batch,
        first,
        ReadyStreamDataBatchBounds {
            stream_id,
            receive_frontier: 20,
            receive_limit: 28,
            payload_limit: 8,
            ready_items: queued.len(),
        },
        || queued.pop_front(),
        attributed_stream_data_extent,
    );
    assert!(deferred.is_none());
    let mut applied = 0usize;
    let applied_batch = apply_ready_stream_data_batch(
        &mut recv_stream,
        &mut batch,
        ReadyStreamDataDirection::ServerUpload,
        false,
        |recv_stream, (_, frame)| {
            applied = applied.saturating_add(1);
            if applied == 2 {
                return Err(RuntimeError::Protocol("synthetic later-frame failure"));
            }
            let Frame::StreamData {
                offset, payload, ..
            } = frame
            else {
                unreachable!("collector admitted only STREAM_DATA");
            };
            recv_stream
                .receive_data(offset, payload)
                .map_err(RuntimeError::Stream)
        },
    );
    assert!(
        applied_batch.has_apply_error(),
        "the synchronous phase exposes a deferred later-frame failure"
    );
    assert_eq!(
        writer.bytes, b"abcdefghijklmnopqrst",
        "an applied prefix remains pending until the write phase"
    );
    let error = write_applied_ready_stream_data_batch(&mut writer, &mut batch, applied_batch)
        .await
        .expect_err("later apply failure remains terminal");
    assert!(matches!(
        error,
        RuntimeError::Protocol("synthetic later-frame failure")
    ));
    assert_eq!(
        recv_stream.next_offset(),
        24,
        "only successfully applied prefix state commits"
    );
    assert_eq!(
        writer.bytes, b"abcdefghijklmnopqrstuvwx",
        "already committed receive state is written before surfacing a later error"
    );
    assert_eq!(writer.scalar_writes, 1);
    assert_eq!(writer.vectored_writes, 2);
    assert_eq!(writer.flushes, 3);
}

#[test]
fn ready_stream_data_batch_stops_at_every_geometry_and_resource_boundary() {
    #[derive(Clone, Copy)]
    struct Case {
        name: &'static str,
        next_stream_id: StreamId,
        next_offset: u64,
        next_payload: &'static [u8],
        receive_limit: u64,
        payload_limit: usize,
    }

    let stream_id = StreamId(701);
    let path = RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        },
        path_instance_id: CarrierPathInstanceId::from_raw(72),
        attachment_id: 702,
    };
    for case in [
        Case {
            name: "gap",
            next_stream_id: stream_id,
            next_offset: 5,
            next_payload: b"efgh",
            receive_limit: u64::MAX,
            payload_limit: 64,
        },
        Case {
            name: "overlap",
            next_stream_id: stream_id,
            next_offset: 3,
            next_payload: b"efgh",
            receive_limit: u64::MAX,
            payload_limit: 64,
        },
        Case {
            name: "different stream",
            next_stream_id: StreamId(702),
            next_offset: 4,
            next_payload: b"efgh",
            receive_limit: u64::MAX,
            payload_limit: 64,
        },
        Case {
            name: "empty payload",
            next_stream_id: stream_id,
            next_offset: 4,
            next_payload: b"",
            receive_limit: u64::MAX,
            payload_limit: 64,
        },
        Case {
            name: "flow or FIN limit",
            next_stream_id: stream_id,
            next_offset: 4,
            next_payload: b"efgh",
            receive_limit: 7,
            payload_limit: 64,
        },
        Case {
            name: "byte limit",
            next_stream_id: stream_id,
            next_offset: 4,
            next_payload: b"efgh",
            receive_limit: u64::MAX,
            payload_limit: 7,
        },
    ] {
        let first = attributed_stream_data(path, stream_id, 0, b"abcd");
        let mut queued = VecDeque::from([attributed_stream_data(
            path,
            case.next_stream_id,
            case.next_offset,
            case.next_payload,
        )]);
        let mut batch = ReadyStreamDataBatch::new();
        let deferred = collect_ready_stream_data_batch(
            &mut batch,
            first,
            ReadyStreamDataBatchBounds {
                stream_id,
                receive_frontier: 0,
                receive_limit: case.receive_limit,
                payload_limit: case.payload_limit,
                ready_items: queued.len(),
            },
            || queued.pop_front(),
            attributed_stream_data_extent,
        );
        assert_eq!(batch.len(), 1, "{}", case.name);
        assert!(deferred.is_some(), "{}", case.name);
        assert!(
            !batch.items_spilled(),
            "single-frame boundary path must stay allocation-free: {}",
            case.name
        );
    }

    let first = attributed_stream_data(path, stream_id, 0, b"abcd");
    let mut queued = VecDeque::from([
        attributed_stream_data(path, stream_id, 4, b"efgh"),
        attributed_stream_data(path, stream_id, 8, b"ijkl"),
    ]);
    let mut batch = ReadyStreamDataBatch::new();
    let deferred = collect_ready_stream_data_batch(
        &mut batch,
        first,
        ReadyStreamDataBatchBounds {
            stream_id,
            receive_frontier: 0,
            receive_limit: u64::MAX,
            payload_limit: 64,
            ready_items: 1,
        },
        || queued.pop_front(),
        attributed_stream_data_extent,
    );
    assert_eq!(batch.len(), 2, "item snapshot admits only one extra frame");
    assert!(deferred.is_none());
    assert_eq!(
        queued.len(),
        1,
        "items beyond the entry snapshot must not be polled"
    );
}

#[test]
fn stream_fin_waits_for_final_offset_before_close() {
    let mut recv_stream = ReliableRecvStream::new(StreamId(1), MuxLimits::default());
    let mut pending_final_offset = None;

    assert!(
        !receive_stream_fin(&recv_stream, &mut pending_final_offset, 5)
            .expect("record pending fin")
    );
    assert_eq!(pending_final_offset, Some(5));
    assert!(!pending_stream_fin_ready(
        &recv_stream,
        pending_final_offset
    ));

    recv_stream
        .receive_data(0, Bytes::from_static(b"hello"))
        .expect("tail data");

    assert!(pending_stream_fin_ready(&recv_stream, pending_final_offset));
}

#[test]
fn stream_fin_rejects_final_offset_behind_reordered_data() {
    let mut recv_stream = ReliableRecvStream::new(StreamId(405), MuxLimits::default());
    recv_stream
        .receive_data(8, Bytes::from_static(b"tail"))
        .expect("buffer reordered data");
    let mut pending_final_offset = None;

    assert!(matches!(
        receive_stream_fin(&recv_stream, &mut pending_final_offset, 10),
        Err(RuntimeError::Protocol(
            "stream FIN final offset is behind received data"
        ))
    ));
    assert_eq!(pending_final_offset, None);
}

#[test]
fn pending_stream_fin_rejects_data_beyond_final_offset() {
    assert!(validate_stream_data_final_offset(Some(10), 8, 2).is_ok());
    assert!(matches!(
        validate_stream_data_final_offset(Some(10), 8, 3),
        Err(RuntimeError::Protocol(
            "stream data exceeds declared final offset"
        ))
    ));
    assert!(validate_stream_data_final_offset(None, u64::MAX, usize::MAX).is_ok());
}

#[test]
fn in_order_stream_fin_remains_pending_until_feedback_commits() {
    let recv_stream = ReliableRecvStream::new(StreamId(2), MuxLimits::default());
    let mut pending_final_offset = None;

    assert!(
        receive_stream_fin(&recv_stream, &mut pending_final_offset, 0)
            .expect("record in-order fin")
    );
    assert_eq!(pending_final_offset, Some(0));
    assert!(pending_stream_fin_ready(&recv_stream, pending_final_offset));
}

#[test]
fn terminal_fin_replay_is_independent_of_payload_ack_progress() {
    assert!(!stream_terminal_fin_replay_required(false, false, true));
    assert!(!stream_terminal_fin_replay_required(true, true, true));
    assert!(!stream_terminal_fin_replay_required(true, false, false));
    assert!(stream_terminal_fin_replay_required(true, false, true));
}

#[test]
fn duplicate_stream_data_below_final_frontier_is_already_delivered() {
    let mut recv_stream = ReliableRecvStream::new(StreamId(1), MuxLimits::default());
    recv_stream
        .receive_data(0, Bytes::from_static(b"hello"))
        .expect("receive data");

    assert!(stream_data_range_already_delivered(&recv_stream, 0, 5));
    assert!(!stream_data_range_already_delivered(&recv_stream, 0, 6));
    assert!(!stream_data_range_already_delivered(&recv_stream, 5, 1));
}

#[test]
fn ack_gap_reinjection_requires_multipath_alternative_and_persistent_gap() {
    assert!(!stream_ack_gap_reinjection_allowed(true, false, true));
    assert!(!stream_ack_gap_reinjection_allowed(true, true, false));
    assert!(stream_ack_gap_reinjection_allowed(true, true, true));
    assert!(!stream_ack_gap_reinjection_allowed(false, true, true));
}

#[test]
fn ack_gap_reinjection_requires_authoritative_ack_gap_shape() {
    assert!(!stream_ack_ranges_expose_authoritative_gap(
        false,
        &[
            OffsetRange {
                start: 0,
                end: 1024,
            },
            OffsetRange {
                start: 2048,
                end: 4096,
            },
        ],
    ));
    assert!(!stream_ack_ranges_expose_authoritative_gap(
        true,
        &[OffsetRange {
            start: 0,
            end: 1024,
        }],
    ));
    assert!(stream_ack_ranges_expose_authoritative_gap(
        true,
        &[OffsetRange {
            start: 1024,
            end: 4096,
        }],
    ));
    assert!(stream_ack_ranges_expose_authoritative_gap(
        true,
        &[
            OffsetRange {
                start: 0,
                end: 1024,
            },
            OffsetRange {
                start: 2048,
                end: 4096,
            },
        ],
    ));
}

#[test]
fn final_ack_extent_uses_the_exact_lowest_hole_across_frame_boundaries() {
    assert_eq!(
        normalized_stream_ack_first_uncovered_extent(
            &[
                OffsetRange { start: 0, end: 20 },
                OffsetRange {
                    start: 80,
                    end: 100
                },
            ],
            160,
        ),
        Some((20, 80)),
    );
    assert_eq!(
        normalized_stream_ack_first_uncovered_extent(&[OffsetRange { start: 0, end: 100 }], 160,),
        Some((100, 160)),
    );
    assert_eq!(
        normalized_stream_ack_first_uncovered_extent(&[], 160),
        Some((0, 160)),
    );
}

#[test]
fn exact_cached_prefix_crosses_storage_chunks_without_changing_the_scored_extent() {
    let stream_id = StreamId(776);
    let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
    send_stream
        .send_data(Bytes::from(vec![0x61; 1024]))
        .expect("cache 1 KiB first chunk");
    send_stream
        .send_data(Bytes::from(vec![0x62; 63 * 1024]))
        .expect("cache 63 KiB second chunk");

    let frames = exact_contiguous_retransmission_frames(
        &send_stream,
        OffsetRange {
            start: 0,
            end: 64 * 1024,
        },
    )
    .expect("storage chunking cannot shorten an exact cached Product prefix");

    assert_eq!(frames.len(), 2);
    assert_eq!(
        reliable_stream_frame_extent(&frames[0]),
        Some((0, 1024, 1024))
    );
    assert_eq!(
        reliable_stream_frame_extent(&frames[1]),
        Some((1024, 64 * 1024, 63 * 1024)),
    );
}

#[test]
fn exact_cached_prefix_fails_closed_at_a_retention_hole() {
    let stream_id = StreamId(777);
    let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
    send_stream
        .send_data(Bytes::from(vec![0x63; 64 * 1024]))
        .expect("cache Product data");
    send_stream
        .apply_ack(&[OffsetRange {
            start: 1024,
            end: 2048,
        }])
        .expect("release a middle cache slice");

    assert!(
        exact_contiguous_retransmission_frames(
            &send_stream,
            OffsetRange {
                start: 0,
                end: 64 * 1024,
            },
        )
        .is_none(),
        "a range model may not score or bind bytes absent from retained cache",
    );
}

#[test]
fn shared_relay_ack_transaction_rejects_extent_before_stream_mutation() {
    let mut send_stream = ReliableSendStream::new(StreamId(77), MuxLimits::default());
    send_stream
        .send_data(Bytes::from_static(b"abcdefgh"))
        .expect("send assigned relay bytes");
    let before = send_stream.clone();

    let rejected =
        begin_reliable_stream_ack(&send_stream, true, vec![OffsetRange { start: 4, end: 9 }]);

    assert!(matches!(
        rejected,
        Err(StreamError::AckRangeBeyondAssigned {
            start: 4,
            end: 9,
            assigned_end: 8,
        })
    ));
    assert_eq!(send_stream, before);
}

#[test]
fn authoritative_ack_snapshot_merges_positive_incomplete_delta_without_regressing() {
    let mut snapshot = AuthoritativeStreamAckSnapshot::default();
    let complete = validate_stream_ack(
        true,
        vec![
            OffsetRange { start: 0, end: 64 },
            OffsetRange {
                start: 96,
                end: 128,
            },
        ],
        256,
    )
    .expect("complete ACK is within its assigned horizon");
    update_reinjection_authoritative_ack_snapshot(&mut snapshot, &complete);
    let filled_gap = validate_stream_ack(false, vec![OffsetRange { start: 64, end: 96 }], 256)
        .expect("positive delta is within the later assigned extent");
    update_reinjection_authoritative_ack_snapshot(&mut snapshot, &filled_gap);
    let after_snapshot = validate_stream_ack(
        false,
        vec![OffsetRange {
            start: 192,
            end: 256,
        }],
        256,
    )
    .expect("positive delta is within the later assigned extent");
    update_reinjection_authoritative_ack_snapshot(&mut snapshot, &after_snapshot);

    assert_eq!(snapshot.ranges(), &[OffsetRange { start: 0, end: 128 }]);
    assert!(snapshot.complete());
    assert_eq!(snapshot.horizon(), Some(128));
    assert!(snapshot.has_unacknowledged_extent(64));
    assert!(!snapshot.has_unacknowledged_extent(128));
}

#[test]
fn retained_authoritative_ack_state_subsumes_redundant_publication() {
    let mut snapshot = AuthoritativeStreamAckSnapshot::default();
    let initial = validate_stream_ack(
        true,
        vec![
            OffsetRange { start: 0, end: 64 },
            OffsetRange {
                start: 96,
                end: 128,
            },
        ],
        256,
    )
    .expect("initial ACK is valid");
    assert!(!snapshot.subsumes(&initial));
    update_reinjection_authoritative_ack_snapshot(&mut snapshot, &initial);

    let duplicate = validate_stream_ack(true, initial.ranges().to_vec(), 256)
        .expect("redundant cumulative ACK is valid");
    let covered_delta = validate_stream_ack(false, vec![OffsetRange { start: 16, end: 48 }], 256)
        .expect("covered positive update is valid");
    let new_positive = validate_stream_ack(
        false,
        vec![OffsetRange {
            start: 128,
            end: 160,
        }],
        256,
    )
    .expect("new positive update is valid");
    let newer_snapshot = validate_stream_ack(true, vec![OffsetRange { start: 0, end: 160 }], 256)
        .expect("newer cumulative ACK is valid");

    assert!(snapshot.subsumes(&duplicate));
    assert!(snapshot.subsumes(&covered_delta));
    assert!(!snapshot.subsumes(&new_positive));
    assert!(!snapshot.subsumes(&newer_snapshot));
}

#[test]
fn incomplete_ack_cannot_establish_gap_authority() {
    let mut snapshot = AuthoritativeStreamAckSnapshot::default();
    let incomplete = validate_stream_ack(
        false,
        vec![OffsetRange {
            start: 192,
            end: 256,
        }],
        256,
    )
    .expect("positive ACK is within assigned data");
    update_reinjection_authoritative_ack_snapshot(&mut snapshot, &incomplete);

    assert!(snapshot.ranges().is_empty());
    assert!(!snapshot.complete());
    assert_eq!(snapshot.horizon(), None);
}

#[test]
fn complete_ack_negative_authority_never_infers_an_unobserved_assignment_tail() {
    let mut snapshot = AuthoritativeStreamAckSnapshot::default();
    let empty =
        validate_stream_ack(true, Vec::new(), 128).expect("empty complete ACK is well formed");
    update_reinjection_authoritative_ack_snapshot(&mut snapshot, &empty);

    assert!(snapshot.complete());
    assert!(snapshot.ranges().is_empty());
    assert_eq!(snapshot.horizon(), Some(0));
    assert!(!snapshot.has_unacknowledged_extent(0));

    let contiguous_prefix = validate_stream_ack(true, vec![OffsetRange { start: 0, end: 64 }], 128)
        .expect("delayed complete prefix is within assigned data");
    update_reinjection_authoritative_ack_snapshot(&mut snapshot, &contiguous_prefix);
    assert_eq!(snapshot.horizon(), Some(64));
    assert!(!snapshot.has_unacknowledged_extent(64));

    let later_positive = validate_stream_ack(
        false,
        vec![OffsetRange {
            start: 128,
            end: 256,
        }],
        256,
    )
    .expect("later positive ACK stays within newly assigned data");
    update_reinjection_authoritative_ack_snapshot(&mut snapshot, &later_positive);
    assert_eq!(snapshot.horizon(), Some(64));
    assert_eq!(snapshot.ranges(), &[OffsetRange { start: 0, end: 64 }]);
}

#[test]
fn authoritative_gap_persistence_ignores_an_older_complete_snapshot() {
    let mut snapshot = AuthoritativeStreamAckSnapshot::default();
    let current = [
        OffsetRange {
            start: 0,
            end: 4096,
        },
        OffsetRange {
            start: 8192,
            end: 16_384,
        },
    ];
    let stale = [
        OffsetRange {
            start: 0,
            end: 2048,
        },
        OffsetRange {
            start: 8192,
            end: 12_288,
        },
    ];
    let now = Instant::now();
    let persistence = Duration::from_millis(300);
    let mut progress = ReliableAckGapReinjectionProgress::default();

    let current_ack = validate_stream_ack(true, current.to_vec(), 16_384)
        .expect("current ACK fits assigned extent");
    update_reinjection_authoritative_ack_snapshot(&mut snapshot, &current_ack);
    assert!(!progress.reinjection_ready_at(
        snapshot.complete(),
        snapshot.ranges(),
        true,
        false,
        now,
    ));

    let stale_ack =
        validate_stream_ack(true, stale.to_vec(), 12_288).expect("stale ACK fits its old horizon");
    update_reinjection_authoritative_ack_snapshot(&mut snapshot, &stale_ack);
    assert_eq!(snapshot.ranges(), current.as_slice());
    assert_eq!(snapshot.horizon(), Some(16_384));
    assert!(progress.reinjection_ready_at(
        snapshot.complete(),
        snapshot.ranges(),
        true,
        true,
        now + persistence,
    ));
}

#[test]
fn critical_reinjection_limit_uses_one_event_quantum() {
    let limits = MuxLimits::default();
    let base_limit = MAX_RELIABLE_SERVICE_QUANTUM_BYTES.min(reliable_relay_buffer_len(limits));
    let reinjection_debt = base_limit.saturating_mul(32);

    let reinjection_limit =
        reliable_critical_tail_reinjection_limit_bytes(base_limit, reinjection_debt, limits);

    assert_eq!(
        reinjection_limit, base_limit,
        "a caller that grants one critical quantum cannot turn it into an unbounded repair flight"
    );
}

#[test]
fn reinjection_service_fails_closed_without_exact_product_authority() {
    let limits = MuxLimits::default();
    let path = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 40.0, 400_000_000.0);

    assert_eq!(path.data_level_limit_bytes, 0);
    assert_eq!(
        reliable_reinjection_service_limit_bytes(
            ReliableReinjectionTargetWork::new(Some(path), 0, 0),
            limits.max_repair_bytes,
            limits,
        ),
        0,
        "an exact target without published Product authority cannot mint an emergency reserve",
    );
}

#[test]
fn native_window_does_not_rewrite_product_recovery_authority() {
    let limits = MuxLimits::default();
    let mut tcp = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 500.0, 400_000_000.0);
    tcp.product_progress_rate_bps = Some(400_000_000.0);
    tcp.has_durable_product_progress = true;
    let base_limit = reliable_bulk_carrier_feed_quantum_bytes(limits).max(
        adaptive_reliable_relay_reinjection_bytes(Some(tcp), TrafficClass::Throughput, limits),
    );
    let target_flight =
        reliable_product_feedback_window_bytes(Some(tcp), TrafficClass::Throughput, limits);
    tcp.data_level_limit_bytes = target_flight as u64;
    tcp.data_level_bytes_in_flight = target_flight as u64;
    tcp.queue_bytes = target_flight as u64;
    tcp.carrier_inflight_limit_bytes = PATH_OPEN_SCORE_BYTES as u64;
    let retained_product_ceiling =
        reliable_product_feedback_window_bytes(Some(tcp), TrafficClass::Throughput, limits);

    assert_eq!(
        reliable_reinjection_service_limit_bytes(
            ReliableReinjectionTargetWork::new(Some(tcp), 0, 0),
            limits.max_repair_bytes,
            limits,
        ),
        reliable_critical_tail_reinjection_limit_bytes(base_limit, limits.max_repair_bytes, limits,),
        "a full target retains one product work quantum while native congestion gates emission",
    );
    assert_eq!(
        retained_product_ceiling, target_flight,
        "a smaller native window gates the carrier writer; it cannot become a second Product controller",
    );
}

#[test]
fn quic_recovery_does_not_replace_product_authority_with_a_sampled_opportunity() {
    let limits = MuxLimits::default();
    let mut quic = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 100.0, 500_000_000.0);
    quic.carrier_inflight_limit_bytes = 1024 * 1024;
    let forward_ceiling =
        reliable_product_feedback_window_bytes(Some(quic), TrafficClass::Throughput, limits);
    quic.data_level_limit_bytes = forward_ceiling as u64;
    let recovery_window =
        reliable_product_recovery_window_bytes(Some(quic), TrafficClass::Throughput, limits);
    let modeled_recovery =
        data_level_service_window_bytes(quic, TrafficClass::Throughput, limits).ceil() as usize;

    assert_eq!(recovery_window, forward_ceiling);
    assert!(
        recovery_window > modeled_recovery,
        "a transient sampled rate must not shrink the exact Product recovery authority",
    );
    assert_eq!(
        reliable_reinjection_service_limit_bytes(
            ReliableReinjectionTargetWork::new(Some(quic), 0, 0),
            limits.max_repair_bytes,
            limits,
        ),
        forward_ceiling,
        "an idle healthy QUIC target keeps the configured Product recovery window",
    );
}

#[test]
fn reinjection_service_is_transport_neutral_above_native_congestion() {
    let limits = MuxLimits::default();
    for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
        let mut path = PathSnapshot::new(PathId(1), underlay, 40.0, 400_000_000.0);
        let target_flight =
            reliable_product_feedback_window_bytes(Some(path), TrafficClass::Throughput, limits);
        path.data_level_limit_bytes = target_flight as u64;
        assert_eq!(
            reliable_reinjection_service_limit_bytes(
                ReliableReinjectionTargetWork::new(Some(path), 0, 0),
                limits.max_repair_bytes,
                limits,
            ),
            reliable_critical_tail_reinjection_limit_bytes(
                target_flight,
                limits.max_repair_bytes,
                limits,
            ),
            "product reinjection is unified while each target retains native admission"
        );
    }
}

#[test]
fn reinjection_service_counts_only_exact_product_recovery_authority() {
    let limits = MuxLimits::default();
    let mut path = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 40.0, 400_000_000.0);
    path.data_level_limit_bytes =
        reliable_product_feedback_window_bytes(Some(path), TrafficClass::Throughput, limits) as u64;
    let target_window =
        reliable_product_recovery_window_bytes(Some(path), TrafficClass::Throughput, limits);
    let emergency_reserve = reliable_bulk_carrier_feed_quantum_bytes(limits).max(
        adaptive_reliable_relay_reinjection_bytes(Some(path), TrafficClass::Throughput, limits),
    );
    let mut occupied = path;
    occupied.data_level_bytes_in_flight = (target_window / 2) as u64;
    let remaining_ordinary = target_window / 2;
    assert!(remaining_ordinary > emergency_reserve);

    assert_eq!(
        reliable_reinjection_service_limit_bytes(
            ReliableReinjectionTargetWork::new(
                Some(occupied),
                remaining_ordinary / 2,
                remaining_ordinary / 2,
            ),
            limits.max_repair_bytes,
            limits,
        ),
        0,
        "exact queued and accepted ReinjectedData consume the remaining target repair authority",
    );

    occupied.queue_bytes = target_window.saturating_mul(2) as u64;
    occupied.bytes_in_flight = target_window.saturating_mul(2) as u64;
    occupied.data_level_queue_bytes = target_window.saturating_mul(2) as u64;
    assert_eq!(
        reliable_reinjection_service_limit_bytes(
            ReliableReinjectionTargetWork::new(Some(occupied), 0, 0),
            limits.max_repair_bytes,
            limits,
        ),
        reliable_critical_tail_reinjection_limit_bytes(
            remaining_ordinary,
            limits.max_repair_bytes,
            limits,
        ),
        "sampled native queue/flight and aggregate Product staging are telemetry, not exact-target repair admission",
    );
}

#[test]
fn native_backlog_cannot_zero_exact_repair_at_500_and_10_mbps() {
    let limits = MuxLimits::default();
    let configured_window =
        usize::try_from(reliable_bulk_product_windows(limits).per_output_product_limit_bytes)
            .unwrap_or(usize::MAX);
    for (rate_bps, carrier_window) in [(500_000_000.0, 6_250_000_u64), (10_000_000.0, 125_000_u64)]
    {
        for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
            let mut path = PathSnapshot::new(PathId(1), underlay, 100.0, rate_bps);
            path.carrier_delivery_rate_bps = Some(rate_bps);
            path.carrier_inflight_limit_bytes = carrier_window;
            path.data_level_limit_bytes = reliable_product_feedback_window_bytes(
                Some(path),
                TrafficClass::Throughput,
                limits,
            ) as u64;
            let target_window = reliable_product_recovery_window_bytes(
                Some(path),
                TrafficClass::Throughput,
                limits,
            );
            assert_eq!(
                target_window, configured_window,
                "sampled rate and native window cannot rewrite Product recovery authority",
            );

            let emergency_reserve = reliable_bulk_carrier_feed_quantum_bytes(limits).max(
                adaptive_reliable_relay_reinjection_bytes(
                    Some(path),
                    TrafficClass::Throughput,
                    limits,
                ),
            );
            path.queue_bytes = target_window.saturating_add(emergency_reserve) as u64;
            path.bytes_in_flight = target_window.saturating_add(emergency_reserve) as u64;

            assert_eq!(
                reliable_reinjection_service_limit_bytes(
                    ReliableReinjectionTargetWork::new(Some(path), 0, 0),
                    limits.max_repair_bytes,
                    limits,
                ),
                target_window,
                "aggregate native queue+flight is not a Product ledger and cannot veto exact repair",
            );
        }
    }
}

#[test]
fn reinjection_consumes_published_product_authority_without_recomputing_older_c() {
    let limits = MuxLimits::default();
    let mut published = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 100.0, 500_000_000.0);
    published.carrier_inflight_limit_bytes = 1024 * 1024;
    published.product_progress_rate_bps = Some(500_000_000.0);
    published.has_durable_product_progress = true;
    published.data_level_limit_bytes = 9_440_536;

    assert_eq!(
        reliable_product_recovery_window_bytes(Some(published), TrafficClass::Throughput, limits,),
        9_440_536,
        "K consumes the timestamp-coherent published P; it cannot reapply an older 1 MiB C to a newer 500 Mbit/s R",
    );
    assert_eq!(
        reliable_reinjection_service_limit_bytes(
            ReliableReinjectionTargetWork::new(Some(published), 0, 0),
            limits.max_repair_bytes,
            limits,
        ),
        9_440_536,
    );
}

#[test]
fn reinjection_service_emergency_quantum_is_one_target_reserve_not_event_credit() {
    let limits = MuxLimits::default();
    let mut path = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 100.0, 400_000_000.0);
    path.data_level_limit_bytes =
        reliable_product_feedback_window_bytes(Some(path), TrafficClass::Throughput, limits) as u64;
    let target_window =
        reliable_product_recovery_window_bytes(Some(path), TrafficClass::Throughput, limits);
    let emergency_quantum = reliable_bulk_carrier_feed_quantum_bytes(limits).max(
        adaptive_reliable_relay_reinjection_bytes(Some(path), TrafficClass::Throughput, limits),
    );
    path.data_level_bytes_in_flight = target_window as u64;

    assert_eq!(
        reliable_reinjection_service_limit_bytes(
            ReliableReinjectionTargetWork::new(Some(path), 0, 0),
            limits.max_repair_bytes,
            limits,
        ),
        emergency_quantum,
        "a full target retains one bounded liveness reserve",
    );

    assert_eq!(
        reliable_reinjection_service_limit_bytes(
            ReliableReinjectionTargetWork::new(Some(path), 0, emergency_quantum),
            limits.max_repair_bytes,
            limits,
        ),
        0,
        "accepted repair consumes the target reserve instead of renewing it on every recovery event",
    );

    assert_eq!(
        reliable_reinjection_service_limit_bytes(
            ReliableReinjectionTargetWork::new(Some(path), 0, emergency_quantum / 2),
            limits.max_repair_bytes,
            limits,
        ),
        emergency_quantum / 2,
        "only the unoccupied portion of the target reserve remains available",
    );

    assert_eq!(
        reliable_reinjection_service_limit_bytes(
            ReliableReinjectionTargetWork::new(
                Some(path),
                emergency_quantum / 2,
                emergency_quantum / 2,
            ),
            limits.max_repair_bytes,
            limits,
        ),
        0,
        "path-unbound queued repair reserves the same emergency authority before dispatch",
    );

    path.queue_bytes = target_window.saturating_add(emergency_quantum) as u64;
    path.bytes_in_flight = target_window.saturating_add(emergency_quantum) as u64;
    assert_eq!(
        reliable_reinjection_service_limit_bytes(
            ReliableReinjectionTargetWork::new(Some(path), 0, 0),
            limits.max_repair_bytes,
            limits,
        ),
        emergency_quantum,
        "native backlog cannot impersonate accepted ReinjectedData or consume the Product reserve",
    );
}

#[test]
fn data_retransmission_keeps_tcp_and_quic_recovery_clocks_separate() {
    assert_eq!(
        reliable_data_retransmission_interval(Some(UnderlayProtocol::Tcp), None),
        Duration::from_secs(1),
    );
    assert_eq!(
        reliable_data_retransmission_interval(Some(UnderlayProtocol::Udp), None),
        transport_pto_from_snapshot(None),
        "TCP data-level recovery and QUIC PTO must not share one timer formula",
    );
}

#[test]
fn exact_failure_reinjection_limit_is_independent_of_optional_budget() {
    let limits = MuxLimits::default();
    let base_limit = MAX_RELIABLE_SERVICE_QUANTUM_BYTES.min(reliable_relay_buffer_len(limits));
    let small_tail = base_limit.saturating_sub(1024).max(1);

    let reinjection_limit =
        reliable_critical_tail_reinjection_limit_bytes(base_limit, small_tail, limits);

    assert_eq!(
        reinjection_limit, small_tail,
        "exact terminal carrier failure retains correctness-recovery authority after optional credit is exhausted"
    );
    assert_eq!(
        reliable_critical_tail_reinjection_limit_bytes(
            limits.max_repair_bytes.saturating_add(base_limit),
            limits.max_repair_bytes.saturating_add(base_limit),
            limits
        ),
        limits.max_repair_bytes.min(limits.max_path_flight_bytes),
        "exact terminal carrier-failure recovery remains bounded by configured reinjection/path-flight caps"
    );
}

#[test]
fn final_tail_critical_reinjection_limit_can_exceed_optional_budget() {
    let limits = MuxLimits::default();
    let base_limit = MAX_RELIABLE_SERVICE_QUANTUM_BYTES.min(reliable_relay_buffer_len(limits));
    let resource_cap = limits.max_repair_bytes.min(limits.max_path_flight_bytes);
    let small_tail = base_limit.saturating_sub(1024).max(1);
    let reinjection_debt = base_limit.saturating_mul(8);

    assert_eq!(
        reliable_critical_tail_reinjection_limit_bytes(base_limit, small_tail, limits),
        small_tail,
        "terminal original-transmission path-tail reinjection may close a retained final tail even after optional reinjection budget is exhausted"
    );

    assert_eq!(
        reliable_critical_tail_reinjection_limit_bytes(base_limit, reinjection_debt, limits),
        base_limit,
        "terminal original-transmission path-tail reinjection keeps a bounded critical path for final stream closure"
    );
    assert_eq!(
        reliable_critical_tail_reinjection_limit_bytes(
            resource_cap.saturating_add(base_limit),
            resource_cap.saturating_add(base_limit),
            limits
        ),
        resource_cap,
        "critical final-tail reinjection remains bounded by configured reinjection resources"
    );
}

#[test]
fn ack_gap_reinjection_still_reinjections_authoritative_ack_gap() {
    let limits = MuxLimits::default();
    let mut send_stream = ReliableSendStream::new(StreamId(9), limits);
    send_stream
        .send_data(Bytes::from_static(&[7; 4096]))
        .expect("send stream data");

    let reinjection_frames = stream_ack_gap_reinjection_frames(
        &send_stream,
        &[
            OffsetRange {
                start: 0,
                end: 1024,
            },
            OffsetRange {
                start: 2048,
                end: 4096,
            },
        ],
        4096,
        true,
        true,
        true,
    );

    assert_eq!(reinjection_frames.len(), 1);
    assert_eq!(
        reliable_stream_frame_extent(&reinjection_frames[0]),
        Some((1024, 2048, 1024))
    );
}

#[test]
fn final_offset_tail_reinjection_can_recover_unacked_terminal_tail() {
    let limits = MuxLimits::default();
    let mut send_stream = ReliableSendStream::new(StreamId(9), limits);
    send_stream
        .send_data(Bytes::from_static(&[7; 4096]))
        .expect("send stream data");

    let reinjection_frames = stream_final_offset_tail_reinjection_frames_normalized(
        &send_stream,
        &[OffsetRange {
            start: 0,
            end: 1024,
        }],
        4096,
        true,
        true,
    );

    assert_eq!(reinjection_frames.len(), 1);
    assert_eq!(
        reliable_stream_frame_extent(&reinjection_frames[0]),
        Some((1024, 4096, 3072))
    );
}

#[test]
fn final_offset_tail_reinjection_can_use_only_available_path() {
    let limits = MuxLimits::default();
    let mut send_stream = ReliableSendStream::new(StreamId(9), limits);
    send_stream
        .send_data(Bytes::from_static(&[7; 4096]))
        .expect("send stream data");

    let reinjection_frames = stream_final_offset_tail_reinjection_frames_normalized(
        &send_stream,
        &[OffsetRange {
            start: 0,
            end: 1024,
        }],
        4096,
        true,
        true,
    );

    assert_eq!(reinjection_frames.len(), 1);
    assert_eq!(
        reliable_stream_frame_extent(&reinjection_frames[0]),
        Some((1024, 4096, 3072)),
        "terminal final-tail reinjection may use the only available path after stall evidence"
    );
}

#[test]
fn final_offset_tail_reinjection_can_recover_tail_with_no_ack_frontier() {
    let limits = MuxLimits::default();
    let mut send_stream = ReliableSendStream::new(StreamId(9), limits);
    send_stream
        .send_data(Bytes::from_static(&[7; 4096]))
        .expect("send stream data");

    let reinjection_frames =
        stream_final_offset_tail_reinjection_frames_normalized(&send_stream, &[], 4096, true, true);

    assert_eq!(reinjection_frames.len(), 1);
    assert_eq!(
        reliable_stream_frame_extent(&reinjection_frames[0]),
        Some((0, 4096, 4096)),
        "a closed stream with no response ACK frontier must be able to reinjection the retained original-transmission path tail from offset zero"
    );
}

#[test]
fn final_offset_tail_reinjection_waits_for_persistent_stall_evidence() {
    let limits = MuxLimits::default();
    let mut send_stream = ReliableSendStream::new(StreamId(9), limits);
    send_stream
        .send_data(Bytes::from_static(&[7; 4096]))
        .expect("send stream data");

    let reinjection_frames = stream_final_offset_tail_reinjection_frames_normalized(
        &send_stream,
        &[OffsetRange {
            start: 0,
            end: 1024,
        }],
        4096,
        true,
        false,
    );

    assert!(
        reinjection_frames.is_empty(),
        "known final offset is not enough to reinject a contiguous original-transmission path tail before persistent stall/failure evidence"
    );
}

#[test]
fn ack_gap_reinjection_progress_keeps_growing_hole_identity() {
    let mut progress = ReliableAckGapReinjectionProgress::default();
    let first = [
        OffsetRange {
            start: 0,
            end: 110_098,
        },
        OffsetRange {
            start: 112_318,
            end: 114_538,
        },
    ];
    let grown = [
        OffsetRange {
            start: 0,
            end: 110_098,
        },
        OffsetRange {
            start: 113_428,
            end: 116_758,
        },
    ];
    let now = Instant::now();
    let interval = reliable_stream_recv_progress_interval(None);
    let reinjection_delay =
        reliable_data_retransmission_interval(Some(UnderlayProtocol::Udp), None);

    assert!(!progress.reinjection_ready_at(true, &first, true, false, now,));
    assert!(!progress.reinjection_ready_at(true, &grown, true, false, now + interval,));
    assert!(
        progress.reinjection_ready_at(true, &grown, true, true, now + reinjection_delay,),
        "a growing ACK horizon with the same missing frontier is one persistent gap"
    );
    assert!(progress.reinjection_ready_at(
        true,
        &grown,
        true,
        true,
        now + reinjection_delay + Duration::from_millis(1),
    ));
    assert!(
        progress.reinjection_ready_at(
            true,
            &grown,
            true,
            true,
            now + reinjection_delay + Duration::from_millis(2),
        ),
        "sender-queue admission is not post-commit repeat authority"
    );
}

#[test]
fn ack_gap_reinjection_progress_accepts_an_advanced_frontier() {
    let mut progress = ReliableAckGapReinjectionProgress::default();
    let first = [
        OffsetRange {
            start: 0,
            end: 1024,
        },
        OffsetRange {
            start: 4096,
            end: 8192,
        },
    ];
    let advanced = [
        OffsetRange {
            start: 0,
            end: 2048,
        },
        OffsetRange {
            start: 4096,
            end: 8192,
        },
    ];
    let now = Instant::now();
    let reinjection_delay =
        reliable_data_retransmission_interval(Some(UnderlayProtocol::Udp), None);

    assert!(!progress.reinjection_ready_at(true, &first, true, false, now,));
    assert!(progress.reinjection_ready_at(true, &first, true, true, now + reinjection_delay,));
    assert!(progress.reinjection_ready_at(
        true,
        &advanced,
        true,
        true,
        now + reinjection_delay + Duration::from_millis(1),
    ));
}

#[test]
fn ack_gap_owner_clocks_are_monotonic_but_target_deadline_is_current() {
    let mut progress = ReliableAckGapReinjectionProgress::default();
    let first = [
        OffsetRange {
            start: 0,
            end: 1024,
        },
        OffsetRange {
            start: 4096,
            end: 8192,
        },
    ];
    let advanced = [
        OffsetRange {
            start: 0,
            end: 2048,
        },
        OffsetRange {
            start: 4096,
            end: 8192,
        },
    ];
    let now = Instant::now();
    let assignment_at = now;
    let timing = crate::model::timing::ReliableDataAckGapTiming {
        assignment_at,
        loss_at: Some(now + Duration::from_millis(100)),
        fallback_at: now + Duration::from_millis(200),
    };
    let later_observation = crate::model::timing::ReliableDataAckGapTiming {
        assignment_at,
        loss_at: Some(now + Duration::from_millis(150)),
        fallback_at: now + Duration::from_millis(250),
    };

    assert_eq!(
        progress.observe_recovery_timing(
            true,
            &first,
            true,
            Some(timing),
            Some(Duration::from_millis(50)),
            Some(Duration::from_millis(200)),
            now,
        ),
        timing.loss_at,
    );
    assert_eq!(
        progress.observe_recovery_timing(true, &first, false, None, None, None, now),
        None,
        "temporary absence of an alternate cannot authorize repair",
    );
    assert_eq!(
        progress.next_reinjection_deadline(),
        None,
        "a departed target cannot leave its candidate wake armed",
    );
    assert_eq!(
        progress.observe_recovery_timing(
            true,
            &first,
            true,
            Some(later_observation),
            Some(Duration::from_millis(50)),
            Some(Duration::from_millis(200)),
            now,
        ),
        timing.loss_at,
        "a later observation or returning alternate cannot restart exact owner clocks",
    );
    assert_eq!(
        progress.observe_recovery_timing(
            true,
            &first,
            true,
            Some(later_observation),
            Some(Duration::from_millis(150)),
            Some(Duration::from_millis(200)),
            now,
        ),
        Some(timing.fallback_at),
        "a slower replacement target must not inherit an earlier target's deadline",
    );
    assert_eq!(
        progress.observe_recovery_timing(true, &first, true, Some(timing), None, None, now,),
        None,
        "without a current target there is no target-bound repair deadline",
    );
    let advanced_timing = crate::model::timing::ReliableDataAckGapTiming {
        assignment_at: now + Duration::from_millis(10),
        loss_at: Some(now + Duration::from_millis(110)),
        fallback_at: now + Duration::from_millis(210),
    };
    assert_eq!(
        progress.observe_recovery_timing(
            true,
            &advanced,
            true,
            Some(advanced_timing),
            Some(Duration::from_millis(50)),
            Some(Duration::from_millis(200)),
            now,
        ),
        advanced_timing.loss_at,
        "an advanced Data ACK frontier starts with its current candidate",
    );
}

#[test]
fn ack_gap_reinjection_requires_measured_loss_but_does_not_own_copy_lifetime() {
    let ranges = [
        OffsetRange {
            start: 0,
            end: 64 * 1024,
        },
        OffsetRange {
            start: 128 * 1024,
            end: 192 * 1024,
        },
    ];
    let now = Instant::now();
    let mut progress = ReliableAckGapReinjectionProgress::default();
    assert!(!progress.reinjection_ready_at(true, &ranges, true, false, now,));
    assert!(progress.reinjection_ready_at(true, &ranges, true, true, now,));
    assert!(
        progress.reinjection_ready_at(true, &ranges, true, true, now + Duration::from_millis(1),),
        "queued overlap owns pre-commit suppression and the exact flight ledger owns post-commit suppression"
    );
}

#[test]
fn accepted_copy_wake_consumes_due_predecessor_without_losing_successor() {
    let now = Instant::now();
    let first = now + Duration::from_millis(10);
    let second = now + Duration::from_millis(30);
    let mut wake = None;

    assert!(!reconcile_accepted_copy_wake(&mut wake, Some(first), now,));
    assert_eq!(wake, Some(first));
    assert!(reconcile_accepted_copy_wake(
        &mut wake,
        Some(second),
        now + Duration::from_millis(11),
    ));
    assert_eq!(
        wake,
        Some(second),
        "consuming D1 must leave a disjoint live D2 armed",
    );
    assert!(!reconcile_accepted_copy_wake(
        &mut wake,
        None,
        now + Duration::from_millis(12),
    ));
    assert_eq!(wake, None, "exact ACK/detach cancels a future wake");
}

#[test]
fn committed_copy_wake_survives_first_observation_after_expiry_and_keeps_batch_minimum() {
    let now = Instant::now();
    let expired = now.checked_sub(Duration::from_millis(1)).unwrap_or(now);
    let later = now + Duration::from_millis(20);
    let mut wake = None;

    retain_accepted_copy_wake(&mut wake, later);
    retain_accepted_copy_wake(&mut wake, expired);
    assert_eq!(
        wake,
        Some(expired),
        "one bounded drain retains its minimum D"
    );
    assert!(
        reconcile_accepted_copy_wake(&mut wake, None, now),
        "a successful commit must remain a due one-shot even when the first ledger observation is already past D",
    );
    assert_eq!(wake, None, "the consumed one-shot does not busy-loop");
}

#[test]
fn accepted_copy_due_boundary_precedes_topology_work() {
    let now = Instant::now();
    assert!(!accepted_copy_wake_is_due(None, now));
    assert!(!accepted_copy_wake_is_due(
        Some(now + Duration::from_millis(1)),
        now,
    ));
    assert!(accepted_copy_wake_is_due(Some(now), now));
    assert!(accepted_copy_wake_is_due(
        Some(now.checked_sub(Duration::from_millis(1)).unwrap_or(now)),
        now,
    ));
}

#[test]
fn request_path_staleness_requires_persistent_missing_data_ack_progress() {
    let path = RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        },
        path_instance_id: CarrierPathInstanceId::from_raw(7),
        attachment_id: 7,
    };
    let now = Instant::now();
    let persistence = Duration::from_millis(300);
    let mut progress = ReliableRequestPathStaleness::default();
    let observation = ReliablePathStalenessObservation::with_persistence(path, true, persistence);

    assert!(progress.stale_paths_at(&[observation], &[], now).is_empty());
    assert_eq!(progress.next_deadline(), Some(now + persistence));
    assert!(
        progress
            .stale_paths_at(
                &[observation],
                &[],
                now + persistence - Duration::from_millis(1),
            )
            .is_empty()
    );
    assert_eq!(
        progress
            .stale_paths_at(&[observation], &[], now + persistence)
            .as_slice(),
        &[path]
    );
    assert!(
        progress
            .stale_paths_at(&[observation], &[path], now + persistence)
            .is_empty(),
        "positive progress on the exact attachment restarts its clock"
    );
    assert_eq!(
        progress
            .stale_paths_at(&[observation], &[], now + persistence * 2)
            .as_slice(),
        &[path],
        "the restarted clock expires after one full persistence interval"
    );
}

#[test]
fn exact_progress_resets_deadline_once_using_the_current_persistence() {
    let path = RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 6,
        },
        path_instance_id: CarrierPathInstanceId::from_raw(26),
        attachment_id: 26,
    };
    let now = Instant::now();
    let initial =
        ReliablePathStalenessObservation::with_persistence(path, true, Duration::from_millis(100));
    let recovered =
        ReliablePathStalenessObservation::with_persistence(path, true, Duration::from_millis(250));
    let later_degraded =
        ReliablePathStalenessObservation::with_persistence(path, true, Duration::from_secs(2));
    let progress_at = now + Duration::from_millis(50);
    let reset_deadline = progress_at + Duration::from_millis(250);
    let mut progress = ReliableRequestPathStaleness::default();

    assert!(progress.stale_paths_at(&[initial], &[], now).is_empty());
    assert!(
        progress
            .stale_paths_at(&[recovered], &[path], progress_at)
            .is_empty(),
        "exact progress resets the owner clock using the persistence observed at that boundary",
    );
    assert_eq!(progress.next_deadline(), Some(reset_deadline));
    assert_eq!(
        progress
            .stale_paths_at(&[later_degraded], &[], reset_deadline)
            .as_slice(),
        &[path],
        "later metric growth cannot move the new absolute deadline after the exact-progress reset",
    );
}

#[test]
fn request_path_staleness_keeps_independent_attachment_clocks() {
    let first = RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        },
        path_instance_id: CarrierPathInstanceId::from_raw(17),
        attachment_id: 17,
    };
    let second = RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 1,
        },
        path_instance_id: CarrierPathInstanceId::from_raw(18),
        attachment_id: 18,
    };
    let now = Instant::now();
    let persistence = Duration::from_millis(300);
    let observations = [
        ReliablePathStalenessObservation::with_persistence(first, true, persistence),
        ReliablePathStalenessObservation::with_persistence(second, true, persistence),
    ];
    let mut progress = ReliableRequestPathStaleness::default();

    assert!(progress.stale_paths_at(&observations, &[], now).is_empty());
    assert!(
        progress
            .stale_paths_at(&observations, &[second], now + Duration::from_millis(200),)
            .is_empty()
    );
    assert_eq!(
        progress
            .stale_paths_at(&observations, &[], now + persistence)
            .as_slice(),
        &[first],
        "progress on the second attachment cannot restart the first clock"
    );
    assert_eq!(
        progress
            .stale_paths_at(
                &observations,
                &[],
                now + persistence + Duration::from_millis(199),
            )
            .as_slice(),
        &[first]
    );
    assert_eq!(
        progress
            .stale_paths_at(
                &observations,
                &[],
                now + persistence + Duration::from_millis(200),
            )
            .as_slice(),
        &[first, second]
    );
}

#[test]
fn request_path_staleness_resets_on_exact_data_ack_progress() {
    let path = RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 1,
        },
        path_instance_id: CarrierPathInstanceId::from_raw(11),
        attachment_id: 11,
    };
    let now = Instant::now();
    let persistence = Duration::from_millis(200);
    let mut progress = ReliableRequestPathStaleness::default();
    let observation = ReliablePathStalenessObservation::with_persistence(path, true, persistence);

    assert!(progress.stale_paths_at(&[observation], &[], now).is_empty());
    assert!(
        progress
            .stale_paths_at(&[observation], &[path], now + persistence)
            .is_empty()
    );
    assert!(
        progress
            .stale_paths_at(
                &[observation],
                &[],
                now + persistence + persistence - Duration::from_millis(1),
            )
            .is_empty()
    );
    assert_eq!(
        progress
            .stale_paths_at(&[observation], &[], now + persistence + persistence,)
            .as_slice(),
        &[path]
    );
}

#[test]
fn partial_data_ack_does_not_erase_request_path_staleness_evidence() {
    let path = RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 2,
        },
        path_instance_id: CarrierPathInstanceId::from_raw(13),
        attachment_id: 13,
    };
    let now = Instant::now();
    let persistence = Duration::from_millis(100);
    let mut progress = ReliableRequestPathStaleness::default();
    let observation = ReliablePathStalenessObservation::with_persistence(path, true, persistence);

    assert!(progress.stale_paths_at(&[observation], &[], now).is_empty());
    assert_eq!(
        progress
            .stale_paths_at(&[observation], &[], now + persistence)
            .as_slice(),
        &[path],
        "an ACK that does not prove progress on this attachment cannot restart it"
    );
}
