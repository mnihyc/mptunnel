use super::*;
use crate::model::capacity::{
    MAX_RELIABLE_SERVICE_QUANTUM_BYTES, PATH_OPEN_SCORE_BYTES,
    adaptive_reliable_relay_inflight_bytes, adaptive_reliable_relay_reinjection_bytes,
    reliable_bulk_carrier_feed_quantum_bytes, reliable_relay_buffer_len,
};
use crate::model::path::{CarrierPathInstanceId, RelayPathInstance, RelayPathKey};
use crate::model::timing::{reliable_data_retransmission_interval, transport_pto_from_snapshot};
use crate::model::work::{
    reliable_critical_tail_reinjection_limit_bytes,
    reliable_failed_original_reinjection_limit_bytes,
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
    let written = apply_and_write_ready_stream_data_batch(
        &mut writer,
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
    )
    .await
    .expect("apply and write ready data");

    assert_eq!(observed_paths, vec![tcp_path, udp_path, tcp_path]);
    assert_eq!(recv_stream.next_offset(), 12);
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
    let error = apply_and_write_ready_stream_data_batch(
        &mut writer,
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
    )
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
        persistence,
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
        persistence,
        now + persistence,
    ));
}

#[test]
fn persistent_ack_gap_reinjection_limit_uses_critical_event_quantum() {
    let limits = MuxLimits::default();
    let base_limit = MAX_RELIABLE_SERVICE_QUANTUM_BYTES.min(reliable_relay_buffer_len(limits));
    let reinjection_debt = base_limit.saturating_mul(32);

    let reinjection_limit =
        reliable_critical_tail_reinjection_limit_bytes(base_limit, reinjection_debt, limits);

    assert_eq!(
        reinjection_limit, base_limit,
        "persistent ACK-gap reinjection may bypass optional budget, but one event reinjections only one bounded quantum"
    );
}

#[test]
fn failed_original_reinjection_uses_available_target_flight() {
    let limits = MuxLimits::default();
    let mut tcp = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 500.0, 400_000_000.0);
    let base_limit = reliable_bulk_carrier_feed_quantum_bytes(limits).max(
        adaptive_reliable_relay_reinjection_bytes(Some(tcp), TrafficClass::Throughput, limits),
    );
    let target_flight =
        adaptive_reliable_relay_inflight_bytes(Some(tcp), TrafficClass::Throughput, limits);
    tcp.data_level_bytes_in_flight = target_flight as u64;
    tcp.queue_bytes = target_flight as u64;
    tcp.carrier_inflight_limit_bytes = PATH_OPEN_SCORE_BYTES as u64;
    let congested_recovery_flight =
        adaptive_reliable_relay_inflight_bytes(Some(tcp), TrafficClass::Throughput, limits);

    assert_eq!(
        reliable_failed_original_reinjection_limit_bytes(
            Some(tcp),
            limits.max_repair_bytes,
            limits,
        ),
        reliable_critical_tail_reinjection_limit_bytes(base_limit, limits.max_repair_bytes, limits,),
        "a full target retains one product work quantum while native congestion gates emission",
    );
    assert_eq!(
        congested_recovery_flight, target_flight,
        "carrier queue and congestion credit gate emission below MPP; they do not shrink the retained-data recovery window a second time"
    );
}

#[test]
fn failed_original_reinjection_is_transport_neutral_above_native_congestion() {
    let limits = MuxLimits::default();
    for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
        let path = PathSnapshot::new(PathId(1), underlay, 40.0, 400_000_000.0);
        let target_flight =
            adaptive_reliable_relay_inflight_bytes(Some(path), TrafficClass::Throughput, limits);
        assert_eq!(
            reliable_failed_original_reinjection_limit_bytes(
                Some(path),
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
fn persistent_ack_gap_reinjection_limit_ignores_optional_budget_exhaustion() {
    let limits = MuxLimits::default();
    let base_limit = MAX_RELIABLE_SERVICE_QUANTUM_BYTES.min(reliable_relay_buffer_len(limits));
    let small_tail = base_limit.saturating_sub(1024).max(1);

    let reinjection_limit =
        reliable_critical_tail_reinjection_limit_bytes(base_limit, small_tail, limits);

    assert_eq!(
        reinjection_limit, small_tail,
        "persistent ACK-gap reinjection is correctness reinjection and must not depend on optional duplicate/probe budget"
    );
    assert_eq!(
        reliable_critical_tail_reinjection_limit_bytes(
            limits.max_repair_bytes.saturating_add(base_limit),
            limits.max_repair_bytes.saturating_add(base_limit),
            limits
        ),
        limits.max_repair_bytes.min(limits.max_path_flight_bytes),
        "persistent ACK-gap reinjection remains bounded by configured reinjection/path-flight caps"
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

    assert!(!progress.reinjection_ready_at(true, &first, true, false, reinjection_delay, now,));
    assert!(!progress.reinjection_ready_at(
        true,
        &grown,
        true,
        false,
        reinjection_delay,
        now + interval,
    ));
    assert!(
        progress.reinjection_ready_at(
            true,
            &grown,
            true,
            true,
            reinjection_delay,
            now + reinjection_delay,
        ),
        "a growing ACK horizon with the same missing frontier is one persistent gap"
    );
    assert!(progress.reinjection_ready_at(
        true,
        &grown,
        true,
        true,
        reinjection_delay,
        now + reinjection_delay + Duration::from_millis(1),
    ));
    progress.record_reinjection_queued_at(now + reinjection_delay + Duration::from_millis(1));
    assert!(!progress.reinjection_ready_at(
        true,
        &grown,
        true,
        true,
        reinjection_delay,
        now + reinjection_delay + Duration::from_millis(2),
    ));
}

#[test]
fn ack_gap_reinjection_progress_resets_repeat_suppression_when_frontier_advances() {
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

    assert!(!progress.reinjection_ready_at(true, &first, true, false, reinjection_delay, now,));
    assert!(progress.reinjection_ready_at(
        true,
        &first,
        true,
        true,
        reinjection_delay,
        now + reinjection_delay,
    ));
    progress.record_reinjection_queued_at(now + reinjection_delay);
    assert!(progress.reinjection_ready_at(
        true,
        &advanced,
        true,
        true,
        reinjection_delay,
        now + reinjection_delay + Duration::from_millis(1),
    ));
}

#[test]
fn ack_gap_recovery_timer_cannot_be_postponed_for_the_same_frontier() {
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
    let first_deadline = now + Duration::from_millis(100);
    let later_deadline = now + Duration::from_millis(200);

    assert_eq!(
        progress.arm_recovery_deadline(true, &first, true, Some(first_deadline)),
        Some(first_deadline),
    );
    assert_eq!(
        progress.arm_recovery_deadline(true, &first, true, Some(later_deadline)),
        Some(first_deadline),
        "metric refresh cannot postpone an armed loss timer",
    );
    assert_eq!(
        progress.arm_recovery_deadline(true, &first, true, None),
        Some(first_deadline),
        "a partial observation cannot disarm established loss evidence",
    );
    assert_eq!(
        progress.arm_recovery_deadline(true, &advanced, true, Some(later_deadline)),
        Some(later_deadline),
        "an advanced Data ACK frontier arms a new flight timer",
    );
}

#[test]
fn ack_gap_reinjection_requires_measured_loss_and_suppresses_repeat_attempts() {
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
    let reinjection_delay = Duration::from_millis(300);

    let mut progress = ReliableAckGapReinjectionProgress::default();
    assert!(!progress.reinjection_ready_at(true, &ranges, true, false, reinjection_delay, now,));
    assert!(progress.reinjection_ready_at(true, &ranges, true, true, reinjection_delay, now,));
    progress.record_reinjection_queued_at(now);
    assert!(!progress.reinjection_ready_at(
        true,
        &ranges,
        true,
        true,
        reinjection_delay,
        now + Duration::from_millis(1),
    ));
    progress.release_reinjection_attempt();
    assert!(progress.reinjection_ready_at(
        true,
        &ranges,
        true,
        true,
        reinjection_delay,
        now + Duration::from_millis(1),
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
