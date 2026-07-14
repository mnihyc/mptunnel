use super::super::{
    ClientRepairOutputIdentity, PersistentClientAckGapBatch, PersistentServerAckGapBatch,
    ServerRepairOutputIdentity,
};
use super::*;
use crate::model::path::CarrierPathKey;
use crate::protocol::{PathId, StreamId, UnderlayProtocol};
use crate::runtime::relay_open::{RelayPathInstance, RelayPathKey};
use std::time::{Duration, Instant};

#[test]
fn sender_queue_dispatches_owner_data_before_ordinary_repair() {
    let stream_id = StreamId(77);
    let mut queue = ReliableRelaySenderQueue::default();

    queue.push_data(Bytes::from_static(b"owner"));
    queue.push_repair(Frame::StreamData {
        stream_id,
        offset: 0,
        flags: StreamFlags::NONE,
        payload: Bytes::from_static(b"repair"),
    });

    let (lane, work) = queue
        .pop_front()
        .expect("ordinary owner data should be queued");
    assert_eq!(
        lane,
        ReliableWorkClass::Data,
        "ordinary RepairData must not preempt OwnerData; repair only preempts when explicitly critical"
    );
    assert_eq!(work.payload_bytes, 5);
}

#[test]
fn sender_queue_dispatches_critical_repair_before_owner_data() {
    let stream_id = StreamId(78);
    let mut queue = ReliableRelaySenderQueue::default();

    queue.push_data(Bytes::from_static(b"owner"));
    queue.push_critical_repair_with_cause(
        Frame::StreamData {
            stream_id,
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(b"repair"),
        },
        RelaySendCause::AckGapRepair,
    );

    let (lane, work) = queue.pop_front().expect("critical repair should be queued");
    assert_eq!(
        lane,
        ReliableWorkClass::Repair,
        "critical RepairData closes an active product hole and must preempt later OwnerData"
    );
    assert_eq!(work.payload_bytes, 6);
}

#[test]
fn sender_queue_trims_and_releases_acked_live_tail_repair() {
    let stream_id = StreamId(80);
    let mut queue = ReliableRelaySenderQueue::default();
    queue.push_critical_repair_with_cause(
        Frame::StreamData {
            stream_id,
            offset: 128,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(&[0x5a; 64]),
        },
        RelaySendCause::LiveOwnerTailRepair,
    );

    assert_eq!(
        queue.release_normalized_acked_repairs(&[OffsetRange { start: 0, end: 160 }]),
        32,
    );
    assert_eq!(queue.bytes(), 32);
    assert!(matches!(
        queue.front().map(|(_, work)| &work.kind),
        Some(ReliableRelayQueuedWorkKind::Repair {
            frame: Frame::StreamData { offset: 160, payload, .. },
            cause: RelaySendCause::LiveOwnerTailRepair,
        }) if payload.len() == 32
    ));

    assert_eq!(
        queue.release_normalized_acked_repairs(&[OffsetRange { start: 0, end: 192 }]),
        32,
    );
    assert!(queue.is_empty());
    assert_eq!(queue.bytes(), 0);
}

#[test]
fn sender_queue_discards_only_unusable_live_owner_tail_repair() {
    let stream_id = StreamId(81);
    let mut queue = ReliableRelaySenderQueue::default();
    for cause in [
        RelaySendCause::LiveOwnerTailRepair,
        RelaySendCause::PathFailureRepair,
    ] {
        queue.push_critical_repair_with_cause(
            Frame::StreamData {
                stream_id,
                offset: if cause == RelaySendCause::LiveOwnerTailRepair {
                    0
                } else {
                    64
                },
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(&[0x5b; 64]),
            },
            cause,
        );
    }

    assert_eq!(
        queue.discard_unusable_live_owner_tail_repairs(|_| false),
        64,
    );
    assert_eq!(queue.bytes(), 64);
    assert!(matches!(
        queue.front().map(|(_, work)| &work.kind),
        Some(ReliableRelayQueuedWorkKind::Repair {
            cause: RelaySendCause::PathFailureRepair,
            ..
        })
    ));
}

#[test]
fn sender_queue_discards_stale_bound_repair_without_touching_ordinary_repair() {
    let stream_id = StreamId(82);
    let mut queue = ReliableRelaySenderQueue::default();
    let cause = RelaySendCause::PersistentClientAckGapRepair(PersistentClientAckGapBatch {
        target: ClientRepairOutputIdentity {
            instance: RelayPathInstance {
                key: RelayPathKey {
                    underlay: UnderlayProtocol::Tcp,
                    index: 2,
                },
                id: 7,
            },
        },
        expires_at: Instant::now() + Duration::from_secs(1),
    });
    for (offset, cause) in [(0, cause), (64, RelaySendCause::AckGapRepair)] {
        queue.push_critical_repair_with_cause(
            Frame::StreamData {
                stream_id,
                offset,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(&[0x5c; 64]),
            },
            cause,
        );
    }

    assert_eq!(
        queue.discard_stale_persistent_ack_gap_repairs(|_| false),
        64
    );
    assert_eq!(queue.bytes(), 64);
    assert!(matches!(
        queue.front().map(|(_, work)| &work.kind),
        Some(ReliableRelayQueuedWorkKind::Repair {
            cause: RelaySendCause::AckGapRepair,
            ..
        })
    ));
}

#[test]
fn sender_queue_discards_expired_bound_repair_on_live_output() {
    let mut queue = ReliableRelaySenderQueue::default();
    queue.push_critical_repair_with_cause(
        Frame::StreamData {
            stream_id: StreamId(83),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(&[0x5d; 64]),
        },
        RelaySendCause::PersistentServerAckGapRepair(PersistentServerAckGapBatch {
            target: ServerRepairOutputIdentity {
                key: CarrierPathKey {
                    underlay: UnderlayProtocol::Udp,
                    path_id: PathId(3),
                },
                incarnation: 9,
            },
            expires_at: Instant::now() - Duration::from_millis(1),
        }),
    );

    assert_eq!(queue.discard_stale_persistent_ack_gap_repairs(|_| true), 64);
    assert!(queue.is_empty());
    assert_eq!(queue.bytes(), 0);
}
