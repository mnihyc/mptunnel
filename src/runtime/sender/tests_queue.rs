use super::super::{
    ClientReinjectionOutputIdentity, PersistentClientAckGapBatch, PersistentServerAckGapBatch,
    ServerReinjectionOutputIdentity,
};
use super::*;
use crate::model::path::CarrierPathKey;
use crate::model::path::{CarrierPathInstanceId, RelayPathInstance, RelayPathKey};
use crate::mux::MuxLimits;
use crate::mux::stream::ReliableSendStream;
use crate::protocol::{PathId, StreamId, UnderlayProtocol};
use std::time::{Duration, Instant};

#[test]
fn sender_queue_read_budget_respects_stream_flow_control_credit() {
    let limits = MuxLimits {
        max_stream_window_bytes: 4,
        max_repair_bytes: 16,
        max_path_flight_bytes: 16,
        max_reliable_relay_chunk_bytes: 16,
        ..MuxLimits::default()
    };
    let mut send_stream = ReliableSendStream::new(StreamId(7), limits);
    let sender_queue = ReliableRelaySenderQueue::default();
    assert_eq!(
        reliable_relay_sender_queue_limit(limits, 0),
        0,
        "no admissible output grants no source staging"
    );
    send_stream
        .send_data(Bytes::from_static(b"data"))
        .expect("initial window payload");

    assert!(!reliable_relay_can_read_into_sender_queue(
        &send_stream,
        &sender_queue,
        16,
    ));
    assert_eq!(
        reliable_relay_sender_queue_read_budget(&send_stream, &sender_queue, 16, 16),
        0,
    );

    send_stream.update_max_offset(6);
    assert!(reliable_relay_can_read_into_sender_queue(
        &send_stream,
        &sender_queue,
        16,
    ));
    assert_eq!(
        reliable_relay_sender_queue_read_budget(&send_stream, &sender_queue, 16, 16),
        2,
    );
}

#[test]
fn sender_queue_preserves_multipath_product_window_above_one_path_cap() {
    let limits = MuxLimits {
        max_stream_window_bytes: 8 * 1024 * 1024,
        max_repair_bytes: 8 * 1024 * 1024,
        max_reorder_bytes: 8 * 1024 * 1024,
        max_path_flight_bytes: 2 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let summed_product_window = 4 * 1024 * 1024;
    assert_eq!(
        reliable_relay_sender_queue_limit(limits, summed_product_window),
        summed_product_window,
        "a per-path cap cannot collapse an exact two-output source window",
    );
}

#[test]
fn sender_queue_dispatches_original_data_before_ordinary_reinjection() {
    let stream_id = StreamId(77);
    let mut queue = ReliableRelaySenderQueue::default();

    queue.push_data(Bytes::from_static(b"owner"));
    queue.push_reinjection(Frame::StreamData {
        stream_id,
        offset: 0,
        payload: Bytes::from_static(b"reinjection"),
    });

    let (lane, work) = queue
        .pop_front()
        .expect("ordinary owner data should be queued");
    assert_eq!(
        lane,
        ReliableWorkClass::Data,
        "ordinary ReinjectedData must not preempt OriginalData; reinjection only preempts when explicitly critical"
    );
    assert_eq!(work.payload_bytes, 5);
}

#[test]
fn sender_queue_dispatches_critical_reinjection_before_original_data() {
    let stream_id = StreamId(78);
    let mut queue = ReliableRelaySenderQueue::default();

    queue.push_data(Bytes::from_static(b"owner"));
    queue.push_critical_reinjection_with_cause(
        Frame::StreamData {
            stream_id,
            offset: 0,
            payload: Bytes::from_static(b"reinjection"),
        },
        RelaySendCause::AckGapReinjection,
    );

    let (lane, work) = queue
        .pop_front()
        .expect("critical reinjection should be queued");
    assert_eq!(
        lane,
        ReliableWorkClass::Reinjection,
        "critical ReinjectedData closes an active product hole and must preempt later OriginalData"
    );
    assert_eq!(work.payload_bytes, b"reinjection".len());
}

#[test]
fn request_target_queue_view_keeps_bound_and_unbound_repair_authority_separate() {
    let instance = |index, id| RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index,
        },
        path_instance_id: CarrierPathInstanceId::from_raw(id),
        attachment_id: id,
    };
    let first = instance(1, 11);
    let second = instance(2, 12);
    let third = instance(3, 13);
    let owner = instance(0, 10);
    let identity = |instance| ClientReinjectionOutputIdentity { instance };
    let stream_id = StreamId(79);
    let mut queue = ReliableRelaySenderQueue::default();
    queue.push_data(Bytes::from(vec![0x60; 100]));

    let repairs = [
        (10, RelaySendCause::AckGapReinjection),
        (
            20,
            RelaySendCause::ClientPathFailureReinjection(identity(first)),
        ),
        (
            30,
            RelaySendCause::PersistentClientAckGapReinjection(PersistentClientAckGapBatch {
                target: identity(second),
                expires_at: Instant::now() + Duration::from_secs(1),
            }),
        ),
        (
            40,
            RelaySendCause::CompletionTailReinjection(identity(first)),
        ),
        (
            50,
            RelaySendCause::ClientStalePathReinjection {
                owner,
                target: identity(second),
            },
        ),
    ];
    let mut offset = 100u64;
    for (payload_bytes, cause) in repairs {
        queue.push_critical_reinjection_with_cause(
            Frame::StreamData {
                stream_id,
                offset,
                payload: Bytes::from(vec![0x61; payload_bytes]),
            },
            cause,
        );
        offset += payload_bytes as u64;
    }

    assert_eq!(queue.bytes(), 250);
    assert_eq!(queue.reinjection_bytes(), 150);
    assert_eq!(
        queue.request_target_queued_reinjection_bytes(first, false),
        70,
        "unbound repair and first-bound repair consume the first target only",
    );
    assert_eq!(
        queue.request_target_queued_reinjection_bytes(second, false),
        90,
        "unbound repair and second-bound repair consume the second target only",
    );
    assert_eq!(
        queue.request_target_queued_reinjection_bytes(third, false),
        10,
        "repair without a selected target remains conservatively charged to every candidate",
    );
    assert_eq!(
        queue.request_target_queued_reinjection_bytes(first, true),
        60,
        "apply-time revalidation excludes only its own front unbound intent",
    );
    assert_eq!(
        queue.request_target_queued_reinjection_bytes(second, true),
        80,
    );
    assert_eq!(
        queue.request_target_queued_reinjection_bytes(third, true),
        0,
    );
}

#[test]
fn response_target_queue_view_keeps_bound_and_unbound_repair_authority_separate() {
    let identity = |path_id, incarnation| ServerReinjectionOutputIdentity {
        key: CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(path_id),
        },
        incarnation,
    };
    let first = identity(1, 11);
    let second = identity(2, 12);
    let third = identity(3, 13);
    let stale_owner = identity(0, 10);
    let stream_id = StreamId(80);
    let mut queue = ReliableRelaySenderQueue::default();
    queue.push_data(Bytes::from(vec![0x60; 100]));
    queue.push_final_control(Frame::StreamFin {
        stream_id,
        final_offset: 100,
    });

    let repairs = [
        (10, RelaySendCause::AckGapReinjection),
        (
            20,
            RelaySendCause::PersistentServerAckGapReinjection(PersistentServerAckGapBatch {
                target: first,
                expires_at: Instant::now() + Duration::from_secs(1),
            }),
        ),
        (
            30,
            RelaySendCause::PersistentServerAckGapReinjection(PersistentServerAckGapBatch {
                target: second,
                expires_at: Instant::now() + Duration::from_secs(1),
            }),
        ),
        (
            40,
            RelaySendCause::StaleResponsePathReinjection(stale_owner),
        ),
    ];
    let mut offset = 100u64;
    for (payload_bytes, cause) in repairs {
        queue.push_critical_reinjection_with_cause(
            Frame::StreamData {
                stream_id,
                offset,
                payload: Bytes::from(vec![0x61; payload_bytes]),
            },
            cause,
        );
        offset += payload_bytes as u64;
    }

    assert_eq!(queue.reinjection_bytes(), 100);
    assert_eq!(
        queue.response_target_queued_reinjection_bytes(first, false),
        70,
        "unbound repair and first-bound repair consume only the first target",
    );
    assert_eq!(
        queue.response_target_queued_reinjection_bytes(second, false),
        80,
        "unbound repair and second-bound repair consume only the second target",
    );
    assert_eq!(
        queue.response_target_queued_reinjection_bytes(third, false),
        50,
        "Data/control and repair bound elsewhere cannot consume a third target",
    );
    assert_eq!(
        queue.response_target_queued_reinjection_bytes(first, true),
        60,
        "apply-time revalidation excludes only its own front unbound repair",
    );
    assert_eq!(
        queue.response_target_queued_reinjection_bytes(second, true),
        70,
    );
    assert_eq!(
        queue.response_target_queued_reinjection_bytes(third, true),
        40,
    );
}

#[test]
fn sender_queue_trims_and_releases_acked_live_tail_reinjection() {
    let stream_id = StreamId(80);
    let mut queue = ReliableRelaySenderQueue::default();
    queue.push_critical_reinjection_with_cause(
        Frame::StreamData {
            stream_id,
            offset: 128,
            payload: Bytes::from_static(&[0x5a; 64]),
        },
        RelaySendCause::TailReinjection,
    );

    assert_eq!(
        queue.release_normalized_acked_reinjections(&[OffsetRange { start: 0, end: 160 }]),
        32,
    );
    assert_eq!(queue.bytes(), 32);
    assert!(matches!(
        queue.front().map(|(_, work)| &work.kind),
        Some(ReliableRelayQueuedWorkKind::Reinjection {
            frame: Frame::StreamData { offset: 160, payload, .. },
            cause: RelaySendCause::TailReinjection,
        }) if payload.len() == 32
    ));

    assert_eq!(
        queue.release_normalized_acked_reinjections(&[OffsetRange { start: 0, end: 192 }]),
        32,
    );
    assert!(queue.is_empty());
    assert_eq!(queue.bytes(), 0);
}

#[test]
fn sender_queue_discards_only_unusable_tail_reinjection() {
    let stream_id = StreamId(81);
    let mut queue = ReliableRelaySenderQueue::default();
    for cause in [
        RelaySendCause::TailReinjection,
        RelaySendCause::PathFailureReinjection,
    ] {
        queue.push_critical_reinjection_with_cause(
            Frame::StreamData {
                stream_id,
                offset: if cause == RelaySendCause::TailReinjection {
                    0
                } else {
                    64
                },
                payload: Bytes::from_static(&[0x5b; 64]),
            },
            cause,
        );
    }

    assert_eq!(queue.discard_unusable_tail_reinjections(|_| false), 64,);
    assert_eq!(queue.bytes(), 64);
    assert!(matches!(
        queue.front().map(|(_, work)| &work.kind),
        Some(ReliableRelayQueuedWorkKind::Reinjection {
            cause: RelaySendCause::PathFailureReinjection,
            ..
        })
    ));
}

#[test]
fn sender_queue_discards_stale_bound_reinjection_without_touching_ordinary_reinjection() {
    let stream_id = StreamId(82);
    let mut queue = ReliableRelaySenderQueue::default();
    let cause = RelaySendCause::PersistentClientAckGapReinjection(PersistentClientAckGapBatch {
        target: ClientReinjectionOutputIdentity {
            instance: RelayPathInstance {
                key: RelayPathKey {
                    underlay: UnderlayProtocol::Tcp,
                    index: 2,
                },
                path_instance_id: CarrierPathInstanceId::from_raw(7),
                attachment_id: 7,
            },
        },
        expires_at: Instant::now() + Duration::from_secs(1),
    });
    for (offset, cause) in [(0, cause), (64, RelaySendCause::AckGapReinjection)] {
        queue.push_critical_reinjection_with_cause(
            Frame::StreamData {
                stream_id,
                offset,
                payload: Bytes::from_static(&[0x5c; 64]),
            },
            cause,
        );
    }

    assert_eq!(
        queue.discard_stale_persistent_ack_gap_reinjections(|_| false),
        64
    );
    assert_eq!(queue.bytes(), 64);
    assert!(matches!(
        queue.front().map(|(_, work)| &work.kind),
        Some(ReliableRelayQueuedWorkKind::Reinjection {
            cause: RelaySendCause::AckGapReinjection,
            ..
        })
    ));
}

#[test]
fn sender_queue_discards_expired_bound_reinjection_on_live_output() {
    let mut queue = ReliableRelaySenderQueue::default();
    queue.push_critical_reinjection_with_cause(
        Frame::StreamData {
            stream_id: StreamId(83),
            offset: 0,
            payload: Bytes::from_static(&[0x5d; 64]),
        },
        RelaySendCause::PersistentServerAckGapReinjection(PersistentServerAckGapBatch {
            target: ServerReinjectionOutputIdentity {
                key: CarrierPathKey {
                    underlay: UnderlayProtocol::Udp,
                    path_id: PathId(3),
                },
                incarnation: 9,
            },
            expires_at: Instant::now() - Duration::from_millis(1),
        }),
    );

    assert_eq!(
        queue.discard_stale_persistent_ack_gap_reinjections(|_| true),
        64
    );
    assert!(queue.is_empty());
    assert_eq!(queue.bytes(), 0);
}

#[test]
fn sender_queue_discards_reinjection_after_exact_path_progress() {
    let path = RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 4,
        },
        path_instance_id: CarrierPathInstanceId::from_raw(12),
        attachment_id: 12,
    };
    let mut queue = ReliableRelaySenderQueue::default();
    let response = ServerReinjectionOutputIdentity {
        key: CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(5),
        },
        incarnation: 17,
    };
    for (offset, cause) in [
        (0, RelaySendCause::StalePathReinjection(path)),
        (64, RelaySendCause::StaleResponsePathReinjection(response)),
        (128, RelaySendCause::AckGapReinjection),
    ] {
        queue.push_critical_reinjection_with_cause(
            Frame::StreamData {
                stream_id: StreamId(84),
                offset,
                payload: Bytes::from_static(&[0x5e; 64]),
            },
            cause,
        );
    }

    assert_eq!(
        queue.discard_resolved_stale_response_path_reinjections(|candidate| {
            candidate != response
        }),
        64
    );
    assert_eq!(
        queue.discard_resolved_stale_path_reinjections(|candidate| candidate != path),
        64
    );
    assert_eq!(queue.bytes(), 64);
    assert!(matches!(
        queue.front().map(|(_, work)| &work.kind),
        Some(ReliableRelayQueuedWorkKind::Reinjection {
            cause: RelaySendCause::AckGapReinjection,
            ..
        })
    ));
}
