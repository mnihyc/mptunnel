use super::RequestFlightLedger;
use crate::model::path::{RelayPathInstance, RelayPathKey};
use crate::protocol::{Frame, OffsetRange, StreamFlags, StreamId, UnderlayProtocol};
use bytes::Bytes;

fn data_frame(offset: u64, len: usize) -> Frame {
    Frame::StreamData {
        stream_id: StreamId(7),
        offset,
        flags: StreamFlags::NONE,
        payload: Bytes::from(vec![0x5a; len]),
    }
}

#[test]
fn ordering_debt_counts_lower_bytes_owned_by_other_paths() {
    let path0 = RelayPathKey {
        underlay: UnderlayProtocol::Udp,
        index: 0,
    };
    let path1 = RelayPathKey {
        underlay: UnderlayProtocol::Udp,
        index: 1,
    };
    let path2 = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    let mut ledger = RequestFlightLedger::default();
    ledger.record_owner_frame(path0, &data_frame(0, 4096));
    ledger.record_owner_frame(path1, &data_frame(4096, 4096));

    assert_eq!(ledger.ordering_debt_bytes_before_offset(path0, 8192), 4096);
    assert_eq!(ledger.ordering_debt_bytes_before_offset(path1, 8192), 4096);
    assert_eq!(ledger.ordering_debt_bytes_before_offset(path2, 8192), 8192);
    assert_eq!(
        ledger.oldest_lower_flight_owner_before_offset(8192),
        Some(path0)
    );
}

#[test]
fn missing_later_owner_is_detected_even_when_oldest_owner_is_live() {
    let live_owner = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    let missing_owner = RelayPathKey {
        underlay: UnderlayProtocol::Udp,
        index: 0,
    };
    let mut ledger = RequestFlightLedger::default();
    ledger.record_owner_frame(live_owner, &data_frame(0, 4096));
    ledger.record_owner_frame(missing_owner, &data_frame(4096, 4096));
    let live_instance = RelayPathInstance {
        key: live_owner,
        id: 0,
    };
    let missing_instance = RelayPathInstance {
        key: missing_owner,
        id: 0,
    };

    assert!(ledger.has_missing_ordering_owner_before_offset(8192, &[live_instance]));
    assert!(
        !ledger.has_missing_ordering_owner_before_offset(8192, &[live_instance, missing_instance],)
    );
}

#[test]
fn same_key_replacement_does_not_mask_stale_instance_owner_flight() {
    let key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    let stale = RelayPathInstance { key, id: 7 };
    let replacement = RelayPathInstance { key, id: 8 };
    let frame = data_frame(0, 4096);
    let mut ledger = RequestFlightLedger::default();
    ledger.record_owner_frame_instance(stale, &frame);

    assert!(ledger.has_missing_ordering_owner_before_offset(4097, &[replacement]));
    assert!(
        ledger
            .ordering_owner_keys_for_frame(&frame, &[replacement])
            .is_empty()
    );
    assert_eq!(
        ledger.ordering_owner_underlay_for_frame(&frame),
        Some(UnderlayProtocol::Tcp),
        "repair policy must retain the stale OwnerData transport family after same-key replacement"
    );
    assert_eq!(
        ledger.latest_unacked_ranges_for_path_instance(stale),
        vec![OffsetRange {
            start: 0,
            end: 4096,
        }]
    );
    assert!(
        ledger
            .latest_unacked_ranges_for_path_instance(replacement)
            .is_empty()
    );
}

#[test]
fn repair_copy_does_not_become_ordering_owner() {
    let owner = RelayPathKey {
        underlay: UnderlayProtocol::Udp,
        index: 0,
    };
    let duplicate = RelayPathKey {
        underlay: UnderlayProtocol::Udp,
        index: 1,
    };
    let frame = data_frame(0, 4096);
    let mut ledger = RequestFlightLedger::default();
    ledger.record_owner_frame(owner, &frame);
    ledger.record_repair_frame(duplicate, &frame);

    assert_eq!(
        ledger.oldest_lower_flight_owner_before_offset(4096),
        Some(owner)
    );
    assert_eq!(ledger.ordering_debt_bytes_before_offset(owner, 4096), 0);
    assert_eq!(
        ledger.ordering_debt_bytes_before_offset(duplicate, 4096),
        4096
    );

    let released = ledger.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: 4096,
    }]);
    assert_eq!(released.len(), 2);
    assert!(released.iter().any(|release| release.key == owner));
    assert!(released.iter().any(|release| release.key == duplicate));
    assert!(
        released.iter().all(|release| !release.path_proving),
        "ACK of duplicated request bytes releases inflight state but is not path-scoped proof"
    );
}

#[test]
fn owner_only_ack_release_is_path_proving() {
    let owner = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    let frame = data_frame(0, 4096);
    let mut ledger = RequestFlightLedger::default();
    ledger.record_owner_frame(owner, &frame);

    let released = ledger.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: 4096,
    }]);

    assert_eq!(released.len(), 1);
    assert_eq!(released[0].key, owner);
    assert!(
        released[0].path_proving,
        "a single outstanding owner copy is path-scoped STREAM_ACK evidence"
    );
}

#[test]
fn partial_same_start_duplicate_ack_retains_owner_suffix() {
    let owner = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    let repair = RelayPathKey {
        underlay: UnderlayProtocol::Udp,
        index: 0,
    };
    let mut ledger = RequestFlightLedger::default();
    ledger.record_owner_frame(owner, &data_frame(0, 4096));
    ledger.record_repair_frame(repair, &data_frame(0, 1024));

    let prefix_releases = ledger.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: 1024,
    }]);
    assert_eq!(prefix_releases.len(), 2);
    assert!(prefix_releases.iter().all(|release| release.bytes == 1024));
    assert!(
        prefix_releases.iter().all(|release| !release.path_proving),
        "an ACK shared by OwnerData and RepairData cannot identify a delivery path"
    );
    assert_eq!(
        ledger.latest_unacked_ranges_for_path(owner),
        vec![OffsetRange {
            start: 1024,
            end: 4096,
        }],
        "releasing the shorter same-start RepairData copy must retain the OwnerData suffix"
    );
    assert!(ledger.latest_unacked_ranges_for_path(repair).is_empty());
    assert_eq!(
        ledger.ordering_owner_keys_for_frame(
            &data_frame(1024, 3072),
            &[
                RelayPathInstance { key: owner, id: 0 },
                RelayPathInstance { key: repair, id: 0 },
            ],
        ),
        vec![owner],
        "the trimmed suffix retains OwnerData identity without retaining the RepairData key"
    );

    let suffix_releases = ledger.release_normalized_acked_ranges(&[OffsetRange {
        start: 1024,
        end: 4096,
    }]);
    assert_eq!(suffix_releases.len(), 1);
    assert_eq!(suffix_releases[0].key, owner);
    assert_eq!(suffix_releases[0].bytes, 3072);
    assert!(
        suffix_releases[0].path_proving,
        "the retained owner-only suffix is unambiguous when it is acknowledged later"
    );
    assert!(ledger.latest_unacked_ranges_for_path(owner).is_empty());
}
