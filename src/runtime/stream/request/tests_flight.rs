use super::RequestFlightLedger;
use crate::model::path::{CarrierPathInstanceId, RelayPathInstance, RelayPathKey};
use crate::protocol::{Frame, OffsetRange, StreamId, UnderlayProtocol};
use crate::runtime::stream::request::RequestPathStates;
use bytes::Bytes;
use std::time::{Duration, Instant};

fn data_frame(offset: u64, len: usize) -> Frame {
    Frame::StreamData {
        stream_id: StreamId(7),
        offset,
        payload: Bytes::from(vec![0x5a; len]),
    }
}

fn path(underlay: UnderlayProtocol, index: usize, id: u64) -> RelayPathInstance {
    RelayPathInstance {
        key: RelayPathKey { underlay, index },
        path_instance_id: CarrierPathInstanceId::from_raw(id.max(1)),
        attachment_id: id,
    }
}

fn assert_original_data_cache(
    ledger: &RequestFlightLedger,
    expected_total: u64,
    expected_instances: &[(RelayPathInstance, u64)],
) {
    let scanned_total = ledger
        .flights
        .values()
        .flat_map(|flights| flights.iter())
        .filter(|flight| flight.kind.is_original_transmission())
        .map(|flight| flight.bytes as u64)
        .sum::<u64>();
    assert_eq!(ledger.total_original_data_in_flight_bytes(), scanned_total);
    assert_eq!(scanned_total, expected_total);
    for (instance, expected_bytes) in expected_instances {
        let scanned_instance = ledger
            .flights
            .values()
            .flat_map(|flights| flights.iter())
            .filter(|flight| flight.instance == *instance && flight.kind.is_original_transmission())
            .map(|flight| flight.bytes as u64)
            .sum::<u64>();
        assert_eq!(
            ledger.original_data_in_flight_bytes(*instance),
            scanned_instance,
        );
        assert_eq!(scanned_instance, *expected_bytes);
    }
}

#[test]
fn duplicate_data_ack_releases_original_and_reinjected_flights_without_path_proof() {
    let owner = path(UnderlayProtocol::Tcp, 0, 7);
    let reinjection = path(UnderlayProtocol::Udp, 1, 11);
    let frame = data_frame(0, 4096);
    let mut ledger = RequestFlightLedger::default();

    assert_eq!(ledger.record_original_frame_instance(owner, &frame), 4096);
    assert_eq!(
        ledger.record_reinjection_frame_instance(reinjection, &frame),
        4096
    );
    assert_eq!(ledger.original_transmission_instances(), vec![owner]);
    assert_eq!(
        ledger.latest_unacked_ranges_for_path_instance(owner),
        vec![OffsetRange {
            start: 0,
            end: 4096,
        }]
    );
    assert!(
        ledger
            .latest_unacked_ranges_for_path_instance(reinjection)
            .is_empty(),
        "a reinjection copy must not become the ordering owner"
    );

    let released = ledger.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: 4096,
    }]);

    assert_eq!(released.len(), 2);
    for instance in [owner, reinjection] {
        let release = released
            .iter()
            .find(|release| release.instance == instance)
            .expect("the ACK releases every exact flight carrying these bytes");
        assert_eq!(release.bytes, 4096);
        assert!(
            !release.path_proving,
            "duplicated bytes cannot identify which path delivered them"
        );
    }
    assert!(ledger.original_transmission_instances().is_empty());
}

#[test]
fn total_original_data_flight_cache_tracks_original_only_ack_and_drain() {
    let owner = path(UnderlayProtocol::Tcp, 0, 7);
    let other = path(UnderlayProtocol::Tcp, 1, 9);
    let duplicate = path(UnderlayProtocol::Udp, 1, 11);
    let first = data_frame(0, 4096);
    let second = data_frame(4096, 4096);
    let mut ledger = RequestFlightLedger::default();

    assert_original_data_cache(&ledger, 0, &[(owner, 0), (other, 0), (duplicate, 0)]);
    ledger.record_original_frame_instance(owner, &first);
    ledger.record_original_frame_instance(other, &second);
    ledger.record_reinjection_frame_instance(duplicate, &first);
    assert_original_data_cache(
        &ledger,
        8192,
        &[(owner, 4096), (other, 4096), (duplicate, 0)],
    );

    ledger.release_normalized_acked_ranges(&[OffsetRange {
        start: 1024,
        end: 6144,
    }]);
    assert_original_data_cache(
        &ledger,
        3072,
        &[(owner, 1024), (other, 2048), (duplicate, 0)],
    );

    ledger.release_normalized_acked_ranges(&[
        OffsetRange { start: 0, end: 512 },
        OffsetRange {
            start: 7168,
            end: 8192,
        },
    ]);
    assert_original_data_cache(
        &ledger,
        1536,
        &[(owner, 512), (other, 1024), (duplicate, 0)],
    );

    ledger.drain_all();
    assert_original_data_cache(&ledger, 0, &[(owner, 0), (other, 0), (duplicate, 0)]);
}

#[test]
fn every_unacked_request_reinjection_flight_consumes_its_exact_target_reserve() {
    let repair = path(UnderlayProtocol::Udp, 1, 11);
    let replacement = path(UnderlayProtocol::Udp, 1, 12);
    let frame = data_frame(0, 4096);
    let mut ledger = RequestFlightLedger::default();
    ledger.record_reinjection_frame_instance(repair, &frame);

    assert_eq!(ledger.reinjected_data_in_flight_bytes(repair), 4096,);
    assert_eq!(
        ledger.reinjected_data_in_flight_bytes(replacement),
        0,
        "a replacement attachment cannot inherit the old target's reserve debt",
    );

    ledger.age_reinjected_flights_for_test(Duration::from_secs(2));
    assert_eq!(
        ledger.reinjected_data_in_flight_bytes(repair),
        4096,
        "expiry may open another target but cannot renew this reliable incarnation",
    );
}

#[test]
fn frame_history_and_original_ownership_are_exact_attachment_instances() {
    let original = path(UnderlayProtocol::Tcp, 0, 3);
    let replacement = path(UnderlayProtocol::Tcp, 0, 4);
    let repair = path(UnderlayProtocol::Udp, 1, 5);
    let frame = data_frame(0, 4096);
    let mut ledger = RequestFlightLedger::default();
    ledger.record_original_frame_instance(original, &frame);
    ledger.record_reinjection_frame_instance(repair, &frame);

    assert_eq!(
        ledger.sent_instances_for_frame(&frame),
        vec![original, repair],
        "history preserves the exact attachments that carried the frame",
    );
    assert_eq!(
        ledger.original_transmission_instances_for_frame(&frame, &[original, replacement, repair],),
        vec![original],
    );
    assert!(
        ledger
            .original_transmission_instances_for_frame(&frame, &[replacement, repair])
            .is_empty(),
        "a reconnect with the same logical key cannot inherit original-flight ownership",
    );
    ledger.age_reinjected_flights_for_test(std::time::Duration::from_secs(2));
    assert_eq!(
        ledger.sent_instances_for_frame(&frame),
        vec![original, repair],
        "un-DataACKed ReinjectedData continues avoiding the same exact reliable incarnation",
    );
}

#[test]
fn an_unambiguous_partial_owner_ack_retains_and_proves_the_suffix() {
    let owner = path(UnderlayProtocol::Tcp, 0, 3);
    let mut ledger = RequestFlightLedger::default();
    ledger.record_original_frame_instance(owner, &data_frame(0, 4096));

    let prefix = ledger.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: 1024,
    }]);
    assert_eq!(prefix.len(), 1);
    assert_eq!(prefix[0].instance, owner);
    assert_eq!(prefix[0].bytes, 1024);
    assert!(prefix[0].path_proving);
    assert_eq!(
        ledger.latest_unacked_ranges_for_path_instance(owner),
        vec![OffsetRange {
            start: 1024,
            end: 4096,
        }]
    );

    let suffix = ledger.release_normalized_acked_ranges(&[OffsetRange {
        start: 1024,
        end: 4096,
    }]);
    assert_eq!(suffix.len(), 1);
    assert_eq!(suffix[0].bytes, 3072);
    assert!(suffix[0].path_proving);
}

#[test]
fn reinjection_ranges_exclude_live_alternate_flights_only() {
    let original = path(UnderlayProtocol::Tcp, 0, 3);
    let alternate = path(UnderlayProtocol::Udp, 1, 5);
    let unavailable = path(UnderlayProtocol::Tcp, 2, 7);
    let mut ledger = RequestFlightLedger::default();
    for offset in [0, 4096, 8192] {
        ledger.record_original_frame_instance(original, &data_frame(offset, 4096));
    }
    ledger.record_reinjection_frame_instance(original, &data_frame(0, 1024));
    ledger.record_reinjection_frame_instance(alternate, &data_frame(1024, 2048));
    ledger.record_reinjection_frame_instance(alternate, &data_frame(5120, 2048));
    ledger.record_reinjection_frame_instance(unavailable, &data_frame(8192, 1024));

    assert_eq!(
        ledger
            .range_recovery_state(original, &[alternate])
            .uncovered_ranges,
        vec![
            OffsetRange {
                start: 0,
                end: 1024,
            },
            OffsetRange {
                start: 3072,
                end: 5120,
            },
            OffsetRange {
                start: 7168,
                end: 12288,
            },
        ],
        "copies on the original or an unavailable path do not suppress reinjection"
    );

    ledger.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: 4096,
    }]);
    assert_eq!(
        ledger
            .range_recovery_state(original, &[alternate])
            .uncovered_ranges,
        vec![
            OffsetRange {
                start: 4096,
                end: 5120,
            },
            OffsetRange {
                start: 7168,
                end: 12288,
            },
        ],
        "Data ACK release advances reinjection beyond live alternate coverage"
    );
}

#[test]
fn ambiguous_prefix_ack_cannot_make_a_fresh_tail_a_staleness_candidate() {
    let owner = path(UnderlayProtocol::Tcp, 0, 3);
    let duplicate = path(UnderlayProtocol::Udp, 1, 5);
    let mut ledger = RequestFlightLedger::default();
    ledger.record_original_frame_instance(owner, &data_frame(0, 4096));
    ledger.record_reinjection_frame_instance(duplicate, &data_frame(0, 4096));
    ledger.record_original_frame_instance(owner, &data_frame(4096, 4096));

    assert!(
        ledger.unacked_original_paths_before(0).is_empty(),
        "retained work without a complete ACK horizon cannot arm withdrawal",
    );

    let released = ledger.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: 4096,
    }]);
    assert!(
        released.iter().all(|release| !release.path_proving),
        "delivery of an overlapping original and reinjection has no exact owner attribution",
    );
    assert!(
        ledger.unacked_original_paths_before(4096).is_empty(),
        "a fresh tail beginning at the complete ACK horizon is not an authoritative omission",
    );
    assert_eq!(
        ledger.unacked_original_paths_before(8192).as_slice(),
        &[owner],
        "the same retained tail becomes eligible only when a later complete horizon covers it",
    );
}

#[test]
fn lower_flight_owner_lookup_tracks_the_oldest_unacked_owner() {
    let first = path(UnderlayProtocol::Udp, 0, 1);
    let second = path(UnderlayProtocol::Tcp, 1, 2);
    let duplicate = path(UnderlayProtocol::Udp, 2, 3);
    let mut ledger = RequestFlightLedger::default();
    ledger.record_original_frame_instance(first, &data_frame(0, 4096));
    ledger.record_reinjection_frame_instance(duplicate, &data_frame(0, 4096));
    ledger.record_original_frame_instance(second, &data_frame(4096, 4096));

    assert_eq!(ledger.oldest_lower_flight_owner_before_offset(0), None);
    assert_eq!(
        ledger.oldest_lower_flight_owner_instance_before_offset(8192),
        Some(first),
        "the apply transaction retains exact attachment identity",
    );
    assert_eq!(
        ledger.oldest_lower_flight_owner_before_offset(8192),
        Some(first.key)
    );
    assert_eq!(
        ledger.ordering_debt_bytes_before_offset(first.key, 8192),
        4096
    );
    assert_eq!(
        ledger.ordering_debt_bytes_before_offset(second.key, 8192),
        4096
    );

    ledger.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: 4096,
    }]);
    assert_eq!(
        ledger.oldest_lower_flight_owner_before_offset(8192),
        Some(second.key)
    );
}

#[test]
fn missing_owner_detection_is_fenced_by_exact_attachment_instance() {
    let live = path(UnderlayProtocol::Tcp, 0, 1);
    let stale = path(UnderlayProtocol::Udp, 0, 7);
    let replacement = path(UnderlayProtocol::Udp, 0, 8);
    let stale_frame = data_frame(4096, 4096);
    let mut ledger = RequestFlightLedger::default();
    ledger.record_original_frame_instance(live, &data_frame(0, 4096));
    ledger.record_original_frame_instance(stale, &stale_frame);

    assert!(ledger.has_missing_original_transmission_before_offset(8192, &[live, replacement]));
    assert!(
        ledger
            .original_transmission_keys_for_frame(&stale_frame, &[replacement])
            .is_empty(),
        "a reconnect using the same configured key cannot inherit old flights"
    );
    assert!(
        !ledger.has_missing_original_transmission_before_offset(8192, &[live, stale, replacement])
    );
}

#[test]
fn original_path_requires_one_instance_to_cover_the_complete_range() {
    let owner = path(UnderlayProtocol::Tcp, 0, 3);
    let replacement = path(UnderlayProtocol::Tcp, 0, 4);
    let mut ledger = RequestFlightLedger::default();
    ledger.record_original_frame_instance(owner, &data_frame(0, 2048));
    ledger.record_original_frame_instance(owner, &data_frame(2048, 2048));

    assert_eq!(
        ledger.unique_original_path_for_range(OffsetRange {
            start: 512,
            end: 3584,
        }),
        Some(owner),
        "adjacent flights from the same attachment cover one Data ACK gap"
    );
    assert_eq!(
        ledger.unique_original_path_for_range(OffsetRange {
            start: 0,
            end: 4097,
        }),
        None,
        "an incompletely covered range has no identifiable original path"
    );

    let replacement_overlap = data_frame(1024, 1024);
    ledger.record_original_frame_instance(replacement, &replacement_overlap);
    assert_eq!(
        ledger.unique_original_path_for_frame(&replacement_overlap),
        None,
        "overlapping ownership from another attachment is ambiguous"
    );
}

#[test]
fn original_send_epoch_is_latest_covering_flight_and_requires_unique_ownership() {
    let owner = path(UnderlayProtocol::Tcp, 0, 3);
    let replacement = path(UnderlayProtocol::Tcp, 0, 4);
    let mut ledger = RequestFlightLedger::default();
    ledger.record_original_frame_instance(owner, &data_frame(0, 2048));
    let second_record_started = Instant::now();
    ledger.record_original_frame_instance(owner, &data_frame(2048, 2048));
    let second_record_finished = Instant::now();

    let sent_at = ledger
        .unique_original_sent_at_for_frame(&data_frame(0, 4096))
        .expect("one attachment owns the complete frame");
    assert!(sent_at >= second_record_started);
    assert!(sent_at <= second_record_finished);

    ledger.record_original_frame_instance(replacement, &data_frame(1024, 1024));
    assert_eq!(
        ledger.unique_original_sent_at_for_frame(&data_frame(0, 4096)),
        None,
        "ambiguous original ownership has no recovery epoch",
    );
}

#[test]
fn partial_ack_splits_and_preserves_only_exact_qualification_receipts() {
    let owner = path(UnderlayProtocol::Tcp, 0, 31);
    let whole = OffsetRange { start: 0, end: 8 };
    let mut states = RequestPathStates::default();
    let receipt = states
        .tag_admitted_original(owner, 8, 8, whole)
        .expect("valid qualification admission")
        .expect("full receipt");
    let mut ledger = RequestFlightLedger::default();
    assert_eq!(
        ledger.record_original_frame_instance_with_evidence(
            owner,
            &data_frame(0, 8),
            true,
            Some(receipt),
        ),
        8,
    );

    let middle = OffsetRange { start: 2, end: 6 };
    let middle_release = ledger.release_normalized_acked_ranges(&[middle]);
    assert_eq!(middle_release.len(), 1);
    let authority = middle_release[0]
        .qualification
        .expect("released tagged middle carries exact authority");
    assert_eq!(
        states.release_exact_product_qualification(authority, middle),
        4,
    );
    let invariant = states
        .get(owner)
        .expect("owner state")
        .product_qualification_invariant();
    assert_eq!(invariant.verified_bytes, 4);
    assert_eq!(invariant.outstanding_tag_bytes, 4);

    let tails = ledger.release_normalized_acked_ranges(&[
        OffsetRange { start: 0, end: 2 },
        OffsetRange { start: 6, end: 8 },
    ]);
    assert_eq!(tails.len(), 2);
    for release in tails {
        let authority = release
            .qualification
            .expect("each retained tagged tail carries clipped authority");
        assert_eq!(
            states.release_exact_product_qualification(authority, release.range),
            release.bytes as u64,
        );
    }
    assert!(
        states
            .get(owner)
            .expect("qualified owner")
            .product_assignment_qualified()
    );
}

#[test]
fn reinjection_scrubs_the_flight_owner_not_a_same_range_replacement() {
    let predecessor = path(UnderlayProtocol::Tcp, 0, 41);
    let replacement = path(UnderlayProtocol::Tcp, 0, 42);
    let range = OffsetRange { start: 0, end: 8 };
    let mut states = RequestPathStates::default();
    let predecessor_receipt = states
        .tag_admitted_original(predecessor, 8, 8, range)
        .expect("predecessor admission")
        .expect("predecessor receipt");
    let _replacement_receipt = states
        .tag_admitted_original(replacement, 8, 8, range)
        .expect("replacement admission")
        .expect("replacement receipt");
    let mut ledger = RequestFlightLedger::default();
    ledger.record_original_frame_instance_with_evidence(
        predecessor,
        &data_frame(0, 8),
        true,
        Some(predecessor_receipt),
    );

    let authorities = ledger.overlapping_original_qualification_receipts(range);
    assert_eq!(authorities.len(), 1);
    assert_eq!(
        states.release_ambiguous_product_qualification(authorities[0], range),
        8,
    );
    assert_eq!(
        states
            .get(predecessor)
            .expect("predecessor state")
            .product_qualification_invariant()
            .outstanding_tag_bytes,
        0,
    );
    assert_eq!(
        states
            .get(replacement)
            .expect("replacement state")
            .product_qualification_invariant()
            .outstanding_tag_bytes,
        8,
        "a raw range broadcast would have incorrectly scrubbed this exact replacement",
    );
}
