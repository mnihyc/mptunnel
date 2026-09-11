use super::{RequestFlightLedger, RequestRecoveryOwnershipView};
use crate::model::path::{CarrierPathInstanceId, RelayPathInstance, RelayPathKey};
use crate::model::work::CarrierWorkKind;
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

fn assert_recovery_ownership_view_matches_oracle(
    ledger: &RequestFlightLedger,
    view: &RequestRecoveryOwnershipView,
    range: OffsetRange,
    instances: &[RelayPathInstance],
) {
    let actual = view.uniform_frontier(range, instances);
    let expected = ledger.live_owner_uniform_frontier(range, instances);
    match (actual, expected) {
        (None, None) => {}
        (Some(actual), Some(expected)) => {
            assert_eq!(
                actual.range, expected.range,
                "query {range:?}, {instances:?}"
            );
            assert_eq!(actual.owners.len(), expected.owners.len());
            assert!(
                actual
                    .owners
                    .iter()
                    .all(|owner| expected.owners.contains(owner))
            );
            assert_eq!(actual.avoid.len(), expected.avoid.len());
            assert!(
                actual
                    .avoid
                    .iter()
                    .all(|owner| expected.avoid.contains(owner))
            );
        }
        (actual, expected) => {
            panic!("query {range:?}, {instances:?}: actual {actual:?}, expected {expected:?}")
        }
    }
}

#[test]
fn recovery_ownership_view_matches_crossing_spans_and_current_masks() {
    let a = path(UnderlayProtocol::Tcp, 0, 301);
    let b = path(UnderlayProtocol::Udp, 1, 302);
    let c = path(UnderlayProtocol::Tcp, 2, 303);
    let replacement = path(UnderlayProtocol::Tcp, 0, 304);
    let instances = [a, b, c, replacement];
    let mut ledger = RequestFlightLedger::default();
    ledger.record_original_frame_instance(a, &data_frame(0, 8));
    ledger.record_original_frame_instance(a, &data_frame(8, 8));
    ledger.record_original_frame_instance(b, &data_frame(16, 8));
    ledger.record_original_frame_instance(replacement, &data_frame(26, 6));
    ledger.record_reinjection_frame_instance(c, &data_frame(2, 10));
    ledger.record_reinjection_frame_instance(c, &data_frame(10, 10));
    ledger.record_reinjection_frame_instance(b, &data_frame(6, 4));
    ledger.record_reinjection_frame_instance(replacement, &data_frame(12, 6));
    let view = ledger.recovery_ownership_view(32);

    // The same immutable view accepts a different exact eligibility mask on
    // every query. Starts inside older copy spans must retain their avoidance.
    for mask in 0..(1usize << instances.len()) {
        let live = instances
            .iter()
            .enumerate()
            .filter_map(|(index, instance)| (mask & (1 << index) != 0).then_some(*instance))
            .collect::<Vec<_>>();
        for start in 0..=32 {
            for end in start..=32 {
                assert_recovery_ownership_view_matches_oracle(
                    &ledger,
                    &view,
                    OffsetRange { start, end },
                    &live,
                );
            }
        }
    }
    assert!(
        ledger
            .flights
            .values()
            .flatten()
            .all(|flight| flight.original_recovery_timing.is_none()),
        "view construction and queries cannot initialize assignment clocks"
    );
}

#[test]
fn recovery_ownership_view_merges_storage_boundaries_but_keeps_membership_changes() {
    let owner = path(UnderlayProtocol::Udp, 0, 305);
    let copy = path(UnderlayProtocol::Tcp, 1, 306);
    let mut ledger = RequestFlightLedger::default();
    for start in (0..24).step_by(4) {
        ledger.record_original_frame_instance(owner, &data_frame(start, 4));
    }
    ledger.record_reinjection_frame_instance(copy, &data_frame(2, 6));
    ledger.record_reinjection_frame_instance(copy, &data_frame(6, 6));
    ledger.record_reinjection_frame_instance(copy, &data_frame(12, 4));
    let view = ledger.recovery_ownership_view(24);
    let frontier = view
        .uniform_frontier(OffsetRange { start: 5, end: 24 }, &[owner, copy])
        .expect("crossing copies cover this Original prefix");
    assert_eq!(frontier.range, OffsetRange { start: 5, end: 16 });
    assert_eq!(frontier.owners, vec![owner]);
    assert_eq!(frontier.avoid.len(), 2);
    assert_recovery_ownership_view_matches_oracle(
        &ledger,
        &view,
        OffsetRange { start: 5, end: 24 },
        &[owner, copy],
    );
    assert_eq!(
        view.uniform_frontier(OffsetRange { start: 5, end: 24 }, &[owner])
            .expect("current mask excludes the copy")
            .range,
        OffsetRange { start: 5, end: 24 },
    );
    assert_eq!(
        view.uniform_frontier(OffsetRange { start: 5, end: 24 }, &[copy]),
        None,
        "accepted copies cannot manufacture an Original owner"
    );
}

#[test]
fn recovery_ownership_view_rebuilds_after_ack_and_keeps_expired_copy_ownership() {
    let owner = path(UnderlayProtocol::Udp, 0, 307);
    let copy = path(UnderlayProtocol::Tcp, 1, 308);
    let mut ledger = RequestFlightLedger::default();
    ledger.record_original_frame_instance(owner, &data_frame(0, 20));
    let (_, deadline) = ledger.record_reinjection_frame_instance_with_suppression_interval(
        copy,
        &data_frame(2, 14),
        Duration::ZERO,
    );
    let deadline = deadline.expect("accepted copy has its immutable deadline");
    assert_eq!(
        ledger.live_copy_coverage(&[owner, copy], deadline),
        (vec![], None)
    );
    let before = ledger.recovery_ownership_view(20);
    assert!(
        before
            .uniform_frontier(OffsetRange { start: 3, end: 20 }, &[owner, copy])
            .expect("expired copy is still retained")
            .avoid
            .contains(&copy)
    );
    let timing = ledger
        .observe_original_recovery_timing_for_range(OffsetRange { start: 0, end: 2 }, |_| {
            Some(recovery_timing_snapshot(owner, 100.0))
        })
        .expect("actual Original timing");
    ledger.release_normalized_acked_ranges(&[OffsetRange { start: 6, end: 10 }]);
    // A view has only this transaction's lifetime; ACK mutation requires a new
    // view, rather than treating an old snapshot as current receipt authority.
    let after = ledger.recovery_ownership_view(20);
    for start in 0..20 {
        assert_recovery_ownership_view_matches_oracle(
            &ledger,
            &after,
            OffsetRange { start, end: 20 },
            &[owner, copy],
        );
    }
    assert_eq!(
        after.uniform_frontier(OffsetRange { start: 6, end: 20 }, &[owner, copy]),
        None,
        "positive ACK holes cannot remain as ownership coverage"
    );
    for flight in ledger.flights.values().flatten() {
        if flight.kind.is_original_transmission() {
            assert_eq!(flight.original_recovery_timing, Some(timing));
        } else {
            assert_eq!(flight.reinjection_suppression_deadline, Some(deadline));
            assert_eq!(flight.original_recovery_timing, None);
        }
    }
}

#[test]
fn recovery_ownership_view_avoidance_order_is_not_ranking_order() {
    let copy = path(UnderlayProtocol::Tcp, 0, 309);
    let owner = path(UnderlayProtocol::Udp, 1, 310);
    let mut ledger = RequestFlightLedger::default();
    ledger.record_reinjection_frame_instance(copy, &data_frame(0, 2));
    ledger.record_original_frame_instance(owner, &data_frame(2, 8));
    ledger.record_reinjection_frame_instance(copy, &data_frame(4, 6));
    let range = OffsetRange { start: 5, end: 10 };
    let view = ledger.recovery_ownership_view(10);
    let actual = view.uniform_frontier(range, &[owner, copy]).unwrap();
    let oracle = ledger
        .live_owner_uniform_frontier(range, &[owner, copy])
        .unwrap();
    assert_eq!(actual.owners, oracle.owners);
    assert_eq!(actual.avoid, vec![copy, owner]);
    assert_eq!(oracle.avoid, vec![owner, copy]);
    assert_recovery_ownership_view_matches_oracle(&ledger, &view, range, &[owner, copy]);
}

#[test]
fn recovery_ownership_view_is_bounded_by_its_captured_horizon() {
    let owner = path(UnderlayProtocol::Tcp, 0, 311);
    let replacement = path(UnderlayProtocol::Tcp, 0, 312);
    let mut ledger = RequestFlightLedger::default();
    ledger.record_original_frame_instance(owner, &data_frame(0, 20));
    let view = ledger.recovery_ownership_view(12);
    assert_recovery_ownership_view_matches_oracle(
        &ledger,
        &view,
        OffsetRange { start: 8, end: 12 },
        &[owner],
    );
    assert_eq!(
        view.uniform_frontier(OffsetRange { start: 8, end: 13 }, &[owner]),
        None
    );
    assert_eq!(
        view.uniform_frontier(OffsetRange { start: 12, end: 12 }, &[owner]),
        None
    );
    assert_eq!(
        view.uniform_frontier(OffsetRange { start: 8, end: 12 }, &[replacement]),
        None
    );
    assert_eq!(
        ledger
            .recovery_ownership_view(0)
            .uniform_frontier(OffsetRange { start: 0, end: 1 }, &[owner]),
        None,
    );
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

fn assert_reinjected_data_totals(
    ledger: &RequestFlightLedger,
    expected_instances: &[(RelayPathInstance, usize)],
) {
    assert_eq!(
        ledger.reinjected_data_in_flight_bytes_by_instance,
        expected_instances
            .iter()
            .filter(|(_, bytes)| *bytes != 0)
            .map(|(instance, bytes)| (*instance, *bytes as u64))
            .collect(),
        "only exact instances with retained copies own aggregate entries"
    );
    let scanned_total = ledger
        .flights
        .values()
        .flat_map(|flights| flights.iter())
        .filter(|flight| flight.kind == CarrierWorkKind::ReinjectedData)
        .map(|flight| flight.bytes)
        .sum::<usize>();
    assert_eq!(
        scanned_total,
        expected_instances
            .iter()
            .map(|(_, bytes)| bytes)
            .sum::<usize>(),
    );
    for (instance, expected_bytes) in expected_instances {
        let scanned_instance = ledger
            .flights
            .values()
            .flat_map(|flights| flights.iter())
            .filter(|flight| {
                flight.instance == *instance && flight.kind == CarrierWorkKind::ReinjectedData
            })
            .map(|flight| flight.bytes)
            .sum::<usize>();
        assert_eq!(
            ledger.reinjected_data_in_flight_bytes(*instance),
            scanned_instance
        );
        assert_eq!(scanned_instance, *expected_bytes);
    }
}

fn ack_support_prefix_with_fragmented_suffix(suffix_records: usize) -> usize {
    const PREFIX: usize = 65_536;
    const COPIED: usize = 14_600;
    let owner = path(UnderlayProtocol::Udp, 0, 71);
    let successor = path(UnderlayProtocol::Udp, 0, 72);
    let copy = path(UnderlayProtocol::Tcp, 1, 73);
    let mut ledger = RequestFlightLedger::default();
    assert_eq!(
        ledger.record_original_frame_instance(owner, &data_frame(0, PREFIX)),
        PREFIX,
    );
    assert_eq!(
        ledger.record_reinjection_frame_instance(copy, &data_frame(0, COPIED)),
        COPIED,
    );
    assert_eq!(PREFIX % suffix_records, 0);
    let fragment_bytes = PREFIX / suffix_records;
    for fragment in 0..suffix_records {
        assert_eq!(
            ledger.record_original_frame_instance(
                successor,
                &data_frame((PREFIX + fragment * fragment_bytes) as u64, fragment_bytes),
            ),
            fragment_bytes,
        );
    }
    let suffix_snapshot = |ledger: &RequestFlightLedger| {
        ledger
            .flights
            .range(PREFIX as u64..)
            .flat_map(|(start, flights)| {
                flights.iter().map(move |flight| {
                    (
                        *start,
                        flight.instance,
                        flight.end,
                        flight.bytes,
                        flight.sent_at,
                        flight.kind,
                        flight.evidence_eligible,
                        flight.qualification,
                        flight.reinjection_suppression_deadline,
                    )
                })
            })
            .collect::<Vec<_>>()
    };
    let suffix_before = suffix_snapshot(&ledger);
    let original_sent_at = ledger.flights[&0][0].sent_at;
    let copy_sent_at = ledger.flights[&0][1].sent_at;
    assert_original_data_cache(
        &ledger,
        (2 * PREFIX) as u64,
        &[(owner, PREFIX as u64), (successor, PREFIX as u64)],
    );
    assert_reinjected_data_totals(&ledger, &[(copy, COPIED)]);
    let ack = crate::protocol::frame::normalize_offset_ranges(vec![
        OffsetRange {
            start: COPIED as u64,
            end: PREFIX as u64,
        },
        OffsetRange {
            start: 0,
            end: COPIED as u64,
        },
        OffsetRange {
            start: 0,
            end: 4096,
        },
    ]);
    assert_eq!(
        ack,
        vec![OffsetRange {
            start: 0,
            end: PREFIX as u64
        }]
    );
    RequestFlightLedger::take_ack_release_flight_visits_for_test();
    let released = ledger.release_normalized_acked_ranges(&ack);
    let visits = RequestFlightLedger::take_ack_release_flight_visits_for_test();

    // These exact release/proof/debt controls precede the work assertion. The
    // same byte suffix is produced as one or many legal Original records; no
    // clock, credit, ownership or evidence flag is synthesized for admission.
    assert_eq!(
        released
            .iter()
            .map(|release| (
                release.instance,
                release.range,
                release.bytes,
                release.kind,
                release.path_proving,
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                owner,
                OffsetRange {
                    start: 0,
                    end: COPIED as u64
                },
                COPIED,
                CarrierWorkKind::OriginalData,
                false
            ),
            (
                owner,
                OffsetRange {
                    start: COPIED as u64,
                    end: PREFIX as u64
                },
                PREFIX - COPIED,
                CarrierWorkKind::OriginalData,
                true
            ),
            (
                copy,
                OffsetRange {
                    start: 0,
                    end: COPIED as u64
                },
                COPIED,
                CarrierWorkKind::ReinjectedData,
                false
            ),
        ],
    );
    for release in &released {
        assert_eq!(release.qualification, None);
        assert_eq!(
            release.sent_at,
            if release.instance == owner {
                original_sent_at
            } else {
                copy_sent_at
            },
        );
    }
    assert_eq!(suffix_snapshot(&ledger), suffix_before);
    assert_original_data_cache(
        &ledger,
        PREFIX as u64,
        &[(owner, 0), (successor, PREFIX as u64)],
    );
    assert_reinjected_data_totals(&ledger, &[(copy, 0)]);
    assert_eq!(
        ledger.latest_unacked_ranges_for_path_instance(successor),
        vec![OffsetRange {
            start: PREFIX as u64,
            end: (2 * PREFIX) as u64
        }],
    );
    visits
}

#[test]
fn ack_release_work_excludes_unacknowledged_suffix_fragments() {
    let coarse = ack_support_prefix_with_fragmented_suffix(1);
    assert!(
        coarse >= 2,
        "the control processes both ACK-support records"
    );
    let fragmented = ack_support_prefix_with_fragmented_suffix(64);
    assert_eq!(
        fragmented, coarse,
        "identical ACK support must not process additional unrelated suffix records; counter excludes map comparisons and boundary-bucket merging",
    );
}

#[test]
fn ack_release_support_boundary_precedes_existing_bucket_and_preserves_metadata() {
    let owner = path(UnderlayProtocol::Udp, 0, 81);
    let crossing_copy = path(UnderlayProtocol::Tcp, 1, 83);
    let existing_copy = path(UnderlayProtocol::Tcp, 1, 84);
    let other_copy = path(UnderlayProtocol::Udp, 0, 82);
    let mut states = RequestPathStates::default();
    let receipt = states
        .tag_admitted_original(owner, 128, 128, OffsetRange { start: 0, end: 128 })
        .expect("valid actual qualification admission")
        .expect("full Original receipt");
    let mut ledger = RequestFlightLedger::default();
    assert_eq!(
        ledger.record_original_frame_instance_with_evidence(
            owner,
            &data_frame(0, 128),
            true,
            Some(receipt),
        ),
        128,
    );
    for (instance, start, bytes) in [
        (crossing_copy, 32, 64),
        (existing_copy, 64, 32),
        (other_copy, 64, 64),
    ] {
        assert_eq!(
            ledger.record_reinjection_frame_instance(instance, &data_frame(start, bytes)),
            bytes,
        );
    }
    let original = ledger.flights[&0][0];
    let crossing = ledger.flights[&32][0];
    let existing = ledger.flights[&64].clone();
    let ack = crate::protocol::frame::normalize_offset_ranges(vec![
        OffsetRange { start: 48, end: 64 },
        OffsetRange { start: 32, end: 48 },
    ]);
    assert_eq!(ack, vec![OffsetRange { start: 32, end: 64 }]);
    let released = ledger.release_normalized_acked_ranges(&ack);
    assert_eq!(released.len(), 2);
    assert_eq!(released[0].instance, owner);
    assert_eq!(released[1].instance, crossing_copy);
    assert!(
        released
            .iter()
            .all(|release| release.range == ack[0] && release.bytes == 32 && !release.path_proving)
    );
    assert_eq!(released[0].qualification, receipt.intersect(ack[0]));
    assert_eq!(released[1].qualification, None);

    // Old key/vector order is semantic: crossing pieces rekeyed at H precede
    // already-retained copies beginning exactly at H. No live record is edited
    // by the fixture; expected fragments retain the actual producer metadata.
    let boundary = &ledger.flights[&64];
    assert_eq!(
        boundary
            .iter()
            .map(|flight| flight.instance)
            .collect::<Vec<_>>(),
        vec![owner, crossing_copy, existing_copy, other_copy],
    );
    let assert_fragment =
        |before: &super::RequestFlight, after: &super::RequestFlight, range: OffsetRange| {
            assert_eq!(after.instance, before.instance);
            assert_eq!(after.end, range.end);
            assert_eq!(after.bytes as u64, range.end - range.start);
            assert_eq!(after.sent_at, before.sent_at);
            assert_eq!(after.kind, before.kind);
            assert_eq!(after.evidence_eligible, before.evidence_eligible);
            assert_eq!(
                after.qualification,
                before.qualification.and_then(|q| q.intersect(range))
            );
            assert_eq!(
                after.reinjection_suppression_deadline,
                before.reinjection_suppression_deadline
            );
        };
    assert_fragment(
        &original,
        &ledger.flights[&0][0],
        OffsetRange { start: 0, end: 32 },
    );
    assert_fragment(
        &original,
        &boundary[0],
        OffsetRange {
            start: 64,
            end: 128,
        },
    );
    assert_fragment(&crossing, &boundary[1], OffsetRange { start: 64, end: 96 });
    for (before, after) in existing.iter().zip(&boundary[2..]) {
        assert_fragment(
            before,
            after,
            OffsetRange {
                start: 64,
                end: before.end,
            },
        );
    }
    assert_original_data_cache(&ledger, 96, &[(owner, 96), (other_copy, 0)]);
    assert_reinjected_data_totals(
        &ledger,
        &[(crossing_copy, 32), (existing_copy, 32), (other_copy, 64)],
    );
    assert!(ledger.release_normalized_acked_ranges(&ack).is_empty());
    assert_eq!(
        ledger.flights[&64]
            .iter()
            .map(|flight| flight.instance)
            .collect::<Vec<_>>(),
        vec![owner, crossing_copy, existing_copy, other_copy],
    );

    // The opposite boundary still processes and settles every retained record
    // when the normalized ACK actually covers the full flight horizon.
    let full_ack =
        crate::protocol::frame::normalize_offset_ranges(vec![OffsetRange { start: 0, end: 128 }]);
    let tail = ledger.release_normalized_acked_ranges(&full_ack);
    assert_eq!(tail.iter().map(|release| release.bytes).sum::<usize>(), 224);
    assert_eq!(
        tail.iter()
            .filter(|release| release.path_proving)
            .map(|release| release.bytes)
            .sum::<usize>(),
        32
    );
    assert!(ledger.flights.is_empty());
    assert_original_data_cache(&ledger, 0, &[(owner, 0), (other_copy, 0)]);
    assert_reinjected_data_totals(
        &ledger,
        &[(crossing_copy, 0), (existing_copy, 0), (other_copy, 0)],
    );
    assert!(ledger.release_normalized_acked_ranges(&full_ack).is_empty());
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

fn partial_copy_ack_proving_bytes(split_ack: bool, invalidate_epoch: bool) -> usize {
    let owner = path(UnderlayProtocol::Udp, 0, 71);
    let copy = path(UnderlayProtocol::Tcp, 1, 73);
    let original = OffsetRange {
        start: 0,
        end: 65_536,
    };
    let retained = OffsetRange {
        start: 65_536,
        end: 131_072,
    };
    let mut ledger = RequestFlightLedger::default();
    for range in [original, retained] {
        assert_eq!(
            ledger.record_original_frame_instance_with_evidence(
                owner,
                &data_frame(range.start, 65_536),
                true,
                None,
            ),
            65_536,
        );
    }
    assert_eq!(
        ledger
            .record_reinjection_frame_instance_with_suppression_interval(
                copy,
                &data_frame(0, 14_600),
                Duration::from_secs(1),
            )
            .0,
        14_600,
    );
    if invalidate_epoch {
        ledger.invalidate_original_evidence(owner);
    }

    let released = if split_ack {
        let mut prefix = ledger.release_normalized_acked_ranges(&[OffsetRange {
            start: 0,
            end: 14_600,
        }]);
        prefix.extend(ledger.release_normalized_acked_ranges(&[OffsetRange {
            start: 14_600,
            end: original.end,
        }]));
        prefix
    } else {
        ledger.release_normalized_acked_ranges(&[original])
    };

    for (instance, expected_bytes) in [(owner, 65_536), (copy, 14_600)] {
        assert_eq!(
            released
                .iter()
                .filter(|release| release.instance == instance)
                .map(|release| release.bytes)
                .sum::<usize>(),
            expected_bytes,
            "ACK settlement releases all original and copy debts, independently of evidence",
        );
    }
    assert_original_data_cache(&ledger, 65_536, &[(owner, 65_536), (copy, 0)]);
    assert_eq!(ledger.reinjected_data_in_flight_bytes(copy), 0);
    assert_eq!(
        ledger.latest_unacked_ranges_for_path_instance(owner),
        vec![retained],
    );
    let expected_candidates = if invalidate_epoch {
        vec![]
    } else {
        vec![owner]
    };
    assert_eq!(
        ledger
            .unacked_original_paths_for_gaps(&[retained])
            .as_slice(),
        expected_candidates.as_slice(),
        "the omitted owner remains a stale-clock candidate only in its eligible epoch",
    );
    assert!(
        ledger
            .release_normalized_acked_ranges(&[original])
            .is_empty()
    );
    let proving = released.iter().filter(|release| release.path_proving);
    proving
        .map(|release| {
            assert_eq!(release.instance, owner);
            assert!(release.kind.is_original_transmission());
            assert!(release.range.start >= 14_600 && release.range.end <= original.end);
            release.bytes
        })
        .sum()
}

#[test]
fn combined_ack_preserves_unique_original_evidence_outside_partial_copy() {
    assert_eq!(
        partial_copy_ack_proving_bytes(false, false),
        50_936,
        "a copied prefix cannot erase adjacent unique progress from the same ACK",
    );
}

#[test]
fn split_ack_preserves_unique_original_evidence_and_epoch_fence() {
    assert_eq!(partial_copy_ack_proving_bytes(true, false), 50_936);
    for split_ack in [false, true] {
        assert_eq!(
            partial_copy_ack_proving_bytes(split_ack, true),
            0,
            "neither ACK partition may revive evidence from a pre-stale epoch",
        );
    }
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
fn accepted_copy_totals_preserve_multiplicity_instance_and_ack_lifecycle() {
    let owner = path(UnderlayProtocol::Tcp, 0, 1);
    let target = path(UnderlayProtocol::Udp, 1, 11);
    let replacement = path(UnderlayProtocol::Udp, 1, 12);
    let other = path(UnderlayProtocol::Tcp, 2, 21);
    let targets = [target, replacement, other];
    let mut ledger = RequestFlightLedger::default();
    assert_eq!(
        ledger.record_original_frame_instance(owner, &data_frame(0, 12_288)),
        12_288
    );

    // These are accepted ledger publications, not a claim that admission must
    // choose them: repeated and overlapping copies each retain their own debt.
    for (instance, offset, bytes, expected_instance_bytes) in [
        (target, 0, 8192, 8192),
        (target, 0, 8192, 16_384),
        (target, 4096, 8192, 24_576),
        (replacement, 2048, 4096, 4096),
        (other, 10_240, 2048, 2048),
    ] {
        let (recorded, deadline) = ledger
            .record_reinjection_frame_instance_with_suppression_interval(
                instance,
                &data_frame(offset, bytes),
                Duration::from_secs(60),
            );
        assert_eq!(recorded, bytes);
        assert!(deadline.is_some());
        assert_eq!(
            ledger.reinjected_data_in_flight_bytes(instance),
            expected_instance_bytes
        );
    }
    assert_reinjected_data_totals(
        &ledger,
        &[
            (owner, 0),
            (target, 24_576),
            (replacement, 4096),
            (other, 2048),
        ],
    );
    assert_original_data_cache(&ledger, 12_288, &[(owner, 12_288), (target, 0)]);

    let middle = [OffsetRange {
        start: 3072,
        end: 5120,
    }];
    assert!(!ledger.release_normalized_acked_ranges(&middle).is_empty());
    assert_reinjected_data_totals(
        &ledger,
        &[
            (owner, 0),
            (target, 19_456),
            (replacement, 2048),
            (other, 2048),
        ],
    );
    assert_original_data_cache(&ledger, 10_240, &[(owner, 10_240)]);

    let disjoint = [
        OffsetRange {
            start: 0,
            end: 1024,
        },
        OffsetRange {
            start: 7168,
            end: 11_264,
        },
    ];
    assert!(!ledger.release_normalized_acked_ranges(&disjoint).is_empty());
    assert_reinjected_data_totals(
        &ledger,
        &[
            (owner, 0),
            (target, 11_264),
            (replacement, 2048),
            (other, 1024),
        ],
    );
    assert_original_data_cache(&ledger, 5120, &[(owner, 5120)]);
    assert!(ledger.release_normalized_acked_ranges(&middle).is_empty());
    assert!(ledger.release_normalized_acked_ranges(&disjoint).is_empty());

    let suppressed = ledger.range_recovery_state(owner, &targets);
    assert!(suppressed.uncovered_ranges.is_empty());
    assert!(suppressed.retry_deadline.is_some());
    ledger.age_reinjected_flights_for_test(Duration::from_secs(120));
    assert_eq!(
        ledger.earliest_reinjection_suppression_deadline(&targets),
        None
    );
    let expired = ledger.range_recovery_state(owner, &targets);
    assert_eq!(expired.retry_deadline, None);
    assert_eq!(
        expired.uncovered_ranges,
        ledger.latest_unacked_ranges_for_path_instance(owner)
    );
    assert_reinjected_data_totals(
        &ledger,
        &[
            (owner, 0),
            (target, 11_264),
            (replacement, 2048),
            (other, 1024),
        ],
    );

    // Settle this exact replacement completely without settling its predecessor
    // or the disjoint target; then reuse the zero-debt instance on a live tail.
    let replacement_remainder = [
        OffsetRange {
            start: 2048,
            end: 3072,
        },
        OffsetRange {
            start: 5120,
            end: 6144,
        },
    ];
    ledger.release_normalized_acked_ranges(&replacement_remainder);
    assert_reinjected_data_totals(
        &ledger,
        &[(owner, 0), (target, 6144), (replacement, 0), (other, 1024)],
    );
    assert_original_data_cache(&ledger, 3072, &[(owner, 3072)]);
    assert!(
        ledger
            .release_normalized_acked_ranges(&replacement_remainder)
            .is_empty()
    );
    assert_eq!(
        ledger.record_reinjection_frame_instance(replacement, &data_frame(11_264, 1024)),
        1024,
    );
    assert_reinjected_data_totals(
        &ledger,
        &[
            (owner, 0),
            (target, 6144),
            (replacement, 1024),
            (other, 1024),
        ],
    );

    let drained = ledger.drain_all();
    for (instance, expected) in [
        (owner, 3072),
        (target, 6144),
        (replacement, 1024),
        (other, 1024),
    ] {
        assert_eq!(
            drained
                .iter()
                .filter(|release| release.instance == instance)
                .map(|release| release.bytes)
                .sum::<usize>(),
            expected,
        );
    }
    assert!(drained.iter().all(|release| !release.path_proving));
    assert_reinjected_data_totals(
        &ledger,
        &[(owner, 0), (target, 0), (replacement, 0), (other, 0)],
    );
    assert_original_data_cache(&ledger, 0, &[(owner, 0)]);
    assert!(ledger.drain_all().is_empty());
    assert!(
        ledger
            .release_normalized_acked_ranges(&[OffsetRange {
                start: 0,
                end: 12_288
            }])
            .is_empty()
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
        ledger.unacked_original_paths_for_gaps(&[]).is_empty(),
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
        ledger
            .unacked_original_paths_for_gaps(&[OffsetRange {
                start: 0,
                end: 4096
            }])
            .is_empty(),
        "a fresh tail beginning at the complete ACK horizon is not an authoritative omission",
    );
    assert_eq!(
        ledger
            .unacked_original_paths_for_gaps(&[OffsetRange {
                start: 4096,
                end: 8192
            }])
            .as_slice(),
        &[owner],
        "the same retained tail becomes eligible only when a later complete horizon covers it",
    );
}

#[test]
fn scoped_request_gap_does_not_withdraw_an_earlier_unknown_owner() {
    let unknown_owner = path(UnderlayProtocol::Tcp, 0, 3);
    let gap_owner = path(UnderlayProtocol::Udp, 1, 5);
    let mut ledger = RequestFlightLedger::default();
    ledger.record_original_frame_instance(unknown_owner, &data_frame(0, 4096));
    ledger.record_original_frame_instance(gap_owner, &data_frame(4096, 4096));
    assert_eq!(
        ledger
            .unacked_original_paths_for_gaps(&[OffsetRange {
                start: 5000,
                end: 6000
            }])
            .as_slice(),
        &[gap_owner],
        "a later scoped omission does not authorize negative evidence for the earlier retained owner",
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

fn recovery_timing_snapshot(
    owner: RelayPathInstance,
    srtt_ms: f64,
) -> crate::scheduler::PathSnapshot {
    crate::scheduler::PathSnapshot::new(
        crate::protocol::PathId(owner.key.index as u16),
        owner.key.underlay,
        srtt_ms,
        1_000_000.0,
    )
}

#[test]
fn recovery_service_copy_coverage_uses_exact_membership_and_immutable_expiry() {
    let owner = path(UnderlayProtocol::Udp, 0, 209);
    let copy = path(UnderlayProtocol::Tcp, 1, 210);
    let predecessor = path(UnderlayProtocol::Tcp, 1, 211);
    let mut ledger = RequestFlightLedger::default();
    ledger.record_original_frame_instance(owner, &data_frame(0, 32));
    let (_, deadline) = ledger.record_reinjection_frame_instance_with_suppression_interval(
        copy,
        &data_frame(0, 12),
        Duration::from_secs(1),
    );
    ledger.record_reinjection_frame_instance(predecessor, &data_frame(20, 4));
    let deadline = deadline.expect("actual accepted copy publishes D");
    let before = deadline.checked_sub(Duration::from_nanos(1)).unwrap();
    assert_eq!(
        ledger.live_copy_coverage(&[copy], before),
        (vec![OffsetRange { start: 0, end: 12 }], Some(deadline)),
        "same numeric path key does not include another attachment's copy"
    );
    assert_eq!(ledger.live_copy_coverage(&[owner], before), (vec![], None));
    assert_eq!(ledger.live_copy_coverage(&[copy], deadline), (vec![], None));
    assert_eq!(
        ledger.reinjected_data_in_flight_bytes(copy),
        12,
        "expiry does not release accepted target ownership"
    );

    ledger.release_normalized_acked_ranges(&[OffsetRange { start: 4, end: 8 }]);
    assert_eq!(
        ledger.live_copy_coverage(&[copy], before),
        (
            vec![
                OffsetRange { start: 0, end: 4 },
                OffsetRange { start: 8, end: 12 }
            ],
            Some(deadline),
        ),
        "coverage follows retained ACK fragments without renewing D"
    );
    assert!(
        ledger
            .flights
            .values()
            .flatten()
            .all(|flight| flight.original_recovery_timing.is_none())
    );
}

#[test]
fn recovery_service_boundaries_keep_assignment_and_retained_copy_scope() {
    let owner = path(UnderlayProtocol::Udp, 0, 212);
    let copy = path(UnderlayProtocol::Tcp, 1, 213);
    let mut ledger = RequestFlightLedger::default();
    ledger.record_original_frame_instance(owner, &data_frame(0, 16));
    ledger.record_original_frame_instance(owner, &data_frame(16, 8));
    ledger.record_original_frame_instance(owner, &data_frame(32, 8));
    ledger.record_reinjection_frame_instance(copy, &data_frame(2, 12));
    ledger.release_normalized_acked_ranges(&[OffsetRange { start: 4, end: 8 }]);
    let gaps = [
        OffsetRange { start: 1, end: 4 },
        OffsetRange { start: 8, end: 20 },
    ];
    assert_eq!(
        ledger.recovery_service_boundaries(&gaps),
        vec![1, 2, 4, 8, 14, 16, 20],
        "only scoped assignment/copy boundaries, not an unrelated silent extent"
    );
    assert!(ledger.recovery_service_boundaries(&[]).is_empty());
    assert!(
        ledger
            .flights
            .values()
            .flatten()
            .all(|flight| flight.original_recovery_timing.is_none())
    );
}

#[test]
fn original_recovery_timing_initializes_siblings_split_before_observation() {
    let owner = path(UnderlayProtocol::Udp, 0, 201);
    let mut ledger = RequestFlightLedger::default();
    ledger.record_original_frame_instance(owner, &data_frame(0, 16));
    ledger.record_original_frame_instance(owner, &data_frame(32, 8));
    ledger.release_normalized_acked_ranges(&[OffsetRange { start: 4, end: 12 }]);
    assert!(ledger.flights[&0][0].original_recovery_timing.is_none());
    assert!(ledger.flights[&12][0].original_recovery_timing.is_none());

    let first = ledger
        .observe_original_recovery_timing_for_range(OffsetRange { start: 0, end: 4 }, |instance| {
            assert_eq!(instance, owner);
            Some(recovery_timing_snapshot(owner, 100.0))
        })
        .expect("real retained Original prefix");
    for start in [0, 12] {
        assert_eq!(
            ledger.flights[&start][0].assignment_range,
            OffsetRange { start: 0, end: 16 },
        );
        assert_eq!(
            ledger.flights[&start][0].original_recovery_timing,
            Some(first)
        );
    }
    assert!(
        ledger.flights[&32][0].original_recovery_timing.is_none(),
        "another silent assignment is not initialized by this query"
    );
    let sibling = ledger
        .observe_original_recovery_timing_for_range(OffsetRange { start: 12, end: 16 }, |_| {
            Some(recovery_timing_snapshot(owner, 1_000.0))
        })
        .expect("right sibling inherits the first observation");
    assert_eq!(sibling, first);
}

#[test]
fn original_recovery_timing_survives_another_range_and_later_inflation() {
    let owner = path(UnderlayProtocol::Udp, 0, 202);
    let mut ledger = RequestFlightLedger::default();
    ledger.record_original_frame_instance(owner, &data_frame(0, 4));
    ledger.record_original_frame_instance(owner, &data_frame(4, 4));
    let first = ledger
        .observe_original_recovery_timing_for_range(OffsetRange { start: 0, end: 4 }, |_| {
            Some(recovery_timing_snapshot(owner, 100.0))
        })
        .unwrap();
    assert!(ledger.flights[&4][0].original_recovery_timing.is_none());
    let second = ledger
        .observe_original_recovery_timing_for_range(OffsetRange { start: 4, end: 8 }, |_| {
            Some(recovery_timing_snapshot(owner, 500.0))
        })
        .unwrap();
    assert!(second.fallback_at > first.fallback_at);
    let revisited = ledger
        .observe_original_recovery_timing_for_range(OffsetRange { start: 0, end: 4 }, |_| {
            Some(recovery_timing_snapshot(owner, 2_000.0))
        })
        .unwrap();
    assert_eq!(revisited, first);
    assert_eq!(ledger.flights[&4][0].original_recovery_timing, Some(second));
}

#[test]
fn original_recovery_timing_tightens_all_siblings_but_not_copy_clocks() {
    let owner = path(UnderlayProtocol::Udp, 0, 203);
    let copy = path(UnderlayProtocol::Tcp, 1, 204);
    let mut ledger = RequestFlightLedger::default();
    ledger.record_original_frame_instance(owner, &data_frame(0, 12));
    ledger.record_reinjection_frame_instance(copy, &data_frame(0, 4));
    let copy_deadline = ledger.flights[&0][1].reinjection_suppression_deadline;
    ledger.release_normalized_acked_ranges(&[OffsetRange { start: 4, end: 8 }]);
    let initial = ledger
        .observe_original_recovery_timing_for_range(OffsetRange { start: 8, end: 12 }, |_| {
            Some(recovery_timing_snapshot(owner, 2_000.0))
        })
        .unwrap();
    let tightened = ledger
        .observe_original_recovery_timing_for_range(OffsetRange { start: 0, end: 4 }, |instance| {
            assert_eq!(instance, owner, "copies cannot lend owner timing");
            Some(recovery_timing_snapshot(owner, 50.0))
        })
        .unwrap();
    assert!(tightened.loss_at < initial.loss_at);
    assert!(tightened.fallback_at < initial.fallback_at);
    for start in [0, 8] {
        assert_eq!(
            ledger.flights[&start][0].original_recovery_timing,
            Some(tightened)
        );
    }
    assert!(ledger.flights[&0][1].original_recovery_timing.is_none());
    assert_eq!(
        ledger.flights[&0][1].reinjection_suppression_deadline,
        copy_deadline
    );
}

#[test]
fn original_recovery_timing_aggregates_absolute_clocks_not_latest_assignment() {
    let owner = path(UnderlayProtocol::Udp, 0, 205);
    let mut ledger = RequestFlightLedger::default();
    ledger.record_original_frame_instance(owner, &data_frame(0, 4));
    ledger.record_original_frame_instance(owner, &data_frame(4, 4));
    let older_at = ledger.flights[&0][0].sent_at;
    let newer_at = ledger.flights[&4][0].sent_at;
    // Derive the fixture's interval from actual producer timestamps so no
    // scheduler pause can invalidate the deliberately later older deadline.
    let slow_rtt_ms = (newer_at.saturating_duration_since(older_at).as_secs_f64() + 1.0) * 1_000.0;
    let older = ledger
        .observe_original_recovery_timing_for_range(OffsetRange { start: 0, end: 4 }, |_| {
            Some(recovery_timing_snapshot(owner, slow_rtt_ms))
        })
        .unwrap();
    let newer = ledger
        .observe_original_recovery_timing_for_range(OffsetRange { start: 4, end: 8 }, |_| {
            Some(recovery_timing_snapshot(owner, 10.0))
        })
        .unwrap();
    assert!(older.fallback_at > newer.fallback_at);
    assert!(older.loss_at > newer.loss_at);
    let combined = ledger
        .observe_original_recovery_timing_for_range(OffsetRange { start: 0, end: 8 }, |_| {
            Some(recovery_timing_snapshot(owner, slow_rtt_ms))
        })
        .unwrap();
    assert_eq!(combined.assignment_at, newer_at);
    assert_eq!(combined.fallback_at, older.fallback_at);
    assert_eq!(combined.loss_at, older.loss_at);
}

#[test]
fn original_recovery_timing_is_inherited_and_reclaimed_by_ack() {
    let owner = path(UnderlayProtocol::Tcp, 0, 206);
    let mut ledger = RequestFlightLedger::default();
    ledger.record_original_frame_instance(owner, &data_frame(0, 8));
    let first = ledger
        .observe_original_recovery_timing_for_range(OffsetRange { start: 0, end: 8 }, |_| {
            Some(recovery_timing_snapshot(owner, 100.0))
        })
        .unwrap();
    ledger.release_normalized_acked_ranges(&[OffsetRange { start: 0, end: 4 }]);
    assert_eq!(
        ledger.flights[&4][0].assignment_range,
        OffsetRange { start: 0, end: 8 }
    );
    assert_eq!(ledger.flights[&4][0].original_recovery_timing, Some(first));
    ledger.release_normalized_acked_ranges(&[OffsetRange { start: 4, end: 8 }]);
    assert!(ledger.flights.is_empty());
    assert_eq!(ledger.total_original_data_in_flight_bytes(), 0);
    assert_eq!(
        ledger.observe_original_recovery_timing_for_range(OffsetRange { start: 0, end: 8 }, |_| {
            panic!("fully ACKed timing has no separate retained owner")
        }),
        None,
    );
    ledger.record_original_frame_instance(owner, &data_frame(8, 4));
    assert!(ledger.flights[&8][0].original_recovery_timing.is_none());
    ledger.drain_all();
    assert!(ledger.flights.is_empty());
}

#[test]
fn original_recovery_timing_requires_full_original_coverage_before_observation() {
    let owner = path(UnderlayProtocol::Tcp, 0, 207);
    let copy = path(UnderlayProtocol::Udp, 1, 208);
    let mut ledger = RequestFlightLedger::default();
    ledger.record_original_frame_instance(owner, &data_frame(0, 4));
    ledger.record_reinjection_frame_instance(copy, &data_frame(4, 4));
    ledger.record_original_frame_instance(owner, &data_frame(8, 4));
    assert_eq!(
        ledger
            .observe_original_recovery_timing_for_range(OffsetRange { start: 0, end: 12 }, |_| {
                panic!("a copy-covered Original hole cannot initialize any clocks")
            }),
        None,
    );
    assert!(
        ledger
            .flights
            .values()
            .flatten()
            .all(|flight| flight.original_recovery_timing.is_none())
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
