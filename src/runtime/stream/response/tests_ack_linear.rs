//! Complete ordering-transaction oracle. This deliberately keeps the old
//! algorithm in test code; it is not another production representation.
use super::*;
use crate::protocol::PathId;

impl ResponseAckOrderingState {
    fn reference_apply(
        &mut self,
        ranges: &[OffsetRange],
        released: &[(u64, CarrierPathReleasedFlight)],
    ) -> ResponseAckOrderingUpdate {
        let previous_frontier = self.contiguous_frontier;
        let previous_hole_bytes = self.acked_hole_bytes();
        let mut newly_contiguous = Vec::new();

        for (offset, release) in released {
            let flight = release.flight;
            let hole = CarrierPathAckedHole {
                key: flight.key,
                output_incarnation: flight.output_incarnation,
                end: flight.end,
                bytes: flight.bytes as u64,
                sent_at: flight.sent_at,
                kind: flight.kind,
                path_proving: release.path_proving,
            };
            if hole.end <= self.contiguous_frontier {
                newly_contiguous.push(hole);
            } else {
                self.acked_holes.entry(*offset).or_default().push(hole);
            }
        }

        self.reference_advance(ranges);
        let frontier = self.contiguous_frontier;
        self.acked_holes.retain(|_, holes| {
            holes.retain(|hole| {
                if hole.end <= frontier {
                    newly_contiguous.push(*hole);
                    false
                } else {
                    true
                }
            });
            !holes.is_empty()
        });
        let acked_hole_bytes = self.acked_hole_bytes();

        ResponseAckOrderingUpdate {
            changed: previous_frontier != self.contiguous_frontier
                || previous_hole_bytes != acked_hole_bytes
                || !newly_contiguous.is_empty(),
            contiguous_frontier: self.contiguous_frontier,
            #[cfg(feature = "lab-diagnostics")]
            acked_hole_bytes,
            newly_contiguous,
        }
    }

    fn reference_advance(&mut self, ranges: &[OffsetRange]) {
        loop {
            let mut next_frontier = self.contiguous_frontier;
            for range in ranges {
                if range.start > next_frontier {
                    break;
                }
                if range.end > next_frontier {
                    next_frontier = range.end;
                }
            }
            for (offset, holes) in self.acked_holes.range(..=next_frontier) {
                if *offset > next_frontier {
                    break;
                }
                for hole in holes {
                    if hole.end > next_frontier {
                        next_frontier = hole.end;
                    }
                }
            }
            if next_frontier == self.contiguous_frontier {
                break;
            }
            self.contiguous_frontier = next_frontier;
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct HoleView {
    key: CarrierPathKey,
    incarnation: u64,
    end: u64,
    bytes: u64,
    sent_at: Instant,
    kind: CarrierWorkKind,
    proving: bool,
}
fn view(hole: &CarrierPathAckedHole) -> HoleView {
    HoleView {
        key: hole.key,
        incarnation: hole.output_incarnation,
        end: hole.end,
        bytes: hole.bytes,
        sent_at: hole.sent_at,
        kind: hole.kind,
        proving: hole.path_proving,
    }
}
fn holes(state: &ResponseAckOrderingState) -> Vec<(u64, Vec<HoleView>)> {
    state
        .acked_holes
        .iter()
        .map(|(&start, entries)| (start, entries.iter().map(view).collect()))
        .collect()
}
fn release(
    start: u64,
    end: u64,
    id: u64,
    copy: bool,
    proving: bool,
    now: Instant,
) -> (u64, CarrierPathReleasedFlight) {
    let mut flight = CarrierPathFlight::fixed_output(
        CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId((id % 8) as u16),
        },
        end,
        (end - start) as usize,
        now,
        if copy {
            CarrierWorkKind::ReinjectedData
        } else {
            CarrierWorkKind::OriginalData
        },
        None,
    );
    flight.output_incarnation = id;
    (
        start,
        CarrierPathReleasedFlight {
            flight,
            path_proving: proving,
            qualification_ambiguous_ranges: SmallVec::new(),
        },
    )
}
fn rng(seed: &mut u64) -> u64 {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    *seed
}
fn assert_update(actual: &ResponseAckOrderingUpdate, expected: &ResponseAckOrderingUpdate) {
    assert_eq!(actual.changed, expected.changed);
    assert_eq!(actual.contiguous_frontier, expected.contiguous_frontier);
    assert_eq!(
        actual.newly_contiguous.iter().map(view).collect::<Vec<_>>(),
        expected
            .newly_contiguous
            .iter()
            .map(view)
            .collect::<Vec<_>>()
    );
    #[cfg(feature = "lab-diagnostics")]
    assert_eq!(actual.acked_hole_bytes, expected.acked_hole_bytes);
}

fn retained_ordering_rows(state: &ResponseAckOrderingState) -> Vec<(u64, HoleView)> {
    state
        .acked_holes
        .iter()
        .flat_map(|(&start, entries)| {
            entries
                .iter()
                .filter(|hole| hole.kind != CarrierWorkKind::ReinjectedData || hole.path_proving)
                .map(move |hole| (start, view(hole)))
        })
        .collect()
}

fn ordering_coverage(state: &ResponseAckOrderingState) -> Vec<(u64, u64)> {
    state
        .acked_holes
        .iter()
        .map(|(&start, entries)| {
            (
                start,
                entries.iter().map(|hole| hole.end).max().unwrap_or(start),
            )
        })
        .collect()
}

fn latest_original_rows(state: &ResponseAckOrderingState) -> Vec<(u64, Option<HoleView>)> {
    state
        .acked_holes
        .iter()
        .map(|(&start, entries)| (start, response_latest_original_hole(entries).map(view)))
        .collect()
}

fn assert_same_consumed_observations(
    actual: &ResponseAckOrderingState,
    expected: &ResponseAckOrderingState,
    actual_update: &ResponseAckOrderingUpdate,
    expected_update: &ResponseAckOrderingUpdate,
) {
    assert_eq!(actual_update.changed, expected_update.changed);
    assert_eq!(
        actual_update.contiguous_frontier,
        expected_update.contiguous_frontier
    );
    assert_eq!(actual.contiguous_frontier, expected.contiguous_frontier);
    assert_eq!(ordering_coverage(actual), ordering_coverage(expected));
    assert_eq!(
        retained_ordering_rows(actual),
        retained_ordering_rows(expected)
    );
    assert_eq!(latest_original_rows(actual), latest_original_rows(expected));
    assert_eq!(
        actual_update
            .newly_contiguous
            .iter()
            .filter(|hole| hole.path_proving)
            .map(view)
            .collect::<Vec<_>>(),
        expected_update
            .newly_contiguous
            .iter()
            .filter(|hole| hole.path_proving)
            .map(view)
            .collect::<Vec<_>>()
    );
    #[cfg(feature = "lab-diagnostics")]
    assert_eq!(
        actual_update.acked_hole_bytes,
        expected_update.acked_hole_bytes
    );
}

#[test]
fn linear_ack_transaction_exact_replay() {
    let now = Instant::now();
    let mut seed = 0x5513_779d_22ae_6743_u64;
    let mut actual = ResponseAckOrderingState::default();
    let mut expected = ResponseAckOrderingState::default();
    for step in 0..12_000_u64 {
        if step.is_multiple_of(100) {
            actual = ResponseAckOrderingState::default();
            expected = ResponseAckOrderingState::default();
        }
        let masks = normalize_offset_ranges(
            (0..rng(&mut seed) % 8)
                .map(|_| {
                    let start = rng(&mut seed) % 256;
                    OffsetRange {
                        start,
                        end: start + 1 + rng(&mut seed) % 32,
                    }
                })
                .collect(),
        );
        let mut released = Vec::new();
        if !step.is_multiple_of(7) {
            for mask in &masks {
                for extra in 0..(1 + u64::from(step.is_multiple_of(9))) {
                    // Proving copy inputs are deliberately preserved verbatim
                    // even though production copies carry no such authority.
                    released.push(release(
                        mask.start,
                        mask.end,
                        step + extra,
                        rng(&mut seed).is_multiple_of(2),
                        true,
                        now,
                    ));
                }
            }
        }
        let a = actual.apply_normalized_ack(&masks, &released);
        let e = expected.reference_apply(&masks, &released);
        assert_update(&a, &e);
        assert_eq!(holes(&actual), holes(&expected));
    }
}

#[test]
fn linear_ack_closes_large_adjacent_history_and_max_offset() {
    let now = Instant::now();
    let mut actual = ResponseAckOrderingState::default();
    for start in 1..=4096_u64 {
        let r = release(start, start + 1, start, false, true, now);
        actual.apply_normalized_ack(
            &[OffsetRange {
                start,
                end: start + 1,
            }],
            &[r],
        );
    }
    let update = actual.apply_normalized_ack(&[OffsetRange { start: 0, end: 1 }], &[]);
    assert_eq!(update.contiguous_frontier, 4097);
    assert_eq!(update.newly_contiguous.len(), 4096);
    assert!(actual.acked_holes.is_empty());
    actual.contiguous_frontier = u64::MAX - 2;
    actual.apply_normalized_ack(
        &[OffsetRange {
            start: u64::MAX - 1,
            end: u64::MAX,
        }],
        &[release(u64::MAX - 1, u64::MAX, 0, false, true, now)],
    );
    let update = actual.apply_normalized_ack(
        &[OffsetRange {
            start: u64::MAX - 2,
            end: u64::MAX - 1,
        }],
        &[],
    );
    assert_eq!(update.contiguous_frontier, u64::MAX);
    assert!(actual.acked_holes.is_empty());
}

#[test]
fn ack_local_delta_preserves_zero_volume_duplicate_and_tie_changes() {
    let now = Instant::now();
    let mut actual = ResponseAckOrderingState::default();
    let mask = [OffsetRange { start: 10, end: 20 }];
    let r = release(10, 20, 1, false, true, now);
    assert!(
        actual
            .apply_normalized_ack(&mask, std::slice::from_ref(&r))
            .changed
    );
    assert!(!actual.apply_normalized_ack(&mask, &[]).changed);
    // Equal-volume replacement leaves the preexisting observable changed flag false.
    assert!(
        !actual
            .apply_normalized_ack(&mask, &[release(10, 20, 2, false, false, now)])
            .changed
    );
    let mut zero = release(10, 20, 3, false, false, now);
    zero.1.flight.bytes = 0;
    assert!(actual.apply_normalized_ack(&mask, &[zero]).changed);
    let update = actual.apply_normalized_ack(&[OffsetRange { start: 0, end: 10 }], &[]);
    assert!(update.changed);
    assert_eq!(update.contiguous_frontier, 20);
    assert_eq!(update.newly_contiguous.len(), 3);
}

#[test]
fn acknowledged_copy_history_has_one_nonproving_witness_per_start() {
    let now = Instant::now();
    let mut state = ResponseAckOrderingState::default();
    for id in 0..10_000_u64 {
        let r = release(10, 20 + (id % 7), id, true, false, now);
        state.apply_normalized_ack(
            &[OffsetRange {
                start: 10,
                end: 20 + (id % 7),
            }],
            &[r],
        );
    }
    let bucket = state.acked_holes.get(&10).expect("copy coverage retained");
    assert_eq!(bucket.len(), 1);
    assert_eq!(bucket[0].end, 26);
    assert!(!bucket[0].path_proving);
    assert_eq!(state.acked_hole_bytes(), 0);
    // Original observations remain distinct and ordered around the witness.
    state.apply_normalized_ack(
        &[OffsetRange { start: 10, end: 20 }],
        &[
            release(10, 20, 10_001, false, true, now),
            release(10, 20, 10_002, false, false, now),
        ],
    );
    assert_eq!(state.acked_holes.get(&10).unwrap().len(), 3);
    let update = state.apply_normalized_ack(&[OffsetRange { start: 0, end: 10 }], &[]);
    assert_eq!(update.contiguous_frontier, 26);
    let originals = update
        .newly_contiguous
        .iter()
        .filter(|hole| hole.kind.is_original_transmission())
        .map(|hole| hole.output_incarnation)
        .collect::<Vec<_>>();
    assert_eq!(originals, vec![10_001, 10_002]);
    assert!(state.acked_holes.is_empty());
}

#[test]
fn acknowledged_copy_witness_preserves_consumed_observations() {
    let now = Instant::now();
    let mut actual = ResponseAckOrderingState::default();
    let mut expected = ResponseAckOrderingState::default();

    // Include a later shorter copy and an equal-end tie. The retained witness
    // must be an actual maximum-end record, and the first tied maximum stays.
    for (id, end, copy, proving) in [
        (1, 13, true, false),
        (2, 17, true, false),
        (3, 12, true, false),
        (4, 17, true, false),
        (5, 15, true, true),
        (6, 16, false, true),
        (7, 14, false, false),
    ] {
        let ranges = [OffsetRange { start: 10, end }];
        let released = [release(10, end, id, copy, proving, now)];
        let actual_update = actual.apply_normalized_ack(&ranges, &released);
        let expected_update = expected.reference_apply(&ranges, &released);
        assert_same_consumed_observations(&actual, &expected, &actual_update, &expected_update);
    }

    let compacted = actual.acked_holes.get(&10).expect("retained copy coverage");
    assert_eq!(
        compacted
            .iter()
            .filter(|hole| { hole.kind == CarrierWorkKind::ReinjectedData && !hole.path_proving })
            .count(),
        1
    );
    assert_eq!(compacted[0].end, 17);
    assert_eq!(
        compacted[0].output_incarnation, 2,
        "equal-end ties keep the first real witness"
    );
    assert_eq!(
        retained_ordering_rows(&actual),
        retained_ordering_rows(&expected)
    );

    // Original records, including a non-proving one, remain exact and
    // ordered. The latest-Original debt query still selects the last row.
    for (id, end, proving) in [(8, 14, true), (9, 16, false), (10, 15, true)] {
        let ranges = [OffsetRange { start: 10, end }];
        let released = [release(10, end, id, false, proving, now)];
        let actual_update = actual.apply_normalized_ack(&ranges, &released);
        let expected_update = expected.reference_apply(&ranges, &released);
        assert_same_consumed_observations(&actual, &expected, &actual_update, &expected_update);
    }

    let ranges = [OffsetRange { start: 0, end: 10 }];
    let actual_update = actual.apply_normalized_ack(&ranges, &[]);
    let expected_update = expected.reference_apply(&ranges, &[]);
    assert_same_consumed_observations(&actual, &expected, &actual_update, &expected_update);
    assert_eq!(actual_update.contiguous_frontier, 17);
    assert!(actual.acked_holes.is_empty());
}

#[test]
fn direct_original_ack_preserves_reference_partition_and_fallback() {
    let now = Instant::now();
    for mut releases in [
        vec![
            release(0, 12, 1, false, true, now),
            release(1, 6, 2, false, false, now),
        ],
        vec![
            release(1, 6, 2, false, false, now),
            release(0, 12, 1, false, true, now),
        ],
        vec![
            release(0, 12, 1, false, true, now),
            release(1, 6, 2, true, true, now),
        ],
    ] {
        let mut actual = ResponseAckOrderingState::default();
        let mut expected = ResponseAckOrderingState::default();
        actual.contiguous_frontier = 8;
        expected.contiguous_frontier = 8;
        // A zero-byte row still carries its exact metadata and order.
        releases[0].1.flight.bytes = 0;
        let mask = [OffsetRange { start: 0, end: 12 }];
        let got = actual.apply_normalized_ack(&mask, &releases);
        let reference = expected.reference_apply(&mask, &releases);
        assert_update(&got, &reference);
        assert_eq!(holes(&actual), holes(&expected));
    }

    // A same-start flight can emit multiple ACK fragments before a later
    // sibling emits from that start, making the released-offset sequence
    // nonmonotone. This must take the general path to preserve BTree/VEC order.
    let mut actual = ResponseAckOrderingState::default();
    let mut expected = ResponseAckOrderingState::default();
    actual.contiguous_frontier = 4;
    expected.contiguous_frontier = 4;
    let releases = [
        release(0, 12, 20, false, true, now),
        release(4, 6, 21, false, false, now),
        release(0, 5, 22, false, true, now),
    ];
    let mask = [OffsetRange { start: 0, end: 12 }];
    let got = actual.apply_normalized_ack(&mask, &releases);
    let reference = expected.reference_apply(&mask, &releases);
    assert_update(&got, &reference);
    assert_eq!(holes(&actual), holes(&expected));
    assert_eq!(
        got.newly_contiguous
            .iter()
            .map(|hole| hole.output_incarnation)
            .collect::<Vec<_>>(),
        vec![20, 22, 21]
    );
}

#[test]
fn ack_local_delta_handles_maximum_extent_and_zero_latest_volume() {
    let now = Instant::now();
    let end = usize::MAX as u64;
    assert!(end > 1);
    let mut actual = ResponseAckOrderingState::default();
    let mut expected = ResponseAckOrderingState::default();

    // This valid Original extent remains below the stream offset ceiling. Its
    // near-maximum volume checks that the wrapping transaction delta still
    // distinguishes a nonzero-to-zero latest-Original replacement.
    let ranges = [OffsetRange { start: 1, end }];
    let first = [release(1, end, 1, false, true, now)];
    let got = actual.apply_normalized_ack(&ranges, &first);
    let reference = expected.reference_apply(&ranges, &first);
    assert_update(&got, &reference);
    assert_eq!(actual.acked_hole_bytes(), end - 1);

    let mut zero = release(1, end, 2, false, false, now);
    zero.1.flight.bytes = 0;
    let got = actual.apply_normalized_ack(&ranges, std::slice::from_ref(&zero));
    let reference = expected.reference_apply(&ranges, std::slice::from_ref(&zero));
    assert_update(&got, &reference);
    assert!(got.changed);
    assert_eq!(actual.acked_hole_bytes(), 0);

    let close = [OffsetRange { start: 0, end: 1 }];
    let got = actual.apply_normalized_ack(&close, &[]);
    let reference = expected.reference_apply(&close, &[]);
    assert_update(&got, &reference);
    assert_eq!(got.contiguous_frontier, end);
    assert_eq!(got.newly_contiguous.len(), 2);
    assert!(actual.acked_holes.is_empty());
}

#[test]
#[ignore = "manual counterbalanced complete response ACK transaction cost fixture"]
fn response_ack_complete_transaction_release_cost_fixture() {
    const LADDER: u64 = 4096;
    const SINGLE_RECORDS: u64 = 20_000;
    const DUPLICATE_REPETITIONS: usize = 1000;
    let now = Instant::now();

    fn timed_single<const REFERENCE: bool>(
        records: &[(OffsetRange, (u64, CarrierPathReleasedFlight))],
    ) -> (std::time::Duration, ResponseAckOrderingState) {
        use std::hint::black_box;
        let mut state = ResponseAckOrderingState::default();
        let started = Instant::now();
        for (range, released) in records {
            let update = if REFERENCE {
                state.reference_apply(std::slice::from_ref(range), std::slice::from_ref(released))
            } else {
                state.apply_normalized_ack(
                    std::slice::from_ref(range),
                    std::slice::from_ref(released),
                )
            };
            black_box(update);
        }
        (started.elapsed(), state)
    }

    fn gap_states(
        now: Instant,
        ladder: u64,
    ) -> (ResponseAckOrderingState, ResponseAckOrderingState) {
        let mut actual = ResponseAckOrderingState::default();
        let mut expected = ResponseAckOrderingState::default();
        for start in 1..=ladder {
            let (_, released) = release(start, start + 1, start, false, true, now);
            let flight = released.flight;
            let hole = CarrierPathAckedHole {
                key: flight.key,
                output_incarnation: flight.output_incarnation,
                end: flight.end,
                bytes: flight.bytes as u64,
                sent_at: flight.sent_at,
                kind: flight.kind,
                path_proving: true,
            };
            actual.acked_holes.insert(start, vec![hole]);
            expected.acked_holes.insert(start, vec![hole]);
        }
        assert_eq!(holes(&actual), holes(&expected));
        (actual, expected)
    }

    fn timed_duplicate<const REFERENCE: bool>(
        mut state: ResponseAckOrderingState,
        ranges: &[OffsetRange],
        repetitions: usize,
    ) -> (std::time::Duration, ResponseAckOrderingState) {
        use std::hint::black_box;
        let started = Instant::now();
        for _ in 0..repetitions {
            let update = if REFERENCE {
                state.reference_apply(ranges, &[])
            } else {
                state.apply_normalized_ack(ranges, &[])
            };
            black_box(update);
        }
        (started.elapsed(), state)
    }

    fn timed_close<const REFERENCE: bool>(
        mut state: ResponseAckOrderingState,
        ranges: &[OffsetRange],
    ) -> (std::time::Duration, ResponseAckOrderingState, usize) {
        use std::hint::black_box;
        let started = Instant::now();
        let update = if REFERENCE {
            state.reference_apply(ranges, &[])
        } else {
            state.apply_normalized_ack(ranges, &[])
        };
        let output_len = update.newly_contiguous.len();
        black_box(update);
        (started.elapsed(), state, output_len)
    }

    let single_records = (0..SINGLE_RECORDS)
        .map(|offset| {
            (
                OffsetRange {
                    start: offset,
                    end: offset + 1,
                },
                release(offset, offset + 1, offset, false, true, now),
            )
        })
        .collect::<Vec<_>>();
    let duplicate_mask = [OffsetRange {
        start: LADDER + 2,
        end: LADDER + 3,
    }];
    let close_mask = [OffsetRange { start: 0, end: 1 }];

    for round in 0..3 {
        let (single_reference, single_actual) = if round % 2 == 0 {
            let reference = timed_single::<true>(&single_records);
            let actual = timed_single::<false>(&single_records);
            (reference, actual)
        } else {
            let actual = timed_single::<false>(&single_records);
            let reference = timed_single::<true>(&single_records);
            (reference, actual)
        };
        assert_eq!(single_reference.1.contiguous_frontier, SINGLE_RECORDS);
        assert_eq!(single_actual.1.contiguous_frontier, SINGLE_RECORDS);
        assert!(single_reference.1.acked_holes.is_empty());
        assert!(single_actual.1.acked_holes.is_empty());
        println!(
            "round={round} operation=single_lifecycle records={SINGLE_RECORDS} reference_ns_per_txn={:.3} candidate_ns_per_txn={:.3}",
            single_reference.0.as_nanos() as f64 / SINGLE_RECORDS as f64,
            single_actual.0.as_nanos() as f64 / SINGLE_RECORDS as f64,
        );

        let (actual_gaps, reference_gaps) = gap_states(now, LADDER);
        let (duplicate_reference, duplicate_actual) = if round % 2 == 0 {
            let reference =
                timed_duplicate::<true>(reference_gaps, &duplicate_mask, DUPLICATE_REPETITIONS);
            let actual =
                timed_duplicate::<false>(actual_gaps, &duplicate_mask, DUPLICATE_REPETITIONS);
            (reference, actual)
        } else {
            let actual =
                timed_duplicate::<false>(actual_gaps, &duplicate_mask, DUPLICATE_REPETITIONS);
            let reference =
                timed_duplicate::<true>(reference_gaps, &duplicate_mask, DUPLICATE_REPETITIONS);
            (reference, actual)
        };
        assert_eq!(holes(&duplicate_reference.1), holes(&duplicate_actual.1));
        println!(
            "round={round} operation=duplicate_gap holes={LADDER} repetitions={DUPLICATE_REPETITIONS} reference_ns_per_txn={:.3} candidate_ns_per_txn={:.3}",
            duplicate_reference.0.as_nanos() as f64 / DUPLICATE_REPETITIONS as f64,
            duplicate_actual.0.as_nanos() as f64 / DUPLICATE_REPETITIONS as f64,
        );

        let (actual_gaps, reference_gaps) = gap_states(now, LADDER);
        let (close_reference, close_actual) = if round % 2 == 0 {
            let reference = timed_close::<true>(reference_gaps, &close_mask);
            let actual = timed_close::<false>(actual_gaps, &close_mask);
            (reference, actual)
        } else {
            let actual = timed_close::<false>(actual_gaps, &close_mask);
            let reference = timed_close::<true>(reference_gaps, &close_mask);
            (reference, actual)
        };
        assert_eq!(close_reference.1.contiguous_frontier, LADDER + 1);
        assert_eq!(close_actual.1.contiguous_frontier, LADDER + 1);
        assert!(close_reference.1.acked_holes.is_empty());
        assert!(close_actual.1.acked_holes.is_empty());
        assert_eq!(close_reference.2, LADDER as usize);
        assert_eq!(close_actual.2, LADDER as usize);
        println!(
            "round={round} operation=close_ladder holes={LADDER} reference_ns={:.3} candidate_ns={:.3}",
            close_reference.0.as_nanos() as f64,
            close_actual.0.as_nanos() as f64,
        );
    }
}
