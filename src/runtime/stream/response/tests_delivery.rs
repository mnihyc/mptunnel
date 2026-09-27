use super::super::attachment::{
    ResponseDispatchTarget, ResponseOutputAttachment, ResponseOutputAttachmentState,
};
use super::super::next_server_carrier_path_instance_id;
use super::super::test_support::{
    binding_for_underlay, native_response_binding_fixture, stream_data_frame_at,
};
use super::*;
use crate::model::carrier_rate_authority::CarrierRateAuthorityBasis;
use crate::model::path::PathPolicy;
use crate::model::product_qualification::ProductQualificationLedger;
use crate::model::response::{response_oldest_lower_flight_owner, response_ordering_debt_bytes};
use crate::model::timing::ReliableDataAckGapTiming;
use crate::model::work::CarrierWorkKind;
use crate::protocol::{ConfiguredMemberSlot, OffsetRange, PathId, UnderlayProtocol};
use crate::runtime::path::commands::{
    ReliablePathCommand, reliable_path_command_channels, try_recv_reliable_path_command,
};
use crate::runtime::sender::ServerReinjectionOutputIdentity;
use crate::scheduler::TrafficClass;
use crate::transport::RateHint;
use std::collections::BTreeMap;
use std::hint::black_box;

fn key(underlay: UnderlayProtocol, path_id: u16) -> CarrierPathKey {
    CarrierPathKey {
        underlay,
        path_id: PathId(path_id),
    }
}

fn flight(key: CarrierPathKey, end: u64, bytes: usize, kind: CarrierWorkKind) -> CarrierPathFlight {
    CarrierPathFlight::fixed_output(
        key,
        end,
        bytes,
        Instant::now(),
        kind,
        (kind == CarrierWorkKind::ReinjectedData).then_some(Duration::from_millis(100)),
    )
}

fn range(start: u64, end: u64) -> OffsetRange {
    OffsetRange::new(start, end).expect("valid test range")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FlightState {
    key: CarrierPathKey,
    output_incarnation: u64,
    configured_slot: Option<ConfiguredMemberSlot>,
    end: u64,
    bytes: usize,
    sent_at: Instant,
    kind: CarrierWorkKind,
    owner_fallback_deadline: Option<Instant>,
    assignment_range: OffsetRange,
    original_recovery_timing: Option<ReliableDataAckGapTiming>,
    evidence_eligible: bool,
    qualification_receipt: Option<crate::model::product_qualification::ProductQualificationReceipt>,
    reinjection_suppression_deadline: Option<Instant>,
}

fn flight_state(flight: CarrierPathFlight) -> FlightState {
    FlightState {
        key: flight.key,
        output_incarnation: flight.output_incarnation,
        configured_slot: flight.configured_slot,
        end: flight.end,
        bytes: flight.bytes,
        sent_at: flight.sent_at,
        kind: flight.kind,
        owner_fallback_deadline: flight.owner_fallback_deadline,
        assignment_range: flight.assignment_range,
        original_recovery_timing: flight.original_recovery_timing,
        evidence_eligible: flight.evidence_eligible,
        qualification_receipt: flight.qualification_receipt,
        reinjection_suppression_deadline: flight.reinjection_suppression_deadline,
    }
}

fn ledger_state(flights: &BTreeMap<u64, Vec<CarrierPathFlight>>) -> Vec<(u64, Vec<FlightState>)> {
    flights
        .iter()
        .map(|(&start, entries)| (start, entries.iter().copied().map(flight_state).collect()))
        .collect()
}

fn release_state(
    released: &[(u64, CarrierPathReleasedFlight)],
) -> Vec<(u64, FlightState, bool, Vec<OffsetRange>)> {
    released
        .iter()
        .map(|(start, released)| {
            (
                *start,
                flight_state(released.flight),
                released.path_proving,
                released
                    .qualification_ambiguous_ranges
                    .iter()
                    .copied()
                    .collect(),
            )
        })
        .collect()
}

fn valid_differential_flight(
    start: u64,
    end: u64,
    kind: CarrierWorkKind,
    index: usize,
    now: Instant,
) -> CarrierPathFlight {
    let extent = end - start;
    let mut flight = flight(
        key(
            if index.is_multiple_of(2) {
                UnderlayProtocol::Tcp
            } else {
                UnderlayProtocol::Udp
            },
            (index % 7) as u16,
        ),
        end,
        extent as usize,
        kind,
    );
    flight.output_incarnation = index as u64 + 1;
    flight.configured_slot = Some(ConfiguredMemberSlot((index % 3) as u16));
    flight.sent_at = now + Duration::from_millis(index as u64);
    flight.assignment_range = range(start, end);
    if kind.is_original_transmission() {
        flight.owner_fallback_deadline = Some(now + Duration::from_secs(2 + index as u64));
        flight.original_recovery_timing = Some(ReliableDataAckGapTiming {
            assignment_at: now,
            loss_at: Some(now + Duration::from_millis(5)),
            fallback_at: now + Duration::from_secs(3),
        });
    }
    flight.evidence_eligible = index.is_multiple_of(2);
    flight.qualification_receipt = if kind.is_original_transmission() {
        let mut ledger = ProductQualificationLedger::default();
        Some(
            ledger
                .tag_admitted_original(1, extent, range(start, end))
                .expect("valid generated qualification receipt")
                .expect("positive generated range has a receipt"),
        )
    } else {
        None
    };
    flight.reinjection_suppression_deadline = (kind == CarrierWorkKind::ReinjectedData)
        .then_some(now + Duration::from_secs(4 + index as u64));
    flight
}

fn ranges_for_four_byte_mask(mask: u8) -> Vec<OffsetRange> {
    let mut ranges = Vec::new();
    let mut cursor = 0;
    while cursor < 4 {
        if mask & (1 << cursor) == 0 {
            cursor += 1;
            continue;
        }
        let start = cursor;
        while cursor < 4 && mask & (1 << cursor) != 0 {
            cursor += 1;
        }
        ranges.push(range(start, cursor));
    }
    ranges
}

fn assert_ack_release_matches_reference(
    initial: BTreeMap<u64, Vec<CarrierPathFlight>>,
    ranges: &[OffsetRange],
    context: &str,
) {
    let mut expected = initial.clone();
    let expected_released =
        super::release_carrier_path_flight_ranges_reference(&mut expected, ranges);
    let indexed_initial = initial.clone();
    let mut actual = initial;
    let actual_released = release_carrier_path_flight_ranges(&mut actual, ranges);
    assert_eq!(
        release_state(&actual_released),
        release_state(&expected_released),
        "released output differs for {context}"
    );
    assert_eq!(
        ledger_state(&actual),
        ledger_state(&expected),
        "retained ledger differs for {context}"
    );

    let mut indexed = ResponseProductFlightLedger::default();
    indexed.extend_for_test(indexed_initial);
    let indexed_released = indexed.release(ranges);
    assert_eq!(
        release_state(&indexed_released),
        release_state(&expected_released),
        "indexed release differs for {context}"
    );
    assert_eq!(
        ledger_state(&indexed),
        ledger_state(&expected),
        "indexed retained ledger differs for {context}"
    );
}

fn lifecycle_records(
    count: usize,
    duplicate_each: bool,
    long_crossing_flight: bool,
    now: Instant,
) -> Vec<(u64, CarrierPathFlight)> {
    let end = (count as u64).saturating_mul(4).saturating_add(1);
    let mut records = Vec::with_capacity(
        count
            .saturating_mul(1 + usize::from(duplicate_each))
            .saturating_add(usize::from(long_crossing_flight)),
    );
    if long_crossing_flight {
        let mut crossing = flight(
            key(UnderlayProtocol::Tcp, 0),
            end,
            end as usize,
            CarrierWorkKind::OriginalData,
        );
        crossing.sent_at = now;
        crossing.assignment_range = range(0, end);
        records.push((0, crossing));
    }
    for index in 0..count {
        let start = (index as u64) * 4;
        let mut original = flight(
            key(UnderlayProtocol::Tcp, (index % 251) as u16),
            start + 1,
            1,
            CarrierWorkKind::OriginalData,
        );
        original.sent_at = now;
        original.assignment_range = range(start, start + 1);
        records.push((start, original));
        if duplicate_each {
            let mut duplicate = flight(
                key(UnderlayProtocol::Udp, (index % 251) as u16),
                start + 1,
                1,
                CarrierWorkKind::ReinjectedData,
            );
            duplicate.sent_at = now;
            duplicate.assignment_range = range(start, start + 1);
            records.push((start, duplicate));
        }
    }
    records
}

fn time_reference_lifecycle(
    records: &[(u64, CarrierPathFlight)],
    ranges: &[OffsetRange],
    runs: usize,
) -> Duration {
    let started = Instant::now();
    let mut result_bytes = 0usize;
    for _ in 0..runs {
        let mut flights = BTreeMap::<u64, Vec<CarrierPathFlight>>::new();
        for &(start, flight) in records {
            flights.entry(start).or_default().push(flight);
        }
        result_bytes = result_bytes.saturating_add(
            black_box(release_carrier_path_flight_ranges(&mut flights, ranges))
                .iter()
                .map(|(_, release)| release.flight.bytes)
                .sum::<usize>(),
        );
        black_box(flights);
    }
    black_box(result_bytes);
    started.elapsed()
}

fn time_indexed_lifecycle(
    records: &[(u64, CarrierPathFlight)],
    ranges: &[OffsetRange],
    runs: usize,
) -> Duration {
    let started = Instant::now();
    let mut result_bytes = 0usize;
    for _ in 0..runs {
        let mut flights = ResponseProductFlightLedger::default();
        for &(start, flight) in records {
            flights.publish(start, flight);
        }
        result_bytes = result_bytes.saturating_add(
            black_box(flights.release(ranges))
                .iter()
                .map(|(_, release)| release.flight.bytes)
                .sum::<usize>(),
        );
        black_box(flights);
    }
    black_box(result_bytes);
    started.elapsed()
}

fn time_reference_cumulative_refill(
    records: &[(u64, CarrierPathFlight)],
    retained: usize,
    updates: usize,
    runs: usize,
) -> Duration {
    let started = Instant::now();
    let mut result_bytes = 0usize;
    for _ in 0..runs {
        let mut flights = BTreeMap::<u64, Vec<CarrierPathFlight>>::new();
        for &(start, flight) in records.iter().take(retained) {
            flights.entry(start).or_default().push(flight);
        }
        for (step, &(start, flight)) in records[retained..retained + updates].iter().enumerate() {
            flights.entry(start).or_default().push(flight);
            let ack_end = records[step].0 + 1;
            result_bytes = result_bytes.saturating_add(
                black_box(release_carrier_path_flight_ranges(
                    &mut flights,
                    &[range(0, ack_end)],
                ))
                .iter()
                .map(|(_, release)| release.flight.bytes)
                .sum::<usize>(),
            );
            debug_assert_eq!(flights.values().map(Vec::len).sum::<usize>(), retained);
        }
        black_box(flights);
    }
    black_box(result_bytes);
    started.elapsed()
}

fn time_indexed_cumulative_refill(
    records: &[(u64, CarrierPathFlight)],
    retained: usize,
    updates: usize,
    runs: usize,
) -> Duration {
    let started = Instant::now();
    let mut result_bytes = 0usize;
    for _ in 0..runs {
        let mut flights = ResponseProductFlightLedger::default();
        for &(start, flight) in records.iter().take(retained) {
            flights.publish(start, flight);
        }
        for (step, &(start, flight)) in records[retained..retained + updates].iter().enumerate() {
            flights.publish(start, flight);
            let ack_end = records[step].0 + 1;
            result_bytes = result_bytes.saturating_add(
                black_box(flights.release(&[range(0, ack_end)]))
                    .iter()
                    .map(|(_, release)| release.flight.bytes)
                    .sum::<usize>(),
            );
            debug_assert_eq!(flights.values().map(Vec::len).sum::<usize>(), retained);
        }
        black_box(flights);
    }
    black_box(result_bytes);
    started.elapsed()
}

fn benchmark_cumulative_refill(name: &str, retained: usize, updates: usize, runs: usize) {
    let now = Instant::now();
    let records = lifecycle_records(retained + updates, false, false, now);
    for round in 0..3 {
        let (reference, indexed, order) = if round % 2 == 0 {
            let reference = time_reference_cumulative_refill(&records, retained, updates, runs);
            let indexed = time_indexed_cumulative_refill(&records, retained, updates, runs);
            (reference, indexed, "reference-first")
        } else {
            let indexed = time_indexed_cumulative_refill(&records, retained, updates, runs);
            let reference = time_reference_cumulative_refill(&records, retained, updates, runs);
            (reference, indexed, "indexed-first")
        };
        eprintln!(
            "lifecycle: case={name} retained_tail={retained} cumulative_ack_refills={updates} runs={runs} order={order} reference_total_ms={:.3} indexed_total_ms={:.3} ratio={:.3}",
            reference.as_secs_f64() * 1_000.0,
            indexed.as_secs_f64() * 1_000.0,
            indexed.as_secs_f64() / reference.as_secs_f64(),
        );
    }
}

#[test]
#[ignore = "manual append+release lifecycle and structural memory comparison"]
fn product_flight_indexed_lifecycle_cost_and_memory() {
    let (index_owner, index_node, union_header, key_bytes) =
        ResponseProductFlightLedger::overlap_layout_for_test();
    eprintln!(
        "ledger-layout: flight_payload={}B btree_map_header={}B vec_header={}B index_owner={}B union_headers={}B index_key={}B avl_node={}B; one Box<Node> allocation per retained flight plus BTreeMap U/M nodes per disjoint run",
        std::mem::size_of::<CarrierPathFlight>(),
        std::mem::size_of::<BTreeMap<u64, Vec<CarrierPathFlight>>>(),
        std::mem::size_of::<Vec<CarrierPathFlight>>(),
        index_owner,
        union_header,
        key_bytes,
        index_node,
    );

    let scenarios = [
        ("small-full-1", 1, false, false, 20_000usize, true),
        ("small-full-8", 8, false, false, 20_000, true),
        ("all-copy-full-4096", 4_096, true, false, 8, true),
        ("large-late-sparse-100k", 100_000, false, false, 3, false),
        ("large-crossing-late-100k", 100_000, false, true, 3, false),
    ];
    for (name, count, copies, crossing, runs, full) in scenarios {
        let now = Instant::now();
        let records = lifecycle_records(count, copies, crossing, now);
        let final_end = (count as u64).saturating_mul(4).saturating_add(1);
        let ack = if full {
            range(0, final_end)
        } else {
            let start = (count.saturating_sub(1) as u64) * 4;
            range(start, start + 1)
        };
        let ranges = [ack];
        for round in 0..3 {
            let (reference, indexed, order) = if round % 2 == 0 {
                let reference = time_reference_lifecycle(&records, &ranges, runs);
                let indexed = time_indexed_lifecycle(&records, &ranges, runs);
                (reference, indexed, "reference-first")
            } else {
                let indexed = time_indexed_lifecycle(&records, &ranges, runs);
                let reference = time_reference_lifecycle(&records, &ranges, runs);
                (reference, indexed, "indexed-first")
            };
            eprintln!(
                "lifecycle: case={name} flights={} runs={runs} order={order} reference_total_ms={:.3} indexed_total_ms={:.3} ratio={:.3} indexed_extra_node_struct_bytes={}B",
                records.len(),
                reference.as_secs_f64() * 1_000.0,
                indexed.as_secs_f64() * 1_000.0,
                indexed.as_secs_f64() / reference.as_secs_f64(),
                records.len().saturating_mul(index_node),
            );
        }
    }
    benchmark_cumulative_refill("steady-cumulative-retained-8", 8, 2_048, 2);
    benchmark_cumulative_refill("steady-cumulative-retained-512", 512, 2_048, 2);
}

fn output_identity(
    binding: &super::super::ResponseStreamBinding,
    key: CarrierPathKey,
) -> (CarrierPathKey, u64) {
    binding
        .sender_path_targets(TrafficClass::Throughput, 1)
        .into_iter()
        .find(|target| target.observation.key == key)
        .map(|target| (key, target.observation.incarnation))
        .expect("attached response output")
}

fn server_output_identity(
    binding: &super::super::ResponseStreamBinding,
    key: CarrierPathKey,
) -> ServerReinjectionOutputIdentity {
    let (key, incarnation) = output_identity(binding, key);
    ServerReinjectionOutputIdentity { key, incarnation }
}

#[test]
fn data_ack_hole_advances_only_after_the_prefix_arrives() {
    let path = key(UnderlayProtocol::Tcp, 0);
    let mut ordering = ResponseAckOrderingState::default();
    let second = CarrierPathReleasedFlight {
        flight: flight(path, 8192, 4096, CarrierWorkKind::OriginalData),
        path_proving: true,
        qualification_ambiguous_ranges: SmallVec::new(),
    };

    let update = ordering.apply_normalized_ack(&[range(4096, 8192)], &[(4096, second)]);
    assert_eq!(update.contiguous_frontier, 0);
    assert_eq!(ordering.acked_hole_bytes(), 4096);

    let first = CarrierPathReleasedFlight {
        flight: flight(path, 4096, 4096, CarrierWorkKind::OriginalData),
        path_proving: true,
        qualification_ambiguous_ranges: SmallVec::new(),
    };
    let update = ordering.apply_normalized_ack(&[range(0, 8192)], &[(0, first)]);
    assert_eq!(update.contiguous_frontier, 8192);
    assert_eq!(ordering.acked_hole_bytes(), 0);
    assert_eq!(update.newly_contiguous.len(), 2);
}

#[test]
fn partial_data_ack_splits_and_retains_exact_flight_ranges() {
    let path = key(UnderlayProtocol::Tcp, 0);
    let mut flights = BTreeMap::from([(
        0,
        vec![flight(path, 4096, 4096, CarrierWorkKind::OriginalData)],
    )]);

    let released = release_carrier_path_flight_ranges(&mut flights, &[range(1024, 3072)]);

    assert_eq!(released.len(), 1);
    assert_eq!(released[0].0, 1024);
    assert_eq!(released[0].1.flight.bytes, 2048);
    assert!(released[0].1.path_proving);
    assert_eq!(flights.get(&0).unwrap()[0].end, 1024);
    assert_eq!(flights.get(&3072).unwrap()[0].end, 4096);
}

#[test]
fn in_place_ack_release_matches_reference_for_small_generated_ledgers() {
    const GEOMETRIES: [(u64, u64); 7] = [(0, 1), (0, 4), (1, 2), (1, 4), (2, 3), (2, 4), (3, 4)];
    let now = Instant::now();
    for count in 1..=3 {
        let sequence_count = GEOMETRIES.len().pow(count as u32);
        for encoded_sequence in 0..sequence_count {
            let mut remaining = encoded_sequence;
            let mut sequence = Vec::with_capacity(count);
            for _ in 0..count {
                sequence.push(GEOMETRIES[remaining % GEOMETRIES.len()]);
                remaining /= GEOMETRIES.len();
            }
            for kind_mask in 0..(1usize << count) {
                let mut initial = BTreeMap::new();
                for (index, (start, end)) in sequence.iter().copied().enumerate() {
                    let kind = if kind_mask & (1 << index) == 0 {
                        CarrierWorkKind::OriginalData
                    } else {
                        CarrierWorkKind::ReinjectedData
                    };
                    initial
                        .entry(start)
                        .or_insert_with(Vec::new)
                        .push(valid_differential_flight(start, end, kind, index, now));
                }
                for ack_mask in 0..16 {
                    let ranges = ranges_for_four_byte_mask(ack_mask);
                    assert_ack_release_matches_reference(
                        initial.clone(),
                        &ranges,
                        &format!(
                            "count={count}, sequence={sequence:?}, kinds={kind_mask:#b}, ack={ack_mask:#06b}"
                        ),
                    );
                }
            }
        }
    }
}

#[test]
fn in_place_ack_release_preserves_metadata_and_repeated_duplicate_behavior() {
    let now = Instant::now();
    let mut initial = BTreeMap::new();
    initial.entry(0).or_insert_with(Vec::new).extend([
        valid_differential_flight(0, 8, CarrierWorkKind::OriginalData, 0, now),
        valid_differential_flight(0, 4, CarrierWorkKind::OriginalData, 2, now),
    ]);
    initial
        .entry(2)
        .or_insert_with(Vec::new)
        .push(valid_differential_flight(
            2,
            6,
            CarrierWorkKind::ReinjectedData,
            1,
            now,
        ));
    initial
        .entry(8)
        .or_insert_with(Vec::new)
        .push(valid_differential_flight(
            8,
            12,
            CarrierWorkKind::OriginalData,
            3,
            now,
        ));

    let mut expected = initial.clone();
    let mut actual = initial;
    for (pass, ranges) in [
        vec![range(2, 4), range(6, 7)],
        vec![range(2, 4), range(6, 7)],
        vec![range(0, 12)],
    ]
    .into_iter()
    .enumerate()
    {
        let expected_released =
            super::release_carrier_path_flight_ranges_reference(&mut expected, &ranges);
        let actual_released = release_carrier_path_flight_ranges(&mut actual, &ranges);
        assert_eq!(
            release_state(&actual_released),
            release_state(&expected_released),
            "release sequence differs on pass {pass}"
        );
        assert_eq!(
            ledger_state(&actual),
            ledger_state(&expected),
            "retained ledger differs on pass {pass}"
        );
    }
    assert!(
        actual.is_empty(),
        "the final ACK releases every remaining flight"
    );
}

#[test]
fn in_place_ack_release_preserves_absent_and_leading_prefix_receipts_across_hole_ack() {
    let now = Instant::now();
    let mut without_receipt =
        valid_differential_flight(0, 8, CarrierWorkKind::OriginalData, 0, now);
    without_receipt.qualification_receipt = None;
    let leading_receipt = valid_differential_flight(0, 8, CarrierWorkKind::OriginalData, 1, now);
    assert_eq!(
        leading_receipt
            .qualification_receipt
            .expect("generated strict-prefix receipt")
            .tagged_range(),
        range(0, 1)
    );
    let mut initial = BTreeMap::from([(0, vec![without_receipt, leading_receipt])]);
    initial
        .entry(2)
        .or_insert_with(Vec::new)
        .push(valid_differential_flight(
            2,
            6,
            CarrierWorkKind::ReinjectedData,
            2,
            now,
        ));

    let mut expected = initial.clone();
    let mut actual = initial;
    for (pass, ranges) in [vec![range(3, 5)], vec![range(3, 5)], vec![range(0, 8)]]
        .into_iter()
        .enumerate()
    {
        let expected_released =
            super::release_carrier_path_flight_ranges_reference(&mut expected, &ranges);
        let actual_released = release_carrier_path_flight_ranges(&mut actual, &ranges);
        assert_eq!(
            release_state(&actual_released),
            release_state(&expected_released),
            "receipt release differs on pass {pass}"
        );
        assert_eq!(
            ledger_state(&actual),
            ledger_state(&expected),
            "receipt ledger differs on pass {pass}"
        );
        for fragments in actual.values().flatten().filter(|flight| {
            flight.kind.is_original_transmission() && flight.output_incarnation <= 2
        }) {
            assert_eq!(
                fragments.assignment_range,
                range(0, 8),
                "split fragments preserve the original assignment"
            );
            if fragments.output_incarnation == 1 {
                assert!(fragments.qualification_receipt.is_none());
            }
        }
        if pass == 0 {
            let prefix_left = actual[&0]
                .iter()
                .find(|flight| flight.output_incarnation == 2)
                .expect("left original prefix remains at its source key");
            assert_eq!(
                prefix_left
                    .qualification_receipt
                    .expect("the tagged leading byte stays with the left fragment")
                    .tagged_range(),
                range(0, 1)
            );
            let prefix_right = actual[&5]
                .iter()
                .find(|flight| flight.output_incarnation == 2)
                .expect("right original suffix is staged at its start key");
            assert!(prefix_right.qualification_receipt.is_none());
        }
    }
    assert!(actual.is_empty());
}

#[test]
fn legacy_ack_rebuild_normalization_cases_are_outside_runtime_invariants() {
    let now = Instant::now();
    let mut oversized_receipt_ledger = ProductQualificationLedger::default();
    let oversized_receipt = oversized_receipt_ledger
        .tag_admitted_original(4, 4, range(0, 4))
        .expect("valid receipt fixture")
        .expect("positive receipt fixture");
    let mut inconsistent = valid_differential_flight(1, 3, CarrierWorkKind::OriginalData, 0, now);
    inconsistent.bytes = 99;
    inconsistent.qualification_receipt = Some(oversized_receipt);
    let zero_extent = flight(
        key(UnderlayProtocol::Tcp, 1),
        6,
        0,
        CarrierWorkKind::OriginalData,
    );
    let initial = BTreeMap::from([
        (1, vec![inconsistent]),
        (6, vec![zero_extent]),
        (9, Vec::new()),
    ]);

    let mut expected = initial.clone();
    let expected_released =
        super::release_carrier_path_flight_ranges_reference(&mut expected, &[range(12, 13)]);
    let mut actual = initial;
    let actual_released = release_carrier_path_flight_ranges(&mut actual, &[range(12, 13)]);

    assert!(expected_released.is_empty());
    assert!(actual_released.is_empty());
    assert_ne!(
        ledger_state(&actual),
        ledger_state(&expected),
        "the old full rebuild normalizes malformed untouched metadata and drops empty/zero extents"
    );
    assert_eq!(actual.get(&1).unwrap()[0].bytes, 99);
    assert_eq!(expected.get(&1).unwrap()[0].bytes, 2);
    assert_eq!(
        actual.get(&1).unwrap()[0]
            .qualification_receipt
            .expect("malformed oversized receipt remains in the no-hit candidate")
            .tagged_range(),
        range(0, 4)
    );
    assert_eq!(
        expected.get(&1).unwrap()[0]
            .qualification_receipt
            .expect("the reference intersects the oversized receipt")
            .tagged_range(),
        range(1, 3)
    );
    assert!(actual.contains_key(&6) && actual.get(&6).unwrap().len() == 1);
    assert!(!expected.contains_key(&6));
    assert!(actual.contains_key(&9) && actual.get(&9).unwrap().is_empty());
    assert!(!expected.contains_key(&9));

    // Both production insertions require checked, nonempty StreamData frames;
    // the ACK helper creates only positive-length, extent-accounted fragments.
    // The inconsistent byte count, oversized receipt, zero extent, and empty
    // vector above can therefore arise only in a deliberately malformed fixture.
}

fn synthetic_ack_ledger(
    records: usize,
    now: Instant,
) -> (BTreeMap<u64, Vec<CarrierPathFlight>>, u64) {
    let mut flights = BTreeMap::new();
    for index in 0..records {
        let start = index as u64 * 4;
        let kind = if index.is_multiple_of(2) {
            CarrierWorkKind::OriginalData
        } else {
            CarrierWorkKind::ReinjectedData
        };
        flights.insert(
            start,
            vec![CarrierPathFlight::fixed_output(
                key(
                    if index.is_multiple_of(2) {
                        UnderlayProtocol::Tcp
                    } else {
                        UnderlayProtocol::Udp
                    },
                    (index % u16::MAX as usize) as u16,
                ),
                start + 2,
                2,
                now,
                kind,
                (kind == CarrierWorkKind::ReinjectedData).then_some(Duration::from_secs(1)),
            )],
        );
    }
    let end = records.saturating_sub(1) as u64 * 4 + if records == 0 { 0 } else { 2 };
    (flights, end)
}

type AckReleaseFn = fn(
    &mut BTreeMap<u64, Vec<CarrierPathFlight>>,
    &[OffsetRange],
) -> Vec<(u64, CarrierPathReleasedFlight)>;

fn time_ack_release_batch(
    ledgers: &mut [BTreeMap<u64, Vec<CarrierPathFlight>>],
    ranges: &[OffsetRange],
    repeats: usize,
    release: AckReleaseFn,
) -> Duration {
    let started = Instant::now();
    let mut released_count = 0usize;
    for flights in ledgers {
        for _ in 0..repeats {
            released_count =
                released_count.saturating_add(black_box(release(flights, ranges)).len());
        }
    }
    black_box(released_count);
    started.elapsed()
}

#[test]
#[ignore = "bounded local ACK-helper comparison; run manually after correctness verification"]
fn ack_release_large_ledger_local_cost() {
    const SIZES: [usize; 3] = [1, 32, 1024];
    const TIMED_ROUNDS: usize = 3;
    const MAX_RECORDS_PER_VARIANT_ROUND: usize = 32_768;
    const MAX_CALLS_PER_ROUND: usize = 4_096;
    let now = Instant::now();

    for records in SIZES {
        let (initial, end) = synthetic_ack_ledger(records, now);
        let cases = [
            ("duplicate/no-hit x4", vec![range(2, 3)], 4),
            ("sparse prefix", vec![range(0, 1)], 1),
            (
                "sparse interior",
                vec![range(
                    (records / 2) as u64 * 4,
                    (records / 2) as u64 * 4 + 1,
                )],
                1,
            ),
            ("full release", vec![range(0, end)], 1),
        ];
        for (name, ranges, repeats) in cases {
            assert_ack_release_matches_reference(
                initial.clone(),
                &ranges,
                &format!("cost fixture {name} with {records} synthetic records"),
            );
            let calls_per_round =
                (MAX_RECORDS_PER_VARIANT_ROUND / records).min(MAX_CALLS_PER_ROUND);
            assert!(calls_per_round * records <= MAX_RECORDS_PER_VARIANT_ROUND);
            for round in 0..TIMED_ROUNDS {
                // Prepare independent ledgers for each helper call before the
                // timer. Per variant and round, no more than 32,768 synthetic
                // flight records are resident; this is a fixture cost bound,
                // not a runtime or product threshold.
                let mut reference_ledgers = (0..calls_per_round)
                    .map(|_| initial.clone())
                    .collect::<Vec<_>>();
                let mut candidate_ledgers = (0..calls_per_round)
                    .map(|_| initial.clone())
                    .collect::<Vec<_>>();
                let (reference_elapsed, candidate_elapsed, order) = if round.is_multiple_of(2) {
                    let reference_elapsed = time_ack_release_batch(
                        &mut reference_ledgers,
                        &ranges,
                        repeats,
                        super::release_carrier_path_flight_ranges_reference,
                    );
                    let candidate_elapsed = time_ack_release_batch(
                        &mut candidate_ledgers,
                        &ranges,
                        repeats,
                        release_carrier_path_flight_ranges,
                    );
                    (
                        reference_elapsed,
                        candidate_elapsed,
                        "reference_then_candidate",
                    )
                } else {
                    let candidate_elapsed = time_ack_release_batch(
                        &mut candidate_ledgers,
                        &ranges,
                        repeats,
                        release_carrier_path_flight_ranges,
                    );
                    let reference_elapsed = time_ack_release_batch(
                        &mut reference_ledgers,
                        &ranges,
                        repeats,
                        super::release_carrier_path_flight_ranges_reference,
                    );
                    (
                        reference_elapsed,
                        candidate_elapsed,
                        "candidate_then_reference",
                    )
                };
                let helper_calls = calls_per_round * repeats;
                eprintln!(
                    "ACK fixture: records={records} case={name} round={} order={order} calls_per_variant={calls_per_round} repeats={repeats} reference_total_us={:.1} reference_us/helper={:.3} candidate_total_us={:.1} candidate_us/helper={:.3}",
                    round + 1,
                    reference_elapsed.as_secs_f64() * 1_000_000.0,
                    reference_elapsed.as_secs_f64() * 1_000_000.0 / helper_calls as f64,
                    candidate_elapsed.as_secs_f64() * 1_000_000.0,
                    candidate_elapsed.as_secs_f64() * 1_000_000.0 / helper_calls as f64,
                );
            }
        }
    }
}

#[test]
fn split_receipt_release_is_exact_and_duplicate_ack_is_idempotent() {
    let (binding, path, _receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    binding.record_original_flight(path, &stream_data_frame_at(0, 4096));

    binding.release_normalized_acked_ranges(&[range(1024, 3072)]);
    {
        let outputs = binding.outputs.lock().expect("response outputs");
        let invariant = outputs.entries[0].product_qualification.invariant();
        assert_eq!(invariant.verified_bytes, 2048);
        assert_eq!(invariant.outstanding_tag_bytes, 2048);
        assert!(invariant.holds());
    }

    binding.release_normalized_acked_ranges(&[range(1024, 3072)]);
    {
        let outputs = binding.outputs.lock().expect("response outputs");
        let invariant = outputs.entries[0].product_qualification.invariant();
        assert_eq!(invariant.verified_bytes, 2048);
        assert_eq!(invariant.outstanding_tag_bytes, 2048);
        assert!(
            invariant.holds(),
            "a replayed split receipt releases no byte twice"
        );
    }

    binding.release_normalized_acked_ranges(&[range(0, 1024), range(3072, 4096)]);
    let outputs = binding.outputs.lock().expect("response outputs");
    let invariant = outputs.entries[0].product_qualification.invariant();
    assert_eq!(invariant.verified_bytes, 4096);
    assert_eq!(invariant.outstanding_tag_bytes, 0);
    assert!(invariant.holds());
}

#[test]
fn duplicate_range_ack_is_not_path_proving_for_either_copy() {
    let original = key(UnderlayProtocol::Tcp, 0);
    let alternate = key(UnderlayProtocol::Udp, 1);
    let mut flights = BTreeMap::from([(
        0,
        vec![
            flight(original, 4096, 4096, CarrierWorkKind::OriginalData),
            flight(alternate, 4096, 4096, CarrierWorkKind::ReinjectedData),
        ],
    )]);

    let released = release_carrier_path_flight_ranges(&mut flights, &[range(0, 4096)]);

    assert_eq!(released.len(), 2);
    assert!(released.iter().all(|(_, release)| !release.path_proving));
    assert!(flights.is_empty());
}

#[test]
fn partial_ambiguous_data_ack_preserves_exact_neighboring_qualification_progress() {
    let (binding, original, _receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let duplicate = key(UnderlayProtocol::Udp, 1);
    let (commands, _duplicate_receivers) = reliable_path_command_channels(8);
    binding.attach(
        duplicate.underlay,
        duplicate.path_id,
        commands,
        TrafficClass::Throughput,
    );
    binding.record_original_flight(original, &stream_data_frame_at(0, 8192));
    binding.record_reinjected_flight(duplicate, &stream_data_frame_at(2048, 2048));

    {
        let outputs = binding.outputs.lock().expect("response outputs");
        let original = outputs
            .entries
            .iter()
            .find(|entry| entry.key == original)
            .expect("original output");
        assert_eq!(
            original
                .product_qualification
                .invariant()
                .outstanding_tag_bytes,
            6144,
            "accepted reinjection removes only its exact overlap from M"
        );
    }

    binding.release_normalized_acked_ranges(&[range(0, 8192)]);
    let outputs = binding.outputs.lock().expect("response outputs");
    let original = outputs
        .entries
        .iter()
        .find(|entry| entry.key == original)
        .expect("original output");
    let ledger = original.product_qualification.invariant();
    assert_eq!(ledger.verified_bytes, 6144);
    assert_eq!(ledger.outstanding_tag_bytes, 0);
    assert_eq!(
        original.original_data_acked_bytes, 6144,
        "unique byte attribution is shared by qualification and path progress"
    );
    assert!(ledger.holds());
}

#[test]
fn combined_ack_partial_copy_preserves_response_unique_path_progress() {
    const ORIGINAL_BYTES: usize = 65_536;
    const COPY_BYTES: usize = 14_600;
    const UNIQUE_BYTES: u64 = (ORIGINAL_BYTES - COPY_BYTES) as u64;

    // The split-ACK control reaches the same settled byte set first. Combining
    // its ACK coverage must not make the original-only suffix ambiguous.
    for split_ack in [true, false] {
        let (binding, original, _original_receivers) = binding_for_underlay(UnderlayProtocol::Udp);
        let duplicate = key(UnderlayProtocol::Tcp, 1);
        let (commands, _duplicate_receivers) = reliable_path_command_channels(8);
        binding.attach(
            duplicate.underlay,
            duplicate.path_id,
            commands,
            TrafficClass::Throughput,
        );
        let original_identity = server_output_identity(&binding, original);
        binding.record_original_flight(original, &stream_data_frame_at(0, ORIGINAL_BYTES));
        binding.record_reinjected_flight(duplicate, &stream_data_frame_at(0, COPY_BYTES));

        {
            let flights = binding.flights.lock().expect("response flights");
            let recorded = flights.get(&0).expect("original and prefix copy");
            assert_eq!(recorded.len(), 2);
            assert!(recorded.iter().any(|flight| {
                flight.key == original
                    && flight.end == ORIGINAL_BYTES as u64
                    && flight.bytes == ORIGINAL_BYTES
                    && flight.kind == CarrierWorkKind::OriginalData
                    && flight.evidence_eligible
            }));
            assert!(recorded.iter().any(|flight| {
                flight.key == duplicate
                    && flight.end == COPY_BYTES as u64
                    && flight.bytes == COPY_BYTES
                    && flight.kind == CarrierWorkKind::ReinjectedData
            }));
        }
        {
            let outputs = binding.outputs.lock().expect("response outputs");
            assert_eq!(outputs.original_data_in_flight_bytes, ORIGINAL_BYTES as u64);
            assert_eq!(
                outputs
                    .entries
                    .iter()
                    .map(|entry| entry.bytes_in_flight)
                    .sum::<u64>(),
                (ORIGINAL_BYTES + COPY_BYTES) as u64
            );
            for entry in &outputs.entries {
                assert_eq!(entry.qualification, StreamPathQualification::Qualified);
                assert!(entry.product_rate_epoch.is_none());
            }
        }

        if split_ack {
            let prefix = binding.release_normalized_acked_ranges(&[range(0, COPY_BYTES as u64)]);
            assert!(prefix.path_progress_outputs.is_empty());
        }
        let release = binding.release_normalized_acked_ranges(&[range(0, ORIGINAL_BYTES as u64)]);
        let replay = binding.release_normalized_acked_ranges(&[range(0, ORIGINAL_BYTES as u64)]);
        assert!(replay.path_progress_outputs.is_empty());
        assert!(binding.flights.lock().expect("response flights").is_empty());

        let outputs = binding.outputs.lock().expect("response outputs");
        assert_eq!(outputs.original_data_in_flight_bytes, 0);
        for entry in &outputs.entries {
            assert_eq!(entry.bytes_in_flight, 0);
            assert_eq!(entry.original_data_in_flight_bytes, 0);
            assert_eq!(entry.qualification, StreamPathQualification::Qualified);
            let ledger = entry.product_qualification.invariant();
            assert_eq!(ledger.outstanding_tag_bytes, 0);
            assert!(ledger.holds());
            if entry.key == duplicate {
                assert_eq!(ledger.verified_bytes, 0);
                assert_eq!(entry.original_data_acked_bytes, 0);
                assert_eq!(entry.delivery_samples, 0);
                assert!(entry.product_rate_epoch.is_none());
            }
        }
        let original = outputs
            .entries
            .iter()
            .find(|entry| entry.key == original)
            .expect("original output");
        assert_eq!(
            original.product_qualification.invariant().verified_bytes,
            UNIQUE_BYTES
        );
        assert_eq!(
            original.original_data_acked_bytes, UNIQUE_BYTES,
            "split_ack={split_ack}: settlement and exact qualification succeed; the 50,936 original-only bytes must also prove path progress"
        );
        assert_eq!(
            release.path_progress_outputs.as_slice(),
            &[original_identity]
        );
        assert_eq!(
            original.delivery_samples, 1,
            "partitioning a single unique suffix does not manufacture delivery confidence"
        );
    }
}

#[test]
fn exact_original_data_ack_releases_output_flight_and_progress() {
    let (binding, path, _receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let frame = stream_data_frame_at(0, 4096);
    binding.record_original_flight(path, &frame);

    binding.release_normalized_acked_ranges_at(
        &[range(0, 4096)],
        Instant::now() + Duration::from_millis(20),
    );

    let outputs = binding.outputs.lock().expect("test response outputs lock");
    let output = outputs.entries.first().expect("initial output");
    assert_eq!(output.original_data_in_flight_bytes, 0);
    assert_eq!(output.bytes_in_flight, 0);
    assert_eq!(output.original_data_acked_bytes, 4096);
    drop(outputs);
    assert!(
        binding
            .original_flight_outputs_overlapping_frame(&frame)
            .is_empty()
    );
}

#[test]
fn one_data_ack_transaction_contributes_at_most_one_sample_per_output() {
    let (binding, path, _receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    for offset in [0, 4096, 8192] {
        binding.record_original_flight(path, &stream_data_frame_at(offset, 4096));
    }

    binding.release_normalized_acked_ranges_at(
        &[range(0, 12_288)],
        Instant::now() + Duration::from_millis(20),
    );

    let outputs = binding.outputs.lock().expect("test response outputs lock");
    let output = outputs.entries.first().expect("initial output");
    assert_eq!(output.original_data_acked_bytes, 12_288);
    assert_eq!(
        output.delivery_samples, 1,
        "one Data ACK transaction may qualify at most one sample on an exact output; splitting its released ledger into records cannot fabricate confidence",
    );
}

#[test]
fn stale_response_output_recovery_preserves_exact_ack_authority() {
    let (binding, original, _receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let original_identity = server_output_identity(&binding, original);
    assert!(
        !binding.mark_output_stale(original_identity, TrafficClass::Throughput),
        "the only live output cannot be withdrawn"
    );

    let alternate = key(UnderlayProtocol::Udp, 1);
    let (commands, _alternate_receivers) = reliable_path_command_channels(8);
    binding.attach(
        alternate.underlay,
        alternate.path_id,
        commands,
        TrafficClass::Throughput,
    );
    let first = stream_data_frame_at(0, 4096);
    let second = stream_data_frame_at(4096, 4096);
    binding.record_original_flight(original, &first);
    binding.record_original_flight(original, &second);

    assert!(binding.mark_output_stale(original_identity, TrafficClass::Throughput));
    assert!(binding.output_is_stale(original_identity));
    assert_eq!(
        binding
            .stale_original_recovery_state(original_identity, TrafficClass::Throughput,)
            .uncovered_ranges,
        vec![range(0, 8192)]
    );

    binding.record_reinjected_flight(alternate, &first);
    assert_eq!(
        binding
            .stale_original_recovery_state(original_identity, TrafficClass::Throughput,)
            .uncovered_ranges,
        vec![range(4096, 8192)],
        "a live exact reinjection copy covers only its own range"
    );
    let ambiguous = binding.release_normalized_acked_ranges(&[range(0, 4096)]);
    assert!(ambiguous.path_progress_outputs.is_empty());
    assert!(
        binding.output_is_stale(original_identity),
        "ambiguous duplicate delivery cannot reactivate an output"
    );

    let exact = binding.release_normalized_acked_ranges(&[range(4096, 8192)]);
    assert!(
        exact.path_progress_outputs.is_empty(),
        "pre-stale assignment ACK releases flight but cannot restore authority"
    );
    assert!(binding.output_is_stale(original_identity));
}

#[test]
fn equal_expiry_response_candidates_preserve_one_nonstale_survivor() {
    let (binding, first, _first_receivers) = binding_for_underlay(UnderlayProtocol::Udp);
    let second = key(UnderlayProtocol::Tcp, 1);
    let (commands, _second_receivers) = reliable_path_command_channels(8);
    binding.attach(
        second.underlay,
        second.path_id,
        commands,
        TrafficClass::Throughput,
    );
    let first_identity = server_output_identity(&binding, first);
    let second_identity = server_output_identity(&binding, second);

    assert!(binding.mark_output_stale(first_identity, TrafficClass::Throughput));
    assert!(
        !binding.mark_output_stale(second_identity, TrafficClass::Throughput),
        "two candidates returned at one deadline are revalidated serially, so the last live output survives",
    );
    assert!(binding.output_is_stale(first_identity));
    assert!(!binding.output_is_stale(second_identity));
}

#[test]
fn draining_response_output_cannot_authorize_withdrawing_the_only_schedulable_owner() {
    let (binding, owner, _owner_receivers) = binding_for_underlay(UnderlayProtocol::Udp);
    let alternate = key(UnderlayProtocol::Tcp, 2);
    let (alternate_commands, _alternate_receivers) = reliable_path_command_channels(8);
    binding.attach(
        alternate.underlay,
        alternate.path_id,
        alternate_commands.clone(),
        TrafficClass::Throughput,
    );
    let owner = server_output_identity(&binding, owner);

    alternate_commands.begin_path_drain();
    assert!(
        !binding.has_nonstale_reinjection_alternative(owner, TrafficClass::Throughput),
        "an attachment whose Product admission is fenced cannot be the recovery alternative",
    );
    assert!(
        !binding.mark_output_stale(owner, TrafficClass::Throughput),
        "serial mark revalidation must preserve the only schedulable owner",
    );
}

#[test]
fn draining_response_output_is_not_a_product_recovery_target() {
    let (binding, owner, _owner_receivers) = binding_for_underlay(UnderlayProtocol::Udp);
    let alternate = key(UnderlayProtocol::Tcp, 3);
    let (alternate_commands, _alternate_receivers) = reliable_path_command_channels(8);
    binding.attach(
        alternate.underlay,
        alternate.path_id,
        alternate_commands.clone(),
        TrafficClass::Throughput,
    );
    let frame = stream_data_frame_at(0, 4096);
    binding.record_original_flight(owner, &frame);

    alternate_commands.begin_path_drain();
    assert!(
        !binding.has_multipath_reinjection_alternative(),
        "a channel retained for ordered drain is not a second Product path",
    );
    assert!(
        !binding.has_reinjection_path_for_frame(&frame),
        "a draining alternate cannot accept a new reinjection commitment",
    );
    assert!(
        !binding.has_tail_reinjection_output_for_frame(&frame),
        "a draining alternate cannot keep queued tail recovery alive",
    );
}

#[test]
fn failed_response_ranges_wait_for_a_schedulable_recovery_output() {
    let (binding, failed, _failed_receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let alternate = key(UnderlayProtocol::Udp, 4);
    let failed_path_instance_id = binding
        .outputs
        .lock()
        .expect("test response outputs lock")
        .entries
        .first()
        .expect("initial output")
        .path_instance_id;
    let (alternate_commands, _alternate_receivers) = reliable_path_command_channels(8);
    binding.attach(
        alternate.underlay,
        alternate.path_id,
        alternate_commands.clone(),
        TrafficClass::Throughput,
    );
    let failed_frame = stream_data_frame_at(0, 4096);
    binding.record_original_flight(failed, &failed_frame);

    alternate_commands.begin_path_drain();
    binding.detach_path_instance(failed, failed_path_instance_id);
    assert!(
        binding.uncovered_failed_original_ranges().is_empty(),
        "failed ownership remains retained, but recovery is not ready until a Product target exists",
    );
    assert!(
        !binding.has_untracked_data_reinjection_path_for_frame(&stream_data_frame_at(4096, 4096)),
        "an unknown-owner recovery frame cannot target the sole draining output",
    );
}

#[test]
fn committed_recovery_copy_releases_publication_at_detach_start() {
    let (binding, owner, _owner_receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let copy = key(UnderlayProtocol::Tcp, 5);
    let survivor = key(UnderlayProtocol::Tcp, 6);
    let successor = key(UnderlayProtocol::Tcp, 7);
    let stable_slot = ConfiguredMemberSlot(5);
    let (copy_commands, _copy_receivers) = reliable_path_command_channels(8);
    let (survivor_commands, _survivor_receivers) = reliable_path_command_channels(8);
    binding.attach(
        copy.underlay,
        copy.path_id,
        copy_commands.clone(),
        TrafficClass::Throughput,
    );
    binding.attach(
        survivor.underlay,
        survivor.path_id,
        survivor_commands,
        TrafficClass::Throughput,
    );
    let copy_identity = server_output_identity(&binding, copy);
    let copy_path_instance_id = binding
        .outputs
        .lock()
        .expect("response outputs")
        .entries
        .iter()
        .find(|entry| entry.key == copy)
        .expect("copy output")
        .path_instance_id;
    let frame = stream_data_frame_at(0, 4096);
    let owner_identity = server_output_identity(&binding, owner);
    binding.record_original_flight(owner, &frame);
    binding.record_reinjected_flight(copy, &frame);
    assert_eq!(
        binding.accepted_reinjected_data_in_flight_bytes_at(copy_identity),
        4096,
    );
    assert!(binding.mark_output_stale(owner_identity, TrafficClass::Throughput));

    copy_commands.begin_path_drain();
    let recovery = binding.stale_original_recovery_state(owner_identity, TrafficClass::Throughput);
    assert!(
        recovery.uncovered_ranges.is_empty(),
        "drain alone does not remove an attachment from Product scheduling membership",
    );
    assert!(
        recovery.retry_deadline.is_some(),
        "the frozen deadline schedules alternate-target reevaluation without releasing this target's K",
    );

    let copy_incarnation = match binding
        .begin_path_detach(copy, copy_path_instance_id)
        .expect("begin exact copy detach")
    {
        super::super::ResponsePathDetachOutcome::Begun(incarnation)
        | super::super::ResponsePathDetachOutcome::Pending(incarnation) => incarnation,
    };
    assert_eq!(copy_incarnation, copy_identity.incarnation);
    let (successor_commands, mut successor_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach_output(ResponseOutputAttachment {
            key: successor,
            path_instance_id: next_server_carrier_path_instance_id(),
            configured_slot: stable_slot,
            local_policy: PathPolicy::default(),
            startup_rate_prior: RateHint::Unknown,
            commands: successor_commands,
            state: ResponseOutputAttachmentState::default(),
        }),
        super::super::ResponseStreamAttachOutcome::Attached,
    );
    let successor_identity = server_output_identity(&binding, successor);
    let survivor_identity = server_output_identity(&binding, survivor);
    assert_eq!(
        binding.accepted_reinjected_data_in_flight_bytes_at(successor_identity),
        0,
        "detach start transfers publication authority; the historical physical attempt remains only in the ACK/wire ledger",
    );
    assert_eq!(
        binding
            .stale_original_recovery_state(owner_identity, TrafficClass::Throughput,)
            .uncovered_ranges,
        vec![range(0, 4096)],
        "membership removal immediately exposes the range to a current successor",
    );
    let avoided = binding.reinjection_avoid_outputs_for_frame(&frame);
    assert!(
        !avoided.contains(&(successor_identity.key, successor_identity.incarnation)),
        "Decide must not inherit historical flight ownership across membership removal",
    );
    assert!(
        !avoided.contains(&(survivor_identity.key, survivor_identity.incarnation)),
        "a distinct configured slot must remain eligible for recovery",
    );
    let successor_target: ResponseDispatchTarget = binding
        .sender_path_targets(TrafficClass::Throughput, 4096)
        .into_iter()
        .find(|target| target.observation.key == successor)
        .expect("same-slot successor target")
        .into();
    binding
        .try_enqueue_reinjected_frame_for_target(
            &successor_target,
            &frame,
            TrafficClass::Throughput,
            0,
            4096,
            None,
        )
        .expect("same-slot successor receives publication authority after detach start");
    assert!(try_recv_reliable_path_command(&mut successor_receivers).is_some());
    assert_eq!(
        binding.accepted_reinjected_data_in_flight_bytes_at(successor_identity),
        4096,
        "the committed successor copy becomes the sole current stable-slot debt",
    );
    binding.complete_path_detach(copy, copy_path_instance_id, copy_incarnation);
    assert_eq!(
        binding
            .stale_original_recovery_state(owner_identity, TrafficClass::Throughput,)
            .uncovered_ranges,
        Vec::<OffsetRange>::new(),
        "physical settlement cannot revoke the successor's current publication",
    );
}

#[test]
fn configured_slot_reinjection_debt_is_an_interval_union_across_physical_attempts() {
    let (binding, _owner, _owner_receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let first = key(UnderlayProtocol::Tcp, 5);
    let second = key(UnderlayProtocol::Tcp, 7);
    let successor = key(UnderlayProtocol::Tcp, 9);
    let stable_slot = ConfiguredMemberSlot(41);
    let (first_commands, _first_receivers) = reliable_path_command_channels(8);
    let (second_commands, _second_receivers) = reliable_path_command_channels(8);
    let (successor_commands, mut successor_receivers) = reliable_path_command_channels(8);
    for (path, commands) in [
        (first, first_commands.clone()),
        (second, second_commands.clone()),
        (successor, successor_commands),
    ] {
        assert_eq!(
            binding.attach_output(ResponseOutputAttachment {
                key: path,
                path_instance_id: next_server_carrier_path_instance_id(),
                configured_slot: stable_slot,
                local_policy: PathPolicy::default(),
                startup_rate_prior: RateHint::Unknown,
                commands,
                state: ResponseOutputAttachmentState::default(),
            }),
            super::super::ResponseStreamAttachOutcome::Attached,
        );
    }
    let second_identity = server_output_identity(&binding, second);
    let successor_identity = server_output_identity(&binding, successor);
    let (first_instance, second_instance) = {
        let outputs = binding.outputs.lock().expect("response outputs");
        let instance = |key| {
            outputs
                .entries
                .iter()
                .find(|entry| entry.key == key)
                .expect("same-slot output")
                .path_instance_id
        };
        (instance(first), instance(second))
    };
    binding.record_reinjected_flight(first, &stream_data_frame_at(0, 4096));
    binding.record_reinjected_flight(second, &stream_data_frame_at(2048, 4096));
    assert_eq!(
        binding.accepted_reinjected_data_in_flight_bytes_at(second_identity),
        6144,
        "overlapping physical attempts in one stable slot consume one Product interval union",
    );
    let blocked_frame = stream_data_frame_at(1024, 1024);
    let successor_target: ResponseDispatchTarget = binding
        .sender_path_targets(TrafficClass::Throughput, 1024)
        .into_iter()
        .find(|target| target.observation.key == successor)
        .expect("current same-slot successor")
        .into();
    assert!(matches!(
        binding.try_enqueue_reinjected_frame_for_target(
            &successor_target,
            &blocked_frame,
            TrafficClass::Throughput,
            0,
            1024,
            None,
        ),
        Err(RuntimeError::SenderServiceBlocked),
    ));
    assert!(try_recv_reliable_path_command(&mut successor_receivers).is_none());

    binding.release_normalized_acked_ranges(&[range(0, 2048)]);
    assert_eq!(
        binding.accepted_reinjected_data_in_flight_bytes_at(second_identity),
        4096,
        "Product DataACK clips the stable-slot interval union exactly",
    );

    binding
        .begin_path_detach(first, first_instance)
        .expect("first current publication leaves membership");
    assert_eq!(
        binding.accepted_reinjected_data_in_flight_bytes_at(second_identity),
        4096,
        "one detached attempt cannot release overlap still owned by another current attempt",
    );
    binding
        .begin_path_detach(second, second_instance)
        .expect("second current publication leaves membership");
    assert_eq!(
        binding.accepted_reinjected_data_in_flight_bytes_at(successor_identity),
        0,
        "the stable slot is vacant once every overlapping publication owner leaves current membership",
    );
}

#[test]
fn committed_response_copy_deadline_is_not_recomputed_from_later_path_timing() {
    let fixture = native_response_binding_fixture(8, Some(120_000_000));
    let binding = fixture.binding.clone();
    let copy = fixture.key;
    let owner = key(UnderlayProtocol::Tcp, 61);
    let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
    binding.attach(
        owner.underlay,
        owner.path_id,
        owner_commands,
        TrafficClass::Throughput,
    );
    let owner_identity = server_output_identity(&binding, owner);
    let frame = stream_data_frame_at(0, 4096);
    binding.record_original_flight(owner, &frame);
    assert!(binding.mark_output_stale(owner_identity, TrafficClass::Throughput));
    let selected = binding
        .sender_path_targets(TrafficClass::Throughput, 4096)
        .into_iter()
        .find(|target| target.observation.key == copy)
        .expect("exact recovery target");
    let committed_target_interval = crate::model::timing::reliable_data_retransmission_interval(
        Some(copy.underlay),
        Some(selected.observation.snapshot),
    );
    let owner_interval_before_commit = crate::model::timing::reliable_data_retransmission_interval(
        Some(owner.underlay),
        binding.response_output_snapshot(owner_identity, TrafficClass::Throughput),
    );
    assert_ne!(
        owner_interval_before_commit, committed_target_interval,
        "the fixture must distinguish the stale owner clock from the selected-copy clock",
    );
    let target = ResponseDispatchTarget::from(&selected);
    let accepted_before = Instant::now();
    let committed_deadline = binding
        .try_enqueue_reinjected_frame_for_target(
            &target,
            &frame,
            TrafficClass::Throughput,
            0,
            4096,
            None,
        )
        .expect("actual carrier command commitment");
    let accepted_after = Instant::now();
    assert!(
        committed_deadline >= accepted_before + committed_target_interval
            && committed_deadline <= accepted_after + committed_target_interval,
        "the accepted deadline must use the selected exact carrier snapshot at commitment",
    );

    let committed = binding.stale_original_recovery_state(owner_identity, TrafficClass::Throughput);
    assert_eq!(committed.retry_deadline, Some(committed_deadline));
    let accepted_at = committed_deadline
        .checked_sub(committed_target_interval)
        .expect("committed deadline retains its accepted-copy epoch");
    binding.set_output_product_model_for_test(owner, 200_000_000.0, 5_000.0);
    let later_owner_interval = crate::model::timing::reliable_data_retransmission_interval(
        Some(owner.underlay),
        binding.response_output_snapshot(owner_identity, TrafficClass::Throughput),
    );
    assert_ne!(
        later_owner_interval, owner_interval_before_commit,
        "the stale owner's actual timing model must change",
    );
    assert_ne!(
        Some(accepted_at + later_owner_interval),
        committed.retry_deadline,
        "the legacy accepted-copy epoch plus current stale-owner interval would move away from the committed selected-copy deadline",
    );
    assert_eq!(
        binding
            .stale_original_recovery_state(owner_identity, TrafficClass::Throughput)
            .retry_deadline,
        committed.retry_deadline,
        "later stale-owner timing cannot move an accepted copy's absolute deadline",
    );
    let later_shape = fixture
        .authority
        .refresh_scheduling_shape_for_test(
            fixture.scope,
            1,
            7,
            Some(120_000_000),
            Duration::from_secs(5),
            Duration::from_secs(1),
            2 * 1024 * 1024,
            256 * 1024,
            1_400,
            Some(100_000_000),
            false,
        )
        .expect("refresh the selected carrier's current Native timing");
    assert!(binding.install_native_scheduling_shape_for_instance(
        copy,
        fixture.scope.carrier_instance_id(),
        later_shape,
    ));
    let later_snapshot = binding
        .sender_path_targets(TrafficClass::Throughput, 4096)
        .into_iter()
        .find(|candidate| candidate.observation.key == copy)
        .expect("mutated exact recovery target")
        .observation
        .snapshot;
    let later_dynamic_interval = crate::model::timing::reliable_data_retransmission_interval(
        Some(copy.underlay),
        Some(later_snapshot),
    );
    assert!(
        later_dynamic_interval > committed_target_interval,
        "the selected carrier's actual RTT/model must change enough to expose dynamic recomputation",
    );
    let after_timing_growth =
        binding.stale_original_recovery_state(owner_identity, TrafficClass::Throughput);
    assert_eq!(
        after_timing_growth.retry_deadline, committed.retry_deadline,
        "later RTT/jitter/model growth cannot postpone an accepted copy's absolute deadline",
    );
}

#[test]
fn draining_stale_owner_transfers_recovery_at_detach_start() {
    let (binding, owner, _owner_receivers) = binding_for_underlay(UnderlayProtocol::Udp);
    let survivor = key(UnderlayProtocol::Tcp, 7);
    let (survivor_commands, _survivor_receivers) = reliable_path_command_channels(8);
    binding.attach(
        survivor.underlay,
        survivor.path_id,
        survivor_commands,
        TrafficClass::Throughput,
    );
    let owner_identity = server_output_identity(&binding, owner);
    let (owner_commands, owner_path_instance_id) = {
        let outputs = binding.outputs.lock().expect("response outputs");
        let owner_entry = outputs
            .entries
            .iter()
            .find(|entry| entry.key == owner)
            .expect("owner output");
        (owner_entry.commands.clone(), owner_entry.path_instance_id)
    };
    let frame = stream_data_frame_at(0, 4096);
    binding.record_original_flight(owner, &frame);
    assert!(binding.mark_output_stale(owner_identity, TrafficClass::Throughput));

    owner_commands.begin_path_drain();
    assert_eq!(
        binding.stale_original_outputs(TrafficClass::Throughput),
        vec![owner_identity],
        "Product drain already withdraws placement but must not suspend retained-range recovery",
    );
    assert_eq!(
        binding
            .stale_original_recovery_state(owner_identity, TrafficClass::Throughput,)
            .uncovered_ranges,
        vec![range(0, 4096)],
    );

    let output_incarnation = match binding
        .begin_path_detach(owner, owner_path_instance_id)
        .expect("begin exact owner detach")
    {
        super::super::ResponsePathDetachOutcome::Begun(incarnation)
        | super::super::ResponsePathDetachOutcome::Pending(incarnation) => incarnation,
    };
    assert_eq!(
        binding
            .stale_original_recovery_state(owner_identity, TrafficClass::Throughput,)
            .uncovered_ranges,
        Vec::<OffsetRange>::new(),
        "detach start withdraws the exact original from current publication membership",
    );
    assert_eq!(
        binding.uncovered_failed_original_ranges(),
        vec![range(0, 4096)],
        "the same exact flight transfers to failed-owner recovery at membership removal",
    );

    binding.complete_path_detach(owner, owner_path_instance_id, output_incarnation);
    assert!(
        binding
            .stale_original_recovery_state(owner_identity, TrafficClass::Throughput,)
            .uncovered_ranges
            .is_empty(),
        "physical settlement cannot restore withdrawn publication membership",
    );
    assert_eq!(
        binding.uncovered_failed_original_ranges(),
        vec![range(0, 4096)],
        "physical settlement cannot duplicate or revoke transferred recovery debt",
    );
}

#[test]
fn failed_owner_releases_committed_copy_at_detach_start() {
    let (binding, failed, _failed_receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let copy = key(UnderlayProtocol::Udp, 8);
    let survivor = key(UnderlayProtocol::Tcp, 9);
    let (copy_commands, _copy_receivers) = reliable_path_command_channels(8);
    let (survivor_commands, _survivor_receivers) = reliable_path_command_channels(8);
    binding.attach(
        copy.underlay,
        copy.path_id,
        copy_commands.clone(),
        TrafficClass::Throughput,
    );
    binding.attach(
        survivor.underlay,
        survivor.path_id,
        survivor_commands,
        TrafficClass::Throughput,
    );
    let (failed_path_instance_id, copy_path_instance_id, copy_incarnation) = {
        let outputs = binding.outputs.lock().expect("response outputs");
        let failed_entry = outputs
            .entries
            .iter()
            .find(|entry| entry.key == failed)
            .expect("failed owner");
        let copy_entry = outputs
            .entries
            .iter()
            .find(|entry| entry.key == copy)
            .expect("copy output");
        (
            failed_entry.path_instance_id,
            copy_entry.path_instance_id,
            copy_entry.incarnation,
        )
    };
    let frame = stream_data_frame_at(0, 4096);
    binding.record_original_flight(failed, &frame);
    binding.record_reinjected_flight(copy, &frame);
    binding.detach_path_instance(failed, failed_path_instance_id);

    copy_commands.begin_path_drain();
    assert!(
        binding.uncovered_failed_original_ranges().is_empty(),
        "the exact committed copy still owns native recovery while a distinct target remains",
    );
    let begun_incarnation = match binding
        .begin_path_detach(copy, copy_path_instance_id)
        .expect("begin copy detach")
    {
        super::super::ResponsePathDetachOutcome::Begun(incarnation)
        | super::super::ResponsePathDetachOutcome::Pending(incarnation) => incarnation,
    };
    assert_eq!(begun_incarnation, copy_incarnation);
    assert_eq!(
        binding.uncovered_failed_original_ranges(),
        vec![range(0, 4096)],
        "detach start withdraws the committed copy's publication ownership",
    );
    binding.complete_path_detach(copy, copy_path_instance_id, copy_incarnation);
    assert_eq!(
        binding.uncovered_failed_original_ranges(),
        vec![range(0, 4096)],
        "physical settlement cannot duplicate or revoke transferred recovery debt",
    );
}

#[test]
fn pre_stale_acked_hole_cannot_rebuild_post_requalification_confidence() {
    let (binding, candidate, _candidate_receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let healthy = key(UnderlayProtocol::Udp, 9);
    let (healthy_commands, _healthy_receivers) = reliable_path_command_channels(8);
    binding.attach(
        healthy.underlay,
        healthy.path_id,
        healthy_commands,
        TrafficClass::Throughput,
    );
    let identity = server_output_identity(&binding, candidate);

    binding.record_original_flight(healthy, &stream_data_frame_at(0, 4096));
    binding.record_original_flight(candidate, &stream_data_frame_at(4096, 4096));
    binding.release_normalized_acked_ranges(&[range(4096, 8192)]);
    assert!(binding.mark_output_stale(identity, TrafficClass::Throughput));
    let acquired_at = Instant::now();
    {
        let mut outputs = binding.outputs.lock().expect("response outputs");
        let entry = outputs
            .entries
            .iter_mut()
            .find(|entry| entry.key == candidate)
            .expect("candidate output");
        assert_eq!(
            entry.product_qualification.reactivate_without_evidence(),
            Ok(true)
        );
        entry.qualification = StreamPathQualification::Acquiring {
            started_at: acquired_at,
        };
    }
    binding.record_original_flight(candidate, &stream_data_frame_at(8192, 4096));
    binding.release_normalized_acked_ranges(&[range(8192, 12288)]);
    assert_eq!(
        binding
            .outputs
            .lock()
            .expect("response outputs")
            .entries
            .iter()
            .find(|entry| entry.key == candidate)
            .expect("candidate output")
            .qualification,
        StreamPathQualification::Qualified
    );

    binding.release_normalized_acked_ranges(&[range(0, 12288)]);
    let outputs = binding.outputs.lock().expect("response outputs");
    let candidate = outputs
        .entries
        .iter()
        .find(|entry| entry.key == candidate)
        .expect("candidate output");
    assert_eq!(
        candidate.delivery_samples, 1,
        "only the fresh post-probe hole may establish current delivery confidence"
    );
}

#[test]
fn data_ack_recovery_candidate_uses_the_blocking_original_flight_identity() {
    let (binding, path, _receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let before = Instant::now();
    binding.record_original_flight(path, &stream_data_frame_at(4096, 4096));
    let after = Instant::now();

    let candidate = binding
        .data_ack_recovery_candidate(4096)
        .expect("blocking original flight");
    assert_eq!(candidate.start, 4096);
    assert_eq!(candidate.end, 8192);
    assert_eq!(candidate.key, path);
    assert!(candidate.sent_at >= before && candidate.sent_at <= after);
    assert_eq!(binding.data_ack_recovery_candidate(8192), None);
}

#[test]
fn scoped_response_gap_does_not_withdraw_an_earlier_unknown_owner() {
    let (binding, unknown_owner, _owner_receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let gap_owner = key(UnderlayProtocol::Udp, 1);
    let (commands, _gap_receivers) = reliable_path_command_channels(8);
    binding.attach(
        gap_owner.underlay,
        gap_owner.path_id,
        commands,
        TrafficClass::Throughput,
    );
    binding.record_original_flight(unknown_owner, &stream_data_frame_at(0, 4096));
    binding.record_original_flight(gap_owner, &stream_data_frame_at(4096, 4096));
    let candidates =
        binding.data_ack_recovery_candidates(&[range(5000, 6000)], TrafficClass::Throughput);
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].key, gap_owner);
    assert_eq!((candidates[0].start, candidates[0].end), (5000, 6000));
}

#[test]
fn ambiguous_prefix_ack_cannot_make_a_fresh_tail_a_staleness_candidate() {
    let (binding, owner, _owner_receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let duplicate = key(UnderlayProtocol::Udp, 1);
    let (commands, _duplicate_receivers) = reliable_path_command_channels(8);
    binding.attach(
        duplicate.underlay,
        duplicate.path_id,
        commands,
        TrafficClass::Throughput,
    );
    let owner_identity = output_identity(&binding, owner);
    binding.record_original_flight(owner, &stream_data_frame_at(0, 4096));
    {
        let outputs = binding.outputs.lock().expect("response outputs");
        assert_eq!(
            outputs.entries[0]
                .product_qualification
                .invariant()
                .outstanding_tag_bytes,
            4096
        );
    }
    binding.record_reinjected_flight(duplicate, &stream_data_frame_at(0, 4096));
    {
        let outputs = binding.outputs.lock().expect("response outputs");
        let owner = outputs
            .entries
            .iter()
            .find(|entry| entry.key == owner)
            .expect("original owner");
        let ledger = owner.product_qualification.invariant();
        assert_eq!(ledger.verified_bytes, 0);
        assert_eq!(ledger.outstanding_tag_bytes, 0);
        assert!(ledger.holds());
    }
    binding.record_original_flight(owner, &stream_data_frame_at(4096, 4096));

    let release = binding.release_normalized_acked_ranges(&[range(0, 4096)]);
    assert!(
        release.path_progress_outputs.is_empty(),
        "delivery of an overlapping original and reinjection has no exact owner attribution",
    );
    {
        let outputs = binding.outputs.lock().expect("response outputs");
        let owner = outputs
            .entries
            .iter()
            .find(|entry| entry.key == owner)
            .expect("original owner");
        let ledger = owner.product_qualification.invariant();
        assert_eq!(ledger.verified_bytes, 0);
        assert_eq!(ledger.outstanding_tag_bytes, 4096);
        assert!(ledger.holds(), "ambiguous ACK cannot advance V");
    }
    assert!(
        binding
            .data_ack_recovery_candidates(&[range(0, 4096)], TrafficClass::Throughput)
            .is_empty(),
        "a fresh tail beginning at the complete ACK horizon is not an authoritative omission",
    );
    let candidates =
        binding.data_ack_recovery_candidates(&[range(4096, 8192)], TrafficClass::Throughput);
    assert_eq!(
        candidates
            .iter()
            .map(|candidate| (candidate.key, candidate.output_incarnation))
            .collect::<Vec<_>>(),
        vec![owner_identity],
        "the same retained tail becomes eligible only when a later complete horizon covers it",
    );
    assert_eq!((candidates[0].start, candidates[0].end), (4096, 8192));
}

#[test]
fn data_ack_recovery_candidates_exclude_nonlive_and_stale_output_incarnations() {
    let (binding, live, _live_receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let stale = key(UnderlayProtocol::Udp, 1);
    let detaching = key(UnderlayProtocol::Tcp, 2);
    let replaced = key(UnderlayProtocol::Udp, 3);

    let (stale_commands, _stale_receivers) = reliable_path_command_channels(8);
    binding.attach(
        stale.underlay,
        stale.path_id,
        stale_commands,
        TrafficClass::Throughput,
    );
    let (detaching_commands, _detaching_receivers) = reliable_path_command_channels(8);
    binding.attach(
        detaching.underlay,
        detaching.path_id,
        detaching_commands,
        TrafficClass::Throughput,
    );
    let (replaced_commands, replaced_receivers) = reliable_path_command_channels(8);
    binding.attach(
        replaced.underlay,
        replaced.path_id,
        replaced_commands,
        TrafficClass::Throughput,
    );

    binding.record_original_flight(live, &stream_data_frame_at(0, 4096));
    binding.record_original_flight(stale, &stream_data_frame_at(4096, 4096));
    binding.record_original_flight(detaching, &stream_data_frame_at(8192, 4096));
    binding.record_original_flight(replaced, &stream_data_frame_at(12288, 4096));

    assert!(binding.mark_output_stale(
        server_output_identity(&binding, stale),
        TrafficClass::Throughput,
    ));
    let detaching_path_instance = binding
        .outputs
        .lock()
        .expect("test response outputs lock")
        .entries
        .iter()
        .find(|entry| entry.key == detaching)
        .expect("detaching output")
        .path_instance_id;
    assert!(
        binding
            .begin_path_detach(detaching, detaching_path_instance)
            .is_some()
    );

    drop(replaced_receivers);
    let closed_candidates =
        binding.data_ack_recovery_candidates(&[range(0, u64::MAX)], TrafficClass::Throughput);
    assert_eq!(closed_candidates.len(), 1);
    assert_eq!(closed_candidates[0].key, live);

    let (replacement_commands, _replacement_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            replaced.underlay,
            replaced.path_id,
            replacement_commands,
            TrafficClass::Throughput,
        ),
        super::super::attachment::ResponseStreamAttachOutcome::ReplacedClosedOutput
    );
    let candidates =
        binding.data_ack_recovery_candidates(&[range(0, u64::MAX)], TrafficClass::Throughput);
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].key, live);
}

#[test]
fn out_of_order_data_ack_exposes_exact_lower_path_debt() {
    let (binding, path, _receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    binding.record_original_flight(path, &stream_data_frame_at(4096, 4096));

    binding.release_normalized_acked_ranges(&[range(4096, 8192)]);

    let debt = binding.lower_flights_before_offset(8192);
    assert_eq!(debt.len(), 1);
    assert_eq!(debt[0].key, path);
    assert_eq!(debt[0].bytes, 4096);
}

#[test]
fn lower_path_debt_merges_unacknowledged_and_out_of_order_acked_ranges() {
    let (binding, first, _receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let second = key(UnderlayProtocol::Udp, 7);
    let (commands, _second_receivers) = reliable_path_command_channels(8);
    binding.attach(
        second.underlay,
        second.path_id,
        commands,
        TrafficClass::Throughput,
    );
    binding.record_original_flight(first, &stream_data_frame_at(0, 4096));
    binding.record_original_flight(second, &stream_data_frame_at(4096, 4096));
    binding.release_normalized_acked_ranges(&[range(4096, 8192)]);

    let debt = binding.lower_flights_before_offset(8192);
    assert_eq!(debt.len(), 2);
    assert_eq!(debt[0].key, first);
    assert_eq!(debt[0].bytes, 4096);
    assert_eq!(debt[1].key, second);
    assert_eq!(debt[1].bytes, 4096);
}

#[test]
fn lower_debt_projection_matches_ordered_reference_for_owner_and_targets() {
    let (binding, first, _first_receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let second = key(UnderlayProtocol::Udp, 7);
    let third = key(UnderlayProtocol::Tcp, 9);
    let (second_commands, _second_receivers) = reliable_path_command_channels(8);
    binding.attach(
        second.underlay,
        second.path_id,
        second_commands,
        TrafficClass::Throughput,
    );
    let (third_commands, _third_receivers) = reliable_path_command_channels(8);
    binding.attach(
        third.underlay,
        third.path_id,
        third_commands,
        TrafficClass::Throughput,
    );

    let targets = binding.sender_path_targets(TrafficClass::Throughput, 4096);
    let target_id = |key| {
        targets
            .iter()
            .find(|target| target.observation.key == key)
            .map(|target| (target.observation.key, target.observation.incarnation))
            .expect("attached test target")
    };
    let (first, first_incarnation) = target_id(first);
    let (second, second_incarnation) = target_id(second);
    let (third, third_incarnation) = target_id(third);

    let empty = binding.project_lower_debt_before_offset(32, targets.clone());
    assert_eq!(empty.oldest_owner(), None);
    assert!(!empty.has_materialized_target_debts());
    assert_eq!(empty.exact_other_path_debt_bytes(0), Some(0));
    assert_eq!(
        empty.exact_other_path_debt_bytes(targets.len()),
        None,
        "an empty all-zero proof still rejects an out-of-range target",
    );

    let mut first_original = flight(first, 1, 11, CarrierWorkKind::OriginalData);
    first_original.output_incarnation = first_incarnation;
    let mut latest_original = flight(second, 1, 0, CarrierWorkKind::OriginalData);
    latest_original.output_incarnation = second_incarnation;
    let mut trailing_reinjection = flight(first, 1, 99, CarrierWorkKind::ReinjectedData);
    trailing_reinjection.output_incarnation = first_incarnation;
    let mut same_key_replacement = flight(first, 10, 5, CarrierWorkKind::OriginalData);
    same_key_replacement.output_incarnation = first_incarnation.wrapping_add(100);
    let mut tied_flight = flight(first, 11, 7, CarrierWorkKind::OriginalData);
    tied_flight.output_incarnation = first_incarnation;
    let mut reinjection_only = flight(third, 17, 17, CarrierWorkKind::ReinjectedData);
    reinjection_only.output_incarnation = third_incarnation;
    binding
        .flights
        .lock()
        .expect("test flights")
        .extend_for_test([
            (
                0,
                vec![first_original, latest_original, trailing_reinjection],
            ),
            (8, vec![same_key_replacement]),
            (10, vec![tied_flight]),
            (16, vec![reinjection_only]),
        ]);

    let tied_ack = CarrierPathAckedHole {
        key: third,
        output_incarnation: third_incarnation,
        end: 11,
        bytes: 2,
        sent_at: Instant::now(),
        kind: CarrierWorkKind::OriginalData,
        path_proving: true,
    };
    let latest_ack_original = CarrierPathAckedHole {
        key: second,
        output_incarnation: second_incarnation,
        end: 13,
        bytes: 4,
        sent_at: Instant::now(),
        kind: CarrierWorkKind::OriginalData,
        path_proving: true,
    };
    let trailing_ack_reinjection = CarrierPathAckedHole {
        key: first,
        output_incarnation: first_incarnation,
        end: 13,
        bytes: 100,
        sent_at: Instant::now(),
        kind: CarrierWorkKind::ReinjectedData,
        path_proving: false,
    };
    let excluded_at_boundary = CarrierPathAckedHole {
        key: third,
        output_incarnation: third_incarnation,
        end: 33,
        bytes: 33,
        sent_at: Instant::now(),
        kind: CarrierWorkKind::OriginalData,
        path_proving: true,
    };
    binding
        .ack_ordering
        .lock()
        .expect("test ACK ordering")
        .acked_holes
        .extend([
            (10, vec![tied_ack]),
            (
                12,
                vec![
                    latest_ack_original,
                    trailing_ack_reinjection,
                    CarrierPathAckedHole {
                        key: third,
                        output_incarnation: third_incarnation,
                        end: 13,
                        bytes: 8,
                        sent_at: Instant::now(),
                        kind: CarrierWorkKind::OriginalData,
                        path_proving: true,
                    },
                ],
            ),
            (32, vec![excluded_at_boundary]),
        ]);

    for offset in [0, 1, 8, 9, 10, 11, 12, 13, 32, 33] {
        let reference = binding.lower_flights_before_offset(offset);
        let projection = binding.project_lower_debt_before_offset(offset, targets.clone());
        assert_eq!(
            projection.oldest_owner(),
            response_oldest_lower_flight_owner(&reference),
            "oldest owner at strict offset {offset}",
        );
        for (target_index, target) in projection.targets().iter().enumerate() {
            assert_eq!(
                projection
                    .exact_other_path_debt_bytes(target_index)
                    .unwrap(),
                response_ordering_debt_bytes(
                    &reference,
                    target.observation.key,
                    target.observation.incarnation,
                ),
                "exact excluded sum at strict offset {offset} for {:?}",
                target.observation.key,
            );
        }
        assert_eq!(projection.targets().len(), targets.len());
        assert_eq!(
            projection.exact_other_path_debt_bytes(targets.len()),
            None,
            "out-of-range target index fails closed at strict offset {offset}",
        );
    }

    let reference = binding.lower_flights_before_offset(32);
    assert_eq!(reference.len(), 4, "reinjection-only start was omitted");
    assert_eq!(reference[0].key, second);
    assert_eq!(reference[0].bytes, 0, "zero-byte oldest owner is retained");
    assert_eq!(reference[1].key, first);
    assert_eq!(
        reference[1].output_incarnation,
        first_incarnation.wrapping_add(100)
    );
    assert_eq!(
        (reference[1].bytes, reference[2].key, reference[2].bytes),
        (5, third, 2)
    );
    assert_eq!((reference[3].key, reference[3].bytes), (third, 8));
}

/// Offline release-musl fixture for the whole debt query plus its exact
/// per-target consumption. Run alone with `--ignored --test-threads=1`; input
/// construction and the matching target snapshots are outside both timers.
#[test]
#[ignore = "manual counterbalanced release-musl response-debt cost fixture"]
fn response_debt_projection_release_cost_fixture() {
    use std::hint::black_box;
    use std::time::Instant;

    fn fixture_binding(
        target_count: usize,
        range_count: usize,
    ) -> (
        std::sync::Arc<ResponseStreamBinding>,
        Vec<super::super::ResponseSenderPathTarget>,
        u64,
    ) {
        let (binding, _first, first_receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
        let mut _receivers = vec![first_receivers];
        for index in 1..target_count {
            let underlay = if index % 2 == 0 {
                UnderlayProtocol::Tcp
            } else {
                UnderlayProtocol::Udp
            };
            let path_id = PathId(index as u16);
            let (commands, receivers) = reliable_path_command_channels(8);
            binding.attach(underlay, path_id, commands, TrafficClass::Throughput);
            _receivers.push(receivers);
        }

        let targets = binding.sender_path_targets(TrafficClass::Throughput, 64 * 1024);
        assert_eq!(targets.len(), target_count);
        // Equal, adjacent 1 KiB DSN ranges keep the synthetic ledgers
        // disjoint and bounded; record count, not payload size, is varied.
        const RANGE_BYTES: u64 = 1024;
        let end_offset = (range_count as u64)
            .saturating_mul(RANGE_BYTES)
            .saturating_add(1);
        {
            let mut flights = binding.flights.lock().expect("fixture flights");
            let mut ack_ordering = binding.ack_ordering.lock().expect("fixture ACK ordering");
            for index in 0..range_count {
                let target = &targets[(index.wrapping_mul(7)) % target_count];
                let key = target.observation.key;
                let incarnation = target.observation.incarnation;
                let start = (index as u64) * RANGE_BYTES;
                let bytes = RANGE_BYTES;
                if index % 2 == 0 {
                    let mut original = flight(
                        key,
                        start + bytes,
                        bytes as usize,
                        CarrierWorkKind::OriginalData,
                    );
                    original.output_incarnation = incarnation;
                    flights.publish(start, original);
                } else {
                    ack_ordering.acked_holes.insert(
                        start,
                        vec![CarrierPathAckedHole {
                            key,
                            output_incarnation: incarnation,
                            end: start + bytes,
                            bytes,
                            sent_at: Instant::now(),
                            kind: CarrierWorkKind::OriginalData,
                            path_proving: true,
                        }],
                    );
                }
            }
        }
        (binding, targets, end_offset)
    }

    fn consume_reference(
        debts: &[CarrierPathFlightDebt],
        targets: &[super::super::ResponseSenderPathTarget],
        consumed_targets: usize,
    ) -> u64 {
        targets
            .iter()
            .take(consumed_targets)
            .fold(0u64, |value, target| {
                value
                    .wrapping_mul(37)
                    .wrapping_add(response_ordering_debt_bytes(
                        debts,
                        target.observation.key,
                        target.observation.incarnation,
                    ))
            })
    }

    fn consume_projection(
        projection: &super::super::ResponseDebtProjection,
        consumed_targets: usize,
    ) -> u64 {
        projection
            .targets()
            .iter()
            .take(consumed_targets)
            .enumerate()
            .fold(0u64, |value, (index, _)| {
                value.wrapping_mul(37).wrapping_add(
                    projection
                        .exact_other_path_debt_bytes(index)
                        .expect("owned target summary"),
                )
            })
    }

    // Each case is run baseline/candidate, candidate/baseline, baseline/
    // candidate. Repeat counts bound total work while giving small cases enough
    // observations to rise above timer granularity.
    let sizes = [0usize, 1, 32, 1024];
    let target_counts = [1usize, 4, 32];
    let round_orders = [
        "baseline_then_fold",
        "fold_then_baseline",
        "baseline_then_fold",
    ];
    let mut fixture_cases = 0usize;
    for range_count in sizes {
        let repeats = match range_count {
            0 => 8192,
            1 => 4096,
            32 => 256,
            _ => 16,
        };
        for target_count in target_counts {
            let (binding, targets, end_offset) = fixture_binding(target_count, range_count);
            for (consume_case, consumed_targets) in
                [("one_target", 1), ("all_targets", target_count)]
            {
                fixture_cases += 1;
                let mut round_baseline_ns = [0u128; 3];
                let mut round_candidate_ns = [0u128; 3];
                let mut expected_checksum = None;
                let mut baseline_returned_capacity = None;
                let mut candidate_summary_capacity = None;
                for round in 0..3 {
                    let baseline_first = round != 1;
                    for _ in 0..repeats {
                        let run_baseline = || {
                            let owned_targets = targets.clone();
                            let started = Instant::now();
                            let debts = binding.lower_flights_before_offset(end_offset);
                            let owner = response_oldest_lower_flight_owner(&debts);
                            let checksum =
                                consume_reference(&debts, &owned_targets, consumed_targets);
                            let returned_capacity = debts.capacity();
                            black_box((owner, checksum, returned_capacity));
                            drop(debts);
                            drop(owned_targets);
                            let elapsed = started.elapsed().as_nanos();
                            (elapsed, owner, checksum, returned_capacity)
                        };
                        let mut run_candidate = || {
                            // Both arms receive the same already-constructed
                            // target snapshot. The production caller has
                            // already built it before beginning this query.
                            let owned_targets = targets.clone();
                            let started = Instant::now();
                            let projection =
                                binding.project_lower_debt_before_offset(end_offset, owned_targets);
                            let owner = projection.oldest_owner();
                            let checksum = consume_projection(&projection, consumed_targets);
                            let summary_len = if projection.has_materialized_target_debts() {
                                target_count
                            } else {
                                0
                            };
                            black_box((owner, checksum, projection.targets().len()));
                            drop(projection);
                            let elapsed = started.elapsed().as_nanos();
                            candidate_summary_capacity.get_or_insert(summary_len);
                            (elapsed, owner, checksum, summary_len)
                        };

                        let (first_run, second_run) = if baseline_first {
                            (run_baseline(), run_candidate())
                        } else {
                            (run_candidate(), run_baseline())
                        };
                        let (
                            (first_elapsed, first_owner, first_checksum, first_capacity),
                            (second_elapsed, second_owner, second_checksum, second_capacity),
                        ) = (first_run, second_run);
                        assert_eq!(first_owner, second_owner);
                        assert_eq!(first_checksum, second_checksum);
                        if let Some(expected) = expected_checksum {
                            assert_eq!(first_checksum, expected);
                        } else {
                            expected_checksum = Some(first_checksum);
                        }
                        if baseline_first {
                            round_baseline_ns[round] += first_elapsed;
                            round_candidate_ns[round] += second_elapsed;
                            baseline_returned_capacity.get_or_insert(first_capacity);
                            candidate_summary_capacity.get_or_insert(second_capacity);
                            black_box((first_capacity, second_capacity));
                        } else {
                            round_candidate_ns[round] += first_elapsed;
                            round_baseline_ns[round] += second_elapsed;
                            candidate_summary_capacity.get_or_insert(first_capacity);
                            baseline_returned_capacity.get_or_insert(second_capacity);
                            black_box((second_capacity, first_capacity));
                        }
                    }
                }

                let baseline_total_ns: u128 = round_baseline_ns.iter().sum();
                let candidate_total_ns: u128 = round_candidate_ns.iter().sum();
                let total_calls = repeats as u128 * 3;
                for round in 0..3 {
                    let baseline_ns_per_query = round_baseline_ns[round] as f64 / repeats as f64;
                    let candidate_ns_per_query = round_candidate_ns[round] as f64 / repeats as f64;
                    eprintln!(
                        "debt-fold fixture round: N={range_count} K={target_count} case={consume_case} consume={consumed_targets} round={} order={} repeats={repeats} baseline_total_ns={} fold_total_ns={} baseline_ns_per_query={baseline_ns_per_query:.1} fold_ns_per_query={candidate_ns_per_query:.1} fold_over_baseline={:.3}",
                        round + 1,
                        round_orders[round],
                        round_baseline_ns[round],
                        round_candidate_ns[round],
                        round_candidate_ns[round] as f64 / round_baseline_ns[round].max(1) as f64,
                    );
                }
                eprintln!(
                    "debt-fold fixture pooled: N={range_count} K={target_count} case={consume_case} consume={consumed_targets} rounds=3 repeats_per_round={repeats} baseline_ns_per_query={:.1} fold_ns_per_query={:.1} fold_over_baseline={:.3} reference_map_entries={range_count} baseline_returned_vec_capacity={} fold_summary_len={} checksum={}",
                    baseline_total_ns as f64 / total_calls as f64,
                    candidate_total_ns as f64 / total_calls as f64,
                    candidate_total_ns as f64 / baseline_total_ns.max(1) as f64,
                    baseline_returned_capacity.unwrap_or(0),
                    candidate_summary_capacity.unwrap_or(0),
                    expected_checksum.unwrap_or_default(),
                );
            }
        }
    }
    assert_eq!(fixture_cases, 24);
}

#[test]
fn native_suppression_deadline_only_controls_alternate_recovery_wake() {
    let path = key(UnderlayProtocol::Udp, 1);
    let mut flights = BTreeMap::from([(
        0,
        vec![flight(path, 4096, 4096, CarrierWorkKind::ReinjectedData)],
    )]);
    assert!(
        product_flights_have_recent_reinjection_overlap(
            &flights,
            0,
            4096,
            Instant::now(),
            |candidate, incarnation| candidate == path && incarnation == 0,
        )
        .is_some()
    );
    assert!(
        product_flights_have_recent_reinjection_overlap(
            &flights,
            0,
            4096,
            Instant::now(),
            |candidate, incarnation| candidate == path && incarnation == 1,
        )
        .is_none()
    );
    assert!(
        product_flights_have_recent_reinjection_overlap(
            &flights,
            0,
            4096,
            Instant::now(),
            |_, _| false,
        )
        .is_none()
    );

    flights.get_mut(&0).unwrap()[0].reinjection_suppression_deadline =
        Some(Instant::now() - Duration::from_millis(100));
    assert!(
        product_flights_have_recent_reinjection_overlap(
            &flights,
            0,
            4096,
            Instant::now(),
            |_, _| true,
        )
        .is_none()
    );
}

#[test]
fn accepted_response_reinjection_holds_exact_output_reserve_until_data_ack() {
    let (binding, original, _receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let alternate = key(UnderlayProtocol::Udp, 1);
    let (commands, _alternate_receivers) = reliable_path_command_channels(8);
    binding.attach(
        alternate.underlay,
        alternate.path_id,
        commands,
        TrafficClass::Throughput,
    );
    let frame = stream_data_frame_at(0, 4096);
    binding.record_original_flight(original, &frame);
    binding.record_reinjected_flight(alternate, &frame);
    let identity = server_output_identity(&binding, alternate);

    assert_eq!(
        binding.live_reinjected_data_in_flight_bytes_at(identity, Instant::now()),
        4096,
    );
    assert_eq!(
        binding.accepted_reinjected_data_in_flight_bytes_at(identity),
        4096,
    );

    binding.age_reinjected_flights_for_test(Duration::from_secs(2));
    assert_eq!(
        binding.live_reinjected_data_in_flight_bytes_at(identity, Instant::now()),
        0,
        "the native suppression interval can expire independently",
    );
    assert_eq!(
        binding.accepted_reinjected_data_in_flight_bytes_at(identity),
        4096,
        "timer expiry cannot mint another exact-target Product reserve",
    );

    binding.release_normalized_acked_ranges(&[range(0, 4096)]);
    assert_eq!(
        binding.accepted_reinjected_data_in_flight_bytes_at(identity),
        0,
        "Product DataACK releases the exact target reserve",
    );
}

#[test]
fn response_reinjection_revalidates_exact_k_after_carrier_reservation() {
    let (binding, output, mut receivers) = binding_for_underlay(UnderlayProtocol::Udp);
    let selected = binding
        .sender_path_targets(TrafficClass::Throughput, 1)
        .into_iter()
        .find(|target| target.observation.key == output)
        .expect("exact response target");
    let target = ResponseDispatchTarget::from(&selected);
    let product_window =
        usize::try_from(selected.observation.snapshot.data_level_limit_bytes).unwrap_or(usize::MAX);
    assert!(product_window > 0);
    binding.record_reinjected_flight(output, &stream_data_frame_at(0, product_window));

    let next = stream_data_frame_at(product_window as u64, 1);
    assert!(matches!(
        binding.try_enqueue_reinjected_frame_for_target(
            &target,
            &next,
            TrafficClass::Throughput,
            0,
            1,
            None,
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    assert!(
        try_recv_reliable_path_command(&mut receivers).is_none(),
        "an exhausted K must drop the reservation before publishing a carrier command",
    );
}

#[test]
fn udp_response_reinjection_without_native_authority_fails_closed() {
    let (binding, output, mut receivers) = binding_for_underlay(UnderlayProtocol::Udp);
    let target: ResponseDispatchTarget = binding
        .sender_path_targets(TrafficClass::Throughput, 4_096)
        .into_iter()
        .find(|target| target.observation.key == output)
        .expect("unfenced UDP is visible only to prove final rejection")
        .into();
    let frame = stream_data_frame_at(0, 4_096);

    assert!(matches!(
        binding.try_enqueue_reinjected_frame_for_target(
            &target,
            &frame,
            TrafficClass::Throughput,
            0,
            4_096,
            None,
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    assert_eq!(
        binding
            .accepted_reinjected_data_in_flight_bytes_at(server_output_identity(&binding, output,)),
        0,
    );
    assert!(binding.flight_outputs_overlapping_frame(&frame).is_empty());
    assert!(try_recv_reliable_path_command(&mut receivers).is_none());
}

#[test]
fn native_reinjection_final_precommit_rejects_stale_generation_after_real_reservation() {
    let mut fixture = native_response_binding_fixture(1, None);
    let target: ResponseDispatchTarget = fixture
        .binding
        .sender_path_targets(TrafficClass::Throughput, 4_096)
        .into_iter()
        .find(|candidate| candidate.observation.key == fixture.key)
        .expect("current fenced Native response target")
        .into();
    let c0_stamp = target
        .native_authority_stamp
        .expect("Native response target carries its exact C0 stamp");
    assert_eq!(
        fixture.authority.stamp().expect("current C0 stamp"),
        c0_stamp,
    );
    assert_eq!(
        fixture
            .authority
            .decision_snapshot(fixture.scope)
            .expect("current fenced C0 decision")
            .basis(),
        CarrierRateAuthorityBasis::StartupPrior,
    );
    let frame = stream_data_frame_at(0, 4_096);

    let result = fixture
        .binding
        .try_enqueue_reinjected_frame_for_target_with_after_reserve(
            &target,
            &frame,
            TrafficClass::Throughput,
            0,
            4_096,
            None,
            || {
                assert!(
                    fixture.commands.pending_bytes() > 0,
                    "the exact reinjection writer slot is charged before final A/G validation",
                );
                fixture
                    .authority
                    .publish_observation_for_test(1, 7, Some(120_000_000))
                    .expect("publish same-A C0 to Bop after the real reservation");
            },
        );

    assert!(matches!(result, Err(RuntimeError::SenderServiceBlocked)));
    assert_ne!(
        fixture.authority.stamp().expect("current Bop stamp"),
        c0_stamp,
        "C0 to Bop advances central G while A and I remain unchanged",
    );
    assert_eq!(
        fixture
            .authority
            .decision_snapshot(fixture.scope)
            .expect("current fenced Bop decision")
            .basis(),
        CarrierRateAuthorityBasis::NativeOperational,
    );
    assert_eq!(fixture.commands.pending_bytes(), 0);
    assert!(
        fixture
            .binding
            .flight_outputs_overlapping_frame(&frame)
            .is_empty(),
        "a stale Native G cannot publish a reinjected Product flight",
    );
    {
        let outputs = fixture
            .binding
            .outputs
            .lock()
            .expect("test response outputs lock");
        assert_eq!(outputs.original_data_in_flight_bytes, 0);
        assert_eq!(outputs.entries[0].original_data_in_flight_bytes, 0);
        assert_eq!(outputs.entries[0].bytes_in_flight, 0);
    }
    assert!(try_recv_reliable_path_command(&mut fixture.receivers).is_none());

    let reservation = fixture
        .commands
        .try_reserve_reinjection_frame(frame.clone(), TrafficClass::Throughput)
        .expect("rejected final precommit refunds the one-slot reinjection queue");
    assert!(fixture.commands.pending_bytes() > 0);
    drop(reservation);
    assert_eq!(fixture.commands.pending_bytes(), 0);
}

#[test]
fn native_reinjection_final_precommit_uses_current_same_stamp_recovery_clock() {
    let mut fixture = native_response_binding_fixture(1, Some(120_000_000));
    let target: ResponseDispatchTarget = fixture
        .binding
        .sender_path_targets(TrafficClass::Throughput, 4_096)
        .into_iter()
        .find(|candidate| candidate.observation.key == fixture.key)
        .expect("current fenced Native response target")
        .into();
    let expected_stamp = target
        .native_authority_stamp
        .expect("Native response target carries its exact stamp");
    let frame = stream_data_frame_at(0, 4_096);
    let before = Instant::now();

    let deadline = fixture
        .binding
        .try_enqueue_reinjected_frame_for_target_with_after_reserve(
            &target,
            &frame,
            TrafficClass::Throughput,
            0,
            4_096,
            None,
            || {
                let refreshed = fixture
                    .authority
                    .refresh_scheduling_shape_for_test(
                        fixture.scope,
                        1,
                        7,
                        Some(120_000_000),
                        Duration::from_secs(5),
                        Duration::from_secs(1),
                        2 * 1024 * 1024,
                        256 * 1024,
                        1_400,
                        Some(100_000_000),
                        false,
                    )
                    .expect("refresh only Quinn timing shape after reservation");
                assert_eq!(refreshed.stamp(), expected_stamp);
                assert_eq!(
                    fixture.authority.stamp().expect("unchanged central stamp"),
                    expected_stamp,
                    "same-controller timing changes do not revise central G",
                );
            },
        )
        .expect("current same-stamp shape still permits reinjection");

    assert!(
        deadline.duration_since(before) >= Duration::from_secs(8),
        "the committed repair clock must come from current 5s/1s Quinn timing",
    );
    assert!(matches!(
        try_recv_reliable_path_command(&mut fixture.receivers),
        Some(ReliablePathCommand::SendFrame(received)) if received == frame
    ));
}

#[test]
fn bound_response_reinjection_rejects_expiry_after_native_reservation() {
    let mut fixture = native_response_binding_fixture(1, None);
    let target: ResponseDispatchTarget = fixture
        .binding
        .sender_path_targets(TrafficClass::Throughput, 4_096)
        .into_iter()
        .find(|candidate| candidate.observation.key == fixture.key)
        .expect("current fenced Native response target")
        .into();
    let frame = stream_data_frame_at(0, 4_096);
    let expires_at = Instant::now() + Duration::from_millis(10);

    let result = fixture
        .binding
        .try_enqueue_reinjected_frame_for_target_with_after_reserve(
            &target,
            &frame,
            TrafficClass::Throughput,
            0,
            4_096,
            Some(expires_at),
            || {
                assert!(
                    fixture.commands.pending_bytes() > 0,
                    "the native writer slot is reserved before the expiry boundary",
                );
                std::thread::sleep(Duration::from_millis(20));
            },
        );

    assert!(matches!(result, Err(RuntimeError::SenderServiceBlocked)));
    assert_eq!(fixture.commands.pending_bytes(), 0);
    assert!(try_recv_reliable_path_command(&mut fixture.receivers).is_none());
    assert!(
        fixture
            .binding
            .flight_outputs_overlapping_frame(&frame)
            .is_empty(),
        "a bound batch that expires during reservation cannot publish Product flight state",
    );
}

#[test]
fn reinjection_does_not_replace_the_original_path_identity() {
    let (binding, original, _receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let alternate = key(UnderlayProtocol::Udp, 1);
    let (commands, _receivers) = crate::runtime::path::commands::reliable_path_command_channels(8);
    binding.attach(
        alternate.underlay,
        alternate.path_id,
        commands,
        TrafficClass::Throughput,
    );
    let original_output = output_identity(&binding, original);
    let alternate_output = output_identity(&binding, alternate);
    let frame = stream_data_frame_at(0, 4096);
    binding.record_original_flight(original, &frame);
    binding.record_reinjected_flight(alternate, &frame);

    assert_eq!(
        binding.original_flight_outputs_overlapping_frame(&frame),
        vec![original_output]
    );
    let mut all = binding.flight_outputs_overlapping_frame(&frame);
    all.sort_by_key(|(candidate, _)| candidate.path_id.0);
    assert_eq!(all, vec![original_output, alternate_output]);
}

#[test]
fn failed_output_reinjection_covers_all_interleaved_original_ranges() {
    let (binding, failed, _receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let alternate = key(UnderlayProtocol::Udp, 1);
    let failed_path_instance_id = binding
        .outputs
        .lock()
        .expect("test response outputs lock")
        .entries
        .first()
        .expect("initial output")
        .path_instance_id;
    let (alternate_commands, _alternate_receivers) = reliable_path_command_channels(8);
    binding.attach(
        alternate.underlay,
        alternate.path_id,
        alternate_commands,
        TrafficClass::Throughput,
    );

    let failed_first = stream_data_frame_at(0, 4096);
    let alternate_first = stream_data_frame_at(4096, 4096);
    let failed_second = stream_data_frame_at(8192, 4096);
    binding.record_original_flight(failed, &failed_first);
    binding.record_original_flight(alternate, &alternate_first);
    binding.record_original_flight(failed, &failed_second);
    binding.detach_path_instance(failed, failed_path_instance_id);

    assert_eq!(
        binding.uncovered_failed_original_ranges(),
        vec![range(0, 4096), range(8192, 12288)],
    );
    binding.record_reinjected_flight(alternate, &failed_first);
    assert_eq!(
        binding.uncovered_failed_original_ranges(),
        vec![range(8192, 12288)],
        "path failure must recover every remaining range owned by that output, not only the first DSN record before a live-path record"
    );
    let active_copy = binding.failed_original_recovery_state();
    assert!(active_copy.retry_deadline.is_some());
    binding.age_reinjected_flights_for_test(Duration::from_secs(2));
    assert_eq!(
        binding.failed_original_recovery_state().uncovered_ranges,
        vec![range(0, 4096), range(8192, 12288)],
        "an expired accepted copy must not cover failed-owner bytes forever",
    );
}

#[test]
fn blocking_flight_cannot_inherit_a_replacement_output_snapshot() {
    let (binding, original, original_receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let alternate = key(UnderlayProtocol::Udp, 1);
    let (alternate_commands, _alternate_receivers) = reliable_path_command_channels(8);
    binding.attach(
        alternate.underlay,
        alternate.path_id,
        alternate_commands,
        TrafficClass::Throughput,
    );
    binding.record_original_flight(original, &stream_data_frame_at(0, 4096));
    drop(original_receivers);
    let (replacement_commands, _replacement_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            original.underlay,
            original.path_id,
            replacement_commands,
            TrafficClass::Throughput,
        ),
        super::super::attachment::ResponseStreamAttachOutcome::ReplacedClosedOutput
    );

    assert!(
        binding
            .tail_reinjection_snapshot(0, TrafficClass::Throughput)
            .is_none(),
        "an old OriginalData flight must not borrow timing from a replacement carrier"
    );
    assert!(binding.has_multipath_reinjection_alternative());
}
