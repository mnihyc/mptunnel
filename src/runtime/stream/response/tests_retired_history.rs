use super::super::attachment::{
    ResponseDispatchTarget, ResponseOutputAttachment, ResponseOutputAttachmentState,
    ResponsePathDetachOutcome, ResponseStreamAttachOutcome,
};
use super::super::test_support::{binding_for_underlay, stream_data_frame_at};
use super::*;
use crate::model::path::{CarrierPathKey, PathPolicy};
use crate::model::product_qualification::ProductQualificationLedger;
use crate::model::work::CarrierWorkKind;
use crate::protocol::{ConfiguredMemberSlot, OffsetRange, PathId, UnderlayProtocol};
use crate::runtime::path::commands::{
    reliable_path_command_channels, try_recv_reliable_path_command,
};
use crate::scheduler::TrafficClass;
use crate::transport::RateHint;
use std::collections::BTreeMap;
use std::hint::black_box;
use std::time::{Duration, Instant};

fn retired_key(path_id: u16) -> CarrierPathKey {
    CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(path_id),
    }
}

fn r(start: u64, end: u64) -> OffsetRange {
    OffsetRange::new(start, end).expect("valid review range")
}

fn make_flight(
    key: CarrierPathKey,
    incarnation: u64,
    start: u64,
    end: u64,
    kind: CarrierWorkKind,
    evidence_eligible: bool,
) -> CarrierPathFlight {
    let mut flight = CarrierPathFlight::fixed_output(
        key,
        end,
        usize::try_from(end - start).expect("small test range"),
        Instant::now(),
        kind,
        (kind == CarrierWorkKind::ReinjectedData).then_some(Duration::from_millis(100)),
    );
    flight.output_incarnation = incarnation;
    flight.configured_slot = Some(ConfiguredMemberSlot(key.path_id.0));
    flight.assignment_range = r(start, end);
    flight.evidence_eligible = evidence_eligible;
    flight
}

fn count_ranges(
    flights: &BTreeMap<u64, Vec<CarrierPathFlight>>,
    upper: u64,
    threshold: usize,
) -> Vec<OffsetRange> {
    let mut result = Vec::new();
    let mut active = None;
    for offset in 0..upper {
        let count = flights
            .range(..=offset)
            .flat_map(|(&start, entries)| {
                entries
                    .iter()
                    .filter(move |flight| start <= offset && flight.end > offset)
            })
            .count();
        match (active, count >= threshold) {
            (None, true) => active = Some(offset),
            (Some(start), false) => {
                result.push(r(start, offset));
                active = None;
            }
            _ => {}
        }
    }
    if let Some(start) = active {
        result.push(r(start, upper));
    }
    result
}

fn assert_index_matches_full_rows(
    ledger: &ResponseProductFlightLedger,
    full_rows: &BTreeMap<u64, Vec<CarrierPathFlight>>,
    upper: u64,
) {
    let window = r(0, upper);
    assert_eq!(
        ledger.overlap.covered_intersections(window),
        count_ranges(full_rows, upper, 1),
        "retained U must match the uncompact rows"
    );
    assert_eq!(
        ledger.overlap.ambiguous_intersections_for_ack(&[window]),
        count_ranges(full_rows, upper, 2),
        "retained M must match the uncompact rows"
    );
}

fn original_release_view(
    released: &[(u64, CarrierPathReleasedFlight)],
) -> Vec<(u64, u64, CarrierPathKey, u64, usize, bool)> {
    released
        .iter()
        .filter(|(_, item)| item.flight.kind.is_original_transmission())
        .map(|(start, item)| {
            (
                *start,
                item.flight.end,
                item.flight.key,
                item.flight.output_incarnation,
                item.flight.bytes,
                item.path_proving,
            )
        })
        .collect()
}

fn assert_release_matches_uncompact_originals(
    ledger: &mut ResponseProductFlightLedger,
    reference_rows: &mut BTreeMap<u64, Vec<CarrierPathFlight>>,
    ack: &[OffsetRange],
) {
    let expected = release_carrier_path_flight_ranges_reference(reference_rows, ack);
    let actual = ledger.release(ack);
    assert_eq!(
        original_release_view(&actual),
        original_release_view(&expected),
        "Original release identity, bytes, order, and proof must match"
    );
}

#[test]
fn retired_single_witness_keeps_um_through_partial_ack_fragments() {
    let retired = retired_key(2);
    let mut ledger = ResponseProductFlightLedger::default();
    let mut reference = BTreeMap::new();
    for (incarnation, end) in [(1, 24), (2, 40), (3, 48), (4, 32)] {
        let flight = make_flight(
            retired,
            incarnation,
            0,
            end,
            CarrierWorkKind::ReinjectedData,
            false,
        );
        ledger.publish(0, flight);
        reference.entry(0).or_insert_with(Vec::new).push(flight);
    }

    assert_eq!(ledger.retire_and_compact_copies(retired, 4, &[]), 3);
    assert_eq!(ledger.flights.get(&0).expect("witness bucket").len(), 1);
    assert_eq!(ledger.flights[&0][0].output_incarnation, 3);
    assert_eq!(ledger.flights[&0][0].end, 48);
    assert_eq!(ledger.overlap.flight_count(), 1);
    assert_index_matches_full_rows(&ledger, &reference, 64);
    assert_eq!(
        ledger.overlap.ambiguous_intersections_for_ack(&[r(0, 64)]),
        vec![r(0, 40)],
        "one physical witness does not erase historical ambiguity"
    );

    // The partial ACK splits the one stored witness into two rows. U/M remain
    // the pre-compaction history minus exactly the acknowledged middle.
    let ack = [r(16, 24)];
    assert_release_matches_uncompact_originals(&mut ledger, &mut reference, &ack);
    assert_eq!(ledger.overlap.flight_count(), 2);
    assert_index_matches_full_rows(&ledger, &reference, 64);
    assert_eq!(
        ledger.overlap.ambiguous_intersections_for_ack(&[r(0, 64)]),
        vec![r(0, 16), r(24, 40)]
    );

    // A later publication must use the retained U when extending M. The
    // fragment insertion above is not a second publication.
    let later = make_flight(
        retired_key(9),
        9,
        8,
        36,
        CarrierWorkKind::ReinjectedData,
        true,
    );
    ledger.publish(8, later);
    reference.entry(8).or_default().push(later);
    assert_index_matches_full_rows(&ledger, &reference, 64);

    let tail_ack = [r(24, 48)];
    assert_release_matches_uncompact_originals(&mut ledger, &mut reference, &tail_ack);
    assert_index_matches_full_rows(&ledger, &reference, 64);
    let final_ack = [r(0, 24)];
    assert_release_matches_uncompact_originals(&mut ledger, &mut reference, &final_ack);
    assert_index_matches_full_rows(&ledger, &reference, 64);
}

#[test]
fn retired_compaction_preserves_original_and_exact_protected_rows() {
    let original_key = retired_key(1);
    let retired_copy_key = retired_key(2);
    let current_key = retired_key(3);
    let detaching_key = retired_key(4);
    let receipt_key = retired_key(5);
    let mut ledger = ResponseProductFlightLedger::default();
    let original = make_flight(original_key, 1, 0, 64, CarrierWorkKind::OriginalData, true);
    ledger.publish(0, original);
    for (incarnation, end) in [(1, 24), (2, 40), (3, 48)] {
        ledger.publish(
            0,
            make_flight(
                retired_copy_key,
                incarnation,
                0,
                end,
                CarrierWorkKind::ReinjectedData,
                false,
            ),
        );
    }
    let current = make_flight(
        current_key,
        7,
        0,
        40,
        CarrierWorkKind::ReinjectedData,
        false,
    );
    let detaching = make_flight(
        detaching_key,
        8,
        0,
        44,
        CarrierWorkKind::ReinjectedData,
        false,
    );
    let mut qualification = ProductQualificationLedger::default();
    let receipt = qualification
        .tag_admitted_original(1, 56, r(0, 56))
        .expect("receipt fixture is valid")
        .expect("receipt fixture is admitted");
    let mut receipt_copy = make_flight(
        receipt_key,
        9,
        0,
        56,
        CarrierWorkKind::ReinjectedData,
        false,
    );
    receipt_copy.qualification_receipt = Some(receipt);
    ledger.publish(0, current);
    ledger.publish(0, detaching);
    ledger.publish(0, receipt_copy);
    let old_ambiguity = ledger.overlap.ambiguous_intersections_for_ack(&[r(0, 64)]);

    let protected = [(current_key, 7), (detaching_key, 8)];
    assert_eq!(
        ledger.retire_and_compact_copies(retired_copy_key, 3, &protected),
        2
    );
    let rows = ledger.flights.get(&0).expect("retained bucket");
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0].kind, CarrierWorkKind::OriginalData);
    assert_eq!(rows[0].key, original.key);
    assert_eq!(rows[1].key, retired_copy_key);
    assert_eq!(rows[1].output_incarnation, 3);
    assert_eq!(rows[1].end, 48, "keep the eligible max-end row");
    assert_eq!(
        (rows[2].key, rows[2].output_incarnation, rows[2].end),
        (current.key, current.output_incarnation, current.end),
        "stale-but-current rows remain exact"
    );
    assert_eq!(
        (rows[3].key, rows[3].output_incarnation, rows[3].end),
        (detaching.key, detaching.output_incarnation, detaching.end),
        "detaching rows remain exact"
    );
    assert_eq!(
        (
            rows[4].key,
            rows[4].output_incarnation,
            rows[4].end,
            rows[4].qualification_receipt,
        ),
        (
            receipt_copy.key,
            receipt_copy.output_incarnation,
            receipt_copy.end,
            receipt_copy.qualification_receipt,
        ),
        "receipt-bearing rows remain exact"
    );
    assert_eq!(
        ledger.overlap.ambiguous_intersections_for_ack(&[r(0, 64)]),
        old_ambiguity,
        "compaction must not reconstruct lifetime ambiguity from rows"
    );
    assert_eq!(ledger.overlap.flight_count(), rows.len());
}

#[test]
fn final_detach_without_stale_callback_and_closed_replacement_keep_exact_lifetimes() {
    let (binding, owner, _owner_receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let frame = stream_data_frame_at(0, 64);
    binding.record_original_flight(owner, &frame);
    let (owner_path_instance_id, owner_incarnation) = {
        let outputs = binding.outputs.lock().expect("response outputs");
        let entry = outputs
            .entries
            .iter()
            .find(|entry| entry.key == owner)
            .expect("Original owner");
        (entry.path_instance_id, entry.incarnation)
    };
    binding
        .begin_path_detach(owner, owner_path_instance_id)
        .expect("begin Original owner detach");
    binding.complete_path_detach(owner, owner_path_instance_id, owner_incarnation);

    let copy_key = retired_key(7);
    let survivor_key = retired_key(8);
    let (survivor_commands, _survivor_receivers) = reliable_path_command_channels(8);
    binding.attach(
        survivor_key.underlay,
        survivor_key.path_id,
        survivor_commands,
        TrafficClass::Throughput,
    );

    // These are final-detach callbacks without any preceding stale callback.
    // Different extents exercise max-end replacement, and detaching membership
    // stays exact until complete_path_detach.
    let mut retired_incarnations = Vec::new();
    for payload_len in [16, 32, 48] {
        let (commands, receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach_output(ResponseOutputAttachment {
                key: copy_key,
                path_instance_id: super::super::next_server_carrier_path_instance_id(),
                configured_slot: ConfiguredMemberSlot(copy_key.path_id.0),
                local_policy: PathPolicy::default(),
                startup_rate_prior: RateHint::Unknown,
                commands,
                state: ResponseOutputAttachmentState::default(),
            }),
            ResponseStreamAttachOutcome::Attached
        );
        binding.record_reinjected_flight(copy_key, &stream_data_frame_at(0, payload_len));
        let (path_instance_id, incarnation) = {
            let outputs = binding.outputs.lock().expect("response outputs");
            let entry = outputs
                .entries
                .iter()
                .find(|entry| entry.key == copy_key)
                .expect("current copy output");
            (entry.path_instance_id, entry.incarnation)
        };
        retired_incarnations.push(incarnation);
        let detached = binding
            .begin_path_detach(copy_key, path_instance_id)
            .expect("begin exact copy detach");
        assert!(matches!(
            detached,
            ResponsePathDetachOutcome::Begun(value) if value == incarnation
        ));
        {
            let outputs = binding.outputs.lock().expect("response outputs");
            assert!(
                outputs
                    .detaching
                    .iter()
                    .any(|entry| { entry.key == copy_key && entry.incarnation == incarnation })
            );
            let flights = binding.flights.lock().expect("response flights");
            assert!(flights.get(&0).expect("copy rows").iter().any(|flight| {
                flight.key == copy_key && flight.output_incarnation == incarnation
            }));
        }
        binding.complete_path_detach(copy_key, path_instance_id, incarnation);
        drop(receivers);
        let flights = binding.flights.lock().expect("response flights");
        let copies = flights
            .get(&0)
            .expect("Original and retired witness")
            .iter()
            .filter(|flight| flight.kind == CarrierWorkKind::ReinjectedData)
            .collect::<Vec<_>>();
        assert_eq!(copies.len(), 1);
        assert_eq!(copies[0].end, payload_len as u64);
        assert!(!copies[0].evidence_eligible);
        assert_eq!(flights.overlap.flight_count(), 2);
    }
    assert_eq!(retired_incarnations.len(), 3);
    assert_eq!(
        binding.original_flight_outputs_overlapping_frame(&frame),
        vec![(owner, owner_incarnation)],
        "the failed Original remains exact after its owner leaves"
    );
    assert!(binding.uncopied_completion_prefix(r(0, 64)).is_none());

    // A closed same-key output is replaced with a fresh identity. Its old copy
    // joins retired history; the replacement is not compacted with that row.
    let (old_commands, old_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach_output(ResponseOutputAttachment {
            key: copy_key,
            path_instance_id: super::super::next_server_carrier_path_instance_id(),
            configured_slot: ConfiguredMemberSlot(copy_key.path_id.0),
            local_policy: PathPolicy::default(),
            startup_rate_prior: RateHint::Unknown,
            commands: old_commands,
            state: ResponseOutputAttachmentState::default(),
        }),
        ResponseStreamAttachOutcome::Attached
    );
    binding.record_reinjected_flight(copy_key, &stream_data_frame_at(0, 40));
    drop(old_receivers);
    let (replacement_commands, _replacement_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            copy_key.underlay,
            copy_key.path_id,
            replacement_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::ReplacedClosedOutput
    );
    let replacement_incarnation = binding
        .outputs
        .lock()
        .expect("response outputs")
        .entries
        .iter()
        .find(|entry| entry.key == copy_key)
        .expect("replacement current owner")
        .incarnation;
    assert!(!retired_incarnations.contains(&replacement_incarnation));
    {
        let flights = binding.flights.lock().expect("response flights");
        let copy = flights
            .get(&0)
            .expect("retired max-end copy")
            .iter()
            .find(|flight| flight.kind == CarrierWorkKind::ReinjectedData)
            .expect("retired witness");
        assert_eq!(copy.end, 48);
        assert!(!copy.evidence_eligible);
        assert_ne!(copy.output_incarnation, replacement_incarnation);
    }
    binding.record_reinjected_flight(copy_key, &stream_data_frame_at(0, 60));
    {
        let flights = binding.flights.lock().expect("response flights");
        let copies = flights
            .get(&0)
            .expect("retired plus current copy")
            .iter()
            .filter(|flight| flight.kind == CarrierWorkKind::ReinjectedData)
            .collect::<Vec<_>>();
        assert_eq!(copies.len(), 2);
        assert!(copies.iter().any(|flight| {
            flight.output_incarnation == replacement_incarnation && flight.evidence_eligible
        }));
    }
    let replacement_path_instance_id = binding
        .outputs
        .lock()
        .expect("response outputs")
        .entries
        .iter()
        .find(|entry| entry.key == copy_key && entry.incarnation == replacement_incarnation)
        .expect("replacement output")
        .path_instance_id;
    binding
        .begin_path_detach(copy_key, replacement_path_instance_id)
        .expect("begin replacement detach");
    binding.complete_path_detach(
        copy_key,
        replacement_path_instance_id,
        replacement_incarnation,
    );
    {
        let flights = binding.flights.lock().expect("response flights");
        let copies = flights
            .get(&0)
            .expect("Original and replacement witness")
            .iter()
            .filter(|flight| flight.kind == CarrierWorkKind::ReinjectedData)
            .collect::<Vec<_>>();
        assert_eq!(copies.len(), 1);
        assert_eq!(copies[0].end, 60);
        assert_eq!(copies[0].output_incarnation, replacement_incarnation);
        assert!(!copies[0].evidence_eligible);
        assert_eq!(flights.overlap.flight_count(), 2);
    }
    assert_eq!(binding.uncovered_failed_original_ranges(), vec![r(0, 64)]);
}

// Exact portable transcription of the supplied unconditional-copy loop. Keep
// this test-only reference so the source suite can compare it with both old
// evidence invalidation and the in-place production refinement.
fn compact_as_supplied_for_test(
    ledger: &mut ResponseProductFlightLedger,
    key: CarrierPathKey,
    incarnation: u64,
    protected: &[(CarrierPathKey, u64)],
) -> usize {
    let mut removed = 0;
    for (&start, entries) in &mut ledger.flights {
        let old_len = entries.len();
        let mut witness: Option<usize> = None;
        let mut write = 0;
        for read in 0..old_len {
            let mut flight = entries[read];
            if flight.key == key && flight.output_incarnation == incarnation {
                flight.evidence_eligible = false;
            }
            let retired_copy = flight.kind == CarrierWorkKind::ReinjectedData
                && !flight.evidence_eligible
                && flight.qualification_receipt.is_none()
                && !protected.contains(&(flight.key, flight.output_incarnation));
            if retired_copy {
                if let Some(index) = witness {
                    if flight.end > entries[index].end {
                        entries[index] = flight;
                    }
                    continue;
                }
                witness = Some(write);
            }
            entries[write] = flight;
            write += 1;
        }
        if write != old_len {
            entries.truncate(write);
            ledger
                .overlap
                .rebuild_bucket(start, old_len, entries.iter().map(|flight| flight.end));
            let reserve = write.saturating_mul(2);
            if entries.capacity() > reserve {
                entries.shrink_to(reserve);
            }
            removed += old_len - write;
        }
    }
    removed
}

fn profile_ledger(originals: usize, copies: usize) -> ResponseProductFlightLedger {
    let mut ledger = ResponseProductFlightLedger::default();
    for index in 0..originals {
        let start = u64::try_from(index * 2).expect("small benchmark offset");
        ledger.publish(
            start,
            make_flight(
                retired_key(1),
                u64::try_from(index + 1).expect("test identity"),
                start,
                start + 1,
                CarrierWorkKind::OriginalData,
                true,
            ),
        );
    }
    let copy_start = u64::try_from(originals * 2 + 16).expect("small benchmark offset");
    for incarnation in 1..=copies {
        ledger.publish(
            copy_start,
            make_flight(
                retired_key(60_000),
                u64::try_from(incarnation).expect("test identity"),
                copy_start,
                copy_start + 1 + u64::try_from(incarnation % 8).expect("small extent"),
                CarrierWorkKind::ReinjectedData,
                incarnation == copies,
            ),
        );
    }
    ledger
}

fn vector_capacity(ledger: &ResponseProductFlightLedger) -> usize {
    ledger.flights.values().map(Vec::capacity).sum()
}

#[test]
#[ignore = "manual retirement latency and vector-capacity profile"]
fn retirement_cost_profiles_compare_invalidation_supplied_and_in_place() {
    const ORIGINALS: usize = 4_096;
    const REPETITIONS: usize = 16;
    // Fresh ledgers keep the retirement/membership workload unchanged between
    // observations. Construction is outside each timed operation. Rotate the
    // three methods rather than always charging one method for the cold fixture.
    for round in 0..3 {
        for protected_width in [4, 64] {
            let protected = (0..protected_width)
                .map(|index| (retired_key(index as u16), index as u64 + 10_000))
                .collect::<Vec<_>>();
            for candidate_copies in [0, 1, 64] {
                let mut elapsed = [0_u128; 3];
                let mut capacities = [0; 3];
                let mut row_counts = [0; 3];
                let target = (retired_key(60_000), candidate_copies as u64);
                for _ in 0..REPETITIONS {
                    for order in 0..3 {
                        let method = (order + round) % 3;
                        let mut ledger = profile_ledger(ORIGINALS, candidate_copies);
                        let before_capacity = vector_capacity(&ledger);
                        let before_rows = if candidate_copies == 0 {
                            Some(format!("{:?}", ledger.flights))
                        } else {
                            None
                        };
                        let started = Instant::now();
                        let removed = match method {
                            0 => {
                                ledger.invalidate_evidence(target.0, target.1);
                                0
                            }
                            1 => compact_as_supplied_for_test(
                                &mut ledger,
                                target.0,
                                target.1,
                                &protected,
                            ),
                            _ => ledger.retire_and_compact_copies(target.0, target.1, &protected),
                        };
                        elapsed[method] += started.elapsed().as_nanos();
                        assert_eq!(
                            removed,
                            if method == 0 {
                                0
                            } else {
                                candidate_copies.saturating_sub(1)
                            }
                        );
                        if let Some(before) = before_rows {
                            assert_eq!(before, format!("{:?}", ledger.flights));
                            assert_eq!(before_capacity, vector_capacity(&ledger));
                        }
                        capacities[method] = vector_capacity(&ledger);
                        row_counts[method] = ledger.overlap.flight_count();
                        black_box(ledger);
                    }
                }
                println!(
                    "retirement-profile round={round} repetitions={REPETITIONS} originals={ORIGINALS} eligible_copy_rows={candidate_copies} protected={protected_width} invalidation_ns={} supplied_ns={} in_place_ns={} rows={row_counts:?} capacities={capacities:?}",
                    elapsed[0] / REPETITIONS as u128,
                    elapsed[1] / REPETITIONS as u128,
                    elapsed[2] / REPETITIONS as u128,
                );
            }
        }
    }
}

#[test]
#[ignore = "manual repeated real producer attachment and retirement stress"]
fn repeated_same_range_producer_retirement_keeps_one_retired_witness() {
    const ROTATIONS: usize = 256;
    let (binding, owner, _owner_receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let frame = stream_data_frame_at(0, 4096);
    binding.record_original_flight(owner, &frame);
    let (owner_path_instance_id, owner_incarnation) = {
        let outputs = binding.outputs.lock().expect("response outputs");
        let entry = outputs
            .entries
            .iter()
            .find(|entry| entry.key == owner)
            .expect("Original owner");
        (entry.path_instance_id, entry.incarnation)
    };
    binding
        .begin_path_detach(owner, owner_path_instance_id)
        .expect("begin failed owner detach");
    binding.complete_path_detach(owner, owner_path_instance_id, owner_incarnation);

    let copy_key = retired_key(5);
    let survivor_key = retired_key(6);
    let (survivor_commands, _survivor_receivers) = reliable_path_command_channels(8);
    binding.attach(
        survivor_key.underlay,
        survivor_key.path_id,
        survivor_commands,
        TrafficClass::Throughput,
    );
    for generation in 0..ROTATIONS {
        let (commands, mut receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach_output(ResponseOutputAttachment {
                key: copy_key,
                path_instance_id: super::super::next_server_carrier_path_instance_id(),
                configured_slot: ConfiguredMemberSlot(copy_key.path_id.0),
                local_policy: PathPolicy::default(),
                startup_rate_prior: RateHint::Unknown,
                commands,
                state: ResponseOutputAttachmentState::default(),
            }),
            ResponseStreamAttachOutcome::Attached
        );
        let target: ResponseDispatchTarget = binding
            .sender_path_targets(TrafficClass::Throughput, 4096)
            .into_iter()
            .find(|target| target.observation.key == copy_key)
            .expect("current producer target")
            .into();
        binding
            .try_enqueue_reinjected_frame_for_target(
                &target,
                &frame,
                TrafficClass::Throughput,
                0,
                4096,
                None,
            )
            .expect("current generation accepts same Product range");
        assert!(try_recv_reliable_path_command(&mut receivers).is_some());
        let (path_instance_id, incarnation) = {
            let outputs = binding.outputs.lock().expect("response outputs");
            let entry = outputs
                .entries
                .iter()
                .find(|entry| entry.key == copy_key)
                .expect("current copy output");
            (entry.path_instance_id, entry.incarnation)
        };
        binding
            .begin_path_detach(copy_key, path_instance_id)
            .expect("begin generation detach");
        binding.complete_path_detach(copy_key, path_instance_id, incarnation);
        let flights = binding.flights.lock().expect("response flights");
        let copies = flights
            .get(&0)
            .expect("Original plus witness")
            .iter()
            .filter(|flight| flight.kind == CarrierWorkKind::ReinjectedData)
            .count();
        assert_eq!(copies, 1, "generation {generation}");
        assert_eq!(flights.overlap.flight_count(), 2);
    }
    let flights = binding.flights.lock().expect("response flights");
    assert_eq!(
        flights
            .overlap
            .ambiguous_intersections_for_ack(&[r(0, 4096)]),
        vec![r(0, 4096)]
    );
    assert!(uncopied_completion_prefix(&flights.flights, r(0, 4096)).is_none());
}

#[test]
fn retired_ten_thousand_generations_preserve_release_without_retaining_attempts() {
    let owner = retired_key(1);
    let copy_owner = retired_key(2);
    let mut exact = ResponseProductFlightLedger::default();
    let mut compact = ResponseProductFlightLedger::default();
    let original = make_flight(owner, 1, 0, 4, CarrierWorkKind::OriginalData, true);
    exact.publish(0, original);
    compact.publish(0, original);
    let mut peak_capacity = 0;
    for incarnation in 1..=10_000 {
        let copy = make_flight(
            copy_owner,
            incarnation,
            0,
            4,
            CarrierWorkKind::ReinjectedData,
            true,
        );
        exact.publish(0, copy);
        compact.publish(0, copy);
        exact.invalidate_evidence(copy_owner, incarnation);
        compact.retire_and_compact_copies(copy_owner, incarnation, &[(owner, 1)]);
        assert_eq!(compact.overlap.flight_count(), 2);
        peak_capacity = peak_capacity.max(vector_capacity(&compact));
    }
    assert_eq!(exact.overlap.flight_count(), 10_001);
    assert_eq!(
        compact.overlap.ambiguous_intersections_for_ack(&[r(0, 4)]),
        vec![r(0, 4)]
    );
    assert!(uncopied_completion_prefix(&compact.flights, r(0, 4)).is_none());
    let node_bytes = ProductFlightIndex::layout_for_test().1;
    let row_bytes = std::mem::size_of::<CarrierPathFlight>();
    let subtotal = |ledger: &ResponseProductFlightLedger| {
        vector_capacity(ledger) * row_bytes + ledger.overlap.flight_count() * node_bytes
    };
    println!(
        "retired-generation-storage generations=10000 exact_rows={} compact_rows={} row_bytes={row_bytes} node_bytes={node_bytes} exact_vector_plus_nodes_bytes={} compact_vector_plus_nodes_bytes={} compact_peak_vector_slots={peak_capacity}",
        exact.overlap.flight_count(),
        compact.overlap.flight_count(),
        subtotal(&exact),
        subtotal(&compact)
    );
    let exact_release = exact.release(&[r(0, 4)]);
    let compact_release = compact.release(&[r(0, 4)]);
    assert_eq!(
        original_release_view(&exact_release),
        original_release_view(&compact_release)
    );
    assert!(
        compact_release
            .iter()
            .all(|(_, release)| !release.path_proving)
    );
    assert!(exact.is_empty());
    assert!(compact.is_empty());
    assert_eq!(compact.overlap.flight_count(), 0);
}
