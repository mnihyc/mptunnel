use super::ResponseStreamBinding;
use super::attachment::{ResponsePathDetachOutcome, ResponseStreamAttachOutcome};
use super::test_support::{
    binding_for_underlay, stream_data_frame_at, with_output_entry_for_key,
    with_output_entry_for_key_mut,
};
use crate::model::path::CarrierPathKey;
use crate::protocol::{ConfiguredMemberSlot, OffsetRange, PathId, UnderlayProtocol};
use crate::runtime::path::commands::reliable_path_command_channels;
use crate::runtime::sender::ServerReinjectionOutputIdentity;
use crate::scheduler::TrafficClass;
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

struct DebtFixture {
    binding: Arc<ResponseStreamBinding>,
    keys: Vec<CarrierPathKey>,
    _receivers: Vec<crate::runtime::path::commands::ReliablePathCommandReceivers>,
}

fn fixture_with_tcp_outputs(width: usize) -> DebtFixture {
    assert!(width > 0);
    let (binding, first_key, first_receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let mut keys = vec![first_key];
    let mut receivers = vec![first_receivers];
    for raw_id in 1..width {
        let path_id = PathId(u16::try_from(raw_id).expect("test path id fits"));
        let (commands, output_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                UnderlayProtocol::Tcp,
                path_id,
                commands,
                TrafficClass::Throughput,
            ),
            ResponseStreamAttachOutcome::Attached
        );
        keys.push(CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id,
        });
        receivers.push(output_receivers);
    }
    DebtFixture {
        binding,
        keys,
        _receivers: receivers,
    }
}

fn output_identity(
    binding: &ResponseStreamBinding,
    key: CarrierPathKey,
) -> ServerReinjectionOutputIdentity {
    let incarnation = with_output_entry_for_key(binding, key, |entry| entry.incarnation);
    ServerReinjectionOutputIdentity { key, incarnation }
}

fn set_slot(binding: &ResponseStreamBinding, key: CarrierPathKey, slot: u16) {
    with_output_entry_for_key_mut(binding, key, |entry| {
        entry.configured_slot = ConfiguredMemberSlot(slot);
    });
}

fn retained_flight_count(binding: &ResponseStreamBinding) -> usize {
    binding
        .flights
        .lock()
        .expect("test response flight lock")
        .values()
        .map(Vec::len)
        .sum()
}

fn assert_batch_matches_reference(
    binding: &ResponseStreamBinding,
    identities: &[ServerReinjectionOutputIdentity],
    expected: &[usize],
) {
    let reference = identities
        .iter()
        .copied()
        .map(|identity| binding.accepted_reinjected_data_in_flight_bytes_at(identity))
        .collect::<Vec<_>>();
    let batch = binding.accepted_reinjected_data_in_flight_bytes_for_outputs_at(identities);
    assert_eq!(reference, expected);
    assert_eq!(batch.as_slice(), expected);
}

fn populated_benchmark_fixture(
    width: usize,
    copies: bool,
) -> (DebtFixture, Vec<ServerReinjectionOutputIdentity>) {
    let fixture = fixture_with_tcp_outputs(width);
    if copies && width > 1 {
        set_slot(&fixture.binding, fixture.keys[0], 77);
        set_slot(&fixture.binding, fixture.keys[1], 77);
    }
    for (index, key) in fixture.keys.iter().copied().enumerate() {
        for fragment in 0..8 {
            let offset = 10_000 + ((index * 8 + fragment) as u64) * 2048;
            fixture
                .binding
                .record_original_flight(key, &stream_data_frame_at(offset, 1024));
        }
        if copies {
            let (offset, bytes) = match index {
                0 => (900_000, 1024),
                1 if width > 1 => (900_512, 1024),
                _ => (1_000_000 + (index as u64) * 4096, 256),
            };
            fixture
                .binding
                .record_reinjected_flight(key, &stream_data_frame_at(offset, bytes));
        }
    }
    let identities = fixture
        .keys
        .iter()
        .copied()
        .map(|key| output_identity(&fixture.binding, key))
        .collect();
    (fixture, identities)
}

fn timed_reference_observation(
    binding: &ResponseStreamBinding,
    identities: &[ServerReinjectionOutputIdentity],
    observations: usize,
) -> (Duration, u64) {
    let started = Instant::now();
    let mut checksum = 0u64;
    for _ in 0..observations {
        for (index, identity) in identities.iter().copied().enumerate() {
            let debt = black_box(binding.accepted_reinjected_data_in_flight_bytes_at(identity));
            checksum = checksum
                .rotate_left(7)
                .wrapping_add(debt as u64)
                .wrapping_add(index as u64 + 1);
        }
        if let Some(identity) = identities.first().copied() {
            let selected_debt =
                black_box(binding.accepted_reinjected_data_in_flight_bytes_at(identity));
            checksum = checksum
                .rotate_left(7)
                .wrapping_add(selected_debt as u64)
                .wrapping_add(identities.len() as u64 + 1);
        }
    }
    (started.elapsed(), black_box(checksum))
}

fn timed_batched_observation(
    binding: &ResponseStreamBinding,
    identities: &[ServerReinjectionOutputIdentity],
    observations: usize,
) -> (Duration, u64) {
    let started = Instant::now();
    let mut checksum = 0u64;
    for _ in 0..observations {
        let debts =
            black_box(binding.accepted_reinjected_data_in_flight_bytes_for_outputs_at(identities));
        for (index, debt) in debts.iter().copied().enumerate() {
            checksum = checksum
                .rotate_left(7)
                .wrapping_add(debt as u64)
                .wrapping_add(index as u64 + 1);
        }
        if let Some(selected_debt) = debts.first().copied() {
            checksum = checksum
                .rotate_left(7)
                .wrapping_add(black_box(selected_debt) as u64)
                .wrapping_add(identities.len() as u64 + 1);
        }
    }
    (started.elapsed(), black_box(checksum))
}

#[test]
fn batched_slot_debt_matches_single_output_reference_and_visits_flights_once() {
    // The widths cover no-copy scans and overlapping copies for one target, a
    // normal small path set, and a bounded wider set of independent domains.
    for (width, copies) in [(1, false), (4, false), (32, false), (4, true), (32, true)] {
        let fixture = fixture_with_tcp_outputs(width);
        for (index, key) in fixture.keys.iter().copied().enumerate() {
            fixture
                .binding
                .record_original_flight(key, &stream_data_frame_at((index as u64) * 4096, 512));
        }

        if copies {
            set_slot(&fixture.binding, fixture.keys[0], 77);
            set_slot(&fixture.binding, fixture.keys[1], 77);
            fixture
                .binding
                .record_reinjected_flight(fixture.keys[0], &stream_data_frame_at(0, 1024));
            fixture
                .binding
                .record_reinjected_flight(fixture.keys[1], &stream_data_frame_at(512, 1024));
            fixture
                .binding
                .record_reinjected_flight(fixture.keys[2], &stream_data_frame_at(0, 2048));
            for index in (3..width).step_by(4) {
                fixture.binding.record_reinjected_flight(
                    fixture.keys[index],
                    &stream_data_frame_at((index as u64) * 8192, 256),
                );
            }
        }

        let identities = fixture
            .keys
            .iter()
            .copied()
            .map(|key| output_identity(&fixture.binding, key))
            .collect::<Vec<_>>();
        let expected = identities
            .iter()
            .copied()
            .map(|identity| {
                fixture
                    .binding
                    .accepted_reinjected_data_in_flight_bytes_at(identity)
            })
            .collect::<Vec<_>>();
        let flight_count = retained_flight_count(&fixture.binding);
        let (actual, batched_flight_visits) = fixture
            .binding
            .accepted_reinjected_data_in_flight_bytes_for_outputs_at_with_flight_visits(
                &identities,
            );

        assert_eq!(
            actual.as_slice(),
            expected.as_slice(),
            "width={width}, copies={copies}"
        );
        assert_eq!(
            batched_flight_visits, flight_count,
            "one batch observation visits each retained flight once"
        );
        assert_eq!(
            identities.len() * flight_count,
            width * flight_count,
            "the former one-query-per-target path visits the ledger once per candidate"
        );
        if copies {
            assert_eq!(actual[0], 1536, "overlapping same-slot copies form a union");
            assert_eq!(actual[1], 1536, "same-slot candidates share the debt");
            assert_eq!(actual[2], 2048, "a distinct slot retains its own union");
            assert!(actual[3..].iter().all(|debt| *debt == 0 || *debt == 256));
        } else {
            assert!(actual.iter().all(|debt| *debt == 0));
        }
    }
}

#[test]
fn batched_slot_debt_uses_current_incarnations_and_underlay_domains() {
    let fixture = fixture_with_tcp_outputs(3);
    let slot = ConfiguredMemberSlot(91);
    set_slot(&fixture.binding, fixture.keys[0], slot.0);
    set_slot(&fixture.binding, fixture.keys[1], slot.0);
    fixture
        .binding
        .record_reinjected_flight(fixture.keys[0], &stream_data_frame_at(0, 1000));
    fixture
        .binding
        .record_reinjected_flight(fixture.keys[1], &stream_data_frame_at(500, 1000));

    let (udp_commands, udp_receivers) = reliable_path_command_channels(8);
    let udp_key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    assert_eq!(
        fixture.binding.attach(
            udp_key.underlay,
            udp_key.path_id,
            udp_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    set_slot(&fixture.binding, udp_key, slot.0);
    fixture
        .binding
        .record_reinjected_flight(udp_key, &stream_data_frame_at(0, 256));
    let _udp_receivers = udp_receivers;

    let tcp_first = output_identity(&fixture.binding, fixture.keys[0]);
    let tcp_second = output_identity(&fixture.binding, fixture.keys[1]);
    let udp = output_identity(&fixture.binding, udp_key);
    let before_detach = fixture
        .binding
        .accepted_reinjected_data_in_flight_bytes_for_outputs_at(&[tcp_first, tcp_second, udp]);
    assert_eq!(before_detach.as_slice(), &[1500, 1500, 256]);

    let (path_instance_id, incarnation) =
        with_output_entry_for_key(&fixture.binding, fixture.keys[1], |entry| {
            (entry.path_instance_id, entry.incarnation)
        });
    assert_eq!(
        fixture
            .binding
            .begin_path_detach(fixture.keys[1], path_instance_id),
        Some(ResponsePathDetachOutcome::Begun(incarnation))
    );

    let (successor_commands, successor_receivers) = reliable_path_command_channels(8);
    let successor_key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(3),
    };
    assert_eq!(
        fixture.binding.attach(
            successor_key.underlay,
            successor_key.path_id,
            successor_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    set_slot(&fixture.binding, successor_key, slot.0);
    let successor = output_identity(&fixture.binding, successor_key);

    let after_membership_change = fixture
        .binding
        .accepted_reinjected_data_in_flight_bytes_for_outputs_at(&[
            tcp_first, tcp_second, successor, udp,
        ]);
    assert_eq!(
        after_membership_change.as_slice(),
        &[1000, 0, 1000, 256],
        "the detached physical attempt remains retained but no longer joins current slot debt"
    );
    let _successor_receivers = successor_receivers;
}

#[test]
fn batched_slot_debt_matches_reference_through_publication_partial_ack_and_full_release() {
    let fixture = fixture_with_tcp_outputs(4);
    set_slot(&fixture.binding, fixture.keys[0], 91);
    set_slot(&fixture.binding, fixture.keys[1], 91);
    fixture
        .binding
        .record_reinjected_flight(fixture.keys[0], &stream_data_frame_at(0, 1000));
    fixture
        .binding
        .record_reinjected_flight(fixture.keys[1], &stream_data_frame_at(500, 1000));
    let identities = fixture
        .keys
        .iter()
        .copied()
        .map(|key| output_identity(&fixture.binding, key))
        .collect::<Vec<_>>();

    // The two published copies overlap across [500, 1000).
    assert_batch_matches_reference(&fixture.binding, &identities, &[1500, 1500, 0, 0]);

    // A partial STREAM_ACK splits the second physical copy around the ACKed
    // hole. The current debt remains the union of retained fragments only.
    fixture
        .binding
        .release_normalized_acked_ranges(&[OffsetRange {
            start: 512,
            end: 1024,
        }]);
    assert_batch_matches_reference(&fixture.binding, &identities, &[988, 988, 0, 0]);

    // Replayed ACK bytes must not subtract the retained union twice.
    fixture
        .binding
        .release_normalized_acked_ranges(&[OffsetRange {
            start: 512,
            end: 1024,
        }]);
    assert_batch_matches_reference(&fixture.binding, &identities, &[988, 988, 0, 0]);

    fixture.binding.release_normalized_acked_ranges(&[
        OffsetRange { start: 0, end: 512 },
        OffsetRange {
            start: 1024,
            end: 1500,
        },
    ]);
    assert_batch_matches_reference(&fixture.binding, &identities, &[0, 0, 0, 0]);
}

#[test]
#[ignore = "bounded debt-observation timing probe; run explicitly with --ignored --nocapture"]
fn benchmark_batched_slot_debt_full_observation() {
    const OBSERVATIONS_PER_BLOCK: usize = 128;
    for width in [1, 4, 32] {
        for copies in [false, true] {
            let (fixture, identities) = populated_benchmark_fixture(width, copies);
            let mut reference_elapsed = Duration::ZERO;
            let mut batch_elapsed = Duration::ZERO;
            let mut reference_checksum = 0u64;
            let mut batch_checksum = 0u64;

            // Fixed ABBA order controls coarse drift without extending the
            // probe into a general benchmark suite.
            for block in 0..4 {
                if block == 0 || block == 3 {
                    let (elapsed, checksum) = timed_reference_observation(
                        &fixture.binding,
                        &identities,
                        OBSERVATIONS_PER_BLOCK,
                    );
                    reference_elapsed += elapsed;
                    reference_checksum = reference_checksum.wrapping_add(checksum);
                } else {
                    let (elapsed, checksum) = timed_batched_observation(
                        &fixture.binding,
                        &identities,
                        OBSERVATIONS_PER_BLOCK,
                    );
                    batch_elapsed += elapsed;
                    batch_checksum = batch_checksum.wrapping_add(checksum);
                }
            }
            assert_eq!(reference_checksum, batch_checksum);
            eprintln!(
                "slot debt observation: targets={width} copies={copies} flights={} observations_per_variant={} reference_ns_per_observation={:.1} batch_ns_per_observation={:.1} checksum={reference_checksum}",
                retained_flight_count(&fixture.binding),
                OBSERVATIONS_PER_BLOCK * 2,
                reference_elapsed.as_secs_f64() * 1_000_000_000.0
                    / (OBSERVATIONS_PER_BLOCK * 2) as f64,
                batch_elapsed.as_secs_f64() * 1_000_000_000.0 / (OBSERVATIONS_PER_BLOCK * 2) as f64,
            );
        }
    }
}

#[test]
fn prepared_copy_view_matches_fresh_debt_after_expiry_and_mutation() {
    for width in [1, 4, 8] {
        let (fixture, identities) = populated_benchmark_fixture(width, true);
        let now = Instant::now();
        // Expiry changes suppression coverage, never configured-slot debt.
        for observed_at in [now, now + Duration::from_secs(3600)] {
            let view = fixture.binding.observe_prepared_copy_work(observed_at);
            assert_eq!(
                view.accepted_debts(&identities),
                fixture
                    .binding
                    .accepted_reinjected_data_in_flight_bytes_for_outputs_at(&identities),
            );
            let stale_view = fixture.binding.observe_prepared_copy_work(observed_at);
            fixture
                .binding
                .record_reinjected_flight(fixture.keys[0], &stream_data_frame_at(7_000_000, 512));
            assert_eq!(
                stale_view.accepted_debts(&identities),
                fixture
                    .binding
                    .accepted_reinjected_data_in_flight_bytes_for_outputs_at(&identities),
                "generation change must abandon the pre-publication debt view",
            );
        }
        let view = fixture.binding.observe_prepared_copy_work(now);
        let duplicate_ids = [identities[0], identities[0]];
        assert_eq!(
            view.accepted_debts(&duplicate_ids)[0],
            view.accepted_debts(&duplicate_ids)[1]
        );
        let absent = ServerReinjectionOutputIdentity {
            incarnation: identities[0].incarnation.wrapping_add(1_000_000),
            ..identities[0]
        };
        assert_eq!(view.accepted_debts(&[absent]).as_slice(), &[0]);
    }
}

#[test]
fn same_generation_copy_view_does_not_lock_the_flight_ledger() {
    let (fixture, identities) = populated_benchmark_fixture(4, true);
    let view = fixture.binding.observe_prepared_copy_work(Instant::now());
    let expected = fixture
        .binding
        .accepted_reinjected_data_in_flight_bytes_for_outputs_at(&identities);
    std::thread::scope(|scope| {
        let locked = fixture.binding.flights.lock().expect("test flights");
        let (send, receive) = std::sync::mpsc::channel();
        let view = &view;
        let identities = &identities;
        let worker = scope.spawn(move || {
            send.send(view.accepted_debts(identities)).unwrap();
        });
        let observed = receive.recv_timeout(Duration::from_secs(1));
        drop(locked);
        worker.join().unwrap();
        assert_eq!(
            observed.expect("read-local view must not rescan flights"),
            expected
        );
    });
}

// Append to tests_slot_debt.rs after its existing helpers/imports.

fn reference_prepared_coverage_audit(
    binding: &ResponseStreamBinding,
    at: Instant,
) -> (Vec<OffsetRange>, Option<Instant>) {
    // Frozen baseline body; do not call the candidate wrapper.
    let outputs = binding.outputs.lock().expect("test outputs");
    let flights = binding.flights.lock().expect("test flights");
    let mut ranges = Vec::new();
    let mut next = None;
    for (&start, rows) in flights.iter() {
        for flight in rows {
            if flight.kind != crate::model::work::CarrierWorkKind::ReinjectedData
                || !outputs.entries.iter().any(|entry| {
                    entry.key == flight.key && entry.incarnation == flight.output_incarnation
                })
            {
                continue;
            }
            if let Some(deadline) = flight.reinjection_suppression_deadline.filter(|d| *d > at) {
                ranges.push(OffsetRange {
                    start,
                    end: flight.end,
                });
                next = Some(next.map_or(deadline, |old: Instant| old.min(deadline)));
            }
        }
    }
    (
        crate::protocol::frame::normalize_offset_ranges(ranges),
        next,
    )
}

fn byte_mask_slot_debts_audit(
    binding: &ResponseStreamBinding,
    ids: &[ServerReinjectionOutputIdentity],
) -> Vec<usize> {
    // Independent oracle: set every covered byte rather than reducing intervals.
    let outputs = binding.outputs.lock().expect("test outputs");
    let flights = binding.flights.lock().expect("test flights");
    ids.iter()
        .map(|id| {
            let Some(target) = outputs
                .entries
                .iter()
                .find(|o| o.key == id.key && o.incarnation == id.incarnation)
            else {
                return 0;
            };
            let (underlay, slot) = (target.key.underlay, target.configured_slot);
            let mut bytes = std::collections::BTreeSet::new();
            for (&start, rows) in flights.iter() {
                for flight in rows {
                    if flight.kind == crate::model::work::CarrierWorkKind::ReinjectedData
                        && flight.key.underlay == underlay
                        && flight.configured_slot == Some(slot)
                        && flight.end > start
                        && outputs.entries.iter().any(|o| {
                            o.key == flight.key
                                && o.incarnation == flight.output_incarnation
                                && o.key.underlay == underlay
                                && o.configured_slot == slot
                        })
                    {
                        bytes.extend(start..flight.end);
                    }
                }
            }
            bytes.len()
        })
        .collect()
}

fn assert_prepared_view_matches_audit(
    binding: &ResponseStreamBinding,
    ids: &[ServerReinjectionOutputIdentity],
    at: Instant,
) -> (Vec<OffsetRange>, Option<Instant>, Vec<usize>) {
    let (coverage, deadline) = reference_prepared_coverage_audit(binding, at);
    let fresh = binding.accepted_reinjected_data_in_flight_bytes_for_outputs_at(ids);
    let mask = byte_mask_slot_debts_audit(binding, ids);
    assert_eq!(fresh.as_slice(), mask.as_slice());
    let view = binding.observe_prepared_copy_work(at);
    assert_eq!(view.covered, coverage);
    assert_eq!(view.next_deadline, deadline);
    let debts = view.accepted_debts(ids);
    assert_eq!(debts, fresh);
    (coverage, deadline, debts.to_vec())
}

fn seed_audit_rows(fixture: &DebtFixture, count: usize, copies: bool) {
    for n in 0..count {
        let key = fixture.keys[n % fixture.keys.len()];
        if copies {
            fixture
                .binding
                .record_reinjected_flight(key, &stream_data_frame_at(n as u64 * 256, 512));
        } else {
            fixture
                .binding
                .record_original_flight(key, &stream_data_frame_at(n as u64 * 2048, 512));
        }
    }
}

// Only fixture rows without compacted historical multiplicity are rebuilt here.
// Production ledgers intentionally expose no mutable map bypass of their index.
fn configure_fixture_copies(
    binding: &ResponseStreamBinding,
    mut configure: impl FnMut(&mut super::CarrierPathFlight),
) {
    let _outputs = binding.outputs.lock().expect("fixture outputs");
    let mut flights = binding.flights.lock().expect("fixture flights");
    let mut replacement = super::delivery::ResponseProductFlightLedger::default();
    for (&start, rows) in flights.iter() {
        for &flight in rows {
            let mut flight = flight;
            if flight.kind == crate::model::work::CarrierWorkKind::ReinjectedData {
                configure(&mut flight);
            }
            replacement.publish(start, flight);
        }
    }
    *flights = replacement;
    binding
        .response_model_generation
        .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
}

fn set_audit_copy_deadline(binding: &ResponseStreamBinding, deadline: Option<Instant>) {
    configure_fixture_copies(binding, |flight| {
        flight.reinjection_suppression_deadline = deadline;
    });
}

#[test]
fn prepared_view_matches_baseline_across_width_expiry_and_no_members() {
    for (width, rows, copies) in [
        (1, 0, false),
        (1, 1, false),
        (1, 1, true),
        (4, 16, true),
        (8, 256, true),
    ] {
        let fixture = fixture_with_tcp_outputs(width);
        if width >= 4 {
            set_slot(&fixture.binding, fixture.keys[0], 77);
            set_slot(&fixture.binding, fixture.keys[1], 77);
        }
        seed_audit_rows(&fixture, rows, copies);
        let deadline = Instant::now() + Duration::from_secs(60);
        set_audit_copy_deadline(&fixture.binding, Some(deadline));
        let ids = fixture
            .keys
            .iter()
            .copied()
            .map(|k| output_identity(&fixture.binding, k))
            .collect::<Vec<_>>();
        let live = assert_prepared_view_matches_audit(
            &fixture.binding,
            &ids,
            deadline - Duration::from_nanos(1),
        );
        if copies {
            assert!(!live.0.is_empty());
            assert_eq!(live.1, Some(deadline));
            assert!(live.2.iter().any(|d| *d > 0));
        }
        let expired = assert_prepared_view_matches_audit(&fixture.binding, &ids, deadline);
        assert!(expired.0.is_empty());
        assert_eq!(expired.1, None);
        assert_eq!(expired.2, live.2);
        set_audit_copy_deadline(&fixture.binding, None);
        let absent = assert_prepared_view_matches_audit(&fixture.binding, &ids, Instant::now());
        assert!(absent.0.is_empty());
        assert_eq!(absent.1, None);
        assert_eq!(absent.2, live.2);
    }

    let fixture = fixture_with_tcp_outputs(1);
    let key = fixture.keys[0];
    fixture
        .binding
        .record_reinjected_flight(key, &stream_data_frame_at(0, 512));
    let old = output_identity(&fixture.binding, key);
    let instance = with_output_entry_for_key(&fixture.binding, key, |e| e.path_instance_id);
    assert!(matches!(
        fixture.binding.begin_path_detach(key, instance),
        Some(ResponsePathDetachOutcome::Begun(_))
    ));
    set_audit_copy_deadline(
        &fixture.binding,
        Some(Instant::now() + Duration::from_secs(60)),
    );
    assert_prepared_view_matches_audit(&fixture.binding, &[], Instant::now());
    let stale = assert_prepared_view_matches_audit(&fixture.binding, &[old], Instant::now());
    assert!(stale.0.is_empty());
    assert_eq!(stale.2, [0]);
}

#[test]
fn prepared_view_preserves_slot_underlay_and_union_semantics() {
    for recorded_slot in [None, Some(ConfiguredMemberSlot(999))] {
        let fixture = fixture_with_tcp_outputs(1);
        let key = fixture.keys[0];
        fixture
            .binding
            .record_reinjected_flight(key, &stream_data_frame_at(0, 512));
        let id = output_identity(&fixture.binding, key);
        let deadline = Instant::now() + Duration::from_secs(60);
        configure_fixture_copies(&fixture.binding, |row| {
            row.configured_slot = recorded_slot;
            row.reinjection_suppression_deadline = Some(deadline);
        });
        let result = assert_prepared_view_matches_audit(&fixture.binding, &[id], Instant::now());
        assert_eq!(result.0, [OffsetRange { start: 0, end: 512 }]);
        assert_eq!(result.1, Some(deadline));
        assert_eq!(result.2, [0]);
    }

    let fixture = fixture_with_tcp_outputs(2);
    for key in fixture.keys.iter().copied() {
        set_slot(&fixture.binding, key, 77);
    }
    for (start, len) in [(0, 500), (0, 1000), (100, 100), (1000, 500), (1500, 200)] {
        fixture
            .binding
            .record_reinjected_flight(fixture.keys[0], &stream_data_frame_at(start, len));
    }
    fixture
        .binding
        .record_reinjected_flight(fixture.keys[1], &stream_data_frame_at(500, 1000));
    let (commands, _rx) = reliable_path_command_channels(8);
    let udp = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(0),
    };
    assert_eq!(
        fixture.binding.attach(
            udp.underlay,
            udp.path_id,
            commands,
            TrafficClass::Throughput
        ),
        ResponseStreamAttachOutcome::Attached
    );
    set_slot(&fixture.binding, udp, 77);
    fixture
        .binding
        .record_reinjected_flight(udp, &stream_data_frame_at(0, 256));
    set_audit_copy_deadline(
        &fixture.binding,
        Some(Instant::now() + Duration::from_secs(60)),
    );
    let mut ids = fixture
        .keys
        .iter()
        .copied()
        .map(|k| output_identity(&fixture.binding, k))
        .collect::<Vec<_>>();
    ids.push(output_identity(&fixture.binding, udp));
    let result = assert_prepared_view_matches_audit(&fixture.binding, &ids, Instant::now());
    assert_eq!(result.2, [1700, 1700, 256]);
    assert_eq!(
        result.0,
        [OffsetRange {
            start: 0,
            end: 1700
        }]
    );
}

#[test]
fn stale_prepared_view_falls_back_after_ack_and_replacement() {
    let fixture = fixture_with_tcp_outputs(1);
    let key = fixture.keys[0];
    fixture
        .binding
        .record_reinjected_flight(key, &stream_data_frame_at(0, 1024));
    let old = output_identity(&fixture.binding, key);
    let before_ack = fixture.binding.observe_prepared_copy_work(Instant::now());
    assert_eq!(before_ack.accepted_debts(&[old]).as_slice(), [1024]);
    fixture
        .binding
        .release_normalized_acked_ranges(&[OffsetRange { start: 0, end: 256 }]);
    assert_eq!(before_ack.accepted_debts(&[old]).as_slice(), [768]);
    assert_eq!(
        fixture
            .binding
            .accepted_reinjected_data_in_flight_bytes_for_outputs_at(&[old])
            .as_slice(),
        [768]
    );

    let before_full_ack = fixture.binding.observe_prepared_copy_work(Instant::now());
    fixture
        .binding
        .release_normalized_acked_ranges(&[OffsetRange {
            start: 256,
            end: 1024,
        }]);
    assert_eq!(before_full_ack.accepted_debts(&[old]).as_slice(), [0]);
    fixture
        .binding
        .record_reinjected_flight(key, &stream_data_frame_at(4096, 512));
    let predecessor = output_identity(&fixture.binding, key);
    let stale = fixture.binding.observe_prepared_copy_work(Instant::now());
    let instance = with_output_entry_for_key(&fixture.binding, key, |e| e.path_instance_id);
    let incarnation = match fixture.binding.begin_path_detach(key, instance) {
        Some(ResponsePathDetachOutcome::Begun(i)) => i,
        other => panic!("detach: {other:?}"),
    };
    let (commands, _rx) = reliable_path_command_channels(8);
    assert_eq!(
        fixture.binding.attach(
            key.underlay,
            key.path_id,
            commands,
            TrafficClass::Throughput
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let successor = output_identity(&fixture.binding, key);
    fixture
        .binding
        .complete_path_detach(key, instance, incarnation);
    assert_ne!(predecessor.incarnation, successor.incarnation);
    assert_eq!(stale.accepted_debts(&[successor]).as_slice(), [0]);
    let fresh = assert_prepared_view_matches_audit(&fixture.binding, &[successor], Instant::now());
    assert!(fresh.0.is_empty());
    assert_eq!(fresh.2, [0]);
}

#[test]
fn prepared_view_preserves_poisoned_flight_query_behavior() {
    let fixture = fixture_with_tcp_outputs(1);
    let key = fixture.keys[0];
    fixture
        .binding
        .record_reinjected_flight(key, &stream_data_frame_at(0, 512));
    let id = output_identity(&fixture.binding, key);
    let view = fixture.binding.observe_prepared_copy_work(Instant::now());
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = fixture.binding.flights.lock().unwrap();
        panic!("isolated poison");
    }));
    assert!(poisoned.is_err());
    // Call the view first while only the flight mutex is poisoned; its panic
    // must arise from the existing fresh-query fallback, not output-lock poison.
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| view.accepted_debts(&[id])))
            .is_err()
    );

    let fresh_fixture = fixture_with_tcp_outputs(1);
    let fresh_id = output_identity(&fresh_fixture.binding, fresh_fixture.keys[0]);
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = fresh_fixture.binding.flights.lock().unwrap();
        panic!("isolated poison");
    }));
    assert!(poisoned.is_err());
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            fresh_fixture
                .binding
                .accepted_reinjected_data_in_flight_bytes_for_outputs_at(&[fresh_id])
        }))
        .is_err()
    );
}

#[test]
fn generated_prepared_copy_observations_match_byte_ownership_after_ack() {
    let mut seed = 0x48be_10d3_79ac_512fu64;
    let mut next = || {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        seed >> 32
    };
    for case in 0..96 {
        let width = [1, 4, 8][case % 3];
        let fixture = fixture_with_tcp_outputs(width);
        for key in fixture.keys.iter().copied() {
            set_slot(&fixture.binding, key, (next() % 3) as u16);
        }
        for _ in 0..16 {
            let key = fixture.keys[next() as usize % width];
            let start = next() % 128;
            let bytes = (next() % 32 + 1) as usize;
            fixture
                .binding
                .record_reinjected_flight(key, &stream_data_frame_at(start, bytes));
        }
        let at = Instant::now();
        configure_fixture_copies(&fixture.binding, |flight| {
            flight.reinjection_suppression_deadline = match next() % 3 {
                0 => None,
                1 => Some(at),
                _ => Some(at + Duration::from_secs(1)),
            };
        });
        let mut ids = fixture
            .keys
            .iter()
            .copied()
            .map(|key| output_identity(&fixture.binding, key))
            .collect::<Vec<_>>();
        ids.push(ids[0]);
        ids.push(ServerReinjectionOutputIdentity {
            key: ids[0].key,
            incarnation: u64::MAX,
        });
        assert_prepared_view_matches_audit(&fixture.binding, &ids, at);
        let prior = fixture.binding.observe_prepared_copy_work(at);
        let ack_start = next() % 96;
        fixture
            .binding
            .release_normalized_acked_ranges(&[OffsetRange {
                start: ack_start,
                end: ack_start + 32,
            }]);
        let (_, _, debt) = assert_prepared_view_matches_audit(&fixture.binding, &ids, at);
        assert_eq!(prior.accepted_debts(&ids).as_slice(), debt);
    }
}

// Append after append_ready_tests.rs in tests_slot_debt.rs.

fn fold_observation_audit(mut sum: u64, ranges: &[OffsetRange], deadline: Option<Instant>) -> u64 {
    sum = sum
        .rotate_left(5)
        .wrapping_add(ranges.len() as u64)
        .wrapping_add(deadline.is_some() as u64);
    for r in ranges {
        sum = sum.rotate_left(7).wrapping_add(r.start).wrapping_add(r.end);
    }
    sum
}

fn fold_debt_audit(mut sum: u64, debts: &[usize]) -> u64 {
    for (i, debt) in debts.iter().copied().enumerate() {
        sum = sum
            .rotate_left(7)
            .wrapping_add(debt as u64)
            .wrapping_add(i as u64 + 1);
    }
    sum
}

fn time_observation_audit(
    binding: &ResponseStreamBinding,
    ids: &[ServerReinjectionOutputIdentity],
    at: Instant,
    iterations: usize,
    joined: bool,
    consume_debt: bool,
) -> (Duration, u64) {
    let started = Instant::now();
    let mut sum = 0x9e37_79b9_u64;
    for _ in 0..iterations {
        if joined {
            let view = binding.observe_prepared_copy_work(at);
            sum = fold_observation_audit(sum, black_box(&view.covered), view.next_deadline);
            if consume_debt {
                let debts = view.accepted_debts(ids);
                sum = fold_debt_audit(sum, black_box(debts.as_slice()));
            }
        } else {
            let (ranges, deadline) = reference_prepared_coverage_audit(binding, at);
            sum = fold_observation_audit(sum, black_box(&ranges), deadline);
            if consume_debt {
                let debts = binding.accepted_reinjected_data_in_flight_bytes_for_outputs_at(ids);
                sum = fold_debt_audit(sum, black_box(debts.as_slice()));
            }
        }
    }
    (started.elapsed(), black_box(sum))
}

fn current_domain_count_audit(binding: &ResponseStreamBinding) -> usize {
    let outputs = binding.outputs.lock().expect("test outputs");
    let mut domains = Vec::new();
    for entry in &outputs.entries {
        let domain = (entry.key.underlay, entry.configured_slot);
        if !domains.contains(&domain) {
            domains.push(domain);
        }
    }
    domains.len()
}

fn copy_count_audit(binding: &ResponseStreamBinding) -> usize {
    binding
        .flights
        .lock()
        .expect("test flights")
        .values()
        .flatten()
        .filter(|f| f.kind == crate::model::work::CarrierWorkKind::ReinjectedData)
        .count()
}

#[test]
#[ignore = "manual complete coverage/debt timing probe; run with --ignored --nocapture"]
fn benchmark_prepared_copy_complete_observation_audit() {
    for (copies, shared_slot) in [(false, false), (true, false), (true, true)] {
        for rows in [1usize, 16, 256, 4096] {
            let fixture = fixture_with_tcp_outputs(4);
            if shared_slot {
                for key in fixture.keys.iter().copied() {
                    set_slot(&fixture.binding, key, 77);
                }
            }
            seed_audit_rows(&fixture, rows, copies);
            let at = Instant::now();
            set_audit_copy_deadline(&fixture.binding, Some(at + Duration::from_secs(3600)));
            let ids = fixture
                .keys
                .iter()
                .copied()
                .map(|k| output_identity(&fixture.binding, k))
                .collect::<Vec<_>>();
            let retained = retained_flight_count(&fixture.binding);
            let copy_rows = copy_count_audit(&fixture.binding);
            let domains = current_domain_count_audit(&fixture.binding);
            // Keep each block near a stable amount of ledger work while ensuring
            // enough repetitions for the one-row cases.
            let iterations = (2_000_000usize / rows).clamp(128, 8192);
            for (joined, consume) in [(false, true), (true, true), (false, false), (true, false)] {
                time_observation_audit(&fixture.binding, &ids, at, 2, joined, consume); // warmup
            }
            let mut full_checksum = None;
            let mut unused_checksum = None;
            for round in 0..4 {
                let full_order = if round % 2 == 0 {
                    [false, true, true, false]
                } else {
                    [true, false, false, true]
                };
                for (position, joined) in full_order.into_iter().enumerate() {
                    let (elapsed, sum) = time_observation_audit(
                        &fixture.binding,
                        &ids,
                        at,
                        iterations,
                        joined,
                        true,
                    );
                    if let Some(expected) = full_checksum {
                        assert_eq!(sum, expected);
                    } else {
                        full_checksum = Some(sum);
                    }
                    eprintln!(
                        "recovery-observation rows={rows} retained={retained} members={} copies={copy_rows} domains={domains} slot_shape={} debt=used round={} position={} arm={} iterations={iterations} raw_ns_per_observation={:.1} checksum={sum}",
                        ids.len(),
                        if shared_slot { "shared" } else { "distinct" },
                        round + 1,
                        position + 1,
                        if joined { "joined" } else { "reference" },
                        elapsed.as_secs_f64() * 1e9 / iterations as f64
                    );
                }
                let unused_order = if round % 2 == 0 {
                    [false, true, true, false]
                } else {
                    [true, false, false, true]
                };
                for (position, joined) in unused_order.into_iter().enumerate() {
                    let (elapsed, sum) = time_observation_audit(
                        &fixture.binding,
                        &ids,
                        at,
                        iterations,
                        joined,
                        false,
                    );
                    if let Some(expected) = unused_checksum {
                        assert_eq!(sum, expected);
                    } else {
                        unused_checksum = Some(sum);
                    }
                    eprintln!(
                        "recovery-observation rows={rows} retained={retained} members={} copies={copy_rows} domains={domains} slot_shape={} debt=unused round={} position={} arm={} iterations={iterations} raw_ns_per_observation={:.1} checksum={sum}",
                        ids.len(),
                        if shared_slot { "shared" } else { "distinct" },
                        round + 1,
                        position + 1,
                        if joined { "joined" } else { "reference" },
                        elapsed.as_secs_f64() * 1e9 / iterations as f64
                    );
                }
            }
        }
    }

    // Isolate generation-miss fallback cost: create the view, mutate accepted
    // copy geometry once to advance the generation, then time stale-view reads
    // against the same fresh batched query. The mutation is outside both timers.
    let fixture = fixture_with_tcp_outputs(4);
    for key in fixture.keys.iter().copied() {
        set_slot(&fixture.binding, key, 77);
    }
    seed_audit_rows(&fixture, 16, true);
    let at = Instant::now();
    let ids = fixture
        .keys
        .iter()
        .copied()
        .map(|k| output_identity(&fixture.binding, k))
        .collect::<Vec<_>>();
    let stale_view = fixture.binding.observe_prepared_copy_work(at);
    fixture
        .binding
        .record_reinjected_flight(fixture.keys[0], &stream_data_frame_at(1_000_000, 512));
    let iterations = 8192;
    let mut expected = None;
    for round in 0..4 {
        let order = if round % 2 == 0 {
            [false, true, true, false]
        } else {
            [true, false, false, true]
        };
        for (position, via_view) in order.into_iter().enumerate() {
            let started = Instant::now();
            let mut sum = 0x9e37_79b9_u64;
            for _ in 0..iterations {
                let debts = if via_view {
                    stale_view.accepted_debts(&ids)
                } else {
                    fixture
                        .binding
                        .accepted_reinjected_data_in_flight_bytes_for_outputs_at(&ids)
                };
                sum = fold_debt_audit(sum, black_box(debts.as_slice()));
            }
            let elapsed = started.elapsed();
            if let Some(value) = expected {
                assert_eq!(sum, value);
            } else {
                expected = Some(sum);
            }
            eprintln!(
                "recovery-observation case=generation-miss members={} retained={} round={} position={} arm={} iterations={iterations} raw_ns_per_observation={:.1} checksum={sum}",
                ids.len(),
                retained_flight_count(&fixture.binding),
                round + 1,
                position + 1,
                if via_view {
                    "stale-view-fallback"
                } else {
                    "fresh-batch"
                },
                elapsed.as_secs_f64() * 1e9 / iterations as f64
            );
        }
    }
}
