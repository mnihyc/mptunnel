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
