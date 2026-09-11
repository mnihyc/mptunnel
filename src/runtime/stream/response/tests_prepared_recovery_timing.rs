use super::super::test_support::{binding_for_underlay, stream_data_frame_at};
use super::*;

#[test]
fn prepared_recovery_clock_survives_sparse_ack_and_later_rtt_growth() {
    let (binding, owner, _receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    binding.record_original_flight(owner, &stream_data_frame_at(0, 4096));
    binding.age_original_flights_for_test(Duration::from_secs(10));
    let snapshot = binding.sender_path_targets(TrafficClass::Throughput, 1)[0]
        .observation
        .snapshot;
    let (first, boundary) = binding
        .observe_prepared_recovery_timing(
            OffsetRange {
                start: 0,
                end: 4096,
            },
            |_| Some(snapshot),
        )
        .unwrap();
    assert_eq!(boundary, 4096);
    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 1024,
        end: 2048,
    }]);
    let mut slower = snapshot;
    slower.srtt_ms = 5_000.0;
    slower.jitter_ms = 1_000.0;
    let (fragment, boundary) = binding
        .observe_prepared_recovery_timing(
            OffsetRange {
                start: 2048,
                end: 4096,
            },
            |_| Some(slower),
        )
        .unwrap();
    assert_eq!(
        fragment, first,
        "a surviving fragment keeps its accepted assignment clock"
    );
    assert_eq!(boundary, 4096);
    binding.record_original_flight(owner, &stream_data_frame_at(4096, 4096));
    let (with_young_suffix, boundary) = binding
        .observe_prepared_recovery_timing(
            OffsetRange {
                start: 2048,
                end: 8192,
            },
            |_| Some(slower),
        )
        .unwrap();
    assert_eq!(
        boundary, 4096,
        "a due prefix can be reconsidered at the real assignment boundary"
    );
    assert!(with_young_suffix.assignment_at > first.assignment_at);
    assert!(with_young_suffix.fallback_at > first.fallback_at);
    assert_eq!(
        binding.observe_prepared_recovery_timing(
            OffsetRange {
                start: 0,
                end: 4096
            },
            |_| Some(snapshot)
        ),
        None,
        "the sparse ACK hole is not retained Original authority"
    );
}

#[test]
fn completion_copy_opportunity_survives_expiry_detach_and_sparse_ack() {
    let (binding, owner, _receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let (commands, _copy_receivers) =
        crate::runtime::path::commands::reliable_path_command_channels(8);
    let alternate = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: crate::protocol::PathId(8),
    };
    binding.attach(
        alternate.underlay,
        alternate.path_id,
        commands.clone(),
        TrafficClass::Latency,
    );
    binding.record_original_flight(owner, &stream_data_frame_at(0, 4096));
    binding.record_reinjected_flight(alternate, &stream_data_frame_at(1024, 1024));
    let range = OffsetRange {
        start: 0,
        end: 4096,
    };
    assert_eq!(
        binding.uncopied_completion_prefix(range),
        Some(OffsetRange {
            start: 0,
            end: 1024
        })
    );
    // Expiry restores normal recovery only. Detach does not erase wire debt.
    for entries in binding.flights.lock().unwrap().values_mut() {
        for flight in entries {
            if flight.kind == CarrierWorkKind::ReinjectedData {
                flight.reinjection_suppression_deadline = Some(Instant::now());
            }
        }
    }
    binding.detach(alternate, &commands);
    assert!(
        binding
            .prepared_recovery_coverage(Instant::now())
            .0
            .is_empty()
    );
    assert_eq!(
        binding.uncopied_completion_prefix(OffsetRange {
            start: 1024,
            end: 4096
        }),
        None
    );
    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 1280,
        end: 1536,
    }]);
    for range in [
        OffsetRange {
            start: 1024,
            end: 1280,
        },
        OffsetRange {
            start: 1536,
            end: 2048,
        },
    ] {
        assert_eq!(binding.uncopied_completion_prefix(range), None);
    }
    assert_eq!(
        binding.uncopied_completion_prefix(OffsetRange {
            start: 2048,
            end: 4096
        }),
        Some(OffsetRange {
            start: 2048,
            end: 4096
        })
    );
}
