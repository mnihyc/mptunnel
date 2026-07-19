use super::super::test_support::{binding_for_underlay, stream_data_frame_at};
use super::*;
use crate::model::work::CarrierWorkKind;
use crate::protocol::{OffsetRange, PathId, UnderlayProtocol};
use crate::runtime::path::commands::reliable_path_command_channels;
use crate::scheduler::TrafficClass;
use std::collections::BTreeMap;

fn key(underlay: UnderlayProtocol, path_id: u16) -> CarrierPathKey {
    CarrierPathKey {
        underlay,
        path_id: PathId(path_id),
    }
}

fn flight(key: CarrierPathKey, end: u64, bytes: usize, kind: CarrierWorkKind) -> CarrierPathFlight {
    CarrierPathFlight::fixed_output(key, end, bytes, Instant::now(), kind)
}

fn range(start: u64, end: u64) -> OffsetRange {
    OffsetRange::new(start, end).expect("valid test range")
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

#[test]
fn data_ack_hole_advances_only_after_the_prefix_arrives() {
    let path = key(UnderlayProtocol::Tcp, 0);
    let mut ordering = ResponseAckOrderingState::default();
    let second = CarrierPathReleasedFlight {
        flight: flight(path, 8192, 4096, CarrierWorkKind::OriginalData),
        path_proving: true,
    };

    let update = ordering.apply_normalized_ack(&[range(4096, 8192)], &[(4096, second)]);
    assert_eq!(update.contiguous_frontier, 0);
    assert_eq!(ordering.acked_hole_bytes(), 4096);

    let first = CarrierPathReleasedFlight {
        flight: flight(path, 4096, 4096, CarrierWorkKind::OriginalData),
        path_proving: true,
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
fn only_live_recent_reinjection_suppresses_another_attempt() {
    let path = key(UnderlayProtocol::Udp, 1);
    let mut flights = BTreeMap::from([(
        0,
        vec![flight(path, 4096, 4096, CarrierWorkKind::ReinjectedData)],
    )]);
    let retry_after = Duration::from_millis(100);

    assert!(product_flights_have_recent_reinjection_overlap(
        &flights,
        0,
        4096,
        Instant::now(),
        retry_after,
        |candidate| candidate == path,
    ));
    assert!(!product_flights_have_recent_reinjection_overlap(
        &flights,
        0,
        4096,
        Instant::now(),
        retry_after,
        |_| false,
    ));

    flights.get_mut(&0).unwrap()[0].sent_at = Instant::now() - Duration::from_millis(200);
    assert!(!product_flights_have_recent_reinjection_overlap(
        &flights,
        0,
        4096,
        Instant::now(),
        retry_after,
        |_| true,
    ));
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
