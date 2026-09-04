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
use crate::model::work::CarrierWorkKind;
use crate::protocol::{ConfiguredMemberSlot, OffsetRange, PathId, UnderlayProtocol};
use crate::runtime::path::commands::{
    ReliablePathCommand, reliable_path_command_channels, try_recv_reliable_path_command,
};
use crate::runtime::sender::ServerReinjectionOutputIdentity;
use crate::scheduler::TrafficClass;
use crate::transport::RateHint;
use std::collections::BTreeMap;

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
        original.original_data_acked_bytes, 0,
        "qualification range precision must not change rate-sample aggregation"
    );
    assert!(ledger.holds());
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
            .data_ack_recovery_candidates(4096, TrafficClass::Throughput)
            .is_empty(),
        "a fresh tail beginning at the complete ACK horizon is not an authoritative omission",
    );
    let candidates = binding.data_ack_recovery_candidates(8192, TrafficClass::Throughput);
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
        binding.data_ack_recovery_candidates(u64::MAX, TrafficClass::Throughput);
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
    let candidates = binding.data_ack_recovery_candidates(u64::MAX, TrafficClass::Throughput);
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
