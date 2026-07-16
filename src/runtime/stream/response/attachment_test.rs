use super::super::next_server_carrier_path_instance_id;
use super::super::test_support::{binding_for_underlay, output_entry_for_key, stream_data_frame};
use super::ResponseStreamAttachOutcome;
use crate::model::path::CarrierPathKey;
use crate::protocol::{Frame, OffsetRange, PathId, PathUsage, UnderlayProtocol};
use crate::runtime::path::commands::{
    ReliablePathCommand, reliable_path_command_channels, try_recv_reliable_path_priority_command,
};
use crate::scheduler::TrafficClass;

fn alternate_key(underlay: UnderlayProtocol) -> CarrierPathKey {
    CarrierPathKey {
        underlay,
        path_id: PathId(1),
    }
}

#[test]
fn neutral_attach_adds_one_output_and_validates_the_carrier() {
    let (binding, initial, _initial_receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let alternate = alternate_key(UnderlayProtocol::Udp);
    let (commands, mut receivers) = reliable_path_command_channels(8);

    assert_eq!(
        binding.attach(
            alternate.underlay,
            alternate.path_id,
            commands,
            TrafficClass::Latency,
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let outputs = binding.outputs.lock().expect("test response outputs lock");
    assert_eq!(outputs.entries.len(), 2);
    assert!(outputs.entries.iter().any(|entry| entry.key == initial));
    assert!(outputs.entries.iter().any(|entry| entry.key == alternate));
    drop(outputs);
    assert_eq!(binding.lane(), TrafficClass::Latency);
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData {
            path_id,
            payload,
            ..
        })) if path_id == alternate.path_id && !payload.is_empty()
    ));
}

#[test]
fn request_feedback_ingress_is_non_owning_and_exact_to_path_instance() {
    let (binding, initial, _initial_receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let feedback = alternate_key(UnderlayProtocol::Tcp);
    let (commands, _receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            feedback.underlay,
            feedback.path_id,
            commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let feedback_instance = output_entry_for_key(&binding, feedback).path_instance_id;

    assert!(binding.record_request_feedback_ingress(feedback, feedback_instance));
    let targets = binding.sender_path_targets(TrafficClass::Control, 1);
    assert!(targets.iter().any(|target| {
        target.observation.key == feedback && target.observation.is_request_feedback
    }));
    assert!(targets.iter().any(|target| {
        target.observation.key == initial && !target.observation.is_request_feedback
    }));

    binding.detach_path_instance(feedback, feedback_instance);
    let (replacement_commands, _replacement_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            feedback.underlay,
            feedback.path_id,
            replacement_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    assert_ne!(
        output_entry_for_key(&binding, feedback).path_instance_id,
        feedback_instance
    );
    assert!(!binding.record_request_feedback_ingress(feedback, feedback_instance));
    assert!(
        binding
            .sender_path_targets(TrafficClass::Control, 1)
            .iter()
            .all(|target| !target.observation.is_request_feedback)
    );
}

#[test]
fn duplicate_live_output_preserves_identity_and_rejects_a_different_channel() {
    let (binding, _initial, _initial_receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let key = alternate_key(UnderlayProtocol::Udp);
    let (commands, mut receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            key.underlay,
            key.path_id,
            commands.clone(),
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let proof = try_recv_reliable_path_priority_command(&mut receivers);
    assert!(matches!(
        proof,
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
    ));
    let before = output_entry_for_key(&binding, key);

    assert_eq!(
        binding.attach(key.underlay, key.path_id, commands, TrafficClass::Latency,),
        ResponseStreamAttachOutcome::Attached
    );
    let after = output_entry_for_key(&binding, key);
    assert_eq!(after.path_instance_id, before.path_instance_id);
    assert_eq!(after.incarnation, before.incarnation);
    assert_eq!(binding.lane(), TrafficClass::Latency);
    assert!(try_recv_reliable_path_priority_command(&mut receivers).is_none());

    let (duplicate_commands, _duplicate_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            key.underlay,
            key.path_id,
            duplicate_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::RejectedDuplicateLiveOutput
    );
    assert_eq!(
        output_entry_for_key(&binding, key).incarnation,
        before.incarnation
    );
}

#[test]
fn closed_output_replacement_resets_evidence_and_cannot_credit_old_flights() {
    let (binding, _initial, _initial_receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let key = alternate_key(UnderlayProtocol::Udp);
    let (commands, receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            key.underlay,
            key.path_id,
            commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let frame = stream_data_frame(4096);
    binding.record_original_flight(key, &frame);
    let before = output_entry_for_key(&binding, key);
    drop(receivers);

    let (replacement_commands, _replacement_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            key.underlay,
            key.path_id,
            replacement_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::ReplacedClosedOutput
    );
    let replacement = output_entry_for_key(&binding, key);
    assert_ne!(replacement.path_instance_id, before.path_instance_id);
    assert_ne!(replacement.incarnation, before.incarnation);
    assert_eq!(replacement.bytes_in_flight, 0);
    assert_eq!(replacement.original_data_in_flight_bytes, 0);
    assert!(replacement.local_path_metrics.is_none());
    assert!(replacement.peer_path_metrics.is_none());
    assert!(replacement.peer_usage.is_none());

    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: 4096,
    }]);
    let replacement = output_entry_for_key(&binding, key);
    assert_eq!(replacement.original_data_acked_bytes, 0);
    assert_eq!(replacement.delivery_samples, 0);
}

#[test]
fn peer_path_usage_rejects_stale_sequences_and_wrong_instances() {
    let (binding, key, _receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let path_instance_id = output_entry_for_key(&binding, key).path_instance_id;
    let generation = binding.response_model_generation();

    assert!(binding.update_peer_path_usage_for_instance(
        key,
        path_instance_id,
        2,
        PathUsage::Backup,
    ));
    assert_eq!(
        binding
            .sender_path_targets(TrafficClass::Throughput, 1)
            .first()
            .expect("response target")
            .observation
            .snapshot
            .peer_usage,
        Some(PathUsage::Backup)
    );
    assert!(!binding.update_peer_path_usage_for_instance(
        key,
        path_instance_id,
        1,
        PathUsage::Available,
    ));
    assert!(!binding.update_peer_path_usage_for_instance(
        key,
        next_server_carrier_path_instance_id(),
        3,
        PathUsage::Available,
    ));
    assert!(binding.update_peer_path_usage_for_instance(
        key,
        path_instance_id,
        3,
        PathUsage::Available,
    ));
    assert_eq!(binding.response_model_generation(), generation + 2);
    let entry = output_entry_for_key(&binding, key);
    assert_eq!(entry.peer_usage, Some(PathUsage::Available));
    assert_eq!(entry.peer_usage_sequence, Some(3));
}
