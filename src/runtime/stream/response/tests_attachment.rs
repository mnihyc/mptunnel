use super::super::next_server_carrier_path_instance_id;
use super::super::test_support::{binding_for_underlay, output_entry_for_key, stream_data_frame};
use super::{
    ResponseOutputAttachment, ResponseOutputAttachmentState, ResponsePathDetachOutcome,
    ResponseStreamAttachOutcome,
};
use crate::model::path::{CarrierPathKey, PathPolicy};
use crate::protocol::{Frame, OffsetRange, PathId, PathUsage, StreamId, UnderlayProtocol};
use crate::runtime::path::commands::{
    ReliablePathCommand, reliable_path_command_channels, try_recv_reliable_path_priority_command,
};
use crate::runtime::sender::ServerReinjectionOutputIdentity;
use crate::scheduler::TrafficClass;

fn alternate_key(underlay: UnderlayProtocol) -> CarrierPathKey {
    CarrierPathKey {
        underlay,
        path_id: PathId(1),
    }
}

#[test]
fn live_output_tracks_real_carrier_receiver_lifetime_and_reattachment() {
    let (binding, _initial, initial_receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    assert!(binding.has_live_output());

    drop(initial_receivers);
    assert!(!binding.has_live_output());

    let alternate = alternate_key(UnderlayProtocol::Udp);
    let (commands, _receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            alternate.underlay,
            alternate.path_id,
            commands,
            TrafficClass::Latency,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    assert!(binding.has_live_output());
}

#[test]
fn output_load_registration_tracks_lane_change_and_withdrawal() {
    let (binding, key, _receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let entry = output_entry_for_key(&binding, key);
    assert_eq!(entry.commands.active_flow_counts(), (1, 0));

    binding.set_lane(TrafficClass::Latency);
    assert_eq!(entry.commands.active_flow_counts(), (1, 1));

    let incarnation = binding
        .begin_path_detach(key, entry.path_instance_id)
        .map(|outcome| match outcome {
            ResponsePathDetachOutcome::Begun(incarnation) => incarnation,
            ResponsePathDetachOutcome::Pending(_) => panic!("first detach must begin withdrawal"),
        })
        .expect("attached output begins withdrawal");
    assert_eq!(entry.commands.active_flow_counts(), (0, 0));

    binding.complete_path_detach(key, entry.path_instance_id, incarnation);
    assert_eq!(entry.commands.active_flow_counts(), (0, 0));
}

#[test]
fn exact_carrier_cannot_reattach_while_ordered_detach_is_pending() {
    let (binding, key, _receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let entry = output_entry_for_key(&binding, key);
    let incarnation = binding
        .begin_path_detach(key, entry.path_instance_id)
        .map(|outcome| match outcome {
            ResponsePathDetachOutcome::Begun(incarnation) => incarnation,
            ResponsePathDetachOutcome::Pending(_) => panic!("first detach must begin withdrawal"),
        })
        .expect("begin ordered detach");
    assert_eq!(
        binding.begin_path_detach(key, entry.path_instance_id),
        Some(ResponsePathDetachOutcome::Pending(incarnation)),
        "repeated control input must share the existing ordered detach"
    );
    let (commands, _replacement_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach_output(ResponseOutputAttachment {
            key,
            path_instance_id: entry.path_instance_id,
            local_policy: PathPolicy::default(),
            commands,
            state: ResponseOutputAttachmentState::default(),
        },),
        ResponseStreamAttachOutcome::RejectedDuplicateLiveOutput,
        "pending ordered lifecycle state must stay one-per-carrier incarnation"
    );
    assert_eq!(
        binding
            .outputs
            .lock()
            .expect("response outputs")
            .detaching
            .len(),
        1
    );
    binding.complete_path_detach(key, entry.path_instance_id, incarnation);
}

#[test]
fn neutral_attach_adds_one_output_without_publishing_protocol_frames() {
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
    assert!(try_recv_reliable_path_priority_command(&mut receivers).is_none());
}

#[test]
fn retained_max_data_retries_only_blocked_outputs_and_replays_on_attach() {
    let stream_id = StreamId(7);
    let (binding, initial, mut initial_receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let initial_entry = output_entry_for_key(&binding, initial);
    for nonce in 0..8 {
        initial_entry
            .commands
            .try_enqueue_admitted_frame(Frame::Ping { nonce }, TrafficClass::Control)
            .expect("fill initial output priority queue");
    }
    let alternate = alternate_key(UnderlayProtocol::Udp);
    let (alternate_commands, mut alternate_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            alternate.underlay,
            alternate.path_id,
            alternate_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );

    let first = binding.retry_pending_max_data(stream_id);
    let published = first
        .published_offset
        .expect("available output publishes retained initial credit");
    assert!(
        !first.pending,
        "the opening attachment already accepted the initial grant"
    );
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut alternate_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamMaxData {
            stream_id: received_stream_id,
            max_offset,
        })) if received_stream_id == stream_id && max_offset == published
    ));

    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut initial_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::Ping { nonce: 0 }))
    ));
    let retry = binding.retry_pending_max_data(stream_id);
    assert_eq!(retry.published_offset, None);
    assert!(!retry.pending);
    for nonce in 1..8 {
        assert!(matches!(
            try_recv_reliable_path_priority_command(&mut initial_receivers),
            Some(ReliablePathCommand::SendFrame(Frame::Ping {
                nonce: received
            })) if received == nonce
        ));
    }
    assert!(
        try_recv_reliable_path_priority_command(&mut initial_receivers).is_none(),
        "the initial attachment must not receive its already accepted opening grant again"
    );
    assert!(
        try_recv_reliable_path_priority_command(&mut alternate_receivers).is_none(),
        "an already-published output must not receive an unchanged duplicate"
    );
}

#[test]
fn sole_surviving_response_output_is_restored_from_stale_placement() {
    let (binding, initial, _initial_receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let initial_entry = output_entry_for_key(&binding, initial);
    let alternate = alternate_key(UnderlayProtocol::Udp);
    let (alternate_commands, alternate_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            alternate.underlay,
            alternate.path_id,
            alternate_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let initial_identity = ServerReinjectionOutputIdentity {
        key: initial,
        incarnation: initial_entry.incarnation,
    };
    assert!(binding.mark_output_stale(initial_identity));
    assert!(
        binding
            .sender_path_targets(TrafficClass::Throughput, 1)
            .into_iter()
            .find(|target| target.observation.key == initial)
            .expect("initial output remains attached")
            .observation
            .stale_for_original_data
    );

    drop(alternate_receivers);
    let targets = binding.sender_path_targets(TrafficClass::Throughput, 1);
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].observation.key, initial);
    assert!(
        !targets[0].observation.stale_for_original_data,
        "the sole live survivor regains OriginalData and reinjection eligibility"
    );
}

#[test]
fn retained_ack_uses_updates_only_for_caught_up_outputs() {
    let stream_id = StreamId(7);
    let (binding, _initial, mut initial_receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let initial_snapshot = vec![Frame::StreamAck {
        stream_id,
        complete: true,
        ranges: vec![OffsetRange { start: 0, end: 8 }],
    }];
    let first = binding.publish_ack(1, &initial_snapshot, &initial_snapshot);
    assert!(first.published);
    assert!(!first.pending);
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut initial_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck {
            complete: true,
            ranges,
            ..
        })) if ranges == vec![OffsetRange { start: 0, end: 8 }]
    ));

    let alternate = alternate_key(UnderlayProtocol::Udp);
    let (alternate_commands, mut alternate_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            alternate.underlay,
            alternate.path_id,
            alternate_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let update = vec![Frame::StreamAck {
        stream_id,
        complete: false,
        ranges: vec![OffsetRange { start: 16, end: 24 }],
    }];
    let cumulative = vec![Frame::StreamAck {
        stream_id,
        complete: true,
        ranges: vec![
            OffsetRange { start: 0, end: 8 },
            OffsetRange { start: 16, end: 24 },
        ],
    }];
    let second = binding.publish_ack(2, &update, &cumulative);
    assert!(second.published);
    assert!(!second.pending);
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut initial_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck {
            complete: false,
            ranges,
            ..
        })) if ranges == vec![OffsetRange { start: 16, end: 24 }]
    ));
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut alternate_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck {
            complete: true,
            ranges,
            ..
        })) if ranges == vec![
            OffsetRange { start: 0, end: 8 },
            OffsetRange { start: 16, end: 24 },
        ]
    ));
}

#[test]
fn retained_ack_retry_resumes_at_the_first_unaccepted_cumulative_chunk() {
    let stream_id = StreamId(7);
    let (binding, initial, mut receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let entry = output_entry_for_key(&binding, initial);
    for nonce in 0..8 {
        entry
            .commands
            .try_enqueue_admitted_frame(Frame::Ping { nonce }, TrafficClass::Control)
            .expect("fill response priority queue");
    }
    let cumulative = vec![
        Frame::StreamAck {
            stream_id,
            complete: false,
            ranges: vec![OffsetRange { start: 0, end: 8 }],
        },
        Frame::StreamAck {
            stream_id,
            complete: false,
            ranges: vec![OffsetRange { start: 16, end: 24 }],
        },
    ];
    let first = binding.publish_ack(1, &cumulative, &cumulative);
    assert!(!first.published);
    assert!(first.pending);

    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::Ping { nonce: 0 }))
    ));
    let partial = binding.retry_pending_ack(1, &cumulative);
    assert!(partial.accepted);
    assert!(!partial.published);
    assert!(partial.pending);
    for nonce in 1..8 {
        assert!(matches!(
            try_recv_reliable_path_priority_command(&mut receivers),
            Some(ReliablePathCommand::SendFrame(Frame::Ping { nonce: received }))
                if received == nonce
        ));
    }
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck { ranges, .. }))
            if ranges == vec![OffsetRange { start: 0, end: 8 }]
    ));

    let complete = binding.retry_pending_ack(1, &cumulative);
    assert!(complete.accepted);
    assert!(complete.published);
    assert!(!complete.pending);
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck { ranges, .. }))
            if ranges == vec![OffsetRange { start: 16, end: 24 }]
    ));
    let fenced = binding.retry_pending_ack(1, &cumulative);
    assert!(!fenced.accepted);
    assert!(fenced.published);
    assert!(!fenced.pending);
}

#[test]
fn retained_ack_publication_status_excludes_a_detached_fence() {
    let stream_id = StreamId(7);
    let (binding, initial, _initial_receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let cumulative = vec![Frame::StreamAck {
        stream_id,
        complete: true,
        ranges: vec![OffsetRange { start: 0, end: 8 }],
    }];
    assert!(binding.publish_ack(1, &cumulative, &cumulative).published);

    let alternate = alternate_key(UnderlayProtocol::Udp);
    let (alternate_commands, _alternate_receivers) = reliable_path_command_channels(8);
    for nonce in 0..8 {
        alternate_commands
            .try_enqueue_admitted_frame(Frame::Ping { nonce }, TrafficClass::Control)
            .expect("fill alternate response priority queue");
    }
    assert_eq!(
        binding.attach(
            alternate.underlay,
            alternate.path_id,
            alternate_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached
    );
    let mixed = binding.retry_pending_ack(1, &cumulative);
    assert!(mixed.published, "the original live output remains fenced");
    assert!(mixed.pending, "the new output still needs cumulative state");

    let initial_entry = output_entry_for_key(&binding, initial);
    binding.detach(initial, &initial_entry.commands);
    let only_blocked_output = binding.retry_pending_ack(1, &cumulative);
    assert!(!only_blocked_output.published);
    assert!(only_blocked_output.pending);
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
    assert!(try_recv_reliable_path_priority_command(&mut receivers).is_none());
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
