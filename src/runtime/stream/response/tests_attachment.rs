use super::super::next_server_carrier_path_instance_id;
use super::super::test_support::{
    binding_for_underlay, qualify_product_assignment, stream_data_frame, with_output_entry_for_key,
};
use super::{
    ResponseOutputAttachment, ResponseOutputAttachmentState, ResponsePathDetachOutcome,
    ResponseProductRateEpoch, ResponseStreamAttachOutcome,
};
use crate::model::path::{CarrierPathKey, PathPolicy};
use crate::mux::MuxLimits;
use crate::protocol::{
    ConfiguredMemberSlot, Frame, OffsetRange, PathId, PathUsage, StreamId, UnderlayProtocol,
};
use crate::runtime::RuntimeError;
use crate::runtime::path::commands::{
    ReliablePathCommand, reliable_path_command_channels, try_recv_reliable_path_priority_command,
};
use crate::runtime::sender::ServerReinjectionOutputIdentity;
use crate::scheduler::TrafficClass;
use crate::transport::RateHint;
use std::time::{Duration, Instant};

fn alternate_key(underlay: UnderlayProtocol) -> CarrierPathKey {
    CarrierPathKey {
        underlay,
        path_id: PathId(1),
    }
}

#[test]
fn output_incarnation_exhaustion_fails_before_new_membership_publication() {
    let (binding, initial, _initial_receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    binding
        .outputs
        .lock()
        .expect("response outputs")
        .next_output_incarnation = Some(u64::MAX);

    let last_key = alternate_key(UnderlayProtocol::Udp);
    let (last_commands, _last_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding
            .try_attach_output(ResponseOutputAttachment {
                key: last_key,
                path_instance_id: next_server_carrier_path_instance_id(),
                configured_slot: ConfiguredMemberSlot(last_key.path_id.0),
                local_policy: PathPolicy::default(),
                startup_rate_prior: RateHint::Unknown,
                commands: last_commands,
                state: ResponseOutputAttachmentState::default(),
            })
            .expect("MAX remains one valid exact incarnation"),
        ResponseStreamAttachOutcome::Attached,
    );
    {
        let outputs = binding.outputs.lock().expect("response outputs");
        assert_eq!(outputs.next_output_incarnation, None);
        assert_eq!(
            outputs
                .entries
                .iter()
                .find(|entry| entry.key == last_key)
                .expect("MAX-incarnation output")
                .incarnation,
            u64::MAX,
        );
    }

    let entries_before = binding
        .outputs
        .lock()
        .expect("response outputs")
        .entries
        .iter()
        .map(|entry| (entry.key, entry.path_instance_id, entry.incarnation))
        .collect::<Vec<_>>();
    let membership_before = binding.output_membership_generation();
    let model_before = binding.response_model_generation();
    for path_id in [PathId(2), PathId(3)] {
        let (commands, _receivers) = reliable_path_command_channels(8);
        assert!(matches!(
            binding.try_attach_output(ResponseOutputAttachment {
                key: CarrierPathKey {
                    underlay: UnderlayProtocol::Tcp,
                    path_id,
                },
                path_instance_id: next_server_carrier_path_instance_id(),
                configured_slot: ConfiguredMemberSlot(path_id.0),
                local_policy: PathPolicy::default(),
                startup_rate_prior: RateHint::Unknown,
                commands,
                state: ResponseOutputAttachmentState::default(),
            }),
            Err(RuntimeError::ExactIdentityExhausted),
        ));
        let outputs = binding.outputs.lock().expect("response outputs");
        assert_eq!(
            outputs
                .entries
                .iter()
                .map(|entry| (entry.key, entry.path_instance_id, entry.incarnation))
                .collect::<Vec<_>>(),
            entries_before,
        );
        assert!(outputs.entries.iter().any(|entry| entry.key == initial));
        drop(outputs);
        assert_eq!(binding.output_membership_generation(), membership_before);
        assert_eq!(binding.response_model_generation(), model_before);
    }
}

#[test]
fn output_incarnation_exhaustion_preserves_closed_predecessor() {
    let (binding, key, predecessor_receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    drop(predecessor_receivers);
    let (identity_before, qualification_before, counters_before) = {
        let mut outputs = binding.outputs.lock().expect("response outputs");
        outputs.next_output_incarnation = None;
        let predecessor = outputs.entries.first().expect("closed predecessor");
        assert!(predecessor.commands.is_closed());
        (
            (predecessor.path_instance_id, predecessor.incarnation),
            predecessor.product_qualification.invariant(),
            (
                predecessor.original_data_in_flight_bytes,
                predecessor.bytes_in_flight,
                outputs.original_data_in_flight_bytes,
            ),
        )
    };
    let membership_before = binding.output_membership_generation();
    let model_before = binding.response_model_generation();

    let (commands, _receivers) = reliable_path_command_channels(8);
    assert!(matches!(
        binding.try_attach_output(ResponseOutputAttachment {
            key,
            path_instance_id: next_server_carrier_path_instance_id(),
            configured_slot: ConfiguredMemberSlot(key.path_id.0),
            local_policy: PathPolicy::default(),
            startup_rate_prior: RateHint::Unknown,
            commands,
            state: ResponseOutputAttachmentState::default(),
        }),
        Err(RuntimeError::ExactIdentityExhausted),
    ));

    let outputs = binding.outputs.lock().expect("response outputs");
    assert_eq!(outputs.entries.len(), 1);
    let predecessor = outputs.entries.first().expect("preserved predecessor");
    assert_eq!(
        (predecessor.path_instance_id, predecessor.incarnation),
        identity_before,
    );
    assert_eq!(
        predecessor.product_qualification.invariant(),
        qualification_before
    );
    assert_eq!(
        (
            predecessor.original_data_in_flight_bytes,
            predecessor.bytes_in_flight,
            outputs.original_data_in_flight_bytes,
        ),
        counters_before,
    );
    drop(outputs);
    assert_eq!(binding.output_membership_generation(), membership_before);
    assert_eq!(binding.response_model_generation(), model_before);
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
    let (commands, path_instance_id) = with_output_entry_for_key(&binding, key, |entry| {
        (entry.commands.clone(), entry.path_instance_id)
    });
    assert_eq!(commands.active_flow_counts(), (0, 0));

    binding.set_lane(TrafficClass::Latency);
    assert_eq!(commands.active_flow_counts(), (0, 0));

    binding.record_original_flight(key, &stream_data_frame(4_096));
    assert_eq!(commands.active_flow_counts(), (1, 1));

    binding.set_lane(TrafficClass::Throughput);
    assert_eq!(commands.active_flow_counts(), (1, 0));

    let incarnation = binding
        .begin_path_detach(key, path_instance_id)
        .map(|outcome| match outcome {
            ResponsePathDetachOutcome::Begun(incarnation) => incarnation,
            ResponsePathDetachOutcome::Pending(_) => panic!("first detach must begin withdrawal"),
        })
        .expect("attached output begins withdrawal");
    assert_eq!(commands.active_flow_counts(), (0, 0));

    binding.complete_path_detach(key, path_instance_id, incarnation);
    assert_eq!(commands.active_flow_counts(), (0, 0));
}

#[test]
fn exact_carrier_cannot_reattach_while_ordered_detach_is_pending() {
    let (binding, key, _receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let path_instance_id = with_output_entry_for_key(&binding, key, |entry| entry.path_instance_id);
    let incarnation = binding
        .begin_path_detach(key, path_instance_id)
        .map(|outcome| match outcome {
            ResponsePathDetachOutcome::Begun(incarnation) => incarnation,
            ResponsePathDetachOutcome::Pending(_) => panic!("first detach must begin withdrawal"),
        })
        .expect("begin ordered detach");
    assert_eq!(
        binding.begin_path_detach(key, path_instance_id),
        Some(ResponsePathDetachOutcome::Pending(incarnation)),
        "repeated control input must share the existing ordered detach"
    );
    let (commands, _replacement_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach_output(ResponseOutputAttachment {
            key,
            path_instance_id,
            configured_slot: ConfiguredMemberSlot(key.path_id.0),
            local_policy: PathPolicy::default(),
            startup_rate_prior: RateHint::Unknown,
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
    binding.complete_path_detach(key, path_instance_id, incarnation);
}

#[test]
fn successor_can_coexist_with_exact_predecessor_detach() {
    let (binding, key, _predecessor_receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let predecessor_path_instance_id =
        with_output_entry_for_key(&binding, key, |entry| entry.path_instance_id);
    let predecessor_incarnation = match binding
        .begin_path_detach(key, predecessor_path_instance_id)
        .expect("predecessor begins ordered detach")
    {
        ResponsePathDetachOutcome::Begun(incarnation) => incarnation,
        ResponsePathDetachOutcome::Pending(_) => panic!("first detach must begin withdrawal"),
    };
    let successor_instance = next_server_carrier_path_instance_id();
    let (successor_commands, _successor_receivers) = reliable_path_command_channels(8);

    assert_eq!(
        binding.attach_output(ResponseOutputAttachment {
            key,
            path_instance_id: successor_instance,
            configured_slot: ConfiguredMemberSlot(key.path_id.0),
            local_policy: PathPolicy::default(),
            startup_rate_prior: RateHint::Unknown,
            commands: successor_commands,
            state: ResponseOutputAttachmentState::default(),
        }),
        ResponseStreamAttachOutcome::Attached,
        "a distinct physical successor does not reuse predecessor ownership"
    );
    let targets = binding.sender_path_targets(TrafficClass::Throughput, 1);
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].observation.path_instance_id, successor_instance);
    assert!(binding.has_output_incarnation(key, predecessor_incarnation));

    binding.complete_path_detach(key, predecessor_path_instance_id, predecessor_incarnation);
    assert!(!binding.has_output_incarnation(key, predecessor_incarnation));
    assert_eq!(
        binding.sender_path_targets(TrafficClass::Throughput, 1)[0]
            .observation
            .path_instance_id,
        successor_instance,
        "exact predecessor cleanup cannot remove its successor"
    );
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
    let established_max_offset = 4096;
    let (binding, initial, mut initial_receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let initial_commands =
        with_output_entry_for_key(&binding, initial, |entry| entry.commands.clone());
    for nonce in 0..8 {
        initial_commands
            .try_enqueue_admitted_frame(Frame::Ping { nonce }, TrafficClass::Control)
            .expect("fill initial output priority queue");
    }
    let blocked = binding.publish_max_data(stream_id, established_max_offset);
    assert_eq!(blocked.published_offset, None);
    assert!(blocked.pending);

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

    let replay = binding.retry_pending_max_data(stream_id);
    let published = replay
        .published_offset
        .expect("available output publishes retained target-established credit");
    assert_eq!(published, established_max_offset);
    assert!(
        replay.pending,
        "the blocked opening attachment still needs the retained grant"
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
    for nonce in 1..8 {
        assert!(matches!(
            try_recv_reliable_path_priority_command(&mut initial_receivers),
            Some(ReliablePathCommand::SendFrame(Frame::Ping {
                nonce: received
            })) if received == nonce
        ));
    }
    let retry = binding.retry_pending_max_data(stream_id);
    assert_eq!(retry.published_offset, Some(established_max_offset));
    assert!(!retry.pending);
    assert!(
        matches!(
            try_recv_reliable_path_priority_command(&mut initial_receivers),
            Some(ReliablePathCommand::SendFrame(Frame::StreamMaxData {
                stream_id: received_stream_id,
                max_offset,
            })) if received_stream_id == stream_id && max_offset == established_max_offset
        ),
        "the opening attachment receives the retained grant after capacity returns"
    );
    assert!(
        try_recv_reliable_path_priority_command(&mut alternate_receivers).is_none(),
        "an already-published output must not receive an unchanged duplicate"
    );
    let settled = binding.retry_pending_max_data(stream_id);
    assert_eq!(settled.published_offset, None);
    assert!(!settled.pending);
}

#[test]
fn sole_surviving_response_output_is_restored_from_stale_placement() {
    let (binding, initial, _initial_receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let initial_incarnation =
        with_output_entry_for_key(&binding, initial, |entry| entry.incarnation);
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
        incarnation: initial_incarnation,
    };
    assert!(binding.mark_output_stale(initial_identity, TrafficClass::Throughput,));
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
        targets[0].observation.stale_for_original_data,
        "sole-survivor fallback must not erase stale evidence"
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
    let commands = with_output_entry_for_key(&binding, initial, |entry| entry.commands.clone());
    for nonce in 0..8 {
        commands
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

    let initial_commands =
        with_output_entry_for_key(&binding, initial, |entry| entry.commands.clone());
    binding.detach(initial, &initial_commands);
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
    let feedback_instance =
        with_output_entry_for_key(&binding, feedback, |entry| entry.path_instance_id);

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
        with_output_entry_for_key(&binding, feedback, |entry| entry.path_instance_id),
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
    let before = with_output_entry_for_key(&binding, key, |entry| {
        (entry.path_instance_id, entry.incarnation)
    });

    assert_eq!(
        binding.attach(key.underlay, key.path_id, commands, TrafficClass::Latency,),
        ResponseStreamAttachOutcome::Attached
    );
    with_output_entry_for_key(&binding, key, |after| {
        assert_eq!(after.path_instance_id, before.0);
        assert_eq!(after.incarnation, before.1);
    });
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
        with_output_entry_for_key(&binding, key, |entry| entry.incarnation),
        before.1
    );
}

#[test]
fn startup_rate_prior_is_immutable_for_live_channel_and_rebound_on_replacement() {
    let (binding, _initial, _initial_receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let key = alternate_key(UnderlayProtocol::Tcp);
    let first_instance = next_server_carrier_path_instance_id();
    let (commands, receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach_output(ResponseOutputAttachment {
            key,
            path_instance_id: first_instance,
            configured_slot: ConfiguredMemberSlot(key.path_id.0),
            local_policy: PathPolicy::default(),
            startup_rate_prior: RateHint::BitsPerSecond(500_000_000),
            commands: commands.clone(),
            state: ResponseOutputAttachmentState::default(),
        }),
        ResponseStreamAttachOutcome::Attached,
    );

    assert_eq!(
        binding.attach_output(ResponseOutputAttachment {
            key,
            path_instance_id: first_instance,
            configured_slot: ConfiguredMemberSlot(key.path_id.0),
            local_policy: PathPolicy::default(),
            startup_rate_prior: RateHint::BitsPerSecond(1),
            commands,
            state: ResponseOutputAttachmentState::default(),
        }),
        ResponseStreamAttachOutcome::Attached,
        "a duplicate transaction may refresh evidence but not configuration",
    );
    with_output_entry_for_key(&binding, key, |entry| {
        assert_eq!(
            entry.startup_rate_prior,
            RateHint::BitsPerSecond(500_000_000),
        );
    });
    assert_eq!(
        binding
            .sender_path_targets(TrafficClass::Throughput, 1)
            .into_iter()
            .find(|target| target.observation.key == key)
            .expect("configured response output")
            .observation
            .snapshot
            .delivery_rate_bps,
        500_000_000.0,
    );

    drop(receivers);
    let second_instance = next_server_carrier_path_instance_id();
    let (replacement_commands, _replacement_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach_output(ResponseOutputAttachment {
            key,
            path_instance_id: second_instance,
            configured_slot: ConfiguredMemberSlot(key.path_id.0),
            local_policy: PathPolicy::default(),
            startup_rate_prior: RateHint::BitsPerSecond(80_000_000),
            commands: replacement_commands,
            state: ResponseOutputAttachmentState::default(),
        }),
        ResponseStreamAttachOutcome::ReplacedClosedOutput,
    );
    with_output_entry_for_key(&binding, key, |entry| {
        assert_eq!(entry.path_instance_id, second_instance);
        assert_eq!(
            entry.startup_rate_prior,
            RateHint::BitsPerSecond(80_000_000),
        );
    });
    assert_eq!(
        binding
            .sender_path_targets(TrafficClass::Throughput, 1)
            .into_iter()
            .find(|target| target.observation.key == key)
            .expect("replacement response output")
            .observation
            .snapshot
            .delivery_rate_bps,
        80_000_000.0,
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
    {
        let mut outputs = binding.outputs.lock().expect("response outputs lock");
        let entry = outputs
            .entries
            .iter_mut()
            .find(|entry| entry.key == key)
            .expect("attached response output");
        qualify_product_assignment(entry, MuxLimits::default());
    }
    let frame = stream_data_frame(4096);
    binding.record_original_flight(key, &frame);
    {
        let mut outputs = binding.outputs.lock().expect("response outputs lock");
        outputs
            .entries
            .iter_mut()
            .find(|entry| entry.key == key)
            .expect("attached response output")
            .product_rate_epoch = ResponseProductRateEpoch::new(
            100_000_000.0,
            1,
            64 * 1024,
            Instant::now(),
            Duration::from_secs(60),
        );
    }
    let before = with_output_entry_for_key(&binding, key, |entry| {
        assert!(entry.product_rate_epoch.is_some());
        assert!(entry.product_qualification.qualified());
        (entry.path_instance_id, entry.incarnation)
    });
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
    with_output_entry_for_key(&binding, key, |replacement| {
        assert_ne!(replacement.path_instance_id, before.0);
        assert_ne!(replacement.incarnation, before.1);
        assert_eq!(replacement.bytes_in_flight, 0);
        assert_eq!(replacement.original_data_in_flight_bytes, 0);
        assert!(replacement.product_rate_epoch.is_none());
        assert!(replacement.tcp_product_rate_evidence.is_none());
        assert!(replacement.local_path_metrics.is_none());
        assert!(replacement.peer_path_metrics.is_none());
        assert!(replacement.peer_usage.is_none());
        assert_eq!(replacement.product_qualification.deficit_bytes(), None);
        assert!(!replacement.product_qualification.qualified());
    });

    binding.release_normalized_acked_ranges(&[OffsetRange {
        start: 0,
        end: 4096,
    }]);
    with_output_entry_for_key(&binding, key, |replacement| {
        assert_eq!(replacement.original_data_acked_bytes, 0);
        assert_eq!(replacement.delivery_samples, 0);
        assert!(!replacement.product_qualification.qualified());
    });
}

#[test]
fn peer_path_usage_rejects_stale_sequences_and_wrong_instances() {
    let (binding, key, _receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let path_instance_id = with_output_entry_for_key(&binding, key, |entry| entry.path_instance_id);
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
    with_output_entry_for_key(&binding, key, |entry| {
        assert_eq!(entry.peer_usage, Some(PathUsage::Available));
        assert_eq!(entry.peer_usage_sequence, Some(3));
    });
}
