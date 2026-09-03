use super::test_support::*;
use super::*;
use crate::runtime::path::commands::reliable_path_command_channels;

#[tokio::test]
async fn pre_model_red_bound_recovery_waits_for_ordered_terminal_before_cancellation() {
    let stream_id = StreamId(712);
    let context =
        client_test_context_with_paths(&["tcp://127.0.0.1:10712", "tcp://127.0.0.1:10713"]);
    let (target_commands, mut target_receivers) = reliable_path_command_channels(4);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_test_relay_stream(stream_id, 0, target_commands.clone()),
        4,
    );
    let (owner_commands, mut owner_receivers) = reliable_path_command_channels(4);
    remotes.attach_candidate(opened_test_relay_stream(stream_id, 1, owner_commands));
    consume_client_path_proof_for_test(&mut target_receivers);
    consume_client_path_proof_for_test(&mut owner_receivers);
    let target = remotes
        .path_instance_for_key(RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        })
        .expect("RED setup failed: bound recovery target");
    let owner = remotes
        .path_instance_for_key(RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 1,
        })
        .expect("RED setup failed: distinct OriginalData owner");
    context.install_relay_path_instance_for_test(target);
    context.install_relay_path_instance_for_test(owner);
    let snapshot = context
        .reliable_path_snapshot_for_instance(target)
        .expect("RED setup failed: installed exact path evidence");
    let cause = RelaySendCause::persistent_client_ack_gap_reinjection(
        ClientReinjectionOutputIdentity { instance: target },
        snapshot,
    );
    let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
    let frame = send_stream
        .send_data(Bytes::from_static(b"repair"))
        .expect("RED setup failed: retained OriginalData debt");
    let mut sender = RequestSenderService::new(stream_id);
    sender.record_original_frame_for_test(owner, &frame);
    let mut sender_queue = ReliableRelaySenderQueue::default();
    sender.enqueue_critical_reinjection_frame(&mut sender_queue, frame, cause);

    target_commands.begin_path_drain();
    target_receivers.close_for_path_drain();
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        sender.dispatch_client_queued_work(
            &context,
            TrafficClass::Throughput,
            &mut remotes,
            &mut send_stream,
            &mut sender_queue,
            6,
            ReliableDataAckFrontierState::Live,
        ),
    )
    .await
    .expect("RED: old dispatch hung while fresh admission was closed");
    assert!(
        matches!(result, Err(RuntimeError::SenderServiceBlocked))
            && remotes.contains_path_instance(target),
        "RED: old sender removes/reclassifies a still-registered exact attachment before its ordered terminal: result={result:?} membership_retained={}",
        remotes.contains_path_instance(target),
    );
    assert_eq!(sender_queue.reinjection_bytes(), 6);
}
