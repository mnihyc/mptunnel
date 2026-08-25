use super::test_support::*;
use super::*;
use crate::runtime::path::commands::reliable_path_command_channels;

#[tokio::test]
async fn pre_model_red_bound_recovery_waits_for_ordered_terminal_before_cancellation() {
    let stream_id = StreamId(712);
    let context = client_test_context_with_paths(&["tcp://127.0.0.1:10712"]);
    let (commands, mut receivers) = reliable_path_command_channels(4);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_test_relay_stream(stream_id, 0, commands.clone()), 4);
    consume_client_path_proof_for_test(&mut receivers);
    let instance = remotes.paths[0].instance();
    context.install_relay_path_instance_for_test(instance);
    let snapshot = context
        .reliable_path_snapshot_for_instance(instance)
        .expect("RED setup failed: installed exact path evidence");
    let cause = RelaySendCause::persistent_client_ack_gap_reinjection(
        ClientReinjectionOutputIdentity { instance },
        snapshot,
    );
    let frame = Frame::StreamData {
        stream_id,
        offset: 0,
        payload: Bytes::from_static(b"repair"),
    };
    let mut sender = RequestSenderService::new(stream_id);

    commands.begin_path_drain();
    receivers.close_for_path_drain();
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        sender.send_reinjection_frame(&context, &mut remotes, frame, cause),
    )
    .await
    .expect("RED: old dispatch hung while fresh admission was closed");
    assert!(
        matches!(result, Err(RuntimeError::SenderServiceBlocked))
            && remotes.contains_path_instance(instance),
        "RED: old sender removes/reclassifies a still-registered exact attachment before its ordered terminal: result={result:?} membership_retained={}",
        remotes.contains_path_instance(instance),
    );
}
