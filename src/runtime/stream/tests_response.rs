use super::attachment::ResponseStreamAttachOutcome;
use super::test_support::{qualify_product_assignment, stream_data_frame};
use super::{ResponseStreamBinding, ServerSessionTracker};
use crate::mux::MuxLimits;
use crate::protocol::{PathId, ResetReason, SessionId, StreamId, UnderlayProtocol};
use crate::runtime::path::commands::{
    ReliablePathCommand, recv_reliable_path_command, reliable_path_command_channels,
    reliable_path_command_pending_bytes, try_recv_reliable_path_command,
};
use crate::scheduler::TrafficClass;
use std::sync::Arc;
use tokio::sync::Barrier;

#[test]
fn response_binding_holds_one_session_reference_until_drop() {
    let session_id = SessionId(95);
    let tracker = Arc::new(ServerSessionTracker::default());
    let (commands, _receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(0),
        commands,
        TrafficClass::Throughput,
        MuxLimits::default(),
        tracker.clone(),
    );

    assert_eq!(tracker.reference_count(session_id), 1);
    drop(binding);
    assert_eq!(tracker.reference_count(session_id), 0);
}

#[tokio::test]
async fn terminal_reset_captures_current_outputs_and_rejects_late_attach() {
    let stream_id = StreamId(97);
    let (first_commands, mut first_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(97),
        UnderlayProtocol::Tcp,
        PathId(0),
        first_commands,
        TrafficClass::Throughput,
    );
    {
        let mut outputs = binding.outputs.lock().expect("response outputs lock");
        qualify_product_assignment(&mut outputs.entries[0], MuxLimits::default());
    }
    binding.record_original_flight(
        crate::model::path::CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        },
        &stream_data_frame(4096),
    );
    let (second_commands, mut second_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            UnderlayProtocol::Udp,
            PathId(1),
            second_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::Attached,
    );
    binding
        .reset_and_close_stream_ordered(stream_id, ResetReason::Refused, TrafficClass::Throughput)
        .await;

    {
        let outputs = binding.outputs.lock().expect("response outputs lock");
        assert!(
            outputs
                .entries
                .iter()
                .all(|entry| !entry.product_qualification.qualified())
        );
        assert_eq!(
            outputs.original_data_in_flight_bytes, 4096,
            "terminal qualification reset cannot delete unresolved Product ownership"
        );
    }

    for receivers in [&mut first_receivers, &mut second_receivers] {
        let terminal = recv_reliable_path_command(receivers)
            .await
            .expect("terminal reset and close");
        assert!(matches!(
            &terminal,
            ReliablePathCommand::ResetAndCloseStream {
                stream_id: reset_stream_id,
                reason: ResetReason::Refused,
            } if *reset_stream_id == stream_id
        ));
        receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&terminal));
        assert!(try_recv_reliable_path_command(receivers).is_none());
    }
    let (late_commands, _late_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            UnderlayProtocol::Udp,
            PathId(2),
            late_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::RejectedClosedStream,
    );
}

#[tokio::test]
async fn idle_reset_closes_attachment_admission_before_blocked_publication() {
    let stream_id = StreamId(971);
    let (commands, mut receivers) = reliable_path_command_channels(1);
    commands
        .send_control(ReliablePathCommand::CloseStream(StreamId(999)))
        .await
        .expect("prefill control queue");
    let binding = ResponseStreamBinding::new(
        SessionId(971),
        UnderlayProtocol::Tcp,
        PathId(0),
        commands,
        TrafficClass::Throughput,
    );

    binding.retire_stream_with_reset(stream_id, ResetReason::TimedOut);
    binding.retire_stream_with_reset(stream_id, ResetReason::TimedOut);
    let (late_commands, _late_receivers) = reliable_path_command_channels(1);
    assert_eq!(
        binding.attach(
            UnderlayProtocol::Udp,
            PathId(1),
            late_commands,
            TrafficClass::Throughput,
        ),
        ResponseStreamAttachOutcome::RejectedClosedStream,
        "local retirement commits before carrier queue publication"
    );

    assert!(matches!(
        recv_reliable_path_command(&mut receivers).await,
        Some(ReliablePathCommand::ResetAndCloseStream {
            stream_id: received,
            reason: ResetReason::TimedOut,
        }) if received == stream_id
    ));
    assert!(matches!(
        recv_reliable_path_command(&mut receivers).await,
        Some(ReliablePathCommand::CloseStream(StreamId(999)))
    ));
    assert!(
        try_recv_reliable_path_command(&mut receivers).is_none(),
        "repeat retirement is idempotent"
    );
}

#[tokio::test]
async fn concurrent_attach_either_joins_terminal_snapshot_or_observes_closed_stream() {
    let stream_id = StreamId(98);
    let (first_commands, mut first_receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new(
        SessionId(98),
        UnderlayProtocol::Tcp,
        PathId(0),
        first_commands,
        TrafficClass::Throughput,
    );
    let (racing_commands, mut racing_receivers) = reliable_path_command_channels(8);
    let barrier = Arc::new(Barrier::new(3));

    let attach_binding = binding.clone();
    let attach_barrier = barrier.clone();
    let attach = tokio::spawn(async move {
        attach_barrier.wait().await;
        attach_binding.attach(
            UnderlayProtocol::Udp,
            PathId(1),
            racing_commands,
            TrafficClass::Throughput,
        )
    });
    let close_binding = binding.clone();
    let close_barrier = barrier.clone();
    let close = tokio::spawn(async move {
        close_barrier.wait().await;
        close_binding
            .reset_and_close_stream_ordered(
                stream_id,
                ResetReason::Refused,
                TrafficClass::Throughput,
            )
            .await;
    });

    barrier.wait().await;
    let attach_outcome = attach.await.expect("attach task");
    close.await.expect("terminal close task");

    let first_terminal = recv_reliable_path_command(&mut first_receivers)
        .await
        .expect("initial output terminal command");
    assert!(matches!(
        &first_terminal,
        ReliablePathCommand::ResetAndCloseStream {
            stream_id: reset_stream_id,
            reason: ResetReason::Refused,
        } if *reset_stream_id == stream_id
    ));

    let mut racing_terminal_seen = false;
    while let Some(command) = try_recv_reliable_path_command(&mut racing_receivers) {
        racing_terminal_seen |= matches!(
            &command,
            ReliablePathCommand::ResetAndCloseStream {
                stream_id: reset_stream_id,
                reason: ResetReason::Refused,
            } if *reset_stream_id == stream_id
        );
    }
    match attach_outcome {
        ResponseStreamAttachOutcome::Attached => assert!(racing_terminal_seen),
        ResponseStreamAttachOutcome::RejectedClosedStream => assert!(!racing_terminal_seen),
        outcome => panic!("unexpected concurrent attach outcome: {outcome:?}"),
    }
}
