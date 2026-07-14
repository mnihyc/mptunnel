use super::super::ResponseStreamBinding;
use super::super::session::{ServerPathLaneTracker, ServerSessionRetentionSnapshot};
use super::super::topology::ResponseStreamAttachOutcome;
use crate::model::path::CarrierPathKey;
use crate::model::response::ResponseServiceFamilyLoads;
use crate::mux::MuxLimits;
use crate::protocol::{PathId, ResetReason, SessionId, StreamId, StreamOpenRole, UnderlayProtocol};
use crate::runtime::path::commands::{
    ReliablePathCommand, recv_reliable_path_command, reliable_path_command_channels,
    reliable_path_command_pending_bytes, try_recv_reliable_path_command,
};
use crate::scheduler::FlowLane;
use std::sync::Arc;
use tokio::sync::Barrier;

#[test]
fn lane_tracker_reclaims_session_state_when_last_binding_drops() {
    let session_id = SessionId(95);
    let lane_tracker = Arc::new(ServerPathLaneTracker::default());
    let (commands, _receivers) = reliable_path_command_channels(8);
    let binding = ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        UnderlayProtocol::Tcp,
        PathId(0),
        commands,
        FlowLane::Throughput,
        MuxLimits::default(),
        lane_tracker.clone(),
    );

    assert_eq!(
        lane_tracker.retention_snapshot_for_test(session_id),
        Some(ServerSessionRetentionSnapshot {
            references: 1,
            generation: binding.lane_generation(),
            attachment_path_count: 1,
            service_path_count: 1,
            realtime_flows: 0,
            active_response_flows: 1,
        })
    );

    drop(binding);

    assert_eq!(lane_tracker.retention_snapshot_for_test(session_id), None);
}

#[tokio::test]
async fn close_command_detaches_shared_lane_load_exactly_once() {
    let session_id = SessionId(96);
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Tcp,
        path_id: PathId(0),
    };
    let lane_tracker = Arc::new(ServerPathLaneTracker::default());
    let (first_commands, _first_receivers) = reliable_path_command_channels(8);
    let first_commands_for_detach = first_commands.clone();
    let first = ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        key.underlay,
        key.path_id,
        first_commands,
        FlowLane::Throughput,
        MuxLimits::default(),
        lane_tracker.clone(),
    );
    let (second_commands, _second_receivers) = reliable_path_command_channels(8);
    let second = ResponseStreamBinding::new_with_limits_and_tracker(
        session_id,
        key.underlay,
        key.path_id,
        second_commands,
        FlowLane::Throughput,
        MuxLimits::default(),
        lane_tracker.clone(),
    );
    assert_eq!(lane_tracker.snapshot(session_id, key).active_flows, 2);
    let stale_target = first
        .sender_path_targets(FlowLane::Throughput, 64 * 1024)
        .into_iter()
        .find(|target| target.key == key)
        .expect("first response Service target");

    first.close_stream(StreamId(10)).await;
    assert_eq!(
        lane_tracker.snapshot(session_id, key).active_flows,
        2,
        "enqueuing close does not complete carrier detachment"
    );
    assert_eq!(
        first.response_scheduling_snapshot().service_family_loads,
        ResponseServiceFamilyLoads::new(1, 0),
        "close retires product Service ownership independently of attachment cleanup"
    );
    assert!(!first.commit_ordered_data_owner_for_target(&stale_target));
    first.set_lane(FlowLane::Latency);
    assert_eq!(
        first.response_scheduling_snapshot().service_family_loads,
        ResponseServiceFamilyLoads::new(1, 0),
        "a stale owner commit or lane change cannot resurrect closed Service load"
    );

    first.detach(key, &first_commands_for_detach);
    first.detach(key, &first_commands_for_detach);
    assert_eq!(
        lane_tracker.snapshot(session_id, key).active_flows,
        1,
        "command handling and repeated cleanup must leave the other stream counted"
    );

    drop(first);
    assert_eq!(lane_tracker.snapshot(session_id, key).active_flows, 1);
    drop(second);
    assert_eq!(lane_tracker.snapshot(session_id, key).active_flows, 0);
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
        FlowLane::Throughput,
    );
    let (second_commands, mut second_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            UnderlayProtocol::Udp,
            PathId(1),
            second_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            64 * 1024,
        ),
        ResponseStreamAttachOutcome::Attached,
    );
    let proof = recv_reliable_path_command(&mut second_receivers)
        .await
        .expect("attached output path proof");
    second_receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&proof));

    binding
        .reset_and_close_stream_ordered(stream_id, ResetReason::Refused, FlowLane::Throughput)
        .await;

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
        assert!(
            try_recv_reliable_path_command(receivers).is_none(),
            "terminal reset and local close must occupy one queue item"
        );
    }

    let (late_commands, _late_receivers) = reliable_path_command_channels(8);
    assert_eq!(
        binding.attach(
            UnderlayProtocol::Udp,
            PathId(2),
            late_commands,
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            64 * 1024,
        ),
        ResponseStreamAttachOutcome::RejectedClosedStream,
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
        FlowLane::Throughput,
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
            FlowLane::Throughput,
            StreamOpenRole::Validation,
            64 * 1024,
        )
    });
    let close_binding = binding.clone();
    let close_barrier = barrier.clone();
    let close = tokio::spawn(async move {
        close_barrier.wait().await;
        close_binding
            .reset_and_close_stream_ordered(stream_id, ResetReason::Refused, FlowLane::Throughput)
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
    first_receivers
        .release_pending_command_bytes(reliable_path_command_pending_bytes(&first_terminal));

    let mut racing_terminal_seen = false;
    while let Some(command) = try_recv_reliable_path_command(&mut racing_receivers) {
        racing_terminal_seen |= matches!(
            &command,
            ReliablePathCommand::ResetAndCloseStream {
                stream_id: reset_stream_id,
                reason: ResetReason::Refused,
            } if *reset_stream_id == stream_id
        );
        racing_receivers
            .release_pending_command_bytes(reliable_path_command_pending_bytes(&command));
    }
    match attach_outcome {
        ResponseStreamAttachOutcome::Attached => assert!(
            racing_terminal_seen,
            "an attachment admitted before close must enter its terminal snapshot"
        ),
        ResponseStreamAttachOutcome::RejectedClosedStream => assert!(
            !racing_terminal_seen,
            "an attachment rejected after close never owned an output"
        ),
        outcome => panic!("unexpected concurrent attach outcome: {outcome:?}"),
    }
}
