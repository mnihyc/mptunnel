use super::*;
use crate::config::{ClientSecurityConfig, ResourceLimits, SharedSecret};
use crate::model::capacity::reliable_relay_buffer_len;
use crate::mux::MuxLimits;
use crate::protocol::{CloseReason, OffsetRange, PathId, StreamId, TargetAddr, UnderlayProtocol};
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandReceivers, recv_reliable_path_command,
    reliable_path_command_channels, try_recv_reliable_path_command,
    try_recv_reliable_path_priority_command,
};
use crate::runtime::stream::{ReliablePathStream, ReliablePathStreamOutput};
use crate::transport::PathSpec;
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};
use tokio::sync::mpsc;

fn test_security() -> ClientSecurityConfig {
    ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    )
}

fn test_opened_remote_stream(
    stream_id: StreamId,
    path_index: usize,
    commands: crate::runtime::path::commands::ReliablePathCommandSender,
    frames: mpsc::Receiver<Result<Frame, RuntimeError>>,
) -> OpenedRemoteStream {
    test_opened_remote_stream_on(
        stream_id,
        path_index,
        UnderlayProtocol::Tcp,
        commands,
        frames,
    )
}

fn test_opened_remote_stream_on(
    stream_id: StreamId,
    path_index: usize,
    underlay: UnderlayProtocol,
    commands: crate::runtime::path::commands::ReliablePathCommandSender,
    frames: mpsc::Receiver<Result<Frame, RuntimeError>>,
) -> OpenedRemoteStream {
    OpenedRemoteStream::pending(
        ReliablePathStream {
            stream_id,
            max_offset: MuxLimits::default().max_stream_window_bytes,
            lane: TrafficClass::Latency,
            underlay,
            max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
            output: ReliablePathStreamOutput::fixed(
                underlay,
                PathId(path_index as u16),
                commands,
                MuxLimits::default(),
            ),
            frames: frames.into(),
        },
        path_index,
    )
}

async fn wait_for_buffered_remote_frame(remotes: &ReliableRelayRemoteSet) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while !remotes.has_buffered_frame() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("relay frame-forwarder deadline");
}

#[test]
fn client_ack_gap_path_model_wait_tracks_measured_and_unmeasured_alternates() {
    assert!(
        reliable_relay_client_ack_gap_path_model_wait_active(true, true,),
        "the wait is deliberately independent of current target measurement: a target can appear, while a measured slow target can improve enough to pull fallback forward to the owner loss boundary"
    );
    assert!(!reliable_relay_client_ack_gap_path_model_wait_active(
        false, true,
    ));
    assert!(!reliable_relay_client_ack_gap_path_model_wait_active(
        true, false,
    ));
    assert!(reliable_relay_client_ack_gap_capacity_wait_arm_active(
        true, true,
    ));
    assert!(!reliable_relay_client_ack_gap_capacity_wait_arm_active(
        true, false,
    ));
}

#[tokio::test]
async fn prearmed_ack_gap_capacity_wait_retains_release_before_select_poll() {
    let capacity = Arc::new(tokio::sync::Notify::new());
    let wait = arm_carrier_capacity_notifies(vec![capacity.clone()])
        .expect("one carrier capacity notification");

    capacity.notify_waiters();

    tokio::time::timeout(Duration::from_millis(50), wait)
        .await
        .expect("pre-armed capacity release must remain ready before the select poll");
}

#[tokio::test]
async fn actor_recovery_pass_prunes_lost_target_before_reselecting_survivor() {
    let stream_id = StreamId(919);
    let context = ClientPathContext::new(
        [
            "tcp://127.0.0.1:10919?initial-srtt-s=0.08&initial-rate-mbps=100",
            "tcp://127.0.0.1:10920?initial-srtt-s=0.005&initial-rate-mbps=1000",
            "tcp://127.0.0.1:10921?initial-srtt-s=0.04&initial-rate-mbps=200",
        ]
        .into_iter()
        .map(|path| path.parse::<PathSpec>().expect("test path"))
        .collect(),
        test_security(),
        ResourceLimits::default(),
    )
    .expect("client context");

    let (owner_commands, mut owner_receivers) = reliable_path_command_channels(8);
    let (owner_frames, owner_frames_rx) = mpsc::channel(1);
    let mut remotes = ReliableRelayRemoteSet::new(
        test_opened_remote_stream(stream_id, 0, owner_commands, owner_frames_rx),
        8,
    );
    let owner = remotes.paths[0].instance();

    let (lost_commands, mut lost_receivers) = reliable_path_command_channels(8);
    let (lost_frames, lost_frames_rx) = mpsc::channel(1);
    remotes.attach_candidate(test_opened_remote_stream(
        stream_id,
        1,
        lost_commands,
        lost_frames_rx,
    ));
    let lost = remotes
        .paths
        .iter()
        .find(|path| path.key().index == 1)
        .expect("initial fast recovery target")
        .instance();

    let (survivor_commands, mut survivor_receivers) = reliable_path_command_channels(8);
    let (survivor_frames, survivor_frames_rx) = mpsc::channel(1);
    remotes.attach_candidate(test_opened_remote_stream(
        stream_id,
        2,
        survivor_commands,
        survivor_frames_rx,
    ));
    let survivor = remotes
        .paths
        .iter()
        .find(|path| path.key().index == 2)
        .expect("surviving recovery target")
        .instance();

    for receivers in [
        &mut owner_receivers,
        &mut lost_receivers,
        &mut survivor_receivers,
    ] {
        assert!(matches!(
            try_recv_reliable_path_priority_command(receivers),
            Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
        ));
    }
    for instance in [owner, lost, survivor] {
        context.install_relay_path_instance_for_test(instance);
    }

    let mut send_stream = ReliableSendStream::new(stream_id, context.mux_limits);
    let original = send_stream
        .send_data(Bytes::from(vec![0x5a; 4096]))
        .expect("retained request range");
    let mut sender = RequestSenderService::new(stream_id);
    sender.record_original_frame_for_test(owner, &original);
    assert!(sender.mark_request_path_stale(&context, &remotes, owner, TrafficClass::Throughput,));
    let mut sender_queue = ReliableRelaySenderQueue::default();
    assert!(
        sender
            .drive_request_path_recovery(
                &mut sender_queue,
                &context,
                &remotes,
                &send_stream,
                TrafficClass::Throughput,
            )
            .queued
    );

    drop(
        remotes
            .remove_path_instance(lost)
            .expect("retire exact queued target"),
    );
    let mut request_recovery_dirty = false;
    assert_eq!(
        prune_unavailable_request_recovery_before_drive(
            &sender,
            &mut sender_queue,
            &remotes,
            &mut request_recovery_dirty,
        ),
        4096,
    );
    assert!(request_recovery_dirty);

    let recovery = sender.drive_request_path_recovery(
        &mut sender_queue,
        &context,
        &remotes,
        &send_stream,
        TrafficClass::Throughput,
    );
    assert!(
        recovery.queued,
        "the same actor recovery pass must bind the uncovered range to the survivor",
    );
    let dispatch = sender
        .dispatch_client_queued_work(
            &context,
            TrafficClass::Throughput,
            &mut remotes,
            &mut send_stream,
            &mut sender_queue,
            4096,
            ReliableDataAckFrontierState::Live,
        )
        .await
        .expect("survivor recovery dispatch");
    assert!(matches!(dispatch, ClientQueuedDispatch::Reinjection { .. }));
    assert!(matches!(
        try_recv_reliable_path_command(&mut survivor_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData {
            offset: 0,
            payload,
            ..
        })) if payload.len() == 4096
    ));
    assert!(try_recv_reliable_path_command(&mut owner_receivers).is_none());

    // Keep the mock attachment inputs alive until after the recovery assertion.
    drop((owner_frames, lost_frames, survivor_frames));
}

#[tokio::test]
async fn stale_path_failure_does_not_blacklist_same_key_successor() {
    let stream_id = StreamId(905);
    let path = "tcp://127.0.0.1:10905"
        .parse::<PathSpec>()
        .expect("test path");
    let context = ClientPathContext::new(vec![path], test_security(), ResourceLimits::default())
        .expect("client context");
    let (old_commands, _old_command_receivers) = reliable_path_command_channels(8);
    let (_old_frames_tx, old_frames_rx) = mpsc::channel(1);
    let mut remotes = ReliableRelayRemoteSet::new(
        test_opened_remote_stream(stream_id, 0, old_commands, old_frames_rx),
        8,
    );
    let stale = remotes.paths[0].instance();
    drop(
        remotes
            .remove_path_instance(stale)
            .expect("remove predecessor attachment"),
    );

    let (replacement_commands, _replacement_command_receivers) = reliable_path_command_channels(8);
    let (_replacement_frames_tx, replacement_frames_rx) = mpsc::channel(1);
    remotes.attach(test_opened_remote_stream(
        stream_id,
        0,
        replacement_commands,
        replacement_frames_rx,
    ));
    let successor = remotes.paths[0].instance();
    assert_eq!(successor.key, stale.key);
    assert_ne!(successor, stale);

    let mut sender = RequestSenderService::new(stream_id);
    let mut suppressions = ClientRelayPathOpenSuppressions::default();
    resolve_client_relay_path_error(
        &mut sender,
        &context,
        &mut remotes,
        &mut suppressions,
        stale,
        &RuntimeError::ReliablePathSessionClosed,
    )
    .await;

    assert_eq!(remotes.paths.len(), 1);
    assert_eq!(remotes.paths[0].instance(), successor);
    assert!(
        !suppressions.blocks(&context, successor.key, tokio::time::Instant::now()),
        "a delayed exact-instance miss must not blacklist the live same-key successor"
    );
}

#[tokio::test]
async fn matching_path_failure_still_removes_and_suppresses_the_failed_instance() {
    let stream_id = StreamId(906);
    let path = "tcp://127.0.0.1:10906"
        .parse::<PathSpec>()
        .expect("test path");
    let context = ClientPathContext::new(vec![path], test_security(), ResourceLimits::default())
        .expect("client context");
    let (commands, _command_receivers) = reliable_path_command_channels(8);
    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let mut remotes = ReliableRelayRemoteSet::new(
        test_opened_remote_stream(stream_id, 0, commands, frames_rx),
        8,
    );
    let failed = remotes.paths[0].instance();
    context.install_relay_path_instance_for_test(failed);
    let mut sender = RequestSenderService::new(stream_id);
    let mut suppressions = ClientRelayPathOpenSuppressions::default();

    resolve_client_relay_path_error(
        &mut sender,
        &context,
        &mut remotes,
        &mut suppressions,
        failed,
        &RuntimeError::ReliablePathSessionClosed,
    )
    .await;

    assert!(remotes.is_empty());
    assert!(suppressions.blocks(&context, failed.key, tokio::time::Instant::now()));
    let health = context.health().lock().expect("path health");
    assert_eq!(health.tcp[0].consecutive_failures, 1);
}

#[tokio::test]
async fn quic_request_stream_abandonment_detaches_only_its_logical_attachment() {
    let stream_id = StreamId(907);
    let path = "quic://127.0.0.1:10907"
        .parse::<PathSpec>()
        .expect("test QUIC path");
    let context = ClientPathContext::new(vec![path], test_security(), ResourceLimits::default())
        .expect("client context");
    let (commands, _command_receivers) = reliable_path_command_channels(8);
    let (frames_tx, frames_rx) = mpsc::channel(1);
    let mut remotes = ReliableRelayRemoteSet::new(
        test_opened_remote_stream_on(stream_id, 0, UnderlayProtocol::Udp, commands, frames_rx),
        8,
    );
    let attached = remotes.paths[0].instance();
    context.install_relay_path_instance_for_test(attached);
    let mut sender = RequestSenderService::new(stream_id);
    let mut suppressions = ClientRelayPathOpenSuppressions::default();
    frames_tx
        .send(Err(RuntimeError::QuicCarrier(
            crate::transport::quic::QuicCarrierError::H3Stream(
                h3::error::StreamError::RemoteTerminate {
                    code: h3::error::Code::from(0_u64),
                },
            ),
        )))
        .await
        .expect("publish request-stream abandonment");
    wait_for_buffered_remote_frame(&remotes).await;
    let ReliableRelayRemoteFrame { instance, frame } = remotes
        .recv_frame()
        .await
        .expect("forwarded request-stream abandonment");
    let error = frame.expect_err("request stream must report its abandonment");
    assert!(reliable_path_error_is_migratable(&error));

    resolve_client_relay_path_error(
        &mut sender,
        &context,
        &mut remotes,
        &mut suppressions,
        instance,
        &error,
    )
    .await;

    assert!(remotes.is_empty(), "only the abandoned attachment retires");
    assert!(
        suppressions.blocks(&context, attached.key, tokio::time::Instant::now()),
        "the failed logical stream must not immediately reopen the same QUIC request path"
    );
    let health = context.health().lock().expect("QUIC path health");
    let record = &health.udp[0];
    assert_eq!(record.path_instance_id(), Some(attached.path_instance_id));
    assert!(
        record.accepts_product_commit(attached.path_instance_id),
        "a request-stream reset must leave the shared QUIC carrier eligible"
    );
    assert_eq!(record.consecutive_failures, 0);
}

#[tokio::test]
async fn planned_drain_before_path_close_does_not_publish_terminal() {
    let stream_id = StreamId(909);
    let (commands, mut command_receivers) = reliable_path_command_channels(8);
    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let mut remotes = ReliableRelayRemoteSet::new(
        test_opened_remote_stream(stream_id, 0, commands.clone(), frames_rx),
        8,
    );

    // Production starts the exact-instance planned lifecycle before the actor
    // closes fresh command admission, then waits for ordered PATH_CLOSE. No
    // ReliablePathRetired input can exist throughout this interval.
    commands.begin_path_drain();
    command_receivers.close_for_path_drain();
    assert!(
        !commands.is_terminal(),
        "closed admission remains a nonterminal planned-drain phase"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(25), remotes.recv_frame())
            .await
            .is_err(),
        "planned drain cannot publish a terminal event before ordered PATH_CLOSE"
    );
    drop(command_receivers);
}

#[tokio::test]
async fn planned_retirement_follows_every_preaccepted_frame_through_cap_one_fan_in() {
    let stream_id = StreamId(910);
    let (commands, mut command_receivers) = reliable_path_command_channels(8);
    let (frames_tx, frames_rx) = mpsc::channel(1);
    let mut remotes = ReliableRelayRemoteSet::new(
        test_opened_remote_stream(stream_id, 0, commands.clone(), frames_rx),
        1,
    );
    let instance = remotes.paths[0].instance();

    frames_tx
        .send(Ok(Frame::StreamData {
            stream_id,
            offset: 0,
            payload: Bytes::from_static(b"A"),
        }))
        .await
        .expect("queue first carrier frame");
    wait_for_buffered_remote_frame(&remotes).await;
    frames_tx
        .send(Ok(Frame::StreamData {
            stream_id,
            offset: 1,
            payload: Bytes::from_static(b"B"),
        }))
        .await
        .expect("queue second carrier frame");
    let held_input_permit = tokio::time::timeout(Duration::from_secs(1), frames_tx.reserve())
        .await
        .expect("forwarder accepted second frame")
        .expect("carrier input remains open");

    commands.begin_path_drain();
    command_receivers.close_for_path_drain();
    assert!(command_receivers.finish_planned_path_retirement());
    assert!(commands.is_terminal());

    let first = remotes
        .try_recv_frame()
        .expect("first merged carrier frame");
    assert_eq!(first.instance, instance);
    assert!(matches!(
        first.frame,
        Ok(Frame::StreamData {
            offset: 0,
            payload,
            ..
        }) if payload == Bytes::from_static(b"A")
    ));
    let second = tokio::time::timeout(Duration::from_secs(1), remotes.recv_frame())
        .await
        .expect("second frame deadline")
        .expect("second merged carrier frame");
    assert_eq!(second.instance, instance);
    assert!(matches!(
        second.frame,
        Ok(Frame::StreamData {
            offset: 1,
            payload,
            ..
        }) if payload == Bytes::from_static(b"B")
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(25), remotes.recv_frame())
            .await
            .is_err(),
        "terminal cannot bypass a pre-terminal input reservation"
    );
    drop(held_input_permit);
    let terminal = tokio::time::timeout(Duration::from_secs(1), remotes.recv_frame())
        .await
        .expect("planned terminal deadline")
        .expect("planned terminal frame");
    assert_eq!(terminal.instance, instance);
    assert!(matches!(
        terminal.frame,
        Err(RuntimeError::ReliablePathRetired)
    ));
}

#[tokio::test]
async fn unexpected_output_owner_drop_closes_retained_input_with_failure() {
    let stream_id = StreamId(908);
    let (commands, command_receivers) = reliable_path_command_channels(8);
    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let mut remotes = ReliableRelayRemoteSet::new(
        test_opened_remote_stream(stream_id, 0, commands, frames_rx),
        8,
    );
    let instance = remotes.paths[0].instance();
    drop(command_receivers);

    let terminal = tokio::time::timeout(Duration::from_secs(1), remotes.recv_frame())
        .await
        .expect("unexpected carrier terminal deadline")
        .expect("unexpected carrier terminal frame");
    assert_eq!(terminal.instance, instance);
    assert!(matches!(
        terminal.frame,
        Err(RuntimeError::ReliablePathSessionClosed)
    ));
}

#[tokio::test]
async fn client_ack_extent_rejection_precedes_all_transaction_mutation() {
    let stream_id = StreamId(901);
    let path = "tcp://127.0.0.1:10901"
        .parse::<PathSpec>()
        .expect("test path");
    let context = ClientPathContext::new(vec![path], test_security(), ResourceLimits::default())
        .expect("client context");
    let limits = context.mux_limits;
    let (commands, _receivers) = reliable_path_command_channels(8);
    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let opened = OpenedRemoteStream::pending(
        ReliablePathStream {
            stream_id,
            max_offset: limits.max_stream_window_bytes,
            lane: TrafficClass::Throughput,
            underlay: UnderlayProtocol::Tcp,
            max_frame_payload_bytes: reliable_relay_buffer_len(limits),
            output: ReliablePathStreamOutput::fixed(
                UnderlayProtocol::Tcp,
                PathId(0),
                commands,
                limits,
            ),
            frames: frames_rx.into(),
        },
        0,
    );
    let mut remotes = ReliableRelayRemoteSet::new(opened, 8);
    let mut send_stream = ReliableSendStream::new(stream_id, limits);
    let sent = send_stream
        .send_data(Bytes::from_static(b"abcdefgh"))
        .expect("send request data");
    let mut sender = RequestSenderService::new(stream_id);
    sender.record_original_frame_for_test(remotes.paths[0].instance(), &sent);
    let mut sender_queue = ReliableRelaySenderQueue::default();
    sender_queue.push_reinjection(sent.clone());
    let mut state = ClientRelayState::new();

    let stream_before = send_stream.clone();
    let queue_bytes_before = sender_queue.bytes();
    let last_stream_at_before = state.progress.last_stream_at;
    let frontier_before = state.progress.last_send_ack_frontier;
    let snapshot_before = state.progress.last_send_ack.clone();
    let rejected = apply_client_stream_ack(
        ClientStreamAckContext {
            state: &mut state,
            sender: &mut sender,
            sender_queue: &mut sender_queue,
            context: &context,
            remotes: &mut remotes,
            send_stream: &mut send_stream,
            path_snapshot: None,
            relay_lane: TrafficClass::Throughput,
        },
        stream_id,
        true,
        vec![OffsetRange { start: 4, end: 9 }],
    );

    assert!(matches!(
        rejected,
        Err(crate::mux::stream::StreamError::AckRangeBeyondAssigned {
            start: 4,
            end: 9,
            assigned_end: 8,
        })
    ));
    assert_eq!(
        send_stream, stream_before,
        "send cache mutated on rejection"
    );
    assert_eq!(sender_queue.bytes(), queue_bytes_before);
    assert!(!sender_queue.is_empty(), "queued repair was released");
    assert_eq!(state.progress.last_stream_at, last_stream_at_before);
    assert_eq!(state.progress.last_send_ack_frontier, frontier_before);
    assert_eq!(state.progress.last_send_ack, snapshot_before);
    let released = apply_client_stream_ack(
        ClientStreamAckContext {
            state: &mut state,
            sender: &mut sender,
            sender_queue: &mut sender_queue,
            context: &context,
            remotes: &mut remotes,
            send_stream: &mut send_stream,
            path_snapshot: None,
            relay_lane: TrafficClass::Throughput,
        },
        stream_id,
        true,
        vec![OffsetRange { start: 0, end: 8 }],
    )
    .expect("exact assigned ACK commits");
    assert_eq!(released, 8);
    assert!(sender_queue.is_empty());
    assert_eq!(state.progress.last_send_ack.horizon(), Some(8));
}

async fn closed_output_relay(
    stream_id: StreamId,
    preaccepted_input: Option<Frame>,
) -> (
    tokio::io::DuplexStream,
    tokio::task::JoinHandle<Result<PathDeliveryStats, RuntimeError>>,
    mpsc::Sender<Result<Frame, RuntimeError>>,
) {
    let unused = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("reserve unused endpoint");
    let unused_addr = unused.local_addr().expect("unused endpoint address");
    drop(unused);
    let path = format!("tcp://{unused_addr}")
        .parse::<PathSpec>()
        .expect("TCP path");
    let mut context =
        ClientPathContext::new(vec![path], test_security(), ResourceLimits::default())
            .expect("client context");
    context.session_retention_timeout = Duration::from_millis(100);

    let limits = context.mux_limits;
    let (commands, command_receivers) = reliable_path_command_channels(4);
    let (frames_tx, frames_rx) = mpsc::channel(4);
    if let Some(frame) = preaccepted_input {
        frames_tx
            .send(Ok(frame))
            .await
            .expect("queue pre-terminal carrier input");
    }
    drop(command_receivers);
    let opened = OpenedRemoteStream::pending(
        ReliablePathStream {
            stream_id,
            max_offset: limits.max_stream_window_bytes,
            lane: TrafficClass::Latency,
            underlay: UnderlayProtocol::Tcp,
            max_frame_payload_bytes: reliable_relay_buffer_len(limits),
            output: ReliablePathStreamOutput::fixed(
                UnderlayProtocol::Tcp,
                PathId(0),
                commands,
                limits,
            ),
            frames: frames_rx.into(),
        },
        0,
    );
    let (application, relay_side) = duplex(4096);
    let relay_context = context.clone();
    let relay = tokio::spawn(async move {
        relay_migrating_tcp_stream(
            relay_side,
            &relay_context,
            MppPerformanceConfig::default(),
            ReliableRelayOpenSpec::new(TargetAddr::Ip(unused_addr), TrafficClass::Latency),
            opened,
            None,
        )
        .await
    });
    (application, relay, frames_tx)
}

async fn blocked_feedback_relay(
    stream_id: StreamId,
) -> (
    tokio::io::DuplexStream,
    tokio::task::JoinHandle<Result<PathDeliveryStats, RuntimeError>>,
    mpsc::Sender<Result<Frame, RuntimeError>>,
    ReliablePathCommandReceivers,
) {
    let unused = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("reserve unused endpoint");
    let unused_addr = unused.local_addr().expect("unused endpoint address");
    drop(unused);
    let path = format!("tcp://{unused_addr}")
        .parse::<PathSpec>()
        .expect("TCP path");
    let context = ClientPathContext::new(vec![path], test_security(), ResourceLimits::default())
        .expect("client context");
    let limits = context.mux_limits;
    let (commands, command_receivers) = reliable_path_command_channels(1);
    commands
        .try_enqueue_admitted_frame(
            Frame::StreamAck {
                stream_id,
                complete: false,
                ranges: Vec::new(),
            },
            TrafficClass::Control,
        )
        .expect("fill carrier control queue");
    let (frames_tx, frames_rx) = mpsc::channel(4);
    let opened = OpenedRemoteStream::pending(
        ReliablePathStream {
            stream_id,
            max_offset: limits.max_stream_window_bytes,
            lane: TrafficClass::Latency,
            underlay: UnderlayProtocol::Tcp,
            max_frame_payload_bytes: reliable_relay_buffer_len(limits),
            output: ReliablePathStreamOutput::fixed(
                UnderlayProtocol::Tcp,
                PathId(0),
                commands,
                limits,
            ),
            frames: frames_rx.into(),
        },
        0,
    );
    let (application, relay_side) = duplex(4096);
    let relay_context = context.clone();
    let relay = tokio::spawn(async move {
        relay_migrating_tcp_stream(
            relay_side,
            &relay_context,
            MppPerformanceConfig::default(),
            ReliableRelayOpenSpec::new(TargetAddr::Ip(unused_addr), TrafficClass::Latency),
            opened,
            None,
        )
        .await
    });
    (application, relay, frames_tx, command_receivers)
}

#[tokio::test]
async fn sticky_session_terminal_at_relay_entry_preempts_without_polling_a_saturated_carrier_queue()
{
    let unused = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("reserve unused endpoint");
    let unused_addr = unused.local_addr().expect("unused endpoint address");
    drop(unused);
    let path = format!("tcp://{unused_addr}")
        .parse::<PathSpec>()
        .expect("TCP path");
    let context = ClientPathContext::new(vec![path], test_security(), ResourceLimits::default())
        .expect("client context");
    let limits = context.mux_limits;
    let stream_id = StreamId(609);
    let (commands, _command_receivers) = reliable_path_command_channels(1);
    let (frames_tx, frames_rx) = mpsc::channel(1);
    frames_tx
        .try_send(Ok(Frame::StreamData {
            stream_id,
            offset: 0,
            payload: Bytes::from_static(b"accepted before close"),
        }))
        .expect("saturate established carrier frame queue");
    assert_eq!(frames_tx.capacity(), 0);
    let opened = OpenedRemoteStream::pending(
        ReliablePathStream {
            stream_id,
            max_offset: limits.max_stream_window_bytes,
            lane: TrafficClass::Latency,
            underlay: UnderlayProtocol::Tcp,
            max_frame_payload_bytes: reliable_relay_buffer_len(limits),
            output: ReliablePathStreamOutput::fixed(
                UnderlayProtocol::Tcp,
                PathId(0),
                commands,
                limits,
            ),
            frames: frames_rx.into(),
        },
        0,
    );
    let (_application, relay_side) = duplex(4096);
    context.retire_session(CloseReason::PolicyRejected);

    let result = relay_migrating_tcp_stream(
        relay_side,
        &context,
        MppPerformanceConfig::default(),
        ReliableRelayOpenSpec::new(TargetAddr::Ip(unused_addr), TrafficClass::Latency),
        opened,
        None,
    )
    .await;
    assert!(matches!(
        result,
        Err(RuntimeError::RemoteClosed(CloseReason::PolicyRejected))
    ));
}

async fn assert_absolute_retention_timeout(
    relay: &mut tokio::task::JoinHandle<Result<PathDeliveryStats, RuntimeError>>,
) {
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(
        !relay.is_finished(),
        "carrier loss bypassed the retention interval"
    );
    let result = tokio::time::timeout(Duration::from_secs(1), relay)
        .await
        .expect("retention expiry timeout")
        .expect("relay task");
    assert!(matches!(result, Err(RuntimeError::SessionRetentionTimeout)));
}

#[tokio::test]
async fn send_first_carrier_loss_enters_absolute_session_retention() {
    let (mut application, mut relay, frames_tx) = closed_output_relay(StreamId(610), None).await;

    application
        .write_all(b"send-first failure")
        .await
        .expect("application write");
    assert_absolute_retention_timeout(&mut relay).await;
    drop(frames_tx);
}

#[tokio::test]
async fn fin_feedback_carrier_loss_enters_absolute_session_retention() {
    let (application, mut relay, frames_tx) = closed_output_relay(
        StreamId(612),
        Some(Frame::StreamFin {
            stream_id: StreamId(612),
            final_offset: 0,
        }),
    )
    .await;

    assert_absolute_retention_timeout(&mut relay).await;
    drop(application);
    drop(frames_tx);
}

#[tokio::test]
async fn retained_in_order_fin_commits_after_reattachment_feedback() {
    let recv_stream = ReliableRecvStream::new(StreamId(613), MuxLimits::default());
    let mut state = ClientRelayState::new();
    assert!(
        receive_stream_fin(
            &recv_stream,
            &mut state.endpoint.pending_remote_fin_offset,
            0,
        )
        .expect("in-order FIN")
    );
    assert_eq!(state.endpoint.pending_remote_fin_offset, Some(0));

    let (mut application, mut relay_side) = duplex(64);
    commit_pending_remote_fin(&mut relay_side, &mut state, &recv_stream, true)
        .await
        .expect("commit retained FIN");

    assert!(!state.endpoint.remote_open);
    assert_eq!(state.endpoint.pending_remote_fin_offset, None);
    let mut byte = [0u8; 1];
    assert_eq!(
        application.read(&mut byte).await.expect("read half-close"),
        0
    );
}

#[tokio::test]
async fn client_completion_retains_ack_until_every_live_attachment_accepts_it() {
    let stream_id = StreamId(615);
    let limits = MuxLimits::default();
    let opened = |path_index, commands| {
        let (frames_tx, frames_rx) = mpsc::channel(1);
        (
            frames_tx,
            OpenedRemoteStream::pending(
                ReliablePathStream {
                    stream_id,
                    max_offset: limits.max_stream_window_bytes,
                    lane: TrafficClass::Latency,
                    underlay: UnderlayProtocol::Tcp,
                    max_frame_payload_bytes: reliable_relay_buffer_len(limits),
                    output: ReliablePathStreamOutput::fixed(
                        UnderlayProtocol::Tcp,
                        PathId(path_index),
                        commands,
                        limits,
                    ),
                    frames: frames_rx.into(),
                },
                usize::from(path_index),
            ),
        )
    };
    let (first_commands, mut first_receivers) = reliable_path_command_channels(4);
    let (_first_frames, first) = opened(0, first_commands);
    let mut remotes = ReliableRelayRemoteSet::new(first, 4);
    while try_recv_reliable_path_priority_command(&mut first_receivers).is_some() {}

    let (blocked_commands, mut blocked_receivers) = reliable_path_command_channels(1);
    let (_blocked_frames, blocked) = opened(1, blocked_commands.clone());
    remotes.attach(blocked);
    while try_recv_reliable_path_priority_command(&mut blocked_receivers).is_some() {}
    blocked_commands
        .try_enqueue_admitted_frame(Frame::Ping { nonce: 1 }, TrafficClass::Control)
        .expect("block one exact attachment");

    let publication = remotes.publish_stream_ack(
        1,
        vec![Frame::StreamAck {
            stream_id,
            complete: true,
            ranges: Vec::new(),
        }],
    );
    assert!(publication.published);
    assert!(publication.pending);

    let mut state = ClientRelayState::new();
    state.record_local_eof();
    state.record_local_fin_sent();
    state.record_terminal_fin_replayed();
    state.record_remote_finished();
    let send_stream = ReliableSendStream::new(stream_id, limits);
    let recv_stream = ReliableRecvStream::new(stream_id, limits);
    let sender_queue = ReliableRelaySenderQueue::default();
    assert!(!client_relay_finished(
        &state,
        &send_stream,
        &recv_stream,
        &sender_queue,
        &remotes,
    ));

    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut blocked_receivers),
        Some(ReliablePathCommand::SendFrame(Frame::Ping { nonce: 1 }))
    ));
    let publication = remotes.retry_pending_stream_ack();
    assert!(publication.published);
    assert!(!publication.pending);
    assert!(client_relay_finished(
        &state,
        &send_stream,
        &recv_stream,
        &sender_queue,
        &remotes,
    ));
}

#[tokio::test]
async fn client_completion_retains_zero_publication_requalification_ack() {
    let stream_id = StreamId(616);
    let limits = MuxLimits::default();
    let (commands, mut receivers) = reliable_path_command_channels(1);
    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let opened = OpenedRemoteStream::pending(
        ReliablePathStream {
            stream_id,
            max_offset: limits.max_stream_window_bytes,
            lane: TrafficClass::Latency,
            underlay: UnderlayProtocol::Tcp,
            max_frame_payload_bytes: reliable_relay_buffer_len(limits),
            output: ReliablePathStreamOutput::fixed(
                UnderlayProtocol::Tcp,
                PathId(0),
                commands.clone(),
                limits,
            ),
            frames: frames_rx.into(),
        },
        0,
    );
    let mut remotes = ReliableRelayRemoteSet::new(opened, 1);
    while try_recv_reliable_path_priority_command(&mut receivers).is_some() {}
    commands
        .try_enqueue_admitted_frame(Frame::Ping { nonce: 616 }, TrafficClass::Control)
        .expect("fill the only exact reverse-control queue");

    let target = remotes.paths[0].instance();
    assert!(
        !remotes
            .publish_requalification_ack(
                target,
                Frame::StreamRequalifyAck {
                    stream_id,
                    probe_id: 1,
                    offset: 4096,
                    payload_bytes: 512,
                },
            )
            .expect("retain a zero-publication exact receipt")
    );
    assert!(remotes.has_pending_requalification_ack());

    let mut state = ClientRelayState::new();
    state.record_local_eof();
    state.record_local_fin_sent();
    state.record_terminal_fin_replayed();
    state.record_remote_finished();
    let send_stream = ReliableSendStream::new(stream_id, limits);
    let recv_stream = ReliableRecvStream::new(stream_id, limits);
    let sender_queue = ReliableRelaySenderQueue::default();
    assert!(
        !client_relay_finished(&state, &send_stream, &recv_stream, &sender_queue, &remotes,),
        "stream completion must not discard its retained exact requalification receipt",
    );
}

#[tokio::test]
async fn final_feedback_backpressure_keeps_fin_pending_until_ack_is_queued() {
    let stream_id = StreamId(614);
    let (mut application, relay, frames_tx, mut command_receivers) =
        blocked_feedback_relay(stream_id).await;
    frames_tx
        .send(Ok(Frame::StreamFin {
            stream_id,
            final_offset: 0,
        }))
        .await
        .expect("remote FIN");

    let mut byte = [0_u8; 1];
    assert!(
        tokio::time::timeout(Duration::from_millis(50), application.read(&mut byte))
            .await
            .is_err(),
        "remote FIN committed before its final Data ACK entered a carrier queue"
    );

    assert!(matches!(
        recv_reliable_path_command(&mut command_receivers).await,
        Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
    ));
    let final_feedback = tokio::time::timeout(
        Duration::from_secs(1),
        recv_reliable_path_command(&mut command_receivers),
    )
    .await
    .expect("final feedback enqueue deadline");
    match final_feedback {
        Some(ReliablePathCommand::SendFrame(frame)) => assert!(
            matches!(
                &frame,
                Frame::StreamAck {
                    stream_id: ack_stream_id,
                    complete: true,
                    ..
                } if *ack_stream_id == stream_id
            ),
            "unexpected frame before final Data ACK: {frame:?}"
        ),
        Some(_) => panic!("unexpected non-frame command before final Data ACK"),
        None => panic!("carrier command queue closed before final Data ACK"),
    }
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), application.read(&mut byte))
            .await
            .expect("remote half-close deadline")
            .expect("application read"),
        0
    );

    relay.abort();
}

#[test]
fn request_outstanding_limit_uses_stream_resources_then_exact_ack_headroom() {
    let mux_limits = MuxLimits {
        max_stream_window_bytes: 4 * 1024 * 1024,
        max_repair_bytes: 4 * 1024 * 1024,
        max_reorder_bytes: 4 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let payload_bytes = 64 * 1024;
    let accounting_limit = mux_limits
        .max_repair_bytes
        .min(mux_limits.max_reorder_bytes)
        .min(mux_limits.max_stream_window_bytes as usize);
    let limit = reliable_relay_request_outstanding_limit_bytes(
        TrafficClass::Throughput,
        payload_bytes,
        accounting_limit,
        mux_limits,
    );
    assert_eq!(limit, accounting_limit);

    let mut send_stream = ReliableSendStream::new(StreamId(90), mux_limits);
    send_stream
        .send_data(Bytes::from(vec![0x11; 512 * 1024]))
        .expect("first dispatched request chunk");
    send_stream
        .send_data(Bytes::from(vec![0x22; 512 * 1024]))
        .expect("second dispatched request chunk");
    let mut sender_queue = ReliableRelaySenderQueue::default();
    sender_queue.push_data(Bytes::from(vec![0x33; 1024 * 1024]));

    assert_eq!(
        reliable_relay_request_outstanding_headroom_bytes(
            &send_stream,
            &sender_queue,
            accounting_limit,
        ),
        2 * 1024 * 1024
    );
    sender_queue.push_data(Bytes::from(vec![0x44; 2 * 1024 * 1024]));
    assert_eq!(
        reliable_relay_request_outstanding_headroom_bytes(
            &send_stream,
            &sender_queue,
            accounting_limit,
        ),
        0,
        "raw request data and Data-ACK-retained ranges share one unique-byte budget"
    );
    let ack = send_stream
        .apply_ack(&[OffsetRange {
            start: 0,
            end: 1024 * 1024,
        }])
        .expect("ACK remains within assigned request data");
    assert_eq!(ack.released_bytes, 1024 * 1024);
    assert_eq!(
        reliable_relay_request_outstanding_headroom_bytes(
            &send_stream,
            &sender_queue,
            accounting_limit,
        ),
        1024 * 1024,
        "unique STREAM_ACK release must resume source reads without double-counting raw queue bytes"
    );
}

#[test]
fn request_source_staging_exhausts_sum_product_window_and_data_ack_reopens_it() {
    let mux_limits = MuxLimits {
        max_stream_window_bytes: 4 * 1024 * 1024,
        max_repair_bytes: 4 * 1024 * 1024,
        max_reorder_bytes: 4 * 1024 * 1024,
        ..MuxLimits::default()
    };
    let product_window = 2 * 1024 * 1024;
    let limit = reliable_relay_request_outstanding_limit_bytes(
        TrafficClass::Throughput,
        64 * 1024,
        product_window,
        mux_limits,
    );
    assert_eq!(limit, product_window);

    let mut send_stream = ReliableSendStream::new(StreamId(91), mux_limits);
    send_stream
        .send_data(Bytes::from(vec![0x51; 512 * 1024]))
        .expect("first retained Product frame");
    send_stream
        .send_data(Bytes::from(vec![0x52; 512 * 1024]))
        .expect("second retained Product frame");
    let mut sender_queue = ReliableRelaySenderQueue::default();
    sender_queue.push_data(Bytes::from(vec![0x53; 1024 * 1024]));
    assert_eq!(
        reliable_relay_request_outstanding_headroom_bytes(&send_stream, &sender_queue, limit,),
        0,
        "retained plus queued unique bytes consume the complete sum(P_i) source envelope",
    );

    let ack = send_stream
        .apply_ack(&[OffsetRange {
            start: 0,
            end: 1024 * 1024,
        }])
        .expect("exact Data ACK");
    assert_eq!(ack.released_bytes, 1024 * 1024);
    assert_eq!(
        reliable_relay_request_outstanding_headroom_bytes(&send_stream, &sender_queue, limit,),
        1024 * 1024,
        "Data ACK release reopens source staging without borrowing another stream's window",
    );
}

#[test]
fn latency_request_outstanding_limit_keeps_the_staging_reservoir() {
    let mux_limits = MuxLimits::default();
    let payload_bytes = 64 * 1024;
    let limit = reliable_relay_request_outstanding_limit_bytes(
        TrafficClass::Latency,
        payload_bytes,
        reliable_relay_buffer_len(mux_limits),
        mux_limits,
    );

    assert_eq!(limit, reliable_relay_buffer_len(mux_limits));
    assert!(limit < mux_limits.max_stream_window_bytes as usize);
}

#[tokio::test]
async fn bulk_request_staging_uses_resource_ceiling_and_bounded_ready_work() {
    let limits = MuxLimits::default();
    assert_eq!(
        reliable_relay_request_outstanding_limit_bytes(
            TrafficClass::Throughput,
            64 * 1024,
            usize::MAX,
            limits,
        ),
        limits
            .max_repair_bytes
            .min(limits.max_reorder_bytes)
            .min(limits.max_stream_window_bytes as usize),
    );

    let staging_limits = MuxLimits {
        max_payload_bytes: 16,
        max_stream_window_bytes: 64,
        max_repair_bytes: 64,
        max_reorder_bytes: 64,
        max_path_flight_bytes: 64,
        max_reliable_relay_chunk_bytes: 16,
        ..MuxLimits::default()
    };
    let sender_queue_limit = reliable_relay_buffer_len(staging_limits);
    let (sender_dispatch_byte_budget, sender_dispatch_item_budget) =
        reliable_relay_sender_dispatch_budget(
            staging_limits,
            TrafficClass::Throughput,
            4,
            sender_queue_limit,
            sender_queue_limit,
        );
    assert_eq!(
        (sender_dispatch_byte_budget, sender_dispatch_item_budget),
        (16, 4),
        "the authoritative bulk sender budget permits bounded batching"
    );

    let send_stream = ReliableSendStream::new(StreamId(451), staging_limits);
    let mut sender_queue = ReliableRelaySenderQueue::default();
    sender_queue.push_data(Bytes::from_static(b"12345"));
    let bounds = ClientOpportunisticReadBounds {
        sender_dispatch_byte_budget,
        sender_dispatch_item_budget,
        sender_queue_limit,
        source_read_ceiling: reliable_relay_buffer_len(staging_limits),
        request_outstanding_limit: 64,
    };
    assert_eq!(
        reliable_relay_client_opportunistic_read_budget(1, &send_stream, &sender_queue, bounds),
        11,
        "bulk may stage more work, but only through the remaining dispatch-byte budget"
    );
    assert_eq!(
        reliable_relay_client_opportunistic_read_budget(
            sender_dispatch_item_budget,
            &send_stream,
            &sender_queue,
            bounds,
        ),
        0,
        "one pass cannot exceed its authoritative dispatch-item budget"
    );
    assert_eq!(
        reliable_relay_client_opportunistic_read_budget(
            1,
            &send_stream,
            &sender_queue,
            ClientOpportunisticReadBounds {
                request_outstanding_limit: sender_queue.data_bytes() + 3,
                ..bounds
            },
        ),
        3,
        "the exact outstanding-resource headroom caps the next source read"
    );
    assert_eq!(
        reliable_relay_client_opportunistic_read_budget(
            1,
            &send_stream,
            &sender_queue,
            ClientOpportunisticReadBounds {
                sender_queue_limit: sender_queue.bytes(),
                ..bounds
            },
        ),
        0,
        "sender queue backpressure stops opportunistic source reads"
    );

    assert_eq!(
        ready_at_entry(std::future::ready(7_u8)).await,
        Some(7),
        "work ready when the bounded drain starts is admitted"
    );
    assert_eq!(
        ready_at_entry(std::future::pending::<u8>()).await,
        None,
        "the opportunistic drain never waits for future source work"
    );

    let session_send_buffer = crate::runtime::stream::SessionSendBuffer::new(8);
    let mut updates = session_send_buffer.subscribe();
    assert_eq!(
        ready_at_entry(async {
            let _permit = session_send_buffer.reserve(&mut updates, 8).await;
            std::future::pending::<()>().await;
        })
        .await,
        None
    );
    assert_eq!(
        session_send_buffer.available_bytes(),
        8,
        "cancelling a not-ready opportunistic source read releases its reservation"
    );
}
