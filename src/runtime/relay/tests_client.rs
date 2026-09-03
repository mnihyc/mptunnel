use super::{
    ClientRelayDeliveryState, ClientRelayDisconnectedState, ClientRelayState,
    ClientStreamAckContext, apply_client_stream_ack, evaluate_client_data_ack_reinjection,
    request_target_reinjection_service_limit, update_reinjection_authoritative_ack_snapshot,
    update_request_path_staleness,
};
use crate::config::{ClientSecurityConfig, ResourceLimits, SharedSecret};
use crate::model::admission::ReliableDataAckFrontierState;
use crate::model::capacity::{
    MAX_RELIABLE_SERVICE_QUANTUM_BYTES, PathRateSample, adaptive_reliable_relay_reinjection_bytes,
    reliable_bulk_carrier_feed_quantum_bytes, reliable_bulk_product_windows,
    reliable_product_recovery_window_bytes, reliable_relay_buffer_len,
};
use crate::model::path::{CarrierPathInstanceId, RelayPathInstance, RelayPathKey};
use crate::model::timing::ReliableDataAckGapTiming;
use crate::mux::MuxLimits;
use crate::mux::stream::{ReliableRecvStream, ReliableSendStream, validate_stream_ack};
use crate::protocol::{Frame, OffsetRange, PathId, StreamId, UnderlayProtocol};
use crate::runtime::path::ClientPathContext;
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandSender, reliable_path_command_channels,
    try_recv_reliable_path_priority_command,
};
use crate::runtime::relay::control::arm_request_path_staleness_model_publication;
use crate::runtime::sender::{
    ClientQueuedDispatch, ClientReinjectionOutputIdentity, RelaySendCause,
    ReliableRelayQueuedWorkKind, ReliableRelaySenderQueue, RequestSenderService,
};
use crate::runtime::stream::{
    OpenedRemoteStream, ReliablePathStream, ReliablePathStreamOutput, ReliableRelayRemoteSet,
};
use crate::scheduler::{PathSnapshot, TrafficClass};
use crate::transport::PathSpec;
use bytes::Bytes;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

fn test_security() -> ClientSecurityConfig {
    ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    )
}

fn opened_request_path(
    stream_id: StreamId,
    path_index: usize,
    commands: ReliablePathCommandSender,
) -> OpenedRemoteStream {
    opened_request_path_with_underlay(stream_id, UnderlayProtocol::Tcp, path_index, commands)
}

fn opened_request_path_with_underlay(
    stream_id: StreamId,
    underlay: UnderlayProtocol,
    path_index: usize,
    commands: ReliablePathCommandSender,
) -> OpenedRemoteStream {
    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let limits = MuxLimits::default();
    OpenedRemoteStream::pending(
        ReliablePathStream {
            stream_id,
            max_offset: limits.max_stream_window_bytes,
            lane: TrafficClass::Throughput,
            underlay,
            max_frame_payload_bytes: reliable_relay_buffer_len(limits),
            output: ReliablePathStreamOutput::fixed(
                underlay,
                PathId(path_index as u16),
                commands,
                limits,
            ),
            frames: frames_rx.into(),
        },
        path_index,
    )
}

fn consume_path_proof(
    receivers: &mut crate::runtime::path::commands::ReliablePathCommandReceivers,
) {
    assert!(matches!(
        try_recv_reliable_path_priority_command(receivers),
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
    ));
}

#[test]
fn disconnected_retries_never_extend_the_absolute_retention_deadline() {
    let since = Instant::now();
    let mut state = ClientRelayDisconnectedState::new(since, tokio::time::Instant::now());
    let retention = Duration::from_secs(300);
    let deadline = state.retention_deadline(retention);

    state.retry_after(Duration::from_secs(1));
    state.retry_after(Duration::from_secs(2));

    assert_eq!(state.since, since);
    assert_eq!(state.retention_deadline(retention), deadline);
    assert!(!state.expired(since + retention - Duration::from_millis(1), retention));
    assert!(state.expired(since + retention, retention));
}

#[test]
fn response_delivery_accounting_is_logical_not_sender_path_evidence() {
    let delivered = [
        Bytes::from_static(&[0; 1024]),
        Bytes::from_static(&[1; 4096]),
    ];
    let mut delivery = ClientRelayDeliveryState::default();

    let delivered_bytes = delivery.record_response(&delivered);

    assert_eq!(delivered_bytes, 5120);
    assert_eq!(delivery.total.payload_bytes, 5120);
}

#[test]
fn endpoint_transitions_keep_fin_state_coherent() {
    let mut state = ClientRelayState::new();
    state.record_local_eof();
    assert!(!state.endpoint.local_open);
    assert!(state.endpoint.pending_local_fin);

    state.record_local_fin_sent();
    assert!(!state.endpoint.pending_local_fin);
    assert!(state.endpoint.local_fin_sent);
    assert!(!state.endpoint.terminal_fin_replayed);

    state.record_terminal_fin_replayed();
    assert!(state.endpoint.terminal_fin_replayed);
}

#[test]
fn completion_requires_terminal_control_ack_and_reorder_drain() {
    let stream_id = StreamId(9);
    let limits = MuxLimits::default();
    let mut state = ClientRelayState::new();
    let mut send_stream = ReliableSendStream::new(stream_id, limits);
    let mut recv_stream = ReliableRecvStream::new(stream_id, limits);
    let mut sender_queue = ReliableRelaySenderQueue::default();

    state.record_local_eof();
    state.record_remote_finished();
    assert!(!state.is_finished(&send_stream, &recv_stream, &sender_queue));

    state.record_local_fin_sent();
    assert!(!state.is_finished(&send_stream, &recv_stream, &sender_queue));
    state.record_terminal_fin_replayed();
    assert!(state.is_finished(&send_stream, &recv_stream, &sender_queue));

    send_stream
        .send_data(Bytes::from_static(b"sent"))
        .expect("retain unique bytes until Data ACK");
    assert!(!state.is_finished(&send_stream, &recv_stream, &sender_queue));
    send_stream
        .apply_ack(&[OffsetRange { start: 0, end: 4 }])
        .expect("ACK assigned request bytes");
    assert!(state.is_finished(&send_stream, &recv_stream, &sender_queue));

    recv_stream
        .receive_data(4, Bytes::from_static(b"tail"))
        .expect("buffer out-of-order response data");
    assert!(!state.is_finished(&send_stream, &recv_stream, &sender_queue));
    recv_stream
        .receive_data(0, Bytes::from_static(b"head"))
        .expect("close response hole");
    assert!(state.is_finished(&send_stream, &recv_stream, &sender_queue));

    sender_queue.push_data(Bytes::from_static(b"queued"));
    assert!(!state.is_finished(&send_stream, &recv_stream, &sender_queue));
    assert!(sender_queue.pop_front().is_some());
    assert!(state.is_finished(&send_stream, &recv_stream, &sender_queue));
}

#[tokio::test]
async fn request_ack_releases_load_only_after_final_original_flight() {
    let stream_id = StreamId(613);
    let context = ClientPathContext::new(
        vec![
            "tcp://127.0.0.1:11613"
                .parse::<PathSpec>()
                .expect("test path"),
        ],
        test_security(),
        ResourceLimits::default(),
    )
    .expect("client context");
    let (commands, _receivers) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(opened_request_path(stream_id, 0, commands), 8);
    let owner = remotes.paths[0].instance();
    context.install_relay_path_instance_for_test(owner);
    let lease = context
        .try_reserve_relay_path_load_if_unchanged(owner, TrafficClass::Throughput, 0, 0)
        .expect("active OriginalData load");
    remotes.commit_path_instance_load_claim(owner, lease);

    let limits = MuxLimits::default();
    let mut send_stream = ReliableSendStream::new(stream_id, limits);
    let first = send_stream
        .send_data(Bytes::from_static(b"first"))
        .expect("first request extent");
    let second = send_stream
        .send_data(Bytes::from_static(b"second"))
        .expect("second request extent");
    let first_end = crate::protocol::frame::reliable_stream_frame_extent(&first)
        .expect("first extent")
        .1;
    let final_end = crate::protocol::frame::reliable_stream_frame_extent(&second)
        .expect("second extent")
        .1;
    let mut sender = RequestSenderService::new(stream_id);
    sender.record_original_frame_for_test(owner, &first);
    sender.record_original_frame_for_test(owner, &second);
    let mut sender_queue = ReliableRelaySenderQueue::default();
    let mut state = ClientRelayState::new();

    let apply = |state: &mut ClientRelayState,
                 sender: &mut RequestSenderService,
                 sender_queue: &mut ReliableRelaySenderQueue,
                 remotes: &mut ReliableRelayRemoteSet,
                 send_stream: &mut ReliableSendStream,
                 complete,
                 ranges| {
        apply_client_stream_ack(
            ClientStreamAckContext {
                state,
                sender,
                sender_queue,
                context: &context,
                remotes,
                send_stream,
                path_snapshot: None,
                relay_lane: TrafficClass::Throughput,
            },
            stream_id,
            complete,
            ranges,
        )
    };

    apply(
        &mut state,
        &mut sender,
        &mut sender_queue,
        &mut remotes,
        &mut send_stream,
        false,
        vec![OffsetRange { start: 0, end: 2 }],
    )
    .expect("partial ACK");
    assert!(remotes.paths[0].has_load_reservation());
    assert_eq!(
        context.health().lock().expect("path health").tcp[0].active_flows,
        1,
    );

    assert_eq!(
        apply(
            &mut state,
            &mut sender,
            &mut sender_queue,
            &mut remotes,
            &mut send_stream,
            false,
            vec![OffsetRange { start: 0, end: 2 }],
        )
        .expect("replayed ACK"),
        0,
    );
    assert!(remotes.paths[0].has_load_reservation());

    apply(
        &mut state,
        &mut sender,
        &mut sender_queue,
        &mut remotes,
        &mut send_stream,
        false,
        vec![OffsetRange {
            start: 0,
            end: first_end,
        }],
    )
    .expect("first extent ACK");
    assert!(remotes.paths[0].has_load_reservation());

    apply(
        &mut state,
        &mut sender,
        &mut sender_queue,
        &mut remotes,
        &mut send_stream,
        true,
        vec![OffsetRange {
            start: 0,
            end: final_end,
        }],
    )
    .expect("final request ACK");
    assert!(!remotes.paths[0].has_load_reservation());
    assert_eq!(
        context.health().lock().expect("path health").tcp[0].active_flows,
        0,
        "the final unique OriginalData ACK returns the attachment to idle",
    );
}

#[tokio::test]
async fn ambiguous_prefix_ack_cannot_withdraw_a_fresh_request_tail_beyond_the_horizon() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(614);
    let context = ClientPathContext::new(
        ["quic://127.0.0.1:11614", "tcp://127.0.0.1:11615"]
            .into_iter()
            .map(|path| path.parse::<PathSpec>().expect("test path"))
            .collect(),
        test_security(),
        ResourceLimits::default(),
    )
    .expect("client context");

    let (quic_commands, mut quic_receivers) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_request_path_with_underlay(stream_id, UnderlayProtocol::Udp, 0, quic_commands),
        8,
    );
    consume_path_proof(&mut quic_receivers);
    let quic = remotes.paths[0].instance();
    let (tcp_commands, mut tcp_receivers) = reliable_path_command_channels(8);
    let _ = remotes.attach_candidate(opened_request_path_with_underlay(
        stream_id,
        UnderlayProtocol::Tcp,
        1,
        tcp_commands,
    ));
    consume_path_proof(&mut tcp_receivers);
    let tcp = remotes
        .paths
        .iter()
        .find(|path| path.key().underlay == UnderlayProtocol::Tcp)
        .expect("TCP alternate")
        .instance();
    for instance in [quic, tcp] {
        context.install_relay_path_instance_for_test(instance);
        match instance.key.underlay {
            UnderlayProtocol::Tcp => context.mark_tcp_path_open_success(
                instance.key.index,
                Duration::from_millis(20),
                TrafficClass::Throughput,
            ),
            UnderlayProtocol::Udp => {
                context.mark_udp_path_open_success(instance.key.index, Duration::from_millis(20));
            }
        }
        context.mark_relay_path_rate_sample_for_test(
            instance.key,
            PathRateSample::new(4 * 1024 * 1024, Duration::from_millis(20))
                .expect("bulk rate sample"),
        );
    }

    let mut send_stream = ReliableSendStream::new(stream_id, limits);
    let prefix = send_stream
        .send_data(Bytes::from_static(b"acked-prefix"))
        .expect("send ACKed prefix");
    let horizon = crate::protocol::frame::reliable_stream_frame_extent(&prefix)
        .expect("prefix extent")
        .1;
    let tail = send_stream
        .send_data(Bytes::from_static(b"silent-tail"))
        .expect("send silent tail");
    let mut sender = RequestSenderService::new(stream_id);
    sender.record_original_frame_for_test(quic, &prefix);
    sender.record_reinjected_frame_for_test(tcp, &prefix);
    sender.record_original_frame_for_test(quic, &tail);
    let mut sender_queue = ReliableRelaySenderQueue::default();
    let mut state = ClientRelayState::new();
    assert!(!update_request_path_staleness(
        &mut state,
        &mut sender,
        &context,
        &remotes,
        &[],
        TrafficClass::Throughput,
        stream_id,
    ));
    assert_eq!(
        state.progress.request_path_staleness.next_deadline(),
        None,
        "retained work without a complete ACK horizon cannot arm withdrawal",
    );
    assert_eq!(
        apply_client_stream_ack(
            ClientStreamAckContext {
                state: &mut state,
                sender: &mut sender,
                sender_queue: &mut sender_queue,
                context: &context,
                remotes: &mut remotes,
                send_stream: &mut send_stream,
                path_snapshot: context.reliable_path_snapshot_for_instance(quic),
                relay_lane: TrafficClass::Throughput,
            },
            stream_id,
            true,
            vec![OffsetRange::new(0, horizon).expect("prefix ACK range")],
        )
        .expect("apply prefix ACK"),
        horizon as usize,
    );
    assert_eq!(
        state.progress.last_send_ack.horizon(),
        Some(horizon),
        "the complete prefix ACK establishes exactly the prefix horizon",
    );
    assert_eq!(
        state.progress.request_path_staleness.next_deadline(),
        None,
        "ambiguous prefix delivery cannot carry its old owner clock into a fresh tail at the horizon",
    );
    assert!(!sender.request_path_is_stale(quic));
    assert!(
        sender
            .unacked_original_paths_before(&remotes, horizon)
            .is_empty(),
        "the fresh tail is retained for exact ACK and failure recovery but is not an authoritative omission",
    );
    assert!(
        arm_request_path_staleness_model_publication(
            &context,
            &sender,
            &remotes,
            horizon,
            context.path_model_generation(),
        )
        .is_none(),
        "a retained fresh tail at or above the complete horizon cannot arm a model wake",
    );
}

#[tokio::test]
async fn request_staleness_reconciles_when_first_alternate_becomes_schedulable() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(615);
    let context = ClientPathContext::new(
        [
            "tcp://127.0.0.1:11616?initial-srtt-s=0.02&initial-rate-mbps=100",
            "tcp://127.0.0.1:11617?initial-srtt-s=0.02&initial-rate-mbps=100",
        ]
        .into_iter()
        .map(|path| path.parse::<PathSpec>().expect("test path"))
        .collect(),
        test_security(),
        ResourceLimits::default(),
    )
    .expect("client context");

    let (owner_commands, mut owner_receivers) = reliable_path_command_channels(8);
    let mut remotes = ReliableRelayRemoteSet::new(
        opened_request_path_with_underlay(stream_id, UnderlayProtocol::Tcp, 0, owner_commands),
        8,
    );
    consume_path_proof(&mut owner_receivers);
    let owner = remotes.paths[0].instance();
    let (alternate_commands, mut alternate_receivers) = reliable_path_command_channels(8);
    let _ = remotes.attach_candidate(opened_request_path_with_underlay(
        stream_id,
        UnderlayProtocol::Tcp,
        1,
        alternate_commands,
    ));
    consume_path_proof(&mut alternate_receivers);
    let alternate = remotes
        .paths
        .iter()
        .find(|path| path.instance() != owner)
        .expect("alternate attachment")
        .instance();
    for instance in [owner, alternate] {
        context.install_relay_path_instance_for_test(instance);
    }
    context.mark_tcp_path_failure(alternate.key.index);
    context.mark_tcp_path_failure(alternate.key.index);

    let mut sender = RequestSenderService::new(stream_id);
    assert!(
        !sender.request_path_has_reinjection_path(
            &context,
            &remotes,
            owner,
            TrafficClass::Throughput,
        ),
        "the failed attached alternate must begin dynamically unavailable",
    );
    let mut send_stream = ReliableSendStream::new(stream_id, limits);
    let omitted = send_stream
        .send_data(Bytes::from_static(b"authoritative-omission"))
        .expect("send omitted owner range");
    let marker = send_stream
        .send_data(Bytes::from_static(b"later-acked-marker"))
        .expect("send later ACK marker");
    let (marker_start, marker_end, _) =
        crate::protocol::frame::reliable_stream_frame_extent(&marker).expect("marker extent");
    sender.record_original_frame_for_test(owner, &omitted);
    sender.record_original_frame_for_test(owner, &marker);

    let mut sender_queue = ReliableRelaySenderQueue::default();
    let mut state = ClientRelayState::new();
    apply_client_stream_ack(
        ClientStreamAckContext {
            state: &mut state,
            sender: &mut sender,
            sender_queue: &mut sender_queue,
            context: &context,
            remotes: &mut remotes,
            send_stream: &mut send_stream,
            path_snapshot: context.reliable_path_snapshot_for_instance(owner),
            relay_lane: TrafficClass::Throughput,
        },
        stream_id,
        true,
        vec![OffsetRange::new(marker_start, marker_end).expect("marker ACK range")],
    )
    .expect("apply complete marker ACK");
    assert_eq!(state.progress.last_send_ack.horizon(), Some(marker_end));
    assert_eq!(
        sender
            .unacked_original_paths_before(&remotes, marker_end)
            .as_slice(),
        &[owner],
        "the omitted owner is authoritative below the complete horizon",
    );
    assert_eq!(
        state.progress.request_path_staleness.next_deadline(),
        None,
        "without a schedulable distinct alternate no withdrawal clock may exist",
    );

    let membership_generation = remotes.membership_generation();
    let observed_model_generation = context.path_model_generation();
    let mut publication = arm_request_path_staleness_model_publication(
        &context,
        &sender,
        &remotes,
        marker_end,
        observed_model_generation,
    )
    .expect("one authoritative owner must arm the model publication");
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut publication)
            .await
            .is_err(),
        "an unchanged path-model generation remains pending instead of polling",
    );

    assert!(context.mark_tcp_path_reserved_open_success_for_instance(
        alternate.key.index,
        alternate.path_instance_id,
        Duration::from_millis(20),
    ));
    tokio::time::timeout(Duration::from_millis(50), &mut publication)
        .await
        .expect("the unavailable-to-available model transition must wake reconciliation");
    assert_eq!(remotes.membership_generation(), membership_generation);
    assert_eq!(state.progress.last_send_ack.horizon(), Some(marker_end));
    assert!(sender.request_path_has_reinjection_path(
        &context,
        &remotes,
        owner,
        TrafficClass::Throughput,
    ));

    assert!(!update_request_path_staleness(
        &mut state,
        &mut sender,
        &context,
        &remotes,
        &[],
        TrafficClass::Throughput,
        stream_id,
    ));
    assert!(
        state
            .progress
            .request_path_staleness
            .next_deadline()
            .is_some(),
        "the generation wake must arm the existing omitted owner on the next serialized pass",
    );
    assert!(
        !sender.request_path_is_stale(owner),
        "arming a persistence clock is not immediate withdrawal",
    );

    let current_generation = context.path_model_generation();
    let mut quiet_publication = arm_request_path_staleness_model_publication(
        &context,
        &sender,
        &remotes,
        marker_end,
        current_generation,
    )
    .expect("the authoritative owner remains observable while its clock runs");
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut quiet_publication)
            .await
            .is_err(),
        "re-arming at the current generation cannot create a busy loop",
    );
}

#[test]
fn persistent_request_revalidation_uses_the_selected_targets_queue_view() {
    let instance = |index, id| RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index,
        },
        path_instance_id: CarrierPathInstanceId::from_raw(id),
        attachment_id: id,
    };
    let first = instance(1, 21);
    let second = instance(2, 22);
    let limits = MuxLimits {
        max_path_flight_bytes: 16 * 1024,
        max_repair_bytes: 16 * 1024,
        max_reliable_relay_chunk_bytes: 4 * 1024,
        ..MuxLimits::default()
    };
    let mut snapshot = PathSnapshot::new(PathId(1), UnderlayProtocol::Tcp, 50.0, 500_000_000.0);
    snapshot.data_level_limit_bytes =
        reliable_bulk_product_windows(limits).per_output_product_limit_bytes;
    let target_window =
        reliable_product_recovery_window_bytes(Some(snapshot), TrafficClass::Throughput, limits);
    let emergency_quantum =
        adaptive_reliable_relay_reinjection_bytes(Some(snapshot), TrafficClass::Throughput, limits)
            .max(reliable_bulk_carrier_feed_quantum_bytes(limits));
    let mut sender_queue = ReliableRelaySenderQueue::default();
    sender_queue.push_data(Bytes::from(vec![0x62; target_window]));
    sender_queue.push_critical_reinjection_with_cause(
        Frame::StreamData {
            stream_id: StreamId(103),
            offset: target_window as u64,
            payload: Bytes::from(vec![0x63; emergency_quantum]),
        },
        RelaySendCause::ClientPathFailureReinjection(ClientReinjectionOutputIdentity::new(first)),
    );

    assert_eq!(
        request_target_reinjection_service_limit(
            first,
            snapshot,
            &sender_queue,
            0,
            limits.max_repair_bytes,
            limits,
        ),
        target_window.saturating_sub(emergency_quantum),
        "only the selected target's queued repair consumes its exact reserve",
    );
    assert_eq!(
        request_target_reinjection_service_limit(
            second,
            snapshot,
            &sender_queue,
            0,
            limits.max_repair_bytes,
            limits,
        ),
        target_window,
        "final persistent-gap revalidation must not charge another target's bound repair",
    );
}

#[tokio::test]
async fn persistent_request_ack_gap_commits_frontier_before_filling_service_window() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(104);
    let context = ClientPathContext::new(
        [
            "tcp://127.0.0.1:11104",
            "tcp://127.0.0.1:11105",
            "tcp://127.0.0.1:11106",
        ]
        .into_iter()
        .map(|path| path.parse::<PathSpec>().expect("test path"))
        .collect(),
        test_security(),
        ResourceLimits::default(),
    )
    .expect("client context");

    let (owner_commands, mut owner_receivers) = reliable_path_command_channels(8);
    let mut remotes =
        ReliableRelayRemoteSet::new(opened_request_path(stream_id, 0, owner_commands), 8);
    consume_path_proof(&mut owner_receivers);
    let owner = remotes.paths[0].instance();

    let (first_commands, mut first_receivers) = reliable_path_command_channels(8);
    let _ = remotes.attach_candidate(opened_request_path(stream_id, 1, first_commands));
    consume_path_proof(&mut first_receivers);
    let first_alternate = remotes
        .paths
        .iter()
        .find(|path| path.key().index == 1)
        .expect("first alternate")
        .instance();

    let (second_commands, mut second_receivers) = reliable_path_command_channels(8);
    let _ = remotes.attach_candidate(opened_request_path(stream_id, 2, second_commands));
    consume_path_proof(&mut second_receivers);
    let second_alternate = remotes
        .paths
        .iter()
        .find(|path| path.key().index == 2)
        .expect("second alternate")
        .instance();
    for alternate in [first_alternate, second_alternate] {
        context.install_relay_path_instance_for_test(alternate);
        context.mark_tcp_path_open_success(
            alternate.key.index,
            Duration::from_millis(20),
            TrafficClass::Throughput,
        );
        context.mark_relay_path_rate_sample_for_test(
            alternate.key,
            PathRateSample::new(4 * 1024 * 1024, Duration::from_millis(20))
                .expect("bulk rate sample"),
        );
    }

    let quantum = MAX_RELIABLE_SERVICE_QUANTUM_BYTES;
    let mut send_stream = ReliableSendStream::new(stream_id, limits);
    let original = send_stream
        .send_data(Bytes::from(vec![0x61; quantum * 3]))
        .expect("sparse request flight");
    let mut sender = RequestSenderService::new(stream_id);
    sender.record_original_frame_for_test(owner, &original);
    let ack_ranges = vec![
        OffsetRange {
            start: 0,
            end: quantum as u64,
        },
        OffsetRange {
            start: (quantum * 2) as u64,
            end: (quantum * 3) as u64,
        },
    ];
    let validated_ack = validate_stream_ack(true, ack_ranges, send_stream.next_offset())
        .expect("valid sparse request ACK");
    let _ = send_stream.apply_validated_ack(&validated_ack);
    let mut state = ClientRelayState::new();
    update_reinjection_authoritative_ack_snapshot(
        &mut state.progress.last_send_ack,
        &validated_ack,
    );
    state.progress.last_send_ack_frontier = quantum as u64;
    drop(
        remotes
            .remove_path_instance(owner)
            .expect("remove missing original output"),
    );

    let authoritative_ranges = state.progress.last_send_ack.ranges().to_vec();
    let observed_at = Instant::now();
    let expired = observed_at - Duration::from_secs(1);
    assert!(
        state
            .progress
            .ack_gap_reinjection
            .observe_recovery_timing(
                true,
                &authoritative_ranges,
                true,
                Some(ReliableDataAckGapTiming {
                    assignment_at: expired,
                    loss_at: Some(expired),
                    fallback_at: expired,
                }),
                Some(Duration::ZERO),
                observed_at,
            )
            .is_some_and(|deadline| deadline <= observed_at)
    );

    let scored_path = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 100.0, 1_500_000.0);
    let scored_frontier_bytes = adaptive_reliable_relay_reinjection_bytes(
        Some(scored_path),
        TrafficClass::Throughput,
        limits,
    );
    assert!(scored_frontier_bytes < quantum);
    let frontier_frame = send_stream
        .retransmission_frames_for_normalized_ack_gaps(&authoritative_ranges, scored_frontier_bytes)
        .into_iter()
        .next()
        .expect("request frontier repair frame");
    let mut sender_queue = ReliableRelaySenderQueue::default();
    sender_queue.push_reinjection(frontier_frame);
    let blocked_bytes = sender_queue.bytes();

    let blocked = evaluate_client_data_ack_reinjection(
        &mut state,
        &mut sender,
        &mut sender_queue,
        &context,
        &remotes,
        &send_stream,
        Some(scored_path),
        TrafficClass::Throughput,
        stream_id,
    );
    assert!(blocked.persistent_ready);
    assert!(blocked.frame_count >= 2);
    assert_eq!(
        sender_queue.bytes(),
        blocked_bytes,
        "a blocked request frontier must not enqueue later service-window repairs"
    );
    assert!(sender_queue.pop_front().is_some());
    assert!(sender_queue.is_empty());

    let admitted = evaluate_client_data_ack_reinjection(
        &mut state,
        &mut sender,
        &mut sender_queue,
        &context,
        &remotes,
        &send_stream,
        Some(scored_path),
        TrafficClass::Throughput,
        stream_id,
    );
    assert!(admitted.persistent_ready);
    assert!(admitted.frame_count >= 2);
    assert!(
        sender_queue.bytes() > scored_frontier_bytes,
        "committing the request frontier unlocks the measured service window"
    );
    let (_, frontier) = sender_queue
        .pop_front()
        .expect("committed request frontier");
    let (frontier_frame, frontier_cause) = match &frontier.kind {
        ReliableRelayQueuedWorkKind::Reinjection { frame, cause } => (frame.clone(), *cause),
        _ => panic!("request frontier must be reinjection work"),
    };
    assert!(matches!(
        frontier.kind,
        ReliableRelayQueuedWorkKind::Reinjection {
            frame: Frame::StreamData {
                offset,
                payload,
                ..
            },
            ..
        } if offset == quantum as u64 && payload.len() == scored_frontier_bytes
    ));
    let (_, later) = sender_queue
        .pop_front()
        .expect("later service-window repair");
    assert!(matches!(
        later.kind,
        ReliableRelayQueuedWorkKind::Reinjection {
            frame: Frame::StreamData { offset, .. },
            ..
        } if offset > quantum as u64
    ));
    while sender_queue.pop_front().is_some() {}

    sender_queue.push_reinjection_with_cause(frontier_frame, frontier_cause);
    let committed_copy = sender
        .dispatch_client_queued_work(
            &context,
            TrafficClass::Throughput,
            &mut remotes,
            &mut send_stream,
            &mut sender_queue,
            scored_frontier_bytes,
            ReliableDataAckFrontierState::AuthoritativeGap,
        )
        .await
        .expect("actual alternate carrier commitment");
    assert!(matches!(
        committed_copy,
        ClientQueuedDispatch::Reinjection {
            accepted_copy_deadline,
            ..
        } if accepted_copy_deadline > Instant::now()
    ));
    assert!(sender_queue.is_empty());

    let globally_suppressed = evaluate_client_data_ack_reinjection(
        &mut state,
        &mut sender,
        &mut sender_queue,
        &context,
        &remotes,
        &send_stream,
        Some(scored_path),
        TrafficClass::Throughput,
        stream_id,
    );
    assert!(globally_suppressed.persistent_ready);
    assert!(
        sender_queue.is_empty(),
        "one live accepted exact-range copy suppresses stacking on every alternate until its immutable D",
    );
    assert!(
        state.progress.data_ack_reinjection_at.is_some(),
        "the global accepted-copy D must remain the ACK-gap wake",
    );
}
