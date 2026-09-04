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
use crate::model::timing::{ReliableDataAckGapTiming, reliable_data_retransmission_interval};
use crate::mux::MuxLimits;
use crate::mux::stream::{ReliableRecvStream, ReliableSendStream, validate_stream_ack};
use crate::protocol::{Frame, OffsetRange, PathId, StreamId, UnderlayProtocol};
use crate::runtime::path::ClientPathContext;
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandSender, reliable_path_command_channels,
    try_recv_reliable_path_priority_command,
};
use crate::runtime::relay::control::arm_request_path_staleness_model_publication;
use crate::runtime::relay::io::stream_ack_gap_reinjection_frames_normalized;
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
async fn persistent_request_ack_gap_commits_only_the_ranked_frontier_quantum() {
    let limits = MuxLimits::default();
    let stream_id = StreamId(104);
    let context = ClientPathContext::new(
        [
            "tcp://127.0.0.1:11104?initial-srtt-s=0.02&initial-rate-mbps=200",
            "tcp://127.0.0.1:11106?initial-srtt-s=0.02&initial-rate-mbps=200",
            "tcp://127.0.0.1:11105?initial-srtt-s=0.02&initial-rate-mbps=200",
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
    let _ = remotes.attach_candidate(opened_request_path(stream_id, 2, first_commands));
    consume_path_proof(&mut first_receivers);
    let first_alternate = remotes
        .path_instance_for_key(RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 2,
        })
        .expect("first alternate");

    let (second_commands, mut second_receivers) = reliable_path_command_channels(8);
    let _ = remotes.attach_candidate(opened_request_path(stream_id, 1, second_commands));
    consume_path_proof(&mut second_receivers);
    let second_alternate = remotes
        .path_instance_for_key(RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 1,
        })
        .expect("second alternate");
    for path in [owner, first_alternate, second_alternate] {
        context.install_relay_path_instance_for_test(path);
        match path.key.underlay {
            UnderlayProtocol::Tcp => context.mark_tcp_path_open_success(
                path.key.index,
                Duration::from_millis(20),
                TrafficClass::Throughput,
            ),
            UnderlayProtocol::Udp => {
                context.mark_udp_path_open_success(path.key.index, Duration::from_millis(20));
            }
        }
        context.mark_relay_path_rate_sample_for_test(
            path.key,
            PathRateSample::new(4 * 1024 * 1024, Duration::from_millis(20))
                .expect("bulk rate sample"),
        );
    }

    let quantum = MAX_RELIABLE_SERVICE_QUANTUM_BYTES;
    let mut send_stream = ReliableSendStream::new(stream_id, limits);
    let original = send_stream
        .send_data(Bytes::from(vec![0x61; quantum * 5]))
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
        OffsetRange {
            start: (quantum * 4) as u64,
            end: (quantum * 5) as u64,
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

    let authoritative_ranges = state.progress.last_send_ack.ranges().to_vec();
    let scored_path = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 100.0, 1_500_000.0);
    let scored_frontier_bytes = adaptive_reliable_relay_reinjection_bytes(
        Some(scored_path),
        TrafficClass::Throughput,
        limits,
    );
    assert!(scored_frontier_bytes < quantum);
    let empty_queue = ReliableRelaySenderQueue::default();
    let original_observation = sender.data_ack_gap_reinjection_model(
        &context,
        &remotes,
        &send_stream,
        &empty_queue,
        &authoritative_ranges,
        scored_frontier_bytes,
        TrafficClass::Throughput,
    );
    assert!(original_observation.has_live_original_path);
    let original_assignment_at = original_observation
        .original_assignment_at
        .expect("live request owner assignment");

    let early_path = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 100.0, 1.0);
    let early_frontier_bytes = adaptive_reliable_relay_reinjection_bytes(
        Some(early_path),
        TrafficClass::Throughput,
        limits,
    );
    for _ in 0..10 {
        for alternate in [first_alternate, second_alternate] {
            context.mark_relay_path_rate_sample_for_test(
                alternate.key,
                PathRateSample::new(4 * 1024 * 1024, Duration::from_millis(20))
                    .expect("measured alternate rate sample"),
            );
        }
    }
    let mut predeadline_sender = RequestSenderService::new(stream_id);
    predeadline_sender.record_original_frame_for_test(owner, &original);
    let mut predeadline_queue = ReliableRelaySenderQueue::default();
    let mut predeadline_state = ClientRelayState::new();
    update_reinjection_authoritative_ack_snapshot(
        &mut predeadline_state.progress.last_send_ack,
        &validated_ack,
    );
    predeadline_state.progress.last_send_ack_frontier = quantum as u64;
    let predeadline_observed_at = Instant::now();
    let predeadline = evaluate_client_data_ack_reinjection(
        &mut predeadline_state,
        &mut predeadline_sender,
        &mut predeadline_queue,
        &context,
        &remotes,
        &send_stream,
        Some(early_path),
        TrafficClass::Throughput,
        stream_id,
    );
    assert_eq!(predeadline.frame_count, 0);
    assert_eq!(predeadline_queue.bytes(), 0);
    assert!(
        predeadline_state
            .progress
            .ack_gap_reinjection
            .next_reinjection_deadline()
            .is_some_and(|deadline| deadline > predeadline_observed_at),
        "request ACK-gap Apply cannot precede the retained T_c deadline",
    );

    let zero_resources = ResourceLimits {
        max_repair_bytes: 0,
        ..ResourceLimits::default()
    };
    let zero_context = ClientPathContext::new(
        [
            "tcp://127.0.0.1:11104?initial-srtt-s=0.02&initial-rate-mbps=200",
            "tcp://127.0.0.1:11106?initial-srtt-s=0.02&initial-rate-mbps=200",
            "tcp://127.0.0.1:11105?initial-srtt-s=0.02&initial-rate-mbps=200",
        ]
        .into_iter()
        .map(|path| path.parse::<PathSpec>().expect("zero-authority path"))
        .collect(),
        test_security(),
        zero_resources,
    )
    .expect("zero-authority context");
    for path in [owner, first_alternate, second_alternate] {
        zero_context.install_relay_path_instance_for_test(path);
        match path.key.underlay {
            UnderlayProtocol::Tcp => zero_context.mark_tcp_path_open_success(
                path.key.index,
                Duration::from_millis(20),
                TrafficClass::Throughput,
            ),
            UnderlayProtocol::Udp => {
                zero_context.mark_udp_path_open_success(path.key.index, Duration::from_millis(20));
            }
        }
    }
    let mut zero_sender = RequestSenderService::new(stream_id);
    zero_sender.record_original_frame_for_test(owner, &original);
    let mut zero_queue = ReliableRelaySenderQueue::default();
    let mut zero_state = ClientRelayState::new();
    update_reinjection_authoritative_ack_snapshot(
        &mut zero_state.progress.last_send_ack,
        &validated_ack,
    );
    zero_state.progress.last_send_ack_frontier = quantum as u64;
    let zero = evaluate_client_data_ack_reinjection(
        &mut zero_state,
        &mut zero_sender,
        &mut zero_queue,
        &zero_context,
        &remotes,
        &send_stream,
        Some(early_path),
        TrafficClass::Throughput,
        stream_id,
    );
    assert!(zero.has_multipath_alternative);
    assert_eq!(zero.frame_count, 0);
    assert_eq!(zero_queue.bytes(), 0);
    assert_eq!(
        zero_state
            .progress
            .ack_gap_reinjection
            .next_reinjection_deadline(),
        None,
        "Q=0 cannot manufacture a request recovery epoch before Apply",
    );
    assert_eq!(zero_sender.live_owner_frontier_floor_deadline(), None);

    for _ in 0..10 {
        context.mark_relay_path_rate_sample_for_test(
            owner.key,
            PathRateSample::new(4 * 1024 * 1024, Duration::from_secs(1))
                .expect("slow owner rate sample"),
        );
        for alternate in [first_alternate, second_alternate] {
            context.mark_relay_path_rate_sample_for_test(
                alternate.key,
                PathRateSample::new(4 * 1024 * 1024, Duration::from_millis(20))
                    .expect("fast alternate rate sample"),
            );
        }
    }
    let mut early_sender = RequestSenderService::new(stream_id);
    early_sender.record_original_frame_for_test(owner, &original);
    let mut early_queue = ReliableRelaySenderQueue::default();
    let mut early_state = ClientRelayState::new();
    update_reinjection_authoritative_ack_snapshot(
        &mut early_state.progress.last_send_ack,
        &validated_ack,
    );
    early_state.progress.last_send_ack_frontier = quantum as u64;
    let early_observation = early_sender.data_ack_gap_reinjection_model(
        &context,
        &remotes,
        &send_stream,
        &early_queue,
        &authoritative_ranges,
        early_frontier_bytes,
        TrafficClass::Throughput,
    );
    assert!(early_observation.reinjection_target.is_some());
    let expected_owner_completion = early_observation
        .original_path_timing
        .and_then(|snapshot| crate::scheduler::score_path(snapshot, TrafficClass::Throughput, 0))
        .filter(|score| score.eta_ms.is_finite())
        .map(|score| Duration::from_secs_f64(score.eta_ms.max(0.0) / 1000.0));
    assert_eq!(
        early_observation.owner_completion, expected_owner_completion,
        "the already-accepted owner frontier must not be charged as a new payload a second time",
    );
    assert!(
        early_observation
            .reinjection_completion
            .zip(early_observation.owner_completion)
            .is_some_and(|(alternate, owner)| alternate < owner),
        "the integration fixture requires a measured completion-winning alternate",
    );
    let early_assignment_at = early_observation
        .original_assignment_at
        .expect("early live request owner assignment");
    let early_observed_at = Instant::now();
    let early_deadline = early_state
        .progress
        .ack_gap_reinjection
        .observe_recovery_timing(
            true,
            &authoritative_ranges,
            true,
            Some(ReliableDataAckGapTiming {
                assignment_at: early_assignment_at,
                loss_at: Some(early_assignment_at),
                fallback_at: early_observed_at + Duration::from_secs(60),
            }),
            early_observation.reinjection_completion,
            early_observation.owner_completion,
            early_observed_at,
        );
    assert!(
        early_deadline.is_some_and(|deadline| deadline <= early_observed_at),
        "loss-boundary request race must be due: deadline={early_deadline:?} completion={:?}",
        early_observation.reinjection_completion,
    );
    let early = evaluate_client_data_ack_reinjection(
        &mut early_state,
        &mut early_sender,
        &mut early_queue,
        &context,
        &remotes,
        &send_stream,
        Some(early_path),
        TrafficClass::Throughput,
        stream_id,
    );
    assert!(
        early.persistent_ready && early_queue.bytes() > 0,
        "persistent={} measured={} exhausted={} frames={} queued={} candidate_in={:?} fallback_in={:?} completion={:?} target={:?}",
        early.persistent_ready,
        early.has_measured_target,
        early.target_service_exhausted,
        early.frame_count,
        early_queue.bytes(),
        early_state
            .progress
            .ack_gap_reinjection
            .next_reinjection_deadline()
            .map(|deadline| deadline.saturating_duration_since(early_observed_at)),
        early_state
            .progress
            .ack_gap_reinjection
            .original_owner_recovery_deadline()
            .map(|deadline| deadline.saturating_duration_since(early_observed_at)),
        early_observation.reinjection_completion,
        early_observation.reinjection_target,
    );
    assert_eq!(
        early_sender.live_owner_frontier_floor_deadline(),
        None,
        "an early measured request race before owner fallback must leave the shared epoch unconsumed",
    );
    while early_queue.pop_front().is_some() {}
    let exact_owner_interval = reliable_data_retransmission_interval(
        Some(owner.key.underlay),
        context.reliable_path_snapshot_for_instance(owner),
    );
    tokio::time::sleep(exact_owner_interval + Duration::from_millis(10)).await;
    let fallback_at = Instant::now();
    early_state
        .progress
        .ack_gap_reinjection
        .observe_recovery_timing(
            true,
            &authoritative_ranges,
            true,
            Some(ReliableDataAckGapTiming {
                assignment_at: early_assignment_at,
                loss_at: Some(early_assignment_at),
                fallback_at,
            }),
            Some(Duration::ZERO),
            None,
            fallback_at,
        );
    let fallback_observation = early_sender.data_ack_gap_reinjection_model(
        &context,
        &remotes,
        &send_stream,
        &early_queue,
        &authoritative_ranges,
        early_frontier_bytes,
        TrafficClass::Throughput,
    );
    let (fallback_target, fallback_snapshot) = fallback_observation
        .reinjection_target
        .expect("fallback retains an exact measured request target");
    let fallback_service_limit = request_target_reinjection_service_limit(
        fallback_target.instance(),
        fallback_snapshot,
        &early_queue,
        fallback_observation.reinjection_target_flight_bytes,
        send_stream.reinjection_bytes(),
        limits,
    );
    let expected_fallback_extent = fallback_service_limit
        .min(fallback_observation.uniform_frontier_extent_bytes)
        .min(early_frontier_bytes);
    let fallback = evaluate_client_data_ack_reinjection(
        &mut early_state,
        &mut early_sender,
        &mut early_queue,
        &context,
        &remotes,
        &send_stream,
        Some(early_path),
        TrafficClass::Throughput,
        stream_id,
    );
    assert!(fallback.persistent_ready);
    assert_eq!(
        early_queue.bytes(),
        expected_fallback_extent,
        "post-fallback request recovery must admit the ranked frontier bounded by current target Product service",
    );
    assert!(
        early_sender.live_owner_frontier_floor_deadline().is_some(),
        "accepted recovery after owner fallback consumes the shared epoch",
    );
    let fallback_epoch_deadline = early_sender
        .live_owner_frontier_floor_deadline()
        .expect("accepted request recovery starts one shared epoch");
    early_sender.record_delivered_data(quantum);
    assert_eq!(
        early_sender.live_owner_frontier_floor_deadline(),
        Some(fallback_epoch_deadline),
        "new Product ACK progress must not postpone an already-started recovery epoch",
    );
    let observed_at = Instant::now();
    assert!(
        state
            .progress
            .ack_gap_reinjection
            .observe_recovery_timing(
                true,
                &authoritative_ranges,
                true,
                Some(ReliableDataAckGapTiming {
                    assignment_at: original_assignment_at,
                    loss_at: Some(original_assignment_at),
                    fallback_at: original_assignment_at,
                }),
                Some(Duration::ZERO),
                None,
                observed_at,
            )
            .is_some_and(|deadline| deadline <= observed_at)
    );

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
    assert_eq!(
        blocked.frame_count, 1,
        "the target-bound live-owner transaction may plan only its ranked frontier quantum",
    );
    assert_eq!(
        sender_queue.bytes(),
        blocked_bytes,
        "a blocked request frontier must not enqueue later service-window repairs"
    );
    assert!(sender_queue.pop_front().is_some());
    assert!(sender_queue.is_empty());

    let candidate_frames = stream_ack_gap_reinjection_frames_normalized(
        &send_stream,
        &authoritative_ranges,
        quantum.saturating_mul(2),
        scored_frontier_bytes,
        true,
        true,
        true,
    );
    assert!(candidate_frames.len() >= 3);
    sender_queue.push_reinjection(candidate_frames[1].clone());
    let middle_blocked_bytes = sender_queue.bytes();
    let middle_blocked = evaluate_client_data_ack_reinjection(
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
    assert_eq!(
        middle_blocked.frame_count, 1,
        "an unscored suffix cannot join the live-owner frontier transaction",
    );
    assert_eq!(
        sender_queue.bytes(),
        middle_blocked_bytes.saturating_add(scored_frontier_bytes),
        "a blocked middle request chunk may retain the committed frontier but must stop before later omitted ranges",
    );
    while sender_queue.pop_front().is_some() {}

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
    assert_eq!(
        admitted.frame_count, 1,
        "one live-owner recovery decision may commit only the quantum it ranked",
    );
    assert_eq!(
        sender_queue.bytes(),
        scored_frontier_bytes,
        "target Product headroom must not widen Apply beyond the ranked frontier quantum",
    );
    let (planner_target, planner_snapshot) = sender
        .data_ack_gap_reinjection_model(
            &context,
            &remotes,
            &send_stream,
            &ReliableRelaySenderQueue::default(),
            &authoritative_ranges,
            scored_frontier_bytes,
            TrafficClass::Throughput,
        )
        .reinjection_target
        .expect("admitted request batch retains its exact planned target");
    let natural_batch_bytes = sender_queue.bytes();
    let queued_after_front =
        sender_queue.request_target_queued_reinjection_bytes(planner_target.instance(), true);
    let (frontier_frame, frontier_cause, frontier_payload_bytes) =
        match sender_queue.front().map(|(_, work)| &work.kind) {
            Some(ReliableRelayQueuedWorkKind::Reinjection {
                frame: Frame::StreamData {
                    offset, payload, ..
                },
                cause,
            }) if *offset == quantum as u64 && payload.len() == scored_frontier_bytes => (
                Frame::StreamData {
                    stream_id,
                    offset: *offset,
                    payload: payload.clone(),
                },
                *cause,
                payload.len(),
            ),
            work => panic!("request frontier must remain the natural first repair: {work:?}"),
        };
    assert!(matches!(
        frontier_cause,
        RelaySendCause::PersistentClientAckGapReinjection(_)
    ));
    assert_eq!(
        sender_queue.request_target_queued_reinjection_bytes(planner_target.instance(), false),
        natural_batch_bytes,
        "the queued request batch must retain the planner's exact target identity",
    );
    assert_eq!(
        sender_queue.request_target_queued_reinjection_bytes(owner, false),
        0,
        "the target-bound batch must not consume an unrelated exact output's reserve",
    );
    assert_eq!(
        queued_after_front.saturating_add(frontier_payload_bytes),
        natural_batch_bytes,
        "excluding the current front must leave exactly the rest of the natural target-bound batch",
    );
    let planner_window = reliable_product_recovery_window_bytes(
        Some(planner_snapshot),
        TrafficClass::Throughput,
        limits,
    );
    let planner_original =
        usize::try_from(planner_snapshot.data_level_bytes_in_flight).unwrap_or(usize::MAX);
    let planner_emergency = adaptive_reliable_relay_reinjection_bytes(
        Some(planner_snapshot),
        TrafficClass::Throughput,
        limits,
    )
    .max(reliable_bulk_carrier_feed_quantum_bytes(limits));
    let planner_repair_cap = planner_window
        .saturating_sub(planner_original)
        .max(planner_emergency);
    let planner_accepted =
        sender.accepted_reinjected_data_bytes_for_test(planner_target.instance());
    let planner_k = planner_repair_cap.saturating_sub(planner_accepted);
    assert_eq!(
        natural_batch_bytes,
        planner_k.min(scored_frontier_bytes),
        "the request batch must equal the ranked frontier bounded by exact target K",
    );

    let apply_snapshot = sender
        .request_reinjection_target_snapshot_for_test(&context, &remotes, planner_target.instance())
        .expect("exact target remains observable immediately before Apply");
    let apply_window = reliable_product_recovery_window_bytes(
        Some(apply_snapshot),
        TrafficClass::Throughput,
        limits,
    );
    let apply_original =
        usize::try_from(apply_snapshot.data_level_bytes_in_flight).unwrap_or(usize::MAX);
    let apply_emergency = adaptive_reliable_relay_reinjection_bytes(
        Some(apply_snapshot),
        TrafficClass::Throughput,
        limits,
    )
    .max(reliable_bulk_carrier_feed_quantum_bytes(limits));
    let apply_repair_cap = apply_window
        .saturating_sub(apply_original)
        .max(apply_emergency);
    let apply_accepted = sender.accepted_reinjected_data_bytes_for_test(planner_target.instance());
    let apply_k_after_excluding_front =
        apply_repair_cap.saturating_sub(queued_after_front.saturating_add(apply_accepted));
    assert!(
        apply_k_after_excluding_front >= frontier_payload_bytes,
        "the natural batch is algebraically admissible before the writer reservation: target={:?} P={} O={} repair_cap={} B_plus_U_after_front={} J={} K_after_front={} front={}",
        planner_target.instance(),
        apply_window,
        apply_original,
        apply_repair_cap,
        queued_after_front,
        apply_accepted,
        apply_k_after_excluding_front,
        frontier_payload_bytes,
    );

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
        .unwrap_or_else(|error| {
            panic!(
                "the natural planner-admitted batch must commit its exact front: error={error:?} target={:?} planner_P={} planner_O={} planner_repair_cap={} planner_B=0 planner_U=0 planner_J={} planner_K={} batch={} apply_P={} apply_O={} apply_repair_cap={} apply_B_plus_U_after_front={} apply_J={} apply_K_after_front={} front={}",
                planner_target.instance(),
                planner_window,
                planner_original,
                planner_repair_cap,
                planner_accepted,
                planner_k,
                natural_batch_bytes,
                apply_window,
                apply_original,
                apply_repair_cap,
                queued_after_front,
                apply_accepted,
                apply_k_after_excluding_front,
                frontier_payload_bytes,
            )
        });
    assert!(matches!(
        committed_copy,
        ClientQueuedDispatch::Reinjection {
            payload_bytes,
            accepted_copy_deadline,
        } if payload_bytes == frontier_payload_bytes && accepted_copy_deadline > Instant::now()
    ));
    assert_eq!(
        sender_queue.bytes(),
        natural_batch_bytes.saturating_sub(frontier_payload_bytes),
    );
    assert!(
        sender_queue.is_empty(),
        "the accepted frontier must not leave an unscored live-owner suffix queued",
    );
    assert_eq!(
        sender_queue.request_target_queued_reinjection_bytes(planner_target.instance(), false),
        natural_batch_bytes.saturating_sub(frontier_payload_bytes),
    );
    while sender_queue.pop_front().is_some() {}

    let accepted_target_bytes =
        sender.accepted_reinjected_data_bytes_for_test(planner_target.instance());
    assert_eq!(
        accepted_target_bytes, frontier_payload_bytes,
        "the accepted exact target ledger must contain the committed frontier once",
    );
    let accepted_copy =
        sender.reinjection_suppression_deadline_for_frame(&frontier_frame, &remotes);
    assert!(
        accepted_copy.is_some_and(|deadline| deadline > Instant::now()),
        "the accepted exact range must retain its immutable suppression deadline",
    );

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
