use super::*;
use crate::config::{ClientSecurityConfig, ResourceLimits, SharedSecret};
use crate::model::capacity::{
    PATH_OPEN_SCORE_BYTES, adaptive_reliable_relay_chunk_bytes, relay_lane_startup_chunk_bytes,
    reliable_relay_buffer_len,
};
use crate::model::path::RelayPathKey;
use crate::mux::MuxLimits;
use crate::mux::stream::ReliableSendStream;
use crate::protocol::{Frame, PathId, ResetReason, StreamId, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::ClientPathContext;
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandReceivers, recv_reliable_path_command,
    reliable_path_command_channels, reliable_path_command_pending_bytes,
    try_recv_reliable_path_command, try_recv_reliable_path_priority_command,
};
use crate::runtime::stream::{OpenedRemoteStream, ReliablePathStream, ReliablePathStreamOutput};
use crate::scheduler::TrafficClass;
use crate::transport::PathSpec;
use bytes::Bytes;
use std::collections::HashSet;
use std::time::Duration;
use tokio::sync::mpsc;

fn security() -> ClientSecurityConfig {
    ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    )
}

fn context(paths: &[&str]) -> ClientPathContext {
    ClientPathContext::new(
        paths
            .iter()
            .map(|path| path.parse::<PathSpec>().expect("test path"))
            .collect(),
        security(),
        ResourceLimits::default(),
    )
    .expect("path context")
}

fn opened_stream(
    stream_id: StreamId,
    underlay: UnderlayProtocol,
    path_index: usize,
) -> (
    OpenedRemoteStream,
    ReliablePathCommandReceivers,
    mpsc::Sender<Result<Frame, RuntimeError>>,
) {
    let mux_limits = MuxLimits::default();
    let (commands, command_rx) = reliable_path_command_channels(4);
    let (frames_tx, frames_rx) = mpsc::channel(4);
    (
        OpenedRemoteStream::pending(
            ReliablePathStream {
                stream_id,
                max_offset: mux_limits.max_stream_window_bytes,
                lane: TrafficClass::Throughput,
                underlay,
                max_frame_payload_bytes: reliable_relay_buffer_len(mux_limits),
                output: ReliablePathStreamOutput::fixed(
                    underlay,
                    PathId(path_index as u16),
                    commands,
                    mux_limits,
                ),
                frames: frames_rx.into(),
            },
            path_index,
        ),
        command_rx,
        frames_tx,
    )
}

#[tokio::test]
async fn failed_path_proof_enqueue_retries_without_sticky_state() {
    let stream_id = StreamId(106);
    let context = context(&["tcp://127.0.0.1:11090"]);
    let (opened, mut receivers, _frames) = opened_stream(stream_id, UnderlayProtocol::Tcp, 0);
    for _ in 0..4 {
        opened
            .stream()
            .try_enqueue_request_control_frame(Frame::StreamAck {
                stream_id,
                complete: false,
                ranges: Vec::new(),
            })
            .expect("fill priority queue");
    }
    let mut remotes = ReliableRelayRemoteSet::new(opened, 4);
    assert!(remotes.paths[0].path_proof_id.is_none());
    for _ in 0..4 {
        assert!(matches!(
            try_recv_reliable_path_priority_command(&mut receivers),
            Some(ReliablePathCommand::SendFrame(Frame::StreamAck { .. }))
        ));
    }

    remotes.retry_pending_path_proofs(&context);

    assert!(remotes.paths[0].path_proof_id.is_some());
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
    ));
}

#[tokio::test]
async fn queued_path_proof_keeps_identity_until_generation_changes() {
    let context = context(&["tcp://127.0.0.1:11091"]);
    let (opened, mut receivers, _frames) = opened_stream(StreamId(108), UnderlayProtocol::Tcp, 0);
    let mut remotes = ReliableRelayRemoteSet::new(opened, 4);
    let first_proof = remotes.paths[0].path_proof_id.expect("initial proof");
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
    ));

    remotes.retry_pending_path_proofs(&context);
    assert_eq!(remotes.paths[0].path_proof_id, Some(first_proof));
    assert!(try_recv_reliable_path_priority_command(&mut receivers).is_none());

    context.health().lock().expect("path health").tcp[0].invalidate_path_proofs();
    remotes.retry_pending_path_proofs(&context);
    assert_ne!(remotes.paths[0].path_proof_id, Some(first_proof));
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData { .. }))
    ));
}

#[tokio::test]
async fn duplicate_attachment_releases_pending_stream_and_load() {
    let stream_id = StreamId(94);
    let context = context(&["quic://127.0.0.1:11094"]);
    let (first, _first_receivers, _first_frames) =
        opened_stream(stream_id, UnderlayProtocol::Udp, 0);
    let mut remotes = ReliableRelayRemoteSet::new(first, 4);
    let (duplicate, mut receivers, _frames) = opened_stream(stream_id, UnderlayProtocol::Udp, 0);
    let lease = context
        .reserve_relay_path_load(
            RelayPathKey {
                underlay: UnderlayProtocol::Udp,
                index: 0,
            },
            TrafficClass::Throughput,
        )
        .expect("pending-open load");

    assert_eq!(
        remotes.attach_candidate(duplicate.with_load_lease(lease)),
        ReliableRelayAttachOutcome::RejectedDuplicate
    );
    assert_eq!(remotes.path_keys().len(), 1);
    assert_eq!(
        context.health().lock().expect("path health").udp[0].active_flows,
        0
    );
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), recv_reliable_path_command(&mut receivers))
            .await
            .expect("detach deadline"),
        Some(ReliablePathCommand::SendFrame(Frame::StreamDetach { stream_id: id })) if id == stream_id
    ));
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), recv_reliable_path_command(&mut receivers))
            .await
            .expect("close deadline"),
        Some(ReliablePathCommand::CloseStream(id)) if id == stream_id
    ));
}

#[tokio::test]
async fn candidate_membership_drops_open_load_until_product_claim_commits() {
    let context = context(&["tcp://127.0.0.1:11100", "tcp://127.0.0.1:11101"]);
    let stream_id = StreamId(97);
    let (first, _receivers, _frames) = opened_stream(stream_id, UnderlayProtocol::Tcp, 0);
    let mut remotes = ReliableRelayRemoteSet::new(first, 4);
    let (candidate, _receivers, _frames) = opened_stream(stream_id, UnderlayProtocol::Tcp, 1);
    let key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 1,
    };
    let open_lease = context
        .reserve_relay_path_load(key, TrafficClass::Throughput)
        .expect("candidate-open load");

    assert_eq!(
        remotes.attach_candidate(candidate.with_load_lease(open_lease)),
        ReliableRelayAttachOutcome::Attached
    );
    let instance = remotes
        .path_instance_for_key(key)
        .expect("candidate instance");
    assert!(!remotes.paths[1].has_load_reservation());
    assert_eq!(
        context.health().lock().expect("path health").tcp[1].active_flows,
        0
    );

    let product_lease = context
        .reserve_relay_path_load(key, TrafficClass::Throughput)
        .expect("product load");
    remotes.commit_path_instance_load_claim(instance, product_lease);
    assert!(remotes.paths[1].has_load_reservation());
    assert_eq!(
        context.health().lock().expect("path health").tcp[1].active_flows,
        1
    );
}

#[tokio::test]
async fn membership_generation_fences_replaced_path_incarnations() {
    let stream_id = StreamId(941);
    let (first, _receivers, _frames) = opened_stream(stream_id, UnderlayProtocol::Udp, 0);
    let mut remotes = ReliableRelayRemoteSet::new(first, 4);
    let first_generation = remotes.membership_generation();
    let first_instance = remotes.path_instances()[0];
    assert_eq!(
        remotes.path_position_at_generation(first_generation, first_instance),
        Some(0)
    );

    remotes
        .remove_path_instance(first_instance)
        .expect("remove first incarnation");
    let (replacement, _receivers, _frames) = opened_stream(stream_id, UnderlayProtocol::Udp, 0);
    assert_eq!(
        remotes.attach(replacement),
        ReliableRelayAttachOutcome::Attached
    );
    let replacement_generation = remotes.membership_generation();
    let replacement_instance = remotes.path_instances()[0];

    assert_eq!(replacement_instance.key, first_instance.key);
    assert_ne!(replacement_instance, first_instance);
    assert_eq!(
        remotes.path_position_at_generation(first_generation, first_instance),
        None
    );
    assert_eq!(
        remotes.path_position_at_generation(replacement_generation, first_instance),
        None
    );
    assert_eq!(
        remotes.path_position_at_generation(replacement_generation, replacement_instance),
        Some(0)
    );
}

#[tokio::test]
async fn attachments_are_append_only_and_counted_without_roles() {
    let stream_id = StreamId(942);
    let (first, _receivers, _frames) = opened_stream(stream_id, UnderlayProtocol::Tcp, 0);
    let mut remotes = ReliableRelayRemoteSet::new(first, 4);
    let (second, _receivers, _frames) = opened_stream(stream_id, UnderlayProtocol::Udp, 0);
    assert_eq!(
        remotes.attach_candidate(second),
        ReliableRelayAttachOutcome::Attached
    );

    assert_eq!(
        remotes.path_keys(),
        vec![
            RelayPathKey {
                underlay: UnderlayProtocol::Tcp,
                index: 0,
            },
            RelayPathKey {
                underlay: UnderlayProtocol::Udp,
                index: 0,
            },
        ]
    );
    assert_eq!(remotes.accepted_path_count(), 2);
}

#[tokio::test]
async fn close_depublishes_load_before_carrier_cleanup_waits() {
    let context = context(&["tcp://127.0.0.1:11095"]);
    let key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    let lease = context
        .reserve_relay_path_load(key, TrafficClass::Throughput)
        .expect("path load");
    let mux_limits = MuxLimits::default();
    let (commands, receivers) = reliable_path_command_channels(1);
    commands
        .send_control(ReliablePathCommand::CloseStream(StreamId(999)))
        .await
        .expect("prefill control queue");
    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let opened = OpenedRemoteStream::pending(
        ReliablePathStream {
            stream_id: StreamId(95),
            max_offset: mux_limits.max_stream_window_bytes,
            lane: TrafficClass::Throughput,
            underlay: UnderlayProtocol::Tcp,
            max_frame_payload_bytes: reliable_relay_buffer_len(mux_limits),
            output: ReliablePathStreamOutput::fixed(
                UnderlayProtocol::Tcp,
                PathId(0),
                commands,
                mux_limits,
            ),
            frames: frames_rx.into(),
        },
        0,
    )
    .with_load_lease(lease);
    let mut remotes = ReliableRelayRemoteSet::new(opened, 1);
    let generation = remotes.membership_generation();
    let instance = remotes.path_instances()[0];

    let mut close = Box::pin(remotes.close_all());
    assert!(matches!(
        futures::poll!(&mut close),
        std::task::Poll::Pending
    ));
    assert_eq!(
        context.health().lock().expect("path health").tcp[0].active_flows,
        0
    );

    drop(receivers);
    close.await;
    assert_ne!(remotes.membership_generation(), generation);
    assert_eq!(
        remotes.path_position_at_generation(generation, instance),
        None
    );
}

#[tokio::test]
async fn idle_reset_retires_membership_before_blocked_carrier_publication() {
    let stream_id = StreamId(951);
    let mux_limits = MuxLimits::default();
    let (commands, mut receivers) = reliable_path_command_channels(1);
    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let opened = OpenedRemoteStream::pending(
        ReliablePathStream {
            stream_id,
            max_offset: mux_limits.max_stream_window_bytes,
            lane: TrafficClass::Throughput,
            underlay: UnderlayProtocol::Tcp,
            max_frame_payload_bytes: reliable_relay_buffer_len(mux_limits),
            output: ReliablePathStreamOutput::fixed(
                UnderlayProtocol::Tcp,
                PathId(0),
                commands.clone(),
                mux_limits,
            ),
            frames: frames_rx.into(),
        },
        0,
    );
    let mut remotes = ReliableRelayRemoteSet::new(opened, 1);
    if let Some(setup) = try_recv_reliable_path_priority_command(&mut receivers) {
        receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&setup));
    }
    commands
        .send_control(ReliablePathCommand::CloseStream(StreamId(999)))
        .await
        .expect("prefill control queue");

    remotes.retire_all_with_reset(ResetReason::TimedOut);
    assert!(
        remotes.is_empty(),
        "Product membership retires without waiting for carrier queue space"
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
}

#[tokio::test]
async fn successful_close_preserves_fin_detach_close_order() {
    let stream_id = StreamId(96);
    let mux_limits = MuxLimits::default();
    let (commands, mut receivers) = reliable_path_command_channels(4);
    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let opened = OpenedRemoteStream::pending(
        ReliablePathStream {
            stream_id,
            max_offset: mux_limits.max_stream_window_bytes,
            lane: TrafficClass::Throughput,
            underlay: UnderlayProtocol::Tcp,
            max_frame_payload_bytes: reliable_relay_buffer_len(mux_limits),
            output: ReliablePathStreamOutput::fixed(
                UnderlayProtocol::Tcp,
                PathId(0),
                commands.clone(),
                mux_limits,
            ),
            frames: frames_rx.into(),
        },
        0,
    );
    let mut remotes = ReliableRelayRemoteSet::new(opened, 1);
    if let Some(setup) = try_recv_reliable_path_priority_command(&mut receivers) {
        receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&setup));
    }

    commands
        .send_stream_ordered_frame(
            Frame::StreamFin {
                stream_id,
                final_offset: 0,
            },
            TrafficClass::Throughput,
        )
        .await
        .expect("queue FIN replay");
    remotes.close_all_ordered().await;

    let first = recv_reliable_path_command(&mut receivers)
        .await
        .expect("FIN replay");
    assert!(matches!(
        &first,
        ReliablePathCommand::SendFrame(Frame::StreamFin {
            stream_id: received,
            ..
        }) if *received == stream_id
    ));
    receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&first));

    let second = recv_reliable_path_command(&mut receivers)
        .await
        .expect("ordered detach");
    assert!(matches!(
        &second,
        ReliablePathCommand::SendFrame(Frame::StreamDetach { stream_id: received })
            if *received == stream_id
    ));
    receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&second));

    let third = recv_reliable_path_command(&mut receivers)
        .await
        .expect("ordered close");
    assert!(matches!(
        &third,
        ReliablePathCommand::CloseStream(received) if *received == stream_id
    ));
    receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&third));
}

#[tokio::test]
async fn terminal_product_failure_resets_before_queued_payload() {
    let stream_id = StreamId(97);
    let mux_limits = MuxLimits::default();
    let (commands, mut receivers) = reliable_path_command_channels(4);
    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let opened = OpenedRemoteStream::pending(
        ReliablePathStream {
            stream_id,
            max_offset: mux_limits.max_stream_window_bytes,
            lane: TrafficClass::Throughput,
            underlay: UnderlayProtocol::Tcp,
            max_frame_payload_bytes: reliable_relay_buffer_len(mux_limits),
            output: ReliablePathStreamOutput::fixed(
                UnderlayProtocol::Tcp,
                PathId(0),
                commands.clone(),
                mux_limits,
            ),
            frames: frames_rx.into(),
        },
        0,
    );
    let mut remotes = ReliableRelayRemoteSet::new(opened, 1);
    if let Some(setup) = try_recv_reliable_path_priority_command(&mut receivers) {
        receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&setup));
    }

    commands
        .send_stream_ordered_frame(
            Frame::StreamData {
                stream_id,
                offset: 0,
                payload: Bytes::from_static(b"stale after product failure"),
            },
            TrafficClass::Throughput,
        )
        .await
        .expect("queue stale payload");
    remotes.reset_all(ResetReason::RemoteClosed).await;

    let terminal = recv_reliable_path_command(&mut receivers)
        .await
        .expect("terminal reset");
    assert!(matches!(
        terminal,
        ReliablePathCommand::ResetAndCloseStream {
            stream_id: received,
            reason: ResetReason::RemoteClosed,
        } if received == stream_id
    ));
    assert!(
        try_recv_reliable_path_command(&mut receivers).is_none(),
        "payload queued before a terminal reset must not reach the carrier writer"
    );
}

#[test]
fn recovery_candidates_skip_failed_path_unless_it_is_last_resort() {
    let tcp0 = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    let tcp1 = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 1,
    };
    let excluded = HashSet::from([tcp0]);

    assert_eq!(
        reliable_relay_recovery_attach_candidates(vec![tcp0, tcp1], &excluded, false),
        vec![tcp1]
    );
    assert!(reliable_relay_recovery_attach_candidates(vec![tcp0], &excluded, false).is_empty());
    assert_eq!(
        reliable_relay_recovery_attach_candidates(vec![tcp0], &excluded, true),
        vec![tcp0]
    );
}

#[test]
fn inflight_open_claim_remains_excluded_from_last_resort_recovery() {
    let claimed = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 1,
    };
    let excluded = HashSet::from([claimed]);
    let inflight = HashSet::from([claimed]);
    let last_resort = reliable_relay_recovery_attach_candidates(vec![claimed], &excluded, true);

    assert_eq!(last_resort, vec![claimed]);
    assert!(reliable_relay_exclude_inflight_open_claims(last_resort, &inflight).is_empty());
}

#[tokio::test]
async fn additional_paths_are_ranked_by_metrics_across_carriers() {
    let context = context(&[
        "quic://127.0.0.1:10179?initial-srtt-s=0.1&initial-rate-mbps=20",
        "tcp://127.0.0.1:10180?initial-srtt-s=0.01&initial-rate-mbps=500",
        "quic://127.0.0.1:10181?initial-srtt-s=0.02&initial-rate-mbps=200",
    ]);
    let (slow, _receivers, _frames) = opened_stream(StreamId(160), UnderlayProtocol::Udp, 0);
    let remotes = ReliableRelayRemoteSet::new(slow, 4);

    assert_eq!(
        reliable_relay_additional_path_candidates(
            &context,
            &remotes,
            TrafficClass::Throughput,
            64 * 1024,
        )
        .first()
        .copied(),
        Some(RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        })
    );
}

#[tokio::test]
async fn available_path_precedes_faster_locally_configured_backup() {
    let context = context(&[
        "tcp://127.0.0.1:10182?initial-srtt-s=0.08&initial-rate-mbps=100",
        "tcp://127.0.0.1:10183?initial-srtt-s=0.005&initial-rate-mbps=1000&backup=true",
        "quic://127.0.0.1:10184?initial-srtt-s=0.1&initial-rate-mbps=20",
    ]);
    let (attached, _receivers, _frames) = opened_stream(StreamId(161), UnderlayProtocol::Udp, 0);
    let remotes = ReliableRelayRemoteSet::new(attached, 4);

    let candidates = reliable_relay_additional_path_candidates(
        &context,
        &remotes,
        TrafficClass::Throughput,
        64 * 1024,
    );
    assert_eq!(
        candidates.first().copied(),
        Some(RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        })
    );
}

#[tokio::test]
async fn reinjection_candidate_uses_distinct_metric_ranked_carrier() {
    let context = context(&[
        "tcp://127.0.0.1:11168?initial-srtt-s=0.02&initial-rate-mbps=500",
        "quic://127.0.0.1:11169?initial-srtt-s=0.18&initial-rate-mbps=40",
    ]);
    let (slow, _receivers, _frames) = opened_stream(StreamId(151), UnderlayProtocol::Udp, 0);
    let remotes = ReliableRelayRemoteSet::new(slow, 4);

    assert_eq!(
        reliable_relay_reinjection_path_candidates(
            &context,
            &remotes,
            TrafficClass::Throughput,
            64 * 1024,
        )
        .first()
        .copied(),
        Some(RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        })
    );
}

#[test]
fn outstanding_original_data_requests_a_reinjection_alternative() {
    let mut send_stream = ReliableSendStream::new(StreamId(152), MuxLimits::default());
    send_stream
        .send_data(Bytes::from_static(b"unacknowledged"))
        .expect("original transmission");

    assert!(reliable_relay_should_open_reinjection_alternative(
        TrafficClass::Throughput,
        &send_stream,
        false,
        ReliableRelayAttachMode::Any,
    ));
    assert!(!reliable_relay_should_open_reinjection_alternative(
        TrafficClass::Throughput,
        &send_stream,
        true,
        ReliableRelayAttachMode::Any,
    ));
}

#[test]
fn pending_fin_is_enqueued_before_attachment_publish() {
    let stream_id = StreamId(153);
    let (opened, mut receivers, _frames) = opened_stream(stream_id, UnderlayProtocol::Tcp, 0);
    let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
    send_stream
        .send_data(Bytes::from_static(b"request"))
        .expect("request data");

    send_request_attach_control_frames(opened.stream(), &send_stream, true)
        .expect("enqueue final offset");
    assert!(matches!(
        try_recv_reliable_path_priority_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamFin {
            stream_id: id,
            final_offset,
        })) if id == stream_id && final_offset == 7
    ));
}

#[test]
fn attachment_scoring_uses_bounded_demand_quanta() {
    let mux_limits = MuxLimits::default();
    let send_stream = ReliableSendStream::new(StreamId(12), mux_limits);

    assert_eq!(
        reliable_relay_attach_payload_bytes(&send_stream, TrafficClass::Latency, mux_limits),
        PATH_OPEN_SCORE_BYTES
    );
    assert_eq!(
        reliable_relay_attach_payload_bytes(&send_stream, TrafficClass::Throughput, mux_limits),
        reliable_relay_buffer_len(mux_limits)
    );

    let striping_quantum = reliable_relay_bulk_striping_payload_bytes(&send_stream, mux_limits);
    let proof_quantum = reliable_relay_additional_path_open_payload_bytes(&send_stream, mux_limits);
    assert_eq!(
        striping_quantum,
        adaptive_reliable_relay_chunk_bytes(None, TrafficClass::Throughput, mux_limits)
    );
    assert!(proof_quantum >= PATH_OPEN_SCORE_BYTES);
    assert!(proof_quantum <= relay_lane_startup_chunk_bytes(TrafficClass::Latency, mux_limits));
    assert!(proof_quantum <= striping_quantum);
}
