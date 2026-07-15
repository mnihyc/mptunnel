use super::{
    ReliableRelayAttachMode, ReliableRelayAttachOutcome, ReliableRelayRemoteSet,
    attach_reliable_relay_paths, reliable_relay_active_path_candidates,
    reliable_relay_attach_payload_bytes, reliable_relay_attach_role,
    reliable_relay_bulk_striping_payload_bytes, reliable_relay_bulk_validation_payload_bytes,
    reliable_relay_exclude_inflight_open_claims, reliable_relay_recovery_attach_candidates,
    reliable_relay_repair_path_candidates, reliable_relay_should_race_repair,
};
use crate::config::{ResourceLimits, SecurityConfig, SharedSecret};
use crate::model::capacity::{
    PATH_OPEN_SCORE_BYTES, adaptive_reliable_relay_chunk_bytes, relay_lane_startup_chunk_bytes,
    reliable_relay_buffer_len,
};
use crate::model::path::RelayPathKey;
use crate::mux::MuxLimits;
use crate::mux::stream::ReliableSendStream;
use crate::protocol::{
    Frame, IngressKind, PathId, StreamFlags, StreamId, StreamOpenRole, TargetAddr, UnderlayProtocol,
};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::ClientPathContext;
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandReceivers, recv_reliable_path_command,
    reliable_path_command_channels, try_recv_reliable_path_command,
};
use crate::runtime::relay::open::{
    OpenedRemoteStream, ReliableRelayOpenSpec, UncommittedRemoteStreamGuard,
};
use crate::runtime::stream::{ReliablePathStream, ReliablePathStreamOutput};
use crate::scheduler::FlowLane;
use crate::transport::PathSpec;
use bytes::Bytes;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::sync::mpsc;

fn security() -> SecurityConfig {
    SecurityConfig::encrypted(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    )
}

fn opened_relay_stream_for_test(
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
        OpenedRemoteStream {
            path_index,
            stream: ReliablePathStream {
                stream_id,
                max_offset: mux_limits.max_stream_window_bytes,
                lane: FlowLane::Throughput,
                underlay,
                max_frame_payload_bytes: reliable_relay_buffer_len(mux_limits),
                output: ReliablePathStreamOutput::fixed(
                    underlay,
                    PathId(path_index as u16),
                    commands,
                    mux_limits,
                ),
                frames: frames_rx,
            },
        },
        command_rx,
        frames_tx,
    )
}

#[test]
fn dropped_uncommitted_stream_guard_queues_detach_and_local_close() {
    let stream_id = StreamId(92);
    let (opened, mut receivers, _frames) =
        opened_relay_stream_for_test(stream_id, UnderlayProtocol::Udp, 0);
    drop(UncommittedRemoteStreamGuard::new(opened.stream));

    assert!(matches!(
        try_recv_reliable_path_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamDetach { stream_id: id })) if id == stream_id
    ));
    assert!(matches!(
        try_recv_reliable_path_command(&mut receivers),
        Some(ReliablePathCommand::CloseStream(id)) if id == stream_id
    ));
}

#[tokio::test]
async fn rejected_opened_stream_detaches_peer_before_local_close() {
    let stream_id = StreamId(93);
    let (opened, mut receivers, _frames) =
        opened_relay_stream_for_test(stream_id, UnderlayProtocol::Udp, 0);
    opened.close().await;

    assert!(matches!(
        recv_reliable_path_command(&mut receivers).await,
        Some(ReliablePathCommand::SendFrame(Frame::StreamDetach { stream_id: id })) if id == stream_id
    ));
    assert!(matches!(
        recv_reliable_path_command(&mut receivers).await,
        Some(ReliablePathCommand::CloseStream(id)) if id == stream_id
    ));
}

#[tokio::test]
async fn duplicate_remote_set_attach_releases_the_uncommitted_stream() {
    let stream_id = StreamId(94);
    let (first, _first_receivers, _first_frames) =
        opened_relay_stream_for_test(stream_id, UnderlayProtocol::Udp, 0);
    let mut remotes = ReliableRelayRemoteSet::new(first, 4);
    let (duplicate, mut duplicate_receivers, _duplicate_frames) =
        opened_relay_stream_for_test(stream_id, UnderlayProtocol::Udp, 0);

    assert_eq!(
        remotes.attach_for_validation(duplicate),
        ReliableRelayAttachOutcome::RejectedDuplicate,
    );

    assert_eq!(remotes.path_keys().len(), 1);
    assert!(matches!(
        tokio::time::timeout(
            Duration::from_secs(1),
            recv_reliable_path_command(&mut duplicate_receivers),
        )
        .await
        .expect("duplicate detach deadline"),
        Some(ReliablePathCommand::SendFrame(Frame::StreamDetach { stream_id: id })) if id == stream_id
    ));
    assert!(matches!(
        tokio::time::timeout(
            Duration::from_secs(1),
            recv_reliable_path_command(&mut duplicate_receivers),
        )
        .await
        .expect("duplicate close deadline"),
        Some(ReliablePathCommand::CloseStream(id)) if id == stream_id
    ));
}

#[tokio::test]
async fn remote_set_close_depublishes_load_before_carrier_cleanup_waits() {
    let stream_id = StreamId(95);
    let path = "tcp://127.0.0.1:11095"
        .parse::<PathSpec>()
        .expect("tcp path");
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("context");
    context.reserve_tcp_path_load(0, FlowLane::Throughput);

    let mux_limits = MuxLimits::default();
    let (commands, receivers) = reliable_path_command_channels(1);
    commands
        .send_control(ReliablePathCommand::CloseStream(StreamId(999)))
        .await
        .expect("prefill carrier control queue");
    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let opened = OpenedRemoteStream {
        path_index: 0,
        stream: ReliablePathStream {
            stream_id,
            max_offset: mux_limits.max_stream_window_bytes,
            lane: FlowLane::Throughput,
            underlay: UnderlayProtocol::Tcp,
            max_frame_payload_bytes: reliable_relay_buffer_len(mux_limits),
            output: ReliablePathStreamOutput::fixed(
                UnderlayProtocol::Tcp,
                PathId(0),
                commands,
                mux_limits,
            ),
            frames: frames_rx,
        },
    };
    let mut remotes = ReliableRelayRemoteSet::new(opened, 1);

    let mut close = Box::pin(remotes.close_all(&context));
    assert!(matches!(
        futures::poll!(&mut close),
        std::task::Poll::Pending
    ));
    assert_eq!(
        context
            .health()
            .lock()
            .expect("client path health lock")
            .tcp[0]
            .active_flows,
        0,
        "carrier cleanup may wait, but scheduling ownership has already ended",
    );

    drop(receivers);
    close.await;
}

#[test]
fn recovery_attach_candidates_skip_failed_stream_path_when_alternative_exists() {
    let tcp0 = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    let tcp1 = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 1,
    };
    let udp0 = RelayPathKey {
        underlay: UnderlayProtocol::Udp,
        index: 0,
    };
    let excluded = HashSet::from([tcp0]);

    assert_eq!(
        reliable_relay_recovery_attach_candidates(vec![tcp0, tcp1, udp0], &excluded, false),
        vec![tcp1, udp0],
        "stream-local failover should not immediately reopen the same failed path while another candidate exists"
    );

    assert!(
        reliable_relay_recovery_attach_candidates(vec![tcp0], &excluded, false).is_empty(),
        "a failed path must not be reopened while the stream still has an attached survivor"
    );

    assert_eq!(
        reliable_relay_recovery_attach_candidates(vec![tcp0], &excluded, true),
        vec![tcp0],
        "a failed path remains retryable only as a last-resort route when no survivor is attached"
    );
}

#[test]
fn inflight_open_claim_stays_hard_after_soft_recovery_fallback() {
    let claimed = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 1,
    };
    let unclaimed = RelayPathKey {
        underlay: UnderlayProtocol::Udp,
        index: 2,
    };
    let soft_excluded = HashSet::from([claimed]);
    let inflight_claims = HashSet::from([claimed]);

    let last_resort =
        reliable_relay_recovery_attach_candidates(vec![claimed], &soft_excluded, true);
    assert_eq!(last_resort, vec![claimed]);
    assert!(
        reliable_relay_exclude_inflight_open_claims(last_resort, &inflight_claims).is_empty(),
        "soft last-resort recovery must never reopen a path whose validation task still owns the logical stream/path claim"
    );
    assert_eq!(
        reliable_relay_exclude_inflight_open_claims(vec![claimed, unclaimed], &inflight_claims,),
        vec![unclaimed],
        "an independent carrier remains available while the claimed open completes"
    );
}

#[tokio::test]
async fn sender_recovery_attach_api_cannot_bypass_inflight_claim() {
    let context = ClientPathContext::new(
        vec![
            "tcp://127.0.0.1:23211?srtt-ms=20&rate-mbps=100"
                .parse()
                .expect("active path"),
            "tcp://127.0.0.1:23212?srtt-ms=30&rate-mbps=200"
                .parse()
                .expect("claimed path"),
        ],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    let (opened, _receivers, _frames) =
        opened_relay_stream_for_test(StreamId(14), UnderlayProtocol::Tcp, 0);
    let mut remotes = ReliableRelayRemoteSet::new(opened, 4);
    let send_stream = ReliableSendStream::new(StreamId(14), MuxLimits::default());
    let spec = ReliableRelayOpenSpec {
        target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80))),
        ingress: IngressKind::Socks5,
    };
    let claimed = HashSet::from([RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 1,
    }]);

    let attached = attach_reliable_relay_paths(
        &context,
        &spec,
        FlowLane::Throughput,
        &mut remotes,
        &send_stream,
        false,
        ReliableRelayAttachMode::RecoveryRepair,
        &claimed,
    )
    .await
    .expect("claimed recovery is a clean no-op");
    assert_eq!(attached, 0);
    assert_eq!(remotes.path_keys().len(), 1);
}

#[tokio::test]
async fn relay_active_attach_candidates_are_metric_ordered_across_carriers() {
    let context = ClientPathContext::new(
        vec![
            "udp://127.0.0.1:10179?srtt-ms=100&rate-mbps=20"
                .parse()
                .expect("active slow udp path"),
            "tcp://127.0.0.1:10180?srtt-ms=10&rate-mbps=500"
                .parse()
                .expect("fast tcp path"),
            "udp://127.0.0.1:10181?srtt-ms=20&rate-mbps=200"
                .parse()
                .expect("moderate udp path"),
        ],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    let (active_udp, _commands, _frames) =
        opened_relay_stream_for_test(StreamId(160), UnderlayProtocol::Udp, 0);
    let remotes = ReliableRelayRemoteSet::new(active_udp, 4);

    let candidates =
        reliable_relay_active_path_candidates(&context, &remotes, FlowLane::Throughput, 64 * 1024);

    assert_eq!(
        candidates.first().copied(),
        Some(RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        }),
        "active UDP must not hide a better measured TCP candidate"
    );
}

#[tokio::test]
async fn repair_attach_candidates_cross_carrier_by_eta_not_active_family() {
    let context = ClientPathContext::new(
        vec![
            "tcp://127.0.0.1:11168?srtt-ms=20&rate-mbps=500"
                .parse()
                .expect("fast tcp repair path"),
            "udp://127.0.0.1:11169?srtt-ms=180&rate-mbps=40"
                .parse()
                .expect("slow active udp path"),
        ],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    let stream_id = StreamId(151);
    let (udp_stream, _udp_commands, _udp_frames) =
        opened_relay_stream_for_test(stream_id, UnderlayProtocol::Udp, 0);
    let remotes = ReliableRelayRemoteSet::new(udp_stream, 4);

    let candidates =
        reliable_relay_repair_path_candidates(&context, &remotes, FlowLane::Throughput, 64 * 1024);

    assert_eq!(
        candidates.first().copied(),
        Some(RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        })
    );
}

#[test]
fn throughput_repair_bytes_use_repair_attachment_role() {
    let mut send_stream = ReliableSendStream::new(StreamId(152), MuxLimits::default());
    send_stream
        .send_data(Bytes::from_static(b"repairable"), StreamFlags::NONE)
        .expect("send data");

    assert!(reliable_relay_should_race_repair(
        FlowLane::Throughput,
        &send_stream,
        false,
        ReliableRelayAttachMode::Any,
    ));
}

#[test]
fn response_recovery_after_request_fin_preserves_active_role() {
    let send_stream = ReliableSendStream::new(StreamId(153), MuxLimits::default());

    assert_eq!(
        reliable_relay_attach_role(
            FlowLane::Throughput,
            &send_stream,
            true,
            ReliableRelayAttachMode::RecoveryRepair,
        ),
        StreamOpenRole::Repair,
        "response-side timer recovery remains Repair after the request FIN"
    );
    assert_eq!(
        reliable_relay_attach_role(
            FlowLane::Throughput,
            &send_stream,
            true,
            ReliableRelayAttachMode::Any,
        ),
        StreamOpenRole::Active,
        "generic Any mode remains available for explicit Active failover"
    );
}

#[test]
fn reliable_relay_attach_scoring_keeps_interactive_repairs_small() {
    let mux_limits = MuxLimits::default();
    let send_stream = ReliableSendStream::new(StreamId(12), mux_limits);

    assert_eq!(
        reliable_relay_attach_payload_bytes(&send_stream, FlowLane::Latency, mux_limits),
        PATH_OPEN_SCORE_BYTES
    );
    assert_eq!(
        reliable_relay_attach_payload_bytes(&send_stream, FlowLane::Throughput, mux_limits),
        reliable_relay_buffer_len(mux_limits)
    );
}

#[test]
fn reliable_relay_bulk_admission_payload_uses_preemptible_quantum_not_inflight_ceiling() {
    let mux_limits = MuxLimits::default();
    let send_stream = ReliableSendStream::new(StreamId(12), mux_limits);
    let expected_quantum =
        adaptive_reliable_relay_chunk_bytes(None, FlowLane::Throughput, mux_limits);

    assert_eq!(
        reliable_relay_bulk_striping_payload_bytes(&send_stream, mux_limits),
        expected_quantum
    );
    let validation_quantum = reliable_relay_bulk_validation_payload_bytes(&send_stream, mux_limits);
    assert!(validation_quantum >= PATH_OPEN_SCORE_BYTES);
    assert!(validation_quantum <= relay_lane_startup_chunk_bytes(FlowLane::Latency, mux_limits));
    assert!(validation_quantum <= expected_quantum);
    assert!(expected_quantum < mux_limits.max_path_flight_bytes);
}
