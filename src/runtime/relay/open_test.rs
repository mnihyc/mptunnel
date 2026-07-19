use super::*;
use crate::config::{ResourceLimits, SecurityConfig, SharedSecret};
use crate::model::capacity::reliable_relay_buffer_len;
use crate::mux::MuxLimits;
use crate::protocol::PathId;
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandReceivers, recv_reliable_path_command,
    reliable_path_command_channels, try_recv_reliable_path_command,
};
use crate::runtime::stream::ReliablePathStreamOutput;
use crate::transport::PathSpec;
use tokio::sync::mpsc;

fn security() -> SecurityConfig {
    SecurityConfig::encrypted(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    )
}

fn pending_stream_for_test(
    stream_id: StreamId,
    underlay: UnderlayProtocol,
    path_index: usize,
) -> (OpenedRemoteStream, ReliablePathCommandReceivers) {
    let mux_limits = MuxLimits::default();
    let (commands, command_rx) = reliable_path_command_channels(4);
    let (_frames_tx, frames_rx) = mpsc::channel(4);
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
    )
}

#[tokio::test]
async fn relay_attach_open_timeout_bounds_pending_connection_setup() {
    let result = relay_path_open_with_deadline(
        tokio::time::Instant::now() + Duration::from_millis(1),
        std::future::pending::<Result<(), RuntimeError>>(),
    )
    .await;

    assert!(matches!(result, Err(RuntimeError::PathOpenTimedOut)));
}

#[test]
fn cold_quic_attachment_budget_covers_serialized_setup_exchanges() {
    let path = "udp://127.0.0.1:11095"
        .parse::<PathSpec>()
        .expect("UDP path");
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("context");
    context.mark_udp_path_probe_success(0, Duration::from_millis(180));
    let key = RelayPathKey {
        underlay: UnderlayProtocol::Udp,
        index: 0,
    };
    let snapshot = context.reliable_path_snapshot(key);
    let timeouts = reliable_relay_attach_open_timeouts(&context, key);

    assert_eq!(timeouts.live, transport_pto_from_snapshot(snapshot));
    assert_eq!(
        timeouts.setup,
        path_open_pto(snapshot, true).saturating_mul(path_open_serialized_exchanges(snapshot)),
    );
    assert!(timeouts.setup > timeouts.live);
}

#[test]
fn dropped_pending_attachment_queues_detach_and_local_close() {
    let stream_id = StreamId(92);
    let (opened, mut receivers) = pending_stream_for_test(stream_id, UnderlayProtocol::Udp, 0);
    drop(opened);

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
async fn explicitly_closed_pending_attachment_detaches_before_local_close() {
    let stream_id = StreamId(93);
    let (opened, mut receivers) = pending_stream_for_test(stream_id, UnderlayProtocol::Udp, 0);
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

#[test]
fn dropping_initial_open_attempt_rolls_back_scheduler_load() {
    let path = "tcp://127.0.0.1:11096"
        .parse::<PathSpec>()
        .expect("tcp path");
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("context");
    let mut attempted = Vec::new();
    let attempt = reserve_reliable_initial_open_attempt(
        &context,
        TrafficClass::Throughput,
        PATH_OPEN_SCORE_BYTES,
        &mut attempted,
    )
    .expect("reserve attempt")
    .expect("candidate");
    assert_eq!(
        context.health().lock().expect("path health").tcp[0].active_flows,
        1
    );

    drop(attempt);
    assert_eq!(
        context.health().lock().expect("path health").tcp[0].active_flows,
        0
    );
}

#[test]
fn dropping_pending_attachment_releases_load_before_stream_cleanup() {
    let path = "udp://127.0.0.1:11097"
        .parse::<PathSpec>()
        .expect("udp path");
    let context =
        ClientPathContext::new(vec![path], security(), ResourceLimits::default()).expect("context");
    let key = RelayPathKey {
        underlay: UnderlayProtocol::Udp,
        index: 0,
    };
    let lease = context
        .reserve_relay_path_load(key, TrafficClass::Throughput)
        .expect("load lease");
    let stream_id = StreamId(94);
    let (opened, mut receivers) = pending_stream_for_test(stream_id, UnderlayProtocol::Udp, 0);
    let opened = opened.with_load_lease(lease);

    drop(opened);
    assert_eq!(
        context.health().lock().expect("path health").udp[0].active_flows,
        0
    );
    assert!(matches!(
        try_recv_reliable_path_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamDetach { stream_id: id })) if id == stream_id
    ));
}

#[test]
fn initial_open_retry_uses_fresh_stream_id() {
    let context = ClientPathContext::new(
        vec![
            "tcp://127.0.0.1:10132?srtt-ms=20&rate-mbps=100"
                .parse()
                .expect("first path"),
            "tcp://127.0.0.1:10133?srtt-ms=80&rate-mbps=200"
                .parse()
                .expect("second path"),
        ],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    let mut attempted = Vec::new();
    let first = reserve_reliable_initial_open_attempt(
        &context,
        TrafficClass::Latency,
        PATH_OPEN_SCORE_BYTES,
        &mut attempted,
    )
    .expect("first attempt")
    .expect("first candidate");
    let first_key = first.key;
    let first_stream_id = first.stream_id;
    drop(first);
    context.mark_relay_path_failure(first_key.underlay, first_key.index);

    let second = reserve_reliable_initial_open_attempt(
        &context,
        TrafficClass::Latency,
        PATH_OPEN_SCORE_BYTES,
        &mut attempted,
    )
    .expect("second attempt")
    .expect("second candidate");
    assert_ne!(first_stream_id, second.stream_id);
    assert_ne!(first_key, second.key);
}

#[test]
fn retryable_initial_open_failure_cools_path_for_next_attempt() {
    let context = ClientPathContext::new(
        vec![
            "tcp://127.0.0.1:10132?srtt-ms=20&rate-mbps=100"
                .parse()
                .expect("failed path"),
            "tcp://127.0.0.1:10133?srtt-ms=80&rate-mbps=100"
                .parse()
                .expect("survivor path"),
        ],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    let failed = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    let failed_lease = context
        .reserve_reliable_stream_path(TrafficClass::Latency, PATH_OPEN_SCORE_BYTES, &[])
        .expect("failed-path reservation");
    assert_eq!(failed_lease.key(), failed);
    drop(failed_lease);
    mark_reliable_initial_open_retryable_failure(&context, failed);

    assert_eq!(
        context
            .reserve_reliable_stream_path(TrafficClass::Latency, PATH_OPEN_SCORE_BYTES, &[])
            .map(|lease| lease.key()),
        Some(RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 1,
        })
    );
}
