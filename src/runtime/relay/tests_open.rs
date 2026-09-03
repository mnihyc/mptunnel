use super::*;
use crate::config::{ClientSecurityConfig, ResourceLimits, SharedSecret};
use crate::model::capacity::reliable_relay_buffer_len;
use crate::model::path::{RelayPathInstance, next_carrier_path_instance_id};
use crate::mux::MuxLimits;
use crate::protocol::PathId;
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandReceivers, recv_reliable_path_command,
    reliable_path_command_channels, try_recv_reliable_path_command,
};
use crate::runtime::path::tcp::group::ClientTcpEndpointControlState;
use crate::runtime::stream::ReliablePathStreamOutput;
use crate::transport::PathSpec;
use tokio::sync::mpsc;

fn security() -> ClientSecurityConfig {
    ClientSecurityConfig::for_test(
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

fn unsettled_initial_stream_for_test(
    stream_id: StreamId,
) -> (
    ReliablePathStream,
    mpsc::Sender<Result<Frame, RuntimeError>>,
    ReliablePathCommandReceivers,
) {
    let mux_limits = MuxLimits::default();
    let (commands, command_rx) = reliable_path_command_channels(4);
    let (frames_tx, frames_rx) = mpsc::channel(4);
    (
        ReliablePathStream {
            stream_id,
            max_offset: 0,
            lane: TrafficClass::Latency,
            underlay: UnderlayProtocol::Udp,
            max_frame_payload_bytes: reliable_relay_buffer_len(mux_limits),
            output: ReliablePathStreamOutput::fixed(
                UnderlayProtocol::Udp,
                PathId(0),
                commands,
                mux_limits,
            ),
            frames: frames_rx.into(),
        },
        frames_tx,
        command_rx,
    )
}

#[test]
fn return_plan_freezes_active_and_pending_configured_slots() {
    let context = ClientPathContext::new(
        [
            "tcp://127.0.0.1:11101?max-tcp-carriers=1",
            "quic://127.0.0.1:11102",
        ]
        .into_iter()
        .map(|path| path.parse::<PathSpec>().expect("test path"))
        .collect(),
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    let tcp = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    let tcp_instance = RelayPathInstance {
        key: tcp,
        path_instance_id: next_carrier_path_instance_id(),
        attachment_id: 0,
    };
    context.install_relay_path_instance_for_test(tcp_instance);

    let plan = freeze_reliable_relay_return_plan(&context, TrafficClass::Throughput)
        .expect("configured return slots");

    assert_eq!(plan.candidates().len(), 2);
    assert_eq!(plan.trigger_bytes(), 58_400);
    assert_eq!(plan.candidate_tier(), PathUsage::Available);
    assert_eq!(plan.candidates()[0].key, tcp);
    assert_eq!(
        plan.candidates()[0].path_instance_id,
        Some(tcp_instance.path_instance_id),
    );
    assert_eq!(
        plan.candidates()[1].key,
        RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        },
    );
    assert_eq!(
        plan.candidates()[1].path_instance_id,
        None,
        "a configured pending slot remains in the immutable candidate total",
    );
}

#[test]
fn expensive_policy_adds_cost_without_removing_return_membership() {
    let context = ClientPathContext::new(
        [
            "tcp://127.0.0.1:11103?max-tcp-carriers=1",
            "quic://127.0.0.1:11104?expensive=true",
        ]
        .into_iter()
        .map(|path| path.parse::<PathSpec>().expect("test path"))
        .collect(),
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    let plan = freeze_reliable_relay_return_plan(&context, TrafficClass::Throughput)
        .expect("configured return slots");

    assert_eq!(plan.candidates().len(), 2);
    assert_eq!(plan.trigger_bytes(), 58_400);
}

#[test]
fn backup_only_config_freezes_a_backup_singleton() {
    let context = ClientPathContext::new(
        vec![
            "tcp://127.0.0.1:11105?backup=true&max-tcp-carriers=1"
                .parse::<PathSpec>()
                .expect("backup path"),
        ],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");

    let plan = freeze_reliable_relay_return_plan(&context, TrafficClass::Throughput)
        .expect("backup return slot");

    assert_eq!(plan.candidates().len(), 1);
    assert_eq!(plan.trigger_bytes(), 0);
    assert_eq!(plan.candidate_tier(), PathUsage::Backup);
}

#[tokio::test]
async fn manually_disabled_slot_is_excluded_while_suspect_pending_slot_remains() {
    let context = ClientPathContext::new(
        [
            "tcp://127.0.0.1:11106?max-tcp-carriers=1",
            "quic://127.0.0.1:11107",
        ]
        .into_iter()
        .map(|path| path.parse::<PathSpec>().expect("test path"))
        .collect(),
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    context.set_tcp_endpoint_control(0, ClientTcpEndpointControlState::Disabled);

    let plan = freeze_reliable_relay_return_plan(&context, TrafficClass::Throughput)
        .expect("pending QUIC slot remains eligible");

    assert_eq!(plan.candidates().len(), 1);
    assert_eq!(plan.trigger_bytes(), 0);
    assert_eq!(
        plan.candidates()[0],
        ReliableRelayReturnCandidate {
            key: RelayPathKey {
                underlay: UnderlayProtocol::Udp,
                index: 0,
            },
            path_instance_id: None,
            ordinal: 0,
        },
    );
}

#[tokio::test]
async fn disabled_available_slot_falls_back_to_configured_backup_tier() {
    let context = ClientPathContext::new(
        [
            "tcp://127.0.0.1:11108?max-tcp-carriers=1",
            "quic://127.0.0.1:11109?backup=true",
        ]
        .into_iter()
        .map(|path| path.parse::<PathSpec>().expect("test path"))
        .collect(),
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    context.set_tcp_endpoint_control(0, ClientTcpEndpointControlState::Disabled);

    let plan = freeze_reliable_relay_return_plan(&context, TrafficClass::Throughput)
        .expect("enabled backup slot remains eligible");

    assert_eq!(plan.candidate_tier(), PathUsage::Backup);
    assert_eq!(plan.candidates().len(), 1);
    assert_eq!(plan.candidates()[0].key.underlay, UnderlayProtocol::Udp);
    assert_eq!(plan.candidates()[0].path_instance_id, None);
}

#[tokio::test(start_paused = true)]
async fn initial_zero_credit_waits_beyond_carrier_pto_for_target_acceptance() {
    let stream_id = StreamId(90);
    let (mut stream, frames, _commands) = unsettled_initial_stream_for_test(stream_id);
    let settlement = tokio::spawn(async move {
        await_reliable_initial_target_acceptance(&mut stream)
            .await
            .map(|()| stream.max_offset)
    });

    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(60)).await;
    assert!(
        !settlement.is_finished(),
        "target establishment must not inherit a second carrier-PTO deadline",
    );

    frames
        .send(Ok(Frame::StreamMaxData {
            stream_id,
            max_offset: 4096,
        }))
        .await
        .expect("publish target-established credit");
    assert_eq!(
        settlement
            .await
            .expect("settlement task")
            .expect("logical target acceptance"),
        4096,
    );
}

#[tokio::test]
async fn initial_target_reset_is_terminal_after_zero_credit_admission() {
    let stream_id = StreamId(91);
    let (mut stream, frames, _commands) = unsettled_initial_stream_for_test(stream_id);
    frames
        .send(Ok(Frame::StreamReset {
            stream_id,
            reason: crate::protocol::ResetReason::Refused,
        }))
        .await
        .expect("publish target failure");

    assert!(matches!(
        await_reliable_initial_target_acceptance(&mut stream).await,
        Err(RuntimeError::RemoteReset(
            crate::protocol::ResetReason::Refused
        ))
    ));
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
fn attachment_refusal_does_not_classify_the_carrier_as_failed() {
    let error = RuntimeError::ReliablePathAttachmentRefused;
    assert!(!stream_open_error_is_path_retryable(&error));
    assert!(!udp_stream_open_error_is_path_retryable(&error));
}

#[test]
fn cold_quic_attachment_budget_covers_serialized_setup_exchanges() {
    let path = "quic://127.0.0.1:11095"
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
    let stream_id = context
        .allocate_reliable_stream_id()
        .expect("allocate logical stream ID");
    let attempt = reserve_reliable_initial_open_attempt(
        &context,
        stream_id,
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
    let path = "quic://127.0.0.1:11097"
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
fn initial_open_retry_reuses_one_logical_stream_id() {
    let context = ClientPathContext::new(
        vec![
            "tcp://127.0.0.1:10132?initial-srtt-s=0.02&initial-rate-mbps=100"
                .parse()
                .expect("first path"),
            "tcp://127.0.0.1:10133?initial-srtt-s=0.08&initial-rate-mbps=200"
                .parse()
                .expect("second path"),
        ],
        security(),
        ResourceLimits::default(),
    )
    .expect("context");
    let mut attempted = Vec::new();
    let stream_id = context
        .allocate_reliable_stream_id()
        .expect("allocate logical stream ID");
    let first = reserve_reliable_initial_open_attempt(
        &context,
        stream_id,
        TrafficClass::Latency,
        PATH_OPEN_SCORE_BYTES,
        &mut attempted,
    )
    .expect("first attempt")
    .expect("first candidate");
    let first_key = first.key;
    let first_stream_id = first.stream_id;
    drop(first);

    let second = reserve_reliable_initial_open_attempt(
        &context,
        stream_id,
        TrafficClass::Latency,
        PATH_OPEN_SCORE_BYTES,
        &mut attempted,
    )
    .expect("second attempt")
    .expect("second candidate");
    assert_eq!(first_stream_id, second.stream_id);
    assert_ne!(first_key, second.key);
}
