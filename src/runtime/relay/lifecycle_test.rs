use super::*;
use crate::config::{ResourceLimits, SecurityConfig, SharedSecret};
use crate::protocol::{PathId, TargetAddr};
use crate::runtime::path::commands::{ReliablePathCommandSender, reliable_path_command_channels};
use crate::runtime::stream::{ReliablePathStream, ReliablePathStreamOutput};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

fn test_security() -> SecurityConfig {
    SecurityConfig::encrypted(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("test secret"),
    )
}

#[test]
fn relay_lane_accounting_updates_on_promotion_and_demotion() {
    assert!(reliable_relay_lane_changed(
        FlowLane::Latency,
        FlowLane::Throughput,
    ));
    assert!(reliable_relay_lane_changed(
        FlowLane::Throughput,
        FlowLane::Latency,
    ));
    assert!(!reliable_relay_lane_changed(
        FlowLane::Throughput,
        FlowLane::Throughput,
    ));
}

fn test_reliable_path_stream(
    stream_id: StreamId,
    underlay: UnderlayProtocol,
    path_index: usize,
    commands: ReliablePathCommandSender,
    lane: FlowLane,
) -> ReliablePathStream {
    let (_frames_tx, frames_rx) = mpsc::channel(1);
    ReliablePathStream {
        stream_id,
        max_offset: MuxLimits::default().max_stream_window_bytes,
        lane,
        underlay,
        max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
        output: ReliablePathStreamOutput::fixed(
            underlay,
            PathId(path_index as u16),
            commands,
            MuxLimits::default(),
        ),
        frames: frames_rx,
    }
}

#[test]
fn pending_fin_policy_is_ordered_queue_state_not_carrier_family() {
    assert!(reliable_relay_can_send_pending_fin(true, true));
    assert!(!reliable_relay_can_send_pending_fin(true, false));
    assert!(!reliable_relay_can_send_pending_fin(false, true));
}

#[test]
fn queued_sender_retry_blocks_even_when_carrier_has_capacity() {
    assert!(reliable_relay_queued_send_blocked_for_retry(
        false,
        Some(tokio::time::Instant::now()),
    ));
    assert!(!reliable_relay_queued_send_blocked_for_retry(
        true,
        Some(tokio::time::Instant::now()),
    ));
    assert!(!reliable_relay_queued_send_blocked_for_retry(false, None));
}

#[test]
fn pending_response_stall_watch_survives_lane_promotion() {
    let send_stream = ReliableSendStream::new(StreamId(1), MuxLimits::default());
    let recv_stream = ReliableRecvStream::new(StreamId(1), MuxLimits::default());

    assert!(reliable_relay_stall_watch_active(
        &send_stream,
        &recv_stream,
        true,
        FlowLane::Throughput,
        true,
        MuxLimits::default(),
    ));
}

#[test]
fn pending_response_stall_anchor_ignores_local_send_progress_before_first_byte() {
    let recv_stream = ReliableRecvStream::new(StreamId(1), MuxLimits::default());
    let started = Instant::now();
    let last_delivery_progress_at = started;
    let last_response_stall_repair_at = started + Duration::from_millis(100);
    let last_local_send_progress_at = started + Duration::from_secs(5);

    let anchor = reliable_relay_stall_progress_anchor(
        last_local_send_progress_at,
        last_delivery_progress_at,
        last_response_stall_repair_at,
        &recv_stream,
        true,
        FlowLane::Latency,
        true,
        MuxLimits::default(),
    );

    assert_eq!(
        anchor, last_response_stall_repair_at,
        "once a response is expected, later request-side send/control progress must not postpone response-stall recovery"
    );
}

#[test]
fn upload_only_stall_attempt_uses_a_future_retry_deadline() {
    let started = Instant::now();
    let stall_timeout = transport_pto_from_snapshot(None);
    assert_eq!(
        reliable_relay_product_stall_deadline(started, None, None),
        tokio::time::Instant::from_std(started + stall_timeout),
    );

    let last_attempt = started + stall_timeout;
    assert_eq!(
        reliable_relay_product_stall_deadline(started, Some(last_attempt), None,),
        tokio::time::Instant::from_std(
            last_attempt + stall_timeout.saturating_mul(QUIC_PERSISTENT_CONGESTION_THRESHOLD),
        ),
        "a request-side attempt must move the timer forward instead of leaving an expired sleep branch continuously ready",
    );

    let later_product_progress = last_attempt + Duration::from_secs(1);
    assert_eq!(
        reliable_relay_product_stall_deadline(later_product_progress, Some(last_attempt), None,),
        tokio::time::Instant::from_std(later_product_progress + stall_timeout),
        "real product progress starts a fresh first-attempt interval",
    );
}

#[tokio::test]
async fn latency_product_stall_keeps_active_and_cross_underlay_repair_membership() {
    let (commands_a, _receivers_a) = reliable_path_command_channels(1);
    let (commands_b, _receivers_b) = reliable_path_command_channels(1);
    let first = OpenedRemoteStream::pending(
        test_reliable_path_stream(
            StreamId(1),
            UnderlayProtocol::Tcp,
            0,
            commands_a,
            FlowLane::Latency,
        ),
        0,
    );
    let second = OpenedRemoteStream::pending(
        test_reliable_path_stream(
            StreamId(1),
            UnderlayProtocol::Udp,
            0,
            commands_b,
            FlowLane::Latency,
        ),
        0,
    );
    let mut remotes = ReliableRelayRemoteSet::new(first, 4);
    let active = remotes.active_path_instance();
    let original_keys = remotes.path_keys();
    remotes.attach_for_repair(second);

    assert!(reliable_relay_product_stall_preserves_attached_path_set(
        &remotes
    ));
    assert!(!reliable_relay_product_stall_should_try_alternate_attach(
        &remotes
    ));
    assert_eq!(remotes.active_path_instance(), active);
    assert_eq!(remotes.path_keys().last(), original_keys.last());
}

#[tokio::test]
async fn product_stall_on_sole_carrier_attempts_alternate_attach() {
    let (commands, _receivers) = reliable_path_command_channels(1);
    let active = OpenedRemoteStream::pending(
        test_reliable_path_stream(
            StreamId(1),
            UnderlayProtocol::Tcp,
            0,
            commands,
            FlowLane::Latency,
        ),
        0,
    );
    let remotes = ReliableRelayRemoteSet::new(active, 4);

    assert!(reliable_relay_product_stall_should_try_alternate_attach(
        &remotes
    ));
}

#[test]
fn validation_probe_candidates_group_one_family_with_bounded_retry() {
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
    let candidates = vec![tcp0, tcp1, udp0, tcp0];
    let pending = HashMap::<RelayPathKey, RelayValidationOpenTask>::new();
    let mut attempts = HashMap::from([(tcp0, 1)]);
    let selected = reliable_relay_validation_probe_candidates(candidates, &pending, &attempts);

    assert_eq!(
        selected,
        vec![tcp0, tcp1],
        "current-family Validation opens should start together and one failed open may retry"
    );

    attempts.insert(tcp0, 2);
    attempts.insert(tcp1, 2);
    assert_eq!(
        reliable_relay_validation_probe_candidates(vec![tcp0, tcp1, udp0], &pending, &attempts,),
        vec![udp0],
        "a later invocation may move to the other carrier family"
    );
    attempts.insert(udp0, 2);
    assert!(
        reliable_relay_validation_probe_candidates(vec![tcp0, tcp1, udp0], &pending, &attempts)
            .is_empty(),
        "two attempts bound reconnect churn for every stream/path"
    );
}

#[tokio::test]
async fn validation_open_candidates_prefer_active_family_survivor_before_cross_family_probe() {
    let context = ClientPathContext::new(
        vec![
            "tcp://127.0.0.1:23101?srtt-ms=80&rate-mbps=80"
                .parse()
                .expect("active tcp path"),
            "tcp://127.0.0.1:23102?srtt-ms=220&rate-mbps=80"
                .parse()
                .expect("same-family tcp survivor"),
            "udp://127.0.0.1:23103?srtt-ms=30&rate-mbps=400"
                .parse()
                .expect("lower-eta cross-family udp probe"),
        ],
        test_security(),
        ResourceLimits::default(),
    )
    .expect("context");
    let (commands, _receivers) = reliable_path_command_channels(1);
    let active = OpenedRemoteStream::pending(
        test_reliable_path_stream(
            StreamId(11),
            UnderlayProtocol::Tcp,
            0,
            commands,
            FlowLane::Throughput,
        ),
        0,
    );
    let remotes = ReliableRelayRemoteSet::new(active, 4);

    let candidates = reliable_relay_validation_open_candidates(
        &context,
        &remotes,
        reliable_relay_buffer_len(MuxLimits::default()),
    );

    assert_eq!(
        candidates.first().copied(),
        Some(RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 1,
        }),
        "validation should first make a same-family survivor available for Service failover before spending the one-shot open on cross-family probes"
    );
    assert!(
        candidates.contains(&RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        }),
        "cross-family validation remains eligible as fallback/probe, but not before the Service-family survivor"
    );
}

#[tokio::test]
async fn validation_open_candidates_offer_distinct_idle_paths_before_occupied_services() {
    let context = ClientPathContext::new(
        (0..5)
            .map(|index| {
                format!(
                    "tcp://127.0.0.1:{}?srtt-ms=180&rate-mbps=500",
                    23201 + index
                )
                .parse()
                .expect("equal TCP path")
            })
            .collect(),
        test_security(),
        ResourceLimits::default(),
    )
    .expect("context");
    let first_service_load = context
        .reserve_relay_path_load(
            RelayPathKey {
                underlay: UnderlayProtocol::Tcp,
                index: 0,
            },
            FlowLane::Throughput,
        )
        .expect("first Service load");
    let second_service_load = context
        .reserve_relay_path_load(
            RelayPathKey {
                underlay: UnderlayProtocol::Tcp,
                index: 1,
            },
            FlowLane::Throughput,
        )
        .expect("second Service load");

    let (first_commands, _first_receivers) = reliable_path_command_channels(1);
    let first = ReliableRelayRemoteSet::new(
        OpenedRemoteStream::pending(
            test_reliable_path_stream(
                StreamId(12),
                UnderlayProtocol::Tcp,
                0,
                first_commands,
                FlowLane::Throughput,
            ),
            0,
        )
        .with_load_lease(first_service_load),
        5,
    );
    let first_candidates = reliable_relay_validation_open_candidates(
        &context,
        &first,
        reliable_relay_buffer_len(MuxLimits::default()),
    );
    assert_eq!(first_candidates[0].index, 2);

    context.reserve_tcp_path_load(2, FlowLane::Throughput);
    let (second_commands, _second_receivers) = reliable_path_command_channels(1);
    let second = ReliableRelayRemoteSet::new(
        OpenedRemoteStream::pending(
            test_reliable_path_stream(
                StreamId(13),
                UnderlayProtocol::Tcp,
                1,
                second_commands,
                FlowLane::Throughput,
            ),
            1,
        )
        .with_load_lease(second_service_load),
        5,
    );
    let second_candidates = reliable_relay_validation_open_candidates(
        &context,
        &second,
        reliable_relay_buffer_len(MuxLimits::default()),
    );
    assert_eq!(
        second_candidates[0].index, 3,
        "the second flow must not collide with either Service or the first claimed candidate"
    );
}

#[tokio::test]
async fn latency_lane_does_not_spawn_standby_validation_probe() {
    let context = ClientPathContext::new(
        vec![
            "udp://127.0.0.1:23001?srtt-ms=30&rate-mbps=100"
                .parse()
                .expect("active path"),
            "udp://127.0.0.1:23002?srtt-ms=35&rate-mbps=100"
                .parse()
                .expect("standby path"),
        ],
        test_security(),
        ResourceLimits::default(),
    )
    .expect("context");
    let (commands, _receivers) = reliable_path_command_channels(1);
    let active = OpenedRemoteStream::pending(
        test_reliable_path_stream(
            StreamId(9),
            UnderlayProtocol::Udp,
            0,
            commands,
            FlowLane::Latency,
        ),
        0,
    );
    let remotes = ReliableRelayRemoteSet::new(active, 4);
    let send_stream = ReliableSendStream::new(StreamId(9), MuxLimits::default());
    let spec = ReliableRelayOpenSpec {
        target: TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80))),
    };
    let (result_tx, _result_rx) = mpsc::channel(1);
    let mut pending = HashMap::<RelayPathKey, RelayValidationOpenTask>::new();
    let mut attempts = HashMap::<RelayPathKey, u8>::new();

    assert!(!spawn_reliable_relay_validation_opens(
        &context,
        &spec,
        FlowLane::Latency,
        &remotes,
        &send_stream,
        &mut pending,
        &mut attempts,
        &result_tx,
    ));
    assert!(
        pending.is_empty() && attempts.is_empty(),
        "validation/probe opens are bulk-only; latency response stalls recover through path health and repair, not proactive per-stream standbys"
    );
}
