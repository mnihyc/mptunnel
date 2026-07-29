use super::*;
use crate::config::{ClientSecurityConfig, ResourceLimits, SharedSecret};
use crate::protocol::{PathId, TargetAddr};
use crate::runtime::path::commands::{ReliablePathCommandSender, reliable_path_command_channels};
use crate::runtime::stream::{ReliablePathStream, ReliablePathStreamOutput};
use crate::transport::PathSpec;
use std::time::Duration;

fn test_stream(
    stream_id: StreamId,
    underlay: UnderlayProtocol,
    path_index: usize,
    commands: ReliablePathCommandSender,
    lane: TrafficClass,
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
        frames: frames_rx.into(),
    }
}

#[test]
fn live_demand_class_change_updates_lane_accounting() {
    assert!(reliable_relay_lane_changed(
        TrafficClass::Latency,
        TrafficClass::Throughput,
    ));
    assert!(reliable_relay_lane_changed(
        TrafficClass::Throughput,
        TrafficClass::Latency,
    ));
    assert!(!reliable_relay_lane_changed(
        TrafficClass::Throughput,
        TrafficClass::Throughput,
    ));
}

#[test]
fn pending_fin_waits_for_the_ordered_sender_queue() {
    assert!(reliable_relay_can_send_pending_fin(true, true));
    assert!(!reliable_relay_can_send_pending_fin(true, false));
    assert!(!reliable_relay_can_send_pending_fin(false, true));
}

#[test]
fn bulk_path_opens_do_not_serialize_tcp_and_quic() {
    let candidates = vec![
        RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 1,
        },
        RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        },
        RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 1,
        },
    ];

    assert_eq!(
        reliable_relay_available_path_open_candidates(candidates.clone(), &HashMap::new()),
        candidates,
    );
}

#[test]
fn scheduled_sender_retry_blocks_immediate_redispatch() {
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
fn expected_response_arms_stall_watch_independent_of_demand_class() {
    let send_stream = ReliableSendStream::new(StreamId(1), MuxLimits::default());
    let recv_stream = ReliableRecvStream::new(StreamId(1), MuxLimits::default());

    for lane in [TrafficClass::Latency, TrafficClass::Throughput] {
        assert!(reliable_relay_stall_watch_active(
            &send_stream,
            &recv_stream,
            true,
            lane,
            true,
            MuxLimits::default(),
        ));
    }
}

#[test]
fn response_stall_anchor_ignores_unrelated_request_progress() {
    let recv_stream = ReliableRecvStream::new(StreamId(1), MuxLimits::default());
    let started = Instant::now();
    let last_delivery = started;
    let last_reinjection = started + Duration::from_millis(100);
    let later_request_progress = started + Duration::from_secs(5);

    assert_eq!(
        reliable_relay_stall_progress_anchor(
            later_request_progress,
            last_delivery,
            last_reinjection,
            &recv_stream,
            true,
            TrafficClass::Latency,
            true,
            MuxLimits::default(),
        ),
        last_reinjection,
    );
}

#[test]
fn repeated_stall_attempts_use_a_future_bounded_deadline() {
    let started = Instant::now();
    let pto = transport_pto_from_snapshot(None);
    assert_eq!(
        reliable_relay_product_stall_deadline(started, None, None),
        tokio::time::Instant::from_std(started + pto),
    );

    let last_attempt = started + pto;
    assert_eq!(
        reliable_relay_product_stall_deadline(started, Some(last_attempt), None),
        tokio::time::Instant::from_std(
            last_attempt + pto.saturating_mul(QUIC_PERSISTENT_CONGESTION_THRESHOLD),
        ),
    );

    let later_progress = last_attempt + Duration::from_secs(1);
    assert_eq!(
        reliable_relay_product_stall_deadline(later_progress, Some(last_attempt), None),
        tokio::time::Instant::from_std(later_progress + pto),
    );
}

#[tokio::test]
async fn product_stall_preserves_an_existing_multipath_attachment_set() {
    let (tcp_commands, _tcp_receivers) = reliable_path_command_channels(1);
    let (udp_commands, _udp_receivers) = reliable_path_command_channels(1);
    let first = OpenedRemoteStream::pending(
        test_stream(
            StreamId(1),
            UnderlayProtocol::Tcp,
            0,
            tcp_commands,
            TrafficClass::Latency,
        ),
        0,
    );
    let second = OpenedRemoteStream::pending(
        test_stream(
            StreamId(1),
            UnderlayProtocol::Udp,
            0,
            udp_commands,
            TrafficClass::Latency,
        ),
        0,
    );
    let mut remotes = ReliableRelayRemoteSet::new(first, 4);
    assert_eq!(
        remotes.attach_candidate(second),
        ReliableRelayAttachOutcome::Attached
    );
    let membership = remotes.path_instances();

    assert!(reliable_relay_product_stall_preserves_attached_path_set(
        &remotes
    ));
    assert!(!reliable_relay_product_stall_should_try_alternate_attach(
        &remotes
    ));
    assert!(!reliable_relay_should_open_recovery_path(&remotes));
    assert_eq!(remotes.path_instances(), membership);
}

#[tokio::test]
async fn product_stall_on_a_sole_carrier_requests_an_alternative() {
    let (commands, _receivers) = reliable_path_command_channels(1);
    let opened = OpenedRemoteStream::pending(
        test_stream(
            StreamId(1),
            UnderlayProtocol::Tcp,
            0,
            commands,
            TrafficClass::Latency,
        ),
        0,
    );
    let remotes = ReliableRelayRemoteSet::new(opened, 4);

    assert!(reliable_relay_product_stall_should_try_alternate_attach(
        &remotes
    ));
    assert!(reliable_relay_should_open_recovery_path(&remotes));
    assert!(!reliable_relay_product_stall_preserves_attached_path_set(
        &remotes
    ));
}

#[tokio::test]
async fn product_stall_recovery_open_is_owned_by_a_pending_task() {
    let security = ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    );
    let context = ClientPathContext::new(
        [
            "tcp://127.0.0.1:11171",
            "udp://127.0.0.1:11172?srtt-ms=180&rate-mbps=500",
        ]
        .into_iter()
        .map(|path| path.parse::<PathSpec>().expect("test path"))
        .collect(),
        security,
        ResourceLimits::default(),
    )
    .expect("client context");
    let (commands, _receivers) = reliable_path_command_channels(1);
    let opened = OpenedRemoteStream::pending(
        test_stream(
            StreamId(2),
            UnderlayProtocol::Tcp,
            0,
            commands,
            TrafficClass::Latency,
        ),
        0,
    );
    let remotes = ReliableRelayRemoteSet::new(opened, 4);
    let send_stream = ReliableSendStream::new(StreamId(2), context.mux_limits);
    let spec = ReliableRelayOpenSpec {
        target: TargetAddr::Ip("127.0.0.1:9".parse().expect("test target")),
    };
    let mut pending = HashMap::new();
    let (result_tx, _result_rx) = mpsc::channel(1);

    assert!(spawn_reliable_relay_recovery_path_open(
        &context,
        &spec,
        TrafficClass::Latency,
        &remotes,
        &send_stream,
        &HashSet::new(),
        &mut pending,
        &result_tx,
    ));
    assert_eq!(pending.len(), 1);
    assert!(pending.contains_key(&RelayPathKey {
        underlay: UnderlayProtocol::Udp,
        index: 0,
    }));
    cancel_pending_additional_path_opens(StreamId(2), &mut pending);
}

#[test]
fn asynchronous_recovery_open_selects_one_non_excluded_path() {
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
    let pending = HashMap::new();

    assert_eq!(
        reliable_relay_recovery_path_open_candidates(
            vec![tcp0, tcp1, udp0],
            &HashSet::new(),
            &pending,
        ),
        vec![tcp0],
    );
    assert_eq!(
        reliable_relay_recovery_path_open_candidates(
            vec![tcp0, tcp1, udp0],
            &HashSet::from([tcp0]),
            &pending,
        ),
        vec![tcp1],
    );
}

#[tokio::test]
async fn stale_additional_path_result_cannot_consume_replacement_open() {
    let key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 0,
    };
    let stale_generation = next_relay_additional_path_open_generation();
    let current_generation = next_relay_additional_path_open_generation();
    let mut pending = HashMap::from([(
        key,
        RelayAdditionalPathOpenTask {
            generation: current_generation,
            #[cfg(feature = "lab-diagnostics")]
            lane: TrafficClass::Throughput,
            handle: tokio::spawn(std::future::pending()),
        },
    )]);

    assert!(
        take_matching_additional_path_open(&mut pending, key, stale_generation).is_none(),
        "a delayed result must not consume a newer open for the same path"
    );
    assert_eq!(pending.len(), 1);

    take_matching_additional_path_open(&mut pending, key, current_generation)
        .expect("exact open generation")
        .handle
        .abort();
    assert!(pending.is_empty());
}

#[test]
fn receive_hole_reinjection_requires_live_remote_and_buffered_gap() {
    let mut recv_stream = ReliableRecvStream::new(StreamId(4), MuxLimits::default());
    recv_stream
        .receive_data(0, bytes::Bytes::from_static(b"prefix"))
        .expect("contiguous prefix");
    recv_stream
        .receive_data(1024, bytes::Bytes::from_static(b"gap"))
        .expect("out-of-order data");

    assert!(reliable_relay_receive_hole_reinjection_active(
        &recv_stream,
        true
    ));
    assert!(!reliable_relay_receive_hole_reinjection_active(
        &recv_stream,
        false
    ));
}
