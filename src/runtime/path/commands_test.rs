use super::{
    TcpCapacityProbeRequest, reliable_path_command_channels, reliable_path_command_pending_bytes,
    reliable_path_effective_frame_lane, reliable_path_frame_uses_priority_queue,
    reliable_path_stream_ordered_queue_lane, try_recv_reliable_path_command,
};
use crate::model::capacity::RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES;
use crate::protocol::{
    DatagramFlowId, Frame, PathId, ResetReason, SessionId, StreamFlags, StreamId,
};
use crate::runtime::error::RuntimeError;
use crate::runtime::stream::{
    ServerPathLaneTracker, TcpCapacityProbeSessionLease, next_server_carrier_path_instance_id,
};
use crate::scheduler::FlowLane;
use bytes::Bytes;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn tcp_probe_tracker_and_lease() -> (Arc<ServerPathLaneTracker>, TcpCapacityProbeSessionLease) {
    let tracker = Arc::new(ServerPathLaneTracker::default());
    let lease = tracker
        .try_reserve_tcp_capacity_probe(SessionId(1), 0)
        .expect("reserve isolated test session");
    (tracker, lease)
}

fn tcp_probe_session_lease() -> TcpCapacityProbeSessionLease {
    tcp_probe_tracker_and_lease().1
}

fn tcp_probe_request(path_id: PathId) -> TcpCapacityProbeRequest {
    TcpCapacityProbeRequest {
        path_id,
        path_instance_id: next_server_carrier_path_instance_id(),
        train_payload_bytes: 2 * 1024 * 1024,
        sample_floor_bytes: RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES / 2,
        expires_at: Instant::now() + Duration::from_secs(1),
    }
}

#[test]
fn tcp_capacity_attempt_is_spent_only_after_queue_admission() {
    let (commands, mut receivers) = reliable_path_command_channels(1);
    commands
        .try_enqueue_admitted_frame(
            Frame::StreamData {
                stream_id: StreamId(1),
                offset: 0,
                flags: StreamFlags::NONE,
                payload: Bytes::from_static(b"queued"),
            },
            reliable_path_stream_ordered_queue_lane(),
        )
        .expect("fill data queue");
    assert!(matches!(
        commands.try_enqueue_tcp_capacity_probe(
            tcp_probe_request(PathId(2)),
            tcp_probe_session_lease(),
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
    assert!(!commands.tcp_capacity_probe_attempted());

    let queued = try_recv_reliable_path_command(&mut receivers).expect("queued ping");
    receivers.release_pending_command_bytes(reliable_path_command_pending_bytes(&queued));
    drop(queued);
    let (tracker, session_lease) = tcp_probe_tracker_and_lease();
    commands
        .try_enqueue_tcp_capacity_probe(tcp_probe_request(PathId(2)), session_lease)
        .expect("admit exact carrier probe");
    assert!(commands.tcp_capacity_probe_attempted());
    assert!(commands.tcp_capacity_probe_active());
    drop(try_recv_reliable_path_command(&mut receivers).expect("queued probe"));
    assert!(!commands.tcp_capacity_probe_active());
    assert!(
        tracker
            .try_reserve_tcp_capacity_probe(SessionId(1), 0)
            .is_some(),
        "command cancellation releases session ownership before carrier wake"
    );
    assert!(matches!(
        commands.try_enqueue_tcp_capacity_probe(
            tcp_probe_request(PathId(2)),
            tcp_probe_session_lease(),
        ),
        Err(RuntimeError::SenderServiceBlocked)
    ));
}

#[test]
fn control_and_ack_frames_never_use_throughput_lane() {
    let priority_frames = [
        (
            Frame::StreamAck {
                stream_id: StreamId(1),
                complete: false,
                ranges: vec![],
            },
            FlowLane::Control,
        ),
        (
            Frame::StreamMaxData {
                stream_id: StreamId(1),
                max_offset: 1024,
            },
            FlowLane::Control,
        ),
        (
            Frame::StreamFin {
                stream_id: StreamId(1),
                final_offset: 64,
            },
            FlowLane::Control,
        ),
        (
            Frame::StreamReset {
                stream_id: StreamId(1),
                reason: ResetReason::RemoteClosed,
            },
            FlowLane::Control,
        ),
        (
            Frame::StreamDetach {
                stream_id: StreamId(1),
            },
            FlowLane::Control,
        ),
        (
            Frame::DatagramFeedback {
                flow_id: DatagramFlowId(1),
                received: vec![],
            },
            FlowLane::RealtimeDatagram,
        ),
        (
            Frame::DatagramClose {
                flow_id: DatagramFlowId(1),
            },
            FlowLane::Control,
        ),
    ];

    for (frame, expected_lane) in priority_frames {
        let effective_lane = reliable_path_effective_frame_lane(&frame, FlowLane::Throughput);
        assert_eq!(effective_lane, expected_lane);
        assert!(reliable_path_frame_uses_priority_queue(effective_lane));
    }
}
