use super::*;

#[test]
fn discarded_request_tcp_receipt_requires_the_exact_completed_epoch() {
    let discarded = DiscardedClientTcpCapacityReceipt {
        calibration_id: 17,
        train_payload_bytes: 3 * 1024 * 1024,
    };
    assert!(discarded.matches(17, 3 * 1024 * 1024));
    assert!(!discarded.matches(18, 3 * 1024 * 1024));
    assert!(!discarded.matches(17, 3 * 1024 * 1024 - 1));
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
