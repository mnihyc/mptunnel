use super::{ReliablePathStreamHandle, ReliablePathStreamOutput};
use crate::model::capacity::{
    MIN_RATE_SAMPLE_BYTES, PATH_OPEN_SCORE_BYTES, RELIABLE_INITIAL_WINDOW_PACKETS,
};
use crate::mux::MuxLimits;
use crate::protocol::frame::reliable_stream_frame_accounted_bytes;
use crate::protocol::{Frame, OffsetRange, PathId, ResetReason, StreamId, UnderlayProtocol};
use crate::runtime::path::commands::{
    ReliablePathCommand, reliable_path_command_channels, try_recv_reliable_path_command,
    try_recv_reliable_path_priority_command,
};
use crate::runtime::stream::reliable_stream_recv_progress_interval;
use crate::scheduler::{PathRateScope, PathSnapshot, TrafficClass};
use bytes::Bytes;
use std::time::Duration;

fn stream_data_frame(payload_len: usize) -> Frame {
    stream_data_frame_at(0, payload_len)
}

fn stream_data_frame_at(offset: u64, payload_len: usize) -> Frame {
    Frame::StreamData {
        stream_id: StreamId(7),
        offset,
        payload: Bytes::from(vec![0x5a; payload_len]),
    }
}

#[test]
fn fixed_priority_path_proof_preserves_attachment_liveness_ordering() {
    let mux_limits = MuxLimits::default();
    let path_id = PathId(4);
    let (commands, mut receivers) = reliable_path_command_channels(4);
    commands
        .try_enqueue_admitted_frame(stream_data_frame(32), TrafficClass::Throughput)
        .expect("queue earlier stream data");
    let stream = ReliablePathStreamHandle {
        stream_id: StreamId(7),
        max_offset: u64::MAX,
        lane: TrafficClass::Throughput,
        underlay: UnderlayProtocol::Tcp,
        max_frame_payload_bytes: mux_limits.max_payload_bytes,
        output: ReliablePathStreamOutput::fixed(
            UnderlayProtocol::Tcp,
            path_id,
            commands,
            mux_limits,
        ),
    };

    let proof_id = stream
        .enqueue_path_proof()
        .expect("queue priority path proof")
        .expect("fixed output has a carrier path");

    match try_recv_reliable_path_priority_command(&mut receivers) {
        Some(ReliablePathCommand::SendFrame(Frame::PathProofData {
            path_id: queued_path_id,
            proof_id: queued_proof_id,
            ..
        })) => {
            assert_eq!(queued_path_id, path_id);
            assert_eq!(queued_proof_id, proof_id);
        }
        _ => panic!("attachment-liveness proof must retain priority ordering"),
    }
    assert!(matches!(
        try_recv_reliable_path_command(&mut receivers),
        Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
    ));
}

#[tokio::test]
async fn fixed_output_publishes_terminal_reset_as_one_ordered_transaction() {
    let mux_limits = MuxLimits::default();
    let stream_id = StreamId(8);
    let (commands, mut receivers) = reliable_path_command_channels(1);
    let output =
        ReliablePathStreamOutput::fixed(UnderlayProtocol::Tcp, PathId(4), commands, mux_limits);

    output
        .reset_and_close_stream_ordered(
            stream_id,
            ResetReason::RemoteClosed,
            TrafficClass::Throughput,
        )
        .await;

    assert!(matches!(
        try_recv_reliable_path_command(&mut receivers),
        Some(ReliablePathCommand::ResetAndCloseStream {
            stream_id: received,
            reason: ResetReason::RemoteClosed,
        }) if received == stream_id
    ));
    assert!(try_recv_reliable_path_command(&mut receivers).is_none());
}

#[test]
fn tcp_fixed_output_startup_prior_yields_after_persistent_local_delivery_samples() {
    let mux_limits = MuxLimits::default();
    let (commands, _receivers) = reliable_path_command_channels(64);
    let startup_rate = 500_000_000.0;
    let startup = PathSnapshot::new(PathId(8), UnderlayProtocol::Tcp, 20.0, startup_rate);
    let output = ReliablePathStreamOutput::fixed_with_snapshot(startup, commands, mux_limits);
    let ReliablePathStreamOutput::Fixed(fixed) = &output else {
        panic!("expected fixed output");
    };
    assert_eq!(
        fixed.send_path_snapshot().rate_scope,
        PathRateScope::PathCapacity
    );
    let mut offset = 0_u64;

    for _ in 0..RELIABLE_INITIAL_WINDOW_PACKETS {
        let frame = stream_data_frame_at(offset, MIN_RATE_SAMPLE_BYTES as usize);
        let end = offset + reliable_stream_frame_accounted_bytes(&frame) as u64;
        fixed.record_original_flight(&frame);
        std::thread::sleep(Duration::from_millis(20));
        fixed.release_normalized_acked_ranges(&[OffsetRange { start: offset, end }]);
        offset = end;
    }

    let learned_rate = fixed
        .model
        .lock()
        .expect("fixed output model lock")
        .delivery_rate_bps
        .expect("persistent samples produce a delivery model");
    assert!(learned_rate < startup_rate * 0.5);

    let snapshot = output
        .send_path_snapshot(TrafficClass::Throughput, MIN_RATE_SAMPLE_BYTES as usize)
        .expect("response binding exposes learned path model");
    assert!(
        snapshot.delivery_rate_bps < startup_rate * 0.5,
        "startup/default rate is only a hint; persistent local delivery samples must correct it downward"
    );
    assert_eq!(snapshot.rate_scope, PathRateScope::PerFlowGoodput);
}

#[test]
fn fixed_output_request_feedback_snapshot_preserves_send_path_timing() {
    let mux_limits = MuxLimits::default();
    let (commands, _receivers) = reliable_path_command_channels(8);
    let startup = PathSnapshot::new(PathId(9), UnderlayProtocol::Tcp, 123.0, 8_000_000.0);
    let output = ReliablePathStreamOutput::fixed_with_snapshot(startup, commands, mux_limits);
    let send_snapshot = output
        .send_path_snapshot(TrafficClass::Latency, PATH_OPEN_SCORE_BYTES)
        .expect("fixed output has a send path snapshot");
    let request_feedback_snapshot = output
        .request_feedback_path_snapshot(TrafficClass::Latency)
        .expect("fixed output has a request-feedback path snapshot");

    assert_eq!(request_feedback_snapshot.id, send_snapshot.id);
    assert_eq!(request_feedback_snapshot.underlay, send_snapshot.underlay);
    assert_eq!(request_feedback_snapshot.srtt_ms, send_snapshot.srtt_ms);
    assert_eq!(
        reliable_stream_recv_progress_interval(Some(request_feedback_snapshot)),
        reliable_stream_recv_progress_interval(Some(send_snapshot)),
        "fixed-path replay cadence must remain unchanged"
    );
}
