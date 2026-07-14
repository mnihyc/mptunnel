//! Compact diagnostics for rejected product-stream relay frames.

use crate::protocol::{Frame, StreamId};

fn frame_subject(frame: &Frame) -> String {
    match frame {
        Frame::SessionHello { session_id } => format!("session_id={}", session_id.0),
        Frame::SessionAuth { session_id, .. } => format!("session_id={}", session_id.0),
        Frame::SessionReady => "none".to_string(),
        Frame::SessionClose { reason } => format!("reason={reason:?}"),
        Frame::PathJoin {
            session_id,
            path_id,
            underlay,
            ..
        } => format!(
            "session_id={} path_id={} underlay={underlay:?}",
            session_id.0, path_id.0
        ),
        Frame::PathJoinOk { path_id, .. }
        | Frame::PathChallenge { path_id, .. }
        | Frame::PathResponse { path_id, .. }
        | Frame::PathDrain { path_id }
        | Frame::PathMtuProbe { path_id, .. }
        | Frame::PathMtuAck { path_id, .. }
        | Frame::PathProofData { path_id, .. }
        | Frame::PathProofAck { path_id, .. }
        | Frame::RxRateHint { path_id, .. } => format!("path_id={}", path_id.0),
        Frame::PathCapacityData {
            path_id,
            calibration_id,
            payload,
        } => format!(
            "path_id={} calibration_id={} payload_len={}",
            path_id.0,
            calibration_id,
            payload.len()
        ),
        Frame::PathCapacityFinish {
            path_id,
            calibration_id,
            payload_bytes,
        } => format!(
            "path_id={} calibration_id={} payload_bytes={}",
            path_id.0, calibration_id, payload_bytes
        ),
        Frame::PathCapacityReceipt {
            path_id,
            calibration_id,
            received_payload_bytes,
        } => format!(
            "path_id={} calibration_id={} received_payload_bytes={}",
            path_id.0, calibration_id, received_payload_bytes
        ),
        Frame::PathStatus {
            path_id, status, ..
        } => format!("path_id={} status={status:?}", path_id.0),
        Frame::PathClose { path_id, reason } => {
            format!("path_id={} reason={reason:?}", path_id.0)
        }
        Frame::OpenStream { stream_id, .. } => format!("stream_id={}", stream_id.0),
        Frame::StreamData {
            stream_id,
            offset,
            payload,
            ..
        } => format!(
            "stream_id={} offset={} payload_len={}",
            stream_id.0,
            offset,
            payload.len()
        ),
        Frame::StreamAck {
            stream_id,
            complete,
            ranges,
        } => format!(
            "stream_id={} complete={} ranges={}",
            stream_id.0,
            complete,
            ranges.len()
        ),
        Frame::StreamMaxData {
            stream_id,
            max_offset,
        } => format!("stream_id={} max_offset={max_offset}", stream_id.0),
        Frame::StreamFin { stream_id, .. } | Frame::StreamDetach { stream_id } => {
            format!("stream_id={}", stream_id.0)
        }
        Frame::StreamReset { stream_id, reason } => {
            format!("stream_id={} reason={reason:?}", stream_id.0)
        }
        Frame::OpenDatagramFlow { flow_id, .. } => format!("flow_id={}", flow_id.0),
        Frame::DatagramData {
            flow_id,
            datagram_id,
            ttl_ms,
            payload,
        } => format!(
            "flow_id={} datagram_id={} ttl_ms={} payload_len={}",
            flow_id.0,
            datagram_id.0,
            ttl_ms,
            payload.len()
        ),
        Frame::DatagramClose { flow_id } => format!("flow_id={}", flow_id.0),
        Frame::DatagramFeedback { flow_id, received } => {
            format!("flow_id={} ranges={}", flow_id.0, received.len())
        }
        Frame::PathMetrics { metrics } => format!("path_id={}", metrics.path_id.0),
        Frame::MaxConnectionData { max_bytes } => format!("max_bytes={max_bytes}"),
        Frame::Ping { nonce } | Frame::Pong { nonce } => format!("nonce={nonce}"),
    }
}

pub(in crate::runtime) fn log_unexpected_stream_relay_frame(
    kind: &'static str,
    expected: StreamId,
    frame: &Frame,
) {
    eprintln!(
        "warning: unexpected {kind} stream relay frame: expected_stream_id={} frame_kind={} {}",
        expected.0,
        frame.kind_name(),
        frame_subject(frame)
    );
}
