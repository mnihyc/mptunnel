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
        Frame::PathDrain { path_id }
        | Frame::PathProofData { path_id, .. }
        | Frame::PathProofAck { path_id, .. } => format!("path_id={}", path_id.0),
        Frame::PathCapacityData {
            path_id,
            measurement_id,
            payload,
        } => format!(
            "path_id={} measurement_id={} payload_len={}",
            path_id.0,
            measurement_id,
            payload.len()
        ),
        Frame::PathCapacityFinish {
            path_id,
            measurement_id,
            payload_bytes,
        } => format!(
            "path_id={} measurement_id={} payload_bytes={}",
            path_id.0, measurement_id, payload_bytes
        ),
        Frame::PathCapacityReceipt {
            path_id,
            measurement_id,
            received_payload_bytes,
        } => format!(
            "path_id={} measurement_id={} received_payload_bytes={}",
            path_id.0, measurement_id, received_payload_bytes
        ),
        Frame::PathStatus {
            path_id,
            sequence,
            usage,
        } => format!(
            "path_id={} sequence={} usage={usage:?}",
            path_id.0, sequence
        ),
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
        Frame::Ping { nonce } | Frame::Pong { nonce } => format!("nonce={nonce}"),
        Frame::PeerStatusRequest { request_id } => format!("request_id={request_id}"),
        Frame::PeerStatusResponse {
            request_id,
            code,
            paths,
        } => format!(
            "request_id={request_id} code={code:?} path_count={}",
            paths.len()
        ),
        Frame::TcpCarrierDemand {
            request_id,
            stream_ids,
        } => format!("request_id={request_id} stream_count={}", stream_ids.len()),
        Frame::TcpCarrierValidate {
            trial_id,
            request_id,
            direction,
            accepted_paths,
            stream_ids,
        } => format!(
            "trial_id={trial_id} request_id={request_id} direction={direction:?} path_count={} stream_count={}",
            accepted_paths.len(),
            stream_ids.len()
        ),
        Frame::TcpCarrierResult {
            trial_id,
            candidate_path_id,
            direction,
            result,
        } => format!(
            "trial_id={trial_id} candidate_path_id={} direction={direction:?} result={result:?}",
            candidate_path_id.0
        ),
    }
}

pub(in crate::runtime) fn log_unexpected_stream_relay_frame(
    kind: &'static str,
    expected: StreamId,
    frame: &Frame,
) {
    crate::observability::process_event!(
        Warn,
        "reliable_relay",
        "unexpected_frame",
        "unexpected {kind} stream relay frame: expected_stream_id={} frame_kind={} {}",
        expected.0,
        frame.kind_name(),
        frame_subject(frame)
    );
}
