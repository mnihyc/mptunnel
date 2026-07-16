//! Optional observation of committed response scheduling decisions.
//!
//! Instrumentation receives the selected path after commit and cannot alter
//! queue admission, Data ACK accounting, or carrier behavior.

#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_sender_service_decision;
use crate::model::path::CarrierPathKey;
#[cfg(feature = "lab-diagnostics")]
use crate::protocol::frame::{
    reliable_path_frame_pacing_bytes, reliable_stream_frame_accounted_bytes,
};
use crate::protocol::{Frame, SessionId, StreamId};
use crate::scheduler::TrafficClass;

pub(in crate::runtime) fn record_server_sender_decision(
    session_id: SessionId,
    stream_id: StreamId,
    key: CarrierPathKey,
    frame: &Frame,
    lane: TrafficClass,
    reason: &'static str,
    bulk_rate_evidence: Option<bool>,
) {
    #[cfg(feature = "lab-diagnostics")]
    lab_sender_service_decision(
        "server",
        Some(session_id.0),
        stream_id.0,
        reason,
        sender_frame_kind(frame),
        reliable_stream_frame_accounted_bytes(frame),
        bulk_rate_evidence,
        format_args!(
            "path_underlay={:?} path_id={} lane={:?} pacing_bytes={}",
            key.underlay,
            key.path_id.0,
            lane,
            reliable_path_frame_pacing_bytes(frame),
        ),
    );
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = (
        session_id,
        stream_id,
        key,
        frame,
        lane,
        reason,
        bulk_rate_evidence,
    );
}

#[cfg(feature = "lab-diagnostics")]
fn sender_frame_kind(frame: &Frame) -> &'static str {
    match frame {
        Frame::StreamData { .. } => "stream_data",
        Frame::StreamAck { .. } => "stream_ack",
        Frame::StreamMaxData { .. } => "stream_max_data",
        Frame::StreamFin { .. } => "stream_fin",
        Frame::StreamReset { .. } => "stream_reset",
        Frame::StreamDetach { .. } => "stream_detach",
        Frame::DatagramData { .. } => "datagram_data",
        Frame::DatagramFeedback { .. } => "datagram_feedback",
        Frame::DatagramClose { .. } => "datagram_close",
        _ => "control",
    }
}
