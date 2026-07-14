//! Optional observation of response sender decisions after policy selection.
//! Instrumentation cannot participate in admission or snapshot construction.

#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_sender_service_decision;
use crate::model::path::CarrierPathKey;
use crate::protocol::{Frame, SessionId, StreamId};
#[cfg(feature = "lab-diagnostics")]
use crate::runtime::relay::io::frame_pacing_bytes;
#[cfg(feature = "lab-diagnostics")]
use crate::runtime::relay_striping::reliable_stream_frame_payload_bytes;
use crate::scheduler::FlowLane;

pub(in crate::runtime) fn record_server_sender_decision(
    session_id: SessionId,
    stream_id: StreamId,
    key: CarrierPathKey,
    frame: &Frame,
    lane: FlowLane,
    reason: &'static str,
    bulk_rate_evidence: Option<bool>,
) {
    #[cfg(feature = "lab-diagnostics")]
    lab_sender_service_decision(
        "server",
        Some(session_id.0),
        stream_id.0,
        reason,
        sender_service_frame_kind(frame),
        reliable_stream_frame_payload_bytes(frame),
        bulk_rate_evidence,
        format_args!(
            "path_underlay={:?} path_id={} lane={:?} pacing_bytes={}",
            key.underlay,
            key.path_id.0,
            lane,
            frame_pacing_bytes(frame),
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
pub(super) fn sender_service_frame_kind(frame: &Frame) -> &'static str {
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
