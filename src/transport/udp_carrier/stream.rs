use super::error::UdpCarrierFrameError;
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_perf_record;
use crate::protocol::Frame;
use crate::protocol::codec::{CodecLimits, decode_frame, encode_frame};
use bytes::Bytes;
#[cfg(feature = "lab-diagnostics")]
use std::time::Instant;
use tokio::sync::mpsc;

#[derive(Debug)]
pub(super) enum StreamCommand {
    SendFrame {
        ordered: bool,
        reliable: bool,
        stream_id: u64,
        frame_id: u64,
        encoded: Bytes,
        next_offset: usize,
    },
    Finish {
        stream_id: u64,
    },
}

impl StreamCommand {
    pub(super) fn stream_id(&self) -> u64 {
        match self {
            Self::SendFrame { stream_id, .. } | Self::Finish { stream_id } => *stream_id,
        }
    }
}

#[derive(Debug)]
pub struct SendStream {
    pub(super) stream_id: u64,
    pub(super) next_ordered_frame_id: u64,
    pub(super) next_unordered_frame_id: u64,
    pub(super) commands: mpsc::Sender<StreamCommand>,
}

#[derive(Debug)]
pub struct RecvStream {
    pub(super) frames: mpsc::Receiver<Bytes>,
}

impl SendStream {
    pub(super) fn new(stream_id: u64, commands: mpsc::Sender<StreamCommand>) -> Self {
        Self {
            stream_id,
            next_ordered_frame_id: 0,
            next_unordered_frame_id: 0,
            commands,
        }
    }

    async fn send_encoded(
        &mut self,
        encoded: Bytes,
        ordered: bool,
        reliable: bool,
    ) -> Result<(), UdpCarrierFrameError> {
        let frame_id = if ordered {
            let frame_id = self.next_ordered_frame_id;
            self.next_ordered_frame_id = self.next_ordered_frame_id.checked_add(1).ok_or(
                UdpCarrierFrameError::InvalidPacket("ordered frame id overflow"),
            )?;
            frame_id
        } else {
            let frame_id = self.next_unordered_frame_id;
            self.next_unordered_frame_id = self.next_unordered_frame_id.checked_add(1).ok_or(
                UdpCarrierFrameError::InvalidPacket("unordered frame id overflow"),
            )?;
            frame_id
        };
        self.commands
            .send(StreamCommand::SendFrame {
                ordered,
                reliable,
                stream_id: self.stream_id,
                frame_id,
                encoded,
                next_offset: 0,
            })
            .await
            .map_err(|_| UdpCarrierFrameError::QueueClosed)
    }
}

impl RecvStream {
    pub(super) fn new(frames: mpsc::Receiver<Bytes>) -> Self {
        Self { frames }
    }
}

pub async fn read_frame(
    recv: &mut RecvStream,
    limits: CodecLimits,
) -> Result<Frame, UdpCarrierFrameError> {
    #[cfg(feature = "lab-diagnostics")]
    let total_started = Instant::now();
    #[cfg(feature = "lab-diagnostics")]
    let stage_started = Instant::now();
    let encoded = recv
        .frames
        .recv()
        .await
        .ok_or(UdpCarrierFrameError::Closed)?;
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record(
        "transport.udp_carrier.read_frame_wait",
        stage_started.elapsed(),
        encoded.len(),
    );
    #[cfg(feature = "lab-diagnostics")]
    let stage_started = Instant::now();
    let frame = decode_frame(&encoded, limits)?;
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record(
        "transport.udp_carrier.decode_frame",
        stage_started.elapsed(),
        encoded.len(),
    );
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record(
        "transport.udp_carrier.read_frame_total",
        total_started.elapsed(),
        encoded.len(),
    );
    Ok(frame)
}

pub async fn write_frame(
    send: &mut SendStream,
    frame: &Frame,
    limits: CodecLimits,
) -> Result<(), UdpCarrierFrameError> {
    #[cfg(feature = "lab-diagnostics")]
    let total_started = Instant::now();
    #[cfg(feature = "lab-diagnostics")]
    let stage_started = Instant::now();
    let encoded = Bytes::from(encode_frame(frame, limits)?);
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record(
        "transport.udp_carrier.encode_frame",
        stage_started.elapsed(),
        encoded.len(),
    );
    #[cfg(feature = "lab-diagnostics")]
    let stage_started = Instant::now();
    #[cfg(feature = "lab-diagnostics")]
    let encoded_len = encoded.len();
    let mode = carrier_frame_mode(frame);
    send.send_encoded(encoded, mode.ordered, mode.reliable)
        .await?;
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record(
        "transport.udp_carrier.write_queue_wait",
        stage_started.elapsed(),
        encoded_len,
    );
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record(
        "transport.udp_carrier.write_frame_total",
        total_started.elapsed(),
        encoded_len,
    );
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct CarrierFrameMode {
    ordered: bool,
    reliable: bool,
}

fn carrier_frame_mode(frame: &Frame) -> CarrierFrameMode {
    match frame {
        Frame::StreamData { .. } => CarrierFrameMode {
            ordered: false,
            reliable: true,
        },
        Frame::StreamAck { .. } => CarrierFrameMode {
            ordered: false,
            reliable: false,
        },
        Frame::DatagramData { .. }
        | Frame::DatagramFeedback { .. }
        | Frame::PathMetrics { .. }
        | Frame::RxRateHint { .. } => CarrierFrameMode {
            ordered: false,
            reliable: false,
        },
        _ => CarrierFrameMode {
            ordered: true,
            reliable: true,
        },
    }
}

pub fn finish_stream(send: &mut SendStream) -> Result<(), UdpCarrierFrameError> {
    let _ = send.commands.try_send(StreamCommand::Finish {
        stream_id: send.stream_id,
    });
    Ok(())
}
