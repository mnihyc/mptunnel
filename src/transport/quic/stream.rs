//! Cancellation-safe framed I/O over ordered QUIC streams.

use super::{QuicCarrierError, QuicCarrierTelemetry};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_perf_record;
use crate::protocol::codec::{
    CodecLimits, decode_frame_bytes, encode_frame_into, encoded_frame_capacity_hint,
};
use crate::protocol::{Frame, FrameWriteClass};
use bytes::BytesMut;
use quinn::VarInt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const FRAME_LEN_BYTES: usize = 4;
const QUIC_RECV_CHUNK_BYTES: usize = 64 * 1024;
// Carrier recordization limit for length-prefixed STREAM_DATA frames written on
// an ordered QUIC stream. This must not be confused with the product sender
// quantum: MPP scheduling still emits its bounded service quantum, while
// this writer splits only the serialized records so a lost QUIC packet does not
// withhold an entire product quantum from the peer.
const QUIC_STREAM_RECORD_PAYLOAD_BYTES: usize = 10 * 1200;
#[derive(Debug)]
pub struct SendStream {
    pub(super) stream: quinn::SendStream,
    pub(super) connection: quinn::Connection,
    pub(super) write_backlog: Arc<AtomicU64>,
    pub(super) delivery_evidence_written: Arc<AtomicU64>,
    pub(super) telemetry: Arc<QuicCarrierTelemetry>,
    pub(super) encode_buffer: Vec<u8>,
}

// Quinn writes can consume a prefix before cancellation. Fail the whole path
// so record framing never resumes from an ambiguous carrier-stream offset.
pub(super) struct QuicWriteTransaction {
    connection: quinn::Connection,
    write_backlog: Arc<AtomicU64>,
    packet_len: u64,
    fail_close: bool,
}

impl QuicWriteTransaction {
    pub(super) fn new(
        connection: quinn::Connection,
        write_backlog: Arc<AtomicU64>,
        packet_len: u64,
    ) -> Self {
        Self {
            connection,
            write_backlog,
            packet_len,
            fail_close: true,
        }
    }

    pub(super) fn commit(mut self) {
        self.fail_close = false;
    }
}

impl Drop for QuicWriteTransaction {
    fn drop(&mut self) {
        let _ = self
            .write_backlog
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(self.packet_len))
            });
        if self.fail_close {
            self.connection
                .close(VarInt::from_u32(1), b"cancelled or failed carrier write");
        }
    }
}

#[derive(Debug)]
pub struct RecvStream {
    stream: quinn::RecvStream,
    // QUIC RecvStream::read_exact is explicitly not cancellation-safe. The
    // runtime polls path reads inside tokio::select!, where a local read, ACK,
    // timer, or measurement notification may cancel the pending branch. Keep the
    // partially-read carrier bytes here and advance the underlying QUIC stream
    // only through cancel-safe read() calls. Otherwise a cancelled frame read
    // can silently drop the already-consumed prefix and desynchronize the
    // length-prefixed mptunnel frame stream, which shows up as random stalls,
    // reinjection storms, and bursty zero-throughput intervals on QUIC paths.
    read_buffer: BytesMut,
    read_scratch: Vec<u8>,
}

impl SendStream {
    pub fn cancel_measurement(&self, token: u64) -> bool {
        let should_close = self.telemetry.abort_measurement(token);
        if should_close {
            self.connection
                .close(VarInt::from_u32(1), b"cancelled measurement epoch");
        }
        should_close
    }
}

pub async fn write_frame(
    send: &mut SendStream,
    frame: &Frame,
    limits: CodecLimits,
) -> Result<(), QuicCarrierError> {
    write_frames(send, std::slice::from_ref(frame), limits).await
}

pub async fn write_frames(
    send: &mut SendStream,
    frames: &[Frame],
    limits: CodecLimits,
) -> Result<(), QuicCarrierError> {
    if frames.is_empty() {
        return Ok(());
    }
    let delivery_evidence_bytes = frames.iter().try_fold(0_u64, |total, frame| {
        let FrameWriteClass::Ordinary {
            delivery_evidence_bytes,
        } = frame.write_class()
        else {
            return Err(QuicCarrierError::MeasurementRecordRequiresDedicatedWrite);
        };
        Ok(total.saturating_add(delivery_evidence_bytes))
    })?;
    let _ordinary_write = send.telemetry.enter_ordinary_writer().await;
    if send.telemetry.measurement_failed_closed() {
        send.connection
            .close(VarInt::from_u32(1), b"measurement epoch failed closed");
        return Err(QuicCarrierError::MeasurementExpired);
    }
    #[cfg(feature = "lab-diagnostics")]
    let encode_started = std::time::Instant::now();
    let packet_len = {
        let packet = &mut send.encode_buffer;
        packet.clear();
        let capacity_hint = frames.iter().fold(0usize, |total, frame| {
            total.saturating_add(quic_encoded_frame_capacity_hint(frame))
        });
        packet.reserve(capacity_hint);
        for frame in frames {
            encode_quic_length_prefixed_frame(frame, limits, packet)?;
        }
        packet.len() as u64
    };
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record(
        "transport.quic.encode_frames",
        encode_started.elapsed(),
        packet_len as usize,
    );
    #[cfg(feature = "lab-diagnostics")]
    let write_started = std::time::Instant::now();
    let transaction_connection = send.connection.clone();
    let transaction_backlog = send.write_backlog.clone();
    send.write_backlog.fetch_add(packet_len, Ordering::Relaxed);
    // Publish before the awaited write. Quinn can ACK earlier chunks while
    // write_all is flow-controlled; publishing afterward loses attribution for
    // those ACKs. A failed write closes the path, so stale evidence cannot be
    // reused by a live measurement target.
    if delivery_evidence_bytes > 0 {
        send.delivery_evidence_written
            .fetch_add(delivery_evidence_bytes, Ordering::Relaxed);
    }
    let write_transaction =
        QuicWriteTransaction::new(transaction_connection, transaction_backlog, packet_len);
    send.stream.write_all(&send.encode_buffer).await?;
    write_transaction.commit();
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record(
        "transport.quic.write_frames_wait",
        write_started.elapsed(),
        packet_len as usize,
    );
    Ok(())
}

pub(super) fn quic_encoded_frame_capacity_hint(frame: &Frame) -> usize {
    match frame {
        Frame::StreamData { payload, .. } if payload.len() > QUIC_STREAM_RECORD_PAYLOAD_BYTES => {
            let chunks = payload.len().div_ceil(QUIC_STREAM_RECORD_PAYLOAD_BYTES);
            encoded_frame_capacity_hint(frame)
                .saturating_add(chunks.saturating_mul(FRAME_LEN_BYTES + 32))
        }
        _ => FRAME_LEN_BYTES.saturating_add(encoded_frame_capacity_hint(frame)),
    }
}

pub(super) fn encode_quic_length_prefixed_frame(
    frame: &Frame,
    limits: CodecLimits,
    packet: &mut Vec<u8>,
) -> Result<(), QuicCarrierError> {
    let Frame::StreamData {
        stream_id,
        offset,
        payload,
    } = frame
    else {
        return encode_length_prefixed_frame(frame, limits, packet);
    };

    if payload.len() <= QUIC_STREAM_RECORD_PAYLOAD_BYTES {
        return encode_length_prefixed_frame(frame, limits, packet);
    }

    let mut cursor = 0usize;
    while cursor < payload.len() {
        let next = cursor
            .saturating_add(QUIC_STREAM_RECORD_PAYLOAD_BYTES)
            .min(payload.len());
        let split = Frame::StreamData {
            stream_id: *stream_id,
            offset: offset.saturating_add(cursor as u64),
            payload: payload.slice(cursor..next),
        };
        encode_length_prefixed_frame(&split, limits, packet)?;
        cursor = next;
    }
    Ok(())
}

fn encode_length_prefixed_frame(
    frame: &Frame,
    limits: CodecLimits,
    packet: &mut Vec<u8>,
) -> Result<(), QuicCarrierError> {
    let len_offset = packet.len();
    packet.extend_from_slice(&[0u8; FRAME_LEN_BYTES]);
    let frame_start = packet.len();
    encode_frame_into(frame, limits, packet)?;
    let frame_len = packet.len().saturating_sub(frame_start);
    let frame_len = u32::try_from(frame_len).map_err(|_| QuicCarrierError::FrameTooLarge)?;
    packet[len_offset..len_offset + FRAME_LEN_BYTES].copy_from_slice(&frame_len.to_be_bytes());
    Ok(())
}

impl RecvStream {
    pub(super) fn new(stream: quinn::RecvStream) -> Self {
        Self {
            stream,
            read_buffer: BytesMut::new(),
            read_scratch: Vec::new(),
        }
    }

    fn buffered_frame_len(&self, limits: CodecLimits) -> Result<Option<usize>, QuicCarrierError> {
        if self.read_buffer.len() < FRAME_LEN_BYTES {
            return Ok(None);
        }
        let len = u32::from_be_bytes([
            self.read_buffer[0],
            self.read_buffer[1],
            self.read_buffer[2],
            self.read_buffer[3],
        ]) as usize;
        if len > limits.max_frame_bytes {
            return Err(QuicCarrierError::FrameTooLarge);
        }
        Ok(Some(len))
    }

    fn pop_buffered_frame(
        &mut self,
        limits: CodecLimits,
    ) -> Result<Option<Frame>, QuicCarrierError> {
        let Some(len) = self.buffered_frame_len(limits)? else {
            return Ok(None);
        };
        let frame_end = FRAME_LEN_BYTES.saturating_add(len);
        if self.read_buffer.len() < frame_end {
            return Ok(None);
        }
        let _ = self.read_buffer.split_to(FRAME_LEN_BYTES);
        let encoded = self.read_buffer.split_to(len).freeze();
        Ok(Some(decode_frame_bytes(encoded, limits)?))
    }

    fn next_read_len(&self, limits: CodecLimits) -> Result<usize, QuicCarrierError> {
        let wanted = match self.buffered_frame_len(limits)? {
            Some(len) => FRAME_LEN_BYTES.saturating_add(len),
            None => FRAME_LEN_BYTES,
        };
        Ok(wanted
            .saturating_sub(self.read_buffer.len())
            .clamp(1, QUIC_RECV_CHUNK_BYTES))
    }
}

pub async fn read_frame(
    recv: &mut RecvStream,
    limits: CodecLimits,
) -> Result<Frame, QuicCarrierError> {
    loop {
        if let Some(frame) = recv.pop_buffered_frame(limits)? {
            return Ok(frame);
        }

        let read_len = recv.next_read_len(limits)?;
        recv.read_scratch.resize(read_len, 0);
        let read = recv
            .stream
            .read(&mut recv.read_scratch[..])
            .await
            .map_err(QuicCarrierError::Read)?
            .ok_or(QuicCarrierError::UnexpectedEnd)?;
        if read == 0 {
            return Err(QuicCarrierError::UnexpectedEnd);
        }
        recv.read_buffer
            .extend_from_slice(&recv.read_scratch[..read]);
    }
}

pub fn finish_stream(send: &mut SendStream) -> Result<(), QuicCarrierError> {
    // FIN is application output too. Refuse it while a measurement epoch owns the
    // connection rather than silently adding unclassified carrier bytes.
    let _ordinary_write = send
        .telemetry
        .try_enter_ordinary_writer()
        .ok_or(QuicCarrierError::MeasurementActive)?;
    if send.telemetry.measurement_failed_closed() {
        send.connection
            .close(VarInt::from_u32(1), b"measurement epoch failed closed");
        return Err(QuicCarrierError::MeasurementExpired);
    }
    Ok(send.stream.finish()?)
}

#[cfg(test)]
#[path = "stream_test.rs"]
mod tests;
