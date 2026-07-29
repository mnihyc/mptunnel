//! Cancellation-safe MPP framing inside HTTP/3 request DATA.

use super::native_datagram::{NativeDatagramReceiver, NativeDatagramSender};
use super::presentation::{H3RecvStream, H3SendStream};
use super::{QuicCarrierError, QuicCarrierTelemetry};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_perf_record;
use crate::protocol::codec::{
    CodecLimits, decode_frame_bytes, encode_frame_into, encoded_frame_capacity_hint,
};
use crate::protocol::{DatagramFlowId, Frame};
use bytes::{Bytes, BytesMut};
use quinn::VarInt;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const FRAME_LEN_BYTES: usize = 4;
const QUIC_STREAM_RECORD_PAYLOAD_BYTES: usize = 10 * 1200;

#[derive(Debug)]
pub struct SendStream {
    pub(super) stream: H3SendStream,
    pub(super) native: NativeDatagramSender,
    pub(super) connection: quinn::Connection,
    pub(super) write_backlog: Arc<AtomicU64>,
    pub(super) telemetry: Arc<QuicCarrierTelemetry>,
    pub(super) known_datagram_flows: Arc<Mutex<DatagramFlowRegistry>>,
    pub(super) priority: i32,
}

impl SendStream {
    /// H3's current Quinn adapter does not expose the raw request stream after
    /// construction. Product/Core still schedule writes by traffic class; this
    /// value preserves the requested class for diagnostics and future RFC 9218
    /// transport-priority plumbing without changing MPP scheduling.
    pub fn set_priority(&mut self, priority: i32) -> Result<(), QuicCarrierError> {
        self.priority = priority;
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn priority(&self) -> Result<i32, QuicCarrierError> {
        Ok(self.priority)
    }

    pub async fn reject(&mut self) -> Result<(), QuicCarrierError> {
        self.stream.reject().await
    }
}

pub(super) struct QuicWriteTransaction {
    connection: quinn::Connection,
    write_backlog: Arc<AtomicU64>,
    packet_len: u64,
    close_if_cancelled: bool,
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
            close_if_cancelled: true,
        }
    }

    pub(super) fn complete(mut self) {
        self.close_if_cancelled = false;
    }

    pub(super) fn fail_stream(mut self) {
        self.close_if_cancelled = false;
    }
}

impl Drop for QuicWriteTransaction {
    fn drop(&mut self) {
        let _ = self
            .write_backlog
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(self.packet_len))
            });
        if self.close_if_cancelled {
            self.connection
                .close(VarInt::from_u32(1), b"cancelled HTTP/3 carrier write");
        }
    }
}

#[derive(Debug)]
pub struct RecvStream {
    pub(super) stream: H3RecvStream,
    pub(super) native: NativeDatagramReceiver,
    pub(super) known_datagram_flows: Arc<Mutex<DatagramFlowRegistry>>,
    read_buffer: BytesMut,
    pending_h3_data: Bytes,
    deferred_native: VecDeque<DeferredNative>,
    deferred_native_bytes: usize,
    max_deferred_native_bytes: usize,
}

#[derive(Debug)]
pub(super) struct DatagramFlowRegistry {
    active: HashSet<DatagramFlowId>,
    // Sorted inclusive ranges of every flow ID observed on this request. The
    // allocator is monotonic, so ordinary churn coalesces into one range.
    // Sparse history is bounded and compacted fail-closed.
    seen_ranges: Vec<(u64, u64)>,
    max_flows: usize,
    max_seen_ranges: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DatagramFlowState {
    Unknown,
    Active,
    Closed,
}

#[derive(Debug)]
struct DeferredNative {
    frame: Frame,
    deadline: std::time::Instant,
}

impl DatagramFlowRegistry {
    pub(super) fn new(max_flows: usize) -> Self {
        Self {
            active: HashSet::new(),
            seen_ranges: Vec::new(),
            max_flows: max_flows.max(1),
            max_seen_ranges: max_flows.max(1),
        }
    }

    fn state(&self, flow_id: DatagramFlowId) -> DatagramFlowState {
        if self.active.contains(&flow_id) {
            DatagramFlowState::Active
        } else if self.flow_was_seen(flow_id) {
            DatagramFlowState::Closed
        } else {
            DatagramFlowState::Unknown
        }
    }

    fn flow_was_seen(&self, flow_id: DatagramFlowId) -> bool {
        let value = flow_id.0;
        let index = self.seen_ranges.partition_point(|(_, end)| *end < value);
        self.seen_ranges
            .get(index)
            .is_some_and(|(start, end)| *start <= value && value <= *end)
    }

    fn record_seen(&mut self, flow_id: DatagramFlowId) {
        let value = flow_id.0;
        let index = self
            .seen_ranges
            .partition_point(|(_, end)| end.saturating_add(1) < value);
        if let Some((start, end)) = self.seen_ranges.get_mut(index)
            && value.saturating_add(1) >= *start
        {
            *start = (*start).min(value);
            *end = (*end).max(value);
            while index + 1 < self.seen_ranges.len()
                && self.seen_ranges[index].1.saturating_add(1) >= self.seen_ranges[index + 1].0
            {
                let next_end = self.seen_ranges.remove(index + 1).1;
                self.seen_ranges[index].1 = self.seen_ranges[index].1.max(next_end);
            }
        } else {
            self.seen_ranges.insert(index, (value, value));
        }
        while self.seen_ranges.len() > self.max_seen_ranges {
            let second_end = self.seen_ranges.remove(1).1;
            self.seen_ranges[0].1 = second_end;
        }
    }

    fn validate_transitions(&self, frames: &[Frame]) -> Result<(), QuicCarrierError> {
        let mut touched = HashMap::<DatagramFlowId, DatagramFlowState>::new();
        let mut active = self.active.len();
        for frame in frames {
            let (flow_id, opening) = match frame {
                Frame::OpenDatagramFlow { flow_id, .. } => (*flow_id, true),
                Frame::DatagramClose { flow_id } => (*flow_id, false),
                _ => continue,
            };
            let state = *touched
                .entry(flow_id)
                .or_insert_with(|| self.state(flow_id));
            let next = match (state, opening) {
                (DatagramFlowState::Unknown, true) => {
                    if active >= self.max_flows {
                        return Err(QuicCarrierError::NativeDatagramFlowsExhausted);
                    }
                    active = active.saturating_add(1);
                    DatagramFlowState::Active
                }
                (DatagramFlowState::Active, true) => DatagramFlowState::Active,
                (DatagramFlowState::Closed, true) => {
                    return Err(QuicCarrierError::InvalidNativeDatagram(
                        "closed HTTP Datagram flow cannot be reopened on the same request",
                    ));
                }
                (DatagramFlowState::Active, false) | (DatagramFlowState::Closed, false) => {
                    if state == DatagramFlowState::Active {
                        active = active.saturating_sub(1);
                    }
                    DatagramFlowState::Closed
                }
                (DatagramFlowState::Unknown, false) => {
                    return Err(QuicCarrierError::InvalidNativeDatagram(
                        "DATAGRAM_CLOSE preceded its reliable OPEN_DATAGRAM_FLOW",
                    ));
                }
            };
            touched.insert(flow_id, next);
        }
        Ok(())
    }

    fn apply_transitions(&mut self, frames: &[Frame]) -> Result<(), QuicCarrierError> {
        self.validate_transitions(frames)?;
        for frame in frames {
            match frame {
                Frame::OpenDatagramFlow { flow_id, .. } => {
                    self.active.insert(*flow_id);
                    self.record_seen(*flow_id);
                }
                Frame::DatagramClose { flow_id } => {
                    self.active.remove(flow_id);
                }
                _ => {}
            }
        }
        Ok(())
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
    if frames.iter().any(Frame::is_path_capacity) {
        return Err(QuicCarrierError::CapacityFrameOnQuic);
    }

    let mut reliable_start = 0usize;
    for (index, frame) in frames.iter().enumerate() {
        if !matches!(frame, Frame::DatagramData { .. }) {
            continue;
        }
        write_reliable_frames(send, &frames[reliable_start..index], limits).await?;
        let Frame::DatagramData { flow_id, .. } = frame else {
            unreachable!("DATAGRAM_DATA matched above");
        };
        if send
            .known_datagram_flows
            .lock()
            .expect("HTTP Datagram flow lock")
            .state(*flow_id)
            != DatagramFlowState::Active
        {
            return Err(QuicCarrierError::InvalidNativeDatagram(
                "DATAGRAM_DATA preceded its reliable OPEN_DATAGRAM_FLOW",
            ));
        }
        send.stream.ensure_datagrams_negotiated().await?;
        send.native.send_frame(frame, limits)?;
        reliable_start = index + 1;
    }
    write_reliable_frames(send, &frames[reliable_start..], limits).await
}

async fn write_reliable_frames(
    send: &mut SendStream,
    frames: &[Frame],
    limits: CodecLimits,
) -> Result<(), QuicCarrierError> {
    if frames.is_empty() {
        return Ok(());
    }
    if frames.iter().any(|frame| {
        matches!(
            frame,
            Frame::OpenDatagramFlow { .. } | Frame::DatagramClose { .. }
        )
    }) {
        send.known_datagram_flows
            .lock()
            .expect("HTTP Datagram flow lock")
            .validate_transitions(frames)?;
    }
    let delivery_evidence_bytes = frames.iter().fold(0_u64, |total, frame| {
        total.saturating_add(frame.delivery_evidence_bytes())
    });
    #[cfg(feature = "lab-diagnostics")]
    let encode_started = std::time::Instant::now();
    let mut packet = Vec::new();
    let capacity_hint = frames.iter().fold(0usize, |total, frame| {
        total.saturating_add(quic_encoded_frame_capacity_hint(frame))
    });
    packet.reserve(capacity_hint);
    for frame in frames {
        encode_quic_length_prefixed_frame(frame, limits, &mut packet)?;
    }
    let packet_len = packet.len() as u64;
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record(
        "transport.quic.encode_frames",
        encode_started.elapsed(),
        packet_len as usize,
    );
    #[cfg(feature = "lab-diagnostics")]
    let write_started = std::time::Instant::now();
    send.write_backlog.fetch_add(packet_len, Ordering::Relaxed);
    if delivery_evidence_bytes > 0 {
        send.telemetry
            .record_delivery_evidence_written(delivery_evidence_bytes);
    }
    let transaction = QuicWriteTransaction::new(
        send.connection.clone(),
        send.write_backlog.clone(),
        packet_len,
    );
    if let Err(err) = send.stream.send_data(Bytes::from(packet)).await {
        transaction.fail_stream();
        return Err(err);
    }
    transaction.complete();
    if frames.iter().any(|frame| {
        matches!(
            frame,
            Frame::OpenDatagramFlow { .. } | Frame::DatagramClose { .. }
        )
    }) {
        send.known_datagram_flows
            .lock()
            .expect("HTTP Datagram flow lock")
            .apply_transitions(frames)?;
    }
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
    pub(super) fn new(
        stream: H3RecvStream,
        native: NativeDatagramReceiver,
        known_datagram_flows: Arc<Mutex<DatagramFlowRegistry>>,
        max_deferred_native_bytes: usize,
    ) -> Self {
        Self {
            stream,
            native,
            known_datagram_flows,
            read_buffer: BytesMut::new(),
            pending_h3_data: Bytes::new(),
            deferred_native: VecDeque::new(),
            deferred_native_bytes: 0,
            max_deferred_native_bytes: max_deferred_native_bytes.max(1),
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
        let frame = decode_frame_bytes(encoded, limits)?;
        self.accept_reliable_frame(frame).map(Some)
    }

    fn pop_pending_h3_frame(
        &mut self,
        limits: CodecLimits,
    ) -> Result<Option<Frame>, QuicCarrierError> {
        let Some(frame) = decode_ready_h3_frame(&mut self.pending_h3_data, limits)? else {
            return Ok(None);
        };
        self.accept_reliable_frame(frame).map(Some)
    }

    fn accept_reliable_frame(&mut self, frame: Frame) -> Result<Frame, QuicCarrierError> {
        if matches!(
            frame,
            Frame::OpenDatagramFlow { .. } | Frame::DatagramClose { .. }
        ) {
            self.known_datagram_flows
                .lock()
                .expect("HTTP Datagram flow lock")
                .apply_transitions(std::slice::from_ref(&frame))?;
        }
        if let Frame::DatagramClose { flow_id } = frame {
            self.purge_deferred_native(flow_id);
        }
        Ok(frame)
    }

    fn pop_ready_native(&mut self) -> Option<Frame> {
        if self.deferred_native.is_empty() {
            return None;
        }
        self.expire_deferred_native();
        if self.deferred_native.is_empty() {
            return None;
        }
        let known = self
            .known_datagram_flows
            .lock()
            .expect("HTTP Datagram flow lock");
        let index = self.deferred_native.iter().position(|deferred| {
            matches!(
                deferred.frame,
                Frame::DatagramData { flow_id, .. }
                    if known.state(flow_id) == DatagramFlowState::Active
            )
        })?;
        let deferred = self
            .deferred_native
            .remove(index)
            .expect("deferred native frame index exists");
        self.deferred_native_bytes = self
            .deferred_native_bytes
            .saturating_sub(deferred.frame.delivery_evidence_bytes() as usize);
        Some(deferred.frame)
    }

    fn native_flow_state(&self, frame: &Frame) -> DatagramFlowState {
        let Frame::DatagramData { flow_id, .. } = frame else {
            return DatagramFlowState::Unknown;
        };
        self.known_datagram_flows
            .lock()
            .expect("HTTP Datagram flow lock")
            .state(*flow_id)
    }

    fn defer_native(&mut self, frame: Frame) {
        let bytes = frame.delivery_evidence_bytes() as usize;
        if self.deferred_native_bytes.saturating_add(bytes) > self.max_deferred_native_bytes {
            return;
        }
        let ttl_ms = match &frame {
            Frame::DatagramData { ttl_ms, .. } => *ttl_ms,
            _ => return,
        };
        if ttl_ms == 0 {
            return;
        }
        self.deferred_native_bytes = self.deferred_native_bytes.saturating_add(bytes);
        self.deferred_native.push_back(DeferredNative {
            frame,
            deadline: std::time::Instant::now()
                + std::time::Duration::from_millis(u64::from(ttl_ms)),
        });
    }

    fn expire_deferred_native(&mut self) {
        let now = std::time::Instant::now();
        let mut retained_bytes = 0usize;
        self.deferred_native.retain(|deferred| {
            let retain = deferred.deadline > now;
            if retain {
                retained_bytes = retained_bytes
                    .saturating_add(deferred.frame.delivery_evidence_bytes() as usize);
            }
            retain
        });
        self.deferred_native_bytes = retained_bytes;
    }

    fn purge_deferred_native(&mut self, closed_flow_id: DatagramFlowId) {
        let mut retained_bytes = 0usize;
        self.deferred_native.retain(|deferred| {
            let retain = !matches!(
                deferred.frame,
                Frame::DatagramData { flow_id, .. } if flow_id == closed_flow_id
            );
            if retain {
                retained_bytes = retained_bytes
                    .saturating_add(deferred.frame.delivery_evidence_bytes() as usize);
            }
            retain
        });
        self.deferred_native_bytes = retained_bytes;
    }
}

fn decode_ready_h3_frame(
    pending: &mut Bytes,
    limits: CodecLimits,
) -> Result<Option<Frame>, QuicCarrierError> {
    if pending.len() < FRAME_LEN_BYTES {
        return Ok(None);
    }
    let frame_len = u32::from_be_bytes([pending[0], pending[1], pending[2], pending[3]]) as usize;
    if frame_len > limits.max_frame_bytes {
        return Err(QuicCarrierError::FrameTooLarge);
    }
    let record_len = FRAME_LEN_BYTES.saturating_add(frame_len);
    if pending.len() < record_len {
        return Ok(None);
    }

    let record = pending.split_to(record_len);
    let frame = decode_frame_bytes(record.slice(FRAME_LEN_BYTES..), limits)?;
    let Frame::StreamData {
        stream_id,
        offset,
        payload,
    } = frame
    else {
        return Ok(Some(frame));
    };

    let frame_overhead = frame_len.saturating_sub(payload.len());
    let mut total_payload_len = payload.len();
    let mut coalesced_payload = None::<BytesMut>;
    loop {
        if pending.len() < FRAME_LEN_BYTES {
            break;
        }
        let next_frame_len =
            u32::from_be_bytes([pending[0], pending[1], pending[2], pending[3]]) as usize;
        if next_frame_len > limits.max_frame_bytes {
            break;
        }
        let next_record_len = FRAME_LEN_BYTES.saturating_add(next_frame_len);
        if pending.len() < next_record_len {
            break;
        }
        let next = match decode_frame_bytes(pending.slice(FRAME_LEN_BYTES..next_record_len), limits)
        {
            Ok(frame) => frame,
            Err(_) => break,
        };
        let Frame::StreamData {
            stream_id: next_stream_id,
            offset: next_offset,
            payload: next_payload,
        } = next
        else {
            break;
        };
        let Some(expected_offset) = offset.checked_add(total_payload_len as u64) else {
            break;
        };
        if next_stream_id != stream_id || next_offset != expected_offset {
            break;
        }
        let Some(next_total_payload_len) = total_payload_len.checked_add(next_payload.len()) else {
            break;
        };
        let within_frame_limit = frame_overhead
            .checked_add(next_total_payload_len)
            .is_some_and(|len| len <= limits.max_frame_bytes);
        if next_total_payload_len > limits.max_payload_bytes || !within_frame_limit {
            break;
        }

        let combined = coalesced_payload.get_or_insert_with(|| {
            let mut combined = BytesMut::with_capacity(next_total_payload_len);
            combined.extend_from_slice(&payload);
            combined
        });
        combined.extend_from_slice(&next_payload);
        total_payload_len = next_total_payload_len;
        let _ = pending.split_to(next_record_len);
    }

    Ok(Some(Frame::StreamData {
        stream_id,
        offset,
        payload: coalesced_payload.map_or(payload, BytesMut::freeze),
    }))
}

pub async fn read_frame(
    recv: &mut RecvStream,
    limits: CodecLimits,
) -> Result<Frame, QuicCarrierError> {
    if let Err(error) = recv.stream.ensure_success_response().await {
        recv.native.retire_route();
        return Err(error);
    }
    loop {
        if let Some(frame) = recv.pop_buffered_frame(limits)? {
            return Ok(frame);
        }
        if let Some(frame) = recv.pop_ready_native() {
            return Ok(frame);
        }
        if !recv.pending_h3_data.is_empty() {
            if recv.read_buffer.is_empty()
                && let Some(frame) = recv.pop_pending_h3_frame(limits)?
            {
                return Ok(frame);
            }
            let required = match recv.buffered_frame_len(limits)? {
                Some(frame_len) => FRAME_LEN_BYTES
                    .saturating_add(frame_len)
                    .saturating_sub(recv.read_buffer.len()),
                None => FRAME_LEN_BYTES.saturating_sub(recv.read_buffer.len()),
            };
            let copy_len = required.min(recv.pending_h3_data.len());
            recv.read_buffer
                .extend_from_slice(&recv.pending_h3_data.split_to(copy_len));
            continue;
        }

        tokio::select! {
            data = recv.stream.recv_data() => {
                let data = match data {
                    Ok(Some(data)) => data,
                    Ok(None) => {
                        recv.native.retire_route();
                        return if recv.read_buffer.is_empty() {
                            Err(QuicCarrierError::StreamFinished)
                        } else {
                            Err(QuicCarrierError::UnexpectedEnd)
                        };
                    }
                    Err(error) => {
                        recv.native.retire_route();
                        return Err(error);
                    }
                };
                if data.is_empty() {
                    continue;
                }
                // H3 DATA boundaries are independent from MPP record
                // boundaries. Retain the adapter-owned chunk and copy only
                // enough to complete the current length-prefixed record so
                // read_buffer itself never exceeds the configured frame cap.
                recv.pending_h3_data = data;
            }
            frame = recv.native.recv_frame(limits) => {
                let frame = match frame {
                    Ok(frame) => frame,
                    Err(error) => {
                        recv.native.retire_route();
                        return Err(error);
                    }
                };
                match recv.native_flow_state(&frame) {
                    DatagramFlowState::Active => return Ok(frame),
                    DatagramFlowState::Closed => {
                        // RFC 9297 requires datagrams associated with a closed
                        // request direction to be silently dropped. MPP also
                        // closes each inner flow reliably before this state.
                    }
                    DatagramFlowState::Unknown => {
                        // QUIC DATAGRAM can legally overtake the reliable
                        // OPEN_DATAGRAM_FLOW DATA. Keep only a bounded amount
                        // until that open is decoded; there is no cross-flow
                        // reliable HOL.
                        recv.defer_native(frame);
                    }
                }
            }
        }
    }
}

pub async fn finish_stream(send: &mut SendStream) -> Result<(), QuicCarrierError> {
    send.stream.finish().await
}

#[cfg(test)]
#[path = "stream_test.rs"]
mod tests;
