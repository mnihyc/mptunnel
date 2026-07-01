use crate::mux::MuxLimits;
use crate::protocol::{Frame, OffsetRange, StreamFlags, StreamId};
use bytes::Bytes;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReliableSendStream {
    stream_id: StreamId,
    next_offset: u64,
    max_offset: u64,
    repair_bytes: usize,
    repair_cache: BTreeMap<u64, SentChunk>,
    limits: MuxLimits,
}

impl ReliableSendStream {
    pub fn new(stream_id: StreamId, limits: MuxLimits) -> Self {
        Self {
            stream_id,
            next_offset: 0,
            max_offset: limits.max_stream_window_bytes,
            repair_bytes: 0,
            repair_cache: BTreeMap::new(),
            limits,
        }
    }

    pub fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    pub fn send_credit_bytes(&self) -> usize {
        usize::try_from(self.max_offset.saturating_sub(self.next_offset)).unwrap_or(usize::MAX)
    }

    pub fn repair_bytes(&self) -> usize {
        self.repair_bytes
    }

    pub fn update_max_offset(&mut self, max_offset: u64) {
        self.max_offset = self.max_offset.max(max_offset);
    }

    pub fn send_data(&mut self, payload: Bytes, flags: StreamFlags) -> Result<Frame, StreamError> {
        if payload.is_empty() {
            return Err(StreamError::EmptyPayload);
        }
        if payload.len() > self.limits.max_payload_bytes {
            return Err(StreamError::PayloadTooLarge {
                actual: payload.len(),
                limit: self.limits.max_payload_bytes,
            });
        }
        let end = self
            .next_offset
            .checked_add(payload.len() as u64)
            .ok_or(StreamError::OffsetOverflow)?;
        if end > self.max_offset {
            return Err(StreamError::FlowControlBlocked {
                end,
                max: self.max_offset,
            });
        }
        let new_repair_bytes =
            self.repair_bytes
                .checked_add(payload.len())
                .ok_or(StreamError::RepairCacheFull {
                    actual: usize::MAX,
                    limit: self.limits.max_repair_bytes,
                })?;
        if new_repair_bytes > self.limits.max_repair_bytes {
            return Err(StreamError::RepairCacheFull {
                actual: new_repair_bytes,
                limit: self.limits.max_repair_bytes,
            });
        }

        let offset = self.next_offset;
        self.next_offset = end;
        self.repair_bytes = new_repair_bytes;
        self.repair_cache.insert(
            offset,
            SentChunk {
                offset,
                payload: payload.clone(),
                flags,
            },
        );

        Ok(Frame::StreamData {
            stream_id: self.stream_id,
            offset,
            flags,
            payload,
        })
    }

    pub fn apply_ack(&mut self, ranges: &[OffsetRange]) -> AckOutcome {
        let ranges = normalized_offset_ranges(ranges);
        if ranges.is_empty() || self.repair_cache.is_empty() {
            return AckOutcome {
                released_bytes: 0,
                released_chunks: 0,
                remaining_repair_bytes: self.repair_bytes,
            };
        }

        let mut released_chunks = 0usize;
        let previous_repair_bytes = self.repair_bytes;
        let mut retained = BTreeMap::new();
        let mut retained_bytes = 0usize;
        for chunk in self.repair_cache.values() {
            let pieces = unacked_chunk_slices(chunk, &ranges);
            if pieces.is_empty() {
                released_chunks += 1;
                continue;
            }
            for piece in pieces {
                retained_bytes = retained_bytes.saturating_add(piece.payload.len());
                retained.insert(piece.offset, piece);
            }
        }
        self.repair_cache = retained;
        self.repair_bytes = retained_bytes;
        let released_bytes = previous_repair_bytes.saturating_sub(retained_bytes);
        AckOutcome {
            released_bytes,
            released_chunks,
            remaining_repair_bytes: self.repair_bytes,
        }
    }

    pub fn retransmission_frames_for_ack_gaps(
        &self,
        ranges: &[OffsetRange],
        byte_limit: usize,
    ) -> Vec<Frame> {
        if byte_limit == 0 {
            return Vec::new();
        }
        let ranges = normalized_offset_ranges(ranges);
        let Some(largest_acked_end) = ranges.iter().map(|range| range.end).max() else {
            return Vec::new();
        };

        let mut frames = Vec::new();
        let mut emitted_bytes = 0usize;
        for chunk in self.repair_cache.values() {
            let chunk_start = chunk.offset;
            let chunk_end = chunk.offset.saturating_add(chunk.payload.len() as u64);
            if chunk_start >= largest_acked_end {
                break;
            }
            let repair_end = chunk_end.min(largest_acked_end);
            let mut cursor = chunk_start;
            let mut range_index = ranges.partition_point(|range| range.end <= chunk_start);
            while range_index < ranges.len() {
                let range = ranges[range_index];
                if range.start >= repair_end {
                    break;
                }
                if range.start > cursor
                    && !push_retransmission_slice(
                        &mut frames,
                        self.stream_id,
                        chunk,
                        cursor,
                        range.start.min(repair_end),
                        byte_limit,
                        &mut emitted_bytes,
                    )
                {
                    return frames;
                }
                cursor = cursor.max(range.end).min(repair_end);
                if cursor >= repair_end {
                    break;
                }
                range_index += 1;
            }
            if cursor < repair_end
                && !push_retransmission_slice(
                    &mut frames,
                    self.stream_id,
                    chunk,
                    cursor,
                    repair_end,
                    byte_limit,
                    &mut emitted_bytes,
                )
            {
                return frames;
            }
        }
        frames
    }

    pub fn retransmission_frames_after_ack_frontier(
        &self,
        ranges: &[OffsetRange],
        byte_limit: usize,
    ) -> Vec<Frame> {
        if byte_limit == 0 || self.repair_cache.is_empty() {
            return Vec::new();
        }
        let ranges = normalized_offset_ranges(ranges);
        let Some(largest_acked_end) = ranges.iter().map(|range| range.end).max() else {
            return Vec::new();
        };
        if largest_acked_end >= self.next_offset {
            return Vec::new();
        }
        self.retransmission_frames_for_ranges(
            &[OffsetRange {
                start: largest_acked_end,
                end: self.next_offset,
            }],
            byte_limit,
        )
    }

    pub fn retransmission_frames_for_ranges(
        &self,
        ranges: &[OffsetRange],
        byte_limit: usize,
    ) -> Vec<Frame> {
        if byte_limit == 0 || ranges.is_empty() {
            return Vec::new();
        }
        let ranges = normalized_offset_ranges(ranges);
        let mut frames = Vec::new();
        let mut emitted_bytes = 0usize;
        let mut range_index = 0usize;
        for chunk in self.repair_cache.values() {
            while range_index < ranges.len() {
                let range = ranges[range_index];
                if range.end <= chunk.offset {
                    range_index += 1;
                    continue;
                }
                break;
            }
            let chunk_start = chunk.offset;
            let chunk_end = chunk.offset.saturating_add(chunk.payload.len() as u64);
            let mut current_index = range_index;
            while current_index < ranges.len() {
                let range = ranges[current_index];
                if range.start >= chunk_end {
                    break;
                }
                if range.end > chunk_start {
                    let start = range.start.max(chunk_start);
                    let end = range.end.min(chunk_end);
                    if !push_retransmission_slice(
                        &mut frames,
                        self.stream_id,
                        chunk,
                        start,
                        end,
                        byte_limit,
                        &mut emitted_bytes,
                    ) {
                        return frames;
                    }
                }
                current_index += 1;
            }
        }
        frames
    }
}

fn unacked_chunk_slices(chunk: &SentChunk, ranges: &[OffsetRange]) -> Vec<SentChunk> {
    let chunk_start = chunk.offset;
    let chunk_end = chunk.offset.saturating_add(chunk.payload.len() as u64);
    let mut cursor = chunk_start;
    let mut pieces = Vec::new();
    for range in ranges {
        if range.end <= cursor {
            continue;
        }
        if range.start >= chunk_end {
            break;
        }
        if range.start > cursor {
            push_sent_chunk_slice(&mut pieces, chunk, cursor, range.start.min(chunk_end));
        }
        cursor = cursor.max(range.end.min(chunk_end));
        if cursor >= chunk_end {
            break;
        }
    }
    if cursor < chunk_end {
        push_sent_chunk_slice(&mut pieces, chunk, cursor, chunk_end);
    }
    pieces
}

fn push_sent_chunk_slice(pieces: &mut Vec<SentChunk>, chunk: &SentChunk, start: u64, end: u64) {
    if start >= end {
        return;
    }
    let slice_start = usize::try_from(start.saturating_sub(chunk.offset)).unwrap_or(usize::MAX);
    let slice_end = usize::try_from(end.saturating_sub(chunk.offset)).unwrap_or(usize::MAX);
    let slice_end = slice_end.min(chunk.payload.len());
    if slice_start >= slice_end {
        return;
    }
    let full_chunk = slice_start == 0 && slice_end == chunk.payload.len();
    pieces.push(SentChunk {
        offset: start,
        payload: chunk.payload.slice(slice_start..slice_end),
        flags: if full_chunk {
            chunk.flags
        } else {
            StreamFlags::NONE
        },
    });
}

fn push_retransmission_slice(
    frames: &mut Vec<Frame>,
    stream_id: StreamId,
    chunk: &SentChunk,
    start: u64,
    end: u64,
    byte_limit: usize,
    emitted_bytes: &mut usize,
) -> bool {
    if start >= end || *emitted_bytes >= byte_limit {
        return false;
    }
    let chunk_end = chunk.offset.saturating_add(chunk.payload.len() as u64);
    let start = start.max(chunk.offset).min(chunk_end);
    let end = end.max(start).min(chunk_end);
    let available = usize::try_from(end.saturating_sub(start)).unwrap_or(usize::MAX);
    let remaining = byte_limit.saturating_sub(*emitted_bytes);
    let len = available.min(remaining);
    if len == 0 {
        return false;
    }
    let slice_start = usize::try_from(start.saturating_sub(chunk.offset)).unwrap_or(usize::MAX);
    let slice_end = slice_start.saturating_add(len).min(chunk.payload.len());
    if slice_start >= slice_end {
        return false;
    }
    let full_chunk = slice_start == 0 && slice_end == chunk.payload.len();
    frames.push(Frame::StreamData {
        stream_id,
        offset: start,
        flags: if full_chunk {
            chunk.flags
        } else {
            StreamFlags::NONE
        },
        payload: chunk.payload.slice(slice_start..slice_end),
    });
    *emitted_bytes = (*emitted_bytes).saturating_add(slice_end - slice_start);
    *emitted_bytes < byte_limit
}

fn normalized_offset_ranges(ranges: &[OffsetRange]) -> Vec<OffsetRange> {
    let mut ranges = ranges.to_vec();
    ranges.sort_unstable_by_key(|range| (range.start, range.end));
    let mut merged: Vec<OffsetRange> = Vec::with_capacity(ranges.len());
    for range in ranges {
        if range.start >= range.end {
            continue;
        }
        match merged.last_mut() {
            Some(previous) if previous.end >= range.start => {
                previous.end = previous.end.max(range.end);
            }
            _ => merged.push(range),
        }
    }
    merged
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SentChunk {
    offset: u64,
    payload: Bytes,
    flags: StreamFlags,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AckOutcome {
    pub released_bytes: usize,
    pub released_chunks: usize,
    pub remaining_repair_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReliableRecvStream {
    stream_id: StreamId,
    next_offset: u64,
    reorder_bytes: usize,
    buffered: BTreeMap<u64, RecvChunk>,
    received_ranges: RangeSet,
    limits: MuxLimits,
}

impl ReliableRecvStream {
    pub fn new(stream_id: StreamId, limits: MuxLimits) -> Self {
        Self {
            stream_id,
            next_offset: 0,
            reorder_bytes: 0,
            buffered: BTreeMap::new(),
            received_ranges: RangeSet::default(),
            limits,
        }
    }

    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    pub fn reorder_bytes(&self) -> usize {
        self.reorder_bytes
    }

    pub fn receive_data(
        &mut self,
        offset: u64,
        payload: Bytes,
        flags: StreamFlags,
    ) -> Result<ReceiveOutcome, StreamError> {
        if payload.is_empty() {
            return Err(StreamError::EmptyPayload);
        }
        if payload.len() > self.limits.max_payload_bytes {
            return Err(StreamError::PayloadTooLarge {
                actual: payload.len(),
                limit: self.limits.max_payload_bytes,
            });
        }
        let end = offset
            .checked_add(payload.len() as u64)
            .ok_or(StreamError::OffsetOverflow)?;
        if self.received_ranges.covers(offset, end) {
            return Ok(ReceiveOutcome::default());
        }
        let missing_ranges = self.received_ranges.uncovered_ranges(offset, end);
        if missing_ranges.is_empty() {
            return Ok(ReceiveOutcome::default());
        }
        let missing_bytes = missing_ranges
            .iter()
            .map(|range| usize::try_from(range.len()).unwrap_or(usize::MAX))
            .try_fold(0usize, |total, len| total.checked_add(len))
            .ok_or(StreamError::ReorderBufferFull {
                actual: usize::MAX,
                limit: self.limits.max_reorder_bytes,
            })?;
        let new_reorder_bytes = self.reorder_bytes.checked_add(missing_bytes).ok_or(
            StreamError::ReorderBufferFull {
                actual: usize::MAX,
                limit: self.limits.max_reorder_bytes,
            },
        )?;
        if new_reorder_bytes > self.limits.max_reorder_bytes {
            return Err(StreamError::ReorderBufferFull {
                actual: new_reorder_bytes,
                limit: self.limits.max_reorder_bytes,
            });
        }

        let mut received_ranges = self.received_ranges.clone();
        received_ranges.insert(offset, end);
        self.reorder_bytes = new_reorder_bytes;
        self.received_ranges = received_ranges;
        for range in missing_ranges {
            let start = usize::try_from(range.start.saturating_sub(offset)).unwrap_or(usize::MAX);
            let stop = usize::try_from(range.end.saturating_sub(offset)).unwrap_or(usize::MAX);
            let stop = stop.min(payload.len());
            if start >= stop {
                continue;
            }
            self.buffered.insert(
                range.start,
                RecvChunk {
                    payload: payload.slice(start..stop),
                    flags: if flags.fin && range.end == end {
                        flags
                    } else {
                        StreamFlags::NONE
                    },
                },
            );
        }

        let mut delivered = Vec::new();
        let mut fin = false;
        while let Some(chunk) = self.buffered.remove(&self.next_offset) {
            self.reorder_bytes = self.reorder_bytes.saturating_sub(chunk.payload.len());
            self.next_offset = self.next_offset.saturating_add(chunk.payload.len() as u64);
            fin |= chunk.flags.fin;
            delivered.push(chunk.payload);
        }

        Ok(ReceiveOutcome { delivered, fin })
    }

    pub fn ack_ranges(&self) -> Vec<OffsetRange> {
        self.received_ranges.ranges()
    }

    pub fn ack_ranges_limited(&self, limit: usize) -> Vec<OffsetRange> {
        if limit == 0 {
            return Vec::new();
        }
        let mut limited = self.received_ranges.ranges();
        limited.truncate(limit);
        limited
    }

    pub fn ack_frame(&self) -> Frame {
        let mut ranges = self.ack_ranges();
        ranges.truncate(self.limits.max_ack_ranges);
        Frame::StreamAck {
            stream_id: self.stream_id,
            complete: true,
            ranges,
        }
    }

    pub fn ack_frames(&self) -> Vec<Frame> {
        let ranges = self.ack_ranges();
        let chunk_size = self.limits.max_ack_ranges.max(1);
        if ranges.is_empty() {
            return vec![Frame::StreamAck {
                stream_id: self.stream_id,
                complete: true,
                ranges,
            }];
        }
        let complete = ranges.len() <= chunk_size;
        ranges
            .chunks(chunk_size)
            .map(|chunk| Frame::StreamAck {
                stream_id: self.stream_id,
                complete,
                ranges: chunk.to_vec(),
            })
            .collect()
    }

    pub fn max_data_offset(&self) -> u64 {
        self.next_offset
            .saturating_add(self.limits.max_stream_window_bytes)
    }

    pub fn max_data_frame(&self) -> Frame {
        Frame::StreamMaxData {
            stream_id: self.stream_id,
            max_offset: self.max_data_offset(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RecvChunk {
    payload: Bytes,
    flags: StreamFlags,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReceiveOutcome {
    pub delivered: Vec<Bytes>,
    pub fin: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct RangeSet {
    ranges: BTreeMap<u64, u64>,
}

impl RangeSet {
    fn insert(&mut self, start: u64, end: u64) {
        if start >= end {
            return;
        }

        let mut merged_start = start;
        let mut merged_end = end;

        if let Some((&prev_start, &prev_end)) = self.ranges.range(..=start).next_back()
            && prev_end >= start
        {
            merged_start = prev_start;
            merged_end = merged_end.max(prev_end);
            self.ranges.remove(&prev_start);
        }

        let overlapping = self
            .ranges
            .range(start..)
            .map(|(&range_start, &range_end)| (range_start, range_end))
            .take_while(|(range_start, _)| *range_start <= merged_end)
            .collect::<Vec<_>>();

        for (range_start, range_end) in overlapping {
            merged_end = merged_end.max(range_end);
            self.ranges.remove(&range_start);
        }

        self.ranges.insert(merged_start, merged_end);
    }

    fn covers(&self, start: u64, end: u64) -> bool {
        self.ranges
            .range(..=start)
            .next_back()
            .is_some_and(|(_, range_end)| *range_end >= end)
    }

    fn uncovered_ranges(&self, start: u64, end: u64) -> Vec<OffsetRange> {
        if start >= end {
            return Vec::new();
        }
        let mut cursor = start;
        let mut ranges = Vec::new();
        if let Some((_, range_end)) = self.ranges.range(..=start).next_back()
            && *range_end > cursor
        {
            cursor = (*range_end).min(end);
        }
        for (&range_start, &range_end) in self.ranges.range(start..) {
            if cursor >= end || range_start >= end {
                break;
            }
            if range_start > cursor
                && let Some(range) = OffsetRange::new(cursor, range_start.min(end))
            {
                ranges.push(range);
            }
            cursor = cursor.max(range_end.min(end));
        }
        if cursor < end
            && let Some(range) = OffsetRange::new(cursor, end)
        {
            ranges.push(range);
        }
        ranges
    }

    fn ranges(&self) -> Vec<OffsetRange> {
        self.ranges
            .iter()
            .filter_map(|(&start, &end)| OffsetRange::new(start, end))
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamError {
    EmptyPayload,
    PayloadTooLarge { actual: usize, limit: usize },
    FlowControlBlocked { end: u64, max: u64 },
    RepairCacheFull { actual: usize, limit: usize },
    ReorderBufferFull { actual: usize, limit: usize },
    TooManyAckRanges { actual: usize, limit: usize },
    OverlappingData,
    OffsetOverflow,
}

impl std::fmt::Display for StreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyPayload => write!(f, "stream data payload must not be empty"),
            Self::PayloadTooLarge { actual, limit } => {
                write!(f, "stream payload is {actual} bytes, limit is {limit}")
            }
            Self::FlowControlBlocked { end, max } => {
                write!(f, "stream offset {end} exceeds max data {max}")
            }
            Self::RepairCacheFull { actual, limit } => {
                write!(
                    f,
                    "repair cache would hold {actual} bytes, limit is {limit}"
                )
            }
            Self::ReorderBufferFull { actual, limit } => {
                write!(
                    f,
                    "reorder buffer would hold {actual} bytes, limit is {limit}"
                )
            }
            Self::TooManyAckRanges { actual, limit } => {
                write!(f, "stream has {actual} ACK ranges, limit is {limit}")
            }
            Self::OverlappingData => write!(f, "stream data overlaps an existing range"),
            Self::OffsetOverflow => write!(f, "stream offset overflow"),
        }
    }
}

impl std::error::Error for StreamError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> MuxLimits {
        MuxLimits {
            max_payload_bytes: 1024,
            max_ack_ranges: 8,
            max_streams: 1024,
            max_stream_window_bytes: 4096,
            max_repair_bytes: 2048,
            max_reorder_bytes: 2048,
            max_datagram_queue_bytes: 2048,
            max_tcp_path_inflight_bytes: 2048,
            max_tcp_relay_chunk_bytes: 1024,
            tcp_path_heartbeat_interval: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_INTERVAL,
            tcp_path_heartbeat_timeout: crate::config::DEFAULT_TCP_PATH_HEARTBEAT_TIMEOUT,
        }
    }

    #[test]
    fn send_stream_keeps_data_until_tunnel_ack() {
        let mut stream = ReliableSendStream::new(StreamId(1), limits());
        stream
            .send_data(Bytes::from_static(b"hello"), StreamFlags::NONE)
            .expect("send");

        assert_eq!(stream.repair_bytes(), 5);

        let outcome = stream.apply_ack(&[OffsetRange::new(0, 5).expect("range")]);
        assert_eq!(
            outcome,
            AckOutcome {
                released_bytes: 5,
                released_chunks: 1,
                remaining_repair_bytes: 0
            }
        );
        assert_eq!(stream.repair_bytes(), 0);
    }

    #[test]
    fn send_stream_trims_repair_cache_by_ack_subranges() {
        let mut stream = ReliableSendStream::new(StreamId(2), limits());
        stream
            .send_data(Bytes::from_static(b"abcdefgh"), StreamFlags::NONE)
            .expect("send");

        let outcome = stream.apply_ack(&[OffsetRange::new(2, 6).expect("range")]);
        assert_eq!(
            outcome,
            AckOutcome {
                released_bytes: 4,
                released_chunks: 0,
                remaining_repair_bytes: 4,
            }
        );

        let frames =
            stream.retransmission_frames_for_ranges(&[OffsetRange { start: 0, end: 8 }], 8);
        assert_eq!(frames.len(), 2);
        assert!(matches!(
            &frames[0],
            Frame::StreamData { offset: 0, flags, payload, .. }
                if *flags == StreamFlags::NONE && payload.as_ref() == b"ab"
        ));
        assert!(matches!(
            &frames[1],
            Frame::StreamData { offset: 6, flags, payload, .. }
                if *flags == StreamFlags::NONE && payload.as_ref() == b"gh"
        ));
    }

    #[test]
    fn send_stream_retransmits_ack_range_holes_before_later_inflight() {
        let mut stream = ReliableSendStream::new(StreamId(7), limits());
        for payload in [b"aaaa", b"bbbb", b"cccc", b"dddd"] {
            stream
                .send_data(Bytes::copy_from_slice(payload), StreamFlags::NONE)
                .expect("send");
        }

        let frames = stream.retransmission_frames_for_ack_gaps(
            &[
                OffsetRange { start: 0, end: 4 },
                OffsetRange { start: 8, end: 12 },
            ],
            1024,
        );

        assert_eq!(frames.len(), 1);
        assert!(matches!(
            &frames[0],
            Frame::StreamData { offset: 4, payload, .. } if payload.as_ref() == b"bbbb"
        ));
        assert!(
            stream
                .retransmission_frames_for_ack_gaps(&[OffsetRange { start: 0, end: 4 }], 1024)
                .is_empty()
        );
    }

    #[test]
    fn send_stream_slices_ack_gap_repairs_to_byte_limit() {
        let mut stream = ReliableSendStream::new(StreamId(8), limits());
        stream
            .send_data(Bytes::from_static(b"abcdefgh"), StreamFlags::NONE)
            .expect("send");

        let frames = stream.retransmission_frames_for_ack_gaps(
            &[
                OffsetRange { start: 0, end: 2 },
                OffsetRange { start: 6, end: 8 },
            ],
            3,
        );

        assert_eq!(frames.len(), 1);
        assert!(matches!(
            &frames[0],
            Frame::StreamData { offset: 2, flags, payload, .. }
                if *flags == StreamFlags::NONE && payload.as_ref() == b"cde"
        ));
    }

    #[test]
    fn send_stream_slices_path_failure_repairs_to_byte_limit() {
        let mut stream = ReliableSendStream::new(StreamId(9), limits());
        stream
            .send_data(Bytes::from_static(b"abcdefgh"), StreamFlags::NONE)
            .expect("send");

        let frames =
            stream.retransmission_frames_for_ranges(&[OffsetRange { start: 2, end: 7 }], 4);

        assert_eq!(frames.len(), 1);
        assert!(matches!(
            &frames[0],
            Frame::StreamData { offset: 2, flags, payload, .. }
                if *flags == StreamFlags::NONE && payload.as_ref() == b"cdef"
        ));
    }

    #[test]
    fn send_stream_repairs_tail_after_ack_frontier() {
        let mut stream = ReliableSendStream::new(StreamId(10), limits());
        for payload in [b"aaaa", b"bbbb", b"cccc"] {
            stream
                .send_data(Bytes::copy_from_slice(payload), StreamFlags::NONE)
                .expect("send");
        }

        let frames =
            stream.retransmission_frames_after_ack_frontier(&[OffsetRange { start: 0, end: 4 }], 6);

        assert_eq!(frames.len(), 2);
        assert!(matches!(
            &frames[0],
            Frame::StreamData { offset: 4, payload, .. } if payload.as_ref() == b"bbbb"
        ));
        assert!(matches!(
            &frames[1],
            Frame::StreamData { offset: 8, payload, .. } if payload.as_ref() == b"cc"
        ));
    }

    #[test]
    fn send_stream_enforces_flow_control_and_repair_limit() {
        let mut limit = limits();
        limit.max_stream_window_bytes = 4;
        let mut stream = ReliableSendStream::new(StreamId(1), limit);

        assert!(matches!(
            stream.send_data(Bytes::from_static(b"hello"), StreamFlags::NONE),
            Err(StreamError::FlowControlBlocked { .. })
        ));

        let mut limit = limits();
        limit.max_repair_bytes = 4;
        let mut stream = ReliableSendStream::new(StreamId(1), limit);
        assert!(matches!(
            stream.send_data(Bytes::from_static(b"hello"), StreamFlags::NONE),
            Err(StreamError::RepairCacheFull { .. })
        ));
    }

    #[test]
    fn recv_stream_reassembles_out_of_order_data_and_builds_ack_ranges() {
        let mut stream = ReliableRecvStream::new(StreamId(7), limits());
        assert_eq!(stream.max_data_offset(), limits().max_stream_window_bytes);
        let first = stream
            .receive_data(
                5,
                Bytes::from_static(b" world"),
                StreamFlags {
                    fin: true,
                    early_data: false,
                },
            )
            .expect("second chunk");
        assert!(first.delivered.is_empty());
        assert_eq!(
            stream.ack_ranges(),
            vec![OffsetRange::new(5, 11).expect("range")]
        );

        let second = stream
            .receive_data(0, Bytes::from_static(b"hello"), StreamFlags::NONE)
            .expect("first chunk");

        assert_eq!(
            second.delivered,
            vec![Bytes::from_static(b"hello"), Bytes::from_static(b" world")]
        );
        assert!(second.fin);
        assert_eq!(stream.next_offset(), 11);
        assert_eq!(stream.reorder_bytes(), 0);
        assert_eq!(
            stream.ack_ranges(),
            vec![OffsetRange::new(0, 11).expect("range")]
        );
        assert_eq!(
            stream.ack_frame(),
            Frame::StreamAck {
                stream_id: StreamId(7),
                complete: true,
                ranges: vec![OffsetRange::new(0, 11).expect("range")]
            }
        );
        assert_eq!(
            stream.max_data_offset(),
            11 + limits().max_stream_window_bytes
        );
        assert_eq!(
            stream.max_data_frame(),
            Frame::StreamMaxData {
                stream_id: StreamId(7),
                max_offset: 11 + limits().max_stream_window_bytes
            }
        );
    }

    #[test]
    fn recv_stream_limits_encoded_ack_ranges_without_rejecting_reordering() {
        let mut limit = limits();
        limit.max_ack_ranges = 4;
        let mut stream = ReliableRecvStream::new(StreamId(7), limit);

        stream
            .receive_data(0, Bytes::from_static(b"a"), StreamFlags::NONE)
            .expect("first chunk");
        stream
            .receive_data(10, Bytes::from_static(b"b"), StreamFlags::NONE)
            .expect("second range");
        stream
            .receive_data(20, Bytes::from_static(b"c"), StreamFlags::NONE)
            .expect("third range");
        stream
            .receive_data(30, Bytes::from_static(b"d"), StreamFlags::NONE)
            .expect("fourth range");
        stream
            .receive_data(40, Bytes::from_static(b"e"), StreamFlags::NONE)
            .expect("fifth range");
        stream
            .receive_data(50, Bytes::from_static(b"f"), StreamFlags::NONE)
            .expect("sixth range");

        assert_eq!(stream.ack_ranges().len(), 6);
        assert_eq!(
            stream.ack_frame(),
            Frame::StreamAck {
                stream_id: StreamId(7),
                complete: true,
                ranges: vec![
                    OffsetRange::new(0, 1).expect("contiguous range"),
                    OffsetRange::new(10, 11).expect("first repair-adjacent range"),
                    OffsetRange::new(20, 21).expect("third range"),
                    OffsetRange::new(30, 31).expect("fourth range"),
                ]
            }
        );
    }

    #[test]
    fn recv_stream_splits_large_ack_sets_into_bounded_frames() {
        let mut limit = limits();
        limit.max_ack_ranges = 2;
        let mut stream = ReliableRecvStream::new(StreamId(7), limit);

        for offset in [0, 10, 20, 30, 40] {
            stream
                .receive_data(offset, Bytes::from_static(b"x"), StreamFlags::NONE)
                .expect("range");
        }

        assert_eq!(
            stream.ack_frames(),
            vec![
                Frame::StreamAck {
                    stream_id: StreamId(7),
                    complete: false,
                    ranges: vec![
                        OffsetRange::new(0, 1).expect("first"),
                        OffsetRange::new(10, 11).expect("second"),
                    ],
                },
                Frame::StreamAck {
                    stream_id: StreamId(7),
                    complete: false,
                    ranges: vec![
                        OffsetRange::new(20, 21).expect("third"),
                        OffsetRange::new(30, 31).expect("fourth"),
                    ],
                },
                Frame::StreamAck {
                    stream_id: StreamId(7),
                    complete: false,
                    ranges: vec![OffsetRange::new(40, 41).expect("fifth")],
                },
            ]
        );
    }

    #[test]
    fn recv_stream_accepts_duplicate_overlap_and_rejects_reorder_pressure() {
        let mut stream = ReliableRecvStream::new(StreamId(7), limits());
        stream
            .receive_data(0, Bytes::from_static(b"hello"), StreamFlags::NONE)
            .expect("first");
        assert_eq!(
            stream.receive_data(2, Bytes::from_static(b"xx"), StreamFlags::NONE),
            Ok(ReceiveOutcome::default())
        );

        let mut stream = ReliableRecvStream::new(StreamId(8), limits());
        stream
            .receive_data(5, Bytes::from_static(b"world"), StreamFlags::NONE)
            .expect("out of order tail");
        let outcome = stream
            .receive_data(0, Bytes::from_static(b"hello w"), StreamFlags::NONE)
            .expect("partially overlapping lower range");
        assert_eq!(
            outcome.delivered,
            vec![Bytes::from_static(b"hello"), Bytes::from_static(b"world")]
        );
        assert_eq!(stream.next_offset(), 10);
        assert_eq!(stream.reorder_bytes(), 0);
        assert_eq!(
            stream.receive_data(3, Bytes::from_static(b"lo wo"), StreamFlags::NONE),
            Ok(ReceiveOutcome::default())
        );

        let mut limit = limits();
        limit.max_reorder_bytes = 4;
        let mut stream = ReliableRecvStream::new(StreamId(9), limit);
        assert!(matches!(
            stream.receive_data(10, Bytes::from_static(b"hello"), StreamFlags::NONE),
            Err(StreamError::ReorderBufferFull { .. })
        ));
    }
}
