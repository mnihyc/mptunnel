use crate::mux::MuxLimits;
use crate::protocol::frame::normalized_offset_ranges;
use crate::protocol::{Frame, OffsetRange, StreamId};
use bytes::Bytes;
use smallvec::SmallVec;
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
        Self::new_with_initial_max_offset(stream_id, limits, limits.max_stream_window_bytes)
    }

    /// Create a send-side product stream with explicit peer flow-control credit.
    ///
    /// Runtime relays use `initial_max_offset=0` and then apply the peer's
    /// `STREAM_MAX_DATA`/open credit before sending. Keeping the public test
    /// constructor above at the configured window avoids rewriting older unit
    /// tests, but production paths must not manufacture a 64 MiB credit before
    /// the receiver advertises it. That uncredited window was the root cause of
    /// QUIC reliable-stream burst delivery: mptunnel could queue tens of MiB
    /// above QUIC before the application receiver had created matching credit.
    pub fn new_with_initial_max_offset(
        stream_id: StreamId,
        limits: MuxLimits,
        initial_max_offset: u64,
    ) -> Self {
        Self {
            stream_id,
            next_offset: 0,
            max_offset: initial_max_offset,
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

    pub fn send_data(&mut self, payload: Bytes) -> Result<Frame, StreamError> {
        let frame = self.prepare_data(payload)?;
        self.commit_prepared_data(&frame)?;
        Ok(frame)
    }

    pub fn prepare_data(&self, payload: Bytes) -> Result<Frame, StreamError> {
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
        Ok(Frame::StreamData {
            stream_id: self.stream_id,
            offset,
            payload,
        })
    }

    pub fn commit_prepared_data(&mut self, frame: &Frame) -> Result<(), StreamError> {
        let Frame::StreamData {
            stream_id,
            offset,
            payload,
        } = frame
        else {
            return Err(StreamError::InvalidPreparedFrame);
        };
        if *stream_id != self.stream_id || *offset != self.next_offset {
            return Err(StreamError::InvalidPreparedFrame);
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
        self.next_offset = end;
        self.repair_bytes = new_repair_bytes;
        self.repair_cache.insert(
            *offset,
            SentChunk {
                offset: *offset,
                payload: payload.clone(),
            },
        );
        Ok(())
    }

    pub fn rollback_committed_data(&mut self, frame: &Frame) -> Result<(), StreamError> {
        let Frame::StreamData {
            stream_id,
            offset,
            payload,
            ..
        } = frame
        else {
            return Err(StreamError::InvalidPreparedFrame);
        };
        let end = offset
            .checked_add(payload.len() as u64)
            .ok_or(StreamError::OffsetOverflow)?;
        if *stream_id != self.stream_id || end != self.next_offset {
            return Err(StreamError::InvalidPreparedFrame);
        }
        let Some(chunk) = self.repair_cache.remove(offset) else {
            return Err(StreamError::InvalidPreparedFrame);
        };
        if chunk.payload.len() != payload.len() || chunk.offset != *offset {
            return Err(StreamError::InvalidPreparedFrame);
        }
        self.next_offset = *offset;
        self.repair_bytes = self.repair_bytes.saturating_sub(payload.len());
        Ok(())
    }

    pub fn apply_ack(&mut self, ranges: &[OffsetRange]) -> AckOutcome {
        let ranges = normalized_offset_ranges(ranges);
        self.apply_normalized_ack(&ranges)
    }

    pub fn apply_normalized_ack(&mut self, ranges: &[OffsetRange]) -> AckOutcome {
        if ranges.is_empty() || self.repair_cache.is_empty() {
            return AckOutcome {
                released_bytes: 0,
                released_chunks: 0,
                remaining_repair_bytes: self.repair_bytes,
            };
        }

        let mut released_chunks = 0usize;
        let previous_repair_bytes = self.repair_bytes;
        let mut released_bytes = 0usize;
        for range in ranges {
            while let Some(offset) =
                first_overlapping_repair_chunk(&self.repair_cache, range.start, range.end)
            {
                let Some(chunk) = self.repair_cache.remove(&offset) else {
                    break;
                };
                let chunk_start = chunk.offset;
                let chunk_end = chunk.offset.saturating_add(chunk.payload.len() as u64);
                let acked_start = chunk_start.max(range.start);
                let acked_end = chunk_end.min(range.end);
                if acked_start >= acked_end {
                    self.repair_cache.insert(chunk.offset, chunk);
                    break;
                }
                released_bytes = released_bytes.saturating_add(
                    usize::try_from(acked_end.saturating_sub(acked_start)).unwrap_or(usize::MAX),
                );
                if acked_start == chunk_start && acked_end == chunk_end {
                    released_chunks += 1;
                    continue;
                }
                if let Some(left) = sent_chunk_slice(&chunk, chunk_start, acked_start) {
                    self.repair_cache.insert(left.offset, left);
                }
                if let Some(right) = sent_chunk_slice(&chunk, acked_end, chunk_end) {
                    self.repair_cache.insert(right.offset, right);
                }
            }
        }
        self.repair_bytes = previous_repair_bytes.saturating_sub(released_bytes);
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
        self.retransmission_frames_for_normalized_ack_gaps(&ranges, byte_limit)
    }

    pub fn retransmission_frames_for_normalized_ack_gaps(
        &self,
        ranges: &[OffsetRange],
        byte_limit: usize,
    ) -> Vec<Frame> {
        if byte_limit == 0 {
            return Vec::new();
        }
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
        self.retransmission_frames_after_normalized_ack_frontier(&ranges, byte_limit)
    }

    pub fn retransmission_frames_after_normalized_ack_frontier(
        &self,
        ranges: &[OffsetRange],
        byte_limit: usize,
    ) -> Vec<Frame> {
        if byte_limit == 0 || self.repair_cache.is_empty() {
            return Vec::new();
        }
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

fn sent_chunk_slice(chunk: &SentChunk, start: u64, end: u64) -> Option<SentChunk> {
    if start >= end {
        return None;
    }
    let slice_start = usize::try_from(start.saturating_sub(chunk.offset)).unwrap_or(usize::MAX);
    let slice_end = usize::try_from(end.saturating_sub(chunk.offset)).unwrap_or(usize::MAX);
    let slice_end = slice_end.min(chunk.payload.len());
    if slice_start >= slice_end {
        return None;
    }
    Some(SentChunk {
        offset: start,
        payload: chunk.payload.slice(slice_start..slice_end),
    })
}

fn first_overlapping_repair_chunk(
    repair_cache: &BTreeMap<u64, SentChunk>,
    start: u64,
    end: u64,
) -> Option<u64> {
    if start >= end {
        return None;
    }
    if let Some((&offset, chunk)) = repair_cache.range(..=start).next_back()
        && chunk.offset.saturating_add(chunk.payload.len() as u64) > start
    {
        return Some(offset);
    }
    repair_cache
        .range(start..end)
        .next()
        .map(|(&offset, _)| offset)
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
    frames.push(Frame::StreamData {
        stream_id,
        offset: start,
        payload: chunk.payload.slice(slice_start..slice_end),
    });
    *emitted_bytes = (*emitted_bytes).saturating_add(slice_end - slice_start);
    *emitted_bytes < byte_limit
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SentChunk {
    offset: u64,
    payload: Bytes,
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
        if offset == self.next_offset
            && missing_ranges.len() == 1
            && missing_ranges[0].start == offset
            && missing_ranges[0].end == end
        {
            self.received_ranges.insert(offset, end);
            self.next_offset = end;
            let mut delivered = SmallVec::new();
            delivered.push(payload);
            while let Some(chunk) = self.buffered.remove(&self.next_offset) {
                self.reorder_bytes = self.reorder_bytes.saturating_sub(chunk.payload.len());
                self.next_offset = self.next_offset.saturating_add(chunk.payload.len() as u64);
                delivered.push(chunk.payload);
            }
            return Ok(ReceiveOutcome { delivered });
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

        self.received_ranges.insert(offset, end);
        self.reorder_bytes = new_reorder_bytes;
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
                },
            );
        }

        let mut delivered = SmallVec::new();
        while let Some(chunk) = self.buffered.remove(&self.next_offset) {
            self.reorder_bytes = self.reorder_bytes.saturating_sub(chunk.payload.len());
            self.next_offset = self.next_offset.saturating_add(chunk.payload.len() as u64);
            delivered.push(chunk.payload);
        }

        Ok(ReceiveOutcome { delivered })
    }

    pub fn ack_ranges(&self) -> Vec<OffsetRange> {
        self.received_ranges.ranges()
    }

    pub fn ack_range_summary(&self) -> AckRangeSummary {
        self.received_ranges.summary()
    }

    pub fn ack_ranges_limited(&self, limit: usize) -> Vec<OffsetRange> {
        if limit == 0 {
            return Vec::new();
        }
        self.received_ranges.ranges_limited(limit)
    }

    /// Build a single ACK frame for callers that cannot batch control frames.
    ///
    /// A single ACK frame is only a complete description when all received
    /// ranges fit in `max_ack_ranges`. If the range set has to be truncated we
    /// must mark the frame `complete=false`; otherwise the sender interprets
    /// omitted higher ranges as real holes and starts product repair. That was
    /// catastrophic for multipath/QUIC because ordinary reordering produced
    /// repair storms, extra traffic, and application stalls.
    pub fn ack_frame(&self) -> Frame {
        let all_ranges = self.ack_ranges();
        let range_limit = self.limits.max_ack_ranges.max(1);
        let complete = all_ranges.len() <= range_limit;
        let ranges = all_ranges.into_iter().take(range_limit).collect();
        Frame::StreamAck {
            stream_id: self.stream_id,
            complete,
            ranges,
        }
    }

    /// Encode newly received ranges without claiming a complete snapshot.
    /// Reliable-carrier sparse ACK deltas release exact new bytes immediately;
    /// periodic `ack_frames` snapshots retain cumulative recovery authority.
    pub fn ack_delta_frames(&self, ranges: &[OffsetRange]) -> Vec<Frame> {
        let chunk_size = self.limits.max_ack_ranges.max(1);
        ranges
            .chunks(chunk_size)
            .map(|chunk| Frame::StreamAck {
                stream_id: self.stream_id,
                complete: false,
                ranges: chunk.to_vec(),
            })
            .collect()
    }

    /// Build every ACK chunk needed to describe the current receive ranges.
    ///
    /// When multiple frames are needed each chunk is explicitly incomplete.
    /// The sender may use incomplete ACK chunks for flight release, but it must
    /// not infer stream gaps from omitted chunks.
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
        self.max_data_offset_with_window(self.limits.max_stream_window_bytes)
    }

    pub fn max_data_offset_with_window(&self, window_bytes: u64) -> u64 {
        self.next_offset.saturating_add(window_bytes.max(1))
    }

    pub fn max_data_frame(&self) -> Frame {
        self.max_data_frame_with_window(self.limits.max_stream_window_bytes)
    }

    pub fn max_data_frame_with_window(&self, window_bytes: u64) -> Frame {
        Frame::StreamMaxData {
            stream_id: self.stream_id,
            max_offset: self.max_data_offset_with_window(window_bytes),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RecvChunk {
    payload: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReceiveOutcome {
    pub delivered: SmallVec<[Bytes; 1]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AckRangeSummary {
    pub count: usize,
    pub largest_end: u64,
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

        loop {
            let next = self
                .ranges
                .range(start..=merged_end)
                .next()
                .map(|(&range_start, &range_end)| (range_start, range_end));
            let Some((range_start, range_end)) = next else {
                break;
            };
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
        self.ranges_limited(usize::MAX)
    }

    fn ranges_limited(&self, limit: usize) -> Vec<OffsetRange> {
        self.ranges
            .iter()
            .take(limit)
            .filter_map(|(&start, &end)| OffsetRange::new(start, end))
            .collect()
    }

    fn summary(&self) -> AckRangeSummary {
        AckRangeSummary {
            count: self.ranges.len(),
            largest_end: self.ranges.last_key_value().map_or(0, |(_, &end)| end),
        }
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
    InvalidPreparedFrame,
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
            Self::InvalidPreparedFrame => {
                write!(f, "prepared stream frame does not match send stream state")
            }
        }
    }
}

impl std::error::Error for StreamError {}

#[cfg(test)]
#[path = "stream_test.rs"]
mod tests;
