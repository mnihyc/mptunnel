use super::error::UdpCarrierFrameError;
use super::stream::{RecvStream, SendStream};
use bytes::{Bytes, BytesMut};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

pub(super) const UNORDERED_DEDUP_WINDOW: usize = 4096;
pub(super) const CLOSED_STREAM_DEDUP_WINDOW: usize = 8192;

#[derive(Debug)]
pub(super) struct StreamState {
    pub(super) frames: mpsc::Sender<Bytes>,
    pub(super) assemblies: BTreeMap<FrameKey, FrameAssembly>,
    pub(super) completed: BTreeMap<u64, Bytes>,
    pub(super) next_frame_id: u64,
    delivered_unordered: HashSet<u64>,
    delivered_unordered_order: VecDeque<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct FrameKey {
    pub(super) ordered: bool,
    pub(super) frame_id: u64,
}

#[derive(Debug)]
pub(super) struct OrphanFragment {
    pub(super) frame_id: u64,
    pub(super) offset: u32,
    pub(super) total_len: usize,
    pub(super) payload: Bytes,
    received_at: Instant,
}

impl OrphanFragment {
    pub(super) fn new(frame_id: u64, offset: u32, total_len: usize, payload: Bytes) -> Self {
        Self {
            frame_id,
            offset,
            total_len,
            payload,
            received_at: Instant::now(),
        }
    }

    fn bytes(&self) -> usize {
        self.payload.len()
    }
}

#[derive(Debug, Default)]
pub(super) struct OrphanFragmentBuffer {
    streams: BTreeMap<u64, VecDeque<OrphanFragment>>,
    bytes: usize,
}

impl OrphanFragmentBuffer {
    pub(super) fn store(
        &mut self,
        stream_id: u64,
        fragment: OrphanFragment,
        byte_limit: usize,
        now: Instant,
        ttl: Duration,
    ) -> bool {
        self.expire(now, ttl);
        let fragment_bytes = fragment.bytes();
        if fragment_bytes > byte_limit {
            return false;
        }
        while self.bytes.saturating_add(fragment_bytes) > byte_limit {
            if !self.evict_oldest() {
                return false;
            }
        }
        self.bytes = self.bytes.saturating_add(fragment_bytes);
        self.streams
            .entry(stream_id)
            .or_default()
            .push_back(fragment);
        true
    }

    pub(super) fn drain(
        &mut self,
        stream_id: u64,
        now: Instant,
        ttl: Duration,
    ) -> Vec<OrphanFragment> {
        self.expire(now, ttl);
        let Some(mut fragments) = self.streams.remove(&stream_id) else {
            return Vec::new();
        };
        let mut drained = Vec::with_capacity(fragments.len());
        while let Some(fragment) = fragments.pop_front() {
            self.bytes = self.bytes.saturating_sub(fragment.bytes());
            drained.push(fragment);
        }
        drained
    }

    #[cfg(test)]
    pub(super) fn bytes(&self) -> usize {
        self.bytes
    }

    fn expire(&mut self, now: Instant, ttl: Duration) {
        let stream_ids = self.streams.keys().copied().collect::<Vec<_>>();
        for stream_id in stream_ids {
            let mut empty = false;
            if let Some(fragments) = self.streams.get_mut(&stream_id) {
                while fragments.front().is_some_and(|fragment| {
                    now.saturating_duration_since(fragment.received_at) > ttl
                }) {
                    if let Some(fragment) = fragments.pop_front() {
                        self.bytes = self.bytes.saturating_sub(fragment.bytes());
                    }
                }
                empty = fragments.is_empty();
            }
            if empty {
                self.streams.remove(&stream_id);
            }
        }
    }

    fn evict_oldest(&mut self) -> bool {
        let oldest = self
            .streams
            .iter()
            .filter_map(|(stream_id, fragments)| {
                fragments
                    .front()
                    .map(|fragment| (*stream_id, fragment.received_at))
            })
            .min_by_key(|(_, received_at)| *received_at)
            .map(|(stream_id, _)| stream_id);
        let Some(stream_id) = oldest else {
            return false;
        };
        if let Some(fragments) = self.streams.get_mut(&stream_id) {
            if let Some(fragment) = fragments.pop_front() {
                self.bytes = self.bytes.saturating_sub(fragment.bytes());
            }
            if fragments.is_empty() {
                self.streams.remove(&stream_id);
            }
            return true;
        }
        false
    }
}

#[derive(Debug, Default)]
pub(super) struct ClosedStreamCache {
    set: HashSet<u64>,
    order: VecDeque<u64>,
}

impl ClosedStreamCache {
    pub(super) fn contains(&self, stream_id: u64) -> bool {
        self.set.contains(&stream_id)
    }

    pub(super) fn remember(&mut self, stream_id: u64) {
        if self.set.insert(stream_id) {
            self.order.push_back(stream_id);
            while self.order.len() > CLOSED_STREAM_DEDUP_WINDOW {
                if let Some(oldest) = self.order.pop_front() {
                    self.set.remove(&oldest);
                }
            }
        }
    }
}

impl StreamState {
    pub(super) fn new(frames: mpsc::Sender<Bytes>) -> Self {
        Self {
            frames,
            assemblies: BTreeMap::new(),
            completed: BTreeMap::new(),
            next_frame_id: 0,
            delivered_unordered: HashSet::new(),
            delivered_unordered_order: VecDeque::new(),
        }
    }

    pub(super) fn should_ignore_fragment(&self, ordered: bool, frame_id: u64) -> bool {
        if ordered {
            frame_id < self.next_frame_id
        } else {
            self.delivered_unordered.contains(&frame_id)
        }
    }

    pub(super) fn remember_unordered_delivery(&mut self, frame_id: u64) {
        if self.delivered_unordered.insert(frame_id) {
            self.delivered_unordered_order.push_back(frame_id);
            while self.delivered_unordered_order.len() > UNORDERED_DEDUP_WINDOW {
                if let Some(oldest) = self.delivered_unordered_order.pop_front() {
                    self.delivered_unordered.remove(&oldest);
                }
            }
        }
    }
}

#[derive(Debug)]
pub(super) struct FrameAssembly {
    pub(super) total_len: usize,
    received_bytes: usize,
    buffer: BytesMut,
    ranges: Vec<(usize, usize)>,
}

impl FrameAssembly {
    pub(super) fn new(total_len: usize) -> Self {
        Self {
            total_len,
            received_bytes: 0,
            buffer: BytesMut::zeroed(total_len),
            ranges: Vec::new(),
        }
    }

    pub(super) fn insert(
        &mut self,
        offset: u32,
        total_len: usize,
        payload: Bytes,
    ) -> Result<Option<Bytes>, UdpCarrierFrameError> {
        if total_len != self.total_len {
            return Err(UdpCarrierFrameError::InvalidPacket(
                "fragment total length changed",
            ));
        }
        let offset_usize = usize::try_from(offset)
            .map_err(|_| UdpCarrierFrameError::InvalidPacket("fragment offset overflow"))?;
        let end =
            offset_usize
                .checked_add(payload.len())
                .ok_or(UdpCarrierFrameError::InvalidPacket(
                    "fragment range overflow",
                ))?;
        if end > self.total_len {
            return Err(UdpCarrierFrameError::InvalidPacket(
                "fragment exceeds frame length",
            ));
        }
        if self
            .ranges
            .iter()
            .any(|(start, existing_end)| offset_usize < *existing_end && end > *start)
        {
            return Ok(None);
        }
        self.received_bytes = self.received_bytes.saturating_add(payload.len());
        self.buffer[offset_usize..end].copy_from_slice(&payload);
        self.ranges.push((offset_usize, end));
        if self.received_bytes < self.total_len {
            return Ok(None);
        }
        self.ranges.sort_unstable_by_key(|(start, _)| *start);
        let mut cursor = 0usize;
        for (start, end) in &self.ranges {
            if *start != cursor {
                return Ok(None);
            }
            cursor = *end;
        }
        if cursor == self.total_len {
            Ok(Some(std::mem::take(&mut self.buffer).freeze()))
        } else {
            Ok(None)
        }
    }
}

pub(super) fn new_stream_pair(
    stream_id: u64,
    commands: tokio::sync::mpsc::Sender<super::stream::StreamCommand>,
    frame_queue: usize,
) -> (StreamState, (SendStream, RecvStream)) {
    let (frames_tx, frames_rx) = mpsc::channel(frame_queue);
    (
        StreamState::new(frames_tx),
        (
            SendStream::new(stream_id, commands),
            RecvStream::new(frames_rx),
        ),
    )
}
