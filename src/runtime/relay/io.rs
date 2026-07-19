use crate::model::path::RelayPathInstance;
use crate::model::timing::reliable_path_stale_interval;
use crate::mux::stream::{ReliableRecvStream, ReliableSendStream};
use crate::protocol::frame::normalize_offset_ranges;
use crate::protocol::{Frame, OffsetRange};
use crate::runtime::error::RuntimeError;
use crate::scheduler::PathSnapshot;
use bytes::Bytes;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

// Relay I/O orchestrates reads, writes, and feedback timing. It observes queue
// counters but delegates product admission limits to their policy modules.

pub(in crate::runtime) fn stream_ack_gap_reinjection_allowed(
    complete: bool,
    has_multipath_reinjection_alternative: bool,
    ack_gap_reinjection_ready: bool,
) -> bool {
    if !complete {
        return false;
    }
    if !has_multipath_reinjection_alternative {
        return false;
    }
    ack_gap_reinjection_ready
}

pub(in crate::runtime) fn stream_ack_ranges_expose_authoritative_gap(
    complete: bool,
    ranges: &[OffsetRange],
) -> bool {
    complete
        && ranges
            .first()
            .is_some_and(|first| first.start > 0 || ranges.len() > 1)
}

pub(in crate::runtime) fn update_reinjection_authoritative_ack_snapshot(
    stored_ranges: &mut Vec<OffsetRange>,
    stored_complete: &mut bool,
    complete: bool,
    ranges: &[OffsetRange],
) {
    // An incomplete frame cannot establish the omitted ranges, but every
    // listed range is still monotonic positive ACK evidence. Once a complete
    // snapshot exists, merge later deltas so a filled gap cannot remain lost.
    if !complete && !*stored_complete {
        return;
    }
    let mut merged = if *stored_complete {
        std::mem::take(stored_ranges)
    } else {
        Vec::new()
    };
    merged.extend_from_slice(ranges);
    merged = normalize_offset_ranges(merged);
    *stored_ranges = merged;
    *stored_complete |= complete;
}

#[cfg(test)]
pub(in crate::runtime) fn stream_ack_gap_reinjection_frames(
    send_stream: &ReliableSendStream,
    ranges: &[OffsetRange],
    byte_limit: usize,
    complete: bool,
    has_multipath_reinjection_alternative: bool,
    ack_gap_reinjection_ready: bool,
) -> Vec<Frame> {
    if stream_ack_ranges_expose_authoritative_gap(complete, ranges)
        && stream_ack_gap_reinjection_allowed(
            complete,
            has_multipath_reinjection_alternative,
            ack_gap_reinjection_ready,
        )
    {
        send_stream.retransmission_frames_for_ack_gaps(ranges, byte_limit)
    } else {
        Vec::new()
    }
}

pub(in crate::runtime) fn stream_ack_gap_reinjection_frames_normalized(
    send_stream: &ReliableSendStream,
    ranges: &[OffsetRange],
    byte_limit: usize,
    complete: bool,
    has_multipath_reinjection_alternative: bool,
    ack_gap_reinjection_ready: bool,
) -> Vec<Frame> {
    if stream_ack_ranges_expose_authoritative_gap(complete, ranges)
        && stream_ack_gap_reinjection_allowed(
            complete,
            has_multipath_reinjection_alternative,
            ack_gap_reinjection_ready,
        )
    {
        send_stream.retransmission_frames_for_normalized_ack_gaps(ranges, byte_limit)
    } else {
        Vec::new()
    }
}

pub(in crate::runtime) fn stream_final_offset_tail_reinjection_frames_normalized(
    send_stream: &ReliableSendStream,
    ranges: &[OffsetRange],
    byte_limit: usize,
    final_offset_known: bool,
    final_tail_stall_ready: bool,
) -> Vec<Frame> {
    if !final_offset_known || !final_tail_stall_ready || byte_limit == 0 {
        return Vec::new();
    }
    let next_offset = send_stream.next_offset();
    let Some(largest_ack_end) = ranges.iter().map(|range| range.end).max() else {
        return send_stream.retransmission_frames_for_ranges(
            &[OffsetRange {
                start: 0,
                end: next_offset,
            }],
            byte_limit,
        );
    };
    if largest_ack_end >= next_offset {
        return Vec::new();
    }
    send_stream.retransmission_frames_after_normalized_ack_frontier(ranges, byte_limit)
}

#[derive(Debug, Default)]
pub(in crate::runtime) struct ReliableAckGapReinjectionProgress {
    first_gap_start: Option<u64>,
    recovery_deadline: Option<Instant>,
    last_reinjection_at: Option<Instant>,
}

/// Detects a path that has retained the earliest unacknowledged data sequence
/// range without making exact Data ACK progress. TCP and QUIC recovery remain
/// path-local; this state only decides when connection-level reinjection may
/// stop assigning new data to that path.
#[derive(Debug, Default)]
pub(in crate::runtime) struct ReliableRequestPathStaleness {
    candidate: Option<RelayPathInstance>,
    first_seen_at: Option<Instant>,
}

impl ReliableRequestPathStaleness {
    pub(in crate::runtime) fn stale_path(
        &mut self,
        complete: bool,
        candidate: Option<RelayPathInstance>,
        candidate_made_progress: bool,
        has_reinjection_path: bool,
        path: Option<PathSnapshot>,
    ) -> Option<RelayPathInstance> {
        self.stale_path_at(
            complete,
            candidate,
            candidate_made_progress,
            has_reinjection_path,
            reliable_path_stale_interval(candidate.map(|path| path.key.underlay), path),
            Instant::now(),
        )
    }

    fn stale_path_at(
        &mut self,
        complete: bool,
        candidate: Option<RelayPathInstance>,
        candidate_made_progress: bool,
        has_reinjection_path: bool,
        persistence: Duration,
        now: Instant,
    ) -> Option<RelayPathInstance> {
        // A partial ACK cannot invalidate an already observed complete Data
        // ACK range set. Wait for the next complete update instead.
        if !complete {
            return None;
        }
        let Some(candidate) = candidate else {
            self.clear();
            return None;
        };
        if !has_reinjection_path {
            self.clear();
            return None;
        }
        if self.candidate != Some(candidate) || candidate_made_progress {
            self.candidate = Some(candidate);
            self.first_seen_at = Some(now);
            return None;
        }
        self.first_seen_at
            .is_some_and(|first_seen_at| {
                now.saturating_duration_since(first_seen_at) >= persistence
            })
            .then_some(candidate)
    }

    fn clear(&mut self) {
        self.candidate = None;
        self.first_seen_at = None;
    }
}

impl ReliableAckGapReinjectionProgress {
    pub(in crate::runtime) fn arm_recovery_deadline(
        &mut self,
        complete: bool,
        normalized_ranges: &[OffsetRange],
        has_multipath_reinjection_alternative: bool,
        candidate: Option<Instant>,
    ) -> Option<Instant> {
        if !self.retain_gap_identity(
            complete,
            normalized_ranges,
            has_multipath_reinjection_alternative,
        ) {
            return None;
        }
        if let Some(candidate) = candidate {
            self.recovery_deadline = Some(
                self.recovery_deadline
                    .map_or(candidate, |deadline| deadline.min(candidate)),
            );
        }
        self.recovery_deadline
    }

    pub(in crate::runtime) fn reinjection_ready(
        &mut self,
        complete: bool,
        normalized_ranges: &[OffsetRange],
        has_multipath_reinjection_alternative: bool,
        measured_reinjection_ready: bool,
        retry_after: Duration,
    ) -> bool {
        self.reinjection_ready_at(
            complete,
            normalized_ranges,
            has_multipath_reinjection_alternative,
            measured_reinjection_ready,
            retry_after,
            Instant::now(),
        )
    }

    fn reinjection_ready_at(
        &mut self,
        complete: bool,
        normalized_ranges: &[OffsetRange],
        has_multipath_reinjection_alternative: bool,
        measured_reinjection_ready: bool,
        retry_after: Duration,
        now: Instant,
    ) -> bool {
        if !self.retain_gap_identity(
            complete,
            normalized_ranges,
            has_multipath_reinjection_alternative,
        ) {
            return false;
        }
        if !measured_reinjection_ready {
            return false;
        }
        if self.last_reinjection_at.is_some_and(|last_reinjection_at| {
            now.saturating_duration_since(last_reinjection_at) < retry_after
        }) {
            return false;
        }
        true
    }

    pub(in crate::runtime) fn record_reinjection_queued(&mut self) {
        self.record_reinjection_queued_at(Instant::now());
    }

    fn record_reinjection_queued_at(&mut self, now: Instant) {
        if self.first_gap_start.is_some() {
            self.last_reinjection_at = Some(now);
        }
    }

    pub(in crate::runtime) fn release_reinjection_attempt(&mut self) {
        self.last_reinjection_at = None;
    }

    pub(in crate::runtime) fn repeat_reinjection_deadline(
        &self,
        retry_after: Duration,
    ) -> Option<Instant> {
        self.last_reinjection_at
            .and_then(|attempt| attempt.checked_add(retry_after))
    }

    fn clear(&mut self) {
        self.first_gap_start = None;
        self.recovery_deadline = None;
        self.last_reinjection_at = None;
    }

    fn retain_gap_identity(
        &mut self,
        complete: bool,
        normalized_ranges: &[OffsetRange],
        has_multipath_reinjection_alternative: bool,
    ) -> bool {
        if !complete || !has_multipath_reinjection_alternative {
            self.clear();
            return false;
        }
        let Some(first_gap) = normalized_stream_ack_first_gap(normalized_ranges) else {
            self.clear();
            return false;
        };
        if self.first_gap_start != Some(first_gap.0) {
            self.first_gap_start = Some(first_gap.0);
            self.recovery_deadline = None;
            self.last_reinjection_at = None;
        }
        true
    }
}

pub(super) fn normalized_stream_ack_first_gap(
    normalized_ranges: &[OffsetRange],
) -> Option<(u64, u64)> {
    debug_assert!(
        normalized_ranges
            .windows(2)
            .all(|ranges| ranges[0].end < ranges[1].start)
    );
    if normalized_ranges.is_empty() {
        return None;
    }
    let mut cursor = 0_u64;
    for range in normalized_ranges {
        if range.end <= cursor {
            continue;
        }
        if range.start > cursor {
            return Some((cursor, range.start));
        }
        cursor = range.end;
    }
    None
}

pub(in crate::runtime) fn resize_reliable_relay_buffer(
    buffer: &mut bytes::BytesMut,
    target_len: usize,
) {
    let target_len = target_len.max(1);
    buffer.clear();
    if buffer.capacity() < target_len {
        buffer.reserve(target_len.saturating_sub(buffer.capacity()));
    }
}

pub(in crate::runtime) async fn read_reliable_relay_payload<S>(
    local: &mut S,
    buffer: &mut bytes::BytesMut,
    read_budget: usize,
) -> std::io::Result<(usize, Option<Bytes>)>
where
    S: AsyncRead + Unpin,
{
    resize_reliable_relay_buffer(buffer, read_budget);
    let read = (&mut *local)
        .take(read_budget.max(1) as u64)
        .read_buf(buffer)
        .await?;
    if read == 0 {
        Ok((0, None))
    } else {
        Ok((read, Some(buffer.split_to(read).freeze())))
    }
}

pub(in crate::runtime) async fn write_delivered_payloads<S>(
    local: &mut S,
    delivered: &[Bytes],
) -> std::io::Result<usize>
where
    S: AsyncWrite + Unpin,
{
    let total_bytes = delivered
        .iter()
        .map(|chunk| chunk.len())
        .fold(0usize, usize::saturating_add);
    match delivered {
        [] => return Ok(0),
        [single] => {
            local.write_all(single).await?;
            return Ok(total_bytes);
        }
        _ => {}
    }

    let mut chunk_index = 0usize;
    let mut chunk_offset = 0usize;
    while chunk_index < delivered.len() {
        let mut slices = smallvec::SmallVec::<[std::io::IoSlice<'_>; 8]>::new();
        if chunk_offset < delivered[chunk_index].len() {
            slices.push(std::io::IoSlice::new(
                &delivered[chunk_index][chunk_offset..],
            ));
        }
        for chunk in delivered.iter().skip(chunk_index + 1) {
            if !chunk.is_empty() {
                slices.push(std::io::IoSlice::new(chunk.as_ref()));
                if slices.len() >= 8 {
                    break;
                }
            }
        }
        if slices.is_empty() {
            break;
        }
        let mut written = local.write_vectored(&slices).await?;
        if written == 0 {
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        while written > 0 && chunk_index < delivered.len() {
            let remaining = delivered[chunk_index].len().saturating_sub(chunk_offset);
            if written < remaining {
                chunk_offset = chunk_offset.saturating_add(written);
                written = 0;
            } else {
                written = written.saturating_sub(remaining);
                chunk_index = chunk_index.saturating_add(1);
                chunk_offset = 0;
            }
        }
    }
    Ok(total_bytes)
}

pub(in crate::runtime) fn receive_stream_fin(
    recv_stream: &ReliableRecvStream,
    pending_final_offset: &mut Option<u64>,
    final_offset: u64,
) -> Result<bool, RuntimeError> {
    if final_offset < recv_stream.ack_range_summary().largest_end {
        return Err(RuntimeError::Protocol(
            "stream FIN final offset is behind received data",
        ));
    }
    if let Some(existing) = *pending_final_offset {
        if existing != final_offset {
            return Err(RuntimeError::Protocol(
                "conflicting stream FIN final offsets",
            ));
        }
    } else {
        // The FIN remains pending until its receive progress is published and
        // the local half-close commits, including across carrier reattachment.
        *pending_final_offset = Some(final_offset);
    }
    Ok(final_offset == recv_stream.next_offset())
}

pub(in crate::runtime) fn validate_stream_data_final_offset(
    pending_final_offset: Option<u64>,
    offset: u64,
    payload_len: usize,
) -> Result<(), RuntimeError> {
    let Some(final_offset) = pending_final_offset else {
        return Ok(());
    };
    let payload_len = u64::try_from(payload_len)
        .map_err(|_| RuntimeError::Protocol("stream data length exceeds offset space"))?;
    let end = offset
        .checked_add(payload_len)
        .ok_or(RuntimeError::Protocol("stream data range overflows"))?;
    if end > final_offset {
        return Err(RuntimeError::Protocol(
            "stream data exceeds declared final offset",
        ));
    }
    Ok(())
}

pub(in crate::runtime) fn stream_data_range_already_delivered(
    recv_stream: &ReliableRecvStream,
    offset: u64,
    payload_len: usize,
) -> bool {
    offset.saturating_add(payload_len as u64) <= recv_stream.next_offset()
}

pub(in crate::runtime) fn pending_stream_fin_ready(
    recv_stream: &ReliableRecvStream,
    pending_final_offset: Option<u64>,
) -> bool {
    pending_final_offset.is_some_and(|final_offset| recv_stream.next_offset() == final_offset)
}

pub(in crate::runtime) fn stream_terminal_fin_replay_required(
    fin_sent: bool,
    fin_replayed: bool,
    sender_queue_empty: bool,
) -> bool {
    fin_sent && !fin_replayed && sender_queue_empty
}

#[cfg(test)]
#[path = "io_test.rs"]
mod tests;
