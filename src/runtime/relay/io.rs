use crate::model::path::RelayPathInstance;
use crate::model::timing::reliable_path_stale_interval;
use crate::mux::stream::{
    ReceiveOutcome, ReliableRecvStream, ReliableSendStream, StreamError, ValidatedStreamAck,
    validate_stream_ack,
};
use crate::protocol::frame::normalize_offset_ranges;
use crate::protocol::{Frame, OffsetRange, StreamId};
use crate::runtime::error::RuntimeError;
use crate::scheduler::PathSnapshot;
use bytes::Bytes;
use smallvec::SmallVec;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

// Relay I/O orchestrates reads, writes, and feedback timing. It observes queue
// counters but delegates product admission limits to their policy modules.

/// Starts one relay ACK transaction by freezing its send-assignment extent.
///
/// Both client and server actors call this before touching any ACK-owned state.
pub(in crate::runtime) fn begin_reliable_stream_ack(
    send_stream: &ReliableSendStream,
    complete: bool,
    ranges: Vec<OffsetRange>,
) -> Result<ValidatedStreamAck, StreamError> {
    validate_stream_ack(complete, ranges, send_stream.next_offset())
}

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

/// Monotonic negative ACK authority retained by one logical stream.
///
/// Positive ACK evidence is applied directly to cache and flight ledgers. This
/// snapshot is narrower: it authorizes recovery for omissions only through the
/// assigned DSN horizon captured by a complete ACK transaction. Incomplete
/// deltas may fill an existing authoritative gap, but cannot extend that
/// horizon to data assigned after the snapshot.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(in crate::runtime) struct AuthoritativeStreamAckSnapshot {
    ranges: Vec<OffsetRange>,
    horizon: Option<u64>,
}

impl AuthoritativeStreamAckSnapshot {
    pub(in crate::runtime) fn complete(&self) -> bool {
        self.horizon.is_some()
    }

    pub(in crate::runtime) fn ranges(&self) -> &[OffsetRange] {
        &self.ranges
    }

    pub(in crate::runtime) fn horizon(&self) -> Option<u64> {
        self.horizon
    }

    pub(in crate::runtime) fn has_unacknowledged_extent(&self, frontier: u64) -> bool {
        self.horizon.is_some_and(|horizon| frontier < horizon)
    }

    fn update(&mut self, ack: &ValidatedStreamAck) {
        if !ack.complete() && self.horizon.is_none() {
            return;
        }

        if ack.complete() {
            self.horizon = Some(self.horizon.map_or(ack.assigned_end(), |horizon| {
                horizon.max(ack.assigned_end())
            }));
        }
        let horizon = self
            .horizon
            .expect("complete ACK authority must have an assigned horizon");
        let mut merged = std::mem::take(&mut self.ranges);
        merged.extend(ack.ranges().iter().filter_map(|range| {
            let end = range.end.min(horizon);
            (range.start < end).then_some(OffsetRange {
                start: range.start,
                end,
            })
        }));
        self.ranges = normalize_offset_ranges(merged);
    }
}

pub(in crate::runtime) fn update_reinjection_authoritative_ack_snapshot(
    stored: &mut AuthoritativeStreamAckSnapshot,
    ack: &ValidatedStreamAck,
) {
    stored.update(ack);
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

/// Frames removed from one bounded relay-input queue in a single ready-only
/// turn. Payload ownership stays in the original items, so collecting a batch
/// never copies stream bytes or erases ingress-path metadata.
pub(in crate::runtime) struct ReadyStreamDataBatch<T> {
    // One frame is the normal latency-sensitive path and stays inline. A
    // stream that observes a real ready backlog grows this storage once and
    // reuses it for the rest of the relay lifetime.
    items: SmallVec<[T; 1]>,
    payload_bytes: usize,
    // Match the existing write_delivered_payloads vectored-write span. Larger
    // batches grow once per relay and retain their allocation.
    delivered: SmallVec<[Bytes; 8]>,
}

impl<T> ReadyStreamDataBatch<T> {
    pub(in crate::runtime) fn new() -> Self {
        Self {
            items: SmallVec::new(),
            payload_bytes: 0,
            delivered: SmallVec::new(),
        }
    }

    pub(in crate::runtime) fn len(&self) -> usize {
        self.items.len()
    }

    #[cfg(any(test, feature = "lab-diagnostics"))]
    pub(in crate::runtime) fn payload_bytes(&self) -> usize {
        self.payload_bytes
    }

    fn prepare_collect(&mut self) {
        debug_assert!(self.items.is_empty());
        debug_assert!(self.delivered.is_empty());
        self.payload_bytes = 0;
    }

    #[cfg(test)]
    fn item_capacity(&self) -> usize {
        self.items.capacity()
    }

    #[cfg(test)]
    fn items_spilled(&self) -> bool {
        self.items.spilled()
    }

    #[cfg(test)]
    fn delivered_spilled(&self) -> bool {
        self.delivered.spilled()
    }
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ReadyStreamDataBatchBounds {
    pub(in crate::runtime) stream_id: StreamId,
    pub(in crate::runtime) receive_frontier: u64,
    pub(in crate::runtime) receive_limit: u64,
    pub(in crate::runtime) payload_limit: usize,
    pub(in crate::runtime) ready_items: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum ReadyStreamDataDirection {
    ClientDownload,
    ServerUpload,
}

impl ReadyStreamDataDirection {
    #[cfg(feature = "lab-diagnostics")]
    fn metric(self) -> &'static str {
        match self {
            Self::ClientDownload => "relay.ready_stream_data_batch.client_download",
            Self::ServerUpload => "relay.ready_stream_data_batch.server_upload",
        }
    }

    #[cfg(feature = "lab-diagnostics")]
    fn label(self) -> &'static str {
        match self {
            Self::ClientDownload => "client_download",
            Self::ServerUpload => "server_upload",
        }
    }
}

/// Collects only the exact in-order STREAM_DATA backlog visible after the
/// first dequeue. `ready_items` must be a queue-length snapshot taken at entry.
///
/// The caller supplies a borrowed frame view so attributed client items and
/// server registry items retain their native ownership shape. The first
/// non-data or non-contiguous item is returned unchanged as an ordering
/// boundary.
pub(in crate::runtime) fn collect_ready_stream_data_batch<T, N, V>(
    batch: &mut ReadyStreamDataBatch<T>,
    first: T,
    bounds: ReadyStreamDataBatchBounds,
    mut try_next: N,
    view: V,
) -> Option<T>
where
    N: FnMut() -> Option<T>,
    V: Fn(&T) -> Option<(StreamId, u64, usize)>,
{
    let ReadyStreamDataBatchBounds {
        stream_id,
        receive_frontier,
        receive_limit,
        payload_limit,
        ready_items,
    } = bounds;
    batch.prepare_collect();
    let Some((first_stream_id, first_offset, first_payload_bytes)) = view(&first) else {
        batch.items.push(first);
        return None;
    };
    let first_end = first_offset.checked_add(first_payload_bytes as u64);
    let first_is_eligible = first_stream_id == stream_id
        && first_offset == receive_frontier
        && first_payload_bytes > 0
        && first_payload_bytes <= payload_limit
        && first_end.is_some_and(|end| end <= receive_limit);
    batch.items.push(first);
    if !first_is_eligible {
        batch.payload_bytes = first_payload_bytes;
        return None;
    }
    let mut payload_bytes = first_payload_bytes;
    let mut expected_offset = first_end.expect("eligible STREAM_DATA has an end offset");
    let mut deferred = None;

    for _ in 0..ready_items {
        let Some(next) = try_next() else {
            break;
        };
        let Some((next_stream_id, next_offset, next_payload_bytes)) = view(&next) else {
            deferred = Some(next);
            break;
        };
        let next_total = payload_bytes.checked_add(next_payload_bytes);
        let next_end = next_offset.checked_add(next_payload_bytes as u64);
        let eligible = next_stream_id == stream_id
            && next_offset == expected_offset
            && next_payload_bytes > 0
            && next_total.is_some_and(|total| total <= payload_limit)
            && next_end.is_some_and(|end| end <= receive_limit);
        if !eligible {
            deferred = Some(next);
            break;
        }
        payload_bytes = next_total.expect("eligible ready batch payload is bounded");
        expected_offset = next_end.expect("eligible STREAM_DATA has an end offset");
        batch.items.push(next);
    }

    batch.payload_bytes = payload_bytes;
    deferred
}

/// Applies each original frame to the RFC receive state, then performs one
/// vectored local write/flush transaction for all bytes released by the ready
/// batch. The per-frame callback preserves role-specific state and path
/// attribution while payload buffers are moved, never copied.
pub(in crate::runtime) async fn apply_and_write_ready_stream_data_batch<S, T, A>(
    local: &mut S,
    recv_stream: &mut ReliableRecvStream,
    batch: &mut ReadyStreamDataBatch<T>,
    direction: ReadyStreamDataDirection,
    flush_empty: bool,
    mut apply: A,
) -> Result<usize, RuntimeError>
where
    S: AsyncWrite + Unpin,
    A: FnMut(&mut ReliableRecvStream, T) -> Result<ReceiveOutcome, RuntimeError>,
{
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = direction;
    #[cfg(feature = "lab-diagnostics")]
    let batch_started = Instant::now();
    let source_frames = batch.len();
    #[cfg(feature = "lab-diagnostics")]
    let source_payload_bytes = batch.payload_bytes();
    let mut apply_error = None;
    {
        let items = &mut batch.items;
        let delivered = &mut batch.delivered;
        for item in items.drain(..) {
            let outcome = match apply(recv_stream, item) {
                Ok(outcome) => outcome,
                Err(err) => {
                    apply_error = Some(err);
                    break;
                }
            };
            delivered.extend(outcome.delivered);
        }
    }

    #[cfg(feature = "lab-diagnostics")]
    let write_started = Instant::now();
    let delivered_bytes = match write_delivered_payloads(local, batch.delivered.as_slice()).await {
        Ok(delivered_bytes) => delivered_bytes,
        Err(err) => {
            batch.delivered.clear();
            batch.payload_bytes = 0;
            return Err(RuntimeError::Io(err));
        }
    };
    #[cfg(feature = "lab-diagnostics")]
    crate::lab_diagnostics::lab_perf_record(
        "relay.local_write_wait",
        write_started.elapsed(),
        delivered_bytes,
    );
    if !batch.delivered.is_empty() || (flush_empty && apply_error.is_none()) {
        #[cfg(feature = "lab-diagnostics")]
        let flush_started = Instant::now();
        if let Err(err) = local.flush().await {
            batch.delivered.clear();
            batch.payload_bytes = 0;
            return Err(RuntimeError::Io(err));
        }
        #[cfg(feature = "lab-diagnostics")]
        crate::lab_diagnostics::lab_perf_record(
            "relay.local_flush_wait",
            flush_started.elapsed(),
            0,
        );
    }
    if source_frames > 1 && apply_error.is_none() {
        #[cfg(feature = "lab-diagnostics")]
        {
            crate::lab_diagnostics::lab_perf_record(
                direction.metric(),
                batch_started.elapsed(),
                delivered_bytes,
            );
            crate::lab_diagnostics::lab_diagnostic(
                "relay_ready_stream_data_batch",
                format_args!(
                    "direction={} source_frames={} source_payload_bytes={} delivered_bytes={}",
                    direction.label(),
                    source_frames,
                    source_payload_bytes,
                    delivered_bytes,
                ),
            );
        }
    }

    batch.delivered.clear();
    batch.payload_bytes = 0;
    if let Some(err) = apply_error {
        return Err(err);
    }
    Ok(delivered_bytes)
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
