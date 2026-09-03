use crate::model::path::RelayPathInstance;
use crate::model::timing::{ReliableDataAckGapTiming, reliable_path_stale_interval};
use crate::mux::stream::{
    ReceiveOutcome, ReliableRecvStream, ReliableSendStream, StreamError, ValidatedStreamAck,
    validate_stream_ack,
};
use crate::protocol::frame::normalize_offset_ranges;
use crate::protocol::{Frame, OffsetRange, StreamId, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::sender::ServerReinjectionOutputIdentity;
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
/// receiver-observed DSN horizon carried by a complete ACK transaction. Incomplete
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

    /// Returns true when this logical stream has already applied every
    /// positive and negative fact carried by `ack`.
    ///
    /// Retained ACK state is shared across attachments. Redundant publication
    /// must therefore be idempotent at this owner rather than re-running
    /// cache, flight, and recovery mutations once per carrier.
    pub(in crate::runtime) fn subsumes(&self, ack: &ValidatedStreamAck) -> bool {
        let Some(horizon) = self.horizon else {
            return false;
        };
        if ack.complete() {
            let observed_horizon = ack.ranges().last().map_or(0, |range| range.end);
            if observed_horizon > horizon {
                return false;
            }
        }
        ack.ranges().iter().all(|range| {
            range.end <= horizon
                && self
                    .ranges
                    .iter()
                    .any(|stored| stored.start <= range.start && range.end <= stored.end)
        })
    }

    fn update(&mut self, ack: &ValidatedStreamAck) {
        if !ack.complete() && self.horizon.is_none() {
            return;
        }

        if ack.complete() {
            let observed_horizon = ack.ranges().last().map_or(0, |range| range.end);
            self.horizon = Some(
                self.horizon
                    .map_or(observed_horizon, |horizon| horizon.max(observed_horizon)),
            );
        }
        let horizon = self
            .horizon
            .expect("complete ACK authority must have a receiver-observed horizon");
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
    frontier_frame_limit: usize,
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
        preserve_reinjection_frontier_quantum(
            send_stream.retransmission_frames_for_normalized_ack_gaps(ranges, byte_limit),
            frontier_frame_limit,
        )
    } else {
        Vec::new()
    }
}

/// Keeps recovery observe/decide/apply on the same lowest-missing quantum.
///
/// Persistent recovery may fill a larger target service window after target
/// selection. Regenerating that window must not coalesce the first range past
/// the payload extent whose completion selected and authorized the target.
fn preserve_reinjection_frontier_quantum(
    frames: Vec<Frame>,
    frontier_frame_limit: usize,
) -> Vec<Frame> {
    let mut frames = frames.into_iter();
    let Some(first) = frames.next() else {
        return Vec::new();
    };
    let mut preserved = Vec::with_capacity(frames.len().saturating_add(2));
    match first {
        Frame::StreamData {
            stream_id,
            offset,
            payload,
        } if payload.len() > frontier_frame_limit.max(1) => {
            let split = frontier_frame_limit.max(1);
            preserved.push(Frame::StreamData {
                stream_id,
                offset,
                payload: payload.slice(..split),
            });
            preserved.push(Frame::StreamData {
                stream_id,
                offset: offset.saturating_add(split as u64),
                payload: payload.slice(split..),
            });
        }
        first => preserved.push(first),
    }
    preserved.extend(frames);
    preserved
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

/// Persistent-gap identity and its immutable first-recovery deadline.
#[derive(Debug, Default)]
pub(in crate::runtime) struct ReliableAckGapReinjectionProgress {
    first_gap_start: Option<u64>,
    original_assignment_at: Option<Instant>,
    loss_at: Option<Instant>,
    fallback_at: Option<Instant>,
    candidate_deadline: Option<Instant>,
}

/// Reconciles the actor's durable accepted-copy wake with one exact ledger
/// observation. A prior due deadline is returned once, while the observation
/// atomically installs any disjoint still-live successor deadline.
pub(in crate::runtime) fn reconcile_accepted_copy_wake(
    wake_at: &mut Option<Instant>,
    observed: Option<Instant>,
    now: Instant,
) -> bool {
    let due = accepted_copy_wake_is_due(*wake_at, now);
    *wake_at = observed;
    due
}

/// A committed accepted-copy deadline takes precedence over topology work at
/// the exact due boundary.
pub(in crate::runtime) fn accepted_copy_wake_is_due(
    wake_at: Option<Instant>,
    now: Instant,
) -> bool {
    wake_at.is_some_and(|deadline| deadline <= now)
}

/// Retains the earliest deadline returned by successful carrier commits in
/// the current actor turn. This closes the interval between command commit
/// and the next ledger observation, including when the deadline has already
/// elapsed before that observation.
pub(in crate::runtime) fn retain_accepted_copy_wake(
    wake_at: &mut Option<Instant>,
    accepted_copy_deadline: Instant,
) {
    *wake_at = Some(wake_at.map_or(accepted_copy_deadline, |current| {
        current.min(accepted_copy_deadline)
    }));
}

/// One exact attachment with authoritative outstanding OriginalData.
#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ReliablePathStalenessObservation<Candidate> {
    candidate: Candidate,
    persistence: Duration,
    has_reinjection_path: bool,
}

impl<Candidate> ReliablePathStalenessObservation<Candidate> {
    pub(in crate::runtime) fn new(
        candidate: Candidate,
        has_reinjection_path: bool,
        underlay: Option<UnderlayProtocol>,
        path: Option<PathSnapshot>,
    ) -> Self {
        Self {
            candidate,
            persistence: reliable_path_stale_interval(underlay, path),
            has_reinjection_path,
        }
    }

    #[cfg(test)]
    fn with_persistence(
        candidate: Candidate,
        has_reinjection_path: bool,
        persistence: Duration,
    ) -> Self {
        Self {
            candidate,
            persistence,
            has_reinjection_path,
        }
    }
}

/// Tracks an independent persistence clock for every exact attachment
/// incarnation.
///
/// TCP and QUIC recovery remain path-local. This state only decides when
/// connection-level reinjection may stop assigning new OriginalData to an
/// attachment. Gap repair or frontier movement on another attachment cannot
/// restart its clock.
#[derive(Debug)]
pub(in crate::runtime) struct ReliablePathStaleness<Candidate> {
    clocks: SmallVec<[ReliablePathStalenessClock<Candidate>; 4]>,
    next_deadline: Option<Instant>,
}

#[derive(Debug)]
struct ReliablePathStalenessClock<Candidate> {
    candidate: Candidate,
    deadline: Instant,
}

impl<Candidate> Default for ReliablePathStaleness<Candidate> {
    fn default() -> Self {
        Self {
            clocks: SmallVec::new(),
            next_deadline: None,
        }
    }
}

pub(in crate::runtime) type ReliableRequestPathStaleness = ReliablePathStaleness<RelayPathInstance>;
pub(in crate::runtime) type ReliableResponsePathStaleness =
    ReliablePathStaleness<ServerReinjectionOutputIdentity>;

impl<Candidate: Copy + Eq> ReliablePathStaleness<Candidate> {
    fn refresh_next_deadline(&mut self) {
        self.next_deadline = self.clocks.iter().map(|clock| clock.deadline).min();
    }

    pub(in crate::runtime) fn stale_paths(
        &mut self,
        observations: &[ReliablePathStalenessObservation<Candidate>],
        made_progress: &[Candidate],
    ) -> SmallVec<[Candidate; 4]> {
        self.stale_paths_at(observations, made_progress, Instant::now())
    }

    pub(in crate::runtime) fn next_deadline(&self) -> Option<Instant> {
        self.next_deadline
    }

    fn stale_paths_at(
        &mut self,
        observations: &[ReliablePathStalenessObservation<Candidate>],
        made_progress: &[Candidate],
        now: Instant,
    ) -> SmallVec<[Candidate; 4]> {
        self.clocks.retain(|clock| {
            observations.iter().any(|observation| {
                observation.candidate == clock.candidate && observation.has_reinjection_path
            })
        });

        let mut stale = SmallVec::<[Candidate; 4]>::new();
        for observation in observations
            .iter()
            .filter(|observation| observation.has_reinjection_path)
        {
            if stale.contains(&observation.candidate) {
                continue;
            }
            let Some(clock) = self
                .clocks
                .iter_mut()
                .find(|clock| clock.candidate == observation.candidate)
            else {
                self.clocks.push(ReliablePathStalenessClock {
                    candidate: observation.candidate,
                    deadline: now + observation.persistence,
                });
                continue;
            };
            if made_progress.contains(&observation.candidate) {
                clock.deadline = now + observation.persistence;
            }
            if clock.deadline <= now {
                stale.push(observation.candidate);
            }
        }
        self.refresh_next_deadline();
        stale
    }
}

impl ReliableAckGapReinjectionProgress {
    pub(in crate::runtime) fn observe_recovery_timing(
        &mut self,
        complete: bool,
        normalized_ranges: &[OffsetRange],
        has_multipath_reinjection_alternative: bool,
        observed_timing: Option<ReliableDataAckGapTiming>,
        alternate_completion: Option<Duration>,
        observed_at: Instant,
    ) -> Option<Instant> {
        if !self.retain_gap_identity(
            complete,
            normalized_ranges,
            has_multipath_reinjection_alternative,
        ) {
            self.candidate_deadline = None;
            return None;
        }
        if let Some(observed_timing) = observed_timing {
            if self.original_assignment_at != Some(observed_timing.assignment_at) {
                self.original_assignment_at = Some(observed_timing.assignment_at);
                self.loss_at = None;
                self.fallback_at = None;
                self.candidate_deadline = None;
            }
            if let Some(loss_at) = observed_timing.loss_at {
                self.loss_at = Some(self.loss_at.map_or(loss_at, |current| current.min(loss_at)));
            }
            self.fallback_at = Some(
                self.fallback_at
                    .map_or(observed_timing.fallback_at, |current| {
                        current.min(observed_timing.fallback_at)
                    }),
            );
        }

        // Owner clocks remain monotonic for the exact assignment, but target
        // eligibility is current. A slower replacement must not inherit an
        // early target's completion claim.
        self.candidate_deadline = self.fallback_at.and_then(|fallback_at| {
            ReliableDataAckGapTiming {
                assignment_at: self.original_assignment_at?,
                loss_at: self.loss_at,
                fallback_at,
            }
            .target_deadline(alternate_completion, observed_at)
        });
        self.candidate_deadline
    }

    pub(in crate::runtime) fn reinjection_ready(
        &mut self,
        complete: bool,
        normalized_ranges: &[OffsetRange],
        has_multipath_reinjection_alternative: bool,
        measured_reinjection_ready: bool,
    ) -> bool {
        self.reinjection_ready_at(
            complete,
            normalized_ranges,
            has_multipath_reinjection_alternative,
            measured_reinjection_ready,
            Instant::now(),
        )
    }

    fn reinjection_ready_at(
        &mut self,
        complete: bool,
        normalized_ranges: &[OffsetRange],
        has_multipath_reinjection_alternative: bool,
        measured_reinjection_ready: bool,
        _now: Instant,
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
        true
    }

    pub(in crate::runtime) fn next_reinjection_deadline(&self) -> Option<Instant> {
        self.candidate_deadline
    }

    fn clear(&mut self) {
        self.first_gap_start = None;
        self.original_assignment_at = None;
        self.loss_at = None;
        self.fallback_at = None;
        self.candidate_deadline = None;
    }

    fn retain_gap_identity(
        &mut self,
        complete: bool,
        normalized_ranges: &[OffsetRange],
        has_multipath_reinjection_alternative: bool,
    ) -> bool {
        if !complete {
            self.clear();
            return false;
        }
        let Some(first_gap) = normalized_stream_ack_first_gap(normalized_ranges) else {
            self.clear();
            return false;
        };
        if self.first_gap_start != Some(first_gap.0) {
            self.first_gap_start = Some(first_gap.0);
            self.original_assignment_at = None;
            self.loss_at = None;
            self.fallback_at = None;
            self.candidate_deadline = None;
        }
        has_multipath_reinjection_alternative
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
#[path = "tests_io.rs"]
mod tests;
