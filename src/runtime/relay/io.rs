#[cfg(test)]
use crate::model::admission::{
    ReliableSourceServiceStagingContext, ReliableSourceStagingContext,
    bulk_service_feed_reservoir_payload_bytes, reliable_relay_source_staging_owner_tail_headroom,
};
#[cfg(test)]
use crate::model::admission::{
    bulk_service_horizon_payload_bytes, bulk_service_product_envelope_payload_bytes,
};
use crate::model::capacity::{
    QUIC_MAX_ACK_DELAY, QUIC_PERSISTENT_CONGESTION_THRESHOLD, QUIC_TIMER_GRANULARITY,
    adaptive_reliable_relay_inflight_bytes, adaptive_reliable_relay_repair_bytes,
    reliable_stream_ack_update_bytes, reliable_stream_advertised_window_bytes,
    reliable_stream_max_data_update_bytes,
};
#[cfg(test)]
use crate::model::capacity::{reliable_bulk_carrier_feed_quantum_bytes, reliable_relay_buffer_len};
use crate::model::timing::transport_pto_from_snapshot;
use crate::mux::MuxLimits;
use crate::mux::stream::{ReliableRecvStream, ReliableSendStream};
use crate::protocol::frame::{normalize_offset_ranges, stream_ack_contiguous_frontier};
use crate::protocol::{Frame, OffsetRange, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::scheduler::{FlowLane, PathSnapshot};
use bytes::Bytes;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

// Relay I/O orchestrates reads, writes, and feedback timing. It observes queue
// counters but delegates product admission limits to their policy modules.

pub(in crate::runtime) fn reliable_relay_error_is_migratable(err: &RuntimeError) -> bool {
    matches!(
        err,
        RuntimeError::PathHeartbeatTimeout
            | RuntimeError::PathOpenTimedOut
            | RuntimeError::ReliablePathSessionClosed
            | RuntimeError::Tcp(_)
            | RuntimeError::Encrypted(_)
            | RuntimeError::RemoteClosed(_)
            | RuntimeError::Protocol(_)
    )
}

pub(in crate::runtime) fn stream_ack_gap_repair_allowed(
    complete: bool,
    has_multipath_repair_alternative: bool,
    ack_gap_repair_ready: bool,
) -> bool {
    if !complete {
        return false;
    }
    if !has_multipath_repair_alternative {
        return false;
    }
    ack_gap_repair_ready
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

pub(in crate::runtime) fn update_repair_authoritative_ack_snapshot(
    stored_frontier: &mut u64,
    stored_ranges: &mut Vec<OffsetRange>,
    stored_complete: &mut bool,
    complete: bool,
    ranges: &[OffsetRange],
) {
    if !complete {
        return;
    }
    let mut merged = if *stored_complete {
        std::mem::take(stored_ranges)
    } else {
        Vec::new()
    };
    merged.extend_from_slice(ranges);
    merged = normalize_offset_ranges(merged);
    *stored_frontier = (*stored_frontier).max(stream_ack_contiguous_frontier(&merged));
    *stored_ranges = merged;
    *stored_complete = true;
}

pub(in crate::runtime) fn reliable_relay_tail_repair_delay(path: Option<PathSnapshot>) -> Duration {
    transport_pto_from_snapshot(path)
}

pub(in crate::runtime) fn reliable_ack_gap_repair_delay(path: Option<PathSnapshot>) -> Duration {
    transport_pto_from_snapshot(path).saturating_mul(QUIC_PERSISTENT_CONGESTION_THRESHOLD)
}

#[cfg(test)]
pub(in crate::runtime) fn stream_ack_gap_repair_frames(
    send_stream: &ReliableSendStream,
    ranges: &[OffsetRange],
    byte_limit: usize,
    complete: bool,
    has_multipath_repair_alternative: bool,
    ack_gap_repair_ready: bool,
) -> Vec<Frame> {
    if stream_ack_ranges_expose_authoritative_gap(complete, ranges)
        && stream_ack_gap_repair_allowed(
            complete,
            has_multipath_repair_alternative,
            ack_gap_repair_ready,
        )
    {
        send_stream.retransmission_frames_for_ack_gaps(ranges, byte_limit)
    } else {
        Vec::new()
    }
}

pub(in crate::runtime) fn stream_ack_gap_repair_frames_normalized(
    send_stream: &ReliableSendStream,
    ranges: &[OffsetRange],
    byte_limit: usize,
    complete: bool,
    has_multipath_repair_alternative: bool,
    ack_gap_repair_ready: bool,
) -> Vec<Frame> {
    if stream_ack_ranges_expose_authoritative_gap(complete, ranges)
        && stream_ack_gap_repair_allowed(
            complete,
            has_multipath_repair_alternative,
            ack_gap_repair_ready,
        )
    {
        send_stream.retransmission_frames_for_normalized_ack_gaps(ranges, byte_limit)
    } else {
        Vec::new()
    }
}

pub(in crate::runtime) fn stream_final_offset_tail_repair_frames_normalized(
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
pub(in crate::runtime) struct ReliableAckGapRepairProgress {
    first_gap_start: Option<u64>,
    first_seen_at: Option<Instant>,
    last_repair_at: Option<Instant>,
}

impl ReliableAckGapRepairProgress {
    pub(in crate::runtime) fn repair_ready(
        &mut self,
        complete: bool,
        normalized_ranges: &[OffsetRange],
        active_underlay: Option<UnderlayProtocol>,
        has_multipath_repair_alternative: bool,
        path: Option<PathSnapshot>,
    ) -> bool {
        self.repair_ready_at(
            complete,
            normalized_ranges,
            active_underlay,
            has_multipath_repair_alternative,
            reliable_ack_gap_repair_delay(path),
            Instant::now(),
        )
    }

    fn repair_ready_at(
        &mut self,
        complete: bool,
        normalized_ranges: &[OffsetRange],
        active_underlay: Option<UnderlayProtocol>,
        has_multipath_repair_alternative: bool,
        progress_interval: Duration,
        now: Instant,
    ) -> bool {
        if !complete {
            self.clear();
            return false;
        }
        if active_underlay.is_none() {
            self.clear();
            return false;
        }
        if !has_multipath_repair_alternative {
            self.clear();
            return false;
        }
        let Some(first_gap) = normalized_stream_ack_first_gap(normalized_ranges) else {
            self.clear();
            return false;
        };
        if self.first_gap_start != Some(first_gap.0) {
            self.first_gap_start = Some(first_gap.0);
            self.first_seen_at = Some(now);
            self.last_repair_at = None;
            return false;
        }
        if self.first_seen_at.is_none_or(|first_seen_at| {
            now.saturating_duration_since(first_seen_at) < progress_interval
        }) {
            return false;
        }
        if self.last_repair_at.is_some_and(|last_repair_at| {
            now.saturating_duration_since(last_repair_at) < progress_interval
        }) {
            return false;
        }
        true
    }

    pub(in crate::runtime) fn record_repair_queued(&mut self) {
        self.record_repair_queued_at(Instant::now());
    }

    fn record_repair_queued_at(&mut self, now: Instant) {
        if self.first_gap_start.is_some() {
            self.last_repair_at = Some(now);
        }
    }

    pub(in crate::runtime) fn release_repair_attempt(&mut self) {
        self.last_repair_at = None;
    }

    fn clear(&mut self) {
        self.first_gap_start = None;
        self.first_seen_at = None;
        self.last_repair_at = None;
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

#[derive(Debug, Clone, Default)]
pub(in crate::runtime) struct ReliableRecvProgress {
    last_max_data_offset: u64,
    last_max_data_window_bytes: u64,
    last_ack_offset: u64,
    last_ack_reorder_bytes: usize,
    last_ack_range_count: usize,
    last_ack_largest_end: u64,
    last_ack_at: Option<Instant>,
}

impl ReliableRecvProgress {
    pub(super) fn has_sent_ack(&self) -> bool {
        self.last_ack_at.is_some()
    }

    pub(in crate::runtime) fn should_send_ack(
        &mut self,
        recv_stream: &ReliableRecvStream,
        path: Option<PathSnapshot>,
        lane: FlowLane,
        mux_limits: MuxLimits,
        force: bool,
    ) -> bool {
        let now = Instant::now();
        let next_offset = recv_stream.next_offset();
        let reorder_bytes = recv_stream.reorder_bytes();
        let ack_summary = recv_stream.ack_range_summary();
        let range_count = ack_summary.count;
        let largest_end = ack_summary.largest_end;
        let has_progress = next_offset > 0 || reorder_bytes > 0;
        let first_ack = self.last_ack_at.is_none() && has_progress;
        let ack_step = reliable_stream_ack_update_bytes(path, lane, mux_limits);
        let horizon_advanced = largest_end.saturating_sub(self.last_ack_largest_end) >= ack_step;
        let reorder_delta = reorder_bytes.abs_diff(self.last_ack_reorder_bytes) as u64 >= ack_step;
        let gap_state_changed = reorder_bytes > 0
            && (range_count != self.last_ack_range_count || horizon_advanced || reorder_delta);
        let delivered_since_ack = next_offset.saturating_sub(self.last_ack_offset);
        let enough_delivered = delivered_since_ack >= ack_step;
        let ack_timer_elapsed = self.last_ack_at.is_some_and(|last_ack_at| {
            now.saturating_duration_since(last_ack_at)
                >= reliable_stream_recv_progress_interval(path)
        });
        if force
            || first_ack
            || gap_state_changed
            || enough_delivered
            || (has_progress && delivered_since_ack > 0 && ack_timer_elapsed)
        {
            self.last_ack_offset = next_offset;
            self.last_ack_reorder_bytes = reorder_bytes;
            self.last_ack_range_count = range_count;
            self.last_ack_largest_end = largest_end;
            self.last_ack_at = Some(now);
            true
        } else {
            false
        }
    }

    pub(in crate::runtime) fn should_send_max_data(
        &mut self,
        recv_stream: &ReliableRecvStream,
        path: Option<PathSnapshot>,
        lane: FlowLane,
        mux_limits: MuxLimits,
        force: bool,
    ) -> bool {
        let window_bytes = reliable_stream_advertised_window_bytes(path, lane, mux_limits);
        let max_offset = recv_stream.max_data_offset_with_window(window_bytes);
        let update_step = reliable_stream_max_data_update_bytes(window_bytes, mux_limits);
        let window_changed = self.last_max_data_window_bytes != 0
            && window_bytes.abs_diff(self.last_max_data_window_bytes) >= update_step;
        if force
            || self.last_max_data_offset == 0
            || window_changed
            || max_offset.saturating_sub(self.last_max_data_offset) >= update_step
        {
            self.last_max_data_offset = max_offset;
            self.last_max_data_window_bytes = window_bytes;
            true
        } else {
            false
        }
    }
}

pub(in crate::runtime) fn reliable_relay_recv_progress_resend_active(
    recv_stream: &ReliableRecvStream,
    remote_open: bool,
    active_underlay: Option<UnderlayProtocol>,
) -> bool {
    remote_open
        && match active_underlay {
            Some(UnderlayProtocol::Udp) => {
                recv_stream.next_offset() > 0 || recv_stream.reorder_bytes() > 0
            }
            Some(UnderlayProtocol::Tcp) => recv_stream.reorder_bytes() > 0,
            None => false,
        }
}

pub(in crate::runtime) fn reliable_stream_recv_progress_interval(
    path: Option<PathSnapshot>,
) -> Duration {
    transport_pto_from_snapshot(path)
        .div_f64(2.0)
        .max(QUIC_TIMER_GRANULARITY)
}

pub(in crate::runtime) fn sender_service_retry_delay(path: Option<PathSnapshot>) -> Duration {
    (transport_pto_from_snapshot(path) / 16)
        .max(Duration::from_millis(5))
        .min(QUIC_MAX_ACK_DELAY)
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
    if final_offset < recv_stream.next_offset() {
        return Err(RuntimeError::Protocol(
            "stream FIN final offset is behind delivered data",
        ));
    }
    if let Some(existing) = *pending_final_offset {
        if existing != final_offset {
            return Err(RuntimeError::Protocol(
                "conflicting stream FIN final offsets",
            ));
        }
    } else if final_offset > recv_stream.next_offset() {
        *pending_final_offset = Some(final_offset);
    }
    Ok(final_offset == recv_stream.next_offset())
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
    pending_final_offset.is_some_and(|final_offset| recv_stream.next_offset() >= final_offset)
}

pub(in crate::runtime) fn stream_terminal_fin_replay_required(
    fin_sent: bool,
    fin_replayed: bool,
    sender_queue_empty: bool,
    repair_bytes: usize,
    ack_frontier: u64,
    final_offset: u64,
) -> bool {
    fin_sent
        && !fin_replayed
        && sender_queue_empty
        && repair_bytes == 0
        && ack_frontier >= final_offset
}

pub(in crate::runtime) fn reliable_critical_tail_repair_limit_bytes(
    event_repair_limit: usize,
    repair_debt_bytes: usize,
    mux_limits: MuxLimits,
) -> usize {
    if repair_debt_bytes == 0 {
        return 0;
    }
    let resource_cap = mux_limits
        .max_repair_bytes
        .min(mux_limits.max_path_flight_bytes)
        .max(1);
    repair_debt_bytes
        .min(event_repair_limit.max(1))
        .min(resource_cap)
}

pub(in crate::runtime) fn reliable_persistent_ack_gap_repair_limit_bytes(
    path: Option<PathSnapshot>,
    owner_underlay: Option<UnderlayProtocol>,
    lane: FlowLane,
    repair_debt_bytes: usize,
    mux_limits: MuxLimits,
) -> usize {
    let event_limit = adaptive_reliable_relay_repair_bytes(path, lane, mux_limits);
    let event_limit = if owner_underlay == Some(UnderlayProtocol::Tcp) && lane.is_bulk() {
        // A complete ACK with the same cross-carrier hole for three PTOs is
        // stronger evidence than ordinary packet loss. Repair one modeled
        // service flight so a dead TCP owner cannot advance a 64 MiB product
        // frontier one 64 KiB quantum per proof interval.
        let service_flight = adaptive_reliable_relay_inflight_bytes(path, lane, mux_limits);
        let existing_service_debt = path.map_or(0, |snapshot| {
            snapshot
                .product_bytes_in_flight
                .max(snapshot.product_queue_bytes)
                .max(snapshot.queue_bytes)
                .min(usize::MAX as u64) as usize
        });
        service_flight.saturating_sub(existing_service_debt)
    } else {
        event_limit
    };
    if event_limit == 0 {
        return 0;
    }
    reliable_critical_tail_repair_limit_bytes(event_limit, repair_debt_bytes, mux_limits)
}

pub(in crate::runtime) fn reliable_critical_tail_repair_is_over_budget(
    budget_remaining: usize,
    repair_limit: usize,
) -> bool {
    budget_remaining == 0 && repair_limit > 0
}

#[cfg(test)]
#[path = "io_test.rs"]
mod tests;
