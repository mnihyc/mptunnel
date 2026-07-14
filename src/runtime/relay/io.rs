use super::*;
use crate::model::admission::{
    ReliableSourceServiceStagingContext, ReliableSourceStagingContext,
    bulk_service_feed_reservoir_payload_bytes, reliable_relay_source_staging_owner_tail_headroom,
};
#[cfg(test)]
use crate::model::admission::{
    bulk_service_horizon_payload_bytes, bulk_service_product_envelope_payload_bytes,
};

// Relay I/O orchestrates reads, writes, and feedback timing. It observes queue
// counters but delegates product admission limits to their policy modules.

pub(in crate::runtime) async fn send_sender_service_attach_control_frames(
    path_stream: &ReliablePathStream,
    send_stream: &ReliableSendStream,
    resend_fin: bool,
) -> Result<(), RuntimeError> {
    if resend_fin {
        send_sender_service_control_frame(
            path_stream,
            Frame::StreamFin {
                stream_id: path_stream.stream_id,
                final_offset: send_stream.next_offset(),
            },
        )?;
    }
    Ok(())
}

pub(in crate::runtime) fn frame_pacing_bytes(frame: &Frame) -> usize {
    match frame {
        Frame::StreamData { payload, .. } | Frame::PathCapacityData { payload, .. } => {
            payload.len().max(1)
        }
        Frame::StreamFin { .. }
        | Frame::StreamAck { .. }
        | Frame::StreamMaxData { .. }
        | Frame::StreamReset { .. }
        | Frame::StreamDetach { .. } => 1,
        _ => 0,
    }
}

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

pub(in crate::runtime) fn stream_tail_timer_repair_allowed(
    live_owner_tail_repair_candidate: bool,
    has_failed_owner_repair_output: bool,
) -> bool {
    live_owner_tail_repair_candidate || has_failed_owner_repair_output
}

pub(in crate::runtime) fn reliable_relay_tail_repair_timer_active(
    repair_bytes: usize,
    live_owner_tail_repair_candidate: bool,
    failed_owner_tail_repair_ready: bool,
) -> bool {
    repair_bytes > 0
        && stream_tail_timer_repair_allowed(
            live_owner_tail_repair_candidate,
            failed_owner_tail_repair_ready,
        )
}

pub(in crate::runtime) fn stream_ack_is_authoritative_contiguous_prefix(
    complete: bool,
    ranges: &[OffsetRange],
    frontier: u64,
) -> bool {
    complete
        && frontier > 0
        && matches!(ranges, [range] if range.start == 0 && range.end == frontier)
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

pub(in crate::runtime) fn reliable_relay_ordered_owner_debt_bytes(
    lane: FlowLane,
    _ack_complete: bool,
    ack_frontier: u64,
    next_offset: u64,
) -> usize {
    if !lane.is_bulk() || ack_frontier >= next_offset {
        return 0;
    }
    // This is a tail guard, not repair debt. It blocks alternate OwnerData and
    // missing-owner failover while lower Service bytes are unresolved, but it
    // must not make the live Service owner itself inadmissible.
    usize::try_from(next_offset.saturating_sub(ack_frontier)).unwrap_or(usize::MAX)
}

pub(in crate::runtime) fn stream_ack_contiguous_frontier(
    _complete: bool,
    ranges: &[OffsetRange],
) -> u64 {
    ranges
        .first()
        .filter(|range| range.start == 0)
        .map_or(0, |range| range.end)
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
        stored_ranges.clone()
    } else {
        Vec::new()
    };
    merged.extend_from_slice(ranges);
    merged = normalized_offset_ranges(&merged);
    *stored_frontier = (*stored_frontier).max(stream_ack_contiguous_frontier(true, &merged));
    *stored_ranges = merged;
    *stored_complete = true;
}

fn reliable_relay_current_ordered_owner_debt_bytes(
    lane: FlowLane,
    send_stream: &ReliableSendStream,
    ack_complete: bool,
    ack_frontier: u64,
) -> usize {
    reliable_relay_ordered_owner_debt_bytes(
        lane,
        ack_complete,
        ack_frontier,
        send_stream.next_offset(),
    )
}

pub(in crate::runtime) fn reliable_relay_tail_repair_deadline(
    last_progress_at: Instant,
    last_repair_at: Instant,
    path: Option<PathSnapshot>,
    lane: FlowLane,
) -> tokio::time::Instant {
    let stall_timeout = reliable_relay_tail_repair_delay(path, lane);
    if last_repair_at > last_progress_at {
        return tokio::time::Instant::from_std(
            last_repair_at + stall_timeout.saturating_mul(QUIC_PERSISTENT_CONGESTION_THRESHOLD),
        );
    }
    tokio::time::Instant::from_std(last_progress_at + stall_timeout)
}

pub(in crate::runtime) fn reliable_relay_effective_tail_repair_deadline(
    last_progress_at: Instant,
    last_repair_at: Instant,
    path: Option<PathSnapshot>,
    lane: FlowLane,
    failed_owner_tail_repair_ready: bool,
) -> tokio::time::Instant {
    if failed_owner_tail_repair_ready {
        let stall_timeout = reliable_relay_tail_repair_delay(None, lane);
        if last_repair_at <= last_progress_at {
            return tokio::time::Instant::from_std(last_progress_at);
        }
        return tokio::time::Instant::from_std(last_repair_at + stall_timeout);
    }
    reliable_relay_tail_repair_deadline(last_progress_at, last_repair_at, path, lane)
}

pub(in crate::runtime) fn reliable_relay_tail_repair_delay(
    path: Option<PathSnapshot>,
    lane: FlowLane,
) -> Duration {
    reliable_relay_stall_timeout(path, lane)
}

pub(in crate::runtime) fn reliable_ack_gap_repair_delay(
    path: Option<PathSnapshot>,
    lane: FlowLane,
) -> Duration {
    reliable_relay_stall_timeout(path, lane).saturating_mul(QUIC_PERSISTENT_CONGESTION_THRESHOLD)
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

pub(in crate::runtime) fn stream_final_offset_tail_repair_frames(
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
    send_stream.retransmission_frames_after_ack_frontier(ranges, byte_limit)
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
        lane: FlowLane,
    ) -> bool {
        self.repair_ready_at(
            complete,
            normalized_ranges,
            active_underlay,
            has_multipath_repair_alternative,
            reliable_ack_gap_repair_delay(path, lane),
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

fn normalized_stream_ack_first_gap(normalized_ranges: &[OffsetRange]) -> Option<(u64, u64)> {
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

#[cfg(feature = "lab-diagnostics")]
#[derive(Debug, Default)]
struct ServerReceiveHoleDiagnostics {
    opened_at: Option<Instant>,
    last_delivery_at: Option<Instant>,
}

#[cfg(feature = "lab-diagnostics")]
impl ServerReceiveHoleDiagnostics {
    fn observe(
        &mut self,
        stream_id: StreamId,
        recv_stream: &ReliableRecvStream,
        delivered_bytes: usize,
    ) {
        let now = Instant::now();
        let reorder_bytes = recv_stream.reorder_bytes();
        let ranges = recv_stream.ack_ranges();
        let first_gap = normalized_stream_ack_first_gap(&ranges);
        if reorder_bytes > 0 && self.opened_at.is_none() {
            self.opened_at = Some(now);
            lab_diagnostic(
                "server_receive_hole",
                format_args!(
                    "stream_id={} state=open next_offset={} reorder_bytes={} range_count={} first_gap_start={} first_gap_end={}",
                    stream_id.0,
                    recv_stream.next_offset(),
                    reorder_bytes,
                    ranges.len(),
                    first_gap.map_or(0, |gap| gap.0),
                    first_gap.map_or(0, |gap| gap.1),
                ),
            );
        } else if reorder_bytes == 0
            && let Some(opened_at) = self.opened_at.take()
        {
            lab_diagnostic(
                "server_receive_hole",
                format_args!(
                    "stream_id={} state=clear duration_us={} next_offset={} delivered_bytes={}",
                    stream_id.0,
                    now.saturating_duration_since(opened_at).as_micros(),
                    recv_stream.next_offset(),
                    delivered_bytes,
                ),
            );
        }
        if delivered_bytes > 0 {
            if let Some(last_delivery_at) = self.last_delivery_at {
                let delivery_gap = now.saturating_duration_since(last_delivery_at);
                // Keep the causal trace bounded to WAN-scale stalls; ordinary
                // per-frame delivery remains visible in the perf counters.
                if delivery_gap >= Duration::from_millis(100) {
                    lab_diagnostic(
                        "server_receive_delivery_stall",
                        format_args!(
                            "stream_id={} duration_us={} delivered_bytes={} next_offset={} reorder_bytes={} range_count={} first_gap_start={} first_gap_end={} hole_open={}",
                            stream_id.0,
                            delivery_gap.as_micros(),
                            delivered_bytes,
                            recv_stream.next_offset(),
                            reorder_bytes,
                            ranges.len(),
                            first_gap.map_or(0, |gap| gap.0),
                            first_gap.map_or(0, |gap| gap.1),
                            self.opened_at.is_some(),
                        ),
                    );
                }
            }
            self.last_delivery_at = Some(now);
        }
    }
}

fn offset_ranges_not_covered(ranges: &[OffsetRange], covered: &[OffsetRange]) -> Vec<OffsetRange> {
    let mut uncovered = Vec::new();
    let mut covered_index = 0usize;
    for range in ranges {
        let mut cursor = range.start;
        while covered_index < covered.len() && covered[covered_index].end <= cursor {
            covered_index += 1;
        }
        let mut index = covered_index;
        while index < covered.len() && covered[index].start < range.end {
            let known = covered[index];
            if known.start > cursor
                && let Some(missing) = OffsetRange::new(cursor, known.start.min(range.end))
            {
                uncovered.push(missing);
            }
            cursor = cursor.max(known.end).min(range.end);
            if cursor >= range.end {
                break;
            }
            index += 1;
        }
        covered_index = index;
        if cursor < range.end
            && let Some(missing) = OffsetRange::new(cursor, range.end)
        {
            uncovered.push(missing);
        }
    }
    uncovered
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

// Sparse history belongs only to server-side request feedback. Keeping it out
// of shared receive progress leaves the cloned response hot path cumulative.
#[derive(Debug, Default)]
pub(in crate::runtime) struct RequestTcpSparseAckProgress {
    acknowledged_ranges: Vec<OffsetRange>,
}

impl ReliableRecvProgress {
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
                >= reliable_stream_recv_progress_interval(path, lane)
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

impl RequestTcpSparseAckProgress {
    pub(in crate::runtime) fn ack_frames(
        &mut self,
        recv_stream: &ReliableRecvStream,
        sparse_delta: bool,
    ) -> Vec<Frame> {
        let current_ranges = recv_stream.ack_ranges();
        if !sparse_delta {
            self.acknowledged_ranges = current_ranges;
            return recv_stream.ack_frames();
        }
        let delta = offset_ranges_not_covered(&current_ranges, &self.acknowledged_ranges);
        if delta.is_empty() {
            return Vec::new();
        }
        let mut acknowledged = std::mem::take(&mut self.acknowledged_ranges);
        acknowledged.extend(delta.iter().copied());
        self.acknowledged_ranges = normalized_offset_ranges(&acknowledged);
        recv_stream.ack_delta_frames(&delta)
    }
}

/// Initial stream-level product receive window advertised to the peer.
///
/// Bulk credit is receiver-memory authority, not path-capacity proof. Both TCP
/// and QUIC therefore advertise the configured product envelope; source
/// staging and each carrier's native congestion controller independently bound
/// how much data can reach that envelope. Latency QUIC keeps a smaller window
/// so unrelated bulk work cannot consume its reserved product memory.
pub(in crate::runtime) fn reliable_stream_initial_advertised_window_bytes(
    underlay: UnderlayProtocol,
    lane: FlowLane,
    mux_limits: MuxLimits,
) -> u64 {
    reliable_stream_advertised_window_from_underlay(None, underlay, lane, mux_limits)
}

pub(in crate::runtime) fn reliable_stream_advertised_window_bytes(
    path: Option<PathSnapshot>,
    lane: FlowLane,
    mux_limits: MuxLimits,
) -> u64 {
    let underlay = path
        .map(|snapshot| snapshot.underlay)
        .unwrap_or(UnderlayProtocol::Tcp);
    reliable_stream_advertised_window_from_underlay(path, underlay, lane, mux_limits)
}

fn reliable_stream_advertised_window_from_underlay(
    _path: Option<PathSnapshot>,
    underlay: UnderlayProtocol,
    lane: FlowLane,
    mux_limits: MuxLimits,
) -> u64 {
    let configured = mux_limits.max_stream_window_bytes.max(1);
    if underlay != UnderlayProtocol::Udp || lane.is_bulk() {
        return configured;
    }

    let relay_chunk = reliable_relay_buffer_len(mux_limits) as u64;
    let min_window = RELIABLE_UDP_MIN_PRODUCT_WINDOW_BYTES
        .max(relay_chunk.saturating_mul(4))
        .min(configured);
    let startup_window = RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES
        .max(min_window)
        .min(configured);
    startup_window
}

pub(in crate::runtime) fn reliable_stream_max_data_update_bytes(
    advertised_window_bytes: u64,
    mux_limits: MuxLimits,
) -> u64 {
    let window_step = advertised_window_bytes.saturating_div(4).max(1);
    let payload_step = reliable_relay_buffer_len(mux_limits) as u64;
    window_step
        .max(payload_step)
        .min(advertised_window_bytes.max(1))
}

pub(in crate::runtime) fn reliable_stream_ack_update_bytes(
    path: Option<PathSnapshot>,
    lane: FlowLane,
    mux_limits: MuxLimits,
) -> u64 {
    if !lane.is_bulk() {
        return 1;
    }
    let advertised_window = reliable_stream_advertised_window_bytes(path, lane, mux_limits);
    let resource_ceiling = reliable_stream_max_data_update_bytes(advertised_window, mux_limits)
        .min(
            (mux_limits.max_repair_bytes as u64)
                .saturating_div(4)
                .max(1),
        )
        .max(PATH_OPEN_SCORE_BYTES as u64);
    let service_floor = BBR_MAX_SEND_QUANTUM_BYTES
        .min(reliable_relay_buffer_len(mux_limits))
        .max(PATH_OPEN_SCORE_BYTES) as u64;
    let measured_step = path
        .map(|path| {
            (reliable_path_product_bdp_bytes(path) / 2.0)
                .ceil()
                .max(1.0) as u64
        })
        .unwrap_or(service_floor);
    measured_step
        .clamp(service_floor.min(resource_ceiling), resource_ceiling)
        .min(mux_limits.max_repair_bytes as u64)
        .max(PATH_OPEN_SCORE_BYTES as u64)
}

pub(in crate::runtime) fn enqueue_tcp_recv_progress(
    response_sender: &mut ServerResponseSenderService,
    recv_stream: &ReliableRecvStream,
    progress: &mut ReliableRecvProgress,
    sparse_ack_progress: &mut RequestTcpSparseAckProgress,
    path: Option<PathSnapshot>,
    lane: FlowLane,
    mux_limits: MuxLimits,
    force_max_data: bool,
) -> bool {
    let mut sent_any = false;
    let sparse_delta = !force_max_data
        && progress.last_ack_at.is_some()
        && lane.is_bulk()
        && path.is_some_and(|snapshot| snapshot.underlay == UnderlayProtocol::Tcp)
        && recv_stream.reorder_bytes() > 0;
    if progress.should_send_ack(recv_stream, path, lane, mux_limits, force_max_data) {
        #[cfg(feature = "lab-diagnostics")]
        let ack_started = Instant::now();
        let ack_frames = sparse_ack_progress.ack_frames(recv_stream, sparse_delta);
        #[cfg(feature = "lab-diagnostics")]
        lab_perf_record("mux.ack_frames", ack_started.elapsed(), ack_frames.len());
        // Multipath receive ranges can exceed one ACK frame under normal
        // reordering. Send every incomplete ACK chunk instead of truncating a
        // single `complete=true` ACK; otherwise the peer treats omitted ranges
        // as loss and starts product repair that cannot improve TCP/QUIC
        // carrier delivery.
        for ack_frame in ack_frames {
            response_sender.enqueue_control_frame(ack_frame);
        }
        sent_any = true;
    }
    if progress.should_send_max_data(recv_stream, path, lane, mux_limits, force_max_data) {
        let advertised_window = reliable_stream_advertised_window_bytes(path, lane, mux_limits);
        response_sender
            .enqueue_control_frame(recv_stream.max_data_frame_with_window(advertised_window));
        sent_any = true;
    }
    sent_any
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

fn reliable_relay_recv_progress_timer_enabled(
    initial_underlay: UnderlayProtocol,
    has_multipath_repair_alternative: bool,
) -> bool {
    initial_underlay == UnderlayProtocol::Udp || has_multipath_repair_alternative
}

pub(in crate::runtime) fn reliable_stream_recv_progress_interval(
    path: Option<PathSnapshot>,
    lane: FlowLane,
) -> Duration {
    reliable_relay_stall_timeout(path, lane)
        .div_f64(2.0)
        .max(QUIC_TIMER_GRANULARITY)
}

pub(in crate::runtime) fn sender_service_retry_delay(
    path: Option<PathSnapshot>,
    lane: FlowLane,
) -> Duration {
    let _ = lane;
    (transport_pto_from_snapshot(path) / 16)
        .max(Duration::from_millis(5))
        .min(QUIC_MAX_ACK_DELAY)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ResponseSenderWaitState {
    blocked: bool,
    ready: bool,
    subscribe_capacity: bool,
    retry_at: Option<tokio::time::Instant>,
}

fn response_sender_wait_state(
    queue_nonempty: bool,
    queue_ready: bool,
    front_has_carrier_credit: bool,
    retry_at: Option<tokio::time::Instant>,
    now: tokio::time::Instant,
    retry_delay: Duration,
) -> ResponseSenderWaitState {
    if !queue_nonempty {
        return ResponseSenderWaitState {
            blocked: false,
            ready: false,
            subscribe_capacity: false,
            retry_at: None,
        };
    }
    if let Some(retry_at) = retry_at.filter(|deadline| *deadline > now) {
        return ResponseSenderWaitState {
            blocked: true,
            ready: false,
            subscribe_capacity: true,
            retry_at: Some(retry_at),
        };
    }
    if front_has_carrier_credit {
        return ResponseSenderWaitState {
            blocked: false,
            ready: queue_ready,
            subscribe_capacity: false,
            retry_at: None,
        };
    }
    let retry_at = now + retry_delay;
    ResponseSenderWaitState {
        blocked: true,
        ready: false,
        subscribe_capacity: true,
        retry_at: Some(retry_at),
    }
}

pub(in crate::runtime) fn reliable_relay_buffer_len(mux_limits: MuxLimits) -> usize {
    mux_limits
        .max_reliable_relay_chunk_bytes
        .min(mux_limits.max_payload_bytes)
        .min(mux_limits.max_path_flight_bytes)
        .max(1)
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

pub(in crate::runtime) fn adaptive_reliable_relay_chunk_bytes(
    path: Option<PathSnapshot>,
    lane: FlowLane,
    mux_limits: MuxLimits,
) -> usize {
    let cap = reliable_relay_scheduler_quantum_cap(path, lane, mux_limits);
    let floor = relay_lane_min_chunk_bytes(path, lane, mux_limits)
        .min(cap)
        .max(1);
    let Some(path) = path else {
        let startup = relay_lane_startup_chunk_bytes(lane, mux_limits);
        let startup = if lane.is_bulk() {
            startup.max(reliable_bulk_carrier_feed_quantum_bytes(mux_limits))
        } else {
            startup
        };
        return startup.min(cap).max(floor);
    };

    let quantum = bbr_send_quantum_bytes(path, mux_limits);
    let condition =
        reliable_path_quantum_condition_factor(path, reliable_path_product_bdp_bytes(path));
    let target = match lane {
        FlowLane::Control | FlowLane::RealtimeDatagram => {
            bbr_min_send_quantum_bytes(mux_limits).min(quantum)
        }
        FlowLane::Latency => PATH_OPEN_SCORE_BYTES.min(quantum).max(floor),
        FlowLane::Throughput | FlowLane::Background => {
            ((quantum as f64) * condition).ceil() as usize
        }
    };
    let target = if lane.is_bulk() {
        // TCP and QUIC UDP already own packet pacing and congestion below
        // mptunnel. Once a reliable stream is classified as throughput demand,
        // the product sender must not keep the carrier app-limited with a
        // 2*MSS application-record loop. Feed the carrier with BBR's bounded
        // maximum send quantum while retaining the configured frame/read
        // envelope and live condition cap as the hard upper bounds.
        target.max(reliable_bulk_carrier_feed_quantum_bytes(mux_limits))
    } else {
        target
    };
    target.clamp(floor, cap)
}

pub(in crate::runtime) fn adaptive_reliable_relay_chunk_bytes_with_frame_limit(
    path: Option<PathSnapshot>,
    lane: FlowLane,
    mux_limits: MuxLimits,
    max_frame_payload_bytes: usize,
) -> usize {
    adaptive_reliable_relay_chunk_bytes(path, lane, mux_limits)
        .min(max_frame_payload_bytes)
        .max(1)
}

pub(in crate::runtime) fn adaptive_reliable_relay_inflight_bytes(
    path: Option<PathSnapshot>,
    lane: FlowLane,
    mux_limits: MuxLimits,
) -> usize {
    let cap = mux_limits.max_path_flight_bytes.max(1);
    let floor = reliable_lane_min_inflight_bytes(lane, mux_limits)
        .min(cap)
        .max(1);
    let Some(path) = path else {
        return reliable_lane_startup_inflight_bytes(lane, mux_limits)
            .min(cap)
            .max(floor);
    };

    let bdp_bytes = reliable_path_product_bdp_bytes(path);
    let target = bbr_inflight_target_bytes(path, lane, mux_limits)
        * reliable_path_stability_factor(path)
        * reliable_path_backlog_factor(path, bdp_bytes);
    (target.ceil() as usize).clamp(floor, cap)
}

pub(in crate::runtime) fn reliable_relay_sender_dispatch_budget(
    mux_limits: MuxLimits,
    lane: FlowLane,
    adaptive_chunk: usize,
    inflight_limit: usize,
    queue_limit: usize,
) -> (usize, usize) {
    let quantum = adaptive_chunk.max(1);
    if !lane.is_bulk() {
        return (quantum, 1);
    }

    // The sender service is the scheduling boundary; path queues are only
    // writer pipes. Bulk may drain one bounded feed window per service pass,
    // but each emitted STREAM_DATA frame remains one adaptive quantum and the
    // pass yields before another ordinary bulk run.
    let service_window = reliable_relay_buffer_len(mux_limits)
        .min(queue_limit.max(1))
        .min(inflight_limit.max(1))
        .max(quantum);
    let items = service_window.div_ceil(quantum).max(1);
    let bytes = quantum
        .saturating_mul(items)
        .min(service_window)
        .max(quantum);
    (bytes, items)
}

pub(in crate::runtime) fn adaptive_reliable_relay_repair_bytes(
    path: Option<PathSnapshot>,
    lane: FlowLane,
    mux_limits: MuxLimits,
) -> usize {
    let repair_lane = match lane {
        FlowLane::Throughput | FlowLane::Background => FlowLane::Latency,
        other => other,
    };
    adaptive_reliable_relay_chunk_bytes(path, repair_lane, mux_limits).max(1)
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

fn reliable_live_owner_tail_repair_limit_bytes(
    path: Option<PathSnapshot>,
    owner_underlay: Option<UnderlayProtocol>,
    lane: FlowLane,
    repair_debt_bytes: usize,
    mux_limits: MuxLimits,
) -> usize {
    let base_limit = adaptive_reliable_relay_repair_bytes(path, lane, mux_limits);
    let event_limit = if owner_underlay == Some(UnderlayProtocol::Tcp) && lane.is_bulk() {
        // TCP recovery remains socket-local. After one owner PTO, reinject one
        // bounded modeled flight so a product prefix is not repaired 64 KiB at a time.
        adaptive_reliable_relay_inflight_bytes(path, lane, mux_limits)
            .min(bulk_service_feed_reservoir_payload_bytes(
                base_limit, mux_limits,
            ))
            .max(base_limit)
    } else {
        base_limit
    };
    reliable_critical_tail_repair_limit_bytes(event_limit, repair_debt_bytes, mux_limits)
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

fn reliable_failed_owner_tail_repair_ready(
    path_stream: &ReliablePathStream,
    send_stream: &ReliableSendStream,
    last_send_ack_ranges: &[OffsetRange],
    last_send_ack_complete: bool,
    last_send_ack_frontier: u64,
    mux_limits: MuxLimits,
) -> bool {
    if send_stream.repair_bytes() == 0 || last_send_ack_frontier >= send_stream.next_offset() {
        return false;
    }
    let no_ack_frontier_failed_owner_tail = last_send_ack_ranges.is_empty()
        && last_send_ack_frontier == 0
        && send_stream.next_offset() > 0;
    if !last_send_ack_complete && !no_ack_frontier_failed_owner_tail {
        return false;
    }
    let probe_limit =
        reliable_critical_tail_repair_limit_bytes(1, send_stream.repair_bytes(), mux_limits);
    if probe_limit == 0 {
        return false;
    }
    let source_frames = if last_send_ack_ranges.is_empty() {
        send_stream.retransmission_frames_for_ranges(
            &[OffsetRange {
                start: 0,
                end: send_stream.next_offset(),
            }],
            probe_limit,
        )
    } else {
        send_stream.retransmission_frames_for_ranges(
            &[OffsetRange {
                start: last_send_ack_frontier,
                end: send_stream.next_offset(),
            }],
            probe_limit,
        )
    };
    if source_frames.is_empty() {
        return false;
    }
    let (failed_owner_frames, _) =
        prefix_repair_frames_with_failed_owner_output(path_stream, source_frames.clone());
    if !failed_owner_frames.is_empty() {
        return true;
    }
    if last_send_ack_frontier == 0 || !last_send_ack_complete {
        return false;
    }
    let source_frames = send_stream.retransmission_frames_for_ranges(
        &[OffsetRange {
            start: last_send_ack_frontier,
            end: send_stream.next_offset(),
        }],
        probe_limit,
    );
    let (unknown_owner_frames, _) =
        prefix_repair_frames_with_unknown_owner_output(path_stream, source_frames);
    !unknown_owner_frames.is_empty()
}

fn reliable_final_tail_repair_ready(
    final_offset_known: bool,
    send_stream: &ReliableSendStream,
    last_send_ack_ranges: &[OffsetRange],
    last_send_ack_frontier: u64,
    tail_repair_deadline: tokio::time::Instant,
    now: tokio::time::Instant,
) -> bool {
    if !final_offset_known
        || send_stream.repair_bytes() == 0
        || last_send_ack_frontier >= send_stream.next_offset()
        || now < tail_repair_deadline
    {
        return false;
    }
    !last_send_ack_ranges.is_empty()
        || (last_send_ack_frontier == 0 && send_stream.next_offset() > 0)
}

pub(in crate::runtime) fn reliable_path_product_bdp_bytes(path: PathSnapshot) -> f64 {
    let rate_bps = path.delivery_rate_bps.max(
        path.product_progress_rate_bps
            .unwrap_or(path.delivery_rate_bps),
    );
    let rate_bps = rate_bps.max(1.0);
    (rate_bps / 8.0) * (path.srtt_ms.max(1.0) / 1000.0)
}

pub(in crate::runtime) fn bbr_min_send_quantum_bytes(mux_limits: MuxLimits) -> usize {
    let cap = reliable_relay_buffer_len(mux_limits).max(1);
    (BBR_MIN_SEND_QUANTUM_PACKETS * TRANSPORT_MSS_BYTES)
        .min(cap)
        .max(1)
}

pub(in crate::runtime) fn reliable_bulk_carrier_feed_quantum_bytes(mux_limits: MuxLimits) -> usize {
    let cap = reliable_relay_buffer_len(mux_limits).max(1);
    BBR_MAX_SEND_QUANTUM_BYTES
        .min(cap)
        .max(bbr_min_send_quantum_bytes(mux_limits))
}

pub(in crate::runtime) fn bbr_min_pipe_cwnd_bytes(mux_limits: MuxLimits) -> usize {
    let cap = mux_limits.max_path_flight_bytes.max(1);
    (BBR_MIN_PIPE_CWND_PACKETS * TRANSPORT_MSS_BYTES)
        .min(cap)
        .max(1)
}

pub(in crate::runtime) fn bbr_send_quantum_bytes(
    path: PathSnapshot,
    mux_limits: MuxLimits,
) -> usize {
    let cap = reliable_relay_buffer_len(mux_limits).max(1);
    let floor = bbr_min_send_quantum_bytes(mux_limits);
    let ceiling = BBR_MAX_SEND_QUANTUM_BYTES.min(cap).max(floor);
    let rate_bps = path
        .pacing_rate_bps
        .max(path.delivery_rate_bps)
        .max(path.product_progress_rate_bps.unwrap_or(0.0))
        .max(1.0);
    let quantum = (rate_bps / 8.0 * BBR_SEND_QUANTUM_INTERVAL.as_secs_f64()).ceil() as usize;
    quantum.clamp(floor, ceiling)
}

pub(in crate::runtime) fn relay_lane_min_chunk_bytes(
    path: Option<PathSnapshot>,
    lane: FlowLane,
    mux_limits: MuxLimits,
) -> usize {
    let cap = reliable_relay_buffer_len(mux_limits).max(1);
    let min_quantum = bbr_min_send_quantum_bytes(mux_limits);
    match lane {
        FlowLane::Control | FlowLane::RealtimeDatagram => min_quantum,
        FlowLane::Latency => PATH_OPEN_SCORE_BYTES.min(cap).max(min_quantum),
        FlowLane::Throughput | FlowLane::Background if path.is_none() => {
            PATH_OPEN_SCORE_BYTES.min(cap).max(min_quantum)
        }
        FlowLane::Throughput | FlowLane::Background => min_quantum,
    }
}

pub(in crate::runtime) fn relay_lane_startup_chunk_bytes(
    lane: FlowLane,
    mux_limits: MuxLimits,
) -> usize {
    let cap = reliable_relay_scheduler_quantum_cap(None, lane, mux_limits);
    let floor = relay_lane_min_chunk_bytes(None, lane, mux_limits);
    let target = match lane {
        FlowLane::Control | FlowLane::RealtimeDatagram => bbr_min_send_quantum_bytes(mux_limits),
        FlowLane::Latency => PATH_OPEN_SCORE_BYTES,
        FlowLane::Throughput | FlowLane::Background => reliable_startup_send_quantum_bytes(),
    };
    target.clamp(floor.min(cap).max(1), cap)
}

pub(in crate::runtime) fn reliable_relay_scheduler_quantum_cap(
    path: Option<PathSnapshot>,
    lane: FlowLane,
    mux_limits: MuxLimits,
) -> usize {
    let read_ceiling = reliable_relay_buffer_len(mux_limits).max(1);
    let Some(path) = path else {
        return read_ceiling;
    };
    if lane != FlowLane::Throughput {
        return read_ceiling;
    }
    let bdp = reliable_path_product_bdp_bytes(path);
    let condition_factor = reliable_path_quantum_condition_factor(path, bdp);
    (((read_ceiling as f64) * condition_factor)
        .ceil()
        .max(bbr_min_send_quantum_bytes(mux_limits) as f64) as usize)
        .min(read_ceiling)
        .max(1)
}

pub(in crate::runtime) fn reliable_lane_min_inflight_bytes(
    lane: FlowLane,
    mux_limits: MuxLimits,
) -> usize {
    let cap = mux_limits.max_path_flight_bytes.max(1);
    let min_pipe = bbr_min_pipe_cwnd_bytes(mux_limits);
    let initial_window = PATH_OPEN_SCORE_BYTES.min(cap).max(min_pipe);
    match lane {
        FlowLane::Control | FlowLane::RealtimeDatagram => min_pipe,
        FlowLane::Latency => initial_window,
        FlowLane::Throughput | FlowLane::Background => reliable_relay_buffer_len(mux_limits)
            .max(initial_window)
            .min(cap)
            .max(1),
    }
}

pub(in crate::runtime) fn reliable_lane_startup_inflight_bytes(
    lane: FlowLane,
    mux_limits: MuxLimits,
) -> usize {
    let cap = mux_limits.max_path_flight_bytes.max(1);
    let floor = reliable_lane_min_inflight_bytes(lane, mux_limits);
    let target = bbr_inflight_target_bytes_for_model(
        reliable_startup_bdp_bytes(),
        reliable_startup_send_quantum_bytes() as f64,
        bbr_min_pipe_cwnd_bytes(mux_limits) as f64,
        lane,
    );
    (target.ceil() as usize).clamp(floor.min(cap).max(1), cap)
}

pub(in crate::runtime) fn bbr_inflight_target_bytes(
    path: PathSnapshot,
    lane: FlowLane,
    mux_limits: MuxLimits,
) -> f64 {
    let bdp = reliable_path_product_bdp_bytes(path);
    let send_quantum = bbr_send_quantum_bytes(path, mux_limits) as f64;
    let min_pipe = bbr_min_pipe_cwnd_bytes(mux_limits) as f64;
    bbr_inflight_target_bytes_for_model(bdp, send_quantum, min_pipe, lane)
}

fn bbr_inflight_target_bytes_for_model(
    bdp: f64,
    send_quantum: f64,
    min_pipe: f64,
    lane: FlowLane,
) -> f64 {
    let bbr_window = (bdp * BBR_DEFAULT_CWND_GAIN)
        .max(send_quantum)
        .max(min_pipe);
    match lane {
        FlowLane::Control | FlowLane::RealtimeDatagram | FlowLane::Latency => {
            bbr_window.min(send_quantum.max(min_pipe))
        }
        FlowLane::Throughput | FlowLane::Background => bbr_window,
    }
}

fn reliable_startup_srtt_ms() -> f64 {
    RELIABLE_INITIAL_RTT.as_secs_f64() * 1000.0
}

fn reliable_startup_rate_bps() -> f64 {
    PATH_OPEN_SCORE_BYTES as f64 * 8.0 / RELIABLE_INITIAL_RTT.as_secs_f64()
}

pub(in crate::runtime) fn reliable_startup_bdp_bytes() -> f64 {
    reliable_startup_rate_bps() / 8.0 * (reliable_startup_srtt_ms() / 1000.0)
}

pub(in crate::runtime) fn reliable_startup_send_quantum_bytes() -> usize {
    bbr_send_quantum_bytes_for_rate(reliable_startup_rate_bps())
}

pub(in crate::runtime) fn reliable_path_stability_factor(path: PathSnapshot) -> f64 {
    let bdp_bytes = reliable_path_product_bdp_bytes(path);
    let min_pipe = (BBR_MIN_PIPE_CWND_PACKETS * TRANSPORT_MSS_BYTES) as f64;
    let floor = adaptive_transport_floor_factor(min_pipe, bdp_bytes);
    let loss_factor = (1.0 - path.loss_rate.clamp(0.0, 1.0)).max(floor);
    let srtt = path.srtt_ms.max(1.0);
    let jitter_factor = (srtt / (srtt + path.jitter_ms.max(0.0))).max(floor);
    loss_factor * jitter_factor
}

pub(in crate::runtime) fn reliable_path_queue_factor(path: PathSnapshot, bdp_bytes: f64) -> f64 {
    let queued = path.queue_bytes.saturating_add(path.bytes_in_flight) as f64;
    let floor = adaptive_transport_floor_factor(
        (BBR_MIN_PIPE_CWND_PACKETS * TRANSPORT_MSS_BYTES) as f64,
        bdp_bytes,
    );
    (bdp_bytes / (bdp_bytes + queued.max(0.0))).max(floor)
}

pub(in crate::runtime) fn reliable_path_backlog_factor(path: PathSnapshot, bdp_bytes: f64) -> f64 {
    let queued = path.queue_bytes as f64;
    let floor = adaptive_transport_floor_factor(
        bbr_send_quantum_bytes_for_rate(
            path.pacing_rate_bps
                .max(path.delivery_rate_bps)
                .max(path.product_progress_rate_bps.unwrap_or(0.0)),
        ) as f64,
        bdp_bytes,
    );
    (bdp_bytes / (bdp_bytes + queued.max(0.0))).max(floor)
}

pub(in crate::runtime) fn reliable_path_quantum_condition_factor(
    path: PathSnapshot,
    bdp_bytes: f64,
) -> f64 {
    let stability = reliable_path_stability_factor(path);
    let queue = reliable_path_queue_factor(path, bdp_bytes);
    let floor = adaptive_transport_floor_factor(
        bbr_send_quantum_bytes_for_rate(
            path.pacing_rate_bps
                .max(path.delivery_rate_bps)
                .max(path.product_progress_rate_bps.unwrap_or(0.0)),
        ) as f64,
        bdp_bytes,
    );
    (stability * queue.sqrt()).max(floor).min(1.0)
}

fn bbr_send_quantum_bytes_for_rate(rate_bps: f64) -> usize {
    let floor = BBR_MIN_SEND_QUANTUM_PACKETS * TRANSPORT_MSS_BYTES;
    let quantum =
        (rate_bps.max(1.0) / 8.0 * BBR_SEND_QUANTUM_INTERVAL.as_secs_f64()).ceil() as usize;
    quantum.clamp(floor, BBR_MAX_SEND_QUANTUM_BYTES)
}

fn adaptive_transport_floor_factor(minimum_bytes: f64, bdp_bytes: f64) -> f64 {
    let denominator = bdp_bytes.max(minimum_bytes).max(1.0);
    (minimum_bytes.max(1.0) / denominator).min(1.0)
}

pub(in crate::runtime) fn reliable_sender_effective_relay_lane(
    local: FlowLane,
    peer: FlowLane,
) -> FlowLane {
    if local == FlowLane::Throughput || peer == FlowLane::Throughput {
        FlowLane::Throughput
    } else if local == FlowLane::Background || peer == FlowLane::Background {
        FlowLane::Background
    } else {
        peer
    }
}

#[cfg(test)]
pub(in crate::runtime) fn prefix_repair_frames_with_available_output(
    path_stream: &ReliablePathStream,
    repair_frames: Vec<Frame>,
    allow_same_output_frontier_retransmit: bool,
) -> (Vec<Frame>, Option<u64>) {
    let (frames, blocked, _) = prefix_repair_frames_with_available_output_classified(
        path_stream,
        repair_frames,
        allow_same_output_frontier_retransmit,
    );
    (frames, blocked)
}

pub(in crate::runtime) fn prefix_final_tail_repair_frames_with_available_output(
    path_stream: &ReliablePathStream,
    repair_frames: Vec<Frame>,
) -> (Vec<Frame>, Option<u64>, bool) {
    prefix_repair_frames_with_available_output_classified(path_stream, repair_frames, true)
}

fn prefix_repair_frames_with_available_output_classified(
    path_stream: &ReliablePathStream,
    repair_frames: Vec<Frame>,
    allow_same_output_frontier_retransmit: bool,
) -> (Vec<Frame>, Option<u64>, bool) {
    let mut accepted = Vec::with_capacity(repair_frames.len());
    for frame in repair_frames {
        if !path_stream.has_repair_output_for_frame(&frame) {
            if allow_same_output_frontier_retransmit && accepted.is_empty() {
                accepted.push(frame);
                return (accepted, None, true);
            }
            return (
                accepted,
                reliable_stream_frame_extent(&frame).map(|(offset, _, _)| offset),
                false,
            );
        }
        accepted.push(frame);
    }
    (accepted, None, false)
}

fn prefix_live_owner_tail_repair_frames_with_available_output(
    path_stream: &ReliablePathStream,
    repair_frames: Vec<Frame>,
) -> (Vec<Frame>, Option<u64>) {
    let mut accepted = Vec::with_capacity(repair_frames.len());
    for frame in repair_frames {
        if !path_stream.has_live_owner_tail_repair_output_for_frame(&frame) {
            return (
                accepted,
                reliable_stream_frame_extent(&frame).map(|(offset, _, _)| offset),
            );
        }
        accepted.push(frame);
    }
    (accepted, None)
}

pub(in crate::runtime) fn prefix_repair_frames_with_failed_owner_output(
    path_stream: &ReliablePathStream,
    repair_frames: Vec<Frame>,
) -> (Vec<Frame>, Option<u64>) {
    let mut accepted = Vec::with_capacity(repair_frames.len());
    for frame in repair_frames {
        if !path_stream.has_failed_owner_repair_output_for_frame(&frame) {
            return (
                accepted,
                reliable_stream_frame_extent(&frame).map(|(offset, _, _)| offset),
            );
        }
        accepted.push(frame);
    }
    (accepted, None)
}

pub(in crate::runtime) fn prefix_repair_frames_with_unknown_owner_output(
    path_stream: &ReliablePathStream,
    repair_frames: Vec<Frame>,
) -> (Vec<Frame>, Option<u64>) {
    let mut accepted = Vec::with_capacity(repair_frames.len());
    for frame in repair_frames {
        if !path_stream.has_unknown_owner_repair_output_for_frame(&frame) {
            return (
                accepted,
                reliable_stream_frame_extent(&frame).map(|(offset, _, _)| offset),
            );
        }
        accepted.push(frame);
    }
    (accepted, None)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TailRepairEnqueueOutcome {
    queued: usize,
    pending: bool,
}

impl TailRepairEnqueueOutcome {
    fn record_as_repair_attempt(self) -> bool {
        let _ = self;
        true
    }
}

fn enqueue_reliable_tail_repair(
    response_sender: &mut ServerResponseSenderService,
    path_stream: &ReliablePathStream,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(unused_variables))] stream_id: StreamId,
    send_stream: &ReliableSendStream,
    last_send_ack_ranges: &[OffsetRange],
    last_send_ack_complete: bool,
    tail_repair_path_snapshot: Option<PathSnapshot>,
    relay_lane: FlowLane,
    mux_limits: MuxLimits,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(unused_variables))]
    performance: MppPerformanceConfig,
    max_frame_payload_bytes: usize,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(unused_variables))]
    last_send_ack_frontier: u64,
) -> TailRepairEnqueueOutcome {
    let base_repair_limit = adaptive_reliable_relay_chunk_bytes_with_frame_limit(
        tail_repair_path_snapshot,
        FlowLane::Throughput,
        mux_limits,
        max_frame_payload_bytes,
    )
    .max(adaptive_reliable_relay_repair_bytes(
        tail_repair_path_snapshot,
        relay_lane,
        mux_limits,
    ));
    let mut repair_limit = 0usize;
    let mut critical_tail_repair = false;
    let mut repair_kind = "none";
    let mut repair_cause = RelaySendCause::AckGapRepair;
    let mut repair_frames = Vec::new();
    let mut blocked_frontier_offset = None;
    let no_ack_frontier_failed_owner_tail = last_send_ack_ranges.is_empty()
        && last_send_ack_frontier == 0
        && send_stream.next_offset() > 0;
    if last_send_ack_complete || no_ack_frontier_failed_owner_tail {
        let event_repair_limit = reliable_critical_tail_repair_limit_bytes(
            base_repair_limit,
            send_stream.repair_bytes(),
            mux_limits,
        );
        let failed_owner_source_frames = if last_send_ack_ranges.is_empty() {
            send_stream.retransmission_frames_for_ranges(
                &[OffsetRange {
                    start: 0,
                    end: send_stream.next_offset(),
                }],
                event_repair_limit,
            )
        } else {
            send_stream.retransmission_frames_for_ranges(
                &[OffsetRange {
                    start: last_send_ack_frontier,
                    end: send_stream.next_offset(),
                }],
                event_repair_limit,
            )
        };
        let (failover_frames, failover_blocked_offset) =
            prefix_repair_frames_with_failed_owner_output(path_stream, failed_owner_source_frames);
        if !failover_frames.is_empty() {
            critical_tail_repair = true;
            repair_limit = event_repair_limit;
            repair_frames = failover_frames;
            blocked_frontier_offset = failover_blocked_offset;
            repair_kind = "failed_owner_tail_repair";
            repair_cause = RelaySendCause::PathFailureRepair;
        } else if blocked_frontier_offset.is_none() {
            blocked_frontier_offset = failover_blocked_offset;
        }
        if repair_frames.is_empty() && last_send_ack_complete && last_send_ack_frontier > 0 {
            let tail_limit = reliable_critical_tail_repair_limit_bytes(
                base_repair_limit,
                send_stream.repair_bytes(),
                mux_limits,
            );
            let tail_source_frames = send_stream.retransmission_frames_for_ranges(
                &[OffsetRange {
                    start: last_send_ack_frontier,
                    end: send_stream.next_offset(),
                }],
                tail_limit,
            );
            let (unknown_owner_frames, unknown_owner_blocked_offset) =
                prefix_repair_frames_with_unknown_owner_output(
                    path_stream,
                    tail_source_frames.clone(),
                );
            if !unknown_owner_frames.is_empty() {
                critical_tail_repair = true;
                repair_limit = tail_limit;
                repair_frames = unknown_owner_frames;
                blocked_frontier_offset = unknown_owner_blocked_offset;
                repair_kind = "tail_unknown_owner";
                repair_cause = RelaySendCause::PathFailureRepair;
            } else if blocked_frontier_offset.is_none() {
                blocked_frontier_offset = unknown_owner_blocked_offset;
            }
        }
        if repair_frames.is_empty()
            && stream_ack_is_authoritative_contiguous_prefix(
                last_send_ack_complete,
                last_send_ack_ranges,
                last_send_ack_frontier,
            )
            && path_stream.has_multipath_repair_alternative()
        {
            let tail_limit = reliable_live_owner_tail_repair_limit_bytes(
                tail_repair_path_snapshot,
                path_stream.tail_repair_owner_underlay(last_send_ack_frontier),
                relay_lane,
                send_stream.repair_bytes(),
                mux_limits,
            );
            let tail_source_frames = send_stream
                .retransmission_frames_after_ack_frontier(last_send_ack_ranges, tail_limit);
            let (tail_repair_frames, tail_repair_blocked_offset) =
                prefix_live_owner_tail_repair_frames_with_available_output(
                    path_stream,
                    tail_source_frames,
                );
            if !tail_repair_frames.is_empty() {
                critical_tail_repair = true;
                repair_limit = tail_limit;
                repair_frames = tail_repair_frames;
                blocked_frontier_offset = tail_repair_blocked_offset;
                repair_kind = "tail_repair";
                // A live carrier still owns recovery for its original flight.
                // Product tail repair may race it only on a distinct output.
                repair_cause = RelaySendCause::LiveOwnerTailRepair;
            } else if blocked_frontier_offset.is_none() {
                blocked_frontier_offset = tail_repair_blocked_offset;
            }
        }
    }
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = base_repair_limit;
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = repair_kind;
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = blocked_frontier_offset;
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = repair_limit;
    #[cfg(feature = "lab-diagnostics")]
    lab_diagnostic(
        "tail_stall_repair",
        format_args!(
            "stream_id={} lane={:?} ack_frontier={} sent_offset={} repair_bytes={} repair_frames={} blocked_frontier_offset={:?} base_repair_limit={} repair_limit={} extra_traffic_hint_percent={} repair_kind={}",
            stream_id.0,
            relay_lane,
            last_send_ack_frontier,
            send_stream.next_offset(),
            send_stream.repair_bytes(),
            repair_frames.len(),
            blocked_frontier_offset,
            base_repair_limit,
            repair_limit,
            performance.extra_traffic_hint_percent,
            repair_kind,
        ),
    );
    let mut repair_count = 0usize;
    let mut repair_pending = false;
    let live_repair_retry_after =
        reliable_relay_tail_repair_delay(tail_repair_path_snapshot, relay_lane);
    for frame in repair_frames {
        if response_sender.has_queued_repair_overlap(&frame)
            || path_stream.has_recent_live_repair_flight_overlap(&frame, live_repair_retry_after)
        {
            repair_pending = true;
            continue;
        }
        let queued = if critical_tail_repair {
            Some(response_sender.enqueue_critical_repair_frame_with_cause(frame, repair_cause))
        } else {
            response_sender.enqueue_repair_frame_with_priority(frame, mux_limits, true)
        };
        if queued.is_some() {
            repair_count = repair_count.saturating_add(1);
        }
    }
    TailRepairEnqueueOutcome {
        queued: repair_count,
        pending: repair_pending,
    }
}

#[allow(clippy::too_many_arguments)]
async fn drain_server_response_sender_ready(
    response_sender: &mut ServerResponseSenderService,
    path_stream: &ReliablePathStream,
    mut ordered_owner_debt_bytes: usize,
    send_stream: &mut ReliableSendStream,
    relay_lane: FlowLane,
    mux_limits: MuxLimits,
    sender_dispatch_byte_budget: usize,
    sender_dispatch_item_budget: usize,
    stats: &mut PathDeliveryStats,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(unused_variables))] session_id: SessionId,
) -> Result<bool, RuntimeError> {
    let mut dispatched_items = 0usize;
    let mut dispatched_payload_bytes = 0usize;
    let mut blocked_by_carrier = false;

    while response_sender.queued_send_ready()
        && dispatched_items < sender_dispatch_item_budget
        && (dispatched_payload_bytes < sender_dispatch_byte_budget || dispatched_items == 0)
    {
        let dispatch = match response_sender.dispatch_next_with_ordered_owner_debt(
            path_stream,
            send_stream,
            relay_lane,
            mux_limits,
            ordered_owner_debt_bytes,
        ) {
            Ok(dispatch) => dispatch,
            Err(RuntimeError::SenderServiceBlocked) => {
                blocked_by_carrier = true;
                break;
            }
            Err(err) => return Err(err),
        };
        dispatched_items = dispatched_items.saturating_add(1);
        if dispatch.lane == ReliableWorkClass::Repair {
            #[cfg(feature = "lab-diagnostics")]
            {
                let (selected_underlay, selected_path_id) = dispatch
                    .selected_path
                    .map(|path| (format!("{:?}", path.underlay), path.path_id.0.to_string()))
                    .unwrap_or_else(|| ("none".to_string(), "none".to_string()));
                lab_diagnostic(
                    "repair_frame_dispatched",
                    format_args!(
                        "session_id={} stream_id={} path_underlay={} path_id={} payload_bytes={}",
                        session_id.0,
                        path_stream.stream_id.0,
                        selected_underlay,
                        selected_path_id,
                        dispatch.payload_bytes,
                    ),
                );
            }
        } else {
            dispatched_payload_bytes =
                dispatched_payload_bytes.saturating_add(dispatch.payload_bytes);
            stats.record_payload_bytes(dispatch.payload_bytes);
            if dispatch.lane == ReliableWorkClass::Data {
                ordered_owner_debt_bytes =
                    ordered_owner_debt_bytes.saturating_add(dispatch.payload_bytes);
            }
        }
    }

    #[cfg(feature = "lab-diagnostics")]
    if dispatched_items > 0 {
        lab_diagnostic(
            "server_sender_drain",
            format_args!(
                "session_id={} stream_id={} lane={:?} dispatches={} payload_bytes={} byte_budget={} item_budget={} queue_bytes_after={} blocked_by_carrier={}",
                session_id.0,
                path_stream.stream_id.0,
                relay_lane,
                dispatched_items,
                dispatched_payload_bytes,
                sender_dispatch_byte_budget,
                sender_dispatch_item_budget,
                response_sender.bytes(),
                blocked_by_carrier,
            ),
        );
    }

    if dispatched_payload_bytes > 0 {
        tokio::task::yield_now().await;
    }

    Ok(blocked_by_carrier)
}

pub(in crate::runtime) async fn relay_reliable_stream<S>(
    mut local: S,
    mut path_stream: ReliablePathStream,
    mux_limits: MuxLimits,
    performance: MppPerformanceConfig,
    session_id: SessionId,
) -> Result<PathDeliveryStats, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let stream_id = path_stream.stream_id;
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = session_id;
    let mut send_stream = ReliableSendStream::new_with_initial_max_offset(stream_id, mux_limits, 0);
    send_stream.update_max_offset(path_stream.max_offset);
    let mut recv_stream = ReliableRecvStream::new(stream_id, mux_limits);
    let chunk_size = adaptive_reliable_relay_chunk_bytes_with_frame_limit(
        None,
        FlowLane::Latency,
        mux_limits,
        path_stream.max_frame_payload_bytes,
    );
    let mut buf = bytes::BytesMut::with_capacity(chunk_size);
    let mut local_open = true;
    let mut remote_open = true;
    let mut stats = PathDeliveryStats::default();
    let mut close_sent = false;
    let mut terminal_fin_replayed = false;
    let mut pending_local_fin = false;
    let mut pending_remote_fin_offset = None;
    let mut recv_progress = ReliableRecvProgress::default();
    let mut request_sparse_ack_progress = RequestTcpSparseAckProgress::default();
    let mut ack_gap_repair = ReliableAckGapRepairProgress::default();
    let mut last_recv_progress_sent_at = Instant::now();
    let mut last_send_ack_progress_at = Instant::now();
    let mut last_tail_repair_at = Instant::now();
    let mut last_send_ack_frontier = 0_u64;
    let mut last_send_ack_ranges = Vec::<OffsetRange>::new();
    let mut last_send_ack_complete = false;
    let mut flow_demand = ReliableRelayFlowDemandTracker::new();
    let mut output_updates = path_stream.subscribe_output_updates();
    let mut multipath_repair_alternative_available = path_stream.has_multipath_repair_alternative();
    let mut response_sender =
        ServerResponseSenderService::new_with_performance(session_id, stream_id, performance);
    let mut response_sender_retry_at: Option<tokio::time::Instant> = None;
    let mut last_relay_lane = path_stream.current_lane();
    let mut last_sender_dispatch_byte_budget =
        relay_lane_startup_chunk_bytes(last_relay_lane, mux_limits)
            .min(path_stream.max_frame_payload_bytes)
            .max(1);
    let mut last_sender_dispatch_item_budget = 1usize;
    #[cfg(feature = "lab-diagnostics")]
    let mut last_reported_budget: Option<(FlowLane, usize, usize)> = None;
    #[cfg(feature = "lab-diagnostics")]
    let mut reported_former_source_staging_block = false;
    #[cfg(feature = "lab-diagnostics")]
    let mut receive_hole_diagnostics = ServerReceiveHoleDiagnostics::default();

    let result = loop {
        if stream_terminal_fin_replay_required(
            close_sent,
            terminal_fin_replayed,
            response_sender.is_empty(),
            send_stream.repair_bytes(),
            last_send_ack_frontier,
            send_stream.next_offset(),
        ) {
            response_sender.enqueue_final_control_frame(Frame::StreamFin {
                stream_id,
                final_offset: send_stream.next_offset(),
            });
            response_sender_retry_at = None;
            terminal_fin_replayed = true;
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "terminal_fin_replay",
                format_args!(
                    "stream_id={} final_offset={} ack_frontier={} repair_bytes=0 role=server",
                    stream_id.0,
                    send_stream.next_offset(),
                    last_send_ack_frontier,
                ),
            );
        }
        if !local_open
            && !remote_open
            && send_stream.repair_bytes() == 0
            && response_sender.is_empty()
            && (!pending_local_fin || close_sent)
        {
            break Ok(stats);
        }
        let peer_lane = path_stream.current_lane();
        let demand_update = flow_demand.refresh(
            ReliableRelayFlowSignals::new(
                send_stream
                    .next_offset()
                    .saturating_add(response_sender.data_bytes() as u64),
                recv_stream.next_offset(),
                send_stream.repair_bytes(),
            ),
            None,
            mux_limits,
        );
        let relay_demand = demand_update.demand;
        let relay_lane = reliable_sender_effective_relay_lane(relay_demand.lane, peer_lane);
        if relay_lane != peer_lane {
            path_stream.set_lane(relay_lane);
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "server_stream_lane_promoted_local",
                format_args!(
                    "stream_id={} previous={:?} local_lane={:?} peer_lane={:?} lane={:?} latency_weight_ppm={} throughput_weight_ppm={} sent_offset={} received_offset={} repair_bytes={}",
                    stream_id.0,
                    demand_update.previous_lane,
                    demand_update.demand.lane,
                    peer_lane,
                    relay_lane,
                    demand_update.demand.latency_weight_ppm,
                    demand_update.demand.throughput_weight_ppm,
                    send_stream.next_offset(),
                    recv_stream.next_offset(),
                    send_stream.repair_bytes(),
                ),
            );
        }
        response_sender.publish_queue_bytes(&path_stream);
        let payload_hint = relay_lane_startup_chunk_bytes(relay_lane, mux_limits)
            .min(path_stream.max_frame_payload_bytes);
        let (send_path_snapshot, source_staging_context) = match &path_stream.output {
            ReliablePathStreamOutput::Switchable(binding) => {
                let read = binding.relay_read_snapshot(relay_lane, payload_hint);
                (
                    read.send_path,
                    ReliableSourceStagingContext {
                        independent: read.independent_source_staging,
                        service: read.source_service.map(|service| {
                            ReliableSourceServiceStagingContext {
                                allows_product_envelope: true,
                                has_latency_pressure: service.active_latency_sensitive_flows > 0,
                                has_feed_evidence: service.has_service_feed_evidence,
                            }
                        }),
                    },
                )
            }
            ReliablePathStreamOutput::Fixed(_) => {
                let path = path_stream.send_path_snapshot(relay_lane, payload_hint);
                // Fixed request-side output retains its path-local progress
                // graduation. Switchable response Service staging uses the
                // canonical carrier-specific bulk predicate above.
                (
                    path,
                    ReliableSourceStagingContext {
                        independent: false,
                        service: path.map(|snapshot| ReliableSourceServiceStagingContext {
                            // Fixed request-side outputs do not expose response
                            // owner cardinality; retain bounded staging.
                            allows_product_envelope: false,
                            has_latency_pressure: snapshot.active_latency_sensitive_flows > 0,
                            has_feed_evidence: snapshot.product_progress_rate_bps.is_some()
                                && snapshot.confidence >= 1.0,
                        }),
                    },
                )
            }
        };
        let tail_repair_path_snapshot = path_stream.tail_repair_snapshot(
            last_send_ack_frontier,
            relay_lane,
            relay_lane_startup_chunk_bytes(relay_lane, mux_limits)
                .min(path_stream.max_frame_payload_bytes),
        );
        let request_active_path_snapshot = path_stream.request_active_path_snapshot(relay_lane);
        let request_active_underlay = request_active_path_snapshot
            .map(|snapshot| snapshot.underlay)
            .or_else(|| path_stream.request_active_underlay())
            .unwrap_or(path_stream.underlay);
        let recv_progress_deadline = tokio::time::Instant::from_std(
            last_recv_progress_sent_at
                + reliable_stream_recv_progress_interval(request_active_path_snapshot, relay_lane),
        );
        let has_tail_repair_alternative = path_stream.has_multipath_repair_alternative();
        let failed_owner_tail_repair_candidate = path_stream.can_attempt_failed_owner_tail_repair()
            && last_send_ack_frontier < send_stream.next_offset();
        let failed_owner_tail_repair_ready = failed_owner_tail_repair_candidate
            && reliable_failed_owner_tail_repair_ready(
                &path_stream,
                &send_stream,
                &last_send_ack_ranges,
                last_send_ack_complete,
                last_send_ack_frontier,
                mux_limits,
            );
        let live_owner_tail_repair_candidate = has_tail_repair_alternative
            && last_send_ack_frontier < send_stream.next_offset()
            && stream_ack_is_authoritative_contiguous_prefix(
                last_send_ack_complete,
                &last_send_ack_ranges,
                last_send_ack_frontier,
            );
        let tail_repair_active = reliable_relay_tail_repair_timer_active(
            send_stream.repair_bytes(),
            live_owner_tail_repair_candidate,
            failed_owner_tail_repair_ready,
        );
        let ordered_owner_debt_bytes = reliable_relay_current_ordered_owner_debt_bytes(
            relay_lane,
            &send_stream,
            last_send_ack_complete,
            last_send_ack_frontier,
        );
        let tail_repair_deadline = reliable_relay_effective_tail_repair_deadline(
            last_send_ack_progress_at,
            last_tail_repair_at,
            tail_repair_path_snapshot,
            relay_lane,
            failed_owner_tail_repair_ready,
        );
        let adaptive_chunk = adaptive_reliable_relay_chunk_bytes_with_frame_limit(
            send_path_snapshot,
            relay_lane,
            mux_limits,
            path_stream.max_frame_payload_bytes,
        );
        let inflight_limit =
            adaptive_reliable_relay_inflight_bytes(send_path_snapshot, relay_lane, mux_limits);
        let sender_queue_limit = reliable_relay_sender_queue_limit(mux_limits, inflight_limit);
        let latency_owner_credit = reliable_latency_startup_owner_credit_remaining_bytes(
            relay_lane,
            send_stream.next_offset(),
            response_sender.data_bytes(),
            mux_limits,
        );
        let owner_tail_read_headroom = reliable_relay_source_staging_owner_tail_headroom(
            source_staging_context,
            relay_lane,
            ordered_owner_debt_bytes,
            response_sender.data_bytes(),
            reliable_bulk_carrier_feed_quantum_bytes(mux_limits),
            mux_limits,
        );
        // Mixed-family response bytes have neither offsets nor owners while
        // staged here; ordered-owner and path admission remain dispatch gates.
        let source_read_ceiling = reliable_relay_buffer_len(mux_limits)
            .min(path_stream.max_frame_payload_bytes)
            .min(sender_queue_limit)
            .min(latency_owner_credit)
            .min(owner_tail_read_headroom);
        #[cfg(feature = "lab-diagnostics")]
        if source_staging_context.independent && !reported_former_source_staging_block {
            let former_owner_tail_read_headroom = reliable_relay_source_staging_owner_tail_headroom(
                ReliableSourceStagingContext {
                    independent: false,
                    ..source_staging_context
                },
                relay_lane,
                ordered_owner_debt_bytes,
                response_sender.data_bytes(),
                reliable_bulk_carrier_feed_quantum_bytes(mux_limits),
                mux_limits,
            );
            if former_owner_tail_read_headroom == 0 && source_read_ceiling > 0 {
                lab_diagnostic(
                    "server_source_staging_policy",
                    format_args!(
                        "session_id={} stream_id={} lane={:?} former_policy_blocked=true actual_headroom={} source_read_ceiling={} assigned_owner_tail_bytes={} raw_queue_bytes={} sender_queue_limit={} repair_bytes={} next_offset={} ack_frontier={}",
                        session_id.0,
                        stream_id.0,
                        relay_lane,
                        owner_tail_read_headroom,
                        source_read_ceiling,
                        ordered_owner_debt_bytes,
                        response_sender.data_bytes(),
                        sender_queue_limit,
                        send_stream.repair_bytes(),
                        send_stream.next_offset(),
                        last_send_ack_frontier,
                    ),
                );
                reported_former_source_staging_block = true;
            }
        }
        if source_read_ceiling > 0 {
            resize_reliable_relay_buffer(&mut buf, source_read_ceiling);
        }
        let (sender_dispatch_byte_budget, sender_dispatch_item_budget) =
            reliable_relay_sender_dispatch_budget(
                mux_limits,
                relay_lane,
                adaptive_chunk,
                inflight_limit,
                sender_queue_limit,
            );
        last_relay_lane = relay_lane;
        last_sender_dispatch_byte_budget = sender_dispatch_byte_budget;
        last_sender_dispatch_item_budget = sender_dispatch_item_budget;
        #[cfg(feature = "lab-diagnostics")]
        if last_reported_budget != Some((relay_lane, adaptive_chunk, inflight_limit)) {
            let snapshot = send_path_snapshot;
            lab_diagnostic(
                "server_relay_budget",
                format_args!(
                    "stream_id={} underlay={:?} lane={:?} chunk_bytes={} inflight_bytes={} max_frame_payload_bytes={} snapshot={} rate_mbps={:.3} pacing_mbps={:.3} product_progress_mbps={:.3} queue_bytes={} product_queue_bytes={} carrier_flight_bytes={} product_flight_bytes={} confidence_ppm={}",
                    stream_id.0,
                    path_stream.underlay,
                    relay_lane,
                    adaptive_chunk,
                    inflight_limit,
                    path_stream.max_frame_payload_bytes,
                    snapshot.is_some(),
                    snapshot.map_or(0.0, |path| path.delivery_rate_bps / 1_000_000.0),
                    snapshot.map_or(0.0, |path| path.pacing_rate_bps / 1_000_000.0),
                    snapshot
                        .and_then(|path| path.product_progress_rate_bps)
                        .unwrap_or(0.0)
                        / 1_000_000.0,
                    snapshot.map_or(0, |path| path.queue_bytes),
                    snapshot.map_or(0, |path| path.product_queue_bytes),
                    snapshot.map_or(0, |path| path.bytes_in_flight),
                    snapshot.map_or(0, |path| path.product_bytes_in_flight),
                    snapshot.map_or(0, |path| (path.confidence.clamp(0.0, 1.0) * 1_000_000.0)
                        .round() as u32),
                ),
            );
            last_reported_budget = Some((relay_lane, adaptive_chunk, inflight_limit));
        }
        let now = tokio::time::Instant::now();
        response_sender.discard_unusable_live_owner_tail_repairs(&path_stream);
        if response_sender.discard_stale_persistent_ack_gap_repairs(&path_stream) > 0 {
            ack_gap_repair.release_repair_attempt();
            response_sender_retry_at = None;
        }
        if response_sender_retry_at.is_some_and(|deadline| deadline <= now) {
            response_sender_retry_at = None;
        }
        let queued_front_has_carrier_credit = response_sender
            .front_has_carrier_credit_with_ordered_owner_debt(
                &path_stream,
                &send_stream,
                relay_lane,
                mux_limits,
                ordered_owner_debt_bytes,
            );
        let sender_wait = response_sender_wait_state(
            !response_sender.is_empty(),
            response_sender.queued_send_ready(),
            queued_front_has_carrier_credit,
            response_sender_retry_at,
            now,
            sender_service_retry_delay(send_path_snapshot, relay_lane),
        );
        response_sender_retry_at = sender_wait.retry_at;
        let queued_send_blocked = sender_wait.blocked;
        let queued_send_ready = sender_wait.ready;
        let queued_send_retry_deadline = sender_wait.retry_at.unwrap_or(now);
        let carrier_capacity_notifies = if sender_wait.subscribe_capacity {
            path_stream.capacity_notifies()
        } else {
            Vec::new()
        };
        let has_carrier_capacity_notify = !carrier_capacity_notifies.is_empty();
        let drain_allows_bounded_source_staging =
            response_sender.drain_allows_bounded_source_staging(&path_stream, queued_send_blocked);
        let queued_send_blocks_source_read =
            queued_send_blocked && !drain_allows_bounded_source_staging;
        let can_read_by_flow = source_read_ceiling > 0
            && owner_tail_read_headroom > 0
            && response_sender.can_read_product_source(
                local_open,
                queued_send_blocks_source_read,
                &send_stream,
                mux_limits,
                sender_queue_limit,
            );
        let read_budget = if can_read_by_flow {
            response_sender.read_budget(
                &send_stream,
                mux_limits,
                sender_queue_limit,
                source_read_ceiling,
            )
        } else {
            0
        };
        let can_read_local = can_read_by_flow && read_budget > 0;
        let can_send_pending_fin = pending_local_fin && response_sender.is_empty() && !close_sent;

        // Carrier input and target responses can both remain continuously
        // ready during an upload. Fair polling keeps response progress from
        // being hidden behind an unbounded run of incoming STREAM_DATA.
        tokio::select! {
            _ = tokio::time::sleep_until(tail_repair_deadline), if tail_repair_active => {
                let repair_outcome = enqueue_reliable_tail_repair(
                    &mut response_sender,
                    &path_stream,
                    stream_id,
                    &send_stream,
                    &last_send_ack_ranges,
                    last_send_ack_complete,
                    tail_repair_path_snapshot,
                    relay_lane,
                    mux_limits,
                    performance,
                    path_stream.max_frame_payload_bytes,
                    last_send_ack_frontier,
                );
                if repair_outcome.record_as_repair_attempt() {
                    last_tail_repair_at = Instant::now();
                }
                let ordered_owner_debt_bytes = reliable_relay_current_ordered_owner_debt_bytes(
                    relay_lane,
                    &send_stream,
                    last_send_ack_complete,
                    last_send_ack_frontier,
                );
                if drain_server_response_sender_ready(
                    &mut response_sender,
                    &path_stream,
                    ordered_owner_debt_bytes,
                    &mut send_stream,
                    relay_lane,
                    mux_limits,
                    sender_dispatch_byte_budget,
                    sender_dispatch_item_budget,
                    &mut stats,
                    session_id,
                )
                .await?
                {
                    response_sender_retry_at =
                        Some(tokio::time::Instant::now() + sender_service_retry_delay(send_path_snapshot, relay_lane));
                }
                continue;
            }
            frame = async {
                #[cfg(feature = "lab-diagnostics")]
                let recv_started = Instant::now();
                let result = path_stream.recv_frame().await;
                #[cfg(feature = "lab-diagnostics")]
                if let Ok(frame) = &result {
                    lab_perf_record("relay.path_recv_frame_wait", recv_started.elapsed(), frame_pacing_bytes(frame));
                }
                result
            }, if remote_open || send_stream.repair_bytes() > 0 => {
                let frame = frame?;
                response_sender_retry_at = None;
                match frame {
                    Frame::StreamData {
                        stream_id: received_stream_id,
                        offset,
                        flags,
                        payload,
                    } if received_stream_id == stream_id && remote_open => {
                        #[cfg(feature = "lab-diagnostics")]
                        let payload_len = payload.len();
                        #[cfg(feature = "lab-diagnostics")]
                        let mux_started = Instant::now();
                        let outcome = recv_stream.receive_data(offset, payload, flags)?;
                        #[cfg(feature = "lab-diagnostics")]
                        lab_perf_record("mux.receive_data", mux_started.elapsed(), payload_len);
                        let delivered = outcome.delivered;
                        #[cfg(feature = "lab-diagnostics")]
                        receive_hole_diagnostics.observe(
                            stream_id,
                            &recv_stream,
                            delivered.iter().map(|chunk| chunk.len()).sum(),
                        );
                        for chunk in delivered.iter() {
                            stats.record_payload_bytes(chunk.len());
                        }
                        #[cfg(feature = "lab-diagnostics")]
                        let write_started = Instant::now();
                        let written = write_delivered_payloads(&mut local, delivered.as_slice()).await?;
                        #[cfg(feature = "lab-diagnostics")]
                        lab_perf_record("relay.local_write_wait", write_started.elapsed(), written);
                        #[cfg(not(feature = "lab-diagnostics"))]
                        let _ = written;
                        if !delivered.is_empty() {
                            #[cfg(feature = "lab-diagnostics")]
                            let flush_started = Instant::now();
                            local.flush().await?;
                            #[cfg(feature = "lab-diagnostics")]
                            lab_perf_record("relay.local_flush_wait", flush_started.elapsed(), 0);
                        }
                        if enqueue_tcp_recv_progress(
                            &mut response_sender,
                            &recv_stream,
                            &mut recv_progress,
                            &mut request_sparse_ack_progress,
                            request_active_path_snapshot,
                            relay_lane,
                            mux_limits,
                            false,
                        )
                        {
                            response_sender_retry_at = None;
                            last_recv_progress_sent_at = Instant::now();
                        }
                        if outcome.fin
                            || pending_stream_fin_ready(&recv_stream, pending_remote_fin_offset)
                        {
                            if enqueue_tcp_recv_progress(
                                &mut response_sender,
                                &recv_stream,
                                &mut recv_progress,
                                &mut request_sparse_ack_progress,
                                request_active_path_snapshot,
                                relay_lane,
                                mux_limits,
                                true,
                            ) {
                                response_sender_retry_at = None;
                                last_recv_progress_sent_at = Instant::now();
                            }
                            local.shutdown().await?;
                            remote_open = false;
                            pending_remote_fin_offset = None;
                        }
                    }
                    Frame::StreamAck {
                        stream_id: ack_stream_id,
                        complete,
                        ranges,
                    } if ack_stream_id == stream_id => {
                        let normalized_ranges = normalized_offset_ranges(&ranges);
                        #[cfg(feature = "lab-diagnostics")]
                        let mux_started = Instant::now();
                        let ack = send_stream.apply_normalized_ack(&normalized_ranges);
                        if ack.released_bytes > 0 {
                            response_sender.record_owner_progress(ack.released_bytes);
                        }
                        #[cfg(feature = "lab-diagnostics")]
                        lab_perf_record("mux.apply_ack", mux_started.elapsed(), ack.released_bytes);
                        path_stream.release_normalized_acked_ranges(&normalized_ranges);
                        response_sender.release_normalized_acked_repairs(&normalized_ranges);
                        #[cfg(feature = "lab-diagnostics")]
                        let largest_ack_end = normalized_ranges.last().map_or(0, |range| range.end);
                        #[cfg(feature = "lab-diagnostics")]
                        let incoming_ack_frontier =
                            stream_ack_contiguous_frontier(complete, &normalized_ranges);
                        let previous_ack_frontier = last_send_ack_frontier;
                        update_repair_authoritative_ack_snapshot(
                            &mut last_send_ack_frontier,
                            &mut last_send_ack_ranges,
                            &mut last_send_ack_complete,
                            complete,
                            &normalized_ranges,
                        );
                        let ack_made_progress = last_send_ack_frontier > previous_ack_frontier;
                        if ack_made_progress {
                            last_send_ack_progress_at = Instant::now();
                            last_tail_repair_at = last_send_ack_progress_at;
                        }
                        let base_repair_limit = adaptive_reliable_relay_repair_bytes(
                            send_path_snapshot,
                            relay_lane,
                            mux_limits,
                        );
                        let repair_event_budget =
                            response_sender.repair_extra_event_budget_remaining(mux_limits);
                        let has_multipath_repair_alternative =
                            path_stream.has_multipath_repair_alternative();
                        let repair_owner_underlay = path_stream
                            .tail_repair_owner_underlay(last_send_ack_frontier);
                        let ack_gap_repair_ready = ack_gap_repair.repair_ready(
                            complete,
                            &normalized_ranges,
                            repair_owner_underlay,
                            has_multipath_repair_alternative,
                            tail_repair_path_snapshot,
                            relay_lane,
                        );
                        let repair_target = ack_gap_repair_ready
                            .then(|| {
                                response_sender.ack_gap_repair_path_snapshot(
                                    &path_stream,
                                    &send_stream,
                                    &normalized_ranges,
                                    base_repair_limit,
                                )
                            })
                            .flatten();
                        let repair_path = repair_target.map(|(_, snapshot)| snapshot);
                        let repair_limit = if ack_gap_repair_ready {
                            reliable_persistent_ack_gap_repair_limit_bytes(
                                repair_path,
                                repair_path.and(repair_owner_underlay),
                                relay_lane,
                                send_stream.repair_bytes(),
                                mux_limits,
                            )
                        } else {
                            base_repair_limit.min(repair_event_budget)
                        };
                        let amplified_ack_gap_repair = ack_gap_repair_ready
                            && repair_limit > base_repair_limit;
                        let ack_gap_repair_cause = if amplified_ack_gap_repair {
                            let (target, snapshot) = repair_target
                                .expect("amplified repair requires a modeled output");
                            RelaySendCause::persistent_server_ack_gap_repair(
                                target,
                                snapshot,
                                relay_lane,
                            )
                        } else {
                            RelaySendCause::AckGapRepair
                        };
                        let mut repair_frames = stream_ack_gap_repair_frames_normalized(
                            &send_stream,
                            &normalized_ranges,
                            repair_limit,
                            complete,
                            has_multipath_repair_alternative,
                            ack_gap_repair_ready,
                        );
                        let mut critical_tail_repair =
                            ack_gap_repair_ready && !repair_frames.is_empty();
                        let repair_kind = if repair_frames.is_empty() {
                            let fin_tail_stall_ready =
                                tokio::time::Instant::now() >= tail_repair_deadline
                                    && !ack_made_progress;
                            let fin_tail_ready = close_sent || pending_local_fin;
                            let fin_tail_limit = if fin_tail_ready {
                                let limit = reliable_critical_tail_repair_limit_bytes(
                                    base_repair_limit,
                                    send_stream.repair_bytes(),
                                    mux_limits,
                                );
                                critical_tail_repair = reliable_critical_tail_repair_is_over_budget(
                                    repair_event_budget,
                                    limit,
                                );
                                limit
                            } else {
                                repair_limit
                            };
                            let (
                                fin_tail_frames,
                                blocked_frontier_offset,
                                _same_output_frontier_retransmit,
                            ) = prefix_final_tail_repair_frames_with_available_output(
                                &path_stream,
                                stream_final_offset_tail_repair_frames(
                                    &send_stream,
                                    &ranges,
                                    fin_tail_limit,
                                    fin_tail_ready,
                                    fin_tail_stall_ready,
                                ),
                            );
                            #[cfg(feature = "lab-diagnostics")]
                            if blocked_frontier_offset.is_some() {
                                lab_diagnostic(
                                    "tail_stall_repair_blocked_frontier",
                                    format_args!(
                                        "stream_id={} blocked_frontier_offset={:?} repair_kind=fin_tail",
                                        stream_id.0, blocked_frontier_offset,
                                    ),
                                );
                            }
                            #[cfg(not(feature = "lab-diagnostics"))]
                            let _ = blocked_frontier_offset;
                            if fin_tail_frames.is_empty() {
                                "ack_gap"
                            } else {
                                repair_frames = fin_tail_frames;
                                "fin_tail"
                            }
                        } else {
                            "ack_gap"
                        };
                        #[cfg(not(feature = "lab-diagnostics"))]
                        let _ = repair_kind;
                        let live_repair_retry_after =
                            reliable_relay_tail_repair_delay(tail_repair_path_snapshot, relay_lane);
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "stream_ack_received",
                            format_args!(
                                "stream_id={} complete={} ranges={} incoming_frontier={} stored_frontier={} largest_end={} released_bytes={} sent_offset={} sender_queue_bytes={} repair_bytes_after={} repair_frames={} repair_kind={} active_underlay={:?} multipath_repair_alternative={} ack_gap_repair_ready={} base_repair_limit={} repair_limit={} extra_traffic_hint_percent={}",
                                stream_id.0,
                                complete,
                                ranges.len(),
                                incoming_ack_frontier,
                                last_send_ack_frontier,
                                largest_ack_end,
                                ack.released_bytes,
                                send_stream.next_offset(),
                                response_sender.bytes(),
                                ack.remaining_repair_bytes,
                                repair_frames.len(),
                                repair_kind,
                                Some(path_stream.underlay),
                                has_multipath_repair_alternative,
                                ack_gap_repair_ready,
                                base_repair_limit,
                                repair_limit,
                                performance.extra_traffic_hint_percent,
                            ),
                        );
                        let mut queued_persistent_ack_gap_repair = false;
                        for frame in repair_frames {
                            let queued = if path_stream.has_recent_live_repair_flight_overlap(
                                &frame,
                                live_repair_retry_after,
                            ) || response_sender.has_queued_repair_overlap(&frame)
                            {
                                false
                            } else if critical_tail_repair {
                                if repair_kind == "fin_tail" {
                                    response_sender
                                        .enqueue_critical_tail_repair_frame(frame)
                                        .is_some()
                                } else {
                                    response_sender.enqueue_critical_repair_frame_with_cause(
                                        frame,
                                        ack_gap_repair_cause,
                                    );
                                    true
                                }
                            } else {
                                response_sender
                                    .enqueue_repair_frame_with_priority(frame, mux_limits, true)
                                    .is_some()
                            };
                            #[cfg(not(feature = "lab-diagnostics"))]
                            let _ = queued;
                            #[cfg(feature = "lab-diagnostics")]
                            lab_diagnostic(
                                "repair",
                                format_args!(
                                    "stream_id={} cause={} queued={}",
                                    stream_id.0, repair_kind, queued,
                                ),
                            );
                            if queued {
                                queued_persistent_ack_gap_repair |=
                                    ack_gap_repair_ready && repair_kind == "ack_gap";
                            }
                        }
                        if queued_persistent_ack_gap_repair {
                            ack_gap_repair.record_repair_queued();
                        }
                        #[cfg(not(feature = "lab-diagnostics"))]
                        let _ = ack;
                        if pending_local_fin
                            && response_sender.is_empty()
                            && send_stream.repair_bytes() == 0
                        {
                            let frame = Frame::StreamFin {
                                stream_id,
                                final_offset: send_stream.next_offset(),
                            };
                            response_sender.enqueue_final_control_frame(frame);
                            response_sender_retry_at = None;
                            close_sent = true;
                            pending_local_fin = false;
                        }
                    }
                    Frame::StreamMaxData {
                        stream_id: max_stream_id,
                        max_offset,
                    } if max_stream_id == stream_id => {
                        send_stream.update_max_offset(max_offset);
                    }
                    Frame::PathStatus {
                        status: crate::protocol::PathStatus::Active,
                        ..
                    } => {}
                    Frame::StreamFin {
                        stream_id: fin_stream_id,
                        final_offset,
                    } if fin_stream_id == stream_id => {
                        if receive_stream_fin(
                            &recv_stream,
                            &mut pending_remote_fin_offset,
                            final_offset,
                        )? {
                            if enqueue_tcp_recv_progress(
                                &mut response_sender,
                                &recv_stream,
                                &mut recv_progress,
                                &mut request_sparse_ack_progress,
                                request_active_path_snapshot,
                                relay_lane,
                                mux_limits,
                                true,
                            ) {
                                response_sender_retry_at = None;
                                last_recv_progress_sent_at = Instant::now();
                            }
                            local.shutdown().await?;
                            remote_open = false;
                            pending_remote_fin_offset = None;
                        }
                    }
                    Frame::StreamReset {
                        stream_id: reset_stream_id,
                        reason,
                    } if reset_stream_id == stream_id => return Err(RuntimeError::RemoteReset(reason)),
                    Frame::StreamData {
                        stream_id: received_stream_id,
                        offset,
                        payload,
                        ..
                    } if received_stream_id == stream_id
                        && stream_data_range_already_delivered(&recv_stream, offset, payload.len()) =>
                    {
                        if enqueue_tcp_recv_progress(
                            &mut response_sender,
                            &recv_stream,
                            &mut recv_progress,
                            &mut request_sparse_ack_progress,
                            request_active_path_snapshot,
                            relay_lane,
                            mux_limits,
                            true,
                        ) {
                            response_sender_retry_at = None;
                            last_recv_progress_sent_at = Instant::now();
                        }
                    }
                    unexpected => {
                        log_unexpected_stream_relay_frame("single", stream_id, &unexpected);
                        return Err(RuntimeError::Protocol("unexpected stream relay frame"));
                    }
                }
                if response_sender.queued_send_ready() {
                    let ordered_owner_debt_bytes = reliable_relay_current_ordered_owner_debt_bytes(
                        relay_lane,
                        &send_stream,
                        last_send_ack_complete,
                        last_send_ack_frontier,
                    );
                    if drain_server_response_sender_ready(
                        &mut response_sender,
                        &path_stream,
                        ordered_owner_debt_bytes,
                        &mut send_stream,
                        relay_lane,
                        mux_limits,
                        sender_dispatch_byte_budget,
                        sender_dispatch_item_budget,
                        &mut stats,
                        session_id,
                    )
                    .await?
                    {
                        response_sender_retry_at =
                            Some(tokio::time::Instant::now() + sender_service_retry_delay(send_path_snapshot, relay_lane));
                    }
                }
            }
            changed = async {
                match output_updates.as_mut() {
                    Some(updates) => updates
                        .changed()
                        .await
                        .map_err(|_| RuntimeError::ReliablePathSessionClosed),
                    None => std::future::pending::<Result<(), RuntimeError>>().await,
                }
            }, if output_updates.is_some() => {
                changed?;
                let now_has_repair_alternative = path_stream.has_multipath_repair_alternative();
                let gained_repair_alternative =
                    now_has_repair_alternative && !multipath_repair_alternative_available;
                #[cfg(not(feature = "lab-diagnostics"))]
                let _ = gained_repair_alternative;
                multipath_repair_alternative_available = now_has_repair_alternative;
                response_sender_retry_at = None;
                let final_tail_repair_ready = reliable_final_tail_repair_ready(
                    close_sent || pending_local_fin,
                    &send_stream,
                    &last_send_ack_ranges,
                    last_send_ack_frontier,
                    tail_repair_deadline,
                    tokio::time::Instant::now(),
                );
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "server_output_update",
                    format_args!(
                        "stream_id={} now_has_repair_alternative={} gained_repair_alternative={} final_tail_repair_ready={} close_sent={} pending_local_fin={} repair_bytes={} ack_ranges={} ack_frontier={} sent_offset={} queue_bytes={}",
                        stream_id.0,
                        now_has_repair_alternative,
                        gained_repair_alternative,
                        final_tail_repair_ready,
                        close_sent,
                        pending_local_fin,
                        send_stream.repair_bytes(),
                        last_send_ack_ranges.len(),
                        last_send_ack_frontier,
                        send_stream.next_offset(),
                        response_sender.bytes(),
                    ),
                );
                if final_tail_repair_ready {
                    let repair_limit = reliable_critical_tail_repair_limit_bytes(
                        adaptive_reliable_relay_repair_bytes(
                            tail_repair_path_snapshot,
                            relay_lane,
                            mux_limits,
                        ),
                        send_stream.repair_bytes(),
                        mux_limits,
                    );
                    let (
                        repair_frames,
                        blocked_frontier_offset,
                        same_output_frontier_retransmit,
                    ) = prefix_final_tail_repair_frames_with_available_output(
                        &path_stream,
                        stream_final_offset_tail_repair_frames(
                            &send_stream,
                            &last_send_ack_ranges,
                            repair_limit,
                            true,
                            true,
                        ),
                    );
                    #[cfg(feature = "lab-diagnostics")]
                    if blocked_frontier_offset.is_some() {
                        lab_diagnostic(
                            "tail_stall_repair_blocked_frontier",
                            format_args!(
                                "stream_id={} blocked_frontier_offset={:?} repair_kind=fin_tail",
                                stream_id.0, blocked_frontier_offset,
                            ),
                        );
                    }
                    #[cfg(not(feature = "lab-diagnostics"))]
                    let _ = blocked_frontier_offset;
                    #[cfg(not(feature = "lab-diagnostics"))]
                    let _ = same_output_frontier_retransmit;
                    let live_repair_retry_after =
                        reliable_relay_tail_repair_delay(tail_repair_path_snapshot, relay_lane);
                    let mut repair_count = 0usize;
                    for frame in repair_frames {
                        let queued = if path_stream.has_recent_live_repair_flight_overlap(
                            &frame,
                            live_repair_retry_after,
                        ) {
                            false
                        } else {
                            response_sender
                                .enqueue_critical_tail_repair_frame(frame)
                                .is_some()
                        };
                        if queued {
                            repair_count = repair_count.saturating_add(1);
                        }
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "repair",
                            format_args!(
                                "stream_id={} cause=fin_tail queued={}",
                                stream_id.0, queued
                            ),
                        );
                    }
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "tail_stall_repair",
                        format_args!(
                            "stream_id={} lane={:?} ack_frontier={} sent_offset={} repair_bytes={} repair_frames={} blocked_frontier_offset={:?} same_output_frontier_retransmit={} base_repair_limit={} repair_limit={} extra_traffic_hint_percent={} repair_kind=fin_tail",
                            stream_id.0,
                            relay_lane,
                            last_send_ack_frontier,
                            send_stream.next_offset(),
                            send_stream.repair_bytes(),
                            repair_count,
                            blocked_frontier_offset,
                            same_output_frontier_retransmit,
                            repair_limit,
                            repair_limit,
                            performance.extra_traffic_hint_percent,
                        ),
                    );
                    if repair_count > 0 {
                        last_tail_repair_at = Instant::now();
                    }
                }
                if response_sender.queued_send_ready() {
                    let ordered_owner_debt_bytes = reliable_relay_current_ordered_owner_debt_bytes(
                        relay_lane,
                        &send_stream,
                        last_send_ack_complete,
                        last_send_ack_frontier,
                    );
                    if drain_server_response_sender_ready(
                        &mut response_sender,
                        &path_stream,
                        ordered_owner_debt_bytes,
                        &mut send_stream,
                        relay_lane,
                        mux_limits,
                        sender_dispatch_byte_budget,
                        sender_dispatch_item_budget,
                        &mut stats,
                        session_id,
                    )
                    .await?
                    {
                        response_sender_retry_at =
                            Some(tokio::time::Instant::now() + sender_service_retry_delay(send_path_snapshot, relay_lane));
                    }
                }
                continue;
            }
            _ = wait_for_carrier_capacity_notifies(carrier_capacity_notifies), if queued_send_blocked && has_carrier_capacity_notify => {
                response_sender_retry_at = None;
                continue;
            }
            _ = tokio::time::sleep_until(queued_send_retry_deadline), if queued_send_blocked => {
                response_sender_retry_at = None;
                continue;
            }
            _ = tokio::time::sleep_until(recv_progress_deadline), if reliable_relay_recv_progress_timer_enabled(
                    request_active_underlay,
                    multipath_repair_alternative_available,
                )
                && reliable_relay_recv_progress_resend_active(
                    &recv_stream,
                    remote_open,
                    Some(request_active_underlay),
                ) => {
                if enqueue_tcp_recv_progress(
                    &mut response_sender,
                    &recv_stream,
                    &mut recv_progress,
                    &mut request_sparse_ack_progress,
                    request_active_path_snapshot,
                    relay_lane,
                    mux_limits,
                    true,
                ) {
                    response_sender_retry_at = None;
                    last_recv_progress_sent_at = Instant::now();
                }
                if response_sender.queued_send_ready() {
                    let ordered_owner_debt_bytes = reliable_relay_current_ordered_owner_debt_bytes(
                        relay_lane,
                        &send_stream,
                        last_send_ack_complete,
                        last_send_ack_frontier,
                    );
                    if drain_server_response_sender_ready(
                        &mut response_sender,
                        &path_stream,
                        ordered_owner_debt_bytes,
                        &mut send_stream,
                        relay_lane,
                        mux_limits,
                        sender_dispatch_byte_budget,
                        sender_dispatch_item_budget,
                        &mut stats,
                        session_id,
                    )
                    .await?
                    {
                        response_sender_retry_at =
                            Some(tokio::time::Instant::now() + sender_service_retry_delay(send_path_snapshot, relay_lane));
                    }
                }
            }
            _ = std::future::ready(()), if can_send_pending_fin => {
                let frame = Frame::StreamFin {
                    stream_id,
                    final_offset: send_stream.next_offset(),
                };
                response_sender.enqueue_final_control_frame(frame);
                response_sender_retry_at = None;
                close_sent = true;
                pending_local_fin = false;
                let ordered_owner_debt_bytes = reliable_relay_current_ordered_owner_debt_bytes(
                    relay_lane,
                    &send_stream,
                    last_send_ack_complete,
                    last_send_ack_frontier,
                );
                if drain_server_response_sender_ready(
                    &mut response_sender,
                    &path_stream,
                    ordered_owner_debt_bytes,
                    &mut send_stream,
                    relay_lane,
                    mux_limits,
                    sender_dispatch_byte_budget,
                    sender_dispatch_item_budget,
                    &mut stats,
                    session_id,
                )
                .await?
                {
                    response_sender_retry_at =
                        Some(tokio::time::Instant::now() + sender_service_retry_delay(send_path_snapshot, relay_lane));
                }
            }
            _ = std::future::ready(()), if queued_send_ready => {
                let ordered_owner_debt_bytes = reliable_relay_current_ordered_owner_debt_bytes(
                    relay_lane,
                    &send_stream,
                    last_send_ack_complete,
                    last_send_ack_frontier,
                );
                if drain_server_response_sender_ready(
                    &mut response_sender,
                    &path_stream,
                    ordered_owner_debt_bytes,
                    &mut send_stream,
                    relay_lane,
                    mux_limits,
                    sender_dispatch_byte_budget,
                    sender_dispatch_item_budget,
                    &mut stats,
                    session_id,
                )
                .await?
                {
                    response_sender_retry_at =
                        Some(tokio::time::Instant::now() + sender_service_retry_delay(send_path_snapshot, relay_lane));
                }
                tokio::task::yield_now().await;
            }
            read = async {
                #[cfg(feature = "lab-diagnostics")]
                let read_started = Instant::now();
                let result = read_reliable_relay_payload(&mut local, &mut buf, read_budget).await;
                #[cfg(feature = "lab-diagnostics")]
                if let Ok((read, _)) = &result {
                    lab_perf_record("relay.local_read_wait", read_started.elapsed(), *read);
                }
                result
            }, if can_read_local => {
                let (read, payload) = read?;
                if read == 0 {
                    pending_local_fin = true;
                    local_open = false;
                } else {
                    let payload = payload.expect("positive read returns payload");
                    #[cfg(feature = "lab-diagnostics")]
                    let enqueue_id = response_sender.enqueue_data_for_lane(payload, relay_lane);
                    #[cfg(not(feature = "lab-diagnostics"))]
                    response_sender.enqueue_data_for_lane(payload, relay_lane);
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "server_sender_enqueue",
                        format_args!(
                            "session_id={} stream_id={} enqueue_id={} lane={:?} payload_bytes={} queue_bytes={} queue_limit={} send_credit_bytes={} repair_bytes={}",
                            session_id.0,
                            stream_id.0,
                            enqueue_id,
                            relay_lane,
                            read,
                            response_sender.bytes(),
                            sender_queue_limit,
                            send_stream.send_credit_bytes(),
                            send_stream.repair_bytes(),
                        ),
                    );
                    let mut opportunistic_reads = 1usize;
                    while local_open
                        && opportunistic_reads < sender_dispatch_item_budget
                        && response_sender.can_read_product_source(
                            local_open,
                            false,
                            &send_stream,
                            mux_limits,
                            sender_queue_limit,
                        )
                        && response_sender.data_bytes() < sender_dispatch_byte_budget
                    {
                        let owner_tail_read_headroom =
                            reliable_relay_source_staging_owner_tail_headroom(
                                source_staging_context,
                                relay_lane,
                                ordered_owner_debt_bytes,
                                response_sender.data_bytes(),
                                reliable_bulk_carrier_feed_quantum_bytes(mux_limits),
                                mux_limits,
                            );
                        if owner_tail_read_headroom == 0 {
                            break;
                        }
                        let next_read_budget = response_sender
                            .read_budget(&send_stream, mux_limits, sender_queue_limit, buf.len())
                            .min(owner_tail_read_headroom);
                        if next_read_budget == 0 {
                            break;
                        }
                        let read = tokio::select! {
                            biased;
                            read = read_reliable_relay_payload(&mut local, &mut buf, next_read_budget) => read,
                            _ = std::future::ready(()) => break,
                        };
                        let (read, payload) = read?;
                        if read == 0 {
                            pending_local_fin = true;
                            local_open = false;
                            break;
                        }
                        let payload = payload.expect("positive read returns payload");
                        #[cfg(feature = "lab-diagnostics")]
                        let enqueue_id = response_sender.enqueue_data_for_lane(payload, relay_lane);
                        #[cfg(not(feature = "lab-diagnostics"))]
                        response_sender.enqueue_data_for_lane(payload, relay_lane);
                        opportunistic_reads = opportunistic_reads.saturating_add(1);
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "server_sender_enqueue",
                            format_args!(
                                "session_id={} stream_id={} enqueue_id={} lane={:?} payload_bytes={} queue_bytes={} queue_limit={} send_credit_bytes={} repair_bytes={} opportunistic=true",
                                session_id.0,
                                stream_id.0,
                                enqueue_id,
                                relay_lane,
                                read,
                                response_sender.bytes(),
                                sender_queue_limit,
                                send_stream.send_credit_bytes(),
                                send_stream.repair_bytes(),
                            ),
                        );
                    }
                    if response_sender.queued_send_ready() {
                        let ordered_owner_debt_bytes = reliable_relay_current_ordered_owner_debt_bytes(
                            relay_lane,
                            &send_stream,
                            last_send_ack_complete,
                            last_send_ack_frontier,
                        );
                        if drain_server_response_sender_ready(
                            &mut response_sender,
                            &path_stream,
                            ordered_owner_debt_bytes,
                            &mut send_stream,
                            relay_lane,
                            mux_limits,
                            sender_dispatch_byte_budget,
                            sender_dispatch_item_budget,
                            &mut stats,
                            session_id,
                        )
                        .await?
                        {
                            response_sender_retry_at =
                                Some(tokio::time::Instant::now() + sender_service_retry_delay(send_path_snapshot, relay_lane));
                        }
                    }
                }
            }
            else => break Ok(stats),
        }
    };

    let mut result = result;
    if result.is_ok() && pending_local_fin && !close_sent {
        while result.is_ok() {
            if response_sender.discard_stale_persistent_ack_gap_repairs(&path_stream) > 0 {
                ack_gap_repair.release_repair_attempt();
            }
            if response_sender.is_empty() {
                break;
            }
            let ordered_owner_debt_bytes = reliable_relay_current_ordered_owner_debt_bytes(
                last_relay_lane,
                &send_stream,
                last_send_ack_complete,
                last_send_ack_frontier,
            );
            match drain_server_response_sender_ready(
                &mut response_sender,
                &path_stream,
                ordered_owner_debt_bytes,
                &mut send_stream,
                last_relay_lane,
                mux_limits,
                last_sender_dispatch_byte_budget,
                last_sender_dispatch_item_budget,
                &mut stats,
                session_id,
            )
            .await
            {
                Ok(true) => {
                    if let Some(deadline) = response_sender.persistent_ack_gap_repair_deadline() {
                        tokio::select! {
                            _ = wait_for_carrier_capacity_notifies(path_stream.capacity_notifies()) => {}
                            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {}
                        }
                    } else {
                        wait_for_carrier_capacity_notifies(path_stream.capacity_notifies()).await;
                    }
                }
                Ok(false) if response_sender.queued_send_ready() => {}
                Ok(false) => break,
                Err(err) => result = Err(err),
            }
        }
        if result.is_ok() && response_sender.is_empty() {
            let frame = Frame::StreamFin {
                stream_id,
                final_offset: send_stream.next_offset(),
            };
            response_sender.enqueue_final_control_frame(frame);
            match response_sender.dispatch_next(
                &path_stream,
                &mut send_stream,
                last_relay_lane,
                mux_limits,
            ) {
                Ok(dispatch) if dispatch.lane == ReliableWorkClass::Control => {
                    close_sent = true;
                }
                Ok(_) => {
                    result = Err(RuntimeError::Protocol(
                        "server response sender dispatched non-control final close",
                    ));
                }
                Err(err) => {
                    result = Err(err);
                }
            }
        }
    }
    if close_sent {
        path_stream.close_ordered(last_relay_lane).await;
    } else {
        path_stream.close().await;
    }
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_flush("stream_close");
    #[cfg(feature = "lab-diagnostics")]
    lab_assert_server_sender_service_balanced(session_id.0, stream_id.0);
    result
}

#[cfg(test)]
mod tests;
