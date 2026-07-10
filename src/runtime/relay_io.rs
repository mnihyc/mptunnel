use super::bulk_admission::{
    bulk_service_feed_reservoir_payload_bytes, bulk_service_horizon_payload_bytes,
};
use super::*;

pub(super) async fn send_sender_service_attach_control_frames(
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
        )
        .await?;
    }
    Ok(())
}

pub(super) fn frame_pacing_bytes(frame: &Frame) -> usize {
    match frame {
        Frame::StreamData { payload, .. } => payload.len().max(1),
        Frame::StreamFin { .. }
        | Frame::StreamAck { .. }
        | Frame::StreamMaxData { .. }
        | Frame::StreamReset { .. }
        | Frame::StreamDetach { .. } => 1,
        _ => 0,
    }
}

pub(super) fn reliable_relay_error_is_migratable(err: &RuntimeError) -> bool {
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

pub(super) fn stream_ack_gap_repair_allowed(
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

pub(super) fn stream_tail_timer_repair_allowed(
    live_owner_tail_repair_candidate: bool,
    has_failed_owner_repair_output: bool,
) -> bool {
    live_owner_tail_repair_candidate || has_failed_owner_repair_output
}

pub(super) fn reliable_relay_tail_repair_timer_active(
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

pub(super) fn stream_ack_is_authoritative_contiguous_prefix(
    complete: bool,
    ranges: &[OffsetRange],
    frontier: u64,
) -> bool {
    complete
        && frontier > 0
        && matches!(ranges, [range] if range.start == 0 && range.end == frontier)
}

pub(super) fn stream_ack_ranges_expose_authoritative_gap(
    complete: bool,
    ranges: &[OffsetRange],
) -> bool {
    complete
        && ranges
            .first()
            .is_some_and(|first| first.start > 0 || ranges.len() > 1)
}

pub(super) fn reliable_relay_ordered_owner_debt_bytes(
    lane: FlowLane,
    _ack_complete: bool,
    ack_frontier: u64,
    next_offset: u64,
) -> usize {
    if !relay_lane_is_bulk(lane) || ack_frontier >= next_offset {
        return 0;
    }
    // This is a tail guard, not repair debt. It blocks alternate OwnerData and
    // missing-owner failover while lower Service bytes are unresolved, but it
    // must not make the live Service owner itself inadmissible.
    usize::try_from(next_offset.saturating_sub(ack_frontier)).unwrap_or(usize::MAX)
}

pub(super) fn reliable_relay_owner_tail_read_headroom(
    lane: FlowLane,
    service_path: Option<PathSnapshot>,
    ordered_owner_debt_bytes: usize,
    queued_data_bytes: usize,
    mux_limits: MuxLimits,
) -> usize {
    if !relay_lane_is_bulk(lane) {
        return usize::MAX;
    }
    let payload = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
    let has_latency_pressure = reliable_relay_owner_tail_has_latency_pressure(service_path);
    let has_feed_reservoir = reliable_relay_owner_tail_has_feed_reservoir(service_path);
    let feed_limit = if has_latency_pressure || !has_feed_reservoir {
        bulk_service_horizon_payload_bytes(payload, mux_limits)
    } else {
        bulk_service_feed_reservoir_payload_bytes(payload, mux_limits)
    };
    feed_limit.saturating_sub(ordered_owner_debt_bytes.saturating_add(queued_data_bytes))
}

fn reliable_relay_owner_tail_has_latency_pressure(path: Option<PathSnapshot>) -> bool {
    path.is_some_and(|path| path.active_latency_sensitive_flows > 0)
}

fn reliable_relay_owner_tail_has_feed_reservoir(path: Option<PathSnapshot>) -> bool {
    path.is_some_and(|path| path.product_progress_rate_bps.is_some() && path.confidence >= 1.0)
}

pub(super) fn stream_ack_contiguous_frontier(_complete: bool, ranges: &[OffsetRange]) -> u64 {
    ranges
        .first()
        .filter(|range| range.start == 0)
        .map_or(0, |range| range.end)
}

pub(super) fn update_repair_authoritative_ack_snapshot(
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

pub(super) fn reliable_relay_tail_repair_deadline(
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

pub(super) fn reliable_relay_effective_tail_repair_deadline(
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

pub(super) fn reliable_relay_tail_repair_delay(
    path: Option<PathSnapshot>,
    lane: FlowLane,
) -> Duration {
    reliable_relay_stall_timeout(path, lane)
}

pub(super) fn reliable_ack_gap_repair_delay(
    path: Option<PathSnapshot>,
    lane: FlowLane,
) -> Duration {
    reliable_relay_stall_timeout(path, lane).saturating_mul(QUIC_PERSISTENT_CONGESTION_THRESHOLD)
}

#[cfg(test)]
pub(super) fn stream_ack_gap_repair_frames(
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

pub(super) fn stream_ack_gap_repair_frames_normalized(
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

pub(super) fn stream_final_offset_tail_repair_frames(
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
pub(super) struct ReliableAckGapRepairProgress {
    first_gap_start: Option<u64>,
    first_seen_at: Option<Instant>,
    last_repair_at: Option<Instant>,
}

impl ReliableAckGapRepairProgress {
    pub(super) fn repair_ready(
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
        self.last_repair_at = Some(now);
        true
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

#[derive(Debug, Clone, Default)]
pub(super) struct ReliableRecvProgress {
    last_max_data_offset: u64,
    last_max_data_window_bytes: u64,
    last_ack_offset: u64,
    last_ack_reorder_bytes: usize,
    last_ack_range_count: usize,
    last_ack_largest_end: u64,
    last_ack_at: Option<Instant>,
}

impl ReliableRecvProgress {
    pub(super) fn should_send_ack(
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

    pub(super) fn should_send_max_data(
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

/// Initial stream-level product receive window advertised to the peer.
///
/// TCP keeps the configured application window because kernel TCP backpressure is
/// the visible carrier queue at this layer. QUIC reliable streams are different:
/// once Quinn accepts bytes, mptunnel no longer observes those bytes as product
/// queue, and a 64 MiB product window can turn into seconds of hidden QUIC
/// stream backlog on shaped links. Start QUIC with a small but BDP-useful window
/// and let `STREAM_MAX_DATA` grow it from real receive progress.
pub(super) fn reliable_stream_initial_advertised_window_bytes(
    underlay: UnderlayProtocol,
    lane: FlowLane,
    mux_limits: MuxLimits,
) -> u64 {
    reliable_stream_advertised_window_from_underlay(None, underlay, lane, mux_limits)
}

pub(super) fn reliable_stream_advertised_window_bytes(
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
    path: Option<PathSnapshot>,
    underlay: UnderlayProtocol,
    lane: FlowLane,
    mux_limits: MuxLimits,
) -> u64 {
    let configured = mux_limits.max_stream_window_bytes.max(1);
    if underlay != UnderlayProtocol::Udp {
        return configured;
    }

    let relay_chunk = reliable_relay_buffer_len(mux_limits) as u64;
    let min_window = RELIABLE_UDP_MIN_PRODUCT_WINDOW_BYTES
        .max(relay_chunk.saturating_mul(4))
        .min(configured);
    let startup_window = RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES
        .max(min_window)
        .min(configured);
    if !relay_lane_is_bulk(lane) {
        return startup_window;
    }

    let Some(path) = path else {
        return startup_window;
    };

    let bdp_window = (reliable_path_product_bdp_bytes(path) * RELIABLE_UDP_BULK_BDP_GAIN)
        .ceil()
        .max(min_window as f64) as u64;
    let carrier_window = path
        .inflight_limit_bytes
        .saturating_mul(4)
        .max(path.bytes_in_flight.saturating_mul(2))
        .max(path.queue_bytes.saturating_mul(2));

    bdp_window
        .max(carrier_window)
        .max(startup_window)
        .min(configured)
}

pub(super) fn reliable_stream_max_data_update_bytes(
    advertised_window_bytes: u64,
    mux_limits: MuxLimits,
) -> u64 {
    let window_step = advertised_window_bytes.saturating_div(4).max(1);
    let payload_step = reliable_relay_buffer_len(mux_limits) as u64;
    window_step
        .max(payload_step)
        .min(advertised_window_bytes.max(1))
}

pub(super) fn reliable_stream_ack_update_bytes(
    path: Option<PathSnapshot>,
    lane: FlowLane,
    mux_limits: MuxLimits,
) -> u64 {
    if !relay_lane_is_bulk(lane) {
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

pub(super) fn enqueue_tcp_recv_progress(
    response_sender: &mut ServerResponseSenderService,
    recv_stream: &ReliableRecvStream,
    progress: &mut ReliableRecvProgress,
    path: Option<PathSnapshot>,
    lane: FlowLane,
    mux_limits: MuxLimits,
    force_max_data: bool,
) -> bool {
    let mut sent_any = false;
    if progress.should_send_ack(recv_stream, path, lane, mux_limits, force_max_data) {
        #[cfg(feature = "lab-diagnostics")]
        let ack_started = Instant::now();
        let ack_frames = recv_stream.ack_frames();
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

pub(super) fn reliable_relay_recv_progress_resend_active(
    recv_stream: &ReliableRecvStream,
    remote_open: bool,
) -> bool {
    remote_open && (recv_stream.next_offset() > 0 || recv_stream.reorder_bytes() > 0)
}

pub(super) fn reliable_stream_recv_progress_interval(
    path: Option<PathSnapshot>,
    lane: FlowLane,
) -> Duration {
    reliable_relay_stall_timeout(path, lane)
        .div_f64(2.0)
        .max(QUIC_TIMER_GRANULARITY)
}

pub(super) fn sender_service_retry_delay(path: Option<PathSnapshot>, lane: FlowLane) -> Duration {
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

pub(super) fn reliable_relay_buffer_len(mux_limits: MuxLimits) -> usize {
    mux_limits
        .max_reliable_relay_chunk_bytes
        .min(mux_limits.max_payload_bytes)
        .min(mux_limits.max_path_flight_bytes)
        .max(1)
}

pub(super) fn resize_reliable_relay_buffer(buffer: &mut bytes::BytesMut, target_len: usize) {
    let target_len = target_len.max(1);
    buffer.clear();
    if buffer.capacity() < target_len {
        buffer.reserve(target_len.saturating_sub(buffer.capacity()));
    }
}

pub(super) async fn read_reliable_relay_payload<S>(
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

pub(super) async fn write_delivered_payloads<S>(
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

pub(super) fn receive_stream_fin(
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

pub(super) fn stream_data_range_already_delivered(
    recv_stream: &ReliableRecvStream,
    offset: u64,
    payload_len: usize,
) -> bool {
    offset.saturating_add(payload_len as u64) <= recv_stream.next_offset()
}

pub(super) fn pending_stream_fin_ready(
    recv_stream: &ReliableRecvStream,
    pending_final_offset: Option<u64>,
) -> bool {
    pending_final_offset.is_some_and(|final_offset| recv_stream.next_offset() >= final_offset)
}

pub(super) fn stream_terminal_fin_replay_required(
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

pub(super) fn adaptive_reliable_relay_chunk_bytes(
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
        let startup = if relay_lane_is_bulk(lane) {
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
    let target = if relay_lane_is_bulk(lane) {
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

pub(super) fn adaptive_reliable_relay_chunk_bytes_with_frame_limit(
    path: Option<PathSnapshot>,
    lane: FlowLane,
    mux_limits: MuxLimits,
    max_frame_payload_bytes: usize,
) -> usize {
    adaptive_reliable_relay_chunk_bytes(path, lane, mux_limits)
        .min(max_frame_payload_bytes)
        .max(1)
}

pub(super) fn adaptive_reliable_relay_inflight_bytes(
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

pub(super) fn reliable_relay_sender_dispatch_budget(
    mux_limits: MuxLimits,
    lane: FlowLane,
    adaptive_chunk: usize,
    inflight_limit: usize,
    queue_limit: usize,
) -> (usize, usize) {
    let quantum = adaptive_chunk.max(1);
    if !relay_lane_is_bulk(lane) {
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

pub(super) fn adaptive_reliable_relay_repair_bytes(
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

pub(super) fn reliable_critical_tail_repair_limit_bytes(
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

pub(super) fn reliable_critical_tail_repair_is_over_budget(
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

pub(super) fn reliable_path_product_bdp_bytes(path: PathSnapshot) -> f64 {
    let rate_bps = path.delivery_rate_bps.max(
        path.product_progress_rate_bps
            .unwrap_or(path.delivery_rate_bps),
    );
    let rate_bps = rate_bps.max(1.0);
    (rate_bps / 8.0) * (path.srtt_ms.max(1.0) / 1000.0)
}

pub(super) fn bbr_min_send_quantum_bytes(mux_limits: MuxLimits) -> usize {
    let cap = reliable_relay_buffer_len(mux_limits).max(1);
    (BBR_MIN_SEND_QUANTUM_PACKETS * TRANSPORT_MSS_BYTES)
        .min(cap)
        .max(1)
}

pub(super) fn reliable_bulk_carrier_feed_quantum_bytes(mux_limits: MuxLimits) -> usize {
    let cap = reliable_relay_buffer_len(mux_limits).max(1);
    BBR_MAX_SEND_QUANTUM_BYTES
        .min(cap)
        .max(bbr_min_send_quantum_bytes(mux_limits))
}

pub(super) fn bbr_min_pipe_cwnd_bytes(mux_limits: MuxLimits) -> usize {
    let cap = mux_limits.max_path_flight_bytes.max(1);
    (BBR_MIN_PIPE_CWND_PACKETS * TRANSPORT_MSS_BYTES)
        .min(cap)
        .max(1)
}

pub(super) fn bbr_send_quantum_bytes(path: PathSnapshot, mux_limits: MuxLimits) -> usize {
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

pub(super) fn relay_lane_min_chunk_bytes(
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

pub(super) fn relay_lane_startup_chunk_bytes(lane: FlowLane, mux_limits: MuxLimits) -> usize {
    let cap = reliable_relay_scheduler_quantum_cap(None, lane, mux_limits);
    let floor = relay_lane_min_chunk_bytes(None, lane, mux_limits);
    let target = match lane {
        FlowLane::Control | FlowLane::RealtimeDatagram => bbr_min_send_quantum_bytes(mux_limits),
        FlowLane::Latency => PATH_OPEN_SCORE_BYTES,
        FlowLane::Throughput | FlowLane::Background => reliable_startup_send_quantum_bytes(),
    };
    target.clamp(floor.min(cap).max(1), cap)
}

pub(super) fn reliable_relay_scheduler_quantum_cap(
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

pub(super) fn reliable_lane_min_inflight_bytes(lane: FlowLane, mux_limits: MuxLimits) -> usize {
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

pub(super) fn reliable_lane_startup_inflight_bytes(lane: FlowLane, mux_limits: MuxLimits) -> usize {
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

pub(super) fn bbr_inflight_target_bytes(
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

pub(super) fn reliable_startup_bdp_bytes() -> f64 {
    reliable_startup_rate_bps() / 8.0 * (reliable_startup_srtt_ms() / 1000.0)
}

pub(super) fn reliable_startup_send_quantum_bytes() -> usize {
    bbr_send_quantum_bytes_for_rate(reliable_startup_rate_bps())
}

pub(super) fn reliable_path_stability_factor(path: PathSnapshot) -> f64 {
    let bdp_bytes = reliable_path_product_bdp_bytes(path);
    let min_pipe = (BBR_MIN_PIPE_CWND_PACKETS * TRANSPORT_MSS_BYTES) as f64;
    let floor = adaptive_transport_floor_factor(min_pipe, bdp_bytes);
    let loss_factor = (1.0 - path.loss_rate.clamp(0.0, 1.0)).max(floor);
    let srtt = path.srtt_ms.max(1.0);
    let jitter_factor = (srtt / (srtt + path.jitter_ms.max(0.0))).max(floor);
    loss_factor * jitter_factor
}

pub(super) fn reliable_path_queue_factor(path: PathSnapshot, bdp_bytes: f64) -> f64 {
    let queued = path.queue_bytes.saturating_add(path.bytes_in_flight) as f64;
    let floor = adaptive_transport_floor_factor(
        (BBR_MIN_PIPE_CWND_PACKETS * TRANSPORT_MSS_BYTES) as f64,
        bdp_bytes,
    );
    (bdp_bytes / (bdp_bytes + queued.max(0.0))).max(floor)
}

pub(super) fn reliable_path_backlog_factor(path: PathSnapshot, bdp_bytes: f64) -> f64 {
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

pub(super) fn reliable_path_quantum_condition_factor(path: PathSnapshot, bdp_bytes: f64) -> f64 {
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

pub(super) fn reliable_sender_effective_relay_lane(local: FlowLane, peer: FlowLane) -> FlowLane {
    if local == FlowLane::Throughput || peer == FlowLane::Throughput {
        FlowLane::Throughput
    } else if local == FlowLane::Background || peer == FlowLane::Background {
        FlowLane::Background
    } else {
        peer
    }
}

#[cfg(test)]
pub(super) fn prefix_repair_frames_with_available_output(
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

pub(super) fn prefix_final_tail_repair_frames_with_available_output(
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

pub(super) fn prefix_repair_frames_with_failed_owner_output(
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

pub(super) fn prefix_repair_frames_with_unknown_owner_output(
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
            let tail_limit = reliable_critical_tail_repair_limit_bytes(
                base_repair_limit,
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
        let dispatch = match response_sender
            .dispatch_next_with_ordered_owner_debt(
                path_stream,
                send_stream,
                relay_lane,
                mux_limits,
                ordered_owner_debt_bytes,
            )
            .await
        {
            Ok(dispatch) => dispatch,
            Err(RuntimeError::SenderServiceBlocked) => {
                blocked_by_carrier = true;
                break;
            }
            Err(err) => return Err(err),
        };
        dispatched_items = dispatched_items.saturating_add(1);
        if dispatch.lane == ReliableRelayQueuedWorkLane::Repair {
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
            if dispatch.lane == ReliableRelayQueuedWorkLane::Data {
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

pub(super) async fn relay_reliable_stream<S>(
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
        let send_path_snapshot = path_stream.send_path_snapshot(
            relay_lane,
            relay_lane_startup_chunk_bytes(relay_lane, mux_limits)
                .min(path_stream.max_frame_payload_bytes),
        );
        let tail_repair_path_snapshot = path_stream.tail_repair_snapshot(
            last_send_ack_frontier,
            relay_lane,
            relay_lane_startup_chunk_bytes(relay_lane, mux_limits)
                .min(path_stream.max_frame_payload_bytes),
        );
        let recv_progress_deadline = tokio::time::Instant::from_std(
            last_recv_progress_sent_at
                + reliable_stream_recv_progress_interval(send_path_snapshot, relay_lane),
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
        let owner_tail_read_headroom = reliable_relay_owner_tail_read_headroom(
            relay_lane,
            send_path_snapshot,
            ordered_owner_debt_bytes,
            response_sender.data_bytes(),
            mux_limits,
        );
        let source_read_ceiling = reliable_relay_buffer_len(mux_limits)
            .min(path_stream.max_frame_payload_bytes)
            .min(sender_queue_limit)
            .min(latency_owner_credit)
            .min(owner_tail_read_headroom);
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
        let can_read_by_flow = source_read_ceiling > 0
            && owner_tail_read_headroom > 0
            && response_sender.can_read_product_source(
                local_open,
                queued_send_blocked,
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

        tokio::select! {
            biased;
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
                            send_path_snapshot,
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
                                send_path_snapshot,
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
                        let base_repair_limit =
                            adaptive_reliable_relay_repair_bytes(None, relay_lane, mux_limits);
                        let repair_event_budget =
                            response_sender.repair_extra_event_budget_remaining(mux_limits);
                        let has_multipath_repair_alternative =
                            path_stream.has_multipath_repair_alternative();
                        let ack_gap_repair_ready = ack_gap_repair.repair_ready(
                            complete,
                            &normalized_ranges,
                            Some(path_stream.underlay),
                            has_multipath_repair_alternative,
                            None,
                            relay_lane,
                        );
                        let repair_limit = if ack_gap_repair_ready {
                            reliable_critical_tail_repair_limit_bytes(
                                base_repair_limit,
                                send_stream.repair_bytes(),
                                mux_limits,
                            )
                        } else {
                            base_repair_limit.min(repair_event_budget)
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
                                "stream_id={} complete={} ranges={} largest_end={} released_bytes={} sent_offset={} sender_queue_bytes={} repair_bytes_after={} repair_frames={} repair_kind={} active_underlay={:?} multipath_repair_alternative={} ack_gap_repair_ready={} base_repair_limit={} repair_limit={} extra_traffic_hint_percent={}",
                                stream_id.0,
                                complete,
                                ranges.len(),
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
                        for frame in repair_frames {
                            let queued = if path_stream.has_recent_live_repair_flight_overlap(
                                &frame,
                                live_repair_retry_after,
                            ) {
                                false
                            } else if critical_tail_repair {
                                if repair_kind == "fin_tail" {
                                    response_sender
                                        .enqueue_critical_tail_repair_frame(frame)
                                        .is_some()
                                } else {
                                    response_sender.enqueue_critical_repair_frame(frame);
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
                                send_path_snapshot,
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
                            send_path_snapshot,
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
            _ = tokio::time::sleep_until(recv_progress_deadline), if path_stream.underlay == UnderlayProtocol::Udp
                && reliable_relay_recv_progress_resend_active(&recv_stream, remote_open) => {
                if enqueue_tcp_recv_progress(
                    &mut response_sender,
                    &recv_stream,
                    &mut recv_progress,
                    send_path_snapshot,
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
                        let owner_tail_read_headroom = reliable_relay_owner_tail_read_headroom(
                            relay_lane,
                            send_path_snapshot,
                            ordered_owner_debt_bytes,
                            response_sender.data_bytes(),
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
        while result.is_ok() && !response_sender.is_empty() {
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
                    wait_for_carrier_capacity_notifies(path_stream.capacity_notifies()).await;
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
            match response_sender
                .dispatch_next(&path_stream, &mut send_stream, last_relay_lane, mux_limits)
                .await
            {
                Ok(dispatch) if dispatch.lane == ReliableRelayQueuedWorkLane::Control => {
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
mod tests {
    use super::*;

    #[test]
    fn stream_fin_waits_for_final_offset_before_close() {
        let mut recv_stream = ReliableRecvStream::new(StreamId(1), MuxLimits::default());
        let mut pending_final_offset = None;

        assert!(
            !receive_stream_fin(&recv_stream, &mut pending_final_offset, 5)
                .expect("record pending fin")
        );
        assert_eq!(pending_final_offset, Some(5));
        assert!(!pending_stream_fin_ready(
            &recv_stream,
            pending_final_offset
        ));

        recv_stream
            .receive_data(0, Bytes::from_static(b"hello"), StreamFlags::NONE)
            .expect("tail data");

        assert!(pending_stream_fin_ready(&recv_stream, pending_final_offset));
    }

    #[test]
    fn terminal_fin_replay_requires_sent_fin_and_completed_owner_bytes() {
        assert!(!stream_terminal_fin_replay_required(
            false, false, true, 0, 64, 64,
        ));
        assert!(!stream_terminal_fin_replay_required(
            true, true, true, 0, 64, 64,
        ));
        assert!(!stream_terminal_fin_replay_required(
            true, false, false, 0, 64, 64,
        ));
        assert!(!stream_terminal_fin_replay_required(
            true, false, true, 1, 64, 64,
        ));
        assert!(!stream_terminal_fin_replay_required(
            true, false, true, 0, 63, 64,
        ));
        assert!(stream_terminal_fin_replay_required(
            true, false, true, 0, 64, 64,
        ));
    }

    #[test]
    fn duplicate_stream_data_below_final_frontier_is_already_delivered() {
        let mut recv_stream = ReliableRecvStream::new(StreamId(1), MuxLimits::default());
        recv_stream
            .receive_data(0, Bytes::from_static(b"hello"), StreamFlags::NONE)
            .expect("receive data");

        assert!(stream_data_range_already_delivered(&recv_stream, 0, 5));
        assert!(!stream_data_range_already_delivered(&recv_stream, 0, 6));
        assert!(!stream_data_range_already_delivered(&recv_stream, 5, 1));
    }

    #[test]
    fn reliable_relay_sender_queue_budget_respects_stream_flow_control_credit() {
        let limits = MuxLimits {
            max_stream_window_bytes: 4,
            max_repair_bytes: 16,
            max_path_flight_bytes: 16,
            max_reliable_relay_chunk_bytes: 16,
            ..MuxLimits::default()
        };
        let mut send_stream = ReliableSendStream::new(StreamId(7), limits);
        let sender_queue = ReliableRelaySenderQueue::default();
        send_stream
            .send_data(Bytes::from_static(b"data"), StreamFlags::NONE)
            .expect("initial window payload");

        assert!(!reliable_relay_can_read_into_sender_queue(
            &send_stream,
            &sender_queue,
            limits,
            16
        ));
        assert_eq!(
            reliable_relay_sender_queue_read_budget(&send_stream, &sender_queue, limits, 16, 16),
            0
        );

        send_stream.update_max_offset(6);
        assert!(reliable_relay_can_read_into_sender_queue(
            &send_stream,
            &sender_queue,
            limits,
            16
        ));
        assert_eq!(
            reliable_relay_sender_queue_read_budget(&send_stream, &sender_queue, limits, 16, 16),
            2
        );
    }

    #[test]
    fn sender_effective_lane_promotes_from_local_or_peer_bulk_evidence() {
        assert_eq!(
            reliable_sender_effective_relay_lane(FlowLane::Latency, FlowLane::Latency),
            FlowLane::Latency
        );
        assert_eq!(
            reliable_sender_effective_relay_lane(FlowLane::Throughput, FlowLane::Latency),
            FlowLane::Throughput
        );
        assert_eq!(
            reliable_sender_effective_relay_lane(FlowLane::Latency, FlowLane::Throughput),
            FlowLane::Throughput
        );
        assert_eq!(
            reliable_sender_effective_relay_lane(FlowLane::Latency, FlowLane::Background),
            FlowLane::Background
        );
    }

    #[test]
    fn response_sender_wait_state_blocks_immediately_without_carrier_credit() {
        let now = tokio::time::Instant::now();
        let retry_delay = Duration::from_millis(10);

        let state = response_sender_wait_state(true, true, false, None, now, retry_delay);

        assert!(state.blocked);
        assert!(!state.ready);
        assert!(state.subscribe_capacity);
        assert_eq!(state.retry_at, Some(now + retry_delay));
    }

    #[test]
    fn response_sender_wait_state_allows_admission_when_carrier_has_credit() {
        let now = tokio::time::Instant::now();
        let retry_delay = Duration::from_millis(10);

        let state = response_sender_wait_state(true, true, true, None, now, retry_delay);

        assert!(!state.blocked);
        assert!(state.ready);
        assert!(
            !state.subscribe_capacity,
            "product-ordering pressure is handled by sender admission, not carrier pipe exhaustion"
        );
        assert_eq!(state.retry_at, None);
    }

    #[test]
    fn response_sender_wait_state_preserves_pending_retry_with_carrier_credit() {
        let now = tokio::time::Instant::now();
        let retry_delay = Duration::from_millis(10);
        let retry_at = now + retry_delay;

        let state = response_sender_wait_state(true, true, true, Some(retry_at), now, retry_delay);

        assert!(state.blocked);
        assert!(!state.ready);
        assert!(state.subscribe_capacity);
        assert_eq!(state.retry_at, Some(retry_at));
    }

    #[test]
    fn ack_gap_repair_requires_multipath_alternative_and_persistent_gap() {
        assert!(!stream_ack_gap_repair_allowed(true, false, true));
        assert!(!stream_ack_gap_repair_allowed(true, true, false));
        assert!(stream_ack_gap_repair_allowed(true, true, true));
        assert!(!stream_ack_gap_repair_allowed(false, true, true));
    }

    #[test]
    fn tail_timer_repair_allows_only_authoritative_or_failed_owner_repair() {
        assert!(
            stream_tail_timer_repair_allowed(false, true),
            "after the owner output is gone, the remaining output is the failover path even though it is no longer a second live alternative"
        );
        assert!(!stream_tail_timer_repair_allowed(false, false));
        assert!(
            stream_tail_timer_repair_allowed(true, false),
            "authoritative ACK-frontier tail repair may use a live alternate"
        );
    }

    #[test]
    fn ack_gap_repair_requires_authoritative_ack_gap_shape() {
        assert!(!stream_ack_ranges_expose_authoritative_gap(
            false,
            &[
                OffsetRange {
                    start: 0,
                    end: 1024,
                },
                OffsetRange {
                    start: 2048,
                    end: 4096,
                },
            ],
        ));
        assert!(!stream_ack_ranges_expose_authoritative_gap(
            true,
            &[OffsetRange {
                start: 0,
                end: 1024,
            }],
        ));
        assert!(stream_ack_ranges_expose_authoritative_gap(
            true,
            &[OffsetRange {
                start: 1024,
                end: 4096,
            }],
        ));
        assert!(stream_ack_ranges_expose_authoritative_gap(
            true,
            &[
                OffsetRange {
                    start: 0,
                    end: 1024,
                },
                OffsetRange {
                    start: 2048,
                    end: 4096,
                },
            ],
        ));
    }

    #[test]
    fn contiguous_ack_frontier_lag_is_tail_guard_not_repair_debt() {
        let ranges = [OffsetRange {
            start: 0,
            end: 1024,
        }];

        assert!(
            !stream_ack_ranges_expose_authoritative_gap(true, &ranges),
            "a contiguous unacknowledged suffix is not an authoritative product repair gap"
        );
        assert_eq!(
            reliable_relay_ordered_owner_debt_bytes(FlowLane::Throughput, true, 1024, 8192,),
            7168,
            "a contiguous unacknowledged suffix is a tail guard for alternate owners"
        );
        assert_eq!(
            reliable_relay_ordered_owner_debt_bytes(FlowLane::Throughput, false, 1024, 8192,),
            7168,
            "an incomplete ACK chunk can still prove the contiguous prefix for owner-tail guarding"
        );
        assert_eq!(
            reliable_relay_ordered_owner_debt_bytes(FlowLane::Throughput, false, 0, 8192,),
            8192,
            "before the first contiguous ACK, already-sent bulk bytes are still owner-tail debt for alternate owners"
        );
        assert_eq!(
            reliable_relay_ordered_owner_debt_bytes(FlowLane::Latency, true, 1024, 8192,),
            0,
            "latency traffic must not be pinned by bulk owner-tail pressure"
        );
    }

    #[test]
    fn bulk_source_read_headroom_uses_reservoir_without_latency_pressure() {
        let mux_limits = MuxLimits::default();
        let payload = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let horizon = bulk_service_horizon_payload_bytes(payload, mux_limits);
        let reservoir = bulk_service_feed_reservoir_payload_bytes(payload, mux_limits);
        let mut service_path =
            PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 100.0, 1_000_000.0);
        service_path.product_progress_rate_bps = Some(1_000_000.0);
        service_path.confidence = 1.0;

        assert_eq!(
            reliable_relay_owner_tail_read_headroom(
                FlowLane::Throughput,
                Some(service_path),
                horizon,
                0,
                mux_limits,
            ),
            reservoir.saturating_sub(horizon),
            "bulk-only Service feed must keep a reservoir beyond the preemptible latency horizon"
        );
    }

    #[test]
    fn bulk_source_read_headroom_uses_horizon_before_service_product_progress() {
        let mux_limits = MuxLimits::default();
        let payload = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let horizon = bulk_service_horizon_payload_bytes(payload, mux_limits);
        let service_path = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 100.0, 1_000_000.0);

        assert_eq!(
            reliable_relay_owner_tail_read_headroom(
                FlowLane::Throughput,
                Some(service_path),
                horizon,
                0,
                mux_limits,
            ),
            0,
            "before Service product progress, no latency pressure must not permit a larger unresolved owner tail"
        );
    }

    #[test]
    fn bulk_source_read_headroom_uses_horizon_until_service_progress_is_confident() {
        let mux_limits = MuxLimits::default();
        let payload = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let horizon = bulk_service_horizon_payload_bytes(payload, mux_limits);
        let mut service_path =
            PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 100.0, 1_000_000.0);
        service_path.product_progress_rate_bps = Some(1_000_000.0);
        service_path.confidence = 0.0;

        assert_eq!(
            reliable_relay_owner_tail_read_headroom(
                FlowLane::Throughput,
                Some(service_path),
                horizon,
                0,
                mux_limits,
            ),
            0,
            "a tiny/app-limited product ACK exposes progress but must not unlock the bulk Service reservoir"
        );
    }

    #[test]
    fn owner_tail_latency_pressure_is_path_local_not_session_global() {
        let mut path = PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 100.0, 1_000_000.0);
        path.session_active_latency_sensitive_flows = 1;
        assert!(
            !reliable_relay_owner_tail_has_latency_pressure(Some(path)),
            "latency work elsewhere in the session must not shrink this Service owner's feed reservoir"
        );

        path.active_latency_sensitive_flows = 1;
        assert!(
            reliable_relay_owner_tail_has_latency_pressure(Some(path)),
            "latency work sharing the same Service path keeps the preemptible horizon cap"
        );
    }

    #[test]
    fn bulk_source_read_headroom_stops_when_latency_pressure_horizon_is_full() {
        let mux_limits = MuxLimits::default();
        let payload = reliable_bulk_carrier_feed_quantum_bytes(mux_limits);
        let horizon = bulk_service_horizon_payload_bytes(payload, mux_limits);
        let mut service_path =
            PathSnapshot::new(PathId(0), UnderlayProtocol::Tcp, 100.0, 1_000_000.0);
        service_path.product_progress_rate_bps = Some(1_000_000.0);
        service_path.active_latency_sensitive_flows = 1;

        assert_eq!(
            reliable_relay_owner_tail_read_headroom(
                FlowLane::Throughput,
                Some(service_path),
                horizon.saturating_sub(payload),
                0,
                mux_limits,
            ),
            payload,
            "bulk Service may read only the remaining owner-tail horizon"
        );
        assert_eq!(
            reliable_relay_owner_tail_read_headroom(
                FlowLane::Throughput,
                Some(service_path),
                horizon.saturating_sub(payload),
                payload,
                mux_limits,
            ),
            0,
            "queued but not yet dispatched OwnerData counts against the same tail horizon"
        );
        assert_eq!(
            reliable_relay_owner_tail_read_headroom(
                FlowLane::Throughput,
                Some(service_path),
                horizon.saturating_add(1),
                0,
                mux_limits,
            ),
            0,
            "once the Service tail exceeds the horizon, reads must pause for ACK or repair progress"
        );
    }

    #[test]
    fn latency_source_read_headroom_is_not_pinned_by_bulk_tail_horizon() {
        assert_eq!(
            reliable_relay_owner_tail_read_headroom(
                FlowLane::Latency,
                None,
                usize::MAX / 2,
                usize::MAX / 2,
                MuxLimits::default(),
            ),
            usize::MAX,
            "latency lane reads remain governed by their own tiny chunking and must not inherit bulk tail pressure"
        );
    }

    #[test]
    fn sparse_ack_largest_end_is_not_contiguous_frontier() {
        let ranges = [
            OffsetRange {
                start: 0,
                end: 1024,
            },
            OffsetRange {
                start: 4096,
                end: 8192,
            },
        ];

        assert_eq!(
            stream_ack_contiguous_frontier(true, &ranges),
            1024,
            "sparse ACK ranges must keep the scheduling frontier at the first hole, not the largest ACK end"
        );
        assert_eq!(
            stream_ack_contiguous_frontier(false, &ranges),
            1024,
            "an incomplete ACK chunk still explicitly proves its contiguous 0-based prefix; incompleteness only forbids inferring gaps from omitted higher ranges"
        );
    }

    #[test]
    fn tail_repair_uses_single_pto_stall_timeout() {
        let last_progress = Instant::now();
        let last_repair = last_progress - Duration::from_secs(1);
        let deadline = reliable_relay_tail_repair_deadline(
            last_progress,
            last_repair,
            None,
            FlowLane::Throughput,
        );
        let expected = tokio::time::Instant::from_std(
            last_progress + reliable_relay_stall_timeout(None, FlowLane::Throughput),
        );

        assert_eq!(deadline, expected);
    }

    #[test]
    fn tail_repair_timer_is_lane_neutral_after_stall_evidence() {
        assert!(
            reliable_relay_tail_repair_timer_active(64, true, false),
            "a complete stalled owner suffix must use bounded alternate-output repair in every reliable lane"
        );
        assert!(
            reliable_relay_tail_repair_timer_active(64, false, true),
            "failed-owner correctness repair must not depend on the product lane"
        );
        assert!(
            !reliable_relay_tail_repair_timer_active(64, false, false),
            "an outstanding suffix without an eligible alternate must remain with its carrier"
        );
        assert!(
            !reliable_relay_tail_repair_timer_active(0, true, true),
            "a fully acknowledged stream must not arm the repair timer"
        );
    }

    #[test]
    fn authoritative_ack_snapshot_does_not_regress_on_stale_or_incomplete_ack() {
        let mut frontier = 0;
        let mut ranges = Vec::new();
        let mut complete = false;
        update_repair_authoritative_ack_snapshot(
            &mut frontier,
            &mut ranges,
            &mut complete,
            true,
            &[OffsetRange { start: 0, end: 128 }],
        );
        update_repair_authoritative_ack_snapshot(
            &mut frontier,
            &mut ranges,
            &mut complete,
            true,
            &[OffsetRange { start: 0, end: 64 }],
        );
        update_repair_authoritative_ack_snapshot(
            &mut frontier,
            &mut ranges,
            &mut complete,
            false,
            &[OffsetRange {
                start: 192,
                end: 256,
            }],
        );

        assert_eq!(frontier, 128);
        assert_eq!(ranges, vec![OffsetRange { start: 0, end: 128 }]);
        assert!(complete);
    }

    #[tokio::test]
    async fn latency_live_owner_tail_repair_dispatches_suffix_on_distinct_repair_without_fin() {
        let limits = MuxLimits::default();
        let stream_id = StreamId(118);
        let owner_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let repair_key = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let (owner_commands, mut owner_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new(
            SessionId(118),
            owner_key.underlay,
            owner_key.path_id,
            owner_commands,
            FlowLane::Latency,
        );
        let (repair_commands, mut repair_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                repair_key.underlay,
                repair_key.path_id,
                repair_commands,
                FlowLane::Latency,
                StreamOpenRole::Repair,
                reliable_relay_buffer_len(limits),
            ),
            ResponseStreamAttachOutcome::Attached
        );

        let (_frame_tx, frame_rx) = mpsc::channel(1);
        let path_stream = ReliablePathStream {
            stream_id,
            max_offset: u64::MAX,
            lane: FlowLane::Latency,
            underlay: owner_key.underlay,
            max_frame_payload_bytes: reliable_relay_buffer_len(limits),
            output: ReliablePathStreamOutput::Switchable(binding.clone()),
            frames: frame_rx,
        };
        let mut send_stream =
            ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
        for value in [0x41, 0x42, 0x43] {
            let frame = send_stream
                .send_data(Bytes::from(vec![value; 64]), StreamFlags::NONE)
                .expect("seed owner response data");
            binding.record_owner_flight(owner_key, &frame);
        }
        let ack_ranges = [OffsetRange { start: 0, end: 128 }];
        let _ = send_stream.apply_ack(&ack_ranges);
        assert_eq!(send_stream.next_offset(), 192);
        assert_eq!(send_stream.repair_bytes(), 64);
        assert!(reliable_relay_tail_repair_timer_active(
            send_stream.repair_bytes(),
            true,
            false,
        ));

        let mut response_sender = ServerResponseSenderService::new_with_performance(
            SessionId(118),
            stream_id,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
        );
        response_sender.record_owner_progress(128);
        let outcome = enqueue_reliable_tail_repair(
            &mut response_sender,
            &path_stream,
            stream_id,
            &send_stream,
            &ack_ranges,
            true,
            None,
            FlowLane::Latency,
            limits,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
            path_stream.max_frame_payload_bytes,
            128,
        );
        assert_eq!(outcome.queued, 1);
        assert!(!outcome.pending);

        let ordered_owner_debt_bytes = send_stream.repair_bytes();
        let dispatch = response_sender
            .dispatch_next_with_ordered_owner_debt(
                &path_stream,
                &mut send_stream,
                FlowLane::Latency,
                limits,
                ordered_owner_debt_bytes,
            )
            .await
            .expect("latency tail repair must dispatch on the distinct Repair output");
        assert_eq!(dispatch.lane, ReliableRelayQueuedWorkLane::Repair);
        assert_eq!(dispatch.selected_path, Some(repair_key));

        let repair_frame = match try_recv_reliable_path_command(&mut repair_receivers) {
            Some(ReliablePathCommand::SendFrame(frame)) => {
                assert!(matches!(
                    &frame,
                    Frame::StreamData {
                        offset: 128,
                        flags,
                        payload,
                        ..
                    } if payload.len() == 64 && !flags.fin
                ));
                frame
            }
            _ => panic!("expected the nonterminal 64-byte suffix on Repair"),
        };
        assert!(try_recv_reliable_path_command(&mut owner_receivers).is_none());
        assert_eq!(binding.ordered_data_owner(), Some(owner_key));
        assert!(path_stream.has_recent_live_repair_flight_overlap(
            &repair_frame,
            reliable_relay_tail_repair_delay(None, FlowLane::Latency),
        ));
    }

    #[test]
    fn sparse_authoritative_ack_does_not_skip_lower_gap_for_live_tail_repair() {
        let limits = MuxLimits::default();
        let stream_id = StreamId(119);
        let owner_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let repair_key = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new(
            SessionId(119),
            owner_key.underlay,
            owner_key.path_id,
            owner_commands,
            FlowLane::Latency,
        );
        let (repair_commands, _repair_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                repair_key.underlay,
                repair_key.path_id,
                repair_commands,
                FlowLane::Latency,
                StreamOpenRole::Repair,
                reliable_relay_buffer_len(limits),
            ),
            ResponseStreamAttachOutcome::Attached
        );

        let (_frame_tx, frame_rx) = mpsc::channel(1);
        let path_stream = ReliablePathStream {
            stream_id,
            max_offset: u64::MAX,
            lane: FlowLane::Latency,
            underlay: owner_key.underlay,
            max_frame_payload_bytes: reliable_relay_buffer_len(limits),
            output: ReliablePathStreamOutput::Switchable(binding.clone()),
            frames: frame_rx,
        };
        let mut send_stream =
            ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
        for value in [0x41, 0x42, 0x43, 0x44] {
            let frame = send_stream
                .send_data(Bytes::from(vec![value; 64]), StreamFlags::NONE)
                .expect("seed owner response data");
            binding.record_owner_flight(owner_key, &frame);
        }
        let ack_ranges = [
            OffsetRange { start: 0, end: 64 },
            OffsetRange {
                start: 128,
                end: 192,
            },
        ];
        let _ = send_stream.apply_ack(&ack_ranges);
        assert_eq!(stream_ack_contiguous_frontier(true, &ack_ranges), 64);
        assert!(!stream_ack_is_authoritative_contiguous_prefix(
            true,
            &ack_ranges,
            64,
        ));
        assert_eq!(send_stream.repair_bytes(), 128);

        let mut response_sender = ServerResponseSenderService::new_with_performance(
            SessionId(119),
            stream_id,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
        );
        let outcome = enqueue_reliable_tail_repair(
            &mut response_sender,
            &path_stream,
            stream_id,
            &send_stream,
            &ack_ranges,
            true,
            None,
            FlowLane::Latency,
            limits,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
            path_stream.max_frame_payload_bytes,
            64,
        );

        assert_eq!(
            outcome.queued, 0,
            "the live-tail timer must not skip an authoritative lower ACK gap"
        );
        assert!(!outcome.pending);
        assert_eq!(response_sender.bytes(), 0);
    }

    #[tokio::test]
    async fn sparse_ack_failed_owner_repair_starts_at_lowest_hole() {
        let limits = MuxLimits::default();
        let stream_id = StreamId(120);
        let owner_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let repair_key = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new(
            SessionId(120),
            owner_key.underlay,
            owner_key.path_id,
            owner_commands.clone(),
            FlowLane::Latency,
        );
        let (repair_commands, mut repair_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                repair_key.underlay,
                repair_key.path_id,
                repair_commands,
                FlowLane::Latency,
                StreamOpenRole::Repair,
                reliable_relay_buffer_len(limits),
            ),
            ResponseStreamAttachOutcome::Attached
        );
        let (_frame_tx, frame_rx) = mpsc::channel(1);
        let path_stream = ReliablePathStream {
            stream_id,
            max_offset: u64::MAX,
            lane: FlowLane::Latency,
            underlay: owner_key.underlay,
            max_frame_payload_bytes: reliable_relay_buffer_len(limits),
            output: ReliablePathStreamOutput::Switchable(binding.clone()),
            frames: frame_rx,
        };
        let mut send_stream =
            ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
        for value in [0x51, 0x52, 0x53, 0x54] {
            let frame = send_stream
                .send_data(Bytes::from(vec![value; 64]), StreamFlags::NONE)
                .expect("seed failed-owner response data");
            binding.record_owner_flight(owner_key, &frame);
        }
        let ack_ranges = [
            OffsetRange { start: 0, end: 64 },
            OffsetRange {
                start: 128,
                end: 192,
            },
        ];
        let _ = send_stream.apply_ack(&ack_ranges);
        binding.release_normalized_acked_ranges(&ack_ranges);
        binding.detach(owner_key, &owner_commands);

        let mut response_sender = ServerResponseSenderService::new_with_performance(
            SessionId(120),
            stream_id,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
        );
        let outcome = enqueue_reliable_tail_repair(
            &mut response_sender,
            &path_stream,
            stream_id,
            &send_stream,
            &ack_ranges,
            true,
            None,
            FlowLane::Latency,
            limits,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
            path_stream.max_frame_payload_bytes,
            64,
        );
        assert!(outcome.queued > 0);
        let ordered_owner_debt_bytes = send_stream.repair_bytes();
        let dispatch = response_sender
            .dispatch_next_with_ordered_owner_debt(
                &path_stream,
                &mut send_stream,
                FlowLane::Latency,
                limits,
                ordered_owner_debt_bytes,
            )
            .await
            .expect("dispatch lowest failed-owner hole");
        assert_eq!(dispatch.selected_path, Some(repair_key));
        assert!(matches!(
            try_recv_reliable_path_command(&mut repair_receivers),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData {
                offset: 64,
                payload,
                ..
            })) if payload.len() == 64
        ));
    }

    #[test]
    fn tail_repair_repeats_after_persistent_delay_without_progress() {
        let last_progress = Instant::now();
        let last_repair = last_progress + reliable_relay_stall_timeout(None, FlowLane::Throughput);
        let deadline = reliable_relay_tail_repair_deadline(
            last_progress,
            last_repair,
            None,
            FlowLane::Throughput,
        );
        let expected = tokio::time::Instant::from_std(
            last_repair
                + reliable_relay_stall_timeout(None, FlowLane::Throughput)
                    .saturating_mul(QUIC_PERSISTENT_CONGESTION_THRESHOLD),
        );

        assert_eq!(deadline, expected);
    }

    #[test]
    fn live_tail_repair_timer_uses_blocking_owner_snapshot_not_fast_alternate() {
        let limits = MuxLimits::default();
        let stream_id = StreamId(110);
        let owner_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let fast_alternate = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        };
        let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new(
            SessionId(110),
            owner_key.underlay,
            owner_key.path_id,
            owner_commands,
            FlowLane::Throughput,
        );
        let (alternate_commands, _alternate_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                fast_alternate.underlay,
                fast_alternate.path_id,
                alternate_commands,
                FlowLane::Throughput,
                StreamOpenRole::Validation,
                reliable_relay_buffer_len(limits),
            ),
            ResponseStreamAttachOutcome::Attached
        );

        let slow_owner_metrics = PathMetrics {
            path_id: owner_key.path_id,
            underlay: owner_key.underlay,
            direction: PathMetricDirection::ServerToClient,
            metric_epoch: metric_epoch_now(),
            metric_age_us: 0,
            min_rtt_us: 480_000,
            srtt_us: 500_000,
            rttvar_us: 60_000,
            jitter_us: 60_000,
            delivery_rate_bps: 80_000_000,
            pacing_rate_bps: 80_000_000,
            loss_ppm: 0,
            ecn_ppm: 0,
            loss_observed: false,
            ecn_observed: false,
            bytes_in_flight: 0,
            queue_bytes: 0,
            inflight_limit_bytes: 0,
            inflight_hi_bytes: 0,
            confidence_ppm: 1_000_000,
            app_limited: false,
            has_ack_derived_data_sample: true,
            data_sample_count: 1,
            data_sample_bytes: 65_536,
        };
        let fast_alternate_metrics = PathMetrics {
            path_id: fast_alternate.path_id,
            underlay: fast_alternate.underlay,
            min_rtt_us: 20_000,
            srtt_us: 25_000,
            rttvar_us: 2_000,
            jitter_us: 2_000,
            delivery_rate_bps: 200_000_000,
            pacing_rate_bps: 200_000_000,
            ..slow_owner_metrics
        };
        binding.update_path_metrics(
            owner_key,
            slow_owner_metrics,
            ServerPathMetricsSource::LocalSender,
        );
        binding.update_path_metrics(
            fast_alternate,
            fast_alternate_metrics,
            ServerPathMetricsSource::LocalSender,
        );

        let (_frame_tx, frame_rx) = mpsc::channel(1);
        let path_stream = ReliablePathStream {
            stream_id,
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: owner_key.underlay,
            max_frame_payload_bytes: reliable_relay_buffer_len(limits),
            output: ReliablePathStreamOutput::Switchable(binding.clone()),
            frames: frame_rx,
        };
        let owner_frame = Frame::StreamData {
            stream_id,
            offset: 1024,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![0x55; 65_536]),
        };
        binding.record_owner_flight(owner_key, &owner_frame);

        let snapshot = path_stream
            .tail_repair_snapshot(1024, FlowLane::Throughput, 65_536)
            .expect("blocking owner path is still attached");

        assert_eq!(snapshot.id, owner_key.path_id);
        assert_eq!(snapshot.underlay, owner_key.underlay);
        assert!(
            transport_pto_from_snapshot(Some(snapshot))
                > transport_pto_from_snapshot(
                    path_stream.send_path_snapshot(FlowLane::Throughput, 65_536)
                ),
            "tail repair timing must follow the blocking OwnerData path, not the fastest attached alternate"
        );
    }

    #[test]
    fn failed_owner_tail_repair_deadline_is_immediate_for_repairable_detached_owner() {
        let limits = MuxLimits::default();
        let stream_id = StreamId(111);
        let owner_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let failover_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new(
            SessionId(111),
            owner_key.underlay,
            owner_key.path_id,
            owner_commands.clone(),
            FlowLane::Throughput,
        );
        let (failover_commands, _failover_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                failover_key.underlay,
                failover_key.path_id,
                failover_commands,
                FlowLane::Throughput,
                StreamOpenRole::Active,
                reliable_relay_buffer_len(limits),
            ),
            ResponseStreamAttachOutcome::Attached
        );

        let (_frame_tx, frame_rx) = mpsc::channel(1);
        let path_stream = ReliablePathStream {
            stream_id,
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: failover_key.underlay,
            max_frame_payload_bytes: reliable_relay_buffer_len(limits),
            output: ReliablePathStreamOutput::Switchable(binding.clone()),
            frames: frame_rx,
        };
        let mut send_stream =
            ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
        let frame = send_stream
            .prepare_data(Bytes::from(vec![0x51; 4096]), StreamFlags::NONE)
            .expect("prepare owner data");
        send_stream
            .commit_prepared_data(&frame)
            .expect("commit owner data");
        binding.record_owner_flight(owner_key, &frame);
        binding.detach(owner_key, &owner_commands);
        let ack_ranges = [OffsetRange {
            start: 0,
            end: 1024,
        }];
        let _ = send_stream.apply_ack(&ack_ranges);

        let last_progress = Instant::now();
        let last_repair = last_progress - Duration::from_secs(1);
        let generic_deadline = reliable_relay_tail_repair_deadline(
            last_progress,
            last_repair,
            None,
            FlowLane::Throughput,
        );
        let failover_deadline = reliable_relay_effective_tail_repair_deadline(
            last_progress,
            last_repair,
            None,
            FlowLane::Throughput,
            reliable_failed_owner_tail_repair_ready(
                &path_stream,
                &send_stream,
                &ack_ranges,
                true,
                1024,
                limits,
            ),
        );

        assert_eq!(
            failover_deadline,
            tokio::time::Instant::from_std(last_progress),
            "detached-owner tail repair should not wait a generic PTO before failing over"
        );
        assert!(
            failover_deadline < generic_deadline,
            "failed-owner repair timing must be faster than live-owner tail repair"
        );
    }

    #[test]
    fn failed_owner_tail_repair_retry_uses_single_pto_not_persistent_backoff() {
        let last_progress = Instant::now();
        let last_repair = last_progress + Duration::from_millis(1);
        let slow_stale_owner = PathSnapshot::new(PathId(9), UnderlayProtocol::Tcp, 20.0, 1.0);

        let deadline = reliable_relay_effective_tail_repair_deadline(
            last_progress,
            last_repair,
            Some(slow_stale_owner),
            FlowLane::Throughput,
            true,
        );
        let expected = tokio::time::Instant::from_std(
            last_repair + reliable_relay_stall_timeout(None, FlowLane::Throughput),
        );

        assert_eq!(
            deadline, expected,
            "failed-owner repair may fire immediately once, then retries one bounded repair quantum per PTO; persistent backoff is for live owner congestion recovery, not detached-owner failover"
        );
    }

    #[test]
    fn persistent_ack_gap_repair_limit_uses_critical_event_quantum() {
        let limits = MuxLimits::default();
        let base_limit = BBR_MAX_SEND_QUANTUM_BYTES.min(reliable_relay_buffer_len(limits));
        let repair_debt = base_limit.saturating_mul(32);

        let repair_limit =
            reliable_critical_tail_repair_limit_bytes(base_limit, repair_debt, limits);

        assert_eq!(
            repair_limit, base_limit,
            "persistent ACK-gap repair may bypass optional budget, but one event repairs only one bounded quantum"
        );
    }

    #[test]
    fn persistent_ack_gap_repair_limit_ignores_optional_budget_exhaustion() {
        let limits = MuxLimits::default();
        let base_limit = BBR_MAX_SEND_QUANTUM_BYTES.min(reliable_relay_buffer_len(limits));
        let small_tail = base_limit.saturating_sub(1024).max(1);

        let repair_limit =
            reliable_critical_tail_repair_limit_bytes(base_limit, small_tail, limits);

        assert_eq!(
            repair_limit, small_tail,
            "persistent ACK-gap repair is correctness repair and must not depend on optional duplicate/probe budget"
        );
        assert_eq!(
            reliable_critical_tail_repair_limit_bytes(
                limits.max_repair_bytes.saturating_add(base_limit),
                limits.max_repair_bytes.saturating_add(base_limit),
                limits
            ),
            limits.max_repair_bytes.min(limits.max_path_flight_bytes),
            "persistent ACK-gap repair remains bounded by configured repair/path-flight caps"
        );
    }

    #[test]
    fn final_tail_critical_repair_limit_can_exceed_optional_budget() {
        let limits = MuxLimits::default();
        let base_limit = BBR_MAX_SEND_QUANTUM_BYTES.min(reliable_relay_buffer_len(limits));
        let resource_cap = limits.max_repair_bytes.min(limits.max_path_flight_bytes);
        let small_tail = base_limit.saturating_sub(1024).max(1);
        let repair_debt = base_limit.saturating_mul(8);

        assert_eq!(
            reliable_critical_tail_repair_limit_bytes(base_limit, small_tail, limits),
            small_tail,
            "terminal owner-tail repair may close a retained final tail even after optional repair budget is exhausted"
        );

        assert_eq!(
            reliable_critical_tail_repair_limit_bytes(base_limit, repair_debt, limits),
            base_limit,
            "terminal owner-tail repair keeps a bounded critical path for final stream closure"
        );
        assert_eq!(
            reliable_critical_tail_repair_limit_bytes(
                resource_cap.saturating_add(base_limit),
                resource_cap.saturating_add(base_limit),
                limits
            ),
            resource_cap,
            "critical final-tail repair remains bounded by configured repair resources"
        );
    }

    #[test]
    fn live_tail_stall_repair_is_not_queued_even_with_optional_budget() {
        let limits = MuxLimits::default();
        let stream_id = StreamId(98);
        let base_limit = BBR_MAX_SEND_QUANTUM_BYTES.min(reliable_relay_buffer_len(limits));
        let repair_debt = base_limit.saturating_mul(8);
        let mut response_sender = ServerResponseSenderService::new_with_performance(
            SessionId(98),
            stream_id,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 1,
            },
        );
        let initial_budget = response_sender.repair_extra_budget_remaining(limits);
        assert!(initial_budget > 0);

        let (commands, _receivers) = reliable_path_command_channels(8);
        let (_frame_tx, frame_rx) = mpsc::channel(1);
        let path_stream = ReliablePathStream {
            stream_id,
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: UnderlayProtocol::Tcp,
            max_frame_payload_bytes: reliable_relay_buffer_len(limits),
            output: ReliablePathStreamOutput::fixed(
                UnderlayProtocol::Tcp,
                PathId(0),
                commands,
                limits,
            ),
            frames: frame_rx,
        };
        let mut send_stream =
            ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
        send_stream
            .send_data(Bytes::from(vec![0x32; repair_debt]), StreamFlags::NONE)
            .expect("owner data");
        let ack_frontier = base_limit as u64;

        let outcome = enqueue_reliable_tail_repair(
            &mut response_sender,
            &path_stream,
            stream_id,
            &send_stream,
            &[OffsetRange {
                start: 0,
                end: ack_frontier,
            }],
            true,
            None,
            FlowLane::Throughput,
            limits,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 1,
            },
            path_stream.max_frame_payload_bytes,
            ack_frontier,
        );

        assert_eq!(
            outcome.queued, 0,
            "live contiguous owner-tail bytes are neither ACK-gap nor final-tail correctness repair"
        );
        assert!(!outcome.pending);
        assert!(
            outcome.record_as_repair_attempt(),
            "an empty tail-repair scan must still advance the retry timer"
        );
    }

    #[test]
    fn failed_owner_tail_repair_uses_remaining_output_after_persistent_stall() {
        let limits = MuxLimits::default();
        let stream_id = StreamId(99);
        let owner_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let failover_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new(
            SessionId(99),
            owner_key.underlay,
            owner_key.path_id,
            owner_commands.clone(),
            FlowLane::Throughput,
        );
        let (failover_commands, _failover_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                failover_key.underlay,
                failover_key.path_id,
                failover_commands,
                FlowLane::Throughput,
                StreamOpenRole::Active,
                reliable_relay_buffer_len(limits),
            ),
            ResponseStreamAttachOutcome::Attached
        );
        let (_frame_tx, frame_rx) = mpsc::channel(1);
        let path_stream = ReliablePathStream {
            stream_id,
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: failover_key.underlay,
            max_frame_payload_bytes: reliable_relay_buffer_len(limits),
            output: ReliablePathStreamOutput::Switchable(binding.clone()),
            frames: frame_rx,
        };
        let mut send_stream =
            ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
        let frame = send_stream
            .prepare_data(Bytes::from(vec![0x42; 4096]), StreamFlags::NONE)
            .expect("prepare owner data");
        send_stream
            .commit_prepared_data(&frame)
            .expect("commit owner data");
        binding.record_owner_flight(owner_key, &frame);
        binding.detach(owner_key, &owner_commands);
        let ack_ranges = [OffsetRange {
            start: 0,
            end: 1024,
        }];
        let _ = send_stream.apply_ack(&ack_ranges);

        let mut response_sender = ServerResponseSenderService::new_with_performance(
            SessionId(99),
            stream_id,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
        );
        response_sender.record_owner_progress(1024);

        let outcome = enqueue_reliable_tail_repair(
            &mut response_sender,
            &path_stream,
            stream_id,
            &send_stream,
            &ack_ranges,
            true,
            None,
            FlowLane::Throughput,
            limits,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
            path_stream.max_frame_payload_bytes,
            1024,
        );

        assert_eq!(
            outcome.queued, 1,
            "a detached owner path turns a persistent contiguous tail into failover repair on the remaining output"
        );
        assert!(!outcome.pending);
    }

    #[test]
    fn failed_owner_tail_repair_queues_single_service_quantum_not_recovery_burst() {
        let limits = MuxLimits::default();
        let stream_id = StreamId(121);
        let owner_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let failover_key = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new(
            SessionId(121),
            owner_key.underlay,
            owner_key.path_id,
            owner_commands.clone(),
            FlowLane::Throughput,
        );
        let (failover_commands, _failover_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                failover_key.underlay,
                failover_key.path_id,
                failover_commands,
                FlowLane::Throughput,
                StreamOpenRole::Active,
                reliable_relay_buffer_len(limits),
            ),
            ResponseStreamAttachOutcome::Attached
        );
        let (_frame_tx, frame_rx) = mpsc::channel(1);
        let path_stream = ReliablePathStream {
            stream_id,
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: failover_key.underlay,
            max_frame_payload_bytes: reliable_relay_buffer_len(limits),
            output: ReliablePathStreamOutput::Switchable(binding.clone()),
            frames: frame_rx,
        };
        let mut send_stream =
            ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
        let unresolved_payload_len = reliable_relay_buffer_len(limits)
            .saturating_add(BBR_MAX_SEND_QUANTUM_BYTES)
            .min(limits.max_payload_bytes);
        let frame = send_stream
            .prepare_data(
                Bytes::from(vec![0x52; unresolved_payload_len]),
                StreamFlags::NONE,
            )
            .expect("prepare owner data");
        send_stream
            .commit_prepared_data(&frame)
            .expect("commit owner data");
        binding.record_owner_flight(owner_key, &frame);
        binding.detach(owner_key, &owner_commands);
        let ack_ranges = [OffsetRange {
            start: 0,
            end: 1024,
        }];
        let _ = send_stream.apply_ack(&ack_ranges);

        let mut response_sender = ServerResponseSenderService::new_with_performance(
            SessionId(121),
            stream_id,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
        );
        response_sender.record_owner_progress(1024);

        let outcome = enqueue_reliable_tail_repair(
            &mut response_sender,
            &path_stream,
            stream_id,
            &send_stream,
            &ack_ranges,
            true,
            None,
            FlowLane::Throughput,
            limits,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
            path_stream.max_frame_payload_bytes,
            1024,
        );

        assert_eq!(outcome.queued, 1);
        assert!(
            response_sender.bytes() <= BBR_MAX_SEND_QUANTUM_BYTES,
            "failed-owner recovery is correctness repair: one stall/failover event must queue one service repair quantum, not a multi-frame burst that inflates overhead under flapping"
        );
    }

    #[test]
    fn unknown_owner_tail_repair_uses_remaining_output_after_persistent_stall() {
        let limits = MuxLimits::default();
        let stream_id = StreamId(119);
        let owner_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let failover_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new(
            SessionId(119),
            owner_key.underlay,
            owner_key.path_id,
            owner_commands.clone(),
            FlowLane::Throughput,
        );
        let (failover_commands, _failover_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                failover_key.underlay,
                failover_key.path_id,
                failover_commands,
                FlowLane::Throughput,
                StreamOpenRole::Active,
                reliable_relay_buffer_len(limits),
            ),
            ResponseStreamAttachOutcome::Attached
        );
        binding.detach(owner_key, &owner_commands);

        let (_frame_tx, frame_rx) = mpsc::channel(1);
        let path_stream = ReliablePathStream {
            stream_id,
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: failover_key.underlay,
            max_frame_payload_bytes: reliable_relay_buffer_len(limits),
            output: ReliablePathStreamOutput::Switchable(binding.clone()),
            frames: frame_rx,
        };
        let mut send_stream =
            ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
        let frame = send_stream
            .prepare_data(Bytes::from(vec![0x43; 4096]), StreamFlags::NONE)
            .expect("prepare owner data");
        send_stream
            .commit_prepared_data(&frame)
            .expect("commit owner data");
        let ack_ranges = [OffsetRange {
            start: 0,
            end: 1024,
        }];
        let _ = send_stream.apply_ack(&ack_ranges);

        let mut response_sender = ServerResponseSenderService::new_with_performance(
            SessionId(119),
            stream_id,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
        );
        response_sender.record_owner_progress(1024);

        let outcome = enqueue_reliable_tail_repair(
            &mut response_sender,
            &path_stream,
            stream_id,
            &send_stream,
            &ack_ranges,
            true,
            None,
            FlowLane::Throughput,
            limits,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
            path_stream.max_frame_payload_bytes,
            1024,
        );

        assert_eq!(
            outcome.queued, 1,
            "when retained owner bytes have no live owner and no path-flight record, persistent tail repair must still use a live survivor instead of deadlocking"
        );
        assert!(!outcome.pending);
    }

    #[tokio::test]
    async fn unknown_owner_tail_repair_dispatches_as_path_failure_repair() {
        let limits = MuxLimits::default();
        let stream_id = StreamId(120);
        let owner_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let failover_key = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new(
            SessionId(120),
            owner_key.underlay,
            owner_key.path_id,
            owner_commands.clone(),
            FlowLane::Throughput,
        );
        let (failover_commands, mut failover_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                failover_key.underlay,
                failover_key.path_id,
                failover_commands,
                FlowLane::Throughput,
                StreamOpenRole::Active,
                reliable_relay_buffer_len(limits),
            ),
            ResponseStreamAttachOutcome::Attached
        );
        binding.detach(owner_key, &owner_commands);

        let (_frame_tx, frame_rx) = mpsc::channel(1);
        let path_stream = ReliablePathStream {
            stream_id,
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: failover_key.underlay,
            max_frame_payload_bytes: reliable_relay_buffer_len(limits),
            output: ReliablePathStreamOutput::Switchable(binding),
            frames: frame_rx,
        };
        let mut send_stream =
            ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
        let frame = send_stream
            .prepare_data(Bytes::from(vec![0x43; 4096]), StreamFlags::NONE)
            .expect("prepare owner data");
        send_stream
            .commit_prepared_data(&frame)
            .expect("commit owner data");
        let ack_ranges = [OffsetRange {
            start: 0,
            end: 1024,
        }];
        let _ = send_stream.apply_ack(&ack_ranges);

        let mut response_sender = ServerResponseSenderService::new_with_performance(
            SessionId(120),
            stream_id,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
        );
        response_sender.record_owner_progress(1024);

        let outcome = enqueue_reliable_tail_repair(
            &mut response_sender,
            &path_stream,
            stream_id,
            &send_stream,
            &ack_ranges,
            true,
            None,
            FlowLane::Throughput,
            limits,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
            path_stream.max_frame_payload_bytes,
            1024,
        );
        assert_eq!(outcome.queued, 1);

        let ordered_owner_debt_bytes = send_stream.repair_bytes();
        let dispatch = response_sender
            .dispatch_next_with_ordered_owner_debt(
                &path_stream,
                &mut send_stream,
                FlowLane::Throughput,
                limits,
                ordered_owner_debt_bytes,
            )
            .await
            .expect("unknown-owner tail repair must be failover-dispatchable");

        assert_eq!(dispatch.lane, ReliableRelayQueuedWorkLane::Repair);
        assert_eq!(dispatch.selected_path, Some(failover_key));
        assert!(matches!(
            try_recv_reliable_path_command(&mut failover_receivers),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData { .. }))
        ));
    }

    #[test]
    fn live_owner_no_ack_frontier_tail_repair_waits_for_authoritative_gap() {
        let limits = MuxLimits::default();
        let stream_id = StreamId(121);
        let owner_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let repair_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new(
            SessionId(121),
            owner_key.underlay,
            owner_key.path_id,
            owner_commands,
            FlowLane::Throughput,
        );
        let (repair_commands, _repair_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                repair_key.underlay,
                repair_key.path_id,
                repair_commands,
                FlowLane::Throughput,
                StreamOpenRole::Active,
                reliable_relay_buffer_len(limits),
            ),
            ResponseStreamAttachOutcome::Attached
        );

        let (_frame_tx, frame_rx) = mpsc::channel(1);
        let path_stream = ReliablePathStream {
            stream_id,
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: repair_key.underlay,
            max_frame_payload_bytes: reliable_relay_buffer_len(limits),
            output: ReliablePathStreamOutput::Switchable(binding.clone()),
            frames: frame_rx,
        };
        let mut send_stream =
            ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
        let frame = send_stream
            .prepare_data(Bytes::from(vec![0x48; 4096]), StreamFlags::NONE)
            .expect("prepare owner data");
        send_stream
            .commit_prepared_data(&frame)
            .expect("commit owner data");
        binding.record_owner_flight(owner_key, &frame);

        let mut response_sender = ServerResponseSenderService::new_with_performance(
            SessionId(121),
            stream_id,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
        );
        let outcome = enqueue_reliable_tail_repair(
            &mut response_sender,
            &path_stream,
            stream_id,
            &send_stream,
            &[],
            false,
            None,
            FlowLane::Throughput,
            limits,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
            path_stream.max_frame_payload_bytes,
            0,
        );

        assert_eq!(
            outcome.queued, 0,
            "no ACK frontier is not an authoritative product gap; live-owner recovery must wait for ACK progress, failed-owner evidence, or terminal-tail repair"
        );
        assert!(!outcome.pending);
    }

    #[test]
    fn live_owner_no_ack_frontier_tail_repair_does_not_probe_prefix() {
        let limits = MuxLimits::default();
        let stream_id = StreamId(122);
        let owner_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let repair_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new(
            SessionId(122),
            owner_key.underlay,
            owner_key.path_id,
            owner_commands,
            FlowLane::Throughput,
        );
        let (repair_commands, _repair_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                repair_key.underlay,
                repair_key.path_id,
                repair_commands,
                FlowLane::Throughput,
                StreamOpenRole::Active,
                reliable_relay_buffer_len(limits),
            ),
            ResponseStreamAttachOutcome::Attached
        );

        let (_frame_tx, frame_rx) = mpsc::channel(1);
        let path_stream = ReliablePathStream {
            stream_id,
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: repair_key.underlay,
            max_frame_payload_bytes: reliable_relay_buffer_len(limits),
            output: ReliablePathStreamOutput::Switchable(binding.clone()),
            frames: frame_rx,
        };
        let mut send_stream =
            ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
        let total = reliable_relay_buffer_len(limits).saturating_mul(4);
        let mut remaining = total;
        while remaining > 0 {
            let chunk = remaining.min(limits.max_payload_bytes);
            let frame = send_stream
                .prepare_data(Bytes::from(vec![0x48; chunk]), StreamFlags::NONE)
                .expect("prepare owner data");
            send_stream
                .commit_prepared_data(&frame)
                .expect("commit owner data");
            binding.record_owner_flight(owner_key, &frame);
            remaining = remaining.saturating_sub(chunk);
        }

        let mut response_sender = ServerResponseSenderService::new_with_performance(
            SessionId(122),
            stream_id,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
        );
        let outcome = enqueue_reliable_tail_repair(
            &mut response_sender,
            &path_stream,
            stream_id,
            &send_stream,
            &[],
            false,
            None,
            FlowLane::Throughput,
            limits,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
            path_stream.max_frame_payload_bytes,
            0,
        );

        assert_eq!(
            outcome.queued, 0,
            "no-frontier live-owner data may still be in carrier recovery and must not become product RepairData"
        );
        assert!(!outcome.pending);
        assert_eq!(response_sender.bytes(), 0);
    }

    #[test]
    fn unknown_owner_tail_repair_without_ack_frontier_waits() {
        let limits = MuxLimits::default();
        let stream_id = StreamId(120);
        let owner_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let failover_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new(
            SessionId(120),
            owner_key.underlay,
            owner_key.path_id,
            owner_commands.clone(),
            FlowLane::Throughput,
        );
        let (failover_commands, _failover_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                failover_key.underlay,
                failover_key.path_id,
                failover_commands,
                FlowLane::Throughput,
                StreamOpenRole::Active,
                reliable_relay_buffer_len(limits),
            ),
            ResponseStreamAttachOutcome::Attached
        );
        binding.detach(owner_key, &owner_commands);

        let (_frame_tx, frame_rx) = mpsc::channel(1);
        let path_stream = ReliablePathStream {
            stream_id,
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: failover_key.underlay,
            max_frame_payload_bytes: reliable_relay_buffer_len(limits),
            output: ReliablePathStreamOutput::Switchable(binding),
            frames: frame_rx,
        };
        let mut send_stream =
            ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
        let frame = send_stream
            .prepare_data(Bytes::from(vec![0x44; 4096]), StreamFlags::NONE)
            .expect("prepare owner data");
        send_stream
            .commit_prepared_data(&frame)
            .expect("commit owner data");

        let mut response_sender = ServerResponseSenderService::new_with_performance(
            SessionId(120),
            stream_id,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
        );

        let outcome = enqueue_reliable_tail_repair(
            &mut response_sender,
            &path_stream,
            stream_id,
            &send_stream,
            &[],
            false,
            None,
            FlowLane::Throughput,
            limits,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
            path_stream.max_frame_payload_bytes,
            0,
        );

        assert_eq!(
            outcome.queued, 0,
            "unknown-owner repair needs an ACK frontier; without one it can duplicate the entire startup tail and inflate overhead"
        );
        assert!(!outcome.pending);
    }

    #[test]
    fn failed_owner_tail_repair_does_not_duplicate_queued_repair_range() {
        let limits = MuxLimits::default();
        let stream_id = StreamId(109);
        let owner_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let failover_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new(
            SessionId(109),
            owner_key.underlay,
            owner_key.path_id,
            owner_commands.clone(),
            FlowLane::Throughput,
        );
        let (failover_commands, _failover_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                failover_key.underlay,
                failover_key.path_id,
                failover_commands,
                FlowLane::Throughput,
                StreamOpenRole::Active,
                reliable_relay_buffer_len(limits),
            ),
            ResponseStreamAttachOutcome::Attached
        );

        let (_frame_tx, frame_rx) = mpsc::channel(1);
        let path_stream = ReliablePathStream {
            stream_id,
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: failover_key.underlay,
            max_frame_payload_bytes: reliable_relay_buffer_len(limits),
            output: ReliablePathStreamOutput::Switchable(binding.clone()),
            frames: frame_rx,
        };
        let mut send_stream =
            ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
        let frame = send_stream
            .prepare_data(Bytes::from(vec![0x43; 4096]), StreamFlags::NONE)
            .expect("prepare owner data");
        send_stream
            .commit_prepared_data(&frame)
            .expect("commit owner data");
        binding.record_owner_flight(owner_key, &frame);
        binding.detach(owner_key, &owner_commands);
        let ack_ranges = [OffsetRange {
            start: 0,
            end: 1024,
        }];
        let _ = send_stream.apply_ack(&ack_ranges);

        let mut response_sender = ServerResponseSenderService::new_with_performance(
            SessionId(109),
            stream_id,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
        );
        response_sender.record_owner_progress(1024);
        let performance = MppPerformanceConfig {
            extra_traffic_hint_percent: 5,
        };

        let first = enqueue_reliable_tail_repair(
            &mut response_sender,
            &path_stream,
            stream_id,
            &send_stream,
            &ack_ranges,
            true,
            None,
            FlowLane::Throughput,
            limits,
            performance,
            path_stream.max_frame_payload_bytes,
            1024,
        );
        let queued_bytes_after_first = response_sender.bytes();
        let second = enqueue_reliable_tail_repair(
            &mut response_sender,
            &path_stream,
            stream_id,
            &send_stream,
            &ack_ranges,
            true,
            None,
            FlowLane::Throughput,
            limits,
            performance,
            path_stream.max_frame_payload_bytes,
            1024,
        );

        assert_eq!(first.queued, 1);
        assert!(!first.pending);
        assert_eq!(
            second.queued, 0,
            "tail repair must not enqueue the same RepairData range while it is already queued"
        );
        assert!(
            second.pending,
            "already queued RepairData should count as a pending repair attempt so the tail timer backs off"
        );
        assert_eq!(response_sender.bytes(), queued_bytes_after_first);
    }

    #[test]
    fn tail_repair_treats_live_inflight_repair_as_pending() {
        let limits = MuxLimits::default();
        let stream_id = StreamId(127);
        let owner_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let repair_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new(
            SessionId(127),
            owner_key.underlay,
            owner_key.path_id,
            owner_commands,
            FlowLane::Throughput,
        );
        let (repair_commands, _repair_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                repair_key.underlay,
                repair_key.path_id,
                repair_commands,
                FlowLane::Throughput,
                StreamOpenRole::Active,
                reliable_relay_buffer_len(limits),
            ),
            ResponseStreamAttachOutcome::Attached
        );

        let (_frame_tx, frame_rx) = mpsc::channel(1);
        let path_stream = ReliablePathStream {
            stream_id,
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: owner_key.underlay,
            max_frame_payload_bytes: reliable_relay_buffer_len(limits),
            output: ReliablePathStreamOutput::Switchable(binding.clone()),
            frames: frame_rx,
        };
        let mut send_stream =
            ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
        let frame = send_stream
            .prepare_data(Bytes::from(vec![0x49; 4096]), StreamFlags::NONE)
            .expect("prepare owner data");
        send_stream
            .commit_prepared_data(&frame)
            .expect("commit owner data");
        binding.record_owner_flight(owner_key, &frame);
        let ack_ranges = [OffsetRange {
            start: 0,
            end: 1024,
        }];
        let _ = send_stream.apply_ack(&ack_ranges);
        let inflight_repair = send_stream
            .retransmission_frames_after_ack_frontier(&ack_ranges, 1024)
            .into_iter()
            .next()
            .expect("expected frontier repair frame");
        binding.record_repair_flight(repair_key, &inflight_repair);

        let mut response_sender = ServerResponseSenderService::new_with_performance(
            SessionId(127),
            stream_id,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
        );
        response_sender.record_owner_progress(1024);

        let outcome = enqueue_reliable_tail_repair(
            &mut response_sender,
            &path_stream,
            stream_id,
            &send_stream,
            &ack_ranges,
            true,
            None,
            FlowLane::Throughput,
            limits,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
            path_stream.max_frame_payload_bytes,
            1024,
        );

        assert_eq!(
            outcome.queued, 0,
            "live in-flight RepairData for the same range must not be stacked"
        );
        assert!(
            outcome.pending,
            "live in-flight RepairData should keep the tail repair timer backed off"
        );
        assert_eq!(response_sender.bytes(), 0);
    }

    #[test]
    fn persistent_tail_repair_waits_when_live_repair_copy_is_in_flight() {
        let limits = MuxLimits::default();
        let stream_id = StreamId(105);
        let owner_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let repair_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new(
            SessionId(105),
            owner_key.underlay,
            owner_key.path_id,
            owner_commands,
            FlowLane::Throughput,
        );
        let (repair_commands, _repair_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                repair_key.underlay,
                repair_key.path_id,
                repair_commands,
                FlowLane::Throughput,
                StreamOpenRole::Active,
                reliable_relay_buffer_len(limits),
            ),
            ResponseStreamAttachOutcome::Attached
        );

        let (_frame_tx, frame_rx) = mpsc::channel(1);
        let path_stream = ReliablePathStream {
            stream_id,
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: owner_key.underlay,
            max_frame_payload_bytes: reliable_relay_buffer_len(limits),
            output: ReliablePathStreamOutput::Switchable(binding.clone()),
            frames: frame_rx,
        };
        let mut send_stream =
            ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
        let frame = send_stream
            .prepare_data(Bytes::from(vec![0x48; 4096]), StreamFlags::NONE)
            .expect("prepare owner data");
        send_stream
            .commit_prepared_data(&frame)
            .expect("commit owner data");
        binding.record_owner_flight(owner_key, &frame);
        binding.record_repair_flight(repair_key, &frame);
        let ack_ranges = [OffsetRange {
            start: 0,
            end: 1024,
        }];
        let _ = send_stream.apply_ack(&ack_ranges);

        let mut response_sender = ServerResponseSenderService::new_with_performance(
            SessionId(105),
            stream_id,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
        );
        response_sender.record_owner_progress(1024);

        let outcome = enqueue_reliable_tail_repair(
            &mut response_sender,
            &path_stream,
            stream_id,
            &send_stream,
            &ack_ranges,
            true,
            None,
            FlowLane::Throughput,
            limits,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
            path_stream.max_frame_payload_bytes,
            1024,
        );

        assert_eq!(
            outcome.queued, 0,
            "persistent tail repair must not stack another copy while a live RepairData flight already covers the frontier range"
        );
        assert!(
            outcome.pending,
            "live in-flight RepairData should back off the tail repair timer"
        );
        assert_eq!(response_sender.bytes(), 0);
    }

    #[test]
    fn stale_live_repair_flight_does_not_block_terminal_tail_retry() {
        let limits = MuxLimits::default();
        let stream_id = StreamId(106);
        let owner_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let repair_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new(
            SessionId(106),
            owner_key.underlay,
            owner_key.path_id,
            owner_commands,
            FlowLane::Throughput,
        );
        let (repair_commands, _repair_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                repair_key.underlay,
                repair_key.path_id,
                repair_commands,
                FlowLane::Throughput,
                StreamOpenRole::Active,
                reliable_relay_buffer_len(limits),
            ),
            ResponseStreamAttachOutcome::Attached
        );

        let (_frame_tx, frame_rx) = mpsc::channel(1);
        let path_stream = ReliablePathStream {
            stream_id,
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: owner_key.underlay,
            max_frame_payload_bytes: reliable_relay_buffer_len(limits),
            output: ReliablePathStreamOutput::Switchable(binding.clone()),
            frames: frame_rx,
        };
        let mut send_stream =
            ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
        let frame = send_stream
            .prepare_data(Bytes::from(vec![0x4a; 4096]), StreamFlags::NONE)
            .expect("prepare owner data");
        send_stream
            .commit_prepared_data(&frame)
            .expect("commit owner data");
        binding.record_owner_flight(owner_key, &frame);
        let ack_ranges = [OffsetRange {
            start: 0,
            end: 1024,
        }];
        let _ = send_stream.apply_ack(&ack_ranges);
        let inflight_repair = send_stream
            .retransmission_frames_after_ack_frontier(&ack_ranges, 1024)
            .into_iter()
            .next()
            .expect("expected frontier repair frame");
        binding.record_repair_flight(repair_key, &inflight_repair);
        binding.age_repair_flights_for_test(
            reliable_relay_tail_repair_delay(None, FlowLane::Throughput)
                .saturating_add(Duration::from_millis(1)),
        );

        let mut response_sender = ServerResponseSenderService::new_with_performance(
            SessionId(106),
            stream_id,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
        );
        response_sender.record_owner_progress(1024);

        let outcome = enqueue_reliable_tail_repair(
            &mut response_sender,
            &path_stream,
            stream_id,
            &send_stream,
            &ack_ranges,
            true,
            None,
            FlowLane::Throughput,
            limits,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
            path_stream.max_frame_payload_bytes,
            1024,
        );

        assert_eq!(
            outcome.queued, 1,
            "stale unacked RepairData must not suppress correctness repair forever"
        );
        assert!(
            !outcome.pending,
            "stale RepairData should be retried instead of keeping the tail timer backed off"
        );
        assert!(response_sender.bytes() > 0);
    }

    #[tokio::test]
    async fn persistent_live_owner_tail_repair_waits_when_distinct_alternate_lacks_queue_credit() {
        let limits = MuxLimits::default();
        let stream_id = StreamId(124);
        let owner_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let repair_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let (owner_commands, mut owner_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new(
            SessionId(124),
            owner_key.underlay,
            owner_key.path_id,
            owner_commands,
            FlowLane::Throughput,
        );
        let (repair_commands, _repair_receivers) = reliable_path_command_channels(1);
        let repair_commands_for_fill = repair_commands.clone();
        assert_eq!(
            binding.attach(
                repair_key.underlay,
                repair_key.path_id,
                repair_commands,
                FlowLane::Throughput,
                StreamOpenRole::Active,
                reliable_relay_buffer_len(limits),
            ),
            ResponseStreamAttachOutcome::Attached
        );

        let (_frame_tx, frame_rx) = mpsc::channel(1);
        let path_stream = ReliablePathStream {
            stream_id,
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: owner_key.underlay,
            max_frame_payload_bytes: reliable_relay_buffer_len(limits),
            output: ReliablePathStreamOutput::Switchable(binding.clone()),
            frames: frame_rx,
        };
        let mut send_stream =
            ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
        let frame = send_stream
            .prepare_data(Bytes::from(vec![0x55; 4096]), StreamFlags::NONE)
            .expect("prepare owner data");
        send_stream
            .commit_prepared_data(&frame)
            .expect("commit owner data");
        binding.record_owner_flight(owner_key, &frame);
        let ack_ranges = [OffsetRange {
            start: 0,
            end: 1024,
        }];
        let _ = send_stream.apply_ack(&ack_ranges);

        let mut response_sender = ServerResponseSenderService::new_with_performance(
            SessionId(124),
            stream_id,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
        );
        response_sender.record_owner_progress(1024);

        let outcome = enqueue_reliable_tail_repair(
            &mut response_sender,
            &path_stream,
            stream_id,
            &send_stream,
            &ack_ranges,
            true,
            None,
            FlowLane::Throughput,
            limits,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
            path_stream.max_frame_payload_bytes,
            1024,
        );
        assert_eq!(outcome.queued, 1);

        repair_commands_for_fill
            .try_enqueue_admitted_frame(
                Frame::StreamData {
                    stream_id,
                    offset: 4096,
                    flags: StreamFlags::NONE,
                    payload: Bytes::from_static(b"queued"),
                },
                FlowLane::Throughput,
            )
            .expect("test setup fills alternate data queue");

        let dispatch = response_sender
            .dispatch_next_with_ordered_owner_debt(
                &path_stream,
                &mut send_stream,
                FlowLane::Throughput,
                limits,
                0,
            )
            .await;

        assert!(matches!(dispatch, Err(RuntimeError::SenderServiceBlocked)));
        assert!(
            try_recv_reliable_path_command(&mut owner_receivers).is_none(),
            "live-owner tail repair must wait rather than retransmit on its owner"
        );
        let completed = [OffsetRange {
            start: 0,
            end: send_stream.next_offset(),
        }];
        let _ = send_stream.apply_ack(&completed);
        path_stream.release_normalized_acked_ranges(&completed);
        response_sender.release_normalized_acked_repairs(&completed);
        assert!(
            response_sender.is_empty(),
            "owner ACK progress must remove a blocked queued live-tail repair before FIN or later data"
        );
    }

    #[tokio::test]
    async fn final_tail_repair_dispatches_on_service_when_alternate_lacks_queue_credit() {
        let limits = MuxLimits::default();
        let stream_id = StreamId(125);
        let owner_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let repair_key = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(1),
        };
        let (owner_commands, mut owner_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new(
            SessionId(125),
            owner_key.underlay,
            owner_key.path_id,
            owner_commands,
            FlowLane::Latency,
        );
        let (repair_commands, _repair_receivers) = reliable_path_command_channels(1);
        let repair_commands_for_fill = repair_commands.clone();
        assert_eq!(
            binding.attach(
                repair_key.underlay,
                repair_key.path_id,
                repair_commands,
                FlowLane::Latency,
                StreamOpenRole::Active,
                reliable_relay_buffer_len(limits),
            ),
            ResponseStreamAttachOutcome::Attached
        );

        let (_frame_tx, frame_rx) = mpsc::channel(1);
        let path_stream = ReliablePathStream {
            stream_id,
            max_offset: u64::MAX,
            lane: FlowLane::Latency,
            underlay: owner_key.underlay,
            max_frame_payload_bytes: reliable_relay_buffer_len(limits),
            output: ReliablePathStreamOutput::Switchable(binding.clone()),
            frames: frame_rx,
        };
        let mut send_stream =
            ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
        let frame = send_stream
            .prepare_data(Bytes::from(vec![0x56; 192]), StreamFlags::NONE)
            .expect("prepare owner data");
        send_stream
            .commit_prepared_data(&frame)
            .expect("commit owner data");
        binding.record_owner_flight(owner_key, &frame);
        let ack_ranges = [OffsetRange { start: 0, end: 128 }];
        let _ = send_stream.apply_ack(&ack_ranges);

        let mut response_sender = ServerResponseSenderService::new_with_performance(
            SessionId(125),
            stream_id,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
        );
        response_sender.record_owner_progress(128);

        let (repair_frames, blocked_frontier_offset, same_output_frontier_retransmit) =
            prefix_final_tail_repair_frames_with_available_output(
                &path_stream,
                stream_final_offset_tail_repair_frames(&send_stream, &ack_ranges, 64, true, true),
            );
        assert_eq!(blocked_frontier_offset, None);
        assert!(!same_output_frontier_retransmit);
        assert_eq!(repair_frames.len(), 1);
        for frame in repair_frames {
            let _ = response_sender.enqueue_critical_tail_repair_frame(frame);
        }

        repair_commands_for_fill
            .try_enqueue_admitted_frame(
                Frame::StreamData {
                    stream_id,
                    offset: 192,
                    flags: StreamFlags::NONE,
                    payload: Bytes::from_static(b"queued"),
                },
                reliable_path_stream_ordered_queue_lane(),
            )
            .expect("test setup fills alternate data queue");

        response_sender
            .dispatch_next_with_ordered_owner_debt(
                &path_stream,
                &mut send_stream,
                FlowLane::Latency,
                limits,
                0,
            )
            .await
            .expect("final-tail RepairData must use the Service path when the alternate has no queue credit");

        let command = try_recv_reliable_path_command(&mut owner_receivers)
            .expect("expected same-Service final-tail repair frame");
        match command {
            ReliablePathCommand::SendFrame(Frame::StreamData {
                offset, payload, ..
            }) => {
                assert_eq!(offset, 128);
                assert_eq!(payload.len(), 64);
            }
            _ => panic!("expected same-Service final-tail repair STREAM_DATA"),
        }
    }

    #[tokio::test]
    async fn final_tail_repair_dispatches_on_service_when_no_alternate_survives() {
        let limits = MuxLimits::default();
        let stream_id = StreamId(126);
        let owner_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let (owner_commands, mut owner_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new(
            SessionId(126),
            owner_key.underlay,
            owner_key.path_id,
            owner_commands,
            FlowLane::Latency,
        );

        let (_frame_tx, frame_rx) = mpsc::channel(1);
        let path_stream = ReliablePathStream {
            stream_id,
            max_offset: u64::MAX,
            lane: FlowLane::Latency,
            underlay: owner_key.underlay,
            max_frame_payload_bytes: reliable_relay_buffer_len(limits),
            output: ReliablePathStreamOutput::Switchable(binding.clone()),
            frames: frame_rx,
        };
        let mut send_stream =
            ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
        let frame = send_stream
            .prepare_data(Bytes::from(vec![0x57; 192]), StreamFlags::NONE)
            .expect("prepare owner data");
        send_stream
            .commit_prepared_data(&frame)
            .expect("commit owner data");
        binding.record_owner_flight(owner_key, &frame);
        let ack_ranges = [OffsetRange { start: 0, end: 128 }];
        let _ = send_stream.apply_ack(&ack_ranges);

        let mut response_sender = ServerResponseSenderService::new_with_performance(
            SessionId(126),
            stream_id,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
        );
        response_sender.record_owner_progress(128);

        let (repair_frames, blocked_frontier_offset, same_output_frontier_retransmit) =
            prefix_final_tail_repair_frames_with_available_output(
                &path_stream,
                stream_final_offset_tail_repair_frames(&send_stream, &ack_ranges, 64, true, true),
            );
        assert_eq!(blocked_frontier_offset, None);
        assert!(same_output_frontier_retransmit);
        assert_eq!(repair_frames.len(), 1);
        for frame in repair_frames {
            let _ = response_sender.enqueue_critical_tail_repair_frame(frame);
        }

        response_sender
            .dispatch_next_with_ordered_owner_debt(
                &path_stream,
                &mut send_stream,
                FlowLane::Latency,
                limits,
                0,
            )
            .await
            .expect("final-tail RepairData must use the only Service survivor");

        let command = try_recv_reliable_path_command(&mut owner_receivers)
            .expect("expected Service final-tail repair frame");
        match command {
            ReliablePathCommand::SendFrame(Frame::StreamData {
                offset, payload, ..
            }) => {
                assert_eq!(offset, 128);
                assert_eq!(payload.len(), 64);
            }
            _ => panic!("expected Service final-tail repair STREAM_DATA"),
        }
    }

    #[tokio::test]
    async fn failed_owner_repair_without_ack_frontier_starts_at_zero() {
        let limits = MuxLimits::default();
        let stream_id = StreamId(103);
        let owner_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let failover_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new(
            SessionId(103),
            owner_key.underlay,
            owner_key.path_id,
            owner_commands.clone(),
            FlowLane::Throughput,
        );
        let (failover_commands, mut failover_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                failover_key.underlay,
                failover_key.path_id,
                failover_commands,
                FlowLane::Throughput,
                StreamOpenRole::Active,
                reliable_relay_buffer_len(limits),
            ),
            ResponseStreamAttachOutcome::Attached
        );

        let (_frame_tx, frame_rx) = mpsc::channel(1);
        let path_stream = ReliablePathStream {
            stream_id,
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: failover_key.underlay,
            max_frame_payload_bytes: reliable_relay_buffer_len(limits),
            output: ReliablePathStreamOutput::Switchable(binding.clone()),
            frames: frame_rx,
        };
        let mut send_stream =
            ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
        let frame = send_stream
            .prepare_data(Bytes::from(vec![0x46; 4096]), StreamFlags::NONE)
            .expect("prepare owner data");
        send_stream
            .commit_prepared_data(&frame)
            .expect("commit owner data");
        binding.record_owner_flight(owner_key, &frame);
        binding.detach(owner_key, &owner_commands);

        let mut response_sender = ServerResponseSenderService::new_with_performance(
            SessionId(103),
            stream_id,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
        );

        let outcome = enqueue_reliable_tail_repair(
            &mut response_sender,
            &path_stream,
            stream_id,
            &send_stream,
            &[],
            false,
            None,
            FlowLane::Throughput,
            limits,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
            path_stream.max_frame_payload_bytes,
            0,
        );

        assert_eq!(
            outcome.queued, 1,
            "failed-owner repair must retransmit from offset zero when no response ACK frontier exists"
        );
        assert!(!outcome.pending);
        response_sender
            .dispatch_next_with_ordered_owner_debt(
                &path_stream,
                &mut send_stream,
                FlowLane::Throughput,
                limits,
                0,
            )
            .await
            .expect("dispatch failed-owner repair");
        let command =
            try_recv_reliable_path_command(&mut failover_receivers).expect("repair frame");
        match command {
            ReliablePathCommand::SendFrame(Frame::StreamData {
                offset, payload, ..
            }) => {
                assert_eq!(offset, 0);
                assert!(!payload.is_empty());
            }
            _ => panic!("expected failed-owner repair STREAM_DATA"),
        }
    }

    #[test]
    fn live_owner_tail_without_ack_frontier_does_not_repair_on_alternate() {
        let limits = MuxLimits::default();
        let stream_id = StreamId(104);
        let owner_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let alternative_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new(
            SessionId(104),
            owner_key.underlay,
            owner_key.path_id,
            owner_commands,
            FlowLane::Throughput,
        );
        let (alternative_commands, _alternative_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                alternative_key.underlay,
                alternative_key.path_id,
                alternative_commands,
                FlowLane::Throughput,
                StreamOpenRole::Active,
                reliable_relay_buffer_len(limits),
            ),
            ResponseStreamAttachOutcome::Attached
        );

        let (_frame_tx, frame_rx) = mpsc::channel(1);
        let path_stream = ReliablePathStream {
            stream_id,
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: owner_key.underlay,
            max_frame_payload_bytes: reliable_relay_buffer_len(limits),
            output: ReliablePathStreamOutput::Switchable(binding),
            frames: frame_rx,
        };
        let mut send_stream =
            ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
        let frame = send_stream
            .prepare_data(Bytes::from(vec![0x47; 4096]), StreamFlags::NONE)
            .expect("prepare owner data");
        send_stream
            .commit_prepared_data(&frame)
            .expect("commit owner data");

        let mut response_sender = ServerResponseSenderService::new_with_performance(
            SessionId(104),
            stream_id,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
        );

        let outcome = enqueue_reliable_tail_repair(
            &mut response_sender,
            &path_stream,
            stream_id,
            &send_stream,
            &[],
            false,
            None,
            FlowLane::Throughput,
            limits,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
            path_stream.max_frame_payload_bytes,
            0,
        );

        assert_eq!(
            outcome.queued, 0,
            "without a complete ACK frontier or failed owner, live owner bytes are normal in-flight data and must not be duplicated onto an alternate"
        );
        assert!(!outcome.pending);
    }

    #[test]
    fn failed_owner_tail_repair_is_not_blocked_by_optional_budget() {
        let limits = MuxLimits::default();
        let stream_id = StreamId(101);
        let owner_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let failover_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new(
            SessionId(101),
            owner_key.underlay,
            owner_key.path_id,
            owner_commands.clone(),
            FlowLane::Throughput,
        );
        let (failover_commands, _failover_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                failover_key.underlay,
                failover_key.path_id,
                failover_commands,
                FlowLane::Throughput,
                StreamOpenRole::Active,
                reliable_relay_buffer_len(limits),
            ),
            ResponseStreamAttachOutcome::Attached
        );

        let (_frame_tx, frame_rx) = mpsc::channel(1);
        let path_stream = ReliablePathStream {
            stream_id,
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: failover_key.underlay,
            max_frame_payload_bytes: reliable_relay_buffer_len(limits),
            output: ReliablePathStreamOutput::Switchable(binding.clone()),
            frames: frame_rx,
        };
        let mut send_stream =
            ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
        let frame = send_stream
            .prepare_data(Bytes::from(vec![0x44; 4096]), StreamFlags::NONE)
            .expect("prepare owner data");
        send_stream
            .commit_prepared_data(&frame)
            .expect("commit owner data");
        binding.record_owner_flight(owner_key, &frame);
        binding.detach(owner_key, &owner_commands);
        let ack_ranges = [OffsetRange {
            start: 0,
            end: 1024,
        }];
        let _ = send_stream.apply_ack(&ack_ranges);

        let mut response_sender = ServerResponseSenderService::new_with_performance(
            SessionId(101),
            stream_id,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
        );
        let optional_budget = response_sender.repair_extra_budget_remaining(limits);
        assert!(optional_budget > 0);
        assert!(
            response_sender
                .enqueue_repair_frame_with_priority(
                    Frame::StreamData {
                        stream_id,
                        offset: 10_000,
                        flags: StreamFlags::NONE,
                        payload: Bytes::from(vec![0x99; optional_budget]),
                    },
                    limits,
                    true,
                )
                .is_some()
        );
        assert_eq!(
            response_sender.repair_extra_event_budget_remaining(limits),
            0
        );

        let outcome = enqueue_reliable_tail_repair(
            &mut response_sender,
            &path_stream,
            stream_id,
            &send_stream,
            &ack_ranges,
            true,
            None,
            FlowLane::Throughput,
            limits,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
            path_stream.max_frame_payload_bytes,
            1024,
        );

        assert!(
            outcome.queued > 0,
            "failed-owner tail recovery is correctness repair and must not depend on optional duplicate/probe budget"
        );
        assert!(!outcome.pending);
    }

    #[test]
    fn persistent_ack_gap_tail_timer_does_not_duplicate_ack_gap_controller() {
        let limits = MuxLimits::default();
        let stream_id = StreamId(102);
        let owner_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let repair_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new(
            SessionId(102),
            owner_key.underlay,
            owner_key.path_id,
            owner_commands,
            FlowLane::Throughput,
        );
        let (repair_commands, _repair_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                repair_key.underlay,
                repair_key.path_id,
                repair_commands,
                FlowLane::Throughput,
                StreamOpenRole::Active,
                reliable_relay_buffer_len(limits),
            ),
            ResponseStreamAttachOutcome::Attached
        );

        let (_frame_tx, frame_rx) = mpsc::channel(1);
        let path_stream = ReliablePathStream {
            stream_id,
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: owner_key.underlay,
            max_frame_payload_bytes: reliable_relay_buffer_len(limits),
            output: ReliablePathStreamOutput::Switchable(binding.clone()),
            frames: frame_rx,
        };
        let mut send_stream =
            ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
        let frame = send_stream
            .prepare_data(Bytes::from(vec![0x45; 4096]), StreamFlags::NONE)
            .expect("prepare owner data");
        send_stream
            .commit_prepared_data(&frame)
            .expect("commit owner data");
        binding.record_owner_flight(owner_key, &frame);
        let ack_ranges = [
            OffsetRange {
                start: 0,
                end: 1024,
            },
            OffsetRange {
                start: 2048,
                end: 4096,
            },
        ];
        let _ = send_stream.apply_ack(&ack_ranges);

        let mut response_sender = ServerResponseSenderService::new_with_performance(
            SessionId(102),
            stream_id,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
        );
        let optional_budget = response_sender.repair_extra_budget_remaining(limits);
        assert!(optional_budget > 0);
        assert!(
            response_sender
                .enqueue_repair_frame_with_priority(
                    Frame::StreamData {
                        stream_id,
                        offset: 10_000,
                        flags: StreamFlags::NONE,
                        payload: Bytes::from(vec![0x99; optional_budget]),
                    },
                    limits,
                    true,
                )
                .is_some()
        );
        assert_eq!(
            response_sender.repair_extra_event_budget_remaining(limits),
            0
        );

        let outcome = enqueue_reliable_tail_repair(
            &mut response_sender,
            &path_stream,
            stream_id,
            &send_stream,
            &ack_ranges,
            true,
            None,
            FlowLane::Throughput,
            limits,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
            path_stream.max_frame_payload_bytes,
            4096,
        );

        assert_eq!(
            outcome.queued, 0,
            "persistent ACK gaps are repaired by the ACK-gap controller; the tail timer must not duplicate live-owner gap repair"
        );
        assert!(!outcome.pending);
    }

    #[test]
    fn persistent_live_owner_tail_repair_queues_repairdata_without_service_migration() {
        let limits = MuxLimits::default();
        let stream_id = StreamId(100);
        let owner_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(0),
        };
        let alternative_key = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(1),
        };
        let (owner_commands, _owner_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new(
            SessionId(100),
            owner_key.underlay,
            owner_key.path_id,
            owner_commands,
            FlowLane::Throughput,
        );
        let (alternative_commands, _alternative_receivers) = reliable_path_command_channels(8);
        assert_eq!(
            binding.attach(
                alternative_key.underlay,
                alternative_key.path_id,
                alternative_commands,
                FlowLane::Throughput,
                StreamOpenRole::Active,
                reliable_relay_buffer_len(limits),
            ),
            ResponseStreamAttachOutcome::Attached
        );

        let (_frame_tx, frame_rx) = mpsc::channel(1);
        let path_stream = ReliablePathStream {
            stream_id,
            max_offset: u64::MAX,
            lane: FlowLane::Throughput,
            underlay: owner_key.underlay,
            max_frame_payload_bytes: reliable_relay_buffer_len(limits),
            output: ReliablePathStreamOutput::Switchable(binding.clone()),
            frames: frame_rx,
        };
        let mut send_stream =
            ReliableSendStream::new_with_initial_max_offset(stream_id, limits, u64::MAX);
        let repair_debt = reliable_relay_buffer_len(limits).saturating_mul(4);
        let mut remaining = repair_debt;
        while remaining > 0 {
            let chunk = remaining.min(limits.max_payload_bytes);
            let frame = send_stream
                .send_data(Bytes::from(vec![0x43; chunk]), StreamFlags::NONE)
                .expect("seed owner data");
            binding.record_owner_flight(owner_key, &frame);
            remaining = remaining.saturating_sub(chunk);
        }
        let ack_ranges = [OffsetRange {
            start: 0,
            end: 1024,
        }];
        let _ = send_stream.apply_ack(&ack_ranges);
        assert!(
            send_stream.repair_bytes() > reliable_relay_buffer_len(limits),
            "test must cover a retained tail larger than one bounded repair event"
        );

        let mut response_sender = ServerResponseSenderService::new_with_performance(
            SessionId(100),
            stream_id,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
        );
        response_sender.record_owner_progress(1024);

        let outcome = enqueue_reliable_tail_repair(
            &mut response_sender,
            &path_stream,
            stream_id,
            &send_stream,
            &ack_ranges,
            true,
            None,
            FlowLane::Throughput,
            limits,
            MppPerformanceConfig {
                extra_traffic_hint_percent: 5,
            },
            path_stream.max_frame_payload_bytes,
            1024,
        );

        assert_eq!(
            outcome.queued, 1,
            "a persistent live-owner tail stall should reinject the lowest blocked range as RepairData on an alternate output without migrating Service ownership"
        );
        assert!(!outcome.pending);
        assert_eq!(
            binding.ordered_data_owner(),
            Some(owner_key),
            "tail repair is RepairData; it must not rewrite the Service owner"
        );
    }

    #[test]
    fn ack_gap_repair_still_repairs_authoritative_ack_gap() {
        let limits = MuxLimits::default();
        let mut send_stream = ReliableSendStream::new(StreamId(9), limits);
        send_stream
            .send_data(Bytes::from_static(&[7; 4096]), StreamFlags::NONE)
            .expect("send stream data");

        let repair_frames = stream_ack_gap_repair_frames(
            &send_stream,
            &[
                OffsetRange {
                    start: 0,
                    end: 1024,
                },
                OffsetRange {
                    start: 2048,
                    end: 4096,
                },
            ],
            4096,
            true,
            true,
            true,
        );

        assert_eq!(repair_frames.len(), 1);
        assert_eq!(
            reliable_stream_frame_extent(&repair_frames[0]),
            Some((1024, 2048, 1024))
        );
    }

    #[test]
    fn final_offset_tail_repair_can_recover_unacked_terminal_tail() {
        let limits = MuxLimits::default();
        let mut send_stream = ReliableSendStream::new(StreamId(9), limits);
        send_stream
            .send_data(Bytes::from_static(&[7; 4096]), StreamFlags::NONE)
            .expect("send stream data");

        let repair_frames = stream_final_offset_tail_repair_frames(
            &send_stream,
            &[OffsetRange {
                start: 0,
                end: 1024,
            }],
            4096,
            true,
            true,
        );

        assert_eq!(repair_frames.len(), 1);
        assert_eq!(
            reliable_stream_frame_extent(&repair_frames[0]),
            Some((1024, 4096, 3072))
        );
    }

    #[test]
    fn final_offset_tail_repair_can_use_service_when_no_alternate_survives() {
        let limits = MuxLimits::default();
        let mut send_stream = ReliableSendStream::new(StreamId(9), limits);
        send_stream
            .send_data(Bytes::from_static(&[7; 4096]), StreamFlags::NONE)
            .expect("send stream data");

        let repair_frames = stream_final_offset_tail_repair_frames(
            &send_stream,
            &[OffsetRange {
                start: 0,
                end: 1024,
            }],
            4096,
            true,
            true,
        );

        assert_eq!(repair_frames.len(), 1);
        assert_eq!(
            reliable_stream_frame_extent(&repair_frames[0]),
            Some((1024, 4096, 3072)),
            "terminal final-tail RepairData is connection completion traffic and may use the Service survivor after stall evidence"
        );
    }

    #[test]
    fn final_offset_tail_repair_can_recover_tail_with_no_ack_frontier() {
        let limits = MuxLimits::default();
        let mut send_stream = ReliableSendStream::new(StreamId(9), limits);
        send_stream
            .send_data(Bytes::from_static(&[7; 4096]), StreamFlags::NONE)
            .expect("send stream data");

        let repair_frames =
            stream_final_offset_tail_repair_frames(&send_stream, &[], 4096, true, true);

        assert_eq!(repair_frames.len(), 1);
        assert_eq!(
            reliable_stream_frame_extent(&repair_frames[0]),
            Some((0, 4096, 4096)),
            "a closed stream with no response ACK frontier must be able to repair the retained owner tail from offset zero"
        );
    }

    #[test]
    fn final_tail_repair_ready_allows_closed_no_ack_frontier_after_deadline() {
        let limits = MuxLimits::default();
        let mut send_stream = ReliableSendStream::new(StreamId(9), limits);
        send_stream
            .send_data(Bytes::from_static(&[7; 4096]), StreamFlags::NONE)
            .expect("send stream data");
        let now = tokio::time::Instant::now();

        assert!(reliable_final_tail_repair_ready(
            true,
            &send_stream,
            &[],
            0,
            now,
            now,
        ));
    }

    #[test]
    fn final_offset_tail_repair_waits_for_persistent_stall_evidence() {
        let limits = MuxLimits::default();
        let mut send_stream = ReliableSendStream::new(StreamId(9), limits);
        send_stream
            .send_data(Bytes::from_static(&[7; 4096]), StreamFlags::NONE)
            .expect("send stream data");

        let repair_frames = stream_final_offset_tail_repair_frames(
            &send_stream,
            &[OffsetRange {
                start: 0,
                end: 1024,
            }],
            4096,
            true,
            false,
        );

        assert!(
            repair_frames.is_empty(),
            "known final offset is not enough to reinject a contiguous owner tail before persistent stall/failure evidence"
        );
    }

    #[test]
    fn ack_gap_repair_progress_keeps_growing_hole_identity() {
        let mut progress = ReliableAckGapRepairProgress::default();
        let first = [
            OffsetRange {
                start: 0,
                end: 110_098,
            },
            OffsetRange {
                start: 112_318,
                end: 114_538,
            },
        ];
        let grown = [
            OffsetRange {
                start: 0,
                end: 110_098,
            },
            OffsetRange {
                start: 113_428,
                end: 116_758,
            },
        ];
        let now = Instant::now();
        let interval = reliable_stream_recv_progress_interval(None, FlowLane::Throughput);
        let repair_delay = reliable_ack_gap_repair_delay(None, FlowLane::Throughput);

        assert!(!progress.repair_ready_at(
            true,
            &first,
            Some(UnderlayProtocol::Udp),
            true,
            repair_delay,
            now,
        ));
        assert!(!progress.repair_ready_at(
            true,
            &grown,
            Some(UnderlayProtocol::Udp),
            true,
            repair_delay,
            now + interval,
        ));
        assert!(
            progress.repair_ready_at(
                true,
                &grown,
                Some(UnderlayProtocol::Udp),
                true,
                repair_delay,
                now + repair_delay,
            ),
            "a growing ACK horizon with the same missing frontier is one persistent hole"
        );
        assert!(!progress.repair_ready_at(
            true,
            &grown,
            Some(UnderlayProtocol::Udp),
            true,
            repair_delay,
            now + repair_delay + Duration::from_millis(1),
        ));
    }

    #[test]
    fn ack_gap_repair_progress_resets_when_frontier_advances() {
        let mut progress = ReliableAckGapRepairProgress::default();
        let first = [
            OffsetRange {
                start: 0,
                end: 1024,
            },
            OffsetRange {
                start: 4096,
                end: 8192,
            },
        ];
        let advanced = [
            OffsetRange {
                start: 0,
                end: 2048,
            },
            OffsetRange {
                start: 4096,
                end: 8192,
            },
        ];
        let now = Instant::now();
        let repair_delay = reliable_ack_gap_repair_delay(None, FlowLane::Throughput);

        assert!(!progress.repair_ready_at(
            true,
            &first,
            Some(UnderlayProtocol::Udp),
            true,
            repair_delay,
            now,
        ));
        assert!(!progress.repair_ready_at(
            true,
            &advanced,
            Some(UnderlayProtocol::Udp),
            true,
            repair_delay,
            now + repair_delay,
        ));
        assert!(progress.repair_ready_at(
            true,
            &advanced,
            Some(UnderlayProtocol::Udp),
            true,
            repair_delay,
            now + repair_delay + repair_delay,
        ));
    }

    #[test]
    fn ack_gap_repair_waits_for_persistent_gap_on_reliable_carriers() {
        let ranges = [
            OffsetRange {
                start: 0,
                end: 64 * 1024,
            },
            OffsetRange {
                start: 128 * 1024,
                end: 192 * 1024,
            },
        ];
        let now = Instant::now();
        let repair_delay = reliable_relay_stall_timeout(None, FlowLane::Throughput)
            .saturating_mul(QUIC_PERSISTENT_CONGESTION_THRESHOLD);

        for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
            let mut progress = ReliableAckGapRepairProgress::default();
            assert!(!progress.repair_ready_at(
                true,
                &ranges,
                Some(underlay),
                true,
                repair_delay,
                now,
            ));
            assert!(!progress.repair_ready_at(
                true,
                &ranges,
                Some(underlay),
                true,
                repair_delay,
                now + repair_delay - Duration::from_millis(1),
            ));
            assert!(
                progress.repair_ready_at(
                    true,
                    &ranges,
                    Some(underlay),
                    true,
                    repair_delay,
                    now + repair_delay,
                ),
                "{underlay:?} product repair should wait for a persistent ordered-stream gap",
            );
            assert!(!progress.repair_ready_at(
                true,
                &ranges,
                Some(underlay),
                true,
                repair_delay,
                now + repair_delay + Duration::from_millis(1),
            ));
        }
    }
}
