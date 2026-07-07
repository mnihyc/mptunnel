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

/// Product tail repair is reinjection onto an independent subflow, not a
/// second retransmission layer behind the same reliable TCP/QUIC stream.
///
/// Same-output retransmission cannot overtake the missing carrier bytes on an
/// in-order reliable carrier, but it does consume tunnel traffic and can create
/// duplicate ACK/repair feedback loops. This guard keeps repair available for
/// real multipath failover while preventing single-path QUIC/TCP repair storms.
pub(super) fn stream_tail_repair_allowed(has_multipath_repair_alternative: bool) -> bool {
    has_multipath_repair_alternative
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

#[allow(clippy::too_many_arguments)]
pub(super) fn reliable_relay_authoritative_owner_debt_bytes(
    lane: FlowLane,
    repair_bytes: usize,
    ack_complete: bool,
    ack_ranges: &[OffsetRange],
    ack_frontier: u64,
    next_offset: u64,
    last_ack_progress_at: Instant,
    now: Instant,
    path: Option<PathSnapshot>,
) -> usize {
    if !relay_lane_is_bulk(lane)
        || repair_bytes == 0
        || ack_frontier >= next_offset
        || !ack_complete
    {
        return 0;
    }
    if stream_ack_ranges_expose_authoritative_gap(ack_complete, ack_ranges) {
        return repair_bytes;
    }
    if ack_frontier == 0 {
        return 0;
    }
    if now.duration_since(last_ack_progress_at) >= reliable_relay_stall_timeout(path, lane) {
        repair_bytes
    } else {
        0
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn reliable_relay_authoritative_owner_debt_state(
    lane: FlowLane,
    repair_bytes: usize,
    ack_complete: bool,
    ack_ranges: &[OffsetRange],
    ack_frontier: u64,
    next_offset: u64,
    last_ack_progress_at: Instant,
    now: Instant,
    path: Option<PathSnapshot>,
) -> usize {
    reliable_relay_authoritative_owner_debt_bytes(
        lane,
        repair_bytes,
        ack_complete,
        ack_ranges,
        ack_frontier,
        next_offset,
        last_ack_progress_at,
        now,
        path,
    )
}

fn reliable_relay_current_authoritative_owner_debt_bytes(
    lane: FlowLane,
    send_stream: &ReliableSendStream,
    ack_complete: bool,
    ack_ranges: &[OffsetRange],
    ack_frontier: u64,
    last_ack_progress_at: Instant,
    path: Option<PathSnapshot>,
) -> usize {
    reliable_relay_authoritative_owner_debt_state(
        lane,
        send_stream.repair_bytes(),
        ack_complete,
        ack_ranges,
        ack_frontier,
        send_stream.next_offset(),
        last_ack_progress_at,
        Instant::now(),
        path,
    )
}

pub(super) fn reliable_relay_tail_repair_deadline(
    last_progress_at: Instant,
    path: Option<PathSnapshot>,
    lane: FlowLane,
) -> tokio::time::Instant {
    tokio::time::Instant::from_std(
        last_progress_at
            + reliable_relay_stall_timeout(path, lane)
                .saturating_mul(QUIC_PERSISTENT_CONGESTION_THRESHOLD),
    )
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
    if stream_ack_gap_repair_allowed(
        complete,
        has_multipath_repair_alternative,
        ack_gap_repair_ready,
    ) {
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
    if stream_ack_gap_repair_allowed(
        complete,
        has_multipath_repair_alternative,
        ack_gap_repair_ready,
    ) {
        send_stream.retransmission_frames_for_normalized_ack_gaps(ranges, byte_limit)
    } else {
        Vec::new()
    }
}

pub(super) fn stream_tail_stall_repair_frames(
    send_stream: &ReliableSendStream,
    ranges: &[OffsetRange],
    byte_limit: usize,
    complete: bool,
) -> (Vec<Frame>, &'static str) {
    if complete {
        let gap_frames = send_stream.retransmission_frames_for_ack_gaps(ranges, byte_limit);
        if !gap_frames.is_empty() {
            return (gap_frames, "ack_gap");
        }
        let tail_frames = send_stream.retransmission_frames_after_ack_frontier(ranges, byte_limit);
        if !tail_frames.is_empty() {
            return (tail_frames, "owner_tail");
        }
    }
    (Vec::new(), "none")
}

pub(super) fn stream_final_offset_tail_repair_frames(
    send_stream: &ReliableSendStream,
    ranges: &[OffsetRange],
    byte_limit: usize,
    final_offset_known: bool,
    has_multipath_repair_alternative: bool,
) -> Vec<Frame> {
    if !final_offset_known || !has_multipath_repair_alternative || byte_limit == 0 {
        return Vec::new();
    }
    let largest_ack_end = ranges.iter().map(|range| range.end).max().unwrap_or(0);
    if largest_ack_end >= send_stream.next_offset() {
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
    let startup_window = RELIABLE_UDP_INITIAL_PRODUCT_WINDOW_BYTES
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
    if front_has_carrier_credit {
        return ResponseSenderWaitState {
            blocked: false,
            ready: queue_ready,
            subscribe_capacity: false,
            retry_at: None,
        };
    }
    let retry_at = retry_at
        .filter(|deadline| *deadline > now)
        .unwrap_or(now + retry_delay);
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

pub(super) fn reliable_tail_stall_repair_limit_bytes(
    base_repair_limit: usize,
    repair_debt_bytes: usize,
    budget_remaining: usize,
    mux_limits: MuxLimits,
) -> usize {
    if repair_debt_bytes == 0 || budget_remaining == 0 {
        return 0;
    }
    let resource_cap = mux_limits
        .max_repair_bytes
        .min(mux_limits.max_path_flight_bytes)
        .max(1);
    repair_debt_bytes
        .max(base_repair_limit.max(1))
        .min(resource_cap)
        .min(budget_remaining)
}

pub(super) fn reliable_critical_tail_repair_limit_bytes(
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
    repair_debt_bytes.min(resource_cap)
}

pub(super) fn reliable_critical_tail_repair_is_over_budget(
    budget_remaining: usize,
    repair_limit: usize,
) -> bool {
    budget_remaining == 0 && repair_limit > 0
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

pub(super) fn prefix_repair_frames_with_available_output(
    path_stream: &ReliablePathStream,
    repair_frames: Vec<Frame>,
    allow_same_output_frontier_retransmit: bool,
) -> (Vec<Frame>, Option<u64>) {
    let mut accepted = Vec::with_capacity(repair_frames.len());
    for frame in repair_frames {
        if !path_stream.has_repair_output_for_frame(&frame) {
            if allow_same_output_frontier_retransmit && accepted.is_empty() {
                accepted.push(frame);
                return (accepted, None);
            }
            return (
                accepted,
                reliable_stream_frame_extent(&frame).map(|(offset, _, _)| offset),
            );
        }
        accepted.push(frame);
    }
    (accepted, None)
}

fn enqueue_reliable_tail_repair(
    response_sender: &mut ServerResponseSenderService,
    path_stream: &ReliablePathStream,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(unused_variables))] stream_id: StreamId,
    send_stream: &ReliableSendStream,
    last_send_ack_ranges: &[OffsetRange],
    last_send_ack_complete: bool,
    send_path_snapshot: Option<PathSnapshot>,
    relay_lane: FlowLane,
    mux_limits: MuxLimits,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(unused_variables))]
    performance: MppPerformanceConfig,
    max_frame_payload_bytes: usize,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(unused_variables))]
    last_send_ack_frontier: u64,
    allow_same_output_frontier_retransmit: bool,
) -> usize {
    let base_repair_limit = adaptive_reliable_relay_chunk_bytes_with_frame_limit(
        send_path_snapshot,
        FlowLane::Throughput,
        mux_limits,
        max_frame_payload_bytes,
    )
    .max(adaptive_reliable_relay_repair_bytes(
        send_path_snapshot,
        relay_lane,
        mux_limits,
    ));
    let budget_remaining = response_sender.repair_extra_event_budget_remaining(mux_limits);
    let repair_limit = reliable_tail_stall_repair_limit_bytes(
        base_repair_limit,
        send_stream.repair_bytes(),
        budget_remaining,
        mux_limits,
    );
    let critical_tail_repair = false;
    let (repair_frames, repair_kind) = stream_tail_stall_repair_frames(
        send_stream,
        last_send_ack_ranges,
        repair_limit,
        last_send_ack_complete,
    );
    let (repair_frames, blocked_frontier_offset) = prefix_repair_frames_with_available_output(
        path_stream,
        repair_frames,
        allow_same_output_frontier_retransmit,
    );
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = repair_kind;
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = blocked_frontier_offset;
    #[cfg(feature = "lab-diagnostics")]
    lab_diagnostic(
        "tail_stall_repair",
        format_args!(
            "stream_id={} lane={:?} ack_frontier={} sent_offset={} repair_bytes={} repair_frames={} blocked_frontier_offset={:?} same_output_frontier_retransmit={} base_repair_limit={} repair_limit={} extra_traffic_hint_percent={} repair_kind={}",
            stream_id.0,
            relay_lane,
            last_send_ack_frontier,
            send_stream.next_offset(),
            send_stream.repair_bytes(),
            repair_frames.len(),
            blocked_frontier_offset,
            allow_same_output_frontier_retransmit,
            base_repair_limit,
            repair_limit,
            performance.extra_traffic_hint_percent,
            repair_kind,
        ),
    );
    let mut repair_count = 0usize;
    for frame in repair_frames {
        let queued = if critical_tail_repair {
            Some(response_sender.enqueue_critical_repair_frame(frame))
        } else {
            response_sender.enqueue_repair_frame_with_priority(frame, mux_limits, true)
        };
        if queued.is_some() {
            repair_count = repair_count.saturating_add(1);
        }
    }
    repair_count
}

#[allow(clippy::too_many_arguments)]
async fn drain_server_response_sender_ready(
    response_sender: &mut ServerResponseSenderService,
    path_stream: &ReliablePathStream,
    product_owner_debt_bytes: usize,
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
            .dispatch_next_with_product_owner_debt(
                path_stream,
                send_stream,
                relay_lane,
                mux_limits,
                product_owner_debt_bytes,
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
        let recv_progress_deadline = tokio::time::Instant::from_std(
            last_recv_progress_sent_at
                + reliable_stream_recv_progress_interval(send_path_snapshot, relay_lane),
        );
        let has_tail_repair_alternative = path_stream.has_multipath_repair_alternative();
        let tail_repair_active = relay_lane_is_bulk(relay_lane)
            && stream_tail_repair_allowed(has_tail_repair_alternative)
            && send_stream.repair_bytes() > 0
            && !last_send_ack_ranges.is_empty()
            && last_send_ack_complete
            && last_send_ack_frontier < send_stream.next_offset();
        let authoritative_owner_debt_bytes = reliable_relay_current_authoritative_owner_debt_bytes(
            relay_lane,
            &send_stream,
            last_send_ack_complete,
            &last_send_ack_ranges,
            last_send_ack_frontier,
            last_send_ack_progress_at,
            send_path_snapshot,
        );
        let tail_repair_deadline = reliable_relay_tail_repair_deadline(
            last_send_ack_progress_at.max(last_tail_repair_at),
            send_path_snapshot,
            relay_lane,
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
        let source_read_ceiling = reliable_relay_buffer_len(mux_limits)
            .min(path_stream.max_frame_payload_bytes)
            .min(sender_queue_limit)
            .min(latency_owner_credit);
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
        if response_sender_retry_at.is_some_and(|deadline| deadline <= now) {
            response_sender_retry_at = None;
        }
        let queued_front_has_carrier_credit = response_sender
            .front_has_carrier_credit_with_product_owner_debt(
                &path_stream,
                &send_stream,
                relay_lane,
                mux_limits,
                authoritative_owner_debt_bytes,
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
                enqueue_reliable_tail_repair(
                    &mut response_sender,
                    &path_stream,
                    stream_id,
                    &send_stream,
                    &last_send_ack_ranges,
                    last_send_ack_complete,
                    send_path_snapshot,
                    relay_lane,
                    mux_limits,
                    performance,
                    path_stream.max_frame_payload_bytes,
                    last_send_ack_frontier,
                    false,
                );
                last_tail_repair_at = Instant::now();
                let product_owner_debt_bytes = reliable_relay_current_authoritative_owner_debt_bytes(
                    relay_lane,
                    &send_stream,
                    last_send_ack_complete,
                    &last_send_ack_ranges,
                    last_send_ack_frontier,
                    last_send_ack_progress_at,
                    send_path_snapshot,
                );
                if drain_server_response_sender_ready(
                    &mut response_sender,
                    &path_stream,
                    product_owner_debt_bytes,
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
                        let largest_ack_end = normalized_ranges.last().map_or(0, |range| range.end);
                        let ack_made_progress =
                            ack.released_bytes > 0 || largest_ack_end > last_send_ack_frontier;
                        if ack_made_progress {
                            last_send_ack_progress_at = Instant::now();
                            last_tail_repair_at = last_send_ack_progress_at;
                        }
                        last_send_ack_frontier = last_send_ack_frontier.max(largest_ack_end);
                        last_send_ack_ranges = normalized_ranges.clone();
                        last_send_ack_complete = complete;
                        let base_repair_limit =
                            adaptive_reliable_relay_repair_bytes(None, relay_lane, mux_limits);
                        let repair_event_budget =
                            response_sender.repair_extra_event_budget_remaining(mux_limits);
                        let repair_limit = base_repair_limit.min(repair_event_budget);
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
                        let mut repair_frames = stream_ack_gap_repair_frames_normalized(
                            &send_stream,
                            &normalized_ranges,
                            repair_limit,
                            complete,
                            has_multipath_repair_alternative,
                            ack_gap_repair_ready,
                        );
                        let mut critical_tail_repair = false;
                        let repair_kind = if repair_frames.is_empty() {
                            let fin_tail_ready = close_sent || pending_local_fin;
                            let fin_tail_limit = if fin_tail_ready {
                                let limit = reliable_critical_tail_repair_limit_bytes(
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
                            let (fin_tail_frames, blocked_frontier_offset) =
                                prefix_repair_frames_with_available_output(
                                    &path_stream,
                                    stream_final_offset_tail_repair_frames(
                                        &send_stream,
                                        &ranges,
                                        fin_tail_limit,
                                        fin_tail_ready,
                                        has_multipath_repair_alternative,
                                    ),
                                    false,
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
                            let queued = if critical_tail_repair && repair_kind == "fin_tail" {
                                response_sender.enqueue_critical_repair_frame(frame);
                                true
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
                    let product_owner_debt_bytes = reliable_relay_current_authoritative_owner_debt_bytes(
                        relay_lane,
                        &send_stream,
                        last_send_ack_complete,
                        &last_send_ack_ranges,
                        last_send_ack_frontier,
                        last_send_ack_progress_at,
                        send_path_snapshot,
                    );
                    if drain_server_response_sender_ready(
                        &mut response_sender,
                        &path_stream,
                    product_owner_debt_bytes,
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
                let final_tail_repair_ready = (close_sent || pending_local_fin)
                    && send_stream.repair_bytes() > 0
                    && !last_send_ack_ranges.is_empty()
                    && last_send_ack_frontier < send_stream.next_offset();
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
                    let repair_count = enqueue_reliable_tail_repair(
                        &mut response_sender,
                        &path_stream,
                        stream_id,
                        &send_stream,
                        &last_send_ack_ranges,
                        last_send_ack_complete,
                        send_path_snapshot,
                        relay_lane,
                        mux_limits,
                        performance,
                        path_stream.max_frame_payload_bytes,
                        last_send_ack_frontier,
                        false,
                    );
                    if repair_count > 0 {
                        last_tail_repair_at = Instant::now();
                    }
                }
                if response_sender.queued_send_ready() {
                    let product_owner_debt_bytes = reliable_relay_current_authoritative_owner_debt_bytes(
                        relay_lane,
                        &send_stream,
                        last_send_ack_complete,
                        &last_send_ack_ranges,
                        last_send_ack_frontier,
                        last_send_ack_progress_at,
                        send_path_snapshot,
                    );
                    if drain_server_response_sender_ready(
                        &mut response_sender,
                        &path_stream,
                    product_owner_debt_bytes,
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
                    let product_owner_debt_bytes = reliable_relay_current_authoritative_owner_debt_bytes(
                        relay_lane,
                        &send_stream,
                        last_send_ack_complete,
                        &last_send_ack_ranges,
                        last_send_ack_frontier,
                        last_send_ack_progress_at,
                        send_path_snapshot,
                    );
                    if drain_server_response_sender_ready(
                        &mut response_sender,
                        &path_stream,
                    product_owner_debt_bytes,
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
                let product_owner_debt_bytes = reliable_relay_current_authoritative_owner_debt_bytes(
                    relay_lane,
                    &send_stream,
                    last_send_ack_complete,
                    &last_send_ack_ranges,
                    last_send_ack_frontier,
                    last_send_ack_progress_at,
                    send_path_snapshot,
                );
                if drain_server_response_sender_ready(
                    &mut response_sender,
                    &path_stream,
                    product_owner_debt_bytes,
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
                let product_owner_debt_bytes = reliable_relay_current_authoritative_owner_debt_bytes(
                    relay_lane,
                    &send_stream,
                    last_send_ack_complete,
                    &last_send_ack_ranges,
                    last_send_ack_frontier,
                    last_send_ack_progress_at,
                    send_path_snapshot,
                );
                if drain_server_response_sender_ready(
                    &mut response_sender,
                    &path_stream,
                    product_owner_debt_bytes,
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
                        let next_read_budget = response_sender.read_budget(
                            &send_stream,
                            mux_limits,
                            sender_queue_limit,
                            buf.len(),
                        );
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
                        let product_owner_debt_bytes = reliable_relay_current_authoritative_owner_debt_bytes(
                            relay_lane,
                            &send_stream,
                            last_send_ack_complete,
                            &last_send_ack_ranges,
                            last_send_ack_frontier,
                            last_send_ack_progress_at,
                            send_path_snapshot,
                        );
                        if drain_server_response_sender_ready(
                            &mut response_sender,
                            &path_stream,
                    product_owner_debt_bytes,
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
            let product_owner_debt_bytes = reliable_relay_current_authoritative_owner_debt_bytes(
                last_relay_lane,
                &send_stream,
                last_send_ack_complete,
                &last_send_ack_ranges,
                last_send_ack_frontier,
                last_send_ack_progress_at,
                None,
            );
            match drain_server_response_sender_ready(
                &mut response_sender,
                &path_stream,
                product_owner_debt_bytes,
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
    fn ack_gap_repair_requires_multipath_alternative_and_persistent_gap() {
        assert!(!stream_ack_gap_repair_allowed(true, false, true));
        assert!(!stream_ack_gap_repair_allowed(true, true, false));
        assert!(stream_ack_gap_repair_allowed(true, true, true));
        assert!(!stream_ack_gap_repair_allowed(false, true, true));
    }

    #[test]
    fn tail_repair_requires_independent_repair_subflow() {
        assert!(!stream_tail_repair_allowed(false));
        assert!(stream_tail_repair_allowed(true));
    }

    #[test]
    fn tail_repair_timer_requires_repair_authoritative_ack_gap_shape() {
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
    fn authoritative_ack_gap_creates_owner_debt_pressure() {
        let now = Instant::now();
        let ranges = [
            OffsetRange {
                start: 0,
                end: 1024,
            },
            OffsetRange {
                start: 2048,
                end: 4096,
            },
        ];

        assert_eq!(
            reliable_relay_authoritative_owner_debt_bytes(
                FlowLane::Throughput,
                4096,
                true,
                &ranges,
                1024,
                8192,
                now,
                now,
                None,
            ),
            4096
        );
        assert_eq!(
            reliable_relay_authoritative_owner_debt_bytes(
                FlowLane::Latency,
                4096,
                true,
                &ranges,
                1024,
                8192,
                now,
                now,
                None,
            ),
            0
        );
        assert_eq!(
            reliable_relay_authoritative_owner_debt_bytes(
                FlowLane::Throughput,
                0,
                true,
                &ranges,
                1024,
                8192,
                now,
                now,
                None,
            ),
            0
        );
        assert_eq!(
            reliable_relay_authoritative_owner_debt_bytes(
                FlowLane::Throughput,
                4096,
                true,
                &[OffsetRange {
                    start: 0,
                    end: 8192,
                }],
                8192,
                8192,
                now,
                now,
                None,
            ),
            0
        );
    }

    #[test]
    fn stalled_contiguous_ack_frontier_creates_owner_debt_pressure() {
        let stall_timeout = reliable_relay_stall_timeout(None, FlowLane::Throughput);
        let now = Instant::now();
        let old_progress = now - stall_timeout - Duration::from_millis(1);
        let recent_progress = now - (stall_timeout / 2);
        let ranges = [OffsetRange {
            start: 0,
            end: 1024,
        }];

        assert_eq!(
            reliable_relay_authoritative_owner_debt_bytes(
                FlowLane::Throughput,
                4096,
                true,
                &ranges,
                1024,
                8192,
                old_progress,
                now,
                None,
            ),
            4096,
            "stalled contiguous ACK frontier is authoritative owner-debt pressure"
        );
        assert_eq!(
            reliable_relay_authoritative_owner_debt_bytes(
                FlowLane::Throughput,
                4096,
                true,
                &ranges,
                1024,
                8192,
                recent_progress,
                now,
                None,
            ),
            0,
            "recent contiguous ACK progress remains ordinary in-flight repair-cache retention"
        );
        assert_eq!(
            reliable_relay_authoritative_owner_debt_bytes(
                FlowLane::Latency,
                4096,
                true,
                &ranges,
                1024,
                8192,
                old_progress,
                now,
                None,
            ),
            0,
            "latency traffic should not enter bulk owner-debt pressure"
        );
    }
    #[test]
    fn tail_repair_uses_persistent_congestion_timeout() {
        let last_progress = Instant::now();
        let deadline =
            reliable_relay_tail_repair_deadline(last_progress, None, FlowLane::Throughput);
        let expected = tokio::time::Instant::from_std(
            last_progress
                + reliable_relay_stall_timeout(None, FlowLane::Throughput)
                    .saturating_mul(QUIC_PERSISTENT_CONGESTION_THRESHOLD),
        );

        assert_eq!(deadline, expected);
    }

    #[test]
    fn tail_stall_without_authoritative_ack_gap_does_not_repair_live_tail() {
        let limits = MuxLimits::default();
        let mut send_stream = ReliableSendStream::new(StreamId(9), limits);
        send_stream
            .send_data(Bytes::from_static(&[7; 4096]), StreamFlags::NONE)
            .expect("send stream data");

        let (repair_frames, repair_kind) = stream_tail_stall_repair_frames(
            &send_stream,
            &[OffsetRange {
                start: 0,
                end: 1024,
            }],
            4096,
            false,
        );

        assert!(repair_frames.is_empty());
        assert_eq!(repair_kind, "none");
    }

    #[test]
    fn tail_stall_repairs_unacked_owner_tail_after_contiguous_frontier() {
        let limits = MuxLimits::default();
        let mut send_stream = ReliableSendStream::new(StreamId(9), limits);
        send_stream
            .send_data(Bytes::from_static(&[7; 4096]), StreamFlags::NONE)
            .expect("send stream data");

        let (repair_frames, repair_kind) = stream_tail_stall_repair_frames(
            &send_stream,
            &[OffsetRange {
                start: 0,
                end: 1024,
            }],
            2048,
            true,
        );

        assert_eq!(
            repair_kind, "owner_tail",
            "persistent failover repair must cover unacked owner bytes after a contiguous ACK frontier"
        );
        assert_eq!(repair_frames.len(), 1);
        assert_eq!(
            reliable_stream_frame_extent(&repair_frames[0]),
            Some((1024, 3072, 2048))
        );
    }

    #[test]
    fn tail_stall_repair_limit_spends_earned_budget_for_live_owner_tail() {
        let limits = MuxLimits::default();
        let base_limit = BBR_MAX_SEND_QUANTUM_BYTES.min(reliable_relay_buffer_len(limits));
        let repair_debt = base_limit.saturating_mul(32);
        let budget_remaining = repair_debt;

        let repair_limit = reliable_tail_stall_repair_limit_bytes(
            base_limit,
            repair_debt,
            budget_remaining,
            limits,
        );

        assert!(
            repair_limit > base_limit,
            "live owner-tail repair may spend earned optional repair budget after a persistent stall"
        );
        assert_eq!(
            repair_limit, repair_debt,
            "the optional repair budget, not a single timer quantum, caps live owner-tail repair"
        );
    }

    #[test]
    fn tail_stall_repair_limit_stops_after_optional_budget_exhaustion() {
        let limits = MuxLimits::default();
        let base_limit = BBR_MAX_SEND_QUANTUM_BYTES.min(reliable_relay_buffer_len(limits));
        let small_tail = base_limit.saturating_sub(1024).max(1);

        let repair_limit =
            reliable_tail_stall_repair_limit_bytes(base_limit, small_tail, 0, limits);

        assert_eq!(
            repair_limit, 0,
            "live owner-tail repair is optional traffic and must stop after the optional budget is exhausted"
        );
        assert_eq!(
            reliable_tail_stall_repair_limit_bytes(
                base_limit,
                limits.max_repair_bytes.saturating_add(base_limit),
                0,
                limits
            ),
            0,
            "live owner-tail repair must not mint critical budget for larger retained tails"
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
            reliable_critical_tail_repair_limit_bytes(small_tail, limits),
            small_tail,
            "terminal owner-tail repair may close a retained final tail even after optional repair budget is exhausted"
        );

        assert!(
            reliable_critical_tail_repair_limit_bytes(repair_debt, limits) >= base_limit,
            "terminal owner-tail repair keeps a bounded critical path for final stream closure"
        );
        assert_eq!(
            reliable_critical_tail_repair_limit_bytes(
                resource_cap.saturating_add(base_limit),
                limits
            ),
            resource_cap,
            "critical final-tail repair remains bounded by configured repair resources"
        );
    }

    #[test]
    fn live_tail_stall_repair_does_not_bypass_optional_budget() {
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
        assert!(
            response_sender
                .enqueue_repair_frame_with_priority(
                    Frame::StreamData {
                        stream_id,
                        offset: 0,
                        flags: StreamFlags::NONE,
                        payload: Bytes::from(vec![0x98; initial_budget]),
                    },
                    limits,
                    false,
                )
                .is_some()
        );
        assert_eq!(
            response_sender.repair_extra_event_budget_remaining(limits),
            0
        );

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

        let queued = enqueue_reliable_tail_repair(
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
            false,
        );

        assert_eq!(
            queued, 0,
            "live contiguous owner-tail repair is optional traffic and must not bypass the extra-traffic budget"
        );
    }

    #[test]
    fn tail_stall_repair_still_repairs_authoritative_ack_gap() {
        let limits = MuxLimits::default();
        let mut send_stream = ReliableSendStream::new(StreamId(9), limits);
        send_stream
            .send_data(Bytes::from_static(&[7; 4096]), StreamFlags::NONE)
            .expect("send stream data");

        let (repair_frames, repair_kind) = stream_tail_stall_repair_frames(
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
        );

        assert_eq!(repair_kind, "ack_gap");
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
