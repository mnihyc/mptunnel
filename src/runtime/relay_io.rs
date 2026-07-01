use super::*;

const TCP_RELAY_MSS_BYTES: usize = 1460;
const TCP_RELAY_MAX_BULK_QUANTUM_BYTES: usize = 64 * 1024;
const TCP_RELAY_MAX_BACKGROUND_QUANTUM_BYTES: usize = 32 * 1024;
const TCP_RELAY_MAX_LATENCY_QUANTUM_BYTES: usize = 16 * 1024;
const TCP_RELAY_MAX_CONTROL_QUANTUM_BYTES: usize = 4 * 1024;

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
            | RuntimeError::TcpPathSessionClosed
            | RuntimeError::Tcp(_)
            | RuntimeError::Encrypted(_)
            | RuntimeError::RemoteClosed(_)
            | RuntimeError::Protocol(_)
    )
}

pub(super) fn stream_ack_gap_repair_allowed(
    complete: bool,
    active_underlay: Option<UnderlayProtocol>,
    has_multipath_repair_alternative: bool,
    udp_gap_repair_ready: bool,
) -> bool {
    if !complete {
        return false;
    }
    if active_underlay != Some(UnderlayProtocol::Udp) {
        return true;
    }
    has_multipath_repair_alternative && udp_gap_repair_ready
}

pub(super) fn stream_ack_gap_repair_frames(
    send_stream: &ReliableSendStream,
    ranges: &[OffsetRange],
    byte_limit: usize,
    complete: bool,
    active_underlay: Option<UnderlayProtocol>,
    has_multipath_repair_alternative: bool,
    udp_gap_repair_ready: bool,
) -> Vec<Frame> {
    if stream_ack_gap_repair_allowed(
        complete,
        active_underlay,
        has_multipath_repair_alternative,
        udp_gap_repair_ready,
    ) {
        send_stream.retransmission_frames_for_ack_gaps(ranges, byte_limit)
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
    }
    (
        send_stream.retransmission_frames_after_ack_frontier(ranges, byte_limit),
        "ack_frontier",
    )
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
        ranges: &[OffsetRange],
        active_underlay: Option<UnderlayProtocol>,
        has_multipath_repair_alternative: bool,
        path: Option<PathSnapshot>,
        lane: FlowLane,
    ) -> bool {
        self.repair_ready_at(
            complete,
            ranges,
            active_underlay,
            has_multipath_repair_alternative,
            reliable_stream_recv_progress_interval(path, lane),
            Instant::now(),
        )
    }

    fn repair_ready_at(
        &mut self,
        complete: bool,
        ranges: &[OffsetRange],
        active_underlay: Option<UnderlayProtocol>,
        has_multipath_repair_alternative: bool,
        progress_interval: Duration,
        now: Instant,
    ) -> bool {
        if !complete {
            self.clear();
            return false;
        }
        if active_underlay != Some(UnderlayProtocol::Udp) {
            self.clear();
            return stream_ack_first_gap(ranges).is_some();
        }
        if !has_multipath_repair_alternative {
            self.clear();
            return false;
        }
        let Some(first_gap) = stream_ack_first_gap(ranges) else {
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

fn stream_ack_first_gap(ranges: &[OffsetRange]) -> Option<(u64, u64)> {
    if ranges.is_empty() {
        return None;
    }
    let mut ranges = ranges.to_vec();
    ranges.sort_by(|left, right| {
        left.start
            .cmp(&right.start)
            .then_with(|| left.end.cmp(&right.end))
    });
    let mut cursor = 0_u64;
    for range in ranges {
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

#[derive(Debug, Default)]
pub(super) struct ReliableRecvProgress {
    last_max_data_offset: u64,
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
        let ack_ranges = recv_stream.ack_ranges();
        let range_count = ack_ranges.len();
        let largest_end = ack_ranges.last().map_or(0, |range| range.end);
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
        mux_limits: MuxLimits,
        force: bool,
    ) -> bool {
        let max_offset = recv_stream.max_data_offset();
        if force
            || self.last_max_data_offset == 0
            || max_offset.saturating_sub(self.last_max_data_offset)
                >= reliable_stream_max_data_update_bytes(mux_limits)
        {
            self.last_max_data_offset = max_offset;
            true
        } else {
            false
        }
    }
}

pub(super) fn reliable_stream_max_data_update_bytes(mux_limits: MuxLimits) -> u64 {
    let window_step = mux_limits.max_stream_window_bytes.saturating_div(4).max(1);
    let payload_step = reliable_relay_buffer_len(mux_limits) as u64;
    window_step
        .max(payload_step)
        .min(mux_limits.max_stream_window_bytes)
}

pub(super) fn reliable_stream_ack_update_bytes(
    path: Option<PathSnapshot>,
    lane: FlowLane,
    mux_limits: MuxLimits,
) -> u64 {
    if !relay_lane_is_bulk(lane) {
        return 1;
    }
    let ack_capacity = mux_limits.max_ack_ranges.max(1) as u64;
    let window_step = mux_limits.max_stream_window_bytes / ack_capacity;
    let repair_step = mux_limits.max_repair_bytes as u64 / ack_capacity;
    let configured_step = window_step.min(repair_step).max(1);
    let measured_step = path
        .map(|path| {
            ((path.delivery_rate_bps.max(1.0) / 8.0) * (path.srtt_ms.max(1.0) / 1000.0))
                .ceil()
                .max(1.0) as u64
                / ack_capacity
        })
        .unwrap_or(configured_step)
        .max(1);
    let blended = ((configured_step as f64) * (measured_step as f64))
        .sqrt()
        .ceil() as u64;
    blended.max(PATH_OPEN_SCORE_BYTES as u64)
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
        let ack_frame = recv_stream.ack_frame();
        #[cfg(feature = "lab-diagnostics")]
        lab_perf_record("mux.ack_frames", ack_started.elapsed(), 1);
        response_sender.enqueue_control_frame(ack_frame);
        sent_any = true;
    }
    if progress.should_send_max_data(recv_stream, mux_limits, force_max_data) {
        response_sender.enqueue_control_frame(recv_stream.max_data_frame());
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
        .max(UDP_MIN_RESPONSE_TIMEOUT)
        .min(TCP_STREAM_STALL_MIN_TIMEOUT)
}

pub(super) fn reliable_relay_buffer_len(mux_limits: MuxLimits) -> usize {
    mux_limits
        .max_reliable_relay_chunk_bytes
        .min(mux_limits.max_payload_bytes)
        .min(mux_limits.max_tcp_path_inflight_bytes)
        .max(1)
}

pub(super) fn resize_reliable_relay_buffer(buffer: &mut Vec<u8>, target_len: usize) {
    let target_len = target_len.max(1);
    if buffer.len() == target_len {
        return;
    }
    if target_len > buffer.len() {
        buffer.resize(target_len, 0);
        return;
    }
    buffer.truncate(target_len);
    let shrink_threshold = target_len.saturating_mul(4).max(64 * 1024);
    if buffer.capacity() > shrink_threshold {
        buffer.shrink_to(target_len);
    }
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
    let floor = tcp_adaptive_lane_min_chunk_bytes(path, lane, mux_limits)
        .min(cap)
        .max(1);
    let Some(path) = path else {
        return tcp_lane_startup_chunk_bytes(lane, mux_limits)
            .min(cap)
            .max(floor);
    };

    let bdp_bytes = tcp_path_bdp_bytes(path);
    let lane_gain = tcp_lane_chunk_gain(lane);
    let stability = tcp_path_stability_factor(path);
    let queue_factor = tcp_path_queue_factor(path, bdp_bytes);
    let target = (bdp_bytes * lane_gain * stability * queue_factor).ceil() as usize;
    target.clamp(floor, cap)
}

pub(super) fn adaptive_relay_chunk_bytes_for_underlay(
    path: Option<PathSnapshot>,
    lane: FlowLane,
    mux_limits: MuxLimits,
    underlay: UnderlayProtocol,
    max_frame_payload_bytes: usize,
) -> usize {
    let chunk = adaptive_reliable_relay_chunk_bytes(path, lane, mux_limits)
        .min(max_frame_payload_bytes)
        .max(1);
    if underlay == UnderlayProtocol::Udp && !relay_lane_is_bulk(lane) {
        return chunk
            .min(udp_carrier::safe_stream_payload_bytes(mux_limits))
            .max(1);
    }
    chunk
}

pub(super) fn adaptive_reliable_relay_inflight_bytes(
    path: Option<PathSnapshot>,
    lane: FlowLane,
    mux_limits: MuxLimits,
) -> usize {
    let cap = mux_limits.max_tcp_path_inflight_bytes.max(1);
    let floor = tcp_lane_min_inflight_bytes(lane, mux_limits)
        .min(cap)
        .max(1);
    let Some(path) = path else {
        return tcp_lane_startup_inflight_bytes(lane, mux_limits)
            .min(cap)
            .max(floor);
    };

    let bdp_bytes = tcp_path_bdp_bytes(path);
    let target = bdp_bytes
        * tcp_lane_inflight_gain(lane)
        * tcp_path_stability_factor(path)
        * tcp_path_queue_factor(path, bdp_bytes);
    (target.ceil() as usize).clamp(floor, cap)
}

pub(super) fn adaptive_reliable_relay_repair_bytes(
    path: Option<PathSnapshot>,
    lane: FlowLane,
    mux_limits: MuxLimits,
) -> usize {
    let read_ceiling = reliable_relay_buffer_len(mux_limits).max(1);
    let mss_floor = TCP_RELAY_MSS_BYTES.min(read_ceiling).max(1);
    let cap = match lane {
        FlowLane::Control | FlowLane::RealtimeDatagram | FlowLane::Latency => {
            TCP_RELAY_MAX_CONTROL_QUANTUM_BYTES
        }
        FlowLane::Throughput => TCP_RELAY_MAX_LATENCY_QUANTUM_BYTES,
        FlowLane::Background => TCP_RELAY_MAX_CONTROL_QUANTUM_BYTES,
    }
    .min(read_ceiling)
    .max(mss_floor);
    let Some(path) = path else {
        return cap;
    };
    let bdp_bytes = tcp_path_bdp_bytes(path);
    let condition =
        (tcp_path_stability_factor(path) * tcp_path_queue_factor(path, bdp_bytes)).clamp(0.25, 1.0);
    ((cap as f64) * condition).ceil() as usize
}

pub(super) fn tcp_path_bdp_bytes(path: PathSnapshot) -> f64 {
    (path.delivery_rate_bps.max(1.0) / 8.0) * (path.srtt_ms.max(1.0) / 1000.0)
}

pub(super) fn tcp_lane_chunk_gain(lane: FlowLane) -> f64 {
    match lane {
        FlowLane::Control | FlowLane::RealtimeDatagram => 1.0 / 256.0,
        FlowLane::Latency => 1.0 / 128.0,
        FlowLane::Throughput => 1.0 / 4.0,
        FlowLane::Background => 1.0 / 16.0,
    }
}

pub(super) fn tcp_lane_min_chunk_bytes(lane: FlowLane, mux_limits: MuxLimits) -> usize {
    let cap = reliable_relay_buffer_len(mux_limits).max(1);
    let mss_floor = (TCP_RELAY_MSS_BYTES * 2).min(cap).max(1);
    match lane {
        FlowLane::Control | FlowLane::RealtimeDatagram => PATH_OPEN_SCORE_BYTES.min(cap).max(1),
        FlowLane::Latency => PATH_OPEN_SCORE_BYTES.min(cap).max(1),
        FlowLane::Throughput => mss_floor.max(PATH_OPEN_SCORE_BYTES.min(cap)),
        FlowLane::Background => mss_floor.max(PATH_OPEN_SCORE_BYTES.min(cap)),
    }
}

pub(super) fn tcp_adaptive_lane_min_chunk_bytes(
    path: Option<PathSnapshot>,
    lane: FlowLane,
    mux_limits: MuxLimits,
) -> usize {
    let floor = tcp_lane_min_chunk_bytes(lane, mux_limits);
    if lane != FlowLane::Throughput {
        return floor;
    }
    tcp_throughput_amortized_chunk_floor(path, mux_limits).max(floor)
}

pub(super) fn tcp_throughput_amortized_chunk_floor(
    path: Option<PathSnapshot>,
    mux_limits: MuxLimits,
) -> usize {
    let read_ceiling = reliable_relay_buffer_len(mux_limits).max(1);
    let protocol_floor = (TCP_RELAY_MSS_BYTES * 8).min(read_ceiling).max(1);
    let cpu_floor = read_ceiling
        .saturating_div(8)
        .max(protocol_floor)
        .min(TCP_RELAY_MAX_BULK_QUANTUM_BYTES)
        .min(read_ceiling)
        .max(1);
    let Some(path) = path else {
        return cpu_floor;
    };

    let bdp = tcp_path_bdp_bytes(path);
    let bdp_floor = ((bdp * 0.5).ceil() as usize)
        .max(protocol_floor)
        .min(read_ceiling)
        .max(1);
    let stable_floor = cpu_floor
        .min(bdp_floor)
        .max(protocol_floor)
        .min(read_ceiling);
    let condition_factor = tcp_path_quantum_condition_factor(path, bdp).clamp(0.25, 1.0);
    ((stable_floor as f64) * condition_factor).ceil() as usize
}

pub(super) fn tcp_lane_startup_chunk_bytes(lane: FlowLane, mux_limits: MuxLimits) -> usize {
    let cap = reliable_relay_scheduler_quantum_cap(None, lane, mux_limits);
    let floor = tcp_lane_min_chunk_bytes(lane, mux_limits);
    match lane {
        FlowLane::Control | FlowLane::RealtimeDatagram => floor,
        FlowLane::Latency => TCP_RELAY_MAX_LATENCY_QUANTUM_BYTES.min(cap).max(floor),
        FlowLane::Throughput => TCP_RELAY_MAX_BULK_QUANTUM_BYTES.min(cap).max(floor),
        FlowLane::Background => TCP_RELAY_MAX_BACKGROUND_QUANTUM_BYTES.min(cap).max(floor),
    }
}

pub(super) fn reliable_relay_scheduler_quantum_cap(
    path: Option<PathSnapshot>,
    lane: FlowLane,
    mux_limits: MuxLimits,
) -> usize {
    let read_ceiling = reliable_relay_buffer_len(mux_limits).max(1);
    let lane_ceiling = match lane {
        FlowLane::Control | FlowLane::RealtimeDatagram => TCP_RELAY_MAX_CONTROL_QUANTUM_BYTES,
        FlowLane::Latency => TCP_RELAY_MAX_LATENCY_QUANTUM_BYTES,
        FlowLane::Background => TCP_RELAY_MAX_BACKGROUND_QUANTUM_BYTES,
        FlowLane::Throughput => {
            let base = TCP_RELAY_MAX_BULK_QUANTUM_BYTES.min(read_ceiling).max(1);
            let Some(path) = path else {
                return base;
            };
            let bdp = tcp_path_bdp_bytes(path);
            let condition_factor = tcp_path_quantum_condition_factor(path, bdp);
            ((base as f64) * condition_factor)
                .ceil()
                .max((TCP_RELAY_MSS_BYTES * 8).min(base).max(1) as f64) as usize
        }
    };
    lane_ceiling.min(read_ceiling).max(1)
}

pub(super) fn tcp_lane_inflight_gain(lane: FlowLane) -> f64 {
    match lane {
        FlowLane::Control | FlowLane::RealtimeDatagram => 0.0625,
        FlowLane::Latency => 0.125,
        FlowLane::Throughput => 2.0,
        FlowLane::Background => 1.0,
    }
}

pub(super) fn tcp_lane_min_inflight_bytes(lane: FlowLane, mux_limits: MuxLimits) -> usize {
    let chunk = reliable_relay_buffer_len(mux_limits).max(1);
    match lane {
        FlowLane::Control | FlowLane::RealtimeDatagram => PATH_OPEN_SCORE_BYTES.min(chunk).max(1),
        FlowLane::Latency => chunk
            .saturating_div(4)
            .max(PATH_OPEN_SCORE_BYTES.min(chunk))
            .max(1),
        FlowLane::Throughput => chunk,
        FlowLane::Background => chunk
            .saturating_div(2)
            .max(PATH_OPEN_SCORE_BYTES.min(chunk))
            .max(1),
    }
}

pub(super) fn tcp_lane_startup_inflight_bytes(lane: FlowLane, mux_limits: MuxLimits) -> usize {
    let floor = tcp_lane_min_inflight_bytes(lane, mux_limits);
    let chunk = reliable_relay_buffer_len(mux_limits).max(1);
    match lane {
        FlowLane::Control | FlowLane::RealtimeDatagram => floor,
        FlowLane::Latency => chunk,
        FlowLane::Throughput => mux_limits.max_tcp_path_inflight_bytes.max(chunk),
        FlowLane::Background => mux_limits
            .max_tcp_path_inflight_bytes
            .saturating_div(2)
            .max(chunk),
    }
}

pub(super) fn tcp_path_stability_factor(path: PathSnapshot) -> f64 {
    let loss_factor = (1.0 - path.loss_rate.clamp(0.0, 1.0)).clamp(0.125, 1.0);
    let srtt = path.srtt_ms.max(1.0);
    let jitter_factor = (srtt / (srtt + path.jitter_ms.max(0.0))).clamp(0.125, 1.0);
    loss_factor * jitter_factor
}

pub(super) fn tcp_path_queue_factor(path: PathSnapshot, bdp_bytes: f64) -> f64 {
    let queued = path.queue_bytes.saturating_add(path.bytes_in_flight) as f64;
    (bdp_bytes / (bdp_bytes + queued.max(0.0))).clamp(0.125, 1.0)
}

pub(super) fn tcp_path_quantum_condition_factor(path: PathSnapshot, bdp_bytes: f64) -> f64 {
    let stability = tcp_path_stability_factor(path);
    let queue = tcp_path_queue_factor(path, bdp_bytes);
    (stability * queue.sqrt()).clamp(0.25, 1.0)
}

pub(super) fn tcp_sender_effective_relay_lane(local: FlowLane, peer: FlowLane) -> FlowLane {
    if local == FlowLane::Throughput || peer == FlowLane::Throughput {
        FlowLane::Throughput
    } else if local == FlowLane::Background || peer == FlowLane::Background {
        FlowLane::Background
    } else {
        peer
    }
}

fn repair_limit_with_extra_traffic_hint(
    base_limit: usize,
    performance: MppPerformanceConfig,
) -> usize {
    let hint = performance.extra_traffic_hint_percent as usize;
    base_limit.saturating_add(base_limit.saturating_mul(hint) / 100)
}

fn enqueue_reliable_tail_repair(
    response_sender: &mut ServerResponseSenderService,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(unused_variables))] stream_id: StreamId,
    send_stream: &ReliableSendStream,
    last_send_ack_ranges: &[OffsetRange],
    last_send_ack_complete: bool,
    send_path_snapshot: Option<PathSnapshot>,
    relay_lane: FlowLane,
    mux_limits: MuxLimits,
    performance: MppPerformanceConfig,
    underlay: UnderlayProtocol,
    max_frame_payload_bytes: usize,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(unused_variables))]
    last_send_ack_frontier: u64,
) -> usize {
    let base_repair_limit = adaptive_relay_chunk_bytes_for_underlay(
        send_path_snapshot,
        FlowLane::Throughput,
        mux_limits,
        underlay,
        max_frame_payload_bytes,
    )
    .max(adaptive_reliable_relay_repair_bytes(
        send_path_snapshot,
        relay_lane,
        mux_limits,
    ));
    let repair_limit = repair_limit_with_extra_traffic_hint(base_repair_limit, performance);
    let (repair_frames, repair_kind) = stream_tail_stall_repair_frames(
        send_stream,
        last_send_ack_ranges,
        repair_limit,
        last_send_ack_complete,
    );
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = repair_kind;
    #[cfg(feature = "lab-diagnostics")]
    lab_diagnostic(
        "tail_stall_repair",
        format_args!(
            "stream_id={} lane={:?} ack_frontier={} sent_offset={} repair_bytes={} repair_frames={} base_repair_limit={} repair_limit={} extra_traffic_hint_percent={} repair_kind={}",
            stream_id.0,
            relay_lane,
            last_send_ack_frontier,
            send_stream.next_offset(),
            send_stream.repair_bytes(),
            repair_frames.len(),
            base_repair_limit,
            repair_limit,
            performance.extra_traffic_hint_percent,
            repair_kind,
        ),
    );
    let repair_count = repair_frames.len();
    for frame in repair_frames {
        response_sender.enqueue_repair_frame(frame);
    }
    repair_count
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
    let mut send_stream = ReliableSendStream::new(stream_id, mux_limits);
    send_stream.update_max_offset(path_stream.max_offset);
    let mut recv_stream = ReliableRecvStream::new(stream_id, mux_limits);
    let chunk_size = adaptive_relay_chunk_bytes_for_underlay(
        None,
        FlowLane::Latency,
        mux_limits,
        path_stream.underlay,
        path_stream.max_frame_payload_bytes,
    );
    let mut buf = vec![0u8; chunk_size];
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
    let mut last_tail_repair_path = None;
    let mut last_send_ack_frontier = 0_u64;
    let mut last_send_ack_ranges = Vec::<OffsetRange>::new();
    let mut last_send_ack_complete = false;
    let mut flow_demand = ReliableRelayFlowDemandTracker::new();
    let mut output_updates = path_stream.subscribe_output_updates();
    let mut response_sender = ServerResponseSenderService::new(session_id, stream_id);
    let mut response_sender_retry_at: Option<tokio::time::Instant> = None;
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
        let relay_lane = tcp_sender_effective_relay_lane(relay_demand.lane, peer_lane);
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
            tcp_lane_startup_chunk_bytes(relay_lane, mux_limits)
                .min(path_stream.max_frame_payload_bytes),
        );
        let recv_progress_deadline = tokio::time::Instant::from_std(
            last_recv_progress_sent_at
                + reliable_stream_recv_progress_interval(send_path_snapshot, relay_lane),
        );
        let tail_repair_active = relay_lane_is_bulk(relay_lane)
            && remote_open
            && send_stream.repair_bytes() > 0
            && !last_send_ack_ranges.is_empty()
            && last_send_ack_frontier < send_stream.next_offset()
            && path_stream.has_multipath_repair_alternative();
        let tail_repair_deadline = reliable_relay_stall_deadline(
            last_send_ack_progress_at.max(last_tail_repair_at),
            send_path_snapshot,
            relay_lane,
        );
        let adaptive_chunk = adaptive_relay_chunk_bytes_for_underlay(
            send_path_snapshot,
            relay_lane,
            mux_limits,
            path_stream.underlay,
            path_stream.max_frame_payload_bytes,
        );
        resize_reliable_relay_buffer(&mut buf, adaptive_chunk);
        let inflight_limit =
            adaptive_reliable_relay_inflight_bytes(send_path_snapshot, relay_lane, mux_limits);
        let sender_queue_limit = reliable_relay_sender_queue_limit(mux_limits, inflight_limit);
        #[cfg(feature = "lab-diagnostics")]
        if last_reported_budget != Some((relay_lane, adaptive_chunk, inflight_limit)) {
            lab_diagnostic(
                "server_relay_budget",
                format_args!(
                    "stream_id={} underlay={:?} lane={:?} chunk_bytes={} inflight_bytes={} max_frame_payload_bytes={}",
                    stream_id.0,
                    path_stream.underlay,
                    relay_lane,
                    adaptive_chunk,
                    inflight_limit,
                    path_stream.max_frame_payload_bytes,
                ),
            );
            last_reported_budget = Some((relay_lane, adaptive_chunk, inflight_limit));
        }
        if response_sender_retry_at.is_some_and(|deadline| deadline <= tokio::time::Instant::now())
        {
            response_sender_retry_at = None;
        }
        let queued_send_blocked = !response_sender.is_empty() && response_sender_retry_at.is_some();
        let queued_send_ready = response_sender.queued_send_ready() && !queued_send_blocked;
        let queued_send_retry_deadline =
            response_sender_retry_at.unwrap_or_else(tokio::time::Instant::now);
        let can_read_by_flow = response_sender.can_read_product_source(
            local_open,
            queued_send_blocked,
            &send_stream,
            mux_limits,
            sender_queue_limit,
        );
        let read_budget = if can_read_by_flow {
            response_sender.read_budget(&send_stream, mux_limits, sender_queue_limit, buf.len())
        } else {
            0
        };
        let can_read_local = can_read_by_flow && read_budget > 0;
        let can_send_pending_fin = pending_local_fin
            && response_sender.is_empty()
            && !close_sent
            && (path_stream.underlay != UnderlayProtocol::Udp || send_stream.repair_bytes() == 0);

        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(tail_repair_deadline), if tail_repair_active => {
                enqueue_reliable_tail_repair(
                    &mut response_sender,
                    stream_id,
                    &send_stream,
                    &last_send_ack_ranges,
                    last_send_ack_complete,
                    send_path_snapshot,
                    relay_lane,
                    mux_limits,
                    performance,
                    path_stream.underlay,
                    path_stream.max_frame_payload_bytes,
                    last_send_ack_frontier,
                );
                last_tail_repair_at = Instant::now();
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
                        for chunk in outcome.delivered {
                            stats.record_payload_bytes(chunk.len());
                            #[cfg(feature = "lab-diagnostics")]
                            let write_started = Instant::now();
                            local.write_all(&chunk).await?;
                            #[cfg(feature = "lab-diagnostics")]
                            lab_perf_record("relay.local_write_wait", write_started.elapsed(), chunk.len());
                        }
                        #[cfg(feature = "lab-diagnostics")]
                        let flush_started = Instant::now();
                        local.flush().await?;
                        #[cfg(feature = "lab-diagnostics")]
                        lab_perf_record("relay.local_flush_wait", flush_started.elapsed(), 0);
                        if enqueue_tcp_recv_progress(
                            &mut response_sender,
                            &recv_stream,
                            &mut recv_progress,
                            None,
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
                        #[cfg(feature = "lab-diagnostics")]
                        let mux_started = Instant::now();
                        let ack = send_stream.apply_ack(&ranges);
                        #[cfg(feature = "lab-diagnostics")]
                        lab_perf_record("mux.apply_ack", mux_started.elapsed(), ack.released_bytes);
                        path_stream.release_acked_ranges(&ranges);
                        let largest_ack_end =
                            ranges.iter().map(|range| range.end).max().unwrap_or(0);
                        let ack_made_progress =
                            ack.released_bytes > 0 || largest_ack_end > last_send_ack_frontier;
                        if ack_made_progress {
                            last_send_ack_progress_at = Instant::now();
                            last_tail_repair_at = last_send_ack_progress_at;
                            if let Some(path_key) = last_tail_repair_path.take()
                                && path_stream.mark_repair_path_delivery_and_promote(path_key)
                            {
                                #[cfg(feature = "lab-diagnostics")]
                                lab_diagnostic(
                                    "repair_path_promoted",
                                    format_args!(
                                        "stream_id={} path_underlay={:?} path_id={} released_bytes={} largest_end={}",
                                        stream_id.0,
                                        path_key.underlay,
                                        path_key.path_id.0,
                                        ack.released_bytes,
                                        largest_ack_end,
                                    ),
                                );
                            }
                        }
                        last_send_ack_frontier = last_send_ack_frontier.max(largest_ack_end);
                        last_send_ack_ranges = ranges.clone();
                        last_send_ack_complete = complete;
                        let base_repair_limit =
                            adaptive_reliable_relay_repair_bytes(None, relay_lane, mux_limits);
                        let repair_limit =
                            repair_limit_with_extra_traffic_hint(base_repair_limit, performance);
                        let has_multipath_repair_alternative =
                            path_stream.has_multipath_repair_alternative();
                        let udp_gap_repair_ready = ack_gap_repair.repair_ready(
                            complete,
                            &ranges,
                            Some(path_stream.underlay),
                            has_multipath_repair_alternative,
                            None,
                            relay_lane,
                        );
                        let repair_frames = stream_ack_gap_repair_frames(
                            &send_stream,
                            &ranges,
                            repair_limit,
                            complete,
                            Some(path_stream.underlay),
                            has_multipath_repair_alternative,
                            udp_gap_repair_ready,
                        );
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "stream_ack_received",
                            format_args!(
                                "stream_id={} complete={} ranges={} largest_end={} released_bytes={} sent_offset={} sender_queue_bytes={} repair_bytes_after={} repair_frames={} active_underlay={:?} multipath_repair_alternative={} udp_gap_repair_ready={} base_repair_limit={} repair_limit={} extra_traffic_hint_percent={}",
                                stream_id.0,
                                complete,
                                ranges.len(),
                                largest_ack_end,
                                ack.released_bytes,
                                send_stream.next_offset(),
                                response_sender.bytes(),
                                ack.remaining_repair_bytes,
                                repair_frames.len(),
                                Some(path_stream.underlay),
                                has_multipath_repair_alternative,
                                udp_gap_repair_ready,
                                base_repair_limit,
                                repair_limit,
                                performance.extra_traffic_hint_percent,
                            ),
                        );
                        for frame in repair_frames {
                            response_sender.enqueue_repair_frame(frame);
                        }
                        #[cfg(not(feature = "lab-diagnostics"))]
                        let _ = ack;
                        if pending_local_fin && send_stream.repair_bytes() == 0 {
                            let frame = Frame::StreamFin {
                                stream_id,
                                final_offset: send_stream.next_offset(),
                            };
                            response_sender.enqueue_control_frame(frame);
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
                            local.shutdown().await?;
                            remote_open = false;
                            pending_remote_fin_offset = None;
                        }
                    }
                    Frame::StreamReset {
                        stream_id: reset_stream_id,
                        reason,
                    } if reset_stream_id == stream_id => return Err(RuntimeError::RemoteReset(reason)),
                    unexpected => {
                        log_unexpected_stream_relay_frame("single", stream_id, &unexpected);
                        return Err(RuntimeError::Protocol("unexpected stream relay frame"));
                    }
                }
            }
            changed = async {
                match output_updates.as_mut() {
                    Some(updates) => updates
                        .changed()
                        .await
                        .map_err(|_| RuntimeError::TcpPathSessionClosed),
                    None => std::future::pending::<Result<(), RuntimeError>>().await,
                }
            }, if queued_send_blocked => {
                changed?;
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
                    None,
                    relay_lane,
                    mux_limits,
                    true,
                ) {
                    response_sender_retry_at = None;
                    last_recv_progress_sent_at = Instant::now();
                }
            }
            _ = std::future::ready(()), if can_send_pending_fin => {
                let frame = Frame::StreamFin {
                    stream_id,
                    final_offset: send_stream.next_offset(),
                };
                response_sender.enqueue_control_frame(frame);
                response_sender_retry_at = None;
                close_sent = true;
                pending_local_fin = false;
            }
            _ = std::future::ready(()), if queued_send_ready => {
                let dispatch = match response_sender.dispatch_next(&path_stream, &mut send_stream, relay_lane).await {
                    Ok(dispatch) => dispatch,
                    Err(RuntimeError::SenderServiceBlocked) => {
                        response_sender_retry_at =
                            Some(tokio::time::Instant::now() + UDP_MIN_RESPONSE_TIMEOUT);
                        continue;
                    }
                    Err(err) => break Err(err),
                };
                if dispatch.lane == ReliableRelayQueuedWorkLane::Repair {
                    last_tail_repair_path = dispatch.selected_path;
                } else {
                    stats.record_payload_bytes(dispatch.payload_bytes);
                }
            }
            read = async {
                #[cfg(feature = "lab-diagnostics")]
                let read_started = Instant::now();
                let result = local.read(&mut buf[..read_budget]).await;
                #[cfg(feature = "lab-diagnostics")]
                if let Ok(read) = &result {
                    lab_perf_record("relay.local_read_wait", read_started.elapsed(), *read);
                }
                result
            }, if can_read_local => {
                let read = read?;
                if read == 0 {
                    pending_local_fin = true;
                    local_open = false;
                } else {
                    #[cfg(feature = "lab-diagnostics")]
                    let copy_started = Instant::now();
                    let payload = Bytes::copy_from_slice(&buf[..read]);
                    #[cfg(feature = "lab-diagnostics")]
                    lab_perf_record("relay.copy_local_chunk", copy_started.elapsed(), read);
                    #[cfg(feature = "lab-diagnostics")]
                    let enqueue_id = response_sender.enqueue_data(payload);
                    #[cfg(not(feature = "lab-diagnostics"))]
                    response_sender.enqueue_data(payload);
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
                }
            }
            else => break Ok(stats),
        }
    };

    let mut result = result;
    if result.is_ok() && pending_local_fin && !close_sent {
        let frame = Frame::StreamFin {
            stream_id,
            final_offset: send_stream.next_offset(),
        };
        response_sender.enqueue_control_frame(frame);
        match response_sender
            .dispatch_next(&path_stream, &mut send_stream, FlowLane::Control)
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
    if !close_sent {
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
    fn reliable_relay_sender_queue_budget_respects_stream_flow_control_credit() {
        let limits = MuxLimits {
            max_stream_window_bytes: 4,
            max_repair_bytes: 16,
            max_tcp_path_inflight_bytes: 16,
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
            tcp_sender_effective_relay_lane(FlowLane::Latency, FlowLane::Latency),
            FlowLane::Latency
        );
        assert_eq!(
            tcp_sender_effective_relay_lane(FlowLane::Throughput, FlowLane::Latency),
            FlowLane::Throughput
        );
        assert_eq!(
            tcp_sender_effective_relay_lane(FlowLane::Latency, FlowLane::Throughput),
            FlowLane::Throughput
        );
        assert_eq!(
            tcp_sender_effective_relay_lane(FlowLane::Latency, FlowLane::Background),
            FlowLane::Background
        );
    }

    #[test]
    fn udp_ack_gap_repair_requires_multipath_alternative() {
        assert!(!stream_ack_gap_repair_allowed(
            true,
            Some(UnderlayProtocol::Udp),
            false,
            true,
        ));
        assert!(!stream_ack_gap_repair_allowed(
            true,
            Some(UnderlayProtocol::Udp),
            true,
            false,
        ));
        assert!(stream_ack_gap_repair_allowed(
            true,
            Some(UnderlayProtocol::Udp),
            true,
            true,
        ));
        assert!(stream_ack_gap_repair_allowed(
            true,
            Some(UnderlayProtocol::Tcp),
            false,
            false,
        ));
        assert!(!stream_ack_gap_repair_allowed(
            false,
            Some(UnderlayProtocol::Udp),
            true,
            true,
        ));
    }

    #[test]
    fn udp_ack_gap_repair_progress_keeps_growing_hole_identity() {
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

        assert!(!progress.repair_ready_at(
            true,
            &first,
            Some(UnderlayProtocol::Udp),
            true,
            interval,
            now,
        ));
        assert!(
            progress.repair_ready_at(
                true,
                &grown,
                Some(UnderlayProtocol::Udp),
                true,
                interval,
                now + interval,
            ),
            "a growing ACK horizon with the same missing frontier is one persistent hole"
        );
        assert!(!progress.repair_ready_at(
            true,
            &grown,
            Some(UnderlayProtocol::Udp),
            true,
            interval,
            now + interval + Duration::from_millis(1),
        ));
    }

    #[test]
    fn udp_ack_gap_repair_progress_resets_when_frontier_advances() {
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
        let interval = reliable_stream_recv_progress_interval(None, FlowLane::Throughput);

        assert!(!progress.repair_ready_at(
            true,
            &first,
            Some(UnderlayProtocol::Udp),
            true,
            interval,
            now,
        ));
        assert!(!progress.repair_ready_at(
            true,
            &advanced,
            Some(UnderlayProtocol::Udp),
            true,
            interval,
            now + interval,
        ));
        assert!(progress.repair_ready_at(
            true,
            &advanced,
            Some(UnderlayProtocol::Udp),
            true,
            interval,
            now + interval + interval,
        ));
    }

    #[test]
    fn tail_repair_hint_scales_repair_budget_by_percent() {
        let base = TCP_RELAY_MAX_LATENCY_QUANTUM_BYTES;
        let low_hint = MppPerformanceConfig {
            extra_traffic_hint_percent: 1,
        };
        let high_hint = MppPerformanceConfig {
            extra_traffic_hint_percent: 100,
        };
        let severe_hint = MppPerformanceConfig {
            extra_traffic_hint_percent: 200,
        };

        let low_limit = repair_limit_with_extra_traffic_hint(base, low_hint);
        let high_limit = repair_limit_with_extra_traffic_hint(base, high_hint);
        let severe_limit = repair_limit_with_extra_traffic_hint(base, severe_hint);

        assert_eq!(low_limit, base + base / 100);
        assert_eq!(high_limit, base * 2);
        assert_eq!(severe_limit, base * 3);
    }
}
