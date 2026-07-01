use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RelaySendCause {
    StreamData,
    StreamFin,
    RecvProgress,
    AckGapRepair,
    PathFailureRepair,
}

impl RelaySendCause {
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    fn as_str(self) -> &'static str {
        match self {
            Self::StreamData => "stream_data",
            Self::StreamFin => "stream_fin",
            Self::RecvProgress => "recv_progress",
            Self::AckGapRepair => "ack_gap_repair",
            Self::PathFailureRepair => "path_failure_repair",
        }
    }

    fn is_repair(self) -> bool {
        matches!(self, Self::AckGapRepair | Self::PathFailureRepair)
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct RelaySendOutcome {
    pub(super) path_key: RelayPathKey,
}

#[derive(Debug)]
pub(super) struct RelaySenderService {
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    stream_id: StreamId,
    flights: RelayPathFlightLedger,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct RelayRecvProgressSend {
    path: Option<PathSnapshot>,
    lane: FlowLane,
    force_max_data: bool,
}

impl RelayRecvProgressSend {
    pub(super) fn new(path: Option<PathSnapshot>, lane: FlowLane, force_max_data: bool) -> Self {
        Self {
            path,
            lane,
            force_max_data,
        }
    }
}

#[derive(Debug)]
pub(super) struct TcpRelayQueuedFrame {
    pub(super) frame: Frame,
    pub(super) payload_bytes: usize,
    #[cfg(feature = "lab-diagnostics")]
    pub(super) enqueue_id: u64,
    #[cfg(feature = "lab-diagnostics")]
    pub(super) queued_at: Instant,
}

#[derive(Debug, Default)]
pub(super) struct TcpRelaySenderQueue {
    frames: VecDeque<TcpRelayQueuedFrame>,
    bytes: usize,
    #[cfg(feature = "lab-diagnostics")]
    next_enqueue_id: u64,
}

impl TcpRelaySenderQueue {
    pub(super) fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    pub(super) fn bytes(&self) -> usize {
        self.bytes
    }

    pub(super) fn push(&mut self, frame: Frame) -> u64 {
        let payload_bytes = reliable_stream_frame_payload_bytes(&frame);
        #[cfg(feature = "lab-diagnostics")]
        let enqueue_id = {
            let enqueue_id = self.next_enqueue_id;
            self.next_enqueue_id = self.next_enqueue_id.saturating_add(1);
            enqueue_id
        };
        #[cfg(not(feature = "lab-diagnostics"))]
        let enqueue_id = 0;
        self.bytes = self.bytes.saturating_add(payload_bytes);
        self.frames.push_back(TcpRelayQueuedFrame {
            frame,
            payload_bytes,
            #[cfg(feature = "lab-diagnostics")]
            enqueue_id,
            #[cfg(feature = "lab-diagnostics")]
            queued_at: Instant::now(),
        });
        enqueue_id
    }

    pub(super) fn pop_front(&mut self) -> Option<TcpRelayQueuedFrame> {
        let frame = self.frames.pop_front()?;
        self.bytes = self.bytes.saturating_sub(frame.payload_bytes);
        Some(frame)
    }

    pub(super) fn front_stream_extent(&self) -> Option<(u64, usize)> {
        let frame = self.frames.front()?;
        match &frame.frame {
            Frame::StreamData {
                offset, payload, ..
            } => Some((*offset, payload.len())),
            _ => None,
        }
    }
}

pub(super) fn tcp_relay_sender_queue_limit(mux_limits: MuxLimits, inflight_limit: usize) -> usize {
    let stream_window = usize::try_from(mux_limits.max_stream_window_bytes).unwrap_or(usize::MAX);
    inflight_limit
        .max(mux_limits.max_tcp_path_inflight_bytes)
        .min(mux_limits.max_repair_bytes)
        .min(stream_window)
        .max(1)
}

pub(super) fn tcp_relay_can_read_into_sender_queue(
    send_stream: &ReliableSendStream,
    sender_queue: &TcpRelaySenderQueue,
    mux_limits: MuxLimits,
    queue_limit: usize,
) -> bool {
    sender_queue.bytes() < queue_limit
        && send_stream.send_credit_bytes() > 0
        && send_stream.repair_bytes() < mux_limits.max_repair_bytes
}

pub(super) fn tcp_relay_can_read_product_source(
    local_open: bool,
    queued_send_blocked: bool,
    send_stream: &ReliableSendStream,
    sender_queue: &TcpRelaySenderQueue,
    mux_limits: MuxLimits,
    queue_limit: usize,
) -> bool {
    local_open
        && !queued_send_blocked
        && tcp_relay_can_read_into_sender_queue(send_stream, sender_queue, mux_limits, queue_limit)
}

pub(super) fn tcp_relay_sender_queue_read_budget(
    send_stream: &ReliableSendStream,
    sender_queue: &TcpRelaySenderQueue,
    mux_limits: MuxLimits,
    queue_limit: usize,
    buffer_len: usize,
) -> usize {
    queue_limit
        .saturating_sub(sender_queue.bytes())
        .min(
            mux_limits
                .max_repair_bytes
                .saturating_sub(send_stream.repair_bytes()),
        )
        .min(send_stream.send_credit_bytes())
        .min(buffer_len)
}

impl RelaySenderService {
    pub(super) fn new(stream_id: StreamId) -> Self {
        Self {
            stream_id,
            flights: RelayPathFlightLedger::default(),
        }
    }

    pub(super) async fn send_stream_data(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut TcpRelayRemoteSet,
        frame: Frame,
    ) -> Result<RelaySendOutcome, RuntimeError> {
        self.send_frame(context, remotes, frame, RelaySendCause::StreamData)
            .await
    }

    pub(super) async fn send_control_frame(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut TcpRelayRemoteSet,
        frame: Frame,
        cause: RelaySendCause,
    ) -> Result<RelaySendOutcome, RuntimeError> {
        debug_assert!(!cause.is_repair());
        self.send_frame(context, remotes, frame, cause).await
    }

    pub(super) async fn send_repair_frame(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut TcpRelayRemoteSet,
        frame: Frame,
        cause: RelaySendCause,
    ) -> Result<RelaySendOutcome, RuntimeError> {
        debug_assert!(cause.is_repair());
        self.send_frame(context, remotes, frame, cause).await
    }

    async fn send_frame(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut TcpRelayRemoteSet,
        frame: Frame,
        cause: RelaySendCause,
    ) -> Result<RelaySendOutcome, RuntimeError> {
        let sent_frame = frame.clone();
        let path_key = if cause.is_repair() {
            let avoid_keys = self.flights.sent_keys_for_frame(&sent_frame);
            remotes
                .send_repair_frame(context, frame, &avoid_keys)
                .await?
        } else if matches!(sent_frame, Frame::StreamData { .. }) {
            remotes
                .send_frame_with_flight_ledger(context, frame, &self.flights)
                .await?
        } else {
            remotes.send_frame(context, frame).await?
        };
        let payload_bytes = self.flights.record_frame(path_key, &sent_frame);
        self.record_decision(path_key, payload_bytes, &sent_frame, cause);
        Ok(RelaySendOutcome { path_key })
    }

    pub(super) fn release_acked_ranges(
        &mut self,
        context: &ClientPathContext,
        ranges: &[OffsetRange],
    ) {
        for release in self.flights.release_acked_ranges(ranges) {
            context.release_relay_path_inflight(
                release.key.underlay,
                release.key.index,
                release.bytes,
            );
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "path_model",
                format_args!(
                    "stream_id={} path_underlay={:?} path_index={} released_bytes={} elapsed_ms={:.3} cause=stream_ack",
                    self.stream_id.0,
                    release.key.underlay,
                    release.key.index,
                    release.bytes,
                    release.elapsed.as_secs_f64() * 1000.0,
                ),
            );
        }
    }

    pub(super) fn release_all(&mut self, context: &ClientPathContext) {
        for release in self.flights.drain_all() {
            context.release_relay_path_inflight(
                release.key.underlay,
                release.key.index,
                release.bytes,
            );
        }
    }

    pub(super) fn can_send_stream_data_extent(
        &self,
        context: &ClientPathContext,
        remotes: &TcpRelayRemoteSet,
        lane: FlowLane,
        offset: u64,
        payload_bytes: usize,
    ) -> bool {
        remotes.bulk_send_ready_for_extent(context, lane, offset, payload_bytes, &self.flights)
    }

    pub(super) async fn send_recv_progress(
        &mut self,
        remotes: &mut TcpRelayRemoteSet,
        context: &ClientPathContext,
        recv_stream: &ReliableRecvStream,
        progress: &mut ReliableRecvProgress,
        request: RelayRecvProgressSend,
    ) -> Result<bool, RuntimeError> {
        let mut sent_any = false;
        if progress.should_send_ack(
            recv_stream,
            request.path,
            request.lane,
            context.mux_limits,
            request.force_max_data,
        ) {
            #[cfg(feature = "lab-diagnostics")]
            let ack_started = Instant::now();
            let ack_frame = recv_stream.ack_frame();
            #[cfg(feature = "lab-diagnostics")]
            lab_perf_record("mux.ack_frames", ack_started.elapsed(), 1);
            self.send_control_frame(context, remotes, ack_frame, RelaySendCause::RecvProgress)
                .await?;
            sent_any = true;
        }
        if progress.should_send_max_data(recv_stream, context.mux_limits, request.force_max_data) {
            self.send_control_frame(
                context,
                remotes,
                recv_stream.max_data_frame(),
                RelaySendCause::RecvProgress,
            )
            .await?;
            sent_any = true;
        }
        Ok(sent_any)
    }

    pub(super) async fn send_failed_path_gap_repairs(
        &mut self,
        context: &ClientPathContext,
        remotes: &mut TcpRelayRemoteSet,
        send_stream: &ReliableSendStream,
        failed_key: RelayPathKey,
        lane: FlowLane,
    ) -> Result<bool, RuntimeError> {
        let ranges = self.flights.latest_unacked_ranges_for_path(failed_key);
        if ranges.is_empty() {
            return Ok(false);
        }
        let repair_path = remotes
            .primary_path_key()
            .and_then(|key| relay_path_snapshot(context, key));
        let repair_limit = adaptive_tcp_relay_repair_bytes(repair_path, lane, context.mux_limits);
        let repair_frames = send_stream.retransmission_frames_for_ranges(&ranges, repair_limit);
        if repair_frames.is_empty() {
            return Ok(false);
        }
        let mut sent = false;
        for frame in repair_frames {
            let outcome = self
                .send_repair_frame(context, remotes, frame, RelaySendCause::PathFailureRepair)
                .await?;
            #[cfg(not(feature = "lab-diagnostics"))]
            let _ = outcome;
            sent = true;
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "repair",
                format_args!(
                    "stream_id={} path_underlay={:?} path_index={} failed_underlay={:?} failed_index={} cause=path_failure",
                    self.stream_id.0,
                    outcome.path_key.underlay,
                    outcome.path_key.index,
                    failed_key.underlay,
                    failed_key.index,
                ),
            );
        }
        Ok(sent)
    }

    fn record_decision(
        &self,
        path_key: RelayPathKey,
        payload_bytes: usize,
        frame: &Frame,
        cause: RelaySendCause,
    ) {
        #[cfg(feature = "lab-diagnostics")]
        lab_sender_service_decision(
            "client",
            None,
            self.stream_id.0,
            "primary",
            sender_service_frame_kind(frame),
            payload_bytes,
            format_args!(
                "cause={} path_underlay={:?} path_index={} pacing_bytes={} repair={}",
                cause.as_str(),
                path_key.underlay,
                path_key.index,
                frame_pacing_bytes(frame),
                cause.is_repair(),
            ),
        );
        #[cfg(not(feature = "lab-diagnostics"))]
        {
            let _ = (path_key, payload_bytes, frame, cause);
        }
    }
}

#[cfg(feature = "lab-diagnostics")]
fn sender_service_frame_kind(frame: &Frame) -> &'static str {
    match frame {
        Frame::StreamData { .. } => "stream_data",
        Frame::StreamAck { .. } => "stream_ack",
        Frame::StreamMaxData { .. } => "stream_max_data",
        Frame::StreamFin { .. } => "stream_fin",
        Frame::StreamReset { .. } => "stream_reset",
        Frame::StreamDetach { .. } => "stream_detach",
        Frame::DatagramData { .. } => "datagram_data",
        Frame::DatagramFeedback { .. } => "datagram_feedback",
        Frame::DatagramClose { .. } => "datagram_close",
        _ => "control",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SharedSecret;

    fn security() -> SecurityConfig {
        SecurityConfig::encrypted(
            SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
        )
    }

    #[test]
    fn stream_ack_releases_sender_service_flights_without_lowering_delivery_rate() {
        let path = "tcp://127.0.0.1:10251".parse::<PathSpec>().expect("path");
        let context = ClientPathContext::new(vec![path], security(), ResourceLimits::default())
            .expect("context");
        let key = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        };
        let seeded = PathRateSample::new(4 * 1024 * 1024, Duration::from_millis(20))
            .expect("seed rate sample");
        context.mark_relay_path_rate_sample(key.underlay, key.index, seeded);

        let frame = Frame::StreamData {
            stream_id: StreamId(7),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![0u8; PATH_OPEN_SCORE_BYTES]),
        };
        context.record_relay_path_send(key.underlay, key.index, PATH_OPEN_SCORE_BYTES);
        let mut sender = RelaySenderService::new(StreamId(7));
        sender.flights.record_frame(key, &frame);

        let before = context.tcp_path_snapshot(0).expect("before snapshot");
        assert_eq!(before.bytes_in_flight, PATH_OPEN_SCORE_BYTES as u64);
        sender.release_acked_ranges(
            &context,
            &[OffsetRange::new(0, PATH_OPEN_SCORE_BYTES as u64).expect("ack range")],
        );
        let after = context.tcp_path_snapshot(0).expect("after snapshot");

        assert_eq!(after.bytes_in_flight, 0);
        assert_eq!(after.delivery_rate_bps, before.delivery_rate_bps);
    }
}
