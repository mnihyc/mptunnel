use super::*;

pub(super) async fn replay_tcp_repair_cache(
    path_stream: &TcpPathStream,
    send_stream: &ReliableSendStream,
    resend_fin: bool,
) -> Result<(), RuntimeError> {
    #[cfg(feature = "lab-diagnostics")]
    let retransmit_started = Instant::now();
    let frames = send_stream.retransmission_frames();
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record(
        "mux.retransmit_all",
        retransmit_started.elapsed(),
        frames.iter().map(frame_pacing_bytes).sum(),
    );
    for frame in frames {
        path_stream.send_frame(frame).await?;
    }
    if resend_fin {
        path_stream
            .send_frame(Frame::StreamFin {
                stream_id: path_stream.stream_id,
                final_offset: send_stream.next_offset(),
            })
            .await?;
    }
    Ok(())
}

#[cfg(feature = "lab-diagnostics")]
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

pub(super) fn tcp_relay_error_is_migratable(err: &RuntimeError) -> bool {
    matches!(
        err,
        RuntimeError::PathHeartbeatTimeout
            | RuntimeError::TcpPathSessionClosed
            | RuntimeError::Tcp(_)
            | RuntimeError::Encrypted(_)
            | RuntimeError::RemoteClosed(_)
            | RuntimeError::Protocol(_)
    )
}

#[derive(Debug, Default)]
pub(super) struct ReliableRecvProgress {
    last_max_data_offset: u64,
}

impl ReliableRecvProgress {
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
    let payload_step = tcp_relay_buffer_len(mux_limits) as u64;
    window_step
        .max(payload_step)
        .min(mux_limits.max_stream_window_bytes)
}

pub(super) async fn send_tcp_recv_progress(
    path_stream: &TcpPathStream,
    recv_stream: &ReliableRecvStream,
    progress: &mut ReliableRecvProgress,
    mux_limits: MuxLimits,
    force_max_data: bool,
) -> Result<(), RuntimeError> {
    #[cfg(feature = "lab-diagnostics")]
    let ack_started = Instant::now();
    let ack_frames = recv_stream.ack_frames();
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record("mux.ack_frames", ack_started.elapsed(), ack_frames.len());
    for frame in ack_frames {
        path_stream.send_frame(frame).await?;
    }
    if progress.should_send_max_data(recv_stream, mux_limits, force_max_data) {
        path_stream.send_frame(recv_stream.max_data_frame()).await?;
    }
    Ok(())
}

pub(super) fn tcp_relay_recv_progress_resend_active(
    recv_stream: &ReliableRecvStream,
    remote_open: bool,
) -> bool {
    remote_open && (recv_stream.next_offset() > 0 || recv_stream.reorder_bytes() > 0)
}

pub(super) fn reliable_stream_recv_progress_interval(
    path: Option<PathSnapshot>,
    class: TrafficClass,
) -> Duration {
    tcp_relay_stall_timeout(path, class)
        .div_f64(2.0)
        .max(UDP_MIN_RESPONSE_TIMEOUT)
        .min(TCP_STREAM_STALL_MIN_TIMEOUT)
}

pub(super) async fn send_tcp_recv_progress_remote_set(
    remotes: &mut TcpRelayRemoteSet,
    context: &ClientPathContext,
    recv_stream: &ReliableRecvStream,
    progress: &mut ReliableRecvProgress,
    force_max_data: bool,
) -> Result<(), RuntimeError> {
    #[cfg(feature = "lab-diagnostics")]
    let ack_started = Instant::now();
    let ack_frames = recv_stream.ack_frames();
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record("mux.ack_frames", ack_started.elapsed(), ack_frames.len());
    for frame in ack_frames {
        remotes.send_frame(context, frame).await?;
    }
    if progress.should_send_max_data(recv_stream, context.mux_limits, force_max_data) {
        remotes
            .send_frame(context, recv_stream.max_data_frame())
            .await?;
    }
    Ok(())
}

pub(super) fn tcp_relay_buffer_len(mux_limits: MuxLimits) -> usize {
    mux_limits
        .max_tcp_relay_chunk_bytes
        .min(mux_limits.max_payload_bytes)
        .min(mux_limits.max_tcp_path_inflight_bytes)
        .max(1)
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

pub(super) fn adaptive_tcp_relay_chunk_bytes(
    path: Option<PathSnapshot>,
    class: TrafficClass,
    mux_limits: MuxLimits,
) -> usize {
    let cap = tcp_relay_buffer_len(mux_limits);
    let Some(path) = path else {
        return cap;
    };

    let bdp_bytes = tcp_path_bdp_bytes(path);
    let class_gain = tcp_class_chunk_gain(class);
    let stability = tcp_path_stability_factor(path);
    let queue_factor = tcp_path_queue_factor(path, bdp_bytes);
    let target = (bdp_bytes * class_gain * stability * queue_factor).ceil() as usize;
    target.clamp(1, cap)
}

pub(super) fn adaptive_tcp_relay_inflight_bytes(
    path: Option<PathSnapshot>,
    class: TrafficClass,
    mux_limits: MuxLimits,
) -> usize {
    let cap = mux_limits.max_tcp_path_inflight_bytes.max(1);
    let floor = tcp_relay_buffer_len(mux_limits).min(cap).max(1);
    let Some(path) = path else {
        return cap;
    };

    let bdp_bytes = tcp_path_bdp_bytes(path);
    let target = bdp_bytes
        * tcp_class_inflight_gain(class)
        * tcp_path_stability_factor(path)
        * tcp_path_queue_factor(path, bdp_bytes);
    (target.ceil() as usize).clamp(floor, cap)
}

pub(super) fn tcp_path_bdp_bytes(path: PathSnapshot) -> f64 {
    (path.delivery_rate_bps.max(1.0) / 8.0) * (path.srtt_ms.max(1.0) / 1000.0)
}

pub(super) fn tcp_class_chunk_gain(class: TrafficClass) -> f64 {
    match class {
        TrafficClass::Control | TrafficClass::RealtimeDatagram => 1.0 / 64.0,
        TrafficClass::Interactive => 1.0 / 16.0,
        TrafficClass::Bulk => 1.0 / 4.0,
        TrafficClass::Background => 1.0 / 8.0,
    }
}

pub(super) fn tcp_class_inflight_gain(class: TrafficClass) -> f64 {
    match class {
        TrafficClass::Control | TrafficClass::RealtimeDatagram => 0.5,
        TrafficClass::Interactive => 1.0,
        TrafficClass::Bulk => 2.0,
        TrafficClass::Background => 1.0,
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

pub(super) async fn relay_tcp_stream<S>(
    mut local: S,
    mut path_stream: TcpPathStream,
    mux_limits: MuxLimits,
) -> Result<PathDeliveryStats, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let stream_id = path_stream.stream_id;
    let mut send_stream = ReliableSendStream::new(stream_id, mux_limits);
    send_stream.update_max_offset(path_stream.max_offset);
    let mut recv_stream = ReliableRecvStream::new(stream_id, mux_limits);
    let chunk_size = tcp_relay_buffer_len(mux_limits)
        .min(path_stream.max_frame_payload_bytes)
        .max(1);
    let mut buf = vec![0u8; chunk_size];
    let mut local_open = true;
    let mut remote_open = true;
    let mut stats = PathDeliveryStats::default();
    let mut close_sent = false;
    let mut pending_local_fin = false;
    let mut pending_remote_fin_offset = None;
    let mut last_repair_replay_at = Instant::now();
    let mut recv_progress = ReliableRecvProgress::default();
    let mut last_recv_progress_sent_at = Instant::now();

    let result = loop {
        if !local_open && !remote_open && send_stream.repair_bytes() == 0 {
            break Ok(stats);
        }
        let relay_class = path_stream.current_class();
        let repair_replay_interval =
            tcp_relay_repair_replay_interval(send_stream.repair_bytes(), mux_limits);
        let repair_replay_deadline =
            tokio::time::Instant::from_std(last_repair_replay_at + repair_replay_interval);
        let recv_progress_deadline = tokio::time::Instant::from_std(
            last_recv_progress_sent_at + reliable_stream_recv_progress_interval(None, relay_class),
        );
        let inflight_limit = mux_limits.max_tcp_path_inflight_bytes;
        let can_read_local =
            local_open && tcp_relay_can_read_with_limit(&send_stream, inflight_limit);
        let read_budget = if can_read_local {
            tcp_relay_read_budget_with_limit(&send_stream, mux_limits, inflight_limit, buf.len())
        } else {
            0
        };

        tokio::select! {
            biased;
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
                match frame? {
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
                        send_tcp_recv_progress(
                            &path_stream,
                            &recv_stream,
                            &mut recv_progress,
                            mux_limits,
                            false,
                        )
                        .await?;
                        last_recv_progress_sent_at = Instant::now();
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
                        ranges,
                    } if ack_stream_id == stream_id => {
                        let previous_repair_bytes = send_stream.repair_bytes();
                        #[cfg(feature = "lab-diagnostics")]
                        let mux_started = Instant::now();
                        let ack = send_stream.apply_ack(&ranges);
                        #[cfg(feature = "lab-diagnostics")]
                        lab_perf_record("mux.apply_ack", mux_started.elapsed(), ack.released_bytes);
                        #[cfg(not(feature = "lab-diagnostics"))]
                        let _ = ack;
                        if send_stream.repair_bytes() < previous_repair_bytes {
                            last_repair_replay_at = Instant::now();
                        }
                        if pending_local_fin && send_stream.repair_bytes() == 0 {
                            let frame = Frame::StreamFin {
                                stream_id,
                                final_offset: send_stream.next_offset(),
                            };
                            path_stream.send_frame(frame).await?;
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
                    } => {
                        replay_tcp_repair_cache(&path_stream, &send_stream, false).await?;
                        last_repair_replay_at = Instant::now();
                    }
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
            _ = tokio::time::sleep_until(repair_replay_deadline), if send_stream.repair_bytes() > 0 => {
                replay_tcp_repair_cache(&path_stream, &send_stream, false).await?;
                last_repair_replay_at = Instant::now();
            }
            _ = tokio::time::sleep_until(recv_progress_deadline), if path_stream.underlay == UnderlayProtocol::Udp
                && tcp_relay_recv_progress_resend_active(&recv_stream, remote_open) => {
                send_tcp_recv_progress(
                    &path_stream,
                    &recv_stream,
                    &mut recv_progress,
                    mux_limits,
                    true,
                )
                .await?;
                last_recv_progress_sent_at = Instant::now();
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
                    if path_stream.underlay == UnderlayProtocol::Udp
                        && send_stream.repair_bytes() > 0
                    {
                        pending_local_fin = true;
                    } else {
                            let frame = Frame::StreamFin {
                                stream_id,
                                final_offset: send_stream.next_offset(),
                            };
                            path_stream.send_frame(frame).await?;
                            close_sent = true;
                        }
                    local_open = false;
                } else {
                    #[cfg(feature = "lab-diagnostics")]
                    let copy_started = Instant::now();
                    let payload = Bytes::copy_from_slice(&buf[..read]);
                    #[cfg(feature = "lab-diagnostics")]
                    lab_perf_record("relay.copy_local_chunk", copy_started.elapsed(), read);
                    #[cfg(feature = "lab-diagnostics")]
                    let mux_started = Instant::now();
                    let frame = send_stream.send_data(payload, StreamFlags::NONE)?;
                    #[cfg(feature = "lab-diagnostics")]
                    lab_perf_record("mux.send_data", mux_started.elapsed(), read);
                    path_stream.send_frame(frame).await?;
                    stats.record_payload_bytes(read);
                }
            }
            else => break Ok(stats),
        }
    };

    if !close_sent {
        path_stream.close().await;
    }
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_flush("stream_close");
    result
}

pub(super) fn tcp_relay_repair_replay_interval(
    repair_bytes: usize,
    mux_limits: MuxLimits,
) -> Duration {
    if repair_bytes == 0 {
        return TCP_STREAM_STALL_MAX_TIMEOUT;
    }
    let inflight = mux_limits.max_tcp_path_inflight_bytes.max(1) as f64;
    let pressure = (repair_bytes as f64 / inflight).clamp(0.0, 1.0);
    let min = TCP_STREAM_STALL_MIN_TIMEOUT.as_secs_f64();
    let max = TCP_STREAM_STALL_MAX_TIMEOUT.as_secs_f64();
    Duration::from_secs_f64(min + (max - min) * pressure)
}

pub(super) fn tcp_relay_can_read_with_limit(
    send_stream: &ReliableSendStream,
    inflight_limit: usize,
) -> bool {
    send_stream.repair_bytes() < inflight_limit.max(1)
}

pub(super) fn tcp_relay_read_budget_with_limit(
    send_stream: &ReliableSendStream,
    mux_limits: MuxLimits,
    inflight_limit: usize,
    buffer_len: usize,
) -> usize {
    inflight_limit
        .max(1)
        .min(mux_limits.max_tcp_path_inflight_bytes)
        .saturating_sub(send_stream.repair_bytes())
        .min(buffer_len)
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
}
