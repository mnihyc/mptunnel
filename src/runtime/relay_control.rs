use super::*;

#[derive(Debug, Clone, Copy)]
pub(super) struct TcpRelayFlowSignals {
    sent_offset: u64,
    received_offset: u64,
    repair_bytes: usize,
}

impl TcpRelayFlowSignals {
    pub(super) fn new(sent_offset: u64, received_offset: u64, repair_bytes: usize) -> Self {
        Self {
            sent_offset,
            received_offset,
            repair_bytes,
        }
    }

    pub(super) fn observed_bytes(self) -> u64 {
        self.sent_offset
            .max(self.received_offset)
            .saturating_add(self.repair_bytes as u64)
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct TcpRelayFlowDemandTracker {
    current: FlowLane,
    rebalance_attempted: bool,
    started_at: Instant,
    last_refresh_at: Instant,
    last_observed_bytes: u64,
    send_rate_bps: f64,
}

impl TcpRelayFlowDemandTracker {
    pub(super) fn new() -> Self {
        let now = Instant::now();
        Self {
            current: FlowLane::Latency,
            rebalance_attempted: false,
            started_at: now,
            last_refresh_at: now,
            last_observed_bytes: 0,
            send_rate_bps: 0.0,
        }
    }

    pub(super) fn refresh(
        &mut self,
        signals: TcpRelayFlowSignals,
        path: Option<PathSnapshot>,
        mux_limits: MuxLimits,
    ) -> TcpRelayFlowDecision {
        let now = Instant::now();
        let observed_bytes = signals.observed_bytes();
        let delta_bytes = observed_bytes.saturating_sub(self.last_observed_bytes);
        let elapsed = now.duration_since(self.last_refresh_at);
        if delta_bytes > 0 || elapsed >= Duration::from_millis(1) {
            let sample_rate = delta_bytes as f64 * 8.0 / elapsed.as_secs_f64().max(0.001);
            self.send_rate_bps = if self.send_rate_bps <= 0.0 {
                sample_rate
            } else {
                self.send_rate_bps * 0.75 + sample_rate * 0.25
            };
        }
        self.last_refresh_at = now;
        self.last_observed_bytes = observed_bytes;
        let previous = self.current;
        let threshold = tcp_auto_bulk_threshold_bytes(path, mux_limits);
        let demand =
            FlowDemand::reliable_stream(observed_bytes, signals.repair_bytes as u64, threshold);
        let mut demand = demand;
        let flow_age = now.duration_since(self.started_at);
        let idle_gap = delta_bytes == 0 && elapsed >= tcp_auto_interactive_idle_gap(path);
        let rate_threshold = tcp_auto_bulk_rate_threshold_bps(path, mux_limits);
        let sustained_bulk = observed_bytes >= threshold
            && (self.send_rate_bps >= rate_threshold
                || flow_age >= tcp_auto_interactive_idle_gap(path) * 2);
        if self.current == FlowLane::Throughput && !idle_gap {
            demand.lane = FlowLane::Throughput;
            demand.throughput_weight_ppm = demand
                .throughput_weight_ppm
                .max(FlowDemand::PPM_MAX / 2 + 1);
            demand.latency_weight_ppm =
                FlowDemand::PPM_MAX.saturating_sub(demand.throughput_weight_ppm);
        } else if !sustained_bulk {
            demand.lane = FlowLane::Latency;
            demand.throughput_weight_ppm =
                demand.throughput_weight_ppm.min(FlowDemand::PPM_MAX / 2);
            demand.latency_weight_ppm =
                FlowDemand::PPM_MAX.saturating_sub(demand.throughput_weight_ppm);
            if idle_gap {
                self.rebalance_attempted = false;
            }
        }
        self.current = demand.lane;
        TcpRelayFlowDecision {
            demand,
            previous_lane: previous,
            promoted_to_throughput: previous != FlowLane::Throughput
                && self.current == FlowLane::Throughput,
        }
    }

    pub(super) fn should_rebalance(self, update: TcpRelayFlowDecision) -> bool {
        update.promoted_to_throughput && !self.rebalance_attempted
    }

    pub(super) fn mark_rebalance_attempted(&mut self) {
        self.rebalance_attempted = true;
    }
}

fn tcp_auto_bulk_rate_threshold_bps(path: Option<PathSnapshot>, mux_limits: MuxLimits) -> f64 {
    path.map_or_else(
        || tcp_relay_buffer_len(mux_limits) as f64 * 8.0 * 4.0,
        |path| path.delivery_rate_bps.max(1.0) * 0.125,
    )
}

fn tcp_auto_interactive_idle_gap(path: Option<PathSnapshot>) -> Duration {
    let srtt_ms = path.map_or(100.0, |path| path.srtt_ms.max(1.0));
    Duration::from_secs_f64((srtt_ms / 1000.0 * 4.0).clamp(0.05, 2.0))
}

#[derive(Debug, Clone, Copy)]
pub(super) struct TcpRelayFlowDecision {
    pub(super) demand: FlowDemand,
    pub(super) previous_lane: FlowLane,
    pub(super) promoted_to_throughput: bool,
}

pub(super) fn tcp_auto_bulk_threshold_bytes(
    path: Option<PathSnapshot>,
    mux_limits: MuxLimits,
) -> u64 {
    let relay_chunk = tcp_relay_buffer_len(mux_limits) as u64;
    let window = mux_limits.max_stream_window_bytes.max(relay_chunk);
    let bdp_bytes = path.map_or(relay_chunk, |path| {
        ((path.delivery_rate_bps.max(1.0) / 8.0) * (path.srtt_ms.max(1.0) / 1000.0)).ceil() as u64
    });
    let ramp_floor = relay_chunk.saturating_mul(2).min(window);
    let ramp_bdp = bdp_bytes.saturating_div(8).max(relay_chunk).max(ramp_floor);
    ramp_bdp.min(window)
}

pub(super) async fn relay_migrating_tcp_stream<S>(
    mut local: S,
    context: &ClientPathContext,
    spec: TcpRelayOpenSpec,
    remote: OpenedRemoteStream,
) -> Result<PathDeliveryStats, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let initial_key = RelayPathKey {
        underlay: remote.stream.underlay,
        index: remote.path_index,
    };
    let mut remotes = TcpRelayRemoteSet::new(remote, tcp_stream_frame_queue(context.mux_limits));
    let stream_id = remotes.stream_id();
    let mut send_stream = ReliableSendStream::new(stream_id, context.mux_limits);
    send_stream.update_max_offset(remotes.max_offset());
    let mut recv_stream = ReliableRecvStream::new(stream_id, context.mux_limits);
    let chunk_size = adaptive_tcp_relay_chunk_bytes(None, FlowLane::Latency, context.mux_limits);
    let mut buf = vec![0u8; chunk_size];
    let mut local_open = true;
    let mut remote_open = true;
    let mut pending_local_fin = false;
    let mut pending_remote_fin_offset = None;
    let mut stats = PathDeliveryStats::default();
    let mut path_stats = HashMap::<RelayPathKey, PathDeliveryStats>::new();
    let mut path_flights = RelayPathFlightLedger::default();
    let mut flow_demand = TcpRelayFlowDemandTracker::new();
    let mut last_stream_progress_at = Instant::now();
    let mut last_delivery_progress_at = Instant::now();
    let mut last_response_stall_repair_at = Instant::now();
    let mut response_stall_reannounce_attempts = 0_u32;
    let mut last_receive_hole_repair_at = Instant::now();
    let mut receive_hole_repair_attempts = 0_u32;
    let mut path_last_delivery_at = HashMap::from([(initial_key, Instant::now())]);
    let mut interactive_response_pending = false;
    let mut recv_progress = ReliableRecvProgress::default();
    let mut last_recv_progress_sent_at = Instant::now();
    #[cfg(feature = "lab-diagnostics")]
    let mut last_reported_budget: Option<(FlowLane, usize, usize)> = None;

    let result = loop {
        if !local_open && !remote_open && send_stream.repair_bytes() == 0 {
            break Ok(stats);
        }
        let path_snapshot = remotes
            .primary_path_key()
            .and_then(|key| relay_path_snapshot(context, key));
        let demand_update = flow_demand.refresh(
            TcpRelayFlowSignals::new(
                send_stream.next_offset(),
                recv_stream.next_offset(),
                send_stream.repair_bytes(),
            ),
            path_snapshot,
            context.mux_limits,
        );
        let relay_demand = demand_update.demand;
        let relay_lane = relay_demand.lane;
        if demand_update.promoted_to_throughput {
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "client_stream_lane_promoted",
                format_args!(
                    "stream_id={} previous={:?} lane={:?} latency_weight_ppm={} throughput_weight_ppm={} sent_offset={} received_offset={} repair_bytes={}",
                    stream_id.0,
                    demand_update.previous_lane,
                    relay_lane,
                    demand_update.demand.latency_weight_ppm,
                    demand_update.demand.throughput_weight_ppm,
                    send_stream.next_offset(),
                    recv_stream.next_offset(),
                    send_stream.repair_bytes(),
                ),
            );
            for key in remotes.path_keys() {
                context.change_relay_path_lane_load(
                    key.underlay,
                    key.index,
                    demand_update.previous_lane,
                    relay_lane,
                );
            }
            remotes.set_lane(relay_lane);
        }
        if flow_demand.should_rebalance(demand_update) {
            flow_demand.mark_rebalance_attempted();
            if let Err(err) = switch_tcp_relay_to_best_path(
                context,
                &spec,
                relay_lane,
                &mut remotes,
                &send_stream,
                !local_open,
                TcpRelayAttachMode::BulkStriping,
            )
            .await
            {
                eprintln!("warning: TCP auto path attachment failed: {err}");
            } else {
                last_stream_progress_at = Instant::now();
            }
            send_stream.update_max_offset(remotes.max_offset());
        }
        let adaptive_chunk =
            adaptive_tcp_relay_chunk_bytes(path_snapshot, relay_lane, context.mux_limits)
                .min(remotes.max_frame_payload_bytes(context.mux_limits))
                .max(1);
        resize_tcp_relay_buffer(&mut buf, adaptive_chunk);
        let adaptive_inflight =
            adaptive_tcp_relay_inflight_bytes(path_snapshot, relay_lane, context.mux_limits);
        #[cfg(feature = "lab-diagnostics")]
        if last_reported_budget != Some((relay_lane, adaptive_chunk, adaptive_inflight)) {
            lab_diagnostic(
                "client_relay_budget",
                format_args!(
                    "stream_id={} lane={:?} chunk_bytes={} inflight_bytes={} path_snapshot={}",
                    stream_id.0,
                    relay_lane,
                    adaptive_chunk,
                    adaptive_inflight,
                    path_snapshot.is_some(),
                ),
            );
            last_reported_budget = Some((relay_lane, adaptive_chunk, adaptive_inflight));
        }
        let stall_watch_active = tcp_relay_stall_watch_active(
            &send_stream,
            &recv_stream,
            remote_open,
            relay_lane,
            interactive_response_pending,
            context.mux_limits,
        );
        let stall_progress_anchor = tcp_relay_stall_progress_anchor(
            last_stream_progress_at,
            last_delivery_progress_at,
            last_response_stall_repair_at,
            &recv_stream,
            remote_open,
            relay_lane,
            context.mux_limits,
        );
        let receive_hole_repair_active =
            tcp_relay_receive_hole_repair_active(&recv_stream, remote_open);
        let receive_hole_repair_deadline = tcp_relay_receive_hole_repair_deadline(
            last_delivery_progress_at,
            last_receive_hole_repair_at,
            path_snapshot,
            relay_lane,
        );
        let stall_deadline =
            tcp_relay_stall_deadline(stall_progress_anchor, path_snapshot, relay_lane);
        let recv_progress_deadline = tokio::time::Instant::from_std(
            last_recv_progress_sent_at
                + reliable_stream_recv_progress_interval(path_snapshot, relay_lane),
        );

        tokio::select! {
            _ = tokio::time::sleep_until(receive_hole_repair_deadline), if receive_hole_repair_active => {
                match attach_tcp_relay_paths(
                    context,
                    &spec,
                    relay_lane,
                    &mut remotes,
                    &send_stream,
                    !local_open,
                    TcpRelayAttachMode::Any,
                )
                .await
                {
                    Ok(attached) if attached > 0 => {
                        send_stream.update_max_offset(remotes.max_offset());
                        last_receive_hole_repair_at = Instant::now();
                        receive_hole_repair_attempts = 0;
                        tcp_relay_refresh_path_tracking(
                            &mut path_last_delivery_at,
                            &remotes.path_keys(),
                            Instant::now(),
                        );
                        continue;
                    }
                    Ok(_) => {
                        receive_hole_repair_attempts =
                            receive_hole_repair_attempts.saturating_add(1);
                        if receive_hole_repair_attempts >= tcp_relay_receive_hole_failure_attempts(relay_lane) {
                            let path_keys = remotes.path_keys();
                            if let Some(path_key) = tcp_relay_receive_hole_victim(
                                context,
                                &path_keys,
                                relay_lane,
                                recv_stream.reorder_bytes().max(1),
                                &path_last_delivery_at,
                            ) && remotes.fail_path_key(context, path_key).await
                            {
                                path_last_delivery_at.remove(&path_key);
                                if !remotes.is_empty()
                                    && let Err(err) = remotes
                                        .reannounce_active_path(context, &spec, relay_lane)
                                        .await
                                {
                                    eprintln!(
                                        "warning: TCP receive-hole survivor reannounce failed: {err}"
                                    );
                                }
                                send_stream.update_max_offset(remotes.max_offset());
                                last_stream_progress_at = Instant::now();
                                last_receive_hole_repair_at = Instant::now();
                                receive_hole_repair_attempts = 0;
                                continue;
                            }
                            if !remotes.is_empty()
                                && let Err(err) = remotes
                                    .reannounce_active_path(context, &spec, relay_lane)
                                    .await
                            {
                                eprintln!(
                                    "warning: TCP receive-hole sole-survivor reannounce failed: {err}"
                                );
                            }
                        }
                        last_receive_hole_repair_at = Instant::now();
                    }
                    Err(err) if remotes.is_empty() => break Err(err),
                    Err(err) => {
                        eprintln!("warning: TCP receive-hole repair failed: {err}");
                        last_receive_hole_repair_at = Instant::now();
                    }
                }
            }
            _ = tokio::time::sleep_until(stall_deadline), if stall_watch_active => {
                if remotes.path_keys().len() <= 1 {
                    let reannounce_budget = tcp_relay_sole_survivor_reannounce_attempts(
                        tcp_relay_stall_timeout(path_snapshot, relay_lane),
                    );
                    if response_stall_reannounce_attempts
                        < reannounce_budget
                    {
                        response_stall_reannounce_attempts =
                            response_stall_reannounce_attempts.saturating_add(1);
                        match remotes
                            .reannounce_active_path(context, &spec, relay_lane)
                            .await
                        {
                            Ok(()) => {
                                send_stream.update_max_offset(remotes.max_offset());
                                last_stream_progress_at = Instant::now();
                                last_response_stall_repair_at = Instant::now();
                                tcp_relay_refresh_path_tracking(
                                    &mut path_last_delivery_at,
                                    &remotes.path_keys(),
                                    Instant::now(),
                                );
                                continue;
                            }
                            Err(err) => {
                                eprintln!(
                                    "warning: TCP stall sole-survivor reannounce failed: {err}"
                                );
                            }
                        }
                    } else {
                        response_stall_reannounce_attempts = 0;
                    }
                }
                if let Some(instance) = remotes.active_path_instance() {
                    remotes.fail_path_instance(context, instance).await;
                }
                if !remotes.is_empty() {
                    match remotes
                        .reannounce_active_path(context, &spec, relay_lane)
                        .await
                    {
                        Ok(()) => {
                            send_stream.update_max_offset(remotes.max_offset());
                            last_stream_progress_at = Instant::now();
                            last_response_stall_repair_at = Instant::now();
                            tcp_relay_refresh_path_tracking(
                                &mut path_last_delivery_at,
                                &remotes.path_keys(),
                                Instant::now(),
                            );
                            continue;
                        }
                        Err(err) => {
                            eprintln!("warning: TCP stall survivor reannounce failed: {err}");
                        }
                    }
                }
                match attach_tcp_relay_paths(
                    context,
                    &spec,
                    relay_lane,
                    &mut remotes,
                    &send_stream,
                    !local_open,
                    TcpRelayAttachMode::Any,
                )
                .await
                {
                        Ok(attached) if attached > 0 => {
                            send_stream.update_max_offset(remotes.max_offset());
                            last_stream_progress_at = Instant::now();
                            last_response_stall_repair_at = Instant::now();
                            tcp_relay_refresh_path_tracking(
                                &mut path_last_delivery_at,
                                &remotes.path_keys(),
                                Instant::now(),
                            );
                            continue;
                        }
                    Ok(_) => {
                        last_stream_progress_at = Instant::now();
                        last_response_stall_repair_at = Instant::now();
                    }
                    Err(err) if remotes.is_empty() => break Err(err),
                    Err(err) => {
                        eprintln!("warning: TCP stream stall repair failed: {err}");
                        last_stream_progress_at = Instant::now();
                        last_response_stall_repair_at = Instant::now();
                    }
                }
            }
            _ = tokio::time::sleep_until(recv_progress_deadline), if remotes.path_keys().len() > 1
                && tcp_relay_recv_progress_resend_active(&recv_stream, remote_open) => {
                match send_tcp_recv_progress_remote_set(
                    &mut remotes,
                    context,
                    &recv_stream,
                    &mut recv_progress,
                    true,
                )
                .await
                {
                    Ok(()) => {
                        last_stream_progress_at = Instant::now();
                        last_recv_progress_sent_at = Instant::now();
                    }
                    Err(err) if tcp_relay_error_is_migratable(&err) => {
                        match attach_tcp_relay_paths(
                            context,
                            &spec,
                            relay_lane,
                            &mut remotes,
                            &send_stream,
                            !local_open,
                            TcpRelayAttachMode::Any,
                        )
                        .await
                        {
                            Ok(attached) if attached > 0 => {
                                send_stream.update_max_offset(remotes.max_offset());
                                last_stream_progress_at = Instant::now();
                                last_recv_progress_sent_at = Instant::now();
                            }
                            Ok(_) => break Err(err),
                            Err(err) => break Err(err),
                        }
                    }
                    Err(err) => break Err(err),
                }
            }
            read = async {
                let read_budget = tcp_relay_read_budget_with_limit(
                    &send_stream,
                    context.mux_limits,
                    adaptive_inflight,
                    adaptive_chunk.min(buf.len()),
                );
                #[cfg(feature = "lab-diagnostics")]
                let read_started = Instant::now();
                let result = local.read(&mut buf[..read_budget]).await;
                #[cfg(feature = "lab-diagnostics")]
                if let Ok(read) = &result {
                    lab_perf_record("relay.local_read_wait", read_started.elapsed(), *read);
                }
                result
            }, if local_open && tcp_relay_can_read_with_limit(&send_stream, adaptive_inflight) => {
                let read = match read {
                    Ok(read) => read,
                    Err(err) => break Err(RuntimeError::Io(err)),
                };
                if read == 0 {
                    local_open = false;
                    if remotes.fin_requires_repair_drain() && send_stream.repair_bytes() > 0 {
                        pending_local_fin = true;
                    } else {
                        match remotes
                            .send_frame(
                                context,
                                Frame::StreamFin {
                                    stream_id,
                                    final_offset: send_stream.next_offset(),
                                },
                            )
                            .await
                        {
                            Ok(_) => {
                                last_stream_progress_at = Instant::now();
                            }
                            Err(err) if tcp_relay_error_is_migratable(&err) => {
                                if let Err(err) = attach_tcp_relay_paths(
                                    context,
                                    &spec,
                                    relay_lane,
                                    &mut remotes,
                                    &send_stream,
                                    !local_open,
                                    TcpRelayAttachMode::Any,
                                )
                                .await
                                {
                                    break Err(err);
                                }
                                last_stream_progress_at = Instant::now();
                            }
                            Err(err) => break Err(err),
                        }
                    }
                } else {
                    if tcp_relay_expects_interactive_response(relay_lane) && remote_open {
                        interactive_response_pending = true;
                    }
                    #[cfg(feature = "lab-diagnostics")]
                    let copy_started = Instant::now();
                    let payload = Bytes::copy_from_slice(&buf[..read]);
                    #[cfg(feature = "lab-diagnostics")]
                    lab_perf_record("relay.copy_local_chunk", copy_started.elapsed(), read);
                    #[cfg(feature = "lab-diagnostics")]
                    let mux_started = Instant::now();
                    let frame = match send_stream.send_data(payload, StreamFlags::NONE) {
                        Ok(frame) => frame,
                        Err(err) => break Err(RuntimeError::Stream(err)),
                    };
                    #[cfg(feature = "lab-diagnostics")]
                    lab_perf_record("mux.send_data", mux_started.elapsed(), read);
                    let sent_frame = frame.clone();
                    match remotes.send_frame(context, frame).await {
                        Ok(path_key) => {
                            last_stream_progress_at = Instant::now();
                            stats.record_payload_bytes(read);
                            path_stats
                                .entry(path_key)
                                .or_default()
                                .record_payload_bytes(read);
                            path_flights.record_frame(path_key, &sent_frame);
                        }
                        Err(err) if tcp_relay_error_is_migratable(&err) => {
                            match attach_tcp_relay_paths(
                                context,
                                &spec,
                                relay_lane,
                                &mut remotes,
                                &send_stream,
                                !local_open,
                                TcpRelayAttachMode::Any,
                            )
                            .await
                            {
                                Ok(attached) if attached > 0 => {
                                    last_stream_progress_at = Instant::now();
                                    stats.record_payload_bytes(read);
                                }
                                Ok(_) => break Err(err),
                                Err(err) => break Err(err),
                            }
                        }
                        Err(err) => break Err(err),
                    }
                }
            }
            frame = async {
                #[cfg(feature = "lab-diagnostics")]
                let recv_started = Instant::now();
                let result = remotes.recv_frame().await;
                #[cfg(feature = "lab-diagnostics")]
                if let Ok(TcpRelayRemoteFrame { frame: Ok(frame), .. }) = &result {
                    lab_perf_record("relay.path_recv_frame_wait", recv_started.elapsed(), frame_pacing_bytes(frame));
                }
                result
            }, if remote_open || send_stream.repair_bytes() > 0 => {
                let TcpRelayRemoteFrame { instance, frame } = match frame {
                    Ok(frame) => frame,
                    Err(err) if tcp_relay_error_is_migratable(&err) => {
                        match attach_tcp_relay_paths(
                            context,
                            &spec,
                            relay_lane,
                            &mut remotes,
                            &send_stream,
                            !local_open,
                            TcpRelayAttachMode::Any,
                        )
                        .await
                        {
                            Ok(attached) if attached > 0 => {
                                last_stream_progress_at = Instant::now();
                                continue;
                            }
                            Ok(_) => break Err(err),
                            Err(_) => break Err(err),
                        }
                    }
                    Err(err) => break Err(err),
                };
                let path_key = instance.key;
                let frame = match frame {
                    Ok(frame) => frame,
                    Err(err) if tcp_relay_error_is_migratable(&err) => {
                        remotes.fail_path_instance(context, instance).await;
                        if !remotes.is_empty()
                            && let Err(err) = remotes
                                .reannounce_active_path(context, &spec, relay_lane)
                                .await
                        {
                            eprintln!("warning: TCP path-error survivor reannounce failed: {err}");
                        } else if !remotes.is_empty() {
                            send_stream.update_max_offset(remotes.max_offset());
                            last_stream_progress_at = Instant::now();
                            last_response_stall_repair_at = Instant::now();
                            tcp_relay_refresh_path_tracking(
                                &mut path_last_delivery_at,
                                &remotes.path_keys(),
                                Instant::now(),
                            );
                        }
                        if remotes.is_empty() {
                            match attach_tcp_relay_paths(
                                context,
                                &spec,
                                relay_lane,
                                &mut remotes,
                                &send_stream,
                                !local_open,
                                TcpRelayAttachMode::Any,
                            )
                            .await
                            {
                                Ok(attached) if attached > 0 => {
                                    send_stream.update_max_offset(remotes.max_offset());
                                    last_stream_progress_at = Instant::now();
                                    tcp_relay_refresh_path_tracking(
                                        &mut path_last_delivery_at,
                                        &remotes.path_keys(),
                                        Instant::now(),
                                    );
                                    continue;
                                }
                                Ok(_) => break Err(err),
                                Err(_) => break Err(err),
                            }
                        }
                        path_last_delivery_at.remove(&path_key);
                        continue;
                    }
                    Err(err) => break Err(err),
                };
                match frame {
                    Frame::StreamData {
                        stream_id: received_stream_id,
                        offset,
                        flags,
                        payload,
                    } if received_stream_id == stream_id && remote_open => {
                        let previous_remote_offset = recv_stream.next_offset();
                        #[cfg(feature = "lab-diagnostics")]
                        let payload_len = payload.len();
                        #[cfg(feature = "lab-diagnostics")]
                        let mux_started = Instant::now();
                        let outcome = match recv_stream.receive_data(offset, payload, flags) {
                            Ok(outcome) => outcome,
                            Err(err) => break Err(RuntimeError::Stream(err)),
                        };
                        #[cfg(feature = "lab-diagnostics")]
                        lab_perf_record("mux.receive_data", mux_started.elapsed(), payload_len);
                        last_stream_progress_at = Instant::now();
                        let delivered_progress = recv_stream.next_offset() > previous_remote_offset;
                        if delivered_progress {
                            last_delivery_progress_at = Instant::now();
                            receive_hole_repair_attempts = 0;
                            response_stall_reannounce_attempts = 0;
                            path_last_delivery_at.insert(path_key, Instant::now());
                            if tcp_relay_delivery_path_should_become_active(
                                context,
                                remotes.active_path_key(),
                                path_key,
                                relay_lane,
                                tcp_relay_attach_payload_bytes(
                                    &send_stream,
                                    relay_lane,
                                    context.mux_limits,
                                ),
                            ) && remotes.promote_path_instance_to_active(instance)
                            {
                                last_stream_progress_at = Instant::now();
                            }
                        }
                        let mut write_error = None;
                        for chunk in outcome.delivered {
                            stats.record_payload_bytes(chunk.len());
                            path_stats
                                .entry(path_key)
                                .or_default()
                                .record_payload_bytes(chunk.len());
                            #[cfg(feature = "lab-diagnostics")]
                            let write_started = Instant::now();
                            if let Err(err) = local.write_all(&chunk).await {
                                write_error = Some(err);
                                break;
                            }
                            #[cfg(feature = "lab-diagnostics")]
                            lab_perf_record("relay.local_write_wait", write_started.elapsed(), chunk.len());
                        }
                        if let Some(err) = write_error {
                            break Err(RuntimeError::Io(err));
                        }
                        #[cfg(feature = "lab-diagnostics")]
                        let flush_started = Instant::now();
                        if let Err(err) = local.flush().await {
                            break Err(RuntimeError::Io(err));
                        }
                        #[cfg(feature = "lab-diagnostics")]
                        lab_perf_record("relay.local_flush_wait", flush_started.elapsed(), 0);
                        if delivered_progress {
                            interactive_response_pending = false;
                        }
                        match send_tcp_recv_progress_remote_set(
                            &mut remotes,
                            context,
                            &recv_stream,
                            &mut recv_progress,
                            false,
                        )
                        .await
                        {
                            Ok(()) => {
                                last_recv_progress_sent_at = Instant::now();
                            }
                            Err(err) if tcp_relay_error_is_migratable(&err) => {
                                match attach_tcp_relay_paths(
                                    context,
                                    &spec,
                                    relay_lane,
                                    &mut remotes,
                                    &send_stream,
                                    !local_open,
                                    TcpRelayAttachMode::Any,
                                )
                            .await
                            {
                                    Ok(attached) if attached > 0 => {
                                        last_stream_progress_at = Instant::now();
                                    }
                                    Ok(_) => break Err(err),
                                    Err(err) => break Err(err),
                                }
                            }
                            Err(err) => break Err(err),
                        }
                        if outcome.fin
                            || pending_stream_fin_ready(&recv_stream, pending_remote_fin_offset)
                        {
                            if let Err(err) = local.shutdown().await {
                                break Err(RuntimeError::Io(err));
                            }
                            remote_open = false;
                            pending_remote_fin_offset = None;
                            interactive_response_pending = false;
                        }
                    }
                    Frame::StreamAck {
                        stream_id: ack_stream_id,
                        ranges,
                    } if ack_stream_id == stream_id => {
                        #[cfg(feature = "lab-diagnostics")]
                        let mux_started = Instant::now();
                        let ack = send_stream.apply_ack(&ranges);
                        #[cfg(feature = "lab-diagnostics")]
                        lab_perf_record("mux.apply_ack", mux_started.elapsed(), ack.released_bytes);
                        for release in path_flights.release_acked_ranges(&ranges) {
                            context.release_relay_path_inflight(
                                release.key.underlay,
                                release.key.index,
                                release.bytes,
                            );
                            #[cfg(feature = "lab-diagnostics")]
                            lab_diagnostic(
                                "path_model",
                                format_args!(
                                    "stream_id={} path_underlay={:?} path_index={} released_bytes={} cause=stream_ack",
                                    stream_id.0,
                                    release.key.underlay,
                                    release.key.index,
                                    release.bytes,
                                ),
                            );
                        }
                        let repair_limit =
                            adaptive_tcp_relay_inflight_bytes(None, relay_lane, context.mux_limits);
                        let repair_frames =
                            send_stream.retransmission_frames_for_ack_gaps(&ranges, repair_limit);
                        let mut repair_error = None;
                        for frame in repair_frames {
                            let sent_frame = frame.clone();
                            let avoid_keys = path_flights.sent_keys_for_frame(&sent_frame);
                            match remotes
                                .send_repair_frame(context, frame, &avoid_keys)
                                .await
                            {
                                Ok(path_key) => {
                                    path_flights.record_frame(path_key, &sent_frame);
                                    #[cfg(feature = "lab-diagnostics")]
                                    lab_diagnostic(
                                        "repair",
                                        format_args!(
                                            "stream_id={} path_underlay={:?} path_index={} cause=ack_gap",
                                            stream_id.0,
                                            path_key.underlay,
                                            path_key.index,
                                        ),
                                    );
                                }
                                Err(err) if tcp_relay_error_is_migratable(&err) => {
                                    match attach_tcp_relay_paths(
                                        context,
                                        &spec,
                                        relay_lane,
                                        &mut remotes,
                                        &send_stream,
                                        !local_open,
                                        TcpRelayAttachMode::Any,
                                    )
                                    .await
                                    {
                                        Ok(attached) if attached > 0 => {}
                                        Ok(_) => {
                                            repair_error = Some(err);
                                            break;
                                        }
                                        Err(err) => {
                                            repair_error = Some(err);
                                            break;
                                        }
                                    }
                                }
                                Err(err) => {
                                    repair_error = Some(err);
                                    break;
                                }
                            }
                        }
                        if let Some(err) = repair_error {
                            break Err(err);
                        }
                        #[cfg(not(feature = "lab-diagnostics"))]
                        let _ = ack;
                        last_stream_progress_at = Instant::now();
                        if pending_local_fin && send_stream.repair_bytes() == 0 {
                            match remotes
                                .send_frame(
                                    context,
                                    Frame::StreamFin {
                                        stream_id,
                                        final_offset: send_stream.next_offset(),
                                    },
                                )
                                .await
                            {
                                Ok(_) => {
                                    pending_local_fin = false;
                                    last_stream_progress_at = Instant::now();
                                }
                                Err(err) if tcp_relay_error_is_migratable(&err) => {
                                    match attach_tcp_relay_paths(
                                        context,
                                        &spec,
                                        relay_lane,
                                        &mut remotes,
                                        &send_stream,
                                        true,
                                        TcpRelayAttachMode::Any,
                                    )
                                    .await
                                    {
                                        Ok(attached) if attached > 0 => {
                                            pending_local_fin = false;
                                            last_stream_progress_at = Instant::now();
                                        }
                                        Ok(_) => break Err(err),
                                        Err(err) => break Err(err),
                                    }
                                }
                                Err(err) => break Err(err),
                            }
                        }
                    }
                    Frame::StreamMaxData {
                        stream_id: max_stream_id,
                        max_offset,
                    } if max_stream_id == stream_id => {
                        send_stream.update_max_offset(max_offset);
                        last_stream_progress_at = Instant::now();
                    }
                    Frame::StreamFin {
                        stream_id: fin_stream_id,
                        final_offset,
                    } if fin_stream_id == stream_id => {
                        last_stream_progress_at = Instant::now();
                        let fin_ready = match receive_stream_fin(
                            &recv_stream,
                            &mut pending_remote_fin_offset,
                            final_offset,
                        ) {
                            Ok(ready) => ready,
                            Err(err) => break Err(err),
                        };
                        if fin_ready {
                            last_delivery_progress_at = Instant::now();
                            if let Err(err) = local.shutdown().await {
                                break Err(RuntimeError::Io(err));
                            }
                            remote_open = false;
                            interactive_response_pending = false;
                            pending_remote_fin_offset = None;
                        }
                    }
                    Frame::PathStatus {
                        status: crate::protocol::PathStatus::Active,
                        ..
                    } => {
                        match remotes
                            .replay_repair_cache_to_instance(
                                instance,
                                &send_stream,
                                pending_local_fin,
                                tcp_relay_attach_payload_bytes(
                                    &send_stream,
                                    relay_lane,
                                    context.mux_limits,
                                )
                                .max(adaptive_tcp_relay_inflight_bytes(
                                    None,
                                    relay_lane,
                                    context.mux_limits,
                                )),
                            )
                            .await
                        {
                            Ok(true) => {
                                last_stream_progress_at = Instant::now();
                                last_response_stall_repair_at = Instant::now();
                            }
                            Ok(false) => {}
                            Err(err) if tcp_relay_error_is_migratable(&err) => {
                                remotes.fail_path_instance(context, instance).await;
                                if remotes.is_empty() {
                                    break Err(err);
                                }
                            }
                            Err(err) => break Err(err),
                        }
                    }
                    Frame::StreamReset {
                        stream_id: reset_stream_id,
                        reason,
                    } if reset_stream_id == stream_id => break Err(RuntimeError::RemoteReset(reason)),
                    unexpected => {
                        log_unexpected_stream_relay_frame(
                            "migrating",
                            stream_id,
                            &unexpected,
                        );
                        break Err(RuntimeError::Protocol("unexpected stream relay frame"));
                    }
                }
            }
            else => break Ok(stats),
        }
    };

    let remaining_paths = remotes
        .paths
        .iter()
        .map(|path| (path.key(), path.stream.lane))
        .collect::<Vec<_>>();
    if result.is_ok() {
        for (key, stats) in path_stats {
            context.mark_relay_path_delivery(key.underlay, key.index, stats);
        }
    }
    if result.is_ok() {
        remotes.close_all().await;
    }
    for release in path_flights.drain_all() {
        context.release_relay_path_inflight(release.key.underlay, release.key.index, release.bytes);
    }
    for (key, lane) in remaining_paths {
        if relay_error_is_tcp_path_failure(&result) {
            context.mark_relay_path_failure(key.underlay, key.index);
        }
        context.release_relay_path_load(key.underlay, key.index, lane);
    }
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_flush("multipath_stream_close");
    result
}

pub(super) async fn switch_tcp_relay_to_best_path(
    context: &ClientPathContext,
    spec: &TcpRelayOpenSpec,
    lane: FlowLane,
    remotes: &mut TcpRelayRemoteSet,
    send_stream: &ReliableSendStream,
    resend_fin: bool,
    mode: TcpRelayAttachMode,
) -> Result<bool, RuntimeError> {
    let attached =
        attach_tcp_relay_paths(context, spec, lane, remotes, send_stream, resend_fin, mode).await?;
    if attached == 0 {
        return Ok(false);
    }
    Ok(true)
}

pub(super) fn tcp_relay_frame_prefers_current_data_path(frame: &Frame, lane: FlowLane) -> bool {
    matches!(frame, Frame::StreamFin { .. })
        || (matches!(frame, Frame::StreamData { .. }) && !relay_lane_is_bulk(lane))
}

pub(super) struct RelayPathAttachRequest<'a> {
    spec: &'a TcpRelayOpenSpec,
    lane: FlowLane,
    send_stream: &'a ReliableSendStream,
    resend_fin: bool,
    candidates: Vec<RelayPathKey>,
    race_repair: bool,
    allow_mixed_carrier: bool,
    replay_repair_cache: bool,
    attach_all_candidates: bool,
}

pub(super) async fn attach_relay_path_candidates(
    context: &ClientPathContext,
    remotes: &mut TcpRelayRemoteSet,
    request: RelayPathAttachRequest<'_>,
) -> Result<usize, RuntimeError> {
    let stream_id = remotes.stream_id();
    let mut last_retryable_error = None;
    let mut attached = 0usize;
    let active_underlay = remotes.active_carrier_underlay();
    let candidates = if request.allow_mixed_carrier {
        request.candidates
    } else {
        relay_path_candidates_for_active_carrier(request.candidates, active_underlay)
    };

    for key in candidates {
        if remotes.contains_path_key(key) {
            continue;
        }
        match open_remote_stream_for_relay_path(
            context,
            stream_id,
            request.spec.target.clone(),
            request.spec.ingress,
            request.lane,
            key,
            if request.race_repair {
                StreamOpenRole::Repair
            } else {
                StreamOpenRole::Active
            },
        )
        .await
        {
            Ok(opened) => {
                let replay_result = if request.replay_repair_cache {
                    let repair_limit = tcp_relay_attach_payload_bytes(
                        request.send_stream,
                        request.lane,
                        context.mux_limits,
                    )
                    .max(adaptive_tcp_relay_inflight_bytes(
                        None,
                        request.lane,
                        context.mux_limits,
                    ));
                    replay_tcp_repair_flight(
                        &opened.stream,
                        request.send_stream,
                        request.resend_fin,
                        repair_limit,
                    )
                    .await
                } else {
                    Ok(())
                };
                match replay_result {
                    Ok(()) => {
                        if request.race_repair {
                            remotes.attach_for_repair(opened);
                        } else {
                            remotes.attach(opened);
                        }
                        attached += 1;
                        if !request.race_repair && !request.attach_all_candidates {
                            return Ok(attached);
                        }
                    }
                    Err(err) if tcp_relay_error_is_migratable(&err) => {
                        context.mark_relay_path_failure(key.underlay, key.index);
                        context.release_relay_path_load(key.underlay, key.index, request.lane);
                        last_retryable_error = Some(err);
                    }
                    Err(err) => {
                        context.release_relay_path_load(key.underlay, key.index, request.lane);
                        return Err(err);
                    }
                }
            }
            Err(err) if relay_path_open_error_is_retryable(key.underlay, &err) => {
                context.mark_relay_path_failure(key.underlay, key.index);
                last_retryable_error = Some(err);
            }
            Err(err) => return Err(err),
        }
    }
    if attached > 0 {
        Ok(attached)
    } else if remotes.is_empty() {
        Err(last_retryable_error.unwrap_or_else(|| no_schedulable_reliable_path_error(context)))
    } else {
        Ok(0)
    }
}

pub(super) async fn open_remote_stream_for_relay_path(
    context: &ClientPathContext,
    stream_id: StreamId,
    target: TargetAddr,
    ingress: IngressKind,
    lane: FlowLane,
    key: RelayPathKey,
    role: StreamOpenRole,
) -> Result<OpenedRemoteStream, RuntimeError> {
    match key.underlay {
        UnderlayProtocol::Tcp => {
            open_remote_stream_on_path(context, stream_id, target, ingress, lane, key.index, role)
                .await
        }
        UnderlayProtocol::Udp => {
            open_remote_stream_on_udp_path(
                context,
                stream_id,
                target,
                ingress,
                lane,
                key.index,
                UdpStreamOpenOptions {
                    wait_for_accept: false,
                    role,
                },
            )
            .await
        }
    }
}

pub(super) fn relay_path_open_error_is_retryable(
    underlay: UnderlayProtocol,
    err: &RuntimeError,
) -> bool {
    match underlay {
        UnderlayProtocol::Tcp => stream_open_error_is_path_retryable(err),
        UnderlayProtocol::Udp => udp_stream_open_error_is_path_retryable(err),
    }
}

pub(super) fn no_schedulable_reliable_path_error(context: &ClientPathContext) -> RuntimeError {
    if !context.tcp_paths.is_empty() {
        RuntimeError::NoSchedulableTcpPath
    } else {
        RuntimeError::NoSchedulableUdpPath
    }
}

pub(super) async fn attach_tcp_relay_paths(
    context: &ClientPathContext,
    spec: &TcpRelayOpenSpec,
    lane: FlowLane,
    remotes: &mut TcpRelayRemoteSet,
    send_stream: &ReliableSendStream,
    resend_fin: bool,
    mode: TcpRelayAttachMode,
) -> Result<usize, RuntimeError> {
    let payload_bytes = match mode {
        TcpRelayAttachMode::Any => {
            tcp_relay_attach_payload_bytes(send_stream, lane, context.mux_limits)
        }
        TcpRelayAttachMode::BulkStriping => {
            tcp_relay_bulk_striping_payload_bytes(send_stream, context.mux_limits)
        }
    };
    if matches!(mode, TcpRelayAttachMode::BulkStriping) {
        let candidates = context.ordered_reliable_bulk_striping_path_keys(payload_bytes);
        return attach_relay_path_candidates(
            context,
            remotes,
            RelayPathAttachRequest {
                spec,
                lane,
                send_stream,
                resend_fin,
                candidates,
                race_repair: false,
                allow_mixed_carrier: true,
                replay_repair_cache: false,
                attach_all_candidates: true,
            },
        )
        .await;
    }
    if context.tcp_paths.is_empty() {
        return attach_udp_relay_paths(context, spec, lane, remotes, send_stream, resend_fin, mode)
            .await;
    }
    if remotes.active_carrier_underlay() == Some(UnderlayProtocol::Udp) {
        return attach_udp_relay_paths(context, spec, lane, remotes, send_stream, resend_fin, mode)
            .await;
    }
    let candidates = context.ordered_tcp_repair_path_indices(
        remotes.active_path_index_for(UnderlayProtocol::Tcp),
        lane,
        payload_bytes,
    );
    let race_repair = tcp_relay_should_race_repair(lane, send_stream, resend_fin, mode);
    let attached = attach_relay_path_candidates(
        context,
        remotes,
        RelayPathAttachRequest {
            spec,
            lane,
            send_stream,
            resend_fin,
            candidates: candidates
                .into_iter()
                .map(|index| RelayPathKey {
                    underlay: UnderlayProtocol::Tcp,
                    index,
                })
                .collect(),
            race_repair,
            allow_mixed_carrier: false,
            replay_repair_cache: true,
            attach_all_candidates: false,
        },
    )
    .await?;
    if attached > 0 {
        return Ok(attached);
    }
    if !context.udp_paths.is_empty() && remotes.is_empty() {
        return attach_udp_relay_paths(context, spec, lane, remotes, send_stream, resend_fin, mode)
            .await;
    }
    Ok(0)
}

pub(super) async fn attach_udp_relay_paths(
    context: &ClientPathContext,
    spec: &TcpRelayOpenSpec,
    lane: FlowLane,
    remotes: &mut TcpRelayRemoteSet,
    send_stream: &ReliableSendStream,
    resend_fin: bool,
    mode: TcpRelayAttachMode,
) -> Result<usize, RuntimeError> {
    if remotes.active_carrier_underlay() == Some(UnderlayProtocol::Tcp) {
        return Ok(0);
    }
    let stream_id = remotes.stream_id();
    let payload_bytes = match mode {
        TcpRelayAttachMode::Any => {
            tcp_relay_attach_payload_bytes(send_stream, lane, context.mux_limits)
        }
        TcpRelayAttachMode::BulkStriping => {
            tcp_relay_bulk_striping_payload_bytes(send_stream, context.mux_limits)
        }
    };
    let mut candidates = match mode {
        TcpRelayAttachMode::Any => {
            let require_delivery_evidence =
                matches!(lane, FlowLane::Throughput | FlowLane::Background) && !remotes.is_empty();
            context.ordered_udp_stream_repair_path_indices(
                remotes.active_path_index_for(UnderlayProtocol::Udp),
                lane,
                payload_bytes,
                require_delivery_evidence,
            )
        }
        TcpRelayAttachMode::BulkStriping => context
            .ordered_reliable_bulk_striping_path_keys(payload_bytes)
            .into_iter()
            .filter_map(|key| (key.underlay == UnderlayProtocol::Udp).then_some(key.index))
            .collect(),
    };
    if candidates.is_empty() && remotes.is_empty() {
        candidates = (0..context.udp_paths.len()).collect();
    }
    if matches!(mode, TcpRelayAttachMode::BulkStriping) {
        candidates.retain(|path_index| {
            !remotes.contains_path_key(RelayPathKey {
                underlay: UnderlayProtocol::Udp,
                index: *path_index,
            })
        });
    }
    let race_repair = tcp_relay_should_race_repair(lane, send_stream, resend_fin, mode);
    let mut last_retryable_error = None;
    let mut attached = 0usize;

    for path_index in candidates {
        let key = RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: path_index,
        };
        if remotes.contains_path_key(key) {
            continue;
        }
        match open_remote_stream_on_udp_path(
            context,
            stream_id,
            spec.target.clone(),
            spec.ingress,
            lane,
            path_index,
            UdpStreamOpenOptions {
                wait_for_accept: false,
                role: if race_repair {
                    StreamOpenRole::Repair
                } else {
                    StreamOpenRole::Active
                },
            },
        )
        .await
        {
            Ok(opened) => {
                let repair_limit =
                    tcp_relay_attach_payload_bytes(send_stream, lane, context.mux_limits).max(
                        adaptive_tcp_relay_inflight_bytes(None, lane, context.mux_limits),
                    );
                match replay_tcp_repair_flight(
                    &opened.stream,
                    send_stream,
                    resend_fin,
                    repair_limit,
                )
                .await
                {
                    Ok(()) => {
                        if race_repair {
                            remotes.attach_for_repair(opened);
                        } else {
                            remotes.attach(opened);
                        }
                        attached += 1;
                        if !race_repair {
                            return Ok(attached);
                        }
                    }
                    Err(err) if tcp_relay_error_is_migratable(&err) => {
                        context.mark_udp_path_failure(path_index);
                        context.release_udp_stream_path_load(path_index, lane);
                        last_retryable_error = Some(err);
                    }
                    Err(err) => {
                        context.release_udp_stream_path_load(path_index, lane);
                        return Err(err);
                    }
                }
            }
            Err(err) if udp_stream_open_error_is_path_retryable(&err) => {
                context.mark_udp_path_failure(path_index);
                last_retryable_error = Some(err);
            }
            Err(err) => return Err(err),
        }
    }
    if attached > 0 {
        Ok(attached)
    } else if remotes.is_empty() {
        Err(last_retryable_error.unwrap_or(RuntimeError::NoSchedulableUdpPath))
    } else {
        Ok(0)
    }
}

pub(super) fn relay_path_candidates_for_active_carrier(
    candidates: Vec<RelayPathKey>,
    active_underlay: Option<UnderlayProtocol>,
) -> Vec<RelayPathKey> {
    let Some(active_underlay) = active_underlay else {
        return candidates;
    };
    candidates
        .into_iter()
        .filter(|candidate| candidate.underlay == active_underlay)
        .collect()
}

pub(super) fn tcp_relay_should_race_repair(
    lane: FlowLane,
    send_stream: &ReliableSendStream,
    resend_fin: bool,
    mode: TcpRelayAttachMode,
) -> bool {
    matches!(mode, TcpRelayAttachMode::Any)
        && !resend_fin
        && tcp_relay_expects_interactive_response(lane)
        && send_stream.repair_bytes() <= PATH_OPEN_SCORE_BYTES
}

pub(super) fn tcp_relay_attach_payload_bytes(
    send_stream: &ReliableSendStream,
    lane: FlowLane,
    mux_limits: MuxLimits,
) -> usize {
    let floor = if tcp_relay_expects_interactive_response(lane) {
        PATH_OPEN_SCORE_BYTES
    } else {
        tcp_relay_buffer_len(mux_limits)
    };
    let repair_bytes = send_stream.repair_bytes().max(floor);
    let stream_window = usize::try_from(mux_limits.max_stream_window_bytes).unwrap_or(usize::MAX);
    repair_bytes.min(stream_window)
}

pub(super) fn tcp_relay_bulk_striping_payload_bytes(
    send_stream: &ReliableSendStream,
    mux_limits: MuxLimits,
) -> usize {
    let attach_payload =
        tcp_relay_attach_payload_bytes(send_stream, FlowLane::Throughput, mux_limits);
    let stream_window = usize::try_from(mux_limits.max_stream_window_bytes).unwrap_or(usize::MAX);
    attach_payload.max(mux_limits.max_tcp_path_inflight_bytes.min(stream_window))
}

pub(super) fn tcp_relay_stall_watch_active(
    send_stream: &ReliableSendStream,
    recv_stream: &ReliableRecvStream,
    remote_open: bool,
    lane: FlowLane,
    interactive_response_pending: bool,
    mux_limits: MuxLimits,
) -> bool {
    send_stream.repair_bytes() > 0
        || (remote_open
            && interactive_response_pending
            && tcp_relay_expects_interactive_response(lane))
        || tcp_relay_response_stall_watch_active(recv_stream, remote_open, lane, mux_limits)
}

pub(super) fn tcp_relay_response_stall_watch_active(
    recv_stream: &ReliableRecvStream,
    remote_open: bool,
    lane: FlowLane,
    mux_limits: MuxLimits,
) -> bool {
    remote_open
        && recv_stream.next_offset() > 0
        && (matches!(lane, FlowLane::Throughput | FlowLane::Background)
            || recv_stream.next_offset() >= tcp_relay_response_stall_watch_bytes(mux_limits))
}

pub(super) fn tcp_relay_stall_progress_anchor(
    last_stream_progress_at: Instant,
    last_delivery_progress_at: Instant,
    last_response_stall_repair_at: Instant,
    recv_stream: &ReliableRecvStream,
    remote_open: bool,
    lane: FlowLane,
    mux_limits: MuxLimits,
) -> Instant {
    if tcp_relay_response_stall_watch_active(recv_stream, remote_open, lane, mux_limits) {
        last_delivery_progress_at.max(last_response_stall_repair_at)
    } else {
        last_stream_progress_at
    }
}

pub(super) fn tcp_relay_receive_hole_repair_active(
    recv_stream: &ReliableRecvStream,
    remote_open: bool,
) -> bool {
    remote_open && recv_stream.next_offset() > 0 && recv_stream.reorder_bytes() > 0
}

pub(super) fn tcp_relay_receive_hole_repair_deadline(
    last_delivery_progress_at: Instant,
    last_receive_hole_repair_at: Instant,
    path: Option<PathSnapshot>,
    lane: FlowLane,
) -> tokio::time::Instant {
    let anchor = if last_delivery_progress_at > last_receive_hole_repair_at {
        last_delivery_progress_at
    } else {
        last_receive_hole_repair_at
    };
    tokio::time::Instant::from_std(anchor + tcp_relay_stall_timeout(path, lane))
}

pub(super) fn tcp_relay_receive_hole_failure_attempts(_lane: FlowLane) -> u32 {
    1
}

pub(super) fn tcp_relay_sole_survivor_reannounce_attempts(stall_timeout: Duration) -> u32 {
    const FLUENT_REPAIR_BUDGET: Duration = Duration::from_millis(4500);
    let timeout = stall_timeout.max(TCP_STREAM_STALL_MIN_TIMEOUT);
    (FLUENT_REPAIR_BUDGET.as_secs_f64() / timeout.as_secs_f64())
        .floor()
        .clamp(1.0, 16.0) as u32
}

pub(super) fn tcp_relay_refresh_path_tracking(
    path_last_delivery_at: &mut HashMap<RelayPathKey, Instant>,
    path_keys: &[RelayPathKey],
    now: Instant,
) {
    let live_paths = path_keys.iter().copied().collect::<HashSet<_>>();
    path_last_delivery_at.retain(|path_key, _| live_paths.contains(path_key));
    for path_key in path_keys {
        path_last_delivery_at.entry(*path_key).or_insert(now);
    }
}

pub(super) fn tcp_relay_receive_hole_victim(
    context: &ClientPathContext,
    path_keys: &[RelayPathKey],
    lane: FlowLane,
    payload_bytes: usize,
    path_last_delivery_at: &HashMap<RelayPathKey, Instant>,
) -> Option<RelayPathKey> {
    if path_keys.len() <= 1 {
        return None;
    }
    path_keys.iter().copied().max_by(|left, right| {
        let left_score = tcp_relay_receive_hole_victim_score(context, *left, lane, payload_bytes);
        let right_score = tcp_relay_receive_hole_victim_score(context, *right, lane, payload_bytes);
        left_score
            .total_cmp(&right_score)
            .then_with(|| tcp_relay_stale_delivery_order(*left, *right, path_last_delivery_at))
    })
}

pub(super) fn tcp_relay_receive_hole_victim_score(
    context: &ClientPathContext,
    key: RelayPathKey,
    lane: FlowLane,
    payload_bytes: usize,
) -> f64 {
    tcp_relay_path_eta_ms(context, key, lane, payload_bytes).unwrap_or(f64::INFINITY)
}

pub(super) fn tcp_relay_delivery_path_should_become_active(
    context: &ClientPathContext,
    current: Option<RelayPathKey>,
    delivered: RelayPathKey,
    lane: FlowLane,
    payload_bytes: usize,
) -> bool {
    if current == Some(delivered) {
        return false;
    }
    let Some(delivered_eta) = tcp_relay_path_eta_ms(context, delivered, lane, payload_bytes) else {
        return false;
    };
    let current_eta = current
        .and_then(|key| tcp_relay_path_eta_ms(context, key, lane, payload_bytes))
        .unwrap_or(f64::INFINITY);
    delivered_eta < current_eta
}

pub(super) fn tcp_relay_path_eta_ms(
    context: &ClientPathContext,
    key: RelayPathKey,
    lane: FlowLane,
    payload_bytes: usize,
) -> Option<f64> {
    relay_path_snapshot(context, key).and_then(|snapshot| {
        scheduler::score_path(snapshot, lane, payload_bytes, SchedulerPolicy::default())
            .map(|score| score.eta_ms)
    })
}

pub(super) fn tcp_relay_stale_delivery_order(
    left: RelayPathKey,
    right: RelayPathKey,
    path_last_delivery_at: &HashMap<RelayPathKey, Instant>,
) -> std::cmp::Ordering {
    match (
        path_last_delivery_at.get(&left),
        path_last_delivery_at.get(&right),
    ) {
        (Some(left_seen), Some(right_seen)) => right_seen
            .cmp(left_seen)
            .then_with(|| relay_path_key_order(right, left)),
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, None) => relay_path_key_order(right, left),
    }
}

pub(super) fn relay_path_snapshot(
    context: &ClientPathContext,
    key: RelayPathKey,
) -> Option<PathSnapshot> {
    match key.underlay {
        UnderlayProtocol::Tcp => context.tcp_path_snapshot(key.index),
        UnderlayProtocol::Udp => context.udp_path_snapshot(key.index),
    }
}

pub(super) fn relay_path_key_order(left: RelayPathKey, right: RelayPathKey) -> std::cmp::Ordering {
    relay_underlay_order(left.underlay)
        .cmp(&relay_underlay_order(right.underlay))
        .then_with(|| left.index.cmp(&right.index))
}

pub(super) fn relay_underlay_order(underlay: UnderlayProtocol) -> u8 {
    match underlay {
        UnderlayProtocol::Tcp => 0,
        UnderlayProtocol::Udp => 1,
    }
}

pub(super) fn tcp_relay_expects_interactive_response(lane: FlowLane) -> bool {
    matches!(
        lane,
        FlowLane::Control | FlowLane::Latency | FlowLane::RealtimeDatagram
    )
}

pub(super) fn tcp_relay_response_stall_watch_bytes(mux_limits: MuxLimits) -> u64 {
    (tcp_relay_buffer_len(mux_limits) as u64).min(mux_limits.max_stream_window_bytes)
}

pub(super) fn tcp_relay_stall_deadline(
    last_progress_at: Instant,
    path: Option<PathSnapshot>,
    lane: FlowLane,
) -> tokio::time::Instant {
    tokio::time::Instant::from_std(last_progress_at + tcp_relay_stall_timeout(path, lane))
}

pub(super) fn tcp_relay_stall_timeout(path: Option<PathSnapshot>, lane: FlowLane) -> Duration {
    let (srtt_ms, jitter_ms) = path.map_or((250.0, 50.0), |path| {
        (path.srtt_ms.max(1.0), path.jitter_ms.max(0.0))
    });
    let rtt_gain = match lane {
        FlowLane::Control | FlowLane::RealtimeDatagram => 1.5,
        FlowLane::Latency => 2.0,
        FlowLane::Throughput => 1.5,
        FlowLane::Background => 3.0,
    };
    Duration::from_secs_f64(
        ((srtt_ms * rtt_gain + jitter_ms * 4.0 + 100.0) / 1000.0).clamp(
            TCP_STREAM_STALL_MIN_TIMEOUT.as_secs_f64(),
            TCP_STREAM_STALL_MAX_TIMEOUT.as_secs_f64(),
        ),
    )
}
