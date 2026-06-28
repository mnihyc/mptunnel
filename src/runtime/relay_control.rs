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
pub(super) struct TcpRelayFlowClassifier {
    current: TrafficClass,
    rebalance_attempted: bool,
}

impl TcpRelayFlowClassifier {
    pub(super) fn new() -> Self {
        Self {
            current: TrafficClass::Interactive,
            rebalance_attempted: false,
        }
    }

    pub(super) fn refresh(
        &mut self,
        signals: TcpRelayFlowSignals,
        path: Option<PathSnapshot>,
        mux_limits: MuxLimits,
    ) -> TcpRelayFlowDecision {
        let previous = self.current;
        self.current =
            if signals.observed_bytes() >= tcp_auto_bulk_threshold_bytes(path, mux_limits) {
                TrafficClass::Bulk
            } else {
                TrafficClass::Interactive
            };
        TcpRelayFlowDecision {
            class: self.current,
            previous_class: previous,
            promoted_to_bulk: previous != TrafficClass::Bulk && self.current == TrafficClass::Bulk,
        }
    }

    pub(super) fn should_rebalance(self, update: TcpRelayFlowDecision) -> bool {
        update.promoted_to_bulk && !self.rebalance_attempted
    }

    pub(super) fn mark_rebalance_attempted(&mut self) {
        self.rebalance_attempted = true;
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct TcpRelayFlowDecision {
    pub(super) class: TrafficClass,
    pub(super) previous_class: TrafficClass,
    pub(super) promoted_to_bulk: bool,
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
    let chunk_size =
        adaptive_tcp_relay_chunk_bytes(None, TrafficClass::Interactive, context.mux_limits);
    let mut buf = vec![0u8; chunk_size];
    let mut local_open = true;
    let mut remote_open = true;
    let mut pending_local_fin = false;
    let mut pending_remote_fin_offset = None;
    let mut stats = PathDeliveryStats::default();
    let mut path_stats = HashMap::<RelayPathKey, PathDeliveryStats>::new();
    let mut flow_classifier = TcpRelayFlowClassifier::new();
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
    let mut last_reported_budget: Option<(TrafficClass, usize, usize)> = None;

    let result = loop {
        if !local_open && !remote_open && send_stream.repair_bytes() == 0 {
            break Ok(stats);
        }
        let path_snapshot = remotes
            .primary_path_key()
            .and_then(|key| relay_path_snapshot(context, key));
        let class_update = flow_classifier.refresh(
            TcpRelayFlowSignals::new(
                send_stream.next_offset(),
                recv_stream.next_offset(),
                send_stream.repair_bytes(),
            ),
            path_snapshot,
            context.mux_limits,
        );
        let relay_class = class_update.class;
        if class_update.promoted_to_bulk {
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "client_stream_class_promoted",
                format_args!(
                    "stream_id={} previous={:?} class={:?} sent_offset={} received_offset={} repair_bytes={}",
                    stream_id.0,
                    class_update.previous_class,
                    relay_class,
                    send_stream.next_offset(),
                    recv_stream.next_offset(),
                    send_stream.repair_bytes(),
                ),
            );
            for key in remotes.path_keys() {
                context.reclassify_relay_path_load(
                    key.underlay,
                    key.index,
                    class_update.previous_class,
                    relay_class,
                );
            }
            remotes.set_class(relay_class);
        }
        if flow_classifier.should_rebalance(class_update) {
            flow_classifier.mark_rebalance_attempted();
            if let Err(err) = switch_tcp_relay_to_best_path(
                context,
                &spec,
                relay_class,
                &mut remotes,
                &send_stream,
                !local_open,
                TcpRelayAttachMode::AutoBulkDiscovery,
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
            adaptive_tcp_relay_chunk_bytes(path_snapshot, relay_class, context.mux_limits)
                .min(remotes.max_frame_payload_bytes(context.mux_limits))
                .max(1);
        resize_tcp_relay_buffer(&mut buf, adaptive_chunk);
        let adaptive_inflight =
            adaptive_tcp_relay_inflight_bytes(path_snapshot, relay_class, context.mux_limits);
        #[cfg(feature = "lab-diagnostics")]
        if last_reported_budget != Some((relay_class, adaptive_chunk, adaptive_inflight)) {
            lab_diagnostic(
                "client_relay_budget",
                format_args!(
                    "stream_id={} class={:?} chunk_bytes={} inflight_bytes={} path_snapshot={}",
                    stream_id.0,
                    relay_class,
                    adaptive_chunk,
                    adaptive_inflight,
                    path_snapshot.is_some(),
                ),
            );
            last_reported_budget = Some((relay_class, adaptive_chunk, adaptive_inflight));
        }
        let stall_watch_active = tcp_relay_stall_watch_active(
            &send_stream,
            &recv_stream,
            remote_open,
            relay_class,
            interactive_response_pending,
            context.mux_limits,
        );
        let stall_progress_anchor = tcp_relay_stall_progress_anchor(
            last_stream_progress_at,
            last_delivery_progress_at,
            last_response_stall_repair_at,
            &recv_stream,
            remote_open,
            relay_class,
            context.mux_limits,
        );
        let receive_hole_repair_active =
            tcp_relay_receive_hole_repair_active(&recv_stream, remote_open);
        let receive_hole_repair_deadline = tcp_relay_receive_hole_repair_deadline(
            last_delivery_progress_at,
            last_receive_hole_repair_at,
            path_snapshot,
            relay_class,
        );
        let stall_deadline =
            tcp_relay_stall_deadline(stall_progress_anchor, path_snapshot, relay_class);
        let recv_progress_deadline = tokio::time::Instant::from_std(
            last_recv_progress_sent_at
                + reliable_stream_recv_progress_interval(path_snapshot, relay_class),
        );

        tokio::select! {
            _ = tokio::time::sleep_until(receive_hole_repair_deadline), if receive_hole_repair_active => {
                match attach_tcp_relay_paths(
                    context,
                    &spec,
                    relay_class,
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
                        if receive_hole_repair_attempts >= tcp_relay_receive_hole_failure_attempts(relay_class) {
                            let path_keys = remotes.path_keys();
                            if let Some(path_key) = tcp_relay_receive_hole_victim(
                                context,
                                &path_keys,
                                relay_class,
                                recv_stream.reorder_bytes().max(1),
                                &path_last_delivery_at,
                            ) && remotes.fail_path_key(context, path_key).await
                            {
                                path_last_delivery_at.remove(&path_key);
                                if !remotes.is_empty()
                                    && let Err(err) = remotes
                                        .reannounce_active_path(context, &spec, relay_class)
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
                                    .reannounce_active_path(context, &spec, relay_class)
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
                        tcp_relay_stall_timeout(path_snapshot, relay_class),
                    );
                    if response_stall_reannounce_attempts
                        < reannounce_budget
                    {
                        response_stall_reannounce_attempts =
                            response_stall_reannounce_attempts.saturating_add(1);
                        match remotes
                            .reannounce_active_path(context, &spec, relay_class)
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
                        .reannounce_active_path(context, &spec, relay_class)
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
                    relay_class,
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
                            relay_class,
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
                                    relay_class,
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
                    if tcp_relay_expects_interactive_response(relay_class) && remote_open {
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
                    match remotes.send_frame(context, frame).await {
                        Ok(path_key) => {
                            last_stream_progress_at = Instant::now();
                            stats.record_payload_bytes(read);
                            path_stats
                                .entry(path_key)
                                .or_default()
                                .record_payload_bytes(read);
                        }
                        Err(err) if tcp_relay_error_is_migratable(&err) => {
                            match attach_tcp_relay_paths(
                                context,
                                &spec,
                                relay_class,
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
                            relay_class,
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
                                .reannounce_active_path(context, &spec, relay_class)
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
                                relay_class,
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
                                relay_class,
                                tcp_relay_attach_payload_bytes(
                                    &send_stream,
                                    relay_class,
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
                                    relay_class,
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
                                        relay_class,
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
        .map(|path| (path.key(), path.stream.class))
        .collect::<Vec<_>>();
    if result.is_ok() {
        for (key, stats) in path_stats {
            context.mark_relay_path_delivery(key.underlay, key.index, stats);
        }
    }
    if result.is_ok() {
        remotes.close_all().await;
    }
    for (key, class) in remaining_paths {
        if relay_error_is_tcp_path_failure(&result) {
            context.mark_relay_path_failure(key.underlay, key.index);
        }
        context.release_relay_path_load(key.underlay, key.index, class);
    }
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_flush("multipath_stream_close");
    result
}

pub(super) async fn switch_tcp_relay_to_best_path(
    context: &ClientPathContext,
    spec: &TcpRelayOpenSpec,
    class: TrafficClass,
    remotes: &mut TcpRelayRemoteSet,
    send_stream: &ReliableSendStream,
    resend_fin: bool,
    mode: TcpRelayAttachMode,
) -> Result<bool, RuntimeError> {
    let attached =
        attach_tcp_relay_paths(context, spec, class, remotes, send_stream, resend_fin, mode)
            .await?;
    if attached == 0 {
        return Ok(false);
    }
    Ok(true)
}

pub(super) fn tcp_relay_frame_prefers_current_data_path(frame: &Frame) -> bool {
    matches!(frame, Frame::StreamData { .. } | Frame::StreamFin { .. })
}

pub(super) struct RelayPathAttachRequest<'a> {
    spec: &'a TcpRelayOpenSpec,
    class: TrafficClass,
    send_stream: &'a ReliableSendStream,
    resend_fin: bool,
    candidates: Vec<RelayPathKey>,
    race_repair: bool,
    allow_mixed_carrier: bool,
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
            request.class,
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
                match replay_tcp_repair_cache(
                    &opened.stream,
                    request.send_stream,
                    request.resend_fin,
                )
                .await
                {
                    Ok(()) => {
                        if request.race_repair {
                            remotes.attach_for_repair(opened);
                        } else {
                            remotes.attach(opened);
                        }
                        attached += 1;
                        if !request.race_repair {
                            return Ok(attached);
                        }
                    }
                    Err(err) if tcp_relay_error_is_migratable(&err) => {
                        context.mark_relay_path_failure(key.underlay, key.index);
                        context.release_relay_path_load(key.underlay, key.index, request.class);
                        last_retryable_error = Some(err);
                    }
                    Err(err) => {
                        context.release_relay_path_load(key.underlay, key.index, request.class);
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
    class: TrafficClass,
    key: RelayPathKey,
    role: StreamOpenRole,
) -> Result<OpenedRemoteStream, RuntimeError> {
    match key.underlay {
        UnderlayProtocol::Tcp => {
            open_remote_stream_on_path(context, stream_id, target, ingress, class, key.index, role)
                .await
        }
        UnderlayProtocol::Udp => {
            open_remote_stream_on_udp_path(
                context,
                stream_id,
                target,
                ingress,
                class,
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
    class: TrafficClass,
    remotes: &mut TcpRelayRemoteSet,
    send_stream: &ReliableSendStream,
    resend_fin: bool,
    mode: TcpRelayAttachMode,
) -> Result<usize, RuntimeError> {
    let payload_bytes = match mode {
        TcpRelayAttachMode::Any => {
            tcp_relay_attach_payload_bytes(send_stream, class, context.mux_limits)
        }
        TcpRelayAttachMode::AutoBulkDiscovery => {
            tcp_relay_auto_bulk_discovery_payload_bytes(send_stream, context.mux_limits)
        }
    };
    if matches!(mode, TcpRelayAttachMode::AutoBulkDiscovery) {
        let candidates = context.ordered_reliable_auto_bulk_discovery_path_keys(
            remotes.active_path_index_for(UnderlayProtocol::Tcp),
            remotes.active_path_index_for(UnderlayProtocol::Udp),
            payload_bytes,
        );
        return attach_relay_path_candidates(
            context,
            remotes,
            RelayPathAttachRequest {
                spec,
                class,
                send_stream,
                resend_fin,
                candidates,
                race_repair: false,
                allow_mixed_carrier: true,
            },
        )
        .await;
    }
    if context.tcp_paths.is_empty() {
        return attach_udp_relay_paths(
            context,
            spec,
            class,
            remotes,
            send_stream,
            resend_fin,
            mode,
        )
        .await;
    }
    if remotes.active_carrier_underlay() == Some(UnderlayProtocol::Udp) {
        return attach_udp_relay_paths(
            context,
            spec,
            class,
            remotes,
            send_stream,
            resend_fin,
            mode,
        )
        .await;
    }
    let candidates = context.ordered_tcp_repair_path_indices(
        remotes.active_path_index_for(UnderlayProtocol::Tcp),
        class,
        payload_bytes,
    );
    let race_repair = tcp_relay_should_race_repair(class, send_stream, resend_fin, mode);
    let attached = attach_relay_path_candidates(
        context,
        remotes,
        RelayPathAttachRequest {
            spec,
            class,
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
        },
    )
    .await?;
    if attached > 0 {
        return Ok(attached);
    }
    if !context.udp_paths.is_empty() && remotes.is_empty() {
        return attach_udp_relay_paths(
            context,
            spec,
            class,
            remotes,
            send_stream,
            resend_fin,
            mode,
        )
        .await;
    }
    Ok(0)
}

pub(super) async fn attach_udp_relay_paths(
    context: &ClientPathContext,
    spec: &TcpRelayOpenSpec,
    class: TrafficClass,
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
            tcp_relay_attach_payload_bytes(send_stream, class, context.mux_limits)
        }
        TcpRelayAttachMode::AutoBulkDiscovery => {
            tcp_relay_auto_bulk_discovery_payload_bytes(send_stream, context.mux_limits)
        }
    };
    let mut candidates = match mode {
        TcpRelayAttachMode::Any => {
            let require_delivery_evidence =
                matches!(class, TrafficClass::Bulk | TrafficClass::Background)
                    && !remotes.is_empty();
            context.ordered_udp_stream_repair_path_indices(
                remotes.active_path_index_for(UnderlayProtocol::Udp),
                class,
                payload_bytes,
                require_delivery_evidence,
            )
        }
        TcpRelayAttachMode::AutoBulkDiscovery => context
            .ordered_udp_stream_auto_bulk_discovery_indices(
                remotes.active_path_index_for(UnderlayProtocol::Udp),
                payload_bytes,
            ),
    };
    if candidates.is_empty() && remotes.is_empty() {
        candidates = (0..context.udp_paths.len()).collect();
    }
    if matches!(mode, TcpRelayAttachMode::AutoBulkDiscovery) {
        candidates.retain(|path_index| {
            !remotes.contains_path_key(RelayPathKey {
                underlay: UnderlayProtocol::Udp,
                index: *path_index,
            })
        });
    }
    let race_repair = tcp_relay_should_race_repair(class, send_stream, resend_fin, mode);
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
            class,
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
                match replay_tcp_repair_cache(&opened.stream, send_stream, resend_fin).await {
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
                        context.release_udp_stream_path_load(path_index, class);
                        last_retryable_error = Some(err);
                    }
                    Err(err) => {
                        context.release_udp_stream_path_load(path_index, class);
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
    class: TrafficClass,
    send_stream: &ReliableSendStream,
    resend_fin: bool,
    mode: TcpRelayAttachMode,
) -> bool {
    matches!(mode, TcpRelayAttachMode::Any)
        && !resend_fin
        && tcp_relay_expects_interactive_response(class)
        && send_stream.repair_bytes() <= PATH_OPEN_SCORE_BYTES
}

pub(super) fn tcp_relay_attach_payload_bytes(
    send_stream: &ReliableSendStream,
    class: TrafficClass,
    mux_limits: MuxLimits,
) -> usize {
    let floor = if tcp_relay_expects_interactive_response(class) {
        PATH_OPEN_SCORE_BYTES
    } else {
        tcp_relay_buffer_len(mux_limits)
    };
    let repair_bytes = send_stream.repair_bytes().max(floor);
    let stream_window = usize::try_from(mux_limits.max_stream_window_bytes).unwrap_or(usize::MAX);
    repair_bytes.min(stream_window)
}

pub(super) fn tcp_relay_auto_bulk_discovery_payload_bytes(
    send_stream: &ReliableSendStream,
    mux_limits: MuxLimits,
) -> usize {
    let attach_payload =
        tcp_relay_attach_payload_bytes(send_stream, TrafficClass::Bulk, mux_limits);
    let stream_window = usize::try_from(mux_limits.max_stream_window_bytes).unwrap_or(usize::MAX);
    attach_payload.max(mux_limits.max_tcp_path_inflight_bytes.min(stream_window))
}

pub(super) fn tcp_relay_stall_watch_active(
    send_stream: &ReliableSendStream,
    recv_stream: &ReliableRecvStream,
    remote_open: bool,
    class: TrafficClass,
    interactive_response_pending: bool,
    mux_limits: MuxLimits,
) -> bool {
    send_stream.repair_bytes() > 0
        || (remote_open
            && interactive_response_pending
            && tcp_relay_expects_interactive_response(class))
        || tcp_relay_response_stall_watch_active(recv_stream, remote_open, class, mux_limits)
}

pub(super) fn tcp_relay_response_stall_watch_active(
    recv_stream: &ReliableRecvStream,
    remote_open: bool,
    class: TrafficClass,
    mux_limits: MuxLimits,
) -> bool {
    remote_open
        && recv_stream.next_offset() > 0
        && (matches!(class, TrafficClass::Bulk | TrafficClass::Background)
            || recv_stream.next_offset() >= tcp_relay_response_stall_watch_bytes(mux_limits))
}

pub(super) fn tcp_relay_stall_progress_anchor(
    last_stream_progress_at: Instant,
    last_delivery_progress_at: Instant,
    last_response_stall_repair_at: Instant,
    recv_stream: &ReliableRecvStream,
    remote_open: bool,
    class: TrafficClass,
    mux_limits: MuxLimits,
) -> Instant {
    if tcp_relay_response_stall_watch_active(recv_stream, remote_open, class, mux_limits) {
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
    class: TrafficClass,
) -> tokio::time::Instant {
    let anchor = if last_delivery_progress_at > last_receive_hole_repair_at {
        last_delivery_progress_at
    } else {
        last_receive_hole_repair_at
    };
    tokio::time::Instant::from_std(anchor + tcp_relay_stall_timeout(path, class))
}

pub(super) fn tcp_relay_receive_hole_failure_attempts(_class: TrafficClass) -> u32 {
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
    class: TrafficClass,
    payload_bytes: usize,
    path_last_delivery_at: &HashMap<RelayPathKey, Instant>,
) -> Option<RelayPathKey> {
    if path_keys.len() <= 1 {
        return None;
    }
    path_keys.iter().copied().max_by(|left, right| {
        let left_score = tcp_relay_receive_hole_victim_score(context, *left, class, payload_bytes);
        let right_score =
            tcp_relay_receive_hole_victim_score(context, *right, class, payload_bytes);
        left_score
            .total_cmp(&right_score)
            .then_with(|| tcp_relay_stale_delivery_order(*left, *right, path_last_delivery_at))
    })
}

pub(super) fn tcp_relay_receive_hole_victim_score(
    context: &ClientPathContext,
    key: RelayPathKey,
    class: TrafficClass,
    payload_bytes: usize,
) -> f64 {
    tcp_relay_path_eta_ms(context, key, class, payload_bytes).unwrap_or(f64::INFINITY)
}

pub(super) fn tcp_relay_delivery_path_should_become_active(
    context: &ClientPathContext,
    current: Option<RelayPathKey>,
    delivered: RelayPathKey,
    class: TrafficClass,
    payload_bytes: usize,
) -> bool {
    if current == Some(delivered) {
        return false;
    }
    let Some(delivered_eta) = tcp_relay_path_eta_ms(context, delivered, class, payload_bytes)
    else {
        return false;
    };
    let current_eta = current
        .and_then(|key| tcp_relay_path_eta_ms(context, key, class, payload_bytes))
        .unwrap_or(f64::INFINITY);
    delivered_eta < current_eta
}

pub(super) fn tcp_relay_path_eta_ms(
    context: &ClientPathContext,
    key: RelayPathKey,
    class: TrafficClass,
    payload_bytes: usize,
) -> Option<f64> {
    relay_path_snapshot(context, key).and_then(|snapshot| {
        scheduler::score_path(snapshot, class, payload_bytes, SchedulerPolicy::default())
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

pub(super) fn tcp_relay_expects_interactive_response(class: TrafficClass) -> bool {
    matches!(
        class,
        TrafficClass::Control | TrafficClass::Interactive | TrafficClass::RealtimeDatagram
    )
}

pub(super) fn tcp_relay_response_stall_watch_bytes(mux_limits: MuxLimits) -> u64 {
    (tcp_relay_buffer_len(mux_limits) as u64).min(mux_limits.max_stream_window_bytes)
}

pub(super) fn tcp_relay_stall_deadline(
    last_progress_at: Instant,
    path: Option<PathSnapshot>,
    class: TrafficClass,
) -> tokio::time::Instant {
    tokio::time::Instant::from_std(last_progress_at + tcp_relay_stall_timeout(path, class))
}

pub(super) fn tcp_relay_stall_timeout(path: Option<PathSnapshot>, class: TrafficClass) -> Duration {
    let (srtt_ms, jitter_ms) = path.map_or((250.0, 50.0), |path| {
        (path.srtt_ms.max(1.0), path.jitter_ms.max(0.0))
    });
    let rtt_gain = match class {
        TrafficClass::Control | TrafficClass::RealtimeDatagram => 1.5,
        TrafficClass::Interactive => 2.0,
        TrafficClass::Bulk => 1.5,
        TrafficClass::Background => 3.0,
    };
    Duration::from_secs_f64(
        ((srtt_ms * rtt_gain + jitter_ms * 4.0 + 100.0) / 1000.0).clamp(
            TCP_STREAM_STALL_MIN_TIMEOUT.as_secs_f64(),
            TCP_STREAM_STALL_MAX_TIMEOUT.as_secs_f64(),
        ),
    )
}
