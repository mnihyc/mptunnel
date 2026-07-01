use super::*;

pub(super) async fn relay_migrating_tcp_stream<S>(
    mut local: S,
    context: &ClientPathContext,
    spec: ReliableRelayOpenSpec,
    remote: OpenedRemoteStream,
) -> Result<PathDeliveryStats, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let initial_key = RelayPathKey {
        underlay: remote.stream.underlay,
        index: remote.path_index,
    };
    let mut remotes =
        ReliableRelayRemoteSet::new(remote, reliable_stream_frame_queue(context.mux_limits));
    let stream_id = remotes.stream_id();
    let mut send_stream = ReliableSendStream::new(stream_id, context.mux_limits);
    send_stream.update_max_offset(remotes.max_offset());
    let mut recv_stream = ReliableRecvStream::new(stream_id, context.mux_limits);
    let chunk_size =
        adaptive_reliable_relay_chunk_bytes(None, FlowLane::Latency, context.mux_limits);
    let mut buf = vec![0u8; chunk_size];
    let mut local_open = true;
    let mut remote_open = true;
    let mut pending_local_fin = false;
    let mut pending_remote_fin_offset = None;
    let mut stats = PathDeliveryStats::default();
    let mut path_stats = HashMap::<RelayPathKey, PathDeliveryStats>::new();
    let mut path_next_live_sample_bytes = HashMap::<RelayPathKey, u64>::new();
    let mut sender = RelaySenderService::new(stream_id);
    let mut flow_demand = ReliableRelayFlowDemandTracker::new();
    let mut last_stream_progress_at = Instant::now();
    let mut last_delivery_progress_at = Instant::now();
    let mut last_response_stall_repair_at = Instant::now();
    let mut response_stall_reannounce_attempts = 0_u32;
    let mut last_receive_hole_repair_at = Instant::now();
    let mut receive_hole_repair_attempts = 0_u32;
    let mut path_last_delivery_at = HashMap::from([(initial_key, Instant::now())]);
    let mut interactive_response_pending = false;
    let mut recv_progress = ReliableRecvProgress::default();
    let mut ack_gap_repair = ReliableAckGapRepairProgress::default();
    let mut last_recv_progress_sent_at = Instant::now();
    let mut sender_queue = ReliableRelaySenderQueue::default();
    let mut sender_retry_at: Option<tokio::time::Instant> = None;
    #[cfg(feature = "lab-diagnostics")]
    let mut last_reported_budget: Option<(FlowLane, usize, usize)> = None;
    #[cfg(feature = "lab-diagnostics")]
    let mut last_reported_read_block: Option<(usize, usize, usize)> = None;
    #[cfg(feature = "lab-diagnostics")]
    let mut last_reported_receive_hole: Option<(u64, usize, usize, u64)> = None;

    let result = loop {
        if !local_open && !remote_open && sender_queue.is_empty() {
            break Ok(stats);
        }
        let path_snapshot = remotes
            .primary_path_key()
            .and_then(|key| relay_path_snapshot(context, key));
        let demand_update = flow_demand.refresh(
            ReliableRelayFlowSignals::new(
                send_stream
                    .next_offset()
                    .saturating_add(sender_queue.data_bytes() as u64),
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
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "client_stream_rebalance_due",
                format_args!(
                    "stream_id={} lane={:?} promoted={} observed_bytes={} send_rate_mbps={:.3} interval_ms={:.3} attached_paths={}",
                    stream_id.0,
                    relay_lane,
                    demand_update.promoted_to_throughput,
                    demand_update.observed_bytes,
                    demand_update.send_rate_bps / 1_000_000.0,
                    demand_update.rebalance_interval.as_secs_f64() * 1000.0,
                    remotes.path_keys().len(),
                ),
            );
            flow_demand.mark_rebalance_attempted();
            if let Err(err) = switch_reliable_relay_to_best_path(
                context,
                &spec,
                relay_lane,
                &mut remotes,
                &send_stream,
                !local_open,
                ReliableRelayAttachMode::BulkStriping,
            )
            .await
            {
                eprintln!("warning: TCP auto path attachment failed: {err}");
            } else {
                last_stream_progress_at = Instant::now();
            }
            if relay_lane_is_bulk(relay_lane)
                && let Err(err) = attach_reliable_relay_validation_paths(
                    context,
                    &spec,
                    relay_lane,
                    &mut remotes,
                    &send_stream,
                    !local_open,
                )
                .await
            {
                eprintln!("warning: TCP auto validation attachment failed: {err}");
            }
            send_stream.update_max_offset(remotes.max_offset());
        }
        let adaptive_chunk = adaptive_relay_chunk_bytes_for_underlay(
            path_snapshot,
            relay_lane,
            context.mux_limits,
            remotes
                .active_carrier_underlay()
                .unwrap_or(UnderlayProtocol::Tcp),
            remotes.max_frame_payload_bytes(context.mux_limits),
        );
        resize_reliable_relay_buffer(&mut buf, adaptive_chunk);
        let adaptive_inflight =
            adaptive_reliable_relay_inflight_bytes(path_snapshot, relay_lane, context.mux_limits);
        let sender_queue_limit =
            reliable_relay_sender_queue_limit(context.mux_limits, adaptive_inflight);
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
        let stall_watch_active = reliable_relay_stall_watch_active(
            &send_stream,
            &recv_stream,
            remote_open,
            relay_lane,
            interactive_response_pending,
            context.mux_limits,
        );
        let stall_progress_anchor = reliable_relay_stall_progress_anchor(
            last_stream_progress_at,
            last_delivery_progress_at,
            last_response_stall_repair_at,
            &recv_stream,
            remote_open,
            relay_lane,
            context.mux_limits,
        );
        let receive_hole_repair_active =
            reliable_relay_receive_hole_repair_active(&recv_stream, remote_open);
        let receive_hole_repair_deadline = reliable_relay_receive_hole_repair_deadline(
            last_delivery_progress_at,
            last_receive_hole_repair_at,
            path_snapshot,
            relay_lane,
        );
        let stall_deadline =
            reliable_relay_stall_deadline(stall_progress_anchor, path_snapshot, relay_lane);
        let recv_progress_deadline = tokio::time::Instant::from_std(
            last_recv_progress_sent_at
                + reliable_stream_recv_progress_interval(path_snapshot, relay_lane),
        );
        if sender_retry_at.is_some_and(|deadline| deadline <= tokio::time::Instant::now()) {
            sender_retry_at = None;
        }
        let inbound_frame_ready = remotes.has_buffered_frame();
        let queued_send_blocked = !sender_queue.is_empty() && sender_retry_at.is_some();
        let queued_send_ready =
            !sender_queue.is_empty() && !queued_send_blocked && !inbound_frame_ready;
        let queued_send_retry_deadline = sender_retry_at.unwrap_or_else(tokio::time::Instant::now);
        let can_read_by_flow = reliable_relay_can_read_product_source(
            local_open,
            queued_send_blocked,
            &send_stream,
            &sender_queue,
            context.mux_limits,
            sender_queue_limit,
        );
        let prospective_read_budget = if can_read_by_flow {
            reliable_relay_sender_queue_read_budget(
                &send_stream,
                &sender_queue,
                context.mux_limits,
                sender_queue_limit,
                adaptive_chunk.min(buf.len()),
            )
        } else {
            0
        };
        let can_read_local =
            can_read_by_flow && prospective_read_budget > 0 && !inbound_frame_ready;
        let can_send_pending_fin = pending_local_fin
            && sender_queue.is_empty()
            && (!remotes.fin_requires_repair_drain() || send_stream.repair_bytes() == 0);
        #[cfg(feature = "lab-diagnostics")]
        {
            if local_open && !can_read_local {
                let blocked_state = (
                    send_stream.repair_bytes(),
                    send_stream.send_credit_bytes(),
                    adaptive_inflight,
                );
                if last_reported_read_block != Some(blocked_state) {
                    lab_diagnostic(
                        "relay_local_read_blocked",
                        format_args!(
                            "stream_id={} lane={:?} repair_bytes={} send_credit_bytes={} inflight_limit={} sent_offset={} received_offset={}",
                            stream_id.0,
                            relay_lane,
                            blocked_state.0,
                            blocked_state.1,
                            blocked_state.2,
                            send_stream.next_offset(),
                            recv_stream.next_offset(),
                        ),
                    );
                    last_reported_read_block = Some(blocked_state);
                }
            } else {
                last_reported_read_block = None;
            }
        }

        tokio::select! {
            _ = tokio::time::sleep_until(receive_hole_repair_deadline), if receive_hole_repair_active => {
                match attach_reliable_relay_paths(
                    context,
                    &spec,
                    relay_lane,
                    &mut remotes,
                    &send_stream,
                    !local_open,
                    ReliableRelayAttachMode::Any,
                )
                .await
                {
                    Ok(attached) if attached > 0 => {
                        sender_retry_at = None;
                        send_stream.update_max_offset(remotes.max_offset());
                        last_receive_hole_repair_at = Instant::now();
                        receive_hole_repair_attempts = 0;
                        reliable_relay_refresh_path_tracking(
                            &mut path_last_delivery_at,
                            &remotes.path_keys(),
                            Instant::now(),
                        );
                        continue;
                    }
                    Ok(_) => {
                        receive_hole_repair_attempts =
                            receive_hole_repair_attempts.saturating_add(1);
                        if receive_hole_repair_attempts >= reliable_relay_receive_hole_failure_attempts(relay_lane) {
                            let path_keys = remotes.path_keys();
                            if let Some(path_key) = reliable_relay_receive_hole_victim(
                                context,
                                &path_keys,
                                relay_lane,
                                recv_stream.reorder_bytes().max(1),
                                &path_last_delivery_at,
                            ) && remotes.fail_path_key(context, path_key).await
                            {
                                path_last_delivery_at.remove(&path_key);
                                if !remotes.is_empty()
                                    && let Err(err) = sender.reannounce_active_path(context, &mut remotes, &spec, relay_lane)
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
                                && let Err(err) = sender.reannounce_active_path(context, &mut remotes, &spec, relay_lane)
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
                    let reannounce_budget = reliable_relay_sole_survivor_reannounce_attempts(
                        reliable_relay_stall_timeout(path_snapshot, relay_lane),
                    );
                    if response_stall_reannounce_attempts
                        < reannounce_budget
                    {
                        response_stall_reannounce_attempts =
                            response_stall_reannounce_attempts.saturating_add(1);
                        match sender.reannounce_active_path(context, &mut remotes, &spec, relay_lane)
                            .await
                        {
                            Ok(()) => {
                                send_stream.update_max_offset(remotes.max_offset());
                                last_stream_progress_at = Instant::now();
                                last_response_stall_repair_at = Instant::now();
                                reliable_relay_refresh_path_tracking(
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
                let failed_key = remotes.active_path_instance().map(|instance| instance.key);
                if let Some(instance) = remotes.active_path_instance() {
                    remotes.fail_path_instance(context, instance).await;
                }
                if !remotes.is_empty() {
                    match sender.reannounce_active_path(context, &mut remotes, &spec, relay_lane)
                        .await
                    {
                        Ok(()) => {
                            send_stream.update_max_offset(remotes.max_offset());
                            last_stream_progress_at = Instant::now();
                            last_response_stall_repair_at = Instant::now();
                            reliable_relay_refresh_path_tracking(
                                &mut path_last_delivery_at,
                                &remotes.path_keys(),
                                Instant::now(),
                            );
                            if let Some(failed_key) = failed_key
                                && sender.enqueue_failed_path_gap_repairs(
                                    &mut sender_queue,
                                    context,
                                    &remotes,
                                    &send_stream,
                                    failed_key,
                                    relay_lane,
                                )
                            {
                                sender_retry_at = None;
                            }
                            continue;
                        }
                        Err(err) => {
                            eprintln!("warning: TCP stall survivor reannounce failed: {err}");
                        }
                    }
                }
                match attach_reliable_relay_paths(
                    context,
                    &spec,
                    relay_lane,
                    &mut remotes,
                    &send_stream,
                    !local_open,
                    ReliableRelayAttachMode::Any,
                )
                .await
                {
                    Ok(attached) if attached > 0 => {
                        sender_retry_at = None;
                        send_stream.update_max_offset(remotes.max_offset());
                        last_stream_progress_at = Instant::now();
                        last_response_stall_repair_at = Instant::now();
                        reliable_relay_refresh_path_tracking(
                            &mut path_last_delivery_at,
                            &remotes.path_keys(),
                            Instant::now(),
                        );
                        if let Some(failed_key) = failed_key
                            && sender.enqueue_failed_path_gap_repairs(
                                &mut sender_queue,
                                context,
                                &remotes,
                                &send_stream,
                                failed_key,
                                relay_lane,
                            )
                        {
                            sender_retry_at = None;
                        }
                        continue;
                    }
                    Ok(_) => {
                        last_stream_progress_at = Instant::now();
                        last_response_stall_repair_at = Instant::now();
                    }
                    Err(err) if remotes.is_empty() => break Err(err),
                    Err(err) => {
                        eprintln!("warning: reliable stream stall repair failed: {err}");
                        last_stream_progress_at = Instant::now();
                        last_response_stall_repair_at = Instant::now();
                    }
                }
            }
            _ = tokio::time::sleep_until(recv_progress_deadline), if remotes.path_keys().len() > 1
                && reliable_relay_recv_progress_resend_active(&recv_stream, remote_open) => {
                match sender.send_recv_progress(
                    &mut remotes,
                    context,
                    &recv_stream,
                    &mut recv_progress,
                    RelayRecvProgressSend::new(path_snapshot, relay_lane, true),
                )
                .await
                {
                    Ok(sent) => {
                        if sent {
                            last_stream_progress_at = Instant::now();
                        }
                        last_recv_progress_sent_at = Instant::now();
                    }
                    Err(err) if reliable_relay_error_is_migratable(&err) => {
                        match attach_reliable_relay_paths(
                            context,
                            &spec,
                            relay_lane,
                            &mut remotes,
                            &send_stream,
                            !local_open,
                            ReliableRelayAttachMode::Any,
                        )
                        .await
                        {
                            Ok(attached) if attached > 0 => {
                                sender_retry_at = None;
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
            _ = std::future::ready(()), if can_send_pending_fin => {
                match sender
                    .send_control_frame(
                        context,
                        &mut remotes,
                        Frame::StreamFin {
                            stream_id,
                            final_offset: send_stream.next_offset(),
                        },
                        RelaySendCause::StreamFin,
                    )
                    .await
                {
                    Ok(_) => {
                        pending_local_fin = false;
                        last_stream_progress_at = Instant::now();
                    }
                    Err(err) if reliable_relay_error_is_migratable(&err) => {
                        match attach_reliable_relay_paths(
                            context,
                            &spec,
                            relay_lane,
                            &mut remotes,
                            &send_stream,
                            true,
                            ReliableRelayAttachMode::Any,
                        )
                        .await
                        {
                            Ok(attached) if attached > 0 => {
                                sender_retry_at = None;
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
            _ = std::future::ready(()), if queued_send_ready => {
                let queued_kind = sender_queue
                    .front()
                    .map(|(_, queued)| queued.kind.clone())
                    .expect("queued_send_ready requires queued data");
                match queued_kind {
                    ReliableRelayQueuedWorkKind::Data(payload) => {
                        let frame = match send_stream.send_data(payload, StreamFlags::NONE) {
                            Ok(frame) => frame,
                            Err(err) => break Err(RuntimeError::Stream(err)),
                        };
                        let retry_frame = frame.clone();
                        match sender.send_stream_data(context, &mut remotes, frame.clone()).await {
                            Ok(outcome) => {
                                let (_, committed) = sender_queue
                                    .commit_front()
                                    .expect("sent queued data must still be at queue front");
                                last_stream_progress_at = Instant::now();
                                stats.record_payload_bytes(committed.payload_bytes);
                                path_stats
                                    .entry(outcome.path_key)
                                    .or_default()
                                    .record_payload_bytes(committed.payload_bytes);
                            }
                            Err(RuntimeError::SenderServiceBlocked) => {
                                let _ = send_stream.rollback_committed_data(&frame);
                                sender_retry_at =
                                    Some(tokio::time::Instant::now() + UDP_MIN_RESPONSE_TIMEOUT);
                                continue;
                            }
                            Err(err) if reliable_relay_error_is_migratable(&err) => {
                                let _ = send_stream.rollback_committed_data(&frame);
                                match attach_reliable_relay_paths(
                                    context,
                                    &spec,
                                    relay_lane,
                                    &mut remotes,
                                    &send_stream,
                                    !local_open,
                                    ReliableRelayAttachMode::Any,
                                )
                                .await
                                {
                                    Ok(attached) if attached > 0 => {
                                        sender_retry_at = None;
                                        if let Err(err) = send_stream.commit_prepared_data(&frame) {
                                            break Err(RuntimeError::Stream(err));
                                        }
                                        match sender
                                            .send_stream_data(context, &mut remotes, retry_frame)
                                            .await
                                        {
                                            Ok(outcome) => {
                                                let (_, committed) = sender_queue
                                                    .commit_front()
                                                    .expect("sent queued data must still be at queue front");
                                                last_stream_progress_at = Instant::now();
                                                stats.record_payload_bytes(committed.payload_bytes);
                                                path_stats
                                                    .entry(outcome.path_key)
                                                    .or_default()
                                                    .record_payload_bytes(committed.payload_bytes);
                                            }
                                            Err(RuntimeError::SenderServiceBlocked) => {
                                                let _ = send_stream.rollback_committed_data(&frame);
                                                sender_retry_at =
                                                    Some(tokio::time::Instant::now() + UDP_MIN_RESPONSE_TIMEOUT);
                                                continue;
                                            }
                                            Err(err) if reliable_relay_error_is_migratable(&err) => {
                                                let _ = send_stream.rollback_committed_data(&frame);
                                                break Err(err);
                                            }
                                            Err(err) => {
                                                let _ = send_stream.rollback_committed_data(&frame);
                                                break Err(err);
                                            }
                                        }
                                    }
                                    Ok(_) => break Err(err),
                                    Err(err) => break Err(err),
                                }
                            }
                            Err(err) => {
                                let _ = send_stream.rollback_committed_data(&frame);
                                break Err(err);
                            }
                        }
                    }
                    ReliableRelayQueuedWorkKind::Repair { frame, cause } => {
                        let retry_frame = frame.clone();
                        match sender
                            .send_repair_frame(context, &mut remotes, frame, cause)
                            .await
                        {
                            Ok(outcome) => {
                                let (_, committed) = sender_queue
                                    .commit_front()
                                    .expect("sent queued repair must still be at queue front");
                                #[cfg(not(feature = "lab-diagnostics"))]
                                let _ = committed;
                                #[cfg(not(feature = "lab-diagnostics"))]
                                let _ = outcome;
                                #[cfg(feature = "lab-diagnostics")]
                                lab_diagnostic(
                                    "repair",
                                    format_args!(
                                        "stream_id={} path_underlay={:?} path_index={} cause={} queued_dispatch=true payload_bytes={}",
                                        stream_id.0,
                                        outcome.path_key.underlay,
                                        outcome.path_key.index,
                                        cause.as_str(),
                                        committed.payload_bytes,
                                    ),
                                );
                                last_stream_progress_at = Instant::now();
                            }
                            Err(RuntimeError::SenderServiceBlocked) => {
                                sender_retry_at =
                                    Some(tokio::time::Instant::now() + UDP_MIN_RESPONSE_TIMEOUT);
                                continue;
                            }
                            Err(err) if reliable_relay_error_is_migratable(&err) => {
                                match attach_reliable_relay_paths(
                                    context,
                                    &spec,
                                    relay_lane,
                                    &mut remotes,
                                    &send_stream,
                                    !local_open,
                                    ReliableRelayAttachMode::Any,
                                )
                                .await
                                {
                                    Ok(attached) if attached > 0 => {
                                        sender_retry_at = None;
                                        match sender
                                            .send_repair_frame(
                                                context,
                                                &mut remotes,
                                                retry_frame,
                                                cause,
                                            )
                                            .await
                                        {
                                            Ok(outcome) => {
                                                let (_, committed) = sender_queue
                                                    .commit_front()
                                                    .expect("sent queued repair must still be at queue front");
                                                #[cfg(not(feature = "lab-diagnostics"))]
                                                let _ = committed;
                                                #[cfg(not(feature = "lab-diagnostics"))]
                                                let _ = outcome;
                                                #[cfg(feature = "lab-diagnostics")]
                                                lab_diagnostic(
                                                    "repair",
                                                    format_args!(
                                                        "stream_id={} path_underlay={:?} path_index={} cause={} queued_dispatch=true after_attach=true payload_bytes={}",
                                                        stream_id.0,
                                                        outcome.path_key.underlay,
                                                        outcome.path_key.index,
                                                        cause.as_str(),
                                                        committed.payload_bytes,
                                                    ),
                                                );
                                                last_stream_progress_at = Instant::now();
                                            }
                                            Err(RuntimeError::SenderServiceBlocked) => {
                                                sender_retry_at =
                                                    Some(tokio::time::Instant::now() + UDP_MIN_RESPONSE_TIMEOUT);
                                                continue;
                                            }
                                            Err(err) => break Err(err),
                                        }
                                    }
                                    Ok(_) => break Err(err),
                                    Err(err) => break Err(err),
                                }
                            }
                            Err(err) => break Err(err),
                        }
                    }
                }
            }
            _ = tokio::time::sleep_until(queued_send_retry_deadline), if queued_send_blocked => {
                sender_retry_at = None;
                continue;
            }
            read = async {
                let read_budget = prospective_read_budget;
                #[cfg(feature = "lab-diagnostics")]
                let read_started = Instant::now();
                let result = local.read(&mut buf[..read_budget]).await;
                #[cfg(feature = "lab-diagnostics")]
                if let Ok(read) = &result {
                    lab_perf_record("relay.local_read_wait", read_started.elapsed(), *read);
                }
                result
            }, if can_read_local => {
                let read = match read {
                    Ok(read) => read,
                    Err(err) => break Err(RuntimeError::Io(err)),
                };
                if read == 0 {
                    local_open = false;
                    pending_local_fin = true;
                } else {
                    if reliable_relay_expects_interactive_response(relay_lane) && remote_open {
                        interactive_response_pending = true;
                    }
                    #[cfg(feature = "lab-diagnostics")]
                    let copy_started = Instant::now();
                    let payload = Bytes::copy_from_slice(&buf[..read]);
                    #[cfg(feature = "lab-diagnostics")]
                    lab_perf_record("relay.copy_local_chunk", copy_started.elapsed(), read);
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "client_sender_enqueue",
                        format_args!(
                            "stream_id={} lane={:?} payload_bytes={} queue_bytes={} queue_limit={}",
                            stream_id.0,
                            relay_lane,
                            read,
                            sender_queue.bytes().saturating_add(read),
                            sender_queue_limit,
                        ),
                    );
                    sender_queue.push_data(payload);
                }
            }
            frame = async {
                #[cfg(feature = "lab-diagnostics")]
                let recv_started = Instant::now();
                let result = remotes.recv_frame().await;
                #[cfg(feature = "lab-diagnostics")]
                if let Ok(ReliableRelayRemoteFrame { frame: Ok(frame), .. }) = &result {
                    lab_perf_record("relay.path_recv_frame_wait", recv_started.elapsed(), frame_pacing_bytes(frame));
                }
                result
            }, if remote_open || send_stream.repair_bytes() > 0 => {
                let ReliableRelayRemoteFrame { instance, frame } = match frame {
                    Ok(frame) => frame,
                    Err(err) if reliable_relay_error_is_migratable(&err) => {
                        match attach_reliable_relay_paths(
                            context,
                            &spec,
                            relay_lane,
                            &mut remotes,
                            &send_stream,
                            !local_open,
                            ReliableRelayAttachMode::Any,
                        )
                        .await
                        {
                            Ok(attached) if attached > 0 => {
                                sender_retry_at = None;
                                last_stream_progress_at = Instant::now();
                                continue;
                            }
                            Ok(_) => {
                                if reliable_relay_can_finish_after_path_loss(
                                    local_open,
                                    remote_open,
                                    &send_stream,
                                    &recv_stream,
                                    &sender_queue,
                                    stats,
                                ) {
                                    break Ok(stats);
                                }
                                break Err(err);
                            }
                            Err(_attach_err) => {
                                if reliable_relay_can_finish_after_path_loss(
                                    local_open,
                                    remote_open,
                                    &send_stream,
                                    &recv_stream,
                                    &sender_queue,
                                    stats,
                                ) {
                                    break Ok(stats);
                                }
                                break Err(err);
                            }
                        }
                    }
                    Err(err) => {
                        if reliable_relay_can_finish_after_path_loss(
                            local_open,
                            remote_open,
                            &send_stream,
                            &recv_stream,
                            &sender_queue,
                            stats,
                        ) {
                            break Ok(stats);
                        }
                        break Err(err);
                    }
                };
                let path_key = instance.key;
                let frame = match frame {
                    Ok(frame) => frame,
                    Err(err) if reliable_relay_error_is_migratable(&err) => {
                        remotes.fail_path_instance(context, instance).await;
                        if !remotes.is_empty()
                            && let Err(err) = sender.reannounce_active_path(context, &mut remotes, &spec, relay_lane)
                                .await
                        {
                            eprintln!("warning: TCP path-error survivor reannounce failed: {err}");
                        } else if !remotes.is_empty() {
                            send_stream.update_max_offset(remotes.max_offset());
                            last_stream_progress_at = Instant::now();
                            last_response_stall_repair_at = Instant::now();
                            reliable_relay_refresh_path_tracking(
                                &mut path_last_delivery_at,
                                &remotes.path_keys(),
                                Instant::now(),
                            );
                            if sender.enqueue_failed_path_gap_repairs(
                                &mut sender_queue,
                                context,
                                &remotes,
                                &send_stream,
                                path_key,
                                relay_lane,
                            ) {
                                sender_retry_at = None;
                            }
                        }
                        if remotes.is_empty() {
                            match attach_reliable_relay_paths(
                                context,
                                &spec,
                                relay_lane,
                                &mut remotes,
                                &send_stream,
                                !local_open,
                                ReliableRelayAttachMode::Any,
                            )
                            .await
                            {
                                Ok(attached) if attached > 0 => {
                                    sender_retry_at = None;
                                    send_stream.update_max_offset(remotes.max_offset());
                                    last_stream_progress_at = Instant::now();
                                    reliable_relay_refresh_path_tracking(
                                        &mut path_last_delivery_at,
                                        &remotes.path_keys(),
                                        Instant::now(),
                                    );
                                    if sender.enqueue_failed_path_gap_repairs(
                                        &mut sender_queue,
                                        context,
                                        &remotes,
                                        &send_stream,
                                        path_key,
                                        relay_lane,
                                    ) {
                                        sender_retry_at = None;
                                    }
                                    continue;
                                }
                                Ok(_) => {
                                    if reliable_relay_can_finish_after_path_loss(
                                        local_open,
                                        remote_open,
                                        &send_stream,
                                        &recv_stream,
                                        &sender_queue,
                                        stats,
                                    ) {
                                        break Ok(stats);
                                    }
                                    break Err(err);
                                }
                                Err(_attach_err) => {
                                    if reliable_relay_can_finish_after_path_loss(
                                        local_open,
                                        remote_open,
                                        &send_stream,
                                        &recv_stream,
                                        &sender_queue,
                                        stats,
                                    ) {
                                        break Ok(stats);
                                    }
                                    break Err(err);
                                }
                            }
                        }
                        path_last_delivery_at.remove(&path_key);
                        continue;
                    }
                    Err(err) => break Err(err),
                };
                sender_retry_at = None;
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
                        {
                            let reorder_bytes = recv_stream.reorder_bytes();
                            if reorder_bytes > 0 {
                                let ack_ranges = recv_stream.ack_ranges();
                                let hole_state = (
                                    recv_stream.next_offset(),
                                    reorder_bytes,
                                    ack_ranges.len(),
                                    ack_ranges.last().map_or(0, |range| range.end),
                                );
                                if last_reported_receive_hole != Some(hole_state) {
                                    lab_diagnostic(
                                        "receive_hole",
                                        format_args!(
                                            "stream_id={} path_underlay={:?} path_index={} next_offset={} reorder_bytes={} ack_ranges={} largest_end={}",
                                            stream_id.0,
                                            path_key.underlay,
                                            path_key.index,
                                            hole_state.0,
                                            hole_state.1,
                                            hole_state.2,
                                            hole_state.3,
                                        ),
                                    );
                                    last_reported_receive_hole = Some(hole_state);
                                }
                            } else {
                                last_reported_receive_hole = None;
                            }
                        }
                        #[cfg(feature = "lab-diagnostics")]
                        lab_perf_record("mux.receive_data", mux_started.elapsed(), payload_len);
                        last_stream_progress_at = Instant::now();
                        let delivered_progress = recv_stream.next_offset() > previous_remote_offset;
                        if delivered_progress {
                            last_delivery_progress_at = Instant::now();
                            receive_hole_repair_attempts = 0;
                            response_stall_reannounce_attempts = 0;
                            path_last_delivery_at.insert(path_key, Instant::now());
                        }
                        let mut write_error = None;
                        let mut delivered_payload_bytes = 0usize;
                        for chunk in outcome.delivered {
                            stats.record_payload_bytes(chunk.len());
                            let path_stat = path_stats
                                .entry(path_key)
                                .or_default();
                            path_stat.record_payload_bytes(chunk.len());
                            delivered_payload_bytes =
                                delivered_payload_bytes.saturating_add(chunk.len());
                            maybe_mark_live_relay_path_delivery(
                                context,
                                path_key,
                                *path_stat,
                                &mut path_next_live_sample_bytes,
                            );
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
                            if delivered_payload_bytes > 0
                                && reliable_relay_delivery_path_should_become_active(
                                    context,
                                    remotes.active_path_key(),
                                    path_key,
                                    relay_lane,
                                    reliable_relay_attach_payload_bytes(
                                        &send_stream,
                                        relay_lane,
                                        context.mux_limits,
                                    ),
                                ) && remotes.promote_path_instance_to_active(instance)
                            {
                                #[cfg(feature = "lab-diagnostics")]
                                lab_diagnostic(
                                    "client_relay_active_path_promoted",
                                    format_args!(
                                        "stream_id={} path_underlay={:?} path_index={} lane={:?} delivered_bytes={} cause=delivery_evidence",
                                        stream_id.0,
                                        path_key.underlay,
                                        path_key.index,
                                        relay_lane,
                                        delivered_payload_bytes,
                                    ),
                                );
                                last_stream_progress_at = Instant::now();
                            }
                        }
                        match sender.send_recv_progress(
                            &mut remotes,
                            context,
                            &recv_stream,
                            &mut recv_progress,
                            RelayRecvProgressSend::new(path_snapshot, relay_lane, false),
                        )
                        .await
                        {
                            Ok(sent) => {
                                if sent {
                                    last_recv_progress_sent_at = Instant::now();
                                }
                            }
                            Err(err) if reliable_relay_error_is_migratable(&err) => {
                                match attach_reliable_relay_paths(
                                    context,
                                    &spec,
                                    relay_lane,
                                    &mut remotes,
                                    &send_stream,
                                    !local_open,
                                    ReliableRelayAttachMode::Any,
                                )
                            .await
                            {
                                    Ok(attached) if attached > 0 => {
                                        sender_retry_at = None;
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
                        complete,
                        ranges,
                    } if ack_stream_id == stream_id => {
                        #[cfg(feature = "lab-diagnostics")]
                        let previous_repair_bytes = send_stream.repair_bytes();
                        #[cfg(feature = "lab-diagnostics")]
                        let mux_started = Instant::now();
                        let ack = send_stream.apply_ack(&ranges);
                        #[cfg(feature = "lab-diagnostics")]
                        lab_perf_record("mux.apply_ack", mux_started.elapsed(), ack.released_bytes);
                        sender.release_acked_ranges(context, &ranges);
                        let repair_limit = adaptive_reliable_relay_repair_bytes(
                            path_snapshot,
                            relay_lane,
                            context.mux_limits,
                        );
                        let has_multipath_repair_alternative = remotes.path_keys().len() > 1;
                        let udp_gap_repair_ready = ack_gap_repair.repair_ready(
                            complete,
                            &ranges,
                            remotes.active_carrier_underlay(),
                            has_multipath_repair_alternative,
                            path_snapshot,
                            relay_lane,
                        );
                        let repair_frames = stream_ack_gap_repair_frames(
                            &send_stream,
                            &ranges,
                            repair_limit,
                            complete,
                            remotes.active_carrier_underlay(),
                            has_multipath_repair_alternative,
                            udp_gap_repair_ready,
                        );
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "stream_ack_received",
                            format_args!(
                                "stream_id={} complete={} ranges={} largest_end={} released_bytes={} repair_bytes_before={} repair_bytes_after={} repair_frames={} active_underlay={:?} multipath_repair_alternative={} udp_gap_repair_ready={}",
                                stream_id.0,
                                complete,
                                ranges.len(),
                                ranges.iter().map(|range| range.end).max().unwrap_or(0),
                                ack.released_bytes,
                                previous_repair_bytes,
                                ack.remaining_repair_bytes,
                                repair_frames.len(),
                                remotes.active_carrier_underlay(),
                                has_multipath_repair_alternative,
                                udp_gap_repair_ready,
                            ),
                        );
                        for frame in repair_frames {
                            sender_queue
                                .push_repair_with_cause(frame, RelaySendCause::AckGapRepair);
                            #[cfg(feature = "lab-diagnostics")]
                            lab_diagnostic(
                                "repair",
                                format_args!(
                                    "stream_id={} cause=ack_gap queued=true",
                                    stream_id.0,
                                ),
                            );
                            sender_retry_at = None;
                        }
                        #[cfg(not(feature = "lab-diagnostics"))]
                        let _ = ack;
                        last_stream_progress_at = Instant::now();
                        if pending_local_fin
                            && sender_queue.is_empty()
                            && send_stream.repair_bytes() == 0
                        {
                            match sender
                                .send_control_frame(
                                    context,
                                    &mut remotes,
                                    Frame::StreamFin {
                                        stream_id,
                                        final_offset: send_stream.next_offset(),
                                    },
                                    RelaySendCause::StreamFin,
                                )
                                .await
                            {
                                Ok(_) => {
                                    pending_local_fin = false;
                                    last_stream_progress_at = Instant::now();
                                }
                                Err(err) if reliable_relay_error_is_migratable(&err) => {
                                    match attach_reliable_relay_paths(
                                        context,
                                        &spec,
                                        relay_lane,
                                        &mut remotes,
                                        &send_stream,
                                        true,
                                        ReliableRelayAttachMode::Any,
                                    )
                                    .await
                                    {
                                        Ok(attached) if attached > 0 => {
                                            sender_retry_at = None;
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
                        match sender.send_attach_control_to_instance(&mut remotes, instance, &send_stream, pending_local_fin)
                            .await
                        {
                            Ok(true) => {
                                last_stream_progress_at = Instant::now();
                                last_response_stall_repair_at = Instant::now();
                            }
                            Ok(false) => {}
                            Err(err) if reliable_relay_error_is_migratable(&err) => {
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
    sender.release_all(context);
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

pub(super) async fn switch_reliable_relay_to_best_path(
    context: &ClientPathContext,
    spec: &ReliableRelayOpenSpec,
    lane: FlowLane,
    remotes: &mut ReliableRelayRemoteSet,
    send_stream: &ReliableSendStream,
    resend_fin: bool,
    mode: ReliableRelayAttachMode,
) -> Result<bool, RuntimeError> {
    let attached =
        attach_reliable_relay_paths(context, spec, lane, remotes, send_stream, resend_fin, mode)
            .await?;
    if attached == 0 {
        return Ok(false);
    }
    Ok(true)
}

fn maybe_mark_live_relay_path_delivery(
    context: &ClientPathContext,
    key: RelayPathKey,
    stats: PathDeliveryStats,
    next_sample_bytes: &mut HashMap<RelayPathKey, u64>,
) {
    let sample_step = reliable_relay_live_delivery_sample_bytes(context.mux_limits);
    let next = next_sample_bytes.entry(key).or_insert(sample_step);
    if stats.payload_bytes < *next {
        return;
    }
    let Some(sample) = stats.rate_sample() else {
        return;
    };
    context.mark_relay_path_rate_sample(key.underlay, key.index, sample);
    *next = stats.payload_bytes.saturating_add(sample_step);
    #[cfg(feature = "lab-diagnostics")]
    lab_diagnostic(
        "path_model",
        format_args!(
            "path_underlay={:?} path_index={} delivered_bytes={} elapsed_ms={:.3} cause=live_delivery",
            key.underlay,
            key.index,
            stats.payload_bytes,
            stats
                .last_payload_at
                .unwrap_or_else(Instant::now)
                .saturating_duration_since(stats.first_payload_at.unwrap_or_else(Instant::now))
                .as_secs_f64()
                * 1000.0,
        ),
    );
}

fn reliable_relay_can_finish_after_path_loss(
    local_open: bool,
    remote_open: bool,
    send_stream: &ReliableSendStream,
    recv_stream: &ReliableRecvStream,
    sender_queue: &ReliableRelaySenderQueue,
    stats: PathDeliveryStats,
) -> bool {
    !local_open
        && remote_open
        && sender_queue.is_empty()
        && send_stream.repair_bytes() == 0
        && recv_stream.reorder_bytes() == 0
        && stats.payload_bytes > 0
}

fn reliable_relay_live_delivery_sample_bytes(mux_limits: MuxLimits) -> u64 {
    reliable_relay_buffer_len(mux_limits) as u64
}

async fn attach_reliable_relay_validation_paths(
    context: &ClientPathContext,
    spec: &ReliableRelayOpenSpec,
    lane: FlowLane,
    remotes: &mut ReliableRelayRemoteSet,
    send_stream: &ReliableSendStream,
    resend_fin: bool,
) -> Result<usize, RuntimeError> {
    if !relay_lane_is_bulk(lane) {
        return Ok(0);
    }
    let payload_bytes =
        reliable_relay_bulk_validation_payload_bytes(send_stream, context.mux_limits);
    let candidates = context
        .ordered_reliable_bulk_validation_path_keys(payload_bytes)
        .into_iter()
        .filter(|key| !remotes.contains_path_key(*key))
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return Ok(0);
    }
    attach_relay_path_candidates(
        context,
        remotes,
        RelayPathAttachRequest {
            spec,
            lane,
            send_stream,
            resend_fin,
            candidates,
            role: StreamOpenRole::Validation,
            allow_mixed_carrier: true,
            send_attach_control: true,
            attach_all_candidates: false,
        },
    )
    .await
}

pub(super) struct RelayPathAttachRequest<'a> {
    spec: &'a ReliableRelayOpenSpec,
    lane: FlowLane,
    send_stream: &'a ReliableSendStream,
    resend_fin: bool,
    candidates: Vec<RelayPathKey>,
    role: StreamOpenRole,
    allow_mixed_carrier: bool,
    send_attach_control: bool,
    attach_all_candidates: bool,
}

pub(super) async fn attach_relay_path_candidates(
    context: &ClientPathContext,
    remotes: &mut ReliableRelayRemoteSet,
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
            request.role,
        )
        .await
        {
            Ok(opened) => {
                let attach_control_result = if request.send_attach_control {
                    send_relay_attach_control_frames(
                        &opened.stream,
                        request.send_stream,
                        request.resend_fin,
                    )
                    .await
                } else {
                    Ok(())
                };
                match attach_control_result {
                    Ok(()) => {
                        match request.role {
                            StreamOpenRole::Active => remotes.attach(opened),
                            StreamOpenRole::Repair => remotes.attach_for_repair(opened),
                            StreamOpenRole::Validation => remotes.attach_for_validation(opened),
                        }
                        attached += 1;
                        if !request.attach_all_candidates {
                            return Ok(attached);
                        }
                    }
                    Err(err) if reliable_relay_error_is_migratable(&err) => {
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
    if !context.tcp_paths.is_empty() && !context.udp_paths.is_empty() {
        RuntimeError::NoSchedulableReliablePath
    } else if !context.tcp_paths.is_empty() {
        RuntimeError::NoSchedulableTcpPath
    } else {
        RuntimeError::NoSchedulableUdpPath
    }
}

pub(super) async fn attach_reliable_relay_paths(
    context: &ClientPathContext,
    spec: &ReliableRelayOpenSpec,
    lane: FlowLane,
    remotes: &mut ReliableRelayRemoteSet,
    send_stream: &ReliableSendStream,
    resend_fin: bool,
    mode: ReliableRelayAttachMode,
) -> Result<usize, RuntimeError> {
    let payload_bytes = match mode {
        ReliableRelayAttachMode::Any => {
            reliable_relay_attach_payload_bytes(send_stream, lane, context.mux_limits)
        }
        ReliableRelayAttachMode::BulkStriping => {
            reliable_relay_bulk_striping_payload_bytes(send_stream, context.mux_limits)
        }
    };
    if matches!(mode, ReliableRelayAttachMode::BulkStriping) {
        let candidates = context.ordered_reliable_bulk_striping_path_keys(payload_bytes);
        let result = attach_relay_path_candidates(
            context,
            remotes,
            RelayPathAttachRequest {
                spec,
                lane,
                send_stream,
                resend_fin,
                candidates,
                role: StreamOpenRole::Validation,
                allow_mixed_carrier: true,
                send_attach_control: false,
                attach_all_candidates: false,
            },
        )
        .await;
        match result {
            Ok(attached) if attached > 0 || !remotes.is_empty() => return Ok(attached),
            Ok(_) => {}
            Err(err)
                if remotes.is_empty()
                    && (stream_open_error_is_path_retryable(&err)
                        || udp_stream_open_error_is_path_retryable(&err)) => {}
            Err(err) => return Err(err),
        }
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
    let role = if matches!(mode, ReliableRelayAttachMode::BulkStriping) {
        StreamOpenRole::Validation
    } else if reliable_relay_should_race_repair(lane, send_stream, resend_fin, mode) {
        StreamOpenRole::Repair
    } else {
        StreamOpenRole::Active
    };
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
            role,
            allow_mixed_carrier: false,
            send_attach_control: true,
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
    spec: &ReliableRelayOpenSpec,
    lane: FlowLane,
    remotes: &mut ReliableRelayRemoteSet,
    send_stream: &ReliableSendStream,
    resend_fin: bool,
    mode: ReliableRelayAttachMode,
) -> Result<usize, RuntimeError> {
    if remotes.active_carrier_underlay() == Some(UnderlayProtocol::Tcp) {
        return Ok(0);
    }
    let stream_id = remotes.stream_id();
    let payload_bytes = match mode {
        ReliableRelayAttachMode::Any => {
            reliable_relay_attach_payload_bytes(send_stream, lane, context.mux_limits)
        }
        ReliableRelayAttachMode::BulkStriping => {
            reliable_relay_bulk_striping_payload_bytes(send_stream, context.mux_limits)
        }
    };
    let mut candidates = match mode {
        ReliableRelayAttachMode::Any => {
            let require_delivery_evidence =
                matches!(lane, FlowLane::Throughput | FlowLane::Background) && !remotes.is_empty();
            context.ordered_udp_stream_repair_path_indices(
                remotes.active_path_index_for(UnderlayProtocol::Udp),
                lane,
                payload_bytes,
                require_delivery_evidence,
            )
        }
        ReliableRelayAttachMode::BulkStriping => context
            .ordered_reliable_bulk_striping_path_keys(payload_bytes)
            .into_iter()
            .filter_map(|key| (key.underlay == UnderlayProtocol::Udp).then_some(key.index))
            .collect(),
    };
    if candidates.is_empty() && remotes.is_empty() {
        candidates = (0..context.udp_paths.len()).collect();
    }
    if matches!(mode, ReliableRelayAttachMode::BulkStriping) {
        candidates.retain(|path_index| {
            !remotes.contains_path_key(RelayPathKey {
                underlay: UnderlayProtocol::Udp,
                index: *path_index,
            })
        });
    }
    let role = if matches!(mode, ReliableRelayAttachMode::BulkStriping) {
        StreamOpenRole::Validation
    } else if reliable_relay_should_race_repair(lane, send_stream, resend_fin, mode) {
        StreamOpenRole::Repair
    } else {
        StreamOpenRole::Active
    };
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
                role,
            },
        )
        .await
        {
            Ok(opened) => {
                match send_relay_attach_control_frames(&opened.stream, send_stream, resend_fin)
                    .await
                {
                    Ok(()) => {
                        match role {
                            StreamOpenRole::Active => remotes.attach(opened),
                            StreamOpenRole::Repair => remotes.attach_for_repair(opened),
                            StreamOpenRole::Validation => remotes.attach_for_validation(opened),
                        }
                        attached += 1;
                        if role == StreamOpenRole::Active
                            || matches!(mode, ReliableRelayAttachMode::BulkStriping)
                        {
                            return Ok(attached);
                        }
                    }
                    Err(err) if reliable_relay_error_is_migratable(&err) => {
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

pub(super) fn reliable_relay_should_race_repair(
    lane: FlowLane,
    send_stream: &ReliableSendStream,
    resend_fin: bool,
    mode: ReliableRelayAttachMode,
) -> bool {
    matches!(mode, ReliableRelayAttachMode::Any)
        && !resend_fin
        && reliable_relay_expects_interactive_response(lane)
        && send_stream.repair_bytes() <= PATH_OPEN_SCORE_BYTES
}

pub(super) fn reliable_relay_attach_payload_bytes(
    send_stream: &ReliableSendStream,
    lane: FlowLane,
    mux_limits: MuxLimits,
) -> usize {
    let floor = if reliable_relay_expects_interactive_response(lane) {
        PATH_OPEN_SCORE_BYTES
    } else {
        reliable_relay_buffer_len(mux_limits)
    };
    let repair_bytes = send_stream.repair_bytes().max(floor);
    let stream_window = usize::try_from(mux_limits.max_stream_window_bytes).unwrap_or(usize::MAX);
    repair_bytes.min(stream_window)
}

pub(super) fn reliable_relay_bulk_striping_payload_bytes(
    send_stream: &ReliableSendStream,
    mux_limits: MuxLimits,
) -> usize {
    let stream_window = usize::try_from(mux_limits.max_stream_window_bytes).unwrap_or(usize::MAX);
    let decision_quantum =
        adaptive_reliable_relay_chunk_bytes(None, FlowLane::Throughput, mux_limits)
            .min(reliable_relay_buffer_len(mux_limits))
            .min(stream_window)
            .max(PATH_OPEN_SCORE_BYTES);
    let repair_bytes = send_stream.repair_bytes();
    if repair_bytes == 0 {
        return decision_quantum;
    }
    repair_bytes
        .min(decision_quantum)
        .min(stream_window)
        .max(PATH_OPEN_SCORE_BYTES)
}

pub(super) fn reliable_relay_bulk_validation_payload_bytes(
    send_stream: &ReliableSendStream,
    mux_limits: MuxLimits,
) -> usize {
    let proof_ceiling = tcp_lane_startup_chunk_bytes(FlowLane::Latency, mux_limits);
    let proof_payload = reliable_relay_bulk_striping_payload_bytes(send_stream, mux_limits)
        .min(proof_ceiling)
        .max(PATH_OPEN_SCORE_BYTES);
    let stream_window = usize::try_from(mux_limits.max_stream_window_bytes).unwrap_or(usize::MAX);
    proof_payload.min(stream_window).max(PATH_OPEN_SCORE_BYTES)
}

pub(super) fn reliable_relay_stall_watch_active(
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
            && reliable_relay_expects_interactive_response(lane))
        || reliable_relay_response_stall_watch_active(recv_stream, remote_open, lane, mux_limits)
}

pub(super) fn reliable_relay_response_stall_watch_active(
    recv_stream: &ReliableRecvStream,
    remote_open: bool,
    lane: FlowLane,
    mux_limits: MuxLimits,
) -> bool {
    remote_open
        && recv_stream.next_offset() > 0
        && (matches!(lane, FlowLane::Throughput | FlowLane::Background)
            || recv_stream.next_offset() >= reliable_relay_response_stall_watch_bytes(mux_limits))
}

pub(super) fn reliable_relay_stall_progress_anchor(
    last_stream_progress_at: Instant,
    last_delivery_progress_at: Instant,
    last_response_stall_repair_at: Instant,
    recv_stream: &ReliableRecvStream,
    remote_open: bool,
    lane: FlowLane,
    mux_limits: MuxLimits,
) -> Instant {
    if reliable_relay_response_stall_watch_active(recv_stream, remote_open, lane, mux_limits) {
        last_delivery_progress_at.max(last_response_stall_repair_at)
    } else {
        last_stream_progress_at
    }
}

pub(super) fn reliable_relay_receive_hole_repair_active(
    recv_stream: &ReliableRecvStream,
    remote_open: bool,
) -> bool {
    remote_open && recv_stream.next_offset() > 0 && recv_stream.reorder_bytes() > 0
}

pub(super) fn reliable_relay_receive_hole_repair_deadline(
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
    tokio::time::Instant::from_std(anchor + reliable_relay_stall_timeout(path, lane))
}

pub(super) fn reliable_relay_receive_hole_failure_attempts(_lane: FlowLane) -> u32 {
    1
}

pub(super) fn reliable_relay_sole_survivor_reannounce_attempts(stall_timeout: Duration) -> u32 {
    let timeout = stall_timeout.max(TCP_STREAM_STALL_MIN_TIMEOUT);
    let stall_scale = (TCP_STREAM_STALL_MAX_TIMEOUT.as_secs_f64() / timeout.as_secs_f64())
        .max(1.0)
        .sqrt();
    (2.0 + stall_scale * 4.0).round().clamp(2.0, 16.0) as u32
}

pub(super) fn reliable_relay_refresh_path_tracking(
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

pub(super) fn reliable_relay_receive_hole_victim(
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
        let left_score =
            reliable_relay_receive_hole_victim_score(context, *left, lane, payload_bytes);
        let right_score =
            reliable_relay_receive_hole_victim_score(context, *right, lane, payload_bytes);
        left_score
            .total_cmp(&right_score)
            .then_with(|| reliable_relay_stale_delivery_order(*left, *right, path_last_delivery_at))
    })
}

pub(super) fn reliable_relay_receive_hole_victim_score(
    context: &ClientPathContext,
    key: RelayPathKey,
    lane: FlowLane,
    payload_bytes: usize,
) -> f64 {
    reliable_relay_path_eta_ms(context, key, lane, payload_bytes).unwrap_or(f64::INFINITY)
}

pub(super) fn reliable_relay_delivery_path_should_become_active(
    context: &ClientPathContext,
    current: Option<RelayPathKey>,
    delivered: RelayPathKey,
    lane: FlowLane,
    payload_bytes: usize,
) -> bool {
    if current == Some(delivered) {
        return false;
    }
    if relay_lane_is_bulk(lane)
        && !context.relay_path_has_delivery_sample(delivered.underlay, delivered.index)
    {
        return false;
    }
    let Some(delivered_eta) = reliable_relay_path_eta_ms(context, delivered, lane, payload_bytes)
    else {
        return false;
    };
    let current_eta = current
        .and_then(|key| reliable_relay_path_eta_ms(context, key, lane, payload_bytes))
        .unwrap_or(f64::INFINITY);
    delivered_eta < current_eta
}

pub(super) fn reliable_relay_path_eta_ms(
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

pub(super) fn reliable_relay_stale_delivery_order(
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

pub(super) fn reliable_relay_expects_interactive_response(lane: FlowLane) -> bool {
    matches!(
        lane,
        FlowLane::Control | FlowLane::Latency | FlowLane::RealtimeDatagram
    )
}

pub(super) fn reliable_relay_response_stall_watch_bytes(mux_limits: MuxLimits) -> u64 {
    (reliable_relay_buffer_len(mux_limits) as u64).min(mux_limits.max_stream_window_bytes)
}

pub(super) fn reliable_relay_stall_deadline(
    last_progress_at: Instant,
    path: Option<PathSnapshot>,
    lane: FlowLane,
) -> tokio::time::Instant {
    tokio::time::Instant::from_std(last_progress_at + reliable_relay_stall_timeout(path, lane))
}

pub(super) fn reliable_relay_stall_timeout(path: Option<PathSnapshot>, lane: FlowLane) -> Duration {
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
