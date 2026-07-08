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
    let mut remotes =
        ReliableRelayRemoteSet::new(remote, reliable_stream_frame_queue(context.mux_limits));
    let stream_id = remotes.stream_id();
    let mut send_stream =
        ReliableSendStream::new_with_initial_max_offset(stream_id, context.mux_limits, 0);
    send_stream.update_max_offset(remotes.max_offset());
    let mut recv_stream = ReliableRecvStream::new(stream_id, context.mux_limits);
    let chunk_size =
        adaptive_reliable_relay_chunk_bytes(None, FlowLane::Latency, context.mux_limits);
    let mut buf = bytes::BytesMut::with_capacity(chunk_size);
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
    let mut interactive_response_pending = false;
    let mut recv_progress = ReliableRecvProgress::default();
    let mut ack_gap_repair = ReliableAckGapRepairProgress::default();
    let mut last_recv_progress_sent_at = Instant::now();
    let mut sender_queue = ReliableRelaySenderQueue::default();
    let mut sender_retry_at: Option<tokio::time::Instant> = None;
    let (validation_open_tx, mut validation_open_rx) = mpsc::channel(
        context
            .tcp_paths
            .len()
            .saturating_add(context.udp_paths.len())
            .max(1),
    );
    let mut pending_validation_opens = HashMap::<RelayPathKey, RelayValidationOpenTask>::new();
    let mut attempted_validation_paths = std::collections::HashSet::<RelayPathKey>::new();
    if reliable_relay_has_evidenced_bulk_alternative(context, &remotes, &send_stream)
        && spawn_reliable_relay_validation_opens(
            context,
            &spec,
            FlowLane::Throughput,
            &remotes,
            &send_stream,
            &mut pending_validation_opens,
            &mut attempted_validation_paths,
            &validation_open_tx,
        )
    {
        last_stream_progress_at = Instant::now();
    }
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
            .and_then(|key| context.reliable_path_snapshot(key));
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
        if demand_update.prevalidate_bulk && !relay_lane_is_bulk(relay_lane) {
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "client_stream_prevalidation_due",
                format_args!(
                    "stream_id={} observed_bytes={} send_rate_mbps={:.3} attached_paths={}",
                    stream_id.0,
                    demand_update.observed_bytes,
                    demand_update.send_rate_bps / 1_000_000.0,
                    remotes.path_keys().len(),
                ),
            );
            if spawn_reliable_relay_validation_opens(
                context,
                &spec,
                FlowLane::Throughput,
                &remotes,
                &send_stream,
                &mut pending_validation_opens,
                &mut attempted_validation_paths,
                &validation_open_tx,
            ) {
                last_stream_progress_at = Instant::now();
            }
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
            if relay_lane_is_bulk(relay_lane) {
                if spawn_reliable_relay_validation_opens(
                    context,
                    &spec,
                    relay_lane,
                    &remotes,
                    &send_stream,
                    &mut pending_validation_opens,
                    &mut attempted_validation_paths,
                    &validation_open_tx,
                ) {
                    last_stream_progress_at = Instant::now();
                }
            } else if let Err(err) = switch_reliable_relay_to_best_path(
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
                eprintln!("warning: reliable auto path attachment failed: {err}");
            } else {
                last_stream_progress_at = Instant::now();
            }
            send_stream.update_max_offset(remotes.max_offset());
        }
        let adaptive_chunk = adaptive_reliable_relay_chunk_bytes_with_frame_limit(
            path_snapshot,
            relay_lane,
            context.mux_limits,
            remotes.max_frame_payload_bytes(context.mux_limits),
        );
        let adaptive_inflight =
            adaptive_reliable_relay_inflight_bytes(path_snapshot, relay_lane, context.mux_limits);
        let sender_queue_limit =
            reliable_relay_sender_queue_limit(context.mux_limits, adaptive_inflight);
        let source_read_ceiling = reliable_relay_buffer_len(context.mux_limits)
            .min(remotes.max_frame_payload_bytes(context.mux_limits))
            .min(sender_queue_limit)
            .max(1);
        resize_reliable_relay_buffer(&mut buf, source_read_ceiling);
        let (sender_dispatch_byte_budget, sender_dispatch_item_budget) =
            reliable_relay_sender_dispatch_budget(
                context.mux_limits,
                relay_lane,
                adaptive_chunk,
                adaptive_inflight,
                sender_queue_limit,
            );
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
        let queued_send_blocked = reliable_relay_queued_send_blocked_for_retry(
            sender_queue.is_empty(),
            sender_retry_at,
            sender_queue
                .front_lane()
                .is_some_and(|work_lane| remotes.can_enqueue_work_lane_now(work_lane, relay_lane)),
        );
        let queued_send_ready =
            !sender_queue.is_empty() && !queued_send_blocked && !inbound_frame_ready;
        let queued_send_retry_deadline = sender_retry_at.unwrap_or_else(tokio::time::Instant::now);
        let carrier_capacity_notifies = if queued_send_blocked {
            remotes
                .paths
                .iter()
                .flat_map(|path| path.stream.capacity_notifies())
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let has_carrier_capacity_notify = !carrier_capacity_notifies.is_empty();
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
                source_read_ceiling,
            )
        } else {
            0
        };
        let can_read_local =
            can_read_by_flow && prospective_read_budget > 0 && !inbound_frame_ready;
        let can_send_pending_fin =
            reliable_relay_can_send_pending_fin(pending_local_fin, sender_queue.is_empty());
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
                        continue;
                    }
                    Ok(_) => {
                        receive_hole_repair_attempts =
                            receive_hole_repair_attempts.saturating_add(1);
                        match sender
                            .send_recv_progress(
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
                                    last_recv_progress_sent_at = Instant::now();
                                    last_stream_progress_at = Instant::now();
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
                                        send_stream.update_max_offset(remotes.max_offset());
                                        last_stream_progress_at = Instant::now();
                                    }
                                    Ok(_) => {}
                                    Err(err) => break Err(err),
                                }
                            }
                            Err(err) => break Err(err),
                        }
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "receive_hole_repair_signal",
                            format_args!(
                                "stream_id={} lane={:?} reorder_bytes={} attempts={} action=ack_progress_only",
                                stream_id.0,
                                relay_lane,
                                recv_stream.reorder_bytes(),
                                receive_hole_repair_attempts,
                            ),
                        );
                        last_receive_hole_repair_at = Instant::now();
                    }
                    Err(err) if remotes.is_empty() => break Err(err),
                    Err(err) => {
                        eprintln!("warning: reliable receive-hole repair failed: {err}");
                        last_receive_hole_repair_at = Instant::now();
                    }
                }
            }
            _ = tokio::time::sleep_until(stall_deadline), if stall_watch_active => {
                if reliable_relay_product_stall_keeps_stable_same_underlay_subflow_set(
                    &remotes,
                    relay_lane,
                ) {
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
                                last_recv_progress_sent_at = Instant::now();
                            }
                        }
                        Err(err) if reliable_relay_error_is_migratable(&err) => {
                            sender_retry_at = None;
                        }
                        Err(err) => break Err(err),
                    }
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "client_product_stall_keeps_same_underlay_subflow_set",
                        format_args!(
                            "stream_id={} active_underlay={:?} attached_paths={} repair_bytes={} recv_reorder_bytes={} sent_offset={} cause=stable_membership",
                            stream_id.0,
                            remotes.active_path_underlay(),
                            remotes.path_keys().len(),
                            send_stream.repair_bytes(),
                            recv_stream.reorder_bytes(),
                            send_stream.next_offset(),
                        ),
                    );
                    last_response_stall_repair_at = Instant::now();
                    continue;
                }
                if remotes.path_keys().len() <= 1 {
                    if remotes.active_path_underlay().is_some() {
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "client_product_stall_keeps_sole_carrier",
                            format_args!(
                                "stream_id={} active_underlay={:?} repair_bytes={} sent_offset={} cause=avoid_same_path_reopen",
                                stream_id.0,
                                remotes.active_path_underlay(),
                                send_stream.repair_bytes(),
                                send_stream.next_offset(),
                            ),
                        );
                        last_response_stall_repair_at = Instant::now();
                        last_stream_progress_at = Instant::now();
                        continue;
                    }
                    let reannounce_budget = reliable_relay_sole_survivor_reannounce_attempts(
                        reliable_relay_stall_timeout(path_snapshot, relay_lane),
                    );
                    if response_stall_reannounce_attempts < reannounce_budget {
                        response_stall_reannounce_attempts =
                            response_stall_reannounce_attempts.saturating_add(1);
                        match sender
                            .reannounce_active_path(context, &mut remotes, &spec, relay_lane)
                            .await
                        {
                            Ok(()) => {
                                send_stream.update_max_offset(remotes.max_offset());
                                last_stream_progress_at = Instant::now();
                                last_response_stall_repair_at = Instant::now();
                                continue;
                            }
                            Err(err) => {
                                eprintln!(
                                    "warning: reliable stall sole-survivor reannounce failed: {err}"
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
                    match sender
                        .reannounce_active_path(context, &mut remotes, &spec, relay_lane)
                        .await
                    {
                        Ok(()) => {
                            send_stream.update_max_offset(remotes.max_offset());
                            last_stream_progress_at = Instant::now();
                            last_response_stall_repair_at = Instant::now();
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
                            eprintln!("warning: reliable stall survivor reannounce failed: {err}");
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
                let mut dispatched_items = 0usize;
                let mut dispatched_payload_bytes = 0usize;
                let mut blocked_by_carrier = false;
                let mut dispatch_error = None;
                while !sender_queue.is_empty()
                    && dispatched_items < sender_dispatch_item_budget
                    && (dispatched_payload_bytes < sender_dispatch_byte_budget
                        || dispatched_items == 0)
                {
                    match sender
                        .dispatch_client_queued_work(
                            context,
                            &spec,
                            relay_lane,
                            &mut remotes,
                            &mut send_stream,
                            &mut sender_queue,
                            local_open,
                            sender_dispatch_byte_budget.saturating_sub(dispatched_payload_bytes),
                        )
                        .await
                    {
                        Ok(ClientQueuedDispatch::Data { payload_bytes }) => {
                            dispatched_items = dispatched_items.saturating_add(1);
                            dispatched_payload_bytes =
                                dispatched_payload_bytes.saturating_add(payload_bytes);
                            last_stream_progress_at = Instant::now();
                            stats.record_payload_bytes(payload_bytes);
                        }
                        Ok(ClientQueuedDispatch::Repair { payload_bytes }) => {
                            let _ = payload_bytes;
                            dispatched_items = dispatched_items.saturating_add(1);
                            last_stream_progress_at = Instant::now();
                        }
                        Err(RuntimeError::SenderServiceBlocked) => {
                            blocked_by_carrier = true;
                            break;
                        }
                        Err(err) => {
                            dispatch_error = Some(err);
                            break;
                        }
                    }
                }
                #[cfg(feature = "lab-diagnostics")]
                if dispatched_items > 0 {
                    lab_diagnostic(
                        "client_sender_drain",
                        format_args!(
                            "stream_id={} lane={:?} dispatches={} payload_bytes={} byte_budget={} item_budget={} queue_bytes_after={} blocked_by_carrier={}",
                            stream_id.0,
                            relay_lane,
                            dispatched_items,
                            dispatched_payload_bytes,
                            sender_dispatch_byte_budget,
                            sender_dispatch_item_budget,
                            sender_queue.bytes(),
                            blocked_by_carrier,
                        ),
                    );
                }
                if blocked_by_carrier {
                    sender_retry_at =
                        Some(tokio::time::Instant::now() + sender_service_retry_delay(path_snapshot, relay_lane));
                }
                if let Some(err) = dispatch_error {
                    break Err(err);
                }
                if dispatched_items > 0 && (remote_open || send_stream.repair_bytes() > 0) {
                    tokio::task::yield_now().await;
                }
            }
            _ = tokio::time::sleep_until(queued_send_retry_deadline), if queued_send_blocked => {
                sender_retry_at = None;
                continue;
            }
            _ = wait_for_carrier_capacity_notifies(carrier_capacity_notifies), if queued_send_blocked && has_carrier_capacity_notify => {
                sender_retry_at = None;
                continue;
            }
            validation_open = validation_open_rx.recv(), if !pending_validation_opens.is_empty() => {
                let Some(validation_open) = validation_open else {
                    cancel_pending_validation_opens(
                        context,
                        stream_id,
                        &mut pending_validation_opens,
                    );
                    continue;
                };
                pending_validation_opens.remove(&validation_open.key);
                if handle_validation_open_result(
                    context,
                    stream_id,
                    &mut remotes,
                    &mut send_stream,
                    validation_open,
                    pending_validation_opens.len(),
                    &mut last_stream_progress_at,
                )
                .await
                {
                    sender_retry_at = None;
                }
            }
            read = async {
                let read_budget = prospective_read_budget;
                #[cfg(feature = "lab-diagnostics")]
                let read_started = Instant::now();
                let result = read_reliable_relay_payload(&mut local, &mut buf, read_budget).await;
                #[cfg(feature = "lab-diagnostics")]
                if let Ok((read, _)) = &result {
                    lab_perf_record("relay.local_read_wait", read_started.elapsed(), *read);
                }
                result
            }, if can_read_local => {
                let (read, payload) = match read {
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
                    let payload = payload.expect("positive read returns payload");
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "client_sender_enqueue",
                        format_args!(
                            "stream_id={} lane={:?} payload_bytes={} queue_bytes={} queue_limit={} opportunistic=false",
                            stream_id.0,
                            relay_lane,
                            read,
                            sender_queue.bytes().saturating_add(read),
                            sender_queue_limit,
                        ),
                    );
                    sender_queue.push_data(payload);
                    let mut opportunistic_reads = 1usize;
                    while local_open
                        && !relay_lane_is_bulk(relay_lane)
                        && opportunistic_reads < sender_dispatch_item_budget
                        && reliable_relay_can_read_product_source(
                            local_open,
                            false,
                            &send_stream,
                            &sender_queue,
                            context.mux_limits,
                            sender_queue_limit,
                        )
                        && sender_queue.data_bytes() < sender_dispatch_byte_budget
                    {
                        let next_read_budget = reliable_relay_sender_queue_read_budget(
                            &send_stream,
                            &sender_queue,
                            context.mux_limits,
                            sender_queue_limit,
                            source_read_ceiling,
                        );
                        if next_read_budget == 0 {
                            break;
                        }
                        let read = tokio::select! {
                            biased;
                            read = read_reliable_relay_payload(&mut local, &mut buf, next_read_budget) => read,
                            _ = std::future::ready(()) => break,
                        };
                        let (read, payload) = read.map_err(RuntimeError::Io)?;
                        if read == 0 {
                            local_open = false;
                            pending_local_fin = true;
                            break;
                        }
                        if reliable_relay_expects_interactive_response(relay_lane) && remote_open {
                            interactive_response_pending = true;
                        }
                        let payload = payload.expect("positive read returns payload");
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "client_sender_enqueue",
                            format_args!(
                                "stream_id={} lane={:?} payload_bytes={} queue_bytes={} queue_limit={} opportunistic=true",
                                stream_id.0,
                                relay_lane,
                                read,
                                sender_queue.bytes().saturating_add(read),
                                sender_queue_limit,
                            ),
                        );
                        sender_queue.push_data(payload);
                        opportunistic_reads = opportunistic_reads.saturating_add(1);
                    }
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
                                    pending_remote_fin_offset,
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
                                    pending_remote_fin_offset,
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
                            pending_remote_fin_offset,
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
                            && let Err(err) = sender
                                .reannounce_active_path(context, &mut remotes, &spec, relay_lane)
                                .await
                        {
                            eprintln!(
                                "warning: reliable path-error survivor reannounce failed: {err}"
                            );
                        } else if !remotes.is_empty() {
                            send_stream.update_max_offset(remotes.max_offset());
                            last_stream_progress_at = Instant::now();
                            last_response_stall_repair_at = Instant::now();
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
                                    if reliable_relay_should_wait_for_pending_path_recovery(
                                        remote_open,
                                        &pending_validation_opens,
                                    ) {
                                        #[cfg(feature = "lab-diagnostics")]
                                        lab_diagnostic(
                                            "client_path_loss_waits_for_pending_recovery",
                                            format_args!(
                                                "stream_id={} path_underlay={:?} path_index={} pending_validation_opens={} cause=no_immediate_attach",
                                                stream_id.0,
                                                path_key.underlay,
                                                path_key.index,
                                                pending_validation_opens.len(),
                                            ),
                                        );
                                        continue;
                                    }
                                    if reliable_relay_can_finish_after_path_loss(
                                        local_open,
                                        remote_open,
                                        pending_remote_fin_offset,
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
                                    if reliable_relay_should_wait_for_pending_path_recovery(
                                        remote_open,
                                        &pending_validation_opens,
                                    ) {
                                        #[cfg(feature = "lab-diagnostics")]
                                        lab_diagnostic(
                                            "client_path_loss_waits_for_pending_recovery",
                                            format_args!(
                                                "stream_id={} path_underlay={:?} path_index={} pending_validation_opens={} cause=attach_error",
                                                stream_id.0,
                                                path_key.underlay,
                                                path_key.index,
                                                pending_validation_opens.len(),
                                            ),
                                        );
                                        continue;
                                    }
                                    if reliable_relay_can_finish_after_path_loss(
                                        local_open,
                                        remote_open,
                                        pending_remote_fin_offset,
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
                                let ack_summary = recv_stream.ack_range_summary();
                                let hole_state = (
                                    recv_stream.next_offset(),
                                    reorder_bytes,
                                    ack_summary.count,
                                    ack_summary.largest_end,
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
                        }
                        let mut write_error = None;
                        let delivered = outcome.delivered;
                        let delivered_payload_bytes = record_client_response_delivery_accounting(
                            &mut stats,
                            &mut path_stats,
                            path_key,
                            delivered.as_slice(),
                            if delivered_progress { payload_len } else { 0 },
                        );
                        if let Some(path_stat) = path_stats.get(&path_key).copied() {
                            maybe_mark_live_relay_path_delivery(
                                context,
                                path_key,
                                path_stat,
                                &mut path_next_live_sample_bytes,
                            );
                        }
                        #[cfg(feature = "lab-diagnostics")]
                        let write_started = Instant::now();
                        if let Err(err) =
                            write_delivered_payloads(&mut local, delivered.as_slice()).await
                        {
                            write_error = Some(err);
                        }
                        #[cfg(feature = "lab-diagnostics")]
                        lab_perf_record(
                            "relay.local_write_wait",
                            write_started.elapsed(),
                            delivered_payload_bytes,
                        );
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
                                        last_recv_progress_sent_at = Instant::now();
                                    }
                                }
                                Err(err) => break Err(err),
                            }
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
                        let normalized_ranges = normalized_offset_ranges(&ranges);
                        #[cfg(feature = "lab-diagnostics")]
                        let previous_repair_bytes = send_stream.repair_bytes();
                        #[cfg(feature = "lab-diagnostics")]
                        let mux_started = Instant::now();
                        let ack = send_stream.apply_normalized_ack(&normalized_ranges);
                        if ack.released_bytes > 0 {
                            sender.record_owner_progress(ack.released_bytes);
                        }
                        #[cfg(feature = "lab-diagnostics")]
                        lab_perf_record("mux.apply_ack", mux_started.elapsed(), ack.released_bytes);
                        sender.release_normalized_acked_ranges(context, &normalized_ranges);
                        let base_repair_limit = adaptive_reliable_relay_repair_bytes(
                            path_snapshot,
                            relay_lane,
                            context.mux_limits,
                        );
                        let repair_event_budget =
                            sender.repair_extra_event_budget_remaining(context.mux_limits);
                        let repair_limit = base_repair_limit.min(repair_event_budget);
                        let has_multipath_repair_alternative = remotes.path_keys().len() > 1;
                        let ack_gap_repair_ready = ack_gap_repair.repair_ready(
                            complete,
                            &normalized_ranges,
                            remotes.active_path_underlay(),
                            has_multipath_repair_alternative,
                            path_snapshot,
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
                            let fin_tail_limit = if !local_open {
                                let limit = reliable_critical_tail_repair_limit_bytes(
                                    send_stream.repair_bytes(),
                                    context.mux_limits,
                                );
                                critical_tail_repair = reliable_critical_tail_repair_is_over_budget(
                                    repair_event_budget,
                                    limit,
                                );
                                limit
                            } else {
                                repair_limit
                            };
                            let fin_tail_frames = stream_final_offset_tail_repair_frames(
                                &send_stream,
                                &ranges,
                                fin_tail_limit,
                                !local_open,
                                has_multipath_repair_alternative,
                            );
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
                                "stream_id={} complete={} ranges={} largest_end={} released_bytes={} repair_bytes_before={} repair_bytes_after={} repair_frames={} repair_kind={} active_underlay={:?} multipath_repair_alternative={} ack_gap_repair_ready={}",
                                stream_id.0,
                                complete,
                                ranges.len(),
                                ranges.iter().map(|range| range.end).max().unwrap_or(0),
                                ack.released_bytes,
                                previous_repair_bytes,
                                ack.remaining_repair_bytes,
                                repair_frames.len(),
                                repair_kind,
                                remotes.active_path_underlay(),
                                has_multipath_repair_alternative,
                                ack_gap_repair_ready,
                            ),
                        );
                        for frame in repair_frames {
                            let queued = if critical_tail_repair && repair_kind == "fin_tail" {
                                sender.enqueue_critical_repair_frame(
                                    &mut sender_queue,
                                    frame,
                                    RelaySendCause::AckGapRepair,
                                );
                                true
                            } else {
                                sender.enqueue_repair_frame_with_priority(
                                    &mut sender_queue,
                                    frame,
                                    RelaySendCause::AckGapRepair,
                                    context.mux_limits,
                                    true,
                                )
                            };
                            #[cfg(feature = "lab-diagnostics")]
                            lab_diagnostic(
                                "repair",
                                format_args!(
                                    "stream_id={} cause={} queued={}",
                                    stream_id.0, repair_kind, queued,
                                ),
                            );
                            if queued {
                                sender_retry_at = None;
                            }
                        }
                        #[cfg(not(feature = "lab-diagnostics"))]
                        let _ = ack;
                        last_stream_progress_at = Instant::now();
                        if reliable_relay_can_send_pending_fin(
                            pending_local_fin,
                            sender_queue.is_empty(),
                        ) {
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
                        #[cfg(feature = "lab-diagnostics")]
                        let receive_frontier = recv_stream.next_offset();
                        let fin_ready = match receive_stream_fin(
                            &recv_stream,
                            &mut pending_remote_fin_offset,
                            final_offset,
                        ) {
                            Ok(ready) => ready,
                            Err(err) => break Err(err),
                        };
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "client_stream_fin_received",
                            format_args!(
                                "stream_id={} final_offset={} receive_frontier_before={} pending_remote_fin_offset={:?} fin_ready={}",
                                stream_id.0,
                                final_offset,
                                receive_frontier,
                                pending_remote_fin_offset,
                                fin_ready,
                            ),
                        );
                        if fin_ready {
                            last_delivery_progress_at = Instant::now();
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
                                        last_recv_progress_sent_at = Instant::now();
                                    }
                                }
                                Err(err) => break Err(err),
                            }
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
                    Frame::StreamData {
                        stream_id: received_stream_id,
                        offset,
                        payload,
                        ..
                    } if received_stream_id == stream_id
                        && stream_data_range_already_delivered(&recv_stream, offset, payload.len()) =>
                    {
                        match sender
                            .send_recv_progress(
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
                                    last_recv_progress_sent_at = Instant::now();
                                }
                            }
                            Err(err) => break Err(err),
                        }
                    }
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

    let _ = drain_completed_validation_opens(
        context,
        stream_id,
        &mut remotes,
        &mut send_stream,
        &mut pending_validation_opens,
        &mut validation_open_rx,
        &mut last_stream_progress_at,
    )
    .await;
    cancel_pending_validation_opens(context, stream_id, &mut pending_validation_opens);

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
    #[cfg(feature = "lab-diagnostics")]
    lab_diagnostic(
        "client_relay_result",
        format_args!(
            "stream_id={} ok={} local_open={} remote_open={} pending_local_fin={} pending_remote_fin_offset={:?} recv_next_offset={} recv_reorder_bytes={} sender_queue_bytes={} send_repair_bytes={} payload_bytes={}",
            stream_id.0,
            result.is_ok(),
            local_open,
            remote_open,
            pending_local_fin,
            pending_remote_fin_offset,
            recv_stream.next_offset(),
            recv_stream.reorder_bytes(),
            sender_queue.bytes(),
            send_stream.repair_bytes(),
            stats.payload_bytes,
        ),
    );
    #[cfg(feature = "lab-diagnostics")]
    if let Err(err) = &result {
        lab_diagnostic(
            "client_relay_error",
            format_args!("stream_id={} error={:?}", stream_id.0, err),
        );
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

fn record_client_response_delivery_accounting(
    total_stats: &mut PathDeliveryStats,
    path_stats: &mut HashMap<RelayPathKey, PathDeliveryStats>,
    path_key: RelayPathKey,
    delivered: &[Bytes],
    path_scoped_received_bytes: usize,
) -> usize {
    let mut delivered_payload_bytes = 0usize;
    for chunk in delivered {
        total_stats.record_payload_bytes(chunk.len());
        delivered_payload_bytes = delivered_payload_bytes.saturating_add(chunk.len());
    }
    let path_scoped_delivered_bytes = path_scoped_received_bytes.min(delivered_payload_bytes);
    if path_scoped_delivered_bytes > 0 {
        path_stats
            .entry(path_key)
            .or_default()
            .record_payload_bytes(path_scoped_delivered_bytes);
    }
    delivered_payload_bytes
}

fn reliable_relay_can_finish_after_path_loss(
    local_open: bool,
    remote_open: bool,
    pending_remote_fin_offset: Option<u64>,
    send_stream: &ReliableSendStream,
    recv_stream: &ReliableRecvStream,
    sender_queue: &ReliableRelaySenderQueue,
    stats: PathDeliveryStats,
) -> bool {
    !local_open
        && !remote_open
        && pending_remote_fin_offset.is_none()
        && sender_queue.is_empty()
        && send_stream.repair_bytes() == 0
        && recv_stream.reorder_bytes() == 0
        && stats.payload_bytes > 0
}

fn reliable_relay_can_send_pending_fin(pending_local_fin: bool, sender_queue_empty: bool) -> bool {
    pending_local_fin && sender_queue_empty
}

fn reliable_relay_queued_send_blocked_for_retry(
    sender_queue_empty: bool,
    sender_retry_at: Option<tokio::time::Instant>,
    _front_has_carrier_credit: bool,
) -> bool {
    !sender_queue_empty && sender_retry_at.is_some()
}

fn reliable_relay_should_wait_for_pending_path_recovery(
    remote_open: bool,
    pending_validation_opens: &HashMap<RelayPathKey, RelayValidationOpenTask>,
) -> bool {
    remote_open && !pending_validation_opens.is_empty()
}

fn reliable_relay_has_evidenced_bulk_alternative(
    context: &ClientPathContext,
    remotes: &ReliableRelayRemoteSet,
    send_stream: &ReliableSendStream,
) -> bool {
    let payload_bytes =
        reliable_relay_bulk_validation_payload_bytes(send_stream, context.mux_limits);
    context
        .ordered_reliable_bulk_striping_path_keys(payload_bytes)
        .into_iter()
        .any(|key| {
            !remotes.contains_path_key(key)
                && context.relay_path_has_bulk_model_evidence(key.underlay, key.index)
        })
}

async fn handle_validation_open_result(
    context: &ClientPathContext,
    stream_id: StreamId,
    remotes: &mut ReliableRelayRemoteSet,
    send_stream: &mut ReliableSendStream,
    validation_open: RelayValidationOpenResult,
    pending_count: usize,
    last_stream_progress_at: &mut Instant,
) -> bool {
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = (stream_id, pending_count);
    match validation_open.result {
        Ok(opened) => {
            if !remotes.contains_path_key(validation_open.key) {
                remotes.attach_for_validation(opened);
                send_stream.update_max_offset(remotes.max_offset());
                *last_stream_progress_at = Instant::now();
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "relay_validation_open_attached",
                    format_args!(
                        "stream_id={} path_underlay={:?} path_index={} pending={}",
                        stream_id.0,
                        validation_open.key.underlay,
                        validation_open.key.index,
                        pending_count,
                    ),
                );
                true
            } else {
                let lane = opened.stream.lane;
                context.release_relay_path_load(
                    validation_open.key.underlay,
                    validation_open.key.index,
                    lane,
                );
                opened.stream.close().await;
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "relay_validation_open_duplicate_closed",
                    format_args!(
                        "stream_id={} path_underlay={:?} path_index={} lane={:?} pending={}",
                        stream_id.0,
                        validation_open.key.underlay,
                        validation_open.key.index,
                        lane,
                        pending_count,
                    ),
                );
                false
            }
        }
        Err(err) if relay_path_open_error_is_retryable(validation_open.key.underlay, &err) => {
            context
                .mark_relay_path_failure(validation_open.key.underlay, validation_open.key.index);
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "relay_validation_open_failed",
                format_args!(
                    "stream_id={} path_underlay={:?} path_index={} retryable=true error={}",
                    stream_id.0, validation_open.key.underlay, validation_open.key.index, err,
                ),
            );
            false
        }
        Err(err) => {
            #[cfg(not(feature = "lab-diagnostics"))]
            let _ = &err;
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "relay_validation_open_failed",
                format_args!(
                    "stream_id={} path_underlay={:?} path_index={} retryable=false error={}",
                    stream_id.0, validation_open.key.underlay, validation_open.key.index, err,
                ),
            );
            false
        }
    }
}

async fn drain_completed_validation_opens(
    context: &ClientPathContext,
    stream_id: StreamId,
    remotes: &mut ReliableRelayRemoteSet,
    send_stream: &mut ReliableSendStream,
    pending: &mut HashMap<RelayPathKey, RelayValidationOpenTask>,
    validation_open_rx: &mut mpsc::Receiver<RelayValidationOpenResult>,
    last_stream_progress_at: &mut Instant,
) -> bool {
    let mut attached = false;
    while let Ok(validation_open) = validation_open_rx.try_recv() {
        if pending.remove(&validation_open.key).is_none() {
            if let Ok(opened) = validation_open.result {
                opened.stream.close().await;
            }
            continue;
        }
        attached |= handle_validation_open_result(
            context,
            stream_id,
            remotes,
            send_stream,
            validation_open,
            pending.len(),
            last_stream_progress_at,
        )
        .await;
    }
    attached
}

fn cancel_pending_validation_opens(
    context: &ClientPathContext,
    stream_id: StreamId,
    pending: &mut HashMap<RelayPathKey, RelayValidationOpenTask>,
) {
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = stream_id;
    for (key, task) in pending.drain() {
        task.handle.abort();
        context.release_relay_path_load(key.underlay, key.index, task.lane);
        #[cfg(feature = "lab-diagnostics")]
        lab_diagnostic(
            "relay_validation_open_cancelled",
            format_args!(
                "stream_id={} path_underlay={:?} path_index={} lane={:?}",
                stream_id.0, key.underlay, key.index, task.lane,
            ),
        );
    }
}

fn reliable_relay_live_delivery_sample_bytes(mux_limits: MuxLimits) -> u64 {
    reliable_relay_buffer_len(mux_limits) as u64
}

struct RelayValidationOpenResult {
    key: RelayPathKey,
    result: Result<OpenedRemoteStream, RuntimeError>,
}

struct RelayValidationOpenTask {
    lane: FlowLane,
    handle: tokio::task::JoinHandle<()>,
}

fn spawn_reliable_relay_validation_opens(
    context: &ClientPathContext,
    spec: &ReliableRelayOpenSpec,
    lane: FlowLane,
    remotes: &ReliableRelayRemoteSet,
    send_stream: &ReliableSendStream,
    pending: &mut HashMap<RelayPathKey, RelayValidationOpenTask>,
    attempted: &mut std::collections::HashSet<RelayPathKey>,
    result_tx: &mpsc::Sender<RelayValidationOpenResult>,
) -> bool {
    if !relay_lane_is_bulk(lane) {
        return false;
    }
    if !pending.is_empty() {
        return false;
    }
    let stream_id = remotes.stream_id();
    let payload_bytes =
        reliable_relay_bulk_validation_payload_bytes(send_stream, context.mux_limits);
    let mut candidates = context
        .ordered_reliable_bulk_striping_path_keys(payload_bytes)
        .into_iter()
        .chain(context.ordered_reliable_bulk_validation_path_keys(payload_bytes))
        .collect::<Vec<_>>();
    let mut unique_candidates = Vec::with_capacity(candidates.len());
    for candidate in candidates.drain(..) {
        if !unique_candidates.contains(&candidate) {
            unique_candidates.push(candidate);
        }
    }
    let candidates = unique_candidates;
    let candidates = candidates
        .into_iter()
        .filter(|key| !remotes.contains_path_key(*key))
        .collect::<Vec<_>>();
    let candidates = reliable_relay_validation_probe_candidates(candidates, pending, attempted);
    if candidates.is_empty() {
        return false;
    }
    let mut spawned = false;
    for key in candidates {
        match key.underlay {
            UnderlayProtocol::Tcp if context.tcp_paths.get(key.index).is_some() => {
                context.reserve_tcp_path_load(key.index, lane);
            }
            UnderlayProtocol::Udp if context.udp_paths.get(key.index).is_some() => {
                context.reserve_udp_stream_path_load(key.index, lane);
            }
            _ => continue,
        }
        attempted.insert(key);
        let context = context.clone();
        let target = spec.target.clone();
        let ingress = spec.ingress;
        let result_tx = result_tx.clone();
        let handle = tokio::spawn(async move {
            let result = match key.underlay {
                UnderlayProtocol::Tcp => {
                    open_remote_stream_on_reserved_path(
                        &context,
                        stream_id,
                        target,
                        ingress,
                        lane,
                        key.index,
                        StreamOpenRole::Validation,
                    )
                    .await
                }
                UnderlayProtocol::Udp => {
                    open_remote_stream_on_reserved_udp_path(
                        &context,
                        stream_id,
                        target,
                        ingress,
                        lane,
                        key.index,
                        UdpStreamOpenOptions {
                            wait_for_accept: false,
                            role: StreamOpenRole::Validation,
                        },
                    )
                    .await
                }
            };
            if result.is_err() {
                context.release_relay_path_load(key.underlay, key.index, lane);
            }
            let message = RelayValidationOpenResult { key, result };
            if let Err(err) = result_tx.send(message).await {
                let RelayValidationOpenResult { key, result } = err.0;
                if let Ok(opened) = result {
                    let lane = opened.stream.lane;
                    context.release_relay_path_load(key.underlay, key.index, lane);
                    opened.stream.close().await;
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "relay_validation_open_orphan_closed",
                        format_args!(
                            "stream_id={} path_underlay={:?} path_index={} lane={:?}",
                            stream_id.0, key.underlay, key.index, lane,
                        ),
                    );
                }
            }
        });
        pending.insert(key, RelayValidationOpenTask { lane, handle });
        spawned = true;
        #[cfg(feature = "lab-diagnostics")]
        lab_diagnostic(
            "relay_validation_open_spawned",
            format_args!(
                "stream_id={} path_underlay={:?} path_index={} lane={:?} pending={}",
                stream_id.0,
                key.underlay,
                key.index,
                lane,
                pending.len(),
            ),
        );
    }
    spawned
}

fn reliable_relay_validation_probe_candidates(
    candidates: Vec<RelayPathKey>,
    pending: &HashMap<RelayPathKey, RelayValidationOpenTask>,
    attempted: &std::collections::HashSet<RelayPathKey>,
) -> Vec<RelayPathKey> {
    let mut selected = Vec::new();
    for candidate in candidates {
        if pending.contains_key(&candidate)
            || attempted.contains(&candidate)
            || selected.contains(&candidate)
        {
            continue;
        }
        selected.push(candidate);
        break;
    }
    selected
}

pub(super) struct RelayPathAttachRequest<'a> {
    spec: &'a ReliableRelayOpenSpec,
    lane: FlowLane,
    send_stream: &'a ReliableSendStream,
    resend_fin: bool,
    candidates: Vec<RelayPathKey>,
    role: StreamOpenRole,
    send_attach_control: bool,
}

pub(super) async fn attach_relay_path_candidates(
    context: &ClientPathContext,
    remotes: &mut ReliableRelayRemoteSet,
    request: RelayPathAttachRequest<'_>,
) -> Result<usize, RuntimeError> {
    let stream_id = remotes.stream_id();
    let mut last_retryable_error = None;
    let mut attached = 0usize;
    let candidates = request.candidates;

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
                    send_sender_service_attach_control_frames(
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
                        return Ok(attached);
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
        let result = attach_relay_path_candidates(
            context,
            remotes,
            RelayPathAttachRequest {
                spec,
                lane,
                send_stream,
                resend_fin,
                candidates: context.ordered_reliable_bulk_striping_path_keys(payload_bytes),
                role: StreamOpenRole::Validation,
                send_attach_control: false,
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
    let role = if matches!(mode, ReliableRelayAttachMode::BulkStriping) {
        StreamOpenRole::Validation
    } else if reliable_relay_should_race_repair(lane, send_stream, resend_fin, mode) {
        StreamOpenRole::Repair
    } else {
        StreamOpenRole::Active
    };
    if matches!(mode, ReliableRelayAttachMode::Any) && role == StreamOpenRole::Repair {
        return attach_relay_path_candidates(
            context,
            remotes,
            RelayPathAttachRequest {
                spec,
                lane,
                send_stream,
                resend_fin,
                candidates: reliable_relay_repair_path_candidates(
                    context,
                    remotes,
                    lane,
                    payload_bytes,
                ),
                role,
                send_attach_control: true,
            },
        )
        .await;
    }
    attach_relay_path_candidates(
        context,
        remotes,
        RelayPathAttachRequest {
            spec,
            lane,
            send_stream,
            resend_fin,
            candidates: reliable_relay_active_path_candidates(
                context,
                remotes,
                lane,
                payload_bytes,
            ),
            role,
            send_attach_control: true,
        },
    )
    .await
}

pub(super) fn reliable_relay_active_path_candidates(
    context: &ClientPathContext,
    remotes: &ReliableRelayRemoteSet,
    lane: FlowLane,
    payload_bytes: usize,
) -> Vec<RelayPathKey> {
    context
        .ordered_reliable_path_keys(lane, payload_bytes)
        .into_iter()
        .filter(|key| !remotes.contains_path_key(*key))
        .collect()
}

pub(super) fn reliable_relay_repair_path_candidates(
    context: &ClientPathContext,
    remotes: &ReliableRelayRemoteSet,
    lane: FlowLane,
    payload_bytes: usize,
) -> Vec<RelayPathKey> {
    context
        .ordered_reliable_repair_path_keys(
            remotes.active_path_index_for(UnderlayProtocol::Tcp),
            remotes.active_path_index_for(UnderlayProtocol::Udp),
            lane,
            payload_bytes,
        )
        .into_iter()
        .filter(|key| !remotes.contains_path_key(*key))
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
        && (send_stream.repair_bytes() > 0
            || (reliable_relay_expects_interactive_response(lane)
                && send_stream.repair_bytes() <= PATH_OPEN_SCORE_BYTES))
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
    let proof_ceiling = relay_lane_startup_chunk_bytes(FlowLane::Latency, mux_limits);
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

pub(super) fn reliable_relay_sole_survivor_reannounce_attempts(stall_timeout: Duration) -> u32 {
    let _ = stall_timeout;
    QUIC_PERSISTENT_CONGESTION_THRESHOLD
}

pub(super) fn reliable_relay_product_stall_keeps_stable_same_underlay_subflow_set(
    remotes: &ReliableRelayRemoteSet,
    lane: FlowLane,
) -> bool {
    if !relay_lane_is_bulk(lane) || remotes.paths.len() <= 1 {
        return false;
    }
    let Some(first) = remotes.paths.first().map(|path| path.stream.underlay) else {
        return false;
    };
    remotes
        .paths
        .iter()
        .all(|path| path.stream.underlay == first)
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
    if relay_lane_is_bulk(lane)
        && !context.relay_path_has_bulk_service_migration_evidence(
            delivered.underlay,
            delivered.index,
            payload_bytes,
        )
    {
        return false;
    }
    let Some(delivered_eta) = context.reliable_relay_path_eta_ms(delivered, lane, payload_bytes)
    else {
        return false;
    };
    let current_eta = current
        .and_then(|key| context.reliable_relay_path_eta_ms(key, lane, payload_bytes))
        .unwrap_or(f64::INFINITY);
    delivered_eta < current_eta
}

pub(super) fn relay_underlay_identity_order(
    left: UnderlayProtocol,
    right: UnderlayProtocol,
) -> std::cmp::Ordering {
    // Stable identity tie-breaker only. Real scheduling order is decided before
    // this by path metrics and original config ordinal; this must not become a
    // TCP-vs-UDP preference.
    (left as u8).cmp(&(right as u8))
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
    let _ = lane;
    transport_pto_from_snapshot(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};

    fn test_reliable_path_stream(
        stream_id: StreamId,
        underlay: UnderlayProtocol,
        path_index: usize,
        commands: ReliablePathCommandSender,
        lane: FlowLane,
    ) -> ReliablePathStream {
        let (_frames_tx, frames_rx) = mpsc::channel(1);
        ReliablePathStream {
            stream_id,
            max_offset: MuxLimits::default().max_stream_window_bytes,
            lane,
            underlay,
            max_frame_payload_bytes: reliable_relay_buffer_len(MuxLimits::default()),
            output: ReliablePathStreamOutput::fixed(
                underlay,
                PathId(path_index as u16),
                commands,
                MuxLimits::default(),
            ),
            frames: frames_rx,
        }
    }

    #[test]
    fn pending_fin_policy_is_ordered_queue_state_not_carrier_family() {
        assert!(reliable_relay_can_send_pending_fin(true, true));
        assert!(!reliable_relay_can_send_pending_fin(true, false));
        assert!(!reliable_relay_can_send_pending_fin(false, true));
    }

    #[test]
    fn queued_sender_retry_blocks_even_when_carrier_has_capacity() {
        assert!(reliable_relay_queued_send_blocked_for_retry(
            false,
            Some(tokio::time::Instant::now()),
            true,
        ));
        assert!(!reliable_relay_queued_send_blocked_for_retry(
            true,
            Some(tokio::time::Instant::now()),
            true,
        ));
        assert!(!reliable_relay_queued_send_blocked_for_retry(
            false, None, false,
        ));
    }

    #[test]
    fn response_delivery_accounting_credits_current_frame_not_released_buffer() {
        let path_key = RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 1,
        };
        let delivered = [
            Bytes::from_static(&[0; 1024]),
            Bytes::from_static(&[1; 4096]),
        ];
        let mut total = PathDeliveryStats::default();
        let mut path_stats = HashMap::<RelayPathKey, PathDeliveryStats>::new();

        let delivered_bytes = record_client_response_delivery_accounting(
            &mut total,
            &mut path_stats,
            path_key,
            &delivered,
            1024,
        );

        assert_eq!(delivered_bytes, 5120);
        assert_eq!(total.payload_bytes, 5120);
        assert_eq!(
            path_stats.get(&path_key).expect("path stat").payload_bytes,
            1024,
            "hole-closing carrier must not inherit buffered bytes released from other paths"
        );
    }

    #[tokio::test]
    async fn product_stall_keeps_stable_same_underlay_bulk_subflow_set() {
        let (commands_a, _receivers_a) = reliable_path_command_channels(1);
        let (commands_b, _receivers_b) = reliable_path_command_channels(1);
        let first = OpenedRemoteStream {
            path_index: 0,
            stream: test_reliable_path_stream(
                StreamId(1),
                UnderlayProtocol::Udp,
                0,
                commands_a,
                FlowLane::Throughput,
            ),
        };
        let second = OpenedRemoteStream {
            path_index: 1,
            stream: test_reliable_path_stream(
                StreamId(1),
                UnderlayProtocol::Udp,
                1,
                commands_b,
                FlowLane::Throughput,
            ),
        };
        let mut remotes = ReliableRelayRemoteSet::new(first, 4);
        remotes.attach_for_validation(second);

        assert!(
            reliable_relay_product_stall_keeps_stable_same_underlay_subflow_set(
                &remotes,
                FlowLane::Throughput,
            )
        );
        assert!(
            !reliable_relay_product_stall_keeps_stable_same_underlay_subflow_set(
                &remotes,
                FlowLane::Latency,
            )
        );
    }

    #[test]
    fn validation_probe_candidates_are_one_shot_per_stream_path() {
        let tcp0 = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        };
        let udp0 = RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        };
        let candidates = vec![tcp0, udp0, tcp0];
        let pending = HashMap::<RelayPathKey, RelayValidationOpenTask>::new();
        let mut attempted = HashSet::from([tcp0]);
        let selected = reliable_relay_validation_probe_candidates(candidates, &pending, &attempted);

        assert_eq!(
            selected,
            vec![udp0],
            "validation/probe attachment is path-scoped and must not reopen a path already attempted for this product stream"
        );

        attempted.insert(udp0);
        assert!(
            reliable_relay_validation_probe_candidates(vec![tcp0, udp0], &pending, &attempted)
                .is_empty(),
            "rebalance cannot turn a closed validation handle into repeated OPEN_STREAM churn"
        );
    }
}
