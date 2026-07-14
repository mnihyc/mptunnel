use super::*;
use crate::model::admission::bulk_service_feed_reservoir_payload_bytes;

fn reliable_relay_request_outstanding_resource_ceiling(mux_limits: MuxLimits) -> usize {
    let stream_window = usize::try_from(mux_limits.max_stream_window_bytes).unwrap_or(usize::MAX);
    mux_limits
        .max_repair_bytes
        .min(mux_limits.max_path_flight_bytes)
        .min(stream_window)
        .max(1)
}

pub(in crate::runtime) fn reliable_relay_client_dispatch_payload_limit(
    adaptive_chunk_bytes: usize,
    remaining_pass_bytes: usize,
) -> usize {
    adaptive_chunk_bytes.min(remaining_pass_bytes).max(1)
}

#[derive(Debug)]
struct ReliableRelayRequestOutstandingWindow {
    service_epoch_instance: Option<RelayPathInstance>,
    product_limit_bytes: usize,
    growth_epoch_at: Instant,
    acked_in_epoch: usize,
}

impl ReliableRelayRequestOutstandingWindow {
    fn new() -> Self {
        Self::new_at(Instant::now())
    }

    fn new_at(now: Instant) -> Self {
        Self {
            service_epoch_instance: None,
            product_limit_bytes: 0,
            growth_epoch_at: now,
            acked_in_epoch: 0,
        }
    }

    fn limit_bytes(
        &mut self,
        service_instance: Option<RelayPathInstance>,
        lane: FlowLane,
        payload_bytes: usize,
        mux_limits: MuxLimits,
    ) -> usize {
        self.limit_bytes_at(
            service_instance,
            lane,
            payload_bytes,
            mux_limits,
            Instant::now(),
        )
    }

    fn resolved_service_instance(
        &self,
        ordered_service: Option<RelayPathInstance>,
        pre_owner_active: Option<RelayPathInstance>,
    ) -> Option<RelayPathInstance> {
        ordered_service.or_else(|| {
            self.service_epoch_instance
                .is_none()
                .then_some(pre_owner_active)
                .flatten()
        })
    }

    fn limit_bytes_at(
        &mut self,
        service_instance: Option<RelayPathInstance>,
        lane: FlowLane,
        payload_bytes: usize,
        mux_limits: MuxLimits,
        now: Instant,
    ) -> usize {
        let resource_ceiling = reliable_relay_request_outstanding_resource_ceiling(mux_limits);
        let startup_reservoir = if lane.is_bulk() {
            bulk_service_feed_reservoir_payload_bytes(payload_bytes, mux_limits)
        } else {
            // Flow classification already expects one full source queue.
            // Keeping this reservoir avoids turning the 14.6 KiB latency
            // probe into a stop-and-wait prerequisite for sustained upload.
            reliable_relay_buffer_len(mux_limits)
        }
        .min(resource_ceiling)
        .max(1);
        if service_instance.is_none() && self.service_epoch_instance.is_some() {
            // Demotion still closes read-ahead during recovery, but promotion
            // cannot manufacture a larger allowance without a Service epoch.
            if !lane.is_bulk() && self.product_limit_bytes > startup_reservoir {
                self.product_limit_bytes = startup_reservoir;
                self.growth_epoch_at = now;
                self.acked_in_epoch = 0;
            }
            return self.product_limit_bytes.min(resource_ceiling).max(1);
        }
        if let Some(instance) = service_instance
            && self.service_epoch_instance != Some(instance)
        {
            // Product admission is unified above the carriers, but its ACK
            // epoch belongs to one exact Service association. Handoffs across
            // either protocol start from the same bounded reservoir.
            self.service_epoch_instance = Some(instance);
            self.product_limit_bytes = 0;
            self.growth_epoch_at = now;
            self.acked_in_epoch = 0;
        }
        // Retain a bounded allowance while Service placement is temporarily
        // absent so recovery cannot reopen source read-ahead.
        let lane_demoted = !lane.is_bulk() && self.product_limit_bytes > startup_reservoir;
        if lane_demoted || self.product_limit_bytes < startup_reservoir {
            self.product_limit_bytes = startup_reservoir;
            self.growth_epoch_at = now;
            self.acked_in_epoch = 0;
        }
        self.product_limit_bytes.min(resource_ceiling).max(1)
    }

    fn record_acked(
        &mut self,
        released_bytes: usize,
        owner_instance: RelayPathInstance,
        service_instance: Option<RelayPathInstance>,
        owner_capable: bool,
        lane: FlowLane,
        growth_interval: Duration,
        mux_limits: MuxLimits,
    ) {
        self.record_acked_at(
            released_bytes,
            owner_instance,
            service_instance,
            owner_capable,
            lane,
            growth_interval,
            mux_limits,
            Instant::now(),
        );
    }

    fn record_acked_at(
        &mut self,
        released_bytes: usize,
        owner_instance: RelayPathInstance,
        service_instance: Option<RelayPathInstance>,
        owner_capable: bool,
        lane: FlowLane,
        growth_interval: Duration,
        mux_limits: MuxLimits,
        now: Instant,
    ) {
        let Some(service_instance) = service_instance else {
            return;
        };
        if released_bytes == 0
            || !owner_capable
            || service_instance.key.underlay != owner_instance.key.underlay
            || self.service_epoch_instance != Some(service_instance)
            || !lane.is_bulk()
        {
            return;
        }
        let resource_ceiling = reliable_relay_request_outstanding_resource_ceiling(mux_limits);
        if self.product_limit_bytes == 0 || self.product_limit_bytes >= resource_ceiling {
            return;
        }
        let growth_interval = growth_interval.max(QUIC_TIMER_GRANULARITY);
        if now.saturating_duration_since(self.growth_epoch_at) > growth_interval {
            self.growth_epoch_at = now;
            self.acked_in_epoch = 0;
            return;
        }
        self.acked_in_epoch = self.acked_in_epoch.saturating_add(released_bytes);
        let durable_product_floor =
            usize::try_from(reliable_subflow_startup_sample_limit_bytes(mux_limits))
                .unwrap_or(usize::MAX)
                .min(self.product_limit_bytes);
        let growth_threshold = self
            .product_limit_bytes
            .div_ceil(2)
            .max(durable_product_floor)
            .max(1);
        if self.acked_in_epoch < growth_threshold {
            return;
        }
        self.product_limit_bytes = self
            .product_limit_bytes
            .saturating_mul(2)
            .min(resource_ceiling)
            .max(1);
        self.growth_epoch_at = now;
        self.acked_in_epoch = 0;
    }

    fn record_tcp_ack_clock_turnover(
        &mut self,
        turnover_bytes: usize,
        service_instance: Option<RelayPathInstance>,
        lane: FlowLane,
        mux_limits: MuxLimits,
    ) {
        let Some(service_instance) = service_instance else {
            return;
        };
        if service_instance.key.underlay != UnderlayProtocol::Tcp
            || self.service_epoch_instance != Some(service_instance)
            || !lane.is_bulk()
        {
            return;
        }
        let resource_ceiling = reliable_relay_request_outstanding_resource_ceiling(mux_limits);
        if self.product_limit_bytes == 0 || self.product_limit_bytes >= resource_ceiling {
            return;
        }
        let durable_product_floor =
            usize::try_from(reliable_subflow_startup_sample_limit_bytes(mux_limits))
                .unwrap_or(usize::MAX)
                .min(self.product_limit_bytes);
        let next_limit = reliable_relay_request_tcp_product_limit_for_turnover(
            self.product_limit_bytes,
            turnover_bytes,
            durable_product_floor,
            resource_ceiling,
        );
        if next_limit > self.product_limit_bytes {
            // TCP uses freshness-bounded per-owner pipe samples. The shared
            // product window only quantizes their aggregate into bounded
            // stages; calibration and lifecycle code own path authority.
            self.product_limit_bytes = next_limit;
            self.growth_epoch_at = Instant::now();
            self.acked_in_epoch = 0;
        }
    }
}

fn reliable_relay_request_tcp_product_limit_for_turnover(
    current_limit: usize,
    turnover_bytes: usize,
    durable_product_floor: usize,
    resource_ceiling: usize,
) -> usize {
    let mut limit = current_limit.max(1).min(resource_ceiling.max(1));
    while limit < resource_ceiling {
        let threshold = limit.div_ceil(2).max(durable_product_floor).max(1);
        if turnover_bytes < threshold {
            break;
        }
        limit = limit.saturating_mul(2).min(resource_ceiling).max(1);
    }
    limit
}

fn reliable_relay_request_outstanding_headroom_bytes(
    send_stream: &ReliableSendStream,
    sender_queue: &ReliableRelaySenderQueue,
    outstanding_limit_bytes: usize,
) -> usize {
    outstanding_limit_bytes.saturating_sub(
        send_stream
            .repair_bytes()
            .saturating_add(sender_queue.data_bytes()),
    )
}

fn reliable_relay_request_ack_growth_interval(
    service_instance: RelayPathInstance,
    context: &ClientPathContext,
) -> Duration {
    context
        .reliable_path_snapshot(service_instance.key)
        .map(|snapshot| transport_pto_from_snapshot(Some(snapshot)))
        .unwrap_or_else(|| transport_pto_from_snapshot(None))
}

fn reliable_tcp_service_request_bulk_flow_is_active(
    local_open: bool,
    request_observed_bytes: u64,
    bulk_threshold_bytes: u64,
    queued_data_bytes: usize,
    outstanding_data_bytes: usize,
) -> bool {
    local_open
        && request_observed_bytes >= bulk_threshold_bytes
        && (queued_data_bytes > 0 || outstanding_data_bytes > 0)
}

fn update_tcp_service_request_bulk_flow_registration(
    registration: &ReliableTcpRequestBulkFlowRegistration,
    sender: &RelaySenderService,
    remotes: &ReliableRelayRemoteSet,
    send_stream: &ReliableSendStream,
    sender_queue: &ReliableRelaySenderQueue,
    local_open: bool,
    path_snapshot: Option<PathSnapshot>,
    mux_limits: MuxLimits,
) {
    let request_observed_bytes = send_stream
        .next_offset()
        .saturating_add(sender_queue.data_bytes() as u64);
    let request_bulk_active = reliable_tcp_service_request_bulk_flow_is_active(
        local_open,
        request_observed_bytes,
        reliable_flow_bulk_threshold_bytes(path_snapshot, mux_limits),
        sender_queue.data_bytes(),
        send_stream.repair_bytes(),
    );
    let service_underlay = sender
        .request_ordered_service_instance()
        .filter(|service| remotes.contains_path_instance(*service))
        .map(|service| service.key.underlay);
    registration.update(request_bulk_active, service_underlay);
}

pub(in crate::runtime) async fn relay_migrating_tcp_stream<S>(
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
    let mut local_fin_sent = false;
    let mut terminal_fin_replayed = false;
    let mut pending_remote_fin_offset = None;
    let mut stats = PathDeliveryStats::default();
    let mut path_stats = HashMap::<RelayPathKey, PathDeliveryStats>::new();
    let mut path_next_live_sample_bytes = HashMap::<RelayPathKey, u64>::new();
    let mut sender = RelaySenderService::new(stream_id);
    let mut request_outstanding_window = ReliableRelayRequestOutstandingWindow::new();
    let mut flow_demand = ReliableRelayFlowDemandTracker::new();
    let mut request_flow_demand = ReliableRelayFlowDemandTracker::new();
    let request_bulk_flow = context.reliable_tcp_request_bulk_flow_registration();
    sender.bind_request_bulk_flow_registration(request_bulk_flow.clone());
    let mut last_stream_progress_at = Instant::now();
    let mut last_delivery_progress_at = Instant::now();
    let mut last_response_stall_repair_at = Instant::now();
    let mut last_product_stall_attempt_at = None;
    let mut last_receive_hole_repair_at = Instant::now();
    let mut receive_hole_repair_attempts = 0_u32;
    let mut interactive_response_pending = false;
    let mut recv_progress = ReliableRecvProgress::default();
    let mut ack_gap_repair = ReliableAckGapRepairProgress::default();
    let mut last_recv_progress_sent_at = Instant::now();
    let mut last_send_ack_frontier = 0_u64;
    let mut last_send_ack_ranges = Vec::<OffsetRange>::new();
    let mut last_send_ack_complete = false;
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
    let mut validation_open_attempts = HashMap::<RelayPathKey, u8>::new();
    let mut recovery_excluded_paths = HashSet::<RelayPathKey>::new();
    #[cfg(feature = "lab-diagnostics")]
    let mut last_reported_budget: Option<(FlowLane, usize, usize)> = None;
    #[cfg(feature = "lab-diagnostics")]
    let mut last_reported_read_block: Option<(usize, usize, usize, usize, usize)> = None;
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
        let request_observed_bytes = send_stream
            .next_offset()
            .saturating_add(sender_queue.data_bytes() as u64);
        let request_demand_update = request_flow_demand.refresh(
            ReliableRelayFlowSignals::new(request_observed_bytes, 0, 0),
            path_snapshot,
            context.mux_limits,
        );
        let request_lane = request_demand_update.demand.lane;
        #[cfg(feature = "lab-diagnostics")]
        if reliable_relay_lane_changed(request_demand_update.previous_lane, request_lane) {
            lab_diagnostic(
                "client_request_lane_changed",
                format_args!(
                    "stream_id={} previous={:?} lane={:?} observed_bytes={} send_rate_mbps={:.3} relay_lane={:?}",
                    stream_id.0,
                    request_demand_update.previous_lane,
                    request_lane,
                    request_demand_update.observed_bytes,
                    request_demand_update.send_rate_bps / 1_000_000.0,
                    relay_lane,
                ),
            );
        }
        update_tcp_service_request_bulk_flow_registration(
            &request_bulk_flow,
            &sender,
            &remotes,
            &send_stream,
            &sender_queue,
            local_open,
            path_snapshot,
            context.mux_limits,
        );
        for failed_instance in sender.unreported_missing_owner_instances(
            &remotes,
            reliable_relay_stall_timeout(path_snapshot, relay_lane),
        ) {
            if sender.enqueue_failed_path_instance_gap_repairs(
                &mut sender_queue,
                context,
                &remotes,
                &send_stream,
                failed_instance,
                relay_lane,
            ) {
                sender_retry_at = None;
            }
        }
        if reliable_relay_lane_changed(demand_update.previous_lane, relay_lane) {
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "client_stream_lane_changed",
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
            for key in remotes.load_reserved_path_keys() {
                context.change_relay_path_lane_load(
                    key.underlay,
                    key.index,
                    demand_update.previous_lane,
                    relay_lane,
                );
            }
            remotes.set_lane(relay_lane);
        }
        if demand_update.prevalidate_bulk && !relay_lane.is_bulk() {
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
                &mut validation_open_attempts,
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
            if relay_lane.is_bulk() {
                if spawn_reliable_relay_validation_opens(
                    context,
                    &spec,
                    relay_lane,
                    &remotes,
                    &send_stream,
                    &mut pending_validation_opens,
                    &mut validation_open_attempts,
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
                &pending_validation_opens,
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
        let request_service_instance = request_outstanding_window.resolved_service_instance(
            sender
                .request_ordered_service_instance()
                .filter(|service| remotes.contains_path_instance(*service)),
            remotes.active_path_instance(),
        );
        let request_outstanding_limit = request_outstanding_window.limit_bytes(
            request_service_instance,
            relay_lane,
            adaptive_chunk,
            context.mux_limits,
        );
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
            interactive_response_pending,
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
        let stall_deadline = reliable_relay_product_stall_deadline(
            stall_progress_anchor,
            last_product_stall_attempt_at,
            path_snapshot,
            relay_lane,
        );
        let recv_progress_deadline = tokio::time::Instant::from_std(
            last_recv_progress_sent_at
                + reliable_stream_recv_progress_interval(path_snapshot, relay_lane),
        );
        if sender_retry_at.is_some_and(|deadline| deadline <= tokio::time::Instant::now()) {
            sender_retry_at = None;
        }
        sender.discard_unusable_live_owner_tail_repairs(&mut sender_queue, &remotes);
        if sender.discard_stale_persistent_ack_gap_repairs(&mut sender_queue, &remotes) > 0 {
            ack_gap_repair.release_repair_attempt();
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
        let request_outstanding_headroom = reliable_relay_request_outstanding_headroom_bytes(
            &send_stream,
            &sender_queue,
            request_outstanding_limit,
        );
        let can_read_by_flow = can_read_by_flow && request_outstanding_headroom > 0;
        let prospective_read_budget = if can_read_by_flow {
            reliable_relay_sender_queue_read_budget(
                &send_stream,
                &sender_queue,
                context.mux_limits,
                sender_queue_limit,
                source_read_ceiling,
            )
            .min(request_outstanding_headroom)
        } else {
            0
        };
        let can_read_local =
            can_read_by_flow && prospective_read_budget > 0 && !inbound_frame_ready;
        let can_send_pending_fin =
            reliable_relay_can_send_pending_fin(pending_local_fin, sender_queue.is_empty());
        let terminal_fin_replay_ready = stream_terminal_fin_replay_required(
            local_fin_sent,
            terminal_fin_replayed,
            sender_queue.is_empty(),
            send_stream.repair_bytes(),
            last_send_ack_frontier,
            send_stream.next_offset(),
        );
        #[cfg(feature = "lab-diagnostics")]
        {
            if local_open && !can_read_local {
                let blocked_state = (
                    send_stream.repair_bytes(),
                    send_stream.send_credit_bytes(),
                    adaptive_inflight,
                    request_outstanding_limit,
                    request_outstanding_headroom,
                );
                if last_reported_read_block != Some(blocked_state) {
                    lab_diagnostic(
                        "relay_local_read_blocked",
                        format_args!(
                            "stream_id={} lane={:?} repair_bytes={} send_credit_bytes={} inflight_limit={} request_outstanding_limit={} request_outstanding_headroom={} sent_offset={} received_offset={}",
                            stream_id.0,
                            relay_lane,
                            blocked_state.0,
                            blocked_state.1,
                            blocked_state.2,
                            blocked_state.3,
                            blocked_state.4,
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
                match attach_reliable_relay_paths_with_recovery_exclusions(
                    context,
                    &spec,
                    relay_lane,
                    &mut remotes,
                    &send_stream,
                    !local_open,
                    ReliableRelayAttachMode::RecoveryRepair,
                    &mut recovery_excluded_paths,
                    &pending_validation_opens,
                )
                .await
                {
                    Ok(attached) if attached > 0 => {
                        sender_retry_at = None;
                        send_stream.update_max_offset(remotes.max_offset());
                        match sender
                            .send_recv_progress(
                                &mut remotes,
                                context,
                                &recv_stream,
                                &mut recv_progress,
                                RelayRecvProgressSend::new(path_snapshot, relay_lane, true)
                                    .recover_stalled_service(),
                            )
                            .await
                        {
                            Ok(sent) => {
                                if sent {
                                    last_recv_progress_sent_at = Instant::now();
                                    last_stream_progress_at = Instant::now();
                                }
                            }
                            Err(err) if reliable_relay_error_is_migratable(&err) => {}
                            Err(err) => break Err(err),
                        }
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
                                RelayRecvProgressSend::new(path_snapshot, relay_lane, true)
                                    .recover_stalled_service(),
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
                                match attach_reliable_relay_paths_with_recovery_exclusions(
                                    context,
                                    &spec,
                                    relay_lane,
                                    &mut remotes,
                                    &send_stream,
                                    !local_open,
                                    ReliableRelayAttachMode::RecoveryRepair,
                                    &mut recovery_excluded_paths,
                                    &pending_validation_opens,
                                )
                                .await
                                {
                                    Ok(attached) if attached > 0 => {
                                        sender_retry_at = None;
                                        send_stream.update_max_offset(remotes.max_offset());
                                        match sender
                                            .send_recv_progress(
                                                &mut remotes,
                                                context,
                                                &recv_stream,
                                                &mut recv_progress,
                                                RelayRecvProgressSend::new(
                                                    path_snapshot,
                                                    relay_lane,
                                                    true,
                                                )
                                                .recover_stalled_service(),
                                            )
                                            .await
                                        {
                                            Ok(sent) => {
                                                if sent {
                                                    last_recv_progress_sent_at = Instant::now();
                                                }
                                            }
                                            Err(recovery_err)
                                                if reliable_relay_error_is_migratable(
                                                    &recovery_err,
                                                ) => {}
                                            Err(recovery_err) => break Err(recovery_err),
                                        }
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
                let queued_existing_tail_repair = sender.enqueue_live_owner_tail_repair(
                    &mut sender_queue,
                    context,
                    &remotes,
                    &send_stream,
                    &last_send_ack_ranges,
                    last_send_ack_complete,
                    last_send_ack_frontier,
                    relay_lane,
                );
                if queued_existing_tail_repair
                    || reliable_relay_product_stall_preserves_attached_path_set(&remotes)
                {
                    if queued_existing_tail_repair {
                        sender_retry_at = None;
                    }
                    match sender.send_recv_progress(
                        &mut remotes,
                        context,
                        &recv_stream,
                        &mut recv_progress,
                        RelayRecvProgressSend::new(path_snapshot, relay_lane, true)
                            .recover_stalled_service(),
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
                        "client_product_stall_keeps_attached_path_set",
                        format_args!(
                            "stream_id={} active_underlay={:?} attached_paths={} repair_bytes={} recv_reorder_bytes={} sent_offset={} cause=product_stall_only",
                            stream_id.0,
                            remotes.active_path_underlay(),
                            remotes.path_keys().len(),
                            send_stream.repair_bytes(),
                            recv_stream.reorder_bytes(),
                            send_stream.next_offset(),
                        ),
                    );
                    last_response_stall_repair_at = Instant::now();
                    last_product_stall_attempt_at = Some(Instant::now());
                    continue;
                }
                if reliable_relay_product_stall_should_try_alternate_attach(&remotes) {
                    match attach_reliable_relay_paths_with_recovery_exclusions(
                        context,
                        &spec,
                        relay_lane,
                        &mut remotes,
                        &send_stream,
                        !local_open,
                        ReliableRelayAttachMode::RecoveryRepair,
                        &mut recovery_excluded_paths,
                        &pending_validation_opens,
                    )
                    .await
                    {
                        Ok(attached) if attached > 0 => {
                            sender_retry_at = None;
                            send_stream.update_max_offset(remotes.max_offset());
                            if sender.enqueue_live_owner_tail_repair(
                                &mut sender_queue,
                                context,
                                &remotes,
                                &send_stream,
                                &last_send_ack_ranges,
                                last_send_ack_complete,
                                last_send_ack_frontier,
                                relay_lane,
                            ) {
                                sender_retry_at = None;
                            }
                            match sender
                                .send_recv_progress(
                                    &mut remotes,
                                    context,
                                    &recv_stream,
                                    &mut recv_progress,
                                    RelayRecvProgressSend::new(path_snapshot, relay_lane, true)
                                        .recover_stalled_service(),
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
                            last_stream_progress_at = Instant::now();
                            last_response_stall_repair_at = Instant::now();
                            last_product_stall_attempt_at = Some(Instant::now());
                            #[cfg(feature = "lab-diagnostics")]
                            lab_diagnostic(
                                "client_product_stall_attached_alternate",
                                format_args!(
                                    "stream_id={} active_underlay={:?} attached_paths={} repair_bytes={} sent_offset={}",
                                    stream_id.0,
                                    remotes.active_path_underlay(),
                                    remotes.path_keys().len(),
                                    send_stream.repair_bytes(),
                                    send_stream.next_offset(),
                                ),
                            );
                            continue;
                        }
                        Ok(_) => {
                            #[cfg(feature = "lab-diagnostics")]
                            lab_diagnostic(
                                "client_product_stall_attach_alternate_unavailable",
                                format_args!(
                                    "stream_id={} active_underlay={:?} repair_bytes={} sent_offset={} cause=no_candidate",
                                    stream_id.0,
                                    remotes.active_path_underlay(),
                                    send_stream.repair_bytes(),
                                    send_stream.next_offset(),
                                ),
                            );
                        }
                        Err(err) => {
                            #[cfg(not(feature = "lab-diagnostics"))]
                            let _ = &err;
                            #[cfg(feature = "lab-diagnostics")]
                            lab_diagnostic(
                                "client_product_stall_attach_alternate_unavailable",
                                format_args!(
                                    "stream_id={} active_underlay={:?} repair_bytes={} sent_offset={} cause=attach_error error={}",
                                    stream_id.0,
                                    remotes.active_path_underlay(),
                                    send_stream.repair_bytes(),
                                    send_stream.next_offset(),
                                    err,
                                ),
                            );
                        }
                    }
                    if remotes.path_keys().len() <= 1 && remotes.active_path_underlay().is_some() {
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
                        last_product_stall_attempt_at = Some(Instant::now());
                        continue;
                    }
                }
                match sender.send_recv_progress(
                    &mut remotes,
                    context,
                    &recv_stream,
                    &mut recv_progress,
                    RelayRecvProgressSend::new(path_snapshot, relay_lane, true)
                        .recover_stalled_service(),
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
                        match attach_reliable_relay_paths_with_recovery_exclusions(
                            context,
                            &spec,
                            relay_lane,
                            &mut remotes,
                            &send_stream,
                            !local_open,
                            ReliableRelayAttachMode::Any,
                            &mut recovery_excluded_paths,
                            &pending_validation_opens,
                        )
                        .await
                        {
                            Ok(attached) if attached > 0 => {
                                send_stream.update_max_offset(remotes.max_offset());
                                match sender
                                    .send_recv_progress(
                                        &mut remotes,
                                        context,
                                        &recv_stream,
                                        &mut recv_progress,
                                        RelayRecvProgressSend::new(
                                            path_snapshot,
                                            relay_lane,
                                            true,
                                        )
                                        .recover_stalled_service(),
                                    )
                                    .await
                                {
                                    Ok(sent) => {
                                        if sent {
                                            last_recv_progress_sent_at = Instant::now();
                                        }
                                    }
                                    Err(recovery_err)
                                        if reliable_relay_error_is_migratable(&recovery_err) => {}
                                    Err(recovery_err) => break Err(recovery_err),
                                }
                            }
                            Ok(_) => break Err(err),
                            Err(err) => break Err(err),
                        }
                    }
                    Err(err) => break Err(err),
                }
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "client_product_stall_keeps_carrier_membership",
                    format_args!(
                        "stream_id={} active_underlay={:?} attached_paths={} repair_bytes={} recv_reorder_bytes={} sent_offset={} cause=product_stall_only",
                        stream_id.0,
                        remotes.active_path_underlay(),
                        remotes.path_keys().len(),
                        send_stream.repair_bytes(),
                        recv_stream.reorder_bytes(),
                        send_stream.next_offset(),
                    ),
                );
                last_response_stall_repair_at = Instant::now();
                last_product_stall_attempt_at = Some(Instant::now());
            }
            _ = tokio::time::sleep_until(recv_progress_deadline), if remotes.path_keys().len() > 1
                && reliable_relay_recv_progress_resend_active(
                    &recv_stream,
                    remote_open,
                    remotes.active_path_underlay(),
                ) => {
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
                        match attach_reliable_relay_paths_with_recovery_exclusions(
                            context,
                            &spec,
                            relay_lane,
                            &mut remotes,
                            &send_stream,
                            !local_open,
                            ReliableRelayAttachMode::Any,
                            &mut recovery_excluded_paths,
                            &pending_validation_opens,
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
                        local_fin_sent = true;
                        terminal_fin_replayed = false;
                        last_stream_progress_at = Instant::now();
                    }
                    Err(err) if reliable_relay_error_is_migratable(&err) => {
                        match attach_reliable_relay_paths_with_recovery_exclusions(
                            context,
                            &spec,
                            relay_lane,
                            &mut remotes,
                            &send_stream,
                            true,
                            ReliableRelayAttachMode::Any,
                            &mut recovery_excluded_paths,
                            &pending_validation_opens,
                        )
                        .await
                        {
                            Ok(attached) if attached > 0 => {
                                sender_retry_at = None;
                                pending_local_fin = false;
                                local_fin_sent = true;
                                terminal_fin_replayed = false;
                                last_stream_progress_at = Instant::now();
                            }
                            Ok(_) => break Err(err),
                            Err(err) => break Err(err),
                        }
                    }
                    Err(err) => break Err(err),
                }
            }
            _ = std::future::ready(()), if terminal_fin_replay_ready => {
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
                        terminal_fin_replayed = true;
                        last_stream_progress_at = Instant::now();
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "terminal_fin_replay",
                            format_args!(
                                "stream_id={} final_offset={} ack_frontier={} repair_bytes=0 role=client",
                                stream_id.0,
                                send_stream.next_offset(),
                                last_send_ack_frontier,
                            ),
                        );
                    }
                    Err(err) if reliable_relay_error_is_migratable(&err) => {
                        match attach_reliable_relay_paths_with_recovery_exclusions(
                            context,
                            &spec,
                            relay_lane,
                            &mut remotes,
                            &send_stream,
                            true,
                            ReliableRelayAttachMode::Any,
                            &mut recovery_excluded_paths,
                            &pending_validation_opens,
                        )
                        .await
                        {
                            Ok(attached) if attached > 0 => {
                                sender_retry_at = None;
                                local_fin_sent = true;
                                terminal_fin_replayed = true;
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
                let inflight_path_claims = pending_validation_opens
                    .keys()
                    .copied()
                    .collect::<HashSet<_>>();
                while !sender_queue.is_empty()
                    && dispatched_items < sender_dispatch_item_budget
                    && (dispatched_payload_bytes < sender_dispatch_byte_budget
                        || dispatched_items == 0)
                {
                    let dispatch = sender
                        .dispatch_client_queued_work(
                            context,
                            &spec,
                            relay_lane,
                            request_lane,
                            &mut remotes,
                            &mut send_stream,
                            &mut sender_queue,
                            local_open,
                            &inflight_path_claims,
                            reliable_relay_client_dispatch_payload_limit(
                                adaptive_chunk,
                                sender_dispatch_byte_budget
                                    .saturating_sub(dispatched_payload_bytes),
                            ),
                        )
                        .await;
                    // A successful Service dispatch may commit a carrier-family
                    // handoff. Publish it before another item in this batch.
                    update_tcp_service_request_bulk_flow_registration(
                        &request_bulk_flow,
                        &sender,
                        &remotes,
                        &send_stream,
                        &sender_queue,
                        local_open,
                        path_snapshot,
                        context.mux_limits,
                    );
                    match dispatch {
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
                        Ok(ClientQueuedDispatch::RepairDeferred) => {
                            dispatched_items = dispatched_items.saturating_add(1);
                        }
                        Ok(ClientQueuedDispatch::PersistentRepairCancelled) => {
                            ack_gap_repair.release_repair_attempt();
                            sender_retry_at = None;
                            dispatched_items = dispatched_items.saturating_add(1);
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
                    cancel_pending_validation_opens(stream_id, &mut pending_validation_opens);
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
                        last_response_stall_repair_at = Instant::now();
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
                        && !relay_lane.is_bulk()
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
                            last_response_stall_repair_at = Instant::now();
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
                        match attach_reliable_relay_paths_with_recovery_exclusions(
                            context,
                            &spec,
                            relay_lane,
                            &mut remotes,
                            &send_stream,
                            !local_open,
                            ReliableRelayAttachMode::Any,
                            &mut recovery_excluded_paths,
                            &pending_validation_opens,
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
                        sender
                            .fail_client_path_instance(context, &mut remotes, instance)
                            .await;
                        recovery_excluded_paths.insert(path_key);
                        match recover_reliable_relay_after_path_failure(
                            &mut sender,
                            &mut sender_queue,
                            context,
                            &spec,
                            relay_lane,
                            &mut remotes,
                            &mut send_stream,
                            instance,
                        )
                        .await
                        {
                            Ok(Some(repair_queued)) => {
                                last_stream_progress_at = Instant::now();
                                last_response_stall_repair_at = Instant::now();
                                if repair_queued {
                                    sender_retry_at = None;
                                }
                            }
                            Ok(None) => {}
                            Err(err) => {
                                eprintln!(
                                    "warning: reliable path-error survivor reannounce failed: {err}"
                                );
                            }
                        }
                        if remotes.is_empty() {
                            match attach_reliable_relay_paths_with_recovery_exclusions(
                                context,
                                &spec,
                                relay_lane,
                                &mut remotes,
                                &send_stream,
                                !local_open,
                                ReliableRelayAttachMode::Any,
                                &mut recovery_excluded_paths,
                                &pending_validation_opens,
                            )
                            .await
                            {
                                Ok(attached) if attached > 0 => {
                                    sender_retry_at = None;
                                    match recover_reliable_relay_after_path_failure(
                                        &mut sender,
                                        &mut sender_queue,
                                        context,
                                        &spec,
                                        relay_lane,
                                        &mut remotes,
                                        &mut send_stream,
                                        instance,
                                    )
                                    .await
                                    {
                                        Ok(Some(repair_queued)) => {
                                            last_stream_progress_at = Instant::now();
                                            last_response_stall_repair_at = Instant::now();
                                            if repair_queued {
                                                sender_retry_at = None;
                                            }
                                        }
                                        Ok(None) => break Err(err),
                                        Err(recovery_err) => break Err(recovery_err),
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
                            let should_activate_delivery_path = delivered_payload_bytes > 0
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
                                );
                            if should_activate_delivery_path {
                                match sender
                                    .reannounce_path_instance_as_active(
                                        context,
                                        &mut remotes,
                                        instance,
                                        &spec,
                                        relay_lane,
                                    )
                                    .await
                                {
                                    Ok(true) => {
                                        send_stream.update_max_offset(remotes.max_offset());
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
                                    Ok(false) => {}
                                    Err(err) => {
                                        eprintln!(
                                            "warning: reliable delivery active reannounce failed: {err}"
                                        );
                                    }
                                }
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
                                match attach_reliable_relay_paths_with_recovery_exclusions(
                                    context,
                                    &spec,
                                    relay_lane,
                                    &mut remotes,
                                    &send_stream,
                                    !local_open,
                                    ReliableRelayAttachMode::Any,
                                    &mut recovery_excluded_paths,
                                    &pending_validation_opens,
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
                        update_repair_authoritative_ack_snapshot(
                            &mut last_send_ack_frontier,
                            &mut last_send_ack_ranges,
                            &mut last_send_ack_complete,
                            complete,
                            &normalized_ranges,
                        );
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
                        let owner_progress = sender
                            .release_normalized_acked_ranges_with_owner_progress(
                                context,
                                &normalized_ranges,
                            );
                        let service_instance = request_outstanding_window
                            .resolved_service_instance(
                                sender
                                    .request_ordered_service_instance()
                                    .filter(|service| remotes.contains_path_instance(*service)),
                                remotes.active_path_instance(),
                            );
                        let udp_growth_interval = service_instance
                            .filter(|service| service.key.underlay == UnderlayProtocol::Udp)
                            .map(|service| {
                                reliable_relay_request_ack_growth_interval(service, context)
                            });
                        let mut tcp_owner_progress = false;
                        for progress in owner_progress {
                            let owner_capable = sender.request_owner_ack_can_grow_window(
                                &remotes,
                                service_instance,
                                progress.instance,
                            );
                            if service_instance.is_some_and(|service| {
                                service.key.underlay == UnderlayProtocol::Tcp
                            }) {
                                tcp_owner_progress |= owner_capable;
                            } else {
                                // QUIC keeps product-ACK turnover separate from
                                // its native packet ACK and congestion model.
                                request_outstanding_window.record_acked(
                                    progress.bytes,
                                    progress.instance,
                                    service_instance,
                                    owner_capable,
                                    relay_lane,
                                    udp_growth_interval
                                        .unwrap_or_else(|| transport_pto_from_snapshot(None)),
                                    context.mux_limits,
                                );
                            }
                        }
                        if tcp_owner_progress {
                            let turnover_bytes = sender.request_tcp_owner_ack_turnover_bytes(
                                &remotes,
                                service_instance,
                                Instant::now(),
                            );
                            request_outstanding_window.record_tcp_ack_clock_turnover(
                                turnover_bytes,
                                service_instance,
                                relay_lane,
                                context.mux_limits,
                            );
                        }
                        sender_queue.release_normalized_acked_repairs(&normalized_ranges);
                        let base_repair_limit = adaptive_reliable_relay_repair_bytes(
                            path_snapshot,
                            relay_lane,
                            context.mux_limits,
                        );
                        let repair_event_budget =
                            sender.repair_extra_event_budget_remaining(context.mux_limits);
                        let has_multipath_repair_alternative = remotes.path_keys().len() > 1;
                        let (owner_underlay, owner_timing_path, repair_target) =
                            sender.ack_gap_repair_path_model(
                                context,
                                &remotes,
                                &send_stream,
                                &normalized_ranges,
                                base_repair_limit,
                                relay_lane,
                            );
                        let ack_gap_repair_ready = ack_gap_repair.repair_ready(
                            complete,
                            &normalized_ranges,
                            owner_timing_path
                                .map(|snapshot| snapshot.underlay)
                                .or(owner_underlay)
                                .or(remotes.active_path_underlay()),
                            has_multipath_repair_alternative,
                            owner_timing_path,
                            relay_lane,
                        );
                        let repair_path = repair_target.map(|(_, snapshot)| snapshot);
                        let repair_limit = if ack_gap_repair_ready {
                            reliable_persistent_ack_gap_repair_limit_bytes(
                                repair_path,
                                repair_path.and(owner_underlay),
                                relay_lane,
                                send_stream.repair_bytes(),
                                context.mux_limits,
                            )
                        } else {
                            base_repair_limit.min(repair_event_budget)
                        };
                        let amplified_ack_gap_repair = ack_gap_repair_ready
                            && repair_limit > base_repair_limit;
                        let ack_gap_repair_cause = if amplified_ack_gap_repair {
                            let (target, snapshot) = repair_target
                                .expect("amplified repair requires a modeled output");
                            RelaySendCause::persistent_client_ack_gap_repair(
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
                            let fin_tail_limit = if !local_open {
                                let limit = reliable_critical_tail_repair_limit_bytes(
                                    base_repair_limit,
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
                                false,
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
                        let mut queued_persistent_ack_gap_repair = false;
                        for frame in repair_frames {
                            let queued = if sender_queue.has_queued_repair_overlap(&frame) {
                                false
                            } else if critical_tail_repair {
                                if repair_kind == "fin_tail" {
                                    sender.enqueue_critical_tail_repair_frame(
                                        &mut sender_queue,
                                        frame,
                                    )
                                } else {
                                    sender.enqueue_critical_repair_frame(
                                        &mut sender_queue,
                                        frame,
                                        ack_gap_repair_cause,
                                    );
                                    true
                                }
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
                                queued_persistent_ack_gap_repair |=
                                    ack_gap_repair_ready && repair_kind == "ack_gap";
                                sender_retry_at = None;
                            }
                        }
                        if queued_persistent_ack_gap_repair {
                            ack_gap_repair.record_repair_queued();
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
                                    local_fin_sent = true;
                                    terminal_fin_replayed = false;
                                    last_stream_progress_at = Instant::now();
                                }
                                Err(err) if reliable_relay_error_is_migratable(&err) => {
                                    match attach_reliable_relay_paths_with_recovery_exclusions(
                                        context,
                                        &spec,
                                        relay_lane,
                                        &mut remotes,
                                        &send_stream,
                                        true,
                                        ReliableRelayAttachMode::Any,
                                        &mut recovery_excluded_paths,
                                        &pending_validation_opens,
                                    )
                                    .await
                                    {
                                        Ok(attached) if attached > 0 => {
                                            sender_retry_at = None;
                                            pending_local_fin = false;
                                            local_fin_sent = true;
                                            terminal_fin_replayed = false;
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
                                pending_local_fin = false;
                                local_fin_sent = true;
                                terminal_fin_replayed = false;
                                last_stream_progress_at = Instant::now();
                                last_response_stall_repair_at = Instant::now();
                            }
                            Ok(false) => {}
                            Err(err) if reliable_relay_error_is_migratable(&err) => {
                                sender
                                    .fail_client_path_instance(context, &mut remotes, instance)
                                    .await;
                                recovery_excluded_paths.insert(path_key);
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

    request_bulk_flow.update(false, None);

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
    cancel_pending_validation_opens(stream_id, &mut pending_validation_opens);

    let remaining_paths = remotes
        .paths
        .iter()
        .map(|path| (path.key(), path.stream.lane, path.load_reserved))
        .collect::<Vec<_>>();
    if result.is_ok() {
        for (key, stats) in path_stats {
            context.mark_relay_path_delivery(key.underlay, key.index, stats);
        }
    }
    // Success and failure both end logical stream ownership. Leaving sibling
    // carrier entries installed after one-path failure poisons later reuse.
    remotes.close_all().await;
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
    for (key, lane, load_reserved) in remaining_paths {
        if relay_error_is_tcp_path_failure(&result) {
            context.mark_relay_path_failure(key.underlay, key.index);
        }
        if load_reserved {
            context.release_relay_path_load(key.underlay, key.index, lane);
        }
    }
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_flush("multipath_stream_close");
    result
}

fn reliable_relay_lane_changed(previous: FlowLane, current: FlowLane) -> bool {
    previous != current
}

#[allow(clippy::too_many_arguments)]
pub(in crate::runtime) async fn recover_reliable_relay_after_path_failure(
    sender: &mut RelaySenderService,
    sender_queue: &mut ReliableRelaySenderQueue,
    context: &ClientPathContext,
    spec: &ReliableRelayOpenSpec,
    lane: FlowLane,
    remotes: &mut ReliableRelayRemoteSet,
    send_stream: &mut ReliableSendStream,
    failed_instance: RelayPathInstance,
) -> Result<Option<bool>, RuntimeError> {
    if remotes.is_empty() {
        return Ok(None);
    }

    if remotes.active_path_instance().is_some() {
        sender
            .reannounce_active_path(context, remotes, spec, lane)
            .await?;
    } else if let Some(instance) = remotes.repair_path_instance_for_service_recovery() {
        let _ = sender
            .reannounce_path_instance_as_active(context, remotes, instance, spec, lane)
            .await?;
    }

    send_stream.update_max_offset(remotes.max_offset());
    let repair_queued = sender.enqueue_failed_path_instance_gap_repairs(
        sender_queue,
        context,
        remotes,
        send_stream,
        failed_instance,
        lane,
    );
    Ok(Some(repair_queued))
}

async fn switch_reliable_relay_to_best_path(
    context: &ClientPathContext,
    spec: &ReliableRelayOpenSpec,
    lane: FlowLane,
    remotes: &mut ReliableRelayRemoteSet,
    send_stream: &ReliableSendStream,
    resend_fin: bool,
    mode: ReliableRelayAttachMode,
    pending_validation_opens: &HashMap<RelayPathKey, RelayValidationOpenTask>,
) -> Result<bool, RuntimeError> {
    let inflight_path_claims = pending_validation_opens
        .keys()
        .copied()
        .collect::<HashSet<_>>();
    let attached = attach_reliable_relay_paths(
        context,
        spec,
        lane,
        remotes,
        send_stream,
        resend_fin,
        mode,
        &inflight_path_claims,
    )
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
                #[cfg(feature = "lab-diagnostics")]
                let lane = opened.stream.lane;
                opened.close().await;
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
            // Preserve global health fencing. The second stream-local attempt
            // becomes eligible only after independent proof reactivates path.
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
            context
                .mark_relay_path_failure(validation_open.key.underlay, validation_open.key.index);
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
                opened.close().await;
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
    stream_id: StreamId,
    pending: &mut HashMap<RelayPathKey, RelayValidationOpenTask>,
) {
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = stream_id;
    for (key, task) in pending.drain() {
        #[cfg(not(feature = "lab-diagnostics"))]
        let _ = key;
        task.handle.abort();
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
    #[cfg(feature = "lab-diagnostics")]
    lane: FlowLane,
    handle: tokio::task::JoinHandle<()>,
}

const MAX_RELIABLE_RELAY_VALIDATION_OPEN_ATTEMPTS_PER_PATH: u8 = 2;

fn spawn_reliable_relay_validation_opens(
    context: &ClientPathContext,
    spec: &ReliableRelayOpenSpec,
    lane: FlowLane,
    remotes: &ReliableRelayRemoteSet,
    send_stream: &ReliableSendStream,
    pending: &mut HashMap<RelayPathKey, RelayValidationOpenTask>,
    attempts: &mut HashMap<RelayPathKey, u8>,
    result_tx: &mpsc::Sender<RelayValidationOpenResult>,
) -> bool {
    if !lane.is_bulk() {
        return false;
    }
    if !pending.is_empty() {
        return false;
    }
    let stream_id = remotes.stream_id();
    let payload_bytes =
        reliable_relay_bulk_validation_payload_bytes(send_stream, context.mux_limits);
    let candidates = reliable_relay_validation_open_candidates(context, remotes, payload_bytes);
    let candidates = reliable_relay_validation_probe_candidates(candidates, pending, attempts);
    if candidates.is_empty() {
        return false;
    }
    let mut spawned = false;
    for key in candidates {
        match key.underlay {
            UnderlayProtocol::Tcp if context.tcp_paths.get(key.index).is_some() => {}
            UnderlayProtocol::Udp if context.udp_paths.get(key.index).is_some() => {}
            _ => continue,
        }
        let attempt = attempts.entry(key).or_default();
        *attempt = attempt.saturating_add(1);
        let context = context.clone();
        let target = spec.target.clone();
        let ingress = spec.ingress;
        let result_tx = result_tx.clone();
        let handle = tokio::spawn(async move {
            let open_timeouts = reliable_relay_attach_open_timeouts(&context, key, lane);
            let result = match key.underlay {
                UnderlayProtocol::Tcp => {
                    let open_deadlines = ClientTcpOpenDeadlines::from_timeouts(
                        tokio::time::Instant::now(),
                        open_timeouts.live,
                        open_timeouts.setup,
                    );
                    let result = relay_path_open_with_timeout(
                        open_timeouts.setup,
                        open_remote_stream_on_reserved_path(
                            &context,
                            stream_id,
                            target,
                            ingress,
                            lane,
                            key.index,
                            StreamOpenRole::Validation,
                            open_deadlines,
                        ),
                    )
                    .await;
                    result
                }
                UnderlayProtocol::Udp => {
                    relay_path_open_with_timeout(
                        open_timeouts.setup,
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
                        ),
                    )
                    .await
                }
            };
            let message = RelayValidationOpenResult { key, result };
            if let Err(err) = result_tx.send(message).await {
                let RelayValidationOpenResult { key, result } = err.0;
                #[cfg(not(feature = "lab-diagnostics"))]
                let _ = key;
                if let Ok(opened) = result {
                    #[cfg(feature = "lab-diagnostics")]
                    let lane = opened.stream.lane;
                    opened.close().await;
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
        pending.insert(
            key,
            RelayValidationOpenTask {
                #[cfg(feature = "lab-diagnostics")]
                lane,
                handle,
            },
        );
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

fn reliable_relay_validation_open_candidates(
    context: &ClientPathContext,
    remotes: &ReliableRelayRemoteSet,
    payload_bytes: usize,
) -> Vec<RelayPathKey> {
    // Admission gives unproven OwnerData only to an idle carrier. Offer the
    // same candidates first here; otherwise the one-shot opener can attach an
    // occupied measured path that startup policy must immediately reject.
    let mut candidates = context
        .ordered_reliable_bulk_validation_path_keys(payload_bytes)
        .into_iter()
        .chain(context.ordered_reliable_bulk_striping_path_keys(payload_bytes))
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
    prefer_active_family_validation_open_candidate(candidates, remotes.active_path_underlay())
}

fn prefer_active_family_validation_open_candidate(
    mut candidates: Vec<RelayPathKey>,
    active_underlay: Option<UnderlayProtocol>,
) -> Vec<RelayPathKey> {
    let Some(active_underlay) = active_underlay else {
        return candidates;
    };
    if let Some(position) = candidates
        .iter()
        .position(|candidate| candidate.underlay == active_underlay)
    {
        let candidate = candidates.remove(position);
        candidates.insert(0, candidate);
    }
    candidates
}

fn reliable_relay_validation_probe_candidates(
    candidates: Vec<RelayPathKey>,
    pending: &HashMap<RelayPathKey, RelayValidationOpenTask>,
    attempts: &HashMap<RelayPathKey, u8>,
) -> Vec<RelayPathKey> {
    let mut selected = Vec::new();
    let mut selected_underlay = None;
    for candidate in candidates {
        if pending.contains_key(&candidate)
            || attempts.get(&candidate).copied().unwrap_or(0)
                >= MAX_RELIABLE_RELAY_VALIDATION_OPEN_ATTEMPTS_PER_PATH
            || selected.contains(&candidate)
        {
            continue;
        }
        let underlay = *selected_underlay.get_or_insert(candidate.underlay);
        if candidate.underlay != underlay {
            continue;
        }
        selected.push(candidate);
    }
    selected
}

pub(in crate::runtime) struct RelayPathAttachRequest<'a> {
    spec: &'a ReliableRelayOpenSpec,
    lane: FlowLane,
    send_stream: &'a ReliableSendStream,
    resend_fin: bool,
    candidates: Vec<RelayPathKey>,
    role: StreamOpenRole,
    send_attach_control: bool,
}

struct RelayPathAttachResult {
    attached: usize,
    key: Option<RelayPathKey>,
}

async fn attach_relay_path_candidates(
    context: &ClientPathContext,
    remotes: &mut ReliableRelayRemoteSet,
    request: RelayPathAttachRequest<'_>,
) -> Result<RelayPathAttachResult, RuntimeError> {
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
                let OpenedRemoteStream { stream, path_index } = opened;
                // Attach-control emission is fallible. Keep cleanup armed until
                // the accepted stream is committed into the remote set.
                let accepted = AcceptedRemoteStreamGuard::new(stream);
                let attach_control_result = if request.send_attach_control {
                    send_sender_service_attach_control_frames(
                        accepted.stream(),
                        request.send_stream,
                        request.resend_fin,
                    )
                    .await
                } else {
                    Ok(())
                };
                match attach_control_result {
                    Ok(()) => {
                        let opened = OpenedRemoteStream {
                            stream: accepted.commit(),
                            path_index,
                        };
                        if request.role != StreamOpenRole::Active {
                            // Path choice temporarily reserves a share while the
                            // open is in flight. Passive attachments keep only
                            // their actual queue/inflight debt after attachment.
                            context.release_relay_path_load(key.underlay, key.index, request.lane);
                        }
                        match request.role {
                            StreamOpenRole::Active => remotes.attach(opened),
                            StreamOpenRole::Repair => remotes.attach_for_repair(opened),
                            StreamOpenRole::Validation => remotes.attach_for_validation(opened),
                        }
                        attached += 1;
                        return Ok(RelayPathAttachResult {
                            attached,
                            key: Some(key),
                        });
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
        Ok(RelayPathAttachResult {
            attached,
            key: None,
        })
    } else if remotes.is_empty() {
        Err(last_retryable_error.unwrap_or_else(|| no_schedulable_reliable_path_error(context)))
    } else {
        Ok(RelayPathAttachResult {
            attached: 0,
            key: None,
        })
    }
}

pub(in crate::runtime) async fn open_remote_stream_for_relay_path(
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
                udp_relay_attachment_open_options(role),
            )
            .await
        }
    }
}

pub(in crate::runtime) fn relay_path_open_error_is_retryable(
    underlay: UnderlayProtocol,
    err: &RuntimeError,
) -> bool {
    match underlay {
        UnderlayProtocol::Tcp => stream_open_error_is_path_retryable(err),
        UnderlayProtocol::Udp => udp_stream_open_error_is_path_retryable(err),
    }
}

pub(in crate::runtime) fn no_schedulable_reliable_path_error(
    context: &ClientPathContext,
) -> RuntimeError {
    if !context.tcp_paths.is_empty() && !context.udp_paths.is_empty() {
        RuntimeError::NoSchedulableReliablePath
    } else if !context.tcp_paths.is_empty() {
        RuntimeError::NoSchedulableTcpPath
    } else {
        RuntimeError::NoSchedulableUdpPath
    }
}

pub(in crate::runtime) async fn attach_reliable_relay_paths(
    context: &ClientPathContext,
    spec: &ReliableRelayOpenSpec,
    lane: FlowLane,
    remotes: &mut ReliableRelayRemoteSet,
    send_stream: &ReliableSendStream,
    resend_fin: bool,
    mode: ReliableRelayAttachMode,
    inflight_path_claims: &HashSet<RelayPathKey>,
) -> Result<usize, RuntimeError> {
    let mut recovery_excluded_paths = HashSet::<RelayPathKey>::new();
    attach_reliable_relay_paths_with_claims_and_recovery_exclusions(
        context,
        spec,
        lane,
        remotes,
        send_stream,
        resend_fin,
        mode,
        &mut recovery_excluded_paths,
        inflight_path_claims,
    )
    .await
}

async fn attach_reliable_relay_paths_with_recovery_exclusions(
    context: &ClientPathContext,
    spec: &ReliableRelayOpenSpec,
    lane: FlowLane,
    remotes: &mut ReliableRelayRemoteSet,
    send_stream: &ReliableSendStream,
    resend_fin: bool,
    mode: ReliableRelayAttachMode,
    recovery_excluded_paths: &mut HashSet<RelayPathKey>,
    pending_validation_opens: &HashMap<RelayPathKey, RelayValidationOpenTask>,
) -> Result<usize, RuntimeError> {
    // Validation already owns logical (stream, path) membership. Synchronous
    // recovery must not race that claim through either TCP or QUIC carriers.
    let inflight_path_claims = pending_validation_opens
        .keys()
        .copied()
        .collect::<HashSet<_>>();
    attach_reliable_relay_paths_with_claims_and_recovery_exclusions(
        context,
        spec,
        lane,
        remotes,
        send_stream,
        resend_fin,
        mode,
        recovery_excluded_paths,
        &inflight_path_claims,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn attach_reliable_relay_paths_with_claims_and_recovery_exclusions(
    context: &ClientPathContext,
    spec: &ReliableRelayOpenSpec,
    lane: FlowLane,
    remotes: &mut ReliableRelayRemoteSet,
    send_stream: &ReliableSendStream,
    resend_fin: bool,
    mode: ReliableRelayAttachMode,
    recovery_excluded_paths: &mut HashSet<RelayPathKey>,
    inflight_path_claims: &HashSet<RelayPathKey>,
) -> Result<usize, RuntimeError> {
    let payload_bytes = match mode {
        ReliableRelayAttachMode::Any | ReliableRelayAttachMode::RecoveryRepair => {
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
                candidates: reliable_relay_exclude_inflight_open_claims(
                    context.ordered_reliable_bulk_striping_path_keys(payload_bytes),
                    &inflight_path_claims,
                ),
                role: StreamOpenRole::Validation,
                send_attach_control: false,
            },
        )
        .await;
        match result {
            Ok(result) if result.attached > 0 || !remotes.is_empty() => {
                return Ok(result.attached);
            }
            Ok(_) => {}
            Err(err)
                if remotes.is_empty()
                    && (stream_open_error_is_path_retryable(&err)
                        || udp_stream_open_error_is_path_retryable(&err)) => {}
            Err(err) => return Err(err),
        }
    }
    let role = reliable_relay_attach_role(lane, send_stream, resend_fin, mode);
    if role == StreamOpenRole::Repair {
        let result = attach_relay_path_candidates(
            context,
            remotes,
            RelayPathAttachRequest {
                spec,
                lane,
                send_stream,
                resend_fin,
                candidates: reliable_relay_exclude_inflight_open_claims(
                    reliable_relay_recovery_attach_candidates(
                        reliable_relay_repair_path_candidates(
                            context,
                            remotes,
                            lane,
                            payload_bytes,
                        ),
                        recovery_excluded_paths,
                        remotes.is_empty(),
                    ),
                    &inflight_path_claims,
                ),
                role,
                send_attach_control: true,
            },
        )
        .await?;
        if result.attached > 0
            && let Some(key) = result.key
        {
            recovery_excluded_paths.insert(key);
        }
        return Ok(result.attached);
    }
    let result = attach_relay_path_candidates(
        context,
        remotes,
        RelayPathAttachRequest {
            spec,
            lane,
            send_stream,
            resend_fin,
            candidates: reliable_relay_exclude_inflight_open_claims(
                reliable_relay_recovery_attach_candidates(
                    reliable_relay_active_path_candidates(context, remotes, lane, payload_bytes),
                    recovery_excluded_paths,
                    remotes.is_empty(),
                ),
                &inflight_path_claims,
            ),
            role,
            send_attach_control: true,
        },
    )
    .await?;
    Ok(result.attached)
}

pub(in crate::runtime) fn reliable_relay_attach_role(
    lane: FlowLane,
    send_stream: &ReliableSendStream,
    resend_fin: bool,
    mode: ReliableRelayAttachMode,
) -> StreamOpenRole {
    match mode {
        ReliableRelayAttachMode::BulkStriping => StreamOpenRole::Validation,
        ReliableRelayAttachMode::RecoveryRepair => StreamOpenRole::Repair,
        ReliableRelayAttachMode::Any
            if reliable_relay_should_race_repair(lane, send_stream, resend_fin, mode) =>
        {
            StreamOpenRole::Repair
        }
        ReliableRelayAttachMode::Any => StreamOpenRole::Active,
    }
}

pub(in crate::runtime) fn reliable_relay_active_path_candidates(
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

fn reliable_relay_recovery_attach_candidates(
    candidates: Vec<RelayPathKey>,
    recovery_excluded_paths: &HashSet<RelayPathKey>,
    allow_excluded_last_resort: bool,
) -> Vec<RelayPathKey> {
    if recovery_excluded_paths.is_empty() {
        return candidates;
    }
    let filtered = candidates
        .iter()
        .copied()
        .filter(|key| !recovery_excluded_paths.contains(key))
        .collect::<Vec<_>>();
    if filtered.is_empty() && allow_excluded_last_resort {
        candidates
    } else {
        filtered
    }
}

fn reliable_relay_exclude_inflight_open_claims(
    candidates: Vec<RelayPathKey>,
    inflight_path_claims: &HashSet<RelayPathKey>,
) -> Vec<RelayPathKey> {
    candidates
        .into_iter()
        .filter(|candidate| !inflight_path_claims.contains(candidate))
        .collect()
}

pub(in crate::runtime) fn reliable_relay_repair_path_candidates(
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

pub(in crate::runtime) fn reliable_relay_should_race_repair(
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

pub(in crate::runtime) fn reliable_relay_attach_payload_bytes(
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

pub(in crate::runtime) fn reliable_relay_bulk_striping_payload_bytes(
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

pub(in crate::runtime) fn reliable_relay_bulk_validation_payload_bytes(
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

pub(in crate::runtime) fn reliable_relay_stall_watch_active(
    send_stream: &ReliableSendStream,
    recv_stream: &ReliableRecvStream,
    remote_open: bool,
    lane: FlowLane,
    interactive_response_pending: bool,
    mux_limits: MuxLimits,
) -> bool {
    send_stream.repair_bytes() > 0
        || (remote_open && interactive_response_pending)
        || reliable_relay_response_stall_watch_active(recv_stream, remote_open, lane, mux_limits)
}

pub(in crate::runtime) fn reliable_relay_response_stall_watch_active(
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

pub(in crate::runtime) fn reliable_relay_stall_progress_anchor(
    last_stream_progress_at: Instant,
    last_delivery_progress_at: Instant,
    last_response_stall_repair_at: Instant,
    recv_stream: &ReliableRecvStream,
    remote_open: bool,
    lane: FlowLane,
    interactive_response_pending: bool,
    mux_limits: MuxLimits,
) -> Instant {
    if (remote_open && interactive_response_pending)
        || reliable_relay_response_stall_watch_active(recv_stream, remote_open, lane, mux_limits)
    {
        last_delivery_progress_at.max(last_response_stall_repair_at)
    } else {
        last_stream_progress_at
    }
}

pub(in crate::runtime) fn reliable_relay_receive_hole_repair_active(
    recv_stream: &ReliableRecvStream,
    remote_open: bool,
) -> bool {
    remote_open && recv_stream.next_offset() > 0 && recv_stream.reorder_bytes() > 0
}

pub(in crate::runtime) fn reliable_relay_receive_hole_repair_deadline(
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

pub(in crate::runtime) fn reliable_relay_product_stall_preserves_attached_path_set(
    remotes: &ReliableRelayRemoteSet,
) -> bool {
    remotes.accepted_product_path_count() > 1
}

pub(in crate::runtime) fn reliable_relay_product_stall_should_try_alternate_attach(
    remotes: &ReliableRelayRemoteSet,
) -> bool {
    remotes.accepted_product_path_count() <= 1 && remotes.active_path_underlay().is_some()
}

pub(in crate::runtime) fn reliable_relay_delivery_path_should_become_active(
    context: &ClientPathContext,
    current: Option<RelayPathKey>,
    delivered: RelayPathKey,
    lane: FlowLane,
    payload_bytes: usize,
) -> bool {
    if current == Some(delivered) {
        return false;
    }
    // Measured bulk delivery admits a Subflow; it does not implicitly rewrite
    // per-stream Service placement. Explicit stall/failure recovery owns bulk
    // Active reannouncement.
    if lane.is_bulk() {
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

pub(in crate::runtime) fn relay_underlay_identity_order(
    left: UnderlayProtocol,
    right: UnderlayProtocol,
) -> std::cmp::Ordering {
    // Stable identity tie-breaker only. Real scheduling order is decided before
    // this by path metrics and original config ordinal; this must not become a
    // TCP-vs-UDP preference.
    (left as u8).cmp(&(right as u8))
}

pub(in crate::runtime) fn reliable_relay_expects_interactive_response(lane: FlowLane) -> bool {
    matches!(
        lane,
        FlowLane::Control | FlowLane::Latency | FlowLane::RealtimeDatagram
    )
}

pub(in crate::runtime) fn reliable_relay_response_stall_watch_bytes(mux_limits: MuxLimits) -> u64 {
    (reliable_relay_buffer_len(mux_limits) as u64).min(mux_limits.max_stream_window_bytes)
}

pub(in crate::runtime) fn reliable_relay_stall_deadline(
    last_progress_at: Instant,
    path: Option<PathSnapshot>,
    lane: FlowLane,
) -> tokio::time::Instant {
    tokio::time::Instant::from_std(last_progress_at + reliable_relay_stall_timeout(path, lane))
}

pub(in crate::runtime) fn reliable_relay_product_stall_deadline(
    last_progress_at: Instant,
    last_attempt_at: Option<Instant>,
    path: Option<PathSnapshot>,
    lane: FlowLane,
) -> tokio::time::Instant {
    let stall_timeout = reliable_relay_stall_timeout(path, lane);
    match last_attempt_at.filter(|attempt| *attempt >= last_progress_at) {
        Some(last_attempt_at) => tokio::time::Instant::from_std(
            last_attempt_at + stall_timeout.saturating_mul(QUIC_PERSISTENT_CONGESTION_THRESHOLD),
        ),
        None => reliable_relay_stall_deadline(last_progress_at, path, lane),
    }
}

pub(in crate::runtime) fn reliable_relay_stall_timeout(
    path: Option<PathSnapshot>,
    lane: FlowLane,
) -> Duration {
    let _ = lane;
    transport_pto_from_snapshot(path)
}

#[cfg(test)]
mod tests;
